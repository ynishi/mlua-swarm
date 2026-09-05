//! Caller-side execution of an `agent_block` step — the piece that lets a
//! Blueprint whose deterministic steps are Lua blocks run against a
//! **hosted** `mse serve`.
//!
//! ## Why the block runs here and not on the server
//!
//! An in-process block (`AgentKind::AgentBlock` + `Runner::AgentBlockInProcess`)
//! runs inside the server process against `init_ctx.work_dir` — the
//! caller's repository. That only works while the server and the repository
//! share a host. A hosted server has neither the block scripts, nor `git`,
//! nor the repository, and the last one cannot be shipped to it: the point
//! of a checkout preparation, a branch merge, or a pre-commit scan is to
//! read or change the caller's working tree.
//!
//! So the invariant is *a block runs on the host that has the repository*.
//! This MCP is a stdio server — it already runs on that host — and the
//! `mse` binary already links `agent-block-core` for the server-side
//! runtime, so the same SDK call can be made from here. No LLM is involved:
//! the block is the worker.
//!
//! ## Wire contract (no server change)
//!
//! The Blueprint declares the step as an Operator agent bound to launch
//! variant [`LAUNCH_VARIANT`]:
//!
//! ```lua
//! { name = "checkout-prep", runner = { backend = "ws_operator", variant = "agent-block" },
//!   spec = { operator_ref = "main-ai" }, ... }
//! ```
//!
//! The server dispatches it like any Operator step — a `Spawn` frame with
//! `worker.variant = "agent-block"` — and this process's WS reader diverts
//! that frame as it arrives (`operator_client::route_frame`), so it never
//! reaches the queue `mse_pending_wait` pops from and runs whether or not
//! the MainAI is polling. A background task resolves the block by **agent
//! name** under the blocks dir ([`blocks_dir`]: [`BLOCKS_DIR_ENV`], else
//! `$MSE_HOME/blocks`) as `<dir>/<agent>/init.lua`, fetches the step's
//! prompt / system through the ordinary worker endpoint, runs the script
//! with the caller's `work_dir` as the project root, POSTs the result the
//! way a SubAgent would (`/v1/worker/artifact` for staged parts,
//! `/v1/worker/submit` for the body), and acks the spawn. The MainAI never
//! sees a turn for it.
//!
//! Naming the block after the agent keeps the Blueprint free of any path:
//! where the blocks live is this host's business, not the Blueprint's. A
//! Blueprint registered with an old server still dispatches — the variant
//! is an ordinary string to it.
//!
//! ## Script contract
//!
//! Identical to the server-side in-process runtime: the step's evaluated
//! `in` is the `_PROMPT` global, `profile.system_prompt` is `_CONTEXT`,
//! the launch's `task_metadata` is `_TASK_METADATA` and the declared agent
//! context is `_AGENT_CTX`. A script returns by `bus.emit(<kind>, payload)`
//! — `payload.content`, else `payload.response`, else the whole payload is
//! the body; `payload.ok = false` fails the attempt. `bus.emit("artifact",
//! {name, content})` stages a named part and leaves the script running.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use agent_block_core::bus::dispatcher::Handler;
use agent_block_core::host::{PromptSource, ScriptSource};
use agent_block_core::{run, BlockConfig};
use agent_block_types::error::BlockError;
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::oneshot;

/// The launch variant a Blueprint binds an agent to when the step is a
/// block this process should run itself.
pub const LAUNCH_VARIANT: &str = "agent-block";

/// Environment variable naming the directory that holds one `<name>/init.lua`
/// per block. Read per spawn. Wins over the `$MSE_HOME/blocks` default
/// (see [`blocks_dir`]).
pub const BLOCKS_DIR_ENV: &str = "MSE_BLOCKS_DIR";

/// The one `bus.emit` kind that stages a named part instead of finishing
/// the script — same reserved kind as the server-side runtime.
const ARTIFACT_EVENT_KIND: &str = "artifact";

/// Lua globals for the launch's `task_metadata` and the agent's declared
/// context — the same names the server-side runtime sets, so a block is
/// portable between the two.
const TASK_METADATA_GLOBAL: &str = "_TASK_METADATA";
const AGENT_CTX_GLOBAL: &str = "_AGENT_CTX";

/// What a popped `Spawn` frame has to carry for this process to run it as
/// a block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockSpawn {
    /// Correlation key for the `spawn_ack`.
    pub req_id: String,
    /// Step id — the `task_id` query of `/v1/worker/prompt`.
    pub task_id: String,
    /// Agent name = block name (`<dir>/<agent>/init.lua`).
    pub agent: String,
    /// Bearer for the worker endpoints.
    pub worker_handle: String,
}

/// Reads a popped frame as a block spawn: a `spawn` frame whose
/// `worker.variant` is [`LAUNCH_VARIANT`] and which carries a worker
/// handle. Anything else is `None` — the frame goes to the MainAI as
/// usual.
pub fn parse_block_spawn(kind: &str, req_id: &str, payload: &Value) -> Option<BlockSpawn> {
    if kind != "spawn" {
        return None;
    }
    let variant = payload.get("worker")?.get("variant")?.as_str()?;
    if variant != LAUNCH_VARIANT {
        return None;
    }
    let task_id = payload.get("task_id")?.as_str()?;
    let agent = payload.get("agent")?.as_str()?;
    let worker_handle = payload.get("worker_handle")?.as_str()?;
    Some(BlockSpawn {
        req_id: req_id.to_string(),
        task_id: task_id.to_string(),
        agent: agent.to_string(),
        worker_handle: worker_handle.to_string(),
    })
}

/// The directory under `$MSE_HOME` that stands in for [`BLOCKS_DIR_ENV`]
/// when it is not set — a sibling of `bp/` and `runs/`, so a symlink to
/// wherever the blocks live is enough and no process environment has to
/// change.
pub const DEFAULT_BLOCKS_SUBDIR: &str = "blocks";

/// The blocks directory this host resolves block names under:
/// [`BLOCKS_DIR_ENV`] when set, else `<mse_home>/blocks` when that exists.
pub fn blocks_dir() -> Result<PathBuf, String> {
    blocks_dir_from(std::env::var_os(BLOCKS_DIR_ENV), &super::mse_home())
}

/// [`blocks_dir`] with its two inputs explicit.
pub fn blocks_dir_from(env: Option<std::ffi::OsString>, mse_home: &Path) -> Result<PathBuf, String> {
    if let Some(v) = env.filter(|v| !v.is_empty()) {
        return Ok(PathBuf::from(v));
    }
    let default = mse_home.join(DEFAULT_BLOCKS_SUBDIR);
    if default.is_dir() {
        return Ok(default);
    }
    Err(format!(
        "no blocks directory: {BLOCKS_DIR_ENV} is not set and {} does not exist — this \
         process was asked to run a block agent step but does not know where the blocks \
         are. Set {BLOCKS_DIR_ENV} to the directory holding one <name>/init.lua per block, \
         or make {} that directory (a symlink is fine)",
        default.display(),
        default.display()
    ))
}

/// `<dir>/<block>/init.lua`, refusing a name that could step outside `dir`.
/// The name is an agent name from the Blueprint, so it is validated rather
/// than trusted: one path component, no separators, no `..`, not hidden.
pub fn resolve_block_script(dir: &Path, block: &str) -> Result<PathBuf, String> {
    if block.is_empty()
        || block.starts_with('.')
        || block.contains('/')
        || block.contains('\\')
        || block.contains("..")
    {
        return Err(format!(
            "block name {block:?} is not a single plain path component (no '/', '\\', '..', \
             leading '.')"
        ));
    }
    let path = dir.join(block).join("init.lua");
    if !path.is_file() {
        return Err(format!(
            "block {block:?} not found: {} is not a file (blocks dir = {})",
            path.display(),
            dir.display()
        ));
    }
    Ok(path)
}

/// What a block produced: the terminal body, whether the attempt passed,
/// and the parts it staged along the way (in emit order).
#[derive(Debug)]
pub struct BlockOutcome {
    pub value: Value,
    pub ok: bool,
    pub artifacts: Vec<(String, Value)>,
}

/// Everything one block run needs, resolved from the worker payload.
#[derive(Debug)]
pub struct BlockInput {
    pub script: PathBuf,
    pub project_root: PathBuf,
    pub prompt: String,
    pub system: Option<String>,
    /// `_TASK_METADATA` / `_AGENT_CTX` (see the module doc).
    pub extra_globals: HashMap<String, Value>,
}

/// The Lua globals derived from a worker payload's `context` object —
/// the same field → global mapping as the server-side runtime, read off the
/// wire JSON rather than the typed view so this stays independent of the
/// context type's evolution.
pub fn context_globals(context: Option<&Value>) -> HashMap<String, Value> {
    let mut globals = HashMap::new();
    let Some(ctx) = context else {
        return globals;
    };
    if let Some(meta) = ctx.get("task_metadata").filter(|v| !v.is_null()) {
        globals.insert(TASK_METADATA_GLOBAL.to_string(), meta.clone());
    }
    if let Some(extra) = ctx.get("extra").and_then(|v| v.as_object()) {
        if !extra.is_empty() {
            globals.insert(
                AGENT_CTX_GLOBAL.to_string(),
                Value::Object(extra.clone()),
            );
        }
    }
    globals
}

/// The project root a block runs in: the launch's `work_dir`, else its
/// `project_root`, else this process's cwd — the same precedence the
/// server-side runtime applies per invocation.
pub fn project_root_from_context(context: Option<&Value>) -> PathBuf {
    context
        .and_then(|c| {
            c.get("work_dir")
                .and_then(Value::as_str)
                .or_else(|| c.get("project_root").and_then(Value::as_str))
        })
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
}

/// Collects what the script emits: staged parts accumulate, the first
/// non-artifact emit is the terminal result.
struct Captor {
    tx: Mutex<Option<oneshot::Sender<(Value, bool)>>>,
    artifacts: Mutex<Vec<(String, Value)>>,
}

#[async_trait]
impl Handler for Captor {
    async fn call(
        &self,
        kind: String,
        _id: String,
        payload: Value,
        _meta: Value,
    ) -> Result<Value, BlockError> {
        if kind == ARTIFACT_EVENT_KIND {
            let name = payload
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    BlockError::Runtime(format!(
                        "bus.emit(\"{ARTIFACT_EVENT_KIND}\", ...) requires a string `name` \
                         field naming the part (got: {payload})"
                    ))
                })?
                .to_string();
            let content = payload.get("content").cloned().unwrap_or(Value::Null);
            if let Ok(mut parts) = self.artifacts.lock() {
                parts.push((name, content));
            }
            return Ok(Value::Null);
        }
        let ok = payload.get("ok").and_then(|v| v.as_bool()).unwrap_or(true);
        let value = payload
            .get("content")
            .cloned()
            .or_else(|| payload.get("response").cloned())
            .unwrap_or_else(|| payload.clone());
        if let Ok(mut guard) = self.tx.lock() {
            if let Some(tx) = guard.take() {
                let _ = tx.send((value, ok));
            }
        }
        Ok(Value::Null)
    }
}

/// Runs one block to completion on this host.
pub async fn run_block(input: BlockInput) -> Result<BlockOutcome, String> {
    let (tx, rx) = oneshot::channel();
    let captor = std::sync::Arc::new(Captor {
        tx: Mutex::new(Some(tx)),
        artifacts: Mutex::new(Vec::new()),
    });
    let handler: std::sync::Arc<dyn Handler> = captor.clone();

    let mut builder = BlockConfig::builder(ScriptSource::Path(input.script), input.project_root)
        .mcp_rpc_timeout(Duration::from_secs(30))
        .prompt(PromptSource::Inline(input.prompt))
        .host_handler(handler)
        .auto_serve_bus(true);
    if let Some(system) = input.system {
        builder = builder.context(PromptSource::Inline(system));
    }
    if !input.extra_globals.is_empty() {
        builder = builder.extra_globals(input.extra_globals);
    }
    let config = builder.build();

    let run_result = tokio::spawn(run(config))
        .await
        .map_err(|e| format!("agent-block task join: {e}"))?;
    run_result.map_err(|e| format!("agent-block run failed: {e}"))?;

    // The script has finished, so whatever it emitted is already in the
    // channel; a script that never emitted leaves it empty. Read it without
    // waiting — this side still holds the captor (for the artifacts), so an
    // `await` here would never see the sender drop.
    let mut rx = rx;
    let (value, ok) = rx.try_recv().map_err(|_| {
        "agent-block script finished without emitting a result via bus".to_string()
    })?;
    let artifacts = captor
        .artifacts
        .lock()
        .map(|parts| parts.clone())
        .unwrap_or_default();
    Ok(BlockOutcome {
        value,
        ok,
        artifacts,
    })
}

/// The text form a value takes on the worker submit wire: a string as is,
/// anything else as JSON — what a SubAgent posting a raw body would send.
pub fn body_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// agent-block-core opens its KV / SQL / TS SQLite stores under
    /// `$HOME/.agent-block/` by default, and a `cargo test --workspace`
    /// runs several test binaries at once — the shared files then contend
    /// (`journal_mode=WAL: database is locked`, seen on the macOS CI lane).
    /// `:memory:` gives each process its own store; nothing here needs
    /// state to outlive a run. Same isolation as
    /// `tests/agent_block_script_e2e.rs`, set once per process before the
    /// first runtime reads the env.
    fn isolate_agent_block_state() {
        static ISOLATED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
        ISOLATED.get_or_init(|| {
            std::env::set_var("AGENT_BLOCK_KV_PATH", ":memory:");
            std::env::set_var("AGENT_BLOCK_SQL_PATH", ":memory:");
            std::env::set_var("AGENT_BLOCK_TS_PATH", ":memory:");
        });
    }

    fn spawn_payload(variant: &str) -> Value {
        serde_json::json!({
            "task_id": "T-1",
            "agent": "checkout-prep",
            "attempt": 1,
            "capability_token": "cap",
            "worker_handle": "wh-deadbeef",
            "worker": { "variant": variant, "tools": [] },
            "directive": "..."
        })
    }

    #[test]
    fn parse_block_spawn_takes_only_the_block_variant() {
        let p = spawn_payload(LAUNCH_VARIANT);
        let s = parse_block_spawn("spawn", "r1", &p).expect("block spawn");
        assert_eq!(
            s,
            BlockSpawn {
                req_id: "r1".into(),
                task_id: "T-1".into(),
                agent: "checkout-prep".into(),
                worker_handle: "wh-deadbeef".into(),
            }
        );
        assert!(parse_block_spawn("spawn", "r1", &spawn_payload("claude")).is_none());
        assert!(parse_block_spawn("ask", "r1", &p).is_none());
        let mut no_handle = p.clone();
        no_handle.as_object_mut().unwrap().remove("worker_handle");
        assert!(parse_block_spawn("spawn", "r1", &no_handle).is_none());
    }

    #[test]
    fn blocks_dir_prefers_env_then_the_mse_home_default() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        let err = blocks_dir_from(None, home).unwrap_err();
        assert!(err.contains(BLOCKS_DIR_ENV) && err.contains("blocks"), "{err}");
        assert!(
            blocks_dir_from(Some(std::ffi::OsString::new()), home).is_err(),
            "an empty env value counts as unset"
        );
        std::fs::create_dir_all(home.join(DEFAULT_BLOCKS_SUBDIR)).unwrap();
        assert_eq!(
            blocks_dir_from(None, home).unwrap(),
            home.join(DEFAULT_BLOCKS_SUBDIR)
        );
        assert_eq!(
            blocks_dir_from(Some("/elsewhere".into()), home).unwrap(),
            PathBuf::from("/elsewhere"),
            "env wins over the default even when the default exists"
        );
    }

    #[test]
    fn resolve_block_script_rejects_names_that_leave_the_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        std::fs::create_dir_all(dir.join("ok")).unwrap();
        std::fs::write(dir.join("ok").join("init.lua"), "return 1").unwrap();
        assert_eq!(
            resolve_block_script(dir, "ok").unwrap(),
            dir.join("ok").join("init.lua")
        );
        for bad in ["", "..", "../ok", "a/b", "a\\b", ".hidden", "ok/../ok"] {
            let err = resolve_block_script(dir, bad).unwrap_err();
            assert!(
                err.contains("not a single plain path component"),
                "{bad:?}: {err}"
            );
        }
        let err = resolve_block_script(dir, "missing").unwrap_err();
        assert!(err.contains("not found"), "{err}");
    }

    #[test]
    fn context_globals_and_project_root_follow_the_server_runtime() {
        let ctx = serde_json::json!({
            "work_dir": "/w",
            "project_root": "/p",
            "task_metadata": { "issue": 7 },
            "extra": { "k": "v" }
        });
        let g = context_globals(Some(&ctx));
        assert_eq!(g[TASK_METADATA_GLOBAL], serde_json::json!({ "issue": 7 }));
        assert_eq!(g[AGENT_CTX_GLOBAL], serde_json::json!({ "k": "v" }));
        assert_eq!(project_root_from_context(Some(&ctx)), PathBuf::from("/w"));
        let no_work_dir = serde_json::json!({ "project_root": "/p", "extra": {} });
        assert!(context_globals(Some(&no_work_dir)).is_empty());
        assert_eq!(
            project_root_from_context(Some(&no_work_dir)),
            PathBuf::from("/p")
        );
        assert!(context_globals(None).is_empty());
    }

    /// The whole contract end to end on a real script: `_PROMPT` /
    /// `_CONTEXT` / `_TASK_METADATA` reach the block, a staged part is
    /// collected, the terminal emit's `response` is the body.
    #[tokio::test]
    async fn run_block_runs_a_script_with_the_worker_globals() {
        isolate_agent_block_state();
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("init.lua");
        std::fs::write(
            &script,
            r#"
bus.emit("artifact", { name = "verdict", content = "PASS" })
bus.emit("worker_result", {
  ok = true,
  response = {
    prompt = _PROMPT,
    context = _CONTEXT,
    issue = _TASK_METADATA and _TASK_METADATA.issue or nil,
  },
})
"#,
        )
        .unwrap();
        let mut globals = HashMap::new();
        globals.insert(
            TASK_METADATA_GLOBAL.to_string(),
            serde_json::json!({ "issue": 42 }),
        );
        let out = run_block(BlockInput {
            script,
            project_root: tmp.path().to_path_buf(),
            prompt: "the seed".into(),
            system: Some("you are a block".into()),
            extra_globals: globals,
        })
        .await
        .expect("block runs");
        assert!(out.ok);
        assert_eq!(out.value["prompt"], "the seed");
        assert_eq!(out.value["context"], "you are a block");
        assert_eq!(out.value["issue"], 42);
        assert_eq!(
            out.artifacts,
            vec![("verdict".to_string(), Value::String("PASS".into()))]
        );
    }

    #[tokio::test]
    async fn run_block_reports_a_script_that_never_emits() {
        isolate_agent_block_state();
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("init.lua");
        std::fs::write(&script, "local x = 1\n").unwrap();
        let err = run_block(BlockInput {
            script,
            project_root: tmp.path().to_path_buf(),
            prompt: String::new(),
            system: None,
            extra_globals: HashMap::new(),
        })
        .await
        .unwrap_err();
        assert!(err.contains("without emitting"), "{err}");
    }

    #[test]
    fn body_text_keeps_strings_and_serializes_the_rest() {
        assert_eq!(body_text(&Value::String("raw".into())), "raw");
        assert_eq!(body_text(&serde_json::json!({ "a": 1 })), r#"{"a":1}"#);
    }
}
