//! [`AgentBlockInProcessSpawnerFactory`] — in-process headless LLM
//! agent execution over the `agent-block-core` SDK.
//!
//! ## Design responsibility — a state-less factory
//!
//! The factory is a **kind-level general-purpose builder** — the
//! process-wide infrastructure layer. It does not carry per-agent
//! specialisation (script / `system_prompt` / tools); all agent
//! specialisation belongs to `AgentDef.spec` + `AgentDef.profile`. The
//! old `default_script_path` / `default_project_root` fields were
//! removed — they were the collision source when a single process
//! hosts multiple agent.md files.
//!
//! ## Two modes (via `ScriptSource`, v0.27.0)
//!
//! | Mode | Trigger | Path |
//! |---|---|---|
//! | **PromptBasedAgent** (default) | `spec.script_path` absent | `ScriptSource::DefaultAgent` — the SDK's embedded invoker (the `agent` StdPkg module invoked with `_PROMPT` / `_CONTEXT`); event kind = `agent_result`. |
//! | **ScriptBasedAgent** | `spec.script_path = "<path>"` | `ScriptSource::Path(...)` — a caller-provided Lua script; event kind = `worker_result`. |
//!
//! `profile.system_prompt` (the agent.md body) is injected into the
//! `_CONTEXT` Lua global through `BlockConfig.context`, and applies to
//! both modes.
//!
//! ## Spec shape (`AgentDef.spec`)
//!
//! ```jsonc
//! {
//!   "project_root": "<path>",          // optional, default = std::env::current_dir()
//!   "script_path": "<path>",           // optional; absent => ScriptSource::DefaultAgent (PromptBased)
//!   "mcp_rpc_timeout_ms": 30000        // optional, default = 30s
//! }
//! ```
//!
//! ## SDK paths introduced from v0.22.0 through v0.27.0
//!
//! | Version | Feature | Use case |
//! |---|---|---|
//! | v0.22.0 | `bus.emit(kind, payload, id?)` Lua bridge | script → host event push |
//! | v0.23.0 | `BlockConfig.host_handlers` | Pre-install a Rust handler on the EventBus |
//! | v0.24.0 | `BlockConfig.auto_serve_bus` | SDK embed drives the dispatcher in the background |
//! | v0.25.0 | `BlockConfig.shutdown_token` + `BlockError::Cancelled` + `Send` on `run()` | `tokio::spawn` and external cancel |
//! | v0.26.0 | `ScriptSource` / `PromptSource` / `SecretKeySource` enums plus the embedded `DefaultAgent` invoker (breaking) | Script becomes optional at the SDK level |
//! | v0.27.0 | Embed the `compile_loop` StdPkg into core | `require("compile_loop")` hits directly |

use crate::worker::adapter::{InProcSpawner, WorkerError, WorkerInvocation, WorkerResult};
use agent_block_core::bus::dispatcher::Handler;
use agent_block_core::host::{PromptSource, ScriptSource};
use agent_block_core::{run, BlockConfig};
use agent_block_types::error::BlockError;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

/// Host-side handler that fires when the Lua script (or the
/// DefaultAgent invoker) calls `bus.emit(<kind>, payload)`. It folds
/// the payload into a [`WorkerResult`] and forwards it on the
/// [`oneshot::Sender`].
///
/// This is **an AgentBlock-internal helper**. Different SDK paths use
/// different event names and payload shapes — the DefaultAgent
/// invoker's `agent_result` event carries the entire `agent.run`
/// return value (`{content, messages, num_turns, ok, usage}`), while a
/// caller script's `worker_result` event carries `{ok, response}`. The
/// captor keeps those quirks contained and **normalises them**, so
/// callers (flow.ir, the engine, higher-level Workers) always see the
/// same single form: "the raw LLM response is `WorkerResult.value`".
///
/// Value extraction priority (the normalisation policy that hides the
/// SDK quirks):
///
/// 1. `payload.content` — from the DefaultAgent invoker / `agent.run`
///    return value; carried as a string.
/// 2. `payload.response` — the caller script's `worker_result`
///    convention; free-form.
/// 3. Fallback: the whole payload — for custom shapes that carry
///    neither of the above.
///
/// `ok` extraction: `payload.ok` if present, otherwise `true` — the
/// DefaultAgent invoker includes `ok`, so this recovers it.
///
/// This is the core of the observation #2 fix. The previous
/// implementation did not consult (1); it only fell back
/// `(2) → (3)`. On the DefaultAgent path that pushed the whole
/// `agent_result` object into `WorkerResult.value`, which then rode
/// through the chain and hit the next step's prompt via
/// JSON-stringification — burning 50-60% of the tokens on
/// boilerplate. Pulling out (1) first normalises the chain to a single
/// LLM raw-text carry and brings the Worker pattern up to the token
/// efficiency of the Phase 3 WS Operator path.
struct WorkerResultCaptor {
    tx: Mutex<Option<oneshot::Sender<WorkerResult>>>,
}

impl WorkerResultCaptor {
    /// SDK-quirks normalisation: extract `(value, ok)` from a
    /// `bus.emit` payload. `pub(crate)` so both callers and unit tests
    /// can reach it.
    fn extract(payload: &Value) -> (Value, bool) {
        let ok = payload.get("ok").and_then(|v| v.as_bool()).unwrap_or(true);
        let value = payload
            .get("content")
            .cloned()
            .or_else(|| payload.get("response").cloned())
            .unwrap_or_else(|| payload.clone());
        (value, ok)
    }
}

#[async_trait]
impl Handler for WorkerResultCaptor {
    async fn call(
        &self,
        _kind: String,
        _id: String,
        payload: Value,
        _meta: Value,
    ) -> Result<Value, BlockError> {
        let (value, ok) = Self::extract(&payload);
        let wr = WorkerResult { value, ok };
        if let Ok(mut guard) = self.tx.lock() {
            if let Some(tx) = guard.take() {
                let _ = tx.send(wr);
            }
        }
        Ok(Value::Null)
    }
}

/// Settings baked per `AgentDef` — the static portion of one
/// invocation.
///
/// v0.28.0 adopted `BlockConfig.host_handler` (a kind-agnostic
/// single sink backed by `EventBus::on_any`); the older
/// `result_event_kind: String` field (which required the caller /
/// script to coordinate a kind string) is gone. One captor per
/// invocation is enough, so a single sink is enough.
#[derive(Clone)]
struct AgentBlockSettings {
    /// Either a PromptBasedAgent — `ScriptSource::Inline` with an
    /// in-line invoker that embeds `mcp_servers` — or a
    /// ScriptBasedAgent (`ScriptSource::Path(...)`, a caller-supplied
    /// script).
    script: ScriptSource,
    project_root: PathBuf,
    mcp_rpc_timeout: Duration,
    /// Agent persona — the `system_prompt` composed from the agent.md
    /// body and frontmatter. `None` maps to `BlockConfig.context = None`
    /// for backwards compatibility with the old path.
    profile_context: Option<String>,
}

/// One invocation's worth of an `agent-block-core` SDK call — the
/// `WorkerFn` body.
///
/// Registers the result captor through the v0.28.0 `host_handler`
/// (single, kind-agnostic fallback). The plural `host_handlers`
/// (string-keyed routing) is not needed — one captor per invocation is
/// enough, and there is no script-side event-kind string to coordinate.
async fn run_agent_block_worker(
    settings: Arc<AgentBlockSettings>,
    inv: WorkerInvocation,
) -> Result<WorkerResult, WorkerError> {
    let (tx, rx) = oneshot::channel();
    let captor: Arc<dyn Handler> = Arc::new(WorkerResultCaptor {
        tx: Mutex::new(Some(tx)),
    });

    // Bridge the shutdown token: forward `WorkerInvocation.cancel_token`
    // into the SDK's `shutdown_token` if one is set; otherwise use a
    // fresh token (no external cancel).
    let shutdown_token = inv.cancel_token.clone().unwrap_or_default();
    let config = BlockConfig {
        script: settings.script.clone(),
        project_root: settings.project_root.clone(),
        relay_url: None,
        secret_key: None,
        mcp_rpc_timeout: settings.mcp_rpc_timeout,
        prompt: Some(PromptSource::Inline(inv.prompt)),
        context: settings.profile_context.clone().map(PromptSource::Inline),
        host_handlers: HashMap::new(),
        host_handler: Some(captor),
        auto_serve_bus: true,
        shutdown_token: Some(shutdown_token.clone()),
    };

    let run_handle = tokio::spawn(run(config));
    let run_result = run_handle
        .await
        .map_err(|e| WorkerError::Failed(format!("agent-block task join: {e}")))?;
    run_result.map_err(|e| WorkerError::Failed(format!("agent-block run failed: {e}")))?;

    rx.await.map_err(|_| {
        WorkerError::Failed("agent-block script finished without emitting result via bus".into())
    })
}

// ─── tools / mcp_servers resolution ───────────────────────────────────────

/// Cross-reference `profile.tools` (the CSV on the `tools:` line of an
/// agent.md frontmatter) with `spec.mcp_servers` (the `"server name" →
/// command + args` mapping provided by the `AgentDef` literal cascade)
/// and resolve the `mcp_servers` config actually exposed to the LLM
/// for this invocation.
///
/// Algorithm:
///
/// 1. Extract `mcp__<server>__<tool>` patterns from `profile.tools`;
///    collect the `<server>` names.
/// 2. Filter `spec.mcp_servers` to just the entries whose name is in
///    that set.
///
/// This is the response to observation #3 — do not hand the LLM
/// `mcp_servers` it does not need (only the servers the profile
/// explicitly asks for), and equally do not expose servers the
/// profile does not know about even if the spec carries them
/// (caller intent wins).
///
/// CC built-in tools (non-`mcp__`-prefixed names like `Read` / `Write`
/// / `WebSearch`) are out of scope here; handling those lives in a
/// different layer — a carry that would come through a future
/// `opts.extra_tools` Rust implementation.
pub fn resolve_needed_mcp_servers(
    profile_tools: &[String],
    spec_mcp_servers: &[Value],
) -> Vec<Value> {
    use std::collections::HashSet;
    // Step 1: server names from `mcp__<server>__<tool>` patterns in
    // `profile.tools`.
    let needed: HashSet<&str> = profile_tools
        .iter()
        .filter_map(|t| {
            let rest = t.strip_prefix("mcp__")?;
            // Split `<server>__<tool>` at the first `__`.
            let idx = rest.find("__")?;
            Some(&rest[..idx])
        })
        .collect();

    // Step 2: filter `spec.mcp_servers` down to entries whose name is
    // in `needed`.
    spec_mcp_servers
        .iter()
        .filter(|cfg| {
            cfg.get("name")
                .and_then(|n| n.as_str())
                .map(|name| needed.contains(name))
                .unwrap_or(false)
        })
        .cloned()
        .collect()
}

/// Build the inline Lua script used on the PromptBasedAgent path (when
/// `spec.script_path` is absent). Instead of the SDK's embedded
/// `DEFAULT_AGENT_INVOKER` (which passes no tools), this embeds
/// `mcp_servers` as a Lua literal table and hands it to `agent.run`.
///
/// This is the core of the observation #3 fix. The old DefaultAgent
/// path had no way to deliver a frontmatter `tools:` line to the SDK.
/// This inline path bakes the `profile.tools` → `mcp_servers` config
/// into the Lua source, so the LLM can actually make tool calls.
///
/// The JSON-stringify + `std.json.decode` route was ruled out because
/// the SDK environment cannot `require` the `std` module (no
/// `package.preload['std']` field), so we take the JSON → Lua-literal
/// conversion on the Rust side and embed the result directly. The
/// event name is `agent_result` — the same convention the SDK's
/// internal `DEFAULT_AGENT_INVOKER` uses.
pub fn build_inline_agent_invoker(mcp_servers: &[Value]) -> ScriptSource {
    let mcp_lua = json_array_to_lua_literal(mcp_servers);
    let source = format!(
        r##"local agent = require("agent")
local mcp_servers = {mcp_lua}
local r = agent.run({{
    prompt = _PROMPT,
    system = _CONTEXT,
    mcp_servers = mcp_servers,
}})
bus.emit("agent_result", r)
"##
    );
    ScriptSource::Inline {
        source,
        name: "mlua_swarm_engine_default_agent_invoker.lua".into(),
    }
}

/// Convert a JSON `Value` into a Lua literal expression, for embedding
/// into the inline script. Lua string escaping is delegated to Rust's
/// `{:?}` `Debug` output — Lua syntax is compatible with the escapes
/// it produces (`"`, `\\`, `\n`, `\r`, `\t`, and so on). Edge cases
/// like `\0` or unusual Unicode escapes are outside the scope of this
/// use.
fn json_to_lua_literal(v: &Value) -> String {
    match v {
        Value::Null => "nil".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("{s:?}"),
        Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(json_to_lua_literal).collect();
            format!("{{{}}}", items.join(", "))
        }
        Value::Object(map) => {
            let items: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("[{k:?}]={}", json_to_lua_literal(v)))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
    }
}

/// Convert a `Vec<Value>` into a Lua literal sequence. An empty array
/// becomes `{}` — a Lua empty table.
fn json_array_to_lua_literal(arr: &[Value]) -> String {
    if arr.is_empty() {
        return "{}".to_string();
    }
    let items: Vec<String> = arr.iter().map(json_to_lua_literal).collect();
    format!("{{{}}}", items.join(", "))
}

// ─── SpawnerFactory ───────────────────────────────────────────────────────

/// The `SpawnerFactory` for AgentBlock. `KIND = AgentKind::AgentBlock`.
///
/// **State-less.** One factory per process; every `AgentDef` uses it
/// as a shared builder. Per-agent specialisation stays **entirely
/// inside `AgentDef.spec` + `AgentDef.profile`** — the old
/// `default_script_path` / `default_project_root` fields are gone.
///
/// Naming convention: `<WorkerIMPL><AdapterType>SpawnerFactory` — an
/// AgentBlock worker on the InProcess adapter.
pub struct AgentBlockInProcessSpawnerFactory;

impl Default for AgentBlockInProcessSpawnerFactory {
    fn default() -> Self {
        Self
    }
}

impl AgentBlockInProcessSpawnerFactory {
    /// Stateless constructor — equivalent to `Default::default()`.
    pub fn new() -> Self {
        Self
    }
}

impl crate::blueprint::compiler::SpawnerFactoryKind for AgentBlockInProcessSpawnerFactory {
    const KIND: crate::blueprint::AgentKind = crate::blueprint::AgentKind::AgentBlock;
    type Worker = AgentBlockWorker;
}

impl crate::blueprint::compiler::SpawnerFactory for AgentBlockInProcessSpawnerFactory {
    fn build(
        &self,
        agent_def: &crate::blueprint::AgentDef,
        _hint: Option<&Value>,
    ) -> Result<
        Arc<dyn crate::worker::adapter::SpawnerAdapter>,
        crate::blueprint::compiler::CompileError,
    > {
        let agent_name = agent_def.name.clone();
        let spec = &agent_def.spec;

        // Resolve the actual mcp_servers config to pass to the real LLM by
        // combining profile.tools (the `tools:` line of the agent.md
        // frontmatter) with spec.mcp_servers (the first axis of AgentDef
        // literal cascade — a "server name → command + args" mapping). The
        // result is JSON-embedded into the Lua source by
        // build_inline_agent_invoker and flows into `agent.run({mcp_servers=...})`.
        let profile_tools: Vec<String> = agent_def
            .profile
            .as_ref()
            .map(|p| p.tools.clone())
            .unwrap_or_default();
        let spec_mcp_servers: Vec<Value> = spec
            .get("mcp_servers")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let needed_mcp_servers = resolve_needed_mcp_servers(&profile_tools, &spec_mcp_servers);

        // script: `spec.script_path` absent → PromptBasedAgent (the new Inline
        //         path, embedding tools and calling agent.run); present →
        //         ScriptBasedAgent (a caller-provided script path where tools
        //         are the caller's responsibility). Event-kind string
        //         dependency was retired — the `host_handler` single sink
        //         captures every kind.
        let script = match spec.get("script_path").and_then(|v| v.as_str()) {
            Some(s) => ScriptSource::Path(PathBuf::from(s)),
            None => build_inline_agent_invoker(&needed_mcp_servers),
        };

        let project_root = match spec.get("project_root").and_then(|v| v.as_str()) {
            Some(s) => PathBuf::from(s),
            None => std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        };
        let mcp_rpc_timeout = match spec.get("mcp_rpc_timeout_ms").and_then(|v| v.as_u64()) {
            Some(ms) => Duration::from_millis(ms),
            None => Duration::from_secs(30),
        };
        let profile_context = agent_def.profile.as_ref().map(|p| p.system_prompt.clone());

        let settings = Arc::new(AgentBlockSettings {
            script,
            project_root,
            mcp_rpc_timeout,
            profile_context,
        });

        let worker_fn: crate::worker::adapter::WorkerFn = Arc::new(move |inv| {
            let settings = settings.clone();
            Box::pin(run_agent_block_worker(settings, inv))
        });

        let mut sp: InProcSpawner<AgentBlockWorker> = InProcSpawner::<AgentBlockWorker>::typed();
        sp.registry.insert(agent_name, worker_fn);
        Ok(Arc::new(sp))
    }
}

/// Concrete Worker type for the AgentBlock kind — the handle for an
/// LLM call routed through the `agent-block-core` SDK. Embeds a
/// `WorkerJoinHandler` to carry the async signal. The intent is to
/// eventually keep the SDK-specific quirks — the `agent_result` event
/// name, payload shape, shutdown-token bridging, agent_result.content
/// normalisation — contained inside this struct. Today it lands as a
/// thin shape holding only the async signal; Phase B adds the
/// normalisation layer here and structurally eliminates the
/// token-boilerplate waste observed in observation #2.
pub struct AgentBlockWorker {
    /// The completion-signal handle for this agent-block SDK call's
    /// spawned task.
    pub handler: crate::worker::WorkerJoinHandler,
}

impl From<crate::worker::WorkerJoinHandler> for AgentBlockWorker {
    fn from(handler: crate::worker::WorkerJoinHandler) -> Self {
        Self { handler }
    }
}

#[async_trait]
impl crate::worker::Worker for AgentBlockWorker {
    fn id(&self) -> &crate::types::WorkerId {
        &self.handler.worker_id
    }
    fn cancel_token(&self) -> tokio_util::sync::CancellationToken {
        self.handler.cancel.clone()
    }
    async fn join(self: Box<Self>) -> Result<(), WorkerError> {
        self.handler.await_completion().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_needed_mcp_servers_filters_by_tool_prefix() {
        let tools = vec![
            "mcp__semantic-scholar__search_papers".to_string(),
            "mcp__semantic-scholar__get_paper".to_string(),
            "Read".to_string(),
            "mcp__outline__list_docs".to_string(),
            "WebSearch".to_string(),
        ];
        let spec_servers = vec![
            serde_json::json!({"name": "semantic-scholar", "command": "ss-mcp", "args": []}),
            serde_json::json!({"name": "outline", "command": "outline-mcp", "args": []}),
            serde_json::json!({"name": "unused", "command": "nope", "args": []}),
        ];
        let needed = resolve_needed_mcp_servers(&tools, &spec_servers);
        assert_eq!(needed.len(), 2, "got: {needed:?}");
        let names: Vec<&str> = needed
            .iter()
            .filter_map(|c| c.get("name").and_then(|n| n.as_str()))
            .collect();
        assert!(names.contains(&"semantic-scholar"));
        assert!(names.contains(&"outline"));
        assert!(!names.contains(&"unused"), "unused server is filtered out");
    }

    #[test]
    fn resolve_needed_mcp_servers_returns_empty_when_no_mcp_tools() {
        let tools = vec!["Read".to_string(), "WebSearch".to_string()];
        let spec_servers =
            vec![serde_json::json!({"name": "outline", "command": "outline-mcp", "args": []})];
        let needed = resolve_needed_mcp_servers(&tools, &spec_servers);
        assert!(
            needed.is_empty(),
            "no mcp__-prefixed tools → empty result, got: {needed:?}"
        );
    }

    #[test]
    fn build_inline_agent_invoker_embeds_mcp_servers_as_lua_literal() {
        let servers =
            vec![serde_json::json!({"name": "outline", "command": "outline-mcp", "args": []})];
        let script = build_inline_agent_invoker(&servers);
        match script {
            ScriptSource::Inline { source, name } => {
                assert!(name.ends_with(".lua"));
                assert!(source.contains("require(\"agent\")"));
                assert!(source.contains("mcp_servers = mcp_servers"));
                assert!(source.contains("bus.emit(\"agent_result\""));
                // Lua literal embed (= keys [\"name\"]=\"outline\" form)
                assert!(source.contains("[\"name\"]=\"outline\""));
                assert!(source.contains("[\"command\"]=\"outline-mcp\""));
                assert!(source.contains("[\"args\"]={}"), "args empty array literal");
            }
            other => panic!("expected Inline, got: {other:?}"),
        }
    }

    #[test]
    fn build_inline_agent_invoker_with_empty_servers_still_valid() {
        let script = build_inline_agent_invoker(&[]);
        match script {
            ScriptSource::Inline { source, .. } => {
                assert!(source.contains("local mcp_servers = {}"));
            }
            other => panic!("expected Inline, got: {other:?}"),
        }
    }

    #[test]
    fn json_to_lua_literal_handles_primitives_and_nested() {
        assert_eq!(json_to_lua_literal(&serde_json::json!(null)), "nil");
        assert_eq!(json_to_lua_literal(&serde_json::json!(true)), "true");
        assert_eq!(json_to_lua_literal(&serde_json::json!(42)), "42");
        assert_eq!(json_to_lua_literal(&serde_json::json!("hi")), "\"hi\"");
        assert_eq!(
            json_to_lua_literal(&serde_json::json!(["a", "b"])),
            "{\"a\", \"b\"}"
        );
        assert_eq!(
            json_to_lua_literal(&serde_json::json!({"k": 1})),
            "{[\"k\"]=1}"
        );
    }

    #[test]
    fn extract_prefers_content_then_response_then_whole() {
        // (1) `content` takes priority (DefaultAgent invoker / agent.run return-value path).
        let p = serde_json::json!({
            "content": "Water boils at 100°C",
            "messages": [{"role": "assistant"}],
            "usage": {"input_tokens": 67, "output_tokens": 29},
            "ok": true,
        });
        let (value, ok) = WorkerResultCaptor::extract(&p);
        assert_eq!(value, serde_json::json!("Water boils at 100°C"));
        assert!(ok);

        // (2) No `content` → `response` (caller-script convention worker_result).
        let p = serde_json::json!({ "ok": false, "response": {"patch": "..."} });
        let (value, ok) = WorkerResultCaptor::extract(&p);
        assert_eq!(value, serde_json::json!({"patch": "..."}));
        assert!(!ok);

        // (3) Neither present → the whole payload (custom shape).
        let p = serde_json::json!({ "custom_field": 42 });
        let (value, ok) = WorkerResultCaptor::extract(&p);
        assert_eq!(value, serde_json::json!({"custom_field": 42}));
        assert!(ok); // `ok` absent → defaults to true
    }

    #[tokio::test]
    async fn captor_emits_worker_result_from_payload() {
        let (tx, rx) = oneshot::channel();
        let captor = WorkerResultCaptor {
            tx: Mutex::new(Some(tx)),
        };
        let payload = serde_json::json!({ "ok": true, "response": "hello" });
        let ack = captor
            .call("worker_result".into(), "evt-1".into(), payload, Value::Null)
            .await
            .expect("handler ack");
        assert_eq!(ack, Value::Null);
        let wr = rx.await.expect("recv");
        assert!(wr.ok);
        assert_eq!(wr.value, serde_json::json!("hello"));
    }

    #[tokio::test]
    async fn factory_builds_prompt_based_agent_when_script_path_absent() {
        use crate::blueprint::compiler::SpawnerFactory;
        use crate::blueprint::{AgentDef, AgentKind, AgentProfile};

        let factory = AgentBlockInProcessSpawnerFactory::new();
        let ad = AgentDef {
            name: "writer".into(),
            kind: AgentKind::AgentBlock,
            spec: serde_json::json!({}),
            profile: Some(AgentProfile {
                system_prompt: "You are writer.".into(),
                ..Default::default()
            }),
            meta: None,
        };
        let _spawner = factory.build(&ad, None).expect("factory build");
        // = ScriptSource::Inline path (self-hosted invoker, mcp_servers embed);
        // the host_handler single sink captures every event kind.
    }

    #[tokio::test]
    async fn factory_builds_script_based_agent_when_script_path_present() {
        use crate::blueprint::compiler::SpawnerFactory;
        use crate::blueprint::{AgentDef, AgentKind, AgentProfile};

        let factory = AgentBlockInProcessSpawnerFactory::new();
        let ad = AgentDef {
            name: "patch-spawner".into(),
            kind: AgentKind::AgentBlock,
            spec: serde_json::json!({
                "script_path": "assets/operator_scripts/blueprint_patch_spawner.lua",
                "project_root": ".",
            }),
            profile: Some(AgentProfile {
                system_prompt: "Patch generator.".into(),
                ..Default::default()
            }),
            meta: None,
        };
        let _spawner = factory.build(&ad, None).expect("factory build");
        // = ScriptSource::Path path; caller-provided script; host_handler single sink.
    }
}
