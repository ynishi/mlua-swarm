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
//! This is the settled `spec` contract for `AgentKind::AgentBlock` (GH
//! #86) — every key is optional, and every key the factory reads is
//! listed here:
//!
//! ```jsonc
//! {
//!   "project_root": "<path>",          // optional, default = std::env::current_dir()
//!   "script_path": "<path>",           // optional; absent => PromptBasedAgent mode
//!   "mcp_rpc_timeout_ms": 30000,       // optional, default = 30s
//!   "mcp_servers": [                   // optional; the pool the tool grant selects from
//!     { "name": "outline", "command": "outline-mcp", "args": [] }
//!   ]
//! }
//! ```
//!
//! ## Tool grant (GH #86)
//!
//! The factory reads the effective tool set off `profile.tools` — which
//! is **already** the resolved `Runner::AgentBlockInProcess.tools`
//! whenever the agent declares that Runner, because
//! `compiler::project_bound_agent_for_legacy_factories` overwrites
//! `profile.tools` from the immutable `BoundAgent` snapshot (including
//! with an empty list, so a Blueprint can revoke an agent.md's inherited
//! `tools:` line). No Runner declared → the agent.md line stands. There
//! is deliberately no build hint for this axis: re-deriving the Runner
//! here would bypass the pinned snapshot and let a `Blueprint.runners`
//! edit change an in-flight Run's grant on resume.
//!
//! Enforcement is per mode:
//!
//! | Mode | Enforcement |
//! |---|---|
//! | PromptBasedAgent | Enforced at **server** granularity. Only the `spec.mcp_servers` entries named by an `mcp__<server>__<tool>` entry of the effective set are embedded into the invoker ([`resolve_needed_mcp_servers`]), so the LLM cannot reach an unlisted server — but it CAN reach every tool of a listed one (the SDK exposes a connected server's full tool list). Grant per server, not per tool. |
//! | ScriptBasedAgent | Not enforceable — the script drives its own `mcp.connect`. Declared `mcp__` entries are therefore **rejected at compile time** rather than silently ignored; drop them and let the script own its connections. |
//!
//! Non-`mcp__`-prefixed names (`Read` / `Write` / `WebSearch`) do not
//! select an MCP server and are inert in both modes (see
//! [`mcp_tools_of`]) — the `opts.extra_tools` carry noted on
//! [`resolve_needed_mcp_servers`].
//!
//! ## Per-task input and result (GH #86)
//!
//! Task context reaches this backend through **one seam**:
//! [`WorkerInvocation::context`], the in-process twin of
//! `WorkerPayload.context`, filled once by `InProcSpawner::spawn` from the
//! materialized [`AgentContextView`]. Nothing here peeks at `Ctx`
//! directly, and no `SpawnerAdapter` wrapper re-resolves it — the three
//! Lua-visible surfaces below are all derived from that one value:
//!
//! | Lua surface | Source |
//! |---|---|
//! | `_PROMPT` | The step's evaluated `in`, via `inv.prompt` → `BlockConfig.prompt`. A **String** — a structured `in` arrives JSON-stringified, so a script that wants a table calls `std.json.decode(_PROMPT)`. |
//! | `_CONTEXT` | `profile.system_prompt`, via `BlockConfig.context`. |
//! | `_TASK_METADATA` | `view.task_metadata` (the launch's `init_ctx.task_metadata` bag), embedded as a Lua literal by [`build_prelude`]. |
//!
//! No server-process env is involved in any of them. The per-task working
//! directory is not a Lua global — it becomes the SDK's `project_root`
//! (see the next section), which surfaces to a script as
//! `std.env.project_root()` and as the default cwd of `sh.exec` and of
//! MCP servers spawned by `mcp.connect`. It does NOT `chdir` the host
//! process, so a bare `io.open("rel/path")` still resolves against the
//! server's own cwd.
//!
//! A script returns its result by calling `bus.emit(<kind>, payload)` —
//! **not** by returning a value from the chunk — and
//! [`WorkerResultCaptor`] normalises the payload into
//! [`WorkerResult`]`.value` (`payload.content` → `payload.response` →
//! the whole payload). Only the FIRST emit is taken. For a
//! `VerdictChannel::Body` contract, that value IS the verdict scalar the
//! engine compares. `VerdictChannel::Part` is NOT reachable from this
//! backend today: staging a named part needs `WorkerInvocation.sink`,
//! which this worker does not yet bridge to Lua.
//!
//! ## `project_root` resolution (issue #17, GH #20)
//!
//! `spec.project_root` (above) is only the **compile-time fallback**
//! tier — resolved once in [`AgentBlockInProcessSpawnerFactory::build`],
//! before any `Ctx` exists. Per invocation, [`resolve_project_root`]
//! applies the task-context tier off [`WorkerInvocation::context`] (GH
//! #20 Contract C — see [`crate::core::agent_context`] for the full
//! narrative) with this priority (highest first):
//!
//! 1. `view.work_dir` — Task-level, set by `TaskInputMiddleware` from
//!    the launch's `init_ctx.work_dir`.
//! 2. `view.project_root` — same middleware, `init_ctx.project_root`.
//! 3. `spec.project_root` / `std::env::current_dir()` (the compile-time
//!    fallback baked into [`AgentBlockSettings`] above).
//!
//! This lets a single Blueprint's `AgentDef.spec.project_root` (fixed at
//! compile time) be overridden per task launch, so the same Blueprint
//! can run against different caller-supplied project roots without a
//! `spec` edit.
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

use crate::core::agent_context::AgentContextView;
use crate::worker::adapter::{InProcSpawner, WorkerError, WorkerInvocation, WorkerResult};
use agent_block_core::bus::dispatcher::Handler;
use agent_block_core::host::{PromptSource, ScriptSource};
use agent_block_core::{run, BlockConfig};
use agent_block_types::error::BlockError;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
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

    /// Stats-sidecar extraction (per-step run stats): the DefaultAgent
    /// invoker's `agent_result` payload carries the full `agent.run`
    /// return, whose `usage` (`{input_tokens, output_tokens,
    /// total_tokens}`, all turns summed) and `num_turns` used to be
    /// DROPPED here — the exact gap this recovers. `None` when the
    /// payload carries neither (caller-script `worker_result` shapes).
    /// The raw `usage` object also rides as `adapter_data` so
    /// provider-specific detail (cache tokens etc.) survives.
    fn extract_stats(payload: &Value) -> Option<crate::store::trace::WorkerStats> {
        let usage_raw = payload.get("usage");
        let usage = usage_raw.and_then(|u| {
            let input = u.get("input_tokens").and_then(|v| v.as_u64());
            let output = u.get("output_tokens").and_then(|v| v.as_u64());
            match (input, output) {
                (Some(i), Some(o)) => Some(crate::store::trace::TokenUsage {
                    input_tokens: i,
                    output_tokens: o,
                    total_tokens: u
                        .get("total_tokens")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(i + o),
                }),
                _ => None,
            }
        });
        let num_turns = payload
            .get("num_turns")
            .and_then(|v| v.as_u64())
            .map(|n| n as u32);
        if usage.is_none() && num_turns.is_none() {
            return None;
        }
        Some(crate::store::trace::WorkerStats {
            worker_kind: Some("agent_block".to_string()),
            model: None,
            usage,
            num_turns,
            adapter_data: usage_raw.cloned(),
        })
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
        let stats = Self::extract_stats(&payload);
        // Even when the SDK payload carries no usage (script-side
        // `worker_result` shapes), the boundary still knows its own
        // kind — surface it so `StepEntry.worker_kind` is never empty.
        let wr = WorkerResult { value, ok, stats }.ensure_worker_kind("agent_block");
        if let Ok(mut guard) = self.tx.lock() {
            if let Some(tx) = guard.take() {
                let _ = tx.send(wr);
            }
        }
        Ok(Value::Null)
    }
}

/// Which Lua chunk this `AgentDef` runs, deferred so the per-invocation
/// prelude (see [`build_prelude`]) can be spliced in at dispatch time
/// rather than baked at compile time.
#[derive(Clone, Debug)]
enum ScriptPlan {
    /// ScriptBasedAgent (`spec.script_path` present) — a caller-supplied
    /// script, read at invocation time so its on-disk content is never
    /// pinned to compile time.
    CallerScript(PathBuf),
    /// PromptBasedAgent (`spec.script_path` absent) — the host-generated
    /// invoker built by [`build_inline_agent_invoker`].
    Invoker { source: String, name: String },
}

impl ScriptPlan {
    /// The directory whose `package.path` entry the prelude must restore
    /// — `Some` only for [`Self::CallerScript`], since that is the sole
    /// variant whose `script_dir` changes when injection forces the
    /// Inline route (see [`build_prelude`]).
    fn caller_script_dir(&self) -> Option<&Path> {
        match self {
            ScriptPlan::CallerScript(path) => path.parent(),
            ScriptPlan::Invoker { .. } => None,
        }
    }

    /// Resolve to the SDK's [`ScriptSource`] for one invocation, splicing
    /// `prelude` in front of the chunk body.
    ///
    /// With an empty prelude a [`Self::CallerScript`] stays
    /// `ScriptSource::Path` — byte-for-byte the pre-GH-#86 path, with no
    /// host-side read and no line-number shift. A non-empty prelude forces
    /// the read-and-inline route, because `ScriptSource::Path` gives the
    /// host nowhere to inject.
    fn resolve(&self, prelude: &str) -> Result<ScriptSource, WorkerError> {
        match self {
            ScriptPlan::CallerScript(path) if prelude.is_empty() => {
                Ok(ScriptSource::Path(path.clone()))
            }
            ScriptPlan::CallerScript(path) => {
                let body = std::fs::read_to_string(path).map_err(|e| {
                    WorkerError::Failed(format!("agent-block script {}: {e}", path.display()))
                })?;
                Ok(ScriptSource::Inline {
                    source: format!("{prelude}{body}"),
                    // Keep the on-disk file name so `_SCRIPT_NAME`, tracing
                    // attribution, and error messages still point at the
                    // author's file rather than at a synthetic chunk.
                    name: path
                        .file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_else(|| "gate.lua".to_string()),
                })
            }
            ScriptPlan::Invoker { source, name } => Ok(ScriptSource::Inline {
                source: format!("{prelude}{source}"),
                name: name.clone(),
            }),
        }
    }
}

/// Build the one-line prelude spliced in front of the chunk body, or an
/// empty string when there is nothing to inject.
///
/// Deliberately **exactly one line** (a single trailing `\n`, no interior
/// newlines) so the line numbers a script author sees in a Lua stack trace
/// are off by a constant `+1` rather than by an amount that varies with
/// what got injected.
///
/// Two things go in:
///
/// 1. `_TASK_METADATA` — the launch's `init_ctx.task_metadata` bag as a Lua
///    literal. This is the delivery point the in-process lane previously
///    lacked entirely (the WS Operator lane has always received it, as a
///    `task_metadata:` line of the Spawn directive header via
///    `AgentContextView::to_directive_header`).
/// 2. A `package.path` restoration, for `script_dir` only. The SDK derives
///    `script_dir` from `ScriptSource::Path(p)` as `p.parent()` but from
///    `ScriptSource::Inline` as `project_root`, and puts it at the FRONT of
///    `package.path`. Since injecting forces the Inline route, a caller
///    script that `require`s a sibling module would otherwise stop
///    resolving it; re-prepending its own directory keeps that working.
///
/// # Trust boundary
///
/// `task_metadata` is **caller-supplied at launch time**, and this splices
/// it into a chunk that is then executed — so [`json_to_lua_literal`]'s
/// escaping is a security boundary here, not just formatting. It is what
/// keeps a hostile value (an unbalanced quote / brace, a `--[[` comment
/// opener, an embedded newline) inert string data instead of executable
/// Lua. `prelude_escaping_contains_a_hostile_task_metadata_payload`
/// asserts that through a real Lua VM; keep it passing before widening
/// what gets embedded here.
fn build_prelude(task_metadata: Option<&Value>, script_dir: Option<&Path>) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(dir) = script_dir {
        let dir = dir.to_string_lossy();
        let patterns =
            json_to_lua_literal(&Value::String(format!("{dir}/?.lua;{dir}/?/init.lua;")));
        parts.push(format!("package.path={patterns}..package.path"));
    }
    if let Some(meta) = task_metadata {
        parts.push(format!("_TASK_METADATA={}", json_to_lua_literal(meta)));
    }
    if parts.is_empty() {
        return String::new();
    }
    format!("{};\n", parts.join("; "))
}

/// Settings baked per `AgentDef` — the static portion of one
/// invocation. Everything task-dependent (`project_root` /
/// `task_metadata`) is resolved per invocation off
/// [`WorkerInvocation::context`] instead, so this struct is built once at
/// compile time and shared by every dispatch of the agent.
///
/// v0.28.0 adopted `BlockConfig.host_handler` (a kind-agnostic
/// single sink backed by `EventBus::on_any`); the older
/// `result_event_kind: String` field (which required the caller /
/// script to coordinate a kind string) is gone. One captor per
/// invocation is enough, so a single sink is enough.
#[derive(Clone)]
struct AgentBlockSettings {
    /// The chunk to run — see [`ScriptPlan`].
    script: ScriptPlan,
    /// Compile-time fallback cwd: `spec.project_root`, else
    /// `env::current_dir()`. Outranked per invocation by the context
    /// view's `work_dir` / `project_root`.
    spec_project_root: PathBuf,
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

    // GH #86: the task-context tier, read off the ONE in-process seam
    // (`WorkerInvocation.context`, filled by `InProcSpawner::spawn` from
    // the materialized `AgentContextView`) instead of a hand-rolled `Ctx`
    // peek in a spawner wrapper.
    let project_root = resolve_project_root(inv.context.as_ref(), &settings.spec_project_root);
    let prelude = build_prelude(
        inv.context.as_ref().and_then(|v| v.task_metadata.as_ref()),
        settings.script.caller_script_dir(),
    );
    let script = settings.script.resolve(&prelude)?;

    // Bridge the shutdown token: forward `WorkerInvocation.cancel_token`
    // into the SDK's `shutdown_token` if one is set; otherwise use a
    // fresh token (no external cancel).
    let shutdown_token = inv.cancel_token.clone().unwrap_or_default();
    let config = BlockConfig {
        script,
        project_root,
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

/// Cross-reference the agent's declared tool set (see
/// [`resolve_effective_tools`] for which tier that comes from) with
/// `spec.mcp_servers` (the `"server name" → command + args` mapping
/// provided by the `AgentDef` literal cascade) and resolve the
/// `mcp_servers` config actually exposed to the LLM for this invocation.
///
/// Algorithm:
///
/// 1. Extract `mcp__<server>__<tool>` patterns from `declared_tools`;
///    collect the `<server>` names.
/// 2. Filter `spec.mcp_servers` to just the entries whose name is in
///    that set.
///
/// This is the response to observation #3 — do not hand the LLM
/// `mcp_servers` it does not need (only the servers the declaration
/// explicitly asks for), and equally do not expose servers the
/// declaration does not know about even if the spec carries them
/// (caller intent wins).
///
/// CC built-in tools (non-`mcp__`-prefixed names like `Read` / `Write`
/// / `WebSearch`) are out of scope here; handling those lives in a
/// different layer — a carry that would come through a future
/// `opts.extra_tools` Rust implementation.
pub fn resolve_needed_mcp_servers(
    declared_tools: &[String],
    spec_mcp_servers: &[Value],
) -> Vec<Value> {
    use std::collections::HashSet;
    // Step 1: server names from `mcp__<server>__<tool>` patterns in the
    // declared tool set.
    let needed: HashSet<&str> = declared_tools
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

/// GH #86 — the subset of an effective tool set that names an MCP server,
/// i.e. the only entries this backend's grant model can act on.
///
/// Everything else (`Read` / `Write` / `WebSearch` …) selects no server and
/// is inert here, so it is neither embedded nor treated as a grant that
/// must be honored — see [`resolve_needed_mcp_servers`]'s `opts.extra_tools`
/// carry. Used by the ScriptBasedAgent guard in
/// [`AgentBlockInProcessSpawnerFactory::build`], which must not fail an
/// agent whose declared tools are all inert.
fn mcp_tools_of(tools: &[String]) -> Vec<&str> {
    tools
        .iter()
        .filter(|t| t.starts_with("mcp__"))
        .map(String::as_str)
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
fn build_inline_agent_invoker(mcp_servers: &[Value]) -> ScriptPlan {
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
    ScriptPlan::Invoker {
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

/// The compile-time (`spec` / `env::current_dir()`) fallback tier of the
/// `project_root` priority chain (issue #17) — the tail two links of
/// **`ctx.meta.runtime` `work_dir` > `ctx.meta.runtime` `project_root` >
/// `spec.project_root` > `env::current_dir()`**. Extracted as a standalone
/// pure fn so it is independently testable without needing a full `Ctx` /
/// `SpawnerAdapter` round-trip.
fn resolve_spec_project_root(spec: &Value) -> PathBuf {
    match spec.get("project_root").and_then(|v| v.as_str()) {
        Some(s) => PathBuf::from(s),
        None => std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
    }
}

/// Apply the task-context tier on top of the compile-time fallback:
/// **`view.work_dir` > `view.project_root` > `spec_fallback`**.
///
/// `work_dir` outranks `project_root` because it names the exact directory
/// this specific worker should run from. A `None` view (no `Ctx` on the
/// caller path) leaves the compile-time fallback in place.
fn resolve_project_root(view: Option<&AgentContextView>, spec_fallback: &Path) -> PathBuf {
    view.and_then(|v| v.work_dir.as_deref().or(v.project_root.as_deref()))
        .map(PathBuf::from)
        .unwrap_or_else(|| spec_fallback.to_path_buf())
}

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
        // combining the effective tool set with spec.mcp_servers (the first
        // axis of AgentDef literal cascade — a "server name → command +
        // args" mapping). The result is JSON-embedded into the Lua source by
        // build_inline_agent_invoker and flows into
        // `agent.run({mcp_servers=...})`.
        //
        // `profile.tools` IS the effective set: when the agent declares a
        // `Runner::AgentBlockInProcess`, the compiler has already overwritten
        // `profile.tools` with that Runner's `tools` off the pinned
        // `BoundAgent` snapshot (`project_bound_agent_for_legacy_factories`),
        // including with an empty list. No Runner declared → the agent.md
        // `tools:` line stands as-is. See the module doc's "Tool grant".
        let effective_tools: Vec<String> = agent_def
            .profile
            .as_ref()
            .map(|p| p.tools.clone())
            .unwrap_or_default();
        let spec_mcp_servers: Vec<Value> = spec
            .get("mcp_servers")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let needed_mcp_servers = resolve_needed_mcp_servers(&effective_tools, &spec_mcp_servers);

        // script: `spec.script_path` absent → PromptBasedAgent (the new Inline
        //         path, embedding tools and calling agent.run); present →
        //         ScriptBasedAgent (a caller-provided script path where tools
        //         are the caller's responsibility). Event-kind string
        //         dependency was retired — the `host_handler` single sink
        //         captures every kind.
        let script = match spec.get("script_path").and_then(|v| v.as_str()) {
            Some(s) => {
                // GH #86: a caller script drives its own `mcp.connect`, so the
                // host has no choke point to enforce an MCP grant through —
                // the Inline invoker's "embed exactly the declared servers"
                // lever does not exist on this path. Declaring MCP tools here
                // would be a promise the runtime cannot keep, so it is
                // rejected at compile time instead of silently ignored.
                //
                // Only `mcp__`-prefixed entries trigger this: everything else
                // selects no server and is inert in both modes, so an agent.md
                // `tools: Read, WebSearch` line must not fail a script-mode
                // agent that compiled before this guard existed.
                let mcp_tools = mcp_tools_of(&effective_tools);
                if !mcp_tools.is_empty() {
                    return Err(crate::blueprint::compiler::CompileError::InvalidSpec {
                        name: agent_name,
                        msg: format!(
                            "agent_block ScriptBasedAgent mode (spec.script_path = {s:?}) cannot \
                             enforce an MCP tool grant: the script opens its own connections via \
                             `mcp.connect`, so the declared tools ({}) would be unenforceable. \
                             Either drop spec.script_path to use PromptBasedAgent mode (where the \
                             declared servers ARE the only ones embedded into the invoker), or \
                             drop the mcp__ entries and let the script own its connections.",
                            mcp_tools.join(", ")
                        ),
                    });
                }
                ScriptPlan::CallerScript(PathBuf::from(s))
            }
            None => build_inline_agent_invoker(&needed_mcp_servers),
        };

        // issue #17: this is the compile-time fallback tier only —
        // `spec.project_root`, then `env::current_dir()`. No `Ctx` exists
        // yet at `build()` time, so the higher-priority task-context tier
        // cannot be consulted here; `run_agent_block_worker` applies it per
        // invocation off `WorkerInvocation.context` (see the module-level
        // "`project_root` resolution" doc).
        let spec_project_root = resolve_spec_project_root(spec);
        let mcp_rpc_timeout = match spec.get("mcp_rpc_timeout_ms").and_then(|v| v.as_u64()) {
            Some(ms) => Duration::from_millis(ms),
            None => Duration::from_secs(30),
        };
        let profile_context = agent_def.profile.as_ref().map(|p| p.system_prompt.clone());

        let settings = Arc::new(AgentBlockSettings {
            script,
            spec_project_root,
            mcp_rpc_timeout,
            profile_context,
        });

        // A plain `InProcSpawner` with this agent's single route. GH #86
        // removed the `AgentBlockCtxAwareSpawner` wrapper that used to sit
        // here purely to re-resolve `ctx.meta.runtime` at spawn time: the
        // task-context tier now arrives on `WorkerInvocation.context`, the
        // same seam every other in-process worker reads, so the worker fn
        // resolves it itself and no bespoke adapter is needed.
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
    use crate::core::agent_context::{TASK_PROJECT_ROOT_KEY, TASK_WORK_DIR_KEY};

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
            ScriptPlan::Invoker { source, name } => {
                assert!(name.ends_with(".lua"));
                assert!(source.contains("require(\"agent\")"));
                assert!(source.contains("mcp_servers = mcp_servers"));
                assert!(source.contains("bus.emit(\"agent_result\""));
                // Lua literal embed (= keys [\"name\"]=\"outline\" form)
                assert!(source.contains("[\"name\"]=\"outline\""));
                assert!(source.contains("[\"command\"]=\"outline-mcp\""));
                assert!(source.contains("[\"args\"]={}"), "args empty array literal");
            }
            other => panic!("expected Invoker, got: {other:?}"),
        }
    }

    #[test]
    fn build_inline_agent_invoker_with_empty_servers_still_valid() {
        let script = build_inline_agent_invoker(&[]);
        match script {
            ScriptPlan::Invoker { source, .. } => {
                assert!(source.contains("local mcp_servers = {}"));
            }
            other => panic!("expected Invoker, got: {other:?}"),
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
            runner: None,
            runner_ref: None,
            verdict: None,
        };
        let _spawner = factory.build(&ad, None).expect("factory build");
        // = ScriptSource::Inline path (self-hosted invoker, mcp_servers embed);
        // the host_handler single sink captures every event kind.
    }

    // ─── GH #86: effective tool grant ─────────────────────────────────────

    fn agent_block_def(name: &str, spec: Value, tools: &[&str]) -> crate::blueprint::AgentDef {
        use crate::blueprint::{AgentDef, AgentKind, AgentProfile};
        AgentDef {
            name: name.into(),
            kind: AgentKind::AgentBlock,
            spec,
            profile: Some(AgentProfile {
                system_prompt: "You are an auditor.".into(),
                tools: tools.iter().map(|t| t.to_string()).collect(),
                ..Default::default()
            }),
            meta: None,
            runner: None,
            runner_ref: None,
            verdict: None,
        }
    }

    #[test]
    fn mcp_tools_of_keeps_only_server_selecting_names() {
        let tools = vec![
            "Read".to_string(),
            "mcp__outline__list_docs".to_string(),
            "WebSearch".to_string(),
        ];
        assert_eq!(mcp_tools_of(&tools), vec!["mcp__outline__list_docs"]);
        assert!(mcp_tools_of(&["Read".to_string()]).is_empty());
    }

    /// PromptBasedAgent mode is where the grant is enforced: only the
    /// `spec.mcp_servers` entries named by the effective set (=
    /// `profile.tools`, which the compiler has already overwritten from a
    /// declared Runner) reach the invoker.
    ///
    /// Enforcement is per **server**, not per tool — granting
    /// `mcp__outline__list_docs` embeds the whole `outline` server, and the
    /// SDK exposes every tool of a connected server to the model.
    #[tokio::test]
    async fn effective_grant_narrows_the_embedded_mcp_servers() {
        use crate::blueprint::compiler::SpawnerFactory;

        let ad = agent_block_def(
            "auditor",
            serde_json::json!({
                "mcp_servers": [
                    {"name": "outline", "command": "outline-mcp", "args": []},
                    {"name": "semantic-scholar", "command": "ss-mcp", "args": []},
                ]
            }),
            &["mcp__outline__list_docs"],
        );

        // The pure resolution the factory performs, asserted directly (the
        // built `Arc<dyn SpawnerAdapter>` is opaque).
        let effective = ad.profile.as_ref().unwrap().tools.clone();
        let servers = resolve_needed_mcp_servers(
            &effective,
            ad.spec["mcp_servers"].as_array().expect("array"),
        );
        let names: Vec<&str> = servers
            .iter()
            .filter_map(|c| c.get("name").and_then(|n| n.as_str()))
            .collect();
        assert_eq!(
            names,
            vec!["outline"],
            "semantic-scholar is declared in spec but not selected by the grant"
        );

        // And the build itself succeeds on this (PromptBased) path.
        AgentBlockInProcessSpawnerFactory::new()
            .build(&ad, None)
            .expect("PromptBasedAgent mode accepts an MCP grant");
    }

    /// ScriptBasedAgent mode cannot enforce an MCP grant (the script drives
    /// its own `mcp.connect`), so declared `mcp__` entries are rejected
    /// rather than silently ignored.
    #[tokio::test]
    async fn script_mode_rejects_a_declared_mcp_grant() {
        use crate::blueprint::compiler::{CompileError, SpawnerFactory};

        let ad = agent_block_def(
            "gate-danger",
            serde_json::json!({ "script_path": "gate.lua" }),
            &["mcp__outline__list_docs"],
        );
        let err = AgentBlockInProcessSpawnerFactory::new()
            .build(&ad, None)
            .err()
            .expect("must reject");
        match err {
            CompileError::InvalidSpec { name, msg } => {
                assert_eq!(name, "gate-danger");
                assert!(msg.contains("mcp.connect"), "explains why: {msg}");
                assert!(
                    msg.contains("PromptBasedAgent"),
                    "names the actionable alternative: {msg}"
                );
            }
            other => panic!("expected InvalidSpec, got: {other:?}"),
        }
    }

    /// The guard is scoped to `mcp__` entries: an empty grant (the issue's
    /// own repro BP) and an inert-only grant (an agent.md `tools: Read,
    /// WebSearch` line, which compiled before the guard existed) both still
    /// build in script mode.
    #[tokio::test]
    async fn script_mode_accepts_empty_and_inert_grants() {
        use crate::blueprint::compiler::SpawnerFactory;

        let spec = serde_json::json!({ "script_path": "gate.lua" });
        for tools in [&[][..], &["Read", "WebSearch"][..]] {
            let ad = agent_block_def("gate-danger", spec.clone(), tools);
            AgentBlockInProcessSpawnerFactory::new()
                .build(&ad, None)
                .unwrap_or_else(|e| panic!("script mode must accept tools {tools:?}: {e}"));
        }
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
            runner: None,
            runner_ref: None,
            verdict: None,
        };
        let _spawner = factory.build(&ad, None).expect("factory build");
        // = ScriptSource::Path path; caller-provided script; host_handler single sink.
    }

    // ─── Issue #17: `project_root` priority chain ─────────────────────────

    #[test]
    fn resolve_spec_project_root_uses_spec_value_when_present() {
        let resolved =
            resolve_spec_project_root(&serde_json::json!({ "project_root": "/spec-root" }));
        assert_eq!(resolved, PathBuf::from("/spec-root"));
    }

    #[test]
    fn resolve_spec_project_root_falls_back_to_env_current_dir_when_spec_absent() {
        let resolved = resolve_spec_project_root(&serde_json::json!({}));
        assert_eq!(
            resolved,
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
        );
    }

    /// A view carrying exactly the task-context fields under test. Built
    /// through the real `AgentContextView::from_ctx` so the field names
    /// stay bound to the canonical `ctx.meta.runtime` keys rather than to
    /// a hand-written literal that could drift from them.
    fn view_with(pairs: &[(&str, Value)]) -> AgentContextView {
        let mut ctx = crate::core::ctx::Ctx::new(
            crate::types::StepId::parse("ST-project-root").unwrap(),
            1,
            "writer",
        );
        for (k, v) in pairs {
            ctx.meta.runtime.insert((*k).to_string(), v.clone());
        }
        AgentContextView::from_ctx(&ctx)
    }

    // ─── project_root priority chain (issue #17, now off the seam) ────────

    #[test]
    fn project_root_falls_back_to_spec_when_the_view_carries_neither() {
        let view = view_with(&[]);
        let resolved = resolve_project_root(Some(&view), Path::new("/spec-root"));
        assert_eq!(resolved, PathBuf::from("/spec-root"));
    }

    /// No `Ctx` on the caller path at all (`inv.context == None`) is the
    /// same outcome as an empty view — the compile-time fallback stands.
    #[test]
    fn project_root_falls_back_to_spec_without_a_view() {
        assert_eq!(
            resolve_project_root(None, Path::new("/spec-root")),
            PathBuf::from("/spec-root")
        );
    }

    #[test]
    fn project_root_prefers_the_view_over_spec() {
        let view = view_with(&[(TASK_PROJECT_ROOT_KEY, serde_json::json!("/ctx-root"))]);
        let resolved = resolve_project_root(Some(&view), Path::new("/spec-root"));
        assert_eq!(resolved, PathBuf::from("/ctx-root"));
    }

    #[test]
    fn project_root_prefers_work_dir_over_project_root() {
        let view = view_with(&[
            (TASK_PROJECT_ROOT_KEY, serde_json::json!("/ctx-root")),
            (TASK_WORK_DIR_KEY, serde_json::json!("/ctx-work")),
        ]);
        let resolved = resolve_project_root(Some(&view), Path::new("/spec-root"));
        assert_eq!(resolved, PathBuf::from("/ctx-work"));
    }

    // ─── GH #86: task_metadata delivery via the generated prelude ─────────

    /// Nothing to inject → empty prelude, which is what keeps a caller
    /// script on the untouched `ScriptSource::Path` route.
    #[test]
    fn prelude_is_empty_when_there_is_nothing_to_inject() {
        assert_eq!(build_prelude(None, None), "");
    }

    #[test]
    fn prelude_embeds_task_metadata_as_a_lua_literal() {
        let meta = serde_json::json!({"issue": 20});
        let prelude = build_prelude(Some(&meta), None);
        assert_eq!(prelude, "_TASK_METADATA={[\"issue\"]=20};\n");
    }

    /// Exactly one line, always — the invariant that keeps a script
    /// author's Lua stack-trace line numbers off by a constant `+1`
    /// instead of by an amount that varies with what got injected.
    #[test]
    fn prelude_is_always_exactly_one_line() {
        let meta = serde_json::json!({"a": 1, "b": [2, 3], "c": "x\ny"});
        for prelude in [
            build_prelude(Some(&meta), None),
            build_prelude(None, Some(Path::new("/gates"))),
            build_prelude(Some(&meta), Some(Path::new("/gates"))),
        ] {
            assert!(
                prelude.ends_with('\n'),
                "must terminate the line: {prelude:?}"
            );
            assert_eq!(
                prelude.matches('\n').count(),
                1,
                "exactly one newline (a `\\n` INSIDE a metadata string must stay escaped): {prelude:?}"
            );
        }
    }

    /// The prelude splices a **caller-supplied** value (the launch's
    /// `init_ctx.task_metadata`) into a chunk that then gets executed, so
    /// the Lua-literal escaping is load-bearing as a trust boundary, not
    /// just as formatting. Run the generated prelude through a real Lua VM
    /// and assert a hostile payload comes back out as inert string data —
    /// if escaping ever regressed, `breakout` would be set and the values
    /// would not round-trip.
    #[test]
    fn prelude_escaping_contains_a_hostile_task_metadata_payload() {
        let hostile = serde_json::json!({
            "quote": "\"; breakout = true; local _ = \"",
            "close_brace": "}; breakout = true; local _ = {",
            "comment": "--[[ breakout = true ]]",
            "newline": "line1\nbreakout = true\nline2",
            "backslash": "c:\\path\\to",
        });
        let prelude = build_prelude(Some(&hostile), None);
        let lua = mlua::Lua::new();
        lua.load(&prelude)
            .exec()
            .expect("the generated prelude must be valid Lua");

        let globals = lua.globals();
        assert!(
            globals.get::<Option<bool>>("breakout").unwrap().is_none(),
            "no payload may escape its string literal: {prelude}"
        );
        let meta: mlua::Table = globals.get("_TASK_METADATA").expect("_TASK_METADATA set");
        for key in ["quote", "close_brace", "comment", "newline", "backslash"] {
            let got: String = meta.get(key).expect("key present");
            assert_eq!(
                got,
                hostile[key].as_str().unwrap(),
                "value must round-trip verbatim for key {key}"
            );
        }
    }

    #[test]
    fn prelude_restores_the_caller_scripts_own_package_path() {
        let prelude = build_prelude(None, Some(Path::new("/gates")));
        assert!(
            prelude.contains("package.path=\"/gates/?.lua;/gates/?/init.lua;\"..package.path"),
            "the script's own dir must be re-prepended: {prelude}"
        );
    }

    // ─── ScriptPlan resolution ────────────────────────────────────────────

    /// The zero-injection path must stay byte-for-byte the pre-GH-#86
    /// behavior: `ScriptSource::Path`, no host-side read (so a script that
    /// does not exist yet at spawn time still fails inside the SDK, where
    /// it always did), and no line-number shift.
    #[test]
    fn caller_script_stays_a_path_when_the_prelude_is_empty() {
        let plan = ScriptPlan::CallerScript(PathBuf::from("/nonexistent/gate.lua"));
        match plan
            .resolve("")
            .expect("empty prelude must not read the file")
        {
            ScriptSource::Path(p) => assert_eq!(p, PathBuf::from("/nonexistent/gate.lua")),
            other => panic!("expected Path, got: {other:?}"),
        }
    }

    #[test]
    fn caller_script_inlines_with_the_prelude_and_keeps_its_file_name() {
        let dir = std::env::temp_dir().join(format!("gh86-plan-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create dir");
        let path = dir.join("danger.lua");
        std::fs::write(&path, "return 1\n").expect("write script");

        let plan = ScriptPlan::CallerScript(path.clone());
        match plan.resolve("_TASK_METADATA={};\n").expect("resolve") {
            ScriptSource::Inline { source, name } => {
                assert_eq!(name, "danger.lua", "author's file name is preserved");
                assert_eq!(source, "_TASK_METADATA={};\nreturn 1\n");
            }
            other => panic!("expected Inline, got: {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn caller_script_read_failure_is_a_worker_error() {
        let plan = ScriptPlan::CallerScript(PathBuf::from("/nonexistent/gate.lua"));
        let err = plan
            .resolve("_TASK_METADATA={};\n")
            .expect_err("missing file must surface");
        assert!(
            format!("{err}").contains("/nonexistent/gate.lua"),
            "names the unreadable path: {err}"
        );
    }

    #[test]
    fn invoker_takes_the_prelude_without_a_package_path_fix() {
        let plan = build_inline_agent_invoker(&[]);
        assert!(
            plan.caller_script_dir().is_none(),
            "the invoker's script_dir is already project_root — nothing to restore"
        );
        match plan.resolve("_TASK_METADATA={};\n").expect("resolve") {
            ScriptSource::Inline { source, .. } => {
                assert!(source.starts_with("_TASK_METADATA={};\n"));
                assert!(source.contains("require(\"agent\")"));
            }
            other => panic!("expected Inline, got: {other:?}"),
        }
    }
}
