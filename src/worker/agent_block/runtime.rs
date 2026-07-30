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
//! ## Two modes (via `ScriptSource`)
//!
//! | Mode | Trigger | Chunk |
//! |---|---|---|
//! | **PromptBasedAgent** (default) | `spec.script_path` absent | `ScriptSource::Inline` — the invoker [`build_inline_agent_invoker`] generates, which calls the SDK's `agent` StdPkg module with the resolved `mcp_servers` embedded. NOT the SDK's own `DefaultAgent`: that one passes no tools, so a frontmatter `tools:` line could never reach the model. |
//! | **ScriptBasedAgent** | `spec.script_path = "<path>"` | `ScriptSource::Path(...)` — a caller-provided Lua script, handed to the SDK verbatim. |
//!
//! Neither mode requires the script to agree on an event-kind string: the
//! `host_handler` sink takes any kind as the terminal result (see
//! "Per-task input and result" below for the one reserved exception).
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
//! directly, and no `SpawnerAdapter` wrapper re-resolves it — every
//! Lua-visible surface below is derived from that one value:
//!
//! | Lua surface | Source |
//! |---|---|
//! | `_PROMPT` | The step's evaluated `in`, via `inv.prompt` → `BlockConfig.prompt`. A **String** — a structured `in` arrives JSON-stringified, so a script that wants a table calls `std.json.decode(_PROMPT)`. |
//! | `_CONTEXT` | `profile.system_prompt`, via `BlockConfig.context`. |
//! | [`TASK_METADATA_GLOBAL`] (`_TASK_METADATA`) | `view.task_metadata` (the launch's `init_ctx.task_metadata` bag), set through the SDK's `extra_globals` — converted natively, so nesting survives and the chunk text is never rewritten. |
//! | [`AGENT_CTX_GLOBAL`] (`_AGENT_CTX`) | `view.extra` — the Blueprint-declared agent context (`default_agent_ctx` / `AgentMeta.ctx`, GH #21) after `ContextPolicy` filtering. |
//!
//! Both come from [`context_globals`], the single place this mapping is
//! decided; the Lua in-process worker renders the same two globals from
//! it, so a gate is portable between the two backends. `view.steps`
//! (prior-step OUTPUT pointers) is deliberately NOT rendered — see the
//! carrier note in [`crate::core::agent_context`].
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
//! **not** by returning a value from the chunk. Two destinations:
//!
//! | emit kind | Effect |
//! |---|---|
//! | [`ARTIFACT_EVENT_KIND`] (`artifact`) | Stages a named part through [`WorkerInvocation::sink`] (`{name = ..., content = ...}`) and leaves the invocation running. Any number of these. |
//! | anything else | The terminal result, **first emit wins**. [`WorkerResultCaptor`] normalises the payload into [`WorkerResult`]`.value` (`payload.content` → `payload.response` → the whole payload). |
//!
//! Both verdict channels are therefore reachable:
//! `VerdictChannel::Body` compares the terminal value, and
//! `VerdictChannel::Part` compares a staged `"verdict"` part — stage it
//! with `bus.emit("artifact", {name = "verdict", content = "PASS"})`
//! before the terminal emit, and the plain body stays free for the
//! report.
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
///
/// # Named parts (GH #86)
///
/// One event kind is reserved: [`ARTIFACT_EVENT_KIND`]. An emit under that
/// kind is staged as an [`OutputEvent::Artifact`] through
/// [`WorkerInvocation::sink`] and does **not** complete the invocation, so
/// a script may stage any number of named parts and then finish with its
/// terminal emit as usual. This is what makes `VerdictChannel::Part`
/// reachable from this backend — the engine's completion-time contract
/// check looks for a staged `"verdict"` artifact.
///
/// Reserving a kind is a deliberate step back from "kind-agnostic": with
/// two destinations there is now something to route, so the script side
/// has one string to coordinate. Every other kind keeps the old
/// first-emit-wins result behavior.
struct WorkerResultCaptor {
    tx: Mutex<Option<oneshot::Sender<WorkerResult>>>,
    /// Intake for staged named parts; `None` when the caller path did not
    /// wire one (an `artifact` emit then degrades to a `tracing::warn!`
    /// rather than silently vanishing).
    sink: Option<Arc<dyn crate::worker::output::OutputSink>>,
    /// The agent's Blueprint-declared model
    /// ([`AgentBlockSettings::declared_model`]) — the fallback for
    /// `stats.model` when the payload does not report one at runtime.
    declared_model: Option<String>,
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
    /// payload carries none of them (caller-script `worker_result`
    /// shapes). The raw `usage` object also rides as `adapter_data` so
    /// provider-specific detail (cache tokens etc.) survives.
    ///
    /// A payload-level `"model"` string is the **runtime-observed** model
    /// and is adopted verbatim; it outranks the Blueprint-declared
    /// fallback applied in [`Handler::call`], because what actually
    /// served the attempt beats what was asked for.
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
        let model = payload
            .get("model")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        if usage.is_none() && num_turns.is_none() && model.is_none() {
            return None;
        }
        Some(crate::store::trace::WorkerStats {
            worker_kind: Some("agent_block".to_string()),
            model,
            usage,
            num_turns,
            adapter_data: usage_raw.cloned(),
        })
    }

    /// GH #86: stage one named part from an [`ARTIFACT_EVENT_KIND`] emit.
    ///
    /// `payload.name` (string) is required — an emit without it cannot
    /// address a part, so it is reported to the script as an error rather
    /// than dropped. `payload.content` is the body; absent means the part
    /// is staged empty (`Value::Null`), which is still a distinct,
    /// addressable staging event.
    async fn stage_artifact(&self, payload: &Value) -> Result<(), BlockError> {
        let name = payload
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                BlockError::Runtime(format!(
                    "bus.emit(\"{ARTIFACT_EVENT_KIND}\", ...) requires a string `name` field \
                     naming the part (got: {payload})"
                ))
            })?
            .to_string();
        let Some(sink) = self.sink.as_ref() else {
            tracing::warn!(
                artifact = %name,
                "agent-block staged an artifact but no OutputSink is wired for this \
                 invocation; the part is dropped"
            );
            return Ok(());
        };
        let content = payload.get("content").cloned().unwrap_or(Value::Null);
        sink.emit(crate::worker::output::OutputEvent::Artifact {
            name: name.clone(),
            content: crate::worker::output::ContentRef::Inline { value: content },
        })
        .await
        .map_err(|e| BlockError::Runtime(format!("staging artifact '{name}': {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl Handler for WorkerResultCaptor {
    async fn call(
        &self,
        kind: String,
        _id: String,
        payload: Value,
        _meta: Value,
    ) -> Result<Value, BlockError> {
        // GH #86: the one reserved kind routes to the named-part intake and
        // leaves the invocation running; everything else is the terminal
        // result (first emit wins).
        if kind == ARTIFACT_EVENT_KIND {
            self.stage_artifact(&payload).await?;
            return Ok(Value::Null);
        }
        let (value, ok) = Self::extract(&payload);
        let stats = Self::extract_stats(&payload);
        // Even when the SDK payload carries no usage (script-side
        // `worker_result` shapes), the boundary still knows its own
        // kind — surface it so `StepEntry.worker_kind` is never empty.
        let mut wr = WorkerResult { value, ok, stats }.ensure_worker_kind("agent_block");
        // `agent.run` returns no model, so without this the sidecar's
        // `model` was always empty on this backend. Fall back to the
        // Blueprint-declared `profile.model`, mirroring the subprocess
        // backend's baked `{model}`. Precedence: runtime-observed
        // (`extract_stats`) > declared > none — `get_or_insert_with`
        // encodes exactly that (it is a no-op once a value is present).
        if let (Some(declared), Some(stats)) = (self.declared_model.as_deref(), wr.stats.as_mut()) {
            stats.model.get_or_insert_with(|| declared.to_string());
        }
        if let Ok(mut guard) = self.tx.lock() {
            if let Some(tx) = guard.take() {
                let _ = tx.send(wr);
            }
        }
        Ok(Value::Null)
    }
}

/// The Lua global carrying the launch's `init_ctx.task_metadata` bag into
/// a script, set through the SDK's `extra_globals` (agent-block-core
/// v0.30+).
///
/// Underscore-prefixed to sit alongside the SDK's own globals.
/// `_PROMPT` / `_CONTEXT` / `_SCRIPT_NAME` are reserved by the SDK and
/// must not be used here; this name is ours.
pub const TASK_METADATA_GLOBAL: &str = "_TASK_METADATA";

/// The Lua global carrying the Blueprint-declared agent context — the
/// `AgentContextView.extra` bag, which `AgentContextMiddleware` fills from
/// `Blueprint.default_agent_ctx` / `AgentMeta.ctx` (GH #21) after applying
/// `ContextPolicy`.
///
/// The WS Operator lane has always received these as `{key}: {value}`
/// lines of the Spawn directive header
/// (`AgentContextView::to_directive_header`); this is the in-process
/// equivalent. Delivered as ONE table rather than one global per key,
/// because the keys are Blueprint-author-chosen and must not be able to
/// shadow `_PROMPT` / `_CONTEXT` / `_SCRIPT_NAME` or each other's
/// namespace.
pub const AGENT_CTX_GLOBAL: &str = "_AGENT_CTX";

/// The Lua globals this backend derives from the materialized context
/// view — the single place the mapping "view field → Lua global" is
/// decided, so [`crate::blueprint::compiler`]'s Lua worker can render the
/// same surface and keep a gate portable between the two in-process
/// backends.
///
/// Absent / empty inputs contribute no entry at all (rather than an empty
/// table), so a script sees `nil` and can branch on presence — the same
/// "insert nothing when absent" contract the rest of this axis follows.
pub fn context_globals(view: Option<&AgentContextView>) -> HashMap<String, Value> {
    let mut globals = HashMap::new();
    let Some(view) = view else {
        return globals;
    };
    if let Some(meta) = view.task_metadata.clone() {
        globals.insert(TASK_METADATA_GLOBAL.to_string(), meta);
    }
    if !view.extra.is_empty() {
        globals.insert(
            AGENT_CTX_GLOBAL.to_string(),
            Value::Object(view.extra.clone()),
        );
    }
    globals
}

/// The one `bus.emit` kind this backend reserves: an emit under it stages
/// a named part instead of completing the invocation (GH #86).
///
/// Payload shape: `{ name = "<part>", content = <any> }`. `name` is
/// required. Reaching `VerdictChannel::Part` from a script means staging
/// `{ name = "verdict", content = "PASS" }` before the terminal emit.
pub const ARTIFACT_EVENT_KIND: &str = "artifact";

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
    script: ScriptSource,
    /// Compile-time fallback cwd: `spec.project_root`, else
    /// `env::current_dir()`. Outranked per invocation by the context
    /// view's `work_dir` / `project_root`.
    spec_project_root: PathBuf,
    mcp_rpc_timeout: Duration,
    /// Agent persona — the `system_prompt` composed from the agent.md
    /// body and frontmatter. `None` maps to `BlockConfig.context = None`
    /// for backwards compatibility with the old path.
    profile_context: Option<String>,
    /// The agent.md frontmatter `model:` line (`profile.model`), baked at
    /// compile time. Observational only — this backend does not select a
    /// model with it (the SDK owns that); it is the declared fallback for
    /// the per-step `stats.model` sidecar, so a step of an agent whose BP
    /// names a model is attributable even though `agent.run` reports
    /// none. Sibling of the subprocess backend's baked `{model}`.
    declared_model: Option<String>,
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
        sink: inv.sink.clone(),
        declared_model: settings.declared_model.clone(),
    });

    // GH #86: the task-context tier, read off the ONE in-process seam
    // (`WorkerInvocation.context`, filled by `InProcSpawner::spawn` from
    // the materialized `AgentContextView`) instead of a hand-rolled `Ctx`
    // peek in a spawner wrapper.
    let project_root = resolve_project_root(inv.context.as_ref(), &settings.spec_project_root);

    // Bridge the shutdown token: forward `WorkerInvocation.cancel_token`
    // into the SDK's `shutdown_token` if one is set; otherwise use a
    // fresh token (no external cancel).
    let shutdown_token = inv.cancel_token.clone().unwrap_or_default();

    let mut builder = BlockConfig::builder(settings.script.clone(), project_root)
        .mcp_rpc_timeout(settings.mcp_rpc_timeout)
        .prompt(PromptSource::Inline(inv.prompt))
        .host_handler(captor)
        .auto_serve_bus(true)
        .shutdown_token(shutdown_token.clone());
    if let Some(system) = settings.profile_context.clone() {
        builder = builder.context(PromptSource::Inline(system));
    }
    // The task-context globals. `extra_globals` (SDK v0.30) sets each entry
    // on both Isles before the chunk runs, converting the JSON natively —
    // so nothing is spliced into the script text: no line-number shift, no
    // `script_dir` / `package.path` disturbance, and no string-escaping
    // trust boundary around caller-supplied values.
    let globals = context_globals(inv.context.as_ref());
    if !globals.is_empty() {
        builder = builder.extra_globals(globals);
    }
    let config = builder.build();

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
                ScriptSource::Path(PathBuf::from(s))
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
        // Same source the subprocess backend bakes its `{model}`
        // placeholder from; here it only ever reaches the per-step stats
        // sidecar (see `AgentBlockSettings::declared_model`).
        let declared_model = agent_def.profile.as_ref().and_then(|p| p.model.clone());

        let settings = Arc::new(AgentBlockSettings {
            script,
            spec_project_root,
            mcp_rpc_timeout,
            profile_context,
            declared_model,
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
    use crate::core::agent_context::{TASK_METADATA_KEY, TASK_PROJECT_ROOT_KEY, TASK_WORK_DIR_KEY};

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
            sink: None,
            declared_model: None,
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

    // ─── declared model → per-step stats sidecar ─────────────────────────

    /// Run one payload through a captor carrying `declared_model` and
    /// return the resulting `stats.model`.
    async fn captured_model(declared: Option<&str>, payload: Value) -> Option<String> {
        let (tx, rx) = oneshot::channel();
        let captor = WorkerResultCaptor {
            tx: Mutex::new(Some(tx)),
            sink: None,
            declared_model: declared.map(str::to_string),
        };
        captor
            .call("agent_result".into(), "evt-1".into(), payload, Value::Null)
            .await
            .expect("handler ack");
        let wr = rx.await.expect("recv");
        wr.stats
            .expect("worker_kind alone guarantees a sidecar")
            .model
    }

    /// `agent.run` reports no model, so the declared `profile.model` is
    /// what lands in the sidecar — the gap that left `StepEntry.stats
    /// .model` permanently `None` on this backend.
    #[tokio::test]
    async fn declared_model_lands_in_the_stats_sidecar() {
        let payload = serde_json::json!({
            "content": "done",
            "usage": {"input_tokens": 10, "output_tokens": 4},
            "num_turns": 2,
        });
        assert_eq!(
            captured_model(Some("opus"), payload).await,
            Some("opus".to_string())
        );
    }

    /// The fallback does not depend on the usage rail: a caller-script
    /// `worker_result` shape (no `usage`, no `num_turns`) still gets the
    /// declared model, because `ensure_worker_kind` has already created
    /// the sidecar.
    #[tokio::test]
    async fn declared_model_lands_even_without_usage_in_the_payload() {
        let payload = serde_json::json!({ "ok": true, "response": "hello" });
        assert_eq!(
            captured_model(Some("sonnet"), payload).await,
            Some("sonnet".to_string())
        );
    }

    /// A runtime-reported `model` is what actually served the attempt, so
    /// it outranks the declaration.
    #[tokio::test]
    async fn runtime_reported_model_wins_over_the_declaration() {
        let payload = serde_json::json!({
            "content": "done",
            "model": "claude-runtime-1",
            "usage": {"input_tokens": 10, "output_tokens": 4},
        });
        assert_eq!(
            captured_model(Some("opus"), payload).await,
            Some("claude-runtime-1".to_string())
        );

        // …including when the payload carries nothing else the stats
        // extractor keys on.
        let payload = serde_json::json!({ "content": "done", "model": "claude-runtime-1" });
        assert_eq!(
            captured_model(Some("opus"), payload).await,
            Some("claude-runtime-1".to_string())
        );
    }

    /// No declaration and no runtime report = no attribution invented.
    #[tokio::test]
    async fn no_declared_model_leaves_the_sidecar_model_empty() {
        let payload = serde_json::json!({ "ok": true, "response": "hello" });
        assert_eq!(captured_model(None, payload).await, None);
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

    // ─── GH #86: task_metadata delivery via SDK extra_globals ─────────────

    /// An `artifact` emit must reach the sink as an
    /// `OutputEvent::Artifact` AND leave the invocation running, so the
    /// script can stage parts and still finish with its terminal emit.
    #[tokio::test]
    async fn artifact_kind_stages_a_named_part_without_completing() {
        use crate::worker::output::{ContentRef, OutputEvent, OutputSink};

        #[derive(Default)]
        struct RecordingSink(Mutex<Vec<OutputEvent>>);
        #[async_trait]
        impl OutputSink for RecordingSink {
            async fn emit(&self, event: OutputEvent) -> Result<(), crate::EngineError> {
                self.0.lock().unwrap().push(event);
                Ok(())
            }
        }

        let sink = Arc::new(RecordingSink::default());
        let (tx, mut rx) = oneshot::channel();
        let captor = WorkerResultCaptor {
            tx: Mutex::new(Some(tx)),
            sink: Some(sink.clone()),
            declared_model: None,
        };

        captor
            .call(
                ARTIFACT_EVENT_KIND.into(),
                "evt-1".into(),
                serde_json::json!({ "name": "verdict", "content": "PASS" }),
                Value::Null,
            )
            .await
            .expect("staging must succeed");

        let staged = sink.0.lock().unwrap().clone();
        assert_eq!(staged.len(), 1, "exactly one artifact staged");
        match &staged[0] {
            OutputEvent::Artifact { name, content } => {
                assert_eq!(name, "verdict");
                // `ContentRef` is not `PartialEq`; match the variant.
                match content {
                    ContentRef::Inline { value } => {
                        assert_eq!(value, &serde_json::json!("PASS"))
                    }
                    other => panic!("expected Inline content, got: {other:?}"),
                }
            }
            other => panic!("expected Artifact, got: {other:?}"),
        }
        assert!(
            rx.try_recv().is_err(),
            "an artifact emit must NOT complete the invocation"
        );

        // The terminal emit still lands afterwards.
        captor
            .call(
                "worker_result".into(),
                "evt-2".into(),
                serde_json::json!({ "ok": true, "response": "done" }),
                Value::Null,
            )
            .await
            .expect("terminal emit");
        assert_eq!(
            rx.await.expect("recv").value,
            serde_json::json!("done"),
            "the non-reserved kind still completes the invocation"
        );
    }

    /// An `artifact` emit with no `name` cannot address a part, so it is
    /// an error back to the script rather than a silent drop.
    #[tokio::test]
    async fn artifact_without_a_name_is_reported_to_the_script() {
        let (tx, _rx) = oneshot::channel();
        let captor = WorkerResultCaptor {
            tx: Mutex::new(Some(tx)),
            sink: None,
            declared_model: None,
        };
        let err = captor
            .call(
                ARTIFACT_EVENT_KIND.into(),
                "evt-1".into(),
                serde_json::json!({ "content": "PASS" }),
                Value::Null,
            )
            .await
            .expect_err("a nameless artifact must fail loud");
        assert!(
            format!("{err}").contains("name"),
            "names the missing field: {err}"
        );
    }

    // ─── GH #86: the shared view → Lua-global mapping ─────────────────────

    #[test]
    fn context_globals_renders_task_metadata_and_agent_ctx() {
        let mut view = view_with(&[(TASK_METADATA_KEY, serde_json::json!({"issue": 86}))]);
        view.extra.insert(
            "org_conventions".to_string(),
            serde_json::json!("two-space indent"),
        );
        let globals = context_globals(Some(&view));
        assert_eq!(
            globals.get(TASK_METADATA_GLOBAL),
            Some(&serde_json::json!({"issue": 86}))
        );
        assert_eq!(
            globals.get(AGENT_CTX_GLOBAL),
            Some(&serde_json::json!({"org_conventions": "two-space indent"})),
            "Blueprint-declared agent ctx must reach the in-process lane too"
        );
    }

    /// Absent fields contribute no entry, so a script sees `nil` and can
    /// branch on presence — an empty table would be indistinguishable from
    /// "the author declared an empty ctx".
    #[test]
    fn context_globals_omits_absent_fields() {
        assert!(context_globals(None).is_empty(), "no view → no globals");
        assert!(
            context_globals(Some(&view_with(&[]))).is_empty(),
            "empty view → no globals"
        );

        let view = view_with(&[(TASK_METADATA_KEY, serde_json::json!({"issue": 86}))]);
        let globals = context_globals(Some(&view));
        assert!(globals.contains_key(TASK_METADATA_GLOBAL));
        assert!(
            !globals.contains_key(AGENT_CTX_GLOBAL),
            "an empty `extra` must not render an empty _AGENT_CTX table"
        );
    }

    /// Neither global may collide with an SDK-reserved name, and the two
    /// must not collide with each other.
    #[test]
    fn context_globals_use_names_the_sdk_does_not_reserve() {
        for name in [TASK_METADATA_GLOBAL, AGENT_CTX_GLOBAL] {
            for reserved in ["_PROMPT", "_CONTEXT", "_SCRIPT_NAME"] {
                assert_ne!(name, reserved);
            }
        }
        assert_ne!(TASK_METADATA_GLOBAL, AGENT_CTX_GLOBAL);
    }

    /// The global name must not collide with the three the SDK reserves
    /// for itself — a collision would be silently overwritten one way or
    /// the other depending on injection order.
    #[test]
    fn task_metadata_global_does_not_shadow_an_sdk_reserved_name() {
        for reserved in ["_PROMPT", "_CONTEXT", "_SCRIPT_NAME"] {
            assert_ne!(TASK_METADATA_GLOBAL, reserved);
        }
    }

    /// Script mode keeps `ScriptSource::Path`: delivering `task_metadata`
    /// through `extra_globals` means the chunk itself is never rewritten,
    /// so a caller script's own directory stays on `package.path` (sibling
    /// `require` keeps working) and its Lua stack-trace line numbers are
    /// unshifted. The `sibling_require_resolves_*` e2e is the behavioural
    /// half of this claim.
    #[tokio::test]
    async fn script_mode_never_rewrites_the_caller_chunk() {
        use crate::blueprint::compiler::SpawnerFactory;

        let ad = agent_block_def(
            "gate-danger",
            serde_json::json!({ "script_path": "/nonexistent/gate.lua" }),
            &[],
        );
        // A build must succeed without touching the file: the path is
        // handed to the SDK verbatim, exactly as before GH #86.
        AgentBlockInProcessSpawnerFactory::new()
            .build(&ad, None)
            .expect("script mode must not read the script at build time");
    }
}
