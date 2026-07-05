//! Blueprint `Compiler`, `CompiledAgentTable`, and the three default
//! `SpawnerFactory` implementations.
//!
//! ## Pipeline
//!
//! ```text
//! Blueprint (= flow + agents + hints + strategy + spawner_hints)
//!     │
//!     │ Compiler.compile(&bp)          ← this module (AgentDef → SpawnerAdapter table)
//!     ▼
//! CompiledBlueprint {
//!     router: Arc<CompiledAgentTable>, // ctx.agent → SpawnerAdapter lookup
//!     flow:   FlowNode,                // the flow.ir source (evaluated via EngineDispatcher)
//!     metadata: BlueprintMetadata,
//! }
//!     │
//!     │ service::linker::link(router, blueprint.spawner_hints.layers, &engine)
//!     ▼                                   ↑ Layer wrapping is done separately (src/service/linker.rs)
//! `Arc<dyn SpawnerAdapter>`            (already wrapped with base + hint SpawnerLayers)
//!     │
//!     ▼ EngineDispatcher::with_spawner → engine.dispatch_attempt_with
//! ```
//!
//! `CompiledAgentTable` is a thin table: it looks up `routes[name]` by
//! `ctx.agent` and hands the spawn off to the matching `SpawnerAdapter`.
//! The `routes` map is built at compile time through `SpawnerFactory`
//! implementations. Layer wrapping is not part of this module — it lives
//! in `service::linker::link`.

use crate::blueprint::{AgentDef, AgentKind, Blueprint, BlueprintMetadata};
use crate::core::ctx::Ctx;
use crate::core::engine::Engine;
use crate::operator::{Operator, OperatorSpawner, WorkerBinding};
use crate::types::{CapToken, TaskId};
use crate::worker::adapter::{InProcSpawner, SpawnError, SpawnerAdapter, WorkerFn};
use crate::worker::process_spawner::{ProcessSpawner, StreamMode};
use crate::worker::Worker;
use async_trait::async_trait;
use mlua_flow_ir::Node as FlowNode;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

// ─── error ───────────────────────────────────────────────────────────────

/// Everything that can go wrong while `Compiler::compile` turns a
/// `Blueprint` into a `CompiledBlueprint`.
#[derive(Debug, Error)]
pub enum CompileError {
    /// An `AgentDef.kind` has no matching entry in the `SpawnerRegistry`
    /// and `Blueprint.strategy.strict_kind` is set.
    #[error("unknown agent kind in SpawnerRegistry: {0:?}")]
    UnknownKind(AgentKind),
    /// The `AgentDef.spec` shape did not match what the factory for its
    /// kind requires (missing/mistyped field, etc.).
    #[error("agent '{name}' spec invalid: {msg}")]
    InvalidSpec {
        /// The offending agent's name.
        name: String,
        /// Human-readable description of what was wrong with the spec.
        msg: String,
    },
    /// The flow references an agent name that has no corresponding
    /// `AgentDef` (and no default spawner is configured).
    #[error("flow references agent '{0}' but no AgentDef matches")]
    UnresolvedRef(String),
    /// Two `AgentDef`s in the same `Blueprint` share a name.
    #[error("duplicate AgentDef name: {0}")]
    DuplicateAgent(String),
    /// A `kind = Operator` agent's `spec.operator_ref` does not match
    /// any `OperatorDef.name` declared in `Blueprint.operators`.
    #[error("agent '{agent}' operator_ref '{op_ref}' does not match any OperatorDef.name in Blueprint.operators (defined: {defined:?})")]
    UnresolvedOperatorRef {
        /// The agent whose `operator_ref` didn't resolve.
        agent: String,
        /// The `operator_ref` value that was looked up.
        op_ref: String,
        /// The `OperatorDef.name`s that *are* declared, for the error
        /// message.
        defined: Vec<String>,
    },
}

// ─── SpawnerFactory + Registry ───────────────────────────────────────────

/// Factory trait that interprets an `AgentDef` and builds the concrete
/// `SpawnerAdapter`. Register one per kind. Parsing the spec,
/// validating it, and baking the profile are the implementation's job.
///
/// The signature was widened in v9 from `(name, spec, hint)` to
/// `(&AgentDef, hint)` so the profile can be passed through. Most
/// implementations still just pull `&agent_def.name` and
/// `&agent_def.spec`, but Operator-backend factories consume
/// `agent_def.profile` to bake the persona in.
pub trait SpawnerFactory: Send + Sync {
    /// Build the concrete `SpawnerAdapter` for one `AgentDef`. `hint` is
    /// the matching entry (if any) from `Blueprint.hints.per_agent`.
    fn build(
        &self,
        agent_def: &AgentDef,
        hint: Option<&Value>,
    ) -> Result<Arc<dyn SpawnerAdapter>, CompileError>;
}

/// Companion trait that carries the **type-side source of truth** for
/// the Adapter ↔ `AgentKind` correspondence.
///
/// The base [`SpawnerFactory`] trait deliberately does not carry an
/// associated const so it stays dyn-compatible — that is, so it can be
/// stored and dispatched as `Arc<dyn SpawnerFactory>`. This companion
/// trait splits `const KIND: AgentKind` out, and
/// [`SpawnerRegistry::register`] uses `F::KIND` as the `HashMap` key.
/// That physically removes the string-lookup failure mode at the type
/// layer.
///
/// The three built-in factories (`Shell` / `InProc` / `Operator`)
/// implement this. Extension backends (say, `AgentBlockSpawnerFactory`)
/// follow the same explicit two-step recipe: add a new `AgentKind`
/// variant and implement this trait.
pub trait SpawnerFactoryKind: SpawnerFactory {
    /// The `AgentKind` this factory handles — used as the `HashMap` key
    /// by `SpawnerRegistry::register`.
    const KIND: AgentKind;
    /// The concrete Worker type produced by this `AgentKind` — this
    /// binds the type chain all the way from `AgentKind` down to `Worker`.
    /// Every factory declares it so the `AgentKind → Worker` mapping is
    /// explicit across all four layers. It is the source of truth for
    /// preserving the concrete type right up until `SpawnerAdapter::spawn`
    /// erases it into `Box<dyn Worker>`.
    type Worker: crate::worker::Worker;
}

/// `AgentKind → SpawnerFactory` mapping. The compiler looks entries up
/// during `compile()`.
#[derive(Clone)]
pub struct SpawnerRegistry {
    factories: HashMap<AgentKind, Arc<dyn SpawnerFactory>>,
}

impl SpawnerRegistry {
    /// Start with an empty `AgentKind → SpawnerFactory` mapping.
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }
    /// **Type-driven registration** — takes `F::KIND` and uses it as the
    /// `HashMap` key.
    ///
    /// Callers use the form
    /// `reg.register::<SubprocessProcessSpawnerFactory>(Arc::new(...))`
    /// and never have to pass an `AgentKind` literal. The Adapter ↔ Kind
    /// correspondence is enforced at the type layer, physically removing
    /// the string / enum-literal lookup failure mode.
    pub fn register<F: SpawnerFactoryKind + 'static>(&mut self, factory: Arc<F>) -> &mut Self {
        let f: Arc<dyn SpawnerFactory> = factory;
        self.factories.insert(F::KIND, f);
        self
    }
}

impl Default for SpawnerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Compiler ────────────────────────────────────────────────────────────

/// Turns a `Blueprint` into a `CompiledBlueprint` by resolving every
/// `AgentDef` against a `SpawnerRegistry`. One-shot: build a fresh
/// `Compiler` per `compile()` call (or reuse it — it holds no
/// per-compile state).
pub struct Compiler {
    registry: SpawnerRegistry,
    default_spawner: Option<Arc<dyn SpawnerAdapter>>,
}

/// The result of `Compiler::compile` — a routing table plus the
/// unmodified flow and metadata, ready to hand to
/// `EngineDispatcher::with_spawner` / `mlua_flow_ir::eval_async`.
pub struct CompiledBlueprint {
    /// `ctx.agent → SpawnerAdapter` lookup table.
    pub router: Arc<CompiledAgentTable>,
    /// The flow.ir source, copied verbatim from `Blueprint.flow`.
    pub flow: FlowNode,
    /// Copied verbatim from `Blueprint.metadata`.
    pub metadata: BlueprintMetadata,
}

impl Compiler {
    /// Build a `Compiler` around the given `SpawnerRegistry`, with no
    /// default spawner (unresolved flow refs are an error unless
    /// `with_default` is chained on).
    pub fn new(registry: SpawnerRegistry) -> Self {
        Self {
            registry,
            default_spawner: None,
        }
    }

    /// Set a default spawner — used for flow refs (and unregistered
    /// `AgentKind`s under non-strict strategy) that don't resolve
    /// against any `AgentDef`/`SpawnerRegistry` entry.
    pub fn with_default(mut self, sp: Arc<dyn SpawnerAdapter>) -> Self {
        self.default_spawner = Some(sp);
        self
    }

    /// Resolve every `Blueprint.agents` entry through the registry,
    /// validate `operator_ref`s and flow refs per `Blueprint.strategy`,
    /// and return the routing table alongside the untouched flow and
    /// metadata.
    pub fn compile(&self, bp: &Blueprint) -> Result<CompiledBlueprint, CompileError> {
        let mut routes: HashMap<String, Arc<dyn SpawnerAdapter>> = HashMap::new();
        let mut seen: HashMap<String, ()> = HashMap::new();

        // Design-time validation (OperatorDef as a first-class value):
        // every `kind = Operator` agent's `spec.operator_ref` must point at
        // one of `bp.operators[].name`. A Blueprint with any Operator agent
        // must therefore declare its operators up front; the empty-operators
        // backward-compat bypass is retired.
        let defined: Vec<String> = bp.operators.iter().map(|o| o.name.clone()).collect();
        for ad in &bp.agents {
            if !matches!(ad.kind, AgentKind::Operator) {
                continue;
            }
            let op_ref = ad.spec.get("operator_ref").and_then(|v| v.as_str());
            if let Some(op_ref) = op_ref {
                if !defined.iter().any(|n| n == op_ref) {
                    return Err(CompileError::UnresolvedOperatorRef {
                        agent: ad.name.clone(),
                        op_ref: op_ref.to_string(),
                        defined: defined.clone(),
                    });
                }
            }
            // A missing `op_ref` is reported through OperatorSpawnerFactory.build under a different error.
        }

        for ad in &bp.agents {
            if seen.contains_key(&ad.name) {
                return Err(CompileError::DuplicateAgent(ad.name.clone()));
            }
            seen.insert(ad.name.clone(), ());

            let factory = match self.registry.factories.get(&ad.kind) {
                Some(f) => f.clone(),
                None => {
                    if bp.strategy.strict_kind {
                        return Err(CompileError::UnknownKind(ad.kind.clone()));
                    } else {
                        tracing::warn!(
                            agent = %ad.name,
                            kind = ?ad.kind,
                            "no spawner factory registered for agent kind; \
                             dropping agent from routing table (strict_kind=false)"
                        );
                        continue;
                    }
                }
            };
            let hint = bp.hints.per_agent.get(&ad.name);
            let spawner = factory.build(ad, hint)?;
            routes.insert(ad.name.clone(), spawner);
        }

        if bp.strategy.strict_refs {
            verify_refs(&bp.flow, &routes, self.default_spawner.is_some())?;
        }

        let router = Arc::new(CompiledAgentTable {
            routes,
            default: self.default_spawner.clone(),
        });
        Ok(CompiledBlueprint {
            router,
            flow: bp.flow.clone(),
            metadata: bp.metadata.clone(),
        })
    }
}

/// Walk the flow `Node`, collect every `Step.ref`, and check that no ref
/// is unresolved against `routes` (or the default, when one exists).
fn verify_refs(
    node: &FlowNode,
    routes: &HashMap<String, Arc<dyn SpawnerAdapter>>,
    has_default: bool,
) -> Result<(), CompileError> {
    let mut refs: Vec<String> = Vec::new();
    collect_refs(node, &mut refs);
    for r in refs {
        if !routes.contains_key(&r) && !has_default {
            return Err(CompileError::UnresolvedRef(r));
        }
    }
    Ok(())
}

fn collect_refs(node: &FlowNode, out: &mut Vec<String>) {
    match node {
        FlowNode::Step { ref_, .. } => out.push(ref_.clone()),
        FlowNode::Seq { children } => {
            for c in children {
                collect_refs(c, out);
            }
        }
        FlowNode::Branch { then_, else_, .. } => {
            collect_refs(then_, out);
            collect_refs(else_, out);
        }
        FlowNode::Fanout { body, .. } => collect_refs(body, out),
        FlowNode::Loop { body, .. } => collect_refs(body, out),
        FlowNode::Try { body, catch, .. } => {
            collect_refs(body, out);
            collect_refs(catch, out);
        }
        FlowNode::Assign { .. } => {} // The Assign node carries no ref.
    }
}

// ─── CompiledAgentTable ───────────────────────────────────────────────────────

/// The compile result: an `agent name → SpawnerAdapter` lookup table.
///
/// Looks `routes` up by `ctx.agent` (the flow.ir `Step.ref`) and hands
/// the spawn to the matching `SpawnerAdapter`. If the name is not
/// registered and a `default` is configured, the default is used; if
/// there is no default, `SpawnError::NotRegistered` is returned.
///
/// Layer wrapping (`AuditMiddleware` / `MainAIMiddleware` and friends) is
/// not this type's concern — that is done separately in
/// `service::linker::link`.
pub struct CompiledAgentTable {
    pub(crate) routes: HashMap<String, Arc<dyn SpawnerAdapter>>,
    pub(crate) default: Option<Arc<dyn SpawnerAdapter>>,
}

impl CompiledAgentTable {
    /// Whether the given agent name is registered in the table — i.e.,
    /// whether its spawner has been resolved.
    pub fn has_route(&self, agent: &str) -> bool {
        self.routes.contains_key(agent)
    }
    /// List every resolved agent name.
    pub fn routed_agents(&self) -> Vec<String> {
        self.routes.keys().cloned().collect()
    }
}

#[async_trait]
impl SpawnerAdapter for CompiledAgentTable {
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: TaskId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        let sp = self
            .routes
            .get(&ctx.agent)
            .cloned()
            .or_else(|| self.default.clone())
            .ok_or_else(|| SpawnError::NotRegistered(ctx.agent.clone()))?;
        sp.spawn(engine, ctx, task_id, attempt, token).await
    }
}

// ─── default factories (three variants) ───────────────────────────────────

/// Factory for `AgentKind::Subprocess`. Turns the spec into a
/// [`ProcessSpawner`].
///
/// Naming convention: `<WorkerIMPL><AdapterType>SpawnerFactory`. Factory
/// names carry both the worker implementation and the host adapter so
/// they are not confused with each other; the old
/// `ShellSpawnerFactory` was renamed to this.
///
/// Spec shape:
/// ```jsonc
/// { "program": "agent-block", "args": ["-s","s.lua"],
///   "use_stdin": true,                       // optional, default = true
///   "stream_mode": "ndjson_lines" | "sse_events" | "length_prefixed" | null  // optional, default = null (plain)
/// }
/// ```
pub struct SubprocessProcessSpawnerFactory;

impl SpawnerFactoryKind for SubprocessProcessSpawnerFactory {
    const KIND: AgentKind = AgentKind::Subprocess;
    type Worker = crate::worker::process_spawner::ProcessWorker;
}

impl SpawnerFactory for SubprocessProcessSpawnerFactory {
    fn build(
        &self,
        agent_def: &AgentDef,
        _hint: Option<&Value>,
    ) -> Result<Arc<dyn SpawnerAdapter>, CompileError> {
        let agent_name = &agent_def.name;
        let spec = &agent_def.spec;
        let invalid = |msg: String| CompileError::InvalidSpec {
            name: agent_name.to_string(),
            msg,
        };
        let program = spec
            .get("program")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid("shell spec: 'program' (string) required".into()))?
            .to_string();
        let args: Vec<String> = spec
            .get("args")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let use_stdin = spec
            .get("use_stdin")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let stream_mode = match spec.get("stream_mode").and_then(|v| v.as_str()) {
            Some("ndjson_lines") => Some(StreamMode::NdjsonLines),
            Some("sse_events") => Some(StreamMode::SseEvents),
            Some("length_prefixed") => Some(StreamMode::LengthPrefixed),
            Some(other) => return Err(invalid(format!("unknown stream_mode: {other}"))),
            None => None,
        };

        let mut sp = ProcessSpawner {
            program,
            args,
            use_stdin,
            stream_mode,
        };
        if let Some(mode) = sp.stream_mode.clone() {
            sp = sp.stream_mode(mode);
        }
        Ok(Arc::new(sp))
    }
}

/// Factory for `AgentKind::Lua`. At `build` time it looks the `fn_id`
/// up in its internal registry and returns an [`InProcSpawner`] with the
/// Lua-eval `WorkerFn` registered under `agent_name` — one `InProcSpawner`
/// instance per agent.
///
/// Naming convention: `<WorkerIMPL><AdapterType>SpawnerFactory` (Lua
/// worker on InProcess adapter). One half of the old
/// `InProcSpawnerFactory`, split into Lua and RustFn variants.
///
/// Spec shape:
/// ```jsonc
/// { "fn_id": "patch-spawner" }     // Lua source id pre-registered with the factory
/// ```
pub struct LuaInProcessSpawnerFactory {
    registry: HashMap<String, WorkerFn>,
    bridges: HashMap<String, HostBridge>,
}

/// Rust-side bridge function callable from Lua.
///
/// Inputs and outputs are both `serde_json::Value` (i.e. JSON). Lua
/// invokes it as `host.<name>(arg_table)`. If the implementation needs
/// to call async Rust, the caller does the sync-ification (typically
/// `tokio::runtime::Handle::current().block_on(...)`).
///
/// Design intent: keep Lua scripts focused on flow control and `ctx`
/// walking, while the heavy lifting (LLM calls, RFC 6902 apply,
/// verifiers, and so on) stays on the Rust side. Going "pure Lua" —
/// removing the bridge — is a carry.
#[derive(Clone)]
pub struct HostBridge(
    Arc<dyn Fn(serde_json::Value) -> Result<serde_json::Value, String> + Send + Sync>,
);

impl HostBridge {
    /// Wrap a Rust closure as a bridge callable from Lua.
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(serde_json::Value) -> Result<serde_json::Value, String> + Send + Sync + 'static,
    {
        Self(Arc::new(f))
    }

    /// Invoke the bridge directly — a thin trampoline over the inner
    /// `Fn`. The production path goes through the Lua runtime, but this
    /// stays `pub` so unit tests can exercise the primitive directly.
    pub fn call(&self, arg: serde_json::Value) -> Result<serde_json::Value, String> {
        (self.0)(arg)
    }
}

/// Carrier type for Lua script sources. Paths are not required — a
/// source string plus an identifying label is all it holds.
///
/// Callers bring in the source (via `include_str!` or similar) and
/// register it with the factory through
/// [`LuaInProcessSpawnerFactory::register_lua`].
#[derive(Clone)]
pub struct LuaScriptSource {
    /// The Lua chunk source.
    pub source: String,
    /// Label used in error messages — typically the script's logical id
    /// (for example `"patch_spawner.lua"`).
    pub label: String,
}

impl LuaScriptSource {
    /// Wrap a Lua chunk source and its error-message label.
    pub fn new(source: impl Into<String>, label: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            label: label.into(),
        }
    }
}

impl LuaInProcessSpawnerFactory {
    /// Start with no registered scripts and no host bridges.
    pub fn new() -> Self {
        Self {
            registry: HashMap::new(),
            bridges: HashMap::new(),
        }
    }

    /// Register a host bridge. Subsequent `register_lua` calls snapshot
    /// the current bridge set.
    ///
    /// Ordering rule: register bridges first, then call `register_lua`;
    /// bridges added after `register_lua` will not be visible to that
    /// script.
    pub fn with_bridge(mut self, name: impl Into<String>, bridge: HostBridge) -> Self {
        self.bridges.insert(name.into(), bridge);
        self
    }

    /// Register a **Lua-eval Worker** under `fn_id`.
    ///
    /// Each dispatch spins up a fresh `mlua::Lua` VM, injects globals
    /// (`_PROMPT` / `_AGENT` / `_TASK_ID` / `_ATTEMPT` / `_CTX` — the last
    /// is `_PROMPT` parsed as JSON, or `nil` if that fails), evaluates
    /// the script, and marshals the returned table into a `WorkerResult`.
    ///
    /// Marshalling rules for the return value:
    /// - `{ value = ..., ok = bool }` → `WorkerResult.value` /
    ///   `WorkerResult.ok` verbatim.
    /// - Anything else → `value = <returned value>`, `ok = true`.
    ///
    /// Execution runs on `tokio::task::spawn_blocking` because `mlua::Lua`
    /// is `!Send` and needs to stay away from the tokio async context.
    /// Host bridges (the Lua-to-Rust callback path) previously registered
    /// with [`Self::with_bridge`] are snapshotted at call time and
    /// injected into every dispatch inside `run_lua_worker`.
    pub fn register_lua(mut self, fn_id: impl Into<String>, source: LuaScriptSource) -> Self {
        let source = Arc::new(source);
        let bridges = Arc::new(self.bridges.clone());
        let wrapped: WorkerFn = Arc::new(move |inv| {
            let source = source.clone();
            let bridges = bridges.clone();
            Box::pin(run_lua_worker(source, bridges, inv))
        });
        self.registry.insert(fn_id.into(), wrapped);
        self
    }
}

/// Body of a single Lua-eval invocation (called from `register_lua`).
async fn run_lua_worker(
    source: Arc<LuaScriptSource>,
    bridges: Arc<HashMap<String, HostBridge>>,
    inv: crate::worker::adapter::WorkerInvocation,
) -> Result<crate::worker::adapter::WorkerResult, crate::worker::adapter::WorkerError> {
    use crate::worker::adapter::WorkerError;
    use mlua::LuaSerdeExt;

    let label = source.label.clone();
    let outcome =
        tokio::task::spawn_blocking(move || -> Result<(serde_json::Value, bool), String> {
            let lua = mlua::Lua::new();
            let g = lua.globals();

            // 1. Base globals.
            g.set("_PROMPT", inv.prompt.clone())
                .map_err(|e| format!("set _PROMPT: {e}"))?;
            g.set("_AGENT", inv.agent.clone())
                .map_err(|e| format!("set _AGENT: {e}"))?;
            g.set("_TASK_ID", inv.task_id.to_string())
                .map_err(|e| format!("set _TASK_ID: {e}"))?;
            g.set("_ATTEMPT", inv.attempt as i64)
                .map_err(|e| format!("set _ATTEMPT: {e}"))?;

            // 2. _CTX = JSON parse(_PROMPT); nil on parse failure (co-exists with the plain-string prompt path).
            if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&inv.prompt) {
                let lua_val = lua
                    .to_value(&json_val)
                    .map_err(|e| format!("_CTX to_value: {e}"))?;
                g.set("_CTX", lua_val)
                    .map_err(|e| format!("set _CTX: {e}"))?;
            }

            // 3. Inject the host bridge (Lua can call `host.<name>(arg)`).
            if !bridges.is_empty() {
                let host = lua
                    .create_table()
                    .map_err(|e| format!("create host table: {e}"))?;
                for (name, bridge) in bridges.iter() {
                    let bridge = bridge.clone();
                    let bname = name.clone();
                    let f = lua
                        .create_function(move |lua, arg: mlua::Value| {
                            let json_arg: serde_json::Value = lua.from_value(arg).map_err(|e| {
                                mlua::Error::external(format!("bridge {bname} arg → json: {e}"))
                            })?;
                            let result_json =
                                bridge.call(json_arg).map_err(mlua::Error::external)?;
                            lua.to_value(&result_json).map_err(|e| {
                                mlua::Error::external(format!("bridge {bname} ret → lua: {e}"))
                            })
                        })
                        .map_err(|e| format!("create_function {name}: {e}"))?;
                    host.set(name.as_str(), f)
                        .map_err(|e| format!("host.{name} set: {e}"))?;
                }
                g.set("host", host).map_err(|e| format!("set host: {e}"))?;
            }

            // 4. eval
            let result: mlua::Value = lua
                .load(&source.source)
                .set_name(&source.label)
                .eval()
                .map_err(|e| format!("lua eval [{}]: {e}", source.label))?;

            // 5. Marshal: shape `{ value=..., ok=true }` or raw value.
            let json_result: serde_json::Value = lua
                .from_value(result)
                .map_err(|e| format!("lua → json [{}]: {e}", source.label))?;

            let (value, ok) = match &json_result {
                serde_json::Value::Object(map)
                    if map.contains_key("value") || map.contains_key("ok") =>
                {
                    let ok = map.get("ok").and_then(|v| v.as_bool()).unwrap_or(true);
                    let value = map.get("value").cloned().unwrap_or(json_result.clone());
                    (value, ok)
                }
                _ => (json_result, true),
            };
            Ok((value, ok))
        })
        .await
        .map_err(|e| WorkerError::Failed(format!("spawn_blocking join [{label}]: {e}")))?
        .map_err(WorkerError::Failed)?;

    Ok(crate::worker::adapter::WorkerResult {
        value: outcome.0,
        ok: outcome.1,
    })
}

impl Default for LuaInProcessSpawnerFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl SpawnerFactoryKind for LuaInProcessSpawnerFactory {
    const KIND: AgentKind = AgentKind::Lua;
    type Worker = LuaWorker;
}

impl SpawnerFactory for LuaInProcessSpawnerFactory {
    fn build(
        &self,
        agent_def: &AgentDef,
        _hint: Option<&Value>,
    ) -> Result<Arc<dyn SpawnerAdapter>, CompileError> {
        build_inproc_from_registry::<LuaWorker>(&self.registry, agent_def, "lua")
    }
}

/// Factory for `AgentKind::RustFn`. At `build` time it looks the `fn_id`
/// up in its internal registry and returns an [`InProcSpawner`] with the
/// Rust closure `WorkerFn` registered under `agent_name`.
///
/// Naming convention: `<WorkerIMPL><AdapterType>SpawnerFactory` (RustFn
/// worker on InProcess adapter). Sibling to
/// [`LuaInProcessSpawnerFactory`] — the Lua-worker half of the same
/// split.
///
/// Spec shape:
/// ```jsonc
/// { "fn_id": "echo" }     // Rust closure id pre-registered with the factory
/// ```
pub struct RustFnInProcessSpawnerFactory {
    registry: HashMap<String, WorkerFn>,
}

impl RustFnInProcessSpawnerFactory {
    /// Start with no registered closures.
    pub fn new() -> Self {
        Self {
            registry: HashMap::new(),
        }
    }

    /// Register a Rust closure `WorkerFn` under `fn_id`, wrapping it so
    /// it matches the `WorkerFn` signature (boxed, pinned future).
    pub fn register_fn<F, Fut>(mut self, fn_id: impl Into<String>, f: F) -> Self
    where
        F: Fn(crate::worker::adapter::WorkerInvocation) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<
                Output = Result<
                    crate::worker::adapter::WorkerResult,
                    crate::worker::adapter::WorkerError,
                >,
            > + Send
            + 'static,
    {
        let f = Arc::new(f);
        let wrapped: WorkerFn = Arc::new(move |inv| {
            let f = f.clone();
            Box::pin(f(inv))
        });
        self.registry.insert(fn_id.into(), wrapped);
        self
    }
}

impl Default for RustFnInProcessSpawnerFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl SpawnerFactoryKind for RustFnInProcessSpawnerFactory {
    const KIND: AgentKind = AgentKind::RustFn;
    type Worker = RustFnWorker;
}

impl SpawnerFactory for RustFnInProcessSpawnerFactory {
    fn build(
        &self,
        agent_def: &AgentDef,
        _hint: Option<&Value>,
    ) -> Result<Arc<dyn SpawnerAdapter>, CompileError> {
        build_inproc_from_registry::<RustFnWorker>(&self.registry, agent_def, "rust_fn")
    }
}

/// Shared build helper used by both the Lua and the RustFn factories —
/// look `spec.fn_id` up in the registry and return an `InProcSpawner`.
/// The generic type parameter `W` fixes the per-kind Worker concrete
/// type at the type level (the build-site half of the trait's
/// associated-type binding across the four-layer cascade).
fn build_inproc_from_registry<W>(
    registry: &HashMap<String, WorkerFn>,
    agent_def: &AgentDef,
    kind_label: &str,
) -> Result<Arc<dyn SpawnerAdapter>, CompileError>
where
    W: crate::worker::Worker + From<crate::worker::WorkerJoinHandler> + Send + Sync + 'static,
{
    let agent_name = &agent_def.name;
    let spec = &agent_def.spec;
    let invalid = |msg: String| CompileError::InvalidSpec {
        name: agent_name.to_string(),
        msg,
    };
    let fn_id = spec
        .get("fn_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| invalid(format!("{kind_label} spec: 'fn_id' (string) required")))?;
    let f = registry
        .get(fn_id)
        .cloned()
        .ok_or_else(|| invalid(format!("fn_id '{fn_id}' not registered in factory")))?;
    let mut sp: InProcSpawner<W> = InProcSpawner::<W>::typed();
    // Register under `agent_name` (the flow's `Step.ref`). Both
    // `CompiledAgentTable` and the `InProcSpawner` look the function up
    // by name, so the same key is needed at both layers.
    sp.registry.insert(agent_name.to_string(), f);
    Ok(Arc::new(sp))
}

/// Concrete Worker type for the Lua kind — a handle to a Lua-eval task
/// inside an mlua VM. Embeds a `WorkerJoinHandler`. Reserved as the home
/// for future Lua-specific extensions (an mlua VM cancellation
/// mechanism, Lua-side error type retention, and so on).
pub struct LuaWorker {
    /// The join handle / cancellation token for the underlying task.
    pub handler: crate::worker::WorkerJoinHandler,
}

impl From<crate::worker::WorkerJoinHandler> for LuaWorker {
    fn from(handler: crate::worker::WorkerJoinHandler) -> Self {
        Self { handler }
    }
}

#[async_trait::async_trait]
impl crate::worker::Worker for LuaWorker {
    fn id(&self) -> &crate::types::WorkerId {
        &self.handler.worker_id
    }
    fn cancel_token(&self) -> tokio_util::sync::CancellationToken {
        self.handler.cancel.clone()
    }
    async fn join(self: Box<Self>) -> Result<(), crate::worker::adapter::WorkerError> {
        self.handler.await_completion().await
    }
}

/// Concrete Worker type for the RustFn kind — a handle to a task that
/// directly calls a Rust closure. Embeds a `WorkerJoinHandler`. Being a
/// pure function, there is minimal kind-specific extension surface here;
/// the primary purpose is to nail down the type binding.
pub struct RustFnWorker {
    /// The join handle / cancellation token for the underlying task.
    pub handler: crate::worker::WorkerJoinHandler,
}

impl From<crate::worker::WorkerJoinHandler> for RustFnWorker {
    fn from(handler: crate::worker::WorkerJoinHandler) -> Self {
        Self { handler }
    }
}

#[async_trait::async_trait]
impl crate::worker::Worker for RustFnWorker {
    fn id(&self) -> &crate::types::WorkerId {
        &self.handler.worker_id
    }
    fn cancel_token(&self) -> tokio_util::sync::CancellationToken {
        self.handler.cancel.clone()
    }
    async fn join(self: Box<Self>) -> Result<(), crate::worker::adapter::WorkerError> {
        self.handler.await_completion().await
    }
}

/// Factory for `AgentKind::Operator`. Looks up the `Arc<dyn Operator>`
/// pre-registered under `spec.operator_ref` and wraps it in an
/// `OperatorSpawner`. Also resolves `AgentDef.profile.worker_binding` into
/// a `WorkerBinding` at compile time and fails loud (`CompileError::InvalidSpec`)
/// when the resolved operator's `Operator::requires_worker_binding` is `true`
/// and no binding was declared.
///
/// Spec shape:
/// ```jsonc
/// { "operator_ref": "main_ai" }     // Operator id pre-registered with the factory
/// ```
///
/// # Split of responsibilities with `OperatorDelegateMiddleware`
///
/// The two axes exist for different reasons:
///
/// - **This factory (`OperatorSpawnerFactory` → `OperatorSpawner`) — the
///   AgentSpec axis.** Bakes a separate Operator backend into each
///   `AgentDef`. A `kind = Operator` `AgentDef` names its backend through
///   `spec.operator_ref`; at `compile()` time the `Arc<dyn Operator>` is
///   baked into `routes[agent_name]`. Because the `agent.md` loader
///   (`agent_md_loader`) defaults `kind` to `Operator`, agents that flow
///   in through agent-profiles land here.
///
/// - **`OperatorDelegateMiddleware` — the Blueprint-global (session)
///   axis.** Delegates every agent to the same Operator backend. At
///   session-attach time you call `engine.register_operator(id, op)`
///   plus `attach_with_ids(.., operator_backend_id = Some(id))` to bind
///   it session-wide, and declare
///   `spawner_hints.layers = ["operator_delegate"]` to opt in. `ctx.agent`
///   is ignored; the operator handles every spawn in that session (a
///   MainAI-wide driver, a human-wide console, that sort of thing).
///
/// # Exclusivity (a double fire is structurally impossible)
///
/// When both are effective — the hint is declared, the session has an
/// operator backend, **and** the Blueprint has a `kind = Operator`
/// `AgentDef` — `OperatorDelegateMiddleware` sits at the outer end of
/// the stack and **completely bypasses** `inner.spawn`. The
/// `OperatorSpawner` is never reached, so under those conditions this
/// factory's routes entry is inert. This is not a double fire — the
/// session axis is overriding the agent axis. Consistent usage means
/// picking one axis per use case.
///
/// Interior mutability is provided by an `Arc<RwLock>`. Even after the
/// factory has been stored as `Arc<dyn SpawnerFactory>` in
/// `SpawnerRegistry`, a caller holding an `Arc` clone can still add
/// Operator backends dynamically via `register_operator(&self, id, op)`.
/// Typical uses: registering a `WSOperatorSession` under the session id
/// on WebSocket connect, binding agents that arrive via the `agent.md`
/// loader to arbitrary backends, and so on. `build()` performs a
/// `read()` lookup each time.
pub struct OperatorSpawnerFactory {
    operators: Arc<std::sync::RwLock<HashMap<String, Arc<dyn Operator>>>>,
}

impl OperatorSpawnerFactory {
    /// Start with no registered Operator backends.
    pub fn new() -> Self {
        Self {
            operators: Arc::new(std::sync::RwLock::new(HashMap::new())),
        }
    }

    /// Register an Operator backend dynamically through `&self`.
    /// Overwrites are allowed — later wins. Callers can still reach this
    /// after the factory has been stored as `Arc<dyn SpawnerFactory>` in
    /// `SpawnerRegistry`, as long as they hold an `Arc` clone; interior
    /// mutability is provided by the inner `RwLock`.
    pub fn register_operator(&self, id: impl Into<String>, op: Arc<dyn Operator>) -> &Self {
        self.operators
            .write()
            .expect("OperatorSpawnerFactory.operators RwLock poisoned")
            .insert(id.into(), op);
        self
    }

    /// Dynamically unregister an id (used to clean up when a WebSocket
    /// disconnects, for example). A missing id is a no-op.
    pub fn unregister_operator(&self, id: &str) -> &Self {
        self.operators
            .write()
            .expect("OperatorSpawnerFactory.operators RwLock poisoned")
            .remove(id);
        self
    }
}

impl Default for OperatorSpawnerFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl SpawnerFactoryKind for OperatorSpawnerFactory {
    const KIND: AgentKind = AgentKind::Operator;
    type Worker = crate::operator::OperatorWorker;
}

impl SpawnerFactory for OperatorSpawnerFactory {
    fn build(
        &self,
        agent_def: &AgentDef,
        _hint: Option<&Value>,
    ) -> Result<Arc<dyn SpawnerAdapter>, CompileError> {
        let agent_name = &agent_def.name;
        let spec = &agent_def.spec;
        // Bake AgentDef.profile.system_prompt into the OperatorSpawner at compile time.
        // `Some` → adopted first at spawn time; `None` → falls back to fetch_prompt (initial_directive).
        // Fallback path. Sibling: AgentBlockInProcessSpawnerFactory
        // (agent_block/runtime.rs) does the same compile-time bake by stuffing
        // the profile into BlockConfig.context.
        let system_prompt = agent_def.profile.as_ref().map(|p| p.system_prompt.clone());
        let invalid = |msg: String| CompileError::InvalidSpec {
            name: agent_name.to_string(),
            msg,
        };
        let op_ref = spec
            .get("operator_ref")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid("operator spec: 'operator_ref' (string) required".into()))?;
        let operators = self
            .operators
            .read()
            .expect("OperatorSpawnerFactory.operators RwLock poisoned");
        let op = operators.get(op_ref).cloned().ok_or_else(|| {
            let mut names: Vec<String> = operators.keys().cloned().collect();
            names.sort();
            let names_list = if names.is_empty() {
                "<none>".to_string()
            } else {
                names.join(", ")
            };
            invalid(format!(
                "operator_ref '{op_ref}' not registered in factory. \
                 Registered sids: [{names_list}]. \
                 Hint: call mse_operator_join(roles=[...]) to mint the sid first."
            ))
        })?;
        drop(operators);

        // Resolve the Blueprint-baked worker binding from
        // `AgentDef.profile.worker_binding` — the SoT for the
        // declaration↔executor binding (see `WorkerBinding` doc). Fail
        // loud at compile time when the operator backend requires one
        // and the Blueprint didn't declare it; this is a compile-time
        // gate, not a runtime guess.
        let worker_binding = agent_def
            .profile
            .as_ref()
            .and_then(|p| p.worker_binding.as_ref())
            .map(|variant| WorkerBinding {
                variant: variant.clone(),
                tools: agent_def
                    .profile
                    .as_ref()
                    .map(|p| p.tools.clone())
                    .unwrap_or_default(),
            });
        if op.requires_worker_binding() && worker_binding.is_none() {
            return Err(invalid(
                "profile.worker_binding is required for this operator backend; \
                 declare it in the agent .md frontmatter"
                    .into(),
            ));
        }
        Ok(Arc::new(OperatorSpawner::new(
            op,
            system_prompt,
            worker_binding,
        )))
    }
}

#[cfg(test)]
mod operator_spawner_factory_worker_binding_tests {
    use super::*;
    use crate::blueprint::AgentProfile;
    use crate::core::ctx::Ctx;
    use crate::types::CapToken;
    use crate::worker::adapter::{WorkerError, WorkerResult};

    /// Minimal `Operator` stub whose `requires_worker_binding` is
    /// configurable — enough to exercise the compile-time fail-loud gate
    /// without standing up a real backend (e.g. `WSOperatorSession`,
    /// which lives in a downstream crate).
    struct StubOperator {
        requires_binding: bool,
    }

    #[async_trait]
    impl Operator for StubOperator {
        async fn execute(
            &self,
            _ctx: &Ctx,
            _system: Option<String>,
            _prompt: String,
            _worker: Option<WorkerBinding>,
            _worker_token: CapToken,
        ) -> Result<WorkerResult, WorkerError> {
            Ok(WorkerResult {
                value: Value::Null,
                ok: true,
            })
        }

        fn requires_worker_binding(&self) -> bool {
            self.requires_binding
        }
    }

    fn agent_def_with(profile: Option<AgentProfile>) -> AgentDef {
        AgentDef {
            name: "test-agent".to_string(),
            kind: AgentKind::Operator,
            spec: serde_json::json!({ "operator_ref": "op1" }),
            profile,
            meta: None,
        }
    }

    #[test]
    fn build_fails_loud_when_binding_required_but_absent() {
        let factory = OperatorSpawnerFactory::new();
        factory.register_operator(
            "op1",
            Arc::new(StubOperator {
                requires_binding: true,
            }) as Arc<dyn Operator>,
        );
        let def = agent_def_with(Some(AgentProfile::default()));
        match factory.build(&def, None) {
            Err(CompileError::InvalidSpec { name, msg }) => {
                assert_eq!(name, "test-agent");
                assert!(
                    msg.contains("worker_binding is required"),
                    "unexpected message: {msg}"
                );
            }
            Err(other) => panic!("expected InvalidSpec, got: {other:?}"),
            Ok(_) => panic!("expected compile-time failure, got Ok"),
        }
    }

    #[test]
    fn build_succeeds_when_binding_required_and_present() {
        let factory = OperatorSpawnerFactory::new();
        factory.register_operator(
            "op1",
            Arc::new(StubOperator {
                requires_binding: true,
            }) as Arc<dyn Operator>,
        );
        let profile = AgentProfile {
            worker_binding: Some("mse-worker-coder".to_string()),
            tools: vec!["Read".to_string(), "Edit".to_string()],
            ..Default::default()
        };
        let def = agent_def_with(Some(profile));
        assert!(
            factory.build(&def, None).is_ok(),
            "expected Ok when worker_binding is declared"
        );
    }

    #[test]
    fn build_succeeds_when_binding_not_required_and_absent() {
        let factory = OperatorSpawnerFactory::new();
        factory.register_operator(
            "op1",
            Arc::new(StubOperator {
                requires_binding: false,
            }) as Arc<dyn Operator>,
        );
        let def = agent_def_with(Some(AgentProfile::default()));
        assert!(
            factory.build(&def, None).is_ok(),
            "backends that don't require a binding must not be gated by its absence"
        );
    }
}
