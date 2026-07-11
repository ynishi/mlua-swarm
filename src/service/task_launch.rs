//! `TaskLaunchService` — the domain service that runs a Blueprint flow
//! to completion through the engine.
//!
//! Responsibilities:
//! 1. Compile the Blueprint and link it into a `SpawnerAdapter` (via
//!    `service::linker::link`, wrapped by `EngineDispatcher::with_spawner`).
//! 2. Acquire an Operator session (via `engine.attach`).
//! 3. Run flow.ir's `eval_async_externs` through an `EngineDispatcher`
//!    (threading the service-held `call_extern` registry) and return
//!    the final `ctx`.
//! 4. If any step fails (dispatcher error), the eval errors and
//!    the failure propagates as-is.
//!
//! Callers on the Application layer never touch the engine directly —
//! `bind`, `start_task`, and `eval_async` all stay inside the Service.
//!
//! A single-task-spawn API (calling `start_task` directly) is
//! deliberately absent here: a single spawn can be modeled as a
//! one-Step flow, and we do not want two interfaces for the same
//! shape.

use crate::blueprint::compiler::{CompileError, Compiler};
use crate::blueprint::{AuditDef, Blueprint, EngineDispatcher};
use crate::core::agent_context::ContextPolicy;
use crate::core::ctx::OperatorKind;
use crate::core::engine::Engine;
use crate::core::errors::EngineError;
use crate::middleware::agent_context::AgentContextMiddleware;
use crate::middleware::project_name_alias::ProjectNameAliasMiddleware;
use crate::middleware::task_input::TaskInputMiddleware;
use crate::middleware::worker_binding::WorkerBindingMiddleware;
use crate::middleware::{AfterRunAuditMiddleware, SpawnerStack};
use crate::operator::WorkerBinding;
use crate::service::linker;
use crate::store::run::RunContext;
use crate::types::{CapToken, Role};
use mlua_flow_ir::{Externs, NoExterns};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

/// Derive the "BP Agent-level" tier of the `OperatorKind` cascade from a
/// Blueprint: for every `AgentDef` whose `spec.operator_ref` resolves to an
/// `OperatorDef` with a `Some` `kind`, map `AgentDef.name -> OperatorKind`.
///
/// Deliberately **not** filtered by `AgentDef.kind == AgentKind::Operator`:
/// the `OperatorKind` cascade is a middleware-level cross-cutting concern
/// (spawn_hook / senior_bridge / operator-delegate gating via `Ctx.operator`),
/// orthogonal to the Worker IMPL axis that `AgentKind` expresses (see the
/// crate root doc, "Operator is delivered as a cross-cutting overlay through
/// `Ctx` plus middleware"). A `RustFn` / `Lua` / `Subprocess` agent can
/// equally declare `spec.operator_ref` to opt into a BP-declared
/// `OperatorKind` without changing its Worker IMPL. Agents without an
/// `operator_ref`, an unresolved `operator_ref`, or an `OperatorDef.kind =
/// None` are simply absent from the map (= that tier falls through for
/// them). This is a separate, independent consumer of `Blueprint.operators`
/// from the design-time `operator_ref` validation in
/// `blueprint::compiler::Compiler::compile` (issue: `OperatorDef`
/// first-class treatment), which only checks the reference resolves for
/// `AgentKind::Operator` agents and is unaffected by this function.
/// Build the `agent name → WorkerBinding` map from
/// `Blueprint.agents[].profile.worker_binding` — the launch-time sibling of
/// the compile-time resolution in `OperatorSpawnerFactory::build`. Consumed
/// by `WorkerBindingMiddleware` so the delegate axis
/// (`OperatorDelegateMiddleware`) can resolve the binding via `ctx.agent`
/// like every other agent-keyed table (`CompiledAgentTable.routes` idiom).
/// Agents without a declared binding are simply absent (no silent default).
fn derive_worker_bindings(blueprint: &Blueprint) -> HashMap<String, WorkerBinding> {
    blueprint
        .agents
        .iter()
        .filter_map(|ad| {
            let profile = ad.profile.as_ref()?;
            let variant = profile.worker_binding.as_ref()?;
            Some((
                ad.name.clone(),
                WorkerBinding {
                    variant: variant.clone(),
                    tools: profile.tools.clone(),
                },
            ))
        })
        .collect()
}

/// GH #34 — extract the Blueprint-declared after-run audit hooks
/// (`Blueprint.audits`), the launch-time input to `AfterRunAuditMiddleware`.
/// Trivial extraction (unlike [`derive_worker_bindings`] / the agent-context
/// derivers below, no per-agent lookup is needed — `AuditDef.agent` is a
/// plain agent-name string already validated against `Blueprint.agents` at
/// `Compiler::compile` time). `[]` (every pre-#34 Blueprint) means "no
/// audit layer at all" — see the conditional `.layer(...)` wiring in
/// [`TaskLaunchService::launch`] (invariant #4: byte-identical behavior).
fn derive_audits(blueprint: &Blueprint) -> Vec<AuditDef> {
    blueprint.audits.clone()
}

/// Issue #21 Phase 1: build the agent-context supply axis's "BP Global" +
/// "BP Agent-level" context tiers from a Blueprint — the launch-time
/// sibling of [`derive_worker_bindings`] (same "no silent default"
/// discipline: an agent's entry is present only when it declares one).
/// Consumed by `AgentContextMiddleware`, which shallow-merges the two
/// tiers per spawn (agent wins) and inserts the result into
/// `ctx.meta.runtime` only-if-absent (see
/// `crate::middleware::agent_context`'s module doc for the full merge +
/// precedence narrative).
///
/// - `.0` (global) = [`Blueprint::default_agent_ctx`], unchanged.
/// - `.1` (per-agent) = `AgentDef.name -> AgentMeta.ctx`, entry present
///   only for agents whose `meta` is `Some` and who declare a `ctx`
///   (directly via `meta.ctx`, and/or indirectly via
///   [`AgentMeta::meta_ref`] — GH #21 Phase 2, see below).
///
/// # GH #21 Phase 2: `AgentMeta.meta_ref` resolution
///
/// When an agent declares `meta.meta_ref`, it is resolved against
/// [`derive_step_metas`]'s pool and used as the BASE layer UNDER the
/// agent's own inline `meta.ctx` (inline wins on key collision, shallow
/// merge — see [`shallow_merge_inline_wins`]). An unresolved `meta_ref`
/// at this point means the caller launched a Blueprint that bypassed
/// `Compiler::compile`'s validation (the loud gate for this case, see
/// `blueprint::compiler::Compiler::compile`'s `UnresolvedMetaRef` check);
/// this function stays defensive and never panics — it logs a warning and
/// skips the base layer, letting the agent's own inline `ctx` (if any)
/// stand alone.
fn derive_agent_ctx(blueprint: &Blueprint) -> (Option<Value>, HashMap<String, Value>) {
    let global = blueprint.default_agent_ctx.clone();
    let meta_pool = derive_step_metas(blueprint);
    let per_agent = blueprint
        .agents
        .iter()
        .filter_map(|ad| {
            let meta = ad.meta.as_ref()?;
            let inline = meta.ctx.clone();
            let base = meta.meta_ref.as_ref().and_then(|name| {
                let resolved = meta_pool.get(name).cloned();
                if resolved.is_none() {
                    tracing::warn!(
                        agent = %ad.name,
                        meta_ref = %name,
                        "derive_agent_ctx: AgentMeta.meta_ref names an undefined Blueprint.metas entry; skipping the base layer"
                    );
                }
                resolved
            });
            let merged = match (base, inline) {
                (None, None) => None,
                (Some(base), None) => Some(base),
                (None, Some(inline)) => Some(inline),
                (Some(base), Some(inline)) => Some(shallow_merge_inline_wins(base, inline)),
            };
            merged.map(|ctx| (ad.name.clone(), ctx))
        })
        .collect();
    (global, per_agent)
}

/// GH #21 Phase 2: shallow-merge `base` with `inline`, `inline` winning
/// key collisions. Both sides being JSON `Object`s is the meaningful case
/// (per-key merge); a non-`Object` `inline` is used as-is (it "wins"
/// entirely — the malformed-shape case is left to
/// `AgentContextMiddleware`'s own tier merge, which already warns + skips
/// a non-`Object` tier value downstream, never failing the spawn).
fn shallow_merge_inline_wins(base: Value, inline: Value) -> Value {
    match (base, inline) {
        (Value::Object(mut base), Value::Object(inline)) => {
            for (k, v) in inline {
                base.insert(k, v);
            }
            Value::Object(base)
        }
        (_, inline) => inline,
    }
}

/// GH #21 Phase 2: build the `Blueprint.metas` named pool (`MetaDef.name
/// -> MetaDef.ctx`) — the launch-time sibling of [`derive_worker_bindings`]
/// / [`derive_agent_ctx`], resolving the Step tier's shared pool instead
/// of a per-agent map. Consumed by `EngineDispatcher::with_step_metas`
/// (the Step tier's `$step_meta.ref` resolver) and, indirectly, by
/// [`derive_agent_ctx`]'s `AgentMeta.meta_ref` resolution (the Agent
/// tier shares the same pool).
fn derive_step_metas(blueprint: &Blueprint) -> HashMap<String, Value> {
    blueprint
        .metas
        .iter()
        .map(|m| (m.name.clone(), m.ctx.clone()))
        .collect()
}

/// Issue #21 Phase 1: build the [`ContextPolicy`] cascade's "BP Global" +
/// "BP Agent-level" tiers from a Blueprint — same shape and discipline as
/// [`derive_agent_ctx`], from `Blueprint.default_context_policy` /
/// `AgentMeta.context_policy` instead. Consumed by
/// `AgentContextMiddleware`, which resolves the effective policy per spawn
/// (per-agent tier outranks the BP-global one; pass-all when neither is
/// declared for the dispatching agent).
fn derive_context_policies(
    blueprint: &Blueprint,
) -> (Option<ContextPolicy>, HashMap<String, ContextPolicy>) {
    let default_policy = blueprint.default_context_policy.clone();
    let per_agent = blueprint
        .agents
        .iter()
        .filter_map(|ad| {
            let meta = ad.meta.as_ref()?;
            let policy = meta.context_policy.clone()?;
            Some((ad.name.clone(), policy))
        })
        .collect();
    (default_policy, per_agent)
}

/// Issue #19 ST3: shallow-merge the "BP Global" default `init_ctx`
/// (`Blueprint.default_init_ctx`) with the Task-level `init_ctx` — the
/// second layer of the (eventual 4-layer) init-ctx cascade, following the
/// same "BP default, Task overrides" shape as the `OperatorKind` cascade
/// (see `derive_bp_agent_kinds` / `TaskLaunchInput::operator_kind`).
///
/// Semantics (deliberately a single rule, no deep merge / JSON Patch):
///
/// - `bp_default = None` → `task_init_ctx` passes through unchanged
///   (pre-#19 Blueprints keep today's exact behavior).
/// - Both sides are `Value::Object` → shallow key-wise merge, Task wins
///   on collision (`task_init_ctx`'s keys are applied last).
/// - `task_init_ctx` is present but not an `Object` (`Null` / `String` /
///   `Array` / `Number` / `Bool`) → Task fully replaces the BP default;
///   the caller's non-Object seed is respected as-is.
fn merge_init_ctx(bp_default: Option<&Value>, task_init_ctx: &Value) -> Value {
    match (bp_default, task_init_ctx) {
        (Some(Value::Object(bp_map)), Value::Object(task_map)) => {
            let mut merged = bp_map.clone();
            for (k, v) in task_map {
                merged.insert(k.clone(), v.clone());
            }
            Value::Object(merged)
        }
        (None, _) => task_init_ctx.clone(),
        (_, task) => task.clone(),
    }
}

/// Issue #19 ST4: 3-layer shallow-merge of the init-ctx cascade — BP
/// default → Task → Run (lowest to highest priority). Built by chaining
/// [`merge_init_ctx`] twice rather than introducing a distinct 3-way merge
/// algorithm, so the Run layer inherits exactly the same "shallow Object
/// merge, non-Object fully replaces" rule [`merge_init_ctx`] already
/// established for the BP/Task pair (see its doc for the full semantics).
///
/// - `run_override: None` is a no-op — the BP+Task merge passes through
///   unchanged, so `POST /v1/tasks/:id/runs` with no body (or a body that
///   omits `init_ctx_override`) preserves today's rekick behavior
///   byte-for-byte.
/// - `run_override: Some(_)` layers on top exactly like `task_init_ctx`
///   layers on top of `bp_default` above: both `Object` → shallow
///   key-wise merge with Run winning collisions; Run non-`Object` →
///   fully replaces the BP+Task merge.
pub fn merge_init_ctx_3layer(
    bp_default: Option<&Value>,
    task_init_ctx: &Value,
    run_override: Option<&Value>,
) -> Value {
    let bp_task = merge_init_ctx(bp_default, task_init_ctx);
    match run_override {
        Some(run) => merge_init_ctx(Some(&bp_task), run),
        None => bp_task,
    }
}

fn derive_bp_agent_kinds(blueprint: &Blueprint) -> HashMap<String, OperatorKind> {
    let mut out = HashMap::new();
    if blueprint.operators.is_empty() {
        return out;
    }
    for agent in &blueprint.agents {
        let Some(op_ref) = agent.spec.get("operator_ref").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(op_def) = blueprint.operators.iter().find(|o| o.name == op_ref) else {
            continue;
        };
        if let Some(kind) = op_def.kind {
            out.insert(agent.name.clone(), OperatorKind::from(kind));
        }
    }
    out
}

/// Failure modes of [`TaskLaunchService::launch`].
#[derive(Debug, Error)]
pub enum TaskLaunchError {
    /// `Compiler::compile` rejected the Blueprint.
    #[error("compile: {0}")]
    Compile(#[from] CompileError),
    /// `Engine::attach_with_ids` failed.
    #[error("engine: {0}")]
    Engine(#[from] EngineError),
    /// A `Step` inside `flow.ir`'s `eval_async` produced a dispatcher
    /// error, or a sub-flow raised.
    #[error("flow eval: {0}")]
    FlowEval(String),
}

/// Canonical bag of Task-level fields (`project_root` / `work_dir` /
/// `task_metadata`) — [`TaskLaunchInput::task_input`]'s type.
///
/// Issue #19 ST2: replaces the ST1 `resolve_task_level_init_ctx`
/// fold-back-into-`init_ctx` bridge (removed from
/// `mlua-swarm-server`'s `run_flow_form`). Callers resolve these three
/// fields once at the wire boundary — sibling body field first, falling
/// back to the legacy shape (same three keys nested directly inside
/// `init_ctx`) only there — and hand the result straight through here;
/// `init_ctx` itself is no longer mutated to carry them, so it stays a
/// pure flow-ir eval seed identical to whatever the caller sent.
///
/// Each field is independently optional — see
/// [`crate::middleware::task_input::TaskInputMiddleware::new_from_fields`],
/// which this is built for.
///
/// Issue #19 ST4: also `Serialize`/`Deserialize` so it can travel over the
/// wire as `RunKickRequest.task_input_override` (`mlua-swarm-server`'s
/// `tasks` module) and be snapshotted into `TaskRecord.task_input_spec`
/// (JSON) for rekick to resolve back out of. Every field is
/// `#[serde(default)]` so a caller may omit any subset (or send `{}`) and
/// still deserialize.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TaskInputSpec {
    /// Task-level project root path.
    #[serde(default)]
    pub project_root: Option<String>,
    /// Task-level working directory path.
    #[serde(default)]
    pub work_dir: Option<String>,
    /// Task-level arbitrary metadata bag (a JSON object, or `None`).
    #[serde(default)]
    #[schemars(with = "Option<Value>")]
    pub task_metadata: Option<Value>,
}

/// Input to [`TaskLaunchService::launch`].
#[derive(Debug, Clone)]
pub struct TaskLaunchInput {
    /// The Blueprint to compile, link, and run.
    pub blueprint: Blueprint,
    /// Caller-supplied id for the Operator that owns this run.
    pub operator_id: String,
    /// The Operator's role for this run.
    pub role: Role,
    /// How long the attached session is allowed to live.
    pub ttl: Duration,
    /// "Runtime Global" tier of the `OperatorKind` cascade. `Some(_)` is
    /// always an explicit request — including `Some(OperatorKind::Automate)`
    /// — that outranks the BP-level tiers (`OperatorDef.kind` /
    /// `Blueprint.default_operator_kind`); `None` leaves it unspecified so
    /// those tiers / the final default decide. Under `MainAi` or
    /// `Composite`, `MainAIMiddleware`'s `spawn_hook` before/after
    /// callbacks become effective. See
    /// `crate::core::ctx::collapse_operator_kind`.
    pub operator_kind: Option<OperatorKind>,
    /// `SeniorBridge` registry ID. `None` — no bridge; `Some(id)` —
    /// attach a bridge previously registered via
    /// `engine.register_senior_bridge`.
    pub bridge_id: Option<String>,
    /// `SpawnHook` registry ID. Same shape as above, via
    /// `engine.register_spawn_hook`.
    pub hook_id: Option<String>,
    /// Operator registry ID — used on the path that hands the whole
    /// spawn off to an external Operator. Name previously registered
    /// with `engine.register_operator`; resolved by
    /// `OperatorDelegateMiddleware`, which — for `kind = MainAi` or
    /// `Composite` — bypasses `inner.spawn` and calls
    /// `operator.execute`.
    pub operator_backend_id: Option<String>,
    /// "Runtime Agent-level" tier (highest priority) of the `OperatorKind`
    /// cascade — per-agent override, keyed by `AgentDef.name`. Empty by
    /// default (no override for any agent). See
    /// `crate::core::ctx::collapse_operator_kind` for the full tier list.
    pub operator_kind_overrides: HashMap<String, OperatorKind>,
    /// The initial `ctx` (JSON `Value`) that flow.ir's `eval_async`
    /// starts from. Every `Step.in` `$.<path>` reference reads from
    /// here. Issue #19 ST2: a pure flow-ir eval seed — no Task-level
    /// field is folded into it anymore; see [`Self::task_input`].
    pub init_ctx: Value,
    /// Task-level canonical fields (issue #19 ST2). `Some` layers a
    /// [`crate::middleware::task_input::TaskInputMiddleware`] (built via
    /// [`crate::middleware::task_input::TaskInputMiddleware::new_from_fields`])
    /// onto the spawner stack just before spawn; `None` is a no-op,
    /// identical to today's behavior for callers with no Task-level
    /// fields to propagate.
    pub task_input: Option<TaskInputSpec>,
    /// Issue #13 run_id propagation: when `Some`, every step this launch
    /// dispatches is traced into `RunRecord.step_entries` and exposes its
    /// `run_id` via `Ctx.meta.runtime["run_id"]` (see
    /// `EngineDispatcher::with_run`). `None` (the default via
    /// [`Self::automate`]) preserves the pre-existing behavior — no run
    /// tracing.
    pub run_ctx: Option<RunContext>,
}

impl TaskLaunchInput {
    /// Helper for existing callers on the default path — no hooks and no
    /// per-agent `OperatorKind` overrides. Leaves the "Runtime Global" tier
    /// unspecified (`None`), so the BP-level tiers / final default
    /// (`OperatorKind::Automate`) decide — this preserves today's
    /// behaviour for every existing caller without silently forcing
    /// `Automate` as an explicit override that would outrank a BP-declared
    /// `MainAi`/`Composite` kind. `run_ctx` and `task_input` both default
    /// to `None` (no run tracing, no Task-level fields); construct the
    /// struct literal directly to set either.
    pub fn automate(
        blueprint: Blueprint,
        operator_id: impl Into<String>,
        role: Role,
        ttl: Duration,
        init_ctx: Value,
    ) -> Self {
        Self {
            blueprint,
            operator_id: operator_id.into(),
            role,
            ttl,
            operator_kind: None,
            bridge_id: None,
            hook_id: None,
            operator_backend_id: None,
            operator_kind_overrides: HashMap::new(),
            init_ctx,
            task_input: None,
            run_ctx: None,
        }
    }
}

/// Result of a successful [`TaskLaunchService::launch`] call.
#[derive(Debug, Clone)]
pub struct TaskLaunchOutput {
    /// The capability token for the attached session.
    pub token: CapToken,
    /// The final `ctx` after the flow ran — every `Step.out` has
    /// been written. Application-layer callers pull the outcome out
    /// of this `Value` and fold it into a domain status.
    pub final_ctx: Value,
}

/// Domain service that compiles, links, and runs a Blueprint's flow to
/// completion through the [`Engine`]. See the module doc for the full
/// responsibility list.
pub struct TaskLaunchService {
    engine: Engine,
    compiler: Compiler,
    /// `call_extern` registry threaded into flow eval. Defaults to
    /// [`NoExterns`] (= every `call_extern` in a Blueprint raises
    /// `ExternError`); hosts opt in via [`Self::with_externs`] with an
    /// `ExternMap` of pure value-shape functions.
    externs: Arc<dyn Externs + Send + Sync>,
}

impl TaskLaunchService {
    /// Build a service bound to one `Engine` and one `Compiler`.
    pub fn new(engine: Engine, compiler: Compiler) -> Self {
        Self {
            engine,
            compiler,
            externs: Arc::new(NoExterns),
        }
    }

    /// Replace the `call_extern` registry (builder style). Entries MUST be
    /// pure functions — no side effects, no flow control; effectful work
    /// belongs to `Step` / agents, not externs (flow-ir canonical contract).
    pub fn with_externs(mut self, externs: Arc<dyn Externs + Send + Sync>) -> Self {
        self.externs = externs;
        self
    }

    /// The bound `Engine`.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// The bound `Compiler`.
    pub fn compiler(&self) -> &Compiler {
        &self.compiler
    }

    /// Run the Blueprint's flow to completion and return the final
    /// `ctx`.
    ///
    /// Failure paths:
    ///
    /// - `compiler.compile` failure → `TaskLaunchError::Compile`.
    /// - `engine.attach` failure → `TaskLaunchError::Engine`.
    /// - A `Step` inside `flow eval` producing a dispatcher error, or
    ///   a sub-flow raising, → `TaskLaunchError::FlowEval`. There is
    ///   no silent partial-success completion; failures always
    ///   propagate.
    pub async fn launch(
        &self,
        input: TaskLaunchInput,
    ) -> Result<TaskLaunchOutput, TaskLaunchError> {
        // After the stateless-executor refactor, the
        // caller (Service) does compile + link +
        // `EngineDispatcher::with_spawner` itself; the engine no longer
        // holds any global spawner state to touch. The link path (base
        // `SpawnerAdapter` +
        // `LayerRegistry` resolution + `SpawnerStack` wrapping) is
        // concentrated inside `service::linker::link` — Service
        // scatter is intentionally prevented.
        let compiled = self.compiler.compile(&input.blueprint)?;
        let spawner = linker::link(
            compiled.router.clone(),
            &input.blueprint.spawner_hints.layers,
            &self.engine,
        );
        // GH #20 Contract C: materialize an `AgentContextView` exactly
        // once per spawn, innermost relative to every other layer below
        // (alias / worker-binding / task-input all insert `ctx.meta.runtime`
        // keys this layer must observe, so it is added FIRST — later
        // `.layer()` calls become outer, see `middleware::SpawnerStack`).
        // Unconditional (always layered): every Blueprint gets this layer
        // even when it declares no agent-context supply tiers at all
        // (`derive_agent_ctx` / `derive_context_policies` both return
        // empty state then, matching the pre-#21 `AgentContextMiddleware`
        // `Default` behavior byte-for-byte). GH #21 Phase 1: the
        // receptacle named in the #20 comment above is now wired —
        // `Blueprint.default_agent_ctx` / `default_context_policy` and
        // `AgentMeta.ctx` / `context_policy` feed this layer's merge +
        // policy resolution (see `middleware::agent_context`'s module doc
        // for the full narrative).
        let (agent_ctx_global, agent_ctx_per_agent) = derive_agent_ctx(&input.blueprint);
        let (context_policy_default, context_policy_per_agent) =
            derive_context_policies(&input.blueprint);
        let spawner = SpawnerStack::new(spawner)
            .layer(AgentContextMiddleware::new(
                agent_ctx_global,
                agent_ctx_per_agent,
                context_policy_default,
                context_policy_per_agent,
            ))
            .build();
        // When `Blueprint.metadata.project_name_alias` is Some, layer a
        // `ProjectNameAliasMiddleware` on top of the stack that injects the
        // alias into `Ctx.meta.runtime.project_name_alias` just before spawn.
        // Downstream operators (for example, the server crate's
        // `Operator.execute`) read `ctx.meta.runtime.get("project_name_alias")`
        // and expand it into the Spawn directive prompt body.
        let spawner = if let Some(alias) = input.blueprint.metadata.project_name_alias.as_deref() {
            SpawnerStack::new(spawner)
                .layer(ProjectNameAliasMiddleware::new(alias))
                .build()
        } else {
            spawner
        };
        // Layer the Blueprint-baked worker bindings (same ctx.meta.runtime
        // inject shape as the alias layer above) so the delegate axis can
        // resolve per-agent variants — see `derive_worker_bindings`.
        let worker_bindings = derive_worker_bindings(&input.blueprint);
        let spawner = if worker_bindings.is_empty() {
            spawner
        } else {
            SpawnerStack::new(spawner)
                .layer(WorkerBindingMiddleware::new(worker_bindings))
                .build()
        };
        // GH #34: Blueprint-declared after-run audit hooks — same
        // conditional-layering shape as the alias / worker-binding blocks
        // above. Empty `Blueprint.audits` (every pre-#34 Blueprint) means
        // no layer at all (invariant #4: byte-identical behavior). The
        // router handle handed to `AfterRunAuditMiddleware` is
        // `compiled.router` — the raw name→adapter table `Compiler::compile`
        // built (NOT this progressively-wrapped `spawner`) — so an audit
        // agent's own dispatch never re-enters this same layer (see
        // `AfterRunAuditMiddleware`'s module doc, Recursion guard section).
        let audit_defs = derive_audits(&input.blueprint);
        let spawner = if audit_defs.is_empty() {
            spawner
        } else {
            SpawnerStack::new(spawner)
                .layer(AfterRunAuditMiddleware::new(
                    audit_defs,
                    compiled.router.clone(),
                ))
                .build()
        };

        // Task-level execution context (`project_root` / `work_dir` /
        // `task_metadata`) — same conditional-layering shape as the alias /
        // worker-binding blocks above. Issue #19 ST2: read directly off
        // `input.task_input` (already resolved by the caller) instead of
        // extracting it back out of `input.init_ctx` — `init_ctx` is a pure
        // flow-ir eval seed now, never folded with these keys.
        let spawner = match input.task_input.as_ref().and_then(|spec| {
            TaskInputMiddleware::new_from_fields(
                spec.project_root.clone(),
                spec.work_dir.clone(),
                spec.task_metadata.clone(),
            )
        }) {
            Some(task_input) => SpawnerStack::new(spawner).layer(task_input).build(),
            None => spawner,
        };

        // "BP Agent-level" (`OperatorDef.kind` via `operator_ref`) + "BP
        // Global" (`Blueprint.default_operator_kind`) tiers of the
        // `OperatorKind` cascade, baked here (the only point that has both
        // the resolved Blueprint and the launch-time overrides in scope).
        let bp_agent_kinds = derive_bp_agent_kinds(&input.blueprint);
        let bp_global_kind = input
            .blueprint
            .default_operator_kind
            .map(OperatorKind::from);

        let token = self
            .engine
            .attach_with_ids(
                input.operator_id,
                input.role,
                input.ttl,
                input.operator_kind,
                input.bridge_id,
                input.hook_id,
                input.operator_backend_id,
                input.operator_kind_overrides,
                bp_agent_kinds,
                bp_global_kind,
            )
            .await?;
        let dispatcher =
            EngineDispatcher::with_spawner(self.engine.clone(), token.clone(), spawner);
        let dispatcher = match input.run_ctx {
            Some(run_ctx) => dispatcher.with_run(run_ctx),
            None => dispatcher,
        };
        // GH #21 Phase 2: attach the Step tier's named `MetaDef` pool.
        // Unconditional — an empty map (every pre-#21-Phase-2 Blueprint)
        // is a no-op, matching `EngineDispatcher::with_spawner`'s default.
        let dispatcher = dispatcher.with_step_metas(derive_step_metas(&input.blueprint));
        // GH #23: attach the `StepNaming` table `Compiler::compile` already
        // built once for this Blueprint (the sole construction site — see
        // `core::step_naming::StepNaming::from_blueprint`'s doc).
        // Unconditional — every compile produces one, undeclared Blueprints
        // included (canonical falls back to `Step.ref` byte-for-byte).
        let dispatcher = dispatcher.with_step_naming(compiled.step_naming.clone());
        // GH #27 (follow-up to #23): attach the `ProjectionPlacement`
        // resolver `Compiler::compile` already built once for this
        // Blueprint (the sole construction site — see
        // `core::projection_placement::ProjectionPlacement::from_spec`'s
        // doc). Unconditional — every compile produces one, undeclared
        // Blueprints included (resolves to `ProjectionPlacement::default()`).
        let dispatcher =
            dispatcher.with_projection_placement(compiled.projection_placement.clone());
        // Issue #19 ST3: BP default + Task init_ctx → merged init_ctx (the
        // 2-layer slice of the eventual 4-layer cascade; Run override is
        // ST4 carry). `input.blueprint.default_init_ctx` is `None` for
        // every pre-#19 Blueprint, so `merge_init_ctx` is a no-op then and
        // this preserves today's behavior byte-for-byte.
        let merged_init_ctx =
            merge_init_ctx(input.blueprint.default_init_ctx.as_ref(), &input.init_ctx);
        let final_ctx = mlua_flow_ir::eval_async_externs(
            &input.blueprint.flow,
            merged_init_ctx,
            &dispatcher,
            &*self.externs,
        )
        .await
        .map_err(|e| TaskLaunchError::FlowEval(e.to_string()))?;
        Ok(TaskLaunchOutput { token, final_ctx })
    }
}

// ──────────────────────────────────────────────────────────────────────────
// UT
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blueprint::compiler::{RustFnInProcessSpawnerFactory, SpawnerRegistry};
    use crate::blueprint::{
        current_schema_version, AgentDef, AgentKind, AgentMeta, BlueprintMetadata, CompilerHints,
        CompilerStrategy, MetaDef,
    };
    use crate::core::config::EngineCfg;
    use crate::worker::adapter::{WorkerError, WorkerResult};
    use mlua_flow_ir::{Expr, JoinMode, Node as FlowNode};
    use serde_json::json;
    use std::sync::Arc;

    fn path(s: &str) -> Expr {
        Expr::Path {
            at: s.parse().expect("literal test path"),
        }
    }
    fn step(ref_: &str, in_: Expr, out: Expr) -> FlowNode {
        FlowNode::Step {
            ref_: ref_.to_string(),
            in_,
            out,
        }
    }

    fn agent(name: &str, fn_id: &str) -> AgentDef {
        AgentDef {
            name: name.to_string(),
            kind: AgentKind::RustFn,
            spec: json!({ "fn_id": fn_id }),
            profile: None,
            meta: Some(AgentMeta::default()),
        }
    }

    fn build_service(factory: RustFnInProcessSpawnerFactory) -> TaskLaunchService {
        let engine = Engine::new(EngineCfg::default());
        let mut reg = SpawnerRegistry::new();
        reg.register::<RustFnInProcessSpawnerFactory>(Arc::new(factory));
        let compiler = Compiler::new(reg);
        TaskLaunchService::new(engine, compiler)
    }

    fn bp(flow: FlowNode, agents: Vec<AgentDef>) -> Blueprint {
        Blueprint {
            schema_version: current_schema_version(),
            id: "ut".into(),
            flow,
            agents,
            operators: vec![],
            metas: vec![],
            hints: CompilerHints::default(),
            strategy: CompilerStrategy::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx: None,
            default_context_policy: None,
            projection_placement: None,
            audits: vec![],
            degradation_policy: None,
        }
    }

    fn launch_input(blueprint: Blueprint, init_ctx: Value) -> TaskLaunchInput {
        TaskLaunchInput::automate(
            blueprint,
            "ut-op",
            Role::Operator,
            Duration::from_secs(30),
            init_ctx,
        )
    }

    // ──────────────────────────────────────────────────────────────
    // GH #34: `derive_audits` + the conditional `AfterRunAuditMiddleware`
    // `.layer(...)` wiring in `TaskLaunchService::launch`
    // ──────────────────────────────────────────────────────────────

    #[test]
    fn derive_audits_empty_by_default() {
        let blueprint = bp(
            step("echo", path("$.input"), path("$.out")),
            vec![agent("echo", "echo")],
        );
        assert!(
            derive_audits(&blueprint).is_empty(),
            "audits_absent_no_layer: an undeclared audits Vec must stay empty"
        );
    }

    #[test]
    fn derive_audits_returns_blueprint_audits_verbatim() {
        let mut blueprint = bp(
            step("echo", path("$.input"), path("$.out")),
            vec![agent("echo", "echo")],
        );
        blueprint.audits = vec![crate::blueprint::AuditDef {
            agent: "auditor".to_string(),
            steps: None,
            mode: crate::blueprint::AuditMode::Async,
        }];
        let got = derive_audits(&blueprint);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].agent, "auditor");
    }

    #[tokio::test]
    async fn launch_appends_audit_artifact_when_audits_declared() {
        use crate::blueprint::{AuditDef, AuditMode};

        let factory = RustFnInProcessSpawnerFactory::new()
            .register_fn("echo", |inv| async move {
                Ok(WorkerResult {
                    value: json!({ "echoed": inv.prompt }),
                    ok: true,
                })
            })
            .register_fn("audit-fn", |_inv| async move {
                Ok(WorkerResult {
                    value: json!({ "finding": "clean" }),
                    ok: true,
                })
            });
        let svc = build_service(factory);
        let mut blueprint = bp(
            step("echo", path("$.input"), path("$.out")),
            vec![agent("echo", "echo"), agent("auditor", "audit-fn")],
        );
        blueprint.audits = vec![AuditDef {
            agent: "auditor".to_string(),
            steps: None,
            mode: AuditMode::Sync,
        }];
        let out = svc
            .launch(launch_input(blueprint, json!({ "input": "hi" })))
            .await
            .expect("launch ok — audits must never alter the audited step's outcome");
        assert_eq!(out.final_ctx["out"]["echoed"], "hi");

        let audited_task_id = svc
            .engine()
            .with_state("test.find_audited_task", |s| {
                s.tasks
                    .iter()
                    .find(|(_, t)| t.spec.agent == "echo")
                    .map(|(id, _)| id.clone())
            })
            .await
            .expect("with_state")
            .expect("the echo task must exist");
        let tail = svc.engine().output_tail(&audited_task_id, 1).await;
        let found = tail.iter().any(|ev| {
            matches!(
                ev,
                crate::worker::output::OutputEvent::Artifact { name, .. } if name == "audit:echo"
            )
        });
        assert!(
            found,
            "launch() must wire AfterRunAuditMiddleware end-to-end when Blueprint.audits is declared"
        );
    }

    #[tokio::test]
    async fn launch_single_step_writes_out_path() {
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", |inv| async move {
            Ok(WorkerResult {
                value: json!({ "echoed": inv.prompt }),
                ok: true,
            })
        });
        let svc = build_service(factory);
        let blueprint = bp(
            step("echo", path("$.input"), path("$.out")),
            vec![agent("echo", "echo")],
        );
        let out = svc
            .launch(launch_input(blueprint, json!({ "input": "hi" })))
            .await
            .expect("launch ok");
        assert_eq!(out.final_ctx["out"]["echoed"], "hi");
    }

    #[tokio::test]
    async fn launch_three_step_seq_threads_ctx_forward() {
        let factory = RustFnInProcessSpawnerFactory::new()
            .register_fn("upper", |inv| async move {
                let s = serde_json::from_str::<String>(&inv.prompt).unwrap_or(inv.prompt);
                Ok(WorkerResult {
                    value: json!(s.to_uppercase()),
                    ok: true,
                })
            })
            .register_fn("suffix", |inv| async move {
                let s = serde_json::from_str::<String>(&inv.prompt).unwrap_or(inv.prompt);
                Ok(WorkerResult {
                    value: json!(format!("{s}!")),
                    ok: true,
                })
            })
            .register_fn("wrap", |inv| async move {
                let s = serde_json::from_str::<String>(&inv.prompt).unwrap_or(inv.prompt);
                Ok(WorkerResult {
                    value: json!(format!("[{s}]")),
                    ok: true,
                })
            });
        let svc = build_service(factory);
        let flow = FlowNode::Seq {
            children: vec![
                step("upper", path("$.in"), path("$.s1")),
                step("suffix", path("$.s1"), path("$.s2")),
                step("wrap", path("$.s2"), path("$.s3")),
            ],
        };
        let blueprint = bp(
            flow,
            vec![
                agent("upper", "upper"),
                agent("suffix", "suffix"),
                agent("wrap", "wrap"),
            ],
        );
        let out = svc
            .launch(launch_input(blueprint, json!({ "in": "hello" })))
            .await
            .expect("launch ok");
        assert_eq!(out.final_ctx["s1"], "HELLO");
        assert_eq!(out.final_ctx["s2"], "HELLO!");
        assert_eq!(out.final_ctx["s3"], "[HELLO!]");
    }

    #[tokio::test]
    async fn launch_fanout_join_all_parallel_completes() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let counter = Arc::new(AtomicU32::new(0));
        let max_seen = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();
        let max_clone = max_seen.clone();

        // Each worker bumps the inflight counter up, sleeps 50ms, then bumps it down.
        // When parallel execution is working, max inflight exceeds 1.
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("para", move |inv| {
            let counter = counter_clone.clone();
            let max_seen = max_clone.clone();
            async move {
                let now = counter.fetch_add(1, Ordering::SeqCst) + 1;
                let mut prev = max_seen.load(Ordering::SeqCst);
                while now > prev {
                    match max_seen.compare_exchange(prev, now, Ordering::SeqCst, Ordering::SeqCst) {
                        Ok(_) => break,
                        Err(p) => prev = p,
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                counter.fetch_sub(1, Ordering::SeqCst);
                let s = serde_json::from_str::<String>(&inv.prompt).unwrap_or(inv.prompt);
                Ok(WorkerResult {
                    value: json!(format!("did:{s}")),
                    ok: true,
                })
            }
        });
        let svc = build_service(factory);
        let flow = FlowNode::Fanout {
            items: path("$.items"),
            bind: path("$.item"),
            body: Box::new(step("para", path("$.item"), path("$.r"))),
            join: JoinMode::All,
            out: path("$.results"),
        };
        let blueprint = bp(flow, vec![agent("para", "para")]);
        let out = svc
            .launch(launch_input(
                blueprint,
                json!({ "items": ["a", "b", "c", "d"] }),
            ))
            .await
            .expect("launch ok");
        let results = out.final_ctx["results"].as_array().expect("array");
        assert_eq!(results.len(), 4);
        for (i, expected) in ["a", "b", "c", "d"].iter().enumerate() {
            assert_eq!(results[i]["r"], json!(format!("did:{expected}")));
        }
        let max = max_seen.load(Ordering::SeqCst);
        assert!(
            max >= 2,
            "expected parallel execution (max inflight >= 2), got {max}"
        );
    }

    #[tokio::test]
    async fn launch_propagates_worker_error_as_flow_eval_err() {
        let factory = RustFnInProcessSpawnerFactory::new()
            .register_fn("ok", |inv| async move {
                Ok(WorkerResult {
                    value: json!(inv.prompt),
                    ok: true,
                })
            })
            .register_fn("boom", |_inv| async move {
                Err(WorkerError::Failed("intentional boom".into()))
            });
        let svc = build_service(factory);
        let flow = FlowNode::Seq {
            children: vec![
                step("ok", path("$.input"), path("$.s1")),
                step("boom", path("$.s1"), path("$.s2")),
                step("ok", path("$.s2"), path("$.s3")),
            ],
        };
        let blueprint = bp(flow, vec![agent("ok", "ok"), agent("boom", "boom")]);
        let err = svc
            .launch(launch_input(blueprint, json!({ "input": "x" })))
            .await
            .expect_err("expected fail");
        match err {
            TaskLaunchError::FlowEval(msg) => {
                assert!(
                    msg.contains("boom") || msg.contains("intentional"),
                    "expected error to mention worker failure, got: {msg}"
                );
            }
            other => panic!("expected FlowEval error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn launch_resolves_call_extern_via_registered_externs() {
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", |inv| async move {
            Ok(WorkerResult {
                value: json!({ "echoed": inv.prompt }),
                ok: true,
            })
        });
        let mut externs = mlua_flow_ir::ExternMap::new();
        externs.register("fmt.greet", |args: &[Value]| {
            let name = args[0].as_str().unwrap_or("?");
            Ok(json!(format!("hello, {name}")))
        });
        let svc = build_service(factory).with_externs(Arc::new(externs));
        let flow = step(
            "echo",
            Expr::CallExtern {
                ref_: "fmt.greet".into(),
                args: vec![path("$.who")],
            },
            path("$.out"),
        );
        let blueprint = bp(flow, vec![agent("echo", "echo")]);
        let out = svc
            .launch(launch_input(blueprint, json!({ "who": "swarm" })))
            .await
            .expect("launch ok");
        assert_eq!(out.final_ctx["out"]["echoed"], json!("hello, swarm"));
    }

    #[tokio::test]
    async fn launch_call_extern_without_registry_fails_as_flow_eval() {
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", |inv| async move {
            Ok(WorkerResult {
                value: json!(inv.prompt),
                ok: true,
            })
        });
        let svc = build_service(factory); // default NoExterns
        let flow = step(
            "echo",
            Expr::CallExtern {
                ref_: "fmt.greet".into(),
                args: vec![],
            },
            path("$.out"),
        );
        let blueprint = bp(flow, vec![agent("echo", "echo")]);
        let err = svc
            .launch(launch_input(blueprint, json!({})))
            .await
            .expect_err("expected fail");
        match err {
            TaskLaunchError::FlowEval(msg) => {
                assert!(msg.contains("extern"), "expected extern error, got: {msg}");
            }
            other => panic!("expected FlowEval error, got {other:?}"),
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // issue #13 run_id propagation (`TaskLaunchInput.run_ctx`)
    // ──────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn launch_with_run_ctx_appends_one_step_entry_per_dispatched_step() {
        use crate::store::run::{InMemoryRunStore, RunContext, RunRecord, RunStatus, RunStore};
        use crate::types::{RunId, TaskId};

        let factory = RustFnInProcessSpawnerFactory::new()
            .register_fn("upper", |inv| async move {
                Ok(WorkerResult {
                    value: json!(inv.prompt.to_uppercase()),
                    ok: true,
                })
            })
            .register_fn("suffix", |inv| async move {
                let s = serde_json::from_str::<String>(&inv.prompt).unwrap_or(inv.prompt);
                Ok(WorkerResult {
                    value: json!(format!("{s}!")),
                    ok: true,
                })
            });
        let svc = build_service(factory);
        let flow = FlowNode::Seq {
            children: vec![
                step("upper", path("$.in"), path("$.s1")),
                step("suffix", path("$.s1"), path("$.s2")),
            ],
        };
        let blueprint = bp(
            flow,
            vec![agent("upper", "upper"), agent("suffix", "suffix")],
        );

        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let run_id = RunId::new();
        run_store
            .create(RunRecord {
                id: run_id.clone(),
                task_id: TaskId::new(),
                status: RunStatus::Running,
                step_entries: Vec::new(),
                degradations: Vec::new(),
                operator_sid: None,
                result_ref: None,
                created_at: 0,
                updated_at: 0,
            })
            .await
            .expect("seed RunRecord");

        let mut input = launch_input(blueprint, json!({ "in": "hi" }));
        input.run_ctx = Some(RunContext {
            run_id: run_id.clone(),
            run_store: run_store.clone(),
        });

        let out = svc.launch(input).await.expect("launch ok");
        assert_eq!(out.final_ctx["s2"], "HI!");

        let run = run_store.get(&run_id).await.expect("run present");
        assert_eq!(
            run.step_entries.len(),
            2,
            "expected one step_entry per dispatched step, got {:?}",
            run.step_entries
        );
        assert_eq!(run.step_entries[0].step_ref, Some("upper".to_string()));
        assert_eq!(run.step_entries[0].status, Some("passed".to_string()));
        assert_eq!(run.step_entries[1].step_ref, Some("suffix".to_string()));
        assert_eq!(run.step_entries[1].status, Some("passed".to_string()));
    }

    #[tokio::test]
    async fn launch_without_run_ctx_appends_no_step_entries() {
        // `run_ctx: None` (the `automate()` default) must not touch any
        // `RunStore` — this is the pre-existing no-tracing behavior, kept
        // as a regression guard alongside the `Some` case above.
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", |inv| async move {
            Ok(WorkerResult {
                value: json!(inv.prompt),
                ok: true,
            })
        });
        let svc = build_service(factory);
        let blueprint = bp(
            step("echo", path("$.input"), path("$.out")),
            vec![agent("echo", "echo")],
        );
        let input = launch_input(blueprint, json!({ "input": "hi" }));
        assert!(
            input.run_ctx.is_none(),
            "automate() defaults run_ctx to None"
        );
        let out = svc.launch(input).await.expect("launch ok");
        assert_eq!(out.final_ctx["out"], "hi");
    }

    // ──────────────────────────────────────────────────────────────────
    // issue #19 ST2: `TaskLaunchInput.task_input` (direct-sibling-read
    // replacement for the ST1 `from_init_ctx(&input.init_ctx)` call)
    // ──────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn launch_with_task_input_leaves_init_ctx_object_seed_unmutated() {
        // Issue #19 ST2 invariant: `init_ctx` is a pure flow-ir eval seed —
        // `task_input` must not be folded into it. Regression guard for the
        // ST1 `resolve_task_level_init_ctx` fold-back this subtask removes:
        // if it ever crept back in here, `project_root` / `work_dir` /
        // `task_metadata` would leak into `final_ctx` as extra top-level
        // keys nobody wrote via a `Step.out`.
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", |inv| async move {
            Ok(WorkerResult {
                value: json!({ "echoed": inv.prompt }),
                ok: true,
            })
        });
        let svc = build_service(factory);
        let blueprint = bp(
            step("echo", path("$.input"), path("$.out")),
            vec![agent("echo", "echo")],
        );
        let mut input = launch_input(blueprint, json!({ "input": "hi" }));
        input.task_input = Some(TaskInputSpec {
            project_root: Some("/repo".to_string()),
            work_dir: Some("/repo/work".to_string()),
            task_metadata: Some(json!({ "issue": 19 })),
        });
        let out = svc.launch(input).await.expect("launch ok");
        assert_eq!(out.final_ctx["out"]["echoed"], "hi");
        assert!(
            out.final_ctx.get("project_root").is_none(),
            "task_input must not be folded into the flow-ir ctx seed, got {:?}",
            out.final_ctx
        );
        assert!(out.final_ctx.get("work_dir").is_none());
        assert!(out.final_ctx.get("task_metadata").is_none());
    }

    #[tokio::test]
    async fn launch_with_task_input_none_is_a_no_op() {
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", |inv| async move {
            Ok(WorkerResult {
                value: json!(inv.prompt),
                ok: true,
            })
        });
        let svc = build_service(factory);
        let blueprint = bp(
            step("echo", path("$.input"), path("$.out")),
            vec![agent("echo", "echo")],
        );
        let mut input = launch_input(blueprint, json!({ "input": "hi" }));
        assert!(input.task_input.is_none(), "automate() defaults to None");
        input.task_input = None;
        let out = svc.launch(input).await.expect("launch ok");
        assert_eq!(out.final_ctx["out"], "hi");
    }

    #[tokio::test]
    async fn launch_with_task_input_all_fields_absent_is_a_no_op() {
        // `Some(TaskInputSpec::default())` — outer Some, all 3 inner fields
        // None — must behave identically to `task_input: None` (mirrors
        // `TaskInputMiddleware::new_from_fields`'s own no-op contract).
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", |inv| async move {
            Ok(WorkerResult {
                value: json!(inv.prompt),
                ok: true,
            })
        });
        let svc = build_service(factory);
        let blueprint = bp(
            step("echo", path("$.input"), path("$.out")),
            vec![agent("echo", "echo")],
        );
        let mut input = launch_input(blueprint, json!({ "input": "hi" }));
        input.task_input = Some(TaskInputSpec::default());
        let out = svc.launch(input).await.expect("launch ok");
        assert_eq!(out.final_ctx["out"], "hi");
    }

    // ──────────────────────────────────────────────────────────────────
    // issue #19 ST3: `merge_init_ctx` (BP default + Task init_ctx)
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn merge_init_ctx_bp_default_only_passes_through_when_task_is_empty_object() {
        let bp_default = json!({ "seeded": "from-bp" });
        let task = json!({});
        let merged = merge_init_ctx(Some(&bp_default), &task);
        assert_eq!(merged, json!({ "seeded": "from-bp" }));
    }

    #[test]
    fn merge_init_ctx_task_only_passes_through_when_bp_default_is_empty_object() {
        let bp_default = json!({});
        let task = json!({ "seeded": "from-task" });
        let merged = merge_init_ctx(Some(&bp_default), &task);
        assert_eq!(merged, json!({ "seeded": "from-task" }));
    }

    #[test]
    fn merge_init_ctx_both_objects_task_wins_on_key_collision() {
        let bp_default = json!({ "a": "bp", "b": "bp-only" });
        let task = json!({ "a": "task", "c": "task-only" });
        let merged = merge_init_ctx(Some(&bp_default), &task);
        assert_eq!(
            merged,
            json!({ "a": "task", "b": "bp-only", "c": "task-only" })
        );
    }

    #[test]
    fn merge_init_ctx_non_object_task_fully_replaces_bp_default() {
        let bp_default = json!({ "seeded": "from-bp" });
        let task = json!("plain-string-seed");
        let merged = merge_init_ctx(Some(&bp_default), &task);
        assert_eq!(merged, json!("plain-string-seed"));
    }

    #[test]
    fn merge_init_ctx_no_bp_default_is_a_no_op() {
        let task = json!({ "input": "hi" });
        let merged = merge_init_ctx(None, &task);
        assert_eq!(merged, task);
    }

    // ──────────────────────────────────────────────────────────────────
    // issue #19 ST4: `merge_init_ctx_3layer` (BP default + Task + Run)
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn merge_init_ctx_3layer_no_run_override_equals_bp_task_merge_only() {
        // `run_override: None` must be a pure pass-through of the BP+Task
        // merge — this is the `POST /v1/tasks/:id/runs` no-body rekick
        // path, which must preserve pre-#19 behavior byte-for-byte.
        let bp_default = json!({ "a": "bp", "b": "bp-only" });
        let task = json!({ "a": "task", "c": "task-only" });
        let three_layer = merge_init_ctx_3layer(Some(&bp_default), &task, None);
        let two_layer = merge_init_ctx(Some(&bp_default), &task);
        assert_eq!(three_layer, two_layer);
        assert_eq!(
            three_layer,
            json!({ "a": "task", "b": "bp-only", "c": "task-only" })
        );
    }

    #[test]
    fn merge_init_ctx_3layer_run_object_wins_on_key_collision_over_bp_and_task() {
        let bp_default = json!({ "a": "bp", "b": "bp-only" });
        let task = json!({ "a": "task", "c": "task-only" });
        let run_override = json!({ "a": "run", "d": "run-only" });
        let merged = merge_init_ctx_3layer(Some(&bp_default), &task, Some(&run_override));
        assert_eq!(
            merged,
            json!({ "a": "run", "b": "bp-only", "c": "task-only", "d": "run-only" }),
            "Run wins on collision (a); BP-only (b) and Task-only (c) keys survive"
        );
    }

    #[test]
    fn merge_init_ctx_3layer_run_non_object_fully_replaces_bp_task_merge() {
        let bp_default = json!({ "seeded": "from-bp" });
        let task = json!({ "seeded": "from-task" });
        let run_override = json!("plain-string-run-seed");
        let merged = merge_init_ctx_3layer(Some(&bp_default), &task, Some(&run_override));
        assert_eq!(merged, json!("plain-string-run-seed"));
    }

    #[test]
    fn merge_init_ctx_3layer_no_bp_default_and_no_run_override_is_task_passthrough() {
        let task = json!({ "input": "hi" });
        let merged = merge_init_ctx_3layer(None, &task, None);
        assert_eq!(merged, task);
    }

    #[tokio::test]
    async fn launch_merges_bp_default_init_ctx_into_task_init_ctx() {
        // End-to-end guard: `Blueprint.default_init_ctx` actually reaches
        // `eval_async_externs` — not merely unit-tested in isolation.
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", |inv| async move {
            Ok(WorkerResult {
                value: json!(inv.prompt),
                ok: true,
            })
        });
        let svc = build_service(factory);
        let mut blueprint = bp(
            step("echo", path("$.greeting"), path("$.out")),
            vec![agent("echo", "echo")],
        );
        blueprint.default_init_ctx = Some(json!({ "greeting": "hello from bp" }));
        // Task supplies an empty object — BP default alone seeds `$.greeting`.
        let out = svc
            .launch(launch_input(blueprint, json!({})))
            .await
            .expect("launch ok");
        assert_eq!(out.final_ctx["out"], "hello from bp");
    }

    // ──────────────────────────────────────────────────────────────────
    // issue #21 Phase 1: `derive_agent_ctx` / `derive_context_policies`
    // ──────────────────────────────────────────────────────────────────

    fn agent_with_meta(name: &str, fn_id: &str, meta: AgentMeta) -> AgentDef {
        AgentDef {
            name: name.to_string(),
            kind: AgentKind::RustFn,
            spec: json!({ "fn_id": fn_id }),
            profile: None,
            meta: Some(meta),
        }
    }

    #[test]
    fn derive_agent_ctx_empty_blueprint_yields_empty_state() {
        let blueprint = bp(step("echo", path("$.in"), path("$.out")), vec![]);
        let (global, per_agent) = derive_agent_ctx(&blueprint);
        assert_eq!(global, None);
        assert!(per_agent.is_empty());
    }

    #[test]
    fn derive_agent_ctx_populated_blueprint_yields_correct_maps() {
        let mut blueprint = bp(
            step("echo", path("$.in"), path("$.out")),
            vec![
                agent_with_meta(
                    "with-ctx",
                    "echo",
                    AgentMeta {
                        ctx: Some(json!({ "org_conventions": "x" })),
                        ..Default::default()
                    },
                ),
                agent("no-ctx", "echo"),
            ],
        );
        blueprint.default_agent_ctx = Some(json!({ "seeded": "from-bp" }));
        let (global, per_agent) = derive_agent_ctx(&blueprint);
        assert_eq!(global, Some(json!({ "seeded": "from-bp" })));
        assert_eq!(
            per_agent.len(),
            1,
            "agents without AgentMeta.ctx are absent, not defaulted to null: {per_agent:?}"
        );
        assert_eq!(
            per_agent.get("with-ctx"),
            Some(&json!({ "org_conventions": "x" }))
        );
        assert!(!per_agent.contains_key("no-ctx"));
    }

    #[test]
    fn derive_context_policies_empty_blueprint_yields_empty_state() {
        let blueprint = bp(step("echo", path("$.in"), path("$.out")), vec![]);
        let (default_policy, per_agent) = derive_context_policies(&blueprint);
        assert_eq!(default_policy, None);
        assert!(per_agent.is_empty());
    }

    #[test]
    fn derive_context_policies_populated_blueprint_yields_correct_maps() {
        let mut blueprint = bp(
            step("echo", path("$.in"), path("$.out")),
            vec![
                agent_with_meta(
                    "with-policy",
                    "echo",
                    AgentMeta {
                        context_policy: Some(ContextPolicy {
                            include: None,
                            exclude: vec!["work_dir".to_string()],
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ),
                agent("no-policy", "echo"),
            ],
        );
        blueprint.default_context_policy = Some(ContextPolicy {
            include: Some(vec!["project_root".to_string()]),
            exclude: vec![],
            ..Default::default()
        });
        let (default_policy, per_agent) = derive_context_policies(&blueprint);
        assert_eq!(
            default_policy,
            Some(ContextPolicy {
                include: Some(vec!["project_root".to_string()]),
                exclude: vec![],
                ..Default::default()
            })
        );
        assert_eq!(per_agent.len(), 1);
        assert_eq!(
            per_agent.get("with-policy"),
            Some(&ContextPolicy {
                include: None,
                exclude: vec!["work_dir".to_string()],
                ..Default::default()
            })
        );
        assert!(!per_agent.contains_key("no-policy"));
    }

    // ──────────────────────────────────────────────────────────────────
    // issue #21 Phase 2: `derive_step_metas` / `AgentMeta.meta_ref`
    // resolution inside `derive_agent_ctx`
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn derive_step_metas_empty_blueprint_yields_empty_map() {
        let blueprint = bp(step("echo", path("$.in"), path("$.out")), vec![]);
        assert!(derive_step_metas(&blueprint).is_empty());
    }

    #[test]
    fn derive_step_metas_populated_blueprint_yields_name_to_ctx_map() {
        let mut blueprint = bp(step("echo", path("$.in"), path("$.out")), vec![]);
        blueprint.metas = vec![
            MetaDef {
                name: "heavy-scan".to_string(),
                ctx: json!({ "work_dir": "/x" }),
            },
            MetaDef {
                name: "light-scan".to_string(),
                ctx: json!({ "work_dir": "/y" }),
            },
        ];
        let metas = derive_step_metas(&blueprint);
        assert_eq!(metas.len(), 2);
        assert_eq!(metas.get("heavy-scan"), Some(&json!({ "work_dir": "/x" })));
        assert_eq!(metas.get("light-scan"), Some(&json!({ "work_dir": "/y" })));
    }

    #[test]
    fn derive_agent_ctx_meta_ref_resolves_as_base_under_inline_ctx() {
        let mut blueprint = bp(
            step("echo", path("$.in"), path("$.out")),
            vec![agent_with_meta(
                "with-meta-ref",
                "echo",
                AgentMeta {
                    ctx: Some(json!({ "work_dir": "/inline-wins" })),
                    meta_ref: Some("shared".to_string()),
                    ..Default::default()
                },
            )],
        );
        blueprint.metas = vec![MetaDef {
            name: "shared".to_string(),
            ctx: json!({ "work_dir": "/base", "extra": "from-pool" }),
        }];
        let (_, per_agent) = derive_agent_ctx(&blueprint);
        assert_eq!(
            per_agent.get("with-meta-ref"),
            Some(&json!({ "work_dir": "/inline-wins", "extra": "from-pool" })),
            "inline ctx must win the collided key while pool-only keys survive the merge"
        );
    }

    #[test]
    fn derive_agent_ctx_meta_ref_alone_uses_pool_ctx_verbatim() {
        let mut blueprint = bp(
            step("echo", path("$.in"), path("$.out")),
            vec![agent_with_meta(
                "with-meta-ref-only",
                "echo",
                AgentMeta {
                    meta_ref: Some("shared".to_string()),
                    ..Default::default()
                },
            )],
        );
        blueprint.metas = vec![MetaDef {
            name: "shared".to_string(),
            ctx: json!({ "work_dir": "/base" }),
        }];
        let (_, per_agent) = derive_agent_ctx(&blueprint);
        assert_eq!(
            per_agent.get("with-meta-ref-only"),
            Some(&json!({ "work_dir": "/base" }))
        );
    }

    #[test]
    fn derive_agent_ctx_unresolved_meta_ref_never_panics_and_falls_back_to_inline() {
        let blueprint = bp(
            step("echo", path("$.in"), path("$.out")),
            vec![agent_with_meta(
                "with-unresolved-meta-ref",
                "echo",
                AgentMeta {
                    ctx: Some(json!({ "work_dir": "/inline-only" })),
                    meta_ref: Some("missing".to_string()),
                    ..Default::default()
                },
            )],
        );
        // No `blueprint.metas` entries at all — `meta_ref` unresolved.
        let (_, per_agent) = derive_agent_ctx(&blueprint);
        assert_eq!(
            per_agent.get("with-unresolved-meta-ref"),
            Some(&json!({ "work_dir": "/inline-only" })),
            "an unresolved meta_ref must never panic; the agent's own inline ctx still applies"
        );
    }
}
