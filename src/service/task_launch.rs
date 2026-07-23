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

use crate::binding::{
    attest_bound_agents, binding_requests, validate_bound_agent_snapshots, AgentBindingProvider,
    LegacyWorkerBindingPolicy, UnboundAgent,
};
use crate::blueprint::compiler::{materialize_bound_blueprint, CompileError, Compiler};
use crate::blueprint::{
    resolve_bound_agents, AuditDef, Blueprint, BoundAgent, EngineDispatcher, Runner,
};
use crate::core::agent_context::ContextPolicy;
use crate::core::config::CheckPolicy;
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
#[cfg(test)]
pub(crate) fn derive_worker_bindings(blueprint: &Blueprint) -> HashMap<String, WorkerBinding> {
    // Kept as a test-facing compatibility name. Production resolves once
    // and calls `worker_bindings_from_bound_agents` with that snapshot.
    let bound_agents = resolve_bound_agents(blueprint)
        .expect("derive_worker_bindings requires a Blueprint with resolvable Runner refs");
    worker_bindings_from_bound_agents(&bound_agents)
}

fn worker_bindings_from_bound_agents(
    bound_agents: &[BoundAgent],
) -> HashMap<String, WorkerBinding> {
    bound_agents
        .iter()
        .filter_map(|bound| match &bound.runner {
            Some(Runner::WsOperator { variant, tools })
            | Some(Runner::WsClaudeCode { variant, tools }) => Some((
                bound.agent.name.clone(),
                WorkerBinding {
                    variant: variant.clone(),
                    tools: tools.clone(),
                    request_digest: Some(bound.binding_digest.clone()),
                    requested_model: bound.agent.profile.as_ref().and_then(|p| p.model.clone()),
                },
            )),
            _ => None,
        })
        .collect()
}

/// Attest a freshly resolved snapshot (or enforce the strict-without-provider
/// gate) exactly once, applied identically on both first-resolution paths in
/// [`load_or_resolve_bound_agents`].
///
/// `strict` = [`crate::blueprint::CompilerStrategy::strict_binding`]:
///
/// - With a provider: every `Bound` outcome is validated and pinned; any
///   `Unbound` agent fails the launch when `strict`, or (non-strict) is
///   reported through a `tracing::warn!` and, if a `RunContext` is present, a
///   `RunRecord.degradations` entry (the existing append-only channel). The
///   agent stays `DeclarationOnly` either way.
/// - Without a provider: a `strict` Blueprint that declares any Runner-backed
///   agent fails fast (`PreDispatch`) because nothing can attest it; a
///   non-strict Blueprint runs `DeclarationOnly` (the embed use case).
async fn attest_or_gate_fresh(
    bound_agents: &mut [BoundAgent],
    binding_provider: Option<&dyn AgentBindingProvider>,
    strict: bool,
    run_ctx: Option<&RunContext>,
) -> Result<(), TaskLaunchError> {
    match binding_provider {
        Some(provider) => {
            let unbound = attest_bound_agents(provider, bound_agents, strict)
                .await
                .map_err(|error| TaskLaunchError::PreDispatch(error.to_string()))?;
            for agent in &unbound {
                record_unbound_degradation(agent, run_ctx).await;
            }
            Ok(())
        }
        None => {
            if strict && !binding_requests(bound_agents).is_empty() {
                return Err(TaskLaunchError::PreDispatch(format!(
                    "strict_binding requires a binding provider but none is injected; \
                     {} Runner-backed agent(s) cannot be attested",
                    binding_requests(bound_agents).len()
                )));
            }
            Ok(())
        }
    }
}

/// Record one non-strict unattested agent: a `tracing::warn!` always, plus a
/// `RunRecord.degradations` append when a `RunContext` carries a run store.
/// Observational only — a failed append is itself logged and never fails the
/// launch (degradation recording must not gate a launch the strict decision
/// already let through).
async fn record_unbound_degradation(agent: &UnboundAgent, run_ctx: Option<&RunContext>) {
    tracing::warn!(
        agent = %agent.agent,
        reason = %agent.reason,
        "binding_unattested: agent runs DeclarationOnly (strict_binding is off)"
    );
    let Some(run_ctx) = run_ctx else {
        return;
    };
    let entry = crate::store::run::DegradationEntry {
        tool: "binding".to_string(),
        error: agent.reason.clone(),
        fallback: "DeclarationOnly".to_string(),
        note: Some(format!(
            "agent '{}' launched without a binding attestation (strict_binding off)",
            agent.agent
        )),
        step_ref: None,
        attempt: None,
        at: crate::types::now_unix(),
    };
    if let Err(error) = run_ctx
        .run_store
        .append_degradation(&run_ctx.run_id, entry)
        .await
    {
        tracing::warn!(
            agent = %agent.agent,
            %error,
            "binding_unattested: failed to record degradation entry"
        );
    }
}

async fn load_or_resolve_bound_agents(
    blueprint: &Blueprint,
    run_ctx: Option<&RunContext>,
    binding_provider: Option<&dyn AgentBindingProvider>,
    legacy_worker_binding_policy: LegacyWorkerBindingPolicy,
) -> Result<Vec<BoundAgent>, TaskLaunchError> {
    // Strict binding is a BP-level opt-in only (server config / launch
    // request cascade is intentionally out of scope for this change).
    let strict = blueprint.strategy.strict_binding;
    let resolve_fresh = || match legacy_worker_binding_policy {
        LegacyWorkerBindingPolicy::Allow => resolve_bound_agents(blueprint),
        LegacyWorkerBindingPolicy::Reject => {
            crate::blueprint::resolve_bound_agents_strict(blueprint)
        }
    };
    let Some(run_ctx) = run_ctx else {
        let mut bound_agents = resolve_fresh().map_err(CompileError::from)?;
        attest_or_gate_fresh(&mut bound_agents, binding_provider, strict, None).await?;
        return Ok(bound_agents);
    };

    let record = run_ctx
        .run_store
        .get(&run_ctx.run_id)
        .await
        .map_err(|e| TaskLaunchError::PreDispatch(format!("load Run binding snapshot: {e}")))?;
    if let Some(input_json) = record.input_json.as_deref() {
        let snapshot: Value = serde_json::from_str(input_json).map_err(|e| {
            TaskLaunchError::PreDispatch(format!("decode Run launch snapshot: {e}"))
        })?;
        if let Some(value) = snapshot.get("bound_agents") {
            let bound_agents: Vec<BoundAgent> =
                serde_json::from_value(value.clone()).map_err(|e| {
                    TaskLaunchError::PreDispatch(format!("decode Run BoundAgent snapshot: {e}"))
                })?;
            validate_bound_agent_snapshots(&bound_agents).map_err(|error| {
                TaskLaunchError::PreDispatch(format!("validate Run BoundAgent snapshot: {error}"))
            })?;
            return Ok(bound_agents);
        }
    }

    let mut bound_agents = resolve_fresh().map_err(CompileError::from)?;
    attest_or_gate_fresh(&mut bound_agents, binding_provider, strict, Some(run_ctx)).await?;
    if let Some(input_json) = record.input_json {
        let mut snapshot: Value = serde_json::from_str(&input_json).map_err(|e| {
            TaskLaunchError::PreDispatch(format!("decode Run launch snapshot: {e}"))
        })?;
        let object = snapshot.as_object_mut().ok_or_else(|| {
            TaskLaunchError::PreDispatch("Run launch snapshot must be a JSON object".to_string())
        })?;
        object.insert(
            "bound_agents".to_string(),
            serde_json::to_value(&bound_agents).map_err(|e| {
                TaskLaunchError::PreDispatch(format!("encode Run BoundAgent snapshot: {e}"))
            })?,
        );
        run_ctx
            .run_store
            .set_input_json(
                &run_ctx.run_id,
                serde_json::to_string(&snapshot).map_err(|e| {
                    TaskLaunchError::PreDispatch(format!("encode Run launch snapshot: {e}"))
                })?,
            )
            .await
            .map_err(|e| {
                TaskLaunchError::PreDispatch(format!("persist Run BoundAgent snapshot: {e}"))
            })?;
    }
    Ok(bound_agents)
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
pub(crate) fn derive_agent_ctx(blueprint: &Blueprint) -> (Option<Value>, HashMap<String, Value>) {
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
pub(crate) fn shallow_merge_inline_wins(base: Value, inline: Value) -> Value {
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
    /// Pre-dispatch validation failed: the launch was rejected before any
    /// step was dispatched. Raised when the effective check_policy
    /// (launch request > blueprint > server config) is Strict and the
    /// launch supplied neither project_root nor work_dir — a strict task
    /// would deterministically fail at its first submit-time file
    /// materialize, so the launch fails fast instead.
    #[error("pre-dispatch: {0}")]
    PreDispatch(String),
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
    /// The "launch request" tier (tier 1, highest
    /// priority) of the `check_policy` cascade
    /// (`launch request > blueprint > server config`).
    /// [`TaskLaunchService::launch`]
    /// collapses `check_policy.or(blueprint.check_policy)` exactly once and
    /// threads the result into every spawned step's `TaskSpec.check_policy`.
    /// `None` (the default via [`Self::automate`]) leaves this tier
    /// unspecified so the Blueprint tier / server-wide default decide —
    /// backward-compat with every pre-cascade caller.
    ///
    /// [`TaskLaunchService::launch`] also collapses this same cascade one
    /// step further
    /// (adding the server-wide `EngineCfg.check_policy` tier) into a
    /// pre-dispatch guard: when the resulting effective policy is
    /// [`CheckPolicy::Strict`] and neither [`Self::task_input`]'s
    /// `project_root` nor `work_dir` is set, the launch is rejected with
    /// `TaskLaunchError::PreDispatch` before any step is dispatched — a
    /// strict task with no resolvable root would deterministically fail
    /// at its first submit-time file materialize anyway. Setting this
    /// field to `Some(CheckPolicy::Warn)` on the launch-request tier is
    /// the escape hatch: it outranks a Blueprint- or server-declared
    /// Strict and lets the guard pass.
    pub check_policy: Option<CheckPolicy>,
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
            check_policy: None,
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
    /// Optional execution-environment binding implementation. When present,
    /// fresh Run snapshots require a complete, Core-validated receipt set.
    binding_provider: Option<Arc<dyn AgentBindingProvider>>,
    /// Whether fresh Blueprint declarations may use the deprecated
    /// `profile.worker_binding` Runner fallback.
    legacy_worker_binding_policy: LegacyWorkerBindingPolicy,
}

impl TaskLaunchService {
    /// Build a service bound to one `Engine` and one `Compiler`.
    pub fn new(engine: Engine, compiler: Compiler) -> Self {
        Self {
            engine,
            compiler,
            externs: Arc::new(NoExterns),
            binding_provider: None,
            legacy_worker_binding_policy: LegacyWorkerBindingPolicy::default(),
        }
    }

    /// Replace the `call_extern` registry (builder style). Entries MUST be
    /// pure functions — no side effects, no flow control; effectful work
    /// belongs to `Step` / agents, not externs (flow-ir canonical contract).
    pub fn with_externs(mut self, externs: Arc<dyn Externs + Send + Sync>) -> Self {
        self.externs = externs;
        self
    }

    /// Inject the execution-environment binding provider. Platform plugins
    /// and Operator/MainAI implementations use this same interface; Core
    /// retains receipt validation and digest ownership.
    pub fn with_binding_provider(mut self, provider: Arc<dyn AgentBindingProvider>) -> Self {
        self.binding_provider = Some(provider);
        self
    }

    /// Configure the migration gate for deprecated `profile.worker_binding`.
    pub fn with_legacy_worker_binding_policy(mut self, policy: LegacyWorkerBindingPolicy) -> Self {
        self.legacy_worker_binding_policy = policy;
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
        mut input: TaskLaunchInput,
    ) -> Result<TaskLaunchOutput, TaskLaunchError> {
        // After the stateless-executor refactor, the
        // caller (Service) does compile + link +
        // `EngineDispatcher::with_spawner` itself; the engine no longer
        // holds any global spawner state to touch. The link path (base
        // `SpawnerAdapter` +
        // `LayerRegistry` resolution + `SpawnerStack` wrapping) is
        // concentrated inside `service::linker::link` — Service
        // scatter is intentionally prevented.
        let bound_agents = load_or_resolve_bound_agents(
            &input.blueprint,
            input.run_ctx.as_ref(),
            self.binding_provider.as_deref(),
            self.legacy_worker_binding_policy,
        )
        .await?;
        let binding_digests: HashMap<String, crate::blueprint::BindingDigest> = bound_agents
            .iter()
            .map(|bound| (bound.agent.name.clone(), bound.binding_digest.clone()))
            .collect();
        if let Some(run_ctx) = input.run_ctx.take() {
            input.run_ctx = Some(run_ctx.with_binding_digests(binding_digests.clone()));
        }
        input.blueprint = materialize_bound_blueprint(&input.blueprint, &bound_agents);
        let compiled = self
            .compiler
            .compile_bound(&input.blueprint, &bound_agents)?;
        // GH #50 (Subtask 2 follow-up): merge this Blueprint's compiled
        // `AgentDef.verdict` contracts into the engine's runtime registry —
        // see `Engine::register_verdict_contracts`'s doc for the additive
        // (last-write-wins per agent name) semantics. This is the ONLY
        // production call site; every other consumer
        // (`Engine::verdict_contract_for_task`, and through it
        // `mlua-swarm-server`'s `worker_submit` / `worker_artifact`
        // submit-time gate) reads from what this line populates.
        self.engine
            .register_verdict_contracts(compiled.router.verdict_contracts.clone());
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
        let worker_bindings = worker_bindings_from_bound_agents(&bound_agents);
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
        // Collapse the `check_policy` cascade EXACTLY ONCE
        // here: `launch request > blueprint > server config` (highest to
        // lowest priority). `input.check_policy` is the launch-request tier;
        // `input.blueprint.check_policy` is the Blueprint tier; a `None`
        // result leaves the engine's submit-time sink to fall back to the
        // server-wide `EngineCfg.check_policy` (tier 3) on its own — the
        // engine's existing `task_policy.unwrap_or(server_policy)` resolution
        // is deliberately NOT duplicated here (no double resolution). The
        // resolved value is threaded (via `with_check_policy`) into EVERY
        // spawned step's `TaskSpec`, not just the first.
        let resolved_check_policy = input.check_policy.or(input.blueprint.check_policy);
        // Pre-dispatch guard: collapse the same cascade one step further
        // (adding the server tier, `EngineCfg.check_policy`, via
        // `self.engine.cfg()`) into a SEPARATE local used only for this
        // check — `resolved_check_policy` above (the Option stamped onto
        // every dispatched step's `TaskSpec`) is left untouched, so the
        // "TaskSpec = None -> engine falls back to server default at the
        // submit-time sink" contract (cascade test case 4) keeps holding.
        // When the effective policy is Strict and the launch supplied
        // neither `project_root` nor `work_dir`, a strict task would
        // deterministically fail at its first submit-time file
        // materialize — fail the launch fast instead of dispatching a
        // step that can only ever hit that wall. `check_policy: "warn"` on
        // the launch-request tier is the escape hatch (it wins the
        // cascade before this fallback ever applies).
        let effective_check_policy =
            resolved_check_policy.unwrap_or(self.engine.cfg().check_policy);
        if effective_check_policy == CheckPolicy::Strict {
            let roots_missing = input
                .task_input
                .as_ref()
                .map(|t| t.project_root.is_none() && t.work_dir.is_none())
                .unwrap_or(true);
            if roots_missing {
                return Err(TaskLaunchError::PreDispatch(
                    "check_policy=strict requires project_root or work_dir, but the launch \
                     supplied neither"
                        .to_string(),
                ));
            }
        }
        let dispatcher =
            EngineDispatcher::with_spawner(self.engine.clone(), token.clone(), spawner);
        let dispatcher = dispatcher.with_check_policy(resolved_check_policy);
        let dispatcher = match input.run_ctx {
            Some(run_ctx) => dispatcher.with_run(run_ctx),
            None => dispatcher,
        };
        // GH #21 Phase 2: attach the Step tier's named `MetaDef` pool.
        // Unconditional — an empty map (every pre-#21-Phase-2 Blueprint)
        // is a no-op, matching `EngineDispatcher::with_spawner`'s default.
        let dispatcher = dispatcher.with_step_metas(derive_step_metas(&input.blueprint));
        let dispatcher = dispatcher.with_binding_digests(binding_digests);
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
        current_schema_version, resolve_runner, AgentDef, AgentKind, AgentMeta, AgentProfile,
        BlueprintMetadata, CompilerHints, CompilerStrategy, MetaDef, Runner,
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
            runner: None,
            runner_ref: None,
            verdict: None,
        }
    }

    fn build_service(factory: RustFnInProcessSpawnerFactory) -> TaskLaunchService {
        let engine = Engine::new(EngineCfg::default());
        let mut reg = SpawnerRegistry::new();
        reg.register::<RustFnInProcessSpawnerFactory>(Arc::new(factory));
        let compiler = Compiler::new(reg);
        TaskLaunchService::new(engine, compiler)
    }

    /// Same as [`build_service`] but with a caller-supplied [`EngineCfg`] —
    /// used by the pre-dispatch guard's server-tier test (T4), which needs
    /// a non-default `EngineCfg.check_policy`.
    fn build_service_with_cfg(
        factory: RustFnInProcessSpawnerFactory,
        cfg: EngineCfg,
    ) -> TaskLaunchService {
        let engine = Engine::new(cfg);
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
            runners: vec![],
            default_runner: None,
            check_policy: None,
            blueprint_ref_includes: Vec::new(),
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

    // ──────────────────────────────────────────────────────────────
    // check_policy cascade (launch > blueprint > server)
    // T2 (cascade 4-case) / T3 (end-to-end strict) / T4 (backward compat)
    // ──────────────────────────────────────────────────────────────

    /// Launch a single-echo Blueprint with the given launch- and
    /// Blueprint-tier `check_policy`, then read back the `check_policy` that
    /// the dispatcher stamped onto the dispatched step's `TaskSpec`. The
    /// launch may complete (Silent / Warn / None → fail-open) — the in-process
    /// RustFn worker fire-and-forgets its submit — so the task and its
    /// resolved spec exist regardless of the launch outcome.
    ///
    /// `task_input` carries a `work_dir` unconditionally (a dummy path, not
    /// resolved on disk) so the pre-dispatch guard (a strict effective
    /// policy with no roots supplied rejects before dispatch) never fires
    /// here — this helper's whole point is "reach dispatch and read back
    /// the stamp", so every case (including the two whose
    /// `bp_policy`/`launch_policy` alone resolve to Strict) must dispatch
    /// uniformly. The guard's own rejection behavior is proven separately
    /// (T3/T4 and `strict_blueprint_without_roots_is_rejected_pre_dispatch`).
    async fn dispatched_check_policy(
        launch_policy: Option<CheckPolicy>,
        bp_policy: Option<CheckPolicy>,
    ) -> Option<CheckPolicy> {
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", |inv| async move {
            Ok(WorkerResult {
                value: json!({ "echoed": inv.prompt }),
                ok: true,
            })
        });
        let svc = build_service(factory);
        let mut blueprint = bp(
            step("echo", path("$.input"), path("$.out")),
            vec![agent("echo", "echo")],
        );
        blueprint.check_policy = bp_policy;
        let mut input = launch_input(blueprint, json!({ "input": "hi" }));
        input.check_policy = launch_policy;
        input.task_input = Some(TaskInputSpec {
            project_root: None,
            work_dir: Some("/dispatched-check-policy-test-root".to_string()),
            task_metadata: None,
        });
        let _ = svc.launch(input).await;
        svc.engine()
            .with_state("test.read_dispatched_check_policy", |s| {
                s.tasks
                    .values()
                    .find(|t| t.spec.agent == "echo")
                    .and_then(|t| t.spec.check_policy)
            })
            .await
            .expect("with_state")
    }

    /// T2 case 1: launch `Some(Silent)` + BP `Some(Strict)` → TaskSpec
    /// `Some(Silent)` (the launch-request tier outranks the Blueprint tier).
    #[tokio::test]
    async fn cascade_launch_tier_wins_over_blueprint_tier() {
        assert_eq!(
            dispatched_check_policy(Some(CheckPolicy::Silent), Some(CheckPolicy::Strict)).await,
            Some(CheckPolicy::Silent),
        );
    }

    /// T2 case 2: launch `None` + BP `Some(Strict)` → TaskSpec `Some(Strict)`
    /// (the Blueprint tier takes effect when the launch tier is unset).
    #[tokio::test]
    async fn cascade_blueprint_tier_used_when_launch_absent() {
        assert_eq!(
            dispatched_check_policy(None, Some(CheckPolicy::Strict)).await,
            Some(CheckPolicy::Strict),
        );
    }

    /// T2 case 3: launch `Some(Strict)` + BP `None` → TaskSpec `Some(Strict)`
    /// (the launch tier alone resolves when the Blueprint tier is unset).
    #[tokio::test]
    async fn cascade_launch_tier_alone_when_blueprint_absent() {
        assert_eq!(
            dispatched_check_policy(Some(CheckPolicy::Strict), None).await,
            Some(CheckPolicy::Strict),
        );
    }

    /// T2 case 4: launch `None` + BP `None` → TaskSpec `None`. NOT omitted as
    /// "trivial": this is the backward-compat proof — the server-fallback
    /// path (`EngineCfg.check_policy` decides at the submit-time sink) is
    /// preserved byte-for-byte because the carrier stays `None`.
    #[tokio::test]
    async fn cascade_both_none_preserves_server_fallback() {
        assert_eq!(dispatched_check_policy(None, None).await, None);
    }

    /// Repurposed 2026-07-16 for the pre-dispatch guard's new contract
    /// (the launch-time validation stage of the check_policy cascade
    /// work). This test used
    /// to prove a strict + no-roots launch dispatched a step that then hit
    /// `EngineError::CheckPolicyStrict` at submit time — exactly the path
    /// the pre-dispatch guard now forecloses (a strict launch with no
    /// resolvable root is rejected BEFORE dispatch instead, see
    /// [`TaskLaunchService::launch`]'s guard). The two sub-assertions this
    /// test used to make are independently covered elsewhere: the
    /// cascade-resolved Strict reaching the dispatched `TaskSpec` is
    /// covered by the `cascade_*` tests above; the submit-time sink
    /// surfacing `CheckPolicyStrict` on an unresolved root is covered by
    /// `crate::core::engine::tests::submit_output_final_check_policy_strict_surfaces_error_when_root_unresolved`
    /// (seeds the task directly at the engine layer, bypassing `launch`).
    /// This test now asserts the NEW contract directly.
    #[tokio::test]
    async fn strict_blueprint_without_roots_is_rejected_pre_dispatch() {
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", |inv| async move {
            Ok(WorkerResult {
                value: json!({ "echoed": inv.prompt }),
                ok: true,
            })
        });
        let svc = build_service(factory);
        let mut blueprint = bp(
            step("echo", path("$.input"), path("$.out")),
            vec![agent("echo", "echo")],
        );
        blueprint.check_policy = Some(CheckPolicy::Strict);
        // No task_input → no work_dir/project_root ever resolves.
        let err = svc
            .launch(launch_input(blueprint, json!({ "input": "hi" })))
            .await
            .expect_err("strict check_policy + no roots must be rejected before dispatch");
        match err {
            TaskLaunchError::PreDispatch(message) => {
                assert!(
                    message.contains("strict"),
                    "message must identify the strict-requires-roots condition: {message}"
                );
            }
            other => panic!("expected TaskLaunchError::PreDispatch, got {other:?}"),
        }

        // No step was ever dispatched — the guard fires after
        // `engine.attach_with_ids` (the token mint) but before the
        // dispatcher is ever built / `eval_async_externs` runs.
        let dispatched = svc
            .engine()
            .with_state("test.no_echo_task_dispatched", |s| {
                s.tasks.values().any(|t| t.spec.agent == "echo")
            })
            .await
            .expect("with_state");
        assert!(
            !dispatched,
            "the pre-dispatch guard must reject before any step is dispatched"
        );
    }

    /// T4 (cascade backward-compat): backward compat — with NO check_policy
    /// anywhere (BP tier + launch tier both `None`), the launch resolves to
    /// the server default (Warn) and completes fail-open exactly as before
    /// this change (the warn-mode materialize skip never turns a
    /// successful submit into a failure).
    ///
    /// This is ALSO the pre-dispatch guard's backward-compat case (T5):
    /// `task_input` is `None` via [`launch_input`]/[`TaskLaunchInput::automate`],
    /// so the guard's effective policy resolves to `Warn` (server default,
    /// [`EngineCfg::default`]) and never fires — the guard changes nothing
    /// about this pre-existing default-path behavior.
    #[tokio::test]
    async fn launch_without_any_check_policy_completes_fail_open() {
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
        assert_eq!(blueprint.check_policy, None, "BP tier must be unset");
        let out = svc
            .launch(launch_input(blueprint, json!({ "input": "hi" })))
            .await
            .expect("warn-mode fail-open must let the launch complete");
        assert_eq!(out.final_ctx["out"]["echoed"], "hi");
    }

    // ──────────────────────────────────────────────────────────────────
    // pre-dispatch validation guard:
    // `TaskLaunchService::launch` rejects BEFORE dispatch when the
    // effective check_policy is Strict and neither `project_root` nor
    // `work_dir` is supplied. T3/T4/T6 live here (T1/T2 are
    // handler-level, in `mlua-swarm-server`'s `projection.rs`; T5 is the
    // `launch_without_any_check_policy_completes_fail_open` test above;
    // the guard-rejection end-to-end case is
    // `strict_blueprint_without_roots_is_rejected_pre_dispatch` above,
    // Option A's repurpose of the former stage-1 T3).
    // ──────────────────────────────────────────────────────────────────

    /// T3 (Crux 3, escape hatch): a Blueprint declaring `check_policy:
    /// strict` is overridden by the launch-request tier's `check_policy:
    /// Some(Warn)` — tier 1 wins the cascade before the guard's
    /// effective-policy fallback ever applies, so the guard passes and the
    /// launch dispatches normally even though `task_input` is `None` (no
    /// project_root/work_dir at all). Regression guard against a future
    /// "the guard judges by the BP tier alone, not the effective/cascaded
    /// value" narrowing.
    #[tokio::test]
    async fn strict_blueprint_with_launch_warn_override_bypasses_pre_dispatch_guard() {
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", |inv| async move {
            Ok(WorkerResult {
                value: json!({ "echoed": inv.prompt }),
                ok: true,
            })
        });
        let svc = build_service(factory);
        let mut blueprint = bp(
            step("echo", path("$.input"), path("$.out")),
            vec![agent("echo", "echo")],
        );
        blueprint.check_policy = Some(CheckPolicy::Strict);
        let mut input = launch_input(blueprint, json!({ "input": "hi" }));
        input.check_policy = Some(CheckPolicy::Warn);
        assert!(input.task_input.is_none(), "no roots supplied at all");
        let out = svc
            .launch(input)
            .await
            .expect("launch-tier warn override must bypass the pre-dispatch guard");
        assert_eq!(out.final_ctx["out"]["echoed"], "hi");
    }

    /// T4 (Crux 2, server tier): with BOTH the launch- and Blueprint-tier
    /// `check_policy` unset, the server-wide `EngineCfg.check_policy` (the
    /// third cascade tier, read via `self.engine.cfg()`) alone must drive
    /// the guard — proof the guard does not stop at the "BP/launch 2-tier"
    /// shortcut Crux 2 forbids.
    #[tokio::test]
    async fn server_tier_strict_alone_triggers_pre_dispatch_guard() {
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", |inv| async move {
            Ok(WorkerResult {
                value: json!({ "echoed": inv.prompt }),
                ok: true,
            })
        });
        let svc = build_service_with_cfg(
            factory,
            EngineCfg {
                check_policy: CheckPolicy::Strict,
                ..EngineCfg::default()
            },
        );
        let blueprint = bp(
            step("echo", path("$.input"), path("$.out")),
            vec![agent("echo", "echo")],
        );
        assert_eq!(blueprint.check_policy, None, "BP tier must be unset");
        let input = launch_input(blueprint, json!({ "input": "hi" }));
        assert!(input.check_policy.is_none(), "launch tier must be unset");
        assert!(input.task_input.is_none(), "no roots supplied");
        let err = svc.launch(input).await.expect_err(
            "server-tier Strict alone (BP/launch tiers both unset) must trigger the guard",
        );
        match err {
            TaskLaunchError::PreDispatch(message) => {
                assert!(
                    message.contains("strict"),
                    "expected the strict-requires-roots message, got: {message}"
                );
            }
            other => panic!("expected TaskLaunchError::PreDispatch, got {other:?}"),
        }
    }

    /// T6 (guard condition, branch 2 of 3): `task_input: Some(_)` with
    /// BOTH `project_root` and `work_dir` absent is still `roots_missing`
    /// — the outer `Some` alone must not short-circuit the check (branch 1,
    /// `task_input: None`, is covered by
    /// `strict_blueprint_without_roots_is_rejected_pre_dispatch` above).
    #[tokio::test]
    async fn pre_dispatch_guard_rejects_when_task_input_present_but_roots_both_none() {
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", |inv| async move {
            Ok(WorkerResult {
                value: json!({ "echoed": inv.prompt }),
                ok: true,
            })
        });
        let svc = build_service(factory);
        let mut blueprint = bp(
            step("echo", path("$.input"), path("$.out")),
            vec![agent("echo", "echo")],
        );
        blueprint.check_policy = Some(CheckPolicy::Strict);
        let mut input = launch_input(blueprint, json!({ "input": "hi" }));
        input.task_input = Some(TaskInputSpec {
            project_root: None,
            work_dir: None,
            task_metadata: Some(json!({ "unrelated": true })),
        });
        let err = svc
            .launch(input)
            .await
            .expect_err("Some(TaskInputSpec) with both roots None must still be roots_missing");
        assert!(
            matches!(err, TaskLaunchError::PreDispatch(_)),
            "expected TaskLaunchError::PreDispatch, got {err:?}"
        );
    }

    /// T6 (guard condition, branch 3 of 3): `work_dir: Some(_)` alone
    /// (with `project_root: None`) is NOT `roots_missing` — either root
    /// being present is sufficient, so the guard passes and the launch
    /// dispatches normally.
    #[tokio::test]
    async fn pre_dispatch_guard_passes_when_work_dir_present_and_project_root_absent() {
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("echo", |inv| async move {
            Ok(WorkerResult {
                value: json!({ "echoed": inv.prompt }),
                ok: true,
            })
        });
        let svc = build_service(factory);
        let mut blueprint = bp(
            step("echo", path("$.input"), path("$.out")),
            vec![agent("echo", "echo")],
        );
        blueprint.check_policy = Some(CheckPolicy::Strict);
        let mut input = launch_input(blueprint, json!({ "input": "hi" }));
        input.task_input = Some(TaskInputSpec {
            project_root: None,
            work_dir: Some("/repo/work".to_string()),
            task_metadata: None,
        });
        let out = svc
            .launch(input)
            .await
            .expect("work_dir alone must satisfy the guard's roots_missing check");
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
    // GH #50 (Subtask 2 follow-up): `TaskLaunchService::launch`'s
    // `compiler.compile` → `engine.register_verdict_contracts(...)` call
    // site — task_launch-level end-to-end (compile → register →
    // `Engine::verdict_contract_for_task` resolves it). The full HTTP
    // submit-time-422 round trip is covered separately: handler-level in
    // `crates/mlua-swarm-server/src/worker.rs`'s own `#[cfg(test)] mod
    // tests` GH #50 section (which seeds `Engine::register_verdict_contracts`
    // directly, bypassing this launch path since `mlua-swarm-server`
    // cannot depend on this crate's private test helpers) and
    // process-boundary-HTTP in
    // `crates/mlua-swarm-server/tests/verdict_contract.rs`. This test is
    // the missing link between those two: it exercises the REAL
    // `TaskLaunchService::launch` call site (not a hand-rolled duplicate
    // of its two lines) end-to-end through a real `Compiler::compile`,
    // proving the production wiring this follow-up added actually
    // populates the registry `Engine::verdict_contract_for_task` reads.
    // ──────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn launch_registers_the_blueprints_verdict_contracts_into_the_engine() {
        let factory = RustFnInProcessSpawnerFactory::new().register_fn("gate", |inv| async move {
            Ok(WorkerResult {
                value: json!(inv.prompt),
                ok: true,
            })
        });
        let svc = build_service(factory);
        let mut gate_agent = agent("gate", "gate");
        gate_agent.verdict = Some(mlua_swarm_schema::VerdictContract {
            channel: mlua_swarm_schema::VerdictChannel::Body,
            values: vec!["PASS".to_string(), "BLOCKED".to_string()],
        });
        let flow = step("gate", path("$.input"), path("$.out"));
        let blueprint = bp(flow, vec![gate_agent]);

        let out = svc
            .launch(launch_input(blueprint, json!({ "input": "PASS" })))
            .await
            .expect("launch ok");
        assert_eq!(out.final_ctx["out"], json!("PASS"));

        // `EngineDispatcher::dispatch` calls `engine.start_task` for every
        // dispatched Step (`TaskSpec.agent = ref_`) — this single-Step
        // Blueprint against a fresh per-test `Engine` (`build_service`)
        // leaves exactly one entry in `EngineState.tasks`.
        let task_id = svc
            .engine()
            .with_state("test.find_dispatched_task_id", |s| {
                s.tasks.keys().next().cloned()
            })
            .await
            .expect("with_state")
            .expect("launch must have dispatched exactly one Step (one TaskState)");

        let contract = svc
            .engine()
            .verdict_contract_for_task(&task_id)
            .await
            .expect(
                "TaskLaunchService::launch must have merged this Blueprint's compiled \
                 verdict_contracts into the engine's runtime registry \
                 (Engine::register_verdict_contracts, called right after \
                 compiler.compile succeeds) — verdict_contract_for_task resolving None \
                 here means that production wiring regressed",
            );
        assert_eq!(contract.channel, mlua_swarm_schema::VerdictChannel::Body);
        assert_eq!(
            contract.values,
            vec!["PASS".to_string(), "BLOCKED".to_string()]
        );
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
                input_json: Some("{}".to_string()),
                created_at: 0,
                updated_at: 0,
            })
            .await
            .expect("seed RunRecord");

        let mut input = launch_input(blueprint, json!({ "in": "hi" }));
        input.run_ctx = Some(RunContext::new(run_id.clone(), run_store.clone()));

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
        assert!(run.step_entries[0].binding_digest.is_some());
        assert_eq!(run.step_entries[1].step_ref, Some("suffix".to_string()));
        assert_eq!(run.step_entries[1].status, Some("passed".to_string()));
        assert!(run.step_entries[1].binding_digest.is_some());
        let snapshot: Value = serde_json::from_str(run.input_json.as_deref().unwrap()).unwrap();
        assert_eq!(snapshot["bound_agents"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn run_snapshot_reuses_bound_agent_after_blueprint_mutation() {
        use crate::store::run::{InMemoryRunStore, RunContext, RunRecord, RunStatus, RunStore};
        use crate::types::{RunId, TaskId};

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
                input_json: Some("{}".to_string()),
                created_at: 0,
                updated_at: 0,
            })
            .await
            .unwrap();
        let run_ctx = RunContext::new(run_id, run_store);
        let mut original_agent = agent("worker", "worker");
        original_agent.profile = Some(crate::blueprint::AgentProfile {
            system_prompt: "original role".to_string(),
            ..Default::default()
        });
        let mut blueprint = bp(
            step("worker", path("$.input"), path("$.out")),
            vec![original_agent],
        );

        let original = load_or_resolve_bound_agents(
            &blueprint,
            Some(&run_ctx),
            None,
            LegacyWorkerBindingPolicy::Allow,
        )
        .await
        .unwrap();
        blueprint.agents[0].profile.as_mut().unwrap().system_prompt = "mutated role".to_string();
        let restored = load_or_resolve_bound_agents(
            &blueprint,
            Some(&run_ctx),
            None,
            LegacyWorkerBindingPolicy::Allow,
        )
        .await
        .unwrap();

        assert_eq!(restored[0].binding_digest, original[0].binding_digest);
        assert_eq!(
            restored[0].agent.profile.as_ref().unwrap().system_prompt,
            "original role"
        );
    }

    #[tokio::test]
    async fn strict_migration_policy_rejects_fresh_legacy_worker_binding() {
        let mut legacy_agent = agent("worker", "worker");
        legacy_agent.profile = Some(AgentProfile {
            worker_binding: Some("legacy-worker".to_string()),
            ..Default::default()
        });
        let blueprint = bp(
            step("worker", path("$.input"), path("$.out")),
            vec![legacy_agent],
        );

        let error =
            load_or_resolve_bound_agents(&blueprint, None, None, LegacyWorkerBindingPolicy::Reject)
                .await
                .expect_err("strict migration policy must reject fallback");
        assert!(error
            .to_string()
            .contains("deprecated profile.worker_binding"));
    }

    #[tokio::test]
    async fn run_snapshot_calls_binding_provider_only_on_first_resolution() {
        use crate::binding::{AgentBindingProvider, BindingProviderError};
        use crate::blueprint::{BindOutcome, BindReceipt, BindRequest};
        use crate::store::run::{InMemoryRunStore, RunContext, RunRecord, RunStatus, RunStore};
        use crate::types::{RunId, TaskId};
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingProvider(AtomicUsize);

        #[async_trait::async_trait]
        impl AgentBindingProvider for CountingProvider {
            async fn bind(
                &self,
                requests: &[BindRequest],
            ) -> Result<Vec<BindOutcome>, BindingProviderError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(requests
                    .iter()
                    .map(|request| BindOutcome::Bound {
                        receipt: BindReceipt {
                            agent: request.agent.clone(),
                            request_digest: request.request_digest.clone(),
                            provider_id: "operator-main-ai".to_string(),
                            provider_revision: Some("test".to_string()),
                            resolved_model: request.requested_model.clone(),
                            effective_tools: request.requested_tools.clone(),
                            launch_variant: request.launch_variant.clone(),
                            capability_snapshot_digest: None,
                        },
                    })
                    .collect())
            }
        }

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
                input_json: Some("{}".to_string()),
                created_at: 0,
                updated_at: 0,
            })
            .await
            .unwrap();
        let run_ctx = RunContext::new(run_id, run_store);
        let mut blueprint = bp(
            step("worker", path("$.input"), path("$.out")),
            vec![agent("worker", "worker")],
        );
        blueprint.agents[0].runner = Some(Runner::WsClaudeCode {
            variant: "mse-worker".to_string(),
            tools: vec!["Read".to_string()],
        });
        let provider = CountingProvider(AtomicUsize::new(0));

        let first = load_or_resolve_bound_agents(
            &blueprint,
            Some(&run_ctx),
            Some(&provider),
            LegacyWorkerBindingPolicy::Allow,
        )
        .await
        .unwrap();
        let restored = load_or_resolve_bound_agents(
            &blueprint,
            Some(&run_ctx),
            Some(&provider),
            LegacyWorkerBindingPolicy::Allow,
        )
        .await
        .unwrap();

        assert_eq!(provider.0.load(Ordering::SeqCst), 1);
        assert!(first[0].attestation.is_some());
        assert_eq!(restored, first);
    }

    // ──────────────────────────────────────────────────────────────────
    // C1: `strict_binding` gate + optional attestation
    // ──────────────────────────────────────────────────────────────────

    /// A Runner-backed Blueprint whose single `worker` agent binds through a
    /// WS Operator variant `mse-worker` requiring tool `Read`.
    fn runner_blueprint(strict_binding: bool) -> Blueprint {
        let mut blueprint = bp(
            step("worker", path("$.input"), path("$.out")),
            vec![agent("worker", "worker")],
        );
        blueprint.strategy.strict_binding = strict_binding;
        blueprint.agents[0].runner = Some(Runner::WsClaudeCode {
            variant: "mse-worker".to_string(),
            tools: vec!["Read".to_string()],
        });
        blueprint
    }

    /// Provider that leaves every request `Unbound` — models a missing /
    /// manifest-less execution environment.
    struct AlwaysUnboundProvider;

    #[async_trait::async_trait]
    impl AgentBindingProvider for AlwaysUnboundProvider {
        async fn bind(
            &self,
            requests: &[crate::blueprint::BindRequest],
        ) -> Result<Vec<crate::blueprint::BindOutcome>, crate::binding::BindingProviderError>
        {
            Ok(requests
                .iter()
                .map(|request| crate::blueprint::BindOutcome::Unbound {
                    agent: request.agent.clone(),
                    reason: "no capability manifest submitted".to_string(),
                })
                .collect())
        }
    }

    /// Non-strict + a provider that cannot attest → launch resolution
    /// succeeds, the agent stays `DeclarationOnly`, and the gap is recorded
    /// as a `RunRecord.degradations` entry.
    #[tokio::test]
    async fn non_strict_unbound_agent_runs_declaration_only_with_degradation() {
        use crate::store::run::{InMemoryRunStore, RunContext, RunRecord, RunStatus, RunStore};
        use crate::types::{RunId, TaskId};

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
                input_json: Some("{}".to_string()),
                created_at: 0,
                updated_at: 0,
            })
            .await
            .unwrap();
        let run_ctx = RunContext::new(run_id.clone(), run_store.clone());

        let bound = load_or_resolve_bound_agents(
            &runner_blueprint(false),
            Some(&run_ctx),
            Some(&AlwaysUnboundProvider),
            LegacyWorkerBindingPolicy::Allow,
        )
        .await
        .expect("non-strict launch must succeed even without an attestation");
        assert!(
            bound[0].attestation.is_none(),
            "an unattested agent must stay DeclarationOnly"
        );

        let run = run_store.get(&run_id).await.expect("run present");
        assert_eq!(run.degradations.len(), 1, "expected one degradation entry");
        assert_eq!(run.degradations[0].tool, "binding");
        assert_eq!(run.degradations[0].fallback, "DeclarationOnly");
        assert!(run.degradations[0].error.contains("no capability manifest"));
    }

    /// Strict + a provider that cannot attest → launch fails, and the error
    /// message names the agent and its requested launch variant / tools so an
    /// Operator can generate a satisfying manifest.
    #[tokio::test]
    async fn strict_unbound_agent_fails_with_requirements_in_message() {
        let error = load_or_resolve_bound_agents(
            &runner_blueprint(true),
            None,
            Some(&AlwaysUnboundProvider),
            LegacyWorkerBindingPolicy::Allow,
        )
        .await
        .expect_err("strict + Unbound must reject the launch");
        match error {
            TaskLaunchError::PreDispatch(message) => {
                assert!(message.contains("worker"), "message: {message}");
                assert!(message.contains("mse-worker"), "message: {message}");
                assert!(message.contains("Read"), "message: {message}");
            }
            other => panic!("expected PreDispatch, got {other:?}"),
        }
    }

    /// Strict + no provider at all → launch fails fast: nothing can attest the
    /// Runner-backed agent.
    #[tokio::test]
    async fn strict_without_provider_rejects_runner_backed_launch() {
        let error = load_or_resolve_bound_agents(
            &runner_blueprint(true),
            None,
            None,
            LegacyWorkerBindingPolicy::Allow,
        )
        .await
        .expect_err("strict + no provider must reject a Runner-backed launch");
        match error {
            TaskLaunchError::PreDispatch(message) => {
                assert!(
                    message.contains("strict_binding requires a binding provider"),
                    "message: {message}"
                );
            }
            other => panic!("expected PreDispatch, got {other:?}"),
        }
    }

    /// Strict + a correct manifest → the agent is Attested (the pre-C1 pass
    /// path still holds under the strict gate).
    #[tokio::test]
    async fn strict_with_correct_manifest_attests_the_agent() {
        use crate::binding::ManifestBindingProvider;
        use crate::blueprint::{AgentProviderCapability, AgentProviderManifest};

        let provider = ManifestBindingProvider::new(AgentProviderManifest {
            provider_id: "operator-main-ai".to_string(),
            provider_revision: Some("1".to_string()),
            capabilities: vec![AgentProviderCapability {
                launch_variant: Some("mse-worker".to_string()),
                resolved_model: None,
                effective_tools: vec!["Read".to_string()],
                capability_snapshot_digest: None,
            }],
        });
        let bound = load_or_resolve_bound_agents(
            &runner_blueprint(true),
            None,
            Some(&provider),
            LegacyWorkerBindingPolicy::Allow,
        )
        .await
        .expect("strict launch with a correct manifest must attest");
        assert!(
            bound[0].attestation.is_some(),
            "a correctly attested agent must carry its attestation"
        );
    }

    // ──────────────────────────────────────────────────────────────────
    // C2: spawn-frame self-check inputs (request_digest / requested_model)
    // ──────────────────────────────────────────────────────────────────

    /// The launch-path `WorkerBinding` map carries the requesting side's
    /// self-check inputs: the immutable snapshot's `binding_digest` and the
    /// profile's declared model, so a non-strict Operator can compare the
    /// spawn frame against its own environment.
    #[test]
    fn worker_bindings_carry_request_digest_and_model() {
        let mut blueprint = runner_blueprint(false);
        blueprint.agents[0].profile = Some(AgentProfile {
            model: Some("claude-sonnet".to_string()),
            ..Default::default()
        });
        let bound = resolve_bound_agents(&blueprint).expect("resolvable Runner refs");
        let bindings = worker_bindings_from_bound_agents(&bound);

        let wb = bindings.get("worker").expect("worker binding present");
        assert_eq!(
            wb.request_digest.as_ref(),
            Some(&bound[0].binding_digest),
            "the spawn frame must carry the immutable snapshot digest"
        );
        assert!(wb
            .request_digest
            .as_ref()
            .unwrap()
            .as_str()
            .starts_with("sha256:"));
        assert_eq!(wb.requested_model.as_deref(), Some("claude-sonnet"));
    }

    /// A Runner-backed agent whose profile declares no model leaves
    /// `requested_model` `None` while still carrying the digest.
    #[test]
    fn worker_bindings_omit_model_when_profile_has_none() {
        let bound = resolve_bound_agents(&runner_blueprint(false)).expect("resolvable Runner refs");
        let bindings = worker_bindings_from_bound_agents(&bound);
        let wb = bindings.get("worker").expect("worker binding present");
        assert!(wb.request_digest.is_some());
        assert!(wb.requested_model.is_none());
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
            runner: None,
            runner_ref: None,
            verdict: None,
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

    // ──────────────────────────────────────────────────────────────────
    // GH #46 Milestone 2 Done Criteria #3 (semantics-match): `resolve_runner`
    // ──────────────────────────────────────────────────────────────────

    /// `resolve_runner` (in `mlua-swarm-schema`) must synthesize the exact
    /// same `(variant, tools)` pair `derive_worker_bindings` does today for
    /// every agent whose Runner comes solely from the legacy
    /// `AgentProfile.worker_binding` fallback (tier 3 of the cascade) — a
    /// machine-checked guard against the two paths silently drifting apart
    /// once a future change touches one but forgets the other, mirroring
    /// `crate::core::explain`'s
    /// `explain_agent_ctx_matches_derive_agent_ctx_semantics` drift guard.
    /// This is a read-only cross-check: it exercises the schema crate's
    /// pure resolver against real Blueprints, without touching the launch
    /// path itself (Milestone 3 scope).
    #[test]
    fn resolve_runner_legacy_fallback_matches_derive_worker_bindings_semantics() {
        fn legacy_agent(name: &str, variant: &str, tools: Vec<&str>) -> AgentDef {
            AgentDef {
                name: name.to_string(),
                kind: AgentKind::Operator,
                spec: json!({}),
                profile: Some(AgentProfile {
                    worker_binding: Some(variant.to_string()),
                    tools: tools.into_iter().map(str::to_string).collect(),
                    ..Default::default()
                }),
                meta: None,
                runner: None,
                runner_ref: None,
                verdict: None,
            }
        }

        let blueprint = bp(
            step("planner", path("$.in"), path("$.out")),
            vec![
                legacy_agent("planner", "mse-worker-planner", vec!["Read", "Grep"]),
                legacy_agent("coder", "mse-worker-coder", vec![]),
                agent("no-binding", "echo"),
            ],
        );

        let derived = derive_worker_bindings(&blueprint);

        for agent_def in &blueprint.agents {
            let resolved = resolve_runner(&blueprint, agent_def).expect("no unresolved refs");
            match derived.get(&agent_def.name) {
                Some(binding) => {
                    assert_eq!(
                        resolved,
                        Some(Runner::WsClaudeCode {
                            variant: binding.variant.clone(),
                            tools: binding.tools.clone(),
                        }),
                        "resolve_runner must synthesize the same WsClaudeCode Runner \
                         derive_worker_bindings produces for agent '{}'",
                        agent_def.name
                    );
                }
                None => {
                    assert_eq!(
                        resolved, None,
                        "agent '{}' has no derive_worker_bindings entry, so resolve_runner \
                         must resolve to None too (no other tier declared)",
                        agent_def.name
                    );
                }
            }
        }
    }

    #[test]
    fn ws_operator_runner_projects_into_the_existing_spawn_binding() {
        let mut blueprint = bp(
            step("reviewer", path("$.in"), path("$.out")),
            vec![agent("reviewer", "echo")],
        );
        blueprint.agents[0].runner = Some(Runner::WsOperator {
            variant: "mse-reviewer".to_string(),
            tools: vec!["Read".to_string(), "Grep".to_string()],
        });

        let derived = derive_worker_bindings(&blueprint);
        let binding = derived
            .get("reviewer")
            .expect("ws_operator must feed the canonical spawn binding path");
        assert_eq!(binding.variant, "mse-reviewer");
        assert_eq!(binding.tools, ["Read", "Grep"]);
    }
}
