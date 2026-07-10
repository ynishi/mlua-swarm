//! Middleware overlay — cross-cutting concerns (Audit / MainAI / Senior /
//! LongHold).
//!
//! Ships four `SpawnerLayer` implementations plus the `SpawnerStack` builder.
//! Some layers key off `Ctx.operator.kind` and only fire for
//! `MainAi` / `Composite` sessions; others (`Audit` / `LongHold`) apply
//! uniformly across every kind.
//!
//! # Extension discipline — this layer is THE extension point (canonical)
//!
//! Background: an earlier iteration grew a verdict-specialised machinery
//! (`judgment.rs` canonical type + 3-form parser + `state.agent_verdicts`
//! map + dedicated accessor) that re-interpreted agent output *inside the
//! engine core* and banned string-literal conds in favour of a Blueprint
//! compile-layer translation. That whole complex was dismantled: the value
//! it added over plain data was zero, while it created an IN-side dialect
//! that every consumer had to learn. The design conclusion is a
//! three-principle layering:
//!
//! 1. **IN is immutable, canonical form is JSON.** `Blueprint` /
//!    `mlua_flow_ir::Node` are plain serde data. No compile pass, no schema
//!    field that the engine expands, no Rust helper that builds `Expr`s.
//!    Flow control is written literally in Flow.ir:
//!    `Eq(Path("$.<step>.verdict"), Lit("blocked"))` — domain verdicts are
//!    plain strings inside step output, consumed by plain conds.
//! 2. **Generation (authoring sugar) lives OUT**, on the consumer side
//!    (e.g. a vendored pure-Lua builder that prints Blueprint JSON). It
//!    never leaks into engine / schema crates, whatever language it is
//!    written in — the ban is on the *placement*, not the language.
//! 3. **Runtime extension lives HERE, as a `SpawnerLayer`.** A middleware
//!    (or any future extension mechanism) may interpret the *results* of a
//!    Flow.ir run — `Ctx`, the `output_tail`, `Final { ok }` — in its own
//!    way and transform them. What it must NOT do:
//!    - introduce a new dialect on the IN side (schema fields / node
//!      rewriting / cond translation) — extensions read and transform, the
//!      wire format stays plain Flow.ir + JSON;
//!    - hide its effect: overrides are *appended* to the output tail
//!      (e.g. `SeniorEscalationMiddleware` pushes an override `Final`
//!      rather than mutating the recorded one), so the trace stays
//!      replayable and the flow stays observable;
//!    - accumulate private engine state keyed by its own semantics (the
//!      `agent_verdicts` anti-pattern) — state lives in ctx / output store
//!      as plain data.
//!
//! `AgentResolver`, `ProjectNameAliasMiddleware`, `SinkMiddleware`,
//! `InputInjectMiddleware`, `LuaMiddleware`, `SeniorEscalationMiddleware`,
//! `TaskInputMiddleware` all follow this shape: edit `ctx` / wrap the
//! worker, call the inner spawner, append observable output. Note
//! `LuaMiddleware`'s scripts are host-constructed — embedding Lua source
//! in a Blueprint is the IN-side dialect this discipline forbids, and
//! would require its own guard design if ever revisited).

pub mod agent_context;
pub mod input_inject;
pub mod lua_layer;
pub mod project_name_alias;
pub mod resolver;
pub mod sink;
pub mod task_input;
pub mod worker_binding;

use crate::blueprint::compiler::CompiledAgentTable;
use crate::blueprint::{AuditDef, AuditMode};
use crate::core::ctx::{Ctx, OperatorKind};
use crate::core::engine::Engine;
use crate::core::state::{DispatchOutcome, Event, TaskSpec};
use crate::types::{CapToken, StepId};
use crate::worker::adapter::{SpawnError, SpawnerAdapter};
use crate::worker::output::{ContentRef, OutputEvent};
use crate::worker::{wrap_join, MiddlewareWorker, Worker, WorkerJoinHandler};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

/// Pull the terminal `Final` event's `(value, ok)` out of the tail (works
/// for both `Inline` and `FileRef` content).
async fn pull_final_value_ok(
    engine: &Engine,
    task_id: &StepId,
    attempt: u32,
) -> Option<(Value, bool)> {
    let tail = engine.output_tail(task_id, attempt).await;
    tail.iter().rev().find_map(|ev| match ev {
        OutputEvent::Final {
            content: ContentRef::Inline { value },
            ok,
        } => Some((value.clone(), *ok)),
        OutputEvent::Final {
            content: ContentRef::FileRef { path, .. },
            ok,
        } => Some((serde_json::json!({"file_ref": path.to_string_lossy()}), *ok)),
        _ => None,
    })
}

/// Layer trait — one middleware stage wrapping a `SpawnerAdapter`.
pub trait SpawnerLayer: Send + Sync + 'static {
    /// Wraps `inner` in this layer's behaviour, returning a new
    /// `SpawnerAdapter` that delegates to `inner` (directly or via
    /// `wrap_join`) while adding this layer's cross-cutting effect.
    fn wrap(&self, inner: Arc<dyn SpawnerAdapter>) -> Arc<dyn SpawnerAdapter>;
}

/// Stack builder that layers `SpawnerLayer`s on top of a base adapter.
///
/// Each `.layer(...)` call wraps a new **outer** stage — same ergonomics as
/// `tower::ServiceBuilder`.
pub struct SpawnerStack {
    inner: Arc<dyn SpawnerAdapter>,
}

impl SpawnerStack {
    /// Starts a stack with `base` as the innermost adapter.
    pub fn new(base: Arc<dyn SpawnerAdapter>) -> Self {
        Self { inner: base }
    }

    /// Wraps the current stack with a statically-typed `SpawnerLayer`,
    /// becoming the new outermost stage.
    pub fn layer<L: SpawnerLayer>(mut self, layer: L) -> Self {
        self.inner = layer.wrap(self.inner);
        self
    }

    /// Dynamically-typed variant taking `Arc<dyn SpawnerLayer>`. Used via
    /// the `LayerRegistry` resolution path (where a factory returns
    /// `Arc<dyn ...>`).
    pub fn layer_dyn(mut self, layer: Arc<dyn SpawnerLayer>) -> Self {
        self.inner = layer.wrap(self.inner);
        self
    }

    /// Finishes the stack, returning the fully-wrapped adapter.
    pub fn build(self) -> Arc<dyn SpawnerAdapter> {
        self.inner
    }
}

// ─── SpawnerLayerFactory + LayerRegistry ─────────────────────────────────
//
// # Design rationale
//
// Wiring is assembled per-launch through `TaskLaunchService.launch`:
//
//   Compiler.compile(bp) ─┬─→ compiled.router (CompiledAgentTable: agent name → SpawnerAdapter dispatch)
//                         │
//                         │   service::linker::link(router, bp.spawner_hints.layers, &engine)
//                         │     internal:
//                         │       SpawnerStack::new(router)
//                         │         .layer_dyn(base_factory_n(engine))   ← every LayerRegistry.base entry
//                         │         .layer_dyn(hint_factory(engine))     ← resolves each bp.spawner_hints.layers key
//                         │         .build()
//                         ▼
//                   EngineDispatcher::with_spawner(engine, op_token, stacked)
//                         ▼
//                   engine.dispatch_attempt_with(op_token, task_id, &stacked)
//
// # base vs hint — when to use each
//
// - **base layer**: wrapped around every Blueprint. Example: AuditMiddleware
//   (a mandatory EventLog audit). The caller registers with
//   `LayerRegistry::with_base(|e| Arc::new(AuditMiddleware::new(e.event_tx())))`.
//
// - **hint layer**: wrapped **only when the Blueprint declares the key** in
//   `spawner_hints.layers`. Examples: MainAIMiddleware /
//   SeniorEscalationMiddleware / OperatorDelegateMiddleware. The Blueprint
//   only declares a capability key (e.g. `"main_ai"`) without knowing the
//   implementation; the engine-side LayerRegistry resolves key → factory,
//   keeping the pure Flow layer separate from implementation details.
//
// # Factory pattern (handles layers that need Engine context)
//
// We do not hold `Arc<dyn SpawnerLayer>` directly because some layers
// depend on the engine instance — for example AuditMiddleware needs
// `engine.event_tx()` and can only be built after the engine exists. A
// factory closure defers construction: the Layer instance is created only
// when the engine is handed in.

/// Factory closure for a `SpawnerLayer`. The caller registers these at
/// startup, and they are called with the engine context at bind time.
/// Stateless layers can use `|_engine| Arc::new(MyLayer)`; layers that need
/// something like `event_tx` should do `|engine| Arc::new(MyLayer::new(engine.event_tx()))`.
pub type LayerFactory =
    Arc<dyn Fn(&crate::core::engine::Engine) -> Arc<dyn SpawnerLayer> + Send + Sync + 'static>;

/// Registry of `LayerFactory`s, split into `base` (always applied) and
/// `hints` (applied only when a Blueprint declares the matching key in
/// `spawner_hints.layers`). See the module-level `# Factory pattern`
/// notes above for why factories rather than pre-built layers.
#[derive(Default, Clone)]
pub struct LayerRegistry {
    base: Vec<LayerFactory>,
    hints: std::collections::HashMap<String, LayerFactory>,
}

impl LayerRegistry {
    /// Empty registry (no base layers, no hint layers).
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a base layer factory that is applied on every Blueprint bind
    /// (for layers that must fire for every task — e.g. `AuditMiddleware`).
    pub fn with_base<F>(mut self, factory: F) -> Self
    where
        F: Fn(&crate::core::engine::Engine) -> Arc<dyn SpawnerLayer> + Send + Sync + 'static,
    {
        self.base.push(Arc::new(factory));
        self
    }

    /// Register a layer factory addressable by hint key. If
    /// `Blueprint.spawner_hints.layers` lists the same key, it is wrapped at
    /// bind time; otherwise it is a no-op.
    pub fn with_hint<F>(mut self, key: impl Into<String>, factory: F) -> Self
    where
        F: Fn(&crate::core::engine::Engine) -> Arc<dyn SpawnerLayer> + Send + Sync + 'static,
    {
        self.hints.insert(key.into(), Arc::new(factory));
        self
    }

    /// All registered base-layer factories, in registration order.
    pub fn base_factories(&self) -> &[LayerFactory] {
        &self.base
    }

    /// Looks up the hint-layer factory registered under `key`, if any.
    pub fn lookup_hint(&self, key: &str) -> Option<&LayerFactory> {
        self.hints.get(key)
    }
}

// ─── AuditMiddleware (pushes into the EventLog broadcast path) ────────────

/// Mandatory base layer that emits `Event::TaskAttemptStarted` on every
/// spawn, before delegating. This is the audit trail's entry point into
/// the EventLog broadcast channel.
pub struct AuditMiddleware {
    /// Broadcast sender the EventLog subscribes to.
    pub event_tx: broadcast::Sender<Event>,
}

impl AuditMiddleware {
    /// Wraps a broadcast sender to notify on every spawn.
    pub fn new(event_tx: broadcast::Sender<Event>) -> Self {
        Self { event_tx }
    }
}

impl SpawnerLayer for AuditMiddleware {
    fn wrap(&self, inner: Arc<dyn SpawnerAdapter>) -> Arc<dyn SpawnerAdapter> {
        Arc::new(AuditWrapped {
            inner,
            event_tx: self.event_tx.clone(),
        })
    }
}

struct AuditWrapped {
    inner: Arc<dyn SpawnerAdapter>,
    event_tx: broadcast::Sender<Event>,
}

#[async_trait]
impl SpawnerAdapter for AuditWrapped {
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: StepId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        let _ = self.event_tx.send(Event::TaskAttemptStarted {
            task_id: task_id.clone(),
            attempt,
        });
        self.inner.spawn(engine, ctx, task_id, attempt, token).await
    }
}

// ─── MainAIMiddleware (fires SpawnHook before/after for MainAI/Composite) ─

/// Hint layer that fires `ctx.operator.spawn_hook.before`/`after` around
/// a spawn, but only for `MainAi` / `Composite` sessions. No-op for
/// other kinds (still delegates, just skips the hook calls).
pub struct MainAIMiddleware;

impl MainAIMiddleware {
    /// Stateless constructor.
    pub fn new() -> Self {
        Self
    }
}

impl Default for MainAIMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

impl SpawnerLayer for MainAIMiddleware {
    fn wrap(&self, inner: Arc<dyn SpawnerAdapter>) -> Arc<dyn SpawnerAdapter> {
        Arc::new(MainAIWrapped { inner })
    }
}

struct MainAIWrapped {
    inner: Arc<dyn SpawnerAdapter>,
}

#[async_trait]
impl SpawnerAdapter for MainAIWrapped {
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: StepId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        let mainai = matches!(
            ctx.operator.kind,
            OperatorKind::MainAi | OperatorKind::Composite
        );
        if mainai {
            if let Some(hook) = &ctx.operator.spawn_hook {
                hook.before(ctx)
                    .await
                    .map_err(SpawnError::RejectedByMiddleware)?;
            }
        }

        let handle = self
            .inner
            .spawn(engine, ctx, task_id.clone(), attempt, token)
            .await?;

        if !mainai {
            return Ok(handle);
        }
        let Some(hook) = ctx.operator.spawn_hook.clone() else {
            return Ok(handle);
        };

        // Wrap the completion signal and call hook.after on finish.
        // Pull the last Final from engine.output_tail as the value.
        let ctx_clone = ctx.clone();
        let engine_clone = engine.clone();
        let task_id_clone = task_id.clone();
        Ok(wrap_join(handle, move |signal| {
            let hook = hook.clone();
            let ctx_clone = ctx_clone.clone();
            let engine_clone = engine_clone.clone();
            let task_id_clone = task_id_clone.clone();
            async move {
                let v = match &signal {
                    Ok(()) => pull_final_value_ok(&engine_clone, &task_id_clone, attempt)
                        .await
                        .map(|(v, _)| v)
                        .unwrap_or(Value::Null),
                    Err(e) => Value::String(e.to_string()),
                };
                let _ = hook.after(&ctx_clone, &v).await;
                signal
            }
        }))
    }
}

// ─── SeniorEscalationMiddleware ───────────────────────────────────────────
//
// When a spawn's completion is `ok=false` and `ctx.operator.senior_bridge` is
// Some, this auxiliary layer calls `SeniorBridge.ask`, merges the answer into
// `WorkerResult.value` under `"senior_answer"`, and upgrades the result to
// `ok=true`. Retry / re-dispatch is the engine (operator) side's job; this
// layer only injects fresh material for that decision.

/// Hint layer: on `ok=false` completion with `ctx.operator.senior_bridge`
/// set, asks the bridge for guidance and pushes an override `Final`
/// (`ok=true`) carrying `senior_answer`. See the module comment above
/// this type for the full contract.
pub struct SeniorEscalationMiddleware;

impl SeniorEscalationMiddleware {
    /// Stateless constructor.
    pub fn new() -> Self {
        Self
    }
}

impl Default for SeniorEscalationMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

impl SpawnerLayer for SeniorEscalationMiddleware {
    fn wrap(&self, inner: Arc<dyn SpawnerAdapter>) -> Arc<dyn SpawnerAdapter> {
        Arc::new(SeniorWrapped { inner })
    }
}

struct SeniorWrapped {
    inner: Arc<dyn SpawnerAdapter>,
}

#[async_trait]
impl SpawnerAdapter for SeniorWrapped {
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: StepId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        let bridge = ctx.operator.senior_bridge.clone();
        let task_id_for_hook = task_id.clone();
        let engine_clone = engine.clone();
        let token_clone = token.clone();
        let handle = self
            .inner
            .spawn(engine, ctx, task_id, attempt, token)
            .await?;
        let Some(bridge) = bridge else {
            return Ok(handle);
        };
        Ok(wrap_join(handle, move |signal| {
            let bridge = bridge.clone();
            let task_id = task_id_for_hook.clone();
            let engine = engine_clone.clone();
            let token = token_clone.clone();
            async move {
                signal?;
                // Read the existing Final.
                let last = pull_final_value_ok(&engine, &task_id, attempt).await;
                if let Some((value, false)) = last {
                    // ok=false: escalate to senior and push an override Final.
                    let question = serde_json::json!({
                        "reason": "worker reported ok=false",
                        "value": value.clone(),
                    });
                    if let Ok(answer) = bridge.ask(&task_id, question).await {
                        let override_val = serde_json::json!({
                            "original": value,
                            "senior_answer": answer,
                        });
                        let _ = engine
                            .submit_output(
                                &token,
                                &task_id,
                                attempt,
                                OutputEvent::Final {
                                    content: ContentRef::Inline {
                                        value: override_val,
                                    },
                                    ok: true,
                                },
                            )
                            .await;
                    }
                }
                Ok(())
            }
        }))
    }
}

// ─── OperatorDelegateMiddleware (delegates the whole spawn to an external Operator when one is attached) ──

/// When `ctx.operator.operator.is_some()` (the session has an Operator
/// backend), **bypass** `inner.spawn`, call `operator.execute(ctx, prompt)`,
/// and box the result up as a `WorkerHandle`. In other words: the path that
/// hands "this spawn" to whatever external Operator backend the engine has
/// registered.
///
/// # Independent of `OperatorKind` (Operator is a generic abstraction)
///
/// An earlier implementation gated on `kind == MainAi | Composite`, which
/// tied the `Operator` abstraction to an "AI driver" assumption — a design
/// weakness. The `Operator` trait is a generic **external processing backend**
/// (LLM, human, external resource, side-effectful operation — anything), and
/// is orthogonal to the kind axis.
///
/// The current implementation decides solely on `operator.is_some()`:
/// - Automate session + operator backend registered → delegate
///   (pure external-execution delegation).
/// - MainAi session + operator backend registered → delegate.
/// - Any kind + `operator` `None` → pass through (normal `inner.spawn`).
///
/// `kind` still matters as a firing condition for `SpawnHook`s over in
/// `MainAIMiddleware`, but this middleware ignores it.
///
/// # Split of responsibilities with `OperatorSpawner`
///
/// The two axes exist for different reasons:
///
/// - **This middleware — the Blueprint-global (session) axis.** Delegate every
///   agent to the same Operator backend. The `operator_backend_id` is set
///   at session-attach time; `ctx.agent` is ignored and every spawn in that
///   session is routed through the operator (e.g. a MainAI-wide driver, or a
///   human-wide console). The Blueprint doesn't have to talk about `kind` —
///   it just declares the capability hint `"operator_delegate"` (keeping the
///   Blueprint clean).
///
/// - **`OperatorSpawner` — the AgentSpec axis.** Each `AgentDef` bakes its
///   own Operator backend. `kind = Operator` `AgentDef`s pick a backend via
///   `spec.operator_ref`; the compiler bakes an `Arc<dyn Operator>` into
///   `routes[agent_name]`. Agents loaded via the `agent.md` loader come in
///   through this path (their default is `kind = Operator`).
///
/// # Exclusivity
///
/// When both are effective — this middleware's hint is declared, the session
/// has an operator backend, **and** the Blueprint has a `kind = Operator`
/// `AgentDef` — this middleware sits at the outer end of the stack and
/// **completely bypasses** `inner.spawn`. The `OperatorSpawner` is never
/// reached, so a double fire cannot occur by construction; the AgentSpec
/// axis is inert. Consistent use means picking one axis per use case.
pub struct OperatorDelegateMiddleware;

impl OperatorDelegateMiddleware {
    /// Stateless constructor.
    pub fn new() -> Self {
        Self
    }
}

impl Default for OperatorDelegateMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

impl SpawnerLayer for OperatorDelegateMiddleware {
    fn wrap(&self, inner: Arc<dyn SpawnerAdapter>) -> Arc<dyn SpawnerAdapter> {
        Arc::new(OperatorDelegateWrapped { inner })
    }
}

struct OperatorDelegateWrapped {
    inner: Arc<dyn SpawnerAdapter>,
}

#[async_trait]
impl SpawnerAdapter for OperatorDelegateWrapped {
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: StepId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        // Kind-independent: we decide purely on whether an operator backend is
        // registered on the session. `kind` matters for SpawnHook-style layers
        // (MainAIMiddleware); this middleware does not consult it.
        let Some(operator) = ctx.operator.operator.clone() else {
            return self.inner.spawn(engine, ctx, task_id, attempt, token).await;
        };

        // Delegate: same shape as OperatorSpawner — fetch_prompt + operator.execute + Final emit.
        let prompt = engine
            .fetch_prompt(&token, &task_id)
            .await
            .map_err(|e| SpawnError::Internal(format!("fetch_prompt: {e}")))?;

        // Resolve the Blueprint-baked worker binding injected into
        // `ctx.meta.runtime` by `WorkerBindingMiddleware` (launch-time layer,
        // built from `AgentDef.profile.worker_binding`). Absent key = agent
        // declared no binding → hand `None` and let binding-requiring
        // backends fail loud (`requires_worker_binding`). A present-but-
        // malformed value is a wiring bug, not a degrade case — fail here.
        let worker: Option<crate::operator::WorkerBinding> = match ctx
            .meta
            .runtime
            .get(crate::middleware::worker_binding::WORKER_BINDING_KEY)
        {
            Some(v) => Some(serde_json::from_value(v.clone()).map_err(|e| {
                SpawnError::Internal(format!(
                    "ctx.meta.runtime['{}'] for agent '{}' is malformed: {e}",
                    crate::middleware::worker_binding::WORKER_BINDING_KEY,
                    ctx.agent
                ))
            })?),
            None => None,
        };

        let engine_clone = engine.clone();
        let token_clone = token.clone();
        let token_for_op = token.clone();
        let task_id_clone = task_id.clone();
        let ctx_clone = ctx.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let cancel = tokio_util::sync::CancellationToken::new();
        let cancel_inner = cancel.clone();
        let worker_id = crate::types::WorkerId::new();
        // issue #11: WorkerId was minted but never observable anywhere;
        // surface it in the trace log, tied to the step it serves.
        tracing::debug!(worker_id = %worker_id, step_id = %task_id, "worker spawned (delegate axis)");

        tokio::spawn(async move {
            let result: Result<
                crate::worker::adapter::WorkerResult,
                crate::worker::adapter::WorkerError,
            > = tokio::select! {
                // OperatorDelegateMiddleware = session-global Operator delegation.
                // Baking per-AgentDef profile.system_prompt is OperatorSpawner's
                // job; this path has no per-agent spawner, so system stays None.
                // The worker binding, however, IS resolved on this axis now:
                // `WorkerBindingMiddleware` (launch-time layer) injects the
                // Blueprint-baked binding into ctx.meta.runtime and we forward
                // it here — the delegate axis is a first-class variant-dispatch
                // path, not a binding-less fallback (issue 45db42a7).
                // We hand the capability token (Role::Worker, 1800s TTL —
                // minted by `Engine::dispatch_attempt_with`) to the
                // operator as `worker_token` — thin-spawn operators (e.g. a
                // WebSocket-backed operator session) forward it to the SubAgent
                // via encode(), while Operator impls that call the LLM directly
                // may ignore it.
                r = operator.execute(&ctx_clone, None, prompt, worker, token_for_op) => r,
                _ = cancel_inner.cancelled() => Err(crate::worker::adapter::WorkerError::Cancelled),
            };
            if let Ok(wr) = &result {
                // If the SubAgent has already pushed a Final through
                // /v1/worker/result or /v1/worker/submit POST, skip a second
                // emit here — the POST value is the canonical one (protocol
                // design intent). Operator impls that never POST (e.g. tests
                // and inline Operators) still get the fallback emit.
                let tail = engine_clone.output_tail(&task_id_clone, attempt).await;
                let has_final = tail
                    .iter()
                    .any(|ev| matches!(ev, crate::worker::output::OutputEvent::Final { .. }));
                if !has_final {
                    let ev = crate::worker::output::OutputEvent::Final {
                        content: crate::worker::output::ContentRef::Inline {
                            value: wr.value.clone(),
                        },
                        ok: wr.ok,
                    };
                    let _ = engine_clone
                        .submit_output(&token_clone, &task_id_clone, attempt, ev)
                        .await;
                }
            }
            let signal: Result<(), crate::worker::adapter::WorkerError> = result.map(|_| ());
            let _ = tx.send(signal);
        });

        Ok(Box::new(MiddlewareWorker {
            handler: WorkerJoinHandler {
                worker_id,
                cancel,
                completion: rx,
            },
        }))
    }
}

// ─── LongHoldMiddleware (warns on the EventLog if completion time exceeds default_hold) ─

/// Base layer that emits `Event::TaskAttemptCompleted` with a
/// `long_hold_warn` marker when a spawn's completion takes longer than
/// `default_hold`. Purely observational — it never alters the signal or
/// blocks completion.
pub struct LongHoldMiddleware {
    /// Threshold above which a completion is flagged as long-held.
    pub default_hold: Duration,
    /// Broadcast sender the EventLog subscribes to.
    pub event_tx: broadcast::Sender<Event>,
}

impl LongHoldMiddleware {
    /// Sets the hold threshold and the event sender to warn through.
    pub fn new(default_hold: Duration, event_tx: broadcast::Sender<Event>) -> Self {
        Self {
            default_hold,
            event_tx,
        }
    }
}

impl SpawnerLayer for LongHoldMiddleware {
    fn wrap(&self, inner: Arc<dyn SpawnerAdapter>) -> Arc<dyn SpawnerAdapter> {
        Arc::new(LongHoldWrapped {
            inner,
            default_hold: self.default_hold,
            event_tx: self.event_tx.clone(),
        })
    }
}

struct LongHoldWrapped {
    inner: Arc<dyn SpawnerAdapter>,
    default_hold: Duration,
    event_tx: broadcast::Sender<Event>,
}

#[async_trait]
impl SpawnerAdapter for LongHoldWrapped {
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: StepId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        let handle = self
            .inner
            .spawn(engine, ctx, task_id.clone(), attempt, token)
            .await?;
        let started = Instant::now();
        let default_hold = self.default_hold;
        let event_tx = self.event_tx.clone();
        let task_id_inner = task_id.clone();
        Ok(wrap_join(handle, move |signal| {
            let elapsed = started.elapsed();
            let default_hold = default_hold;
            let event_tx = event_tx.clone();
            let task_id_inner = task_id_inner.clone();
            async move {
                if elapsed > default_hold {
                    let _ = event_tx.send(Event::TaskAttemptCompleted {
                        task_id: task_id_inner,
                        attempt,
                        result: serde_json::json!({
                            "long_hold_warn": true,
                            "elapsed_ms": elapsed.as_millis() as u64,
                            "default_hold_ms": default_hold.as_millis() as u64,
                        }),
                    });
                }
                signal
            }
        }))
    }
}

// ─── AfterRunAuditMiddleware (GH #34: Blueprint-declared after-run audit hooks) ──

/// One-paragraph instruction handed to the audit agent alongside the
/// structured `after_run_audit` envelope (see [`AfterRunAuditMiddleware`]
/// for the full contract).
const AUDIT_INSTRUCTION: &str = "Inspect this step's transcript/output for degradations, tool \
    failures, or silent fallbacks, and emit your findings as a structured JSON object in your \
    final output.";

/// Blueprint-declared after-run audit hook layer (GH #34).
///
/// Wraps every spawn. After a matched step's inner signal SETTLES (`Ok`),
/// dispatches the Blueprint-declared audit agent(s) for that step as an
/// independent, synthetic sub-task — via `Engine::start_task` +
/// `Engine::dispatch_attempt_with`, the same "recursive swarming" path a
/// `Role::Worker` token is allow-listed for (`types::WORKER_SWARM_VERBS`) —
/// reusing the AUDITED step's own worker token. Findings are persisted as
/// an `OutputEvent::Artifact` named `"audit:<step_ref>"` on the AUDITED
/// step's own output tail. Downstream steps read those findings via
/// `WorkerPayload.context.steps["audit:<step_ref>"]` (fold-final drops
/// them from the BP-chain value, but `Engine::submit_output` dual-writes
/// every Artifact into `OutputStore` keyed by its own name — see
/// `src/core/engine.rs`).
///
/// # Invariant (observational-only, binding — issue.md #1/#2/#3)
///
/// Every failure in the audit path (spawn/dispatch failure, audit worker
/// failure, submit failure) is `tracing::warn!`-logged and swallowed. The
/// audited step's own signal, returned to the caller, is ALWAYS the
/// original inner signal, bit-for-bit — same `signal?; ...; Ok(())` shape
/// as `SeniorEscalationMiddleware` above, so an inner `Err` short-circuits
/// the audit entirely and propagates untouched, and an inner `Ok(())`
/// always returns as `Ok(())` regardless of what happens inside the audit.
///
/// # Recursion guard
///
/// An agent name declared as an `AuditDef.agent` (an "auditor") is never
/// itself audited — even if a real flow Step happens to be named after a
/// declared auditor (e.g. a Blueprint audits every step via `steps: None`
/// and also has a flow Step literally named after the auditor). The
/// audit's OWN dispatch additionally never revisits this layer to begin
/// with: it goes through `router` (the raw `CompiledAgentTable` —
/// `Compiler::compile`'s name→adapter table), not the fully-layered stack
/// this middleware itself sits inside, so there is no path back into
/// `AfterRunAuditWrapped::spawn` from an audit dispatch. The name-set
/// check in `audit_def_matches_step` (below) is a second, independent
/// belt-and-suspenders guard for the real-flow-Step scenario.
///
/// Wired conditionally by `service::task_launch::TaskLaunchService::launch`
/// (empty `Blueprint.audits` → no layer, invariant #4 — byte-identical
/// behavior).
pub struct AfterRunAuditMiddleware {
    defs: Vec<AuditDef>,
    router: Arc<CompiledAgentTable>,
}

impl AfterRunAuditMiddleware {
    /// Holds the audit defs relevant to wiring, and the compiled
    /// name→adapter table (`Compiler::compile`'s `CompiledBlueprint.router`)
    /// used to dispatch each audit agent by name via
    /// `Engine::start_task` + `Engine::dispatch_attempt_with` — the
    /// narrowest handle that resolves an agent name to its
    /// `SpawnerAdapter` without re-entering this same layer (see the
    /// module comment's Recursion guard section).
    pub fn new(defs: Vec<AuditDef>, router: Arc<CompiledAgentTable>) -> Self {
        Self { defs, router }
    }
}

impl SpawnerLayer for AfterRunAuditMiddleware {
    fn wrap(&self, inner: Arc<dyn SpawnerAdapter>) -> Arc<dyn SpawnerAdapter> {
        Arc::new(AfterRunAuditWrapped {
            inner,
            defs: self.defs.clone(),
            router: self.router.clone(),
        })
    }
}

struct AfterRunAuditWrapped {
    inner: Arc<dyn SpawnerAdapter>,
    defs: Vec<AuditDef>,
    router: Arc<CompiledAgentTable>,
}

/// Whether `def` applies to a step whose agent ref is `step_ref`. `None`,
/// or a list containing the literal `"*"`, matches every step; otherwise
/// only an exact name match. `Some(vec![])` (declared-but-empty) matches
/// nothing.
fn audit_def_matches_step(def: &AuditDef, step_ref: &str) -> bool {
    match &def.steps {
        None => true,
        Some(list) => list.iter().any(|s| s == "*" || s == step_ref),
    }
}

/// Dispatches one audit agent as an independent sub-task and — best
/// effort — appends its findings as an `OutputEvent::Artifact` named
/// `"audit:<step_ref>"` on the AUDITED task's own output tail. See the
/// module comment above [`AfterRunAuditMiddleware`] for the full
/// contract; every failure path here only `tracing::warn!`s and returns
/// (invariant #1 — the audited step's outcome is unaffected regardless).
#[allow(clippy::too_many_arguments)]
async fn run_one_audit(
    engine: &Engine,
    router: &Arc<CompiledAgentTable>,
    token: &CapToken,
    audited_task_id: &StepId,
    attempt: u32,
    step_ref: &str,
    audit_agent: &str,
    directive: Value,
) {
    let spec = TaskSpec {
        agent: audit_agent.to_string(),
        initial_directive: directive,
        step_ctx: None,
    };
    let audit_task_id = match engine.start_task(token, spec).await {
        Ok(tid) => tid,
        Err(e) => {
            tracing::warn!(
                audited_task_id = %audited_task_id,
                step_ref,
                audit_agent,
                error = %e,
                "AfterRunAuditMiddleware: start_task failed for audit agent; \
                 audited step's outcome is unaffected"
            );
            return;
        }
    };
    let spawner: Arc<dyn SpawnerAdapter> = router.clone();
    let findings = match engine
        .dispatch_attempt_with(token, &audit_task_id, &spawner, None)
        .await
    {
        Ok(DispatchOutcome::Pass(v)) | Ok(DispatchOutcome::Blocked(v)) => v,
        Ok(other) => {
            tracing::warn!(
                audited_task_id = %audited_task_id,
                step_ref,
                audit_agent,
                outcome = ?other,
                "AfterRunAuditMiddleware: audit agent did not settle (Pass/Blocked); \
                 audited step's outcome is unaffected"
            );
            return;
        }
        Err(e) => {
            tracing::warn!(
                audited_task_id = %audited_task_id,
                step_ref,
                audit_agent,
                error = %e,
                "AfterRunAuditMiddleware: dispatch_attempt_with failed for audit agent; \
                 audited step's outcome is unaffected"
            );
            return;
        }
    };
    if let Err(e) = engine
        .submit_output(
            token,
            audited_task_id,
            attempt,
            OutputEvent::Artifact {
                name: format!("audit:{step_ref}"),
                content: ContentRef::Inline { value: findings },
            },
        )
        .await
    {
        tracing::warn!(
            audited_task_id = %audited_task_id,
            step_ref,
            audit_agent,
            error = %e,
            "AfterRunAuditMiddleware: submit_output failed for audit findings; \
             audited step's outcome is unaffected"
        );
    }
}

#[async_trait]
impl SpawnerAdapter for AfterRunAuditWrapped {
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: StepId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        let step_ref = ctx.agent.clone();
        let handle = self
            .inner
            .spawn(engine, ctx, task_id.clone(), attempt, token.clone())
            .await?;

        // Recursion guard (see the module comment's Recursion guard
        // section): an auditor's own spawn is never itself audited.
        let is_auditor = self.defs.iter().any(|d| d.agent == step_ref);
        let matched: Vec<AuditDef> = if is_auditor {
            Vec::new()
        } else {
            self.defs
                .iter()
                .filter(|d| audit_def_matches_step(d, &step_ref))
                .cloned()
                .collect()
        };

        if matched.is_empty() {
            return Ok(handle);
        }

        let engine = engine.clone();
        let router = self.router.clone();
        Ok(wrap_join(handle, move |signal| async move {
            // INVARIANT (issue.md #1): `signal?` propagates an inner
            // `Err` untouched (short-circuits the audit entirely); an
            // inner `Ok(())` falls through to the `Ok(())` at the bottom
            // of this block — byte-identical to what we matched on. The
            // returned signal is ALWAYS the original inner signal,
            // bit-for-bit.
            signal?;

            let (final_value, ok) = pull_final_value_ok(&engine, &task_id, attempt)
                .await
                .unwrap_or((Value::Null, true));

            for def in matched {
                let directive = serde_json::json!({
                    "kind": "after_run_audit",
                    "task_id": task_id.to_string(),
                    "step_ref": step_ref.clone(),
                    "attempt": attempt,
                    "ok": ok,
                    "final_value": final_value.clone(),
                    "instruction": AUDIT_INSTRUCTION,
                });
                match def.mode {
                    AuditMode::Sync => {
                        run_one_audit(
                            &engine, &router, &token, &task_id, attempt, &step_ref, &def.agent,
                            directive,
                        )
                        .await;
                    }
                    AuditMode::Async => {
                        let engine = engine.clone();
                        let router = router.clone();
                        let token = token.clone();
                        let task_id = task_id.clone();
                        let step_ref = step_ref.clone();
                        let agent = def.agent.clone();
                        tokio::spawn(async move {
                            run_one_audit(
                                &engine, &router, &token, &task_id, attempt, &step_ref, &agent,
                                directive,
                            )
                            .await;
                        });
                    }
                }
            }
            Ok(())
        }))
    }
}

// Boundary regression spec for the delegate-axis worker-binding handoff
// (issue 45db42a7): OperatorDelegateMiddleware must forward the binding
// injected into ctx.meta.runtime by WorkerBindingMiddleware — both the
// hit path (Some(worker) reaches Operator::execute) and the absent path
// (None reaches it), plus fail-loud on a malformed value.
#[cfg(test)]
mod operator_delegate_worker_binding_tests {
    use super::*;
    use crate::core::config::EngineCfg;
    use crate::core::state::TaskSpec;
    use crate::operator::WorkerBinding;
    use crate::types::Role;
    use crate::worker::adapter::{WorkerError, WorkerResult};
    use std::sync::Mutex;

    /// Operator stub recording the `worker` argument it was executed with.
    struct RecordingOperator {
        seen: Arc<Mutex<Option<Option<WorkerBinding>>>>,
    }

    #[async_trait]
    impl crate::operator::Operator for RecordingOperator {
        async fn execute(
            &self,
            _ctx: &Ctx,
            _system: Option<String>,
            _prompt: Value,
            worker: Option<WorkerBinding>,
            _worker_token: CapToken,
        ) -> Result<WorkerResult, WorkerError> {
            *self.seen.lock().unwrap() = Some(worker);
            Ok(WorkerResult {
                value: Value::Null,
                ok: true,
            })
        }
    }

    /// Inner spawner that must never be reached when an operator is attached.
    struct MustNotSpawn;

    #[async_trait]
    impl SpawnerAdapter for MustNotSpawn {
        async fn spawn(
            &self,
            _engine: &Engine,
            _ctx: &Ctx,
            _task_id: StepId,
            _attempt: u32,
            _token: CapToken,
        ) -> Result<Box<dyn Worker>, SpawnError> {
            panic!("delegate axis must bypass inner.spawn when an operator is attached");
        }
    }

    async fn seeded_engine() -> (Engine, CapToken, StepId) {
        let engine = Engine::new(EngineCfg::default());
        let op_token = engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach");
        let task_id = engine
            .start_task(
                &op_token,
                TaskSpec {
                    agent: "planner".to_string(),
                    initial_directive: "do the thing".into(),
                    step_ctx: None,
                },
            )
            .await
            .expect("start_task");
        // Mint + register a worker token the same way
        // `dispatch_attempt_with` does — the spawner path runs with a
        // `Role::Worker` token (FetchPrompt is worker-verb-gated).
        let worker_token = engine.signer().session(
            format!("worker-of-{task_id}"),
            Role::Worker,
            vec!["*".into()],
            Duration::from_secs(600),
        );
        let fp = worker_token.fingerprint();
        let record = crate::core::state::CapTokenRecord::from_worker_token(
            worker_token.clone(),
            task_id.clone(),
        );
        engine
            .with_state("test.mint_worker", move |s| {
                s.tokens.insert(fp, record);
            })
            .await
            .expect("mint worker token");
        (engine, worker_token, task_id)
    }

    fn delegate_stack() -> Arc<dyn SpawnerAdapter> {
        OperatorDelegateMiddleware::new().wrap(Arc::new(MustNotSpawn))
    }

    async fn recorded_worker(
        seen: &Arc<Mutex<Option<Option<WorkerBinding>>>>,
    ) -> Option<WorkerBinding> {
        for _ in 0..100 {
            if let Some(w) = seen.lock().unwrap().clone() {
                return w;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("operator.execute was never called within 1s");
    }

    #[tokio::test]
    async fn forwards_ctx_injected_binding_to_operator_execute() {
        let (engine, token, task_id) = seeded_engine().await;
        let seen = Arc::new(Mutex::new(None));
        let op = Arc::new(RecordingOperator { seen: seen.clone() });

        let mut ctx = Ctx::new(task_id.clone(), 1, "planner");
        ctx.operator.operator = Some(op);
        ctx.meta.runtime.insert(
            crate::middleware::worker_binding::WORKER_BINDING_KEY.to_string(),
            serde_json::to_value(WorkerBinding {
                variant: "mse-worker-coder".to_string(),
                tools: vec!["Edit".to_string()],
            })
            .unwrap(),
        );

        let _worker = delegate_stack()
            .spawn(&engine, &ctx, task_id, 1, token)
            .await
            .expect("delegate spawn ok");

        let got = recorded_worker(&seen).await.expect("binding forwarded");
        assert_eq!(got.variant, "mse-worker-coder");
        assert_eq!(got.tools, vec!["Edit".to_string()]);
    }

    #[tokio::test]
    async fn absent_binding_stays_none_no_silent_default() {
        let (engine, token, task_id) = seeded_engine().await;
        let seen = Arc::new(Mutex::new(None));
        let op = Arc::new(RecordingOperator { seen: seen.clone() });

        let mut ctx = Ctx::new(task_id.clone(), 1, "planner");
        ctx.operator.operator = Some(op);

        let _worker = delegate_stack()
            .spawn(&engine, &ctx, task_id, 1, token)
            .await
            .expect("delegate spawn ok");

        assert!(
            recorded_worker(&seen).await.is_none(),
            "no binding declared must reach the operator as None (fail-loud stays downstream)"
        );
    }

    #[tokio::test]
    async fn malformed_binding_fails_loud_before_execute() {
        let (engine, token, task_id) = seeded_engine().await;
        let seen = Arc::new(Mutex::new(None));
        let op = Arc::new(RecordingOperator { seen: seen.clone() });

        let mut ctx = Ctx::new(task_id.clone(), 1, "planner");
        ctx.operator.operator = Some(op);
        ctx.meta.runtime.insert(
            crate::middleware::worker_binding::WORKER_BINDING_KEY.to_string(),
            serde_json::json!({ "not_a_binding": true }),
        );

        let err = match delegate_stack()
            .spawn(&engine, &ctx, task_id, 1, token)
            .await
        {
            Ok(_) => panic!("malformed binding must fail the spawn"),
            Err(e) => e,
        };
        let msg = format!("{err:?}");
        assert!(
            msg.contains("worker_binding") && msg.contains("malformed"),
            "error must name the malformed key: {msg}"
        );
        assert!(
            seen.lock().unwrap().is_none(),
            "operator.execute must not run on malformed binding"
        );
    }
}

// ─── GH #34: `AfterRunAuditMiddleware` ─────────────────────────────────────
#[cfg(test)]
mod after_run_audit_tests {
    use super::*;
    use crate::blueprint::compiler::{Compiler, RustFnInProcessSpawnerFactory, SpawnerRegistry};
    use crate::blueprint::{
        current_schema_version, AgentDef, AgentKind, Blueprint, BlueprintMetadata, CompilerHints,
        CompilerStrategy,
    };
    use crate::core::config::EngineCfg;
    use crate::types::Role;
    use crate::worker::adapter::{WorkerError as StubWorkerError, WorkerResult};
    use mlua_flow_ir::Node as FlowNode;

    fn rustfn_agent(name: &str, fn_id: &str) -> AgentDef {
        AgentDef {
            name: name.to_string(),
            kind: AgentKind::RustFn,
            spec: serde_json::json!({ "fn_id": fn_id }),
            profile: None,
            meta: None,
        }
    }

    fn minimal_bp(agents: Vec<AgentDef>, audits: Vec<AuditDef>) -> Blueprint {
        crate::blueprint::Blueprint {
            schema_version: current_schema_version(),
            id: "afterrun-audit-ut".into(),
            // Unused directly by these tests — each dispatches one agent's
            // step at a time via `run_step` (start_task +
            // dispatch_attempt_with), the same shape
            // `EngineDispatcher::dispatch` uses per flow.ir Step. The
            // AfterRunAudit layer keys off `ctx.agent`/`AuditDef.steps`
            // only, so a real multi-step flow.ir Seq is not needed to
            // exercise it.
            flow: FlowNode::Seq { children: vec![] },
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
            audits,
            degradation_policy: None,
        }
    }

    /// Registers three stub `RustFn` workers shared across this module's
    /// tests: `"worker"` (ok, generic step body), `"auditor"` (ok, fixed
    /// findings), `"bad-auditor"` (always fails — GH #34 test 2).
    fn test_registry() -> SpawnerRegistry {
        let factory = RustFnInProcessSpawnerFactory::new()
            .register_fn("worker", |_inv| async move {
                Ok(WorkerResult {
                    value: serde_json::json!({ "result": "done" }),
                    ok: true,
                })
            })
            .register_fn("auditor", |_inv| async move {
                Ok(WorkerResult {
                    value: serde_json::json!({ "finding": "clean" }),
                    ok: true,
                })
            })
            .register_fn("bad-auditor", |_inv| async move {
                Err(StubWorkerError::Failed("boom".to_string()))
            });
        let mut reg = SpawnerRegistry::new();
        reg.register::<RustFnInProcessSpawnerFactory>(Arc::new(factory));
        reg
    }

    /// Dispatches `agent_name` as its own independent single-step task
    /// through `spawner` (start_task + dispatch_attempt_with — the same
    /// shape `EngineDispatcher::dispatch` uses per flow.ir Step), reusing
    /// `op_token` (a `Role::Operator` token — `start_task` mints a fresh
    /// `Role::Worker` token per attempt internally, exactly as
    /// `dispatch_attempt_with` always does).
    async fn run_step(
        engine: &Engine,
        op_token: &CapToken,
        agent_name: &str,
        spawner: &Arc<dyn SpawnerAdapter>,
    ) -> (
        StepId,
        Result<DispatchOutcome, crate::core::errors::EngineError>,
    ) {
        let task_id = engine
            .start_task(
                op_token,
                TaskSpec {
                    agent: agent_name.to_string(),
                    initial_directive: serde_json::json!("go"),
                    step_ctx: None,
                },
            )
            .await
            .expect("start_task");
        let outcome = engine
            .dispatch_attempt_with(op_token, &task_id, spawner, None)
            .await;
        (task_id, outcome)
    }

    async fn seeded_op_token(engine: &Engine) -> CapToken {
        engine
            .attach("ut-op", Role::Operator, Duration::from_secs(30))
            .await
            .expect("attach")
    }

    fn find_artifact(tail: &[OutputEvent], name: &str) -> Option<Value> {
        tail.iter().find_map(|ev| match ev {
            OutputEvent::Artifact {
                name: n,
                content: ContentRef::Inline { value },
            } if n == name => Some(value.clone()),
            _ => None,
        })
    }

    /// GH #34 test 1: a matched step's Sync-mode audit appends
    /// `audit:<step_ref>` to the AUDITED step's own output tail, and the
    /// audited step's own outcome is unaffected (the worker's own value).
    #[tokio::test]
    async fn audit_fires_after_step_and_appends_artifact() {
        let agents = vec![
            rustfn_agent("worker", "worker"),
            rustfn_agent("auditor", "auditor"),
        ];
        let audits = vec![AuditDef {
            agent: "auditor".to_string(),
            steps: None,
            mode: AuditMode::Sync,
        }];
        let bp = minimal_bp(agents, audits.clone());
        let compiled = Compiler::new(test_registry())
            .compile(&bp)
            .expect("compile");
        let spawner: Arc<dyn SpawnerAdapter> =
            AfterRunAuditMiddleware::new(audits, compiled.router.clone())
                .wrap(compiled.router.clone());

        let engine = Engine::new(EngineCfg::default());
        let op_token = seeded_op_token(&engine).await;
        let (task_id, outcome) = run_step(&engine, &op_token, "worker", &spawner).await;
        match outcome.expect("dispatch ok") {
            DispatchOutcome::Pass(v) => assert_eq!(v, serde_json::json!({ "result": "done" })),
            other => panic!("expected Pass (the worker's own outcome), got {other:?}"),
        }

        let tail = engine.output_tail(&task_id, 1).await;
        let findings =
            find_artifact(&tail, "audit:worker").expect("audit:worker artifact must be appended");
        assert_eq!(findings, serde_json::json!({ "finding": "clean" }));
    }

    /// GH #34 test 2: an auditor that errors never alters the audited
    /// step's own outcome or status — the failure is swallowed (a warn is
    /// logged, not asserted here — this asserts outcome + artifact-absence
    /// only, per the subtask spec).
    #[tokio::test]
    async fn audit_failure_never_alters_outcome() {
        let agents = vec![
            rustfn_agent("worker", "worker"),
            rustfn_agent("bad-auditor", "bad-auditor"),
        ];
        let audits = vec![AuditDef {
            agent: "bad-auditor".to_string(),
            steps: None,
            mode: AuditMode::Sync,
        }];
        let bp = minimal_bp(agents, audits.clone());
        let compiled = Compiler::new(test_registry())
            .compile(&bp)
            .expect("compile");
        let spawner: Arc<dyn SpawnerAdapter> =
            AfterRunAuditMiddleware::new(audits, compiled.router.clone())
                .wrap(compiled.router.clone());

        let engine = Engine::new(EngineCfg::default());
        let op_token = seeded_op_token(&engine).await;
        let (task_id, outcome) = run_step(&engine, &op_token, "worker", &spawner).await;
        match outcome.expect("audited step's dispatch must still succeed despite auditor failure") {
            DispatchOutcome::Pass(v) => assert_eq!(v, serde_json::json!({ "result": "done" })),
            other => panic!("expected Pass identical to a no-audit run, got {other:?}"),
        }

        let tail = engine.output_tail(&task_id, 1).await;
        assert!(
            find_artifact(&tail, "audit:worker").is_none(),
            "auditor failure must not append an audit artifact"
        );
    }

    /// GH #34 test 3 (mirrors `audits_absent_no_layer`, exercised more
    /// directly against `derive_audits` in
    /// `service::task_launch::tests`): with no `AuditDef` at all, the base
    /// (unwrapped) adapter chain behaves identically — no artifact is ever
    /// appended.
    #[tokio::test]
    async fn no_audit_defs_appends_no_artifact() {
        let agents = vec![rustfn_agent("worker", "worker")];
        let bp = minimal_bp(agents, vec![]);
        let compiled = Compiler::new(test_registry())
            .compile(&bp)
            .expect("compile");
        let spawner: Arc<dyn SpawnerAdapter> = compiled.router.clone();

        let engine = Engine::new(EngineCfg::default());
        let op_token = seeded_op_token(&engine).await;
        let (task_id, outcome) = run_step(&engine, &op_token, "worker", &spawner).await;
        assert!(matches!(
            outcome.expect("dispatch ok"),
            DispatchOutcome::Pass(_)
        ));

        let tail = engine.output_tail(&task_id, 1).await;
        assert!(
            !tail
                .iter()
                .any(|ev| matches!(ev, OutputEvent::Artifact { .. })),
            "no audits declared must never append any audit artifact"
        );
    }

    /// GH #34 test 4: `AuditDef.steps` filters which step names an audit
    /// applies to — only the listed step gets an artifact.
    #[tokio::test]
    async fn steps_filter_respected() {
        let agents = vec![
            rustfn_agent("a", "worker"),
            rustfn_agent("b", "worker"),
            rustfn_agent("auditor", "auditor"),
        ];
        let audits = vec![AuditDef {
            agent: "auditor".to_string(),
            steps: Some(vec!["b".to_string()]),
            mode: AuditMode::Sync,
        }];
        let bp = minimal_bp(agents, audits.clone());
        let compiled = Compiler::new(test_registry())
            .compile(&bp)
            .expect("compile");
        let spawner: Arc<dyn SpawnerAdapter> =
            AfterRunAuditMiddleware::new(audits, compiled.router.clone())
                .wrap(compiled.router.clone());

        let engine = Engine::new(EngineCfg::default());
        let op_token = seeded_op_token(&engine).await;

        let (task_a, outcome_a) = run_step(&engine, &op_token, "a", &spawner).await;
        outcome_a.expect("dispatch a ok");
        let (task_b, outcome_b) = run_step(&engine, &op_token, "b", &spawner).await;
        outcome_b.expect("dispatch b ok");

        let tail_a = engine.output_tail(&task_a, 1).await;
        assert!(
            find_artifact(&tail_a, "audit:a").is_none(),
            "step 'a' is not listed in AuditDef.steps and must not be audited"
        );
        let tail_b = engine.output_tail(&task_b, 1).await;
        assert!(
            find_artifact(&tail_b, "audit:b").is_some(),
            "step 'b' is listed in AuditDef.steps and must be audited"
        );
    }

    /// GH #34 test 5: an agent name declared as an auditor is never
    /// itself audited, even when a Blueprint audits every step
    /// (`steps: None`) and a real flow Step happens to dispatch that same
    /// agent name.
    #[tokio::test]
    async fn auditor_not_audited() {
        let agents = vec![
            rustfn_agent("worker", "worker"),
            rustfn_agent("auditor", "auditor"),
        ];
        let audits = vec![AuditDef {
            agent: "auditor".to_string(),
            steps: None,
            mode: AuditMode::Sync,
        }];
        let bp = minimal_bp(agents, audits.clone());
        let compiled = Compiler::new(test_registry())
            .compile(&bp)
            .expect("compile");
        let spawner: Arc<dyn SpawnerAdapter> =
            AfterRunAuditMiddleware::new(audits, compiled.router.clone())
                .wrap(compiled.router.clone());

        let engine = Engine::new(EngineCfg::default());
        let op_token = seeded_op_token(&engine).await;

        // The worker step gets audited as usual.
        let (worker_task, worker_outcome) = run_step(&engine, &op_token, "worker", &spawner).await;
        worker_outcome.expect("dispatch worker ok");
        let worker_tail = engine.output_tail(&worker_task, 1).await;
        assert!(find_artifact(&worker_tail, "audit:worker").is_some());

        // A real flow Step happening to dispatch the "auditor" agent name
        // must not recurse into auditing itself.
        let (auditor_task, auditor_outcome) =
            run_step(&engine, &op_token, "auditor", &spawner).await;
        auditor_outcome.expect("dispatch auditor ok");
        let auditor_tail = engine.output_tail(&auditor_task, 1).await;
        assert!(
            find_artifact(&auditor_tail, "audit:auditor").is_none(),
            "an agent declared as an auditor must never audit itself"
        );
    }
}
