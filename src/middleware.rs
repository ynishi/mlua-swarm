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
//! `InputInjectMiddleware`, `LuaMiddleware`, `SeniorEscalationMiddleware`
//! all follow this shape: edit `ctx` / wrap the worker, call the inner
//! spawner, append observable output. Note `LuaMiddleware`'s scripts are
//! host-constructed — embedding Lua source in a Blueprint is the IN-side
//! dialect this discipline forbids, and would require its own guard
//! design if ever revisited).

pub mod input_inject;
pub mod lua_layer;
pub mod project_name_alias;
pub mod resolver;
pub mod sink;

use crate::core::ctx::{Ctx, OperatorKind};
use crate::core::engine::Engine;
use crate::core::state::Event;
use crate::types::{CapToken, TaskId};
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
    task_id: &TaskId,
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
        task_id: TaskId,
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
        task_id: TaskId,
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
        task_id: TaskId,
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
        task_id: TaskId,
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

        let engine_clone = engine.clone();
        let token_clone = token.clone();
        let token_for_op = token.clone();
        let task_id_clone = task_id.clone();
        let ctx_clone = ctx.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let cancel = tokio_util::sync::CancellationToken::new();
        let cancel_inner = cancel.clone();
        let worker_id = crate::types::WorkerId::new();

        tokio::spawn(async move {
            let result: Result<
                crate::worker::adapter::WorkerResult,
                crate::worker::adapter::WorkerError,
            > = tokio::select! {
                // OperatorDelegateMiddleware = session-global Operator delegation.
                // Baking per-AgentDef profile.system_prompt is OperatorSpawner's job;
                // this path has no profile, so we execute with system=None.
                // We hand the capability token (Role::Worker, 600s TTL) to the
                // operator as `worker_token` — thin-spawn operators (e.g. a
                // WebSocket-backed operator session) forward it to the SubAgent
                // via encode(), while Operator impls that call the LLM directly
                // may ignore it.
                r = operator.execute(&ctx_clone, None, prompt, token_for_op) => r,
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
        task_id: TaskId,
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
