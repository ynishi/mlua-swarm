//! Operator abstraction.
//!
//! ## Roles
//!
//! - **Spawners** (`SpawnerAdapter`) do not know about `Operator` `kind`s.
//!   Ordinary dispatches are handled by `ProcessSpawner` /
//!   `InProcSpawner` / etc.
//! - `OperatorSpawner` is the `SpawnerAdapter` that routes dispatches
//!   through an operator. It holds an `Arc<dyn Operator>` and does one
//!   thing: hand every spawn request to that operator's `execute`. It
//!   still does not know the operator's `kind` (`MainAi` / `Human` /
//!   `Automate` / `Composite`).
//! - The `Operator` trait itself returns a `WorkerResult`, as a
//!   synchronous backend. Implementations are free per kind — a `MainAi`
//!   operator might round-trip through Claude via an HTTP callback, a
//!   `Human` operator might prompt on a CLI, an `Automate` operator
//!   might delegate to a different spawner, and so on.
//!
//! Which dispatches go through the `OperatorSpawner` is decided at the
//! flow.ir layer (designer + hints + Swarm compiler). The algocline
//! strategy side never says "hand this to the operator" — a firm
//! separation of concerns.

pub mod render;

pub use render::{render_system, slots_from_prompt, RenderError};

use crate::core::ctx::Ctx;
use crate::core::engine::Engine;
use crate::types::{CapToken, StepId, WorkerId};
use crate::worker::adapter::{SpawnError, SpawnerAdapter, WorkerError, WorkerResult};
use crate::worker::output::{ContentRef, OutputEvent};
use crate::worker::{Worker, WorkerJoinHandler};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// Worker binding baked from `AgentDef.profile` at compile time — which
/// worker variant the operator backend must run, plus the tool surface
/// the Blueprint declared for this agent.
///
/// `variant` is mse domain vocabulary; backend-specific terms (e.g. the
/// Claude Code Agent tool's `subagent_type` parameter) belong to the
/// rendering boundary (`operator_ws::session` directive render), not here.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerBinding {
    /// Worker variant name (for the Claude Code backend this maps onto
    /// the Agent tool `subagent_type` at directive-render time).
    #[serde(alias = "subagent_type")]
    pub variant: String,
    /// Tool list declared in `AgentDef.profile.tools` (informational
    /// for the MainAI / observability; the SubAgent's own frontmatter
    /// is what actually grants tools).
    pub tools: Vec<String>,
    /// Digest of the immutable declaration-only `BoundAgent` snapshot this
    /// binding was resolved from (`sha256:<hex>`). Carried into the spawn
    /// frame so a non-strict Operator can correlate the request and self-check
    /// its own environment against it. Like `tools`, this is informational —
    /// a self-check input for the Operator, not a Server-enforced gate. `None`
    /// on construction sites that have no snapshot (compile-time / test paths).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_digest: Option<crate::blueprint::BindingDigest>,
    /// Model name or tier declared in `AgentDef.profile.model`, forwarded so
    /// the Operator can compare the requested model against what its
    /// environment actually runs. Informational (self-check input), not an
    /// enforcement field. `None` when the profile declares no model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_model: Option<String>,
}

/// The `Operator` trait: takes a spawn request and returns a
/// `WorkerResult`. The backend for `OperatorSpawner`. Implementations
/// are free to differ per kind; the spawner just calls `execute` and
/// stays out of the internals.
///
/// Arguments — a two-slot payload plus `worker_token` (the thin path
/// was added later) plus `worker` (the Blueprint-baked binding, added
/// later still):
///
/// - `system`: the agent persona — the rendered value of
///   `AgentDef.profile.system_prompt` after template expansion. `None`
///   means no profile. Expected to map straight onto the LLM API's
///   system message; direct-LLM operators consume this.
/// - `prompt`: task-specific intent — `TaskSpec.initial_directive`,
///   pulled server-side via `engine.fetch_prompt`. Expected to map
///   straight onto the LLM API's user message.
/// - `worker`: the compile-time-baked [`WorkerBinding`] (subagent type +
///   declared tools) resolved from `AgentDef.profile.worker_binding`.
///   `None` for agents whose profile has no `worker_binding` set.
///   Backends that require one (see [`Operator::requires_worker_binding`])
///   must fail loud rather than silently degrade when this is `None`.
/// - `worker_token`: a capability token (`Role::Worker`,
///   `scopes = ["*"]`, TTL from
///   [`EngineCfg::worker_token_ttl_secs`](crate::EngineCfg) — default
///   1800s). Thin-path operators (a `a WebSocket-backed operator session`,
///   for instance) `encode()` this token and hand it to the MainAI
///   WebSocket client, so the SubAgent can hit `/v1/worker/prompt` +
///   `/v1/worker/result` with `Authorization: Bearer <encoded>`.
///   Direct-LLM operators may ignore it.
///
/// The trait passes both slots so the same signature works for the
/// thin path and the direct path; the implementation picks which one
/// it takes (consume the server-rendered `system` directly, or forward
/// the token and let the client fetch).
#[async_trait]
pub trait Operator: Send + Sync {
    /// Executes one spawn request against this operator's backend and
    /// returns the resulting `WorkerResult` (or a `WorkerError` if the
    /// backend failed). See the trait doc above for the meaning of each
    /// argument.
    async fn execute(
        &self,
        ctx: &Ctx,
        system: Option<String>,
        prompt: Value,
        worker: Option<WorkerBinding>,
        worker_token: CapToken,
    ) -> Result<WorkerResult, WorkerError>;

    /// Whether this operator backend requires a non-`None` `worker`
    /// binding to execute at all. `false` by default (direct-LLM
    /// operators consume `system` / `prompt` directly and have no
    /// SubAgent to dispatch). WS thin-path operators override this to
    /// `true` — the compiler uses it to fail loud at `compile()` time
    /// when `AgentDef.profile.worker_binding` is absent, rather than
    /// silently degrading at dispatch time.
    fn requires_worker_binding(&self) -> bool {
        false
    }
}

/// Resolves the `Arc<dyn Operator>` a Blueprint-declared Operator seat
/// dispatches through.
///
/// # Why a hook instead of a lookup
///
/// `AgentDef.spec.operator_ref` names a **seat** (one of
/// `Blueprint.operators[]`), not a backend. Historically
/// [`OperatorSpawnerFactory`](crate::OperatorSpawnerFactory) answered it by
/// looking the name up in its own `id → Arc<dyn Operator>` map, which baked
/// whichever session held that name at compile time into
/// `routes[agent_name]` for the whole Run — so re-assigning the seat later
/// could not change where a dispatch went (model §4.3 **A10**: *the
/// destination is not baked in*).
///
/// A host that records seat holders per Run installs a resolver instead. It
/// is handed the seat name and returns the indirection that performs the
/// per-dispatch holder lookup (`mlua-swarm-server`'s `AssigneeRouter`), so
/// what gets baked is **which seat**, never **who holds it**.
///
/// The hook lives here rather than in the compiler because the resolving
/// type needs a `RunStore` and the live session registry, both of which are
/// the host's; the core only needs to know that something can answer
/// "operator for seat *X*".
pub trait OperatorSlotResolver: Send + Sync {
    /// The backend for `slot`, or `None` when this resolver cannot serve
    /// that seat — which fails the compile loudly (there is deliberately no
    /// fallback to the factory's own registry, since falling back is how a
    /// dispatch ends up somewhere the caller never named).
    fn resolve(&self, slot: &str) -> Option<Arc<dyn Operator>>;
}

/// A `SpawnerAdapter` implementation that hands the dispatch off to an
/// `Arc<dyn Operator>`.
///
/// `OperatorSpawner` itself does not inspect the operator's `kind` —
/// `MainAi` / `Human` / `Automate` / `Composite` all go through the same
/// path, and the operator implementation absorbs the differences.
///
/// # Position — the AgentSpec-axis Operator path
///
/// Use this type on the path that **bakes a separate Operator backend
/// into every `AgentDef`**. For an `AgentKind::Operator` `AgentDef`, the
/// `OperatorSpawnerFactory` produces one with
/// `OperatorSpawner::new(op, system_prompt, worker_binding)` and places it
/// in `routes[agent_name]`. Agents flowing in through the `agent.md`
/// loader default to `kind = Operator`, so they land here.
///
/// This is now the **only** way a dispatch reaches an `Operator`. There
/// used to be a paired Blueprint-global (session) axis,
/// `crate::middleware::OperatorDelegateMiddleware`, which registered one
/// backend on the session and applied it uniformly to every agent; when
/// both were effective it sat at the outer end of the stack and bypassed
/// `inner.spawn`, leaving this type inert. It was removed — it resolved
/// its destination from the launch record rather than the Run's seat (so
/// a handover could not move it) and, having no per-agent spawner, could
/// not render or bake an agent's `system_prompt`. With one axis left,
/// the exclusivity question it created goes away with it.
pub struct OperatorSpawner {
    operator: Arc<dyn Operator>,
    /// The compile-time-baked `AgentDef.profile.system_prompt` — the
    /// agent's persona. If `Some`, it takes priority at spawn time; if
    /// `None`, we fall back to `fetch_prompt` (`initial_directive`).
    system_prompt: Option<String>,
    /// The compile-time-baked worker binding — resolved from
    /// `AgentDef.profile.worker_binding` by `OperatorSpawnerFactory`.
    /// Passed straight through to `Operator::execute` on every spawn.
    worker_binding: Option<WorkerBinding>,
}

impl OperatorSpawner {
    /// Binds an operator backend plus an optional compile-time
    /// `system_prompt` template (rendered per-spawn via `render_system`)
    /// and an optional compile-time-baked `worker_binding`.
    pub fn new(
        operator: Arc<dyn Operator>,
        system_prompt: Option<String>,
        worker_binding: Option<WorkerBinding>,
    ) -> Self {
        Self {
            operator,
            system_prompt,
            worker_binding,
        }
    }
}

#[async_trait]
impl SpawnerAdapter for OperatorSpawner {
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: StepId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        // By convention the spawner pulls `prompt`
        // through `fetch_prompt`. The `system_prompt` (from
        // `AgentDef.profile`) travels on the other slot — sibling to the
        // AgentBlock path's `BlockConfig.context` / `.prompt` split.
        let prompt = engine
            .fetch_prompt(&token, &task_id)
            .await
            .map_err(|e| SpawnError::Internal(format!("fetch_prompt: {e}")))?;

        // Render the `system_prompt` template.
        // Expand the prompt into a slot map and hand the template to
        // minijinja. The syntax used inside the agent.md body is
        // Jinja2-compatible (`{{ directive }}` / `{% if intent %}` /
        // `{{ x | upper }}`), with strict undefined variables and
        // auto-escape disabled.
        let system = match self.system_prompt.as_deref() {
            Some(tmpl) => {
                let slots = render::slots_from_prompt(&prompt);
                let rendered = render::render_system(tmpl, &slots)
                    .map_err(|e| SpawnError::Internal(format!("render system_prompt: {e}")))?;
                Some(rendered)
            }
            None => None,
        };

        // Bake the rendered `system`
        // into engine state so the SubAgent can fetch it alongside
        // `prompt` on the `HTTP /v1/worker/prompt` path. Failures are
        // fail-loud via `SpawnError::Internal` — no silent fallback.
        engine
            .bake_worker_system_prompt(&task_id, attempt, system.clone())
            .await
            .map_err(|e| SpawnError::Internal(format!("bake system_prompt: {e}")))?;

        let op = self.operator.clone();
        let engine_clone = engine.clone();
        let token_clone = token.clone();
        let token_for_op = token.clone();
        let task_id_clone = task_id.clone();
        let ctx_clone = ctx.clone();
        let worker_binding = self.worker_binding.clone();
        let (tx, rx) = oneshot::channel();
        let cancel = CancellationToken::new();
        let cancel_inner = cancel.clone();
        let worker_id = WorkerId::new();
        // issue #11: surface the minted WorkerId in the trace log.
        tracing::debug!(worker_id = %worker_id, step_id = %task_id, "worker spawned (operator spawner)");

        tokio::spawn(async move {
            let result: Result<WorkerResult, WorkerError> = tokio::select! {
                r = op.execute(&ctx_clone, system, prompt, worker_binding, token_for_op) => r,
                _ = cancel_inner.cancelled() => Err(WorkerError::Cancelled),
            };
            // Per-step run stats: the WS operator ack may attach
            // harness-reported SubAgent usage — forward it to the
            // engine so the dispatcher's outcome fold lands it on the
            // terminal StepEntry. Even without stats attached,
            // `ensure_worker_kind` guarantees the `worker_kind:
            // "operator"` label always rides (same funnel as the InProc /
            // subprocess fold sites).
            let result = result.map(|wr| wr.ensure_worker_kind("operator"));
            if let Ok(wr) = &result {
                if let Some(stats) = wr.stats.clone() {
                    engine_clone
                        .record_worker_stats(&task_id_clone, attempt, stats)
                        .await;
                }
            }
            // Emit `WorkerResult` → `OutputEvent::Final` in
            // parallel. If the SubAgent already
            // pushed a `Final` via HTTP (`/v1/worker/result` or
            // `/v1/worker/submit`), skip. The POSTed value is canonical
            // — protocol.rs L107-110 design intent. Only operator
            // implementations that do not POST (tests, inline
            // operators) need this fallback emit.
            if let Ok(wr) = &result {
                let tail = engine_clone.output_tail(&task_id_clone, attempt).await;
                let has_final = tail
                    .iter()
                    .any(|ev| matches!(ev, OutputEvent::Final { .. }));
                if !has_final {
                    let ev = OutputEvent::Final {
                        content: ContentRef::Inline {
                            value: wr.value.clone(),
                        },
                        ok: wr.ok,
                    };
                    // The capability this closure captured was minted
                    // before `execute` was called, and `execute` is where
                    // the wait lives: the WS backend parks a spawn frame
                    // for the length of a client disconnect with no
                    // deadline, while the token counts down
                    // `EngineCfg::worker_token_ttl_secs`. Past that,
                    // `submit_output`'s `verify_token_for_task` rejects it
                    // with `TokenExpired` — and because this emit is the
                    // fallback for a SubAgent that never POSTed a `Final`
                    // of its own, nothing else would write one: the
                    // attempt's whole result would be lost to the clock,
                    // not to anything about the work. Re-mint against the
                    // record the engine already holds, which cannot widen
                    // the grant (same subject / role / scopes / bound
                    // task; only `expire_at` moves — see
                    // `Engine::remint_worker_token`).
                    //
                    // The un-lapsed case is left alone deliberately rather
                    // than re-minting unconditionally: every operator
                    // dispatch reaches this line, and `remint` leaves the
                    // old record in place by design, so minting on each
                    // one would grow `EngineState.tokens` by a spare entry
                    // per step for no gain.
                    let submit_token = if token_clone.is_expired(crate::types::now_unix()) {
                        match engine_clone.remint_worker_token(&token_clone).await {
                            Ok(fresh) => fresh,
                            Err(e) => {
                                // Nothing here is retryable with the token
                                // in hand — it is already past its TTL, so
                                // submitting with it is a call known to
                                // fail. Say what was lost instead.
                                tracing::error!(
                                    step_id = %task_id_clone,
                                    attempt,
                                    error = %e,
                                    "operator fallback Final dropped: the worker capability \
                                     lapsed while the spawn frame was parked and could not be \
                                     re-minted; this attempt has no Final"
                                );
                                let _ = tx.send(result.map(|_| ()));
                                return;
                            }
                        }
                    } else {
                        token_clone.clone()
                    };
                    // GH #51: `submit_output` embeds the completion-time
                    // verdict-contract check (see
                    // `Engine::verdict_contract_completion_check`'s doc)
                    // — this fallback emit is gated by it exactly like
                    // the HTTP routes are, with zero new WS protocol
                    // surface. On rejection the `Final` is simply never
                    // written: `output_tail` stays without one, and the
                    // downstream `dispatch_attempt_with` Final-pull
                    // naturally treats the attempt as incomplete — no new
                    // reject-back-to-client message is synthesized (the
                    // deliberate "Zero flow-ir changes" design choice, not
                    // a gap to fill).
                    if let Err(e) = engine_clone
                        .submit_output(&submit_token, &task_id_clone, attempt, ev)
                        .await
                    {
                        // A contract rejection is this gate working, and
                        // reads as a `warn`. Anything else means the
                        // `Final` went missing for a reason nobody chose,
                        // and the old wording — which named the verdict
                        // gate unconditionally — would have reported a
                        // lapsed token as a rejected value. Split them so
                        // the log says which happened.
                        if matches!(
                            e,
                            crate::core::errors::EngineError::VerdictValueRejected { .. }
                                | crate::core::errors::EngineError::VerdictPartMissing { .. }
                        ) {
                            tracing::warn!(
                                step_id = %task_id_clone,
                                attempt,
                                error = %e,
                                "operator fallback Final rejected by verdict-contract \
                                 completion gate"
                            );
                        } else {
                            tracing::error!(
                                step_id = %task_id_clone,
                                attempt,
                                error = %e,
                                "operator fallback Final was not written; this attempt has \
                                 no Final"
                            );
                        }
                    }
                }
            }
            let signal: Result<(), WorkerError> = result.map(|_| ());
            let _ = tx.send(signal);
        });

        Ok(Box::new(OperatorWorker {
            handler: WorkerJoinHandler {
                worker_id,
                cancel,
                completion: rx,
            },
        }))
    }
}

/// Concrete Worker type for the Operator kind — wraps the async
/// `Operator::execute` call. This represents the handle for a task
/// backed by an operator (SDK, WebSocket bridge, direct LLM call, etc.)
/// and embeds a `WorkerJoinHandler` that carries the async signal.
pub struct OperatorWorker {
    /// The completion-signal handle for this operator call's spawned
    /// task.
    pub handler: WorkerJoinHandler,
}

#[async_trait]
impl Worker for OperatorWorker {
    fn id(&self) -> &WorkerId {
        &self.handler.worker_id
    }
    fn cancel_token(&self) -> CancellationToken {
        self.handler.cancel.clone()
    }
    async fn join(self: Box<Self>) -> Result<(), WorkerError> {
        self.handler.await_completion().await
    }
}

// ─── the fallback Final outliving its capability ──────────────────────────
//
// `OperatorSpawner::spawn`'s completion path writes a `Final` for the
// operator whose SubAgent never POSTed one. It does that with the token it
// was handed at spawn time — and `Operator::execute` is allowed to take
// arbitrarily long (the WS backend parks a spawn frame across a client
// disconnect with no deadline), so by the time the fallback runs, that
// token may be past `EngineCfg::worker_token_ttl_secs`. These tests hold
// the two halves apart: the parked case must still land its `Final`, and
// the ordinary case must not start paying for a re-mint it does not need.
#[cfg(test)]
mod parked_fallback_capability_tests {
    use super::*;
    use crate::core::config::EngineCfg;
    use crate::core::state::TaskSpec;
    use crate::types::Role;
    use crate::worker::adapter::SpawnerAdapter;
    use std::time::Duration;

    /// The worker-token TTL these tests run against. One second is short
    /// enough to outlive in a unit test and long enough that the dispatch
    /// preamble (mint → `fetch_prompt` → spawn) is comfortably inside it,
    /// so a park is what expires the token and not test scheduling.
    const TTL_SECS: u64 = 1;

    /// An `Operator` that succeeds without ever writing a `Final` of its
    /// own — the only shape for which the fallback emit exists — after
    /// holding for `hold`. `hold` past the TTL reproduces the park.
    struct SilentOperator {
        hold: Duration,
    }

    #[async_trait]
    impl Operator for SilentOperator {
        async fn execute(
            &self,
            _ctx: &Ctx,
            _system: Option<String>,
            _prompt: Value,
            _worker: Option<WorkerBinding>,
            _worker_token: CapToken,
        ) -> Result<WorkerResult, WorkerError> {
            tokio::time::sleep(self.hold).await;
            Ok(WorkerResult {
                value: serde_json::json!({"held": true}),
                ok: true,
                stats: None,
            })
        }
    }

    /// Dispatch one attempt through an `OperatorSpawner` wrapping a
    /// [`SilentOperator`] that holds for `hold`, and hand back the engine
    /// and the task it ran, for the caller to read state off.
    async fn dispatch_holding_for(hold: Duration) -> (Engine, StepId) {
        let engine = Engine::new(EngineCfg {
            worker_token_ttl_secs: TTL_SECS,
            ..EngineCfg::default()
        });
        let op_token = engine
            .attach(
                "op-parked-fallback",
                Role::Operator,
                Duration::from_secs(600),
            )
            .await
            .expect("attach");
        let task_id = engine
            .start_task(
                &op_token,
                TaskSpec {
                    agent: "held-agent".to_string(),
                    initial_directive: Value::String("go".to_string()),
                    step_ctx: None,
                    check_policy: None,
                },
            )
            .await
            .expect("start_task");
        let spawner: Arc<dyn SpawnerAdapter> = Arc::new(OperatorSpawner::new(
            Arc::new(SilentOperator { hold }),
            None,
            None,
        ));
        engine
            .dispatch_attempt_with(&op_token, &task_id, &spawner, None)
            .await
            .expect("dispatch_attempt_with");
        (engine, task_id)
    }

    /// How many stored capability records bind `task_id` — one after an
    /// ordinary dispatch, two once the fallback has re-minted (the reissue
    /// is added and the original deliberately left in place; see
    /// `Engine::remint_worker_token`). This is the mechanism the test
    /// below asserts on, as opposed to the outcome alone.
    async fn records_bound_to(engine: &Engine, task_id: &StepId) -> usize {
        let wanted = task_id.clone();
        engine
            .with_state("test.count_bound_records", move |s| {
                s.tokens
                    .values()
                    .filter(|r| r.task_id.as_ref() == Some(&wanted))
                    .count()
            })
            .await
            .expect("read token records")
    }

    fn has_final(tail: &[OutputEvent]) -> bool {
        tail.iter()
            .any(|ev| matches!(ev, OutputEvent::Final { .. }))
    }

    /// The failure this fix is for. A hold past the TTL leaves the spawn
    /// token expired by the time the fallback writes, `submit_output`'s
    /// `verify_token_for_task` rejects an expired Worker token, and the
    /// attempt's only `Final` would be lost to the clock rather than to
    /// anything about the work — `dispatch_attempt_with` then folds the
    /// empty tail into "no Final in output_tail". Re-minting at the moment
    /// of the write is what keeps it.
    #[tokio::test]
    async fn a_hold_past_the_ttl_still_lands_the_fallback_final() {
        let (engine, task_id) =
            dispatch_holding_for(Duration::from_millis(TTL_SECS * 1000 + 500)).await;

        let tail = engine.output_tail(&task_id, 1).await;
        assert!(
            has_final(&tail),
            "the fallback Final must survive a hold longer than the worker-token TTL, \
             got tail: {tail:?}"
        );
        assert_eq!(
            records_bound_to(&engine, &task_id).await,
            2,
            "the surviving Final must be the re-minted capability's doing — one record \
             for the spawn token, one for the reissue"
        );
    }

    /// Control, and the reason the re-mint is conditional: a dispatch that
    /// returns inside the TTL writes its `Final` with the token it already
    /// holds and mints nothing extra. Without this, "always re-mint" would
    /// pass the test above while adding a spare token record to every
    /// operator step in the process.
    #[tokio::test]
    async fn a_dispatch_inside_the_ttl_lands_its_final_without_re_minting() {
        let (engine, task_id) = dispatch_holding_for(Duration::from_millis(10)).await;

        let tail = engine.output_tail(&task_id, 1).await;
        assert!(
            has_final(&tail),
            "an un-parked operator's fallback Final must land unchanged, got tail: {tail:?}"
        );
        assert_eq!(
            records_bound_to(&engine, &task_id).await,
            1,
            "a live token needs no reissue — only the spawn token should be on record"
        );
    }
}
