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
use crate::types::{CapToken, TaskId, WorkerId};
use crate::worker::adapter::{SpawnError, SpawnerAdapter, WorkerError, WorkerResult};
use crate::worker::output::{ContentRef, OutputEvent};
use crate::worker::{Worker, WorkerJoinHandler};
use async_trait::async_trait;
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
/// - `worker_token`: a capability token (`Role::Worker`, 600s TTL,
///   `scopes = ["*"]`). Thin-path operators (a `a WebSocket-backed operator session`,
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
        prompt: String,
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
/// The paired **Blueprint-global (session) axis** is
/// `crate::middleware::OperatorDelegateMiddleware` — a single operator
/// backend registered on the session and applied uniformly across every
/// agent. When both are effective, the delegate middleware sits at the
/// outer end of the stack and bypasses `inner.spawn`; this type is inert
/// and no double fire can occur. See the `OperatorSpawnerFactory` doc
/// for the exclusivity narrative.
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
        task_id: TaskId,
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

        tokio::spawn(async move {
            let result: Result<WorkerResult, WorkerError> = tokio::select! {
                r = op.execute(&ctx_clone, system, prompt, worker_binding, token_for_op) => r,
                _ = cancel_inner.cancelled() => Err(WorkerError::Cancelled),
            };
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
                    let _ = engine_clone
                        .submit_output(&token_clone, &task_id_clone, attempt, ev)
                        .await;
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
