//! The second stage of the two-stage pipeline: `SpawnerAdapter`.
//!
//! From the engine's viewpoint there is only one trait,
//! `SpawnerAdapter`; its `spawn` returns `Box<dyn Worker>` (see
//! `crate::worker::Worker`). Worker shape is an implementation detail of
//! each spawner; the engine only touches Workers through three
//! operations — `id()` / `cancel_token()` / `join()`.
//!
//! The old `WorkerAdapter` trait and `InProcWorker` struct — which
//! assumed a three-stage `Spawner.spawn → WorkerAdapter → invoke`
//! pipeline — were removed on this turn. Nothing instantiated or
//! dispatched them (dead code), and the multi-invocation path from
//! was collapsed in the implementation anyway.
//! The interface is now consolidated into the new `trait Worker` in
//! `src/worker.rs`.

use crate::core::ctx::Ctx;
use crate::core::engine::Engine;
use crate::types::{CapToken, StepId};
use crate::worker::Worker;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;

/// Errors that can occur while `SpawnerAdapter::spawn` is setting up a
/// worker, before the worker itself starts running.
#[derive(Debug, Error)]
pub enum SpawnError {
    /// No `WorkerFn` is registered for the requested agent name.
    #[error("worker not registered: {0}")]
    NotRegistered(String),
    /// A middleware layer vetoed the spawn (e.g. capability check, rate
    /// limit, policy gate).
    #[error("spawn rejected by middleware: {0}")]
    RejectedByMiddleware(String),
    /// Any other setup failure (e.g. `fetch_prompt` failed).
    #[error("internal: {0}")]
    Internal(String),
}

/// Errors surfaced once a worker is running, via `Worker::join`.
#[derive(Debug, Error)]
pub enum WorkerError {
    /// The worker fn itself returned an error.
    #[error("worker fn returned error: {0}")]
    Failed(String),
    /// The worker was cancelled through its `CancellationToken`.
    #[error("cancelled")]
    Cancelled,
}

/// The value a `WorkerFn` hands back on success, folded into an
/// `OutputEvent::Final` by the spawner.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerResult {
    /// The worker fn's output payload.
    pub value: Value,
    /// Whether the agent itself considers this a successful result
    /// (distinct from `Result::Err` — a worker fn can return `Ok(..)`
    /// with `ok: false` to signal an agent-level failure).
    pub ok: bool,
}

/// First stage of the two-stage pipeline: builds a `Box<dyn Worker>` for
/// one attempt. Every concrete spawner (`InProcSpawner`, `ProcessSpawner`,
/// the Operator spawner) implements this; the engine only ever holds a
/// `Arc<dyn SpawnerAdapter>` and knows nothing about the Worker shape
/// behind it.
#[async_trait]
pub trait SpawnerAdapter: Send + Sync {
    /// Spawn one attempt as a worker. Returns `Box<dyn Worker>`.
    ///
    /// The `directive` argument was removed in design intent: prompts are
    /// pulled on demand through
    /// `engine.fetch_prompt(token, task_id, attempt)`. Spawners are free
    /// to use whatever protocol they like internally — push, pull, or a
    /// hybrid. `ProcessSpawner` runs `fetch_prompt` and pushes the
    /// result into the child's stdin; `InProcSpawner` injects a prep
    /// snapshot as `WorkerInvocation.prompt`; a child process could
    /// even re-pull with the token itself.
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: StepId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError>;
}

// ─── InProcSpawner ────────────────────────────────────────────────────────

/// Invocation context handed to a Worker fn. Bundles `token` +
/// `task_id` + `prompt` + `sink`.
///
/// The `prompt` field was added in design intent, folding the old
/// `Fn(inv, directive)` `directive` argument into the invocation. The
/// spawner is expected to call
/// `engine.fetch_prompt(token, task_id, attempt)` in its prep step and
/// inject the snapshot into the invocation (push form). The `WorkerFn`
/// side may still re-pull if it needs to — for example to fetch the
/// prompt for a different attempt.
///
/// The `sink` field was added in design intent as the formal contract for
/// the spawner's intake surface. A worker fn can stream intermediate
/// events with things like
/// `inv.sink.emit(OutputEvent::Progress { .. })`. Child-process
/// spawners (`ProcessSpawner`, etc.) do not use `sink` — the child
/// speaks the stdout protocol; `InProcSpawner` injects one. Even
/// without `sink`, the `WorkerResult` returned by the fn is still
/// folded into a `Final` event on the spawner side, running alongside
/// the older return-value path.
#[derive(Clone)]
pub struct WorkerInvocation {
    /// Capability token authorizing this attempt.
    pub token: CapToken,
    /// The task this invocation belongs to.
    pub task_id: StepId,
    /// Attempt number within the task (used to key output events).
    pub attempt: u32,
    /// Registered agent name the `WorkerFn` was looked up under.
    pub agent: String,
    /// The prompt/prep snapshot pulled via `engine.fetch_prompt`,
    /// injected here (push form) so the worker fn does not need to call
    /// back into the engine for the common case.
    pub prompt: String,
    /// Intake: sink the worker fn uses to emit intermediate
    /// `OutputEvent`s. Injected by `InProcSpawner`. `None` means the
    /// sink path is not wired for this invocation.
    pub sink: Option<std::sync::Arc<dyn crate::worker::output::OutputSink>>,
    /// Upstream task cancel token — the clone of `cancel_inner`
    /// generated by `InProcSpawner` for `JoinHandleWorker`. Worker fns
    /// bridge this to their child futures or their SDK's
    /// `shutdown_token`, propagating external cancellation all the way
    /// down. `None` — like `sink` above — means the caller path is not
    /// carrying the cancel channel.
    pub cancel_token: Option<tokio_util::sync::CancellationToken>,
}

impl std::fmt::Debug for WorkerInvocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerInvocation")
            .field("token", &self.token)
            .field("task_id", &self.task_id)
            .field("attempt", &self.attempt)
            .field("agent", &self.agent)
            .field("prompt", &self.prompt)
            .field("sink", &self.sink.as_ref().map(|_| "<OutputSink>"))
            .field(
                "cancel_token",
                &self.cancel_token.as_ref().map(|_| "<CancellationToken>"),
            )
            .finish()
    }
}

/// A registered agent implementation: takes a `WorkerInvocation` and
/// resolves to a `WorkerResult` (or a `WorkerError`). Boxed as a
/// type-erased `Future` so heterogeneous agent implementations (async
/// fns, closures capturing state, etc.) can share one registry entry
/// type.
pub type WorkerFn = Arc<
    dyn Fn(
            WorkerInvocation,
        ) -> Pin<Box<dyn Future<Output = Result<WorkerResult, WorkerError>> + Send>>
        + Send
        + Sync,
>;

/// `agent`-string → `WorkerFn` registry. The generic parameter `W` pins
/// the per-kind Worker concrete type at the type level, so AgentBlock /
/// Lua / RustFn each produce their own Worker type through
/// `InProcSpawner<W>` and the type binding is preserved right up until
/// `SpawnerAdapter::spawn()` erases the return as `Box<dyn Worker>`.
/// `W` must be constructible from `WorkerJoinHandler` via `From` — i.e.
/// a newtype that embeds the async-signal handle.
pub struct InProcSpawner<W = crate::worker::MiddlewareWorker> {
    /// Agent name → implementation lookup table.
    pub registry: HashMap<String, WorkerFn>,
    _phantom: std::marker::PhantomData<W>,
}

// Inherent impl for the default W = MiddlewareWorker (so `InProcSpawner::new()`
// in existing tests picks this default).
impl InProcSpawner {
    /// Creates an empty registry, defaulting the Worker type to
    /// `MiddlewareWorker` (used by existing call sites and tests).
    pub fn new() -> Self {
        Self {
            registry: HashMap::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Registers a `WorkerFn`-shaped async closure under `agent`,
    /// overwriting any previous registration for the same name. Returns
    /// `&mut Self` for chained registration calls.
    pub fn register<F, Fut>(&mut self, agent: impl Into<String>, f: F) -> &mut Self
    where
        F: Fn(WorkerInvocation) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<WorkerResult, WorkerError>> + Send + 'static,
    {
        let f = Arc::new(f);
        let wrapped: WorkerFn = Arc::new(move |inv| {
            let f = f.clone();
            Box::pin(f(inv))
        });
        self.registry.insert(agent.into(), wrapped);
        self
    }
}

// Generic typed impl (the factory.build path that constructs a per-kind Worker).
impl<W> InProcSpawner<W>
where
    W: Worker + From<crate::worker::WorkerJoinHandler> + Send + Sync + 'static,
{
    /// Creates an empty registry pinned to Worker type `W` (the
    /// `factory.build` path uses this to get a per-kind Worker out of
    /// `spawn()` instead of the default `MiddlewareWorker`).
    pub fn typed() -> Self {
        Self {
            registry: HashMap::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl Default for InProcSpawner {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<W: Worker + From<crate::worker::WorkerJoinHandler> + Send + Sync + 'static> SpawnerAdapter
    for InProcSpawner<W>
{
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: StepId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        let f = self
            .registry
            .get(&ctx.agent)
            .cloned()
            .ok_or_else(|| SpawnError::NotRegistered(ctx.agent.clone()))?;

        // design intent: prompts are pulled via engine.fetch_prompt (the directive argument is retired)
        let prompt = engine
            .fetch_prompt(&token, &task_id)
            .await
            .map_err(|e| SpawnError::Internal(format!("fetch_prompt: {e}")))?;

        let (tx, rx) = tokio::sync::oneshot::channel();
        let cancel = tokio_util::sync::CancellationToken::new();
        let cancel_inner = cancel.clone();
        let worker_id = crate::types::WorkerId::new();
        // design intent: hand `engine` / `token` to the spawn task so it can emit
        // OutputEvent::Final via submit_output (side-by-side with the
        // WorkerResult oneshot path).
        let engine_for_emit = engine.clone();
        let token_for_emit = token.clone();
        let task_id_for_emit = task_id.clone();
        // Wire the receiving end by injecting an EngineSink into WorkerInvocation.sink.
        let sink = std::sync::Arc::new(crate::worker::output::EngineSink::new(
            engine.clone(),
            token.clone(),
            task_id.clone(),
            attempt,
        )) as std::sync::Arc<dyn crate::worker::output::OutputSink>;
        let inv = WorkerInvocation {
            token,
            task_id,
            attempt,
            agent: ctx.agent.clone(),
            prompt,
            sink: Some(sink),
            cancel_token: Some(cancel_inner.clone()),
        };

        tokio::spawn(async move {
            let result = tokio::select! {
                r = f(inv) => r,
                _ = cancel_inner.cancelled() => Err(WorkerError::Cancelled),
            };
            // Fold WorkerResult into OutputEvent::Final. Contract: one Final per attempt.
            if let Ok(wr) = &result {
                let ev = crate::worker::output::OutputEvent::Final {
                    content: crate::worker::output::ContentRef::Inline {
                        value: wr.value.clone(),
                    },
                    ok: wr.ok,
                };
                let _ = engine_for_emit
                    .submit_output(&token_for_emit, &task_id_for_emit, attempt, ev)
                    .await;
            }
            let signal: Result<(), WorkerError> = result.map(|_| ());
            let _ = tx.send(signal);
        });

        let handler = crate::worker::WorkerJoinHandler {
            worker_id,
            cancel,
            completion: rx,
        };
        Ok(Box::new(W::from(handler)))
    }
}
