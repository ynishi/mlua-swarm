//! The `Worker` trait — the shared interface for the execution units
//! each spawner keeps internally.
//!
//! ## Roles — the boundary between the Engine view and the Spawner's
//! internal view
//!
//! - **Engine view.** Only `SpawnerAdapter` is visible. The engine does
//!   not know about Workers, and it does not care about their shape.
//! - **Spawner's internal view.** Each `SpawnerAdapter::spawn` builds a
//!   concrete Worker internally (`ChildProcessWorker` / `ClosureWorker`
//!   / `OperatorWorker` and friends), type-erases it as
//!   `Box<dyn Worker>`, and returns that. This trait fixes the interface
//!   so the Worker shape does not drift between spawner implementations.
//!
//! ## Worker lifetime semantics
//!
//! The current contract is "one spawn = one worker = one join". At
//! `spawn()` time the worker has already spun up an internal tokio task
//! (it is already running); the caller just needs to `join()` and wait
//! for the completion signal. The value comes from
//! `engine.output_tail(task_id, attempt)` via `OutputEvent::Final`
//! — the oneshot channel carries the signal, not the
//! value.
//!
//! Extending to "one worker = N invocations" (calling the same worker
//! multiple times while the token's TTL is alive) is a carry on a
//! separate axis. That was the original design intent
//! v6.md:174, but the shape was collapsed for the sake of implementation
//! simplicity. The route when it is needed: add
//! `async fn invoke(&mut self, token, prompt) -> WorkerResult` to the
//! trait and redefine `join` as "the last invocation's completion plus
//! cleanup".
//!
//! ## `WorkerJoinHandler` — the canonical shape shared by the three
//! spawners today
//!
//! All three current spawners (Shell / InProc / Operator) call
//! `tokio::spawn` internally and push their completion signal through a
//! oneshot channel. `WorkerJoinHandler` is the helper that wraps that
//! shape into a `dyn Worker`. We share the helper until spawner
//! implementations need to define their own Worker structs; further
//! specialisation is a future carry.

pub mod adapter;
pub mod agent_block;
pub mod baseline;
pub mod output;
pub mod process_spawner;

use crate::types::WorkerId;
use crate::worker::adapter::WorkerError;
use async_trait::async_trait;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// Shared interface for the execution units spawners launch internally.
///
/// Every spawner implementation returns a concrete Worker struct that
/// implements this trait (today that is `WorkerJoinHandler`) as a
/// `Box<dyn Worker>`. The caller (the engine) only interacts with
/// workers through three operations: `id()` / `cancel_token()` /
/// `join()`.
#[async_trait]
pub trait Worker: Send {
    /// This worker's identity — used for logging and to tie cancellation
    /// back to the right worker.
    fn id(&self) -> &WorkerId;

    /// Token that carries the cancel signal. Clonable — this is the
    /// path the engine uses to cancel from the outside.
    fn cancel_token(&self) -> CancellationToken;

    /// Await the completion signal. The worker is consumed — one
    /// worker, one join. `Ok(())` means the worker ran to completion;
    /// `Err` means it was cancelled, failed, or panicked internally.
    /// Values do not come back through this trait; use
    /// `engine.output_tail` for those.
    async fn join(self: Box<Self>) -> Result<(), WorkerError>;
}

/// **Handler for a Worker's async completion signal.** A building
/// block; it does not implement `Worker` itself. Holds the
/// `(worker_id, cancel token, oneshot receiver)` triple and is embedded
/// by every per-kind Worker (`AgentBlockWorker` / `LuaWorker` /
/// `RustFnWorker` / `ProcessWorker` / `OperatorWorker`).
///
/// "The Worker that actually does the work" and "the mechanism that
/// waits for its async completion" are two different concepts. This
/// struct is dedicated to the latter; the former is expressed by
/// per-kind Worker structs — one type per `AgentKind`, each hiding its
/// kind-specific state (SDK quirks, VM state, child-process handles,
/// etc.) inside itself.
pub struct WorkerJoinHandler {
    /// Identity of the worker this handler belongs to.
    pub worker_id: WorkerId,
    /// Cancellation token shared with the running task; cloned out via
    /// `Worker::cancel_token`.
    pub cancel: CancellationToken,
    /// Receiver side of the oneshot channel the spawned task completes
    /// through. Consumed by `await_completion`.
    pub completion: oneshot::Receiver<Result<(), WorkerError>>,
}

impl WorkerJoinHandler {
    /// Shared helper that receives the `join` async signal. This is the
    /// canonical path called from every per-kind Worker's `Worker::join`
    /// implementation.
    pub async fn await_completion(self) -> Result<(), WorkerError> {
        match self.completion.await {
            Ok(r) => r,
            Err(_) => Err(WorkerError::Failed(
                "worker completion channel closed".into(),
            )),
        }
    }
}

/// Generic Worker used only on the middleware (`wrap_join`) wrap path,
/// so kind-agnostic post-processing wrap results can be returned as
/// `Box<dyn Worker>`. Unlike a per-kind Worker, this does not represent
/// "a specific kind's execution" — it is a thin wrapper that layers a
/// post-processor on top of an existing Worker.
///
/// Named after its role: the "Worker for the middleware path" — the
/// type boxed as the return value by `wrap_join` consumers (Audit /
/// MainAI / Senior / LongHold / Lua after-hook, and so on).
pub struct MiddlewareWorker {
    /// The wrapped completion handle; `join` delegates to this.
    pub handler: WorkerJoinHandler,
}

impl From<WorkerJoinHandler> for MiddlewareWorker {
    fn from(handler: WorkerJoinHandler) -> Self {
        Self { handler }
    }
}

#[async_trait]
impl Worker for MiddlewareWorker {
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

/// Helper that wraps the inner Worker's `join()` completion signal in a
/// post-processor and returns a fresh `Box<dyn Worker>`. All the
/// middleware wrap paths (Audit / MainAI / Senior / LongHold / Lua
/// after-hook, and so on) go through this helper for consistency.
///
/// The cancel token is inherited from the inner Worker verbatim, so
/// cancelling from outside the engine still reaches the inner Worker.
/// `worker_id` is also carried over from the inner Worker.
pub fn wrap_join<F, Fut>(inner: Box<dyn Worker>, post: F) -> Box<dyn Worker>
where
    F: FnOnce(Result<(), WorkerError>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), WorkerError>> + Send,
{
    let worker_id = inner.id().clone();
    let cancel = inner.cancel_token();
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        let r = inner.join().await;
        let result = post(r).await;
        let _ = tx.send(result);
    });
    Box::new(MiddlewareWorker {
        handler: WorkerJoinHandler {
            worker_id,
            cancel,
            completion: rx,
        },
    })
}
