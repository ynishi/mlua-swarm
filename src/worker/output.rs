//! Output path.
//!
//! The worker → `SpawnerAdapter` → engine output path. All structuring is
//! completed inside the `SpawnerAdapter`; by the time an event reaches the
//! engine it is a Rust-typed `OutputEvent`. The wire form the worker uses
//! (stdout / NDJSON / file path / IPC) is opaque to the engine.
//!
//! The `WorkerResult` type was folded into `OutputEvent::Final`.
//!
//! # Canonical type locations
//!
//! `OutputEvent` and `ContentRef` are canonical in [`crate::store::output`];
//! this module is narrowed to re-exports plus the engine-specific
//! `OutputSink` / `EngineSink`.

use crate::core::errors::EngineError;
use async_trait::async_trait;

pub use crate::store::output::{ContentRef, OutputEvent};

/// Sink used inside a worker function to emit events. The `InProcSpawner`
/// injects one into `WorkerInvocation`. The `ProcessSpawner` / child-process
/// pull path folds stdout / IPC into `OutputEvent` internally and calls
/// `engine.submit_output` directly, so it does not go through `OutputSink`
/// (it lands in the same engine state, but not via this trait).
#[async_trait]
pub trait OutputSink: Send + Sync {
    /// Emits one `OutputEvent` (progress, final, etc.) into the engine's
    /// output stream for this attempt.
    async fn emit(&self, event: OutputEvent) -> Result<(), EngineError>;
}

/// Concrete `OutputSink` — the default implementation that closes over
/// `engine`, `token`, `task_id`, and `attempt`, and calls
/// `engine.submit_output` for every `emit`. Injected by the `InProcSpawner`
/// into `WorkerInvocation`.
#[derive(Clone)]
pub struct EngineSink {
    engine: crate::core::engine::Engine,
    token: crate::types::CapToken,
    task_id: crate::types::StepId,
    attempt: u32,
}

impl EngineSink {
    /// Binds a sink to one attempt's identity so every `emit` call knows
    /// where to route the event without the caller repeating the
    /// coordinates each time.
    pub fn new(
        engine: crate::core::engine::Engine,
        token: crate::types::CapToken,
        task_id: crate::types::StepId,
        attempt: u32,
    ) -> Self {
        Self {
            engine,
            token,
            task_id,
            attempt,
        }
    }
}

#[async_trait]
impl OutputSink for EngineSink {
    async fn emit(&self, event: OutputEvent) -> Result<(), EngineError> {
        self.engine
            .submit_output(&self.token, &self.task_id, self.attempt, event)
            .await
    }
}
