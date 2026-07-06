//! `RunStore` — persistence for `Run` records (one kick of a `Task`).
//!
//! Part of the issue #13 ID-hierarchy reconciliation: Blueprint -> Task ->
//! Run -> Step -> Attempt. A [`RunId`](crate::types::RunId) is minted
//! server-side each time a [`crate::store::task::TaskRecord`] is kicked; it
//! carries a lightweight trace of the steps dispatched during that kick
//! ([`StepEntry`]) for observability, plus its own outcome status
//! independent of the owning Task's coarser status. A single Task can have
//! N `Run`s over its lifetime (`list_by_task`).
//!
//! Current scope:
//!
//! - [`InMemoryRunStore`] — process-volatile default.
//! - [`SqliteRunStore`] — file-backed persistence via `rusqlite-isle`.
//!   `step_entries` is a JSON column, not normalized into its own table —
//!   this is a trace/observability artifact, not something queried
//!   relationally.
//! - Other persistent backends (Git / mini-app / …) are future carries.

use crate::types::{RunId, StepId, TaskId};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use thiserror::Error;

pub mod inmemory;
pub mod sqlite;
pub use inmemory::InMemoryRunStore;
pub use sqlite::SqliteRunStore;

// ──────────────────────────────────────────────────────────────────────────
// RunStatus / StepEntry / RunRecord
// ──────────────────────────────────────────────────────────────────────────

/// Lifecycle status of a [`RunRecord`] — the outcome of one specific kick
/// of a Task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Minted, not yet dispatched.
    Pending,
    /// Steps are currently being dispatched for this Run.
    Running,
    /// The Run completed successfully.
    Done,
    /// The Run failed.
    Failed,
}

/// One entry in a Run's step trace — appended as the engine dispatches
/// (and finishes) each step. Purely observational: no field here is
/// consulted for flow control.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepEntry {
    /// The step this entry traces.
    pub step_id: StepId,
    /// The Blueprint step ref (`Step.ref`) that was dispatched, if known.
    pub step_ref: Option<String>,
    /// Free-form status label for this step at the time the entry was
    /// recorded (e.g. `"dispatched"`, `"passed"`, `"blocked"`).
    pub status: Option<String>,
    /// Unix epoch seconds — when this entry was recorded.
    pub at: u64,
}

/// One persisted `Run` row — one kick of a [`crate::store::task::TaskRecord`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    /// Run identifier.
    pub id: RunId,
    /// The Task this Run was kicked from.
    pub task_id: TaskId,
    /// Current lifecycle status.
    pub status: RunStatus,
    /// Trace of dispatched steps, in append order.
    pub step_entries: Vec<StepEntry>,
    /// Operator session id bound to this Run, if any (WS operator
    /// correlation).
    pub operator_sid: Option<String>,
    /// The Run's terminal result payload, set once by
    /// [`RunStore::set_result`]. `None` while the Run is in flight.
    pub result_ref: Option<serde_json::Value>,
    /// Unix epoch seconds — creation time.
    pub created_at: u64,
    /// Unix epoch seconds — last update time.
    pub updated_at: u64,
}

/// Errors surfaced by a [`RunStore`] implementation.
#[derive(Debug, Error)]
pub enum RunStoreError {
    /// No Run exists for the given id.
    #[error("run not found: {0}")]
    NotFound(RunId),

    /// `create` was called with an id that is already stored.
    #[error("run already exists: {0}")]
    Duplicate(RunId),

    /// Backend-specific failure not covered by the other variants.
    #[error("other: {0}")]
    Other(String),
}

// ──────────────────────────────────────────────────────────────────────────
// RunStore trait
// ──────────────────────────────────────────────────────────────────────────

/// Persistence interface for `Run` records — one kick of a Task, in the
/// issue #13 ID hierarchy.
#[async_trait]
pub trait RunStore: Send + Sync {
    /// Backend name — for diagnostics/logging.
    fn name(&self) -> &str;

    /// Create a new Run row. Returns `Duplicate` if `record.id` is already
    /// stored.
    async fn create(&self, record: RunRecord) -> Result<(), RunStoreError>;

    /// Fetch a Run by id.
    async fn get(&self, id: &RunId) -> Result<RunRecord, RunStoreError>;

    /// List every Run kicked from `task_id`, ascending by `created_at`
    /// (oldest kick first).
    async fn list_by_task(&self, task_id: &TaskId) -> Result<Vec<RunRecord>, RunStoreError>;

    /// Append one step-trace entry to a Run's `step_entries`, bumping
    /// `updated_at` to now.
    async fn append_step_entry(&self, id: &RunId, entry: StepEntry) -> Result<(), RunStoreError>;

    /// Update a Run's status, bumping `updated_at` to now.
    async fn update_status(&self, id: &RunId, status: RunStatus) -> Result<(), RunStoreError>;

    /// Set a Run's terminal `result_ref`, bumping `updated_at` to now.
    async fn set_result(
        &self,
        id: &RunId,
        result_ref: serde_json::Value,
    ) -> Result<(), RunStoreError>;
}

// ──────────────────────────────────────────────────────────────────────────
// Shared inner state used by the InMemory backend.
// ──────────────────────────────────────────────────────────────────────────

#[derive(Default)]
pub(crate) struct Inner {
    /// Insertion order — used as a stable tie-break under `list_by_task()`.
    pub(crate) order: Vec<RunId>,
    pub(crate) records: HashMap<RunId, RunRecord>,
}

pub(crate) type SharedInner = Mutex<Inner>;
