//! `TaskStore` — persistence for `Task` records.
//!
//! Part of the issue #13 ID-hierarchy reconciliation: Blueprint -> Task ->
//! Run -> Step -> Attempt. A `Task` is the **work-item identity**: one row
//! per unit of work ("resolve issue #10" + a Blueprint ref snapshot + an
//! input ctx), created once when the work is submitted (e.g.
//! `POST /v1/tasks`). A single Task can be kicked N times; each kick mints
//! a [`RunId`](crate::types::RunId) and is tracked by the sibling
//! [`crate::store::run`] store — this module owns only the 1-row-per-Task
//! identity and its coarse lifecycle status.
//!
//! Current scope:
//!
//! - [`InMemoryTaskStore`] — process-volatile default.
//! - [`SqliteTaskStore`] — file-backed persistence via `rusqlite-isle`
//!   (thread-isolated `Connection`, single-writer FIFO discipline; same
//!   shape as [`crate::store::issue::sqlite::SqliteIssueStore`]).
//! - Other persistent backends (Git / mini-app / …) are future carries.

use crate::types::TaskId;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use thiserror::Error;

pub mod inmemory;
pub mod sqlite;
pub use inmemory::InMemoryTaskStore;
pub use sqlite::SqliteTaskStore;

// ──────────────────────────────────────────────────────────────────────────
// TaskRecordStatus / TaskRecord
// ──────────────────────────────────────────────────────────────────────────

/// Lifecycle status of a [`TaskRecord`].
///
/// Coarser than [`crate::store::run::RunStatus`]: a Task's status tracks
/// "is there work in flight / did the most recent kick finish", while a
/// Run's status tracks one specific kick's own outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskRecordStatus {
    /// Created, no [`crate::store::run::RunRecord`] started yet.
    Pending,
    /// A Run is currently in flight for this Task.
    Running,
    /// The Task's most recent Run completed successfully.
    Done,
    /// The Task's most recent Run failed.
    Failed,
}

/// One persisted `Task` row — the work-item identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    /// Task identifier.
    pub id: TaskId,
    /// Human-facing goal / description (e.g. "resolve issue #10").
    pub goal: String,
    /// Snapshot of the Blueprint selector supplied at creation (the
    /// `POST /v1/tasks` body's `BlueprintSelector`). Kept as a bare
    /// `serde_json::Value` so the store layer does not depend on the
    /// selector's Rust type — callers decode/encode at the API boundary.
    pub blueprint_ref: serde_json::Value,
    /// Input context supplied at task creation.
    pub input_ctx: serde_json::Value,
    /// Issue #19 ST4: Task-level canonical fields (`project_root` /
    /// `work_dir` / `task_metadata`) snapshot for rekick, stored as JSON
    /// (a serialized `TaskInputSpec`) — same "bare `Value`, no Rust-type
    /// dependency" rationale as [`Self::blueprint_ref`] /
    /// [`Self::input_ctx`]. `None` for every pre-#19 `TaskRecord`
    /// (backward compat) and for callers whose request carried no
    /// Task-level fields at all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_input_spec: Option<serde_json::Value>,
    /// Current lifecycle status.
    pub status: TaskRecordStatus,
    /// Unix epoch seconds — creation time.
    pub created_at: u64,
    /// Unix epoch seconds — last update time.
    pub updated_at: u64,
}

/// Errors surfaced by a [`TaskStore`] implementation.
#[derive(Debug, Error)]
pub enum TaskStoreError {
    /// No Task exists for the given id.
    #[error("task not found: {0}")]
    NotFound(TaskId),

    /// `create` was called with an id that is already stored.
    #[error("task already exists: {0}")]
    Duplicate(TaskId),

    /// Backend-specific failure not covered by the other variants.
    #[error("other: {0}")]
    Other(String),
}

// ──────────────────────────────────────────────────────────────────────────
// TaskStore trait
// ──────────────────────────────────────────────────────────────────────────

/// Persistence interface for `Task` records — the work-item identity
/// layer of the issue #13 ID hierarchy.
#[async_trait]
pub trait TaskStore: Send + Sync {
    /// Backend name — for diagnostics/logging.
    fn name(&self) -> &str;

    /// Create a new Task row. Returns `Duplicate` if `record.id` is
    /// already stored.
    async fn create(&self, record: TaskRecord) -> Result<(), TaskStoreError>;

    /// Fetch a Task by id.
    async fn get(&self, id: &TaskId) -> Result<TaskRecord, TaskStoreError>;

    /// List every Task, newest first (descending `created_at`).
    async fn list(&self) -> Result<Vec<TaskRecord>, TaskStoreError>;

    /// Update a Task's status, bumping `updated_at` to now.
    async fn update_status(
        &self,
        id: &TaskId,
        status: TaskRecordStatus,
    ) -> Result<(), TaskStoreError>;
}

// ──────────────────────────────────────────────────────────────────────────
// Shared inner state used by the InMemory backend.
// ──────────────────────────────────────────────────────────────────────────

#[derive(Default)]
pub(crate) struct Inner {
    /// Insertion order — used as a stable tie-break under `list()`.
    pub(crate) order: Vec<TaskId>,
    pub(crate) records: HashMap<TaskId, TaskRecord>,
}

pub(crate) type SharedInner = Mutex<Inner>;
