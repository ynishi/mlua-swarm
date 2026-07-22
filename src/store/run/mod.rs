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

use crate::blueprint::BindingDigest;
use crate::store::replay::{ReplayCursor, ReplayStore};
use crate::types::{RunId, StepId, TaskId};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
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
    /// The Run was still `Running` when the server process restarted
    /// (issue #35 ST2 boot-time recovery sweep). Terminal — in-flight
    /// `EngineState` is process-local and unrecoverable; this variant
    /// records the fact without attempting to reconstruct or resume it.
    Interrupted,
}

/// One worker-reported degradation entry — a worker fell back to a
/// substitute behavior instead of failing outright (e.g. a tool call errored
/// and the worker used a cached/default value). Independent channel from
/// [`StepEntry`]/`result_ref`: degradations never flow through step OUTPUT
/// or the fold path (GH #32; sibling of the GH #34 audit sidecar — both
/// keep observational signal off the BP-chain value). Reported via `POST
/// /v1/worker/degradation`; the server injects `step_ref`/`attempt`/`at`
/// before persisting.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct DegradationEntry {
    /// The tool (or capability) the worker attempted to use.
    pub tool: String,
    /// The error that triggered the fallback, in the worker's own words.
    pub error: String,
    /// What the worker substituted instead of failing.
    pub fallback: String,
    /// Optional free-form context from the worker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// The Blueprint step ref (`Step.ref`) this degradation was reported
    /// under, if known. Server-injected metadata, not worker-supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_ref: Option<String>,
    /// The attempt number this degradation was reported under, if known.
    /// Server-injected metadata, not worker-supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
    /// Unix epoch seconds — when this entry was recorded. Server-injected.
    pub at: u64,
}

/// One entry in a Run's step trace — appended as the engine dispatches
/// (and finishes) each step. Purely observational: no field here is
/// consulted for flow control.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StepEntry {
    /// The step this entry traces.
    #[schemars(with = "String")]
    pub step_id: StepId,
    /// The Blueprint step ref (`Step.ref`) that was dispatched, if known.
    pub step_ref: Option<String>,
    /// Free-form status label for this step at the time the entry was
    /// recorded (e.g. `"dispatched"`, `"passed"`, `"blocked"`).
    pub status: Option<String>,
    /// Immutable Runner/Agent/Context snapshot digest used for this step.
    /// `None` for rows created before BoundAgent launch wiring.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding_digest: Option<BindingDigest>,
    /// Unix epoch seconds — when this entry was recorded.
    pub at: u64,
}

/// One persisted `Run` row — one kick of a [`crate::store::task::TaskRecord`].
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RunRecord {
    /// Run identifier.
    #[schemars(with = "String")]
    pub id: RunId,
    /// The Task this Run was kicked from.
    #[schemars(with = "String")]
    pub task_id: TaskId,
    /// Current lifecycle status.
    pub status: RunStatus,
    /// Trace of dispatched steps, in append order.
    pub step_entries: Vec<StepEntry>,
    /// Worker-reported degradations, in append order (GH #32). Independent
    /// channel from [`Self::step_entries`]/[`Self::result_ref`] — see
    /// [`DegradationEntry`]'s doc for the invariant. `[]` (the default) =
    /// no degradations reported — every pre-#32 Run is unaffected.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub degradations: Vec<DegradationEntry>,
    /// Operator session id bound to this Run, if any (WS operator
    /// correlation).
    pub operator_sid: Option<String>,
    /// The Run's terminal result payload, set once by
    /// [`RunStore::set_result`]. `None` while the Run is in flight.
    #[schemars(with = "Option<serde_json::Value>")]
    pub result_ref: Option<serde_json::Value>,
    /// Opaque JSON snapshot of the launch input this Run was kicked with
    /// (blueprint / init_ctx / operator injection / ttl / …). The server
    /// serializes its own launch-input struct into this string at Run
    /// creation time so an `Interrupted` Run can be resumed under the SAME
    /// `run_id` without re-deriving the input from a since-stale request
    /// body. The store treats it as an opaque blob — the schema is owned by
    /// the caller (the server crate). `None` = no snapshot recorded (older
    /// rows predating resume support, or a caller that never opts in); such
    /// a Run cannot be resumed. Additive with `#[serde(default)]` so
    /// pre-existing serialized rows deserialize unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_json: Option<String>,
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

/// Pairs a [`RunId`] with the [`RunStore`] used to persist its trace.
///
/// Threaded from the server entry points (`POST /v1/tasks`, `POST
/// /v1/tasks/:id/runs`) down through `TaskApplication::handle_with_run` /
/// `TaskLaunchService::launch` / `EngineDispatcher` (issue #13 run_id
/// propagation) so every step the dispatcher runs can be appended to
/// `RunRecord.step_entries` and the run's id exposed to workers via
/// `Ctx.meta.runtime["run_id"]`. Kept as a distinct type — rather than a
/// new field on `TaskApplicationInput` — so the pre-existing exhaustive
/// struct literal in `mlua-swarm-cli`'s MCP adapter (`TaskApplicationInput
/// { .. }`, no `run_ctx`) keeps compiling unchanged: callers that don't
/// care about run tracing keep calling `TaskApplication::handle` /
/// `TaskLaunchService::launch`, which pass `None` through internally.
#[derive(Clone)]
pub struct RunContext {
    /// The Run this dispatch's steps should be traced into.
    pub run_id: RunId,
    /// Where to append [`StepEntry`] rows as steps are dispatched.
    pub run_store: Arc<dyn RunStore>,
    /// Optional [`ReplayStore`] the engine will append a Ctx-snapshot +
    /// step-output row to after every completed step (see
    /// [`crate::store::replay`] for the primitive). `None` (the default)
    /// disables logging entirely — pre-replay callers keep their behavior
    /// byte-for-byte.
    pub replay_store: Option<Arc<dyn ReplayStore>>,
    /// Optional [`ReplayCursor`] the engine consults BEFORE dispatching
    /// each step. When present and the cursor has a matching row for
    /// `(step_ref, input_hash, occurrence)`, the engine returns the
    /// stored `DispatchOutcome::Pass(value)` verbatim and skips the
    /// Adapter spawn — this is the replay-hit path. `None` (the default)
    /// disables replay entirely.
    pub replay_cursor: Option<Arc<Mutex<ReplayCursor>>>,
    /// Run-pinned replay identity component, keyed by logical agent name.
    pub binding_digests: Arc<HashMap<String, BindingDigest>>,
}

impl RunContext {
    /// Construct a `RunContext` with just the RunStore wired — the same
    /// shape all pre-replay callers use (`replay_store` / `replay_cursor`
    /// both `None`). Preserved as a convenience so a caller that never
    /// opts into replay can keep constructing `RunContext` positionally.
    pub fn new(run_id: RunId, run_store: Arc<dyn RunStore>) -> Self {
        Self {
            run_id,
            run_store,
            replay_store: None,
            replay_cursor: None,
            binding_digests: Arc::new(HashMap::new()),
        }
    }

    /// Builder-style setter: attach a [`ReplayStore`] to log every
    /// completed step's Ctx snapshot + output into.
    pub fn with_replay_store(mut self, store: Arc<dyn ReplayStore>) -> Self {
        self.replay_store = Some(store);
        self
    }

    /// Builder-style setter: attach a [`ReplayCursor`] the dispatcher
    /// consults for a hit before dispatching each step.
    pub fn with_replay_cursor(mut self, cursor: Arc<Mutex<ReplayCursor>>) -> Self {
        self.replay_cursor = Some(cursor);
        self
    }

    /// Attach immutable binding digests so replay keys distinguish the same
    /// step/input executed under different Runner/Agent/Context snapshots.
    pub fn with_binding_digests(mut self, digests: HashMap<String, BindingDigest>) -> Self {
        self.binding_digests = Arc::new(digests);
        self
    }
}

impl std::fmt::Debug for RunContext {
    // `dyn RunStore` carries no `Debug` bound (backend implementations
    // shouldn't be forced to derive it just to satisfy this struct's
    // `Debug`); render `run_store` as its `name()` instead, same idiom as
    // `WorkerInvocation`'s manual `Debug` for its `Arc<dyn OutputSink>`
    // field.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunContext")
            .field("run_id", &self.run_id)
            .field("run_store", &self.run_store.name())
            .field(
                "replay_store",
                &self.replay_store.as_ref().map(|s| s.name()),
            )
            .field("replay_cursor", &self.replay_cursor.is_some())
            .field("binding_digests", &self.binding_digests.len())
            .finish()
    }
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

    /// Append one worker-reported degradation to a Run's `degradations`
    /// (GH #32), bumping `updated_at` to now. Independent of
    /// [`Self::append_step_entry`] — degradations never flow through step
    /// OUTPUT/fold.
    async fn append_degradation(
        &self,
        id: &RunId,
        entry: DegradationEntry,
    ) -> Result<(), RunStoreError>;

    /// Update a Run's status, bumping `updated_at` to now.
    async fn update_status(&self, id: &RunId, status: RunStatus) -> Result<(), RunStoreError>;

    /// Atomically transition a Run's status from `from` to `to`, bumping
    /// `updated_at` to now — the compare-and-set primitive the resume path
    /// (`POST /v1/runs/:id/resume`) uses to guard against a double resume
    /// racing the same `Interrupted` Run into `Running` twice.
    ///
    /// Returns `Ok(true)` when a row with this `id` AND current status
    /// `from` was found and flipped to `to`; `Ok(false)` when the row's
    /// current status was not `from` (a concurrent transition already won,
    /// or the Run is absent). Never a hard error for the status-mismatch /
    /// absent case — the boolean is the caller's race signal.
    async fn try_transition(
        &self,
        id: &RunId,
        from: RunStatus,
        to: RunStatus,
    ) -> Result<bool, RunStoreError>;

    /// Set a Run's terminal `result_ref`, bumping `updated_at` to now.
    async fn set_result(
        &self,
        id: &RunId,
        result_ref: serde_json::Value,
    ) -> Result<(), RunStoreError>;

    /// Replace the opaque launch snapshot after pre-dispatch binding has
    /// enriched it (for example with immutable `bound_agents`).
    async fn set_input_json(&self, id: &RunId, input_json: String) -> Result<(), RunStoreError>;

    /// List every Run currently `Running` (issue #35 ST2 boot sweep +
    /// ST4 occupancy check reuse this). No ordering guarantee.
    async fn list_running(&self) -> Result<Vec<RunRecord>, RunStoreError>;
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
