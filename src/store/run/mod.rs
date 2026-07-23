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

/// The provenance of a Run snapshot's `bound_agents` array.
///
/// Persisted as the [`BOUND_AGENTS_ORIGIN_KEY`] sibling of `bound_agents`
/// inside the opaque [`RunRecord::input_json`] blob. This is Run-store
/// metadata, **not** a schema-crate Blueprint wire type: it never enters
/// [`crate::blueprint::BoundAgent`], `BoundAgentDigestInput`, or any digest
/// computation. It lives here beside [`RunContext`] — rather than in
/// `crate::service::task_launch` — because both the domain launch service
/// (which writes it) and the server crate's bindings-explain handler (which
/// reads it) consume it, and both already depend on this module; parking it
/// in the service module would force the server crate to reach into a
/// service-private type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotOrigin {
    /// `bound_agents` were resolved and pinned at the Run's initial launch —
    /// the binding identity is the launch-time pin.
    Launch,
    /// `bound_agents` were backfilled from the current Blueprint when a
    /// pre-binding-snapshot Run was resumed or reran. The binding identity
    /// carries no launch-time pin guarantee.
    ResumeBackfill,
}

/// JSON key the [`SnapshotOrigin`] is persisted under, beside `bound_agents`,
/// in [`RunRecord::input_json`].
pub const BOUND_AGENTS_ORIGIN_KEY: &str = "bound_agents_origin";

impl SnapshotOrigin {
    /// Read the origin marker from a decoded launch snapshot. An absent (or
    /// unparseable) [`BOUND_AGENTS_ORIGIN_KEY`] maps to
    /// [`SnapshotOrigin::ResumeBackfill`] — the safe side: a snapshot whose
    /// `bound_agents` were persisted before this marker existed cannot prove
    /// they were pinned at launch, so it must not be reported as a launch pin
    /// and (on the replay axis) must not have binding digests mixed into its
    /// replay keys. Only test artifacts hit this case in practice — the
    /// strict-binding series is unreleased, so no real snapshot predates the
    /// marker.
    pub fn from_snapshot(snapshot: &serde_json::Value) -> Self {
        snapshot
            .get(BOUND_AGENTS_ORIGIN_KEY)
            .and_then(|v| serde_json::from_value::<SnapshotOrigin>(v.clone()).ok())
            .unwrap_or(SnapshotOrigin::ResumeBackfill)
    }
}

/// GH #76 error surface: single-slot breadcrumb the dispatcher writes when a step
/// aborts the flow (currently: [`crate::core::state::DispatchOutcome::Blocked`]),
/// so the surrounding [`crate::service::task_launch::TaskLaunchService::launch`]
/// `map_err` closure can lift `failed_step` + `verdict_value` off the eval
/// boundary into the structured [`crate::service::task_launch::TaskLaunchError::FlowEval`]
/// variant. Sibling to `step_entries` (append-only per-step trace) — this
/// slot is last-write-wins because only ONE aborting step matters for the
/// eval's terminal error envelope, and flow-ir stops dispatching further
/// steps after `EvalError::DispatcherError`.
#[derive(Debug, Clone)]
pub struct LastFailure {
    /// The `StepId` (dispatch-time tid) the dispatcher assigned to the
    /// aborting step.
    pub step_id: StepId,
    /// The Blueprint `Step.ref` that dispatched the aborting step, if
    /// known (dispatcher fills this from its own `ref_` param — never `None`
    /// on the current write path, but modeled `Option` because the
    /// `LastFailure` shape is a public read surface and future breadcrumb
    /// writers may not have a ref in hand).
    pub step_ref: Option<String>,
    /// The verdict value the aborting step carried
    /// (e.g. `DispatchOutcome::Blocked(v)`'s `v`, cloned by the dispatcher
    /// before mapping the outcome to `EvalError::DispatcherError`).
    pub verdict_value: serde_json::Value,
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
    /// Whether this dispatch is a resume / rerun-from of an existing Run
    /// rather than an initial launch. `false` (the default) marks an initial
    /// launch. Set to `true` ONLY by the server's resume and rerun-from
    /// handlers — it is the sole, explicit signal that decides a backfilled
    /// snapshot's [`SnapshotOrigin`] (never inferred from replay-cursor or
    /// step-entry state, whose wiring is free to change).
    pub resume: bool,
    /// GH #76 error surface: shared single-slot breadcrumb the dispatcher writes when
    /// a step aborts the flow (`DispatchOutcome::Blocked` → `EvalError`).
    /// Read by the enclosing [`crate::service::task_launch::TaskLaunchService::launch`]
    /// `map_err` closure to populate the structured
    /// [`crate::service::task_launch::TaskLaunchError::FlowEval`] variant's
    /// `failed_step` / `verdict_value` fields. `None` (the default) means
    /// no aborting step was recorded — either the run succeeded, or an
    /// error path fired that does not go through the dispatcher's Blocked
    /// arm (e.g. `EvalError` raised by flow-ir itself before dispatch).
    /// Behind `std::sync::Mutex` to match the `replay_cursor` sibling
    /// (same crate-level convention — dispatcher writes are short critical
    /// sections, no `.await` held across).
    pub last_failure: Arc<Mutex<Option<LastFailure>>>,
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
            resume: false,
            last_failure: Arc::new(Mutex::new(None)),
        }
    }

    /// GH #76 error surface: write the aborting-step breadcrumb (last-write-wins).
    /// Called by [`crate::blueprint::EngineDispatcher::dispatch`]'s Blocked
    /// arm BEFORE it maps the outcome to `EvalError::DispatcherError`.
    /// Silently succeeds if the mutex is poisoned — this is an
    /// observability breadcrumb, not a load-bearing invariant, and a
    /// poisoned mutex here must never prevent the primary abort error
    /// from propagating (same fail-open convention as the sibling
    /// `append_step_entry` warn-and-swallow at
    /// `EngineDispatcher::dispatch`).
    pub fn set_last_failure(&self, failure: LastFailure) {
        if let Ok(mut slot) = self.last_failure.lock() {
            *slot = Some(failure);
        }
    }

    /// GH #76 error surface: reconstruct a partial-ctx snapshot from the step-entry
    /// trace persisted so far — the in-tree substitute for a full
    /// `storage.snapshot()` from flow-ir (upstream carry).
    ///
    /// Shape: `{ "steps": { "<step_id>": { "step_ref": ..., "status": ...,
    /// "binding_digest": ..., "at": ... } } }` — a JSON object keyed by
    /// each dispatched `StepId` with its recorded [`StepEntry`] metadata.
    /// This is metadata-level, NOT value-level (no `StepEntry` carries the
    /// step's actual OUTPUT value; that requires upstream mlua-flow-ir
    /// support to expose `storage.snapshot()` on error). Consumers who
    /// need value-level partial ctx must wait for the upstream carry —
    /// see the FlowEval `partial_ctx` field rustdoc.
    ///
    /// Returns `Value::Null` if the store lookup fails (e.g. the row was
    /// deleted between dispatch and error surfacing) — the caller's
    /// `partial_ctx: Option<Value>` field wraps this so `Null` is
    /// distinguishable from "no snapshot attempt at all".
    pub async fn snapshot_partial_ctx(&self) -> serde_json::Value {
        let record = match self.run_store.get(&self.run_id).await {
            Ok(r) => r,
            Err(_) => return serde_json::Value::Null,
        };
        let mut steps = serde_json::Map::new();
        for entry in &record.step_entries {
            let mut fields = serde_json::Map::new();
            if let Some(ref_) = &entry.step_ref {
                fields.insert(
                    "step_ref".to_string(),
                    serde_json::Value::String(ref_.clone()),
                );
            }
            if let Some(status) = &entry.status {
                fields.insert(
                    "status".to_string(),
                    serde_json::Value::String(status.clone()),
                );
            }
            if let Some(digest) = &entry.binding_digest {
                fields.insert(
                    "binding_digest".to_string(),
                    serde_json::Value::String(digest.to_string()),
                );
            }
            fields.insert("at".to_string(), serde_json::Value::Number(entry.at.into()));
            steps.insert(entry.step_id.to_string(), serde_json::Value::Object(fields));
        }
        let mut out = serde_json::Map::new();
        out.insert("steps".to_string(), serde_json::Value::Object(steps));
        serde_json::Value::Object(out)
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

    /// Builder-style setter: mark this dispatch as a resume / rerun-from of
    /// an existing Run (see [`Self::resume`]). Called only by the server's
    /// resume and rerun-from handlers; every other construction site leaves
    /// the default `false` (initial launch).
    pub fn with_resume(mut self) -> Self {
        self.resume = true;
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
            .field("resume", &self.resume)
            .field(
                "last_failure",
                &self.last_failure.lock().ok().and_then(|slot| slot.clone()),
            )
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
