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
use std::collections::{BTreeMap, HashMap};
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
    /// A cancel request landed on the Run (via `POST /v1/runs/:id/cancel`
    /// / `mse_cancel` / `swarm_cancel`). Terminal — the current wiring
    /// records the intent + trace event; live in-flight abort of the
    /// still-dispatching flow remains a v3 carry, so a Run that reaches
    /// its Ok outcome after this marker keeps its terminal `result_ref`,
    /// but the Cancelled marker itself is observable via
    /// `swarm_status.cancel_requested` and `core.cancel_requested` on
    /// the trace stream.
    Cancelled,
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
///
/// The per-step stats extension (started/completed timestamps, duration,
/// token usage, model, worker kind, variant-specific `adapter_data`) is
/// additive: every field is `Option` + `#[serde(default)]` so rows
/// written before the extension deserialize unchanged, and a dispatch
/// where no boundary reported stats still appends a valid entry. The
/// entry stays **write-once** — in-flight visibility belongs to the
/// sibling [`crate::store::trace::TraceEvent`] stream, never to
/// in-place updates here.
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
    /// The attempt number the stats below describe (the LAST attempt the
    /// dispatch ran), when a worker boundary reported one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
    /// Unix epoch milliseconds — when the dispatcher began this step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<i64>,
    /// Unix epoch milliseconds — when the step reached its outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<i64>,
    /// Wall-clock dispatch duration in milliseconds (dispatcher-measured,
    /// worker-kind independent).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Worker kind label (`"agent_block"` / `"subprocess"` / `"operator"`
    /// / …) as reported by the worker boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_kind: Option<String>,
    /// The model that served the attempt, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Normalized token usage, when a worker boundary reported one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<crate::store::trace::TokenUsage>,
    /// Number of LLM turns the attempt ran, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub num_turns: Option<u32>,
    /// Worker-kind-specific raw payload (size-capped, engine-opaque).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adapter_data: Option<serde_json::Value>,
}

impl StepEntry {
    /// Construct an entry with only the pre-stats fields set — the shape
    /// every pre-extension writer produced. Stats fields default to
    /// `None`; the dispatcher's fold fills them when available.
    pub fn basic(
        step_id: StepId,
        step_ref: Option<String>,
        status: Option<String>,
        binding_digest: Option<BindingDigest>,
        at: u64,
    ) -> Self {
        Self {
            step_id,
            step_ref,
            status,
            binding_digest,
            at,
            attempt: None,
            started_at_ms: None,
            completed_at_ms: None,
            duration_ms: None,
            worker_kind: None,
            model: None,
            usage: None,
            num_turns: None,
            adapter_data: None,
        }
    }

    /// Fold a boundary-reported [`crate::store::trace::WorkerStats`]
    /// into this entry (the dispatcher's outcome-time fold). `None`
    /// fields in `stats` leave the entry untouched; `adapter_data` is
    /// size-capped via [`crate::store::trace::cap_payload`].
    pub fn with_worker_stats(mut self, stats: crate::store::trace::WorkerStats) -> Self {
        self.worker_kind = stats.worker_kind.or(self.worker_kind);
        self.model = stats.model.or(self.model);
        self.usage = stats.usage.or(self.usage);
        self.num_turns = stats.num_turns.or(self.num_turns);
        self.adapter_data = stats
            .adapter_data
            .map(crate::store::trace::cap_payload)
            .or(self.adapter_data);
        self
    }
}

/// Who currently holds one **slot** of a Run — the model's `Assignee`
/// (`{ op, desc, gen }`), persisted as one value of the
/// [`RunRecord::current`] map.
///
/// A "slot" is a Blueprint-declared Operator seat: the `operator_ref` an
/// agent names (`Blueprint.operators[].name`). A Blueprint may declare
/// several, so a Run has as many slots as its Blueprint declares, each
/// with its own holder over time.
///
/// Invariants this type carries (model §4.3):
///
/// - **A1** `|Run.current| ≤ 1` **per slot** — expressed as the map keyed
///   by slot on [`RunRecord::current`]: one key cannot hold two values, so
///   a seat cannot have two holders. The slot is the map key, never a
///   field of this struct.
/// - **A3** [`Self::gen`] is immutable for the lifetime of an instance.
///   Re-assignment never mutates an existing `Assignee`; the store mints a
///   fresh instance with the next generation (**Q3**). Nothing in this
///   crate takes `&mut Assignee`.
/// - **A9** [`Self::desc`] is mandatory. The store rejects an empty (or
///   whitespace-only) `desc` with
///   [`RunStoreError::AssigneeDescRequired`]; the HTTP layer maps that to
///   `400`. The store itself never decides a status code.
/// - **A10** this is the one place a slot's current holder is recorded and
///   the one place it is read from — the destination is not baked into any
///   sibling field.
///
/// The `Assignee` does not cross the SAP boundary (model §4.7 T1): the
/// primitives below the boundary carry an `operator`, never an assignee or
/// a generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Assignee {
    /// Who holds the slot — the model's `OperatorId`, which is the key
    /// space of the engine's operator registry. Session ids (`S-<hex>`)
    /// and role aliases (`main-ai`) share that one key space (the WS login
    /// path registers both), so this stays a plain `String` rather than
    /// narrowing to a session id.
    pub op: String,
    /// Why this holder was assigned — the human-readable record of the
    /// assignment. Required (**A9**); an empty value is rejected at the
    /// store boundary rather than stored as `""`.
    pub desc: String,
    /// The generation stamped on this holder at acquire time (**A4**:
    /// `G` after the increment). Immutable for the lifetime of the
    /// instance (**A3**) — a later acquire produces a NEW `Assignee` with
    /// a higher `gen` instead of rewriting this one.
    ///
    /// `G` is a single counter per **Run**, not per slot: an assignment to
    /// any slot advances the one counter, so two holders of different
    /// slots can be ordered against each other by `gen` alone.
    pub gen: u64,
}

/// What [`RunStore::vacate_assignee`] did — it releases a seat only while
/// that seat still holds the generation the caller observed, so "released"
/// and "someone else got there first" are two answers, not one.
///
/// # Why a release has to name a generation
///
/// A release is issued by a caller that *read* the holder earlier and then
/// decided it should go: **A7** reads a holder, asks its adapter for
/// `T-ALIVE`, and releases on `Disconnected`; **O8**'s cascade reads a
/// holder, matches it against a deleted operator's names, and releases.
/// Both decisions are about the `Assignee` that was read, and both have
/// `.await` points between the read and the write, during which an
/// `acquire` (which never excludes — **A8**) can seat somebody else.
///
/// Addressed at `(run, slot)` alone, such a release would delete whatever
/// holder happened to be there — a holder whose liveness was never asked
/// for and whose deletion no premise in the model supports. That is a lost
/// update, not **A8**: the acquirer was answered `200` with its generation
/// (**Q4**) and has no channel to learn it was undone. Carrying the
/// observed generation turns the write back into the decision that was
/// actually made.
///
/// `gen` alone identifies the holder because `G` is Run-wide and advances
/// on every assignment event, so a generation is never reused — a seat
/// holding `expected_gen` is holding the very instance the caller read,
/// including when a re-acquire put the *same* operator back (**A8**).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VacateOutcome {
    /// The seat still held the observed generation, so it was released and
    /// the slot is now `Vacant`.
    Released {
        /// The Run-wide counter `G` after this event (**A4**: a `Vacant`
        /// advances it exactly like an `Assign` does).
        generation: u64,
        /// The holder that was released — the instance the caller read.
        released: Assignee,
    },
    /// The seat did not hold the observed generation, so **nothing was
    /// written**: no holder removed, `G` not advanced, `updated_at`
    /// untouched.
    ///
    /// The caller's reading is stale — either an `acquire` moved the seat
    /// on (**A8** already decided that contest, and the newer holder
    /// stands) or the seat was released by someone else in between. Either
    /// way the release is not re-issued against the new state: the
    /// decision behind it was made about a holder that is no longer there.
    Stale {
        /// Who holds the seat now, for the message the caller reports.
        /// `None` = the slot is already `Vacant`.
        current: Option<Assignee>,
    },
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
    ///
    /// This is the **launch-time snapshot** of who was pinned when the Run
    /// was kicked — not the live holder. The live holder is
    /// [`Self::current`]; nothing resolves a dispatch destination from this
    /// field, so the two are not competing destinations (**A10**).
    pub operator_sid: Option<String>,
    /// The Run's live holders, keyed by **slot** — the model's
    /// `Run.current` (§4.3).
    ///
    /// A slot is a Blueprint-declared Operator seat (`operator_ref` =
    /// `Blueprint.operators[].name`): a Blueprint may declare several, and
    /// each agent picks the one it dispatches through. The cardinality is
    /// therefore `Run 1 : N Operator` and `Operator 1 : 1 Assignee` (at a
    /// time), hence `Run 1 : N Assignee` — with **A1** reading "at most one
    /// holder **per slot**", which is exactly what a map expresses. An
    /// absent key is that slot's `Vacant`; an empty map is a Run with no
    /// slot held at all.
    ///
    /// **R2** — a `Vacant` slot does not stop the Run; only a dispatch that
    /// needs *that* slot's holder is affected, and dispatches through other
    /// slots are untouched. **R6** — this travels with the Run row, so a
    /// restart does not drop the assignments.
    ///
    /// Only [`RunStore::acquire_assignee`] / [`RunStore::vacate_assignee`]
    /// write it, both scoped to one slot, and both mint a fresh
    /// [`Assignee`] rather than mutating a stored one (**Q3**).
    ///
    /// [`BTreeMap`](std::collections::BTreeMap) rather than `HashMap`: the
    /// map is serialized into a persisted column and into observation
    /// payloads, and key-sorted output keeps those bytes stable across
    /// processes. Additive with `#[serde(default)]` so rows serialized
    /// before the assignment axis existed decode unchanged (as an empty
    /// map = every slot Vacant).
    ///
    /// # An empty map is written out, not skipped
    ///
    /// This field used to carry `skip_serializing_if =
    /// "BTreeMap::is_empty"`, so a Run holding nothing had no `current`
    /// key on the wire at all. That made "nobody holds anything on this
    /// Run" and "this response does not report holders" the same bytes,
    /// and §4.3 asks for the opposite (*居なければ居ないと分かる* — when
    /// nobody is there, it must be possible to tell that nobody is there).
    /// `"current": {}` says it. The per-seat form of the same answer, which
    /// also names the seats nobody holds, is
    /// [`crate::handover::run_assignees`].
    #[serde(default)]
    pub current: BTreeMap<String, Assignee>,
    /// The Run's generation counter — the model's `G` (**A4**).
    ///
    /// `0` at launch. Every assignment event (`Assign` **or** `Vacant`)
    /// increments it by one **before** stamping, so the first `Assign`
    /// yields `gen == 1`; the counter therefore holds the generation of
    /// the most recent event, and the next event will use this value `+ 1`.
    /// The bump is unconditional — re-acquiring for the SAME `op` still
    /// increments, because the counter counts events, not state changes.
    ///
    /// **One counter per Run, shared by every slot.** An `Assign` to slot
    /// `b` advances the same `G` that a preceding `Assign` to slot `a`
    /// advanced, so any two holders — of the same slot or of different
    /// ones — can be ordered by `gen`. Per-slot counters would buy nothing
    /// and would make that comparison meaningless.
    ///
    /// **A2** (`current = Assigned(a) ⟹ a.gen ≤ G`) holds on two legs, not
    /// one. On the write path it holds by construction: every `current`
    /// value's `gen` is stamped from this counter at the moment it is
    /// bumped, so an acquire can never leave a holder above it. On the way
    /// **in** it is checked — [`RunStore::create`] takes a caller-supplied
    /// record with both fields public, so a record that arrives already
    /// violating A2 is refused with
    /// [`RunStoreError::AssigneeGenerationAhead`] rather than stored (see
    /// [`RunRecord::validate_assignment_generations`]). Left unchecked, that
    /// record would stay violated: the next acquire stamps generation 1,
    /// below the seeded incumbent, and ordering two holders by `gen` — the
    /// whole reason `G` is Run-wide — would silently invert.
    ///
    /// Additive with `#[serde(default)]` (pre-existing rows read back `0`).
    #[serde(default)]
    pub next_generation: u64,
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

impl RunRecord {
    /// **A2** as a check: every holder in [`Self::current`] must have been
    /// stamped at or below [`Self::next_generation`]
    /// (`current = Assigned(a) ⟹ a.gen ≤ G`).
    ///
    /// [`RunStore::create`] calls this on the record it is handed, and every
    /// [`RunStore`] implementation is expected to — an out-of-tree backend
    /// that skips it accepts records the two in-tree backends refuse.
    /// Nothing else needs it: `acquire_assignee` stamps `gen` from the
    /// counter it has just bumped, so no write path in this crate can
    /// produce a record this rejects.
    ///
    /// Reports the first offending seat in [`Self::current`]'s key order,
    /// which is stable (`BTreeMap`), so the same bad record always names the
    /// same seat.
    pub fn validate_assignment_generations(&self) -> Result<(), RunStoreError> {
        for (slot, assignee) in &self.current {
            if assignee.gen > self.next_generation {
                return Err(RunStoreError::AssigneeGenerationAhead {
                    slot: slot.clone(),
                    gen: assignee.gen,
                    next_generation: self.next_generation,
                });
            }
        }
        Ok(())
    }
}

/// Filter/paging parameters for [`RunStore::list`] — the `GET /v1/runs`
/// collection query. All filters AND together; results are newest-first
/// (`created_at` descending, ties broken by insertion order where the
/// backend tracks one).
#[derive(Debug, Clone, Default)]
pub struct RunListFilter {
    /// Only Runs kicked from this Task.
    pub task_id: Option<TaskId>,
    /// Only Runs currently in this status.
    pub status: Option<RunStatus>,
    /// Page size cap. `None` = no cap.
    pub limit: Option<usize>,
    /// Skip the first N matching rows (after ordering).
    pub offset: Option<usize>,
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

    /// **A9**: [`RunStore::acquire_assignee`] was called without a `desc`.
    /// The record is mandatory, so the acquire is refused rather than
    /// stored with an empty one. The store deliberately does not name an
    /// HTTP status — the caller maps this to `400`.
    #[error("assignee desc is required")]
    AssigneeDescRequired,

    /// **A2**: a record handed to [`RunStore::create`] carries a holder
    /// whose generation is above the Run's counter `G`
    /// (`current[slot].gen > next_generation`), so it would be stored
    /// already violating `current = Assigned(a) ⟹ a.gen ≤ G`.
    ///
    /// The acquire path cannot produce this — it stamps `gen` from the
    /// counter it just bumped — but `create` accepts a caller-built
    /// [`RunRecord`] with both fields public, and that is a published
    /// surface. Refused rather than stored: the violation is permanent
    /// (the next acquire stamps a *lower* generation than the incumbent's,
    /// inverting the ordering `G` being Run-wide exists to provide) and
    /// invisible afterwards. Callers map this to `400`, same as
    /// [`Self::AssigneeDescRequired`].
    #[error(
        "assignee generation is ahead of the run's counter: current['{slot}'].gen = {gen} > \
         next_generation = {next_generation}"
    )]
    AssigneeGenerationAhead {
        /// The seat whose holder is ahead of the counter.
        slot: String,
        /// That holder's generation.
        gen: u64,
        /// The Run counter `G` it was measured against.
        next_generation: u64,
    },

    /// An assignment event named no slot. `Run.current` is keyed by slot,
    /// so an `Assign` (or a `Vacant`) with an empty slot names no seat to
    /// write — it is refused rather than collapsed onto a `""` key that
    /// no `operator_ref` can ever resolve to. Callers map this to `400`,
    /// same as [`Self::AssigneeDescRequired`].
    #[error("assignee slot is required")]
    AssigneeSlotRequired,

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
    /// Optional [`crate::store::trace::TraceHandle`] bound to this Run —
    /// the write port for the per-Run [`crate::store::trace::TraceEvent`]
    /// stream. When present the dispatcher appends `core.*` events
    /// around every step and registers the handle with the engine
    /// (`Engine::trace_handle`) so middlewares/workers can append their
    /// own kinds. `None` (the default) disables the trace rail entirely
    /// — pre-trace callers keep their behavior byte-for-byte.
    pub trace: Option<crate::store::trace::TraceHandle>,
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
            trace: None,
        }
    }

    /// Builder-style setter: attach a
    /// [`crate::store::trace::TraceHandle`] so the dispatcher appends
    /// `core.*` trace events around every step and exposes the handle
    /// to middlewares/workers via the engine.
    pub fn with_trace(mut self, trace: crate::store::trace::TraceHandle) -> Self {
        self.trace = Some(trace);
        self
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
            .field("trace", &self.trace.is_some())
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
    ///
    /// This is the one door into the store that carries a caller-built
    /// [`RunRecord`], so it is where the assignment axis is checked rather
    /// than assumed: a record whose `current` holds a generation above
    /// `next_generation` is refused with
    /// [`RunStoreError::AssigneeGenerationAhead`] (**A2**, see
    /// [`RunRecord::validate_assignment_generations`]). Implementations must
    /// run that check before persisting anything. The rest of the record —
    /// `step_entries`, `status`, timestamps — is still trusted as given.
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

    /// Assign this Run's `slot` to `op` — the model's `Assign` event
    /// (§4.3).
    ///
    /// `slot` is the Blueprint-declared Operator seat (`operator_ref`) the
    /// assignment applies to; only that key of
    /// [`RunRecord::current`] is touched, so assigning one seat never
    /// disturbs another's holder.
    ///
    /// Bumps the Run's generation counter `G` by one and stamps the new
    /// value onto a **freshly minted** [`Assignee`], which replaces
    /// `current[slot]` (**A4** / **Q3**: the previously stored `Assignee`
    /// is returned untouched, never rewritten in place). `G` is Run-wide,
    /// so this advances the same counter every other slot's events advance.
    /// `updated_at` is bumped to now.
    ///
    /// **A8**: this succeeds regardless of who holds the slot — a live
    /// holder is displaced (last writer wins); there is no exclusion and
    /// no rejection path for a contended slot. The only refusals are
    /// **A9** (an empty or whitespace-only `desc` returns
    /// [`RunStoreError::AssigneeDescRequired`]) and an empty `slot`
    /// (returns [`RunStoreError::AssigneeSlotRequired`]); an unknown `id`
    /// returns [`RunStoreError::NotFound`].
    ///
    /// The read of `G` and the write of both columns happen atomically, so
    /// two concurrent acquires can never read the same `G` and hand out a
    /// duplicate generation — including when they name different slots.
    ///
    /// Returns `(new generation, the holder this call displaced from this
    /// slot)` — the caller needs both to tell whether it took over from
    /// someone and under which generation it now dispatches.
    async fn acquire_assignee(
        &self,
        id: &RunId,
        slot: &str,
        op: &str,
        desc: &str,
    ) -> Result<(u64, Option<Assignee>), RunStoreError>;

    /// Release the holder of this Run's `slot` — the model's `Vacant`
    /// event (§4.3) — **but only while that seat still holds the
    /// generation the caller observed**. Other slots keep their holders.
    ///
    /// `expected_gen` is the `gen` of the [`Assignee`] the caller read and
    /// decided about. The comparison and the write happen in one critical
    /// section (the same transaction / lock `acquire_assignee` uses), so a
    /// concurrent `acquire` either lands before the check — and the
    /// release becomes a no-op — or after the write, which is an ordinary
    /// **A8** takeover of an already-Vacant seat. There is no window in
    /// which a stale reader deletes a newer holder. See [`VacateOutcome`]
    /// for why the generation has to travel with the call at all; this is
    /// the only release verb, because both production callers (**A7** at
    /// `AssigneeRouter::execute` and **O8**'s cascade) are stale readers,
    /// and an unconditional sibling would exist only to be picked by
    /// mistake.
    ///
    /// **A4**: a release that actually happens bumps the Run-wide
    /// generation counter exactly like `Assign` does; it just mints no
    /// [`Assignee`]. A subsequent [`Self::acquire_assignee`] — on this slot
    /// or any other — therefore continues from the bumped value rather than
    /// reusing the generation the released holder had. `updated_at` is
    /// bumped to now. A [`VacateOutcome::Stale`] result is **not** an
    /// assignment event and writes nothing at all: the counter counts
    /// events, and a release that did not release is not one.
    ///
    /// An already-Vacant slot therefore answers
    /// `Stale { current: None }` rather than burning a generation — no
    /// generation can match an absent holder. An empty `slot` returns
    /// [`RunStoreError::AssigneeSlotRequired`] and an unknown `id` returns
    /// [`RunStoreError::NotFound`].
    async fn vacate_assignee(
        &self,
        id: &RunId,
        slot: &str,
        expected_gen: u64,
    ) -> Result<VacateOutcome, RunStoreError>;

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

    /// List Runs matching `filter`, newest-first (`created_at`
    /// descending) — the `GET /v1/runs` collection read.
    async fn list(&self, filter: &RunListFilter) -> Result<Vec<RunRecord>, RunStoreError>;

    /// Delete a Run row (the `DELETE /v1/runs/:id` retention operation).
    /// The caller is responsible for pruning the sibling trace stream
    /// ([`crate::store::trace::RunTraceStore::delete_run`]) — the two
    /// stores are deliberately uncoupled at the trait level.
    async fn delete(&self, id: &RunId) -> Result<(), RunStoreError>;
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
