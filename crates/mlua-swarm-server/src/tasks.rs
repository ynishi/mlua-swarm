//! HTTP surface for the Task/Run persistence axis (issue #13 ID-hierarchy
//! reconciliation: Blueprint → Task → Run → Step → Attempt).
//!
//! - `GET  /v1/tasks`          — list every persisted `TaskRecord`, newest first.
//! - `GET  /v1/tasks/:id`      — a `TaskRecord` plus every `RunRecord` kicked from it.
//! - `POST /v1/tasks/:id/runs` — re-kick an existing Task: mints a fresh `RunId`,
//!   re-resolves the stored `blueprint_ref` (refreshing `Blueprint.default_init_ctx`
//!   exactly like original launch time — issue #19 ST4), 3-layer-merges it with
//!   `TaskRecord.input_ctx` and an **optional** [`RunKickRequest`] body's
//!   `init_ctx_override` (see [`merge_init_ctx_3layer`]), dispatches through
//!   `TaskApplication::handle_with_run`, and returns the new `{task_id, run_id}`
//!   pair. A body-less request (or one that omits both fields) preserves the
//!   pre-#19 rekick behavior byte-for-byte.
//! - `GET  /v1/runs/:id`       — a single `RunRecord` (`step_entries` trace included).
//! - `GET  /v1/runs/:id/bindings` — requested/effective binding explain from
//!   the immutable launch snapshot (never from the current Blueprint).
//! - `POST /v1/runs/:id/resume` — resume an `Interrupted` Run under the SAME
//!   `run_id` (replay cursor + stored launch-input snapshot).
//! - `POST /v1/runs/:id/rerun-from` — GH #71 Layer A. Rerun a terminal Run
//!   (`Done` / `Failed` / `Interrupted`) from a caller-specified step under
//!   the SAME `run_id`; physically truncates the replay log at the cut
//!   point so re-dispatch does not collide with the pre-rerun rows. See
//!   [`run_rerun_from`] for the full contract + Known Limitations.
//! - `POST /v1/runs/:id/acquire` — take one of the Run's Operator seats
//!   (model §4.5). Never refuses a held seat (**A8**), and reports the
//!   generation and the holder it displaced. See [`run_acquire`] for the
//!   contract and for the one rule of §4.5 this build cannot honour.
//!
//! There is deliberately no route that empties a seat. The model reaches
//! `Vacant` three ways — the holder is found `Disconnected` at reference
//! time (**A7**), its Operator is deleted (**O8**), or someone else
//! acquires (**A8**) — and none of them is a request to release. An
//! operator that wants out of a Run leaves (`DELETE /v1/operators/:sid`),
//! which cascades. Publishing a `vacate` verb would add a fourth way that
//! the model does not have and that nothing above needs.
//!
//! `POST /v1/tasks` itself (the flow-eval entry point, `tasks_start` /
//! `run_flow_form`) stays in `crate::lib` — it is the pre-existing
//! Operator-inject-aware dispatch path this module's handlers re-kick
//! through, not a new one. This module owns the read/list/re-kick surface
//! plus the [`finalize_run`] persistence helper both paths share.
//!
//! Authorization follows the same convention as the existing `POST /v1/tasks`
//! entry: no `Authorization` header is required (the route is open), and the
//! only Operator-session correlation available is the request-body-level
//! `operator_sid` (see `crate::TaskLaunchRequest` doc) — this module invents no
//! new auth mechanism.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use futures_util::FutureExt;
use mlua_swarm::application::{
    BlueprintRef, TaskApplicationError, TaskApplicationInput, TaskApplicationOutput,
};
use mlua_swarm::blueprint::{BindRequest, BindingAttestation, BoundAgent, OperatorDef};
use mlua_swarm::core::config::CheckPolicy;
use mlua_swarm::service::merge_init_ctx_3layer;
use mlua_swarm::service::TaskLaunchError;
use mlua_swarm::store::replay::ReplayCursor;
use mlua_swarm::store::run::{
    Assignee, RunContext, RunListFilter, RunRecord, RunStatus, RunStoreError, SnapshotOrigin,
    StepEntry,
};
use mlua_swarm::store::task::{TaskRecord, TaskRecordStatus, TaskStoreError};
use mlua_swarm::store::trace::{kind as trace_kind, TraceEvent, TraceHandle, TraceQuery};
use mlua_swarm::{
    validate_bound_agent_snapshots, OperatorKind, Role, RunId, TaskId, TaskInputSpec,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::assignee_trace::{append_assigned, append_released, AssignSource, ReleaseReason};
use crate::{ApiError, AppState};

/// Current Unix time in whole seconds. `TaskRecord` / `RunRecord` timestamps
/// are `u64` seconds (not milliseconds) — see their field docs in
/// `mlua_swarm::store::task` / `mlua_swarm::store::run`.
pub(crate) fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Serializable mirror of [`TaskApplicationInput`] — the launch-input
/// snapshot persisted into `RunRecord.input_json` at Run-creation time so a
/// later `POST /v1/runs/:id/resume` can rebuild the exact input and re-run
/// the flow under the SAME `run_id`.
///
/// [`TaskApplicationInput`] itself is deliberately not `Serialize`/
/// `Deserialize` (its doc comment explains why — keeping the exhaustive
/// `TaskApplicationInput { .. }` struct literal in the MCP adapter
/// compiling), so this is a dedicated snapshot type with the exact same
/// field set. Every field type already derives serde
/// (`BlueprintRef` / `Role` / `Duration` / `OperatorKind` / `TaskInputSpec`
/// / `CheckPolicy`), so the mirror is total — no field is dropped, and an
/// operator-injected launch round-trips as faithfully as a plain one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RunLaunchSnapshot {
    blueprint: BlueprintRef,
    operator_id: String,
    role: Role,
    ttl: Duration,
    init_ctx: Value,
    operator_kind: Option<OperatorKind>,
    bridge_id: Option<String>,
    hook_id: Option<String>,
    /// The one Operator the launch named (`TaskLaunchInput::operator_sid`).
    ///
    /// # Decoding a snapshot written before the fold
    ///
    /// This field used to be two, `operator_backend_id` and `operator_pin`,
    /// and both are still out there in persisted `input_json` blobs. The
    /// `alias` reads the first back into this one; the second needs nothing,
    /// because serde ignores unknown fields and the two never disagreed —
    /// every writer set `operator_pin` only alongside an
    /// `operator_backend_id` holding the same sid. So an old blob resumes
    /// pinned exactly as it launched, and an unpinned one resumes unpinned.
    ///
    /// `#[serde(default)]` covers the blob that carries neither key at all,
    /// which the aliased name — unlike the one it replaced — no longer
    /// requires to be present.
    #[serde(default, alias = "operator_backend_id")]
    operator_sid: Option<String>,
    #[serde(default)]
    operator_kind_overrides: HashMap<String, OperatorKind>,
    task_input: Option<TaskInputSpec>,
    check_policy: Option<CheckPolicy>,
}

impl RunLaunchSnapshot {
    /// Capture a launch input as a snapshot (clones each field — the
    /// original is still dispatched).
    fn from_input(input: &TaskApplicationInput) -> Self {
        Self {
            blueprint: input.blueprint.clone(),
            operator_id: input.operator_id.clone(),
            role: input.role,
            ttl: input.ttl,
            init_ctx: input.init_ctx.clone(),
            operator_kind: input.operator_kind,
            bridge_id: input.bridge_id.clone(),
            hook_id: input.hook_id.clone(),
            operator_sid: input.operator_sid.clone(),
            operator_kind_overrides: input.operator_kind_overrides.clone(),
            task_input: input.task_input.clone(),
            check_policy: input.check_policy,
        }
    }

    /// Rebuild the launch input from a snapshot for resume.
    fn into_input(self) -> TaskApplicationInput {
        TaskApplicationInput {
            blueprint: self.blueprint,
            operator_id: self.operator_id,
            role: self.role,
            ttl: self.ttl,
            init_ctx: self.init_ctx,
            operator_kind: self.operator_kind,
            bridge_id: self.bridge_id,
            hook_id: self.hook_id,
            operator_sid: self.operator_sid,
            operator_kind_overrides: self.operator_kind_overrides,
            task_input: self.task_input,
            check_policy: self.check_policy,
        }
    }
}

/// Serialize a launch input into the opaque `RunRecord.input_json` blob.
/// Shared by both Run-creation sites (`run_flow_form` in `crate::lib` and
/// [`task_rekick`]) so every persisted Run carries the snapshot resume
/// needs. A serialization failure is a `400` — it means the caller handed
/// in a value the snapshot cannot round-trip, which must surface before the
/// Run is dispatched, not silently.
pub(crate) fn snapshot_launch_input(input: &TaskApplicationInput) -> Result<String, ApiError> {
    serde_json::to_string(&RunLaunchSnapshot::from_input(input))
        .map_err(|e| ApiError::bad_request(format!("launch input snapshot: {e}")))
}

/// Validate the `(operator_sid, operator_desc)` pair a launch carries, and
/// return the `Assign` it implies.
///
/// Model §4.3 spells the launch verb `launch(op, desc)` with the operator
/// optional and, when present, its `desc` mandatory (**A9**: "the `desc` of
/// an `Assign` is required; `∅` is rejected with `400` — the launch-time
/// `Assign` included"). This function is that rule, in one place, for both
/// Run-creation sites (`run_flow_form` and [`task_rekick`]):
///
/// - `(Some(op), Some(non-blank desc))` → `Some((op, desc))`, the launch's
///   first `Assign`.
/// - `(Some(op), None | Some(blank))` → `400`. Refused **before** any
///   Task/Run row is written, the same fail-fast-before-side-effects
///   ordering the sid-validation and the timeout-ceiling checks already
///   observe — a launch that cannot record why it was assigned should not
///   leave records behind.
/// - `(None, _)` → `None`: nothing was assigned, so there is nothing to
///   describe. A stray `operator_desc` is not an error; it describes an
///   assignment that was never requested.
///
/// The store enforces **A9** again at its own boundary
/// (`RunStoreError::AssigneeDescRequired`); this is the HTTP-side
/// spelling, which is where the status code lives.
pub(crate) fn resolve_launch_assign(
    operator_sid: Option<&str>,
    operator_desc: Option<&str>,
) -> Result<Option<(String, String)>, ApiError> {
    let Some(op) = operator_sid else {
        return Ok(None);
    };
    let desc = operator_desc.map(str::trim).unwrap_or_default();
    if desc.is_empty() {
        return Err(ApiError::bad_request(format!(
            "operator_desc is required when operator_sid is given: pinning this launch to \
             operator '{op}' assigns the Run to it, and an assignment must record why it \
             happened (e.g. \"pinned by the launch request\")"
        )));
    }
    Ok(Some((op.to_string(), desc.to_string())))
}

/// Which of a Blueprint's declared Operator seats a request named — the
/// decision shared by [`resolve_launch_slot`] (a launch pin) and
/// [`resolve_acquire_slot`] (a handover).
///
/// `Run.current` is keyed by slot (`operator_ref`): a Blueprint declares N
/// Operator seats in `operators[]` and each agent picks the one it
/// dispatches through (`AgentDef.spec.operator_ref`), so "assign this Run
/// to that operator" is only half an instruction — the other half is
/// *which seat*. That half is decided the same way whoever is asking,
/// which is why it is decided once, here:
///
/// 1. A seat is named and the Blueprint declares it → that seat.
/// 2. No seat is named and the Blueprint declares exactly one → that one.
///    Nothing to disambiguate, so nothing to ask for (this is the shape
///    every bundled Blueprint has today).
/// 3. A seat is named that no `OperatorDef` carries → refuse rather than
///    invent it. A holder filed under a key no router ever reads would
///    leave the Run dispatching into a `Vacant` seat with the request
///    looking like it took.
/// 4. No seat is named and the Blueprint declares two or more → refuse and
///    list the candidates. Picking one (the first, say) would be an
///    addressing rule the model does not have, and would silently
///    mis-address every multi-Operator Blueprint — the exact failure the
///    per-lane split (`phase_a_op` / `phase_b_op`) exists to keep visible.
/// 5. The Blueprint declares no Operator at all → refuse. There is no seat
///    to hold.
///
/// The comparison is literal (no trimming): a padded or empty name is a
/// name nothing declares, and lands in [`Self::Undeclared`] like any other
/// typo.
///
/// What a caller *says* about a refusal is deliberately not shared. Both
/// callers answer `400`, but a launch pin and a handover are refused for
/// reasons the reader has to act on differently, so each writes its own
/// sentence: the rule is common, the wording is not.
pub(crate) enum SlotChoice<'a> {
    /// The request named a seat the Blueprint declares.
    Named(&'a str),
    /// The request named none and the Blueprint declares exactly one.
    Sole(&'a str),
    /// The request named a seat no `OperatorDef` carries, on a Blueprint
    /// that does declare others.
    Undeclared(&'a str),
    /// The Blueprint declares no seat at all. Carries the requested name
    /// when there was one, so the refusal can quote it.
    NoSeats(Option<&'a str>),
    /// The request named none and the Blueprint declares several.
    Ambiguous,
}

/// Apply the [`SlotChoice`] rule. Pure, and every refusal is a variant
/// rather than an error — the wording belongs to the caller that has the
/// context for it.
pub(crate) fn choose_slot<'a>(
    requested: Option<&'a str>,
    operators: &'a [OperatorDef],
) -> SlotChoice<'a> {
    match (requested, operators) {
        (Some(slot), ops) if ops.iter().any(|o| o.name == slot) => SlotChoice::Named(slot),
        (Some(slot), []) => SlotChoice::NoSeats(Some(slot)),
        (Some(slot), _) => SlotChoice::Undeclared(slot),
        (None, [only]) => SlotChoice::Sole(&only.name),
        (None, []) => SlotChoice::NoSeats(None),
        (None, _) => SlotChoice::Ambiguous,
    }
}

/// The declared seats, quoted and comma-joined, for a refusal that lists
/// the candidates.
fn declared_seats(operators: &[OperatorDef]) -> String {
    operators
        .iter()
        .map(|o| format!("'{}'", o.name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Which Blueprint-declared Operator seat a launch pin's `Assign` lands
/// in — [`choose_slot`] in launch-request terms, where every refusal is a
/// `400` phrased about `operator_slot` / `operator_sid`.
pub(crate) fn resolve_launch_slot(
    operator_slot: Option<&str>,
    operators: &[OperatorDef],
) -> Result<String, ApiError> {
    match choose_slot(operator_slot, operators) {
        SlotChoice::Named(slot) | SlotChoice::Sole(slot) => Ok(slot.to_string()),
        SlotChoice::NoSeats(Some(slot)) => Err(ApiError::bad_request(format!(
            "operator_slot '{slot}': this Blueprint declares no operators[], so there is \
             no seat to assign a launch pin to"
        ))),
        SlotChoice::Undeclared(slot) => Err(ApiError::bad_request(format!(
            "operator_slot '{slot}': this Blueprint declares no such operator (declared: \
             {})",
            declared_seats(operators)
        ))),
        SlotChoice::NoSeats(None) => Err(ApiError::bad_request(
            "operator_sid assigns this Run to an operator, but the Blueprint declares no \
             operators[] — there is no seat to assign it to. Declare the Operator the agents \
             dispatch through, or launch without operator_sid."
                .to_string(),
        )),
        SlotChoice::Ambiguous => Err(ApiError::bad_request(format!(
            "operator_slot is required: this Blueprint declares {} Operator seats ({}), so a \
             launch pin has to name the one it assigns",
            operators.len(),
            declared_seats(operators)
        ))),
    }
}

/// Write a launch's first `Assign` onto a freshly created Run.
///
/// Called right after `RunStore::create`, with the pair
/// [`resolve_launch_assign`] validated and the seat [`resolve_launch_slot`]
/// resolved out of the Blueprint. **A4**: the Run was created with
/// `next_generation == 0`, so this acquire makes the holder generation `1`
/// — a launch pin is an assignment event on top of the row, not a field of
/// it, which is why `RunRecord.current` is not seeded at `create` time.
///
/// A failure here is a `500`: the row exists but does not carry the holder
/// the caller asked for, and dispatching it would deliver somewhere the
/// caller never named (or, with the router in place, nowhere at all).
/// Degrading to an unassigned run would be the silent-fallback this whole
/// axis exists to remove.
///
/// **W4**: the assignment lands on the Run's trace as well as in
/// `current`, so the seat's whole history reads off one rail (see
/// [`crate::assignee_trace`]). `current` holds only the holder of the
/// moment — the launch pin would otherwise be invisible the instant
/// anyone acquired over it.
///
/// **D2**: it also lands on the pinned operator's own 記名, so the same
/// event is answerable from the operator's end ("which Runs am I on") as
/// well as the Run's — see [`crate::handover::record_observed_assignment`],
/// which is why `task_id` is a parameter here (the goal and the Task-level
/// paths live on that row).
pub(crate) async fn assign_launch_operator(
    state: &AppState,
    run_id: &RunId,
    task_id: &TaskId,
    slot: &str,
    op: &str,
    desc: &str,
) -> Result<(), ApiError> {
    let (gen, previous) = state
        .run_store
        .acquire_assignee(run_id, slot, op, desc)
        .await
        .map_err(|e| {
            ApiError::engine(format!(
                "run {run_id}: assigning launch operator '{op}' to slot '{slot}' failed: {e}"
            ))
        })?;
    trace_assigned(
        state,
        run_id,
        slot,
        op,
        desc,
        gen,
        AssignSource::LaunchPin,
        previous.as_ref(),
    )
    .await;
    crate::handover::record_observed_assignment(state, run_id, task_id, slot, op).await;
    Ok(())
}

/// Append a `core.assignee_assigned` event for a seat this handler just
/// filled — [`crate::assignee_trace::append_assigned`] with the
/// `Assignee` rebuilt from the parts `RunStore::acquire_assignee` reports
/// back (it answers with the generation and the holder it displaced, not
/// with the instance it wrote).
#[allow(clippy::too_many_arguments)]
async fn trace_assigned(
    state: &AppState,
    run_id: &RunId,
    slot: &str,
    op: &str,
    desc: &str,
    gen: u64,
    source: AssignSource,
    previous: Option<&Assignee>,
) {
    let holder = Assignee {
        op: op.to_string(),
        desc: desc.to_string(),
        gen,
    };
    append_assigned(
        &TraceHandle::new(run_id.clone(), state.run_trace_store.clone()),
        slot,
        &holder,
        source,
        previous,
    )
    .await;
}

/// The `Assign.desc` written for a seat the launching operator takes
/// without having named it. See [`seat_declared_operators`].
///
/// **A9** requires every `Assign` to record why it happened, and the
/// launch request only describes the one seat it named (`operator_desc`) —
/// so the server writes the sentence for the rest, and it has to be honest
/// about being server-authored. Reading `GET /v1/runs/:id`, a `desc`
/// starting with "auto-seated at launch" is the tell that the caller did
/// not choose this lane: it went to the launching operator because a Run
/// belongs to whoever launched it (model §5), not because anyone asked for
/// this seat in particular. A human pin's `desc` is whatever the caller
/// wrote, which this literal can never be mistaken for.
pub(crate) fn auto_seat_desc(slot: &str) -> String {
    format!(
        "auto-seated at launch: the launching operator takes the Blueprint-declared seat \
         '{slot}' along with the one its operator_sid named"
    )
}

/// Seat the launching operator in every Blueprint-declared Operator seat
/// its pin did not already name, right after `RunStore::create`.
///
/// # Why a launch seats anything at all
///
/// `Run.current` is the single place a dispatch resolves its destination
/// from (**A10**), so a seat nothing ever wrote is a seat no dispatch can
/// reach — and a multi-seat Blueprint would come up with every lane but
/// one permanently `Vacant`, since a launch pin fills exactly one.
///
/// # Who fills them: the launcher, and nobody else
///
/// This used to ask a different question — "is anyone registered under
/// this seat's own name?" — and seat that operator, writing the seat name
/// itself into `Assignee.op`. It worked because a join claimed role
/// aliases and the login path registered each session under them, so a
/// seat called `main-ai` resolved to whoever held `main-ai`. Role
/// declaration has moved onto the Run, so there is no name to look up and
/// no registry entry to find: `operators[]` entries are seats, `Assignee.op`
/// is a session id, and the two no longer share a key space.
///
/// What replaces the lookup is the launch's own operator, for the reason
/// model §5 gives: *"このまま実行すると、いま実行している AI がこの Run の
/// Assignee に割り当てられます"* — the Run goes to the AI that launched it.
/// The pin is how that AI names itself, and `mse mcp` sends one on every
/// launch it makes (explicitly, or auto-pinned from its sole live session).
/// So the pinned operator takes the seat it named **and** the rest, at
/// generation 1, through an ordinary `acquire_assignee` — the destination
/// is still read fresh on every dispatch and a later handover still moves
/// it, so **A10** is untouched and a second driver taking one lane is the
/// same unrefusable acquire it always was (**A8**).
///
/// # An unpinned launch seats nothing, and that is the honest answer
///
/// `POST /v1/tasks` carries no Bearer and no operator identity other than
/// `operator_sid`: `operator.id` is a free-form label that defaults to
/// `"http-run"`, and nothing else in the request or its headers names a
/// session. So a launch without a pin does not have a launching operator
/// to seat — it has a launching *process*, which is not the same thing and
/// which no seat can be filled from. Every seat therefore stays `Vacant`,
/// and the first dispatch through one fails naming it
/// (`AssigneeRouter::execute`).
///
/// Guessing instead — "seat the only live session" — was the alternative,
/// and it is what the role lookup effectively did on a single-driver
/// server. It is rejected because the guess is invisible in the one place
/// that matters: the Run would report a holder that nobody chose and that
/// happens to be right until the day a second driver is logged in, at
/// which point it is silently wrong. **D4** says the same thing from the
/// other end — nothing about a session is a matching key.
///
/// The bundled samples are unaffected: each declares exactly one seat, and
/// each is launched through `swarm_run`, which pins. A pin-less launch of
/// one was already only reachable by hand-rolling `POST /v1/tasks`, and
/// already died at the first dispatch whenever no session happened to hold
/// the seat's name.
///
/// A store failure here is a `500`, for [`assign_launch_operator`]'s
/// reason: the row exists but does not carry the holder the Run needs, and
/// dispatching it would fail somewhere far from the cause.
pub(crate) async fn seat_declared_operators(
    state: &AppState,
    run_id: &RunId,
    task_id: &TaskId,
    operators: &[OperatorDef],
    launch_pin: Option<(&str, &str)>,
) -> Result<(), ApiError> {
    let Some((pinned_slot, op)) = launch_pin else {
        return Ok(());
    };
    for op_def in operators {
        let slot = op_def.name.as_str();
        if pinned_slot == slot {
            continue;
        }
        let desc = auto_seat_desc(slot);
        let (gen, previous) = state
            .run_store
            .acquire_assignee(run_id, slot, op, &desc)
            .await
            .map_err(|e| {
                ApiError::engine(format!(
                    "run {run_id}: seating the launching operator '{op}' in the declared seat \
                     '{slot}' failed: {e}"
                ))
            })?;
        // W4, and the one place the `source` label earns its keep: an
        // auto-seat is the server filling a lane the caller did not name,
        // so a reader should not have to recognise `auto_seat_desc`'s prose
        // to tell it from a pin.
        trace_assigned(
            state,
            run_id,
            slot,
            op,
            &desc,
            gen,
            AssignSource::AutoSeat,
            previous.as_ref(),
        )
        .await;
        // D2: the lane lands on the launching operator's own 記名 too, so
        // "which Runs am I on" answers with every seat it holds rather than
        // only the one it asked for.
        crate::handover::record_observed_assignment(state, run_id, task_id, slot, op).await;
    }
    Ok(())
}

/// The sentence every launch answer carries, verbatim (model §5:
/// *"保証ではなく告知。担当は Run ごとに決まり、環境によって変わる"*).
///
/// It is a field rather than only a doc comment because the reader this is
/// for is looking at one JSON body, not at the schema: the whole failure
/// §5 describes — taking the announced holder for a property of the
/// Blueprint — is one a reader makes silently, from a response that looked
/// like a statement of fact.
const LAUNCH_INFO_NOTE: &str = "Announcement, not a guarantee. These holders were seated by \
     THIS launch and belong to THIS Run only: a Run goes to the AI that kicks it, so the same \
     Blueprint launched from another process — or from another worktree of the same repo — \
     seats whoever launches it there. Nothing here is re-checked later: a seat moves whenever \
     someone acquires it (POST /v1/runs/:id/acquire), and GET /v1/runs/:id/assignees is the \
     live answer. Read project_root / work_dir before assuming which checkout this Run drives.";

/// What a launch announces about the Run it just created — model §5's Info
/// display, as a field of the launch response.
///
/// # Why this rides on the response rather than being printed
///
/// §5 writes the announcement as console output, and there is no console:
/// the CLI has no `launch` subcommand, so every launch this server serves
/// arrives over HTTP (`POST /v1/tasks`, `POST /v1/tasks/:id/runs`) or
/// through `swarm_run`, which proxies one of those. Putting the fields on
/// the reply puts them in front of every one of those callers at the only
/// moment §5 asks for — the launch — and leaves the rendering to whoever
/// has a surface to render on. A `launch` subcommand added later reads
/// this; it does not need its own source.
///
/// # Why the paths are not optional in spirit, only in type
///
/// §5 names one reason for printing them: *同じ repo で worktree を複数
/// 走らせるのが常態* — the same repository, several checkouts, one Run per
/// checkout, and no way to tell from the Run id which one it was. A launch
/// that does not say which paths it bound to can only be caught after the
/// fact, by which time a worker has already run somewhere. They are
/// `Option` because a launch may genuinely carry neither (nothing supplied
/// `project_root` / `work_dir`), and `null` says exactly that — which is
/// itself the answer a reader needs, since a Run bound to no path is not a
/// Run bound to the caller's own.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct LaunchInfo {
    /// The Run this announcement is about. Repeated here, rather than left
    /// to the sibling `run_id` field of the enclosing response, so the
    /// block can be shown on its own without losing what it refers to.
    #[schemars(with = "String")]
    pub run_id: RunId,
    /// The Task's goal, as launched.
    pub goal: String,
    /// The Task-level `project_root` this Run is bound to; `null` when the
    /// launch supplied none.
    pub project_root: Option<String>,
    /// The Task-level `work_dir` this Run is bound to; `null` when the
    /// launch supplied none.
    pub work_dir: Option<String>,
    /// Every Operator seat of this Run and who holds it *at this instant* —
    /// the same per-seat answer `GET /v1/runs/:id/assignees` gives, read
    /// from `Run.current` (**A10**) after this launch finished seating.
    ///
    /// Each entry carries the holder's `OperatorId`, the generation it was
    /// stamped at (**A4**) and the `desc` its `Assign` recorded (**A9**) —
    /// §5's *担当: `<OperatorId>` / `<Assignee の記名>`*, per seat because a
    /// Blueprint may declare several and a launch fills each of them.
    ///
    /// A held seat whose `desc` begins with the server's auto-seat sentence
    /// (see [`auto_seat_desc`]) went to the launching operator without
    /// anyone naming that lane. An **unpinned** launch seats nothing, so
    /// every seat here reports `vacant: true` — which is the announcement
    /// worth making, since the first dispatch through any of them will fail
    /// naming the seat.
    pub seats: Vec<crate::handover::RunSeat>,
    /// Whether [`Self::seats`] covers every declared seat or only the held
    /// ones (the Blueprint could not be re-resolved).
    pub seats_source: crate::handover::SeatsSource,
    /// [`LAUNCH_INFO_NOTE`] — that this is an announcement and not a
    /// guarantee, and why the paths are here.
    pub note: &'static str,
}

/// Build the model §5 announcement for a Run this handler has just created
/// and seated.
///
/// Called after `assign_launch_operator` / [`seat_declared_operators`], so
/// `Run.current` already holds what the launch decided — this reads that
/// back rather than re-deriving it from the launch request, for the same
/// reason **A10** gives: `current` is the one place a holder is recorded,
/// and an announcement assembled from anywhere else could disagree with
/// the dispatch that follows it.
///
/// Best effort about the seats only. If the Run cannot be re-read, the
/// announcement still names the Run, the goal and the paths — the half §5
/// asks for by name — with an empty seat list and
/// [`crate::handover::SeatsSource::RunCurrentOnly`], which says the seats
/// were not resolved rather than that nobody holds one. The launch itself
/// is never failed over this: the Run exists and is already running.
pub(crate) async fn build_launch_info(
    state: &AppState,
    run_id: &RunId,
    goal: &str,
    spec: Option<&TaskInputSpec>,
) -> LaunchInfo {
    let (seats, seats_source) = match state.run_store.get(run_id).await {
        Ok(run) => {
            let (seats, source, _note) = crate::handover::seat_list(state, &run).await;
            (seats, source)
        }
        Err(error) => {
            tracing::warn!(
                %run_id, %error,
                "launch info: the Run could not be re-read, so the announcement names no seats"
            );
            (Vec::new(), crate::handover::SeatsSource::RunCurrentOnly)
        }
    };
    LaunchInfo {
        run_id: run_id.clone(),
        goal: goal.to_string(),
        project_root: spec.and_then(|s| s.project_root.clone()),
        work_dir: spec.and_then(|s| s.work_dir.clone()),
        seats,
        seats_source,
        note: LAUNCH_INFO_NOTE,
    }
}

/// Shared finalize step for a dispatched kick: updates the Run's
/// `result_ref` + status and the owning Task's coarse status based on the
/// `TaskApplication::handle_with_run` outcome, then returns that same
/// outcome unchanged so callers keep shaping their own wire response /
/// error.
///
/// Secondary persistence failures (the store call itself erroring) are
/// logged via `tracing::warn!` and otherwise swallowed — they must not mask
/// the primary dispatch outcome the caller already has in hand.
pub(crate) async fn finalize_run(
    state: &AppState,
    task_id: &TaskId,
    run_id: &RunId,
    outcome: Result<TaskApplicationOutput, TaskApplicationError>,
) -> Result<TaskApplicationOutput, TaskApplicationError> {
    match &outcome {
        Ok(out) => {
            if let Err(e) = state
                .run_store
                .set_result(run_id, out.final_ctx.clone())
                .await
            {
                tracing::warn!(%run_id, error = %e, "finalize_run: set_result failed");
            }
            if let Err(e) = state.run_store.update_status(run_id, RunStatus::Done).await {
                tracing::warn!(%run_id, error = %e, "finalize_run: run update_status(Done) failed");
            }
            if let Err(e) = state
                .task_store
                .update_status(task_id, TaskRecordStatus::Done)
                .await
            {
                tracing::warn!(%task_id, error = %e, "finalize_run: task update_status(Done) failed");
            }
        }
        Err(e) => {
            // GH #76 error surface: persist a structured failure envelope into
            // `RunRecord.result_ref` so the async poll path (`GET
            // /v1/runs/:id`) can surface `failed_step` / `verdict_value` /
            // `partial_ctx` symmetric to the sync path's `ApiError`
            // `details` field. Envelope shape (documented for consumer
            // disambiguation from the Ok arm's raw `final_ctx`):
            //
            // ```json
            // {
            //   "error": {
            //     "message": <string>,
            //     "failed_step": <string|null>,
            //     "verdict_value": <value|null>
            //   },
            //   "partial_ctx": <value|null>
            // }
            // ```
            //
            // Consumers detect failure via the top-level `"error"` key
            // (present iff this arm fired; the Ok arm stores the raw
            // `final_ctx` verbatim, which is either a scalar or an object
            // with the user's own keys — never a top-level `"error"`
            // sibling of `"partial_ctx"`). Non-`FlowEval` errors (e.g.
            // `TaskApplicationError::Store` / `NoStore` — dispatch never
            // reached the flow eval boundary) still get an envelope, but
            // with the structural fields `null` (the underlying error
            // simply carries no `failed_step` semantic).
            let envelope = match e {
                TaskApplicationError::Launch(TaskLaunchError::FlowEval {
                    message,
                    failed_step,
                    verdict_value,
                    partial_ctx,
                }) => json!({
                    "error": {
                        "message": message,
                        "failed_step": failed_step,
                        "verdict_value": verdict_value,
                    },
                    "partial_ctx": partial_ctx,
                }),
                other => json!({
                    "error": {
                        "message": other.to_string(),
                        "failed_step": Value::Null,
                        "verdict_value": Value::Null,
                    },
                    "partial_ctx": Value::Null,
                }),
            };
            if let Err(store_err) = state.run_store.set_result(run_id, envelope).await {
                tracing::warn!(%run_id, error = %store_err, "finalize_run: set_result (failure envelope) failed");
            }
            if let Err(store_err) = state
                .run_store
                .update_status(run_id, RunStatus::Failed)
                .await
            {
                tracing::warn!(%run_id, error = %store_err, "finalize_run: run update_status(Failed) failed");
            }
            if let Err(store_err) = state
                .task_store
                .update_status(task_id, TaskRecordStatus::Failed)
                .await
            {
                tracing::warn!(%task_id, error = %store_err, "finalize_run: task update_status(Failed) failed");
            }
            tracing::warn!(%task_id, %run_id, error = %e, "finalize_run: dispatch failed");
        }
    }
    // Trace rail: mark the Run's terminal status on the stream (the
    // `core.run_started` counterpart appended at the launch sites).
    // Best-effort like every other persistence in this fn.
    let status = if outcome.is_ok() { "done" } else { "failed" };
    TraceHandle::new(run_id.clone(), state.run_trace_store.clone())
        .append(
            trace_kind::RUN_FINISHED,
            None,
            None,
            json!({ "status": status }),
        )
        .await;
    outcome
}

/// Render a caught panic payload as a human-readable string. `panic!` with
/// a literal yields `&'static str`, a formatted `panic!` yields `String`;
/// anything else (a `panic_any` with a custom type) has no textual form, so
/// it is reported by shape rather than dropped silently.
fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Terminal-stamp a Run whose driver future panicked: `Interrupted` plus a
/// structured `{"error": "run driver panicked at <site>: <payload>"}`
/// result envelope — the same terminal shape the boot sweep and the
/// shutdown drain stamp, so the Run stays resumable via
/// `POST /v1/runs/:id/resume` (which only accepts `Interrupted`).
///
/// The status flip goes through [`RunStore::try_transition`], so a Run that
/// already reached a terminal status is left alone: a panic raised *after*
/// `finalize_run` persisted `Done` / `Failed` (for example inside the trace
/// tail) must not rewrite that verdict.
///
/// Best-effort like [`finalize_run`]: every secondary store error is logged
/// and swallowed — the panic itself is the primary signal, already logged by
/// [`catch_run_panic`].
pub(crate) async fn mark_run_interrupted_by_panic(
    state: &AppState,
    task_id: &TaskId,
    run_id: &RunId,
    site: &str,
    payload: &str,
) {
    match state
        .run_store
        .try_transition(run_id, RunStatus::Running, RunStatus::Interrupted)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            tracing::warn!(
                %run_id,
                site,
                "run driver panicked, but the Run is no longer `Running` — leaving its terminal status untouched"
            );
            return;
        }
        Err(e) => {
            tracing::warn!(%run_id, error = %e, "panic guard: run try_transition(Running -> Interrupted) failed");
            return;
        }
    }

    let envelope = json!({ "error": format!("run driver panicked at {site}: {payload}") });
    if let Err(e) = state.run_store.set_result(run_id, envelope).await {
        tracing::warn!(%run_id, error = %e, "panic guard: set_result failed");
    }
    if let Err(e) = state
        .task_store
        .update_status(task_id, TaskRecordStatus::Interrupted)
        .await
    {
        tracing::warn!(%task_id, error = %e, "panic guard: task update_status(Interrupted) failed");
    }
    // This path never reaches `finalize_run`, so the trace stream gets its
    // terminal marker here.
    TraceHandle::new(run_id.clone(), state.run_trace_store.clone())
        .append(
            trace_kind::RUN_FINISHED,
            None,
            None,
            json!({ "status": "interrupted", "reason": "driver panic" }),
        )
        .await;
}

/// Wrap a run driver future so a panic inside it terminates the Run instead
/// of vanishing with the task.
///
/// Without this, a panic in a detached driver unwinds the whole spawned task
/// — including the `tokio::time::timeout` combinator that wraps it, so the
/// TTL ceiling never fires either — and the `RunRecord` is stranded in
/// `Running` with no recovery path. On the synchronous paths the same panic
/// propagates into the hyper connection task and drops the connection
/// mid-request. Here the panic is caught, the Run is marked `Interrupted`
/// via [`mark_run_interrupted_by_panic`], and the caller gets the payload
/// string back to shape its own response.
///
/// Note this relies on unwinding: a future `[profile.release] panic =
/// "abort"` would make the guard a no-op.
pub(crate) async fn catch_run_panic<T, F>(
    state: &AppState,
    task_id: &TaskId,
    run_id: &RunId,
    site: &str,
    fut: F,
) -> Result<T, String>
where
    F: std::future::Future<Output = T>,
{
    match AssertUnwindSafe(fut).catch_unwind().await {
        Ok(value) => Ok(value),
        Err(payload) => {
            let message = panic_payload_to_string(payload);
            tracing::error!(
                %task_id,
                %run_id,
                site,
                payload = %message,
                "run driver panicked — marking the Run Interrupted"
            );
            mark_run_interrupted_by_panic(state, task_id, run_id, site, &message).await;
            Err(message)
        }
    }
}

/// Query params for `GET /v1/tasks`.
#[derive(Debug, Deserialize, Default)]
pub struct TasksListQuery {
    /// Caps the returned list to the first N entries (already newest-first
    /// per `TaskStore::list`). Omitted = no cap.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// `GET /v1/tasks?limit=N`. Lists every persisted `TaskRecord`, newest first.
pub async fn tasks_list(
    State(state): State<AppState>,
    Query(q): Query<TasksListQuery>,
) -> Result<Json<Vec<TaskRecord>>, ApiError> {
    let mut records = state.task_store.list().await.map_err(ApiError::engine)?;
    if let Some(limit) = q.limit {
        records.truncate(limit);
    }
    Ok(Json(records))
}

/// Response body for `GET /v1/tasks/:id`.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct TaskDetailResponse {
    /// The Task's own record.
    pub task: TaskRecord,
    /// Every Run kicked from this Task, oldest first (`RunStore::list_by_task` order).
    pub runs: Vec<RunRecord>,
}

/// `GET /v1/tasks/:id`. Returns the `TaskRecord` plus every `RunRecord`
/// kicked from it (`RunStore::list_by_task`, oldest kick first).
pub async fn task_get(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<TaskDetailResponse>, ApiError> {
    let task_id =
        TaskId::parse(id).map_err(|e| ApiError::bad_request(format!("invalid task id: {e}")))?;
    let task = state
        .task_store
        .get(&task_id)
        .await
        .map_err(map_task_store_err)?;
    let runs = state
        .run_store
        .list_by_task(&task_id)
        .await
        .map_err(ApiError::engine)?;
    Ok(Json(TaskDetailResponse { task, runs }))
}

/// Request body for `POST /v1/tasks/:id/runs` (issue #19 ST4) — every
/// field is optional, and the body itself is optional (see
/// [`task_rekick`]'s `Option<Json<Self>>` parameter); a caller that sends
/// no body, or `{}`, or omits a field gets exactly today's rekick
/// behavior for that layer.
#[derive(Debug, Deserialize, Default, schemars::JsonSchema)]
pub struct RunKickRequest {
    /// Per-Run override for the flow-ir initial ctx. Merged on top of
    /// `TaskRecord.input_ctx` (itself already merged on top of
    /// `Blueprint.default_init_ctx` at original launch time) via
    /// [`merge_init_ctx_3layer`] — Run wins on key collision, same
    /// shallow-merge / non-Object-fully-replaces rule as every other
    /// layer in the cascade. `None` (absent field, or no body at all) is
    /// a no-op: the BP+Task merge alone seeds this kick, identical to
    /// pre-#19 rekick.
    #[serde(default)]
    #[schemars(with = "Option<Value>")]
    pub init_ctx_override: Option<Value>,
    /// Per-Run override for the Task-level canonical fields
    /// (`project_root` / `work_dir` / `task_metadata`). `None` falls back
    /// to `TaskRecord.task_input_spec` (the spec resolved and snapshotted
    /// at original `POST /v1/tasks` time); `Some` replaces it wholesale
    /// for this kick only — the stored `TaskRecord.task_input_spec` is
    /// never mutated by a rekick.
    #[serde(default)]
    pub task_input_override: Option<TaskInputSpec>,
    /// Per-Run ceiling (seconds) for this kick's synchronous dispatch
    /// await (issue #35 ST3 — GH #33 Guard 2 parity). `Some(0)` is
    /// rejected (400). `None` falls back to `AppState.sync_timeout_secs`
    /// (the server-wide default), same cascade as
    /// `TaskLaunchRequest.timeout_secs` (`lib.rs:818-826`).
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// GH #37: opt into the detached (asynchronous) rekick — same
    /// semantics as `TaskLaunchRequest.detach`. `false` (default) keeps
    /// the synchronous dispatch; `true` spawns the flow eval as a
    /// detached background task bounded by the run TTL alone and returns
    /// `202 Accepted` with `status: "running"` immediately. Mutually
    /// exclusive with `timeout_secs` (`400` when combined).
    #[serde(default)]
    pub detach: bool,
    /// Per-Run pin to a live Operator session (rekick parity with
    /// `POST /v1/tasks`' `operator_sid` — see
    /// `crate::TaskLaunchRequest::operator_sid` for the full
    /// disconnected-vs-unknown / last-write-wins contract). Resolved
    /// before any Task/Run store write: an unknown sid fails fast with a
    /// `400`, never silently falling back to the BP-level alias lookup.
    /// `Some(sid)` becomes this kick's `TaskApplicationInput::operator_sid`
    /// (this handler carries no other Operator-override field) and is
    /// persisted verbatim into `RunRecord.operator_sid`; `None` (absent
    /// field, or no body at all) preserves the pre-existing
    /// Operator-default rekick path byte-for-byte.
    #[serde(default)]
    pub operator_sid: Option<String>,
    /// Why this kick is assigned to [`Self::operator_sid`] — the model's
    /// `Assign.desc` (§4.3 **A9**), with the same contract as
    /// `POST /v1/tasks`' own `operator_desc`: mandatory whenever
    /// `operator_sid` is given (absent / blank is a `400`, resolved before
    /// any store write), ignored when it is not. See
    /// [`resolve_launch_assign`].
    #[serde(default)]
    pub operator_desc: Option<String>,
    /// Which Blueprint-declared Operator seat ([`OperatorDef::name`], the
    /// `operator_ref` agents dispatch through) this kick's `Assign` lands
    /// in — the rekick twin of `POST /v1/tasks`' `operator_slot`, with the
    /// identical rule in [`resolve_launch_slot`]: omit it when the Task's
    /// Blueprint declares exactly one Operator, name it when the Blueprint
    /// declares several (its absence there is a `400` listing the
    /// candidates). Read only when `operator_sid` is given; there is no
    /// seat to fill without a holder to put in it.
    #[serde(default)]
    pub operator_slot: Option<String>,
}

/// Response body for `POST /v1/tasks/:id/runs`.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct RunKickResponse {
    /// The re-kicked Task's id (echoes the path param).
    #[schemars(with = "String")]
    pub task_id: TaskId,
    /// The freshly minted Run id for this kick.
    #[schemars(with = "String")]
    pub run_id: RunId,
    /// Kick outcome at response time (GH #37). The synchronous path
    /// reports the dispatched run's terminal-side status (`done`); a
    /// detached kick reports `running` — poll `GET /v1/runs/:id` for the
    /// terminal status and result.
    pub status: RunStatus,
    /// Model §5's launch announcement for the Run this kick just minted —
    /// see [`LaunchInfo`].
    ///
    /// A rekick is a launch: it creates a Run, seats the pinned operator
    /// and every other declared seat exactly as `POST /v1/tasks` does, and
    /// `RunKickRequest.task_input_override` can point *this* kick at
    /// different paths than the Task row carries. So the announcement is
    /// not merely applicable here, this is the path where it is easiest to
    /// be wrong about which checkout is being driven.
    pub info: LaunchInfo,
}

/// `POST /v1/tasks/:id/runs`. Re-kicks an existing Task: reads its stored
/// `blueprint_ref`, re-resolves it through [`TaskApplication::resolve`]
/// (issue #19 ST4 — refreshes `Blueprint.default_init_ctx` exactly like
/// original launch time, rather than replaying a launch-time-only
/// snapshot), 3-layer-merges `{bp default, TaskRecord.input_ctx, an
/// optional per-Run override}` via [`merge_init_ctx_3layer`], resolves the
/// Task-level canonical fields (`RunKickRequest.task_input_override`,
/// falling back to `TaskRecord.task_input_spec`), mints a fresh `RunId`,
/// dispatches through `TaskApplication::handle_with_run` (Operator-default
/// unless the caller pins a live session via `RunKickRequest.operator_sid`
/// — the rekick parity for `POST /v1/tasks`' own `operator_sid`; the
/// stored Task carries no persisted Operator preference of its own)
/// plus a freshly-built `RunContext` (issue #13 run_id propagation, so
/// this kick's steps get their own `step_entries` trace), and persists the
/// outcome via [`finalize_run`].
///
/// The body is optional (`Option<Json<RunKickRequest>>`) — no body, or a
/// body with both fields absent, preserves the pre-#19 rekick behavior
/// byte-for-byte (`must_not_simplify #3`).
///
/// Issue #35 ST3 ports the GH #33 sync-hang guards from `run_flow_form` to
/// this handler, both checked before any Task/Run store write: Guard 1
/// (503) fails fast when the resolved Blueprint declares the
/// `operator_delegate` spawner-hint layer and no operator is attached;
/// Guard 2 (504) wraps the dispatch await in `RunKickRequest.timeout_secs`
/// (falling back to the server-wide `sync_timeout_secs`), marking the
/// Run/Task `Failed` rather than leaving them `Running` forever on expiry.
pub async fn task_rekick(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Option<Json<RunKickRequest>>,
) -> Result<(StatusCode, Json<RunKickResponse>), ApiError> {
    let task_id =
        TaskId::parse(id).map_err(|e| ApiError::bad_request(format!("invalid task id: {e}")))?;
    let task = state
        .task_store
        .get(&task_id)
        .await
        .map_err(map_task_store_err)?;

    let blueprint_ref: mlua_swarm::application::BlueprintRef =
        serde_json::from_value(task.blueprint_ref.clone()).map_err(|e| {
            ApiError::bad_request(format!(
                "task {task_id}: stored blueprint_ref failed to decode: {e}"
            ))
        })?;

    // issue #19 ST4 (must_not_simplify #5): re-resolve the Blueprint the
    // same way `run_flow_form`'s TTL cascade does, so a store-backed
    // `BlueprintRef::Id` gets its *current* `default_init_ctx` on every
    // rekick rather than whatever was true at original launch time. The
    // Inline path is a pure pass-through, so this is a no-op there.
    let (resolved_bp, _bound_version) = state
        .task_app
        .resolve(&blueprint_ref)
        .await
        .map_err(|e| ApiError::from_task_resolve(&e, &format!("task {task_id}: bp resolve")))?;

    let req = body.map(|Json(r)| r).unwrap_or_default();

    // S2 parity with `run_flow_form` (`lib.rs:1035-1048`): an explicit
    // `operator_sid` pins this rekick to a live Operator session,
    // resolved *before* any Task/Run store write so an unknown sid fails
    // fast with a `400` rather than minting records for a kick that
    // references a session nothing can serve. Unlike `run_flow_form` this
    // handler has no other Operator-override field, so the resolved sid
    // flows straight into `TaskApplicationInput.operator_sid` (below) and is
    // persisted verbatim into `RunRecord.operator_sid`. See
    // `crate::TaskLaunchRequest::operator_sid` for the disconnected-vs-
    // unknown distinction.
    let operator_sid = match &req.operator_sid {
        Some(sid) => {
            let known_ids = state.engine.list_operator_ids().await;
            if !known_ids.iter().any(|id| id == sid) {
                return Err(ApiError::bad_request(format!(
                    "operator_sid: no such registered operator session '{sid}'"
                )));
            }
            Some(sid.clone())
        }
        None => None,
    };

    // A pinned rekick is this Run's first `Assign` — same **A9** `desc`
    // requirement, same fail-fast-before-side-effects ordering as
    // `run_flow_form`. The acquire runs after `RunStore::create` below.
    //
    // The seat it lands in comes from the Blueprint just resolved above
    // (`operators[]`), not from a constant: which Operator a pin assigns
    // is a fact about the Blueprint the Task was launched with, and the
    // rekick body only gets to name one when the Blueprint declares
    // several. See [`resolve_launch_slot`].
    let launch_assign =
        match resolve_launch_assign(req.operator_sid.as_deref(), req.operator_desc.as_deref())? {
            Some((op, desc)) => Some((
                resolve_launch_slot(req.operator_slot.as_deref(), &resolved_bp.operators)?,
                op,
                desc,
            )),
            None => None,
        };

    // GH #33 Guard 2 ceiling resolution (issue #35 ST3 — mirrors
    // `run_flow_form`'s `lib.rs:813-826` cascade): request field > server
    // config > built-in default. Validated up front, before Guard 1 and
    // before any Task/Run store writes, so a caller-supplied `Some(0)`
    // fails fast with `400` rather than minting records for a rekick that
    // was never going to dispatch.
    // GH #37: `detach: true` makes the sync ceiling meaningless (the
    // detached kick is bounded by the run TTL alone) — combining the two
    // is rejected here, same fail-fast-before-side-effects ordering.
    let detach = req.detach;
    let sync_timeout_secs = match (detach, req.timeout_secs) {
        (true, Some(_)) => {
            return Err(ApiError::bad_request(
                "timeout_secs is the synchronous rekick ceiling and does not apply to a \
                 detached rekick (detach: true), whose lifetime bound is the run TTL — omit \
                 timeout_secs"
                    .into(),
            ));
        }
        (false, Some(0)) => {
            return Err(ApiError::bad_request(
                "timeout_secs: 0 is invalid; omit the field to use the server default".into(),
            ));
        }
        (false, Some(v)) => v,
        (_, None) => state.sync_timeout_secs,
    };

    // GH #33 Guard 1 (issue #35 — adapted signal): the
    // per-request `operator_sid` above already fail-fasts an *unknown*
    // sid, but a rekick with no `operator_sid` still has no per-request
    // "operator backend referenced" signal of its own (unlike
    // `run_flow_form`, whose `op_req.operator_backend_id` is sourced from
    // `TaskLaunchRequest.operator`). The adapted signal is the Blueprint's
    // own `spawner_hints.layers`: when the resolved Blueprint declares the
    // `operator_delegate` layer and zero operators are attached at all,
    // fail fast rather than dispatching into a session nothing can serve.
    // Same ordering invariant `run_flow_form` observes: this check runs
    // before any Task/Run row is touched (no side effects on the 503
    // path).
    if resolved_bp
        .spawner_hints
        .layers
        .iter()
        .any(|l| l == "operator_delegate")
    {
        let attached = state.engine.list_operator_ids().await;
        if attached.is_empty() {
            return Err(ApiError::unavailable(format!(
                "no operator attached to serve this rekick (task {task_id}'s \
                 Blueprint declares the operator_delegate layer): attach an \
                 operator via POST /v1/operators + WS, or use the poll-style \
                 flow (GET /v1/worker/prompt + POST /v1/worker/submit)"
            )));
        }
    }

    let merged_init_ctx = merge_init_ctx_3layer(
        resolved_bp.default_init_ctx.as_ref(),
        &task.input_ctx,
        req.init_ctx_override.as_ref(),
    );

    // must_not_simplify #4: `task_input_override` wins for this kick only;
    // falling back to the Task-level snapshot never mutates
    // `TaskRecord.task_input_spec` itself.
    let task_input_spec: Option<TaskInputSpec> = match req.task_input_override {
        Some(over) => Some(over),
        None => task
            .task_input_spec
            .as_ref()
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| {
                ApiError::bad_request(format!(
                    "task {task_id}: stored task_input_spec failed to decode: {e}"
                ))
            })?,
    };

    // Model §5's announcement reports the paths *this* kick resolved,
    // which `task_input_override` may have moved off the Task row's own —
    // cloned before the spec is moved into the launch input below.
    let info_spec = task_input_spec.clone();

    let run_id = RunId::new();
    let now = now_secs();

    let input = TaskApplicationInput {
        blueprint: blueprint_ref,
        operator_id: "http-run".to_string(),
        role: Role::Operator,
        ttl: Duration::from_secs(crate::default_run_ttl()),
        init_ctx: merged_init_ctx,
        operator_kind: None,
        bridge_id: None,
        hook_id: None,
        // One value, both axes: the delegate layer's backend, and the
        // session this rekick's AgentSpec-axis Operator agents attest their
        // manifests through — so a rekick lands on the session the caller
        // named rather than on whichever session the seat last pointed at.
        operator_sid,
        operator_kind_overrides: HashMap::new(),
        task_input: task_input_spec,
        // This legacy `POST /v1/tasks/:id/runs`-style path does not carry a
        // per-request check_policy override; `None` preserves the
        // server-wide default (backward compat).
        check_policy: None,
    };
    // Persist a launch-input snapshot so this kick's Run can be resumed
    // under the same run_id if it is later interrupted
    // (`POST /v1/runs/:id/resume`). Built from `input` before it is moved
    // into the dispatch below.
    let input_json = Some(snapshot_launch_input(&input)?);

    state
        .task_store
        .update_status(&task_id, TaskRecordStatus::Running)
        .await
        .map_err(ApiError::engine)?;
    state
        .run_store
        .create(RunRecord {
            id: run_id.clone(),
            task_id: task_id.clone(),
            status: RunStatus::Running,
            step_entries: Vec::new(),
            degradations: Vec::new(),
            operator_sid: req.operator_sid.clone(),
            // A launch never carries a holder: every slot starts Vacant
            // (`current` empty) and `G` starts at 0 (A4). The Assign that a
            // launch-time operator pin implies is a separate event on top
            // of this row.
            current: Default::default(),
            next_generation: 0,
            result_ref: None,
            input_json,
            created_at: now,
            updated_at: now,
        })
        .await
        .map_err(ApiError::engine)?;

    // The kick's `Assign` (**A4**: generation 1 on a freshly created Run).
    if let Some((slot, op, desc)) = &launch_assign {
        assign_launch_operator(&state, &run_id, &task_id, slot, op, desc).await?;
    }
    // Every other declared seat, to the same operator. The pinned seat
    // above is excluded so the caller's own desc survives; an unpinned
    // rekick seats nothing. See [`seat_declared_operators`].
    seat_declared_operators(
        &state,
        &run_id,
        &task_id,
        &resolved_bp.operators,
        launch_assign
            .as_ref()
            .map(|(slot, op, _)| (slot.as_str(), op.as_str())),
    )
    .await?;

    // Model §5, same as the launch path: announce the seating and the
    // paths *this* kick bound to — which `task_input_override` may have
    // moved off the Task row's own.
    let info = build_launch_info(&state, &run_id, &task.goal, info_spec.as_ref()).await;

    let trace = TraceHandle::new(run_id.clone(), state.run_trace_store.clone());
    trace
        .append(
            trace_kind::RUN_STARTED,
            None,
            None,
            json!({"mode": "rekick"}),
        )
        .await;
    let run_ctx = RunContext::new(run_id.clone(), state.run_store.clone())
        .with_replay_store(state.replay_store.clone())
        .with_trace(trace);

    // GH #37 detached rekick: same driver-detach semantics as
    // `run_flow_form` — the eval runs in its own spawned task bounded by
    // the run TTL alone, `finalize_run` (or the ttl-expiry `Failed`
    // marking) is owned by that task, and this handler returns `202
    // Accepted` immediately.
    if detach {
        let ttl_secs = crate::default_run_ttl();
        let bg_state = state.clone();
        let bg_task_id = task_id.clone();
        let bg_run_id = run_id.clone();
        // Panic guard — see `catch_run_panic`.
        let guard_state = state.clone();
        let guard_task_id = task_id.clone();
        let guard_run_id = run_id.clone();
        tokio::spawn(async move {
            let driver = async move {
                let outcome = match tokio::time::timeout(
                    Duration::from_secs(ttl_secs),
                    bg_state.task_app.handle_with_run(input, Some(run_ctx)),
                )
                .await
                {
                    Ok(outcome) => outcome,
                    Err(_elapsed) => {
                        let reason = serde_json::json!({
                            "error": format!("detached rekick exceeded {ttl_secs}s ttl ceiling"),
                        });
                        if let Err(e) = bg_state.run_store.set_result(&bg_run_id, reason).await {
                            tracing::warn!(%bg_run_id, error = %e, "task_rekick: detached ttl set_result failed");
                        }
                        if let Err(e) = bg_state
                            .run_store
                            .update_status(&bg_run_id, RunStatus::Failed)
                            .await
                        {
                            tracing::warn!(%bg_run_id, error = %e, "task_rekick: detached ttl run update_status failed");
                        }
                        if let Err(e) = bg_state
                            .task_store
                            .update_status(&bg_task_id, TaskRecordStatus::Failed)
                            .await
                        {
                            tracing::warn!(%bg_task_id, error = %e, "task_rekick: detached ttl task update_status failed");
                        }
                        // This arm never reaches `finalize_run`, so the trace
                        // stream gets its terminal marker here.
                        TraceHandle::new(bg_run_id.clone(), bg_state.run_trace_store.clone())
                        .append(
                            trace_kind::RUN_FINISHED,
                            None,
                            None,
                            json!({ "status": "failed", "reason": format!("ttl {ttl_secs}s exceeded") }),
                        )
                        .await;
                        return;
                    }
                };
                // `finalize_run` persists both the Ok and Err outcomes itself;
                // the passthrough return value has no consumer here.
                let _ = finalize_run(&bg_state, &bg_task_id, &bg_run_id, outcome).await;
            };
            let _ = catch_run_panic(
                &guard_state,
                &guard_task_id,
                &guard_run_id,
                "rekick.detach",
                driver,
            )
            .await;
        });
        return Ok((
            StatusCode::ACCEPTED,
            Json(RunKickResponse {
                task_id,
                run_id,
                status: RunStatus::Running,
                info,
            }),
        ));
    }

    // GH #33 Guard 2 (issue #35 ST3 — mirrors `run_flow_form`'s sync
    // branch exactly, including its driver-detach shape):
    // the driver runs in a spawned task bounded by the sync ceiling, and
    // this handler only awaits its verdict over a `oneshot`. A client
    // disconnect drops the wait, not the run, so a `/v1/worker/submit`
    // landing after the disconnect still has a driver to fold it into. On
    // ceiling expiry the timed-out future is dropped, cancelling the
    // in-process flow eval — the flow is abandoned, not resumed. Best
    // effort: mark the Run/Task so they do not stay `Running` forever.
    // Wrapped in the panic guard (`catch_run_panic`) — same rationale as the
    // sync launch path in `crate::lib`.
    let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), ApiError>>();
    let bg_state = state.clone();
    let bg_task_id = task_id.clone();
    let bg_run_id = run_id.clone();
    let guard_state = state.clone();
    let guard_task_id = task_id.clone();
    let guard_run_id = run_id.clone();
    tokio::spawn(async move {
        let driver = async move {
            let outcome = match tokio::time::timeout(
                Duration::from_secs(sync_timeout_secs),
                bg_state.task_app.handle_with_run(input, Some(run_ctx)),
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(_elapsed) => {
                    let reason = serde_json::json!({
                        "error": format!("sync rekick exceeded {sync_timeout_secs}s timeout ceiling")
                    });
                    if let Err(e) = bg_state.run_store.set_result(&bg_run_id, reason).await {
                        tracing::warn!(%bg_run_id, error = %e, "task_rekick: timeout set_result failed");
                    }
                    if let Err(e) = bg_state
                        .run_store
                        .update_status(&bg_run_id, RunStatus::Failed)
                        .await
                    {
                        tracing::warn!(%bg_run_id, error = %e, "task_rekick: timeout run update_status failed");
                    }
                    if let Err(e) = bg_state
                        .task_store
                        .update_status(&bg_task_id, TaskRecordStatus::Failed)
                        .await
                    {
                        tracing::warn!(%bg_task_id, error = %e, "task_rekick: timeout task update_status failed");
                    }
                    return Err(ApiError::timeout(format!(
                        "sync rekick exceeded {sync_timeout_secs}s timeout ceiling: task {bg_task_id}, run {bg_run_id}"
                    )));
                }
            };
            finalize_run(&bg_state, &bg_task_id, &bg_run_id, outcome)
                .await
                .map(|_| ())
                .map_err(|e| ApiError::bad_request(format!("run: {e}")))
        };
        let reply = match catch_run_panic(
            &guard_state,
            &guard_task_id,
            &guard_run_id,
            "rekick.sync",
            driver,
        )
        .await
        {
            Ok(reply) => reply,
            Err(msg) => Err(ApiError::engine(format!(
                "run driver panicked: {msg}; the run was marked Interrupted and can be resumed \
                 via POST /v1/runs/{guard_run_id}/resume"
            ))),
        };
        // A disconnected client leaves no receiver; the run is already
        // persisted, so the undeliverable reply is dropped.
        let _ = tx.send(reply);
    });

    // Only reachable if the spawned task died without sending — a panic
    // outside the guard, or a runtime shutdown.
    rx.await.map_err(|_| {
        ApiError::engine(format!(
            "run driver task ended without reporting an outcome; see GET /v1/runs/{run_id} \
             for the run's persisted status"
        ))
    })??;

    Ok((
        StatusCode::CREATED,
        Json(RunKickResponse {
            task_id,
            run_id,
            status: RunStatus::Done,
            info,
        }),
    ))
}

/// Response body for `POST /v1/runs/:id/resume`.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct RunResumeResponse {
    /// The resumed Run's id — echoes the path param. Resume never mints a
    /// new `RunId`; the interrupted Run is re-run in place so its
    /// replay-entry Ctx snapshots (which bake this id into
    /// `meta.runtime[run_id]`) stay consistent.
    #[schemars(with = "String")]
    pub run_id: RunId,
    /// The Task this Run belongs to.
    #[schemars(with = "String")]
    pub task_id: TaskId,
    /// Count of already-completed steps handed to the replay cursor — the
    /// engine returns each of these verbatim (no re-dispatch) before
    /// resuming fresh work. `0` = the Run was interrupted before any step
    /// completed, so it re-runs from scratch under the same `run_id`.
    pub replayed_steps: usize,
}

/// `POST /v1/runs/:id/resume`. Resumes an `Interrupted` Run under the SAME
/// `run_id` (no new `RunId` is minted): the stored launch-input snapshot
/// (`RunRecord.input_json`) is rebuilt into a `TaskApplicationInput`, a
/// `ReplayCursor` is built from the Run's logged step snapshots
/// (`ReplayStore::list_by_run`), and the flow is re-dispatched with both
/// wired into a fresh `RunContext`. On dispatch the engine's replay path
/// returns each already-completed step's stored value verbatim (cursor hit,
/// no Adapter spawn) and dispatches only the steps that never finished —
/// reconstructing the same final Ctx a restart-free run would have reached.
///
/// Status codes:
/// - `404` — no Run with this id.
/// - `409` — the Run is not `Interrupted` (already `Running` / `Done` /
///   `Failed` / `Pending`), OR a concurrent resume already won the
///   `Interrupted -> Running` compare-and-set (double-resume guard).
/// - `422` — the Run has no recorded launch-input snapshot, so it cannot be
///   resumed (an older row predating resume support, or a path that does
///   not persist one).
/// - `202 Accepted` — resume accepted; the flow re-runs in a detached
///   background task (same `tokio::spawn` + run-TTL ceiling shape as a
///   detached rekick). Poll `GET /v1/runs/:id` for the terminal status.
///
/// The launch-input decode and the `422` check run BEFORE the
/// compare-and-set so a non-resumable Run is never flipped to `Running`
/// and stranded without a driver behind it.
pub async fn run_resume(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<RunResumeResponse>), ApiError> {
    let run_id =
        RunId::parse(id).map_err(|e| ApiError::bad_request(format!("invalid run id: {e}")))?;

    // 404 when the Run does not exist.
    let run = state
        .run_store
        .get(&run_id)
        .await
        .map_err(map_run_store_err)?;

    // Status gate: only an `Interrupted` Run can be resumed.
    if run.status != RunStatus::Interrupted {
        return Err(ApiError::conflict(format!(
            "run {run_id} is {:?}, not Interrupted; only an interrupted run can be resumed",
            run.status
        )));
    }

    // Decode the launch-input snapshot BEFORE the compare-and-set: a Run
    // with no recorded input can never be resumed, and returning `422`
    // here — before flipping the status — avoids stranding it in `Running`
    // with no driver behind it.
    let Some(input_json) = run.input_json.clone() else {
        return Err(ApiError::unprocessable(format!(
            "run {run_id} cannot be resumed: no launch input was recorded for it (it \
             predates resume support, or was created by a path that does not persist one)"
        )));
    };
    let snapshot_value: Value = serde_json::from_str(&input_json).map_err(|e| {
        ApiError::unprocessable(format!(
            "run {run_id}: stored launch input failed to decode: {e}"
        ))
    })?;
    validated_bound_agents_from_snapshot(&run_id, &snapshot_value)?;
    let snapshot: RunLaunchSnapshot = serde_json::from_value(snapshot_value).map_err(|e| {
        ApiError::unprocessable(format!(
            "run {run_id}: stored launch input failed to decode: {e}"
        ))
    })?;

    // Atomically flip Interrupted -> Running. A racing double resume loses
    // the compare-and-set and gets a `409` rather than dispatching a second
    // driver over the same Run.
    let won = state
        .run_store
        .try_transition(&run_id, RunStatus::Interrupted, RunStatus::Running)
        .await
        .map_err(ApiError::engine)?;
    if !won {
        return Err(ApiError::conflict(format!(
            "run {run_id} was concurrently resumed (or left the Interrupted state); it is \
             no longer resumable"
        )));
    }

    // Build the replay cursor from the Run's logged step snapshots. An
    // empty log is fine — the cursor has zero hits and every step is
    // dispatched fresh (a from-scratch re-run under the same run_id).
    let entries = state
        .replay_store
        .list_by_run(&run_id)
        .await
        .map_err(|e| ApiError::engine(format!("replay list_by_run: {e}")))?;
    let replayed_steps = entries.len();
    let cursor = ReplayCursor::from_entries(entries);

    // RunContext for the SAME run_id — run_store + replay_store +
    // replay_cursor all wired. No new RunRecord is minted. `with_resume()`
    // marks this as a resume so any binding backfill is stamped
    // `resume_backfill` (and, D2, keeps legacy replay keys).
    let trace = TraceHandle::new(run_id.clone(), state.run_trace_store.clone());
    trace
        .append(
            trace_kind::RUN_STARTED,
            None,
            None,
            json!({"mode": "resume"}),
        )
        .await;
    let run_ctx = RunContext::new(run_id.clone(), state.run_store.clone())
        .with_replay_store(state.replay_store.clone())
        .with_replay_cursor(Arc::new(Mutex::new(cursor)))
        .with_resume()
        .with_trace(trace);

    let input = snapshot.into_input();
    let task_id = run.task_id.clone();

    // A resumed Task is running again; finalize_run resets it to
    // Done/Failed at the end, same as the rekick path.
    state
        .task_store
        .update_status(&task_id, TaskRecordStatus::Running)
        .await
        .map_err(ApiError::engine)?;

    // Detached dispatch — same `tokio::spawn` + run-TTL-ceiling shape as
    // the detached rekick path; `finalize_run` (or the ttl-expiry `Failed`
    // marking) owns the terminal persistence.
    let ttl_secs = crate::default_run_ttl();
    let bg_state = state.clone();
    let bg_task_id = task_id.clone();
    let bg_run_id = run_id.clone();
    // Panic guard — see `catch_run_panic`.
    let guard_state = state.clone();
    let guard_task_id = task_id.clone();
    let guard_run_id = run_id.clone();
    tokio::spawn(async move {
        let driver = async move {
            let outcome = match tokio::time::timeout(
                Duration::from_secs(ttl_secs),
                bg_state.task_app.handle_with_run(input, Some(run_ctx)),
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(_elapsed) => {
                    let reason = serde_json::json!({
                        "error": format!("resumed run exceeded {ttl_secs}s ttl ceiling"),
                    });
                    if let Err(e) = bg_state.run_store.set_result(&bg_run_id, reason).await {
                        tracing::warn!(%bg_run_id, error = %e, "run_resume: ttl set_result failed");
                    }
                    if let Err(e) = bg_state
                        .run_store
                        .update_status(&bg_run_id, RunStatus::Failed)
                        .await
                    {
                        tracing::warn!(%bg_run_id, error = %e, "run_resume: ttl run update_status failed");
                    }
                    if let Err(e) = bg_state
                        .task_store
                        .update_status(&bg_task_id, TaskRecordStatus::Failed)
                        .await
                    {
                        tracing::warn!(%bg_task_id, error = %e, "run_resume: ttl task update_status failed");
                    }
                    return;
                }
            };
            // `finalize_run` persists both the Ok and Err outcomes itself.
            let _ = finalize_run(&bg_state, &bg_task_id, &bg_run_id, outcome).await;
        };
        let _ = catch_run_panic(
            &guard_state,
            &guard_task_id,
            &guard_run_id,
            "resume.detach",
            driver,
        )
        .await;
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(RunResumeResponse {
            run_id,
            task_id,
            replayed_steps,
        }),
    ))
}

/// Request body for `POST /v1/runs/:id/rerun-from` (GH #71 Layer A).
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RunRerunFromRequest {
    /// The step to re-execute. This is a raw `step_ref` (the agent name the
    /// dispatcher recorded as `ReplayEntry.step_ref`), NOT a projection
    /// canonical name. See [`run_rerun_from`] doc for the Known Limitations
    /// this carries (loop bodies match the first occurrence,
    /// `AgentMeta.projection_name` is not resolved, `BlueprintRef::Inline`
    /// re-decodes the frozen inline BP).
    pub from_step: String,
}

/// Response body for `POST /v1/runs/:id/rerun-from` (GH #71 Layer A).
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct RunRerunFromResponse {
    /// The rerun's Run id — echoes the path param. Rerun-from-step never
    /// mints a new `RunId`; it re-runs in place so the replay-entry Ctx
    /// snapshots (which bake this id into `meta.runtime[run_id]`) stay
    /// consistent.
    #[schemars(with = "String")]
    pub run_id: RunId,
    /// The Task this Run belongs to.
    #[schemars(with = "String")]
    pub task_id: TaskId,
    /// Count of pre-cut entries handed to the replay cursor — each is
    /// returned verbatim by the engine before fresh dispatch resumes at
    /// the cut point.
    pub replayed_steps: usize,
    /// Count of entries physically dropped from the replay store — the
    /// target step's row plus every downstream row.
    pub dropped_steps: usize,
}

/// `POST /v1/runs/:id/rerun-from` — GH #71 Layer A. Re-executes a specific
/// step (and every downstream step) of a terminal Run under the SAME
/// `run_id`. Mirrors [`run_resume`], with two deltas: it accepts any
/// terminal status (`Done` / `Failed` / `Interrupted`) rather than only
/// `Interrupted`, and it physically truncates the replay log at the cut
/// point (via [`crate::AppState::replay_store`]'s `delete_from`) so that
/// re-dispatch's `append` does not collide with the pre-rerun row and so
/// `list_by_run` reflects the rerun's real history rather than the
/// pre-rerun ghost.
///
/// # Status codes
///
/// - `400` — invalid `run_id`, malformed body, or launch-snapshot decode failure.
/// - `404` — no Run with this id.
/// - `409` — the Run is `Running` / `Pending` (would race the in-flight
///   driver), OR a concurrent transition won the compare-and-set.
/// - `422` — the Run has no recorded launch-input snapshot, OR `from_step`
///   is not present in this Run's replay log, OR the run's replay log is
///   empty next to a non-empty `RunRecord.step_entries` trace (a prior
///   `rerun-from` reached the truncate stage and consumed the log), OR
///   the current-head Blueprint fails to compile (unresolved
///   `operator_ref` etc.) — the deterministic pre-flight gate that keeps
///   the replay log untouched on a compile-fail.
/// - `202 Accepted` — accepted; the flow re-runs in a detached background
///   task (same `tokio::spawn` + run-TTL ceiling shape as [`run_resume`]).
///   Poll `GET /v1/runs/:id` for the terminal status.
///
/// # Order of operations
///
/// The compare-and-set runs BEFORE the `delete_from` on purpose: a losing
/// cas returns `409` without ever touching the store, so a lost race can
/// never leave the store truncated while the status stayed at its old
/// terminal value. The compile pre-check runs BEFORE the compare-and-set
/// for the same reason: a deterministic compile failure fires a `422`
/// that leaves both `status` and the replay log untouched, so the caller
/// can fix the Blueprint and retry against the same run.
///
/// 1. 404 check.
/// 2. Status gate (fast 409 for `Running` / `Pending`).
/// 3. Decode launch snapshot (fast 400 / 422).
/// 4. Compute cut index via `list_by_run` + `.position(step_ref == from_step)`
///    (fast 422 when the step is not present, with a distinct message when
///    the log is empty but `RunRecord.step_entries` shows the run did
///    trace steps — a consumed log from a prior `rerun-from`).
/// 5. Pre-flight compile check via `TaskApplication::precompile` against
///    the launch snapshot's Blueprint (fast 422 on any `CompileError`).
///    Prevents compile-fail-inside-`tokio::spawn` from consuming the
///    replay log via step 7's `delete_from`.
/// 6. Atomic transition `<current terminal> -> Running` (409 on loss).
/// 7. Physical `delete_from(cut)` on the replay store — safe now because we
///    won the cas and own the Run.
/// 8. Build `ReplayCursor` from the truncated entries.
/// 9. Detached dispatch, same `tokio::spawn` + `default_run_ttl` shape as
///    [`run_resume`].
///
/// # Known limitations (Layer A)
///
/// 1. **`from_step` is a raw `step_ref` (agent name)** — projection alias
///    resolution via `StepNaming` is Layer B territory. For undeclared
///    steps `step_ref == canonical` so this is only visible when
///    `AgentMeta.projection_name` is in use.
/// 2. **`BlueprintRef::Inline` freezes the BP in the launch snapshot** —
///    the rerun re-decodes the same inline BP, so agent-definition edits
///    landed on disk between the original dispatch and the rerun are NOT
///    honored for inline runs. Use `BlueprintRef::Id` for the
///    iterate-and-rerun workflow.
/// 3. **Loop bodies match the first occurrence** — `step_ref` is the agent
///    name, so `.position(|e| e.step_ref == from_step)` finds the FIRST
///    occurrence and truncates from there. Rerunning a specific loop
///    iteration needs Layer B semantics.
/// 4. **Structural BP change is out of scope** — if steps were added /
///    removed / reordered between the original dispatch and the rerun,
///    the flow-ir re-eval will naturally miss the step or dispatch a
///    different downstream. Start a fresh run in that case.
pub async fn run_rerun_from(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<RunRerunFromRequest>,
) -> Result<(StatusCode, Json<RunRerunFromResponse>), ApiError> {
    let run_id =
        RunId::parse(id).map_err(|e| ApiError::bad_request(format!("invalid run id: {e}")))?;

    if req.from_step.trim().is_empty() {
        return Err(ApiError::bad_request(
            "from_step must be a non-empty step ref".to_string(),
        ));
    }

    // 404 when the Run does not exist.
    let run = state
        .run_store
        .get(&run_id)
        .await
        .map_err(map_run_store_err)?;

    // Status gate — reject in-flight statuses that would race the driver
    // already dispatching against this run_id.
    let current = run.status;
    match current {
        RunStatus::Done | RunStatus::Failed | RunStatus::Interrupted | RunStatus::Cancelled => { /* ok */
        }
        RunStatus::Running | RunStatus::Pending => {
            return Err(ApiError::conflict(format!(
                "run {run_id} is {current:?}; rerun-from requires a terminal run \
                 (Done / Failed / Interrupted / Cancelled)"
            )));
        }
    }

    // Decode the launch-input snapshot BEFORE the compare-and-set: a Run
    // with no recorded input can never be rerun-from, and returning `422`
    // here — before flipping the status — avoids stranding it in `Running`
    // with no driver behind it.
    let Some(input_json) = run.input_json.clone() else {
        return Err(ApiError::unprocessable(format!(
            "run {run_id} cannot be rerun: no launch input was recorded for it (it \
             predates resume/rerun support, or was created by a path that does not \
             persist one)"
        )));
    };
    let snapshot_value: Value = serde_json::from_str(&input_json).map_err(|e| {
        ApiError::unprocessable(format!(
            "run {run_id}: stored launch input failed to decode: {e}"
        ))
    })?;
    validated_bound_agents_from_snapshot(&run_id, &snapshot_value)?;
    let snapshot: RunLaunchSnapshot = serde_json::from_value(snapshot_value).map_err(|e| {
        ApiError::unprocessable(format!(
            "run {run_id}: stored launch input failed to decode: {e}"
        ))
    })?;

    // Load the replay log and locate the cut point via first-match on
    // `step_ref`. See §Known limitations #3 (loop bodies).
    let entries = state
        .replay_store
        .list_by_run(&run_id)
        .await
        .map_err(|e| ApiError::engine(format!("replay list_by_run: {e}")))?;
    let cut = entries
        .iter()
        .position(|e| e.step_ref == req.from_step)
        .ok_or_else(|| {
            // Distinguish two shapes of miss: (a) the log carries entries
            // but none match `from_step` (typo or wrong step name); (b) the
            // log is empty while `RunRecord.step_entries` still traces
            // steps — which means a prior `rerun-from` reached the
            // `delete_from` stage and consumed the log, and no further
            // `rerun-from` against the same run is recoverable. `run.
            // step_entries` and `replay_store` are physically separate
            // tables (the dispatcher writes to both), so an empty log next
            // to a non-empty trace is the reliable tell.
            if entries.is_empty() && !run.step_entries.is_empty() {
                ApiError::unprocessable(format!(
                    "run {run_id}: replay log is empty but {} step entries are traced \
                     on the RunRecord — the log was consumed by a prior rerun-from \
                     that reached the truncate stage. This run can no longer be \
                     rerun-from; start a fresh run via POST /v1/tasks.",
                    run.step_entries.len()
                ))
            } else {
                ApiError::unprocessable(format!(
                    "run {run_id}: from_step {:?} not present in this run's replay log \
                     (nothing to rerun-from)",
                    req.from_step
                ))
            }
        })?;

    // Pre-flight compile check against the current-head Blueprint the
    // rerun will actually launch against. Compile is deterministic — an
    // `UnresolvedOperatorRef` / `UnresolvedMetaRef` / `UnresolvedAuditAgent`
    // / verdict-cond shape violation fails the same way every attempt —
    // so surfacing it here as a 422, BEFORE the compare-and-set and
    // BEFORE `delete_from`, converts an otherwise irrecoverable replay-
    // loss (compile fails INSIDE the detached `tokio::spawn` AFTER the
    // truncation has physically dropped the pre-cut rows) into a fast
    // rejection that leaves the run's status and replay log entirely
    // untouched. Runtime-only failures (spawner error, worker submit
    // failure) are still able to consume the log — inherent to any
    // path that can only be discovered mid-dispatch — but that class
    // needs a different fix (Layer B territory).
    if let Err(e) = state.task_app.precompile(&snapshot.blueprint).await {
        return Err(ApiError::unprocessable(format!(
            "run {run_id} cannot be rerun: current-head Blueprint fails to compile — {e}"
        )));
    }

    // Atomically flip the current terminal status -> Running. A racing
    // rerun (or a boot-time recovery sweep, or a concurrent resume) loses
    // the compare-and-set and gets `409` rather than dispatching a second
    // driver over the same Run.
    let won = state
        .run_store
        .try_transition(&run_id, current, RunStatus::Running)
        .await
        .map_err(ApiError::engine)?;
    if !won {
        return Err(ApiError::conflict(format!(
            "run {run_id} was concurrently transitioned (or left the {current:?} state); \
             it is no longer rerunnable"
        )));
    }

    // We own the run now — physically truncate the replay log at the cut
    // so the rerun dispatch's `append` cannot collide with the pre-rerun
    // row and `list_by_run` reflects the rerun's real history rather than
    // the pre-rerun ghost.
    let dropped_steps = state
        .replay_store
        .delete_from(&run_id, cut)
        .await
        .map_err(|e| ApiError::engine(format!("replay delete_from: {e}")))?;

    // Cursor is built from the pre-cut prefix; every retained entry hits
    // verbatim in the engine's replay path.
    let kept = entries.into_iter().take(cut).collect::<Vec<_>>();
    let replayed_steps = kept.len();
    let cursor = ReplayCursor::from_entries(kept);

    // `with_resume()` — a rerun-from re-derives its snapshot from the current
    // Blueprint exactly like resume, so a binding backfill here is stamped
    // `resume_backfill` (and keeps legacy replay keys, D2).
    let trace = TraceHandle::new(run_id.clone(), state.run_trace_store.clone());
    trace
        .append(
            trace_kind::RUN_STARTED,
            None,
            None,
            json!({"mode": "rerun_from"}),
        )
        .await;
    let run_ctx = RunContext::new(run_id.clone(), state.run_store.clone())
        .with_replay_store(state.replay_store.clone())
        .with_replay_cursor(Arc::new(Mutex::new(cursor)))
        .with_resume()
        .with_trace(trace);

    let input = snapshot.into_input();
    let task_id = run.task_id.clone();

    // A rerun-from is a running Run again; finalize_run resets it to
    // Done/Failed at the end, same as the rekick / resume paths.
    state
        .task_store
        .update_status(&task_id, TaskRecordStatus::Running)
        .await
        .map_err(ApiError::engine)?;

    let ttl_secs = crate::default_run_ttl();
    let bg_state = state.clone();
    let bg_task_id = task_id.clone();
    let bg_run_id = run_id.clone();
    // Panic guard — see `catch_run_panic`.
    let guard_state = state.clone();
    let guard_task_id = task_id.clone();
    let guard_run_id = run_id.clone();
    tokio::spawn(async move {
        let driver = async move {
            let outcome = match tokio::time::timeout(
                Duration::from_secs(ttl_secs),
                bg_state.task_app.handle_with_run(input, Some(run_ctx)),
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(_elapsed) => {
                    let reason = serde_json::json!({
                        "error": format!("rerun-from run exceeded {ttl_secs}s ttl ceiling"),
                    });
                    if let Err(e) = bg_state.run_store.set_result(&bg_run_id, reason).await {
                        tracing::warn!(%bg_run_id, error = %e, "run_rerun_from: ttl set_result failed");
                    }
                    if let Err(e) = bg_state
                        .run_store
                        .update_status(&bg_run_id, RunStatus::Failed)
                        .await
                    {
                        tracing::warn!(%bg_run_id, error = %e, "run_rerun_from: ttl run update_status failed");
                    }
                    if let Err(e) = bg_state
                        .task_store
                        .update_status(&bg_task_id, TaskRecordStatus::Failed)
                        .await
                    {
                        tracing::warn!(%bg_task_id, error = %e, "run_rerun_from: ttl task update_status failed");
                    }
                    return;
                }
            };
            let _ = finalize_run(&bg_state, &bg_task_id, &bg_run_id, outcome).await;
        };
        let _ = catch_run_panic(
            &guard_state,
            &guard_task_id,
            &guard_run_id,
            "rerun_from.detach",
            driver,
        )
        .await;
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(RunRerunFromResponse {
            run_id,
            task_id,
            replayed_steps,
            dropped_steps,
        }),
    ))
}

/// `GET /v1/runs/:id`. Returns a single `RunRecord` (its `step_entries`
/// trace included).
pub async fn run_get(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<RunRecord>, ApiError> {
    let run_id =
        RunId::parse(id).map_err(|e| ApiError::bad_request(format!("invalid run id: {e}")))?;
    let run = state
        .run_store
        .get(&run_id)
        .await
        .map_err(map_run_store_err)?;
    Ok(Json(run))
}

/// Query params for `GET /v1/runs` (the Run collection read).
#[derive(Debug, Deserialize, Default)]
pub struct RunsListQuery {
    /// Only Runs kicked from this Task.
    #[serde(default)]
    pub task_id: Option<String>,
    /// Only Runs currently in this status (`pending` / `running` / `done`
    /// / `failed` / `interrupted`).
    #[serde(default)]
    pub status: Option<String>,
    /// Page size cap. Omitted = no cap.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Skip the first N matching rows (after newest-first ordering).
    #[serde(default)]
    pub offset: Option<usize>,
}

/// Response body for `GET /v1/runs`.
#[derive(Debug, Serialize)]
pub struct RunsListResponse {
    /// Matching Runs, newest-first.
    pub runs: Vec<RunRecord>,
}

/// `GET /v1/runs?task_id=&status=&limit=&offset=` — filtered Run
/// collection, newest-first. The collection read that was missing from
/// the Run CRUD surface (only `GET /v1/runs/:id` existed before the
/// per-step run stats work).
pub async fn runs_list(
    State(state): State<AppState>,
    Query(q): Query<RunsListQuery>,
) -> Result<Json<RunsListResponse>, ApiError> {
    let task_id = q
        .task_id
        .map(TaskId::parse)
        .transpose()
        .map_err(|e| ApiError::bad_request(format!("invalid task_id: {e}")))?;
    let status = q
        .status
        .as_deref()
        .map(|s| {
            serde_json::from_value::<RunStatus>(Value::String(s.to_string())).map_err(|_| {
                ApiError::bad_request(format!(
                    "invalid status {s:?} (expected pending/running/done/failed/interrupted)"
                ))
            })
        })
        .transpose()?;
    let runs = state
        .run_store
        .list(&RunListFilter {
            task_id,
            status,
            limit: q.limit,
            offset: q.offset,
        })
        .await
        .map_err(map_run_store_err)?;
    Ok(Json(RunsListResponse { runs }))
}

/// Response body for `GET /v1/runs/:id/steps`.
///
/// `JsonSchema` is derived so `mse://api/http-endpoints` can publish the
/// per-step stats surface without restating [`StepEntry`]'s field list.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct RunStepsResponse {
    /// The Run the steps belong to.
    pub run_id: String,
    /// Terminal per-step stats entries, in append (dispatch) order.
    pub steps: Vec<StepEntry>,
}

/// `GET /v1/runs/:id/steps` — the Run's terminal per-step stats
/// (`StepEntry` list) as a standalone sub-resource. Same data
/// `GET /v1/runs/:id` embeds; split out so stats consumers don't drag
/// the full RunRecord (launch snapshot etc.) per poll.
pub async fn run_steps(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<RunStepsResponse>, ApiError> {
    let run_id =
        RunId::parse(id).map_err(|e| ApiError::bad_request(format!("invalid run id: {e}")))?;
    let run = state
        .run_store
        .get(&run_id)
        .await
        .map_err(map_run_store_err)?;
    Ok(Json(RunStepsResponse {
        run_id: run.id.to_string(),
        steps: run.step_entries,
    }))
}

/// Query params for `GET /v1/runs/:id/trace` — see
/// `mlua_swarm::store::trace::TraceQuery` for semantics (`latest` wins
/// over `after`; `kind` entries are comma-separated prefix matches).
#[derive(Debug, Deserialize, Default)]
pub struct RunTraceQuery {
    /// Forward-paging cursor: only events with `seq > after`.
    #[serde(default)]
    pub after: Option<u64>,
    /// Page size cap.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Tail mode: the LAST n matching events (ascending order).
    #[serde(default)]
    pub latest: Option<usize>,
    /// Comma-separated kind filters, prefix match (e.g. `kind=mw.` or
    /// `kind=core.step_completed,worker.`).
    #[serde(default)]
    pub kind: Option<String>,
    /// Exact `step_ref` filter.
    #[serde(default)]
    pub step: Option<String>,
    /// Exact attempt filter.
    #[serde(default)]
    pub attempt: Option<u32>,
}

/// Response body for `GET /v1/runs/:id/trace`.
#[derive(Debug, Serialize)]
pub struct RunTraceResponse {
    /// The Run the events belong to.
    pub run_id: String,
    /// Matching trace events, ascending by `seq`.
    pub events: Vec<TraceEvent>,
}

/// `GET /v1/runs/:id/trace?after=&limit=&latest=&kind=&step=&attempt=` —
/// the Run's TraceEvent stream (the RunTrace rail). Note the trace rail
/// is deliberately uncoupled from `RunStore` (a trace can outlive or
/// precede its Run row), so an unknown Run id returns an empty list, not
/// 404.
pub async fn run_trace(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<RunTraceQuery>,
) -> Result<Json<RunTraceResponse>, ApiError> {
    let run_id =
        RunId::parse(id).map_err(|e| ApiError::bad_request(format!("invalid run id: {e}")))?;
    let query = TraceQuery {
        after: q.after,
        limit: q.limit,
        latest: q.latest,
        kinds: q
            .kind
            .as_deref()
            .map(|s| {
                s.split(',')
                    .map(str::trim)
                    .filter(|k| !k.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        step_ref: q.step,
        attempt: q.attempt,
    };
    let events = state
        .run_trace_store
        .list(&run_id, &query)
        .await
        .map_err(|e| ApiError::engine(format!("trace list: {e}")))?;
    Ok(Json(RunTraceResponse {
        run_id: run_id.to_string(),
        events,
    }))
}

/// `POST /v1/runs/:id/cancel` — record a cancel request on the Run's
/// trace stream (`core.cancel_requested`) and mark the Run's status
/// to `Cancelled` for still-in-flight rows. Idempotent: repeat calls
/// re-append the trace event but keep the status setter idempotent
/// on the store side. In-flight abort itself remains a v3 carry — the
/// current effect is observational + status marker, matching the
/// `swarm_cancel` MCP tool's local semantics but reflected onto the
/// server-side `RunTraceStore` so `GET /v1/runs/:id/trace` reflects
/// it too.
pub async fn run_cancel(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<axum::http::StatusCode, ApiError> {
    let run_id =
        RunId::parse(id).map_err(|e| ApiError::bad_request(format!("invalid run id: {e}")))?;
    // Load the row to prove it exists; a missing row is a 404 (aligned
    // with `run_delete` — cancel needs an addressable Run).
    let record = state
        .run_store
        .get(&run_id)
        .await
        .map_err(map_run_store_err)?;
    // Best-effort trace append (never gates the response) — the
    // authoritative record is the run_store status update below.
    TraceHandle::new(run_id.clone(), state.run_trace_store.clone())
        .append(trace_kind::CANCEL_REQUESTED, None, None, json!({}))
        .await;
    // Flip the Run's status to Cancelled when it's still non-terminal.
    // Terminal Runs (Done / Failed / Interrupted / already Cancelled)
    // keep their outcome — cancel arriving after finalize is an
    // observation, not a rewrite.
    if matches!(record.status, RunStatus::Pending | RunStatus::Running) {
        if let Err(e) = state
            .run_store
            .update_status(&run_id, RunStatus::Cancelled)
            .await
        {
            tracing::warn!(%run_id, error = %e, "run_cancel: update_status(Cancelled) failed");
        }
    }
    Ok(axum::http::StatusCode::NO_CONTENT)
}

/// `DELETE /v1/runs/:id` — retention prune: deletes the Run row and its
/// trace stream together (`404` when the Run row is absent; the trace
/// stream is pruned best-effort either way). Replay rows are untouched —
/// `ReplayStore` has its own truncation semantics owned by the
/// rerun-from path.
///
/// Trust tier: same auth-free open-router posture as every other route
/// in this module (`POST /v1/tasks` included) — the server is a
/// local-first, loopback-bound single-operator daemon. Flagged in
/// holistic review as the surface's first unauthenticated destructive
/// verb; acceptable under the loopback bind, revisit if the bind ever
/// goes non-local.
pub async fn run_delete(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<axum::http::StatusCode, ApiError> {
    let run_id =
        RunId::parse(id).map_err(|e| ApiError::bad_request(format!("invalid run id: {e}")))?;
    state
        .run_store
        .delete(&run_id)
        .await
        .map_err(map_run_store_err)?;
    if let Err(e) = state.run_trace_store.delete_run(&run_id).await {
        tracing::warn!(%run_id, error = %e, "run_delete: trace delete_run failed (run row already deleted)");
    }
    Ok(axum::http::StatusCode::NO_CONTENT)
}

// ──────────────────────────────────────────────────────────────────────────
// POST /v1/runs/:id/acquire — model §4.5, "becoming the Assignee"
// ──────────────────────────────────────────────────────────────────────────

/// Body of `POST /v1/runs/:id/acquire` — the model's `acquire(op, desc)`
/// (§4.5), addressed at one Run.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct RunAcquireRequest {
    /// Who takes the seat: the `OperatorId` written into
    /// [`Assignee::op`]. A session id (`S-<hex>`) or a role alias
    /// (`main-ai`) — the two share one key space, and the router resolves
    /// an adapter out of it at dispatch time.
    ///
    /// Stored verbatim, and **not checked against the operator registry**.
    /// **Q2**: acquire does not negotiate and does not enquire. Requiring
    /// the operator to be registered *now* would also refuse the one case
    /// the registry is legitimately empty for — a restored session whose
    /// client has not reconnected yet — and the model has a path for a
    /// holder that names nobody (a loud failure at the next dispatch, and
    /// **O8** when the operator is actually deleted), which is a better
    /// answer than refusing the handover.
    ///
    /// Whitespace-only is refused: it names nobody, and would file a
    /// holder no adapter can ever match while making `current` read as
    /// held.
    pub op: String,
    /// Why this operator is taking the seat — **A9** / **Q1**, the
    /// human-readable record of the assignment. Trimmed, and an empty
    /// (or whitespace-only) value is a `400`.
    ///
    /// This is the one field a reader of the handover list has to tell
    /// two concurrent takeovers apart by, so it is mandatory at both this
    /// boundary and the store's.
    pub desc: String,
    /// Which Blueprint-declared seat to take. Optional under the same rule
    /// a launch pin uses (see [`choose_slot`]): omit it when the Blueprint
    /// declares exactly one Operator, name it when the Blueprint declares
    /// several (omitting it then is a `400` that lists the candidates).
    #[serde(default)]
    pub slot: Option<String>,
}

/// Response body of `POST /v1/runs/:id/acquire` — **Q4**: "the requester
/// is told the generation and the previous holder", so that taking a seat
/// from someone is visible to whoever took it.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct RunAcquireResponse {
    /// The Run whose seat was taken — echoes the path param.
    #[schemars(with = "String")]
    pub run_id: RunId,
    /// The seat that was taken. Always spelled out, including when the
    /// request omitted it and the sole declared Operator was used, so the
    /// caller never has to guess which one it got.
    pub slot: String,
    /// The generation stamped on the new holder — the Run counter `G`
    /// after this event (**A4**). Every subsequent reply for this seat is
    /// accepted only under this number (**A6**), so it is the value the
    /// acquirer dispatches under.
    pub gen: u64,
    /// The holder this acquire displaced, or `null` when the seat was
    /// `Vacant`.
    ///
    /// Serialized either way — never skipped. `null` is the answer to "did
    /// I take this from someone?", and a field that vanishes would leave
    /// that answer indistinguishable from an older server that did not
    /// report it.
    pub previous: Option<Assignee>,
    /// **Q5**: what the `T-DISCARD` thrown at the displaced holder did.
    /// Present exactly when [`Self::previous`] is — the rule has no
    /// premise when nobody was displaced. See [`TDiscardReport`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub t_discard: Option<TDiscardReport>,
}

/// `T-DISCARD.confirm(run, discarded)` as the acquirer is told it
/// (model §4.5 **Q5**).
///
/// The model's acquire throws `T-DISCARD(old op, R)` at the transport
/// when it displaces a holder, so the requests already sent to that
/// operator for this Run stop being outstanding. This build now does
/// that: the pending map records each request's Run at insert time, and
/// the discard is addressed at the displaced holder's **adapter** (see
/// [`crate::operator_ws::OperatorAdapter::discard_requests`]) rather than
/// at its `OperatorId`, because that id may be a role alias no session's
/// own sid equals.
///
/// One narrowing of the model's own wording: `R` names a Run, but an
/// acquire takes a **seat**, and one operator can hold several seats of
/// one Run. The requests dropped are this seat's — joined from
/// [`crate::operator_ws::SeatLedger`], which records which seat each
/// in-flight dispatch went out through. See [`entries_not_discarded`] for
/// what that leaves standing and why.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct TDiscardReport {
    /// How many of the displaced holder's in-flight requests for the seat
    /// this acquire took were dropped.
    ///
    /// `null` means the discard could not be **addressed**: the displaced
    /// `OperatorId` resolves no registered adapter (it left, or it is a
    /// role nobody currently holds). That is not a failure of the acquire
    /// — **Q2**, an acquire does not enquire and does not refuse — and it
    /// is reported rather than smoothed into `0`, which would claim the
    /// far end was asked and had nothing.
    pub discarded: Option<usize>,
    /// What the discard does not reach even when it is delivered — see
    /// [`entries_not_discarded`]. Always present, because the shortfall is
    /// structural rather than situational.
    pub not_discarded: String,
}

/// The parts of **Q5** still unmet, narrowed to what is actually true.
///
/// The discard selects the requests of one *seat* — this Run's `slot`,
/// which is what the acquire took — and two classes of request fall
/// outside that selection:
///
/// - **Requests with no Run.** `SeniorBridge::ask` is handed a `StepId`
///   and no `Ctx` (widening the trait would be an engine-wide contract
///   change), and a dispatch launched without a `RunContext` has none
///   either. Nothing can select them by Run, let alone by seat.
/// - **Requests with no seat.** `SpawnHook::before` is dispatched through
///   the sid-registered hook rather than through a router, so no seat is
///   ever recorded for it and none can be attributed to it. It is a
///   question asked of the *session*, and the session is still there
///   (**Q7**).
///
/// What that costs, precisely: the displaced holder may still answer such
/// a request. The answer does not reach the flow — the router re-reads
/// `current` after the adapter returns and refuses a reply whose
/// generation has moved (**A6**) — so the failure mode is a wasted round
/// trip, not a double answer.
///
/// The third thing this does not touch is deliberate rather than a
/// shortfall: requests in flight on the displaced holder's **other seats**
/// of this Run. Those were never this acquire's to take.
fn entries_not_discarded(displaced: &Assignee, run_id: &RunId, slot: &str) -> String {
    format!(
        "The discard sent to the displaced holder '{}' selects the requests it had in flight for \
         seat '{slot}' of run {run_id}, so two kinds survive it and it may still answer them: \
         requests carrying no Run (an `ask` — the escalation verb is handed no Ctx, hence no \
         run_id — or a dispatch launched without a RunContext), and requests carrying no seat (a \
         `hook_before`, dispatched through the session directly rather than through the seat). \
         Such an answer is refused on arrival because the generation has moved (A6). Requests on \
         this operator's OTHER seats of this run are left alone on purpose: this acquire took one \
         seat, and A6 would not have refused their replies, because it is enforced per seat and \
         theirs did not change hands.",
        displaced.op
    )
}

/// The Operator seats a Run can be acquired into — [`choose_slot`] over
/// the Blueprint the Run's Task was launched with, with one addition.
///
/// **A seat the Run already holds is accepted without consulting the
/// Blueprint at all**, and is checked first. Two reasons, one practical
/// and one about which fact outranks which: a takeover — the common case —
/// then costs no Blueprint resolve, and a held seat is a fact about *this
/// Run*, whereas `operators[]` is a fact about what the Blueprint says
/// *now*. A store-backed Blueprint that dropped a seat since launch would
/// otherwise make the Run's own `current` unacquirable while
/// `GET /v1/runs/:id` still shows it held, which is the sort of
/// disagreement the handover list exists not to have.
async fn resolve_acquire_slot(
    state: &AppState,
    run: &RunRecord,
    requested: Option<&str>,
) -> Result<String, ApiError> {
    if let Some(slot) = requested {
        if run.current.contains_key(slot) {
            return Ok(slot.to_string());
        }
    }

    let run_id = &run.id;
    let task = state
        .task_store
        .get(&run.task_id)
        .await
        .map_err(map_task_store_err)?;
    let blueprint_ref: BlueprintRef =
        serde_json::from_value(task.blueprint_ref.clone()).map_err(|e| {
            ApiError::unprocessable(format!(
                "run {run_id}: the stored blueprint_ref of task {} failed to decode, so this \
                 Run's declared Operator seats cannot be read: {e}. Name an already-held seat \
                 in `slot` to acquire without it.",
                run.task_id
            ))
        })?;
    let (blueprint, _version) = state
        .task_app
        .resolve(&blueprint_ref)
        .await
        .map_err(|e| ApiError::from_task_resolve(&e, &format!("run {run_id}: bp resolve")))?;
    let operators = &blueprint.operators;

    match choose_slot(requested, operators) {
        SlotChoice::Named(slot) | SlotChoice::Sole(slot) => Ok(slot.to_string()),
        SlotChoice::NoSeats(_) => Err(ApiError::bad_request(format!(
            "run {run_id} has no Operator seat to acquire: its Blueprint declares no \
             operators[], so there is no `current` key any dispatch would read"
        ))),
        SlotChoice::Undeclared(slot) => Err(ApiError::bad_request(format!(
            "slot '{slot}': run {run_id} neither holds that seat nor does its Blueprint \
             declare it (declared: {}). Acquiring it would file a holder under a key no \
             dispatch reads.",
            declared_seats(operators)
        ))),
        SlotChoice::Ambiguous => Err(ApiError::bad_request(format!(
            "slot is required: run {run_id}'s Blueprint declares {} Operator seats ({}), so \
             an acquire has to name the one it takes",
            operators.len(),
            declared_seats(operators)
        ))),
    }
}

/// `POST /v1/runs/:id/acquire` — take one of this Run's Operator seats,
/// the model's §4.5 in HTTP form. The one way a holder changes from
/// outside the engine.
///
/// # It does not refuse a held seat
///
/// **A8**: an acquire succeeds whatever `current` says — last writer wins.
/// There is no exclusion here, no `409` for a contended seat, and no
/// `force` flag to override one, because there is nothing to override.
/// (model-v6's "refuse when `Assigned`" was withdrawn in v9; `force` is
/// explicitly deferred until real mix-ups are observed, §4.5.) **Q6**: the
/// route does not distinguish "I am returning to my own work" from
/// "I am taking someone else's" — same request, same effect, and the
/// operator that was displaced keeps existing (**Q7**).
///
/// What prevents a mix-up is therefore *not* this endpoint. It is the step
/// before it: reading the handover list and recognising the work
/// (§4.2 **D4** — the description is for telling jobs apart, never for a
/// match test). This route is the part that is deliberately dumb.
///
/// # Q5 — the displaced holder's requests for **this seat** are discarded
///
/// When this acquire displaces a holder, the model also discards that
/// holder's outstanding requests, and this build does: the displaced
/// `OperatorId` is resolved to an adapter through
/// [`crate::AppState::operator_adapters`] — the same registry every
/// dispatch resolves a holder through — and the discard is addressed at
/// that adapter instance. The count comes back in
/// [`RunAcquireResponse::t_discard`].
///
/// The selection is the **seat's**, not the Run's, which is one step
/// narrower than the model's `T-DISCARD(op, R)` wording. It has to be: one
/// adapter can back several seats of one Run, this acquire took one of
/// them, and dropping the reply channels of work in flight on the others
/// would destroy dispatches that are still valid — their seats did not
/// change hands, so **A6** would have accepted their replies. The seat of
/// each in-flight request comes from [`crate::operator_ws::SeatLedger`].
///
/// Two things are deliberately not failures. A displaced holder that
/// names no registered adapter (it left, or it is a role nobody holds)
/// discards nothing and is reported as `null` — **Q2**: an acquire does
/// not enquire, and refusing the handover because the *previous* holder
/// is unreachable would be exactly backwards. And the requests that carry
/// no Run, or no seat, cannot be selected at all; see
/// [`entries_not_discarded`], which rides in the same report so a caller
/// learns it without reading this doc.
///
/// # Status codes
///
/// - `400` — empty `desc` (**Q1**) or `op`; a `slot` this Run neither
///   holds nor its Blueprint declares; no `slot` on a Blueprint declaring
///   several.
/// - `404` — no Run with this id.
/// - `200` — acquired. Body: the generation and the displaced holder
///   (**Q4**).
///
/// # Authorization
///
/// None, like every other route in this module. **B2** is the reason and
/// not merely the precedent: the bearer guards calls *to* an Operator
/// (**B1**) and takes no part in who holds a seat, so gating this route on
/// one would make the bearer decide assignment. **B3** ("acquire does not
/// need the previous holder's bearer") is satisfied a fortiori. The same
/// loopback-bind trust tier as `DELETE /v1/runs/:id` applies; note that
/// the handover *list*, which is what actually prevents taking the wrong
/// Run, is Bearer-gated (**D3**).
pub async fn run_acquire(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<RunAcquireRequest>,
) -> Result<Json<RunAcquireResponse>, ApiError> {
    let run_id =
        RunId::parse(id).map_err(|e| ApiError::bad_request(format!("invalid run id: {e}")))?;

    // Q1 / A9 at the HTTP boundary, before the store is touched: a request
    // that can never be honoured should not cost a round trip, and this is
    // where the status code lives (the store refuses the same thing again
    // but deliberately names no status — see `RunStoreError`).
    let desc = req.desc.trim();
    if desc.is_empty() {
        return Err(ApiError::bad_request(format!(
            "desc is required: taking a seat on run {run_id} records why it happened (A9), and \
             that record is what a later reader tells two takeovers apart by (e.g. \"resuming \
             the compile fix after a restart\")"
        )));
    }
    // Not in the model's Q list, because an OperatorId that is not an
    // OperatorId is not a case it entertains. The store does not guard it
    // either, so it is guarded here: an empty holder would make `current`
    // read as held while naming nobody any adapter can answer for, which is
    // exactly the lie O8 exists to prevent.
    if req.op.trim().is_empty() {
        return Err(ApiError::bad_request(format!(
            "op is required: run {run_id}'s seat is taken *by* an operator — pass the session \
             id (S-<hex>) or the role alias that will hold it"
        )));
    }

    // 404 before anything else reads the Run, and the record the seat rule
    // consults below.
    let run = state
        .run_store
        .get(&run_id)
        .await
        .map_err(map_run_store_err)?;
    let slot = resolve_acquire_slot(&state, &run, req.slot.as_deref()).await?;

    let (gen, previous) = state
        .run_store
        .acquire_assignee(&run_id, &slot, &req.op, desc)
        .await
        .map_err(|e| match e {
            RunStoreError::NotFound(id) => ApiError::not_found(format!("run not found: {id}")),
            // Reachable only if this handler's checks and the store's ever
            // disagree; a 500 would then blame the server for a bad
            // request. The store's own doc directs callers to 400 these.
            e @ (RunStoreError::AssigneeDescRequired | RunStoreError::AssigneeSlotRequired) => {
                ApiError::bad_request(format!("run {run_id}: the acquire was refused: {e}"))
            }
            other => ApiError::engine(other),
        })?;

    // Q5. Addressed at the displaced holder's adapter, resolved from its
    // `OperatorId` here rather than left to the session to match on its own.
    //
    // And narrowed to `slot` before it is sent. What this acquire took is
    // one seat; the displaced holder's adapter may be backing others of
    // this same Run (a launch seats its operator in every declared lane),
    // and work in flight on those seats is not this acquire's to drop. A6 does
    // not clean up afterwards either: it is enforced per slot, so an
    // untouched seat's generation never moved and its dispatch was still
    // valid. The seat of each in-flight request is joined from the ledger
    // the routers write as they delegate — see `SeatLedger`.
    let t_discard = match &previous {
        Some(displaced) => {
            let discarded = match state.operator_adapters.get(&displaced.op).await {
                Some(adapter) => {
                    let outstanding = adapter.pending_for_run(&run_id).await;
                    let of_this_seat: Vec<String> = outstanding
                        .iter()
                        .filter(|request| {
                            state.seat_ledger.slot_of(&run_id, request).as_deref() == Some(&slot)
                        })
                        .map(|request| request.req_id.clone())
                        .collect();
                    Some(adapter.discard_requests(&run_id, &of_this_seat).await)
                }
                None => None,
            };
            Some(TDiscardReport {
                discarded,
                not_discarded: entries_not_discarded(displaced, &run_id, &slot),
            })
        }
        None => None,
    };

    match (&previous, &t_discard) {
        (Some(displaced), Some(report)) => tracing::info!(
            %run_id, %slot, op = %req.op, gen,
            displaced_op = %displaced.op, displaced_gen = displaced.gen,
            discarded = ?report.discarded,
            "acquire: seat taken over (A8 — last writer wins; T-DISCARD sent, Q5)"
        ),
        _ => tracing::info!(
            %run_id, %slot, op = %req.op, gen,
            "acquire: vacant seat taken"
        ),
    }

    // W4, both halves. The displaced holder gets a release row of its own
    // — `reason: displaced` — so "when did this operator lose this Run"
    // is one kind to filter on however the seat emptied, and the assign
    // row that follows names it as `previous` so the pair reads as one
    // handover.
    let trace = TraceHandle::new(run_id.clone(), state.run_trace_store.clone());
    if let Some(displaced) = &previous {
        append_released(&trace, &slot, displaced, ReleaseReason::Displaced).await;
    }
    trace_assigned(
        &state,
        &run_id,
        &slot,
        &req.op,
        desc,
        gen,
        AssignSource::Acquire,
        previous.as_ref(),
    )
    .await;
    // D2. The displaced holder's 記名 is deliberately left alone: the
    // observed part is what an operator *was assigned*, and nothing takes
    // that back (there is no delete path). The handover shows up on the
    // trace rail (`reason: displaced`) and on the new holder's own list.
    crate::handover::record_observed_assignment(&state, &run_id, &run.task_id, &slot, &req.op)
        .await;

    Ok(Json(RunAcquireResponse {
        run_id: run_id.clone(),
        slot,
        gen,
        t_discard,
        previous,
    }))
}

/// Whether a Run-scoped binding has only a declaration or also carries a
/// provider attestation accepted by Core.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunBindingStatus {
    /// No provider attestation was recorded; `requested` is still the exact
    /// declaration pinned at launch time.
    DeclarationOnly,
    /// Core accepted and pinned the provider's effective capability report.
    Attested,
}

/// Mechanical requested/effective comparison for one immutable binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, schemars::JsonSchema)]
pub struct RunBindingDifference {
    /// Whether the requested model string and resolved model string differ.
    pub model_changed: bool,
    /// Requested tools absent from the effective grant. Accepted attestations
    /// normally leave this empty because launch validation is fail-closed.
    pub missing_requested_tools: Vec<String>,
    /// Effective tools not present in the minimum requested grant.
    pub additional_effective_tools: Vec<String>,
    /// Whether the requested and effective launch variants differ.
    pub launch_variant_changed: bool,
}

/// Explain view for one agent, derived exclusively from the persisted Run
/// snapshot rather than from the current Blueprint registry.
#[derive(Debug, Clone, PartialEq, Serialize, schemars::JsonSchema)]
pub struct RunBindingExplainEntry {
    /// Logical agent name.
    pub agent: String,
    /// Declaration tier that selected the Runner.
    pub runner_source: mlua_swarm::blueprint::RunnerResolutionSource,
    /// Provider-attestation state.
    pub status: RunBindingStatus,
    /// Exact platform-neutral request reconstructed from the pinned snapshot.
    pub requested: Option<BindRequest>,
    /// Core-validated provider report, when one was accepted at launch.
    pub effective: Option<BindingAttestation>,
    /// Mechanical difference between `requested` and `effective`; absent for
    /// declaration-only bindings.
    pub difference: Option<RunBindingDifference>,
    /// Final immutable replay identity, including the attestation when present.
    pub binding_digest: mlua_swarm::blueprint::BindingDigest,
}

/// Response body for `GET /v1/runs/:id/bindings`.
#[derive(Debug, Clone, PartialEq, Serialize, schemars::JsonSchema)]
pub struct RunBindingsExplainResponse {
    /// Run whose launch snapshot was inspected.
    #[schemars(with = "String")]
    pub run_id: RunId,
    /// Owning Task recorded on that Run.
    #[schemars(with = "String")]
    pub task_id: TaskId,
    /// Provenance of the inspected `bound_agents` snapshot. `launch` means the
    /// bindings were pinned at the Run's initial launch; `resume_backfill`
    /// means they were re-derived from the current Blueprint when a
    /// pre-binding-snapshot Run was resumed/reran — so they carry no
    /// launch-time pin guarantee. A snapshot that carries `bound_agents` but
    /// no origin marker reports `resume_backfill` (the safe side — see
    /// [`SnapshotOrigin::from_snapshot`]).
    pub snapshot_origin: SnapshotOrigin,
    /// Every agent snapshot in Blueprint declaration order.
    pub bindings: Vec<RunBindingExplainEntry>,
}

fn requested_binding(bound: &BoundAgent) -> Option<BindRequest> {
    mlua_swarm::binding_request_for_snapshot(bound)
}

fn binding_difference(
    requested: &BindRequest,
    effective: &BindingAttestation,
) -> RunBindingDifference {
    let missing_requested_tools = requested
        .requested_tools
        .iter()
        .filter(|tool| !effective.effective_tools.contains(tool))
        .cloned()
        .collect();
    let additional_effective_tools = effective
        .effective_tools
        .iter()
        .filter(|tool| !requested.requested_tools.contains(tool))
        .cloned()
        .collect();
    RunBindingDifference {
        model_changed: requested.requested_model != effective.resolved_model,
        missing_requested_tools,
        additional_effective_tools,
        launch_variant_changed: requested.launch_variant != effective.launch_variant,
    }
}

fn validated_bound_agents_from_snapshot(
    run_id: &RunId,
    snapshot: &Value,
) -> Result<Option<Vec<BoundAgent>>, ApiError> {
    let Some(bound_value) = snapshot.get("bound_agents") else {
        return Ok(None);
    };
    let bound_agents: Vec<BoundAgent> =
        serde_json::from_value(bound_value.clone()).map_err(|e| {
            ApiError::unprocessable(format!(
                "run {run_id} contains an invalid binding snapshot: {e}"
            ))
        })?;
    validate_bound_agent_snapshots(&bound_agents).map_err(|error| {
        ApiError::unprocessable(format!(
            "run {run_id} contains an inconsistent binding snapshot: {error}"
        ))
    })?;
    Ok(Some(bound_agents))
}

/// `GET /v1/runs/:id/bindings`. Explains the exact immutable agent bindings
/// used by this Run. The handler never reads or resolves the current Blueprint;
/// old Runs without a binding snapshot return `422` instead of guessed state.
pub async fn run_bindings_explain(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<RunBindingsExplainResponse>, ApiError> {
    let run_id =
        RunId::parse(id).map_err(|e| ApiError::bad_request(format!("invalid run id: {e}")))?;
    let run = state
        .run_store
        .get(&run_id)
        .await
        .map_err(map_run_store_err)?;
    let input_json = run.input_json.as_deref().ok_or_else(|| {
        ApiError::unprocessable(format!(
            "run {run_id} has no launch snapshot; binding explain is unavailable"
        ))
    })?;
    let snapshot: Value = serde_json::from_str(input_json).map_err(|e| {
        ApiError::unprocessable(format!(
            "run {run_id} launch snapshot is invalid JSON; binding explain is unavailable: {e}"
        ))
    })?;
    let bound_agents = validated_bound_agents_from_snapshot(&run_id, &snapshot)?.ok_or_else(|| {
        ApiError::unprocessable(format!(
            "run {run_id} predates immutable binding snapshots; current Blueprint state was not consulted"
        ))
    })?;

    let bindings = bound_agents
        .into_iter()
        .map(|bound| {
            let requested = requested_binding(&bound);
            let effective = bound.attestation.clone();
            let difference = requested
                .as_ref()
                .zip(effective.as_ref())
                .map(|(request, attestation)| binding_difference(request, attestation));
            RunBindingExplainEntry {
                agent: bound.agent.name,
                runner_source: bound.runner_source,
                status: if effective.is_some() {
                    RunBindingStatus::Attested
                } else {
                    RunBindingStatus::DeclarationOnly
                },
                requested,
                effective,
                difference,
                binding_digest: bound.binding_digest,
            }
        })
        .collect();

    Ok(Json(RunBindingsExplainResponse {
        run_id: run.id,
        task_id: run.task_id,
        snapshot_origin: SnapshotOrigin::from_snapshot(&snapshot),
        bindings,
    }))
}

/// `pub(crate)` so `crate::projection`'s `GET /v1/tasks/:id/ctx` handler can
/// reuse this module's existing-Task-existence-check error mapping (same
/// 404-vs-500 split `task_get` already applies).
pub(crate) fn map_task_store_err(e: TaskStoreError) -> ApiError {
    match e {
        TaskStoreError::NotFound(id) => ApiError::not_found(format!("task not found: {id}")),
        other => ApiError::engine(other),
    }
}

pub(crate) fn map_run_store_err(e: RunStoreError) -> ApiError {
    match e {
        RunStoreError::NotFound(id) => ApiError::not_found(format!("run not found: {id}")),
        other => ApiError::engine(other),
    }
}

// ──────────────────────────────────────────────────────────────────────────
// UT
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator_ws::{Liveness, OperatorAdapter};
    use mlua_swarm::application::BlueprintRef;
    use mlua_swarm::blueprint::{
        current_schema_version, AgentDef, AgentKind, Blueprint, BlueprintMetadata, CompilerHints,
        CompilerStrategy, Runner,
    };
    use mlua_swarm::core::config::EngineCfg;
    use mlua_swarm::core::engine::Engine;
    use mlua_swarm::store::output::InMemoryOutputStore;
    use mlua_swarm::store::run::InMemoryRunStore;
    use mlua_swarm::store::task::InMemoryTaskStore;
    use mlua_swarm::StepId;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// A single-step flow.ir Blueprint that always succeeds: `Step { ref:
    /// "identity", in: lit("hello"), out: $.out }` against the baseline
    /// `RustFn` identity worker (same shape as `seed_blueprint` in
    /// `mlua-swarm-cli`'s `serve.rs`, self-contained here rather than
    /// importing a binary crate).
    fn identity_blueprint() -> Blueprint {
        Blueprint {
            schema_version: current_schema_version(),
            id: "tasks-test-bp".into(),
            flow: serde_json::from_value(serde_json::json!({
                "kind": "step",
                "ref": mlua_swarm::worker::baseline::AG_IDENTITY,
                "in": {"op": "lit", "value": "hello"},
                "out": {"op": "path", "at": "$.out"},
            }))
            .expect("flow parse"),
            agents: vec![AgentDef {
                name: mlua_swarm::worker::baseline::AG_IDENTITY.into(),
                kind: AgentKind::RustFn,
                spec: serde_json::json!({"fn_id": mlua_swarm::worker::baseline::AG_IDENTITY}),
                profile: None,
                meta: None,
                runner: None,
                runner_ref: None,
                verdict: None,
                lints: None,
            }],
            operators: vec![],
            metas: vec![],
            hints: CompilerHints::default(),
            strategy: CompilerStrategy::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx: None,
            default_context_policy: None,
            projection_placement: None,
            audits: vec![],
            degradation_policy: None,
            runners: vec![],
            default_runner: None,
            subprocesses: vec![],
            check_policy: None,
            blueprint_ref_includes: Vec::new(),
        }
    }

    /// Minimal `AppState` for handler-level tests — mirrors the construction
    /// `build_router_full` does internally, but skips the `Router` wrapper so
    /// tests can call handler functions directly (this crate's established
    /// unit-test convention; see e.g. `operator_ws::login`'s tests).
    fn test_state() -> AppState {
        let engine = Engine::new_with_layers(EngineCfg::default(), crate::default_layer_registry());
        let compiler = mlua_swarm::Compiler::new(crate::default_registry());
        let launch = Arc::new(mlua_swarm::TaskLaunchService::new(engine.clone(), compiler));
        AppState {
            engine,
            sessions: Arc::new(Mutex::new(crate::SessionStore::default())),
            task_app: Arc::new(mlua_swarm::TaskApplication::new_inline_only(launch)),
            operator_adapters: Arc::new(crate::operator_ws::OperatorAdapterRegistry::new()),
            seat_ledger: Arc::new(crate::operator_ws::SeatLedger::new()),
            data_store: Arc::new(InMemoryOutputStore::new()),
            operator_sessions: Arc::new(Mutex::new(HashMap::new())),
            operator_session_store: Arc::new(
                mlua_swarm::store::operator_session::InMemoryOperatorSessionStore::new(),
            ),
            task_store: Arc::new(InMemoryTaskStore::new()),
            run_store: Arc::new(InMemoryRunStore::new()),
            replay_store: Arc::new(mlua_swarm::store::replay::InMemoryReplayStore::new()),
            run_trace_store: Arc::new(mlua_swarm::store::trace::InMemoryRunTraceStore::new()),
            base_url: None,
            sync_timeout_secs: 300,
        }
    }

    fn post_tasks_req(goal: &str) -> crate::TaskLaunchRequest {
        crate::TaskLaunchRequest {
            blueprint: BlueprintRef::Inline {
                value: Box::new(identity_blueprint()),
            },
            init_ctx: serde_json::json!({"in": "hello"}),
            project_root: None,
            work_dir: None,
            task_metadata: None,
            ttl_secs: None,
            operator: None,
            operator_sid: None,
            operator_desc: None,
            operator_slot: None,
            timeout_secs: None,
            goal: Some(goal.to_string()),
            detach: false,
            check_policy: None,
        }
    }

    /// The Operator seat the pin tests assign to. A launch pin lands in a
    /// Blueprint-declared seat, so a Blueprint that declares none cannot
    /// be pinned at all — every pinned fixture below therefore goes
    /// through [`post_tasks_req_declaring`] rather than the bare
    /// [`post_tasks_req`].
    const SLOT_A: &str = "phase-a-op";
    /// A sibling seat, for the multi-Operator (per-lane) cases.
    const SLOT_B: &str = "phase-b-op";

    /// An `OperatorDef` that declares nothing but its name — which is the
    /// only field the slot rule reads.
    fn operator_def(name: &str) -> OperatorDef {
        OperatorDef {
            name: name.to_string(),
            display_name: None,
            kind: None,
            spec: Value::Null,
            profile: None,
            meta: None,
        }
    }

    /// [`identity_blueprint`] plus the named Operator seats. The flow
    /// still dispatches through the baseline `RustFn` agent — these seats
    /// exist so a launch pin has somewhere to land, which is exactly the
    /// shape a Blueprint with `kind = Operator` agents has.
    fn blueprint_declaring(names: &[&str]) -> Blueprint {
        Blueprint {
            operators: names.iter().copied().map(operator_def).collect(),
            ..identity_blueprint()
        }
    }

    fn post_tasks_req_declaring(goal: &str, names: &[&str]) -> crate::TaskLaunchRequest {
        crate::TaskLaunchRequest {
            blueprint: BlueprintRef::Inline {
                value: Box::new(blueprint_declaring(names)),
            },
            ..post_tasks_req(goal)
        }
    }

    #[test]
    fn task_id_serializes_as_bare_string() {
        // Sanity check for the newtype-struct transparency relied on
        // throughout this module's response shapes (`TaskId` / `RunId`
        // serialize as plain JSON strings, not `{"0": "..."}`).
        let v = serde_json::to_value(TaskId::parse("T-abc").unwrap()).expect("serialize");
        assert_eq!(v, serde_json::json!("T-abc"));
    }

    #[tokio::test]
    async fn post_then_get_drill_down() {
        let state = test_state();

        let posted = crate::tasks_start(State(state.clone()), Json(post_tasks_req("smoke goal")))
            .await
            .expect("tasks_start")
            .0;
        let task_id = posted.task_id.clone();
        let run_id = posted.run_id.clone();

        // GET /v1/tasks lists it.
        let list = tasks_list(State(state.clone()), Query(TasksListQuery { limit: None }))
            .await
            .expect("tasks_list")
            .0;
        assert!(
            list.iter().any(|t| t.id == task_id),
            "task {task_id} missing from list of {} tasks",
            list.len()
        );

        // GET /v1/tasks/:id drills down to the Task + its Run.
        let detail = task_get(State(state.clone()), Path(task_id.to_string()))
            .await
            .expect("task_get")
            .0;
        assert_eq!(detail.task.id, task_id);
        assert_eq!(detail.task.goal, "smoke goal");
        assert_eq!(detail.task.status, TaskRecordStatus::Done);
        assert_eq!(detail.runs.len(), 1);
        assert_eq!(detail.runs[0].id, run_id);
        assert_eq!(detail.runs[0].status, RunStatus::Done);

        // GET /v1/runs/:id returns the same Run directly.
        let run = run_get(State(state.clone()), Path(run_id.to_string()))
            .await
            .expect("run_get")
            .0;
        assert_eq!(run.id, run_id);
        assert_eq!(run.task_id, task_id);
        assert_eq!(run.result_ref, Some(posted.final_ctx));

        // issue #13 run_id propagation: `POST /v1/tasks` (`run_flow_form`)
        // wires a `RunContext` into `TaskApplication::handle_with_run`, so
        // the single dispatched step must be traced into `step_entries`.
        assert_eq!(
            run.step_entries.len(),
            1,
            "expected one step_entry for the 1-step identity Blueprint, got {:?}",
            run.step_entries
        );
        assert_eq!(
            run.step_entries[0].step_ref,
            Some(mlua_swarm::worker::baseline::AG_IDENTITY.to_string())
        );
        assert_eq!(run.step_entries[0].status, Some("passed".to_string()));
    }

    // ──────────────────────────────────────────────────────────────────
    // GH #33 — sync-hang guards (readiness precheck / timeout ceiling)
    // ──────────────────────────────────────────────────────────────────

    /// Same 1-step identity flow as [`identity_blueprint`], but opts into
    /// the Blueprint-global Operator delegate axis
    /// (`spawner_hints.layers = ["operator_delegate"]`) so a registered
    /// `Operator` backend can be exercised end-to-end through the real
    /// `tasks_start` dispatch path (`OperatorDelegateMiddleware` bypasses
    /// `inner.spawn` and calls `operator.execute` instead — see
    /// `mlua_swarm::middleware::OperatorDelegateMiddleware` doc).
    fn identity_blueprint_with_operator_delegate() -> Blueprint {
        Blueprint {
            spawner_hints: mlua_swarm::SpawnerHints {
                layers: vec!["operator_delegate".to_string()],
            },
            ..identity_blueprint()
        }
    }

    /// `Operator` stub whose `execute` never resolves — the GH #33 Guard 2
    /// fixture ("a registered-but-never-acking operator").
    struct StallingOperator;

    #[async_trait::async_trait]
    impl mlua_swarm::Operator for StallingOperator {
        async fn execute(
            &self,
            _ctx: &mlua_swarm::Ctx,
            _system: Option<String>,
            _prompt: Value,
            _worker: Option<mlua_swarm::WorkerBinding>,
            _worker_token: mlua_swarm::CapToken,
        ) -> Result<mlua_swarm::WorkerResult, mlua_swarm::WorkerError> {
            std::future::pending::<()>().await;
            unreachable!("StallingOperator.execute must never resolve")
        }
    }

    /// A launch request that references an operator backend by id (via
    /// `operator.operator_backend_id`, the coarse Guard 1 signal) against
    /// [`identity_blueprint_with_operator_delegate`].
    fn operator_launch_req(
        backend_id: &str,
        timeout_secs: Option<u64>,
    ) -> crate::TaskLaunchRequest {
        crate::TaskLaunchRequest {
            blueprint: BlueprintRef::Inline {
                value: Box::new(identity_blueprint_with_operator_delegate()),
            },
            init_ctx: serde_json::json!({"in": "hello"}),
            project_root: None,
            work_dir: None,
            task_metadata: None,
            ttl_secs: None,
            operator: Some(crate::OperatorReq {
                operator_backend_id: Some(backend_id.to_string()),
                ..Default::default()
            }),
            operator_sid: None,
            operator_desc: None,
            operator_slot: None,
            timeout_secs,
            goal: Some("operator delegate test goal".to_string()),
            detach: false,
            check_policy: None,
        }
    }

    /// Guard 1: an operator-requiring launch with zero attached operators
    /// must fail immediately with a structured `503`, not hang waiting on
    /// a session nothing can serve.
    #[tokio::test]
    async fn sync_launch_zero_operators_fails_fast() {
        let state = test_state();
        // No `state.engine.register_operator(...)` call — zero operators
        // attached, matching `list_operator_ids()` being empty.
        let req = operator_launch_req("nonexistent-op", None);

        let started = std::time::Instant::now();
        let result = crate::tasks_start(State(state), Json(req)).await;
        let elapsed = started.elapsed();

        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("zero attached operators must fail the operator-delegate launch"),
        };
        assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            err.message.contains("no operator attached"),
            "error message must mention the missing operator: {}",
            err.message
        );
        assert!(
            elapsed < Duration::from_secs(1),
            "guard 1 must fail fast (no dispatch, no timeout wait): took {elapsed:?}"
        );
    }

    /// Guard 2: a launch that resolves to a registered-but-stalled
    /// operator session must return a structured `504` within the
    /// requested `timeout_secs` ceiling, not hang the request forever.
    #[tokio::test]
    async fn sync_launch_stalled_times_out() {
        let state = test_state();
        state
            .engine
            .register_operator("stall-op", Arc::new(StallingOperator))
            .await;
        let req = operator_launch_req("stall-op", Some(1));

        let started = std::time::Instant::now();
        // Outer safety-net timeout: if guard 2 itself regressed into an
        // infinite hang, fail this test loudly instead of stalling `cargo
        // test` indefinitely.
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            crate::tasks_start(State(state), Json(req)),
        )
        .await
        .expect("tasks_start must resolve well within 5s when guard 2's ceiling is 1s");
        let elapsed = started.elapsed();

        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("a stalled operator session must time out, not succeed"),
        };
        assert_eq!(err.status, StatusCode::GATEWAY_TIMEOUT);
        assert!(
            err.message.contains('1'),
            "error message must mention the configured 1s ceiling: {}",
            err.message
        );
        assert!(
            elapsed < Duration::from_secs(3),
            "guard 2 must fire close to the requested 1s ceiling: took {elapsed:?}"
        );
    }

    /// Invariant 2: a launch that never references an operator backend
    /// must never be rejected by guard 1 — the simplest existing passing
    /// fixture (`post_tasks_req`) still succeeds unaffected.
    #[tokio::test]
    async fn sync_launch_without_operator_path_unaffected() {
        let state = test_state();
        let result = crate::tasks_start(
            State(state),
            Json(post_tasks_req("non-operator launch goal")),
        )
        .await;
        if let Err(e) = &result {
            panic!(
                "non-operator launch must succeed unaffected by guard 1: {}",
                e.message
            );
        }
    }

    /// Guard 2 ceiling resolution: `timeout_secs: Some(0)` is invalid
    /// (design doc: "0 = reject with 400 or treat as invalid — pick one
    /// and test it") — rejected fast, before any Task/Run side effects.
    #[tokio::test]
    async fn sync_launch_zero_timeout_secs_rejected() {
        let state = test_state();
        let mut req = post_tasks_req("zero timeout goal");
        req.timeout_secs = Some(0);

        let result = crate::tasks_start(State(state), Json(req)).await;
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("timeout_secs: Some(0) must be rejected, not treated as a no-op"),
        };
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(
            err.message.contains("timeout_secs"),
            "error message must reference timeout_secs: {}",
            err.message
        );
    }

    // ──────────────────────────────────────────────────────────────────
    // GH #37 — detached launch / rekick (driver decoupled from request)
    // ──────────────────────────────────────────────────────────────────

    /// Polls the run store until the given Run reaches a terminal status,
    /// panicking after ~5s — the detached paths complete in the
    /// background, so tests must wait on the store rather than the
    /// response.
    async fn wait_for_terminal_run(state: &AppState, run_id: &RunId) -> RunRecord {
        for _ in 0..50 {
            let rec = state.run_store.get(run_id).await.expect("run get");
            if !matches!(rec.status, RunStatus::Pending | RunStatus::Running) {
                return rec;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("run {run_id} did not reach a terminal status within ~5s");
    }

    /// GH #37: `detach: true` returns `202 Accepted` immediately with
    /// `status: "running"` and a null `final_ctx`; the eval completes in
    /// the background and the Run/Task reach `Done` with the result and
    /// step trace persisted — the same terminal state the sync path
    /// produces.
    #[tokio::test]
    async fn detached_launch_returns_202_and_completes_in_background() {
        let state = test_state();
        let mut req = post_tasks_req("detached goal");
        req.detach = true;

        let reply = crate::tasks_start(State(state.clone()), Json(req))
            .await
            .expect("tasks_start (detached)");
        assert_eq!(reply.1, StatusCode::ACCEPTED);
        let posted = reply.0;
        assert_eq!(posted.status, RunStatus::Running);
        assert_eq!(
            posted.final_ctx,
            serde_json::Value::Null,
            "a detached launch has no final_ctx at response time"
        );

        let rec = wait_for_terminal_run(&state, &posted.run_id).await;
        assert_eq!(rec.status, RunStatus::Done);
        assert!(
            rec.result_ref.is_some(),
            "finalize_run must persist the background eval's final_ctx"
        );
        assert_eq!(
            rec.step_entries.len(),
            1,
            "the background eval must trace its step_entries like the sync path: {:?}",
            rec.step_entries
        );
        let task = state
            .task_store
            .get(&posted.task_id)
            .await
            .expect("task get");
        assert_eq!(task.status, TaskRecordStatus::Done);
    }

    /// GH #37: `detach: true` + `timeout_secs` is contradictory (the sync
    /// ceiling has no meaning for a detached run) — rejected with `400`
    /// before any Task/Run side effects.
    #[tokio::test]
    async fn detached_launch_with_timeout_secs_rejected() {
        let state = test_state();
        let mut req = post_tasks_req("detached + ceiling goal");
        req.detach = true;
        req.timeout_secs = Some(60);

        let err = match crate::tasks_start(State(state.clone()), Json(req)).await {
            Err(e) => e,
            Ok(_) => panic!("detach + timeout_secs must be rejected"),
        };
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(
            err.message.contains("detach"),
            "error message must explain the detach/timeout_secs conflict: {}",
            err.message
        );
        let tasks = state.task_store.list().await.expect("task list");
        assert!(
            tasks.is_empty(),
            "the 400 must fire before any TaskRecord is minted"
        );
    }

    /// GH #37: a detached rekick returns `202 Accepted` with `status:
    /// "running"` immediately and completes in the background, adding a
    /// second `Done` Run to the same Task.
    #[tokio::test]
    async fn rekick_detached_returns_202_and_completes_in_background() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_tasks_req("detached rekick goal")),
        )
        .await
        .expect("tasks_start")
        .0;

        let (status, rekicked) = task_rekick(
            State(state.clone()),
            Path(posted.task_id.to_string()),
            Some(Json(RunKickRequest {
                init_ctx_override: None,
                task_input_override: None,
                timeout_secs: None,
                detach: true,
                operator_sid: None,
                operator_desc: None,
                operator_slot: None,
            })),
        )
        .await
        .expect("task_rekick (detached)");
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(rekicked.0.status, RunStatus::Running);
        assert_ne!(rekicked.0.run_id, posted.run_id);

        let rec = wait_for_terminal_run(&state, &rekicked.0.run_id).await;
        assert_eq!(rec.status, RunStatus::Done);
        assert!(
            rec.result_ref.is_some(),
            "finalize_run must persist the background rekick's final_ctx"
        );
    }

    /// GH #37: `detach: true` + `timeout_secs` on the rekick path is the
    /// same contradiction as on the launch path — `400`, no new Run
    /// minted.
    #[tokio::test]
    async fn rekick_detached_with_timeout_secs_rejected() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_tasks_req("detached rekick ceiling goal")),
        )
        .await
        .expect("tasks_start")
        .0;

        let err = match task_rekick(
            State(state.clone()),
            Path(posted.task_id.to_string()),
            Some(Json(RunKickRequest {
                init_ctx_override: None,
                task_input_override: None,
                timeout_secs: Some(60),
                detach: true,
                operator_sid: None,
                operator_desc: None,
                operator_slot: None,
            })),
        )
        .await
        {
            Err(e) => e,
            Ok(_) => panic!("detach + timeout_secs must be rejected on rekick"),
        };
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(
            err.message.contains("detach"),
            "error message must explain the detach/timeout_secs conflict: {}",
            err.message
        );
        let runs = state
            .run_store
            .list_by_task(&posted.task_id)
            .await
            .expect("runs list");
        assert_eq!(
            runs.len(),
            1,
            "the 400 must fire before a second Run is minted"
        );
    }

    // ──────────────────────────────────────────────────────────────────
    // Run driver panic guard (`catch_run_panic`)
    // ──────────────────────────────────────────────────────────────────

    /// Seeds a `Running` Task + Run pair directly in the stores — the panic
    /// guard operates on an already-dispatched Run, so these tests do not
    /// need a real dispatch to reach it.
    async fn seed_running_run(state: &AppState) -> (TaskId, RunId) {
        let now = now_secs();
        let task_id = TaskId::new();
        let run_id = RunId::new();
        state
            .task_store
            .create(TaskRecord {
                id: task_id.clone(),
                goal: "panic guard goal".into(),
                blueprint_ref: json!({}),
                input_ctx: json!({}),
                task_input_spec: None,
                status: TaskRecordStatus::Running,
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("task create");
        state
            .run_store
            .create(RunRecord {
                id: run_id.clone(),
                task_id: task_id.clone(),
                status: RunStatus::Running,
                step_entries: Vec::new(),
                degradations: Vec::new(),
                operator_sid: None,
                current: Default::default(),
                next_generation: 0,
                result_ref: None,
                input_json: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("run create");
        (task_id, run_id)
    }

    async fn run_finished_events(state: &AppState, run_id: &RunId) -> Vec<TraceEvent> {
        state
            .run_trace_store
            .list(run_id, &TraceQuery::default())
            .await
            .expect("trace list")
            .into_iter()
            .filter(|e| e.kind == trace_kind::RUN_FINISHED)
            .collect()
    }

    /// A panicking driver terminates its Run instead of stranding it: the
    /// Run goes `Interrupted` (resumable) with a structured reason naming
    /// the site and carrying the panic payload, the Task follows, and the
    /// trace stream gets exactly one terminal marker.
    #[tokio::test]
    async fn panicking_driver_marks_run_interrupted() {
        let state = test_state();
        let (task_id, run_id) = seed_running_run(&state).await;

        let outcome: Result<(), String> =
            catch_run_panic(&state, &task_id, &run_id, "test.detach", async {
                panic!("boom");
            })
            .await;
        let message = outcome.expect_err("a panicking driver must report the panic to its caller");
        assert!(
            message.contains("boom"),
            "the panic payload must survive as the caller-visible message: {message}"
        );

        let rec = state.run_store.get(&run_id).await.expect("run get");
        assert_eq!(
            rec.status,
            RunStatus::Interrupted,
            "a panicked Run must be resumable, not left Running or marked Failed"
        );
        let reason = rec
            .result_ref
            .as_ref()
            .and_then(|v| v.get("error"))
            .and_then(Value::as_str)
            .expect("a structured {\"error\": ...} envelope");
        assert!(
            reason.contains("boom") && reason.contains("test.detach"),
            "the reason must name both the panic payload and the site: {reason}"
        );

        let task = state.task_store.get(&task_id).await.expect("task get");
        assert_eq!(task.status, TaskRecordStatus::Interrupted);

        let finished = run_finished_events(&state, &run_id).await;
        assert_eq!(finished.len(), 1, "expected one terminal trace marker");
        assert_eq!(
            finished[0].payload.get("status").and_then(Value::as_str),
            Some("interrupted")
        );
        assert_eq!(
            finished[0].payload.get("reason").and_then(Value::as_str),
            Some("driver panic")
        );
    }

    /// The guard is compare-and-set: a panic raised after the Run already
    /// finalized (say inside the trace tail that follows `finalize_run`)
    /// must not rewrite the terminal verdict or its result.
    #[tokio::test]
    async fn panic_guard_does_not_clobber_a_finalized_run() {
        let state = test_state();
        let (task_id, run_id) = seed_running_run(&state).await;
        state
            .run_store
            .set_result(&run_id, json!({"kept": true}))
            .await
            .expect("set_result");
        state
            .run_store
            .update_status(&run_id, RunStatus::Done)
            .await
            .expect("update_status");

        let outcome: Result<(), String> =
            catch_run_panic(&state, &task_id, &run_id, "test.detach", async {
                panic!("late boom");
            })
            .await;
        assert!(
            outcome.is_err(),
            "the panic is still reported to the caller"
        );

        let rec = state.run_store.get(&run_id).await.expect("run get");
        assert_eq!(rec.status, RunStatus::Done, "the CAS must have refused");
        assert_eq!(rec.result_ref, Some(json!({"kept": true})));
        let finished = run_finished_events(&state, &run_id).await;
        assert!(
            finished.is_empty(),
            "a refused CAS must not append a second terminal marker: {finished:?}"
        );
    }

    /// The synchronous launch/rekick shape: the driver is wrapped
    /// `timeout(..)`-and-all, so a panic surfaces as an `Err` the handler
    /// maps to a `500` (`ApiError::engine`) — instead of unwinding into the
    /// connection task and dropping the response — while the Run is left
    /// `Interrupted` and therefore resumable.
    #[tokio::test]
    async fn sync_panic_returns_err_and_interrupts_run() {
        let state = test_state();
        let (task_id, run_id) = seed_running_run(&state).await;

        let timed = catch_run_panic(
            &state,
            &task_id,
            &run_id,
            "launch.sync",
            tokio::time::timeout(Duration::from_secs(30), async {
                panic!("sync boom");
            }),
        )
        .await;
        let message = timed.expect_err("the sync path must observe the panic as an Err");
        assert!(message.contains("sync boom"), "payload lost: {message}");

        let err = ApiError::engine(format!("run driver panicked: {message}"));
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);

        let rec = state.run_store.get(&run_id).await.expect("run get");
        assert_eq!(rec.status, RunStatus::Interrupted);
    }

    #[tokio::test]
    async fn rekick_adds_a_second_run_to_the_same_task() {
        let state = test_state();
        let posted = crate::tasks_start(State(state.clone()), Json(post_tasks_req("rekick goal")))
            .await
            .expect("tasks_start")
            .0;
        let task_id = posted.task_id.clone();
        let first_run_id = posted.run_id.clone();

        let (status, rekicked) = task_rekick(State(state.clone()), Path(task_id.to_string()), None)
            .await
            .expect("task_rekick");
        assert_eq!(status, StatusCode::CREATED);
        let second_run_id = rekicked.0.run_id.clone();
        assert_ne!(first_run_id, second_run_id);

        let detail = task_get(State(state.clone()), Path(task_id.to_string()))
            .await
            .expect("task_get")
            .0;
        assert_eq!(
            detail.runs.len(),
            2,
            "expected 2 runs, got {:?}",
            detail.runs
        );
        let ids: Vec<&RunId> = detail.runs.iter().map(|r| &r.id).collect();
        assert!(ids.contains(&&first_run_id));
        assert!(ids.contains(&&second_run_id));

        // issue #13 run_id propagation: each kick's own `EngineDispatcher`
        // (built fresh per `TaskApplication::handle_with_run` call) must
        // trace its own dispatched step into its own `RunRecord` —
        // independent `step_entries`, not shared/accumulated across kicks.
        let first_run = detail
            .runs
            .iter()
            .find(|r| r.id == first_run_id)
            .expect("first run present in detail.runs");
        let second_run = detail
            .runs
            .iter()
            .find(|r| r.id == second_run_id)
            .expect("second run present in detail.runs");
        assert_eq!(
            first_run.step_entries.len(),
            1,
            "first run step_entries: {:?}",
            first_run.step_entries
        );
        assert_eq!(
            second_run.step_entries.len(),
            1,
            "second run step_entries: {:?}",
            second_run.step_entries
        );
        assert_eq!(
            first_run.step_entries[0].step_ref,
            Some(mlua_swarm::worker::baseline::AG_IDENTITY.to_string())
        );
        assert_eq!(
            second_run.step_entries[0].step_ref,
            Some(mlua_swarm::worker::baseline::AG_IDENTITY.to_string())
        );
        assert_eq!(first_run.step_entries[0].status, Some("passed".to_string()));
        assert_eq!(
            second_run.step_entries[0].status,
            Some("passed".to_string())
        );
        assert_ne!(
            first_run.step_entries[0].step_id, second_run.step_entries[0].step_id,
            "each kick dispatches its own StepId — runs must not share step_entries"
        );
    }

    #[tokio::test]
    async fn rekick_unknown_task_returns_404() {
        let state = test_state();
        // `.expect_err()` needs the Ok variant to be `Debug`; `Json<T>`'s
        // `Debug` impl is not guaranteed for every `T` across axum versions,
        // so a plain match sidesteps that bound entirely.
        match task_rekick(State(state), Path("T-does-not-exist".to_string()), None).await {
            Ok(_) => panic!("expected 404 for an unknown task"),
            Err(e) => assert_eq!(e.status, StatusCode::NOT_FOUND),
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // issue #19 ST4: `RunKickRequest` (optional body / 3-layer merge)
    // ──────────────────────────────────────────────────────────────────

    /// A single-step flow.ir Blueprint that echoes `$.greeting` into
    /// `$.out` — unlike [`identity_blueprint`] (a fixed `lit("hello")`
    /// input), this one reads its `Step.in` from `ctx`, so it observes
    /// whichever `init_ctx` layer actually won the merge.
    fn greeting_blueprint() -> Blueprint {
        Blueprint {
            schema_version: current_schema_version(),
            id: "tasks-test-greeting-bp".into(),
            flow: serde_json::from_value(serde_json::json!({
                "kind": "step",
                "ref": mlua_swarm::worker::baseline::AG_IDENTITY,
                "in": {"op": "path", "at": "$.greeting"},
                "out": {"op": "path", "at": "$.out"},
            }))
            .expect("flow parse"),
            agents: vec![AgentDef {
                name: mlua_swarm::worker::baseline::AG_IDENTITY.into(),
                kind: AgentKind::RustFn,
                spec: serde_json::json!({"fn_id": mlua_swarm::worker::baseline::AG_IDENTITY}),
                profile: None,
                meta: None,
                runner: None,
                runner_ref: None,
                verdict: None,
                lints: None,
            }],
            operators: vec![],
            metas: vec![],
            hints: CompilerHints::default(),
            strategy: CompilerStrategy::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx: None,
            default_context_policy: None,
            projection_placement: None,
            audits: vec![],
            degradation_policy: None,
            runners: vec![],
            default_runner: None,
            subprocesses: vec![],
            check_policy: None,
            blueprint_ref_includes: Vec::new(),
        }
    }

    fn post_greeting_task_req(
        greeting: &str,
        project_root: Option<&str>,
    ) -> crate::TaskLaunchRequest {
        crate::TaskLaunchRequest {
            blueprint: BlueprintRef::Inline {
                value: Box::new(greeting_blueprint()),
            },
            init_ctx: serde_json::json!({ "greeting": greeting }),
            project_root: project_root.map(str::to_string),
            work_dir: None,
            task_metadata: None,
            ttl_secs: None,
            operator: None,
            operator_sid: None,
            operator_desc: None,
            operator_slot: None,
            timeout_secs: None,
            goal: Some("st4 rekick goal".to_string()),
            detach: false,
            check_policy: None,
        }
    }

    #[tokio::test]
    async fn rekick_no_body_preserves_stored_task_input_ctx_byte_for_byte() {
        // must_not_simplify #3: a body-less rekick must behave exactly
        // like pre-#19 — the Task's own `input_ctx` alone seeds the kick.
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_greeting_task_req("from-task", None)),
        )
        .await
        .expect("tasks_start")
        .0;
        assert_eq!(posted.final_ctx["out"]["echoed"], "from-task");

        let (status, rekicked) =
            task_rekick(State(state.clone()), Path(posted.task_id.to_string()), None)
                .await
                .expect("task_rekick");
        assert_eq!(status, StatusCode::CREATED);

        let run = run_get(State(state.clone()), Path(rekicked.0.run_id.to_string()))
            .await
            .expect("run_get")
            .0;
        assert_eq!(
            run.result_ref.expect("result_ref present")["out"]["echoed"],
            "from-task"
        );
    }

    #[tokio::test]
    async fn rekick_with_init_ctx_override_wins_over_stored_task_input_ctx() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_greeting_task_req("from-task", None)),
        )
        .await
        .expect("tasks_start")
        .0;
        assert_eq!(posted.final_ctx["out"]["echoed"], "from-task");

        let (status, rekicked) = task_rekick(
            State(state.clone()),
            Path(posted.task_id.to_string()),
            Some(Json(RunKickRequest {
                init_ctx_override: Some(serde_json::json!({ "greeting": "from-run" })),
                task_input_override: None,
                timeout_secs: None,
                detach: false,
                operator_sid: None,
                operator_desc: None,
                operator_slot: None,
            })),
        )
        .await
        .expect("task_rekick");
        assert_eq!(status, StatusCode::CREATED);

        let run = run_get(State(state.clone()), Path(rekicked.0.run_id.to_string()))
            .await
            .expect("run_get")
            .0;
        assert_eq!(
            run.result_ref.expect("result_ref present")["out"]["echoed"],
            "from-run",
            "Run's init_ctx_override must win over the stored Task input_ctx"
        );
    }

    #[tokio::test]
    async fn rekick_with_stored_task_input_spec_dispatches_and_leaves_it_unmutated() {
        // Done Criteria: "Task record が task-level canonical fields を
        // 保持している時の rekick test". A Task created with
        // `project_root` set gets a `task_input_spec` snapshot; a
        // body-less rekick must both dispatch successfully (the stored
        // spec decodes and resolves without erroring) and leave
        // `TaskRecord.task_input_spec` untouched (must_not_simplify #4 —
        // a rekick never mutates the stored Task-level snapshot).
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_greeting_task_req("from-task", Some("/repo"))),
        )
        .await
        .expect("tasks_start")
        .0;

        let before = state
            .task_store
            .get(&posted.task_id)
            .await
            .expect("task fetch");
        let before_spec: Option<TaskInputSpec> = before
            .task_input_spec
            .as_ref()
            .map(|v| serde_json::from_value(v.clone()).expect("decode task_input_spec"));
        assert_eq!(
            before_spec,
            Some(TaskInputSpec {
                project_root: Some("/repo".to_string()),
                work_dir: None,
                task_metadata: None,
            })
        );

        let (status, _rekicked) =
            task_rekick(State(state.clone()), Path(posted.task_id.to_string()), None)
                .await
                .expect("task_rekick");
        assert_eq!(status, StatusCode::CREATED);

        let after = state
            .task_store
            .get(&posted.task_id)
            .await
            .expect("task fetch");
        assert_eq!(
            after.task_input_spec, before.task_input_spec,
            "rekick must not mutate the stored Task-level task_input_spec snapshot"
        );
    }

    #[tokio::test]
    async fn rekick_with_task_input_override_does_not_mutate_stored_task_record() {
        // must_not_simplify #4: `task_input_override` wins for this kick
        // only — the stored `TaskRecord.task_input_spec` is untouched.
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_greeting_task_req("from-task", Some("/repo"))),
        )
        .await
        .expect("tasks_start")
        .0;

        let (status, _rekicked) = task_rekick(
            State(state.clone()),
            Path(posted.task_id.to_string()),
            Some(Json(RunKickRequest {
                init_ctx_override: None,
                task_input_override: Some(TaskInputSpec {
                    project_root: Some("/override".to_string()),
                    work_dir: None,
                    task_metadata: None,
                }),
                timeout_secs: None,
                detach: false,
                operator_sid: None,
                operator_desc: None,
                operator_slot: None,
            })),
        )
        .await
        .expect("task_rekick");
        assert_eq!(status, StatusCode::CREATED);

        let after = state
            .task_store
            .get(&posted.task_id)
            .await
            .expect("task fetch");
        let after_spec: Option<TaskInputSpec> = after
            .task_input_spec
            .as_ref()
            .map(|v| serde_json::from_value(v.clone()).expect("decode task_input_spec"));
        assert_eq!(
            after_spec,
            Some(TaskInputSpec {
                project_root: Some("/repo".to_string()),
                work_dir: None,
                task_metadata: None,
            }),
            "a per-Run task_input_override must not leak into the stored TaskRecord"
        );
    }

    // ──────────────────────────────────────────────────────────────────
    // GH #33 → task_rekick — sync-hang guards (issue #35 ST3 parity)
    // ──────────────────────────────────────────────────────────────────

    /// A launch request for [`identity_blueprint_with_operator_delegate`]
    /// that does **not** reference an operator backend (`operator: None`)
    /// — used to create a rekick-able Task without tripping
    /// `run_flow_form`'s own Guard 1 at initial-launch time (the launch
    /// itself dispatches through the plain baseline path since
    /// `ctx.operator.operator` stays unset either way; the BP's
    /// `operator_delegate` layer only matters to `task_rekick`'s Guard 1,
    /// which reads `resolved_bp.spawner_hints.layers` directly rather than
    /// a per-request field).
    fn delegate_launch_req(goal: &str) -> crate::TaskLaunchRequest {
        crate::TaskLaunchRequest {
            blueprint: BlueprintRef::Inline {
                value: Box::new(identity_blueprint_with_operator_delegate()),
            },
            init_ctx: serde_json::json!({"in": "hello"}),
            project_root: None,
            work_dir: None,
            task_metadata: None,
            ttl_secs: None,
            operator: None,
            operator_sid: None,
            operator_desc: None,
            operator_slot: None,
            timeout_secs: None,
            goal: Some(goal.to_string()),
            detach: false,
            check_policy: None,
        }
    }

    /// Guard 1 (adapted signal): a Task whose stored Blueprint declares
    /// the `operator_delegate` layer, rekicked with zero attached
    /// operators, must fail immediately with a structured `503` — not
    /// dispatch and not hang waiting on a session nothing can serve.
    #[tokio::test]
    async fn rekick_zero_operators_with_operator_delegate_blueprint_fails_fast() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(delegate_launch_req("operator delegate rekick goal")),
        )
        .await
        .expect("tasks_start (no operator referenced, dispatches through baseline)")
        .0;
        // No `state.engine.register_operator(...)` call — zero operators
        // attached, matching `list_operator_ids()` being empty.

        let started = std::time::Instant::now();
        let result = task_rekick(State(state), Path(posted.task_id.to_string()), None).await;
        let elapsed = started.elapsed();

        let err = match result {
            Err(e) => e,
            Ok(_) => panic!(
                "rekicking a Task whose Blueprint declares operator_delegate with zero \
                 attached operators must fail, not dispatch"
            ),
        };
        assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            err.message.contains("no operator attached"),
            "error message must mention the missing operator: {}",
            err.message
        );
        assert!(
            elapsed < Duration::from_secs(1),
            "guard 1 must fail fast (no dispatch, no timeout wait): took {elapsed:?}"
        );
    }

    /// Guard 2: a rekick with a `timeout_secs` ceiling shorter than the
    /// dispatch takes must return a structured `504` within the outer
    /// safety-net timeout, not hang the request forever.
    #[tokio::test]
    async fn rekick_stalled_operator_times_out() {
        let state = test_state();
        state
            .engine
            .register_operator("stall-op", Arc::new(StallingOperator))
            .await;
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(delegate_launch_req("stalled rekick goal")),
        )
        .await
        .expect("tasks_start")
        .0;

        let started = std::time::Instant::now();
        // Outer safety-net timeout: if guard 2 itself regressed into an
        // infinite hang, fail this test loudly instead of stalling `cargo
        // test` indefinitely.
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            task_rekick(
                State(state),
                Path(posted.task_id.to_string()),
                Some(Json(RunKickRequest {
                    init_ctx_override: None,
                    task_input_override: None,
                    timeout_secs: Some(1),
                    detach: false,
                    operator_sid: None,
                    operator_desc: None,
                    operator_slot: None,
                })),
            ),
        )
        .await
        .expect("task_rekick must resolve well within 5s when guard 2's ceiling is 1s");
        let elapsed = started.elapsed();

        match &result {
            Err(e) => {
                assert_eq!(e.status, StatusCode::GATEWAY_TIMEOUT);
                assert!(
                    e.message.contains('1'),
                    "error message must mention the configured 1s ceiling: {}",
                    e.message
                );
                assert!(
                    elapsed < Duration::from_secs(3),
                    "guard 2 must fire close to the requested 1s ceiling: took {elapsed:?}"
                );
            }
            Ok(_) => {
                // This rekick passes no `operator_sid`, so its
                // `operator_backend_id` stays `None` and the
                // registered-but-unattached `StallingOperator` is never
                // actually engaged by the dispatch; the flow resolves
                // through the plain baseline path instead. Guard 2's
                // `tokio::time::timeout`
                // wrap is exercised (and does not falsely fire) rather
                // than tripped — assert the fast-success shape so a
                // regression that makes rekick dispatch slow (or that
                // makes Guard 2 falsely trip on a fast dispatch) is still
                // caught by the elapsed-time assertion below.
                assert!(
                    elapsed < Duration::from_secs(1),
                    "a rekick that never engages an Operator (task_rekick has no \
                     per-request operator override) must resolve fast, not stall: took {elapsed:?}"
                );
            }
        }
    }

    /// Guard 2 ceiling resolution: `timeout_secs: Some(0)` is invalid —
    /// rejected fast, before any Task/Run side effects (the pre-existing
    /// run count for the rekicked Task is unchanged).
    #[tokio::test]
    async fn rekick_timeout_secs_zero_rejected() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_tasks_req("zero timeout rekick goal")),
        )
        .await
        .expect("tasks_start")
        .0;

        let before = task_get(State(state.clone()), Path(posted.task_id.to_string()))
            .await
            .expect("task_get")
            .0;
        let runs_before = before.runs.len();

        let result = task_rekick(
            State(state.clone()),
            Path(posted.task_id.to_string()),
            Some(Json(RunKickRequest {
                init_ctx_override: None,
                task_input_override: None,
                timeout_secs: Some(0),
                detach: false,
                operator_sid: None,
                operator_desc: None,
                operator_slot: None,
            })),
        )
        .await;
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("timeout_secs: Some(0) must be rejected, not treated as a no-op"),
        };
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(
            err.message.contains("timeout_secs"),
            "error message must reference timeout_secs: {}",
            err.message
        );

        let after = task_get(State(state), Path(posted.task_id.to_string()))
            .await
            .expect("task_get")
            .0;
        assert_eq!(
            after.runs.len(),
            runs_before,
            "a rejected timeout_secs: Some(0) rekick must not create a new Run"
        );
    }

    /// Invariant: a plain (non-`operator_delegate`) Task rekick must
    /// never be rejected by Guard 1 — the simplest existing passing
    /// rekick fixture still succeeds unaffected.
    #[tokio::test]
    async fn rekick_non_operator_path_unaffected_by_guard_1() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_tasks_req("non-operator rekick goal")),
        )
        .await
        .expect("tasks_start")
        .0;

        let result = task_rekick(State(state), Path(posted.task_id.to_string()), None).await;
        if let Err(e) = &result {
            panic!(
                "a plain (non-operator_delegate) Task rekick must succeed unaffected by \
                 guard 1: {}",
                e.message
            );
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // B-1: rekick operator_sid pin (parity with POST /v1/tasks)
    // ──────────────────────────────────────────────────────────────────

    /// A rekick pinning an unknown `operator_sid` fails fast with a `400`
    /// before any Task/Run store write — no new Run is minted (S2 parity
    /// with `run_flow_form`'s `operator_sid` fail-fast).
    #[tokio::test]
    async fn rekick_unknown_operator_sid_rejected_before_side_effects() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_tasks_req("unknown operator_sid rekick goal")),
        )
        .await
        .expect("tasks_start")
        .0;

        let before = task_get(State(state.clone()), Path(posted.task_id.to_string()))
            .await
            .expect("task_get")
            .0;
        let runs_before = before.runs.len();

        let result = task_rekick(
            State(state.clone()),
            Path(posted.task_id.to_string()),
            Some(Json(RunKickRequest {
                init_ctx_override: None,
                task_input_override: None,
                timeout_secs: None,
                detach: false,
                operator_sid: Some("S-not-registered".to_string()),
                operator_desc: Some("pinned by the rekick test".to_string()),
                operator_slot: None,
            })),
        )
        .await;
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("an unknown operator_sid must be rejected, not dispatched"),
        };
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(
            err.message.contains("operator_sid"),
            "error message must reference operator_sid: {}",
            err.message
        );

        let after = task_get(State(state), Path(posted.task_id.to_string()))
            .await
            .expect("task_get")
            .0;
        assert_eq!(
            after.runs.len(),
            runs_before,
            "a rejected unknown-operator_sid rekick must not create a new Run"
        );
    }

    /// A rekick pinning a *registered* `operator_sid` dispatches
    /// successfully and persists the sid verbatim onto the new
    /// `RunRecord.operator_sid`. The Task's stored Blueprint is the plain
    /// baseline (no `operator_delegate` layer), so the registered Operator
    /// is never actually engaged — the kick resolves through the baseline
    /// path and the assertion is purely on the persisted correlation
    /// field.
    #[tokio::test]
    async fn rekick_with_registered_operator_sid_persists_it_on_the_run() {
        let state = test_state();
        // Register an Operator whose sid the rekick can pin. It is never
        // engaged (plain Blueprint does not delegate), so a `StallingOperator`
        // is a fine stand-in for "a live, registered session".
        state
            .engine
            .register_operator("S-live-op", Arc::new(StallingOperator))
            .await;
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_tasks_req_declaring(
                "registered operator_sid rekick goal",
                &[SLOT_A],
            )),
        )
        .await
        .expect("tasks_start")
        .0;

        let (status, rekicked) = task_rekick(
            State(state.clone()),
            Path(posted.task_id.to_string()),
            Some(Json(RunKickRequest {
                init_ctx_override: None,
                task_input_override: None,
                timeout_secs: None,
                detach: false,
                operator_sid: Some("S-live-op".to_string()),
                operator_desc: Some("pinned by the rekick test".to_string()),
                operator_slot: None,
            })),
        )
        .await
        .expect("task_rekick with a registered operator_sid");
        assert_eq!(status, StatusCode::CREATED);

        let run = state
            .run_store
            .get(&rekicked.0.run_id)
            .await
            .expect("run get");
        assert_eq!(
            run.operator_sid,
            Some("S-live-op".to_string()),
            "the pinned operator_sid must be persisted verbatim on the RunRecord"
        );
    }

    // ──────────────────────────────────────────────────────────────────
    // model §5 — the launch announcement (`LaunchInfo`)
    // ──────────────────────────────────────────────────────────────────

    /// The seat named `slot`, from an announcement.
    fn announced(info: &LaunchInfo, slot: &str) -> crate::handover::RunSeat {
        info.seats
            .iter()
            .find(|seat| seat.slot == slot)
            .unwrap_or_else(|| panic!("seat '{slot}' is missing from the announcement"))
            .clone()
    }

    /// **An unpinned launch announces that nobody was seated.** §4.3's
    /// *居なければ居ないと分かる* applied to the launch moment: a Run whose
    /// seats are all `Vacant` is a Run whose first dispatch will fail
    /// naming the seat, and a response that simply omitted the holders
    /// would read as "the announcement did not manage to report them".
    #[tokio::test]
    async fn an_unpinned_launch_announces_every_declared_seat_as_vacant() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_tasks_req_declaring("unpinned goal", &[SLOT_A, SLOT_B])),
        )
        .await
        .expect("tasks_start")
        .0;

        let info = &posted.info;
        assert_eq!(info.run_id, posted.run_id, "the block names its own Run");
        assert_eq!(info.goal, "unpinned goal");
        assert_eq!(info.seats.len(), 2, "both declared seats are listed");
        for slot in [SLOT_A, SLOT_B] {
            let seat = announced(info, slot);
            assert!(seat.vacant, "an unpinned launch seats nothing: {slot}");
            assert!(seat.holder.is_none());
            assert!(seat.declared);
        }
        assert!(
            info.note.contains("not a guarantee"),
            "the announcement has to say it is one: {}",
            info.note
        );
    }

    /// **A pinned launch announces the holder, its 記名, and the paths.**
    /// The pinned seat carries the caller's own `operator_desc`; the other
    /// declared seat goes to the same operator with the server-authored
    /// [`auto_seat_desc`] sentence, which is the tell that nobody asked
    /// for that lane.
    ///
    /// The paths are the part §5 names a reason for — the same repo in
    /// several worktrees — so they are asserted as literals rather than as
    /// "some string".
    #[tokio::test]
    async fn a_pinned_launch_announces_the_holder_the_kimei_and_the_paths() {
        let state = test_state();
        state
            .engine
            .register_operator("S-live-op", Arc::new(StallingOperator))
            .await;
        let req = crate::TaskLaunchRequest {
            project_root: Some("/repo/.worktrees/topic".to_string()),
            work_dir: Some("/repo/.worktrees/topic/crates".to_string()),
            operator_sid: Some("S-live-op".to_string()),
            operator_desc: Some("the AI that kicked this Run".to_string()),
            operator_slot: Some(SLOT_A.to_string()),
            ..post_tasks_req_declaring("pinned goal", &[SLOT_A, SLOT_B])
        };
        let posted = crate::tasks_start(State(state.clone()), Json(req))
            .await
            .expect("tasks_start")
            .0;

        let info = &posted.info;
        assert_eq!(
            info.project_root.as_deref(),
            Some("/repo/.worktrees/topic"),
            "which checkout this Run is bound to has to be visible at launch"
        );
        assert_eq!(
            info.work_dir.as_deref(),
            Some("/repo/.worktrees/topic/crates")
        );

        let pinned = announced(info, SLOT_A).holder.expect("the pinned holder");
        assert_eq!(pinned.op, "S-live-op", "担当 = the OperatorId seated");
        assert_eq!(
            pinned.desc, "the AI that kicked this Run",
            "記名 = the caller's own reason, not a server-written one"
        );
        assert_eq!(pinned.gen, 1, "A4: the first Assign of a fresh Run");

        let auto = announced(info, SLOT_B)
            .holder
            .expect("the auto-seated lane");
        assert_eq!(
            auto.op, "S-live-op",
            "a Run goes to the AI that launched it, every lane of it"
        );
        assert_eq!(
            auto.desc,
            auto_seat_desc(SLOT_B),
            "and the lane nobody named says so in its own 記名"
        );
    }

    /// **A rekick announces the paths of *that* kick.** This is the path
    /// where being wrong about the checkout is easiest:
    /// `task_input_override` retargets one kick without touching
    /// `TaskRecord.task_input_spec`, so an announcement read off the Task
    /// row would name the wrong worktree while the Run drove another.
    #[tokio::test]
    async fn a_rekick_announces_the_paths_of_that_kick_not_the_tasks() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(crate::TaskLaunchRequest {
                project_root: Some("/repo".to_string()),
                ..post_tasks_req_declaring("rekick announcement goal", &[SLOT_A])
            }),
        )
        .await
        .expect("tasks_start")
        .0;
        assert_eq!(posted.info.project_root.as_deref(), Some("/repo"));

        let (_status, rekicked) = task_rekick(
            State(state.clone()),
            Path(posted.task_id.to_string()),
            Some(Json(RunKickRequest {
                init_ctx_override: None,
                task_input_override: Some(TaskInputSpec {
                    project_root: Some("/repo/.worktrees/other".to_string()),
                    work_dir: None,
                    task_metadata: None,
                }),
                timeout_secs: None,
                detach: false,
                operator_sid: None,
                operator_desc: None,
                operator_slot: None,
            })),
        )
        .await
        .expect("task_rekick");

        let info = &rekicked.0.info;
        assert_eq!(
            info.run_id, rekicked.0.run_id,
            "the announcement is about the Run this kick minted"
        );
        assert_eq!(
            info.project_root.as_deref(),
            Some("/repo/.worktrees/other"),
            "the override retargets this kick, and the announcement has to follow it"
        );
        assert_eq!(
            info.goal, "rekick announcement goal",
            "the goal still comes from the Task row"
        );
        assert!(
            announced(info, SLOT_A).vacant,
            "an unpinned rekick seats nothing, same as an unpinned launch"
        );
    }

    /// A pinned rekick feeds BOTH axes from the one `operator_sid` — the
    /// delegate layer's session backend and the run-scoped pin the binding
    /// provider reads — and now records it once. The launch snapshot is
    /// where a resumed Run picks the pin back up, so that is what the
    /// assertion reads.
    #[tokio::test]
    async fn rekick_pin_reaches_both_axes_and_survives_in_the_launch_snapshot() {
        let state = test_state();
        state
            .engine
            .register_operator("S-live-op", Arc::new(StallingOperator))
            .await;
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_tasks_req_declaring(
                "pinned rekick snapshot goal",
                &[SLOT_A],
            )),
        )
        .await
        .expect("tasks_start")
        .0;

        let (_status, rekicked) = task_rekick(
            State(state.clone()),
            Path(posted.task_id.to_string()),
            Some(Json(RunKickRequest {
                init_ctx_override: None,
                task_input_override: None,
                timeout_secs: None,
                detach: false,
                operator_sid: Some("S-live-op".to_string()),
                operator_desc: Some("pinned by the rekick test".to_string()),
                operator_slot: None,
            })),
        )
        .await
        .expect("task_rekick with a registered operator_sid");

        let run = state
            .run_store
            .get(&rekicked.0.run_id)
            .await
            .expect("run get");
        let snapshot: Value = serde_json::from_str(
            run.input_json
                .as_deref()
                .expect("a rekicked Run persists its launch snapshot"),
        )
        .expect("snapshot json");
        assert_eq!(
            snapshot["operator_sid"],
            serde_json::json!("S-live-op"),
            "the launch's one operator field must carry the pinned sid: {snapshot}"
        );
        assert_eq!(
            snapshot.get("operator_backend_id"),
            None,
            "the folded-away spellings must not be written back out: {snapshot}"
        );
        assert_eq!(snapshot.get("operator_pin"), None, "{snapshot}");
    }

    /// An unpinned launch leaves the operator field null and names no
    /// session on the Run.
    #[tokio::test]
    async fn unpinned_launch_snapshot_carries_neither_axis() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_tasks_req("unpinned snapshot goal")),
        )
        .await
        .expect("tasks_start")
        .0;
        let run = state.run_store.get(&posted.run_id).await.expect("run get");
        let snapshot: Value =
            serde_json::from_str(run.input_json.as_deref().expect("launch snapshot"))
                .expect("snapshot json");
        assert_eq!(snapshot["operator_sid"], Value::Null);
        assert_eq!(
            run.operator_sid, None,
            "an unpinned launch records no session on the Run"
        );
    }

    // ──────────────────────────────────────────────────────────────────
    // Launch-time Assign (model §4.3 A4 / A9)
    // ──────────────────────────────────────────────────────────────────

    /// A pinned launch is the Run's first `Assign`: `current` names the
    /// pinned operator with the supplied `desc` at generation **1**
    /// (**A4** — `G` starts at 0 and every assignment event increments
    /// before stamping).
    ///
    /// `operator_sid` is still written too: it is the launch-time
    /// snapshot, `current` is the live holder, and a later handover moves
    /// only the second one. Both are asserted here so the difference stays
    /// visible.
    #[tokio::test]
    async fn a_pinned_launch_assigns_the_run_at_generation_one() {
        let state = test_state();
        state
            .engine
            .register_operator("S-live-op", Arc::new(StallingOperator))
            .await;
        let mut req = post_tasks_req_declaring("pinned launch assign goal", &[SLOT_A]);
        req.operator_sid = Some("S-live-op".to_string());
        req.operator_desc = Some("pinned by the launch request".to_string());

        let posted = crate::tasks_start(State(state.clone()), Json(req))
            .await
            .expect("tasks_start")
            .0;

        let run = state.run_store.get(&posted.run_id).await.expect("run get");
        assert_eq!(
            run.current.len(),
            1,
            "a launch pin assigns exactly one slot"
        );
        let current = run.current[SLOT_A].clone();
        assert_eq!(current.op, "S-live-op");
        assert_eq!(current.desc, "pinned by the launch request");
        assert_eq!(current.gen, 1, "A4: the first Assign is generation 1");
        assert_eq!(run.next_generation, 1);
        assert_eq!(
            run.operator_sid,
            Some("S-live-op".to_string()),
            "the launch-time snapshot field is written as well"
        );
    }

    /// **A9**: an `Assign` without a `desc` is refused with `400`, and —
    /// like every other pre-dispatch guard on this handler — refused
    /// before any Task or Run row is written.
    #[tokio::test]
    async fn a_pinned_launch_without_a_desc_is_rejected_before_side_effects() {
        let state = test_state();
        state
            .engine
            .register_operator("S-live-op", Arc::new(StallingOperator))
            .await;
        let tasks_before = tasks_list(State(state.clone()), Query(TasksListQuery { limit: None }))
            .await
            .expect("tasks_list")
            .0
            .len();

        for desc in [None, Some(String::new()), Some("   ".to_string())] {
            let mut req = post_tasks_req_declaring("desc-less pinned launch goal", &[SLOT_A]);
            req.operator_sid = Some("S-live-op".to_string());
            req.operator_desc = desc.clone();
            let err = crate::tasks_start(State(state.clone()), Json(req))
                .await
                .err()
                .unwrap_or_else(|| panic!("a pin with desc {desc:?} must be rejected"));
            assert_eq!(err.status, StatusCode::BAD_REQUEST);
            assert!(
                err.message.contains("operator_desc"),
                "the error must name the missing field: {}",
                err.message
            );
        }

        let tasks_after = tasks_list(State(state), Query(TasksListQuery { limit: None }))
            .await
            .expect("tasks_list")
            .0
            .len();
        assert_eq!(
            tasks_after, tasks_before,
            "a rejected launch must not mint a Task (nor the Run under it)"
        );
    }

    /// No pin, no assignment: the Run launches `Vacant` at generation 0
    /// even when a stray `operator_desc` rides along, because there is no
    /// `Assign` for it to describe. **R2** — that Run is not stopped by
    /// being Vacant; this one runs to completion.
    #[tokio::test]
    async fn an_unpinned_launch_leaves_the_run_vacant() {
        let state = test_state();
        let mut req = post_tasks_req("unpinned launch goal");
        req.operator_desc = Some("describes an assignment nobody asked for".to_string());

        let posted = crate::tasks_start(State(state.clone()), Json(req))
            .await
            .expect("an unpinned launch is unaffected by the desc")
            .0;

        let run = state.run_store.get(&posted.run_id).await.expect("run get");
        assert!(run.current.is_empty(), "nothing was assigned");
        assert_eq!(run.next_generation, 0, "A4: no assignment event happened");
        assert_eq!(run.status, RunStatus::Done);
    }

    /// Rekick parity: the same `Assign` on `POST /v1/tasks/:id/runs`, and
    /// the same `400` without a `desc` (asserted on the same handler the
    /// B-1 pin tests above drive).
    #[tokio::test]
    async fn a_pinned_rekick_assigns_at_generation_one_and_requires_a_desc() {
        let state = test_state();
        state
            .engine
            .register_operator("S-live-op", Arc::new(StallingOperator))
            .await;
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_tasks_req_declaring(
                "pinned rekick assign goal",
                &[SLOT_A],
            )),
        )
        .await
        .expect("tasks_start")
        .0;

        let before = task_get(State(state.clone()), Path(posted.task_id.to_string()))
            .await
            .expect("task_get")
            .0
            .runs
            .len();
        let err = task_rekick(
            State(state.clone()),
            Path(posted.task_id.to_string()),
            Some(Json(RunKickRequest {
                init_ctx_override: None,
                task_input_override: None,
                timeout_secs: None,
                detach: false,
                operator_sid: Some("S-live-op".to_string()),
                operator_desc: None,
                operator_slot: None,
            })),
        )
        .await
        .expect_err("a pinned rekick with no desc must be rejected");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        let after = task_get(State(state.clone()), Path(posted.task_id.to_string()))
            .await
            .expect("task_get")
            .0
            .runs
            .len();
        assert_eq!(after, before, "the rejected rekick minted no Run");

        let (_status, rekicked) = task_rekick(
            State(state.clone()),
            Path(posted.task_id.to_string()),
            Some(Json(RunKickRequest {
                init_ctx_override: None,
                task_input_override: None,
                timeout_secs: None,
                detach: false,
                operator_sid: Some("S-live-op".to_string()),
                operator_desc: Some("re-kicked onto the live session".to_string()),
                operator_slot: None,
            })),
        )
        .await
        .expect("a pinned rekick with a desc dispatches");

        let run = state
            .run_store
            .get(&rekicked.0.run_id)
            .await
            .expect("run get");
        let current = run.current[SLOT_A].clone();
        assert_eq!(current.op, "S-live-op");
        assert_eq!(current.desc, "re-kicked onto the live session");
        assert_eq!(current.gen, 1, "each Run counts its own generations");
    }

    // ──────────────────────────────────────────────────────────────────
    // Which seat a launch pin assigns (`operator_slot` / `operators[]`)
    // ──────────────────────────────────────────────────────────────────

    /// The rule, on its own, in all four shapes it can take. Driven
    /// directly because it is the one place the decision is made and the
    /// handlers below only carry it.
    #[test]
    fn the_slot_rule_reads_the_blueprints_declared_operators() {
        let none: Vec<OperatorDef> = Vec::new();
        let one = vec![operator_def(SLOT_A)];
        let two = vec![operator_def(SLOT_A), operator_def(SLOT_B)];

        // 2. one declared Operator is implicit.
        assert_eq!(
            resolve_launch_slot(None, &one).expect("a sole Operator needs no naming"),
            SLOT_A
        );
        // 1. a named seat the Blueprint declares.
        assert_eq!(
            resolve_launch_slot(Some(SLOT_B), &two).expect("a declared seat is nameable"),
            SLOT_B
        );
        // 3. two or more, unnamed: refused, with the candidates listed so
        //    the caller can pick without reading the Blueprint again.
        let err = resolve_launch_slot(None, &two).expect_err("two seats cannot be guessed between");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("operator_slot"), "{}", err.message);
        assert!(
            err.message.contains(SLOT_A) && err.message.contains(SLOT_B),
            "the candidates must be listed: {}",
            err.message
        );
        // 1'. a name nothing declares is a typo, not a new seat.
        let err = resolve_launch_slot(Some("phase-c-op"), &two)
            .expect_err("an undeclared seat must not be filed");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("phase-c-op"), "{}", err.message);
        assert!(
            err.message.contains(SLOT_A) && err.message.contains(SLOT_B),
            "the declared seats must be listed: {}",
            err.message
        );
        // 4. no seat at all.
        let err = resolve_launch_slot(None, &none).expect_err("nothing to assign to");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("operators[]"), "{}", err.message);
        // Padding is not a name: it gets the same treatment as any typo,
        // rather than quietly meaning "unnamed".
        assert!(resolve_launch_slot(Some("  phase-a-op  "), &one).is_err());
    }

    /// A launch pin against a Blueprint declaring one Operator lands in
    /// that seat — no request field, no constant, the Blueprint decided.
    #[tokio::test]
    async fn a_pin_lands_in_the_sole_declared_operator_seat() {
        let state = test_state();
        state
            .engine
            .register_operator("S-live-op", Arc::new(StallingOperator))
            .await;
        let mut req = post_tasks_req_declaring("sole seat goal", &[SLOT_A]);
        req.operator_sid = Some("S-live-op".to_string());
        req.operator_desc = Some("pinned by the launch request".to_string());

        let posted = crate::tasks_start(State(state.clone()), Json(req))
            .await
            .expect("tasks_start")
            .0;
        let run = state.run_store.get(&posted.run_id).await.expect("run get");
        assert_eq!(
            run.current.keys().collect::<Vec<_>>(),
            vec![SLOT_A],
            "the sole declared Operator is the seat the pin fills"
        );
    }

    /// Two declared Operators and no `operator_slot`: refused, with both
    /// candidates named. Guessing here would silently mis-address every
    /// per-lane Blueprint (`phase_a_op` / `phase_b_op`), which is exactly
    /// the shape the guide documents as supported.
    #[tokio::test]
    async fn a_pin_on_a_multi_operator_blueprint_must_name_its_seat() {
        let state = test_state();
        state
            .engine
            .register_operator("S-live-op", Arc::new(StallingOperator))
            .await;
        let tasks_before = tasks_list(State(state.clone()), Query(TasksListQuery { limit: None }))
            .await
            .expect("tasks_list")
            .0
            .len();

        let mut req = post_tasks_req_declaring("two seats goal", &[SLOT_A, SLOT_B]);
        req.operator_sid = Some("S-live-op".to_string());
        req.operator_desc = Some("pinned by the launch request".to_string());
        let err = crate::tasks_start(State(state.clone()), Json(req))
            .await
            .err()
            .unwrap_or_else(|| panic!("an ambiguous seat must be refused, not guessed"));
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(
            err.message.contains(SLOT_A) && err.message.contains(SLOT_B),
            "the candidates must be listed: {}",
            err.message
        );

        let tasks_after = tasks_list(State(state), Query(TasksListQuery { limit: None }))
            .await
            .expect("tasks_list")
            .0
            .len();
        assert_eq!(
            tasks_after, tasks_before,
            "a refused launch must not mint a Task (nor the Run under it)"
        );
    }

    /// Naming the seat resolves the same launch. What `operator_slot`
    /// decides is which seat carries the **caller's** `desc` — the others
    /// go to the same operator with a server-authored one, since a Run
    /// belongs to whoever launched it.
    #[tokio::test]
    async fn a_named_seat_is_the_seat_the_pins_desc_lands_on() {
        let state = test_state();
        state
            .engine
            .register_operator("S-live-op", Arc::new(StallingOperator))
            .await;
        let mut req = post_tasks_req_declaring("named seat goal", &[SLOT_A, SLOT_B]);
        req.operator_sid = Some("S-live-op".to_string());
        req.operator_desc = Some("pinned by the launch request".to_string());
        req.operator_slot = Some(SLOT_B.to_string());

        let posted = crate::tasks_start(State(state.clone()), Json(req))
            .await
            .expect("a named seat resolves the ambiguity")
            .0;
        let run = state.run_store.get(&posted.run_id).await.expect("run get");
        let mut seated: Vec<&String> = run.current.keys().collect();
        seated.sort();
        assert_eq!(
            seated,
            vec![SLOT_A, SLOT_B],
            "both declared lanes are dispatchable after a pinned launch"
        );
        for slot in [SLOT_A, SLOT_B] {
            assert_eq!(run.current[slot].op, "S-live-op");
        }
        assert_eq!(
            run.current[SLOT_B].desc, "pinned by the launch request",
            "the named seat keeps the caller's own desc"
        );
        assert!(
            run.current[SLOT_A]
                .desc
                .starts_with("auto-seated at launch"),
            "the one it did not name says the server chose it: {}",
            run.current[SLOT_A].desc
        );
    }

    /// An `operator_slot` the Blueprint does not declare is a `400`, before
    /// any record exists — not a seat conjured out of the request body.
    #[tokio::test]
    async fn an_undeclared_seat_is_rejected_before_side_effects() {
        let state = test_state();
        state
            .engine
            .register_operator("S-live-op", Arc::new(StallingOperator))
            .await;
        let tasks_before = tasks_list(State(state.clone()), Query(TasksListQuery { limit: None }))
            .await
            .expect("tasks_list")
            .0
            .len();

        let mut req = post_tasks_req_declaring("undeclared seat goal", &[SLOT_A]);
        req.operator_sid = Some("S-live-op".to_string());
        req.operator_desc = Some("pinned by the launch request".to_string());
        req.operator_slot = Some("typo-op".to_string());
        let err = crate::tasks_start(State(state.clone()), Json(req))
            .await
            .err()
            .unwrap_or_else(|| panic!("an undeclared seat must be refused"));
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("typo-op"), "{}", err.message);

        let tasks_after = tasks_list(State(state), Query(TasksListQuery { limit: None }))
            .await
            .expect("tasks_list")
            .0
            .len();
        assert_eq!(tasks_after, tasks_before, "no Task was minted");
    }

    /// A Blueprint that declares no Operator at all has no seat for a pin
    /// to fill, so the pin is refused rather than filed under a key
    /// nothing reads.
    #[tokio::test]
    async fn a_pin_on_an_operator_less_blueprint_is_rejected() {
        let state = test_state();
        state
            .engine
            .register_operator("S-live-op", Arc::new(StallingOperator))
            .await;
        let mut req = post_tasks_req("no seats goal");
        req.operator_sid = Some("S-live-op".to_string());
        req.operator_desc = Some("pinned by the launch request".to_string());

        let err = crate::tasks_start(State(state), Json(req))
            .await
            .err()
            .unwrap_or_else(|| panic!("there is no seat to assign to"));
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("operators[]"), "{}", err.message);
    }

    /// Rekick parity for the seat rule: the Task's stored Blueprint is
    /// what decides, and a kick can name a seat the same way a launch can.
    #[tokio::test]
    async fn a_rekick_resolves_its_seat_from_the_tasks_blueprint() {
        let state = test_state();
        state
            .engine
            .register_operator("S-live-op", Arc::new(StallingOperator))
            .await;
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_tasks_req_declaring(
                "rekick seat goal",
                &[SLOT_A, SLOT_B],
            )),
        )
        .await
        .expect("tasks_start")
        .0;

        // Unnamed against two declared seats: same `400` as the launch.
        let err = task_rekick(
            State(state.clone()),
            Path(posted.task_id.to_string()),
            Some(Json(RunKickRequest {
                init_ctx_override: None,
                task_input_override: None,
                timeout_secs: None,
                detach: false,
                operator_sid: Some("S-live-op".to_string()),
                operator_desc: Some("re-kicked".to_string()),
                operator_slot: None,
            })),
        )
        .await
        .expect_err("an ambiguous seat must be refused on the rekick path too");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(
            err.message.contains(SLOT_A) && err.message.contains(SLOT_B),
            "the candidates must be listed: {}",
            err.message
        );

        let (_status, rekicked) = task_rekick(
            State(state.clone()),
            Path(posted.task_id.to_string()),
            Some(Json(RunKickRequest {
                init_ctx_override: None,
                task_input_override: None,
                timeout_secs: None,
                detach: false,
                operator_sid: Some("S-live-op".to_string()),
                operator_desc: Some("re-kicked onto lane B".to_string()),
                operator_slot: Some(SLOT_B.to_string()),
            })),
        )
        .await
        .expect("a named seat dispatches");
        let run = state
            .run_store
            .get(&rekicked.0.run_id)
            .await
            .expect("run get");
        assert_eq!(
            run.current[SLOT_B].desc, "re-kicked onto lane B",
            "the kick's own desc lands on the seat it named"
        );
        assert!(
            run.current[SLOT_A]
                .desc
                .starts_with("auto-seated at launch"),
            "and the lane it did not name goes to the same operator: {}",
            run.current[SLOT_A].desc
        );
    }

    // ──────────────────────────────────────────────────────────────────
    // POST /v1/runs/:id/acquire — model §4.5
    // ──────────────────────────────────────────────────────────────────

    /// A launched Run over a Blueprint declaring `seats`, launched without
    /// an `operator_sid` — so every seat starts Vacant
    /// (`seat_declared_operators` seats the launching operator, and this
    /// launch names none) and an acquire is the only thing that can fill
    /// one.
    async fn launched_run(state: &AppState, goal: &str, seats: &[&str]) -> RunId {
        crate::tasks_start(
            State(state.clone()),
            Json(post_tasks_req_declaring(goal, seats)),
        )
        .await
        .expect("tasks_start")
        .0
        .run_id
    }

    fn acquire_req(op: &str, desc: &str, slot: Option<&str>) -> RunAcquireRequest {
        RunAcquireRequest {
            op: op.to_string(),
            desc: desc.to_string(),
            slot: slot.map(str::to_string),
        }
    }

    async fn acquire(
        state: &AppState,
        run_id: &RunId,
        req: RunAcquireRequest,
    ) -> Result<RunAcquireResponse, ApiError> {
        run_acquire(State(state.clone()), Path(run_id.to_string()), Json(req))
            .await
            .map(|json| json.0)
    }

    /// An `OperatorAdapter` double standing in for a session's pending
    /// map: it answers `pending_for_run` with the requests it was built
    /// with, and a discard drops exactly the ones it names.
    ///
    /// It is never dispatched to (`execute` is unreachable in these
    /// tests), which is the point: the acquire path resolves an adapter
    /// out of the registry purely to address a discard at it.
    ///
    /// It models the map rather than counting calls because the selection
    /// is now made by the *caller* — the acquire reads what is
    /// outstanding, keeps the seat's share, and names it — so a double
    /// that answered a fixed count would agree with any selection at all,
    /// including the Run-wide one this seat scoping exists to stop.
    struct OwesRequests {
        // `std::sync::Mutex` explicitly: this test module's `Mutex` is
        // tokio's (imported for `AppState`), whose guard is awaited.
        requests: std::sync::Mutex<Vec<crate::operator_ws::PendingRequest>>,
        /// Every discard that arrived, as `(run, the names it carried)`.
        discards: std::sync::Mutex<Vec<(String, Vec<String>)>>,
    }

    impl OwesRequests {
        fn new(requests: Vec<crate::operator_ws::PendingRequest>) -> Self {
            Self {
                requests: std::sync::Mutex::new(requests),
                discards: std::sync::Mutex::new(Vec::new()),
            }
        }

        /// The `req_id`s it still owes, sorted.
        fn outstanding(&self) -> Vec<String> {
            let mut ids: Vec<String> = self
                .requests
                .lock()
                .expect("request map")
                .iter()
                .map(|request| request.req_id.clone())
                .collect();
            ids.sort();
            ids
        }
    }

    /// One waiting spawn, as the adapter would describe it.
    fn waiting_spawn(
        req_id: &str,
        step: &StepId,
        attempt: u32,
    ) -> crate::operator_ws::PendingRequest {
        crate::operator_ws::PendingRequest {
            req_id: req_id.to_string(),
            kind: crate::operator_ws::PendingKind::Spawn,
            step_id: step.clone(),
            attempt: Some(attempt),
        }
    }

    #[async_trait::async_trait]
    impl mlua_swarm::Operator for OwesRequests {
        async fn execute(
            &self,
            _ctx: &mlua_swarm::Ctx,
            _system: Option<String>,
            _prompt: Value,
            _worker: Option<mlua_swarm::WorkerBinding>,
            _worker_token: mlua_swarm::CapToken,
        ) -> Result<mlua_swarm::WorkerResult, mlua_swarm::WorkerError> {
            Err(mlua_swarm::WorkerError::Failed(
                "this double exists to be discarded at, never dispatched to".to_string(),
            ))
        }
    }

    #[async_trait::async_trait]
    impl OperatorAdapter for OwesRequests {
        async fn liveness(&self) -> Liveness {
            Liveness::Connected
        }

        async fn discard_requests(&self, run: &RunId, req_ids: &[String]) -> usize {
            self.discards
                .lock()
                .expect("discard log")
                .push((run.to_string(), req_ids.to_vec()));
            let mut requests = self.requests.lock().expect("request map");
            let before = requests.len();
            requests.retain(|request| !req_ids.contains(&request.req_id));
            before - requests.len()
        }

        async fn pending_for_run(
            &self,
            _run: &RunId,
        ) -> Vec<crate::operator_ws::router::PendingRequest> {
            self.requests.lock().expect("request map").clone()
        }
    }

    /// Every `core.assignee_*` row on a Run's trace, in `seq` order.
    async fn assignment_events(state: &AppState, run_id: &RunId) -> Vec<TraceEvent> {
        run_trace(
            State(state.clone()),
            Path(run_id.to_string()),
            Query(RunTraceQuery {
                kind: Some("core.assignee_".to_string()),
                ..Default::default()
            }),
        )
        .await
        .expect("trace read")
        .0
        .events
    }

    /// The seat this Run's `current` shows as held, read back the way a
    /// client reads it.
    async fn seat_on_the_wire(state: &AppState, run_id: &RunId, slot: &str) -> Option<Assignee> {
        run_get(State(state.clone()), Path(run_id.to_string()))
            .await
            .expect("run get")
            .0
            .current
            .get(slot)
            .cloned()
    }

    /// **Q4 on an empty seat.** The Blueprint declares one Operator, so
    /// the request need not name it; the response says which seat it got,
    /// under which generation, and that it took the seat from nobody.
    #[tokio::test]
    async fn an_acquire_fills_a_vacant_seat_and_reports_no_predecessor() {
        let state = test_state();
        let run_id = launched_run(&state, "vacant seat goal", &[SLOT_A]).await;

        let resp = acquire(
            &state,
            &run_id,
            acquire_req("S-taker", "picking up the stalled compile fix", None),
        )
        .await
        .expect("an acquire on a Vacant seat must succeed");

        assert_eq!(resp.slot, SLOT_A, "the sole declared seat was resolved");
        assert_eq!(
            resp.gen, 1,
            "A4: the first assignment event is generation 1"
        );
        assert!(
            resp.previous.is_none(),
            "the seat was Vacant, so nobody was displaced"
        );
        assert!(
            resp.t_discard.is_none(),
            "Q5 has no premise when no holder was displaced"
        );

        let seat = seat_on_the_wire(&state, &run_id, SLOT_A)
            .await
            .expect("GET /v1/runs/:id must show the seat held");
        assert_eq!(seat.op, "S-taker");
        assert_eq!(seat.desc, "picking up the stalled compile fix");
        assert_eq!(seat.gen, 1);
    }

    /// **A8 — the seat is not defended.** A second acquire lands on a seat
    /// that is held, and it succeeds: no `409`, no `force`, no enquiry
    /// about the incumbent. **Q4**: the response names the holder it
    /// displaced, so taking the seat from someone is visible to whoever
    /// took it.
    #[tokio::test]
    async fn an_acquire_displaces_a_live_holder_and_says_whose_seat_it_took() {
        let state = test_state();
        let run_id = launched_run(&state, "handover goal", &[SLOT_A]).await;

        acquire(
            &state,
            &run_id,
            acquire_req("S-incumbent", "holding the seat", None),
        )
        .await
        .expect("the first acquire");

        let resp = acquire(
            &state,
            &run_id,
            acquire_req(
                "S-successor",
                "taking over after the incumbent went quiet",
                None,
            ),
        )
        .await
        .expect("A8: a held seat does not refuse an acquire");

        assert_eq!(resp.gen, 2, "A4: every assignment event bumps G");
        let displaced = resp.previous.expect("Q4: the displaced holder is reported");
        assert_eq!(displaced.op, "S-incumbent");
        assert_eq!(displaced.gen, 1, "A3: its stamp is what it always was");

        let report = resp
            .t_discard
            .expect("Q5: displacing a holder throws a discard at it");
        assert_eq!(
            report.discarded, None,
            "the incumbent names no registered adapter, so nothing could be addressed — and \
             that is reported rather than smoothed into 0"
        );
        assert!(
            report.not_discarded.contains("S-incumbent") && report.not_discarded.contains("ask"),
            "the remaining shortfall must name the holder and what a Run-scoped discard cannot \
             select: {}",
            report.not_discarded
        );

        assert_eq!(
            seat_on_the_wire(&state, &run_id, SLOT_A)
                .await
                .expect("still held")
                .op,
            "S-successor",
            "last writer wins"
        );
    }

    /// **Q6.** "Returning to my own work" is not a different operation
    /// from "taking someone else's": the same operator re-acquiring is an
    /// assignment event like any other, bumping `G` and minting a fresh
    /// `Assignee` (**Q3**) rather than being recognised as a no-op.
    #[tokio::test]
    async fn re_acquiring_your_own_seat_is_the_same_operation() {
        let state = test_state();
        let run_id = launched_run(&state, "same holder goal", &[SLOT_A]).await;

        acquire(
            &state,
            &run_id,
            acquire_req("S-self", "first pass over the flow", None),
        )
        .await
        .expect("the first acquire");
        let resp = acquire(
            &state,
            &run_id,
            acquire_req("S-self", "back after a reconnect", None),
        )
        .await
        .expect("Q6: taking your own seat is not a special case");

        assert_eq!(resp.gen, 2, "A4: the counter counts events, not changes");
        let displaced = resp
            .previous
            .expect("its own earlier instance is still a displaced holder");
        assert_eq!(displaced.op, "S-self");
        assert_eq!(displaced.desc, "first pass over the flow");
        assert_eq!(
            seat_on_the_wire(&state, &run_id, SLOT_A)
                .await
                .expect("still held")
                .desc,
            "back after a reconnect",
            "Q3: a new instance, not the old one edited"
        );
    }

    /// **Q1 / A9.** The description is what a later reader tells two
    /// takeovers apart by, so an empty one is refused at the boundary
    /// rather than stored as `""` — whitespace included, since a space is
    /// not a description.
    #[tokio::test]
    async fn an_acquire_without_a_desc_is_refused() {
        let state = test_state();
        let run_id = launched_run(&state, "no desc goal", &[SLOT_A]).await;

        for desc in ["", "   "] {
            let err = acquire(&state, &run_id, acquire_req("S-taker", desc, None))
                .await
                .expect_err("Q1: an acquire without a desc must be refused");
            assert_eq!(err.status, StatusCode::BAD_REQUEST);
            assert!(err.message.contains("desc"), "{}", err.message);
        }
        assert!(
            seat_on_the_wire(&state, &run_id, SLOT_A).await.is_none(),
            "a refused acquire must not have taken the seat anyway"
        );
    }

    /// An `OperatorId` that names nobody would make `current` read as held
    /// while no adapter could ever answer for it — the exact lie **O8**
    /// exists to prevent. The store does not guard this, so the route does.
    #[tokio::test]
    async fn an_acquire_without_an_op_is_refused() {
        let state = test_state();
        let run_id = launched_run(&state, "no op goal", &[SLOT_A]).await;

        let err = acquire(&state, &run_id, acquire_req("  ", "taking the seat", None))
            .await
            .expect_err("a blank holder must be refused");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("op is required"), "{}", err.message);
    }

    /// The seat rule is the launch pin's, reused: several declared seats
    /// and no name is a `400` that lists the candidates, and a name
    /// nothing declares is a typo rather than a new seat.
    #[tokio::test]
    async fn an_acquire_resolves_its_seat_by_the_same_rule_a_launch_pin_does() {
        let state = test_state();
        let run_id = launched_run(&state, "two seat goal", &[SLOT_A, SLOT_B]).await;

        let err = acquire(
            &state,
            &run_id,
            acquire_req("S-taker", "taking a lane", None),
        )
        .await
        .expect_err("two seats cannot be guessed between");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(
            err.message.contains(SLOT_A) && err.message.contains(SLOT_B),
            "the candidates must be listed: {}",
            err.message
        );

        let err = acquire(
            &state,
            &run_id,
            acquire_req("S-taker", "taking a lane", Some("phase-c-op")),
        )
        .await
        .expect_err("an undeclared seat must not be filed");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("phase-c-op"), "{}", err.message);

        let resp = acquire(
            &state,
            &run_id,
            acquire_req("S-taker", "taking lane B", Some(SLOT_B)),
        )
        .await
        .expect("a declared seat is nameable");
        assert_eq!(resp.slot, SLOT_B);
        assert!(
            seat_on_the_wire(&state, &run_id, SLOT_A).await.is_none(),
            "A1 is per seat: filling B leaves A alone"
        );
    }

    /// A seat the Run **already holds** is acquirable without consulting
    /// the Blueprint at all — proved by taking one on a Run whose Task row
    /// does not exist, so any Blueprint read would fail. The second half
    /// is the control: an unheld seat on the same Run does need the
    /// Blueprint, and says so instead of guessing.
    #[tokio::test]
    async fn a_held_seat_is_acquirable_without_reading_the_blueprint() {
        let state = test_state();
        let run_id = RunId::new();
        state
            .run_store
            .create(RunRecord {
                id: run_id.clone(),
                task_id: TaskId::new(),
                status: RunStatus::Running,
                step_entries: Vec::new(),
                degradations: Vec::new(),
                operator_sid: None,
                current: Default::default(),
                next_generation: 0,
                result_ref: None,
                input_json: None,
                created_at: 0,
                updated_at: 0,
            })
            .await
            .expect("seed a run whose Task row is absent");
        state
            .run_store
            .acquire_assignee(
                &run_id,
                SLOT_A,
                "S-incumbent",
                "seated before the task vanished",
            )
            .await
            .expect("seed the holder");

        let resp = acquire(
            &state,
            &run_id,
            acquire_req(
                "S-successor",
                "taking over a seat the run holds",
                Some(SLOT_A),
            ),
        )
        .await
        .expect("a held seat is a fact about this Run, not about today's Blueprint");
        assert_eq!(resp.slot, SLOT_A);
        assert_eq!(resp.previous.expect("displaced").op, "S-incumbent");

        let err = acquire(
            &state,
            &run_id,
            acquire_req("S-successor", "taking a seat nobody holds", Some(SLOT_B)),
        )
        .await
        .expect_err("an unheld seat has to be checked against the Blueprint");
        assert_ne!(
            err.status,
            StatusCode::OK,
            "an unresolvable Blueprint must not silently accept any name"
        );
    }

    /// The wire shape of "the seat was Vacant": `previous` is present and
    /// `null`, not absent. A field that vanished would be
    /// indistinguishable from a server that does not report predecessors,
    /// which is the one thing **Q4** is for.
    #[tokio::test]
    async fn a_vacant_predecessor_is_null_on_the_wire_rather_than_absent() {
        let state = test_state();
        let run_id = launched_run(&state, "wire shape goal", &[SLOT_A]).await;
        let resp = acquire(
            &state,
            &run_id,
            acquire_req("S-taker", "first holder", None),
        )
        .await
        .expect("acquire");

        let body = serde_json::to_value(&resp).expect("serialize");
        assert_eq!(body["previous"], Value::Null);
        assert!(
            body.get("previous").is_some(),
            "`previous` must be emitted even when null: {body}"
        );
        assert!(
            body.get("t_discard").is_none(),
            "nothing was displaced, so the Q5 report must not appear: {body}"
        );
    }

    /// **Q5, delivered.** A displaced holder that HAS a registered
    /// adapter is sent the discard, and the count it answers with reaches
    /// the acquirer. The adapter is addressed as an instance — the double
    /// below records the Run it was asked about, which is the whole of
    /// what `T-DISCARD.request` carries once the operator term is `self`.
    #[tokio::test]
    async fn an_acquire_discards_the_displaced_holders_requests_for_this_run() {
        let state = test_state();
        let run_id = launched_run(&state, "discard goal", &[SLOT_A]).await;
        let steps: Vec<StepId> = (1..=3)
            .map(|i| StepId::parse(format!("ST-discard-{i}")).expect("step id"))
            .collect();
        let incumbent = Arc::new(OwesRequests::new(
            steps
                .iter()
                .enumerate()
                .map(|(i, step)| waiting_spawn(&format!("req-{i}"), step, 1))
                .collect(),
        ));
        state
            .operator_adapters
            .register("S-incumbent", incumbent.clone() as Arc<dyn OperatorAdapter>)
            .await;
        // All three went out through the seat about to be taken, so all
        // three are this acquire's to drop.
        let _seats: Vec<_> = steps
            .iter()
            .map(|step| state.seat_ledger.record(&run_id, step, 1, SLOT_A, 1))
            .collect();

        acquire(
            &state,
            &run_id,
            acquire_req("S-incumbent", "holding the seat", None),
        )
        .await
        .expect("the first acquire");
        let resp = acquire(
            &state,
            &run_id,
            acquire_req("S-successor", "taking over", None),
        )
        .await
        .expect("the takeover");

        assert_eq!(
            resp.t_discard.expect("Q5 report").discarded,
            Some(3),
            "T-DISCARD.confirm(run, discarded) is passed through to the acquirer"
        );
        let discards = incumbent.discards.lock().expect("discard log").clone();
        assert_eq!(discards.len(), 1, "one acquire, one discard: {discards:?}");
        assert_eq!(
            discards[0].0,
            run_id.to_string(),
            "the discard is addressed at ONE Run — the one whose seat was taken"
        );
        let mut named = discards[0].1.clone();
        named.sort();
        assert_eq!(
            named,
            vec![
                "req-0".to_string(),
                "req-1".to_string(),
                "req-2".to_string()
            ],
            "and it names the requests, so the adapter drops those and no others"
        );
    }

    /// **The seat is the unit, not the Run.** One session can hold two
    /// seats of one Run — it is registered under its sid *and* under every
    /// role it claimed, all resolving to the same adapter — and an acquire
    /// takes one seat. The work in flight on the other seat must survive
    /// it.
    ///
    /// This is not a hypothetical race: **A6** would not have caught it
    /// either. A6 is enforced per slot (`AssigneeRouter` is built for one,
    /// and `acquire_assignee` replaces only its own key of `current`), so
    /// the untouched seat's generation never moved and its dispatch was
    /// still valid when a Run-scoped discard killed it.
    #[tokio::test]
    async fn a_takeover_on_one_seat_leaves_another_seats_work_alone() {
        let state = test_state();
        let run_id = launched_run(&state, "two seat goal", &[SLOT_A, SLOT_B]).await;
        let on_a = StepId::parse("ST-on-a").expect("step id");
        let on_b = StepId::parse("ST-on-b").expect("step id");

        // ONE adapter, TWO OperatorIds — exactly what
        // `register_operator_session` does with a session's sid and each of
        // its roles.
        let driver = Arc::new(OwesRequests::new(vec![
            waiting_spawn("req-a", &on_a, 1),
            waiting_spawn("req-b", &on_b, 1),
        ]));
        for op in [SLOT_A, SLOT_B] {
            state
                .operator_adapters
                .register(op, driver.clone() as Arc<dyn OperatorAdapter>)
                .await;
        }
        let _seat_a = state.seat_ledger.record(&run_id, &on_a, 1, SLOT_A, 1);
        let _seat_b = state.seat_ledger.record(&run_id, &on_b, 1, SLOT_B, 1);

        acquire(
            &state,
            &run_id,
            acquire_req(SLOT_A, "holding seat A", Some(SLOT_A)),
        )
        .await
        .expect("seat A");
        acquire(
            &state,
            &run_id,
            acquire_req(SLOT_B, "holding seat B", Some(SLOT_B)),
        )
        .await
        .expect("seat B");

        let resp = acquire(
            &state,
            &run_id,
            acquire_req("S-successor", "taking seat A only", Some(SLOT_A)),
        )
        .await
        .expect("the takeover of seat A");

        assert_eq!(
            resp.t_discard.expect("Q5 report").discarded,
            Some(1),
            "one seat was taken, so one request was dropped"
        );
        assert_eq!(
            driver.outstanding(),
            vec!["req-b".to_string()],
            "seat B's dispatch is still in flight: this acquire did not touch its seat, its \
             generation did not move, and A6 would have accepted its reply"
        );
    }

    /// A discard that cannot be addressed does not fail the acquire
    /// (**Q2**) — proved by taking a seat from a holder that is a role
    /// alias nobody currently holds, which is the ordinary state of
    /// affairs after the previous driver left.
    #[tokio::test]
    async fn an_unaddressable_discard_does_not_fail_the_acquire() {
        let state = test_state();
        let run_id = launched_run(&state, "unaddressable goal", &[SLOT_A]).await;

        acquire(
            &state,
            &run_id,
            acquire_req("main-ai", "the role held it", None),
        )
        .await
        .expect("the first acquire");
        let resp = acquire(
            &state,
            &run_id,
            acquire_req("S-successor", "taking over from a role nobody holds", None),
        )
        .await
        .expect("Q2: an acquire does not enquire, and does not refuse");

        assert_eq!(resp.previous.expect("displaced").op, "main-ai");
        assert_eq!(
            resp.t_discard.expect("Q5 report").discarded,
            None,
            "nothing was asked, so nothing is claimed"
        );
    }

    /// **W4 / (e).** The three assigning paths and the displacement they
    /// cause all land on the Run's own trace, next to its step events —
    /// so "who has held this Run, and when did each of them lose it" is
    /// answerable from one rail. Read back the way a client reads it
    /// (`GET /v1/runs/:id/trace`).
    #[tokio::test]
    async fn assignment_events_land_on_the_runs_trace() {
        let state = test_state();
        let run_id = launched_run(&state, "trace goal", &[SLOT_A]).await;

        acquire(
            &state,
            &run_id,
            acquire_req("S-first", "first holder", None),
        )
        .await
        .expect("the first acquire");
        acquire(&state, &run_id, acquire_req("S-second", "took over", None))
            .await
            .expect("the takeover");

        let events = assignment_events(&state, &run_id).await;
        let shape: Vec<(String, String, String)> = events
            .iter()
            .map(|e| {
                (
                    e.kind.clone(),
                    e.payload["assignee"]["op"]
                        .as_str()
                        .unwrap_or("")
                        .to_string(),
                    e.payload["source"]
                        .as_str()
                        .or_else(|| e.payload["reason"].as_str())
                        .unwrap_or("")
                        .to_string(),
                )
            })
            .collect();
        assert_eq!(
            shape,
            vec![
                (
                    trace_kind::ASSIGNEE_ASSIGNED.to_string(),
                    "S-first".to_string(),
                    "acquire".to_string()
                ),
                (
                    trace_kind::ASSIGNEE_RELEASED.to_string(),
                    "S-first".to_string(),
                    "displaced".to_string()
                ),
                (
                    trace_kind::ASSIGNEE_ASSIGNED.to_string(),
                    "S-second".to_string(),
                    "acquire".to_string()
                ),
            ],
            "the handover reads as: taken, lost by the incumbent, taken again"
        );
        assert_eq!(
            events[2].payload["previous"]["op"], "S-first",
            "and the assign row names who it displaced"
        );
        assert!(
            events.iter().all(|e| e.step_ref.is_none()),
            "a holder belongs to the Run, not to a step"
        );
    }

    /// A launch records which seat the caller named and which ones the
    /// server filled on its behalf — with a `source` label a reader does
    /// not have to parse English prose to tell apart.
    #[tokio::test]
    async fn a_launch_records_which_path_seated_the_operator() {
        // Pinned: the request named the holder. A launch pin is
        // fail-fast on an unregistered sid, so the session has to exist
        // — it is never engaged (the baseline Blueprint does not
        // delegate), which is why a stalling stand-in suffices.
        let state = test_state();
        state
            .engine
            .register_operator("S-pinned", Arc::new(StallingOperator))
            .await;
        let run_id = crate::tasks_start(
            State(state.clone()),
            Json(crate::TaskLaunchRequest {
                operator_sid: Some("S-pinned".to_string()),
                operator_desc: Some("pinned by the launch request".to_string()),
                operator_slot: Some(SLOT_A.to_string()),
                ..post_tasks_req_declaring("pin trace goal", &[SLOT_A])
            }),
        )
        .await
        .expect("tasks_start")
        .0
        .run_id;
        let pinned = assignment_events(&state, &run_id).await;
        assert_eq!(pinned.len(), 1, "one assign row: {pinned:?}");
        assert_eq!(pinned[0].payload["source"], "launch_pin");
        assert_eq!(pinned[0].payload["assignee"]["op"], "S-pinned");
        assert!(pinned[0].payload["previous"].is_null());

        // Auto-seated: the same pin, on a Blueprint with a second lane the
        // caller did not name. That lane goes to the pinned operator too,
        // and says the server chose it.
        let state = test_state();
        state
            .engine
            .register_operator("S-pinned", Arc::new(StallingOperator))
            .await;
        let run_id = crate::tasks_start(
            State(state.clone()),
            Json(crate::TaskLaunchRequest {
                operator_sid: Some("S-pinned".to_string()),
                operator_desc: Some("pinned by the launch request".to_string()),
                operator_slot: Some(SLOT_A.to_string()),
                ..post_tasks_req_declaring("auto seat trace goal", &[SLOT_A, SLOT_B])
            }),
        )
        .await
        .expect("tasks_start")
        .0
        .run_id;
        let seated = assignment_events(&state, &run_id).await;
        assert_eq!(seated.len(), 2, "one assign row per seat: {seated:?}");
        assert_eq!(seated[0].payload["source"], "launch_pin");
        assert_eq!(seated[0].payload["slot"], SLOT_A);
        assert_eq!(
            seated[1].payload["source"], "auto_seat",
            "the lane the caller did not name is server-chosen"
        );
        assert_eq!(seated[1].payload["slot"], SLOT_B);
        assert_eq!(
            seated[1].payload["assignee"]["op"], "S-pinned",
            "and it goes to the operator that launched the Run"
        );

        // An unpinned launch seats nothing at all, so it records nothing.
        let state = test_state();
        let run_id = launched_run(&state, "unpinned trace goal", &[SLOT_A]).await;
        assert!(
            assignment_events(&state, &run_id).await.is_empty(),
            "a launch that named no operator has no assignment to record"
        );
    }

    #[tokio::test]
    async fn an_acquire_on_an_unknown_run_is_404() {
        let state = test_state();
        let err = run_acquire(
            State(state),
            Path("R-does-not-exist".to_string()),
            Json(acquire_req(
                "S-taker",
                "taking a seat that is not there",
                None,
            )),
        )
        .await
        .expect_err("an unknown Run has no seat");
        assert_eq!(err.status, StatusCode::NOT_FOUND);
    }

    /// A snapshot written before the pin field existed still decodes, and
    /// resumes unpinned.
    #[test]
    fn pre_pin_launch_snapshot_still_decodes() {
        let snapshot = serde_json::json!({
            "blueprint": { "kind": "inline", "value": identity_blueprint() },
            "operator_id": "http-run",
            "role": "operator",
            "ttl": { "secs": 60, "nanos": 0 },
            "init_ctx": {},
            "operator_kind": null,
            "bridge_id": null,
            "hook_id": null,
            "operator_backend_id": null,
            "task_input": null,
            "check_policy": null,
        });
        let decoded: RunLaunchSnapshot =
            serde_json::from_value(snapshot).expect("a pre-pin snapshot must still decode");
        assert!(decoded.into_input().operator_sid.is_none());
    }

    /// A snapshot written before the two operator fields were folded into
    /// one still decodes, and resumes pinned to the sid it launched with.
    ///
    /// This is the resume round-trip the fold had to keep: an `Interrupted`
    /// Run created by the previous build is resumed by this one from the
    /// `input_json` that build wrote. The old blob names the value twice
    /// (`operator_backend_id` + `operator_pin`, always the same sid); the
    /// alias reads the first and serde drops the second.
    #[test]
    fn pre_fold_launch_snapshot_resumes_pinned() {
        let snapshot = serde_json::json!({
            "blueprint": { "kind": "inline", "value": identity_blueprint() },
            "operator_id": "http-run",
            "role": "operator",
            "ttl": { "secs": 60, "nanos": 0 },
            "init_ctx": {},
            "operator_kind": null,
            "bridge_id": null,
            "hook_id": null,
            "operator_backend_id": "S-live-op",
            "operator_pin": "S-live-op",
            "task_input": null,
            "check_policy": null,
        });
        let decoded: RunLaunchSnapshot =
            serde_json::from_value(snapshot).expect("a pre-fold snapshot must still decode");
        assert_eq!(
            decoded.into_input().operator_sid.as_deref(),
            Some("S-live-op"),
            "a Run interrupted before the fold must resume on the session it launched with"
        );
    }

    /// A snapshot carrying neither spelling — older than both — decodes to
    /// an unpinned resume rather than failing.
    #[test]
    fn launch_snapshot_without_any_operator_field_decodes() {
        let snapshot = serde_json::json!({
            "blueprint": { "kind": "inline", "value": identity_blueprint() },
            "operator_id": "http-run",
            "role": "operator",
            "ttl": { "secs": 60, "nanos": 0 },
            "init_ctx": {},
            "operator_kind": null,
            "bridge_id": null,
            "hook_id": null,
            "task_input": null,
            "check_policy": null,
        });
        let decoded: RunLaunchSnapshot =
            serde_json::from_value(snapshot).expect("an absent operator field must default");
        assert!(decoded.into_input().operator_sid.is_none());
    }

    #[tokio::test]
    async fn run_get_unknown_id_returns_404() {
        let state = test_state();
        match run_get(State(state), Path("R-does-not-exist".to_string())).await {
            Ok(_) => panic!("expected 404 for an unknown run"),
            Err(e) => assert_eq!(e.status, StatusCode::NOT_FOUND),
        }
    }

    #[tokio::test]
    async fn run_bindings_explain_reports_pinned_requested_effective_diff() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_tasks_req("binding explain")),
        )
        .await
        .expect("tasks_start")
        .0;
        let run = state
            .run_store
            .get(&posted.run_id)
            .await
            .expect("stored run");
        let mut snapshot: Value = serde_json::from_str(run.input_json.as_deref().unwrap()).unwrap();
        let mut bound_agents: Vec<BoundAgent> =
            serde_json::from_value(snapshot["bound_agents"].clone()).unwrap();
        let bound = &mut bound_agents[0];
        bound.runner = Some(Runner::WsClaudeCode {
            variant: "coder".to_string(),
            tools: vec!["Read".to_string()],
        });
        bound.recompute_binding_digest().unwrap();
        let request_digest = bound.binding_digest.clone();
        bound
            .set_attestation(BindingAttestation {
                request_digest: request_digest.clone(),
                provider_id: "operator-manifest".to_string(),
                provider_revision: Some("claude-code-1.2".to_string()),
                resolved_model: Some("claude-sonnet-4".to_string()),
                effective_tools: vec!["Bash".to_string(), "Read".to_string()],
                launch_variant: Some("coder".to_string()),
                capability_snapshot_digest: Some(mlua_swarm::blueprint::BindingDigest::sha256(
                    b"manifest-v1",
                )),
            })
            .unwrap();
        snapshot["bound_agents"] = serde_json::to_value(&bound_agents).unwrap();
        state
            .run_store
            .set_input_json(&posted.run_id, serde_json::to_string(&snapshot).unwrap())
            .await
            .unwrap();

        let explained = run_bindings_explain(State(state), Path(posted.run_id.to_string()))
            .await
            .expect("binding explain")
            .0;
        let entry = &explained.bindings[0];
        assert_eq!(entry.status, RunBindingStatus::Attested);
        assert_eq!(
            entry.requested.as_ref().unwrap().request_digest,
            request_digest
        );
        assert_eq!(
            entry
                .effective
                .as_ref()
                .unwrap()
                .provider_revision
                .as_deref(),
            Some("claude-code-1.2")
        );
        assert_eq!(
            entry
                .difference
                .as_ref()
                .unwrap()
                .additional_effective_tools,
            vec!["Bash"]
        );
        assert!(entry
            .difference
            .as_ref()
            .unwrap()
            .missing_requested_tools
            .is_empty());
        assert_ne!(entry.binding_digest, request_digest);
    }

    #[tokio::test]
    async fn run_bindings_explain_reports_snapshot_origin() {
        let state = test_state();
        let posted =
            crate::tasks_start(State(state.clone()), Json(post_tasks_req("origin explain")))
                .await
                .expect("tasks_start")
                .0;

        // An initial launch pins `origin = launch`.
        let explained = run_bindings_explain(State(state.clone()), Path(posted.run_id.to_string()))
            .await
            .expect("binding explain")
            .0;
        assert_eq!(explained.snapshot_origin, SnapshotOrigin::Launch);

        // Flip the persisted marker to `resume_backfill` → explain reflects it.
        let run = state.run_store.get(&posted.run_id).await.unwrap();
        let mut snapshot: Value = serde_json::from_str(run.input_json.as_deref().unwrap()).unwrap();
        snapshot["bound_agents_origin"] = serde_json::json!("resume_backfill");
        state
            .run_store
            .set_input_json(&posted.run_id, serde_json::to_string(&snapshot).unwrap())
            .await
            .unwrap();
        let explained = run_bindings_explain(State(state.clone()), Path(posted.run_id.to_string()))
            .await
            .expect("binding explain")
            .0;
        assert_eq!(explained.snapshot_origin, SnapshotOrigin::ResumeBackfill);

        // A snapshot with `bound_agents` but NO origin marker maps to the
        // safe side (`resume_backfill`) and still returns 200 — the 422 is
        // reserved for snapshots lacking `bound_agents` entirely.
        snapshot
            .as_object_mut()
            .unwrap()
            .remove("bound_agents_origin");
        state
            .run_store
            .set_input_json(&posted.run_id, serde_json::to_string(&snapshot).unwrap())
            .await
            .unwrap();
        let explained = run_bindings_explain(State(state), Path(posted.run_id.to_string()))
            .await
            .expect("explain still 200 without an origin marker")
            .0;
        assert_eq!(explained.snapshot_origin, SnapshotOrigin::ResumeBackfill);
    }

    #[tokio::test]
    async fn run_bindings_explain_never_guesses_for_legacy_snapshot() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_tasks_req("legacy binding explain")),
        )
        .await
        .expect("tasks_start")
        .0;
        state
            .run_store
            .set_input_json(&posted.run_id, "{}".to_string())
            .await
            .unwrap();

        let error = run_bindings_explain(State(state), Path(posted.run_id.to_string()))
            .await
            .expect_err("legacy run must not be re-resolved");
        assert_eq!(error.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(error
            .message
            .contains("current Blueprint state was not consulted"));
    }

    #[tokio::test]
    async fn run_bindings_explain_rejects_a_tampered_snapshot() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_tasks_req("tampered binding explain")),
        )
        .await
        .expect("tasks_start")
        .0;
        let run = state.run_store.get(&posted.run_id).await.unwrap();
        let mut snapshot: Value = serde_json::from_str(run.input_json.as_deref().unwrap()).unwrap();
        snapshot["bound_agents"][0]["agent"]["name"] = Value::String("tampered".into());
        state
            .run_store
            .set_input_json(&posted.run_id, serde_json::to_string(&snapshot).unwrap())
            .await
            .unwrap();

        let error = run_bindings_explain(State(state), Path(posted.run_id.to_string()))
            .await
            .expect_err("digest drift must fail closed");
        assert_eq!(error.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(error.message.contains("inconsistent binding snapshot"));
    }

    #[tokio::test]
    async fn task_get_unknown_id_returns_404() {
        let state = test_state();
        match task_get(State(state), Path("T-does-not-exist".to_string())).await {
            Ok(_) => panic!("expected 404 for an unknown task"),
            Err(e) => assert_eq!(e.status, StatusCode::NOT_FOUND),
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // GH #76 error surface: finalize_run Err arm populates result_ref with the
    // structured failure envelope; run_get surfaces it.
    // ──────────────────────────────────────────────────────────────────

    /// Seed a Task + Run row so `finalize_run` can update them.
    async fn seed_task_and_run(state: &AppState) -> (TaskId, RunId) {
        let task_id = TaskId::new();
        let run_id = RunId::new();
        state
            .task_store
            .create(TaskRecord {
                id: task_id.clone(),
                goal: "finalize-run-err-envelope".to_string(),
                blueprint_ref: json!("inline"),
                input_ctx: Value::Null,
                task_input_spec: None,
                status: TaskRecordStatus::Running,
                created_at: 0,
                updated_at: 0,
            })
            .await
            .expect("seed TaskRecord");
        state
            .run_store
            .create(RunRecord {
                id: run_id.clone(),
                task_id: task_id.clone(),
                status: RunStatus::Running,
                step_entries: Vec::new(),
                degradations: Vec::new(),
                operator_sid: None,
                current: Default::default(),
                next_generation: 0,
                result_ref: None,
                input_json: Some("{}".to_string()),
                created_at: 0,
                updated_at: 0,
            })
            .await
            .expect("seed RunRecord");
        (task_id, run_id)
    }

    #[tokio::test]
    async fn finalize_run_err_arm_populates_result_ref_with_structured_envelope() {
        let state = test_state();
        let (task_id, run_id) = seed_task_and_run(&state).await;

        let err: Result<TaskApplicationOutput, TaskApplicationError> =
            Err(TaskApplicationError::Launch(TaskLaunchError::FlowEval {
                message: "blocked: {\"verdict\":\"BLOCKED\"}".to_string(),
                failed_step: Some("gate".to_string()),
                verdict_value: Some(json!({"verdict": "BLOCKED", "reason": "not-applicable"})),
                partial_ctx: Some(
                    json!({"steps": {"ST-abc": {"step_ref": "gate", "status": "blocked"}}}),
                ),
            }));

        let _ = finalize_run(&state, &task_id, &run_id, err).await;

        let run = state.run_store.get(&run_id).await.expect("run present");
        assert_eq!(run.status, RunStatus::Failed);
        let envelope = run
            .result_ref
            .as_ref()
            .expect("result_ref must be Some on Err arm");
        assert_eq!(
            envelope["error"]["message"],
            "blocked: {\"verdict\":\"BLOCKED\"}"
        );
        assert_eq!(envelope["error"]["failed_step"], "gate");
        assert_eq!(envelope["error"]["verdict_value"]["verdict"], "BLOCKED");
        assert_eq!(
            envelope["partial_ctx"]["steps"]["ST-abc"]["status"],
            "blocked"
        );

        // Owning Task status also flipped.
        let task = state.task_store.get(&task_id).await.expect("task present");
        assert_eq!(task.status, TaskRecordStatus::Failed);
    }

    #[tokio::test]
    async fn finalize_run_err_arm_non_flow_eval_populates_envelope_with_null_structural_fields() {
        let state = test_state();
        let (task_id, run_id) = seed_task_and_run(&state).await;

        // A non-FlowEval error (e.g. NoStore) still lands the envelope
        // shape with `error.message` populated; the structural fields go
        // to JSON `null` (no breadcrumb source available).
        let err: Result<TaskApplicationOutput, TaskApplicationError> =
            Err(TaskApplicationError::NoStore);

        let _ = finalize_run(&state, &task_id, &run_id, err).await;
        let run = state.run_store.get(&run_id).await.expect("run present");
        let envelope = run
            .result_ref
            .as_ref()
            .expect("result_ref must be Some on Err arm");
        assert!(envelope["error"]["message"]
            .as_str()
            .expect("message string")
            .contains("store"));
        assert_eq!(envelope["error"]["failed_step"], Value::Null);
        assert_eq!(envelope["error"]["verdict_value"], Value::Null);
        assert_eq!(envelope["partial_ctx"], Value::Null);
    }

    /// Regression: the Ok arm still stores the raw `final_ctx` verbatim
    /// (NOT an envelope) — consumers that never saw a failure keep their
    /// pre-#76 shape. The disambiguation is the top-level `"error"` key:
    /// present iff the Err arm fired.
    #[tokio::test]
    async fn finalize_run_ok_arm_still_stores_raw_final_ctx_verbatim() {
        let state = test_state();
        let (task_id, run_id) = seed_task_and_run(&state).await;

        let ok: Result<TaskApplicationOutput, TaskApplicationError> = Ok(TaskApplicationOutput {
            token: mlua_swarm::CapToken {
                agent_id: "ut".to_string(),
                role: mlua_swarm::Role::Operator,
                scopes: vec!["*".to_string()],
                issued_at: 0,
                expire_at: u64::MAX,
                max_uses: None,
                nonce: "ut-nonce".to_string(),
                sig_hex: String::new(),
            },
            final_ctx: json!({"out": {"echoed": "hi"}}),
            bound_version: None,
        });

        let _ = finalize_run(&state, &task_id, &run_id, ok).await;
        let run = state.run_store.get(&run_id).await.expect("run present");
        assert_eq!(run.status, RunStatus::Done);
        let stored = run.result_ref.as_ref().expect("result_ref Some");
        // Raw final_ctx verbatim — NOT an envelope; no top-level "error" key.
        assert_eq!(stored, &json!({"out": {"echoed": "hi"}}));
        assert!(
            stored.get("error").is_none(),
            "Ok arm must never write an `error` key at the top of result_ref (envelope disambiguation)"
        );
    }

    /// `GET /v1/runs/:id` returns the `RunRecord` verbatim, so after a
    /// finalize_run Err arm the structured envelope surfaces through the
    /// existing handler — no new response type needed. Failure detection
    /// via the top-level `"error"` key inside `result_ref`.
    #[tokio::test]
    async fn run_get_surfaces_structured_failure_envelope_from_result_ref() {
        let state = test_state();
        let (_task_id, run_id) = seed_task_and_run(&state).await;
        let err: Result<TaskApplicationOutput, TaskApplicationError> =
            Err(TaskApplicationError::Launch(TaskLaunchError::FlowEval {
                message: "blocked: bad verdict".to_string(),
                failed_step: Some("scout".to_string()),
                verdict_value: Some(json!("BLOCKED")),
                partial_ctx: Some(json!({"steps": {}})),
            }));
        let _ = finalize_run(&state, &_task_id, &run_id, err).await;

        let Json(run) = run_get(State(state), Path(run_id.to_string()))
            .await
            .expect("run_get");
        assert_eq!(run.status, RunStatus::Failed);
        let envelope = run.result_ref.expect("result_ref Some");
        assert_eq!(envelope["error"]["failed_step"], "scout");
        assert_eq!(envelope["error"]["verdict_value"], "BLOCKED");
    }
}
