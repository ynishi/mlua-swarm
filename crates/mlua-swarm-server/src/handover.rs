//! The two things that make a handover safe to attempt — model §4.5:
//! *"A8 で排他を外した以上、記名一覧と担当者リストが唯一の取り違え防止装置"*
//! (with exclusivity gone, the 記名 list and the holder list are the only
//! thing standing between an Assignee and the wrong Run).
//!
//! This module owns:
//!
//! - the **write** side of the 記名's observed part (§4.2 **D2**) —
//!   [`record_observed_assignment`], called from each of the three paths
//!   that seat a holder;
//! - the **read** side of the holder list (§4.3) —
//!   [`run_assignees`], `GET /v1/runs/:id/assignees`;
//! - the four-axis read surface (§4.6 **W5**) — [`run_handover`],
//!   `GET /v1/runs/:id/handover`, and [`run_step_material`],
//!   `GET /v1/runs/:id/material`.
//!
//! The first two are one feature seen from the two ends: the first records
//! *which Runs an operator has*, the second records *which operators a Run
//! has*. An Assignee about to take a seat reads both and decides whether
//! the Run in front of it is the one it means.
//!
//! # The four axes, and why they are two routes and not four (or one)
//!
//! **W5** lists what an Assignee must be able to read *"前任者の有無に
//! 関わらず"*: the trace, the holder list, what is in flight, and the
//! material for the next action together with whether that attempt already
//! produced a `Final`. Issue: *"引き継ぎ専用ではなく Assignee が常時使う
//! 口"* — a surface used constantly, not only at a handover. Four calls to
//! answer one question would make it a handover ritual again.
//!
//! They land like this:
//!
//! | axis | where it is answered |
//! |---|---|
//! | 1 これまで何が行われたか | *referenced* from [`RunHandoverResp::trace`] — `GET /v1/runs/:id/trace` |
//! | 2 いま誰が担当か | [`RunHandoverResp::seats`], inline (and standalone on `/assignees`) |
//! | 3 いま何が宙に浮いているか | [`RunHandoverResp::unanswered`], inline |
//! | 4 次に何をやるべきか | the `Final` half inline per entry; the material on `/material` |
//!
//! **Axes 2 and 3 must be one read.** Axis 3's `OperatorId` and
//! `generation` do not exist below the SAP (**T1**) and are joined from
//! axis 2 — so if they were two calls, a seat changing hands in between
//! would produce a list whose generation column describes a holder the
//! list no longer shows. That is precisely the mistaken handover §4.5
//! leaves these two lists standing to prevent, manufactured by the reading
//! of them. They are taken from one `RunRecord` read here.
//!
//! **The join key is the seat, and it is not the adapter.** One
//! `Arc<dyn OperatorAdapter>` can answer for several seats of one Run — a
//! session is registered under its sid *and* under each of its roles, and
//! a launch auto-seats each declared slot from the adapter answering to
//! that slot's own name — so "which seat owes this request" is a fact
//! neither the adapter's answer nor the key used to reach it can supply.
//! Taking it from the latter is what put each waiting request on the list
//! once per seat, under two different `slot` / `op` / `generation`
//! triples. It comes from
//! [`SeatLedger`](crate::operator_ws::SeatLedger) instead: the router
//! records which seat it dispatched through, and this list reads it back.
//! A request that belongs to no seat is listed with no seat named — see
//! [`UnansweredStep`].
//!
//! **The `Final` bit belongs with the step it qualifies**, for the same
//! reason. `model.md:378-379` — *"値があるのに再実行して副作用を二重に
//! する / 値が無いのに完了扱いにする、のどちらも起こしうる"* — is a
//! decision made per un-answered step, so "this step is waiting" and "this
//! attempt already has a value" arriving from two instants is the one
//! skew that matters. It is a `bool` per entry, bounded by the list.
//!
//! **Axis 1 and the material are referenced, not embedded**, because both
//! are unbounded and both already have a home. The trace is a paginated
//! stream on its own route; copying a page of it in would either truncate
//! the answer or duplicate the route, and the reference carries the one
//! thing a snapshot can usefully add — `latest_seq`, the watermark
//! separating "already in this snapshot" from "happened after it". The
//! material is a whole `WorkerPayload` per step (prompt, system, context
//! pointers); N of them inline would make the constantly-used surface the
//! most expensive call on the server, and an Assignee reads the list to
//! decide *which* step it needs the material for.
//!
//! # What is deliberately not here
//!
//! No resume, no skip, no retry, and no route that empties a seat — **W1**
//! (*"server は resolve も skip も restart も自動で行わない"*) and **W2**
//! (*"Resume / 復旧の専用手続きを置かない … 通常経路で次の action を打て
//! ば済む"*). Everything in this module is a read; the next action is an
//! ordinary acquire followed by an ordinary dispatch.
//!
//! No deadline and no age (**R5**: the model sets no upper bound on the
//! wait), and no distinction between a request parked waiting for a
//! reconnect and one already written to a socket (**W3**: どちらも
//! DELIVER の応答待ち). An un-answered step is not a fault report — *"担当
//! が途切れた Step は「待ち」で止まっているだけ"* — and the field that
//! would say how long it had been waiting exists only to be compared
//! against a threshold, which is the mechanism **W2** declines.
//!
//! # What the observed part can actually be filled from
//!
//! §4.2 asks for five things per `Assign` — the Run and its goal,
//! `project_root`, `work_dir`, `task_metadata`, and the time. At the
//! moment a seat is taken they resolve like this:
//!
//! | piece | where it comes from |
//! |---|---|
//! | Run | the `run_id` being assigned; in scope at all three sites |
//! | goal | `TaskRecord::goal` on the owning Task row |
//! | `project_root` / `work_dir` / `task_metadata` | `TaskRecord::task_input_spec` — the persisted `TaskInputSpec` the launch was given |
//! | time | the server clock at the append |
//!
//! `TaskInputSpec` is the same value `TaskLaunchService` later builds
//! `TaskInputMiddleware` from, so reading it off the Task row is reading
//! the middleware's own input rather than guessing at its output — which
//! matters, because the middleware runs per *spawn*, well after the
//! `Assign`, and has no value to offer at this point in the Run.
//!
//! A launch that carried no Task-level input has no
//! `project_root` / `work_dir` / `task_metadata` at all, and the three
//! fields are then recorded as absent. Nothing is substituted: a
//! `project_root` the server invented would be read as a fact about where
//! the work is happening.

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use mlua_swarm::store::operator_session::ObservedAssignment;
use mlua_swarm::store::output::OutputEvent;
use mlua_swarm::store::run::{Assignee, RunRecord};
use mlua_swarm::store::task::TaskRecord;
use mlua_swarm::store::trace::TraceQuery;
use mlua_swarm::{BlueprintRef, RunId, StepId, TaskId, TaskInputSpec, WorkerPayload};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use crate::operator_ws::login::{authorize_any_operator, LoginSession};
use crate::operator_ws::{PendingKind, PendingRequest};
use crate::tasks::{map_run_store_err, now_secs};
use crate::{ApiError, AppState};

// ──────────────────────────────────────────────────────────────────────────
// (a) the observed part — write side
// ──────────────────────────────────────────────────────────────────────────

/// Record one `Assign` onto the assigned operator's 記名 (**D2**).
///
/// Called from all three paths that seat a holder — the launch pin, the
/// launch-time auto-seat, and `POST /v1/runs/:id/acquire` — with the same
/// arguments each site already has in hand.
///
/// # Which session gets the entry
///
/// `op` is an `OperatorId`, which is a sid *or* a role alias (both are
/// keys of the same space; `login::register_operator_session` files a
/// session under its sid and each of its roles). It is resolved the same
/// way a dispatch resolves one: sid first, then the role map. An `op` that
/// resolves to no live session gets no entry and no error — a seat can be
/// acquired for a role nobody currently holds (**Q2**: an acquire does not
/// enquire), and there is simply no 記名 to write on.
///
/// # Best effort, on purpose
///
/// Every failure here — the Task row unreadable, the session gone, the
/// store write refused — leaves the `Assign` itself untouched. A seat that
/// changed hands but could not be described is still a seat that changed
/// hands; refusing the handover over the description would invert the
/// priority. The same call [`crate::assignee_trace`] makes.
pub(crate) async fn record_observed_assignment(
    state: &AppState,
    run_id: &RunId,
    task_id: &TaskId,
    slot: &str,
    op: &str,
) {
    let Some(live) = resolve_session(state, op).await else {
        return;
    };

    // One read supplies four of the five pieces; see the module doc.
    let task = match state.task_store.get(task_id).await {
        Ok(task) => Some(task),
        Err(error) => {
            tracing::warn!(
                %run_id, %task_id, %error,
                "record_observed_assignment: the Task row could not be read, so this entry \
                 carries the Run and the time but no goal or paths"
            );
            None
        }
    };
    let goal = task.as_ref().map(|t| t.goal.clone());
    let spec = task.as_ref().and_then(task_input_spec);

    let entry = ObservedAssignment::new(
        run_id.to_string(),
        slot.to_string(),
        goal,
        spec.as_ref().and_then(|s| s.project_root.clone()),
        spec.as_ref().and_then(|s| s.work_dir.clone()),
        spec.as_ref().and_then(|s| s.task_metadata.clone()),
        now_secs(),
    );
    live.record_observed(&state.operator_session_store, entry)
        .await;
}

/// Decode a Task row's stored `task_input_spec` blob.
///
/// The column is a bare `Value` by design (the store layer does not depend
/// on the spec's Rust type), so a row written by a build with a different
/// shape can fail to decode. That is warned about and treated as "no
/// Task-level input", which is the honest reading: the paths could not be
/// read, so they are not reported.
fn task_input_spec(task: &TaskRecord) -> Option<TaskInputSpec> {
    let value = task.task_input_spec.clone()?;
    match serde_json::from_value::<TaskInputSpec>(value) {
        Ok(spec) => Some(spec),
        Err(error) => {
            tracing::warn!(
                task_id = %task.id, %error,
                "record_observed_assignment: the stored task_input_spec did not decode; \
                 the observed entry omits project_root / work_dir / task_metadata"
            );
            None
        }
    }
}

/// `OperatorId` → the live session it names, by sid then by role alias.
async fn resolve_session(state: &AppState, op: &str) -> Option<Arc<LoginSession>> {
    let sessions = state.operator_sessions.lock().await;
    if let Ok(sid) = mlua_swarm::SessionId::parse(op.to_string()) {
        if let Some(live) = sessions.get(&sid) {
            return Some(live.clone());
        }
    }
    drop(sessions);

    let sid = {
        let roles = state.roles_to_sid.lock().await;
        roles.get(op).cloned()
    }?;
    let sessions = state.operator_sessions.lock().await;
    sessions.get(&sid).cloned()
}

// ──────────────────────────────────────────────────────────────────────────
// (b) the holder list — read side
// ──────────────────────────────────────────────────────────────────────────

/// One Operator seat of a Run, held or not (model §4.3: *"居るなら
/// `OperatorId` / 現在の世代 / Assignee の記名、居なければ居ないと分かる"*).
///
/// # Vacant is a value, not an omission
///
/// [`Self::vacant`] is always present and [`Self::holder`] is always
/// serialized — `null` when nobody holds the seat. Neither is skipped.
/// The alternative, leaving an unheld seat out of the array, makes
/// "nobody is on this" indistinguishable from "the list did not manage to
/// report it", and an Assignee reading the second as the first takes a
/// Run somebody else is driving.
///
/// This is the same objection `RunRecord::current` used to invite by
/// skipping an empty map on the wire (`GET /v1/runs/:id` simply had no
/// `current` key), which is why that skip is gone.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct RunSeat {
    /// The Blueprint-declared Operator seat (`Run.current`'s key,
    /// `AgentDef.spec.operator_ref`'s target).
    pub slot: String,
    /// `true` iff nobody holds this seat right now.
    pub vacant: bool,
    /// The holder: its `OperatorId`, the generation it was stamped at
    /// (**A4**), and the `desc` its `Assign` recorded (**A9** — the
    /// Assignee-side 記名, which is a different thing from the
    /// Operator-side one on `GET /v1/operators`). `null` iff
    /// [`Self::vacant`].
    pub holder: Option<Assignee>,
    /// `true` when the Blueprint declares this seat. A seat held but not
    /// declared is reachable — a store-backed Blueprint can drop an
    /// `operators[]` entry after a Run was launched against it — and it is
    /// reported rather than hidden, because the holder is real and a
    /// dispatch through it is not.
    pub declared: bool,
}

/// Where the seat names in a [`RunAssigneesResp`] came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SeatsSource {
    /// The Run's Blueprint was resolved, so every declared seat is listed
    /// — including the ones nobody holds, which is the whole point.
    Blueprint,
    /// The Blueprint could not be resolved, so only the seats the Run
    /// actually holds are listed. A seat that is declared and `Vacant`
    /// cannot appear in this mode; [`RunAssigneesResp::note`] says so.
    RunCurrentOnly,
}

/// Response body for `GET /v1/runs/:id/assignees`.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct RunAssigneesResp {
    /// The Run these seats belong to.
    #[schemars(with = "String")]
    pub run_id: RunId,
    /// The Run's generation counter `G` (**A4**) as of this read — the
    /// value the next assignment event will exceed. A holder whose `gen`
    /// equals it is the most recent event on this Run.
    pub generation: u64,
    /// Every seat, `slot`-sorted, held and vacant alike.
    pub seats: Vec<RunSeat>,
    /// Whether [`Self::seats`] covers the declared seats or only the held
    /// ones.
    pub seats_source: SeatsSource,
    /// Why the Blueprint could not be read, when it could not. `null` on
    /// the ordinary path.
    pub note: Option<String>,
}

/// `GET /v1/runs/:id/assignees`. The holder list of one Run (model §4.3) —
/// who holds each Operator seat, and which seats nobody holds.
///
/// # Bearer required (**D3**)
///
/// Any live Operator session's token, same rule as the 記名 list; see
/// [`authorize_any_operator`]. **W5** names the reader: an Assignee, which
/// is someone who has joined. Note the asymmetry with
/// `POST /v1/runs/:id/acquire`, which is deliberately *not* gated
/// (**B2**/**B3**): taking a seat needs no token, and it is *reading who
/// is on it* that does. That is the right way round — the bearer must not
/// decide assignment, and the thing that actually prevents a mistaken
/// handover is this list.
///
/// # Scope: one Run
///
/// There is no cross-Run form of this. `RunListFilter` filters on
/// `task_id` / `status` / `limit` / `offset` and cannot select by holder,
/// so answering "every Run this operator holds" from here would mean
/// scanning every Run — and the same question is already answered from the
/// other end, in one row, by the 記名's observed part on
/// `GET /v1/operators`.
///
/// # Status codes
///
/// - `401` — no Bearer, or one no live session matches.
/// - `400` — the id is not a `RunId`.
/// - `404` — no Run with this id.
/// - `200` — the seat list.
pub async fn run_assignees(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = authorize_any_operator(&state, &headers).await {
        return resp;
    }
    match run_assignees_inner(&state, id).await {
        Ok(body) => (StatusCode::OK, Json(body)).into_response(),
        Err(error) => error.into_response(),
    }
}

async fn run_assignees_inner(state: &AppState, id: String) -> Result<RunAssigneesResp, ApiError> {
    let run_id =
        RunId::parse(id).map_err(|e| ApiError::bad_request(format!("invalid run id: {e}")))?;
    let run = state
        .run_store
        .get(&run_id)
        .await
        .map_err(map_run_store_err)?;
    let (seats, seats_source, note) = seat_list(state, &run).await;

    Ok(RunAssigneesResp {
        run_id,
        generation: run.next_generation,
        seats,
        seats_source,
        note,
    })
}

/// Every seat of `run` — the union of the seats its Blueprint declares and
/// the seats it actually holds — with where the declared half came from.
///
/// Shared by `/assignees` and the axis-2 half of `/handover` so the two
/// routes cannot drift into disagreeing about who holds what.
async fn seat_list(
    state: &AppState,
    run: &RunRecord,
) -> (Vec<RunSeat>, SeatsSource, Option<String>) {
    let (declared, seats_source, note) = declared_seats(state, run).await;
    let mut names: BTreeSet<String> = run.current.keys().cloned().collect();
    names.extend(declared.iter().cloned());

    let seats = names
        .into_iter()
        .map(|slot| {
            let holder = run.current.get(&slot).cloned();
            RunSeat {
                vacant: holder.is_none(),
                declared: declared.contains(&slot),
                holder,
                slot,
            }
        })
        .collect();
    (seats, seats_source, note)
}

/// The seat names this Run's Blueprint declares.
///
/// Resolved the same way `tasks::resolve_acquire_slot` does it — the Task
/// row's `blueprint_ref`, through `TaskApplication::resolve`. Every
/// failure degrades to [`SeatsSource::RunCurrentOnly`] with a note rather
/// than to an error: a Run whose Blueprint has since become unresolvable
/// still has holders, and refusing to name them would take the list away
/// exactly when the Run is in trouble.
async fn declared_seats(
    state: &AppState,
    run: &RunRecord,
) -> (BTreeSet<String>, SeatsSource, Option<String>) {
    let degraded = |reason: String| (BTreeSet::new(), SeatsSource::RunCurrentOnly, Some(reason));

    let task = match state.task_store.get(&run.task_id).await {
        Ok(task) => task,
        Err(error) => {
            return degraded(format!(
                "task {} could not be read ({error}), so only the seats this Run holds are \
                 listed; a declared-but-vacant seat is not shown",
                run.task_id
            ));
        }
    };
    let blueprint_ref: BlueprintRef = match serde_json::from_value(task.blueprint_ref.clone()) {
        Ok(r) => r,
        Err(error) => {
            return degraded(format!(
                "the stored blueprint_ref of task {} did not decode ({error}), so only the \
                 seats this Run holds are listed; a declared-but-vacant seat is not shown",
                run.task_id
            ));
        }
    };
    match state.task_app.resolve(&blueprint_ref).await {
        Ok((blueprint, _version)) => (
            blueprint.operators.iter().map(|o| o.name.clone()).collect(),
            SeatsSource::Blueprint,
            None,
        ),
        Err(error) => degraded(format!(
            "this Run's Blueprint did not resolve ({error}), so only the seats it holds are \
             listed; a declared-but-vacant seat is not shown"
        )),
    }
}

// ──────────────────────────────────────────────────────────────────────────
// (d) the four-axis read surface — W5
// ──────────────────────────────────────────────────────────────────────────

/// One Step waiting for an answer (**W5** axis 3) — a `T-DELIVER` this
/// Run's current holder has been given and has not confirmed.
///
/// # Not a fault
///
/// *"担当が途切れた Step は「待ち」で止まっているだけ。壊れた状態では
/// ないので、復旧の手続きも Resume のような特殊な操作も要らない"*
/// (`model.md:360-361`). Nothing on this struct grades the wait: no age,
/// no deadline, no attempt counter, and no flag separating "still parked
/// for a reconnect" from "already on the wire" (**W3**). What it answers
/// is *which* question is outstanding and *who* was asked.
///
/// # Half of it is joined on above the SAP
///
/// [`Self::req_id`], [`Self::kind`], [`Self::step_id`] and
/// [`Self::attempt`] come up from the adapter as a
/// [`PendingRequest`]. [`Self::slot`], [`Self::op`] and
/// [`Self::generation`] cannot: neither an `OperatorId` nor a generation
/// crosses the SAP (**T1**), so they are taken from `Run.current` — the
/// place they are true — as of the same read that produced the seats.
///
/// # Which seat, and when there is none
///
/// The join needs one more fact than the adapter's answer carries: *which
/// seat* the request went out through. It cannot be inferred from which
/// adapter answered, because one adapter can back several seats of one Run
/// (see [`SeatLedger`](crate::operator_ws::SeatLedger)), and inferring it
/// that way is what made each waiting request appear once per seat, under
/// two different `slot` / `op` / `generation` triples, at most one of them
/// true. It comes from the ledger the routers write instead.
///
/// A request the ledger cannot place — a `hook_before`, which is
/// dispatched through the sid-registered hook and never reaches a router —
/// is listed with all three fields `null`. It is listed because it *is*
/// outstanding and an omission would read as "nothing is waiting there",
/// which is the answer that invites a re-run; the three are `null` because
/// naming one of the seats the answering adapter happens to back would be
/// the guess this type exists to stop making.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct UnansweredStep {
    /// The Operator seat this request was dispatched through, or `null`
    /// when it was not dispatched through one — see the type doc.
    pub slot: Option<String>,
    /// The `OperatorId` of whoever holds [`Self::slot`] now, from
    /// `Run.current[slot].op` — a sid or a role alias, whichever the
    /// `Assign` named.
    ///
    /// `null` when [`Self::slot`] is, and also when the seat is named but
    /// has since been vacated: the request is still owed, and there is
    /// simply nobody currently in the seat to name.
    pub op: Option<String>,
    /// The generation the seat's current holder was stamped at (**A4**),
    /// from `Run.current[slot].gen`. `null` under the same two conditions
    /// as [`Self::op`].
    ///
    /// This is the *seat's* generation now, not a generation the request
    /// was sent under — nothing below the SAP records one (**T1**), and
    /// **A6** is enforced where it can be, in `AssigneeRouter::execute`,
    /// which re-reads `current` after the adapter answers and refuses a
    /// reply whose generation has moved.
    pub generation: Option<u64>,
    /// The correlator the eventual `answer` / `hook_ack` / `spawn_ack`
    /// will quote.
    pub req_id: String,
    /// Which reply-expecting verb is waiting.
    pub kind: PendingKind,
    /// The step the request is addressed at.
    #[schemars(with = "String")]
    pub step_id: StepId,
    /// The attempt, when the parking verb had a `Ctx` to read one from.
    /// See [`PendingRequest::attempt`] — unreachable as `null` through a
    /// Run-scoped read today, and left honest rather than defaulted.
    pub attempt: Option<u32>,
    /// **W5** axis 4, first half: whether this `(step_id, attempt)`
    /// already has a `Final` in the output tail.
    ///
    /// `null` when [`Self::attempt`] is `null` — a `Final` is addressed by
    /// attempt, so with no attempt there is nothing to look one up under,
    /// and `false` would be a claim rather than an absence.
    pub final_present: Option<bool>,
    /// The `ok` flag of that `Final`, when there is one. `null` when there
    /// is none.
    ///
    /// The flag rather than the body: it is the one field the engine's own
    /// dispatch path consults for flow control, and it separates "there is
    /// a value and it succeeded" from "there is a value and it failed",
    /// which are different next actions. The body is not here — see
    /// [`StepMaterialResp`].
    pub final_ok: Option<bool>,
    /// Where to fetch the material for this step (**W5** axis 4, second
    /// half). A path, not a URL: the scheme and host are the caller's, the
    /// same convention `SystemRef.uri` follows in `Http` mode.
    pub material_route: String,
}

/// A held seat whose in-flight requests could not be read.
///
/// Reported for the same reason [`RunSeat::vacant`] is a value rather than
/// an omission: an [`RunHandoverResp::unanswered`] list that silently
/// skipped a seat would read as "nothing is in flight there", and "nothing
/// is in flight" is the answer that invites a re-run.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct UnreadSeat {
    /// The seat.
    pub slot: String,
    /// Its holder's `OperatorId`.
    pub op: String,
    /// Why the read did not happen.
    pub reason: String,
}

/// A pointer to the Run's trace (**W5** axis 1), which this response does
/// not carry inline.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct TraceRef {
    /// The route that serves it, already paginated.
    pub route: String,
    /// The highest `seq` on the rail as of this snapshot, or `null` when
    /// the rail is empty.
    ///
    /// Doubles as a watermark and as an `after=` cursor: an event with a
    /// greater `seq` happened after the seats and the un-answered list
    /// below were read, so a reader can tell the two apart instead of
    /// guessing.
    pub latest_seq: Option<u64>,
}

/// Response body for `GET /v1/runs/:id/handover` — **W5**'s four axes in
/// one read. See the module doc for why this is the shape.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct RunHandoverResp {
    /// The Run.
    #[schemars(with = "String")]
    pub run_id: RunId,
    /// The Run's generation counter `G` (**A4**) as of this read.
    pub generation: u64,
    /// **Axis 1**, by reference.
    pub trace: TraceRef,
    /// **Axis 2** — every seat, held and vacant alike; the same value
    /// `GET /v1/runs/:id/assignees` serves.
    pub seats: Vec<RunSeat>,
    /// Whether [`Self::seats`] covers the declared seats or only the held
    /// ones.
    pub seats_source: SeatsSource,
    /// Why the Blueprint could not be read, when it could not.
    pub note: Option<String>,
    /// **Axis 3** — every request a current holder still owes this Run,
    /// each listed **once**, sorted by seat then by `req_id`, with the
    /// requests that belong to no seat last. Empty means every holder
    /// answered when asked, not that nobody was asked.
    pub unanswered: Vec<UnansweredStep>,
    /// Seats [`Self::unanswered`] could not account for. Empty on the
    /// ordinary path.
    pub unread_seats: Vec<UnreadSeat>,
}

/// `GET /v1/runs/:id/handover`. The four axes **W5** requires an Assignee
/// to be able to read, *"前任者の有無に関わらず"*.
///
/// # Bearer required (**D3** / **W5**)
///
/// Any live Operator session's token, as on `/assignees` and the 記名
/// list; see [`authorize_any_operator`].
///
/// # What an empty `unanswered` does and does not mean
///
/// It means no *current* holder owes this Run a reply. The list is built
/// by asking the adapters this Run's held seats resolve to, so an operator
/// that has since lost **every** seat of this Run — released by **A7**
/// when it was found Disconnected, or by the **O8** cascade — is not asked
/// and its parked requests are not listed. Finding them would mean
/// enumerating every adapter registered on the server on every read. That
/// history is on the trace instead, as a `core.assignee_released` row
/// (**W4** — one rail, so a reader does not have to line two up).
///
/// An operator that lost one seat and kept another is a different case and
/// *is* asked, because it is still a holder. A request of its released
/// seat then appears with that seat named and `op` / `generation` `null` —
/// the seat is a fact the ledger recorded, the holder is not, and a
/// request that is genuinely still outstanding is better listed than
/// hidden.
///
/// # Status codes
///
/// - `401` — no Bearer, or one no live session matches.
/// - `400` — the id is not a `RunId`.
/// - `404` — no Run with this id.
/// - `200` — the snapshot.
pub async fn run_handover(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = authorize_any_operator(&state, &headers).await {
        return resp;
    }
    match run_handover_inner(&state, id).await {
        Ok(body) => (StatusCode::OK, Json(body)).into_response(),
        Err(error) => error.into_response(),
    }
}

async fn run_handover_inner(state: &AppState, id: String) -> Result<RunHandoverResp, ApiError> {
    let run_id =
        RunId::parse(id).map_err(|e| ApiError::bad_request(format!("invalid run id: {e}")))?;
    // One `RunRecord` read feeds both axis 2 and axis 3's joined fields —
    // see the module doc on why they must not come from two.
    let run = state
        .run_store
        .get(&run_id)
        .await
        .map_err(map_run_store_err)?;
    let (seats, seats_source, note) = seat_list(state, &run).await;
    let (unanswered, unread_seats) = unanswered_steps(state, &run_id, &seats).await;

    Ok(RunHandoverResp {
        run_id: run_id.clone(),
        generation: run.next_generation,
        trace: trace_ref(state, &run_id).await,
        seats,
        seats_source,
        note,
        unanswered,
        unread_seats,
    })
}

/// Axis 1's pointer: the trace route plus the rail's current head.
///
/// A failed read degrades to `latest_seq: null` rather than to an error.
/// The rail is deliberately uncoupled from `RunStore` (a trace can outlive
/// or precede its Run row, which is why `GET /v1/runs/:id/trace` answers
/// an unknown Run with an empty list rather than a `404`), so a watermark
/// that could not be taken must not take the seats and the un-answered
/// list down with it.
async fn trace_ref(state: &AppState, run_id: &RunId) -> TraceRef {
    let query = TraceQuery {
        latest: Some(1),
        ..Default::default()
    };
    let latest_seq = match state.run_trace_store.list(run_id, &query).await {
        Ok(events) => events.last().map(|event| event.seq),
        Err(error) => {
            tracing::warn!(
                %run_id, %error,
                "handover snapshot: the trace watermark could not be read; the route is still \
                 reported, without a latest_seq"
            );
            None
        }
    };
    TraceRef {
        route: format!("/v1/runs/{run_id}/trace"),
        latest_seq,
    }
}

/// Axis 3: ask this Run's held seats what is still owed, once per
/// *adapter*, and attribute each answer to the seat it was dispatched
/// through.
///
/// # One read per adapter, not one per seat
///
/// The seats are the reason to ask, but they are not the unit that
/// answers: one `Arc<dyn OperatorAdapter>` can back several seats of one
/// Run (a session is registered under its sid *and* under each of its
/// roles, and `seat_declared_operators` auto-seats each declared slot from
/// the adapter answering to that slot's own name). Asking per seat asked
/// the same object twice and got the same requests twice, and the join
/// then stamped each copy with a different seat's `op` and `gen` — two
/// rows for one waiting request, at most one of them true.
///
/// So adapters are visited once, identified by pointer rather than by the
/// `OperatorId` that reached them (two `OperatorId`s resolving to one
/// object is exactly the case being handled). Which seat a request belongs
/// to is then a separate question, answered by
/// [`SeatLedger`](crate::operator_ws::SeatLedger) — the fact the router
/// recorded on the way down — rather than by which key was used to find
/// the adapter.
///
/// Vacant seats are skipped without a note — a seat nobody holds has
/// nobody to ask, and [`RunSeat::vacant`] already says so on the same
/// response. A *held* seat that cannot be asked is reported in the second
/// return value.
async fn unanswered_steps(
    state: &AppState,
    run_id: &RunId,
    seats: &[RunSeat],
) -> (Vec<UnansweredStep>, Vec<UnreadSeat>) {
    let mut unread = Vec::new();
    let mut visited: HashSet<usize> = HashSet::new();
    // `req_id`-keyed, so a request cannot enter twice however it was
    // reached, and so the order is stable across reads of an unchanged
    // server (the adapters answer from a `HashMap`).
    let mut outstanding: BTreeMap<String, PendingRequest> = BTreeMap::new();

    for seat in seats {
        let Some(holder) = seat.holder.as_ref() else {
            continue;
        };
        let Some(adapter) = state.operator_adapters.get(&holder.op).await else {
            // Exactly the condition `AssigneeRouter::execute` refuses to
            // read as Vacant: a registry miss is a fact about wiring, not
            // about the holder. It is named here for the same reason —
            // and a boot that restored sessions before their clients
            // reconnected reaches it legitimately.
            unread.push(UnreadSeat {
                slot: seat.slot.clone(),
                op: holder.op.clone(),
                reason: format!(
                    "operator '{}' holds this seat but is not registered in the adapter registry, \
                     so what it owes this Run could not be read; registered: [{}]",
                    holder.op,
                    state.operator_adapters.ids().await.join(", ")
                ),
            });
            continue;
        };
        // Thin-pointer identity: `Arc::ptr_eq` over a set is what this is,
        // written as a key so the visit is O(1) per seat.
        if !visited.insert(Arc::as_ptr(&adapter) as *const () as usize) {
            continue;
        }
        for request in adapter.pending_for_run(run_id).await {
            outstanding.insert(request.req_id.clone(), request);
        }
    }

    let mut steps = Vec::new();
    for request in outstanding.into_values() {
        let slot = state.seat_ledger.slot_of(run_id, &request);
        let holder = slot
            .as_ref()
            .and_then(|slot| seats.iter().find(|seat| &seat.slot == slot))
            .and_then(|seat| seat.holder.as_ref());
        steps.push(unanswered_step(state, run_id, slot, holder, request).await);
    }
    // Seat-attributed rows first, in seat order, then the ones that belong
    // to no seat — each group already `req_id`-ordered by the map above.
    steps.sort_by(|a, b| match (&a.slot, &b.slot) {
        (Some(x), Some(y)) => x.cmp(y),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });
    (steps, unread)
}

/// One [`PendingRequest`] plus the seat it was dispatched through (when it
/// was), whoever holds that seat now (when anyone does), and the `Final`
/// its attempt already has, if any.
async fn unanswered_step(
    state: &AppState,
    run_id: &RunId,
    slot: Option<String>,
    holder: Option<&Assignee>,
    request: PendingRequest,
) -> UnansweredStep {
    let found = match request.attempt {
        Some(attempt) => Some(final_of(state, &request.step_id, attempt).await),
        None => None,
    };
    UnansweredStep {
        slot,
        op: holder.map(|h| h.op.clone()),
        generation: holder.map(|h| h.gen),
        material_route: material_route(run_id, &request.step_id),
        req_id: request.req_id,
        kind: request.kind,
        step_id: request.step_id,
        attempt: request.attempt,
        final_present: found.map(|f| f.is_some()),
        final_ok: found.flatten(),
    }
}

/// The path [`run_step_material`] answers for one step.
fn material_route(run_id: &RunId, step_id: &StepId) -> String {
    format!("/v1/runs/{run_id}/material?step_id={step_id}")
}

/// Whether `(step_id, attempt)` has a `Final` in the output tail, and what
/// its `ok` flag was — `None` when there is none.
///
/// Reads the tail through the engine, which is where a `Final` submitted
/// via `POST /v1/worker/result` lands, and takes the **last** one. Exactly
/// one `Final` per attempt is the contract
/// ([`OutputEvent::Final`](mlua_swarm::store::output::OutputEvent::Final):
/// *"Exactly one per attempt, emitted last"*); reading from the end means
/// a tail that somehow carried two would answer with the one that would
/// actually be folded, rather than with a stale first.
async fn final_of(state: &AppState, step_id: &StepId, attempt: u32) -> Option<bool> {
    state
        .engine
        .output_tail(step_id, attempt)
        .await
        .iter()
        .rev()
        .find_map(|event| match event {
            OutputEvent::Final { ok, .. } => Some(*ok),
            _ => None,
        })
}

/// Query params for `GET /v1/runs/:id/material`.
#[derive(Debug, Deserialize)]
pub struct StepMaterialQuery {
    /// The step to fetch the material for — typically one an
    /// [`UnansweredStep`] named.
    pub step_id: StepId,
}

/// Whether the requested step could be shown to belong to the Run in the
/// path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunLink {
    /// The payload's own context names this Run.
    Confirmed,
    /// The payload carries no Run identity to check against, so the
    /// path's Run is taken at face value. Reached when the dispatch was
    /// made without `AgentContextMiddleware` layered, or before it existed
    /// — see [`RunHandoverResp::note`]'s sibling,
    /// [`StepMaterialResp::note`].
    Unconfirmed,
}

/// Response body for `GET /v1/runs/:id/material` — **W5** axis 4's second
/// half, together with the first so the route answers "what do I do next"
/// on its own.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct StepMaterialResp {
    /// The Run the material was asked for under.
    #[schemars(with = "String")]
    pub run_id: RunId,
    /// Whether the payload confirmed it belongs to that Run.
    pub run_link: RunLink,
    /// Why the link is [`RunLink::Unconfirmed`], when it is. `null`
    /// otherwise.
    pub note: Option<String>,
    /// The material itself: the same `WorkerPayload` a SubAgent fetches
    /// from `GET /v1/worker/prompt`, `context.steps` assembled fresh.
    pub payload: WorkerPayload,
    /// Whether this attempt already has a `Final`.
    pub final_present: bool,
    /// That `Final`'s `ok` flag, if there is one. The body is deliberately
    /// not returned — see below.
    pub final_ok: Option<bool>,
}

/// `GET /v1/runs/:id/material?step_id=<id>`. The material for one step,
/// readable with an Assignee's bearer.
///
/// # Why this route exists next to `GET /v1/worker/prompt`
///
/// The payload is the same and the engine call is the same; the *gate* is
/// what differs. `/v1/worker/prompt` is held by a worker `CapToken` (or a
/// `wh-` handle) minted at dispatch and bound to one task — a SubAgent's
/// own credential, which an Assignee does not have and must not be issued.
/// **W5** nevertheless requires the Assignee to be able to read the
/// material. So the worker route is left exactly as it is, and this one
/// answers the same question under **D3**'s bearer. Widening the worker
/// gate to admit operator tokens would have been the smaller diff and the
/// wrong one: it would make one credential check answer for two different
/// principals, and a bug in it would hand every operator token the
/// worker's `EmitOutput` / `PostResult` surface too.
///
/// # Why the `Final` is presence and flag, never the value
///
/// `model.md:378-379` says what the reader is deciding: *"値があるのに
/// 再実行して副作用を二重にする / 値が無いのに完了扱いにする、のどちらも
/// 起こしうる。判断は必ず状態を見た Assignee が行う"*. Both mistakes are
/// avoided by knowing **whether** a value exists; neither needs the value.
/// The body is a `ContentRef` — inline JSON of any size, or a file
/// reference whose contents are not the server's to inline — so returning
/// it would put an unbounded payload on the axis whose whole job is to be
/// cheap enough to read constantly, and would do it in a response that
/// already carries a full prompt. The existing OUTPUT surfaces serve the
/// value to a reader that has decided it needs it.
///
/// # Status codes
///
/// - `401` — no Bearer, or one no live session matches.
/// - `400` — the id is not a `RunId`.
/// - `404` — no Run with this id; or the step is unknown to the engine; or
///   the step's own context names a different Run.
/// - `200` — the material.
pub async fn run_step_material(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<StepMaterialQuery>,
    headers: HeaderMap,
) -> Response {
    if let Err(resp) = authorize_any_operator(&state, &headers).await {
        return resp;
    }
    match run_step_material_inner(&state, id, query.step_id).await {
        Ok(body) => (StatusCode::OK, Json(body)).into_response(),
        Err(error) => error.into_response(),
    }
}

async fn run_step_material_inner(
    state: &AppState,
    id: String,
    step_id: StepId,
) -> Result<StepMaterialResp, ApiError> {
    let run_id =
        RunId::parse(id).map_err(|e| ApiError::bad_request(format!("invalid run id: {e}")))?;
    // Prove the Run exists before answering about one of its steps, so an
    // id typo is a 404 here as it is on every other `/v1/runs/:id/*`.
    state
        .run_store
        .get(&run_id)
        .await
        .map_err(map_run_store_err)?;

    let mut payload = state
        .engine
        .fetch_worker_payload_trusted(&step_id)
        .await
        .map_err(|e| ApiError::not_found(format!("step {step_id} has no material: {e}")))?;

    // The payload names its own Run when the dispatch carried a context
    // view. When it does and the names differ, this is a step of some
    // other Run and the answer is a miss rather than a cross-Run read.
    let declared = payload
        .context
        .as_ref()
        .and_then(|context| context.run_id.clone());
    let (run_link, note) = match declared {
        Some(declared) if declared == run_id.to_string() => (RunLink::Confirmed, None),
        Some(declared) => {
            return Err(ApiError::not_found(format!(
                "step {step_id} belongs to run {declared}, not {run_id}"
            )));
        }
        None => (
            RunLink::Unconfirmed,
            Some(format!(
                "step {step_id} carries no context view, so its membership of run {run_id} could \
                 not be confirmed; the material is the step's own either way"
            )),
        ),
    };

    crate::worker::assemble_step_pointers(state, &mut payload).await;
    let found = final_of(state, &step_id, payload.attempt).await;

    Ok(StepMaterialResp {
        run_id,
        run_link,
        note,
        payload,
        final_present: found.is_some(),
        final_ok: found,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mlua_swarm::store::run::Assignee;
    use std::collections::BTreeMap;

    fn assignee(op: &str, gen: u64) -> Assignee {
        Assignee {
            op: op.to_string(),
            desc: "took the seat".to_string(),
            gen,
        }
    }

    /// The seat list is the union of the declared seats and the held ones,
    /// so a declared seat nobody holds is present and says `vacant: true`.
    #[test]
    fn a_declared_seat_with_no_holder_is_a_vacant_entry() {
        let mut current: BTreeMap<String, Assignee> = BTreeMap::new();
        current.insert("phase-a-op".to_string(), assignee("S-1", 1));
        let declared: BTreeSet<String> = ["phase-a-op", "phase-b-op"]
            .into_iter()
            .map(str::to_string)
            .collect();

        let mut names: BTreeSet<String> = current.keys().cloned().collect();
        names.extend(declared.iter().cloned());
        let seats: Vec<RunSeat> = names
            .into_iter()
            .map(|slot| {
                let holder = current.get(&slot).cloned();
                RunSeat {
                    vacant: holder.is_none(),
                    declared: declared.contains(&slot),
                    holder,
                    slot,
                }
            })
            .collect();

        assert_eq!(seats.len(), 2);
        assert_eq!(seats[0].slot, "phase-a-op");
        assert!(!seats[0].vacant);
        assert_eq!(seats[1].slot, "phase-b-op");
        assert!(seats[1].vacant);
        assert!(seats[1].holder.is_none());
        assert!(seats[1].declared);
    }

    /// A vacant seat serializes its emptiness rather than dropping the
    /// keys — the objection the whole holder list exists to answer.
    #[test]
    fn a_vacant_seat_serializes_its_holder_as_an_explicit_null() {
        let seat = RunSeat {
            slot: "phase-b-op".to_string(),
            vacant: true,
            holder: None,
            declared: true,
        };
        let value = serde_json::to_value(&seat).expect("serialize");
        assert_eq!(value["vacant"], true);
        assert!(
            value.get("holder").is_some() && value["holder"].is_null(),
            "the holder key must be present and null, not absent: {value}"
        );
    }
}
