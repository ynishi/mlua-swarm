//! `WSOperatorSession`: 1 sid = 1 session = 3 traits co-hosted (`SeniorBridge` /
//! `SpawnHook` / `Operator`). Registered simultaneously into 3 registries under
//! the same sid — the canonical pattern where 1 WS connection covers all 3
//! faces of the Operator role (judgment / observation / execution).
//!
//! `tx` is a `Mutex<Option<Sender>>`: `None` on disconnect, swappable to
//! `Some(new_tx)` on reconnect. The `pending` `HashMap` persists on the session
//! side, so a client holding answer/ack values across a disconnect can reconnect
//! and resend them.
//!
//! Disconnect and teardown are separate events. Clearing `tx` says "this
//! socket went away, a reconnect may follow"; `WSOperatorSession::close`
//! says "this session is over" and pushes that fact out to the socket
//! itself (see its doc).
//!
//! ## Reconnect-wait on the send path (issue abcb43e2)
//!
//! A reply-expecting send (`ask` / `hook_before` / `Operator::execute`) issued
//! while `tx` is `None` does **not** fail: it parks on [`ConnState`] until a
//! reconnect swaps a sender back in, then sends. Delivery is what this path
//! owes its caller, and a disconnect window is not a reason to kill a step —
//! an unanswered step is stopped, not broken. Consequences of that choice:
//!
//! - There is **no send queue / buffer + flush**: the caller's own task is the
//!   queue, and each parked send re-reads `tx` when it wakes.
//! - There is **no wait deadline**. A parked send has exactly two exits: a
//!   reconnect ([`ConnState::Connected`]) or session teardown
//!   ([`ConnState::TornDown`], published by [`WSOperatorSession::fail_pending`]),
//!   which fails it loud. Bounding the wait is infra's call, not this layer's.
//! - A frame that *waited* is refreshed before it goes out. The wait has no
//!   deadline but the capability inside a Spawn frame does, so
//!   [`WSOperatorSession::refresh_parked_frame`] re-mints a worker token
//!   that would otherwise arrive expired. Nothing else in any frame ages.
//! - `after` (fire-and-forget, [`WSOperatorSession::send_oneway`]) is
//!   deliberately excluded — it has no reply to wait for, and parking it
//!   would stall the step's completion rather than a reply (see
//!   [`SpawnHook::after`]'s doc on this type). It keeps its
//!   drop-on-disconnect behaviour, now with a `warn!` naming what was lost.
//!
//! For the detailed S↔C message flow, see the overview figure in `mod.rs`.

use async_trait::async_trait;
use mlua_swarm::core::agent_context::{AgentContextView, PROJECTION_PLACEMENT_KEY};
use mlua_swarm::core::projection::{
    FileProjectionAdapter, ProjectionAdapter, ProjectionKey, ProjectionRef,
};
use mlua_swarm::core::projection_placement::ProjectionPlacement;
use mlua_swarm::{
    CapToken, Ctx, Operator, RunId, SeniorBridge, SessionId, SpawnHook, StepId, WorkerBinding,
    WorkerError, WorkerResult,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch, Mutex};

use super::protocol::{current_parent_req_id, PendingReply, ServerMsg};

/// Observable connectivity of a [`WSOperatorSession`], published on a
/// `watch` channel so a send parked during a disconnect can be woken.
///
/// `Connected` / `Disconnected` mirror `tx` being `Some` / `None` and are
/// published under the same `tx` lock the swap happens in, so a reader that
/// observes `Connected` can rely on `tx` having been `Some` at that instant.
///
/// `TornDown` is **terminal**: once published it is never overwritten (see
/// [`WSOperatorSession::publish_conn`]). Teardown calls `fail_pending` and
/// *then* `clear_tx`; without the sticky rule that second call would demote
/// the state back to `Disconnected` and re-park the very sends teardown just
/// woke.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnState {
    /// `tx` holds a live sender.
    Connected,
    /// `tx` is `None` — the client may still reconnect and swap one back in.
    Disconnected,
    /// The session was torn down (`DELETE /v1/operators/:sid`) and removed
    /// from `operator_sessions`; no reconnect can find it again.
    TornDown,
}

/// Which reply-expecting verb parked a [`PendingEntry`].
///
/// The `req_id` already spells this out in an infix (`-ask-` / `-hb-` /
/// `-spawn-`), but that is a string built for the wire's benefit, and
/// reading a scope decision back out of it would make the format
/// load-bearing. The verb is known at the call site; it is recorded there.
///
/// Public because it rides out on
/// [`OperatorAdapter::pending_for_run`](crate::operator_ws::OperatorAdapter::pending_for_run):
/// the un-answered list names the verb that is waiting, and **W3** is the
/// reason that is safe to publish — it distinguishes *which question was
/// asked*, never how far along the answering is. There is no
/// "sent" / "not yet sent" axis here to leak.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum PendingKind {
    /// [`SeniorBridge::ask`] — an escalation question. The one verb with
    /// no `Ctx`, hence no Run (see [`PendingEntry::run_id`]).
    Ask,
    /// [`SpawnHook::before`] — the pre-spawn ack.
    HookBefore,
    /// [`Operator::execute`] — the Spawn itself.
    Spawn,
}

/// One in-flight request awaiting its `ClientMsg`: the reply channel plus
/// the scope needed to select it later.
///
/// # Why there is no `generation` here
///
/// The obvious fourth scope field would be the generation the dispatch was
/// addressed under, and it is deliberately absent. A generation is an
/// `Assignee` field, and **T1** stops the `Assignee` at the SAP: the
/// delegation into this object passes exactly [`Operator::execute`]'s five
/// arguments, so nothing below the boundary is ever *told* which
/// generation addressed it (see the `router` module doc). Recording one
/// here would require inventing a way to smuggle it down, which is the one
/// thing that layering forbids.
///
/// Nothing is lost by the omission: **A6** — the rule a generation would
/// serve — is enforced above the boundary, where
/// `AssigneeRouter::execute` re-reads `Run.current` after the adapter
/// answers and refuses a reply whose generation has moved. A reader that
/// needs a generation next to one of these entries (the "what is in
/// flight" view) joins it from `Run.current`, which is where the fact
/// lives.
///
/// # Why there is no `OperatorId` here either
///
/// The only value this side could record is `self.sid` — the insert point
/// knows nothing else. But `Assignee.op` shares one key space with role
/// aliases (`main-ai`), so a holder assigned by role would never match a
/// session's own sid and a discard addressed by string would silently miss
/// exactly the entries it was aimed at. Hence `T-DISCARD` is addressed to
/// the **adapter instance** ([`crate::operator_ws::OperatorAdapter`]),
/// resolved from `Assignee.op` by the caller: from below the boundary
/// there is only ever one operator — this one.
pub(super) struct PendingEntry {
    /// Resolved by [`WSOperatorSession::resolve_pending`] when the
    /// matching `answer` / `hook_ack` / `spawn_ack` arrives; dropped
    /// (which fails the waiter) by [`WSOperatorSession::fail_pending`] and
    /// [`WSOperatorSession::discard_pending_requests`].
    reply: oneshot::Sender<PendingReply>,
    /// The Run this request belongs to, when the calling verb carried a
    /// `Ctx` to read it off (`ctx.meta.runtime["run_id"]`).
    ///
    /// `None` for [`PendingKind::Ask`], whose trait method takes no `Ctx`
    /// at all — and for a `Ctx` that carries no `RunContext`. An entry
    /// with no Run cannot be selected by one, which is what
    /// [`WSOperatorSession::discard_pending_requests`] documents as its
    /// shortfall.
    run_id: Option<RunId>,
    /// The step this request is for. Always known: every one of the three
    /// verbs is addressed at a step.
    step_id: StepId,
    /// The attempt number, from the `Ctx`. `None` for
    /// [`PendingKind::Ask`], same reason as [`Self::run_id`].
    attempt: Option<u32>,
    /// Which verb parked this entry.
    kind: PendingKind,
}

impl PendingKind {
    /// Wire/log label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ask => "ask",
            Self::HookBefore => "hook_before",
            Self::Spawn => "spawn",
        }
    }
}

impl PendingEntry {
    /// One-line "what this request was" label — verb, step, and attempt.
    ///
    /// Every scope field the entry carries is legible from a log line
    /// this way, which is what makes a discard or a teardown say *what*
    /// it dropped rather than only how many. The Run is not repeated
    /// here: both callers already log it as its own field (a discard is
    /// addressed at one Run, and a teardown drops every Run at once).
    fn label(&self) -> String {
        match self.attempt {
            Some(attempt) => format!("{}:{} attempt {attempt}", self.kind.as_str(), self.step_id),
            None => format!("{}:{}", self.kind.as_str(), self.step_id),
        }
    }
}

/// Comma-joined [`PendingEntry::label`]s, for the one log line a drop
/// (teardown or discard) writes about what it took down.
fn labels(entries: &[(String, PendingEntry)]) -> String {
    entries
        .iter()
        .map(|(_, entry)| entry.label())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The scope half of a [`PendingEntry`], assembled by each verb before it
/// hands its message to [`WSOperatorSession::send_and_await`].
///
/// Separate from the entry itself because the reply channel is created
/// inside `send_and_await` and never travels: a caller supplies what only
/// it knows (its `Ctx`), and the send path supplies the rest.
pub(super) struct PendingScope {
    /// See [`PendingEntry::run_id`].
    run_id: Option<RunId>,
    /// See [`PendingEntry::step_id`].
    step_id: StepId,
    /// See [`PendingEntry::attempt`].
    attempt: Option<u32>,
    /// See [`PendingEntry::kind`].
    kind: PendingKind,
}

impl PendingScope {
    /// The scope of a request issued with a `Ctx` in hand
    /// ([`SpawnHook::before`] / [`Operator::execute`]).
    ///
    /// `run_id` comes off `ctx.meta.runtime["run_id"]`, the same key the
    /// router resolves a holder by, and a value that is missing or not a
    /// `RunId` degrades to `None` rather than failing the send: a dispatch
    /// launched without a `RunContext` is legal (see
    /// `Engine::dispatch_attempt_with`), and the only cost of not knowing
    /// its Run is that a Run-scoped discard cannot select it.
    fn from_ctx(ctx: &Ctx, kind: PendingKind) -> Self {
        Self {
            run_id: ctx
                .meta
                .runtime
                .get("run_id")
                .and_then(|v| v.as_str())
                .and_then(|raw| RunId::parse(raw).ok()),
            step_id: ctx.task_id.clone(),
            attempt: Some(ctx.attempt),
            kind,
        }
    }

    /// The scope of a [`SeniorBridge::ask`], which is handed a `StepId`
    /// and nothing else.
    ///
    /// The trait signature is **not** widened to take a `Ctx`: it belongs
    /// to the engine, and changing it to give this map one more field
    /// would be a contract change for every `SeniorBridge` implementor,
    /// paid for a discard the model does not address at `ask` in the first
    /// place.
    fn from_step(step_id: StepId) -> Self {
        Self {
            run_id: None,
            step_id,
            attempt: None,
            kind: PendingKind::Ask,
        }
    }
}

/// Reissues a `Role::Worker` capability that is about to be delivered late.
///
/// The park in [`WSOperatorSession::send_when_connected`] has no deadline,
/// and the Spawn frame it holds was built before the wait began — token
/// included. That token's TTL (`EngineCfg::worker_token_ttl_secs`,
/// default 1800s) has been running since the engine minted it, so a park
/// longer than the TTL used to hand the SubAgent a capability that was
/// already expired: it did the whole job and failed on the last call. The
/// session refreshes the frame instead, through this seam.
///
/// A trait rather than an `Engine` field so the session's dependency is
/// the one operation it actually needs — and so a test can hand it a
/// minter that counts calls without standing up an engine.
#[async_trait]
pub(crate) trait WorkerTokenMinter: Send + Sync {
    /// Mint a replacement for `expiring`, carrying the same grant with a
    /// fresh expiry. `Err` is the human-readable reason; the caller logs
    /// it and sends the frame it already has.
    async fn remint_worker_token(&self, expiring: &CapToken) -> Result<CapToken, String>;
}

/// The production minter. `Engine::remint_worker_token` is the authority —
/// it re-derives every field of the reissue from the record it already
/// holds, so this impl adds nothing but the error rendering.
#[async_trait]
impl WorkerTokenMinter for mlua_swarm::Engine {
    async fn remint_worker_token(&self, expiring: &CapToken) -> Result<CapToken, String> {
        mlua_swarm::Engine::remint_worker_token(self, expiring)
            .await
            .map_err(|e| e.to_string())
    }
}

/// How much life a parked frame's capability must have left for the frame
/// to go out unchanged.
///
/// The refresh is not unconditional: re-minting on every wake would put a
/// token record in engine state per disconnect flap, for frames whose
/// token has 29 minutes left. Below this margin the token is reissued, and
/// since a reissue carries the full TTL, a flapping client re-mints at
/// most once per TTL rather than once per flap.
///
/// 60s is sized against what has to happen after the send: the MainAI
/// relays the frame, a SubAgent starts, fetches its prompt and submits.
/// A token with less than a minute left would not survive that, so
/// delivering one is the same failure as delivering an expired one — just
/// later.
const PARKED_TOKEN_MIN_REMAINING_SECS: u64 = 60;

/// 1 sid = 1 session. Looked up by sid in the `operator_sessions` store on reconnect.
pub struct WSOperatorSession {
    sid: SessionId,
    /// The current mpsc sender on the write path. `None` on disconnect;
    /// swapped to `Some(new_tx)` on reconnect.
    tx: Mutex<Option<mpsc::UnboundedSender<ServerMsg>>>,
    /// Connectivity broadcast for [`Self::send_when_connected`]'s park.
    /// Written only via [`Self::publish_conn`], always while the `tx` lock
    /// is held, so the pair stays consistent for a parked reader.
    conn: watch::Sender<ConnState>,
    /// `req_id` → the in-flight request it identifies. Resolved when
    /// `answer` / `hook_ack` / `spawn_ack` arrives.
    ///
    /// The key stays the `req_id` — that is what the wire carries back
    /// ([`super::protocol::ClientMsg`] has no other correlator, and
    /// `ServerMsg::Spawn` sends no `run_id` out) — and the scope needed to
    /// select entries by anything else rides in the value. Recording it at
    /// insert time is the only opportunity there is: nothing on the ack
    /// path could tell a reader which Run a `req_id` belonged to.
    pending: Mutex<HashMap<String, PendingEntry>>,
    /// Server-initiated close signal for whichever socket is pumping this
    /// session. `None` = none requested; `Some(reason)` is latched by
    /// [`Self::close`] and never cleared — a session is torn down once.
    /// The pump subscribes via [`Self::close_signal`].
    close: watch::Sender<Option<Arc<str>>>,
    /// Public HTTP base URL the server is reachable at (from
    /// `AppState.base_url`, sourced from the binary at boot time).
    /// Rendered literally into the Spawn `directive`'s `base_url` line
    /// when `Some`; `None` falls back to a `mse_doctor`-pointer
    /// placeholder (issue #8).
    base_url: Option<std::sync::Arc<str>>,
    /// Refreshes a parked Spawn frame's worker capability before it goes
    /// out — see [`WorkerTokenMinter`].
    ///
    /// `None` for a session built without one (the test constructors, and
    /// any future caller that has no engine to hand): the frame is then
    /// sent exactly as it was built, which is the pre-refresh behaviour.
    /// It is not an error state, so it is not logged as one; what *is*
    /// logged is a refresh that was wanted and failed.
    token_minter: Option<Arc<dyn WorkerTokenMinter>>,
}

impl WSOperatorSession {
    /// Test-only shorthand for "a session that already has a sender".
    ///
    /// Production has no such constructor any more: every session is born
    /// disconnected via [`Self::disconnected_with_base_url`] — at mint
    /// (`login::operators_create`) or at boot
    /// (`login::restored_login_session`) — and acquires its sender
    /// through [`Self::replace_tx`] when a socket attaches. This used to be
    /// the first-connect constructor in `login::handle_operator_socket`;
    /// that arm registered the session it built, which is exactly what a
    /// connect racing a teardown must not be able to do.
    ///
    /// `base_url` is the server's public HTTP root (e.g.
    /// `"http://127.0.0.1:7777"`), threaded from `AppState.base_url`.
    /// When `Some`, it is rendered literally into Spawn directives
    /// (issue #8); `None` falls back to a `mse_doctor`-pointer
    /// placeholder.
    #[cfg(test)]
    pub(super) fn new_with_base_url(
        sid: SessionId,
        tx: mpsc::UnboundedSender<ServerMsg>,
        base_url: Option<std::sync::Arc<str>>,
    ) -> Self {
        Self {
            sid,
            tx: Mutex::new(Some(tx)),
            conn: watch::Sender::new(ConnState::Connected),
            pending: Mutex::new(HashMap::new()),
            close: watch::Sender::new(None),
            base_url,
            token_minter: None,
        }
    }

    /// Constructor for a session that exists with no socket behind it yet:
    /// the boot-time restore path (`OperatorSessionPersistence::restore`),
    /// which puts a persisted login record into the engine's three
    /// registries before its owning client has reconnected.
    ///
    /// `tx: None` is the very state a live session falls into on
    /// [`Self::clear_tx`] / [`Self::clear_tx_if`], so nothing downstream
    /// needs a new branch: it starts [`ConnState::Disconnected`], which is
    /// what a reply-expecting send parks on until the client's first
    /// connect swaps a sender in (issue abcb43e2), and
    /// [`Self::is_connected`] answers `false` meanwhile. Being registered
    /// and being reachable stay separate facts — that first connect walks
    /// `login::handle_operator_socket`'s existing reconnect arm and only
    /// swaps the sender in ([`Self::replace_tx`]).
    ///
    /// # What the park still covers, and what **A7** took out of it
    ///
    /// A reply-expecting send issued while `tx` is `None` parks rather than
    /// failing, and that is still the wanted shape for the paths that reach
    /// this object **directly by sid**: the `SeniorBridge` and `SpawnHook`
    /// registrations (`login::register_operator_session`) hand the engine
    /// this session itself, so a dispatch through them waits for the
    /// client's first connect instead of failing on the gap between boot
    /// and reconnect. It also covers a dispatch that was already admitted —
    /// once a holder answered `Connected` at reference time, a socket lost
    /// *afterwards* parks the in-flight send below the boundary (**T2**)
    /// until the client returns or teardown publishes
    /// [`ConnState::TornDown`].
    ///
    /// It does **not** cover a dispatch routed through the assignee. That
    /// path (`AssigneeRouter::execute`) pulls `T-ALIVE` before delegating,
    /// and a restored session is registered while disconnected — so **A7**
    /// releases the seat and fails the dispatch as Vacant *before* anything
    /// reaches this object. A Run whose seat is held by a restored session
    /// therefore does not wait for its operator to come back: its first
    /// dispatch empties the seat, and an `acquire`
    /// (`POST /v1/runs/:id/acquire`, **A8**) is what puts a reachable
    /// holder back. That is **A7** as specified — the state is examined at
    /// reference time, with no grace window (**T7**) — and it is the reason
    /// this paragraph no longer promises parking for pinned runs.
    ///
    /// `send_oneway` (`after`) still drops during the gap, as it does on
    /// any disconnect.
    ///
    /// `token_minter` is the seam a parked Spawn frame's capability is
    /// refreshed through ([`WorkerTokenMinter`]); pass the engine that
    /// minted the token, or `None` to send parked frames exactly as built.
    pub(super) fn disconnected_with_base_url(
        sid: SessionId,
        base_url: Option<std::sync::Arc<str>>,
        token_minter: Option<Arc<dyn WorkerTokenMinter>>,
    ) -> Self {
        Self {
            sid,
            tx: Mutex::new(None),
            conn: watch::Sender::new(ConnState::Disconnected),
            pending: Mutex::new(HashMap::new()),
            close: watch::Sender::new(None),
            base_url,
            token_minter,
        }
    }

    /// Publishes a connectivity transition to every parked send.
    ///
    /// Two rules, both load-bearing:
    /// - [`ConnState::TornDown`] is terminal — a later `Disconnected` from
    ///   teardown's own `clear_tx` must not re-park sends teardown just woke.
    /// - An unchanged state is not re-published; `Connected` and `tx` being
    ///   `Some` are kept in lockstep by every caller holding the `tx` lock
    ///   across this call.
    fn publish_conn(&self, next: ConnState) {
        self.conn.send_if_modified(|current| {
            if *current == ConnState::TornDown || *current == next {
                false
            } else {
                *current = next;
                true
            }
        });
    }

    /// Swaps in a new tx on reconnect. Expected to be called only from the handler side.
    ///
    /// This is what unparks sends waiting in [`Self::send_when_connected`],
    /// which is why that park must never hold the `tx` lock across its wait —
    /// it would deadlock right here.
    pub(super) async fn replace_tx(&self, new_tx: mpsc::UnboundedSender<ServerMsg>) {
        let mut current = self.tx.lock().await;
        *current = Some(new_tx);
        self.publish_conn(ConnState::Connected);
    }

    /// Whether this session currently has a live WebSocket sender.
    pub(super) async fn is_connected(&self) -> bool {
        self.tx.lock().await.is_some()
    }

    /// Clear the sender only when it still belongs to the connection that is
    /// shutting down. A stale socket can finish after a reconnect installed a
    /// replacement sender; that stale cleanup must not disconnect the new
    /// socket.
    pub(super) async fn clear_tx_if(&self, expected: &mpsc::UnboundedSender<ServerMsg>) {
        let mut current = self.tx.lock().await;
        if current
            .as_ref()
            .is_some_and(|sender| sender.same_channel(expected))
        {
            *current = None;
            self.publish_conn(ConnState::Disconnected);
        }
    }

    /// Clears tx to `None` on disconnect. Expected to be called only from the handler side.
    pub(crate) async fn clear_tx(&self) {
        let mut current = self.tx.lock().await;
        *current = None;
        self.publish_conn(ConnState::Disconnected);
    }

    /// Ask the socket currently pumping this session to shut down, carrying
    /// `reason` to the client.
    ///
    /// Clearing `tx` is not enough to end a connection: the pump
    /// (`login::handle_operator_socket`) holds its own clone of the sender,
    /// so the mpsc channel — and with it the write task and the socket —
    /// stays alive even after the session has let go. A client torn down by
    /// a third party (`DELETE /v1/operators/by-role/:role`) was left holding
    /// a socket nothing would ever speak on again, with no error to notice:
    /// its session was already out of `operator_sessions`, so no frame could
    /// be routed to it and no reconnect could find it.
    ///
    /// Latching this signal is what the pump watches; it answers by sending
    /// a **WS Close frame** (a standard disconnect every client already
    /// understands — no new `ServerMsg` variant, so the wire shape is
    /// unchanged) and ending both of its tasks.
    ///
    /// Called on **teardown** only. A plain disconnect must not fire it:
    /// the two are separate events, and a reconnect is legitimate after the
    /// second but impossible after the first.
    pub(crate) fn close(&self, reason: &str) {
        self.close.send_replace(Some(Arc::from(reason)));
    }

    /// Subscribe to [`Self::close`]. Because the signal is latched rather
    /// than edge-triggered, a subscriber that arrives *after* the teardown
    /// still observes it (see `login::close_requested`) — which is what a
    /// socket that connected while its session was being torn down needs.
    pub(super) fn close_signal(&self) -> watch::Receiver<Option<Arc<str>>> {
        self.close.subscribe()
    }

    /// Drains every in-flight `pending` entry, dropping its oneshot
    /// `Sender`. Dropping the sender closes the reply channel, so the
    /// matching `send_and_await` unparks from `orx.await` with an `Err`
    /// immediately — an in-flight `ask` / `hook_before` / `spawn` fails
    /// loud right away (for `Operator::execute`, a `WorkerError::Failed`)
    /// instead of orphaning in `orx.await` until the run's sync timeout
    /// (up to 300s) fires. `reason` is recorded for the log only — a
    /// dropped `oneshot::Sender` can carry no payload, so the receiver
    /// observes the generic "reply path closed" error.
    ///
    /// It also publishes [`ConnState::TornDown`] **first**, which is the
    /// other half of the guarantee: a send parked in
    /// [`Self::send_when_connected`] has not reached `orx.await` yet, so
    /// draining `pending` alone would not touch it and it would wait for a
    /// reconnect that can never come (issue abcb43e2). Publishing before
    /// draining also removes the register-vs-drain race — `TornDown` is
    /// terminal, so a send that registers after the drain still observes it
    /// and fails itself.
    ///
    /// Called on **session teardown** (`DELETE /v1/operators/:sid`), where
    /// the session is being removed from `operator_sessions` and can never
    /// be reconnected — so the disconnect-survives-in-`pending`
    /// reconnect/resend contract (see the module doc) does not apply. It is
    /// deliberately NOT called on a plain WS disconnect, which keeps
    /// `pending` alive for a reconnecting client to resend against.
    ///
    /// # Why this was not removed
    ///
    /// The Operator-lifecycle teardown listed this for removal, with **W2**
    /// as the replacement: the server resolves nothing on its own, an
    /// unanswered Step stays unanswered, and the next Assignee answers it by
    /// `req_id`. Its prerequisite landed — a [`PendingEntry`] now records
    /// run / step / attempt, so an un-answered request is nameable. It is
    /// still kept, for two reasons that are about teardown specifically.
    ///
    /// **W2's answer path cannot reach a pre-send park.** Publishing
    /// [`ConnState::TornDown`] is not the same service as draining
    /// `pending`. An entry registers in [`Self::send_and_await`] *before*
    /// the send, so a request parked in [`Self::send_when_connected`] is
    /// already listed by [`Self::pending_for_run`] (**W3** — unsent and
    /// sent are one state) but its task is blocked one step earlier than
    /// `orx.await`. Answering it through [`Self::resolve_pending`] puts the
    /// reply in a `oneshot` nobody is receiving yet and leaves the task
    /// waiting on a `watch` whose last publisher just went away with the
    /// session. Teardown removes the sid from `operator_sessions`, so the
    /// reconnect that is the park's only other exit can never arrive
    /// either. Removing this call would therefore not hand those sends to
    /// the next Assignee; it would leave them parked in silence until the
    /// run's TTL — the exact outcome the park was introduced (abcb43e2) to
    /// avoid.
    ///
    /// **Dropping only the drain would not implement W2 either.** For the
    /// next Assignee to answer, it must be able to *read* the outstanding
    /// requests, and both ways in — `pending_for_run` and
    /// [`Self::discard_pending_requests`] — are reached through
    /// `AppState.operator_adapters`, keyed by sid. `teardown_operator_session`
    /// unregisters that adapter and drops the session from
    /// `operator_sessions` before calling this. After teardown the map is
    /// unreachable by any name, so an un-drained entry is not "waiting for
    /// the next Assignee", it is orphaned: the trade would be a prompt loud
    /// failure for a silent wait on the sync ceiling.
    ///
    /// The W2-shaped path for a live operator losing work already exists
    /// and is the sibling below: `discard_pending_requests`, the handover
    /// (**A8**) case, where the session survives, the caller has read the
    /// `req_id`s first, and the seat's new holder can answer them. Teardown
    /// is not a handover — the operator is gone, not displaced. Removing
    /// this would need `pending` to outlive its session under a key
    /// something still resolves (a Run-scoped index rather than a
    /// session-scoped map), which is a different change from the one the
    /// removal list assumed.
    pub(crate) async fn fail_pending(&self, reason: &str) {
        // Wake the pre-send parks before draining the post-send ones.
        self.publish_conn(ConnState::TornDown);
        let drained: Vec<(String, PendingEntry)> = self.pending.lock().await.drain().collect();
        if !drained.is_empty() {
            tracing::warn!(
                sid = %self.sid,
                count = drained.len(),
                requests = %labels(&drained),
                reason,
                "ws operator session: failing in-flight pending replies"
            );
        }
        // `drained` drops here: each `oneshot::Sender` closes its channel,
        // unparking the corresponding `send_and_await` with an `Err`.
    }

    /// `T-DISCARD.request(operator, run)` → `T-DISCARD.confirm(run,
    /// discarded)` (model §4.7), as the session performs it: drop the
    /// in-flight requests `req_ids` names, and answer with how many there
    /// were.
    ///
    /// # Why a list of names and not a Run
    ///
    /// This used to select on `run_id` alone, which is one step coarser
    /// than what an acquire takes. A Run has several Operator seats, one
    /// session can hold more than one of them (its sid *and* each of its
    /// roles resolve to this same object — `login::register_operator_session`),
    /// and an acquire displaces exactly one. Selecting by Run therefore
    /// dropped the reply channels of work still in flight on a seat this
    /// session had not lost — and **A6** does not catch that afterwards,
    /// because it is enforced per slot and the untouched seat's generation
    /// never moved.
    ///
    /// The seat is not knowable from here — it is an `Assignee` term, and
    /// **T1** keeps those above the SAP — so the caller makes the
    /// selection and names it in this session's own correlator space. See
    /// [`OperatorAdapter::discard_requests`](crate::operator_ws::OperatorAdapter::discard_requests)
    /// for why that is the narrow way round.
    ///
    /// **`run_id` is still checked**, per name: a request is dropped only
    /// if it is *both* named and recorded against `run_id`. The caller
    /// selected from a read that has since had time to go stale, and a
    /// `req_id` that has moved on is not a licence to drop whatever now
    /// answers to it.
    ///
    /// # This is not a teardown
    ///
    /// [`Self::fail_pending`] drains the whole map and publishes
    /// [`ConnState::TornDown`], because it is called when the session is
    /// being removed and can never be reconnected. This is the opposite
    /// situation: the operator was displaced from one seat (**A8**) and
    /// goes on holding whatever else it holds, so nothing is published,
    /// no unnamed entry is touched, and a send parked waiting for a
    /// reconnect on another seat or another Run stays parked. What the
    /// displaced holder loses is exactly the work that is no longer its
    /// own.
    ///
    /// Each dropped `oneshot::Sender` closes its channel, so the matching
    /// `send_and_await` unparks from `orx.await` with an `Err` — the same
    /// mechanism teardown uses, and the reason a discarded spawn surfaces
    /// promptly as a failed step rather than orphaning until the launch
    /// ceiling fires.
    ///
    /// # What it cannot reach
    ///
    /// Entries with no Run — [`PendingKind::Ask`], whose trait method is
    /// handed no `Ctx` (see [`PendingScope::from_step`]), and any dispatch
    /// launched without a `RunContext` — fail the `run_id` re-check and
    /// are **not** discarded, whatever the caller names. Selecting them
    /// would mean guessing, and an escalation question dropped because it
    /// *might* have belonged to the Run is worse than one that outlives
    /// it: the far end is still free to answer, and an answer that
    /// arrives for a seat since re-acquired is refused above the boundary
    /// by **A6**. So the shortfall is a stale question, not a double one.
    pub(crate) async fn discard_pending_requests(
        &self,
        run_id: &RunId,
        req_ids: &[String],
    ) -> usize {
        let discarded: Vec<(String, PendingEntry)> = {
            let mut pending = self.pending.lock().await;
            let mut taken = Vec::new();
            for req_id in req_ids {
                let belongs = pending
                    .get(req_id)
                    .is_some_and(|entry| entry.run_id.as_ref() == Some(run_id));
                if belongs {
                    if let Some(entry) = pending.remove(req_id) {
                        taken.push((req_id.clone(), entry));
                    }
                }
            }
            taken
        };
        if !discarded.is_empty() {
            tracing::info!(
                sid = %self.sid,
                %run_id,
                count = discarded.len(),
                named = req_ids.len(),
                requests = %labels(&discarded),
                "T-DISCARD: dropping this session's in-flight requests for a seat it no longer holds"
            );
        }
        discarded.len()
        // `discarded` drops here — outside the `pending` lock, as in
        // `fail_pending`: waking a waiter that immediately re-locks the
        // map while the lock is still held would be a self-inflicted
        // stall.
    }

    /// The set [`Self::discard_pending_requests`] is selected from,
    /// answered instead of acted on: every request this session still owes
    /// `run_id` a reply for. Backs
    /// [`OperatorAdapter::pending_for_run`](crate::operator_ws::OperatorAdapter::pending_for_run)
    /// and, through it, the un-answered Step list (model §4.6 **W5**, axis
    /// 3).
    ///
    /// # Why the reply channel does not come out with it
    ///
    /// The map's value owns a `oneshot::Sender`, which is the *capability
    /// to answer*. What crosses the boundary is a description of the
    /// request — `req_id`, verb, step, attempt — and nothing that could be
    /// used to complete or cancel it. A reader of the list is meant to
    /// decide what to do next (**W1**: the server resolves nothing on its
    /// own); handing it the sender would make "read the list" and "answer
    /// on the operator's behalf" the same call.
    ///
    /// # Waiting is not a state
    ///
    /// Every entry here is waiting, and there is deliberately no field
    /// saying *how* — parked for a reconnect and written to a live socket
    /// are the same value of the same variable (**W3**: 未送信と送信済みを
    /// 別状態にしない). Nor is there a timestamp to compute an age from:
    /// **R5** sets no upper bound on the wait, so an age would exist only
    /// to be compared against a threshold nothing here defines.
    ///
    /// Ordering is by `req_id`, which is stable across reads of an
    /// unchanged map; the map itself is a `HashMap` and would otherwise
    /// answer in a different order every time.
    pub(crate) async fn pending_for_run(
        &self,
        run_id: &RunId,
    ) -> Vec<super::router::PendingRequest> {
        let mut requests: Vec<super::router::PendingRequest> = self
            .pending
            .lock()
            .await
            .iter()
            .filter(|(_, entry)| entry.run_id.as_ref() == Some(run_id))
            .map(|(req_id, entry)| super::router::PendingRequest {
                req_id: req_id.clone(),
                kind: entry.kind,
                step_id: entry.step_id.clone(),
                attempt: entry.attempt,
            })
            .collect();
        requests.sort_by(|a, b| a.req_id.cmp(&b.req_id));
        requests
    }

    /// Resolves the pending oneshot when a `ClientMsg` arrives on the handler's
    /// read task. If `req_id` is not registered, no-op (= silently drops unknown acks).
    ///
    /// An ack for a request [`Self::discard_pending_requests`] has already
    /// dropped lands in that same no-op arm: the entry is gone, so the late
    /// answer is discarded exactly like an unknown one.
    pub(super) async fn resolve_pending(&self, req_id: &str, reply: PendingReply) {
        if let Some(entry) = self.pending.lock().await.remove(req_id) {
            let _ = entry.reply.send(reply);
        }
    }

    /// Inserts an entry into pending, sends S→C (parking until connected if
    /// the client is away), and waits for the reply.
    ///
    /// No deadline on either wait — see the module doc's reconnect-wait
    /// section. The pending entry is registered *before* the send parks and
    /// is kept across the park, so a reply that arrives right after the
    /// reconnect still finds its slot; it is removed only when the send
    /// itself fails and no reply can ever arrive.
    ///
    /// `scope` is what the entry is selectable by afterwards (see
    /// [`PendingEntry`]). It is supplied by the caller because this is the
    /// last place the caller's `Ctx` is still in reach — the ack path that
    /// resolves the entry carries a `req_id` and nothing more.
    async fn send_and_await(
        &self,
        req_id: String,
        msg: ServerMsg,
        scope: PendingScope,
    ) -> Result<PendingReply, String> {
        let (otx, orx) = oneshot::channel::<PendingReply>();
        self.pending.lock().await.insert(
            req_id.clone(),
            PendingEntry {
                reply: otx,
                run_id: scope.run_id,
                step_id: scope.step_id,
                attempt: scope.attempt,
                kind: scope.kind,
            },
        );

        if let Err(e) = self.send_when_connected(msg).await {
            self.pending.lock().await.remove(&req_id);
            return Err(e);
        }

        orx.await
            .map_err(|_| "ws operator: oneshot cancelled (= reply path closed)".to_string())
    }

    /// Sends `msg` on the current write path, parking until a sender exists
    /// when the client is disconnected (issue abcb43e2). Returns `Err` only
    /// when the message can never be delivered: the session was torn down, or
    /// the sender it was handed to was already closed.
    ///
    /// The `tx` guard is taken per attempt and dropped **before** awaiting —
    /// holding it across the wait would deadlock [`Self::replace_tx`], i.e.
    /// the one event this park exists to wait for.
    ///
    /// `conn_rx` is subscribed before the first `tx` read so a reconnect
    /// landing mid-attempt cannot be missed; the `Connected` arm re-loops
    /// instead of waiting for the *next* change, which closes the same
    /// window on the state read.
    ///
    /// # A frame that waited is refreshed before it goes
    ///
    /// The message is built by the caller before the park and could sit
    /// here for the length of a disconnect, so anything time-bounded
    /// inside it is stale by the time a sender appears. Exactly one field
    /// is: `Spawn.capability_token`. After a wait — and only after one —
    /// [`Self::refresh_parked_frame`] re-mints it when it is close enough
    /// to expiry to matter. The refresh runs **outside** the `tx` guard,
    /// for the same reason the park does: it awaits the engine, and
    /// holding the guard across an await would deadlock
    /// [`Self::replace_tx`].
    async fn send_when_connected(&self, mut msg: ServerMsg) -> Result<(), String> {
        let mut conn_rx = self.conn.subscribe();
        let mut waited = false;
        loop {
            if waited {
                self.refresh_parked_frame(&mut msg).await;
                waited = false;
            }
            {
                let guard = self.tx.lock().await;
                if let Some(tx) = guard.as_ref() {
                    return tx
                        .send(msg)
                        .map_err(|_| "ws send channel closed".to_string());
                }
            }
            match *conn_rx.borrow_and_update() {
                ConnState::TornDown => {
                    return Err("ws operator session torn down while a send was parked".to_string());
                }
                // Reconnected between the `tx` read and this state read —
                // retry the send rather than park on an already-seen change.
                ConnState::Connected => continue,
                ConnState::Disconnected => {}
            }
            tracing::debug!(
                sid = %self.sid,
                "ws operator disconnected: parking a send until reconnect"
            );
            if conn_rx.changed().await.is_err() {
                return Err("ws operator: connection state channel closed".to_string());
            }
            waited = true;
        }
    }

    /// Re-mint a parked [`ServerMsg::Spawn`]'s worker capability when it is
    /// within [`PARKED_TOKEN_MIN_REMAINING_SECS`] of expiring.
    ///
    /// Every other frame passes through untouched: `Ask` / `HookBefore` /
    /// `HookAfter` carry nothing that ages, and a Spawn whose token still
    /// has most of its TTL is delivered exactly as it was built.
    ///
    /// # Every failure keeps the original frame
    ///
    /// An undecodable token, no minter wired, or a minter that refuses —
    /// each leaves `msg` alone and lets the send proceed. That is the
    /// pre-refresh behaviour, and it is the right fallback: a token that
    /// might still work beats a spawn failed here on the strength of a
    /// refresh that did not. A refusal is `warn!`-logged with the reason,
    /// because it means the SubAgent is about to receive a capability the
    /// server itself judged too old.
    async fn refresh_parked_frame(&self, msg: &mut ServerMsg) {
        let ServerMsg::Spawn {
            capability_token,
            task_id,
            attempt,
            ..
        } = msg
        else {
            return;
        };
        let Some(minter) = self.token_minter.as_ref() else {
            return;
        };
        let current = match CapToken::decode(capability_token) {
            Ok(token) => token,
            Err(error) => {
                tracing::warn!(
                    sid = %self.sid, %task_id, attempt, %error,
                    "parked spawn: its capability token could not be decoded, so its \
                     remaining life is unknown; sending the frame as it was built"
                );
                return;
            }
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if current.expire_at > now.saturating_add(PARKED_TOKEN_MIN_REMAINING_SECS) {
            return;
        }
        match minter.remint_worker_token(&current).await {
            Ok(fresh) => {
                tracing::info!(
                    sid = %self.sid, %task_id, attempt,
                    "parked spawn: its capability token was re-minted before delivery \
                     (the park outlived the worker-token TTL)"
                );
                *capability_token = fresh.encode();
            }
            Err(error) => tracing::warn!(
                sid = %self.sid, %task_id, attempt, %error,
                "parked spawn: its capability token is at or past expiry and could not be \
                 re-minted; the SubAgent will be handed it anyway and may fail at submit"
            ),
        }
    }

    /// Fire-and-forget send for `after` (= no reply expected).
    ///
    /// Deliberately **not** on the reconnect-wait path
    /// ([`Self::send_when_connected`]): `after` has no reply to wait for,
    /// and parking it would hold the step's completion open for the length
    /// of the disconnect. Issue abcb43e2's scope was the three
    /// reply-expecting verbs (`spawn` / `ask` / `hook_before`); the
    /// question it left open for this one — is dropping acceptable? — is
    /// answered on [`SpawnHook::after`]'s doc for this type. The `Err` is
    /// no longer swallowed there.
    async fn send_oneway(&self, msg: ServerMsg) -> Result<(), String> {
        let guard = self.tx.lock().await;
        match guard.as_ref() {
            Some(tx) => tx
                .send(msg)
                .map_err(|_| "ws send channel closed".to_string()),
            None => Err("ws operator disconnected".to_string()),
        }
    }
}

#[async_trait]
impl SeniorBridge for WSOperatorSession {
    async fn ask(&self, task_id: &StepId, question: Value) -> Result<Value, String> {
        let req_id = format!("{}-ask-{}", self.sid, uuid::Uuid::new_v4());
        let msg = ServerMsg::Ask {
            req_id: req_id.clone(),
            parent_req_id: current_parent_req_id(),
            task_id: task_id.clone(),
            question,
        };
        match self
            .send_and_await(req_id, msg, PendingScope::from_step(task_id.clone()))
            .await?
        {
            PendingReply::Answer(v) => Ok(v),
            PendingReply::HookAck { .. } => {
                Err("ws operator: unexpected hook_ack reply to ask".into())
            }
            PendingReply::SpawnAck { .. } => {
                Err("ws operator: unexpected spawn_ack reply to ask".into())
            }
            PendingReply::SpawnHalt { .. } => {
                Err("ws operator: unexpected spawn_halt reply to ask".into())
            }
        }
    }
}

#[async_trait]
impl SpawnHook for WSOperatorSession {
    async fn before(&self, ctx: &Ctx) -> Result<(), String> {
        let req_id = format!("{}-hb-{}", self.sid, uuid::Uuid::new_v4());
        let msg = ServerMsg::HookBefore {
            req_id: req_id.clone(),
            parent_req_id: current_parent_req_id(),
            task_id: ctx.task_id.clone(),
            agent: ctx.agent.clone(),
            attempt: ctx.attempt,
        };
        match self
            .send_and_await(
                req_id,
                msg,
                PendingScope::from_ctx(ctx, PendingKind::HookBefore),
            )
            .await?
        {
            PendingReply::HookAck { ok: true, .. } => Ok(()),
            PendingReply::HookAck { ok: false, reason } => {
                Err(reason.unwrap_or_else(|| "ws operator: spawn rejected".into()))
            }
            PendingReply::Answer(_) => {
                Err("ws operator: unexpected answer reply to hook_before".into())
            }
            PendingReply::SpawnAck { .. } => {
                Err("ws operator: unexpected spawn_ack reply to hook_before".into())
            }
            PendingReply::SpawnHalt { .. } => {
                Err("ws operator: unexpected spawn_halt reply to hook_before".into())
            }
        }
    }

    /// Fire-and-forget completion notice. A send failure is reported and
    /// dropped — it is **not** parked, and it is no longer silent.
    ///
    /// # Why it is dropped rather than parked
    ///
    /// Its three siblings (`spawn` / `ask` / `before`) park across a
    /// disconnect because each owes its caller a reply. `after` owes
    /// nothing: the server holds no `pending` entry for it, and the frame's
    /// only consumer is the MainAI's `mse_pending_wait` queue, which reads
    /// it and answers nothing (`mse_ack` has no `hook_after` reply to
    /// give). Nothing anywhere holds state on its arrival.
    ///
    /// Parking it would not be free, either. `after` is awaited inside the
    /// completion wrapper `SpawnHookMiddleware` puts around the worker's
    /// join (`mse::middleware`, the `wrap_join` closure that calls
    /// `hook.after` before returning the signal), so a park would hold the
    /// step's completion — and therefore the Run — for the entire
    /// disconnect window, with the same two exits as any other park
    /// (reconnect, or teardown). That trades a lost notification for a
    /// stalled Run, which is a worse failure and a less visible one.
    ///
    /// # What changed
    ///
    /// The drop stays; the silence does not. The result used to be
    /// discarded here *and* at the engine's call site, so a disconnect ate
    /// the notice with no error, no log and no retry — and the operator
    /// had no way to tell "no step finished" from "a step finished while I
    /// was away". A `warn!` naming the step is the whole fix, because the
    /// loss is observability and only observability.
    async fn after(&self, ctx: &Ctx, result: &Value) -> Result<(), String> {
        let req_id = format!("{}-ha-{}", self.sid, uuid::Uuid::new_v4());
        let msg = ServerMsg::HookAfter {
            req_id,
            parent_req_id: current_parent_req_id(),
            task_id: ctx.task_id.clone(),
            agent: ctx.agent.clone(),
            attempt: ctx.attempt,
            result: result.clone(),
        };
        if let Err(error) = self.send_oneway(msg).await {
            tracing::warn!(
                sid = %self.sid,
                task_id = %ctx.task_id,
                agent = %ctx.agent,
                attempt = ctx.attempt,
                %error,
                "hook_after dropped: the operator never learned this step completed \
                 (fire-and-forget, so it is not parked and not retried)"
            );
        }
        Ok(())
    }
}

#[async_trait]
impl Operator for WSOperatorSession {
    /// Thin control channel impl (the Spawn thin-control axis): `system` / `prompt`
    /// have already been baked into engine state on the server side
    /// (= `bake_worker_system_prompt` in `OperatorSpawner.spawn` + the existing
    /// `fetch_prompt` path). This impl encodes `worker_token` and hands it to
    /// the MainAI in a single Spawn message; the SubAgent then hits
    /// `/v1/worker/prompt` + `/v1/worker/result` itself over HTTP. `system` is
    /// intentionally **not used here** (heavy payloads are not carried on WS —
    /// thin-path discipline); `prompt` (issue #18) is used only to recover a
    /// `Value` for the `Spawn.directive` reminder line (see
    /// `default_spawn_directive_with_task_directive`) — the SubAgent still
    /// self-fetches the full prompt over HTTP, unchanged.
    ///
    /// The SubAgent's result post (= HTTP POST `/v1/worker/result`) appends
    /// `Final` to `output_tail`; when the MainAI returns `SpawnAck`, this
    /// `execute` returns `WorkerResult` and control returns to the dispatch path.
    ///
    /// `worker` is required (see `requires_worker_binding`) — the compile-time
    /// gate in `OperatorSpawnerFactory::build` is the primary defense, but a
    /// `None` can still reach here on paths that bypass compilation (e.g. an
    /// operator-sid-pin path). This runtime check is the defensive second
    /// layer: fail the task loud rather than silently degrade to the old
    /// hardcoded `"mse-worker"` literal.
    async fn execute(
        &self,
        ctx: &Ctx,
        _system: Option<String>,
        prompt: Value,
        worker: Option<WorkerBinding>,
        worker_token: CapToken,
    ) -> Result<WorkerResult, WorkerError> {
        let Some(worker) = worker else {
            return Err(WorkerError::Failed(format!(
                "agent '{}' has no worker_binding; WS thin-path requires one \
                 (Blueprint AgentDef.profile.worker_binding)",
                ctx.agent
            )));
        };
        let req_id = format!("{}-spawn-{}", self.sid, uuid::Uuid::new_v4());
        let worker_handle = ctx
            .meta
            .runtime
            .get("worker_handle")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let data_sink_endpoint = ctx
            .meta
            .runtime
            .get("data_sink_endpoint")
            .and_then(|v| v.as_str());
        // issue #13 run_id propagation: `EngineDispatcher::with_run` (when
        // the launch carries a `RunContext`) inserts this into
        // `Ctx.meta.runtime["run_id"]`; `None` on launches with no run
        // tracing (see `Engine::dispatch_attempt_with`'s `run_id` param).
        let run_id = ctx.meta.runtime.get("run_id").and_then(|v| v.as_str());
        // GH #20 Contract C: `project_name_alias` / `project_root` /
        // `work_dir` (previously read individually here) now come off one
        // materialized `AgentContextView` — reads back the view
        // `AgentContextMiddleware` stashed into
        // `ctx.meta.runtime[AGENT_CONTEXT_KEY]`, falling back to a
        // field-by-field pull off `ctx.meta.runtime` when that middleware
        // was never layered (backward compat). See the module doc on
        // `mlua_swarm::core::agent_context` for the full narrative.
        let view = AgentContextView::materialized_or_from_ctx(ctx);
        // issue #18: `prompt` is `TaskSpec.initial_directive`, threaded as
        // `Value` end-to-end through `EngineState.prompts` /
        // `Engine::fetch_prompt`. The WS Spawn frame text render is the
        // sole String boundary on this axis — no re-parse round trip,
        // and Object / Array / Number seeds keep their structural shape
        // all the way to the render call.
        let directive = default_spawn_directive_with_task_directive(
            &ctx.agent,
            ctx.task_id.as_str(),
            &worker.variant,
            &view,
            data_sink_endpoint,
            self.base_url.as_deref(),
            run_id,
            &prompt,
        );
        // GH #27 (follow-up to #23): the ProjectionPlacement resolver
        // `AgentContextMiddleware` resolved (via `Engine::projection_placement_for`,
        // which this WS session has no direct handle to call itself) and
        // stashed into `ctx.meta.runtime[PROJECTION_PLACEMENT_KEY]` — falls
        // back to the byte-compat default when absent or undeserializable
        // (middleware never layered onto this spawner stack, e.g. tests
        // driving `execute` directly against a bare `Ctx`).
        let projection_placement = ctx
            .meta
            .runtime
            .get(PROJECTION_PLACEMENT_KEY)
            .and_then(|v| serde_json::from_value::<ProjectionPlacement>(v.clone()).ok())
            .unwrap_or_default();
        // issue #21/ST2 in-flight projection hook: materializes `view`
        // (already `apply_policy`-filtered — see `AgentContextMiddleware`)
        // to file and appends a `ctx_projection:` pointer line. See
        // `append_projection_pointer`'s doc for the fallback contract.
        let directive = append_projection_pointer(
            directive,
            &ctx.task_id,
            &view,
            run_id,
            &projection_placement,
        );
        let msg = ServerMsg::Spawn {
            req_id: req_id.clone(),
            parent_req_id: current_parent_req_id(),
            task_id: ctx.task_id.clone(),
            agent: ctx.agent.clone(),
            attempt: ctx.attempt,
            capability_token: worker_token.encode(),
            worker_handle,
            worker: Some(worker),
            directive,
        };
        match self
            .send_and_await(req_id, msg, PendingScope::from_ctx(ctx, PendingKind::Spawn))
            .await
        {
            Ok(PendingReply::SpawnAck {
                value,
                ok,
                error: None,
                stats,
            }) => Ok(WorkerResult {
                value,
                ok,
                // Operator-proxied stats (the harness reports the
                // SubAgent's usage to the Operator, who attaches it to
                // this ack). Best-effort decode — an unknown shape is
                // dropped, never a spawn failure.
                stats: stats.and_then(decode_ack_stats),
            }),
            Ok(PendingReply::SpawnAck {
                error: Some(msg), ..
            }) => Err(WorkerError::Failed(msg)),
            // `spawn_halt` (issue #7): controlled halt. Return
            // `Ok(WorkerResult { ok: true, value: halt_marker })` so the
            // step lands as a normal termination rather than a
            // `WorkerError::Failed` — log stays `info`, downstream retry
            // logic doesn't fire. The halt marker carries the caller's
            // partial value and reason string in a fixed shape.
            Ok(PendingReply::SpawnHalt { value, reason }) => {
                let marker = serde_json::json!({
                    "halted": true,
                    "reason": reason,
                    "value": value,
                });
                Ok(WorkerResult {
                    value: marker,
                    ok: true,
                    stats: None,
                })
            }
            Ok(_) => Err(WorkerError::Failed(
                "ws operator: unexpected non-spawn reply".into(),
            )),
            Err(e) => Err(WorkerError::Failed(format!("ws operator spawn: {e}"))),
        }
    }

    fn requires_worker_binding(&self) -> bool {
        true
    }
}

/// Decode an ack-attached stats `Value` into
/// [`mlua_swarm::store::trace::WorkerStats`], defaulting `worker_kind`
/// to `"operator"` (the axis this ack rode in on). Best-effort: a shape
/// that doesn't decode, or an all-empty stats object, maps to `None` —
/// stats must never gate the ack itself.
///
/// A failed decode is logged at `warn`: dropping it silently is what
/// made a whole run's ack-reported stats vanish with `mse_ack` still
/// answering `{"sent": true}`, leaving `swarm_run_stats` at
/// `steps_with_stats: 0` and no signal anywhere pointing at the shape
/// that was rejected.
fn decode_ack_stats(v: serde_json::Value) -> Option<mlua_swarm::store::trace::WorkerStats> {
    let mut stats: mlua_swarm::store::trace::WorkerStats = match serde_json::from_value(v.clone()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                error = %e,
                stats = %v,
                "spawn_ack: attached stats failed to decode — dropped (the ack itself \
                 still succeeded). Expected an object with optional worker_kind / \
                 model / num_turns / adapter_data plus an optional usage object of \
                 optional input_tokens / output_tokens / total_tokens"
            );
            return None;
        }
    };
    // Emptiness is judged on what the ack actually reported — BEFORE
    // the `worker_kind` default. Defaulting first made `is_empty()`
    // unreachable (the label alone is never empty), so an ack carrying
    // `stats: {}` recorded a content-free entry that still counted
    // toward `swarm_run_stats.steps_with_stats` — the very number this
    // path exists to make trustworthy.
    if stats.is_empty() {
        return None;
    }
    if stats.worker_kind.is_none() {
        stats.worker_kind = Some("operator".to_string());
    }
    Some(stats)
}

/// Literal instruction text for the MainAI (= WS Client = Operator role). Fix
/// for observation #7.
///
/// Minimal hand-off form parallel to /orch (agent_primitive): sends an
/// `[agent_primitive dispatch=@<agent>]` marker + worker endpoint + auth +
/// task_id in the payload; the MainAI **kicks a SubAgent by specifying AgentId +
/// Token** and **forwards the return string verbatim into `SpawnAck.value`**.
///
/// The detailed instructions for the SubAgent are consolidated into the
/// agent.md `system` (= the body fetched by `GET /v1/worker/prompt`); the
/// directive is narrowed to the minimum routing information.
///
/// # `project_name_alias` / `project_root` / `work_dir` / `task_metadata`
/// (GH #20 Contract C — `AgentContextView.to_directive_header`)
///
/// These task-level context header lines are no longer read individually
/// here — they come off one materialized `view: &AgentContextView`
/// (see `mlua_swarm::core::agent_context` for the full Contract C
/// narrative) via [`AgentContextView::to_directive_header`], rendered
/// verbatim at the top of the "worker endpoint" block below. Format is
/// byte-identical to the pre-#20 individual splices
/// (`project_name_alias: {a}` / `project_root: {p}` / `work_dir: {w}`,
/// each independently absent-or-present, no empty-string placeholder) —
/// the additive change is the new `task_metadata: {compact-json}` line
/// (closes the F2 gap tracked in the `operator-execution-model` guide)
/// plus one line per `view.extra` entry.
///
/// `project_name_alias` is ALSO used below (via `view.project_name_alias`)
/// to expand the "LDS Session Alias" mandatory reminder block for the
/// MainAI — the engine itself performs no other action on the alias; the
/// expansion here is what the MainAI actually reads.
///
/// # `subagent_type` (Blueprint-baked worker binding)
///
/// Resolved from `AgentDef.profile.worker_binding` (see `WorkerBinding`) and
/// literally substituted for the old hardcoded `"mse-worker"` string — the
/// Blueprint is the single source of truth for which Claude Code SubAgent
/// definition the MainAI must dispatch. There is deliberately **no fallback**
/// to another `subagent_type` here: if the named SubAgent definition is not
/// registered, the MainAI is instructed to fail the SpawnAck loud rather than
/// silently substitute a different one.
/// `base_url` is the server's public HTTP root (e.g.
/// `"http://127.0.0.1:7777"`). When `Some`, it is rendered verbatim into
/// the SubAgent prompt block so the operator can copy the frame
/// straight through without a `mse_doctor` lookup (issue #8). When
/// `None`, a fallback placeholder points the reader at `mse_doctor` —
/// no fake port number appears in the directive.
///
/// `run_id` (issue #13 ID-hierarchy persistence) is `Some` whenever this
/// dispatch's `Ctx.meta.runtime["run_id"]` is populated (see
/// `Engine::dispatch_attempt_with`), and is rendered into the observation
/// route hint below (`GET /v1/runs/{run_id}`) so a MainAI reading the
/// directive can drill into that specific kick's `RunRecord.step_entries`
/// trace. `None` falls back to a generic `<run_id>` placeholder. Kept as
/// its own parameter (not read off `view`) — the directive's observation
/// route hint is a separate rendering concern from the task-level context
/// header.
#[allow(clippy::too_many_arguments)]
pub(super) fn default_spawn_directive(
    agent: &str,
    task_id: &str,
    subagent_type: &str,
    view: &AgentContextView,
    data_sink_endpoint: Option<&str>,
    base_url: Option<&str>,
    run_id: Option<&str>,
) -> String {
    // GH #20: task-level context header lines (project_name_alias /
    // project_root / work_dir / task_metadata / extra), rendered by the
    // materialized view itself. See the doc above.
    let context_header = view.to_directive_header();
    // Endpoint hint for the Data path (Big Response routing). Only when
    // Some, inject a convention line telling the MainAgent to pass the Big
    // EMIT POST target URL into the SubAgent prompt or environment when it
    // kicks a SubAgent. Audience: MainAgent (the SubAgent-launcher side).
    // A single authenticated emit endpoint: the token can be passed as
    // Bearer or `?token=`; both consume the same CapToken material.
    let data_endpoint_block = match data_sink_endpoint {
        Some(base) => format!(
            "\n\
             [Data path endpoint — MainAgent reminder]\n\
             When you kick a SubAgent, inject the following two lines into\n\
             its prompt / environment so Big Response payloads (4k+ tokens,\n\
             files, intermediate artifacts) flow directly to the Store owner,\n\
             bypassing the MainAgent (context stays small; only the out_id\n\
             ref is passed around).\n  \
             DATA_EMIT: {base}/v1/data/emit  (POST, auth = Bearer worker_handle or ?token=)\n  \
             DATA_GET:  {base}/v1/data/<out_id|out_name>  (the next SubAgent fetches from $IN_REFS)\n\
             When a SubAgent produces a Big Response, POST it to DATA_EMIT\n\
             and return only the one-line out_id ref (do not mix the body\n\
             in; the MainAgent must not answer directly).\n\
             \n"
        ),
        None => String::new(),
    };
    let main_ai_reminder = match view.project_name_alias.as_deref() {
        Some(a) => format!(
            "\n\
             [LDS Session Alias Reminder — MainAI mandatory]\n\
             Before kicking the SubAgent below, call:\n  \
             mcp__lds__session_create(root=<working_dir>, alias=\"{a}\")\n\
             (= establish a single task-level lds session; reuse on repeated dispatch).\n\
             Then add this literal line to the SubAgent prompt body below:\n  \
             LDS Session Alias: {a}\n\
             The SubAgent will call mcp__lds__session_start(alias=\"{a}\") on init,\n\
             keeping worktree ownership unified across dispatches.\n\
             (Full discipline rationale is inlined above; reach is via this directive itself,\n\
              not via any external doc path. The 2 steps above are the complete contract.)\n\
             \n"
        ),
        None => String::new(),
    };
    // Issue #8: render the actual server bind literally when it was
    // sourced at boot; fall back to a pointer at `mse_doctor` rather
    // than a fake port number.
    let base_url_line = match base_url {
        Some(u) => u.to_string(),
        None => "<your server's actual bind — check with mse_doctor>".to_string(),
    };
    // issue #13: the real drill-down route is `GET /v1/runs/{run_id}` (a
    // single `RunRecord`, `step_entries` trace included) — `GET
    // /v1/tasks/{id}` does exist but returns the coarser `TaskRecord` +
    // every `RunRecord` kicked from it, not this specific kick.
    let run_route_line = match run_id {
        Some(rid) => format!("GET <base_url>/v1/runs/{rid}"),
        None => "GET <base_url>/v1/runs/<run_id>".to_string(),
    };
    format!(
        "[agent_primitive dispatch=@{agent}]\n\
         worker endpoint:\n  \
         GET  <base_url>/v1/worker/prompt?task_id={task_id}\n  \
         POST <base_url>/v1/worker/submit\n\
         auth: Bearer <worker_handle from THIS Spawn payload (= short `wh-XXXXXXXX` form)>\n\
         task_id: {task_id}\n\
         agent_id: {agent}\n\
         {context_header}\
         {data_endpoint_block}\
         {main_ai_reminder}\
         Kick a SubAgent via Agent tool with subagent_type=\"{subagent_type}\" (= project-local \
         `.claude/agents/{subagent_type}.md`, this agent's Blueprint-declared worker binding). \
         The prompt you pass to it MUST be EXACTLY these 4 lines (no preamble, no extra text):\n\
         \n  \
         agent_id: {agent}\n  \
         worker_handle: <THIS Spawn payload's `worker_handle` field (short string `wh-XXXXXXXX`)>\n  \
         base_url: {base_url_line}\n  \
         task_id: {task_id}\n\
         \n\
         The SubAgent self-fetches system + prompt via GET (Bearer = handle), \
         executes as agent @{agent}, POSTs raw body to /v1/worker/submit (Bearer = handle, \
         server resolves task_id from handle), and replies `OUTPUT` 1 word. You then forward \
         SpawnAck {{req_id, value:{{}}, ok:true}} through your operator client — MCP path: \
         mse_ack(sid, req_id, kind=\"spawn_ack\", ok=true) (= empty value because canonical \
         body lives in output_tail via the POST). \
         Do NOT fetch /v1/worker/prompt yourself. Do NOT wrap, summarize, or field-select \
         the SubAgent reply. Observation / debug is a separate channel (= agent-inspect MCP / \
         {run_route_line}), do NOT mix it into the forward path. \
         If the SubAgent type is not registered, FAIL LOUD: reply SpawnAck ok=false with an \
         error explaining the missing `.claude/agents/{subagent_type}.md` — do NOT fall back \
         to another subagent_type."
    )
}

/// Wraps [`default_spawn_directive`]'s routing/reminder text as the WS
/// `Spawn.directive` `Value` (issue #18), additionally splicing in a
/// `task_directive` line built from `TaskSpec.initial_directive` when the
/// task was seeded with one.
///
/// This is the sole place the render from `Value` (`task_directive`) down
/// to `String` literal happens for the WS Spawn path — the coercion that
/// used to sit in `EngineDispatcher::dispatch` moved here. `task_directive
/// == Value::Null` (no seed, or the caller could not recover one) omits
/// the line entirely, leaving the output byte-identical to
/// [`default_spawn_directive`]'s own text — this preserves every existing
/// [`default_spawn_directive`] test unchanged, since that function's
/// signature and body are untouched by issue #18.
#[allow(clippy::too_many_arguments)]
pub(super) fn default_spawn_directive_with_task_directive(
    agent: &str,
    task_id: &str,
    subagent_type: &str,
    view: &AgentContextView,
    data_sink_endpoint: Option<&str>,
    base_url: Option<&str>,
    run_id: Option<&str>,
    task_directive: &Value,
) -> String {
    let base = default_spawn_directive(
        agent,
        task_id,
        subagent_type,
        view,
        data_sink_endpoint,
        base_url,
        run_id,
    );
    // Strings pass through verbatim; anything else (Object / Array /
    // Number / Bool) is serde-stringified — the same coercion pattern
    // `EngineDispatcher::dispatch` used to apply eagerly, now applied
    // lazily at this render boundary only.
    let task_directive_line = match task_directive {
        Value::Null => String::new(),
        Value::String(s) => format!("task_directive: {s}\n"),
        other => format!("task_directive: {other}\n"),
    };
    format!("{base}{task_directive_line}")
}

/// issue #21/ST2 in-flight projection hook: projects `view` (the
/// spawn-time, already `apply_policy`-filtered [`AgentContextView`] — see
/// `AgentContextMiddleware`'s module doc) to file via a fresh
/// [`FileProjectionAdapter`] rooted at the materialize root
/// `placement.resolve_root(view)` resolves (GH #27, follow-up to #23 —
/// see `mlua_swarm::core::projection_placement`'s module doc for the "3
/// path" convergence this closes: this hook used to check `view.work_dir`
/// ONLY, with no fallback to `view.project_root`, an asymmetry against
/// the other two call sites the shared resolver now removes), and appends
/// a single `ctx_projection: {json}\n` line to `directive` — a
/// `{key}: {value}\n` splice matching [`AgentContextView::to_directive_header`]'s
/// own line convention (e.g. its `task_metadata: {compact-json}\n` line),
/// never the projected value itself (pointer-only supply; see
/// `mlua_swarm::core::projection`'s module doc for why).
///
/// An unresolved root, `view` failing to serialize, or the materialize
/// write itself failing, all fall back to `directive` unchanged (no
/// pointer line) rather than failing the spawn — subtask-2's Invariants
/// require this hook to never turn a would-have-succeeded spawn into a
/// failure.
///
/// # projection-adapter ST5: `ctx_step_dir` line retired
///
/// This spawn-time `ctx_projection:` line supplies *this spawning agent's
/// own* `AgentContextView` (kept, unchanged, above). A companion
/// `ctx_step_dir:` line — pointing the worker at
/// `<root>/workspace/tasks/<task_id>/ctx/` plus the `mse_ctx_get` MCP tool
/// as a way to pull a *prior* step's OUTPUT out of it — existed from
/// subtask-4 through ST4; ST5 retires both (see
/// `mlua_swarm::core::agent_context`'s module doc): the Worker axis now
/// gets prior steps' OUTPUT pointers automatically, pre-filtered through
/// `ContextPolicy.steps`, on `AgentContextView.steps` (assembled by
/// `crates/mlua-swarm-server/src/worker.rs`'s `GET /v1/worker/prompt`
/// handler) — no separate directory hint or MCP tool call needed.
fn append_projection_pointer(
    directive: String,
    task_id: &StepId,
    view: &AgentContextView,
    run_id: Option<&str>,
    placement: &ProjectionPlacement,
) -> String {
    let Some(root) = placement.resolve_root(view) else {
        return directive;
    };
    match serde_json::to_value(view) {
        Ok(ctx_data) => {
            let key = ProjectionKey {
                task_id: task_id.to_string(),
                run_id: run_id.map(str::to_string),
                step: None,
                path: None,
            };
            let adapter = FileProjectionAdapter::with_placement(root, placement.clone());
            match adapter.project(&key, &ctx_data) {
                Ok(reference) => {
                    let pointer_value = match &reference {
                        ProjectionRef::File { path } => serde_json::json!({ "file": path }),
                        ProjectionRef::Query { endpoint, key } => {
                            serde_json::json!({ "endpoint": endpoint, "key": key })
                        }
                    };
                    format!("{directive}ctx_projection: {pointer_value}\n")
                }
                Err(err) => {
                    tracing::warn!(
                        %task_id,
                        error = %err,
                        "projection hook: materialize failed, spawning without a pointer"
                    );
                    directive
                }
            }
        }
        Err(err) => {
            tracing::warn!(
                %task_id,
                error = %err,
                "projection hook: AgentContextView serialize failed, spawning without a pointer"
            );
            directive
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mlua_swarm::core::agent_context::{
        TASK_METADATA_KEY, TASK_PROJECT_ROOT_KEY, TASK_WORK_DIR_KEY,
    };

    /// Test helper: builds an `AgentContextView` with only
    /// `project_name_alias` / `project_root` / `work_dir` set (the three
    /// fields `default_spawn_directive`'s retired individual params used
    /// to carry) — everything else stays at `Default`. Mirrors the
    /// pre-#20 call shape so the mechanical rewrite of every existing
    /// test stays a 1:1 argument swap.
    fn view_with(
        alias: Option<&str>,
        project_root: Option<&str>,
        work_dir: Option<&str>,
    ) -> AgentContextView {
        AgentContextView {
            project_name_alias: alias.map(String::from),
            project_root: project_root.map(String::from),
            work_dir: work_dir.map(String::from),
            ..AgentContextView::default()
        }
    }

    #[test]
    fn ack_stats_survive_a_total_only_operator_report() {
        // The shape a Claude Code driver relays from the harness
        // completion notice: one token total, no split. This used to
        // fail the WorkerStats decode outright, so a whole run's
        // ack-reported stats vanished and swarm_run_stats read
        // steps_with_stats: 0 while every mse_ack answered {sent: true}.
        let stats = decode_ack_stats(serde_json::json!({
            "usage": {"total_tokens": 198471},
            "model": "opus",
            "num_turns": 22,
        }))
        .expect("a total-only report must land on the StepEntry");
        assert_eq!(stats.usage.expect("usage").total_tokens, 198471);
        assert_eq!(stats.model.as_deref(), Some("opus"));
        assert_eq!(stats.num_turns, Some(22));
        assert_eq!(
            stats.worker_kind.as_deref(),
            Some("operator"),
            "the ack axis labels itself"
        );
    }

    #[test]
    fn ack_stats_of_an_undecodable_shape_are_dropped_not_fatal() {
        // Best-effort stays best-effort — the drop is now logged at
        // warn, but it must still never gate the ack.
        assert!(decode_ack_stats(serde_json::json!("not-an-object")).is_none());
        assert!(
            decode_ack_stats(serde_json::json!({})).is_none(),
            "an all-empty stats object records nothing"
        );
    }

    #[tokio::test]
    async fn connection_state_tracks_the_current_sender() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let session = WSOperatorSession::new_with_base_url(
            SessionId::parse("S-connection-state").unwrap(),
            tx.clone(),
            None,
        );
        assert!(session.is_connected().await);

        session.clear_tx_if(&tx).await;
        assert!(!session.is_connected().await);
    }

    #[tokio::test]
    async fn stale_disconnect_does_not_clear_a_reconnected_sender() {
        let (old_tx, _old_rx) = mpsc::unbounded_channel();
        let (new_tx, _new_rx) = mpsc::unbounded_channel();
        let session = WSOperatorSession::new_with_base_url(
            SessionId::parse("S-reconnect-state").unwrap(),
            old_tx.clone(),
            None,
        );

        session.replace_tx(new_tx).await;
        session.clear_tx_if(&old_tx).await;

        assert!(session.is_connected().await);
    }

    #[test]
    fn directive_omits_project_name_alias_when_none() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
        );
        assert!(!d.contains("project_name_alias:"));
        assert!(!d.contains("LDS Session Alias"));
        assert!(!d.contains("session_create"));
    }

    #[test]
    fn directive_emits_project_name_alias_when_some() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(Some("mse-task-7785"), None, None),
            None,
            None,
            None,
        );
        // Header line (expanded verbatim from the value).
        assert!(
            d.contains("project_name_alias: mse-task-7785"),
            "directive missing project_name_alias header: {d}"
        );
        // MainAI mandatory reminder (= session_create + SubAgent prompt inject)
        assert!(
            d.contains("mcp__lds__session_create(root=<working_dir>, alias=\"mse-task-7785\")"),
            "directive missing session_create reminder: {d}"
        );
        assert!(
            d.contains("LDS Session Alias: mse-task-7785"),
            "directive missing SubAgent prompt inject line: {d}"
        );
        // Reach discipline: the rationale is inlined into the directive (no external doc path reference).
        assert!(
            d.contains("inlined above") || d.contains("complete contract"),
            "directive should inline rationale rather than point at external doc: {d}"
        );
        // The SoT is not pointed at an AI personal memory file (which is
        // outside the MainAI's reach) — reach-axis consistency. Path
        // references coming from the subagent registration convention (for
        // example `agents/<variant>.md`) are a separate case and are
        // allowed. This one forbidden pattern is assembled by string
        // concat rather than written literally, so that a scan for it does
        // not match its own assertion — do not "simplify" it back into a
        // literal. (Unrelated: the wrapper-path assertion further down
        // does carry a literal, because `.claude/agents/<variant>.md` is
        // the documented Claude Code wrapper convention this server
        // renders — see `DEFAULT_WRAPPER_DIR` — not a leaked path.)
        let forbidden_doc_ref = format!(".{}/CLAUDE.md", "claude");
        assert!(
            !d.contains(&forbidden_doc_ref),
            "directive must not reference {forbidden_doc_ref} (out of MainAI scope): {d}"
        );
    }

    #[test]
    fn directive_omits_data_endpoint_when_none() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
        );
        assert!(!d.contains("[Data path endpoint"));
        assert!(!d.contains("DATA_EMIT"));
        assert!(!d.contains("DATA_GET"));
    }

    #[test]
    fn directive_emits_data_endpoint_when_some() {
        let base = "http://127.0.0.1:7785";
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            Some(base),
            None,
            None,
        );
        assert!(
            d.contains("[Data path endpoint"),
            "directive missing data endpoint block header: {d}"
        );
        assert!(
            d.contains(&format!("DATA_EMIT: {base}/v1/data/emit")),
            "directive missing single-mouth emit line: {d}"
        );
        assert!(
            d.contains("Bearer worker_handle or ?token="),
            "directive missing auth transport hint: {d}"
        );
        assert!(
            d.contains(&format!("DATA_GET:  {base}/v1/data/<out_id|out_name>")),
            "directive missing GET line: {d}"
        );
        assert!(
            !d.contains("emit-auth"),
            "old split endpoint must not leak into directive: {d}"
        );
        assert!(
            d.contains("bypassing the MainAgent") && d.contains("out_id ref"),
            "directive should carry the ownership + bypass reasoning: {d}"
        );
    }

    #[test]
    fn directive_carries_declared_subagent_type_and_has_no_fallback() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
        );
        assert!(
            d.contains("subagent_type=\"code-worker\""),
            "directive must carry the Blueprint-declared subagent_type literally: {d}"
        );
        assert!(
            d.contains(".claude/agents/code-worker.md"),
            "directive must reference the declared subagent's own .md path: {d}"
        );
        // The old hardcoded default and its silent-fallback text must be gone.
        assert!(
            !d.contains("general-purpose"),
            "directive must not fall back to subagent_type=\"general-purpose\": {d}"
        );
        assert!(
            !d.contains("mse-worker\""),
            "directive must not carry the old hardcoded \"mse-worker\" literal: {d}"
        );
        assert!(
            d.contains("FAIL LOUD"),
            "directive must instruct the MainAI to fail loud instead of falling back: {d}"
        );
    }

    // ─── Issue #8: base_url rendering + fallback framing ─────────────────

    /// Layer 1: when `base_url` is `Some`, it must land verbatim in the
    /// SubAgent-prompt block, so the operator can copy the frame
    /// through without a `mse_doctor` lookup.
    #[test]
    fn directive_renders_actual_base_url_when_some() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            Some("http://127.0.0.1:8888"),
            None,
        );
        assert!(
            d.contains("base_url: http://127.0.0.1:8888"),
            "directive must render the actual bind literally: {d}"
        );
        assert!(
            !d.contains("mse_doctor"),
            "no mse_doctor detour when bind is known: {d}"
        );
    }

    /// Layer 3: when `base_url` is `None` (unit tests, mock harnesses,
    /// pre-serve rendering) the fallback line must point the reader at
    /// `mse_doctor` — never a fake port number.
    #[test]
    fn directive_falls_back_to_mse_doctor_pointer_when_none() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
        );
        assert!(
            d.contains("check with mse_doctor"),
            "fallback must point at mse_doctor: {d}"
        );
    }

    /// Regression guard: the historical `7786` example port (the whole
    /// origin of issue #8) must not survive in the rendered directive
    /// under any input combination.
    #[test]
    fn directive_never_contains_stale_example_port_7786() {
        for base in [
            None,
            Some("http://127.0.0.1:7777"),
            Some("http://192.0.2.1:9000"),
        ] {
            let d = default_spawn_directive(
                "implementer",
                "task-x",
                "code-worker",
                &view_with(Some("mse-task-alias"), None, None),
                Some("http://127.0.0.1:7785"),
                base,
                None,
            );
            assert!(
                !d.contains("7786"),
                "stale example port 7786 leaked: base={base:?}, d={d}"
            );
        }
    }

    // ─── Issue #13: run_id observation route (doc-drift fix) ─────────────

    /// Regression guard: the stale `GET /v1/tasks/{id}` observation hint
    /// (a route that never returns a single `RunRecord`) must be gone —
    /// the directive must point at the real drill-down route instead.
    #[test]
    fn directive_never_contains_stale_tasks_id_route() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            Some("R-abc123"),
        );
        assert!(
            !d.contains("/v1/tasks/{id}") && !d.contains("/v1/tasks/{{id}}"),
            "stale /v1/tasks/{{id}} observation hint leaked: {d}"
        );
    }

    /// When `run_id` is `Some`, it is rendered literally into the
    /// observation route hint (`GET /v1/runs/<run_id>`).
    #[test]
    fn directive_renders_actual_run_id_when_some() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            Some("R-abc123"),
        );
        assert!(
            d.contains("GET <base_url>/v1/runs/R-abc123"),
            "directive missing real run_id in observation route: {d}"
        );
    }

    /// `run_id: None` (no run tracing for this launch) falls back to a
    /// generic placeholder route rather than a stale/incorrect one.
    #[test]
    fn directive_falls_back_to_run_id_placeholder_when_none() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
        );
        assert!(
            d.contains("GET <base_url>/v1/runs/<run_id>"),
            "directive missing placeholder observation route: {d}"
        );
    }

    // ─── Issue #17: project_root / work_dir header lines ─────────────────

    /// Both absent → neither header line appears (no empty-string
    /// placeholder either).
    #[test]
    fn directive_omits_project_root_and_work_dir_when_both_none() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
        );
        assert!(!d.contains("project_root:"));
        assert!(!d.contains("work_dir:"));
    }

    /// Both present → both header lines render literally, alongside
    /// `project_name_alias`'s existing splice.
    #[test]
    fn directive_splices_project_root_and_work_dir_when_both_present() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, Some("/repo"), Some("/repo/work")),
            None,
            None,
            None,
        );
        assert!(
            d.contains("project_root: /repo"),
            "directive missing project_root header: {d}"
        );
        assert!(
            d.contains("work_dir: /repo/work"),
            "directive missing work_dir header: {d}"
        );
    }

    /// Partial: `project_root` present, `work_dir` absent — each field is
    /// independent, so only the present one renders.
    #[test]
    fn directive_splices_project_root_only_when_work_dir_absent() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, Some("/repo"), None),
            None,
            None,
            None,
        );
        assert!(
            d.contains("project_root: /repo"),
            "directive missing project_root header: {d}"
        );
        assert!(!d.contains("work_dir:"));
    }

    // ─── GH #20: task_metadata header line (Contract C, closes the F2 gap) ─

    /// `task_metadata` renders as a new `task_metadata: {compact-json}`
    /// line — the F2 gap the `operator-execution-model` guide tracked
    /// (`task_metadata`'s inner keys were never spliced into the
    /// directive before GH #20).
    #[test]
    fn directive_splices_task_metadata_when_some() {
        let view = AgentContextView {
            task_metadata: Some(serde_json::json!({"issue": 20})),
            ..view_with(None, Some("/repo"), None)
        };
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view,
            None,
            None,
            None,
        );
        assert!(
            d.contains(r#"task_metadata: {"issue":20}"#),
            "directive missing task_metadata header: {d}"
        );
        // Additive-only: the pre-existing project_root line still renders.
        assert!(d.contains("project_root: /repo"));
    }

    /// `task_metadata: None` (absent) omits the line entirely — no
    /// empty-string placeholder, matching every other header line's
    /// absent-field contract.
    #[test]
    fn directive_omits_task_metadata_when_none() {
        let d = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
        );
        assert!(!d.contains("task_metadata:"));
    }

    // ─── Issue #7: spawn_halt handling in Operator::execute ──────────────

    fn test_ctx(task_id: &str) -> mlua_swarm::Ctx {
        mlua_swarm::Ctx::new(mlua_swarm::StepId::parse(task_id).unwrap(), 1, "a")
    }

    fn test_worker_binding() -> mlua_swarm::WorkerBinding {
        mlua_swarm::WorkerBinding {
            variant: "test-variant".into(),
            tools: vec![],
            request_digest: None,
            requested_model: None,
        }
    }

    fn test_cap_token() -> mlua_swarm::CapToken {
        mlua_swarm::CapToken {
            agent_id: "a".into(),
            role: mlua_swarm::Role::Worker,
            scopes: vec!["*".into()],
            issued_at: 0,
            expire_at: u64::MAX / 2,
            max_uses: None,
            nonce: "test-nonce".into(),
            sig_hex: "".into(),
        }
    }

    /// A `PendingReply::SpawnHalt` reply must translate into a
    /// `Ok(WorkerResult { ok: true, value: <halt marker> })` — a normal
    /// termination, not a `WorkerError::Failed` (fail-loud). This is
    /// the whole point of the new verb: distinguishing a controlled
    /// halt from a real worker error at the log / retry-signal level.
    #[tokio::test]
    async fn spawn_halt_reply_lands_as_ok_worker_result_with_marker() {
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-halt").unwrap(),
            tx,
            None,
        ));

        // Kick execute() in a background task so we can grab the
        // req_id the server assigns and inject a matching SpawnHalt.
        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &test_ctx("ST-halt"),
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn { req_id, .. } => req_id,
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnHalt {
                    value: serde_json::json!({"partial": "abc"}),
                    reason: Some("shape verified".into()),
                },
            )
            .await;

        let result = handle.await.expect("join").expect("execute Ok");
        assert!(
            result.ok,
            "spawn_halt must land as ok=true (normal termination), got: {result:?}"
        );
        assert_eq!(result.value["halted"], true);
        assert_eq!(result.value["reason"], "shape verified");
        assert_eq!(result.value["value"], serde_json::json!({"partial": "abc"}));
    }

    /// `spawn_ack { ok: false, error: Some(_) }` must retain its
    /// current fail-loud behaviour (backward compat guard).
    #[tokio::test]
    async fn spawn_ack_with_error_still_lands_as_worker_error() {
        use mlua_swarm::{Operator, WorkerError};
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-err").unwrap(),
            tx,
            None,
        ));

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &test_ctx("ST-err"),
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn { req_id, .. } => req_id,
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: false,
                    error: Some("real crash".into()),
                    stats: None,
                },
            )
            .await;

        let err = handle.await.expect("join").expect_err("must be error");
        assert!(matches!(err, WorkerError::Failed(msg) if msg.contains("real crash")));
    }

    /// B-2: a spawn parked in `execute` (awaiting a `SpawnAck` that never
    /// arrives) must unblock with a `WorkerError::Failed` as soon as
    /// `fail_pending` drains the pending map — the teardown path's
    /// immediate-fail guarantee, so a torn-down session does not leave a
    /// spawn orphaned until the run's sync timeout fires.
    #[tokio::test]
    async fn fail_pending_unblocks_a_parked_spawn_with_worker_error() {
        use mlua_swarm::{Operator, WorkerError};
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-teardown").unwrap(),
            tx,
            None,
        ));

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &test_ctx("ST-teardown"),
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        // Wait until the Spawn is actually parked (its pending entry is
        // registered) before tearing down, so the drain has something to
        // fail rather than racing the insert.
        let _sent = rx.recv().await.expect("Spawn sent");

        session.fail_pending("operator session torn down").await;

        let err = handle
            .await
            .expect("join")
            .expect_err("a parked spawn must fail once pending is drained");
        assert!(
            matches!(err, WorkerError::Failed(_)),
            "fail_pending must surface a WorkerError::Failed, got: {err:?}"
        );
    }

    // ─── issue abcb43e2: reconnect-wait on the send path ─────────────────
    //
    // Every wait below is bounded by an explicit `tokio::time::timeout` in
    // the TEST — production has no deadline by design, so an unwoken park
    // would otherwise hang `cargo test` rather than fail it.

    /// The headline case: a `Spawn` issued while the operator is
    /// disconnected must NOT come back as
    /// `WorkerError::Failed("… ws operator disconnected")`. It parks, and
    /// the reconnect both delivers it and lets the ack resolve normally.
    ///
    /// The `replace_tx` call here is also the deadlock guard: it takes the
    /// same `tx` lock the parked send reads under, so a park that held its
    /// guard across the wait would hang this test at that line.
    #[tokio::test]
    async fn a_spawn_sent_while_disconnected_parks_and_lands_after_reconnect() {
        use mlua_swarm::Operator;
        use std::time::Duration;
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-park-reconnect").unwrap(),
            tx.clone(),
            None,
        ));
        session.clear_tx_if(&tx).await;
        assert!(!session.is_connected().await, "precondition: disconnected");

        let session_bg = session.clone();
        let mut handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &test_ctx("ST-park-reconnect"),
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        // It parked: no early return, and nothing was pushed at the
        // disconnected sender.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut handle)
                .await
                .is_err(),
            "a spawn during a disconnect must park, not resolve"
        );
        assert!(
            rx.try_recv().is_err(),
            "nothing may be written while disconnected"
        );

        let (new_tx, mut new_rx) = mpsc::unbounded_channel();
        session.replace_tx(new_tx).await;

        let sent = tokio::time::timeout(Duration::from_secs(5), new_rx.recv())
            .await
            .expect("the parked Spawn must be delivered once reconnected")
            .expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn { req_id, .. } => req_id,
            other => panic!("expected Spawn, got {other:?}"),
        };

        // The pending entry survived the park, so the ack still finds its
        // slot — this is why the park must not drop it.
        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({"delivered": true}),
                    ok: true,
                    error: None,
                    stats: None,
                },
            )
            .await;

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("execute must return once acked")
            .expect("join")
            .expect("execute Ok");
        assert!(result.ok);
        assert_eq!(result.value["delivered"], true);
    }

    // ── a parked capability is re-minted before it is delivered ──────────

    /// A [`WorkerTokenMinter`] that hands back a token distinguishable
    /// from the one it was given, and counts how often it was asked.
    ///
    /// The reissue keeps subject / role / scopes and moves only
    /// `expire_at` — the same contract `Engine::remint_worker_token`
    /// implements against its own records. The nonce changes so the test
    /// can tell the two apart on the wire.
    struct CountingMinter {
        calls: std::sync::atomic::AtomicUsize,
        fresh_expire_at: u64,
    }

    impl CountingMinter {
        fn new(fresh_expire_at: u64) -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                calls: std::sync::atomic::AtomicUsize::new(0),
                fresh_expire_at,
            })
        }

        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl WorkerTokenMinter for CountingMinter {
        async fn remint_worker_token(&self, expiring: &CapToken) -> Result<CapToken, String> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(CapToken {
                expire_at: self.fresh_expire_at,
                nonce: "re-minted-nonce".into(),
                ..expiring.clone()
            })
        }
    }

    /// A minter that refuses, to prove the frame still goes out.
    struct RefusingMinter;

    #[async_trait]
    impl WorkerTokenMinter for RefusingMinter {
        async fn remint_worker_token(&self, _expiring: &CapToken) -> Result<CapToken, String> {
            Err("token not found in store".to_string())
        }
    }

    fn now_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock at or after the epoch")
            .as_secs()
    }

    /// A worker token expiring `in_secs` from now, in the shape
    /// `Engine::dispatch_attempt_with` mints.
    fn cap_token_expiring_in(in_secs: u64) -> CapToken {
        CapToken {
            expire_at: now_secs() + in_secs,
            ..test_cap_token()
        }
    }

    /// Park a spawn on a disconnected `session`, reconnect, and return the
    /// `capability_token` string the frame carried when it finally went
    /// out.
    async fn parked_spawn_token(
        session: std::sync::Arc<WSOperatorSession>,
        token: CapToken,
    ) -> String {
        use mlua_swarm::Operator;
        use std::time::Duration;
        use tokio::sync::mpsc;

        let session_bg = session.clone();
        let mut handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &test_ctx("ST-token-refresh"),
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    token,
                )
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut handle)
                .await
                .is_err(),
            "precondition: the spawn is parked"
        );

        let (new_tx, mut new_rx) = mpsc::unbounded_channel();
        session.replace_tx(new_tx).await;
        let sent = tokio::time::timeout(Duration::from_secs(5), new_rx.recv())
            .await
            .expect("the parked Spawn must be delivered once reconnected")
            .expect("Spawn sent");
        handle.abort();
        match sent {
            ServerMsg::Spawn {
                capability_token, ..
            } => capability_token,
            other => panic!("expected Spawn, got {other:?}"),
        }
    }

    /// A disconnected session wired to `minter`.
    fn disconnected_with_minter(
        sid: &str,
        minter: Option<Arc<dyn WorkerTokenMinter>>,
    ) -> std::sync::Arc<WSOperatorSession> {
        std::sync::Arc::new(WSOperatorSession::disconnected_with_base_url(
            SessionId::parse(sid).unwrap(),
            None,
            minter,
        ))
    }

    /// **The failure this locks in.** A spawn frame is built before the
    /// park and the worker token inside it keeps ageing while the park
    /// waits, so a disconnect longer than `worker_token_ttl_secs` used to
    /// deliver an already-expired capability — and the SubAgent only found
    /// out at submit, after doing the entire job.
    #[tokio::test]
    async fn a_parked_spawn_re_mints_a_capability_that_would_arrive_expired() {
        let minter = CountingMinter::new(now_secs() + 1800);
        let session = disconnected_with_minter(
            "S-token-stale",
            Some(minter.clone() as Arc<dyn WorkerTokenMinter>),
        );

        // Expired while it waited: this is the state a park longer than the
        // TTL leaves the frame in.
        let delivered = parked_spawn_token(session, cap_token_expiring_in(0)).await;

        assert_eq!(minter.calls(), 1, "the parked frame must be refreshed once");
        let token = CapToken::decode(&delivered).expect("the frame carries a decodable token");
        assert_eq!(
            token.nonce, "re-minted-nonce",
            "the frame must carry the reissue, not the token it was built with"
        );
        assert!(
            token.expire_at > now_secs(),
            "the delivered capability must still be alive when it arrives"
        );
        // Nothing here asserts that the reissue carries the grant it
        // replaces. It cannot: `CountingMinter` builds its answer as
        // `CapToken { expire_at, nonce, ..expiring.clone() }`, so role and
        // scopes come back unchanged by construction and the assertions
        // would hold against any implementation whatsoever — including one
        // that widened the grant, since the double is not that
        // implementation. That property belongs to the real minter and is
        // checked there, against `Engine::remint_worker_token`, by
        // `a_remint_carries_the_grant_it_replaces_and_widens_nothing` in
        // `src/core/engine.rs`. What this test owns is the *parking* side:
        // that a frame whose token expired while parked is refreshed once
        // before it goes out.
    }

    /// The refresh is conditional. A park that ended well inside the
    /// token's life delivers exactly the frame that was built — re-minting
    /// every wake would put a token record in engine state per disconnect
    /// flap, for a capability with 29 minutes left on it.
    #[tokio::test]
    async fn a_parked_spawn_whose_token_is_still_fresh_is_delivered_unchanged() {
        let minter = CountingMinter::new(now_secs() + 1800);
        let session = disconnected_with_minter(
            "S-token-fresh",
            Some(minter.clone() as Arc<dyn WorkerTokenMinter>),
        );

        let original = cap_token_expiring_in(1800);
        let delivered = parked_spawn_token(session, original.clone()).await;

        assert_eq!(minter.calls(), 0, "a live capability must not be re-minted");
        assert_eq!(
            delivered,
            original.encode(),
            "the frame must go out byte-identical to the one that was built"
        );
    }

    /// A refusal is not a spawn failure. The frame still goes out with the
    /// token it had — which might yet work — instead of failing the step
    /// here on the strength of a refresh that did not happen.
    #[tokio::test]
    async fn a_refused_re_mint_still_delivers_the_original_frame() {
        let session = disconnected_with_minter(
            "S-token-refused",
            Some(Arc::new(RefusingMinter) as Arc<dyn WorkerTokenMinter>),
        );

        let original = cap_token_expiring_in(0);
        let delivered = parked_spawn_token(session, original.clone()).await;

        assert_eq!(
            delivered,
            original.encode(),
            "a refused refresh must leave the frame alone rather than fail the spawn"
        );
    }

    // ── a dropped hook_after is reported ─────────────────────────────────

    /// `after` is fire-and-forget, so a disconnect drops it — that part is
    /// deliberate (parking it would hold the step's completion open for the
    /// whole disconnect window). What was not deliberate is that the drop
    /// was **silent**: `send_oneway`'s `Err` was discarded here and again
    /// at the engine's call site, so nothing anywhere said a completion
    /// notice had been lost.
    #[tokio::test]
    async fn a_hook_after_dropped_while_disconnected_says_so() {
        use mlua_swarm::SpawnHook;
        use tokio::sync::mpsc;

        let buf = CaptureBuf::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);

        let (tx, _rx) = mpsc::unbounded_channel();
        let session = WSOperatorSession::new_with_base_url(
            SessionId::parse("S-after-drop").unwrap(),
            tx.clone(),
            None,
        );
        session.clear_tx_if(&tx).await;

        let outcome = session
            .after(&test_ctx("ST-after-drop"), &serde_json::json!({"ok": true}))
            .await;
        drop(guard);

        assert!(
            outcome.is_ok(),
            "a lost notification must not fail the step it is about"
        );
        let logged = buf.contents();
        assert!(
            logged.contains("hook_after dropped") && logged.contains("ST-after-drop"),
            "the drop must name the step whose completion the operator never heard about: \
             {logged}"
        );
    }

    /// The counter-case: connected, so nothing is lost and nothing is
    /// logged. Without it the assertion above would also pass on an
    /// implementation that warned unconditionally.
    #[tokio::test]
    async fn a_delivered_hook_after_logs_nothing() {
        use mlua_swarm::SpawnHook;
        use tokio::sync::mpsc;

        let buf = CaptureBuf::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = WSOperatorSession::new_with_base_url(
            SessionId::parse("S-after-sent").unwrap(),
            tx,
            None,
        );
        session
            .after(&test_ctx("ST-after-sent"), &serde_json::json!({"ok": true}))
            .await
            .expect("after is infallible to its caller");
        drop(guard);

        assert!(
            matches!(rx.try_recv(), Ok(ServerMsg::HookAfter { .. })),
            "a connected session delivers the notice"
        );
        assert!(
            buf.contents().is_empty(),
            "nothing was lost, so nothing is reported: {}",
            buf.contents()
        );
    }

    /// A capturing `MakeWriter`, so a test can assert on what was logged.
    /// `#[tokio::test]` runs on a current-thread runtime, so the
    /// thread-local subscriber covers the whole call.
    #[derive(Clone, Default)]
    struct CaptureBuf(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl CaptureBuf {
        fn contents(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
        }
    }

    impl std::io::Write for CaptureBuf {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureBuf {
        type Writer = Self;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// The other exit: teardown. A send parked BEFORE it ever reached
    /// `pending`'s `orx.await` is invisible to `fail_pending`'s drain, so
    /// the drain alone would leave it waiting for a reconnect that can
    /// never come (the session is being removed from `operator_sessions`).
    /// It must fail loud instead.
    ///
    /// `clear_tx` is called after `fail_pending` exactly as
    /// `teardown_operator_session` does it — it must not demote the
    /// terminal torn-down state and re-park the send.
    #[tokio::test]
    async fn a_spawn_parked_while_disconnected_is_woken_by_teardown() {
        use mlua_swarm::{Operator, WorkerError};
        use std::time::Duration;
        use tokio::sync::mpsc;

        let (tx, _rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-park-teardown").unwrap(),
            tx.clone(),
            None,
        ));
        session.clear_tx_if(&tx).await;

        let session_bg = session.clone();
        let mut handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &test_ctx("ST-park-teardown"),
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut handle)
                .await
                .is_err(),
            "precondition: the spawn is parked"
        );

        session.fail_pending("operator session torn down").await;
        session.clear_tx().await;

        let err = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("teardown must wake a parked send, not leave it waiting")
            .expect("join")
            .expect_err("a torn-down session can never deliver");
        assert!(
            matches!(&err, WorkerError::Failed(msg) if msg.contains("torn down")),
            "the failure must name teardown as the cause, got: {err:?}"
        );
    }

    /// Same contract on the `SeniorBridge` axis: `ask` parks during the
    /// disconnect window and its answer still resolves after the
    /// reconnect. (`hook_before` shares the identical `send_and_await`
    /// path.)
    #[tokio::test]
    async fn an_ask_sent_while_disconnected_parks_and_lands_after_reconnect() {
        use mlua_swarm::SeniorBridge;
        use std::time::Duration;
        use tokio::sync::mpsc;

        let (tx, _rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-park-ask").unwrap(),
            tx.clone(),
            None,
        ));
        session.clear_tx_if(&tx).await;

        let session_bg = session.clone();
        let mut handle = tokio::spawn(async move {
            session_bg
                .ask(
                    &StepId::parse("ST-park-ask").unwrap(),
                    serde_json::json!({"question": "still there?"}),
                )
                .await
        });

        // Without this the test also passes when the ask never parked: if
        // `replace_tx` wins the race, the first `tx` read already succeeds.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut handle)
                .await
                .is_err(),
            "precondition: the ask is parked"
        );

        let (new_tx, mut new_rx) = mpsc::unbounded_channel();
        session.replace_tx(new_tx).await;

        let sent = tokio::time::timeout(Duration::from_secs(5), new_rx.recv())
            .await
            .expect("the parked Ask must be delivered once reconnected")
            .expect("Ask sent");
        let req_id = match sent {
            ServerMsg::Ask { req_id, .. } => req_id,
            other => panic!("expected Ask, got {other:?}"),
        };

        session
            .resolve_pending(&req_id, PendingReply::Answer(serde_json::json!("yes")))
            .await;

        let answer = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("ask must return once answered")
            .expect("join")
            .expect("ask Ok");
        assert_eq!(answer, serde_json::json!("yes"));
    }

    // ─── Issue #17: end-to-end `execute()` splice (ctx.meta.runtime → Spawn.directive) ───

    /// `Ctx.meta.runtime` carrying both `project_root` and `work_dir`
    /// (the `TaskInputMiddleware` injection shape) must land in the
    /// `ServerMsg::Spawn.directive` actually sent over the wire — not
    /// just in the pure `default_spawn_directive` helper.
    #[tokio::test]
    async fn execute_splices_project_root_and_work_dir_from_ctx_meta_runtime() {
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-ctxroot").unwrap(),
            tx,
            None,
        ));

        let mut ctx = test_ctx("ST-ctxroot");
        ctx.meta.runtime.insert(
            TASK_PROJECT_ROOT_KEY.to_string(),
            serde_json::json!("/repo"),
        );
        ctx.meta.runtime.insert(
            TASK_WORK_DIR_KEY.to_string(),
            serde_json::json!("/repo/work"),
        );

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &ctx,
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn {
                req_id, directive, ..
            } => {
                // issue #18: `Spawn.directive` is now `Value`; extract the
                // `String` it wraps (always a `Value::String` on this
                // path — see `default_spawn_directive_with_task_directive`).
                let directive = directive.as_str();
                assert!(
                    directive.contains("project_root: /repo"),
                    "directive missing project_root splice: {directive}"
                );
                assert!(
                    directive.contains("work_dir: /repo/work"),
                    "directive missing work_dir splice: {directive}"
                );
                req_id
            }
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: true,
                    error: None,
                    stats: None,
                },
            )
            .await;
        handle.await.expect("join").expect("execute Ok");
    }

    /// Partial: only `project_root` present in `ctx.meta.runtime` (no
    /// `TaskInputMiddleware`-populated `work_dir`) — the splice is
    /// per-field independent, matching `TaskInputMiddleware`'s own
    /// per-field-optional contract.
    #[tokio::test]
    async fn execute_splices_project_root_only_when_ctx_meta_runtime_partial() {
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-ctxpartial").unwrap(),
            tx,
            None,
        ));

        let mut ctx = test_ctx("ST-ctxpartial");
        ctx.meta.runtime.insert(
            TASK_PROJECT_ROOT_KEY.to_string(),
            serde_json::json!("/repo"),
        );

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &ctx,
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn {
                req_id, directive, ..
            } => {
                let directive = directive.as_str();
                assert!(
                    directive.contains("project_root: /repo"),
                    "directive missing project_root splice: {directive}"
                );
                assert!(!directive.contains("work_dir:"));
                req_id
            }
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: true,
                    error: None,
                    stats: None,
                },
            )
            .await;
        handle.await.expect("join").expect("execute Ok");
    }

    /// Neither present in `ctx.meta.runtime` (no `TaskInputMiddleware`
    /// layered for this launch) — the directive carries neither header
    /// line, matching pre-issue-#17 behavior exactly.
    #[tokio::test]
    async fn execute_omits_project_root_and_work_dir_when_ctx_meta_runtime_absent() {
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-ctxabsent").unwrap(),
            tx,
            None,
        ));

        let ctx = test_ctx("ST-ctxabsent");

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &ctx,
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn {
                req_id, directive, ..
            } => {
                let directive = directive.as_str();
                assert!(!directive.contains("project_root:"));
                assert!(!directive.contains("work_dir:"));
                req_id
            }
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: true,
                    error: None,
                    stats: None,
                },
            )
            .await;
        handle.await.expect("join").expect("execute Ok");
    }

    /// GH #20 / F2 gap: `task_metadata` in `ctx.meta.runtime` (the
    /// `TaskInputMiddleware` injection shape) now reaches the
    /// `ServerMsg::Spawn.directive` actually sent over the wire, via
    /// `AgentContextView::materialized_or_from_ctx` falling back to
    /// `from_ctx` when `AgentContextMiddleware` was not layered.
    #[tokio::test]
    async fn execute_splices_task_metadata_from_ctx_meta_runtime() {
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-ctxmeta").unwrap(),
            tx,
            None,
        ));

        let mut ctx = test_ctx("ST-ctxmeta");
        ctx.meta.runtime.insert(
            TASK_METADATA_KEY.to_string(),
            serde_json::json!({"issue": 20}),
        );

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &ctx,
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn {
                req_id, directive, ..
            } => {
                let directive = directive.as_str();
                assert!(
                    directive.contains(r#"task_metadata: {"issue":20}"#),
                    "directive missing task_metadata splice: {directive}"
                );
                req_id
            }
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: true,
                    error: None,
                    stats: None,
                },
            )
            .await;
        handle.await.expect("join").expect("execute Ok");
    }

    // ─── Issue #18: `Value` pass-through render boundary
    //     (`default_spawn_directive_with_task_directive`) ───

    /// A `String` seed splices in verbatim, unquoted (matching
    /// `Value::String(s) => s.clone()` — no JSON-quoting artifact).
    #[test]
    fn with_task_directive_splices_string_seed_verbatim() {
        let directive = default_spawn_directive_with_task_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
            &serde_json::json!("do the thing"),
        );
        let text = directive.as_str();
        assert!(
            text.contains("task_directive: do the thing"),
            "missing task_directive line for a String seed: {text}"
        );
    }

    /// An Object seed renders as its JSON literal (issue #18 Invariant 3 —
    /// same shape `Engine::start_task` / `Engine::dispatch_attempt_with`
    /// produce for the Worker HTTP path via `render_directive_to_string`).
    #[test]
    fn with_task_directive_renders_object_seed_as_json_literal() {
        let directive = default_spawn_directive_with_task_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
            &serde_json::json!({"key": "value"}),
        );
        let text = directive.as_str();
        assert!(
            text.contains(r#"task_directive: {"key":"value"}"#),
            "missing JSON-literal task_directive line for an Object seed: {text}"
        );
    }

    /// `Value::Null` (no seed recovered) omits the line entirely — the
    /// output is byte-identical to `default_spawn_directive`'s own text,
    /// preserving every pre-issue-#18 caller unchanged.
    #[test]
    fn with_task_directive_omits_line_when_null() {
        let wrapped = default_spawn_directive_with_task_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
            &serde_json::Value::Null,
        );
        let plain = default_spawn_directive(
            "implementer",
            "task-x",
            "code-worker",
            &view_with(None, None, None),
            None,
            None,
            None,
        );
        assert_eq!(
            wrapped,
            serde_json::Value::String(plain),
            "Value::Null seed must not add a task_directive line"
        );
    }

    /// End-to-end via `execute()`: an Object-shaped `Step.in` seed, once
    /// rendered to a JSON-literal `String` by the engine (the Worker HTTP
    /// path's `render_directive_to_string`), reaches `ServerMsg::Spawn`
    /// with the same JSON literal spliced into `directive` — the WS
    /// render layer is the sole `Value → String` coercion point on this
    /// path (issue #18).
    #[tokio::test]
    async fn execute_splices_json_literal_task_directive_for_object_seed() {
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-objseed").unwrap(),
            tx,
            None,
        ));

        let ctx = test_ctx("ST-objseed");
        // Issue #18: `Value` flows end-to-end from `Step.in` through the
        // engine, so the Object seed reaches `execute()` as `Value` — no
        // stringification upstream. Only the WS Spawn frame render
        // performs the `Value → String` coercion.
        let rendered_prompt = serde_json::json!({"key": "value"});

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &ctx,
                    None,
                    rendered_prompt,
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn {
                req_id, directive, ..
            } => {
                let directive = directive.as_str();
                assert!(
                    directive.contains(r#"task_directive: {"key":"value"}"#),
                    "directive missing JSON-literal task_directive splice: {directive}"
                );
                req_id
            }
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: true,
                    error: None,
                    stats: None,
                },
            )
            .await;
        handle.await.expect("join").expect("execute Ok");
    }

    // ─── issue #21/ST2: in-flight projection hook (`append_projection_pointer`) ───

    /// `view.work_dir` present → the spawn directive carries a
    /// `ctx_projection:` pointer line, and the pointed-at file actually
    /// exists on disk (subtask-2 Tests #3).
    #[tokio::test]
    async fn execute_with_work_dir_appends_ctx_projection_pointer_and_materializes_file() {
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let dir = tempfile::TempDir::new().unwrap();
        let mut ctx = test_ctx("ST-proj-1");
        ctx.meta.runtime.insert(
            TASK_WORK_DIR_KEY.to_string(),
            Value::String(dir.path().to_string_lossy().into_owned()),
        );

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-proj-1").unwrap(),
            tx,
            None,
        ));

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &ctx,
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn {
                req_id, directive, ..
            } => {
                assert!(
                    directive.contains("ctx_projection:"),
                    "directive missing ctx_projection pointer line: {directive}"
                );
                // ST5 (`projection-adapter`) removal confirmation: the
                // pre-ST5 `ctx_step_dir:` companion line (pointing a
                // worker at the raw materialize directory + the retired
                // `mse_ctx_get` MCP tool) must never reappear — the
                // Worker axis now gets prior steps' OUTPUT pointers
                // automatically via `context.steps`.
                assert!(
                    !directive.contains("ctx_step_dir:"),
                    "directive must not carry the retired ctx_step_dir line: {directive}"
                );
                req_id
            }
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: true,
                    error: None,
                    stats: None,
                },
            )
            .await;
        handle.await.expect("join").expect("execute Ok");

        let expected_file = dir.path().join("workspace/tasks/ST-proj-1/ctx/_ctx.md");
        assert!(
            expected_file.exists(),
            "materialized projection file missing at {expected_file:?}"
        );
    }

    /// `view.work_dir` absent → the spawn directive carries no
    /// `ctx_projection:` line, and the spawn still succeeds (non-fatal
    /// fallback, subtask-2 Tests #4 + Invariant "must never turn a
    /// would-have-succeeded spawn into a failure").
    #[tokio::test]
    async fn execute_without_work_dir_spawns_without_ctx_projection_pointer() {
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-proj-2").unwrap(),
            tx,
            None,
        ));

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &test_ctx("ST-proj-2"),
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn {
                req_id, directive, ..
            } => {
                assert!(
                    !directive.contains("ctx_projection:"),
                    "directive must not carry a pointer line when work_dir is absent \
                     (fallback): {directive}"
                );
                req_id
            }
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: true,
                    error: None,
                    stats: None,
                },
            )
            .await;
        handle
            .await
            .expect("join")
            .expect("execute Ok — a materialize skip must not fail the spawn");
    }

    // ──────────────────────────────────────────────────────────────
    // GH #27 (follow-up to #23): ProjectionPlacement resolver wiring
    // ──────────────────────────────────────────────────────────────

    /// `view.work_dir` ABSENT but `view.project_root` present, with the
    /// byte-compat default `ProjectionPlacement` (`root_preference =
    /// WorkDir`, falling back to `project_root`) — the asymmetry fix: a
    /// pre-GH-#27 build would have skipped the pointer entirely here
    /// (`view.work_dir` ONLY, no fallback); this build now falls back the
    /// SAME way the submit-time sink and server read-back always did.
    #[tokio::test]
    async fn execute_with_project_root_only_appends_ctx_projection_pointer_default_placement() {
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let dir = tempfile::TempDir::new().unwrap();
        let mut ctx = test_ctx("ST-proj-3");
        ctx.meta.runtime.insert(
            TASK_PROJECT_ROOT_KEY.to_string(),
            Value::String(dir.path().to_string_lossy().into_owned()),
        );

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-proj-3").unwrap(),
            tx,
            None,
        ));

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &ctx,
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn {
                req_id, directive, ..
            } => {
                assert!(
                    directive.contains("ctx_projection:"),
                    "work_dir absent must still fall back to project_root: {directive}"
                );
                req_id
            }
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: true,
                    error: None,
                    stats: None,
                },
            )
            .await;
        handle.await.expect("join").expect("execute Ok");

        let expected_file = dir.path().join("workspace/tasks/ST-proj-3/ctx/_ctx.md");
        assert!(
            expected_file.exists(),
            "materialized projection file missing at {expected_file:?}"
        );
    }

    /// A `ProjectionPlacement` stashed into
    /// `ctx.meta.runtime[PROJECTION_PLACEMENT_KEY]` (the same channel
    /// `AgentContextMiddleware` populates at spawn time) with
    /// `root_preference = ProjectRoot` and a custom `dir_template` changes
    /// BOTH which root is preferred (even though `work_dir` is ALSO
    /// present) AND the target directory layout the in-flight pointer
    /// materializes to.
    #[tokio::test]
    async fn execute_with_custom_projection_placement_uses_declared_root_and_template() {
        use mlua_swarm::core::projection_placement::{ProjectionPlacement, RootPreference};
        use mlua_swarm::Operator;
        use tokio::sync::mpsc;

        let work_dir = tempfile::TempDir::new().unwrap();
        let project_root = tempfile::TempDir::new().unwrap();
        let mut ctx = test_ctx("ST-proj-4");
        ctx.meta.runtime.insert(
            TASK_WORK_DIR_KEY.to_string(),
            Value::String(work_dir.path().to_string_lossy().into_owned()),
        );
        ctx.meta.runtime.insert(
            TASK_PROJECT_ROOT_KEY.to_string(),
            Value::String(project_root.path().to_string_lossy().into_owned()),
        );
        let placement = ProjectionPlacement {
            root_preference: RootPreference::ProjectRoot,
            dir_template: "custom/{task_id}/out".to_string(),
        };
        ctx.meta.runtime.insert(
            PROJECTION_PLACEMENT_KEY.to_string(),
            serde_json::to_value(&placement).expect("placement serializes"),
        );

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-proj-4").unwrap(),
            tx,
            None,
        ));

        let session_bg = session.clone();
        let handle = tokio::spawn(async move {
            session_bg
                .execute(
                    &ctx,
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });

        let sent = rx.recv().await.expect("Spawn sent");
        let req_id = match sent {
            ServerMsg::Spawn {
                req_id, directive, ..
            } => {
                assert!(
                    directive.contains("ctx_projection:"),
                    "directive missing ctx_projection pointer line: {directive}"
                );
                req_id
            }
            other => panic!("expected Spawn, got {other:?}"),
        };

        session
            .resolve_pending(
                &req_id,
                PendingReply::SpawnAck {
                    value: serde_json::json!({}),
                    ok: true,
                    error: None,
                    stats: None,
                },
            )
            .await;
        handle.await.expect("join").expect("execute Ok");

        let expected_file = project_root.path().join("custom/ST-proj-4/out/_ctx.md");
        assert!(
            expected_file.exists(),
            "materialized projection file missing at custom placement target {expected_file:?}"
        );
        let unexpected_file = work_dir
            .path()
            .join("workspace/tasks/ST-proj-4/ctx/_ctx.md");
        assert!(
            !unexpected_file.exists(),
            "declared root_preference=ProjectRoot must not fall back to work_dir: {unexpected_file:?}"
        );
    }

    // ──────────────────────────────────────────────────────────────
    // T-DISCARD: the Run-scoped drop (model §4.7)
    // ──────────────────────────────────────────────────────────────

    /// A `Ctx` carrying the Run identity a dispatch is launched under —
    /// the same `ctx.meta.runtime["run_id"]` key the router resolves a
    /// holder by.
    fn test_ctx_in_run(task_id: &str, run_id: &RunId) -> mlua_swarm::Ctx {
        let mut ctx = test_ctx(task_id);
        ctx.meta
            .runtime
            .insert("run_id".to_string(), Value::String(run_id.to_string()));
        ctx
    }

    /// Park a spawn on `session` for `ctx` and return its `req_id` once
    /// the frame is on the wire — i.e. once the pending entry exists.
    /// The join handle is dropped: these tests are about what happens to
    /// the entry, and a discarded spawn's waiter is expected to fail.
    async fn park_spawn(
        session: &std::sync::Arc<WSOperatorSession>,
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<ServerMsg>,
        ctx: mlua_swarm::Ctx,
    ) -> String {
        use mlua_swarm::Operator;

        let session_bg = session.clone();
        tokio::spawn(async move {
            session_bg
                .execute(
                    &ctx,
                    None,
                    "".into(),
                    Some(test_worker_binding()),
                    test_cap_token(),
                )
                .await
        });
        match rx.recv().await.expect("Spawn sent") {
            ServerMsg::Spawn { req_id, .. } => req_id,
            other => panic!("expected Spawn, got {other:?}"),
        }
    }

    /// What a caller does when it means "everything this session owes this
    /// Run": read the outstanding requests, then name them all.
    ///
    /// In production one filter sits between those two steps — the seat
    /// the acquire took (`SeatLedger::slot_of`, above the SAP). It is
    /// absent here on purpose: these tests are about what the session does
    /// with the names it is handed, and the seat is not a fact this layer
    /// has or should have.
    async fn discard_all_for_run(session: &WSOperatorSession, run_id: &RunId) -> usize {
        let named: Vec<String> = session
            .pending_for_run(run_id)
            .await
            .into_iter()
            .map(|request| request.req_id)
            .collect();
        session.discard_pending_requests(run_id, &named).await
    }

    /// **The spine of (d) axis 3.** The read answers exactly what a
    /// discard is selected from — same Run scope, same entries — and
    /// describes each one with the three fields that exist below the SAP.
    #[tokio::test]
    async fn the_unanswered_read_describes_what_a_discard_would_drop() {
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-pending-read").unwrap(),
            tx,
            None,
        ));
        let watched = RunId::new();
        let other_run = RunId::new();

        let first = park_spawn(&session, &mut rx, test_ctx_in_run("ST-read-1", &watched)).await;
        let second = park_spawn(&session, &mut rx, test_ctx_in_run("ST-read-2", &watched)).await;
        park_spawn(
            &session,
            &mut rx,
            test_ctx_in_run("ST-elsewhere", &other_run),
        )
        .await;

        let waiting = session.pending_for_run(&watched).await;
        assert_eq!(
            waiting.len(),
            2,
            "the read is scoped to one Run, exactly as the discard is"
        );
        let req_ids: Vec<&str> = waiting.iter().map(|r| r.req_id.as_str()).collect();
        assert!(req_ids.contains(&first.as_str()) && req_ids.contains(&second.as_str()));
        let steps: Vec<String> = waiting.iter().map(|r| r.step_id.to_string()).collect();
        assert!(steps.contains(&"ST-read-1".to_string()));
        for request in &waiting {
            assert_eq!(request.kind, PendingKind::Spawn);
            assert_eq!(
                request.attempt,
                Some(1),
                "a verb with a Ctx always has an attempt"
            );
        }

        // Reading does not resolve, drop, or otherwise disturb them: the
        // discard that follows still finds both (**W1** — the server acts
        // on nothing of its own accord).
        assert_eq!(
            discard_all_for_run(&session, &watched).await,
            2,
            "the read left the entries where they were"
        );
        assert!(
            session.pending_for_run(&watched).await.is_empty(),
            "and after the discard there is nothing left to describe"
        );
        assert_eq!(
            session.pending_for_run(&other_run).await.len(),
            1,
            "the other Run's request was never in scope for either call"
        );
    }

    /// The `ask` shortfall, from the read side: an entry with no Run is
    /// invisible to a Run-scoped read for the same reason it is invisible
    /// to a Run-scoped discard.
    #[tokio::test]
    async fn an_ask_is_not_described_by_a_run_scoped_read() {
        use mlua_swarm::SeniorBridge;
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-ask-read").unwrap(),
            tx,
            None,
        ));
        let run_id = RunId::new();
        let step = StepId::parse("ST-ask-read").unwrap();

        let session_bg = session.clone();
        tokio::spawn(async move { session_bg.ask(&step, "which lane?".into()).await });
        match rx.recv().await.expect("Ask sent") {
            ServerMsg::Ask { .. } => {}
            other => panic!("expected Ask, got {other:?}"),
        }

        assert!(
            session.pending_for_run(&run_id).await.is_empty(),
            "an entry with no Run cannot be selected by one, so the un-answered list \
             cannot name it either"
        );
    }

    /// **The spine of (c).** A discard addressed at one Run drops exactly
    /// that Run's in-flight requests, reports how many, and leaves every
    /// other Run's alone — the whole difference between `T-DISCARD` and
    /// the teardown drain `fail_pending` performs.
    #[tokio::test]
    async fn a_run_scoped_discard_drops_only_that_runs_requests() {
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-discard").unwrap(),
            tx,
            None,
        ));
        let displaced_run = RunId::new();
        let other_run = RunId::new();

        park_spawn(
            &session,
            &mut rx,
            test_ctx_in_run("ST-discard-1", &displaced_run),
        )
        .await;
        park_spawn(
            &session,
            &mut rx,
            test_ctx_in_run("ST-discard-2", &displaced_run),
        )
        .await;
        let survivor = park_spawn(&session, &mut rx, test_ctx_in_run("ST-keep", &other_run)).await;

        assert_eq!(
            discard_all_for_run(&session, &displaced_run).await,
            2,
            "T-DISCARD.confirm(run, discarded) counts what it dropped"
        );
        assert_eq!(
            discard_all_for_run(&session, &displaced_run).await,
            0,
            "a repeat discard has nothing left to drop"
        );

        // The untouched Run's entry is still resolvable, which is the
        // proof it was never dropped: `resolve_pending` on a missing
        // req_id is a silent no-op, so a surviving waiter is the only
        // observable difference.
        assert_eq!(
            discard_all_for_run(&session, &other_run).await,
            1,
            "the other Run's request was not collateral"
        );
        assert!(
            !survivor.is_empty(),
            "sanity: the survivor's req_id was captured"
        );
    }

    /// The `run` re-check the discard applies to every name it is handed.
    /// A caller selects from a read that has had time to go stale, so a
    /// `req_id` naming another Run's request is refused rather than
    /// honoured — the selection is made above the SAP, but it is not
    /// trusted blindly below it.
    #[tokio::test]
    async fn a_named_request_of_another_run_is_not_dropped() {
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-cross-run").unwrap(),
            tx,
            None,
        ));
        let displaced_run = RunId::new();
        let other_run = RunId::new();

        park_spawn(
            &session,
            &mut rx,
            test_ctx_in_run("ST-mine", &displaced_run),
        )
        .await;
        let elsewhere =
            park_spawn(&session, &mut rx, test_ctx_in_run("ST-theirs", &other_run)).await;

        assert_eq!(
            session
                .discard_pending_requests(&displaced_run, std::slice::from_ref(&elsewhere))
                .await,
            0,
            "the name belongs to another Run, so it is not this discard's to drop"
        );
        assert_eq!(
            session.pending_for_run(&other_run).await.len(),
            1,
            "and the request is still outstanding"
        );
    }

    /// The session is **not** torn down by a discard: the operator was
    /// displaced from one Run, not deleted. A send parked waiting for a
    /// reconnect must therefore still be parked (`TornDown` is what would
    /// have failed it), and the session still reports itself connectable.
    #[tokio::test]
    async fn a_discard_does_not_tear_the_session_down() {
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-discard-live").unwrap(),
            tx,
            None,
        ));
        let run_id = RunId::new();
        park_spawn(&session, &mut rx, test_ctx_in_run("ST-live", &run_id)).await;

        assert_eq!(discard_all_for_run(&session, &run_id).await, 1);

        assert!(
            session.is_connected().await,
            "a discard must not clear the sender"
        );
        assert_eq!(
            *session.conn.borrow(),
            ConnState::Connected,
            "a discard must not publish TornDown — the session outlives the handover"
        );
    }

    /// The documented shortfall, asserted rather than only described: an
    /// `ask` carries no `Ctx`, so its entry has no Run and no Run-scoped
    /// discard can select it.
    #[tokio::test]
    async fn an_ask_is_not_discardable_by_run_because_it_has_no_run() {
        use mlua_swarm::SeniorBridge;
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-ask-scope").unwrap(),
            tx,
            None,
        ));
        let run_id = RunId::new();

        let session_bg = session.clone();
        tokio::spawn(async move {
            session_bg
                .ask(
                    &mlua_swarm::StepId::parse("ST-ask").unwrap(),
                    serde_json::json!({"q": "which branch?"}),
                )
                .await
        });
        let req_id = match rx.recv().await.expect("Ask sent") {
            ServerMsg::Ask { req_id, .. } => req_id,
            other => panic!("expected Ask, got {other:?}"),
        };

        assert_eq!(
            discard_all_for_run(&session, &run_id).await,
            0,
            "an entry with no Run cannot be selected by one"
        );

        // Still live: the far end may answer it, and that answer still
        // resolves. What A6 does with a stale answer is a question for
        // the layer above the SAP.
        session
            .resolve_pending(&req_id, PendingReply::Answer(serde_json::json!("main")))
            .await;
        assert_eq!(
            discard_all_for_run(&session, &run_id).await,
            0,
            "and it was resolved, not left behind"
        );
    }

    /// A dispatch launched without a `RunContext` (`ctx.meta.runtime` has
    /// no `run_id`) parks an entry with no Run — the same unreachable
    /// case as `ask`, reached from the verb that normally does carry one.
    #[tokio::test]
    async fn a_spawn_without_a_run_context_is_not_discardable_either() {
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = std::sync::Arc::new(WSOperatorSession::new_with_base_url(
            SessionId::parse("S-no-run").unwrap(),
            tx,
            None,
        ));
        park_spawn(&session, &mut rx, test_ctx("ST-no-run")).await;

        assert_eq!(
            discard_all_for_run(&session, &RunId::new()).await,
            0,
            "no run_id on the dispatch means no Run to select it by"
        );
    }
}
