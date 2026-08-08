//! `AssigneeRouter`: the `Arc<dyn Operator>` that follows a Run's holder.
//!
//! # Why this type exists (model §4.3 **A10**)
//!
//! > Re-assignment happens N times, the lookup happens on every dispatch,
//! > and the destination is not baked in. **What performs that lookup is
//! > the `Arc<dyn Operator>` implementation, not the engine.**
//!
//! Before this type, the `Arc<dyn Operator>` an engine dispatch resolved
//! *was* a [`WSOperatorSession`] — one socket, baked in at compile time
//! (`OperatorSpawnerFactory`) or at login time (the engine registry). A
//! handover could rewrite the Run's holder all it liked; the dispatch
//! still went to whichever session had been baked in, so the handover was
//! not observable from the delivery side at all.
//!
//! [`AssigneeRouter`] is that missing indirection: it holds no session, it
//! holds a [`RunStore`] and the name of one Blueprint-declared Operator
//! slot. Every `execute` reads that slot's entry of `Run.current` afresh
//! and delegates to the adapter that the holder names right now.
//!
//! One router per slot: a Blueprint may declare several Operator seats
//! (`operators[]`, picked per agent via `spec.operator_ref`), and each
//! seat has its own holder over time, so each needs its own lookup.
//!
//! ```text
//!   engine dispatch                                    (T1 boundary)
//!        │                                                  ┆
//!        ▼                                                  ┆
//!   Arc<dyn Operator> = AssigneeRouter                       ┆
//!        │  1. ctx.meta.runtime["run_id"]                    ┆
//!        │  2. run_store.get(run_id).current[slot] ─ Assignee┤ stops here
//!        │  3. adapters.get(current.op)                      ┆
//!        ▼                                                  ┆
//!   Arc<dyn OperatorAdapter> = WSOperatorSession  ───────────┤ ctx / prompt /
//!        │                                                  ┆ worker / token
//!        ▼                                                  ┆ only
//!   the socket                                              ┆
//! ```
//!
//! # The Assignee stops here (model §4.7 **T1**)
//!
//! *Below T1 only the Operator is known — the Assignee does not cross the
//! SAP.* The delegation call therefore passes exactly the five arguments
//! [`Operator::execute`] already defines. Neither `gen` nor `desc` is
//! handed down, stashed on the `Ctx`, or spliced into the directive: an
//! adapter cannot behave differently depending on which generation
//! addressed it, because it is never told.
//!
//! # Why the router cannot route to itself
//!
//! The router is registered *into* the engine's operator registry (and the
//! `OperatorSpawnerFactory`'s), which is where a dispatch picks up an
//! `Arc<dyn Operator>`. If it resolved its adapters out of that same map,
//! a holder naming the key the router itself sits under would resolve to
//! the router, whose `execute` would resolve it again, forever — a hang,
//! or a stack overflow, on a data condition (`current.op` equal to a
//! registration key) that nothing in the model forbids.
//!
//! So the adapter side is a **separate map with a separate element type**:
//! [`OperatorAdapterRegistry`] stores `Arc<dyn OperatorAdapter>`, and
//! [`AssigneeRouter`] deliberately does **not** implement
//! [`OperatorAdapter`]. The recursion is not avoided by convention or by a
//! runtime guard that could be forgotten — a router simply cannot be
//! passed to [`OperatorAdapterRegistry::register`], because it is not of
//! the type that method accepts. Keep it that way: never write a blanket
//! `impl<T: Operator> OperatorAdapter for T`, which would hand the
//! guarantee back.
//!
//! # Why nothing here subscribes to connect / disconnect
//!
//! Model §4.7 defines `T-CONNECT.indication(operator)` and
//! `T-DISCONNECT.indication(operator, reason?)` as provider-initiated
//! events, and this layer subscribes to **neither**. That is not an
//! omission — it is what **T6** ("`T-CONNECT` is not used to decide the
//! holder") and **A7** ("the judgment is made at reference time; nothing
//! monitors periodically") ask for, read together.
//!
//! An event stream would only be worth carrying if some verdict were kept
//! between references. None is. [`AssigneeRouter::execute`] pulls
//! [`OperatorAdapter::liveness`] on the dispatch it is about to make and
//! acts on that answer immediately, so there is no cached state for an
//! indication to correct and nothing to invalidate. Subscribing would mean
//! keeping a second copy of connectivity up here — a copy that can only
//! ever be staler than the pull, and whose staleness would show up as a
//! dispatch delivered to an operator this layer still believed was
//! connected. **T7**: the layer above the SAP does not infer liveness, and
//! a remembered indication is exactly such an inference.
//!
//! The connectivity signal that *is* consumed lives below the boundary:
//! `WSOperatorSession` watches its own `ConnState` to unpark a send it
//! parked during a disconnect. That is `T-DELIVER` taking its time to
//! deliver, which **T2** leaves to the adapter's discretion, and it is
//! invisible from here — the router sees one `execute` call, however long
//! the socket took to come back.
//!
//! The one consequence worth naming: a dispatch that passes the liveness
//! check and *then* loses its socket is not released by **A7**, because
//! **A7** fires where it is read and this dispatch has already read it. It
//! parks below the boundary until the operator returns or the session is
//! torn down. Releasing it early would take a push subscription **and** a
//! deadline up here — the timer **T7** forbids. It is the next reference
//! that finds the seat Disconnected and vacates it.

use async_trait::async_trait;
use mlua_swarm::store::run::{RunStore, VacateOutcome};
use mlua_swarm::{
    CapToken, Ctx, Operator, OperatorSlotResolver, OperatorSpawnerFactory, RunId, WorkerBinding,
    WorkerError, WorkerResult,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::session::WSOperatorSession;

/// The `ctx.meta.runtime` key a dispatch carries its Run identity in.
///
/// Written by `Engine::dispatch_attempt_with` / `..._with_run_ctx` when the
/// launch supplied a `RunContext`; read here and by
/// [`WSOperatorSession::execute`] (which renders it into the Spawn
/// directive's observation route). The literal is the engine's, not this
/// module's — it is spelled out rather than imported because the engine
/// does not export it.
const RUN_ID_KEY: &str = "run_id";

/// The `state` of a `T-ALIVE.confirm` — the two values model §4.7 gives
/// that primitive, and the whole of what the layer above the SAP is told
/// about connectivity.
///
/// **T4**: this is a *projection* of whatever the adapter tracks
/// internally, not a copy of it. [`WSOperatorSession`] holds three states
/// (its `ConnState` adds a terminal `TornDown`); an adapter that grows a
/// fourth still answers here with one of these two. The primitive does not
/// widen when the implementation does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Liveness {
    /// The operator can be reached right now.
    Connected,
    /// It cannot. Says nothing about whether it will be again — that is
    /// the adapter's business (**T2**), and **O7**: this value is not the
    /// Operator's registration state.
    Disconnected,
}

/// A **terminal** Operator backend: something a dispatch can actually be
/// delivered to (a WS session, and in tests a double), as opposed to
/// something that decides where to deliver it.
///
/// This marker is the type-level half of the no-self-recursion argument in
/// the module doc: [`OperatorAdapterRegistry`] accepts only implementors,
/// and [`AssigneeRouter`] is not one. Implement it for backends that
/// terminate a dispatch; never for a type that resolves another
/// `Arc<dyn Operator>` and forwards to it, and never as a blanket impl
/// over [`Operator`].
///
/// # The SAP surface (model §4.7)
///
/// Two of the model's four primitive pairs cross this trait, and two do
/// not:
///
/// - **`T-DELIVER`** is [`Operator::execute`], which the supertrait
///   already provides. It is not redeclared here — one primitive, one
///   method.
/// - **`T-ALIVE`** is [`Self::liveness`], added because nothing else on
///   the boundary could answer it.
/// - **`T-CONNECT` / `T-DISCONNECT`** are provider-initiated indications
///   with no subscriber above the boundary; see the module doc.
/// - **`T-DISCARD`** is **deliberately absent**. It is defined over a
///   `run`, and no adapter can currently select by one (the WS session's
///   `pending` map is keyed by `req_id` alone and the wire carries no
///   `run_id`), so a method here could not be honoured. An unhonourable
///   primitive that merely *exists* reads, to whoever implements the rest
///   of the handover, as one already wired up. Adding it belongs with the
///   `pending` rework that can actually satisfy it.
#[async_trait]
pub trait OperatorAdapter: Operator {
    /// `T-ALIVE.request(operator)` → `T-ALIVE.confirm(operator, state)`.
    ///
    /// **T3 — this answers immediately.** It reports the state the adapter
    /// already holds; it must not probe the peer, await a round trip, or
    /// block on anything but its own lock. A caller reads this on the
    /// dispatch path (see [`AssigneeRouter::execute`]), where a wait would
    /// be indistinguishable from the delivery it is deciding whether to
    /// attempt.
    ///
    /// The answer is a fact about *this instant* and carries no promise
    /// about the next one. **T7**: nothing above the SAP may extrapolate
    /// from it — no cached verdict, no timer, no retry budget built on a
    /// previous answer.
    async fn liveness(&self) -> Liveness;
}

/// The one WS-side implementor: a session is exactly "the socket a spawn
/// is written to", the terminal case this marker describes.
#[async_trait]
impl OperatorAdapter for WSOperatorSession {
    /// Projects the session's connectivity onto the primitive's two values
    /// (**T4**).
    ///
    /// `is_connected` reads `tx` — `Some` exactly when a live sender is
    /// installed — under a `tokio::sync::Mutex` held for the read alone,
    /// which is the immediacy **T3** asks for.
    ///
    /// A torn-down session collapses into `Disconnected` rather than
    /// getting a value of its own: teardown clears `tx`, and from above
    /// the boundary "gone for good" and "away right now" call for the same
    /// decision (**T5** — release the seat). The distinction still exists
    /// below, where it decides whether a parked send may keep waiting.
    async fn liveness(&self) -> Liveness {
        if self.is_connected().await {
            Liveness::Connected
        } else {
            Liveness::Disconnected
        }
    }
}

/// `OperatorId` → the adapter that currently answers for it.
///
/// A second, deliberately separate registry from `Engine.operators` /
/// `OperatorSpawnerFactory.operators`. Those two answer the question "who
/// does this dispatch go to" and will hold the [`AssigneeRouter`] itself
/// (wired in the follow-up subtask); this one answers "what does an
/// `OperatorId` deliver through", and holds only real backends.
///
/// The key space is the model's `OperatorId` — the same one
/// [`crate::store::run::Assignee::op`](mlua_swarm::store::run::Assignee::op)
/// records, in which a session id (`S-<hex>`) and a role alias
/// (`main-ai`) are both first-class.
#[derive(Default)]
pub struct OperatorAdapterRegistry {
    adapters: RwLock<HashMap<String, Arc<dyn OperatorAdapter>>>,
}

impl OperatorAdapterRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind `op` to `adapter`, replacing any previous binding — same
    /// last-write-wins shape as `Engine::register_operator`, since the two
    /// are populated from the same login path.
    pub async fn register(&self, op: impl Into<String>, adapter: Arc<dyn OperatorAdapter>) {
        self.adapters.write().await.insert(op.into(), adapter);
    }

    /// Drop `op`'s binding. A missing key is a no-op.
    pub async fn unregister(&self, op: &str) {
        self.adapters.write().await.remove(op);
    }

    /// The adapter bound to `op`, if any.
    pub async fn get(&self, op: &str) -> Option<Arc<dyn OperatorAdapter>> {
        self.adapters.read().await.get(op).cloned()
    }

    /// Every bound `OperatorId`, sorted. Used to make the "holder names no
    /// adapter" failure legible instead of a bare miss.
    pub async fn ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.adapters.read().await.keys().cloned().collect();
        ids.sort();
        ids
    }
}

/// The `Arc<dyn Operator>` that resolves its destination from
/// `Run.current` on every dispatch. See the module doc.
pub struct AssigneeRouter {
    /// Where `Run.current` lives — the single place a slot's holder is
    /// recorded and the single place it is read from (**A10**).
    run_store: Arc<dyn RunStore>,
    /// Which slot (`operator_ref` — the Blueprint-declared Operator seat)
    /// this router speaks for.
    ///
    /// A Blueprint declares N seats and `Run.current` holds one holder per
    /// seat, so a router must know *which* seat's holder to look up. This
    /// is the one thing baked into the instance, and it is baked from the
    /// Blueprint (design-time), not from a session: **which seat** is
    /// static, **who holds it** is read fresh on every dispatch. That
    /// split is what keeps **A10** — the destination is still not baked
    /// in.
    slot: String,
    /// `OperatorId` → adapter. Separate from the registry this router is
    /// itself registered in; see the module doc.
    adapters: Arc<OperatorAdapterRegistry>,
}

/// The tail of the Vacant failure when the seat was simply never taken —
/// the launch seated nobody here and no handover has since.
const NEVER_SEATED: &str = "The launch seated every declared seat that had an operator registered \
     under its own name, and this one had none — nor did an operator_sid pin name it. Log in an \
     operator holding that role before launching, or pin the launch with operator_sid + \
     operator_desc (plus operator_slot when the Blueprint declares several seats). This used to \
     fail earlier, at compile time, with 'not registered in factory'; it now fails here, at the \
     first dispatch that needs the seat.";

/// The one Vacant failure, worded once and reached two ways: the seat was
/// already unheld when this dispatch read it, or **A7** released it a
/// moment ago because its holder was Disconnected.
///
/// They converge deliberately. From the dispatch's side the two are one
/// condition — this seat has nobody to deliver to — and **A8** gives them
/// one remedy, an acquire. Splitting them into two failure shapes would
/// invite a caller to handle one and not the other, when the difference is
/// only in how recently the seat emptied. `because` carries that history
/// in the message, where it informs without branching.
///
/// Model §4.3 **A6** calls this a service-unavailable condition;
/// [`WorkerError`] carries no status code, so it surfaces as a failure
/// whose message names the slot that has no holder. Note **R2**: the Run
/// itself is not stopped by being Vacant — only a dispatch that needs
/// *this* slot's holder is, and other slots keep dispatching.
fn vacant_failure(agent: &str, run_id: &RunId, slot: &str, because: &str) -> WorkerError {
    WorkerError::Failed(format!(
        "agent '{agent}': run {run_id} has no current holder for operator slot '{slot}' \
         (Vacant). {because}"
    ))
}

impl AssigneeRouter {
    /// Build a router for one slot, over a Run store and an adapter
    /// registry.
    pub fn new(
        run_store: Arc<dyn RunStore>,
        slot: impl Into<String>,
        adapters: Arc<OperatorAdapterRegistry>,
    ) -> Self {
        Self {
            run_store,
            slot: slot.into(),
            adapters,
        }
    }

    /// The slot this router resolves holders for.
    pub fn slot(&self) -> &str {
        &self.slot
    }

    /// The adapter registry this router resolves holders through — the
    /// handle the login path registers sessions into.
    pub fn adapters(&self) -> &Arc<OperatorAdapterRegistry> {
        &self.adapters
    }
}

#[async_trait]
impl Operator for AssigneeRouter {
    /// Resolve this Run's holder, check it is reachable, delegate, and
    /// check the answer is still this generation's to give.
    ///
    /// Six ways this returns without a delivered result, all loud:
    ///
    /// 1. **No `run_id` on the dispatch.** A dispatch with no Run identity
    ///    has no `current` to read, so there is no holder to resolve — and
    ///    no defensible guess either. There is no fallback here on
    ///    purpose: every production dispatch that can reach an Operator
    ///    session carries a `RunContext` (`POST /v1/tasks` and
    ///    `POST /v1/tasks/:id/runs`, their resume / rerun-from siblings,
    ///    all pass `Some(run_ctx)`), and the one path that can produce
    ///    `None` — the stdio MCP adapter's inline/file run, when its own
    ///    `RunRecord` create failed — rejects an `operator_sid` outright
    ///    and registers no Operator session to route to. Picking "the only
    ///    adapter" or "the last one registered" would invent an
    ///    addressing rule the model does not have.
    /// 2. **The Run is not readable** (unknown id, store failure).
    /// 3. **This router's slot is `Vacant`** — see [`vacant_failure`].
    /// 4. **The holder names no registered adapter.** The holder is a
    ///    fact; an adapter for it is not. Rather than fall through to
    ///    somewhere else, say which `OperatorId` could not be delivered to
    ///    and what was registered at the time.
    ///
    ///    This case is **not** folded into `Vacant`, tempting as the
    ///    resemblance is. A registry miss is a fact about wiring, not
    ///    about liveness: `T-ALIVE` cannot even be requested without an
    ///    adapter, so **A7**'s premise (`ALIVE(a.op) = Disconnected`) is
    ///    never established, and treating the miss as though it were is
    ///    the inference **T7** forbids. The model already has a path for
    ///    "that operator is gone" — **O8**, the cascade on `delete(op)`,
    ///    which runs where the deletion is known rather than guessing from
    ///    a lookup. And the guess would not be free: vacating burns a
    ///    generation and drops the holder, so an adapter that is merely
    ///    *not registered yet* (boot restores sessions before their
    ///    clients reconnect) would lose a valid assignment permanently,
    ///    where this failure merely fails the dispatch and lets the next
    ///    one succeed.
    /// 5. **The holder is `Disconnected`** (**A7**) — below.
    /// 6. **The generation moved while the adapter was answering**
    ///    (**A6**) — below.
    ///
    /// # A7 — the seat is released where it is read
    ///
    /// Between resolving the adapter and handing it the dispatch, this
    /// pulls `T-ALIVE`. `Disconnected` means **A5** forbids the delivery
    /// (`DELIVER` needs `current = Assigned(a) ∧ ALIVE(a.op) = Connected`),
    /// and **T5** says what to do about it, unconditionally: the seat
    /// becomes Vacant. So the seat is vacated and the dispatch fails as
    /// Vacant — no grace window, no retry, no second look, all of which
    /// would be this layer guessing that the operator is about to return
    /// (**T7**).
    ///
    /// The judgment happens here and nowhere else. **A7** says the state
    /// is examined *at reference time*; there is no sweeper walking Runs
    /// looking for disconnected holders, and adding one would be a second
    /// place where seats change hands, running on a timer, against
    /// operators nobody is dispatching to.
    ///
    /// **What is released is the holder, not the seat.** Two `.await`
    /// points separate the read of `current[slot]` from the release — the
    /// adapter lookup and `T-ALIVE` itself — and `POST /v1/runs/:id/acquire`
    /// never excludes (**A8**/**Q2**), so a seat can change hands inside
    /// that window. The release therefore carries the generation that was
    /// read
    /// ([`RunStore::vacate_assignee`](mlua_swarm::store::run::RunStore::vacate_assignee)),
    /// and the store applies it only while the seat still holds that exact
    /// [`Assignee`](mlua_swarm::store::run::Assignee). A
    /// [`VacateOutcome::Stale`] answer means the reading is out of date:
    /// nothing is written, the newer holder stands (**A8** already decided
    /// that contest), and the dispatch fails with a message naming who
    /// holds the seat now. Vacating unconditionally here would delete an
    /// assignment whose `T-ALIVE` was never requested — outside **A7**'s
    /// own premise, which names the `a` that was read — and the displaced
    /// acquirer would never learn it: it was answered `200` with its
    /// generation (**Q4**) and has no channel back.
    ///
    /// # A6 — a reply belongs to the generation that asked
    ///
    /// The generation the dispatch was addressed under is noted before
    /// delegating and `current` is read again after the adapter answers.
    /// If the seat has since been re-acquired — by anyone, **including the
    /// same operator** — the answer is the displaced holder's and is not
    /// accepted.
    ///
    /// This is why `gen` and not `from`: **A8** lets a seat be re-acquired
    /// by the operator that already held it, so a matching `from` proves
    /// nothing about *which* acquisition asked. `from` is already
    /// structurally guaranteed here — the reply comes back from the very
    /// adapter `assignee.op` resolved to, and no other — which leaves
    /// `gen` as the only one of **A6**'s three terms this position can
    /// still get wrong. Its third term, `ALIVE(a.op) = Connected`, is the
    /// check above.
    ///
    /// The check gates the `Ok` arm only. An `Err` is not a reply to be
    /// accepted or refused; it is the delivery itself having failed, and
    /// its diagnosis is worth more to the caller than a generic
    /// service-unavailable would be.
    ///
    /// On the success path the five arguments are forwarded verbatim. The
    /// [`Assignee`](mlua_swarm::store::run::Assignee) does not travel with
    /// them (**T1**) — neither `gen` nor `desc` is handed down, so an
    /// adapter cannot tell which generation addressed it.
    async fn execute(
        &self,
        ctx: &Ctx,
        system: Option<String>,
        prompt: Value,
        worker: Option<WorkerBinding>,
        worker_token: CapToken,
    ) -> Result<WorkerResult, WorkerError> {
        let Some(raw_run_id) = ctx.meta.runtime.get(RUN_ID_KEY).and_then(|v| v.as_str()) else {
            return Err(WorkerError::Failed(format!(
                "agent '{}': this dispatch carries no run_id, so the Run's current holder \
                 cannot be resolved; an Operator dispatch must be launched with a RunContext",
                ctx.agent
            )));
        };
        let run_id = RunId::parse(raw_run_id).map_err(|e| {
            WorkerError::Failed(format!(
                "agent '{}': ctx.meta.runtime[\"run_id\"] = '{raw_run_id}' is not a run id: {e}",
                ctx.agent
            ))
        })?;
        let record = self.run_store.get(&run_id).await.map_err(|e| {
            WorkerError::Failed(format!(
                "agent '{}': run {run_id} could not be read to resolve its holder: {e}",
                ctx.agent
            ))
        })?;
        let Some(assignee) = record.current.get(&self.slot) else {
            return Err(vacant_failure(
                &ctx.agent,
                &run_id,
                &self.slot,
                NEVER_SEATED,
            ));
        };
        let Some(adapter) = self.adapters.get(&assignee.op).await else {
            let registered = self.adapters.ids().await;
            return Err(WorkerError::Failed(format!(
                "agent '{}': run {run_id} is held by operator '{}', which has no registered \
                 Operator adapter (registered: [{}])",
                ctx.agent,
                assignee.op,
                registered.join(", ")
            )));
        };

        // A7 / T5. Read once, act on that reading — and release exactly the
        // holder that was read, never whoever occupies the seat by the time
        // the release lands.
        if adapter.liveness().await == Liveness::Disconnected {
            let outcome = self
                .run_store
                .vacate_assignee(&run_id, &self.slot, assignee.gen)
                .await
                .map_err(|e| {
                    tracing::warn!(
                        run_id = %run_id,
                        slot = %self.slot,
                        op = %assignee.op,
                        error = %e,
                        "A7: holder is Disconnected but the seat could not be released"
                    );
                    WorkerError::Failed(format!(
                        "agent '{}': run {run_id} is held by operator '{}' for operator slot \
                         '{}', which is Disconnected; the seat could not be released (A7) \
                         and the dispatch was not delivered: {e}",
                        ctx.agent, assignee.op, self.slot
                    ))
                })?;
            if let VacateOutcome::Stale { current } = outcome {
                // Somebody acquired the seat between the read above and
                // this write, so the holder that was found Disconnected is
                // no longer the holder — releasing now would delete an
                // assignment whose liveness was never asked for, which is
                // outside A7's premise and is a lost update rather than
                // A8's last-writer-wins. Nothing was written; the dispatch
                // still fails, because it resolved its adapter from the
                // holder that has since been displaced.
                let now = current
                    .as_ref()
                    .map(|c| format!("operator '{}' at generation {}", c.op, c.gen))
                    .unwrap_or_else(|| "nobody (Vacant)".to_string());
                tracing::info!(
                    run_id = %run_id,
                    slot = %self.slot,
                    op = %assignee.op,
                    addressed_gen = assignee.gen,
                    "A7: the seat changed hands before the release landed; left alone"
                );
                return Err(WorkerError::Failed(format!(
                    "agent '{}': run {run_id} was resolved to operator '{}' at generation {} for \
                     operator slot '{}', which was Disconnected — but the seat now holds {}, so \
                     it was left alone (A7 releases the holder it read, not the seat) and the \
                     dispatch was not delivered. Dispatch again to address the current holder.",
                    ctx.agent, assignee.op, assignee.gen, self.slot, now
                )));
            }
            tracing::info!(
                run_id = %run_id,
                slot = %self.slot,
                op = %assignee.op,
                addressed_gen = assignee.gen,
                "A7: holder was Disconnected at reference time; seat released"
            );
            return Err(vacant_failure(
                &ctx.agent,
                &run_id,
                &self.slot,
                &format!(
                    "Its holder, operator '{}', was Disconnected when this dispatch read it, so \
                     the seat was released (A7 — the state is examined at reference time, and \
                     nothing scans for this in the background). Acquire the seat for a \
                     reachable operator to dispatch again.",
                    assignee.op
                ),
            ));
        }

        // A6: which acquisition this dispatch is speaking on behalf of.
        let addressed_gen = assignee.gen;
        let result = adapter
            .execute(ctx, system, prompt, worker, worker_token)
            .await?;

        let after = self.run_store.get(&run_id).await.map_err(|e| {
            tracing::warn!(
                run_id = %run_id,
                slot = %self.slot,
                error = %e,
                "A6: the reply could not be checked against the current generation"
            );
            WorkerError::Failed(format!(
                "agent '{}': run {run_id} could not be re-read to check whether the reply for \
                 operator slot '{}' is still generation {addressed_gen}'s, so it was not \
                 accepted (A6): {e}",
                ctx.agent, self.slot
            ))
        })?;
        match after.current.get(&self.slot) {
            Some(current) if current.gen == addressed_gen => Ok(result),
            Some(current) => Err(WorkerError::Failed(format!(
                "agent '{}': run {run_id} answered for operator slot '{}' under generation \
                 {addressed_gen} (operator '{}'), but the seat is now generation {} (operator \
                 '{}'). The reply is the displaced holder's and is not accepted (A6).",
                ctx.agent, self.slot, assignee.op, current.gen, current.op
            ))),
            None => Err(WorkerError::Failed(format!(
                "agent '{}': run {run_id} answered for operator slot '{}' under generation \
                 {addressed_gen} (operator '{}'), but the seat is now Vacant, so the reply is \
                 not accepted (A6).",
                ctx.agent, self.slot, assignee.op
            ))),
        }
    }

    /// `true`, matching the backends this router fronts: it exists to sit
    /// in front of WS thin-path sessions, which require a worker binding
    /// (see [`WSOperatorSession::requires_worker_binding`]). Reporting
    /// `false` here would move that compile-time gate to dispatch time for
    /// every routed agent.
    fn requires_worker_binding(&self) -> bool {
        true
    }
}

/// Builds one [`AssigneeRouter`] per Blueprint-declared Operator seat, on
/// demand, for [`OperatorSpawnerFactory`].
///
/// This is the whole of "who answers `spec.operator_ref`" on the server:
/// every seat resolves to a router over the same Run store and the same
/// adapter registry, differing only in the seat name baked into it. Seats
/// are not enumerated up front because they are a property of each compiled
/// Blueprint, not of the process — a router is cheap (three `Arc`s) and
/// holds nothing that would need cleaning up.
///
/// Every seat is served: an `operator_ref` naming no declared Operator is
/// already a `CompileError::UnresolvedOperatorRef` before the factory is
/// reached, and a seat with no holder is a dispatch-time failure that names
/// the seat (see [`AssigneeRouter::execute`]) rather than a compile-time
/// one that would depend on who happened to be logged in at compile time.
pub struct AssigneeRouterResolver {
    run_store: Arc<dyn RunStore>,
    adapters: Arc<OperatorAdapterRegistry>,
}

impl AssigneeRouterResolver {
    /// Resolve every seat to a router over `run_store` and `adapters`.
    pub fn new(run_store: Arc<dyn RunStore>, adapters: Arc<OperatorAdapterRegistry>) -> Self {
        Self {
            run_store,
            adapters,
        }
    }
}

impl OperatorSlotResolver for AssigneeRouterResolver {
    fn resolve(&self, slot: &str) -> Option<Arc<dyn Operator>> {
        Some(Arc::new(AssigneeRouter::new(
            self.run_store.clone(),
            slot,
            self.adapters.clone(),
        )))
    }
}

/// The server's Operator wiring, as one value: the
/// [`OperatorSpawnerFactory`] a compile resolves `kind = Operator` agents
/// through, and the [`OperatorAdapterRegistry`] the login path registers
/// sessions into.
///
/// The two have to agree — the routers the factory hands out resolve
/// holders through exactly this registry — and [`Self::new`] is what makes
/// disagreeing impossible: it installs the resolver over the registry it
/// returns, so a caller cannot wire the factory to one registry and the
/// login path to another. That mismatch would be silent (launches accepted,
/// every dispatch failing with "holder names no registered adapter"), which
/// is why it is designed out rather than documented.
#[derive(Clone)]
pub struct WsOperatorWiring {
    /// Handed to the `SpawnerRegistry` and to the router builder.
    pub factory: Arc<OperatorSpawnerFactory>,
    /// Written by `login::register_operator_session`, read by every router.
    pub adapters: Arc<OperatorAdapterRegistry>,
}

impl WsOperatorWiring {
    /// Pair `factory` with a fresh adapter registry and install the
    /// [`AssigneeRouterResolver`] that resolves seats to routers over
    /// `run_store`.
    ///
    /// `run_store` must be the same store the router builder is given: it
    /// is where `Run.current` is written by a launch pin and by a handover,
    /// and where every dispatch reads it back (**A10** — one place).
    pub fn new(factory: Arc<OperatorSpawnerFactory>, run_store: Arc<dyn RunStore>) -> Self {
        let adapters = Arc::new(OperatorAdapterRegistry::new());
        factory.set_slot_resolver(Arc::new(AssigneeRouterResolver::new(
            run_store,
            adapters.clone(),
        )));
        Self { factory, adapters }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mlua_swarm::store::run::{InMemoryRunStore, RunRecord, RunStatus};
    use mlua_swarm::{Role, StepId, TaskId};
    use std::sync::Mutex as StdMutex;

    /// The Blueprint-declared Operator slot the routers under test speak
    /// for, and a sibling seat used to show one slot's traffic is not the
    /// other's.
    const SLOT: &str = "phase-a-op";
    const OTHER_SLOT: &str = "phase-b-op";

    /// Records every dispatch it is handed, and answers with its own name
    /// so a test can tell which adapter a dispatch landed on.
    struct RecordingAdapter {
        name: &'static str,
        seen: Arc<StdMutex<Vec<String>>>,
        /// What this double answers `T-ALIVE` with. Fixed per instance:
        /// the router reads it once per dispatch, so a test that wants a
        /// disconnected holder wants it disconnected for that read.
        liveness: Liveness,
    }

    #[async_trait]
    impl Operator for RecordingAdapter {
        async fn execute(
            &self,
            ctx: &Ctx,
            _system: Option<String>,
            _prompt: Value,
            _worker: Option<WorkerBinding>,
            _worker_token: CapToken,
        ) -> Result<WorkerResult, WorkerError> {
            self.seen
                .lock()
                .expect("recording adapter mutex")
                .push(format!("{}:{}", self.name, ctx.agent));
            Ok(WorkerResult {
                value: serde_json::json!({ "delivered_to": self.name }),
                ok: true,
                stats: None,
            })
        }
    }

    #[async_trait]
    impl OperatorAdapter for RecordingAdapter {
        async fn liveness(&self) -> Liveness {
            self.liveness
        }
    }

    fn adapter(name: &'static str, seen: &Arc<StdMutex<Vec<String>>>) -> Arc<dyn OperatorAdapter> {
        Arc::new(RecordingAdapter {
            name,
            seen: seen.clone(),
            liveness: Liveness::Connected,
        })
    }

    /// A registered adapter whose operator is away — the A7 condition.
    fn away_adapter(
        name: &'static str,
        seen: &Arc<StdMutex<Vec<String>>>,
    ) -> Arc<dyn OperatorAdapter> {
        Arc::new(RecordingAdapter {
            name,
            seen: seen.clone(),
            liveness: Liveness::Disconnected,
        })
    }

    /// Answers normally, but re-acquires the seat for `next_op` first —
    /// the shape of another acquire landing while the holder it displaces
    /// is still composing its reply. Deterministic where a real race is
    /// not: the handover is guaranteed to be committed before this
    /// `execute` returns, which is exactly the window **A6** is about.
    struct HandsOverWhileAnswering {
        store: Arc<dyn RunStore>,
        run_id: RunId,
        next_op: &'static str,
    }

    #[async_trait]
    impl Operator for HandsOverWhileAnswering {
        async fn execute(
            &self,
            _ctx: &Ctx,
            _system: Option<String>,
            _prompt: Value,
            _worker: Option<WorkerBinding>,
            _worker_token: CapToken,
        ) -> Result<WorkerResult, WorkerError> {
            self.store
                .acquire_assignee(&self.run_id, SLOT, self.next_op, "took over mid-answer")
                .await
                .expect("the interleaved acquire");
            Ok(WorkerResult {
                value: serde_json::json!({ "delivered_to": "answered-late" }),
                ok: true,
                stats: None,
            })
        }
    }

    #[async_trait]
    impl OperatorAdapter for HandsOverWhileAnswering {
        async fn liveness(&self) -> Liveness {
            Liveness::Connected
        }
    }

    /// Answers `T-ALIVE` with `Disconnected` — the **A7** condition — but
    /// re-acquires the seat for `next_op` while doing so. That is the
    /// window between the router's read of `current[slot]` and its release:
    /// two `.await` points wide in production (the adapter lookup and this
    /// call), and `acquire` never excludes (**A8**/**Q2**). Deterministic
    /// where the real race is not — the handover is committed before the
    /// router can issue its release.
    struct HandsOverWhileAnsweringAlive {
        store: Arc<dyn RunStore>,
        run_id: RunId,
        next_op: &'static str,
    }

    #[async_trait]
    impl Operator for HandsOverWhileAnsweringAlive {
        async fn execute(
            &self,
            _ctx: &Ctx,
            _system: Option<String>,
            _prompt: Value,
            _worker: Option<WorkerBinding>,
            _worker_token: CapToken,
        ) -> Result<WorkerResult, WorkerError> {
            Err(WorkerError::Failed(
                "A5 forbids delivering to a Disconnected holder; this must never be called"
                    .to_string(),
            ))
        }
    }

    #[async_trait]
    impl OperatorAdapter for HandsOverWhileAnsweringAlive {
        async fn liveness(&self) -> Liveness {
            self.store
                .acquire_assignee(&self.run_id, SLOT, self.next_op, "took over mid-check")
                .await
                .expect("the interleaved acquire");
            Liveness::Disconnected
        }
    }

    fn ctx_for(run_id: Option<&RunId>) -> Ctx {
        let mut ctx = Ctx::new(StepId::parse("ST-router").expect("step id"), 1, "scout");
        if let Some(run_id) = run_id {
            ctx.meta.runtime.insert(
                RUN_ID_KEY.to_string(),
                serde_json::json!(run_id.to_string()),
            );
        }
        ctx
    }

    fn cap_token() -> CapToken {
        CapToken {
            agent_id: "scout".into(),
            role: Role::Worker,
            scopes: vec!["*".into()],
            issued_at: 0,
            expire_at: u64::MAX / 2,
            max_uses: None,
            nonce: "test-nonce".into(),
            sig_hex: String::new(),
        }
    }

    async fn seeded_run(store: &Arc<dyn RunStore>) -> RunId {
        let run_id = RunId::new();
        store
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
            .expect("seed run");
        run_id
    }

    fn delivered_to(result: &WorkerResult) -> String {
        result.value["delivered_to"]
            .as_str()
            .expect("adapter answered with its name")
            .to_string()
    }

    /// **The spine.** One router, one Run, two dispatches — with the
    /// holder re-assigned in between. The second dispatch must land on the
    /// new holder's adapter, without the router (or anything else in the
    /// engine) being re-registered or rebuilt. That is A10: the
    /// destination was never baked in.
    #[tokio::test]
    async fn re_assigning_current_moves_the_next_dispatch_to_the_new_holder() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let run_id = seeded_run(&store).await;
        let seen = Arc::new(StdMutex::new(Vec::new()));

        let adapters = Arc::new(OperatorAdapterRegistry::new());
        adapters.register("S-first", adapter("first", &seen)).await;
        adapters
            .register("S-second", adapter("second", &seen))
            .await;

        let router = AssigneeRouter::new(store.clone(), SLOT, adapters);
        let ctx = ctx_for(Some(&run_id));

        store
            .acquire_assignee(&run_id, SLOT, "S-first", "launch pin")
            .await
            .expect("first acquire");
        let out = router
            .execute(&ctx, None, serde_json::json!("go"), None, cap_token())
            .await
            .expect("dispatch under the first holder");
        assert_eq!(delivered_to(&out), "first");

        // The handover. Nothing about the router or the registries changes.
        let (gen, displaced) = store
            .acquire_assignee(&run_id, SLOT, "S-second", "took over")
            .await
            .expect("second acquire");
        assert_eq!(gen, 2, "A4: the second assignment event is generation 2");
        assert_eq!(
            displaced.expect("the first holder was displaced").op,
            "S-first"
        );

        let out = router
            .execute(&ctx, None, serde_json::json!("go"), None, cap_token())
            .await
            .expect("dispatch under the second holder");
        assert_eq!(
            delivered_to(&out),
            "second",
            "re-assigning current must change where the next dispatch lands"
        );

        assert_eq!(
            *seen.lock().expect("seen"),
            vec!["first:scout".to_string(), "second:scout".to_string()],
            "each dispatch was delivered exactly once, to the holder of its moment"
        );
    }

    /// The lookup happens per dispatch, not once: vacating between two
    /// dispatches stops the second one even though the first succeeded.
    #[tokio::test]
    async fn vacating_between_dispatches_stops_the_next_one() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let run_id = seeded_run(&store).await;
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let adapters = Arc::new(OperatorAdapterRegistry::new());
        adapters
            .register("S-holder", adapter("holder", &seen))
            .await;
        let router = AssigneeRouter::new(store.clone(), SLOT, adapters);
        let ctx = ctx_for(Some(&run_id));

        let (held_gen, _) = store
            .acquire_assignee(&run_id, SLOT, "S-holder", "holds it")
            .await
            .expect("acquire");
        router
            .execute(&ctx, None, serde_json::json!("go"), None, cap_token())
            .await
            .expect("dispatch while held");

        store
            .vacate_assignee(&run_id, SLOT, held_gen)
            .await
            .expect("vacate");
        let err = router
            .execute(&ctx, None, serde_json::json!("go"), None, cap_token())
            .await
            .expect_err("a Vacant Run has no holder to dispatch to");
        assert!(
            err.to_string().contains("no current holder"),
            "the failure must name the Vacant condition: {err}"
        );
        assert_eq!(
            seen.lock().expect("seen").len(),
            1,
            "the vacated dispatch must not reach any adapter"
        );
    }

    /// A dispatch with no `run_id` fails loud. Measured, not assumed: no
    /// live path reaches a WS operator without one (see `execute`'s doc),
    /// so there is nothing to fall back to.
    #[tokio::test]
    async fn a_dispatch_without_a_run_id_fails_loud() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let adapters = Arc::new(OperatorAdapterRegistry::new());
        // A sole registered adapter is exactly the tempting fallback.
        adapters.register("S-only", adapter("only", &seen)).await;
        let router = AssigneeRouter::new(store, SLOT, adapters);

        let err = router
            .execute(
                &ctx_for(None),
                None,
                serde_json::json!("go"),
                None,
                cap_token(),
            )
            .await
            .expect_err("no run_id means no resolvable holder");
        assert!(
            err.to_string().contains("no run_id"),
            "the failure must name the missing run_id: {err}"
        );
        assert!(
            seen.lock().expect("seen").is_empty(),
            "the sole registered adapter must not be used as a fallback"
        );
    }

    /// A holder naming no registered adapter fails loud, and says both
    /// which `OperatorId` could not be delivered to and what was
    /// registered — rather than falling through to another adapter.
    #[tokio::test]
    async fn a_holder_with_no_adapter_fails_loud_naming_both_sides() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let run_id = seeded_run(&store).await;
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let adapters = Arc::new(OperatorAdapterRegistry::new());
        adapters.register("S-live", adapter("live", &seen)).await;
        let router = AssigneeRouter::new(store.clone(), SLOT, adapters);

        store
            .acquire_assignee(&run_id, SLOT, "S-gone", "held by a session that left")
            .await
            .expect("acquire");
        let err = router
            .execute(
                &ctx_for(Some(&run_id)),
                None,
                serde_json::json!("go"),
                None,
                cap_token(),
            )
            .await
            .expect_err("an unroutable holder cannot be delivered to");
        let msg = err.to_string();
        assert!(msg.contains("S-gone"), "must name the holder: {msg}");
        assert!(
            msg.contains("S-live"),
            "must name what was registered: {msg}"
        );
        assert!(
            seen.lock().expect("seen").is_empty(),
            "no other adapter may absorb the dispatch"
        );
    }

    /// A router speaks for ONE slot: a holder assigned to a different seat
    /// of the same Run is not this router's holder, and borrowing it would
    /// deliver a dispatch to whoever happens to hold some other lane.
    #[tokio::test]
    async fn a_holder_of_another_slot_is_not_this_routers_holder() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let run_id = seeded_run(&store).await;
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let adapters = Arc::new(OperatorAdapterRegistry::new());
        adapters
            .register("S-other", adapter("other-lane", &seen))
            .await;
        let router = AssigneeRouter::new(store.clone(), SLOT, adapters);

        store
            .acquire_assignee(&run_id, OTHER_SLOT, "S-other", "holds the other lane")
            .await
            .expect("acquire");

        let err = router
            .execute(
                &ctx_for(Some(&run_id)),
                None,
                serde_json::json!("go"),
                None,
                cap_token(),
            )
            .await
            .expect_err("this router's own slot is still Vacant");
        assert!(
            err.to_string().contains(SLOT),
            "the failure must name the slot that has no holder: {err}"
        );
        assert!(
            seen.lock().expect("seen").is_empty(),
            "another slot's holder must not absorb this slot's dispatch"
        );
    }

    /// **Per-lane independence.** Two seats of one Run, two routers, and a
    /// handover on one of them: the re-assigned seat's next dispatch moves,
    /// the other seat's does not.
    ///
    /// This is the routing contract behind the per-lane alias split the
    /// operator-execution-model guide documents (`phase_a_op` /
    /// `phase_b_op` as independent registry keys): with a Run-wide holder,
    /// re-assigning one lane would drag the other lane's traffic along with
    /// it, and a two-lane Blueprint could not hand over one lane at a time
    /// at all. `Run.current` being keyed by seat is what keeps the lanes
    /// separable — asserted here on both sides, so a regression that
    /// collapses the map back to a single holder fails on the untouched
    /// lane rather than passing quietly.
    #[tokio::test]
    async fn re_assigning_one_slot_leaves_the_other_slots_destination_alone() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let run_id = seeded_run(&store).await;
        let seen = Arc::new(StdMutex::new(Vec::new()));

        let adapters = Arc::new(OperatorAdapterRegistry::new());
        adapters
            .register("S-a-first", adapter("a-first", &seen))
            .await;
        adapters
            .register("S-a-second", adapter("a-second", &seen))
            .await;
        adapters.register("S-b", adapter("b", &seen)).await;

        // One router per seat, sharing one Run and one adapter registry.
        let router_a = AssigneeRouter::new(store.clone(), SLOT, adapters.clone());
        let router_b = AssigneeRouter::new(store.clone(), OTHER_SLOT, adapters);
        let ctx = ctx_for(Some(&run_id));

        store
            .acquire_assignee(&run_id, SLOT, "S-a-first", "lane A launch pin")
            .await
            .expect("lane A acquire");
        store
            .acquire_assignee(&run_id, OTHER_SLOT, "S-b", "lane B launch pin")
            .await
            .expect("lane B acquire");

        let out_a = router_a
            .execute(&ctx, None, serde_json::json!("go"), None, cap_token())
            .await
            .expect("lane A dispatch");
        assert_eq!(delivered_to(&out_a), "a-first");
        let out_b = router_b
            .execute(&ctx, None, serde_json::json!("go"), None, cap_token())
            .await
            .expect("lane B dispatch");
        assert_eq!(delivered_to(&out_b), "b");

        // Hand lane A over. Lane B is not touched — not re-assigned, not
        // vacated, and its router is not rebuilt.
        let (gen, displaced) = store
            .acquire_assignee(&run_id, SLOT, "S-a-second", "lane A took over")
            .await
            .expect("lane A handover");
        assert_eq!(
            gen, 3,
            "A4: G is one counter for the whole Run — two launch pins then a handover is 3"
        );
        assert_eq!(
            displaced.expect("lane A had a holder").op,
            "S-a-first",
            "the displaced holder is lane A's, not lane B's"
        );

        let out_a = router_a
            .execute(&ctx, None, serde_json::json!("go"), None, cap_token())
            .await
            .expect("lane A dispatch after the handover");
        assert_eq!(
            delivered_to(&out_a),
            "a-second",
            "the re-assigned lane must follow its new holder"
        );
        let out_b = router_b
            .execute(&ctx, None, serde_json::json!("go"), None, cap_token())
            .await
            .expect("lane B dispatch after lane A's handover");
        assert_eq!(
            delivered_to(&out_b),
            "b",
            "the untouched lane must keep delivering to its own holder"
        );

        let record = store.get(&run_id).await.expect("run get");
        assert_eq!(
            record.current[SLOT].op, "S-a-second",
            "lane A's seat carries the new holder"
        );
        assert_eq!(
            record.current[OTHER_SLOT].op, "S-b",
            "lane B's seat is untouched by lane A's handover"
        );
        assert_eq!(
            record.current[OTHER_SLOT].gen, 2,
            "A3: an untouched holder's generation does not move"
        );

        assert_eq!(
            *seen.lock().expect("seen"),
            vec![
                "a-first:scout".to_string(),
                "b:scout".to_string(),
                "a-second:scout".to_string(),
                "b:scout".to_string(),
            ],
            "every dispatch landed on the holder of its own lane, at its own moment"
        );
    }

    /// The registry is keyed by `OperatorId`, so a role alias is a
    /// first-class holder — not a second-class one that needs a session id
    /// to be resolved through first.
    #[tokio::test]
    async fn a_role_alias_holder_routes_like_a_session_id_holder() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let run_id = seeded_run(&store).await;
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let adapters = Arc::new(OperatorAdapterRegistry::new());
        adapters
            .register("main-ai", adapter("by-alias", &seen))
            .await;
        let router = AssigneeRouter::new(store.clone(), SLOT, adapters);

        store
            .acquire_assignee(&run_id, SLOT, "main-ai", "assigned by role")
            .await
            .expect("acquire");
        let out = router
            .execute(
                &ctx_for(Some(&run_id)),
                None,
                serde_json::json!("go"),
                None,
                cap_token(),
            )
            .await
            .expect("an alias holder routes");
        assert_eq!(delivered_to(&out), "by-alias");
    }

    /// The router fronts WS thin-path sessions, so it inherits their
    /// compile-time worker-binding gate.
    #[test]
    fn the_router_requires_a_worker_binding() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let router = AssigneeRouter::new(store, SLOT, Arc::new(OperatorAdapterRegistry::new()));
        assert!(router.requires_worker_binding());
    }

    /// **A7 / T5.** Dispatching to a holder that is Disconnected does not
    /// deliver, does not wait for it to come back, and does not leave the
    /// seat held: the seat is released at the moment it was read, and the
    /// next reference finds it Vacant — so getting this Run moving again
    /// takes an acquire, not a reconnect.
    #[tokio::test]
    async fn a_disconnected_holder_is_released_at_reference_time_and_stays_released() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let run_id = seeded_run(&store).await;
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let adapters = Arc::new(OperatorAdapterRegistry::new());
        adapters
            .register("S-away", away_adapter("away", &seen))
            .await;
        let router = AssigneeRouter::new(store.clone(), SLOT, adapters);
        let ctx = ctx_for(Some(&run_id));

        store
            .acquire_assignee(&run_id, SLOT, "S-away", "held by a client that went quiet")
            .await
            .expect("acquire");

        let err = router
            .execute(&ctx, None, serde_json::json!("go"), None, cap_token())
            .await
            .expect_err("A5 forbids delivering to a Disconnected holder");
        let msg = err.to_string();
        assert!(
            msg.contains("no current holder"),
            "it fails as Vacant, the same condition an unheld seat fails as: {msg}"
        );
        assert!(
            msg.contains("S-away") && msg.contains("Disconnected"),
            "and says which holder was away: {msg}"
        );

        let record = store.get(&run_id).await.expect("run get");
        assert!(
            !record.current.contains_key(SLOT),
            "T5: the seat is Vacant, not merely undeliverable-to"
        );

        // The judgment stuck. A second reference does not re-derive a
        // holder from anywhere, so the Run needs an acquire to move.
        let err = router
            .execute(&ctx, None, serde_json::json!("go"), None, cap_token())
            .await
            .expect_err("the released seat is still Vacant");
        assert!(
            err.to_string().contains("no current holder"),
            "the release persisted: {err}"
        );

        assert!(
            seen.lock().expect("seen").is_empty(),
            "no dispatch reached the away operator's adapter"
        );
    }

    /// **A7 releases the holder it read, not the seat.** An acquire lands
    /// between the read of `current[slot]` and the release, so the release
    /// names a generation the seat no longer holds: it must write nothing
    /// and leave the new holder standing. Vacating unconditionally here
    /// would destroy a live assignment whose `T-ALIVE` was never requested
    /// — a lost update, and invisible to the acquirer, which was answered
    /// `200` with its generation (**Q4**).
    #[tokio::test]
    async fn a_stale_a7_release_does_not_disturb_the_current_holder() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let run_id = seeded_run(&store).await;
        let adapters = Arc::new(OperatorAdapterRegistry::new());
        adapters
            .register(
                "S-away",
                Arc::new(HandsOverWhileAnsweringAlive {
                    store: store.clone(),
                    run_id: run_id.clone(),
                    next_op: "S-fresh",
                }) as Arc<dyn OperatorAdapter>,
            )
            .await;
        let router = AssigneeRouter::new(store.clone(), SLOT, adapters);

        store
            .acquire_assignee(&run_id, SLOT, "S-away", "held by a client that went quiet")
            .await
            .expect("acquire");

        let err = router
            .execute(
                &ctx_for(Some(&run_id)),
                None,
                serde_json::json!("go"),
                None,
                cap_token(),
            )
            .await
            .expect_err("the dispatch resolved a holder that has since been displaced");
        let msg = err.to_string();
        assert!(
            msg.contains("(A7 releases the holder it read, not the seat)"),
            "the failure says why nothing was released: {msg}"
        );
        assert!(
            msg.contains("S-fresh") && msg.contains("generation 2"),
            "and names who holds the seat now: {msg}"
        );
        assert!(
            !msg.contains("no current holder"),
            "it must NOT report the seat as Vacant — the seat is held: {msg}"
        );

        let record = store.get(&run_id).await.expect("run get");
        let held = record
            .current
            .get(SLOT)
            .expect("the acquirer's seat survived the stale release");
        assert_eq!(
            held.op, "S-fresh",
            "A8 already decided this contest; the newer holder stands"
        );
        assert_eq!(held.gen, 2);
        assert_eq!(
            record.next_generation, 2,
            "the refused release burned no generation"
        );
    }

    /// **A6.** A reply that arrives after the seat has been re-acquired is
    /// the displaced holder's, and is refused rather than returned as this
    /// dispatch's result.
    #[tokio::test]
    async fn a_reply_is_refused_when_the_generation_moved_while_it_was_answering() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let run_id = seeded_run(&store).await;
        let adapters = Arc::new(OperatorAdapterRegistry::new());
        adapters
            .register(
                "S-first",
                Arc::new(HandsOverWhileAnswering {
                    store: store.clone(),
                    run_id: run_id.clone(),
                    next_op: "S-second",
                }) as Arc<dyn OperatorAdapter>,
            )
            .await;
        let router = AssigneeRouter::new(store.clone(), SLOT, adapters);

        store
            .acquire_assignee(&run_id, SLOT, "S-first", "the first holder")
            .await
            .expect("acquire");

        let err = router
            .execute(
                &ctx_for(Some(&run_id)),
                None,
                serde_json::json!("go"),
                None,
                cap_token(),
            )
            .await
            .expect_err("the answer belongs to a generation that no longer holds the seat");
        let msg = err.to_string();
        assert!(msg.contains("(A6)"), "the refusal names the rule: {msg}");
        assert!(
            msg.contains("generation 1") && msg.contains("S-second"),
            "and both generations it compared: {msg}"
        );
    }

    /// **A6 + A8, the reason the check is on `gen` and not on `from`.**
    /// The same operator re-acquires the seat mid-answer: every identity
    /// term still matches, and the reply is refused anyway, because it was
    /// the *previous* acquisition that asked.
    #[tokio::test]
    async fn a_reply_is_refused_even_when_the_same_operator_re_acquired() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let run_id = seeded_run(&store).await;
        let adapters = Arc::new(OperatorAdapterRegistry::new());
        adapters
            .register(
                "S-same",
                Arc::new(HandsOverWhileAnswering {
                    store: store.clone(),
                    run_id: run_id.clone(),
                    next_op: "S-same",
                }) as Arc<dyn OperatorAdapter>,
            )
            .await;
        let router = AssigneeRouter::new(store.clone(), SLOT, adapters);

        store
            .acquire_assignee(&run_id, SLOT, "S-same", "took the seat")
            .await
            .expect("acquire");

        let err = router
            .execute(
                &ctx_for(Some(&run_id)),
                None,
                serde_json::json!("go"),
                None,
                cap_token(),
            )
            .await
            .expect_err("a matching `from` does not make the reply this generation's");
        assert!(
            err.to_string().contains("(A6)"),
            "refused on the generation, with the operator identical on both sides: {err}"
        );

        let record = store.get(&run_id).await.expect("run get");
        assert_eq!(
            record.current[SLOT].op, "S-same",
            "Q7 / A8: the re-acquire stands — refusing the reply does not undo it"
        );
        assert_eq!(record.current[SLOT].gen, 2);
    }

    /// **T4.** The session's three internal states project onto the
    /// primitive's two, and the terminal one is not smuggled out as a
    /// third value: a torn-down session is `Disconnected`, which is the
    /// answer the seat-releasing decision above needs.
    #[tokio::test]
    async fn a_sessions_liveness_is_the_two_valued_projection_of_its_state() {
        use mlua_swarm::SessionId;
        use tokio::sync::mpsc;

        let (tx, _rx) = mpsc::unbounded_channel();
        let session =
            WSOperatorSession::new_with_base_url(SessionId::parse("S-alive").unwrap(), tx, None);
        assert_eq!(session.liveness().await, Liveness::Connected);

        session.clear_tx().await;
        assert_eq!(session.liveness().await, Liveness::Disconnected);

        session.fail_pending("torn down in a test").await;
        assert_eq!(
            session.liveness().await,
            Liveness::Disconnected,
            "TornDown is a distinction below the SAP; above it there are two values"
        );
    }
}
