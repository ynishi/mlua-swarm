//! REST-like Operator session resource.
//!
//! Provides the `POST/GET/DELETE /v1/operators` + `WS /v1/operators/:sid/ws`
//! route family — the sole WS Operator session route. `session.rs` /
//! `protocol.rs` are unchanged by this module.
//!
//! In the layered credential model (`mse://guides/auth-token-model`) the
//! session token issued here is **L1 identity**. Join itself needs no
//! Bearer — it *mints* one — which is exactly why the **L0 perimeter**
//! (`crate::access`) matters on a remote bind: with an access token
//! configured, issuance sits behind the perimeter instead of being open to
//! any network peer.
//!
//! ## Login flow
//!
//! ```text
//! POST /v1/operators { desc?: "...", capability_manifest?: {...} }
//!   → { sid: "S-<hex>", token: "<10-hex>" }
//!   → builds a disconnected `WSOperatorSession` and registers it into the
//!     engine's 3 registries (senior_bridge / spawn_hook / operator) under
//!     its sid. The sid is therefore usable as an `operator_sid` pin
//!     from the moment it is minted; it is not yet *reachable* (no socket).
//!   The manifest is pinned to this session and later resolved through the
//!   Core `AgentBindingProvider` interface before any Runner-backed spawn.
//!
//! WS /v1/operators/:sid/ws
//!   Authorization: Bearer <token>   (mandatory — no empty-string default)
//!   → 401 missing/empty Bearer, 404 unknown sid, 401 token mismatch
//!   → attaches this socket to the `WSOperatorSession` the sid already owns
//!     (`replace_tx`). Connect and reconnect are the same operation, and
//!     neither touches a registry — registration happened at mint, or at
//!     boot on the restore path.
//!
//! DELETE /v1/operators/:sid   (Bearer required)
//!   → unregisters the 3 registries + the adapter binding +
//!     `operator_sessions` entry, then vacates every Run seat still held
//!     under this sid (model O8 — `cascade_vacate_seats`; a delete leaves
//!     no holder behind).
//!
//! GET /v1/operators/:sid   (Bearer required)
//!   → { sid, connected, desc, observed, observed_total }
//!
//! GET /v1/operators   (Bearer required — any live session's token)
//!   → the 記名 list (model §4.2): every live session's identity, its
//!     join-time description (D1) and the seats it has been assigned (D2),
//!     newest activity first with a count limit (D5).
//! ```
//!
//! ## A join names nothing
//!
//! `POST /v1/operators` used to take `roles: ["main-ai"]`, reserve those
//! names process-globally in `AppState.roles_to_sid`, answer `409` when one
//! was taken, and register the session under each of them. The whole
//! apparatus is gone, and it is worth saying what replaced it: **nothing
//! did.** A role was a *server-wide* reservation, and Runs are concurrent —
//! two Runs wanting a `main-ai` each is the ordinary case, not a conflict —
//! so the name never had to be unique in the first place. Which operator
//! serves which lane is a per-Run fact, and it lives where every other
//! per-Run fact lives: `Run.current`, written by a launch pin or an
//! acquire (model §4.3).
//!
//! So the 409 did not need switching off; its subject stopped existing.
//! What that buys is not merely one fewer refusal:
//!
//! - `Assignee.op` is a session id on every path now — there is no other
//!   kind of string it could hold, which is what **O4** ("join issues a new
//!   `OperatorId` every time") means once the alias key space is gone;
//! - the adapter registry is keyed by sid alone, so the last-write-wins
//!   registration that let a second session claiming `main-ai` silently
//!   take over the first one's dispatches has no key to collide on.
//!
//! ## 記名 (model §4.2)
//!
//! A session carries two halves, and this module owns both:
//!
//! - the **confirmed part** — `desc`, written by the joining AI, fixed at
//!   join (**D1**), stored on [`OperatorSessionRecord::desc`];
//! - the **observed part** — one entry per `Assign`, written by the server
//!   from what it can actually read at that moment, appended only
//!   (**D2**). It lives on [`LoginSession::observed`] while the process
//!   runs and is written through to the session store on every append.
//!
//! Neither half is ever compared against anything (**D4**). A dispatch
//! resolves its destination from `Run.current` alone; the 記名 is for a
//! reader deciding "is this my work".
//!
//! ## Four types spell "session"; only the first belongs to this module
//!
//! - [`LoginSession`] — the value in `AppState.operator_sessions`, keyed by
//!   `sid`. Pairs the durable record with the WS session that sid
//!   dispatches through, and is what every route here resolves a request
//!   to.
//! - [`mlua_swarm::store::operator_session::OperatorSessionRecord`] — the
//!   durable half on its own, as persisted and rehydrated at boot.
//! - [`WSOperatorSession`] — the 3-trait WS session object (`session.rs`):
//!   the engine's dispatch target, whose sender is the only part that
//!   tracks connectivity.
//! - `mlua_swarm::LaunchEnvelope` — the engine-side `attach` record behind
//!   `/v1/sessions`: one per launch, holding that launch's token and the
//!   ids its dispatches rebuild `OperatorInfo` from. Unrelated to this
//!   route family, and no longer *named* like a session — which is the
//!   confusion the rename was for: the sessions this module owns are the
//!   two above, and they are the ones a sid addresses.

use axum::{
    extract::{
        ws::{close_code, CloseFrame, Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use futures_util::{sink::SinkExt, stream::StreamExt};
use mlua_swarm::store::operator_session::{
    ObservedAssignment, OperatorSessionRecord, OperatorSessionStoreError,
};
use mlua_swarm::store::run::{Assignee, RunListFilter, RunStatus, VacateOutcome};
use mlua_swarm::store::trace::TraceHandle;
use mlua_swarm::{AgentProviderManifest, Engine, Operator, SeniorBridge, SessionId, SpawnHook};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

use super::protocol::{ClientMsg, PendingReply, ServerMsg};
use super::router::{OperatorAdapter, OperatorAdapterRegistry};
use super::session::{WSOperatorSession, WorkerTokenMinter};
use crate::assignee_trace::{append_released, ReleaseReason};
use crate::AppState;

/// A minted Operator login session: the durable record plus the WS session
/// it dispatches through.
///
/// Held in `AppState.operator_sessions`, keyed by `sid`. The dispatch
/// target is attached before the value is published and is never
/// detached — not an `Option`. A teardown closes it in place instead, so
/// a route that captured this value across the WebSocket upgrade finds
/// the closed session and is answered with the Close frame the teardown
/// latched, rather than minting a replacement nothing could unregister.
pub struct LoginSession {
    /// Read-only snapshot of the persisted row's **immutable** half: the
    /// identity, the manifest, the mint time, and the 記名's
    /// confirmed part (**D1** — written at join and never rewritten). Its
    /// `token_digest` is never read directly — [`Self::verify_bearer`] is
    /// the one predicate over it.
    ///
    /// `record.observed` is **not** kept in step here; [`Self::observed`]
    /// is the live copy. See that field.
    record: OperatorSessionRecord,
    /// The 記名's observed part (**D2**), live.
    ///
    /// Split out of [`Self::record`] because the two halves of the 記名
    /// have opposite lifetimes and the split says so in the type: the
    /// confirmed part is a plain immutable field, the observed part is the
    /// only thing an `Assign` may touch. Rebuilding the whole
    /// `LoginSession` on each append is not an option — `LoginSession::new`
    /// mints a *new* [`WSOperatorSession`], and the old one is what the
    /// engine's registries and every upgraded socket already hold.
    ///
    /// A `tokio::sync::Mutex` rather than a `std` one because
    /// [`Self::record_observed`] holds it across the store write, which is
    /// what makes append-then-persist atomic per session: two concurrent
    /// `Assign`s cannot interleave into a `put` that writes back a log
    /// missing one of them. It is a leaf lock — nothing is acquired while
    /// it is held except the store's own internals — so there is no order
    /// to invert.
    observed: tokio::sync::Mutex<ObservedLog>,
    /// The 24h expiry's clock: when this session was last accessed, live.
    ///
    /// The horizon is model §4.1's state diagram
    /// (`最終アクセスから 24h ──▶ ╳ 削除`) and carries no predicate number
    /// — see
    /// [`OPERATOR_SESSION_MAX_IDLE_SECS`](mlua_swarm::store::operator_session::OPERATOR_SESSION_MAX_IDLE_SECS)
    /// for why this file does not call it `O1`.
    ///
    /// Split out of [`Self::record`] for the same reason
    /// [`Self::observed`] is — the record's other fields are fixed at join
    /// and this one is not. An `AtomicU64` rather than a `Mutex` because
    /// every write is a monotone `fetch_max` of a `u64` and no reader needs
    /// it consistent with anything else; `Relaxed` is enough for the same
    /// reason (the value is a timestamp compared against a 24h horizon, not
    /// a lock or a flag ordering other memory).
    last_access_secs: std::sync::atomic::AtomicU64,
    /// The value of [`Self::last_access_secs`] as last written through to
    /// the store — the other half of [`Self::touch`]'s write-coalescing.
    last_access_persisted_secs: std::sync::atomic::AtomicU64,
    /// Shared, not owned — the same object the engine's registries hold and a
    /// teardown latches.
    dispatch_target: Arc<WSOperatorSession>,
}

/// The mutable half of a session's 記名: the observed entries plus the
/// monotone count of how many `Assign`s produced them.
#[derive(Debug, Clone, Default)]
pub struct ObservedLog {
    /// Oldest first, bounded by `OBSERVED_CAP`.
    pub entries: Vec<ObservedAssignment>,
    /// Every `Assign` ever recorded, including folded and aged-out ones.
    pub total: u64,
}

impl LoginSession {
    /// Builds the session for `record` and the disconnected
    /// [`WSOperatorSession`] its sid dispatches through.
    ///
    /// `base_url` is the server's public HTTP root, rendered into this
    /// session's Spawn directives once a socket attaches.
    ///
    /// The record's observed part is lifted into [`Self::observed`], so a
    /// session restored at boot resumes with the log it was persisted with
    /// (**D2**: nothing removes entries, and a restart is not a removal).
    ///
    /// `token_minter` is handed to the WS session so a Spawn frame that
    /// parked across a disconnect can have its worker capability re-minted
    /// before it goes out (see
    /// [`WorkerTokenMinter`](super::session::WorkerTokenMinter)). Both
    /// production callers pass the engine; `None` sends parked frames
    /// exactly as they were built.
    pub(crate) fn new(
        record: OperatorSessionRecord,
        base_url: Option<Arc<str>>,
        token_minter: Option<Arc<dyn WorkerTokenMinter>>,
    ) -> Arc<Self> {
        let dispatch_target = Arc::new(WSOperatorSession::disconnected_with_base_url(
            record.sid.clone(),
            base_url,
            token_minter,
        ));
        let observed = ObservedLog {
            entries: record.observed.clone(),
            total: record.observed_total,
        };
        // Seeded from what was persisted, so a restored session resumes
        // the expiry clock where it left off rather than looking freshly
        // accessed — a restart is not an access.
        let last_access = record.last_access_secs();
        Arc::new(Self {
            record,
            observed: tokio::sync::Mutex::new(observed),
            last_access_secs: std::sync::atomic::AtomicU64::new(last_access),
            last_access_persisted_secs: std::sync::atomic::AtomicU64::new(last_access),
            dispatch_target,
        })
    }

    /// The durable login record this session was minted (or restored) from.
    ///
    /// Its `observed` / `observed_total` are the values as of that mint or
    /// restore. For the live 記名 use [`Self::kimei`].
    pub fn record(&self) -> &OperatorSessionRecord {
        &self.record
    }

    /// The full persisted record as it stands now — [`Self::record`] with
    /// the live observed part folded back in. This is what a `put` writes
    /// and what a read surface reports.
    pub async fn kimei(&self) -> OperatorSessionRecord {
        let observed = self.observed.lock().await;
        let mut record = self.record.clone();
        record.observed = observed.entries.clone();
        record.observed_total = observed.total;
        record.last_access_secs = self
            .last_access_secs
            .load(std::sync::atomic::Ordering::Relaxed);
        record
    }

    /// Record that something reached this session — §4.1's "最終アクセス".
    ///
    /// # What counts as an access
    ///
    /// Every call that proves the driver behind this session is still
    /// there: attaching a socket, reading the session's own row, and being
    /// assigned a seat (which [`Self::record_observed`] persists anyway, so
    /// it needs no separate touch). Reading *somebody else's* row does not
    /// touch theirs — a recovery driver enumerating the 記名 list must not
    /// keep the corpses it is reading alive.
    ///
    /// # The durable write is coalesced
    ///
    /// In memory the value always advances; the store is written at most
    /// once per [`TOUCH_PERSIST_INTERVAL_SECS`]. A busy driver would
    /// otherwise put a full session row — 記名 ring included — per HTTP
    /// call, to move a number the expiry compares against a 24-hour
    /// horizon. The staleness that buys is bounded by the interval and is
    /// four orders of magnitude below the horizon it feeds.
    ///
    /// A store failure is logged and swallowed, exactly as in
    /// [`Self::record_observed`] and for the same reason: an observability
    /// write must not decide whether a live session keeps working.
    pub(crate) async fn touch(
        &self,
        store: &Arc<dyn mlua_swarm::store::operator_session::OperatorSessionStore>,
    ) {
        use std::sync::atomic::Ordering::Relaxed;
        let now = now_secs();
        // Monotone: a clock that stepped back must not age the session.
        if self.last_access_secs.fetch_max(now, Relaxed) >= now {
            return;
        }
        if now.saturating_sub(self.last_access_persisted_secs.load(Relaxed))
            < TOUCH_PERSIST_INTERVAL_SECS
        {
            return;
        }
        // Held across the write for the same reason `record_observed`
        // holds it: both write the whole row, and serialising them on this
        // leaf lock is what stops a touch's snapshot from landing after —
        // and therefore erasing — an `Assign` appended beside it.
        let observed = self.observed.lock().await;
        let mut record = self.record.clone();
        record.observed = observed.entries.clone();
        record.observed_total = observed.total;
        record.last_access_secs = self.last_access_secs.load(Relaxed);
        self.last_access_persisted_secs
            .store(record.last_access_secs, Relaxed);
        if let Err(error) = store.put(record).await {
            tracing::warn!(
                sid = %self.record.sid,
                %error,
                "touch: the last-access clock could not be persisted; it is live in this \
                 process but the durable copy still reads older, so a restart in the next \
                 24h could expire a session that is in use"
            );
        }
    }

    /// The 24h horizon: is this session expired as of `now`?
    ///
    /// Two conditions, and the second is not in the model's text:
    ///
    /// - the record's own predicate
    ///   ([`OperatorSessionRecord::is_expired_at`]) — 24h since the last
    ///   access;
    /// - **and no socket is attached right now.**
    ///
    /// The connectivity clause is what stops the expiry from reaping a
    /// driver that is plainly present. §4.1's O7 says connectedness is not
    /// the Operator's *state*, and this does not make it one — the state is
    /// still `Registered`, and a disconnect still changes nothing. It is
    /// read as evidence about the *access* clock instead: a client holding
    /// a socket open is in contact with the server continuously, whether or
    /// not it has said anything, so treating that as an access is closer to
    /// 最終アクセス than pretending the last HTTP call was the last contact.
    /// Without it, a driver that connected, took no seat and sat idle for a
    /// day would have its session deleted out from under its live socket.
    pub(crate) async fn is_expired(&self, now: u64) -> bool {
        if self.dispatch_target.is_connected().await {
            return false;
        }
        self.kimei().await.is_expired_at(now)
    }

    /// Append one `Assign` to this session's observed part (**D2**) and
    /// write the session through to `store`.
    ///
    /// Best-effort on the persistence side: a store failure is logged and
    /// swallowed, leaving the in-memory 記名 updated and the durable copy
    /// one entry behind. The alternative — failing the `Assign` — would let
    /// an observability write decide whether a seat changes hands, which is
    /// the tail wagging the dog (the same call
    /// [`crate::assignee_trace`] makes for the trace rail).
    pub async fn record_observed(
        &self,
        store: &Arc<dyn mlua_swarm::store::operator_session::OperatorSessionStore>,
        entry: ObservedAssignment,
    ) {
        // Held across the write on purpose — see the field doc.
        let mut observed = self.observed.lock().await;
        let mut record = self.record.clone();
        record.observed = std::mem::take(&mut observed.entries);
        record.observed_total = observed.total;
        // An `Assign` is an access, and this write is the one that
        // is happening anyway — so the clock rides along rather than
        // needing a `touch` of its own.
        let now = now_secs();
        self.last_access_secs
            .fetch_max(now, std::sync::atomic::Ordering::Relaxed);
        record.last_access_secs = self
            .last_access_secs
            .load(std::sync::atomic::Ordering::Relaxed);
        self.last_access_persisted_secs.store(
            record.last_access_secs,
            std::sync::atomic::Ordering::Relaxed,
        );
        record.record_observed(entry);
        observed.entries = record.observed.clone();
        observed.total = record.observed_total;
        if let Err(error) = store.put(record).await {
            tracing::warn!(
                sid = %self.record.sid,
                %error,
                "record_observed: the 記名's observed part could not be persisted; \
                 it is live in this process but one entry behind on disk"
            );
        }
    }

    /// The WS session the engine dispatches to for this sid.
    ///
    /// Held inline rather than as a registry id: `mlua_swarm::LaunchEnvelope`
    /// persists ids and rebuilds its handles at dispatch time, which does not
    /// work here — a route that upgraded a socket must keep observing this
    /// same object after a teardown removed the sid from
    /// `AppState.operator_sessions`, when a registry lookup would resolve to
    /// nothing.
    pub fn dispatch_target(&self) -> &Arc<WSOperatorSession> {
        &self.dispatch_target
    }

    /// Constant-time check of a presented bearer against this session's
    /// stored token digest.
    pub fn verify_bearer(&self, bearer: &str) -> bool {
        self.record.verify_bearer(bearer)
    }
}

/// Wall-clock seconds since the epoch, or `0` from a clock that cannot
/// answer.
///
/// `0` is the oldest possible mint and the oldest possible access, which
/// makes such a session the one a recovery driver always picks — and, for
/// the expiry, one that `saturating_sub` can never age (the horizon is
/// measured *from* `now`, so a `now` of `0` expires nothing). Both are the
/// conservative direction for a broken clock.
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The shortest gap between two durable writes of the expiry's last-access
/// clock (see [`LoginSession::touch`]).
///
/// Sized as a fraction of the horizon it feeds
/// ([`mlua_swarm::store::operator_session::OPERATOR_SESSION_MAX_IDLE_SECS`],
/// 24h): a value this coarse can make a session look at most 60s more idle
/// than it is across a restart, against a horizon of 86,400. What it buys
/// is that a driver polling its own session every second writes one row a
/// minute instead of sixty.
const TOUCH_PERSIST_INTERVAL_SECS: u64 = 60;

/// The 24h horizon at the moment of observation: tear `live` down if it has gone
/// [`OPERATOR_SESSION_MAX_IDLE_SECS`](mlua_swarm::store::operator_session::OPERATOR_SESSION_MAX_IDLE_SECS)
/// unaccessed, and report whether it did.
///
/// # Where this is called from
///
/// §4.1's state diagram ends `Registered` two ways, `leave` and
/// `最終アクセスから 24h`. The first has a route; the second is this
/// function, and it is reached two ways.
///
/// **At every read of a session** — the WS upgrade, `GET /v1/operators/:sid`,
/// the 記名 list, the Bearer gate, and the store's boot-time `list`. Judging
/// at the read is what makes the promise absolute for anyone *asking*: an
/// expired session is never returned to anybody, decided by the code that is
/// about to answer for it. It is the same shape the two sibling judgments
/// use — **A7** decides a seat's liveness at reference time, **O8**'s cascade
/// fires at delete time and "nowhere else".
///
/// **And on a schedule**, through
/// [`sweep_expired_operator_sessions`] on the `operator-session-expiry`
/// periodic job ([`crate::periodic`]). The reads alone leave the deletion
/// waiting on an unrelated caller, and the deletion is not only a hiding: it
/// unregisters the session from the engine and the adapter registry and
/// cascades **O8** over every seat it still held. Those effects have a
/// reader that is not a read — a dispatch routed to a dead sid resolves
/// through the registries and *parks*, and parking is not a path that
/// expires anything. So without the sweep, "it goes on its own after 24
/// hours" is true only on a server somebody happens to be listing.
///
/// The job is legitimate under [`crate::periodic`]'s registration rule
/// precisely because of the paragraph above it: the predicate
/// ([`LoginSession::is_expired`]) and the effect
/// ([`teardown_operator_session`]) are the read path's, unchanged. The timer
/// supplies no judgment of its own, which is the whole difference from the
/// stale-`Run` sweeper `31fefc1` removed for inventing one.
///
/// # It is the same teardown a leave performs
///
/// [`teardown_operator_session`], so an expiry drops the persisted row,
/// all four registrations, the socket and the map entry, and cascades
/// **O8** over every seat the session still held. An expiry that left
/// seats behind would put the model's own `╳ 削除 ── cascade ──▶` arrow
/// (§4.1) in a state it does not have.
///
/// A teardown that fails is logged and the session is left intact — the
/// caller then answers about a session that still exists, which is true.
/// The next read judges it again.
async fn reap_if_expired(state: &AppState, sid: &SessionId, live: &Arc<LoginSession>) -> bool {
    if !live.is_expired(now_secs()).await {
        return false;
    }
    match teardown_operator_session(state, sid, live).await {
        Ok(()) => {
            tracing::info!(
                %sid,
                desc = live.record().desc.as_deref().unwrap_or("<none>"),
                "operator session expired (24h since last access, no socket attached); \
                 released with the same teardown a leave performs"
            );
            true
        }
        Err(error) => {
            tracing::warn!(
                %sid, %error,
                "expired operator session could not be released; it stays live and will be \
                 judged again on the next read"
            );
            false
        }
    }
}

/// One pass of the `operator-session-expiry` periodic job: apply the horizon
/// to every live session and report how many it released.
///
/// This is [`reap_if_expired`] over `AppState.operator_sessions`, and
/// deliberately nothing more — the timer's entire contribution is arriving
/// without a caller. See [`reap_if_expired`]'s "Where this is called from"
/// for why the reads are not enough on their own, and [`crate::periodic`]
/// for the rule that lets this be scheduled at all.
///
/// # The map is the whole population
///
/// Sweeping the in-memory map rather than the store covers every persisted
/// row too, because the two are kept in step in both directions: a mint
/// writes the row and inserts the map entry
/// (see [`operators_create`]'s ordering note), and boot rehydrates one live
/// session per surviving row ([`crate::OperatorSessionPersistence::restore`],
/// which already drops the expired ones rather than restoring them). A row
/// with no map entry is therefore not a thing this sweep would miss; it is a
/// thing that does not exist.
///
/// # It never fails as a whole
///
/// A teardown that fails is logged by [`reap_if_expired`] and leaves that
/// session intact for the next pass, exactly as it does on a read — one
/// session's failure is not the run's failure, so this returns `Ok` with the
/// count it did release. The snapshot is taken under the lock and released
/// before any teardown runs, because [`teardown_operator_session`] takes the
/// same lock to remove its entry.
pub(crate) async fn sweep_expired_operator_sessions(state: &AppState) -> u64 {
    let live: Vec<(SessionId, Arc<LoginSession>)> = {
        let map = state.operator_sessions.lock().await;
        map.iter()
            .map(|(sid, s)| (sid.clone(), s.clone()))
            .collect()
    };
    let mut released = 0;
    for (sid, session) in live {
        if reap_if_expired(state, &sid, &session).await {
            released += 1;
        }
    }
    released
}

// ─── POST /v1/operators (mint) ──────────────────────────────────────────────

/// Body for `POST /v1/operators`.
///
/// # There is no `roles` field
///
/// A join used to claim role aliases here and hold them against every
/// other session on the server. Role declaration moved onto the Run (see
/// the module doc), so the only thing a join carries about *what* this
/// session is for is [`Self::desc`] — which is read by a person or an AI
/// and matched against nothing (**D4**).
///
/// A caller that still sends `roles` is not refused: serde ignores the
/// unknown key, and the join succeeds as the sid-only join it now is.
/// Refusing would break the one thing a join must never break — a driver
/// getting in — for a field whose removal costs the caller nothing.
#[derive(Debug, Deserialize, Default)]
pub struct OperatorsCreateReq {
    /// Effective execution capabilities supplied by the Operator/MainAI.
    #[serde(default)]
    pub capability_manifest: Option<AgentProviderManifest>,
    /// The 記名's confirmed part (model §4.2, **D1**) — what this session
    /// is here to do, in about 50 characters, fixed at join.
    ///
    /// The instruction the joining AI is written for, verbatim from the
    /// model:
    ///
    /// > 担当している内容を **50 字程度**で書いてください。用途は、**同じ
    /// > repo / 同じ worktree で並行している別タスクと自分を見分けること**
    /// > です。あとであなた自身、または引き継ぐ相手が記名一覧を見て「これは
    /// > 自分の仕事か」を判断します。
    /// >
    /// > **以下は自動で記録されるので書かないでください** — repo path /
    /// > worktree path / Run id / goal / 開始時刻。
    /// >
    /// > 書くのは、いま触っている対象と、そこで何をしているか。直前の経緯が
    /// > あれば 1 つ足してください。
    ///
    /// # Why this is not required
    ///
    /// **A9** rejects an `Assign` without a `desc`; **D1-D5** name no such
    /// refusal, and the reason is in **D3**'s own rationale — join is
    /// deliberately the one unguarded step so that an incoming Assignee can
    /// always get in. A `400` here would put a gate on exactly that step.
    /// So a missing description is recorded as missing (the list reports
    /// `desc: null`) rather than refused, and the place it is *insisted*
    /// on is the tool an AI actually joins through (`mse_operator_join`),
    /// where the caller has the sentence for free.
    ///
    /// Blank input (`""`, whitespace) is stored as absent: it is the same
    /// state, and keeping two spellings of it would make every reader
    /// handle both.
    #[serde(default)]
    pub desc: Option<String>,
}

/// Response for `POST /v1/operators`.
#[derive(Debug, Serialize)]
pub struct OperatorsCreateResp {
    /// Newly minted session id (typed [`SessionId`]; serializes as the
    /// plain `S-<hex>` string — the wire shape is unchanged).
    pub sid: SessionId,
    /// Bearer auth token required on the WS upgrade and admin routes.
    pub token: String,
}

/// `POST /v1/operators`. Mints `sid` (`S-<hex>` — the shared `SessionId`
/// shape; issue #11) + a 128-bit bearer token
/// (`mlua_swarm::types::operator_bearer_token` — OS-RNG hex, unguessable
/// across calls and restarts, which is the point: this token is the sole
/// bearer secret on the short-handle path).
///
/// # This route cannot refuse
///
/// Every arm below is `200` or a `500` from the store: there is no
/// conflict left to detect, so the only way in for an incoming Assignee is
/// also the only way this can answer. That is **D3**'s carve-out
/// ("join 自体は無認証なので引き継ぎは妨げない") holding at the status-code
/// level as well as at the auth one.
///
/// # The bearer exists only inside this function
///
/// The response below is the one and only place the plaintext leaves the
/// server: everything retained afterwards — the `operator_sessions` value
/// and the persisted [`OperatorSessionRecord`] — holds
/// `hex(SHA-256(bearer))` instead, and every later check runs
/// [`OperatorSessionRecord::verify_bearer`] against that digest.
pub async fn operators_create(
    State(state): State<AppState>,
    Json(req): Json<OperatorsCreateReq>,
) -> Response {
    let capability_manifest = req.capability_manifest;
    // D1's value, normalised once at the boundary: blank is absent.
    let desc = req
        .desc
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    // The sid is the operator-session identity, so it mints in the same
    // `SessionId` shape (`S-<hex>`) as the engine-side session id — one
    // session-id form across the system (issue #11 observation 2; the old
    // `op-<uuid>` shape collided with the operator-backend registry prefix).
    // It is an identifier, not a secret: `token` is the sole bearer
    // credential on this path.
    let sid = SessionId::new();
    let token = mlua_swarm::types::operator_bearer_token();
    // Derived once, here: the plaintext `token` is moved into the response
    // at the end of this function and never stored anywhere.
    let token_digest = OperatorSessionRecord::digest_of(&token);

    // A clock that cannot answer yields `0` (see `now_secs`), which
    // `GET /v1/operators` reads as the oldest possible mint — so such a
    // session is the one a recovery driver picking "the oldest stale
    // session" always picks, permanently.
    let joined_at_secs = now_secs();

    // Write-through BEFORE the in-memory insert and the mint response: a
    // sid the client can see must already be durable, or a crash between
    // response and persist would resurrect the pre-persistence forced
    // logout this store exists to remove.
    let record = OperatorSessionRecord {
        sid: sid.clone(),
        token_digest,
        capability_manifest,
        joined_at_secs,
        // The expiry clock starts at the join: minting is the first access.
        last_access_secs: joined_at_secs,
        desc,
        // D2: the observed part starts empty and only ever grows, from the
        // `Assign` sites. A mint has assigned nothing yet.
        observed: Vec::new(),
        observed_total: 0,
    };
    if let Err(error) = state.operator_session_store.put(record.clone()).await {
        tracing::error!(%sid, %error, "operators_create: session persist failed");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("session persist failed: {error}") })),
        )
            .into_response();
    }

    // Registered here, at mint — the same shape the restore path uses at
    // boot (see [`restored_login_session`], which carries the
    // full "registered is not reachable" rationale). The session starts
    // disconnected: `after` (`send_oneway`) is dropped until a socket
    // attaches, while the three reply-expecting verbs (`spawn` / `ask` /
    // `hook_before`) park on `ConnState` until the first connect swaps a
    // sender in — see `WSOperatorSession::send_when_connected`.
    //
    // Registering at mint rather than on first WS connect is what keeps
    // the connect path out of the registries entirely. When the two were
    // split, a connect whose upgrade completed just before a teardown
    // landed would run afterwards, find no session on its (already
    // removed) entry, and register a fresh one — a registration no route
    // could reach to undo, since every teardown route looks the sid up in
    // `operator_sessions` first. Registering once, here, removes that
    // second registration site rather than trying to order it correctly.
    //
    // # Why after `put`, not before
    //
    // The alternative — register first, unregister on a `put` failure —
    // guards one fallible step with another. The durable write is already
    // this function's gate for every in-memory effect (see the
    // write-through note above), so registration sits on the same side of
    // it as the map insert below, and the failure path above stays exactly
    // as it was: answer `500`, leave nothing behind.
    //
    // # Reachable last: the map before the registries
    //
    // These two writes publish different things, and the order between
    // them decides what a half-built session can do. The registries are
    // what a *dispatch* resolves through — the engine's three by sid, the
    // adapter registry by holder — so registering is what makes a session
    // routable. `operator_sessions` is where a routed send finds its two
    // exits: `replace_tx` (reached only via `operators_ws_connect`) and
    // `ConnState::TornDown` (published only by `teardown_operator_session`)
    // both look the sid up in that map first.
    //
    // Registering first therefore opened a window in which a session was
    // routable with neither exit reachable. A dispatch landing in it parked
    // and stayed parked: the map had no entry for a connect to attach to
    // or for a teardown to close. It was bounded, not stuck — the park sits
    // inside the run driver, which ends at its own `sync_timeout_secs` or
    // detach TTL — but the run burned its whole ceiling and then reported a
    // timeout naming the wrong cause, where a session that had simply not
    // finished being built should fail in milliseconds. Reaching the window
    // needed this request to die between the two statements, which hyper
    // does for us whenever the peer disconnects mid-request (see the
    // dropped-response-future note on `operators_delete`).
    //
    // Publishing the map entry first closes it by making the map a
    // superset of the registries: *registered implies reachable-exits*. A
    // request that dies between the two now leaves a session that no
    // dispatch can reach at all — an `operator_sid` pin is refused at
    // launch with `400 no such registered operator session`, and a seat
    // naming it fails its next dispatch at the registry lookup, repaired by
    // an `acquire` (**A8**). Both are immediate and say what happened,
    // which is the trade: a loud failure at the start instead of a silent
    // one at the ceiling.
    //
    // # Why this does not reopen the durable-write question
    //
    // The `put` above still gates *both* in-memory effects — it is the
    // reordering of two writes on the same side of that gate, not a move
    // across it. The failure path is unchanged: answer `500`, leave
    // nothing behind.
    let live = LoginSession::new(
        record,
        state.base_url.clone(),
        Some(Arc::new(state.engine.clone())),
    );

    state
        .operator_sessions
        .lock()
        .await
        .insert(sid.clone(), live.clone());

    register_operator_session(
        &state.engine,
        Some(&state.operator_adapters),
        &sid,
        live.dispatch_target(),
    )
    .await;

    (StatusCode::OK, Json(OperatorsCreateResp { sid, token })).into_response()
}

// ─── WS /v1/operators/:sid/ws (Bearer required) ─────────────────────────────

/// Extracts `Authorization: Bearer <token>`; missing header, wrong scheme, or
/// an empty token all resolve to a `401` response. `Authorization` is
/// mandatory on the WS path — there is no empty-string default.
fn extract_bearer_token_required(headers: &HeaderMap) -> Result<String, Box<Response>> {
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    token.ok_or_else(|| {
        Box::new((StatusCode::UNAUTHORIZED, "missing or empty Bearer token").into_response())
    })
}

/// `GET /v1/operators/:sid/ws` (WS upgrade). Bearer mandatory. `404` on
/// unknown sid, `401` on token mismatch. On successful upgrade, registers (or
/// reuses, on reconnect) a `WSOperatorSession` under `sid` — same 3-registry
/// pattern as `handler::handle_socket`, plus role-alias registration for
/// every role minted alongside this sid.
pub async fn operators_ws_connect(
    State(state): State<AppState>,
    Path(sid): Path<String>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let bearer = match extract_bearer_token_required(&headers) {
        Ok(t) => t,
        Err(resp) => return *resp,
    };
    // A string that doesn't even parse as a SessionId can't be a known sid.
    let Ok(sid) = SessionId::parse(sid) else {
        return (StatusCode::NOT_FOUND, "unknown sid").into_response();
    };

    let live = {
        let map = state.operator_sessions.lock().await;
        map.get(&sid).cloned()
    };
    let live = match live {
        Some(e) => e,
        None => return (StatusCode::NOT_FOUND, "unknown sid").into_response(),
    };
    if !live.verify_bearer(&bearer) {
        return (StatusCode::UNAUTHORIZED, "token mismatch").into_response();
    }
    // The horizon is judged before the connect is honoured, not after: a session
    // that went 24h without contact was due for deletion at some point in
    // that window, and letting a late arrival cancel that would make the
    // horizon mean "24h unless somebody eventually turns up". The client
    // is not stranded — a join is unauthenticated (**D3**), so re-minting
    // is one call, and `acquire` (**A8**) takes the seat back.
    if reap_if_expired(&state, &sid, &live).await {
        return (StatusCode::NOT_FOUND, "unknown sid").into_response();
    }
    // Attaching is the strongest access there is; from here on the socket
    // itself holds the session alive (see `LoginSession::is_expired`).
    live.touch(&state.operator_session_store).await;

    let store = state.operator_session_store.clone();
    ws.on_upgrade(move |socket| handle_operator_socket(socket, live, store, KeepAlive::DEFAULT))
}

/// Binds a session's dispatch target into every registry an Operator
/// session must be reachable through: the engine's three (`senior_bridge` /
/// `spawn_hook` / `operator`) and the `OperatorAdapterRegistry` when one is
/// wired — all four under `sid`, and under nothing else.
///
/// # One key per session
///
/// Each session used to be filed under its sid *and* under every role
/// alias it claimed, because `Assignee.op` could be either. It cannot any
/// more, so the alias registrations are gone — and with them the way a
/// second session claiming an already-held alias silently took over the
/// first one's dispatches: both registries are last-write-wins and neither
/// reports a collision, so the `409` at mint was the only thing keeping
/// two sessions off one key. A sid cannot collide (it is minted fresh
/// every time, **O4**), so there is nothing left for a guard to guard.
///
/// # Why two operator-side registries
///
/// They answer different questions and are read at different times:
///
/// - **`engine.register_operator`** — the id space a launch is validated
///   against. `POST /v1/tasks` answers "is this `operator_sid` a live
///   session?" out of [`Engine::list_operator_ids`], which reads exactly
///   this map, so a session missing here cannot be pinned at all: the
///   launch is refused with `400 no such registered operator session`.
///   (The `ctx`-mediated reader this used to name, the Blueprint-global
///   `operator_delegate` layer, was removed — the entry is now read by the
///   launch gate and the seat resolver, not by a middleware.)
/// - **`adapters`** — the AgentSpec axis's delivery side. A dispatch
///   through a `kind = Operator` agent resolves its seat's *current holder*
///   off `Run.current` and turns that `OperatorId` into a destination
///   through this map (see [`AssigneeRouter`](super::router::AssigneeRouter)).
///   Nothing about the session
///   is baked into the compiled Blueprint any more, which is the whole
///   point of the seat/holder split.
///
/// Both are written here, under the same key, from this one call. That is
/// what keeps the launch-time guard (`engine.list_operator_ids()`, which
/// answers "is this `operator_sid` a live session") and the dispatch-time
/// lookup on the same id space: a sid that passes the guard is one a
/// router can deliver to.
///
/// The single spelling of that registration, and — since the mint path
/// took it over from the first-connect arm — the only one. Two callers
/// reach it, a mint ([`operators_create`]) and the boot-time restore of a
/// persisted record ([`restored_login_session`]), and they have to
/// leave identical registry state, or a session ends up resolvable on one
/// axis (`GET /v1/operators/:sid`) and missing on another (an
/// `operator_sid` pin, a seat that names it).
///
/// # Registering is the last step of publishing a session
///
/// A registration is what makes a session routable, and a routed send
/// finds its exits (connect, teardown) through `operator_sessions` — so
/// the mint puts the session in that map **before** calling this, and the
/// invariant to preserve is *registered implies present in
/// `operator_sessions`*. See [`operators_create`]'s ordering note for the
/// failure the other order produced.
///
/// The boot path is the one place the two land in the opposite order:
/// [`restored_login_session`] registers and hands the session back, and
/// the map it goes into is built from the returned values by
/// `build_router_full_with_operator_session_persistence`. Nothing is
/// serving during that gap — the router being assembled *is* the thing
/// that would route — so the window has no observer, which is why the
/// restore path is left as it is rather than given a map to write into
/// before one exists.
///
/// Nothing on the WS connect path calls this: a connect that races a
/// teardown must not be able to put a registration back.
async fn register_operator_session(
    engine: &Engine,
    adapters: Option<&Arc<OperatorAdapterRegistry>>,
    sid: &SessionId,
    dispatch_target: &Arc<WSOperatorSession>,
) {
    engine
        .register_senior_bridge(
            sid.clone(),
            dispatch_target.clone() as Arc<dyn SeniorBridge>,
        )
        .await;
    engine
        .register_spawn_hook(sid.clone(), dispatch_target.clone() as Arc<dyn SpawnHook>)
        .await;
    engine
        .register_operator(sid.clone(), dispatch_target.clone() as Arc<dyn Operator>)
        .await;
    if let Some(adapters) = adapters {
        adapters
            .register(
                sid.clone(),
                dispatch_target.clone() as Arc<dyn OperatorAdapter>,
            )
            .await;
    }
}

/// Builds the [`LoginSession`] for a persisted login record and registers
/// it — the boot-time half of session persistence, called from
/// [`crate::OperatorSessionPersistence::restore`].
///
/// Restoring the login map alone leaves a window between boot and the
/// owning client's WS reconnect in which the sid is known to
/// `GET /v1/operators/:sid` but not to the engine: a launch pinning it
/// (`POST /v1/tasks` `operator_sid`) is rejected with `400 no such
/// registered operator session`, and a Run whose `current` names it has no
/// operator to reach. Registering here closes that window, which is the
/// whole point of persisting `RunRecord.operator_sid` pins across a
/// restart.
///
/// The session is created **disconnected**
/// ([`WSOperatorSession::disconnected_with_base_url`]): it is registered,
/// not reachable. A reply-expecting send issued before the client attaches
/// a socket parks until that connect, or until a teardown publishes
/// [`ConnState::TornDown`]; only `after` (`send_oneway`) still fails with
/// `"ws operator disconnected"`. The client's connect is then a plain
/// `replace_tx` in [`handle_operator_socket`] — no second registration.
///
/// That park applies to the paths that address this session **by sid**
/// (`SeniorBridge` / `SpawnHook`), not to a dispatch routed through the
/// assignee: `AssigneeRouter::execute` pulls `T-ALIVE` first, and a
/// registered-but-unreconnected session answers `Disconnected`, so **A7**
/// releases the seat before the send would have parked. See
/// [`WSOperatorSession::disconnected_with_base_url`] for the full split.
///
/// [`operators_create`] does the same thing at mint time, so this is the
/// restore half of one shared shape rather than a special case.
pub(crate) async fn restored_login_session(
    engine: &Engine,
    adapters: Option<&Arc<OperatorAdapterRegistry>>,
    base_url: Option<Arc<str>>,
    record: OperatorSessionRecord,
) -> Arc<LoginSession> {
    let live = LoginSession::new(record, base_url, Some(Arc::new(engine.clone())));
    register_operator_session(engine, adapters, &live.record().sid, live.dispatch_target()).await;
    live
}

/// Reason handed to a torn-down session's parked replies *and* put on the
/// WS Close frame its client receives. One literal, both audiences.
const TEARDOWN_REASON: &str = "operator session torn down";

/// Close-frame text for the degenerate case where the reason could not be
/// read back off the session (it was dropped outright).
const SESSION_CLOSED_REASON: &str = "operator session closed";

/// How long [`handle_operator_socket`] gives its write task to drain — and,
/// on a teardown, to flush the Close frame — before aborting it. Only a
/// sink that refuses to make progress ever reaches this bound; the ordinary
/// path finishes as soon as the last sender drops.
const WRITE_TASK_DRAIN_GRACE: Duration = Duration::from_secs(5);

/// How the server keeps asking a still-attached socket whether anybody is
/// behind it.
///
/// # Why an attached socket needed a probe at all
///
/// [`LoginSession::is_expired`] short-circuits on "a socket is attached",
/// and that clause is only as good as `tx` being cleared when the peer
/// goes. `tx` is cleared in exactly one place — the read loop's exit — so
/// what clears it is the *stream ending*: a Close frame, a FIN, or a read
/// error.
///
/// A peer that vanishes without any of those (power cut, NAT table reap,
/// a cable) sends nothing and ends nothing. `ws_stream.next()` parks
/// forever; the write half is not sending, and even when it does, a TCP
/// write into a black hole buffers rather than errors. So `tx` stayed
/// `Some`, [`LoginSession::is_expired`] answered `false` on every read,
/// no reap path could touch the session — and the bearer had died with
/// the peer, so `DELETE /v1/operators/:sid` could not either. That is an
/// immortal row: precisely the thing the 24h horizon was added to
/// eliminate, reachable by the one failure mode a horizon cannot see.
///
/// A server-side Ping closes it because it makes the peer *produce*
/// something on a schedule the server chooses. Any inbound frame answers
/// the question — the Pong a conforming client sends back is the usual
/// one, but a client that happens to be talking anyway has already
/// answered it.
///
/// # It doubles as the connected session's access clock
///
/// The same tick is where a connected session is
/// [touched](LoginSession::touch). Without that, "a session with a socket
/// attached is never expired" was true only in this process: the durable
/// `last_access_secs` stopped moving at the connect, so a driver that
/// stayed attached and quiet for a day had a row the next boot's
/// `list` — which reads the record alone and knows nothing about sockets
/// — deleted out from under its live connection. **R6** (再起動は担当を
/// 落とさない) is the predicate that was against, and a Ping cadence is
/// exactly the periodic event the durable clock was missing.
#[derive(Debug, Clone, Copy)]
pub(crate) struct KeepAlive {
    /// Gap between two server-sent Pings, and the resolution at which
    /// [`Self::silence_before_dead`] is checked.
    ping_every: Duration,
    /// How long a socket may go without a single inbound frame before the
    /// read loop treats the peer as gone and exits, which clears `tx` and
    /// hands the session back to the ordinary expiry.
    silence_before_dead: Duration,
}

impl KeepAlive {
    /// What every route-driven socket uses.
    ///
    /// 30s between Pings and five of them missed before a socket is
    /// declared dead. The tolerance is deliberately several intervals
    /// wide: the cost of declaring a live driver dead is that its seats
    /// cascade `Vacant` under it, while the cost of waiting is a couple of
    /// minutes of an already-broken socket looking attached. Those are not
    /// symmetric, so the threshold is set by the expensive side.
    ///
    /// It is stated in intervals rather than as a bare duration because
    /// that is the quantity that means anything here — "150 seconds" is a
    /// number, "five missed Pings" is a claim about how much evidence is
    /// required.
    pub(crate) const DEFAULT: Self = Self {
        ping_every: Duration::from_secs(OPERATOR_WS_PING_INTERVAL_SECS),
        silence_before_dead: Duration::from_secs(
            OPERATOR_WS_PING_INTERVAL_SECS * OPERATOR_WS_MISSED_PINGS_BEFORE_DEAD,
        ),
    };
}

/// Gap between two server-sent WS Pings on an attached Operator socket.
///
/// Sized against what it feeds rather than picked for its own sake: it is
/// also the cadence at which a connected session's durable access clock is
/// advanced, and [`TOUCH_PERSIST_INTERVAL_SECS`] coalesces those writes to
/// one a minute regardless — so anything at or below that interval costs
/// the store nothing extra.
const OPERATOR_WS_PING_INTERVAL_SECS: u64 = 30;

/// How many consecutive Pings may go unanswered — with no other inbound
/// frame either — before the peer is treated as gone. See
/// [`KeepAlive::DEFAULT`] for why the tolerance is this wide.
const OPERATOR_WS_MISSED_PINGS_BEFORE_DEAD: u64 = 5;

/// Resolves once [`WSOperatorSession::close`] has been called on this
/// session, yielding the reason to hand the client.
///
/// `wait_for` inspects the *current* value before parking, so a close
/// latched before this socket subscribed still resolves immediately — which
/// is what a socket that upgrades while its session is being torn down
/// needs, since no second signal is ever sent.
async fn close_requested(signal: &mut watch::Receiver<Option<Arc<str>>>) -> Arc<str> {
    match signal.wait_for(|reason| reason.is_some()).await {
        // The predicate above already established `Some`.
        Ok(reason) => (*reason)
            .clone()
            .unwrap_or_else(|| Arc::from(SESSION_CLOSED_REASON)),
        // The sender is gone, i.e. the session itself was dropped: there is
        // nothing left to pump into, so end the socket as well.
        Err(_) => Arc::from(SESSION_CLOSED_REASON),
    }
}

/// Bidirectional pump for a single WS connection, bound to a
/// [`LoginSession`]. Owns the full wire protocol pump (write task /
/// read task / `ClientMsg` dispatch / disconnect) for this session.
///
/// Both halves also watch the session's close signal
/// ([`WSOperatorSession::close`]): the write half answers it with a WS
/// Close frame, the read half stops waiting on a client that may never
/// reply to one. Without that the local `tx` below outlives the session's
/// own sender, keeping the channel — and the socket — alive after teardown.
///
/// # The third thing both halves do
///
/// The write half sends a Ping every [`KeepAlive::ping_every`]; the read
/// half holds the deadline, ends the loop when nothing has come back for
/// [`KeepAlive::silence_before_dead`], and advances the session's access
/// clock on every tick that finds the peer answering. See [`KeepAlive`]
/// for the failure that needs — a peer that vanishes without closing
/// anything otherwise leaves `tx` set forever, and a session whose `tx` is
/// set is one nothing may expire.
///
/// A liveness timeout exits by exactly the same path a Close frame does:
/// the loop breaks, `clear_tx_if` runs, the write task drains. There is no
/// second teardown to keep in step, and the session is left in the state
/// the ordinary expiry already knows how to judge — disconnected, with
/// whatever last-access clock it had.
async fn handle_operator_socket(
    socket: WebSocket,
    live: Arc<LoginSession>,
    store: Arc<dyn mlua_swarm::store::operator_session::OperatorSessionStore>,
    keepalive: KeepAlive,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<ServerMsg>();

    // Attach to the dispatch target this session has carried since it was
    // built (see [`LoginSession`]'s type doc). First connect and reconnect
    // are therefore the same operation: swap `tx` in, touch no registry.
    //
    // A socket whose upgrade completed just before its session was torn
    // down arrives here too, holding a value that has already left
    // `operator_sessions`. It finds the closed session, swaps its sender
    // in, and is answered below with the Close frame teardown latched —
    // instead of minting a replacement session and registering it behind
    // teardown's back.
    let session = live.dispatch_target();
    session.replace_tx(tx.clone()).await;

    let (mut ws_sink, mut ws_stream) = socket.split();
    let mut write_close_signal = session.close_signal();
    let mut read_close_signal = session.close_signal();

    // write task: mpsc → WebSocket, until the channel ends or the session
    // is closed out from under it (which the client is told about).
    let ping_every = keepalive.ping_every;
    let mut write_task = tokio::spawn(async move {
        let mut ping = tokio::time::interval(ping_every);
        // A tokio interval's first tick is immediate, and a Ping the
        // instant the socket attaches proves nothing the upgrade has not
        // already proved — so it is consumed rather than sent.
        ping.tick().await;
        // A write half that was parked on a slow sink must not owe a burst
        // of Pings when it comes back; the peer is asked again on the next
        // whole interval instead.
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                // Invariant: whenever both branches are ready, close wins.
                // The `recv()` arm's channel-end exit closes the socket
                // with no reason, silently degrading the frame below — so
                // `biased;` is load-bearing here, not an optimisation.
                //
                // The Ping arm sits above `recv()` for the same kind of
                // reason: with `biased;` a permanently-ready `recv()` would
                // starve the arms below it, and the one thing that must not
                // stop while frames are flowing is the probe that decides
                // whether anyone is receiving them.
                biased;

                reason = close_requested(&mut write_close_signal) => {
                    // A standard Close frame, not a new protocol verb: the
                    // client already handles this, and the wire shape stays
                    // exactly as it was.
                    let _ = ws_sink
                        .send(Message::Close(Some(CloseFrame {
                            code: close_code::NORMAL,
                            reason: reason.to_string().into(),
                        })))
                        .await;
                    break;
                }
                _ = ping.tick() => {
                    // An empty payload: the frame is the question, and
                    // nothing about *which* Ping this is would be read.
                    // A send error here is the ordinary "the socket is
                    // gone" exit, not a keepalive-specific one.
                    if ws_sink.send(Message::Ping(Vec::new())).await.is_err() {
                        break;
                    }
                }
                msg = rx.recv() => {
                    let Some(msg) = msg else { break };
                    let txt = match serde_json::to_string(&msg) {
                        Ok(s) => s,
                        Err(_) => continue,
                    };
                    if ws_sink.send(Message::Text(txt)).await.is_err() {
                        break;
                    }
                }
            }
        }
        let _ = ws_sink.close().await;
    });

    // read task: WS message → ClientMsg parse → session.resolve_pending
    let session_for_read = session.clone();
    // When the newest inbound frame arrived. A monotonic `Instant` rather
    // than the wall clock this file measures **everything else** in: the
    // 24h horizon is a statement about calendar time and has to survive a
    // process restart, while this is a stopwatch over one socket's life,
    // and a wall clock that stepped mid-window would either fake silence
    // or hide it. Confined to this loop — the write half only sends the
    // Pings that provoke the frames — so it needs no synchronization.
    let mut last_inbound = tokio::time::Instant::now();
    let mut liveness = tokio::time::interval(keepalive.ping_every);
    // Same two adjustments as the write half's ticker, for the same
    // reasons: the immediate first tick would judge a socket that has had
    // no time to answer anything, and a burst of catch-up ticks after a
    // long `resolve_pending` would spend the tolerance without any of the
    // waiting the tolerance is made of.
    liveness.tick().await;
    liveness.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let item = tokio::select! {
            item = ws_stream.next() => match item {
                Some(item) => item,
                None => break,
            },
            // The write half is sending the Close frame; a client that
            // never answers it must not keep this half parked forever.
            _ = close_requested(&mut read_close_signal) => break,
            _ = liveness.tick() => {
                let silent_for = last_inbound.elapsed();
                if silent_for >= keepalive.silence_before_dead {
                    tracing::info!(
                        sid = %live.record().sid,
                        silent_for_ms = silent_for.as_millis() as u64,
                        window_ms = keepalive.silence_before_dead.as_millis() as u64,
                        "operator socket answered no Ping within the keepalive window; \
                         treating the peer as gone and detaching, which hands the session \
                         back to the 24h expiry"
                    );
                    break;
                }
                // The peer is answering, so the driver behind this session
                // is present — and this is the only periodic moment that
                // knows it. Writing it through is what makes an attached
                // socket hold the session alive across a restart and not
                // only inside this process (see `KeepAlive`).
                live.touch(&store).await;
                continue;
            }
        };
        // Any inbound frame is proof the peer is still there, whatever it
        // carries: a Pong answering our Ping is the expected one, but a
        // client that is answering an `Ask` has said the same thing. The
        // deadline above is therefore about silence, not about Pongs.
        if item.is_ok() {
            last_inbound = tokio::time::Instant::now();
        }
        match item {
            Ok(Message::Text(t)) => {
                let parsed: ClientMsg = match serde_json::from_str(&t) {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                match parsed {
                    ClientMsg::Answer { req_id, value } => {
                        session_for_read
                            .resolve_pending(&req_id, PendingReply::Answer(value))
                            .await;
                    }
                    ClientMsg::HookAck { req_id, ok, reason } => {
                        session_for_read
                            .resolve_pending(&req_id, PendingReply::HookAck { ok, reason })
                            .await;
                    }
                    ClientMsg::SpawnAck {
                        req_id,
                        value,
                        ok,
                        error,
                        stats,
                    } => {
                        session_for_read
                            .resolve_pending(
                                &req_id,
                                PendingReply::SpawnAck {
                                    value,
                                    ok,
                                    error,
                                    stats,
                                },
                            )
                            .await;
                    }
                    ClientMsg::SpawnHalt {
                        req_id,
                        value,
                        reason,
                    } => {
                        session_for_read
                            .resolve_pending(&req_id, PendingReply::SpawnHalt { value, reason })
                            .await;
                    }
                }
            }
            // Nothing more to do with either: axum answers an inbound
            // Ping with a Pong itself, and the liveness both of them carry
            // was recorded above, before the match.
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
            Ok(Message::Close(_)) | Err(_) => break,
            _ => {}
        }
    }

    // Clear only this socket's sender. A reconnect may already have installed
    // a replacement while this older socket was unwinding.
    session.clear_tx_if(&tx).await;
    // Dropping the last sender is what lets the write task's `rx.recv()`
    // end on its own — on a teardown exit, only after it has flushed the
    // Close frame. Aborting straight away would race that frame, which is
    // the very thing this path exists to deliver; the abort stays as the
    // backstop for a sink that refuses to drain.
    drop(tx);
    if tokio::time::timeout(WRITE_TASK_DRAIN_GRACE, &mut write_task)
        .await
        .is_err()
    {
        write_task.abort();
    }
}

// ─── DELETE /v1/operators/:sid (Bearer required) ────────────────────────────

/// Teardown for `DELETE /v1/operators/:sid` (`operators_delete`): drops the
/// persisted row, the 3 engine registries + the adapter-registry binding +
/// the `operator_sessions` entry, closes the session's socket, and finally
/// releases every Run seat the operator still held (**O8** — see
/// [`cascade_vacate_seats`]). Idempotent w.r.t. a concurrent delete —
/// every `remove` / `unregister` is a no-op when the entry is already
/// gone, the socket close is latched (see [`WSOperatorSession::close`]),
/// and a repeated cascade finds the seats already vacant.
///
/// # The asymmetry that used to be deferred is gone
///
/// This teardown carried a note saying its two unregister paths disagreed:
/// the role keys came out of the registries unconditionally, while the
/// `roles_to_sid` release was guarded by ownership, so a role re-minted to
/// another sid mid-teardown lost its registry binding to a teardown that
/// no longer owned the name. Making the two predicates agree was deferred
/// to "the RoleGrant lifecycle work".
///
/// That work is not needed, because the discrepancy was between two
/// spellings of one thing that no longer exists: with no role keys and no
/// `roles_to_sid`, a teardown removes exactly the sid it was called with,
/// under one predicate, in one place. There is no second name to be
/// re-granted underneath it and therefore no ownership question to get
/// wrong. The deferral is closed by removal rather than by repair.
///
/// # The persisted row goes first
///
/// `Err` means **nothing was torn down**: the store still holds the row and
/// every in-memory map still holds the session, so the caller can answer
/// `5xx` and the client can retry against a state that never diverged.
///
/// Dropping the row afterwards instead — the original order — could not
/// offer that. A failure there was logged and swallowed because the
/// in-memory teardown had already happened and could not be rolled back,
/// which left the row behind for the next boot's restore to resurrect: the
/// session came back after being deliberately released. Doing the fallible
/// step first makes that state unreachable rather than merely reported.
///
/// `NotFound` stays a success — the concurrent-delete case, same contract
/// as the map removals below.
async fn teardown_operator_session(
    state: &AppState,
    sid: &SessionId,
    live: &Arc<LoginSession>,
) -> Result<(), OperatorSessionStoreError> {
    match state.operator_session_store.delete(sid).await {
        Ok(()) | Err(OperatorSessionStoreError::NotFound(_)) => {}
        Err(error) => {
            tracing::error!(
                %sid, %error,
                "operator session teardown: persisted row delete failed; \
                 teardown abandoned, the session stays live"
            );
            return Err(error);
        }
    }

    state.engine.unregister_senior_bridge(sid.as_str()).await;
    state.engine.unregister_spawn_hook(sid.as_str()).await;
    state.engine.unregister_operator(sid.as_str()).await;
    state.operator_adapters.unregister(sid.as_str()).await;

    // Closed in place, never detached. A socket that upgraded before this
    // teardown and has not reached `handle_operator_socket` yet still holds
    // this `LoginSession`, and must find the closed session rather than an
    // empty slot — an empty slot is precisely the state the old
    // first-connect arm answered by registering a fresh session nothing
    // could unregister. It finds the closed one instead and is shut down by
    // the latch below.
    //
    // Idempotent w.r.t. a repeat teardown on the same session:
    // `fail_pending` drains an already-empty map, `close` re-latches the
    // same reason, and `clear_tx` is a plain `None` write.
    let session = live.dispatch_target();
    // B-2: fail every parked spawn/ask/hook_before on this session right
    // away. Teardown removes the session from `operator_sessions` below (no
    // reconnect can find it again), so unlike a plain WS disconnect there
    // is no reconnect/resend contract to preserve — an in-flight spawn
    // parked in `send_and_await` would otherwise orphan until the run's
    // sync timeout (up to 300s) fires.
    //
    // This is the call the teardown work proposed to drop, leaving the
    // requests for the next Assignee to answer by `req_id` (**W2**). It
    // stays: by this line the adapter is already unregistered and the sid
    // is about to leave `operator_sessions`, so there is no longer any way
    // to *read* those requests, and a send still parked before its first
    // write has no exit left but the `TornDown` this publishes. The full
    // argument, including what would have to change to remove it, is on
    // `WSOperatorSession::fail_pending`.
    session.fail_pending(TEARDOWN_REASON).await;
    // Same reasoning one step further out: with no reconnect possible, the
    // socket itself has no future either. Clearing `tx` alone left the
    // client parked on a live WebSocket that nothing would ever be routed
    // to again — `close` is what actually ends it (a WS Close frame, sent
    // by the pump; see `handle_operator_socket`).
    session.close(TEARDOWN_REASON);
    session.clear_tx().await;

    state.operator_sessions.lock().await.remove(sid);

    // O8, last: by here the operator is unreachable under the one name it
    // answered to, so no dispatch can re-establish what this releases.
    cascade_vacate_seats(state, sid).await;

    Ok(())
}

/// The Run statuses **O8**'s cascade walks — every status except the two
/// that mean the Run reached its own end.
///
/// The model says `∀run`, and the scan is narrower than that on purpose;
/// this is where the reason lives.
///
/// **Why not every Run.** There is no way to ask for "Runs whose `current`
/// holds `op`": `RunListFilter` filters on `task_id` / `status` only, the
/// sole index on `runs` is `ix_runs_task_id`, and `current_json` is a JSON
/// text column no `WHERE` can reach into. So the cascade is a scan, and a
/// scan is bounded by whatever it enumerates. `runs` is append-only —
/// pruning is a manual `DELETE /v1/runs/:id` — so "every Run" grows without
/// limit. Filtering by status pushes the predicate into SQL, so each pass
/// materialises only rows of that status rather than every `RunRecord`
/// (`step_entries` and launch snapshot included) ever written.
///
/// **What that actually bounds it by — non-terminal Runs, which also
/// accumulate.** Only `Running` tracks live work. A `Cancelled` Run stays
/// `Cancelled`; an `Interrupted` one leaves that status only if a human
/// calls `POST /v1/runs/:id/resume`; a `Pending` one that never kicked
/// stays `Pending`. So three of these four statuses grow with history too,
/// just more slowly than the whole table. And the filter does not make the
/// pass cheap: there is no index on `status`, so each of the four `list()`
/// calls is a full table scan, `limit` is `None`, and every surviving row
/// is deserialized into a complete `RunRecord` — step trace included —
/// only for its `current` map to be read. The cost of a leave (every
/// `mse-mcp` shutdown) is therefore linear in accumulated non-terminal
/// Runs, not in live ones.
/// Measured against a workstation database of hundreds of Runs that is
/// nothing; it is written down because the next reader would otherwise
/// extend "bounded by live work" to a bigger scan. The fix, when the store
/// surface is next opened, is a narrow query returning `id` +
/// `current_json` (or a holder filter on `RunListFilter`) rather than an
/// index alone — the per-row deserialization is the larger term.
///
/// **Why these four.** `Pending` and `Running` are plainly live.
/// `Interrupted` is resumable in place (`POST /v1/runs/:id/resume`), so its
/// seats will be dispatched through again. `Cancelled` is a marker, not a
/// stop — in-flight abort is still a carry, so a cancelled Run's driver may
/// well be dispatching right now.
///
/// **Why `Done` / `Failed` are left alone.** Their `current` is no longer a
/// live pointer but the record of who held the seat when the Run ended,
/// which is worth more to a reader than an empty map — and vacating would
/// destroy it while bumping `G` on a finished row.
///
/// The one path that revives such a Run
/// (`POST /v1/runs/:id/rerun-from`) then dispatches against the preserved
/// holder, and that holder is a sid, which is never re-minted (**O4**) —
/// so it resolves to nothing and the dispatch fails loudly at the registry
/// lookup, repaired by an acquire (**A8**).
///
/// It used to be possible for that dispatch to *succeed* instead: a holder
/// could be a role alias, teardown re-opened the alias for a future mint,
/// and the next session to claim it registered under the same key — so the
/// revived Run silently re-routed to whoever held the name now. With
/// aliases gone there is one outcome, and it is the loud one. **O8** stays
/// nice-to-have rather than load-bearing either way, because the repair is
/// the same acquire.
const CASCADE_STATUSES: [RunStatus; 4] = [
    RunStatus::Pending,
    RunStatus::Running,
    RunStatus::Interrupted,
    RunStatus::Cancelled,
];

/// **O8**: `delete(op) ⟹ ∀run. current = Assigned(a) ∧ a.op = op ⟹
/// current := Vacant`.
///
/// # One name
///
/// A session answers to its sid and to nothing else, so `current[slot].op`
/// either is that sid or is not this operator's. The cascade used to match
/// against a *set* of names — the sid plus every role the session held —
/// because a launch could seat a role name as readily as a sid; releasing
/// only the sid then left the alias-held seats pointing at an operator
/// that had just been deleted, which is exactly the lie **O8** exists to
/// prevent. With no aliases the set is a single value and the comparison
/// is an equality.
///
/// # Best effort, and why that is right here
///
/// Called last, after the persisted row and every in-memory registration
/// are gone, and it never fails the teardown: at that point the session is
/// already released and there is nothing to roll back to, so refusing
/// would report a failure the caller cannot act on while leaving the
/// session torn down anyway. A seat this could not release stays held by a
/// name nothing resolves — loud at the next dispatch, and overwritten by
/// the next acquire (**A8**). Every failure is logged with the Run and slot
/// it could not release.
///
/// # Each release names the holder it read
///
/// The scan is a snapshot: a `list()` per status, then one release per
/// matching seat, with every earlier release an `.await` an `acquire` can
/// land inside. So each release carries the generation the scan observed
/// and the store applies it only while the seat still holds that exact
/// holder ([`VacateOutcome`]). A seat that changed hands in between is
/// left alone and logged — the holder **O8** judged is gone, and the one
/// there now is by definition not the deleted operator's, so releasing it
/// would destroy a live assignment on the strength of a stale reading.
///
/// # Each release is recorded on the Run's trace
///
/// A seat emptied here was not emptied by its holder, so the next thing
/// that happens to that Run is a dispatch failing as `Vacant` some time
/// later, far from the cause. **W4** puts the cause on the same rail as
/// the step events: one `core.assignee_released` row per seat actually
/// released, carrying the holder and `reason: o8_operator_deleted` (see
/// [`crate::assignee_trace`]). Only the `Released` arm writes one — a
/// stale release changed nothing and must not claim otherwise.
///
/// # No timer
///
/// This fires at delete time and nowhere else. Nothing scans for
/// deleted-operator holders in the background — the sibling judgment
/// (**A7**) is likewise made where the seat is read. A periodic sweeper
/// would be a second place where seats change hands, running against Runs
/// nobody is dispatching to.
///
/// That the server has a periodic-job runner ([`crate::periodic`]) does not
/// reopen this: a job there may only apply a predicate some non-timer path
/// already applies, and "this seat's holder has gone" has no such
/// predicate — it is a judgment made *about a dispatch*, at the moment one
/// is made. The 24h session horizon is registered there precisely because
/// it is the opposite case.
async fn cascade_vacate_seats(state: &AppState, sid: &SessionId) {
    let mut released = 0usize;
    for status in CASCADE_STATUSES {
        let runs = match state
            .run_store
            .list(&RunListFilter {
                status: Some(status),
                ..Default::default()
            })
            .await
        {
            Ok(runs) => runs,
            Err(error) => {
                tracing::warn!(
                    %sid, ?status, %error,
                    "O8 cascade: runs of this status could not be listed; seats held by the \
                     deleted operator may remain in their current"
                );
                continue;
            }
        };
        for run in runs {
            // Collected first: `vacate_assignee` rewrites the very map
            // being read, and the borrow ends here either way. The
            // generation travels with each entry because the release
            // below is conditional on it — this snapshot is taken before
            // the `list()` round trip has even finished being consumed,
            // and every prior release is an `.await` an acquire can land
            // inside.
            let held: Vec<(String, Assignee)> = run
                .current
                .iter()
                .filter(|(_, assignee)| assignee.op == sid.as_str())
                .map(|(slot, assignee)| (slot.clone(), assignee.clone()))
                .collect();
            for (slot, holder) in held {
                let op = &holder.op;
                let observed_gen = holder.gen;
                match state
                    .run_store
                    .vacate_assignee(&run.id, &slot, observed_gen)
                    .await
                {
                    Ok(VacateOutcome::Released { .. }) => {
                        released += 1;
                        tracing::info!(
                            run_id = %run.id, %slot, %op, %sid,
                            "O8 cascade: seat released because its operator was deleted"
                        );
                        // W4: a seat emptied by the system goes on the
                        // Run's own trace, next to its step events. Only
                        // on this arm — the `Stale` arm below released
                        // nothing, and a row there would report a
                        // handover that did not happen.
                        append_released(
                            &TraceHandle::new(run.id.clone(), state.run_trace_store.clone()),
                            &slot,
                            &holder,
                            ReleaseReason::O8OperatorDeleted,
                        )
                        .await;
                    }
                    // The seat moved on after this scan read it, so the
                    // holder the cascade judged is gone and the one there
                    // now was never this operator's. O8 has no claim on
                    // it: releasing would destroy a live assignment on the
                    // strength of a stale reading.
                    Ok(VacateOutcome::Stale { current }) => tracing::info!(
                        run_id = %run.id, %slot, %op, %sid,
                        observed_gen,
                        current_op = current.as_ref().map(|c| c.op.as_str()).unwrap_or("<vacant>"),
                        "O8 cascade: seat changed hands after the scan read it; left alone"
                    ),
                    Err(error) => tracing::warn!(
                        run_id = %run.id, %slot, %op, %sid, %error,
                        "O8 cascade: seat could not be released; its current still names a \
                         deleted operator"
                    ),
                }
            }
        }
    }
    if released > 0 {
        tracing::info!(%sid, released, "O8 cascade: seats released on operator delete");
    }
}

/// `DELETE /v1/operators/:sid`. Bearer mandatory. `404` on unknown sid,
/// `401` on token mismatch. Drops the persisted row, the 3 engine
/// registries + the adapter-registry binding + the `operator_sessions`
/// entry, closes the session's socket, and vacates every Run seat this
/// operator still held (**O8**).
///
/// **The only leave route.** `DELETE /v1/operators/by-role/:role` used to
/// sit alongside it as the recovery door for a stale session whose driver
/// crashed without keeping its sid — a role name was the one handle such a
/// caller still had. A join claims no name now, so that door has no
/// keyhole; what a recovery caller reads instead is `GET /v1/operators`,
/// which lists every live session's sid next to the 記名 that says what it
/// was doing (**D1** / **D2**), and then deletes the one it recognises by
/// sid. That names the session it is about to kill rather than a name
/// several sessions could answer to, and it is the reason the by-role
/// route's whole retinue (`?force=true`, the in-flight `409` guard, the
/// torn-mapping arm) went with it: each existed to make releasing
/// *somebody else's* session by a shared name survivable.
///
/// # What that recovery cannot do
///
/// It cannot release a session whose driver is gone. This route checks
/// the Bearer against *that session's own* digest, and the client keeps
/// the token in process — a crash loses it. So the sid a recovery caller
/// reads off `GET /v1/operators` is one it can identify and cannot
/// delete: this route answers `401` for every caller except the corpse.
///
/// That is survivable for **assignment** — a replacement driver joins
/// freely, pins its own launches, and `acquire` never refuses, so the
/// stale session holds no seat anyone needs. It used not to be survivable
/// for **storage**: the persisted row has no other deleter, and boot
/// restore re-materialized every row with no age filter, so the set grew
/// once per crashed driver and survived every restart.
///
/// That half is closed. §4.1's other exit from `Registered`
/// (`最終アクセスから 24h ──▶ ╳ 削除`, unnumbered) is enforced at every
/// read of a session — this crate's [`reap_if_expired`] and the store's
/// boot-time `list` — and, for a session no read reaches, by the
/// `operator-session-expiry` job ([`sweep_expired_operator_sessions`]). So
/// a stale row is released a day after its driver stopped, by the same
/// teardown this route performs, cascade included, whether or not anybody
/// is looking.
///
/// `500` when the persisted row cannot be dropped: the session is then
/// still live and fully intact (see [`teardown_operator_session`]), and the
/// call is safe to retry.
pub async fn operators_delete(
    State(state): State<AppState>,
    Path(sid): Path<String>,
    headers: HeaderMap,
) -> Response {
    let bearer = match extract_bearer_token_required(&headers) {
        Ok(t) => t,
        Err(resp) => return *resp,
    };
    let Ok(sid) = SessionId::parse(sid) else {
        return (StatusCode::NOT_FOUND, "unknown sid").into_response();
    };

    let live = {
        let map = state.operator_sessions.lock().await;
        map.get(&sid).cloned()
    };
    let live = match live {
        Some(e) => e,
        None => return (StatusCode::NOT_FOUND, "unknown sid").into_response(),
    };
    if !live.verify_bearer(&bearer) {
        return (StatusCode::UNAUTHORIZED, "token mismatch").into_response();
    }

    if let Err(error) = teardown_operator_session(&state, &sid, &live).await {
        return teardown_failed_response(&sid, &error);
    }

    StatusCode::NO_CONTENT.into_response()
}

/// `500` body shared by both delete routes when
/// [`teardown_operator_session`] refuses. Says plainly that nothing was
/// torn down, because that is what makes the call retryable.
fn teardown_failed_response(sid: &SessionId, error: &OperatorSessionStoreError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "error": format!("session teardown failed: {error}"),
            "sid": sid,
            "hint": "the persisted session row could not be dropped, so the session was \
                     left intact rather than half torn down; retry once the store is healthy",
        })),
    )
        .into_response()
}

// ─── GET /v1/operators — the 記名 list ──────────────────────────────────────

/// One entry in the `GET /v1/operators` list response — a live session's
/// identity plus its 記名 (model §4.2).
///
/// Still carries no token and no capability manifest: the former is the
/// bearer secret, and the latter is what `GET /v1/operators/:sid` is for.
#[derive(Debug, Serialize)]
pub struct OperatorsListEntry {
    /// Session id (`S-<hex>`) — safe to expose; token is the sole bearer
    /// secret, and this is the whole of what identifies a session now that
    /// a join claims no name.
    pub sid: SessionId,
    /// Unix epoch seconds when the session minted (from
    /// [`OperatorSessionRecord::joined_at_secs`]).
    pub joined_at_secs: u64,
    /// Whether a WS is currently attached to this session (matches the
    /// `connected` field on `GET /v1/operators/:sid`).
    pub connected: bool,
    /// The 記名's confirmed part (**D1**) — what this session wrote about
    /// itself at join. `null` when it joined without one, and never
    /// omitted: "nobody wrote a description" is the fact a reader needs
    /// most, because such a session is the one it cannot tell apart.
    pub desc: Option<String>,
    /// The 記名's observed part (**D2**) — every seat this session has been
    /// assigned, oldest first, up to
    /// [`OBSERVED_CAP`](mlua_swarm::store::operator_session::OBSERVED_CAP).
    pub observed: Vec<ObservedAssignment>,
    /// How many `Assign`s produced [`Self::observed`], including entries
    /// the ring has aged out and re-assignments folded into an existing
    /// one. Greater than `observed.len()` means this is a window.
    pub observed_total: u64,
    /// Unix epoch seconds of this session's newest observed activity, or
    /// its join time when it has never been assigned anything. **D5**'s
    /// default sort key, reported so a reader can see the order it was
    /// sorted by.
    pub last_activity_secs: u64,
}

/// Response body for `GET /v1/operators`.
#[derive(Debug, Serialize)]
pub struct OperatorsListResp {
    /// One entry per live session, newest activity first (**D5**).
    pub operators: Vec<OperatorsListEntry>,
    /// How many live sessions there are in total, before
    /// [`Self::limit`] cut the list. `total > operators.len()` means the
    /// page is short of the whole.
    pub total: usize,
    /// The page size actually applied — **D5** requires the list to have
    /// one, and reporting it saves the caller guessing whether it hit the
    /// cap or the end.
    pub limit: usize,
}

/// Query string of `GET /v1/operators`.
#[derive(Debug, Deserialize, Default)]
pub struct OperatorsListQuery {
    /// Page size. Absent = [`OPERATORS_LIST_DEFAULT_LIMIT`]; clamped to
    /// [`OPERATORS_LIST_MAX_LIMIT`].
    #[serde(default)]
    pub limit: Option<usize>,
}

/// **D5**'s count limit, applied when the caller names none.
pub const OPERATORS_LIST_DEFAULT_LIMIT: usize = 50;

/// The ceiling a caller-supplied `?limit=` is clamped to. **D5** asks the
/// list to *have* a limit; an unbounded opt-out would give that back.
pub const OPERATORS_LIST_MAX_LIMIT: usize = 200;

/// Accept any live Operator session's bearer.
///
/// **D3** ("一覧の取得は bearer 必須") gates the 記名 list, and **W5** says
/// the reader is an Assignee — which is any logged-in Operator, not
/// specifically the session being read. There is no single sid to check
/// against on a collection route, so the presented bearer is matched
/// against every live session and one match is enough. Each comparison is
/// [`OperatorSessionRecord::verify_bearer`], i.e. constant-time over the
/// digests.
///
/// This is what makes the list a *handover* device: an incoming Assignee
/// joins (unguarded, **D3**'s own carve-out), and its fresh bearer is
/// immediately good enough to read who else is here.
///
/// # The horizon is judged before the presenter is touched
///
/// This route is the third reader of a session, and the two others
/// ([`operators_ws_connect`], [`operators_info`]) both judge the 24h
/// horizon *before* recording the access. This one used to do the
/// opposite — find the session by bearer and touch it, with no expiry
/// judgment at all — and the ordering was load-bearing rather than
/// incidental: the reap loop in [`operators_list`] then ran against a
/// clock that had been reset one statement earlier, so it always found
/// the presenter fresh.
///
/// The observable failure was that a driver's own session lived or died
/// by which route it happened to call first. Given a session 25h past its
/// last access with its socket down, `GET /v1/operators/:sid` tore it
/// down and cascaded its seats `Vacant`, while `GET /v1/operators` with
/// the same bearer revived it — and this guard also covers the handover
/// routes, including the `acquire` a recovering driver is told to call,
/// so the reviving path was the one a real recovery takes.
///
/// The rule is the one [`operators_ws_connect`] states: a session that
/// went 24h without contact was due for deletion at some point in that
/// window, and letting a late arrival cancel that would make the horizon
/// mean "24h unless somebody eventually turns up". So an expired
/// presenter is reaped and answered `401` — its session is gone, and a
/// join is one unauthenticated call away.
///
/// Reaping the presenter out of the list it is asking for reads like
/// self-harm and is not: the caller is past the horizon, so the row it
/// would have been shown as is one no reader may see. What the answer
/// owes it is knowing *which* `401` it got, which is why the two carry
/// different `error` / `hint` pairs — "your token is not a session's" and
/// "your session expired, re-join" call for different next moves.
pub(crate) async fn authorize_any_operator(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(), Response> {
    let bearer = extract_bearer_token_required(headers).map_err(|resp| *resp)?;
    let sessions: Vec<Arc<LoginSession>> = {
        let map = state.operator_sessions.lock().await;
        map.values().cloned().collect()
    };
    if let Some(live) = sessions.iter().find(|live| live.verify_bearer(&bearer)) {
        // Judged first, touched second — see the "horizon" section above.
        if reap_if_expired(state, &live.record().sid, live).await {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(json!({
                    "error": "the operator session this token belongs to expired and was released",
                    "hint": "the session went 24h without contact, so it was torn down (§4.1's \
                             state diagram) and every seat it held went Vacant (O8); the token \
                             is not malformed and re-presenting it will not help. Join again \
                             with POST /v1/operators (no Bearer needed), then take back the \
                             seats you were driving with POST /v1/runs/:id/acquire (A8)",
                })),
            )
                .into_response());
        }
        // The presenter is demonstrably alive, so this is an access. Only
        // the matching session is touched: reading the 記名 list must not
        // keep the stale rows it is being read to find alive.
        live.touch(&state.operator_session_store).await;
        return Ok(());
    }
    Err((
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "error": "token matches no live operator session",
            "hint": "join with POST /v1/operators (no Bearer needed) and present the token it \
                     mints; any live session's token opens this list (model D3/W5). A token \
                     that used to work may have belonged to a session released at the 24h \
                     horizon — re-join rather than retrying it",
        })),
    )
        .into_response())
}

/// `GET /v1/operators`. The 記名 list (model §4.2) — every live session's
/// identity, its join-time description (**D1**) and the seats it has been
/// assigned (**D2**).
///
/// # Breaking change: Bearer is now mandatory
///
/// This route was unauthenticated (GH #81 Layer 2 (b), same trust tier as
/// `GET /v1/status`). **D3** makes it Bearer-gated, and it now carries the
/// 記名, which is a description of what someone is working on rather than
/// a bare sid. Any live session's token is accepted — see
/// [`authorize_any_operator`]. Callers that used to read this anonymously
/// (`mse mcp`'s `mse_operator_list`, recovery scripts) must present one.
///
/// # Order and size (**D5**)
///
/// Newest activity first — the newest [`ObservedAssignment::at_secs`], or
/// the join time for a session that has never held a seat — with the sid
/// as a tie-break so equal-second sessions still order deterministically.
/// The page is capped at [`OPERATORS_LIST_DEFAULT_LIMIT`], overridable up
/// to [`OPERATORS_LIST_MAX_LIMIT`] with `?limit=`.
///
/// # **D4**
///
/// Nothing here is a matching key. The server does not compare `desc`,
/// `project_root` or anything else in the 記名 against anything; a
/// dispatch is addressed by `Run.current`'s `OperatorId` alone. The 記名
/// exists so a human or an AI can tell two sessions apart by eye.
pub async fn operators_list(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Query(query): axum::extract::Query<OperatorsListQuery>,
) -> Response {
    if let Err(resp) = authorize_any_operator(&state, &headers).await {
        return resp;
    }
    let limit = query
        .limit
        .unwrap_or(OPERATORS_LIST_DEFAULT_LIMIT)
        .min(OPERATORS_LIST_MAX_LIMIT);

    let entries: Vec<Arc<LoginSession>> = {
        let map = state.operator_sessions.lock().await;
        map.values().cloned().collect()
    };
    // The horizon, at the read that would otherwise be the one place these sessions
    // are visible forever. This route is where a crashed driver's row
    // accumulates — it cannot be deleted by anyone but the driver that is
    // gone — so it is also where the horizon has to bite. The list is
    // built from what survives, and `total` counts the same set, so an
    // expired session is not merely hidden from the page: it is gone, and
    // the count says so.
    let mut alive = Vec::with_capacity(entries.len());
    for live in entries {
        if !reap_if_expired(&state, &live.record().sid, &live).await {
            alive.push(live);
        }
    }
    let total = alive.len();
    let mut operators = Vec::with_capacity(total);
    for live in alive {
        let connected = live.dispatch_target().is_connected().await;
        let record = live.kimei().await;
        operators.push(OperatorsListEntry {
            sid: record.sid.clone(),
            joined_at_secs: record.joined_at_secs,
            connected,
            desc: record.desc.clone(),
            last_activity_secs: record.last_activity_secs(),
            observed: record.observed,
            observed_total: record.observed_total,
        });
    }
    operators.sort_by(|a, b| {
        b.last_activity_secs
            .cmp(&a.last_activity_secs)
            .then_with(|| a.sid.as_str().cmp(b.sid.as_str()))
    });
    operators.truncate(limit);
    (
        StatusCode::OK,
        Json(OperatorsListResp {
            operators,
            total,
            limit,
        }),
    )
        .into_response()
}

// ─── GET /v1/operators/:sid (Bearer required) ───────────────────────────────

/// Response for `GET /v1/operators/:sid`.
#[derive(Debug, Serialize)]
pub struct OperatorsInfoResp {
    /// Echoes the requested session id.
    pub sid: SessionId,
    /// Capability manifest pinned when this session joined.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capability_manifest: Option<AgentProviderManifest>,
    /// Whether a WS is currently attached (not merely that the session ever connected).
    pub connected: bool,
    /// This session's 記名, confirmed part (**D1**). `null`, never omitted
    /// — same reason as on [`OperatorsListEntry::desc`].
    pub desc: Option<String>,
    /// This session's 記名, observed part (**D2**), oldest first.
    pub observed: Vec<ObservedAssignment>,
    /// Every `Assign` recorded onto [`Self::observed`], including folded
    /// and aged-out ones.
    pub observed_total: u64,
}

/// `GET /v1/operators/:sid`. Bearer mandatory. `404` on unknown sid, `401` on
/// token mismatch. `connected` reflects whether the reusable session currently
/// owns a live sender, not merely whether it connected at least once.
pub async fn operators_info(
    State(state): State<AppState>,
    Path(sid): Path<String>,
    headers: HeaderMap,
) -> Response {
    let bearer = match extract_bearer_token_required(&headers) {
        Ok(t) => t,
        Err(resp) => return *resp,
    };
    let Ok(sid) = SessionId::parse(sid) else {
        return (StatusCode::NOT_FOUND, "unknown sid").into_response();
    };

    let live = {
        let map = state.operator_sessions.lock().await;
        map.get(&sid).cloned()
    };
    let live = match live {
        Some(e) => e,
        None => return (StatusCode::NOT_FOUND, "unknown sid").into_response(),
    };
    if !live.verify_bearer(&bearer) {
        return (StatusCode::UNAUTHORIZED, "token mismatch").into_response();
    }
    // Same ordering as the WS connect: the expiry is judged against the
    // access clock as it stood *before* this call, then this call becomes
    // the newest access.
    if reap_if_expired(&state, &sid, &live).await {
        return (StatusCode::NOT_FOUND, "unknown sid").into_response();
    }
    live.touch(&state.operator_session_store).await;

    let connected = live.dispatch_target().is_connected().await;
    let record = live.kimei().await;
    (
        StatusCode::OK,
        Json(OperatorsInfoResp {
            sid: record.sid.clone(),
            capability_manifest: record.capability_manifest.clone(),
            connected,
            desc: record.desc,
            observed: record.observed,
            observed_total: record.observed_total,
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use tokio::sync::Mutex;

    fn headers_with_bearer(token: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        h
    }

    #[test]
    fn extract_bearer_token_required_accepts_valid() {
        let h = headers_with_bearer("abc123");
        assert_eq!(extract_bearer_token_required(&h).unwrap(), "abc123");
    }

    #[test]
    fn extract_bearer_token_required_rejects_missing_header() {
        let h = HeaderMap::new();
        assert!(extract_bearer_token_required(&h).is_err());
    }

    #[test]
    fn extract_bearer_token_required_rejects_empty_token() {
        let h = headers_with_bearer("");
        assert!(extract_bearer_token_required(&h).is_err());
    }

    #[test]
    fn extract_bearer_token_required_rejects_wrong_scheme() {
        let mut h = HeaderMap::new();
        h.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_static("Basic dXNlcjpwYXNz"),
        );
        assert!(extract_bearer_token_required(&h).is_err());
    }

    #[test]
    fn operators_create_request_accepts_capability_manifest() {
        let req: OperatorsCreateReq = serde_json::from_value(serde_json::json!({
            "capability_manifest": {
                "provider_id": "main-ai-self-report",
                "capabilities": [{
                    "launch_variant": "mse-coder",
                    "resolved_model": "claude-sonnet-4",
                    "effective_tools": ["Read", "Edit"]
                }]
            }
        }))
        .unwrap();
        assert_eq!(
            req.capability_manifest.unwrap().provider_id,
            "main-ai-self-report"
        );
    }

    #[test]
    fn operators_create_request_keeps_manifest_optional_on_wire() {
        let req: OperatorsCreateReq = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(req.capability_manifest.is_none());
    }

    /// A caller still sending the removed `roles` field joins anyway. Join
    /// is the one step that must never refuse an incoming Assignee
    /// (**D3**), so an unknown key is ignored rather than made into a
    /// `422` that would lock an older driver out of a newer server.
    #[test]
    fn operators_create_request_ignores_a_stale_roles_field() {
        let req: OperatorsCreateReq = serde_json::from_value(serde_json::json!({
            "roles": ["main-ai"],
            "desc": "an older client that still sends roles",
        }))
        .unwrap();
        assert_eq!(
            req.desc.as_deref(),
            Some("an older client that still sends roles")
        );
    }

    // ── shared fixtures ──────────────────────────────────────────────────

    /// The `AppState` and body helper every test module below builds on.
    ///
    /// This used to be `by_role_in_flight`, whose own four tests covered
    /// `DELETE /v1/operators/by-role/:role`: its `?force=true` escape
    /// hatch, its in-flight `409`, and the `404` for a role nobody held.
    /// The route is gone with role declaration, and so are they — what a
    /// stale session is released by now is its sid, which
    /// `operators_delete`'s own tests already cover.
    mod support {
        use super::*;
        use mlua_swarm::core::config::EngineCfg;
        use mlua_swarm::core::engine::Engine;
        use mlua_swarm::store::output::InMemoryOutputStore;
        use mlua_swarm::store::run::InMemoryRunStore;
        use mlua_swarm::store::task::InMemoryTaskStore;
        use std::collections::HashMap;

        pub(super) fn test_state() -> AppState {
            let engine =
                Engine::new_with_layers(EngineCfg::default(), crate::default_layer_registry());
            let compiler = mlua_swarm::Compiler::new(crate::default_registry());
            let launch = Arc::new(mlua_swarm::TaskLaunchService::new(engine.clone(), compiler));
            AppState {
                engine,
                sessions: Arc::new(Mutex::new(crate::SessionStore::default())),
                task_app: Arc::new(mlua_swarm::TaskApplication::new_inline_only(launch)),
                operator_adapters: Arc::new(OperatorAdapterRegistry::new()),
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
                periodic_reports: Default::default(),
            }
        }

        pub(super) async fn body_json(response: Response) -> serde_json::Value {
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("read body");
            serde_json::from_slice(&bytes).expect("json body")
        }
    }

    // ── O8: a delete leaves no holder behind ─────────────────────────────

    /// **O8** exists for the sake of the handover list: a `current` that
    /// names an Operator nobody can reach makes the material a joining AI
    /// reads to answer "is this mine, and is anyone on it?" lie to it.
    /// These tests are written against that reading — they assert on what
    /// `GET /v1/runs/:id` shows, not on the store call underneath.
    mod o8_cascade {
        use super::support::test_state;
        use super::*;
        use mlua_swarm::store::run::{RunRecord, RunStatus};
        use mlua_swarm::{RunId, TaskId};

        /// The seat names in play. Neither is a role word — an Operator
        /// seat is a lane of the flow, not a job title.
        const SEAT_A: &str = "phase-a-op";
        const SEAT_B: &str = "phase-b-op";
        /// The bearer every seeded session answers to.
        const TOKEN: &str = "token";

        async fn seed_session(state: &AppState) -> SessionId {
            let sid = SessionId::new();
            let live = LoginSession::new(
                OperatorSessionRecord {
                    sid: sid.clone(),
                    token_digest: OperatorSessionRecord::digest_of(TOKEN),
                    capability_manifest: None,
                    // Wall clock rather than `0`: `0` is 1970, which
                    // the 24h horizon expires on sight, and these tests are about a
                    // live session.
                    joined_at_secs: now_secs(),
                    last_access_secs: now_secs(),
                    desc: None,
                    observed: Vec::new(),
                    observed_total: 0,
                },
                None,
                None,
            );
            state
                .operator_sessions
                .lock()
                .await
                .insert(sid.clone(), live);
            sid
        }

        async fn seed_run(state: &AppState, status: RunStatus) -> RunId {
            let run_id = RunId::new();
            state
                .run_store
                .create(RunRecord {
                    id: run_id.clone(),
                    task_id: TaskId::new(),
                    status,
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

        async fn seat(state: &AppState, run_id: &RunId, slot: &str, op: &str) {
            state
                .run_store
                .acquire_assignee(run_id, slot, op, "seated by the cascade test")
                .await
                .expect("seed the holder");
        }

        /// Who `GET /v1/runs/:id` reports holding `slot` — the read the
        /// cascade exists to keep honest.
        async fn holder_on_the_wire(
            state: &AppState,
            run_id: &RunId,
            slot: &str,
        ) -> Option<String> {
            crate::tasks::run_get(State(state.clone()), Path(run_id.to_string()))
                .await
                .expect("run get")
                .0
                .current
                .get(slot)
                .map(|assignee| assignee.op.clone())
        }

        async fn delete_session(state: &AppState, sid: &SessionId) {
            let response = operators_delete(
                State(state.clone()),
                Path(sid.to_string()),
                headers_with_bearer(TOKEN),
            )
            .await;
            assert_eq!(response.status(), StatusCode::NO_CONTENT);
        }

        /// **O8, every seat of every Run.** A launch seats its operator in
        /// each declared lane, so one delete has several seats to release
        /// across several Runs. It releases all of them — and leaves a seat
        /// held by an unrelated operator exactly where it was, because O8
        /// is scoped to the operator that was deleted, not to the Run.
        #[tokio::test]
        async fn deleting_an_operator_releases_every_seat_it_held() {
            let state = test_state();
            let sid = seed_session(&state).await;

            let one_lane = seed_run(&state, RunStatus::Running).await;
            seat(&state, &one_lane, SEAT_A, sid.as_str()).await;

            let two_lanes = seed_run(&state, RunStatus::Running).await;
            seat(&state, &two_lanes, SEAT_A, sid.as_str()).await;
            seat(&state, &two_lanes, SEAT_B, "S-somebody-else").await;

            delete_session(&state, &sid).await;

            assert_eq!(
                holder_on_the_wire(&state, &one_lane, SEAT_A).await,
                None,
                "the seat it held must not survive the delete"
            );
            assert_eq!(
                holder_on_the_wire(&state, &two_lanes, SEAT_A).await,
                None,
                "nor the one it held on another Run"
            );
            assert_eq!(
                holder_on_the_wire(&state, &two_lanes, SEAT_B).await,
                Some("S-somebody-else".to_string()),
                "another operator's seat on the same Run is none of this delete's business"
            );
        }

        /// **A4.** `Vacant` is an assignment event like any other, so the
        /// cascade advances `G`: the next acquire continues from the
        /// bumped counter rather than reusing the released holder's
        /// generation, which is what keeps a late reply from the deleted
        /// operator from matching (**A6**).
        #[tokio::test]
        async fn the_cascade_counts_as_an_assignment_event() {
            let state = test_state();
            let sid = seed_session(&state).await;
            let run_id = seed_run(&state, RunStatus::Running).await;
            seat(&state, &run_id, SEAT_A, sid.as_str()).await;
            assert_eq!(
                state.run_store.get(&run_id).await.expect("run").current[SEAT_A].gen,
                1
            );

            delete_session(&state, &sid).await;

            let after = state.run_store.get(&run_id).await.expect("run");
            assert!(after.current.is_empty(), "T5 / O8: the seat is released");
            assert_eq!(
                after.next_generation, 2,
                "A4: releasing the seat is an event and bumps G"
            );
        }

        /// The scan covers every status a Run can still be dispatched
        /// under — including the two that read as finished but are not:
        /// `Interrupted` (resumable in place) and `Cancelled` (a marker;
        /// in-flight abort is still a carry).
        #[tokio::test]
        async fn every_dispatchable_status_is_swept() {
            let state = test_state();
            let sid = seed_session(&state).await;

            let mut runs = Vec::new();
            for status in [
                RunStatus::Pending,
                RunStatus::Running,
                RunStatus::Interrupted,
                RunStatus::Cancelled,
            ] {
                let run_id = seed_run(&state, status).await;
                seat(&state, &run_id, SEAT_A, sid.as_str()).await;
                runs.push((status, run_id));
            }

            delete_session(&state, &sid).await;

            for (status, run_id) in runs {
                assert_eq!(
                    holder_on_the_wire(&state, &run_id, SEAT_A).await,
                    None,
                    "a {status:?} run can still dispatch, so its seat must be released"
                );
            }
        }

        /// `Done` / `Failed` are left alone on purpose: their `current` is
        /// no longer a live pointer but the record of who held the seat
        /// when the Run ended, and that is worth more to a reader than an
        /// empty map. The scan is bounded by live work rather than by
        /// history — see [`super::super::CASCADE_STATUSES`].
        #[tokio::test]
        async fn a_finished_run_keeps_its_record_of_who_held_the_seat() {
            let state = test_state();
            let sid = seed_session(&state).await;
            let done = seed_run(&state, RunStatus::Done).await;
            let failed = seed_run(&state, RunStatus::Failed).await;
            seat(&state, &done, SEAT_A, sid.as_str()).await;
            seat(&state, &failed, SEAT_A, sid.as_str()).await;

            delete_session(&state, &sid).await;

            assert_eq!(
                holder_on_the_wire(&state, &done, SEAT_A).await,
                Some(sid.to_string()),
                "a Done run records who finished it"
            );
            assert_eq!(
                holder_on_the_wire(&state, &failed, SEAT_A).await,
                Some(sid.to_string()),
                "and a Failed one records who was holding it when it failed"
            );
        }

        /// **R7 / W4.** A seat emptied by the cascade was not emptied by
        /// its holder, so the loss goes on the Run's own trace next to
        /// its step events — naming the seat, the holder that was
        /// released, and that an operator delete is what did it. Without
        /// the row the only evidence is a dispatch failing as `Vacant`
        /// some time later.
        ///
        /// A `Done` Run is the control: nothing was released there, so
        /// nothing is recorded there either.
        #[tokio::test]
        async fn the_cascade_records_each_release_on_the_runs_trace() {
            use mlua_swarm::store::trace::{kind as trace_kind, TraceQuery};

            let state = test_state();
            let sid = seed_session(&state).await;
            let live = seed_run(&state, RunStatus::Running).await;
            let finished = seed_run(&state, RunStatus::Done).await;
            seat(&state, &live, SEAT_A, sid.as_str()).await;
            seat(&state, &finished, SEAT_A, sid.as_str()).await;

            delete_session(&state, &sid).await;

            let events = state
                .run_trace_store
                .list(&live, &TraceQuery::default())
                .await
                .expect("trace list");
            assert_eq!(events.len(), 1, "one row per seat released: {events:?}");
            assert_eq!(events[0].kind, trace_kind::ASSIGNEE_RELEASED);
            assert_eq!(events[0].payload["slot"], SEAT_A);
            assert_eq!(events[0].payload["assignee"]["op"], sid.to_string());
            assert_eq!(events[0].payload["assignee"]["gen"], 1);
            assert_eq!(events[0].payload["reason"], "o8_operator_deleted");

            assert!(
                state
                    .run_trace_store
                    .list(&finished, &TraceQuery::default())
                    .await
                    .expect("trace list")
                    .is_empty(),
                "a Done run keeps its holder, so there is no release to record"
            );
        }
    }

    // ── registration moved to mint: the invariant behind the deleted arm ──

    /// `handle_operator_socket` no longer has a "this entry has no session
    /// yet" arm, because no entry is ever in that state. These tests hold
    /// the two halves of that invariant in place.
    ///
    /// The arm mattered: it built a `WSOperatorSession` and **registered**
    /// it. `operators_ws_connect` clones the entry, verifies the Bearer and
    /// returns `ws.on_upgrade(...)`, so the closure runs only after the
    /// HTTP response is written — and a teardown can land in that gap. The
    /// arm then registered a session under a sid that had already left
    /// `operator_sessions`, which no route could undo: `DELETE
    /// /v1/operators/:sid` answers `404` without the entry. The
    /// registration survived until process exit.
    ///
    /// Driving that interleaving through a live server is not something a
    /// test can order (the gap is between hyper writing `101` and running
    /// the upgrade callback), so it is pinned structurally instead: mint
    /// publishes a [`LoginSession`] whose dispatch target is already
    /// registered, and teardown closes that target in place. A late socket
    /// therefore always finds it, and the only thing it can do is
    /// `replace_tx`.
    mod registration_is_owned_by_mint {
        use super::support::{body_json, test_state};
        use super::*;

        async fn mint(state: &AppState) -> SessionId {
            let response = operators_create(
                State(state.clone()),
                Json(OperatorsCreateReq {
                    capability_manifest: None,
                    desc: None,
                }),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK, "mint must succeed");
            let body = body_json(response).await;
            SessionId::parse(body["sid"].as_str().expect("sid").to_string()).expect("parse sid")
        }

        /// Half 1: minting registers, and publishes a session whose
        /// dispatch target is already attached. Nothing is left for a
        /// connect to do. That the target is present is carried by the type
        /// now, so what is checked here is the registry membership.
        #[tokio::test]
        async fn mint_publishes_an_entry_that_already_carries_a_registered_session() {
            let state = test_state();
            let sid = mint(&state).await;

            let registered = state.engine.list_operator_ids().await;
            assert!(
                registered.contains(&sid.to_string()),
                "mint must register the sid (this is what makes it usable as an \
                 `operator_sid` pin before any WS connect): {registered:?}"
            );
            assert_eq!(
                registered.len(),
                1,
                "and nothing else: a session is registered under its sid alone, so a \
                 second session can never collide with this one's key: {registered:?}"
            );

            assert!(
                state.operator_sessions.lock().await.contains_key(&sid),
                "mint must publish the session it registered"
            );
        }

        /// **Nothing is routable before its exits exist.**
        ///
        /// A mint publishes two things: the `operator_sessions` entry a
        /// routed send finds its exits through (a connect's `replace_tx`, a
        /// teardown's `TornDown`), and the registrations that make the sid
        /// routable at all. Registering first left a window — a request
        /// dying between the two statements, which hyper does whenever the
        /// peer disconnects mid-request — in which a dispatch could reach
        /// the sid, park, and never be woken by either exit, burning the
        /// run's whole `sync_timeout_secs` before reporting a timeout that
        /// named the wrong cause.
        ///
        /// Holding the map's lock is what makes that window observable
        /// without racing anything: the mint blocks at exactly the
        /// statement whose ordering is under test, and the assertion is
        /// that **nothing is registered** while it is stuck there. Under
        /// the old order the four registrations had already landed by this
        /// point, with the map entry they need still unwritten.
        #[tokio::test]
        async fn a_mint_registers_nothing_until_its_session_is_in_the_map() {
            let state = test_state();

            let guard = state.operator_sessions.lock().await;
            let minting = tokio::spawn({
                let state = state.clone();
                async move {
                    operators_create(
                        State(state),
                        Json(OperatorsCreateReq {
                            capability_manifest: None,
                            desc: Some("a mint held at the map insert".to_string()),
                        }),
                    )
                    .await
                }
            });

            // Long enough for the mint to have reached — and blocked on —
            // the map insert. Under the old order it is also long enough
            // for the registrations it did first to be observable.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            let registered = state.engine.list_operator_ids().await;
            assert!(
                registered.is_empty(),
                "a session must not be routable before the map holds it: a send routed \
                 here would park with neither of its two exits reachable, and wait out \
                 the run's whole ceiling: {registered:?}"
            );
            let adapters = state.operator_adapters.ids().await;
            assert!(
                adapters.is_empty(),
                "the same holds for the adapter registry a seat's holder resolves \
                 through: {adapters:?}"
            );

            drop(guard);
            let response = minting.await.expect("the mint task must not panic");
            assert_eq!(response.status(), StatusCode::OK);
            let sid = SessionId::parse(
                body_json(response).await["sid"]
                    .as_str()
                    .expect("sid")
                    .to_string(),
            )
            .expect("parse sid");

            // ...and once the lock is released both halves land, so the
            // ordering costs the finished session nothing.
            assert!(state.operator_sessions.lock().await.contains_key(&sid));
            assert!(state
                .engine
                .list_operator_ids()
                .await
                .contains(&sid.to_string()));
            assert!(state
                .operator_adapters
                .ids()
                .await
                .contains(&sid.to_string()));
        }

        /// Half 2: teardown closes the dispatch target but leaves it on the
        /// session, so a socket still holding that session finds the very
        /// same object and is answered with the latched Close — rather than
        /// finding an empty slot.
        #[tokio::test]
        async fn teardown_leaves_the_closed_session_on_the_entry() {
            let state = test_state();
            let sid = mint(&state).await;
            let live = state
                .operator_sessions
                .lock()
                .await
                .get(&sid)
                .cloned()
                .expect("mint must publish the session");
            // Stands in for the handle a socket grabbed in
            // `operators_ws_connect` before the teardown ran.
            let captured = live.dispatch_target().clone();

            teardown_operator_session(&state, &sid, &live)
                .await
                .expect("teardown must succeed against a healthy store");

            assert!(
                !state.operator_sessions.lock().await.contains_key(&sid),
                "teardown must remove the session from the map"
            );
            let registered = state.engine.list_operator_ids().await;
            assert!(
                !registered.contains(&sid.to_string()),
                "teardown must unregister the sid: {registered:?}"
            );

            assert!(
                Arc::ptr_eq(&captured, live.dispatch_target()),
                "teardown must leave the same dispatch target in place: a socket \
                 that upgraded before it observes the close through the handle it \
                 already holds, so a replacement object would be invisible to it"
            );
            let session = captured;
            let mut signal = session.close_signal();
            assert!(
                signal.borrow_and_update().is_some(),
                "the close must already be latched, so a socket subscribing after \
                 the teardown still observes it and shuts down"
            );
            assert!(
                !session.is_connected().await,
                "teardown must have cleared the sender"
            );
        }

        /// A minted session must be reachable on both operator-side
        /// registries, under its `OperatorId`.
        ///
        /// This is what keeps the launch-time guard honest. `POST /v1/tasks`
        /// answers "is this `operator_sid` a live session?" out of
        /// `engine.list_operator_ids()`, while a dispatch resolves the
        /// holder recorded in `Run.current` out of the adapter registry. Two
        /// maps, one id space — a sid the guard accepts but no router could
        /// deliver to would turn a `400` at launch into a failure several
        /// steps later, naming the wrong thing. One call site writes both,
        /// and this pins that both directions of it (register and
        /// unregister) stay in step.
        #[tokio::test]
        async fn a_minted_session_is_reachable_on_the_guard_and_the_dispatch_side() {
            let state = test_state();
            let sid = mint(&state).await;

            let guard_side = state.engine.list_operator_ids().await;
            let dispatch_side = state.operator_adapters.ids().await;
            let id = sid.to_string();
            assert!(
                guard_side.contains(&id),
                "the launch guard must know '{id}': {guard_side:?}"
            );
            assert!(
                dispatch_side.contains(&id),
                "a holder recorded as '{id}' must be deliverable to: {dispatch_side:?}"
            );

            let live = state
                .operator_sessions
                .lock()
                .await
                .get(&sid)
                .cloned()
                .expect("mint must publish the session");
            teardown_operator_session(&state, &sid, &live)
                .await
                .expect("teardown must succeed against a healthy store");

            assert!(
                state.operator_adapters.ids().await.is_empty(),
                "teardown must drop the adapter bindings too, or a Run still \
                 naming this sid would keep resolving to a torn-down session"
            );
            assert!(state.engine.list_operator_ids().await.is_empty());
        }

        /// A repeat teardown on the same session stays a no-op. Keeping the
        /// dispatch target in place (rather than detaching it) must not turn
        /// the second call into a second round of side effects.
        #[tokio::test]
        async fn a_repeated_teardown_is_still_idempotent() {
            let state = test_state();
            let sid = mint(&state).await;
            let live = state
                .operator_sessions
                .lock()
                .await
                .get(&sid)
                .cloned()
                .expect("mint must publish the session");

            teardown_operator_session(&state, &sid, &live)
                .await
                .expect("first teardown");
            teardown_operator_session(&state, &sid, &live)
                .await
                .expect("a repeat teardown must still report success");

            assert!(!state.operator_sessions.lock().await.contains_key(&sid));
            assert!(!state
                .engine
                .list_operator_ids()
                .await
                .contains(&sid.to_string()));
        }

        /// A mint whose persist fails must leave nothing behind — no
        /// registration and no entry. This is why the registration sits
        /// *after* `store.put` rather than before it with a compensating
        /// unregister.
        #[tokio::test]
        async fn a_mint_whose_persist_fails_registers_nothing() {
            let mut state = test_state();
            state.operator_session_store = Arc::new(AlwaysFailingPutStore);

            let response = operators_create(
                State(state.clone()),
                Json(OperatorsCreateReq {
                    capability_manifest: None,
                    desc: None,
                }),
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::INTERNAL_SERVER_ERROR,
                "a persist failure must surface as 5xx"
            );

            assert!(
                state.engine.list_operator_ids().await.is_empty(),
                "a failed mint must not leave a registration behind — nothing \
                 would ever be able to unregister it"
            );
            assert!(state.operator_sessions.lock().await.is_empty());
        }

        /// The `409` that is gone: two sessions minted back to back both
        /// come up live, registered under their own sids. This used to be
        /// the collision case (`roles conflict`), and it is now simply two
        /// drivers on one server — the ordinary shape when two tasks run in
        /// parallel.
        #[tokio::test]
        async fn two_joins_in_a_row_both_succeed_and_both_stay_reachable() {
            let state = test_state();
            let first = mint(&state).await;
            let second = mint(&state).await;
            assert_ne!(first, second, "O4: each join mints a new OperatorId");

            let registered = state.engine.list_operator_ids().await;
            for sid in [&first, &second] {
                assert!(
                    registered.contains(&sid.to_string()),
                    "the second join must not have displaced the first: {registered:?}"
                );
            }
            assert_eq!(state.operator_sessions.lock().await.len(), 2);
        }

        /// Store whose `put` always fails; `delete` / `list` are honest.
        struct AlwaysFailingPutStore;

        #[async_trait::async_trait]
        impl mlua_swarm::store::operator_session::OperatorSessionStore for AlwaysFailingPutStore {
            fn name(&self) -> &str {
                "always-failing-put"
            }

            async fn put(
                &self,
                _record: OperatorSessionRecord,
            ) -> Result<(), OperatorSessionStoreError> {
                Err(OperatorSessionStoreError::Other(
                    "injected persist failure".to_string(),
                ))
            }

            async fn delete(&self, sid: &SessionId) -> Result<(), OperatorSessionStoreError> {
                Err(OperatorSessionStoreError::NotFound(sid.clone()))
            }

            /// Honest about what a store whose every `put` failed holds:
            /// nothing.
            async fn get(
                &self,
                _sid: &SessionId,
            ) -> Result<Option<OperatorSessionRecord>, OperatorSessionStoreError> {
                Ok(None)
            }

            async fn list(&self) -> Result<Vec<OperatorSessionRecord>, OperatorSessionStoreError> {
                Ok(Vec::new())
            }
        }
    }

    // ── the one rule an OperatorRef carries: not empty ───────────────────

    /// A join that carries nothing at all still mints.
    ///
    /// This module used to hold the `roles: [""]` rejection — an empty
    /// alias named no Operator, so a session claiming it could never be
    /// routed to, and the mint answered `400` before reserving anything.
    /// With no aliases there is no such request to refuse, and what is
    /// worth pinning instead is the other half of that old pair: the
    /// emptiest possible join is a valid one, and the route has no `400`
    /// arm left for anything to fall into.
    mod a_join_carries_nothing {
        use super::support::{body_json, test_state};
        use super::*;

        #[tokio::test]
        async fn an_empty_body_mints_a_session() {
            let state = test_state();
            let response =
                operators_create(State(state.clone()), Json(OperatorsCreateReq::default())).await;
            assert_eq!(
                response.status(),
                StatusCode::OK,
                "join must never refuse — an incoming Assignee has to be able to get in (D3)"
            );

            let body = body_json(response).await;
            let sid = body["sid"].as_str().expect("the mint answers with a sid");
            assert!(
                body.get("roles").is_none(),
                "the response carries no roles field any more: {body}"
            );
            assert!(
                state
                    .engine
                    .list_operator_ids()
                    .await
                    .contains(&sid.to_string()),
                "and the sid it answered with is registered"
            );
        }
    }

    // ── the spine: a handover moves where the NEXT dispatch lands ─────────

    /// The whole point of the seat/holder split, exercised through the
    /// paths production uses rather than hand-built parts:
    ///
    /// - the sessions are minted through `operators_create`, so they are
    ///   registered by `register_operator_session` — the one call site;
    /// - the Operator wiring is `WsOperatorWiring::new`, exactly what
    ///   `mse serve` builds, so the seats resolve through the factory's own
    ///   slot resolver over the same adapter registry the mint wrote into;
    /// - the Blueprint is compiled by a real `Compiler` over a
    ///   `SpawnerRegistry` holding that factory, so the `Arc<dyn Operator>`
    ///   under test is the one `OperatorSpawnerFactory::build` produces;
    /// - each dispatch is answered like a WS client answers it (read the
    ///   `Spawn` frame off the session's sender, resolve its `req_id`), so
    ///   "where did it land" is measured at the socket, not inferred.
    ///
    /// What is deliberately *not* re-done between the two dispatches: the
    /// compile, the registration, the routers. Only `Run.current` moves.
    mod the_spine {
        use super::support::{body_json, test_state};
        use super::*;
        use crate::operator_ws::router::WsOperatorWiring;
        use mlua_swarm::blueprint::Blueprint;
        use mlua_swarm::store::run::{RunRecord, RunStatus};
        use mlua_swarm::{
            CapToken, Compiler, Ctx, Operator, OperatorSpawnerFactory, Role, RunId,
            SpawnerRegistry, StepId, TaskId, WorkerBinding,
        };
        use tokio::sync::mpsc::UnboundedReceiver;

        /// The two Blueprint-declared Operator seats, and the agent that
        /// dispatches through each. Named for the lane they serve — a seat
        /// is a position in the Blueprint, never a person.
        const SEAT_A: &str = "phase-a-op";
        const SEAT_B: &str = "phase-b-op";
        const AGENT_A: &str = "lane-a-relay";
        const AGENT_B: &str = "lane-b-relay";

        /// Two seats, one `kind = Operator` agent each. Both agents declare
        /// a `worker_binding`: the WS thin path requires one, and the
        /// routers in front of those sessions report the same requirement,
        /// so a Blueprint without it would fail this compile.
        fn two_seat_blueprint() -> Blueprint {
            serde_json::from_value(serde_json::json!({
                "schema_version": mlua_swarm::blueprint::current_schema_version(),
                "id": "spine-two-seat-bp",
                "flow": {
                    "kind": "step",
                    "ref": AGENT_A,
                    "in": { "op": "path", "at": "$.input" },
                    "out": { "op": "path", "at": "$.output" }
                },
                "agents": [
                    {
                        "name": AGENT_A,
                        "kind": "operator",
                        "spec": { "operator_ref": SEAT_A },
                        "profile": { "worker_binding": "mse-worker" }
                    },
                    {
                        "name": AGENT_B,
                        "kind": "operator",
                        "spec": { "operator_ref": SEAT_B },
                        "profile": { "worker_binding": "mse-worker" }
                    }
                ],
                "operators": [{ "name": SEAT_A }, { "name": SEAT_B }],
                "strategy": { "strict_refs": false }
            }))
            .expect("test Blueprint literal")
        }

        /// A minted Operator session with a sender attached — what
        /// `handle_operator_socket` leaves behind when a client's WS
        /// connects, minus the socket. `inbox` is the wire: every frame the
        /// session would have written to that client shows up here.
        struct Client {
            name: &'static str,
            sid: SessionId,
            session: Arc<WSOperatorSession>,
            inbox: UnboundedReceiver<ServerMsg>,
        }

        async fn mint_client(state: &AppState, name: &'static str) -> Client {
            let response = operators_create(
                State(state.clone()),
                Json(OperatorsCreateReq {
                    capability_manifest: None,
                    desc: Some(format!("spine test client: {name}")),
                }),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK, "mint must succeed");
            let body = body_json(response).await;
            let sid = SessionId::parse(body["sid"].as_str().expect("sid").to_string())
                .expect("parse sid");
            let live = state
                .operator_sessions
                .lock()
                .await
                .get(&sid)
                .cloned()
                .expect("mint must publish the session");
            let (tx, inbox) = mpsc::unbounded_channel();
            let session = live.dispatch_target().clone();
            session.replace_tx(tx).await;
            Client {
                name,
                sid,
                session,
                inbox,
            }
        }

        /// The wiring `mse serve` builds, over this state's Run store and
        /// this state's adapter registry, plus a compile of
        /// [`two_seat_blueprint`] through it. Returns the factory so a test
        /// can ask it for the same `Arc<dyn Operator>` a built agent got.
        fn wire_operator_axis(state: &mut AppState) -> Arc<OperatorSpawnerFactory> {
            let factory = Arc::new(OperatorSpawnerFactory::new());
            let wiring = WsOperatorWiring::new(factory.clone(), state.run_store.clone());
            state.operator_adapters = wiring.adapters.clone();
            factory
        }

        /// Compile the Blueprint the way a launch does. Nothing is
        /// registered under a seat name anywhere, which is the point: the
        /// compile resolves seats, not sessions, so it cannot depend on who
        /// happens to be logged in.
        fn compile_through(factory: &Arc<OperatorSpawnerFactory>) {
            let mut registry = SpawnerRegistry::new();
            registry.register::<OperatorSpawnerFactory>(factory.clone());
            Compiler::new(registry)
                .compile(&two_seat_blueprint())
                .expect("a two-seat Blueprint must compile with no session registered anywhere");
        }

        /// A `Running` Run and the Task row it belongs to.
        ///
        /// The Task is real rather than a bare id because the 記名's
        /// observed part is written from it (`goal` + `task_input_spec`),
        /// so a seating test exercises the whole write.
        async fn seeded_run(state: &AppState) -> (RunId, TaskId) {
            let run_id = RunId::new();
            let task_id = TaskId::new();
            state
                .task_store
                .create(mlua_swarm::store::task::TaskRecord {
                    id: task_id.clone(),
                    goal: "resolve issue #10".to_string(),
                    blueprint_ref: serde_json::json!({"inline": {}}),
                    input_ctx: serde_json::json!({}),
                    task_input_spec: Some(serde_json::json!({
                        "project_root": "/repo",
                        "work_dir": "/repo/.worktrees/topic",
                        "task_metadata": {"issue": 10},
                    })),
                    status: mlua_swarm::store::task::TaskRecordStatus::Running,
                    created_at: 0,
                    updated_at: 0,
                })
                .await
                .expect("seed task");
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
                    created_at: 0,
                    updated_at: 0,
                })
                .await
                .expect("seed run");
            (run_id, task_id)
        }

        fn ctx_for(run_id: &RunId, agent: &str) -> Ctx {
            let mut ctx = Ctx::new(StepId::parse("ST-spine").expect("step id"), 1, agent);
            ctx.meta
                .runtime
                .insert("run_id".to_string(), serde_json::json!(run_id.to_string()));
            ctx
        }

        fn worker_binding() -> WorkerBinding {
            WorkerBinding {
                variant: "mse-worker".to_string(),
                tools: Vec::new(),
                request_digest: None,
                requested_model: None,
            }
        }

        fn cap_token(agent: &str) -> CapToken {
            CapToken {
                agent_id: agent.to_string(),
                role: Role::Worker,
                scopes: vec!["*".into()],
                issued_at: 0,
                expire_at: u64::MAX / 2,
                max_uses: None,
                nonce: "spine-test-nonce".into(),
                sig_hex: String::new(),
            }
        }

        /// Dispatch through `router` and answer whichever of the two
        /// sessions receives the `Spawn` frame, the way that session's WS
        /// client would. Returns the name of the session it landed on —
        /// cross-checked against the value that came back out of `execute`,
        /// so a frame delivered to one session and answered as another
        /// cannot pass.
        async fn dispatch_and_name_the_receiver(
            router: Arc<dyn Operator>,
            ctx: Ctx,
            a: &mut Client,
            b: &mut Client,
        ) -> &'static str {
            let agent = ctx.agent.clone();
            let dispatch = tokio::spawn(async move {
                router
                    .execute(
                        &ctx,
                        None,
                        serde_json::json!("go"),
                        Some(worker_binding()),
                        cap_token(&agent),
                    )
                    .await
            });

            let (a_name, a_session) = (a.name, a.session.clone());
            let (b_name, b_session) = (b.name, b.session.clone());
            let (name, session, frame) = tokio::select! {
                frame = a.inbox.recv() => (a_name, a_session, frame.expect("session A inbox")),
                frame = b.inbox.recv() => (b_name, b_session, frame.expect("session B inbox")),
            };
            let ServerMsg::Spawn { req_id, .. } = frame else {
                panic!("a dispatch must reach the session as a Spawn frame, got: {frame:?}");
            };
            session
                .resolve_pending(
                    &req_id,
                    PendingReply::SpawnAck {
                        value: serde_json::json!({ "answered_by": name }),
                        ok: true,
                        error: None,
                        stats: None,
                    },
                )
                .await;
            let result = dispatch
                .await
                .expect("the dispatch task")
                .expect("the answered dispatch must succeed");
            assert_eq!(
                result.value["answered_by"], name,
                "the answer must come back out of the same dispatch that was delivered"
            );
            name
        }

        /// **The spine.** One compile, one registration, one router — and a
        /// re-assignment of `Run.current` in between two dispatches. The
        /// second one must land on the session that holds the seat *now*.
        ///
        /// Before the seat/holder split this was structurally impossible:
        /// the compile baked the session that held the seat at compile time
        /// into `routes[agent]`, so a handover could rewrite the Run's
        /// holder all it liked and every dispatch still went to the first
        /// session (model §4.3 **A10** — the destination is not baked in).
        #[tokio::test]
        async fn re_assigning_current_moves_the_next_dispatch_to_the_new_holder() {
            let mut state = test_state();
            let factory = wire_operator_axis(&mut state);
            compile_through(&factory);

            let mut first = mint_client(&state, "first").await;
            let mut second = mint_client(&state, "second").await;
            let (run_id, _task_id) = seeded_run(&state).await;

            // The one `Arc<dyn Operator>` under test: the same lookup
            // `OperatorSpawnerFactory::build` performs for `AGENT_A`, kept
            // across both dispatches exactly as a compiled route would be.
            let router = factory
                .resolve_operator(SEAT_A, AGENT_A)
                .expect("the wired factory must resolve a declared seat");

            // The launch pin: seat A is held by the first session.
            state
                .run_store
                .acquire_assignee(&run_id, SEAT_A, first.sid.as_str(), "pinned by the launch")
                .await
                .expect("launch assign");
            assert_eq!(
                dispatch_and_name_the_receiver(
                    router.clone(),
                    ctx_for(&run_id, AGENT_A),
                    &mut first,
                    &mut second,
                )
                .await,
                "first",
                "the launch pin's holder must receive the first dispatch"
            );

            // The handover. Nothing is recompiled, re-registered or rebuilt
            // — one row changes.
            let (gen, displaced) = state
                .run_store
                .acquire_assignee(&run_id, SEAT_A, second.sid.as_str(), "took over mid-run")
                .await
                .expect("handover assign");
            assert_eq!(gen, 2, "A4: the second assignment event is generation 2");
            assert_eq!(
                displaced.expect("seat A had a holder").op,
                first.sid.to_string(),
                "the displaced holder is the session that held the seat"
            );

            assert_eq!(
                dispatch_and_name_the_receiver(
                    router,
                    ctx_for(&run_id, AGENT_A),
                    &mut first,
                    &mut second,
                )
                .await,
                "second",
                "re-pointing Run.current must change where the NEXT dispatch lands"
            );
            assert!(
                first.inbox.try_recv().is_err(),
                "the displaced session must receive nothing after the handover"
            );
        }

        /// Per-lane independence on the same production path: two seats,
        /// two routers, one handover. The re-assigned lane follows its new
        /// holder; the untouched lane keeps delivering to its own.
        ///
        /// This is the routing contract behind the per-lane alias split the
        /// operator-execution-model guide documents (`phase_a_op` /
        /// `phase_b_op` as independent seats) — with a Run-wide holder,
        /// handing one lane over would drag the other lane's traffic with
        /// it.
        #[tokio::test]
        async fn re_assigning_one_seat_leaves_the_other_seats_destination_alone() {
            let mut state = test_state();
            let factory = wire_operator_axis(&mut state);
            compile_through(&factory);

            let mut first = mint_client(&state, "first").await;
            let mut second = mint_client(&state, "second").await;
            let (run_id, _task_id) = seeded_run(&state).await;

            let router_a = factory
                .resolve_operator(SEAT_A, AGENT_A)
                .expect("seat A resolves");
            let router_b = factory
                .resolve_operator(SEAT_B, AGENT_B)
                .expect("seat B resolves");

            state
                .run_store
                .acquire_assignee(&run_id, SEAT_A, first.sid.as_str(), "lane A launch pin")
                .await
                .expect("lane A assign");
            state
                .run_store
                .acquire_assignee(&run_id, SEAT_B, second.sid.as_str(), "lane B launch pin")
                .await
                .expect("lane B assign");

            // Hand lane A over to the session that already holds lane B.
            // Lane B is not touched.
            state
                .run_store
                .acquire_assignee(&run_id, SEAT_A, second.sid.as_str(), "lane A took over")
                .await
                .expect("lane A handover");

            assert_eq!(
                dispatch_and_name_the_receiver(
                    router_a,
                    ctx_for(&run_id, AGENT_A),
                    &mut first,
                    &mut second,
                )
                .await,
                "second",
                "the re-assigned lane must follow its new holder"
            );
            assert_eq!(
                dispatch_and_name_the_receiver(
                    router_b,
                    ctx_for(&run_id, AGENT_B),
                    &mut first,
                    &mut second,
                )
                .await,
                "second",
                "the untouched lane must keep delivering to its own holder"
            );

            let record = state.run_store.get(&run_id).await.expect("run get");
            assert_eq!(
                record.current[SEAT_B].gen, 2,
                "A3: an untouched seat's holder generation does not move"
            );
            assert!(
                first.inbox.try_recv().is_err(),
                "the session that holds neither seat any more receives nothing"
            );
        }

        /// **The launch reaches its operator, in every lane.** A pinned
        /// launch seats the pinning session in the seat it named *and* in
        /// every other declared seat, so a multi-seat Blueprint comes up
        /// fully dispatchable rather than with one lane seated and the rest
        /// Vacant.
        ///
        /// The regression this locks in has moved once already. Routing
        /// went from the factory's role lookup
        /// (`lookup_key = pin.unwrap_or(operator_ref)`) to `Run.current`,
        /// and the only writer of `current` was a launch carrying a pin —
        /// so a launch without one compiled and died on its first dispatch
        /// naming a `Vacant` seat. That was patched by seating each
        /// declared seat from whoever held the *role* of the same name;
        /// with roles gone, it is the launching operator that takes them
        /// (model §5), which is the same guarantee without the name lookup.
        ///
        /// The seat the pin named keeps the caller's own `desc`; the rest
        /// carry the server-authored one, so `GET /v1/runs/:id` can tell
        /// which lane was actually asked for (**A9**).
        #[tokio::test]
        async fn a_launch_seats_its_operator_in_every_declared_seat_and_reaches_it() {
            let mut state = test_state();
            let factory = wire_operator_axis(&mut state);
            compile_through(&factory);

            let mut launcher = mint_client(&state, "launcher").await;
            let mut bystander = mint_client(&state, "bystander").await;
            let (run_id, task_id) = seeded_run(&state).await;

            // A launch carrying `operator_sid` + `operator_desc`, in the
            // order the handlers run them: the pin first, then the rest of
            // the declared seats.
            crate::tasks::assign_launch_operator(
                &state,
                &run_id,
                &task_id,
                SEAT_A,
                launcher.sid.as_str(),
                "pinned by the launch request",
            )
            .await
            .expect("launch pin");
            crate::tasks::seat_declared_operators(
                &state,
                &run_id,
                &task_id,
                &two_seat_blueprint().operators,
                Some((SEAT_A, launcher.sid.as_str())),
            )
            .await
            .expect("seating the seats the pin did not name");

            let record = state.run_store.get(&run_id).await.expect("run get");
            for seat in [SEAT_A, SEAT_B] {
                let seated = record
                    .current
                    .get(seat)
                    .unwrap_or_else(|| panic!("seat '{seat}' must be seated by the launch"));
                assert_eq!(
                    seated.op,
                    launcher.sid.to_string(),
                    "every lane goes to the launching operator, and `op` is its sid"
                );
            }
            assert_eq!(
                record.current[SEAT_A].desc, "pinned by the launch request",
                "the caller's own desc survives on the seat it named"
            );
            assert!(
                record.current[SEAT_B]
                    .desc
                    .starts_with("auto-seated at launch"),
                "A9: the lane the caller did not name says the server chose it: {}",
                record.current[SEAT_B].desc
            );
            assert_eq!(
                record.current[SEAT_A].gen, 1,
                "A4: the launch pin is the Run's first assignment event"
            );

            // And both lanes dispatch — measured at the socket.
            for (seat, agent) in [(SEAT_A, AGENT_A), (SEAT_B, AGENT_B)] {
                assert_eq!(
                    dispatch_and_name_the_receiver(
                        factory
                            .resolve_operator(seat, agent)
                            .expect("the seat resolves"),
                        ctx_for(&run_id, agent),
                        &mut launcher,
                        &mut bystander,
                    )
                    .await,
                    "launcher",
                    "lane '{seat}' must reach the operator that launched the Run"
                );
            }
            assert!(
                bystander.inbox.try_recv().is_err(),
                "a session that launched nothing receives nothing"
            );
        }

        /// **An unpinned launch seats nobody, and the first dispatch says
        /// so.** Nothing in a pin-less `POST /v1/tasks` names an operator,
        /// so filling a seat would mean picking one — and the pick would be
        /// invisible in the Run, right until a second driver made it wrong.
        /// The seat stays `Vacant` and fails loudly instead.
        #[tokio::test]
        async fn an_unpinned_launch_leaves_every_seat_vacant_and_fails_loudly() {
            let mut state = test_state();
            let factory = wire_operator_axis(&mut state);
            compile_through(&factory);

            // A live, connected session exists — so the only thing keeping
            // it out of the seat is the refusal to guess.
            let mut only_driver = mint_client(&state, "the only driver here").await;
            let (run_id, task_id) = seeded_run(&state).await;

            crate::tasks::seat_declared_operators(
                &state,
                &run_id,
                &task_id,
                &two_seat_blueprint().operators,
                None,
            )
            .await
            .expect("an unpinned launch has nothing to seat, which is not a failure");

            let record = state.run_store.get(&run_id).await.expect("run get");
            assert!(
                record.current.is_empty(),
                "no lane may be filled from a launch that named no operator: {:?}",
                record.current
            );
            assert_eq!(
                record.next_generation, 0,
                "A4: nothing was assigned, so no assignment event happened"
            );

            let err = factory
                .resolve_operator(SEAT_A, AGENT_A)
                .expect("seat A resolves")
                .execute(
                    &ctx_for(&run_id, AGENT_A),
                    None,
                    serde_json::json!("go"),
                    Some(worker_binding()),
                    cap_token(AGENT_A),
                )
                .await
                .expect_err("a Vacant seat has no holder to dispatch to");
            let msg = err.to_string();
            assert!(
                msg.contains(SEAT_A) && msg.contains("Vacant"),
                "the failure must name the Vacant seat: {msg}"
            );
            assert!(
                msg.contains("operator_sid"),
                "and say how the seat could have been filled: {msg}"
            );
            assert!(
                only_driver.inbox.try_recv().is_err(),
                "the one live session must not absorb a dispatch nobody addressed to it"
            );
        }
    }

    // ── 記名 (§4.2) and the holder list (§4.3) ───────────────────────────

    /// The two devices §4.5 leaves standing once **A8** removed
    /// exclusivity, from the read end.
    mod kimei {
        use super::support::{body_json, test_state};
        use super::*;
        use mlua_swarm::blueprint::Blueprint;
        use mlua_swarm::store::run::{RunRecord, RunStatus};
        use mlua_swarm::store::task::{TaskRecord, TaskRecordStatus};
        use mlua_swarm::{BlueprintRef, RunId, TaskId};

        pub(super) const SEAT_A: &str = "phase-a-op";
        pub(super) const SEAT_B: &str = "phase-b-op";

        pub(super) async fn mint(state: &AppState, desc: Option<&str>) -> (SessionId, String) {
            let response = operators_create(
                State(state.clone()),
                Json(OperatorsCreateReq {
                    capability_manifest: None,
                    desc: desc.map(str::to_string),
                }),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK, "mint must succeed");
            let body = body_json(response).await;
            let sid = SessionId::parse(body["sid"].as_str().expect("sid").to_string())
                .expect("parse sid");
            let token = body["token"].as_str().expect("token").to_string();
            (sid, token)
        }

        /// A Blueprint declaring two Operator seats and nothing else worth
        /// resolving — enough for the holder list to enumerate them.
        fn two_seat_blueprint() -> Blueprint {
            serde_json::from_value(serde_json::json!({
                "schema_version": mlua_swarm::blueprint::current_schema_version(),
                "id": "kimei-two-seat-bp",
                "flow": {
                    "kind": "step",
                    "ref": "agent-a",
                    "in": { "op": "path", "at": "$.input" },
                    "out": { "op": "path", "at": "$.output" }
                },
                "agents": [
                    {
                        "name": "agent-a",
                        "kind": "operator",
                        "spec": { "operator_ref": SEAT_A },
                        "profile": { "worker_binding": "mse-worker" }
                    }
                ],
                "operators": [{ "name": SEAT_A }, { "name": SEAT_B }],
                "strategy": { "strict_refs": false }
            }))
            .expect("test Blueprint literal")
        }

        /// A Task carrying a goal and Task-level paths, plus its Run.
        pub(super) async fn seed_task_and_run(state: &AppState) -> (RunId, TaskId) {
            let task_id = TaskId::new();
            let run_id = RunId::new();
            state
                .task_store
                .create(TaskRecord {
                    id: task_id.clone(),
                    goal: "resolve issue #10".to_string(),
                    blueprint_ref: serde_json::to_value(BlueprintRef::Inline {
                        value: Box::new(two_seat_blueprint()),
                    })
                    .expect("encode blueprint_ref"),
                    input_ctx: serde_json::json!({}),
                    task_input_spec: Some(serde_json::json!({
                        "project_root": "/repo",
                        "work_dir": "/repo/.worktrees/topic",
                        "task_metadata": {"issue": 10},
                    })),
                    status: TaskRecordStatus::Running,
                    created_at: 0,
                    updated_at: 0,
                })
                .await
                .expect("seed task");
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
                    created_at: 0,
                    updated_at: 0,
                })
                .await
                .expect("seed run");
            (run_id, task_id)
        }

        /// **D2**: what the server could actually read at the moment of the
        /// `Assign` lands on the assigned session.
        #[tokio::test]
        async fn an_assign_writes_what_the_task_row_says_onto_the_holders_kimei() {
            let state = test_state();
            let (sid, _token) = mint(&state, Some("seating lane A")).await;
            let (run_id, task_id) = seed_task_and_run(&state).await;

            // Addressed by the sid, which is what every `Assign` records.
            crate::handover::record_observed_assignment(
                &state,
                &run_id,
                &task_id,
                SEAT_A,
                sid.as_str(),
            )
            .await;

            let live = state
                .operator_sessions
                .lock()
                .await
                .get(&sid)
                .cloned()
                .expect("the minted session");
            let record = live.kimei().await;
            assert_eq!(record.observed.len(), 1);
            assert_eq!(record.observed_total, 1);
            let entry = &record.observed[0];
            assert_eq!(entry.run_id, run_id.to_string());
            assert_eq!(entry.slot, SEAT_A);
            assert_eq!(entry.goal.as_deref(), Some("resolve issue #10"));
            assert_eq!(entry.project_root.as_deref(), Some("/repo"));
            assert_eq!(entry.work_dir.as_deref(), Some("/repo/.worktrees/topic"));
            assert_eq!(entry.task_metadata, Some(serde_json::json!({"issue": 10})));
            assert!(!entry.task_metadata_omitted);

            // And it is durable: the store holds the same log.
            let persisted = state
                .operator_session_store
                .list()
                .await
                .expect("list the store");
            let row = persisted
                .iter()
                .find(|r| r.sid == sid)
                .expect("the session's row");
            assert_eq!(row.observed, record.observed);
            assert_eq!(row.desc.as_deref(), Some("seating lane A"));
        }

        /// An `op` no live session answers to writes nothing and fails
        /// nothing — **Q2**, a seat can be taken for a role nobody holds.
        #[tokio::test]
        async fn an_assign_to_nobody_records_nothing_and_does_not_fail() {
            let state = test_state();
            let (run_id, task_id) = seed_task_and_run(&state).await;
            crate::handover::record_observed_assignment(
                &state,
                &run_id,
                &task_id,
                SEAT_A,
                "nobody-holds-this",
            )
            .await;
            assert!(state
                .operator_session_store
                .list()
                .await
                .expect("list")
                .is_empty());
        }

        /// A session that joined long ago and has never held a seat — the
        /// old end of **D5**'s ordering.
        ///
        /// Seeded rather than minted so its `joined_at_secs` is *strictly*
        /// older than the busy session's. Minting both lands them in the
        /// same wall-clock second, and `last_activity_secs` counts in
        /// seconds, so the comparison would fall through to the sid
        /// tie-break — which `uid_hex` (a random per-process salt XOR a
        /// counter) makes a coin flip. The assertion below is about
        /// activity order; it must not be decided by which random sid came
        /// out smaller.
        async fn seed_idle_session(state: &AppState, desc: &str) -> SessionId {
            let sid = SessionId::new();
            let live = LoginSession::new(
                OperatorSessionRecord {
                    sid: sid.clone(),
                    token_digest: OperatorSessionRecord::digest_of("idle-token"),
                    capability_manifest: None,
                    // Ten seconds older, not a literal `0`: the horizon deletes
                    // a session 24h past its last access, and this one has
                    // to be idle in D5's sense (holding no seat) while
                    // still being live.
                    joined_at_secs: now_secs() - 10,
                    last_access_secs: now_secs() - 10,
                    desc: Some(desc.to_string()),
                    observed: Vec::new(),
                    observed_total: 0,
                },
                None,
                None,
            );
            state
                .operator_sessions
                .lock()
                .await
                .insert(sid.clone(), live);
            sid
        }

        /// **D5**: newest activity first, and the page has a limit.
        #[tokio::test]
        async fn the_list_is_ordered_by_last_activity_and_capped() {
            let state = test_state();
            let idle_sid = seed_idle_session(&state, "has done nothing yet").await;
            let (busy_sid, token) = mint(&state, Some("seating lane A")).await;
            let (run_id, task_id) = seed_task_and_run(&state).await;
            crate::handover::record_observed_assignment(
                &state,
                &run_id,
                &task_id,
                SEAT_A,
                busy_sid.as_str(),
            )
            .await;

            let body = body_json(
                operators_list(
                    State(state.clone()),
                    headers_with_bearer(&token),
                    axum::extract::Query(OperatorsListQuery { limit: None }),
                )
                .await,
            )
            .await;
            let ops = body["operators"].as_array().expect("operators");
            assert_eq!(ops.len(), 2);
            assert_eq!(
                ops[0]["sid"].as_str().expect("sid"),
                busy_sid.to_string(),
                "the session that just took a seat sorts first"
            );
            assert_eq!(ops[1]["sid"].as_str().expect("sid"), idle_sid.to_string());
            assert_eq!(ops[0]["observed_total"], 1);
            assert_eq!(body["total"], 2);
            assert_eq!(body["limit"], OPERATORS_LIST_DEFAULT_LIMIT);

            // The limit cuts the page and `total` still reports the whole.
            let body = body_json(
                operators_list(
                    State(state.clone()),
                    headers_with_bearer(&token),
                    axum::extract::Query(OperatorsListQuery { limit: Some(1) }),
                )
                .await,
            )
            .await;
            assert_eq!(body["operators"].as_array().expect("operators").len(), 1);
            assert_eq!(body["total"], 2);
            assert_eq!(body["limit"], 1);

            // And a caller cannot opt out of having one.
            let body = body_json(
                operators_list(
                    State(state.clone()),
                    headers_with_bearer(&token),
                    axum::extract::Query(OperatorsListQuery {
                        limit: Some(usize::MAX),
                    }),
                )
                .await,
            )
            .await;
            assert_eq!(body["limit"], OPERATORS_LIST_MAX_LIMIT);
        }

        /// **D3**: no token, or one no session answers to, is a `401`.
        #[tokio::test]
        async fn the_list_refuses_without_a_live_session_bearer() {
            let state = test_state();
            let (_sid, _token) = mint(&state, None).await;

            let anonymous = operators_list(
                State(state.clone()),
                HeaderMap::new(),
                axum::extract::Query(OperatorsListQuery { limit: None }),
            )
            .await;
            assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);

            let wrong = operators_list(
                State(state.clone()),
                headers_with_bearer("not-a-minted-token"),
                axum::extract::Query(OperatorsListQuery { limit: None }),
            )
            .await;
            assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);
        }

        /// §4.3 from the Run's end: the declared seat nobody holds is in
        /// the list, saying so.
        #[tokio::test]
        async fn the_holder_list_reports_a_declared_but_unheld_seat_as_vacant() {
            let state = test_state();
            let (sid, token) = mint(&state, Some("seating lane A")).await;
            let (run_id, _task_id) = seed_task_and_run(&state).await;
            state
                .run_store
                .acquire_assignee(&run_id, SEAT_A, sid.as_str(), "took lane A")
                .await
                .expect("seat lane A");

            let response = crate::handover::run_assignees(
                State(state.clone()),
                axum::extract::Path(run_id.to_string()),
                headers_with_bearer(&token),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = body_json(response).await;

            assert_eq!(body["seats_source"], "blueprint");
            assert!(body["note"].is_null());
            assert_eq!(body["generation"], 1);
            let seats = body["seats"].as_array().expect("seats");
            assert_eq!(seats.len(), 2, "both declared seats are listed");
            assert_eq!(seats[0]["slot"], SEAT_A);
            assert_eq!(seats[0]["vacant"], false);
            assert_eq!(seats[0]["holder"]["op"], sid.to_string());
            assert_eq!(seats[0]["holder"]["gen"], 1);
            assert_eq!(seats[1]["slot"], SEAT_B);
            assert_eq!(seats[1]["vacant"], true);
            assert!(
                seats[1]["holder"].is_null(),
                "an unheld seat says so with a null holder rather than by not appearing: {body}"
            );
        }

        /// **D3** again, on the holder list — and note that the *acquire*
        /// on the same Run stays unguarded (**B2**).
        #[tokio::test]
        async fn the_holder_list_refuses_without_a_bearer() {
            let state = test_state();
            let (run_id, _task_id) = seed_task_and_run(&state).await;
            let response = crate::handover::run_assignees(
                State(state.clone()),
                axum::extract::Path(run_id.to_string()),
                HeaderMap::new(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }

        /// A Run holding nothing still answers with its seats rather than
        /// with an empty object — the `skip_serializing_if` objection, in
        /// its per-seat form.
        #[tokio::test]
        async fn a_run_holding_nothing_still_names_its_seats() {
            let state = test_state();
            let (_sid, token) = mint(&state, None).await;
            let (run_id, _task_id) = seed_task_and_run(&state).await;

            let body = body_json(
                crate::handover::run_assignees(
                    State(state.clone()),
                    axum::extract::Path(run_id.to_string()),
                    headers_with_bearer(&token),
                )
                .await,
            )
            .await;
            let seats = body["seats"].as_array().expect("seats");
            assert_eq!(seats.len(), 2);
            assert!(seats.iter().all(|s| s["vacant"] == true));
            assert_eq!(body["generation"], 0);
        }

        /// And the Run row itself now serializes an empty `current` rather
        /// than dropping the key.
        #[test]
        fn an_empty_current_is_written_out_not_skipped() {
            let record = RunRecord {
                id: RunId::new(),
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
            };
            let value = serde_json::to_value(&record).expect("serialize");
            assert_eq!(
                value["current"],
                serde_json::json!({}),
                "an empty holder map must reach the wire as {{}}: {value}"
            );
        }
    }

    // ── W5's four axes (§4.6) ────────────────────────────────────────────

    /// `GET /v1/runs/:id/handover` and `GET /v1/runs/:id/material` — what
    /// **W5** says an Assignee must be able to read *"前任者の有無に
    /// 関わらず"*, and what **W1** / **W2** / **W3** / **R5** say must not
    /// appear alongside it.
    mod w5_four_axes {
        use super::kimei::{mint, seed_task_and_run, SEAT_A, SEAT_B};
        use super::support::{body_json, test_state};
        use super::*;
        use crate::operator_ws::router::{Liveness, OperatorAdapter, PendingRequest};
        use crate::operator_ws::PendingKind;
        use mlua_swarm::core::state::SubmitOutcome;
        use mlua_swarm::{RunId, StepId};

        /// The step ids the waiting requests are addressed at.
        const STEP_ONE: &str = "ST-scout";
        const STEP_TWO: &str = "ST-drafter";

        /// An adapter that owes a fixed set of replies. It stands in for a
        /// `WSOperatorSession` whose `pending` map is non-empty — the map
        /// itself is exercised in `session`'s own tests; what is under test
        /// here is what the read surface does with the answer.
        struct OwesReplies {
            requests: Vec<PendingRequest>,
        }

        #[async_trait::async_trait]
        impl mlua_swarm::Operator for OwesReplies {
            async fn execute(
                &self,
                _ctx: &mlua_swarm::Ctx,
                _system: Option<String>,
                _prompt: serde_json::Value,
                _worker: Option<mlua_swarm::WorkerBinding>,
                _worker_token: mlua_swarm::CapToken,
            ) -> Result<mlua_swarm::WorkerResult, mlua_swarm::WorkerError> {
                Err(mlua_swarm::WorkerError::Failed(
                    "this double exists to be read, never dispatched to".to_string(),
                ))
            }
        }

        #[async_trait::async_trait]
        impl OperatorAdapter for OwesReplies {
            async fn liveness(&self) -> Liveness {
                Liveness::Connected
            }

            async fn discard_requests(&self, _run: &RunId, req_ids: &[String]) -> usize {
                req_ids.len()
            }

            async fn pending_for_run(&self, _run: &RunId) -> Vec<PendingRequest> {
                self.requests.clone()
            }
        }

        fn waiting(kind: PendingKind, req_id: &str, step: &str, attempt: u32) -> PendingRequest {
            PendingRequest {
                req_id: req_id.to_string(),
                kind,
                step_id: StepId::parse(step).expect("test step id literal"),
                attempt: Some(attempt),
            }
        }

        /// Register `op` as an adapter owing `requests`.
        async fn seat_owes(state: &AppState, op: &str, requests: Vec<PendingRequest>) {
            state
                .operator_adapters
                .register(op, Arc::new(OwesReplies { requests }))
                .await;
        }

        /// Register one adapter owing `requests` — the shape a Run reaches
        /// when a launch seats the same operator in several of its lanes,
        /// which is every multi-seat launch.
        async fn one_adapter_seats(state: &AppState, op: &str, requests: Vec<PendingRequest>) {
            state
                .operator_adapters
                .register(op, Arc::new(OwesReplies { requests }))
                .await;
        }

        async fn handover(state: &AppState, run_id: &RunId, token: &str) -> serde_json::Value {
            let response = crate::handover::run_handover(
                State(state.clone()),
                Path(run_id.to_string()),
                headers_with_bearer(token),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            body_json(response).await
        }

        /// Axis 3 with axis 2 joined onto it: the adapter supplies the
        /// request, the seat ledger says which seat it went out through,
        /// and `Run.current` supplies the `op` and the `generation` neither
        /// the wire nor the `pending` map can carry (**T1**).
        ///
        /// The `hook_before` alongside it is the case with no seat: it is
        /// dispatched through the sid-registered hook, never through a
        /// router, so nothing recorded a seat for it and none is invented.
        #[tokio::test]
        async fn the_unanswered_list_joins_the_holder_onto_each_waiting_request() {
            let state = test_state();
            let (sid, token) = mint(&state, Some("seating lane A")).await;
            let (run_id, _task_id) = seed_task_and_run(&state).await;
            state
                .run_store
                .acquire_assignee(&run_id, SEAT_A, sid.as_str(), "took lane A")
                .await
                .expect("seat lane A");
            seat_owes(
                &state,
                sid.as_str(),
                vec![
                    waiting(PendingKind::Spawn, "S-1-spawn-aaa", STEP_ONE, 1),
                    waiting(PendingKind::HookBefore, "S-1-hb-bbb", STEP_TWO, 2),
                ],
            )
            .await;
            // The spawn went out through lane A; the router's record of
            // that is what the join reads.
            let _seat = state.seat_ledger.record(
                &run_id,
                &StepId::parse(STEP_ONE).expect("step id"),
                1,
                SEAT_A,
                1,
            );

            let body = handover(&state, &run_id, &token).await;

            // Axis 2 rides along, from the same RunRecord read.
            let seats = body["seats"].as_array().expect("seats");
            assert_eq!(seats.len(), 2);
            assert_eq!(seats[0]["slot"], SEAT_A);
            assert_eq!(seats[0]["holder"]["op"], sid.to_string());
            assert_eq!(seats[1]["slot"], SEAT_B);
            assert_eq!(seats[1]["vacant"], true);

            // Axis 3. Seat-attributed rows first, then the ones that
            // belong to no seat.
            let waiting = body["unanswered"].as_array().expect("unanswered");
            assert_eq!(waiting.len(), 2);
            assert_eq!(waiting[0]["req_id"], "S-1-spawn-aaa");
            assert_eq!(waiting[0]["kind"], "spawn");
            assert_eq!(waiting[0]["step_id"], STEP_ONE);
            assert_eq!(waiting[0]["attempt"], 1);
            assert_eq!(waiting[0]["slot"], SEAT_A);
            assert_eq!(
                waiting[0]["op"],
                sid.to_string(),
                "the OperatorId is joined from the seat, not read from below the SAP"
            );
            assert_eq!(
                waiting[0]["generation"], 1,
                "and so is the generation, for the same reason"
            );

            assert_eq!(waiting[1]["kind"], "hook_before");
            assert_eq!(waiting[1]["attempt"], 2);
            assert!(
                waiting[1]["slot"].is_null()
                    && waiting[1]["op"].is_null()
                    && waiting[1]["generation"].is_null(),
                "a hook_before never reaches a router, so no seat can be named for it — and \
                 naming the seat that happened to answer would be a guess: {}",
                waiting[1]
            );

            for entry in waiting {
                assert!(entry["material_route"]
                    .as_str()
                    .expect("a material route")
                    .starts_with(&format!("/v1/runs/{run_id}/material?step_id=")));
            }

            // Axis 4's first half: neither attempt has produced anything.
            assert_eq!(waiting[0]["final_present"], false);
            assert!(waiting[0]["final_ok"].is_null());

            // Axis 1 is a reference, and an empty rail says so with `null`
            // rather than by pretending to a seq.
            assert_eq!(body["trace"]["route"], format!("/v1/runs/{run_id}/trace"));
            assert!(body["trace"]["latest_seq"].is_null());
            assert!(body["unread_seats"].as_array().expect("unread").is_empty());
        }

        /// **Each waiting request appears once.** Two seats of one Run
        /// resolve to the same adapter whenever one operator holds both —
        /// which every multi-seat launch produces, since the launching
        /// operator takes each declared lane. Asking per seat asked that
        /// one object twice and stamped the two copies with two different
        /// `slot` / `op` / `generation` triples, at most one of which was
        /// true.
        #[tokio::test]
        async fn a_request_owed_through_two_seats_is_listed_once() {
            let state = test_state();
            let (sid, token) = mint(&state, Some("driving both lanes")).await;
            let (run_id, _task_id) = seed_task_and_run(&state).await;
            for seat in [SEAT_A, SEAT_B] {
                state
                    .run_store
                    .acquire_assignee(&run_id, seat, sid.as_str(), "one driver, both lanes")
                    .await
                    .expect("seat");
            }
            one_adapter_seats(
                &state,
                sid.as_str(),
                vec![
                    waiting(PendingKind::Spawn, "S-1-spawn-aaa", STEP_ONE, 1),
                    waiting(PendingKind::Spawn, "S-1-spawn-bbb", STEP_TWO, 1),
                ],
            )
            .await;
            let _on_a = state.seat_ledger.record(
                &run_id,
                &StepId::parse(STEP_ONE).expect("step id"),
                1,
                SEAT_A,
                1,
            );
            let _on_b = state.seat_ledger.record(
                &run_id,
                &StepId::parse(STEP_TWO).expect("step id"),
                1,
                SEAT_B,
                2,
            );

            let body = handover(&state, &run_id, &token).await;
            let waiting = body["unanswered"].as_array().expect("unanswered");

            assert_eq!(
                waiting.len(),
                2,
                "two requests are outstanding, and one adapter backing two seats must not \
                 turn them into four: {waiting:?}"
            );
            let mut named: Vec<(&str, &str)> = waiting
                .iter()
                .map(|entry| {
                    (
                        entry["req_id"].as_str().expect("req_id"),
                        entry["slot"].as_str().expect("an attributed seat"),
                    )
                })
                .collect();
            named.sort_unstable();
            assert_eq!(
                named,
                vec![("S-1-spawn-aaa", SEAT_A), ("S-1-spawn-bbb", SEAT_B)],
                "and each is attributed to the seat it was actually dispatched through"
            );
        }

        /// **W3** / **R5** / **W2**, as a shape assertion: an un-answered
        /// entry has no field that could grade the wait. If one is ever
        /// added — `parked`, `sent_at`, `age_ms`, `stuck`, `resumable` —
        /// this fails, which is the point.
        #[tokio::test]
        async fn an_unanswered_entry_carries_nothing_that_grades_the_wait() {
            let state = test_state();
            let (sid, token) = mint(&state, None).await;
            let (run_id, _task_id) = seed_task_and_run(&state).await;
            state
                .run_store
                .acquire_assignee(&run_id, SEAT_A, sid.as_str(), "took lane A")
                .await
                .expect("seat lane A");
            seat_owes(
                &state,
                sid.as_str(),
                vec![waiting(PendingKind::Spawn, "S-1-spawn-aaa", STEP_ONE, 1)],
            )
            .await;

            let body = handover(&state, &run_id, &token).await;
            let entry = body["unanswered"][0].as_object().expect("one entry");
            let mut keys: Vec<&str> = entry.keys().map(String::as_str).collect();
            keys.sort_unstable();
            assert_eq!(
                keys,
                vec![
                    "attempt",
                    "final_ok",
                    "final_present",
                    "generation",
                    "kind",
                    "material_route",
                    "op",
                    "req_id",
                    "slot",
                    "step_id",
                ],
                "a waiting step is waiting, not broken: no age, no deadline, and no \
                 sent/unsent split (W3 / R5)"
            );
        }

        /// Axis 4's first half against a real tail: the attempt already
        /// produced a `Final`, so re-running it would double the side
        /// effect (`model.md:378-379`).
        #[tokio::test]
        async fn an_attempt_that_already_has_a_final_says_so_without_the_value() {
            let state = test_state();
            let (sid, token) = mint(&state, None).await;
            let (run_id, _task_id) = seed_task_and_run(&state).await;
            state
                .run_store
                .acquire_assignee(&run_id, SEAT_A, sid.as_str(), "took lane A")
                .await
                .expect("seat lane A");
            seat_owes(
                &state,
                sid.as_str(),
                vec![waiting(PendingKind::Spawn, "S-1-spawn-aaa", STEP_ONE, 1)],
            )
            .await;
            state
                .engine
                .submit_worker_result_trusted(
                    &StepId::parse(STEP_ONE).expect("step id"),
                    1,
                    serde_json::json!({ "verdict": "pass" }),
                    SubmitOutcome::Pass,
                )
                .await
                .expect("submit a Final for the attempt that is still awaited");

            let entry = &handover(&state, &run_id, &token).await["unanswered"][0];
            assert_eq!(entry["final_present"], true);
            assert_eq!(entry["final_ok"], true, "the flag the dispatch path reads");
            assert!(
                entry.get("final_value").is_none() && entry.get("final_content").is_none(),
                "presence and the ok flag decide the next action; the body does not: {entry}"
            );
            // And it is still listed as un-answered — a `Final` on the tail
            // is not an ack, and the server does not close the request on
            // the operator's behalf (**W1**).
            assert_eq!(entry["req_id"], "S-1-spawn-aaa");
        }

        /// A held seat whose holder names no registered adapter is named,
        /// not silently dropped: an `unanswered` that quietly omitted it
        /// would read as "nothing is in flight", which is the answer that
        /// invites a re-run.
        #[tokio::test]
        async fn a_held_seat_with_no_adapter_is_reported_rather_than_skipped() {
            let state = test_state();
            let (_sid, token) = mint(&state, None).await;
            let (run_id, _task_id) = seed_task_and_run(&state).await;
            state
                .run_store
                .acquire_assignee(
                    &run_id,
                    SEAT_A,
                    "an-operator-that-never-registered",
                    "took it",
                )
                .await
                .expect("seat lane A");

            let body = handover(&state, &run_id, &token).await;
            assert!(body["unanswered"]
                .as_array()
                .expect("unanswered")
                .is_empty());
            let unread = body["unread_seats"].as_array().expect("unread");
            assert_eq!(unread.len(), 1);
            assert_eq!(unread[0]["slot"], SEAT_A);
            assert_eq!(unread[0]["op"], "an-operator-that-never-registered");
            assert!(unread[0]["reason"]
                .as_str()
                .expect("a reason")
                .contains("not registered in the adapter registry"));
        }

        /// A vacant seat is not an unread one — there is nobody to ask, and
        /// the seat list on the same response already says so.
        #[tokio::test]
        async fn a_vacant_seat_is_not_reported_as_unread() {
            let state = test_state();
            let (_sid, token) = mint(&state, None).await;
            let (run_id, _task_id) = seed_task_and_run(&state).await;

            let body = handover(&state, &run_id, &token).await;
            assert!(body["unanswered"]
                .as_array()
                .expect("unanswered")
                .is_empty());
            assert!(body["unread_seats"].as_array().expect("unread").is_empty());
            assert!(body["seats"]
                .as_array()
                .expect("seats")
                .iter()
                .all(|seat| seat["vacant"] == true));
        }

        /// **D3** / **W5**: the reader is an Assignee, which is someone who
        /// has joined.
        #[tokio::test]
        async fn both_reads_refuse_without_a_bearer() {
            let state = test_state();
            let (run_id, _task_id) = seed_task_and_run(&state).await;

            let snapshot = crate::handover::run_handover(
                State(state.clone()),
                Path(run_id.to_string()),
                HeaderMap::new(),
            )
            .await;
            assert_eq!(snapshot.status(), StatusCode::UNAUTHORIZED);

            let material = crate::handover::run_step_material(
                State(state.clone()),
                Path(run_id.to_string()),
                axum::extract::Query(crate::handover::StepMaterialQuery {
                    step_id: StepId::parse(STEP_ONE).expect("step id"),
                }),
                HeaderMap::new(),
            )
            .await;
            assert_eq!(material.status(), StatusCode::UNAUTHORIZED);
        }

        /// A step the engine never dispatched has no material, and that is
        /// a miss rather than an empty payload.
        #[tokio::test]
        async fn material_for_a_step_the_engine_does_not_know_is_a_miss() {
            let state = test_state();
            let (_sid, token) = mint(&state, None).await;
            let (run_id, _task_id) = seed_task_and_run(&state).await;

            let response = crate::handover::run_step_material(
                State(state.clone()),
                Path(run_id.to_string()),
                axum::extract::Query(crate::handover::StepMaterialQuery {
                    step_id: StepId::parse(STEP_ONE).expect("step id"),
                }),
                headers_with_bearer(&token),
            )
            .await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }
    }

    // ── the 24h horizon ──────────────────────────────────────────────────

    /// §4.1's second exit from `Registered` — `最終アクセスから 24h ──▶ ╳
    /// 削除`, which carries no predicate number (the `O1` these tests used
    /// to cite is `join は無認証`) — and which had no implementation at
    /// all: `DELETE
    /// /v1/operators/:sid` wants the session's own bearer, and a crashed
    /// driver lost it, so the row it left behind could never be removed by
    /// anybody.
    ///
    /// The rule is applied at every read of a session, and — for a session
    /// no read reaches — by the `operator-session-expiry` periodic job. The
    /// last three tests here are that job's, and they are about the same
    /// predicate and the same teardown arriving without a caller.
    mod o1_expiry {
        use super::kimei::mint;
        use super::support::{body_json, test_state};
        use super::*;
        use mlua_swarm::store::operator_session::{
            OperatorSessionStore, OPERATOR_SESSION_MAX_IDLE_SECS,
        };

        /// Put a session into the map with its access clock placed
        /// explicitly — the state a driver that crashed `idle_secs` ago
        /// leaves behind. Registered as well as published, so the reap can
        /// be observed to undo both.
        async fn seed_session_idle_for(
            state: &AppState,
            desc: &str,
            idle_secs: u64,
        ) -> (SessionId, Arc<LoginSession>) {
            let sid = SessionId::new();
            let accessed_at = now_secs() - idle_secs;
            let record = OperatorSessionRecord {
                sid: sid.clone(),
                token_digest: OperatorSessionRecord::digest_of("stale-bearer"),
                capability_manifest: None,
                joined_at_secs: accessed_at,
                last_access_secs: accessed_at,
                desc: Some(desc.to_string()),
                observed: Vec::new(),
                observed_total: 0,
            };
            state
                .operator_session_store
                .put(record.clone())
                .await
                .expect("seed the persisted row");
            let live = LoginSession::new(record, None, None);
            register_operator_session(
                &state.engine,
                Some(&state.operator_adapters),
                &sid,
                live.dispatch_target(),
            )
            .await;
            state
                .operator_sessions
                .lock()
                .await
                .insert(sid.clone(), live.clone());
            (sid, live)
        }

        /// The accumulation this closes: one row per crashed driver, on a
        /// list with a count ceiling (50 by default), pushing live sessions
        /// off the page a recovery driver reads.
        #[tokio::test]
        async fn the_list_releases_a_session_that_has_not_been_accessed_for_a_day() {
            let state = test_state();
            let (stale_sid, _stale) = seed_session_idle_for(
                &state,
                "crashed halfway through the seat rework",
                OPERATOR_SESSION_MAX_IDLE_SECS + 60,
            )
            .await;
            let (live_sid, token) = mint(&state, Some("the driver reading this list")).await;

            let body = body_json(
                operators_list(
                    State(state.clone()),
                    headers_with_bearer(&token),
                    axum::extract::Query(OperatorsListQuery { limit: None }),
                )
                .await,
            )
            .await;

            let sids: Vec<&str> = body["operators"]
                .as_array()
                .expect("operators")
                .iter()
                .map(|e| e["sid"].as_str().expect("sid"))
                .collect();
            assert_eq!(
                sids,
                vec![live_sid.to_string().as_str()],
                "the expired session must be gone from the list, not merely sorted last"
            );
            assert_eq!(
                body["total"], 1,
                "and gone from the count as well: the row was released, not hidden"
            );

            // Released by the same teardown a leave performs, on every axis.
            assert!(
                !state
                    .operator_sessions
                    .lock()
                    .await
                    .contains_key(&stale_sid),
                "the expired session must leave the map"
            );
            let registered = state.engine.list_operator_ids().await;
            assert!(
                !registered.contains(&stale_sid.to_string()),
                "and the engine registries: {registered:?}"
            );
            // Through `get`, not `list`: `list` filters the expired
            // unconditionally, so their absence from it is true of a
            // backend that never deleted anything. What has to be true
            // here is that the row left the file.
            assert!(
                state
                    .operator_session_store
                    .get(&stale_sid)
                    .await
                    .expect("store get")
                    .is_none(),
                "and the persisted row, which is what stops it coming back at the next boot"
            );
        }

        /// A session inside the horizon is left alone. Without this the
        /// assertion above would also pass on a reaper that deleted
        /// everything it read.
        #[tokio::test]
        async fn a_session_accessed_within_the_horizon_survives_the_list() {
            let state = test_state();
            let (recent_sid, _recent) = seed_session_idle_for(
                &state,
                "quiet, but not for a day",
                OPERATOR_SESSION_MAX_IDLE_SECS - 600,
            )
            .await;
            let (_live_sid, token) = mint(&state, None).await;

            let body = body_json(
                operators_list(
                    State(state.clone()),
                    headers_with_bearer(&token),
                    axum::extract::Query(OperatorsListQuery { limit: None }),
                )
                .await,
            )
            .await;
            assert_eq!(
                body["total"], 2,
                "a session inside the horizon stays: {body}"
            );
            assert!(state
                .operator_sessions
                .lock()
                .await
                .contains_key(&recent_sid));
        }

        /// **O7 is not violated by the connectivity clause.** A driver that
        /// attached a socket and then held a Blueprint open all day is
        /// present — the socket *is* contact — so expiring it would be the
        /// reaper causing the outage it exists to prevent.
        #[tokio::test]
        async fn a_connected_session_is_never_expired_however_quiet() {
            let state = test_state();
            let (sid, live) = seed_session_idle_for(
                &state,
                "attached and quiet",
                OPERATOR_SESSION_MAX_IDLE_SECS * 3,
            )
            .await;
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            live.dispatch_target().replace_tx(tx).await;

            let (_live_sid, token) = mint(&state, None).await;
            let body = body_json(
                operators_list(
                    State(state.clone()),
                    headers_with_bearer(&token),
                    axum::extract::Query(OperatorsListQuery { limit: None }),
                )
                .await,
            )
            .await;

            assert_eq!(body["total"], 2, "an attached socket is an access: {body}");
            assert!(state.operator_sessions.lock().await.contains_key(&sid));
        }

        /// The write path the expiry reads. Without it `last_access_secs`
        /// would never move off the join time, and every session would be
        /// deleted 24h after minting however hard its driver was working.
        #[tokio::test]
        async fn reading_a_session_is_an_access_and_moves_its_expiry() {
            let state = test_state();
            // Five seconds short of the horizon: still alive, and about to
            // cross it.
            let (sid, live) = seed_session_idle_for(
                &state,
                "about to expire",
                OPERATOR_SESSION_MAX_IDLE_SECS - 5,
            )
            .await;
            let horizon_without_a_touch = now_secs() + 10;
            assert!(
                live.is_expired(horizon_without_a_touch).await,
                "precondition: ten seconds from now this session is past the horizon"
            );

            let response = operators_info(
                State(state.clone()),
                Path(sid.to_string()),
                headers_with_bearer("stale-bearer"),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);

            assert!(
                !live.is_expired(horizon_without_a_touch).await,
                "the read must have restarted the expiry clock: the session was still here to \
                 answer, which is what 最終アクセス means"
            );
            // ...and durably, so a restart inside the next 24h does not
            // expire a session that is in use.
            let persisted = state
                .operator_session_store
                .list()
                .await
                .expect("store list")
                .into_iter()
                .find(|r| r.sid == sid)
                .expect("the row is still there");
            assert!(
                persisted.last_access_secs() >= now_secs() - 5,
                "the touch must be written through, or the next boot reads the old clock"
            );
        }

        /// Reading the 記名 list must not keep alive the very rows it is
        /// being read to find. Only the bearer's own session is touched.
        #[tokio::test]
        async fn listing_touches_only_the_reader_s_own_session() {
            let state = test_state();
            let (_stale_sid, stale) = seed_session_idle_for(
                &state,
                "somebody else's corpse",
                OPERATOR_SESSION_MAX_IDLE_SECS - 5,
            )
            .await;
            let (reader_sid, token) = mint(&state, Some("the reader")).await;
            let horizon = now_secs() + 10;

            let _ = operators_list(
                State(state.clone()),
                headers_with_bearer(&token),
                axum::extract::Query(OperatorsListQuery { limit: None }),
            )
            .await;

            assert!(
                stale.is_expired(horizon).await,
                "being enumerated is not being accessed: a recovery driver reading the \
                 list must not postpone the expiry of what it is reading about"
            );
            let reader = state
                .operator_sessions
                .lock()
                .await
                .get(&reader_sid)
                .cloned()
                .expect("the reader is live");
            assert!(
                !reader.is_expired(horizon).await,
                "the presenter of the bearer is demonstrably alive, so its own clock moves"
            );
        }

        /// **The route that used to revive what the other two destroyed.**
        ///
        /// `authorize_any_operator` found the presenter by bearer and
        /// touched it with no expiry judgment at all, so the reap loop in
        /// `operators_list` then ran against a clock reset one statement
        /// earlier and always found it fresh. The outcome for one and the
        /// same session therefore depended on which route its driver
        /// called first: `GET /v1/operators/:sid` tore it down and
        /// cascaded its seats `Vacant`, `GET /v1/operators` revived it —
        /// and this guard also covers `acquire`, which is the call a
        /// recovering driver is told to make.
        #[tokio::test]
        async fn presenting_an_expired_bearer_does_not_revive_the_session() {
            let state = test_state();
            let (stale_sid, _stale) = seed_session_idle_for(
                &state,
                "crashed a day and an hour ago",
                OPERATOR_SESSION_MAX_IDLE_SECS + 3600,
            )
            .await;

            let response = operators_list(
                State(state.clone()),
                headers_with_bearer("stale-bearer"),
                axum::extract::Query(OperatorsListQuery { limit: None }),
            )
            .await;

            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "an expired session is not a session: presenting its bearer must not open \
                 the list"
            );
            let body = body_json(response).await;
            assert!(
                body["hint"]
                    .as_str()
                    .expect("the 401 carries a hint")
                    .contains("POST /v1/operators"),
                "the hint must send the caller to a re-join rather than leaving it to read \
                 the 401 as a malformed token: {body}"
            );

            // Judged *before* the touch, so the presenter was reaped by
            // its own call rather than kept alive by it.
            assert!(
                !state
                    .operator_sessions
                    .lock()
                    .await
                    .contains_key(&stale_sid),
                "the expired presenter must leave the map, exactly as it does on the two \
                 routes that always judged first"
            );
            assert!(
                state
                    .operator_session_store
                    .get(&stale_sid)
                    .await
                    .expect("store get")
                    .is_none(),
                "and the persisted row with it"
            );

            // The single-session read agrees, which is the whole point:
            // one session, one outcome, whichever route asks.
            let info = operators_info(
                State(state.clone()),
                Path(stale_sid.to_string()),
                headers_with_bearer("stale-bearer"),
            )
            .await;
            assert_eq!(
                info.status(),
                StatusCode::NOT_FOUND,
                "the collection route must not have left the session readable"
            );
        }

        /// The other half of the pair above: a presenter inside the
        /// horizon still opens the list. Without this the fix would also
        /// be satisfied by refusing every bearer.
        #[tokio::test]
        async fn presenting_a_live_bearer_still_opens_the_list() {
            let state = test_state();
            let (recent_sid, _recent) = seed_session_idle_for(
                &state,
                "quiet, but not for a day",
                OPERATOR_SESSION_MAX_IDLE_SECS - 600,
            )
            .await;

            let response = operators_list(
                State(state.clone()),
                headers_with_bearer("stale-bearer"),
                axum::extract::Query(OperatorsListQuery { limit: None }),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            assert!(state
                .operator_sessions
                .lock()
                .await
                .contains_key(&recent_sid));
        }

        /// A boot that restored expired rows was the other half of the
        /// accumulation: `OperatorSessionPersistence::restore` reads the
        /// store once, so a row that survives that read is back for another
        /// restart. The store drops and deletes it instead.
        #[tokio::test]
        async fn the_store_neither_returns_nor_keeps_an_expired_row() {
            let store = mlua_swarm::store::operator_session::InMemoryOperatorSessionStore::new();
            let stale = SessionId::new();
            let fresh = SessionId::new();
            let record = |sid: &SessionId, accessed_at: u64| OperatorSessionRecord {
                sid: sid.clone(),
                token_digest: OperatorSessionRecord::digest_of("bearer"),
                capability_manifest: None,
                joined_at_secs: accessed_at,
                last_access_secs: accessed_at,
                desc: None,
                observed: Vec::new(),
                observed_total: 0,
            };
            store
                .put(record(
                    &stale,
                    now_secs() - OPERATOR_SESSION_MAX_IDLE_SECS - 1,
                ))
                .await
                .expect("put");
            store.put(record(&fresh, now_secs())).await.expect("put");

            let restored = store.list().await.expect("list");
            let sids: Vec<String> = restored.iter().map(|r| r.sid.to_string()).collect();
            assert_eq!(
                sids,
                vec![fresh.to_string()],
                "an expired row must not come back at boot"
            );

            // The distinction this test exists for, and the one a second
            // `list()` cannot make: `list` filters the expired every time
            // it runs, so a filter-only backend answers it identically.
            // `get` reports the row as stored.
            assert!(
                store.get(&stale).await.expect("get").is_none(),
                "and it must be gone from the store, not filtered on every read forever"
            );
            assert!(
                store.get(&fresh).await.expect("get").is_some(),
                "while the row inside the horizon stays — without this, a store that \
                 deleted everything it listed would pass the assertion above"
            );
        }

        // ── the same horizon, arriving on its own ────────────────────────

        /// What the reads above cannot do: release a session **nobody is
        /// reading**. Every assertion in this module so far had a caller —
        /// a list, an info, a bearer, a boot. A server whose driver crashed
        /// and whose operator is not looking has none of those, and the
        /// registrations that make a dispatch park on the corpse stay up
        /// until the teardown runs. That is what the
        /// `operator-session-expiry` periodic job supplies, and it supplies
        /// only that: same predicate, same teardown.
        #[tokio::test]
        async fn the_sweep_releases_a_session_no_read_ever_reaches() {
            let state = test_state();
            let (stale_sid, _stale) = seed_session_idle_for(
                &state,
                "crashed a week ago, and nobody has listed since",
                OPERATOR_SESSION_MAX_IDLE_SECS * 7,
            )
            .await;

            let released = sweep_expired_operator_sessions(&state).await;

            assert_eq!(released, 1, "the sweep reports what it released");
            assert!(
                !state
                    .operator_sessions
                    .lock()
                    .await
                    .contains_key(&stale_sid),
                "the session is gone from the map without anyone having read it"
            );
            assert!(
                state
                    .operator_session_store
                    .get(&stale_sid)
                    .await
                    .expect("store get")
                    .is_none(),
                "and so is the persisted row, so a restart does not bring it back"
            );
            assert!(
                !state
                    .engine
                    .list_operator_ids()
                    .await
                    .contains(&stale_sid.to_string()),
                "and the engine registration with it — that is the part a read-time \
                 expiry leaves standing, and what a dispatch would otherwise park on"
            );
        }

        /// The sweep is the horizon, not a reaper with its own opinion: a
        /// session inside the horizon and a connected-but-quiet one both
        /// survive it, exactly as they survive a list. Without this the
        /// test above would also pass on a job that emptied the map.
        #[tokio::test]
        async fn the_sweep_leaves_live_sessions_alone() {
            let state = test_state();
            let (recent_sid, _recent) = seed_session_idle_for(
                &state,
                "quiet, but not for a day",
                OPERATOR_SESSION_MAX_IDLE_SECS - 600,
            )
            .await;
            let (attached_sid, attached) = seed_session_idle_for(
                &state,
                "attached and quiet for a week",
                OPERATOR_SESSION_MAX_IDLE_SECS * 7,
            )
            .await;
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            attached.dispatch_target().replace_tx(tx).await;

            let released = sweep_expired_operator_sessions(&state).await;

            assert_eq!(released, 0, "neither session is past the horizon");
            let map = state.operator_sessions.lock().await;
            assert!(
                map.contains_key(&recent_sid),
                "a session inside the horizon is not the sweep's business"
            );
            assert!(
                map.contains_key(&attached_sid),
                "and an attached socket is contact, however quiet (O7 clause)"
            );
        }

        /// One session's teardown failing is not the run's failure. The
        /// sweep is a loop over independent items, so a store that refuses
        /// one delete leaves that session for the next pass and releases
        /// the rest — the same thing a read does when its teardown fails.
        #[tokio::test]
        async fn a_teardown_failure_does_not_stop_the_rest_of_the_sweep() {
            let mut state = test_state();
            let store = Arc::new(RefusingDeleteStore::default());
            state.operator_session_store = store.clone();
            let (refuses_sid, _a) = seed_session_idle_for(
                &state,
                "the one whose row will not delete",
                OPERATOR_SESSION_MAX_IDLE_SECS * 2,
            )
            .await;
            let (deletable_sid, _b) = seed_session_idle_for(
                &state,
                "the one behind it in the map",
                OPERATOR_SESSION_MAX_IDLE_SECS * 2,
            )
            .await;
            store.refuse(refuses_sid.clone());

            let released = sweep_expired_operator_sessions(&state).await;

            assert_eq!(
                released, 1,
                "the count reports releases, not attempts: only one teardown completed"
            );
            let map = state.operator_sessions.lock().await;
            assert!(
                map.contains_key(&refuses_sid),
                "a session whose teardown failed stays live and is judged again next pass"
            );
            assert!(
                !map.contains_key(&deletable_sid),
                "and the failure does not abort the sweep before its neighbours"
            );
        }

        /// In-memory store that refuses `delete` for the sids it was told
        /// to refuse, and is otherwise honest — which is what makes the
        /// test above about the sweep's loop rather than about a store
        /// that fails everything.
        #[derive(Default)]
        struct RefusingDeleteStore {
            inner: mlua_swarm::store::operator_session::InMemoryOperatorSessionStore,
            refuse: std::sync::Mutex<Vec<SessionId>>,
        }

        impl RefusingDeleteStore {
            fn refuse(&self, sid: SessionId) {
                self.refuse.lock().expect("refuse list").push(sid);
            }
        }

        #[async_trait::async_trait]
        impl mlua_swarm::store::operator_session::OperatorSessionStore for RefusingDeleteStore {
            fn name(&self) -> &str {
                "refusing-delete"
            }

            async fn put(
                &self,
                record: OperatorSessionRecord,
            ) -> Result<(), OperatorSessionStoreError> {
                self.inner.put(record).await
            }

            async fn delete(&self, sid: &SessionId) -> Result<(), OperatorSessionStoreError> {
                if self.refuse.lock().expect("refuse list").contains(sid) {
                    return Err(OperatorSessionStoreError::Other(
                        "injected delete failure".to_string(),
                    ));
                }
                self.inner.delete(sid).await
            }

            async fn get(
                &self,
                sid: &SessionId,
            ) -> Result<Option<OperatorSessionRecord>, OperatorSessionStoreError> {
                self.inner.get(sid).await
            }

            async fn list(&self) -> Result<Vec<OperatorSessionRecord>, OperatorSessionStoreError> {
                self.inner.list().await
            }
        }
    }

    // ── the keepalive behind "a connected session is never expired" ──────

    /// `LoginSession::is_expired` short-circuits on "a socket is
    /// attached", and `tx` is cleared in exactly one place: the read
    /// loop's exit. A peer that vanishes without a FIN ends nothing, so
    /// `ws_stream.next()` parked forever, `tx` stayed `Some`, and the row
    /// became immortal — unreapable (never expired) and undeletable (the
    /// bearer died with the peer). These tests are about the probe that
    /// closes it, and about the durable clock that rides along with it.
    mod keepalive {
        use super::support::test_state;
        use super::*;
        use mlua_swarm::store::operator_session::OPERATOR_SESSION_MAX_IDLE_SECS;
        use std::time::Duration;

        /// Fast enough to run in a test, wide enough that the tolerance is
        /// still several intervals — the same shape as
        /// [`KeepAlive::DEFAULT`], four orders of magnitude down.
        const TEST_KEEPALIVE: KeepAlive = KeepAlive {
            ping_every: Duration::from_millis(40),
            silence_before_dead: Duration::from_millis(200),
        };

        /// A session whose durable row says it was last accessed
        /// `idle_secs` ago, published into the store and returned with the
        /// live handle.
        async fn seed(state: &AppState, idle_secs: u64) -> (SessionId, Arc<LoginSession>) {
            let sid = SessionId::new();
            let accessed_at = now_secs() - idle_secs;
            let record = OperatorSessionRecord {
                sid: sid.clone(),
                token_digest: OperatorSessionRecord::digest_of("keepalive-bearer"),
                capability_manifest: None,
                joined_at_secs: accessed_at,
                last_access_secs: accessed_at,
                desc: Some("holding a socket open".to_string()),
                observed: Vec::new(),
                observed_total: 0,
            };
            state
                .operator_session_store
                .put(record.clone())
                .await
                .expect("seed the persisted row");
            let live = LoginSession::new(record, None, None);
            state
                .operator_sessions
                .lock()
                .await
                .insert(sid.clone(), live.clone());
            (sid, live)
        }

        /// Serve exactly one route that upgrades into
        /// [`handle_operator_socket`] with test-sized keepalive params, and
        /// answer with its `ws://` URL.
        ///
        /// A local `axum::serve` rather than the real router because the
        /// route hands [`KeepAlive::DEFAULT`] in, and a test that waited
        /// out five 30-second Pings would be a test nobody runs.
        async fn serve_one_socket(state: &AppState, live: Arc<LoginSession>) -> String {
            let store = state.operator_session_store.clone();
            let app = axum::Router::new().route(
                "/ws",
                axum::routing::get(move |ws: WebSocketUpgrade| {
                    let live = live.clone();
                    let store = store.clone();
                    async move {
                        ws.on_upgrade(move |socket| {
                            handle_operator_socket(socket, live, store, TEST_KEEPALIVE)
                        })
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind an ephemeral port");
            let addr = listener.local_addr().expect("local addr");
            tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            format!("ws://{addr}/ws")
        }

        /// Poll `is_connected` until it answers `want`, or give up after
        /// `within`. Returns what it last saw, so the assertion reads as a
        /// statement about the session rather than about the sleep.
        async fn connected_becomes(live: &Arc<LoginSession>, want: bool, within: Duration) -> bool {
            let deadline = tokio::time::Instant::now() + within;
            loop {
                let now = live.dispatch_target().is_connected().await;
                if now == want || tokio::time::Instant::now() >= deadline {
                    return now;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        /// A peer that answers stays attached however quiet it is — and
        /// its **durable** access clock moves, which is the half that was
        /// missing.
        ///
        /// Before the keepalive there was no periodic event on a connected
        /// socket at all, so `last_access_secs` on disk stopped at the
        /// connect. A driver attached and quiet for over a day therefore
        /// had a row the next boot deleted (the store's predicate is
        /// `is_expired_at` over the record alone; it knows nothing about
        /// sockets) — the guide claimed the opposite, and **R6**
        /// (再起動は担当を落とさない) is what it was against.
        #[tokio::test]
        async fn a_peer_that_answers_stays_attached_and_keeps_its_durable_clock_moving() {
            let state = test_state();
            // An hour idle: inside the horizon, and far enough behind that
            // a write-through is unmistakable (`touch` coalesces durable
            // writes to one per `TOUCH_PERSIST_INTERVAL_SECS`).
            let (sid, live) = seed(&state, 3600).await;
            let url = serve_one_socket(&state, live.clone()).await;

            let (socket, _resp) = tokio_tungstenite::connect_async(url)
                .await
                .expect("the client connects");
            // Driving the stream is what makes tungstenite emit the Pong
            // it queues on an inbound Ping — i.e. this is an ordinary,
            // conforming client.
            let (sink, mut source) = futures_util::StreamExt::split(socket);
            let pump = tokio::spawn(async move {
                while let Some(Ok(_)) = futures_util::StreamExt::next(&mut source).await {}
            });

            assert!(
                connected_becomes(&live, true, Duration::from_secs(2)).await,
                "precondition: the socket attached"
            );
            // Three full silence windows: a probe that judged wrongly has
            // had every chance to.
            tokio::time::sleep(TEST_KEEPALIVE.silence_before_dead * 3).await;

            assert!(
                live.dispatch_target().is_connected().await,
                "a peer that answers must stay attached, however quiet the protocol is"
            );
            let persisted = state
                .operator_session_store
                .get(&sid)
                .await
                .expect("store get")
                .expect("the row is still there");
            assert!(
                persisted.last_access_secs() >= now_secs() - 5,
                "the connected session's durable clock must move, or a boot inside the \
                 next 24h deletes the row out from under a live socket (R6); it still \
                 reads {} against a now of {}",
                persisted.last_access_secs(),
                now_secs()
            );

            pump.abort();
            drop(sink);
        }

        /// The immortal row, reproduced: a peer that stops answering
        /// without closing anything.
        ///
        /// The client below holds its socket open and never polls it, so
        /// it emits nothing — no Pong, no FIN, no error. That is what a
        /// vanished host looks like from the server end, and before the
        /// keepalive it left `tx` set forever, which made the session
        /// permanently un-expirable.
        #[tokio::test]
        async fn a_peer_that_stops_answering_is_detached_and_becomes_expirable_again() {
            let state = test_state();
            let (_sid, live) = seed(&state, 0).await;
            let url = serve_one_socket(&state, live.clone()).await;

            let socket = tokio_tungstenite::connect_async(url)
                .await
                .expect("the client connects")
                .0;

            // A `now` past any horizon this session could reach. While a
            // socket is attached the record's own predicate is not even
            // consulted, so this is the sharpest form of the
            // short-circuit: no future whatsoever expires an attached
            // session, which is what made a dead peer's row immortal.
            let past_every_horizon = now_secs() + OPERATOR_SESSION_MAX_IDLE_SECS * 2;

            assert!(
                connected_becomes(&live, true, Duration::from_secs(2)).await,
                "precondition: the socket attached"
            );
            assert!(
                !live.is_expired(past_every_horizon).await,
                "precondition: an attached socket is never expired at any now — which is \
                 exactly why a peer that dies with `tx` still set is unreachable by every \
                 reaper there is"
            );

            // From here the client is a corpse holding a file descriptor:
            // it is never polled again, so tungstenite never answers the
            // Pings, and nothing closes the connection.
            assert!(
                !connected_becomes(&live, false, Duration::from_secs(5)).await,
                "a socket whose peer answered nothing for {:?} must be detached",
                TEST_KEEPALIVE.silence_before_dead
            );
            assert!(
                live.is_expired(past_every_horizon).await,
                "and with `tx` cleared the record's own predicate decides again: the \
                 session is back under the 24h horizon instead of outside every horizon"
            );

            drop(socket);
        }
    }
}
