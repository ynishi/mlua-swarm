//! REST-like Operator session resource.
//!
//! Provides the `POST/GET/DELETE /v1/operators` + `WS /v1/operators/:sid/ws`
//! route family — the sole WS Operator session route. `session.rs` /
//! `protocol.rs` are unchanged by this module.
//!
//! ## Login flow
//!
//! ```text
//! POST /v1/operators { roles?: ["main-ai"], capability_manifest?: {...} }
//!   → 409 if any role already owns a live entry (roles alias exclusivity,
//!     v1.md §Auth session flow)
//!   → { sid: "S-<hex>", token: "<10-hex>", roles: [...] }
//!   → builds a disconnected `WSOperatorSession` and registers it into the
//!     engine's 3 registries (senior_bridge / spawn_hook / operator) +
//!     role aliases. The sid is therefore usable as an `operator_sid` pin
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
//!   → unregisters the 3 registries + role aliases + `operator_sessions`
//!     entry + releases `roles_to_sid` ownership, then vacates every Run
//!     seat still held under this sid or one of its roles (model O8 —
//!     `cascade_vacate_seats`; a delete leaves no holder behind).
//!
//! GET /v1/operators/:sid   (Bearer required)
//!   → { sid, roles, connected, desc, observed, observed_total }
//!
//! GET /v1/operators   (Bearer required — any live session's token)
//!   → the 記名 list (model §4.2): every live session's identity, its
//!     join-time description (D1) and the seats it has been assigned (D2),
//!     newest activity first with a count limit (D5).
//! ```
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
//! - `mlua_swarm::OperatorSession` — the engine-side `attach`/session-token
//!   record behind `/v1/sessions`. Unrelated to this route family.

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
use mlua_swarm::{
    AgentProviderManifest, Engine, Operator, OperatorRef, SeniorBridge, SessionId, SpawnHook,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

use super::protocol::{ClientMsg, PendingReply, ServerMsg};
use super::router::{OperatorAdapter, OperatorAdapterRegistry};
use super::session::WSOperatorSession;
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
    /// identity, the roles, the manifest, the mint time, and the 記名's
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
    pub(crate) fn new(record: OperatorSessionRecord, base_url: Option<Arc<str>>) -> Arc<Self> {
        let dispatch_target = Arc::new(WSOperatorSession::disconnected_with_base_url(
            record.sid.clone(),
            base_url,
        ));
        let observed = ObservedLog {
            entries: record.observed.clone(),
            total: record.observed_total,
        };
        Arc::new(Self {
            record,
            observed: tokio::sync::Mutex::new(observed),
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
        record
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
    /// Held inline rather than as a registry id: `mlua_swarm::OperatorSession`
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

// ─── POST /v1/operators (mint) ──────────────────────────────────────────────

/// Body for `POST /v1/operators`.
#[derive(Debug, Deserialize, Default)]
pub struct OperatorsCreateReq {
    /// Role aliases to claim exclusively (empty = no exclusivity claimed).
    ///
    /// # Why this is `String` when everything downstream is [`OperatorRef`]
    ///
    /// This is the untrusted wire-decode boundary, the same shape
    /// `Path(sid): Path<String>` has on the sibling routes: the request
    /// arrives as strings and [`operators_create`] validates them into
    /// `OperatorRef` itself, so a rejection is answered with this module's
    /// own `400` (and its own message naming the offending element)
    /// instead of axum's extractor-level `422`. Nothing past that
    /// conversion handles a role as a bare string.
    #[serde(default)]
    pub roles: Vec<String>,
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
    /// Echoes the granted role aliases (each serializes as the plain role
    /// string, unchanged from before the [`OperatorRef`] typing).
    pub roles: Vec<OperatorRef>,
}

/// `POST /v1/operators`. Mints `sid` (`S-<hex>` — the shared `SessionId`
/// shape; issue #11) + a 128-bit bearer token
/// (`mlua_swarm::types::operator_bearer_token` — OS-RNG hex, unguessable
/// across calls and restarts, which is the point: this token is the sole
/// bearer secret on the short-handle path). When `roles` is non-empty,
/// checks `AppState.roles_to_sid` for conflicts under a single lock (the
/// check and the insert are atomic w.r.t. concurrent mints) and returns
/// `409 CONFLICT` with the conflicting role names on collision. Empty
/// `roles` never conflicts (= no exclusivity is claimed).
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
    // Validate the wire strings into role handles before anything is
    // reserved, minted, or persisted. An empty element is the one thing
    // rejected here: it names no Operator, so a session claiming it could
    // never be routed to, and every failure it caused would surface far
    // from the caller that sent it.
    let roles: Vec<OperatorRef> = match req
        .roles
        .into_iter()
        .map(OperatorRef::new)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(roles) => roles,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": format!("invalid role alias: {error}"),
                    "hint": "omit `roles` (or send an empty array) to claim no alias at all",
                })),
            )
                .into_response();
        }
    };
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

    {
        let mut map = state.roles_to_sid.lock().await;
        let conflicts: Vec<OperatorRef> = roles
            .iter()
            .filter(|r| map.contains_key(r.as_str()))
            .cloned()
            .collect();
        if !conflicts.is_empty() {
            // GH #81 Layer 2 (a): identify the holding session per
            // conflicted role so a recovery driver knows which sid to
            // release without probing. The pre-#81 `conflicts: [role]`
            // array stays byte-identical for callers that already
            // ignore unknown keys; the new `conflicts_detail: [{role,
            // sid}]` array is an additive companion.
            let conflicts_detail: Vec<serde_json::Value> = conflicts
                .iter()
                .map(|r| {
                    let holder = map.get(r.as_str()).map(|sid| sid.to_string());
                    json!({ "role": r, "sid": holder })
                })
                .collect();
            return (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": "roles conflict",
                    "conflicts": conflicts,
                    "conflicts_detail": conflicts_detail,
                })),
            )
                .into_response();
        }
        for r in &roles {
            map.insert(r.clone(), sid.clone());
        }
    }

    // A clock that cannot answer yields `0`, which `GET /v1/operators`
    // reads as the oldest possible mint — so such a session is the one a
    // recovery driver picking "the oldest stale session" always picks,
    // permanently.
    let joined_at_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    // Write-through BEFORE the in-memory insert and the mint response: a
    // sid the client can see must already be durable, or a crash between
    // response and persist would resurrect the pre-persistence forced
    // logout this store exists to remove. On a store failure the roles
    // reserved above are released so the names stay claimable.
    let record = OperatorSessionRecord {
        sid: sid.clone(),
        token_digest,
        roles: roles.clone(),
        capability_manifest,
        joined_at_secs,
        desc,
        // D2: the observed part starts empty and only ever grows, from the
        // `Assign` sites. A mint has assigned nothing yet.
        observed: Vec::new(),
        observed_total: 0,
    };
    if let Err(error) = state.operator_session_store.put(record.clone()).await {
        tracing::error!(%sid, %error, "operators_create: session persist failed");
        let mut map = state.roles_to_sid.lock().await;
        for r in &roles {
            if map.get(r.as_str()) == Some(&sid) {
                map.remove(r.as_str());
            }
        }
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
    // as it was: release the roles, answer `500`, leave nothing behind.
    //
    // # The window this ordering leaves
    //
    // Between the registration below and the map insert after it, the sid
    // and its roles resolve in the engine while `operator_sessions` does
    // not yet hold the session. Both exits a parked send has —
    // `replace_tx` (reached only via `operators_ws_connect`) and
    // `ConnState::TornDown` (published only by `teardown_operator_session`)
    // — look the sid up in that map first, so a dispatch landing in the
    // window parks with neither exit reachable. It is bounded, not stuck:
    // the park sits inside the run driver, which ends at its own
    // `sync_timeout_secs` or detach TTL. The cost is a run that burns its
    // full ceiling and then reports a timeout, naming the wrong cause.
    // Reaching the window needs this request to die between the two
    // statements (see the dropped-response-future note below).
    let live = LoginSession::new(record, state.base_url.clone());
    register_operator_session(
        &state.engine,
        Some(&state.operator_adapters),
        &sid,
        &roles,
        live.dispatch_target(),
    )
    .await;

    state
        .operator_sessions
        .lock()
        .await
        .insert(sid.clone(), live);

    (
        StatusCode::OK,
        Json(OperatorsCreateResp { sid, token, roles }),
    )
        .into_response()
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

    ws.on_upgrade(move |socket| handle_operator_socket(socket, live))
}

/// Binds a session's dispatch target into every registry an Operator
/// session must be reachable through: the engine's three (`senior_bridge` /
/// `spawn_hook` / `operator`) under `sid`, the `OperatorAdapterRegistry`
/// when one is wired, and the operator registries again under each of
/// `roles`.
///
/// # Why two operator-side registries
///
/// They answer different questions and are read at different times:
///
/// - **`engine.register_operator`** — the Blueprint-global *delegate* axis
///   (`OperatorDelegateMiddleware`). A launch names a backend id
///   (`operator_backend_id`, set from `operator_sid`) and the engine
///   resolves it once at attach time; an id that resolves to nothing makes
///   the middleware fall through to `inner.spawn` silently, so a session
///   missing here is a quiet loss of a shipped feature.
/// - **`adapters`** — the AgentSpec axis's delivery side. A dispatch
///   through a `kind = Operator` agent resolves its seat's *current holder*
///   off `Run.current` and turns that `OperatorId` into a destination
///   through this map (see [`AssigneeRouter`](super::router::AssigneeRouter)).
///   Nothing about the session
///   is baked into the compiled Blueprint any more, which is the whole
///   point of the seat/holder split.
///
/// Both are written here, under the same keys, from this one call. That is
/// what keeps the launch-time guard (`engine.list_operator_ids()`, which
/// answers "is this `operator_sid` a live session") and the dispatch-time
/// lookup on the same id space: a sid or role that passes the guard is one
/// a router can deliver to.
///
/// The single spelling of that registration, and — since the mint path
/// took it over from the first-connect arm — the only one. Two callers
/// reach it, a mint ([`operators_create`]) and the boot-time restore of a
/// persisted record ([`restored_login_session`]), and they have to
/// leave identical registry state, or a session ends up resolvable on one
/// axis (`GET /v1/operators/:sid`) and missing on another (an
/// `operator_sid` pin, a role-aliased spawn).
///
/// Both callers run **before** their [`LoginSession`] reaches
/// `operator_sessions`, so every one of them is registered by the time
/// anything can look it up. Nothing on the WS connect path calls this: a
/// connect that races a teardown must not be able to put a registration
/// back.
///
/// Role exclusivity is settled at mint time (`operators_create`); this only
/// binds the aliases it granted.
async fn register_operator_session(
    engine: &Engine,
    adapters: Option<&Arc<OperatorAdapterRegistry>>,
    sid: &SessionId,
    roles: &[OperatorRef],
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
    for role in roles {
        // The registries are keyed by plain strings (a sid and a role alias
        // share that key space by design — both are `OperatorId`s and both
        // can be what `Assignee.op` records), so the alias is spelled out
        // here rather than handed over as a typed ref.
        if let Some(adapters) = adapters {
            adapters
                .register(
                    role.as_str().to_string(),
                    dispatch_target.clone() as Arc<dyn OperatorAdapter>,
                )
                .await;
        }
        engine
            .register_operator(
                role.as_str().to_string(),
                dispatch_target.clone() as Arc<dyn Operator>,
            )
            .await;
    }
}

/// Builds the [`LoginSession`] for a persisted login record and registers
/// it — the boot-time half of session persistence, called from
/// [`crate::OperatorSessionPersistence::restore`].
///
/// Restoring the login maps alone leaves a window between boot and the
/// owning client's WS reconnect in which the sid is known to
/// `GET /v1/operators/:sid` but not to the engine: a launch pinning it
/// (`POST /v1/tasks` `operator_sid`) is rejected with `400 no such
/// registered operator session`, and a role-routed launch has no operator
/// to reach. Registering here closes that window, which is the whole point
/// of persisting `RunRecord.operator_sid` pins across a restart.
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
    let live = LoginSession::new(record, base_url);
    register_operator_session(
        engine,
        adapters,
        &live.record().sid,
        &live.record().roles,
        live.dispatch_target(),
    )
    .await;
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
async fn handle_operator_socket(socket: WebSocket, live: Arc<LoginSession>) {
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
    let mut write_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                // Invariant: whenever both branches are ready, close wins.
                // The `recv()` arm's channel-end exit closes the socket
                // with no reason, silently degrading the frame below — so
                // `biased;` is load-bearing here, not an optimisation.
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
    loop {
        let item = tokio::select! {
            item = ws_stream.next() => match item {
                Some(item) => item,
                None => break,
            },
            // The write half is sending the Close frame; a client that
            // never answers it must not keep this half parked forever.
            _ = close_requested(&mut read_close_signal) => break,
        };
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

/// Shared teardown for `DELETE /v1/operators/:sid` (`operators_delete`) and
/// `DELETE /v1/operators/by-role/:role` (`operators_delete_by_role` — GH #81
/// Layer 2 (c)): drops the persisted row, the 3 engine registries + role
/// aliases + the adapter-registry bindings + `operator_sessions` entry,
/// releases the sid's ownership in `roles_to_sid`, closes the session's
/// socket, and finally releases every Run seat the operator still held
/// (**O8** — see [`cascade_vacate_seats`]). Idempotent w.r.t. a concurrent
/// delete — every `remove` / `unregister` is a no-op when the entry is
/// already gone, the socket close is latched (see
/// [`WSOperatorSession::close`]), and a repeated cascade finds the seats
/// already vacant.
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
    // The role key unregister above is unconditional, while the
    // `roles_to_sid` release below is guarded by ownership. That asymmetry
    // is reachable today — a role re-minted to another sid between this
    // teardown's start and here loses its registry binding to a teardown
    // that no longer owns the name. Making the two predicates symmetric
    // belongs to the RoleGrant lifecycle work, not here: this teardown's
    // call shapes are deliberately frozen.
    for role in &live.record().roles {
        state.engine.unregister_operator(role.as_str()).await;
        state.operator_adapters.unregister(role.as_str()).await;
    }

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
    session.fail_pending(TEARDOWN_REASON).await;
    // Same reasoning one step further out: with no reconnect possible, the
    // socket itself has no future either. Clearing `tx` alone left the
    // client parked on a live WebSocket that nothing would ever be routed
    // to again — `close` is what actually ends it (a WS Close frame, sent
    // by the pump; see `handle_operator_socket`).
    session.close(TEARDOWN_REASON);
    session.clear_tx().await;

    state.operator_sessions.lock().await.remove(sid);

    {
        let mut map = state.roles_to_sid.lock().await;
        for role in &live.record().roles {
            if map.get(role.as_str()) == Some(sid) {
                map.remove(role.as_str());
            }
        }
    }

    // O8, last: by here the operator is unreachable under every name it
    // answered to, so no dispatch can re-establish what this releases.
    cascade_vacate_seats(state, sid, &live.record().roles).await;

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
/// `mse-mcp` shutdown, and `DELETE /v1/operators/by-role/:role`) is
/// therefore linear in accumulated non-terminal Runs, not in live ones.
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
/// holder, and what happens next depends on **which name** that holder is.
/// A sid is never re-minted, so it resolves to nothing and the dispatch
/// fails loudly at the registry lookup, fixed by an acquire (**A8**). A
/// **role alias does not fail**: teardown re-opens the role names it held
/// for a future mint, and the next session to claim one registers under it
/// — so the lookup succeeds and the dispatch reaches whoever holds that
/// role now. That is what a role alias means (`seat_declared_operators`
/// seats role names for exactly this reason), so the outcome is correct,
/// but it is a live re-route rather than a loud failure and should not be
/// read as one. Either way the repair is the same acquire, which is why
/// **O8** is described as nice-to-have rather than load-bearing.
const CASCADE_STATUSES: [RunStatus; 4] = [
    RunStatus::Pending,
    RunStatus::Running,
    RunStatus::Interrupted,
    RunStatus::Cancelled,
];

/// **O8**: `delete(op) ⟹ ∀run. current = Assigned(a) ∧ a.op = op ⟹
/// current := Vacant`.
///
/// # Both names, not just the sid
///
/// A session answers to its sid *and* to every role it holds — the login
/// path registers the adapter under all of them, and a launch seats a role
/// name as readily as a sid — so `current[slot].op` may be either. The
/// cascade therefore matches against the whole set; releasing only the sid
/// would leave the role-aliased seats pointing at the operator that was
/// just deleted, which is precisely the lie **O8** exists to prevent.
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
async fn cascade_vacate_seats(state: &AppState, sid: &SessionId, roles: &[OperatorRef]) {
    let mut names: Vec<&str> = Vec::with_capacity(roles.len() + 1);
    names.push(sid.as_str());
    names.extend(roles.iter().map(|role| role.as_str()));

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
                .filter(|(_, assignee)| names.contains(&assignee.op.as_str()))
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

/// `DELETE /v1/operators/:sid`. Bearer mandatory. `404` on unknown sid, `401`
/// on token mismatch. Drops the persisted row, the 3 engine registries +
/// role aliases + the adapter-registry bindings + `operator_sessions`
/// entry, releases this sid's ownership in `roles_to_sid` (re-opening the
/// role names for a future mint), closes the session's socket, and vacates
/// every Run seat this operator still held under its sid or any of its
/// roles (**O8**).
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

// ─── GH #81 Layer 2: GET /v1/operators + DELETE /v1/operators/by-role/:role

/// One entry in the `GET /v1/operators` list response — a live session's
/// identity plus its 記名 (model §4.2).
///
/// Still carries no token and no capability manifest: the former is the
/// bearer secret, and the latter is what `GET /v1/operators/:sid` is for.
#[derive(Debug, Serialize)]
pub struct OperatorsListEntry {
    /// Session id (`S-<hex>`) — safe to expose; token is the sole bearer secret.
    pub sid: SessionId,
    /// Role aliases held by this session.
    pub roles: Vec<OperatorRef>,
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
pub(crate) async fn authorize_any_operator(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(), Response> {
    let bearer = extract_bearer_token_required(headers).map_err(|resp| *resp)?;
    let sessions: Vec<Arc<LoginSession>> = {
        let map = state.operator_sessions.lock().await;
        map.values().cloned().collect()
    };
    if sessions.iter().any(|live| live.verify_bearer(&bearer)) {
        return Ok(());
    }
    Err((
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "error": "token matches no live operator session",
            "hint": "join with POST /v1/operators (no Bearer needed) and present the token it \
                     mints; any live session's token opens this list (model D3/W5)",
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
    let total = entries.len();
    let mut operators = Vec::with_capacity(total);
    for live in entries {
        let connected = live.dispatch_target().is_connected().await;
        let record = live.kimei().await;
        operators.push(OperatorsListEntry {
            sid: record.sid.clone(),
            roles: record.roles.clone(),
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

/// `DELETE /v1/operators/by-role/:role`. Releases the session currently
/// holding `role` without requiring the caller to know the sid or its
/// Bearer token (GH #81 Layer 2 (c)). Recovery route for a stale session
/// whose driver crashed after minting the sid — pre-#81 the only reliable
/// recovery was a full server restart, which also dropped every OTHER live
/// session. Same trust tier as the server-shutdown surface
/// (`mlua_swarm_server_shutdown`): admin observability, no Bearer.
///
/// `404` when no session holds the role, `204` on successful teardown, and
/// `500` when the persisted row could not be dropped (in which case the
/// session is left fully intact — see [`teardown_operator_session`]). The
/// response body on `204` is empty (`teardown_operator_session` performs
/// the same cleanup as `operators_delete`).
///
/// # In-flight protection (`409` unless `?force=true`)
///
/// A role name is process-global, so "the session holding `main-ai`" may
/// well be another driver's live session rather than the stale one the
/// caller meant to clear — and teardown fails every parked spawn on it
/// ([`teardown_operator_session`]'s `fail_pending`). When the holding sid is
/// pinned by at least one `Running` Run (`RunRecord.operator_sid`), this
/// route refuses with `409` and lists those run ids, so the recovery habit
/// cannot take a working run down as collateral. `?force=true` performs the
/// teardown anyway — the escape hatch for a genuinely wedged session whose
/// runs will never finish.
///
/// The check reads through [`crate::AppState::run_store`]; a store read
/// failure is itself a `409` (refuse rather than tear down blind).
pub async fn operators_delete_by_role(
    State(state): State<AppState>,
    Path(role): Path<String>,
    axum::extract::Query(query): axum::extract::Query<OperatorsDeleteByRoleQuery>,
) -> Response {
    // Same shape as the sibling `:sid` routes' `SessionId::parse`: a path
    // segment that cannot be a role cannot name a holder of one. Axum does
    // not match an empty segment, so this arm is unreachable in practice —
    // it exists so the rest of the handler works in `OperatorRef`.
    let Ok(role) = OperatorRef::new(role) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "no session holds this role"})),
        )
            .into_response();
    };
    let sid = {
        let map = state.roles_to_sid.lock().await;
        match map.get(role.as_str()) {
            Some(sid) => sid.clone(),
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": "no session holds this role", "role": role})),
                )
                    .into_response();
            }
        }
    };
    let live = {
        let map = state.operator_sessions.lock().await;
        map.get(&sid).cloned()
    };
    let live = match live {
        Some(e) => e,
        None => {
            // The role was mapped to a sid that has no matching
            // `operator_sessions` entry. Reachable while a mint is still
            // in flight: `operators_create` reserves the role names first
            // and inserts the entry last, with the persist write-through
            // and the engine registration in between. Release the stale
            // role mapping so a future mint can reclaim the name, then
            // report NOT_FOUND.
            //
            // Deliberately no unregister here: this branch has no entry,
            // so it has no `roles` list to unregister and no session to
            // close. A sid can still arrive here already registered — a
            // mint cancelled between `register_operator_session` and the
            // map insert leaves exactly that, both being yield points and
            // hyper dropping the response future when the peer goes away.
            //
            // That residue is bounded rather than permanent. The persisted
            // row survives it (`put` had succeeded and no `delete` ran), so
            // the next boot's `restored_login_session` registers
            // the same sid and roles over the stale keys and this time
            // publishes a real entry. Until then nothing outside the
            // process can name the sid: the response carrying it was never
            // written.
            let mut map = state.roles_to_sid.lock().await;
            if map.get(role.as_str()) == Some(&sid) {
                map.remove(role.as_str());
            }
            return (
                StatusCode::NOT_FOUND,
                Json(json!({
                    "error": "torn role mapping cleared; role now open",
                    "role": role,
                })),
            )
                .into_response();
        }
    };
    if !query.force {
        match active_runs_for_sid(&state, &sid).await {
            Ok(active_runs) if !active_runs.is_empty() => {
                return (
                    StatusCode::CONFLICT,
                    Json(json!({
                        "error": "session is driving in-flight runs; \
                                  tearing it down would fail their parked spawns",
                        "role": role,
                        "sid": sid,
                        "active_runs": active_runs,
                        "hint": "wait for the runs to finish, or repeat with ?force=true \
                                 to tear the session down anyway",
                    })),
                )
                    .into_response();
            }
            Ok(_) => {}
            Err(error) => {
                // Unknown occupancy is not "no occupancy": refusing keeps a
                // store outage from turning this recovery route into a
                // silent killer of someone else's runs. `?force=true` still
                // gets through.
                tracing::warn!(%role, %sid, %error, "operators_delete_by_role: run occupancy check failed");
                return (
                    StatusCode::CONFLICT,
                    Json(json!({
                        "error": format!(
                            "cannot verify whether this session is driving in-flight runs: {error}"
                        ),
                        "role": role,
                        "sid": sid,
                        "hint": "retry, or repeat with ?force=true to tear the session down \
                                 without the check",
                    })),
                )
                    .into_response();
            }
        }
    }
    if let Err(error) = teardown_operator_session(&state, &sid, &live).await {
        return teardown_failed_response(&sid, &error);
    }
    StatusCode::NO_CONTENT.into_response()
}

/// Query string of `DELETE /v1/operators/by-role/:role`.
#[derive(Debug, Deserialize, Default)]
pub struct OperatorsDeleteByRoleQuery {
    /// Tear the session down even while it is driving `Running` runs.
    /// `false` (the default, and the shape every pre-guard caller sends)
    /// refuses with `409` in that case.
    #[serde(default)]
    pub force: bool,
}

/// Ids of the `Running` runs pinned to `sid` (`RunRecord.operator_sid`),
/// ascending by `created_at` so the response order is stable. Empty means
/// the session drives nothing right now.
async fn active_runs_for_sid(state: &AppState, sid: &SessionId) -> Result<Vec<String>, String> {
    let mut running = state
        .run_store
        .list_running()
        .await
        .map_err(|e| e.to_string())?;
    running.sort_by_key(|record| record.created_at);
    Ok(running
        .into_iter()
        .filter(|record| record.operator_sid.as_deref() == Some(sid.as_str()))
        .map(|record| record.id.to_string())
        .collect())
}

// ─── GET /v1/operators/:sid (Bearer required) ───────────────────────────────

/// Response for `GET /v1/operators/:sid`.
#[derive(Debug, Serialize)]
pub struct OperatorsInfoResp {
    /// Echoes the requested session id.
    pub sid: SessionId,
    /// Role aliases held by this session.
    pub roles: Vec<OperatorRef>,
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

    let connected = live.dispatch_target().is_connected().await;
    let record = live.kimei().await;
    (
        StatusCode::OK,
        Json(OperatorsInfoResp {
            sid: record.sid.clone(),
            roles: record.roles.clone(),
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
            "roles": ["main-ai"],
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
        assert_eq!(req.roles, ["main-ai"]);
        assert_eq!(
            req.capability_manifest.unwrap().provider_id,
            "main-ai-self-report"
        );
    }

    #[test]
    fn operators_create_request_keeps_manifest_optional_on_wire() {
        let req: OperatorsCreateReq =
            serde_json::from_value(serde_json::json!({ "roles": [] })).unwrap();
        assert!(req.capability_manifest.is_none());
    }

    // ── by-role teardown: in-flight protection ───────────────────────────

    mod by_role_in_flight {
        use super::*;
        use mlua_swarm::core::config::EngineCfg;
        use mlua_swarm::core::engine::Engine;
        use mlua_swarm::store::output::InMemoryOutputStore;
        use mlua_swarm::store::run::{InMemoryRunStore, RunRecord, RunStatus};
        use mlua_swarm::store::task::InMemoryTaskStore;
        use mlua_swarm::RunId;
        use mlua_swarm::TaskId;
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
                roles_to_sid: Arc::new(Mutex::new(HashMap::new())),
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

        /// Seed one live session holding `role`. Its dispatch target has
        /// never seen a socket; teardown and the guard both work off the
        /// login record.
        async fn seed_session(state: &AppState, role: &str) -> SessionId {
            let role = OperatorRef::new(role).expect("test role literal is never empty");
            let sid = SessionId::new();
            let live = LoginSession::new(
                OperatorSessionRecord {
                    sid: sid.clone(),
                    token_digest: OperatorSessionRecord::digest_of("token"),
                    roles: vec![role.clone()],
                    capability_manifest: None,
                    joined_at_secs: 0,
                    desc: None,
                    observed: Vec::new(),
                    observed_total: 0,
                },
                None,
            );
            state
                .operator_sessions
                .lock()
                .await
                .insert(sid.clone(), live);
            state.roles_to_sid.lock().await.insert(role, sid.clone());
            sid
        }

        async fn seed_run(state: &AppState, sid: Option<&SessionId>, status: RunStatus) -> RunId {
            let run_id = RunId::new();
            state
                .run_store
                .create(RunRecord {
                    id: run_id.clone(),
                    task_id: TaskId::new(),
                    status,
                    step_entries: Vec::new(),
                    degradations: Vec::new(),
                    operator_sid: sid.map(|s| s.to_string()),
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

        pub(super) async fn body_json(response: Response) -> serde_json::Value {
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("read body");
            serde_json::from_slice(&bytes).expect("json body")
        }

        async fn session_is_live(state: &AppState, sid: &SessionId) -> bool {
            state.operator_sessions.lock().await.contains_key(sid)
        }

        /// No run pins the holder: the recovery route behaves exactly as it
        /// did before the guard.
        #[tokio::test]
        async fn idle_holder_is_still_torn_down() {
            let state = test_state();
            let sid = seed_session(&state, "main-ai").await;
            // A Running run belonging to a DIFFERENT session must not
            // protect this one.
            let other = SessionId::new();
            seed_run(&state, Some(&other), RunStatus::Running).await;
            // ...and a finished run of this session must not either.
            seed_run(&state, Some(&sid), RunStatus::Done).await;

            let response = operators_delete_by_role(
                State(state.clone()),
                Path("main-ai".to_string()),
                axum::extract::Query(OperatorsDeleteByRoleQuery::default()),
            )
            .await;
            assert_eq!(response.status(), StatusCode::NO_CONTENT);
            assert!(!session_is_live(&state, &sid).await);
        }

        /// The holder is driving a Running run: refuse, and say which runs
        /// would have been failed.
        #[tokio::test]
        async fn holder_driving_a_running_run_is_refused_with_its_run_ids() {
            let state = test_state();
            let sid = seed_session(&state, "main-ai").await;
            let run_id = seed_run(&state, Some(&sid), RunStatus::Running).await;

            let response = operators_delete_by_role(
                State(state.clone()),
                Path("main-ai".to_string()),
                axum::extract::Query(OperatorsDeleteByRoleQuery::default()),
            )
            .await;
            assert_eq!(response.status(), StatusCode::CONFLICT);
            let body = body_json(response).await;
            assert_eq!(
                body["active_runs"],
                serde_json::json!([run_id.to_string()]),
                "the 409 must name the in-flight runs: {body}"
            );
            assert!(
                session_is_live(&state, &sid).await,
                "a refused teardown must leave the session (and its parked spawns) alone"
            );
            // The role stays claimed — a refused recovery changes nothing.
            assert_eq!(
                state.roles_to_sid.lock().await.get("main-ai"),
                Some(&sid),
                "a refused teardown must not release the role"
            );
        }

        /// `?force=true` is the escape hatch for a wedged session whose runs
        /// will never finish.
        #[tokio::test]
        async fn force_tears_down_despite_in_flight_runs() {
            let state = test_state();
            let sid = seed_session(&state, "main-ai").await;
            seed_run(&state, Some(&sid), RunStatus::Running).await;

            let response = operators_delete_by_role(
                State(state.clone()),
                Path("main-ai".to_string()),
                axum::extract::Query(OperatorsDeleteByRoleQuery { force: true }),
            )
            .await;
            assert_eq!(response.status(), StatusCode::NO_CONTENT);
            assert!(!session_is_live(&state, &sid).await);
        }

        /// Unknown role keeps its pre-guard `404` (the guard runs after the
        /// lookup, not before it).
        #[tokio::test]
        async fn unknown_role_still_404s() {
            let state = test_state();
            let response = operators_delete_by_role(
                State(state),
                Path("nobody-holds-this".to_string()),
                axum::extract::Query(OperatorsDeleteByRoleQuery::default()),
            )
            .await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }
    }

    // ── O8: a delete leaves no holder behind ─────────────────────────────

    /// **O8** exists for the sake of the handover list: a `current` that
    /// names an Operator nobody can reach makes the material a joining AI
    /// reads to answer "is this mine, and is anyone on it?" lie to it.
    /// These tests are written against that reading — they assert on what
    /// `GET /v1/runs/:id` shows, not on the store call underneath.
    mod o8_cascade {
        use super::by_role_in_flight::test_state;
        use super::*;
        use mlua_swarm::store::run::{RunRecord, RunStatus};
        use mlua_swarm::{RunId, TaskId};

        /// The seat names in play. Neither is a role word — an Operator
        /// seat is a lane of the flow, not a job title.
        const SEAT_A: &str = "phase-a-op";
        const SEAT_B: &str = "phase-b-op";
        /// The role alias the session under test holds, and therefore the
        /// second name its seats can be filed under.
        const ROLE: &str = "ws-relay-one";
        /// The bearer every seeded session answers to.
        const TOKEN: &str = "token";

        async fn seed_session(state: &AppState, role: &str) -> SessionId {
            let role = OperatorRef::new(role).expect("test role literal is never empty");
            let sid = SessionId::new();
            let live = LoginSession::new(
                OperatorSessionRecord {
                    sid: sid.clone(),
                    token_digest: OperatorSessionRecord::digest_of(TOKEN),
                    roles: vec![role.clone()],
                    capability_manifest: None,
                    joined_at_secs: 0,
                    desc: None,
                    observed: Vec::new(),
                    observed_total: 0,
                },
                None,
            );
            state
                .operator_sessions
                .lock()
                .await
                .insert(sid.clone(), live);
            state.roles_to_sid.lock().await.insert(role, sid.clone());
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

        /// **O8, both names.** A session answers to its sid *and* to every
        /// role it holds, and a seat can be filed under either. Deleting
        /// it releases both — and leaves a seat held by an unrelated
        /// operator exactly where it was, because O8 is scoped to the
        /// operator that was deleted, not to the Run.
        #[tokio::test]
        async fn deleting_an_operator_releases_every_seat_it_held_under_either_name() {
            let state = test_state();
            let sid = seed_session(&state, ROLE).await;

            let by_sid = seed_run(&state, RunStatus::Running).await;
            seat(&state, &by_sid, SEAT_A, sid.as_str()).await;

            let by_role = seed_run(&state, RunStatus::Running).await;
            seat(&state, &by_role, SEAT_A, ROLE).await;
            seat(&state, &by_role, SEAT_B, "S-somebody-else").await;

            delete_session(&state, &sid).await;

            assert_eq!(
                holder_on_the_wire(&state, &by_sid, SEAT_A).await,
                None,
                "the seat held under the sid must not survive the delete"
            );
            assert_eq!(
                holder_on_the_wire(&state, &by_role, SEAT_A).await,
                None,
                "nor the seat held under one of its role aliases"
            );
            assert_eq!(
                holder_on_the_wire(&state, &by_role, SEAT_B).await,
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
            let sid = seed_session(&state, ROLE).await;
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
            let sid = seed_session(&state, ROLE).await;

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
            let sid = seed_session(&state, ROLE).await;
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
            let sid = seed_session(&state, ROLE).await;
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

        /// The by-role recovery route shares `teardown_operator_session`,
        /// so it cascades identically — the seats do not depend on which
        /// door the delete came through.
        #[tokio::test]
        async fn the_by_role_route_cascades_too() {
            let state = test_state();
            let sid = seed_session(&state, ROLE).await;
            let run_id = seed_run(&state, RunStatus::Running).await;
            seat(&state, &run_id, SEAT_A, sid.as_str()).await;

            let response = operators_delete_by_role(
                State(state.clone()),
                Path(ROLE.to_string()),
                axum::extract::Query(OperatorsDeleteByRoleQuery { force: true }),
            )
            .await;
            assert_eq!(response.status(), StatusCode::NO_CONTENT);
            assert_eq!(holder_on_the_wire(&state, &run_id, SEAT_A).await, None);
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
    /// /v1/operators/:sid` answers `404` without the entry, and the
    /// by-role route's torn branch clears the role map without
    /// unregistering. The registration survived until process exit.
    ///
    /// Driving that interleaving through a live server is not something a
    /// test can order (the gap is between hyper writing `101` and running
    /// the upgrade callback), so it is pinned structurally instead: mint
    /// publishes a [`LoginSession`] whose dispatch target is already
    /// registered, and teardown closes that target in place. A late socket
    /// therefore always finds it, and the only thing it can do is
    /// `replace_tx`.
    mod registration_is_owned_by_mint {
        use super::by_role_in_flight::{body_json, test_state};
        use super::*;

        /// convention-token-ok: mlua-swarm public operator role literal.
        const ROLE: &str = "main-ai";

        async fn mint(state: &AppState) -> SessionId {
            let response = operators_create(
                State(state.clone()),
                Json(OperatorsCreateReq {
                    roles: vec![ROLE.to_string()],
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
            assert!(
                registered.contains(&ROLE.to_string()),
                "mint must register the role alias too: {registered:?}"
            );

            assert!(
                state.operator_sessions.lock().await.contains_key(&sid),
                "mint must publish the session it registered"
            );
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
                !registered.contains(&sid.to_string()) && !registered.contains(&ROLE.to_string()),
                "teardown must unregister both the sid and its role: {registered:?}"
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
        /// registries, under both of its `OperatorId`s.
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
            for id in [sid.to_string(), ROLE.to_string()] {
                assert!(
                    guard_side.contains(&id),
                    "the launch guard must know '{id}': {guard_side:?}"
                );
                assert!(
                    dispatch_side.contains(&id),
                    "a holder recorded as '{id}' must be deliverable to: {dispatch_side:?}"
                );
            }

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
            assert!(state.roles_to_sid.lock().await.get(ROLE).is_none());
            assert!(!state
                .engine
                .list_operator_ids()
                .await
                .contains(&sid.to_string()));
        }

        /// A mint whose persist fails must leave nothing behind — no
        /// registration, no entry, no role claim. This is why the
        /// registration sits *after* `store.put` rather than before it
        /// with a compensating unregister.
        #[tokio::test]
        async fn a_mint_whose_persist_fails_registers_nothing() {
            let mut state = test_state();
            state.operator_session_store = Arc::new(AlwaysFailingPutStore);

            let response = operators_create(
                State(state.clone()),
                Json(OperatorsCreateReq {
                    roles: vec![ROLE.to_string()],
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
            assert!(
                state.roles_to_sid.lock().await.get(ROLE).is_none(),
                "a failed mint must release the role names it reserved"
            );
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

            async fn list(&self) -> Result<Vec<OperatorSessionRecord>, OperatorSessionStoreError> {
                Ok(Vec::new())
            }
        }
    }

    // ── the one rule an OperatorRef carries: not empty ───────────────────

    /// `roles: [""]` names no Operator: nothing can hold the role `""`, so a
    /// session claiming it could never be routed to and every later failure
    /// would point somewhere other than the caller that sent it. The mint
    /// rejects it up front with `400`.
    ///
    /// The neighbouring case — `roles: []` — is a different thing entirely
    /// ("claim no alias") and has to keep working; it is asserted here
    /// alongside so the two can never be conflated by a later change.
    mod empty_role_is_rejected {
        use super::by_role_in_flight::{body_json, test_state};
        use super::*;

        async fn mint_with_roles(state: &AppState, roles: Vec<String>) -> Response {
            operators_create(
                State(state.clone()),
                Json(OperatorsCreateReq {
                    roles,
                    capability_manifest: None,
                    desc: None,
                }),
            )
            .await
        }

        #[tokio::test]
        async fn an_empty_role_is_rejected_with_400_and_mints_nothing() {
            let state = test_state();

            let response = mint_with_roles(&state, vec![String::new()]).await;
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "an empty role alias must be refused at the boundary"
            );
            let body = body_json(response).await;
            assert!(
                body["error"].as_str().unwrap_or_default().contains("role"),
                "the 400 must say what was wrong with the request: {body}"
            );

            // A refused mint is a mint that never happened — the same
            // all-or-nothing contract the persist-failure path holds to.
            assert!(
                state.engine.list_operator_ids().await.is_empty(),
                "a refused mint must not register anything"
            );
            assert!(state.operator_sessions.lock().await.is_empty());
            assert!(state.roles_to_sid.lock().await.is_empty());
            assert!(
                state
                    .operator_session_store
                    .list()
                    .await
                    .expect("list the store")
                    .is_empty(),
                "a refused mint must not persist a row"
            );
        }

        /// One empty element poisons the whole request rather than being
        /// silently dropped: the caller asked for a role it cannot have.
        #[tokio::test]
        async fn an_empty_role_alongside_a_valid_one_still_rejects_the_whole_mint() {
            let state = test_state();
            let response =
                mint_with_roles(&state, vec!["main-ai".to_string(), String::new()]).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert!(
                state.roles_to_sid.lock().await.is_empty(),
                "the valid role must not be reserved by a request that was refused"
            );
        }

        /// The regression guard for the distinction above: claiming *no*
        /// alias is not the same as claiming an empty one, and stays a
        /// successful mint.
        #[tokio::test]
        async fn claiming_no_roles_at_all_still_mints() {
            let state = test_state();
            let response = mint_with_roles(&state, Vec::new()).await;
            assert_eq!(
                response.status(),
                StatusCode::OK,
                "an empty `roles` array claims no alias and must keep working"
            );
            let body = body_json(response).await;
            assert_eq!(
                body["roles"],
                serde_json::json!([]),
                "the response still echoes an empty array: {body}"
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
        use super::by_role_in_flight::{body_json, test_state};
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

        async fn mint_client(state: &AppState, role: &str, name: &'static str) -> Client {
            let response = operators_create(
                State(state.clone()),
                Json(OperatorsCreateReq {
                    roles: vec![role.to_string()],
                    capability_manifest: None,
                    desc: None,
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

            let mut first = mint_client(&state, "ws-relay-one", "first").await;
            let mut second = mint_client(&state, "ws-relay-two", "second").await;
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

            let mut first = mint_client(&state, "ws-relay-one", "first").await;
            let mut second = mint_client(&state, "ws-relay-two", "second").await;
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

        /// A seat whose holder is a role alias routes like one held by a
        /// sid: both are `OperatorId`s, and the mint registers the session
        /// as an adapter under both.
        #[tokio::test]
        async fn a_seat_held_by_a_role_alias_routes_to_that_sessions_socket() {
            let mut state = test_state();
            let factory = wire_operator_axis(&mut state);
            let mut first = mint_client(&state, "ws-relay-one", "first").await;
            let mut second = mint_client(&state, "ws-relay-two", "second").await;
            let (run_id, _task_id) = seeded_run(&state).await;

            state
                .run_store
                .acquire_assignee(&run_id, SEAT_A, "ws-relay-two", "assigned by role")
                .await
                .expect("assign by role alias");

            assert_eq!(
                dispatch_and_name_the_receiver(
                    factory
                        .resolve_operator(SEAT_A, AGENT_A)
                        .expect("seat A resolves"),
                    ctx_for(&run_id, AGENT_A),
                    &mut first,
                    &mut second,
                )
                .await,
                "second",
                "a role-alias holder must reach the session that holds the role"
            );
        }

        /// **The unpinned launch reaches its operator.** Every bundled
        /// sample launches with no `operator_sid`, and this is the path that
        /// carries it.
        ///
        /// The regression this locks in: routing moved from the factory's
        /// role lookup (`lookup_key = pin.unwrap_or(operator_ref)`) to
        /// `Run.current`, and for a moment the only writer of `current` was
        /// a launch that carried a pin. An unpinned launch then compiled and
        /// died on its first dispatch, naming a `Vacant` seat. The launch
        /// now seats every declared seat whose role has a holder
        /// (`tasks::seat_declared_operators`), so the same driver is reached
        /// — through `current`, where a handover can still move it.
        ///
        /// Both lanes are asserted: a multi-seat Blueprint comes up fully
        /// dispatchable, not with one lane seated and the rest Vacant.
        #[tokio::test]
        async fn an_unpinned_launch_seats_every_declared_seat_and_reaches_it() {
            let mut state = test_state();
            let factory = wire_operator_axis(&mut state);
            compile_through(&factory);

            // Two drivers, each claiming the role one seat is named after —
            // what `mse_operator_join(roles=["phase-a-op"])` produces.
            let mut a = mint_client(&state, SEAT_A, "lane-a-driver").await;
            let mut b = mint_client(&state, SEAT_B, "lane-b-driver").await;
            let (run_id, task_id) = seeded_run(&state).await;

            // The launch. No `operator_sid`, so no pinned seat to exclude.
            crate::tasks::seat_declared_operators(
                &state,
                &run_id,
                &task_id,
                &two_seat_blueprint().operators,
                None,
            )
            .await
            .expect("seating declared seats whose roles have holders");

            let record = state.run_store.get(&run_id).await.expect("run get");
            for seat in [SEAT_A, SEAT_B] {
                let seated = record.current.get(seat).unwrap_or_else(|| {
                    panic!("seat '{seat}' must be seated by an unpinned launch")
                });
                assert_eq!(
                    seated.op, seat,
                    "the holder is the role alias the seat is named after"
                );
                assert!(
                    seated.desc.starts_with("auto-seated at launch"),
                    "A9: the server-authored desc must say the seat was not chosen by a \
                     caller, so `GET /v1/runs/:id` can tell it from a pin: {}",
                    seated.desc
                );
            }
            assert_eq!(
                state.run_store.get(&run_id).await.expect("run get").current[SEAT_A].gen,
                1,
                "A4: the first seating of the launch is generation 1"
            );

            // And it dispatches — measured at the socket, both lanes.
            assert_eq!(
                dispatch_and_name_the_receiver(
                    factory
                        .resolve_operator(SEAT_A, AGENT_A)
                        .expect("seat A resolves"),
                    ctx_for(&run_id, AGENT_A),
                    &mut a,
                    &mut b,
                )
                .await,
                "lane-a-driver",
                "an unpinned launch must still reach the driver holding the seat's role"
            );
            assert_eq!(
                dispatch_and_name_the_receiver(
                    factory
                        .resolve_operator(SEAT_B, AGENT_B)
                        .expect("seat B resolves"),
                    ctx_for(&run_id, AGENT_B),
                    &mut a,
                    &mut b,
                )
                .await,
                "lane-b-driver",
                "the second lane is seated too — a multi-seat Blueprint has no permanently \
                 Vacant lane"
            );
        }

        /// The pin outranks the role holder for the seat it names, and a
        /// seat nobody holds stays `Vacant` rather than being filled with a
        /// guess.
        #[tokio::test]
        async fn a_pin_wins_its_own_seat_and_an_unheld_seat_stays_vacant() {
            let mut state = test_state();
            let factory = wire_operator_axis(&mut state);
            compile_through(&factory);

            // `pinned` claims no seat's role; `role_holder` claims seat A's.
            // Nobody claims seat B's.
            let mut pinned = mint_client(&state, "ws-relay-one", "pinned").await;
            let mut role_holder = mint_client(&state, SEAT_A, "role-holder").await;
            let (run_id, task_id) = seeded_run(&state).await;

            // A launch carrying `operator_sid` + `operator_desc`, in the
            // order the handlers run them: the pin first, then the rest of
            // the declared seats.
            crate::tasks::assign_launch_operator(
                &state,
                &run_id,
                &task_id,
                SEAT_A,
                pinned.sid.as_str(),
                "pinned by the launch request",
            )
            .await
            .expect("launch pin");
            crate::tasks::seat_declared_operators(
                &state,
                &run_id,
                &task_id,
                &two_seat_blueprint().operators,
                Some(SEAT_A),
            )
            .await
            .expect("seating the seats the pin did not name");

            let record = state.run_store.get(&run_id).await.expect("run get");
            assert_eq!(
                record.current[SEAT_A].op,
                pinned.sid.to_string(),
                "the pin must not be displaced by the holder of the seat's role"
            );
            assert_eq!(
                record.current[SEAT_A].desc, "pinned by the launch request",
                "the caller's own desc survives — it is not overwritten by the seating literal"
            );
            assert!(
                !record.current.contains_key(SEAT_B),
                "seat B's role has no holder, so it stays Vacant rather than being guessed at"
            );

            assert_eq!(
                dispatch_and_name_the_receiver(
                    factory
                        .resolve_operator(SEAT_A, AGENT_A)
                        .expect("seat A resolves"),
                    ctx_for(&run_id, AGENT_A),
                    &mut pinned,
                    &mut role_holder,
                )
                .await,
                "pinned",
                "the pinned session receives the dispatch, not the role's holder"
            );

            // The Vacant lane fails loudly instead of borrowing seat A's
            // holder, and the message says where the seat would have come
            // from.
            let err = factory
                .resolve_operator(SEAT_B, AGENT_B)
                .expect("seat B resolves")
                .execute(
                    &ctx_for(&run_id, AGENT_B),
                    None,
                    serde_json::json!("go"),
                    Some(worker_binding()),
                    cap_token(AGENT_B),
                )
                .await
                .expect_err("a Vacant seat has no holder to dispatch to");
            let msg = err.to_string();
            assert!(
                msg.contains(SEAT_B) && msg.contains("Vacant"),
                "the failure must name the Vacant seat: {msg}"
            );
            assert!(
                msg.contains("registered under its own name") && msg.contains("operator_sid"),
                "and say both ways the seat could have been filled: {msg}"
            );
            assert!(
                role_holder.inbox.try_recv().is_err(),
                "no other session may absorb the Vacant lane's dispatch"
            );
        }
    }

    // ── 記名 (§4.2) and the holder list (§4.3) ───────────────────────────

    /// The two devices §4.5 leaves standing once **A8** removed
    /// exclusivity, from the read end.
    mod kimei {
        use super::by_role_in_flight::{body_json, test_state};
        use super::*;
        use mlua_swarm::blueprint::Blueprint;
        use mlua_swarm::store::run::{RunRecord, RunStatus};
        use mlua_swarm::store::task::{TaskRecord, TaskRecordStatus};
        use mlua_swarm::{BlueprintRef, RunId, TaskId};

        pub(super) const SEAT_A: &str = "phase-a-op";
        pub(super) const SEAT_B: &str = "phase-b-op";

        pub(super) async fn mint(
            state: &AppState,
            role: &str,
            desc: Option<&str>,
        ) -> (SessionId, String) {
            let response = operators_create(
                State(state.clone()),
                Json(OperatorsCreateReq {
                    roles: vec![role.to_string()],
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
        /// `Assign` lands on the assigned session, addressed by role alias
        /// as well as by sid.
        #[tokio::test]
        async fn an_assign_writes_what_the_task_row_says_onto_the_holders_kimei() {
            let state = test_state();
            let (sid, _token) = mint(&state, SEAT_A, Some("seating lane A")).await;
            let (run_id, task_id) = seed_task_and_run(&state).await;

            // Addressed by the role alias, which is what an auto-seat uses.
            crate::handover::record_observed_assignment(&state, &run_id, &task_id, SEAT_A, SEAT_A)
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
        async fn seed_idle_session(state: &AppState, role: &str, desc: &str) -> SessionId {
            let sid = SessionId::new();
            let live = LoginSession::new(
                OperatorSessionRecord {
                    sid: sid.clone(),
                    token_digest: OperatorSessionRecord::digest_of("idle-token"),
                    roles: vec![OperatorRef::new(role).expect("test role literal is never empty")],
                    capability_manifest: None,
                    joined_at_secs: 0,
                    desc: Some(desc.to_string()),
                    observed: Vec::new(),
                    observed_total: 0,
                },
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
            let idle_sid = seed_idle_session(&state, "idle-op", "has done nothing yet").await;
            let (busy_sid, token) = mint(&state, SEAT_A, Some("seating lane A")).await;
            let (run_id, task_id) = seed_task_and_run(&state).await;
            crate::handover::record_observed_assignment(&state, &run_id, &task_id, SEAT_A, SEAT_A)
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
            let (_sid, _token) = mint(&state, SEAT_A, None).await;

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
            let (_sid, token) = mint(&state, SEAT_A, Some("seating lane A")).await;
            let (run_id, _task_id) = seed_task_and_run(&state).await;
            state
                .run_store
                .acquire_assignee(&run_id, SEAT_A, SEAT_A, "took lane A")
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
            assert_eq!(seats[0]["holder"]["op"], SEAT_A);
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
            let (_sid, token) = mint(&state, SEAT_A, None).await;
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
        use super::by_role_in_flight::{body_json, test_state};
        use super::kimei::{mint, seed_task_and_run, SEAT_A, SEAT_B};
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

        /// Register **one** adapter under several `OperatorId`s, owing
        /// `requests` — the shape `register_operator_session` produces for a
        /// session that claimed more than one role, and the shape a Run
        /// reaches when two of its seats are auto-seated from it.
        async fn one_adapter_seats(state: &AppState, ops: &[&str], requests: Vec<PendingRequest>) {
            let adapter = Arc::new(OwesReplies { requests });
            for op in ops {
                state
                    .operator_adapters
                    .register(*op, adapter.clone() as Arc<dyn OperatorAdapter>)
                    .await;
            }
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
            let (_sid, token) = mint(&state, SEAT_A, Some("seating lane A")).await;
            let (run_id, _task_id) = seed_task_and_run(&state).await;
            state
                .run_store
                .acquire_assignee(&run_id, SEAT_A, SEAT_A, "took lane A")
                .await
                .expect("seat lane A");
            seat_owes(
                &state,
                SEAT_A,
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
            );

            let body = handover(&state, &run_id, &token).await;

            // Axis 2 rides along, from the same RunRecord read.
            let seats = body["seats"].as_array().expect("seats");
            assert_eq!(seats.len(), 2);
            assert_eq!(seats[0]["slot"], SEAT_A);
            assert_eq!(seats[0]["holder"]["op"], SEAT_A);
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
                waiting[0]["op"], SEAT_A,
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

        /// **Each waiting request appears once.** Two seats of one Run can
        /// resolve to the same adapter — a session is registered under its
        /// sid *and* under each of its roles, and a launch auto-seats each
        /// declared slot from the adapter answering to that slot's name.
        /// Asking per seat asked that one object twice and stamped the two
        /// copies with two different `slot` / `op` / `generation` triples,
        /// at most one of which was true.
        #[tokio::test]
        async fn a_request_owed_through_two_seats_is_listed_once() {
            let state = test_state();
            let (_sid, token) = mint(&state, SEAT_A, Some("driving both lanes")).await;
            let (run_id, _task_id) = seed_task_and_run(&state).await;
            for seat in [SEAT_A, SEAT_B] {
                state
                    .run_store
                    .acquire_assignee(&run_id, seat, seat, "one driver, both lanes")
                    .await
                    .expect("seat");
            }
            one_adapter_seats(
                &state,
                &[SEAT_A, SEAT_B],
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
            );
            let _on_b = state.seat_ledger.record(
                &run_id,
                &StepId::parse(STEP_TWO).expect("step id"),
                1,
                SEAT_B,
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
            let (_sid, token) = mint(&state, SEAT_A, None).await;
            let (run_id, _task_id) = seed_task_and_run(&state).await;
            state
                .run_store
                .acquire_assignee(&run_id, SEAT_A, SEAT_A, "took lane A")
                .await
                .expect("seat lane A");
            seat_owes(
                &state,
                SEAT_A,
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
            let (_sid, token) = mint(&state, SEAT_A, None).await;
            let (run_id, _task_id) = seed_task_and_run(&state).await;
            state
                .run_store
                .acquire_assignee(&run_id, SEAT_A, SEAT_A, "took lane A")
                .await
                .expect("seat lane A");
            seat_owes(
                &state,
                SEAT_A,
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
            let (_sid, token) = mint(&state, SEAT_A, None).await;
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
            let (_sid, token) = mint(&state, SEAT_A, None).await;
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
            let (_sid, token) = mint(&state, SEAT_A, None).await;
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
}
