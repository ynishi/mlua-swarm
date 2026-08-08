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
//!     entry + releases `roles_to_sid` ownership.
//!
//! GET /v1/operators/:sid   (Bearer required)
//!   → { sid, roles, connected }
//! ```
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
use mlua_swarm::store::operator_session::{OperatorSessionRecord, OperatorSessionStoreError};
use mlua_swarm::{
    AgentProviderManifest, Engine, Operator, OperatorRef, OperatorSpawnerFactory, SeniorBridge,
    SessionId, SpawnHook,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

use super::protocol::{ClientMsg, PendingReply, ServerMsg};
use super::session::WSOperatorSession;
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
    /// Read-only snapshot of the persisted row: a later update replaces the
    /// whole `LoginSession` in the map rather than writing through this
    /// field. Its `token_digest` is never read directly —
    /// [`Self::verify_bearer`] is the one predicate over it.
    record: OperatorSessionRecord,
    /// Shared, not owned — the same object the engine's registries hold and a
    /// teardown latches.
    dispatch_target: Arc<WSOperatorSession>,
}

impl LoginSession {
    /// Builds the session for `record` and the disconnected
    /// [`WSOperatorSession`] its sid dispatches through.
    ///
    /// `base_url` is the server's public HTTP root, rendered into this
    /// session's Spawn directives once a socket attaches.
    pub(crate) fn new(record: OperatorSessionRecord, base_url: Option<Arc<str>>) -> Arc<Self> {
        let dispatch_target = Arc::new(WSOperatorSession::disconnected_with_base_url(
            record.sid.clone(),
            base_url,
        ));
        Arc::new(Self {
            record,
            dispatch_target,
        })
    }

    /// The durable login record this session was minted (or restored) from.
    pub fn record(&self) -> &OperatorSessionRecord {
        &self.record
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
    // disconnected: anything sent to it before the client attaches a
    // socket fails loud with `"ws operator disconnected"`.
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
    let live = LoginSession::new(record, state.base_url.clone());
    register_operator_session(
        &state.engine,
        state.ws_operator_factory.as_ref(),
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
/// `spawn_hook` / `operator`) under `sid`, the `OperatorSpawnerFactory` when
/// one is wired, and the operator registries again under each of `roles`.
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
    ws_operator_factory: Option<&Arc<OperatorSpawnerFactory>>,
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
    if let Some(factory) = ws_operator_factory {
        factory.register_operator(sid.clone(), dispatch_target.clone() as Arc<dyn Operator>);
    }
    for role in roles {
        // The registries are keyed by plain strings (a sid and a role alias
        // share that key space by design), so the alias is spelled out here
        // rather than handed over as a typed ref.
        if let Some(factory) = ws_operator_factory {
            factory.register_operator(
                role.as_str().to_string(),
                dispatch_target.clone() as Arc<dyn Operator>,
            );
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
/// not reachable. Anything actually sent to it before the client attaches a
/// socket fails loud with `"ws operator disconnected"`, and the client's
/// connect is then a plain `replace_tx` in [`handle_operator_socket`] — no
/// second registration.
///
/// [`operators_create`] does the same thing at mint time, so this is the
/// restore half of one shared shape rather than a special case.
pub(crate) async fn restored_login_session(
    engine: &Engine,
    ws_operator_factory: Option<&Arc<OperatorSpawnerFactory>>,
    base_url: Option<Arc<str>>,
    record: OperatorSessionRecord,
) -> Arc<LoginSession> {
    let live = LoginSession::new(record, base_url);
    register_operator_session(
        engine,
        ws_operator_factory,
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
/// aliases + `ws_operator_factory` bindings + `operator_sessions` entry,
/// releases the sid's ownership in `roles_to_sid`, and closes the session's
/// socket. Idempotent w.r.t. a concurrent delete — every `remove` /
/// `unregister` is a no-op when the entry is already gone, and the socket
/// close is latched (see [`WSOperatorSession::close`]).
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
    if let Some(factory) = &state.ws_operator_factory {
        factory.unregister_operator(sid.as_str());
    }
    // The role key unregister above is unconditional, while the
    // `roles_to_sid` release below is guarded by ownership. That asymmetry
    // is reachable today — a role re-minted to another sid between this
    // teardown's start and here loses its registry binding to a teardown
    // that no longer owns the name. Making the two predicates symmetric
    // belongs to the RoleGrant lifecycle work, not here: this teardown's
    // call shapes are deliberately frozen.
    for role in &live.record().roles {
        state.engine.unregister_operator(role.as_str()).await;
        if let Some(factory) = &state.ws_operator_factory {
            factory.unregister_operator(role.as_str());
        }
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

    Ok(())
}

/// `DELETE /v1/operators/:sid`. Bearer mandatory. `404` on unknown sid, `401`
/// on token mismatch. Drops the persisted row, the 3 engine registries +
/// role aliases + `ws_operator_factory` bindings + `operator_sessions`
/// entry, releases this sid's ownership in `roles_to_sid` (re-opening the
/// role names for a future mint), and closes the session's socket.
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

/// GH #81 Layer 2 (b): one entry in the `GET /v1/operators` list response.
/// Bare identity fields (no token, no capability manifest — those live
/// behind Bearer on `GET /v1/operators/:sid`); this list surface is
/// read-only observability, on the same trust tier as `GET /v1/status`.
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
}

/// Response body for `GET /v1/operators` (GH #81 Layer 2 (b)).
#[derive(Debug, Serialize)]
pub struct OperatorsListResp {
    /// One entry per live session, ordered by `sid` (deterministic —
    /// callers can `.iter().find(...)` without probing the map order).
    pub operators: Vec<OperatorsListEntry>,
}

/// `GET /v1/operators`. Read-only enumeration of every live session's
/// `{sid, roles, joined_at_secs, connected}` (GH #81 Layer 2 (b)). Same
/// trust tier as `GET /v1/status` — no Bearer required; sids are
/// identifiers, not secrets. Answers "which sid holds `main-ai`?"
/// without probing every sid individually via `GET /v1/operators/:sid`,
/// which was the pre-#81 recovery gap.
pub async fn operators_list(State(state): State<AppState>) -> Response {
    let entries: Vec<(SessionId, Arc<LoginSession>)> = {
        let map = state.operator_sessions.lock().await;
        map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    };
    let mut operators = Vec::with_capacity(entries.len());
    for (sid, live) in entries {
        let connected = live.dispatch_target().is_connected().await;
        operators.push(OperatorsListEntry {
            sid,
            roles: live.record().roles.clone(),
            joined_at_secs: live.record().joined_at_secs,
            connected,
        });
    }
    operators.sort_by(|a, b| a.sid.as_str().cmp(b.sid.as_str()));
    (StatusCode::OK, Json(OperatorsListResp { operators })).into_response()
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
    (
        StatusCode::OK,
        Json(OperatorsInfoResp {
            sid: live.record().sid.clone(),
            roles: live.record().roles.clone(),
            capability_manifest: live.record().capability_manifest.clone(),
            connected,
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
                ws_operator_factory: None,
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
}
