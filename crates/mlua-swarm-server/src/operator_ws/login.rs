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
//!   The manifest is pinned to this session and later resolved through the
//!   Core `AgentBindingProvider` interface before any Runner-backed spawn.
//!
//! WS /v1/operators/:sid/ws
//!   Authorization: Bearer <token>   (mandatory — no empty-string default)
//!   → 401 missing/empty Bearer, 404 unknown sid, 401 token mismatch
//!   → registers a `WSOperatorSession` into the engine's 3 registries
//!     (senior_bridge / spawn_hook / operator) + role aliases, same pattern
//!     as `handler::handle_socket`. Reconnect (same sid, matching token)
//!     reuses the existing `WSOperatorSession` via `replace_tx`.
//!
//! DELETE /v1/operators/:sid   (Bearer required)
//!   → unregisters the 3 registries + role aliases + `operator_sessions`
//!     entry + releases `roles_to_sid` ownership.
//!
//! GET /v1/operators/:sid   (Bearer required)
//!   → { sid, roles, connected }
//! ```
//!
//! `OperatorSessionEntry` is the login-flow record (`AppState.operator_sessions`),
//! distinct from `mlua_swarm::OperatorSession` (the engine-side
//! `attach`/session-token record) and from `WSOperatorSession` (the 3-trait WS
//! session, `session.rs`) — this module owns the mapping `sid → (token, roles,
//! Option<WSOperatorSession>)` that the login flow is built on.

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use futures_util::{sink::SinkExt, stream::StreamExt};
use mlua_swarm::store::operator_session::OperatorSessionRecord;
use mlua_swarm::{
    AgentProviderManifest, Engine, Operator, OperatorSpawnerFactory, SeniorBridge, SessionId,
    SpawnHook,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

use super::protocol::{ClientMsg, PendingReply, ServerMsg};
use super::session::WSOperatorSession;
use crate::AppState;

/// Login-flow record for a minted Operator session. Held in
/// `AppState.operator_sessions`, keyed by `sid`. On the mint path
/// `ws_session` starts `None` (login only mints sid+token) and is set on
/// first successful WS connect; on the restore path it is already `Some`
/// (a disconnected session, registered at boot — see
/// [`restored_operator_session_entry`]). Either way a (re)connect reuses
/// that same `WSOperatorSession` via `replace_tx` rather than
/// re-registering it.
pub struct OperatorSessionEntry {
    /// Server-minted session id (typed [`SessionId`] since issue #14).
    pub sid: SessionId,
    /// `hex(SHA-256(bearer))` of the auth token required on the WS upgrade
    /// and admin routes — never the bearer itself, in memory or at rest
    /// (see [`OperatorSessionRecord`]'s type doc). Compare a presented
    /// bearer with [`Self::verify_bearer`].
    pub token_digest: String,
    /// Role aliases claimed by this session (roles-exclusivity set).
    pub roles: Vec<String>,
    /// Provider-owned effective capability manifest submitted at join.
    pub capability_manifest: Option<AgentProviderManifest>,
    /// GH #81 Layer 2: unix epoch seconds when `POST /v1/operators` minted
    /// this entry. Surfaced by `GET /v1/operators` so a recovery driver
    /// can pick the oldest stale session without probing each sid
    /// individually.
    pub joined_at_secs: u64,
    /// The reusable 3-trait session object once a WS has connected at least
    /// once; `None` before first connect. Its sender tracks current connectivity.
    pub ws_session: Mutex<Option<Arc<WSOperatorSession>>>,
}

impl OperatorSessionEntry {
    /// Constant-time check of a presented bearer against
    /// [`Self::token_digest`] — the sole authentication predicate on every
    /// Bearer-guarded operator route.
    pub fn verify_bearer(&self, bearer: &str) -> bool {
        mlua_swarm::types::ct_eq(
            self.token_digest.as_bytes(),
            OperatorSessionRecord::digest_of(bearer).as_bytes(),
        )
    }
}

// ─── POST /v1/operators (mint) ──────────────────────────────────────────────

/// Body for `POST /v1/operators`.
#[derive(Debug, Deserialize, Default)]
pub struct OperatorsCreateReq {
    /// Role aliases to claim exclusively (empty = no exclusivity claimed).
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
    /// Echoes the granted role aliases.
    pub roles: Vec<String>,
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
/// server: everything retained afterwards — the `operator_sessions` entry
/// and the persisted [`OperatorSessionRecord`] — holds
/// `hex(SHA-256(bearer))` instead, and every later check runs
/// [`OperatorSessionEntry::verify_bearer`] against that digest.
pub async fn operators_create(
    State(state): State<AppState>,
    Json(req): Json<OperatorsCreateReq>,
) -> Response {
    let roles = req.roles;
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
        let conflicts: Vec<String> = roles
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
        token_digest: token_digest.clone(),
        roles: roles.clone(),
        capability_manifest: capability_manifest.clone(),
        joined_at_secs,
    };
    if let Err(error) = state.operator_session_store.put(record).await {
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

    let entry = Arc::new(OperatorSessionEntry {
        sid: sid.clone(),
        token_digest,
        roles: roles.clone(),
        capability_manifest,
        joined_at_secs,
        ws_session: Mutex::new(None),
    });
    state
        .operator_sessions
        .lock()
        .await
        .insert(sid.clone(), entry);

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

    let entry = {
        let map = state.operator_sessions.lock().await;
        map.get(&sid).cloned()
    };
    let entry = match entry {
        Some(e) => e,
        None => return (StatusCode::NOT_FOUND, "unknown sid").into_response(),
    };
    if !entry.verify_bearer(&bearer) {
        return (StatusCode::UNAUTHORIZED, "token mismatch").into_response();
    }

    ws.on_upgrade(move |socket| handle_operator_socket(socket, state, entry))
}

/// Binds `ws_session` into every registry an Operator session must be
/// reachable through: the engine's three (`senior_bridge` / `spawn_hook` /
/// `operator`) under `sid`, the `OperatorSpawnerFactory` when one is wired,
/// and the operator registries again under each of `roles`.
///
/// The single spelling of that registration. Two paths reach it — a first
/// WS connect ([`handle_operator_socket`]) and the boot-time restore of a
/// persisted record ([`restored_operator_session_entry`]) — and they have to
/// leave identical registry state, or a session ends up resolvable on one
/// axis (`GET /v1/operators/:sid`) and missing on another (an
/// `operator_sid` pin, a role-aliased spawn).
///
/// Role exclusivity is settled at mint time (`operators_create`); this only
/// binds the aliases it granted.
async fn register_operator_session(
    engine: &Engine,
    ws_operator_factory: Option<&Arc<OperatorSpawnerFactory>>,
    sid: &SessionId,
    roles: &[String],
    ws_session: &Arc<WSOperatorSession>,
) {
    engine
        .register_senior_bridge(sid.clone(), ws_session.clone() as Arc<dyn SeniorBridge>)
        .await;
    engine
        .register_spawn_hook(sid.clone(), ws_session.clone() as Arc<dyn SpawnHook>)
        .await;
    engine
        .register_operator(sid.clone(), ws_session.clone() as Arc<dyn Operator>)
        .await;
    if let Some(factory) = ws_operator_factory {
        factory.register_operator(sid.clone(), ws_session.clone() as Arc<dyn Operator>);
    }
    for role in roles {
        if let Some(factory) = ws_operator_factory {
            factory.register_operator(role.clone(), ws_session.clone() as Arc<dyn Operator>);
        }
        engine
            .register_operator(role.clone(), ws_session.clone() as Arc<dyn Operator>)
            .await;
    }
}

/// Builds the `operator_sessions` entry for a persisted login record and
/// registers it — the boot-time half of session persistence, called from
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
/// connect then takes [`handle_operator_socket`]'s reconnect arm (a
/// `replace_tx`, no second registration).
pub(crate) async fn restored_operator_session_entry(
    engine: &Engine,
    ws_operator_factory: Option<&Arc<OperatorSpawnerFactory>>,
    base_url: Option<Arc<str>>,
    record: OperatorSessionRecord,
) -> Arc<OperatorSessionEntry> {
    let ws_session = Arc::new(WSOperatorSession::disconnected_with_base_url(
        record.sid.clone(),
        base_url,
    ));
    register_operator_session(
        engine,
        ws_operator_factory,
        &record.sid,
        &record.roles,
        &ws_session,
    )
    .await;
    Arc::new(OperatorSessionEntry {
        sid: record.sid,
        token_digest: record.token_digest,
        roles: record.roles,
        capability_manifest: record.capability_manifest,
        joined_at_secs: record.joined_at_secs,
        ws_session: Mutex::new(Some(ws_session)),
    })
}

/// Bidirectional pump for a single WS connection, bound to an
/// `OperatorSessionEntry`. Owns the full wire protocol pump (write task /
/// read task / `ClientMsg` dispatch / disconnect) for this session.
async fn handle_operator_socket(
    socket: WebSocket,
    state: AppState,
    entry: Arc<OperatorSessionEntry>,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<ServerMsg>();

    let existing_ws = entry.ws_session.lock().await.clone();
    let session = match existing_ws {
        Some(ws_session) => {
            // Reconnect: reuse the existing WSOperatorSession on this entry; only swap out `tx`.
            ws_session.replace_tx(tx.clone()).await;
            ws_session
        }
        None => {
            let ws_session = Arc::new(WSOperatorSession::new_with_base_url(
                entry.sid.clone(),
                tx.clone(),
                state.base_url.clone(),
            ));
            register_operator_session(
                &state.engine,
                state.ws_operator_factory.as_ref(),
                &entry.sid,
                &entry.roles,
                &ws_session,
            )
            .await;
            *entry.ws_session.lock().await = Some(ws_session.clone());
            ws_session
        }
    };

    let (mut ws_sink, mut ws_stream) = socket.split();

    // write task: mpsc → WebSocket
    let write_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let txt = match serde_json::to_string(&msg) {
                Ok(s) => s,
                Err(_) => continue,
            };
            if ws_sink.send(Message::Text(txt)).await.is_err() {
                break;
            }
        }
        let _ = ws_sink.close().await;
    });

    // read task: WS message → ClientMsg parse → session.resolve_pending
    let session_for_read = session.clone();
    let read_result: Result<(), String> = async {
        while let Some(item) = ws_stream.next().await {
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
        Ok(())
    }
    .await;

    // Clear only this socket's sender. A reconnect may already have installed
    // a replacement while this older socket was unwinding.
    session.clear_tx_if(&tx).await;
    write_task.abort();
    let _ = read_result;
}

// ─── DELETE /v1/operators/:sid (Bearer required) ────────────────────────────

/// Shared teardown for `DELETE /v1/operators/:sid` (`operators_delete`) and
/// `DELETE /v1/operators/by-role/:role` (`operators_delete_by_role` — GH #81
/// Layer 2 (c)): drops the 3 engine registries + role aliases +
/// `ws_operator_factory` bindings + `operator_sessions` entry, and releases
/// the sid's ownership in `roles_to_sid`. Idempotent w.r.t. a concurrent
/// delete — every `remove` / `unregister` is a no-op when the entry is
/// already gone.
async fn teardown_operator_session(
    state: &AppState,
    sid: &SessionId,
    entry: &Arc<OperatorSessionEntry>,
) {
    state.engine.unregister_senior_bridge(sid.as_str()).await;
    state.engine.unregister_spawn_hook(sid.as_str()).await;
    state.engine.unregister_operator(sid.as_str()).await;
    if let Some(factory) = &state.ws_operator_factory {
        factory.unregister_operator(sid.as_str());
    }
    for role in &entry.roles {
        state.engine.unregister_operator(role).await;
        if let Some(factory) = &state.ws_operator_factory {
            factory.unregister_operator(role);
        }
    }

    if let Some(session) = entry.ws_session.lock().await.take() {
        // B-2: fail every parked spawn/ask/hook_before on this session
        // right away. Teardown removes the session from `operator_sessions`
        // below (no reconnect can find it again), so unlike a plain WS
        // disconnect there is no reconnect/resend contract to preserve —
        // an in-flight spawn parked in `send_and_await` would otherwise
        // orphan until the run's sync timeout (up to 300s) fires.
        session.fail_pending("operator session torn down").await;
        session.clear_tx().await;
    }

    state.operator_sessions.lock().await.remove(sid);

    {
        let mut map = state.roles_to_sid.lock().await;
        for role in &entry.roles {
            if map.get(role) == Some(sid) {
                map.remove(role);
            }
        }
    }

    // Drop the persisted row too, so a restart does not resurrect a
    // deliberately torn-down session. Best-effort: `NotFound` is the
    // idempotent-concurrent-delete case (same contract as the map
    // removals above), any other failure is logged and swallowed — the
    // in-memory teardown already happened and must not be rolled back.
    use mlua_swarm::store::operator_session::OperatorSessionStoreError;
    match state.operator_session_store.delete(sid).await {
        Ok(()) | Err(OperatorSessionStoreError::NotFound(_)) => {}
        Err(error) => {
            tracing::warn!(%sid, %error, "operator session teardown: persisted row delete failed");
        }
    }
}

/// `DELETE /v1/operators/:sid`. Bearer mandatory. `404` on unknown sid, `401`
/// on token mismatch. Drops the 3 engine registries + role aliases +
/// `ws_operator_factory` bindings + `operator_sessions` entry, and releases
/// this sid's ownership in `roles_to_sid` (re-opening the role names for a
/// future mint).
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

    let entry = {
        let map = state.operator_sessions.lock().await;
        map.get(&sid).cloned()
    };
    let entry = match entry {
        Some(e) => e,
        None => return (StatusCode::NOT_FOUND, "unknown sid").into_response(),
    };
    if !entry.verify_bearer(&bearer) {
        return (StatusCode::UNAUTHORIZED, "token mismatch").into_response();
    }

    teardown_operator_session(&state, &sid, &entry).await;

    StatusCode::NO_CONTENT.into_response()
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
    pub roles: Vec<String>,
    /// Unix epoch seconds when the session minted (from
    /// [`OperatorSessionEntry::joined_at_secs`]).
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
    let entries: Vec<(SessionId, Arc<OperatorSessionEntry>)> = {
        let map = state.operator_sessions.lock().await;
        map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    };
    let mut operators = Vec::with_capacity(entries.len());
    for (sid, entry) in entries {
        let session = entry.ws_session.lock().await.clone();
        let connected = match session {
            Some(session) => session.is_connected().await,
            None => false,
        };
        operators.push(OperatorsListEntry {
            sid,
            roles: entry.roles.clone(),
            joined_at_secs: entry.joined_at_secs,
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
/// `404` when no session holds the role, `204` on successful teardown. The
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
    let entry = {
        let map = state.operator_sessions.lock().await;
        map.get(&sid).cloned()
    };
    let entry = match entry {
        Some(e) => e,
        None => {
            // The role was mapped to a sid that has no matching
            // `operator_sessions` entry. Reachable during a mint's
            // roles-reserved → persisted → map-inserted window (the
            // session-persistence write-through sits between the two map
            // updates), or after a mint whose persist failed. Release the
            // stale role mapping so a future mint can reclaim the name,
            // then report NOT_FOUND.
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
    teardown_operator_session(&state, &sid, &entry).await;
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
    pub roles: Vec<String>,
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

    let entry = {
        let map = state.operator_sessions.lock().await;
        map.get(&sid).cloned()
    };
    let entry = match entry {
        Some(e) => e,
        None => return (StatusCode::NOT_FOUND, "unknown sid").into_response(),
    };
    if !entry.verify_bearer(&bearer) {
        return (StatusCode::UNAUTHORIZED, "token mismatch").into_response();
    }

    let session = entry.ws_session.lock().await.clone();
    let connected = match session {
        Some(session) => session.is_connected().await,
        None => false,
    };
    (
        StatusCode::OK,
        Json(OperatorsInfoResp {
            sid: entry.sid.clone(),
            roles: entry.roles.clone(),
            capability_manifest: entry.capability_manifest.clone(),
            connected,
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

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

        fn test_state() -> AppState {
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

        /// Seed one live session holding `role` (no WS attached — teardown
        /// and the guard both work off the login record).
        async fn seed_session(state: &AppState, role: &str) -> SessionId {
            let sid = SessionId::new();
            let entry = Arc::new(OperatorSessionEntry {
                sid: sid.clone(),
                token_digest: OperatorSessionRecord::digest_of("token"),
                roles: vec![role.to_string()],
                capability_manifest: None,
                joined_at_secs: 0,
                ws_session: Mutex::new(None),
            });
            state
                .operator_sessions
                .lock()
                .await
                .insert(sid.clone(), entry);
            state
                .roles_to_sid
                .lock()
                .await
                .insert(role.to_string(), sid.clone());
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

        async fn body_json(response: Response) -> serde_json::Value {
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
}
