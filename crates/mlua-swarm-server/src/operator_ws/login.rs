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
use mlua_swarm::{AgentProviderManifest, Operator, SeniorBridge, SessionId, SpawnHook};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

use super::protocol::{ClientMsg, PendingReply, ServerMsg};
use super::session::WSOperatorSession;
use crate::AppState;

/// Login-flow record for a minted Operator session. Held in
/// `AppState.operator_sessions`, keyed by `sid`. `ws_session` starts `None`
/// (login only mints sid+token) and is set on first successful WS connect;
/// on reconnect the same `WSOperatorSession` is reused (`replace_tx`) rather
/// than re-registered.
pub struct OperatorSessionEntry {
    /// Server-minted session id (typed [`SessionId`] since issue #14).
    pub sid: SessionId,
    /// Bearer auth token (10-hex-char) required on the WS upgrade and admin routes.
    pub token: String,
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
/// shape; issue #11) + a 10-hex-char token
/// (`mlua_swarm::types::secure_hex(5)` — OS-RNG hex, unguessable across
/// calls and restarts, which is the point: this token is the sole bearer
/// secret on the short-handle path). When `roles` is non-empty, checks
/// `AppState.roles_to_sid` for conflicts under a single lock (check + insert
/// atomic w.r.t. concurrent mints) and returns `409 CONFLICT` with the
/// conflicting role names on collision. Empty `roles` never conflicts (= no
/// exclusivity is claimed).
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
    // It is an identifier, not a secret: `token` (secure_hex) is the sole
    // bearer credential on this path.
    let sid = SessionId::new();
    let token = mlua_swarm::types::secure_hex(5);

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
    let entry = Arc::new(OperatorSessionEntry {
        sid: sid.clone(),
        token: token.clone(),
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
    if !mlua_swarm::types::ct_eq(entry.token.as_bytes(), bearer.as_bytes()) {
        return (StatusCode::UNAUTHORIZED, "token mismatch").into_response();
    }

    ws.on_upgrade(move |socket| handle_operator_socket(socket, state, entry))
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
            state
                .engine
                .register_senior_bridge(
                    entry.sid.clone(),
                    ws_session.clone() as Arc<dyn SeniorBridge>,
                )
                .await;
            state
                .engine
                .register_spawn_hook(entry.sid.clone(), ws_session.clone() as Arc<dyn SpawnHook>)
                .await;
            state
                .engine
                .register_operator(entry.sid.clone(), ws_session.clone() as Arc<dyn Operator>)
                .await;
            if let Some(factory) = &state.ws_operator_factory {
                factory
                    .register_operator(entry.sid.clone(), ws_session.clone() as Arc<dyn Operator>);
            }
            // Role exclusivity was already resolved at login (POST) time. Here
            // we just bind the same session into the three registries + factory
            // under its role aliases (same shape as handler::handle_socket's
            // ?roles= path).
            for role in &entry.roles {
                if let Some(factory) = &state.ws_operator_factory {
                    factory
                        .register_operator(role.clone(), ws_session.clone() as Arc<dyn Operator>);
                }
                state
                    .engine
                    .register_operator(role.clone(), ws_session.clone() as Arc<dyn Operator>)
                    .await;
            }
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
    if !mlua_swarm::types::ct_eq(entry.token.as_bytes(), bearer.as_bytes()) {
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
pub async fn operators_delete_by_role(
    State(state): State<AppState>,
    Path(role): Path<String>,
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
            // `operator_sessions` entry — a torn state that a mint-time
            // atomic guard prevents in normal operation. Release the
            // stale role mapping so a future mint can reclaim the
            // name, then report NOT_FOUND.
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
    teardown_operator_session(&state, &sid, &entry).await;
    StatusCode::NO_CONTENT.into_response()
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
    if !mlua_swarm::types::ct_eq(entry.token.as_bytes(), bearer.as_bytes()) {
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
}
