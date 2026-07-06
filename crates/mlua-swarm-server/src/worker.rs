//! HTTP `/v1/worker/*` endpoints (SubAgent self-fetch path).
//!
//! # 7-Entry pointer #6 (Output Event design)
//!
//! **This endpoint accesses `OutputStore` directly and does NOT go through the engine.**
//! It is one of the seven entry points enumerated in project `CLAUDE.md` §"Output Event
//! Design SoT". For the canonical description, see the crate root doc of
//! `mlua-swarm-output-store` (`cargo doc -p mlua-swarm-output-store`).
//!
//! # Path
//!
//! A thin-payload path where a SubAgent (= worker process launched by a MainAI) uses
//! the capability token it received via WS Spawn to self-fetch its prompt and
//! submit its result — putting the token in `Authorization: Bearer <encoded CapToken>`.
//!
//! ## Routes
//!
//! - `GET /v1/worker/prompt?task_id=<tid>` — via `engine.fetch_worker_payload`,
//!   returns `{task_id, attempt, agent, system?, prompt}`.
//! - `POST /v1/worker/result` with body `{task_id, value, ok}` — appends one `Final`
//!   to the output tail via `engine.submit_output(Final)` (= the canonical path
//!   through which the dispatch layer decides Pass/Blocked) and updates
//!   `task.last_result` via `engine.post_result`.
//!
//! ## Bearer authentication
//!
//! The Bearer value is the string produced by `CapToken::encode()` (= URL-safe
//! base64 of serde_json). The server decodes it with `CapToken::decode` and then,
//! inside the engine, verifies HMAC sig + role × verb gate + TTL via
//! `verify_token_for_task` (= self-contained capability token; no server-side
//! store lookup required).
//!
//! Tokens are minted during the "2) mint outside the lock" phase of
//! `engine.dispatch_attempt` (`Role::Worker`, 600s TTL, `scopes=["*"]`).
//! The verb gate covers `FetchPrompt` / `EmitOutput` / `PostResult` — the worker
//! leaf capability set (`crate::types::WORKER_LEAF_VERBS`).

use axum::{
    extract::{Query, State},
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    Json,
};
use mlua_swarm::{CapToken, ContentRef, OutputEvent, StepId, WorkerPayload};
use serde::Deserialize;
use serde_json::Value;

use crate::{ApiError, AppState};

/// Query params for `GET /v1/worker/prompt`.
#[derive(Debug, Deserialize)]
pub struct PromptQuery {
    /// Task the fetched prompt belongs to; cross-checked against the Bearer
    /// handle/token. Typed [`StepId`] since issue #14 — the wire shape stays
    /// a plain string; a bad prefix is rejected at deserialize.
    pub task_id: StepId,
}

/// `GET /v1/worker/prompt?task_id=<tid>`. Bearer = encoded `CapToken` or short `wh-` handle.
/// Thin HTTP wrapper over `engine.fetch_worker_payload` / `fetch_worker_payload_trusted`.
/// Short-handle path (recommended for SubAgents): handle → task_id
/// cross-check → trusted fetch.
/// Full-`CapToken` path: token decode → verify → fetch.
pub async fn worker_prompt(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<PromptQuery>,
) -> Result<Json<WorkerPayload>, ApiError> {
    let task_id = q.task_id;
    let bearer = extract_bearer_raw(&headers)?;
    let payload = if let Some(handle) = parse_worker_handle(&bearer) {
        // Short-handle path: verify handle → task_id (security: confirm the handle is bound to this task).
        let resolved = state
            .engine
            .task_id_from_handle(handle)
            .await
            .map_err(|e| ApiError::engine(format!("task_id_from_handle: {e}")))?;
        if resolved != task_id {
            return Err(ApiError::bad_request(format!(
                "handle {handle} is bound to task {resolved}, not {task_id}"
            )));
        }
        state
            .engine
            .fetch_worker_payload_trusted(&task_id)
            .await
            .map_err(|e| ApiError::engine(format!("fetch_worker_payload_trusted: {e}")))?
    } else {
        // Full CapToken path (the alternate Bearer form).
        let token = CapToken::decode(bearer.trim())
            .map_err(|e| ApiError::bad_request(format!("invalid token: {e}")))?;
        state
            .engine
            .fetch_worker_payload(&token, &task_id)
            .await
            .map_err(|e| ApiError::engine(format!("fetch_worker_payload: {e}")))?
    };
    Ok(Json(payload))
}

/// Body for `POST /v1/worker/result`.
#[derive(Debug, Deserialize)]
pub struct WorkerResultReq {
    /// Task this result belongs to (looked up together with the Bearer
    /// token). Typed [`StepId`] since issue #14 (see [`PromptQuery`]).
    pub task_id: StepId,
    /// `WorkerResult.value` (= the value returned by the Operator: LLM inference result or tool execution result).
    pub value: Value,
    /// `WorkerResult.ok`. `false` makes the dispatch path decide Blocked
    /// (= same semantics as `OutputEvent::Final { ok: false, .. }` from a
    /// `SpawnerAdapter`). Defaults to `true`.
    #[serde(default = "default_ok_true")]
    pub ok: bool,
    /// Optional explicit attempt. Normally omitted (= the server looks up `task.attempt`).
    /// A carry for race-condition tests that need to write to a fixed attempt.
    #[serde(default)]
    pub attempt: Option<u32>,
}

fn default_ok_true() -> bool {
    true
}

/// `POST /v1/worker/result`. Bearer = encoded `CapToken`.
/// Fires `engine.submit_output(Final)` + `engine.post_result`.
pub async fn worker_result(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<WorkerResultReq>,
) -> Result<StatusCode, ApiError> {
    let token = decode_worker_bearer(&headers)?;
    let task_id = req.task_id.clone();

    // Use body-explicit attempt if provided; otherwise the current task.attempt.
    let attempt = match req.attempt {
        Some(n) => n,
        None => state
            .engine
            .task_attempt(&task_id)
            .await
            .map_err(|e| ApiError::engine(format!("task_attempt: {e}")))?,
    };

    let event = OutputEvent::Final {
        content: ContentRef::Inline {
            value: req.value.clone(),
        },
        ok: req.ok,
    };
    state
        .engine
        .submit_output(&token, &task_id, attempt, event)
        .await
        .map_err(|e| ApiError::engine(format!("submit_output: {e}")))?;
    state
        .engine
        .post_result(&token, &task_id, req.value)
        .await
        .map_err(|e| ApiError::engine(format!("post_result: {e}")))?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /v1/worker/submit`. Bearer = encoded `CapToken`. Body = raw text/octet.
///
/// Simplification-axis endpoint for SubAgents. Removes the JSON construction,
/// duplicated `task_id`, and JSON-escape burden of `/v1/worker/result` — the
/// worker completes a POST with just token + raw body. Origin: the recent clean-up
/// of the SubAgent contract drift (fewer IDs to pass around, multi-line escape
/// accidents eliminated).
///
/// Behavior:
/// - `task_id` is auto-looked-up server-side from the token (already bound to the `CapToken`).
/// - Body raw bytes go as-is into `Value::String` for `submit_output` + `post_result`.
/// - `ok=true` fixed (= the submit endpoint is success-path only). For the error
///   path, use `/v1/worker/result` with an explicit `ok=false`.
#[derive(Debug, Deserialize, Default)]
pub struct SubmitQuery {
    /// Optional. `ok=false` signals failure (= `DispatchOutcome::Blocked`, caught
    /// by the flow.ir Try path). Unspecified (`None`) is treated as `ok=true`
    /// (= normal success).
    #[serde(default)]
    pub ok: Option<bool>,
}

/// `POST /v1/worker/submit`. Simplified counterpart of [`worker_result`]:
/// the caller sends only the raw result body, `task_id` is resolved
/// server-side from the Bearer handle/token, and `ok` defaults to `true`
/// unless overridden via [`SubmitQuery::ok`]. See the module doc for the
/// short-handle vs full-`CapToken` Bearer forms.
pub async fn worker_submit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<SubmitQuery>,
    body: axum::body::Bytes,
) -> Result<StatusCode, ApiError> {
    // Bearer accepts either (a) `wh-<8 hex>` short handle (recommended for
    // SubAgents) or (b) base64-wrapped CapToken JSON (the full-token form).
    let bearer = extract_bearer_raw(&headers)?;
    let task_id = if let Some(handle) = parse_worker_handle(&bearer) {
        state
            .engine
            .task_id_from_handle(handle)
            .await
            .map_err(|e| ApiError::engine(format!("task_id_from_handle: {e}")))?
    } else {
        let token = CapToken::decode(bearer.trim())
            .map_err(|e| ApiError::bad_request(format!("invalid token: {e}")))?;
        state
            .engine
            .task_id_from_token(&token)
            .await
            .map_err(|e| ApiError::engine(format!("task_id_from_token: {e}")))?
    };
    let attempt = state
        .engine
        .task_attempt(&task_id)
        .await
        .map_err(|e| ApiError::engine(format!("task_attempt: {e}")))?;
    // Strip trailing whitespace (newlines, etc.) so flow.ir `Eq` string matches
    // don't drift on `"BLOCKED\n" == "BLOCKED"` false results. Origin: the recent clean-up
    // verdict_loop smoke — sharp-edge removal. Internal `\n` inside the raw bytes
    // is preserved (= only trailing).
    let body_str = String::from_utf8_lossy(&body).trim_end().to_string();
    let value = Value::String(body_str);

    // The handle path = trusted internal API (= the server-minted handle is validated
    // by the earlier lookup); the full-token path = existing verify-by-token API.
    // Both are reflected identically into final + last_result.
    // `?ok=false` in the query signals failure (= `DispatchOutcome::Blocked`,
    // the flow.ir Try catch path).
    let ok = q.ok.unwrap_or(true);
    state
        .engine
        .submit_worker_result_trusted(&task_id, attempt, value, ok)
        .await
        .map_err(|e| ApiError::engine(format!("submit_worker_result_trusted: {e}")))?;
    Ok(StatusCode::NO_CONTENT)
}

/// Extracts the raw string from the `Authorization` header (= strips the `Bearer `
/// prefix). To let `worker_submit` accept both short handles and full tokens, we
/// fetch the raw value before any decode.
fn extract_bearer_raw(headers: &HeaderMap) -> Result<String, ApiError> {
    let v = headers
        .get(AUTHORIZATION)
        .ok_or_else(|| ApiError::bad_request("missing Authorization header".into()))?
        .to_str()
        .map_err(|_| ApiError::bad_request("invalid Authorization header encoding".into()))?;
    let s = v
        .strip_prefix("Bearer ")
        .ok_or_else(|| ApiError::bad_request("Authorization must be 'Bearer <token>'".into()))?
        .trim();
    if s.is_empty() {
        return Err(ApiError::bad_request("Bearer is empty".into()));
    }
    Ok(s.to_string())
}

/// Decides whether the Bearer is a short handle (`wh-XXXXXXXX`). Returns
/// `Some(handle)` on a match, `None` otherwise (= caller proceeds to try decoding
/// as full `CapToken` JSON).
fn parse_worker_handle(s: &str) -> Option<&str> {
    let s = s.trim();
    if s.starts_with("wh-")
        && s.len() >= 5
        && s.len() <= 64
        && s[3..].chars().all(|c| c.is_ascii_alphanumeric())
    {
        Some(s)
    } else {
        None
    }
}

/// Decodes an encoded `CapToken` from `Authorization: Bearer <encoded CapToken>`.
/// Kept separate from `extract_bearer` (sid-only) — kept as a distinct fn so
/// that sid strings and encoded tokens are not confused, distinguishing them by type.
fn decode_worker_bearer(headers: &HeaderMap) -> Result<CapToken, ApiError> {
    let v = headers
        .get(AUTHORIZATION)
        .ok_or_else(|| ApiError::bad_request("missing Authorization header".into()))?
        .to_str()
        .map_err(|_| ApiError::bad_request("invalid Authorization header encoding".into()))?;
    let encoded = v
        .strip_prefix("Bearer ")
        .ok_or_else(|| ApiError::bad_request("Authorization must be 'Bearer <token>'".into()))?
        .trim();
    if encoded.is_empty() {
        return Err(ApiError::bad_request("Bearer token is empty".into()));
    }
    CapToken::decode(encoded).map_err(|e| ApiError::bad_request(format!("invalid token: {e}")))
}
