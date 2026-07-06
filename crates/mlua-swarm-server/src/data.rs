//! HTTP `/v1/data/*` endpoints (v9 Data path, for Big Response handling).
//!
//! Thin HTTP wrapper that lets SubAgents push Big Responses (4k-token-scale
//! bodies / intermediate artifacts / file paths) **directly to the Store owner,
//! bypassing the MainAgent**. The Store implements
//! `mlua_swarm::store::output::OutputStore` (default =
//! `InMemoryOutputStore`). This module never touches the Engine core or the
//! Domain path (`/v1/worker/result` / `submit_output` / `output_tail` /
//! dispatch verdict) — it is the boundary point that physically wires the
//! Data / Domain separation axis. For the canonical narrative, see
//! `mlua_swarm::store::output` module docs.
//!
//! ## Routes
//!
//! - `POST /v1/data/emit` — body `{task_id, attempt, producer_agent, event, parent_refs?}`
//!   → calls `OutputStore.append` and returns `{out_id}`. The MainAgent only
//!   needs to receive an out_id ref (avoids context bloat).
//! - `POST /v1/data/:name` — same body **minus `producer_agent`** (the path
//!   segment is the producer name); the write-side twin of name addressing.
//! - `GET /v1/data/:key` — `key` is an `out_id` (`out-<10hex>`) or an
//!   `out_name` (producer agent name → latest emit). Id lookup first, name
//!   fallback. Used by the next Spawn's `$IN_REFS` to fetch.
//!
//! ## Auth (single-mouth contract)
//!
//! Every emit requires a worker `CapToken`, carried either as
//! `Authorization: Bearer <token>` or `?token=<token>` (same token material —
//! the transport is the caller's choice). The token passes the
//! `Verb::EmitOutput` gate and is verified against the body's `task_id`.
//! The former split surface (`/v1/data/emit` unauthenticated +
//! `/v1/data/emit-auth` Bearer) was collapsed into this single mouth
//! (the emit-auth API consolidation): expressing auth as endpoint forks multiplies the API
//! surface without adding capability. How far to tighten GET (and token
//! scoping in general) is deferred to the security-hardening pass after
//! dogfooding.

use axum::{
    extract::{Path, Query, State},
    http::{header::AUTHORIZATION, HeaderMap},
    Json,
};
use mlua_swarm::store::output::{OutputEvent, OutputRecord, OutputRef};
use mlua_swarm::{types::Verb, CapToken, StepId};
use serde::{Deserialize, Serialize};

use crate::{ApiError, AppState};

/// Input for `POST /v1/data/emit`.
#[derive(Debug, Deserialize)]
pub struct DataEmitReq {
    /// Producing task.
    pub task_id: String,
    /// Attempt number.
    pub attempt: u32,
    /// Producer agent name.
    pub producer_agent: String,
    /// Event body (`Progress` / `Partial` / `Artifact` / `Final`).
    pub event: OutputEvent,
    /// Refs to upstream outputs (= chain, list of ids received via handoff). May be empty.
    #[serde(default)]
    pub parent_refs: Vec<OutputRef>,
}

/// Input for `POST /v1/data/:name` (name addressing — `producer_agent` comes
/// from the path segment, not the body).
#[derive(Debug, Deserialize)]
pub struct DataEmitNamedReq {
    /// Producing task.
    pub task_id: String,
    /// Attempt number.
    pub attempt: u32,
    /// Event body (`Progress` / `Partial` / `Artifact` / `Final`).
    pub event: OutputEvent,
    /// Refs to upstream outputs. May be empty.
    #[serde(default)]
    pub parent_refs: Vec<OutputRef>,
}

/// Response for the emit endpoints.
#[derive(Debug, Serialize)]
pub struct DataEmitResp {
    /// Assigned ref. The caller (MainAgent) forwards this into the next Spawn's `$IN_REFS`.
    pub out_id: OutputRef,
}

/// Auth-carrying query params (`?token=` — the header-less twin of Bearer).
#[derive(Debug, Deserialize)]
pub struct TokenQuery {
    /// Encoded worker `CapToken`. Same material as the Bearer form.
    pub token: Option<String>,
}

/// Handler for `POST /v1/data/emit` (single mouth, auth required).
pub async fn data_emit(
    State(state): State<AppState>,
    Query(q): Query<TokenQuery>,
    headers: HeaderMap,
    Json(req): Json<DataEmitReq>,
) -> Result<Json<DataEmitResp>, ApiError> {
    emit_inner(&state, &headers, q.token.as_deref(), req).await
}

/// Handler for `POST /v1/data/:name` (name addressing, auth required).
///
/// The static `/v1/data/emit` route shadows this for the literal segment
/// `emit`, so `emit` is effectively a reserved producer name.
pub async fn data_emit_named(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(q): Query<TokenQuery>,
    headers: HeaderMap,
    Json(req): Json<DataEmitNamedReq>,
) -> Result<Json<DataEmitResp>, ApiError> {
    let req = DataEmitReq {
        task_id: req.task_id,
        attempt: req.attempt,
        producer_agent: name,
        event: req.event,
        parent_refs: req.parent_refs,
    };
    emit_inner(&state, &headers, q.token.as_deref(), req).await
}

async fn emit_inner(
    state: &AppState,
    headers: &HeaderMap,
    query_token: Option<&str>,
    req: DataEmitReq,
) -> Result<Json<DataEmitResp>, ApiError> {
    let token = extract_captoken(headers, query_token)?;
    let tid = StepId(req.task_id.clone());
    state
        .engine
        .verify_token_for_task(&token, Verb::EmitOutput, &tid)
        .await
        .map_err(|e| ApiError::engine(format!("data_emit verify: {e}")))?;
    let out_id = state
        .data_store
        .append(
            &req.task_id,
            req.attempt,
            &req.producer_agent,
            req.event,
            req.parent_refs,
        )
        .await
        .map_err(|e| ApiError::engine(format!("data_emit: {e}")))?;
    Ok(Json(DataEmitResp { out_id }))
}

/// Pull the worker `CapToken` from `Authorization: Bearer <t>` or `?token=<t>`
/// (header wins when both are present — it is the more deliberate form).
fn extract_captoken(headers: &HeaderMap, query_token: Option<&str>) -> Result<CapToken, ApiError> {
    let encoded: &str = if let Some(v) = headers.get(AUTHORIZATION) {
        v.to_str()
            .map_err(|_| ApiError::bad_request("invalid Authorization header encoding".into()))?
            .strip_prefix("Bearer ")
            .ok_or_else(|| ApiError::bad_request("Authorization must be 'Bearer <token>'".into()))?
            .trim()
    } else if let Some(t) = query_token {
        t.trim()
    } else {
        return Err(ApiError::bad_request(
            "missing token: pass 'Authorization: Bearer <token>' or '?token=<token>'".into(),
        ));
    };
    if encoded.is_empty() {
        return Err(ApiError::bad_request("token is empty".into()));
    }
    CapToken::decode(encoded).map_err(|e| ApiError::bad_request(format!("invalid token: {e}")))
}

/// Handler for `GET /v1/data/:key` (`key` = out_id, falling back to out_name).
pub async fn data_get(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<OutputRecord>, ApiError> {
    use mlua_swarm::store::output::OutputStoreError;
    let id = OutputRef(key.clone());
    match state.data_store.get(&id).await {
        Ok(record) => Ok(Json(record)),
        Err(OutputStoreError::NotFound(_)) => {
            let record = state
                .data_store
                .get_latest_by_name(&key)
                .await
                .map_err(|e| match e {
                    OutputStoreError::NotFound(k) => {
                        ApiError::not_found(format!("output not found (id nor name): {k}"))
                    }
                    other => ApiError::engine(format!("data_get by name: {other}")),
                })?;
            Ok(Json(record))
        }
        Err(other) => Err(ApiError::engine(format!("data_get: {other}"))),
    }
}
