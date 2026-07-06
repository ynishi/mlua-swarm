//! HTTP surface for inspecting Blueprint state (= for debug / animation verification).
//! `/v1/blueprints/:id/head` returns the head Blueprint JSON;
//! `/v1/blueprints/:id/history` returns the commit-version list.
//! Callers pass a shared `Store` via `Arc` and mount the router.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use mlua_swarm::blueprint::loader::{expand_file_refs, pre_read_default_agent_kind};
use mlua_swarm::blueprint::store::{
    blueprint_version, BlueprintId, BlueprintStore, CommitMetadata,
};
use mlua_swarm::blueprint::{default_global_agent_kind, AgentKind, Blueprint};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

/// Router state: BP store + the base dir used to resolve `$file` / `$agent_md`
/// refs + `default_agent_kind` from the CLI (= layer (2) of the 4-tier cascade —
/// the CLI override layer).
/// When `ref_base = None`, ref expansion is skipped (= seed bodies are parsed
/// as raw JSON).
#[derive(Clone)]
pub struct BlueprintsState {
    /// Backing Blueprint store (git2 or in-memory backend).
    pub store: Arc<dyn BlueprintStore>,
    /// Base dir for `$file` / `$agent_md` ref expansion; `None` skips expansion.
    pub ref_base: Option<PathBuf>,
    /// CLI-level `default_agent_kind` override (layer (2) of the 4-tier cascade).
    pub cli_default_agent_kind: Option<AgentKind>,
}

/// Minimal entry: no `ref_base` (ref expansion skipped) and no CLI default kind override.
pub fn build_blueprints_router(store: Arc<dyn BlueprintStore>) -> Router {
    build_blueprints_router_with_refs(store, None, None)
}

/// When `ref_base` is set, `seed_blueprint` resolves `{"$file": ...}` /
/// `{"$agent_md": ...}` refs in the body under that base dir and expands them.
/// Path hygiene (absolute paths and `..` are rejected) is enforced inside
/// `expand_file_refs`, sandboxed to the subtree under the base dir.
///
/// `cli_default_agent_kind` = the override from CLI `--default-agent-kind`
/// (= layer (2) of the 4-tier cascade). Falls back when the BP JSON top-level
/// `default_agent_kind` (= (3)) is absent; if that too is absent, uses the
/// Schema `impl Default` = `Operator` (= (1)).
pub fn build_blueprints_router_with_refs(
    store: Arc<dyn BlueprintStore>,
    ref_base: Option<PathBuf>,
    cli_default_agent_kind: Option<AgentKind>,
) -> Router {
    let state = BlueprintsState {
        store,
        ref_base,
        cli_default_agent_kind,
    };
    Router::new()
        .route("/v1/blueprints/:id/head", get(get_head))
        .route("/v1/blueprints/:id/history", get(get_history))
        .route("/v1/blueprints/:id/unarchive", post(unarchive_blueprint))
        .route(
            "/v1/blueprints/:id",
            post(seed_blueprint).delete(archive_blueprint),
        )
        .with_state(state)
}

/// `DELETE /v1/blueprints/:id` — archive (logical soft-delete) the id.
/// Appends an archive marker commit; the underlying Blueprint YAML is
/// preserved as history. After archive, `read_head` /
/// `TaskApplication::resolve` reject with `Archived`, and `list_ids`
/// filters the id out by default.
///
/// Semantic rename: the HTTP path stays `DELETE` for client
/// compatibility, but the behavior is archive, not physical delete.
/// Restore via `POST /v1/blueprints/:id/unarchive`.
///
/// Returns: 204 No Content.
async fn archive_blueprint(
    State(state): State<BlueprintsState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let bp_id = BlueprintId::new(id.clone());
    state.store.archive_id(&bp_id).await.map_err(|e| match e {
        mlua_swarm::blueprint::store::BlueprintStoreError::HeadEmpty(_)
        | mlua_swarm::blueprint::store::BlueprintStoreError::IdNotFound(_) => {
            (StatusCode::NOT_FOUND, format!("archive_id: {e}"))
        }
        other => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("archive_id: {other}"),
        ),
    })?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /v1/blueprints/:id/unarchive` — reverse of archive. Appends
/// an unarchive marker commit so the audit trail records the event.
async fn unarchive_blueprint(
    State(state): State<BlueprintsState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let bp_id = BlueprintId::new(id.clone());
    state
        .store
        .unarchive_id(&bp_id)
        .await
        .map_err(|e| match e {
            mlua_swarm::blueprint::store::BlueprintStoreError::HeadEmpty(_)
            | mlua_swarm::blueprint::store::BlueprintStoreError::IdNotFound(_) => {
                (StatusCode::NOT_FOUND, format!("unarchive_id: {e}"))
            }
            other => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("unarchive_id: {other}"),
            ),
        })?;
    Ok(StatusCode::NO_CONTENT)
}

/// Format a Blueprint deserialization failure with a schema pointer, so a
/// register error is self-serviceable (the schema export is the MCP adapter
/// `bp_schema` tool = schemars JSON Schema of `Blueprint`).
fn parse_error_with_schema_hint(e: &serde_json::Error) -> String {
    format!(
        "blueprint parse: {e} \
         (hint: fetch the Blueprint JSON Schema via the MCP adapter bp_schema tool)"
    )
}

/// `POST /v1/blueprints/:id` — register / re-register a Blueprint.
///
/// Semantics:
/// - No prior head → seed as first commit (`write_new`, empty
///   parents). Returns 201.
/// - Prior head with **same** `ContentHash` → idempotent no-op.
///   Returns 200 with `seeded: false`.
/// - Prior head with **different** `ContentHash` → append a new
///   commit on top of the current head (Git-native commit graph
///   advance). Returns 201.
/// - Prior head archived → returns 409 `Archived` (call
///   `POST /:id/unarchive` first).
/// - Concurrent POST on the same id → per-id lock contention returns
///   429 Too Many Requests (client retry).
///
/// Path id vs body.id mismatch returns 400.
///
/// When `BlueprintsState.ref_base = Some(dir)`, `{"$file": ...}` /
/// `{"$agent_md": ...}` refs in the body are expanded under the base
/// dir via `expand_file_refs` before being parsed into a typed
/// `Blueprint` (= path hygiene is applied by the loader, rejecting
/// absolute paths and `..`).
async fn seed_blueprint(
    State(state): State<BlueprintsState>,
    Path(id): Path<String>,
    Json(raw_body): Json<serde_json::Value>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, String)> {
    let body: Blueprint = if let Some(base) = state.ref_base.as_ref() {
        // Four-tier cascade for the kind resolution: (3) BP JSON top-level
        // `default_agent_kind` → (2) CLI value → (1) Schema impl Default =
        // Operator. Handed to expand_file_refs so the loader can resolve the
        // kind when the $agent_md sibling is missing. The sibling `"kind"`
        // literal (tier 4) wins first inside expand_file_refs.
        let default_kind = match pre_read_default_agent_kind(&raw_body) {
            // BP top-level carries a literal → use it verbatim.
            kind if raw_body.get("default_agent_kind").is_some() => kind,
            // BP top-level absent → CLI value fallback → Schema default.
            _ => state
                .cli_default_agent_kind
                .clone()
                .unwrap_or_else(default_global_agent_kind),
        };
        let expanded = expand_file_refs(raw_body, base, default_kind)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("ref expand: {e}")))?;
        serde_json::from_value(expanded)
            .map_err(|e| (StatusCode::BAD_REQUEST, parse_error_with_schema_hint(&e)))?
    } else {
        serde_json::from_value(raw_body)
            .map_err(|e| (StatusCode::BAD_REQUEST, parse_error_with_schema_hint(&e)))?
    };
    let store = state.store;
    if id != body.id.as_str() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("path id={id} != body.id={}", body.id),
        ));
    }
    let bp_id = BlueprintId::new(id.clone());
    let v = blueprint_version(&body).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("bp version: {e}"),
        )
    })?;
    let prev_head = match store.read_head(&bp_id).await {
        Ok(traced) => Some(traced),
        Err(mlua_swarm::blueprint::store::BlueprintStoreError::HeadEmpty(_)) => None,
        Err(mlua_swarm::blueprint::store::BlueprintStoreError::Archived(_)) => {
            return Err((
                StatusCode::CONFLICT,
                format!("blueprint {id} is archived; POST /v1/blueprints/{id}/unarchive first"),
            ));
        }
        Err(e) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("read_head: {e}")));
        }
    };
    if let Some(traced) = &prev_head {
        if traced.trace.version == v {
            return Ok((
                StatusCode::OK,
                Json(serde_json::json!({"id": id, "version": format!("{:?}", v), "seeded": false})),
            ));
        }
    }
    let parents: Vec<_> = prev_head
        .as_ref()
        .map(|t| vec![t.trace.version])
        .unwrap_or_default();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .as_millis() as i64;
    let meta = CommitMetadata::seed(bp_id.clone(), v, now_ms);
    store
        .write_new(&bp_id, &body, &parents, meta)
        .await
        .map_err(|e| match &e {
            mlua_swarm::blueprint::store::BlueprintStoreError::LockBusy => (
                StatusCode::TOO_MANY_REQUESTS,
                format!("blueprint {id} lock busy; retry"),
            ),
            mlua_swarm::blueprint::store::BlueprintStoreError::Archived(_) => (
                StatusCode::CONFLICT,
                format!("blueprint {id} is archived; POST /v1/blueprints/{id}/unarchive first"),
            ),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, format!("write_new: {e}")),
        })?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({"id": id, "version": format!("{:?}", v), "seeded": true})),
    ))
}

#[derive(Debug, Serialize)]
struct HeadResponse {
    id: String,
    version: String,
    blueprint: Blueprint,
}

async fn get_head(
    State(state): State<BlueprintsState>,
    Path(id): Path<String>,
) -> Result<Json<HeadResponse>, (StatusCode, String)> {
    let store = state.store;
    let bp_id = BlueprintId::new(id.clone());
    let traced = store
        .read_head(&bp_id)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, format!("read_head: {e}")))?;
    Ok(Json(HeadResponse {
        id,
        version: format!("{:?}", traced.trace.version),
        blueprint: traced.value,
    }))
}

#[derive(Debug, Deserialize)]
struct HistoryQuery {
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    20
}

#[derive(Debug, Serialize)]
struct HistoryEntry {
    /// Content hash (= debug representation of `BlueprintVersion`).
    hash: String,
    /// SemVer label (`Blueprint.metadata.version_label`); `null` when unset.
    version_label: Option<String>,
    /// One-line changelog (= `CommitMetadata.rationale`).
    rationale: String,
}

#[derive(Debug, Serialize)]
struct HistoryResponse {
    count: usize,
    entries: Vec<HistoryEntry>,
}

async fn get_history(
    State(state): State<BlueprintsState>,
    Path(id): Path<String>,
    Query(q): Query<HistoryQuery>,
) -> Result<Json<HistoryResponse>, (StatusCode, String)> {
    let store = state.store;
    let bp_id = BlueprintId::new(id);
    let versions = store
        .history(&bp_id, q.limit)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, format!("history: {e}")))?;
    let mut entries = Vec::with_capacity(versions.len());
    for v in versions {
        let traced = store.read_version(&bp_id, v).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("read_version: {e}"),
            )
        })?;
        let rationale = store
            .read_commit_rationale(&bp_id, v)
            .await
            .unwrap_or(None)
            .unwrap_or_default();
        entries.push(HistoryEntry {
            hash: format!("{:?}", v),
            version_label: traced.value.metadata.version_label.clone(),
            rationale,
        });
    }
    let count = entries.len();
    Ok(Json(HistoryResponse { count, entries }))
}
