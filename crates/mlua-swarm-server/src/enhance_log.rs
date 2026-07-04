//! HTTP surface for the `/v1/enhance/log` axis.
//!
//! - `GET /v1/enhance/log?blueprint_id=<id>` → list of `LogEntry`s tied to
//!   that BP, sorted by `ts` ascending. If `blueprint_id` is omitted, returns
//!   all entries.
//! - `GET /v1/enhance/log/:issue_id` → a single `LogEntry`. NotFound → 404.
//!
//! Shares an `EnhanceLogStore` trait object (= pass in the same instance as
//! `EnhanceApplication` via `Arc`).

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use mlua_swarm::blueprint::store::BlueprintId;
use mlua_swarm::store::enhance_log::{EnhanceLogEntry, EnhanceLogStore, EnhanceLogStoreError};
use mlua_swarm::store::issue::IssueId;
use serde::Deserialize;
use std::sync::Arc;

/// Query params for `GET /v1/enhance/log`.
#[derive(Debug, Deserialize)]
pub struct ListQuery {
    /// When set, restricts the listing to entries tied to this Blueprint id.
    /// `None` returns all entries.
    pub blueprint_id: Option<String>,
}

/// Builds the `/v1/enhance/log*` router backed by the given `EnhanceLogStore`.
pub fn build_enhance_log_router(store: Arc<dyn EnhanceLogStore>) -> Router {
    Router::new()
        .route("/v1/enhance/log", get(list_entries))
        .route("/v1/enhance/log/:issue_id", get(get_entry))
        .with_state(store)
}

async fn list_entries(
    State(store): State<Arc<dyn EnhanceLogStore>>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<EnhanceLogEntry>>, (StatusCode, String)> {
    let entries = match q.blueprint_id {
        Some(bp) => store
            .list_by_blueprint(&BlueprintId::new(bp))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?,
        None => store
            .list_all()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?,
    };
    Ok(Json(entries))
}

async fn get_entry(
    State(store): State<Arc<dyn EnhanceLogStore>>,
    Path(issue_id): Path<String>,
) -> impl IntoResponse {
    match store.get(&IssueId::new(issue_id)).await {
        Ok(e) => (StatusCode::OK, Json(e)).into_response(),
        Err(EnhanceLogStoreError::NotFound(_)) => (
            StatusCode::NOT_FOUND,
            "enhance log entry not found".to_string(),
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
