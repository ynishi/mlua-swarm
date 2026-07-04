//! HTTP surface for the Enhance issue axis (current design).
//!
//! - `POST /v1/issues` submits an issue (= `IssueStore.create`).
//! - `GET  /v1/issues/:id` returns its status (= `IssueStore.get + status`).
//!
//! The backend is an `IssueStore` trait object (= the caller selects an
//! `InMemoryIssueStore` or a persistent backend and passes it in). This
//! replaces the pre-v0.9 `/issues` (= no `/v1/` prefix, `InMemoryIssueSource`
//! backend).

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use mlua_swarm::blueprint::store::BlueprintId;
use mlua_swarm::store::issue::{IssueId, IssuePayload, IssueStatus, IssueStore, IssueStoreError};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Body for `POST /v1/issues`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostIssueRequest {
    /// Target Blueprint the issue proposes a change against.
    pub blueprint_id: String,
    /// Free-text description of the desired change; must be non-empty.
    pub intent: String,
}

/// Response for `POST /v1/issues`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostIssueResponse {
    /// Server-minted id (`h-<uuid>`).
    pub issue_id: String,
    /// Always `"pending"` at creation time.
    pub status: String,
}

/// Response for `GET /v1/issues/:id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetIssueResponse {
    /// Echoes the requested issue id.
    pub issue_id: String,
    /// One of `"pending"` / `"in_flight"` / `"applied"` / `"rejected"`.
    pub status: String, // "pending" / "in_flight" / "applied" / "rejected"
    /// Target Blueprint id, when the payload is still available.
    pub blueprint_id: Option<String>,
    /// Original intent text, when the payload is still available.
    pub intent: Option<String>,
    /// Rejection reason; `Some` only when `status == "rejected"`.
    pub reason: Option<String>,
    /// New Blueprint version produced by the change; `Some` only when `status == "applied"`.
    pub new_version: Option<String>,
}

/// Router that provides `/v1/issues` + `/v1/issues/:id`. Callers integrate it
/// into an existing Router via `Router::merge`. The backend is the `IssueStore`
/// passed in as an argument (= pass in the same instance as `EnhancePipeline`
/// via `Arc`).
pub fn build_issues_router(store: Arc<dyn IssueStore>) -> Router {
    Router::new()
        .route("/v1/issues", post(post_issue))
        .route("/v1/issues/:issue_id", get(get_issue))
        .with_state(store)
}

async fn post_issue(
    State(store): State<Arc<dyn IssueStore>>,
    Json(req): Json<PostIssueRequest>,
) -> Result<(StatusCode, Json<PostIssueResponse>), (StatusCode, String)> {
    if req.intent.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "intent must be non-empty".into()));
    }
    let issue_id = format!("h-{}", uuid::Uuid::new_v4());
    let payload = IssuePayload {
        issue_id: IssueId::new(issue_id.clone()),
        blueprint_id: BlueprintId::new(req.blueprint_id),
        intent: req.intent,
    };
    store
        .create(payload)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok((
        StatusCode::CREATED,
        Json(PostIssueResponse {
            issue_id,
            status: "pending".into(),
        }),
    ))
}

async fn get_issue(
    State(store): State<Arc<dyn IssueStore>>,
    Path(issue_id): Path<String>,
) -> Result<Json<GetIssueResponse>, (StatusCode, String)> {
    let id = IssueId::new(issue_id.clone());
    let status = match store.status(&id).await {
        Ok(s) => s,
        Err(IssueStoreError::NotFound(_)) => {
            return Err((
                StatusCode::NOT_FOUND,
                format!("issue not found: {issue_id}"),
            ));
        }
        Err(e) => return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    // Fetch the payload too (return the body when status is Pending / InFlight).
    let payload = store.get(&id).await.ok();

    let (status_str, reason, new_version) = match &status {
        IssueStatus::Pending => ("pending".to_string(), None, None),
        IssueStatus::InFlight => ("in_flight".to_string(), None, None),
        IssueStatus::Applied { new_version } => {
            ("applied".to_string(), None, Some(new_version.clone()))
        }
        IssueStatus::Rejected { reason } => ("rejected".to_string(), Some(reason.clone()), None),
    };
    Ok(Json(GetIssueResponse {
        issue_id,
        status: status_str,
        blueprint_id: payload
            .as_ref()
            .map(|p| p.blueprint_id.as_str().to_string()),
        intent: payload.as_ref().map(|p| p.intent.clone()),
        reason,
        new_version,
    }))
}

/// Wraps `IssueStoreError` into a 500 response (safety net for future extension).
pub struct IssueStoreErrorResponse(
    /// The underlying store error being wrapped.
    pub IssueStoreError,
);

impl IntoResponse for IssueStoreErrorResponse {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}
