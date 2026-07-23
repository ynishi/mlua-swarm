//! `GET /v1/doctor` — server-side infra info snapshot.
//!
//! Surfaces startup config read-only: the Blueprint store real (backend /
//! root path), ref_base, bind, enhance flow on/off, etc. An entry point for
//! callers (the MCP adapter's doctor tool / operator) to answer "where is the Store?"
//! and "how many BPs are registered?" in one shot.
//!
//! Store contents (BP list / head / history) are peeked via the existing
//! `/v1/blueprints/...` routes; doctor covers only the infra layer.

use axum::{extract::State, routing::get, Json, Router};
use mlua_swarm::blueprint::store::BlueprintStore;
use serde::Serialize;
use std::sync::Arc;

/// Startup config snapshot. Populated from `Args` in `main.rs` and mounted on the router.
#[derive(Clone, Serialize)]
pub struct DoctorInfo {
    /// Listen address (`--bind` value).
    pub bind: String,
    /// Backend type: `"git2"` | `"in_memory"`.
    pub blueprint_backend: String,
    /// Git backend root (Git2 only). `None` for InMemory.
    pub blueprint_store_root: Option<String>,
    /// `--blueprint-ref-base` (= base dir for `$agent_md` / `$file` expansion).
    pub blueprint_ref_base: Option<String>,
    /// `--enable-enhance-flow` on/off.
    pub enhance_flow_enabled: bool,
    /// Fresh-launch migration policy for deprecated `profile.worker_binding`.
    pub legacy_worker_binding_policy: mlua_swarm::LegacyWorkerBindingPolicy,
    /// Seed blueprint id (= combined mode default).
    pub seed_blueprint_id: String,
    /// Server-wide [`mlua_swarm::core::config::CheckPolicy`] resolved
    /// from CLI flag > config file > built-in default (`Warn`). See
    /// `mlua_swarm_server::config::ResolvedConfig.check_policy` for the
    /// full cascade. Serialised as snake_case (`"silent"` / `"warn"` /
    /// `"strict"`).
    pub check_policy: mlua_swarm::core::config::CheckPolicy,
}

#[derive(Clone)]
struct DoctorState {
    info: Arc<DoctorInfo>,
    store: Arc<dyn BlueprintStore>,
}

/// Builds the `/v1/doctor` router. `info` is the (immutable) startup snapshot
/// to serve; `store` is used to peek the registered Blueprint id count/list.
pub fn build_doctor_router(info: DoctorInfo, store: Arc<dyn BlueprintStore>) -> Router {
    let state = DoctorState {
        info: Arc::new(info),
        store,
    };
    Router::new()
        .route("/v1/doctor", get(doctor_get))
        .with_state(state)
}

#[derive(Serialize)]
struct DoctorResponse {
    #[serde(flatten)]
    info: DoctorInfo,
    /// Registered BP id list (best-effort; currently returns empty for the InMemory backend).
    registered_blueprint_ids: Vec<String>,
    registered_blueprint_count: usize,
}

async fn doctor_get(State(state): State<DoctorState>) -> Json<DoctorResponse> {
    // `store.list_ids()` applies the archive filter (archived ids are excluded by default).
    // The InMemory backend is expected to return Ok(vec![]).
    let mut ids: Vec<String> = state
        .store
        .list_ids()
        .await
        .map(|v| v.into_iter().map(|id| id.to_string()).collect())
        .unwrap_or_default();
    ids.sort();
    let count = ids.len();
    Json(DoctorResponse {
        info: (*state.info).clone(),
        registered_blueprint_ids: ids,
        registered_blueprint_count: count,
    })
}
