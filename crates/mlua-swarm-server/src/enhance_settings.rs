//! `EnhanceSetting` HTTP CRUD (= K-V store entry + `BPStore` commit
//! orchestration).
//!
//! - `POST   /v1/enhance-settings`      — create (body = `EnhanceSettingInput`, includes data)
//! - `GET    /v1/enhance-settings/:id`  — read (= `EnhanceSetting` = Ref form)
//! - `PUT    /v1/enhance-settings/:id`  — update (body = `EnhanceSettingInput`)
//! - `DELETE /v1/enhance-settings/:id`  — delete (= K-V only; `BPStore` history remains)
//! - `GET    /v1/enhance-settings`      — list ids
//!
//! The POST/PUT input form is [`EnhanceSettingInput`] (= Blueprint embedded
//! with data). Inside the server, `into_ref()` splits it into (`Blueprint`,
//! `EnhanceSetting` Ref form); the BP is committed via `BPStore.write_new`
//! first, then `setting_store.put` (= BP first, setting second, so a failed
//! BP commit does not leave an orphan setting). Response / read return
//! `EnhanceSetting` (= `BlueprintId` Ref + `ttl_secs` + meta).
//!
//! Current scope:
//! - Consecutive PUTs of the same content produce duplicate commits in BP
//!   history (= idempotency is a carry).

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use mlua_swarm::blueprint::store::{blueprint_version, BlueprintStore, CommitMetadata};
use mlua_swarm::blueprint::Blueprint;
use mlua_swarm::enhance::{EnhanceSetting, EnhanceSettingInput};
use mlua_swarm::store::enhance_setting::{
    EnhanceSettingId, EnhanceSettingStore, EnhanceSettingStoreError,
};
use std::sync::Arc;

/// Router state for the `/v1/enhance-settings*` handlers.
#[derive(Clone)]
pub struct EnhanceSettingsState {
    /// K-V backend for `EnhanceSetting` Ref-form records.
    pub setting_store: Arc<dyn EnhanceSettingStore>,
    /// Blueprint store the embedded Blueprint is committed to before the K-V write.
    pub bp_store: Arc<dyn BlueprintStore>,
}

/// Builds the `/v1/enhance-settings*` router. See the module doc for the
/// commit-then-K-V-write ordering and the POST/PUT input shape.
pub fn build_enhance_settings_router(
    setting_store: Arc<dyn EnhanceSettingStore>,
    bp_store: Arc<dyn BlueprintStore>,
) -> Router {
    let state = EnhanceSettingsState {
        setting_store,
        bp_store,
    };
    Router::new()
        .route(
            "/v1/enhance-settings",
            get(list_settings).post(post_setting),
        )
        .route(
            "/v1/enhance-settings/:id",
            get(get_setting).put(put_setting).delete(delete_setting),
        )
        .with_state(state)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Commits `setting.blueprint` to the `BPStore`. Idempotent (= if a head with
/// the same `ContentHash` already exists, skip). If the BP commit fails,
/// early-returns without calling `setting_store.put`.
async fn commit_blueprint(
    bp_store: &Arc<dyn BlueprintStore>,
    blueprint: &Blueprint,
    rationale: String,
) -> Result<(), (StatusCode, String)> {
    let bp_id = blueprint.id.clone();
    let v = blueprint_version(blueprint).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("bp version: {e}"),
        )
    })?;

    // Idempotency: skip when head's version already matches (prevents duplicate commits of the same content).
    if let Ok(traced) = bp_store.read_head(&bp_id).await {
        if traced.trace.version == v {
            return Ok(());
        }
    }

    let mut meta = CommitMetadata::seed(bp_id.clone(), v, now_ms());
    meta.rationale = rationale;
    bp_store
        .write_new(&bp_id, blueprint, &[], meta)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("bp commit: {e}")))?;
    Ok(())
}

async fn list_settings(
    State(state): State<EnhanceSettingsState>,
) -> Result<Json<Vec<String>>, (StatusCode, String)> {
    let ids = state
        .setting_store
        .list()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ids.into_iter().map(|id| id.0).collect()))
}

async fn post_setting(
    State(state): State<EnhanceSettingsState>,
    Json(input): Json<EnhanceSettingInput>,
) -> Result<(StatusCode, Json<EnhanceSetting>), (StatusCode, String)> {
    let (blueprint, setting) = input.into_ref();
    let rationale = format!(
        "enhance-setting POST id={} blueprint_id={}",
        setting.id, setting.blueprint_id
    );
    commit_blueprint(&state.bp_store, &blueprint, rationale).await?;
    state
        .setting_store
        .put(&EnhanceSettingId::new(setting.id.clone()), setting.clone())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok((StatusCode::CREATED, Json(setting)))
}

async fn get_setting(
    State(state): State<EnhanceSettingsState>,
    Path(id): Path<String>,
) -> Result<Json<EnhanceSetting>, (StatusCode, String)> {
    let setting = state
        .setting_store
        .get(&EnhanceSettingId::new(id))
        .await
        .map_err(|e| match e {
            EnhanceSettingStoreError::NotFound(id) => (
                StatusCode::NOT_FOUND,
                format!("enhance setting not found: {id}"),
            ),
            EnhanceSettingStoreError::Other(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        })?;
    Ok(Json(setting))
}

async fn put_setting(
    State(state): State<EnhanceSettingsState>,
    Path(id): Path<String>,
    Json(input): Json<EnhanceSettingInput>,
) -> Result<Json<EnhanceSetting>, (StatusCode, String)> {
    if input.id != id {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("path id {id:?} != body id {:?}", input.id),
        ));
    }
    let (blueprint, setting) = input.into_ref();
    let rationale = format!(
        "enhance-setting PUT id={} blueprint_id={}",
        setting.id, setting.blueprint_id
    );
    commit_blueprint(&state.bp_store, &blueprint, rationale).await?;
    state
        .setting_store
        .put(&EnhanceSettingId::new(id), setting.clone())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(setting))
}

async fn delete_setting(
    State(state): State<EnhanceSettingsState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    state
        .setting_store
        .delete(&EnhanceSettingId::new(id))
        .await
        .map_err(|e| match e {
            EnhanceSettingStoreError::NotFound(id) => (
                StatusCode::NOT_FOUND,
                format!("enhance setting not found: {id}"),
            ),
            EnhanceSettingStoreError::Other(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        })?;
    Ok(StatusCode::NO_CONTENT)
}
