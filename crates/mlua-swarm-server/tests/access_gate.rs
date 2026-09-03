//! L0 perimeter coverage over the *merged* router shape (GH #101).
//!
//! `mse serve` merges five sub-routers (issues / blueprints / enhance-log /
//! enhance-settings / doctor) into the core app *after* the server crate's
//! builder returns, and the access gate must wrap that merged result — a
//! gate attached inside the core builder would silently leave those five
//! outside the perimeter. This test reproduces exactly that merge shape and
//! pins one route per merged sub-router to 401-without-header /
//! non-401-with-header, plus the healthz exemption.

use std::sync::Arc;

use axum::{body::Body, http::Request, http::StatusCode, routing::get, Router};
use mlua_swarm::blueprint::store::InMemoryBlueprintStore;
use mlua_swarm::store::enhance_log::InMemoryEnhanceLogStore;
use mlua_swarm::store::enhance_setting::InMemoryEnhanceSettingStore;
use mlua_swarm::store::issue::InMemoryIssueStore;
use mlua_swarm_server::doctor::{build_doctor_router, DoctorInfo};
use mlua_swarm_server::{
    apply_access_gate, build_blueprints_router, build_enhance_log_router,
    build_enhance_settings_router, build_issues_router, AccessGate, ACCESS_TOKEN_HEADER,
};
use tower::ServiceExt;

const TOKEN: &str = "test-access-token";

/// The serve.rs merge shape: a stand-in core route + the five late-merged
/// sub-routers, gated as one.
fn merged_app() -> Router {
    let bp_store = Arc::new(InMemoryBlueprintStore::new());
    let doctor_info = DoctorInfo {
        server_version: "test".into(),
        bind: "127.0.0.1:0".into(),
        blueprint_backend: "in_memory".into(),
        blueprint_store_root: None,
        blueprint_ref_base: None,
        enhance_flow_enabled: false,
        legacy_worker_binding_policy: Default::default(),
        seed_blueprint_id: "main".into(),
        check_policy: Default::default(),
    };
    let app = Router::new()
        .route("/v1/healthz", get(|| async { "ok" }))
        .route("/v1/status", get(|| async { "{}" }));
    let app = app
        .merge(build_issues_router(Arc::new(InMemoryIssueStore::new())))
        .merge(build_blueprints_router(bp_store.clone()))
        .merge(build_enhance_log_router(Arc::new(
            InMemoryEnhanceLogStore::new(),
        )))
        .merge(build_enhance_settings_router(
            Arc::new(InMemoryEnhanceSettingStore::new()),
            bp_store.clone(),
        ))
        .merge(build_doctor_router(doctor_info, bp_store));
    apply_access_gate(app, Some(AccessGate::new(TOKEN)))
}

async fn status_of(app: Router, path: &str, with_token: bool) -> StatusCode {
    let mut req = Request::builder().uri(path);
    if with_token {
        req = req.header(ACCESS_TOKEN_HEADER, TOKEN);
    }
    app.oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap()
        .status()
}

/// One route per merged sub-router + the core stand-in. GET routes on
/// empty stores: the point is 401 vs anything-but-401, not the payload.
const GATED_ROUTES: &[&str] = &[
    "/v1/status",               // core router stand-in
    "/v1/issues/nonexistent",   // issues sub-router
    "/v1/blueprints/nope/head", // blueprints sub-router
    "/v1/enhance/log",          // enhance-log sub-router
    "/v1/enhance-settings", // enhance-settings sub-router (list route may 404/405 — still not 401)
    "/v1/doctor",           // doctor sub-router
];

#[tokio::test]
async fn every_merged_sub_router_is_behind_the_gate() {
    for path in GATED_ROUTES {
        let without = status_of(merged_app(), path, false).await;
        assert_eq!(
            without,
            StatusCode::UNAUTHORIZED,
            "{path} must be 401 without the access token"
        );
        let with = status_of(merged_app(), path, true).await;
        assert_ne!(
            with,
            StatusCode::UNAUTHORIZED,
            "{path} must pass the gate with the access token"
        );
    }
}

#[tokio::test]
async fn healthz_stays_open() {
    assert_eq!(
        status_of(merged_app(), "/v1/healthz", false).await,
        StatusCode::OK
    );
}
