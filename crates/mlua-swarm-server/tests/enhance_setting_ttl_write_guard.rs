//! `POST` / `PUT /v1/enhance-settings` refuse `ttl_secs: 0` before storing
//! anything.
//!
//! `EnhanceSettingInput.ttl_secs` is the wall-clock ceiling on one enhance
//! epoch (`EnhanceApplication::dispatch_one` applies it as
//! `tokio::time::timeout`). A zero duration elapses on its first poll, so a
//! stored `0` does not mean "unbounded" — it means every epoch this setting
//! drives is aborted before its first step. The dispatcher already refuses
//! it, but by then the author is gone and the failure surfaces as a
//! rejected issue rather than as an answer to the request that caused it.
//!
//! These pin the earlier of the two checks, and pin that it runs before
//! `into_ref` — this module writes commit-then-K-V, so a `400` that fired
//! after the Blueprint commit would leave half a setting behind.

use mlua_swarm::blueprint::store::InMemoryBlueprintStore;
use mlua_swarm::blueprint::{
    current_schema_version, AgentDef, AgentKind, Blueprint, BlueprintMetadata, CompilerHints,
    CompilerStrategy,
};
use mlua_swarm::store::enhance_setting::{EnhanceSettingStore, InMemoryEnhanceSettingStore};
use serde_json::json;
use std::sync::Arc;

/// Minimal one-step Blueprint — the setting's payload has to deserialize,
/// but nothing here compiles or runs it.
fn one_step_blueprint() -> Blueprint {
    Blueprint {
        schema_version: current_schema_version(),
        id: "enhance-ttl-guard-bp".into(),
        flow: serde_json::from_value(json!({
            "kind": "step",
            "ref": "agent-a",
            "in": {"op": "lit", "value": "hello"},
            "out": {"op": "path", "at": "$.a"},
        }))
        .expect("flow parse"),
        agents: vec![AgentDef {
            name: "agent-a".into(),
            kind: AgentKind::RustFn,
            spec: json!({"fn_id": mlua_swarm::worker::baseline::AG_IDENTITY}),
            profile: None,
            meta: None,
            runner: None,
            runner_ref: None,
            verdict: None,
            lints: None,
        }],
        operators: vec![],
        metas: vec![],
        hints: CompilerHints::default(),
        strategy: CompilerStrategy::default(),
        metadata: BlueprintMetadata::default(),
        spawner_hints: Default::default(),
        default_agent_kind: AgentKind::Operator,
        default_operator_kind: None,
        default_init_ctx: None,
        default_agent_ctx: None,
        default_context_policy: None,
        projection_placement: None,
        audits: vec![],
        degradation_policy: None,
        runners: vec![],
        default_runner: None,
        subprocesses: vec![],
        check_policy: None,
        blueprint_ref_includes: Vec::new(),
    }
}

fn setting_body(id: &str, ttl_secs: u64) -> serde_json::Value {
    json!({
        "id": id,
        "blueprint": one_step_blueprint(),
        "ttl_secs": ttl_secs,
    })
}

struct Harness {
    base: String,
    setting_store: Arc<InMemoryEnhanceSettingStore>,
}

/// Same shape as the other integration tests in this directory: a real
/// listener on an ephemeral port, driven with `reqwest`.
async fn harness() -> Harness {
    let setting_store = Arc::new(InMemoryEnhanceSettingStore::new());
    let bp_store = Arc::new(InMemoryBlueprintStore::new());
    let router = mlua_swarm_server::build_enhance_settings_router(setting_store.clone(), bp_store);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    Harness {
        base: format!("http://{addr}"),
        setting_store,
    }
}

async fn send(
    base: &str,
    method: reqwest::Method,
    path: &str,
    body: serde_json::Value,
) -> reqwest::StatusCode {
    reqwest::Client::new()
        .request(method, format!("{base}{path}"))
        .json(&body)
        .send()
        .await
        .expect("request")
        .status()
}

#[tokio::test]
async fn post_refuses_a_zero_ttl_and_stores_nothing() {
    let h = harness().await;
    let status = send(
        &h.base,
        reqwest::Method::POST,
        "/v1/enhance-settings",
        setting_body("zero-ttl", 0),
    )
    .await;

    assert_eq!(
        status,
        reqwest::StatusCode::BAD_REQUEST,
        "a setting whose every epoch would abort on its first poll must not be storable"
    );
    let stored = h.setting_store.list().await.expect("list");
    assert!(
        stored.is_empty(),
        "the guard must run before into_ref, so a refused setting leaves neither the K-V \
         row nor a committed Blueprint behind; got: {stored:?}"
    );
}

#[tokio::test]
async fn put_refuses_a_zero_ttl() {
    let h = harness().await;
    // Seed a runnable setting first, so the PUT under test is an update of
    // something valid rather than a create in disguise.
    let created = send(
        &h.base,
        reqwest::Method::POST,
        "/v1/enhance-settings",
        setting_body("live", 60),
    )
    .await;
    assert_eq!(
        created,
        reqwest::StatusCode::CREATED,
        "seed must be accepted"
    );

    let status = send(
        &h.base,
        reqwest::Method::PUT,
        "/v1/enhance-settings/live",
        setting_body("live", 0),
    )
    .await;
    assert_eq!(
        status,
        reqwest::StatusCode::BAD_REQUEST,
        "an update must not turn a runnable setting into one that can never run"
    );

    let still = h
        .setting_store
        .get(&mlua_swarm::store::enhance_setting::EnhanceSettingId::new(
            "live".to_string(),
        ))
        .await
        .expect("the seeded setting must survive a refused update");
    assert_eq!(
        still.ttl_secs, 60,
        "the refused update must not have overwritten the stored ceiling"
    );
}

#[tokio::test]
async fn a_positive_ttl_is_accepted() {
    // The counter-case that keeps the guard honest: it must reject zero
    // only, not every ceiling it is handed.
    let h = harness().await;
    let status = send(
        &h.base,
        reqwest::Method::POST,
        "/v1/enhance-settings",
        setting_body("ok", 1),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CREATED);
    let stored = h.setting_store.list().await.expect("list");
    assert_eq!(stored.len(), 1, "a valid setting must still be stored");
}
