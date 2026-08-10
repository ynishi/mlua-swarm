//! `POST /v1/sessions` — the `ttl_enforced` reply field.
//!
//! `82d9da9` ("stop expiring operator tokens") made `Engine::verify_token`'s
//! expiry check role-conditional, which left `AttachReq.ttl_secs` accepted
//! but inert for `role: "operator"` and enforced for every other role. The
//! field is not deprecated — three of the four roles it accepts still fail
//! verification past `expire_at` — so what the route owes a caller is to say
//! which of the two it just did, in the response to the request that set it.
//!
//! (Not the reason it would be tempting to give: a Worker token minted by
//! `POST /v1/sessions` cannot reach `/v1/worker/*` at all, because
//! `Engine::attach` binds no `task_id` and the ownership gate refuses it
//! before `expire_at` is consulted. That is fail-closed. See
//! `AttachReq::ttl_secs`.)
//!
//! Three tests, over a real HTTP round trip (the harness shape this crate's
//! other wire tests use — see `worker_submit_skip.rs`). Two pin today's
//! answer per role. The third is the one that matters structurally: it
//! compares what the route reports against what `Engine::verify_token`
//! actually does to an expired token of that role, so the two copies of the
//! exemption condition cannot drift apart silently. Literal-only assertions
//! would survive a widened exemption and leave the route reporting
//! `ttl_enforced: true` for a session nothing expires — which is worse than
//! reporting nothing.

use mlua_swarm::core::config::EngineCfg;
use mlua_swarm::core::engine::Engine;
use mlua_swarm::core::errors::EngineError;
use mlua_swarm::types::{Role, Verb};
use std::time::Duration;

/// Starts a real HTTP server for `engine` on an ephemeral port and
/// returns its base URL. Mirror of the helper in `worker_submit_skip.rs`
/// (an integration test cannot reach another test binary's helper).
async fn spawn_server(engine: Engine) -> String {
    let router = mlua_swarm_server::build_router(engine);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    format!("http://{addr}")
}

/// `POST /v1/sessions` with `role` and a 600s TTL; returns the decoded
/// reply body.
async fn attach(base_url: &str, role: &str) -> serde_json::Value {
    let resp = reqwest::Client::new()
        .post(format!("{base_url}/v1/sessions"))
        .json(&serde_json::json!({
            "agent_id": "ut-agent",
            "role": role,
            "ttl_secs": 600,
        }))
        .send()
        .await
        .expect("request");
    assert!(
        resp.status().is_success(),
        "attach as {role} must succeed, got {}",
        resp.status()
    );
    resp.json().await.expect("reply is JSON")
}

/// An Operator session reports its TTL as not enforced — the expiry
/// exemption in `Engine::verify_token` means nothing will ever read the
/// `expire_at` this request stamped.
#[tokio::test]
async fn attach_as_operator_reports_ttl_not_enforced() {
    let base_url = spawn_server(Engine::new(EngineCfg::default())).await;
    let body = attach(&base_url, "operator").await;

    assert_eq!(
        body["ttl_enforced"],
        serde_json::Value::Bool(false),
        "an Operator token verifies past its expire_at, so ttl_secs gates nothing: {body}"
    );
    // The session itself is unaffected — reporting the TTL's status must
    // not change what the route hands back.
    assert!(
        body["session_id"].as_str().is_some_and(|s| !s.is_empty()),
        "the attach must still mint a usable session id: {body}"
    );
    assert_eq!(body["role"], "operator");
}

/// Every other role keeps the expiry check, so the same request reports
/// the TTL as enforced.
#[tokio::test]
async fn attach_as_worker_reports_ttl_enforced() {
    let base_url = spawn_server(Engine::new(EngineCfg::default())).await;

    for role in ["worker", "senior", "observer"] {
        let body = attach(&base_url, role).await;
        assert_eq!(
            body["ttl_enforced"],
            serde_json::Value::Bool(true),
            "role={role} still fails verification past expire_at, so ttl_secs is enforced: {body}"
        );
    }
}

/// Ask the engine what it actually does to an expired token of `role`.
///
/// `Engine::attach` with a zero TTL mints a token whose `expire_at` is
/// `now`, and `CapToken::is_expired` is `now >= expire_at`, so the token is
/// expired the instant it exists — no sleeping, no clock control.
///
/// The classification is deliberately "was it rejected *for expiry*",
/// not "was it rejected". `verify_token` checks signature, then expiry,
/// then the role×verb gate, then uses. Only `TokenExpired` means the expiry
/// check bit; anything else — `Ok`, or a `RoleViolation` from a later step —
/// means execution got *past* expiry, which is precisely what "not enforced"
/// means. That keeps this probe independent of which `Verb` we happen to
/// pick, so a change to the role gate cannot masquerade as a change to TTL
/// enforcement.
async fn engine_enforces_expiry_for(engine: &Engine, role: Role) -> bool {
    let token = engine
        .attach(format!("ttl-probe-{role:?}"), role, Duration::from_secs(0))
        .await
        .expect("attach mints a token for every role");
    matches!(
        engine.verify_token(&token, Verb::ReadTaskState).await,
        Err(EngineError::TokenExpired)
    )
}

/// The drift detector.
///
/// `sessions_attach` computes `ttl_enforced` from its own copy of the
/// exemption condition (`role != Role::Operator`), duplicating the one in
/// `Engine::verify_token`. This test is what makes that duplication safe:
/// the expected value is not a literal, it is what the engine is observed
/// doing to an expired token of the same role in the same process. Either
/// copy moving on its own fails it.
///
/// Concretely, the change this exists to catch: someone widens the
/// exemption to also skip expiry for `Role::Observer` (a plausible
/// "observer is read-only" argument). The engine probe for `observer` flips
/// to `false`, the route keeps reporting `true`, and this assert fires.
/// Without it, `POST /v1/sessions {"role":"observer","ttl_secs":600}` would
/// go on promising a bound that no longer exists.
#[tokio::test]
async fn ttl_enforced_matches_what_the_engine_actually_does_per_role() {
    let engine = Engine::new(EngineCfg::default());
    let base_url = spawn_server(engine.clone()).await;

    for (wire_role, role) in [
        ("operator", Role::Operator),
        ("worker", Role::Worker),
        ("senior", Role::Senior),
        ("observer", Role::Observer),
    ] {
        let engine_side = engine_enforces_expiry_for(&engine, role).await;
        let body = attach(&base_url, wire_role).await;
        assert_eq!(
            body["ttl_enforced"],
            serde_json::Value::Bool(engine_side),
            "role={wire_role}: POST /v1/sessions reports ttl_enforced={}, but \
             Engine::verify_token {} an expired {role:?} token. The route's copy of the \
             expiry exemption has drifted from the engine's — fix whichever one moved, \
             and note that the reported value is what a caller acts on: {body}",
            body["ttl_enforced"],
            if engine_side { "rejects" } else { "accepts" },
        );
    }
}
