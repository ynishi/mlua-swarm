//! GH #76 HTTP wire — `POST /v1/worker/submit?verdict=skip` HTTP wire tests.
//!
//! Exercises the opt-in `?verdict=<pass|blocked|skip>` query param added
//! in HTTP wire over a real HTTP round trip (mirrors `verdict_contract.rs`'s
//! pattern rather than introducing a new `tower::ServiceExt::oneshot`
//! dependency this crate does not otherwise carry). Wire-shape only:
//! the engine-level "flow-continuation without binding write" invariant
//! is already covered by Skip tier's engine tests
//! (`submit_worker_result_trusted_skip_outcome_records_final_ok_true_with_sentinel`,
//! `dispatcher_folds_skip_sentinel_into_skip_outcome`, and siblings in
//! `src/core/engine.rs`) — this file asserts the boundary contract
//! `worker_submit`'s query-param parser produces:
//!
//! - `verdict=skip` → engine sees `SubmitOutcome::Skip` → `Final.ok=true`
//!   AND `last_result` wraps the body in the reserved `__mse_skip`
//!   sentinel (matches `wrap_skip_marker`'s shape — the exact wire form
//!   the dispatcher folds back into `DispatchOutcome::Skip`, so the
//!   downstream binding-write short-circuit fires).
//! - Absent `verdict` param preserves the pre-#76 byte-for-byte wire
//!   (`ok=true|None → Pass`, `ok=false → Blocked`).
//! - Conflicting `verdict=skip&ok=false` → 400 (opt-in Skip is
//!   `ok=true`-only per plan.md Invariants).
//! - Invalid `verdict` value → 400 naming the valid set.

use mlua_swarm::core::config::EngineCfg;
use mlua_swarm::core::engine::Engine;
use mlua_swarm::core::state::{
    is_skip_marker, unwrap_skip_marker, CapTokenRecord, TaskSpec, TaskState,
};
use mlua_swarm::{CapToken, Role, StepId};

/// Seeds a `Pending` task bound to `agent`, mints a short worker handle
/// for it, and returns the handle. Mirror of `verdict_contract.rs`'s
/// helper of the same name — an integration test cannot reach a
/// `#[cfg(test)]`-gated helper in the library crate.
async fn seed_task_with_handle(engine: &Engine, task_id: &StepId, agent: &str) -> String {
    let handle = format!("wh-{}", mlua_swarm::types::secure_hex(4));
    let task_id = task_id.clone();
    let agent = agent.to_string();
    let handle_clone = handle.clone();
    engine
        .with_state("test.seed_task_with_handle", move |s| {
            let task = TaskState::new(
                task_id.clone(),
                TaskSpec {
                    agent: agent.clone(),
                    initial_directive: serde_json::json!("x"),
                    step_ctx: None,
                    check_policy: None,
                },
            );
            s.tasks.insert(task_id.clone(), task);
            let token = CapToken {
                agent_id: agent,
                role: Role::Worker,
                scopes: vec!["*".to_string()],
                issued_at: 0,
                expire_at: u64::MAX,
                max_uses: None,
                nonce: format!("test-nonce-{task_id}"),
                sig_hex: String::new(),
            };
            let fp = token.fingerprint();
            s.tokens.insert(
                fp.clone(),
                CapTokenRecord {
                    token,
                    uses_left: None,
                    revoked: false,
                    task_id: Some(task_id),
                },
            );
            s.worker_handles.insert(handle_clone, fp);
        })
        .await
        .expect("seed_task_with_handle");
    handle
}

/// Starts a real HTTP server for `engine` on an ephemeral port and
/// returns its base URL (`http://127.0.0.1:<port>`).
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

/// A `POST /v1/worker/submit?verdict=skip` is accepted with 204 and the
/// engine records a `Final { ok: true }` — the "flow-continuation" wire
/// half of the Skip contract (see Skip tier's engine tests for the
/// dispatcher-side sentinel-fold). Named per HTTP wire subtask.md's Verify list.
#[tokio::test]
async fn post_worker_submit_verdict_skip_produces_skip_outcome_and_continues_flow() {
    let engine = Engine::new(EngineCfg::default());
    let task_id = StepId::new();
    let handle = seed_task_with_handle(&engine, &task_id, "analyst").await;
    let base_url = spawn_server(engine.clone()).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/v1/worker/submit?verdict=skip"))
        .header("Authorization", format!("Bearer {handle}"))
        .body("SKIP")
        .send()
        .await
        .expect("request");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NO_CONTENT,
        "verdict=skip is a successful completion (wire ack)"
    );

    // Final.ok = true (flow-continuation, not error propagation).
    // Seed-based tests bypass `dispatch_attempt_with` (which bumps
    // `TaskState.attempt` to 1), so the fresh task's attempt is still 0
    // when `worker_submit`'s `task_attempt` lookup fires — the Final
    // lands under `(task_id, 0)`.
    let tail = engine.output_tail(&task_id, 0).await;
    let (_, final_ok) = tail
        .iter()
        .rev()
        .find_map(|ev| match ev {
            mlua_swarm::OutputEvent::Final { content, ok } => Some((content.clone(), *ok)),
            _ => None,
        })
        .expect("Final present after Skip submit");
    assert!(
        final_ok,
        "Skip records Final.ok = true (flow continues, no error propagation)"
    );
}

/// The `last_result` written by a `verdict=skip` submit wraps the body
/// in the reserved `__mse_skip` sentinel — the exact shape the
/// dispatcher's Skip-fold path recognizes (see Skip tier's
/// `dispatcher_folds_skip_sentinel_into_skip_outcome`), which is what
/// short-circuits the downstream `out` binding write. Asserting the
/// wire-level marker here is the process-boundary complement to the
/// engine-level dispatcher assertion.
#[tokio::test]
async fn post_worker_submit_verdict_skip_downstream_binding_unresolved() {
    let engine = Engine::new(EngineCfg::default());
    let task_id = StepId::new();
    let handle = seed_task_with_handle(&engine, &task_id, "analyst").await;
    let base_url = spawn_server(engine.clone()).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/v1/worker/submit?verdict=skip"))
        .header("Authorization", format!("Bearer {handle}"))
        .body("SKIP")
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);

    let last_result = engine
        .with_state("test.read_last_result", {
            let task_id = task_id.clone();
            move |s| s.tasks.get(&task_id).and_then(|t| t.last_result.clone())
        })
        .await
        .expect("read last_result");
    let last_result = last_result.expect("last_result present after submit");
    assert!(
        is_skip_marker(&last_result),
        "verdict=skip wraps last_result in the __mse_skip sentinel, got: {last_result}"
    );
    assert_eq!(
        unwrap_skip_marker(&last_result),
        Some(serde_json::json!("SKIP")),
        "sentinel carries the raw body as its inner value"
    );
}

/// Anything other than `pass` / `blocked` / `skip` in the query param
/// is a 400 that names the valid set — the query-param parser's own
/// contract, exercised over the wire.
#[tokio::test]
async fn post_worker_submit_verdict_invalid_returns_400() {
    let engine = Engine::new(EngineCfg::default());
    let task_id = StepId::new();
    let handle = seed_task_with_handle(&engine, &task_id, "analyst").await;
    let base_url = spawn_server(engine).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/v1/worker/submit?verdict=bogus"))
        .header("Authorization", format!("Bearer {handle}"))
        .body("payload")
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.expect("json body");
    let error = body["error"].as_str().expect("error string");
    assert!(
        error.contains("pass") && error.contains("blocked") && error.contains("skip"),
        "error should enumerate the valid tier set: {error}"
    );
}

/// `verdict=skip` combined with an explicit `ok=false` is a
/// conflicting signal — Skip is the "not applicable, continue"
/// (ok=true) tier by construction, so the wire rejects the
/// contradiction rather than silently privileging one side.
#[tokio::test]
async fn post_worker_submit_verdict_skip_with_ok_false_returns_400() {
    let engine = Engine::new(EngineCfg::default());
    let task_id = StepId::new();
    let handle = seed_task_with_handle(&engine, &task_id, "analyst").await;
    let base_url = spawn_server(engine).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/v1/worker/submit?verdict=skip&ok=false"))
        .header("Authorization", format!("Bearer {handle}"))
        .body("SKIP")
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.expect("json body");
    let error = body["error"].as_str().expect("error string");
    assert!(
        error.contains("conflict") || error.contains("conflicting"),
        "error should describe the ok/verdict conflict: {error}"
    );
}

/// Regression guard: without `verdict` in the query, the pre-#76
/// wire shape is preserved byte-for-byte — plain `POST
/// /v1/worker/submit` with no query params completes as
/// `SubmitOutcome::Pass` (Final.ok=true, `last_result` is the raw body,
/// no sentinel wrapping).
#[tokio::test]
async fn post_worker_submit_no_verdict_param_preserves_ok_true_pass_behavior() {
    let engine = Engine::new(EngineCfg::default());
    let task_id = StepId::new();
    let handle = seed_task_with_handle(&engine, &task_id, "analyst").await;
    let base_url = spawn_server(engine.clone()).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/v1/worker/submit"))
        .header("Authorization", format!("Bearer {handle}"))
        .body("PASS")
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);

    let last_result = engine
        .with_state("test.read_last_result", {
            let task_id = task_id.clone();
            move |s| s.tasks.get(&task_id).and_then(|t| t.last_result.clone())
        })
        .await
        .expect("read last_result");
    assert_eq!(
        last_result,
        Some(serde_json::json!("PASS")),
        "no verdict param: raw body flows through unwrapped (pre-#76 shape)"
    );
    assert!(
        !is_skip_marker(&last_result.expect("checked above")),
        "no verdict param must NOT wrap in skip marker"
    );

    // Seed-based tests bypass `dispatch_attempt_with` (attempt=0 for a
    // fresh Pending task), so the Final lands under `(task_id, 0)`.
    let tail = engine.output_tail(&task_id, 0).await;
    let (_, final_ok) = tail
        .iter()
        .rev()
        .find_map(|ev| match ev {
            mlua_swarm::OutputEvent::Final { content, ok } => Some((content.clone(), *ok)),
            _ => None,
        })
        .expect("Final present");
    assert!(
        final_ok,
        "no verdict param, no ok override: Final.ok = true"
    );
}
