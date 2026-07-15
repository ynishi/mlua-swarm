//! GH #50 Subtask 2 — submit-time verdict contract enforcement, exercised
//! over a real HTTP round trip.
//!
//! Acceptance Criterion #7 requires the process-boundary HTTP contract to
//! be under test (not just handler-level unit behavior — see
//! `crates/mlua-swarm-server/src/worker.rs`'s own
//! `#[cfg(test)] mod tests` GH #50 section for the fast in-process
//! counterpart). This file starts a real `axum::serve` on an ephemeral
//! port and drives it with `reqwest`, mirroring the established pattern
//! already in this crate
//! (`crate::projection::tests::old_ctx_route_returns_404_not_found_by_router`)
//! rather than introducing a new `tower::ServiceExt::oneshot` dependency
//! this crate does not otherwise carry.
//!
//! Seeding a task + registering a `VerdictContract` bypasses `POST
//! /v1/tasks` + `TaskLaunchService::launch` (compiling a live Blueprint's
//! `AgentDef.verdict` into `CompiledAgentTable.verdict_contracts` and
//! wiring THAT into `Engine::register_verdict_contracts` is a follow-up —
//! that call site, `src/service/task_launch.rs`, sits outside this
//! subtask's file scope). Instead this seeds `EngineState` directly via
//! `Engine::with_state`, mirroring `worker.rs`'s own
//! `tests::seed_task_with_handle` helper (duplicated here — an
//! integration test cannot reach a `#[cfg(test)]`-gated helper in the
//! library crate) — the exact minimal state `worker_submit` /
//! `worker_artifact` need to resolve a Bearer handle to a task and agent.

use mlua_swarm::core::config::EngineCfg;
use mlua_swarm::core::engine::Engine;
use mlua_swarm::core::state::{CapTokenRecord, TaskSpec, TaskState};
use mlua_swarm::{CapToken, Role, StepId};
use mlua_swarm_schema::{VerdictChannel, VerdictContract};
use std::collections::HashMap;
use std::time::Duration;

/// Seeds a `Pending` task bound to `agent`, mints a short worker handle
/// for it, and returns the handle.
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

/// GH #51 — seeds a `Pending` task bound to `agent` and mints + registers
/// a properly HMAC-signed `Role::Worker` `CapToken` for it, returning the
/// token itself (not a handle). `POST /v1/worker/result`'s
/// `decode_worker_bearer` requires a full encoded `CapToken` — unlike
/// `worker_submit`/`worker_artifact`'s short-handle Bearer support (what
/// [`seed_task_with_handle`] mints), so this is a distinct helper rather
/// than a variant of that one.
async fn seed_task_with_token(engine: &Engine, task_id: &StepId, agent: &str) -> CapToken {
    let task_id_for_state = task_id.clone();
    let agent_for_state = agent.to_string();
    engine
        .with_state("test.seed_task_with_token", move |s| {
            let task = TaskState::new(
                task_id_for_state.clone(),
                TaskSpec {
                    agent: agent_for_state,
                    initial_directive: serde_json::json!("x"),
                    step_ctx: None,
                    check_policy: None,
                },
            );
            s.tasks.insert(task_id_for_state.clone(), task);
        })
        .await
        .expect("seed_task_with_token");
    let token = engine.signer().session(
        agent.to_string(),
        Role::Worker,
        vec!["*".to_string()],
        Duration::from_secs(600),
    );
    let fp = token.fingerprint();
    let record = CapTokenRecord::from_worker_token(token.clone(), task_id.clone());
    engine
        .with_state("test.register_token", move |s| {
            s.tokens.insert(fp, record);
        })
        .await
        .expect("register token");
    token
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

fn body_contract(values: &[&str]) -> VerdictContract {
    VerdictContract {
        channel: VerdictChannel::Body,
        values: values.iter().map(|v| v.to_string()).collect(),
    }
}

fn part_contract(values: &[&str]) -> VerdictContract {
    VerdictContract {
        channel: VerdictChannel::Part,
        values: values.iter().map(|v| v.to_string()).collect(),
    }
}

/// Case 1: a `channel: "body"` contract rejects a `worker_submit` body
/// outside its declared `values` with HTTP 422, echoing the expected
/// token set.
#[tokio::test]
async fn worker_submit_rejects_body_outside_contract_values() {
    let engine = Engine::new(EngineCfg::default());
    engine.register_verdict_contracts(HashMap::from([(
        "gate".to_string(),
        body_contract(&["PASS", "BLOCKED"]),
    )]));
    let task_id = StepId::new();
    let handle = seed_task_with_handle(&engine, &task_id, "gate").await;
    let base_url = spawn_server(engine).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/v1/worker/submit"))
        .header("Authorization", format!("Bearer {handle}"))
        .body("UNKNOWN")
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    let body: serde_json::Value = resp.json().await.expect("json body");
    let error = body["error"].as_str().expect("error string");
    assert!(
        error.contains("PASS") && error.contains("BLOCKED"),
        "error should echo declared values: {error}"
    );
}

/// Case 2: the same contract accepts a body that IS a member of `values`
/// — HTTP 204, and the value lands in the task's `last_result`.
#[tokio::test]
async fn worker_submit_accepts_body_inside_contract_values() {
    let engine = Engine::new(EngineCfg::default());
    engine.register_verdict_contracts(HashMap::from([(
        "gate".to_string(),
        body_contract(&["PASS", "BLOCKED"]),
    )]));
    let task_id = StepId::new();
    let handle = seed_task_with_handle(&engine, &task_id, "gate").await;
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
    assert_eq!(last_result, Some(serde_json::json!("PASS")));
}

/// Case 3: a `channel: "part"` contract rejects a
/// `worker_artifact?name=verdict` value outside `values` with HTTP 422.
#[tokio::test]
async fn worker_artifact_verdict_part_rejects_value_outside_contract() {
    let engine = Engine::new(EngineCfg::default());
    engine.register_verdict_contracts(HashMap::from([(
        "gate".to_string(),
        part_contract(&["PASS", "BLOCKED"]),
    )]));
    let task_id = StepId::new();
    let handle = seed_task_with_handle(&engine, &task_id, "gate").await;
    let base_url = spawn_server(engine).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/v1/worker/artifact?name=verdict"))
        .header("Authorization", format!("Bearer {handle}"))
        .body("UNKNOWN")
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
}

/// Case 4: a part named anything OTHER than `"verdict"` skips the gate
/// entirely — even with a `channel: "part"` contract declared, existing
/// (pre-GH-#50) `204` behavior is unchanged.
#[tokio::test]
async fn worker_artifact_non_verdict_part_skips_the_gate() {
    let engine = Engine::new(EngineCfg::default());
    engine.register_verdict_contracts(HashMap::from([(
        "gate".to_string(),
        part_contract(&["PASS", "BLOCKED"]),
    )]));
    let task_id = StepId::new();
    let handle = seed_task_with_handle(&engine, &task_id, "gate").await;
    let base_url = spawn_server(engine).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/v1/worker/artifact?name=notes"))
        .header("Authorization", format!("Bearer {handle}"))
        .body("anything at all")
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
}

/// Case 5 (opt-in regression guard): an agent with NO declared verdict
/// contract is unaffected — `worker_submit` returns 204 for an arbitrary
/// body, same as pre-GH-#50.
#[tokio::test]
async fn worker_submit_without_a_declared_contract_is_unaffected() {
    let engine = Engine::new(EngineCfg::default());
    // No `register_verdict_contracts` call at all for this agent.
    let task_id = StepId::new();
    let handle = seed_task_with_handle(&engine, &task_id, "undeclared-agent").await;
    let base_url = spawn_server(engine).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/v1/worker/submit"))
        .header("Authorization", format!("Bearer {handle}"))
        .body("anything at all, no contract to violate")
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
}

// ─── GH #51 — completion-time verdict-contract enforcement (follow-up to
// the above GH #50 submit-time cases): part-presence at completion, and
// gate coverage on `POST /v1/worker/result` (route 2 of 3) ────────────────

/// Route 1 (`POST /v1/worker/submit`), part-presence at completion: a
/// `channel: "part"` contract agent completes via a plain submit WITHOUT
/// ever staging a `"verdict"` part — rejected with HTTP 422 naming the
/// missing part, before the pre-GH-#51 bypass this closes (the old
/// submit-time gate only checked `channel: "body"`, never part
/// PRESENCE).
#[tokio::test]
async fn worker_submit_rejects_missing_verdict_part_when_channel_is_part() {
    let engine = Engine::new(EngineCfg::default());
    engine.register_verdict_contracts(HashMap::from([(
        "gate".to_string(),
        part_contract(&["PASS", "BLOCKED"]),
    )]));
    let task_id = StepId::new();
    let handle = seed_task_with_handle(&engine, &task_id, "gate").await;
    let base_url = spawn_server(engine).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/v1/worker/submit"))
        .header("Authorization", format!("Bearer {handle}"))
        .body("a full report, never staged as a verdict part")
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    let body: serde_json::Value = resp.json().await.expect("json body");
    let error = body["error"].as_str().expect("error string");
    assert!(
        error.contains("verdict"),
        "error should name the missing part: {error}"
    );
}

/// Route 1 — `ok=false` bypasses the completion-time check entirely,
/// regardless of channel or membership. A `channel: "body"` contract
/// declaring `["PASS", "BLOCKED"]` would reject `"UNKNOWN"` under
/// `ok=true` (Case 1 above); with `ok=false` the same value completes.
#[tokio::test]
async fn worker_submit_ok_false_bypasses_the_gate_regardless_of_value() {
    let engine = Engine::new(EngineCfg::default());
    engine.register_verdict_contracts(HashMap::from([(
        "gate".to_string(),
        body_contract(&["PASS", "BLOCKED"]),
    )]));
    let task_id = StepId::new();
    let handle = seed_task_with_handle(&engine, &task_id, "gate").await;
    let base_url = spawn_server(engine).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/v1/worker/submit?ok=false"))
        .header("Authorization", format!("Bearer {handle}"))
        .body("UNKNOWN")
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
}

/// Route 2 (`POST /v1/worker/result`), part-presence at completion: this
/// route called no gate at all pre-GH-#51 — a `channel: "part"` contract
/// agent completing here without ever staging `"verdict"` is now
/// rejected with HTTP 422 naming the missing part.
#[tokio::test]
async fn worker_result_rejects_missing_verdict_part_when_channel_is_part() {
    let engine = Engine::new(EngineCfg::default());
    engine.register_verdict_contracts(HashMap::from([(
        "gate".to_string(),
        part_contract(&["PASS", "BLOCKED"]),
    )]));
    let task_id = StepId::new();
    let token = seed_task_with_token(&engine, &task_id, "gate").await;
    let base_url = spawn_server(engine).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/v1/worker/result"))
        .header("Authorization", format!("Bearer {}", token.encode()))
        .json(&serde_json::json!({
            "task_id": task_id.as_str(),
            "value": "a full report, never staged as a verdict part",
            "ok": true,
        }))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    let body: serde_json::Value = resp.json().await.expect("json body");
    let error = body["error"].as_str().expect("error string");
    assert!(
        error.contains("verdict"),
        "error should name the missing part: {error}"
    );
}

/// Route 2 — a `channel: "body"` contract's completing value is NOT a
/// member of `values`: this route called no gate at all pre-GH-#51 —
/// closes the gap this subtask's acceptance criteria describes for
/// routes 2 and 3.
#[tokio::test]
async fn worker_result_rejects_body_value_outside_contract() {
    let engine = Engine::new(EngineCfg::default());
    engine.register_verdict_contracts(HashMap::from([(
        "gate".to_string(),
        body_contract(&["PASS", "BLOCKED"]),
    )]));
    let task_id = StepId::new();
    let token = seed_task_with_token(&engine, &task_id, "gate").await;
    let base_url = spawn_server(engine).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/v1/worker/result"))
        .header("Authorization", format!("Bearer {}", token.encode()))
        .json(&serde_json::json!({
            "task_id": task_id.as_str(),
            "value": "UNKNOWN",
            "ok": true,
        }))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    let body: serde_json::Value = resp.json().await.expect("json body");
    let error = body["error"].as_str().expect("error string");
    assert!(
        error.contains("PASS") && error.contains("BLOCKED"),
        "error should echo declared values: {error}"
    );
}

/// Route 2 — `ok=false` bypasses the completion-time check entirely,
/// regardless of channel or membership.
#[tokio::test]
async fn worker_result_ok_false_bypasses_the_gate_regardless_of_value() {
    let engine = Engine::new(EngineCfg::default());
    engine.register_verdict_contracts(HashMap::from([(
        "gate".to_string(),
        body_contract(&["PASS", "BLOCKED"]),
    )]));
    let task_id = StepId::new();
    let token = seed_task_with_token(&engine, &task_id, "gate").await;
    let base_url = spawn_server(engine).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/v1/worker/result"))
        .header("Authorization", format!("Bearer {}", token.encode()))
        .json(&serde_json::json!({
            "task_id": task_id.as_str(),
            "value": "UNKNOWN",
            "ok": false,
        }))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
}
