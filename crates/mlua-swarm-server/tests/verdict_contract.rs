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
