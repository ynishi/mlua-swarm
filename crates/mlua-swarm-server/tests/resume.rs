//! Integration coverage for `POST /v1/runs/:id/resume` (state-driven
//! resume of an `Interrupted` Run under the SAME `run_id`), exercised over
//! a real HTTP round trip.
//!
//! The 404 / 409 / 422 gates are driven against runs seeded directly into a
//! caller-supplied `RunStore` (fast, no dispatch). The happy path goes end
//! to end: a real `POST /v1/tasks` mints + completes a RustFn run (with the
//! launch-input snapshot persisted by `run_flow_form`), the run is then
//! forced to `Interrupted` via the same store handle, and `resume` re-runs
//! it to `Done` under the same id — proving the persisted snapshot rebuilds
//! into a runnable input and the replay cursor lets the flow complete.
//!
//! Uses `build_router_full` with caller-supplied `run_store` / `replay_store`
//! Arcs so the test can both seed and observe them, mirroring this crate's
//! established `reqwest`-over-`axum::serve` integration pattern
//! (`tests/verdict_contract.rs`).

use mlua_swarm::blueprint::{
    current_schema_version, AgentDef, AgentKind, Blueprint, BlueprintMetadata, CompilerHints,
    CompilerStrategy,
};
use mlua_swarm::core::config::EngineCfg;
use mlua_swarm::core::engine::Engine;
use mlua_swarm::store::replay::{InMemoryReplayStore, ReplayStore};
use mlua_swarm::store::run::{InMemoryRunStore, RunRecord, RunStatus, RunStore};
use mlua_swarm::{RunId, TaskId};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

/// A single-step RustFn identity Blueprint (same shape as the handler-level
/// unit tests' `identity_blueprint`), self-contained here since an
/// integration test cannot reach the library crate's `#[cfg(test)]`
/// helpers.
fn identity_blueprint() -> Blueprint {
    Blueprint {
        schema_version: current_schema_version(),
        id: "resume-test-bp".into(),
        flow: serde_json::from_value(json!({
            "kind": "step",
            "ref": mlua_swarm::worker::baseline::AG_IDENTITY,
            "in": {"op": "lit", "value": "hello"},
            "out": {"op": "path", "at": "$.out"},
        }))
        .expect("flow parse"),
        agents: vec![AgentDef {
            name: mlua_swarm::worker::baseline::AG_IDENTITY.into(),
            kind: AgentKind::RustFn,
            spec: json!({"fn_id": mlua_swarm::worker::baseline::AG_IDENTITY}),
            profile: None,
            meta: None,
            runner: None,
            runner_ref: None,
            verdict: None,
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
        check_policy: None,
    }
}

/// Boots a real HTTP server on an ephemeral port whose `AppState` uses the
/// caller-supplied `run_store` / `replay_store` (so the test can seed +
/// observe them), returning its base URL.
async fn spawn_server(run_store: Arc<dyn RunStore>, replay_store: Arc<dyn ReplayStore>) -> String {
    let engine = Engine::new_with_layers(
        EngineCfg::default(),
        mlua_swarm_server::default_layer_registry(),
    );
    let router = mlua_swarm_server::build_router_full(
        engine,
        mlua_swarm_server::default_registry(),
        None, // BlueprintStore (Inline-only)
        None, // ws_operator_factory
        None, // output_store
        None, // base_url
        None, // task_store (default InMemory)
        Some(run_store),
        Some(replay_store),
        300, // sync_timeout_secs
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    format!("http://{addr}")
}

fn seed_run(
    run_id: &RunId,
    task_id: &TaskId,
    status: RunStatus,
    input_json: Option<String>,
) -> RunRecord {
    RunRecord {
        id: run_id.clone(),
        task_id: task_id.clone(),
        status,
        step_entries: vec![],
        degradations: vec![],
        operator_sid: None,
        result_ref: None,
        input_json,
        created_at: 0,
        updated_at: 0,
    }
}

#[tokio::test]
async fn resume_unknown_run_returns_404() {
    let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
    let replay_store: Arc<dyn ReplayStore> = Arc::new(InMemoryReplayStore::new());
    let base = spawn_server(run_store, replay_store).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/runs/{}/resume", RunId::new()))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn resume_non_interrupted_run_returns_409() {
    let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
    let replay_store: Arc<dyn ReplayStore> = Arc::new(InMemoryReplayStore::new());
    let run_id = RunId::new();
    let task_id = TaskId::new();
    // A `Running` run is not resumable — only `Interrupted` is.
    run_store
        .create(seed_run(
            &run_id,
            &task_id,
            RunStatus::Running,
            Some("{}".to_string()),
        ))
        .await
        .expect("seed run");
    let base = spawn_server(run_store, replay_store).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/runs/{run_id}/resume"))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
}

#[tokio::test]
async fn resume_interrupted_run_without_input_returns_422() {
    let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
    let replay_store: Arc<dyn ReplayStore> = Arc::new(InMemoryReplayStore::new());
    let run_id = RunId::new();
    let task_id = TaskId::new();
    // Interrupted, but no launch-input snapshot recorded — cannot resume.
    run_store
        .create(seed_run(&run_id, &task_id, RunStatus::Interrupted, None))
        .await
        .expect("seed run");
    let base = spawn_server(run_store.clone(), replay_store).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/runs/{run_id}/resume"))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    // The 422 must fire before the compare-and-set — the run stays
    // Interrupted, not stranded in Running with no driver behind it.
    let after = run_store.get(&run_id).await.expect("run present");
    assert_eq!(after.status, RunStatus::Interrupted);
}

#[tokio::test]
async fn resume_interrupted_run_completes_under_same_run_id() {
    let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
    let replay_store: Arc<dyn ReplayStore> = Arc::new(InMemoryReplayStore::new());
    let base = spawn_server(run_store.clone(), replay_store).await;
    let client = reqwest::Client::new();

    // 1. Real launch — mints + completes a RustFn run whose launch-input
    //    snapshot is persisted by `run_flow_form`.
    let launch = client
        .post(format!("{base}/v1/tasks"))
        .json(&json!({
            "blueprint": { "kind": "inline", "value": identity_blueprint() },
            "init_ctx": { "in": "hello" },
            "goal": "resume happy path",
        }))
        .send()
        .await
        .expect("launch request");
    assert_eq!(launch.status(), reqwest::StatusCode::OK);
    let launched: serde_json::Value = launch.json().await.expect("launch json");
    let run_id =
        RunId::parse(launched["run_id"].as_str().expect("run_id string")).expect("run_id parse");

    // 2. Force the completed run back to `Interrupted` (simulating a
    //    mid-flight restart) so it becomes resumable.
    run_store
        .update_status(&run_id, RunStatus::Interrupted)
        .await
        .expect("force interrupted");

    // 3. Resume — same run_id, replay cursor + persisted snapshot.
    let resume = client
        .post(format!("{base}/v1/runs/{run_id}/resume"))
        .send()
        .await
        .expect("resume request");
    assert_eq!(resume.status(), reqwest::StatusCode::ACCEPTED);
    let resumed: serde_json::Value = resume.json().await.expect("resume json");
    assert_eq!(
        resumed["run_id"].as_str(),
        Some(run_id.to_string().as_str()),
        "resume must not mint a new run_id"
    );
    assert!(
        resumed["replayed_steps"].is_u64(),
        "resume must report replayed_steps: {resumed}"
    );

    // 4. The resumed run reaches `Done` under the same id.
    let mut terminal = None;
    for _ in 0..50 {
        let rec = run_store.get(&run_id).await.expect("run get");
        if !matches!(rec.status, RunStatus::Pending | RunStatus::Running) {
            terminal = Some(rec);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let rec = terminal.expect("resumed run reached a terminal status within ~5s");
    assert_eq!(
        rec.status,
        RunStatus::Done,
        "resumed run must complete to Done"
    );
    assert!(
        rec.result_ref.is_some(),
        "finalize_run must persist the resumed run's final_ctx"
    );
}
