//! Integration coverage for `POST /v1/runs/:id/rerun-from` — GH #71 Layer A.
//!
//! The 404 / 409 / 422 gates are driven against runs seeded directly into a
//! caller-supplied `RunStore` / `ReplayStore` (fast, no dispatch). The happy
//! path goes end to end: a real `POST /v1/tasks` mints + completes a
//! two-step RustFn run (both steps map to the baseline `identity` fn but
//! carry distinct `AgentDef.name`s so `step_ref` is unambiguous), and
//! `rerun-from` on the second step re-executes it under the SAME `run_id`,
//! proving the physical truncation frees the `(step_ref, input_hash,
//! occurrence)` slot for the rerun `append` and the flow reaches `Done`
//! again.
//!
//! Uses `build_router_full` with caller-supplied `run_store` / `replay_store`
//! Arcs so the test can both seed and observe them, mirroring `resume.rs`.

use mlua_swarm::blueprint::{
    current_schema_version, AgentDef, AgentKind, Blueprint, BlueprintMetadata, CompilerHints,
    CompilerStrategy,
};
use mlua_swarm::core::config::EngineCfg;
use mlua_swarm::core::engine::Engine;
use mlua_swarm::store::replay::{InMemoryReplayStore, ReplayEntry, ReplayStore};
use mlua_swarm::store::run::{InMemoryRunStore, RunRecord, RunStatus, RunStore};
use mlua_swarm::{RunId, TaskId};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

/// Two-step RustFn Blueprint: `agent-a` then `agent-b`, both bound to the
/// baseline `identity` fn but with distinct `AgentDef.name`s so `step_ref`
/// is uniquely identifiable in the replay log.
fn two_step_blueprint() -> Blueprint {
    Blueprint {
        schema_version: current_schema_version(),
        id: "rerun-from-test-bp".into(),
        flow: serde_json::from_value(json!({
            "kind": "seq",
            "children": [
                {
                    "kind": "step",
                    "ref": "agent-a",
                    "in": {"op": "lit", "value": "hello"},
                    "out": {"op": "path", "at": "$.a"},
                },
                {
                    "kind": "step",
                    "ref": "agent-b",
                    "in": {"op": "path", "at": "$.a"},
                    "out": {"op": "path", "at": "$.b"},
                },
            ],
        }))
        .expect("flow parse"),
        agents: vec![
            AgentDef {
                name: "agent-a".into(),
                kind: AgentKind::RustFn,
                spec: json!({"fn_id": mlua_swarm::worker::baseline::AG_IDENTITY}),
                profile: None,
                meta: None,
                runner: None,
                runner_ref: None,
                verdict: None,
            },
            AgentDef {
                name: "agent-b".into(),
                kind: AgentKind::RustFn,
                spec: json!({"fn_id": mlua_swarm::worker::baseline::AG_IDENTITY}),
                profile: None,
                meta: None,
                runner: None,
                runner_ref: None,
                verdict: None,
            },
        ],
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
        blueprint_ref_includes: Vec::new(),
    }
}

async fn spawn_server(run_store: Arc<dyn RunStore>, replay_store: Arc<dyn ReplayStore>) -> String {
    let engine = Engine::new_with_layers(
        EngineCfg::default(),
        mlua_swarm_server::default_layer_registry(),
    );
    let router = mlua_swarm_server::build_router_full(
        engine,
        mlua_swarm_server::default_registry(),
        None,
        None,
        None,
        None,
        None,
        Some(run_store),
        Some(replay_store),
        300,
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

/// Build a synthetic `RunLaunchSnapshot` JSON for a seeded run that carries
/// the two-step BP. The handler decodes this via `serde_json::from_str::
/// <RunLaunchSnapshot>` before flipping status → Running. Since
/// `RunLaunchSnapshot` is `pub(crate)`, we encode the same shape by hand.
fn snapshot_json_for(bp: &Blueprint) -> String {
    // Mirrors the crate-private `RunLaunchSnapshot` shape. Every field
    // that `TaskApplicationInput` requires is present so `into_input`
    // rebuilds a runnable input.
    json!({
        "blueprint": { "kind": "inline", "value": bp },
        "operator_id": "test-op",
        "role": "operator",
        "ttl": { "secs": 30, "nanos": 0 },
        "init_ctx": { "in": "hello" },
        "operator_kind": null,
        "bridge_id": null,
        "hook_id": null,
        "operator_backend_id": null,
        "operator_kind_overrides": {},
        "task_input": null,
        "check_policy": null,
    })
    .to_string()
}

#[tokio::test]
async fn rerun_from_unknown_run_returns_404() {
    let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
    let replay_store: Arc<dyn ReplayStore> = Arc::new(InMemoryReplayStore::new());
    let base = spawn_server(run_store, replay_store).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/runs/{}/rerun-from", RunId::new()))
        .json(&json!({ "from_step": "agent-b" }))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn rerun_from_running_run_returns_409() {
    let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
    let replay_store: Arc<dyn ReplayStore> = Arc::new(InMemoryReplayStore::new());
    let run_id = RunId::new();
    let task_id = TaskId::new();
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
        .post(format!("{base}/v1/runs/{run_id}/rerun-from"))
        .json(&json!({ "from_step": "agent-b" }))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
}

#[tokio::test]
async fn rerun_from_done_run_without_input_returns_422() {
    let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
    let replay_store: Arc<dyn ReplayStore> = Arc::new(InMemoryReplayStore::new());
    let run_id = RunId::new();
    let task_id = TaskId::new();
    // Done but no launch input snapshot — cannot rerun.
    run_store
        .create(seed_run(&run_id, &task_id, RunStatus::Done, None))
        .await
        .expect("seed run");
    let base = spawn_server(run_store.clone(), replay_store).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/runs/{run_id}/rerun-from"))
        .json(&json!({ "from_step": "agent-b" }))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    // The 422 must fire before the compare-and-set — the run stays Done.
    let after = run_store.get(&run_id).await.expect("run present");
    assert_eq!(after.status, RunStatus::Done);
}

#[tokio::test]
async fn rerun_from_missing_step_returns_422() {
    let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
    let replay_store: Arc<dyn ReplayStore> = Arc::new(InMemoryReplayStore::new());
    let run_id = RunId::new();
    let task_id = TaskId::new();
    let bp = two_step_blueprint();
    run_store
        .create(seed_run(
            &run_id,
            &task_id,
            RunStatus::Done,
            Some(snapshot_json_for(&bp)),
        ))
        .await
        .expect("seed run");

    // Seed one replay entry for `agent-a` — but the caller will ask to
    // rerun from `nonexistent-step`, which is not in the log.
    let ctx = mlua_swarm::core::ctx::Ctx::new(mlua_swarm::types::StepId::new(), 1, "agent-a");
    replay_store
        .append(
            ReplayEntry::from_completion(
                run_id.clone(),
                "agent-a",
                "h",
                0,
                &ctx,
                &json!({ "v": 1 }),
            )
            .expect("entry build"),
        )
        .await
        .expect("seed replay");

    let base = spawn_server(run_store.clone(), replay_store.clone()).await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/runs/{run_id}/rerun-from"))
        .json(&json!({ "from_step": "nonexistent-step" }))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    // Run stays Done — the 422 must fire before the compare-and-set, and
    // the replay log must stay untouched.
    let after = run_store.get(&run_id).await.expect("run present");
    assert_eq!(after.status, RunStatus::Done);
    let entries_after = replay_store.list_by_run(&run_id).await.expect("list");
    assert_eq!(
        entries_after.len(),
        1,
        "replay log must be untouched on 422"
    );
}

#[tokio::test]
async fn rerun_from_empty_from_step_returns_400() {
    let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
    let replay_store: Arc<dyn ReplayStore> = Arc::new(InMemoryReplayStore::new());
    let run_id = RunId::new();
    let task_id = TaskId::new();
    run_store
        .create(seed_run(
            &run_id,
            &task_id,
            RunStatus::Done,
            Some("{}".to_string()),
        ))
        .await
        .expect("seed run");
    let base = spawn_server(run_store, replay_store).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/runs/{run_id}/rerun-from"))
        .json(&json!({ "from_step": "" }))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rerun_from_happy_path_truncates_and_completes() {
    let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
    let replay_store: Arc<dyn ReplayStore> = Arc::new(InMemoryReplayStore::new());
    let base = spawn_server(run_store.clone(), replay_store.clone()).await;
    let client = reqwest::Client::new();

    // 1. Real launch — dispatches the two-step BP to Done and persists a
    //    real launch-input snapshot on the RunRecord.
    let launch = client
        .post(format!("{base}/v1/tasks"))
        .json(&json!({
            "blueprint": { "kind": "inline", "value": two_step_blueprint() },
            "init_ctx": {},
            "goal": "rerun-from happy path",
        }))
        .send()
        .await
        .expect("launch request");
    assert_eq!(
        launch.status(),
        reqwest::StatusCode::OK,
        "launch body: {}",
        launch.text().await.unwrap_or_default()
    );
    let launched: serde_json::Value = launch.json().await.expect("launch json");
    let run_id =
        RunId::parse(launched["run_id"].as_str().expect("run_id string")).expect("run_id parse");

    // Sanity: run completed and both steps landed rows in the replay log
    // in dispatch order — [agent-a, agent-b].
    let before_entries = replay_store
        .list_by_run(&run_id)
        .await
        .expect("list before rerun");
    let refs_before: Vec<String> = before_entries.iter().map(|e| e.step_ref.clone()).collect();
    assert_eq!(
        refs_before,
        vec!["agent-a".to_string(), "agent-b".to_string()],
        "seeded replay log shape: {refs_before:?}"
    );
    let run_before = run_store.get(&run_id).await.expect("run get");
    assert_eq!(run_before.status, RunStatus::Done);

    // 2. Rerun from agent-b.
    let resp = client
        .post(format!("{base}/v1/runs/{run_id}/rerun-from"))
        .json(&json!({ "from_step": "agent-b" }))
        .send()
        .await
        .expect("rerun request");
    assert_eq!(resp.status(), reqwest::StatusCode::ACCEPTED);
    let body: serde_json::Value = resp.json().await.expect("rerun json");
    assert_eq!(
        body["run_id"].as_str(),
        Some(run_id.to_string().as_str()),
        "rerun-from must not mint a new run_id"
    );
    assert_eq!(
        body["replayed_steps"].as_u64(),
        Some(1),
        "one entry (agent-a) survives the cut"
    );
    assert_eq!(
        body["dropped_steps"].as_u64(),
        Some(1),
        "one entry (agent-b) is dropped"
    );

    // 3. Poll: rerun run reaches Done again under the same id, and the
    //    replay log has TWO entries again (agent-a survived + agent-b
    //    re-appended by the rerun dispatch).
    let mut terminal = None;
    for _ in 0..50 {
        let rec = run_store.get(&run_id).await.expect("run get");
        if !matches!(rec.status, RunStatus::Pending | RunStatus::Running) {
            terminal = Some(rec);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let rec = terminal.expect("rerun run reached a terminal status within ~5s");
    assert_eq!(rec.status, RunStatus::Done, "rerun must complete to Done");
    let after_entries = replay_store
        .list_by_run(&run_id)
        .await
        .expect("list after rerun");
    let refs_after: Vec<String> = after_entries.iter().map(|e| e.step_ref.clone()).collect();
    assert_eq!(
        refs_after,
        vec!["agent-a".to_string(), "agent-b".to_string()],
        "post-rerun replay log carries the fresh agent-b row, not the ghost: {refs_after:?}"
    );
}
