//! Regression coverage for the `Running` flip rollback on `POST
//! /v1/runs/:id/resume` and `POST /v1/runs/:id/rerun-from`.
//!
//! Both handlers flip the Run to `Running` with a compare-and-set and only
//! spawn the driver several store calls later. A store fault inside that
//! window used to leave the row `Running` with nothing behind it: the
//! run-TTL ceiling lives *inside* the task that was never created, so
//! nothing reclaimed the row, and both entry points then refused it on
//! their own status gate — resume wants `Interrupted`, rerun-from wants a
//! terminal status. `POST /v1/runs/:id/cancel` was the only way out of a
//! state no driver had ever entered.
//!
//! Each test drives one of the four failure sites through a store whose
//! single faulted method returns `Err`, and asserts the row came back to
//! the status the compare-and-set moved it away from. The rerun-from pair
//! also pins what the rollback deliberately does *not* claim: past
//! `delete_from` the replay log really is truncated, and the run still
//! returns to its terminal status rather than being recorded as a run
//! failure it never suffered.

use async_trait::async_trait;
use mlua_swarm::blueprint::{
    current_schema_version, AgentDef, AgentKind, Blueprint, BlueprintMetadata, CompilerHints,
    CompilerStrategy,
};
use mlua_swarm::core::config::EngineCfg;
use mlua_swarm::core::engine::Engine;
use mlua_swarm::store::replay::{InMemoryReplayStore, ReplayEntry, ReplayStore, ReplayStoreError};
use mlua_swarm::store::run::{InMemoryRunStore, RunRecord, RunStatus, RunStore, StepEntry};
use mlua_swarm::store::task::{
    InMemoryTaskStore, TaskRecord, TaskRecordStatus, TaskStore, TaskStoreError,
};
use mlua_swarm::{RunId, TaskId};
use serde_json::json;
use std::sync::Arc;

// ──────────────────────────────────────────────────────────────────────────
// Faulted stores.
// ──────────────────────────────────────────────────────────────────────────

/// [`ReplayStore`] that delegates to an in-memory backend except for the
/// one method a given test wants to fail. Models the store fault class the
/// v0.19.0 CI red actually hit (SQLite `database is locked` under parallel
/// access), without needing a real contended backend.
struct FaultyReplayStore {
    inner: InMemoryReplayStore,
    fail_list_by_run: bool,
    fail_delete_from: bool,
}

impl FaultyReplayStore {
    fn failing_list_by_run() -> Self {
        Self {
            inner: InMemoryReplayStore::new(),
            fail_list_by_run: true,
            fail_delete_from: false,
        }
    }

    fn failing_delete_from() -> Self {
        Self {
            inner: InMemoryReplayStore::new(),
            fail_list_by_run: false,
            fail_delete_from: true,
        }
    }

    fn healthy() -> Self {
        Self {
            inner: InMemoryReplayStore::new(),
            fail_list_by_run: false,
            fail_delete_from: false,
        }
    }
}

#[async_trait]
impl ReplayStore for FaultyReplayStore {
    fn name(&self) -> &str {
        "faulty"
    }

    async fn append(&self, entry: ReplayEntry) -> Result<(), ReplayStoreError> {
        self.inner.append(entry).await
    }

    async fn list_by_run(&self, run_id: &RunId) -> Result<Vec<ReplayEntry>, ReplayStoreError> {
        if self.fail_list_by_run {
            return Err(ReplayStoreError::Other("injected list_by_run fault".into()));
        }
        self.inner.list_by_run(run_id).await
    }

    async fn delete_from(
        &self,
        run_id: &RunId,
        from_index: usize,
    ) -> Result<usize, ReplayStoreError> {
        if self.fail_delete_from {
            return Err(ReplayStoreError::Other("injected delete_from fault".into()));
        }
        self.inner.delete_from(run_id, from_index).await
    }
}

/// [`TaskStore`] whose `update_status` always fails — the last store call
/// both handlers make before the driver `tokio::spawn`.
struct FaultyTaskStore {
    inner: InMemoryTaskStore,
}

impl FaultyTaskStore {
    fn new() -> Self {
        Self {
            inner: InMemoryTaskStore::new(),
        }
    }
}

#[async_trait]
impl TaskStore for FaultyTaskStore {
    fn name(&self) -> &str {
        "faulty"
    }

    async fn create(&self, record: TaskRecord) -> Result<(), TaskStoreError> {
        self.inner.create(record).await
    }

    async fn get(&self, id: &TaskId) -> Result<TaskRecord, TaskStoreError> {
        self.inner.get(id).await
    }

    async fn list(&self) -> Result<Vec<TaskRecord>, TaskStoreError> {
        self.inner.list().await
    }

    async fn update_status(
        &self,
        _id: &TaskId,
        _status: TaskRecordStatus,
    ) -> Result<(), TaskStoreError> {
        Err(TaskStoreError::Other("injected update_status fault".into()))
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Fixtures.
// ──────────────────────────────────────────────────────────────────────────

/// Two-step RustFn Blueprint, mirroring `rerun_from.rs`: both steps run the
/// baseline `identity` fn but carry distinct `AgentDef.name`s so `step_ref`
/// is unambiguous in the replay log. It has to compile cleanly because
/// `rerun-from` pre-flight-compiles before its compare-and-set.
fn two_step_blueprint() -> Blueprint {
    Blueprint {
        schema_version: current_schema_version(),
        id: "dispatch-rollback-test-bp".into(),
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
                lints: None,
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
                lints: None,
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
        subprocesses: vec![],
        check_policy: None,
        blueprint_ref_includes: Vec::new(),
    }
}

fn seed_run(
    run_id: &RunId,
    task_id: &TaskId,
    status: RunStatus,
    input_json: Option<String>,
    step_entries: Vec<StepEntry>,
) -> RunRecord {
    RunRecord {
        id: run_id.clone(),
        task_id: task_id.clone(),
        status,
        step_entries,
        degradations: vec![],
        operator_sid: None,
        current: Default::default(),
        next_generation: 0,
        result_ref: None,
        input_json,
        created_at: 0,
        updated_at: 0,
    }
}

/// Mirrors the crate-private `RunLaunchSnapshot` shape (same approach as
/// `rerun_from.rs`) so a seeded run decodes into a runnable input.
fn snapshot_json_for(bp: &Blueprint) -> String {
    json!({
        "blueprint": { "kind": "inline", "value": bp },
        "operator_id": "test-op",
        "role": "operator",
        "ttl": { "secs": 30, "nanos": 0 },
        "init_ctx": { "in": "hello" },
        "operator_kind": null,
        "bridge_id": null,
        "hook_id": null,
        "operator_sid": null,
        "operator_kind_overrides": {},
        "task_input": null,
        "check_policy": null,
    })
    .to_string()
}

/// Append `agent-a` then `agent-b` to the replay log so a rerun-from on
/// `agent-b` lands on cut index 1.
async fn seed_two_entries(replay_store: &Arc<dyn ReplayStore>, run_id: &RunId) {
    for (step_ref, value) in [("agent-a", json!({"v": 1})), ("agent-b", json!({"v": 2}))] {
        let ctx = mlua_swarm::core::ctx::Ctx::new(mlua_swarm::types::StepId::new(), 1, step_ref);
        replay_store
            .append(
                ReplayEntry::from_completion(run_id.clone(), step_ref, "h", 0, &ctx, &value)
                    .expect("entry build"),
            )
            .await
            .expect("seed replay");
    }
}

async fn spawn_server(
    run_store: Arc<dyn RunStore>,
    replay_store: Arc<dyn ReplayStore>,
    task_store: Option<Arc<dyn TaskStore>>,
) -> String {
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
        task_store,
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

// ──────────────────────────────────────────────────────────────────────────
// resume — rolls back to Interrupted.
// ──────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn resume_rolls_back_to_interrupted_when_replay_list_faults() {
    let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
    let replay_store: Arc<dyn ReplayStore> = Arc::new(FaultyReplayStore::failing_list_by_run());
    let run_id = RunId::new();
    let task_id = TaskId::new();
    run_store
        .create(seed_run(
            &run_id,
            &task_id,
            RunStatus::Interrupted,
            Some(snapshot_json_for(&two_step_blueprint())),
            vec![],
        ))
        .await
        .expect("seed run");
    let base = spawn_server(run_store.clone(), replay_store, None).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/runs/{run_id}/resume"))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);

    let after = run_store.get(&run_id).await.expect("run present");
    assert_eq!(
        after.status,
        RunStatus::Interrupted,
        "a replay-list fault past the compare-and-set must not strand the run in Running"
    );
}

#[tokio::test]
async fn resume_rolls_back_to_interrupted_when_task_update_faults() {
    let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
    let replay_store: Arc<dyn ReplayStore> = Arc::new(FaultyReplayStore::healthy());
    let task_store: Arc<dyn TaskStore> = Arc::new(FaultyTaskStore::new());
    let run_id = RunId::new();
    let task_id = TaskId::new();
    run_store
        .create(seed_run(
            &run_id,
            &task_id,
            RunStatus::Interrupted,
            Some(snapshot_json_for(&two_step_blueprint())),
            vec![],
        ))
        .await
        .expect("seed run");
    let base = spawn_server(run_store.clone(), replay_store, Some(task_store)).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/runs/{run_id}/resume"))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);

    let after = run_store.get(&run_id).await.expect("run present");
    assert_eq!(
        after.status,
        RunStatus::Interrupted,
        "a task-store fault past the compare-and-set must not strand the run in Running"
    );
}

// ──────────────────────────────────────────────────────────────────────────
// rerun-from — rolls back to the terminal status it started from.
// ──────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn rerun_from_rolls_back_to_terminal_when_truncate_faults() {
    let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
    let replay_store: Arc<dyn ReplayStore> = Arc::new(FaultyReplayStore::failing_delete_from());
    let run_id = RunId::new();
    let task_id = TaskId::new();
    run_store
        .create(seed_run(
            &run_id,
            &task_id,
            RunStatus::Done,
            Some(snapshot_json_for(&two_step_blueprint())),
            vec![],
        ))
        .await
        .expect("seed run");
    seed_two_entries(&replay_store, &run_id).await;
    let base = spawn_server(run_store.clone(), replay_store.clone(), None).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/runs/{run_id}/rerun-from"))
        .json(&json!({ "from_step": "agent-b" }))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);

    let after = run_store.get(&run_id).await.expect("run present");
    assert_eq!(
        after.status,
        RunStatus::Done,
        "a truncate fault must return the run to the terminal status it was rerun from"
    );
    let entries = replay_store.list_by_run(&run_id).await.expect("list");
    assert_eq!(
        entries.len(),
        2,
        "the truncate never landed, so the replay log is still whole"
    );
}

#[tokio::test]
async fn rerun_from_rolls_back_to_terminal_when_task_update_faults_after_truncate() {
    let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
    let replay_store: Arc<dyn ReplayStore> = Arc::new(FaultyReplayStore::healthy());
    let task_store: Arc<dyn TaskStore> = Arc::new(FaultyTaskStore::new());
    let run_id = RunId::new();
    let task_id = TaskId::new();
    run_store
        .create(seed_run(
            &run_id,
            &task_id,
            RunStatus::Done,
            Some(snapshot_json_for(&two_step_blueprint())),
            vec![],
        ))
        .await
        .expect("seed run");
    seed_two_entries(&replay_store, &run_id).await;
    let base = spawn_server(run_store.clone(), replay_store.clone(), Some(task_store)).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/runs/{run_id}/rerun-from"))
        .json(&json!({ "from_step": "agent-b" }))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);

    let after = run_store.get(&run_id).await.expect("run present");
    assert_eq!(
        after.status,
        RunStatus::Done,
        "past delete_from the rollback still returns the terminal status — the replay loss \
         is real, but it is not a run failure and must not be recorded as one"
    );
    let entries = replay_store.list_by_run(&run_id).await.expect("list");
    assert_eq!(
        entries.len(),
        1,
        "the truncate did land: this is exactly the state the rollback declines to lie about"
    );
}
