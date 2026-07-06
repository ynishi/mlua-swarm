//! HTTP surface for the Task/Run persistence axis (issue #13 ID-hierarchy
//! reconciliation: Blueprint → Task → Run → Step → Attempt).
//!
//! - `GET  /v1/tasks`          — list every persisted `TaskRecord`, newest first.
//! - `GET  /v1/tasks/:id`      — a `TaskRecord` plus every `RunRecord` kicked from it.
//! - `POST /v1/tasks/:id/runs` — re-kick an existing Task: mints a fresh `RunId`,
//!   replays the stored `blueprint_ref` / `input_ctx` through
//!   `TaskApplication::handle_with_run`, and returns the new `{task_id, run_id}` pair.
//! - `GET  /v1/runs/:id`       — a single `RunRecord` (`step_entries` trace included).
//!
//! `POST /v1/tasks` itself (the flow-eval entry point, `tasks_start` /
//! `run_flow_form`) stays in `crate::lib` — it is the pre-existing
//! Operator-inject-aware dispatch path this module's handlers re-kick
//! through, not a new one. This module owns the read/list/re-kick surface
//! plus the [`finalize_run`] persistence helper both paths share.
//!
//! Authorization follows the same convention as the existing `POST /v1/tasks`
//! entry: no `Authorization` header is required (the route is open), and the
//! only Operator-session correlation available is the request-body-level
//! `operator_sid` (see `crate::FlowTasksReq` doc) — this module invents no
//! new auth mechanism.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use mlua_swarm::application::{TaskApplicationError, TaskApplicationInput, TaskApplicationOutput};
use mlua_swarm::store::run::{RunContext, RunRecord, RunStatus, RunStoreError};
use mlua_swarm::store::task::{TaskRecord, TaskRecordStatus, TaskStoreError};
use mlua_swarm::{Role, RunId, TaskId};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::{ApiError, AppState};

/// Current Unix time in whole seconds. `TaskRecord` / `RunRecord` timestamps
/// are `u64` seconds (not milliseconds) — see their field docs in
/// `mlua_swarm::store::task` / `mlua_swarm::store::run`.
pub(crate) fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Shared finalize step for a dispatched kick: updates the Run's
/// `result_ref` + status and the owning Task's coarse status based on the
/// `TaskApplication::handle_with_run` outcome, then returns that same
/// outcome unchanged so callers keep shaping their own wire response /
/// error.
///
/// Secondary persistence failures (the store call itself erroring) are
/// logged via `tracing::warn!` and otherwise swallowed — they must not mask
/// the primary dispatch outcome the caller already has in hand.
pub(crate) async fn finalize_run(
    state: &AppState,
    task_id: &TaskId,
    run_id: &RunId,
    outcome: Result<TaskApplicationOutput, TaskApplicationError>,
) -> Result<TaskApplicationOutput, TaskApplicationError> {
    match &outcome {
        Ok(out) => {
            if let Err(e) = state
                .run_store
                .set_result(run_id, out.final_ctx.clone())
                .await
            {
                tracing::warn!(%run_id, error = %e, "finalize_run: set_result failed");
            }
            if let Err(e) = state.run_store.update_status(run_id, RunStatus::Done).await {
                tracing::warn!(%run_id, error = %e, "finalize_run: run update_status(Done) failed");
            }
            if let Err(e) = state
                .task_store
                .update_status(task_id, TaskRecordStatus::Done)
                .await
            {
                tracing::warn!(%task_id, error = %e, "finalize_run: task update_status(Done) failed");
            }
        }
        Err(e) => {
            if let Err(store_err) = state
                .run_store
                .update_status(run_id, RunStatus::Failed)
                .await
            {
                tracing::warn!(%run_id, error = %store_err, "finalize_run: run update_status(Failed) failed");
            }
            if let Err(store_err) = state
                .task_store
                .update_status(task_id, TaskRecordStatus::Failed)
                .await
            {
                tracing::warn!(%task_id, error = %store_err, "finalize_run: task update_status(Failed) failed");
            }
            tracing::warn!(%task_id, %run_id, error = %e, "finalize_run: dispatch failed");
        }
    }
    outcome
}

/// Query params for `GET /v1/tasks`.
#[derive(Debug, Deserialize, Default)]
pub struct TasksListQuery {
    /// Caps the returned list to the first N entries (already newest-first
    /// per `TaskStore::list`). Omitted = no cap.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// `GET /v1/tasks?limit=N`. Lists every persisted `TaskRecord`, newest first.
pub async fn tasks_list(
    State(state): State<AppState>,
    Query(q): Query<TasksListQuery>,
) -> Result<Json<Vec<TaskRecord>>, ApiError> {
    let mut records = state.task_store.list().await.map_err(ApiError::engine)?;
    if let Some(limit) = q.limit {
        records.truncate(limit);
    }
    Ok(Json(records))
}

/// Response body for `GET /v1/tasks/:id`.
#[derive(Debug, Clone, Serialize)]
pub struct TaskDetailResponse {
    /// The Task's own record.
    pub task: TaskRecord,
    /// Every Run kicked from this Task, oldest first (`RunStore::list_by_task` order).
    pub runs: Vec<RunRecord>,
}

/// `GET /v1/tasks/:id`. Returns the `TaskRecord` plus every `RunRecord`
/// kicked from it (`RunStore::list_by_task`, oldest kick first).
pub async fn task_get(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<TaskDetailResponse>, ApiError> {
    let task_id = TaskId(id);
    let task = state
        .task_store
        .get(&task_id)
        .await
        .map_err(map_task_store_err)?;
    let runs = state
        .run_store
        .list_by_task(&task_id)
        .await
        .map_err(ApiError::engine)?;
    Ok(Json(TaskDetailResponse { task, runs }))
}

/// Response body for `POST /v1/tasks/:id/runs`.
#[derive(Debug, Clone, Serialize)]
pub struct RunKickResponse {
    /// The re-kicked Task's id (echoes the path param).
    pub task_id: TaskId,
    /// The freshly minted Run id for this kick.
    pub run_id: RunId,
}

/// `POST /v1/tasks/:id/runs`. Re-kicks an existing Task: reads its stored
/// `blueprint_ref` / `input_ctx`, mints a fresh `RunId`, dispatches through
/// `TaskApplication::handle_with_run` via `TaskApplicationInput::automate`
/// (the unadorned Operator-default path — no per-request Operator override
/// support here, unlike `POST /v1/tasks`; the stored Task carries no such
/// preferences) plus a freshly-built `RunContext` (issue #13 run_id
/// propagation, so this kick's steps get their own `step_entries` trace),
/// and persists the outcome via [`finalize_run`].
pub async fn task_rekick(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<RunKickResponse>), ApiError> {
    let task_id = TaskId(id);
    let task = state
        .task_store
        .get(&task_id)
        .await
        .map_err(map_task_store_err)?;

    let blueprint: mlua_swarm::application::BlueprintRef =
        serde_json::from_value(task.blueprint_ref.clone()).map_err(|e| {
            ApiError::bad_request(format!(
                "task {task_id}: stored blueprint_ref failed to decode: {e}"
            ))
        })?;

    let run_id = RunId::new();
    let now = now_secs();
    state
        .task_store
        .update_status(&task_id, TaskRecordStatus::Running)
        .await
        .map_err(ApiError::engine)?;
    state
        .run_store
        .create(RunRecord {
            id: run_id.clone(),
            task_id: task_id.clone(),
            status: RunStatus::Running,
            step_entries: Vec::new(),
            operator_sid: None,
            result_ref: None,
            created_at: now,
            updated_at: now,
        })
        .await
        .map_err(ApiError::engine)?;

    let input = TaskApplicationInput::automate(
        blueprint,
        "http-run",
        Role::Operator,
        Duration::from_secs(crate::default_run_ttl()),
        task.input_ctx.clone(),
    );
    let run_ctx = RunContext {
        run_id: run_id.clone(),
        run_store: state.run_store.clone(),
    };
    let outcome = state.task_app.handle_with_run(input, Some(run_ctx)).await;
    finalize_run(&state, &task_id, &run_id, outcome)
        .await
        .map_err(|e| ApiError::bad_request(format!("run: {e}")))?;

    Ok((
        StatusCode::CREATED,
        Json(RunKickResponse { task_id, run_id }),
    ))
}

/// `GET /v1/runs/:id`. Returns a single `RunRecord` (its `step_entries`
/// trace included).
pub async fn run_get(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<RunRecord>, ApiError> {
    let run_id = RunId(id);
    let run = state
        .run_store
        .get(&run_id)
        .await
        .map_err(map_run_store_err)?;
    Ok(Json(run))
}

fn map_task_store_err(e: TaskStoreError) -> ApiError {
    match e {
        TaskStoreError::NotFound(id) => ApiError::not_found(format!("task not found: {id}")),
        other => ApiError::engine(other),
    }
}

fn map_run_store_err(e: RunStoreError) -> ApiError {
    match e {
        RunStoreError::NotFound(id) => ApiError::not_found(format!("run not found: {id}")),
        other => ApiError::engine(other),
    }
}

// ──────────────────────────────────────────────────────────────────────────
// UT
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use mlua_swarm::application::BlueprintRef;
    use mlua_swarm::blueprint::{
        current_schema_version, AgentDef, AgentKind, Blueprint, BlueprintMetadata, CompilerHints,
        CompilerStrategy,
    };
    use mlua_swarm::core::config::EngineCfg;
    use mlua_swarm::core::engine::Engine;
    use mlua_swarm::store::output::InMemoryOutputStore;
    use mlua_swarm::store::run::InMemoryRunStore;
    use mlua_swarm::store::task::InMemoryTaskStore;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// A single-step flow.ir Blueprint that always succeeds: `Step { ref:
    /// "identity", in: lit("hello"), out: $.out }` against the baseline
    /// `RustFn` identity worker (same shape as `seed_blueprint` in
    /// `mlua-swarm-cli`'s `serve.rs`, self-contained here rather than
    /// importing a binary crate).
    fn identity_blueprint() -> Blueprint {
        Blueprint {
            schema_version: current_schema_version(),
            id: "tasks-test-bp".into(),
            flow: serde_json::from_value(serde_json::json!({
                "kind": "step",
                "ref": mlua_swarm::worker::baseline::AG_IDENTITY,
                "in": {"op": "lit", "value": "hello"},
                "out": {"op": "path", "at": "$.out"},
            }))
            .expect("flow parse"),
            agents: vec![AgentDef {
                name: mlua_swarm::worker::baseline::AG_IDENTITY.into(),
                kind: AgentKind::RustFn,
                spec: serde_json::json!({"fn_id": mlua_swarm::worker::baseline::AG_IDENTITY}),
                profile: None,
                meta: None,
            }],
            operators: vec![],
            hints: CompilerHints::default(),
            strategy: CompilerStrategy::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
        }
    }

    /// Minimal `AppState` for handler-level tests — mirrors the construction
    /// `build_router_full` does internally, but skips the `Router` wrapper so
    /// tests can call handler functions directly (this crate's established
    /// unit-test convention; see e.g. `operator_ws::login`'s tests).
    fn test_state() -> AppState {
        let engine = Engine::new_with_layers(EngineCfg::default(), crate::default_layer_registry());
        let compiler = mlua_swarm::Compiler::new(crate::default_registry());
        let launch = Arc::new(mlua_swarm::TaskLaunchService::new(engine.clone(), compiler));
        AppState {
            engine,
            sessions: Arc::new(Mutex::new(crate::SessionStore::default())),
            task_app: Arc::new(mlua_swarm::TaskApplication::new_inline_only(launch)),
            ws_operator_factory: None,
            data_store: Arc::new(InMemoryOutputStore::new()),
            operator_sessions: Arc::new(Mutex::new(HashMap::new())),
            roles_to_sid: Arc::new(Mutex::new(HashMap::new())),
            task_store: Arc::new(InMemoryTaskStore::new()),
            run_store: Arc::new(InMemoryRunStore::new()),
            base_url: None,
        }
    }

    fn post_tasks_req(goal: &str) -> crate::FlowTasksReq {
        crate::FlowTasksReq {
            blueprint: BlueprintRef::Inline {
                value: Box::new(identity_blueprint()),
            },
            init_ctx: serde_json::json!({"in": "hello"}),
            ttl_secs: None,
            operator: None,
            operator_sid: None,
            goal: Some(goal.to_string()),
        }
    }

    #[test]
    fn task_id_serializes_as_bare_string() {
        // Sanity check for the newtype-struct transparency relied on
        // throughout this module's response shapes (`TaskId` / `RunId`
        // serialize as plain JSON strings, not `{"0": "..."}`).
        let v = serde_json::to_value(TaskId("T-abc".into())).expect("serialize");
        assert_eq!(v, serde_json::json!("T-abc"));
    }

    #[tokio::test]
    async fn post_then_get_drill_down() {
        let state = test_state();

        let posted = crate::tasks_start(State(state.clone()), Json(post_tasks_req("smoke goal")))
            .await
            .expect("tasks_start")
            .0;
        let task_id = posted.task_id.clone();
        let run_id = posted.run_id.clone();

        // GET /v1/tasks lists it.
        let list = tasks_list(State(state.clone()), Query(TasksListQuery { limit: None }))
            .await
            .expect("tasks_list")
            .0;
        assert!(
            list.iter().any(|t| t.id == task_id),
            "task {task_id} missing from list of {} tasks",
            list.len()
        );

        // GET /v1/tasks/:id drills down to the Task + its Run.
        let detail = task_get(State(state.clone()), Path(task_id.0.clone()))
            .await
            .expect("task_get")
            .0;
        assert_eq!(detail.task.id, task_id);
        assert_eq!(detail.task.goal, "smoke goal");
        assert_eq!(detail.task.status, TaskRecordStatus::Done);
        assert_eq!(detail.runs.len(), 1);
        assert_eq!(detail.runs[0].id, run_id);
        assert_eq!(detail.runs[0].status, RunStatus::Done);

        // GET /v1/runs/:id returns the same Run directly.
        let run = run_get(State(state.clone()), Path(run_id.0.clone()))
            .await
            .expect("run_get")
            .0;
        assert_eq!(run.id, run_id);
        assert_eq!(run.task_id, task_id);
        assert_eq!(run.result_ref, Some(posted.final_ctx));

        // issue #13 run_id propagation: `POST /v1/tasks` (`run_flow_form`)
        // wires a `RunContext` into `TaskApplication::handle_with_run`, so
        // the single dispatched step must be traced into `step_entries`.
        assert_eq!(
            run.step_entries.len(),
            1,
            "expected one step_entry for the 1-step identity Blueprint, got {:?}",
            run.step_entries
        );
        assert_eq!(
            run.step_entries[0].step_ref,
            Some(mlua_swarm::worker::baseline::AG_IDENTITY.to_string())
        );
        assert_eq!(run.step_entries[0].status, Some("passed".to_string()));
    }

    #[tokio::test]
    async fn rekick_adds_a_second_run_to_the_same_task() {
        let state = test_state();
        let posted = crate::tasks_start(State(state.clone()), Json(post_tasks_req("rekick goal")))
            .await
            .expect("tasks_start")
            .0;
        let task_id = posted.task_id.clone();
        let first_run_id = posted.run_id.clone();

        let (status, rekicked) = task_rekick(State(state.clone()), Path(task_id.0.clone()))
            .await
            .expect("task_rekick");
        assert_eq!(status, StatusCode::CREATED);
        let second_run_id = rekicked.0.run_id.clone();
        assert_ne!(first_run_id, second_run_id);

        let detail = task_get(State(state.clone()), Path(task_id.0.clone()))
            .await
            .expect("task_get")
            .0;
        assert_eq!(
            detail.runs.len(),
            2,
            "expected 2 runs, got {:?}",
            detail.runs
        );
        let ids: Vec<&RunId> = detail.runs.iter().map(|r| &r.id).collect();
        assert!(ids.contains(&&first_run_id));
        assert!(ids.contains(&&second_run_id));

        // issue #13 run_id propagation: each kick's own `EngineDispatcher`
        // (built fresh per `TaskApplication::handle_with_run` call) must
        // trace its own dispatched step into its own `RunRecord` —
        // independent `step_entries`, not shared/accumulated across kicks.
        let first_run = detail
            .runs
            .iter()
            .find(|r| r.id == first_run_id)
            .expect("first run present in detail.runs");
        let second_run = detail
            .runs
            .iter()
            .find(|r| r.id == second_run_id)
            .expect("second run present in detail.runs");
        assert_eq!(
            first_run.step_entries.len(),
            1,
            "first run step_entries: {:?}",
            first_run.step_entries
        );
        assert_eq!(
            second_run.step_entries.len(),
            1,
            "second run step_entries: {:?}",
            second_run.step_entries
        );
        assert_eq!(
            first_run.step_entries[0].step_ref,
            Some(mlua_swarm::worker::baseline::AG_IDENTITY.to_string())
        );
        assert_eq!(
            second_run.step_entries[0].step_ref,
            Some(mlua_swarm::worker::baseline::AG_IDENTITY.to_string())
        );
        assert_eq!(first_run.step_entries[0].status, Some("passed".to_string()));
        assert_eq!(
            second_run.step_entries[0].status,
            Some("passed".to_string())
        );
        assert_ne!(
            first_run.step_entries[0].step_id, second_run.step_entries[0].step_id,
            "each kick dispatches its own StepId — runs must not share step_entries"
        );
    }

    #[tokio::test]
    async fn rekick_unknown_task_returns_404() {
        let state = test_state();
        // `.expect_err()` needs the Ok variant to be `Debug`; `Json<T>`'s
        // `Debug` impl is not guaranteed for every `T` across axum versions,
        // so a plain match sidesteps that bound entirely.
        match task_rekick(State(state), Path("T-does-not-exist".to_string())).await {
            Ok(_) => panic!("expected 404 for an unknown task"),
            Err(e) => assert_eq!(e.status, StatusCode::NOT_FOUND),
        }
    }

    #[tokio::test]
    async fn run_get_unknown_id_returns_404() {
        let state = test_state();
        match run_get(State(state), Path("R-does-not-exist".to_string())).await {
            Ok(_) => panic!("expected 404 for an unknown run"),
            Err(e) => assert_eq!(e.status, StatusCode::NOT_FOUND),
        }
    }

    #[tokio::test]
    async fn task_get_unknown_id_returns_404() {
        let state = test_state();
        match task_get(State(state), Path("T-does-not-exist".to_string())).await {
            Ok(_) => panic!("expected 404 for an unknown task"),
            Err(e) => assert_eq!(e.status, StatusCode::NOT_FOUND),
        }
    }
}
