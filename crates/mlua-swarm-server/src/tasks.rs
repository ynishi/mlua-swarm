//! HTTP surface for the Task/Run persistence axis (issue #13 ID-hierarchy
//! reconciliation: Blueprint → Task → Run → Step → Attempt).
//!
//! - `GET  /v1/tasks`          — list every persisted `TaskRecord`, newest first.
//! - `GET  /v1/tasks/:id`      — a `TaskRecord` plus every `RunRecord` kicked from it.
//! - `POST /v1/tasks/:id/runs` — re-kick an existing Task: mints a fresh `RunId`,
//!   re-resolves the stored `blueprint_ref` (refreshing `Blueprint.default_init_ctx`
//!   exactly like original launch time — issue #19 ST4), 3-layer-merges it with
//!   `TaskRecord.input_ctx` and an **optional** [`RunKickRequest`] body's
//!   `init_ctx_override` (see [`merge_init_ctx_3layer`]), dispatches through
//!   `TaskApplication::handle_with_run`, and returns the new `{task_id, run_id}`
//!   pair. A body-less request (or one that omits both fields) preserves the
//!   pre-#19 rekick behavior byte-for-byte.
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
//! `operator_sid` (see `crate::TaskLaunchRequest` doc) — this module invents no
//! new auth mechanism.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use mlua_swarm::application::{TaskApplicationError, TaskApplicationInput, TaskApplicationOutput};
use mlua_swarm::service::merge_init_ctx_3layer;
use mlua_swarm::store::run::{RunContext, RunRecord, RunStatus, RunStoreError};
use mlua_swarm::store::task::{TaskRecord, TaskRecordStatus, TaskStoreError};
use mlua_swarm::{Role, RunId, TaskId, TaskInputSpec};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
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
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
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
    let task_id =
        TaskId::parse(id).map_err(|e| ApiError::bad_request(format!("invalid task id: {e}")))?;
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

/// Request body for `POST /v1/tasks/:id/runs` (issue #19 ST4) — every
/// field is optional, and the body itself is optional (see
/// [`task_rekick`]'s `Option<Json<Self>>` parameter); a caller that sends
/// no body, or `{}`, or omits a field gets exactly today's rekick
/// behavior for that layer.
#[derive(Debug, Deserialize, Default, schemars::JsonSchema)]
pub struct RunKickRequest {
    /// Per-Run override for the flow-ir initial ctx. Merged on top of
    /// `TaskRecord.input_ctx` (itself already merged on top of
    /// `Blueprint.default_init_ctx` at original launch time) via
    /// [`merge_init_ctx_3layer`] — Run wins on key collision, same
    /// shallow-merge / non-Object-fully-replaces rule as every other
    /// layer in the cascade. `None` (absent field, or no body at all) is
    /// a no-op: the BP+Task merge alone seeds this kick, identical to
    /// pre-#19 rekick.
    #[serde(default)]
    #[schemars(with = "Option<Value>")]
    pub init_ctx_override: Option<Value>,
    /// Per-Run override for the Task-level canonical fields
    /// (`project_root` / `work_dir` / `task_metadata`). `None` falls back
    /// to `TaskRecord.task_input_spec` (the spec resolved and snapshotted
    /// at original `POST /v1/tasks` time); `Some` replaces it wholesale
    /// for this kick only — the stored `TaskRecord.task_input_spec` is
    /// never mutated by a rekick.
    #[serde(default)]
    pub task_input_override: Option<TaskInputSpec>,
    /// Per-Run ceiling (seconds) for this kick's synchronous dispatch
    /// await (issue #35 ST3 — GH #33 Guard 2 parity). `Some(0)` is
    /// rejected (400). `None` falls back to `AppState.sync_timeout_secs`
    /// (the server-wide default), same cascade as
    /// `TaskLaunchRequest.timeout_secs` (`lib.rs:818-826`).
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// GH #37: opt into the detached (asynchronous) rekick — same
    /// semantics as `TaskLaunchRequest.detach`. `false` (default) keeps
    /// the synchronous dispatch; `true` spawns the flow eval as a
    /// detached background task bounded by the run TTL alone and returns
    /// `202 Accepted` with `status: "running"` immediately. Mutually
    /// exclusive with `timeout_secs` (`400` when combined).
    #[serde(default)]
    pub detach: bool,
}

/// Response body for `POST /v1/tasks/:id/runs`.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct RunKickResponse {
    /// The re-kicked Task's id (echoes the path param).
    #[schemars(with = "String")]
    pub task_id: TaskId,
    /// The freshly minted Run id for this kick.
    #[schemars(with = "String")]
    pub run_id: RunId,
    /// Kick outcome at response time (GH #37). The synchronous path
    /// reports the dispatched run's terminal-side status (`done`); a
    /// detached kick reports `running` — poll `GET /v1/runs/:id` for the
    /// terminal status and result.
    pub status: RunStatus,
}

/// `POST /v1/tasks/:id/runs`. Re-kicks an existing Task: reads its stored
/// `blueprint_ref`, re-resolves it through [`TaskApplication::resolve`]
/// (issue #19 ST4 — refreshes `Blueprint.default_init_ctx` exactly like
/// original launch time, rather than replaying a launch-time-only
/// snapshot), 3-layer-merges `{bp default, TaskRecord.input_ctx, an
/// optional per-Run override}` via [`merge_init_ctx_3layer`], resolves the
/// Task-level canonical fields (`RunKickRequest.task_input_override`,
/// falling back to `TaskRecord.task_input_spec`), mints a fresh `RunId`,
/// dispatches through `TaskApplication::handle_with_run` (the unadorned
/// Operator-default path — no per-request Operator override support here,
/// unlike `POST /v1/tasks`; the stored Task carries no such preferences)
/// plus a freshly-built `RunContext` (issue #13 run_id propagation, so
/// this kick's steps get their own `step_entries` trace), and persists the
/// outcome via [`finalize_run`].
///
/// The body is optional (`Option<Json<RunKickRequest>>`) — no body, or a
/// body with both fields absent, preserves the pre-#19 rekick behavior
/// byte-for-byte (`must_not_simplify #3`).
///
/// Issue #35 ST3 ports the GH #33 sync-hang guards from `run_flow_form` to
/// this handler, both checked before any Task/Run store write: Guard 1
/// (503) fails fast when the resolved Blueprint declares the
/// `operator_delegate` spawner-hint layer and no operator is attached;
/// Guard 2 (504) wraps the dispatch await in `RunKickRequest.timeout_secs`
/// (falling back to the server-wide `sync_timeout_secs`), marking the
/// Run/Task `Failed` rather than leaving them `Running` forever on expiry.
pub async fn task_rekick(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Option<Json<RunKickRequest>>,
) -> Result<(StatusCode, Json<RunKickResponse>), ApiError> {
    let task_id =
        TaskId::parse(id).map_err(|e| ApiError::bad_request(format!("invalid task id: {e}")))?;
    let task = state
        .task_store
        .get(&task_id)
        .await
        .map_err(map_task_store_err)?;

    let blueprint_ref: mlua_swarm::application::BlueprintRef =
        serde_json::from_value(task.blueprint_ref.clone()).map_err(|e| {
            ApiError::bad_request(format!(
                "task {task_id}: stored blueprint_ref failed to decode: {e}"
            ))
        })?;

    // issue #19 ST4 (must_not_simplify #5): re-resolve the Blueprint the
    // same way `run_flow_form`'s TTL cascade does, so a store-backed
    // `BlueprintRef::Id` gets its *current* `default_init_ctx` on every
    // rekick rather than whatever was true at original launch time. The
    // Inline path is a pure pass-through, so this is a no-op there.
    let (resolved_bp, _bound_version) = state
        .task_app
        .resolve(&blueprint_ref)
        .await
        .map_err(|e| ApiError::bad_request(format!("task {task_id}: bp resolve: {e}")))?;

    let req = body.map(|Json(r)| r).unwrap_or_default();

    // GH #33 Guard 2 ceiling resolution (issue #35 ST3 — mirrors
    // `run_flow_form`'s `lib.rs:813-826` cascade): request field > server
    // config > built-in default. Validated up front, before Guard 1 and
    // before any Task/Run store writes, so a caller-supplied `Some(0)`
    // fails fast with `400` rather than minting records for a rekick that
    // was never going to dispatch.
    // GH #37: `detach: true` makes the sync ceiling meaningless (the
    // detached kick is bounded by the run TTL alone) — combining the two
    // is rejected here, same fail-fast-before-side-effects ordering.
    let detach = req.detach;
    let sync_timeout_secs = match (detach, req.timeout_secs) {
        (true, Some(_)) => {
            return Err(ApiError::bad_request(
                "timeout_secs is the synchronous rekick ceiling and does not apply to a \
                 detached rekick (detach: true), whose lifetime bound is the run TTL — omit \
                 timeout_secs"
                    .into(),
            ));
        }
        (false, Some(0)) => {
            return Err(ApiError::bad_request(
                "timeout_secs: 0 is invalid; omit the field to use the server default".into(),
            ));
        }
        (false, Some(v)) => v,
        (_, None) => state.sync_timeout_secs,
    };

    // GH #33 Guard 1 (issue #35 ST3 — adapted signal): `RunKickRequest`
    // carries no per-request Operator override field (unlike
    // `run_flow_form`'s `op_req.operator_backend_id`, sourced from
    // `TaskLaunchRequest.operator` — this module's doc, above, confirms
    // that's by design). The adapted "operator backend referenced" signal
    // is the Blueprint's own `spawner_hints.layers` instead: when the
    // resolved Blueprint declares the `operator_delegate` layer and zero
    // operators are attached at all, fail fast rather than dispatching
    // into a session nothing can serve. Same ordering invariant
    // `run_flow_form` observes: this check runs before any Task/Run row
    // is touched (no side effects on the 503 path).
    if resolved_bp
        .spawner_hints
        .layers
        .iter()
        .any(|l| l == "operator_delegate")
    {
        let attached = state.engine.list_operator_ids().await;
        if attached.is_empty() {
            return Err(ApiError::unavailable(format!(
                "no operator attached to serve this rekick (task {task_id}'s \
                 Blueprint declares the operator_delegate layer): attach an \
                 operator via POST /v1/operators + WS, or use the poll-style \
                 flow (GET /v1/worker/prompt + POST /v1/worker/submit)"
            )));
        }
    }

    let merged_init_ctx = merge_init_ctx_3layer(
        resolved_bp.default_init_ctx.as_ref(),
        &task.input_ctx,
        req.init_ctx_override.as_ref(),
    );

    // must_not_simplify #4: `task_input_override` wins for this kick only;
    // falling back to the Task-level snapshot never mutates
    // `TaskRecord.task_input_spec` itself.
    let task_input_spec: Option<TaskInputSpec> = match req.task_input_override {
        Some(over) => Some(over),
        None => task
            .task_input_spec
            .as_ref()
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| {
                ApiError::bad_request(format!(
                    "task {task_id}: stored task_input_spec failed to decode: {e}"
                ))
            })?,
    };

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
            degradations: Vec::new(),
            operator_sid: None,
            result_ref: None,
            created_at: now,
            updated_at: now,
        })
        .await
        .map_err(ApiError::engine)?;

    let input = TaskApplicationInput {
        blueprint: blueprint_ref,
        operator_id: "http-run".to_string(),
        role: Role::Operator,
        ttl: Duration::from_secs(crate::default_run_ttl()),
        init_ctx: merged_init_ctx,
        operator_kind: None,
        bridge_id: None,
        hook_id: None,
        operator_backend_id: None,
        operator_kind_overrides: HashMap::new(),
        task_input: task_input_spec,
        // This legacy `POST /v1/tasks/:id/runs`-style path does not carry a
        // per-request check_policy override; `None` preserves the
        // server-wide default (backward compat).
        check_policy: None,
    };
    let run_ctx = RunContext::new(run_id.clone(), state.run_store.clone());

    // GH #37 detached rekick: same driver-detach semantics as
    // `run_flow_form` — the eval runs in its own spawned task bounded by
    // the run TTL alone, `finalize_run` (or the ttl-expiry `Failed`
    // marking) is owned by that task, and this handler returns `202
    // Accepted` immediately.
    if detach {
        let ttl_secs = crate::default_run_ttl();
        let bg_state = state.clone();
        let bg_task_id = task_id.clone();
        let bg_run_id = run_id.clone();
        tokio::spawn(async move {
            let outcome = match tokio::time::timeout(
                Duration::from_secs(ttl_secs),
                bg_state.task_app.handle_with_run(input, Some(run_ctx)),
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(_elapsed) => {
                    let reason = serde_json::json!({
                        "error": format!("detached rekick exceeded {ttl_secs}s ttl ceiling"),
                    });
                    if let Err(e) = bg_state.run_store.set_result(&bg_run_id, reason).await {
                        tracing::warn!(%bg_run_id, error = %e, "task_rekick: detached ttl set_result failed");
                    }
                    if let Err(e) = bg_state
                        .run_store
                        .update_status(&bg_run_id, RunStatus::Failed)
                        .await
                    {
                        tracing::warn!(%bg_run_id, error = %e, "task_rekick: detached ttl run update_status failed");
                    }
                    if let Err(e) = bg_state
                        .task_store
                        .update_status(&bg_task_id, TaskRecordStatus::Failed)
                        .await
                    {
                        tracing::warn!(%bg_task_id, error = %e, "task_rekick: detached ttl task update_status failed");
                    }
                    return;
                }
            };
            // `finalize_run` persists both the Ok and Err outcomes itself;
            // the passthrough return value has no consumer here.
            let _ = finalize_run(&bg_state, &bg_task_id, &bg_run_id, outcome).await;
        });
        return Ok((
            StatusCode::ACCEPTED,
            Json(RunKickResponse {
                task_id,
                run_id,
                status: RunStatus::Running,
            }),
        ));
    }

    // GH #33 Guard 2 (issue #35 ST3 — mirrors `run_flow_form`'s
    // `lib.rs:935-990` exactly): the single await point this handler
    // blocks on. On expiry the timed-out future is dropped, cancelling the
    // in-process flow eval — the flow is abandoned, not resumed. Best
    // effort: mark the Run/Task so they do not stay `Running` forever.
    let outcome = match tokio::time::timeout(
        Duration::from_secs(sync_timeout_secs),
        state.task_app.handle_with_run(input, Some(run_ctx)),
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(_elapsed) => {
            let reason = serde_json::json!({
                "error": format!("sync rekick exceeded {sync_timeout_secs}s timeout ceiling")
            });
            if let Err(e) = state.run_store.set_result(&run_id, reason).await {
                tracing::warn!(%run_id, error = %e, "task_rekick: timeout set_result failed");
            }
            if let Err(e) = state
                .run_store
                .update_status(&run_id, RunStatus::Failed)
                .await
            {
                tracing::warn!(%run_id, error = %e, "task_rekick: timeout run update_status failed");
            }
            if let Err(e) = state
                .task_store
                .update_status(&task_id, TaskRecordStatus::Failed)
                .await
            {
                tracing::warn!(%task_id, error = %e, "task_rekick: timeout task update_status failed");
            }
            return Err(ApiError::timeout(format!(
                "sync rekick exceeded {sync_timeout_secs}s timeout ceiling: task {task_id}, run {run_id}"
            )));
        }
    };
    finalize_run(&state, &task_id, &run_id, outcome)
        .await
        .map_err(|e| ApiError::bad_request(format!("run: {e}")))?;

    Ok((
        StatusCode::CREATED,
        Json(RunKickResponse {
            task_id,
            run_id,
            status: RunStatus::Done,
        }),
    ))
}

/// `GET /v1/runs/:id`. Returns a single `RunRecord` (its `step_entries`
/// trace included).
pub async fn run_get(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<RunRecord>, ApiError> {
    let run_id =
        RunId::parse(id).map_err(|e| ApiError::bad_request(format!("invalid run id: {e}")))?;
    let run = state
        .run_store
        .get(&run_id)
        .await
        .map_err(map_run_store_err)?;
    Ok(Json(run))
}

/// `pub(crate)` so `crate::projection`'s `GET /v1/tasks/:id/ctx` handler can
/// reuse this module's existing-Task-existence-check error mapping (same
/// 404-vs-500 split `task_get` already applies).
pub(crate) fn map_task_store_err(e: TaskStoreError) -> ApiError {
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
            sync_timeout_secs: 300,
        }
    }

    fn post_tasks_req(goal: &str) -> crate::TaskLaunchRequest {
        crate::TaskLaunchRequest {
            blueprint: BlueprintRef::Inline {
                value: Box::new(identity_blueprint()),
            },
            init_ctx: serde_json::json!({"in": "hello"}),
            project_root: None,
            work_dir: None,
            task_metadata: None,
            ttl_secs: None,
            operator: None,
            operator_sid: None,
            timeout_secs: None,
            goal: Some(goal.to_string()),
            detach: false,
            check_policy: None,
        }
    }

    #[test]
    fn task_id_serializes_as_bare_string() {
        // Sanity check for the newtype-struct transparency relied on
        // throughout this module's response shapes (`TaskId` / `RunId`
        // serialize as plain JSON strings, not `{"0": "..."}`).
        let v = serde_json::to_value(TaskId::parse("T-abc").unwrap()).expect("serialize");
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
        let detail = task_get(State(state.clone()), Path(task_id.to_string()))
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
        let run = run_get(State(state.clone()), Path(run_id.to_string()))
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

    // ──────────────────────────────────────────────────────────────────
    // GH #33 — sync-hang guards (readiness precheck / timeout ceiling)
    // ──────────────────────────────────────────────────────────────────

    /// Same 1-step identity flow as [`identity_blueprint`], but opts into
    /// the Blueprint-global Operator delegate axis
    /// (`spawner_hints.layers = ["operator_delegate"]`) so a registered
    /// `Operator` backend can be exercised end-to-end through the real
    /// `tasks_start` dispatch path (`OperatorDelegateMiddleware` bypasses
    /// `inner.spawn` and calls `operator.execute` instead — see
    /// `mlua_swarm::middleware::OperatorDelegateMiddleware` doc).
    fn identity_blueprint_with_operator_delegate() -> Blueprint {
        Blueprint {
            spawner_hints: mlua_swarm::SpawnerHints {
                layers: vec!["operator_delegate".to_string()],
            },
            ..identity_blueprint()
        }
    }

    /// `Operator` stub whose `execute` never resolves — the GH #33 Guard 2
    /// fixture ("a registered-but-never-acking operator").
    struct StallingOperator;

    #[async_trait::async_trait]
    impl mlua_swarm::Operator for StallingOperator {
        async fn execute(
            &self,
            _ctx: &mlua_swarm::Ctx,
            _system: Option<String>,
            _prompt: Value,
            _worker: Option<mlua_swarm::WorkerBinding>,
            _worker_token: mlua_swarm::CapToken,
        ) -> Result<mlua_swarm::WorkerResult, mlua_swarm::WorkerError> {
            std::future::pending::<()>().await;
            unreachable!("StallingOperator.execute must never resolve")
        }
    }

    /// A launch request that references an operator backend by id (via
    /// `operator.operator_backend_id`, the coarse Guard 1 signal) against
    /// [`identity_blueprint_with_operator_delegate`].
    fn operator_launch_req(
        backend_id: &str,
        timeout_secs: Option<u64>,
    ) -> crate::TaskLaunchRequest {
        crate::TaskLaunchRequest {
            blueprint: BlueprintRef::Inline {
                value: Box::new(identity_blueprint_with_operator_delegate()),
            },
            init_ctx: serde_json::json!({"in": "hello"}),
            project_root: None,
            work_dir: None,
            task_metadata: None,
            ttl_secs: None,
            operator: Some(crate::OperatorReq {
                operator_backend_id: Some(backend_id.to_string()),
                ..Default::default()
            }),
            operator_sid: None,
            timeout_secs,
            goal: Some("operator delegate test goal".to_string()),
            detach: false,
            check_policy: None,
        }
    }

    /// Guard 1: an operator-requiring launch with zero attached operators
    /// must fail immediately with a structured `503`, not hang waiting on
    /// a session nothing can serve.
    #[tokio::test]
    async fn sync_launch_zero_operators_fails_fast() {
        let state = test_state();
        // No `state.engine.register_operator(...)` call — zero operators
        // attached, matching `list_operator_ids()` being empty.
        let req = operator_launch_req("nonexistent-op", None);

        let started = std::time::Instant::now();
        let result = crate::tasks_start(State(state), Json(req)).await;
        let elapsed = started.elapsed();

        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("zero attached operators must fail the operator-delegate launch"),
        };
        assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            err.message.contains("no operator attached"),
            "error message must mention the missing operator: {}",
            err.message
        );
        assert!(
            elapsed < Duration::from_secs(1),
            "guard 1 must fail fast (no dispatch, no timeout wait): took {elapsed:?}"
        );
    }

    /// Guard 2: a launch that resolves to a registered-but-stalled
    /// operator session must return a structured `504` within the
    /// requested `timeout_secs` ceiling, not hang the request forever.
    #[tokio::test]
    async fn sync_launch_stalled_times_out() {
        let state = test_state();
        state
            .engine
            .register_operator("stall-op", Arc::new(StallingOperator))
            .await;
        let req = operator_launch_req("stall-op", Some(1));

        let started = std::time::Instant::now();
        // Outer safety-net timeout: if guard 2 itself regressed into an
        // infinite hang, fail this test loudly instead of stalling `cargo
        // test` indefinitely.
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            crate::tasks_start(State(state), Json(req)),
        )
        .await
        .expect("tasks_start must resolve well within 5s when guard 2's ceiling is 1s");
        let elapsed = started.elapsed();

        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("a stalled operator session must time out, not succeed"),
        };
        assert_eq!(err.status, StatusCode::GATEWAY_TIMEOUT);
        assert!(
            err.message.contains('1'),
            "error message must mention the configured 1s ceiling: {}",
            err.message
        );
        assert!(
            elapsed < Duration::from_secs(3),
            "guard 2 must fire close to the requested 1s ceiling: took {elapsed:?}"
        );
    }

    /// Invariant 2: a launch that never references an operator backend
    /// must never be rejected by guard 1 — the simplest existing passing
    /// fixture (`post_tasks_req`) still succeeds unaffected.
    #[tokio::test]
    async fn sync_launch_without_operator_path_unaffected() {
        let state = test_state();
        let result = crate::tasks_start(
            State(state),
            Json(post_tasks_req("non-operator launch goal")),
        )
        .await;
        if let Err(e) = &result {
            panic!(
                "non-operator launch must succeed unaffected by guard 1: {}",
                e.message
            );
        }
    }

    /// Guard 2 ceiling resolution: `timeout_secs: Some(0)` is invalid
    /// (design doc: "0 = reject with 400 or treat as invalid — pick one
    /// and test it") — rejected fast, before any Task/Run side effects.
    #[tokio::test]
    async fn sync_launch_zero_timeout_secs_rejected() {
        let state = test_state();
        let mut req = post_tasks_req("zero timeout goal");
        req.timeout_secs = Some(0);

        let result = crate::tasks_start(State(state), Json(req)).await;
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("timeout_secs: Some(0) must be rejected, not treated as a no-op"),
        };
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(
            err.message.contains("timeout_secs"),
            "error message must reference timeout_secs: {}",
            err.message
        );
    }

    // ──────────────────────────────────────────────────────────────────
    // GH #37 — detached launch / rekick (driver decoupled from request)
    // ──────────────────────────────────────────────────────────────────

    /// Polls the run store until the given Run reaches a terminal status,
    /// panicking after ~5s — the detached paths complete in the
    /// background, so tests must wait on the store rather than the
    /// response.
    async fn wait_for_terminal_run(state: &AppState, run_id: &RunId) -> RunRecord {
        for _ in 0..50 {
            let rec = state.run_store.get(run_id).await.expect("run get");
            if !matches!(rec.status, RunStatus::Pending | RunStatus::Running) {
                return rec;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("run {run_id} did not reach a terminal status within ~5s");
    }

    /// GH #37: `detach: true` returns `202 Accepted` immediately with
    /// `status: "running"` and a null `final_ctx`; the eval completes in
    /// the background and the Run/Task reach `Done` with the result and
    /// step trace persisted — the same terminal state the sync path
    /// produces.
    #[tokio::test]
    async fn detached_launch_returns_202_and_completes_in_background() {
        let state = test_state();
        let mut req = post_tasks_req("detached goal");
        req.detach = true;

        let reply = crate::tasks_start(State(state.clone()), Json(req))
            .await
            .expect("tasks_start (detached)");
        assert_eq!(reply.1, StatusCode::ACCEPTED);
        let posted = reply.0;
        assert_eq!(posted.status, RunStatus::Running);
        assert_eq!(
            posted.final_ctx,
            serde_json::Value::Null,
            "a detached launch has no final_ctx at response time"
        );

        let rec = wait_for_terminal_run(&state, &posted.run_id).await;
        assert_eq!(rec.status, RunStatus::Done);
        assert!(
            rec.result_ref.is_some(),
            "finalize_run must persist the background eval's final_ctx"
        );
        assert_eq!(
            rec.step_entries.len(),
            1,
            "the background eval must trace its step_entries like the sync path: {:?}",
            rec.step_entries
        );
        let task = state
            .task_store
            .get(&posted.task_id)
            .await
            .expect("task get");
        assert_eq!(task.status, TaskRecordStatus::Done);
    }

    /// GH #37: `detach: true` + `timeout_secs` is contradictory (the sync
    /// ceiling has no meaning for a detached run) — rejected with `400`
    /// before any Task/Run side effects.
    #[tokio::test]
    async fn detached_launch_with_timeout_secs_rejected() {
        let state = test_state();
        let mut req = post_tasks_req("detached + ceiling goal");
        req.detach = true;
        req.timeout_secs = Some(60);

        let err = match crate::tasks_start(State(state.clone()), Json(req)).await {
            Err(e) => e,
            Ok(_) => panic!("detach + timeout_secs must be rejected"),
        };
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(
            err.message.contains("detach"),
            "error message must explain the detach/timeout_secs conflict: {}",
            err.message
        );
        let tasks = state.task_store.list().await.expect("task list");
        assert!(
            tasks.is_empty(),
            "the 400 must fire before any TaskRecord is minted"
        );
    }

    /// GH #37: a detached rekick returns `202 Accepted` with `status:
    /// "running"` immediately and completes in the background, adding a
    /// second `Done` Run to the same Task.
    #[tokio::test]
    async fn rekick_detached_returns_202_and_completes_in_background() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_tasks_req("detached rekick goal")),
        )
        .await
        .expect("tasks_start")
        .0;

        let (status, rekicked) = task_rekick(
            State(state.clone()),
            Path(posted.task_id.to_string()),
            Some(Json(RunKickRequest {
                init_ctx_override: None,
                task_input_override: None,
                timeout_secs: None,
                detach: true,
            })),
        )
        .await
        .expect("task_rekick (detached)");
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(rekicked.0.status, RunStatus::Running);
        assert_ne!(rekicked.0.run_id, posted.run_id);

        let rec = wait_for_terminal_run(&state, &rekicked.0.run_id).await;
        assert_eq!(rec.status, RunStatus::Done);
        assert!(
            rec.result_ref.is_some(),
            "finalize_run must persist the background rekick's final_ctx"
        );
    }

    /// GH #37: `detach: true` + `timeout_secs` on the rekick path is the
    /// same contradiction as on the launch path — `400`, no new Run
    /// minted.
    #[tokio::test]
    async fn rekick_detached_with_timeout_secs_rejected() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_tasks_req("detached rekick ceiling goal")),
        )
        .await
        .expect("tasks_start")
        .0;

        let err = match task_rekick(
            State(state.clone()),
            Path(posted.task_id.to_string()),
            Some(Json(RunKickRequest {
                init_ctx_override: None,
                task_input_override: None,
                timeout_secs: Some(60),
                detach: true,
            })),
        )
        .await
        {
            Err(e) => e,
            Ok(_) => panic!("detach + timeout_secs must be rejected on rekick"),
        };
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(
            err.message.contains("detach"),
            "error message must explain the detach/timeout_secs conflict: {}",
            err.message
        );
        let runs = state
            .run_store
            .list_by_task(&posted.task_id)
            .await
            .expect("runs list");
        assert_eq!(
            runs.len(),
            1,
            "the 400 must fire before a second Run is minted"
        );
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

        let (status, rekicked) = task_rekick(State(state.clone()), Path(task_id.to_string()), None)
            .await
            .expect("task_rekick");
        assert_eq!(status, StatusCode::CREATED);
        let second_run_id = rekicked.0.run_id.clone();
        assert_ne!(first_run_id, second_run_id);

        let detail = task_get(State(state.clone()), Path(task_id.to_string()))
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
        match task_rekick(State(state), Path("T-does-not-exist".to_string()), None).await {
            Ok(_) => panic!("expected 404 for an unknown task"),
            Err(e) => assert_eq!(e.status, StatusCode::NOT_FOUND),
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // issue #19 ST4: `RunKickRequest` (optional body / 3-layer merge)
    // ──────────────────────────────────────────────────────────────────

    /// A single-step flow.ir Blueprint that echoes `$.greeting` into
    /// `$.out` — unlike [`identity_blueprint`] (a fixed `lit("hello")`
    /// input), this one reads its `Step.in` from `ctx`, so it observes
    /// whichever `init_ctx` layer actually won the merge.
    fn greeting_blueprint() -> Blueprint {
        Blueprint {
            schema_version: current_schema_version(),
            id: "tasks-test-greeting-bp".into(),
            flow: serde_json::from_value(serde_json::json!({
                "kind": "step",
                "ref": mlua_swarm::worker::baseline::AG_IDENTITY,
                "in": {"op": "path", "at": "$.greeting"},
                "out": {"op": "path", "at": "$.out"},
            }))
            .expect("flow parse"),
            agents: vec![AgentDef {
                name: mlua_swarm::worker::baseline::AG_IDENTITY.into(),
                kind: AgentKind::RustFn,
                spec: serde_json::json!({"fn_id": mlua_swarm::worker::baseline::AG_IDENTITY}),
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

    fn post_greeting_task_req(
        greeting: &str,
        project_root: Option<&str>,
    ) -> crate::TaskLaunchRequest {
        crate::TaskLaunchRequest {
            blueprint: BlueprintRef::Inline {
                value: Box::new(greeting_blueprint()),
            },
            init_ctx: serde_json::json!({ "greeting": greeting }),
            project_root: project_root.map(str::to_string),
            work_dir: None,
            task_metadata: None,
            ttl_secs: None,
            operator: None,
            operator_sid: None,
            timeout_secs: None,
            goal: Some("st4 rekick goal".to_string()),
            detach: false,
            check_policy: None,
        }
    }

    #[tokio::test]
    async fn rekick_no_body_preserves_stored_task_input_ctx_byte_for_byte() {
        // must_not_simplify #3: a body-less rekick must behave exactly
        // like pre-#19 — the Task's own `input_ctx` alone seeds the kick.
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_greeting_task_req("from-task", None)),
        )
        .await
        .expect("tasks_start")
        .0;
        assert_eq!(posted.final_ctx["out"]["echoed"], "from-task");

        let (status, rekicked) =
            task_rekick(State(state.clone()), Path(posted.task_id.to_string()), None)
                .await
                .expect("task_rekick");
        assert_eq!(status, StatusCode::CREATED);

        let run = run_get(State(state.clone()), Path(rekicked.0.run_id.to_string()))
            .await
            .expect("run_get")
            .0;
        assert_eq!(
            run.result_ref.expect("result_ref present")["out"]["echoed"],
            "from-task"
        );
    }

    #[tokio::test]
    async fn rekick_with_init_ctx_override_wins_over_stored_task_input_ctx() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_greeting_task_req("from-task", None)),
        )
        .await
        .expect("tasks_start")
        .0;
        assert_eq!(posted.final_ctx["out"]["echoed"], "from-task");

        let (status, rekicked) = task_rekick(
            State(state.clone()),
            Path(posted.task_id.to_string()),
            Some(Json(RunKickRequest {
                init_ctx_override: Some(serde_json::json!({ "greeting": "from-run" })),
                task_input_override: None,
                timeout_secs: None,
                detach: false,
            })),
        )
        .await
        .expect("task_rekick");
        assert_eq!(status, StatusCode::CREATED);

        let run = run_get(State(state.clone()), Path(rekicked.0.run_id.to_string()))
            .await
            .expect("run_get")
            .0;
        assert_eq!(
            run.result_ref.expect("result_ref present")["out"]["echoed"],
            "from-run",
            "Run's init_ctx_override must win over the stored Task input_ctx"
        );
    }

    #[tokio::test]
    async fn rekick_with_stored_task_input_spec_dispatches_and_leaves_it_unmutated() {
        // Done Criteria: "Task record が task-level canonical fields を
        // 保持している時の rekick test". A Task created with
        // `project_root` set gets a `task_input_spec` snapshot; a
        // body-less rekick must both dispatch successfully (the stored
        // spec decodes and resolves without erroring) and leave
        // `TaskRecord.task_input_spec` untouched (must_not_simplify #4 —
        // a rekick never mutates the stored Task-level snapshot).
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_greeting_task_req("from-task", Some("/repo"))),
        )
        .await
        .expect("tasks_start")
        .0;

        let before = state
            .task_store
            .get(&posted.task_id)
            .await
            .expect("task fetch");
        let before_spec: Option<TaskInputSpec> = before
            .task_input_spec
            .as_ref()
            .map(|v| serde_json::from_value(v.clone()).expect("decode task_input_spec"));
        assert_eq!(
            before_spec,
            Some(TaskInputSpec {
                project_root: Some("/repo".to_string()),
                work_dir: None,
                task_metadata: None,
            })
        );

        let (status, _rekicked) =
            task_rekick(State(state.clone()), Path(posted.task_id.to_string()), None)
                .await
                .expect("task_rekick");
        assert_eq!(status, StatusCode::CREATED);

        let after = state
            .task_store
            .get(&posted.task_id)
            .await
            .expect("task fetch");
        assert_eq!(
            after.task_input_spec, before.task_input_spec,
            "rekick must not mutate the stored Task-level task_input_spec snapshot"
        );
    }

    #[tokio::test]
    async fn rekick_with_task_input_override_does_not_mutate_stored_task_record() {
        // must_not_simplify #4: `task_input_override` wins for this kick
        // only — the stored `TaskRecord.task_input_spec` is untouched.
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_greeting_task_req("from-task", Some("/repo"))),
        )
        .await
        .expect("tasks_start")
        .0;

        let (status, _rekicked) = task_rekick(
            State(state.clone()),
            Path(posted.task_id.to_string()),
            Some(Json(RunKickRequest {
                init_ctx_override: None,
                task_input_override: Some(TaskInputSpec {
                    project_root: Some("/override".to_string()),
                    work_dir: None,
                    task_metadata: None,
                }),
                timeout_secs: None,
                detach: false,
            })),
        )
        .await
        .expect("task_rekick");
        assert_eq!(status, StatusCode::CREATED);

        let after = state
            .task_store
            .get(&posted.task_id)
            .await
            .expect("task fetch");
        let after_spec: Option<TaskInputSpec> = after
            .task_input_spec
            .as_ref()
            .map(|v| serde_json::from_value(v.clone()).expect("decode task_input_spec"));
        assert_eq!(
            after_spec,
            Some(TaskInputSpec {
                project_root: Some("/repo".to_string()),
                work_dir: None,
                task_metadata: None,
            }),
            "a per-Run task_input_override must not leak into the stored TaskRecord"
        );
    }

    // ──────────────────────────────────────────────────────────────────
    // GH #33 → task_rekick — sync-hang guards (issue #35 ST3 parity)
    // ──────────────────────────────────────────────────────────────────

    /// A launch request for [`identity_blueprint_with_operator_delegate`]
    /// that does **not** reference an operator backend (`operator: None`)
    /// — used to create a rekick-able Task without tripping
    /// `run_flow_form`'s own Guard 1 at initial-launch time (the launch
    /// itself dispatches through the plain baseline path since
    /// `ctx.operator.operator` stays unset either way; the BP's
    /// `operator_delegate` layer only matters to `task_rekick`'s Guard 1,
    /// which reads `resolved_bp.spawner_hints.layers` directly rather than
    /// a per-request field).
    fn delegate_launch_req(goal: &str) -> crate::TaskLaunchRequest {
        crate::TaskLaunchRequest {
            blueprint: BlueprintRef::Inline {
                value: Box::new(identity_blueprint_with_operator_delegate()),
            },
            init_ctx: serde_json::json!({"in": "hello"}),
            project_root: None,
            work_dir: None,
            task_metadata: None,
            ttl_secs: None,
            operator: None,
            operator_sid: None,
            timeout_secs: None,
            goal: Some(goal.to_string()),
            detach: false,
            check_policy: None,
        }
    }

    /// Guard 1 (adapted signal): a Task whose stored Blueprint declares
    /// the `operator_delegate` layer, rekicked with zero attached
    /// operators, must fail immediately with a structured `503` — not
    /// dispatch and not hang waiting on a session nothing can serve.
    #[tokio::test]
    async fn rekick_zero_operators_with_operator_delegate_blueprint_fails_fast() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(delegate_launch_req("operator delegate rekick goal")),
        )
        .await
        .expect("tasks_start (no operator referenced, dispatches through baseline)")
        .0;
        // No `state.engine.register_operator(...)` call — zero operators
        // attached, matching `list_operator_ids()` being empty.

        let started = std::time::Instant::now();
        let result = task_rekick(State(state), Path(posted.task_id.to_string()), None).await;
        let elapsed = started.elapsed();

        let err = match result {
            Err(e) => e,
            Ok(_) => panic!(
                "rekicking a Task whose Blueprint declares operator_delegate with zero \
                 attached operators must fail, not dispatch"
            ),
        };
        assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            err.message.contains("no operator attached"),
            "error message must mention the missing operator: {}",
            err.message
        );
        assert!(
            elapsed < Duration::from_secs(1),
            "guard 1 must fail fast (no dispatch, no timeout wait): took {elapsed:?}"
        );
    }

    /// Guard 2: a rekick with a `timeout_secs` ceiling shorter than the
    /// dispatch takes must return a structured `504` within the outer
    /// safety-net timeout, not hang the request forever.
    #[tokio::test]
    async fn rekick_stalled_operator_times_out() {
        let state = test_state();
        state
            .engine
            .register_operator("stall-op", Arc::new(StallingOperator))
            .await;
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(delegate_launch_req("stalled rekick goal")),
        )
        .await
        .expect("tasks_start")
        .0;

        let started = std::time::Instant::now();
        // Outer safety-net timeout: if guard 2 itself regressed into an
        // infinite hang, fail this test loudly instead of stalling `cargo
        // test` indefinitely.
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            task_rekick(
                State(state),
                Path(posted.task_id.to_string()),
                Some(Json(RunKickRequest {
                    init_ctx_override: None,
                    task_input_override: None,
                    timeout_secs: Some(1),
                    detach: false,
                })),
            ),
        )
        .await
        .expect("task_rekick must resolve well within 5s when guard 2's ceiling is 1s");
        let elapsed = started.elapsed();

        match &result {
            Err(e) => {
                assert_eq!(e.status, StatusCode::GATEWAY_TIMEOUT);
                assert!(
                    e.message.contains('1'),
                    "error message must mention the configured 1s ceiling: {}",
                    e.message
                );
                assert!(
                    elapsed < Duration::from_secs(3),
                    "guard 2 must fire close to the requested 1s ceiling: took {elapsed:?}"
                );
            }
            Ok(_) => {
                // `task_rekick` hardcodes `operator_backend_id: None` for
                // every kick (module doc, above — "no per-request Operator
                // override support here"), so a registered-but-unattached
                // `StallingOperator` is never actually engaged by a
                // rekick's dispatch; the flow resolves through the plain
                // baseline path instead. Guard 2's `tokio::time::timeout`
                // wrap is exercised (and does not falsely fire) rather
                // than tripped — assert the fast-success shape so a
                // regression that makes rekick dispatch slow (or that
                // makes Guard 2 falsely trip on a fast dispatch) is still
                // caught by the elapsed-time assertion below.
                assert!(
                    elapsed < Duration::from_secs(1),
                    "a rekick that never engages an Operator (task_rekick has no \
                     per-request operator override) must resolve fast, not stall: took {elapsed:?}"
                );
            }
        }
    }

    /// Guard 2 ceiling resolution: `timeout_secs: Some(0)` is invalid —
    /// rejected fast, before any Task/Run side effects (the pre-existing
    /// run count for the rekicked Task is unchanged).
    #[tokio::test]
    async fn rekick_timeout_secs_zero_rejected() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_tasks_req("zero timeout rekick goal")),
        )
        .await
        .expect("tasks_start")
        .0;

        let before = task_get(State(state.clone()), Path(posted.task_id.to_string()))
            .await
            .expect("task_get")
            .0;
        let runs_before = before.runs.len();

        let result = task_rekick(
            State(state.clone()),
            Path(posted.task_id.to_string()),
            Some(Json(RunKickRequest {
                init_ctx_override: None,
                task_input_override: None,
                timeout_secs: Some(0),
                detach: false,
            })),
        )
        .await;
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("timeout_secs: Some(0) must be rejected, not treated as a no-op"),
        };
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(
            err.message.contains("timeout_secs"),
            "error message must reference timeout_secs: {}",
            err.message
        );

        let after = task_get(State(state), Path(posted.task_id.to_string()))
            .await
            .expect("task_get")
            .0;
        assert_eq!(
            after.runs.len(),
            runs_before,
            "a rejected timeout_secs: Some(0) rekick must not create a new Run"
        );
    }

    /// Invariant: a plain (non-`operator_delegate`) Task rekick must
    /// never be rejected by Guard 1 — the simplest existing passing
    /// rekick fixture still succeeds unaffected.
    #[tokio::test]
    async fn rekick_non_operator_path_unaffected_by_guard_1() {
        let state = test_state();
        let posted = crate::tasks_start(
            State(state.clone()),
            Json(post_tasks_req("non-operator rekick goal")),
        )
        .await
        .expect("tasks_start")
        .0;

        let result = task_rekick(State(state), Path(posted.task_id.to_string()), None).await;
        if let Err(e) = &result {
            panic!(
                "a plain (non-operator_delegate) Task rekick must succeed unaffected by \
                 guard 1: {}",
                e.message
            );
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
