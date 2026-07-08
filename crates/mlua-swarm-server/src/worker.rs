//! HTTP `/v1/worker/*` endpoints (SubAgent self-fetch path).
//!
//! # 7-Entry pointer #6 (Output Event design)
//!
//! **This endpoint accesses `OutputStore` directly and does NOT go through the engine.**
//! It is one of the seven entry points enumerated in project `CLAUDE.md` §"Output Event
//! Design SoT". For the canonical description, see the crate root doc of
//! `mlua-swarm-output-store` (`cargo doc -p mlua-swarm-output-store`).
//!
//! # Path
//!
//! A thin-payload path where a SubAgent (= worker process launched by a MainAI) uses
//! the capability token it received via WS Spawn to self-fetch its prompt and
//! submit its result — putting the token in `Authorization: Bearer <encoded CapToken>`.
//!
//! ## Routes
//!
//! - `GET /v1/worker/prompt?task_id=<tid>` — via `engine.fetch_worker_payload`,
//!   returns `{task_id, attempt, agent, system?, prompt, context?}`.
//!   `context.steps` (`projection-adapter` ST5, [`assemble_step_pointers`])
//!   is assembled fresh on every fetch: a `ContextPolicy.steps`-filtered
//!   pointer list to preceding steps' OUTPUT, resolved through
//!   `crate::projection::McpQueryAdapter`'s Data-plane + `result_ref`
//!   enumeration — no separate MCP tool call needed to discover a prior
//!   step's OUTPUT.
//! - `POST /v1/worker/result` with body `{task_id, value, ok}` — appends one `Final`
//!   to the output tail via `engine.submit_output(Final)` (= the canonical path
//!   through which the dispatch layer decides Pass/Blocked) and updates
//!   `task.last_result` via `engine.post_result`.
//!
//! ## Bearer authentication
//!
//! The Bearer value is the string produced by `CapToken::encode()` (= URL-safe
//! base64 of serde_json). The server decodes it with `CapToken::decode` and then,
//! inside the engine, verifies HMAC sig + role × verb gate + TTL via
//! `verify_token_for_task` (= self-contained capability token; no server-side
//! store lookup required).
//!
//! Tokens are minted during the "2) mint outside the lock" phase of
//! `engine.dispatch_attempt` (`Role::Worker`, 600s TTL, `scopes=["*"]`).
//! The verb gate covers `FetchPrompt` / `EmitOutput` / `PostResult` — the worker
//! leaf capability set (`crate::types::WORKER_LEAF_VERBS`).

use axum::{
    extract::{Query, State},
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    Json,
};
use mlua_swarm::core::agent_context::StepPointer;
use mlua_swarm::{CapToken, ContentRef, OutputEvent, RunId, StepId, WorkerPayload};
use serde::Deserialize;
use serde_json::Value;

use crate::projection::McpQueryAdapter;
use crate::{ApiError, AppState};

/// Query params for `GET /v1/worker/prompt`.
#[derive(Debug, Deserialize)]
pub struct PromptQuery {
    /// Task the fetched prompt belongs to; cross-checked against the Bearer
    /// handle/token. Typed [`StepId`] since issue #14 — the wire shape stays
    /// a plain string; a bad prefix is rejected at deserialize.
    pub task_id: StepId,
}

/// `GET /v1/worker/prompt?task_id=<tid>`. Bearer = encoded `CapToken` or short `wh-` handle.
/// Thin HTTP wrapper over `engine.fetch_worker_payload` / `fetch_worker_payload_trusted`.
/// Short-handle path (recommended for SubAgents): handle → task_id
/// cross-check → trusted fetch.
/// Full-`CapToken` path: token decode → verify → fetch.
pub async fn worker_prompt(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<PromptQuery>,
) -> Result<Json<WorkerPayload>, ApiError> {
    let task_id = q.task_id;
    let bearer = extract_bearer_raw(&headers)?;
    let mut payload = if let Some(handle) = parse_worker_handle(&bearer) {
        // Short-handle path: verify handle → task_id (security: confirm the handle is bound to this task).
        let resolved = state
            .engine
            .task_id_from_handle(handle)
            .await
            .map_err(|e| ApiError::engine(format!("task_id_from_handle: {e}")))?;
        if resolved != task_id {
            return Err(ApiError::bad_request(format!(
                "handle {handle} is bound to task {resolved}, not {task_id}"
            )));
        }
        state
            .engine
            .fetch_worker_payload_trusted(&task_id)
            .await
            .map_err(|e| ApiError::engine(format!("fetch_worker_payload_trusted: {e}")))?
    } else {
        // Full CapToken path (the alternate Bearer form).
        let token = CapToken::decode(bearer.trim())
            .map_err(|e| ApiError::bad_request(format!("invalid token: {e}")))?;
        state
            .engine
            .fetch_worker_payload(&token, &task_id)
            .await
            .map_err(|e| ApiError::engine(format!("fetch_worker_payload: {e}")))?
    };
    assemble_step_pointers(&state, &mut payload).await;
    Ok(Json(payload))
}

/// Assembles `payload.context.steps` — the `ContextPolicy.steps`-filtered
/// pointer list to preceding steps' OUTPUT (`projection-adapter` ST5's
/// Worker axis; see `mlua_swarm::core::agent_context`'s module doc).
/// Resolved fresh on every fetch (not baked at spawn time), so a step
/// submitted after this agent spawned — but before it fetches its prompt
/// — is still visible.
///
/// No-op (`context.steps` stays empty) when: the payload carries no
/// `context` at all; the context has no `run_id` (a spawn that never
/// threaded one through — pre-run-tracking callers, or a spawner stack
/// without the Run-tracking layer); or the addressed Run cannot be
/// resolved. All three are fail-open, matching this crate's other
/// best-effort projection hooks (a missing pointer list must never turn a
/// would-have-succeeded fetch into a failure).
async fn assemble_step_pointers(state: &AppState, payload: &mut WorkerPayload) {
    let Some(context) = payload.context.as_mut() else {
        return;
    };
    let Some(run_id_str) = context.run_id.clone() else {
        return;
    };
    let Ok(run_id) = RunId::parse(run_id_str) else {
        return;
    };

    let adapter = McpQueryAdapter::new(state.data_store.clone(), state.run_store.clone());
    let Ok((run, resolved_steps)) = adapter.list_steps_by_run_id(&run_id).await else {
        return;
    };

    let policy = state
        .engine
        .context_policy_for(&payload.task_id, payload.attempt)
        .await;
    let self_name = payload.agent.clone();

    let mut pointers = Vec::new();
    for step in &resolved_steps {
        if step.name == self_name || !policy.allows_step(&step.name) {
            continue;
        }
        if let Some((size_bytes, file_path, content_url, sha256)) =
            crate::projection::resolve_step_pointer_fields(state, &run, step).await
        {
            pointers.push(StepPointer {
                name: step.name.clone(),
                size_bytes,
                file_path,
                content_url,
                sha256,
            });
        }
    }
    context.steps = pointers;
}

/// Body for `POST /v1/worker/result`.
#[derive(Debug, Deserialize)]
pub struct WorkerResultReq {
    /// Task this result belongs to (looked up together with the Bearer
    /// token). Typed [`StepId`] since issue #14 (see [`PromptQuery`]).
    pub task_id: StepId,
    /// `WorkerResult.value` (= the value returned by the Operator: LLM inference result or tool execution result).
    pub value: Value,
    /// `WorkerResult.ok`. `false` makes the dispatch path decide Blocked
    /// (= same semantics as `OutputEvent::Final { ok: false, .. }` from a
    /// `SpawnerAdapter`). Defaults to `true`.
    #[serde(default = "default_ok_true")]
    pub ok: bool,
    /// Optional explicit attempt. Normally omitted (= the server looks up `task.attempt`).
    /// A carry for race-condition tests that need to write to a fixed attempt.
    #[serde(default)]
    pub attempt: Option<u32>,
}

fn default_ok_true() -> bool {
    true
}

/// `POST /v1/worker/result`. Bearer = encoded `CapToken`.
/// Fires `engine.submit_output(Final)` + `engine.post_result`.
pub async fn worker_result(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<WorkerResultReq>,
) -> Result<StatusCode, ApiError> {
    let token = decode_worker_bearer(&headers)?;
    let task_id = req.task_id.clone();

    // Use body-explicit attempt if provided; otherwise the current task.attempt.
    let attempt = match req.attempt {
        Some(n) => n,
        None => state
            .engine
            .task_attempt(&task_id)
            .await
            .map_err(|e| ApiError::engine(format!("task_attempt: {e}")))?,
    };

    let event = OutputEvent::Final {
        content: ContentRef::Inline {
            value: req.value.clone(),
        },
        ok: req.ok,
    };
    state
        .engine
        .submit_output(&token, &task_id, attempt, event)
        .await
        .map_err(|e| ApiError::engine(format!("submit_output: {e}")))?;
    state
        .engine
        .post_result(&token, &task_id, req.value)
        .await
        .map_err(|e| ApiError::engine(format!("post_result: {e}")))?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /v1/worker/submit`. Bearer = encoded `CapToken`. Body = raw text/octet.
///
/// Simplification-axis endpoint for SubAgents. Removes the JSON construction,
/// duplicated `task_id`, and JSON-escape burden of `/v1/worker/result` — the
/// worker completes a POST with just token + raw body. Origin: the recent clean-up
/// of the SubAgent contract drift (fewer IDs to pass around, multi-line escape
/// accidents eliminated).
///
/// Behavior:
/// - `task_id` is auto-looked-up server-side from the token (already bound to the `CapToken`).
/// - Body raw bytes go as-is into `Value::String` for `submit_output` + `post_result`.
/// - `ok=true` fixed (= the submit endpoint is success-path only). For the error
///   path, use `/v1/worker/result` with an explicit `ok=false`.
#[derive(Debug, Deserialize, Default)]
pub struct SubmitQuery {
    /// Optional. `ok=false` signals failure (= `DispatchOutcome::Blocked`, caught
    /// by the flow.ir Try path). Unspecified (`None`) is treated as `ok=true`
    /// (= normal success).
    #[serde(default)]
    pub ok: Option<bool>,
}

/// `POST /v1/worker/submit`. Simplified counterpart of [`worker_result`]:
/// the caller sends only the raw result body, `task_id` is resolved
/// server-side from the Bearer handle/token, and `ok` defaults to `true`
/// unless overridden via [`SubmitQuery::ok`]. See the module doc for the
/// short-handle vs full-`CapToken` Bearer forms.
pub async fn worker_submit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<SubmitQuery>,
    body: axum::body::Bytes,
) -> Result<StatusCode, ApiError> {
    // Bearer accepts either (a) `wh-<8 hex>` short handle (recommended for
    // SubAgents) or (b) base64-wrapped CapToken JSON (the full-token form).
    let bearer = extract_bearer_raw(&headers)?;
    let task_id = if let Some(handle) = parse_worker_handle(&bearer) {
        state
            .engine
            .task_id_from_handle(handle)
            .await
            .map_err(|e| ApiError::engine(format!("task_id_from_handle: {e}")))?
    } else {
        let token = CapToken::decode(bearer.trim())
            .map_err(|e| ApiError::bad_request(format!("invalid token: {e}")))?;
        state
            .engine
            .task_id_from_token(&token)
            .await
            .map_err(|e| ApiError::engine(format!("task_id_from_token: {e}")))?
    };
    let attempt = state
        .engine
        .task_attempt(&task_id)
        .await
        .map_err(|e| ApiError::engine(format!("task_attempt: {e}")))?;
    // Strip trailing whitespace (newlines, etc.) so flow.ir `Eq` string matches
    // don't drift on `"BLOCKED\n" == "BLOCKED"` false results. Origin: the recent clean-up
    // verdict_loop smoke — sharp-edge removal. Internal `\n` inside the raw bytes
    // is preserved (= only trailing).
    let body_str = String::from_utf8_lossy(&body).trim_end().to_string();
    let value = Value::String(body_str);

    // The handle path = trusted internal API (= the server-minted handle is validated
    // by the earlier lookup); the full-token path = existing verify-by-token API.
    // Both are reflected identically into final + last_result.
    // `?ok=false` in the query signals failure (= `DispatchOutcome::Blocked`,
    // the flow.ir Try catch path).
    let ok = q.ok.unwrap_or(true);
    state
        .engine
        .submit_worker_result_trusted(&task_id, attempt, value, ok)
        .await
        .map_err(|e| ApiError::engine(format!("submit_worker_result_trusted: {e}")))?;
    Ok(StatusCode::NO_CONTENT)
}

/// Extracts the raw string from the `Authorization` header (= strips the `Bearer `
/// prefix). To let `worker_submit` accept both short handles and full tokens, we
/// fetch the raw value before any decode.
fn extract_bearer_raw(headers: &HeaderMap) -> Result<String, ApiError> {
    let v = headers
        .get(AUTHORIZATION)
        .ok_or_else(|| ApiError::bad_request("missing Authorization header".into()))?
        .to_str()
        .map_err(|_| ApiError::bad_request("invalid Authorization header encoding".into()))?;
    let s = v
        .strip_prefix("Bearer ")
        .ok_or_else(|| ApiError::bad_request("Authorization must be 'Bearer <token>'".into()))?
        .trim();
    if s.is_empty() {
        return Err(ApiError::bad_request("Bearer is empty".into()));
    }
    Ok(s.to_string())
}

/// Decides whether the Bearer is a short handle (`wh-XXXXXXXX`). Returns
/// `Some(handle)` on a match, `None` otherwise (= caller proceeds to try decoding
/// as full `CapToken` JSON).
fn parse_worker_handle(s: &str) -> Option<&str> {
    let s = s.trim();
    if s.starts_with("wh-")
        && s.len() >= 5
        && s.len() <= 64
        && s[3..].chars().all(|c| c.is_ascii_alphanumeric())
    {
        Some(s)
    } else {
        None
    }
}

/// Decodes an encoded `CapToken` from `Authorization: Bearer <encoded CapToken>`.
/// Kept separate from `extract_bearer` (sid-only) — kept as a distinct fn so
/// that sid strings and encoded tokens are not confused, distinguishing them by type.
fn decode_worker_bearer(headers: &HeaderMap) -> Result<CapToken, ApiError> {
    let v = headers
        .get(AUTHORIZATION)
        .ok_or_else(|| ApiError::bad_request("missing Authorization header".into()))?
        .to_str()
        .map_err(|_| ApiError::bad_request("invalid Authorization header encoding".into()))?;
    let encoded = v
        .strip_prefix("Bearer ")
        .ok_or_else(|| ApiError::bad_request("Authorization must be 'Bearer <token>'".into()))?
        .trim();
    if encoded.is_empty() {
        return Err(ApiError::bad_request("Bearer token is empty".into()));
    }
    CapToken::decode(encoded).map_err(|e| ApiError::bad_request(format!("invalid token: {e}")))
}

// ──────────────────────────────────────────────────────────────────────────
// UT — `assemble_step_pointers` (`projection-adapter` ST5 Worker axis)
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use mlua_swarm::core::agent_context::AgentContextView;
    use mlua_swarm::core::config::EngineCfg;
    use mlua_swarm::core::engine::Engine;
    use mlua_swarm::store::output::{InMemoryOutputStore, OutputStore};
    use mlua_swarm::store::run::{InMemoryRunStore, RunRecord, RunStatus, RunStore, StepEntry};
    use mlua_swarm::store::task::InMemoryTaskStore;
    use mlua_swarm::{RunId, StepId, TaskId};
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// Per-module test-helper convention (this crate's established
    /// pattern — see e.g. `projection::tests::test_state`): a minimal
    /// `AppState` wired with the caller-supplied `data_store` / `run_store`
    /// so a test can seed both directly rather than driving a real
    /// dispatch through them.
    fn test_state(data_store: Arc<dyn OutputStore>, run_store: Arc<dyn RunStore>) -> AppState {
        let engine = Engine::new(EngineCfg::default());
        let compiler = mlua_swarm::Compiler::new(crate::default_registry());
        let launch = Arc::new(mlua_swarm::TaskLaunchService::new(engine.clone(), compiler));
        AppState {
            engine,
            sessions: Arc::new(Mutex::new(crate::SessionStore::default())),
            task_app: Arc::new(mlua_swarm::TaskApplication::new_inline_only(launch)),
            ws_operator_factory: None,
            data_store,
            operator_sessions: Arc::new(Mutex::new(HashMap::new())),
            roles_to_sid: Arc::new(Mutex::new(HashMap::new())),
            task_store: Arc::new(InMemoryTaskStore::new()),
            run_store,
            base_url: None,
        }
    }

    async fn append_final(
        data_store: &Arc<dyn OutputStore>,
        task_id: &str,
        producer: &str,
        value: Value,
    ) {
        data_store
            .append(
                task_id,
                1,
                producer,
                OutputEvent::Final {
                    content: ContentRef::Inline { value },
                    ok: true,
                },
                vec![],
            )
            .await
            .expect("append final");
    }

    fn step_entry(step_id: &StepId, step_ref: &str) -> StepEntry {
        StepEntry {
            step_id: step_id.clone(),
            step_ref: Some(step_ref.to_string()),
            status: Some("passed".to_string()),
            at: 0,
        }
    }

    fn run_record(task_id: &TaskId, run_id: &RunId, step_entries: Vec<StepEntry>) -> RunRecord {
        RunRecord {
            id: run_id.clone(),
            task_id: task_id.clone(),
            status: RunStatus::Running,
            step_entries,
            operator_sid: None,
            result_ref: None,
            created_at: 0,
            updated_at: 0,
        }
    }

    fn consumer_payload(consumer_step_id: &StepId, run_id: &RunId) -> WorkerPayload {
        WorkerPayload {
            task_id: consumer_step_id.clone(),
            attempt: 1,
            agent: "consumer".to_string(),
            system: None,
            prompt: String::new(),
            context: Some(AgentContextView {
                task_id: consumer_step_id.to_string(),
                agent: "consumer".to_string(),
                attempt: 1,
                run_id: Some(run_id.to_string()),
                ..Default::default()
            }),
        }
    }

    /// Test 1: `ContextPolicy.steps` unspecified (no policy seeded at all
    /// — `Engine::context_policy_for`'s "no entry" default is `None` /
    /// pass-all) → the fetch payload carries every submitted step's
    /// `StepPointer`.
    #[tokio::test]
    async fn context_policy_unspecified_yields_every_submitted_step() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let task_id = TaskId::new();
        let run_id = RunId::new();
        let planner_id = StepId::new();
        let coder_id = StepId::new();

        append_final(
            &data_store,
            planner_id.as_str(),
            "planner",
            json!({"plan": "x"}),
        )
        .await;
        append_final(
            &data_store,
            coder_id.as_str(),
            "coder",
            json!({"code": "y"}),
        )
        .await;
        run_store
            .create(run_record(
                &task_id,
                &run_id,
                vec![
                    step_entry(&planner_id, "planner"),
                    step_entry(&coder_id, "coder"),
                ],
            ))
            .await
            .expect("create run");

        let state = test_state(data_store, run_store);
        let consumer_id = StepId::new();
        let mut payload = consumer_payload(&consumer_id, &run_id);
        assemble_step_pointers(&state, &mut payload).await;

        let names: Vec<&str> = payload
            .context
            .as_ref()
            .expect("context")
            .steps
            .iter()
            .map(|p| p.name.as_str())
            .collect();
        assert!(names.contains(&"planner"), "names: {names:?}");
        assert!(names.contains(&"coder"), "names: {names:?}");
    }

    /// Test 2: `steps: ["planner"]` → only `planner`'s pointer is present.
    #[tokio::test]
    async fn context_policy_steps_include_list_filters_to_named_steps() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let task_id = TaskId::new();
        let run_id = RunId::new();
        let planner_id = StepId::new();
        let coder_id = StepId::new();
        append_final(&data_store, planner_id.as_str(), "planner", json!("x")).await;
        append_final(&data_store, coder_id.as_str(), "coder", json!("y")).await;
        run_store
            .create(run_record(
                &task_id,
                &run_id,
                vec![
                    step_entry(&planner_id, "planner"),
                    step_entry(&coder_id, "coder"),
                ],
            ))
            .await
            .expect("create run");

        let state = test_state(data_store, run_store);
        let consumer_id = StepId::new();
        state
            .engine
            .with_state("test.seed_policy", {
                let consumer_id = consumer_id.clone();
                move |s| {
                    s.context_policies.insert(
                        (consumer_id, 1),
                        mlua_swarm_schema::ContextPolicy {
                            steps: Some(vec!["planner".to_string()]),
                            ..Default::default()
                        },
                    );
                }
            })
            .await
            .expect("seed policy");

        let mut payload = consumer_payload(&consumer_id, &run_id);
        assemble_step_pointers(&state, &mut payload).await;

        let names: Vec<&str> = payload
            .context
            .as_ref()
            .expect("context")
            .steps
            .iter()
            .map(|p| p.name.as_str())
            .collect();
        assert_eq!(names, vec!["planner"], "names: {names:?}");
    }

    /// Test 3: `steps: []` → the pointer list is empty.
    #[tokio::test]
    async fn context_policy_steps_empty_list_yields_no_pointers() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let task_id = TaskId::new();
        let run_id = RunId::new();
        let planner_id = StepId::new();
        append_final(&data_store, planner_id.as_str(), "planner", json!("x")).await;
        run_store
            .create(run_record(
                &task_id,
                &run_id,
                vec![step_entry(&planner_id, "planner")],
            ))
            .await
            .expect("create run");

        let state = test_state(data_store, run_store);
        let consumer_id = StepId::new();
        state
            .engine
            .with_state("test.seed_policy", {
                let consumer_id = consumer_id.clone();
                move |s| {
                    s.context_policies.insert(
                        (consumer_id, 1),
                        mlua_swarm_schema::ContextPolicy {
                            steps: Some(vec![]),
                            ..Default::default()
                        },
                    );
                }
            })
            .await
            .expect("seed policy");

        let mut payload = consumer_payload(&consumer_id, &run_id);
        assemble_step_pointers(&state, &mut payload).await;

        assert!(payload.context.expect("context").steps.is_empty());
    }

    /// Test 4: `steps_exclude` wins over `steps` for a name in both.
    #[tokio::test]
    async fn context_policy_steps_exclude_wins_over_steps() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let task_id = TaskId::new();
        let run_id = RunId::new();
        let planner_id = StepId::new();
        let coder_id = StepId::new();
        append_final(&data_store, planner_id.as_str(), "planner", json!("x")).await;
        append_final(&data_store, coder_id.as_str(), "coder", json!("y")).await;
        run_store
            .create(run_record(
                &task_id,
                &run_id,
                vec![
                    step_entry(&planner_id, "planner"),
                    step_entry(&coder_id, "coder"),
                ],
            ))
            .await
            .expect("create run");

        let state = test_state(data_store, run_store);
        let consumer_id = StepId::new();
        state
            .engine
            .with_state("test.seed_policy", {
                let consumer_id = consumer_id.clone();
                move |s| {
                    s.context_policies.insert(
                        (consumer_id, 1),
                        mlua_swarm_schema::ContextPolicy {
                            steps: Some(vec!["planner".to_string(), "coder".to_string()]),
                            steps_exclude: vec!["planner".to_string()],
                            ..Default::default()
                        },
                    );
                }
            })
            .await
            .expect("seed policy");

        let mut payload = consumer_payload(&consumer_id, &run_id);
        assemble_step_pointers(&state, &mut payload).await;

        let names: Vec<&str> = payload
            .context
            .as_ref()
            .expect("context")
            .steps
            .iter()
            .map(|p| p.name.as_str())
            .collect();
        assert_eq!(names, vec!["coder"], "names: {names:?}");
    }

    /// Test 5 (in-flight window, subtask-4-style invariant): the Run has
    /// NOT finalized (`result_ref: None`, mirroring a Run still `Running`)
    /// yet the fetch payload still carries a `StepPointer` for a step
    /// already visible through the Data-plane store — the same mechanism
    /// `crates/mlua-swarm-server/src/projection.rs`'s
    /// `steps_list_returns_in_flight_step_output_before_run_completes`
    /// proves end-to-end through a real gated 2-step dispatch; this test
    /// isolates the same invariant at the `assemble_step_pointers` level.
    #[tokio::test]
    async fn in_flight_step_output_is_visible_before_run_finalizes() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let task_id = TaskId::new();
        let run_id = RunId::new();
        let step1_id = StepId::new();
        append_final(
            &data_store,
            step1_id.as_str(),
            "step1",
            json!({"step1_out": "hi"}),
        )
        .await;
        let mut run = run_record(&task_id, &run_id, vec![step_entry(&step1_id, "step1")]);
        run.status = RunStatus::Running;
        run.result_ref = None; // the in-flight window: not yet finalized.
        run_store.create(run).await.expect("create run");

        let state = test_state(data_store, run_store);
        let consumer_id = StepId::new();
        let mut payload = consumer_payload(&consumer_id, &run_id);
        assemble_step_pointers(&state, &mut payload).await;

        let steps = &payload.context.expect("context").steps;
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].name, "step1");
    }

    /// Test 6: the fetching agent's own name is always excluded, even if
    /// (e.g. a loop re-dispatching the same agent) it also appears in
    /// `run.step_entries` with a resolvable Data-plane record.
    #[tokio::test]
    async fn self_agent_name_is_always_excluded() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let task_id = TaskId::new();
        let run_id = RunId::new();
        let planner_id = StepId::new();
        let consumer_prior_id = StepId::new();
        append_final(&data_store, planner_id.as_str(), "planner", json!("x")).await;
        append_final(
            &data_store,
            consumer_prior_id.as_str(),
            "consumer",
            json!("self"),
        )
        .await;
        run_store
            .create(run_record(
                &task_id,
                &run_id,
                vec![
                    step_entry(&planner_id, "planner"),
                    step_entry(&consumer_prior_id, "consumer"),
                ],
            ))
            .await
            .expect("create run");

        let state = test_state(data_store, run_store);
        let consumer_id = StepId::new();
        let mut payload = consumer_payload(&consumer_id, &run_id);
        assemble_step_pointers(&state, &mut payload).await;

        let names: Vec<&str> = payload
            .context
            .as_ref()
            .expect("context")
            .steps
            .iter()
            .map(|p| p.name.as_str())
            .collect();
        assert!(!names.contains(&"consumer"), "names: {names:?}");
        assert!(names.contains(&"planner"), "names: {names:?}");
    }

    /// Test 7 (pointer-only invariant): a `StepPointer`'s serialized JSON
    /// carries no preview / content-bytes field — only `name` /
    /// `size_bytes` / `file_path?` / `content_url` / `sha256`.
    #[tokio::test]
    async fn step_pointer_serializes_with_no_preview_or_content_bytes() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let task_id = TaskId::new();
        let run_id = RunId::new();
        let planner_id = StepId::new();
        append_final(
            &data_store,
            planner_id.as_str(),
            "planner",
            json!({"plan": "do the thing, at length".repeat(50)}),
        )
        .await;
        run_store
            .create(run_record(
                &task_id,
                &run_id,
                vec![step_entry(&planner_id, "planner")],
            ))
            .await
            .expect("create run");

        let state = test_state(data_store, run_store);
        let consumer_id = StepId::new();
        let mut payload = consumer_payload(&consumer_id, &run_id);
        assemble_step_pointers(&state, &mut payload).await;

        let steps = &payload.context.expect("context").steps;
        assert_eq!(steps.len(), 1);
        let json_value = serde_json::to_value(&steps[0]).expect("serialize StepPointer");
        let obj = json_value.as_object().expect("object");
        for forbidden in ["preview", "content", "value", "bytes"] {
            assert!(
                !obj.contains_key(forbidden),
                "StepPointer must not carry a {forbidden:?} field: {obj:?}"
            );
        }
        assert!(obj.contains_key("name"));
        assert!(obj.contains_key("size_bytes"));
        assert!(obj.contains_key("content_url"));
        assert!(obj.contains_key("sha256"));
    }
}
