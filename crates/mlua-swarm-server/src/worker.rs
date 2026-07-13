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
//! - `POST /v1/worker/artifact?name=<name>` (GH #36 ST1) — stages one named
//!   part per POST via `engine.stage_worker_artifact_trusted`. Completing the
//!   attempt is still `POST /v1/worker/submit` / `/v1/worker/result` — this
//!   route only stages; the dispatch layer's Final-pull folds every staged
//!   part into `{"out": <final>, "parts": {<name>: <value>, ...}}`.
//! - `GET /v1/worker/prompt/system?task_id=<tid>&attempt=<n>` (GH #31) —
//!   raw baked `system` bytes for `(task_id, attempt)`, the `Http`-mode
//!   fetch target for `system_ref.uri`. Same Bearer flow as
//!   `/v1/worker/prompt`; body is `text/plain`, not JSON.
//! - `GET /v1/agents/:name/render-size` (GH #31) — no Bearer required, same
//!   trust tier as `GET /v1/blueprints/:id/head`. Live per-agent most-recently
//!   observed render size, backing `bp_doctor`'s post-render check.
//! - `POST /v1/worker/degradation` (GH #32) — structured JSON `{tool, error,
//!   fallback, note?}`, same Bearer flow as [`worker_submit`]. An
//!   **independent channel**: entries are appended to `RunRecord.degradations`
//!   via `RunStore::append_degradation` directly and never touch
//!   `OutputStore` / the fold path (Crux invariant 2 — a degradation must
//!   never surface as step OUTPUT). `step_ref` / `attempt` / `at` are
//!   server-injected, never trusted from the client. Silent `204` (no
//!   append) when the dispatch task carries no Run linkage — same
//!   fail-open contract as [`reject_if_run_terminal`]'s own resolution
//!   steps, since a pre-run-tracking dispatch has nowhere to record a
//!   degradation and that must not become a client-visible error.
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
    http::{header, header::AUTHORIZATION, HeaderMap, StatusCode},
    Json,
};
use mlua_swarm::core::agent_context::StepPointer;
use mlua_swarm::core::step_naming::StepNaming;
use mlua_swarm::store::run::{DegradationEntry, RunStatus, RunStoreError};
use mlua_swarm::{CapToken, ContentRef, EngineError, OutputEvent, RunId, StepId, WorkerPayload};
use mlua_swarm_schema::{ContextPolicy, VerdictChannel};
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
/// GH #23 subtask-3: `resolved_steps` (from
/// `McpQueryAdapter::list_steps_by_run_id`) always reports the CANONICAL
/// name (see `crate::projection`'s module doc), so both the self-exclusion
/// check and the `ContextPolicy` match are done against canonical names —
/// `payload.agent` (the raw `Step.ref` this fetching agent was dispatched
/// under) is canonicalized via `Engine::step_naming_for(&payload.task_id)`
/// (the FETCHING agent's own dispatch id — the same `StepNaming` `Arc`
/// every step of this Blueprint launch shares, see [`StepNaming`]'s module
/// doc), and `policy.allows_step` itself is left untouched (schema crate
/// stays name-agnostic) — [`allows_step_canonical`] is the caller-side seam
/// that resolves each policy-declared name through the table before
/// comparing.
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

    let adapter = McpQueryAdapter::new(
        state.data_store.clone(),
        state.run_store.clone(),
        state.engine.clone(),
    );
    let Ok((run, resolved_steps)) = adapter.list_steps_by_run_id(&run_id).await else {
        return;
    };

    let naming = state.engine.step_naming_for(&payload.task_id).await;
    let policy = state
        .engine
        .context_policy_for(&payload.task_id, payload.attempt)
        .await;
    let self_canonical = naming
        .as_deref()
        .and_then(|n| n.canonical_of_producer(&payload.agent))
        .map(str::to_string)
        .unwrap_or_else(|| payload.agent.clone());

    let mut pointers = Vec::new();
    for step in &resolved_steps {
        if step.name == self_canonical
            || !allows_step_canonical(&policy, naming.as_deref(), &step.name)
        {
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

/// GH #23 subtask-3: caller-side canonical/alias expansion for
/// `ContextPolicy.allows_step` — same precedence as
/// `ContextPolicy::allows_step` itself (`steps_exclude` wins; `steps:
/// None` = pass-all, `Some(list)` = named-only), but each
/// policy-declared name is resolved through the Blueprint's `StepNaming`
/// table before comparison, so a Blueprint author's `steps: [...]` entry
/// naming either the canonical projection name OR any alias (`Step.ref` /
/// the `out` ctx-path's top-level segment) matches the same step.
/// `ContextPolicy::allows_step` (schema crate) is untouched — this is the
/// GH #23 seam, kept out of the name-agnostic schema type. `naming: None`
/// degrades to a literal string comparison, byte-identical to
/// `ContextPolicy::allows_step` itself (defensive-only fallback, matching
/// `crate::projection::McpQueryAdapter::step_naming_for_run`'s own
/// contract).
fn allows_step_canonical(
    policy: &ContextPolicy,
    naming: Option<&StepNaming>,
    canonical_name: &str,
) -> bool {
    let resolves_to = |raw: &str| -> bool {
        match naming {
            Some(n) => n
                .resolve(raw)
                .map(|c| c == canonical_name)
                .unwrap_or(raw == canonical_name),
            None => raw == canonical_name,
        }
    };
    if policy
        .steps_exclude
        .iter()
        .any(|excluded| resolves_to(excluded))
    {
        return false;
    }
    match &policy.steps {
        None => true,
        Some(list) => list.iter().any(|included| resolves_to(included)),
    }
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
    // GH #51: completion-time verdict-contract enforcement now runs
    // inside `Engine::submit_output` itself (see
    // `map_completion_result`'s doc) — this route previously called no
    // gate at all.
    map_completion_result(
        state
            .engine
            .submit_output(&token, &task_id, attempt, event)
            .await,
        "submit_output",
    )?;
    state
        .engine
        .post_result(&token, &task_id, req.value)
        .await
        .map_err(|e| ApiError::engine(format!("post_result: {e}")))?;
    Ok(StatusCode::NO_CONTENT)
}

/// Body-level protocol prefix recognized by [`worker_submit`] and
/// [`worker_artifact`] (GH #42). When the trimmed request body starts with
/// this sentinel, the rest is treated as an absolute path; the file is
/// read and its contents replace the submitted body. Non-sentinel bodies
/// are unchanged.
///
/// See [`resolve_file_sentinel`] for the resolution rules and guards.
const FILE_SENTINEL_PREFIX: &str = "@file:";

/// Byte ceiling on the resolved-file body, matching the HTTP
/// `DefaultBodyLimit` applied to inline bodies at the router (2 MiB, see
/// the `/v1/worker/submit` layer in `crate::app_router`). Sentinel
/// bodies bypass that axum body layer (the request itself is small), so
/// the guard is checked in [`resolve_file_sentinel`] instead.
const FILE_SENTINEL_MAX_BYTES: u64 = 2 * 1024 * 1024;

/// `AgentContextView.extra` key that opts a step into `@file:` sentinel
/// resolution (GH #43). Declared through the GH #21 meta channels
/// (`Blueprint.metas` / `AgentMeta.ctx` / step-level `$step_meta`) and
/// folded into the view at spawn time by `AgentContextMiddleware`.
///
/// Default-deny: absent, or any value other than the strict boolean
/// `true` (a string `"true"` does not count), rejects the sentinel with
/// `400`. The v0.9.x line has no sentinel at all, so deny-by-default is
/// the released-behavior-compatible default; a step whose output
/// contract legitimately needs file submission opts in with one
/// declaration.
const FILE_SENTINEL_ALLOW_KEY: &str = "allow_file_submit";

/// Resolves the `@file:<abs-path>` sentinel (GH #42) when present at the
/// start of `body_str`. When absent, returns `body_str` unchanged — this
/// is the byte-for-byte compatible path for all pre-#42 workers.
///
/// # Sentinel form
///
/// The trimmed body is `@file:<abs-path>` on a single line — a worker
/// materializes the large payload to a file under its task's `work_dir`
/// with its existing `Write` capability, then submits the sentinel body
/// instead of streaming the payload back through the LLM.
///
/// # Guards
///
/// - Empty / multi-line path → `400`.
/// - Relative path → `400` (the allowlist works only in
///   canonicalized-absolute form).
/// - `AgentContextView` not materialized for `(task_id, attempt)` → `400`
///   (spawn must have run through `AgentContextMiddleware`; without a
///   view there is no allowlist root to check against).
/// - `view.extra[`[`FILE_SENTINEL_ALLOW_KEY`]`]` is not boolean `true` →
///   `400` (GH #43 — file submission is opt-in per step; default-deny).
/// - `view.work_dir` is `None` → `400`.
/// - Canonicalized path is not under canonicalized `work_dir` → `400`
///   (blocks `..`-escapes and symlinks pointing outside the allowlist).
/// - File does not exist → `404`.
/// - File size > [`FILE_SENTINEL_MAX_BYTES`] → `413`.
/// - Any other I/O / canonicalize error → `500`.
///
/// The resolved contents are `trim_end()`-ed to match the inline path's
/// own trailing-whitespace strip, so the downstream `Value::String` is
/// observationally identical whether the body arrived inline or via
/// sentinel.
async fn resolve_file_sentinel(
    state: &AppState,
    task_id: &StepId,
    attempt: u32,
    body_str: String,
) -> Result<String, ApiError> {
    let Some(rest) = body_str.strip_prefix(FILE_SENTINEL_PREFIX) else {
        return Ok(body_str);
    };
    let path_str = rest.trim();
    if path_str.is_empty() {
        return Err(ApiError::bad_request(
            "@file: sentinel: empty path".to_string(),
        ));
    }
    if path_str.contains('\n') || path_str.contains('\r') {
        return Err(ApiError::bad_request(
            "@file: sentinel: path must be a single line".to_string(),
        ));
    }
    let path = std::path::Path::new(path_str);
    if !path.is_absolute() {
        return Err(ApiError::bad_request(format!(
            "@file: sentinel: path must be absolute (got {path_str:?})"
        )));
    }
    let view = state
        .engine
        .agent_context_for(task_id, attempt)
        .await
        .ok_or_else(|| {
            ApiError::bad_request(
                "@file: sentinel: no AgentContextView for this task/attempt \
                 (spawn must run through AgentContextMiddleware to enable \
                 sentinel resolution)"
                    .to_string(),
            )
        })?;
    // GH #43: file submission is opt-in per step (default-deny). Strict
    // boolean `true` only — folded from the Blueprint meta channels by
    // `AgentContextMiddleware` at spawn time.
    if view.extra.get(FILE_SENTINEL_ALLOW_KEY) != Some(&Value::Bool(true)) {
        return Err(ApiError::bad_request(format!(
            "@file: sentinel: file submission is not allowed for this step \
             (declare `{FILE_SENTINEL_ALLOW_KEY}: true` via `$step_meta` / \
             `AgentMeta.ctx` / `Blueprint.metas`; strict boolean `true` \
             required)"
        )));
    }
    let work_dir = view.work_dir.ok_or_else(|| {
        ApiError::bad_request("@file: sentinel: task has no resolved work_dir".to_string())
    })?;
    let work_dir_canon = tokio::fs::canonicalize(&work_dir).await.map_err(|e| {
        ApiError::engine(format!(
            "@file: sentinel: canonicalize work_dir {work_dir:?}: {e}"
        ))
    })?;
    let path_canon = match tokio::fs::canonicalize(path).await {
        Ok(p) => p,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(ApiError::not_found(format!(
                "@file: sentinel: file not found: {path_str}"
            )));
        }
        Err(e) => {
            return Err(ApiError::engine(format!(
                "@file: sentinel: canonicalize {path_str:?}: {e}"
            )));
        }
    };
    if !path_canon.starts_with(&work_dir_canon) {
        return Err(ApiError::bad_request(format!(
            "@file: sentinel: path {} is not under work_dir {} (canonicalized: {} vs {})",
            path_str,
            work_dir,
            path_canon.display(),
            work_dir_canon.display(),
        )));
    }
    let meta = tokio::fs::metadata(&path_canon)
        .await
        .map_err(|e| ApiError::engine(format!("@file: sentinel: metadata {path_str:?}: {e}")))?;
    if meta.len() > FILE_SENTINEL_MAX_BYTES {
        return Err(ApiError::payload_too_large(format!(
            "@file: sentinel: file size {} exceeds limit {}",
            meta.len(),
            FILE_SENTINEL_MAX_BYTES
        )));
    }
    let bytes = tokio::fs::read(&path_canon)
        .await
        .map_err(|e| ApiError::engine(format!("@file: sentinel: read {path_str:?}: {e}")))?;
    // Match the `trim_end()` the inline path applies (see `worker_submit`).
    Ok(String::from_utf8_lossy(&bytes).trim_end().to_string())
}

/// GH #50 (Subtask 2) — submit-time verdict contract gate, shared by
/// [`worker_submit`] (`channel = Body`) and [`worker_artifact`]
/// (`channel = Part`, only when `name == "verdict"`). Enforcement Point 2
/// (the submit-time complement to `Compiler::compile`'s register-time lint
/// in `mlua_swarm::blueprint::compiler`, Enforcement Point 1) — called
/// after the final value string is resolved and BEFORE it is handed to
/// `submit_worker_result_trusted` / `stage_worker_artifact_trusted`, so a
/// rejected value never reaches the flow ctx.
///
/// No-op (`Ok(())`) in every case that must preserve pre-GH-#50 behavior
/// byte-for-byte:
/// - the dispatching agent declared no `VerdictContract` at all (opt-in).
/// - the agent's declared contract addresses the OTHER channel — a
///   channel/shape mismatch is the compile-time lint's job (Enforcement
///   Point 1); this gate only validates value membership for the channel
///   it was called for.
/// - `value` IS a member of the contract's declared `values`.
///
/// `Err(ApiError::unprocessable(..))` (HTTP 422) otherwise, echoing the
/// expected token set.
async fn check_verdict_contract(
    state: &AppState,
    task_id: &StepId,
    channel: VerdictChannel,
    value: &str,
) -> Result<(), ApiError> {
    let Some(contract) = state.engine.verdict_contract_for_task(task_id).await else {
        return Ok(());
    };
    if contract.channel != channel {
        return Ok(());
    }
    if contract.values.iter().any(|v| v == value) {
        return Ok(());
    }
    Err(ApiError::unprocessable(format!(
        "verdict contract violation: {value:?} is not a member of the declared values {:?}",
        contract.values
    )))
}

/// GH #51 — maps the 2 completion-time verdict-contract `EngineError`
/// variants (raised by the embedded choke point inside
/// `Engine::submit_worker_result_trusted` / `Engine::submit_output`) to
/// their `422` HTTP shape; every other `EngineError` variant falls back
/// to the pre-existing generic `500` `ApiError::engine` wrapping,
/// unchanged. Shared by [`worker_submit`] and [`worker_result`] — both
/// routes surface the SAME embedded engine-side check, so their
/// HTTP-layer error translation is identical too (this is HTTP
/// status-code translation, not the verdict-contract logic itself, which
/// stays the single engine-side choke point per GH #51's "not duplicated
/// into each route handler" constraint).
///
/// `context` labels the wrapped `EngineError`'s `Display` text for the
/// fallback `500` case only, matching the pre-existing
/// `format!("<call>: {e}")` style each call site used before this
/// helper.
fn map_completion_result<T>(result: Result<T, EngineError>, context: &str) -> Result<T, ApiError> {
    result.map_err(|e| match e {
        EngineError::VerdictValueRejected { value, allowed } => ApiError::unprocessable(format!(
            "verdict contract violation: {value:?} is not a member of the declared values {allowed:?}"
        )),
        EngineError::VerdictPartMissing { allowed } => ApiError::unprocessable(format!(
            "verdict contract violation: no staged \"verdict\" part found for this attempt; declared values {allowed:?}"
        )),
        other => ApiError::engine(format!("{context}: {other}")),
    })
}

/// `POST /v1/worker/submit`. Bearer = encoded `CapToken`. Body = raw text/octet.
///
/// Simplification-axis endpoint for SubAgents. Removes the JSON construction,
/// duplicated `task_id`, and JSON-escape burden of `/v1/worker/result` — the
/// worker completes a POST with just token + raw body. Origin: the recent clean-up
/// of the SubAgent contract drift (fewer IDs to pass around, multi-line escape
/// accidents eliminated).
///
/// **GH #42 `@file:` sentinel**: workers whose result body is too large to
/// re-emit inline (multi-KB structured output) may `Write` the payload to
/// a file under their task's `work_dir` and submit the body
/// `@file:<abs-path>` instead — see [`resolve_file_sentinel`].
/// Non-sentinel bodies pass through unchanged. The step must opt in via
/// `allow_file_submit: true` (GH #43, default-deny — see
/// [`FILE_SENTINEL_ALLOW_KEY`]).
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
    // GH #37: fail loud (410) instead of silently accepting a submit whose
    // addressed Run is already terminal — see `reject_if_run_terminal`.
    reject_if_run_terminal(&state, &task_id, attempt).await?;
    // Strip trailing whitespace (newlines, etc.) so flow.ir `Eq` string matches
    // don't drift on `"BLOCKED\n" == "BLOCKED"` false results. Origin: the recent clean-up
    // verdict_loop smoke — sharp-edge removal. Internal `\n` inside the raw bytes
    // is preserved (= only trailing).
    let body_str = String::from_utf8_lossy(&body).trim_end().to_string();
    // GH #42: `@file:<abs-path>` sentinel — pass through unchanged when
    // absent (byte-for-byte compat with pre-#42 callers).
    let body_str = resolve_file_sentinel(&state, &task_id, attempt, body_str).await?;
    // GH #51: the `channel: "body"` submit-time check formerly performed
    // here (`check_verdict_contract(&state, &task_id, VerdictChannel::Body,
    // ..)`) is now performed inside `Engine::submit_worker_result_trusted`
    // itself — the single completion-time choke point shared by all 3
    // completion routes (see `map_completion_result`'s doc). No separate
    // call is needed here; `check_verdict_contract` remains in use by
    // `worker_artifact`'s staging-time `name == "verdict"` early
    // validation, unchanged.
    let value = Value::String(body_str);

    // The handle path = trusted internal API (= the server-minted handle is validated
    // by the earlier lookup); the full-token path = existing verify-by-token API.
    // Both are reflected identically into final + last_result.
    // `?ok=false` in the query signals failure (= `DispatchOutcome::Blocked`,
    // the flow.ir Try catch path).
    let ok = q.ok.unwrap_or(true);
    map_completion_result(
        state
            .engine
            .submit_worker_result_trusted(&task_id, attempt, value, ok)
            .await,
        "submit_worker_result_trusted",
    )?;
    Ok(StatusCode::NO_CONTENT)
}

/// Query params for `POST /v1/worker/artifact`.
#[derive(Debug, Deserialize)]
pub struct ArtifactQuery {
    /// Artifact name (GH #36 ST1: named multi-part worker output). Required
    /// and non-empty (400 otherwise) — becomes the object key
    /// `Engine::dispatch_attempt_with`'s Final-pull folds this part under
    /// (`{"out": <final>, "parts": {<name>: <value>, ...}}`, see that
    /// method's doc). No character restriction is enforced here (a BP
    /// author references it via bracket notation, e.g. `$.out.parts["a.b"]`).
    pub name: String,
}

/// `POST /v1/worker/artifact?name=<name>`. Bearer = same short-handle /
/// full-`CapToken` forms as [`worker_submit`]. Body = raw text/octet.
///
/// Simplification-axis sibling of [`worker_submit`] (GH #36 ST1): lets a
/// worker with more than one named result POST each part independently —
/// same 1-part-per-POST simplicity as `/v1/worker/submit`, no Single Big
/// JSON the worker has to construct/escape itself — then complete the
/// attempt with an ordinary `/v1/worker/submit` (unchanged). Staging alone
/// never completes the attempt; `dispatch_attempt_with` only pulls the
/// tail's `Final` (whichever endpoint submits it) and folds every staged
/// `Artifact` into `"parts"` at that point.
///
/// Behavior:
/// - `task_id` is auto-looked-up server-side from the token/handle, same as
///   [`worker_submit`].
/// - `name` is required and non-empty; missing or blank → 400.
/// - Body raw bytes go as-is into `Value::String` (same trailing-whitespace
///   trim as `worker_submit`) and are staged via
///   [`mlua_swarm::core::engine::Engine::stage_worker_artifact_trusted`].
/// - Staging the same `name` twice within one attempt: last write wins (the
///   Final-pull fold walks the tail in event order — see its doc).
pub async fn worker_artifact(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<ArtifactQuery>,
    body: axum::body::Bytes,
) -> Result<StatusCode, ApiError> {
    let name = q.name.trim();
    if name.is_empty() {
        return Err(ApiError::bad_request("name must not be empty".into()));
    }
    let name = name.to_string();

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
    // GH #37: fail loud (410) instead of silently staging a part whose
    // addressed Run is already terminal — see `reject_if_run_terminal`.
    reject_if_run_terminal(&state, &task_id, attempt).await?;
    let body_str = String::from_utf8_lossy(&body).trim_end().to_string();
    // GH #42: same `@file:<abs-path>` sentinel as `worker_submit`.
    let body_str = resolve_file_sentinel(&state, &task_id, attempt, body_str).await?;
    // GH #50: submit-time verdict contract gate (Enforcement Point 2),
    // ONLY for the literal `"verdict"` part name (Pattern B's staging
    // channel — see `blueprint-authoring.md`'s "Returning verdicts to
    // drive BP flow" section). Every other part name skips the gate
    // entirely, unchanged from pre-GH-#50 behavior.
    if name == "verdict" {
        check_verdict_contract(&state, &task_id, VerdictChannel::Part, &body_str).await?;
    }
    let value = Value::String(body_str);

    state
        .engine
        .stage_worker_artifact_trusted(&task_id, attempt, name, value)
        .await
        .map_err(|e| ApiError::engine(format!("stage_worker_artifact_trusted: {e}")))?;
    Ok(StatusCode::NO_CONTENT)
}

/// Body for `POST /v1/worker/degradation` (GH #32).
#[derive(Debug, Deserialize)]
pub struct DegradationBody {
    /// The tool (or capability) the worker attempted to use.
    pub tool: String,
    /// The error that triggered the fallback, in the worker's own words.
    pub error: String,
    /// What the worker substituted instead of failing.
    pub fallback: String,
    /// Optional free-form context from the worker.
    #[serde(default)]
    pub note: Option<String>,
}

/// `POST /v1/worker/degradation` (GH #32). Bearer = same short-handle /
/// full-`CapToken` forms as [`worker_submit`]. Body = JSON, not raw bytes —
/// this endpoint carries structured data, unlike its raw-bytes siblings.
///
/// Independent channel: appends a [`DegradationEntry`] to
/// `RunRecord.degradations` via `RunStore::append_degradation` directly.
/// Never touches `OutputStore` / the fold path (Crux invariant 2 — a
/// degradation must not surface as step OUTPUT / `$.step.parts`).
///
/// Behavior:
/// - `task_id` is auto-looked-up server-side from the token/handle, same as
///   [`worker_submit`] / [`worker_artifact`].
/// - GH #37 terminal-run guard applies first — a degradation addressed at
///   an already-terminal Run is rejected with `410 Gone`
///   ([`reject_if_run_terminal`]), same as a submit/artifact would be.
/// - `step_ref` / `attempt` / `at` are server-injected — `step_ref` is the
///   fetching agent's resolved name (`AgentContextView.agent`, the best
///   proxy for `Step.ref` available at this layer), `attempt` is the
///   task's current attempt, `at` is now (Unix epoch seconds). The client
///   body never supplies any of the three.
/// - No Run linkage in `agent_ctx` (a pre-run-tracking dispatch), an
///   unparseable `run_id`, or an `append_degradation` call against a Run
///   the store doesn't actually hold (`RunStoreError::NotFound` — the same
///   condition [`reject_if_run_terminal`] itself fails open on) all take
///   the same silent `204 No Content` path, logged via `tracing::warn!` —
///   this is a legitimate no-tracking codepath, not a client error. Any
///   other `RunStore` failure propagates as `ApiError::engine`.
pub async fn worker_degradation(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<DegradationBody>,
) -> Result<StatusCode, ApiError> {
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
    // GH #37: the same terminal-run guard `worker_submit` / `worker_artifact`
    // apply — a dead Run must not accumulate signals.
    reject_if_run_terminal(&state, &task_id, attempt).await?;

    // Same `with_state` resolution pattern as `reject_if_run_terminal`: an
    // engine-level failure here is fail-open too (`_ => ...`), matching
    // that guard's own "every resolution step is fail-open" contract —
    // this lookup isn't a second, stricter gate on top of it.
    let tid = task_id.clone();
    let (run_id_str, agent) = match state
        .engine
        .with_state("worker_degradation_run_lookup", move |s| {
            s.agent_ctx.get(&(tid, attempt)).and_then(|e| {
                e.view
                    .run_id
                    .clone()
                    .map(|run_id| (run_id, e.view.agent.clone()))
            })
        })
        .await
    {
        Ok(Some(pair)) => pair,
        _ => {
            tracing::warn!(%task_id, "worker_degradation: no run linkage for this task; entry dropped");
            return Ok(StatusCode::NO_CONTENT);
        }
    };
    let Ok(run_id) = RunId::parse(run_id_str) else {
        tracing::warn!(%task_id, "worker_degradation: run_id failed to parse; entry dropped");
        return Ok(StatusCode::NO_CONTENT);
    };

    let entry = DegradationEntry {
        tool: body.tool,
        error: body.error,
        fallback: body.fallback,
        note: body.note,
        step_ref: Some(agent),
        attempt: Some(attempt),
        at: crate::tasks::now_secs(),
    };
    match state.run_store.append_degradation(&run_id, entry).await {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(RunStoreError::NotFound(_)) => {
            tracing::warn!(%task_id, %run_id, "worker_degradation: run not found in run_store; entry dropped");
            Ok(StatusCode::NO_CONTENT)
        }
        Err(e) => Err(ApiError::engine(format!("append_degradation: {e}"))),
    }
}

/// GH #37: terminal-run guard shared by [`worker_submit`] / [`worker_artifact`].
///
/// Resolves the dispatch task's `AgentContextView.run_id` (threaded at
/// spawn time when a `RunContext` accompanied the launch) and rejects the
/// submit with `410 Gone` when the addressed Run has already reached a
/// terminal status (`Done` / `Failed` / `Interrupted`) — the flow-eval
/// driver for that Run is gone, so the staged/final value could never be
/// folded into a flow context. Before this guard, such a submit was
/// silently accepted with `204` and the worker's output orphaned — the
/// exact failure shape observed when a long-running worker outlived the
/// GH #33 sync launch ceiling.
///
/// Every resolution step is fail-open (missing agent-ctx entry / missing
/// `run_id` / unparseable id / unknown Run → `Ok(())`), matching this
/// crate's other best-effort projection hooks: a pre-run-tracking dispatch
/// must keep working exactly as before.
async fn reject_if_run_terminal(
    state: &AppState,
    task_id: &StepId,
    attempt: u32,
) -> Result<(), ApiError> {
    let tid = task_id.clone();
    let run_id_str = match state
        .engine
        .with_state("worker_terminal_run_guard", move |s| {
            s.agent_ctx
                .get(&(tid, attempt))
                .and_then(|e| e.view.run_id.clone())
        })
        .await
    {
        Ok(Some(rid)) => rid,
        _ => return Ok(()),
    };
    let Ok(run_id) = RunId::parse(run_id_str) else {
        return Ok(());
    };
    let Ok(rec) = state.run_store.get(&run_id).await else {
        return Ok(());
    };
    match rec.status {
        RunStatus::Done | RunStatus::Failed | RunStatus::Interrupted => {
            Err(ApiError::gone(format!(
                "run {run_id} is already terminal ({:?}): this attempt's output cannot be \
                 delivered to a flow context; re-kick the task (POST /v1/tasks/:id/runs) and \
                 fetch a fresh prompt",
                rec.status
            )))
        }
        RunStatus::Pending | RunStatus::Running => Ok(()),
    }
}

/// Query params for `GET /v1/worker/prompt/system`. Field names are fixed to
/// `task_id` / `attempt` — this is the exact shape the engine bakes into
/// `system_ref.uri`'s query string for `Http` mode (GH #31), so the names
/// here must match verbatim.
#[derive(Debug, Deserialize)]
pub struct PromptSystemQuery {
    /// Task the fetched raw system prompt belongs to; cross-checked
    /// against the Bearer handle/token, same as [`PromptQuery::task_id`].
    pub task_id: StepId,
    /// Attempt number the baked system prompt was recorded under.
    pub attempt: u32,
}

/// `GET /v1/worker/prompt/system?task_id=<tid>&attempt=<n>` (GH #31). The
/// `Http`-mode fetch target for `system_ref.uri`: serves the exact baked
/// `system` bytes for `(task_id, attempt)` as a raw `text/plain` body — not
/// JSON-wrapped, since `mse_worker_fetch` needs the precise byte sequence to
/// sha256-verify against `system_ref.sha256`.
///
/// Same Bearer auth flow as [`worker_prompt`] (short handle or full
/// `CapToken`); 404 via [`ApiError::not_found`] if no baked system exists for
/// that `(task_id, attempt)`.
pub async fn worker_prompt_system(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<PromptSystemQuery>,
) -> Result<impl axum::response::IntoResponse, ApiError> {
    let task_id = q.task_id;
    let attempt = q.attempt;
    let bearer = extract_bearer_raw(&headers)?;
    if let Some(handle) = parse_worker_handle(&bearer) {
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
    } else {
        let token = CapToken::decode(bearer.trim())
            .map_err(|e| ApiError::bad_request(format!("invalid token: {e}")))?;
        state
            .engine
            .verify_token_for_task(&token, mlua_swarm::Verb::FetchPrompt, &task_id)
            .await
            .map_err(|e| ApiError::engine(format!("verify_token_for_task: {e}")))?;
    }
    let system = state
        .engine
        .raw_system_prompt(&task_id, attempt)
        .await
        .map_err(|e| ApiError::engine(format!("raw_system_prompt: {e}")))?
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "no baked system prompt for task {task_id} attempt {attempt}"
            ))
        })?;
    Ok((
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        system,
    ))
}

/// Response body for `GET /v1/agents/:name/render-size`.
#[derive(Debug, serde::Serialize)]
pub struct AgentRenderSizeResponse {
    /// The agent name looked up (echoed back verbatim from the path param).
    pub agent: String,
    /// Most-recently-baked `system_prompt` render size in bytes for this
    /// agent, or `None` if `bake_worker_system_prompt` has never recorded
    /// one (a freshly-added agent that has never been dispatched).
    pub last_rendered_bytes: Option<usize>,
}

/// `GET /v1/agents/:name/render-size` (GH #31). Live per-agent-name lookup
/// of the most-recently-baked `system_prompt` render size, backing
/// `bp_doctor`'s post-render size check. No Bearer required — same
/// unauthenticated trust tier as `GET /v1/blueprints/:id/head`
/// (`blueprints::get_head`), an operator-diagnostic route.
///
/// `last_rendered_bytes: null` is a normal, expected response (a
/// freshly-added agent that has never been dispatched yet) — always
/// `200 OK`, never a 404.
pub async fn agent_render_size(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Json<AgentRenderSizeResponse> {
    let last_rendered_bytes = state.engine.agent_last_rendered_size(&name).await;
    Json(AgentRenderSizeResponse {
        agent: name,
        last_rendered_bytes,
    })
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
    use axum::response::IntoResponse;
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
            sync_timeout_secs: 300,
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
            degradations: Vec::new(),
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
            system_ref: None,
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
                    s.agent_ctx.insert(
                        (consumer_id, 1),
                        mlua_swarm::core::state::AgentCtxEntry {
                            policy: mlua_swarm_schema::ContextPolicy {
                                steps: Some(vec!["planner".to_string()]),
                                ..Default::default()
                            },
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
                    s.agent_ctx.insert(
                        (consumer_id, 1),
                        mlua_swarm::core::state::AgentCtxEntry {
                            policy: mlua_swarm_schema::ContextPolicy {
                                steps: Some(vec![]),
                                ..Default::default()
                            },
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
                    s.agent_ctx.insert(
                        (consumer_id, 1),
                        mlua_swarm::core::state::AgentCtxEntry {
                            policy: mlua_swarm_schema::ContextPolicy {
                                steps: Some(vec!["planner".to_string(), "coder".to_string()]),
                                steps_exclude: vec!["planner".to_string()],
                                ..Default::default()
                            },
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

    /// A single-step Blueprint whose `planner` agent declares
    /// `AgentMeta.projection_name = "plan-out"` — the `StepNaming` fixture
    /// for [`declared_projection_name_pointer_name_is_canonical_and_policy_matches_it`],
    /// mirroring `crate::projection::tests`' own
    /// `declared_projection_name_blueprint` helper (duplicated here rather
    /// than shared — this crate's established per-module test-helper
    /// convention).
    fn declared_name_bp() -> mlua_swarm::blueprint::Blueprint {
        use mlua_flow_ir::{Expr, Node};
        use mlua_swarm::blueprint::{
            current_schema_version, AgentDef, AgentKind, AgentMeta, Blueprint, BlueprintMetadata,
            CompilerHints, CompilerStrategy,
        };
        Blueprint {
            schema_version: current_schema_version(),
            id: "worker-test-declared-name-bp".into(),
            flow: Node::Step {
                ref_: "planner".to_string(),
                in_: Expr::Path {
                    at: "$.in".parse().expect("literal test path: $.in"),
                },
                out: Expr::Path {
                    at: "$.plan".parse().expect("literal test path: $.plan"),
                },
            },
            agents: vec![AgentDef {
                name: "planner".to_string(),
                kind: AgentKind::RustFn,
                spec: json!({"fn_id": "planner"}),
                profile: None,
                meta: Some(AgentMeta {
                    projection_name: Some("plan-out".to_string()),
                    ..Default::default()
                }),
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
        }
    }

    /// Test 8 (GH #23 subtask-3, declared-name E2E — Worker axis half): a
    /// declared `projection_name` makes `StepPointer.name` the CANONICAL
    /// name (not the raw `Step.ref` the Data-plane / `step_entries` still
    /// index by), and `ContextPolicy.steps` naming the canonical name
    /// matches it.
    #[tokio::test]
    async fn declared_projection_name_pointer_name_is_canonical_and_policy_matches_it() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let task_id = TaskId::new();
        let run_id = RunId::new();
        let planner_id = StepId::new();

        // The Data-plane store is keyed by the CANONICAL name — GH #23
        // subtask-2's sink already writes it that way.
        append_final(
            &data_store,
            planner_id.as_str(),
            "plan-out",
            json!({"plan": "x"}),
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

        // Seed the `StepNaming` table the way `Compiler::compile` +
        // `EngineDispatcher::dispatch` would have — the same `Arc` stashed
        // under every dispatched step's own id, including the FETCHING
        // agent's (`consumer_id`), which `assemble_step_pointers` looks up
        // via `Engine::step_naming_for(&payload.task_id)`.
        let (naming, _warnings) =
            mlua_swarm::core::step_naming::StepNaming::from_blueprint(&declared_name_bp())
                .expect("no collision");
        let naming = Arc::new(naming);
        let consumer_id = StepId::new();
        state
            .engine
            .with_state("test.seed_step_naming", {
                let naming = naming.clone();
                let planner_id = planner_id.clone();
                let consumer_id = consumer_id.clone();
                move |s| {
                    s.step_namings.insert(planner_id, naming.clone());
                    s.step_namings.insert(consumer_id, naming);
                }
            })
            .await
            .expect("seed step naming");
        state
            .engine
            .with_state("test.seed_policy", {
                let consumer_id = consumer_id.clone();
                move |s| {
                    s.agent_ctx.insert(
                        (consumer_id, 1),
                        mlua_swarm::core::state::AgentCtxEntry {
                            policy: mlua_swarm_schema::ContextPolicy {
                                steps: Some(vec!["plan-out".to_string()]),
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                    );
                }
            })
            .await
            .expect("seed policy");

        let mut payload = consumer_payload(&consumer_id, &run_id);
        assemble_step_pointers(&state, &mut payload).await;

        let steps = &payload.context.expect("context").steps;
        assert_eq!(steps.len(), 1, "steps: {steps:?}");
        assert_eq!(
            steps[0].name, "plan-out",
            "StepPointer.name must be the canonical name"
        );
    }

    // ──────────────────────────────────────────────────────────────────────
    // GH #31 — `/v1/worker/prompt/system` + `/v1/agents/:name/render-size`
    // ──────────────────────────────────────────────────────────────────────

    /// Seeds a task + baked system prompt + a short worker handle bound to
    /// it, mirroring the shape `Engine::dispatch_attempt` would have
    /// produced (minus the parts these two routes don't touch: no real
    /// HMAC-signed `CapToken`, since `task_id_from_handle`'s handle → fp →
    /// task_id chain is what's under test, not signature verification).
    async fn seed_task_with_handle(
        state: &AppState,
        task_id: &StepId,
        agent: &str,
        attempt: u32,
        system: Option<String>,
    ) -> String {
        let handle = format!("wh-{}", mlua_swarm::types::secure_hex(4));
        let task_id = task_id.clone();
        let agent = agent.to_string();
        let handle_clone = handle.clone();
        state
            .engine
            .with_state("test.seed_task_with_handle", move |s| {
                let mut task = mlua_swarm::core::state::TaskState::new(
                    task_id.clone(),
                    mlua_swarm::core::state::TaskSpec {
                        agent: agent.clone(),
                        initial_directive: json!("x"),
                        step_ctx: None,
                    },
                );
                task.attempt = attempt;
                s.tasks.insert(task_id.clone(), task);
                s.systems.insert((task_id.clone(), attempt), system);
                let token = CapToken {
                    agent_id: agent,
                    role: mlua_swarm::Role::Worker,
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
                    mlua_swarm::core::state::CapTokenRecord {
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

    fn bearer_headers(handle: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {handle}").parse().expect("header value"),
        );
        headers
    }

    /// `GET /v1/worker/prompt/system` returns the exact raw baked bytes
    /// (not JSON-wrapped) with `Content-Type: text/plain`, for the
    /// `(task_id, attempt)` the handle is bound to.
    #[tokio::test]
    async fn worker_prompt_system_returns_raw_bytes_for_baked_system() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);
        let task_id = StepId::new();
        let rendered = "# Hello\n\nThis is the baked system prompt.".to_string();
        let handle =
            seed_task_with_handle(&state, &task_id, "planner", 1, Some(rendered.clone())).await;

        let resp = worker_prompt_system(
            State(state.clone()),
            bearer_headers(&handle),
            Query(PromptSystemQuery {
                task_id: task_id.clone(),
                attempt: 1,
            }),
        )
        .await
        .expect("worker_prompt_system")
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        let content_type = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .expect("content-type header")
            .to_str()
            .expect("ascii");
        assert_eq!(content_type, "text/plain; charset=utf-8");
        let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        assert_eq!(body_bytes.as_ref(), rendered.as_bytes());
    }

    /// No baked system for the given `(task_id, attempt)` → 404, not a
    /// panic or a 200-with-empty-body.
    #[tokio::test]
    async fn worker_prompt_system_404s_when_no_baked_system() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "planner", 1, None).await;

        let result = worker_prompt_system(
            State(state.clone()),
            bearer_headers(&handle),
            Query(PromptSystemQuery {
                task_id: task_id.clone(),
                attempt: 1,
            }),
        )
        .await;
        let err = match result {
            Ok(_) => panic!("expected 404 ApiError, got Ok"),
            Err(e) => e,
        };
        assert_eq!(err.into_response().status(), StatusCode::NOT_FOUND);
    }

    /// A handle bound to a different task than the one requested must be
    /// rejected (400) — this is the same cross-check `worker_prompt` does.
    #[tokio::test]
    async fn worker_prompt_system_rejects_handle_task_mismatch() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);
        let task_id = StepId::new();
        let other_task_id = StepId::new();
        let handle =
            seed_task_with_handle(&state, &task_id, "planner", 1, Some("x".to_string())).await;

        let result = worker_prompt_system(
            State(state.clone()),
            bearer_headers(&handle),
            Query(PromptSystemQuery {
                task_id: other_task_id,
                attempt: 1,
            }),
        )
        .await;
        let err = match result {
            Ok(_) => panic!("expected 400 ApiError for task mismatch, got Ok"),
            Err(e) => e,
        };
        assert_eq!(err.into_response().status(), StatusCode::BAD_REQUEST);
    }

    /// `GET /v1/agents/:name/render-size` requires no auth, and reports
    /// `last_rendered_bytes: null` for an agent that has never had a
    /// `system_prompt` baked — a normal 200, not a 404.
    #[tokio::test]
    async fn agent_render_size_returns_null_for_unknown_agent() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);

        let Json(body) = agent_render_size(
            State(state.clone()),
            axum::extract::Path("never-dispatched".to_string()),
        )
        .await;
        assert_eq!(body.agent, "never-dispatched");
        assert_eq!(body.last_rendered_bytes, None);
    }

    /// Once `bake_worker_system_prompt` has recorded a render size for an
    /// agent, the route reports the most-recently-observed value.
    #[tokio::test]
    async fn agent_render_size_reports_last_rendered_bytes() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);
        let task_id = StepId::new();
        state
            .engine
            .with_state("test.seed_agent_ctx_for_bake", {
                let task_id = task_id.clone();
                move |s| {
                    s.tasks.insert(
                        task_id.clone(),
                        mlua_swarm::core::state::TaskState::new(
                            task_id,
                            mlua_swarm::core::state::TaskSpec {
                                agent: "coder".to_string(),
                                initial_directive: json!("x"),
                                step_ctx: None,
                            },
                        ),
                    );
                }
            })
            .await
            .expect("seed task");
        state
            .engine
            .bake_worker_system_prompt(&task_id, 1, Some("z".repeat(42)))
            .await
            .expect("bake_worker_system_prompt");

        let Json(body) = agent_render_size(
            State(state.clone()),
            axum::extract::Path("coder".to_string()),
        )
        .await;
        assert_eq!(body.agent, "coder");
        assert_eq!(body.last_rendered_bytes, Some(42));
    }

    // ──────────────────────────────────────────────────────────────────────
    // GH #36 ST1 — `POST /v1/worker/artifact`
    // ──────────────────────────────────────────────────────────────────────

    /// A valid `?name=` + short-handle Bearer stages the raw body (trailing
    /// whitespace trimmed, same as `worker_submit`) as an `Artifact` on the
    /// task's current-attempt tail, and returns `204 No Content`.
    #[tokio::test]
    async fn worker_artifact_stages_and_204s_for_valid_request() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "planner", 1, None).await;

        let status = worker_artifact(
            State(state.clone()),
            bearer_headers(&handle),
            Query(ArtifactQuery {
                name: "summary".to_string(),
            }),
            axum::body::Bytes::from_static(b"hello artifact\n"),
        )
        .await
        .expect("worker_artifact");
        assert_eq!(status, StatusCode::NO_CONTENT);

        let tail = state.engine.output_tail(&task_id, 1).await;
        assert_eq!(tail.len(), 1, "tail: {tail:?}");
        match &tail[0] {
            OutputEvent::Artifact { name, content } => {
                assert_eq!(name, "summary");
                match content {
                    ContentRef::Inline { value } => {
                        assert_eq!(value, &json!("hello artifact"));
                    }
                    other => panic!("expected Inline content, got {other:?}"),
                }
            }
            other => panic!("expected Artifact event, got {other:?}"),
        }
    }

    /// `?name=` missing entirely → axum's `Query` extractor rejection
    /// (400), not a panic. `Query<ArtifactQuery>` is constructed directly
    /// in this test (mirroring the other handlers' unit style, which call
    /// the handler fn with an already-extracted `Query`) — an empty `name`
    /// is exercised separately below since that case is NOT caught by the
    /// extractor and must be checked in the handler body.
    #[tokio::test]
    async fn worker_artifact_rejects_blank_name() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "planner", 1, None).await;

        let result = worker_artifact(
            State(state.clone()),
            bearer_headers(&handle),
            Query(ArtifactQuery {
                name: "   ".to_string(),
            }),
            axum::body::Bytes::from_static(b"x"),
        )
        .await;
        let err = match result {
            Ok(_) => panic!("expected 400 ApiError for blank name, got Ok"),
            Err(e) => e,
        };
        assert_eq!(err.into_response().status(), StatusCode::BAD_REQUEST);

        // Nothing was staged.
        assert!(state.engine.output_tail(&task_id, 1).await.is_empty());
    }

    /// Staging the same `name` twice within one attempt is last-write-wins
    /// on the folded value (`fold_final_and_parts` in `mlua_swarm::core::
    /// engine`) — this test only asserts the raw tail carries both events
    /// in order (the fold itself is covered by that crate's own unit
    /// tests); `Engine::stage_worker_artifact_trusted`'s doc.
    #[tokio::test]
    async fn worker_artifact_staging_same_name_twice_appends_both_events_in_order() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "planner", 1, None).await;

        for body in [b"first".as_slice(), b"second".as_slice()] {
            worker_artifact(
                State(state.clone()),
                bearer_headers(&handle),
                Query(ArtifactQuery {
                    name: "a".to_string(),
                }),
                axum::body::Bytes::copy_from_slice(body),
            )
            .await
            .expect("worker_artifact");
        }

        let tail = state.engine.output_tail(&task_id, 1).await;
        assert_eq!(tail.len(), 2, "tail: {tail:?}");
        let values: Vec<&str> = tail
            .iter()
            .map(|ev| match ev {
                OutputEvent::Artifact {
                    content: ContentRef::Inline { value },
                    ..
                } => value.as_str().expect("string value"),
                other => panic!("expected Artifact/Inline event, got {other:?}"),
            })
            .collect();
        assert_eq!(values, vec!["first", "second"]);
    }

    // ──────────────────────────────────────────────────────────────────
    // GH #37 — terminal-run guard (`reject_if_run_terminal`)
    // ──────────────────────────────────────────────────────────────────

    /// Links a seeded dispatch task to a Run the same way
    /// `AgentContextMiddleware` does at spawn time: an `agent_ctx` entry
    /// whose view carries the `run_id`.
    async fn link_task_to_run(state: &AppState, task_id: &StepId, attempt: u32, run_id: &RunId) {
        let tid = task_id.clone();
        let rid_str = run_id.to_string();
        state
            .engine
            .with_state("test.link_task_to_run", move |s| {
                let mut entry = mlua_swarm::core::state::AgentCtxEntry::default();
                entry.view.run_id = Some(rid_str);
                s.agent_ctx.insert((tid, attempt), entry);
            })
            .await
            .expect("link_task_to_run");
    }

    /// GH #37: a submit / artifact addressed at a Run that already
    /// reached a terminal status must be rejected with `410 Gone` — the
    /// flow-eval driver for that Run is gone, so a silent `204` here
    /// would orphan the worker's output.
    #[tokio::test]
    async fn submit_and_artifact_against_terminal_run_return_410() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store.clone());
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "planner", 1, None).await;

        let owner_task = TaskId::new();
        let run_id = RunId::new();
        let mut rec = run_record(&owner_task, &run_id, vec![]);
        rec.status = RunStatus::Failed;
        run_store.create(rec).await.expect("run create");
        link_task_to_run(&state, &task_id, 1, &run_id).await;

        let err = worker_submit(
            State(state.clone()),
            bearer_headers(&handle),
            Query(SubmitQuery { ok: None }),
            axum::body::Bytes::from_static(b"LATE OUTPUT"),
        )
        .await
        .expect_err("a submit against a Failed run must be rejected");
        assert_eq!(err.status, StatusCode::GONE);
        assert!(
            err.message.contains(&run_id.to_string()),
            "the 410 must name the terminal run: {}",
            err.message
        );

        let err = worker_artifact(
            State(state.clone()),
            bearer_headers(&handle),
            Query(ArtifactQuery {
                name: "part.md".to_string(),
            }),
            axum::body::Bytes::from_static(b"LATE PART"),
        )
        .await
        .expect_err("an artifact staged against a Failed run must be rejected");
        assert_eq!(err.status, StatusCode::GONE);

        // The rejected values must not have reached the output tail.
        let tail = state.engine.output_tail(&task_id, 1).await;
        assert!(tail.is_empty(), "rejected submits must not land: {tail:?}");
    }

    /// GH #37 fail-open contract: the guard must never turn a
    /// would-have-succeeded submit into a failure — no run linkage at
    /// all, an unknown Run, and a live (`Running`) Run all pass.
    #[tokio::test]
    async fn terminal_run_guard_is_fail_open() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store.clone());
        let task_id = StepId::new();
        seed_task_with_handle(&state, &task_id, "planner", 1, None).await;

        // (a) No agent-ctx linkage at all (pre-run-tracking dispatch).
        reject_if_run_terminal(&state, &task_id, 1)
            .await
            .expect("no linkage must fail open");

        // (b) Linked to a Run the store does not know.
        let unknown_run = RunId::new();
        link_task_to_run(&state, &task_id, 1, &unknown_run).await;
        reject_if_run_terminal(&state, &task_id, 1)
            .await
            .expect("unknown run must fail open");

        // (c) Linked to a live Run.
        let owner_task = TaskId::new();
        let live_run = RunId::new();
        run_store
            .create(run_record(&owner_task, &live_run, vec![]))
            .await
            .expect("run create");
        link_task_to_run(&state, &task_id, 1, &live_run).await;
        reject_if_run_terminal(&state, &task_id, 1)
            .await
            .expect("a Running run must pass the guard");
    }

    // ──────────────────────────────────────────────────────────────────
    // GH #32 — `POST /v1/worker/degradation`
    // ──────────────────────────────────────────────────────────────────

    fn degradation_body(tool: &str, note: Option<&str>) -> DegradationBody {
        DegradationBody {
            tool: tool.to_string(),
            error: "boom".to_string(),
            fallback: "used cached value".to_string(),
            note: note.map(str::to_string),
        }
    }

    /// [`link_task_to_run`] plus the `view.agent` name — production's
    /// `AgentContextMiddleware` sets both fields on the same `agent_ctx`
    /// entry; the shared GH #37 helper only needed `run_id`, so this
    /// sibling fills in `agent` too for tests that assert on the
    /// server-injected `step_ref`.
    async fn link_task_to_run_with_agent(
        state: &AppState,
        task_id: &StepId,
        attempt: u32,
        run_id: &RunId,
        agent: &str,
    ) {
        let tid = task_id.clone();
        let rid_str = run_id.to_string();
        let agent = agent.to_string();
        state
            .engine
            .with_state("test.link_task_to_run_with_agent", move |s| {
                let mut entry = mlua_swarm::core::state::AgentCtxEntry::default();
                entry.view.run_id = Some(rid_str);
                entry.view.agent = agent;
                s.agent_ctx.insert((tid, attempt), entry);
            })
            .await
            .expect("link_task_to_run_with_agent");
    }

    /// A worker-reported degradation is persisted to the linked Run's
    /// `degradations` with the server-injected `step_ref` / `attempt` /
    /// `at` fields filled in — the client body never supplies any of the
    /// three.
    #[tokio::test]
    async fn worker_degradation_persists_entry_when_run_tracked() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store.clone());
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "planner", 1, None).await;

        let owner_task = TaskId::new();
        let run_id = RunId::new();
        run_store
            .create(run_record(&owner_task, &run_id, vec![]))
            .await
            .expect("run create");
        link_task_to_run_with_agent(&state, &task_id, 1, &run_id, "planner").await;

        let status = worker_degradation(
            State(state.clone()),
            bearer_headers(&handle),
            Json(degradation_body("web_search", Some("rate limited"))),
        )
        .await
        .expect("worker_degradation");
        assert_eq!(status, StatusCode::NO_CONTENT);

        let rec = run_store.get(&run_id).await.expect("run get");
        assert_eq!(
            rec.degradations.len(),
            1,
            "degradations: {:?}",
            rec.degradations
        );
        let entry = &rec.degradations[0];
        assert_eq!(entry.tool, "web_search");
        assert_eq!(entry.error, "boom");
        assert_eq!(entry.fallback, "used cached value");
        assert_eq!(entry.note.as_deref(), Some("rate limited"));
        assert_eq!(entry.step_ref.as_deref(), Some("planner"));
        assert_eq!(entry.attempt, Some(1));
        assert!(entry.at > 0, "at must be a real timestamp: {}", entry.at);
    }

    /// Two entries POSTed in sequence are appended in order.
    #[tokio::test]
    async fn worker_degradation_appends_in_order() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store.clone());
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "planner", 1, None).await;

        let owner_task = TaskId::new();
        let run_id = RunId::new();
        run_store
            .create(run_record(&owner_task, &run_id, vec![]))
            .await
            .expect("run create");
        link_task_to_run(&state, &task_id, 1, &run_id).await;

        for tool in ["first_tool", "second_tool"] {
            worker_degradation(
                State(state.clone()),
                bearer_headers(&handle),
                Json(degradation_body(tool, None)),
            )
            .await
            .expect("worker_degradation");
        }

        let rec = run_store.get(&run_id).await.expect("run get");
        let tools: Vec<&str> = rec.degradations.iter().map(|e| e.tool.as_str()).collect();
        assert_eq!(tools, vec!["first_tool", "second_tool"]);
    }

    /// A task whose `agent_ctx` carries no Run linkage (pre-run-tracking
    /// dispatch) silently 204s — nothing to append to, and this must not
    /// surface as a client error.
    #[tokio::test]
    async fn worker_degradation_silent_ok_when_no_run_tracked() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "planner", 1, None).await;

        let status = worker_degradation(
            State(state.clone()),
            bearer_headers(&handle),
            Json(degradation_body("some_tool", None)),
        )
        .await
        .expect("worker_degradation must not error on missing run linkage");
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    /// GH #37 terminal-run guard applies to the degradation channel too — a
    /// dead Run must not accumulate signals.
    #[tokio::test]
    async fn worker_degradation_rejects_terminal_run() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store.clone());
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "planner", 1, None).await;

        let owner_task = TaskId::new();
        let run_id = RunId::new();
        let mut rec = run_record(&owner_task, &run_id, vec![]);
        rec.status = RunStatus::Done;
        run_store.create(rec).await.expect("run create");
        link_task_to_run(&state, &task_id, 1, &run_id).await;

        let err = worker_degradation(
            State(state.clone()),
            bearer_headers(&handle),
            Json(degradation_body("some_tool", None)),
        )
        .await
        .expect_err("a degradation against a Done run must be rejected");
        assert_eq!(err.status, StatusCode::GONE);

        let rec = run_store.get(&run_id).await.expect("run get");
        assert!(
            rec.degradations.is_empty(),
            "rejected degradation must not land: {:?}",
            rec.degradations
        );
    }

    // ──────────────────────────────────────────────────────────────────
    // GH #42 — `@file:<abs-path>` sentinel resolution in `worker_submit`
    // / `worker_artifact`. Guards each verified independently: sentinel
    // resolves to the file's trimmed contents; path outside `work_dir`,
    // missing file, oversized file, and non-sentinel bodies each get the
    // documented behavior.
    // ──────────────────────────────────────────────────────────────────

    /// Seeds an `agent_ctx` entry whose view carries `work_dir` and, when
    /// `allow_file_submit` is `Some`, that value under the GH #43
    /// [`FILE_SENTINEL_ALLOW_KEY`] in `view.extra` — matching the shape
    /// `AgentContextMiddleware` writes at spawn time. Sentinel resolution
    /// requires both the `work_dir` and the strict `Bool(true)` opt-in.
    async fn seed_work_dir(
        state: &AppState,
        task_id: &StepId,
        attempt: u32,
        work_dir: &str,
        allow_file_submit: Option<Value>,
    ) {
        let tid = task_id.clone();
        let work_dir = work_dir.to_string();
        state
            .engine
            .with_state("test.seed_work_dir", move |s| {
                let mut entry = mlua_swarm::core::state::AgentCtxEntry::default();
                entry.view.work_dir = Some(work_dir);
                if let Some(v) = allow_file_submit {
                    entry
                        .view
                        .extra
                        .insert(FILE_SENTINEL_ALLOW_KEY.to_string(), v);
                }
                s.agent_ctx.insert((tid, attempt), entry);
            })
            .await
            .expect("seed_work_dir");
    }

    /// Sentinel body `@file:<abs-path>` resolves to the file's trimmed
    /// contents and reaches the `OutputStore` via the normal Final-append
    /// path — same 204 the inline path returns.
    #[tokio::test]
    async fn worker_submit_resolves_file_sentinel_under_work_dir() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store.clone(), run_store);
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "planner", 1, None).await;

        let tmp = tempfile::tempdir().expect("tempdir");
        let work_dir = tmp.path().to_path_buf();
        seed_work_dir(
            &state,
            &task_id,
            1,
            work_dir.to_str().expect("work_dir utf-8"),
            Some(Value::Bool(true)),
        )
        .await;

        let payload_path = work_dir.join("scout.md");
        let payload = "## Context Package (broad)\n\nlarge body content\n";
        tokio::fs::write(&payload_path, payload)
            .await
            .expect("write payload");
        let body = format!(
            "@file:{}",
            payload_path.to_str().expect("payload path utf-8")
        );

        let status = worker_submit(
            State(state.clone()),
            bearer_headers(&handle),
            Query(SubmitQuery { ok: None }),
            axum::body::Bytes::from(body),
        )
        .await
        .expect("worker_submit sentinel");
        assert_eq!(status, StatusCode::NO_CONTENT);

        // Final event lands with the file's trimmed contents on
        // `EngineState.output_store` (the in-memory tail
        // `submit_worker_result_trusted` writes to).
        let tid = task_id.clone();
        let value = state
            .engine
            .with_state("test.inspect_output_store", move |s| {
                s.output_store.get(&(tid.clone(), 1)).and_then(|evs| {
                    evs.iter().find_map(|ev| match ev {
                        OutputEvent::Final {
                            content: ContentRef::Inline { value },
                            ..
                        } => Some(value.clone()),
                        _ => None,
                    })
                })
            })
            .await
            .expect("with_state")
            .expect("Final event present");
        assert_eq!(value, Value::String(payload.trim_end().to_string()));
    }

    /// A non-sentinel body is passed through byte-for-byte (pre-#42
    /// callers see zero behavior change).
    #[tokio::test]
    async fn worker_submit_passes_non_sentinel_body_unchanged() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store.clone(), run_store);
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "planner", 1, None).await;
        // No agent_ctx / work_dir seeded — the inline path must not
        // require one.

        let status = worker_submit(
            State(state.clone()),
            bearer_headers(&handle),
            Query(SubmitQuery { ok: None }),
            axum::body::Bytes::from_static(b"DONE yes=1 maybe=0 no=0"),
        )
        .await
        .expect("worker_submit inline");
        assert_eq!(status, StatusCode::NO_CONTENT);

        let tid = task_id.clone();
        let value = state
            .engine
            .with_state("test.inspect_output_store", move |s| {
                s.output_store.get(&(tid.clone(), 1)).and_then(|evs| {
                    evs.iter().find_map(|ev| match ev {
                        OutputEvent::Final {
                            content: ContentRef::Inline { value },
                            ..
                        } => Some(value.clone()),
                        _ => None,
                    })
                })
            })
            .await
            .expect("with_state")
            .expect("Final event present");
        assert_eq!(value, Value::String("DONE yes=1 maybe=0 no=0".to_string()));
    }

    /// Sentinel with a path outside the task's `work_dir` (`..`-escape
    /// via a sibling tempdir) → `400`. `canonicalize` collapses the
    /// `..`, so a symlink pointing outside the allowlist would be caught
    /// by the same check.
    #[tokio::test]
    async fn worker_submit_rejects_sentinel_path_outside_work_dir() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "planner", 1, None).await;

        let allowed = tempfile::tempdir().expect("allowed tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        seed_work_dir(
            &state,
            &task_id,
            1,
            allowed.path().to_str().expect("utf-8"),
            Some(Value::Bool(true)),
        )
        .await;

        let outside_file = outside.path().join("leak.md");
        tokio::fs::write(&outside_file, b"outside content")
            .await
            .expect("write outside");
        let body = format!(
            "@file:{}",
            outside_file.to_str().expect("outside path utf-8")
        );

        let err = worker_submit(
            State(state.clone()),
            bearer_headers(&handle),
            Query(SubmitQuery { ok: None }),
            axum::body::Bytes::from(body),
        )
        .await
        .expect_err("outside-work_dir sentinel must be rejected");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    /// Sentinel pointing at a non-existent file → `404`.
    #[tokio::test]
    async fn worker_submit_rejects_sentinel_missing_file() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "planner", 1, None).await;

        let tmp = tempfile::tempdir().expect("tempdir");
        seed_work_dir(
            &state,
            &task_id,
            1,
            tmp.path().to_str().expect("utf-8"),
            Some(Value::Bool(true)),
        )
        .await;
        let missing = tmp.path().join("does-not-exist.md");
        let body = format!("@file:{}", missing.to_str().expect("utf-8"));

        let err = worker_submit(
            State(state.clone()),
            bearer_headers(&handle),
            Query(SubmitQuery { ok: None }),
            axum::body::Bytes::from(body),
        )
        .await
        .expect_err("missing-file sentinel must be rejected");
        assert_eq!(err.status, StatusCode::NOT_FOUND);
    }

    /// Sentinel body with a relative path → `400` before any FS lookup.
    #[tokio::test]
    async fn worker_submit_rejects_sentinel_relative_path() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "planner", 1, None).await;

        let err = worker_submit(
            State(state.clone()),
            bearer_headers(&handle),
            Query(SubmitQuery { ok: None }),
            axum::body::Bytes::from_static(b"@file:relative/path.md"),
        )
        .await
        .expect_err("relative-path sentinel must be rejected");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    /// Sentinel body when the task has no `AgentContextView` (spawn
    /// didn't run through `AgentContextMiddleware`) → `400`. This is the
    /// documented pre-condition for sentinel use.
    #[tokio::test]
    async fn worker_submit_rejects_sentinel_without_agent_context_view() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "planner", 1, None).await;
        // No seed_work_dir — the agent_ctx map has no entry for this task.

        let err = worker_submit(
            State(state.clone()),
            bearer_headers(&handle),
            Query(SubmitQuery { ok: None }),
            axum::body::Bytes::from_static(b"@file:/tmp/anywhere.md"),
        )
        .await
        .expect_err("missing AgentContextView must reject sentinel");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    /// The same sentinel form works on `POST /v1/worker/artifact` — the
    /// artifact endpoint shares the resolver with `worker_submit`, so the
    /// resolved file contents land under the artifact's `name` key.
    #[tokio::test]
    async fn worker_artifact_resolves_file_sentinel_under_work_dir() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "planner", 1, None).await;

        let tmp = tempfile::tempdir().expect("tempdir");
        seed_work_dir(
            &state,
            &task_id,
            1,
            tmp.path().to_str().expect("utf-8"),
            Some(Value::Bool(true)),
        )
        .await;

        let payload_path = tmp.path().join("part.md");
        let payload = "artifact part body\n";
        tokio::fs::write(&payload_path, payload)
            .await
            .expect("write payload");
        let body = format!("@file:{}", payload_path.to_str().expect("utf-8"));

        let status = worker_artifact(
            State(state.clone()),
            bearer_headers(&handle),
            Query(ArtifactQuery {
                name: "scout".to_string(),
            }),
            axum::body::Bytes::from(body),
        )
        .await
        .expect("worker_artifact sentinel");
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    /// GH #43 — sentinel with `work_dir` seeded but no
    /// `allow_file_submit` opt-in → `400` (default-deny). The file exists
    /// and sits under `work_dir`, so the rejection is attributable to the
    /// missing opt-in alone.
    #[tokio::test]
    async fn worker_submit_rejects_sentinel_without_allow_flag() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "planner", 1, None).await;

        let tmp = tempfile::tempdir().expect("tempdir");
        seed_work_dir(
            &state,
            &task_id,
            1,
            tmp.path().to_str().expect("utf-8"),
            None,
        )
        .await;

        let payload_path = tmp.path().join("out.md");
        tokio::fs::write(&payload_path, b"resolvable body")
            .await
            .expect("write payload");
        let body = format!("@file:{}", payload_path.to_str().expect("utf-8"));

        let err = worker_submit(
            State(state.clone()),
            bearer_headers(&handle),
            Query(SubmitQuery { ok: None }),
            axum::body::Bytes::from(body),
        )
        .await
        .expect_err("missing opt-in must reject sentinel");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(
            err.message.contains("not allowed"),
            "rejection must name the opt-in guard, got: {}",
            err.message
        );
    }

    /// GH #43 — the opt-in is the strict boolean `true`: `Bool(false)`
    /// and the string `"true"` are both rejected with `400`.
    #[tokio::test]
    async fn worker_submit_rejects_sentinel_with_non_true_allow_values() {
        for allow in [Value::Bool(false), Value::String("true".to_string())] {
            let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
            let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
            let state = test_state(data_store, run_store);
            let task_id = StepId::new();
            let handle = seed_task_with_handle(&state, &task_id, "planner", 1, None).await;

            let tmp = tempfile::tempdir().expect("tempdir");
            seed_work_dir(
                &state,
                &task_id,
                1,
                tmp.path().to_str().expect("utf-8"),
                Some(allow.clone()),
            )
            .await;

            let payload_path = tmp.path().join("out.md");
            tokio::fs::write(&payload_path, b"resolvable body")
                .await
                .expect("write payload");
            let body = format!("@file:{}", payload_path.to_str().expect("utf-8"));

            let err = worker_submit(
                State(state.clone()),
                bearer_headers(&handle),
                Query(SubmitQuery { ok: None }),
                axum::body::Bytes::from(body),
            )
            .await
            .expect_err("non-true opt-in value must reject sentinel");
            assert_eq!(err.status, StatusCode::BAD_REQUEST, "value: {allow:?}");
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // GH #50 (Subtask 2) — submit-time verdict contract gate, handler-
    // level unit coverage. The full process-boundary HTTP round trip
    // (Acceptance Criterion #7) lives in
    // `crates/mlua-swarm-server/tests/verdict_contract.rs`; these are the
    // fast in-process counterpart exercising `worker_submit` /
    // `worker_artifact` directly, same convention as the sentinel tests
    // above.
    // ──────────────────────────────────────────────────────────────────

    fn body_verdict_contract(values: &[&str]) -> mlua_swarm_schema::VerdictContract {
        mlua_swarm_schema::VerdictContract {
            channel: VerdictChannel::Body,
            values: values.iter().map(|v| v.to_string()).collect(),
        }
    }

    fn part_verdict_contract(values: &[&str]) -> mlua_swarm_schema::VerdictContract {
        mlua_swarm_schema::VerdictContract {
            channel: VerdictChannel::Part,
            values: values.iter().map(|v| v.to_string()).collect(),
        }
    }

    /// A `channel: "body"` contract rejects a `worker_submit` body outside
    /// its declared `values` with `422`, echoing the expected token set.
    #[tokio::test]
    async fn worker_submit_rejects_body_outside_contract_values_with_422() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "gate", 1, None).await;
        state.engine.register_verdict_contracts(HashMap::from([(
            "gate".to_string(),
            body_verdict_contract(&["PASS", "BLOCKED"]),
        )]));

        let err = worker_submit(
            State(state.clone()),
            bearer_headers(&handle),
            Query(SubmitQuery { ok: None }),
            axum::body::Bytes::from("UNKNOWN"),
        )
        .await
        .expect_err("value outside declared values must reject");
        assert_eq!(err.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            err.message.contains("PASS") && err.message.contains("BLOCKED"),
            "rejection must echo the declared values, got: {}",
            err.message
        );
    }

    /// The same contract accepts a body that IS a member of `values` —
    /// `204`, unaffected submit.
    #[tokio::test]
    async fn worker_submit_accepts_body_inside_contract_values() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "gate", 1, None).await;
        state.engine.register_verdict_contracts(HashMap::from([(
            "gate".to_string(),
            body_verdict_contract(&["PASS", "BLOCKED"]),
        )]));

        let status = worker_submit(
            State(state.clone()),
            bearer_headers(&handle),
            Query(SubmitQuery { ok: None }),
            axum::body::Bytes::from("PASS"),
        )
        .await
        .expect("value inside declared values must succeed");
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    /// Opt-in regression guard: an agent with NO declared verdict contract
    /// is entirely unaffected — `worker_submit` still returns `204` for an
    /// arbitrary body, exactly the pre-GH-#50 behavior.
    #[tokio::test]
    async fn worker_submit_without_a_declared_contract_is_unaffected() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);
        let task_id = StepId::new();
        // No `register_verdict_contracts` call — the agent declared no contract.
        let handle = seed_task_with_handle(&state, &task_id, "undeclared-agent", 1, None).await;

        let status = worker_submit(
            State(state.clone()),
            bearer_headers(&handle),
            Query(SubmitQuery { ok: None }),
            axum::body::Bytes::from("anything at all, no contract to violate"),
        )
        .await
        .expect("no contract declared must never reject");
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    /// A `channel: "part"` contract rejects a `worker_artifact?name=verdict`
    /// value outside `values` with `422`.
    #[tokio::test]
    async fn worker_artifact_verdict_part_rejects_value_outside_contract_with_422() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "gate", 1, None).await;
        state.engine.register_verdict_contracts(HashMap::from([(
            "gate".to_string(),
            part_verdict_contract(&["PASS", "BLOCKED"]),
        )]));

        let err = worker_artifact(
            State(state.clone()),
            bearer_headers(&handle),
            Query(ArtifactQuery {
                name: "verdict".to_string(),
            }),
            axum::body::Bytes::from("UNKNOWN"),
        )
        .await
        .expect_err("value outside declared values must reject");
        assert_eq!(err.status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    /// A part named anything OTHER than `"verdict"` skips the gate
    /// entirely, even with a `channel: "part"` contract declared — `204`,
    /// existing pre-GH-#50 behavior unchanged.
    #[tokio::test]
    async fn worker_artifact_non_verdict_part_skips_the_gate() {
        let data_store: Arc<dyn OutputStore> = Arc::new(InMemoryOutputStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let state = test_state(data_store, run_store);
        let task_id = StepId::new();
        let handle = seed_task_with_handle(&state, &task_id, "gate", 1, None).await;
        state.engine.register_verdict_contracts(HashMap::from([(
            "gate".to_string(),
            part_verdict_contract(&["PASS", "BLOCKED"]),
        )]));

        let status = worker_artifact(
            State(state.clone()),
            bearer_headers(&handle),
            Query(ArtifactQuery {
                name: "notes".to_string(),
            }),
            axum::body::Bytes::from("anything at all"),
        )
        .await
        .expect("non-verdict part name must never be gated");
        assert_eq!(status, StatusCode::NO_CONTENT);
    }
}
