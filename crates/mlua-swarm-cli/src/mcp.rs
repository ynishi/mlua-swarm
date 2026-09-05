//! mse mcp: MCP server (stdio) for mlua-swarm-engine.
//!
//! Sibling of `mse serve` (HTTP). External AI agents (Claude Code / other MCP clients)
//! call the `swarm.run` / `swarm.status` / `swarm.cancel` tools via stdio JSON-RPC.
//!
//! v2 wiring: `swarm.run` is wired to `TaskApplication.handle` (= the same entry
//! point as `mse serve`'s `/v1/tasks`). Engine boot reuses `default_registry` from
//! the mse serve lib (= the baseline `identity` RustFn is pre-baked, the shared SoT
//! across the three sibling binaries).

mod block_runner;
mod operator_client;
mod resources;
// launchd knowledge lives in `crate::server::launchd` (relocated from
// `mcp/server_control.rs`). Every lifecycle MCP tool body forwards to
// `launchd::*` — the tool bodies themselves stay free of `launchctl` /
// plist path / launchd state-parsing literals (Crux #1).
use crate::server::launchd;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures_util::FutureExt;
use mlua_swarm::application::{
    BlueprintRef, TaskApplication, TaskApplicationInput, TaskApplicationOutput,
};
use mlua_swarm::blueprint::store::{BlueprintStore, InMemoryBlueprintStore};
use mlua_swarm::blueprint::{resolve_bound_agents, Blueprint, RunnerResolutionSource};
use mlua_swarm::store::run::{
    InMemoryRunStore, RunContext, RunRecord, RunStatus as StoreRunStatus, RunStore,
};
use mlua_swarm::store::trace::TokenUsage;
use mlua_swarm::types::{RunId, StepId, TaskId};
use mlua_swarm::{
    binding_requests, Compiler, Engine, EngineCfg, OperatorKind, Role, TaskLaunchService,
};
use operator_client::{ClientError, OperatorClientState};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    AnnotateAble, CallToolResult, Content, Implementation, ListResourcesResult,
    PaginatedRequestParams, RawResource, ReadResourceRequestParams, ReadResourceResult,
    ResourceContents, ServerCapabilities, ServerInfo,
};
use rmcp::service::RequestContext;
use rmcp::{
    tool, tool_handler, tool_router, ErrorData as McpError, RoleServer, ServerHandler, ServiceExt,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::sync::RwLock;

/// In-process run handle.
#[allow(dead_code)]
#[derive(Clone, Debug)]
struct RunHandle {
    run_id: String,
    status: RunStatus,
    /// The Run's owning Task, when known. `None` briefly for an
    /// HTTP-proxied (`Id` selector) dispatch before the server's response
    /// is parsed. Populated for in-process (Inline/File) dispatch from the
    /// start (issue GH #34 — `mse_doctor`'s `audit_findings` scan needs
    /// `task_id` to address `GET /v1/tasks/:id/runs/:run/steps`).
    task_id: Option<String>,
    /// Local cancel-request mark, flipped by `swarm_cancel`. Independent
    /// from `status` because `swarm_status`'s HTTP enrichment overwrites
    /// `status` with the server's authoritative view — which currently
    /// does not yet know about the cancel (in-flight handle abort is v3
    /// carry). Callers who need to know "was cancel requested locally?"
    /// read this flag instead of relying on `status` staying `Cancelled`.
    cancel_requested: bool,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum RunStatus {
    Pending,
    Running,
    Done,
    Cancelled,
    Failed,
}

struct Inner {
    runs: HashMap<String, RunHandle>,
    task_app: Arc<TaskApplication>,
    store: Arc<dyn BlueprintStore>,
    /// In-process run trace (issue #13): in-memory only — the stdio MCP
    /// adapter has no persistence; `swarm_status` reads step_entries here.
    run_store: Arc<dyn RunStore>,
    /// In-process RunTrace rail (per-step run stats): the TraceEvent
    /// stream `swarm_run` / `swarm_status` surface as `log_tail`.
    /// In-memory only, same lifetime caveat as `run_store`.
    run_trace_store: Arc<dyn mlua_swarm::store::trace::RunTraceStore>,
}

#[derive(Clone)]
struct MseServer {
    state: Arc<RwLock<Inner>>,
    /// WS client embedding: owns the `sid → SessionEntry` map backing
    /// `mse_operator_join` / `mse_pending_wait` / `mse_ack` / `mse_operator_leave`.
    op_client: Arc<OperatorClientState>,
}

impl MseServer {
    fn new() -> Self {
        let engine = Engine::new(EngineCfg::default());
        // default_registry (from the server lib SoT) = Subprocess + RustFn
        // (baseline `identity` already baked in) + an empty Operator
        // factory. Shares the bootstrap worker wiring with `mse serve`;
        // the old path that injected a separate implementation has been
        // retired.
        let registry = mlua_swarm_server::default_registry();
        let store: Arc<dyn BlueprintStore> = Arc::new(InMemoryBlueprintStore::new());
        let compiler = Compiler::new(registry);
        let launch = Arc::new(TaskLaunchService::new(engine, compiler));
        let task_app = Arc::new(TaskApplication::new(launch, store.clone()));
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let run_trace_store: Arc<dyn mlua_swarm::store::trace::RunTraceStore> =
            Arc::new(mlua_swarm::store::trace::InMemoryRunTraceStore::new());
        Self {
            state: Arc::new(RwLock::new(Inner {
                runs: HashMap::new(),
                task_app,
                store,
                run_store,
                run_trace_store,
            })),
            op_client: Arc::new(OperatorClientState::new()),
        }
    }
}

/// Unix epoch seconds (same convention as the store records' timestamps).
/// GH #67: best-effort `GET /v1/runs/:id` used by `swarm_status` to reach
/// past the local `RunHandle` and pick up the server-side
/// `SqliteRunStore`'s authoritative view of a detached run. `None` on any
/// error (HTTP client build / send / non-2xx / non-JSON body / timeout) —
/// callers fall back to the local run store trace.
async fn fetch_run_via_http(bind: &str, run_id: &str) -> Option<JsonValue> {
    let url = crate::http::Endpoint::resolve(Some(bind)).url(&format!("/v1/runs/{run_id}"));
    let client = crate::http::client_builder()
        .timeout(Duration::from_secs(5))
        .build()
        .ok()?;
    let resp = client.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    resp.json::<JsonValue>().await.ok()
}

/// Why a `GET /v1/runs/:id` fetch failed, for the callers that must tell
/// "no such run" (a caller mistake) from "could not ask" (a transport
/// fault) — the distinction [`fetch_run_via_http`] deliberately discards.
#[derive(Debug)]
enum RunFetchError {
    /// The server answered `404`: no Run with that id.
    NotFound,
    /// Client build / send / non-JSON body / any other non-2xx status.
    Transport(String),
}

/// Status-preserving `GET /v1/runs/:id`, the read path behind
/// [`MseServer::swarm_run_stats`]. Same route as [`fetch_run_via_http`],
/// opposite error contract: nothing is swallowed, so an unknown run id
/// surfaces as `invalid_params` and an unreachable server as
/// `internal_error` instead of both becoming an empty report.
async fn fetch_run_strict(bind: &str, run_id: &str) -> Result<JsonValue, RunFetchError> {
    let url = crate::http::Endpoint::resolve(Some(bind)).url(&format!("/v1/runs/{run_id}"));
    let client = crate::http::client_builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| RunFetchError::Transport(format!("client build: {e}")))?;
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| RunFetchError::Transport(format!("GET {url}: {e}")))?;
    let status = resp.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Err(RunFetchError::NotFound);
    }
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(RunFetchError::Transport(format!(
            "GET {url}: HTTP {} — {body}",
            status.as_u16()
        )));
    }
    resp.json::<JsonValue>()
        .await
        .map_err(|e| RunFetchError::Transport(format!("GET {url}: decode: {e}")))
}

/// Fold a Run's `step_entries` into the per-step / per-model / whole-run
/// cost view [`MseServer::swarm_run_stats`] returns. Pure (no I/O, no
/// clock) so the aggregation rules are unit-testable against literal
/// entries.
///
/// The rules, all of which have to tolerate a partially-reported run —
/// stats are optional at every worker boundary, and absence must never
/// look like zero cost:
///
/// - `steps[]` mirrors the input order, one row per entry, carrying only
///   the cost-relevant fields (`step_ref` / `status` / `attempt` /
///   `duration_ms` / `worker_kind` / `model` / `usage`). A field the
///   entry does not carry is omitted from the row rather than nulled.
/// - `totals.input_tokens` / `output_tokens` / `total_tokens` sum only
///   the entries that carry a `usage` object; `steps_with_stats` counts
///   exactly those, against `steps_total` for every entry — so a reader
///   can see how much of the run the totals actually cover.
/// - `totals.duration_ms_sum` sums every entry's dispatcher-measured
///   `duration_ms` (independent of worker self-reporting). It is a sum of
///   per-step durations, NOT the run's wall-clock time: steps that ran
///   concurrently (a `Fanout`) are counted in full, each.
/// - `by_model` groups by the self-reported `model`; an entry without one
///   contributes to the totals but to no model bucket.
fn aggregate_run_stats(step_entries: &[JsonValue]) -> JsonValue {
    let mut steps: Vec<JsonValue> = Vec::with_capacity(step_entries.len());
    let (mut input, mut output, mut total) = (0u64, 0u64, 0u64);
    let mut duration_sum = 0u64;
    let mut with_stats = 0usize;
    // BTreeMap: a stable (model-sorted) key order in the JSON object.
    let mut by_model: BTreeMap<String, (u64, u64, u64, u64)> = BTreeMap::new();

    for entry in step_entries {
        let mut row = serde_json::Map::new();
        for field in [
            "step_ref",
            "status",
            "attempt",
            "duration_ms",
            "worker_kind",
            "model",
            "usage",
        ] {
            if let Some(v) = entry.get(field) {
                if !v.is_null() {
                    row.insert(field.to_string(), v.clone());
                }
            }
        }
        steps.push(JsonValue::Object(row));

        let usage = entry.get("usage").and_then(|u| u.as_object());
        let read = |key: &str| -> u64 {
            usage
                .and_then(|u| u.get(key))
                .and_then(JsonValue::as_u64)
                .unwrap_or(0)
        };
        let (e_in, e_out, e_total) = (
            read("input_tokens"),
            read("output_tokens"),
            read("total_tokens"),
        );
        if usage.is_some() {
            with_stats += 1;
            input += e_in;
            output += e_out;
            total += e_total;
        }
        if let Some(ms) = entry.get("duration_ms").and_then(JsonValue::as_u64) {
            duration_sum += ms;
        }
        if let Some(model) = entry.get("model").and_then(JsonValue::as_str) {
            let bucket = by_model.entry(model.to_string()).or_insert((0, 0, 0, 0));
            bucket.0 += 1;
            bucket.1 += e_in;
            bucket.2 += e_out;
            bucket.3 += e_total;
        }
    }

    let by_model: serde_json::Map<String, JsonValue> = by_model
        .into_iter()
        .map(|(model, (steps, i, o, t))| {
            (
                model,
                serde_json::json!({
                    "steps": steps,
                    "input_tokens": i,
                    "output_tokens": o,
                    "total_tokens": t,
                }),
            )
        })
        .collect();

    serde_json::json!({
        "steps": steps,
        "totals": {
            "input_tokens": input,
            "output_tokens": output,
            "total_tokens": total,
            "duration_ms_sum": duration_sum,
            "steps_with_stats": with_stats,
            "steps_total": step_entries.len(),
        },
        "by_model": JsonValue::Object(by_model),
    })
}

/// Best-effort `GET /v1/runs/:id/trace?latest=50` — the server-side
/// RunTrace tail `swarm_status` folds into `log_tail`. Same silent-`None`
/// error contract as [`fetch_run_via_http`].
async fn fetch_trace_tail_via_http(bind: &str, run_id: &str) -> Option<Vec<JsonValue>> {
    let url = crate::http::Endpoint::resolve(Some(bind))
        .url(&format!("/v1/runs/{run_id}/trace?latest=50"));
    let client = crate::http::client_builder()
        .timeout(Duration::from_secs(5))
        .build()
        .ok()?;
    let resp = client.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body = resp.json::<JsonValue>().await.ok()?;
    body.get("events")
        .and_then(|e| e.as_array())
        .map(|a| a.to_vec())
}

/// GH #67: reflect a server-reported run status string back into the local
/// `RunHandle`'s `RunStatus`. Returns `None` for unrecognized strings
/// (leaves the handle untouched) or when the server's status is still
/// `Running` (no change needed).
fn parse_run_status(s: &str) -> Option<RunStatus> {
    match s {
        "done" => Some(RunStatus::Done),
        "failed" => Some(RunStatus::Failed),
        _ => None,
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Render a caught panic payload as a human-readable string (sibling of the
/// server crate's helper of the same name).
fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Panic guard for the in-process (stdio MCP) run driver — the local
/// counterpart of the server crate's `tasks::catch_run_panic`.
///
/// A panic inside the driver would otherwise unwind the detached task (the
/// `tokio::time::timeout` ceiling with it) or the tool call itself, leaving
/// the local `RunRecord` / `RunHandle` stuck at `Running` with `swarm_status`
/// polling forever. Here the panic is caught and the Run is terminated:
/// `RunStore` gets `Interrupted` (guarded by
/// [`RunStore::try_transition`] so a Run that already finalized keeps its
/// verdict) plus a structured `{"error": ...}` result, and the local
/// `RunHandle` gets `Failed` — the stdio adapter's status enum has no
/// `Interrupted` variant, and inventing one would change the tool's wire
/// contract.
///
/// Relies on unwinding: a future `[profile.release] panic = "abort"` would
/// make this a no-op.
async fn catch_in_process_run_panic<T, F>(
    state: &Arc<RwLock<Inner>>,
    run_store: &Arc<dyn RunStore>,
    run_id: &str,
    run_id_typed: &RunId,
    site: &str,
    fut: F,
) -> Result<T, String>
where
    F: std::future::Future<Output = T>,
{
    match std::panic::AssertUnwindSafe(fut).catch_unwind().await {
        Ok(value) => Ok(value),
        Err(payload) => {
            let message = panic_payload_to_string(payload);
            tracing::error!(
                run_id,
                site,
                payload = %message,
                "in-process run driver panicked — marking the Run Interrupted"
            );
            match run_store
                .try_transition(
                    run_id_typed,
                    StoreRunStatus::Running,
                    StoreRunStatus::Interrupted,
                )
                .await
            {
                Ok(true) => {
                    let envelope = serde_json::json!({
                        "error": format!("run driver panicked at {site}: {message}"),
                    });
                    let _ = run_store.set_result(run_id_typed, envelope).await;
                }
                Ok(false) => {
                    tracing::warn!(
                        run_id,
                        site,
                        "in-process run driver panicked, but the Run is no longer `Running` — leaving its terminal status untouched"
                    );
                    return Err(message);
                }
                Err(e) => {
                    tracing::warn!(run_id, error = %e, "panic guard: run try_transition failed");
                    return Err(message);
                }
            }
            let mut inner = state.write().await;
            if let Some(h) = inner.runs.get_mut(run_id) {
                h.status = RunStatus::Failed;
            }
            Err(message)
        }
    }
}

/// What the spawned in-process run driver reports back to a synchronous
/// `swarm_run` tool call: the driver owns the run to its
/// terminal state on its own task, so an aborted tool call drops only the
/// wait for this value.
enum SyncRunReport {
    /// The flow ran to completion; the payload shapes the `status: "done"`
    /// tool body.
    Done(Box<TaskApplicationOutput>),
    /// The flow failed or hit the TTL ceiling — the string is the `error`
    /// field of the `status: "failed"` tool body.
    Failed(String),
    /// The driver never produced an outcome (panic caught by
    /// [`catch_in_process_run_panic`], or the driver task itself vanished).
    /// Reported through the short body shape that carries no post-run
    /// store snapshot.
    Aborted(String),
}

/// Maps `operator_client::ClientError` to an `McpError` for tool responses.
/// `UnknownSid` / `InvalidAckKind` / `SessionClosed` all name something the
/// caller passed in as the thing that could not be served, so they are
/// `invalid_params`; `Http` / `Ws` are transport-layer failures
/// (`internal_error`). `SessionClosed` says the sid's WS could not be
/// re-established *for this call* — the sid itself stays valid, and the
/// driver is free to call again once the server is back.
/// Adds the [`block_runner::LAUNCH_VARIANT`] capability to a join manifest
/// that does not already declare it. Idempotent: a manifest that names the
/// variant (with whatever model / tools) is returned untouched.
fn with_block_capability(
    mut manifest: mlua_swarm::AgentProviderManifest,
) -> mlua_swarm::AgentProviderManifest {
    let declared = manifest
        .capabilities
        .iter()
        .any(|c| c.launch_variant.as_deref() == Some(block_runner::LAUNCH_VARIANT));
    if !declared {
        manifest
            .capabilities
            .push(mlua_swarm::AgentProviderCapability {
                launch_variant: Some(block_runner::LAUNCH_VARIANT.to_string()),
                resolved_model: None,
                effective_tools: Vec::new(),
                capability_snapshot_digest: None,
            });
    }
    manifest
}

/// Runs one `agent-block` spawn to completion on this host and reports it
/// back to the server the way a SubAgent would: staged parts to
/// `/v1/worker/artifact`, the body to `/v1/worker/submit` (with `ok=false`
/// on failure), then `spawn_ack`. Every failure mode lands as a failed
/// attempt whose body says why — a block the server cannot see the
/// outcome of would wedge the step until its TTL.
async fn dispatch_block_spawn(
    op: Arc<OperatorClientState>,
    sid: String,
    spawn: block_runner::BlockSpawn,
) {
    let base = op.http_base().trim_end_matches('/').to_string();
    let client = match crate::http::client_builder()
        .timeout(Duration::from_secs(30))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(agent = %spawn.agent, "block runner: client build: {e}");
            let _ = op
                .ack(
                    &sid,
                    spawn.req_id.clone(),
                    "spawn_ack",
                    Some(serde_json::json!({})),
                    false,
                    Some(format!("block runner: client build: {e}")),
                    None,
                )
                .await;
            return;
        }
    };

    let outcome = run_block_spawn(&client, &base, &spawn).await;

    let (body, ok, error) = match &outcome {
        Ok(out) => {
            for (name, content) in &out.artifacts {
                if let Err(e) = post_worker_body(
                    &client,
                    &base,
                    &spawn.worker_handle,
                    Some(name),
                    block_runner::body_text(content),
                    true,
                )
                .await
                {
                    tracing::error!(agent = %spawn.agent, part = %name, "block runner: artifact: {e}");
                }
            }
            (block_runner::body_text(&out.value), out.ok, None)
        }
        Err(e) => {
            tracing::error!(agent = %spawn.agent, "block runner: {e}");
            (e.clone(), false, Some(e.clone()))
        }
    };

    if let Err(e) = post_worker_body(&client, &base, &spawn.worker_handle, None, body, ok).await {
        tracing::error!(agent = %spawn.agent, "block runner: submit: {e}");
    }
    if let Err(e) = op
        .ack(
            &sid,
            spawn.req_id.clone(),
            "spawn_ack",
            Some(serde_json::json!({})),
            ok,
            error,
            None,
        )
        .await
    {
        tracing::error!(agent = %spawn.agent, "block runner: spawn_ack: {e}");
    }
    tracing::info!(agent = %spawn.agent, task_id = %spawn.task_id, ok, "block runner: done");
}

/// Fetches the step's worker payload and runs the block named by the
/// spawn's agent under `MSE_BLOCKS_DIR`.
async fn run_block_spawn(
    client: &reqwest::Client,
    base: &str,
    spawn: &block_runner::BlockSpawn,
) -> Result<block_runner::BlockOutcome, String> {
    let dir = block_runner::blocks_dir()?;
    let script = block_runner::resolve_block_script(&dir, &spawn.agent)?;

    let url = format!("{base}/v1/worker/prompt");
    let resp = client
        .get(&url)
        .query(&[("task_id", spawn.task_id.as_str())])
        .header("Authorization", format!("Bearer {}", spawn.worker_handle))
        .send()
        .await
        .map_err(|e| format!("worker fetch: {e}"))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(format!("worker fetch: HTTP {} — {text}", status.as_u16()));
    }
    let payload: JsonValue =
        serde_json::from_str(&text).map_err(|e| format!("worker fetch decode: {e}"))?;

    let prompt = payload
        .get("prompt")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let system = match payload.get("system_ref") {
        Some(sr) => {
            let system_ref: mlua_swarm::types::SystemRef = serde_json::from_value(sr.clone())
                .map_err(|e| format!("system_ref decode: {e}"))?;
            let bytes = fetch_system_ref_bytes(client, base, &system_ref).await?;
            Some(String::from_utf8_lossy(&bytes).into_owned())
        }
        None => payload
            .get("system")
            .and_then(|v| v.as_str())
            .map(str::to_string),
    };
    let context = payload.get("context");
    let input = block_runner::BlockInput {
        script,
        project_root: block_runner::project_root_from_context(context),
        prompt,
        system,
        extra_globals: block_runner::context_globals(context),
    };
    tracing::info!(
        agent = %spawn.agent,
        script = %input.script.display(),
        project_root = %input.project_root.display(),
        "block runner: start"
    );
    block_runner::run_block(input).await
}

/// One worker POST: `name` = Some → `/v1/worker/artifact` (a staged part),
/// None → `/v1/worker/submit` (the body, `?ok=false` when the attempt
/// failed). Same wire as `mse_worker_submit`, minus the tool surface.
async fn post_worker_body(
    client: &reqwest::Client,
    base: &str,
    worker_handle: &str,
    name: Option<&str>,
    body: String,
    ok: bool,
) -> Result<(), String> {
    let url = worker_submit_endpoint_url(base, name)?;
    let mut request = client
        .post(url)
        .header("Authorization", format!("Bearer {worker_handle}"))
        .header("Content-Type", "text/plain");
    if name.is_none() && !ok {
        request = request.query(&[("ok", "false")]);
    }
    let resp = request
        .body(body)
        .send()
        .await
        .map_err(|e| format!("worker post: {e}"))?;
    let status = resp.status();
    if status != reqwest::StatusCode::NO_CONTENT {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!(
            "worker post: HTTP {} (expected 204) — {text}",
            status.as_u16()
        ));
    }
    Ok(())
}

fn client_error_to_mcp(e: ClientError) -> McpError {
    match e {
        ClientError::UnknownSid(_)
        | ClientError::InvalidAckKind(_)
        | ClientError::SessionClosed(_) => McpError::invalid_params(e.to_string(), None),
        ClientError::Http(_) | ClientError::Ws(_) => McpError::internal_error(e.to_string(), None),
    }
}

/// Parse a tool argument as a `RunId` before any network I/O.
///
/// The server's own `/v1/runs/:id/*` handlers open with the same
/// `RunId::parse` and answer `400`, so this adds no rule of its own — it
/// just spends the round trip only when there is a chance of an answer,
/// the way `mse_worker_fetch` treats a `task_id`.
fn parse_run_id(run_id: String) -> Result<RunId, McpError> {
    RunId::parse(run_id).map_err(|e| McpError::invalid_params(format!("invalid run_id: {e}"), None))
}

/// One `audit:<step_ref>` artifact spotted by `mse_doctor`'s `audit_findings`
/// scan (GH #34) — an after-run audit agent (`AfterRunAuditMiddleware`,
/// `mlua-swarm` core) left a finding on a tracked run's step output.
/// Purely observational: this struct's presence never implies the audited
/// step failed or was gated (`Blueprint.audits`'s binding invariant).
#[derive(Debug, Clone, Serialize)]
struct AuditFinding {
    task_id: String,
    run_id: String,
    /// The AUDITED step's own ref name (the artifact name's `audit:` prefix
    /// stripped) — e.g. `"echo"` for an `audit:echo` artifact.
    step: String,
    /// The raw artifact name as it appears in the steps listing
    /// (`"audit:<step_ref>"`).
    artifact_name: String,
}

/// Pure extraction: given a `GET /v1/tasks/:id/runs/:run/steps` response
/// body (`{task_id, run_id, steps: [{name, ...}, ...]}`), pick out every
/// step whose `name` starts with `audit:` — the
/// `AfterRunAuditMiddleware`/`OutputEvent::Artifact` naming convention
/// (GH #34). A step whose name does not carry that prefix (the
/// audited step itself, or any other OUTPUT artifact) is not a finding.
///
/// Kept a pure function (no I/O, no `self`) so it is testable without a
/// live `mse serve` process — feed it a hand-built
/// `serde_json::json!({"task_id": ..., "run_id": ..., "steps": [...]})`.
fn extract_audit_findings(steps_body: &JsonValue) -> Vec<AuditFinding> {
    let task_id = steps_body
        .get("task_id")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let run_id = steps_body
        .get("run_id")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let Some(steps) = steps_body.get("steps").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    steps
        .iter()
        .filter_map(|step| {
            let name = step.get("name")?.as_str()?;
            let step_ref = name.strip_prefix("audit:")?;
            Some(AuditFinding {
                task_id: task_id.to_string(),
                run_id: run_id.to_string(),
                step: step_ref.to_string(),
                artifact_name: name.to_string(),
            })
        })
        .collect()
}

#[derive(Deserialize, JsonSchema)]
struct DoctorReq {
    /// Where `mse serve` is: a base URL (`https://host`, scheme included)
    /// or a bare `host:port` (which gets `http://`). Omitted falls back to
    /// `MSE_HTTP`, then to `http://127.0.0.1:7777`.
    #[serde(default)]
    bind: Option<String>,
}

/// Request for `mse_run_ctx` — reading back a run's recorded ctx.
#[derive(Deserialize, JsonSchema)]
struct RunCtxReq {
    /// The run whose ctx to read (`R-<hex>`), as returned by `swarm_run`.
    run_id: String,
    /// Which branch to read, as a `$.a.b` path (for example
    /// `$.aggregate.out`). Omitted enumerates the top level with each
    /// branch's size, so a caller can see where the bytes are before
    /// asking for them.
    #[serde(default)]
    at: Option<String>,
}

/// Request for the `mse_http` escape hatch.
///
/// Deliberately carries neither a URL nor a token: the endpoint is
/// resolved from this process's configuration
/// ([`crate::http::Endpoint`]), and the access-token header is attached by
/// [`crate::http::client_builder`]. A caller that cannot name a host
/// cannot aim this at one, and a token that never appears in an argument
/// never gets transcribed into a transcript. Adding a `url` / `base_url` /
/// `token` field here would undo both at once — the test
/// `mse_http_req_exposes_no_url_or_token_field` is there to make that a
/// deliberate act rather than an accident.
#[derive(Deserialize, JsonSchema)]
struct HttpReq {
    /// HTTP method: `GET` (default), `POST`, `PATCH`, or `DELETE`.
    #[serde(default)]
    method: Option<String>,
    /// Absolute API path, `/v1/…` only (see
    /// [`crate::http::validate_api_path`]).
    path: String,
    /// Optional JSON request body. Ignored for `GET`.
    #[serde(default)]
    body: Option<JsonValue>,
}

/// Default `agent.md` size thresholds used by the `bp_doctor` tool when the
/// caller does not override them.
///
/// Rationale is in the guide `mse://guides/agent-md-authoring §Size targets`:
/// the fetched `system_prompt` body has to leave headroom in the SubAgent's
/// context window for the actual task payload (Read results, `tool_result`
/// bodies, PreOut file contents). Well above these thresholds, SubAgents on
/// a 200 K-window model deterministically fail with "Prompt is too long" on
/// the first non-trivial follow-up payload.
///
/// The BLOCK band is a report label, **not** enforcement — `bp_doctor` never
/// prevents any dispatch. Models with larger context windows (e.g. Opus-tier
/// or long-window Fable variants) can override the thresholds per call or
/// pass `disable_block=true` to skip the BLOCK band entirely.
const AGENT_MD_DEFAULT_WARN_BYTES: usize = 25 * 1024;
const AGENT_MD_DEFAULT_WARN_LINES: usize = 200;
const AGENT_MD_DEFAULT_BLOCK_BYTES: usize = 50 * 1024;
const AGENT_MD_DEFAULT_BLOCK_LINES: usize = 500;

/// Resolved severity thresholds for a single `bp_doctor` invocation. Built
/// from `BpDoctorReq`, applying defaults where the caller omitted a field.
#[derive(Debug, Clone, Copy)]
struct AgentMdThresholds {
    warn_bytes: usize,
    warn_lines: usize,
    block_bytes: usize,
    block_lines: usize,
    /// When true, BLOCK is not emitted — an agent that would otherwise be
    /// BLOCK is reported as WARN instead (bytes/lines still shown raw).
    disable_block: bool,
}

impl AgentMdThresholds {
    fn from_req(
        warn_bytes: Option<usize>,
        warn_lines: Option<usize>,
        block_bytes: Option<usize>,
        block_lines: Option<usize>,
        disable_block: Option<bool>,
    ) -> Self {
        Self {
            warn_bytes: warn_bytes.unwrap_or(AGENT_MD_DEFAULT_WARN_BYTES),
            warn_lines: warn_lines.unwrap_or(AGENT_MD_DEFAULT_WARN_LINES),
            block_bytes: block_bytes.unwrap_or(AGENT_MD_DEFAULT_BLOCK_BYTES),
            block_lines: block_lines.unwrap_or(AGENT_MD_DEFAULT_BLOCK_LINES),
            // BLOCK is disabled by default. Modern Claude models (Opus-tier
            // and long-window Fable variants) tolerate large system prompts,
            // and the tool never enforces anything anyway — the label alone
            // is not worth the false alarm. Callers who want the BLOCK band
            // pass `disable_block=false` explicitly.
            disable_block: disable_block.unwrap_or(true),
        }
    }
}

/// Pure classifier for `agent.md` severity — kept out of the tool method so it
/// is directly unit-testable. Returns `"OK" | "WARN" | "BLOCK"`.
///
/// BLOCK dominates WARN when either dimension trips the higher band. When
/// `thresholds.disable_block` is true, no agent is ever reported as BLOCK;
/// over-block-threshold agents fall back to WARN.
fn classify_agent_md_severity(
    bytes: usize,
    lines: usize,
    thresholds: &AgentMdThresholds,
) -> &'static str {
    let over_block = bytes >= thresholds.block_bytes || lines >= thresholds.block_lines;
    if over_block && !thresholds.disable_block {
        "BLOCK"
    } else if bytes >= thresholds.warn_bytes || lines >= thresholds.warn_lines {
        "WARN"
    } else {
        "OK"
    }
}

/// Aggregate the overall Blueprint verdict from per-agent severities.
/// BLOCK dominates WARN dominates OK. An empty list is OK (nothing to warn
/// about — the Blueprint has no agent bodies to fetch).
fn aggregate_agent_md_verdict(severities: &[&str]) -> &'static str {
    if severities.contains(&"BLOCK") {
        "BLOCK"
    } else if severities.contains(&"WARN") {
        "WARN"
    } else {
        "OK"
    }
}

fn default_true_bool() -> bool {
    true
}

#[derive(Deserialize, JsonSchema)]
struct BpArchiveReq {
    /// Blueprint id to archive (logical soft-delete via marker commit; reversible).
    id: String,
    /// Where `mse serve` is: a base URL (`https://host`, scheme included)
    /// or a bare `host:port` (which gets `http://`). Omitted falls back to
    /// `MSE_HTTP`, then to `http://127.0.0.1:7777`.
    #[serde(default)]
    bind: Option<String>,
    /// Safety guard: must be `true` to actually execute. Default false = dry-run report.
    #[serde(default)]
    confirm: bool,
}

#[derive(Deserialize, JsonSchema)]
struct BpSchemaReq {}

#[derive(Deserialize, JsonSchema)]
struct BpUnarchiveReq {
    /// Blueprint id to unarchive (appends an unarchive marker commit; audit-trail preserved).
    id: String,
    /// Where `mse serve` is: a base URL (`https://host`, scheme included)
    /// or a bare `host:port` (which gets `http://`). Omitted falls back to
    /// `MSE_HTTP`, then to `http://127.0.0.1:7777`.
    #[serde(default)]
    bind: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct BpDoctorReq {
    /// Blueprint id to inspect (agent `profile.system_prompt` bodies are what
    /// the SubAgent receives via fetch — this tool measures those directly).
    id: String,
    /// Where `mse serve` is: a base URL (`https://host`, scheme included)
    /// or a bare `host:port` (which gets `http://`). Omitted falls back to
    /// `MSE_HTTP`, then to `http://127.0.0.1:7777`.
    #[serde(default)]
    bind: Option<String>,
    /// Override WARN byte threshold. Default 25 * 1024 (25 KB). Set higher
    /// when targeting a large-context model.
    #[serde(default)]
    warn_bytes: Option<usize>,
    /// Override WARN line threshold. Default 200.
    #[serde(default)]
    warn_lines: Option<usize>,
    /// Override BLOCK byte threshold. Default 50 * 1024 (50 KB). Ignored
    /// when `disable_block=true`.
    #[serde(default)]
    block_bytes: Option<usize>,
    /// Override BLOCK line threshold. Default 500. Ignored when
    /// `disable_block=true`.
    #[serde(default)]
    block_lines: Option<usize>,
    /// When true (default), the BLOCK severity band is not emitted —
    /// over-threshold agents fall back to WARN. BLOCK is disabled by
    /// default because modern Claude models (Opus-tier / long-window Fable
    /// variants) tolerate large system prompts, and this tool never
    /// enforces anything. Pass `disable_block=false` to opt into the BLOCK
    /// band when running against a strict 200 K-window model.
    #[serde(default)]
    disable_block: Option<bool>,
    /// Clippy-style lint level overrides for this call — the
    /// top-precedence layer of the three-layer cascade (this >
    /// `agents[].lints` > `metadata.lints`). Keys are a stable lint kind
    /// literal (`"agent-md-size"`), a
    /// `"category:<correctness|suspicious|style|contract|migration>"`
    /// group, or `"all"`; values are `"allow"` / `"warn"` / `"deny"`.
    ///
    /// Parsed leniently on both axes: an unknown key *and* an
    /// unparseable value degrade to the `unknown-lint-kind` meta-lint —
    /// a typo never rejects the request. `allow` moves the finding to
    /// the response's `suppressed[]` array (omitted ≠ passed); `deny`
    /// escalates it to the BLOCK band of the aggregate verdict, which
    /// stays a report label (this tool never blocks anything).
    ///
    /// Finer-grained than the legacy `disable_*_lint` flags below: those
    /// are family-granular and call-site-only, this is per-kind,
    /// per-category and declarable in the Blueprint itself.
    #[serde(default)]
    lints: Option<BTreeMap<String, String>>,
    /// GH #45 tool_lint family (default enabled): when true, skip
    /// checking each agent profile's `tools` list against the live
    /// `mse://api/mcp-tools` registry. Set true to bypass the family
    /// when running against a Blueprint that intentionally references
    /// tool names not surfaced by the local `mse` build.
    ///
    /// Legacy spelling of the call-site `lints` entry
    /// `{"tool-unknown-mcp-ref": "allow"}`, with one difference kept for
    /// compatibility: this flag omits the family's per-agent field
    /// entirely instead of reporting the finding under `suppressed[]`.
    #[serde(default)]
    disable_tool_lint: Option<bool>,
    /// GH #45 output_contract_lint family (default enabled): when true,
    /// skip checking each agent profile's `extras.expected_output`
    /// declaration (see GH #44 for the field convention). Set true to
    /// bypass while the convention is still being rolled out.
    ///
    /// Legacy spelling of `{"output-contract-missing": "allow"}` (see
    /// `disable_tool_lint` for the one behavioral difference).
    #[serde(default)]
    disable_output_contract_lint: Option<bool>,
    /// GH #61 worker_binding_lint family (default enabled): when true,
    /// skip checking each operator-kind agent for the compile-required
    /// `profile.worker_binding`. Set true to bypass when auditing a
    /// Blueprint whose operator backends genuinely do not need one (i.e.
    /// direct-LLM operators; `mse serve`'s stock WS thin-path backend
    /// requires it).
    ///
    /// Legacy spelling of `{"worker-binding-missing": "allow"}` (see
    /// `disable_tool_lint` for the one behavioral difference).
    #[serde(default)]
    disable_worker_binding_lint: Option<bool>,
    /// C4 binding_lint family (default enabled): when true, skip the
    /// Blueprint-level operator-binding advisories
    /// (`binding_requirements_info` / `strict_binding_without_runners` /
    /// `legacy_worker_binding`). Set true to omit the top-level
    /// `binding_lint` section when auditing a Blueprint whose binding
    /// requirements are already understood.
    ///
    /// Legacy spelling of a call-site `allow` on the family's four kinds
    /// (`binding-requirements-info`, `strict-binding-without-runners`,
    /// `legacy-worker-binding`, `binding-resolution-error`) — see
    /// `disable_tool_lint` for the one behavioral difference.
    #[serde(default)]
    disable_binding_lint: Option<bool>,
    /// GH #76 DSL sugar skip_on_lint family (default enabled): when true,
    /// skip the Blueprint-level Skip-tier / `skip_on` DSL cross-check
    /// (`skip_on_missing_for_skip_like_verdict_value` /
    /// `skip_on_declared_but_no_matching_verdict_value` /
    /// `skip_on_pattern_conflicts_with_halt_on`). The family is
    /// BLOCK-disabled by default (WARN is the maximum severity ever
    /// emitted); pass `disable_skip_on_lint=true` to omit the
    /// top-level `skip_on_lint` section entirely.
    ///
    /// Legacy spelling of a call-site `allow` on the family's three
    /// `skip-on-*` kinds — see `disable_tool_lint` for the one
    /// behavioral difference.
    #[serde(default)]
    disable_skip_on_lint: Option<bool>,
    /// GH #78 context_policy_lint family (default enabled): when true,
    /// skip the context_policy / projection-root cross-checks
    /// (`context_policy_strips_projection_roots` /
    /// `projection_root_seed_missing`) and omit the top-level
    /// `context_policy_lint` section entirely. WARN is the maximum
    /// severity ever emitted.
    ///
    /// Legacy spelling of a call-site `allow` on
    /// `context-policy-strips-projection-roots` +
    /// `projection-root-seed-missing` — see `disable_tool_lint` for the
    /// one behavioral difference.
    #[serde(default)]
    disable_context_policy_lint: Option<bool>,
    /// `verdict_contract_lint` family (default enabled): when true, skip
    /// the reverse-direction verdict check (`verdict_value_unhandled` — a
    /// declared `agents[].verdict.values` entry no downstream cond reads)
    /// and omit the top-level `verdict_contract_lint` section entirely.
    /// WARN is the maximum severity ever emitted. Disable it for a
    /// Blueprint that deliberately declares informational verdict tokens
    /// nothing branches on.
    ///
    /// Legacy spelling of a call-site `allow` on
    /// `verdict-value-unhandled` + `verdict-contract-never-read` — see
    /// `disable_tool_lint` for the one behavioral difference.
    #[serde(default)]
    disable_verdict_contract_lint: Option<bool>,
    /// `spawner_hint_lint` family (default enabled): when true, skip the
    /// withdrawn-layer check (`removed_spawner_hint` — a
    /// `spawner_hints.layers` key naming a layer the engine no longer
    /// installs) and omit the top-level `spawner_hint_lint` section
    /// entirely. WARN is the maximum severity ever emitted here; the
    /// compile stage refuses the same kind outright.
    ///
    /// Disabling this hides the one authoring-time signal that an
    /// already-registered Blueprint cannot launch — registering does not
    /// compile, so nothing else in the report will say so. Legacy
    /// spelling of a call-site `allow` on `removed-spawner-hint`; see
    /// `disable_tool_lint` for the one behavioral difference.
    #[serde(default)]
    disable_spawner_hint_lint: Option<bool>,
    /// GH #78: optional simulated launch payload for the
    /// `projection_root_seed_missing` check — an object whose
    /// `"project_root"` / `"work_dir"` string fields mirror the canonical
    /// seed the real `POST /v1/tasks` request would carry. When omitted,
    /// the seed simulation is skipped (only the static
    /// `context_policy_strips_projection_roots` check runs); pass `{}`
    /// to simulate a launch that seeds neither root.
    #[serde(default)]
    simulated_launch: Option<serde_json::Value>,
}

#[derive(Deserialize, JsonSchema)]
struct BpNewReq {
    /// Template kind: `pipeline` (N-stage main-ai) / `single` (one-agent
    /// one-step) / `verdict` (3-stage verdict-gated with retry-through-fixer) /
    /// `fanout` (N parallel checkers + aggregate stage, GH #82). Any other
    /// value returns `status: "error"`, `stage: "render"` with the accepted
    /// list — the DSL parser stays strict; the "fuzzy" scope (`GH #62 Axis
    /// B`) is separate.
    template: String,
    /// Blueprint id (also the emitted `id` field in the rendered script).
    name: String,
    /// Stage names, comma-separated. `pipeline` / `verdict` / `fanout`.
    /// `pipeline` default: `stage1,stage2`. `verdict` default:
    /// `analyze,review,publish` (fixed 3-stage — extras ignored, missing
    /// slots fall back to defaults per position). `fanout` default:
    /// `checker1,checker2` for the parallel branches, followed by an
    /// implicit `aggregate` stage.
    #[serde(default)]
    stages: Option<String>,
    /// Agent name for the `single` template. Default `solo`.
    #[serde(default)]
    agent: Option<String>,
    /// Operator role name every emitted agent points at. Default `main-ai`
    /// (the same convention every bundled sample uses).
    #[serde(default)]
    operator: Option<String>,
    /// `profile.worker_binding` value for every emitted operator agent.
    /// Default `claude` (the Claude Code catch-all SubAgent variant).
    #[serde(default)]
    binding: Option<String>,
    /// Write the rendered `.bp.lua` here (absolute, or relative to the
    /// mse-mcp process CWD). When omitted, the rendered text is included
    /// in the response as `script`.
    #[serde(default)]
    out: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct BpBuildReq {
    /// Path to the `.bp.lua` DSL script (absolute, or relative to the
    /// mse-mcp process CWD).
    script_path: String,
    /// POST the built JSON to the running `mse serve`
    /// (`/v1/blueprints/:id`). Default true — this tool exists so a
    /// `.bp.lua` script can be registered without shelling out to
    /// `mse bp build --register`. Pass false for a build+lint-only dry
    /// run (the built JSON is then included in the response).
    #[serde(default = "default_true_bool")]
    register: bool,
    /// Where `mse serve` is: a base URL (`https://host`, scheme included)
    /// or a bare `host:port` (which gets `http://`). Omitted falls back to
    /// `MSE_HTTP`, then to `http://127.0.0.1:7777`.
    #[serde(default)]
    bind: Option<String>,
    /// Where to write the built Blueprint JSON, pretty-printed — same as
    /// the CLI's `-o`. The JSON is always written to a file: here when
    /// given, otherwise to `$MSE_HOME/bp/<bp id>.json`; the response
    /// names the file as `blueprint_file {path, bytes}`. Pre-expansion
    /// by default; refs embedded when `strict_embed` is set (the two
    /// differ only in that).
    #[serde(default)]
    out: Option<String>,
    /// Extra directories to resolve `$file` / `$agent_md` refs against
    /// (tier 4 of the include cascade — same as the CLI's repeatable
    /// `--include <DIR>`). Absolute, or relative to the mse-mcp process
    /// CWD. Tiers 1 (the script's own dir), 2 (in-bp
    /// `blueprint_ref_includes`), 3 (`MSE_BLUEPRINT_INCLUDES`) and 6
    /// (bundled samples) apply without this.
    #[serde(default)]
    include: Vec<String>,
    /// Require every `$file` / `$agent_md` ref to embed at build time,
    /// and emit them embedded. Default false: refs travel raw in the
    /// wire JSON and the server resolves them at register time; an
    /// unresolved ref only downgrades the lint to `warn (…)`. `true`
    /// mirrors the CLI's `--strict-embed` — an unresolved ref returns
    /// `status: "error"` with `stage: "lint"` and no register attempt,
    /// and a resolved one is written into the file / `blueprint` / the
    /// registered body in place of the ref. That is what lets a server
    /// which cannot read the author's files (a hosted `mse serve`)
    /// receive the prompts at all. Two more things follow from "the
    /// Blueprint is self-contained": a `$agent_md` whose kind the
    /// Blueprint does not declare (no sibling `kind`, no top-level
    /// `default_agent_kind`) is a `stage: "lint"` error rather than a
    /// silent pin to the schema default, and every embedded agent
    /// carries `profile.extras.embed {source, repo, rev}` naming the
    /// file and git revision it was built from.
    /// Independent from the server-side `mse serve
    /// --blueprint-strict-embed` switch (register-time raw-ref
    /// reject) — see `mse://guides/strict-embed-modes`.
    #[serde(default)]
    strict_embed: bool,
}

/// Default directory holding worker wrapper `.md` files, relative to the
/// mse-mcp process CWD — matches the Claude Code convention
/// (`.claude/agents/<variant>.md`).
const DEFAULT_WRAPPER_DIR: &str = ".claude/agents";

#[derive(Deserialize, JsonSchema)]
struct BpExplainAgentReq {
    /// Blueprint id (registered on the HTTP server).
    bp_id: String,
    /// Agent name inside the blueprint.
    agent: String,
    /// Where `mse serve` is: a base URL (`https://host`, scheme included)
    /// or a bare `host:port` (which gets `http://`). Omitted falls back to
    /// `MSE_HTTP`, then to `http://127.0.0.1:7777`.
    #[serde(default)]
    bind: Option<String>,
    /// Directory holding worker wrapper `.md` files (default
    /// `.claude/agents`). The wrapper lookup is a Claude Code backend
    /// concern and happens client-side; the server never reads wrapper
    /// files.
    #[serde(default)]
    wrapper_dir: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct BpExplainAgentsReq {
    /// Blueprint id (registered on the HTTP server).
    bp_id: String,
    /// Where `mse serve` is: a base URL (`https://host`, scheme included)
    /// or a bare `host:port` (which gets `http://`). Omitted falls back to
    /// `MSE_HTTP`, then to `http://127.0.0.1:7777`.
    #[serde(default)]
    bind: Option<String>,
    /// Directory holding worker wrapper `.md` files (default
    /// `.claude/agents`) — same client-side concern as
    /// `bp_explain_agent`'s `wrapper_dir`; the server never reads wrapper
    /// files.
    #[serde(default)]
    wrapper_dir: Option<String>,
}

/// Per-tool classification comparing a Blueprint agent's declared
/// (informational-only) `profile.tools` against the worker wrapper's
/// actual frontmatter `tools` list. Set comparison — order-independent,
/// exact string match, duplicates deduped, output vectors sorted
/// (backed by `BTreeSet`).
#[derive(Debug, Clone, PartialEq, Serialize)]
struct ToolDrift {
    /// Present in both the Blueprint's declared tools and the wrapper's
    /// actual tools.
    matched: Vec<String>,
    /// Declared in the Blueprint but absent from the wrapper — the agent
    /// designer believes this tool is usable, but the wrapper does not
    /// actually grant it. The most important signal of the three.
    declared_only: Vec<String>,
    /// Present in the wrapper but never declared in the Blueprint —
    /// informational only (the wrapper grants something the Blueprint
    /// never mentions).
    wrapper_only: Vec<String>,
}

/// Compare Blueprint-declared tools against the wrapper's actual
/// frontmatter tools. Pure, unit-testable (bp_doctor's classifier
/// functions follow the same convention).
fn diff_tools(declared: &[String], wrapper: &[String]) -> ToolDrift {
    use std::collections::BTreeSet;
    let declared_set: BTreeSet<&String> = declared.iter().collect();
    let wrapper_set: BTreeSet<&String> = wrapper.iter().collect();
    ToolDrift {
        matched: declared_set
            .intersection(&wrapper_set)
            .map(|s| (*s).clone())
            .collect(),
        declared_only: declared_set
            .difference(&wrapper_set)
            .map(|s| (*s).clone())
            .collect(),
        wrapper_only: wrapper_set
            .difference(&declared_set)
            .map(|s| (*s).clone())
            .collect(),
    }
}

/// GH #45: builds the MCP tool-name registry that bp_doctor's
/// `tool_lint` family compares agent-profile tool declarations against.
/// Pulls the set of tool names from the live `mse://api/mcp-tools`
/// resource (same source of truth every other schema resource uses), so
/// a phantom tool reference in an agent profile is a WARN even when the
/// server binary and the agent profile were authored against different
/// tool surfaces.
///
/// The registry is a `BTreeSet<String>` of *bare* MCP tool names (no
/// `mcp__mse__` prefix — that prefix is a Claude Code frontmatter
/// convention that lives on the profile side and is stripped when the
/// lint compares).
fn build_mcp_tool_registry() -> std::collections::BTreeSet<String> {
    use std::collections::BTreeSet;
    let value = match resources::mcp_tools_schema_value() {
        Ok(v) => v,
        Err(_) => return BTreeSet::new(),
    };
    value
        .get("tools")
        .and_then(|t| t.as_object())
        .map(|obj| obj.keys().cloned().collect())
        .unwrap_or_default()
}

/// GH #45 Lint 1: extract MCP tool references from an agent profile's
/// `tools` list and check each against the live registry. Pure,
/// unit-testable — the actual registry is built once per `bp_doctor`
/// invocation and threaded in by reference.
///
/// The heuristic for what counts as "an MCP tool reference" is
/// deliberately conservative:
///
/// - Entries with the `mcp__mse__` prefix are treated as MCP references;
///   the prefix is stripped and the tail must appear in the registry.
/// - Everything else (Claude Code built-ins such as `Read` / `Edit` /
///   `Grep` / `Bash` / `WebFetch` / `WebSearch`) is skipped — those are
///   not in the MCP registry by design, and false-positive averse is
///   this lint's stated posture (v1: `WARN` only, no BLOCK, per GH #45).
///
/// A profile with no `tools` entries returns severity `OK` and an empty
/// `unknown_tools` list.
fn classify_tool_lint(
    profile_tools: &[String],
    registry: &std::collections::BTreeSet<String>,
) -> serde_json::Value {
    let mcp_prefix = "mcp__mse__";
    let unknown: Vec<String> = profile_tools
        .iter()
        .filter_map(|t| {
            t.strip_prefix(mcp_prefix)
                .filter(|bare| !registry.contains(*bare))
                .map(|_| t.clone())
        })
        .collect();
    let severity = if unknown.is_empty() { "OK" } else { "WARN" };
    serde_json::json!({
        "severity": severity,
        "unknown_tools": unknown,
    })
}

/// GH #45 Lint 2: check whether an agent profile declares a
/// machine-readable output contract in the documented `extras`
/// convention. Sibling issue GH #44 defines the field:
///
/// ```json
/// {"expected_output": {"kind": "literal_enum" | "inline_markdown" | "file_sentinel",
///                       "pattern": <optional enum values or regex>}}
/// ```
///
/// The lint is intentionally permissive at v1 — the `pattern` field is
/// not validated, only `kind`. A missing `expected_output` is `WARN`
/// with a documented reason; a present-but-malformed one is `WARN` with
/// the specific defect named. A well-formed one is `OK`.
fn classify_output_contract_lint(extras: &serde_json::Value) -> serde_json::Value {
    let expected = match extras.get("expected_output") {
        Some(v) => v,
        None => {
            return serde_json::json!({
                "severity": "WARN",
                "present": false,
                "reason": "no expected_output declared in profile.extras",
            });
        }
    };
    let obj = match expected.as_object() {
        Some(o) => o,
        None => {
            return serde_json::json!({
                "severity": "WARN",
                "present": true,
                "reason": "expected_output is not a JSON object",
            });
        }
    };
    let kind = match obj.get("kind").and_then(|v| v.as_str()) {
        Some(k) => k,
        None => {
            return serde_json::json!({
                "severity": "WARN",
                "present": true,
                "reason": "expected_output missing string field `kind`",
            });
        }
    };
    match kind {
        "literal_enum" | "inline_markdown" | "file_sentinel" => serde_json::json!({
            "severity": "OK",
            "present": true,
            "kind": kind,
        }),
        other => serde_json::json!({
            "severity": "WARN",
            "present": true,
            "reason": format!("unknown expected_output.kind: {other}"),
        }),
    }
}

/// GH #61: check whether an operator-backed agent declares the
/// `profile.worker_binding` the WS thin-path operator (`mse serve`'s only
/// production operator backend, `WSOperatorSession`) requires at
/// dispatch. Front-loads the same fail-loud check `Compiler::compile`
/// applies at dispatch (`src/blueprint/compiler.rs` —
/// `profile.worker_binding is required`) into a lint the author sees
/// before the undispatchable Blueprint is registered.
///
/// Severity `WARN` — matches the sibling `tool_lint` /
/// `output_contract_lint` families' `bp_doctor` posture (report-only,
/// never blocks). BLOCK-severity front-loading is `bp_build`'s job (the
/// compile-lint stage there is fail-loud via `LintStubOperator`).
///
/// The `reason` field on WARN reuses the exact stderr message the
/// Compiler emits — same fix hint on either path (JSON literal /
/// `$agent_md` frontmatter), so an author who sees the lint here and an
/// author who sees the dispatch-time error read the same guidance.
///
/// Non-operator kinds (RustFn / Lua / AgentBlock / Subprocess) return
/// `OK` unconditionally — `worker_binding` is only meaningful for
/// WS-thin-path operator backends. `AgentKind::Operator` is the only
/// arm this lint fires on.
fn classify_worker_binding_lint(
    kind: &mlua_swarm::blueprint::AgentKind,
    worker_binding: Option<&str>,
) -> serde_json::Value {
    if !matches!(kind, mlua_swarm::blueprint::AgentKind::Operator) {
        return serde_json::json!({
            "severity": "OK",
            "kind_requires_binding": false,
        });
    }
    let present = worker_binding.is_some_and(|s| !s.is_empty());
    if present {
        serde_json::json!({
            "severity": "OK",
            "kind_requires_binding": true,
            "present": true,
        })
    } else {
        serde_json::json!({
            "severity": "WARN",
            "kind_requires_binding": true,
            "present": false,
            "reason": "profile.worker_binding is required for this operator backend. \
                       Fix by either: \
                       (a) if authoring the Blueprint JSON directly, add \
                       `agents[N].profile.worker_binding: \"<subagent-type>\"` \
                       to the JSON literal; or \
                       (b) if using an $agent_md file ref, add \
                       `worker_binding: <subagent-type>` to the agent .md frontmatter.",
        })
    }
}

/// C4 `binding_lint` family: resolves the Blueprint's Runner-backed agents
/// and emits advisory operator-binding findings. Three checks, all report-only
/// (INFO / WARN — never BLOCK), matching the sibling `tool_lint` /
/// `output_contract_lint` / `worker_binding_lint` posture:
///
/// - `binding_requirements_info` (INFO): one finding per Runner-backed agent
///   listing the launch variant / tools / model a joining operator's
///   `capability_manifest` must cover — the same declarations `GET
///   /v1/blueprints/:id/binding-requirements` returns (built here from the
///   identical `resolve_bound_agents` + `binding_requests` pair).
/// - `strict_binding_without_runners` (WARN): `strategy.strict_binding` is
///   `true` but no agent resolves to a Runner, so strict binding is a no-op
///   (there is nothing for a provider to attest).
/// - `legacy_worker_binding` (WARN): an agent's Runner came from the
///   deprecated `profile.worker_binding` fallback
///   ([`RunnerResolutionSource::LegacyWorkerBinding`]); points at `runner` /
///   `runner_ref` as the migration target. A legacy agent is still
///   Runner-backed, so it also appears once under `binding_requirements_info`
///   — the two findings are complementary (what to attest vs. migrate away
///   from the fallback).
///
/// Pure over the resolved Blueprint (no I/O), unit-testable. Uses the
/// legacy-permissive [`resolve_bound_agents`] (same as the explain endpoint) so
/// the `bp_doctor` advisory never fails on a Blueprint the server accepted; a
/// resolution failure (an unresolvable `runner_ref` — near-impossible for an
/// already-registered Blueprint) degrades to a single WARN note rather than
/// aborting the whole tool.
fn classify_binding_lint(bp: &Blueprint) -> serde_json::Value {
    let bound = match resolve_bound_agents(bp) {
        Ok(bound) => bound,
        Err(e) => {
            return serde_json::json!({
                "findings": [{
                    "check": "binding_resolution_error",
                    "severity": "WARN",
                    "message": format!("could not resolve Runner bindings: {e}"),
                }],
            });
        }
    };

    let mut findings: Vec<serde_json::Value> = Vec::new();

    // Check 1 — binding_requirements_info (INFO): one per Runner-backed agent.
    let requests = binding_requests(&bound);
    for req in &requests {
        findings.push(serde_json::json!({
            "check": "binding_requirements_info",
            "severity": "INFO",
            "agent": req.agent,
            "launch_variant": req.launch_variant,
            "tools": req.requested_tools,
            "model": req.requested_model,
            "message": format!(
                "agent '{}' needs a capability_manifest entry covering launch variant {:?}, \
                 tools {:?}, model {:?}",
                req.agent, req.launch_variant, req.requested_tools, req.requested_model
            ),
        }));
    }

    // Check 2 — strict_binding_without_runners (WARN): Blueprint-level.
    if bp.strategy.strict_binding && requests.is_empty() {
        findings.push(serde_json::json!({
            "check": "strict_binding_without_runners",
            "severity": "WARN",
            "message": "strategy.strict_binding is true but no agent resolves to a Runner; \
                        strict binding is a no-op (there is nothing for a provider to attest).",
        }));
    }

    // Check 3 — legacy_worker_binding (WARN): one per legacy-resolved agent.
    for bound_agent in &bound {
        if bound_agent.runner_source == RunnerResolutionSource::LegacyWorkerBinding {
            findings.push(serde_json::json!({
                "check": "legacy_worker_binding",
                "severity": "WARN",
                "agent": bound_agent.agent.name,
                "message": format!(
                    "agent '{}' resolves its Runner from the deprecated profile.worker_binding \
                     fallback; migrate to an explicit `runner` (inline) or `runner_ref` \
                     declaration.",
                    bound_agent.agent.name
                ),
            }));
        }
    }

    serde_json::json!({ "findings": findings })
}

/// GH #76 DSL sugar `skip_on_lint` family: Blueprint-scoped advisory checks
/// covering the DSL sugar `skip_on = { ... }` on a `B.stage` (which
/// compiles to a `Branch{cond = in(verdict, skip_on_list), then =
/// Seq{}, else = <stage body>}`) and its cross-reference against
/// declared `agents[].verdict.values`.
///
/// Three checks, all report-only (WARN — never BLOCK), matching the
/// sibling `binding_lint` family's posture. **BLOCK-disabled by
/// default** — the family's maximum severity emitted is `WARN`, per
/// the GH #76 DSL sugar spec: this lint surfaces DSL / verdict-value drift,
/// never enforces a fix.
///
/// - `skip_on_missing_for_skip_like_verdict_value` (WARN): an agent
///   declares a `verdict.values` entry that reads like a Skip signal
///   (`SKIP` / `NOT_APPLICABLE` / `N/A`, case-insensitive), yet no
///   `Branch` in the compiled flow uses that value in a `skip_on`
///   `in(...)` list. Points at either a missing `skip_on = { ... }`
///   or a stale verdict value.
/// - `skip_on_declared_but_no_matching_verdict_value` (WARN): a
///   compiled `skip_on` list carries a value that appears in no
///   agent's `verdict.values`. Dead skip guard — the upstream verdict
///   never produces that value, so the guard can never fire.
/// - `skip_on_pattern_conflicts_with_halt_on` (WARN): the same value
///   appears in a `skip_on` list AND in a `halt_on` / gate check in
///   the flow. Only one of the two guards will fire on that verdict
///   at run time; the overlap is at best redundant, at worst a logic
///   bug.
///
/// Pure over the resolved Blueprint (no I/O), unit-testable. Detection
/// is best-effort static: works against the `Branch{cond=In}` shape
/// the DSL emits, and against `Branch{cond=Eq/Ne{path, lit}}` /
/// `Branch{cond=Or{...eq...}}` for `halt_on` (the shape `bp_dsl`
/// `gate_cond` emits). Other shapes (arithmetic conds / callee-side
/// tests / hand-authored flow_dsl variants) are skipped silently
/// (same posture as `Compiler::verify_verdict_conds`).
fn classify_skip_on_lint(bp: &Blueprint) -> serde_json::Value {
    use mlua_flow_ir::{Expr, Node as FlowNode};

    /// Set of literal string values collected off the flow, tagged by
    /// which guard family they came from.
    #[derive(Default)]
    struct GuardValues {
        /// Every string appearing in a `Branch{cond = In{needle: Path
        /// ending in .parts.verdict, haystack: Lit(Array<String>)}}` —
        /// the shape `bp_dsl` `skip_on` emits.
        skip_on: std::collections::BTreeSet<String>,
        /// Every string appearing in a `Branch{cond = Eq/Ne{Path.verdict,
        /// Lit(String)}}` or an `Or{args = [Eq..., Eq...]}` of that shape
        /// — the shape `bp_dsl` `gate_cond` (halt_on) emits.
        halt_on: std::collections::BTreeSet<String>,
    }

    /// A `Path`'s string form ends at `.parts["verdict"]` or
    /// `.parts.verdict` (either bracket or dot notation is valid input
    /// to the flow-ir Path parser — the runtime accepts both).
    fn is_verdict_path(at: &mlua_flow_ir::Path) -> bool {
        let s = at.to_string();
        s.ends_with(".parts[\"verdict\"]") || s.ends_with(".parts.verdict")
    }

    fn collect_string_literals(v: &serde_json::Value, out: &mut Vec<String>) {
        match v {
            serde_json::Value::String(s) => out.push(s.clone()),
            serde_json::Value::Array(items) => {
                for it in items {
                    collect_string_literals(it, out);
                }
            }
            _ => {}
        }
    }

    fn walk_expr_for_halt(expr: &Expr, out: &mut GuardValues) {
        match expr {
            Expr::Eq { lhs, rhs } | Expr::Ne { lhs, rhs } => {
                let pair = match (lhs.as_ref(), rhs.as_ref()) {
                    (Expr::Path { at }, Expr::Lit { value }) => Some((at, value)),
                    (Expr::Lit { value }, Expr::Path { at }) => Some((at, value)),
                    _ => None,
                };
                if let Some((at, value)) = pair {
                    if is_verdict_path(at) {
                        let mut lits = Vec::new();
                        collect_string_literals(value, &mut lits);
                        for s in lits {
                            out.halt_on.insert(s);
                        }
                    }
                }
            }
            Expr::And { args } | Expr::Or { args } => {
                for a in args {
                    walk_expr_for_halt(a, out);
                }
            }
            Expr::Not { arg } => walk_expr_for_halt(arg, out),
            _ => {}
        }
    }

    fn walk_expr_for_skip(expr: &Expr, out: &mut GuardValues) {
        match expr {
            Expr::In { needle, haystack } => {
                if let (
                    Expr::Path { at },
                    Expr::Lit {
                        value: serde_json::Value::Array(items),
                    },
                ) = (needle.as_ref(), haystack.as_ref())
                {
                    if is_verdict_path(at) {
                        for it in items {
                            if let serde_json::Value::String(s) = it {
                                out.skip_on.insert(s.clone());
                            }
                        }
                    }
                }
            }
            Expr::And { args } | Expr::Or { args } => {
                for a in args {
                    walk_expr_for_skip(a, out);
                }
            }
            Expr::Not { arg } => walk_expr_for_skip(arg, out),
            _ => {}
        }
    }

    fn walk_node(node: &FlowNode, out: &mut GuardValues) {
        match node {
            FlowNode::Branch { cond, then_, else_ } => {
                // Distinguish skip guard vs halt guard by cond shape.
                // `In{Path.verdict, Lit[array]}` = skip_on (the shape
                // `bp_dsl` `skip_on` emits). `Eq/Ne{Path.verdict,
                // Lit}` or `Or{Eq...Eq...}` = halt_on (the shape
                // `gate_cond` emits). Both walkers are separate + skip
                // shapes they don't recognize, so a hand-authored flow
                // that combines both patterns still gets classified
                // correctly.
                walk_expr_for_skip(cond, out);
                walk_expr_for_halt(cond, out);
                walk_node(then_, out);
                walk_node(else_, out);
            }
            FlowNode::Loop { cond, body, .. } => {
                walk_expr_for_skip(cond, out);
                walk_expr_for_halt(cond, out);
                walk_node(body, out);
            }
            FlowNode::Seq { children } => {
                for c in children {
                    walk_node(c, out);
                }
            }
            FlowNode::Fanout { body, .. } => walk_node(body, out),
            FlowNode::Try { body, catch, .. } => {
                walk_node(body, out);
                walk_node(catch, out);
            }
            FlowNode::Step { .. } | FlowNode::Assign { .. } => {}
        }
    }

    /// Case-insensitive membership of a verdict value in the fixed
    /// skip-like pattern set.
    fn is_skip_like(value: &str) -> bool {
        matches!(
            value.to_ascii_uppercase().as_str(),
            "SKIP" | "NOT_APPLICABLE" | "N/A"
        )
    }

    let mut guards = GuardValues::default();
    walk_node(&bp.flow, &mut guards);

    // Every declared verdict value across every agent, with the
    // owning agent for message hints.
    let mut declared_values: Vec<(String, String)> = Vec::new();
    for agent in &bp.agents {
        if let Some(contract) = &agent.verdict {
            for v in &contract.values {
                declared_values.push((agent.name.clone(), v.clone()));
            }
        }
    }

    let declared_value_set: std::collections::BTreeSet<&str> =
        declared_values.iter().map(|(_, v)| v.as_str()).collect();

    let mut findings: Vec<serde_json::Value> = Vec::new();

    // Check 1 — skip_on_missing_for_skip_like_verdict_value (WARN):
    // agent declares a skip-like verdict value, but no `skip_on` list
    // in the flow captures that value. One finding per (agent, value)
    // pair so a Blueprint declaring the pattern across N agents surfaces
    // the full list, not just the first hit.
    for (agent, value) in &declared_values {
        if is_skip_like(value) && !guards.skip_on.contains(value) {
            findings.push(serde_json::json!({
                "check": "skip_on_missing_for_skip_like_verdict_value",
                "severity": "WARN",
                "agent": agent,
                "value": value,
                "message": format!(
                    "agent '{agent}' declares verdict.values entry '{value}' which \
                     reads like a Skip signal (SKIP / NOT_APPLICABLE / N/A, \
                     case-insensitive), but no Branch in the flow uses it in a \
                     `skip_on = {{ \"{value}\" }}` list. Add `skip_on = {{ \"{value}\" \
                     }}` on the downstream stage that should opt out on this \
                     verdict, or remove the value from the agent's verdict.values \
                     if it is stale."
                ),
            }));
        }
    }

    // Check 2 — skip_on_declared_but_no_matching_verdict_value (WARN):
    // a `skip_on` list captures a value that no agent's verdict.values
    // declares. Dead guard.
    for skip_value in &guards.skip_on {
        if !declared_value_set.contains(skip_value.as_str()) {
            findings.push(serde_json::json!({
                "check": "skip_on_declared_but_no_matching_verdict_value",
                "severity": "WARN",
                "value": skip_value,
                "message": format!(
                    "a `skip_on` list captures the value '{skip_value}', but no agent's \
                     verdict.values declares it; the guard can never fire (dead branch). \
                     Either add '{skip_value}' to an upstream agent's verdict.values, or \
                     remove it from the skip_on list."
                ),
            }));
        }
    }

    // Check 3 — skip_on_pattern_conflicts_with_halt_on (WARN): the
    // same value appears in both a skip_on list and a halt_on cond.
    // Overlap.
    for value in guards.skip_on.intersection(&guards.halt_on) {
        findings.push(serde_json::json!({
            "check": "skip_on_pattern_conflicts_with_halt_on",
            "severity": "WARN",
            "value": value,
            "message": format!(
                "value '{value}' appears in both a `skip_on` list and a `halt_on` \
                 (gate) cond in the flow. Only one of the two guards fires on this \
                 verdict at run time — the overlap is at best redundant, at worst a \
                 logic bug. Split the verdict values across skip_on / halt_on so \
                 each value routes through exactly one guard."
            ),
        }));
    }

    serde_json::json!({ "findings": findings })
}

/// Tools every mse-worker wrapper carries regardless of author intent
/// (the fetch/submit contract) — every wrapper gets `mse_worker_fetch` /
/// `mse_worker_submit` whether or not the agent's wrapper author
/// deliberately reached for them, so surfacing them under
/// `wrapper_only_meaningful` would just be noise. Wrappers list these in
/// their frontmatter as the full MCP tool identifiers (as reported by
/// the client that grants them), which is what the drift comparison sees
/// and what this allow-list must therefore match.
const WRAPPER_ONLY_CONTRACT_TOOLS: &[&str] =
    &["mcp__mse__mse_worker_fetch", "mcp__mse__mse_worker_submit"];

/// GH #78 Layer 1 `context_policy_lint` family: Blueprint-scoped advisory
/// checks that front-load the "silent `file_path: null`" failure — a
/// Blueprint registers and launches with 200s, but
/// `resolve_materialized_file`'s root resolution
/// (`ProjectionPlacement::resolve_root`) silently returns `None` at read
/// time because neither `work_dir` nor `project_root` survives into the
/// step's `AgentContextView`. Report-only (WARN — never BLOCK), matching
/// the sibling `binding_lint` / `skip_on_lint` posture.
///
/// Two checks:
///
/// - `context_policy_strips_projection_roots` (WARN, static): an agent's
///   effective [`mlua_swarm_schema::ContextPolicy`]
///   (`AgentMeta.context_policy`, else `Blueprint.default_context_policy`)
///   filters out BOTH `"work_dir"` AND `"project_root"`. Root resolution
///   is then guaranteed to fail whatever the launch seeds — every step
///   artifact for that agent materializes `content_url`-only. A policy
///   that strips only one root field is not flagged (the other still
///   resolves via `RootPreference`'s fallback).
/// - `projection_root_seed_missing` (WARN, simulation): evaluated only
///   when the caller passes `simulated_launch` (an object whose
///   `"project_root"` / `"work_dir"` string fields mirror the canonical
///   launch-time seed). Flags every agent for which no seeded field
///   survives its effective policy — the exact GH #78 P1a repro
///   (`projection_placement.root = "project_root"` declared, launch omits
///   `project_root`), evaluated before any real work is dispatched.
///
/// A Blueprint that declares no `context_policy` anywhere and is checked
/// without `simulated_launch` produces no findings (OK) — the family
/// stays silent for the pre-#20/#21 Blueprint population.
///
/// Pure over the resolved Blueprint (no I/O), unit-testable.
fn classify_context_policy_lint(
    bp: &Blueprint,
    simulated_launch: Option<&serde_json::Value>,
) -> serde_json::Value {
    const ROOT_FIELDS: [&str; 2] = ["work_dir", "project_root"];

    // The declared placement preference, for the finding's hint field —
    // resolution failure is both-absent regardless of preference, but
    // naming the declared preference points the author at the field the
    // BP most likely intended to seed.
    let root_preference: &str = bp
        .projection_placement
        .as_ref()
        .and_then(|spec| spec.root.as_deref())
        .unwrap_or("work_dir");

    let mut findings: Vec<serde_json::Value> = Vec::new();
    for agent in &bp.agents {
        let (policy, policy_source): (Option<&mlua_swarm_schema::ContextPolicy>, &str) =
            match agent.meta.as_ref().and_then(|m| m.context_policy.as_ref()) {
                Some(p) => (Some(p), "agent"),
                None => match bp.default_context_policy.as_ref() {
                    Some(p) => (Some(p), "bp-global"),
                    None => (None, "none"),
                },
            };

        // Check 1 — static: the effective policy strips both root fields.
        if let Some(p) = policy {
            let survives_any = ROOT_FIELDS.iter().any(|f| p.allows(f));
            if !survives_any {
                findings.push(serde_json::json!({
                    "check": "context_policy_strips_projection_roots",
                    "severity": "WARN",
                    "agent": agent.name,
                    "policy_source": policy_source,
                    "message": format!(
                        "agent '{}' effective context_policy ({policy_source} tier) filters \
                         out both 'work_dir' and 'project_root'; the projection placement \
                         root can never resolve, so this agent's step artifacts will \
                         materialize with file_path: null (content_url-only). Allow at \
                         least one root field, or drop the projection expectation \
                         downstream.",
                        agent.name
                    ),
                }));
            }
        }

        // Check 2 — simulation: no seeded field survives the policy.
        if let Some(sim) = simulated_launch {
            let seeded_and_surviving = ROOT_FIELDS
                .iter()
                .filter(|f| {
                    sim.get(**f)
                        .and_then(|v| v.as_str())
                        .is_some_and(|s| !s.is_empty())
                })
                .any(|f| policy.map_or(true, |p| p.allows(f)));
            if !seeded_and_surviving {
                // Name the alternate root field so the "seed X or Y" hint
                // never repeats the preferred field. `ROOT_FIELDS` has
                // exactly two entries, so the alternate is deterministic
                // given `root_preference`.
                let alternate_root = if root_preference == "work_dir" {
                    "project_root"
                } else {
                    "work_dir"
                };
                findings.push(serde_json::json!({
                    "check": "projection_root_seed_missing",
                    "severity": "WARN",
                    "agent": agent.name,
                    "policy_source": policy_source,
                    "root_preference": root_preference,
                    "message": format!(
                        "under the simulated launch payload, agent '{}' has no \
                         policy-surviving root seed (declared preference: \
                         '{root_preference}'); step artifacts will materialize with \
                         file_path: null (content_url-only). Seed '{root_preference}' \
                         (or '{alternate_root}' as the fallback) in the launch \
                         request's canonical fields, or via Blueprint defaults.",
                        agent.name
                    ),
                }));
            }
        }
    }
    serde_json::json!({ "findings": findings })
}

/// `verdict_contract_lint` family: Blueprint-scoped, report-only surface
/// for the reverse-direction verdict lint — "a declared
/// `agents[].verdict.values` entry that no downstream `Branch`/`Loop`
/// `cond` ever compares against".
///
/// One check, `verdict_value_unhandled` (WARN). The producer is
/// [`mlua_swarm::unhandled_verdict_values`] — the same fold the compile
/// gate runs, so this family cannot drift from `Compiler::compile`'s view
/// of the flow.
///
/// **Why a report-only surface for a check that already exists.** The
/// compile gate only turns these findings into a hard error under
/// `metadata.strict_verdict_handling`; on the default path they are a
/// `tracing::warn!` and therefore invisible to `bp_build` / `bp_doctor`
/// callers — the check ran, found the problem, and told nobody the author
/// would ever read. That gap has a specific failure it lets through: a
/// Blueprint whose flow has no `Branch` at all still compiles with a
/// `channel: "body"` contract on every gate, and `channel: "body"`
/// additionally constrains the terminal OUTPUT *value* to be one of the
/// declared tokens. An in-process gate that returns its report as the body
/// then has its `Final` rejected at completion time, and the attempt dies
/// with a missing-Final symptom far from the declaration that caused it.
/// Surfacing the unread contract at authoring time is the earliest point
/// that mistake is visible.
///
/// A declared contract WITH a matching cond produces no finding, so the
/// family stays silent for Blueprints that use verdicts as intended.
/// Blueprints that intentionally declare informational tokens nothing
/// branches on (the back-compat population `strict_verdict_handling`
/// exists for) will see a WARN — that is the same trade the compile-side
/// `tracing::warn!` already makes, now visible.
///
/// Pure over the resolved Blueprint (no I/O), unit-testable.
fn classify_verdict_contract_lint(bp: &Blueprint) -> serde_json::Value {
    let contracts: std::collections::HashMap<String, mlua_swarm_schema::VerdictContract> = bp
        .agents
        .iter()
        .filter_map(|a| a.verdict.clone().map(|v| (a.name.clone(), v)))
        .collect();

    let channel_of = |agent: &str| match contracts.get(agent).map(|c| c.channel) {
        Some(mlua_swarm_schema::VerdictChannel::Part) => "part",
        _ => "body",
    };
    // Body-channel `channel_note` sentence, shared between the per-agent
    // aggregate and the per-value findings: it is the case where an unread
    // contract also constrains the step's terminal OUTPUT shape, so the
    // author needs to know the declaration is not merely decorative.
    let body_channel_note = " Note that `channel: \"body\"` also requires this step's terminal \
                              OUTPUT value to BE one of the declared tokens — a step that \
                              returns a report body will have its Final rejected at completion \
                              time. Use `channel: \"part\"` to keep the body free for a report.";

    // Per-agent aggregate first — the "whole gate is dead" reading. Placed
    // before the per-value findings so a reader scanning findings[] sees
    // the fully-decorative agents before the per-value baseline (a normal
    // halt gate always leaks one per-value finding per agent for the
    // always-unread PASS token; the aggregate lets the reader tell "1
    // gate dropped" from "1 baseline PASS").
    let mut findings: Vec<serde_json::Value> =
        mlua_swarm::agents_with_all_verdict_values_unread(&bp.flow, &contracts)
            .into_iter()
            .map(|f| {
                let channel = channel_of(&f.agent);
                let channel_note = if channel == "body" {
                    body_channel_note
                } else {
                    ""
                };
                serde_json::json!({
                    "check": "verdict_contract_never_read",
                    "severity": "WARN",
                    "agent": f.agent,
                    "channel": channel,
                    "declared_values": f.declared_values,
                    "step_ref": f.step_ref,
                    "message": format!(
                        "agent '{}' declares verdict values {:?} (channel: {channel}) at step \
                         '{}', but no downstream Branch/Loop cond reads any of them — the \
                         contract is decorative and this step cannot halt the flow. Add a gate \
                         that reads the verdict (e.g. `gate = true` on the B.pipeline stage), \
                         or drop the verdict declaration.{channel_note}",
                        f.agent, f.declared_values, f.step_ref,
                    ),
                })
            })
            .collect();

    findings.extend(
        mlua_swarm::unhandled_verdict_values(&bp.flow, &contracts)
            .into_iter()
            .map(|f| {
                let channel = channel_of(&f.agent);
                let channel_note = if channel == "body" {
                    body_channel_note
                } else {
                    ""
                };
                serde_json::json!({
                    "check": "verdict_value_unhandled",
                    "severity": "WARN",
                    "agent": f.agent,
                    "value": f.value,
                    "channel": channel,
                    "declared_values": f.declared_values,
                    "step_ref": f.step_ref,
                    "message": format!(
                        "agent '{}' declares verdict value {:?} (channel: {channel}) at step \
                         '{}', but no downstream Branch/Loop cond ever compares against it. \
                         Either add a cond that handles it, or drop the value from \
                         verdict.values.{channel_note}",
                        f.agent, f.value, f.step_ref,
                    ),
                })
            }),
    );

    serde_json::json!({ "findings": findings })
}

/// `spawner_hint_lint` family: Blueprint-scoped, report-only surface for
/// `spawner_hints.layers` keys naming a layer the engine has withdrawn.
///
/// One check, `removed_spawner_hint` (WARN here; the compile stage
/// refuses the same kind as `Error`).
///
/// **Why this needs a `bp_doctor` surface at all when compile already
/// refuses.** Registering a Blueprint does not compile it —
/// `blueprints.rs` stores the document, and `Compiler::compile` first
/// runs at launch. So a Blueprint carrying a removed hint registers
/// cleanly, passes a `bp_doctor` run that has no producer for it, and
/// then dies on its first dispatch, at which point the author is reading
/// a launch failure rather than an authoring one. That is the same gap
/// `worker_binding_lint` was added to close ("the same fail-loud
/// condition `Compiler::compile` enforces at dispatch, retroactively
/// surfaced on already-registered Blueprints"), and it is worse here: the
/// hint is inert wiring rather than a missing field, so nothing else in
/// the doctor report hints that the Blueprint is unlaunchable.
///
/// The severity split is deliberate and matches the model's per-stage
/// levels: the compile stage refuses because continuing would run a
/// different execution shape than the author wrote, while `bp_doctor` is
/// report-only by construction and its verdict is a label, so it reports.
///
/// The removed-key table itself lives in
/// [`mlua_swarm::removed_spawner_hint_reason`], shared with the compile
/// gate so the two stages cannot disagree about which keys are dead.
///
/// Pure over the resolved Blueprint (no I/O), unit-testable.
fn classify_spawner_hint_lint(bp: &Blueprint) -> serde_json::Value {
    let findings: Vec<serde_json::Value> = bp
        .spawner_hints
        .layers
        .iter()
        .filter_map(|key| {
            let reason = mlua_swarm::removed_spawner_hint_reason(key)?;
            Some(serde_json::json!({
                "check": "removed_spawner_hint",
                "severity": "WARN",
                "layer": key,
                "message": format!(
                    "spawner_hints.layers declares '{key}', but that layer has been removed: \
                     {reason}. This Blueprint is registered and will fail to compile on its \
                     next dispatch. Drop the key and route through the AgentSpec axis instead \
                     — declare the seat in `operators[]`, point each operator-backed agent at \
                     it with `spec.operator_ref`, and name the seat's holder per launch with \
                     `operator_sid` (a later handover then moves the destination without \
                     recompiling anything)."
                ),
            }))
        })
        .collect();

    serde_json::json!({ "findings": findings })
}

// ─── GH #79 Phase 3: classify_* → Diagnostic siblings ─────────────────────
//
// Each `classify_*` family gains a sibling that projects its JSON
// verdict into the unified `mlua_swarm_diag::Diagnostic` shape. The
// siblings consume the classifier *output* rather than re-running the
// family logic — one logic source, one projection — and only emit a
// Diagnostic for findings (an `OK` verdict projects to nothing). The
// old family-specific response fields stay in place (additive, minor
// bump); Phase 4 retires them.

/// Map a `bp_doctor` severity string to the unified level. `OK` never
/// reaches this fn (callers skip OK verdicts before projecting).
fn diag_level_from_severity(severity: &str) -> mlua_swarm_diag::DiagLevel {
    match severity {
        "BLOCK" => mlua_swarm_diag::DiagLevel::Error,
        "INFO" => mlua_swarm_diag::DiagLevel::Info,
        _ => mlua_swarm_diag::DiagLevel::Warn,
    }
}

/// Shorthand: an `agents[]` span for a named agent.
fn diag_agent_span(agent: &str) -> mlua_swarm_diag::DiagSpan {
    mlua_swarm_diag::DiagSpan {
        element: mlua_swarm_diag::DiagElement::Agent {
            name: agent.to_string(),
        },
        json_path: Some(format!("$.agents[?(@.name=='{agent}')]")),
    }
}

/// Sibling of [`classify_agent_md_severity`]: project one agent's size
/// verdict. `OK` → `None`.
fn diag_from_agent_md(
    agent: &str,
    severity: &str,
    bytes: usize,
    lines: usize,
) -> Option<mlua_swarm_diag::Diagnostic> {
    use mlua_swarm_diag::{BpDoctorFamily, DiagStage, Diagnostic, DocsRef};
    if severity == "OK" {
        return None;
    }
    Some(
        Diagnostic::new(
            "agent-md-size",
            DiagStage::BpDoctor {
                family: BpDoctorFamily::AgentMdSize,
            },
            diag_level_from_severity(severity),
            format!(
                "agent '{agent}' system_prompt is {bytes} bytes / {lines} lines — over the \
                 authoring-guide size target"
            ),
        )
        .with_docs_ref(DocsRef {
            uri: "mse://guides/agent-md-authoring",
            anchor: None,
        })
        .with_span(diag_agent_span(agent)),
    )
}

/// Sibling of [`classify_tool_lint`]: project one agent's phantom-tool
/// verdict. `OK` → `None`.
fn diag_from_tool_lint(
    agent: &str,
    verdict: &serde_json::Value,
) -> Option<mlua_swarm_diag::Diagnostic> {
    use mlua_swarm_diag::{BpDoctorFamily, DiagStage, Diagnostic, DocsRef};
    let severity = verdict.get("severity")?.as_str()?;
    if severity == "OK" {
        return None;
    }
    let unknown: Vec<String> = verdict
        .get("unknown_tools")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|t| t.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let mut d = Diagnostic::new(
        "tool-unknown-mcp-ref",
        DiagStage::BpDoctor {
            family: BpDoctorFamily::ToolLint,
        },
        diag_level_from_severity(severity),
        format!(
            "agent '{agent}' profile.tools references {} mcp__mse__ tool(s) absent from the \
             live registry",
            unknown.len()
        ),
    )
    .with_docs_ref(DocsRef {
        uri: "mse://guides/mcp-tool-reference",
        anchor: None,
    })
    .with_span(diag_agent_span(agent));
    for tool in unknown {
        d = d.with_note(format!("unknown tool: {tool}"));
    }
    Some(d)
}

/// Sibling of [`classify_output_contract_lint`]: project one agent's
/// output-contract verdict. `OK` → `None`.
fn diag_from_output_contract_lint(
    agent: &str,
    verdict: &serde_json::Value,
) -> Option<mlua_swarm_diag::Diagnostic> {
    use mlua_swarm_diag::{BpDoctorFamily, DiagStage, Diagnostic, DocsRef};
    let severity = verdict.get("severity")?.as_str()?;
    if severity == "OK" {
        return None;
    }
    let reason = verdict
        .get("reason")
        .and_then(|v| v.as_str())
        .unwrap_or("expected_output declaration missing or malformed");
    Some(
        Diagnostic::new(
            "output-contract-missing",
            DiagStage::BpDoctor {
                family: BpDoctorFamily::OutputContractLint,
            },
            diag_level_from_severity(severity),
            format!("agent '{agent}': {reason}"),
        )
        .with_docs_ref(DocsRef {
            uri: "mse://guides/agent-md-authoring",
            anchor: None,
        })
        .with_span(diag_agent_span(agent)),
    )
}

/// Sibling of [`classify_worker_binding_lint`]: project one agent's
/// worker-binding verdict. `OK` → `None`.
///
/// Same lint kind (`worker-binding-missing`) the compile stage emits as
/// `Error` via `From<&CompileError>` — here at `Warn` (report-only),
/// realizing the GH #79 dual-stage story: one `LintDecl`, one docs
/// anchor, one downstream switch key.
fn diag_from_worker_binding_lint(
    agent: &str,
    verdict: &serde_json::Value,
) -> Option<mlua_swarm_diag::Diagnostic> {
    use mlua_swarm_diag::{
        Applicability, BpDoctorFamily, DiagStage, Diagnostic, DocsRef, Suggestion,
    };
    let severity = verdict.get("severity")?.as_str()?;
    if severity == "OK" {
        return None;
    }
    let mut d = Diagnostic::new(
        "worker-binding-missing",
        DiagStage::BpDoctor {
            family: BpDoctorFamily::WorkerBindingLint,
        },
        diag_level_from_severity(severity),
        format!("operator agent '{agent}' lacks worker_binding"),
    )
    .with_help(
        "the WS thin-path operator backend requires a Runner (or legacy \
         profile.worker_binding) at dispatch",
    )
    .with_suggestion(Suggestion {
        msg: "add an explicit Runner (or legacy profile.worker_binding)".into(),
        patch: "runner = { backend = \"ws_operator\", variant = \"claude\", tools = {} }".into(),
        applicability: Applicability::HasPlaceholders,
    })
    .with_docs_ref(DocsRef {
        uri: "mse://guides/bp-dsl-templates",
        anchor: None,
    })
    .with_span(mlua_swarm_diag::DiagSpan {
        element: mlua_swarm_diag::DiagElement::Agent {
            name: agent.to_string(),
        },
        json_path: Some(format!(
            "$.agents[?(@.name=='{agent}')].profile.worker_binding"
        )),
    });
    if let Some(reason) = verdict.get("reason").and_then(|v| v.as_str()) {
        d = d.with_note(reason.to_string());
    }
    Some(d)
}

/// Sibling of [`classify_binding_lint`] / [`classify_skip_on_lint`]:
/// project a Blueprint-scoped `{"findings": [...]}` verdict. Shared
/// walker — the two families' findings carry the same
/// `check` / `severity` / `message` / optional `agent` shape; `check`
/// keys map to the kebab-case registry kinds.
///
/// The tool itself walks the array through
/// [`apply_findings_family_lints`] instead (a lint-suppressed finding has
/// to leave that same array, which a project-only pass cannot express);
/// this whole-verdict form remains as the family-level projection the
/// per-family tests assert against.
#[cfg(test)]
fn diag_from_findings(
    family: mlua_swarm_diag::BpDoctorFamily,
    verdict: &serde_json::Value,
) -> Vec<mlua_swarm_diag::Diagnostic> {
    let Some(findings) = verdict.get("findings").and_then(|f| f.as_array()) else {
        return Vec::new();
    };
    findings
        .iter()
        .filter_map(|finding| diag_from_finding(family, finding))
        .collect()
}

/// Per-entry half of [`diag_from_findings`] — projects a single
/// `{"check", "severity", "message", "agent"?}` finding. Split out so the
/// lint-level resolution can walk the `findings` array positionally (a
/// suppressed finding has to be removed from that same array, which the
/// vector-returning form cannot express). `None` = the check has no
/// registry kind yet.
fn diag_from_finding(
    family: mlua_swarm_diag::BpDoctorFamily,
    finding: &serde_json::Value,
) -> Option<mlua_swarm_diag::Diagnostic> {
    use mlua_swarm_diag::{
        Applicability, DiagElement, DiagSpan, DiagStage, Diagnostic, DocsRef, Suggestion,
    };
    let docs_uri = match family {
        mlua_swarm_diag::BpDoctorFamily::SkipOnLint => "mse://guides/skip-tier-and-skip-on",
        mlua_swarm_diag::BpDoctorFamily::VerdictContractLint => "mse://guides/blueprint-authoring",
        mlua_swarm_diag::BpDoctorFamily::SpawnerHintLint => "mse://guides/blueprint-authoring",
        _ => "mse://guides/operator-execution-model",
    };
    let check = finding.get("check")?.as_str()?;
    let kind: &'static str = match check {
        "binding_requirements_info" => "binding-requirements-info",
        "strict_binding_without_runners" => "strict-binding-without-runners",
        "legacy_worker_binding" => "legacy-worker-binding",
        "binding_resolution_error" => "binding-resolution-error",
        "skip_on_missing_for_skip_like_verdict_value" => {
            "skip-on-missing-for-skip-like-verdict-value"
        }
        "skip_on_declared_but_no_matching_verdict_value" => {
            "skip-on-declared-but-no-matching-verdict-value"
        }
        "skip_on_pattern_conflicts_with_halt_on" => "skip-on-pattern-conflicts-with-halt-on",
        "context_policy_strips_projection_roots" => "context-policy-strips-projection-roots",
        "projection_root_seed_missing" => "projection-root-seed-missing",
        "verdict_value_unhandled" => "verdict-value-unhandled",
        "verdict_contract_never_read" => "verdict-contract-never-read",
        "removed_spawner_hint" => "removed-spawner-hint",
        // A future check without a registry kind is skipped
        // rather than emitted with an undeclared kind — the
        // old `findings` field still carries it verbatim.
        _ => return None,
    };
    let severity = finding
        .get("severity")
        .and_then(|s| s.as_str())
        .unwrap_or("WARN");
    let message = finding
        .get("message")
        .and_then(|m| m.as_str())
        .unwrap_or(check)
        .to_string();
    let span = match finding.get("agent").and_then(|a| a.as_str()) {
        Some(agent) => diag_agent_span(agent),
        None => DiagSpan {
            element: DiagElement::BlueprintRoot,
            json_path: None,
        },
    };
    let mut d = Diagnostic::new(
        kind,
        DiagStage::BpDoctor { family },
        diag_level_from_severity(severity),
        message,
    )
    .with_docs_ref(DocsRef {
        uri: docs_uri,
        anchor: None,
    })
    .with_span(span);
    // The per-agent "whole gate is dead" finding carries a
    // concrete recovery: opt into a gate at the B.pipeline
    // stage. `MaybeIncorrect` because the fix presumes the
    // Blueprint was authored via `B.pipeline` (the common
    // path); a hand-rolled flow needs the equivalent Branch
    // shape by hand.
    if kind == "verdict-contract-never-read" {
        d = d.with_suggestion(Suggestion {
            msg: "opt the stage into gate emission (bafe47d4 opt-in flip)".into(),
            patch: "gate = true,".into(),
            applicability: Applicability::MaybeIncorrect,
        });
    }
    // Same lint, same recovery as the compile stage's arm in
    // `impl From<&CompileError> for Diagnostic`. Both call the one
    // constructor rather than each spelling the prose out: an author who
    // meets this kind at either stage must not be told two different
    // things, and the two arms are in different crates, so nothing local
    // to either would notice them drifting apart.
    if kind == "removed-spawner-hint" {
        d = d.with_suggestion(mlua_swarm_diag::removed_spawner_hint_suggestion());
    }
    Some(d)
}

// ─── Lint level control: allow / warn / deny over three layers ────────────
//
// `mlua_swarm_diag::resolve_level` owns precedence (call-site > agent >
// Blueprint, first layer with any matching key wins outright). This
// section owns the `bp_doctor` stage contract: which kinds the stage
// emits, how an allowed-away finding stays visible on the wire, and the
// two meta-lints about the lints config itself.

/// Every lint kind `bp_doctor` can emit, grouped by producing family —
/// keep in sync with the `diag_from_*` helpers above.
///
/// This set is the stage half of the non-suppressible boundary. The
/// boundary is stage-scoped, not kind-scoped: a compile hard error
/// ([`mlua_swarm_diag::is_compile_hard_error`]) that appears here stays
/// suppressible at `bp_doctor`, because this stage emits it at `Warn`
/// (`worker-binding-missing`, `verdict-value-unhandled` — the legacy
/// `disable_*_lint` flags already suppress them here). An exact-kind
/// `allow` / `warn` on a compile hard error *absent* from this set can
/// have no effect at any stage and raises `non-suppressible-lint`.
const BP_DOCTOR_EMITTED_KINDS: &[&str] = &[
    // agent.md size family — `diag_from_agent_md`
    "agent-md-size",
    // per-agent families — `diag_from_tool_lint` /
    // `diag_from_output_contract_lint` / `diag_from_worker_binding_lint`
    "tool-unknown-mcp-ref",
    "output-contract-missing",
    "worker-binding-missing",
    // Blueprint-scoped families — `diag_from_finding`
    "binding-requirements-info",
    "strict-binding-without-runners",
    "legacy-worker-binding",
    "binding-resolution-error",
    "skip-on-missing-for-skip-like-verdict-value",
    "skip-on-declared-but-no-matching-verdict-value",
    "skip-on-pattern-conflicts-with-halt-on",
    "context-policy-strips-projection-roots",
    "projection-root-seed-missing",
    "verdict-value-unhandled",
    "verdict-contract-never-read",
    // Dual-stage kind, like `worker-binding-missing`: `Error` at compile
    // (the compile refuses), `Warn` here (report-only). Listing it keeps
    // it suppressible at *this* stage — an author who has not migrated
    // yet can `allow` the doctor noise, and the compile still refuses.
    "removed-spawner-hint",
];

/// Inverse of [`diag_level_from_severity`]: the `bp_doctor` severity
/// label a resolved level reports as.
fn severity_from_diag_level(level: mlua_swarm_diag::DiagLevel) -> &'static str {
    match level {
        mlua_swarm_diag::DiagLevel::Error => "BLOCK",
        mlua_swarm_diag::DiagLevel::Warn => "WARN",
        mlua_swarm_diag::DiagLevel::Info => "INFO",
    }
}

/// Bridges the schema's author-facing enum onto the diag crate's twin —
/// the diag crate depends on no other mlua-swarm crate, so the consumer
/// maps one onto the other.
fn diag_lint_setting(setting: mlua_swarm_schema::LintSetting) -> mlua_swarm_diag::LintSetting {
    match setting {
        mlua_swarm_schema::LintSetting::Allow => mlua_swarm_diag::LintSetting::Allow,
        mlua_swarm_schema::LintSetting::Warn => mlua_swarm_diag::LintSetting::Warn,
        mlua_swarm_schema::LintSetting::Deny => mlua_swarm_diag::LintSetting::Deny,
    }
}

/// What the declared layers say about one finding.
enum LintOutcome {
    /// Allowed away by the named layer (`"call-site"` / `"agent:<name>"`
    /// / `"blueprint"`) — the finding moves to the response's
    /// `suppressed[]` array and folds into nothing.
    Suppressed {
        /// The layer whose key matched.
        source: String,
    },
    /// The finding fires at this level.
    Reported {
        /// The declared override, or the level the producing family
        /// emitted the finding at when no layer spoke.
        level: mlua_swarm_diag::DiagLevel,
    },
}

/// The three declared lint layers of one `bp_doctor` invocation.
struct LintLayers {
    /// `BpDoctorReq.lints` — top precedence, untyped (lenient) input.
    call_site: mlua_swarm_diag::LintConfig,
    /// `metadata.lints` — Blueprint-wide, lowest precedence.
    blueprint: mlua_swarm_diag::LintConfig,
    /// `agents[].lints`, keyed by agent name. Applies only to findings
    /// spanning that agent (or a step referencing it).
    per_agent: BTreeMap<String, mlua_swarm_diag::LintConfig>,
}

impl LintLayers {
    /// Build all three layers for one invocation. The call-site map is
    /// parsed leniently (an unknown key or an unparseable value becomes a
    /// meta-lint, never a rejected request); the two Blueprint-declared
    /// layers arrive already typed by serde and only need the key check.
    fn new(call_site: Option<&BTreeMap<String, String>>, bp: &Blueprint) -> Self {
        use mlua_swarm_diag::LintConfig;
        let to_pairs = |map: &BTreeMap<String, mlua_swarm_schema::LintSetting>| {
            LintConfig::from_pairs(
                map.iter()
                    .map(|(key, setting)| (key.clone(), diag_lint_setting(*setting)))
                    .collect::<Vec<_>>(),
            )
        };
        Self {
            call_site: call_site.map(LintConfig::from_str_map).unwrap_or_default(),
            blueprint: bp.metadata.lints.as_ref().map(to_pairs).unwrap_or_default(),
            per_agent: bp
                .agents
                .iter()
                .filter_map(|agent| Some((agent.name.clone(), to_pairs(agent.lints.as_ref()?))))
                .collect(),
        }
    }

    /// The layers applying to a finding scoped to `agent`, most specific
    /// first — the order [`mlua_swarm_diag::resolve_level`] documents.
    /// The per-agent layer is present only for a finding that spans that
    /// agent.
    fn layers_for(&self, agent: Option<&str>) -> Vec<(String, &mlua_swarm_diag::LintConfig)> {
        let mut layers: Vec<(String, &mlua_swarm_diag::LintConfig)> = Vec::with_capacity(3);
        layers.push(("call-site".to_string(), &self.call_site));
        if let Some((name, cfg)) =
            agent.and_then(|name| self.per_agent.get(name).map(|cfg| (name, cfg)))
        {
            layers.push((format!("agent:{name}"), cfg));
        }
        layers.push(("blueprint".to_string(), &self.blueprint));
        layers
    }

    /// Every declared layer with its source label, for the meta-lints
    /// (which are about the config, not about any one finding).
    fn declared_layers(&self) -> Vec<(String, &mlua_swarm_diag::LintConfig)> {
        let mut layers = vec![("call-site".to_string(), &self.call_site)];
        layers.extend(
            self.per_agent
                .iter()
                .map(|(name, cfg)| (format!("agent:{name}"), cfg)),
        );
        layers.push(("blueprint".to_string(), &self.blueprint));
        layers
    }

    /// Resolve one finding of `kind`, scoped to `agent` when it has one.
    ///
    /// `stage_level` is the level the producing family emitted the
    /// finding at and is what survives when no layer declares anything:
    /// [`mlua_swarm_diag::resolve_level`]'s own fallback is the *registry
    /// default*, which for a dual-stage kind is the compile level
    /// (`Error` for `worker-binding-missing`) rather than the report-only
    /// level this stage uses. Probing for a declaring layer first also
    /// yields the `source` label a suppressed finding is reported under.
    fn resolve(
        &self,
        kind: &str,
        agent: Option<&str>,
        stage_level: mlua_swarm_diag::DiagLevel,
    ) -> LintOutcome {
        let Some(decl) = mlua_swarm_diag::lint_decl(kind) else {
            return LintOutcome::Reported { level: stage_level };
        };
        let layers = self.layers_for(agent);
        let Some(source) = layers
            .iter()
            .find(|(_, cfg)| cfg.setting_for(decl).is_some())
            .map(|(source, _)| source.clone())
        else {
            return LintOutcome::Reported { level: stage_level };
        };
        let configs: Vec<&mlua_swarm_diag::LintConfig> =
            layers.iter().map(|(_, cfg)| *cfg).collect();
        match mlua_swarm_diag::resolve_level(decl, &configs) {
            mlua_swarm_diag::ResolvedLint::Suppressed => LintOutcome::Suppressed { source },
            mlua_swarm_diag::ResolvedLint::Level(level) => LintOutcome::Reported { level },
        }
    }

    /// The meta-lints about the config itself, walked layer by layer:
    /// `unknown-lint-kind` (one per key no layer could honor — unknown
    /// key or unparseable value) and `non-suppressible-lint` (one per
    /// exact-kind `allow` / `warn` that can have no effect at any stage,
    /// see [`BP_DOCTOR_EMITTED_KINDS`]). `category:` / `all` keys never
    /// raise the latter: addressing whole sets is expected to cover kinds
    /// this stage never emits. Both fold as `Warn`.
    fn meta_diagnostics(&self) -> Vec<mlua_swarm_diag::Diagnostic> {
        let mut out = Vec::new();
        for (source, cfg) in self.declared_layers() {
            for key in cfg.unknown_keys() {
                out.push(meta_lint_diagnostic(
                    "unknown-lint-kind",
                    &source,
                    format!(
                        "lints key '{key}' matches no lint kind, no 'category:<cat>' group and is \
                         not 'all' (or its value is not allow/warn/deny) — the entry has no effect"
                    ),
                ));
            }
            for (key, setting) in cfg.entries() {
                if matches!(setting, mlua_swarm_diag::LintSetting::Deny) {
                    continue;
                }
                let Some(decl) = mlua_swarm_diag::lint_decl(key) else {
                    continue;
                };
                if mlua_swarm_diag::is_compile_hard_error(decl)
                    && !BP_DOCTOR_EMITTED_KINDS.contains(&decl.kind)
                {
                    out.push(meta_lint_diagnostic(
                        "non-suppressible-lint",
                        &source,
                        format!(
                            "lints key '{key}' targets a compile-stage hard error that bp_doctor \
                             never emits — the setting is ignored at every stage"
                        ),
                    ));
                }
            }
        }
        out
    }
}

/// One meta-lint diagnostic. Blueprint-scoped span: the finding is about
/// the `lints` declaration, not about any Blueprint element, and the
/// declaring layer is named in a note (`"declared by: agent:planner"`).
fn meta_lint_diagnostic(
    kind: &'static str,
    source: &str,
    message: String,
) -> mlua_swarm_diag::Diagnostic {
    use mlua_swarm_diag::{
        BpDoctorFamily, DiagElement, DiagLevel, DiagSpan, DiagStage, Diagnostic, DocsRef,
    };
    Diagnostic::new(
        kind,
        DiagStage::BpDoctor {
            family: BpDoctorFamily::LintControl,
        },
        DiagLevel::Warn,
        message,
    )
    .with_note(format!("declared by: {source}"))
    .with_docs_ref(DocsRef {
        uri: "mse://guides/lint-diagnostic-model",
        anchor: None,
    })
    .with_span(DiagSpan {
        element: DiagElement::BlueprintRoot,
        json_path: None,
    })
}

/// Route one built diagnostic through the declared layers. Returns it at
/// its resolved level, or `None` when a layer allowed it away — in which
/// case a `{kind, span, source, message}` record is appended to
/// `suppressed`, so an allowed finding stays visible (omitted ≠ passed).
fn resolve_diagnostic(
    layers: &LintLayers,
    suppressed: &mut Vec<JsonValue>,
    agent: Option<&str>,
    mut d: mlua_swarm_diag::Diagnostic,
) -> Option<mlua_swarm_diag::Diagnostic> {
    match layers.resolve(d.kind, agent, d.level) {
        LintOutcome::Suppressed { source } => {
            suppressed.push(serde_json::json!({
                "kind": d.kind,
                "span": d.span,
                "source": source,
                "message": d.message,
            }));
            None
        }
        LintOutcome::Reported { level } => {
            d.level = level;
            Some(d)
        }
    }
}

/// Resolve one per-agent family verdict object (`tool_lint` /
/// `output_contract_lint` / `worker_binding_lint`) in place.
///
/// The object stays on the agent entry either way — its measurements are
/// still true — but its `severity` reports the resolved outcome: `OK`
/// when a layer allowed the finding away (the finding itself moves to
/// `suppressed[]`), `BLOCK` under `deny`. `built = None` means the family
/// verdict was already `OK` and there is nothing to resolve.
fn apply_agent_family_lints(
    verdict: &mut JsonValue,
    agent: &str,
    built: Option<mlua_swarm_diag::Diagnostic>,
    layers: &LintLayers,
    suppressed: &mut Vec<JsonValue>,
    diagnostics: &mut Vec<mlua_swarm_diag::Diagnostic>,
) {
    let Some(built) = built else {
        return;
    };
    let severity = match resolve_diagnostic(layers, suppressed, Some(agent), built) {
        Some(d) => {
            let severity = severity_from_diag_level(d.level);
            diagnostics.push(d);
            severity
        }
        None => "OK",
    };
    if let Some(obj) = verdict.as_object_mut() {
        obj.insert("severity".to_string(), JsonValue::from(severity));
    }
}

/// Resolve one Blueprint-scoped `{"findings": [...]}` family verdict in
/// place: allowed findings are removed from the array (and recorded in
/// `suppressed`), the rest carry their resolved severity label and feed
/// `diagnostics`. A finding whose `check` has no registry kind yet is
/// carried verbatim — no kind, no lint control.
///
/// Returns the retained severities in array order, for the aggregate
/// verdict fold.
fn apply_findings_family_lints(
    family: mlua_swarm_diag::BpDoctorFamily,
    verdict: &mut JsonValue,
    layers: &LintLayers,
    suppressed: &mut Vec<JsonValue>,
    diagnostics: &mut Vec<mlua_swarm_diag::Diagnostic>,
) -> Vec<String> {
    let Some(findings) = verdict.get_mut("findings").and_then(|f| f.as_array_mut()) else {
        return Vec::new();
    };
    let mut severities = Vec::with_capacity(findings.len());
    let mut retained = Vec::with_capacity(findings.len());
    for mut finding in std::mem::take(findings) {
        let Some(built) = diag_from_finding(family, &finding) else {
            if let Some(sev) = finding.get("severity").and_then(|s| s.as_str()) {
                severities.push(sev.to_string());
            }
            retained.push(finding);
            continue;
        };
        let agent = finding
            .get("agent")
            .and_then(|a| a.as_str())
            .map(String::from);
        if let Some(d) = resolve_diagnostic(layers, suppressed, agent.as_deref(), built) {
            let severity = severity_from_diag_level(d.level);
            if let Some(obj) = finding.as_object_mut() {
                obj.insert("severity".to_string(), JsonValue::from(severity));
            }
            severities.push(severity.to_string());
            diagnostics.push(d);
            retained.push(finding);
        }
    }
    *findings = retained;
    severities
}

/// Builds the [`WRAPPER_ONLY_CONTRACT_TOOLS`] allow-list as a `BTreeSet`,
/// for [`classify_wrapper_only`]'s `contract` parameter.
fn wrapper_only_contract_set() -> std::collections::BTreeSet<String> {
    WRAPPER_ONLY_CONTRACT_TOOLS
        .iter()
        .map(|s| s.to_string())
        .collect()
}

/// Splits a [`ToolDrift::wrapper_only`] list into the mse-worker
/// fetch/submit `contract` subset and everything else (`meaningful`) — the
/// contract tools are present on every wrapper regardless of author
/// intent, so surfacing them as "unexpected" wrapper tools is noise;
/// `meaningful` is the actionable subset. Pure, unit-testable (mirrors
/// [`diff_tools`]'s convention). Both outputs sorted + deduped (backed by
/// `BTreeSet`).
fn classify_wrapper_only(
    wrapper_only: &[String],
    contract: &std::collections::BTreeSet<String>,
) -> (Vec<String>, Vec<String>) {
    use std::collections::BTreeSet;
    let wrapper_only_set: BTreeSet<&String> = wrapper_only.iter().collect();
    let contract_out: Vec<String> = wrapper_only_set
        .iter()
        .filter(|s| contract.contains(s.as_str()))
        .map(|s| (*s).clone())
        .collect();
    let meaningful_out: Vec<String> = wrapper_only_set
        .iter()
        .filter(|s| !contract.contains(s.as_str()))
        .map(|s| (*s).clone())
        .collect();
    (contract_out, meaningful_out)
}

/// Reads and parses a worker wrapper `.md` file at
/// `{wrapper_dir}/{variant}.md` via `agent_md_loader::parse`. Shared by
/// `bp_explain_agent` (single-agent) and `bp_explain_agents` (batch) — the
/// wrapper-loading side of the drift check is identical for both; only
/// what each tool does with the parsed `AgentDef.profile.tools` differs.
fn load_wrapper_tools(wrapper_dir: &str, variant: &str) -> Result<Vec<String>, String> {
    let wrapper_path = format!("{wrapper_dir}/{variant}.md");
    let text =
        std::fs::read_to_string(&wrapper_path).map_err(|e| format!("read {wrapper_path}: {e}"))?;
    let def = mlua_swarm::lua::agent_md_loader::parse(
        &text,
        &wrapper_path,
        mlua_swarm::blueprint::AgentKind::Operator,
    )
    .map_err(|e| format!("parse {wrapper_path}: {e}"))?;
    Ok(def.profile.map(|p| p.tools).unwrap_or_default())
}

/// Serializes a computed [`ToolDrift`] and augments the JSON with the
/// `wrapper_only` classifier split (`wrapper_only_contract` /
/// `wrapper_only_meaningful`, via [`classify_wrapper_only`]). `wrapper_only`
/// (flat) itself is retained unmodified alongside the two new fields for
/// one release cycle — it may be removed in a later release.
fn tool_drift_json_with_wrapper_only_split(
    drift: &ToolDrift,
    contract: &std::collections::BTreeSet<String>,
) -> JsonValue {
    let (wrapper_only_contract, wrapper_only_meaningful) =
        classify_wrapper_only(&drift.wrapper_only, contract);
    let mut value = serde_json::to_value(drift).unwrap_or(JsonValue::Null);
    if let Some(obj) = value.as_object_mut() {
        obj.insert(
            "wrapper_only_contract".to_string(),
            serde_json::json!(wrapper_only_contract),
        );
        obj.insert(
            "wrapper_only_meaningful".to_string(),
            serde_json::json!(wrapper_only_meaningful),
        );
    }
    value
}

#[derive(Deserialize, JsonSchema)]
struct ServerStartReq {
    /// Where to healthz-poll after `launchctl kickstart`: a base URL
    /// (`https://host`, scheme included) or a bare `host:port` (which gets
    /// `http://`). Omitted falls back to `MSE_HTTP`, then to
    /// `http://127.0.0.1:7777`. Starting a daemon only makes sense for a
    /// local endpoint — a remote one has nothing here to kickstart.
    /// Server-side settings (store root / enhance flow / etc.) come from
    /// `~/.mse/config.toml`, not from this tool call — see `mlua_swarm_server_restart` doc.
    #[serde(default)]
    bind: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct ServerStatusReq {
    /// Where `mse serve` is: a base URL (`https://host`, scheme included)
    /// or a bare `host:port` (which gets `http://`). Omitted falls back to
    /// `MSE_HTTP`, then to `http://127.0.0.1:7777`.
    #[serde(default)]
    bind: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct ServerShutdownReq {
    /// Where `mse serve` is: a base URL (`https://host`, scheme included)
    /// or a bare `host:port` (which gets `http://`). Omitted falls back to
    /// `MSE_HTTP`, then to `http://127.0.0.1:7777`.
    #[serde(default)]
    bind: Option<String>,
    /// Skip the occupancy check (in-flight runs / attached operators) and
    /// kill unconditionally. Default `false` — a busy server refuses.
    #[serde(default)]
    force: Option<bool>,
}

#[derive(Deserialize, JsonSchema)]
struct ServerRestartReq {
    /// Where `mse serve` is: a base URL (`https://host`, scheme included)
    /// or a bare `host:port` (which gets `http://`). Omitted falls back to
    /// `MSE_HTTP`, then to `http://127.0.0.1:7777`.
    #[serde(default)]
    bind: Option<String>,
    /// Skip the occupancy check (in-flight runs / attached operators) and
    /// kill unconditionally. Default `false` — a busy server refuses.
    #[serde(default)]
    force: Option<bool>,
}

/// Input for `mlua_swarm_server_bootstrap` — load the LaunchAgent so
/// launchd owns the mse-serve job. Idempotent on repeat.
#[derive(Deserialize, JsonSchema, Default)]
struct ServerBootstrapReq {
    /// Reserved for a future signature widening of the underlying
    /// bootstrap primitive; currently ignored — the handler always uses
    /// the default healthz bind (`127.0.0.1:7777`).
    #[serde(default)]
    bind: Option<String>,
    /// Reserved for a future signature widening of the underlying
    /// bootstrap primitive; currently ignored — the handler always uses
    /// the canonical installed LaunchAgent plist path. Declared as
    /// `String` (not `PathBuf`) so the JSON Schema stays a concrete
    /// `{"type":"string"}` when the field is re-wired — see GH #24
    /// (schemars any-schema drop).
    #[serde(default)]
    plist_path: Option<String>,
}

/// Input for `mlua_swarm_server_install` — render the LaunchAgent plist
/// and load it. Idempotent on repeat (a re-install with the same params
/// produces a byte-identical plist and re-bootstraps).
#[derive(Deserialize, JsonSchema)]
struct ServerInstallReq {
    /// Override cargo bin dir (default `$HOME/.cargo/bin`). Declared as
    /// `String` for schema stability (see `ServerBootstrapReq::plist_path`).
    #[serde(default)]
    cargo_bin: Option<String>,
    /// Override the daemon's `WorkingDirectory` (default `~/.mse`, the
    /// service's own state directory — never the installer's CWD; a
    /// checkout-dependent working directory makes the daemon
    /// unstartable once the checkout moves, GH #97). Created if
    /// missing. Declared as `String` for schema stability. The
    /// pre-GH-#97 field name `project_root` is accepted as an alias.
    #[serde(default, alias = "project_root")]
    working_dir: Option<String>,
}

/// Input for `mlua_swarm_server_uninstall` — remove the LaunchAgent and
/// its plist. Idempotent on repeat.
#[derive(Deserialize, JsonSchema, Default)]
struct ServerUninstallReq {}

// ---- S3 operator client tool param schemas ----
// (see the WS multi-session design for the MCP tool set).

#[derive(Deserialize, JsonSchema)]
struct OperatorJoinReq {
    /// Effective model/tool/variant capabilities enforced by this
    /// Operator/MainAI. Required for fail-closed Runner binding.
    #[serde(default)]
    capability_manifest: Option<mlua_swarm::AgentProviderManifest>,
    /// Describe what you are working on, in about 50 characters.
    ///
    /// It is used to tell you apart from other tasks running in parallel in
    /// the same repo or the same worktree. Later, you or whoever takes over
    /// from you will read the operator list and decide "is this my work" by
    /// this line.
    ///
    /// Do NOT write any of these — they are recorded automatically: repo
    /// path, worktree path, Run id, goal, start time.
    ///
    /// Write what you are touching and what you are doing to it. Add one
    /// piece of immediately preceding context if there is any.
    ///
    /// Required here even though `POST /v1/operators` accepts a join
    /// without one: this is the tool an AI joins through, and the AI
    /// already knows the answer at that moment, so the only thing an
    /// optional field would buy is an operator list nobody can read.
    desc: String,
}

#[derive(Deserialize, JsonSchema)]
struct OperatorListReq {
    /// sid returned by `mse_operator_join` — whose Bearer token this
    /// process presents. `GET /v1/operators` is Bearer-gated, and any live
    /// session's token opens it; omitted = this process's sole live
    /// session, which fails if it holds none or several.
    #[serde(default)]
    sid: Option<String>,
    /// Page size. Omitted = the server default (50); the server clamps to
    /// its own ceiling (200).
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Deserialize, JsonSchema)]
struct OperatorPendingWaitReq {
    /// sid returned by `mse_operator_join`.
    sid: String,
    /// Long-poll timeout in milliseconds. Default 30000 (30s).
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Deserialize, JsonSchema)]
struct OperatorAckReq {
    /// sid returned by `mse_operator_join`.
    sid: String,
    /// `req_id` from the `mse_pending_wait` frame being acked.
    req_id: String,
    /// One of `"answer"` (SeniorBridge.ask reply — pass `value`),
    /// `"hook_ack"` (SpawnHook.before OK/NG — pass `ok` + optional `error` as
    /// the rejection reason), `"spawn_ack"` (Operator.execute result —
    /// pass `value` + `ok` + optional `error`), or `"spawn_halt"` (issue #7:
    /// controlled halt for the current spawn — pass `value` (optional
    /// partial ctx) + `error` (reused as the human-readable halt
    /// reason). The step lands as `WorkerResult { ok: true, value:
    /// {"halted": true, "reason": <reason>, "value": <partial>} }` —
    /// distinct from `spawn_ack ok=false`, which is the fail-loud path
    /// for real worker errors).
    kind: String,
    #[serde(default)]
    #[schemars(schema_with = "any_json_schema")]
    value: Option<JsonValue>,
    /// `true` = pass (default). For `hook_ack`, `false` rejects the spawn.
    /// Ignored for `spawn_halt` (halt is always a normal termination).
    #[serde(default = "default_true_bool")]
    ok: bool,
    /// `hook_ack`: rejection reason when `ok=false`. `spawn_ack`: error
    /// message when `ok=false`. `spawn_halt`: human-readable halt reason
    /// (for logs). Ignored for `answer`.
    #[serde(default)]
    error: Option<String>,
    /// `spawn_ack` only: Operator-proxied per-step run stats for the
    /// SubAgent this ack closes. The SubAgent cannot observe its own
    /// token usage — the harness reports it to the OPERATOR in the
    /// completion notification (`subagent_tokens` etc.), so pass it
    /// here and the server folds it into the terminal `StepEntry`
    /// (surfaced by `GET /v1/runs/:id/steps` / `swarm_status`). Shape:
    /// `{"usage": {"input_tokens": N, "output_tokens": N,
    /// "total_tokens": N}, "model": "...", "num_turns": N,
    /// "adapter_data": {...}}` — every field optional, **including each
    /// of the three token fields**: a harness that only surfaces one
    /// total reports `{"usage": {"total_tokens": N}}` and the splits
    /// read as `0` (an omitted total is derived as `input + output`).
    /// Ignored for the other kinds.
    #[serde(default)]
    #[schemars(schema_with = "any_json_schema")]
    stats: Option<JsonValue>,
}

#[derive(Deserialize, JsonSchema)]
struct OperatorLeaveReq {
    /// sid returned by `mse_operator_join`.
    sid: String,
}

// ---- handover surface tool param schemas (server model §4.3 / §4.5 / W5) ----
//
// The three reads are Bearer-gated and take a `sid` on the same rule
// `OperatorListReq` does; the acquire deliberately takes none, because the
// bearer plays no part in assignment (**B2**) — see `RunAcquireReq`.

#[derive(Deserialize, JsonSchema)]
struct RunAssigneesReq {
    /// The Run to read the holder list of (`R-<hex>`).
    run_id: String,
    /// sid whose Bearer token this process presents. Any live session's
    /// token opens the route; omitted = this process's sole live session,
    /// which fails if it holds none or several.
    #[serde(default)]
    sid: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct RunHandoverReq {
    /// The Run to take the snapshot of (`R-<hex>`).
    run_id: String,
    /// sid whose Bearer token this process presents — same rule as
    /// `mse_run_assignees`.
    #[serde(default)]
    sid: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct RunMaterialReq {
    /// The Run the step belongs to (`R-<hex>`).
    run_id: String,
    /// The step to fetch the material for (`ST-<hex>`) — typically one
    /// `mse_run_handover` listed in `unanswered[]`, whose entries carry
    /// the same id in `step_id`. Required: the route answers about one
    /// step and has no default for which.
    step_id: String,
    /// sid whose Bearer token this process presents — same rule as
    /// `mse_run_assignees`.
    #[serde(default)]
    sid: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct RunAcquireReq {
    /// The Run whose seat to take (`R-<hex>`).
    run_id: String,
    /// Who takes the seat: the `OperatorId` recorded as the holder —
    /// normally the `sid` returned by `mse_operator_join`, i.e. the
    /// session that intends to drive this Run.
    ///
    /// Required, and not defaulted from this process's live session on
    /// purpose. The seat's holder is a decision the caller makes; the
    /// server stores this verbatim without checking it against the
    /// registry (**Q2** — an acquire does not enquire), so a wrong value
    /// here surfaces as a Run whose dispatches reach nobody.
    op: String,
    /// Why this operator is taking the seat (**A9** / **Q1**). Mandatory
    /// and non-empty at the server too — it is the line a later reader of
    /// the holder list tells two concurrent takeovers apart by, so write
    /// what you are about to do with the Run rather than "takeover".
    desc: String,
    /// Which Blueprint-declared seat to take. Omit when the Blueprint
    /// declares exactly one Operator; name it when it declares several
    /// (omitting it then is a `400` listing the candidates).
    #[serde(default)]
    slot: Option<String>,
}

// ---- worker HTTP tool param schemas ----
// Pure-MCP replacements for the two Bash curl steps in the mse-worker
// wrapper agents, so their tools list can drop `Bash` entirely (the curl
// allowance kept getting repurposed as a grep/find workaround channel).

#[derive(Deserialize, JsonSchema)]
struct WorkerFetchReq {
    /// Bearer for `/v1/worker/*`: the `wh-<hex>` short handle from the
    /// Spawn frame's `worker_handle` field (recommended), or the full
    /// encoded `capability_token`.
    worker_handle: String,
    /// Server HTTP root, e.g. `http://127.0.0.1:7777`. Usually omitted:
    /// this process records it per `worker_handle` when the Spawn frame
    /// passes through `mse_pending_wait`. Pass explicitly to override, or
    /// when the Bearer is a full `capability_token` (no recorded route).
    #[serde(default)]
    base_url: Option<String>,
    /// Step id (`ST-<hex>`) the prompt belongs to. Usually omitted — same
    /// auto-resolution as `base_url` (from the Spawn frame's `task_id`).
    #[serde(default)]
    task_id: Option<String>,
    /// GH #31: local path `system_ref` resolution (by-reference delivery
    /// mode) writes the verified `system` bytes to, once downloaded/read
    /// and sha256-verified. Optional — defaults to `<temp
    /// dir>/{task_id}-{attempt}.md`, matching the server-side `File`-mode
    /// store's naming convention (different directory/host, same naming
    /// intent). Ignored entirely when the fetched payload has no
    /// `system_ref` (inline `system` case).
    #[serde(default)]
    system_ref_path: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct WorkerSubmitReq {
    /// Bearer for `/v1/worker/*`: the `wh-<hex>` short handle from the
    /// Spawn frame's `worker_handle` field (recommended), or the full
    /// encoded `capability_token`.
    worker_handle: String,
    /// Server HTTP root, e.g. `http://127.0.0.1:7777`. Usually omitted:
    /// this process records it per `worker_handle` when the Spawn frame
    /// passes through `mse_pending_wait`. Pass explicitly to override, or
    /// when the Bearer is a full `capability_token` (no recorded route).
    #[serde(default)]
    base_url: Option<String>,
    /// Raw result body, POSTed verbatim as `text/plain` (the server strips
    /// trailing whitespace only; internal newlines are preserved).
    body: String,
    /// `false` marks the attempt failed (`?ok=false` — lands as
    /// `DispatchOutcome::Blocked`, the flow.ir Try catch path). Omitted /
    /// `true` = normal success. Mutually exclusive with `name` — a named
    /// artifact part has no pass/fail state of its own (only the attempt,
    /// completed via a later `ok=false`-capable submit, does).
    #[serde(default)]
    ok: Option<bool>,
    /// GH #36: when given, this call **stages one named output part**
    /// (`POST /v1/worker/artifact?name=<name>`) instead of completing the
    /// attempt (`POST /v1/worker/submit`) — the task stays open, and the
    /// worker may POST any number of additional named parts (same or
    /// different `name`s) before finally submitting a plain (no-`name`)
    /// call to complete. A step with staged parts ends up with output
    /// shape `{"out": <final submit body>, "parts": {<name>: <value>,
    /// ...}}`; a downstream step reads a part via bracket notation, e.g.
    /// `"in": "$.<step>.parts[\"plan.md\"]"`. Re-staging the same `name`
    /// within one attempt replaces the earlier value (last write wins).
    /// Omitted (`None`) = unchanged legacy behavior (this call completes
    /// the attempt).
    #[serde(default)]
    name: Option<String>,
    /// GH #32: optional structured worker-degradation entries. When
    /// non-empty, each entry is POSTed to `/v1/worker/degradation` BEFORE
    /// this call's own submit/artifact POST (serial, in append order) —
    /// an independent channel from `body`/`name`, never folded into step
    /// OUTPUT (see [`mlua_swarm::store::run::DegradationEntry`]'s doc for
    /// the invariant). Omitted (`None`) = unchanged pre-#32 behavior.
    #[serde(default)]
    degradations: Option<Vec<DegradationInput>>,
    /// Optional self-reported per-attempt stats (token usage / model /
    /// turns). POSTed to `/v1/worker/stats` BEFORE this call's own
    /// submit/artifact POST, so the dispatcher's outcome fold picks them
    /// up — a WS-operator SubAgent has no in-process fold site of its
    /// own, and this is that boundary's only way to report cost without
    /// a raw HTTP call. Observational: never folded into step OUTPUT.
    /// Report on the FINAL (plain, no-`name`) submit; stats sent on an
    /// earlier artifact-staging call are kept per `(task_id, attempt)`
    /// and overwritten by a later report (last write wins). Omitted
    /// (`None`) = no stats POST at all.
    #[serde(default)]
    stats: Option<StatsInput>,
}

/// Client-facing shape for one worker-reported degradation entry (GH #32) —
/// mirrors the wire body `mlua-swarm-server`'s `POST
/// /v1/worker/degradation` endpoint expects
/// (`crates/mlua-swarm-server/src/worker.rs`'s `DegradationBody`). The
/// server-injected metadata (`step_ref` / `attempt` / `at`) is deliberately
/// NOT part of this client-facing shape — the worker only supplies what it
/// observed.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
struct DegradationInput {
    /// The tool (or capability) the worker attempted to use.
    tool: String,
    /// The error that triggered the fallback, in the worker's own words.
    error: String,
    /// What the worker substituted instead of failing.
    fallback: String,
    /// Optional free-form context from the worker.
    #[serde(default)]
    note: Option<String>,
}

/// Client-facing shape for a worker's self-reported per-attempt stats —
/// mirrors the wire body `mlua-swarm-server`'s `POST /v1/worker/stats`
/// endpoint expects (`crates/mlua-swarm-server/src/worker.rs`'s
/// `StatsBody`), which is itself the wire twin of
/// [`mlua_swarm::store::trace::WorkerStats`]. Every field is optional and
/// an all-empty body is accepted server-side (and dropped), matching
/// `DegradationInput`'s "the worker only supplies what it observed"
/// convention. `usage` reuses the engine's own [`TokenUsage`] type rather
/// than restating its three counters, so the client shape cannot drift
/// from what the fold stores.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
struct StatsInput {
    /// Worker kind label. Server-side default is `"operator"`.
    #[serde(default)]
    worker_kind: Option<String>,
    /// The model that served the attempt.
    #[serde(default)]
    model: Option<String>,
    /// Normalized token usage (`input_tokens` / `output_tokens` /
    /// `total_tokens`) — each one optional, so a worker that only knows
    /// its total still reports usable usage.
    #[serde(default)]
    usage: Option<TokenUsage>,
    /// Number of LLM turns the attempt ran.
    #[serde(default)]
    num_turns: Option<u32>,
    /// Free-form worker-specific detail (size-capped on fold, never
    /// interpreted by the engine).
    #[serde(default)]
    #[schemars(schema_with = "any_object_schema")]
    adapter_data: Option<JsonValue>,
}

/// Builds the `/v1/worker/submit` or `/v1/worker/artifact?name=<name>`
/// endpoint URL for [`MseServer::mse_worker_submit`]. `base_url`'s trailing
/// slash (if any) is trimmed before joining. `name`, when given, is
/// percent-encoded into the `name` query parameter via
/// [`reqwest::Url::query_pairs_mut`] (`url`/`form_urlencoded` under the
/// hood — handles dots, spaces, and non-ASCII without any hand-rolled
/// escaping). Pure and side-effect-free so the URL shape is unit-testable
/// without a network call. Error is the parse failure's `Display` text
/// (the `url` crate is only reachable here via `reqwest`'s `pub use
/// url::Url` re-export, not as a direct dependency, so its `ParseError`
/// type is deliberately not named in this signature).
fn worker_submit_endpoint_url(base_url: &str, name: Option<&str>) -> Result<reqwest::Url, String> {
    let base = base_url.trim_end_matches('/');
    let path = if name.is_some() { "artifact" } else { "submit" };
    let mut url =
        reqwest::Url::parse(&format!("{base}/v1/worker/{path}")).map_err(|e| e.to_string())?;
    if let Some(name) = name {
        url.query_pairs_mut().append_pair("name", name);
    }
    Ok(url)
}

// convention-token-ok: mse_worker_submit is a mlua-swarm public MCP tool name.
/// Builds the `/v1/worker/degradation` endpoint URL for
/// [`MseServer::mse_worker_submit`]'s pre-submit degradation POSTs (GH #32).
/// `base_url`'s trailing slash (if any) is trimmed before joining;
/// no query params — unlike [`worker_submit_endpoint_url`]'s `name` case,
/// the degradation body carries its shape as a JSON payload, not a query
/// key. Pure and side-effect-free, mirroring `worker_submit_endpoint_url`'s
/// own unit-testable shape.
fn worker_degradation_endpoint_url(base_url: &str) -> Result<reqwest::Url, String> {
    let base = base_url.trim_end_matches('/');
    reqwest::Url::parse(&format!("{base}/v1/worker/degradation")).map_err(|e| e.to_string())
}

// convention-token-ok: mse_worker_submit is a mlua-swarm public MCP tool name.
/// Builds the `/v1/worker/stats` endpoint URL for
/// [`MseServer::mse_worker_submit`]'s pre-submit stats POST. Same shape
/// and error contract as [`worker_degradation_endpoint_url`] — trailing
/// slash trimmed, no query params, pure and unit-testable.
fn worker_stats_endpoint_url(base_url: &str) -> Result<reqwest::Url, String> {
    let base = base_url.trim_end_matches('/');
    reqwest::Url::parse(&format!("{base}/v1/worker/stats")).map_err(|e| e.to_string())
}

// ---- tool param schemas ----

#[derive(Deserialize, JsonSchema)]
struct SwarmRunReq {
    /// How to resolve the Blueprint. Accepts either a
    /// `BlueprintSelector` (`{kind: "inline"|"id"|"file", ...}`) or, for
    /// backward compat, a bare Blueprint object (implicitly wrapped as
    /// `{kind: "inline", blueprint: <it>}`).
    blueprint: BlueprintInput,
    /// Optional init context passed to the swarm. Default `{}`.
    #[serde(default)]
    #[schemars(schema_with = "any_object_schema")]
    init_ctx: Option<JsonValue>,
    /// Timeout in seconds. Default 300 (= 5 min).
    #[serde(default)]
    timeout_secs: Option<u64>,
    /// Operator id label. Default "mcp-run".
    #[serde(default)]
    operator_id: Option<String>,
    /// Run-scoped Operator session pin (`S-<hex>`): the session this run's
    /// Spawn frames are delivered to, regardless of which session currently
    /// holds the Blueprint's logical `operator_ref` role. Sent as the
    /// `operator_sid` field of `POST /v1/tasks`.
    ///
    /// Usually omitted: when this process holds exactly one live Operator
    /// session (`mse_operator_join`) and the run targets the server that
    /// session is joined to, that sid is pinned automatically — a driver's
    /// runs come back to the driver without any extra argument. Zero or two
    /// or more live sessions auto-pin nothing. Set the field explicitly to
    /// pin a specific session (for example when this process holds several).
    ///
    /// Only the `{kind: "id"}` selector can carry a pin: it is the path that
    /// launches on `mse serve`, where Operator sessions live. Pinning an
    /// inline / file Blueprint (which runs inside this process, with no
    /// Operator sessions of its own) is rejected rather than ignored.
    #[serde(default)]
    operator_sid: Option<String>,
    /// Which Blueprint-declared Operator seat the pin assigns — an
    /// `OperatorDef.name` from the Blueprint's `operators[]`, sent as the
    /// `operator_slot` field of `POST /v1/tasks`.
    ///
    /// Usually omitted: a Blueprint declaring exactly one Operator has
    /// only one seat to fill, so the server fills it. Name it when the
    /// Blueprint declares several (per-lane Blueprints such as
    /// `phase_a_op` / `phase_b_op`), where omitting it is a `400` that
    /// lists the candidates rather than a guess.
    ///
    /// Applies to the pin, so — like `operator_sid` — only the
    /// `{kind: "id"}` selector carries it.
    #[serde(default)]
    operator_slot: Option<String>,
    /// `main_ai` / `automate` / `composite` — the "Runtime Global" tier of
    /// the 4-tier `OperatorKind` cascade. Unspecified falls through to the
    /// BP-level tiers (`OperatorDef.kind` / `Blueprint.default_operator_kind`)
    /// instead of eagerly defaulting to `automate`.
    #[serde(default)]
    operator_kind: Option<String>,
    /// "Runtime Agent-level" tier (highest priority) — per-agent override,
    /// keyed by `AgentDef.name`, value is `main_ai` / `automate` / `composite`.
    #[serde(default)]
    operator_kind_overrides: Option<HashMap<String, String>>,
    /// GH #37: opt into the detached (asynchronous) launch. `false`
    /// (default) keeps the blocking run-to-completion behavior. `true`
    /// returns `{run_id, task_id, status: "running"}` immediately — the
    /// flow eval continues in the background bounded by `ttl_secs`
    /// (detach path) / `timeout_secs` (in-process sync); poll
    /// `swarm_status` for the terminal status and result.
    #[serde(default)]
    detach: Option<bool>,
    /// Detach-path TTL in seconds — the lifetime bound of a
    /// `detach: true` run (the timed-out future is dropped and the run
    /// row is finalized with a `failed` result + `core.run_finished
    /// { status: "failed", reason: "ttl <n>s exceeded" }` trace
    /// event). Unset falls through the TTL resolution cascade: (1)
    /// `TaskLaunchRequest.ttl_secs` from the request body (this
    /// field), (2) the resolved Blueprint's
    /// `metadata.default_run_ttl_secs`, (3) the server global
    /// `default_run_ttl()` (1800s). Ignored on `detach: false` (sync)
    /// launches — those are bounded by `timeout_secs`.
    #[serde(default)]
    ttl_secs: Option<u64>,
}

/// How to resolve a Blueprint for `swarm_run`. Symmetric with the
/// `POST /v1/tasks` request shape.
#[derive(Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum BlueprintSelector {
    /// Full Blueprint value embedded in the tool call.
    Inline {
        /// Blueprint payload. Schema = mlua-swarm-blueprint-schema.
        blueprint: JsonValue,
    },
    /// Reference a Blueprint registered on the mse serve store by id.
    /// Proxies to `POST /v1/tasks`.
    Id {
        /// Registered BlueprintId (server-side store).
        id: String,
        /// Where `mse serve` is: a base URL (`https://host`, scheme
        /// included) or a bare `host:port` (which gets `http://`).
        /// Omitted falls back to `MSE_HTTP`, then to
        /// `http://127.0.0.1:7777`.
        #[serde(default)]
        bind: Option<String>,
    },
    /// Read Blueprint JSON from a file rooted at the mse-mcp process CWD.
    /// Absolute paths and `..` (parent-dir) components are rejected.
    File {
        /// Relative path to a Blueprint JSON file (CWD-rooted).
        path: String,
    },
}

/// Accepts either the new `BlueprintSelector` shape or, for backward
/// compat, a bare Blueprint object treated as
/// `{kind: "inline", blueprint: <it>}`.
///
/// Note: `serde(untagged)` tries `Selector` first; if the object lacks a
/// recognized `kind` field, it falls through to `BareInline`.
#[derive(Deserialize, JsonSchema)]
#[serde(untagged)]
enum BlueprintInput {
    Selector(BlueprintSelector),
    /// A bare Blueprint JSON object (backward-compat). The schema is
    /// pinned to `{"type": "object"}` so MCP clients keep the payload
    /// as an object instead of string-encoding it (issue #5, layer 1).
    #[schemars(schema_with = "bare_blueprint_schema")]
    BareInline(JsonValue),
}

fn bare_blueprint_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    // Explicitly declare the JSON type as "object" so MCP clients keep
    // the payload as a real object; without this, `JsonValue` renders
    // to schemars' any-schema (`true`) which triggers the layer-1 bug.
    schemars::json_schema!({
        "type": "object",
        "description": "Backward-compat: bare Blueprint object; treated as {kind: \"inline\", blueprint: <it>}."
    })
}

/// JSON Schema pin for `Option<JsonValue>` fields that carry a JSON object
/// by contract (currently `SwarmRunReq.init_ctx`, the flow.ir root ctx).
///
/// GH #24: same shape as [`bare_blueprint_schema`] — declaring the type as
/// `"object"` keeps MCP clients from dropping the field. Without it,
/// schemars renders `JsonValue` to the any-schema (`true`) and clients that
/// filter tool call arguments against the tool inputSchema silently strip
/// the payload.
fn any_object_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "object",
        "description": "Arbitrary JSON object."
    })
}

/// JSON Schema pin for `Option<JsonValue>` fields that carry any concrete
/// JSON value (currently `OperatorAckReq.value`: the ack payload varies by
/// kind — `answer` reply, `spawn_ack` result, `spawn_halt` partial ctx).
///
/// GH #24: same rationale as [`any_object_schema`], with the type widened
/// to the six concrete JSON types so structured / scalar / null payloads
/// all survive MCP client filtering.
fn any_json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": ["object", "array", "string", "number", "boolean", "null"],
        "description": "Arbitrary JSON value."
    })
}

/// Whether the auto-pin may apply: does a `swarm_run(kind = "id")` launch
/// bound for `bind` target the same server this process's Operator sessions
/// are joined to (`http_base`)?
///
/// An omitted `bind` means the default server, so it matches when
/// `http_base` is that same default. A sid is only meaningful on the server
/// that minted it — auto-pinning it at a different server would turn a
/// launch that used to work into a `400`, so the pin is simply not applied
/// there. An explicit `operator_sid` is never filtered this way: the caller
/// named the session, and an unknown sid failing loudly is the point.
///
/// Pure and side-effect-free so the matching rule is unit-testable without a
/// live session.
fn auto_pin_targets_joined_server(http_base: &str, bind: Option<&str>) -> bool {
    crate::http::Endpoint::resolve(bind).base() == http_base.trim_end_matches('/')
}

/// A run-scoped Operator pin, together with the record of how this process
/// arrived at it.
///
/// A pin assigns the Run to that operator, and an assignment carries a
/// mandatory `desc` (model §4.3 **A9** — `POST /v1/tasks` answers `400`
/// without one). Two things can produce a sid here and they mean different
/// things to whoever reads the Run afterwards: the caller naming a session
/// ([`Self::explicit`]) versus this process pinning its own sole live one
/// ([`Self::auto`]). Writing them as one indistinguishable string would
/// throw away the only fact that distinguishes "the driver asked for this
/// session" from "this session happened to be the only one joined".
struct OperatorPin {
    /// The `OperatorId` sent as `operator_sid`.
    sid: String,
    /// The `Assign.desc` sent as `operator_desc`.
    desc: String,
    /// Which Blueprint-declared Operator seat the pin assigns, sent as
    /// `operator_slot`.
    ///
    /// `None` leaves the seat to the server's rule: a Blueprint declaring
    /// exactly one Operator needs no naming, which is the shape of every
    /// bundled Blueprint and therefore of the auto-pin path. A Blueprint
    /// declaring several answers `400` listing its seats — the caller then
    /// names one via `swarm_run(operator_slot = ...)`, rather than this
    /// process picking a lane on their behalf.
    slot: Option<String>,
}

impl OperatorPin {
    /// The caller named this session in `swarm_run(operator_sid = ...)`,
    /// and optionally the seat in `swarm_run(operator_slot = ...)`.
    fn explicit(sid: String, slot: Option<String>) -> Self {
        Self {
            sid,
            desc: "operator_sid named by the swarm_run caller".to_string(),
            slot,
        }
    }

    /// No `operator_sid` was given, and this process holds exactly one live
    /// session joined to the server the launch targets. The seat may still
    /// be named — "which session" and "which lane" are independent
    /// questions, and only the first one is being answered automatically.
    fn auto(sid: String, slot: Option<String>) -> Self {
        Self {
            sid,
            desc: "mse-mcp auto-pin: this process's sole live operator session".to_string(),
            slot,
        }
    }
}

/// Parse a wire-level kind string into `OperatorKind`. Shared by
/// `SwarmRunReq.operator_kind` and `.operator_kind_overrides` values.
fn parse_operator_kind_str(s: &str) -> Result<OperatorKind, McpError> {
    match s {
        "main_ai" => Ok(OperatorKind::MainAi),
        "composite" => Ok(OperatorKind::Composite),
        "automate" => Ok(OperatorKind::Automate),
        other => Err(McpError::invalid_params(
            format!("operator_kind: unknown value '{other}' (expected main_ai|automate|composite)"),
            None,
        )),
    }
}

/// Read a Blueprint JSON file from the mse-mcp process CWD.
///
/// Path hygiene: absolute paths and any `..` (parent-dir) component are
/// rejected. This is a tool-call argument (user-initiated), so the guard
/// is a straightforward path-traversal block rather than the tighter
/// `$file` ref sandbox described in the Blueprint authoring guide.
fn read_blueprint_from_file(path: &str) -> Result<JsonValue, String> {
    use std::path::{Component, PathBuf};

    let p = PathBuf::from(path);
    if p.is_absolute() {
        return Err(format!(
            "file: absolute path rejected (got {path:?}); use a CWD-relative path"
        ));
    }
    for c in p.components() {
        if matches!(c, Component::ParentDir) {
            return Err(format!(
                "file: `..` parent-dir component rejected (got {path:?})"
            ));
        }
    }
    let bytes = std::fs::read(&p).map_err(|e| format!("file: read {path:?} failed: {e}"))?;
    serde_json::from_slice::<JsonValue>(&bytes)
        .map_err(|e| format!("file: parse {path:?} as JSON failed: {e}"))
}

#[derive(Deserialize, JsonSchema)]
struct SwarmStatusReq {
    run_id: String,
    /// GH #67: where the `mse serve` the run was launched against is — a
    /// base URL (`https://host`, scheme included) or a bare `host:port`
    /// (which gets `http://`). Omitted falls back to `MSE_HTTP`, then to
    /// `http://127.0.0.1:7777`.
    /// When present (or defaulted via `launchd::DEFAULT_BIND`), the
    /// tool issues a best-effort `GET /v1/runs/:id` to fold the server's
    /// authoritative `RunRecord` (`status` / `step_entries` / `result_ref`)
    /// into the response — so a `detach: true` run whose completion the
    /// local `RunHandle` never observed is no longer reported as stale
    /// `running`. The HTTP fetch is guarded by a short timeout and its
    /// failure is silent: the tool falls back to the local run store.
    #[serde(default)]
    bind: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct SwarmRunStatsReq {
    /// The run to report on (`R-<hex>`). Read over HTTP from the server's
    /// own run store, so any run that server knows about works — this
    /// process does not need a local `RunHandle` for it (unlike
    /// `swarm_status`), and a run launched by a different driver session
    /// is reachable by id alone.
    run_id: String,
    /// Where the `mse serve` holding the run is: a base URL
    /// (`https://host`, scheme included) or a bare `host:port` (which gets
    /// `http://`). Omitted falls back to `MSE_HTTP`, then to
    /// `http://127.0.0.1:7777`.
    #[serde(default)]
    bind: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct SwarmCancelReq {
    run_id: String,
    /// Where `mse serve` is: a base URL (`https://host`, scheme included)
    /// or a bare `host:port` (which gets `http://`). Omitted falls back to
    /// `MSE_HTTP`, then to `http://127.0.0.1:7777`. The tool
    /// proxies to `POST /v1/runs/:id/cancel` on this bind so the
    /// server-side `RunTraceStore` picks up the `core.cancel_requested`
    /// event and the Run row's status flips to `Cancelled` — the
    /// detach-launched path where the local `RunHandle` isn't the
    /// authoritative source.
    #[serde(default)]
    bind: Option<String>,
}

// ---- tools ----

#[tool_router]
impl MseServer {
    #[tool(
        description = "Join as an Operator session: POST /v1/operators (mint sid+token, submit capability_manifest and the `desc` line that identifies this session) then connect WS /v1/operators/:sid/ws with the returned Bearer token. `desc` is mandatory here — see its schema description; it is what a later reader (you, or whoever takes over) tells your session apart by on mse_operator_list. The token stays process-local (never returned to the caller). Runner-backed launches resolve the manifest fail-closed by launch_variant, model, and tools. Returns {sid} — use it with mse_pending_wait / mse_ack / mse_operator_leave. A join claims no role and can never conflict: which Run you drive is decided per Run, by launching it (swarm_run pins this session) or by taking a seat on one (POST /v1/runs/:id/acquire), so joining twice for two parallel tasks is the ordinary case rather than a collision."
    )]
    async fn mse_operator_join(
        &self,
        Parameters(req): Parameters<OperatorJoinReq>,
    ) -> Result<CallToolResult, McpError> {
        // This process runs `agent-block` spawns itself (see
        // `block_runner`), so a manifest it submits covers that variant
        // whether or not the caller thought to list it — a strict-binding
        // server would otherwise refuse the block steps this very process
        // is the executor for.
        let manifest = req.capability_manifest.map(with_block_capability);
        self.ensure_block_drain().await;
        let sid = self
            .op_client
            .join(manifest, Some(req.desc))
            .await
            .map_err(client_error_to_mcp)?;
        json_result(&serde_json::json!({ "sid": sid }))
    }

    #[tool(
        description = "List every live Operator session with its 記名 (the join-time description each session wrote about itself, plus the seats it has been assigned: Run, goal, project_root, work_dir, task_metadata, time). GET /v1/operators, which is Bearer-gated — this tool presents the token of `sid` (default: this process's sole live session). Read it before acquiring a Run seat: with exclusivity removed, this list and the per-Run holder list (GET /v1/runs/:id/assignees) are what tell two parallel tasks in the same worktree apart. It is also the recovery path for a stale session whose driver crashed: find it by its 記名, then mse_operator_leave(sid) — there is no by-role release, because a session claims no name to be released by. Ordered by most recent activity first; `limit` caps the page (server default 50, ceiling 200). Returns the server's response body verbatim: {operators: [...], total, limit}."
    )]
    async fn mse_operator_list(
        &self,
        Parameters(req): Parameters<OperatorListReq>,
    ) -> Result<CallToolResult, McpError> {
        let body = self
            .op_client
            .list_operators(req.sid.as_deref(), req.limit)
            .await
            .map_err(client_error_to_mcp)?;
        json_result(&body)
    }

    #[tool(
        description = "Who holds each Operator seat of ONE Run, and which seats nobody holds. GET /v1/runs/:id/assignees, Bearer-gated — this tool presents the token of `sid` (default: this process's sole live session), which is why the route is reachable from here at all. A seat nobody holds is present in `seats[]` with `vacant: true` and `holder: null`; it is never omitted, so \"nobody is on this Run\" and \"this answer did not report holders\" are different bytes. `seats_source: \"run_current_only\"` means the Blueprint could not be resolved and declared-but-vacant seats are therefore missing — `note` says why. This is the narrow half of the pair that prevents a wrong takeover: read it (and mse_operator_list, which names the sessions) BEFORE mse_run_acquire, because acquire itself never refuses and never asks — nothing downstream of this read will stop you taking somebody else's Run. Returns the server's response body verbatim."
    )]
    async fn mse_run_assignees(
        &self,
        Parameters(req): Parameters<RunAssigneesReq>,
    ) -> Result<CallToolResult, McpError> {
        let run_id = parse_run_id(req.run_id)?;
        let body = self
            .op_client
            .run_assignees(req.sid.as_deref(), &run_id)
            .await
            .map_err(client_error_to_mcp)?;
        json_result(&body)
    }

    #[tool(
        description = "The four things you need in order to decide what to do next on a Run, in ONE read — whether or not you are taking over from anybody. GET /v1/runs/:id/handover, Bearer-gated; this tool presents the token of `sid` (default: this process's sole live session). Body: (1) `trace` is a REFERENCE {route, latest_seq}, not the events — `latest_seq` is the watermark telling what is in this snapshot from what happened after it; (2) `seats` / `seats_source` / `note` are the mse_run_assignees body, taken from the same server-side read; (3) `unanswered[]` is every request a current holder still owes this Run, listed once, where `slot` / `op` / `generation` name the seat it went out through and whoever holds that seat now (all three are null for a request that belongs to no seat — a hook_before never passes through one, so naming a seat would be a guess); (4) each entry's `final_present` / `final_ok` say whether that (step_id, attempt) ALREADY produced a value — the difference between re-running a step and doubling its side effect — and `material_route` points at mse_run_material for the rest. `unread_seats[]` names a held seat whose holder could not be asked, so an empty `unanswered` means everyone was asked and owed nothing, never that nobody was asked. Nothing here grades the wait and nothing here acts: a step whose driver went away is waiting, not broken, and the next move is an ordinary mse_run_acquire followed by an ordinary dispatch. Returns the server's response body verbatim — the axes are answered from one read so a seat cannot change hands between them, and re-assembling them from separate calls would put that skew back."
    )]
    async fn mse_run_handover(
        &self,
        Parameters(req): Parameters<RunHandoverReq>,
    ) -> Result<CallToolResult, McpError> {
        let run_id = parse_run_id(req.run_id)?;
        let body = self
            .op_client
            .run_handover(req.sid.as_deref(), &run_id)
            .await
            .map_err(client_error_to_mcp)?;
        json_result(&body)
    }

    #[tool(
        description = "What one step of a Run needs in order to be run — the second half of \"what do I do next\", pointed at by each mse_run_handover `unanswered[].material_route`. GET /v1/runs/:id/material?step_id=<id>, Bearer-gated; this tool presents the token of `sid` (default: this process's sole live session). `step_id` is required. Body: `payload` is the same WorkerPayload a SubAgent self-fetches from GET /v1/worker/prompt (this route exists beside it because the GATE differs, not the payload — the worker route is held by a per-task CapToken an Assignee does not have and must not be issued); `run_link` is `confirmed` when the payload's own context names the Run in the path and `unconfirmed` when it carries no Run identity to check against (`note` says why); `final_present` / `final_ok` repeat axis 4's first half so this answers \"what do I do next\" on its own. The Final's VALUE is deliberately not here — presence and the ok flag are what the decision needs, and the value is unbounded. 404 when the step is unknown to the engine or belongs to a different Run. Returns the server's response body verbatim."
    )]
    async fn mse_run_material(
        &self,
        Parameters(req): Parameters<RunMaterialReq>,
    ) -> Result<CallToolResult, McpError> {
        let run_id = parse_run_id(req.run_id)?;
        let step_id = StepId::parse(req.step_id)
            .map_err(|e| McpError::invalid_params(format!("invalid step_id: {e}"), None))?;
        let body = self
            .op_client
            .run_material(req.sid.as_deref(), &run_id, &step_id)
            .await
            .map_err(client_error_to_mcp)?;
        json_result(&body)
    }

    #[tool(
        description = "Take one Operator seat of a Run: POST /v1/runs/:id/acquire with `op` (who takes it) / `desc` (why) / `slot` (which seat — omit when the Blueprint declares exactly one Operator). Read FIRST, then take: this call presents no Bearer and needs none, it does not enquire, and it never refuses — a held seat is taken from its holder, last writer wins. That is deliberate (the bearer must not decide who holds a seat), and it means nothing here will catch you acquiring the wrong Run. What catches it is the pair of reads in front: mse_operator_list to see which session is doing what, and mse_run_assignees / mse_run_handover to see who is on this Run and what is in flight on it. Returns the server's response body verbatim, and read all of it: `gen` is the generation your seat is stamped at and the number every later reply of yours is accepted under; `previous` is the holder you displaced (`null` if the seat was vacant) — that is the answer to \"did I take this from someone\"; `t_discard` reports what happened to that holder's in-flight requests for this seat (`discarded: null` means the discard could not be addressed at all, not that there was nothing to drop). Taking a seat is a read-then-act, not a lock: no queue, no wait, and no route that empties a seat."
    )]
    async fn mse_run_acquire(
        &self,
        Parameters(req): Parameters<RunAcquireReq>,
    ) -> Result<CallToolResult, McpError> {
        let run_id = parse_run_id(req.run_id)?;
        let body = self
            .op_client
            .run_acquire(&run_id, &req.op, &req.desc, req.slot.as_deref())
            .await
            .map_err(client_error_to_mcp)?;
        json_result(&body)
    }

    #[tool(
        description = "Pop one pending server frame (ask / hook_before / hook_after / spawn) for `sid`, waiting up to `timeout_ms` (default 30000) if the queue is empty. Returns {timed_out, req_id?, type?, payload?} on delivery — `type` mirrors the server's ServerMsg discriminant, `payload` carries the remaining frame fields verbatim. Returns {timed_out: true} on timeout. Reply via mse_ack with a matching `kind`. A spawn whose `worker.variant` is `agent-block` never reaches this queue: this process runs that block itself, on this host, with no SubAgent and without waiting for anyone to poll — the WS reader diverts it the moment it arrives (so it runs during a blocking `swarm_run` too), and a background task resolves `<MSE_BLOCKS_DIR>/<agent name>/init.lua`, fetches the step's prompt through the worker endpoint, runs the script with the launch's work_dir as project root, POSTs staged parts and the body the way a SubAgent would, and acks the spawn. `blocks_dispatched` counts the spawns diverted that way since the previous call (a block that fails — missing `MSE_BLOCKS_DIR`, unknown block, script error — is submitted as a failed attempt with the reason as its body, never silently dropped). Guide: `mse://guides/agent-block-runner`."
    )]
    async fn mse_pending_wait(
        &self,
        Parameters(req): Parameters<OperatorPendingWaitReq>,
    ) -> Result<CallToolResult, McpError> {
        self.ensure_block_drain().await;
        let timeout_ms = req.timeout_ms.unwrap_or(30_000);
        let frame = self
            .op_client
            .pending_wait(&req.sid, timeout_ms)
            .await
            .map_err(client_error_to_mcp)?;
        // Block spawns never reach this queue — the WS reader diverts them
        // to the drain (see `ensure_block_drain`). The count is the one
        // trace of them a driver gets to see here.
        let blocks_dispatched = self.op_client.blocks_intercepted_since_last();
        match frame {
            Some(f) => json_result(&serde_json::json!({
                "timed_out": false,
                "req_id": f.req_id,
                "type": f.kind,
                "payload": f.payload,
                "blocks_dispatched": blocks_dispatched,
            })),
            None => json_result(&serde_json::json!({
                "timed_out": true,
                "blocks_dispatched": blocks_dispatched,
            })),
        }
    }

    /// Starts, once per process, the task that runs the block spawns the
    /// WS readers divert (see `OperatorClientState::take_block_rx`). Called
    /// from every tool that can lead to a spawn frame arriving, so the
    /// drain exists before the first session does; a second call is a
    /// no-op.
    async fn ensure_block_drain(&self) {
        let Some(mut rx) = self.op_client.take_block_rx().await else {
            return;
        };
        let op = self.op_client.clone();
        tokio::spawn(async move {
            while let Some((sid, frame)) = rx.recv().await {
                let Some(spawn) =
                    block_runner::parse_block_spawn(frame.kind, &frame.req_id, &frame.payload)
                else {
                    continue;
                };
                // Each block on its own task: a slow lane must not hold
                // back the next spawn, and a fanout of lanes runs them
                // side by side the way the server would.
                let op = op.clone();
                tokio::spawn(async move {
                    dispatch_block_spawn(op, sid, spawn).await;
                });
            }
        });
    }

    #[tool(
        description = "Ack a pending frame popped via mse_pending_wait. kind=\"answer\" (SeniorBridge.ask reply, pass `value`), kind=\"hook_ack\" (SpawnHook.before OK/NG, pass `ok` + optional `error` as the rejection reason), kind=\"spawn_ack\" (Operator.execute result, pass `value` + `ok` + optional `error` + optional `stats` — the Operator's proxy report of the SubAgent's resource usage from the harness completion notification, e.g. {\"usage\": {\"input_tokens\": N, \"output_tokens\": N, \"total_tokens\": N}, \"model\": \"...\", \"num_turns\": N}; every field is optional including each token field, so a harness that only knows one total sends {\"usage\": {\"total_tokens\": N}} and the splits read as 0; the server folds it into the terminal StepEntry per-step run stats), kind=\"spawn_halt\" (issue #7: controlled halt for the current spawn — pass optional `value` (partial ctx) + optional `error` (halt reason); the step lands as WorkerResult{ok:true, value:{halted:true, reason, value}} — a normal termination, not a worker error). Sends the corresponding ClientMsg over the sid's WS connection. Returns {sent: true}."
    )]
    async fn mse_ack(
        &self,
        Parameters(req): Parameters<OperatorAckReq>,
    ) -> Result<CallToolResult, McpError> {
        self.op_client
            .ack(
                &req.sid, req.req_id, &req.kind, req.value, req.ok, req.error, req.stats,
            )
            .await
            .map_err(client_error_to_mcp)?;
        json_result(&serde_json::json!({ "sent": true }))
    }

    #[tool(
        description = "Leave an Operator session: DELETE /v1/operators/:sid (Bearer), abort the WS reader task, and drop the local sid entry. Returns {removed: true}."
    )]
    async fn mse_operator_leave(
        &self,
        Parameters(req): Parameters<OperatorLeaveReq>,
    ) -> Result<CallToolResult, McpError> {
        self.op_client
            .leave(&req.sid)
            .await
            .map_err(client_error_to_mcp)?;
        json_result(&serde_json::json!({ "removed": true }))
    }

    #[tool(
        description = "Worker-side fetch: GET <base_url>/v1/worker/prompt?task_id=<task_id> with `Authorization: Bearer <worker_handle>`. Normally the `worker_handle` (`wh-` short handle from the Spawn frame) is the ONLY required param — base_url and task_id auto-resolve from the route this process recorded when the Spawn frame passed through mse_pending_wait; pass them explicitly to override (or when the Bearer is a full capability_token). Returns the server's WorkerPayload JSON verbatim ({task_id, attempt, agent, prompt, system?, context?} — `context` is the AgentContextView task-level context: project_root / work_dir / task_metadata / run_id / project_name_alias, GH #20 Contract C). Pure-MCP replacement for the wrapper agents' Bash curl step — no shell involved. GH #31: when the fetched payload carries `system_ref` instead of `system` (the baked prompt exceeded the server's by-reference size threshold), this tool automatically resolves it — downloads (`Http` mode) or reads (`File` mode) the referenced content, sha256-verifies it against `system_ref.sha256` (one retry on mismatch), writes the verified bytes to a local file (default `<temp dir>/{task_id}-{attempt}.md`, override with `system_ref_path`), and reads the file back to confirm the write landed. On full success the returned JSON is the original payload verbatim plus a top-level `system_ref_resolution: {ok: true, path, sha256, size_bytes}` companion field — `ok: true` here means only that the file was written to disk intact, NOT that the caller has loaded its contents into an LLM context. On any resolution failure the tool returns a standalone `{ok: false, stage: \"download\"|\"hash_mismatch\"|\"write\", error}` value instead of the payload (this is a value-level result, not a McpError — the outer WorkerPayload fetch itself already succeeded)."
    )]
    async fn mse_worker_fetch(
        &self,
        Parameters(req): Parameters<WorkerFetchReq>,
    ) -> Result<CallToolResult, McpError> {
        // Explicit params win; otherwise fall back to the route captured
        // from the Spawn frame (keyed by worker_handle) at pending_wait
        // time — the MainAI only has to relay the handle to the SubAgent.
        let route = self.op_client.worker_route(&req.worker_handle).await;
        let base_url = req
            .base_url
            .or_else(|| route.as_ref().map(|r| r.base_url.clone()))
            .ok_or_else(|| {
                McpError::invalid_params(
                    "base_url not given and no Spawn route is recorded for this worker_handle \
                     — pass base_url explicitly (routes are recorded when the Spawn frame is \
                     popped via mse_pending_wait in this process)"
                        .to_string(),
                    None,
                )
            })?;
        let task_id_raw = req
            .task_id
            .or_else(|| route.as_ref().map(|r| r.task_id.clone()))
            .ok_or_else(|| {
                McpError::invalid_params(
                    "task_id not given and no Spawn route is recorded for this worker_handle \
                     — pass task_id explicitly"
                        .to_string(),
                    None,
                )
            })?;
        // Fail fast before any network I/O — the server's typed PromptQuery
        // would reject a malformed step id with a 400 anyway (issue #14).
        let task_id = StepId::parse(task_id_raw)
            .map_err(|e| McpError::invalid_params(format!("invalid task_id: {e}"), None))?;
        let base = base_url.trim_end_matches('/');
        let url = format!("{base}/v1/worker/prompt");
        let client = crate::http::client_builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| McpError::internal_error(format!("client build: {e}"), None))?;
        let resp = client
            .get(&url)
            .query(&[("task_id", task_id.as_str())])
            .header("Authorization", format!("Bearer {}", req.worker_handle))
            .send()
            .await
            .map_err(|e| McpError::internal_error(format!("worker fetch: {e}"), None))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(McpError::internal_error(
                format!("worker fetch: HTTP {} — {body}", status.as_u16()),
                None,
            ));
        }
        let payload: JsonValue = serde_json::from_str(&body)
            .map_err(|e| McpError::internal_error(format!("worker fetch decode: {e}"), None))?;

        // GH #31: `system_ref` (by-reference delivery) resolution. Absent
        // key ⇒ inline `system` case — pass through byte-for-byte
        // unchanged (debt #1's compatibility boundary; do not touch).
        let Some(system_ref_value) = payload.get("system_ref") else {
            return json_result(&payload);
        };
        let system_ref: mlua_swarm::types::SystemRef =
            match serde_json::from_value(system_ref_value.clone()) {
                Ok(sr) => sr,
                Err(e) => {
                    return json_result(&serde_json::json!({
                        "ok": false,
                        "stage": "download",
                        "error": format!("system_ref decode: {e}"),
                    }));
                }
            };
        let attempt = payload.get("attempt").and_then(|v| v.as_u64()).unwrap_or(0);

        let mut bytes = match fetch_system_ref_bytes(&client, base, &system_ref).await {
            Ok(b) => b,
            Err(e) => {
                return json_result(&serde_json::json!({
                    "ok": false,
                    "stage": "download",
                    "error": e,
                }));
            }
        };
        use sha2::Digest;
        let mut sha256_hex = hex::encode(sha2::Sha256::digest(&bytes));
        if sha256_hex != system_ref.sha256 {
            // One retry on mismatch, per Acceptance Criteria.
            bytes = match fetch_system_ref_bytes(&client, base, &system_ref).await {
                Ok(b) => b,
                Err(e) => {
                    return json_result(&serde_json::json!({
                        "ok": false,
                        "stage": "download",
                        "error": e,
                    }));
                }
            };
            sha256_hex = hex::encode(sha2::Sha256::digest(&bytes));
            if sha256_hex != system_ref.sha256 {
                return json_result(&serde_json::json!({
                    "ok": false,
                    "stage": "hash_mismatch",
                    "error": format!(
                        "sha256 mismatch after 1 retry: expected {}, got {}",
                        system_ref.sha256, sha256_hex
                    ),
                }));
            }
        }

        let write_path = req
            .system_ref_path
            .clone()
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                std::env::temp_dir().join(format!("{}-{}.md", task_id.as_str(), attempt))
            });
        if let Err(e) = tokio::fs::write(&write_path, &bytes).await {
            return json_result(&serde_json::json!({
                "ok": false,
                "stage": "write",
                "error": format!("write {}: {e}", write_path.display()),
            }));
        }
        let readback = match tokio::fs::read(&write_path).await {
            Ok(rb) => rb,
            Err(e) => {
                return json_result(&serde_json::json!({
                    "ok": false,
                    "stage": "write",
                    "error": format!("readback {}: {e}", write_path.display()),
                }));
            }
        };
        if readback != bytes {
            return json_result(&serde_json::json!({
                "ok": false,
                "stage": "write",
                "error": format!(
                    "readback mismatch at {}: wrote {} bytes, read back {}",
                    write_path.display(),
                    bytes.len(),
                    readback.len()
                ),
            }));
        }

        // Success: pass the original payload through verbatim, plus a
        // top-level `system_ref_resolution` companion field. `ok: true`
        // here means "file written to disk intact" only — it does NOT
        // mean the caller has loaded the content into an LLM context
        // (see the "Prompt delivery modes" guide section).
        let mut out = payload.clone();
        if let Some(obj) = out.as_object_mut() {
            obj.insert(
                "system_ref_resolution".to_string(),
                serde_json::json!({
                    "ok": true,
                    "path": write_path.display().to_string(),
                    "sha256": sha256_hex,
                    "size_bytes": bytes.len(),
                }),
            );
        }
        json_result(&out)
    }

    // convention-token-ok: mse_pending_wait is a mlua-swarm public MCP tool name.
    #[tool(
        description = "Worker-side submit: POST <base_url>/v1/worker/submit with `Authorization: Bearer <worker_handle>` and the raw `body` as text/plain (task_id is resolved server-side from the Bearer). Normally `worker_handle` + `body` are the ONLY required params — base_url auto-resolves from the route this process recorded when the Spawn frame passed through mse_pending_wait; pass it explicitly to override (or when the Bearer is a full capability_token). Optional ok=false marks the attempt failed (flow.ir Try catch path); mutually exclusive with `name`. Optional `name` (GH #36) stages ONE named output part instead of completing the attempt — POST /v1/worker/artifact?name=<name> — call again (same or different name) for more parts, then finish with a plain (no-name) call; the step's final output becomes {\"out\": <final submit body>, \"parts\": {<name>: <value>, ...}}, read downstream via bracket notation e.g. \"$.<step>.parts[\\\"plan.md\\\"]\". Optional `degradations` array (GH #32) — each entry POSTed to /v1/worker/degradation before the main submit, structured tool-failure trace persisted on the Run record. Backward compat: absent field = pre-#32 behavior. Optional `stats` object ({worker_kind?, model?, usage?: {input_tokens?, output_tokens?, total_tokens?}, num_turns?, adapter_data?}; each token field is optional — report just `total_tokens` when that is all the worker knows, and the splits read as 0) — POSTed to /v1/worker/stats after the degradations and before the submit, so the dispatcher's outcome fold lands it on the attempt's StepEntry; report it on the FINAL (no-name) submit, since stats arriving after the fold are dropped. Aggregate a whole run's reports with swarm_run_stats. Expects HTTP 204 and returns {submitted: true} (name path) or {submitted: true} (plain path); any other status is an error. Pure-MCP replacement for the wrapper agents' Bash curl step — no shell involved."
    )]
    async fn mse_worker_submit(
        &self,
        Parameters(req): Parameters<WorkerSubmitReq>,
    ) -> Result<CallToolResult, McpError> {
        if req.name.is_some() && req.ok == Some(false) {
            return Err(McpError::invalid_params(
                "name and ok=false are mutually exclusive: `name` stages one named output \
                 part (POST /v1/worker/artifact — no pass/fail state of its own), `ok=false` \
                 marks the whole attempt failed via POST /v1/worker/submit — pass one or the \
                 other, not both"
                    .to_string(),
                None,
            ));
        }
        let base_url = match req.base_url {
            Some(b) => b,
            None => self
                .op_client
                .worker_route(&req.worker_handle)
                .await
                .map(|r| r.base_url)
                .ok_or_else(|| {
                    McpError::invalid_params(
                        "base_url not given and no Spawn route is recorded for this \
                         worker_handle — pass base_url explicitly (routes are recorded when \
                         the Spawn frame is popped via mse_pending_wait in this process)"
                            .to_string(),
                        None,
                    )
                })?,
        };
        let client = crate::http::client_builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| McpError::internal_error(format!("client build: {e}"), None))?;

        // GH #32: pre-submit degradation reporting. Each entry is
        // POSTed to `/v1/worker/degradation` BEFORE the submit/artifact
        // call below (serial, in append order — not parallelized, since
        // ordering matters for the append semantics and the POSTs are
        // cheap). Independent channel from `body`/`name` — never folded
        // into step OUTPUT. A non-204 response fails loud: the caller
        // opted into structured degradation reporting, so it must not be
        // silently swallowed.
        if let Some(entries) = req.degradations.filter(|v| !v.is_empty()) {
            let deg_url = worker_degradation_endpoint_url(&base_url)
                .map_err(|e| McpError::invalid_params(format!("invalid base_url: {e}"), None))?;
            for entry in entries {
                let resp = client
                    .post(deg_url.clone())
                    .header("Authorization", format!("Bearer {}", req.worker_handle))
                    .header("Content-Type", "application/json")
                    .json(&entry)
                    .send()
                    .await
                    .map_err(|e| {
                        McpError::internal_error(format!("worker degradation: {e}"), None)
                    })?;
                let status = resp.status();
                if status != reqwest::StatusCode::NO_CONTENT {
                    let body = resp.text().await.unwrap_or_default();
                    return Err(McpError::internal_error(
                        format!(
                            "worker degradation: HTTP {} (expected 204) — {body}",
                            status.as_u16()
                        ),
                        None,
                    ));
                }
            }
        }

        // Pre-submit stats reporting, on the same observational plane as
        // the degradation POSTs above and ordered after them. The
        // dispatcher drains recorded stats at outcome time — which the
        // submit POST below triggers — so the report has to land first or
        // it is dropped with the attempt's cleanup. A non-204 response
        // fails loud for the same reason as the degradation path: the
        // caller opted in, so a lost report must not be silent.
        if let Some(stats) = &req.stats {
            let stats_url = worker_stats_endpoint_url(&base_url)
                .map_err(|e| McpError::invalid_params(format!("invalid base_url: {e}"), None))?;
            let resp = client
                .post(stats_url)
                .header("Authorization", format!("Bearer {}", req.worker_handle))
                .header("Content-Type", "application/json")
                .json(stats)
                .send()
                .await
                .map_err(|e| McpError::internal_error(format!("worker stats: {e}"), None))?;
            let status = resp.status();
            if status != reqwest::StatusCode::NO_CONTENT {
                let body = resp.text().await.unwrap_or_default();
                return Err(McpError::internal_error(
                    format!(
                        "worker stats: HTTP {} (expected 204) — {body}",
                        status.as_u16()
                    ),
                    None,
                ));
            }
        }

        let url = worker_submit_endpoint_url(&base_url, req.name.as_deref())
            .map_err(|e| McpError::invalid_params(format!("invalid base_url: {e}"), None))?;
        let mut request = client
            .post(url)
            .header("Authorization", format!("Bearer {}", req.worker_handle))
            .header("Content-Type", "text/plain");
        if req.name.is_none() && req.ok == Some(false) {
            request = request.query(&[("ok", "false")]);
        }
        let resp = request
            .body(req.body)
            .send()
            .await
            .map_err(|e| McpError::internal_error(format!("worker submit: {e}"), None))?;
        let status = resp.status();
        if status != reqwest::StatusCode::NO_CONTENT {
            let body = resp.text().await.unwrap_or_default();
            return Err(McpError::internal_error(
                format!(
                    "worker submit: HTTP {} (expected 204) — {body}",
                    status.as_u16()
                ),
                None,
            ));
        }
        json_result(&serde_json::json!({ "submitted": true }))
    }

    #[tool(
        description = "Run a Blueprint via TaskApplication.handle. Blocking by default (returns run_id + final_ctx + bound_version on completion; the whole `final_ctx` is always written to a file and `ctx_file` {path, bytes} says where — this MCP runs on the caller's own machine, so nothing is trimmed away to fit a response. `final_ctx` is inlined as well when it is under 16 KiB; above that it is `null` and `ctx_file.note` says to read the file. Read it with `mse_run_ctx(run_id, at)` when you would rather not open a file yourself — it selects one branch (`$.aggregate.out`) instead of handing back the whole ctx. Nothing partial is ever returned under the name `final_ctx`, and a write that failed reports `ctx_file.error` with a null path rather than naming a file that is not there); pass `detach: true` for the asynchronous launch — returns `{run_id, task_id, status: \"running\"}` immediately, poll `swarm_status` for the result. `blueprint` accepts a BlueprintSelector `{kind: \"inline\"|\"id\"|\"file\", ...}` or, for backward compat, a bare Blueprint object (treated as inline). On the `{kind: \"id\"}` path the run is pinned to an Operator session: `operator_sid` when given, otherwise this process's sole live session (joined via mse_operator_join, and only when the run targets that session's server) — so with several drivers sharing one server, a run's Spawn frames come back to the driver that launched it instead of to whichever session currently holds the Blueprint's logical operator role."
    )]
    async fn swarm_run(
        &self,
        Parameters(req): Parameters<SwarmRunReq>,
    ) -> Result<CallToolResult, McpError> {
        // R-<hex> RunId (issue #13): the in-process path traces into the
        // local run store under this id; the HTTP proxy path re-keys to the
        // server-minted run_id once the response arrives.
        let run_id_typed = RunId::new();
        let run_id = run_id_typed.to_string();
        let ttl = Duration::from_secs(req.timeout_secs.unwrap_or(300));
        let detach = req.detach.unwrap_or(false);

        // Normalize BlueprintInput → BlueprintSelector.
        let selector = match req.blueprint {
            BlueprintInput::Selector(s) => s,
            BlueprintInput::BareInline(v) => BlueprintSelector::Inline { blueprint: v },
        };

        // Id kind: proxy POST /v1/tasks. Uses the server-side store; the
        // in-process store dedicated to Inline is not consulted.
        if let BlueprintSelector::Id { id, bind } = &selector {
            // Run-scoped Operator pin: explicit param first; otherwise this
            // process's sole live session, but only when the run targets the
            // very server that session is joined to (a sid means nothing on
            // another server, and auto-pinning one there would turn a
            // working launch into a 400).
            let operator_pin = match req.operator_sid {
                Some(sid) => Some(OperatorPin::explicit(sid, req.operator_slot.clone())),
                None => match self.op_client.sole_live_sid().await {
                    Some(sid)
                        if auto_pin_targets_joined_server(
                            self.op_client.http_base(),
                            bind.as_deref(),
                        ) =>
                    {
                        Some(OperatorPin::auto(sid, req.operator_slot.clone()))
                    }
                    _ => None,
                },
            };
            return self
                .swarm_run_via_http(
                    run_id,
                    id.clone(),
                    bind.clone(),
                    req.init_ctx,
                    ttl,
                    req.operator_id,
                    req.operator_kind,
                    req.operator_kind_overrides,
                    detach,
                    req.ttl_secs,
                    operator_pin,
                )
                .await;
        }

        // Inline / File run inside this process, whose engine has no
        // Operator session registry at all — a pin here could never be
        // honored. Say so instead of accepting the argument and quietly
        // running unpinned: a caller that asked for a specific session must
        // not be told "done" by a path that cannot deliver there.
        if req.operator_sid.is_some() {
            return Err(McpError::invalid_params(
                "operator_sid pins a run to an Operator session on `mse serve`, so it only \
                 applies to the {kind: \"id\"} selector; an inline / file Blueprint runs \
                 inside this mcp process, which holds no Operator sessions. Register the \
                 Blueprint on the server and launch it by id to use the pin."
                    .to_string(),
                None,
            ));
        }
        // `operator_slot` names the seat a pin fills, so it is the same
        // argument by another half: rejected here for the same reason,
        // rather than accepted and dropped on a path that pins nothing.
        if req.operator_slot.is_some() {
            return Err(McpError::invalid_params(
                "operator_slot names the Blueprint-declared Operator seat an operator_sid pin \
                 assigns, so it only applies to the {kind: \"id\"} selector; an inline / file \
                 Blueprint runs inside this mcp process, which pins nothing. Register the \
                 Blueprint on the server and launch it by id to use the pin."
                    .to_string(),
                None,
            ));
        }

        // Minted here (rather than just before `run_store.create` below) so
        // the initial `RunHandle` insert already carries it — `mse_doctor`'s
        // `audit_findings` scan (GH #34) addresses the steps API by
        // `task_id`, and in-process runs are its only source until the
        // dispatch below finishes.
        let task_id_typed = TaskId::new();

        let (task_app, run_store, run_trace_store) = {
            let mut inner = self.state.write().await;
            inner.runs.insert(
                run_id.clone(),
                RunHandle {
                    run_id: run_id.clone(),
                    status: RunStatus::Running,
                    task_id: Some(task_id_typed.to_string()),
                    cancel_requested: false,
                },
            );
            (
                inner.task_app.clone(),
                inner.run_store.clone(),
                inner.run_trace_store.clone(),
            )
        };

        // Resolve Inline / File → Blueprint JSON.
        let bp_json: JsonValue = match selector {
            BlueprintSelector::Inline { blueprint } => blueprint,
            BlueprintSelector::File { path } => match read_blueprint_from_file(&path) {
                Ok(v) => v,
                Err(msg) => {
                    let body = serde_json::json!({
                        "run_id": run_id,
                        "status": "failed",
                        "error": msg,
                    });
                    let mut inner = self.state.write().await;
                    if let Some(h) = inner.runs.get_mut(&run_id) {
                        h.status = RunStatus::Failed;
                    }
                    drop(inner);
                    return json_result(&body);
                }
            },
            BlueprintSelector::Id { .. } => unreachable!("Id handled above"),
        };

        let blueprint: Blueprint = match serde_json::from_value(bp_json) {
            Ok(b) => b,
            Err(e) => {
                let body = serde_json::json!({
                    "run_id": run_id,
                    "status": "failed",
                    "error": format!(
                        "blueprint decode failed: {} (hint: call the bp_schema tool for the Blueprint JSON Schema)",
                        e
                    ),
                });
                let mut inner = self.state.write().await;
                if let Some(h) = inner.runs.get_mut(&run_id) {
                    h.status = RunStatus::Failed;
                }
                drop(inner);
                return json_result(&body);
            }
        };
        let bp_id = blueprint.id.clone();

        // "Runtime Global" tier: `Some(_)` — including `Some(Automate)` — is
        // always an explicit request that outranks the BP-level tiers; an
        // absent/unset `operator_kind` stays `None`, leaving the BP-level
        // tiers (`OperatorDef.kind` / `Blueprint.default_operator_kind`) to
        // decide instead of eagerly defaulting to `Automate`.
        let operator_kind = req
            .operator_kind
            .as_deref()
            .map(parse_operator_kind_str)
            .transpose()?;
        let mut operator_kind_overrides: HashMap<String, OperatorKind> = HashMap::new();
        for (agent, kind_str) in req.operator_kind_overrides.unwrap_or_default() {
            operator_kind_overrides.insert(agent, parse_operator_kind_str(&kind_str)?);
        }

        let input = TaskApplicationInput {
            blueprint: BlueprintRef::Inline {
                value: Box::new(blueprint),
            },
            operator_id: req.operator_id.unwrap_or_else(|| "mcp-run".into()),
            role: Role::Operator,
            ttl,
            init_ctx: req.init_ctx.unwrap_or_else(|| serde_json::json!({})),
            operator_kind,
            bridge_id: None,
            hook_id: None,
            // The in-process path has no Operator session registry to name
            // (an explicit `operator_sid` was rejected above), and no
            // delegate backend registered either.
            operator_sid: None,
            operator_kind_overrides,
            task_input: None,
            // Local MCP run path does not expose a check_policy override;
            // `None` preserves the server-wide default (backward compat).
            check_policy: None,
        };

        // Trace this kick in the local run store (in-memory; issue #13).
        // The stdio adapter has no TaskStore, so the work-item id is minted
        // ad hoc (above) — it groups re-runs only within this process's
        // lifetime.
        let now = now_secs();
        let run_ctx = match run_store
            .create(RunRecord {
                id: run_id_typed.clone(),
                task_id: task_id_typed.clone(),
                status: StoreRunStatus::Running,
                step_entries: Vec::new(),
                degradations: Vec::new(),
                operator_sid: None,
                current: Default::default(),
                next_generation: 0,
                result_ref: None,
                input_json: None,
                created_at: now,
                updated_at: now,
            })
            .await
        {
            Ok(()) => {
                let trace = mlua_swarm::store::trace::TraceHandle::new(
                    run_id_typed.clone(),
                    run_trace_store.clone(),
                );
                trace
                    .append(
                        mlua_swarm::store::trace::kind::RUN_STARTED,
                        None,
                        None,
                        serde_json::json!({"mode": "launch"}),
                    )
                    .await;
                Some(RunContext::new(run_id_typed.clone(), run_store.clone()).with_trace(trace))
            }
            // A trace-store failure must not block the run itself.
            Err(_) => None,
        };

        // GH #37 detached launch (in-process path): the eval runs in its
        // own spawned task bounded by `ttl` alone; the spawned task owns
        // finalizing both the local run trace and the `RunHandle`, and the
        // tool returns `{run_id, task_id, status: "running"}` immediately.
        // Poll `swarm_status` for the terminal status and result.
        if detach {
            let state_bg = self.state.clone();
            let run_id_bg = run_id.clone();
            let run_id_typed_bg = run_id_typed.clone();
            let run_store_bg = run_store.clone();
            // Panic guard — see `catch_in_process_run_panic`.
            let guard_state = self.state.clone();
            let guard_run_id = run_id.clone();
            let guard_run_id_typed = run_id_typed.clone();
            let guard_run_store = run_store.clone();
            tokio::spawn(async move {
                let driver = async move {
                    let result =
                        tokio::time::timeout(ttl, task_app.handle_with_run(input, run_ctx)).await;
                    let (status, store_status, final_ctx) = match result {
                        Ok(Ok(out)) => (RunStatus::Done, StoreRunStatus::Done, Some(out.final_ctx)),
                        Ok(Err(_)) | Err(_) => (RunStatus::Failed, StoreRunStatus::Failed, None),
                    };
                    let _ = run_store_bg
                        .update_status(&run_id_typed_bg, store_status)
                        .await;
                    if let Some(fc) = final_ctx {
                        let _ = run_store_bg.set_result(&run_id_typed_bg, fc).await;
                    }
                    let mut inner = state_bg.write().await;
                    if let Some(h) = inner.runs.get_mut(&run_id_bg) {
                        h.status = status;
                    }
                };
                let _ = catch_in_process_run_panic(
                    &guard_state,
                    &guard_run_store,
                    &guard_run_id,
                    &guard_run_id_typed,
                    "mcp.launch.detach",
                    driver,
                )
                .await;
            });
            return json_result(&serde_json::json!({
                "run_id": run_id,
                "task_id": task_id_typed,
                "status": "running",
                "detached": true,
            }));
        }

        // Driver-lifetime fix: the driver runs in its own spawned task — same
        // shape as the detached branch above — and this tool call only
        // awaits its report over a `oneshot`. An aborted tool call
        // therefore drops the wait, not the run: the driver still reaches
        // its terminal store write, so a `/v1/worker/submit` arriving
        // after the abort has a driver left to fold it into. The TTL
        // ceiling stays inside the driver, so no second wait bound is
        // needed here.
        //
        // Panic guard — see `catch_in_process_run_panic`. A panicking driver
        // yields a structured `failed` tool response instead of unwinding the
        // tool call and leaving the Run pinned at `running`.
        let (tx, rx) = tokio::sync::oneshot::channel::<SyncRunReport>();
        let state_bg = self.state.clone();
        let run_id_bg = run_id.clone();
        let run_id_typed_bg = run_id_typed.clone();
        let run_store_bg = run_store.clone();
        let guard_state = self.state.clone();
        let guard_run_id = run_id.clone();
        let guard_run_id_typed = run_id_typed.clone();
        let guard_run_store = run_store.clone();
        tokio::spawn(async move {
            let driver = async move {
                let result =
                    tokio::time::timeout(ttl, task_app.handle_with_run(input, run_ctx)).await;
                let (status, store_status, report) = match result {
                    Ok(Ok(out)) => (
                        RunStatus::Done,
                        StoreRunStatus::Done,
                        SyncRunReport::Done(Box::new(out)),
                    ),
                    Ok(Err(e)) => (
                        RunStatus::Failed,
                        StoreRunStatus::Failed,
                        SyncRunReport::Failed(e.to_string()),
                    ),
                    Err(_) => (
                        RunStatus::Failed,
                        StoreRunStatus::Failed,
                        SyncRunReport::Failed(format!("timeout after {}s", ttl.as_secs())),
                    ),
                };
                // Finalize the local run trace (best effort; the wire
                // response is authoritative for the caller).
                let _ = run_store_bg
                    .update_status(&run_id_typed_bg, store_status)
                    .await;
                if let SyncRunReport::Done(out) = &report {
                    let _ = run_store_bg
                        .set_result(&run_id_typed_bg, out.final_ctx.clone())
                        .await;
                }
                {
                    let mut inner = state_bg.write().await;
                    if let Some(h) = inner.runs.get_mut(&run_id_bg) {
                        h.status = status;
                    }
                }
                report
            };
            let report = match catch_in_process_run_panic(
                &guard_state,
                &guard_run_store,
                &guard_run_id,
                &guard_run_id_typed,
                "mcp.launch.sync",
                driver,
            )
            .await
            {
                Ok(report) => report,
                Err(message) => SyncRunReport::Aborted(format!(
                    "run driver panicked: {message}; the run was marked Interrupted"
                )),
            };
            // An aborted tool call leaves no receiver; the run is already
            // finalized, so the undeliverable report is dropped.
            let _ = tx.send(report);
        });

        // A receive error only happens if the spawned task died without
        // reporting — a panic outside the guard, or a runtime shutdown.
        let report = rx.await.unwrap_or_else(|_| {
            SyncRunReport::Aborted(
                "run driver task ended without reporting an outcome; poll swarm_status for the \
                 run's persisted state"
                    .to_string(),
            )
        });
        let outcome = match report {
            SyncRunReport::Done(out) => Ok(out),
            SyncRunReport::Failed(error) => Err(error),
            SyncRunReport::Aborted(error) => {
                return json_result(&serde_json::json!({
                    "run_id": run_id,
                    "task_id": task_id_typed,
                    "status": "failed",
                    "error": error,
                }));
            }
        };

        // Post-action store snapshot. Inline mode does not write to the
        // store, so head=None / history_len=0 is the default; once the Id
        // mode path lands, head + history become populated.
        let store = {
            let inner = self.state.read().await;
            inner.store.clone()
        };
        let head_id: Option<String> = match store.read_head(&bp_id).await {
            Ok(_traced) => Some(bp_id.to_string()),
            Err(_) => None,
        };
        let history_len: usize = store
            .history(&bp_id, 100)
            .await
            .map(|v| v.len())
            .unwrap_or(0);
        // log_tail: the tail of the in-process RunTrace stream (per-step
        // run stats — `core.*` dispatch events + any middleware/worker
        // kinds). Field name kept for wire compat with pre-trace callers
        // (it used to be a hardcoded empty array).
        let log_tail: Vec<JsonValue> = run_trace_store
            .list(
                &run_id_typed,
                &mlua_swarm::store::trace::TraceQuery {
                    latest: Some(50),
                    ..Default::default()
                },
            )
            .await
            .map(|events| {
                events
                    .into_iter()
                    .filter_map(|e| serde_json::to_value(e).ok())
                    .collect()
            })
            .unwrap_or_default();

        // The Run's own terminal state (`RunStore` + local `RunHandle`) was
        // persisted by the driver task; what is left here is purely the
        // wire body the caller sees.
        let body = match outcome {
            Ok(out) => {
                let ctx = run_ctx_report(&mse_home(), out.final_ctx, &run_id.to_string());
                serde_json::json!({
                "run_id": run_id,
                "task_id": task_id_typed,
                "status": "done",
                "final_ctx": ctx["final_ctx"],
                "ctx_file": ctx["ctx_file"],
                "bound_version": out.bound_version.map(|v| format!("{:?}", v)),
                "head": head_id,
                "history_len": history_len,
                "log_tail": log_tail,
                })
            }
            Err(error) => serde_json::json!({
                "run_id": run_id,
                "task_id": task_id_typed,
                "status": "failed",
                "error": error,
                "head": head_id,
                "history_len": history_len,
                "log_tail": log_tail,
            }),
        };
        json_result(&body)
    }

    /// Proxy `swarm_run(kind=id)` to `POST /v1/tasks` on the mse serve
    /// process. The registered Blueprint lives in the server-side store,
    /// so this cannot be resolved locally.
    #[allow(clippy::too_many_arguments)]
    async fn swarm_run_via_http(
        &self,
        run_id: String,
        id: String,
        bind: Option<String>,
        init_ctx: Option<JsonValue>,
        ttl: Duration,
        operator_id: Option<String>,
        operator_kind: Option<String>,
        operator_kind_overrides: Option<HashMap<String, String>>,
        detach: bool,
        ttl_override: Option<u64>,
        operator_pin: Option<OperatorPin>,
    ) -> Result<CallToolResult, McpError> {
        {
            let mut inner = self.state.write().await;
            inner.runs.insert(
                run_id.clone(),
                RunHandle {
                    run_id: run_id.clone(),
                    status: RunStatus::Running,
                    // Not known yet — the server mints/reports it in the
                    // POST /v1/tasks response body, parsed below.
                    task_id: None,
                    cancel_requested: false,
                },
            );
        }

        let bind = bind.unwrap_or_else(|| crate::http::Endpoint::resolve(None).base().to_string());
        let url = crate::http::Endpoint::resolve(Some(&bind)).url("/v1/tasks");

        let mut operator_obj = serde_json::Map::new();
        if let Some(k) = operator_kind {
            operator_obj.insert("kind".into(), JsonValue::String(k));
        }
        if let Some(id) = operator_id {
            operator_obj.insert("id".into(), JsonValue::String(id));
        }
        if let Some(map) = operator_kind_overrides {
            if !map.is_empty() {
                operator_obj.insert(
                    "per_agent_kinds".into(),
                    serde_json::to_value(map).unwrap_or(JsonValue::Null),
                );
            }
        }

        let mut payload = serde_json::Map::new();
        payload.insert(
            "blueprint".into(),
            serde_json::json!({ "kind": "id", "id": id }),
        );
        payload.insert(
            "init_ctx".into(),
            init_ctx.unwrap_or_else(|| serde_json::json!({})),
        );
        // Explicit `ttl_secs` (detach lifetime override) wins over the
        // shortcut of `timeout_secs`-as-TTL — detach and sync are
        // bounded by different clocks, so the caller gets a distinct
        // knob for each.
        payload.insert(
            "ttl_secs".into(),
            JsonValue::from(ttl_override.unwrap_or(ttl.as_secs())),
        );
        if detach {
            // GH #37: opt the server into the detached launch — it answers
            // `202 {run_id, task_id, status: "running", final_ctx: null}`
            // immediately; the `status` field is folded into the response
            // parsing below.
            payload.insert("detach".into(), JsonValue::Bool(true));
        }
        if !operator_obj.is_empty() {
            payload.insert("operator".into(), JsonValue::Object(operator_obj));
        }
        // Run-scoped Operator pin (explicit param or this process's sole
        // live session): the server binds the whole Spawn stream of this run
        // to that session instead of resolving the Blueprint's logical role
        // through whichever session happens to hold it. Absent field = the
        // pre-pin wire body, byte-for-byte.
        //
        // The pin is the Run's first `Assign`, so the server requires the
        // `desc` that goes with it (model §4.3 A9 — a `400` without one).
        // `OperatorPin` carries a `desc` that distinguishes the two ways
        // this process arrives at a sid, because "which one was it" is
        // exactly the question a reader of the Run has later.
        //
        // `operator_slot` rides along only when the caller named a seat:
        // the server treats an absent slot as "the Blueprint declares one
        // Operator, so it is that one", and answers `400` with the
        // candidates when it declares several. Sending a guess instead
        // would be this process inventing a lane.
        if let Some(pin) = operator_pin {
            payload.insert("operator_sid".into(), JsonValue::String(pin.sid));
            payload.insert("operator_desc".into(), JsonValue::String(pin.desc));
            if let Some(slot) = pin.slot {
                payload.insert("operator_slot".into(), JsonValue::String(slot));
            }
        }

        let client = match crate::http::client_builder()
            .timeout(ttl + Duration::from_secs(5))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                let mut inner = self.state.write().await;
                if let Some(h) = inner.runs.get_mut(&run_id) {
                    h.status = RunStatus::Failed;
                }
                drop(inner);
                return json_result(&serde_json::json!({
                    "run_id": run_id,
                    "status": "failed",
                    "error": format!("client build: {e}"),
                }));
            }
        };

        let resp = match client.post(&url).json(&payload).send().await {
            Ok(r) => r,
            Err(e) => {
                let mut inner = self.state.write().await;
                if let Some(h) = inner.runs.get_mut(&run_id) {
                    h.status = RunStatus::Failed;
                }
                drop(inner);
                return json_result(&serde_json::json!({
                    "run_id": run_id,
                    "status": "failed",
                    "error": format!("POST {url} failed: {e} (is mse serve running at {bind}?)"),
                }));
            }
        };
        let http_status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        // On success the server response is the id authority (issue #13):
        // adopt its run_id / task_id instead of the locally minted
        // placeholder, so the caller-visible run_id matches what
        // GET /v1/runs/:id on the server will resolve.
        let mut effective_run_id = run_id.clone();
        // GH #34: `mse_doctor`'s `audit_findings` scan addresses the steps
        // API by `task_id` — capture the server-minted one alongside
        // `effective_run_id` so the tracked `RunHandle` carries it.
        let mut effective_task_id: Option<String> = None;
        let (final_status, body) = if http_status.is_success() {
            let parsed: JsonValue =
                serde_json::from_str(&text).unwrap_or_else(|_| JsonValue::String(text.clone()));
            if let Some(sid) = parsed.get("run_id").and_then(|v| v.as_str()) {
                effective_run_id = sid.to_string();
            }
            effective_task_id = parsed
                .get("task_id")
                .and_then(|v| v.as_str())
                .map(String::from);
            // GH #37: the server reports the launch outcome in `status` —
            // `"done"` for the synchronous path, `"running"` for a
            // detached (`202 Accepted`) launch. Absent (pre-#37 server)
            // means the old always-synchronous behavior: done.
            let status_str = parsed
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("done")
                .to_string();
            let ctx = run_ctx_report(
                &mse_home(),
                parsed.get("final_ctx").cloned().unwrap_or(JsonValue::Null),
                &effective_run_id,
            );
            (
                if status_str == "running" {
                    RunStatus::Running
                } else {
                    RunStatus::Done
                },
                serde_json::json!({
                    "run_id": effective_run_id.clone(),
                    "task_id": parsed.get("task_id").cloned().unwrap_or(JsonValue::Null),
                    "status": status_str,
                    "final_ctx": ctx["final_ctx"],
                    "ctx_file": ctx["ctx_file"],
                    "bound_version": parsed.get("bound_version").cloned().unwrap_or(JsonValue::Null),
                    "effective_ttl_secs": parsed.get("effective_ttl_secs").cloned().unwrap_or(JsonValue::Null),
                    "ttl_source": parsed.get("ttl_source").cloned().unwrap_or(JsonValue::Null),
                    // Model §5's launch announcement, passed through
                    // verbatim: who the server seated for this Run and
                    // which project_root / work_dir it is bound to. This
                    // proxy is the surface that announcement has to reach —
                    // there is no `mse launch` subcommand, so `swarm_run`'s
                    // return IS the launch output an AI reads. Forwarded as
                    // a whole rather than field by field so the `note` (it
                    // is an announcement, not a guarantee) cannot be
                    // dropped on the way. `null` from a server that predates
                    // it.
                    "info": parsed.get("info").cloned().unwrap_or(JsonValue::Null),
                    "head": id,
                    "resolved_via": "http",
                }),
            )
        } else {
            (
                RunStatus::Failed,
                serde_json::json!({
                    "run_id": run_id,
                    "status": "failed",
                    "error": format!("POST {url} returned {}: {}", http_status.as_u16(), text),
                    "resolved_via": "http",
                }),
            )
        };
        {
            let mut inner = self.state.write().await;
            if effective_run_id != run_id {
                // Re-key the handle to the server-minted run_id.
                inner.runs.remove(&run_id);
                inner.runs.insert(
                    effective_run_id.clone(),
                    RunHandle {
                        run_id: effective_run_id.clone(),
                        status: final_status,
                        task_id: effective_task_id,
                        cancel_requested: false,
                    },
                );
            } else if let Some(h) = inner.runs.get_mut(&run_id) {
                h.status = final_status;
                if effective_task_id.is_some() {
                    h.task_id = effective_task_id;
                }
            }
        }
        json_result(&body)
    }

    #[tool(
        description = "Peek at a known run by run_id. Returns a status snapshot enriched, best-effort, from three sources in this order: (1) the local `RunHandle` (in-process detach runs update this handle when they finish); (2) `GET /v1/runs/:id` on the server bind (GH #67 — folds `status` / `step_entries` / `result_ref` for `detach: true` runs whose completion the local handle never observed, and the tool updates the local handle to match); (3) the local run store's `RunRecord` (fallback for in-process trace when the server is unreachable). The HTTP lookup uses a short timeout and its failure is silent. Always includes `cancel_requested: bool` from the local handle — flipped by `swarm_cancel` and preserved through the HTTP enrichment even when the server still reports `status: \"running\"` (in-flight abort is v3 carry)."
    )]
    async fn swarm_status(
        &self,
        Parameters(req): Parameters<SwarmStatusReq>,
    ) -> Result<CallToolResult, McpError> {
        let (handle, run_store, run_trace_store) = {
            let inner = self.state.read().await;
            (
                inner.runs.get(&req.run_id).cloned(),
                inner.run_store.clone(),
                inner.run_trace_store.clone(),
            )
        };
        let Some(h) = handle else {
            return Err(McpError::invalid_params(
                format!("run_id not found: {}", req.run_id),
                None,
            ));
        };
        let mut body = serde_json::json!({
            "run_id": h.run_id,
            "status": h.status,
            // Local cancel-request mark (issue 9b3f225b): flipped by
            // `swarm_cancel`, independent from `status` so the HTTP
            // enrichment below cannot overwrite it. In-flight handle
            // abort remains v3 carry.
            "cancel_requested": h.cancel_requested,
        });
        if let Some(task_id) = &h.task_id {
            body["task_id"] = serde_json::json!(task_id);
        }

        // GH #67: HTTP-proxied `detach: true` runs never update the local
        // handle after the initial 202 (the tool does not spawn a polling
        // task), so the local `h.status` sits at `Running` forever. Poll
        // the server's `GET /v1/runs/:id`, which reads the same
        // `SqliteRunStore` the run's finalizer wrote its terminal state to,
        // and fold the authoritative view over the local snapshot. In-process
        // detach runs also gain the enrichment (their handle transitions to
        // Done on its own, so the poll is redundant but harmless).
        let bind = req
            .bind
            .unwrap_or_else(|| crate::http::Endpoint::resolve(None).base().to_string());
        let http_body: Option<JsonValue> = fetch_run_via_http(&bind, &req.run_id).await;
        if let Some(server_body) = http_body {
            // Server is the id authority; overwrite the fields it knows.
            if let Some(status) = server_body.get("status").cloned() {
                body["status"] = status.clone();
                if let Some(new_status) = status.as_str().and_then(parse_run_status) {
                    let mut inner = self.state.write().await;
                    if let Some(handle) = inner.runs.get_mut(&req.run_id) {
                        handle.status = new_status;
                    }
                }
            }
            for field in ["task_id", "step_entries", "result_ref"] {
                if let Some(v) = server_body.get(field).cloned() {
                    body[field] = v;
                }
            }
            // RunTrace tail (per-step run stats): the server axis's
            // `log_tail` source. Best-effort, silent on error.
            if let Some(events) = fetch_trace_tail_via_http(&bind, &req.run_id).await {
                body["log_tail"] = JsonValue::Array(events);
            }
        } else {
            // Fallback: enrich from the local run store trace (in-process
            // runs — issue #13). Same best-effort behavior as before GH #67.
            if let Ok(rid) = RunId::parse(req.run_id.clone()) {
                if let Ok(rec) = run_store.get(&rid).await {
                    body["task_id"] = serde_json::json!(rec.task_id);
                    body["step_entries"] =
                        serde_json::to_value(&rec.step_entries).unwrap_or(JsonValue::Null);
                }
                if let Ok(events) = run_trace_store
                    .list(
                        &rid,
                        &mlua_swarm::store::trace::TraceQuery {
                            latest: Some(50),
                            ..Default::default()
                        },
                    )
                    .await
                {
                    if !events.is_empty() {
                        body["log_tail"] = serde_json::to_value(events).unwrap_or(JsonValue::Null);
                    }
                }
            }
        }
        json_result(&body)
    }

    #[tool(
        description = "Cost/latency report for one run: reads GET /v1/runs/:id on `bind` and folds its step trace into per-step rows (step_ref / status / attempt / duration_ms / worker_kind / model / usage), whole-run `totals` (input_tokens, output_tokens, total_tokens, duration_ms_sum, steps_with_stats, steps_total) and a `by_model` breakdown (steps + token counts per self-reported model). Works off the run id alone — no local run handle required, so a run launched by another driver session reports fine (that is the difference from swarm_status, which needs a run this process launched and returns a status snapshot rather than an aggregate). Stats are optional at every worker boundary: compare `steps_with_stats` against `steps_total` before reading the totals as the run's full cost, and note `duration_ms_sum` adds up per-step durations, so concurrent (Fanout) steps are each counted in full instead of by wall-clock. Workers report their own usage via mse_worker_submit's `stats` param (or POST /v1/worker/stats directly). An unknown run id is an invalid_params error; an unreachable server is internal_error. Params: `run_id`, `bind?`."
    )]
    async fn swarm_run_stats(
        &self,
        Parameters(req): Parameters<SwarmRunStatsReq>,
    ) -> Result<CallToolResult, McpError> {
        let bind = req
            .bind
            .unwrap_or_else(|| crate::http::Endpoint::resolve(None).base().to_string());
        let record = fetch_run_strict(&bind, &req.run_id)
            .await
            .map_err(|e| match e {
                RunFetchError::NotFound => McpError::invalid_params(
                    format!("run not found: {} (bind {bind})", req.run_id),
                    None,
                ),
                RunFetchError::Transport(msg) => {
                    McpError::internal_error(format!("run stats: {msg}"), None)
                }
            })?;
        let entries = record
            .get("step_entries")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut body = aggregate_run_stats(&entries);
        body["run_id"] = serde_json::json!(req.run_id);
        body["bind"] = serde_json::json!(bind);
        for field in ["task_id", "status"] {
            if let Some(v) = record.get(field).cloned() {
                body[field] = v;
            }
        }
        json_result(&body)
    }

    #[tool(
        description = "Archive a Blueprint (logical soft-delete via marker commit; reversible via bp_unarchive). Appends `archive: true` marker to head, filters id from list_ids default, and hard-rejects downstream resolvers with Archived. Safety: pass confirm=true to execute, otherwise returns dry-run report. Wraps DELETE /v1/blueprints/:id (path preserved for client compat; behavior is archive)."
    )]
    async fn bp_archive(
        &self,
        Parameters(req): Parameters<BpArchiveReq>,
    ) -> Result<CallToolResult, McpError> {
        let bind = req
            .bind
            .unwrap_or_else(|| crate::http::Endpoint::resolve(None).base().to_string());
        if !req.confirm {
            return json_result(&serde_json::json!({
                "status": "dry_run",
                "id": req.id,
                "bind": bind,
                "note": "Pass confirm=true to archive. Reversible via bp_unarchive (marker commit; audit-trail preserved).",
            }));
        }
        let url =
            crate::http::Endpoint::resolve(Some(&bind)).url(&format!("/v1/blueprints/{}", req.id));
        let client = crate::http::client_builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| McpError::internal_error(format!("client build: {e}"), None))?;
        let resp = client
            .delete(&url)
            .send()
            .await
            .map_err(|e| McpError::internal_error(format!("archive: {e}"), None))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        json_result(&serde_json::json!({
            "status": if status.is_success() { "archived" } else { "error" },
            "http_status": status.as_u16(),
            "id": req.id,
            "bind": bind,
            "body": body,
        }))
    }

    #[tool(
        description = "Return the Blueprint JSON Schema (schemars-generated from mlua-swarm-blueprint-schema types). Use it before authoring / registering a BP, or when a register / swarm_run parse error points here. Note: the `flow` field is opaque in the schema (flow.ir Node grammar is owned by the mlua-flow-ir crate). Identical body to the `mse://api/blueprint-schema` resource."
    )]
    async fn bp_schema(
        &self,
        Parameters(_req): Parameters<BpSchemaReq>,
    ) -> Result<CallToolResult, McpError> {
        let body = resources::blueprint_schema_value()
            .map_err(|e| McpError::internal_error(format!("schema serialize: {e}"), None))?;
        json_result(&body)
    }

    #[tool(
        description = "Scaffold a minimal `.bp.lua` from a bundled template with every currently-mandatory field pre-filled (`halted_at` compile-lint default, each operator agent's explicit platform-neutral `ws_operator` Runner, `strict_refs`/`strict_kind`) — the MCP twin of the `mse bp new` CLI (GH #62 Axis A). Templates: `pipeline` (N-stage main-ai with `--stages`), `single` (one-agent one-step), `verdict` (3-stage verdict-gated with retry-through-fixer, fixed shape mirroring `mse://blueprints/samples/07-dsl-pipeline`), `fanout` (N parallel checkers + aggregate stage using `F.fanout` — GH #82; the shape `bp_dsl` previously could not express without `F.raw()`. Each lane dispatches one checker, selected by branching on the bound `$.item`; the homogeneous one-agent variant is `mse://blueprints/samples/10-fanout`). When `out` is set, writes the rendered text to that path (relative resolves against the mse-mcp process CWD) and reports the byte count; when omitted, includes the rendered `.bp.lua` inline as `script`. Failures return `status: \"error\"` with `stage` (`render` for unknown template / rendering, `write_out` for I/O). Guide: `mse://guides/bp-dsl-templates` lists every template + flag surface. Non-goal: fuzzy parsing — the DSL parser stays strict; the fuzzy scope (GH #62 Axis B, lint→patch mapping) is a separate follow-up."
    )]
    async fn bp_new(
        &self,
        Parameters(req): Parameters<BpNewReq>,
    ) -> Result<CallToolResult, McpError> {
        let rendered = match crate::bp::render_template_by_kind(
            &req.template,
            &req.name,
            req.stages.as_deref(),
            req.agent.as_deref(),
            req.operator.as_deref(),
            req.binding.as_deref(),
        ) {
            Ok(s) => s,
            Err(e) => {
                return json_result(&serde_json::json!({
                    "status": "error",
                    "stage": "render",
                    "template": req.template,
                    "name": req.name,
                    "error": format!("{e:#}"),
                }))
            }
        };
        let bytes = rendered.len();
        if let Some(out) = &req.out {
            if let Err(e) = std::fs::write(out, &rendered) {
                return json_result(&serde_json::json!({
                    "status": "error",
                    "stage": "write_out",
                    "template": req.template,
                    "name": req.name,
                    "out": out,
                    "error": e.to_string(),
                }));
            }
            return json_result(&serde_json::json!({
                "status": "scaffolded",
                "template": req.template,
                "name": req.name,
                "out": out,
                "bytes": bytes,
                "guide_ref": "mse://guides/bp-dsl-templates",
            }));
        }
        json_result(&serde_json::json!({
            "status": "scaffolded",
            "template": req.template,
            "name": req.name,
            "bytes": bytes,
            "script": rendered,
            "guide_ref": "mse://guides/bp-dsl-templates",
        }))
    }

    #[tool(
        description = "Build a `.bp.lua` authoring-DSL script into canonical Blueprint JSON and (by default) register it with the running `mse serve` — the MCP twin of the `mse bp build --register` CLI, so a Blueprint can go from Lua script to registered without shelling out. Pipeline: run the script in an embedded Lua VM (`require(\"flow_dsl\")` / `require(\"bp_dsl\")`), best-effort compile-lint the result through the real Compiler (includes the GH #50 verdict-contract lints; reported as `lint: \"skipped: ...\"` — never silently — when `$file`/`$agent_md` refs cannot be resolved relative to the script's own directory, since the server resolves those itself against its `--blueprint-ref-base` at register time), then POST the built JSON to `/v1/blueprints/:id`. The server never runs Lua — JSON stays the canonical wire format; the DSL is an authoring frontend (GH #52). Failures return `status: \"error\"` with a `stage` field (read | build | lint | write_out | register) so an authoring loop can fix the script and re-call. GH #62 Axis B.1: on `stage: \"lint\"` failures whose Compiler message matches a known lint kind (worker-binding-missing / verdict-value-not-in-contract / halted-at-missing), the response also carries `fix_hint: {kind, reason, patch_suggestion, docs_ref}` — a Clippy-style structured recovery hint the caller can render. `fix_hint` is `null` on lint failures without a canonical fix recipe (never a wrong-but-confident hint). Pass `register=false` for a build+lint-only dry run. The built JSON is always written to a file — `out` when given, else `$MSE_HOME/bp/<bp id>.json` — and every successful build (dry run or registered) names it as `blueprint_file {path, bytes}`; the dry run additionally inlines it as `blueprint` when it is under 16 KiB (above that `blueprint` is `null` and `blueprint_file.note` says to read the file — a `strict_embed` build carries every agent's prompt and is the case this is for), a lint error inlines the pre-expansion JSON as `blueprint` for inspection, and a successful register returns `json_bytes` alongside the file. Pass `include` (a list of dirs) to resolve refs the script's own dir / in-bp includes / `MSE_BLUEPRINT_INCLUDES` do not cover, same as the CLI's `--include`. Every successful build also returns authoring_warnings — bp_dsl-level lint lines (e.g. the B.pipeline dead-halt check: pipeline-level halt_on with zero gate-emitting stages), additive and report-only."
    )]
    async fn bp_build(
        &self,
        Parameters(req): Parameters<BpBuildReq>,
    ) -> Result<CallToolResult, McpError> {
        let script_path = std::path::PathBuf::from(&req.script_path);
        let script = match std::fs::read_to_string(&script_path) {
            Ok(s) => s,
            Err(e) => {
                return json_result(&serde_json::json!({
                    "status": "error",
                    "stage": "read",
                    "script_path": req.script_path,
                    "error": e.to_string(),
                }))
            }
        };
        let (bp_value, authoring_warnings) =
            match mlua_swarm_cli::dsl::build_bp_from_script_with_warnings(&script) {
                Ok(v) => v,
                Err(e) => {
                    return json_result(&serde_json::json!({
                        "status": "error",
                        "stage": "build",
                        "script_path": req.script_path,
                        "error": format!("{e:#}"),
                    }))
                }
            };
        // Set under `strict_embed` to the linker's expanded product, which
        // then stands in for `bp_value` at write_out / register / response.
        // This MCP is a stdio server — it runs on the caller's own machine,
        // so it can read the `agent.md` files a remote `mse serve` cannot.
        let mut embedded: Option<serde_json::Value> = None;
        let includes: Vec<std::path::PathBuf> =
            req.include.iter().map(std::path::PathBuf::from).collect();
        let lint = match crate::bp::compile_lint(
            &bp_value,
            &script_path,
            &includes,
            req.strict_embed,
        ) {
            Ok(crate::bp::LintReport::Ok {
                agents,
                operators,
                expanded,
            }) => {
                if req.strict_embed {
                    embedded = Some(*expanded);
                }
                format!("ok ({agents} agent(s), {operators} operator(s) checked)")
            }
            Ok(crate::bp::LintReport::Warn {
                agents,
                operators,
                reason,
                warnings,
            }) => {
                // `strict_embed=true` mirrors the CLI's `--strict-embed`,
                // promoting the unresolved-ref WARN to a hard
                // `stage: "lint"` error so the caller does not proceed
                // to write_out / register with refs still in the wire
                // JSON. Default (false) preserves wire-layer
                // partial-preserve behavior — the server resolves refs
                // at register time.
                if req.strict_embed {
                    return json_result(&serde_json::json!({
                        "status": "error",
                        "stage": "lint",
                        "script_path": req.script_path,
                        "error": format!("strict_embed: {reason}"),
                        "warnings": warnings,
                        "authoring_warnings": authoring_warnings,
                        "fix_hint": serde_json::Value::Null,
                        "blueprint": bp_value,
                    }));
                }
                let warn_lines = if warnings.is_empty() {
                    String::new()
                } else {
                    format!(" [{}]", warnings.join("; "))
                };
                format!(
                    "warn ({agents} agent(s), {operators} operator(s) checked): {reason}{warn_lines}"
                )
            }
            Err(e) => {
                // GH #62 Axis B.1: attach a structured `fix_hint` when
                // the Compiler error matches a known lint kind. `null`
                // otherwise — never a wrong-but-confident hint.
                // GH #79 Phase 2: also attach the unified `diagnostic`
                // (typed `CompileError` → `Diagnostic` projection, no
                // substring re-parse) — additive alongside `fix_hint`,
                // which now derives from the same Diagnostic when the
                // typed path recognizes the failure.
                let msg = format!("{e:#}");
                let diagnostic = crate::bp::diagnostic_for_error(&e);
                let fix_hint = diagnostic
                    .as_ref()
                    .and_then(crate::bp::fix_hint_from_diagnostic)
                    .or_else(|| crate::bp::fix_hint_from_compile_error(&msg))
                    .map(|h| {
                        serde_json::json!({
                            "kind": h.kind,
                            "reason": h.reason,
                            "patch_suggestion": h.patch_suggestion,
                            "docs_ref": h.docs_ref,
                        })
                    });
                let diagnostic_json = diagnostic
                    .as_ref()
                    .map(|d| serde_json::to_value(d).unwrap_or(serde_json::Value::Null))
                    .unwrap_or(serde_json::Value::Null);
                return json_result(&serde_json::json!({
                    "status": "error",
                    "stage": "lint",
                    "script_path": req.script_path,
                    "error": msg,
                    "fix_hint": fix_hint,
                    "diagnostic": diagnostic_json,
                    "authoring_warnings": authoring_warnings,
                    "blueprint": bp_value,
                }));
            }
        };
        let wire = embedded.as_ref().unwrap_or(&bp_value);
        let bp_id = bp_value
            .get("id")
            .and_then(|v| v.as_str())
            .map(String::from);
        // The built JSON always goes to a file (`out`, or a default under
        // `$MSE_HOME/bp/`) and the response names it; an embedded Blueprint
        // is every agent's prompt in one document and does not belong in a
        // tool response. Inline only when small — same contract as
        // `swarm_run`'s `final_ctx` / `ctx_file`.
        let file_stem = bp_id.clone().unwrap_or_else(|| {
            script_path
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| "blueprint".to_string())
        });
        let report = bp_build_report(&mse_home(), wire, &file_stem, req.out.as_deref());
        if let Some(err) = report["blueprint_file"]["error"].as_str() {
            return json_result(&serde_json::json!({
                "status": "error",
                "stage": "write_out",
                "bp_id": bp_id,
                "lint": lint,
                "authoring_warnings": authoring_warnings,
                "out": req.out,
                "error": err,
            }));
        }
        if !req.register {
            return json_result(&serde_json::json!({
                "status": "built",
                "bp_id": bp_id,
                "lint": lint,
                "authoring_warnings": authoring_warnings,
                "out": req.out,
                "blueprint": report["blueprint"],
                "blueprint_file": report["blueprint_file"],
            }));
        }
        let bind = req
            .bind
            .unwrap_or_else(|| crate::http::Endpoint::resolve(None).base().to_string());
        match crate::bp::register(wire, Some(&bind)).await {
            Ok(outcome) => {
                let json_bytes = serde_json::to_vec(wire).map(|v| v.len()).unwrap_or(0);
                json_result(&serde_json::json!({
                    "status": "registered",
                    "bp_id": bp_id,
                    "lint": lint,
                    "authoring_warnings": authoring_warnings,
                    "out": req.out,
                    "url": outcome.url,
                    "http_status": outcome.http_status,
                    "body": outcome.body,
                    "json_bytes": json_bytes,
                    "blueprint_file": report["blueprint_file"],
                }))
            }
            Err(e) => json_result(&serde_json::json!({
                "status": "error",
                "stage": "register",
                "bp_id": bp_id,
                "lint": lint,
                "bind": bind,
                "error": format!("{e:#}"),
            })),
        }
    }

    #[tool(
        description = "Unarchive a Blueprint — reverse of bp_archive. Appends `archive: false` marker commit to head, re-exposing the id to list_ids / read_head / swarm_run. Wraps POST /v1/blueprints/:id/unarchive."
    )]
    async fn bp_unarchive(
        &self,
        Parameters(req): Parameters<BpUnarchiveReq>,
    ) -> Result<CallToolResult, McpError> {
        let bind = req
            .bind
            .unwrap_or_else(|| crate::http::Endpoint::resolve(None).base().to_string());
        let url = crate::http::Endpoint::resolve(Some(&bind))
            .url(&format!("/v1/blueprints/{}/unarchive", req.id));
        let client = crate::http::client_builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| McpError::internal_error(format!("client build: {e}"), None))?;
        let resp = client
            .post(&url)
            .send()
            .await
            .map_err(|e| McpError::internal_error(format!("unarchive: {e}"), None))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        json_result(&serde_json::json!({
            "status": if status.is_success() { "unarchived" } else { "error" },
            "http_status": status.as_u16(),
            "id": req.id,
            "bind": bind,
            "body": body,
        }))
    }

    #[tool(
        description = "Per-Blueprint agent.md size check plus GH #45 contract lints and GH #61 worker_binding lint. Fetches the Blueprint head from GET /v1/blueprints/:id/head and inspects every agent's profile.system_prompt (= the body that will be pushed to the SubAgent context via fetch). Reports per-agent bytes / lines / severity (OK|WARN|BLOCK) plus an aggregate verdict. The verdict is a report label only — this tool never blocks any dispatch. Default thresholds (`mse://guides/agent-md-authoring §Size targets`): WARN at ≥ 25 KB or ≥ 200 lines, BLOCK at ≥ 50 KB or ≥ 500 lines. BLOCK is disabled by default; callers targeting a strict 200 K-window model can pass `disable_block=false` to opt into the BLOCK band. Any threshold can also be overridden per call. Agents without a profile (RustFn / spec-only) are reported with severity OK and bytes/lines 0. GH #31: each agent entry additionally carries `last_rendered_bytes` (the live, most-recently-baked post-render size from GET /v1/agents/:name/render-size — `null` when never dispatched, an N+1-per-agent HTTP cost this operator-diagnostic tool accepts) and, only once that value crosses the same `warn_bytes` threshold, a `delivery: \"system_ref\"` note (omitted entirely, not false/null, when under threshold) flagging that this agent's prompt is delivered by-reference rather than inline. GH #45: each agent entry also carries `tool_lint` (phantom MCP tool refs — profile.tools entries with the `mcp__mse__` prefix are compared against the live `mse://api/mcp-tools` registry; unknown names surface as WARN with the unknown tool list) and `output_contract_lint` (absent or malformed `profile.extras.expected_output` under the GH #44 convention surfaces as WARN with a specific reason). GH #61: each operator-kind agent additionally carries `worker_binding_lint` (missing `profile.worker_binding` surfaces as WARN — same fail-loud condition `Compiler::compile` enforces at dispatch, retroactively surfaced on already-registered Blueprints); non-operator kinds are OK and carry `kind_requires_binding: false`. Any per-agent family can be disabled per call via `disable_tool_lint` / `disable_output_contract_lint` / `disable_worker_binding_lint`; the disabled family's field is omitted entirely from each entry (not `null`) so a caller cannot mistake a disabled family for a passed check. C4: a Blueprint-scoped `binding_lint` family (default enabled; disable via `disable_binding_lint`) is attached as a top-level `binding_lint.findings` array (omitted entirely when disabled) — advisory operator-binding checks: `binding_requirements_info` (INFO — one finding per Runner-backed agent listing the launch variant / tools / model a joining operator's capability_manifest must cover, identical to GET /v1/blueprints/:id/binding-requirements), `strict_binding_without_runners` (WARN — strategy.strict_binding is true but no agent resolves to a Runner, so strict is a no-op), and `legacy_worker_binding` (WARN — an agent's Runner came from the deprecated profile.worker_binding fallback; migrate to runner / runner_ref). The aggregate verdict folds size + tool + contract + worker_binding + binding_lint-WARN severities via the same OK/WARN/BLOCK precedence (binding_lint INFO findings never escalate the verdict). GH #79 Phase 3: the response additionally carries a top-level `diagnostics: [...]` array — the unified Clippy-style projection of every finding across every family (`mlua-swarm-diag` `Diagnostic` wire shape: stable `kind`, `stage {type, family}`, `level`, `message`/`notes`/`help`, optional `suggestion {msg, patch, applicability}`, `docs_ref`, `span`). Additive alongside the family-specific fields above (which remain authoritative for current callers and are slated for removal in a future major bump); an OK verdict contributes no diagnostics entry, so an all-clear Blueprint reports `diagnostics: []`. Lint kinds and the add-a-lint recipe: `mse://guides/lint-diagnostic-model`. GH #78: a Blueprint-scoped `context_policy_lint` family (default enabled; disable via `disable_context_policy_lint`) is attached as a top-level `context_policy_lint.findings` array — front-loads the silent `file_path: null` failure: `context_policy_strips_projection_roots` (WARN — an agent's effective context_policy filters out both `work_dir` and `project_root`, so the projection root can never resolve) and, only when the caller passes `simulated_launch` (an object whose `project_root` / `work_dir` string fields mirror the canonical launch seed; pass `{}` to simulate a seedless launch), `projection_root_seed_missing` (WARN — no seeded field survives the agent's policy, so materialized step reads will return `content_url`-only). WARN-only family; its findings fold into the aggregate verdict and the `diagnostics` array like every other family. A Blueprint-scoped `verdict_contract_lint` family (default enabled; disable via `disable_verdict_contract_lint`) is attached as a top-level `verdict_contract_lint.findings` array — `verdict_value_unhandled` (WARN — an `agents[].verdict.values` entry that no downstream Branch/Loop cond ever compares against). This is the reader-visible surface for the reverse-direction check `Compiler::compile` already runs but only reports as a `tracing::warn!` outside `metadata.strict_verdict_handling`. It front-loads the decorative-contract failure: a flow with no Branch at all still compiles with a `channel: \"body\"` contract on every gate, and `channel: \"body\"` additionally constrains the step's terminal OUTPUT value to be one of the declared tokens — so a gate that returns a report body has its `Final` rejected at completion time and the attempt fails far from the declaration that caused it. Findings carry `agent` / `value` / `channel` / `declared_values` / `step_ref`, and the body-channel message names that OUTPUT-shape consequence. The family also emits a per-agent aggregate finding `verdict_contract_never_read` (WARN, one per agent whose entire declared `verdict.values` set is unread — the \"whole gate is dead\" reading, separated from the per-value baseline so a full gate loss is not hidden by the always-unread PASS token; carries a concrete `gate = true` suggestion for the B.pipeline authoring path). Aggregate findings appear first in `findings[]`; both views coexist and both fold into `verdict_contract_lint_warn_count`. WARN-only; folds into the aggregate verdict and `diagnostics` like every other family. Lint level control (Clippy-style `allow` / `warn` / `deny`): every finding of every family above is resolved against three declared layers before it folds into the verdict — the call-site `lints` request field (top precedence), `agents[].lints` (applies only to findings spanning that agent), and `metadata.lints` (Blueprint-wide). The first layer with any matching key wins outright, no merging across layers (rustc's attribute-proximity model); within one layer an exact kind key (`\"agent-md-size\"`) beats a `\"category:<correctness|suspicious|style|contract|migration>\"` group, which beats `\"all\"`. `allow` moves the finding to the new always-present top-level `suppressed: [{kind, span, source, message}]` array (`source` is `\"call-site\"` / `\"agent:<name>\"` / `\"blueprint\"`) — it leaves its family surface and `diagnostics[]` and folds into nothing, but is never silently dropped, the same \"omitted ≠ passed\" discipline the per-family disable flags follow; an empty `suppressed[]` is the nothing-was-allowed reading. `deny` escalates a finding to `DiagLevel::Error` in `diagnostics[]` and to the BLOCK band of the aggregate verdict — including for WARN-only families — and the verdict stays a report label: this tool still blocks nothing. Per-agent surfaces keep their measurements either way and report the resolved band in their own `severity` (an allowed `agent-md-size` finding leaves the agent entry with its real bytes/lines and `severity: \"OK\"`); Blueprint-scoped `findings[]` arrays drop the allowed entries. Two meta-lints about the `lints` config itself land in `diagnostics[]` and fold as WARN: `unknown-lint-kind` (a key matching no kind, no category group and not `all`, or a call-site value that is not allow/warn/deny — a typo degrades to this, it never rejects the request; the note names the declaring layer) and `non-suppressible-lint` (an exact-kind `allow`/`warn` on a compile-stage hard error this tool never emits, e.g. `duplicate-agent-name`, where the setting cannot have any effect anywhere; `category:` / `all` keys never raise it). Full grammar and the recipe: `mse://guides/lint-diagnostic-model`."
    )]
    async fn bp_doctor(
        &self,
        Parameters(req): Parameters<BpDoctorReq>,
    ) -> Result<CallToolResult, McpError> {
        let bind = req
            .bind
            .unwrap_or_else(|| crate::http::Endpoint::resolve(None).base().to_string());
        let thresholds = AgentMdThresholds::from_req(
            req.warn_bytes,
            req.warn_lines,
            req.block_bytes,
            req.block_lines,
            req.disable_block,
        );
        let url = crate::http::Endpoint::resolve(Some(&bind))
            .url(&format!("/v1/blueprints/{}/head", req.id));
        let client = crate::http::client_builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| McpError::internal_error(format!("client build: {e}"), None))?;
        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(|e| McpError::internal_error(format!("bp_doctor fetch: {e}"), None))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return json_result(&serde_json::json!({
                "bp_id": req.id,
                "bind": bind,
                "http_status": status.as_u16(),
                "error": body,
                "guide_ref": "mse://guides/agent-md-authoring",
            }));
        }
        let head: JsonValue = resp
            .json()
            .await
            .map_err(|e| McpError::internal_error(format!("bp_doctor decode: {e}"), None))?;
        let bp_value = head.get("blueprint").cloned().ok_or_else(|| {
            McpError::internal_error("bp_doctor: response missing `blueprint`", None)
        })?;
        let bp: Blueprint = serde_json::from_value(bp_value)
            .map_err(|e| McpError::internal_error(format!("bp_doctor bp parse: {e}"), None))?;

        // GH #45: build the MCP tool-name registry once per invocation
        // so `classify_tool_lint` is a pure function (registry threaded
        // in by reference), and cache the per-family disable flags so
        // the flag lookup is out of the per-agent loop.
        let tool_registry = build_mcp_tool_registry();
        let disable_tool_lint = req.disable_tool_lint.unwrap_or(false);
        let disable_output_contract_lint = req.disable_output_contract_lint.unwrap_or(false);
        let disable_worker_binding_lint = req.disable_worker_binding_lint.unwrap_or(false);
        let disable_binding_lint = req.disable_binding_lint.unwrap_or(false);
        let disable_skip_on_lint = req.disable_skip_on_lint.unwrap_or(false);
        let disable_context_policy_lint = req.disable_context_policy_lint.unwrap_or(false);
        let disable_verdict_contract_lint = req.disable_verdict_contract_lint.unwrap_or(false);
        let disable_spawner_hint_lint = req.disable_spawner_hint_lint.unwrap_or(false);

        // Clippy-style lint level control: the call-site `lints` field,
        // `agents[].lints` and `metadata.lints` resolve every finding
        // below before it folds into the verdict. An allowed finding is
        // removed from its family surface and re-surfaces in
        // `suppressed[]` — always present (empty when nothing was
        // allowed) so callers can rely on the key.
        let lint_layers = LintLayers::new(req.lints.as_ref(), &bp);
        let mut suppressed: Vec<JsonValue> = Vec::new();

        let mut per_agent = Vec::with_capacity(bp.agents.len());
        let mut severities: Vec<&'static str> = Vec::with_capacity(bp.agents.len());
        // GH #79 Phase 3: the unified `diagnostics` projection,
        // accumulated alongside (not instead of) the family-specific
        // fields below — additive; Phase 4 retires the old surface.
        let mut diagnostics: Vec<mlua_swarm_diag::Diagnostic> = Vec::new();
        // GH #45 / #61: track lint-family severities separately so the
        // aggregate verdict can factor them in without disturbing the
        // size-check `severities` vec (which downstream callers already
        // consume verbatim).
        let mut tool_lint_severities: Vec<String> = Vec::new();
        let mut output_contract_lint_severities: Vec<String> = Vec::new();
        let mut worker_binding_lint_severities: Vec<String> = Vec::new();
        for agent in &bp.agents {
            let (bytes, lines) = match &agent.profile {
                Some(p) => (p.system_prompt.len(), p.system_prompt.lines().count()),
                None => (0usize, 0usize),
            };
            // The measured band, then the band after lint resolution: the
            // entry keeps its raw bytes/lines either way, but `severity`
            // reports the resolved outcome (`OK` when a layer allowed the
            // size finding away, `BLOCK` under `deny`).
            let measured = classify_agent_md_severity(bytes, lines, &thresholds);
            let severity = match diag_from_agent_md(&agent.name, measured, bytes, lines) {
                None => measured,
                Some(built) => {
                    match resolve_diagnostic(
                        &lint_layers,
                        &mut suppressed,
                        Some(&agent.name),
                        built,
                    ) {
                        Some(d) => {
                            let severity = severity_from_diag_level(d.level);
                            diagnostics.push(d);
                            severity
                        }
                        None => "OK",
                    }
                }
            };
            severities.push(severity);

            // GH #31: live post-render size lookup, reusing the same
            // `bind`/`client` already constructed above.
            // `last_rendered_bytes: null` is a normal response
            // (agent never dispatched) — always 200, never a 404.
            let render_size_url = crate::http::Endpoint::resolve(Some(&bind))
                .url(&format!("/v1/agents/{}/render-size", agent.name));
            let last_rendered_bytes: Option<usize> = match client.get(&render_size_url).send().await
            {
                Ok(resp) if resp.status().is_success() => resp
                    .json::<JsonValue>()
                    .await
                    .ok()
                    .and_then(|v| v.get("last_rendered_bytes").cloned())
                    .and_then(|v| v.as_u64())
                    .map(|n| n as usize),
                _ => None,
            };

            let mut entry = serde_json::json!({
                "name": agent.name,
                "kind": format!("{:?}", agent.kind),
                "has_profile": agent.profile.is_some(),
                "bytes": bytes,
                "lines": lines,
                "severity": severity,
                "last_rendered_bytes": last_rendered_bytes,
            });
            // Delivery-mode note: only when the post-render size crosses
            // the same `thresholds.warn_bytes` single-source-of-truth
            // that Engine's `SystemRefConfig.threshold_bytes` mirrors —
            // omit the key entirely (not `false`/`null`) when under
            // threshold, matching the per-agent entry's other
            // conditional-presence fields.
            if let Some(rendered_bytes) = last_rendered_bytes {
                if rendered_bytes >= thresholds.warn_bytes {
                    if let Some(obj) = entry.as_object_mut() {
                        obj.insert("delivery".to_string(), serde_json::json!("system_ref"));
                    }
                }
            }

            // GH #45: attach the two lint-family sections. When a
            // family is disabled at the call site, the field is
            // omitted entirely (not `null`) — matching the `delivery`
            // field's conditional-presence convention above so a
            // caller inspecting the response cannot mistake a
            // disabled family for a passed check.
            if !disable_tool_lint {
                let tools_ref: &[String] = agent
                    .profile
                    .as_ref()
                    .map(|p| p.tools.as_slice())
                    .unwrap_or(&[]);
                let mut tool_lint = classify_tool_lint(tools_ref, &tool_registry);
                let built = diag_from_tool_lint(&agent.name, &tool_lint);
                apply_agent_family_lints(
                    &mut tool_lint,
                    &agent.name,
                    built,
                    &lint_layers,
                    &mut suppressed,
                    &mut diagnostics,
                );
                if let Some(sev) = tool_lint.get("severity").and_then(|v| v.as_str()) {
                    tool_lint_severities.push(sev.to_string());
                }
                if let Some(obj) = entry.as_object_mut() {
                    obj.insert("tool_lint".to_string(), tool_lint);
                }
            }
            if !disable_output_contract_lint {
                let extras = agent
                    .profile
                    .as_ref()
                    .map(|p| &p.extras)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                let mut contract_lint = classify_output_contract_lint(&extras);
                let built = diag_from_output_contract_lint(&agent.name, &contract_lint);
                apply_agent_family_lints(
                    &mut contract_lint,
                    &agent.name,
                    built,
                    &lint_layers,
                    &mut suppressed,
                    &mut diagnostics,
                );
                if let Some(sev) = contract_lint.get("severity").and_then(|v| v.as_str()) {
                    output_contract_lint_severities.push(sev.to_string());
                }
                if let Some(obj) = entry.as_object_mut() {
                    obj.insert("output_contract_lint".to_string(), contract_lint);
                }
            }
            if !disable_worker_binding_lint {
                let wb: Option<&str> = agent
                    .profile
                    .as_ref()
                    .and_then(|p| p.worker_binding.as_deref());
                let mut wb_lint = classify_worker_binding_lint(&agent.kind, wb);
                let built = diag_from_worker_binding_lint(&agent.name, &wb_lint);
                apply_agent_family_lints(
                    &mut wb_lint,
                    &agent.name,
                    built,
                    &lint_layers,
                    &mut suppressed,
                    &mut diagnostics,
                );
                if let Some(sev) = wb_lint.get("severity").and_then(|v| v.as_str()) {
                    worker_binding_lint_severities.push(sev.to_string());
                }
                if let Some(obj) = entry.as_object_mut() {
                    obj.insert("worker_binding_lint".to_string(), wb_lint);
                }
            }

            per_agent.push(entry);
        }

        // C4: the binding_lint family is Blueprint-scoped (one resolution
        // over all agents), not per-agent, so it runs once after the loop
        // and attaches as a top-level section rather than an `agents[]`
        // field. When disabled, the section is omitted entirely (matching
        // the per-agent families' conditional-presence convention). Its
        // WARN findings feed the aggregate verdict; INFO findings do not
        // (they are informational manifest requirements, not defects).
        //
        // Blueprint-scoped findings resolve against the call-site and
        // Blueprint layers (plus the per-agent layer for a finding that
        // names an agent); an allowed finding leaves `findings[]`
        // entirely and re-surfaces in `suppressed[]`.
        let mut binding_lint = (!disable_binding_lint).then(|| classify_binding_lint(&bp));
        let binding_lint_severities: Vec<String> = binding_lint
            .as_mut()
            .map(|b| {
                apply_findings_family_lints(
                    mlua_swarm_diag::BpDoctorFamily::BindingLint,
                    b,
                    &lint_layers,
                    &mut suppressed,
                    &mut diagnostics,
                )
            })
            .unwrap_or_default();

        // GH #76 DSL sugar — Blueprint-scoped skip_on_lint (see
        // `classify_skip_on_lint` doc for the 3 checks). Same
        // conditional-presence convention as `binding_lint`: omitted
        // entirely when the family is disabled.
        // GH #78 — Blueprint-scoped context_policy_lint (see
        // `classify_context_policy_lint` doc for the 2 checks). Same
        // conditional-presence convention as `binding_lint`.
        let mut context_policy_lint = (!disable_context_policy_lint)
            .then(|| classify_context_policy_lint(&bp, req.simulated_launch.as_ref()));
        let context_policy_lint_severities: Vec<String> = context_policy_lint
            .as_mut()
            .map(|c| {
                apply_findings_family_lints(
                    mlua_swarm_diag::BpDoctorFamily::ContextPolicyLint,
                    c,
                    &lint_layers,
                    &mut suppressed,
                    &mut diagnostics,
                )
            })
            .unwrap_or_default();

        // Blueprint-scoped verdict_contract_lint (see
        // `classify_verdict_contract_lint` doc for the single check and
        // why the compile-side `tracing::warn!` needed a reader-visible
        // surface). Same conditional-presence convention as `binding_lint`.
        let mut verdict_contract_lint =
            (!disable_verdict_contract_lint).then(|| classify_verdict_contract_lint(&bp));
        let verdict_contract_lint_severities: Vec<String> = verdict_contract_lint
            .as_mut()
            .map(|v| {
                apply_findings_family_lints(
                    mlua_swarm_diag::BpDoctorFamily::VerdictContractLint,
                    v,
                    &lint_layers,
                    &mut suppressed,
                    &mut diagnostics,
                )
            })
            .unwrap_or_default();

        // Blueprint-scoped spawner_hint_lint (see
        // `classify_spawner_hint_lint` doc for why a compile-refused kind
        // still needs a report-only surface: registering does not compile,
        // so a Blueprint carrying a withdrawn layer key otherwise passes
        // its doctor run and dies at the first dispatch instead).
        let mut spawner_hint_lint =
            (!disable_spawner_hint_lint).then(|| classify_spawner_hint_lint(&bp));
        let spawner_hint_lint_severities: Vec<String> = spawner_hint_lint
            .as_mut()
            .map(|v| {
                apply_findings_family_lints(
                    mlua_swarm_diag::BpDoctorFamily::SpawnerHintLint,
                    v,
                    &lint_layers,
                    &mut suppressed,
                    &mut diagnostics,
                )
            })
            .unwrap_or_default();

        let mut skip_on_lint = (!disable_skip_on_lint).then(|| classify_skip_on_lint(&bp));
        let skip_on_lint_severities: Vec<String> = skip_on_lint
            .as_mut()
            .map(|s| {
                apply_findings_family_lints(
                    mlua_swarm_diag::BpDoctorFamily::SkipOnLint,
                    s,
                    &lint_layers,
                    &mut suppressed,
                    &mut diagnostics,
                )
            })
            .unwrap_or_default();

        // The meta-lints are about the `lints` config itself, so they are
        // independent of every family above and fold as WARN.
        let meta_lint_diagnostics = lint_layers.meta_diagnostics();
        let meta_lint_severities: Vec<&str> = vec!["WARN"; meta_lint_diagnostics.len()];
        diagnostics.extend(meta_lint_diagnostics);

        // GH #45 / #61 + C4: fold the four lint families into the aggregate
        // verdict. `aggregate_agent_md_verdict` already implements the
        // BLOCK-dominates-WARN-dominates-OK precedence — reuse it by
        // pushing each family's severities into a single flattened
        // vector so an agent with a passing size-check and a
        // phantom-tool WARN still surfaces at the top-level verdict.
        // `binding_lint`'s INFO findings fall through to OK (they contain
        // neither "WARN" nor "BLOCK"), so only its WARN findings escalate.
        let mut all_severities: Vec<&str> = Vec::with_capacity(
            severities.len()
                + tool_lint_severities.len()
                + output_contract_lint_severities.len()
                + worker_binding_lint_severities.len()
                + binding_lint_severities.len()
                + skip_on_lint_severities.len()
                + context_policy_lint_severities.len()
                + verdict_contract_lint_severities.len()
                + spawner_hint_lint_severities.len()
                + meta_lint_severities.len(),
        );
        all_severities.extend(severities.iter().copied());
        all_severities.extend(tool_lint_severities.iter().map(|s| s.as_str()));
        all_severities.extend(output_contract_lint_severities.iter().map(|s| s.as_str()));
        all_severities.extend(worker_binding_lint_severities.iter().map(|s| s.as_str()));
        all_severities.extend(binding_lint_severities.iter().map(|s| s.as_str()));
        all_severities.extend(skip_on_lint_severities.iter().map(|s| s.as_str()));
        all_severities.extend(context_policy_lint_severities.iter().map(|s| s.as_str()));
        all_severities.extend(verdict_contract_lint_severities.iter().map(|s| s.as_str()));
        all_severities.extend(spawner_hint_lint_severities.iter().map(|s| s.as_str()));
        all_severities.extend(meta_lint_severities.iter().copied());
        let verdict = aggregate_agent_md_verdict(&all_severities);
        let over_threshold_count = severities.iter().filter(|s| **s != "OK").count();
        let tool_lint_warn_count = tool_lint_severities
            .iter()
            .filter(|s| s.as_str() != "OK")
            .count();
        let output_contract_lint_warn_count = output_contract_lint_severities
            .iter()
            .filter(|s| s.as_str() != "OK")
            .count();
        let worker_binding_lint_warn_count = worker_binding_lint_severities
            .iter()
            .filter(|s| s.as_str() != "OK")
            .count();
        // Only WARN findings count here — INFO (`binding_requirements_info`)
        // is an informational manifest requirement, never a defect.
        let binding_lint_warn_count = binding_lint_severities
            .iter()
            .filter(|s| s.as_str() == "WARN")
            .count();
        // GH #76 DSL sugar — skip_on_lint findings are WARN-only (family is
        // BLOCK-disabled by default), so this counts every finding
        // that carries a severity string; if a future check emits
        // INFO/OK, adjust here.
        let skip_on_lint_warn_count = skip_on_lint_severities
            .iter()
            .filter(|s| s.as_str() == "WARN")
            .count();
        // GH #78 — context_policy_lint findings are WARN-only (same
        // BLOCK-disabled posture as skip_on_lint).
        let context_policy_lint_warn_count = context_policy_lint_severities
            .iter()
            .filter(|s| s.as_str() == "WARN")
            .count();
        // verdict_contract_lint findings are WARN-only (same BLOCK-disabled
        // posture as skip_on_lint / context_policy_lint).
        let verdict_contract_lint_warn_count = verdict_contract_lint_severities
            .iter()
            .filter(|s| s.as_str() == "WARN")
            .count();
        // spawner_hint_lint findings are WARN-only at this stage (the
        // compile stage is where the same kind is an Error).
        let spawner_hint_lint_warn_count = spawner_hint_lint_severities
            .iter()
            .filter(|s| s.as_str() == "WARN")
            .count();

        let mut body = serde_json::json!({
            "bp_id": req.id,
            "bind": bind,
            "http_status": status.as_u16(),
            "verdict": verdict,
            "agent_count": bp.agents.len(),
            "over_threshold_count": over_threshold_count,
            "tool_lint_warn_count": tool_lint_warn_count,
            "output_contract_lint_warn_count": output_contract_lint_warn_count,
            "worker_binding_lint_warn_count": worker_binding_lint_warn_count,
            "binding_lint_warn_count": binding_lint_warn_count,
            "skip_on_lint_warn_count": skip_on_lint_warn_count,
            "context_policy_lint_warn_count": context_policy_lint_warn_count,
            "verdict_contract_lint_warn_count": verdict_contract_lint_warn_count,
            "spawner_hint_lint_warn_count": spawner_hint_lint_warn_count,
            "thresholds": {
                "warn_bytes": thresholds.warn_bytes,
                "warn_lines": thresholds.warn_lines,
                "block_bytes": thresholds.block_bytes,
                "block_lines": thresholds.block_lines,
                "disable_block": thresholds.disable_block,
            },
            "lint_families": {
                "tool_lint_enabled": !disable_tool_lint,
                "output_contract_lint_enabled": !disable_output_contract_lint,
                "worker_binding_lint_enabled": !disable_worker_binding_lint,
                "binding_lint_enabled": !disable_binding_lint,
                "skip_on_lint_enabled": !disable_skip_on_lint,
                "context_policy_lint_enabled": !disable_context_policy_lint,
                "verdict_contract_lint_enabled": !disable_verdict_contract_lint,
                "spawner_hint_lint_enabled": !disable_spawner_hint_lint,
            },
            "agents": per_agent,
            // GH #79 Phase 3: the unified projection — one entry per
            // finding across every family, in the mlua-swarm-diag
            // `Diagnostic` wire shape. Additive alongside the
            // family-specific fields; Phase 4 retires those.
            "diagnostics": diagnostics,
            // Findings a `lints` layer allowed away: removed from their
            // family surface and from `diagnostics[]`, folding into
            // nothing, but never silently dropped. Always present —
            // an empty array is the "nothing was allowed" reading.
            "suppressed": suppressed,
            "guide_ref": "mse://guides/agent-md-authoring",
        });
        // C4: Blueprint-scoped operator-binding advisories. Inserted only
        // when the family is enabled — omitted entirely (not `null`) when
        // disabled, matching the per-agent families' conditional-presence
        // convention (the `lint_families.binding_lint_enabled` flag stays
        // the single disabled/enabled signal).
        if let Some(binding_lint) = binding_lint {
            if let Some(obj) = body.as_object_mut() {
                obj.insert("binding_lint".to_string(), binding_lint);
            }
        }
        // GH #76 DSL sugar — attach `skip_on_lint` when the family is enabled,
        // mirroring `binding_lint`'s conditional-presence convention
        // (the `lint_families.skip_on_lint_enabled` flag stays the
        // single enabled/disabled signal).
        if let Some(skip_on_lint) = skip_on_lint {
            if let Some(obj) = body.as_object_mut() {
                obj.insert("skip_on_lint".to_string(), skip_on_lint);
            }
        }
        // GH #78 — attach `context_policy_lint` when the family is
        // enabled, same conditional-presence convention.
        if let Some(context_policy_lint) = context_policy_lint {
            if let Some(obj) = body.as_object_mut() {
                obj.insert("context_policy_lint".to_string(), context_policy_lint);
            }
        }
        // Attach `verdict_contract_lint` when the family is enabled, same
        // conditional-presence convention.
        if let Some(verdict_contract_lint) = verdict_contract_lint {
            if let Some(obj) = body.as_object_mut() {
                obj.insert("verdict_contract_lint".to_string(), verdict_contract_lint);
            }
        }
        // Attach `spawner_hint_lint` when the family is enabled, same
        // conditional-presence convention.
        if let Some(spawner_hint_lint) = spawner_hint_lint {
            if let Some(obj) = body.as_object_mut() {
                obj.insert("spawner_hint_lint".to_string(), spawner_hint_lint);
            }
        }
        json_result(&body)
    }

    #[tool(
        description = "Explain how a Blueprint agent's definition materializes into its runtime worker contract — read-only, dry-run. Proxies GET /v1/blueprints/:bp_id/agents/:agent/explain (identity / declared_tools / system_prompt template diagnostics / effective_ctx 3-tier cascade / output projection naming), then augments the response with a client-side check the server cannot do itself: when the agent has a `worker_binding`, the worker wrapper `.claude/agents/<variant>.md` (override via `wrapper_dir`) is read and its frontmatter `tools` compared against `declared_tools.tools` via a new `tool_drift: {matched, declared_only, wrapper_only}` field — `declared_only` is the most important signal (tools the Blueprint author believes are usable but the wrapper does not actually grant). `tool_drift` is `null` when the agent has no `worker_binding` (nothing to compare against — same case the underlying `binding_note` already explains). A missing or unparsable wrapper file sets `wrapper_missing: true` + `wrapper_error` and leaves `tool_drift: null` — this is the tool's primary reason for existing (the current biggest invisibility point in the agent.md → worker pipeline). 404s exactly like the underlying endpoint: unregistered Blueprint or unknown agent name."
    )]
    async fn bp_explain_agent(
        &self,
        Parameters(req): Parameters<BpExplainAgentReq>,
    ) -> Result<CallToolResult, McpError> {
        let bind = req
            .bind
            .unwrap_or_else(|| crate::http::Endpoint::resolve(None).base().to_string());
        let url = crate::http::Endpoint::resolve(Some(&bind)).url(&format!(
            "/v1/blueprints/{}/agents/{}/explain",
            req.bp_id, req.agent
        ));
        let client = crate::http::client_builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| McpError::internal_error(format!("client build: {e}"), None))?;
        let resp =
            client.get(&url).send().await.map_err(|e| {
                McpError::internal_error(format!("bp_explain_agent fetch: {e}"), None)
            })?;
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return json_result(&serde_json::json!({
                "bp_id": req.bp_id,
                "agent": req.agent,
                "bind": bind,
                "http_status": status.as_u16(),
                "error": body_text,
            }));
        }
        let mut explain: JsonValue = serde_json::from_str(&body_text)
            .map_err(|e| McpError::internal_error(format!("bp_explain_agent decode: {e}"), None))?;

        // The server never reads wrapper files (Claude Code backend
        // concern, kept client-side) — this is the one piece of the
        // explain view this tool adds on top of the HTTP truth.
        let variant = explain
            .get("worker_binding")
            .and_then(|v| v.get("variant"))
            .and_then(|v| v.as_str())
            .map(str::to_string);

        let declared_tools: Vec<String> = explain
            .get("declared_tools")
            .and_then(|v| v.get("tools"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();

        let (tool_drift, wrapper_missing, wrapper_error): (
            Option<ToolDrift>,
            bool,
            Option<String>,
        ) = match &variant {
            None => (None, false, None),
            Some(variant) => {
                let wrapper_dir = req
                    .wrapper_dir
                    .clone()
                    .unwrap_or_else(|| DEFAULT_WRAPPER_DIR.to_string());
                match load_wrapper_tools(&wrapper_dir, variant) {
                    Ok(wrapper_tools) => (
                        Some(diff_tools(&declared_tools, &wrapper_tools)),
                        false,
                        None,
                    ),
                    Err(e) => (None, true, Some(e)),
                }
            }
        };

        if let Some(obj) = explain.as_object_mut() {
            let contract = wrapper_only_contract_set();
            let tool_drift_value = tool_drift
                .as_ref()
                .map(|drift| tool_drift_json_with_wrapper_only_split(drift, &contract))
                .unwrap_or(JsonValue::Null);
            obj.insert("tool_drift".to_string(), tool_drift_value);
            obj.insert(
                "wrapper_missing".to_string(),
                serde_json::json!(wrapper_missing),
            );
            if let Some(err) = wrapper_error {
                obj.insert("wrapper_error".to_string(), serde_json::json!(err));
            }
        }
        json_result(&explain)
    }

    #[tool(
        description = "Blueprint-wide sweep of the tool_drift check bp_explain_agent performs one agent at a time. Fetches GET /v1/blueprints/:bp_id/agents/explain (the batch summary route) for the agent/variant list, then GET /v1/blueprints/:bp_id/head once to read each agent's `declared_tools` locally (the batch summary route only carries `declared_tools_count`, not the tool names themselves). For every agent with a `worker_binding`, the worker wrapper `.claude/agents/<variant>.md` (override via `wrapper_dir`) is read and diffed the same way `bp_explain_agent` does, then split via the same `wrapper_only` classifier (`wrapper_only_contract` / `wrapper_only_meaningful`) — the per-row output stays compact (counts, not full lists), since a whole-Blueprint sweep response must stay small; drill down with `bp_explain_agent` for the full tool_drift detail on any one agent. Agents without a `worker_binding` get `variant: null` and every wrapper-side field `null`. A missing or unparsable wrapper file sets `wrapper_missing: true` + `wrapper_error`, with every drift count at 0. 404s exactly like `bp_explain_agent` / `bp_doctor`: unregistered Blueprint id."
    )]
    async fn bp_explain_agents(
        &self,
        Parameters(req): Parameters<BpExplainAgentsReq>,
    ) -> Result<CallToolResult, McpError> {
        let bind = req
            .bind
            .unwrap_or_else(|| crate::http::Endpoint::resolve(None).base().to_string());
        let client = crate::http::client_builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| McpError::internal_error(format!("client build: {e}"), None))?;

        let batch_url = crate::http::Endpoint::resolve(Some(&bind))
            .url(&format!("/v1/blueprints/{}/agents/explain", req.bp_id));
        let batch_resp =
            client.get(&batch_url).send().await.map_err(|e| {
                McpError::internal_error(format!("bp_explain_agents fetch: {e}"), None)
            })?;
        let batch_status = batch_resp.status();
        let batch_body_text = batch_resp.text().await.unwrap_or_default();
        if !batch_status.is_success() {
            return json_result(&serde_json::json!({
                "bp_id": req.bp_id,
                "bind": bind,
                "http_status": batch_status.as_u16(),
                "error": batch_body_text,
            }));
        }
        let batch: JsonValue = serde_json::from_str(&batch_body_text).map_err(|e| {
            McpError::internal_error(format!("bp_explain_agents batch decode: {e}"), None)
        })?;

        let head_url = crate::http::Endpoint::resolve(Some(&bind))
            .url(&format!("/v1/blueprints/{}/head", req.bp_id));
        let head_resp = client.get(&head_url).send().await.map_err(|e| {
            McpError::internal_error(format!("bp_explain_agents head fetch: {e}"), None)
        })?;
        let head_status = head_resp.status();
        if !head_status.is_success() {
            let body = head_resp.text().await.unwrap_or_default();
            return json_result(&serde_json::json!({
                "bp_id": req.bp_id,
                "bind": bind,
                "http_status": head_status.as_u16(),
                "error": body,
            }));
        }
        let head: JsonValue = head_resp.json().await.map_err(|e| {
            McpError::internal_error(format!("bp_explain_agents head decode: {e}"), None)
        })?;
        let bp_value = head.get("blueprint").cloned().ok_or_else(|| {
            McpError::internal_error("bp_explain_agents: response missing `blueprint`", None)
        })?;
        let bp: Blueprint = serde_json::from_value(bp_value).map_err(|e| {
            McpError::internal_error(format!("bp_explain_agents bp parse: {e}"), None)
        })?;

        let wrapper_dir = req
            .wrapper_dir
            .clone()
            .unwrap_or_else(|| DEFAULT_WRAPPER_DIR.to_string());
        let contract = wrapper_only_contract_set();

        let empty_agents: Vec<JsonValue> = Vec::new();
        let batch_agents = batch
            .get("agents")
            .and_then(|v| v.as_array())
            .unwrap_or(&empty_agents);

        let mut rows = Vec::with_capacity(batch_agents.len());
        for row in batch_agents {
            let name = row
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let variant = row
                .get("worker_binding")
                .and_then(|v| v.get("variant"))
                .and_then(|v| v.as_str())
                .map(str::to_string);

            let Some(variant) = variant else {
                rows.push(serde_json::json!({
                    "name": name,
                    "variant": JsonValue::Null,
                    "wrapper_missing": JsonValue::Null,
                    "wrapper_error": JsonValue::Null,
                    "declared_only_count": JsonValue::Null,
                    "wrapper_only_contract_count": JsonValue::Null,
                    "wrapper_only_meaningful_count": JsonValue::Null,
                }));
                continue;
            };

            let declared_tools: Vec<String> = bp
                .agents
                .iter()
                .find(|a| a.name == name)
                .and_then(|a| a.profile.as_ref())
                .map(|p| p.tools.clone())
                .unwrap_or_default();

            let (
                declared_only_count,
                wrapper_only_contract_count,
                wrapper_only_meaningful_count,
                wrapper_missing,
                wrapper_error,
            ) = match load_wrapper_tools(&wrapper_dir, &variant) {
                Ok(wrapper_tools) => {
                    let drift = diff_tools(&declared_tools, &wrapper_tools);
                    let (wrapper_only_contract, wrapper_only_meaningful) =
                        classify_wrapper_only(&drift.wrapper_only, &contract);
                    (
                        drift.declared_only.len(),
                        wrapper_only_contract.len(),
                        wrapper_only_meaningful.len(),
                        false,
                        None,
                    )
                }
                Err(e) => (0, 0, 0, true, Some(e)),
            };

            rows.push(serde_json::json!({
                "name": name,
                "variant": variant,
                "wrapper_missing": wrapper_missing,
                "wrapper_error": wrapper_error,
                "declared_only_count": declared_only_count,
                "wrapper_only_contract_count": wrapper_only_contract_count,
                "wrapper_only_meaningful_count": wrapper_only_meaningful_count,
            }));
        }

        let blueprint_ref = batch.get("blueprint").cloned().unwrap_or(JsonValue::Null);
        json_result(&serde_json::json!({
            "blueprint": blueprint_ref,
            "agents": rows,
        }))
    }

    #[tool(
        description = "Doctor snapshot, in five sections that answer separate questions. `mse_mcp`: this process (build version, in-process store = InMemory ephemeral, in-flight runs). `endpoint`: where this call connected and WHY — `url`, `source` (argument | env | default, i.e. which layer supplied it, so a reader knows what to change), and `probe` from GET /v1/healthz. The probe answers two questions separately, because a 301 answers them differently: `host_network` {reachable, error} is whether the host answered at all (any HTTP status counts), and `server_available` {status: pass|fail, note} is whether it is actually serving (200 with body `ok`); `note` carries the explanation only on fail, and `http_status` is the raw code. `server`: what the far end says about itself — `self_report_read` (whether GET /v1/doctor could be read, which also needs the right scheme and a token — NOT reachability, that is `host_network`) plus its `self_report` (its own bind / backend / store root / ref_base / registered BP list). `supervision`: local launchd state, and ONLY when the endpoint is this machine — against a hosted endpoint it is `{applicable: false, reason}` rather than six nulls, because launchd does not supervise someone else's server. Plus `version_drift` comparing the two processes, `audit_findings` (GH #34) flagging `audit:<step>` artifacts across tracked runs, and `degradations` (GH #32) counting per-Run worker-degradation entries. `version_drift.drift` is tri-state: true / false / null (null = could not compare, NOT 'no drift'). A `probe.http_status` of 301/308 means the endpoint redirects — the server is reachable and the endpoint needs its scheme (https://host), not a restart."
    )]
    async fn mse_doctor(
        &self,
        Parameters(req): Parameters<DoctorReq>,
    ) -> Result<CallToolResult, McpError> {
        let endpoint = crate::http::Endpoint::resolve(req.bind.as_deref());
        let bind = endpoint.base().to_string();
        let server_status = launchd::status(&bind).await;
        let server_up = server_status.up;

        let server_info: JsonValue = if server_up {
            let url = crate::http::Endpoint::resolve(Some(&bind)).url("/v1/doctor");
            match crate::http::client_builder()
                .timeout(Duration::from_secs(3))
                .build()
            {
                Ok(client) => match client.get(&url).send().await {
                    Ok(r) => r.json::<JsonValue>().await.unwrap_or_else(
                        |e| serde_json::json!({"error": format!("doctor decode: {e}")}),
                    ),
                    Err(e) => serde_json::json!({"error": format!("doctor fetch: {e}")}),
                },
                Err(e) => serde_json::json!({"error": format!("client build: {e}")}),
            }
        } else {
            // Nothing was read, so there is nothing the server said. The
            // reason belongs to the probe, which already carries it —
            // putting our diagnosis under `self_report` would make the
            // section answer a question it was not asked.
            JsonValue::Null
        };

        let (run_count, tracked_runs) = {
            let inner = self.state.read().await;
            let tracked: Vec<(String, Option<String>)> = inner
                .runs
                .iter()
                .map(|(rid, h)| (rid.clone(), h.task_id.clone()))
                .collect();
            (inner.runs.len(), tracked)
        };
        // GH #32: the degradations scan below iterates the same tracked-run
        // list the audit scan consumes — clone before the audit `for` loop
        // takes ownership.
        let tracked_runs_for_degradations = tracked_runs.clone();

        // GH #34: audit_findings — for each tracked run whose task_id is
        // known, fetch its steps via the same HTTP steps API
        // (`GET /v1/tasks/:id/runs/:run/steps`) the REST debug plane
        // exposes, and flag entries whose name starts with `audit:` (the
        // `AfterRunAuditMiddleware` artifact naming convention). Runs with
        // no known task_id yet (an HTTP-proxied dispatch whose response is
        // still in flight) are silently skipped, not noted — that is not a
        // fetch failure. Invariant: this scan NEVER fails the doctor call —
        // every error becomes a note.
        let mut audit_findings: Vec<AuditFinding> = Vec::new();
        let mut audit_fetch_notes: Vec<String> = Vec::new();
        if server_up {
            let client = crate::http::client_builder()
                .timeout(Duration::from_secs(3))
                .build();
            match client {
                Ok(client) => {
                    for (run_id, task_id) in tracked_runs {
                        let Some(task_id) = task_id else {
                            continue;
                        };
                        let url = crate::http::Endpoint::resolve(Some(&bind))
                            .url(&format!("/v1/tasks/{task_id}/runs/{run_id}/steps"));
                        match client.get(&url).send().await {
                            Ok(resp) if resp.status().is_success() => {
                                match resp.json::<JsonValue>().await {
                                    Ok(steps_body) => {
                                        audit_findings.extend(extract_audit_findings(&steps_body));
                                    }
                                    Err(e) => audit_fetch_notes.push(format!(
                                        "run {run_id} (task {task_id}): steps decode failed: {e}"
                                    )),
                                }
                            }
                            Ok(resp) => audit_fetch_notes.push(format!(
                                "run {run_id} (task {task_id}): steps fetch returned HTTP {}",
                                resp.status().as_u16()
                            )),
                            Err(e) => audit_fetch_notes.push(format!(
                                "run {run_id} (task {task_id}): steps fetch failed: {e}"
                            )),
                        }
                    }
                }
                Err(e) => audit_fetch_notes.push(format!("audit scan client build failed: {e}")),
            }
        } else {
            audit_fetch_notes.push(format!(
                "audit_findings scan skipped: {}",
                unreachable_note(server_status.probe.status, &server_status.probe.url)
            ));
        }

        // GH #32: degradations — for each tracked run whose task_id is
        // known, fetch the plain `RunRecord` via `GET /v1/runs/:id` (not
        // the steps listing above — degradations never surface there,
        // per Crux invariant 2) and sum its `degradations` array length.
        // Same fail-safe contract as `audit_findings`: this scan NEVER
        // fails the doctor call, every error becomes a note; a run whose
        // `degradations` is absent or empty is skipped (no entry in
        // `runs`).
        let mut degradation_runs: Vec<JsonValue> = Vec::new();
        let mut degradation_total: usize = 0;
        let mut degradation_notes: Vec<String> = Vec::new();
        if server_up {
            let client = crate::http::client_builder()
                .timeout(Duration::from_secs(3))
                .build();
            match client {
                Ok(client) => {
                    for (run_id, task_id) in tracked_runs_for_degradations {
                        let Some(task_id) = task_id else {
                            continue;
                        };
                        let url = crate::http::Endpoint::resolve(Some(&bind))
                            .url(&format!("/v1/runs/{run_id}"));
                        match client.get(&url).send().await {
                            Ok(resp) if resp.status().is_success() => {
                                match resp.json::<JsonValue>().await {
                                    Ok(run_body) => {
                                        let count = run_body
                                            .get("degradations")
                                            .and_then(|v| v.as_array())
                                            .map(|a| a.len())
                                            .unwrap_or(0);
                                        if count > 0 {
                                            degradation_total += count;
                                            degradation_runs.push(serde_json::json!({
                                                "run_id": run_id,
                                                "task_id": task_id,
                                                "count": count,
                                            }));
                                        }
                                    }
                                    Err(e) => degradation_notes.push(format!(
                                        "run {run_id} (task {task_id}): run decode failed: {e}"
                                    )),
                                }
                            }
                            Ok(resp) => degradation_notes.push(format!(
                                "run {run_id} (task {task_id}): run fetch returned HTTP {}",
                                resp.status().as_u16()
                            )),
                            Err(e) => degradation_notes.push(format!(
                                "run {run_id} (task {task_id}): run fetch failed: {e}"
                            )),
                        }
                    }
                }
                Err(e) => {
                    degradation_notes.push(format!("degradations scan client build failed: {e}"))
                }
            }
        } else {
            degradation_notes.push(format!(
                "degradations scan skipped: {}",
                unreachable_note(server_status.probe.status, &server_status.probe.url)
            ));
        }

        // Version drift across the three independently-aged `mse`
        // processes (this `mse mcp`, the launchd `mse serve`, and whatever
        // `mse <cmd>` picks up off disk). `cargo install` replaces the
        // binary but leaves already-running processes on their original
        // vintage, so "is the thing answering me the version I just
        // built?" is not answerable from `mse --version` alone.
        //
        // Tri-state on purpose: `null` means "could not compare" (server
        // down, or its doctor payload predates `server_version`) — never
        // collapse an unchecked comparison into `false`, which would read
        // as "verified no drift".
        let mcp_version = env!("CARGO_PKG_VERSION");
        let server_version = server_info
            .get("server_version")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let version_drift: JsonValue = match server_version.as_deref() {
            Some(sv) => JsonValue::Bool(sv != mcp_version),
            None => JsonValue::Null,
        };

        let body = serde_json::json!({
            "mse_mcp": {
                "version": mcp_version,
                "in_process_blueprint_store": "InMemory (ephemeral, mse mcp process-local)",
                "in_flight_run_count": run_count,
                "note": "The mse mcp in-process store is dedicated to swarm_run(Inline). The register path uses a separate store on the HTTP server side (POST /v1/blueprints/:id).",
            },
            // Three questions that used to share one object, and whose
            // answers contradicted each other when they did: where did we
            // connect and why (`endpoint`), what does the thing at the
            // other end say about itself (`server`), and is a local daemon
            // even part of this picture (`supervision`). The old shape put
            // our `bind` next to the server's own `bind`, under the same
            // name, and listed six null launchd fields for a hosted server
            // that has no launchd.
            "endpoint": endpoint_report(&endpoint, &server_status),
            "server": {
                // Not "reachable" — that is the host's question, answered
                // above. This one is only whether GET /v1/doctor could be
                // read, which also needs the right scheme and a token.
                "self_report_read": server_up,
                "self_report": server_info,
            },
            "supervision": supervision_report(&endpoint, &server_status),
            "version_drift": {
                "mse_mcp": mcp_version,
                "mlua_swarm_server": server_version,
                "drift": version_drift,
                "note": "drift=null means the comparison could not be made — not 'no drift'. Either the server's self-report could not be read (see endpoint.probe for why: nothing answered, a redirect, a rejected token) or its /v1/doctor predates server_version. Each process keeps the version it was started with; restart the drifting side to pick up a newly installed binary.",
            },
            "audit_findings": {
                "count": audit_findings.len(),
                "findings": audit_findings,
                "notes": audit_fetch_notes,
            },
            "degradations": {
                "count": degradation_total,
                "runs": degradation_runs,
                "notes": degradation_notes,
            },
        });
        json_result(&body)
    }

    #[tool(
        description = "Start mse serve via `launchctl kickstart gui/<uid>/com.mse.server`, then healthz-polls up to 30s. No-op if healthz is already up. Server settings come from ~/.mse/config.toml, not this call. Returns {status: already_running|started, bind}. Errors with install instructions if the launchd job is not bootstrapped yet. Auto-bootstraps on missing-job (calls `mlua_swarm_server_bootstrap` transparently and retries kickstart). See `mse://guides/server-management` for recovery SOP."
    )]
    async fn mlua_swarm_server_start(
        &self,
        Parameters(req): Parameters<ServerStartReq>,
    ) -> Result<CallToolResult, McpError> {
        let bind = req
            .bind
            .unwrap_or_else(|| crate::http::Endpoint::resolve(None).base().to_string());
        match launchd::start(&bind).await {
            Ok(outcome) => json_result(&outcome),
            Err(e) => Err(McpError::internal_error(e.to_string(), None)),
        }
    }

    #[tool(
        description = "Read back a run's ctx — the one `swarm_run` wrote to `ctx_file.path` — without needing a filesystem read of your own. Selects a branch rather than paging bytes: `at` takes a `$.a.b` path (`$.aggregate.out`, `$.plan.parts`) and returns that branch whole, so the response is small because you asked for a branch, not because a threshold cut one. Omit `at` to enumerate the top level with each branch's size in bytes; do the same on any branch that comes back too large — `keys` is then the list to narrow into, and `truncated.note` says so. A run with no recorded ctx and a path that is not present report differently, and neither is reported as an empty result."
    )]
    async fn mse_run_ctx(
        &self,
        Parameters(req): Parameters<RunCtxReq>,
    ) -> Result<CallToolResult, McpError> {
        json_result(&run_ctx_read(&mse_home(), &req.run_id, req.at.as_deref()))
    }

    #[tool(
        description = "Escape hatch for a `/v1/**` route this MCP does not wrap yet: issues one request against the configured mse serve and returns `endpoint` {url, source} — the base it connected to and which layer supplied it — plus `request` {method, path, url}, `status`, `body`, and `truncated` (null unless the body exceeded the 16 KiB cap, in which case it carries bytes_total / bytes_returned / a note; a capped body is never presented as a whole one). Takes NO url and NO token — the endpoint comes from `MSE_HTTP` (else the loopback default) and the access-token header is attached by this process, so neither ever has to be typed into a tool call or copied out of a config file. `path` is allow-listed to `/v1/**`: a scheme, a host, a protocol-relative `//`, parent-directory traversal, and control characters are all rejected, so this cannot be aimed at another host. `method` defaults to GET; `body` is sent as JSON for POST/PATCH/DELETE. Redirects are not followed (a 3xx comes back as-is), which is how a wrong-scheme endpoint shows itself instead of silently succeeding."
    )]
    async fn mse_http(
        &self,
        Parameters(req): Parameters<HttpReq>,
    ) -> Result<CallToolResult, McpError> {
        crate::http::validate_api_path(&req.path).map_err(|e| McpError::invalid_params(e, None))?;

        let method = req.method.as_deref().unwrap_or("GET").to_ascii_uppercase();
        let endpoint = crate::http::Endpoint::resolve(None);
        let url = endpoint.url(&req.path);

        let client = crate::http::client_builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| McpError::internal_error(format!("client build: {e}"), None))?;

        let request = match method.as_str() {
            "GET" => client.get(&url),
            "POST" => client.post(&url),
            "PATCH" => client.patch(&url),
            "DELETE" => client.delete(&url),
            other => {
                return Err(McpError::invalid_params(
                    format!("unsupported method {other:?} (GET / POST / PATCH / DELETE)"),
                    None,
                ))
            }
        };
        let request = match (&req.body, method.as_str()) {
            (Some(body), m) if m != "GET" => request.json(body),
            _ => request,
        };

        let response = request
            .send()
            .await
            .map_err(|e| McpError::internal_error(format!("{method} {url}: {e}"), None))?;
        let status = response.status().as_u16();
        let text = response.text().await.unwrap_or_default();

        json_result(&http_report(
            &endpoint, &method, &req.path, &url, status, text,
        ))
    }

    #[tool(
        description = "Report mse serve state, in the same two sections `mse_doctor` uses so the two never disagree. `endpoint`: where this call connected and why — `url`, `source` (argument | env | default), and `probe` from GET /v1/healthz, whose `host_network` {reachable, error} and `server_available` {status: pass|fail, note} answer separately because a 301 answers them differently (the host replied; it is not serving — give the endpoint its scheme). `supervision`: the `launchctl print gui/<uid>/com.mse.server` summary (state / pid / last exit code) and the installed plist's WorkingDirectory — ONLY when the endpoint is this machine; otherwise `{applicable: false, reason}`, because launchd does not supervise someone else's server. `plist_working_directory_exists: false` is the signature of a zero-log EX_CONFIG (78) crash loop — launchd cannot chdir, the daemon dies before its log sinks open; recover with `mlua_swarm_server_install` (GH #97). See `mse://guides/server-management` for recovery flow."
    )]
    async fn mlua_swarm_server_status(
        &self,
        Parameters(req): Parameters<ServerStatusReq>,
    ) -> Result<CallToolResult, McpError> {
        let endpoint = crate::http::Endpoint::resolve(req.bind.as_deref());
        let out = launchd::status(endpoint.base()).await;
        json_result(&serde_json::json!({
            "endpoint": endpoint_report(&endpoint, &out),
            "supervision": supervision_report(&endpoint, &out),
        }))
    }

    #[tool(
        description = "Fully stop mse serve via `launchctl bootout gui/<uid>/com.mse.server` (unloads the job; KeepAlive will not restart it until the next `mlua_swarm_server_start` / `mlua_swarm_server_restart`). Refuses (structured error) if the server reports in-flight runs or attached operators via GET /v1/status; pass force=true to skip the check and kill unconditionally. Returns {bind, stopped}. See `mse://guides/server-management` for occupancy-gate behavior."
    )]
    async fn mlua_swarm_server_shutdown(
        &self,
        Parameters(req): Parameters<ServerShutdownReq>,
    ) -> Result<CallToolResult, McpError> {
        let bind = req
            .bind
            .unwrap_or_else(|| crate::http::Endpoint::resolve(None).base().to_string());
        let force = req.force.unwrap_or(false);
        if !force && launchd::healthz_ok(&bind).await {
            match launchd::occupancy(&bind).await {
                Ok(occ) if occ.running_runs > 0 || occ.attached_operators > 0 => {
                    return Err(McpError::invalid_params(
                        format!(
                            "refusing to shutdown: {} in-flight run(s), {} attached \
                             operator(s). Pass force=true to override.",
                            occ.running_runs, occ.attached_operators,
                        ),
                        None,
                    ));
                }
                Ok(_) => {}
                Err(e) => {
                    // Occupancy unknown (network hiccup / older server binary
                    // without the /v1/status occupancy fields) — fail open,
                    // do not block a legitimate shutdown/restart indefinitely.
                    // Log for visibility.
                    eprintln!("mse mcp: occupancy check failed, proceeding: {e}");
                }
            }
        }
        match launchd::shutdown(&bind).await {
            Ok(out) => json_result(&out),
            Err(e) => Err(McpError::internal_error(e.to_string(), None)),
        }
    }

    #[tool(
        description = "Kill + restart mse serve via `launchctl kickstart -k gui/<uid>/com.mse.server`, then healthz-polls up to 30s. Use after editing ~/.mse/config.toml to pick up the new settings. Refuses (structured error) if the server reports in-flight runs or attached operators via GET /v1/status; pass force=true to skip the check and kill unconditionally. Returns {status: started, bind}. Auto-bootstraps on missing-job (calls `mlua_swarm_server_bootstrap` transparently and retries kickstart). See `mse://guides/server-management` for recovery SOP."
    )]
    async fn mlua_swarm_server_restart(
        &self,
        Parameters(req): Parameters<ServerRestartReq>,
    ) -> Result<CallToolResult, McpError> {
        let bind = req
            .bind
            .unwrap_or_else(|| crate::http::Endpoint::resolve(None).base().to_string());
        let force = req.force.unwrap_or(false);
        if !force && launchd::healthz_ok(&bind).await {
            match launchd::occupancy(&bind).await {
                Ok(occ) if occ.running_runs > 0 || occ.attached_operators > 0 => {
                    return Err(McpError::invalid_params(
                        format!(
                            "refusing to restart: {} in-flight run(s), {} attached \
                             operator(s). Pass force=true to override.",
                            occ.running_runs, occ.attached_operators,
                        ),
                        None,
                    ));
                }
                Ok(_) => {}
                Err(e) => {
                    // Occupancy unknown (network hiccup / older server binary
                    // without the /v1/status occupancy fields) — fail open,
                    // do not block a legitimate shutdown/restart indefinitely.
                    // Log for visibility.
                    eprintln!("mse mcp: occupancy check failed, proceeding: {e}");
                }
            }
        }
        match launchd::restart(&bind).await {
            Ok(outcome) => json_result(&outcome),
            Err(e) => Err(McpError::internal_error(e.to_string(), None)),
        }
    }

    /// Load the mse-serve LaunchAgent into launchd. Thin forwarder to
    /// `crate::server::launchd::bootstrap()` — the tool description
    /// spells out the exact launchctl invocation for MCP callers.
    /// Idempotent on repeat: an already-loaded job returns
    /// `{status: "already_loaded", plist_path}` instead of failing. Does
    /// not stop or start the server; use `mlua_swarm_server_start` after
    /// bootstrap to bring healthz up. See
    /// `mse://guides/server-management` for recovery SOP.
    #[tool(
        annotations(read_only_hint = false, idempotent_hint = true),
        description = "Load the LaunchAgent (com.mse.server) via `launchctl bootstrap gui/<uid> ~/Library/LaunchAgents/com.mse.server.plist`. Idempotent on repeat: an already-loaded job returns `{status: \"already_loaded\", plist_path}` instead of failing; a fresh bootstrap returns `{status: \"bootstrapped\", plist_path}`. Missing plist is surfaced as a hard error pointing at `mlua_swarm_server_install`. Does not touch running processes — call `mlua_swarm_server_start` next to bring healthz up. See `mse://guides/server-management` for recovery SOP."
    )]
    async fn mlua_swarm_server_bootstrap(
        &self,
        Parameters(req): Parameters<ServerBootstrapReq>,
    ) -> Result<CallToolResult, McpError> {
        // `bind` / `plist_path` are accepted for schema forward-compat
        // and per-caller override intent, but the current
        // `launchd::bootstrap()` signature is bind-agnostic and uses the
        // canonical `installed_plist_path()`. Ack the fields (silence
        // dead-code warnings) so the JSON Schema stays stable across
        // future signature widening.
        let _ = req.bind;
        let _ = req.plist_path;
        match launchd::bootstrap().await {
            Ok(outcome) => json_result(&outcome),
            Err(e) => Err(McpError::internal_error(e.to_string(), None)),
        }
    }

    /// Render the compile-time-baked LaunchAgent plist template, write
    /// it to the canonical installed location, and load it. Thin
    /// forwarder to `crate::server::launchd::install()` — the tool
    /// description spells out the exact filesystem and launchctl
    /// operations for MCP callers. Idempotent on repeat: re-running with
    /// the same params produces a byte-identical plist (existing job is
    /// unloaded first so the new plist body takes effect). See
    /// `mse://guides/server-management` for recovery SOP.
    #[tool(
        annotations(read_only_hint = false, idempotent_hint = true),
        description = "Render the embedded launchd plist template with `{{HOME}}` / `{{CARGO_BIN}}` / `{{WORKING_DIR}}` substitution and write it to `~/Library/LaunchAgents/com.mse.server.plist`, then run `launchctl bootstrap gui/<uid> <plist>` to load the job. `WorkingDirectory` defaults to `~/.mse` (the service's own state directory, created if missing) — never the installer's CWD, so the daemon's ability to start cannot depend on a source checkout (GH #97). Idempotent on repeat: an already-loaded job is bootout'd first so the new plist body takes effect. Returns `{plist_path, bootstrap: {status, plist_path}}`. See `mse://guides/server-management` for recovery SOP."
    )]
    async fn mlua_swarm_server_install(
        &self,
        Parameters(req): Parameters<ServerInstallReq>,
    ) -> Result<CallToolResult, McpError> {
        let cargo_bin_pb = req.cargo_bin.as_deref().map(std::path::PathBuf::from);
        let working_dir_pb = req.working_dir.as_deref().map(std::path::PathBuf::from);
        match launchd::install(cargo_bin_pb.as_deref(), working_dir_pb.as_deref()).await {
            Ok(outcome) => json_result(&outcome),
            Err(e) => Err(McpError::internal_error(e.to_string(), None)),
        }
    }

    /// Unload the mse-serve LaunchAgent and remove its plist. Thin
    /// forwarder to `crate::server::launchd::uninstall()` — the tool
    /// description spells out the exact launchctl and filesystem
    /// operations for MCP callers. Idempotent on repeat: a missing job
    /// / missing plist are both treated as success. See
    /// `mse://guides/server-management` for recovery SOP.
    #[tool(
        annotations(read_only_hint = false, idempotent_hint = true),
        description = "Run `launchctl bootout gui/<uid>/com.mse.server` and remove `~/Library/LaunchAgents/com.mse.server.plist`. Idempotent on repeat: a missing job / missing plist are both treated as success. Returns `{plist_path}` (the path that was, or would have been, removed). See `mse://guides/server-management` for recovery SOP."
    )]
    async fn mlua_swarm_server_uninstall(
        &self,
        Parameters(_req): Parameters<ServerUninstallReq>,
    ) -> Result<CallToolResult, McpError> {
        match launchd::uninstall().await {
            Ok(outcome) => json_result(&outcome),
            Err(e) => Err(McpError::internal_error(e.to_string(), None)),
        }
    }

    #[tool(
        description = "Mark a run as cancelled in the local registry. Sets a `cancel_requested` mark on the local `RunHandle` — `swarm_status` surfaces it as `cancel_requested: true` alongside whatever `status` the server reports (the mark is preserved even when the server's HTTP enrichment overwrites `status` back to `running` / `done`). Note: in-flight handle abort is v3 carry; only the local mark flips today. Consumers watching for `swarm_status` to report `status: \"cancelled\"` should watch `cancel_requested` instead for immediate feedback."
    )]
    async fn swarm_cancel(
        &self,
        Parameters(req): Parameters<SwarmCancelReq>,
    ) -> Result<CallToolResult, McpError> {
        // Flip the local `RunHandle.cancel_requested` mark first
        // (unchanged pre-server-cancel behavior) so `swarm_status`
        // surfaces the request even when the server-side proxy fails.
        let has_local = {
            let mut inner = self.state.write().await;
            if let Some(h) = inner.runs.get_mut(&req.run_id) {
                h.status = RunStatus::Cancelled;
                h.cancel_requested = true;
                true
            } else {
                false
            }
        };
        // Server-side proxy: `POST /v1/runs/:id/cancel` records the
        // `core.cancel_requested` trace event on the persistent
        // `RunTraceStore` and flips the Run row's status when it is
        // still non-terminal. Without this, detach runs — whose
        // `RunTraceStore` is the server's, not the local one — never
        // showed the cancel event on `GET /v1/runs/:id/trace`.
        let bind = req
            .bind
            .clone()
            .unwrap_or_else(|| crate::http::Endpoint::resolve(None).base().to_string());
        let url = crate::http::Endpoint::resolve(Some(&bind))
            .url(&format!("/v1/runs/{}/cancel", req.run_id));
        let server_ok = match crate::http::client_builder()
            .timeout(Duration::from_secs(5))
            .build()
        {
            Ok(client) => client
                .post(&url)
                .send()
                .await
                .map(|r| r.status().is_success())
                .unwrap_or(false),
            Err(_) => false,
        };
        if !has_local && !server_ok {
            return Err(McpError::invalid_params(
                format!("run_id not found: {}", req.run_id),
                None,
            ));
        }
        json_result(&serde_json::json!({
            "ok": true,
            "run_id": req.run_id,
            "cancel_requested": true,
            "server_ack": server_ok,
        }))
    }
}

#[tool_handler]
impl ServerHandler for MseServer {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        // `ServerInfo::default()` fills `server_info` via
        // `Implementation::from_build_env()`, whose `env!` macros expand
        // inside the *rmcp* crate — so the handshake would otherwise
        // advertise `{name: "rmcp", version: "<rmcp ver>"}` instead of
        // ours. Override with this crate's own build env.
        info.server_info = Implementation::new("mse-mcp", env!("CARGO_PKG_VERSION"));
        info.instructions = Some(
            "mse mcp: MCP server for mlua-swarm-engine (stdio, sibling of mse serve). Bundled \
             guides, Blueprint samples, and the live Blueprint JSON Schema are exposed as MCP \
             resources under mse://."
                .into(),
        );
        info.capabilities = ServerCapabilities::builder()
            .enable_tools()
            .enable_resources()
            .build();
        info
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        let resources = resources::RESOURCES
            .iter()
            .map(|r| {
                RawResource::new(r.uri.to_string(), r.title.to_string())
                    .with_description(r.description.to_string())
                    .with_mime_type(r.mime_type.to_string())
                    .no_annotation()
            })
            .collect();
        Ok(ListResourcesResult {
            resources,
            next_cursor: None,
            meta: None,
        })
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        let Some(entry) = resources::find_by_uri(&request.uri) else {
            return Err(McpError::resource_not_found(
                format!("unknown resource: {}", request.uri),
                None,
            ));
        };
        let body = resources::body_for(entry).map_err(|e| McpError::internal_error(e, None))?;
        Ok(ReadResourceResult::new(vec![ResourceContents::text(
            body,
            request.uri,
        )]))
    }
}

/// Explains why the server could not be read, from what the health probe
/// actually saw.
///
/// The previous single sentence — "mse serve down; start via
/// `mlua_swarm_server_start`" — was emitted for every non-`ok` probe, so a
/// server that answered `301` (reachable, wrong scheme) or `401`
/// (reachable, token rejected) was reported as stopped, with advice that
/// could not help. Worse, it sat next to the probe fields that said
/// otherwise. Each branch here names what came back and what would fix it,
/// and the local-start hint appears only where starting a local daemon is
/// the actual remedy.
fn unreachable_note(probe_status: Option<u16>, probe_url: &str) -> String {
    match probe_status {
        Some(status) if (300..400).contains(&status) => format!(
            "server answered {status} at {probe_url} — it is reachable, not down. \
             That status is a redirect: name the endpoint with its scheme \
             (https://host) instead of a bare host:port. Redirects are not \
             followed, so the access token is never re-sent to the target."
        ),
        Some(status @ (401 | 403)) => format!(
            "server answered {status} at {probe_url} — it is reachable, but the \
             access token is missing or rejected. Set MSE_ACCESS_TOKEN to the \
             value this server was started with."
        ),
        Some(status) => format!(
            "server answered {status} at {probe_url}, which is not a healthy \
             healthz (expected 200 with body `ok`)."
        ),
        // Nothing answered. Offering to start a local daemon only makes
        // sense when the endpoint is one this machine could be serving.
        None if is_loopback_url(probe_url) => format!(
            "nothing answered at {probe_url}; start the local server via \
             mlua_swarm_server_start."
        ),
        None => format!(
            "nothing answered at {probe_url} — the endpoint is not local, so \
             check that it is running and reachable from here."
        ),
    }
}

/// [`unreachable_note`], or `null` when the probe passed — the IETF
/// health-check draft says `output` SHOULD be omitted for a `pass` state,
/// and a note explaining a failure that did not happen is noise.
fn unreachable_note_when_failing(
    ok: bool,
    probe_status: Option<u16>,
    probe_url: &str,
) -> JsonValue {
    if ok {
        JsonValue::Null
    } else {
        JsonValue::String(unreachable_note(probe_status, probe_url))
    }
}

/// Size above which a run's `final_ctx` is left out of the response and
/// only written to its file.
///
/// Unlike the thresholds this replaced, it is not load-bearing: the file
/// holds the whole ctx either way, so a value that is too high costs the
/// caller nothing but one `Read`, and one that is too low costs nothing at
/// all. Three earlier attempts sized a *trim* by this kind of number and
/// each lost something a caller reads — the fix was to stop deciding what
/// to discard, not to pick a better number.
const RUN_CTX_INLINE_BYTES: usize = 16 * 1024;

/// Writes a run's `final_ctx` to a file and reports where it went.
///
/// A ctx is the run's result, so trimming it is the wrong shape of answer:
/// per-key, per-depth and per-measured-threshold trims were each tried and
/// each dropped something (a gate's verdict, then the response's own size
/// bound). The MCP transport here is stdio — the tool runs on the caller's
/// machine — so the whole ctx goes to a file the caller can open, and the
/// response carries the path. `final_ctx` is inlined too when it is small
/// enough to be free; when it is not, it is `null` rather than a trimmed
/// object wearing the same name.
///
/// A write failure is reported, never swallowed: a response naming a file
/// that does not exist is worse than one saying it could not write.
fn run_ctx_report(root: &std::path::Path, final_ctx: JsonValue, run_id: &str) -> JsonValue {
    let serialized = serde_json::to_string_pretty(&final_ctx).unwrap_or_default();
    let bytes = serde_json::to_string(&final_ctx)
        .map(|s| s.len())
        .unwrap_or(0);

    let dir = root.join("runs").join(run_id);
    let path = dir.join("ctx.json");
    let write_result =
        std::fs::create_dir_all(&dir).and_then(|()| std::fs::write(&path, &serialized));

    let ctx_file = match write_result {
        Ok(()) if bytes <= RUN_CTX_INLINE_BYTES => serde_json::json!({
            "path": path.to_string_lossy(),
            "bytes": bytes,
        }),
        Ok(()) => serde_json::json!({
            "path": path.to_string_lossy(),
            "bytes": bytes,
            "note": format!(
                "final_ctx was not inlined ({bytes} bytes); read the file at this path \
                 for the whole ctx"
            ),
        }),
        Err(e) => serde_json::json!({
            "path": JsonValue::Null,
            "bytes": bytes,
            "error": format!("could not write the ctx to {}: {e}", path.display()),
        }),
    };

    let inline = if bytes <= RUN_CTX_INLINE_BYTES {
        final_ctx
    } else {
        JsonValue::Null
    };

    serde_json::json!({ "final_ctx": inline, "ctx_file": ctx_file })
}

/// Writes a built Blueprint to a file and reports where it went — the
/// `bp_build` twin of [`run_ctx_report`], under the same contract: the
/// whole document always goes to a file the caller can open (`out` when
/// given, else `<root>/bp/<stem>.json`), the response carries the path and
/// size, and `blueprint` is inlined only when it is under
/// [`RUN_CTX_INLINE_BYTES`] — above that it is `null` and the file entry
/// carries a note, never a trimmed object under the same name. A fully
/// embedded Blueprint (every agent's prompt in one document) is the case
/// this exists for.
///
/// `bytes` is the size of the file as written — the pretty-printed form —
/// so a reader who does `ls -l` on the path sees the same number; the
/// inline threshold is applied to that same value.
///
/// A write failure is reported in `blueprint_file.error` with a null path,
/// never swallowed.
fn bp_build_report(
    root: &std::path::Path,
    wire: &JsonValue,
    stem: &str,
    out: Option<&str>,
) -> JsonValue {
    let serialized = serde_json::to_string_pretty(wire).unwrap_or_default();
    let bytes = serialized.len();

    let path = match out {
        Some(p) => std::path::PathBuf::from(p),
        None => root.join("bp").join(format!("{stem}.json")),
    };
    let write_result = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => std::fs::create_dir_all(dir),
        _ => Ok(()),
    }
    .and_then(|()| std::fs::write(&path, &serialized));

    let blueprint_file = match write_result {
        Ok(()) if bytes <= RUN_CTX_INLINE_BYTES => serde_json::json!({
            "path": path.to_string_lossy(),
            "bytes": bytes,
        }),
        Ok(()) => serde_json::json!({
            "path": path.to_string_lossy(),
            "bytes": bytes,
            "note": format!(
                "blueprint was not inlined ({bytes} bytes); read the file at this path \
                 for the whole Blueprint"
            ),
        }),
        Err(e) => serde_json::json!({
            "path": JsonValue::Null,
            "bytes": bytes,
            "error": format!("could not write the Blueprint to {}: {e}", path.display()),
        }),
    };

    let inline = if bytes <= RUN_CTX_INLINE_BYTES {
        wire.clone()
    } else {
        JsonValue::Null
    };

    serde_json::json!({ "blueprint": inline, "blueprint_file": blueprint_file })
}

/// Reads back a run's recorded ctx, selecting a branch rather than paging
/// bytes.
///
/// `swarm_run` hands out a path, which only helps a caller that can open
/// files — a worker with a narrowed tool scope cannot. This closes that
/// inside the same MCP.
///
/// Selection is by path (`$.aggregate.out`) on purpose. A byte pager would
/// put a size threshold back in the middle of the contract, and the caller
/// would have to track how far it had read; with a path, the response is
/// small because the caller asked for a branch. `at` omitted enumerates
/// the top level with each branch's size, so a caller can see which one is
/// the large one before asking for it.
///
/// The one surviving cap applies to the selected branch, and says to
/// narrow `at` — a lever the caller always has, unlike a pager's offset.
fn run_ctx_read(root: &std::path::Path, run_id: &str, at: Option<&str>) -> JsonValue {
    let path = root.join("runs").join(run_id).join("ctx.json");
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) => {
            return serde_json::json!({
                "run_id": run_id,
                "error": format!(
                    "no ctx recorded for {run_id} at {}: {e}",
                    path.display()
                ),
            })
        }
    };
    let ctx: JsonValue = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(e) => {
            return serde_json::json!({
                "run_id": run_id,
                "error": format!("the ctx file for {run_id} is not JSON: {e}"),
            })
        }
    };

    /// Walks a `$.a.b` selector. Only named keys — an array index is not
    /// a thing a caller has needed, and leaving it out keeps the selector
    /// unambiguous.
    fn select<'a>(value: &'a JsonValue, at: &str) -> Option<&'a JsonValue> {
        let mut cur = value;
        for segment in at.trim_start_matches("$.").split('.') {
            if segment.is_empty() {
                continue;
            }
            cur = cur.get(segment)?;
        }
        Some(cur)
    }

    /// Immediate children with their serialized sizes, so a caller can see
    /// where the bytes are before asking for them.
    fn enumerate(value: &JsonValue, prefix: &str) -> JsonValue {
        match value {
            JsonValue::Object(map) => {
                let mut keys = serde_json::Map::new();
                for (key, child) in map {
                    let size = serde_json::to_string(child).map(|s| s.len()).unwrap_or(0);
                    keys.insert(format!("{prefix}.{key}"), JsonValue::from(size));
                }
                JsonValue::Object(keys)
            }
            _ => JsonValue::Null,
        }
    }

    let Some(at) = at else {
        return serde_json::json!({
            "run_id": run_id,
            "at": "$",
            "keys": enumerate(&ctx, "$"),
            "value": JsonValue::Null,
            "note": "pass `at` (for example \"$.aggregate.out\") to read one branch",
        });
    };

    let Some(selected) = select(&ctx, at) else {
        return serde_json::json!({
            "run_id": run_id,
            "error": format!("{at} is not present in this run's ctx"),
            "keys": enumerate(&ctx, "$"),
        });
    };

    let bytes = serde_json::to_string(selected)
        .map(|s| s.len())
        .unwrap_or(0);
    if bytes <= RUN_CTX_INLINE_BYTES {
        return serde_json::json!({
            "run_id": run_id,
            "at": at,
            "bytes": bytes,
            "value": selected,
            "truncated": JsonValue::Null,
        });
    }

    // Too large to hand back whole. Enumerate its children instead of
    // cutting it: the caller narrows `at` and gets a complete answer,
    // which a byte offset could never give.
    serde_json::json!({
        "run_id": run_id,
        "at": at,
        "bytes": bytes,
        "value": JsonValue::Null,
        "keys": enumerate(selected, at),
        "truncated": {
            "bytes_total": bytes,
            "note": format!(
                "{at} is {bytes} bytes, over the {RUN_CTX_INLINE_BYTES} cap — narrow \
                 `at` to one of the keys listed here and the whole branch comes back"
            ),
        },
    })
}

/// Where run artifacts live. `MSE_HOME` overrides it; otherwise `~/.mse`,
/// the same root `mse serve` uses for its store.
fn mse_home() -> std::path::PathBuf {
    std::env::var("MSE_HOME")
        .ok()
        .filter(|v| !v.is_empty())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var("HOME")
                .map(|h| std::path::PathBuf::from(h).join(".mse"))
                .unwrap_or_else(|_| std::path::PathBuf::from(".mse"))
        })
}

/// How much of a response body `mse_http` hands back.
///
/// The tool can reach any `/v1/**` route, including ones that answer with
/// megabytes — a run trace, a Blueprint head. Returning all of it makes a
/// diagnostic call capable of taking the caller's context down with it.
///
/// The number is measured, not chosen: at 64 KiB the cap fired correctly
/// and the result was still rejected by the MCP client and spilled to a
/// file (a 129 KB Blueprint head came back as a 69 KB tool result), which
/// is the failure the cap exists to prevent. The smallest rejection
/// observed was ~69 KB, so the cap sits a factor of four below it.
const HTTP_BODY_CAP_BYTES: usize = 16 * 1024;

/// Builds `mse_http`'s response.
///
/// Pure, so the shape and the cap are testable without pointing the
/// process's `MSE_HTTP` somewhere — which is what left the earlier version
/// of this tool covered only by its parts.
///
/// `endpoint.url` is the base and `request.url` is the full URL, named
/// apart because the sibling tools already use `endpoint.url` for the base
/// and one word cannot mean both. Neither is withheld: the host reaches
/// the caller through `mse_doctor` anyway, so hiding it here would cost
/// the ability to attribute a failure and buy nothing.
fn http_report(
    endpoint: &crate::http::Endpoint,
    method: &str,
    path: &str,
    url: &str,
    status: u16,
    text: String,
) -> JsonValue {
    let total = text.len();
    let (body, truncated) = if total > HTTP_BODY_CAP_BYTES {
        // Cut on a character boundary — slicing mid-character panics, a
        // poor end for a tool whose job is surviving whatever a route
        // answers with.
        let mut end = HTTP_BODY_CAP_BYTES;
        while end > 0 && !text.is_char_boundary(end) {
            end -= 1;
        }
        let kept = text[..end].to_string();
        let returned = kept.len();
        (
            JsonValue::String(kept),
            serde_json::json!({
                "bytes_total": total,
                "bytes_returned": returned,
                "note": "this is not the whole body — it was cut at the size cap. \
                         Narrow the request, or use a tool that streams.",
            }),
        )
    } else {
        // Hand back parsed JSON when it is JSON, and the raw text when it
        // is not — `/v1/healthz` answers `ok`, which is not.
        let parsed = serde_json::from_str(&text).unwrap_or(JsonValue::String(text));
        (parsed, JsonValue::Null)
    };

    serde_json::json!({
        "endpoint": {
            "url": endpoint.base(),
            "source": endpoint.source(),
        },
        "request": { "method": method, "path": path, "url": url },
        "status": status,
        "body": body,
        "truncated": truncated,
    })
}

/// "Where did we connect, and what came back" — shared by `mse_doctor` and
/// `mlua_swarm_server_status`, which ask the same question and so should
/// not answer it in two shapes.
fn endpoint_report(endpoint: &crate::http::Endpoint, status: &launchd::StatusOutcome) -> JsonValue {
    serde_json::json!({
        "url": endpoint.base(),
        "source": endpoint.source(),
        "source_note": format!(
            "resolved from the {} — change that to point elsewhere",
            endpoint.source().as_str()
        ),
        "probe": {
            "url": status.probe.url,
            "http_status": status.probe.status,
            // Two questions, because a 301 answers them differently: the
            // host answered, the server is not serving. One bool could
            // only ever be wrong about one of them.
            "host_network": {
                "reachable": status.probe.host_reachable(),
                "error": status.probe.error,
            },
            "server_available": {
                // "pass" / "fail" per the IETF health-check draft
                // vocabulary (draft-inadarei-api-health-check).
                "status": status.probe.availability(),
                "note": unreachable_note_when_failing(
                    status.up,
                    status.probe.status,
                    &status.probe.url,
                ),
            },
        },
    })
}

/// "Is a local daemon even part of this picture" — launchd supervises a
/// daemon on *this* machine, so against a hosted endpoint the whole
/// section is a category error. Reporting six nulls invited reading them
/// as "the daemon is unwell" rather than "there is no daemon here".
fn supervision_report(
    endpoint: &crate::http::Endpoint,
    status: &launchd::StatusOutcome,
) -> JsonValue {
    if is_loopback_url(&status.probe.url) {
        serde_json::json!({
            "applicable": true,
            "launchd_state": status.launchd_state,
            "launchd_pid": status.launchd_pid,
            "launchd_last_exit_code": status.launchd_last_exit_code,
            "plist_working_directory": status.plist_working_directory,
            "plist_working_directory_exists": status.plist_working_directory_exists,
        })
    } else {
        serde_json::json!({
            "applicable": false,
            "reason": format!(
                "{} is not this machine, so no local daemon supervises it — \
                 launchd state, pid and plist do not apply",
                endpoint.base()
            ),
        })
    }
}

/// Whether a URL names this machine, i.e. whether a local daemon could be
/// the thing serving it.
fn is_loopback_url(url: &str) -> bool {
    let host = url
        .split("://")
        .nth(1)
        .unwrap_or(url)
        .split('/')
        .next()
        .unwrap_or_default();
    let host = host.rsplit_once(':').map(|(h, _)| h).unwrap_or(host);
    matches!(
        host,
        "127.0.0.1" | "localhost" | "::1" | "[::1]" | "0.0.0.0"
    )
}

fn json_result<T: Serialize>(value: &T) -> Result<CallToolResult, McpError> {
    let text = serde_json::to_string_pretty(value)
        .map_err(|e| McpError::internal_error(e.to_string(), None))?;
    Ok(CallToolResult::success(vec![Content::text(text)]))
}

/// GH #31: fetches (`Http` mode) or reads (`File` mode) the content a
/// `SystemRef` points to. `base` is the already-`trim_end_matches('/')`d
/// server root (only consulted for `Http` mode when `system_ref.uri` is a
/// bare path, per the shipped `SystemRef` contract — `Http`-mode `uri` is
/// never fully-qualified). Errors are returned as a display string, not a
/// typed error — the caller wraps every failure into a value-level
/// `{ok: false, stage: "download", ...}` JSON result, never an `McpError`.
async fn fetch_system_ref_bytes(
    client: &reqwest::Client,
    base: &str,
    system_ref: &mlua_swarm::types::SystemRef,
) -> Result<Vec<u8>, String> {
    match system_ref.mode {
        mlua_swarm::types::SystemRefMode::Http => {
            let url = if system_ref.uri.starts_with("http://")
                || system_ref.uri.starts_with("https://")
            {
                system_ref.uri.clone()
            } else {
                format!("{base}{}", system_ref.uri)
            };
            let resp = client
                .get(&url)
                .send()
                .await
                .map_err(|e| format!("download {url}: {e}"))?;
            let status = resp.status();
            if !status.is_success() {
                return Err(format!("download {url}: HTTP {}", status.as_u16()));
            }
            resp.bytes()
                .await
                .map(|b| b.to_vec())
                .map_err(|e| format!("download {url}: {e}"))
        }
        mlua_swarm::types::SystemRefMode::File => {
            let path = system_ref.uri.trim_start_matches("file://");
            tokio::fs::read(path)
                .await
                .map_err(|e| format!("read {path}: {e}"))
        }
    }
}

pub async fn run() -> Result<()> {
    tracing::info!("mse mcp starting (stdio transport)");
    let server = MseServer::new();
    let service = server.serve(rmcp::transport::io::stdio()).await?;
    service.waiting().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A join manifest gains the `agent-block` capability once, and only
    /// when it does not already declare it.
    #[test]
    fn with_block_capability_adds_the_variant_once() {
        let bare = mlua_swarm::AgentProviderManifest {
            provider_id: "claude-code".into(),
            provider_revision: None,
            capabilities: vec![mlua_swarm::AgentProviderCapability {
                launch_variant: Some("claude".into()),
                resolved_model: Some("sonnet".into()),
                effective_tools: vec!["Read".into()],
                capability_snapshot_digest: None,
            }],
        };
        let added = with_block_capability(bare.clone());
        assert_eq!(added.capabilities.len(), 2, "{added:?}");
        assert_eq!(
            added.capabilities[1].launch_variant.as_deref(),
            Some(block_runner::LAUNCH_VARIANT)
        );
        assert_eq!(added.capabilities[0], bare.capabilities[0], "existing entry untouched");

        let again = with_block_capability(added.clone());
        assert_eq!(again, added, "idempotent: already declared, nothing appended");

        let declared_by_caller = mlua_swarm::AgentProviderManifest {
            provider_id: "x".into(),
            provider_revision: None,
            capabilities: vec![mlua_swarm::AgentProviderCapability {
                launch_variant: Some(block_runner::LAUNCH_VARIANT.into()),
                resolved_model: None,
                effective_tools: vec!["custom".into()],
                capability_snapshot_digest: None,
            }],
        };
        assert_eq!(
            with_block_capability(declared_by_caller.clone()),
            declared_by_caller,
            "a caller's own declaration (with its tools) is kept as is"
        );
    }

    /// A small Blueprint is written to the default file *and* inlined; the
    /// response names the file either way.
    #[test]
    fn bp_build_report_writes_default_file_and_inlines_when_small() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let wire = serde_json::json!({ "id": "tiny", "agents": [] });
        let report = bp_build_report(tmp.path(), &wire, "tiny", None);

        let path = report["blueprint_file"]["path"]
            .as_str()
            .expect("blueprint_file.path present");
        assert_eq!(
            std::path::Path::new(path),
            tmp.path().join("bp").join("tiny.json"),
            "default location is <root>/bp/<stem>.json: {report}"
        );
        let on_disk: JsonValue =
            serde_json::from_str(&std::fs::read_to_string(path).expect("file written"))
                .expect("file is JSON");
        assert_eq!(on_disk, wire, "the whole Blueprint is on disk");
        assert_eq!(report["blueprint"], wire, "small enough to inline");
        assert!(
            report["blueprint_file"]["note"].is_null(),
            "no note when inlined: {report}"
        );
        assert_eq!(
            report["blueprint_file"]["bytes"].as_u64().unwrap_or(0) as usize,
            std::fs::metadata(path).expect("stat").len() as usize,
            "bytes is the size of the file on disk: {report}"
        );
    }

    /// Past the inline threshold the file is still whole, `blueprint` is
    /// `null` (never a trimmed object), and the note says where to read.
    /// `out` wins over the default location.
    #[test]
    fn bp_build_report_nulls_inline_past_threshold_and_honors_out() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let out = tmp.path().join("nested").join("big.json");
        let prompt = "x".repeat(RUN_CTX_INLINE_BYTES + 1);
        let wire = serde_json::json!({
            "id": "big",
            "agents": [ { "name": "a", "profile": { "system_prompt": prompt } } ],
        });
        let report = bp_build_report(tmp.path(), &wire, "big", Some(out.to_str().unwrap()));

        assert_eq!(
            report["blueprint_file"]["path"].as_str(),
            out.to_str(),
            "`out` is honored, parents created: {report}"
        );
        assert!(report["blueprint"].is_null(), "not inlined: {report}");
        assert!(
            report["blueprint_file"]["note"]
                .as_str()
                .is_some_and(|n| n.contains("read the file")),
            "note points at the file: {report}"
        );
        let on_disk: JsonValue =
            serde_json::from_str(&std::fs::read_to_string(&out).expect("file written"))
                .expect("file is JSON");
        assert_eq!(on_disk, wire, "nothing trimmed on disk");
        assert_eq!(
            report["blueprint_file"]["bytes"].as_u64().unwrap_or(0),
            std::fs::metadata(&out).expect("stat").len(),
            "bytes is the size of the file on disk: {report}"
        );
    }

    /// A write failure is reported with a null path — never a path to a
    /// file that is not there.
    #[test]
    fn bp_build_report_reports_write_failure_with_null_path() {
        let tmp = tempfile::tempdir().expect("tempdir");
        // A file where the parent directory must go.
        let blocker = tmp.path().join("blocker");
        std::fs::write(&blocker, "not a dir").unwrap();
        let out = blocker.join("x.json");
        let wire = serde_json::json!({ "id": "x" });
        let report = bp_build_report(tmp.path(), &wire, "x", Some(out.to_str().unwrap()));

        assert!(report["blueprint_file"]["path"].is_null(), "{report}");
        assert!(
            report["blueprint_file"]["error"]
                .as_str()
                .is_some_and(|e| e.contains("could not write")),
            "{report}"
        );
    }

    /// The note that goes out when the server could not be read must not
    /// contradict the probe printed beside it.
    ///
    /// `up: false` used to mean one thing — "down, go start it" — and said
    /// so even when the probe had just come back `301`, i.e. the server was
    /// answering and only the scheme was wrong. Two lines of the same
    /// response disagreed, and the wrong one was the one phrased as advice.
    #[test]
    fn unreachable_note_names_a_redirect_instead_of_claiming_the_server_is_down() {
        let note = unreachable_note(Some(301), "http://example.com/v1/healthz");
        assert!(
            note.contains("301") && note.contains("scheme"),
            "a redirect must be named and its fix stated: {note}"
        );
        assert!(
            !note.contains("mlua_swarm_server_start"),
            "a redirecting server is not a stopped server, so do not offer to start one: {note}"
        );
    }

    #[test]
    fn unreachable_note_names_an_auth_rejection() {
        for status in [401, 403] {
            let note = unreachable_note(Some(status), "https://example.com/v1/healthz");
            assert!(
                note.contains(&status.to_string()) && note.contains("MSE_ACCESS_TOKEN"),
                "an auth rejection must name the token: {note}"
            );
            assert!(
                !note.contains("mlua_swarm_server_start"),
                "not a stopped server: {note}"
            );
        }
    }

    #[test]
    fn unreachable_note_still_says_down_when_nothing_answered() {
        let note = unreachable_note(None, "http://127.0.0.1:7777/v1/healthz");
        assert!(
            note.contains("127.0.0.1:7777") && note.contains("mlua_swarm_server_start"),
            "no answer at loopback is the case the start hint is for: {note}"
        );
    }

    #[test]
    fn unreachable_note_does_not_offer_a_local_start_for_a_remote_endpoint() {
        let note = unreachable_note(None, "https://example.com/v1/healthz");
        assert!(
            !note.contains("mlua_swarm_server_start"),
            "starting a local daemon cannot fix a remote endpoint: {note}"
        );
    }

    #[test]
    fn unreachable_note_reports_an_unexpected_status_verbatim() {
        let note = unreachable_note(Some(500), "https://example.com/v1/healthz");
        assert!(note.contains("500"), "{note}");
    }

    /// T10 — `mse_http` must not grow a way to name the host or the token.
    ///
    /// The tool's safety rests on two things a caller cannot supply: the
    /// endpoint (resolved from configuration) and the access token
    /// (attached by the shared client). A `url` / `base_url` / `endpoint`
    /// field would turn it into a request forwarder aimed anywhere; a
    /// `token` field would put a credential into tool arguments, which is
    /// where transcripts come from. Either one is a decision, not a
    /// refactor, so it has to break this test on the way in.
    #[test]
    fn mse_http_req_exposes_no_url_or_token_field() {
        let schema = serde_json::to_value(schemars::schema_for!(HttpReq))
            .expect("HttpReq schema serializes");
        let properties = schema
            .get("properties")
            .and_then(|p| p.as_object())
            .expect("HttpReq schema has properties");

        let mut names: Vec<&str> = properties.keys().map(String::as_str).collect();
        names.sort_unstable();
        assert_eq!(
            names,
            ["body", "method", "path"],
            "mse_http's argument surface changed — see this test's doc comment"
        );

        for banned in [
            "url",
            "base_url",
            "endpoint",
            "bind",
            "host",
            "server",
            "token",
            "access_token",
        ] {
            assert!(
                !properties.contains_key(banned),
                "mse_http must not accept {banned:?}"
            );
        }
    }
    use mlua_flow_ir::{Expr, Node as FlowNode};
    use mlua_swarm::blueprint::{
        current_schema_version, AgentDef, AgentKind, AgentMeta, AgentProfile, AuditDef, AuditMode,
        BlueprintMetadata, CompilerHints, CompilerStrategy, Runner,
    };

    fn identity_blueprint() -> Blueprint {
        use mlua_swarm::worker::baseline::AG_IDENTITY;
        Blueprint {
            schema_version: current_schema_version(),
            id: "mse mcp-l2-identity".into(),
            flow: FlowNode::Step {
                ref_: AG_IDENTITY.into(),
                in_: Expr::Path {
                    at: "$.in".parse().expect("literal test path: $.in"),
                },
                out: Expr::Path {
                    at: "$.out".parse().expect("literal test path: $.out"),
                },
            },
            agents: vec![AgentDef {
                name: AG_IDENTITY.into(),
                kind: AgentKind::RustFn,
                spec: serde_json::json!({"fn_id": AG_IDENTITY}),
                profile: None,
                meta: Some(AgentMeta::default()),
                runner: None,
                runner_ref: None,
                verdict: None,
                lints: None,
            }],
            operators: vec![],
            metas: vec![],
            hints: CompilerHints::default(),
            strategy: CompilerStrategy::default(),
            metadata: BlueprintMetadata {
                description: Some("mse mcp L2 fixture".into()),
                origin: Default::default(),
                tags: vec![],
                version_label: Some("0.1.0".into()),
                project_name_alias: None,
                default_run_ttl_secs: None,
                strict_verdict_handling: None,
                lints: None,
            },
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

    fn extract_text_payload(result: &rmcp::model::CallToolResult) -> String {
        match &result.content.first().expect("content").raw {
            rmcp::model::RawContent::Text(t) => t.text.clone(),
            _ => panic!("expected text content"),
        }
    }

    #[tokio::test]
    async fn swarm_run_registers_handle_and_returns_status() {
        let server = MseServer::new();
        // empty / minimal blueprint will likely fail decode inside handle,
        // but the response shape should still be a valid CallToolResult.
        let req = SwarmRunReq {
            blueprint: BlueprintInput::BareInline(serde_json::json!({})),
            init_ctx: None,
            timeout_secs: Some(5),
            operator_id: None,
            operator_sid: None,
            operator_slot: None,
            operator_kind: None,
            operator_kind_overrides: None,
            detach: None,
            ttl_secs: None,
        };
        let res = server.swarm_run(Parameters(req)).await.unwrap();
        assert!(!res.content.is_empty());
        let inner = server.state.read().await;
        assert_eq!(inner.runs.len(), 1);
    }

    #[tokio::test]
    async fn swarm_status_unknown_run_id_returns_invalid_params() {
        let server = MseServer::new();
        let err = server
            .swarm_status(Parameters(SwarmStatusReq {
                run_id: "nope".into(),
                bind: None,
            }))
            .await
            .unwrap_err();
        let _ = format!("{:?}", err);
    }

    /// GH #67: helper that maps a server-reported status string back to
    /// the local `RunStatus` — `done` / `failed` are terminal, everything
    /// else (including `running`) leaves the handle untouched.
    #[test]
    fn parse_run_status_maps_terminal_states_only() {
        assert!(matches!(parse_run_status("done"), Some(RunStatus::Done)));
        assert!(matches!(
            parse_run_status("failed"),
            Some(RunStatus::Failed)
        ));
        assert!(parse_run_status("running").is_none());
        assert!(parse_run_status("").is_none());
        assert!(parse_run_status("something-else").is_none());
    }

    /// GH #67: the HTTP enrichment is best-effort — an unreachable server
    /// bind must return `None` (so `swarm_status` silently falls back to
    /// the local run store trace) rather than propagating a client error.
    /// Uses port 1 (RFC 6335 reserved, connect always refuses) as an
    /// unreachable bind.
    #[tokio::test]
    async fn fetch_run_via_http_returns_none_when_server_unreachable() {
        // A short client timeout keeps the test snappy even if a
        // reactor happens to accept the connection.
        let result = fetch_run_via_http("127.0.0.1:1", "R-does-not-matter").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn swarm_run_with_valid_identity_blueprint_completes_done() {
        let server = MseServer::new();
        let bp_json = serde_json::to_value(identity_blueprint()).expect("serialize blueprint");
        let req = SwarmRunReq {
            blueprint: BlueprintInput::BareInline(bp_json),
            init_ctx: Some(serde_json::json!({"in": "hello"})),
            timeout_secs: Some(10),
            operator_id: None,
            operator_sid: None,
            operator_slot: None,
            operator_kind: None,
            operator_kind_overrides: None,
            detach: None,
            ttl_secs: None,
        };
        let result = server.swarm_run(Parameters(req)).await.expect("swarm_run");
        let text = extract_text_payload(&result);
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("parse json");
        assert_eq!(parsed["status"], "done", "payload: {text}");
        // baseline identity RustFn writes
        //   {"by":"baseline-identity","agent":"identity","echoed":"<prompt>"}
        let out = &parsed["final_ctx"]["out"];
        assert_eq!(out["by"], "baseline-identity", "payload: {text}");
        assert_eq!(out["agent"], "identity", "payload: {text}");
        // v2 wiring: head/history_len/log_tail must be present (Inline mode -> head=null, history_len=0)
        assert!(parsed.get("head").is_some(), "payload: {text}");
        assert!(parsed.get("history_len").is_some(), "payload: {text}");
        assert!(parsed.get("log_tail").is_some(), "payload: {text}");
        assert_eq!(parsed["history_len"], 0, "Inline mode -> 0");
    }

    /// GH #37: `detach: true` returns `{status: "running", detached: true}`
    /// immediately; the eval completes in the background and
    /// `swarm_status` eventually reports `done` with the result persisted
    /// in the local run store.
    #[tokio::test]
    async fn swarm_run_detached_returns_running_and_completes_in_background() {
        let server = MseServer::new();
        let bp_json = serde_json::to_value(identity_blueprint()).expect("serialize blueprint");
        let req = SwarmRunReq {
            blueprint: BlueprintInput::BareInline(bp_json),
            init_ctx: Some(serde_json::json!({"in": "hello"})),
            timeout_secs: Some(10),
            operator_id: None,
            operator_sid: None,
            operator_slot: None,
            operator_kind: None,
            operator_kind_overrides: None,
            detach: Some(true),
            ttl_secs: None,
        };
        let result = server.swarm_run(Parameters(req)).await.expect("swarm_run");
        let text = extract_text_payload(&result);
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("parse json");
        assert_eq!(parsed["status"], "running", "payload: {text}");
        assert_eq!(parsed["detached"], true, "payload: {text}");
        let run_id = parsed["run_id"].as_str().expect("run_id").to_string();

        // Poll swarm_status until the background eval finishes (~5s cap).
        let mut last = String::new();
        for _ in 0..50 {
            let status_res = server
                .swarm_status(Parameters(SwarmStatusReq {
                    run_id: run_id.clone(),
                    bind: None,
                }))
                .await
                .expect("swarm_status");
            last = extract_text_payload(&status_res);
            let status: serde_json::Value = serde_json::from_str(&last).expect("parse status");
            match status["status"].as_str() {
                Some("done") => return,
                Some("running") => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
                other => panic!("unexpected status {other:?}: {last}"),
            }
        }
        panic!("detached run did not reach done within ~5s: {last}");
    }

    /// Issue #13: an in-process run mints R-/T- prefixed ids and traces its
    /// steps into the local run store, visible via `swarm_status`.
    #[tokio::test]
    async fn swarm_run_traces_steps_and_status_exposes_them() {
        let server = MseServer::new();
        let bp_json = serde_json::to_value(identity_blueprint()).expect("serialize blueprint");
        let req = SwarmRunReq {
            blueprint: BlueprintInput::BareInline(bp_json),
            init_ctx: Some(serde_json::json!({"in": "hello"})),
            timeout_secs: Some(10),
            operator_id: None,
            operator_sid: None,
            operator_slot: None,
            operator_kind: None,
            operator_kind_overrides: None,
            detach: None,
            ttl_secs: None,
        };
        let result = server.swarm_run(Parameters(req)).await.expect("swarm_run");
        let parsed: serde_json::Value =
            serde_json::from_str(&extract_text_payload(&result)).expect("parse json");
        let run_id = parsed["run_id"].as_str().expect("run_id");
        let task_id = parsed["task_id"].as_str().expect("task_id");
        assert!(run_id.starts_with("R-"), "run_id: {run_id}");
        assert!(task_id.starts_with("T-"), "task_id: {task_id}");

        let status = server
            .swarm_status(Parameters(SwarmStatusReq {
                run_id: run_id.to_string(),
                bind: None,
            }))
            .await
            .expect("swarm_status");
        let sparsed: serde_json::Value =
            serde_json::from_str(&extract_text_payload(&status)).expect("parse status json");
        assert_eq!(sparsed["task_id"], task_id);
        let entries = sparsed["step_entries"].as_array().expect("step_entries");
        assert!(!entries.is_empty(), "expected at least one step entry");
        let step_id = entries[0]["step_id"].as_str().expect("step_id");
        assert!(step_id.starts_with("ST-"), "step_id: {step_id}");
    }

    // ─── BlueprintSelector: shape / File hygiene / bare-object compat ───────

    /// Selector `{kind: "inline", blueprint: {...}}` end-to-end.
    #[tokio::test]
    async fn swarm_run_accepts_inline_selector_form() {
        let server = MseServer::new();
        let bp_json = serde_json::to_value(identity_blueprint()).expect("serialize");
        let sel_json = serde_json::json!({
            "kind": "inline",
            "blueprint": bp_json,
        });
        let input: BlueprintInput = serde_json::from_value(sel_json).expect("selector parse");
        let req = SwarmRunReq {
            blueprint: input,
            init_ctx: Some(serde_json::json!({"in": "hello"})),
            timeout_secs: Some(10),
            operator_id: None,
            operator_sid: None,
            operator_slot: None,
            operator_kind: None,
            operator_kind_overrides: None,
            detach: None,
            ttl_secs: None,
        };
        let result = server.swarm_run(Parameters(req)).await.expect("swarm_run");
        let text = extract_text_payload(&result);
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("json");
        assert_eq!(parsed["status"], "done", "payload: {text}");
    }

    /// Backward compat: a bare Blueprint object (no `kind` wrapper) is
    /// treated as inline.
    #[tokio::test]
    async fn swarm_run_bare_blueprint_still_works() {
        let server = MseServer::new();
        let bp_json = serde_json::to_value(identity_blueprint()).expect("serialize");
        let input: BlueprintInput = serde_json::from_value(bp_json).expect("bare parse");
        let req = SwarmRunReq {
            blueprint: input,
            init_ctx: Some(serde_json::json!({"in": "hi"})),
            timeout_secs: Some(10),
            operator_id: None,
            operator_sid: None,
            operator_slot: None,
            operator_kind: None,
            operator_kind_overrides: None,
            detach: None,
            ttl_secs: None,
        };
        let result = server.swarm_run(Parameters(req)).await.expect("swarm_run");
        let text = extract_text_payload(&result);
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("json");
        assert_eq!(parsed["status"], "done", "payload: {text}");
    }

    /// Selector `{kind: "file", path: "..."}` reads the Blueprint from a
    /// CWD-relative file and runs it.
    #[tokio::test]
    async fn swarm_run_file_selector_reads_and_runs() {
        let server = MseServer::new();
        let bp_json = serde_json::to_value(identity_blueprint()).expect("serialize");
        // write to a unique CWD-relative filename to avoid path-traversal
        // rejection; clean up after.
        let name = format!("__mse_test_bp_{}.json", uuid::Uuid::new_v4());
        std::fs::write(&name, serde_json::to_vec(&bp_json).unwrap()).expect("write bp");
        let sel_json = serde_json::json!({ "kind": "file", "path": &name });
        let input: BlueprintInput = serde_json::from_value(sel_json).expect("selector parse");
        let req = SwarmRunReq {
            blueprint: input,
            init_ctx: Some(serde_json::json!({"in": "hi"})),
            timeout_secs: Some(10),
            operator_id: None,
            operator_sid: None,
            operator_slot: None,
            operator_kind: None,
            operator_kind_overrides: None,
            detach: None,
            ttl_secs: None,
        };
        let result = server.swarm_run(Parameters(req)).await.expect("swarm_run");
        let _ = std::fs::remove_file(&name);
        let text = extract_text_payload(&result);
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("json");
        assert_eq!(parsed["status"], "done", "payload: {text}");
    }

    /// The run-scoped pin only means something on the server-backed path.
    /// An inline Blueprint runs inside this process, which holds no
    /// Operator sessions — accepting the argument and running unpinned
    /// would tell the caller "done" on a route that could never deliver
    /// where they asked.
    #[tokio::test]
    async fn swarm_run_rejects_an_operator_sid_on_the_inline_path() {
        let server = MseServer::new();
        let bp_json = serde_json::to_value(identity_blueprint()).expect("serialize");
        let input: BlueprintInput = serde_json::from_value(bp_json).expect("bare parse");
        let req = SwarmRunReq {
            blueprint: input,
            init_ctx: Some(serde_json::json!({"in": "hi"})),
            timeout_secs: Some(10),
            operator_id: None,
            operator_sid: Some("S-somewhere".to_string()),
            operator_slot: None,
            operator_kind: None,
            operator_kind_overrides: None,
            detach: None,
            ttl_secs: None,
        };
        let err = server
            .swarm_run(Parameters(req))
            .await
            .expect_err("an inline run must not silently ignore the pin");
        let message = err.to_string();
        assert!(
            message.contains("operator_sid"),
            "the error must name the rejected param: {message}"
        );
        assert!(
            message.contains("id"),
            "the error must point at the selector that does support it: {message}"
        );
    }

    /// The other half of the pin gets the same treatment: naming a seat on
    /// a path that pins nothing is refused rather than dropped.
    #[tokio::test]
    async fn swarm_run_rejects_an_operator_slot_on_the_inline_path() {
        let server = MseServer::new();
        let bp_json = serde_json::to_value(identity_blueprint()).expect("serialize");
        let input: BlueprintInput = serde_json::from_value(bp_json).expect("bare parse");
        let req = SwarmRunReq {
            blueprint: input,
            init_ctx: Some(serde_json::json!({"in": "hi"})),
            timeout_secs: Some(10),
            operator_id: None,
            operator_sid: None,
            operator_slot: Some("phase-a-op".to_string()),
            operator_kind: None,
            operator_kind_overrides: None,
            detach: None,
            ttl_secs: None,
        };
        let err = server
            .swarm_run(Parameters(req))
            .await
            .expect_err("an inline run must not silently ignore the seat");
        let message = err.to_string();
        assert!(
            message.contains("operator_slot"),
            "the error must name the rejected param: {message}"
        );
    }

    /// Auto-pin only applies when the launch targets the server this
    /// process's sessions are joined to — a sid means nothing elsewhere.
    #[test]
    fn auto_pin_matches_only_the_joined_server() {
        assert!(
            auto_pin_targets_joined_server("http://127.0.0.1:7777", None),
            "an omitted bind means the default server"
        );
        assert!(auto_pin_targets_joined_server(
            "http://127.0.0.1:7777",
            Some("127.0.0.1:7777")
        ));
        assert!(
            auto_pin_targets_joined_server("http://127.0.0.1:7777/", Some("127.0.0.1:7777")),
            "a trailing slash on the joined base must not defeat the match"
        );
        assert!(
            !auto_pin_targets_joined_server("http://127.0.0.1:7777", Some("127.0.0.1:9999")),
            "another server never inherits this process's sid"
        );
        assert!(
            !auto_pin_targets_joined_server("http://elsewhere:7777", None),
            "the default bind is not this process's server here"
        );
    }

    /// Both ways of arriving at a pin carry the sid unchanged and a
    /// non-empty `Assign.desc` (the server rejects a blank one with
    /// `400`), and the two descs differ — "the caller named this session"
    /// and "this process auto-pinned its only one" are different facts
    /// about the same run, and the Run record is where that gets read
    /// back.
    #[test]
    fn an_operator_pin_carries_a_desc_that_says_how_it_was_chosen() {
        let explicit = OperatorPin::explicit("S-abc".to_string(), None);
        let auto = OperatorPin::auto("S-abc".to_string(), None);
        assert_eq!(explicit.sid, "S-abc");
        assert_eq!(auto.sid, "S-abc");
        assert!(!explicit.desc.trim().is_empty());
        assert!(!auto.desc.trim().is_empty());
        assert_ne!(
            explicit.desc, auto.desc,
            "an auto-pin must be distinguishable from a caller-named one"
        );
    }

    /// The seat is carried verbatim on both paths and defaults to unset —
    /// "which session" is answered automatically, "which lane" never is.
    #[test]
    fn an_operator_pin_carries_the_seat_only_when_the_caller_named_one() {
        assert_eq!(OperatorPin::explicit("S-abc".to_string(), None).slot, None);
        assert_eq!(
            OperatorPin::auto("S-abc".to_string(), None).slot,
            None,
            "an auto-pin picks a session, never a lane"
        );
        assert_eq!(
            OperatorPin::explicit("S-abc".to_string(), Some("phase-b-op".to_string())).slot,
            Some("phase-b-op".to_string())
        );
        assert_eq!(
            OperatorPin::auto("S-abc".to_string(), Some("phase-b-op".to_string())).slot,
            Some("phase-b-op".to_string()),
            "a named seat survives the auto-pinned session"
        );
    }

    /// File hygiene: `..` parent-dir components are rejected.
    #[test]
    fn file_path_with_parent_dir_component_rejected() {
        let e = read_blueprint_from_file("../etc/passwd").unwrap_err();
        assert!(e.contains("parent-dir") || e.contains(".."), "err: {e}");
    }

    /// File hygiene: absolute paths are rejected.
    #[test]
    fn file_absolute_path_rejected() {
        let e = read_blueprint_from_file("/etc/passwd").unwrap_err();
        assert!(e.contains("absolute"), "err: {e}");
    }

    /// `mse://guides/lint-diagnostic-model`'s "Legacy `disable_*_lint`
    /// flags" table must list every such flag `BpDoctorReq` actually
    /// accepts, and its prose count must match the table.
    ///
    /// Bound to the schema rather than to a hand-kept list, because the
    /// drift this catches is exactly the hand-kept kind: adding
    /// `disable_spawner_hint_lint` to `BpDoctorReq` left both the count
    /// ("seven") and the table one row short, and nothing said so. Step 2
    /// of that guide's own add-a-lint recipe is to update the guide; this
    /// is the assertion that makes skipping it fail.
    #[test]
    fn lint_guide_legacy_flag_table_matches_bp_doctor_req() {
        use schemars::schema_for;

        const GUIDE: &str = include_str!("./mcp/resources/guides/lint-diagnostic-model.md");

        let schema = schema_for!(BpDoctorReq);
        let schema_json = serde_json::to_value(&schema).expect("schema to json");
        let mut declared: Vec<String> = schema_json
            .get("properties")
            .and_then(|p| p.as_object())
            .expect("BpDoctorReq properties")
            .keys()
            .filter(|k| k.starts_with("disable_") && k.ends_with("_lint"))
            .cloned()
            .collect();
        declared.sort();

        // The table rows: every `| \`disable_x_lint\` | ... |` line in the
        // guide. The section has the only such rows in the file.
        let mut documented: Vec<String> = GUIDE
            .lines()
            .filter_map(|l| l.strip_prefix("| `disable_"))
            .filter_map(|rest| rest.split('`').next())
            .map(|name| format!("disable_{name}"))
            .filter(|name| name.ends_with("_lint"))
            .collect();
        documented.sort();

        assert_eq!(
            documented, declared,
            "the guide's legacy-flag table and BpDoctorReq's `disable_*_lint` fields \
             have drifted apart"
        );

        // …and the sentence introducing the table counts the same rows.
        let count_word = match declared.len() {
            6 => "six",
            7 => "seven",
            8 => "eight",
            9 => "nine",
            n => panic!("extend the numeral table for {n} flags"),
        };
        assert!(
            GUIDE.contains(&format!("The {count_word} `bp_doctor` request flags")),
            "the guide must introduce the table as \"The {count_word} `bp_doctor` request \
             flags\" ({} rows)",
            declared.len()
        );
    }

    /// Annotation regression guard: every `swarm_run.blueprint` variant must
    /// expose `type: object` in the JSON Schema (either directly or via a
    /// `oneOf` branch). Layer 1 of the issue was that a bare `JsonValue`
    /// omitted `type` entirely and the MCP client fell back to
    /// string-encoding the payload.
    #[test]
    fn swarm_run_blueprint_schema_declares_object_type() {
        use schemars::schema_for;
        let schema = schema_for!(SwarmRunReq);
        let schema_json = serde_json::to_value(&schema).expect("schema to json");
        let defs = schema_json.get("$defs").expect("$defs");

        // Resolve BlueprintInput (referenced from properties.blueprint).
        let input = defs.get("BlueprintInput").expect("BlueprintInput def");
        let anyof = input
            .get("anyOf")
            .expect("BlueprintInput anyOf")
            .as_array()
            .unwrap();

        // Every anyOf branch must resolve to an object-typed schema:
        //   - Selector branch: $ref → BlueprintSelector (oneOf of objects)
        //   - BareInline branch: direct `type: "object"`
        for (i, branch) in anyof.iter().enumerate() {
            if let Some(t) = branch.get("type").and_then(|v| v.as_str()) {
                assert_eq!(t, "object", "branch {i}: {branch}");
            } else if let Some(r) = branch.get("$ref").and_then(|v| v.as_str()) {
                let name = r.rsplit('/').next().unwrap();
                let referenced = defs.get(name).expect("resolves def");
                let oneof = referenced
                    .get("oneOf")
                    .expect("selector def oneOf")
                    .as_array()
                    .unwrap();
                for v in oneof {
                    assert_eq!(
                        v.get("type").and_then(|x| x.as_str()),
                        Some("object"),
                        "selector variant {v}"
                    );
                }
            } else {
                panic!("branch {i} has neither type nor $ref: {branch}");
            }
        }
    }

    // ─── worker HTTP tools (mse_worker_fetch / mse_worker_submit) ──────────

    #[tokio::test]
    async fn mse_worker_fetch_rejects_malformed_task_id_before_network() {
        let server = MseServer::new();
        let err = server
            .mse_worker_fetch(Parameters(WorkerFetchReq {
                worker_handle: "wh-deadbeef".into(),
                // Wrong prefix — must fail at parse, before any HTTP I/O
                // (base_url is a black-hole address on purpose).
                base_url: Some("http://127.0.0.1:1".into()),
                task_id: Some("T-abc".into()),
                system_ref_path: None,
            }))
            .await
            .unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("invalid task_id"), "err: {msg}");
    }

    /// Without an explicit base_url and with no Spawn frame having passed
    /// through this process, the tools must fail loud with guidance instead
    /// of guessing an endpoint.
    #[tokio::test]
    async fn mse_worker_tools_require_a_route_or_explicit_params() {
        let server = MseServer::new();
        let err = server
            .mse_worker_fetch(Parameters(WorkerFetchReq {
                worker_handle: "wh-noroute".into(),
                base_url: None,
                task_id: None,
                system_ref_path: None,
            }))
            .await
            .unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("no Spawn route"), "err: {msg}");

        let err = server
            .mse_worker_submit(Parameters(WorkerSubmitReq {
                worker_handle: "wh-noroute".into(),
                base_url: None,
                body: "RESULT".into(),
                ok: None,
                name: None,
                degradations: None,
                stats: None,
            }))
            .await
            .unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("no Spawn route"), "err: {msg}");
    }

    /// Round-trips both tools against a real in-process `mse serve` router.
    /// A bogus (never-minted) handle exercises the full HTTP path — URL
    /// shape, Bearer header, query encoding, status/error surfacing —
    /// without needing a live dispatch.
    #[tokio::test]
    async fn mse_worker_fetch_and_submit_hit_the_http_endpoints() {
        let engine = Engine::new(EngineCfg::default());
        let router = mlua_swarm_server::build_router(engine);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let base_url = format!("http://{addr}");

        let server = MseServer::new();
        let err = server
            .mse_worker_fetch(Parameters(WorkerFetchReq {
                worker_handle: "wh-deadbeef".into(),
                base_url: Some(base_url.clone()),
                task_id: Some("ST-nope".into()),
                system_ref_path: None,
            }))
            .await
            .expect_err("unknown handle must surface the HTTP error");
        let msg = format!("{err:?}");
        assert!(msg.contains("worker fetch: HTTP"), "err: {msg}");

        let err = server
            .mse_worker_submit(Parameters(WorkerSubmitReq {
                worker_handle: "wh-deadbeef".into(),
                base_url: Some(base_url),
                body: "RESULT".into(),
                ok: None,
                name: None,
                degradations: None,
                stats: None,
            }))
            .await
            .expect_err("unknown handle must surface the HTTP error");
        let msg = format!("{err:?}");
        assert!(msg.contains("expected 204"), "err: {msg}");
    }

    /// GH #36: `name` and `ok=false` are mutually exclusive — the
    /// mismatch must be rejected as an MCP `invalid_params` error *before*
    /// any HTTP I/O (base_url is a black-hole address on purpose, so a
    /// network attempt would hang/timeout instead of failing fast).
    #[tokio::test]
    async fn mse_worker_submit_rejects_name_with_ok_false() {
        let server = MseServer::new();
        let err = server
            .mse_worker_submit(Parameters(WorkerSubmitReq {
                worker_handle: "wh-deadbeef".into(),
                base_url: Some("http://127.0.0.1:1".into()),
                body: "part body".into(),
                ok: Some(false),
                name: Some("plan.md".into()),
                degradations: None,
                stats: None,
            }))
            .await
            .unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("mutually exclusive"), "err: {msg}");
    }

    /// GH #36: a `name`-bearing submit call hits `POST
    /// /v1/worker/artifact?name=<name>` (not `/v1/worker/submit`) against a
    /// real in-process router — same "bogus handle surfaces the HTTP
    /// error" shape as the sibling submit test above, confirming the URL
    /// routing switch actually reaches the artifact endpoint.
    #[tokio::test]
    async fn mse_worker_submit_with_name_hits_the_artifact_endpoint() {
        let engine = Engine::new(EngineCfg::default());
        let router = mlua_swarm_server::build_router(engine);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let base_url = format!("http://{addr}");

        let server = MseServer::new();
        let err = server
            .mse_worker_submit(Parameters(WorkerSubmitReq {
                worker_handle: "wh-deadbeef".into(),
                base_url: Some(base_url),
                body: "part body".into(),
                ok: None,
                name: Some("plan.md".into()),
                degradations: None,
                stats: None,
            }))
            .await
            .expect_err("unknown handle must surface the HTTP error");
        let msg = format!("{err:?}");
        // Same shape as the plain-submit sibling test above (an unknown
        // handle fails handle resolution inside the handler, not routing —
        // a nonexistent route would 404 instead of reaching this
        // HTTP-status-surfacing error path at all).
        assert!(msg.contains("expected 204"), "err: {msg}");
    }

    /// GH #32: a `degradations`-bearing submit call POSTs each entry to
    /// `/v1/worker/degradation` BEFORE the plain submit — against a real
    /// in-process router, a bogus (never-minted) handle fails handle
    /// resolution inside `worker_degradation` itself (500, not 204), which
    /// must surface as a `worker degradation: HTTP ...` error — proving
    /// the pre-submit POST actually fires and its failure short-circuits
    /// before the submit POST is ever attempted.
    #[tokio::test]
    async fn mse_worker_submit_posts_degradations_before_submit() {
        let engine = Engine::new(EngineCfg::default());
        let router = mlua_swarm_server::build_router(engine);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let base_url = format!("http://{addr}");

        let server = MseServer::new();
        let err = server
            .mse_worker_submit(Parameters(WorkerSubmitReq {
                worker_handle: "wh-deadbeef".into(),
                base_url: Some(base_url),
                body: "RESULT".into(),
                ok: None,
                name: None,
                degradations: Some(vec![DegradationInput {
                    tool: "code_index".into(),
                    error: "unavailable".into(),
                    fallback: "grep".into(),
                    note: None,
                }]),
                stats: None,
            }))
            .await
            .expect_err("unknown handle must surface the HTTP error");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("worker degradation: HTTP"),
            "the pre-submit degradation POST must fail first, not the plain submit: {msg}"
        );
        assert!(
            !msg.contains("worker submit: HTTP"),
            "the plain submit POST must never fire once the degradation POST fails: {msg}"
        );
    }

    /// GH #32: without `degradations`, the request path is byte-for-byte
    /// the pre-#32 behavior — the error is the existing `worker submit:
    /// HTTP ...` message, proving the new field is truly opt-in.
    #[tokio::test]
    async fn mse_worker_submit_without_degradations_unchanged() {
        let engine = Engine::new(EngineCfg::default());
        let router = mlua_swarm_server::build_router(engine);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let base_url = format!("http://{addr}");

        let server = MseServer::new();
        let err = server
            .mse_worker_submit(Parameters(WorkerSubmitReq {
                worker_handle: "wh-deadbeef".into(),
                base_url: Some(base_url),
                body: "RESULT".into(),
                ok: None,
                name: None,
                degradations: None,
                stats: None,
            }))
            .await
            .expect_err("unknown handle must surface the HTTP error");
        let msg = format!("{err:?}");
        assert!(msg.contains("worker submit: HTTP"), "err: {msg}");
    }

    /// A `stats`-bearing submit call POSTs to `/v1/worker/stats` BEFORE
    /// the submit itself — same proof shape as the degradation sibling
    /// above: against a real in-process router a bogus (never-minted)
    /// handle fails handle resolution inside `worker_stats` (not 204),
    /// which must surface as a `worker stats: HTTP ...` error, showing
    /// the pre-submit POST fires and short-circuits before the submit
    /// POST is attempted. Ordering is the whole point: the dispatcher
    /// folds recorded stats at outcome time, which the submit triggers.
    #[tokio::test]
    async fn mse_worker_submit_posts_stats_before_submit() {
        let engine = Engine::new(EngineCfg::default());
        let router = mlua_swarm_server::build_router(engine);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let base_url = format!("http://{addr}");

        let server = MseServer::new();
        let err = server
            .mse_worker_submit(Parameters(WorkerSubmitReq {
                worker_handle: "wh-deadbeef".into(),
                base_url: Some(base_url),
                body: "RESULT".into(),
                ok: None,
                name: None,
                degradations: None,
                stats: Some(StatsInput {
                    worker_kind: Some("operator".into()),
                    model: Some("test-model".into()),
                    usage: Some(TokenUsage {
                        input_tokens: 1200,
                        output_tokens: 340,
                        total_tokens: 1540,
                    }),
                    num_turns: Some(3),
                    adapter_data: None,
                }),
            }))
            .await
            .expect_err("unknown handle must surface the HTTP error");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("worker stats: HTTP"),
            "the pre-submit stats POST must fail first, not the plain submit: {msg}"
        );
        assert!(
            !msg.contains("worker submit: HTTP"),
            "the plain submit POST must never fire once the stats POST fails: {msg}"
        );
    }

    /// Without `stats`, the request path is byte-for-byte the pre-existing
    /// behavior — the error is the plain `worker submit: HTTP ...`
    /// message, proving the new field is truly opt-in.
    #[tokio::test]
    async fn mse_worker_submit_without_stats_unchanged() {
        let engine = Engine::new(EngineCfg::default());
        let router = mlua_swarm_server::build_router(engine);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let base_url = format!("http://{addr}");

        let server = MseServer::new();
        let err = server
            .mse_worker_submit(Parameters(WorkerSubmitReq {
                worker_handle: "wh-deadbeef".into(),
                base_url: Some(base_url),
                body: "RESULT".into(),
                ok: None,
                name: None,
                degradations: None,
                stats: None,
            }))
            .await
            .expect_err("unknown handle must surface the HTTP error");
        let msg = format!("{err:?}");
        assert!(msg.contains("worker submit: HTTP"), "err: {msg}");
        assert!(!msg.contains("worker stats"), "err: {msg}");
    }

    // --- worker_stats_endpoint_url (pure URL-building) ---

    #[test]
    fn worker_stats_endpoint_url_shape() {
        let with_slash = worker_stats_endpoint_url("http://127.0.0.1:7777/").unwrap();
        let without_slash = worker_stats_endpoint_url("http://127.0.0.1:7777").unwrap();
        assert_eq!(with_slash.as_str(), without_slash.as_str());
        assert_eq!(
            without_slash.as_str(),
            "http://127.0.0.1:7777/v1/worker/stats"
        );
        assert_eq!(without_slash.path(), "/v1/worker/stats");
        assert_eq!(without_slash.query_pairs().count(), 0);
    }

    #[test]
    fn worker_stats_endpoint_url_rejects_malformed_base_url() {
        let err = worker_stats_endpoint_url("not a url").unwrap_err();
        assert!(!err.is_empty());
    }

    // --- aggregate_run_stats (pure folding) ---

    /// Two reported steps plus the dispatcher-measured durations: totals
    /// sum both usages, `steps_with_stats` matches `steps_total`, and
    /// each per-step row keeps the cost fields.
    #[test]
    fn aggregate_run_stats_sums_usage_and_durations() {
        let entries = vec![
            serde_json::json!({
                "step_ref": "planner",
                "status": "passed",
                "attempt": 1,
                "duration_ms": 1200,
                "worker_kind": "operator",
                "model": "model-a",
                "usage": {"input_tokens": 100, "output_tokens": 20, "total_tokens": 120},
            }),
            serde_json::json!({
                "step_ref": "writer",
                "status": "passed",
                "attempt": 2,
                "duration_ms": 800,
                "worker_kind": "agent_block",
                "model": "model-b",
                "usage": {"input_tokens": 5, "output_tokens": 7, "total_tokens": 12},
            }),
        ];
        let out = aggregate_run_stats(&entries);
        assert_eq!(out["totals"]["input_tokens"], 105);
        assert_eq!(out["totals"]["output_tokens"], 27);
        assert_eq!(out["totals"]["total_tokens"], 132);
        assert_eq!(out["totals"]["duration_ms_sum"], 2000);
        assert_eq!(out["totals"]["steps_with_stats"], 2);
        assert_eq!(out["totals"]["steps_total"], 2);
        assert_eq!(out["steps"][0]["step_ref"], "planner");
        assert_eq!(out["steps"][0]["attempt"], 1);
        assert_eq!(out["steps"][1]["worker_kind"], "agent_block");
    }

    /// `by_model` groups by the self-reported model; two steps on one
    /// model fold into one bucket.
    #[test]
    fn aggregate_run_stats_groups_by_model() {
        let entries = vec![
            serde_json::json!({
                "step_ref": "a", "model": "model-a",
                "usage": {"input_tokens": 10, "output_tokens": 1, "total_tokens": 11},
            }),
            serde_json::json!({
                "step_ref": "b", "model": "model-a",
                "usage": {"input_tokens": 30, "output_tokens": 3, "total_tokens": 33},
            }),
            serde_json::json!({
                "step_ref": "c", "model": "model-b",
                "usage": {"input_tokens": 5, "output_tokens": 5, "total_tokens": 10},
            }),
        ];
        let out = aggregate_run_stats(&entries);
        assert_eq!(out["by_model"]["model-a"]["steps"], 2);
        assert_eq!(out["by_model"]["model-a"]["input_tokens"], 40);
        assert_eq!(out["by_model"]["model-a"]["total_tokens"], 44);
        assert_eq!(out["by_model"]["model-b"]["steps"], 1);
        assert!(out["by_model"].get("model-c").is_none());
    }

    /// Stats are optional at every worker boundary: an entry without
    /// `usage` contributes nothing to the token totals and is excluded
    /// from `steps_with_stats` (so a reader can tell partial coverage
    /// from a genuinely cheap run), while its dispatcher-measured
    /// `duration_ms` still counts and a model-less entry lands in no
    /// bucket.
    #[test]
    fn aggregate_run_stats_skips_entries_without_stats() {
        let entries = vec![
            serde_json::json!({
                "step_ref": "reported", "duration_ms": 500, "model": "model-a",
                "usage": {"input_tokens": 10, "output_tokens": 2, "total_tokens": 12},
            }),
            serde_json::json!({"step_ref": "silent", "status": "passed", "duration_ms": 700}),
        ];
        let out = aggregate_run_stats(&entries);
        assert_eq!(out["totals"]["input_tokens"], 10);
        assert_eq!(out["totals"]["total_tokens"], 12);
        assert_eq!(out["totals"]["duration_ms_sum"], 1200);
        assert_eq!(out["totals"]["steps_with_stats"], 1);
        assert_eq!(out["totals"]["steps_total"], 2);
        assert_eq!(out["by_model"].as_object().map(|m| m.len()), Some(1));
        // The unreported step still appears, carrying only what it had.
        assert_eq!(out["steps"][1]["step_ref"], "silent");
        assert!(out["steps"][1].get("usage").is_none());
        assert!(out["steps"][1].get("model").is_none());
    }

    /// A run with no step entries at all reports zeros, not an error —
    /// `swarm_run_stats` is a read tool and an empty run is a legitimate
    /// (just-launched) state.
    #[test]
    fn aggregate_run_stats_empty_is_all_zeros() {
        let out = aggregate_run_stats(&[]);
        assert_eq!(out["totals"]["input_tokens"], 0);
        assert_eq!(out["totals"]["output_tokens"], 0);
        assert_eq!(out["totals"]["total_tokens"], 0);
        assert_eq!(out["totals"]["duration_ms_sum"], 0);
        assert_eq!(out["totals"]["steps_with_stats"], 0);
        assert_eq!(out["totals"]["steps_total"], 0);
        assert_eq!(out["steps"].as_array().map(Vec::len), Some(0));
        assert_eq!(out["by_model"].as_object().map(|m| m.len()), Some(0));
    }

    /// `swarm_run_stats` keeps the "no such run" / "could not ask"
    /// distinction its strict fetch exists for: a server that answers
    /// `404` for an unknown id must surface as `invalid_params`, not as
    /// an empty report and not as a transport error.
    #[tokio::test]
    async fn swarm_run_stats_unknown_run_id_returns_invalid_params() {
        let engine = Engine::new(EngineCfg::default());
        let router = mlua_swarm_server::build_router(engine);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        let server = MseServer::new();
        let err = server
            .swarm_run_stats(Parameters(SwarmRunStatsReq {
                run_id: "R-does-not-exist".into(),
                bind: Some(addr.to_string()),
            }))
            .await
            .expect_err("an unknown run id must fail loud");
        let msg = format!("{err:?}");
        assert!(msg.contains("run not found"), "err: {msg}");
    }

    /// The sibling half of the error contract: an unreachable bind is a
    /// transport fault (`internal_error`), never reported as a missing
    /// run. Port 1 (RFC 6335 reserved) always refuses.
    #[tokio::test]
    async fn swarm_run_stats_unreachable_server_is_not_reported_as_missing_run() {
        let server = MseServer::new();
        let err = server
            .swarm_run_stats(Parameters(SwarmRunStatsReq {
                run_id: "R-whatever".into(),
                bind: Some("127.0.0.1:1".into()),
            }))
            .await
            .expect_err("an unreachable server must fail loud");
        let msg = format!("{err:?}");
        assert!(msg.contains("run stats:"), "err: {msg}");
        assert!(!msg.contains("run not found"), "err: {msg}");
    }

    // --- worker_submit_endpoint_url (pure URL-building) tests ---

    #[test]
    fn worker_submit_endpoint_url_no_name_hits_submit() {
        let url = worker_submit_endpoint_url("http://127.0.0.1:7777", None).unwrap();
        assert_eq!(url.as_str(), "http://127.0.0.1:7777/v1/worker/submit");
    }

    #[test]
    fn worker_submit_endpoint_url_trims_trailing_slash() {
        let with_slash = worker_submit_endpoint_url("http://127.0.0.1:7777/", None).unwrap();
        let without_slash = worker_submit_endpoint_url("http://127.0.0.1:7777", None).unwrap();
        assert_eq!(with_slash.as_str(), without_slash.as_str());
    }

    #[test]
    fn worker_submit_endpoint_url_with_name_hits_artifact_and_round_trips() {
        let url = worker_submit_endpoint_url("http://127.0.0.1:7777", Some("plan.md")).unwrap();
        assert_eq!(url.path(), "/v1/worker/artifact");
        let name = url
            .query_pairs()
            .find(|(k, _)| k == "name")
            .map(|(_, v)| v.into_owned());
        assert_eq!(name.as_deref(), Some("plan.md"));
    }

    /// Names with dots, spaces, and non-ASCII must round-trip through the
    /// query string unscathed — `Url::query_pairs`/`query_pairs_mut` handle
    /// the percent-encoding; this only asserts the decoded value survives,
    /// not any particular encoded literal (encoding scheme is an
    /// implementation detail of the `url` crate).
    #[test]
    fn worker_submit_endpoint_url_name_round_trips_special_chars() {
        for name in ["a.b.c", "plan file.md", "計画.md", "a&b=c"] {
            let url = worker_submit_endpoint_url("http://127.0.0.1:7777", Some(name)).unwrap();
            let decoded = url
                .query_pairs()
                .find(|(k, _)| k == "name")
                .map(|(_, v)| v.into_owned());
            assert_eq!(decoded.as_deref(), Some(name), "name={name}");
        }
    }

    #[test]
    fn worker_submit_endpoint_url_rejects_malformed_base_url() {
        let err = worker_submit_endpoint_url("not a url", None).unwrap_err();
        assert!(!err.is_empty());
    }

    // --- worker_degradation_endpoint_url (GH #32, pure URL-building) ---

    #[test]
    fn worker_degradation_endpoint_url_shape() {
        let with_slash = worker_degradation_endpoint_url("http://127.0.0.1:7777/").unwrap();
        let without_slash = worker_degradation_endpoint_url("http://127.0.0.1:7777").unwrap();
        assert_eq!(with_slash.as_str(), without_slash.as_str());
        assert_eq!(
            without_slash.as_str(),
            "http://127.0.0.1:7777/v1/worker/degradation"
        );
        assert_eq!(without_slash.path(), "/v1/worker/degradation");
        assert_eq!(without_slash.query_pairs().count(), 0);
    }

    #[test]
    fn worker_degradation_endpoint_url_rejects_malformed_base_url() {
        let err = worker_degradation_endpoint_url("not a url").unwrap_err();
        assert!(!err.is_empty());
    }

    /// GH #31 test helper: seeds a real task + baked (possibly
    /// over-threshold) `system` prompt + a bound `wh-` short handle, the
    /// exact shape `Engine::dispatch_attempt` would have produced — mirrors
    /// `crates/mlua-swarm-server/src/worker.rs`'s own
    /// `seed_task_with_handle` test helper (not reusable directly: it's
    /// private to that crate), built from the public `Engine::with_state`
    /// + `core::state` surface.
    async fn gh31_seed_task_with_handle(
        engine: &Engine,
        task_id: &StepId,
        agent: &str,
        attempt: u32,
        system: Option<String>,
    ) -> String {
        let handle = format!("wh-{}", mlua_swarm::types::secure_hex(4));
        let task_id = task_id.clone();
        let agent = agent.to_string();
        let handle_clone = handle.clone();
        engine
            .with_state("test.gh31_seed_task_with_handle", move |s| {
                let mut task = mlua_swarm::core::state::TaskState::new(
                    task_id.clone(),
                    mlua_swarm::core::state::TaskSpec {
                        agent: agent.clone(),
                        initial_directive: serde_json::json!("x"),
                        step_ctx: None,
                        check_policy: None,
                    },
                );
                task.attempt = attempt;
                s.tasks.insert(task_id.clone(), task);
                s.prompts
                    .insert((task_id.clone(), attempt), serde_json::json!("x"));
                s.systems.insert((task_id.clone(), attempt), system);
                let token = mlua_swarm::CapToken {
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
            .expect("gh31_seed_task_with_handle");
        handle
    }

    /// GH #31 E2E: a real server, with `system_ref` config
    /// tuned to a tiny threshold so an intentionally-oversized
    /// `system_prompt` triggers `File`-mode by-reference delivery, then
    /// `mse_worker_fetch` resolves it — asserts `{ok: true, path, sha256,
    /// size_bytes}` in `system_ref_resolution`, that the sha256 matches a
    /// manually-computed hash of the known input, and that the file at
    /// `path` contains the exact original content.
    #[tokio::test]
    async fn mse_worker_fetch_resolves_system_ref_file_mode_end_to_end() {
        let unique = format!("{}-{}", std::process::id(), StepId::new());
        let mut cfg = EngineCfg::default();
        cfg.system_ref.threshold_bytes = 16;
        cfg.system_ref.mode = mlua_swarm::types::SystemRefMode::File;
        cfg.system_ref.store_dir =
            std::env::temp_dir().join(format!("mse-mcp-system-ref-{unique}"));
        let engine = Engine::new(cfg);

        let task_id = StepId::new();
        let rendered =
            "this system prompt is deliberately longer than the 16 byte threshold".to_string();
        let handle =
            gh31_seed_task_with_handle(&engine, &task_id, "planner", 1, Some(rendered.clone()))
                .await;

        let router = mlua_swarm_server::build_router(engine);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let base_url = format!("http://{addr}");

        let server = MseServer::new();
        let result = server
            .mse_worker_fetch(Parameters(WorkerFetchReq {
                worker_handle: handle,
                base_url: Some(base_url),
                task_id: Some(task_id.as_str().to_string()),
                system_ref_path: None,
            }))
            .await
            .expect("mse_worker_fetch");
        let value: JsonValue =
            serde_json::from_str(&extract_text_payload(&result)).expect("mse_worker_fetch json");

        assert!(
            value.get("system").is_none(),
            "over-threshold payload must not also inline `system`: {value}"
        );
        assert!(
            value.get("system_ref").is_some(),
            "payload must still carry the original system_ref: {value}"
        );
        assert_eq!(value["task_id"], task_id.as_str());
        assert_eq!(value["attempt"], 1);
        assert_eq!(value["agent"], "planner");

        let resolution = value
            .get("system_ref_resolution")
            .expect("system_ref_resolution present on success");
        assert_eq!(resolution["ok"], true, "resolution: {resolution}");

        use sha2::Digest;
        let expected_sha256 = hex::encode(sha2::Sha256::digest(rendered.as_bytes()));
        assert_eq!(resolution["sha256"], expected_sha256);
        assert_eq!(resolution["size_bytes"], rendered.len());

        let path = resolution["path"].as_str().expect("path is a string");
        let written = tokio::fs::read_to_string(path)
            .await
            .expect("mse_worker_fetch must have written the resolved file");
        assert_eq!(written, rendered);
    }

    /// GH #31 E2E, `hash_mismatch` path: a minimal fake HTTP
    /// server (not the real `Engine`) serves a `WorkerPayload` whose
    /// `system_ref.sha256` deliberately does not match the bytes served at
    /// `system_ref.uri` (simulating server/client corruption or a stale
    /// hash). A fake server (rather than tampering with the real `Engine`'s
    /// `File`-mode store) is necessary here: `apply_system_ref_threshold`
    /// re-renders and re-writes the store file from the live in-memory
    /// `system` string on every `/v1/worker/prompt` fetch (Phase 3 Option
    /// B's documented re-fetch behavior), so any tamper made against a real
    /// engine's store file gets silently overwritten with the original
    /// (correct) content the moment `mse_worker_fetch`'s own outer fetch
    /// re-triggers that route — there is no race-free way to hold a real
    /// `Engine`'s store content mismatched across the outer fetch and the
    /// by-reference download. Expects a standalone `{ok: false, stage:
    /// "hash_mismatch", error}` value, not an `McpError`, and not the
    /// passed-through payload.
    #[tokio::test]
    async fn mse_worker_fetch_reports_hash_mismatch_after_one_retry() {
        const ACTUAL_BYTES: &[u8] = b"actual bytes served by the fake system_ref route";
        const WRONG_SHA256: &str =
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

        let app = axum::Router::new()
            .route(
                "/v1/worker/prompt",
                axum::routing::get(|| async {
                    axum::Json(serde_json::json!({
                        "task_id": "ST-fakefakefakefake",
                        "attempt": 1,
                        "agent": "planner",
                        "prompt": "x",
                        "system_ref": {
                            "uri": "/system-bytes",
                            "sha256": WRONG_SHA256,
                            "size_bytes": ACTUAL_BYTES.len(),
                            "mode": "http",
                        },
                    }))
                }),
            )
            .route(
                "/system-bytes",
                axum::routing::get(|| async { ACTUAL_BYTES.to_vec() }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let base_url = format!("http://{addr}");

        let server = MseServer::new();
        let result = server
            .mse_worker_fetch(Parameters(WorkerFetchReq {
                worker_handle: "wh-deadbeef".into(),
                base_url: Some(base_url),
                task_id: Some("ST-fakefakefakefake".into()),
                system_ref_path: None,
            }))
            .await
            .expect("mse_worker_fetch must return a value-level result, not an McpError");
        let value: JsonValue =
            serde_json::from_str(&extract_text_payload(&result)).expect("mse_worker_fetch json");

        assert_eq!(value["ok"], false, "value: {value}");
        assert_eq!(value["stage"], "hash_mismatch", "value: {value}");
        assert!(
            value.get("error").and_then(|e| e.as_str()).is_some(),
            "value: {value}"
        );
    }

    /// `projection-adapter` removal confirmation: `mse_ctx_get` no longer
    /// exists as an MCP tool — the Worker axis now gets prior
    /// steps' OUTPUT pointers automatically via `context.steps` on `GET
    /// /v1/worker/prompt` (see `mlua_swarm::core::agent_context`'s module
    /// doc), so the tool's existence reason (a manual pull wrapper over
    /// `GET /v1/tasks/:id/ctx`) is gone. `MseServer::tool_router()`'s tool
    /// name list is the single source of truth for what this MCP server
    /// exposes; asserting its absence here catches a regression re-adding
    /// it under the same name.
    #[test]
    fn mse_ctx_get_tool_is_not_registered() {
        let tools = MseServer::tool_router().list_all();
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        assert!(
            !names.contains(&"mse_ctx_get"),
            "mse_ctx_get must be retired: {names:?}"
        );
    }

    // ─── handover surface tool registration (§4.3 / §4.5 / W5) ──────────

    /// The four routes the handover surface is made of must each have a
    /// tool. Before they did, the three reads answered `401` to every
    /// caller that was not this process (the Bearer never leaves it) while
    /// the acquire stayed wide open — the guard unreachable and the verb
    /// it guards reachable, which is the inversion this family fixes.
    #[test]
    fn handover_surface_tools_are_registered() {
        let tools = MseServer::tool_router().list_all();
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        for expected in [
            "mse_run_assignees",
            "mse_run_handover",
            "mse_run_material",
            "mse_run_acquire",
        ] {
            assert!(
                names.contains(&expected),
                "{expected} must be registered: {names:?}"
            );
        }
    }

    /// **B2**: the bearer takes no part in assignment. The three reads
    /// select which session's token to present with `sid`; the acquire has
    /// no such argument at all, because there is no token in it to select
    /// — a `sid` on that surface would be the first step towards the
    /// bearer deciding who holds a seat.
    #[test]
    fn only_the_handover_reads_take_a_sid() {
        let tools = MseServer::tool_router().list_all();
        let properties = |name: &str| -> JsonValue {
            tools
                .iter()
                .find(|t| t.name.as_ref() == name)
                .unwrap_or_else(|| panic!("tool {name} not registered"))
                .input_schema
                .get("properties")
                .cloned()
                .unwrap_or_else(|| panic!("{name} must declare properties"))
        };
        for read in ["mse_run_assignees", "mse_run_handover", "mse_run_material"] {
            let props = properties(read);
            assert!(
                props.get("sid").is_some(),
                "{read} presents a Bearer, so it has to be able to say whose: {props}"
            );
            assert!(props.get("run_id").is_some(), "{read} is Run-scoped");
        }
        let acquire = properties("mse_run_acquire");
        assert!(
            acquire.get("sid").is_none(),
            "an acquire presents no Bearer and must not ask for one: {acquire}"
        );
        for field in ["run_id", "op", "desc", "slot"] {
            assert!(
                acquire.get(field).is_some(),
                "mse_run_acquire.{field} must be on the tool surface: {acquire}"
            );
        }
    }

    /// `mse_run_material` answers about one step and has no default for
    /// which, so `step_id` is required rather than optional — and `slot`
    /// on the acquire is the opposite (the server resolves the sole
    /// declared Operator when it is omitted).
    #[test]
    fn handover_tool_required_arguments_match_the_routes() {
        let tools = MseServer::tool_router().list_all();
        let required = |name: &str| -> Vec<String> {
            tools
                .iter()
                .find(|t| t.name.as_ref() == name)
                .unwrap_or_else(|| panic!("tool {name} not registered"))
                .input_schema
                .get("required")
                .and_then(|r| r.as_array())
                .map(|r| {
                    r.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default()
        };
        let material = required("mse_run_material");
        for field in ["run_id", "step_id"] {
            assert!(
                material.contains(&field.to_string()),
                "mse_run_material.{field} must be required: {material:?}"
            );
        }
        assert!(
            !material.contains(&"sid".to_string()),
            "sid defaults to this process's sole live session: {material:?}"
        );
        let acquire = required("mse_run_acquire");
        for field in ["run_id", "op", "desc"] {
            assert!(
                acquire.contains(&field.to_string()),
                "mse_run_acquire.{field} must be required — an acquire with no `desc` is one a \
                 later reader cannot tell from any other: {acquire:?}"
            );
        }
        assert!(
            !acquire.contains(&"slot".to_string()),
            "a Blueprint with one declared Operator needs no seat named: {acquire:?}"
        );
    }

    /// The descriptions are what an AI reads to learn the order. The
    /// acquire's is the one that has to carry it: nothing downstream of it
    /// refuses, so "read first" is the whole guard and it exists only as
    /// text.
    #[test]
    fn the_acquire_description_names_the_reads_that_come_first() {
        let tools = MseServer::tool_router().list_all();
        let acquire = tools
            .iter()
            .find(|t| t.name.as_ref() == "mse_run_acquire")
            .expect("mse_run_acquire registered");
        let description = acquire
            .description
            .as_ref()
            .expect("mse_run_acquire must carry a description")
            .to_string();
        for expected in ["mse_operator_list", "mse_run_assignees", "never refuses"] {
            assert!(
                description.contains(expected),
                "the acquire description must point at {expected:?}: {description}"
            );
        }
    }

    // ─── GH #69 launchd lifecycle tool registration ────────

    /// Registration smoke test — confirms the new `bootstrap` tool
    /// appears in `MseServer::tool_router().list_all()` (the same source
    /// of truth `tools/list` JSON-RPC returns). Guards against silent
    /// dispatch loss if a future refactor drops the `#[tool]` attribute.
    #[test]
    fn mlua_swarm_server_bootstrap_registered() {
        let tools = MseServer::tool_router().list_all();
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        assert!(
            names.contains(&"mlua_swarm_server_bootstrap"),
            "mlua_swarm_server_bootstrap must be registered: {names:?}"
        );
    }

    /// Registration smoke test — confirms the new `install` tool appears
    /// in `MseServer::tool_router().list_all()`.
    #[test]
    fn mlua_swarm_server_install_registered() {
        let tools = MseServer::tool_router().list_all();
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        assert!(
            names.contains(&"mlua_swarm_server_install"),
            "mlua_swarm_server_install must be registered: {names:?}"
        );
    }

    /// Registration smoke test — confirms the new `uninstall` tool
    /// appears in `MseServer::tool_router().list_all()`.
    #[test]
    fn mlua_swarm_server_uninstall_registered() {
        let tools = MseServer::tool_router().list_all();
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        assert!(
            names.contains(&"mlua_swarm_server_uninstall"),
            "mlua_swarm_server_uninstall must be registered: {names:?}"
        );
    }

    /// The 4 pre-existing lifecycle tools must remain registered
    /// alongside the 3 new ones — the launchd-forwarder refactor (GH #69) changed only the
    /// bodies (thin forwarders) and the `#[tool(description = ...)]`
    /// literals, never the tool names / signatures. Guards against a
    /// rename drift that would break every existing MCP caller.
    #[test]
    fn existing_launchd_lifecycle_tools_stay_registered() {
        let tools = MseServer::tool_router().list_all();
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        for expected in [
            "mlua_swarm_server_start",
            "mlua_swarm_server_status",
            "mlua_swarm_server_shutdown",
            "mlua_swarm_server_restart",
        ] {
            assert!(
                names.contains(&expected),
                "{expected} must remain registered after the launchd-forwarder rewire: {names:?}"
            );
        }
    }

    /// Schema stability guard — `plist_path` / `cargo_bin` /
    /// `working_dir` are declared as `Option<String>` (not
    /// `Option<PathBuf>`) so schemars emits a concrete `type: "string"`
    /// in the tool inputSchema (see GH #24 any-schema drop). A future
    /// hand that flips the field type to `PathBuf` would silently drop
    /// the field on the MCP wire; this asserts the concrete type
    /// literal stays put. (`working_dir` is the GH #97 rename of
    /// `project_root`; the old name survives only as a serde alias,
    /// which is deliberately invisible in the schema.)
    #[test]
    fn new_lifecycle_tool_paths_resolve_to_string_type_in_schema() {
        let tools = MseServer::tool_router().list_all();
        let by_name = |n: &str| {
            tools
                .iter()
                .find(|t| t.name.as_ref() == n)
                .unwrap_or_else(|| panic!("tool {n} not registered"))
        };
        for (tool_name, field) in [
            ("mlua_swarm_server_bootstrap", "plist_path"),
            ("mlua_swarm_server_install", "cargo_bin"),
            ("mlua_swarm_server_install", "working_dir"),
        ] {
            let schema = &by_name(tool_name).input_schema;
            let prop = schema
                .get("properties")
                .and_then(|p| p.get(field))
                .unwrap_or_else(|| panic!("{tool_name}.properties.{field} present"));
            // Option<String> renders as either `{"type": "string"}` or
            // `{"type": ["string", "null"]}` depending on schemars
            // version; both are concrete-type resolutions (not any-
            // schema drop = `{}`). Accept either shape.
            let ty = prop.get("type").unwrap_or_else(|| {
                panic!(
                    "{tool_name}.{field} missing `type` — schemars any-schema regression \
                     (GH #24): {prop:?}"
                )
            });
            let matches_string = ty == &JsonValue::String("string".into())
                || ty
                    .as_array()
                    .map(|arr| arr.iter().any(|v| v == &JsonValue::String("string".into())))
                    .unwrap_or(false);
            assert!(
                matches_string,
                "{tool_name}.{field}.type must include \"string\": {ty:?}"
            );
        }
    }

    /// GH #24 regression: `Option<JsonValue>` fields on the tool surface
    /// must render with a concrete `type` in the inputSchema. Without the
    /// `#[schemars(schema_with = ...)]` pin schemars emits the any-schema
    /// (`true`) — MCP clients that filter arguments against the schema
    /// then drop the payload silently and callers see the field arrive as
    /// `None` server-side.
    ///
    /// Asserted per tool + field: the JSON Schema fragment at
    /// `properties.<field>` carries a `type` key (either the string
    /// `"object"` for `init_ctx`, or a 6-element array for `value`).
    #[test]
    fn json_value_fields_pin_a_concrete_type_in_input_schema() {
        let tools = MseServer::tool_router().list_all();
        let by_name = |n: &str| {
            tools
                .iter()
                .find(|t| t.name.as_ref() == n)
                .unwrap_or_else(|| panic!("tool {n} not registered"))
        };

        // swarm_run.init_ctx → "object" (flow.ir root ctx is an object).
        let swarm_run_schema = &by_name("swarm_run").input_schema;
        let init_ctx = swarm_run_schema
            .get("properties")
            .and_then(|p| p.get("init_ctx"))
            .expect("swarm_run.properties.init_ctx present");
        let init_ctx_type = init_ctx
            .get("type")
            .unwrap_or_else(|| panic!("swarm_run.init_ctx missing `type` — schemars any-schema regression (GH #24): {init_ctx:?}"));
        assert_eq!(
            init_ctx_type,
            &JsonValue::String("object".into()),
            "swarm_run.init_ctx.type must be \"object\": {init_ctx_type:?}"
        );

        // mse_ack.value → the 6 concrete JSON types (any JSON value).
        let mse_ack_schema = &by_name("mse_ack").input_schema;
        let value = mse_ack_schema
            .get("properties")
            .and_then(|p| p.get("value"))
            .expect("mse_ack.properties.value present");
        let value_type = value.get("type").unwrap_or_else(|| {
            panic!(
                "mse_ack.value missing `type` — schemars any-schema regression (GH #24): {value:?}"
            )
        });
        let arr = value_type
            .as_array()
            .expect("mse_ack.value.type must be an array of type strings");
        for expected in ["object", "array", "string", "number", "boolean", "null"] {
            assert!(
                arr.iter().any(|v| v == &JsonValue::String(expected.into())),
                "mse_ack.value.type missing {expected:?}: {arr:?}"
            );
        }
    }

    // ─── S3 operator client tools: error paths (no network required) ───────

    #[tokio::test]
    async fn mse_pending_wait_unknown_sid_returns_invalid_params() {
        let server = MseServer::new();
        let err = server
            .mse_pending_wait(Parameters(OperatorPendingWaitReq {
                sid: "no-such-sid".into(),
                timeout_ms: Some(10),
            }))
            .await
            .unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("no-such-sid"), "err: {msg}");
    }

    #[tokio::test]
    async fn mse_ack_invalid_kind_returns_invalid_params() {
        let server = MseServer::new();
        let err = server
            .mse_ack(Parameters(OperatorAckReq {
                sid: "whatever".into(),
                req_id: "r1".into(),
                kind: "bogus".into(),
                value: None,
                ok: true,
                error: None,
                stats: None,
            }))
            .await
            .unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("bogus"), "err: {msg}");
    }

    #[tokio::test]
    async fn mse_ack_unknown_sid_returns_invalid_params_for_valid_kind() {
        let server = MseServer::new();
        let err = server
            .mse_ack(Parameters(OperatorAckReq {
                sid: "no-such-sid".into(),
                req_id: "r1".into(),
                kind: "answer".into(),
                value: Some(serde_json::json!({"v": 1})),
                ok: true,
                error: None,
                stats: None,
            }))
            .await
            .unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("no-such-sid"), "err: {msg}");
    }

    #[tokio::test]
    async fn mse_operator_leave_unknown_sid_returns_invalid_params() {
        let server = MseServer::new();
        let err = server
            .mse_operator_leave(Parameters(OperatorLeaveReq {
                sid: "no-such-sid".into(),
            }))
            .await
            .unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("no-such-sid"), "err: {msg}");
    }

    #[tokio::test]
    async fn swarm_cancel_marks_handle_cancelled() {
        let server = MseServer::new();
        // seed a run first
        let _ = server
            .swarm_run(Parameters(SwarmRunReq {
                blueprint: BlueprintInput::BareInline(serde_json::json!({})),
                init_ctx: None,
                timeout_secs: Some(5),
                operator_id: None,
                operator_sid: None,
                operator_slot: None,
                operator_kind: None,
                operator_kind_overrides: None,
                detach: None,
                ttl_secs: None,
            }))
            .await
            .unwrap();
        let run_id = {
            let inner = server.state.read().await;
            inner.runs.keys().next().cloned().unwrap()
        };
        let _ = server
            .swarm_cancel(Parameters(SwarmCancelReq {
                run_id: run_id.clone(),
                bind: None,
            }))
            .await
            .unwrap();
        let inner = server.state.read().await;
        let h = inner.runs.get(&run_id).unwrap();
        assert!(matches!(h.status, RunStatus::Cancelled));
        assert!(h.cancel_requested);
    }

    /// Issue 9b3f225b: after `swarm_cancel`, `swarm_status` must surface
    /// the local `cancel_requested` mark even if the HTTP enrichment
    /// (unreachable server here — port 1) can't reach the server. The
    /// mark is independent from `status`, so it survives regardless of
    /// what the server would have reported.
    #[tokio::test]
    async fn swarm_status_surfaces_cancel_requested_after_cancel() {
        let server = MseServer::new();
        let _ = server
            .swarm_run(Parameters(SwarmRunReq {
                blueprint: BlueprintInput::BareInline(serde_json::json!({})),
                init_ctx: None,
                timeout_secs: Some(5),
                operator_id: None,
                operator_sid: None,
                operator_slot: None,
                operator_kind: None,
                operator_kind_overrides: None,
                detach: None,
                ttl_secs: None,
            }))
            .await
            .unwrap();
        let run_id = {
            let inner = server.state.read().await;
            inner.runs.keys().next().cloned().unwrap()
        };
        let _ = server
            .swarm_cancel(Parameters(SwarmCancelReq {
                run_id: run_id.clone(),
                bind: None,
            }))
            .await
            .unwrap();
        let result = server
            .swarm_status(Parameters(SwarmStatusReq {
                run_id: run_id.clone(),
                // Port 1 is RFC 6335 reserved — connect always refuses,
                // so the HTTP enrichment falls back to the local view.
                bind: Some("127.0.0.1:1".into()),
            }))
            .await
            .unwrap();
        let text = extract_text_payload(&result);
        let body: serde_json::Value = serde_json::from_str(&text).expect("json body");
        assert_eq!(body["cancel_requested"], serde_json::json!(true));
    }

    // --- agent.md size classifier tests (bp_doctor pure logic) ---

    /// Default request thresholds — matches what a caller with no override
    /// gets. Note that `disable_block` defaults to `true` here, so BLOCK is
    /// only exercised in tests that explicitly pass `Some(false)`.
    fn default_thresholds() -> AgentMdThresholds {
        AgentMdThresholds::from_req(None, None, None, None, None)
    }

    /// Same defaults, but with the BLOCK band explicitly re-enabled. Used
    /// by tests that verify the BLOCK classification logic itself.
    fn block_enabled_thresholds() -> AgentMdThresholds {
        AgentMdThresholds::from_req(None, None, None, None, Some(false))
    }

    #[test]
    fn classify_agent_md_severity_ok_at_zero() {
        assert_eq!(
            classify_agent_md_severity(0, 0, &default_thresholds()),
            "OK"
        );
    }

    #[test]
    fn classify_agent_md_severity_ok_just_under_warn() {
        assert_eq!(
            classify_agent_md_severity(
                AGENT_MD_DEFAULT_WARN_BYTES - 1,
                AGENT_MD_DEFAULT_WARN_LINES - 1,
                &default_thresholds()
            ),
            "OK"
        );
    }

    #[test]
    fn classify_agent_md_severity_warn_at_byte_threshold() {
        // exactly 25 KB, 0 lines → WARN by bytes alone.
        assert_eq!(
            classify_agent_md_severity(AGENT_MD_DEFAULT_WARN_BYTES, 0, &default_thresholds()),
            "WARN"
        );
    }

    #[test]
    fn classify_agent_md_severity_warn_at_line_threshold() {
        // 0 bytes, 200 lines → WARN by lines alone.
        assert_eq!(
            classify_agent_md_severity(0, AGENT_MD_DEFAULT_WARN_LINES, &default_thresholds()),
            "WARN"
        );
    }

    #[test]
    fn classify_agent_md_severity_block_at_byte_threshold() {
        // exactly 50 KB, few lines → BLOCK by bytes alone (block band opted in).
        assert_eq!(
            classify_agent_md_severity(
                AGENT_MD_DEFAULT_BLOCK_BYTES,
                10,
                &block_enabled_thresholds()
            ),
            "BLOCK"
        );
    }

    #[test]
    fn classify_agent_md_severity_block_at_line_threshold() {
        // small bytes, 500 lines → BLOCK by lines alone (block band opted in).
        assert_eq!(
            classify_agent_md_severity(
                1024,
                AGENT_MD_DEFAULT_BLOCK_LINES,
                &block_enabled_thresholds()
            ),
            "BLOCK"
        );
    }

    #[test]
    fn classify_agent_md_severity_block_dominates_warn_mixed() {
        // 25 KB (WARN by bytes) but 500 lines (BLOCK by lines) → BLOCK wins
        // (block band opted in).
        assert_eq!(
            classify_agent_md_severity(
                AGENT_MD_DEFAULT_WARN_BYTES,
                AGENT_MD_DEFAULT_BLOCK_LINES,
                &block_enabled_thresholds()
            ),
            "BLOCK"
        );
    }

    #[test]
    fn classify_agent_md_severity_default_disables_block_downgrades_to_warn() {
        // 60 KB, 600 lines would BLOCK if opted in; with default (disable_block=true) → WARN.
        assert_eq!(
            classify_agent_md_severity(60 * 1024, 600, &default_thresholds()),
            "WARN"
        );
    }

    #[test]
    fn classify_agent_md_severity_default_disables_block_leaves_ok_alone() {
        // Small file stays OK under defaults regardless of disable_block.
        assert_eq!(
            classify_agent_md_severity(1024, 20, &default_thresholds()),
            "OK"
        );
    }

    #[test]
    fn classify_agent_md_severity_custom_warn_override_raises_bar() {
        // Raise both WARN (100 KB / 1000 lines) and BLOCK (200 KB / 2000 lines),
        // with BLOCK band explicitly opted in so we can observe all 3 bands.
        let t = AgentMdThresholds::from_req(
            Some(100 * 1024),
            Some(1000),
            Some(200 * 1024),
            Some(2000),
            Some(false),
        );
        assert_eq!(classify_agent_md_severity(50 * 1024, 400, &t), "OK");
        assert_eq!(classify_agent_md_severity(120 * 1024, 400, &t), "WARN");
        assert_eq!(classify_agent_md_severity(210 * 1024, 400, &t), "BLOCK");
    }

    #[test]
    fn aggregate_agent_md_verdict_empty_is_ok() {
        assert_eq!(aggregate_agent_md_verdict(&[]), "OK");
    }

    #[test]
    fn aggregate_agent_md_verdict_all_ok() {
        assert_eq!(aggregate_agent_md_verdict(&["OK", "OK", "OK"]), "OK");
    }

    #[test]
    fn aggregate_agent_md_verdict_warn_dominates_ok() {
        assert_eq!(aggregate_agent_md_verdict(&["OK", "WARN", "OK"]), "WARN");
    }

    #[test]
    fn aggregate_agent_md_verdict_block_dominates_all() {
        assert_eq!(
            aggregate_agent_md_verdict(&["OK", "WARN", "BLOCK", "WARN"]),
            "BLOCK"
        );
    }

    // ─── explain-agent: diff_tools drift classifier ──────────────

    fn strs(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn diff_tools_exact_match_yields_matched_only() {
        let declared = strs(&["Read", "Edit"]);
        let wrapper = strs(&["Read", "Edit"]);
        let drift = diff_tools(&declared, &wrapper);
        assert_eq!(drift.matched, strs(&["Edit", "Read"]));
        assert!(drift.declared_only.is_empty());
        assert!(drift.wrapper_only.is_empty());
    }

    #[test]
    fn diff_tools_mixed_case_reports_both_sides() {
        let declared = strs(&["Read", "Bash", "Edit"]);
        let wrapper = strs(&["Read", "Grep"]);
        let drift = diff_tools(&declared, &wrapper);
        assert_eq!(drift.matched, strs(&["Read"]));
        assert_eq!(drift.declared_only, strs(&["Bash", "Edit"]));
        assert_eq!(drift.wrapper_only, strs(&["Grep"]));
    }

    #[test]
    fn diff_tools_both_empty_yields_all_empty_fields() {
        let drift = diff_tools(&[], &[]);
        assert!(drift.matched.is_empty());
        assert!(drift.declared_only.is_empty());
        assert!(drift.wrapper_only.is_empty());
    }

    #[test]
    fn diff_tools_dedups_duplicate_entries_and_is_case_sensitive() {
        // "Read" duplicated in `declared`; "read" (lowercase) in `wrapper`
        // is a distinct string — exact match only, no case folding.
        let declared = strs(&["Read", "Read", "Edit"]);
        let wrapper = strs(&["Read", "read"]);
        let drift = diff_tools(&declared, &wrapper);
        assert_eq!(drift.matched, strs(&["Read"]));
        assert_eq!(drift.declared_only, strs(&["Edit"]));
        assert_eq!(drift.wrapper_only, strs(&["read"]));
    }

    // ─── GH #48: classify_wrapper_only (wrapper_only 2-tier split) ─────────

    #[test]
    fn classify_wrapper_only_empty_yields_both_empty() {
        let contract = wrapper_only_contract_set();
        let (contract_out, meaningful_out) = classify_wrapper_only(&[], &contract);
        assert!(contract_out.is_empty());
        assert!(meaningful_out.is_empty());
    }

    #[test]
    fn classify_wrapper_only_only_contract_tools() {
        let contract = wrapper_only_contract_set();
        let wrapper_only = strs(&["mcp__mse__mse_worker_fetch", "mcp__mse__mse_worker_submit"]);
        let (contract_out, meaningful_out) = classify_wrapper_only(&wrapper_only, &contract);
        assert_eq!(
            contract_out,
            strs(&["mcp__mse__mse_worker_fetch", "mcp__mse__mse_worker_submit"])
        );
        assert!(meaningful_out.is_empty());
    }

    #[test]
    fn classify_wrapper_only_only_meaningful_tools() {
        let contract = wrapper_only_contract_set();
        let wrapper_only = strs(&["Bash", "Read"]);
        let (contract_out, meaningful_out) = classify_wrapper_only(&wrapper_only, &contract);
        assert!(contract_out.is_empty());
        assert_eq!(meaningful_out, strs(&["Bash", "Read"]));
    }

    #[test]
    fn classify_wrapper_only_mixed_splits_contract_from_meaningful() {
        let contract = wrapper_only_contract_set();
        let wrapper_only = strs(&[
            "mcp__mse__mse_worker_fetch",
            "Bash",
            "mcp__mse__mse_worker_submit",
            "Grep",
        ]);
        let (contract_out, meaningful_out) = classify_wrapper_only(&wrapper_only, &contract);
        assert_eq!(
            contract_out,
            strs(&["mcp__mse__mse_worker_fetch", "mcp__mse__mse_worker_submit"])
        );
        assert_eq!(meaningful_out, strs(&["Bash", "Grep"]));
    }

    #[test]
    fn classify_wrapper_only_short_names_do_not_match_contract() {
        // Regression guard: wrappers list fetch/submit as full MCP tool
        // identifiers (`mcp__mse__mse_worker_*`). Short forms must not
        // match the allow-list, or the noise-reduction split flips.
        let contract = wrapper_only_contract_set();
        let wrapper_only = strs(&["mse_worker_fetch", "mse_worker_submit"]);
        let (contract_out, meaningful_out) = classify_wrapper_only(&wrapper_only, &contract);
        assert!(contract_out.is_empty());
        assert_eq!(
            meaningful_out,
            strs(&["mse_worker_fetch", "mse_worker_submit"])
        );
    }

    #[test]
    fn classify_wrapper_only_is_case_sensitive_and_dedups_carried_through() {
        // Same case-sensitivity + dedup contract as `diff_tools` — the
        // wrapper_only slice already came out of a `BTreeSet` diff, but
        // this asserts the split preserves that on its own output too.
        let contract = wrapper_only_contract_set();
        let wrapper_only = strs(&["Read", "read", "Read"]);
        let (contract_out, meaningful_out) = classify_wrapper_only(&wrapper_only, &contract);
        assert!(contract_out.is_empty());
        assert_eq!(meaningful_out, strs(&["Read", "read"]));
    }

    // ─── GH #45: bp_doctor tool_lint / output_contract_lint ───────────

    #[test]
    fn build_mcp_tool_registry_contains_the_actual_mcp_tools() {
        // The registry is the ground truth for `classify_tool_lint`;
        // asserting non-emptiness and a couple of load-bearing tool
        // names here catches a schema-generation regression that would
        // otherwise silently flag every real tool call as unknown.
        let reg = build_mcp_tool_registry();
        assert!(
            !reg.is_empty(),
            "registry must be populated from the tool router"
        );
        for name in ["mse_worker_fetch", "mse_worker_submit", "bp_doctor"] {
            assert!(
                reg.contains(name),
                "registry must include {name} (all: {reg:?})"
            );
        }
    }

    fn tool_registry_fixture() -> std::collections::BTreeSet<String> {
        // A hand-rolled subset of the real registry — keeps the pure
        // helper tests independent of the tool router's live output so
        // adding / renaming a tool does not force this file to move.
        ["mse_worker_fetch", "mse_worker_submit", "bp_doctor"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    #[test]
    fn tool_lint_returns_ok_when_every_mcp_ref_is_in_the_registry() {
        let tools = strs(&[
            "Read",
            "Edit",
            "mcp__mse__mse_worker_fetch",
            "mcp__mse__mse_worker_submit",
        ]);
        let lint = classify_tool_lint(&tools, &tool_registry_fixture());
        assert_eq!(lint["severity"], "OK");
        assert_eq!(lint["unknown_tools"], serde_json::json!([]));
    }

    #[test]
    fn tool_lint_flags_a_phantom_mcp_tool_reference_as_warn() {
        let tools = strs(&[
            "Read",
            "mcp__mse__mse_worker_fetch",
            "mcp__mse__mse_ghost_tool",
        ]);
        let lint = classify_tool_lint(&tools, &tool_registry_fixture());
        assert_eq!(lint["severity"], "WARN");
        assert_eq!(
            lint["unknown_tools"],
            serde_json::json!(["mcp__mse__mse_ghost_tool"])
        );
    }

    #[test]
    fn tool_lint_skips_claude_builtins_that_are_not_in_the_registry() {
        // Read / Edit / Grep / Bash never appear in an MCP registry;
        // they must not surface as phantom references. This asserts
        // the heuristic's stated `mcp__mse__`-prefix scope.
        let tools = strs(&["Read", "Edit", "Grep", "Bash", "WebFetch"]);
        let lint = classify_tool_lint(&tools, &tool_registry_fixture());
        assert_eq!(lint["severity"], "OK");
        assert_eq!(lint["unknown_tools"], serde_json::json!([]));
    }

    #[test]
    fn tool_lint_empty_profile_tools_is_ok() {
        let lint = classify_tool_lint(&[], &tool_registry_fixture());
        assert_eq!(lint["severity"], "OK");
        assert_eq!(lint["unknown_tools"], serde_json::json!([]));
    }

    #[test]
    fn output_contract_lint_returns_warn_when_extras_has_no_expected_output() {
        let extras = serde_json::json!({ "other_key": 1 });
        let lint = classify_output_contract_lint(&extras);
        assert_eq!(lint["severity"], "WARN");
        assert_eq!(lint["present"], false);
        assert!(lint["reason"]
            .as_str()
            .unwrap()
            .contains("no expected_output"));
    }

    #[test]
    fn output_contract_lint_returns_warn_when_extras_is_null() {
        // profile-less agents surface as `Value::Null` extras — this
        // asserts the helper handles that path without panicking or
        // treating null as "present".
        let lint = classify_output_contract_lint(&serde_json::Value::Null);
        assert_eq!(lint["severity"], "WARN");
        assert_eq!(lint["present"], false);
    }

    #[test]
    fn output_contract_lint_accepts_every_documented_kind() {
        for kind in ["literal_enum", "inline_markdown", "file_sentinel"] {
            let extras = serde_json::json!({
                "expected_output": {"kind": kind, "pattern": "any"}
            });
            let lint = classify_output_contract_lint(&extras);
            assert_eq!(lint["severity"], "OK", "kind={kind}");
            assert_eq!(lint["present"], true);
            assert_eq!(lint["kind"], kind);
        }
    }

    #[test]
    fn output_contract_lint_flags_unknown_kind_as_warn() {
        let extras = serde_json::json!({
            "expected_output": {"kind": "something_else"}
        });
        let lint = classify_output_contract_lint(&extras);
        assert_eq!(lint["severity"], "WARN");
        assert_eq!(lint["present"], true);
        assert!(lint["reason"]
            .as_str()
            .unwrap()
            .contains("unknown expected_output.kind: something_else"));
    }

    #[test]
    fn output_contract_lint_flags_expected_output_that_is_not_an_object() {
        let extras = serde_json::json!({ "expected_output": "literal_enum" });
        let lint = classify_output_contract_lint(&extras);
        assert_eq!(lint["severity"], "WARN");
        assert_eq!(lint["present"], true);
        assert!(lint["reason"]
            .as_str()
            .unwrap()
            .contains("not a JSON object"));
    }

    #[test]
    fn output_contract_lint_flags_object_missing_kind_as_warn() {
        let extras = serde_json::json!({
            "expected_output": {"pattern": "foo|bar"}
        });
        let lint = classify_output_contract_lint(&extras);
        assert_eq!(lint["severity"], "WARN");
        assert_eq!(lint["present"], true);
        assert!(lint["reason"]
            .as_str()
            .unwrap()
            .contains("missing string field `kind`"));
    }

    // ─── GH #61: bp_doctor worker_binding_lint ────────────────────

    #[test]
    fn worker_binding_lint_returns_ok_when_operator_kind_has_worker_binding() {
        let lint = classify_worker_binding_lint(
            &mlua_swarm::blueprint::AgentKind::Operator,
            Some("claude"),
        );
        assert_eq!(lint["severity"], "OK");
        assert_eq!(lint["kind_requires_binding"], true);
        assert_eq!(lint["present"], true);
    }

    #[test]
    fn worker_binding_lint_flags_operator_kind_missing_worker_binding_as_warn() {
        let lint = classify_worker_binding_lint(&mlua_swarm::blueprint::AgentKind::Operator, None);
        assert_eq!(lint["severity"], "WARN");
        assert_eq!(lint["kind_requires_binding"], true);
        assert_eq!(lint["present"], false);
        // Reason reuses the Compiler's fail-loud message so both fix
        // paths (JSON literal / $agent_md frontmatter) are named.
        let reason = lint["reason"].as_str().expect("reason is a string");
        assert!(reason.contains("profile.worker_binding is required"));
        assert!(reason.contains("agents[N].profile.worker_binding"));
        assert!(reason.contains("$agent_md file ref"));
    }

    #[test]
    fn worker_binding_lint_flags_empty_string_worker_binding_as_warn() {
        // Non-empty string is the contract — an empty literal is
        // treated the same as absent (the Compiler would build a
        // WorkerBinding with an empty variant, which the WS thin-path
        // has no way to resolve).
        let lint =
            classify_worker_binding_lint(&mlua_swarm::blueprint::AgentKind::Operator, Some(""));
        assert_eq!(lint["severity"], "WARN");
        assert_eq!(lint["present"], false);
    }

    #[test]
    fn worker_binding_lint_is_ok_for_non_operator_kinds_regardless_of_binding() {
        // RustFn / Lua / Subprocess don't consume worker_binding — the
        // lint is scoped to operator-tier backends only.
        for kind in [
            mlua_swarm::blueprint::AgentKind::RustFn,
            mlua_swarm::blueprint::AgentKind::Lua,
            mlua_swarm::blueprint::AgentKind::AgentBlock,
            mlua_swarm::blueprint::AgentKind::Subprocess,
        ] {
            let lint_absent = classify_worker_binding_lint(&kind, None);
            assert_eq!(lint_absent["severity"], "OK", "kind={kind:?}");
            assert_eq!(lint_absent["kind_requires_binding"], false, "kind={kind:?}");
            // Non-operator kinds don't grow a `present` field — the
            // check simply doesn't apply.
            assert!(
                lint_absent.get("present").is_none(),
                "kind={kind:?} unexpectedly carries `present`"
            );
        }
    }

    // ─── C4: bp_doctor binding_lint family ─────────────────────

    /// Single-agent Blueprint fixture — the binding_lint family reads only
    /// the agent's Runner wiring and the Blueprint's `strict_binding` flag,
    /// so the base flow/metadata (from `identity_blueprint`) is irrelevant.
    fn binding_lint_bp(agent: AgentDef, strict_binding: bool) -> Blueprint {
        let mut bp = identity_blueprint();
        bp.agents = vec![agent];
        bp.strategy = CompilerStrategy {
            strict_binding,
            ..CompilerStrategy::default()
        };
        bp
    }

    /// Operator agent with an inline `WsOperator` Runner (resolution source
    /// `AgentInline`) — Runner-backed, not legacy.
    fn agent_with_inline_runner() -> AgentDef {
        AgentDef {
            name: "worker".into(),
            kind: AgentKind::Operator,
            spec: serde_json::json!({}),
            profile: Some(AgentProfile {
                model: Some("sonnet".into()),
                ..AgentProfile::default()
            }),
            meta: None,
            runner: Some(Runner::WsOperator {
                variant: "ws-operator".into(),
                tools: vec!["mcp__mse__mse_worker_fetch".into()],
            }),
            runner_ref: None,
            verdict: None,
            lints: None,
        }
    }

    /// Operator agent whose Runner resolves through the deprecated
    /// `profile.worker_binding` fallback (resolution source
    /// `LegacyWorkerBinding`) — still Runner-backed.
    fn agent_with_legacy_worker_binding() -> AgentDef {
        AgentDef {
            name: "legacy".into(),
            kind: AgentKind::Operator,
            spec: serde_json::json!({}),
            profile: Some(AgentProfile {
                worker_binding: Some("claude".into()),
                tools: vec!["Read".into()],
                ..AgentProfile::default()
            }),
            meta: None,
            runner: None,
            runner_ref: None,
            verdict: None,
            lints: None,
        }
    }

    /// RustFn agent with no Runner wiring at all — resolves to no Runner.
    fn agent_no_runner() -> AgentDef {
        AgentDef {
            name: "plain".into(),
            kind: AgentKind::RustFn,
            spec: serde_json::json!({"fn_id": "plain"}),
            profile: None,
            meta: None,
            runner: None,
            runner_ref: None,
            verdict: None,
            lints: None,
        }
    }

    fn binding_findings(bp: &Blueprint) -> Vec<serde_json::Value> {
        classify_binding_lint(bp)["findings"]
            .as_array()
            .cloned()
            .expect("binding_lint always carries a `findings` array")
    }

    fn find_binding_check<'a>(
        findings: &'a [serde_json::Value],
        check: &str,
    ) -> Option<&'a serde_json::Value> {
        findings.iter().find(|f| f["check"] == check)
    }

    #[test]
    fn binding_lint_requirements_info_fires_for_a_runner_backed_agent() {
        let bp = binding_lint_bp(agent_with_inline_runner(), false);
        let findings = binding_findings(&bp);
        let info = find_binding_check(&findings, "binding_requirements_info")
            .expect("a Runner-backed agent must emit binding_requirements_info");
        assert_eq!(info["severity"], "INFO");
        assert_eq!(info["agent"], "worker");
        assert_eq!(info["launch_variant"], "ws-operator");
        assert_eq!(info["model"], "sonnet");
        assert_eq!(
            info["tools"],
            serde_json::json!(["mcp__mse__mse_worker_fetch"])
        );
        // Message names the manifest coverage the finding is about.
        assert!(info["message"]
            .as_str()
            .unwrap()
            .contains("capability_manifest"));
    }

    #[test]
    fn binding_lint_requirements_info_absent_when_no_agent_is_runner_backed() {
        let bp = binding_lint_bp(agent_no_runner(), false);
        let findings = binding_findings(&bp);
        assert!(
            find_binding_check(&findings, "binding_requirements_info").is_none(),
            "a Blueprint with no Runner-backed agent must emit no binding_requirements_info"
        );
    }

    #[test]
    fn binding_lint_strict_without_runners_warns() {
        let bp = binding_lint_bp(agent_no_runner(), true);
        let findings = binding_findings(&bp);
        let warn = find_binding_check(&findings, "strict_binding_without_runners")
            .expect("strict_binding with no Runner-backed agent must warn");
        assert_eq!(warn["severity"], "WARN");
        assert!(warn["message"].as_str().unwrap().contains("no-op"));
    }

    #[test]
    fn binding_lint_strict_with_a_runner_does_not_warn() {
        // strict_binding is meaningful here (there is a Runner to attest),
        // so the no-op warning must not fire.
        let bp = binding_lint_bp(agent_with_inline_runner(), true);
        let findings = binding_findings(&bp);
        assert!(
            find_binding_check(&findings, "strict_binding_without_runners").is_none(),
            "strict_binding with a Runner-backed agent must not warn about a no-op"
        );
    }

    #[test]
    fn binding_lint_legacy_worker_binding_warns() {
        let bp = binding_lint_bp(agent_with_legacy_worker_binding(), false);
        let findings = binding_findings(&bp);
        let warn = find_binding_check(&findings, "legacy_worker_binding")
            .expect("an agent resolved via profile.worker_binding must warn");
        assert_eq!(warn["severity"], "WARN");
        assert_eq!(warn["agent"], "legacy");
        // Message points at the migration target.
        let msg = warn["message"].as_str().unwrap();
        assert!(msg.contains("profile.worker_binding"));
        assert!(msg.contains("runner_ref"));
    }

    #[test]
    fn binding_lint_legacy_worker_binding_absent_for_a_first_class_runner() {
        let bp = binding_lint_bp(agent_with_inline_runner(), false);
        let findings = binding_findings(&bp);
        assert!(
            find_binding_check(&findings, "legacy_worker_binding").is_none(),
            "an inline-Runner agent must not warn about a legacy worker_binding"
        );
    }

    // ─── GH #76 DSL sugar: bp_doctor skip_on_lint family ─────────────

    /// Build a minimal `Blueprint` fixture whose flow is the given
    /// `FlowNode` and whose agents' `verdict.values` reflect the
    /// declared token list per agent. Only fields `classify_skip_on_lint`
    /// reads are populated (flow + agents' verdict); everything else
    /// piggy-backs on `identity_blueprint`.
    fn skip_on_bp(flow: FlowNode, agents_verdicts: &[(&str, &[&str])]) -> Blueprint {
        let mut bp = identity_blueprint();
        bp.flow = flow;
        bp.agents = agents_verdicts
            .iter()
            .map(|(name, values)| AgentDef {
                name: (*name).into(),
                kind: AgentKind::Operator,
                spec: serde_json::json!({}),
                profile: None,
                meta: Some(AgentMeta::default()),
                runner: None,
                runner_ref: None,
                verdict: Some(mlua_swarm_schema::VerdictContract {
                    channel: mlua_swarm_schema::VerdictChannel::Part,
                    values: values.iter().map(|s| (*s).into()).collect(),
                }),
                lints: None,
            })
            .collect();
        bp
    }

    /// Convenience: `path("$.<step>.parts.verdict") in Lit([v1, ...])`
    /// — the shape `bp_dsl` `skip_on` emits (dot / bracket normalized
    /// by the flow-ir Path parser).
    fn skip_branch(upstream_step: &str, values: &[&str], body: FlowNode) -> FlowNode {
        let path_at = format!("$.{upstream_step}.parts.verdict");
        FlowNode::Branch {
            cond: Expr::In {
                needle: Box::new(Expr::Path {
                    at: path_at.parse().expect("literal verdict path"),
                }),
                haystack: Box::new(Expr::Lit {
                    value: serde_json::Value::Array(
                        values
                            .iter()
                            .map(|s| serde_json::Value::String((*s).into()))
                            .collect(),
                    ),
                }),
            },
            then_: Box::new(FlowNode::Seq { children: vec![] }),
            else_: Box::new(body),
        }
    }

    /// Convenience: `eq(path("$.<step>.parts.verdict"), Lit(value))`
    /// — the shape `bp_dsl` `gate_cond` emits for a single halt_on value.
    fn halt_branch(step: &str, value: &str, then_: FlowNode, else_: FlowNode) -> FlowNode {
        let path_at = format!("$.{step}.parts.verdict");
        FlowNode::Branch {
            cond: Expr::Eq {
                lhs: Box::new(Expr::Path {
                    at: path_at.parse().expect("literal verdict path"),
                }),
                rhs: Box::new(Expr::Lit {
                    value: serde_json::Value::String(value.into()),
                }),
            },
            then_: Box::new(then_),
            else_: Box::new(else_),
        }
    }

    fn nop_step() -> FlowNode {
        FlowNode::Seq { children: vec![] }
    }

    fn find_skip_check<'a>(
        findings: &'a [serde_json::Value],
        check: &str,
    ) -> Option<&'a serde_json::Value> {
        findings.iter().find(|f| f["check"] == check)
    }

    /// (a) `skip_on_missing_for_skip_like_verdict_value`: agent
    /// declares a skip-like verdict value but no `skip_on` list in the
    /// flow captures that value → WARN.
    #[test]
    fn skip_on_lints_warn_on_skip_like_verdict_without_skip_on() {
        let bp = skip_on_bp(
            nop_step(),
            &[("triager", &["APPLICABLE", "NOT_APPLICABLE"])],
        );
        let lint = classify_skip_on_lint(&bp);
        let findings = lint["findings"].as_array().expect("findings array");
        let warn = find_skip_check(findings, "skip_on_missing_for_skip_like_verdict_value")
            .expect("a skip-like verdict value without a matching skip_on list must WARN");
        assert_eq!(warn["severity"], "WARN");
        assert_eq!(warn["agent"], "triager");
        assert_eq!(warn["value"], "NOT_APPLICABLE");
        let msg = warn["message"].as_str().expect("message string");
        assert!(msg.contains("NOT_APPLICABLE"));
        assert!(msg.contains("skip_on"));
    }

    /// The check is case-insensitive on the skip-like pattern set.
    /// Non-skip-like values (e.g. `"PASS"`, `"BLOCKED"`) do not
    /// trigger this warning.
    #[test]
    fn skip_on_lint_case_insensitive_and_only_flags_skip_like_values() {
        let bp = skip_on_bp(
            nop_step(),
            &[("mixed", &["Pass", "Blocked", "N/A", "skip"])],
        );
        let lint = classify_skip_on_lint(&bp);
        let findings = lint["findings"].as_array().expect("findings array");
        let missing: Vec<_> = findings
            .iter()
            .filter(|f| f["check"] == "skip_on_missing_for_skip_like_verdict_value")
            .collect();
        // Two skip-like values (`N/A`, `skip`), two non-skip
        // (`Pass`, `Blocked`) — only the two skip-likes WARN.
        assert_eq!(missing.len(), 2, "findings={findings:?}");
        let flagged: std::collections::BTreeSet<&str> =
            missing.iter().filter_map(|f| f["value"].as_str()).collect();
        assert!(flagged.contains("N/A"));
        assert!(flagged.contains("skip"));
    }

    /// (b) `skip_on_declared_but_no_matching_verdict_value`: a
    /// `skip_on` list captures a value no agent declares → WARN.
    #[test]
    fn skip_on_lints_warn_on_dead_skip_on_declaration() {
        // Flow has `skip_on = ["GHOST"]`; the sole agent's verdict.values
        // is `["APPLICABLE"]` — nothing else can produce "GHOST", so
        // the skip guard is dead.
        let flow = skip_branch("triager", &["GHOST"], nop_step());
        let bp = skip_on_bp(flow, &[("triager", &["APPLICABLE"])]);
        let lint = classify_skip_on_lint(&bp);
        let findings = lint["findings"].as_array().expect("findings array");
        let warn = find_skip_check(findings, "skip_on_declared_but_no_matching_verdict_value")
            .expect("a skip_on list capturing an undeclared value must WARN");
        assert_eq!(warn["severity"], "WARN");
        assert_eq!(warn["value"], "GHOST");
        let msg = warn["message"].as_str().expect("message string");
        assert!(msg.contains("GHOST"));
        assert!(msg.contains("dead branch"));
    }

    /// (c) `skip_on_pattern_conflicts_with_halt_on`: the same value
    /// appears in both a skip_on list and a halt_on cond → WARN.
    #[test]
    fn skip_on_lints_warn_on_halt_on_skip_on_overlap() {
        // Flow has:  branch{ eq(triager.verdict, "OVERLAP") ->
        //              branch{ in(triager.verdict, ["OVERLAP"]) -> ...
        //                                                       else ... }
        //            else ... }.
        let inner = skip_branch("triager", &["OVERLAP"], nop_step());
        let flow = halt_branch("triager", "OVERLAP", nop_step(), inner);
        let bp = skip_on_bp(flow, &[("triager", &["OVERLAP"])]);
        let lint = classify_skip_on_lint(&bp);
        let findings = lint["findings"].as_array().expect("findings array");
        let warn = find_skip_check(findings, "skip_on_pattern_conflicts_with_halt_on")
            .expect("a value appearing in both skip_on and halt_on must WARN");
        assert_eq!(warn["severity"], "WARN");
        assert_eq!(warn["value"], "OVERLAP");
        let msg = warn["message"].as_str().expect("message string");
        assert!(msg.contains("OVERLAP"));
        assert!(msg.contains("halt_on"));
    }

    /// A well-formed skip_on Blueprint (skip-like value declared AND
    /// captured by a `skip_on = { ... }` list, no halt_on overlap)
    /// emits no findings.
    #[test]
    fn skip_on_lint_no_findings_on_well_formed_blueprint() {
        let flow = skip_branch("triager", &["NOT_APPLICABLE"], nop_step());
        let bp = skip_on_bp(flow, &[("triager", &["APPLICABLE", "NOT_APPLICABLE"])]);
        let lint = classify_skip_on_lint(&bp);
        let findings = lint["findings"].as_array().expect("findings array");
        assert!(
            findings.is_empty(),
            "well-formed skip_on Blueprint must emit no findings: {findings:?}"
        );
    }

    /// The bundled `samples/bp/skip-on-example.bp.lua` must produce
    /// zero skip_on_lint findings — it is the canonical illustration
    /// of the DSL sugar and must not itself trigger any of the three
    /// checks (would defeat the guide's purpose).
    #[test]
    fn bundled_sample_skip_on_example_has_no_skip_on_lint_findings() {
        let body = include_str!("./mcp/resources/samples/bp/skip-on-example.bp.lua");
        let value = mlua_swarm_cli::dsl::build_bp_from_script(body)
            .expect("bundled sample must build via dsl::build_bp_from_script");
        let bp: Blueprint = serde_json::from_value(value).expect("valid Blueprint");
        let lint = classify_skip_on_lint(&bp);
        let findings = lint["findings"].as_array().expect("findings array");
        assert!(
            findings.is_empty(),
            "canonical sample must emit zero skip_on_lint findings, got: {findings:?}"
        );
    }

    // ─── GH #34: mse_doctor audit_findings surfacing ───────────

    #[test]
    fn extract_audit_findings_returns_empty_for_no_steps() {
        let body = serde_json::json!({
            "task_id": "T-abc",
            "run_id": "R-def",
            "steps": [],
        });
        assert!(extract_audit_findings(&body).is_empty());
    }

    #[test]
    fn extract_audit_findings_ignores_non_audit_step_names() {
        let body = serde_json::json!({
            "task_id": "T-abc",
            "run_id": "R-def",
            "steps": [
                { "name": "worker" },
                { "name": "not-an-audit-artifact" },
            ],
        });
        assert!(extract_audit_findings(&body).is_empty());
    }

    #[test]
    fn extract_audit_findings_flags_audit_prefixed_steps_and_copies_ids() {
        let body = serde_json::json!({
            "task_id": "T-abc",
            "run_id": "R-def",
            "steps": [
                { "name": "worker" },
                { "name": "audit:worker" },
                { "name": "audit:committer" },
            ],
        });
        let findings = extract_audit_findings(&body);
        assert_eq!(findings.len(), 2);
        assert_eq!(findings[0].task_id, "T-abc");
        assert_eq!(findings[0].run_id, "R-def");
        assert_eq!(findings[0].step, "worker");
        assert_eq!(findings[0].artifact_name, "audit:worker");
        assert_eq!(findings[1].step, "committer");
        assert_eq!(findings[1].artifact_name, "audit:committer");
    }

    #[test]
    fn extract_audit_findings_missing_steps_key_returns_empty_not_panic() {
        let body = serde_json::json!({ "task_id": "T-abc", "run_id": "R-def" });
        assert!(extract_audit_findings(&body).is_empty());
    }

    #[test]
    fn extract_audit_findings_skips_step_entries_without_a_name() {
        let body = serde_json::json!({
            "task_id": "T-abc",
            "run_id": "R-def",
            "steps": [ { "size_bytes": 4 } ],
        });
        assert!(extract_audit_findings(&body).is_empty());
    }

    /// `mse serve` unreachable: the audit scan must degrade to an empty
    /// section plus a note, never fail the doctor call (audit-scan
    /// Invariant #1: this scan NEVER fails the doctor call).
    #[tokio::test]
    async fn mse_doctor_server_down_notes_the_audit_scan_skip() {
        let server = MseServer::new();
        {
            let mut inner = server.state.write().await;
            inner.runs.insert(
                "R-unknown".into(),
                RunHandle {
                    run_id: "R-unknown".into(),
                    status: RunStatus::Running,
                    task_id: Some("T-unknown".into()),
                    cancel_requested: false,
                },
            );
        }
        let result = server
            .mse_doctor(Parameters(DoctorReq {
                // Black-hole address (same convention as the worker-fetch
                // tests above): fails fast, never a live server.
                bind: Some("127.0.0.1:1".into()),
            }))
            .await
            .expect("mse_doctor must never fail on an audit-scan issue");
        let json: JsonValue =
            serde_json::from_str(&extract_text_payload(&result)).expect("doctor json");
        assert_eq!(json["audit_findings"]["count"], 0, "body: {json}");
        assert!(
            json["audit_findings"]["findings"]
                .as_array()
                .expect("findings array")
                .is_empty(),
            "body: {json}"
        );
        let notes = json["audit_findings"]["notes"]
            .as_array()
            .expect("notes array");
        // The note now explains what the probe saw rather than asserting
        // the server is down — nothing answered at loopback, which is the
        // one case where offering a local start is useful.
        assert!(
            notes.iter().any(|n| {
                let n = n.as_str().unwrap_or_default();
                n.contains("audit_findings scan skipped")
                    && n.contains("nothing answered at http://127.0.0.1:1/v1/healthz")
                    && n.contains("mlua_swarm_server_start")
            }),
            "notes: {notes:?}"
        );
    }

    /// The MCP handshake must advertise *this* crate, not rmcp.
    ///
    /// `ServerInfo::default()` fills `server_info` from
    /// `Implementation::from_build_env()`, whose `env!` macros expand
    /// inside the rmcp crate — so the default advertises rmcp's own name
    /// and version. Locking this down keeps the protocol-level identity
    /// honest and prevents a silent regression if the override is ever
    /// dropped during an rmcp bump.
    #[test]
    fn get_info_advertises_this_crate_not_rmcp() {
        let info = MseServer::new().get_info();
        assert_eq!(info.server_info.name, "mse-mcp");
        assert_eq!(info.server_info.version, env!("CARGO_PKG_VERSION"));
        assert_ne!(
            info.server_info.name, "rmcp",
            "regressed to the rmcp-supplied build env"
        );
    }

    /// Server unreachable: the drift comparison must report `null`
    /// (could-not-compare), never `false` — a `false` here would read as a
    /// verified "both sides agree" when nothing was actually checked.
    #[tokio::test]
    async fn mse_doctor_version_drift_is_null_when_the_server_is_unreachable() {
        let server = MseServer::new();
        let result = server
            .mse_doctor(Parameters(DoctorReq {
                // Black-hole address: fails fast, never a live server.
                bind: Some("127.0.0.1:1".into()),
            }))
            .await
            .expect("mse_doctor must never fail on an unreachable server");
        let json: JsonValue =
            serde_json::from_str(&extract_text_payload(&result)).expect("doctor json");

        assert_eq!(json["mse_mcp"]["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(
            json["version_drift"]["mse_mcp"],
            env!("CARGO_PKG_VERSION"),
            "body: {json}"
        );
        assert!(
            json["version_drift"]["drift"].is_null(),
            "unchecked comparison must stay null, not collapse to false: {json}"
        );
        assert!(
            json["version_drift"]["mlua_swarm_server"].is_null(),
            "body: {json}"
        );
    }

    /// The three questions the response used to answer in one object, now
    /// answered in three.
    ///
    /// `endpoint` is where we connected and why; `server` is what the thing
    /// at the other end says about itself. They previously shared a `bind`
    /// key holding two different values — ours and the server's — which is
    /// how a reader ends up believing a hosted server is bound to their
    /// loopback.
    #[tokio::test]
    async fn mse_doctor_separates_the_endpoint_from_the_server_and_says_why() {
        use axum::routing::get;
        use axum::{Json, Router};

        let router = Router::new()
            .route("/v1/healthz", get(|| async { "ok" }))
            .route(
                "/v1/doctor",
                get(|| async {
                    Json(serde_json::json!({
                        "server_version": env!("CARGO_PKG_VERSION"),
                        "bind": "0.0.0.0:7777",
                    }))
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        let server = MseServer::new();
        let result = server
            .mse_doctor(Parameters(DoctorReq {
                bind: Some(addr.to_string()),
            }))
            .await
            .expect("doctor");
        let json: JsonValue =
            serde_json::from_str(&extract_text_payload(&result)).expect("doctor json");

        assert_eq!(
            json["endpoint"]["url"],
            format!("http://{addr}"),
            "body: {json}"
        );
        assert_eq!(json["endpoint"]["source"], "argument", "body: {json}");
        let probe = &json["endpoint"]["probe"];
        assert_eq!(probe["http_status"], 200, "body: {json}");
        assert_eq!(probe["host_network"]["reachable"], true, "body: {json}");
        assert_eq!(probe["server_available"]["status"], "pass", "body: {json}");
        assert!(
            probe["server_available"]["note"].is_null(),
            "a passing check explains nothing — the health-check draft omits `output` on pass: {json}"
        );
        assert_eq!(json["server"]["self_report_read"], true, "body: {json}");
        assert_eq!(
            json["server"]["self_report"]["bind"], "0.0.0.0:7777",
            "the server's own bind stays the server's, not ours: {json}"
        );
        assert!(
            json["mlua_swarm_server"].is_null(),
            "the merged section is gone: {json}"
        );
        // Loopback stub, so a local daemon could plausibly be serving it.
        assert_eq!(json["supervision"]["applicable"], true, "body: {json}");
    }

    /// launchd supervises daemons on this machine. Against a hosted
    /// endpoint the section is a category error, and six nulls read as "the
    /// daemon is unwell" rather than "there is no daemon here".
    #[tokio::test]
    async fn mse_doctor_marks_supervision_inapplicable_for_a_remote_endpoint() {
        let server = MseServer::new();
        let result = server
            .mse_doctor(Parameters(DoctorReq {
                // Nothing listens; only the endpoint's shape is under test.
                bind: Some("https://example.invalid".into()),
            }))
            .await
            .expect("doctor must not fail when the endpoint is unreachable");
        let json: JsonValue =
            serde_json::from_str(&extract_text_payload(&result)).expect("doctor json");

        assert_eq!(json["supervision"]["applicable"], false, "body: {json}");
        assert!(
            json["supervision"]["launchd_state"].is_null()
                && json["supervision"]["plist_working_directory"].is_null(),
            "inapplicable means absent, not null-valued: {json}"
        );
        assert!(
            json["supervision"]["reason"]
                .as_str()
                .unwrap_or_default()
                .contains("not this machine"),
            "body: {json}"
        );
        assert_eq!(json["server"]["self_report_read"], false, "body: {json}");
        assert_eq!(json["endpoint"]["source"], "argument", "body: {json}");
        assert_eq!(
            json["endpoint"]["probe"]["host_network"]["reachable"], false,
            "nothing answered, so the host was not reached either: {json}"
        );
    }

    /// `self_report` is what the far end says about itself. When it could
    /// not be read there is nothing it said, and putting our own diagnosis
    /// there makes the section answer a question it was not asked — the
    /// same confusion, one field over, that this whole restructure was
    /// about. The reason belongs in `endpoint.probe.server_available.note`,
    /// where it already is.
    #[tokio::test]
    async fn mse_doctor_omits_the_self_report_when_it_could_not_be_read() {
        let server = MseServer::new();
        let result = server
            .mse_doctor(Parameters(DoctorReq {
                bind: Some("https://example.invalid".into()),
            }))
            .await
            .expect("doctor");
        let json: JsonValue =
            serde_json::from_str(&extract_text_payload(&result)).expect("doctor json");

        assert_eq!(json["server"]["self_report_read"], false, "body: {json}");
        assert!(
            json["server"]["self_report"].is_null(),
            "nothing was read, so there is nothing the server said: {json}"
        );
        assert!(
            json["endpoint"]["probe"]["server_available"]["note"]
                .as_str()
                .unwrap_or_default()
                .contains("nothing answered"),
            "the reason lives in the probe, and only there: {json}"
        );
    }

    // ─── swarm_run final_ctx delivery ──────────────────────────────────
    //
    // A run's ctx is its result, so no amount of trimming is the right
    // answer — three attempts at one (per key, then per depth, then per
    // measured threshold) each lost something a caller reads. The MCP
    // transport is stdio, so the tool runs on the caller's own machine:
    // the ctx goes to a file the caller can open, and the response says
    // where. The inline copy is a convenience for small runs, and the
    // threshold that decides it is no longer load-bearing — everything is
    // in the file either way, so getting it wrong costs one Read.

    fn sample_ctx(size: usize) -> JsonValue {
        serde_json::json!({
            "aggregate": {"out": "# pre-commit-gates aggregate: BLOCKED"},
            "evidence": "x".repeat(size),
        })
    }

    #[test]
    fn run_ctx_is_written_beside_the_run_and_its_path_returned() {
        let root = tempfile::tempdir().expect("tempdir");
        let ctx = sample_ctx(64);
        let report = run_ctx_report(root.path(), ctx.clone(), "R-abc");

        let path = report["ctx_file"]["path"]
            .as_str()
            .expect("the response names the file");
        assert!(
            path.contains("R-abc"),
            "the file sits under the run it belongs to: {path}"
        );

        let written: JsonValue = serde_json::from_str(
            &std::fs::read_to_string(path).expect("the file the response names exists"),
        )
        .expect("and holds JSON");
        assert_eq!(written, ctx, "the file holds the whole ctx, untrimmed");
        assert_eq!(
            report["ctx_file"]["bytes"].as_u64().unwrap_or(0) as usize,
            serde_json::to_string(&ctx).expect("serialize").len()
        );
    }

    #[test]
    fn a_small_run_ctx_is_inlined_as_well() {
        let root = tempfile::tempdir().expect("tempdir");
        let ctx = sample_ctx(64);
        let report = run_ctx_report(root.path(), ctx.clone(), "R-small");

        assert_eq!(
            report["final_ctx"], ctx,
            "a small ctx costs the caller nothing to inline"
        );
    }

    #[test]
    fn a_large_run_ctx_is_not_inlined_and_the_response_says_where_it_is() {
        let root = tempfile::tempdir().expect("tempdir");
        let ctx = sample_ctx(RUN_CTX_INLINE_BYTES * 2);
        let report = run_ctx_report(root.path(), ctx.clone(), "R-large");

        assert!(
            report["final_ctx"].is_null(),
            "null, not a trimmed object dressed up as the ctx: {}",
            report["final_ctx"]
        );
        let note = report["ctx_file"]["note"]
            .as_str()
            .expect("a ctx left out of the response explains itself");
        assert!(
            note.contains("not inlined") && note.contains("read the file"),
            "note: {note}"
        );

        let path = report["ctx_file"]["path"].as_str().expect("path");
        let written: JsonValue =
            serde_json::from_str(&std::fs::read_to_string(path).expect("file exists"))
                .expect("JSON");
        assert_eq!(
            written, ctx,
            "the file is whole even though the response is not"
        );
    }

    /// A ctx that cannot be written is a fact the caller has to be told:
    /// the alternative is a response that looks complete while the data it
    /// points at does not exist.
    #[test]
    fn a_ctx_that_could_not_be_written_is_reported_not_swallowed() {
        // A file where the run directory needs to be: the write fails, and
        // the run still has to report its outcome.
        let root = tempfile::tempdir().expect("tempdir");
        let blocker = root.path().join("runs");
        std::fs::write(&blocker, b"not a directory").expect("place the blocker");

        let ctx = sample_ctx(RUN_CTX_INLINE_BYTES * 2);
        let report = run_ctx_report(root.path(), ctx, "R-blocked");

        assert!(
            report["ctx_file"]["error"].is_string(),
            "the failure is named: {}",
            report["ctx_file"]
        );
        assert!(
            report["ctx_file"]["path"].is_null(),
            "and no path is offered that does not exist: {}",
            report["ctx_file"]
        );
    }

    // ─── run ctx reader ────────────────────────────────────────────────
    //
    // Handing back a path is only half an answer: a caller whose tool
    // scope has no filesystem read cannot open it. The reader closes that
    // inside the same MCP — and selects by path rather than paging by
    // bytes, so the response is small because the caller asked for a
    // branch, not because a threshold cut one. Three attempts at guessing
    // that threshold are the reason this is not a pager.

    fn write_sample_ctx(root: &std::path::Path, run_id: &str, ctx: &JsonValue) {
        let dir = root.join("runs").join(run_id);
        std::fs::create_dir_all(&dir).expect("mkdir");
        std::fs::write(
            dir.join("ctx.json"),
            serde_json::to_string_pretty(ctx).expect("serialize"),
        )
        .expect("write");
    }

    fn gate_shaped_ctx() -> JsonValue {
        serde_json::json!({
            "aggregate": {
                "out": "# pre-commit-gates aggregate: BLOCKED",
                "parts": {"evidence": vec!["x".repeat(64); 400]},
            },
            "project_root": "/home/u/p",
        })
    }

    #[test]
    fn run_ctx_read_returns_only_the_branch_that_was_asked_for() {
        let root = tempfile::tempdir().expect("tempdir");
        write_sample_ctx(root.path(), "R-sel", &gate_shaped_ctx());

        let got = run_ctx_read(root.path(), "R-sel", Some("$.aggregate.out"));

        assert_eq!(got["at"], "$.aggregate.out");
        assert_eq!(got["value"], "# pre-commit-gates aggregate: BLOCKED");
        assert!(got["truncated"].is_null(), "a small branch is whole: {got}");
    }

    #[test]
    fn run_ctx_read_without_a_path_lists_the_keys_and_their_sizes() {
        let root = tempfile::tempdir().expect("tempdir");
        write_sample_ctx(root.path(), "R-list", &gate_shaped_ctx());

        let got = run_ctx_read(root.path(), "R-list", None);

        let keys = got["keys"].as_object().expect("keys map");
        assert!(
            keys.contains_key("$.aggregate") && keys.contains_key("$.project_root"),
            "the top level is enumerated so a caller knows what to ask for: {got}"
        );
        assert!(
            keys["$.aggregate"].as_u64().unwrap_or(0)
                > keys["$.project_root"].as_u64().unwrap_or(0),
            "with sizes, so the caller can tell which branch is the large one: {got}"
        );
        assert!(
            got["value"].is_null(),
            "listing does not also dump the ctx: {got}"
        );
    }

    #[test]
    fn run_ctx_read_descends_into_a_named_child() {
        let root = tempfile::tempdir().expect("tempdir");
        write_sample_ctx(root.path(), "R-deep", &gate_shaped_ctx());

        let got = run_ctx_read(root.path(), "R-deep", Some("$.aggregate"));
        let keys = got["keys"].as_object();
        assert!(
            got["value"]["out"].is_string() || keys.is_some(),
            "asking for a subtree returns it (or enumerates it when large): {got}"
        );
    }

    /// The one place a threshold survives — and it names the way out
    /// instead of quietly cutting, because the caller can always narrow
    /// `at` and get the whole thing.
    #[test]
    fn run_ctx_read_caps_a_large_branch_and_says_how_to_narrow_it() {
        let root = tempfile::tempdir().expect("tempdir");
        let big = serde_json::json!({"blob": "y".repeat(RUN_CTX_INLINE_BYTES * 2)});
        write_sample_ctx(root.path(), "R-big", &big);

        let got = run_ctx_read(root.path(), "R-big", Some("$.blob"));

        assert!(
            got["truncated"]["bytes_total"].as_u64().unwrap_or(0) > 0,
            "body: {got}"
        );
        let note = got["truncated"]["note"].as_str().unwrap_or_default();
        assert!(
            note.contains("narrow") && note.contains("at"),
            "the note points at the lever the caller has: {note}"
        );
    }

    #[test]
    fn run_ctx_read_reports_a_missing_run_and_a_missing_path_differently() {
        let root = tempfile::tempdir().expect("tempdir");
        write_sample_ctx(root.path(), "R-here", &gate_shaped_ctx());

        let no_run = run_ctx_read(root.path(), "R-nope", None);
        assert!(
            no_run["error"]
                .as_str()
                .unwrap_or_default()
                .contains("no ctx recorded"),
            "a run with no ctx says so: {no_run}"
        );

        let no_path = run_ctx_read(root.path(), "R-here", Some("$.missing.key"));
        assert!(
            no_path["error"]
                .as_str()
                .unwrap_or_default()
                .contains("$.missing.key"),
            "a path that is not there names itself: {no_path}"
        );
    }

    // ─── mse_http response shape ───────────────────────────────────────
    //
    // Built as a pure function so it can be tested without moving
    // `MSE_HTTP` process-wide, which is what kept the earlier version of
    // this tool covered only by its parts.

    fn sample_endpoint() -> crate::http::Endpoint {
        crate::http::Endpoint::resolve(Some("https://example.test"))
    }

    /// The host reaches the caller either way — `mse_doctor` hands out
    /// `endpoint.url` for the asking — so withholding it here would cost
    /// the ability to attribute a failure while buying no secrecy. What it
    /// must not do is call two different things `url`: this tool meant the
    /// full request URL while its siblings mean the base.
    #[test]
    fn http_report_separates_the_endpoint_from_the_request() {
        let report = http_report(
            &sample_endpoint(),
            "GET",
            "/v1/doctor",
            "https://example.test/v1/doctor",
            200,
            "{\"ok\":true}".into(),
        );

        assert_eq!(report["endpoint"]["url"], "https://example.test");
        assert_eq!(report["endpoint"]["source"], "argument");
        assert_eq!(report["request"]["method"], "GET");
        assert_eq!(report["request"]["path"], "/v1/doctor");
        assert_eq!(
            report["request"]["url"], "https://example.test/v1/doctor",
            "the full URL stays, under a name that says which one it is"
        );
        assert_eq!(report["status"], 200);
        assert_eq!(report["body"]["ok"], true, "JSON comes back parsed");
        assert!(
            report["truncated"].is_null(),
            "a body under the cap is whole, and says nothing about truncation"
        );
    }

    #[test]
    fn http_report_keeps_a_non_json_body_as_text() {
        let report = http_report(
            &sample_endpoint(),
            "GET",
            "/v1/healthz",
            "https://example.test/v1/healthz",
            200,
            "ok".into(),
        );
        assert_eq!(report["body"], "ok");
    }

    /// This tool can reach any `/v1/**` route, including ones that answer
    /// with megabytes. An unbounded body is how a diagnostic call takes a
    /// caller's context down with it — so it is capped, and the cap is
    /// declared rather than applied quietly.
    #[test]
    fn http_report_caps_a_large_body_and_says_that_it_did() {
        let huge = "x".repeat(HTTP_BODY_CAP_BYTES * 2);
        let total = huge.len();
        let report = http_report(
            &sample_endpoint(),
            "GET",
            "/v1/runs/R-1/trace",
            "https://example.test/v1/runs/R-1/trace",
            200,
            huge,
        );

        let returned = report["body"].as_str().expect("a capped body is text");
        assert_eq!(returned.len(), HTTP_BODY_CAP_BYTES);
        assert_eq!(report["truncated"]["bytes_total"], total);
        assert_eq!(report["truncated"]["bytes_returned"], HTTP_BODY_CAP_BYTES);
        assert!(
            report["truncated"]["note"]
                .as_str()
                .unwrap_or_default()
                .contains("not the whole body"),
            "a truncated body must never read as a complete one: {report}"
        );
    }

    /// Cutting bytes out of the middle of a character would panic on the
    /// way to a `String`, which is a poor outcome for a tool whose job is
    /// to survive whatever a route answers with.
    #[test]
    fn http_report_truncates_multibyte_text_without_splitting_a_character() {
        let huge = "あ".repeat(HTTP_BODY_CAP_BYTES); // 3 bytes each
        let report = http_report(
            &sample_endpoint(),
            "GET",
            "/v1/doctor",
            "https://example.test/v1/doctor",
            200,
            huge,
        );
        let returned = report["body"].as_str().expect("a capped body is text");
        assert!(returned.len() <= HTTP_BODY_CAP_BYTES);
        assert!(
            returned.chars().all(|c| c == 'あ'),
            "no partial character survived the cut"
        );
    }

    /// Every `bind` argument must say what it now accepts.
    ///
    /// They all read "mse serve bind address (default 127.0.0.1:7777)",
    /// which described a `host:port` with the scheme hard-coded — the
    /// shape that could not name an https server at all. The argument was
    /// widened to take a whole base URL and to fall through to `MSE_HTTP`,
    /// and a caller who reads the old sentence has no way to know either.
    ///
    /// Checked here rather than trusted to review, because there are seven
    /// of them and they drifted as one.
    #[test]
    fn every_bind_argument_describes_the_forms_it_accepts() {
        /// Every `bind` property anywhere in a schema, not just at the top
        /// level: `swarm_run` carries one nested inside its Blueprint
        /// selector, and a check that only looked at the root would have
        /// left it stale while reporting success.
        fn bind_properties(schema: &JsonValue, found: &mut Vec<JsonValue>) {
            match schema {
                JsonValue::Object(map) => {
                    if let Some(JsonValue::Object(props)) = map.get("properties") {
                        if let Some(bind) = props.get("bind") {
                            found.push(bind.clone());
                        }
                    }
                    for value in map.values() {
                        bind_properties(value, found);
                    }
                }
                JsonValue::Array(items) => {
                    for value in items {
                        bind_properties(value, found);
                    }
                }
                _ => {}
            }
        }

        let tools = MseServer::tool_router().list_all();
        let mut checked = 0;

        for tool in &tools {
            let schema = JsonValue::Object((*tool.input_schema).clone());
            let mut binds = Vec::new();
            bind_properties(&schema, &mut binds);

            for bind in binds {
                let description = bind
                    .get("description")
                    .and_then(|d| d.as_str())
                    .unwrap_or_default();
                assert!(
                    !description.is_empty(),
                    "{}'s `bind` has no description — a caller cannot guess whether it takes a \
                 host:port or a base URL",
                    tool.name
                );
                // A `bind` the handler ignores has no forms to describe; it
                // only has to say that it is ignored, which is the more
                // important thing for a caller passing one.
                if description.contains("currently ignored") {
                    checked += 1;
                    continue;
                }
                assert!(
                    description.contains("base URL"),
                    "{}'s `bind` must say it accepts a base URL (scheme included), or an https \
                 endpoint stays unreachable with no hint why: {description}",
                    tool.name
                );
                assert!(
                    description.contains("MSE_HTTP"),
                    "{}'s `bind` must name the fallback it now has, since omitting the argument \
                 no longer means loopback: {description}",
                    tool.name
                );
                checked += 1;
            }
        }

        assert!(
            checked >= 12,
            "expected every bind argument to be covered, only found {checked}"
        );
    }

    /// A tool description is what a caller plans against, so a field name
    /// in one that the response does not have is a defect in the tool.
    ///
    /// This is mechanically checkable and was not being checked: the
    /// description said `probe.status` while the response carried
    /// `probe.http_status`, introduced by the very commit that fixed the
    /// previous description drift. Every dotted `a.b` path in a
    /// description must resolve against a real response.
    #[tokio::test]
    async fn tool_descriptions_do_not_name_response_fields_that_do_not_exist() {
        /// Does some object in `json` have key `parent` whose value is an
        /// object carrying `child`? Subtree match, because descriptions
        /// name paths relative to their section, not to the root.
        fn has_nested_pair(json: &JsonValue, parent: &str, child: &str) -> bool {
            match json {
                JsonValue::Object(map) => {
                    if let Some(JsonValue::Object(inner)) = map.get(parent) {
                        if inner.contains_key(child) {
                            return true;
                        }
                    }
                    map.values().any(|v| has_nested_pair(v, parent, child))
                }
                JsonValue::Array(items) => items.iter().any(|v| has_nested_pair(v, parent, child)),
                _ => false,
            }
        }

        /// Backticked `a.b` tokens that look like response field paths.
        /// URIs, routes and anything capitalized are not field paths.
        fn field_paths(description: &str) -> Vec<(String, String)> {
            description
                .split('`')
                .skip(1)
                .step_by(2)
                .filter(|token| {
                    !token.contains('/')
                        && !token.contains(' ')
                        && !token.contains(':')
                        && token.contains('.')
                        && token
                            .chars()
                            .all(|c| c.is_ascii_lowercase() || c == '_' || c == '.')
                })
                .filter_map(|token| {
                    let mut parts = token.split('.');
                    match (parts.next(), parts.next(), parts.next()) {
                        (Some(parent), Some(child), None) => {
                            Some((parent.to_string(), child.to_string()))
                        }
                        _ => None,
                    }
                })
                .collect()
        }

        let server = MseServer::new();
        // An unreachable endpoint still produces every section, which is
        // the shape the descriptions describe.
        let doctor: JsonValue = serde_json::from_str(&extract_text_payload(
            &server
                .mse_doctor(Parameters(DoctorReq {
                    bind: Some("https://example.invalid".into()),
                }))
                .await
                .expect("doctor"),
        ))
        .expect("doctor json");
        let status: JsonValue = serde_json::from_str(&extract_text_payload(
            &server
                .mlua_swarm_server_status(Parameters(ServerStatusReq {
                    bind: Some("https://example.invalid".into()),
                }))
                .await
                .expect("status"),
        ))
        .expect("status json");

        let tools = MseServer::tool_router().list_all();
        for (name, response) in [
            ("mse_doctor", &doctor),
            ("mlua_swarm_server_status", &status),
        ] {
            let tool = tools
                .iter()
                .find(|t| t.name == name)
                .unwrap_or_else(|| panic!("{name} must be a registered tool"));
            let description = tool.description.as_deref().unwrap_or_default();
            for (parent, child) in field_paths(description) {
                assert!(
                    has_nested_pair(response, &parent, &child),
                    "{name}'s description names `{parent}.{child}`, which the response does not \
                     carry — the description is the contract, so fix whichever is wrong: {response}"
                );
            }
        }
    }

    /// `mlua_swarm_server_status` answers the same two questions
    /// `mse_doctor` does — where did we connect, and does a local daemon
    /// supervise it — so it reports them the same way.
    ///
    /// It used to serialize the raw `StatusOutcome`: a flat `up` carrying
    /// several questions at once, and five launchd nulls printed for a
    /// hosted endpoint launchd has never heard of. Its description also
    /// listed a field set that no longer matched what it returned.
    #[tokio::test]
    async fn server_status_reports_the_endpoint_and_supervision_like_the_doctor_does() {
        use axum::routing::get;
        use axum::Router;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(
                listener,
                Router::new().route("/v1/healthz", get(|| async { "ok" })),
            )
            .await;
        });

        let server = MseServer::new();
        let result = server
            .mlua_swarm_server_status(Parameters(ServerStatusReq {
                bind: Some(addr.to_string()),
            }))
            .await
            .expect("status");
        let json: JsonValue =
            serde_json::from_str(&extract_text_payload(&result)).expect("status json");

        assert_eq!(json["endpoint"]["url"], format!("http://{addr}"));
        assert_eq!(
            json["endpoint"]["probe"]["server_available"]["status"], "pass",
            "body: {json}"
        );
        assert_eq!(
            json["endpoint"]["probe"]["host_network"]["reachable"], true,
            "body: {json}"
        );
        assert_eq!(json["supervision"]["applicable"], true, "body: {json}");
        assert!(
            json["up"].is_null(),
            "the overloaded bool is gone — `server_available.status` says it precisely: {json}"
        );
    }

    #[tokio::test]
    async fn server_status_does_not_print_launchd_nulls_for_a_remote_endpoint() {
        let server = MseServer::new();
        let result = server
            .mlua_swarm_server_status(Parameters(ServerStatusReq {
                bind: Some("https://example.invalid".into()),
            }))
            .await
            .expect("status");
        let json: JsonValue =
            serde_json::from_str(&extract_text_payload(&result)).expect("status json");

        assert_eq!(json["supervision"]["applicable"], false, "body: {json}");
        assert!(
            json["supervision"]["launchd_state"].is_null()
                && json["supervision"]["reason"].is_string(),
            "inapplicable carries a reason, not five nulls: {json}"
        );
    }

    /// The split that `reachable` alone could not express: a redirect means
    /// the host answered and the server is not serving. Reported as one
    /// bool, whichever value it took contradicted the note beside it.
    #[tokio::test]
    async fn mse_doctor_reports_a_redirect_as_host_reached_but_server_unavailable() {
        use axum::http::{header, StatusCode};
        use axum::routing::get;
        use axum::Router;

        let router = Router::new().route(
            "/v1/healthz",
            get(|| async {
                (
                    StatusCode::MOVED_PERMANENTLY,
                    [(header::LOCATION, "https://example.com/v1/healthz")],
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        let server = MseServer::new();
        let result = server
            .mse_doctor(Parameters(DoctorReq {
                bind: Some(addr.to_string()),
            }))
            .await
            .expect("doctor");
        let json: JsonValue =
            serde_json::from_str(&extract_text_payload(&result)).expect("doctor json");

        let probe = &json["endpoint"]["probe"];
        assert_eq!(probe["http_status"], 301, "body: {json}");
        assert_eq!(
            probe["host_network"]["reachable"], true,
            "a 301 is an answer — the host was reached: {json}"
        );
        assert_eq!(
            probe["server_available"]["status"], "fail",
            "answering 301 is not serving: {json}"
        );
        let note = probe["server_available"]["note"]
            .as_str()
            .expect("a failing check carries its output");
        assert!(
            note.contains("301") && note.contains("scheme"),
            "note: {note}"
        );
        assert_eq!(json["server"]["self_report_read"], false, "body: {json}");
    }

    /// Both determinate arms of the tri-state, against a stub `/v1/doctor`
    /// (same stub-router pattern as
    /// `mse_doctor_surfaces_audit_findings_via_stub_steps_api`): a server
    /// built from a different `mse` vintage flags `drift: true`, a
    /// matching one flags `drift: false`. This is the
    /// `cargo install`-without-restart case the section exists for.
    #[tokio::test]
    async fn mse_doctor_version_drift_compares_against_the_server_reported_version() {
        use axum::routing::get;
        use axum::{Json, Router};

        async fn spawn_stub(server_version: String) -> String {
            let router = Router::new()
                .route("/v1/healthz", get(|| async { "ok" }))
                .route(
                    "/v1/doctor",
                    get(move || {
                        let v = server_version.clone();
                        async move { Json(serde_json::json!({ "server_version": v })) }
                    }),
                );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind ephemeral port");
            let addr = listener.local_addr().expect("local addr");
            tokio::spawn(async move {
                let _ = axum::serve(listener, router).await;
            });
            addr.to_string()
        }

        async fn drift_for(bind: String) -> JsonValue {
            let result = MseServer::new()
                .mse_doctor(Parameters(DoctorReq { bind: Some(bind) }))
                .await
                .expect("mse_doctor");
            serde_json::from_str::<JsonValue>(&extract_text_payload(&result)).expect("doctor json")
                ["version_drift"]
                .clone()
        }

        // A stale `mse serve` still running a pre-install build.
        let stale = drift_for(spawn_stub("0.0.0-stale".to_string()).await).await;
        assert_eq!(stale["drift"], JsonValue::Bool(true), "body: {stale}");
        assert_eq!(stale["mlua_swarm_server"], "0.0.0-stale", "body: {stale}");
        assert_eq!(stale["mse_mcp"], env!("CARGO_PKG_VERSION"), "body: {stale}");

        // Both sides restarted onto the same build.
        let matched = drift_for(spawn_stub(env!("CARGO_PKG_VERSION").to_string()).await).await;
        assert_eq!(matched["drift"], JsonValue::Bool(false), "body: {matched}");
    }

    /// GH #34: end-to-end coverage that dispatches a real Blueprint with
    /// `audits` declared through a real in-process `mse serve` router
    /// (same setup pattern as `mse_worker_fetch_and_submit_hit_the_http_endpoints`)
    /// and inspects the real `GET /v1/tasks/:id/runs/:run/steps` response.
    ///
    /// **Historical gap**: `Engine::submit_output` (`src/core/engine.rs`)
    /// only dual-wrote to the Data-plane `OutputStore` the HTTP steps API
    /// reads from for `OutputEvent::Final` events.
    /// `AfterRunAuditMiddleware` submits `OutputEvent::Artifact` — a
    /// different variant — so the audit finding never reached the
    /// Data-plane store and never appeared in the steps listing, even
    /// though it WAS recorded in the domain-plane (`Engine::output_tail`).
    ///
    /// **Current shape**: two changes were needed, not one.
    ///
    /// 1. `Engine::submit_output` (`src/core/engine.rs`) dual-writes
    ///    `Artifact` events too (general form — every `Artifact`, no
    ///    name-prefix gate), keyed under the artifact's own `name`
    ///    verbatim, into the SAME `(task_id, attempt)` coordinates as the
    ///    AUDITED step's own `Final` (`AfterRunAuditMiddleware` submits
    ///    its `"audit:<step_ref>"` finding against the audited task's own
    ///    id — see `src/middleware.rs`'s `run_one_audit` — not a separate
    ///    id for the auditor agent).
    /// 2. THIS turned out to be necessary but not sufficient:
    ///    `McpQueryAdapter::enumerate_steps_via_table`
    ///    (`crates/mlua-swarm-server/src/projection.rs`) only ever looked
    ///    up ONE name per `RunRecord.step_entries` row — the row's own
    ///    canonical producer name (`"echo"`) — so a differently-named
    ///    `Artifact` dual-written under the SAME `StepId` was invisible to
    ///    it even after change (1) landed (confirmed empirically: this
    ///    test still failed with `step_names == ["echo"]` before change
    ///    (2)). `enumerate_steps_via_table` now ALSO lists every
    ///    `OutputEvent::Artifact` under a row's `StepId`
    ///    (`OutputStore::list_for_attempt`) and surfaces each under its
    ///    own name — additive, never overrides the canonical-name lookup.
    #[tokio::test]
    async fn steps_api_exposes_both_the_audited_steps_own_output_and_the_audit_artifact() {
        use mlua_swarm::{RustFnInProcessSpawnerFactory, SpawnerRegistry, WorkerResult};
        use std::sync::Arc;

        let factory = RustFnInProcessSpawnerFactory::new()
            .register_fn("echo", |inv| async move {
                Ok(WorkerResult {
                    value: serde_json::json!({ "echoed": inv.prompt }),
                    ok: true,
                    stats: None,
                })
            })
            .register_fn("audit-fn", |_inv| async move {
                Ok(WorkerResult {
                    value: serde_json::json!({ "finding": "clean" }),
                    ok: true,
                    stats: None,
                })
            });
        let mut registry = SpawnerRegistry::new();
        registry.register::<RustFnInProcessSpawnerFactory>(Arc::new(factory));

        let engine = Engine::new(EngineCfg::default());
        let router = mlua_swarm_server::build_router_with(engine, registry, None);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let bind = addr.to_string();

        let bp = Blueprint {
            schema_version: current_schema_version(),
            id: "mse mcp-audit-findings-fixture".into(),
            flow: FlowNode::Step {
                ref_: "echo".into(),
                in_: Expr::Path {
                    at: "$.input".parse().expect("literal test path: $.input"),
                },
                out: Expr::Path {
                    at: "$.out".parse().expect("literal test path: $.out"),
                },
            },
            agents: vec![
                AgentDef {
                    name: "echo".into(),
                    kind: AgentKind::RustFn,
                    spec: serde_json::json!({"fn_id": "echo"}),
                    profile: None,
                    meta: Some(AgentMeta::default()),
                    runner: None,
                    runner_ref: None,
                    verdict: None,
                    lints: None,
                },
                AgentDef {
                    name: "auditor".into(),
                    kind: AgentKind::RustFn,
                    spec: serde_json::json!({"fn_id": "audit-fn"}),
                    profile: None,
                    meta: Some(AgentMeta::default()),
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
            audits: vec![AuditDef {
                agent: "auditor".into(),
                steps: None,
                mode: AuditMode::Sync,
            }],
            degradation_policy: None,
            runners: vec![],
            default_runner: None,
            subprocesses: vec![],
            check_policy: None,
            blueprint_ref_includes: Vec::new(),
        };

        let client = crate::http::client_builder()
            .build()
            .expect("http client build");
        let launch_resp = client
            .post(format!("http://{bind}/v1/tasks"))
            .json(&serde_json::json!({
                "blueprint": { "kind": "inline", "value": bp },
                "init_ctx": { "input": "hi" },
            }))
            .send()
            .await
            .expect("POST /v1/tasks");
        assert!(
            launch_resp.status().is_success(),
            "launch status: {}",
            launch_resp.status()
        );
        let launch_body: JsonValue = launch_resp.json().await.expect("launch response json");
        let task_id = launch_body["task_id"]
            .as_str()
            .expect("task_id in response")
            .to_string();
        let run_id = launch_body["run_id"]
            .as_str()
            .expect("run_id in response")
            .to_string();

        let steps_resp = client
            .get(format!(
                "http://{bind}/v1/tasks/{task_id}/runs/{run_id}/steps"
            ))
            .send()
            .await
            .expect("GET steps");
        assert!(steps_resp.status().is_success());
        let steps_body: JsonValue = steps_resp.json().await.expect("steps response json");
        let step_names: Vec<String> = steps_body["steps"]
            .as_array()
            .expect("steps array")
            .iter()
            .filter_map(|s| s["name"].as_str().map(String::from))
            .collect();
        assert!(
            step_names.contains(&"echo".to_string()),
            "steps API must expose the echo step's own output: {step_names:?}"
        );
        assert!(
            step_names.contains(&"audit:echo".to_string()),
            "steps API must expose the audit finding once submit_output's \
             Artifact dual-write lands: {step_names:?}"
        );
    }

    /// `mse_doctor`'s own HTTP-calling + extraction logic, isolated from the
    /// historical core-crate gap where `OutputEvent::Artifact` did not
    /// dual-write to the Data-plane `OutputStore`: a stub router serving
    /// the real `GET /v1/tasks/:id/runs/:run/steps` response *shape* (not
    /// a real dispatch) proves the doctor tool round-trips correctly once
    /// the steps API genuinely returns an `audit:`-prefixed entry — i.e.
    /// the doctor code works correctly against the documented contract,
    /// decoupled from whether core currently honors that contract for
    /// `OutputEvent::Artifact`.
    #[tokio::test]
    async fn mse_doctor_surfaces_audit_findings_via_stub_steps_api() {
        use axum::extract::Path as AxumPath;
        use axum::routing::get;
        use axum::{Json, Router};

        async fn stub_healthz() -> &'static str {
            "ok"
        }
        async fn stub_steps(
            AxumPath((task_id, run_id)): AxumPath<(String, String)>,
        ) -> Json<JsonValue> {
            Json(serde_json::json!({
                "task_id": task_id,
                "run_id": run_id,
                "steps": [
                    { "name": "worker" },
                    { "name": "audit:worker" },
                ],
            }))
        }

        let router = Router::new()
            .route("/v1/healthz", get(stub_healthz))
            .route("/v1/tasks/:task_id/runs/:run_id/steps", get(stub_steps));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let bind = addr.to_string();

        let server = MseServer::new();
        {
            let mut inner = server.state.write().await;
            inner.runs.insert(
                "R-stub".into(),
                RunHandle {
                    run_id: "R-stub".into(),
                    status: RunStatus::Done,
                    task_id: Some("T-stub".into()),
                    cancel_requested: false,
                },
            );
        }
        let result = server
            .mse_doctor(Parameters(DoctorReq { bind: Some(bind) }))
            .await
            .expect("mse_doctor");
        let json: JsonValue =
            serde_json::from_str(&extract_text_payload(&result)).expect("doctor json");
        let findings = json["audit_findings"]["findings"]
            .as_array()
            .expect("audit_findings.findings array");
        assert_eq!(json["audit_findings"]["count"], 1, "body: {json}");
        assert_eq!(findings.len(), 1, "body: {json}");
        assert_eq!(findings[0]["task_id"], "T-stub");
        assert_eq!(findings[0]["run_id"], "R-stub");
        assert_eq!(findings[0]["step"], "worker");
        assert_eq!(findings[0]["artifact_name"], "audit:worker");
    }

    /// GH #32: `mse_doctor`'s own HTTP-calling + extraction logic for
    /// the `degradations` section, isolated the same way
    /// `mse_doctor_surfaces_audit_findings_via_stub_steps_api` isolates the
    /// `audit_findings` one — a stub router serving the real `GET
    /// /v1/runs/:id` response *shape* (a plain `RunRecord` with a
    /// non-empty `degradations` array), not a real dispatch.
    #[tokio::test]
    async fn mse_doctor_degradations_scan_present_in_response() {
        use axum::extract::Path as AxumPath;
        use axum::routing::get;
        use axum::{Json, Router};

        async fn stub_healthz() -> &'static str {
            "ok"
        }
        async fn stub_run(AxumPath(run_id): AxumPath<String>) -> Json<JsonValue> {
            Json(serde_json::json!({
                "id": run_id,
                "task_id": "T-stub",
                "status": "running",
                "step_entries": [],
                "degradations": [
                    {
                        "tool": "code_index",
                        "error": "unavailable",
                        "fallback": "grep",
                        "step_ref": "worker",
                        "attempt": 1,
                        "at": 0,
                    }
                ],
                "operator_sid": null,
                "result_ref": null,
                "created_at": 0,
                "updated_at": 0,
            }))
        }

        let router = Router::new()
            .route("/v1/healthz", get(stub_healthz))
            .route("/v1/runs/:run_id", get(stub_run));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let bind = addr.to_string();

        let server = MseServer::new();
        {
            let mut inner = server.state.write().await;
            inner.runs.insert(
                "R-stub".into(),
                RunHandle {
                    run_id: "R-stub".into(),
                    status: RunStatus::Running,
                    task_id: Some("T-stub".into()),
                    cancel_requested: false,
                },
            );
        }
        let result = server
            .mse_doctor(Parameters(DoctorReq { bind: Some(bind) }))
            .await
            .expect("mse_doctor must never fail on a degradations-scan issue");
        let json: JsonValue =
            serde_json::from_str(&extract_text_payload(&result)).expect("doctor json");
        assert_eq!(json["degradations"]["count"], 1, "body: {json}");
        let runs = json["degradations"]["runs"]
            .as_array()
            .expect("degradations.runs array");
        assert_eq!(runs.len(), 1, "body: {json}");
        assert_eq!(runs[0]["run_id"], "R-stub");
        assert_eq!(runs[0]["task_id"], "T-stub");
        assert_eq!(runs[0]["count"], 1);
    }
}

// ─── GH #79 Phase 3: classify_* → Diagnostic sibling projections ──────────
#[cfg(test)]
mod diag_sibling_tests {
    use super::*;
    use mlua_swarm_diag::{BpDoctorFamily, DiagLevel, DiagStage};

    #[test]
    fn ok_verdicts_project_to_no_diagnostic() {
        assert!(diag_from_agent_md("a", "OK", 10, 2).is_none());
        let ok = serde_json::json!({"severity": "OK", "unknown_tools": []});
        assert!(diag_from_tool_lint("a", &ok).is_none());
        let ok = serde_json::json!({"severity": "OK", "present": true, "kind": "literal_enum"});
        assert!(diag_from_output_contract_lint("a", &ok).is_none());
        let ok = serde_json::json!({"severity": "OK", "kind_requires_binding": false});
        assert!(diag_from_worker_binding_lint("a", &ok).is_none());
    }

    #[test]
    fn agent_md_warn_projects_with_size_kind_and_agent_span() {
        let d = diag_from_agent_md("planner", "WARN", 30_000, 250).expect("WARN must project");
        assert_eq!(d.kind, "agent-md-size");
        assert_eq!(d.level, DiagLevel::Warn);
        assert!(matches!(
            d.stage,
            DiagStage::BpDoctor {
                family: BpDoctorFamily::AgentMdSize
            }
        ));
        assert!(d.message.contains("planner"));
        // BLOCK maps to Error.
        let d = diag_from_agent_md("planner", "BLOCK", 60_000, 600).expect("BLOCK must project");
        assert_eq!(d.level, DiagLevel::Error);
    }

    #[test]
    fn tool_lint_warn_carries_each_unknown_tool_as_a_note() {
        let verdict = classify_tool_lint(
            &["mcp__mse__no_such_tool".to_string(), "Read".to_string()],
            &std::collections::BTreeSet::new(),
        );
        let d = diag_from_tool_lint("scout", &verdict).expect("WARN must project");
        assert_eq!(d.kind, "tool-unknown-mcp-ref");
        assert!(d.notes.iter().any(|n| n.contains("mcp__mse__no_such_tool")));
    }

    #[test]
    fn worker_binding_warn_realizes_the_dual_stage_kind_with_suggestion() {
        let verdict =
            classify_worker_binding_lint(&mlua_swarm::blueprint::AgentKind::Operator, None);
        let d = diag_from_worker_binding_lint("greeter", &verdict).expect("WARN must project");
        // Same kind the compile stage emits as Error — the GH #79
        // dual-stage story realized: one LintDecl, two stages.
        assert_eq!(d.kind, "worker-binding-missing");
        assert_eq!(d.level, DiagLevel::Warn);
        assert!(matches!(
            d.stage,
            DiagStage::BpDoctor {
                family: BpDoctorFamily::WorkerBindingLint
            }
        ));
        let s = d
            .suggestion
            .expect("suggestion must carry the runner patch");
        assert!(s.patch.contains("backend = \"ws_operator\""));
        assert!(!d.notes.is_empty(), "the classifier reason must carry over");
    }

    #[test]
    fn findings_walker_maps_checks_levels_and_spans() {
        let verdict = serde_json::json!({
            "findings": [
                {"check": "binding_requirements_info", "severity": "INFO",
                 "agent": "worker", "message": "agent 'worker' needs a manifest entry"},
                {"check": "legacy_worker_binding", "severity": "WARN",
                 "agent": "old", "message": "agent 'old' uses the deprecated fallback"},
                {"check": "strict_binding_without_runners", "severity": "WARN",
                 "message": "strict binding is a no-op"},
                {"check": "some_future_check", "severity": "WARN",
                 "message": "not in the registry yet"},
            ]
        });
        let ds = diag_from_findings(BpDoctorFamily::BindingLint, &verdict);
        // The unknown check is skipped (old `findings` field still
        // carries it), the three known ones project.
        assert_eq!(ds.len(), 3);
        assert_eq!(ds[0].kind, "binding-requirements-info");
        assert_eq!(ds[0].level, DiagLevel::Info);
        assert_eq!(ds[1].kind, "legacy-worker-binding");
        assert_eq!(ds[1].level, DiagLevel::Warn);
        // Blueprint-scoped finding (no agent field) spans BlueprintRoot.
        match &ds[2].span.as_ref().expect("span must be set").element {
            mlua_swarm_diag::DiagElement::BlueprintRoot => {}
            other => panic!("expected BlueprintRoot span, got {other:?}"),
        }
    }

    #[test]
    fn skip_on_findings_project_with_the_skip_guide_docs_ref() {
        let verdict = serde_json::json!({
            "findings": [
                {"check": "skip_on_pattern_conflicts_with_halt_on", "severity": "WARN",
                 "value": "BLOCKED", "message": "value 'BLOCKED' appears in both guards"},
            ]
        });
        let ds = diag_from_findings(BpDoctorFamily::SkipOnLint, &verdict);
        assert_eq!(ds.len(), 1);
        assert_eq!(ds[0].kind, "skip-on-pattern-conflicts-with-halt-on");
        assert_eq!(
            ds[0].docs_ref.expect("docs_ref must be set").uri,
            "mse://guides/skip-tier-and-skip-on"
        );
    }

    /// Every kind a sibling projection can emit must be declared in the
    /// `LINT_DECLS` registry.
    #[test]
    fn every_sibling_emitted_kind_is_a_declared_lint() {
        let kinds = [
            "agent-md-size",
            "tool-unknown-mcp-ref",
            "output-contract-missing",
            "worker-binding-missing",
            "binding-requirements-info",
            "strict-binding-without-runners",
            "legacy-worker-binding",
            "binding-resolution-error",
            "skip-on-missing-for-skip-like-verdict-value",
            "skip-on-declared-but-no-matching-verdict-value",
            "skip-on-pattern-conflicts-with-halt-on",
            "context-policy-strips-projection-roots",
            "projection-root-seed-missing",
        ];
        for kind in kinds {
            assert!(
                mlua_swarm_diag::lint_decl(kind).is_some(),
                "sibling-emitted kind '{kind}' has no LINT_DECLS entry"
            );
        }
    }
}

// ─── GH #78 Layer 1: context_policy_lint family ───────────────────────────
#[cfg(test)]
mod context_policy_lint_tests {
    use super::*;
    use mlua_swarm::blueprint::{AgentDef, AgentMeta};
    use mlua_swarm_schema::ContextPolicy;

    /// Minimal Blueprint fixture via serde (the Blueprint type has no
    /// `Default`; the wire format's serde defaults are the canonical
    /// minimal shape).
    fn bp_with(agents: Vec<AgentDef>) -> Blueprint {
        let mut bp: Blueprint = serde_json::from_value(serde_json::json!({
            "id": "cp-lint-fixture",
            "flow": {"kind": "seq", "children": []},
            "agents": [],
        }))
        .expect("minimal fixture blueprint must deserialize");
        bp.agents = agents;
        bp
    }

    fn agent(name: &str, meta: Option<AgentMeta>) -> AgentDef {
        let mut def: AgentDef = serde_json::from_value(serde_json::json!({
            "name": name,
            "kind": "rust_fn",
            "spec": {},
        }))
        .expect("minimal fixture agent must deserialize");
        def.meta = meta;
        def
    }

    fn meta_with_policy(policy: ContextPolicy) -> AgentMeta {
        AgentMeta {
            context_policy: Some(policy),
            ..Default::default()
        }
    }

    fn exclude_both_roots() -> ContextPolicy {
        ContextPolicy {
            exclude: vec!["work_dir".into(), "project_root".into()],
            ..Default::default()
        }
    }

    fn checks_of(verdict: &serde_json::Value) -> Vec<String> {
        verdict["findings"]
            .as_array()
            .expect("findings array")
            .iter()
            .map(|f| f["check"].as_str().expect("check string").to_string())
            .collect()
    }

    // AC case 3: neither context_policy declared, no simulation → OK.
    #[test]
    fn no_policy_and_no_simulation_produces_no_findings() {
        let bp = bp_with(vec![agent("solo", None)]);
        let verdict = classify_context_policy_lint(&bp, None);
        assert!(checks_of(&verdict).is_empty(), "verdict: {verdict}");
    }

    // AC case 1: BP-global default_context_policy declared.
    #[test]
    fn bp_global_policy_stripping_both_roots_warns_per_agent() {
        let mut bp = bp_with(vec![agent("a", None), agent("b", None)]);
        bp.default_context_policy = Some(exclude_both_roots());
        let verdict = classify_context_policy_lint(&bp, None);
        let checks = checks_of(&verdict);
        assert_eq!(
            checks,
            vec![
                "context_policy_strips_projection_roots",
                "context_policy_strips_projection_roots"
            ],
            "verdict: {verdict}"
        );
        assert_eq!(verdict["findings"][0]["policy_source"], "bp-global");
        assert_eq!(verdict["findings"][0]["severity"], "WARN");
    }

    // AC case 2: per-agent AgentMeta.context_policy declared (outranks
    // the BP-global tier).
    #[test]
    fn per_agent_policy_stripping_both_roots_warns_only_that_agent() {
        let bp = bp_with(vec![
            agent("clean", None),
            agent("filtered", Some(meta_with_policy(exclude_both_roots()))),
        ]);
        let verdict = classify_context_policy_lint(&bp, None);
        let findings = verdict["findings"].as_array().expect("findings");
        assert_eq!(findings.len(), 1, "verdict: {verdict}");
        assert_eq!(findings[0]["agent"], "filtered");
        assert_eq!(findings[0]["policy_source"], "agent");
    }

    // A policy that strips only ONE root is not flagged — the other
    // still resolves via RootPreference's fallback.
    #[test]
    fn policy_stripping_one_root_is_not_flagged() {
        let policy = ContextPolicy {
            exclude: vec!["work_dir".into()],
            ..Default::default()
        };
        let bp = bp_with(vec![agent("half", Some(meta_with_policy(policy)))]);
        let verdict = classify_context_policy_lint(&bp, None);
        assert!(checks_of(&verdict).is_empty(), "verdict: {verdict}");
    }

    // An include-list that omits both roots is equivalent to excluding
    // them (allows() semantics) — flagged.
    #[test]
    fn include_list_omitting_both_roots_is_flagged() {
        let policy = ContextPolicy {
            include: Some(vec!["task_metadata".into()]),
            ..Default::default()
        };
        let bp = bp_with(vec![agent("narrow", Some(meta_with_policy(policy)))]);
        let verdict = classify_context_policy_lint(&bp, None);
        assert_eq!(
            checks_of(&verdict),
            vec!["context_policy_strips_projection_roots"]
        );
    }

    // AC: seedless simulated launch → projection_root_seed_missing WARN.
    #[test]
    fn seedless_simulated_launch_warns_seed_missing() {
        let bp = bp_with(vec![agent("solo", None)]);
        let sim = serde_json::json!({});
        let verdict = classify_context_policy_lint(&bp, Some(&sim));
        assert_eq!(checks_of(&verdict), vec!["projection_root_seed_missing"]);
        assert_eq!(verdict["findings"][0]["severity"], "WARN");
    }

    // AC: the launch payload seeding the required field → OK.
    #[test]
    fn seeded_simulated_launch_produces_no_findings() {
        let bp = bp_with(vec![agent("solo", None)]);
        let sim = serde_json::json!({"project_root": "/repo"});
        let verdict = classify_context_policy_lint(&bp, Some(&sim));
        assert!(checks_of(&verdict).is_empty(), "verdict: {verdict}");
    }

    // The GH #78 P1a interaction: the launch seeds a root, but the
    // agent's policy filters that very field out — seed simulation must
    // apply the policy, not just check payload presence.
    #[test]
    fn seeded_field_filtered_by_policy_still_warns() {
        let policy = ContextPolicy {
            exclude: vec!["project_root".into()],
            ..Default::default()
        };
        let bp = bp_with(vec![agent("solo", Some(meta_with_policy(policy)))]);
        let sim = serde_json::json!({"project_root": "/repo"});
        let verdict = classify_context_policy_lint(&bp, Some(&sim));
        assert_eq!(checks_of(&verdict), vec!["projection_root_seed_missing"]);
    }

    // The finding names the declared placement preference.
    #[test]
    fn seed_missing_finding_carries_the_declared_root_preference() {
        let mut bp = bp_with(vec![agent("solo", None)]);
        bp.projection_placement = Some(mlua_swarm_schema::ProjectionPlacementSpec {
            root: Some("project_root".into()),
            dir_template: None,
        });
        let sim = serde_json::json!({});
        let verdict = classify_context_policy_lint(&bp, Some(&sim));
        assert_eq!(verdict["findings"][0]["root_preference"], "project_root");
    }

    /// jikki-caught regression (2026-07-24): the seed-missing message
    /// used to repeat the preferred root as a literal — `Seed 'work_dir'
    /// (or 'work_dir'/'project_root') ...` — because the alternate list
    /// was hardcoded rather than derived from the preference. The fix
    /// names the alternate deterministically; this test locks it in for
    /// both preferences so the drift cannot recur.
    #[test]
    fn seed_missing_message_names_the_alternate_root_not_the_preferred_one() {
        for (preference, alternate) in [("work_dir", "project_root"), ("project_root", "work_dir")]
        {
            let mut bp = bp_with(vec![agent("solo", None)]);
            bp.projection_placement = Some(mlua_swarm_schema::ProjectionPlacementSpec {
                root: Some(preference.into()),
                dir_template: None,
            });
            let sim = serde_json::json!({});
            let verdict = classify_context_policy_lint(&bp, Some(&sim));
            let msg = verdict["findings"][0]["message"]
                .as_str()
                .expect("message string");
            let preferred_quoted = format!("'{preference}'");
            let alternate_quoted = format!("'{alternate}'");
            // The preferred root is named exactly twice (the "declared
            // preference: 'X'" clause and the "Seed 'X'" clause); the
            // alternate is named exactly once (the "(or 'Y' as the
            // fallback)" clause).
            assert_eq!(
                msg.matches(&preferred_quoted).count(),
                2,
                "preferred root '{preference}' occurrences: {msg}"
            );
            assert_eq!(
                msg.matches(&alternate_quoted).count(),
                1,
                "alternate root '{alternate}' occurrences: {msg}"
            );
        }
    }

    // Sibling projection: both checks map to declared registry kinds.
    #[test]
    fn context_policy_findings_project_to_declared_diagnostics() {
        let mut bp = bp_with(vec![agent(
            "filtered",
            Some(meta_with_policy(exclude_both_roots())),
        )]);
        bp.projection_placement = None;
        let sim = serde_json::json!({});
        let verdict = classify_context_policy_lint(&bp, Some(&sim));
        let ds = diag_from_findings(mlua_swarm_diag::BpDoctorFamily::ContextPolicyLint, &verdict);
        assert_eq!(ds.len(), 2, "verdict: {verdict}");
        assert_eq!(ds[0].kind, "context-policy-strips-projection-roots");
        assert_eq!(ds[1].kind, "projection-root-seed-missing");
        for d in &ds {
            assert!(
                mlua_swarm_diag::lint_decl(d.kind).is_some(),
                "kind {} must be declared",
                d.kind
            );
            assert_eq!(d.level, mlua_swarm_diag::DiagLevel::Warn);
        }
    }
}

// ─── bp_doctor spawner_hint_lint family ──────────────────────────────────
//
// Regression axis: registering a Blueprint does not compile it, so a
// Blueprint still declaring the removed `operator_delegate` layer
// registers clean and only dies at its first dispatch. This family is the
// authoring-time surface for that state; the compile stage refuses the
// same lint kind as an Error.
#[cfg(test)]
mod spawner_hint_lint_tests {
    use super::*;

    /// A minimal Blueprint carrying the given `spawner_hints.layers`,
    /// built through serde so the fixture cannot drift from the wire
    /// format authors actually write.
    fn bp_with_layers(layers: &[&str]) -> Blueprint {
        serde_json::from_value(serde_json::json!({
            "id": "spawner-hint-lint-fixture",
            "flow": {"kind": "seq", "children": []},
            "agents": [],
            "spawner_hints": {"layers": layers},
        }))
        .expect("fixture blueprint must deserialize")
    }

    fn findings(verdict: &serde_json::Value) -> &Vec<serde_json::Value> {
        verdict
            .get("findings")
            .and_then(|f| f.as_array())
            .expect("the family always reports a findings array")
    }

    #[test]
    fn flags_the_removed_operator_delegate_layer() {
        let verdict = classify_spawner_hint_lint(&bp_with_layers(&["operator_delegate"]));
        let found = findings(&verdict);
        assert_eq!(found.len(), 1, "one declared removed layer, one finding");
        assert_eq!(found[0]["check"], "removed_spawner_hint");
        assert_eq!(found[0]["severity"], "WARN");
        assert_eq!(found[0]["layer"], "operator_delegate");

        let message = found[0]["message"].as_str().expect("message is a string");
        assert!(
            message.contains("will fail to compile on its next dispatch"),
            "the reader has to learn this Blueprint is already unlaunchable, not merely \
             untidy: {message}"
        );
        assert!(
            message.contains("operators[]")
                && message.contains("spec.operator_ref")
                && message.contains("operator_sid"),
            "the message must name the AgentSpec-axis replacement: {message}"
        );
    }

    /// The all-clear case: a live layer key, and no layers at all, both
    /// report an empty findings array (not an absent field).
    #[test]
    fn is_silent_for_live_and_absent_layer_keys() {
        for layers in [&["main_ai", "senior_escalation"][..], &[][..]] {
            let verdict = classify_spawner_hint_lint(&bp_with_layers(layers));
            assert!(
                findings(&verdict).is_empty(),
                "no removed layer declared, so nothing to report (layers: {layers:?})"
            );
        }
    }

    #[test]
    fn projects_a_warn_diagnostic_of_the_declared_migration_kind() {
        let verdict = classify_spawner_hint_lint(&bp_with_layers(&["operator_delegate"]));
        let diags = diag_from_findings(mlua_swarm_diag::BpDoctorFamily::SpawnerHintLint, &verdict);
        assert_eq!(diags.len(), 1);
        let d = &diags[0];

        assert_eq!(d.kind, "removed-spawner-hint");
        // The stage asymmetry the model allows and this lint uses: Error
        // at compile (the compile refuses), Warn here (report-only).
        assert_eq!(d.level, mlua_swarm_diag::DiagLevel::Warn);
        assert!(matches!(
            d.stage,
            mlua_swarm_diag::DiagStage::BpDoctor {
                family: mlua_swarm_diag::BpDoctorFamily::SpawnerHintLint
            }
        ));

        let decl = mlua_swarm_diag::lint_decl("removed-spawner-hint")
            .expect("the kind must resolve in LINT_DECLS");
        assert_eq!(decl.category, mlua_swarm_diag::LintCategory::Migration);

        // "The same patch the compile arm does" asserted as whole-value
        // equality against the one constructor both arms call, not as
        // `patch.contains("operator_ref")` — the substring form is
        // satisfied by any two texts that mention the field, so it would
        // stay green through exactly the drift it is meant to catch. The
        // compile-stage sibling of this assertion is in
        // `blueprint::compiler`'s
        // `removed_spawner_hint_projects_a_migration_diagnostic_naming_the_replacement`.
        let suggestion = d
            .suggestion
            .as_ref()
            .expect("the doctor finding carries the same patch the compile arm does");
        assert_eq!(
            suggestion,
            &mlua_swarm_diag::removed_spawner_hint_suggestion()
        );
        assert!(
            suggestion.patch.contains("\"operator_ref\""),
            "the shared patch has to show the field an author must add: {}",
            suggestion.patch
        );
        assert_eq!(
            suggestion.applicability,
            mlua_swarm_diag::Applicability::HasPlaceholders
        );
        assert_eq!(
            d.docs_ref.as_ref().expect("docs_ref must be set").uri,
            "mse://guides/blueprint-authoring"
        );
    }

    /// The stage contract: a kind this stage can emit has to be listed,
    /// or an author's `allow` on it raises `non-suppressible-lint`
    /// instead of suppressing.
    #[test]
    fn the_kind_is_declared_as_bp_doctor_emitted() {
        assert!(BP_DOCTOR_EMITTED_KINDS.contains(&"removed-spawner-hint"));
    }
}

// ─── bp_doctor verdict_contract_lint family ──────────────────────────────
//
// Regression axis: a Blueprint that declares verdict contracts and then
// never branches on them compiles clean, because the reverse-direction
// check is `tracing::warn!`-only outside `strict_verdict_handling`. This
// family is the reader-visible surface for exactly that state.
#[cfg(test)]
mod verdict_contract_lint_tests {
    use super::*;

    /// A Blueprint whose flow is the given wire-format node and whose
    /// agents carry the given `(name, channel, values)` verdict
    /// contracts. Built through serde (the canonical minimal shape) so
    /// the fixture cannot drift from the wire format authors actually
    /// write.
    fn verdict_bp(flow: serde_json::Value, agents_verdicts: &[(&str, &str, &[&str])]) -> Blueprint {
        let agents: Vec<serde_json::Value> = agents_verdicts
            .iter()
            .map(|(name, channel, values)| {
                serde_json::json!({
                    "name": name,
                    "kind": "agent_block",
                    "spec": {},
                    "verdict": {"channel": channel, "values": values},
                })
            })
            .collect();
        serde_json::from_value(serde_json::json!({
            "id": "verdict-lint-fixture",
            "flow": flow,
            "agents": agents,
        }))
        .expect("fixture blueprint must deserialize")
    }

    /// `Step{ref = <agent>, out = <out_path>}` — the producer side a
    /// cond's Path has to resolve back through.
    fn verdict_step(agent: &str, out_path: &str) -> serde_json::Value {
        serde_json::json!({
            "kind": "step",
            "ref": agent,
            "in": {"op": "lit", "value": null},
            "out": {"op": "path", "at": out_path},
        })
    }

    /// `eq(path(<path>), lit(<value>))` around no-op arms — the
    /// body-channel cond shape (bare `$.<step>`, no `.parts.verdict`).
    fn body_cond_branch(path: &str, value: &str) -> serde_json::Value {
        serde_json::json!({
            "kind": "branch",
            "cond": {
                "op": "eq",
                "lhs": {"op": "path", "at": path},
                "rhs": {"op": "lit", "value": value},
            },
            "then": {"kind": "seq", "children": []},
            "else": {"kind": "seq", "children": []},
        })
    }

    fn seq(children: Vec<serde_json::Value>) -> serde_json::Value {
        serde_json::json!({"kind": "seq", "children": children})
    }

    /// The exact production shape that motivated this family: a
    /// sequential gate chain with `channel: "body"` contracts and no
    /// `Branch` anywhere. Every declared value is unhandled — the per-
    /// value findings surface with the decorative contract visible before
    /// the first dispatch. Aggregate-vs-per-value separation is verified
    /// separately in `verdict_contract_never_read_flags_agents_whose_gate_is_fully_dead`;
    /// this test locks in the per-value shape.
    #[test]
    fn verdict_contract_lint_flags_a_gate_chain_that_never_branches() {
        let bp = verdict_bp(
            seq(vec![
                verdict_step("gate-danger", "$.r.danger"),
                verdict_step("gate-leak", "$.r.leak"),
            ]),
            &[
                ("gate-danger", "body", &["PASS", "BLOCKED"]),
                ("gate-leak", "body", &["PASS", "BLOCKED"]),
            ],
        );
        let lint = classify_verdict_contract_lint(&bp);
        let findings = lint["findings"].as_array().expect("findings array");
        // Ordering: 2 per-agent aggregates first, then 4 per-value. The
        // per-value findings occupy [2..6].
        assert_eq!(findings.len(), 6, "{lint}");
        let per_value: Vec<&serde_json::Value> = findings
            .iter()
            .filter(|f| f["check"] == "verdict_value_unhandled")
            .collect();
        assert_eq!(
            per_value.len(),
            4,
            "2 agents x 2 declared values, none handled: {lint}"
        );
        // Stable order among per-value: sorted by agent name, then declared-value order.
        assert_eq!(per_value[0]["agent"], "gate-danger");
        assert_eq!(per_value[0]["value"], "PASS");
        assert_eq!(per_value[0]["severity"], "WARN");
        assert_eq!(per_value[0]["channel"], "body");
        let msg = per_value[0]["message"].as_str().expect("message string");
        assert!(msg.contains("gate-danger"), "{msg}");
        assert!(
            msg.contains("channel: \"body\""),
            "a body-channel finding must warn that the OUTPUT value itself is \
             constrained, which is what turns this into a rejected Final: {msg}"
        );
    }

    /// A handled value produces no finding — the family stays silent for
    /// Blueprints that use verdicts as intended.
    #[test]
    fn verdict_contract_lint_is_silent_when_every_value_is_branched_on() {
        let bp = verdict_bp(
            seq(vec![
                verdict_step("gate", "$.gate_out"),
                body_cond_branch("$.gate_out", "PASS"),
                body_cond_branch("$.gate_out", "BLOCKED"),
            ]),
            &[("gate", "body", &["PASS", "BLOCKED"])],
        );
        let lint = classify_verdict_contract_lint(&bp);
        assert!(
            lint["findings"]
                .as_array()
                .expect("findings array")
                .is_empty(),
            "both declared values are read by a cond: {lint}"
        );
    }

    /// Partial coverage is the realistic drift: the happy path is
    /// branched on, the failure token is not.
    #[test]
    fn verdict_contract_lint_flags_only_the_unhandled_value() {
        let bp = verdict_bp(
            seq(vec![
                verdict_step("gate", "$.gate_out"),
                body_cond_branch("$.gate_out", "PASS"),
            ]),
            &[("gate", "body", &["PASS", "BLOCKED"])],
        );
        let lint = classify_verdict_contract_lint(&bp);
        let findings = lint["findings"].as_array().expect("findings array");
        assert_eq!(findings.len(), 1, "{lint}");
        assert_eq!(findings[0]["value"], "BLOCKED");
    }

    /// An agent with no declared contract contributes nothing — the
    /// family is opt-in via `agents[].verdict`, like the compile gate.
    #[test]
    fn verdict_contract_lint_ignores_agents_without_a_contract() {
        let mut bp = verdict_bp(
            verdict_step("gate", "$.gate_out"),
            &[("gate", "body", &["PASS"])],
        );
        bp.agents[0].verdict = None;
        let lint = classify_verdict_contract_lint(&bp);
        assert!(lint["findings"]
            .as_array()
            .expect("findings array")
            .is_empty());
    }

    /// The part-channel finding omits the body-channel warning sentence:
    /// `channel: "part"` leaves the OUTPUT body free, so an unread
    /// contract there is a dead branch, not a rejected Final.
    #[test]
    fn verdict_contract_lint_part_channel_message_omits_the_body_note() {
        let bp = verdict_bp(
            verdict_step("aggregate", "$.aggregate"),
            &[("aggregate", "part", &["PASS"])],
        );
        let lint = classify_verdict_contract_lint(&bp);
        let findings = lint["findings"].as_array().expect("findings array");
        // Per-agent aggregate + per-value; the per-value finding is [1].
        assert_eq!(findings.len(), 2, "{lint}");
        let per_value = findings
            .iter()
            .find(|f| f["check"] == "verdict_value_unhandled")
            .expect("per-value finding present");
        assert_eq!(per_value["channel"], "part");
        let msg = per_value["message"].as_str().expect("message string");
        assert!(
            !msg.contains("report body"),
            "the body-channel-only note must not leak into a part finding: {msg}"
        );
    }

    /// Sibling projection: the finding maps to the declared registry kind
    /// (shared with the compile stage's `VerdictValueUnhandled`).
    #[test]
    fn verdict_contract_findings_project_to_declared_diagnostics() {
        let bp = verdict_bp(
            verdict_step("gate", "$.gate_out"),
            &[("gate", "body", &["PASS"])],
        );
        let verdict = classify_verdict_contract_lint(&bp);
        let ds = diag_from_findings(
            mlua_swarm_diag::BpDoctorFamily::VerdictContractLint,
            &verdict,
        );
        // Both per-agent aggregate and per-value fire for a single-value
        // contract that no cond reads. The aggregate carries the concrete
        // gate = true patch; the per-value keeps parity with strict mode.
        assert_eq!(ds.len(), 2, "verdict: {verdict}");
        assert_eq!(ds[0].kind, "verdict-contract-never-read");
        assert_eq!(ds[1].kind, "verdict-value-unhandled");
        assert!(
            mlua_swarm_diag::lint_decl(ds[0].kind).is_some(),
            "kind must be declared in LINT_DECLS"
        );
        assert_eq!(ds[0].level, mlua_swarm_diag::DiagLevel::Warn);
        assert_eq!(
            ds[0].docs_ref.as_ref().map(|d| d.uri),
            Some("mse://guides/blueprint-authoring")
        );
    }

    /// The production shape that motivated this finding: N agents in a
    /// straight-line pipeline, each declaring `[PASS, BLOCKED]` with
    /// nothing branched on. Aggregate finding fires once per agent —
    /// separating "gate ごと消滅" (N agents) from the per-value baseline
    /// noise (2N findings for the normal `PASS + BLOCKED` unread).
    #[test]
    fn verdict_contract_never_read_flags_agents_whose_gate_is_fully_dead() {
        let bp = verdict_bp(
            seq(vec![
                verdict_step("gate-danger", "$.r.danger"),
                verdict_step("gate-leak", "$.r.leak"),
            ]),
            &[
                ("gate-danger", "body", &["PASS", "BLOCKED"]),
                ("gate-leak", "body", &["PASS", "BLOCKED"]),
            ],
        );
        let lint = classify_verdict_contract_lint(&bp);
        let findings = lint["findings"].as_array().expect("findings array");
        // Ordering: 2 per-agent aggregates (before) + 4 per-value (after).
        assert_eq!(findings.len(), 6, "{lint}");
        assert_eq!(findings[0]["check"], "verdict_contract_never_read");
        assert_eq!(findings[0]["agent"], "gate-danger");
        assert_eq!(findings[0]["severity"], "WARN");
        assert_eq!(findings[0]["channel"], "body");
        assert_eq!(
            findings[0]["declared_values"],
            serde_json::json!(["PASS", "BLOCKED"])
        );
        let msg0 = findings[0]["message"].as_str().expect("message string");
        assert!(
            msg0.contains("no downstream Branch/Loop cond reads any of them"),
            "{msg0}"
        );
        assert!(
            msg0.contains("gate = true"),
            "must name the recovery: {msg0}"
        );
        assert!(
            msg0.contains("channel: \"body\""),
            "body-channel aggregate must inherit the OUTPUT-shape warning: {msg0}"
        );

        assert_eq!(findings[1]["check"], "verdict_contract_never_read");
        assert_eq!(findings[1]["agent"], "gate-leak");

        // Per-value findings still follow, unchanged in shape.
        assert_eq!(findings[2]["check"], "verdict_value_unhandled");
        assert_eq!(findings[5]["check"], "verdict_value_unhandled");
    }

    /// Partial coverage never fires the aggregate: at least one value is
    /// read, so the gate is not "dead", only the specific token is unread
    /// (which the per-value finding already names).
    #[test]
    fn verdict_contract_never_read_silent_on_partial_coverage() {
        let bp = verdict_bp(
            seq(vec![
                verdict_step("gate", "$.gate_out"),
                body_cond_branch("$.gate_out", "PASS"),
            ]),
            &[("gate", "body", &["PASS", "BLOCKED"])],
        );
        let lint = classify_verdict_contract_lint(&bp);
        let findings = lint["findings"].as_array().expect("findings array");
        // Only the per-value BLOCKED finding, no aggregate.
        assert_eq!(findings.len(), 1, "{lint}");
        assert_eq!(findings[0]["check"], "verdict_value_unhandled");
        assert_eq!(findings[0]["value"], "BLOCKED");
    }

    /// Part-channel aggregate omits the body-channel OUTPUT-shape warning,
    /// mirroring the per-value finding's discipline: `channel: "part"`
    /// leaves the body free, so an unread contract is a dead branch, not
    /// a rejected Final.
    #[test]
    fn verdict_contract_never_read_part_channel_omits_the_body_note() {
        let bp = verdict_bp(
            verdict_step("aggregate", "$.aggregate"),
            &[("aggregate", "part", &["PASS"])],
        );
        let lint = classify_verdict_contract_lint(&bp);
        let findings = lint["findings"].as_array().expect("findings array");
        // One aggregate + one per-value; the aggregate is [0].
        assert_eq!(findings.len(), 2, "{lint}");
        assert_eq!(findings[0]["check"], "verdict_contract_never_read");
        assert_eq!(findings[0]["channel"], "part");
        let msg = findings[0]["message"].as_str().expect("message string");
        assert!(
            !msg.contains("report body"),
            "the body-channel-only note must not leak into a part aggregate: {msg}"
        );
    }

    /// The `Suggestion` payload is what turns this finding into an
    /// actionable fix without the reader diffing per-value noise —
    /// verify it lands on the aggregate and stays off the per-value
    /// projection.
    #[test]
    fn verdict_contract_never_read_diagnostic_carries_gate_true_suggestion() {
        let bp = verdict_bp(
            verdict_step("gate", "$.gate_out"),
            &[("gate", "body", &["PASS", "BLOCKED"])],
        );
        let verdict = classify_verdict_contract_lint(&bp);
        let ds = diag_from_findings(
            mlua_swarm_diag::BpDoctorFamily::VerdictContractLint,
            &verdict,
        );
        // 1 aggregate + 2 per-value.
        assert_eq!(ds.len(), 3, "verdict: {verdict}");
        assert_eq!(ds[0].kind, "verdict-contract-never-read");
        let sug = ds[0]
            .suggestion
            .as_ref()
            .expect("aggregate diagnostic must carry a suggestion");
        assert_eq!(sug.patch, "gate = true,");
        assert_eq!(
            sug.applicability,
            mlua_swarm_diag::Applicability::MaybeIncorrect
        );
        // Per-value projections keep no suggestion — the aggregate owns
        // the recovery narrative for the fully-dead-gate case.
        assert!(ds[1].suggestion.is_none());
        assert!(ds[2].suggestion.is_none());
    }
}

// ─── bp_doctor lint level control (allow / warn / deny, 3 layers) ─────────
//
// End-to-end over the real tool body against a stub
// `GET /v1/blueprints/:id/head`: the resolution seam only exists inside
// `bp_doctor` (which family surface a finding leaves, what the verdict
// folds), so a unit test of the resolver alone would not cover it.
#[cfg(test)]
mod bp_doctor_lint_control_tests {
    use super::*;

    /// Number of `system_prompt` lines every fixture agent carries — over
    /// the 200-line WARN threshold, far under every byte threshold, so
    /// `agent-md-size` fires at WARN by lines alone.
    const OVERSIZED_LINES: usize = 250;

    fn text_payload(result: &rmcp::model::CallToolResult) -> String {
        match &result.content.first().expect("content").raw {
            rmcp::model::RawContent::Text(t) => t.text.clone(),
            _ => panic!("expected text content"),
        }
    }

    /// One operator agent whose `system_prompt` trips the size WARN band.
    fn oversized_agent(name: &str) -> mlua_swarm::blueprint::AgentDef {
        serde_json::from_value(serde_json::json!({
            "name": name,
            "kind": "operator",
            "spec": {},
            "profile": { "system_prompt": "x\n".repeat(OVERSIZED_LINES) },
        }))
        .expect("fixture agent must deserialize")
    }

    /// Minimal Blueprint carrying the given over-threshold agents (serde
    /// defaults are the canonical minimal shape — `Blueprint` has no
    /// `Default`).
    fn oversized_bp(agents: &[&str]) -> Blueprint {
        let mut bp: Blueprint = serde_json::from_value(serde_json::json!({
            "id": "lint-control-fixture",
            "flow": {"kind": "seq", "children": []},
            "agents": [],
        }))
        .expect("fixture blueprint must deserialize");
        bp.agents = agents.iter().copied().map(oversized_agent).collect();
        bp
    }

    fn lint_map(pairs: &[(&str, &str)]) -> BTreeMap<String, mlua_swarm_schema::LintSetting> {
        pairs
            .iter()
            .map(|(key, value)| {
                (
                    (*key).to_string(),
                    match *value {
                        "allow" => mlua_swarm_schema::LintSetting::Allow,
                        "warn" => mlua_swarm_schema::LintSetting::Warn,
                        "deny" => mlua_swarm_schema::LintSetting::Deny,
                        other => panic!("fixture lint value: {other}"),
                    },
                )
            })
            .collect()
    }

    fn call_site_lints(pairs: &[(&str, &str)]) -> Option<BTreeMap<String, String>> {
        Some(
            pairs
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
        )
    }

    /// Serves the fixture at `GET /v1/blueprints/:id/head`. Every other
    /// route 404s, which the per-agent render-size lookup tolerates
    /// (`last_rendered_bytes: null`).
    async fn spawn_head_stub(bp: Blueprint) -> String {
        use axum::routing::get;
        use axum::{Json, Router};

        let head = serde_json::json!({ "blueprint": bp });
        let router = Router::new().route(
            "/v1/blueprints/:id/head",
            get(move || {
                let head = head.clone();
                async move { Json(head) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        addr.to_string()
    }

    /// Run the tool against `bp` with the given call-site `lints`. Every
    /// family except `agent-md-size` is disabled so the assertions read
    /// the size family's resolution alone.
    async fn doctor(bp: Blueprint, lints: Option<BTreeMap<String, String>>) -> JsonValue {
        let bind = spawn_head_stub(bp).await;
        let result = MseServer::new()
            .bp_doctor(Parameters(BpDoctorReq {
                id: "lint-control-fixture".into(),
                bind: Some(bind),
                warn_bytes: None,
                warn_lines: None,
                block_bytes: None,
                block_lines: None,
                disable_block: None,
                lints,
                disable_tool_lint: Some(true),
                disable_output_contract_lint: Some(true),
                disable_worker_binding_lint: Some(true),
                disable_binding_lint: Some(true),
                disable_skip_on_lint: Some(true),
                disable_context_policy_lint: Some(true),
                disable_verdict_contract_lint: Some(true),
                disable_spawner_hint_lint: Some(true),
                simulated_launch: None,
            }))
            .await
            .expect("bp_doctor");
        serde_json::from_str(&text_payload(&result)).expect("bp_doctor json")
    }

    fn diagnostic_kinds(json: &JsonValue) -> Vec<String> {
        json["diagnostics"]
            .as_array()
            .expect("diagnostics array")
            .iter()
            .map(|d| d["kind"].as_str().expect("kind string").to_string())
            .collect()
    }

    /// Baseline + the call-site layer: an `allow` moves the size WARN out
    /// of `diagnostics[]` into `suppressed[]`, improves the verdict, and
    /// leaves the agent entry with its real measurements under a resolved
    /// `severity: "OK"`.
    #[tokio::test]
    async fn call_site_allow_moves_the_size_warn_into_suppressed() {
        let baseline = doctor(oversized_bp(&["planner"]), None).await;
        assert_eq!(baseline["verdict"], "WARN", "body: {baseline}");
        assert_eq!(baseline["agents"][0]["severity"], "WARN");
        assert!(
            baseline["suppressed"]
                .as_array()
                .expect("suppressed is always present")
                .is_empty(),
            "body: {baseline}"
        );

        let json = doctor(
            oversized_bp(&["planner"]),
            call_site_lints(&[("agent-md-size", "allow")]),
        )
        .await;
        assert_eq!(json["verdict"], "OK", "body: {json}");
        assert_eq!(json["agents"][0]["severity"], "OK");
        assert_eq!(json["agents"][0]["lines"], OVERSIZED_LINES);
        assert_eq!(json["over_threshold_count"], 0);
        assert!(diagnostic_kinds(&json).is_empty(), "body: {json}");

        let suppressed = json["suppressed"].as_array().expect("suppressed array");
        assert_eq!(suppressed.len(), 1, "body: {json}");
        assert_eq!(suppressed[0]["kind"], "agent-md-size");
        assert_eq!(suppressed[0]["source"], "call-site");
        assert_eq!(suppressed[0]["span"]["element"]["name"], "planner");
        assert!(suppressed[0]["message"]
            .as_str()
            .expect("message string")
            .contains("planner"));
    }

    /// The per-agent layer is scoped: it silences its own agent's finding
    /// and nothing else, so the second over-threshold agent still WARNs.
    #[tokio::test]
    async fn agent_layer_allow_suppresses_only_that_agents_finding() {
        let mut bp = oversized_bp(&["quiet", "loud"]);
        bp.agents[0].lints = Some(lint_map(&[("agent-md-size", "allow")]));

        let json = doctor(bp, None).await;
        assert_eq!(json["verdict"], "WARN", "body: {json}");
        assert_eq!(json["agents"][0]["severity"], "OK");
        assert_eq!(json["agents"][1]["severity"], "WARN");
        assert_eq!(diagnostic_kinds(&json), vec!["agent-md-size"]);

        let suppressed = json["suppressed"].as_array().expect("suppressed array");
        assert_eq!(suppressed.len(), 1, "body: {json}");
        assert_eq!(suppressed[0]["source"], "agent:quiet");
        assert_eq!(suppressed[0]["span"]["element"]["name"], "quiet");
    }

    /// The Blueprint layer: declared once in `metadata.lints`, no caller
    /// flag needed on any later invocation.
    #[tokio::test]
    async fn blueprint_layer_allow_suppresses_without_a_call_site_flag() {
        let mut bp = oversized_bp(&["planner"]);
        bp.metadata.lints = Some(lint_map(&[("agent-md-size", "allow")]));

        let json = doctor(bp, None).await;
        assert_eq!(json["verdict"], "OK", "body: {json}");
        let suppressed = json["suppressed"].as_array().expect("suppressed array");
        assert_eq!(suppressed.len(), 1, "body: {json}");
        assert_eq!(suppressed[0]["source"], "blueprint");
    }

    /// Precedence: the first layer with any matching key wins outright.
    /// A call-site `warn` overrides the agent's `allow` — the finding
    /// fires and nothing is suppressed.
    #[tokio::test]
    async fn call_site_warn_beats_an_agent_layer_allow() {
        let mut bp = oversized_bp(&["planner"]);
        bp.agents[0].lints = Some(lint_map(&[("agent-md-size", "allow")]));

        let json = doctor(bp, call_site_lints(&[("agent-md-size", "warn")])).await;
        assert_eq!(json["verdict"], "WARN", "body: {json}");
        assert_eq!(json["agents"][0]["severity"], "WARN");
        assert_eq!(diagnostic_kinds(&json), vec!["agent-md-size"]);
        assert!(
            json["suppressed"]
                .as_array()
                .expect("suppressed array")
                .is_empty(),
            "body: {json}"
        );
    }

    /// `deny` escalates a WARN-only family to a BLOCK verdict and to
    /// `DiagLevel::Error` in `diagnostics[]`. The verdict stays a report
    /// label — this tool blocks nothing either way.
    #[tokio::test]
    async fn deny_escalates_the_size_warn_to_a_block_verdict() {
        let json = doctor(
            oversized_bp(&["planner"]),
            call_site_lints(&[("agent-md-size", "deny")]),
        )
        .await;
        assert_eq!(json["verdict"], "BLOCK", "body: {json}");
        assert_eq!(json["agents"][0]["severity"], "BLOCK");
        assert_eq!(json["diagnostics"][0]["kind"], "agent-md-size");
        assert_eq!(json["diagnostics"][0]["level"], "Error");
        assert!(
            json["suppressed"]
                .as_array()
                .expect("suppressed array")
                .is_empty(),
            "body: {json}"
        );
    }

    /// A key typo (and an unparseable value) degrade to the
    /// `unknown-lint-kind` meta-lint: the request is answered, the
    /// targeted finding is untouched, and the note names the layer.
    #[tokio::test]
    async fn unknown_key_becomes_a_meta_lint_and_never_rejects_the_request() {
        let json = doctor(
            oversized_bp(&["planner"]),
            call_site_lints(&[("agent-md-sizes", "allow"), ("agent-md-size", "allowe")]),
        )
        .await;
        // Nothing was honored: the size WARN still stands.
        assert_eq!(json["verdict"], "WARN", "body: {json}");
        assert_eq!(json["agents"][0]["severity"], "WARN");
        assert!(
            json["suppressed"]
                .as_array()
                .expect("suppressed array")
                .is_empty(),
            "body: {json}"
        );

        let metas: Vec<&JsonValue> = json["diagnostics"]
            .as_array()
            .expect("diagnostics array")
            .iter()
            .filter(|d| d["kind"] == "unknown-lint-kind")
            .collect();
        // One per unhonored entry: the unknown key, and the known key
        // whose value did not parse.
        assert_eq!(metas.len(), 2, "body: {json}");
        for meta in &metas {
            assert_eq!(meta["level"], "Warn");
            assert_eq!(meta["notes"][0], "declared by: call-site");
        }
        assert!(metas.iter().any(|m| m["message"]
            .as_str()
            .unwrap_or_default()
            .contains("'agent-md-sizes'")));
    }

    /// The stage-scoped non-suppressible boundary: an exact-kind `allow`
    /// on a compile hard error `bp_doctor` never emits cannot have any
    /// effect, and says so. A blanket `all` key never raises it.
    #[tokio::test]
    async fn exact_kind_allow_on_a_compile_only_kind_raises_non_suppressible_lint() {
        let json = doctor(
            oversized_bp(&["planner"]),
            call_site_lints(&[("duplicate-agent-name", "allow")]),
        )
        .await;
        let metas: Vec<&JsonValue> = json["diagnostics"]
            .as_array()
            .expect("diagnostics array")
            .iter()
            .filter(|d| d["kind"] == "non-suppressible-lint")
            .collect();
        assert_eq!(metas.len(), 1, "body: {json}");
        assert_eq!(metas[0]["level"], "Warn");
        assert_eq!(metas[0]["notes"][0], "declared by: call-site");
        assert!(metas[0]["message"]
            .as_str()
            .expect("message string")
            .contains("'duplicate-agent-name'"));
        // The setting is inert, so the size family is untouched.
        assert_eq!(json["agents"][0]["severity"], "WARN");

        // `all` addresses whole sets — covering kinds this stage never
        // emits is expected there, so no meta-lint noise.
        let blanket = doctor(
            oversized_bp(&["planner"]),
            call_site_lints(&[("all", "allow")]),
        )
        .await;
        assert!(
            !diagnostic_kinds(&blanket).contains(&"non-suppressible-lint".to_string()),
            "body: {blanket}"
        );
        assert_eq!(blanket["verdict"], "OK", "body: {blanket}");
        assert_eq!(
            blanket["suppressed"]
                .as_array()
                .expect("suppressed array")
                .len(),
            1,
            "body: {blanket}"
        );
    }
}
