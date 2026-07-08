//! mse mcp: MCP server (stdio) for mlua-swarm-engine.
//!
//! Sibling of `mse serve` (HTTP). External AI agents (Claude Code / other MCP clients)
//! call the `swarm.run` / `swarm.status` / `swarm.cancel` tools via stdio JSON-RPC.
//!
//! v2 wiring: `swarm.run` is wired to `TaskApplication.handle` (= the same entry
//! point as `mse serve`'s `/v1/tasks`). Engine boot reuses `default_registry` from
//! the mse serve lib (= the baseline `identity` RustFn is pre-baked, the shared SoT
//! across the three sibling binaries).

mod operator_client;
mod resources;
mod server_control;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use mlua_swarm::application::{BlueprintRef, TaskApplication, TaskApplicationInput};
use mlua_swarm::blueprint::store::{BlueprintStore, InMemoryBlueprintStore};
use mlua_swarm::blueprint::Blueprint;
use mlua_swarm::store::run::{
    InMemoryRunStore, RunContext, RunRecord, RunStatus as StoreRunStatus, RunStore,
};
use mlua_swarm::types::{RunId, StepId, TaskId};
use mlua_swarm::{Compiler, Engine, EngineCfg, OperatorKind, Role, TaskLaunchService};
use operator_client::{ClientError, OperatorClientState};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    AnnotateAble, CallToolResult, Content, ListResourcesResult, PaginatedRequestParams,
    RawResource, ReadResourceRequestParams, ReadResourceResult, ResourceContents,
    ServerCapabilities, ServerInfo,
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
        Self {
            state: Arc::new(RwLock::new(Inner {
                runs: HashMap::new(),
                task_app,
                store,
                run_store,
            })),
            op_client: Arc::new(OperatorClientState::new()),
        }
    }
}

/// Unix epoch seconds (same convention as the store records' timestamps).
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Maps `operator_client::ClientError` to an `McpError` for tool responses.
/// `UnknownSid` / `InvalidAckKind` are caller-input mistakes (`invalid_params`);
/// `Http` / `Ws` are transport-layer failures (`internal_error`).
fn client_error_to_mcp(e: ClientError) -> McpError {
    match e {
        ClientError::UnknownSid(_) | ClientError::InvalidAckKind(_) => {
            McpError::invalid_params(e.to_string(), None)
        }
        ClientError::Http(_) | ClientError::Ws(_) => McpError::internal_error(e.to_string(), None),
    }
}

#[derive(Deserialize, JsonSchema)]
struct DoctorReq {
    #[serde(default)]
    bind: Option<String>,
}

fn default_true_bool() -> bool {
    true
}

#[derive(Deserialize, JsonSchema)]
struct BpArchiveReq {
    /// Blueprint id to archive (logical soft-delete via marker commit; reversible).
    id: String,
    /// mse serve bind address (default 127.0.0.1:7777).
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
    /// mse serve bind address (default 127.0.0.1:7777).
    #[serde(default)]
    bind: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct ServerStartReq {
    /// listen address to healthz-poll after `launchctl kickstart` (default "127.0.0.1:7777").
    /// Server-side settings (store root / enhance flow / etc.) come from
    /// `~/.mse/config.toml`, not from this tool call — see `mlua_swarm_server_restart` doc.
    #[serde(default)]
    bind: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct ServerStatusReq {
    #[serde(default)]
    bind: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct ServerShutdownReq {
    #[serde(default)]
    bind: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct ServerRestartReq {
    #[serde(default)]
    bind: Option<String>,
}

// ---- S3 operator client tool param schemas ----
// (see the WS multi-session design for the MCP tool set).

#[derive(Deserialize, JsonSchema)]
struct OperatorJoinReq {
    /// Logical operator role alias(es), e.g. `["main-ai"]`. Checked for
    /// exclusivity server-side (`POST /v1/operators` returns 409 on
    /// conflict). Omitted/empty = no alias claimed.
    #[serde(default)]
    roles: Option<Vec<String>>,
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
}

#[derive(Deserialize, JsonSchema)]
struct OperatorLeaveReq {
    /// sid returned by `mse_operator_join`.
    sid: String,
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
    /// `true` = normal success.
    #[serde(default)]
    ok: Option<bool>,
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
    init_ctx: Option<JsonValue>,
    /// Timeout in seconds. Default 300 (= 5 min).
    #[serde(default)]
    timeout_secs: Option<u64>,
    /// Operator id label. Default "mcp-run".
    #[serde(default)]
    operator_id: Option<String>,
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
        /// mse serve bind address (default `127.0.0.1:7777`).
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
}

#[derive(Deserialize, JsonSchema)]
struct SwarmCancelReq {
    run_id: String,
}

// ---- tools ----

#[tool_router]
impl MseServer {
    #[tool(
        description = "Join as an Operator session: POST /v1/operators (mint sid+token) then connect WS /v1/operators/:sid/ws with the returned Bearer token. The token stays process-local (never returned to the caller). Returns {sid, roles}. Use `sid` with mse_pending_wait / mse_ack / mse_operator_leave."
    )]
    async fn mse_operator_join(
        &self,
        Parameters(req): Parameters<OperatorJoinReq>,
    ) -> Result<CallToolResult, McpError> {
        let roles = req.roles.unwrap_or_default();
        let (sid, roles) = self
            .op_client
            .join(roles)
            .await
            .map_err(client_error_to_mcp)?;
        json_result(&serde_json::json!({ "sid": sid, "roles": roles }))
    }

    #[tool(
        description = "Pop one pending server frame (ask / hook_before / hook_after / spawn) for `sid`, waiting up to `timeout_ms` (default 30000) if the queue is empty. Returns {timed_out, req_id?, type?, payload?} on delivery — `type` mirrors the server's ServerMsg discriminant, `payload` carries the remaining frame fields verbatim. Returns {timed_out: true} on timeout. Reply via mse_ack with a matching `kind`."
    )]
    async fn mse_pending_wait(
        &self,
        Parameters(req): Parameters<OperatorPendingWaitReq>,
    ) -> Result<CallToolResult, McpError> {
        let timeout_ms = req.timeout_ms.unwrap_or(30_000);
        let frame = self
            .op_client
            .pending_wait(&req.sid, timeout_ms)
            .await
            .map_err(client_error_to_mcp)?;
        match frame {
            Some(f) => json_result(&serde_json::json!({
                "timed_out": false,
                "req_id": f.req_id,
                "type": f.kind,
                "payload": f.payload,
            })),
            None => json_result(&serde_json::json!({ "timed_out": true })),
        }
    }

    #[tool(
        description = "Ack a pending frame popped via mse_pending_wait. kind=\"answer\" (SeniorBridge.ask reply, pass `value`), kind=\"hook_ack\" (SpawnHook.before OK/NG, pass `ok` + optional `error` as the rejection reason), kind=\"spawn_ack\" (Operator.execute result, pass `value` + `ok` + optional `error`), kind=\"spawn_halt\" (issue #7: controlled halt for the current spawn — pass optional `value` (partial ctx) + optional `error` (halt reason); the step lands as WorkerResult{ok:true, value:{halted:true, reason, value}} — a normal termination, not a worker error). Sends the corresponding ClientMsg over the sid's WS connection. Returns {sent: true}."
    )]
    async fn mse_ack(
        &self,
        Parameters(req): Parameters<OperatorAckReq>,
    ) -> Result<CallToolResult, McpError> {
        self.op_client
            .ack(
                &req.sid, req.req_id, &req.kind, req.value, req.ok, req.error,
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
        description = "Worker-side fetch: GET <base_url>/v1/worker/prompt?task_id=<task_id> with `Authorization: Bearer <worker_handle>`. Normally the `worker_handle` (`wh-` short handle from the Spawn frame) is the ONLY required param — base_url and task_id auto-resolve from the route this process recorded when the Spawn frame passed through mse_pending_wait; pass them explicitly to override (or when the Bearer is a full capability_token). Returns the server's WorkerPayload JSON verbatim ({task_id, attempt, agent, prompt, system?, context?} — `context` is the AgentContextView task-level context: project_root / work_dir / task_metadata / run_id / project_name_alias, GH #20 Contract C). Pure-MCP replacement for the wrapper agents' Bash curl step — no shell involved."
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
        let client = reqwest::Client::builder()
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
        json_result(&payload)
    }

    #[tool(
        description = "Worker-side submit: POST <base_url>/v1/worker/submit with `Authorization: Bearer <worker_handle>` and the raw `body` as text/plain (task_id is resolved server-side from the Bearer). Normally `worker_handle` + `body` are the ONLY required params — base_url auto-resolves from the route this process recorded when the Spawn frame passed through mse_pending_wait; pass it explicitly to override (or when the Bearer is a full capability_token). Optional ok=false marks the attempt failed (flow.ir Try catch path). Expects HTTP 204 and returns {submitted: true}; any other status is an error. Pure-MCP replacement for the wrapper agents' Bash curl step — no shell involved."
    )]
    async fn mse_worker_submit(
        &self,
        Parameters(req): Parameters<WorkerSubmitReq>,
    ) -> Result<CallToolResult, McpError> {
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
        let base = base_url.trim_end_matches('/');
        let url = format!("{base}/v1/worker/submit");
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| McpError::internal_error(format!("client build: {e}"), None))?;
        let mut request = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", req.worker_handle))
            .header("Content-Type", "text/plain");
        if req.ok == Some(false) {
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
        description = "Run a Blueprint to completion via TaskApplication.handle. Blocking. Returns run_id + final_ctx + bound_version. `blueprint` accepts a BlueprintSelector `{kind: \"inline\"|\"id\"|\"file\", ...}` or, for backward compat, a bare Blueprint object (treated as inline)."
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

        // Normalize BlueprintInput → BlueprintSelector.
        let selector = match req.blueprint {
            BlueprintInput::Selector(s) => s,
            BlueprintInput::BareInline(v) => BlueprintSelector::Inline { blueprint: v },
        };

        // Id kind: proxy POST /v1/tasks. Uses the server-side store; the
        // in-process store dedicated to Inline is not consulted.
        if let BlueprintSelector::Id { id, bind } = &selector {
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
                )
                .await;
        }

        let (task_app, run_store) = {
            let mut inner = self.state.write().await;
            inner.runs.insert(
                run_id.clone(),
                RunHandle {
                    run_id: run_id.clone(),
                    status: RunStatus::Running,
                },
            );
            (inner.task_app.clone(), inner.run_store.clone())
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
            operator_backend_id: None,
            operator_kind_overrides,
            task_input: None,
        };

        // Trace this kick in the local run store (in-memory; issue #13).
        // The stdio adapter has no TaskStore, so the work-item id is minted
        // ad hoc — it groups re-runs only within this process's lifetime.
        let task_id_typed = TaskId::new();
        let now = now_secs();
        let run_ctx = match run_store
            .create(RunRecord {
                id: run_id_typed.clone(),
                task_id: task_id_typed.clone(),
                status: StoreRunStatus::Running,
                step_entries: Vec::new(),
                operator_sid: None,
                result_ref: None,
                created_at: now,
                updated_at: now,
            })
            .await
        {
            Ok(()) => Some(RunContext {
                run_id: run_id_typed.clone(),
                run_store: run_store.clone(),
            }),
            // A trace-store failure must not block the run itself.
            Err(_) => None,
        };

        let exec = task_app.handle_with_run(input, run_ctx);
        let result = tokio::time::timeout(ttl, exec).await;

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
        // log_tail: the task axis has no log store (that is exclusive to
        // the enhance axis); this will be filled in when the enhance path
        // integrates. For now, always an empty array.
        let log_tail: Vec<JsonValue> = Vec::new();

        let (status, body) = match result {
            Ok(Ok(out)) => (
                RunStatus::Done,
                serde_json::json!({
                    "run_id": run_id,
                    "task_id": task_id_typed,
                    "status": "done",
                    "final_ctx": out.final_ctx,
                    "bound_version": out.bound_version.map(|v| format!("{:?}", v)),
                    "head": head_id,
                    "history_len": history_len,
                    "log_tail": log_tail,
                }),
            ),
            Ok(Err(e)) => (
                RunStatus::Failed,
                serde_json::json!({
                    "run_id": run_id,
                    "task_id": task_id_typed,
                    "status": "failed",
                    "error": e.to_string(),
                    "head": head_id,
                    "history_len": history_len,
                    "log_tail": log_tail,
                }),
            ),
            Err(_) => (
                RunStatus::Failed,
                serde_json::json!({
                    "run_id": run_id,
                    "task_id": task_id_typed,
                    "status": "failed",
                    "error": format!("timeout after {}s", ttl.as_secs()),
                    "head": head_id,
                    "history_len": history_len,
                    "log_tail": log_tail,
                }),
            ),
        };

        // Finalize the local run trace (best effort; the wire response is
        // authoritative for the caller).
        let store_status = match status {
            RunStatus::Done => StoreRunStatus::Done,
            _ => StoreRunStatus::Failed,
        };
        let _ = run_store.update_status(&run_id_typed, store_status).await;
        if matches!(status, RunStatus::Done) {
            if let Some(fc) = body.get("final_ctx") {
                let _ = run_store.set_result(&run_id_typed, fc.clone()).await;
            }
        }

        {
            let mut inner = self.state.write().await;
            if let Some(h) = inner.runs.get_mut(&run_id) {
                h.status = status;
            }
        }
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
    ) -> Result<CallToolResult, McpError> {
        {
            let mut inner = self.state.write().await;
            inner.runs.insert(
                run_id.clone(),
                RunHandle {
                    run_id: run_id.clone(),
                    status: RunStatus::Running,
                },
            );
        }

        let bind = bind.unwrap_or_else(|| server_control::DEFAULT_BIND.to_string());
        let url = format!("http://{bind}/v1/tasks");

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
        payload.insert("ttl_secs".into(), JsonValue::from(ttl.as_secs()));
        if !operator_obj.is_empty() {
            payload.insert("operator".into(), JsonValue::Object(operator_obj));
        }

        let client = match reqwest::Client::builder()
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
        let (final_status, body) = if http_status.is_success() {
            let parsed: JsonValue =
                serde_json::from_str(&text).unwrap_or_else(|_| JsonValue::String(text.clone()));
            if let Some(sid) = parsed.get("run_id").and_then(|v| v.as_str()) {
                effective_run_id = sid.to_string();
            }
            (
                RunStatus::Done,
                serde_json::json!({
                    "run_id": effective_run_id.clone(),
                    "task_id": parsed.get("task_id").cloned().unwrap_or(JsonValue::Null),
                    "status": "done",
                    "final_ctx": parsed.get("final_ctx").cloned().unwrap_or(JsonValue::Null),
                    "bound_version": parsed.get("bound_version").cloned().unwrap_or(JsonValue::Null),
                    "effective_ttl_secs": parsed.get("effective_ttl_secs").cloned().unwrap_or(JsonValue::Null),
                    "ttl_source": parsed.get("ttl_source").cloned().unwrap_or(JsonValue::Null),
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
                    },
                );
            } else if let Some(h) = inner.runs.get_mut(&run_id) {
                h.status = final_status;
            }
        }
        json_result(&body)
    }

    #[tool(
        description = "Peek at a known run by run_id. Returns status snapshot; for in-process runs the per-step trace (step_entries) is included."
    )]
    async fn swarm_status(
        &self,
        Parameters(req): Parameters<SwarmStatusReq>,
    ) -> Result<CallToolResult, McpError> {
        let (handle, run_store) = {
            let inner = self.state.read().await;
            (
                inner.runs.get(&req.run_id).cloned(),
                inner.run_store.clone(),
            )
        };
        match handle {
            Some(h) => {
                let mut body = serde_json::json!({
                    "run_id": h.run_id,
                    "status": h.status,
                });
                // In-process runs carry a local step trace (issue #13);
                // HTTP-proxied runs live on the server — drill down there
                // via GET /v1/runs/:id instead. Both lookups are best-effort
                // enrichment, so a non-`R-` run_id simply skips the trace.
                if let Ok(rid) = RunId::parse(req.run_id.clone()) {
                    if let Ok(rec) = run_store.get(&rid).await {
                        body["task_id"] = serde_json::json!(rec.task_id);
                        body["step_entries"] =
                            serde_json::to_value(&rec.step_entries).unwrap_or(JsonValue::Null);
                    }
                }
                json_result(&body)
            }
            None => Err(McpError::invalid_params(
                format!("run_id not found: {}", req.run_id),
                None,
            )),
        }
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
            .unwrap_or_else(|| server_control::DEFAULT_BIND.to_string());
        if !req.confirm {
            return json_result(&serde_json::json!({
                "status": "dry_run",
                "id": req.id,
                "bind": bind,
                "note": "Pass confirm=true to archive. Reversible via bp_unarchive (marker commit; audit-trail preserved).",
            }));
        }
        let url = format!("http://{bind}/v1/blueprints/{}", req.id);
        let client = reqwest::Client::builder()
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
        description = "Unarchive a Blueprint — reverse of bp_archive. Appends `archive: false` marker commit to head, re-exposing the id to list_ids / read_head / swarm_run. Wraps POST /v1/blueprints/:id/unarchive."
    )]
    async fn bp_unarchive(
        &self,
        Parameters(req): Parameters<BpUnarchiveReq>,
    ) -> Result<CallToolResult, McpError> {
        let bind = req
            .bind
            .unwrap_or_else(|| server_control::DEFAULT_BIND.to_string());
        let url = format!("http://{bind}/v1/blueprints/{}/unarchive", req.id);
        let client = reqwest::Client::builder()
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
        description = "Doctor snapshot: mse mcp self state (in-process store = InMemory ephemeral) + server-side config (backend / store root / ref_base / registered BP list) fetched from GET /v1/doctor. Answers 'where is the store?' and 'how many BPs are registered?' in a single call."
    )]
    async fn mse_doctor(
        &self,
        Parameters(req): Parameters<DoctorReq>,
    ) -> Result<CallToolResult, McpError> {
        let bind = req
            .bind
            .unwrap_or_else(|| server_control::DEFAULT_BIND.to_string());
        let server_status = server_control::status(&bind).await;
        let server_up = server_status.up;

        let server_info: JsonValue = if server_up {
            let url = format!("http://{bind}/v1/doctor");
            match reqwest::Client::builder()
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
            serde_json::json!({"note": "mse serve down; start via mlua_swarm_server_start"})
        };

        let run_count = self.state.read().await.runs.len();

        let body = serde_json::json!({
            "mse_mcp": {
                "in_process_blueprint_store": "InMemory (ephemeral, mse mcp process-local)",
                "in_flight_run_count": run_count,
                "note": "The mse mcp in-process store is dedicated to swarm_run(Inline). The register path uses a separate store on the HTTP server side (POST /v1/blueprints/:id).",
            },
            "mlua_swarm_server": {
                "bind": bind,
                "up": server_up,
                "launchd_state": server_status.launchd_state,
                "launchd_pid": server_status.launchd_pid,
                "doctor": server_info,
            },
        });
        json_result(&body)
    }

    #[tool(
        description = "Start mse serve via `launchctl kickstart gui/<uid>/com.mse.server`, then healthz-polls up to 30s. No-op if healthz is already up. Server settings come from ~/.mse/config.toml, not this call. Returns {status: already_running|started, bind}. Errors with install instructions if the launchd job is not bootstrapped yet."
    )]
    async fn mlua_swarm_server_start(
        &self,
        Parameters(req): Parameters<ServerStartReq>,
    ) -> Result<CallToolResult, McpError> {
        let bind = req
            .bind
            .unwrap_or_else(|| server_control::DEFAULT_BIND.to_string());
        match server_control::start(&bind).await {
            Ok(outcome) => json_result(&outcome),
            Err(e) => Err(McpError::internal_error(e, None)),
        }
    }

    #[tool(
        description = "Report mse serve state: healthz + a `launchctl print gui/<uid>/com.mse.server` summary (state / pid / last exit code). Returns {bind, up, launchd_state, launchd_pid, launchd_last_exit_code}."
    )]
    async fn mlua_swarm_server_status(
        &self,
        Parameters(req): Parameters<ServerStatusReq>,
    ) -> Result<CallToolResult, McpError> {
        let bind = req
            .bind
            .unwrap_or_else(|| server_control::DEFAULT_BIND.to_string());
        let out = server_control::status(&bind).await;
        json_result(&out)
    }

    #[tool(
        description = "Fully stop mse serve via `launchctl bootout gui/<uid>/com.mse.server` (unloads the job; KeepAlive will not restart it until the next `mlua_swarm_server_start` / `mlua_swarm_server_restart`). Returns {bind, stopped}."
    )]
    async fn mlua_swarm_server_shutdown(
        &self,
        Parameters(req): Parameters<ServerShutdownReq>,
    ) -> Result<CallToolResult, McpError> {
        let bind = req
            .bind
            .unwrap_or_else(|| server_control::DEFAULT_BIND.to_string());
        match server_control::shutdown(&bind).await {
            Ok(out) => json_result(&out),
            Err(e) => Err(McpError::internal_error(e, None)),
        }
    }

    #[tool(
        description = "Kill + restart mse serve via `launchctl kickstart -k gui/<uid>/com.mse.server`, then healthz-polls up to 30s. Use after editing ~/.mse/config.toml to pick up the new settings. Returns {status: started, bind}."
    )]
    async fn mlua_swarm_server_restart(
        &self,
        Parameters(req): Parameters<ServerRestartReq>,
    ) -> Result<CallToolResult, McpError> {
        let bind = req
            .bind
            .unwrap_or_else(|| server_control::DEFAULT_BIND.to_string());
        match server_control::restart(&bind).await {
            Ok(outcome) => json_result(&outcome),
            Err(e) => Err(McpError::internal_error(e, None)),
        }
    }

    #[tool(
        description = "Mark a run as cancelled in the local registry. Note: in-flight handle abort is v3 carry."
    )]
    async fn swarm_cancel(
        &self,
        Parameters(req): Parameters<SwarmCancelReq>,
    ) -> Result<CallToolResult, McpError> {
        let mut inner = self.state.write().await;
        match inner.runs.get_mut(&req.run_id) {
            Some(h) => {
                h.status = RunStatus::Cancelled;
                json_result(&serde_json::json!({ "ok": true, "run_id": req.run_id }))
            }
            None => Err(McpError::invalid_params(
                format!("run_id not found: {}", req.run_id),
                None,
            )),
        }
    }
}

#[tool_handler]
impl ServerHandler for MseServer {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
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

fn json_result<T: Serialize>(value: &T) -> Result<CallToolResult, McpError> {
    let text = serde_json::to_string_pretty(value)
        .map_err(|e| McpError::internal_error(e.to_string(), None))?;
    Ok(CallToolResult::success(vec![Content::text(text)]))
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
    use mlua_flow_ir::{Expr, Node as FlowNode};
    use mlua_swarm::blueprint::{
        current_schema_version, AgentDef, AgentKind, AgentMeta, BlueprintMetadata, CompilerHints,
        CompilerStrategy,
    };

    fn identity_blueprint() -> Blueprint {
        use mlua_swarm::worker::baseline::AG_IDENTITY;
        Blueprint {
            schema_version: current_schema_version(),
            id: "mse mcp-l2-identity".into(),
            flow: FlowNode::Step {
                ref_: AG_IDENTITY.into(),
                in_: Expr::Path { at: "$.in".into() },
                out: Expr::Path { at: "$.out".into() },
            },
            agents: vec![AgentDef {
                name: AG_IDENTITY.into(),
                kind: AgentKind::RustFn,
                spec: serde_json::json!({"fn_id": AG_IDENTITY}),
                profile: None,
                meta: Some(AgentMeta::default()),
            }],
            operators: vec![],
            hints: CompilerHints::default(),
            strategy: CompilerStrategy::default(),
            metadata: BlueprintMetadata {
                description: Some("mse mcp L2 fixture".into()),
                origin: Default::default(),
                tags: vec![],
                version_label: Some("0.1.0".into()),
                project_name_alias: None,
                default_run_ttl_secs: None,
            },
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
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
            operator_kind: None,
            operator_kind_overrides: None,
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
            }))
            .await
            .unwrap_err();
        let _ = format!("{:?}", err);
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
            operator_kind: None,
            operator_kind_overrides: None,
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
            operator_kind: None,
            operator_kind_overrides: None,
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
            operator_kind: None,
            operator_kind_overrides: None,
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
            operator_kind: None,
            operator_kind_overrides: None,
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
            operator_kind: None,
            operator_kind_overrides: None,
        };
        let result = server.swarm_run(Parameters(req)).await.expect("swarm_run");
        let _ = std::fs::remove_file(&name);
        let text = extract_text_payload(&result);
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("json");
        assert_eq!(parsed["status"], "done", "payload: {text}");
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
            }))
            .await
            .expect_err("unknown handle must surface the HTTP error");
        let msg = format!("{err:?}");
        assert!(msg.contains("expected 204"), "err: {msg}");
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
                operator_kind: None,
                operator_kind_overrides: None,
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
            }))
            .await
            .unwrap();
        let inner = server.state.read().await;
        assert!(matches!(
            inner.runs.get(&run_id).unwrap().status,
            RunStatus::Cancelled
        ));
    }
}
