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
    /// The Run's owning Task, when known. `None` briefly for an
    /// HTTP-proxied (`Id` selector) dispatch before the server's response
    /// is parsed. Populated for in-process (Inline/File) dispatch from the
    /// start (issue GH #34 — `mse_doctor`'s `audit_findings` scan needs
    /// `task_id` to address `GET /v1/tasks/:id/runs/:run/steps`).
    task_id: Option<String>,
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
/// (GH #34, ST1). A step whose name does not carry that prefix (the
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
    #[serde(default)]
    bind: Option<String>,
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
struct BpDoctorReq {
    /// Blueprint id to inspect (agent `profile.system_prompt` bodies are what
    /// the SubAgent receives via fetch — this tool measures those directly).
    id: String,
    /// mse serve bind address (default 127.0.0.1:7777).
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
    /// Skip the occupancy check (in-flight runs / attached operators) and
    /// kill unconditionally. Default `false` — a busy server refuses.
    #[serde(default)]
    force: Option<bool>,
}

#[derive(Deserialize, JsonSchema)]
struct ServerRestartReq {
    #[serde(default)]
    bind: Option<String>,
    /// Skip the occupancy check (in-flight runs / attached operators) and
    /// kill unconditionally. Default `false` — a busy server refuses.
    #[serde(default)]
    force: Option<bool>,
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
    /// GH #36 ST2: when given, this call **stages one named output part**
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
    /// flow eval continues in the background bounded by `timeout_secs`
    /// (in-process) / the server run TTL (id proxy); poll `swarm_status`
    /// for the terminal status and result.
    #[serde(default)]
    detach: Option<bool>,
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

    #[tool(
        description = "Worker-side submit: POST <base_url>/v1/worker/submit with `Authorization: Bearer <worker_handle>` and the raw `body` as text/plain (task_id is resolved server-side from the Bearer). Normally `worker_handle` + `body` are the ONLY required params — base_url auto-resolves from the route this process recorded when the Spawn frame passed through mse_pending_wait; pass it explicitly to override (or when the Bearer is a full capability_token). Optional ok=false marks the attempt failed (flow.ir Try catch path); mutually exclusive with `name`. Optional `name` (GH #36 ST2) stages ONE named output part instead of completing the attempt — POST /v1/worker/artifact?name=<name> — call again (same or different name) for more parts, then finish with a plain (no-name) call; the step's final output becomes {\"out\": <final submit body>, \"parts\": {<name>: <value>, ...}}, read downstream via bracket notation e.g. \"$.<step>.parts[\\\"plan.md\\\"]\". Expects HTTP 204 and returns {submitted: true} (name path) or {submitted: true} (plain path); any other status is an error. Pure-MCP replacement for the wrapper agents' Bash curl step — no shell involved."
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
        let url = worker_submit_endpoint_url(&base_url, req.name.as_deref())
            .map_err(|e| McpError::invalid_params(format!("invalid base_url: {e}"), None))?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| McpError::internal_error(format!("client build: {e}"), None))?;
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
        description = "Run a Blueprint via TaskApplication.handle. Blocking by default (returns run_id + final_ctx + bound_version on completion); pass `detach: true` for the asynchronous launch — returns `{run_id, task_id, status: \"running\"}` immediately, poll `swarm_status` for the result. `blueprint` accepts a BlueprintSelector `{kind: \"inline\"|\"id\"|\"file\", ...}` or, for backward compat, a bare Blueprint object (treated as inline)."
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
                )
                .await;
        }

        // Minted here (rather than just before `run_store.create` below) so
        // the initial `RunHandle` insert already carries it — `mse_doctor`'s
        // `audit_findings` scan (GH #34) addresses the steps API by
        // `task_id`, and in-process runs are its only source until the
        // dispatch below finishes.
        let task_id_typed = TaskId::new();

        let (task_app, run_store) = {
            let mut inner = self.state.write().await;
            inner.runs.insert(
                run_id.clone(),
                RunHandle {
                    run_id: run_id.clone(),
                    status: RunStatus::Running,
                    task_id: Some(task_id_typed.to_string()),
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
        // ad hoc (above) — it groups re-runs only within this process's
        // lifetime.
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
            tokio::spawn(async move {
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
            });
            return json_result(&serde_json::json!({
                "run_id": run_id,
                "task_id": task_id_typed,
                "status": "running",
                "detached": true,
            }));
        }

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
        detach: bool,
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
                        task_id: effective_task_id,
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
        description = "Per-Blueprint agent.md size check. Fetches the Blueprint head from GET /v1/blueprints/:id/head and inspects every agent's profile.system_prompt (= the body that will be pushed to the SubAgent context via fetch). Reports per-agent bytes / lines / severity (OK|WARN|BLOCK) plus an aggregate verdict. The verdict is a report label only — this tool never blocks any dispatch. Default thresholds (`mse://guides/agent-md-authoring §Size targets`): WARN at ≥ 25 KB or ≥ 200 lines, BLOCK at ≥ 50 KB or ≥ 500 lines. BLOCK is disabled by default; callers targeting a strict 200 K-window model can pass `disable_block=false` to opt into the BLOCK band. Any threshold can also be overridden per call. Agents without a profile (RustFn / spec-only) are reported with severity OK and bytes/lines 0. GH #31: each agent entry additionally carries `last_rendered_bytes` (the live, most-recently-baked post-render size from GET /v1/agents/:name/render-size — `null` when never dispatched, an N+1-per-agent HTTP cost this operator-diagnostic tool accepts) and, only once that value crosses the same `warn_bytes` threshold, a `delivery: \"system_ref\"` note (omitted entirely, not false/null, when under threshold) flagging that this agent's prompt is delivered by-reference rather than inline."
    )]
    async fn bp_doctor(
        &self,
        Parameters(req): Parameters<BpDoctorReq>,
    ) -> Result<CallToolResult, McpError> {
        let bind = req
            .bind
            .unwrap_or_else(|| server_control::DEFAULT_BIND.to_string());
        let thresholds = AgentMdThresholds::from_req(
            req.warn_bytes,
            req.warn_lines,
            req.block_bytes,
            req.block_lines,
            req.disable_block,
        );
        let url = format!("http://{bind}/v1/blueprints/{}/head", req.id);
        let client = reqwest::Client::builder()
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

        let mut per_agent = Vec::with_capacity(bp.agents.len());
        let mut severities: Vec<&'static str> = Vec::with_capacity(bp.agents.len());
        for agent in &bp.agents {
            let (bytes, lines) = match &agent.profile {
                Some(p) => (p.system_prompt.len(), p.system_prompt.lines().count()),
                None => (0usize, 0usize),
            };
            let severity = classify_agent_md_severity(bytes, lines, &thresholds);
            severities.push(severity);

            // GH #31: live post-render size lookup, reusing the same
            // `bind`/`client` already constructed above (Subtask 2's new
            // route). `last_rendered_bytes: null` is a normal response
            // (agent never dispatched) — always 200, never a 404.
            let render_size_url = format!("http://{bind}/v1/agents/{}/render-size", agent.name);
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
            per_agent.push(entry);
        }
        let verdict = aggregate_agent_md_verdict(&severities);
        let over_threshold_count = severities.iter().filter(|s| **s != "OK").count();

        let body = serde_json::json!({
            "bp_id": req.id,
            "bind": bind,
            "http_status": status.as_u16(),
            "verdict": verdict,
            "agent_count": bp.agents.len(),
            "over_threshold_count": over_threshold_count,
            "thresholds": {
                "warn_bytes": thresholds.warn_bytes,
                "warn_lines": thresholds.warn_lines,
                "block_bytes": thresholds.block_bytes,
                "block_lines": thresholds.block_lines,
                "disable_block": thresholds.disable_block,
            },
            "agents": per_agent,
            "guide_ref": "mse://guides/agent-md-authoring",
        });
        json_result(&body)
    }

    #[tool(
        description = "Doctor snapshot: mse mcp self state (in-process store = InMemory ephemeral) + server-side config (backend / store root / ref_base / registered BP list) fetched from GET /v1/doctor + an audit_findings section (GH #34) flagging `audit:<step>` artifacts across every run this mse mcp process is tracking. Answers 'where is the store?', 'how many BPs are registered?', and 'did any after-run audit leave a finding?' in a single call."
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

        let (run_count, tracked_runs) = {
            let inner = self.state.read().await;
            let tracked: Vec<(String, Option<String>)> = inner
                .runs
                .iter()
                .map(|(rid, h)| (rid.clone(), h.task_id.clone()))
                .collect();
            (inner.runs.len(), tracked)
        };

        // GH #34: audit_findings — for each tracked run whose task_id is
        // known, fetch its steps via the same HTTP steps API
        // (`GET /v1/tasks/:id/runs/:run/steps`) the REST debug plane
        // exposes, and flag entries whose name starts with `audit:` (the
        // `AfterRunAuditMiddleware` artifact naming convention). Runs with
        // no known task_id yet (an HTTP-proxied dispatch whose response is
        // still in flight) are silently skipped, not noted — that is not a
        // fetch failure. Per Invariant #1 (subtask-2): this scan NEVER
        // fails the doctor call — every error becomes a note.
        let mut audit_findings: Vec<AuditFinding> = Vec::new();
        let mut audit_fetch_notes: Vec<String> = Vec::new();
        if server_up {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(3))
                .build();
            match client {
                Ok(client) => {
                    for (run_id, task_id) in tracked_runs {
                        let Some(task_id) = task_id else {
                            continue;
                        };
                        let url = format!("http://{bind}/v1/tasks/{task_id}/runs/{run_id}/steps");
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
            audit_fetch_notes.push("mse serve down; audit_findings scan skipped".to_string());
        }

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
            "audit_findings": {
                "count": audit_findings.len(),
                "findings": audit_findings,
                "notes": audit_fetch_notes,
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
        description = "Fully stop mse serve via `launchctl bootout gui/<uid>/com.mse.server` (unloads the job; KeepAlive will not restart it until the next `mlua_swarm_server_start` / `mlua_swarm_server_restart`). Refuses (structured error) if the server reports in-flight runs or attached operators via GET /v1/status; pass force=true to skip the check and kill unconditionally. Returns {bind, stopped}."
    )]
    async fn mlua_swarm_server_shutdown(
        &self,
        Parameters(req): Parameters<ServerShutdownReq>,
    ) -> Result<CallToolResult, McpError> {
        let bind = req
            .bind
            .unwrap_or_else(|| server_control::DEFAULT_BIND.to_string());
        let force = req.force.unwrap_or(false);
        if !force && server_control::healthz_ok(&bind).await {
            match server_control::occupancy(&bind).await {
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
                    // Occupancy unknown (network hiccup / pre-ST4 server
                    // binary) — fail open, do not block a legitimate
                    // shutdown/restart indefinitely. Log for visibility.
                    eprintln!("mse mcp: occupancy check failed, proceeding: {e}");
                }
            }
        }
        match server_control::shutdown(&bind).await {
            Ok(out) => json_result(&out),
            Err(e) => Err(McpError::internal_error(e, None)),
        }
    }

    #[tool(
        description = "Kill + restart mse serve via `launchctl kickstart -k gui/<uid>/com.mse.server`, then healthz-polls up to 30s. Use after editing ~/.mse/config.toml to pick up the new settings. Refuses (structured error) if the server reports in-flight runs or attached operators via GET /v1/status; pass force=true to skip the check and kill unconditionally. Returns {status: started, bind}."
    )]
    async fn mlua_swarm_server_restart(
        &self,
        Parameters(req): Parameters<ServerRestartReq>,
    ) -> Result<CallToolResult, McpError> {
        let bind = req
            .bind
            .unwrap_or_else(|| server_control::DEFAULT_BIND.to_string());
        let force = req.force.unwrap_or(false);
        if !force && server_control::healthz_ok(&bind).await {
            match server_control::occupancy(&bind).await {
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
                    // Occupancy unknown (network hiccup / pre-ST4 server
                    // binary) — fail open, do not block a legitimate
                    // shutdown/restart indefinitely. Log for visibility.
                    eprintln!("mse mcp: occupancy check failed, proceeding: {e}");
                }
            }
        }
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

/// GH #31: fetches (`Http` mode) or reads (`File` mode) the content a
/// `SystemRef` points to. `base` is the already-`trim_end_matches('/')`d
/// server root (only consulted for `Http` mode when `system_ref.uri` is a
/// bare path, per the shipped Subtask 1 contract — `Http`-mode `uri` is
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
    use mlua_flow_ir::{Expr, Node as FlowNode};
    use mlua_swarm::blueprint::{
        current_schema_version, AgentDef, AgentKind, AgentMeta, AuditDef, AuditMode,
        BlueprintMetadata, CompilerHints, CompilerStrategy,
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
            },
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx: None,
            default_context_policy: None,
            projection_placement: None,
            audits: vec![],
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
            detach: None,
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
            detach: None,
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
            operator_kind: None,
            operator_kind_overrides: None,
            detach: Some(true),
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
            operator_kind: None,
            operator_kind_overrides: None,
            detach: None,
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
            detach: None,
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
            detach: None,
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
            detach: None,
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
            }))
            .await
            .expect_err("unknown handle must surface the HTTP error");
        let msg = format!("{err:?}");
        assert!(msg.contains("expected 204"), "err: {msg}");
    }

    /// GH #36 ST2: `name` and `ok=false` are mutually exclusive — the
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
            }))
            .await
            .unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("mutually exclusive"), "err: {msg}");
    }

    /// GH #36 ST2: a `name`-bearing submit call hits `POST
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

    /// GH #31 subtask-3 E2E: a real server, with `system_ref` config
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

    /// GH #31 subtask-3 E2E, `hash_mismatch` path: a minimal fake HTTP
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

    /// ST5 (`projection-adapter`) removal confirmation: `mse_ctx_get` no
    /// longer exists as an MCP tool — the Worker axis now gets prior
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
            "mse_ctx_get must be retired (ST5): {names:?}"
        );
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
                detach: None,
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

    // ─── GH #34: mse_doctor audit_findings surfacing (subtask-2) ───────────

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
    /// section plus a note, never fail the doctor call (subtask-2
    /// Invariant #1).
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
        assert!(
            notes
                .iter()
                .any(|n| n.as_str().unwrap_or_default().contains("mse serve down")),
            "notes: {notes:?}"
        );
    }

    /// GH #34 subtask-3 gap fix: dispatches a real Blueprint with `audits`
    /// declared through a real in-process `mse serve` router (same setup
    /// pattern as `mse_worker_fetch_and_submit_hit_the_http_endpoints`)
    /// and inspects the real `GET /v1/tasks/:id/runs/:run/steps` response.
    ///
    /// **Formerly** (subtask-2's `..._but_not_yet_the_audit_artifact`
    /// name): `Engine::submit_output` (`src/core/engine.rs`) only
    /// dual-wrote to the Data-plane `OutputStore` the HTTP steps API reads
    /// from for `OutputEvent::Final` events. `AfterRunAuditMiddleware`
    /// submits `OutputEvent::Artifact` — a different variant — so the
    /// audit finding never reached the Data-plane store and never
    /// appeared in the steps listing, even though it WAS recorded in the
    /// domain-plane (`Engine::output_tail`).
    ///
    /// **Now**: two changes were needed, not one.
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
    ///    This is a deviation from subtask-3.md's literal "Do NOT touch
    ///    ... server routes" scope note, made because the flipped
    ///    assertion below could not otherwise pass; see the impl-lead
    ///    report for this task for the full rationale.
    #[tokio::test]
    async fn steps_api_exposes_both_the_audited_steps_own_output_and_the_audit_artifact() {
        use mlua_swarm::{RustFnInProcessSpawnerFactory, SpawnerRegistry, WorkerResult};
        use std::sync::Arc;

        let factory = RustFnInProcessSpawnerFactory::new()
            .register_fn("echo", |inv| async move {
                Ok(WorkerResult {
                    value: serde_json::json!({ "echoed": inv.prompt }),
                    ok: true,
                })
            })
            .register_fn("audit-fn", |_inv| async move {
                Ok(WorkerResult {
                    value: serde_json::json!({ "finding": "clean" }),
                    ok: true,
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
                    at: "$.input".into(),
                },
                out: Expr::Path { at: "$.out".into() },
            },
            agents: vec![
                AgentDef {
                    name: "echo".into(),
                    kind: AgentKind::RustFn,
                    spec: serde_json::json!({"fn_id": "echo"}),
                    profile: None,
                    meta: Some(AgentMeta::default()),
                },
                AgentDef {
                    name: "auditor".into(),
                    kind: AgentKind::RustFn,
                    spec: serde_json::json!({"fn_id": "audit-fn"}),
                    profile: None,
                    meta: Some(AgentMeta::default()),
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
        };

        let client = reqwest::Client::new();
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
    /// core-crate gap documented on
    /// `steps_api_exposes_the_audited_steps_own_output_but_not_yet_the_audit_artifact`
    /// above: a stub router serving the real `GET
    /// /v1/tasks/:id/runs/:run/steps` response *shape* (not a real
    /// dispatch) proves the doctor tool round-trips correctly once the
    /// steps API genuinely returns an `audit:`-prefixed entry — i.e. this
    /// is subtask-2's own code working correctly against the documented
    /// contract, decoupled from whether core currently honors that
    /// contract for `OutputEvent::Artifact`.
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
}
