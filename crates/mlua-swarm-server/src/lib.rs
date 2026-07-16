//! the server lib: axum Router + handler set. Split out as a library so it can
//! be used from both `main.rs` (CLI) and integration tests.
//!
//! # Endpoints
//!
//! - `GET /v1/healthz`
//! - `POST /v1/sessions` / `DELETE /v1/sessions` (= operator attach / detach, Bearer sid)
//! - `POST /v1/tasks` (= unified Flow-form entry, Operator inject supported;
//!   `operator_sid` explicitly pins the task to a registered Operator session, S2).
//!   Also creates a `TaskRecord` + `RunRecord` (issue #13 ID-hierarchy persistence)
//!   and echoes their ids in the response; see the `tasks` module doc. Always
//!   synchronous, guarded against hanging (GH #33) by a readiness precheck
//!   (`503` when the launch resolves to an operator-delegate path with zero
//!   attached operators) and a `tokio::time::timeout` ceiling around the
//!   dispatch await (`504` on expiry) — see `run_flow_form`'s doc comment.
//! - `GET /v1/tasks` — list every persisted `TaskRecord` (newest first).
//! - `GET /v1/tasks/:id` — a `TaskRecord` plus every `RunRecord` kicked from it.
//! - `POST /v1/tasks/:id/runs` — re-kick an existing Task (new `RunId`, same
//!   `blueprint_ref` / `input_ctx`).
//! - `GET /v1/tasks/:id/runs/:run/steps` / `.../steps/:step` /
//!   `.../steps/:step/content` — the metadata + content debug plane over a
//!   Run's step OUTPUT (`:run` accepts `latest` or an explicit `R-<hex>`,
//!   `projection::McpQueryAdapter`); see the `projection` module doc. This
//!   is the operator / human-debug counterpart to the Worker axis's
//!   `context.steps` pointer list on `GET /v1/worker/prompt`
//!   (`projection-adapter` ST5 — replaces the ST2/ST4 single-value `GET
//!   /v1/tasks/:id/ctx`).
//! - `GET /v1/runs/:id` — a single `RunRecord` (its `step_entries` trace included).
//! - `POST /v1/operators` / `GET /v1/operators/:sid` / `DELETE /v1/operators/:sid` /
//!   `GET /v1/operators/:sid/ws` (WS upgrade) — REST-like Operator login flow,
//!   Bearer-mandatory; the sole WS Operator session route. See `operator_ws::login`
//!   module doc.
//!
//! The Enhance issue axis (`/issues`) lives in the `issues` module; callers merge
//! `build_issues_router` to integrate it into the same server.
//!
//! # The 3 faces of the Operator role (= registered directly on the engine SoT)
//!
//! The engine stateless-executor refactor removed the three
//! `AppState` registries (former `HookRegistry` / `BridgeRegistry` / `OperatorRegistry`);
//! all registration now goes directly to the engine SoT via
//! `engine.register_spawn_hook` / `register_senior_bridge` / `register_operator`.
//! `WSOperatorSession` (in the `operator_ws` module) registers all three traits
//! simultaneously under a single sid — one WS connection covers all 3 faces of
//! the Operator role, the canonical pattern.
//!
//! # `build_*` family
//!
//! - [`build_router`] — minimal entry (= `default_registry()`)
//! - [`build_router_with`] — caller provides a `SpawnerRegistry` and optional `BlueprintStore`
//!
//! The engine should be started with [`default_layer_registry`] (= `Engine::new_with_layers`);
//! otherwise `Blueprint.spawner_hints` is ignored.

#![warn(missing_docs)]

/// HTTP surface for inspecting/registering Blueprint state (`/v1/blueprints/*`).
pub mod blueprints;
/// Server config file support (`~/.mse/config.toml`, CLI > file > default merge).
pub mod config;
/// `/v1/data/*` endpoints (v9 Big Response handling, Store-owner direct path).
pub mod data;
/// `GET /v1/doctor` — read-only startup config / Store snapshot.
pub mod doctor;
/// HTTP surface for the `/v1/enhance/log` axis.
pub mod enhance_log;
/// `EnhanceSetting` HTTP CRUD (`/v1/enhance-settings*`).
pub mod enhance_settings;
/// HTTP surface for the Enhance issue axis (`/v1/issues*`).
pub mod issues;
/// WebSocket Operator Callback IF (`/v1/operators*`).
pub mod operator_ws;
/// `GET /v1/tasks/:id/runs/:run/steps*` (the metadata + content debug
/// plane over a Run's step OUTPUT — `McpQueryAdapter`, a server-side
/// `mlua_swarm::core::projection::ProjectionAdapter` impl reading through
/// the Data-plane `OutputStore` with a persisted `RunRecord.result_ref`
/// fallback). See the module doc for how this relates to
/// `operator_ws::session`'s in-flight `FileProjectionAdapter` hook and
/// `worker`'s Worker-axis `context.steps` pointer assembly.
pub mod projection;
/// HTTP surface for the Task/Run persistence axis (issue #13 ID hierarchy;
/// `GET /v1/tasks`, `GET /v1/tasks/:id`, `POST /v1/tasks/:id/runs`,
/// `GET /v1/runs/:id`). `POST /v1/tasks` itself stays in this module (it is
/// the entry point `tasks_start` shares with the flow-eval path) — see the
/// `tasks` module doc for the split rationale.
pub mod tasks;
/// `/v1/worker/*` endpoints (SubAgent self-fetch path).
pub mod worker;
pub use blueprints::{build_blueprints_router, build_blueprints_router_with_refs};
pub use enhance_log::build_enhance_log_router;
pub use enhance_settings::build_enhance_settings_router;
pub use issues::{build_issues_router, GetIssueResponse, PostIssueRequest, PostIssueResponse};
pub use operator_ws::{
    operators_create, operators_delete, operators_info, operators_ws_connect, ClientMsg,
    OperatorSessionEntry, ServerMsg, WSOperatorSession,
};
pub use projection::{McpQueryAdapter, ProjectionSource, StepList, StepPathQuery, StepSummary};
pub use tasks::{RunKickRequest, RunKickResponse, TaskDetailResponse};
pub use worker::{
    worker_artifact, worker_prompt, worker_result, ArtifactQuery, PromptQuery, WorkerResultReq,
};

use axum::{
    extract::{DefaultBodyLimit, State},
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use mlua_swarm::application::{BlueprintRef, TaskApplication};
use mlua_swarm::blueprint::store::BlueprintStore;
use mlua_swarm::core::config::CheckPolicy;
use mlua_swarm::service::TaskLaunchService;
use mlua_swarm::store::run::{RunContext, RunRecord, RunStatus, RunStore};
use mlua_swarm::store::task::{TaskRecord, TaskRecordStatus, TaskStore};
use mlua_swarm::{
    CapToken, Compiler, Engine, LayerRegistry, LuaInProcessSpawnerFactory, MainAIMiddleware,
    OperatorDelegateMiddleware, OperatorSpawnerFactory, Role, RunId, RustFnInProcessSpawnerFactory,
    SeniorEscalationMiddleware, SessionId, SpawnerRegistry, SubprocessProcessSpawnerFactory,
    TaskId,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// In-memory session map backing `/v1/sessions` attach/detach.
///
/// The `sid` handed to the client on this REST path is the token nonce
/// itself (a bearer secret), so the server never uses it as a map key —
/// entries are keyed by its fingerprint
/// (`mlua_swarm::types::token_fingerprint`; issue #14).
#[derive(Default)]
pub struct SessionStore {
    /// Live session tokens keyed by the sid's fingerprint.
    pub map: HashMap<String, CapToken>,
}

/// Shared axum handler state for the whole router. Cloned per-request (all
/// fields are `Arc`/cheap-clone), constructed once in [`build_router_with_ws_factory`].
#[derive(Clone)]
pub struct AppState {
    /// The engine SoT (attach/detach, dispatch, registries).
    pub engine: Engine,
    /// Live `/v1/sessions` attach records (Operator/Worker/etc session tokens).
    pub sessions: Arc<Mutex<SessionStore>>,
    /// Application used at the task entry to resolve `BlueprintRef`. Without a Store, runs in Inline-only mode.
    pub task_app: Arc<TaskApplication>,
    /// When `Some`, on WS connect a new `WSOperatorSession` is automatically registered
    /// with this factory under the sid name (= a `kind=operator` + `operator_ref=<sid>` AgentDef
    /// binds to the `WSOperatorSession` backend).
    /// When `None`, no auto-registration happens; the session is only registered on
    /// `engine.OperatorRegistry` (= only the `OperatorDelegateMiddleware` path is effective;
    /// the `OperatorSpawnerFactory` path is dead).
    pub ws_operator_factory: Option<Arc<OperatorSpawnerFactory>>,
    /// Owner of the Store on the Data path (Big Response handling). Added in v9.
    /// Independent layer — the Engine core and the Domain path (`/v1/worker/result`)
    /// are not involved.
    /// Default = `InMemoryOutputStore` (constructed inside `build_router_with_ws_factory`);
    /// callers can swap in an sqlite/fs backend later (future carry).
    pub data_store: Arc<dyn mlua_swarm::store::output::OutputStore>,
    /// Login-flow session store (`POST /v1/operators` mint records). `sid` →
    /// `OperatorSessionEntry`. This is the sole session store for the WS
    /// Operator role. See `operator_ws::login` module doc.
    pub operator_sessions:
        Arc<Mutex<HashMap<SessionId, Arc<crate::operator_ws::login::OperatorSessionEntry>>>>,
    /// S1 login-flow roles-exclusivity map. Role name → owning `sid`. Checked
    /// (and updated) atomically under a single lock in
    /// `operator_ws::login::operators_create` — a role already present here
    /// causes `POST /v1/operators` to return `409 CONFLICT`. Entries are
    /// released on `DELETE /v1/operators/:sid`.
    pub roles_to_sid: Arc<Mutex<HashMap<String, SessionId>>>,
    /// Persistence for `Task` records (issue #13 ID-hierarchy work-item
    /// identity; see `mlua_swarm::store::task` module doc). Default =
    /// `InMemoryTaskStore` (constructed inside `build_router_full`); callers
    /// can swap in a `SqliteTaskStore` via the `task_store` argument.
    pub task_store: Arc<dyn TaskStore>,
    /// Persistence for `Run` records (one kick of a Task; see
    /// `mlua_swarm::store::run` module doc). Default = `InMemoryRunStore`;
    /// callers can swap in a `SqliteRunStore` via the `run_store` argument.
    pub run_store: Arc<dyn RunStore>,
    /// Public HTTP base URL the server is reachable at (e.g.
    /// `"http://127.0.0.1:7777"`), sourced from the binary at boot time.
    /// When `Some`, `WSOperatorSession` renders it literally into the
    /// Spawn `directive`'s `base_url` line so the receiving operator can
    /// paste the frame into a SubAgent prompt without a `mse_doctor`
    /// detour (issue #8). `None` preserves the historical fallback
    /// (a placeholder that points at `mse_doctor`).
    pub base_url: Option<Arc<str>>,
    /// Server-wide fallback ceiling (seconds) for the `POST /v1/tasks`
    /// synchronous launch await (GH #33 Guard 2; see `run_flow_form`'s doc
    /// comment). Sourced from `config::ResolvedConfig::sync_timeout_secs`.
    /// A per-request `TaskLaunchRequest.timeout_secs` override, when
    /// present, takes priority over this value.
    pub sync_timeout_secs: u64,
}

/// Minimal entry point: builds a router with [`default_registry`] and no
/// `BlueprintStore` (Inline-only mode) or `ws_operator_factory`.
pub fn build_router(engine: Engine) -> Router {
    build_router_with(engine, default_registry(), None)
}

/// Default `LayerRegistry` for the server. Hint keys:
/// - `"main_ai"` → `MainAIMiddleware` (= fires SpawnHook before/after)
/// - `"senior_escalation"` → `SeniorEscalationMiddleware` (= on `ok=false`, escalates via `SeniorBridge.ask`)
/// - `"operator_delegate"` → `OperatorDelegateMiddleware` (= when an operator backend is registered, delegates the entire spawn)
///
/// Including any of these keys in `Blueprint.spawner_hints.layers` causes them to
/// be wrapped into a `SpawnerStack` at `service::linker::link` time (= per-launch;
/// the old `engine.bind` global-state path is retired).
/// Callers (the engine builder side) receive it via
/// `Engine::new_with_layers(cfg, mse_server::default_layer_registry())`.
pub fn default_layer_registry() -> LayerRegistry {
    LayerRegistry::new()
        .with_hint("main_ai", |_engine| Arc::new(MainAIMiddleware::new()))
        .with_hint("senior_escalation", |_engine| {
            Arc::new(SeniorEscalationMiddleware::new())
        })
        .with_hint("operator_delegate", |_engine| {
            Arc::new(OperatorDelegateMiddleware::new())
        })
}

/// Build form where the caller supplies a registry and an optional `BlueprintStore`.
/// The Operator callback path (= external HTTP / WS callers acting as an Operator)
/// must be pre-registered via `engine.register_*` (= the engine is the SoT).
/// See the `operator_ws` module doc and `OperatorInfo` (engine-side `ctx.rs`) for details.
pub fn build_router_with(
    engine: Engine,
    registry: SpawnerRegistry,
    store: Option<Arc<dyn BlueprintStore>>,
) -> Router {
    build_router_with_ws_factory(engine, registry, store, None)
}

/// 4-argument variant of `build_router_with`. Passing `ws_operator_factory = Some(arc)`
/// causes each WS connect to auto-register a new `WSOperatorSession` under its sid
/// name with the factory (= a `kind=operator` AgentDef with `operator_ref: <sid>`
/// can then bind to the WS client backend). Callers are expected to also install
/// the same `Arc` into the `SpawnerRegistry` via
/// `reg.register::<OperatorSpawnerFactory>(arc.clone())`.
pub fn build_router_with_ws_factory(
    engine: Engine,
    registry: SpawnerRegistry,
    store: Option<Arc<dyn BlueprintStore>>,
    ws_operator_factory: Option<Arc<OperatorSpawnerFactory>>,
) -> Router {
    build_router_with_ws_factory_and_output(engine, registry, store, ws_operator_factory, None)
}

/// 5-argument variant of [`build_router_with_ws_factory`]. Passing
/// `output_store = Some(arc)` swaps the default `InMemoryOutputStore` for a
/// caller-supplied backend (a `SqliteOutputStore`, for instance). `None`
/// preserves the historical behaviour (fresh in-memory store per call).
pub fn build_router_with_ws_factory_and_output(
    engine: Engine,
    registry: SpawnerRegistry,
    store: Option<Arc<dyn BlueprintStore>>,
    ws_operator_factory: Option<Arc<OperatorSpawnerFactory>>,
    output_store: Option<Arc<dyn mlua_swarm::store::output::OutputStore>>,
) -> Router {
    build_router_full(
        engine,
        registry,
        store,
        ws_operator_factory,
        output_store,
        None,
        None,
        None,
        crate::config::default_sync_timeout_secs(),
    )
}

/// 8-argument variant of [`build_router_with_ws_factory_and_output`].
/// Passing `base_url = Some(...)` (e.g. `"http://127.0.0.1:7777"`) makes
/// `WSOperatorSession` render the actual server bind into the Spawn
/// directive's `base_url` line, so the receiving operator can copy the
/// frame straight into a SubAgent prompt (issue #8). `None` preserves
/// the historical fallback (`<check with mse_doctor>` placeholder).
/// `task_store` / `run_store` swap the default `InMemoryTaskStore` /
/// `InMemoryRunStore` (issue #13 ID-hierarchy persistence) for a
/// caller-supplied backend (`SqliteTaskStore` / `SqliteRunStore`, for
/// instance); `None` preserves the process-volatile default.
/// `sync_timeout_secs` is the server-wide fallback ceiling for the `POST
/// /v1/tasks` synchronous launch await (GH #33 Guard 2) — see
/// `AppState::sync_timeout_secs` / `run_flow_form`'s doc comment.
// This is the terminal builder in the `build_router*` delegation chain
// (each variant adds one more caller-overridable store/factory); the
// argument count grows with the number of pluggable backends, not with
// unrelated responsibilities, so a plain allow is preferable to bundling
// them into a config struct only this one function would consume.
#[allow(clippy::too_many_arguments)]
pub fn build_router_full(
    engine: Engine,
    registry: SpawnerRegistry,
    store: Option<Arc<dyn BlueprintStore>>,
    ws_operator_factory: Option<Arc<OperatorSpawnerFactory>>,
    output_store: Option<Arc<dyn mlua_swarm::store::output::OutputStore>>,
    base_url: Option<Arc<str>>,
    task_store: Option<Arc<dyn TaskStore>>,
    run_store: Option<Arc<dyn RunStore>>,
    sync_timeout_secs: u64,
) -> Router {
    let compiler = Compiler::new(registry);
    let launch = Arc::new(TaskLaunchService::new(engine.clone(), compiler));
    let task_app = Arc::new(match store {
        Some(s) => TaskApplication::new(launch, s),
        None => TaskApplication::new_inline_only(launch),
    });
    let data_store: Arc<dyn mlua_swarm::store::output::OutputStore> = match output_store {
        Some(s) => s,
        None => Arc::new(mlua_swarm::store::output::InMemoryOutputStore::new()),
    };
    // subtask-4 / ST2 rework: wire the SAME `data_store` instance into the
    // engine's submit-time projection sink (`Engine::submit_output` /
    // `submit_worker_result_trusted`), so an ordinary worker
    // `/v1/worker/submit` — not just the explicit `POST /v1/data/emit` —
    // lands in this store too. `projection::McpQueryAdapter` (`GET
    // /v1/tasks/:id/runs/:run/steps*`) reads through this same `Arc`,
    // which is what makes an in-flight run's already-submitted step
    // OUTPUT queryable.
    engine.set_output_store(data_store.clone());
    let task_store: Arc<dyn TaskStore> = match task_store {
        Some(s) => s,
        None => Arc::new(mlua_swarm::store::task::InMemoryTaskStore::new()),
    };
    let run_store: Arc<dyn RunStore> = match run_store {
        Some(s) => s,
        None => Arc::new(mlua_swarm::store::run::InMemoryRunStore::new()),
    };
    let state = AppState {
        engine,
        sessions: Arc::new(Mutex::new(SessionStore::default())),
        task_app,
        ws_operator_factory,
        data_store,
        operator_sessions: Arc::new(Mutex::new(HashMap::new())),
        roles_to_sid: Arc::new(Mutex::new(HashMap::new())),
        task_store,
        run_store,
        base_url,
        sync_timeout_secs,
    };
    Router::new()
        .route("/v1/healthz", get(healthz))
        .route("/v1/status", get(status_get))
        // session = collection (POST = attach, DELETE = detach, sid via Authorization)
        .route(
            "/v1/sessions",
            post(sessions_attach).delete(sessions_detach),
        )
        // task = flat, single level; authz resolved via Authorization: Bearer <sid>
        .route("/v1/tasks", post(tasks_start).get(tasks::tasks_list))
        .route("/v1/tasks/:id", get(tasks::task_get))
        .route("/v1/tasks/:id/runs", post(tasks::task_rekick))
        .route("/v1/tasks/:id/runs/:run/steps", get(projection::steps_list))
        .route(
            "/v1/tasks/:id/runs/:run/steps/:step",
            get(projection::step_get),
        )
        .route(
            "/v1/tasks/:id/runs/:run/steps/:step/content",
            get(projection::step_content),
        )
        .route("/v1/runs/:id", get(tasks::run_get))
        // REST-like Operator login flow (Bearer-mandatory, roles exclusivity).
        // Sole WS Operator session route; see `operator_ws::login` module doc.
        .route("/v1/operators", post(operators_create))
        .route("/v1/operators/:sid/ws", get(operators_ws_connect))
        .route(
            "/v1/operators/:sid",
            get(operators_info).delete(operators_delete),
        )
        // SubAgent self-fetch path (the SubAgent self-fetch design). The SubAgent puts the
        // CapToken handed over via WS Spawn into Bearer and hits the prompt / result
        // endpoints directly over HTTP. See the `worker` module doc for details.
        .route("/v1/worker/prompt", get(worker::worker_prompt))
        .route("/v1/worker/result", post(worker::worker_result))
        // Simplified endpoint (= worker POSTs with just token + raw body; task_id is auto-looked-up).
        // `DefaultBodyLimit::max` is applied explicitly here (and on the sibling
        // `/v1/worker/artifact` below) — same 2MB axum ships as its implicit
        // global default, made visible rather than relied on silently.
        .route(
            "/v1/worker/submit",
            post(worker::worker_submit).layer(DefaultBodyLimit::max(2 * 1024 * 1024)),
        )
        // GH #36 ST1: named multi-part worker output. A worker stages one
        // named part per POST here, then completes the attempt with the
        // ordinary `/v1/worker/submit` above — see the `worker` module doc.
        .route(
            "/v1/worker/artifact",
            post(worker::worker_artifact).layer(DefaultBodyLimit::max(2 * 1024 * 1024)),
        )
        // GH #31: `Http`-mode fetch target for `system_ref.uri` (raw baked system
        // bytes, same Bearer flow as `/v1/worker/prompt`) + live per-agent render-size
        // lookup for `bp_doctor` (no Bearer, same trust tier as blueprints `get_head`).
        .route(
            "/v1/worker/prompt/system",
            get(worker::worker_prompt_system),
        )
        .route(
            "/v1/agents/:name/render-size",
            get(worker::agent_render_size),
        )
        // GH #32: structured worker degradation reporting — independent channel,
        // never touches OutputStore / the fold path. See the `worker` module doc.
        .route("/v1/worker/degradation", post(worker::worker_degradation))
        // Data path (v9 Big Response handling, independent from Domain / verdict flow)
        .route("/v1/data/emit", post(data::data_emit))
        .route(
            "/v1/data/:key",
            get(data::data_get).post(data::data_emit_named),
        )
        .with_state(state)
}

/// Default registry = Subprocess + RustFn (baseline `identity` worker pre-baked) + empty Operator factory.
///
/// `RustFnInProcessSpawnerFactory` gets one baseline entry (`fn_id = "identity"`)
/// baked in via [`mlua_swarm::worker::baseline::extend_with_baseline`]. This
/// is the shared bootstrap / smoke worker SoT across each binary (the server / MCP adapter /
/// one-shot runner) — it structurally replaces the old per-binary inline echo injection.
///
/// Usage: default Task path at server startup. If production needs additional
/// backends, callers bring in a different registry via
/// `build_router_with(engine, custom_registry)`. The enhance flow
/// (= patch-spawner / patch-applier / verifier-router / committer axes) uses
/// [`default_registry_with_enhance_flow`].
///
/// The Operator factory is an empty shell with zero registrations (= sids are
/// dynamically registered per WS connect; see the `operator_ws` module).
pub fn default_registry() -> SpawnerRegistry {
    let rustfn_factory =
        mlua_swarm::worker::baseline::extend_with_baseline(RustFnInProcessSpawnerFactory::new());

    let mut reg = SpawnerRegistry::new();
    reg.register::<SubprocessProcessSpawnerFactory>(Arc::new(SubprocessProcessSpawnerFactory));
    reg.register::<RustFnInProcessSpawnerFactory>(Arc::new(rustfn_factory));
    // Empty `LuaInProcessSpawnerFactory`: no `fn_id` is pre-registered here,
    // but BP agents can still declare `kind: lua` by carrying an inline
    // `spec.source` (or a `$file`-expanded Lua chunk). This lets a BP ship
    // deterministic Lua gates on the vanilla registry, without opting into
    // the enhance flow. See `LuaInProcessSpawnerFactory` docs for the spec
    // shape.
    reg.register::<LuaInProcessSpawnerFactory>(Arc::new(LuaInProcessSpawnerFactory::new()));
    reg.register::<OperatorSpawnerFactory>(Arc::new(OperatorSpawnerFactory::new()));
    reg
}

/// Opt-in registry that merges [`default_registry`] with the enhance flow
/// (Lua factory + AgentBlock factory).
///
/// Selected via the `the server` CLI flag `--enable-enhance-flow`. The enhance
/// flow is a separate-axis wrapper: the Lua factory (= 3 Lua workers + 3 primitive
/// bridges) and the AgentBlock factory (= patch-spawner path, expects
/// `assets/operator_scripts/blueprint_patch_spawner.lua` + `ANTHROPIC_API_KEY`)
/// are baked in as pipeline defaults. The baseline RustFn (`identity`) is pre-baked
/// the same way as in `default_registry`.
pub fn default_registry_with_enhance_flow() -> SpawnerRegistry {
    let lua_factory =
        mlua_swarm::enhance::blueprint::extend_factory(LuaInProcessSpawnerFactory::new());
    // The Factory is stateless (= 1 process → 1 factory shared by all AgentDefs).
    // Per-agent specialization (script_path / project_root, etc.) goes through AgentDef.spec.
    // The enhance-flow patch-spawner is declared literally in agents[].spec of `default_blueprint.yaml`.
    let agent_block_factory =
        mlua_swarm::worker::agent_block::AgentBlockInProcessSpawnerFactory::new();
    let rustfn_factory =
        mlua_swarm::worker::baseline::extend_with_baseline(RustFnInProcessSpawnerFactory::new());

    let mut reg = SpawnerRegistry::new();
    reg.register::<SubprocessProcessSpawnerFactory>(Arc::new(SubprocessProcessSpawnerFactory));
    reg.register::<RustFnInProcessSpawnerFactory>(Arc::new(rustfn_factory));
    reg.register::<LuaInProcessSpawnerFactory>(Arc::new(lua_factory));
    reg.register::<mlua_swarm::worker::agent_block::AgentBlockInProcessSpawnerFactory>(Arc::new(
        agent_block_factory,
    ));
    reg.register::<OperatorSpawnerFactory>(Arc::new(OperatorSpawnerFactory::new()));
    reg
}

// ─── handlers ────────────────────────────────────────────────────────────

async fn healthz() -> &'static str {
    "ok"
}

/// Response body for `GET /v1/status` (issue #35 ST4 — lifecycle
/// occupancy guard). Cheap-to-poll summary of "is it safe to kill this
/// server right now".
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct StatusResponse {
    /// Count of `Run`s currently `Running` (`RunStore::list_running`).
    /// Degrades to `0` on a store error rather than 500ing — see
    /// module doc rationale.
    pub running_runs: usize,
    /// Count of attached Operator ids (`engine.list_operator_ids()`,
    /// same idiom as `run_flow_form`'s Guard 1).
    pub attached_operators: usize,
}

/// `GET /v1/status`. Infallible summary for the ST4 occupancy guard —
/// store/engine query failures degrade the corresponding count to `0`
/// (logged via `tracing::warn!`) rather than 500ing, since this
/// endpoint may be polled frequently by a lifecycle-check caller that
/// should not itself become a hang/error surface.
async fn status_get(State(state): State<AppState>) -> Json<StatusResponse> {
    let running_runs = state
        .run_store
        .list_running()
        .await
        .map(|v| v.len())
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "status_get: list_running failed");
            0
        });
    let attached_operators = state.engine.list_operator_ids().await.len();
    Json(StatusResponse {
        running_runs,
        attached_operators,
    })
}

#[derive(Deserialize)]
struct AttachReq {
    agent_id: String,
    role: String,
    ttl_secs: u64,
}

#[derive(Serialize)]
struct AttachResp {
    session_id: String,
    role: String,
}

async fn sessions_attach(
    State(state): State<AppState>,
    Json(req): Json<AttachReq>,
) -> Result<Json<AttachResp>, ApiError> {
    let role = parse_role(&req.role)?;
    let token = state
        .engine
        .attach(req.agent_id, role, Duration::from_secs(req.ttl_secs))
        .await
        .map_err(ApiError::engine)?;
    // The wire `session_id` stays the nonce (Bearer credential contract);
    // the server-side map key is its fingerprint (issue #14).
    let sid = token.nonce.clone();
    let key = token.fingerprint();
    state.sessions.lock().await.map.insert(key, token);
    Ok(Json(AttachResp {
        session_id: sid,
        role: req.role,
    }))
}

async fn sessions_detach(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let sid = extract_bearer(&headers)?;
    let token = take_session_token(&state, &sid).await?;
    state
        .engine
        .detach(&token)
        .await
        .map_err(ApiError::engine)?;
    Ok(StatusCode::NO_CONTENT)
}

// ─── Unified /v1/tasks schema (= flow-eval path, Operator inject supported) ───────

/// `/v1/tasks` POST schema. Uses the flow-eval path and supports Operator inject
/// (kind / spawn_hook / senior_bridge). Expressing a one-shot task as a 1-Step
/// Blueprint is the only correct model.
///
/// `pub` (issue #19 ST5) so its `schemars`-derived JSON Schema can be
/// generated cross-crate by `mlua-swarm-cli`'s `mse://api/http-endpoints`
/// MCP resource; fields stay module-private (no public field-level API
/// surface is intended).
#[derive(Deserialize, schemars::JsonSchema)]
pub struct TaskLaunchRequest {
    /// `BlueprintRef` selects Inline (a full Blueprint value) or Id (a
    /// store lookup). Left opaque here — its own schema nests the full
    /// `Blueprint` schema (owned by `mse://api/blueprint-schema`), and
    /// mixing the two into this HTTP-endpoint resource would violate
    /// their separation of concerns (see the resource's module doc).
    #[schemars(with = "Value")]
    blueprint: BlueprintRef,
    /// flow.ir's initial `ctx` — every `Step.in` `$.<path>` reads from
    /// here. This field's role is limited to the flow-ir eval seed
    /// (issue #19); the Task-level execution context lives in the
    /// sibling top-level fields below (`project_root` / `work_dir` /
    /// `task_metadata`), promoted out of `init_ctx` to remove the
    /// prior "free bag nested in free JSON" duplication.
    ///
    /// Backward compat: the pre-#19 shape — the same three keys nested
    /// directly inside this object — is still honored as a fallback
    /// when the sibling field is absent; see `run_flow_form`'s 2-stage
    /// resolution and `TaskInputMiddleware::from_init_ctx`.
    #[schemars(with = "Value")]
    init_ctx: Value,
    /// Task-level project root (issue #19 canonical Task IF field —
    /// promoted out of `init_ctx`). Takes priority over a same-named
    /// key nested inside `init_ctx` (backward-compat fallback).
    #[serde(default)]
    project_root: Option<String>,
    /// Task-level working directory (issue #19), same priority rule as
    /// `project_root`.
    #[serde(default)]
    work_dir: Option<String>,
    /// Task-level arbitrary metadata bag (issue #19), same priority
    /// rule as `project_root`.
    #[serde(default)]
    #[schemars(with = "Option<Value>")]
    task_metadata: Option<Value>,
    /// TTL in seconds. When unspecified (`None`), falls back in this order:
    /// (1) `metadata.default_run_ttl_secs` from the resolved BP,
    /// (2) if absent, the server global `default_run_ttl()` (1800s).
    #[serde(default)]
    ttl_secs: Option<u64>,
    #[serde(default)]
    operator: Option<OperatorReq>,
    /// Explicit Operator session sid (or role alias) this task's entire Spawn
    /// stream should be routed to (runtime Operator match stage 1).
    ///
    /// When `Some`, it is validated at request time against
    /// `state.engine.list_operator_ids()` (the live `engine.operators`
    /// registry key set): an unknown/never-registered id returns `400`
    /// immediately — this is a deliberate hard-fail, in contrast to
    /// `OperatorDelegateWrapped::spawn`, which silently falls through to
    /// `inner.spawn` on a registry miss. A sid that *was* registered but has
    /// since disconnected (WS `tx` cleared, session entry retained for
    /// reconnect) passes this check and surfaces as an explicit dispatch-time
    /// error instead (`WSOperatorSession::send_and_await` returns `Err` when
    /// `tx` is `None`), which also propagates as a request failure rather
    /// than a silent fallback.
    ///
    /// On success this value **overrides** `operator.operator_backend_id`
    /// (last-write-wins, `operator_sid` takes priority) before the flow is
    /// dispatched — see `run_flow_form`. Dispatch still only delegates if the
    /// Blueprint opts into `spawner_hints.layers = ["operator_delegate"]`
    /// (unchanged precondition, same as the existing `operator_backend_id`
    /// field).
    ///
    /// When unset, behavior is unchanged: whatever
    /// `operator.operator_backend_id` / BP-level `operator_ref` alias
    /// resolution already does still applies.
    #[serde(default)]
    operator_sid: Option<String>,
    /// Per-request override for the sync launch's timeout ceiling (GH #33
    /// Guard 2, see `run_flow_form`'s doc comment). `None` (the default;
    /// existing clients are unaffected) falls back to
    /// `AppState::sync_timeout_secs` (server config), then the built-in
    /// default (300s). `Some(0)` is rejected with `400` — omit the field
    /// to defer to the server default rather than sending an explicit
    /// zero.
    #[serde(default)]
    timeout_secs: Option<u64>,
    /// Human-facing description of the work item (e.g. "resolve issue #10"),
    /// stashed verbatim into the minted `TaskRecord.goal`. Omitted / `None`
    /// stores an empty string — the flow-eval path itself never reads it.
    #[serde(default)]
    goal: Option<String>,
    /// The "launch request" tier (tier 1, highest
    /// priority) of the `check_policy` cascade
    /// (`launch request > blueprint > server config`). `None` (the default;
    /// existing clients are unaffected) leaves the tier unspecified so the
    /// Blueprint-declared `check_policy` and, failing that, the server-wide
    /// `EngineCfg.check_policy` default decide. Wire form is snake_case
    /// (`"silent"` / `"warn"` / `"strict"`). Threaded verbatim into
    /// `TaskApplicationInput.check_policy`.
    #[serde(default)]
    check_policy: Option<CheckPolicy>,
    /// GH #37: opt into the detached (asynchronous) launch. `false` (the
    /// default; existing clients are unaffected) keeps the synchronous
    /// launch: the handler drives the flow eval inline and returns the
    /// `final_ctx` on completion. `true` spawns the flow eval as a
    /// detached background task and returns `202 Accepted` immediately
    /// with `{task_id, run_id, status: "running"}` (`final_ctx` is
    /// `null`) — the run's only lifetime bound is `ttl_secs`, and its
    /// outcome is observed via `GET /v1/runs/:id` (or the `swarm_status`
    /// MCP tool). Mutually exclusive with `timeout_secs` (the sync-launch
    /// ceiling has no meaning for a detached run; combining them is a
    /// `400`).
    #[serde(default)]
    detach: bool,
}

/// Operator inject sub-schema of [`TaskLaunchRequest`] (`kind` / `id` /
/// `spawn_hook_id` / `senior_bridge_id` / `operator_backend_id` /
/// `per_agent_kinds`). `pub` for the same cross-crate schema-generation
/// reason as `TaskLaunchRequest`.
#[derive(Deserialize, Default, schemars::JsonSchema)]
pub struct OperatorReq {
    /// `main_ai` / `automate` / `composite`. This is the "Runtime Global"
    /// tier of the 4-tier `OperatorKind` cascade (see `mlua_swarm
    /// ::ctx::collapse_operator_kind`); when unspecified, falls through to
    /// the BP-level tiers (`OperatorDef.kind` / `Blueprint
    /// .default_operator_kind`) instead of eagerly defaulting to `automate`.
    #[serde(default)]
    kind: Option<String>,
    /// Operator id at attach time (= sessions tracking key in the EventLog); unspecified defaults to `"http-run"`.
    #[serde(default)]
    id: Option<String>,
    /// Name of a hook pre-registered via `engine.register_spawn_hook`; `None` if unspecified.
    #[serde(default)]
    spawn_hook_id: Option<String>,
    /// Name of a bridge pre-registered via `engine.register_senior_bridge`; `None` if unspecified.
    #[serde(default)]
    senior_bridge_id: Option<String>,
    /// Name of an Operator backend pre-registered via `engine.register_operator`
    /// (= the path that delegates the entire spawn to an external Operator);
    /// `None` if unspecified. When `kind == MainAi/Composite` and this id is `Some`,
    /// `OperatorDelegateMiddleware` bypasses `inner.spawn` and calls `operator.execute` instead.
    /// This is a different axis from `operator.id` (= session tracking label);
    /// `operator_backend_id` is the registry lookup key.
    #[serde(default)]
    operator_backend_id: Option<String>,
    /// "Runtime Agent-level" tier (highest priority) of the `OperatorKind`
    /// cascade — per-agent override, keyed by `AgentDef.name`, value is
    /// `main_ai` / `automate` / `composite` (same parsing as `kind`).
    /// `None` / absent means no per-agent override.
    #[serde(default)]
    per_agent_kinds: Option<HashMap<String, String>>,
}

/// Parse a wire-level kind string (`"main_ai"` / `"automate"` / `"composite"`)
/// into `OperatorKind`. Shared by `OperatorReq.kind` and
/// `OperatorReq.per_agent_kinds` values.
fn parse_operator_kind_str(s: &str) -> Result<mlua_swarm::OperatorKind, ApiError> {
    use mlua_swarm::OperatorKind;
    match s {
        "main_ai" => Ok(OperatorKind::MainAi),
        "composite" => Ok(OperatorKind::Composite),
        "automate" => Ok(OperatorKind::Automate),
        other => Err(ApiError::bad_request(format!(
            "operator kind: unknown value '{other}' (expected main_ai|automate|composite)"
        ))),
    }
}

/// `/v1/tasks` POST response body. `pub` for the same cross-crate
/// schema-generation reason as [`TaskLaunchRequest`].
#[derive(Serialize, schemars::JsonSchema)]
pub struct TaskLaunchResponse {
    /// The final flow.ir `ctx` after every `Step.out` has been written.
    #[schemars(with = "Value")]
    final_ctx: Value,
    /// Debug-formatted `BlueprintVersion` the run resolved against, when
    /// the Blueprint came from a store lookup (`None` for `Inline` refs).
    bound_version: Option<String>,
    /// Resolved TTL (seconds) actually applied to the run. Exposes the
    /// 3-layer cascade (request body → BP metadata → server default) so
    /// clients can verify which value took effect without re-deriving it.
    effective_ttl_secs: u64,
    /// Which layer of the TTL cascade won.
    ttl_source: TtlSource,
    /// The `TaskRecord` minted for this request (issue #13 ID-hierarchy
    /// persistence). `GET /v1/tasks/:id` re-fetches it; `POST
    /// /v1/tasks/:id/runs` re-kicks it under a fresh `RunId`.
    #[schemars(with = "String")]
    task_id: TaskId,
    /// The `RunRecord` minted for this specific kick. `GET /v1/runs/:id`
    /// re-fetches it (`step_entries` included).
    #[schemars(with = "String")]
    run_id: RunId,
    /// Launch outcome at response time (GH #37). The synchronous path
    /// (default) reports `done` — the flow eval completed before this
    /// response was built. A detached launch (`detach: true`) reports
    /// `running` — the eval continues in the background; poll `GET
    /// /v1/runs/:id` for the terminal status and result.
    status: RunStatus,
}

/// `tasks_start`'s reply — a [`TaskLaunchResponse`] plus the HTTP status
/// it rides out on (`200 OK` for the synchronous path, `202 Accepted` for
/// a detached launch, GH #37). A tuple struct with the body first so
/// handler-level tests keep their established `.0` access to the response
/// body regardless of which path produced it.
pub struct TaskLaunchReply(pub TaskLaunchResponse, pub StatusCode);

impl IntoResponse for TaskLaunchReply {
    fn into_response(self) -> Response {
        (self.1, Json(self.0)).into_response()
    }
}

/// Which layer of the TTL cascade (request body → BP metadata → server
/// default) resolved [`TaskLaunchResponse::effective_ttl_secs`]. `pub` for
/// the same cross-crate schema-generation reason as `TaskLaunchRequest`.
#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TtlSource {
    /// The request body's `ttl_secs` was set explicitly.
    RequestBody,
    /// The request body omitted `ttl_secs`; the resolved Blueprint's
    /// `metadata.default_run_ttl_secs` was set.
    BpMetadata,
    /// Both the request body and the Blueprint metadata omitted a TTL;
    /// the server-global `default_run_ttl()` (1800s) applied.
    ServerDefault,
}

/// Unified `/v1/tasks` POST entry (= Flow form only).
/// Runs `Blueprint.flow` to completion via flow eval in a single round-trip.
/// One-shot tasks are also expressed as a 1-Step Blueprint. Operator
/// (kind / spawn_hook / senior_bridge) can be injected per request body.
/// `operator_sid` (S2, runtime Operator match stage 1) additionally
/// lets the caller pin the task to a specific already-registered Operator
/// session sid, bypassing BP-level alias lookup — see `TaskLaunchRequest` doc.
async fn tasks_start(
    State(state): State<AppState>,
    Json(req): Json<TaskLaunchRequest>,
) -> Result<TaskLaunchReply, ApiError> {
    run_flow_form(&state, req).await
}

/// Flow-form path (= via `TaskApplication::handle_with_run`).
/// Core handler behind the `/v1/tasks` entry (`tasks_start`).
///
/// Engine stateless-executor refactor: the per-request
/// sub_engine + 3-registry propagate loop is retired; the startup-built
/// `state.task_app` (= a `TaskLaunchService` wrap around `state.engine`) is
/// used directly. The Operator callback IF (`spawn_hook_id` /
/// `senior_bridge_id` / `operator_backend_id`) is registered on
/// `state.engine.register_*` at WS connect time — the engine is the SoT.
/// See the `operator_ws` module doc for details.
///
/// # GH #33 — sync-hang guards
///
/// This handler is always synchronous end-to-end (no sync/async branch);
/// two fail-loud guards keep a bad launch from hanging the HTTP request
/// forever:
///
/// - **Guard 1 (readiness precheck, `503`)**: when the request/BP
///   references an operator backend (`operator.operator_backend_id`, set
///   directly or via `operator_sid`) and `state.engine.list_operator_ids()`
///   is empty, the request fails immediately rather than dispatching into
///   a session with nothing attached to serve it. Coarse by design — a
///   launch that cannot be cheaply determined to route through an operator
///   is never rejected here (Guard 2 still covers the hang in that case).
/// - **Guard 2 (sync timeout, `504`)**: the single
///   `state.task_app.handle_with_run` await is wrapped in
///   `tokio::time::timeout`. Ceiling cascade, highest priority first:
///   request `timeout_secs` (rejecting `Some(0)` with `400`), then
///   `AppState::sync_timeout_secs` (server config), then the built-in
///   default (300s). On expiry the timed-out future is dropped — this
///   cancels the in-process flow eval (the flow is abandoned, not
///   resumed; intended v1 semantics) — and the Task/Run records are
///   best-effort marked `Failed` so they do not stay `Running` forever.
///
/// # GH #37 — detached launch (`detach: true`)
///
/// The sync semantics above tie the flow-eval driver's lifetime to this
/// request's future — a long-running detached worker that outlives the
/// ceiling gets its (individually successful) `/v1/worker/*` submits
/// orphaned when the driver is cancelled. `detach: true` decouples them:
/// the eval (plus `finalize_run`) runs in a `tokio::spawn`ed background
/// task whose only lifetime bound is the resolved `ttl_secs` (marked
/// `Failed` on expiry, same best-effort persistence as Guard 2), and the
/// handler returns `202 Accepted` with `status: "running"` immediately.
/// Guard 1 still applies (checked before any store write); Guard 2's
/// ceiling does not (`timeout_secs` + `detach` together is a `400`).
/// Client disconnect after the `202` cannot cancel the run.
async fn run_flow_form(
    state: &AppState,
    req: TaskLaunchRequest,
) -> Result<TaskLaunchReply, ApiError> {
    use mlua_swarm::application::{BlueprintRef as AppBlueprintRef, TaskApplicationInput};
    use mlua_swarm::OperatorKind;

    // Snapshot everything the TaskRecord needs before `req.blueprint` /
    // `req.init_ctx` are moved into the dispatch path below.
    let blueprint_ref_json = serde_json::to_value(&req.blueprint)
        .map_err(|e| ApiError::bad_request(format!("blueprint snapshot: {e}")))?;
    let input_ctx_snapshot = req.init_ctx.clone();
    let goal = req.goal.clone().unwrap_or_default();

    // issue #19 ST2: resolve the Task-level canonical fields
    // (`project_root` / `work_dir` / `task_metadata`) once, at the wire
    // boundary. Sibling top-level fields on the request body take
    // priority; the pre-#19 shape (same key nested inside `init_ctx`) is
    // only a fallback for legacy callers. The result is threaded straight
    // through as `TaskApplicationInput.task_input` — `init_ctx` itself is
    // NOT mutated, so it stays a pure flow-ir eval seed identical to
    // whatever the caller sent.
    let task_input_spec = build_task_input_spec_from_request(&req);
    // Issue #19 ST4: snapshot the resolved spec into the `TaskRecord` (JSON,
    // same "bare `Value`" rationale as `blueprint_ref_json` /
    // `input_ctx_snapshot` above) so `POST /v1/tasks/:id/runs` can resolve
    // it back out on rekick without re-deriving it from a since-stale
    // request body. Cloned rather than computed from `task_input_spec`
    // after the fact — the original is still moved into
    // `TaskApplicationInput.task_input` below.
    let task_input_spec_snapshot = task_input_spec
        .clone()
        .map(|spec| serde_json::to_value(&spec))
        .transpose()
        .map_err(|e| ApiError::bad_request(format!("task_input_spec snapshot: {e}")))?;
    let init_ctx = req.init_ctx.clone();

    let mut op_req = req.operator.unwrap_or_default();

    // S2: explicit `operator_sid` override (runtime Operator match stage 1).
    // Resolved *before* building `operator_kind` / dispatching so an
    // unknown sid fails fast with a 400, never silently falling back to the
    // BP-level alias lookup. See `TaskLaunchRequest::operator_sid` doc for the
    // disconnected-vs-unknown distinction.
    if let Some(sid) = &req.operator_sid {
        let known_ids = state.engine.list_operator_ids().await;
        if !known_ids.iter().any(|id| id == sid) {
            return Err(ApiError::bad_request(format!(
                "operator_sid: no such registered operator session '{sid}'"
            )));
        }
        op_req.operator_backend_id = Some(sid.clone());
    }

    // GH #33 Guard 2 ceiling resolution: request field > server config >
    // built-in default (300s, `config::default_sync_timeout_secs`).
    // Validated up front — before any TaskRecord/RunRecord side effects —
    // so a caller-supplied `Some(0)` fails fast with `400` rather than
    // minting records for a launch that was never going to dispatch.
    // GH #37: `detach: true` makes the sync ceiling meaningless (the
    // detached run is bounded by `ttl_secs` alone) — combining the two
    // is rejected here, same fail-fast-before-side-effects ordering.
    let detach = req.detach;
    let sync_timeout_secs = match (detach, req.timeout_secs) {
        (true, Some(_)) => {
            return Err(ApiError::bad_request(
                "timeout_secs is the synchronous launch ceiling and does not apply to a \
                 detached launch (detach: true), whose lifetime bound is ttl_secs — omit \
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

    // GH #33 Guard 1: operator readiness precheck. Coarse signal — this
    // handler can cheaply see whether the request/BP references an
    // operator backend (`operator.operator_backend_id`, set directly or
    // resolved above from `operator_sid`), but not the full
    // `OperatorDelegateMiddleware` routing decision (that also considers
    // BP-level `kind` tiers, resolved only at dispatch time). When a
    // backend is referenced and *zero* operators are attached at all,
    // fail fast rather than dispatching into a session nothing can serve.
    // A launch this coarse check cannot positively identify as
    // operator-delegate is never rejected here — Guard 2 (the timeout
    // wrap below) still covers the hang in that case.
    if let Some(backend_id) = op_req.operator_backend_id.as_deref() {
        let attached = state.engine.list_operator_ids().await;
        if attached.is_empty() {
            return Err(ApiError::unavailable(format!(
                "no operator attached to serve this launch (operator backend '{backend_id}' \
                 requested): attach an operator via POST /v1/operators + WS, or use the \
                 poll-style flow (GET /v1/worker/prompt + POST /v1/worker/submit)"
            )));
        }
    }

    // "Runtime Global" tier: `Some(_)` — including `Some(Automate)` — is
    // always an explicit request that outranks the BP-level tiers; an
    // absent/unset `kind` in the request body stays `None`, leaving the
    // BP-level tiers (`OperatorDef.kind` / `Blueprint.default_operator_kind`)
    // to decide instead of eagerly defaulting to `Automate`.
    let operator_kind = op_req
        .kind
        .as_deref()
        .map(parse_operator_kind_str)
        .transpose()?;
    let operator_id = op_req.id.unwrap_or_else(|| "http-run".to_string());
    // "Runtime Agent-level" tier: per-agent overrides. Absent/empty = no
    // override for any agent, letting the BP-level tiers decide per agent.
    let mut operator_kind_overrides: HashMap<String, OperatorKind> = HashMap::new();
    for (agent, kind_str) in op_req.per_agent_kinds.take().unwrap_or_default() {
        operator_kind_overrides.insert(agent, parse_operator_kind_str(&kind_str)?);
    }

    let blueprint: AppBlueprintRef = match req.blueprint {
        AppBlueprintRef::Inline { value } => AppBlueprintRef::Inline { value },
        AppBlueprintRef::Id { id, version } => AppBlueprintRef::Id { id, version },
    };

    // TTL resolution cascade: (1) request body value, (2) BP metadata `default_run_ttl_secs`,
    // (3) server global default (`default_run_ttl()`, 1800s).
    let (ttl_secs, ttl_source) = match req.ttl_secs {
        Some(v) => (v, TtlSource::RequestBody),
        None => {
            let (resolved_bp, _ver) = state
                .task_app
                .resolve(&blueprint)
                .await
                .map_err(|e| ApiError::bad_request(format!("bp resolve: {e}")))?;
            match resolved_bp.metadata.default_run_ttl_secs {
                Some(v) => (v, TtlSource::BpMetadata),
                None => (default_run_ttl(), TtlSource::ServerDefault),
            }
        }
    };

    // issue #13 ID-hierarchy persistence: mint the work-item identity (Task)
    // and this kick's identity (Run) *before* dispatching, so a Task/Run
    // pair always exists even if the flow itself fails mid-way (the
    // Failed-status paths below still have a row to update).
    let task_id = TaskId::new();
    let run_id = RunId::new();
    let now = tasks::now_secs();
    state
        .task_store
        .create(TaskRecord {
            id: task_id.clone(),
            goal,
            blueprint_ref: blueprint_ref_json,
            input_ctx: input_ctx_snapshot,
            task_input_spec: task_input_spec_snapshot,
            status: TaskRecordStatus::Running,
            created_at: now,
            updated_at: now,
        })
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
            operator_sid: req.operator_sid.clone(),
            result_ref: None,
            created_at: now,
            updated_at: now,
        })
        .await
        .map_err(ApiError::engine)?;

    let run_ctx = RunContext::new(run_id.clone(), state.run_store.clone());
    let input = TaskApplicationInput {
        blueprint,
        operator_id: operator_id.clone(),
        role: Role::Operator,
        ttl: Duration::from_secs(ttl_secs),
        init_ctx,
        operator_kind,
        bridge_id: op_req.senior_bridge_id,
        hook_id: op_req.spawn_hook_id,
        operator_backend_id: op_req.operator_backend_id,
        operator_kind_overrides,
        task_input: task_input_spec,
        // The request-body top-level `check_policy` (tier 1)
        // flows straight into the cascade resolved once in
        // `TaskLaunchService::launch`.
        check_policy: req.check_policy,
    };

    // GH #37 detached launch: the eval driver runs in its own spawned
    // task — its lifetime is bound to `ttl_secs`, not to this request's
    // future (client disconnect / handler completion cannot cancel it).
    // The spawned task owns the run to its terminal status: `finalize_run`
    // on completion, or the same best-effort `Failed` marking as Guard 2
    // if the ttl ceiling expires first.
    if detach {
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
                    let reason = json!({
                        "error": format!("detached run exceeded {ttl_secs}s ttl ceiling"),
                    });
                    if let Err(e) = bg_state.run_store.set_result(&bg_run_id, reason).await {
                        tracing::warn!(%bg_run_id, error = %e, "run_flow_form: detached ttl set_result failed");
                    }
                    if let Err(e) = bg_state
                        .run_store
                        .update_status(&bg_run_id, RunStatus::Failed)
                        .await
                    {
                        tracing::warn!(%bg_run_id, error = %e, "run_flow_form: detached ttl run update_status(Failed) failed");
                    }
                    if let Err(e) = bg_state
                        .task_store
                        .update_status(&bg_task_id, TaskRecordStatus::Failed)
                        .await
                    {
                        tracing::warn!(%bg_task_id, error = %e, "run_flow_form: detached ttl task update_status(Failed) failed");
                    }
                    return;
                }
            };
            // `finalize_run` persists both the Ok and Err outcomes itself;
            // the passthrough return value has no consumer here.
            let _ = tasks::finalize_run(&bg_state, &bg_task_id, &bg_run_id, outcome).await;
        });
        return Ok(TaskLaunchReply(
            TaskLaunchResponse {
                final_ctx: Value::Null,
                bound_version: None,
                effective_ttl_secs: ttl_secs,
                ttl_source,
                task_id,
                run_id,
                status: RunStatus::Running,
            },
            StatusCode::ACCEPTED,
        ));
    }

    // GH #33 Guard 2: the single await point this handler blocks on. On
    // expiry the timed-out future is dropped, cancelling the in-process
    // flow eval — the flow is abandoned, not resumed (intended v1
    // semantics; stage-granularity resume is a coarser guarantee than
    // this handler makes, out of scope here).
    let outcome = match tokio::time::timeout(
        Duration::from_secs(sync_timeout_secs),
        state.task_app.handle_with_run(input, Some(run_ctx)),
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(_elapsed) => {
            // Best effort: mark the Task/Run so they do not stay `Running`
            // forever. Reuses the existing `Failed` variant (no new
            // schema-crate enum additions) and stashes a reason string
            // into `RunRecord.result_ref` — the only free-form field the
            // Run schema carries; secondary persistence failures here are
            // logged and swallowed, mirroring `tasks::finalize_run`'s
            // error-path convention.
            let reason = json!({
                "error": format!("sync launch exceeded {sync_timeout_secs}s timeout ceiling"),
            });
            if let Err(e) = state.run_store.set_result(&run_id, reason).await {
                tracing::warn!(%run_id, error = %e, "run_flow_form: timeout run set_result failed");
            }
            if let Err(e) = state
                .run_store
                .update_status(&run_id, RunStatus::Failed)
                .await
            {
                tracing::warn!(%run_id, error = %e, "run_flow_form: timeout run update_status(Failed) failed");
            }
            if let Err(e) = state
                .task_store
                .update_status(&task_id, TaskRecordStatus::Failed)
                .await
            {
                tracing::warn!(%task_id, error = %e, "run_flow_form: timeout task update_status(Failed) failed");
            }
            return Err(ApiError::timeout(format!(
                "sync launch exceeded {sync_timeout_secs}s timeout ceiling: the in-process flow \
                 eval was abandoned (dropping the future cancels it); attach an operator that \
                 acks promptly (POST /v1/operators + WS), or raise timeout_secs / sync_timeout_secs"
            )));
        }
    };

    let out = tasks::finalize_run(state, &task_id, &run_id, outcome)
        .await
        .map_err(|e| ApiError::bad_request(format!("run: {e}")))?;

    Ok(TaskLaunchReply(
        TaskLaunchResponse {
            final_ctx: out.final_ctx,
            bound_version: out.bound_version.map(|v| format!("{:?}", v)),
            effective_ttl_secs: ttl_secs,
            ttl_source,
            task_id,
            run_id,
            status: RunStatus::Done,
        },
        StatusCode::OK,
    ))
}

/// issue #19 ST2 direct sibling-field resolver — extracts the three
/// Task-level canonical fields (`project_root` / `work_dir` /
/// `task_metadata`) once at the wire boundary. Sibling top-level body
/// fields take priority; the pre-#19 shape (same key nested inside
/// `init_ctx`) is only a fallback for legacy callers. Unlike the ST1
/// `resolve_task_level_init_ctx` bridge this replaced, `init_ctx` is
/// NOT mutated — the resolved values are handed straight to
/// [`mlua_swarm::service::TaskLaunchInput::task_input`], keeping
/// `init_ctx` a pure flow-ir eval seed.
///
/// Returns `None` when all three fields resolve to `None` (no
/// middleware is layered onto the spawner stack downstream — the
/// [`mlua_swarm::middleware::task_input::TaskInputMiddleware::new_from_fields`]
/// contract).
fn build_task_input_spec_from_request(
    req: &TaskLaunchRequest,
) -> Option<mlua_swarm::service::TaskInputSpec> {
    let project_root = req.project_root.clone().or_else(|| {
        req.init_ctx
            .get("project_root")
            .and_then(Value::as_str)
            .map(String::from)
    });
    let work_dir = req.work_dir.clone().or_else(|| {
        req.init_ctx
            .get("work_dir")
            .and_then(Value::as_str)
            .map(String::from)
    });
    let task_metadata = req.task_metadata.clone().or_else(|| {
        req.init_ctx
            .get("task_metadata")
            .filter(|v| v.is_object())
            .cloned()
    });

    if project_root.is_none() && work_dir.is_none() && task_metadata.is_none() {
        None
    } else {
        Some(mlua_swarm::service::TaskInputSpec {
            project_root,
            work_dir,
            task_metadata,
        })
    }
}

// ─── helpers ─────────────────────────────────────────────────────────────

async fn take_session_token(state: &AppState, sid: &str) -> Result<CapToken, ApiError> {
    // `sid` on this path is the token nonce itself (a bearer secret), so
    // both the map key and the not-found diagnostic use its fingerprint
    // (issue #14 — never echo the nonce back in an error body).
    let key = mlua_swarm::types::token_fingerprint(sid);
    state
        .sessions
        .lock()
        .await
        .map
        .remove(&key)
        .ok_or_else(|| ApiError::not_found(format!("session: fp={key}")))
}

/// Extracts sid from `Authorization: Bearer <sid>`. Strict — does not accept any other scheme prefix.
fn extract_bearer(headers: &HeaderMap) -> Result<String, ApiError> {
    let v = headers
        .get(AUTHORIZATION)
        .ok_or_else(|| ApiError::bad_request("missing Authorization header".into()))?
        .to_str()
        .map_err(|_| ApiError::bad_request("invalid Authorization header encoding".into()))?;
    let sid = v
        .strip_prefix("Bearer ")
        .ok_or_else(|| ApiError::bad_request("Authorization must be 'Bearer <sid>'".into()))?
        .trim();
    if sid.is_empty() {
        return Err(ApiError::bad_request("Bearer sid is empty".into()));
    }
    Ok(sid.to_string())
}

fn parse_role(s: &str) -> Result<Role, ApiError> {
    match s.to_ascii_lowercase().as_str() {
        "operator" => Ok(Role::Operator),
        "worker" => Ok(Role::Worker),
        "observer" => Ok(Role::Observer),
        "senior" => Ok(Role::Senior),
        other => Err(ApiError::bad_request(format!("unknown role: {other}"))),
    }
}

// ─── error type ──────────────────────────────────────────────────────────

/// Uniform error response type for the handlers in this module. Converts to
/// a JSON `{"error": message}` body with the given status via [`IntoResponse`].
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    /// Wraps an engine-side error as `500 Internal Server Error`.
    pub fn engine(e: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: format!("engine: {e}"),
        }
    }
    /// Builds a `404 Not Found` with the given message.
    pub fn not_found(m: String) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: m,
        }
    }
    /// Builds a `400 Bad Request` with the given message.
    pub fn bad_request(m: String) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: m,
        }
    }
    /// Builds a `503 Service Unavailable` with the given message (GH #33
    /// Guard 1 — operator readiness precheck).
    pub fn unavailable(m: String) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: m,
        }
    }
    /// Builds a `504 Gateway Timeout` with the given message (GH #33
    /// Guard 2 — sync launch timeout ceiling).
    pub fn timeout(m: String) -> Self {
        Self {
            status: StatusCode::GATEWAY_TIMEOUT,
            message: m,
        }
    }
    /// Builds a `410 Gone` with the given message (GH #37 — worker
    /// submit/artifact addressed at a Run that already reached a terminal
    /// status; the silent-`204`-then-orphan alternative is the failure
    /// shape this replaces).
    pub fn gone(m: String) -> Self {
        Self {
            status: StatusCode::GONE,
            message: m,
        }
    }
    /// Builds a `413 Payload Too Large` with the given message (GH #42 —
    /// `@file:` sentinel resolves to a file larger than the shared
    /// `DefaultBodyLimit`; same size ceiling as the inline body path).
    pub fn payload_too_large(m: String) -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            message: m,
        }
    }
    /// Builds a `422 Unprocessable Entity` with the given message (GH #50
    /// — a `worker_submit` / `worker_artifact` value violates the
    /// dispatching agent's declared `VerdictContract`: rejected before it
    /// reaches `submit_worker_result_trusted` / `stage_worker_artifact_trusted`,
    /// i.e. before it can land in the flow ctx).
    pub fn unprocessable(m: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            message: m.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({"error": self.message}))).into_response()
    }
}

fn default_run_ttl() -> u64 {
    // 1800s (= 30 min). Prevents op_token expiry across a flow.ir multi-step chain
    // (= 5+ SubAgent dispatches at 30–60s each). Origin: the observed fvloop smoke
    // where a post-gate mock-commit dispatch blew past 300s and expired — sibling of worker_token TTL.
    1800
}

/// TTL cascade resolve helper (Blueprint metadata → server default fallback).
/// Second-stage fallback, called when the POST `/v1/tasks` body does not set `ttl_secs`.
/// (1) If BP metadata `default_run_ttl_secs` is `Some`, use it.
/// (2) If `None`, fall back to the server global `default_run_ttl()` (1800s).
///
/// # Full cascade (combined in `run_flow_form`)
///
/// - request body `ttl_secs=Some(v)` → v (this helper is not called)
/// - request body `None` + metadata `Some(v)` → v
/// - request body `None` + metadata `None` → `default_run_ttl()` = 1800s
#[cfg(test)]
fn resolve_ttl_from_metadata(metadata_ttl: Option<u64>) -> u64 {
    metadata_ttl.unwrap_or_else(default_run_ttl)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TTL cascade case 1: when the request body sets it, that value is used as-is
    /// (upper branch that does not go through the helper; semantic verify of the
    /// `Some(v) => v` direct-return path in `run_flow_form`).
    #[test]
    fn ttl_cascade_request_body_wins_over_metadata() {
        let req_ttl: Option<u64> = Some(100);
        let metadata_ttl: Option<u64> = Some(3600);
        let effective = match req_ttl {
            Some(v) => v,
            None => resolve_ttl_from_metadata(metadata_ttl),
        };
        assert_eq!(
            effective, 100,
            "request body ttl_secs=100 must win over metadata=3600 (cascade priority (1) > (2))"
        );
    }

    /// TTL cascade case 2: request body omitted + BP metadata `Some(N)` → `N` is effective.
    #[test]
    fn ttl_cascade_metadata_used_when_body_missing() {
        let req_ttl: Option<u64> = None;
        let metadata_ttl: Option<u64> = Some(3600);
        let effective = match req_ttl {
            Some(v) => v,
            None => resolve_ttl_from_metadata(metadata_ttl),
        };
        assert_eq!(
            effective, 3600,
            "body None + metadata=3600 must resolve to 3600 (cascade (2))"
        );
    }

    /// TTL cascade case 3: request body omitted + BP metadata `None` → server default (1800s).
    #[test]
    fn ttl_cascade_server_default_when_both_missing() {
        let req_ttl: Option<u64> = None;
        let metadata_ttl: Option<u64> = None;
        let effective = match req_ttl {
            Some(v) => v,
            None => resolve_ttl_from_metadata(metadata_ttl),
        };
        assert_eq!(
            effective,
            default_run_ttl(),
            "body None + metadata None must fall back to default_run_ttl() = 1800s"
        );
        assert_eq!(effective, 1800, "default_run_ttl() literal = 1800s");
    }

    /// Helper unit: metadata `None` → 1800 (server default expansion).
    #[test]
    fn resolve_ttl_from_metadata_none_returns_server_default() {
        assert_eq!(resolve_ttl_from_metadata(None), 1800);
    }

    /// Helper unit: metadata `Some(N)` → `N` (server default ignored).
    #[test]
    fn resolve_ttl_from_metadata_some_returns_value() {
        assert_eq!(resolve_ttl_from_metadata(Some(7200)), 7200);
        assert_eq!(resolve_ttl_from_metadata(Some(60)), 60);
    }

    // ──────────────────────────────────────────────────────────────────
    // `TaskLaunchRequest.check_policy` wire field (T5)
    // ──────────────────────────────────────────────────────────────────

    /// T5: a `POST /v1/tasks` body carrying a top-level `check_policy`
    /// deserializes into `TaskLaunchRequest.check_policy` using the
    /// snake_case wire form.
    #[test]
    fn task_launch_request_parses_check_policy_wire_field() {
        let body = json!({
            "blueprint": { "kind": "id", "id": "some-bp" },
            "init_ctx": {},
            "check_policy": "silent",
        });
        let req: TaskLaunchRequest =
            serde_json::from_value(body).expect("request must deserialize");
        assert_eq!(req.check_policy, Some(CheckPolicy::Silent));
    }

    /// A body that omits `check_policy` leaves the field `None` (existing
    /// clients are unaffected — `#[serde(default)]`).
    #[test]
    fn task_launch_request_check_policy_defaults_to_none_when_omitted() {
        let body = json!({
            "blueprint": { "kind": "id", "id": "some-bp" },
            "init_ctx": {},
        });
        let req: TaskLaunchRequest =
            serde_json::from_value(body).expect("request must deserialize");
        assert_eq!(req.check_policy, None);
    }

    // ──────────────────────────────────────────────────────────────────
    // issue #19 ST2: `build_task_input_spec_from_request` direct resolver
    // ──────────────────────────────────────────────────────────────────

    fn task_req(
        init_ctx: Value,
        project_root: Option<&str>,
        work_dir: Option<&str>,
        task_metadata: Option<Value>,
    ) -> TaskLaunchRequest {
        TaskLaunchRequest {
            blueprint: BlueprintRef::Id {
                id: mlua_swarm::blueprint::store::BlueprintId::new("ut"),
                version: Default::default(),
            },
            init_ctx,
            project_root: project_root.map(String::from),
            work_dir: work_dir.map(String::from),
            task_metadata,
            ttl_secs: None,
            operator: None,
            operator_sid: None,
            timeout_secs: None,
            goal: None,
            detach: false,
            check_policy: None,
        }
    }

    /// (a) Sibling fields only — no legacy keys in `init_ctx` — are
    /// returned in the `TaskInputSpec` unchanged. `init_ctx` itself is
    /// untouched by this resolver (checked separately at the call site).
    #[test]
    fn build_task_input_spec_from_request_returns_sibling_fields_when_present() {
        let req = task_req(
            json!({"free": "form"}),
            Some("/repo/sibling"),
            Some("/repo/sibling/work"),
            Some(json!({"issue": 19})),
        );
        let spec = build_task_input_spec_from_request(&req).expect("spec must be Some");
        assert_eq!(spec.project_root.as_deref(), Some("/repo/sibling"));
        assert_eq!(spec.work_dir.as_deref(), Some("/repo/sibling/work"));
        assert_eq!(spec.task_metadata, Some(json!({"issue": 19})));
    }

    /// (b) No sibling fields — the pre-#19 shape (same 3 keys nested
    /// inside `init_ctx`) is used as the fallback source.
    #[test]
    fn build_task_input_spec_from_request_falls_back_to_legacy_init_ctx_shape() {
        let req = task_req(
            json!({
                "project_root": "/repo/legacy",
                "work_dir": "/repo/legacy/work",
                "task_metadata": {"issue": 17},
            }),
            None,
            None,
            None,
        );
        let spec = build_task_input_spec_from_request(&req).expect("spec must be Some");
        assert_eq!(spec.project_root.as_deref(), Some("/repo/legacy"));
        assert_eq!(spec.work_dir.as_deref(), Some("/repo/legacy/work"));
        assert_eq!(spec.task_metadata, Some(json!({"issue": 17})));
    }

    /// (c) Both present — the sibling field must win over the legacy
    /// `init_ctx`-nested value.
    #[test]
    fn build_task_input_spec_from_request_sibling_wins_over_legacy_shape() {
        let req = task_req(
            json!({
                "project_root": "/repo/legacy",
                "work_dir": "/repo/legacy/work",
                "task_metadata": {"issue": 17},
            }),
            Some("/repo/sibling"),
            Some("/repo/sibling/work"),
            Some(json!({"issue": 19})),
        );
        let spec = build_task_input_spec_from_request(&req).expect("spec must be Some");
        assert_eq!(
            spec.project_root.as_deref(),
            Some("/repo/sibling"),
            "sibling field must win over the legacy init_ctx-nested value"
        );
        assert_eq!(spec.work_dir.as_deref(), Some("/repo/sibling/work"));
        assert_eq!(spec.task_metadata, Some(json!({"issue": 19})));
    }

    /// (d) All three fields absent from both sibling and legacy shapes —
    /// resolver returns `None`, and no middleware is layered downstream.
    #[test]
    fn build_task_input_spec_from_request_returns_none_when_no_fields_present() {
        let req = task_req(json!({"unrelated": "value"}), None, None, None);
        assert!(build_task_input_spec_from_request(&req).is_none());
    }

    /// Minimal `AppState` for the `status_get` handler-fn-direct-call test
    /// below — same construction shape as `tasks.rs::test_state()`
    /// (mirrors what `build_router_full` does internally, skipping the
    /// `Router` wrapper).
    fn status_test_state() -> AppState {
        let engine = Engine::new(mlua_swarm::EngineCfg::default());
        let compiler = mlua_swarm::Compiler::new(default_registry());
        let launch = Arc::new(mlua_swarm::TaskLaunchService::new(engine.clone(), compiler));
        AppState {
            engine,
            sessions: Arc::new(Mutex::new(SessionStore::default())),
            task_app: Arc::new(mlua_swarm::TaskApplication::new_inline_only(launch)),
            ws_operator_factory: None,
            data_store: Arc::new(mlua_swarm::store::output::InMemoryOutputStore::new()),
            operator_sessions: Arc::new(Mutex::new(HashMap::new())),
            roles_to_sid: Arc::new(Mutex::new(HashMap::new())),
            task_store: Arc::new(mlua_swarm::store::task::InMemoryTaskStore::new()),
            run_store: Arc::new(mlua_swarm::store::run::InMemoryRunStore::new()),
            base_url: None,
            sync_timeout_secs: 300,
        }
    }

    /// issue #35 ST4 Acceptance Criteria: `GET /v1/status` reports the
    /// count of `Running` `Run`s (`RunStore::list_running`) and attached
    /// Operator ids (`engine.list_operator_ids()`), called directly as a
    /// handler fn (no `Router` wrapper — this crate's established
    /// unit-test convention).
    #[tokio::test]
    async fn status_get_reports_running_runs_and_operators() {
        let state = status_test_state();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        state
            .run_store
            .create(RunRecord {
                id: RunId::new(),
                task_id: TaskId::new(),
                status: RunStatus::Running,
                step_entries: Vec::new(),
                degradations: Vec::new(),
                operator_sid: None,
                result_ref: None,
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("seed running RunRecord");

        // Throwaway `Operator` impl — only registration/list-count matters
        // for this test, `execute` is never dispatched (same idiom as
        // `tasks.rs::StallingOperator`).
        struct NoopOperator;
        #[async_trait::async_trait]
        impl mlua_swarm::Operator for NoopOperator {
            async fn execute(
                &self,
                _ctx: &mlua_swarm::Ctx,
                _system: Option<String>,
                _prompt: Value,
                _worker: Option<mlua_swarm::WorkerBinding>,
                _worker_token: mlua_swarm::CapToken,
            ) -> Result<mlua_swarm::WorkerResult, mlua_swarm::WorkerError> {
                unimplemented!("not exercised by this test — only registration/list matters")
            }
        }
        state
            .engine
            .register_operator("test-op", Arc::new(NoopOperator))
            .await;

        let Json(resp) = status_get(State(state)).await;
        assert_eq!(resp.running_runs, 1);
        assert_eq!(resp.attached_operators, 1);
    }
}
