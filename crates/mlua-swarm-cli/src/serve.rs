//! mse serve CLI: takes startup args with clap, constructs the Engine, assembles
//! an axum Router via the library's `build_router`, and binds & serves it.
//!
//! During the current period the server is **fixed to combined mode with enhance**
//! (= mlua-swarm's essential property is running task + enhance + Operator dispatch
//! in one process side-by-side). The old `--mode` switching flag has been removed
//! (= the dolphin split mode will be decided on re-introduction when going Prod).
//! All routes are served:
//! `/v1/tasks` / `/v1/operators` (WS login flow) / `/v1/blueprints` / `/v1/issues` /
//! `/v1/enhance-settings` / `/v1/worker/*`.

use clap::Parser;
use mlua_swarm::blueprint::store::{
    blueprint_version, BlueprintId, BlueprintStore, CommitMetadata, Git2BlueprintStore,
};
use mlua_swarm::blueprint::{
    current_schema_version, AgentDef, AgentKind, Blueprint, BlueprintMetadata, BlueprintOrigin,
    CompilerHints, CompilerStrategy,
};
use mlua_swarm::store::enhance_log::{
    EnhanceLogStore, InMemoryEnhanceLogStore, SqliteEnhanceLogStore,
};
use mlua_swarm::store::enhance_setting::{
    EnhanceSettingId, EnhanceSettingStore, InMemoryEnhanceSettingStore, SqliteEnhanceSettingStore,
};
use mlua_swarm::store::issue::{InMemoryIssueStore, IssueStore, SqliteIssueStore};
use mlua_swarm::store::operator_session::{
    InMemoryOperatorSessionStore, OperatorSessionStore, SqliteOperatorSessionStore,
};
use mlua_swarm::store::output::{InMemoryOutputStore, OutputStore, SqliteOutputStore};
use mlua_swarm::store::replay::{InMemoryReplayStore, ReplayStore, SqliteReplayStore};
use mlua_swarm::store::run::{InMemoryRunStore, RunStore, SqliteRunStore};
use mlua_swarm::store::task::{InMemoryTaskStore, SqliteTaskStore, TaskStore};
use mlua_swarm::{
    AgentBlockInProcessSpawnerFactory, LuaInProcessSpawnerFactory, OperatorSpawnerFactory,
    RustFnInProcessSpawnerFactory, SpawnerRegistry, SubprocessProcessSpawnerFactory,
};
use mlua_swarm::{
    Compiler, Engine, EngineCfg, EnhanceApplication, EnhanceApplicationConfig, Role,
    TaskLaunchService,
};
use mlua_swarm_server::{
    build_blueprints_router_with_refs, build_enhance_log_router, build_enhance_settings_router,
    build_issues_router, default_registry_with_enhance_flow,
    doctor::{build_doctor_router, DoctorInfo},
};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(about = "Run the HTTP server (mse serve).")]
pub struct Args {
    /// Path to the TOML config file. Precedence: CLI flag > config file > built-in
    /// default. Defaults to `~/.mse/config.toml`; a missing file is not an error
    /// (built-in defaults apply). See `mlua_swarm_server::config` module doc.
    #[arg(long)]
    config: Option<std::path::PathBuf>,
    /// listen address. Overrides the config file's `bind`.
    #[arg(long)]
    bind: Option<String>,
    /// Token signing secret (hex). Overrides the config file's `token_secret`.
    /// When both are omitted, uses the default current secret.
    #[arg(long)]
    token_secret: Option<String>,
    /// Seed Blueprint id for enhance mode. Overrides the config file's `seed_blueprint_id`.
    #[arg(long)]
    seed_blueprint_id: Option<String>,
    /// Root path for the git-backed `BlueprintStore` (when omitted, uses the
    /// config file's `git_store_path`, then `~/.mse/store`). If the path does
    /// not exist, `init`; if an existing repo, `open` (= if the seed already
    /// exists, skip). The store is always git-backed and persistent; this flag
    /// only overrides where the repos live.
    #[arg(long)]
    git_store_path: Option<std::path::PathBuf>,
    /// Path to the SQLite database file backing the `IssueStore`. When omitted
    /// (and absent from the config file), falls back to the process-volatile
    /// `InMemoryIssueStore`. Overrides the config file's `issue_store_path`.
    #[arg(long)]
    issue_store_path: Option<std::path::PathBuf>,
    /// Path to the SQLite database file backing the `EnhanceSettingStore`.
    /// Omit for the in-memory default. Overrides
    /// `enhance_setting_store_path` in the config file.
    #[arg(long)]
    enhance_setting_store_path: Option<std::path::PathBuf>,
    /// Path to the SQLite database file backing the `EnhanceLogStore`.
    /// Omit for the in-memory default. Overrides `enhance_log_store_path`
    /// in the config file.
    #[arg(long)]
    enhance_log_store_path: Option<std::path::PathBuf>,
    /// Path to the SQLite database file backing the `OutputStore`. Omit for
    /// the in-memory default. Overrides `output_store_path` in the config
    /// file.
    #[arg(long)]
    output_store_path: Option<std::path::PathBuf>,
    /// Path to the SQLite database file backing the `TaskStore` (issue #13
    /// ID-hierarchy `POST /v1/tasks` work-item records). Persisted by
    /// default even when omitted (issue #35 ST1): falls back to
    /// `~/.mse/store/task.sqlite` unless `--ephemeral` is set. Overrides
    /// `task_store_path` in the config file, and always wins over
    /// `--ephemeral` / the persist-by-default when set.
    #[arg(long)]
    task_store_path: Option<std::path::PathBuf>,
    /// Path to the SQLite database file backing the `RunStore` (one kick of
    /// a Task). Persisted by default even when omitted (issue #35 ST1):
    /// falls back to `~/.mse/store/run.sqlite` unless `--ephemeral` is set.
    /// Overrides `run_store_path` in the config file, and always wins over
    /// `--ephemeral` / the persist-by-default when set.
    #[arg(long)]
    run_store_path: Option<std::path::PathBuf>,
    /// Path to the SQLite database file backing the `ReplayStore` (per-run
    /// Ctx-snapshot + step-output log). Persisted by default even when
    /// omitted (sibling of `--run-store-path`): falls back to
    /// `~/.mse/store/replay.sqlite` unless `--ephemeral` is set. Overrides
    /// `replay_store_path` in the config file, and always wins over
    /// `--ephemeral` / the persist-by-default when set.
    #[arg(long)]
    replay_store_path: Option<std::path::PathBuf>,
    /// Path to the SQLite database file backing the `OperatorSessionStore`
    /// (Operator login sessions — sid / 記名 / bearer digest; the bearer
    /// itself is never written, and the file is `0600` on unix). Persisted
    /// by default even when omitted (sibling of `--run-store-path`): falls
    /// back to `~/.mse/store/operator_session.sqlite` unless `--ephemeral`
    /// is set, so a server restart keeps logged-in Operators logged in.
    /// Overrides `operator_session_store_path` in the config file, and
    /// always wins over `--ephemeral` / the persist-by-default when set.
    #[arg(long)]
    operator_session_store_path: Option<std::path::PathBuf>,
    /// Merges the 4 enhance-flow workers (patch-spawner / patch-applier /
    /// verifier-router / committer) + 3 host bridges into `default_registry`.
    /// Used when running the default enhance Blueprint through `/v1/tasks`. A pure
    /// switch: absent = no override (defers to the config file / built-in default
    /// `false`); passing it always forces `true`.
    #[arg(long)]
    enable_enhance_flow: bool,
    /// Migration policy for deprecated `profile.worker_binding` Runner
    /// fallback: `allow` (default, compatibility) or `reject` (strict).
    #[arg(long, value_name = "allow|reject")]
    legacy_worker_binding_policy: Option<String>,
    /// Base dir for expanding `{"$file": ...}` / `{"$agent_md": ...}` refs found
    /// in `POST /v1/blueprints/:id` seed bodies. When omitted (and absent from the
    /// config file), ref expansion is disabled (= parses raw JSON). Used by the
    /// step 7 L4 smoke path where `agent.md` is embedded into the BP via `$agent_md`.
    /// Overrides the config file's `blueprint_ref_base`.
    #[arg(long)]
    blueprint_ref_base: Option<std::path::PathBuf>,
    /// Additional directory searched when resolving `$agent_md` /
    /// `$file` refs at server register time. Repeatable (tier 4 of the
    /// include cascade — see `mlua-swarm-compile::ResolveConfig`).
    /// Appended after CLI `--blueprint-ref-base` (legacy single dir)
    /// and before the config file's `blueprint_ref_includes` list.
    #[arg(long = "include", action = clap::ArgAction::Append, value_name = "DIR")]
    blueprint_ref_includes: Vec<std::path::PathBuf>,
    /// Reject `POST /v1/blueprints/:id` bodies that still carry raw
    /// `$file` / `$agent_md` refs (Phase 6, issue 4c4e3eb8). Design
    /// table row 3, strict opt-in — pushes ref resolution onto the
    /// client (`mse bp build --strict-embed`) so the server only ever
    /// sees pre-embedded Blueprint JSON. A pure switch: absent = no
    /// override (defers to the config file / built-in default `false`,
    /// which keeps the linker running server-side for
    /// backward-compat); passing it always forces `true`.
    /// Independent from the client-side `mse bp build --strict-embed`
    /// flag despite the shared token — that one pre-embeds at build
    /// time, this one rejects at register time; side-by-side
    /// comparison: `mse://guides/strict-embed-modes`.
    #[arg(long)]
    blueprint_strict_embed: bool,
    /// Opt-in: inject the server's public endpoint (base URL) into
    /// worker-facing data — the WS Spawn directive's `base_url` line and
    /// the `StepPointer.content_url` absolute-URL prefix. Default off:
    /// workers are never handed the server endpoint (the directive
    /// renders its placeholder; `content_url` stays a relative path) and
    /// reach the server via their own configured bind (e.g. the mse-mcp
    /// tools' `bind` parameter). Overrides the config file's
    /// `inject_endpoint_for_worker`.
    #[arg(long)]
    inject_endpoint_for_worker: bool,
    /// Install the observational LongHold layer as a base layer with
    /// this threshold in milliseconds. Every dispatched step whose
    /// completion time exceeds the threshold emits
    /// `Event::TaskAttemptCompleted { long_hold_warn: true, .. }` on
    /// the broadcast bus and appends a `mw.long_hold_warn` event to
    /// the persistent `RunTraceStore`. Purely observational — never
    /// alters the step signal or blocks completion. Omit / leave unset
    /// to skip the layer entirely (byte-for-byte compat with the
    /// pre-config shape).
    #[arg(long)]
    long_hold_warn_ms: Option<u64>,
    /// The (2) CLI override layer of the 4-tier cascade. Falls back when the BP
    /// top-level `default_agent_kind` JSON literal is absent; if that is also
    /// absent, the Schema-impl `Default` = `Operator` is used. The value is the
    /// snake_case form of the `AgentKind` enum (`operator` / `agent_block` /
    /// `rust_fn` / `lua` / `subprocess`). Example: `--default-agent-kind agent_block`.
    /// Overrides the config file's `default_agent_kind`.
    #[arg(long)]
    default_agent_kind: Option<String>,
    /// Ceiling (seconds) for the `POST /v1/tasks` synchronous launch await
    /// (GH #33 Guard 2). Per-request `timeout_secs` in the request body
    /// takes priority; this is the server-wide fallback. Overrides the
    /// config file's `sync_timeout_secs`; built-in default is 3600s (60 min).
    #[arg(long)]
    sync_timeout_secs: Option<u64>,
    /// How often (seconds) the `operator-session-expiry` job sweeps Operator
    /// login sessions past model §4.1's 24h horizon. This is the sweep's
    /// period, **not** the horizon: turning it down does not expire sessions
    /// sooner, it only shortens how long a release waits (and every read of a
    /// session applies the horizon regardless). Overrides the config file's
    /// `operator_session_sweep_secs`; built-in default is 300s. `0` leaves
    /// the job registered but unscheduled, which `GET /v1/status` reports as
    /// `enabled: false`.
    #[arg(long)]
    operator_session_sweep_secs: Option<u64>,
    /// R4 lock-hold guard threshold (milliseconds) for the engine: how
    /// long a single `Engine::with_state` closure may hold the state
    /// lock before the engine reports a suspected long operation inside
    /// the lock. Overrides the config file's `engine_max_hold_ms`; when
    /// both are omitted the engine's built-in default (50ms) stands.
    #[arg(long)]
    engine_max_hold_ms: Option<u64>,
    /// TTL (seconds) for the worker capability tokens the engine mints and
    /// hands to SubAgents. A Step whose SubAgent runs longer than this
    /// fails authentication mid-flight, so raise it alongside the run TTL
    /// when running long Steps. Overrides the config file's
    /// `worker_token_ttl_secs`; when both are omitted the engine's built-in
    /// default (1800s / 30 min) stands. `0` is rejected at startup — it would
    /// mint worker tokens that are already expired.
    #[arg(long)]
    worker_token_ttl_secs: Option<u64>,
    /// Opt-out of the persist-by-default `TaskStore`/`RunStore` (issue #35
    /// ST1): restores the previous InMemory default. Has no effect when an
    /// explicit `--task-store-path`/`--run-store-path` (or the config
    /// file's equivalent) is set — explicit paths always win over both
    /// `--ephemeral` and the persist-by-default. Mirrors the config file's
    /// `ephemeral`. A pure switch: absent = no override (defers to the
    /// config file / built-in default `false`); passing it always forces
    /// `true`.
    #[arg(long)]
    ephemeral: bool,
    /// Server-wide `CheckPolicy` for submit-time projection sinks.
    /// One of `silent` / `warn` / `strict`
    /// (snake_case). Overrides the config file's `check_policy`. When
    /// omitted (and absent from the config file), falls back to `warn`
    /// (byte-identical to the pre-`CheckPolicy` fail-open behaviour).
    /// `strict` returns `EngineError::CheckPolicyStrict` from the sink
    /// so a caller can fail-loud instead of proceeding with a
    /// partially-realized submission. Per-task
    /// `TaskSpec.check_policy` (set via caller code) wins over this
    /// server-wide value.
    #[arg(long)]
    check_policy: Option<String>,
}

fn parse_agent_kind_cli(s: &str) -> Result<mlua_swarm::blueprint::AgentKind, String> {
    serde_json::from_value(serde_json::Value::String(s.to_string()))
        .map_err(|e| format!("invalid --default-agent-kind {s:?}: {e}"))
}

fn parse_check_policy_cli(s: &str) -> Result<mlua_swarm::core::config::CheckPolicy, String> {
    serde_json::from_value(serde_json::Value::String(s.to_string()))
        .map_err(|e| format!("invalid --check-policy {s:?}: {e}"))
}

fn parse_legacy_worker_binding_policy_cli(
    s: &str,
) -> Result<mlua_swarm::LegacyWorkerBindingPolicy, String> {
    serde_json::from_value(serde_json::Value::String(s.to_string()))
        .map_err(|e| format!("invalid --legacy-worker-binding-policy {s:?}: {e}"))
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    let config_path = args
        .config
        .clone()
        .unwrap_or_else(mlua_swarm_server::config::default_config_path);
    let file_config = mlua_swarm_server::config::load_file_config(&config_path)
        .unwrap_or_else(|e| panic!("mse serve: config load failed: {e}"));
    let cli_overrides = mlua_swarm_server::config::CliOverrides {
        bind: args.bind.clone(),
        enable_enhance_flow: if args.enable_enhance_flow {
            Some(true)
        } else {
            None
        },
        legacy_worker_binding_policy: args.legacy_worker_binding_policy.as_ref().map(|s| {
            parse_legacy_worker_binding_policy_cli(s).unwrap_or_else(|e| panic!("mse serve: {e}"))
        }),
        blueprint_ref_base: args.blueprint_ref_base.clone(),
        blueprint_ref_includes: args.blueprint_ref_includes.clone(),
        blueprint_strict_embed: if args.blueprint_strict_embed {
            Some(true)
        } else {
            None
        },
        inject_endpoint_for_worker: if args.inject_endpoint_for_worker {
            Some(true)
        } else {
            None
        },
        long_hold_warn_ms: args.long_hold_warn_ms,
        git_store_path: args.git_store_path.clone(),
        issue_store_path: args.issue_store_path.clone(),
        enhance_setting_store_path: args.enhance_setting_store_path.clone(),
        enhance_log_store_path: args.enhance_log_store_path.clone(),
        output_store_path: args.output_store_path.clone(),
        task_store_path: args.task_store_path.clone(),
        run_store_path: args.run_store_path.clone(),
        replay_store_path: args.replay_store_path.clone(),
        operator_session_store_path: args.operator_session_store_path.clone(),
        ephemeral: if args.ephemeral { Some(true) } else { None },
        seed_blueprint_id: args.seed_blueprint_id.clone(),
        default_agent_kind: args.default_agent_kind.clone(),
        token_secret: args.token_secret.clone(),
        sync_timeout_secs: args.sync_timeout_secs,
        operator_session_sweep_secs: args.operator_session_sweep_secs,
        engine_max_hold_ms: args.engine_max_hold_ms,
        worker_token_ttl_secs: args.worker_token_ttl_secs,
        check_policy: args
            .check_policy
            .as_ref()
            .map(|s| parse_check_policy_cli(s).unwrap_or_else(|e| panic!("mse serve: {e}"))),
    };
    let cfg = mlua_swarm_server::config::resolve(cli_overrides, file_config)
        .unwrap_or_else(|e| panic!("mse serve: config resolve failed: {e}"));
    let default_agent_kind: Option<mlua_swarm::blueprint::AgentKind> = cfg
        .default_agent_kind
        .as_ref()
        .map(|s| parse_agent_kind_cli(s).unwrap_or_else(|e| panic!("mse serve: {e}")));
    eprintln!("mse serve: config loaded from {}", config_path.display());

    // Engine stateless-executor refactor:
    // A single Engine instance is used (the old task / enhance axis split
    // guarded against bind-state races that dispatch_attempt_with's
    // per-request spawner already prevents — no global-state race remains).
    // The Engine is built with a LayerRegistry so that
    // `Blueprint.spawner_hints` values ("main_ai" / "senior_escalation")
    // get wrapped into the SpawnerStack inside TaskLaunchService.
    let engine = Engine::new_with_layers(
        engine_cfg_from(&cfg),
        mlua_swarm_server::default_layer_registry_with(mlua_swarm_server::LayerOptions {
            long_hold_warn_ms: cfg.long_hold_warn_ms,
        }),
    );

    // The Operator callback registry is held directly on the engine
    // (state.engine is the SoT). On WS connect, the operator_ws handler
    // registers the session via state.engine.register_*.

    // Combined mode is fixed (running task + enhance + Operator side by side is mlua-swarm's essential property).

    // Store construction (always needed under combined mode). Always
    // git-backed: per-id repos are split under <root>/blueprints/<id>/.git/,
    // and EnhanceConfig lives under <root>/enhance-configs/<id>/.git/.
    // <root> defaults to ~/.mse/store (config/CLI only override location).
    let store: Arc<dyn BlueprintStore> = {
        let bp_root = cfg.git_store_path.join("blueprints");
        let s = Git2BlueprintStore::open_or_init(&bp_root).expect("git store open_or_init");
        eprintln!(
            "mse serve: blueprint store = Git2 root={} (per-id repos)",
            bp_root.display()
        );
        Arc::new(s)
    };

    // Seed (always runs — required under fixed combined mode).
    let id = BlueprintId::new(cfg.seed_blueprint_id.clone());
    let need_seed = store.read_head(&id).await.is_err();
    if need_seed {
        let bp = seed_blueprint(&cfg.seed_blueprint_id);
        let v0 = blueprint_version(&bp).expect("blueprint_version");
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        store
            .write_new(&id, &bp, &[], CommitMetadata::seed(id.clone(), v0, now_ms))
            .await
            .expect("seed write");
        eprintln!("mse serve: seeded blueprint_id={}", id.as_str());
    } else {
        eprintln!("mse serve: existing head found, skip seed");
    }

    // Build the SpawnerRegistry once and share the OperatorSpawnerFactory as
    // an Arc: hand the same Arc to both (a) the registry and (b) the
    // `WsOperatorWiring` below, which installs the slot resolver on it so a
    // compiled `kind = Operator` agent resolves its seat's current holder
    // per dispatch.
    let op_factory = Arc::new(OperatorSpawnerFactory::new());
    let make_registry = || -> SpawnerRegistry {
        let mut reg = if cfg.enable_enhance_flow {
            default_registry_with_enhance_flow()
        } else {
            // Reproduce default_registry, replacing only the OperatorSpawnerFactory with the shared Arc.
            let rustfn_factory = mlua_swarm::worker::baseline::extend_with_baseline(
                RustFnInProcessSpawnerFactory::new(),
            );
            let mut r = SpawnerRegistry::new();
            r.register::<SubprocessProcessSpawnerFactory>(Arc::new(
                SubprocessProcessSpawnerFactory,
            ));
            r.register::<RustFnInProcessSpawnerFactory>(Arc::new(rustfn_factory));
            // Same rationale as `default_registry` in `mlua-swarm-server`:
            // register an empty `LuaInProcessSpawnerFactory` so BPs on this
            // (non-enhance) path can still declare `kind: lua` via inline
            // `spec.source`. The enhance-flow branch above already carries
            // its own Lua factory (with the enhance-flow `fn_id`s baked in).
            r.register::<LuaInProcessSpawnerFactory>(Arc::new(LuaInProcessSpawnerFactory::new()));
            // GH #86: same rationale as `default_registry` — the stateless
            // AgentBlock factory makes `kind: agent_block` dispatchable on
            // the vanilla path; it carries no enhance-flow specialization
            // (that lives in the branch above's Lua `fn_id`s).
            r.register::<AgentBlockInProcessSpawnerFactory>(Arc::new(
                AgentBlockInProcessSpawnerFactory::new(),
            ));
            r.register::<OperatorSpawnerFactory>(op_factory.clone());
            r
        };
        // Even on the enhance_flow path, overwrite the OperatorSpawnerFactory
        // with the shared Arc (drop the one default_registry_with_enhance_flow
        // built separately).
        reg.register::<OperatorSpawnerFactory>(op_factory.clone());
        reg
    };

    // Store backend selection.
    //
    // Each of the four stores (Issue / EnhanceSetting / EnhanceLog / Output)
    // picks a SQLite-backed impl when its `*_store_path` is set in the
    // resolved config; otherwise it falls back to the process-volatile
    // in-memory default. The `AsyncIsleDriver` handles are collected into
    // `isle_drivers` and drained on shutdown so their SQLite threads join
    // cleanly instead of racing process exit.
    let mut isle_drivers: Vec<rusqlite_isle::AsyncIsleDriver> = Vec::new();

    let issue_store: Arc<dyn IssueStore> = match &cfg.issue_store_path {
        Some(path) => {
            eprintln!("mse serve: SqliteIssueStore at {}", path.display());
            let (s, driver) = SqliteIssueStore::open(path)
                .await
                .unwrap_or_else(|e| panic!("mse serve: SqliteIssueStore open failed: {e}"));
            isle_drivers.push(driver);
            Arc::new(s)
        }
        None => Arc::new(InMemoryIssueStore::new()),
    };
    let setting_store: Arc<dyn EnhanceSettingStore> = match &cfg.enhance_setting_store_path {
        Some(path) => {
            eprintln!("mse serve: SqliteEnhanceSettingStore at {}", path.display());
            let (s, driver) = SqliteEnhanceSettingStore::open(path)
                .await
                .unwrap_or_else(|e| {
                    panic!("mse serve: SqliteEnhanceSettingStore open failed: {e}")
                });
            isle_drivers.push(driver);
            Arc::new(s)
        }
        None => Arc::new(InMemoryEnhanceSettingStore::new()),
    };
    let log_store: Arc<dyn EnhanceLogStore> = match &cfg.enhance_log_store_path {
        Some(path) => {
            eprintln!("mse serve: SqliteEnhanceLogStore at {}", path.display());
            let (s, driver) = SqliteEnhanceLogStore::open(path)
                .await
                .unwrap_or_else(|e| panic!("mse serve: SqliteEnhanceLogStore open failed: {e}"));
            isle_drivers.push(driver);
            Arc::new(s)
        }
        None => Arc::new(InMemoryEnhanceLogStore::new()),
    };
    let output_store: Option<Arc<dyn OutputStore>> = match &cfg.output_store_path {
        Some(path) => {
            eprintln!("mse serve: SqliteOutputStore at {}", path.display());
            let (s, driver) = SqliteOutputStore::open(path)
                .await
                .unwrap_or_else(|e| panic!("mse serve: SqliteOutputStore open failed: {e}"));
            isle_drivers.push(driver);
            Some(Arc::new(s))
        }
        // Explicit `InMemoryOutputStore` construction here (rather than
        // leaving `output_store = None` and letting the router build one)
        // keeps the branch symmetric with the other three stores.
        None => Some(Arc::new(InMemoryOutputStore::new())),
    };
    let task_store: Arc<dyn TaskStore> = match &cfg.task_store_path {
        Some(path) => {
            eprintln!("mse serve: SqliteTaskStore at {}", path.display());
            let (s, driver) = SqliteTaskStore::open(path)
                .await
                .unwrap_or_else(|e| panic!("mse serve: SqliteTaskStore open failed: {e}"));
            isle_drivers.push(driver);
            Arc::new(s)
        }
        None => Arc::new(InMemoryTaskStore::new()),
    };
    let run_store: Arc<dyn RunStore> = match &cfg.run_store_path {
        Some(path) => {
            eprintln!("mse serve: SqliteRunStore at {}", path.display());
            let (s, driver) = SqliteRunStore::open(path)
                .await
                .unwrap_or_else(|e| panic!("mse serve: SqliteRunStore open failed: {e}"));
            isle_drivers.push(driver);
            Arc::new(s)
        }
        None => Arc::new(InMemoryRunStore::new()),
    };
    // The Operator wiring, in one value: the shared `OperatorSpawnerFactory`
    // (already inside `make_registry()`'s SpawnerRegistry) gains the slot
    // resolver that turns every Blueprint-declared Operator seat into an
    // `AssigneeRouter` over THIS run store, and the adapter registry those
    // routers resolve holders through is the one the login path registers
    // sessions into. Built here, after `run_store`, because the routers need
    // it: `Run.current` is the single place a seat's holder is written and
    // read (model A10).
    let ws_operator =
        mlua_swarm_server::WsOperatorWiring::new(op_factory.clone(), run_store.clone());

    let replay_store: Arc<dyn ReplayStore> = match &cfg.replay_store_path {
        Some(path) => {
            eprintln!("mse serve: SqliteReplayStore at {}", path.display());
            let (s, driver) = SqliteReplayStore::open(path)
                .await
                .unwrap_or_else(|e| panic!("mse serve: SqliteReplayStore open failed: {e}"));
            isle_drivers.push(driver);
            Arc::new(s)
        }
        None => Arc::new(InMemoryReplayStore::new()),
    };
    let operator_session_store: Arc<dyn OperatorSessionStore> = match &cfg
        .operator_session_store_path
    {
        Some(path) => {
            eprintln!(
                "mse serve: SqliteOperatorSessionStore at {}",
                path.display()
            );
            let (s, driver) = SqliteOperatorSessionStore::open(path)
                .await
                .unwrap_or_else(|e| {
                    panic!("mse serve: SqliteOperatorSessionStore open failed: {e}")
                });
            isle_drivers.push(driver);
            Arc::new(s)
        }
        None => Arc::new(InMemoryOperatorSessionStore::new()),
    };
    // Issue #8: source the public base URL from the same bind the
    // listener will use, so `WSOperatorSession` can render it into
    // Spawn directives literally (no example port drift). Since the
    // `inject_endpoint_for_worker` opt-in this is OFF by default —
    // `None` makes the directive render its historical placeholder and
    // keeps `StepPointer.content_url` a relative path, so the server
    // endpoint is never handed to workers unless explicitly requested
    // (`--inject-endpoint-for-worker` / config `inject_endpoint_for_worker`).
    // Resolved here, ahead of the session restore below, because a
    // restored session is built with the same base URL a freshly
    // connected one gets.
    let base_url: Option<std::sync::Arc<str>> = if cfg.inject_endpoint_for_worker {
        Some(format!("http://{}", cfg.bind).into())
    } else {
        None
    };

    // Rehydrate persisted Operator login sessions so a restart keeps every
    // logged-in Operator logged in: their saved sid + token reconnect the
    // WS directly (no re-mint), and persisted `RunRecord.operator_sid`
    // pins stay resolvable instead of stranding on `404 unknown sid`.
    // The call also registers each restored session with the engine and as
    // an adapter in the wiring's registry, so a Run still holding one of
    // those sids can be dispatched from boot rather than only after the
    // owning client's WS reconnects.
    let operator_session_persistence = mlua_swarm_server::OperatorSessionPersistence::restore(
        operator_session_store,
        &engine,
        Some(&ws_operator),
        base_url.clone(),
    )
    .await
    .unwrap_or_else(|e| panic!("mse serve: operator session restore failed: {e}"));
    if !operator_session_persistence.prepared.is_empty() {
        eprintln!(
            "mse serve: restored {} operator session(s) (reconnect with the saved sid + token)",
            operator_session_persistence.prepared.len()
        );
    }
    // The trace rail shares the Run store's database FILE (one Run = one
    // artifact on disk) in its own `run_trace` table — see
    // `mlua_swarm::store::trace::sqlite`. Ephemeral mode (no
    // run_store_path) keeps the whole rail in-memory.
    let run_trace_store: Arc<dyn mlua_swarm::store::trace::RunTraceStore> =
        match &cfg.run_store_path {
            Some(path) => {
                eprintln!("mse serve: SqliteRunTraceStore at {}", path.display());
                let (s, driver) = mlua_swarm::store::trace::SqliteRunTraceStore::open(path)
                    .await
                    .unwrap_or_else(|e| panic!("mse serve: SqliteRunTraceStore open failed: {e}"));
                isle_drivers.push(driver);
                Arc::new(s)
            }
            None => Arc::new(mlua_swarm::store::trace::InMemoryRunTraceStore::new()),
        };

    recover_interrupted_runs(&task_store, &run_store, &replay_store).await;

    // B-4 graceful shutdown drain: keep handles to the Task/Run stores
    // before they are moved into the router below, so the post-serve drain
    // can mark any still-`Running` Run `Interrupted` (they are `Arc`s, so
    // this is a refcount bump, not a second store).
    let shutdown_task_store = task_store.clone();
    let shutdown_run_store = run_store.clone();

    // Router assembly (fixed combined mode): merges task, the Operator wiring, and every enhance route.
    // The second element owns the process's periodic jobs — it is held for
    // the whole of `serve` and shut down beside the enhance loop below,
    // because dropping it stops them.
    let (mut app, periodic_jobs) =
        mlua_swarm_server::build_router_full_with_operator_session_persistence(
            engine.clone(),
            make_registry(),
            Some(store.clone()),
            Some(ws_operator),
            output_store,
            base_url,
            Some(task_store),
            Some(run_store),
            Some(replay_store),
            Some(run_trace_store),
            Some(operator_session_persistence),
            cfg.sync_timeout_secs,
            cfg.legacy_worker_binding_policy,
            cfg.operator_session_sweep_secs,
        );
    for job in periodic_jobs.snapshot() {
        eprintln!(
            "mse serve: periodic job {} {}",
            job.name,
            if job.enabled {
                format!("every {}s", job.period_secs)
            } else {
                "disabled (period 0; expiry still applies at every read)".to_string()
            }
        );
    }

    let compiler = Compiler::new(make_registry());
    let launch_enhance = Arc::new(
        TaskLaunchService::new(engine.clone(), compiler)
            .with_legacy_worker_binding_policy(cfg.legacy_worker_binding_policy),
    );

    let enhance_app = Arc::new(EnhanceApplication::new(
        EnhanceApplicationConfig {
            name: "enhance".into(),
            setting_id: EnhanceSettingId::default_id(),
            operator_id: "mse-enhance".into(),
            role: Role::Operator,
        },
        issue_store.clone(),
        setting_store.clone(),
        store.clone(),
        log_store.clone(),
        launch_enhance,
    ));

    let enhance_loop = tokio::spawn(enhance_app.clone().run_forever(Duration::from_millis(100)));

    let doctor_info = DoctorInfo {
        // The `mse serve` process IS this binary, so its own crate version
        // is the authoritative "what vintage is actually running" answer.
        server_version: env!("CARGO_PKG_VERSION").to_string(),
        bind: cfg.bind.to_string(),
        blueprint_backend: "git2".into(),
        blueprint_store_root: Some(cfg.git_store_path.join("blueprints").display().to_string()),
        blueprint_ref_base: cfg
            .blueprint_ref_base
            .as_ref()
            .map(|p| p.display().to_string()),
        enhance_flow_enabled: cfg.enable_enhance_flow,
        legacy_worker_binding_policy: cfg.legacy_worker_binding_policy,
        seed_blueprint_id: cfg.seed_blueprint_id.clone(),
        check_policy: cfg.check_policy,
    };

    app = app
        .merge(build_issues_router(issue_store.clone()))
        .merge(build_blueprints_router_with_refs(
            store.clone(),
            cfg.blueprint_ref_base.clone(),
            cfg.blueprint_ref_includes.clone(),
            default_agent_kind,
            cfg.blueprint_strict_embed,
            cfg.legacy_worker_binding_policy,
        ))
        .merge(build_enhance_log_router(log_store.clone()))
        .merge(build_enhance_settings_router(
            setting_store.clone(),
            store.clone(),
        ))
        .merge(build_doctor_router(doctor_info, store.clone()));

    let _ = id;

    eprintln!(
        "mse serve: combined mode (task+enhance+operator) listening on http://{}",
        cfg.bind
    );
    let listener = tokio::net::TcpListener::bind(cfg.bind).await.expect("bind");
    // B-4: graceful shutdown. `with_graceful_shutdown` stops accepting new
    // connections when the signal fires but lets in-flight requests finish
    // draining, instead of the old `tokio::select!` shape that dropped the
    // `serve` future outright (which reset in-flight HTTP to an empty reply
    // — curl exit 52). The shutdown future selects the same two signals as
    // before; `wait_sigterm` keeps its existing `#[cfg(unix)]` gate so the
    // Windows build stays clean (no unconditional `tokio::signal::unix`).
    let shutdown_signal = async {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => { eprintln!("mse serve: ctrl-c, shutting down"); }
            _ = wait_sigterm() => { eprintln!("mse serve: SIGTERM, shutting down"); }
        }
    };
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await
        .expect("serve");
    enhance_loop.abort();
    // Same reason the enhance loop is aborted here: the drain has finished,
    // so a sweep starting now would be walking state on its way out. (Drop
    // would do it too — this is the explicit half of that contract.)
    periodic_jobs.shutdown();
    // B-4: mark any Run still `Running` after the drain `Interrupted`, so a
    // restart does not leave it stranded `Running` forever. Runs BEFORE the
    // isle drivers are drained below — it writes through the SQLite stores.
    interrupt_running_on_shutdown(&shutdown_task_store, &shutdown_run_store).await;
    // Drain SQLite isle drivers (drops queued jobs, joins the SQLite thread).
    // Errors are logged but do not fail shutdown — the process is exiting.
    for driver in isle_drivers {
        if let Err(e) = driver.shutdown().await {
            eprintln!("mse serve: isle driver shutdown error: {e}");
        }
    }
    Ok(())
}

/// Build the [`EngineCfg`] the server runs with from the resolved config.
///
/// A free fn (rather than the closure it grew out of) so the config →
/// engine mapping is unit-testable on its own: `token_secret` is decoded
/// here, `check_policy` is carried through, and `engine_max_hold_ms` /
/// `worker_token_ttl_secs` override their engine counterparts only when
/// actually set — absent keys leave `EngineCfg::default()`'s 50ms / 1800s
/// in place.
fn engine_cfg_from(cfg: &mlua_swarm_server::config::ResolvedConfig) -> EngineCfg {
    let mut c = EngineCfg::default();
    if let Some(hex_secret) = &cfg.token_secret {
        c.token_secret = hex::decode(hex_secret).expect("token-secret must be hex");
    }
    c.check_policy = cfg.check_policy;
    if let Some(ms) = cfg.engine_max_hold_ms {
        c.max_hold_ms = ms as u128;
    }
    if let Some(secs) = cfg.worker_token_ttl_secs {
        c.worker_token_ttl_secs = secs;
    }
    c
}

/// Boot-time recovery sweep (issue #35): any Run left `Running` from a
/// previous process (crash / supervisor restart) is marked `Interrupted`
/// with a structured reason; the owning Task is marked `Interrupted`
/// likewise. Terminal-only — never touches `EngineState`, never
/// re-dispatches. Only meaningful when the store is persistent; on a
/// fresh `InMemoryRunStore` this is always a no-op (nothing survives to
/// sweep).
///
/// After each Interrupted mark the replay log for the run is consulted
/// via `ReplayStore::list_by_run`. A run counts as a **resumable
/// candidate** when it has at least one replay entry OR a persisted
/// launch-input snapshot (`RunRecord.input_json` — `run_resume` can
/// re-dispatch from scratch off the snapshot even at 0 replayed steps);
/// such runs are emitted at `tracing::info!` level so the attached
/// operator can kick `POST /v1/runs/<id>/resume` under the same `run_id`
/// (state-driven resume endpoint). A run with neither is not resumable,
/// but is still logged at `info!` (not `debug!`) so the orphan stays
/// visible at the default log level. This function itself never
/// auto-respawns: an
/// operator that has not attached would burn its TTL for nothing, so the
/// actual resume kick is left to the operator (per User direction —
/// boot-time auto-respawn is deferred to a separate issue).
///
/// Replay-store failures follow the same best-effort discipline as the
/// per-run store updates above: a warning is emitted and the sweep
/// continues; a single `list_by_run` error must not stall the boot path.
async fn recover_interrupted_runs(
    task_store: &std::sync::Arc<dyn mlua_swarm::store::task::TaskStore>,
    run_store: &std::sync::Arc<dyn mlua_swarm::store::run::RunStore>,
    replay_store: &std::sync::Arc<dyn mlua_swarm::store::replay::ReplayStore>,
) {
    let running = match run_store.list_running().await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("mse serve: boot sweep: list_running failed: {e}");
            return;
        }
    };
    for run in running {
        mark_run_interrupted(task_store, run_store, &run, "server restart", "boot sweep").await;

        // Classify each Interrupted run as resumable / non-resumable. A run
        // is resumable via either axis: at least one replay entry (an
        // in-place replay resume) OR a persisted launch-input snapshot
        // (`RunRecord.input_json` — `run_resume` rebuilds the input and
        // re-dispatches from scratch even at 0 replayed steps). Both are
        // surfaced at info! so the operator can see every orphan that
        // `POST /v1/runs/<id>/resume` can still recover; only a run with
        // neither is truly non-resumable — and even that is emitted at
        // info! (not debug!) so the orphan stays visible at the default
        // log level rather than disappearing. Failures are logged as
        // warn! and skipped so a single lookup error cannot stall the
        // whole sweep.
        match replay_store.list_by_run(&run.id).await {
            Ok(entries) => {
                let replayed_steps = entries.len();
                if replayed_steps > 0 || run.input_json.is_some() {
                    tracing::info!(
                        run_id = %run.id,
                        task_id = %run.task_id,
                        replayed_steps,
                        resume_url = %format!("POST /v1/runs/{}/resume", run.id),
                        "boot sweep: resumable Interrupted run"
                    );
                } else {
                    tracing::info!(
                        run_id = %run.id,
                        task_id = %run.task_id,
                        "boot sweep: not resumable (no replay entries, no input snapshot)"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    run_id = %run.id,
                    task_id = %run.task_id,
                    error = %e,
                    "boot sweep: replay_store list_by_run failed; skipping resumable classification"
                );
            }
        }
    }
}

/// Mark a single `Running` run (and its owning Task) `Interrupted` with a
/// structured `{"error": <reason>}` result envelope. Shared by the
/// boot-time recovery sweep ([`recover_interrupted_runs`], `reason =
/// "server restart"`) and the graceful-shutdown drain
/// ([`interrupt_running_on_shutdown`], `reason = "server shutdown"`) so
/// both stamp the identical terminal shape. Best-effort: every store error
/// is logged (prefixed with `context`) and swallowed — neither lifecycle
/// path can fail on a persistence hiccup while the process is starting or
/// exiting.
async fn mark_run_interrupted(
    task_store: &std::sync::Arc<dyn mlua_swarm::store::task::TaskStore>,
    run_store: &std::sync::Arc<dyn mlua_swarm::store::run::RunStore>,
    run: &mlua_swarm::store::run::RunRecord,
    reason: &str,
    context: &str,
) {
    let envelope = serde_json::json!({ "error": reason });
    if let Err(e) = run_store.set_result(&run.id, envelope).await {
        eprintln!(
            "mse serve: {context}: run {} set_result failed: {e}",
            run.id
        );
    }
    if let Err(e) = run_store
        .update_status(&run.id, mlua_swarm::store::run::RunStatus::Interrupted)
        .await
    {
        eprintln!(
            "mse serve: {context}: run {} update_status failed: {e}",
            run.id
        );
    }
    if let Err(e) = task_store
        .update_status(
            &run.task_id,
            mlua_swarm::store::task::TaskRecordStatus::Interrupted,
        )
        .await
    {
        eprintln!(
            "mse serve: {context}: task {} update_status failed: {e}",
            run.task_id
        );
    }
}

/// Graceful-shutdown drain (B-4): after `axum::serve`'s
/// `with_graceful_shutdown` future has resolved (all in-flight HTTP
/// drained), any Run still `Running` belonged to a synchronous dispatch
/// whose handler future the drain let finish — or a detached driver the
/// process is about to drop. Mark each `Interrupted` (same terminal shape
/// as the boot sweep) so it does not linger `Running` forever across the
/// restart, leaving a resumable orphan the next boot sweep / operator can
/// pick up via `POST /v1/runs/<id>/resume`. Only meaningful with a
/// persistent store; a fresh `InMemoryRunStore` has nothing that survives
/// the process. Must run BEFORE the isle SQLite drivers are drained (it
/// writes through them).
async fn interrupt_running_on_shutdown(
    task_store: &std::sync::Arc<dyn mlua_swarm::store::task::TaskStore>,
    run_store: &std::sync::Arc<dyn mlua_swarm::store::run::RunStore>,
) {
    let running = match run_store.list_running().await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("mse serve: shutdown drain: list_running failed: {e}");
            return;
        }
    };
    for run in running {
        mark_run_interrupted(
            task_store,
            run_store,
            &run,
            "server shutdown",
            "shutdown drain",
        )
        .await;
    }
}

/// Awaits `SIGTERM` (Unix). `launchctl bootout` sends `SIGTERM` to request a
/// graceful shutdown, so this is that handler's registration point (see
/// (see the server-lifecycle design). If the
/// signal handler itself fails to install, this future never resolves so
/// `tokio::select!` falls back to the other two arms (ctrl_c / serve).
///
/// On non-Unix targets (Windows) `SIGTERM` does not exist; this future
/// simply never resolves so the same `tokio::select!` falls through to the
/// `ctrl_c` arm.
#[cfg(unix)]
async fn wait_sigterm() {
    use tokio::signal::unix::{signal, SignalKind};
    match signal(SignalKind::terminate()) {
        Ok(mut sig) => {
            sig.recv().await;
        }
        Err(e) => {
            eprintln!("mse serve: failed to install SIGTERM handler: {e}");
            std::future::pending::<()>().await;
        }
    }
}

#[cfg(not(unix))]
async fn wait_sigterm() {
    std::future::pending::<()>().await;
}

fn seed_blueprint(id: &str) -> Blueprint {
    Blueprint {
        schema_version: current_schema_version(),
        id: id.into(),
        flow: serde_json::from_value(json!({
            "kind": "step",
            "ref": mlua_swarm::worker::baseline::AG_IDENTITY,
            "in": {"op": "lit", "value": "hello"},
            "out": {"op": "path", "at": "$.out"},
        }))
        .unwrap(),
        agents: vec![AgentDef {
            name: mlua_swarm::worker::baseline::AG_IDENTITY.into(),
            kind: AgentKind::RustFn,
            spec: json!({"fn_id": mlua_swarm::worker::baseline::AG_IDENTITY}),
            profile: None,
            meta: None,
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
            description: Some("mse serve enhance seed".into()),
            origin: BlueprintOrigin::Inline,
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

#[cfg(test)]
mod tests {
    use super::*;
    use mlua_swarm::store::replay::ReplayEntry;
    use mlua_swarm::store::run::{RunRecord, RunStatus};
    use mlua_swarm::store::task::{TaskRecord, TaskRecordStatus};
    use mlua_swarm::types::{RunId, TaskId};

    #[tokio::test]
    async fn recover_interrupted_runs_marks_running_as_interrupted() {
        let task_store: Arc<dyn TaskStore> = Arc::new(InMemoryTaskStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let replay_store: Arc<dyn ReplayStore> = Arc::new(InMemoryReplayStore::new());

        let running_task_id = TaskId::parse("T-running").unwrap();
        let done_task_id = TaskId::parse("T-done").unwrap();
        let running_run_id = RunId::parse("R-running").unwrap();
        let done_run_id = RunId::parse("R-done").unwrap();

        task_store
            .create(TaskRecord {
                id: running_task_id.clone(),
                goal: "resolve issue #35".into(),
                blueprint_ref: json!({}),
                input_ctx: json!({}),
                task_input_spec: None,
                status: TaskRecordStatus::Running,
                created_at: 1,
                updated_at: 1,
            })
            .await
            .unwrap();
        task_store
            .create(TaskRecord {
                id: done_task_id.clone(),
                goal: "unrelated done task".into(),
                blueprint_ref: json!({}),
                input_ctx: json!({}),
                task_input_spec: None,
                status: TaskRecordStatus::Done,
                created_at: 2,
                updated_at: 2,
            })
            .await
            .unwrap();

        run_store
            .create(RunRecord {
                id: running_run_id.clone(),
                task_id: running_task_id.clone(),
                status: RunStatus::Running,
                step_entries: vec![],
                degradations: vec![],
                operator_sid: None,
                current: Default::default(),
                next_generation: 0,
                result_ref: None,
                input_json: None,
                created_at: 1,
                updated_at: 1,
            })
            .await
            .unwrap();
        run_store
            .create(RunRecord {
                id: done_run_id.clone(),
                task_id: done_task_id.clone(),
                status: RunStatus::Done,
                step_entries: vec![],
                degradations: vec![],
                operator_sid: None,
                current: Default::default(),
                next_generation: 0,
                result_ref: None,
                input_json: None,
                created_at: 2,
                updated_at: 2,
            })
            .await
            .unwrap();

        recover_interrupted_runs(&task_store, &run_store, &replay_store).await;

        let running_run = run_store.get(&running_run_id).await.unwrap();
        assert_eq!(running_run.status, RunStatus::Interrupted);
        assert_eq!(
            running_run.result_ref,
            Some(json!({"error": "server restart"}))
        );
        let running_task = task_store.get(&running_task_id).await.unwrap();
        assert_eq!(running_task.status, TaskRecordStatus::Interrupted);

        // Control: the Done run/task pair is untouched.
        let done_run = run_store.get(&done_run_id).await.unwrap();
        assert_eq!(done_run.status, RunStatus::Done);
        assert_eq!(done_run.result_ref, None);
        let done_task = task_store.get(&done_task_id).await.unwrap();
        assert_eq!(done_task.status, TaskRecordStatus::Done);
    }

    /// The sweep classifies each Interrupted run as resumable /
    /// non-resumable based on the replay-log entry count. This test
    /// covers the pure sweep semantics (both runs get marked
    /// Interrupted, one has replay entries, one does not); the actual
    /// `tracing` field emission is exercised by the integration test in
    /// `mlua-swarm-server/tests/replay_e2e.rs` (which drives a real
    /// two-server-process partial-then-resume flow).
    #[tokio::test]
    async fn recover_interrupted_runs_classifies_by_replay_entries() {
        let task_store: Arc<dyn TaskStore> = Arc::new(InMemoryTaskStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
        let replay_store: Arc<dyn ReplayStore> = Arc::new(InMemoryReplayStore::new());

        let task_with_replay = TaskId::parse("T-resumable").unwrap();
        let task_without_replay = TaskId::parse("T-not-resumable").unwrap();
        let run_with_replay = RunId::parse("R-resumable").unwrap();
        let run_without_replay = RunId::parse("R-not-resumable").unwrap();

        for (tid, rid) in [
            (&task_with_replay, &run_with_replay),
            (&task_without_replay, &run_without_replay),
        ] {
            task_store
                .create(TaskRecord {
                    id: tid.clone(),
                    goal: "resume classification fixture".into(),
                    blueprint_ref: json!({}),
                    input_ctx: json!({}),
                    task_input_spec: None,
                    status: TaskRecordStatus::Running,
                    created_at: 1,
                    updated_at: 1,
                })
                .await
                .unwrap();
            run_store
                .create(RunRecord {
                    id: rid.clone(),
                    task_id: tid.clone(),
                    status: RunStatus::Running,
                    step_entries: vec![],
                    degradations: vec![],
                    operator_sid: None,
                    current: Default::default(),
                    next_generation: 0,
                    result_ref: None,
                    input_json: Some("{}".to_string()),
                    created_at: 1,
                    updated_at: 1,
                })
                .await
                .unwrap();
        }

        // Seed exactly one replay entry for the resumable run.
        replay_store
            .append(ReplayEntry {
                run_id: run_with_replay.clone(),
                step_ref: "step-a".into(),
                input_hash: "hash-a".into(),
                occurrence: 0,
                ctx_snapshot_json: "{}".into(),
                step_output_json: "{}".into(),
                created_at: 1,
            })
            .await
            .unwrap();

        recover_interrupted_runs(&task_store, &run_store, &replay_store).await;

        // Both runs are marked Interrupted regardless of replay-log
        // membership — the classification is orthogonal to the mark.
        let with_replay = run_store.get(&run_with_replay).await.unwrap();
        assert_eq!(with_replay.status, RunStatus::Interrupted);
        let without_replay = run_store.get(&run_without_replay).await.unwrap();
        assert_eq!(without_replay.status, RunStatus::Interrupted);

        // Sanity: the replay log for the resumable run does carry
        // exactly one entry, matching what the sweep's `info!` branch
        // observed.
        let entries = replay_store.list_by_run(&run_with_replay).await.unwrap();
        assert_eq!(entries.len(), 1);
        let empty = replay_store.list_by_run(&run_without_replay).await.unwrap();
        assert!(empty.is_empty());
    }

    /// B-4: the graceful-shutdown drain marks any still-`Running` Run (and
    /// its owning Task) `Interrupted` with a `{"error": "server shutdown"}`
    /// envelope, while leaving already-terminal runs untouched — the same
    /// terminal shape the boot sweep stamps, only with a shutdown-specific
    /// reason.
    #[tokio::test]
    async fn interrupt_running_on_shutdown_marks_running_as_interrupted() {
        let task_store: Arc<dyn TaskStore> = Arc::new(InMemoryTaskStore::new());
        let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());

        let running_task_id = TaskId::parse("T-shutdown-running").unwrap();
        let done_task_id = TaskId::parse("T-shutdown-done").unwrap();
        let running_run_id = RunId::parse("R-shutdown-running").unwrap();
        let done_run_id = RunId::parse("R-shutdown-done").unwrap();

        for (tid, status) in [
            (&running_task_id, TaskRecordStatus::Running),
            (&done_task_id, TaskRecordStatus::Done),
        ] {
            task_store
                .create(TaskRecord {
                    id: tid.clone(),
                    goal: "shutdown drain fixture".into(),
                    blueprint_ref: json!({}),
                    input_ctx: json!({}),
                    task_input_spec: None,
                    status,
                    created_at: 1,
                    updated_at: 1,
                })
                .await
                .unwrap();
        }
        run_store
            .create(RunRecord {
                id: running_run_id.clone(),
                task_id: running_task_id.clone(),
                status: RunStatus::Running,
                step_entries: vec![],
                degradations: vec![],
                operator_sid: None,
                current: Default::default(),
                next_generation: 0,
                result_ref: None,
                input_json: None,
                created_at: 1,
                updated_at: 1,
            })
            .await
            .unwrap();
        run_store
            .create(RunRecord {
                id: done_run_id.clone(),
                task_id: done_task_id.clone(),
                status: RunStatus::Done,
                step_entries: vec![],
                degradations: vec![],
                operator_sid: None,
                current: Default::default(),
                next_generation: 0,
                result_ref: None,
                input_json: None,
                created_at: 2,
                updated_at: 2,
            })
            .await
            .unwrap();

        interrupt_running_on_shutdown(&task_store, &run_store).await;

        let running_run = run_store.get(&running_run_id).await.unwrap();
        assert_eq!(running_run.status, RunStatus::Interrupted);
        assert_eq!(
            running_run.result_ref,
            Some(json!({"error": "server shutdown"}))
        );
        let running_task = task_store.get(&running_task_id).await.unwrap();
        assert_eq!(running_task.status, TaskRecordStatus::Interrupted);

        // Control: the already-Done run/task pair is untouched.
        let done_run = run_store.get(&done_run_id).await.unwrap();
        assert_eq!(done_run.status, RunStatus::Done);
        assert_eq!(done_run.result_ref, None);
        let done_task = task_store.get(&done_task_id).await.unwrap();
        assert_eq!(done_task.status, TaskRecordStatus::Done);
    }

    // ──────────────────────────────────────────────────────────────────
    // `engine_max_hold_ms` → `EngineCfg.max_hold_ms`
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn engine_cfg_from_keeps_the_engine_default_max_hold_when_unset() {
        let cfg = mlua_swarm_server::config::ResolvedConfig::default();
        assert_eq!(cfg.engine_max_hold_ms, None);
        assert_eq!(engine_cfg_from(&cfg).max_hold_ms, 50);
    }

    #[test]
    fn engine_cfg_from_applies_the_configured_max_hold() {
        let cfg = mlua_swarm_server::config::ResolvedConfig {
            engine_max_hold_ms: Some(200),
            ..Default::default()
        };
        assert_eq!(engine_cfg_from(&cfg).max_hold_ms, 200);
    }

    // ──────────────────────────────────────────────────────────────────
    // `worker_token_ttl_secs` → `EngineCfg.worker_token_ttl_secs`
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn engine_cfg_from_keeps_the_engine_default_worker_token_ttl_when_unset() {
        let cfg = mlua_swarm_server::config::ResolvedConfig::default();
        assert_eq!(cfg.worker_token_ttl_secs, None);
        assert_eq!(engine_cfg_from(&cfg).worker_token_ttl_secs, 1800);
    }

    #[test]
    fn engine_cfg_from_applies_the_configured_worker_token_ttl() {
        let cfg = mlua_swarm_server::config::ResolvedConfig {
            worker_token_ttl_secs: Some(7200),
            ..Default::default()
        };
        assert_eq!(engine_cfg_from(&cfg).worker_token_ttl_secs, 7200);
    }
}
