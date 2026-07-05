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
use mlua_swarm::store::output::{InMemoryOutputStore, OutputStore, SqliteOutputStore};
use mlua_swarm::{
    Compiler, Engine, EngineCfg, EnhanceApplication, EnhanceApplicationConfig, Role,
    TaskLaunchService,
};
use mlua_swarm::{
    OperatorSpawnerFactory, RustFnInProcessSpawnerFactory, SpawnerRegistry,
    SubprocessProcessSpawnerFactory,
};
use mlua_swarm_server::{
    build_blueprints_router_with_refs, build_enhance_log_router, build_enhance_settings_router,
    build_issues_router, build_router_with_ws_factory_and_output, default_layer_registry,
    default_registry_with_enhance_flow,
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
    /// Merges the 4 enhance-flow workers (patch-spawner / patch-applier /
    /// verifier-router / committer) + 3 host bridges into `default_registry`.
    /// Used when running the default enhance Blueprint through `/v1/tasks`. A pure
    /// switch: absent = no override (defers to the config file / built-in default
    /// `false`); passing it always forces `true`.
    #[arg(long)]
    enable_enhance_flow: bool,
    /// Base dir for expanding `{"$file": ...}` / `{"$agent_md": ...}` refs found
    /// in `POST /v1/blueprints/:id` seed bodies. When omitted (and absent from the
    /// config file), ref expansion is disabled (= parses raw JSON). Used by the
    /// step 7 L4 smoke path where `agent.md` is embedded into the BP via `$agent_md`.
    /// Overrides the config file's `blueprint_ref_base`.
    #[arg(long)]
    blueprint_ref_base: Option<std::path::PathBuf>,
    /// The (2) CLI override layer of the 4-tier cascade. Falls back when the BP
    /// top-level `default_agent_kind` JSON literal is absent; if that is also
    /// absent, the Schema-impl `Default` = `Operator` is used. The value is the
    /// snake_case form of the `AgentKind` enum (`operator` / `agent_block` /
    /// `rust_fn` / `lua` / `subprocess`). Example: `--default-agent-kind agent_block`.
    /// Overrides the config file's `default_agent_kind`.
    #[arg(long)]
    default_agent_kind: Option<String>,
}

fn parse_agent_kind_cli(s: &str) -> Result<mlua_swarm::blueprint::AgentKind, String> {
    serde_json::from_value(serde_json::Value::String(s.to_string()))
        .map_err(|e| format!("invalid --default-agent-kind {s:?}: {e}"))
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
        blueprint_ref_base: args.blueprint_ref_base.clone(),
        git_store_path: args.git_store_path.clone(),
        issue_store_path: args.issue_store_path.clone(),
        enhance_setting_store_path: args.enhance_setting_store_path.clone(),
        enhance_log_store_path: args.enhance_log_store_path.clone(),
        output_store_path: args.output_store_path.clone(),
        seed_blueprint_id: args.seed_blueprint_id.clone(),
        default_agent_kind: args.default_agent_kind.clone(),
        token_secret: args.token_secret.clone(),
    };
    let cfg = mlua_swarm_server::config::resolve(cli_overrides, file_config)
        .unwrap_or_else(|e| panic!("mse serve: config resolve failed: {e}"));
    let default_agent_kind: Option<mlua_swarm::blueprint::AgentKind> = cfg
        .default_agent_kind
        .as_ref()
        .map(|s| parse_agent_kind_cli(s).unwrap_or_else(|e| panic!("mse serve: {e}")));
    eprintln!("mse serve: config loaded from {}", config_path.display());

    let make_cfg = || {
        let mut c = EngineCfg::default();
        if let Some(hex_secret) = &cfg.token_secret {
            c.token_secret = hex::decode(hex_secret).expect("token-secret must be hex");
        }
        c
    };
    // Engine stateless-executor refactor:
    // A single Engine instance is used (the old task / enhance axis split
    // guarded against bind-state races that dispatch_attempt_with's
    // per-request spawner already prevents — no global-state race remains).
    // The Engine is built with a LayerRegistry so that
    // `Blueprint.spawner_hints` values ("main_ai" / "senior_escalation" /
    // "operator_delegate") get wrapped into the SpawnerStack inside
    // TaskLaunchService.
    let engine = Engine::new_with_layers(make_cfg(), default_layer_registry());

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
    // ws_operator_factory arg of build_router_with_ws_factory. On WS
    // connect, the handler registers the sid directly on that factory.
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

    // Router assembly (fixed combined mode): merges task, ws_operator_factory, and every enhance route.
    let mut app = build_router_with_ws_factory_and_output(
        engine.clone(),
        make_registry(),
        Some(store.clone()),
        Some(op_factory.clone()),
        output_store,
    );

    let compiler = Compiler::new(make_registry());
    let launch_enhance = Arc::new(TaskLaunchService::new(engine.clone(), compiler));

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
        bind: cfg.bind.to_string(),
        blueprint_backend: "git2".into(),
        blueprint_store_root: Some(cfg.git_store_path.join("blueprints").display().to_string()),
        blueprint_ref_base: cfg
            .blueprint_ref_base
            .as_ref()
            .map(|p| p.display().to_string()),
        enhance_flow_enabled: cfg.enable_enhance_flow,
        seed_blueprint_id: cfg.seed_blueprint_id.clone(),
    };

    app = app
        .merge(build_issues_router(issue_store.clone()))
        .merge(build_blueprints_router_with_refs(
            store.clone(),
            cfg.blueprint_ref_base.clone(),
            default_agent_kind,
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
    let serve = axum::serve(listener, app);
    tokio::select! {
        r = serve => { r.expect("serve"); }
        _ = tokio::signal::ctrl_c() => { eprintln!("mse serve: ctrl-c, shutting down"); }
        _ = wait_sigterm() => { eprintln!("mse serve: SIGTERM, shutting down"); }
    }
    enhance_loop.abort();
    // Drain SQLite isle drivers (drops queued jobs, joins the SQLite thread).
    // Errors are logged but do not fail shutdown — the process is exiting.
    for driver in isle_drivers {
        if let Err(e) = driver.shutdown().await {
            eprintln!("mse serve: isle driver shutdown error: {e}");
        }
    }
    Ok(())
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
        }],
        operators: vec![],
        hints: CompilerHints::default(),
        strategy: CompilerStrategy::default(),
        metadata: BlueprintMetadata {
            description: Some("mse serve enhance seed".into()),
            origin: BlueprintOrigin::Inline,
            tags: vec![],
            version_label: Some("0.1.0".into()),
            project_name_alias: None,
            default_run_ttl_secs: None,
        },
        spawner_hints: Default::default(),
        default_agent_kind: AgentKind::Operator,
        default_operator_kind: None,
    }
}
