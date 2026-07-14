//! `mse bp build` — compile-lint + emit (+ optionally register) a
//! `.bp.lua` DSL script's built Blueprint JSON.
//!
//! Pipeline:
//!
//! 1. Read the script file, run it through
//!    [`mlua_swarm_cli::dsl::build_bp_from_script`] to get the raw
//!    Blueprint `serde_json::Value` (still carrying unexpanded
//!    `$file`/`$agent_md` refs — the same shape the DSL always emits).
//! 2. Best-effort **compile lint**: resolve those refs relative to the
//!    script's own directory via the existing loader mechanism
//!    ([`mlua_swarm::expand_file_refs`] — the same one
//!    `load_blueprint_from_path` uses), then run the result through
//!    [`mlua_swarm::Compiler::compile`] against a lint-only
//!    [`SpawnerRegistry`] (every built-in `AgentKind` factory
//!    registered, with a [`LintStubOperator`] pre-baked under every
//!    declared `Blueprint.operators[]` name so `kind = operator` agents
//!    — the `$agent_md` loader's default kind — resolve without a live
//!    WS session). Surfaces [`mlua_swarm::CompileError`] (including the
//!    GH #50 `VerdictChannelMismatch` / `VerdictValueNotInContract`
//!    lints) as a CLI error. When the refs can't be resolved relative to
//!    the script's directory (they may point at a directory outside the
//!    script's own tree — the server resolves those itself against its
//!    own `--blueprint-ref-base` at register time), the lint is
//!    explicitly reported as skipped rather than silently dropped.
//! 3. Emit the (pre-expansion) Blueprint JSON — `-o <path>` or stdout.
//! 4. `--register`: POST that same JSON to
//!    `http://<server>/v1/blueprints/<id>` (the server resolves
//!    `$agent_md` itself). Failure exits non-zero with a message; no
//!    retry.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use clap::{Args, Subcommand};
use mlua_swarm::{
    Compiler, LuaInProcessSpawnerFactory, OperatorSpawnerFactory, RustFnInProcessSpawnerFactory,
    SpawnerRegistry, SubprocessProcessSpawnerFactory,
};
use mlua_swarm_cli::dsl;

/// `mse bp <subcommand>`.
#[derive(Debug, Args)]
pub struct BpArgs {
    #[command(subcommand)]
    cmd: BpCmd,
}

#[derive(Debug, Subcommand)]
enum BpCmd {
    /// Build a `.bp.lua` DSL script into Blueprint JSON (compile-lint +
    /// emit, optionally register with a running `mse serve`).
    Build(BuildArgs),
}

#[derive(Debug, Args)]
struct BuildArgs {
    /// Path to the `.bp.lua` DSL script.
    script: PathBuf,
    /// Write the built JSON here instead of stdout.
    #[arg(short = 'o', long = "out")]
    out: Option<PathBuf>,
    /// POST the built JSON to a running `mse serve` (`/v1/blueprints/:id`).
    #[arg(long)]
    register: bool,
    /// Server bind address for `--register` (`host:port`, no scheme).
    /// Defaults to `mse serve`'s own default bind.
    #[arg(long)]
    server: Option<String>,
}

/// Default `mse serve` bind address. Kept as a local literal (matching
/// `mcp::server_control::DEFAULT_BIND`'s value) rather than reaching
/// into that bin-private module — see `server_control.rs` for the
/// source of truth this default tracks.
const DEFAULT_SERVER: &str = "127.0.0.1:7777";

/// Entry point wired from `main.rs`'s `Cmd::Bp` arm.
pub async fn run(args: BpArgs) -> Result<()> {
    match args.cmd {
        BpCmd::Build(build_args) => run_build(build_args).await,
    }
}

async fn run_build(args: BuildArgs) -> Result<()> {
    let script = std::fs::read_to_string(&args.script)
        .with_context(|| format!("reading {}", args.script.display()))?;
    let bp_value = dsl::build_bp_from_script(&script)
        .with_context(|| format!("building Blueprint from {}", args.script.display()))?;

    compile_lint(&bp_value, &args.script)?;

    let out_str = serde_json::to_string_pretty(&bp_value)?;
    match &args.out {
        Some(path) => {
            std::fs::write(path, &out_str)
                .with_context(|| format!("writing {}", path.display()))?;
        }
        None => println!("{out_str}"),
    }

    if args.register {
        register(&bp_value, args.server.as_deref()).await?;
    }

    Ok(())
}

/// Step 2 of the module doc's pipeline: best-effort compile lint. Never
/// hard-fails on an unresolved `$agent_md`/`$file` ref — that's the
/// server's job at register time via its own `--blueprint-ref-base` —
/// but always reports explicitly when it had to skip (no silent skip).
fn compile_lint(bp_value: &serde_json::Value, script_path: &Path) -> Result<()> {
    let base = script_path.parent().unwrap_or_else(|| Path::new("."));
    let default_kind = mlua_swarm::blueprint::loader::pre_read_default_agent_kind(bp_value);
    let expanded = match mlua_swarm::expand_file_refs(bp_value.clone(), base, default_kind) {
        Ok(v) => v,
        Err(e) => {
            eprintln!(
                "compile lint: skipped — could not resolve $file/$agent_md refs relative to \
                 {} ({e}). Only the static DSL shape was validated; the server resolves these \
                 refs against its own --blueprint-ref-base at register time.",
                base.display()
            );
            return Ok(());
        }
    };
    let bp: mlua_swarm::Blueprint = serde_json::from_value(expanded).map_err(|e| {
        anyhow!("compile lint: blueprint shape invalid after $agent_md expansion: {e}")
    })?;

    let registry = lint_registry(&bp);
    Compiler::new(registry)
        .compile(&bp)
        .map_err(|e| anyhow!("compile lint FAILED: {e}"))?;
    eprintln!(
        "compile lint: OK ({} agent(s), {} operator(s) checked)",
        bp.agents.len(),
        bp.operators.len()
    );
    Ok(())
}

/// A stub `Operator` backend used only so `kind = operator` agents (the
/// `$agent_md` loader's default kind) resolve during lint — no live WS
/// session exists at authoring time, so `execute` is never actually
/// called. `requires_worker_binding() = false` keeps the lint focused on
/// the GH #50 verdict-contract checks this command exists for, rather
/// than also gating on `profile.worker_binding` presence.
struct LintStubOperator;

#[async_trait::async_trait]
impl mlua_swarm::Operator for LintStubOperator {
    async fn execute(
        &self,
        _ctx: &mlua_swarm::Ctx,
        _system: Option<String>,
        _prompt: serde_json::Value,
        _worker: Option<mlua_swarm::WorkerBinding>,
        _worker_token: mlua_swarm::CapToken,
    ) -> Result<mlua_swarm::WorkerResult, mlua_swarm::WorkerError> {
        Ok(mlua_swarm::WorkerResult {
            value: serde_json::Value::Null,
            ok: true,
        })
    }
    fn requires_worker_binding(&self) -> bool {
        false
    }
}

/// Every built-in `AgentKind` factory, so `Blueprint.strategy.strict_kind`
/// (the schema default) never rejects a lint solely because no live
/// backend is registered — with a [`LintStubOperator`] pre-baked under
/// every declared `Blueprint.operators[].name` so `kind = operator`
/// agents' `spec.operator_ref` resolves too.
fn lint_registry(bp: &mlua_swarm::Blueprint) -> SpawnerRegistry {
    let mut reg = SpawnerRegistry::new();
    reg.register::<SubprocessProcessSpawnerFactory>(Arc::new(SubprocessProcessSpawnerFactory));
    reg.register::<RustFnInProcessSpawnerFactory>(Arc::new(RustFnInProcessSpawnerFactory::new()));
    reg.register::<LuaInProcessSpawnerFactory>(Arc::new(LuaInProcessSpawnerFactory::new()));
    let op_factory = OperatorSpawnerFactory::new();
    for op in &bp.operators {
        op_factory.register_operator(op.name.clone(), Arc::new(LintStubOperator));
    }
    reg.register::<OperatorSpawnerFactory>(Arc::new(op_factory));
    reg
}

/// Step 4: `--register`. Failure exits non-zero with a message; no retry.
async fn register(bp_value: &serde_json::Value, server: Option<&str>) -> Result<()> {
    let server = server.unwrap_or(DEFAULT_SERVER);
    let id = bp_value
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("register: Blueprint JSON has no top-level 'id' string field"))?;
    let url = format!("http://{server}/v1/blueprints/{id}");
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .json(bp_value)
        .send()
        .await
        .map_err(|e| anyhow!("register: request to {url} failed: {e}"))?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!("register: {url} returned HTTP {status}: {body}"));
    }
    eprintln!("register: {url} -> HTTP {status}: {body}");
    Ok(())
}
