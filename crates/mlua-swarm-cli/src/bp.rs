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
//!
//! The same pipeline is exposed as the `bp_build` MCP tool (`mse mcp`,
//! see `crate::mcp`) so MCP clients can register a `.bp.lua` without
//! shelling out — [`compile_lint`] / [`register`] are `pub(crate)` for
//! that caller.

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
    /// Lint a `.bp.lua` script — resolve refs via the include cascade,
    /// run the compile lint, and print a structured verdict (OK / WARN /
    /// ERROR). Does not emit Blueprint JSON. Independent from `bp build`
    /// (precedent: `cargo check`, `tsc --noEmit`, `eslint`).
    /// `--strict` exits non-zero on any WARN/ERROR (CI use).
    Lint(LintArgs),
    /// Scaffold a minimal `.bp.lua` from a bundled template with all
    /// currently-mandatory fields pre-filled (`halted_at`, `runner`,
    /// `strict_refs`/`strict_kind`). GH #62 Axis A. See
    /// `mse://guides/bp-dsl-templates` for template inventory.
    New(NewArgs),
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
    /// Additional directory to search when resolving `$agent_md` /
    /// `$file` refs. Repeatable (tier 4 of the include cascade — see
    /// `mlua-swarm-compile::ResolveConfig`). Search order:
    /// bp.lua parent → in-bp `blueprint_ref_includes` → env
    /// `MSE_BLUEPRINT_INCLUDES` → `--include` (this flag) →
    /// bundled default samples dir.
    #[arg(long = "include", action = clap::ArgAction::Append, value_name = "DIR")]
    include: Vec<PathBuf>,
    /// Require every `$file` / `$agent_md` ref to embed at build time.
    /// Default behavior on an unresolved ref: emit the raw wire JSON
    /// (refs preserved) and print a WARN — the server resolves refs
    /// itself at register time. With `--strict-embed`, an unresolved
    /// ref hard-fails the build (non-zero exit, no JSON emitted). Name
    /// mirrors "require refs to be embedded": not `--strict-refs`
    /// (which would misleadingly suggest refs themselves are
    /// disallowed). Independent from the server-side `mse serve
    /// --blueprint-strict-embed` switch (register-time raw-ref
    /// reject) — the two layers are compared side by side in
    /// `mse://guides/strict-embed-modes`.
    #[arg(long = "strict-embed")]
    strict_embed: bool,
}

#[derive(Debug, Args)]
struct LintArgs {
    /// Path to the `.bp.lua` DSL script.
    script: PathBuf,
    /// Additional directory to search when resolving `$agent_md` /
    /// `$file` refs. Repeatable (tier 4 of the include cascade — see
    /// `mlua-swarm-compile::ResolveConfig`).
    #[arg(long = "include", action = clap::ArgAction::Append, value_name = "DIR")]
    include: Vec<PathBuf>,
    /// Exit non-zero on any WARN or ERROR verdict. Default is exit 0 on
    /// WARN, non-zero only on ERROR. Precedent: `mypy --strict`,
    /// `eslint --max-warnings 0`, `cargo clippy -- -D warnings`.
    #[arg(long)]
    strict: bool,
}

#[derive(Debug, Args)]
struct NewArgs {
    /// Template kind: `pipeline` (N-stage main-ai) / `single`
    /// (one-agent one-step) / `verdict` (3-stage verdict-gated with
    /// retry-through-fixer).
    template: String,
    /// Blueprint id, also the emitted `id` field.
    name: String,
    /// Stage names, comma-separated (`pipeline` / `verdict` templates only).
    /// `pipeline`: defaults to `stage1,stage2`. `verdict`: defaults to
    /// `analyze,review,publish` — fixed 3-stage shape, extra values ignored.
    #[arg(long)]
    stages: Option<String>,
    /// Agent name (`single` template only). Defaults to `solo`.
    #[arg(long)]
    agent: Option<String>,
    /// Operator role name every agent points at. Defaults to `main-ai`.
    #[arg(long)]
    operator: Option<String>,
    /// `runner.variant` value for every emitted operator agent. The historical
    /// flag name remains for CLI compatibility.
    /// Defaults to `claude` (the Claude Code catch-all SubAgent variant).
    #[arg(long)]
    binding: Option<String>,
    /// Write the rendered `.bp.lua` here instead of stdout.
    #[arg(short = 'o', long = "out")]
    out: Option<PathBuf>,
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
        BpCmd::Lint(lint_args) => run_lint(lint_args),
        BpCmd::New(new_args) => run_new(new_args),
    }
}

async fn run_build(args: BuildArgs) -> Result<()> {
    let script = std::fs::read_to_string(&args.script)
        .with_context(|| format!("reading {}", args.script.display()))?;
    let bp_value = dsl::build_bp_from_script(&script)
        .with_context(|| format!("building Blueprint from {}", args.script.display()))?;

    match compile_lint(&bp_value, &args.script, &args.include) {
        Ok(LintReport::Ok { agents, operators }) => {
            eprintln!("compile lint: OK ({agents} agent(s), {operators} operator(s) checked)");
        }
        Ok(LintReport::Warn {
            agents,
            operators,
            reason,
            warnings,
        }) => {
            eprintln!(
                "compile lint: WARN ({agents} agent(s), {operators} operator(s) checked) — {reason}"
            );
            for w in &warnings {
                eprintln!("  - {w}");
            }
            // `--strict-embed` promotes an unresolved-ref WARN to a
            // hard failure (non-zero exit, no JSON emitted). Default
            // stays "raw emit + Warn" so the wire layer can still
            // carry refs to a server that resolves them itself — the
            // layered wire/typed policy: typed BP is resolved-only,
            // the wire layer is partial-preserve.
            if args.strict_embed {
                return Err(anyhow!(
                    "compile lint: --strict-embed, refusing to emit Blueprint JSON with unresolved refs"
                ));
            }
        }
        Err(e) => {
            // GH #62 Axis B.1: on a lint failure, render a structured
            // fix hint after the raw Compiler error when the message
            // matches a known lint kind — Clippy-style diagnostic
            // affordance without changing what stderr's exit code
            // signals (still non-zero via the `?` on this arm below).
            if let Some(hint) = fix_hint_for_error(&e) {
                eprintln!();
                eprintln!("fix hint ({}):", hint.kind);
                eprintln!("  {}", hint.reason);
                eprintln!();
                eprintln!("  suggested patch:");
                for line in hint.patch_suggestion.lines() {
                    eprintln!("    {line}");
                }
                if let Some(docs) = &hint.docs_ref {
                    eprintln!();
                    eprintln!("  see: {docs}");
                }
            }
            return Err(e);
        }
    }

    let out_str = serde_json::to_string_pretty(&bp_value)?;
    match &args.out {
        Some(path) => {
            std::fs::write(path, &out_str)
                .with_context(|| format!("writing {}", path.display()))?;
        }
        None => println!("{out_str}"),
    }

    if args.register {
        let outcome = register(&bp_value, args.server.as_deref()).await?;
        eprintln!(
            "register: {} -> HTTP {}: {}",
            outcome.url, outcome.http_status, outcome.body
        );
    }

    Ok(())
}

/// `mse bp lint` entry — independent from `mse bp build`. Runs the same
/// [`compile_lint`] pipeline (include-cascade linker + [`Compiler`]) but
/// does not emit Blueprint JSON. Prints a structured verdict:
///
/// ```text
/// bp lint: OK    (N agent(s), M operator(s) checked)
/// bp lint: WARN  (…, some refs unresolved) → non-zero with --strict
/// bp lint: ERROR (…, compile lint failed)  → always non-zero
/// ```
///
/// Precedent: `cargo check`, `tsc --noEmit`, `eslint`, `ruff check`.
/// `--strict` mirrors `mypy --strict` / `cargo clippy -- -D warnings`
/// (promote WARN to non-zero exit for CI use).
fn run_lint(args: LintArgs) -> Result<()> {
    let script = std::fs::read_to_string(&args.script)
        .with_context(|| format!("reading {}", args.script.display()))?;
    let bp_value = dsl::build_bp_from_script(&script)
        .with_context(|| format!("building Blueprint from {}", args.script.display()))?;

    match compile_lint(&bp_value, &args.script, &args.include) {
        Ok(LintReport::Ok { agents, operators }) => {
            eprintln!("bp lint: OK ({agents} agent(s), {operators} operator(s) checked)");
            Ok(())
        }
        Ok(LintReport::Warn {
            agents,
            operators,
            reason,
            warnings,
        }) => {
            eprintln!(
                "bp lint: WARN ({agents} agent(s), {operators} operator(s) checked) — {reason}"
            );
            for w in &warnings {
                eprintln!("  - {w}");
            }
            if args.strict {
                Err(anyhow!("bp lint: --strict, exiting non-zero on WARN"))
            } else {
                Ok(())
            }
        }
        Err(e) => {
            let msg = format!("{e:#}");
            eprintln!("bp lint: ERROR — {msg}");
            if let Some(hint) = fix_hint_for_error(&e) {
                eprintln!();
                eprintln!("fix hint ({}):", hint.kind);
                eprintln!("  {}", hint.reason);
                eprintln!();
                eprintln!("  suggested patch:");
                for line in hint.patch_suggestion.lines() {
                    eprintln!("    {line}");
                }
                if let Some(docs) = &hint.docs_ref {
                    eprintln!();
                    eprintln!("  see: {docs}");
                }
            }
            Err(e)
        }
    }
}

/// GH #62 Axis A default Runner variant — the Claude Code catch-all
/// SubAgent variant. Overridable per invocation via `--binding`.
const DEFAULT_BINDING: &str = "claude";
/// GH #62 Axis A default operator role name — the same `main-ai`
/// convention every bundled sample points at. Overridable via `--operator`.
const DEFAULT_OPERATOR: &str = "main-ai";
/// GH #62 Axis A `pipeline` template default stage list when `--stages`
/// is not supplied.
const DEFAULT_PIPELINE_STAGES: &[&str] = &["stage1", "stage2"];
/// GH #62 Axis A `verdict` template canonical 3-stage names. The verdict
/// template's shape is fixed at 3 (analyze / review / publish) mirroring
/// `mse://blueprints/samples/07-dsl-pipeline`; extra `--stages` values are
/// ignored, fewer than 3 fall back to these defaults per position.
const DEFAULT_VERDICT_STAGES: [&str; 3] = ["analyze", "review", "publish"];
/// GH #62 Axis A `single` template default sole-agent name.
const DEFAULT_SINGLE_AGENT: &str = "solo";
/// GH #82 `fanout` template default parallel-branch stage names.
/// (The aggregate stage is a fixed positional slot appended by the
/// renderer; only the parallel branches are `--stages`-configurable.)
const DEFAULT_FANOUT_STAGES: &[&str] = &["checker1", "checker2"];
/// GH #82 `fanout` template fixed name for the join/aggregate stage
/// that consumes the fanout's `out` array.
const FANOUT_AGGREGATE_STAGE: &str = "aggregate";

fn run_new(args: NewArgs) -> Result<()> {
    let out = render_template(&args)?;
    match &args.out {
        Some(path) => {
            std::fs::write(path, &out).with_context(|| format!("writing {}", path.display()))?;
            eprintln!("mse bp new: wrote {} ({} bytes)", path.display(), out.len());
        }
        None => print!("{out}"),
    }
    Ok(())
}

fn render_template(args: &NewArgs) -> Result<String> {
    render_template_by_kind(
        &args.template,
        &args.name,
        args.stages.as_deref(),
        args.agent.as_deref(),
        args.operator.as_deref(),
        args.binding.as_deref(),
    )
}

/// GH #62 Axis A: dispatch on template name. Pure — no I/O. Shared with the
/// `bp_new` MCP tool (`crate::mcp`), which returns the rendered `.bp.lua`
/// as a response field. Errors on unknown template (with the accepted list)
/// so an author who typos `template` sees the closed set. Primitive-typed
/// signature so the MCP request struct (which doesn't own a `NewArgs`) can
/// call it directly.
pub(crate) fn render_template_by_kind(
    template: &str,
    name: &str,
    stages: Option<&str>,
    agent: Option<&str>,
    operator: Option<&str>,
    binding: Option<&str>,
) -> Result<String> {
    let operator = operator.unwrap_or(DEFAULT_OPERATOR);
    let binding = binding.unwrap_or(DEFAULT_BINDING);
    match template {
        "pipeline" => Ok(render_pipeline_template(
            name,
            &parse_stages(stages, DEFAULT_PIPELINE_STAGES),
            operator,
            binding,
        )),
        "single" => Ok(render_single_template(
            name,
            agent.unwrap_or(DEFAULT_SINGLE_AGENT),
            operator,
            binding,
        )),
        "verdict" => Ok(render_verdict_template(
            name,
            &parse_verdict_stages(stages),
            operator,
            binding,
        )),
        // GH #82: `fanout` template — N parallel checker stages + one
        // aggregate stage. `stages` names only the parallel branches;
        // the aggregate stage's id is fixed (`FANOUT_AGGREGATE_STAGE`).
        "fanout" => Ok(render_fanout_template(
            name,
            &parse_stages(stages, DEFAULT_FANOUT_STAGES),
            operator,
            binding,
        )),
        other => Err(anyhow!(
            "unknown template '{other}': accepted = pipeline / single / verdict / fanout"
        )),
    }
}

fn parse_stages(raw: Option<&str>, default: &[&str]) -> Vec<String> {
    match raw {
        Some(s) => s
            .split(',')
            .map(|part| part.trim())
            .filter(|part| !part.is_empty())
            .map(String::from)
            .collect(),
        None => default.iter().map(|s| (*s).to_string()).collect(),
    }
}

/// `verdict` template stage names — 3 fixed positional slots. Fewer than 3
/// supplied → remaining fall back to `DEFAULT_VERDICT_STAGES[i]`; more than
/// 3 → tail truncated. This is deliberate: `verdict`'s shape ties stage
/// identity to role (analyze produces the input, review issues the
/// verdict, publish consumes on PASS) — variable stage counts would
/// change the flow shape, not just names.
fn parse_verdict_stages(raw: Option<&str>) -> [String; 3] {
    let supplied = parse_stages(raw, &[]);
    let mut out = DEFAULT_VERDICT_STAGES.map(String::from);
    for (slot, val) in out.iter_mut().zip(supplied) {
        *slot = val;
    }
    out
}

fn render_pipeline_template(
    name: &str,
    stages: &[String],
    operator: &str,
    binding: &str,
) -> String {
    let stages: &[String] = if stages.is_empty() {
        // parse_stages returns empty only when `--stages ""` is passed
        // literally; fall back to the same default the None arm uses.
        return render_pipeline_template(
            name,
            &DEFAULT_PIPELINE_STAGES
                .iter()
                .map(|s| (*s).to_string())
                .collect::<Vec<_>>(),
            operator,
            binding,
        );
    } else {
        stages
    };
    let init_ctx_sample = stages
        .iter()
        .map(|stage| format!("{stage} = \"...\""))
        .collect::<Vec<_>>()
        .join(", ");
    let mut out = String::new();
    out.push_str("-- Scaffolded by `mse bp new pipeline` (GH #62 Axis A).\n");
    out.push_str("-- Every mandatory field is pre-filled: `halted_at` (compile-lint\n");
    out.push_str("-- default), each operator agent's explicit `ws_operator` Runner,\n");
    out.push_str("-- the operator's `kind` (main_ai,\n");
    out.push_str("-- so a caller can omit `operator_kind` at swarm_run time and the\n");
    out.push_str("-- BP Agent-level tier of the OperatorKind cascade routes spawns to\n");
    out.push_str("-- a joined main-ai session instead of silently falling through to\n");
    out.push_str("-- the Automate backend, GH #66), `strict_refs` + `strict_kind`.\n");
    out.push_str("--\n");
    out.push_str("-- Launch prerequisite (GH #64): the pipeline sugar reads each stage's\n");
    out.push_str("-- input from `$.d.<stage_name>` and does NOT auto-chain outputs into\n");
    out.push_str("-- the next stage. Seed every stage under `d` when starting a run:\n");
    out.push_str("--\n");
    out.push_str(&format!(
        "--   swarm_run(blueprint = ..., init_ctx = {{ d = {{ {init_ctx_sample} }} }})\n"
    ));
    out.push_str("--\n");
    out.push_str("-- See `mse://guides/bp-dsl-templates` for the `$.d.<stage>` convention\n");
    out.push_str(
        "-- and recipes for hand-chaining outputs (e.g. `F.step { input = F.p \"$.<prev>\" }`).\n",
    );
    out.push_str("--\n");
    out.push_str(
        "-- Naming glue (see `mse://guides/operator-execution-model` §Operator naming):\n",
    );
    out.push_str(
        "--   `operators[].name` == mint-time `roles[]` alias == every agent's `operator_ref`.\n",
    );
    out.push_str(
        "--   The literal is arbitrary (`main-ai` is a convention, not a system name); to run\n",
    );
    out.push_str("--   two MainAIs in parallel, split into per-lane aliases (e.g. `phase_a_op`\n");
    out.push_str("--   / `phase_b_op`) and rebind the agents' `operator_ref` accordingly.\n\n");
    out.push_str("local B = require(\"bp_dsl\")\n\n");
    out.push_str("local flow = B.pipeline({\n");
    for stage in stages {
        out.push_str(&format!(
            "  B.stage \"{stage}\" {{ agent = \"{stage}\" }},\n"
        ));
    }
    out.push_str("  halted_at = \"$.halted_at\",\n");
    out.push_str("  done      = \"$.pipeline_complete\",\n");
    out.push_str("})\n\n");
    out.push_str(&format!("return {{\n  id = \"{name}\",\n  flow = flow,\n"));
    out.push_str("  agents = {\n");
    for stage in stages {
        out.push_str(&format!(
            "    {{ name = \"{stage}\", kind = \"operator\",\n      \
             spec = {{ operator_ref = \"{operator}\" }},\n      \
             profile = {{ system_prompt = \"TODO: describe {stage}\", tools = {{}} }},\n      \
             runner = {{ backend = \"ws_operator\", variant = \"{binding}\", tools = {{}} }} }},\n"
        ));
    }
    out.push_str("  },\n");
    out.push_str(&format!(
        "  operators = {{ {{ name = \"{operator}\", kind = \"main_ai\" }} }},\n"
    ));
    out.push_str("  strategy = { strict_refs = true, strict_kind = true },\n");
    out.push_str(&format!(
        "  metadata = {{ description = \"TODO: describe {name}\" }},\n"
    ));
    out.push_str("}\n");
    out
}

fn render_single_template(name: &str, agent: &str, operator: &str, binding: &str) -> String {
    let mut out = String::new();
    out.push_str("-- Scaffolded by `mse bp new single` (GH #62 Axis A).\n");
    out.push_str("-- Minimal 1-step 1-agent shape — `flow_dsl` directly, no pipeline\n");
    out.push_str("-- sugar. All mandatory fields (`runner`, `strict_refs`,\n");
    out.push_str("-- `strict_kind`) are pre-filled. The operator's `kind` is also\n");
    out.push_str("-- pre-filled as `main_ai` (GH #66) so `swarm_run` can omit\n");
    out.push_str("-- `operator_kind` at launch and spawns route to a joined\n");
    out.push_str("-- main-ai session.\n");
    out.push_str("--\n");
    out.push_str(
        "-- Naming glue (see `mse://guides/operator-execution-model` §Operator naming):\n",
    );
    out.push_str(
        "--   `operators[].name` == mint-time `roles[]` alias == agent's `operator_ref`.\n",
    );
    out.push_str("--   The literal is arbitrary (`main-ai` is a convention, not a system name);\n");
    out.push_str("--   rename it (e.g. `phase_a_op`) and update `operator_ref` in lockstep to\n");
    out.push_str("--   run this BP on a dedicated MainAI alongside other BPs.\n\n");
    out.push_str("local F = require(\"flow_dsl\")\n\n");
    out.push_str(&format!(
        "local flow = F.step({{ id = \"{agent}\", agent = \"{agent}\", \
         input = F.lit(\"\"), out = F.p(\"$.{agent}\") }})\n\n"
    ));
    out.push_str(&format!("return {{\n  id = \"{name}\",\n  flow = flow,\n"));
    out.push_str("  agents = {\n");
    out.push_str(&format!(
        "    {{ name = \"{agent}\", kind = \"operator\",\n      \
         spec = {{ operator_ref = \"{operator}\" }},\n      \
         profile = {{ system_prompt = \"TODO: describe {agent}\", tools = {{}} }},\n      \
         runner = {{ backend = \"ws_operator\", variant = \"{binding}\", tools = {{}} }} }},\n"
    ));
    out.push_str("  },\n");
    out.push_str(&format!(
        "  operators = {{ {{ name = \"{operator}\", kind = \"main_ai\" }} }},\n"
    ));
    out.push_str("  strategy = { strict_refs = true, strict_kind = true },\n");
    out.push_str(&format!(
        "  metadata = {{ description = \"TODO: describe {name}\" }},\n"
    ));
    out.push_str("}\n");
    out
}

fn render_verdict_template(
    name: &str,
    stages: &[String; 3],
    operator: &str,
    binding: &str,
) -> String {
    let [analyze, review, publish] = stages;
    let mut out = String::new();
    out.push_str("-- Scaffolded by `mse bp new verdict` (GH #62 Axis A).\n");
    out.push_str(&format!(
        "-- Mirrors `mse://blueprints/samples/07-dsl-pipeline`: {analyze} -> \
         {review} (verdict-gated, bounded retry through fixer on BLOCKED) -> \
         {publish}. All mandatory fields pre-filled.\n"
    ));
    out.push_str("-- The operator's `kind` is also pre-filled as `main_ai` (GH #66)\n");
    out.push_str("-- so `swarm_run` can omit `operator_kind` at launch and spawns\n");
    out.push_str("-- route to a joined main-ai session.\n");
    out.push_str("--\n");
    out.push_str("-- Launch prerequisite (GH #64): the pipeline sugar reads each stage's\n");
    out.push_str("-- input from `$.d.<stage_name>` and does NOT auto-chain outputs into\n");
    out.push_str("-- the next stage. Seed every stage under `d` when starting a run:\n");
    out.push_str("--\n");
    out.push_str(&format!(
        "--   swarm_run(blueprint = ..., init_ctx = {{ d = {{ {analyze} = \"...\", {review} = \"...\", {publish} = \"...\" }} }})\n"
    ));
    out.push_str("--\n");
    out.push_str("-- See `mse://guides/bp-dsl-templates` for the `$.d.<stage>` convention\n");
    out.push_str(
        "-- and recipes for hand-chaining outputs (e.g. `F.step { input = F.p \"$.<prev>\" }`).\n",
    );
    out.push_str("--\n");
    out.push_str(
        "-- Naming glue (see `mse://guides/operator-execution-model` §Operator naming):\n",
    );
    out.push_str(
        "--   `operators[].name` == mint-time `roles[]` alias == every agent's `operator_ref`.\n",
    );
    out.push_str(
        "--   The literal is arbitrary (`main-ai` is a convention, not a system name); to run\n",
    );
    out.push_str("--   two MainAIs in parallel, split into per-lane aliases (e.g. `phase_a_op`\n");
    out.push_str("--   / `phase_b_op`) and rebind the agents' `operator_ref` accordingly.\n\n");
    out.push_str("local B = require(\"bp_dsl\")\n\n");
    out.push_str("local flow = B.pipeline({\n");
    out.push_str(&format!(
        "  B.stage \"{analyze}\" {{ agent = \"{analyze}\" }},\n"
    ));
    out.push_str(&format!(
        "  B.stage \"{review}\" {{\n    \
         agent = \"{review}\",\n    \
         retry = {{\n      \
         max = 2,\n      \
         fix = B.stage \"fix\" {{ agent = \"fixer\", input = B.from \"{review}\" }},\n    \
         }},\n  }},\n"
    ));
    out.push_str(&format!(
        "  B.stage \"{publish}\" {{ agent = \"{publish}\" }},\n"
    ));
    out.push_str("  halt_on   = { \"BLOCKED\" },\n");
    out.push_str("  halted_at = \"$.halted_at\",\n");
    out.push_str("  done      = \"$.pipeline_complete\",\n");
    out.push_str("})\n\n");
    out.push_str(&format!("return {{\n  id = \"{name}\",\n  flow = flow,\n"));
    out.push_str("  agents = {\n");
    // analyze / publish: plain operator, no verdict contract.
    for stage in [analyze.as_str(), publish.as_str()] {
        out.push_str(&format!(
            "    {{ name = \"{stage}\", kind = \"operator\",\n      \
             spec = {{ operator_ref = \"{operator}\" }},\n      \
             profile = {{ system_prompt = \"TODO: describe {stage}\", tools = {{}} }},\n      \
             runner = {{ backend = \"ws_operator\", variant = \"{binding}\", tools = {{}} }} }},\n"
        ));
    }
    // review: verdict-gated (PASS / BLOCKED).
    out.push_str(&format!(
        "    {{ name = \"{review}\", kind = \"operator\",\n      \
         spec = {{ operator_ref = \"{operator}\" }},\n      \
         profile = {{ system_prompt = \"TODO: stage a `verdict` part = \
         `PASS` or `BLOCKED`, then finish with report body\", \
         tools = {{}} }},\n      \
         runner = {{ backend = \"ws_operator\", variant = \"{binding}\", tools = {{}} }},\n      \
         verdict = {{ channel = \"part\", values = {{ \"PASS\", \"BLOCKED\" }} }} }},\n"
    ));
    // fixer: plain operator, referenced by retry.fix.
    out.push_str(&format!(
        "    {{ name = \"fixer\", kind = \"operator\",\n      \
         spec = {{ operator_ref = \"{operator}\" }},\n      \
         profile = {{ system_prompt = \"TODO: given the reviewer's report, \
         emit a fix and reply so the review can retry\", \
         tools = {{}} }},\n      \
         runner = {{ backend = \"ws_operator\", variant = \"{binding}\", tools = {{}} }} }},\n"
    ));
    out.push_str("  },\n");
    out.push_str(&format!(
        "  operators = {{ {{ name = \"{operator}\", kind = \"main_ai\" }} }},\n"
    ));
    out.push_str("  strategy = { strict_refs = true, strict_kind = true },\n");
    out.push_str(&format!(
        "  metadata = {{ description = \"TODO: describe {name}\" }},\n"
    ));
    out.push_str("}\n");
    out
}

/// GH #82: `mse bp new fanout` — parallel branch dispatch + aggregate.
/// Emits a `flow_dsl` shape that used to require `F.raw()`:
/// `F.seq{ assign(items), fanout(items -> N checkers -> results), aggregate step }`.
/// The parallel branches are named by `stages` (2+ entries); the aggregate
/// stage's id is fixed as `FANOUT_AGGREGATE_STAGE` and reads the
/// fanout's `$.results` array. Every declared agent — the N checkers
/// and the aggregate — is a plain operator with the standard
/// `ws_operator` Runner shape the other templates use, so the emitted
/// script round-trips through the same compile-lint path.
///
/// The `bind` expression the fanout uses (`$.d.<checker>`) is the same
/// `$.d.<stage>` convention `pipeline` / `verdict` follow, so the
/// launch prerequisite is symmetric: seed each parallel branch's own
/// input under `d` at `swarm_run` time.
fn render_fanout_template(
    name: &str,
    checkers: &[String],
    operator: &str,
    binding: &str,
) -> String {
    let checkers: &[String] = if checkers.is_empty() {
        // parse_stages returns empty only when `--stages ""` is passed
        // literally; fall back to the same default the None arm uses.
        return render_fanout_template(
            name,
            &DEFAULT_FANOUT_STAGES
                .iter()
                .map(|s| (*s).to_string())
                .collect::<Vec<_>>(),
            operator,
            binding,
        );
    } else {
        checkers
    };
    let aggregate = FANOUT_AGGREGATE_STAGE;
    let checker_list_lua = checkers
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let init_ctx_sample = checkers
        .iter()
        .map(|c| format!("{c} = \"...\""))
        .collect::<Vec<_>>()
        .join(", ");
    let mut out = String::new();
    out.push_str("-- Scaffolded by `mse bp new fanout` (GH #82).\n");
    out.push_str("-- Parallel branch dispatch + aggregate — the shape `bp_dsl` used to\n");
    out.push_str("-- require `F.raw()` for. Every mandatory field is pre-filled\n");
    out.push_str("-- (`halted_at` is unused here since no halt-on rule is declared;\n");
    out.push_str("-- each operator agent's explicit `ws_operator` Runner; the\n");
    out.push_str("-- operator's `kind = main_ai`; `strict_refs` + `strict_kind`).\n");
    out.push_str("--\n");
    out.push_str(
        "-- Launch prerequisite (mirrors `pipeline` / `verdict` — GH #64): each\n",
    );
    out.push_str("-- checker reads its input from `$.d.<checker>`. Seed every\n");
    out.push_str("-- checker under `d` when starting a run:\n");
    out.push_str("--\n");
    out.push_str(&format!(
        "--   swarm_run(blueprint = ..., init_ctx = {{ d = {{ {init_ctx_sample} }} }})\n"
    ));
    out.push_str("--\n");
    out.push_str(
        "-- The fanout `join = \"all\"` gathers every branch's final ctx into an\n",
    );
    out.push_str("-- ordered array at `$.results`; the aggregate stage reads that\n");
    out.push_str("-- array and produces the final decision.\n");
    out.push_str("--\n");
    out.push_str("-- See `mse://guides/blueprint-authoring` § \"Flow node kinds\" for\n");
    out.push_str("-- the four `join` modes (`all` / `any` / `race` / `all_settled`)\n");
    out.push_str("-- and `mse://guides/bp-dsl-templates` for this template's shape.\n\n");
    out.push_str("local F = require(\"flow_dsl\")\n\n");
    // Flow: pre-seed items array with the checker names, run the fanout
    // per element (each branch runs its own step keyed on `$.d.<bind>`),
    // then dispatch the aggregate step reading `$.results`.
    out.push_str("local flow = F.seq({\n");
    out.push_str(&format!(
        "  F.assign({{ at = F.p(\"$.checkers\"), value = F.lit({{ {checker_list_lua} }}) }}),\n"
    ));
    // The fanout's `body` dispatches ONE step per element; the element
    // (a checker's name) binds under `$.item`, and each branch reads
    // its own input from `$.d.<checker_name>` — resolved statically at
    // scaffold time so every branch dispatches its own operator agent.
    out.push_str("  F.fanout({\n");
    out.push_str("    items = F.p(\"$.checkers\"),\n");
    out.push_str("    bind  = F.p(\"$.item\"),\n");
    out.push_str("    join  = \"all\",\n");
    out.push_str("    out   = F.p(\"$.results\"),\n");
    // Bodies dispatched in parallel: one step per checker, each reading
    // its own `$.d.<checker>` slot. seq-of-1 keeps the body a Node
    // rather than a bare Step, matching the branch shape flow.ir
    // expects.
    out.push_str("    body  = F.seq({\n");
    for checker in checkers {
        out.push_str(&format!(
            "      F.step({{ agent = \"{checker}\", input = F.p(\"$.d.{checker}\"), out = F.p(\"$.branch_out\") }}),\n"
        ));
    }
    out.push_str("    }),\n");
    out.push_str("  }),\n");
    out.push_str(&format!(
        "  F.step({{ agent = \"{aggregate}\", input = F.p(\"$.results\"), out = F.p(\"$.{aggregate}\") }}),\n"
    ));
    out.push_str("})\n\n");
    out.push_str(&format!("return {{\n  id = \"{name}\",\n  flow = flow,\n"));
    out.push_str("  agents = {\n");
    for stage in checkers.iter().map(String::as_str).chain([aggregate]) {
        out.push_str(&format!(
            "    {{ name = \"{stage}\", kind = \"operator\",\n      \
             spec = {{ operator_ref = \"{operator}\" }},\n      \
             profile = {{ system_prompt = \"TODO: describe {stage}\", tools = {{}} }},\n      \
             runner = {{ backend = \"ws_operator\", variant = \"{binding}\", tools = {{}} }} }},\n"
        ));
    }
    out.push_str("  },\n");
    out.push_str(&format!(
        "  operators = {{ {{ name = \"{operator}\", kind = \"main_ai\" }} }},\n"
    ));
    out.push_str("  strategy = { strict_refs = true, strict_kind = true },\n");
    out.push_str(&format!(
        "  metadata = {{ description = \"TODO: describe {name}\" }},\n"
    ));
    out.push_str("}\n");
    out
}

/// GH #62 Axis B.1: structured hint attached to a compile-lint failure
/// so authors see a concrete "add this line, here" instead of only the
/// Compiler's symptom text (e.g. `missing field 'at'` naming a JSON
/// shape violation without naming which DSL knob to add). The sibling
/// `mse bp fix` auto-apply command (Axis B.2) is deliberately out of
/// scope — the hint is prose the author applies by hand, not a
/// machine-applied patch. Text-substring matching against the raw
/// Compiler error is intentional: the underlying messages are stable
/// literals in the `mlua-swarm` crate (see `src/blueprint/compiler.rs`)
/// and coupling via typed error variants would require re-exporting
/// the full `CompileError` shape through the crate boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FixHint {
    /// Machine-readable lint kind key, stable across the hint content.
    /// Downstream tooling (a future `mse bp fix`, a Clippy-style
    /// diagnostic renderer, etc.) can match on this rather than on the
    /// human-facing `reason` text.
    pub kind: &'static str,
    /// One-line human-readable statement of what the author must add
    /// or change. Written in the imperative ("add … / change …").
    pub reason: String,
    /// The patch text the author would paste in place. Format-agnostic —
    /// may be a single-line snippet or a multi-line block; renderers
    /// indent it as a code block.
    pub patch_suggestion: String,
    /// Optional pointer to a bundled MCP resource or docs section that
    /// explains the underlying contract. `None` when no single guide
    /// covers the lint kind.
    pub docs_ref: Option<String>,
}

/// GH #62 Axis B.1: pattern-match on a compile-lint failure message
/// and return a canonical [`FixHint`] for known lint kinds. Returns
/// `None` for lint failures without a canonical fix recipe — the
/// author sees the raw Compiler message and no synthesized hint,
/// avoiding a wrong-but-confident "fix" for cases requiring
/// surrounding-context judgment (per GH #62 Axis B "never a wrong
/// `mse bp fix` command").
pub(crate) fn fix_hint_from_compile_error(err_msg: &str) -> Option<FixHint> {
    // GH #61 — profile.worker_binding required for the WS thin-path
    // backend. The Compiler emits the agent name in single quotes:
    //   "agent 'greeter' spec invalid: profile.worker_binding is required..."
    if err_msg.contains("profile.worker_binding is required") {
        let agent = extract_between(err_msg, "agent '", "'");
        let reason = match agent {
            Some(name) => format!(
                "operator agent '{name}' has no explicit Runner or legacy `profile.worker_binding`"
            ),
            None => {
                "an operator agent has no explicit Runner or legacy `profile.worker_binding`".into()
            }
        };
        return Some(FixHint {
            kind: "worker-binding-missing",
            reason,
            patch_suggestion:
                "runner = { backend = \"ws_operator\", variant = \"claude\", tools = {} }".into(),
            docs_ref: Some("mse://guides/bp-dsl-templates".into()),
        });
    }
    // GH #50 — verdict contract mismatch. Compiler wording:
    //   "value 'X' is not a member of the declared values [...]"
    if err_msg.contains("is not a member of the declared values") {
        return Some(FixHint {
            kind: "verdict-value-not-in-contract",
            reason: "a Branch / Loop cond literal is outside its agent's declared verdict contract (`agents[N].verdict.values`)".into(),
            patch_suggestion: "either add the cond's literal to `agents[N].verdict.values`, or change the cond to a value that is already declared".into(),
            docs_ref: Some("mse://guides/blueprint-authoring".into()),
        });
    }
    // GH #60 — B.pipeline default landed in commit 31d9c8e so most
    // `bp_dsl` authors no longer hit this, but JSON-direct Blueprints
    // and hand-rolled `flow_dsl` shapes with a halt-on rule still can.
    if err_msg.contains("missing field `at`") || err_msg.contains("halted_at") {
        return Some(FixHint {
            kind: "halted-at-missing",
            reason: "the flow declares a halt-on rule but has no `halted_at` sink — where should the halted-stage id land in ctx?".into(),
            patch_suggestion:
                "halted_at = \"$.halted_at\",  -- add inside the B.pipeline { ... } block, before `done = ...`"
                    .into(),
            docs_ref: Some("mse://guides/bp-dsl-templates".into()),
        });
    }
    None
}

/// GH #79 Phase 2: derive the unified [`mlua_swarm_diag::Diagnostic`]
/// for a compile-lint failure. Two paths, tried in order:
///
/// 1. **Typed** — walk the anyhow chain for the
///    [`mlua_swarm::CompileError`] that [`compile_lint`] preserved and
///    project it via `Diagnostic::from(&err)` (no substring re-parse;
///    the variant's typed fields carry over).
/// 2. **String fallback** — the halted-at case is a serde shape error
///    (`missing field \`at\``), not a `CompileError`, so it is still
///    recognized from the formatted message and synthesized here.
///
/// `None` when neither path recognizes the failure — the caller then
/// falls back to the raw error text alone.
pub(crate) fn diagnostic_for_error(err: &anyhow::Error) -> Option<mlua_swarm_diag::Diagnostic> {
    if let Some(ce) = err
        .chain()
        .find_map(|c| c.downcast_ref::<mlua_swarm::CompileError>())
    {
        return Some(mlua_swarm_diag::Diagnostic::from(ce));
    }
    let msg = format!("{err:#}");
    if msg.contains("missing field `at`") || msg.contains("halted_at") {
        return Some(halted_at_missing_diagnostic());
    }
    None
}

/// The `halted-at-missing` [`mlua_swarm_diag::Diagnostic`] — synthesized
/// on the CLI side because the underlying failure is a serde shape
/// error (the Blueprint deserializer, not `Compiler::compile`) and thus
/// never reaches the typed `From<&CompileError>` path.
fn halted_at_missing_diagnostic() -> mlua_swarm_diag::Diagnostic {
    use mlua_swarm_diag::{Applicability, DiagLevel, DiagStage, Diagnostic, DocsRef, Suggestion};
    Diagnostic::new(
        "halted-at-missing",
        DiagStage::CompileLint,
        DiagLevel::Error,
        "the flow declares a halt-on rule but has no `halted_at` sink — where should the \
         halted-stage id land in ctx?",
    )
    .with_suggestion(Suggestion {
        msg: "add a `halted_at` sink to the pipeline block".into(),
        patch: "halted_at = \"$.halted_at\",  -- add inside the B.pipeline { ... } block, \
                before `done = ...`"
            .into(),
        applicability: Applicability::MaybeIncorrect,
    })
    .with_docs_ref(DocsRef {
        uri: "mse://guides/bp-dsl-templates",
        anchor: None,
    })
}

/// GH #79 Phase 2: the backward-compatibility adapter — wrap a
/// [`mlua_swarm_diag::Diagnostic`] in the existing [`FixHint`] shape.
/// Only the three kinds with a canonical fix recipe produce a hint
/// (same closed set the string matcher recognizes); everything else is
/// `None`, preserving the "never a wrong-but-confident hint" contract
/// (GH #62 Axis B).
pub(crate) fn fix_hint_from_diagnostic(d: &mlua_swarm_diag::Diagnostic) -> Option<FixHint> {
    const FIXABLE_KINDS: [&str; 3] = [
        "worker-binding-missing",
        "verdict-value-not-in-contract",
        "halted-at-missing",
    ];
    if !FIXABLE_KINDS.contains(&d.kind) {
        return None;
    }
    let suggestion = d.suggestion.as_ref()?;
    Some(FixHint {
        kind: d.kind,
        reason: d.message.clone(),
        patch_suggestion: suggestion.patch.clone(),
        docs_ref: d.docs_ref.map(|r| r.uri.to_string()),
    })
}

/// GH #79 Phase 2: preferred [`FixHint`] entry point for callers holding
/// the full error — routes through the typed [`diagnostic_for_error`]
/// path first, then falls back to the legacy string matcher
/// ([`fix_hint_from_compile_error`]) so failures whose typed error was
/// severed somewhere in the chain keep their hint.
pub(crate) fn fix_hint_for_error(err: &anyhow::Error) -> Option<FixHint> {
    if let Some(hint) = diagnostic_for_error(err)
        .as_ref()
        .and_then(fix_hint_from_diagnostic)
    {
        return Some(hint);
    }
    fix_hint_from_compile_error(&format!("{err:#}"))
}

/// Substring helper for [`fix_hint_from_compile_error`]. Returns the slice
/// between the first occurrence of `prefix` and the next occurrence of
/// `suffix` after it, or `None` if either isn't found. Non-greedy —
/// stops at the first `suffix` match, so `agent 'foo' ...` extracts
/// `foo`.
fn extract_between<'a>(s: &'a str, prefix: &str, suffix: &str) -> Option<&'a str> {
    let start = s.find(prefix)? + prefix.len();
    let rest = s.get(start..)?;
    let end = rest.find(suffix)?;
    Some(&rest[..end])
}

/// How a [`compile_lint`] invocation concluded (a lint *failure* is the
/// `Err` arm of the `Result`, not a variant here). Shared with the
/// `bp_build` MCP tool (`crate::mcp`), which reports it as a response
/// field instead of printing to stderr.
pub(crate) enum LintReport {
    /// The full compile lint ran against the resolved Blueprint.
    Ok { agents: usize, operators: usize },
    /// The compile lint ran, but the linker only partially resolved the
    /// Blueprint (e.g. some `$file`/`$agent_md` refs were unresolvable
    /// against the include cascade so only the static DSL shape was
    /// validated). Not a hard failure — `mse bp lint` reports this as
    /// verdict `WARN` (exit 0 unless `--strict`), and `mse bp build`
    /// emits the raw wire JSON (Phase 5 `--strict-embed` promotes this
    /// to a hard error). Carries a structured `warnings` list in
    /// addition to `reason` — Phase 4 replacement for the legacy
    /// `Skipped` variant (removed).
    Warn {
        agents: usize,
        operators: usize,
        reason: String,
        warnings: Vec<String>,
    },
}

/// Tier 6 of the include cascade — the bundled `agent.md` samples
/// shipped inside this crate's source tree
/// (`src/mcp/resources/samples/agents/`). Resolved from
/// `CARGO_MANIFEST_DIR` at compile time; returns `None` when that
/// directory is not on disk at run time (e.g. the crate source tree
/// was pruned after `cargo install` copied the binary out) so the
/// linker simply skips the tier instead of erroring on a stale path.
///
/// This is a CLI-only wiring: the server binary does not ship
/// authoring samples, so `mse serve`'s `seed_blueprint` never
/// registers a bundled default (tier 6 stays unused server-side).
/// The full cascade — and this tier's role in it — is documented in
/// `mse://guides/blueprint-ref-paths`.
fn bundled_agents_dir() -> Option<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/mcp/resources/samples/agents");
    dir.is_dir().then_some(dir)
}

/// Step 2 of the module doc's pipeline: best-effort compile lint. Never
/// hard-fails on an unresolved `$agent_md`/`$file` ref — that's the
/// server's job at register time via its own `--blueprint-ref-base` —
/// but always reports explicitly when it had to skip (no silent skip).
///
/// `cli_includes` are the `--include <DIR>` flag values from the outer
/// `mse bp build` command (tier 4 of the include cascade). The lint
/// itself also picks up in-bp `blueprint_ref_includes` (tier 2),
/// `MSE_BLUEPRINT_INCLUDES` (tier 3), and the bundled samples dir
/// (tier 6, see [`bundled_agents_dir`]).
pub(crate) fn compile_lint(
    bp_value: &serde_json::Value,
    script_path: &Path,
    cli_includes: &[PathBuf],
) -> Result<LintReport> {
    use mlua_swarm_compile::{
        env_blueprint_includes, expand_file_refs_with_config, pre_read_in_bp_includes,
        ResolveConfig,
    };
    let base = script_path.parent().unwrap_or_else(|| Path::new("."));
    let default_kind = mlua_swarm::blueprint::loader::pre_read_default_agent_kind(bp_value);
    let cfg = ResolveConfig::new(base.to_path_buf())
        .with_in_bp_includes(pre_read_in_bp_includes(bp_value))
        .with_env_includes(env_blueprint_includes())
        .with_cli_includes(cli_includes.to_vec())
        .with_bundled_default(bundled_agents_dir());
    let expanded = match expand_file_refs_with_config(bp_value.clone(), &cfg, default_kind) {
        Ok(v) => v,
        Err(e) => {
            // Phase 4: promoted the legacy `Skipped` semantic to the
            // structured `Warn` variant. `mse bp lint` reports this as
            // verdict `WARN` (exit 0 unless `--strict`); `mse bp build`
            // still emits the raw wire JSON (Phase 5's `--strict-embed`
            // promotes this to a hard error). The `Skipped` variant is
            // kept in the enum for MCP-caller back-compat but is no
            // longer emitted from this codepath.
            let reason = format!(
                "could not resolve $file/$agent_md refs relative to {} ({e}). Only the \
                 static DSL shape was validated; the server resolves these refs against \
                 its own include cascade at register time.",
                base.display()
            );
            return Ok(LintReport::Warn {
                agents: 0,
                operators: 0,
                reason,
                warnings: vec![format!("unresolved refs: {e}")],
            });
        }
    };
    let bp: mlua_swarm::Blueprint = serde_json::from_value(expanded).map_err(|e| {
        anyhow!("compile lint: blueprint shape invalid after $agent_md expansion: {e}")
    })?;

    let registry = lint_registry(&bp);
    // GH #79 Phase 2: keep the typed `CompileError` in the anyhow chain
    // (`Error::new` + `context`) instead of formatting it away — the
    // downstream fix-hint / Diagnostic path downcasts it back out
    // (`diagnostic_for_error`). `{e:#}` renders identically to the old
    // `anyhow!("compile lint FAILED: {e}")` string.
    Compiler::new(registry)
        .compile(&bp)
        .map_err(|e| anyhow::Error::new(e).context("compile lint FAILED"))?;
    Ok(LintReport::Ok {
        agents: bp.agents.len(),
        operators: bp.operators.len(),
    })
}

/// A stub `Operator` backend used only so `kind = operator` agents (the
/// `$agent_md` loader's default kind) resolve during lint — no live WS
/// session exists at authoring time, so `execute` is never actually
/// called. `requires_worker_binding() = true` mirrors the production
/// `WSOperatorSession` (the only real operator backend `mse serve` ships)
/// so the compile-lint fails loud at authoring time when
/// `profile.worker_binding` is absent — the same fail-loud gate the
/// Compiler applies at dispatch (`src/blueprint/compiler.rs` — the
/// `profile.worker_binding is required` path), just front-loaded into
/// `bp_build`'s lint stage so an author sees the failure before the
/// undispatchable Blueprint is registered. GH #61.
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
            stats: None,
        })
    }
    fn requires_worker_binding(&self) -> bool {
        true
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

/// A successful `--register` POST, for the caller to report (the CLI
/// prints it to stderr; the `bp_build` MCP tool returns it as response
/// fields).
pub(crate) struct RegisterOutcome {
    pub url: String,
    pub http_status: u16,
    pub body: String,
}

/// Step 4: `--register`. Failure exits non-zero with a message; no retry.
pub(crate) async fn register(
    bp_value: &serde_json::Value,
    server: Option<&str>,
) -> Result<RegisterOutcome> {
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
    Ok(RegisterOutcome {
        url,
        http_status: status.as_u16(),
        body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── GH #62 Axis A: template rendering + round-trip through Compiler ───

    fn build_and_compile_lint(rendered: &str) -> Result<LintReport> {
        let bp_value = dsl::build_bp_from_script(rendered)?;
        // No `$file` / `$agent_md` refs in rendered templates → the
        // `script_path` arg is only used as the parent for ref
        // resolution, so any path resolves (the loader never touches disk
        // when the Blueprint carries no refs).
        compile_lint(&bp_value, Path::new("/tmp/nonexistent.bp.lua"), &[])
    }

    #[test]
    fn pipeline_template_round_trips_with_defaults() {
        let rendered =
            render_template_by_kind("pipeline", "roundtrip-pipe", None, None, None, None)
                .expect("render must succeed with defaults");
        // Sanity: the two mandatory fields GH #61 / GH #60 tightened must
        // both appear in the rendered text.
        assert!(rendered.contains("backend = \"ws_operator\", variant = \"claude\""));
        assert!(rendered.contains("halted_at = \"$.halted_at\""));
        // GH #66: operator entry must pre-declare `kind = "main_ai"` so the
        // BP Agent-level tier of the OperatorKind cascade resolves without
        // requiring a `swarm_run(operator_kind = ...)` override.
        assert!(rendered.contains("kind = \"main_ai\""));
        // Round-trip through the real Compiler — this is the AC.
        let report = build_and_compile_lint(&rendered).expect("compile lint must succeed");
        match report {
            LintReport::Ok { agents, operators } => {
                assert_eq!(agents, DEFAULT_PIPELINE_STAGES.len());
                assert_eq!(operators, 1);
            }
            LintReport::Warn { reason, .. } => panic!("expected Ok, got Warn: {reason}"),
        }
    }

    #[test]
    fn pipeline_template_documents_init_ctx_seeding() {
        // GH #64: the header must tell the author that each stage reads
        // from `$.d.<stage>` and needs an init_ctx seed at launch —
        // without this the golden `bp new pipeline → bp build → swarm_run`
        // path silent-fails at flow eval with `path not found: $.d.<first>`.
        let rendered = render_template_by_kind(
            "pipeline",
            "seed-doc",
            Some("ingest,transform,emit"),
            None,
            None,
            None,
        )
        .expect("render must succeed");
        assert!(
            rendered.contains("$.d.<stage_name>"),
            "header must name the input-path convention verbatim"
        );
        assert!(
            rendered.contains(
                "init_ctx = { d = { ingest = \"...\", transform = \"...\", emit = \"...\" } }"
            ),
            "header must ship a concrete init_ctx sample tied to the actual stage names"
        );
        assert!(
            rendered.contains("mse://guides/bp-dsl-templates"),
            "header must link to the guide covering the convention"
        );
    }

    #[test]
    fn all_templates_pre_declare_operator_kind_main_ai() {
        // GH #66: silent-fall-through fix. Every emitted template must
        // include `kind = "main_ai"` inside its `operators = { ... }`
        // block so a `swarm_run` invocation without an explicit
        // `operator_kind` resolves through the BP Agent-level tier to
        // `MainAi` instead of dropping to the default `Automate` — the
        // silent fall-through the issue documents. Assert exactly the
        // literal shape (`name = "..."` followed by `kind = "main_ai"`
        // in the same operator entry) so a future refactor that splits
        // the entry across lines still passes iff both fields land on
        // the same entry.
        for template in ["pipeline", "single", "verdict"] {
            let rendered =
                render_template_by_kind(template, "op-kind-check", None, None, None, None)
                    .expect("render must succeed with defaults");
            assert!(
                rendered.contains("operators = { { name = \"main-ai\", kind = \"main_ai\" } }"),
                "{template} template must emit an operator entry with `kind = \"main_ai\"` pre-declared, got: {rendered}"
            );
        }
    }

    #[test]
    fn pipeline_template_honours_stages_operator_binding_flags() {
        let rendered = render_template_by_kind(
            "pipeline",
            "roundtrip-pipe-3",
            Some("greet,echo,farewell"),
            None,
            Some("primary"),
            Some("claude-lite"),
        )
        .expect("render must succeed with custom flags");
        assert!(rendered.contains("backend = \"ws_operator\", variant = \"claude-lite\""));
        assert!(rendered.contains("operator_ref = \"primary\""));
        // 3 stages requested → 3 agents rendered.
        assert_eq!(rendered.matches("kind = \"operator\"").count(), 3);
        let report = build_and_compile_lint(&rendered).expect("compile lint must succeed");
        assert!(matches!(
            report,
            LintReport::Ok {
                agents: 3,
                operators: 1
            }
        ));
    }

    #[test]
    fn single_template_round_trips_with_defaults() {
        let rendered =
            render_template_by_kind("single", "roundtrip-single", None, None, None, None)
                .expect("render must succeed with defaults");
        assert!(rendered.contains(&format!("variant = \"{DEFAULT_BINDING}\"")));
        assert!(rendered.contains(&format!("operator_ref = \"{DEFAULT_OPERATOR}\"")));
        assert!(rendered.contains(&format!("agent = \"{DEFAULT_SINGLE_AGENT}\"")));
        let report = build_and_compile_lint(&rendered).expect("compile lint must succeed");
        assert!(matches!(
            report,
            LintReport::Ok {
                agents: 1,
                operators: 1
            }
        ));
    }

    #[test]
    fn verdict_template_round_trips_with_defaults() {
        let rendered =
            render_template_by_kind("verdict", "roundtrip-verdict", None, None, None, None)
                .expect("render must succeed with defaults");
        // Verdict template ships 3 stage agents + 1 fixer agent.
        assert_eq!(rendered.matches("kind = \"operator\"").count(), 4);
        assert!(rendered.contains("verdict = { channel = \"part\""));
        let report = build_and_compile_lint(&rendered).expect("compile lint must succeed");
        assert!(matches!(
            report,
            LintReport::Ok {
                agents: 4,
                operators: 1
            }
        ));
    }

    #[test]
    fn verdict_template_documents_init_ctx_seeding() {
        // GH #64: the verdict template shares the pipeline sugar and
        // therefore inherits the same `$.d.<stage>` seeding requirement.
        let rendered =
            render_template_by_kind("verdict", "seed-doc-verdict", None, None, None, None)
                .expect("render must succeed with defaults");
        assert!(
            rendered.contains("$.d.<stage_name>"),
            "verdict header must name the input-path convention verbatim"
        );
        let [analyze, review, publish] = &DEFAULT_VERDICT_STAGES;
        assert!(
            rendered.contains(&format!(
                "init_ctx = {{ d = {{ {analyze} = \"...\", {review} = \"...\", {publish} = \"...\" }} }}"
            )),
            "verdict header must ship a concrete init_ctx sample tied to the 3 canonical stage names"
        );
        assert!(
            rendered.contains("mse://guides/bp-dsl-templates"),
            "verdict header must link to the guide covering the convention"
        );
    }

    #[test]
    fn verdict_template_stage_override_stays_3_slot() {
        // Fewer than 3 → remaining slots use defaults.
        let rendered =
            render_template_by_kind("verdict", "rv-partial", Some("scan"), None, None, None)
                .expect("render must succeed with partial stages");
        assert!(rendered.contains("B.stage \"scan\""));
        // Slot 2 / 3 fall back to defaults.
        assert!(rendered.contains(&format!("B.stage \"{}\"", DEFAULT_VERDICT_STAGES[1])));
        assert!(rendered.contains(&format!("B.stage \"{}\"", DEFAULT_VERDICT_STAGES[2])));
        // Extra names are truncated (>3 supplied → tail dropped).
        let over =
            render_template_by_kind("verdict", "rv-over", Some("a,b,c,d,e"), None, None, None)
                .expect("render must succeed with over-supplied stages");
        assert!(!over.contains("B.stage \"d\""));
        assert!(!over.contains("B.stage \"e\""));
    }

    #[test]
    fn unknown_template_returns_error_naming_accepted_list() {
        let err = render_template_by_kind("bogus", "x", None, None, None, None)
            .expect_err("unknown template must error");
        let msg = format!("{err:#}");
        assert!(msg.contains("unknown template 'bogus'"));
        // Accepted list appears in the error so an author who typos sees
        // the closed set. GH #82 grew this set to include `fanout`.
        assert!(msg.contains("pipeline"));
        assert!(msg.contains("single"));
        assert!(msg.contains("verdict"));
        assert!(msg.contains("fanout"));
    }

    // ─── GH #82: fanout template ─────────────────────────────────────

    #[test]
    fn fanout_template_round_trips_with_defaults() {
        let rendered =
            render_template_by_kind("fanout", "roundtrip-fanout", None, None, None, None)
                .expect("render must succeed with defaults");
        // Default: 2 parallel checkers + 1 aggregate = 3 operator agents,
        // all pointing at the single main-ai operator.
        assert_eq!(rendered.matches("kind = \"operator\"").count(), 3);
        assert!(rendered.contains("F.fanout({"));
        assert!(rendered.contains("join  = \"all\","));
        for checker in DEFAULT_FANOUT_STAGES {
            assert!(
                rendered.contains(&format!("agent = \"{checker}\"")),
                "default checker '{checker}' missing"
            );
        }
        assert!(rendered.contains(&format!("agent = \"{FANOUT_AGGREGATE_STAGE}\"")));
        let report = build_and_compile_lint(&rendered).expect("compile lint must succeed");
        assert!(matches!(
            report,
            LintReport::Ok {
                agents: 3,
                operators: 1
            }
        ));
    }

    #[test]
    fn fanout_template_honours_stages_flag() {
        let rendered = render_template_by_kind(
            "fanout",
            "custom-fanout",
            Some("lint,test,build"),
            None,
            None,
            None,
        )
        .expect("render must succeed with 3 stages");
        // 3 checkers + aggregate = 4 operator agents.
        assert_eq!(rendered.matches("kind = \"operator\"").count(), 4);
        for checker in ["lint", "test", "build"] {
            assert!(
                rendered.contains(&format!("agent = \"{checker}\"")),
                "custom checker '{checker}' missing"
            );
        }
        assert!(rendered.contains(&format!("agent = \"{FANOUT_AGGREGATE_STAGE}\"")));
        let report = build_and_compile_lint(&rendered).expect("compile lint must succeed");
        assert!(matches!(
            report,
            LintReport::Ok {
                agents: 4,
                operators: 1
            }
        ));
    }

    #[test]
    fn fanout_template_documents_init_ctx_seeding() {
        // GH #82 mirrors GH #64: fanout shares the `$.d.<stage>` seeding
        // convention every other operator-agent template uses.
        let rendered = render_template_by_kind(
            "fanout",
            "seed-doc-fanout",
            Some("lint,test"),
            None,
            None,
            None,
        )
        .expect("render must succeed");
        assert!(
            rendered.contains("$.d.lint"),
            "each checker must read its input from $.d.<checker>"
        );
        assert!(
            rendered.contains("init_ctx = { d = { lint = \"...\", test = \"...\" } }"),
            "fanout header must ship a concrete init_ctx sample tied to the checker names"
        );
        assert!(
            rendered.contains("mse://guides/bp-dsl-templates"),
            "fanout header must link to the guide covering the convention"
        );
    }

    #[test]
    fn fanout_template_with_empty_stages_flag_falls_back_to_defaults() {
        // parse_stages returns empty for `--stages ""`; render_fanout_template
        // recovers by recursing with DEFAULT_FANOUT_STAGES rather than
        // emitting an empty fanout body (which would fail compile-lint).
        let rendered = render_template_by_kind("fanout", "recov", Some(""), None, None, None)
            .expect("render must succeed with empty stages");
        for checker in DEFAULT_FANOUT_STAGES {
            assert!(
                rendered.contains(&format!("agent = \"{checker}\"")),
                "empty-stages fallback must include default checker '{checker}'"
            );
        }
        build_and_compile_lint(&rendered).expect("empty-stages fallback must compile-lint");
    }

    // ─── GH #62 Axis B.1: fix_hint pattern matching ─────────────────

    #[test]
    fn fix_hint_worker_binding_extracts_agent_name_and_names_kind() {
        let msg = "compile lint FAILED: agent 'greeter' spec invalid: \
                   profile.worker_binding is required for this operator backend. Fix by either: \
                   (a) if authoring the Blueprint JSON directly, ...";
        let hint = fix_hint_from_compile_error(msg).expect("worker_binding hint must fire");
        assert_eq!(hint.kind, "worker-binding-missing");
        assert!(hint.reason.contains("greeter"));
        assert!(hint.patch_suggestion.contains("backend = \"ws_operator\""));
        assert_eq!(
            hint.docs_ref.as_deref(),
            Some("mse://guides/bp-dsl-templates")
        );
    }

    #[test]
    fn fix_hint_worker_binding_reason_falls_back_when_no_agent_quoted() {
        let msg =
            "compile lint FAILED: profile.worker_binding is required for this operator backend.";
        let hint = fix_hint_from_compile_error(msg).expect("worker_binding hint must fire");
        // Fallback reason (no agent name parsed) still names the kind
        // and remedy.
        assert!(hint.reason.contains("operator agent"));
        assert!(hint.reason.contains("explicit Runner"));
    }

    #[test]
    fn fix_hint_verdict_contract_mismatch_names_the_contract_field() {
        let msg = "compile lint FAILED: value 'NOT_DECLARED' is not a member of the declared values [\"PASS\", \"BLOCKED\"]";
        let hint = fix_hint_from_compile_error(msg).expect("verdict hint must fire");
        assert_eq!(hint.kind, "verdict-value-not-in-contract");
        assert!(hint.reason.contains("`agents[N].verdict.values`"));
        assert!(hint.patch_suggestion.contains("add the cond's literal"));
    }

    #[test]
    fn fix_hint_halted_at_fires_on_missing_field_at() {
        let msg =
            "compile lint FAILED: missing field `at` (hint: fetch the Blueprint JSON Schema...)";
        let hint = fix_hint_from_compile_error(msg).expect("halted_at hint must fire");
        assert_eq!(hint.kind, "halted-at-missing");
        assert!(hint
            .patch_suggestion
            .contains("halted_at = \"$.halted_at\""));
    }

    #[test]
    fn fix_hint_returns_none_for_unknown_lint_shape() {
        // A lint kind without a canonical fix recipe returns None so
        // the caller never renders a wrong-but-confident hint.
        assert!(
            fix_hint_from_compile_error("some new lint the mapping doesn't know about").is_none()
        );
        assert!(fix_hint_from_compile_error("").is_none());
    }

    // ─── GH #79 Phase 2: typed Diagnostic path + FixHint adapter ─────

    #[test]
    fn typed_worker_binding_error_routes_through_diagnostic_to_fix_hint() {
        let ce = mlua_swarm::CompileError::InvalidSpec {
            name: "greeter".into(),
            msg: format!(
                "{}. Fix by either: (a) ...",
                mlua_swarm::WORKER_BINDING_REQUIRED_MSG_PREFIX
            ),
        };
        let err = anyhow::Error::new(ce).context("compile lint FAILED");
        let d = diagnostic_for_error(&err).expect("typed path must recognize CompileError");
        assert_eq!(d.kind, "worker-binding-missing");
        let hint = fix_hint_for_error(&err).expect("hint must derive from the diagnostic");
        assert_eq!(hint.kind, "worker-binding-missing");
        assert!(hint.reason.contains("greeter"));
        assert!(hint.patch_suggestion.contains("backend = \"ws_operator\""));
        assert_eq!(
            hint.docs_ref.as_deref(),
            Some("mse://guides/bp-dsl-templates")
        );
    }

    #[test]
    fn typed_non_fixable_error_yields_diagnostic_but_no_hint() {
        // A kind without a canonical fix recipe still produces the
        // unified Diagnostic (for the additive `diagnostic` response
        // field) but no FixHint — the GH #62 "never a wrong-but-confident
        // hint" contract carries over to the typed path.
        let ce = mlua_swarm::CompileError::DuplicateAgent("scout".into());
        let err = anyhow::Error::new(ce).context("compile lint FAILED");
        let d = diagnostic_for_error(&err).expect("typed path must recognize CompileError");
        assert_eq!(d.kind, "duplicate-agent-name");
        assert!(fix_hint_from_diagnostic(&d).is_none());
        assert!(fix_hint_for_error(&err).is_none());
    }

    #[test]
    fn halted_at_serde_error_synthesizes_the_diagnostic_on_the_string_path() {
        // The halted-at case is a serde shape error, not a CompileError —
        // the string fallback must still synthesize the Diagnostic and
        // the derived hint.
        let err = anyhow!("compile lint: blueprint shape invalid after $agent_md expansion: missing field `at`");
        let d = diagnostic_for_error(&err).expect("string path must recognize the serde error");
        assert_eq!(d.kind, "halted-at-missing");
        let hint = fix_hint_for_error(&err).expect("halted-at hint must fire");
        assert_eq!(hint.kind, "halted-at-missing");
        assert!(hint.patch_suggestion.contains("halted_at = \"$.halted_at\""));
    }

    #[test]
    fn compile_lint_preserves_the_typed_compile_error_in_the_chain() {
        // End-to-end over compile_lint: a rendered template stripped of
        // its runner must fail with a downcastable CompileError so the
        // typed Diagnostic path works on real compile_lint output.
        let rendered = render_single_template("solo-bp", "solo", "main-ai", "claude");
        let runner_clause =
            "runner = { backend = \"ws_operator\", variant = \"claude\", tools = {} } },";
        assert!(
            rendered.contains(runner_clause),
            "template shape drifted; update the runner_clause literal"
        );
        // The clause's trailing `},` closes the agent table — keep it.
        let stripped = rendered.replace(runner_clause, "},");
        let bp_value = dsl::build_bp_from_script(&stripped).expect("script must build");
        let err = compile_lint(&bp_value, Path::new("/tmp/nonexistent.bp.lua"), &[])
            .err()
            .expect("compile lint must fail without a runner");
        assert!(
            err.chain()
                .any(|c| c.downcast_ref::<mlua_swarm::CompileError>().is_some()),
            "typed CompileError must survive the compile_lint anyhow chain: {err:#}"
        );
        let d = diagnostic_for_error(&err)
            .expect("typed CompileError must project into a Diagnostic");
        assert!(
            mlua_swarm_diag::lint_decl(d.kind).is_some(),
            "projected kind '{}' must be a declared lint",
            d.kind
        );
        // `{e:#}` still renders the legacy "compile lint FAILED: ..."
        // prefix the string matcher (and existing callers) rely on.
        assert!(format!("{err:#}").starts_with("compile lint FAILED: "));
    }

    #[test]
    fn extract_between_returns_first_match_only() {
        assert_eq!(extract_between("agent 'a' 'b'", "agent '", "'"), Some("a"));
        assert_eq!(extract_between("no prefix here", "agent '", "'"), None);
        assert_eq!(extract_between("agent 'unclosed", "agent '", "'"), None);
    }

    #[test]
    fn pipeline_template_with_empty_stages_flag_falls_back_to_defaults() {
        // `--stages ""` at the CLI parses to `Some("")` which becomes
        // an empty Vec; the render fn falls back to the default set
        // rather than emitting a stage-less pipeline (which would be
        // rejected at compile-lint anyway).
        let rendered = render_template_by_kind("pipeline", "rp-empty", Some(""), None, None, None)
            .expect("render must succeed and fall back");
        let report = build_and_compile_lint(&rendered).expect("compile lint must succeed");
        assert!(matches!(
            report,
            LintReport::Ok {
                agents: n,
                operators: 1
            } if n == DEFAULT_PIPELINE_STAGES.len()
        ));
    }
}
