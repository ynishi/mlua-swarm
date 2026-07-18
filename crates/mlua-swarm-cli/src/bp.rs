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
    /// Scaffold a minimal `.bp.lua` from a bundled template with all
    /// currently-mandatory fields pre-filled (`halted_at`, `worker_binding`,
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
    /// `profile.worker_binding` value for every emitted operator agent.
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
        BpCmd::New(new_args) => run_new(new_args),
    }
}

async fn run_build(args: BuildArgs) -> Result<()> {
    let script = std::fs::read_to_string(&args.script)
        .with_context(|| format!("reading {}", args.script.display()))?;
    let bp_value = dsl::build_bp_from_script(&script)
        .with_context(|| format!("building Blueprint from {}", args.script.display()))?;

    match compile_lint(&bp_value, &args.script) {
        Ok(LintReport::Ok { agents, operators }) => {
            eprintln!("compile lint: OK ({agents} agent(s), {operators} operator(s) checked)");
        }
        Ok(LintReport::Skipped { reason }) => {
            eprintln!("compile lint: skipped — {reason}");
        }
        Err(e) => {
            // GH #62 Axis B.1: on a lint failure, render a structured
            // fix hint after the raw Compiler error when the message
            // matches a known lint kind — Clippy-style diagnostic
            // affordance without changing what stderr's exit code
            // signals (still non-zero via the `?` on this arm below).
            let msg = format!("{e:#}");
            if let Some(hint) = fix_hint_from_compile_error(&msg) {
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

/// GH #62 Axis A default `worker_binding` — the Claude Code catch-all
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
        other => Err(anyhow!(
            "unknown template '{other}': accepted = pipeline / single / verdict"
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
    let mut out = String::new();
    out.push_str("-- Scaffolded by `mse bp new pipeline` (GH #62 Axis A).\n");
    out.push_str("-- Every mandatory field is pre-filled: `halted_at` (compile-lint\n");
    out.push_str("-- default), each operator agent's `profile.worker_binding` (WS\n");
    out.push_str("-- thin-path requirement, GH #61), the operator's `kind` (main_ai,\n");
    out.push_str("-- so a caller can omit `operator_kind` at swarm_run time and the\n");
    out.push_str("-- BP Agent-level tier of the OperatorKind cascade routes spawns to\n");
    out.push_str("-- a joined main-ai session instead of silently falling through to\n");
    out.push_str("-- the Automate backend, GH #66), `strict_refs` + `strict_kind`.\n\n");
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
             profile = {{ system_prompt = \"TODO: describe {stage}\", \
             tools = {{}}, worker_binding = \"{binding}\" }} }},\n"
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
    out.push_str("-- sugar. All mandatory fields (`worker_binding`, `strict_refs`,\n");
    out.push_str("-- `strict_kind`) are pre-filled. The operator's `kind` is also\n");
    out.push_str("-- pre-filled as `main_ai` (GH #66) so `swarm_run` can omit\n");
    out.push_str("-- `operator_kind` at launch and spawns route to a joined\n");
    out.push_str("-- main-ai session.\n\n");
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
         profile = {{ system_prompt = \"TODO: describe {agent}\", \
         tools = {{}}, worker_binding = \"{binding}\" }} }},\n"
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
    out.push_str("-- route to a joined main-ai session.\n\n");
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
             profile = {{ system_prompt = \"TODO: describe {stage}\", \
             tools = {{}}, worker_binding = \"{binding}\" }} }},\n"
        ));
    }
    // review: verdict-gated (PASS / BLOCKED).
    out.push_str(&format!(
        "    {{ name = \"{review}\", kind = \"operator\",\n      \
         spec = {{ operator_ref = \"{operator}\" }},\n      \
         profile = {{ system_prompt = \"TODO: stage a `verdict` part = \
         `PASS` or `BLOCKED`, then finish with report body\", \
         tools = {{}}, worker_binding = \"{binding}\" }},\n      \
         verdict = {{ channel = \"part\", values = {{ \"PASS\", \"BLOCKED\" }} }} }},\n"
    ));
    // fixer: plain operator, referenced by retry.fix.
    out.push_str(&format!(
        "    {{ name = \"fixer\", kind = \"operator\",\n      \
         spec = {{ operator_ref = \"{operator}\" }},\n      \
         profile = {{ system_prompt = \"TODO: given the reviewer's report, \
         emit a fix and reply so the review can retry\", \
         tools = {{}}, worker_binding = \"{binding}\" }} }},\n"
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
                "operator agent '{name}' has no `profile.worker_binding` — required for the WS thin-path backend (GH #61)"
            ),
            None => "an operator agent has no `profile.worker_binding` — required for the WS thin-path backend (GH #61)"
                .into(),
        };
        return Some(FixHint {
            kind: "worker-binding-missing",
            reason,
            patch_suggestion:
                "profile = { system_prompt = \"...\", tools = {}, worker_binding = \"claude\" }"
                    .into(),
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
    /// `$file`/`$agent_md` refs could not be resolved locally, so only the
    /// static DSL shape was validated (never a silent skip).
    Skipped { reason: String },
}

/// Step 2 of the module doc's pipeline: best-effort compile lint. Never
/// hard-fails on an unresolved `$agent_md`/`$file` ref — that's the
/// server's job at register time via its own `--blueprint-ref-base` —
/// but always reports explicitly when it had to skip (no silent skip).
pub(crate) fn compile_lint(bp_value: &serde_json::Value, script_path: &Path) -> Result<LintReport> {
    let base = script_path.parent().unwrap_or_else(|| Path::new("."));
    let default_kind = mlua_swarm::blueprint::loader::pre_read_default_agent_kind(bp_value);
    let expanded = match mlua_swarm::expand_file_refs(bp_value.clone(), base, default_kind) {
        Ok(v) => v,
        Err(e) => {
            return Ok(LintReport::Skipped {
                reason: format!(
                    "could not resolve $file/$agent_md refs relative to {} ({e}). Only the \
                     static DSL shape was validated; the server resolves these refs against \
                     its own --blueprint-ref-base at register time.",
                    base.display()
                ),
            });
        }
    };
    let bp: mlua_swarm::Blueprint = serde_json::from_value(expanded).map_err(|e| {
        anyhow!("compile lint: blueprint shape invalid after $agent_md expansion: {e}")
    })?;

    let registry = lint_registry(&bp);
    Compiler::new(registry)
        .compile(&bp)
        .map_err(|e| anyhow!("compile lint FAILED: {e}"))?;
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
        compile_lint(&bp_value, Path::new("/tmp/nonexistent.bp.lua"))
    }

    #[test]
    fn pipeline_template_round_trips_with_defaults() {
        let rendered =
            render_template_by_kind("pipeline", "roundtrip-pipe", None, None, None, None)
                .expect("render must succeed with defaults");
        // Sanity: the two mandatory fields GH #61 / GH #60 tightened must
        // both appear in the rendered text.
        assert!(rendered.contains("worker_binding = \"claude\""));
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
            LintReport::Skipped { reason } => panic!("expected Ok, got Skipped: {reason}"),
        }
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
                rendered
                    .contains("operators = { { name = \"main-ai\", kind = \"main_ai\" } }"),
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
        assert!(rendered.contains("worker_binding = \"claude-lite\""));
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
        assert!(rendered.contains(&format!("worker_binding = \"{DEFAULT_BINDING}\"")));
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
        // the closed set.
        assert!(msg.contains("pipeline"));
        assert!(msg.contains("single"));
        assert!(msg.contains("verdict"));
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
        assert!(hint
            .patch_suggestion
            .contains("worker_binding = \"claude\""));
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
        assert!(hint.reason.contains("`profile.worker_binding`"));
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
