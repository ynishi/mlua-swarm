//! CLI smoke tests for `mse bp build`.

use assert_cmd::Command;
use predicates::str::contains;
use std::fs;

#[test]
fn bp_build_writes_parseable_json_with_top_level_keys() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out_path = tmp.path().join("out.json");

    Command::cargo_bin("mse")
        .expect("mse binary")
        .args(["bp", "build", "tests/fixtures/pipeline.bp.lua", "-o"])
        .arg(&out_path)
        .assert()
        .success();

    let out = fs::read_to_string(&out_path).expect("out.json written");
    let value: serde_json::Value = serde_json::from_str(&out).expect("out.json is valid JSON");
    for key in ["id", "flow", "agents", "operators", "strategy", "metadata"] {
        assert!(value.get(key).is_some(), "missing top-level key: {key}");
    }
}

#[test]
fn bp_build_reports_undeclared_verdict_literal_as_a_compile_error() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let script_path = tmp.path().join("bad_verdict.bp.lua");
    fs::write(
        &script_path,
        r#"
local F = require("flow_dsl")

return {
  id = "bad-verdict-lint",
  flow = F.seq({
    F.step({ agent = "checker", input = F.lit("go"), out = F.p("$.checker") }),
    F.branch({
      cond = F.p('$.checker.parts["verdict"]'):eq("NOT_A_DECLARED_VALUE"),
      on_true = F.assign({ at = F.p("$.halted_at"), value = F.lit("checker") }),
      on_false = F.assign({ at = F.p("$.done"), value = F.lit(true) }),
    }),
  }),
  agents = {
    {
      name = "checker",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      -- worker_binding is set here so this fixture exercises the
      -- GH #50 verdict-contract lint specifically — without it, the
      -- GH #61 worker_binding lint would short-circuit first.
      profile = { system_prompt = "check", tools = {}, worker_binding = "claude" },
      verdict = { channel = "part", values = { "PASS", "BLOCKED" } },
    },
  },
  operators = {
    { name = "main-ai" },
  },
}
"#,
    )
    .expect("write bad-verdict fixture script");

    Command::cargo_bin("mse")
        .expect("mse binary")
        .args(["bp", "build"])
        .arg(&script_path)
        .assert()
        .failure()
        .stderr(contains("compile lint FAILED"))
        .stderr(contains("is not a member of the declared values"))
        // GH #62 Axis B.1: the verdict-contract lint gets a canonical
        // fix hint block; assert on the kind key + the actionable text
        // so a regression in `fix_hint_from_compile_error`'s mapping
        // for this lint kind fails here.
        .stderr(contains("fix hint (verdict-value-not-in-contract)"))
        .stderr(contains("`agents[N].verdict.values`"));
}

/// GH #61: `bp_build` compile-lint must fail loud when an operator-kind
/// agent lacks `profile.worker_binding` — the same fail-loud gate the
/// runtime Compiler applies at dispatch (`src/blueprint/compiler.rs` —
/// `profile.worker_binding is required`), front-loaded into the lint
/// stage so the author sees it before the undispatchable Blueprint is
/// registered.
#[test]
fn bp_build_reports_missing_worker_binding_on_operator_agent_as_a_compile_error() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let script_path = tmp.path().join("missing_worker_binding.bp.lua");
    fs::write(
        &script_path,
        r#"
local B = require("bp_dsl")

local flow = B.pipeline({
  B.stage "greet" { agent = "greeter" },
  halted_at = "$.halted_at",
  done = "$.pipeline_complete",
})

return {
  id = "gh61-missing-worker-binding",
  flow = flow,
  agents = {
    { name = "greeter", kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = { system_prompt = "Say hello.", tools = {} } },  -- worker_binding intentionally missing
  },
  operators = { { name = "main-ai" } },
  strategy = { strict_refs = true, strict_kind = true },
}
"#,
    )
    .expect("write missing-worker-binding fixture script");

    Command::cargo_bin("mse")
        .expect("mse binary")
        .args(["bp", "build"])
        .arg(&script_path)
        .assert()
        .failure()
        .stderr(contains("compile lint FAILED"))
        .stderr(contains("profile.worker_binding is required"))
        // Both fix paths named in the Compiler message.
        .stderr(contains("agents[N].profile.worker_binding"))
        .stderr(contains("$agent_md file ref"))
        // GH #62 Axis B.1: the worker_binding lint gets a canonical
        // fix hint block naming the offending agent and the concrete
        // patch line.
        .stderr(contains("fix hint (worker-binding-missing)"))
        .stderr(contains("operator agent 'greeter'"))
        .stderr(contains("worker_binding = \"claude\""));
}

/// GH #61 regression guard: the bundled `07-dsl-pipeline.bp.lua` sample
/// must still round-trip through `bp build` after every operator agent
/// gained `worker_binding = "claude"` in the same series of edits that
/// tightened compile-lint. If this fails the sample slipped out of sync
/// with the lint.
#[test]
fn bp_build_dsl_pipeline_sample_still_lints_ok_after_worker_binding_tightening() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out_path = tmp.path().join("out.json");

    Command::cargo_bin("mse")
        .expect("mse binary")
        .args([
            "bp",
            "build",
            "src/mcp/resources/samples/07-dsl-pipeline.bp.lua",
            "-o",
        ])
        .arg(&out_path)
        .assert()
        .success()
        // `LintReport::Ok` prints the count line to stderr; assert on
        // it so a regression to `Skipped` (silently degrading the lint
        // to shape-only) also surfaces here.
        .stderr(contains("compile lint: OK"));
}

/// GH #62 Axis A CLI round-trip: `mse bp new pipeline` output must
/// round-trip through `mse bp build` with `compile lint: OK`. Covers the
/// AC on the CLI-invoke path (unit tests already cover the pure render
/// function).
#[test]
fn bp_new_pipeline_scaffold_round_trips_through_bp_build() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let scaffold_path = tmp.path().join("hello.bp.lua");

    Command::cargo_bin("mse")
        .expect("mse binary")
        .args([
            "bp",
            "new",
            "pipeline",
            "hello",
            "--stages",
            "greet,echo",
            "-o",
        ])
        .arg(&scaffold_path)
        .assert()
        .success()
        .stderr(contains("mse bp new: wrote"));

    Command::cargo_bin("mse")
        .expect("mse binary")
        .args(["bp", "build"])
        .arg(&scaffold_path)
        .assert()
        .success()
        .stderr(contains("compile lint: OK"));
}

/// GH #62 Axis A CLI round-trip: `mse bp new single` — the minimal
/// `flow_dsl`-only shape. Separate test so a regression in either
/// template's DSL surface stays specific.
#[test]
fn bp_new_single_scaffold_round_trips_through_bp_build() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let scaffold_path = tmp.path().join("solo.bp.lua");

    Command::cargo_bin("mse")
        .expect("mse binary")
        .args(["bp", "new", "single", "solo-run", "--agent", "solo", "-o"])
        .arg(&scaffold_path)
        .assert()
        .success();

    Command::cargo_bin("mse")
        .expect("mse binary")
        .args(["bp", "build"])
        .arg(&scaffold_path)
        .assert()
        .success()
        .stderr(contains("compile lint: OK"));
}

/// GH #62 Axis A CLI round-trip: `mse bp new verdict` — 3-stage
/// verdict-gated shape. This is the most complex template; if it lints
/// OK from the CLI, the two simpler ones do too, but each has its own
/// test to keep failure attribution specific.
#[test]
fn bp_new_verdict_scaffold_round_trips_through_bp_build() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let scaffold_path = tmp.path().join("review-loop.bp.lua");

    Command::cargo_bin("mse")
        .expect("mse binary")
        .args(["bp", "new", "verdict", "review-loop", "-o"])
        .arg(&scaffold_path)
        .assert()
        .success();

    Command::cargo_bin("mse")
        .expect("mse binary")
        .args(["bp", "build"])
        .arg(&scaffold_path)
        .assert()
        .success()
        .stderr(contains("compile lint: OK"));
}

/// Linker include-cascade Warn/strict-embed:`mse bp build` on a `.bp.lua` whose
/// `$agent_md` ref can not be resolved through the include cascade must
/// still emit the raw wire JSON (refs preserved) and exit zero — the
/// server resolves refs itself at register time. The lint report is
/// `compile lint: WARN`. Regression guard for the Phase 4 → Phase 5
/// default: an unresolved ref never silently degrades the emit path.
#[test]
fn bp_build_unresolved_ref_default_emits_raw_and_warns() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let script_path = tmp.path().join("unresolved.bp.lua");
    let out_path = tmp.path().join("out.json");
    // Reference an `agent.md` file that does not exist in the script's
    // parent dir, in-bp includes, env includes, CLI includes, or the
    // bundled default samples dir — so the include cascade hits none of
    // the 6 tiers and the linker returns Err (→ LintReport::Warn).
    fs::write(
        &script_path,
        r#"
local F = require("flow_dsl")

return {
  id = "unresolved-ref-warn",
  flow = F.step({ id = "solo", agent = "solo", input = F.lit(""), out = F.p("$.solo") }),
  agents = {
    {
      name = "solo",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      ["$agent_md"] = "definitely-not-present.md",
    },
  },
  operators = { { name = "main-ai", kind = "main_ai" } },
  strategy = { strict_refs = true, strict_kind = true },
}
"#,
    )
    .expect("write unresolved-ref fixture script");

    Command::cargo_bin("mse")
        .expect("mse binary")
        .args(["bp", "build"])
        .arg(&script_path)
        .args(["-o"])
        .arg(&out_path)
        .assert()
        .success()
        .stderr(contains("compile lint: WARN"))
        .stderr(contains("could not resolve $file/$agent_md refs"));

    let out = fs::read_to_string(&out_path).expect("out.json written");
    let value: serde_json::Value = serde_json::from_str(&out).expect("out.json is valid JSON");
    // Raw wire JSON: the `$agent_md` ref is preserved verbatim on the
    // agent entry — the server will resolve it at register time.
    let agent = value
        .get("agents")
        .and_then(|a| a.get(0))
        .expect("agents[0] present");
    assert_eq!(
        agent.get("$agent_md").and_then(|v| v.as_str()),
        Some("definitely-not-present.md"),
        "raw emit must preserve the unresolved $agent_md ref"
    );
}

/// Linker include-cascade Warn/strict-embed:`--strict-embed` promotes the same
/// unresolved-ref WARN to a hard failure — non-zero exit, no JSON
/// emitted. Mirrors the CLI flag semantic ("require refs to be embedded
/// at build time, hard-fail if any unresolved") and matches the design
/// §Behavior on unresolved refs table (`mse bp build` strict opt-in).
#[test]
fn bp_build_unresolved_ref_strict_embed_hard_fails() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let script_path = tmp.path().join("unresolved-strict.bp.lua");
    let out_path = tmp.path().join("out.json");
    fs::write(
        &script_path,
        r#"
local F = require("flow_dsl")

return {
  id = "unresolved-ref-strict",
  flow = F.step({ id = "solo", agent = "solo", input = F.lit(""), out = F.p("$.solo") }),
  agents = {
    {
      name = "solo",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      ["$agent_md"] = "definitely-not-present.md",
    },
  },
  operators = { { name = "main-ai", kind = "main_ai" } },
  strategy = { strict_refs = true, strict_kind = true },
}
"#,
    )
    .expect("write unresolved-strict fixture script");

    Command::cargo_bin("mse")
        .expect("mse binary")
        .args(["bp", "build", "--strict-embed"])
        .arg(&script_path)
        .args(["-o"])
        .arg(&out_path)
        .assert()
        .failure()
        .stderr(contains("compile lint: WARN"))
        .stderr(contains("--strict-embed"))
        .stderr(contains("refusing to emit"));

    assert!(
        !out_path.exists(),
        "--strict-embed must not write out.json when the linker fails"
    );
}

/// Phase 7 (linker refactor 4c4e3eb8) tier-6 round-trip: `mse bp build`
/// must resolve a `$agent_md = "researcher.md"` bare-name ref via the
/// bundled samples/agents directory when no other cascade tier
/// (bp.lua parent, in-bp includes, `MSE_BLUEPRINT_INCLUDES`, `--include`,
/// or a server config) resolves it — exercises the CLI's
/// `bundled_agents_dir()` wiring end-to-end, and guarantees the bundled
/// fixtures stay resolvable by name.
#[test]
fn bp_build_resolves_bare_agent_md_ref_via_bundled_default_tier() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let script_path = tmp.path().join("bundled-tier.bp.lua");
    let out_path = tmp.path().join("out.json");
    fs::write(
        &script_path,
        r#"
local F = require("flow_dsl")

return {
  id = "bundled-tier6-round-trip",
  flow = F.step({ id = "solo", agent = "researcher", input = F.lit(""), out = F.p("$.solo") }),
  agents = {
    {
      -- Bare filename: no `agents/` prefix, so tier 1 (this script's
      -- parent = a fresh tempdir) misses. There are no in-bp includes,
      -- no `MSE_BLUEPRINT_INCLUDES` set for this test, and no `--include`
      -- flag on the command line — so the ref must resolve through the
      -- tier-6 bundled default (`src/mcp/resources/samples/agents/`).
      ["$agent_md"] = "researcher.md",
      spec = { operator_ref = "main-ai" },
    },
  },
  operators = { { name = "main-ai" } },
  strategy = { strict_refs = true, strict_kind = true },
}
"#,
    )
    .expect("write bundled-tier fixture script");

    // Explicitly clear MSE_BLUEPRINT_INCLUDES so a session-level env that
    // happens to hit `researcher.md` does not mask the tier under test.
    Command::cargo_bin("mse")
        .expect("mse binary")
        .env_remove("MSE_BLUEPRINT_INCLUDES")
        .args(["bp", "build"])
        .arg(&script_path)
        .args(["-o"])
        .arg(&out_path)
        .assert()
        .success()
        .stderr(contains("compile lint: OK"));

    // Emit is the raw wire JSON — the `$agent_md` ref is preserved even
    // though the linker resolved it during lint (design table §Behavior
    // on unresolved refs: default `mse bp build` keeps the wire form so
    // the server resolves at register time).
    let out = fs::read_to_string(&out_path).expect("out.json written");
    let value: serde_json::Value = serde_json::from_str(&out).expect("out.json is valid JSON");
    let agent = value
        .get("agents")
        .and_then(|a| a.get(0))
        .expect("agents[0] present");
    assert_eq!(
        agent.get("$agent_md").and_then(|v| v.as_str()),
        Some("researcher.md"),
        "raw emit preserves the ref that tier-6 resolved during lint"
    );
}

/// GH #62 Axis A CLI error path: an unknown template must exit non-zero
/// with the accepted list named — closed set discoverable from the
/// error rather than requiring the author to open the guide.
#[test]
fn bp_new_unknown_template_exits_error_with_accepted_list() {
    Command::cargo_bin("mse")
        .expect("mse binary")
        .args(["bp", "new", "bogus", "foo"])
        .assert()
        .failure()
        .stderr(contains("unknown template 'bogus'"))
        .stderr(contains("pipeline"))
        .stderr(contains("single"))
        .stderr(contains("verdict"));
}
