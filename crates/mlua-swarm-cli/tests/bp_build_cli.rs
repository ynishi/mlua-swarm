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
        .stderr(contains("is not a member of the declared values"));
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
        .stderr(contains("$agent_md file ref"));
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
