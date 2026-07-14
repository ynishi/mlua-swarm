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
