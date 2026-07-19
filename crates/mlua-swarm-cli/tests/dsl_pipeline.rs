//! `B.pipeline` default-wiring tests:
//!
//! (a) R1/R2 default `in`/`out` derivation
//! (b) automatic verdict-gate insertion shape
//! (c) `halted_at` + `done` assigns
//! (d) retry's 3-part expansion (loop counter path / cond structure / body order)
//! (e) `B.from` resolution
//! (f) `gate = false` opt-out
//! (g) an undefined `B.from` reference raising `error()`
//! (h) `halted_at` omitted → `B.pipeline` defaults it to `"$.halted_at"`
//!     so the produced flow.ir Node still validates
//! (+) an `eval` smoke test: a small flow_dsl-built flow (`assign` +
//!     `branch`) run end-to-end through `mlua-flow-ir`'s Lua `flow.eval`,
//!     no server involved.

use mlua_swarm_cli::dsl;

fn build_pipeline(body: &str) -> serde_json::Value {
    let source = format!(
        r#"
        local F = require("flow_dsl")
        local B = require("bp_dsl")
        {body}
        "#
    );
    dsl::build_bp_from_script(&source)
        .unwrap_or_else(|e| panic!("script failed: {e}\nsource:\n{source}"))
}

/// (a) R1/R2 default in/out + (b) gate shape + (c) halted_at/done assigns,
/// all on a single one-stage pipeline (the smallest scenario that exercises
/// every default-wiring rule at once).
#[test]
fn single_stage_default_wiring_and_gate_shape() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "scout" { agent = "mock-scout" },
          halt_on = { "BLOCKED" },
          halted_at = "$.halted_at",
          done = "$.done",
          gate_default = "auto",  -- bafe47d4: legacy cascade shape asserted below
        }
        "#,
    );

    assert_eq!(value["kind"], serde_json::json!("seq"));
    let children = value["children"].as_array().expect("seq children");
    assert_eq!(children.len(), 2, "single stage -> [step, gate]");

    // R1/R2: default in = $.d.scout, default out = $.scout.
    let step = &children[0];
    assert_eq!(step["kind"], serde_json::json!("step"));
    assert_eq!(step["ref"], serde_json::json!("mock-scout"));
    assert_eq!(
        step["in"],
        serde_json::json!({"op": "path", "at": "$.d.scout"})
    );
    assert_eq!(
        step["out"],
        serde_json::json!({"op": "path", "at": "$.scout"})
    );

    // R3/R4: gate cond addresses parts["verdict"] on the stage's own out.
    let gate = &children[1];
    assert_eq!(gate["kind"], serde_json::json!("branch"));
    assert_eq!(
        gate["cond"],
        serde_json::json!({
            "op": "eq",
            "lhs": {"op": "path", "at": "$.scout.parts[\"verdict\"]"},
            "rhs": {"op": "lit", "value": "BLOCKED"},
        })
    );
    assert_eq!(
        gate["then"],
        serde_json::json!({
            "kind": "assign",
            "at": {"op": "path", "at": "$.halted_at"},
            "value": {"op": "lit", "value": "scout"},
        })
    );
    // Last stage's else = the done assign.
    assert_eq!(
        gate["else"],
        serde_json::json!({
            "kind": "assign",
            "at": {"op": "path", "at": "$.done"},
            "value": {"op": "lit", "value": true},
        })
    );

    serde_json::from_value::<mlua_flow_ir::Node>(value).expect("must be a valid flow.ir Node");
}

/// halt_on with more than one value combines the per-value `eq` conds with
/// an `or` (documented as the "halt_on 複数値なら or" rule).
#[test]
fn multiple_halt_on_values_combine_with_or() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "gate" { agent = "mock-gate" },
          halt_on = { "BLOCKED", "ESCALATE" },
          halted_at = "$.halted_at",
          gate_default = "auto",  -- bafe47d4: legacy cascade shape asserted below
        }
        "#,
    );
    let cond = &value["children"][1]["cond"];
    assert_eq!(cond["op"], serde_json::json!("or"));
    let args = cond["args"].as_array().expect("or args");
    assert_eq!(args.len(), 2);
    assert_eq!(args[0]["rhs"]["value"], serde_json::json!("BLOCKED"));
    assert_eq!(args[1]["rhs"]["value"], serde_json::json!("ESCALATE"));
}

/// (f) `gate = false` splices the stage's step directly into the
/// enclosing `seq` with no `branch` inserted, and the rest of the pipeline
/// continues unconditionally (not nested under an `else`).
#[test]
fn gate_false_opts_out_of_branch_insertion() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "a" { agent = "agent-a", gate = false },
          B.stage "b" { agent = "agent-b" },
          halt_on = { "BLOCKED" },
          halted_at = "$.halted_at",
          gate_default = "auto",  -- bafe47d4: b inherits the cascade, so we can assert its gate shape
        }
        "#,
    );
    let children = value["children"].as_array().expect("seq children");
    assert_eq!(children.len(), 2, "[a's step, rest] — no gate for a");
    assert_eq!(children[0]["kind"], serde_json::json!("step"));
    assert_eq!(children[0]["ref"], serde_json::json!("agent-a"));

    // `rest` is stage b's own [step, gate] seq.
    let rest = &children[1];
    assert_eq!(rest["kind"], serde_json::json!("seq"));
    let b_children = rest["children"].as_array().expect("stage b children");
    assert_eq!(b_children.len(), 2);
    assert_eq!(b_children[0]["ref"], serde_json::json!("agent-b"));
    assert_eq!(b_children[1]["kind"], serde_json::json!("branch"));
}

/// (e) `B.from "stage_id"` resolves to that stage's `out` path.
#[test]
fn from_resolves_to_referenced_stage_out() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "scout" { agent = "mock-scout" },
          B.stage "planner" { agent = "mock-planner", input = B.from "scout" },
          halt_on = { "BLOCKED" },
          halted_at = "$.halted_at",
          gate_default = "auto",  -- bafe47d4: legacy cascade shape asserted below (scout gate wraps planner)
        }
        "#,
    );
    let scout_gate = &value["children"][1];
    let planner_seq = &scout_gate["else"];
    let planner_step = &planner_seq["children"][0];
    assert_eq!(planner_step["ref"], serde_json::json!("mock-planner"));
    assert_eq!(
        planner_step["in"],
        serde_json::json!({"op": "path", "at": "$.scout"})
    );
}

/// (g) referencing an undefined stage id via `B.from` is an `error()`.
#[test]
fn from_undefined_stage_errors() {
    let source = r#"
        local F = require("flow_dsl")
        local B = require("bp_dsl")
        return B.pipeline{
          B.stage "planner" { agent = "mock-planner", input = B.from "missing" },
          halt_on = { "BLOCKED" },
          halted_at = "$.halted_at",
        }
    "#;
    let err = dsl::build_bp_from_script(source).expect_err("undefined B.from target must error");
    let message = err.to_string();
    assert!(
        message.contains("missing"),
        "error message should name the undefined stage id, got: {message}"
    );
}

// ─────────────────────────────────────────────────────────────────────
// bafe47d4: opt-in verdict gate default + `gate_default = "auto"`
// escape hatch. Pre-fix, pipeline-level `halt_on` cascaded to every
// stage regardless of whether the stage emitted a verdict, producing
// dead branches. The fix inverts the default (no gate unless the stage
// opts in) and keeps the legacy shape reachable via
// `gate_default = "auto"` for callers that need the old form.
// ─────────────────────────────────────────────────────────────────────

/// Default (post-bafe47d4): a stage with no `gate` / `halt_on` / `retry`
/// declaration emits NO verdict gate. Pipeline-level `halt_on` no longer
/// forces a cascade; it stays as a shared default value for stages that
/// do opt in.
#[test]
fn default_no_stage_opt_in_emits_no_gate() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "scout" { agent = "mock-scout" },
          halt_on = { "BLOCKED" },  -- present but no longer cascades
          halted_at = "$.halted_at",
          done = "$.done",
        }
        "#,
    );
    let children = value["children"].as_array().expect("seq children");
    assert_eq!(
        children.len(),
        2,
        "single stage without opt-in -> [step, final_else]"
    );
    assert_eq!(children[0]["kind"], serde_json::json!("step"));
    // No branch anywhere along the chain — final_else is the `done` assign.
    assert_eq!(children[1]["kind"], serde_json::json!("assign"));
    assert_eq!(
        children[1]["at"],
        serde_json::json!({"op": "path", "at": "$.done"})
    );
}

/// Explicit `gate = true` on a stage opts it in even when the pipeline
/// has no `halt_on`. Pipeline-level `halt_on` still supplies the value
/// list once a stage opts in.
#[test]
fn stage_gate_true_opts_in_without_pipeline_cascade() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "scout" { agent = "mock-scout", gate = true },
          B.stage "follow" { agent = "mock-follow" },
          halt_on = { "BLOCKED" },
          halted_at = "$.halted_at",
        }
        "#,
    );
    // scout gates (opted in), follow does not (no opt-in). Shape:
    //   seq{scout_step, branch{ cond=verdict==BLOCKED, then=halt, else=seq{follow_step, final_else} }}
    let gate = &value["children"][1];
    assert_eq!(gate["kind"], serde_json::json!("branch"));
    assert_eq!(gate["cond"]["rhs"]["value"], serde_json::json!("BLOCKED"));
    let follow_seq = &gate["else"];
    let follow_children = follow_seq["children"].as_array().expect("follow children");
    assert_eq!(
        follow_children.len(),
        2,
        "follow (no opt-in) -> [step, final_else], no gate"
    );
    assert_eq!(follow_children[0]["ref"], serde_json::json!("mock-follow"));
}

/// Stage-level `halt_on = {...}` implies opt-in and shadows pipeline-level
/// `halt_on` for the cond value list.
#[test]
fn stage_halt_on_implies_gate_and_shadows_pipeline_default() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "checker" { agent = "mock-checker", halt_on = { "ESCALATE" } },
          halt_on = { "BLOCKED" },  -- shared default, but checker names its own
          halted_at = "$.halted_at",
        }
        "#,
    );
    let gate = &value["children"][1];
    assert_eq!(gate["kind"], serde_json::json!("branch"));
    assert_eq!(
        gate["cond"]["rhs"]["value"],
        serde_json::json!("ESCALATE"),
        "stage-level halt_on wins for the cond value list"
    );
}

/// `retry = {...}` implies opt-in — the retry loop already reads verdict,
/// so a post-retry gate makes sense. (Also verified indirectly by
/// `retry_expands_to_step_loop_gate` which asserts on children.len() == 3
/// even without a `gate = true`.)
#[test]
fn retry_implies_gate_without_explicit_opt_in() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "checker" {
            agent = "mock-checker",
            retry = {
              max = 2,
              fix = B.stage "checker_fix" { agent = "mock-fix" },
            },
          },
          halt_on = { "BLOCKED" },
          halted_at = "$.halted_at",
        }
        "#,
    );
    let children = value["children"].as_array().expect("seq children");
    assert_eq!(
        children.len(),
        3,
        "retry implies gate -> [step, loop, branch]"
    );
    assert_eq!(children[2]["kind"], serde_json::json!("branch"));
}

/// `gate = false` beats every opt-in signal (retry included) — an author
/// can carry a verdict-emitting stage that intentionally doesn't halt the
/// pipeline on BLOCKED.
#[test]
fn explicit_gate_false_overrides_retry_opt_in() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "checker" {
            agent = "mock-checker",
            gate = false,
            retry = {
              max = 2,
              fix = B.stage "checker_fix" { agent = "mock-fix" },
            },
          },
          B.stage "follow" { agent = "mock-follow" },
          halt_on = { "BLOCKED" },
          halted_at = "$.halted_at",
        }
        "#,
    );
    let children = value["children"].as_array().expect("seq children");
    assert_eq!(
        children.len(),
        3,
        "gate=false: [step, loop, rest] — no branch, `follow` splices in"
    );
    assert_eq!(children[0]["kind"], serde_json::json!("step"));
    assert_eq!(children[1]["kind"], serde_json::json!("loop"));
    // The rest is a seq wrapping follow's step + final_else — no branch.
    let rest = &children[2];
    assert_eq!(rest["kind"], serde_json::json!("seq"));
    let rest_children = rest["children"].as_array().expect("rest children");
    assert_eq!(rest_children[0]["ref"], serde_json::json!("mock-follow"));
}

/// `gate_default = "auto"` restores the pre-fix cascade — pipeline-level
/// `halt_on` triggers a gate on every stage whose own `gate` / `halt_on` /
/// `retry` are all unset. Escape hatch for pre-bafe47d4 bp.lua sources.
#[test]
fn gate_default_auto_restores_pipeline_cascade() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "a" { agent = "mock-a" },
          B.stage "b" { agent = "mock-b" },
          halt_on = { "BLOCKED" },
          halted_at = "$.halted_at",
          gate_default = "auto",
        }
        "#,
    );
    // Cascade shape: seq{a_step, branch{ cond=..., then=halt, else=seq{b_step, branch{...}} }}
    let a_gate = &value["children"][1];
    assert_eq!(a_gate["kind"], serde_json::json!("branch"));
    let b_seq = &a_gate["else"];
    let b_gate = &b_seq["children"][1];
    assert_eq!(b_gate["kind"], serde_json::json!("branch"));
}

/// `gate_default = "auto"` with an empty pipeline `halt_on` still emits
/// no cascade — the cascade only kicks in when there IS a pipeline-level
/// halt-value list to inherit. Regression guard against a naive `auto`
/// implementation that would emit a gate with an empty cond value list.
#[test]
fn gate_default_auto_with_empty_halt_on_still_emits_no_gate() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "scout" { agent = "mock-scout" },
          halted_at = "$.halted_at",
          gate_default = "auto",
          -- halt_on intentionally unset
        }
        "#,
    );
    let children = value["children"].as_array().expect("seq children");
    assert_eq!(children.len(), 2, "no halt_on -> no gate even under auto");
    assert_eq!(children[0]["kind"], serde_json::json!("step"));
    assert_ne!(children[1]["kind"], serde_json::json!("branch"));
}

/// Unknown `gate_default` values must fail loud — typo protection.
#[test]
fn gate_default_unknown_value_errors() {
    let source = r#"
        local F = require("flow_dsl")
        local B = require("bp_dsl")
        return B.pipeline{
          B.stage "scout" { agent = "mock-scout" },
          halt_on = { "BLOCKED" },
          halted_at = "$.halted_at",
          gate_default = "sometimes",
        }
    "#;
    let err = dsl::build_bp_from_script(source).expect_err("unknown gate_default must error");
    let message = err.to_string();
    assert!(
        message.contains("gate_default"),
        "error must name the offending option, got: {message}"
    );
    assert!(
        message.contains("sometimes"),
        "error must echo the bad value, got: {message}"
    );
}

/// GH #65: `chain = true` opts the pipeline into stage-to-stage
/// chaining — stage N (N ≥ 2) whose `input` is nil defaults to
/// `$.{stage[N-1]_id}` (the previous stage's own `out`) instead of the
/// `$.d.{stage_id}` R1 default. Stage 1 is unchanged.
#[test]
fn chain_true_wires_stage_n_input_to_previous_out() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "ingest"    { agent = "mock-ingest" },
          B.stage "transform" { agent = "mock-transform" },
          B.stage "emit"      { agent = "mock-emit" },
          chain = true,
          halt_on = { "BLOCKED" },
          halted_at = "$.halted_at",
          gate_default = "auto",  -- bafe47d4: legacy cascade shape asserted below (walks gate.else chain)
        }
        "#,
    );

    // The pipeline is `seq{step_1, gate_1(else = seq{step_2, gate_2(else =
    // seq{step_3, gate_3(...)})})}`. Walk the tree picking each step off
    // its `in` path.
    let step_1 = &value["children"][0];
    assert_eq!(step_1["ref"], serde_json::json!("mock-ingest"));
    assert_eq!(
        step_1["in"],
        serde_json::json!({"op": "path", "at": "$.d.ingest"}),
        "stage 1 default stays $.d.<id> — no earlier stage to chain from"
    );

    let step_2 = &value["children"][1]["else"]["children"][0];
    assert_eq!(step_2["ref"], serde_json::json!("mock-transform"));
    assert_eq!(
        step_2["in"],
        serde_json::json!({"op": "path", "at": "$.ingest"}),
        "chain=true rewires stage 2 default to $.<stage_1_id> (previous stage's out)"
    );

    let step_3 = &value["children"][1]["else"]["children"][1]["else"]["children"][0];
    assert_eq!(step_3["ref"], serde_json::json!("mock-emit"));
    assert_eq!(
        step_3["in"],
        serde_json::json!({"op": "path", "at": "$.transform"}),
        "chain=true rewires stage 3 default to $.<stage_2_id>"
    );
}

/// GH #65: an explicit per-stage `input` (path string or `B.from`
/// placeholder) still overrides the chained default. Combined check so
/// the two override paths are asserted against the same emitted flow.
#[test]
fn chain_true_respects_explicit_input_and_b_from_overrides() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "ingest"    { agent = "mock-ingest" },
          B.stage "transform" { agent = "mock-transform", input = "$.d.custom" },
          B.stage "emit"      { agent = "mock-emit",      input = B.from "ingest" },
          chain = true,
          halt_on = { "BLOCKED" },
          halted_at = "$.halted_at",
          gate_default = "auto",  -- bafe47d4: legacy cascade shape asserted below
        }
        "#,
    );

    let step_2 = &value["children"][1]["else"]["children"][0];
    assert_eq!(
        step_2["in"],
        serde_json::json!({"op": "path", "at": "$.d.custom"}),
        "explicit input= path string overrides the chained default"
    );

    let step_3 = &value["children"][1]["else"]["children"][1]["else"]["children"][0];
    assert_eq!(
        step_3["in"],
        serde_json::json!({"op": "path", "at": "$.ingest"}),
        "B.from placeholder overrides the chained default and points at the referenced stage's out"
    );
}

/// GH #65: omitting `chain` (the pre-existing behavior) leaves every
/// stage's default at `$.d.{stage_id}`, including stage 2+. Regression
/// guard.
#[test]
fn chain_omitted_keeps_dot_d_default_on_every_stage() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "ingest"    { agent = "mock-ingest" },
          B.stage "transform" { agent = "mock-transform" },
          halt_on = { "BLOCKED" },
          halted_at = "$.halted_at",
          gate_default = "auto",  -- bafe47d4: legacy cascade shape asserted below (walks stage 1 gate.else)
        }
        "#,
    );

    let step_1 = &value["children"][0];
    assert_eq!(
        step_1["in"],
        serde_json::json!({"op": "path", "at": "$.d.ingest"})
    );

    let step_2 = &value["children"][1]["else"]["children"][0];
    assert_eq!(
        step_2["in"],
        serde_json::json!({"op": "path", "at": "$.d.transform"}),
        "chain omitted -> stage 2 keeps $.d.<own_id> default"
    );
}

/// (d) `retry = { max = N, fix = <stage record> }` expands to the
/// documented 3 parts, in order: step, loop, gate.
#[test]
fn retry_expands_to_step_loop_gate() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "resolver" {
            agent = "mock-resolver",
            retry = {
              max = 3,
              fix = B.stage "resolver_fix" { agent = "mock-fix" },
            },
          },
          halt_on = { "BLOCKED" },
          halted_at = "$.halted_at",
          done = "$.done",
        }
        "#,
    );
    let children = value["children"].as_array().expect("seq children");
    assert_eq!(children.len(), 3, "[step, loop, gate]");

    assert_eq!(children[0]["kind"], serde_json::json!("step"));
    assert_eq!(children[0]["ref"], serde_json::json!("mock-resolver"));

    let loop_node = &children[1];
    assert_eq!(loop_node["kind"], serde_json::json!("loop"));
    assert_eq!(
        loop_node["counter"],
        serde_json::json!({"op": "path", "at": "$.resolver_n"})
    );
    // max + 1 per the documented rule.
    assert_eq!(loop_node["max"], serde_json::json!(4));

    let cond = &loop_node["cond"];
    assert_eq!(cond["op"], serde_json::json!("and"));
    let cond_args = cond["args"].as_array().expect("and args");
    assert_eq!(cond_args.len(), 2);
    assert_eq!(cond_args[0]["op"], serde_json::json!("lt"));
    assert_eq!(cond_args[1]["op"], serde_json::json!("eq"));

    let body = &loop_node["body"];
    assert_eq!(body["kind"], serde_json::json!("seq"));
    let body_children = body["children"].as_array().expect("loop body children");
    assert_eq!(body_children.len(), 2, "fix step, then the stage re-run");
    assert_eq!(body_children[0]["ref"], serde_json::json!("mock-fix"));
    assert_eq!(body_children[1]["ref"], serde_json::json!("mock-resolver"));

    assert_eq!(children[2]["kind"], serde_json::json!("branch"));

    serde_json::from_value::<mlua_flow_ir::Node>(value).expect("must be a valid flow.ir Node");
}

/// `retry.counter` overrides the loop's counter path (the default is
/// `"$.{stage_id}_n"` — see `retry_expands_to_step_loop_gate` above for
/// that default case); every other retry-expansion detail (loop `max`,
/// `cond` shape, body order) is unaffected by the override.
#[test]
fn retry_counter_override_changes_loop_counter_path() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "resolver" {
            agent = "mock-resolver",
            retry = {
              max = 3,
              counter = "$.custom_counter",
              fix = B.stage "resolver_fix" { agent = "mock-fix" },
            },
          },
          halt_on = { "BLOCKED" },
          halted_at = "$.halted_at",
        }
        "#,
    );
    let loop_node = &value["children"][1];
    assert_eq!(loop_node["kind"], serde_json::json!("loop"));
    assert_eq!(
        loop_node["counter"],
        serde_json::json!({"op": "path", "at": "$.custom_counter"})
    );
    let cond_args = loop_node["cond"]["args"].as_array().expect("and args");
    assert_eq!(
        cond_args[0]["lhs"],
        serde_json::json!({"op": "path", "at": "$.custom_counter"}),
        "the lt-comparison in cond must also read the overridden counter path"
    );

    serde_json::from_value::<mlua_flow_ir::Node>(value).expect("must be a valid flow.ir Node");
}

/// `F.obj()` emits a genuine empty JSON object wherever it's substituted
/// for a raw Lua table field, exercised here through the same
/// `dsl::build_bp_from_script` entry point `B.pipeline` results flow
/// through (this module's helper preloads both `flow_dsl` and `bp_dsl`,
/// even though `F.obj()` itself has no dependency on `bp_dsl`).
#[test]
fn f_obj_emits_an_empty_json_object() {
    let value = build_pipeline(r#"return { spec = F.obj() }"#);
    assert_eq!(value["spec"], serde_json::json!({}));
    assert!(value["spec"].is_object());
}

/// (h) `halted_at` omitted → `B.pipeline` defaults it to `"$.halted_at"`
/// so the produced flow.ir Node still validates. Regression guard for the
/// pre-fix crash where the per-stage gate emitted
/// `assign{at=path{}, value=lit(stage_id)}` (empty `at`) whenever the
/// caller omitted `halted_at`, tripping shape validation downstream. The
/// canonical fix path (documented in mse://guides/dsl-authoring) is the
/// DSL-side default; if this ever regresses to erroring, the fix belongs
/// in `bp_dsl.M.pipeline`, not in the compiler.
#[test]
fn halted_at_defaults_when_unset() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "scout" { agent = "mock-scout", gate = true },
          done = "$.done",
          -- halted_at intentionally omitted; `gate = true` (bafe47d4) opts
          -- the single stage into gate emission so the halted_at default
          -- assertion still has a `branch` node to inspect.
        }
        "#,
    );

    // Shape validation must pass — this is what failed before the fix.
    serde_json::from_value::<mlua_flow_ir::Node>(value.clone())
        .expect("must be a valid flow.ir Node even without an explicit halted_at");

    let gate = &value["children"][1];
    assert_eq!(gate["kind"], serde_json::json!("branch"));
    assert_eq!(
        gate["then"],
        serde_json::json!({
            "kind": "assign",
            "at": {"op": "path", "at": "$.halted_at"},
            "value": {"op": "lit", "value": "scout"},
        }),
        "default halted_at must be `$.halted_at` (matches the bundled sample convention)",
    );
}

/// eval smoke: a small flow_dsl-built flow (`assign` +
/// `branch`, no `step`/dispatch involved) runs end-to-end through
/// mlua-flow-ir's Lua `flow.eval` binding — no server, no HTTP, nothing but
/// the pure Lua VM + the flow-ir interpreter. `flow.eval`'s Rust binding
/// (`mlua_flow_ir::module`) requires a dispatcher *function* positionally
/// (its `eval_fn` signature types the 3rd arg as `mlua::Function`). This
/// flow never dispatches a `step`, so the stub is a no-op passthrough.
#[test]
fn eval_smoke_runs_a_small_flow_via_flow_eval() {
    use mlua::LuaSerdeExt;

    let lua = mlua::Lua::new();
    dsl::preload(&lua).expect("dsl preload must succeed");
    let flow_module = mlua_flow_ir::module(&lua).expect("flow module must register");
    lua.globals()
        .set("flow", flow_module)
        .expect("must set the `flow` global");

    let script = r#"
        local F = require("flow_dsl")

        local node = F.seq({
          F.assign({ at = F.p("$.count"), value = F.lit(1) }),
          F.branch({
            cond = F.p("$.count"):eq(1),
            on_true = F.assign({ at = F.p("$.label"), value = F.lit("one") }),
            on_false = F.assign({ at = F.p("$.label"), value = F.lit("other") }),
          }),
        })

        -- No `step` Node in this flow, so the dispatcher is never invoked
        -- — but flow.eval requires one positionally regardless.
        local function dispatcher(_ref, input)
          return input
        end

        return flow.eval(node, {}, dispatcher)
    "#;

    let result: mlua::Value = lua
        .load(script)
        .set_name("eval-smoke")
        .eval()
        .expect("flow.eval must run without a server");

    let ctx: serde_json::Value = lua
        .from_value_with(
            result,
            mlua::serde::de::Options::new().encode_empty_tables_as_array(true),
        )
        .expect("result must convert to JSON");

    assert_eq!(ctx["count"], serde_json::json!(1));
    assert_eq!(ctx["label"], serde_json::json!("one"));
}
