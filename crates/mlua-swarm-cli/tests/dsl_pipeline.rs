//! `B.pipeline` default-wiring tests:
//!
//! (a) R1/R2 default `in`/`out` derivation
//! (b) automatic verdict-gate insertion shape
//! (c) `halted_at` + `done` assigns
//! (d) retry's 3-part expansion (loop counter path / cond structure / body order)
//! (e) `B.from` resolution
//! (f) `gate = false` opt-out
//! (g) an undefined `B.from` reference raising `error()`
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
