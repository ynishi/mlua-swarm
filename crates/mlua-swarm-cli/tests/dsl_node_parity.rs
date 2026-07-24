//! Parity test: every one of the 7 flow_dsl Node builders (`F.step` /
//! `F.seq` / `F.branch` / `F.loop_` / `F.fanout` / `F.assign` /
//! `F.try_`) is reachable through `flow_dsl`, and every table it emits
//! deserializes into `mlua_flow_ir::Node` — proving flow_dsl's Node
//! tables are wire-shape-compatible without touching the flow-ir /
//! schema crates. Mirrors `tests/dsl_parity.rs` (the Expr op version,
//! same technique) one layer up: Node builders instead of Expr ops.
//! GH #82 closes the previous 6-only gap by adding `F.fanout` — the
//! 7th (and final) Node kind — so the closed-set assertion below tracks
//! flow.ir's complete Node grammar.

use mlua_swarm_cli::dsl;

/// Evaluate `node_lua` (a flow_dsl Node builder call, e.g.
/// `F.assign({ at = F.p("$.x"), value = F.lit(1) })`) and return its raw
/// table as `serde_json::Value`. Unlike `build_expr` in `dsl_parity.rs`,
/// no `F.unwrap` is needed here: every flow_dsl Node builder already
/// returns a plain Lua table (not an Expr wrapper).
fn build_node(node_lua: &str) -> serde_json::Value {
    let source = format!(
        r#"
        local F = require("flow_dsl")
        return {node_lua}
        "#
    );
    dsl::build_bp_from_script(&source)
        .unwrap_or_else(|e| panic!("script failed: {e}\nsource:\n{source}"))
}

fn assert_node_parity(kind: &str, node_lua: &str) {
    let value = build_node(node_lua);
    serde_json::from_value::<mlua_flow_ir::Node>(value.clone()).unwrap_or_else(|e| {
        panic!(
            "flow_dsl Node builder for kind `{kind}` (`{node_lua}` -> {value}) does not \
             deserialize as mlua_flow_ir::Node: {e}"
        )
    });
}

#[test]
fn every_node_builder_round_trips_through_flow_dsl() {
    let cases: &[(&str, &str)] = &[
        (
            "step",
            r#"F.step({ id = "a", agent = "mock-agent", input = F.lit(1), out = F.p("$.out") })"#,
        ),
        (
            "seq",
            r#"F.seq({ F.assign({ at = F.p("$.x"), value = F.lit(1) }) })"#,
        ),
        (
            "branch",
            r#"F.branch({ cond = F.lit(true), on_true = F.assign({ at = F.p("$.x"), value = F.lit(1) }), on_false = F.assign({ at = F.p("$.x"), value = F.lit(2) }) })"#,
        ),
        (
            "loop",
            r#"F.loop_({ counter = F.p("$.n"), cond = F.lit(true), max = 3, body = F.seq({}) })"#,
        ),
        (
            "assign",
            r#"F.assign({ at = F.p("$.x"), value = F.lit(1) })"#,
        ),
        (
            "try",
            r#"F.try_({ body = F.seq({}), catch = F.seq({}), err_at = F.p("$.err") })"#,
        ),
        (
            "fanout",
            // GH #82: F.fanout — the 7th Node builder.
            r#"F.fanout({ items = F.lit({1, 2}), bind = F.p("$.item"), body = F.seq({}), join = "all", out = F.p("$.results") })"#,
        ),
    ];

    assert_eq!(
        cases.len(),
        7,
        "the guide documents exactly 7 Node builders (GH #82 added F.fanout — flow.ir's full Node grammar is now reachable via flow_dsl)"
    );
    for (kind, node_lua) in cases {
        assert_node_parity(kind, node_lua);
    }
}

#[test]
fn step_node_emits_the_documented_field_names() {
    let value = build_node(
        r#"F.step({ id = "a", agent = "mock-agent", input = F.lit(1), out = F.p("$.out") })"#,
    );
    assert_eq!(
        value,
        serde_json::json!({
            "kind": "step",
            "ref": "mock-agent",
            "in": {"op": "lit", "value": 1},
            "out": {"op": "path", "at": "$.out"},
        })
    );
}

#[test]
fn fanout_node_emits_the_documented_field_names_across_join_modes() {
    // GH #82: assert the wire shape verbatim + parity across all four
    // join modes. The blueprint-authoring guide's Node kind table lists
    // exactly these four; if flow-ir grows a fifth, this test must be
    // updated in lockstep.
    for join in ["all", "any", "race", "all_settled"] {
        let value = build_node(&format!(
            r#"F.fanout({{ items = F.lit({{1, 2}}), bind = F.p("$.item"), body = F.seq({{}}), join = "{join}", out = F.p("$.results") }})"#
        ));
        assert_eq!(
            value,
            serde_json::json!({
                "kind": "fanout",
                "items": {"op": "lit", "value": [1, 2]},
                "bind": {"op": "path", "at": "$.item"},
                "body": {"kind": "seq", "children": []},
                "join": join,
                "out": {"op": "path", "at": "$.results"},
            }),
            "join mode `{join}` did not round-trip through the wire shape"
        );
        serde_json::from_value::<mlua_flow_ir::Node>(value)
            .unwrap_or_else(|e| panic!("fanout join=`{join}` failed Node deserialize: {e}"));
    }
}

#[test]
fn branch_node_emits_the_documented_field_names() {
    // Verifies the `then`/`else` field names explicitly: the Node
    // builder's doc explains that `then`/`else` are Lua reserved words,
    // so the builder API renames them to `on_true`/`on_false` while still
    // emitting the wire-shape's `then`/`else` fields.
    let value = build_node(
        r#"F.branch({ cond = F.lit(true), on_true = F.assign({ at = F.p("$.x"), value = F.lit(1) }), on_false = F.assign({ at = F.p("$.x"), value = F.lit(2) }) })"#,
    );
    assert_eq!(
        value,
        serde_json::json!({
            "kind": "branch",
            "cond": {"op": "lit", "value": true},
            "then": {"kind": "assign", "at": {"op": "path", "at": "$.x"}, "value": {"op": "lit", "value": 1}},
            "else": {"kind": "assign", "at": {"op": "path", "at": "$.x"}, "value": {"op": "lit", "value": 2}},
        })
    );
}
