//! Parity test: every one of the 20 flow.ir `Expr` ops
//! is reachable through `flow_dsl`'s builders, and every table it emits
//! deserializes into `mlua_flow_ir::Expr` — proving flow_dsl's tables are
//! wire-shape-compatible without touching the flow-ir / schema crates.
//! Mirrors `guide_expr_ops_match_schema_field_names` in
//! `crates/mlua-swarm-cli/src/mcp/resources.rs` (same 20-op list, same
//! technique), one layer up: DSL output instead of hand-written JSON.

use mlua_swarm_cli::dsl;

/// Evaluate `expr_lua` (a flow_dsl expression, e.g. `F.lit(1):eq(1)`) and
/// return its raw AST table as `serde_json::Value` (`F.unwrap` extracts
/// the AST out of the Expr wrapper — every flow_dsl builder returns a
/// wrapper, not the raw table, so a parity check needs the explicit
/// unwrap step a Node builder would otherwise do internally).
fn build_expr(expr_lua: &str) -> serde_json::Value {
    let source = format!(
        r#"
        local F = require("flow_dsl")
        return F.unwrap({expr_lua})
        "#
    );
    dsl::build_bp_from_script(&source)
        .unwrap_or_else(|e| panic!("script failed: {e}\nsource:\n{source}"))
}

fn assert_expr_parity(op: &str, expr_lua: &str) {
    let value = build_expr(expr_lua);
    serde_json::from_value::<mlua_flow_ir::Expr>(value.clone()).unwrap_or_else(|e| {
        panic!(
            "flow_dsl op `{op}` (`{expr_lua}` -> {value}) does not deserialize as \
             mlua_flow_ir::Expr: {e}"
        )
    });
}

#[test]
fn every_expr_op_round_trips_through_flow_dsl() {
    let cases: &[(&str, &str)] = &[
        ("path", r#"F.p("$.x")"#),
        ("lit", "F.lit(42)"),
        ("eq", "F.lit(1):eq(1)"),
        ("ne", "F.lit(1):ne(2)"),
        ("lt", "F.lit(1):lt(2)"),
        ("lte", "F.lit(1):lte(2)"),
        ("gt", "F.lit(2):gt(1)"),
        ("gte", "F.lit(2):gte(1)"),
        ("not", "F.lit(true):Not()"),
        ("and", "F.all{F.lit(true)}"),
        ("or", "F.any{F.lit(true)}"),
        ("exists", r#"F.p("$.x"):exists()"#),
        ("add", "F.lit(1) + F.lit(2)"),
        ("sub", "F.lit(3) - F.lit(1)"),
        ("mul", "F.lit(2) * F.lit(3)"),
        ("div", "F.lit(6) / F.lit(2)"),
        ("mod", "F.lit(5) % F.lit(2)"),
        ("len", r#"F.lit("hi"):len()"#),
        ("in", "F.lit({1, 2, 3}):contains(F.lit(1))"),
        ("call_extern", r#"F.call_extern("math.sqrt", { F.lit(9) })"#),
    ];

    assert_eq!(cases.len(), 20, "the guide documents exactly 20 Expr ops");
    for (op, expr_lua) in cases {
        assert_expr_parity(op, expr_lua);
    }
}

#[test]
fn eq_op_emits_the_documented_field_names() {
    // Spot-check one op's exact shape against the guide's own snippet
    // (`{"op":"eq","lhs":{...},"rhs":{...}}`), not just "deserializes ok".
    let value = build_expr("F.lit(1):eq(1)");
    assert_eq!(
        value,
        serde_json::json!({
            "op": "eq",
            "lhs": {"op": "lit", "value": 1},
            "rhs": {"op": "lit", "value": 1},
        })
    );
}

#[test]
fn call_extern_emits_the_documented_field_names() {
    // Spot-check `F.call_extern`'s exact shape: `ref` for the extern
    // registry key (not the Rust-side field name `ref_`) and `args` as a
    // plain unwrapped-Expr list.
    let value = build_expr(r#"F.call_extern("math.sqrt", { F.lit(9) })"#);
    assert_eq!(
        value,
        serde_json::json!({
            "op": "call_extern",
            "ref": "math.sqrt",
            "args": [{"op": "lit", "value": 9}],
        })
    );
}

#[test]
fn call_extern_accepts_raw_values_alongside_expr_wrappers() {
    // Every other N-ary builder (`F.all`/`F.any`) auto-`lit`s raw Lua
    // values via `F.unwrap`; `F.call_extern`'s `args` list follows the
    // same convention.
    let value = build_expr(r#"F.call_extern("math.pow", { 2, F.lit(3) })"#);
    assert_eq!(
        value,
        serde_json::json!({
            "op": "call_extern",
            "ref": "math.pow",
            "args": [{"op": "lit", "value": 2}, {"op": "lit", "value": 3}],
        })
    );
}

#[test]
fn path_bracket_notation_survives_round_trip() {
    // The `parts["verdict"]` bracket-notation path syntax bp_dsl's gate
    // cond relies on (see mse://guides/blueprint-authoring § Worker
    // output) must parse cleanly as a `Path` too.
    let value = build_expr(r#"F.p("$.gate.parts[\"verdict\"]")"#);
    serde_json::from_value::<mlua_flow_ir::Expr>(value)
        .expect("bracket-notation path must deserialize as a Path expr");
}
