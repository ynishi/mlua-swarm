//! `B.pipeline` fanout-stage sugar (`B.stage "id" { fanout = {...} }`):
//!
//! (a) the homogeneous shape (`fanout = { agent = ... }`) — items / bind /
//!     join / out defaults and the one-step lane body
//! (b) the heterogeneous shape (`fanout = { lanes = {...} }`) — literal
//!     item array, branch cascade, per-lane in/out defaults
//! (c) a single lane degenerating to a bare step (no cascade)
//! (d) sugar vs hand-built `F.fanout` JSON equality (the two fixtures)
//! (e) composition with the surrounding stage options: the aggregate stage
//!     reading the fanout `out` via `B.from` / `chain = true`, and
//!     `skip_on` wrapping the whole fanout node
//! (f) `B.from` in `fanout.items` resolving to a `path`, never a `lit`
//!
//! The error / warning cases (`retry` on a fanout stage, `agent` + `fanout`
//! together, a bad `join`, keyed `lanes`, and the gate-on-fanout warning)
//! live in `mlua-swarm-dsl`'s own unit tests, next to the dead-halt lint.

use mlua_swarm_cli::dsl;

const SUGAR_FIXTURE: &str = include_str!("fixtures/fanout_stage.bp.lua");
const RAW_FIXTURE: &str = include_str!("fixtures/fanout_stage_raw.bp.lua");

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

/// (a) `fanout = { agent = ... }` expands the stage's slot into a `fanout`
/// node: `items` from the stage's own resolved `input` (`$.d.{stage_id}`),
/// `bind` `$.item`, `join` `"all"`, `out` the stage's ordinary
/// `$.{stage_id}`, and a one-step body reading the bound item.
#[test]
fn homogeneous_fanout_stage_default_wiring() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "targets" { fanout = { agent = "check" } },
          B.stage "aggregate" { agent = "aggregate", input = B.from "targets", gate = true },
          halt_on = { "BLOCKED" },
          halted_at = "$.halted_at",
          done = "$.done",
        }
        "#,
    );

    let fanout = &value["children"][0];
    assert_eq!(fanout["kind"], serde_json::json!("fanout"));
    assert_eq!(
        fanout["items"],
        serde_json::json!({"op": "path", "at": "$.d.targets"}),
        "items default to the stage's own resolved input"
    );
    assert_eq!(
        fanout["bind"],
        serde_json::json!({"op": "path", "at": "$.item"})
    );
    assert_eq!(fanout["join"], serde_json::json!("all"));
    assert_eq!(
        fanout["out"],
        serde_json::json!({"op": "path", "at": "$.targets"}),
        "the stage's out is unchanged by the fanout sugar"
    );
    assert_eq!(
        fanout["body"],
        serde_json::json!({
            "kind": "step",
            "ref": "check",
            "in": {"op": "path", "at": "$.item"},
            "out": {"op": "path", "at": "$.branch_out"},
        }),
        "one step per item, reading the bound item"
    );

    serde_json::from_value::<mlua_flow_ir::Node>(value).expect("must be a valid flow.ir Node");
}

/// (a) `bind` / `join` / `lane_out` overrides land on the emitted node.
#[test]
fn homogeneous_fanout_stage_honors_bind_join_and_lane_out_overrides() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "targets" {
            fanout = {
              agent    = "check",
              bind     = "$.target",
              join     = "all_settled",
              lane_out = "$.checked",
            },
          },
          halted_at = "$.halted_at",
        }
        "#,
    );

    let fanout = &value["children"][0];
    assert_eq!(
        fanout["bind"],
        serde_json::json!({"op": "path", "at": "$.target"})
    );
    assert_eq!(fanout["join"], serde_json::json!("all_settled"));
    assert_eq!(
        fanout["body"]["in"],
        serde_json::json!({"op": "path", "at": "$.target"}),
        "the lane body reads whatever `bind` names"
    );
    assert_eq!(
        fanout["body"]["out"],
        serde_json::json!({"op": "path", "at": "$.checked"})
    );

    serde_json::from_value::<mlua_flow_ir::Node>(value).expect("must be a valid flow.ir Node");
}

/// (b) `fanout = { lanes = {...} }` emits the literal lane-name array as
/// `items` (in declaration order) and a branch cascade as the body: one arm
/// per lane, the last lane the terminal `else`. Each lane's `in` defaults to
/// `$.d.{lane}` and its `out` to `$.lane.{lane}`. The bare-string entry is
/// the shorthand for `{ lane = s, agent = s }`.
#[test]
fn heterogeneous_lanes_emit_the_branch_cascade() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "gates" {
            fanout = { lanes = {
              { lane = "danger", agent = "gate-danger" },
              { lane = "leak",   agent = "gate-leak" },
              "hygiene",
            } },
          },
          halted_at = "$.halted_at",
        }
        "#,
    );

    let fanout = &value["children"][0];
    assert_eq!(fanout["kind"], serde_json::json!("fanout"));
    assert_eq!(
        fanout["items"],
        serde_json::json!({"op": "lit", "value": ["danger", "leak", "hygiene"]}),
        "items default to the literal lane-name array, in declaration order"
    );
    assert_eq!(
        fanout["out"],
        serde_json::json!({"op": "path", "at": "$.gates"})
    );

    // Lane 1: branch on the bound item.
    let cascade = &fanout["body"];
    assert_eq!(cascade["kind"], serde_json::json!("branch"));
    assert_eq!(
        cascade["cond"],
        serde_json::json!({
            "op": "eq",
            "lhs": {"op": "path", "at": "$.item"},
            "rhs": {"op": "lit", "value": "danger"},
        })
    );
    assert_eq!(
        cascade["then"],
        serde_json::json!({
            "kind": "step",
            "ref": "gate-danger",
            "in": {"op": "path", "at": "$.d.danger"},
            "out": {"op": "path", "at": "$.lane.danger"},
        })
    );

    // Lane 2: nested branch.
    let second = &cascade["else"];
    assert_eq!(second["kind"], serde_json::json!("branch"));
    assert_eq!(
        second["cond"]["rhs"],
        serde_json::json!({"op": "lit", "value": "leak"})
    );
    assert_eq!(second["then"]["ref"], serde_json::json!("gate-leak"));

    // Lane 3 (the bare-string shorthand): terminal `else`, a bare step.
    let last = &second["else"];
    assert_eq!(
        last,
        &serde_json::json!({
            "kind": "step",
            "ref": "hygiene",
            "in": {"op": "path", "at": "$.d.hygiene"},
            "out": {"op": "path", "at": "$.lane.hygiene"},
        }),
        "the last lane is the terminal else, not another branch"
    );

    serde_json::from_value::<mlua_flow_ir::Node>(value).expect("must be a valid flow.ir Node");
}

/// (b) per-lane `input` (path string or `B.from`) and `out` overrides.
#[test]
fn lane_input_and_out_overrides_win_over_the_defaults() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "plan" { agent = "planner" },
          B.stage "gates" {
            fanout = { lanes = {
              { lane = "danger", agent = "gate-danger", input = B.from "plan" },
              { lane = "leak",   agent = "gate-leak",   input = "$.d.custom", out = "$.leak_out" },
            } },
          },
          halted_at = "$.halted_at",
        }
        "#,
    );

    let cascade = &value["children"][1]["children"][0]["body"];
    assert_eq!(
        cascade["then"]["in"],
        serde_json::json!({"op": "path", "at": "$.plan"}),
        "a lane's B.from input resolves against the referenced stage's out"
    );
    let last = &cascade["else"];
    assert_eq!(
        last["in"],
        serde_json::json!({"op": "path", "at": "$.d.custom"})
    );
    assert_eq!(
        last["out"],
        serde_json::json!({"op": "path", "at": "$.leak_out"})
    );
}

/// (c) a single lane needs no cascade — the body is the bare step.
#[test]
fn single_lane_degenerates_to_a_bare_step() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "gates" { fanout = { lanes = { "danger" } } },
          halted_at = "$.halted_at",
        }
        "#,
    );

    let fanout = &value["children"][0];
    assert_eq!(
        fanout["items"],
        serde_json::json!({"op": "lit", "value": ["danger"]})
    );
    assert_eq!(
        fanout["body"],
        serde_json::json!({
            "kind": "step",
            "ref": "danger",
            "in": {"op": "path", "at": "$.d.danger"},
            "out": {"op": "path", "at": "$.lane.danger"},
        }),
        "one lane -> bare step, no branch"
    );

    serde_json::from_value::<mlua_flow_ir::Node>(value).expect("must be a valid flow.ir Node");
}

/// (d) the sugar and the hand-built `F.fanout` fixture build to the same
/// value — the permanent lock on the expansion (same technique as the
/// `dsl_json_equivalence_*` tests).
#[test]
fn sugar_matches_hand_written_fanout() {
    let sugar = dsl::build_bp_from_script(SUGAR_FIXTURE).expect("sugar fixture must build");
    let raw = dsl::build_bp_from_script(RAW_FIXTURE).expect("hand-built fixture must build");

    assert_eq!(
        sugar, raw,
        "the B.pipeline fanout sugar diverges from the hand-built flow_dsl \
         shape (tests/fixtures/fanout_stage{{,_raw}}.bp.lua)"
    );

    serde_json::from_value::<mlua_flow_ir::Node>(sugar["flow"].clone())
        .expect("must be a valid flow.ir Node");
}

/// (e) the canonical gate pattern: the aggregate stage reads the fanout's
/// `out` through `B.from` and carries the verdict gate itself.
#[test]
fn aggregate_stage_reads_the_fanout_out_via_from() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "gates" {
            fanout = { lanes = { "danger", "leak" } },
          },
          B.stage "aggregate" { agent = "aggregate", input = B.from "gates", gate = true },
          halt_on = { "BLOCKED" },
          halted_at = "$.halted_at",
          done = "$.gates_ok",
        }
        "#,
    );

    // seq{ fanout, seq{ aggregate_step, gate } } — the fanout stage itself
    // does not gate.
    let rest = &value["children"][1];
    let aggregate_step = &rest["children"][0];
    assert_eq!(aggregate_step["ref"], serde_json::json!("aggregate"));
    assert_eq!(
        aggregate_step["in"],
        serde_json::json!({"op": "path", "at": "$.gates"}),
        "B.from resolves to the fanout stage's out"
    );

    let gate = &rest["children"][1];
    assert_eq!(gate["kind"], serde_json::json!("branch"));
    assert_eq!(
        gate["cond"]["lhs"],
        serde_json::json!({"op": "path", "at": "$.aggregate.parts[\"verdict\"]"}),
        "the gate reads the aggregate stage's verdict, not the join result"
    );

    serde_json::from_value::<mlua_flow_ir::Node>(value).expect("must be a valid flow.ir Node");
}

/// (e) `chain = true` threads a fanout stage like any other: the previous
/// stage's `out` becomes the fanout's `items` source, and the fanout's own
/// `out` becomes the next stage's `in`.
#[test]
fn chain_true_threads_fanout_out_into_the_next_stage() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "plan"      { agent = "planner" },
          B.stage "targets"   { fanout = { agent = "check" } },
          B.stage "aggregate" { agent = "aggregate" },
          chain = true,
          halted_at = "$.halted_at",
        }
        "#,
    );

    let fanout = &value["children"][1]["children"][0];
    assert_eq!(fanout["kind"], serde_json::json!("fanout"));
    assert_eq!(
        fanout["items"],
        serde_json::json!({"op": "path", "at": "$.plan"}),
        "chain=true feeds the previous stage's out into the fanout items"
    );

    let aggregate_step = &value["children"][1]["children"][1]["children"][0];
    assert_eq!(aggregate_step["ref"], serde_json::json!("aggregate"));
    assert_eq!(
        aggregate_step["in"],
        serde_json::json!({"op": "path", "at": "$.targets"}),
        "chain=true feeds the fanout stage's out into the next stage"
    );

    serde_json::from_value::<mlua_flow_ir::Node>(value).expect("must be a valid flow.ir Node");
}

/// (e) `skip_on` wraps the whole fanout node in the same pre-emptive branch
/// it wraps an ordinary step's body in.
#[test]
fn skip_on_wraps_the_fanout_node() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "plan" { agent = "planner" },
          B.stage "targets" {
            fanout  = { agent = "check" },
            input   = B.from "plan",
            skip_on = { "SKIP" },
          },
          halted_at = "$.halted_at",
        }
        "#,
    );

    let guard = &value["children"][1]["children"][0];
    assert_eq!(guard["kind"], serde_json::json!("branch"));
    assert_eq!(guard["cond"]["op"], serde_json::json!("in"));
    assert_eq!(
        guard["cond"]["needle"],
        serde_json::json!({"op": "path", "at": "$.plan.parts[\"verdict\"]"})
    );
    assert_eq!(
        guard["then"],
        serde_json::json!({"kind": "seq", "children": []}),
        "a skipped stage runs no lane at all"
    );

    let body = &guard["else"];
    assert_eq!(body["children"][0]["kind"], serde_json::json!("fanout"));
    assert_eq!(
        body["children"][0]["items"],
        serde_json::json!({"op": "path", "at": "$.plan"}),
        "the stage's B.from input is still what feeds items"
    );

    serde_json::from_value::<mlua_flow_ir::Node>(value).expect("must be a valid flow.ir Node");
}

/// (f) `B.from` handed to `fanout.items` must resolve to the referenced
/// stage's `out` as a `path` Expr. Regression guard for the failure mode
/// where the placeholder record falls through flow_dsl's auto-`lit`
/// convention and is emitted as literal data instead.
#[test]
fn b_from_items_resolves_to_a_path_not_a_literal() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "plan" { agent = "planner", out = "$.plan_out" },
          B.stage "targets" {
            fanout = { agent = "check", items = B.from "plan" },
          },
          halted_at = "$.halted_at",
        }
        "#,
    );

    let items = &value["children"][1]["children"][0]["items"];
    assert_eq!(
        items,
        &serde_json::json!({"op": "path", "at": "$.plan_out"}),
        "B.from in fanout.items must resolve to the referenced stage's out path"
    );
    assert_ne!(
        items["op"],
        serde_json::json!("lit"),
        "a B.from placeholder must never be emitted as a literal: {items}"
    );

    serde_json::from_value::<mlua_flow_ir::Node>(value).expect("must be a valid flow.ir Node");
}

/// (f) the same resolution for the heterogeneous shape, where `B.from`
/// overrides the literal lane-name array default.
#[test]
fn b_from_items_overrides_the_lane_name_array() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "plan" { agent = "planner" },
          B.stage "gates" {
            fanout = {
              items = B.from "plan",
              lanes = { "danger", "leak" },
            },
          },
          halted_at = "$.halted_at",
        }
        "#,
    );

    let fanout = &value["children"][1]["children"][0];
    assert_eq!(
        fanout["items"],
        serde_json::json!({"op": "path", "at": "$.plan"}),
        "an explicit items overrides the lane-name array default"
    );
    assert_eq!(
        fanout["body"]["kind"],
        serde_json::json!("branch"),
        "the cascade still branches on the bound item"
    );
}

/// A raw Lua value in `items` follows flow_dsl's auto-`lit` convention —
/// the documented difference from a stage's `input` (where a bare string is
/// a path).
#[test]
fn raw_items_value_is_a_literal_array() {
    let value = build_pipeline(
        r#"
        return B.pipeline{
          B.stage "targets" {
            fanout = { agent = "check", items = { "core", "server", "cli" } },
          },
          halted_at = "$.halted_at",
        }
        "#,
    );

    assert_eq!(
        value["children"][0]["items"],
        serde_json::json!({"op": "lit", "value": ["core", "server", "cli"]})
    );
}
