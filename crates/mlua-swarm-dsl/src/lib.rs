//! Lua internal DSL for Blueprint authoring (`flow_dsl` + `bp_dsl`).
//!
//! Raw AST (JSON) authoring cost is high at real Blueprint scale (hundreds
//! of lines and deep nesting for a multi-stage flow) — that cost motivated
//! this module. `flow_dsl.lua` (flow.ir vocabulary) and `bp_dsl.lua`
//! (Blueprint vocabulary, depends on `flow_dsl`) are baked into this
//! binary via `include_str!` and preloaded into a fresh `mlua::Lua` VM so
//! `require("flow_dsl")` / `require("bp_dsl")` resolve without touching
//! the filesystem. The `flow-ir` / `mlua-swarm-schema` crates are not
//! touched by this module — canonical JSON stays the wire format; the DSL
//! is purely an authoring-time convenience that emits it.
//!
//! # Crate positioning
//!
//! This crate is the DSL frontend only (`.bp.lua` → `serde_json::Value`).
//! The compile pipeline (linker → shape lint → BPReady) lives in the
//! sibling `mlua-swarm-compile` crate, which consumes the JSON this crate
//! produces. `mlua-swarm-schema` (types) stays free of the `mlua` runtime
//! dep so that consumers who only need type surfaces (e.g. the server's
//! wire codec) do not transitively pull the Lua interpreter.

const FLOW_DSL_SRC: &str = include_str!("flow_dsl.lua");
const BP_DSL_SRC: &str = include_str!("bp_dsl.lua");

/// The wire-level key `F.obj()` (`flow_dsl.lua`) emits — must match the
/// Lua-side `M.EMPTY_OBJECT_MARKER_KEY` literal exactly.
const EMPTY_OBJECT_MARKER_KEY: &str = "__mse_empty_object__";

/// Walk `value` in place and replace every JSON object shaped exactly
/// like `{ "<EMPTY_OBJECT_MARKER_KEY>": true }` (the wire shape `F.obj()`
/// emits) with a genuine empty JSON object (`{}`). Limited to that exact
/// single-key shape so an ordinary data field that happens to carry a key
/// with the same name is left untouched.
fn replace_empty_object_markers(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            let is_marker = map.len() == 1
                && map.get(EMPTY_OBJECT_MARKER_KEY) == Some(&serde_json::Value::Bool(true));
            if is_marker {
                *value = serde_json::Value::Object(serde_json::Map::new());
                return;
            }
            for v in map.values_mut() {
                replace_empty_object_markers(v);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                replace_empty_object_markers(v);
            }
        }
        _ => {}
    }
}

/// Register `flow_dsl` and `bp_dsl` in `lua`'s `package.preload` table so
/// `require("flow_dsl")` / `require("bp_dsl")` resolve to the baked-in Lua
/// source. Idempotent to call more than once on the same `Lua` (each call
/// simply re-sets the same two `preload` entries).
pub fn preload(lua: &mlua::Lua) -> mlua::Result<()> {
    let package: mlua::Table = lua.globals().get("package")?;
    let preload: mlua::Table = package.get("preload")?;

    preload.set(
        "flow_dsl",
        lua.create_function(|lua, ()| {
            lua.load(FLOW_DSL_SRC)
                .set_name("flow_dsl.lua")
                .eval::<mlua::Value>()
        })?,
    )?;
    preload.set(
        "bp_dsl",
        lua.create_function(|lua, ()| {
            lua.load(BP_DSL_SRC)
                .set_name("bp_dsl.lua")
                .eval::<mlua::Value>()
        })?,
    )?;
    Ok(())
}

/// Run a `.bp.lua` DSL script (source text, not a file path) in a fresh
/// `mlua::Lua` VM and return its result as `serde_json::Value`.
///
/// The script is expected to `require("flow_dsl")` and/or
/// `require("bp_dsl")` and `return` a Blueprint-shaped (or Expr/Node
/// -shaped, for narrower scripts) Lua table as its last expression.
///
/// Empty Lua tables are treated as empty JSON arrays rather than empty
/// objects (`encode_empty_tables_as_array`) — every plain empty table this
/// DSL can emit is a `Node`/`Expr` list field (`seq.children`, `and.args`,
/// `or.args`), never a legitimately-empty JSON object. A field that must
/// serialize as an empty JSON object uses the `F.obj()` marker
/// (`flow_dsl.lua`) instead of a bare `{}` table literal; this function
/// replaces every occurrence of that marker with a genuine empty JSON
/// object as a post-pass (`replace_empty_object_markers`) over the
/// converted value.
pub fn build_bp_from_script(script: &str) -> anyhow::Result<serde_json::Value> {
    use mlua::LuaSerdeExt;

    // `mlua::Error` wraps a boxed `dyn std::error::Error` without a
    // `Send + Sync` bound, so it does not satisfy anyhow's blanket `From`
    // impl (`?` cannot convert it directly) — stringify explicitly instead.
    let lua = mlua::Lua::new();
    preload(&lua).map_err(|e| anyhow::anyhow!("dsl preload failed: {e}"))?;
    let result: mlua::Value = lua
        .load(script)
        .set_name("<bp-script>")
        .eval()
        .map_err(|e| anyhow::anyhow!("bp-script eval failed: {e}"))?;
    let options = mlua::serde::de::Options::new().encode_empty_tables_as_array(true);
    let mut value: serde_json::Value = lua
        .from_value_with(result, options)
        .map_err(|e| anyhow::anyhow!("lua value -> json conversion failed: {e}"))?;
    replace_empty_object_markers(&mut value);
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preload_exposes_flow_dsl_and_bp_dsl() {
        let lua = mlua::Lua::new();
        preload(&lua).expect("preload must succeed");
        let ok: bool = lua
            .load(
                r#"
                local F = require("flow_dsl")
                local B = require("bp_dsl")
                return F ~= nil and B ~= nil
                "#,
            )
            .eval()
            .expect("require must succeed for both modules");
        assert!(ok, "flow_dsl / bp_dsl must both resolve via require()");
    }

    #[test]
    fn build_bp_from_script_returns_json_value() {
        let out = build_bp_from_script(
            r#"
            local F = require("flow_dsl")
            return { id = "t", flow = F.assign{ at = F.p("$.x"), value = F.lit(1) } }
            "#,
        )
        .expect("script must build");
        assert_eq!(out["id"], serde_json::json!("t"));
        assert_eq!(out["flow"]["kind"], serde_json::json!("assign"));
        assert_eq!(
            out["flow"]["at"],
            serde_json::json!({"op": "path", "at": "$.x"})
        );
    }

    #[test]
    fn build_bp_from_script_surfaces_lua_errors() {
        let err = build_bp_from_script("error(\"boom\")").expect_err("must propagate the error");
        assert!(err.to_string().contains("boom"));
    }

    #[test]
    fn f_obj_marker_becomes_a_genuine_empty_json_object() {
        let out = build_bp_from_script(
            r#"
            local F = require("flow_dsl")
            return { spec = F.obj(), other = {} }
            "#,
        )
        .expect("script must build");
        assert_eq!(out["spec"], serde_json::json!({}));
        assert!(
            out["spec"].is_object(),
            "F.obj() must become an object, not an array"
        );
        // A plain empty Lua table is still converted to an empty JSON
        // array (the pre-existing `encode_empty_tables_as_array` rule),
        // proving the marker replacement is scoped to `F.obj()`'s exact
        // one-key shape and does not affect ordinary empty tables.
        assert_eq!(out["other"], serde_json::json!([]));
    }

    /// GH #76 DSL sugar: `skip_on = { "SKIP", ... }` on a stage record wraps
    /// the stage's own body (step + optional retry loop) in a Branch
    /// whose `cond` is `in(<input>.parts["verdict"], <skip_on_list>)`.
    /// When the skip check hits, the stage body is elided (`then` =
    /// empty `Seq`); the gate/rest chain continues unchanged.
    #[test]
    fn bp_dsl_skip_on_compiles_to_branch_in_verdict_skip_on_list() {
        let out = build_bp_from_script(
            r#"
            local F = require("flow_dsl")
            local B = require("bp_dsl")
            return B.pipeline({
              B.stage "gate" { agent = "mock-gate" },
              B.stage "worker" {
                agent = "mock-worker",
                input = B.from "gate",
                skip_on = { "SKIP", "NOT_APPLICABLE" },
              },
              halted_at = "$.halted_at",
            })
            "#,
        )
        .expect("skip_on pipeline must build");

        // Top-level seq: [gate_step, rest].
        assert_eq!(out["kind"], serde_json::json!("seq"));
        let top_children = out["children"].as_array().expect("top seq children");
        assert_eq!(top_children.len(), 2);
        assert_eq!(top_children[0]["kind"], serde_json::json!("step"));
        assert_eq!(top_children[0]["ref"], serde_json::json!("mock-gate"));

        // `rest` = worker stage's compiled form. With skip_on, the
        // stage's body is wrapped in a branch whose cond is `in(...)`.
        let rest = &top_children[1];
        assert_eq!(rest["kind"], serde_json::json!("seq"));
        let rest_children = rest["children"].as_array().expect("rest seq children");
        // No gate/rest chain past worker (last stage, no halt_on / retry).
        let worker_guarded = &rest_children[0];
        assert_eq!(worker_guarded["kind"], serde_json::json!("branch"));

        // cond: in(needle=path("$.gate.parts[\"verdict\"]"),
        //         haystack=lit(["SKIP", "NOT_APPLICABLE"])).
        let cond = &worker_guarded["cond"];
        assert_eq!(cond["op"], serde_json::json!("in"));
        assert_eq!(
            cond["needle"],
            serde_json::json!({"op": "path", "at": "$.gate.parts[\"verdict\"]"})
        );
        assert_eq!(cond["haystack"]["op"], serde_json::json!("lit"));
        assert_eq!(
            cond["haystack"]["value"],
            serde_json::json!(["SKIP", "NOT_APPLICABLE"])
        );

        // then = empty seq (skip elides body).
        assert_eq!(
            worker_guarded["then"],
            serde_json::json!({"kind": "seq", "children": []})
        );

        // else = the original stage body (just the worker step here —
        // no retry, no gate).
        let body = &worker_guarded["else"];
        assert_eq!(body["kind"], serde_json::json!("seq"));
        assert_eq!(body["children"][0]["ref"], serde_json::json!("mock-worker"));
    }

    /// GH #76 DSL sugar: `skip_on` may coexist with `halt_on` on the same
    /// stage — the skip guard wraps the stage's OWN body (step +
    /// optional retry loop) and sits INSIDE the enclosing gate/rest
    /// chain, so a skipped stage still lets `halt_on`'s gate cond be
    /// evaluated against the (absent) `<out>` and thread through to
    /// `rest`.
    #[test]
    fn bp_dsl_skip_on_coexists_with_halt_on() {
        let out = build_bp_from_script(
            r#"
            local F = require("flow_dsl")
            local B = require("bp_dsl")
            return B.pipeline({
              B.stage "planner" { agent = "mock-planner" },
              B.stage "worker" {
                agent = "mock-worker",
                input = B.from "planner",
                skip_on = { "SKIP" },
                halt_on = { "BLOCKED" },
              },
              B.stage "publisher" { agent = "mock-publisher" },
              halted_at = "$.halted_at",
            })
            "#,
        )
        .expect("skip_on + halt_on pipeline must build");

        // Walk to the worker stage. Structure: top seq -> [planner,
        // rest]; rest = seq -> [worker_body, gate]; worker_body =
        // branch (skip guard).
        let rest = &out["children"][1];
        let worker_seq = rest;
        assert_eq!(worker_seq["kind"], serde_json::json!("seq"));
        let worker_children = worker_seq["children"]
            .as_array()
            .expect("worker seq children");
        assert_eq!(
            worker_children.len(),
            2,
            "skip guard + halt_on gate (with publisher threaded into gate else)"
        );

        // Child 0 = skip guard branch (skip_on).
        let skip_branch = &worker_children[0];
        assert_eq!(skip_branch["kind"], serde_json::json!("branch"));
        assert_eq!(skip_branch["cond"]["op"], serde_json::json!("in"));

        // Child 1 = halt_on gate (`branch`) whose cond is `eq` against
        // the current stage's own out.parts["verdict"].
        let halt_gate = &worker_children[1];
        assert_eq!(halt_gate["kind"], serde_json::json!("branch"));
        assert_eq!(halt_gate["cond"]["op"], serde_json::json!("eq"));
        assert_eq!(
            halt_gate["cond"]["lhs"],
            serde_json::json!({"op": "path", "at": "$.worker.parts[\"verdict\"]"})
        );
        // gate's else is publisher's compiled form (the pipeline tail).
        let gate_else = &halt_gate["else"];
        assert_eq!(gate_else["kind"], serde_json::json!("seq"));
        // publisher's step should be somewhere in that seq's children.
        let contains_publisher = gate_else["children"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .any(|c| c["ref"] == serde_json::json!("mock-publisher"))
            })
            .unwrap_or(false);
        assert!(
            contains_publisher,
            "halt_on gate else must thread the publisher stage through: {gate_else}"
        );
    }

    /// GH #76 DSL sugar: `skip_on = {}` is a no-op (equivalent to omitting
    /// the option). No branch is emitted, the stage compiles exactly
    /// as if `skip_on` were absent.
    #[test]
    fn bp_dsl_skip_on_empty_list_is_noop() {
        let with_empty = build_bp_from_script(
            r#"
            local B = require("bp_dsl")
            return B.pipeline({
              B.stage "worker" { agent = "mock-worker", skip_on = {} },
              halted_at = "$.halted_at",
            })
            "#,
        )
        .expect("skip_on={} pipeline must build");

        // Without any gate, retry, or a firing skip_on, the pipeline
        // compiles to [step, <final_else>] (no branch wrapping).
        let children = with_empty["children"].as_array().expect("seq children");
        assert_eq!(
            children.len(),
            2,
            "no skip guard emitted for empty skip_on: {with_empty}"
        );
        assert_eq!(children[0]["kind"], serde_json::json!("step"));
        assert_eq!(children[0]["ref"], serde_json::json!("mock-worker"));

        // Byte-identical to the same script without skip_on.
        let baseline = build_bp_from_script(
            r#"
            local B = require("bp_dsl")
            return B.pipeline({
              B.stage "worker" { agent = "mock-worker" },
              halted_at = "$.halted_at",
            })
            "#,
        )
        .expect("baseline pipeline must build");
        assert_eq!(with_empty, baseline, "skip_on = {{}} must be a no-op");
    }

    #[test]
    fn empty_object_marker_replacement_does_not_misfire_on_ordinary_data() {
        // A field that legitimately reuses the marker key name for
        // something other than `true` (or carries sibling keys) must not
        // be collapsed to `{}`.
        let out = build_bp_from_script(
            r#"
            return {
              a = { __mse_empty_object__ = false },
              b = { __mse_empty_object__ = true, extra = 1 },
            }
            "#,
        )
        .expect("script must build");
        assert_eq!(out["a"], serde_json::json!({"__mse_empty_object__": false}));
        assert_eq!(
            out["b"],
            serde_json::json!({"__mse_empty_object__": true, "extra": 1})
        );
    }
}
