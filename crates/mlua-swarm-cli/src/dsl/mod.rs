//! Lua internal DSL for Blueprint authoring (`flow_dsl` + `bp_dsl`).
//!
//! Raw AST (JSON) authoring cost is high at real BP scale — see the
//! `flow-dsl` design issue for the phase-b/c flow numbers that motivated
//! this module. `flow_dsl.lua` (flow.ir vocabulary) and `bp_dsl.lua`
//! (Blueprint vocabulary, depends on `flow_dsl`) are baked into this
//! binary via `include_str!` and preloaded into a fresh `mlua::Lua` VM so
//! `require("flow_dsl")` / `require("bp_dsl")` resolve without touching
//! the filesystem. The `flow-ir` / `mlua-swarm-schema` crates are not
//! touched by this module — canonical JSON stays the wire format; the DSL
//! is purely an authoring-time convenience that emits it.

const FLOW_DSL_SRC: &str = include_str!("flow_dsl.lua");
const BP_DSL_SRC: &str = include_str!("bp_dsl.lua");

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
/// objects (`encode_empty_tables_as_array`) — every empty table this DSL
/// can emit is a `Node`/`Expr` list field (`seq.children`, `and.args`,
/// `or.args`), never a legitimately-empty JSON object, so this is safe
/// for every shape this module produces.
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
    let value: serde_json::Value = lua
        .from_value_with(result, options)
        .map_err(|e| anyhow::anyhow!("lua value -> json conversion failed: {e}"))?;
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
}
