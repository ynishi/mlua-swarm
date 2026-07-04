//! Lua bridge — parses Lua-authored BluePrints (a Lua-table
//! representation of `mlua_flow_ir::Node`) into a Rust `Node`, plus the
//! initial `ctx` when the Lua source also builds one.
//!
//! This module is **parse-only**. Execution goes through
//! `crate::service::TaskLaunchService` (compile, `service::linker::link`,
//! `Engine::dispatch_attempt_with`), not through this bridge — the
//! one-shot glue that used to live here (build a throwaway `mlua::Lua`,
//! parse, then drive the result straight through an `EngineDispatcher`)
//! was retired once `TaskLaunchService` became the sole production entry
//! point.
//!
//! `mlua-flow-ir` also ships a sync path (`flow.eval`) that lets a
//! dispatcher be written on the Lua side, but engine-async ↔ sync
//! impedance forces a `block_on` there. That would violate our
//! discipline, so we do not use it.

use crate::core::errors::EngineError;
use mlua::LuaSerdeExt;
use mlua_flow_ir::Node;
use serde_json::Value;

/// Load a Lua source, evaluate it, and pull out a BluePrint `Node`
/// (`mlua_flow_ir::Node`). The Lua source must ultimately
/// **`return` a table** — one that follows the flow.ir schema.
///
/// Example:
/// ```lua
/// return {
///   kind = "seq",
///   children = {
///     { kind = "step", ref = "agent-a",
///       in_  = {op="path", at="$.input"},
///       out  = {op="path", at="$.mid"} },
///     ...
///   }
/// }
/// ```
///
/// Note: the serde tag is `kind` for `Node` and `op` for `Expr`;
/// field names are the same snake_case as the Rust struct. `ref` is a
/// reserved word, so the Lua key stays `ref` and serde renames
/// `ref` ↔ `ref_`; the same holds for `in`.
pub fn parse_lua_blueprint(lua_src: &str) -> Result<Node, EngineError> {
    let lua = mlua::Lua::new();
    let bp_val: mlua::Value = lua
        .load(lua_src)
        .eval()
        .map_err(|e| EngineError::Internal(format!("lua eval: {e}")))?;
    let bp: Node = lua
        .from_value(bp_val)
        .map_err(|e| EngineError::Internal(format!("lua → Node parse: {e}")))?;
    Ok(bp)
}

/// Load a Lua source, and also build the initial `ctx` (a Lua table)
/// on the Lua side and convert it to a JSON `Value`. Returns
/// `(BluePrint, initial ctx)`. The Lua source is expected to return a
/// table of the form `return { bp = ..., ctx = ... }`.
pub fn parse_lua_blueprint_with_ctx(lua_src: &str) -> Result<(Node, Value), EngineError> {
    let lua = mlua::Lua::new();
    let outer: mlua::Table = lua
        .load(lua_src)
        .eval()
        .map_err(|e| EngineError::Internal(format!("lua eval: {e}")))?;

    let bp_val: mlua::Value = outer
        .get("bp")
        .map_err(|e| EngineError::Internal(format!("table missing `bp`: {e}")))?;
    let ctx_val: mlua::Value = outer
        .get("ctx")
        .map_err(|e| EngineError::Internal(format!("table missing `ctx`: {e}")))?;

    let bp: Node = lua
        .from_value(bp_val)
        .map_err(|e| EngineError::Internal(format!("lua → Node parse: {e}")))?;
    let ctx: Value = lua
        .from_value(ctx_val)
        .map_err(|e| EngineError::Internal(format!("lua → ctx parse: {e}")))?;
    Ok((bp, ctx))
}
