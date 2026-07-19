//! `mlua-swarm-cli` library surface.
//!
//! The `mse` binary (`src/main.rs`) owns the CLI entry points (`serve` /
//! `mcp`) as binary-only modules. This library crate exists so that
//! `tests/*.rs` integration tests can exercise crate-internal modules
//! that have no CLI surface of their own yet.
//!
//! The DSL (`flow_dsl` + `bp_dsl`) previously lived here as a `dsl`
//! module; it now lives in the sibling `mlua-swarm-dsl` crate. This
//! module re-exports it for pre-migration call sites (`mlua_swarm_cli::dsl`)
//! and can be dropped once every caller updates to `mlua_swarm_dsl` directly.
pub use mlua_swarm_dsl as dsl;
