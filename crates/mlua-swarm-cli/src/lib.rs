//! `mlua-swarm-cli` library surface.
//!
//! The `mse` binary (`src/main.rs`) owns the CLI entry points (`serve` /
//! `mcp`) as binary-only modules. This library crate exists so that
//! `tests/*.rs` integration tests can exercise crate-internal modules that
//! have no CLI surface of their own yet — currently just [`dsl`], the
//! flow.ir / Blueprint authoring DSL (see its module doc for the Lua API).

pub mod dsl;
