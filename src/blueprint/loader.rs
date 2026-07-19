//! Compat shim — the Blueprint loader (`expand_file_refs` /
//! `load_blueprint_from_path` / `pre_read_default_agent_kind` /
//! `LoadError`) moved to the sibling `mlua-swarm-compile` crate's
//! `linker` module so that the CLI (`mse bp lint` / `mse bp build`)
//! and the server register path share one linker binary instead of
//! two hand-copied bodies. This module re-exports the moved surface
//! for pre-migration call sites; new code should reach into
//! `mlua_swarm_compile::linker` directly (issue 4c4e3eb8 Phase 2).

pub use mlua_swarm_compile::linker::{
    expand_file_refs, load_blueprint_from_path, pre_read_default_agent_kind, LoadError,
};
