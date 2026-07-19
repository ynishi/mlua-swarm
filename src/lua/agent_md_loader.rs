//! Compat shim — the `agent.md` frontmatter loader (`parse` /
//! `load_file` / `load_dir` / `compute_body_hash` / `LoadError`) moved
//! to the sibling `mlua-swarm-compile` crate's `agent_md` module (issue
//! 4c4e3eb8 Phase 2). New code should reach into
//! `mlua_swarm_compile::agent_md` directly; this module re-exports the
//! same surface so pre-migration call sites keep compiling.

pub use mlua_swarm_compile::agent_md::{compute_body_hash, load_dir, load_file, parse, LoadError};
