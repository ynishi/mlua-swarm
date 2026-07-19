//! Blueprint compile pipeline for mlua-swarm.
//!
//! # Position in the crate graph
//!
//! - `mlua-swarm-schema` — pure Blueprint / AgentDef types (no runtime dep).
//! - `mlua-swarm-dsl` — Lua authoring frontend (`.bp.lua` → `serde_json::Value`).
//! - **`mlua-swarm-compile`** (this crate) — takes a wire body
//!   (`serde_json::Value`, whether hand-written or DSL-produced) and turns
//!   it into a `BPReady` state: refs resolved via the linker's include
//!   cascade, agent.md frontmatter parsed into `AgentDef`, and shape checks
//!   applied. Consumed by both the CLI (`mse bp lint` / `mse bp build`) and
//!   the server register path.
//! - `mlua-swarm` — engine runtime (dispatch / middleware / spawner
//!   adapters), depends on this crate for the register-time transformation.
//! - `mlua-swarm-cli` / `mlua-swarm-server` — call into this crate at the
//!   HTTP request → typed BP boundary, so authoring and registration go
//!   through the same code (single-linker discipline, GH issue 4c4e3eb8).

pub mod agent_md;
pub mod linker;

pub use linker::{
    env_blueprint_includes, expand_file_refs, expand_file_refs_with_config,
    load_blueprint_from_path, pre_read_default_agent_kind, pre_read_in_bp_includes, LoadError,
    ResolveConfig,
};
