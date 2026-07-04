//! Enhance domain — Rust wiring for the Blueprint self-enhancement flow.
//!
//! ## Post-refactor scope
//!
//! The old Rust implementation (`PatchSpawner` trait + impls, `PatchApplier`,
//! four `VerifierAdapter` variants, `VerifierChain`, `VerifyContext`,
//! `VerifyOutcome`) is gone. Everything moved to Pure Lua — see
//! `scripts/{patch_applier,verifier_router,committer}.lua` and the three
//! primitive bridges. The patch spawner rides on the `AgentKind::AgentBlock`
//! axis, driven by an LLM through the `agent-block-core` SDK.
//!
//! What stayed on the Rust side:
//!
//! - `blueprint::default_blueprint()` — loader for the `default_blueprint.yaml`
//!   source of truth.
//! - `blueprint::extend_factory()` — builder that registers the Lua scripts
//!   and the primitive bridges.
//! - `EnhanceSetting` — the domain Entity. Its persistence contract lives in
//!   `crate::store::enhance_setting`; the run log lives in
//!   `crate::store::enhance_log`.

pub mod blueprint;
pub mod setting;

pub use setting::{EnhanceSetting, EnhanceSettingInput, EnhanceSettingMeta};
