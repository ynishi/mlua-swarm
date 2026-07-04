//! Baseline agent — the canonical `RustFn` worker for bootstrap and
//! smoke tests.
//!
//! The library-layer source of truth that structurally removes the
//! pattern of each binary (server / MCP adapter / one-shot runner) inlining
//! its own "let's just wire up a quick echo" duplicate. Same shape as
//! `enhance/blueprint.rs`:
//!
//! - Export the agent name as a **role-literal const** ([`AG_IDENTITY`]).
//!   The naming reflects the function itself — an identity /
//!   passthrough of `ctx` — and deliberately avoids demo / echo
//!   framing.
//! - Bake **the real implementation** ([`extend_with_baseline`]). Not a
//!   false-positive fake — a straightforward passthrough that returns
//!   the input prompt in the value field, so callers can build smoke
//!   tests around "the baseline agent exists".
//! - **A builder** ([`extend_with_baseline`]) that adds the RustFn to
//!   an existing `RustFnInProcessSpawnerFactory`. Same shape as
//!   `enhance::blueprint::extend_factory`.
//!
//! ## Usage
//!
//! ```ignore
//! use mlua_swarm_engine_core::baseline::{extend_with_baseline, AG_IDENTITY};
//! use mlua_swarm_engine_core::RustFnInProcessSpawnerFactory;
//!
//! let factory = extend_with_baseline(RustFnInProcessSpawnerFactory::new());
//! // The factory now has a RustFn registered under fn_id = AG_IDENTITY ("identity").
//! ```
//!
//! Blueprint flows reference it as `spec: {"fn_id": baseline::AG_IDENTITY}`
//! (or the literal `"identity"`).

use crate::blueprint::compiler::RustFnInProcessSpawnerFactory;
use crate::worker::adapter::WorkerResult;
use serde_json::json;

/// The baseline `RustFn` agent name — `"identity"`.
///
/// Role: a passthrough worker that echoes the input prompt into
/// `value`. The logical id referenced from both the flow's `Step.ref`
/// and the agent spec's `fn_id`.
pub const AG_IDENTITY: &str = "identity";

/// Builder that adds the baseline `RustFn` to a base factory.
///
/// Only one worker exists today ([`AG_IDENTITY`]); if additional
/// baseline axes appear, they will be collected here.
pub fn extend_with_baseline(base: RustFnInProcessSpawnerFactory) -> RustFnInProcessSpawnerFactory {
    base.register_fn(AG_IDENTITY, |inv| async move {
        Ok(WorkerResult {
            value: json!({
                "by": "baseline-identity",
                "agent": inv.agent,
                "echoed": inv.prompt,
            }),
            ok: true,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blueprint::compiler::{SpawnerFactory, SpawnerFactoryKind};
    use crate::blueprint::{AgentDef, AgentKind};
    use serde_json::json;

    #[test]
    fn ag_identity_is_stable_literal() {
        assert_eq!(AG_IDENTITY, "identity");
    }

    #[test]
    fn extend_with_baseline_builds_identity_adapter() {
        let factory = extend_with_baseline(RustFnInProcessSpawnerFactory::new());
        assert_eq!(
            <RustFnInProcessSpawnerFactory as SpawnerFactoryKind>::KIND,
            AgentKind::RustFn
        );
        let agent_def = AgentDef {
            name: "id".into(),
            kind: AgentKind::RustFn,
            spec: json!({ "fn_id": AG_IDENTITY }),
            profile: None,
            meta: None,
        };
        factory
            .build(&agent_def, None)
            .expect("AG_IDENTITY fn must be registered for AgentDef build");
    }
}
