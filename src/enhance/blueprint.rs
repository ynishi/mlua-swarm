//! Enhance flow — the system-default Blueprint plus its Lua-based worker
//! wiring.
//!
//! Expresses the existing enhance pipeline (`PatchSpawner` /
//! `PatchApplier` / `VerifierChain`) as flow.ir `Node`s split across four
//! steps. Evaluating this through an `EngineDispatcher` runs one issue as
//! one task: `patch → apply → verify (Fanout, N axes in parallel) →
//! commit/reject`.
//!
//! ## Worker shape per step
//!
//! - `patch-spawner`   — `AgentKind::AgentBlock` (LLM-driven; turns the
//!   issue's natural-language `intent` into RFC 6902 ops). Wired via
//!   `AgentBlockInProcessSpawnerFactory`; the script is
//!   `assets/operator_scripts/blueprint_patch_spawner.lua`.
//! - `patch-applier`   — `AgentKind::Lua` (pure-Lua RFC 6902 apply plus
//!   the semver bump).
//! - `verifier-router` — `AgentKind::Lua` (four verifier implementations
//!   inline in pure Lua).
//! - `committer`       — `AgentKind::Lua` (verdict reduction in pure
//!   Lua).
//!
//! ## Input context (`TaskSpec.initial_directive` JSON)
//!
//! ```jsonc
//! {
//!   "issue": { "issue_id": "...", "intent": "...", "target_blueprint_id": "..." },
//!   "prev_bp_yaml": "<full yaml>",
//!   "prev_hash":    "<hex>",
//!   "epoch_id":     "<epoch>",
//!   "verifiers":    ["des", "canonical", "noop", "agent-ref"]
//! }
//! ```
//!
//! ## Flow
//!
//! ```text
//! Seq
//!  ├─ Step    ref=patch-spawner    in=$              out=$.patch     (AgentBlock + Anthropic call)
//!  ├─ Step    ref=patch-applier    in=$              out=$.applied   (Pure Lua)
//!  ├─ Fanout  items=$.verifiers  bind=$.axis  join=All  out=$.verdicts
//!  │    body= Step ref=verifier-router  in=$  out=$.verdict          (Pure Lua)
//!  └─ Step    ref=committer        in=$              out=$.commit    (Pure Lua)
//! ```
//!
//! ## Host bridges — three primitives (#15)
//!
//! - `host.yaml_to_json(yaml_str)` — `serde_yaml::from_str` returning a
//!   JSON `Value`.
//! - `host.canonical_yaml(json_val)` — a `Blueprint` round-trip that
//!   returns canonical YAML.
//! - `host.content_hash(bytes)` — 32-byte blake3, hex-encoded.
//!
//! All three are pure ser/de/hash primitives; the domain logic lives on
//! the Lua side. The old `host.patch_spawn` / `host.dry_run` /
//! `host.verify` (domain delegates) are gone.

use crate::blueprint::compiler::{HostBridge, LuaInProcessSpawnerFactory, LuaScriptSource};
use crate::blueprint::store::ContentHash;
use crate::blueprint::Blueprint;
use serde_json::Value;

// ──────────────────────────────────────────────────────────────────────────
// Blueprint factory
// ──────────────────────────────────────────────────────────────────────────

/// Agent name constants (used as `flow Step.ref`). The same logical ids
/// are referenced from the YAML and the Lua scripts.
pub const AG_PATCH_SPAWNER: &str = "patch-spawner";
/// `patch-applier` step ref — pure-Lua RFC 6902 apply plus the semver bump.
pub const AG_PATCH_APPLIER: &str = "patch-applier";
/// `verifier-router` step ref — the four verifier implementations, pure Lua.
pub const AG_VERIFIER_ROUTER: &str = "verifier-router";
/// `committer` step ref — verdict reduction, pure Lua.
pub const AG_COMMITTER: &str = "committer";

/// Default verifier axes — used when `EnhanceSetting.verifier_axes` is
/// unset, and by smoke tests.
pub const DEFAULT_VERIFIER_AXES: &[&str] = &["des", "canonical", "noop", "agent-ref"];

/// Thin loader that returns the system-default Enhance Blueprint by
/// reading the `default_blueprint.yaml` source of truth.
pub fn default_blueprint() -> Blueprint {
    const YAML: &str = include_str!("default_blueprint.yaml");
    serde_yaml::from_str(YAML)
        .expect("enhance/default_blueprint.yaml must be a valid Blueprint serialization")
}

// ──────────────────────────────────────────────────────────────────────────
// LuaInProcessSpawnerFactory builder — 3 Lua worker + 3 primitive bridge
// ──────────────────────────────────────────────────────────────────────────

/// Build the enhance [`LuaInProcessSpawnerFactory`].
///
/// **Adds** three Lua workers (`patch-applier` / `verifier-router` /
/// `committer`) and three primitive bridges to `base`. The
/// `patch-spawner` moved to the `AgentKind::AgentBlock` axis, so the
/// caller registers an `AgentBlockInProcessSpawnerFactory` on
/// `SpawnerRegistry` separately.
///
/// The old `patch_spawner: Arc<dyn PatchSpawner>` argument is gone
/// (routed through the AgentBlock axis, #18).
pub fn extend_factory(base: LuaInProcessSpawnerFactory) -> LuaInProcessSpawnerFactory {
    base.with_bridge("yaml_to_json", make_yaml_to_json_bridge())
        .with_bridge("canonical_yaml", make_canonical_yaml_bridge())
        .with_bridge("content_hash", make_content_hash_bridge())
        .register_lua(
            AG_PATCH_APPLIER,
            LuaScriptSource::new(
                include_str!("scripts/patch_applier.lua"),
                "patch_applier.lua",
            ),
        )
        .register_lua(
            AG_VERIFIER_ROUTER,
            LuaScriptSource::new(
                include_str!("scripts/verifier_router.lua"),
                "verifier_router.lua",
            ),
        )
        .register_lua(
            AG_COMMITTER,
            LuaScriptSource::new(include_str!("scripts/committer.lua"), "committer.lua"),
        )
}

// ──────────────────────────────────────────────────────────────────────────
// three host primitive bridges
// ──────────────────────────────────────────────────────────────────────────

/// `host.yaml_to_json(yaml_str)` bridge — a primitive.
///
/// Runs `serde_yaml::from_str::<serde_json::Value>` to turn YAML into a
/// JSON `Value`. Used by `patch_applier.lua` to turn `prev_bp_yaml` into
/// a Lua table.
fn make_yaml_to_json_bridge() -> HostBridge {
    HostBridge::new(|arg: Value| -> Result<Value, String> {
        let yaml_str = arg
            .as_str()
            .ok_or_else(|| "yaml_to_json: expected string arg (= YAML source)".to_string())?;
        let json_val: Value =
            serde_yaml::from_str(yaml_str).map_err(|e| format!("yaml_to_json: parse: {e}"))?;
        Ok(json_val)
    })
}

/// `host.canonical_yaml(json_val)` bridge — a primitive.
///
/// Runs `serde_json::Value` → `Blueprint` round-trip →
/// `serde_yaml::to_string` to produce canonical YAML — the same shape
/// the Rust-side `PatchApplier::dry_run` produced.
///
/// The Blueprint round-trip is deliberate: if the Lua table produced by
/// RFC 6902 apply is not schema-consistent, the re-deserialise fails.
/// That doubles as a DesVerifier-equivalent guarantee — "YAML build
/// succeeded" implies "Blueprint shape is consistent".
fn make_canonical_yaml_bridge() -> HostBridge {
    HostBridge::new(|arg: Value| -> Result<Value, String> {
        let bp: Blueprint = serde_json::from_value(arg)
            .map_err(|e| format!("canonical_yaml: blueprint deserialize: {e}"))?;
        let yaml =
            serde_yaml::to_string(&bp).map_err(|e| format!("canonical_yaml: serialize: {e}"))?;
        Ok(Value::String(yaml))
    })
}

/// `host.content_hash(bytes)` bridge — a primitive.
///
/// Runs `ContentHash::from_bytes` (a 32-byte blake3 hash) and returns
/// the hex-encoded string.
fn make_content_hash_bridge() -> HostBridge {
    HostBridge::new(|arg: Value| -> Result<Value, String> {
        let s = arg
            .as_str()
            .ok_or_else(|| "content_hash: expected string arg (= bytes to hash)".to_string())?;
        let hash = ContentHash::from_bytes(s.as_bytes());
        Ok(Value::String(hash.to_hex()))
    })
}

// ──────────────────────────────────────────────────────────────────────────
// UT (L1) — 3 primitive bridge + default_blueprint loader
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Test helper for invoking a bridge directly (a `HostBridge` runs
    /// as a single-`Value`-argument function).
    fn invoke(bridge: HostBridge, arg: Value) -> Result<Value, String> {
        bridge.call(arg)
    }

    #[test]
    fn yaml_to_json_parses_valid_yaml() {
        let b = make_yaml_to_json_bridge();
        let got = invoke(b, json!("a: 1\nb: [2, 3]\n")).unwrap();
        assert_eq!(got, json!({"a": 1, "b": [2, 3]}));
    }

    #[test]
    fn yaml_to_json_rejects_non_string_arg() {
        let b = make_yaml_to_json_bridge();
        let err = invoke(b, json!(42)).unwrap_err();
        assert!(err.contains("expected string arg"), "got: {err}");
    }

    #[test]
    fn yaml_to_json_propagates_parse_error() {
        let b = make_yaml_to_json_bridge();
        let err = invoke(b, json!("a: [unterminated")).unwrap_err();
        assert!(err.starts_with("yaml_to_json: parse:"), "got: {err}");
    }

    #[test]
    fn canonical_yaml_round_trips_default_blueprint() {
        // default_blueprint.yaml → Blueprint → JSON → canonical_yaml →
        // re-deserialise: the round-trip must be lossless.
        let bp = default_blueprint();
        let json_val = serde_json::to_value(&bp).unwrap();
        let b = make_canonical_yaml_bridge();
        let yaml = invoke(b, json_val).unwrap();
        let yaml_str = yaml.as_str().unwrap();
        let bp2: Blueprint = serde_yaml::from_str(yaml_str).unwrap();
        assert_eq!(
            serde_json::to_value(&bp).unwrap(),
            serde_json::to_value(&bp2).unwrap(),
        );
    }

    #[test]
    fn canonical_yaml_rejects_non_blueprint_shape() {
        let b = make_canonical_yaml_bridge();
        let err = invoke(b, json!({"not": "a blueprint"})).unwrap_err();
        assert!(
            err.starts_with("canonical_yaml: blueprint deserialize:"),
            "got: {err}"
        );
    }

    #[test]
    fn content_hash_is_deterministic() {
        let b1 = make_content_hash_bridge();
        let b2 = make_content_hash_bridge();
        let h1 = invoke(b1, json!("hello")).unwrap();
        let h2 = invoke(b2, json!("hello")).unwrap();
        assert_eq!(h1, h2);
        // blake3 hex = 64 chars
        assert_eq!(h1.as_str().unwrap().len(), 64);
    }

    #[test]
    fn content_hash_differs_for_different_input() {
        let b1 = make_content_hash_bridge();
        let b2 = make_content_hash_bridge();
        let h1 = invoke(b1, json!("foo")).unwrap();
        let h2 = invoke(b2, json!("bar")).unwrap();
        assert_ne!(h1, h2);
    }

    #[test]
    fn content_hash_rejects_non_string_arg() {
        let b = make_content_hash_bridge();
        let err = invoke(b, json!(null)).unwrap_err();
        assert!(err.contains("expected string arg"), "got: {err}");
    }

    #[test]
    fn default_blueprint_loads_and_has_4_enhance_agents() {
        let bp = default_blueprint();
        let yaml = serde_yaml::to_string(&bp).unwrap();
        for name in [
            AG_PATCH_SPAWNER,
            AG_PATCH_APPLIER,
            AG_VERIFIER_ROUTER,
            AG_COMMITTER,
        ] {
            assert!(
                yaml.contains(name),
                "default_blueprint must reference agent {name}, yaml=\n{yaml}"
            );
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // patch_applier.lua post-hook UT (hash-consistency axis)
    //
    // When the post-hook receives a `/agents/N/profile/system_prompt`
    // replace op, it must auto-update the same agent's
    // profile.version_hash to the blake3 hex of the new body. Isolated
    // test: raw mlua VM + three host-bridge mocks.
    // ──────────────────────────────────────────────────────────────────────

    use mlua::{Lua, LuaSerdeExt};

    fn run_patch_applier_lua(ctx_json: Value) -> Value {
        let lua = Lua::new();
        // Inject three bridges — `host = { yaml_to_json, canonical_yaml,
        // content_hash }` — as Rust closures.
        let host = lua.create_table().unwrap();
        host.set(
            "yaml_to_json",
            lua.create_function(|lua, arg: mlua::Value| -> mlua::Result<mlua::Value> {
                let s: String = lua.from_value(arg)?;
                let json: Value = serde_yaml::from_str(&s).map_err(mlua::Error::external)?;
                lua.to_value(&json)
            })
            .unwrap(),
        )
        .unwrap();
        host.set(
            "canonical_yaml",
            lua.create_function(|lua, arg: mlua::Value| -> mlua::Result<String> {
                let json: Value = lua.from_value(arg)?;
                let bp: Blueprint = serde_json::from_value(json).map_err(mlua::Error::external)?;
                serde_yaml::to_string(&bp).map_err(mlua::Error::external)
            })
            .unwrap(),
        )
        .unwrap();
        host.set(
            "content_hash",
            lua.create_function(|_, arg: String| -> mlua::Result<String> {
                Ok(ContentHash::from_bytes(arg.as_bytes()).to_hex())
            })
            .unwrap(),
        )
        .unwrap();
        lua.globals().set("host", host).unwrap();
        lua.globals()
            .set("_CTX", lua.to_value(&ctx_json).unwrap())
            .unwrap();

        let script = include_str!("scripts/patch_applier.lua");
        let ret: mlua::Value = lua.load(script).eval().unwrap();
        lua.from_value(ret).unwrap()
    }

    fn seed_bp_with_profile(system_prompt: &str) -> Blueprint {
        use crate::blueprint::{AgentDef, AgentKind, AgentProfile, BlueprintMetadata};
        use mlua_flow_ir::{Expr, Node as FlowNode};
        Blueprint {
            schema_version: crate::blueprint::current_schema_version(),
            id: "test-bp".into(),
            flow: FlowNode::Step {
                ref_: "worker".into(),
                in_: Expr::Lit { value: Value::Null },
                out: Expr::Path { at: "$.out".into() },
            },
            agents: vec![AgentDef {
                name: "worker".into(),
                kind: AgentKind::Operator,
                spec: Value::Null,
                profile: Some(AgentProfile {
                    system_prompt: system_prompt.into(),
                    version_hash: Some(ContentHash::from_bytes(system_prompt.as_bytes()).to_hex()),
                    ..Default::default()
                }),
                meta: None,
            }],
            operators: vec![],
            metas: vec![],
            hints: Default::default(),
            strategy: Default::default(),
            metadata: BlueprintMetadata {
                version_label: Some("0.0.1".into()),
                ..Default::default()
            },
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx: None,
            default_context_policy: None,
            projection_placement: None,
        }
    }

    #[test]
    fn post_hook_updates_version_hash_on_system_prompt_replace() {
        let bp = seed_bp_with_profile("old body");
        let prev_yaml = serde_yaml::to_string(&bp).unwrap();
        let old_hash = ContentHash::from_bytes("old body".as_bytes()).to_hex();
        let new_body = "brand new body";
        let expected_new_hash = ContentHash::from_bytes(new_body.as_bytes()).to_hex();

        let ctx = json!({
            "prev_bp_yaml": prev_yaml,
            "prev_hash": "prev-hash-placeholder",
            "epoch_id": "epoch-1",
            "patch": {
                "ops": [
                    {"op": "replace", "path": "/agents/0/profile/system_prompt", "value": new_body}
                ],
                "bump": "patch"
            }
        });

        let out = run_patch_applier_lua(ctx);
        let new_bp = out.get("new_bp_json").expect("new_bp_json present");
        let agent0 = &new_bp["agents"][0];
        assert_eq!(agent0["profile"]["system_prompt"].as_str(), Some(new_body));
        assert_eq!(
            agent0["profile"]["version_hash"].as_str(),
            Some(expected_new_hash.as_str()),
            "post-hook must recompute version_hash for replaced body"
        );
        // old hash should NOT survive
        assert_ne!(
            agent0["profile"]["version_hash"].as_str(),
            Some(old_hash.as_str())
        );
    }

    #[test]
    fn post_hook_no_op_when_no_agent_body_touched() {
        let bp = seed_bp_with_profile("keep me");
        let prev_yaml = serde_yaml::to_string(&bp).unwrap();
        let expected = ContentHash::from_bytes("keep me".as_bytes()).to_hex();
        let ctx = json!({
            "prev_bp_yaml": prev_yaml,
            "prev_hash": "prev",
            "epoch_id": "e",
            "patch": {
                "ops": [
                    {"op": "replace", "path": "/metadata/description", "value": "updated"}
                ],
                "bump": "patch"
            }
        });
        let out = run_patch_applier_lua(ctx);
        let new_bp = out.get("new_bp_json").unwrap();
        // metadata patched, body untouched → version_hash unchanged
        assert_eq!(
            new_bp["agents"][0]["profile"]["version_hash"].as_str(),
            Some(expected.as_str()),
        );
    }
}
