//! `EnhanceSetting` — the internal model that configures an
//! `EnhanceApplication`.
//!
//! The internal storage form is a **BlueprintId ref**: the store does not
//! hold the Blueprint body itself; that is resolved through
//! `BlueprintStore`. HTTP `POST`/`PUT` input goes through
//! [`EnhanceSettingInput`] and receives Blueprint data inline; the server
//! orchestrates a `BPStore.write_new` and converts to a Ref before
//! persisting.
//!
//! Runtime parameters (`ttl_secs`, `meta`) live on `EnhanceSetting`. The
//! `EnhanceApplication` fetches the setting on every tick and picks up
//! changes, so setting edits act as a hot reload.

use crate::application::VersionSelector;
use crate::blueprint::store::BlueprintId;
use crate::blueprint::{AgentDef, Blueprint};
use serde::{Deserialize, Serialize};

/// Internal storage form — the view held by the store and by
/// `EnhanceApplication`. A `BlueprintId` ref plus runtime parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnhanceSetting {
    /// Setting id — the server's single default setting uses `"default"`.
    pub id: String,
    /// The Blueprint this setting resolves to, via `BlueprintStore`.
    pub blueprint_id: BlueprintId,
    /// Operator-session lifetime (the TTL passed to `Engine::attach`).
    pub ttl_secs: u64,
    /// Which `BlueprintVersion` to take (`Latest` / `Fixed` /
    /// `SemverReq`).
    #[serde(default)]
    pub version: VersionSelector,
    /// Enhance-flow verifier axes: on/off. Injected into the init ctx as
    /// `$.verifiers` and fanned out in parallel by the flow.ir `Fanout`.
    /// An empty array skips verification — the committer commits
    /// unconditionally. Default: the four axes `["des", "canonical",
    /// "noop", "agent-ref"]`.
    #[serde(default = "default_verifier_axes")]
    pub verifier_axes: Vec<String>,
    /// Overrides the Blueprint's own `patch-spawner` agent definition.
    ///
    /// `None` = use whatever the orbit Blueprint declares. `Some(def)`
    /// swaps that agent out at dispatch time, so the spawner's execution
    /// backend (`agent_block` / `subprocess` / `operator`) can be changed
    /// without rewriting the Blueprint. Dispatch fails loud when the
    /// orbit Blueprint declares no agent under that name — a silently
    /// ignored override is the worst way for this to surface.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spawner: Option<AgentDef>,
    /// Extension metadata slot (currently empty).
    #[serde(default)]
    pub meta: EnhanceSettingMeta,
}

fn default_verifier_axes() -> Vec<String> {
    vec![
        "des".to_string(),
        "canonical".to_string(),
        "noop".to_string(),
        "agent-ref".to_string(),
    ]
}

/// HTTP `POST`/`PUT` input shape — the caller's view. Blueprint data is
/// inline; the server does `BPStore.write_new` and converts it to a Ref
/// before persisting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnhanceSettingInput {
    /// Setting id — the server's single default setting uses `"default"`.
    pub id: String,
    /// Blueprint data inline; the server persists it via `BPStore.write_new`
    /// and converts it to a `blueprint_id` ref before storing.
    pub blueprint: Blueprint,
    /// Operator-session lifetime (the TTL passed to `Engine::attach`).
    pub ttl_secs: u64,
    /// Which `BlueprintVersion` to take (`Latest` / `Fixed` / `SemverReq`).
    #[serde(default)]
    pub version: VersionSelector,
    /// Enhance-flow verifier axes: on/off. Defaults to the four canonical
    /// axes when omitted.
    #[serde(default = "default_verifier_axes")]
    pub verifier_axes: Vec<String>,
    /// Overrides the Blueprint's own `patch-spawner` agent definition —
    /// carried through to [`EnhanceSetting::spawner`] verbatim by
    /// [`EnhanceSettingInput::into_ref`]. It is *not* folded into the
    /// Blueprint that gets persisted: the override is a setting-level
    /// knob, so editing the setting reswaps the spawner without writing
    /// a new Blueprint version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spawner: Option<AgentDef>,
    /// Extension metadata slot (currently empty).
    #[serde(default)]
    pub meta: EnhanceSettingMeta,
}

impl EnhanceSettingInput {
    /// Convert an inline-data input into the Ref form
    /// (`EnhanceSetting`). The Blueprint's `id` becomes the
    /// setting's `blueprint_id`.
    pub fn into_ref(self) -> (Blueprint, EnhanceSetting) {
        let blueprint_id = self.blueprint.id.clone();
        (
            self.blueprint,
            EnhanceSetting {
                id: self.id,
                blueprint_id,
                ttl_secs: self.ttl_secs,
                version: self.version,
                verifier_axes: self.verifier_axes,
                spawner: self.spawner,
                meta: self.meta,
            },
        )
    }
}

/// Extension metadata attached to an `EnhanceSetting`. Placeholder —
/// something will land here for certain, so the slot exists up front.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EnhanceSettingMeta {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enhance::blueprint::default_blueprint;

    #[test]
    fn default_verifier_axes_has_4_canonical_axes() {
        let axes = default_verifier_axes();
        assert_eq!(axes, vec!["des", "canonical", "noop", "agent-ref"]);
    }

    #[test]
    fn input_into_ref_splits_blueprint_and_setting() {
        let bp = default_blueprint();
        let bp_id = bp.id.clone();
        let input = EnhanceSettingInput {
            id: "s1".into(),
            blueprint: bp,
            ttl_secs: 60,
            version: VersionSelector::default(),
            verifier_axes: default_verifier_axes(),
            spawner: None,
            meta: EnhanceSettingMeta::default(),
        };
        let (split_bp, setting) = input.into_ref();
        assert_eq!(setting.id, "s1");
        assert_eq!(setting.blueprint_id, bp_id);
        assert_eq!(setting.ttl_secs, 60);
        assert_eq!(setting.verifier_axes.len(), 4);
        assert_eq!(split_bp.id, bp_id);
    }

    #[test]
    fn setting_serde_roundtrip_preserves_verifier_axes() {
        let bp_id = BlueprintId::new("bp-xyz".to_string());
        let s = EnhanceSetting {
            id: "s2".into(),
            blueprint_id: bp_id,
            ttl_secs: 30,
            version: VersionSelector::default(),
            verifier_axes: vec!["des".into(), "noop".into()],
            spawner: None,
            meta: EnhanceSettingMeta::default(),
        };
        let j = serde_json::to_value(&s).unwrap();
        let s2: EnhanceSetting = serde_json::from_value(j).unwrap();
        assert_eq!(s2.verifier_axes, vec!["des", "noop"]);
        assert_eq!(s2.ttl_secs, 30);
    }

    #[test]
    fn setting_deserialize_applies_default_verifier_axes_when_omitted() {
        let json = serde_json::json!({
            "id": "s3",
            "blueprint_id": "bp-1",
            "ttl_secs": 10,
        });
        let s: EnhanceSetting = serde_json::from_value(json).unwrap();
        assert_eq!(s.verifier_axes, default_verifier_axes());
    }

    #[test]
    fn setting_deserialize_without_spawner_is_none_and_omits_it_on_serialize() {
        // Every pre-existing stored setting predates `spawner`, so the
        // absent key must round-trip as `None` and stay absent.
        let json = serde_json::json!({
            "id": "s4",
            "blueprint_id": "bp-1",
            "ttl_secs": 10,
        });
        let s: EnhanceSetting = serde_json::from_value(json).unwrap();
        assert!(s.spawner.is_none());
        let back = serde_json::to_value(&s).unwrap();
        assert!(back.get("spawner").is_none());
    }

    #[test]
    fn input_into_ref_carries_spawner_override_to_the_setting() {
        let bp = default_blueprint();
        let spawner: AgentDef = serde_json::from_value(serde_json::json!({
            "name": "patch-spawner",
            "kind": "subprocess",
            "spec": { "program": "true", "args": [] },
        }))
        .unwrap();
        let input = EnhanceSettingInput {
            id: "s5".into(),
            blueprint: bp,
            ttl_secs: 60,
            version: VersionSelector::default(),
            verifier_axes: default_verifier_axes(),
            spawner: Some(spawner.clone()),
            meta: EnhanceSettingMeta::default(),
        };
        let (split_bp, setting) = input.into_ref();
        assert_eq!(setting.spawner.as_ref(), Some(&spawner));
        // The override is a setting-level knob — it must not be folded
        // into the Blueprint that gets persisted.
        let bp_spawner = split_bp
            .agents
            .iter()
            .find(|a| a.name == "patch-spawner")
            .expect("default blueprint declares a patch-spawner agent");
        assert_ne!(bp_spawner, &spawner);
    }
}
