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
use crate::blueprint::Blueprint;
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
    /// Extension metadata slot (currently empty).
    #[serde(default)]
    pub meta: EnhanceSettingMeta,
}

impl EnhanceSettingInput {
    /// Convert an inline-data input into the Ref form
    /// (`EnhanceSetting`). The Blueprint's `id` becomes the
    /// setting's `blueprint_id`.
    pub fn into_ref(self) -> (Blueprint, EnhanceSetting) {
        let blueprint_id = BlueprintId::new(self.blueprint.id.clone());
        (
            self.blueprint,
            EnhanceSetting {
                id: self.id,
                blueprint_id,
                ttl_secs: self.ttl_secs,
                version: self.version,
                verifier_axes: self.verifier_axes,
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
            meta: EnhanceSettingMeta::default(),
        };
        let (split_bp, setting) = input.into_ref();
        assert_eq!(setting.id, "s1");
        assert_eq!(setting.blueprint_id.as_str(), bp_id);
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
}
