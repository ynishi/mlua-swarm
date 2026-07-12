//! `explain` — agent 定義 → 実行時契約の static 変換結果を可視化する
//! explain 層。
//!
//! # Architecture
//!
//! This is deliberately NOT a reimplementation of
//! `crate::middleware::agent_context::AgentContextMiddleware`'s merge
//! logic, nor of `crate::service::task_launch::derive_agent_ctx`.
//! Duplicating either would drift from the runtime the moment one side
//! changes without the other — exactly the "runtime との乖離" failure
//! mode that motivated rejecting a client-only MCP-side
//! reimplementation of the cascade for this explain layer.
//!
//! Instead, [`explain_agent_ctx`] reads the SAME three raw ingredients
//! `derive_agent_ctx` reads — `Blueprint.default_agent_ctx`, the agent's
//! `AgentMeta.meta_ref` resolved against `Blueprint.metas`, and the
//! agent's own `AgentMeta.ctx` — and applies the SAME
//! `AgentInline > MetaRef > BpGlobal` precedence that
//! `derive_agent_ctx` (base/inline shallow merge) composed with
//! `AgentContextMiddleware::merge_ctx_tiers` (global/per-agent shallow
//! merge, agent wins) already establishes end-to-end — just resolved
//! per-key, so each key's *winning tier* is visible instead of only the
//! final merged object. `explain_agent_ctx_matches_derive_agent_ctx_semantics`
//! (below) asserts the two paths agree on every key for the same
//! Blueprint, so a future change to the runtime merge order that this
//! module forgets to mirror fails a test rather than silently drifting.

use crate::blueprint::{AgentMeta, Blueprint};
use serde_json::Value;
use std::collections::BTreeMap;

/// One key's static cascade resolution result.
#[derive(Debug, Clone, PartialEq)]
pub struct CtxKeyResolution {
    /// The value this key resolves to (the winning tier's value).
    pub value: Value,
    /// Which tier supplied [`Self::value`].
    pub winning_tier: CtxTier,
}

/// The 3 tiers that are resolvable statically, from a `Blueprint` alone —
/// no launch-time (Run / Task) or dispatch-time (Step) input required.
///
/// At runtime the Run / Task / Step tiers always win over these (see
/// `crate::middleware::agent_context`'s module doc: "the full cascade is
/// Run > Task > Step > Agent > BP-global"); they only exist once a launch
/// (Run/Task) or a dispatched Step supplies them, so a static explain view
/// has nothing to show for them and they are out of scope here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CtxTier {
    /// `AgentMeta.ctx` — this agent's own inline context. Highest static
    /// priority.
    AgentInline,
    /// `AgentMeta.meta_ref`, resolved against the named `Blueprint.metas`
    /// pool. Sits between [`Self::AgentInline`] and [`Self::BpGlobal`].
    MetaRef,
    /// `Blueprint.default_agent_ctx` — the BP-wide default. Lowest static
    /// priority.
    BpGlobal,
}

/// Resolve `agent`'s effective static context (the 3-tier
/// `AgentInline > MetaRef > BpGlobal` subset of the full cascade — see
/// [`CtxTier`]'s doc) into a per-key winner table.
///
/// `None` when `agent` is not a name in `bp.agents`. A key present in
/// more than one tier resolves to its highest-priority tier's value. A
/// tier whose declared value is not a JSON `Object` is skipped entirely
/// — matching `AgentContextMiddleware::merge_ctx_tiers`'s
/// warn-and-skip behavior (that malformed shape never fails a spawn); an
/// explain view has no channel to surface the warning, so it silently
/// omits the tier rather than failing the whole call. An unresolved
/// `AgentMeta.meta_ref` (a name absent from `bp.metas`) is likewise
/// skipped — same defensive contract as `derive_agent_ctx`'s own
/// `meta_ref` resolution.
pub fn explain_agent_ctx(
    bp: &Blueprint,
    agent: &str,
) -> Option<BTreeMap<String, CtxKeyResolution>> {
    let agent_def = bp.agents.iter().find(|ad| ad.name == agent)?;
    let meta: Option<&AgentMeta> = agent_def.meta.as_ref();

    let mut out: BTreeMap<String, CtxKeyResolution> = BTreeMap::new();

    // Lowest priority first — later tiers below overwrite earlier ones.
    if let Some(Value::Object(obj)) = bp.default_agent_ctx.as_ref() {
        for (k, v) in obj {
            out.insert(
                k.clone(),
                CtxKeyResolution {
                    value: v.clone(),
                    winning_tier: CtxTier::BpGlobal,
                },
            );
        }
    }

    if let Some(meta_ref) = meta.and_then(|m| m.meta_ref.as_ref()) {
        if let Some(meta_def) = bp.metas.iter().find(|m| &m.name == meta_ref) {
            if let Value::Object(obj) = &meta_def.ctx {
                for (k, v) in obj {
                    out.insert(
                        k.clone(),
                        CtxKeyResolution {
                            value: v.clone(),
                            winning_tier: CtxTier::MetaRef,
                        },
                    );
                }
            }
        }
    }

    if let Some(Value::Object(obj)) = meta.and_then(|m| m.ctx.as_ref()) {
        for (k, v) in obj {
            out.insert(
                k.clone(),
                CtxKeyResolution {
                    value: v.clone(),
                    winning_tier: CtxTier::AgentInline,
                },
            );
        }
    }

    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blueprint::{
        current_schema_version, AgentDef, AgentKind, BlueprintMetadata, CompilerHints,
        CompilerStrategy, MetaDef,
    };
    use mlua_flow_ir::{Expr, Node as FlowNode};
    use serde_json::json;

    fn path(s: &str) -> Expr {
        Expr::Path {
            at: s.parse().expect("literal test path"),
        }
    }

    fn noop_flow() -> FlowNode {
        FlowNode::Step {
            ref_: "noop".to_string(),
            in_: path("$.input"),
            out: path("$.out"),
        }
    }

    fn agent(name: &str, meta: Option<AgentMeta>) -> AgentDef {
        AgentDef {
            name: name.to_string(),
            kind: AgentKind::RustFn,
            spec: json!({ "fn_id": name }),
            profile: None,
            meta,
        }
    }

    fn bp(
        agents: Vec<AgentDef>,
        metas: Vec<MetaDef>,
        default_agent_ctx: Option<Value>,
    ) -> Blueprint {
        Blueprint {
            schema_version: current_schema_version(),
            id: "explain-test".into(),
            flow: noop_flow(),
            agents,
            operators: vec![],
            metas,
            hints: CompilerHints::default(),
            strategy: CompilerStrategy::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx,
            default_context_policy: None,
            projection_placement: None,
            audits: vec![],
            degradation_policy: None,
        }
    }

    #[test]
    fn bp_global_only() {
        let blueprint = bp(
            vec![agent("scout", None)],
            vec![],
            Some(json!({ "work_dir": "/bp-global" })),
        );
        let resolved = explain_agent_ctx(&blueprint, "scout").expect("agent present");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved["work_dir"].value, json!("/bp-global"));
        assert_eq!(resolved["work_dir"].winning_tier, CtxTier::BpGlobal);
    }

    #[test]
    fn agent_inline_overrides_bp_global() {
        let blueprint = bp(
            vec![agent(
                "scout",
                Some(AgentMeta {
                    ctx: Some(json!({ "work_dir": "/inline" })),
                    ..Default::default()
                }),
            )],
            vec![],
            Some(json!({ "work_dir": "/bp-global", "extra": "kept" })),
        );
        let resolved = explain_agent_ctx(&blueprint, "scout").expect("agent present");
        assert_eq!(resolved["work_dir"].value, json!("/inline"));
        assert_eq!(resolved["work_dir"].winning_tier, CtxTier::AgentInline);
        // A BP-global-only key survives untouched.
        assert_eq!(resolved["extra"].value, json!("kept"));
        assert_eq!(resolved["extra"].winning_tier, CtxTier::BpGlobal);
    }

    #[test]
    fn meta_ref_wins_over_bp_global_and_agent_inline_wins_over_meta_ref() {
        let blueprint = bp(
            vec![agent(
                "scout",
                Some(AgentMeta {
                    ctx: Some(json!({ "work_dir": "/inline-wins" })),
                    meta_ref: Some("heavy-scan".to_string()),
                    ..Default::default()
                }),
            )],
            vec![MetaDef {
                name: "heavy-scan".to_string(),
                ctx: json!({ "work_dir": "/meta-ref", "budget": "high" }),
            }],
            Some(json!({ "work_dir": "/bp-global", "budget": "low", "extra": "kept" })),
        );
        let resolved = explain_agent_ctx(&blueprint, "scout").expect("agent present");
        // `work_dir`: AgentInline > MetaRef > BpGlobal.
        assert_eq!(resolved["work_dir"].value, json!("/inline-wins"));
        assert_eq!(resolved["work_dir"].winning_tier, CtxTier::AgentInline);
        // `budget`: only declared at MetaRef + BpGlobal, MetaRef wins.
        assert_eq!(resolved["budget"].value, json!("high"));
        assert_eq!(resolved["budget"].winning_tier, CtxTier::MetaRef);
        // `extra`: only declared at BpGlobal.
        assert_eq!(resolved["extra"].value, json!("kept"));
        assert_eq!(resolved["extra"].winning_tier, CtxTier::BpGlobal);
    }

    #[test]
    fn agent_not_in_blueprint_returns_none() {
        let blueprint = bp(vec![agent("scout", None)], vec![], None);
        assert!(explain_agent_ctx(&blueprint, "no-such-agent").is_none());
    }

    /// Done Criteria (subtask-1 §2): `derive_agent_ctx` semantics must
    /// stay byte-identical to what `explain_agent_ctx` reports as each
    /// key's winner — a machine-checked guard against the two paths
    /// silently drifting apart (see this module's Architecture doc).
    #[test]
    fn explain_agent_ctx_matches_derive_agent_ctx_semantics() {
        use crate::service::task_launch::derive_agent_ctx;

        let blueprint = bp(
            vec![
                agent(
                    "scout",
                    Some(AgentMeta {
                        ctx: Some(json!({ "work_dir": "/inline-wins" })),
                        meta_ref: Some("heavy-scan".to_string()),
                        ..Default::default()
                    }),
                ),
                agent("plain", None),
            ],
            vec![MetaDef {
                name: "heavy-scan".to_string(),
                ctx: json!({ "work_dir": "/meta-ref", "budget": "high" }),
            }],
            Some(json!({ "work_dir": "/bp-global", "budget": "low", "extra": "kept" })),
        );

        let (global, per_agent) = derive_agent_ctx(&blueprint);

        for agent_name in ["scout", "plain"] {
            // Reconstruct the same "global ⊕ per_agent[agent]" shallow
            // merge `AgentContextMiddleware::merge_ctx_tiers` performs at
            // spawn time (agent wins on key collision).
            let mut derived_merged = serde_json::Map::new();
            if let Some(Value::Object(g)) = &global {
                derived_merged.extend(g.clone());
            }
            if let Some(Value::Object(pa)) = per_agent.get(agent_name) {
                for (k, v) in pa {
                    derived_merged.insert(k.clone(), v.clone());
                }
            }

            let explained = explain_agent_ctx(&blueprint, agent_name).expect("agent present");
            let explained_values: serde_json::Map<String, Value> = explained
                .into_iter()
                .map(|(k, resolution)| (k, resolution.value))
                .collect();

            assert_eq!(
                explained_values, derived_merged,
                "explain_agent_ctx must resolve to the exact same merged \
                 result as derive_agent_ctx for agent '{agent_name}'"
            );
        }
    }
}
