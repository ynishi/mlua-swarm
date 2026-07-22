//! Platform-neutral execution binding boundary.
//!
//! Swarm owns request construction, requested/effective validation, and
//! immutable Run snapshots. Execution environments implement
//! [`AgentBindingProvider`] and report what they can actually enforce; an
//! official platform adapter is one implementation of this same interface.

use crate::blueprint::{BindReceipt, BindRequest, BindingAttestation, BoundAgent, Runner};
use async_trait::async_trait;
use std::collections::{BTreeSet, HashMap, HashSet};
use thiserror::Error;

/// Execution-environment provider for effective agent capabilities.
#[async_trait]
pub trait AgentBindingProvider: Send + Sync {
    /// Resolve all requested bindings as one launch-time transaction.
    /// Returning fewer, extra, or duplicate receipts is rejected by Core.
    async fn bind(
        &self,
        requests: &[BindRequest],
    ) -> Result<Vec<BindReceipt>, BindingProviderError>;
}

/// Failure reported by a provider or by Core's receipt validation.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BindingProviderError {
    /// The provider could not inspect or resolve its execution environment.
    #[error("binding provider failed: {0}")]
    Provider(String),
    /// More than one receipt used the same logical agent correlation key.
    #[error("binding provider returned duplicate receipt for agent '{agent}'")]
    DuplicateReceipt {
        /// Duplicate logical agent name.
        agent: String,
    },
    /// A requested agent had no receipt.
    #[error("binding provider returned no receipt for agent '{agent}'")]
    MissingReceipt {
        /// Missing logical agent name.
        agent: String,
    },
    /// A receipt did not correspond to any request.
    #[error("binding provider returned unexpected receipt for agent '{agent}'")]
    UnexpectedReceipt {
        /// Unexpected logical agent name.
        agent: String,
    },
    /// A receipt was produced for an older or different declaration.
    #[error(
        "binding receipt for agent '{agent}' attests request digest '{effective}', expected '{requested}'"
    )]
    RequestDigestMismatch {
        /// Logical agent name.
        agent: String,
        /// Digest sent in the current request.
        requested: crate::blueprint::BindingDigest,
        /// Digest echoed by the provider.
        effective: crate::blueprint::BindingDigest,
    },
    /// The provider identifier is required for provenance.
    #[error("binding receipt for agent '{agent}' has an empty provider_id")]
    EmptyProviderId {
        /// Logical agent name.
        agent: String,
    },
    /// Model resolution was requested but the provider did not identify the
    /// effective model.
    #[error(
        "binding receipt for agent '{agent}' omitted resolved_model for requested model '{requested}'"
    )]
    MissingResolvedModel {
        /// Logical agent name.
        agent: String,
        /// Requested model alias or tier.
        requested: String,
    },
    /// The execution environment cannot enforce every requested tool.
    #[error("binding receipt for agent '{agent}' is missing requested tools: {missing:?}")]
    MissingTools {
        /// Logical agent name.
        agent: String,
        /// Requested tools absent from the effective grant.
        missing: Vec<String>,
    },
    /// The effective launch variant differs from the requested variant.
    #[error(
        "binding receipt for agent '{agent}' resolved launch variant {effective:?}, expected '{requested}'"
    )]
    VariantMismatch {
        /// Logical agent name.
        agent: String,
        /// Requested launch variant.
        requested: String,
        /// Provider-reported effective launch variant.
        effective: Option<String>,
    },
    /// The accepted attestation could not be incorporated into replay
    /// identity.
    #[error("binding digest recompute failed: {0}")]
    Digest(String),
}

/// Build platform-neutral requests for every Runner-bound agent.
pub fn binding_requests(bound_agents: &[BoundAgent]) -> Vec<BindRequest> {
    bound_agents
        .iter()
        .filter_map(|bound| {
            let runner = bound.runner.as_ref()?;
            let (requested_tools, launch_variant) = match runner {
                Runner::WsClaudeCode { variant, tools } => {
                    (canonical_tools(tools), Some(variant.clone()))
                }
                Runner::AgentBlockInProcess { tools } => (canonical_tools(tools), None),
            };
            Some(BindRequest {
                agent: bound.agent.name.clone(),
                request_digest: bound.binding_digest.clone(),
                requested_model: bound
                    .agent
                    .profile
                    .as_ref()
                    .and_then(|profile| profile.model.clone()),
                requested_tools,
                launch_variant,
            })
        })
        .collect()
}

/// Ask `provider` to bind all Runner-backed agents, validate every receipt,
/// and pin accepted attestations into the snapshots.
pub async fn attest_bound_agents(
    provider: &dyn AgentBindingProvider,
    bound_agents: &mut [BoundAgent],
) -> Result<(), BindingProviderError> {
    let requests = binding_requests(bound_agents);
    if requests.is_empty() {
        return Ok(());
    }

    let receipts = provider.bind(&requests).await?;
    let requested_names: HashSet<&str> = requests.iter().map(|r| r.agent.as_str()).collect();
    let mut by_agent = HashMap::with_capacity(receipts.len());
    for receipt in receipts {
        if !requested_names.contains(receipt.agent.as_str()) {
            return Err(BindingProviderError::UnexpectedReceipt {
                agent: receipt.agent,
            });
        }
        let agent = receipt.agent.clone();
        if by_agent.insert(agent.clone(), receipt).is_some() {
            return Err(BindingProviderError::DuplicateReceipt { agent });
        }
    }

    let mut accepted = Vec::with_capacity(requests.len());
    for request in requests {
        let receipt = by_agent.remove(&request.agent).ok_or_else(|| {
            BindingProviderError::MissingReceipt {
                agent: request.agent.clone(),
            }
        })?;
        let attestation = validate_receipt(&request, receipt)?;
        accepted.push((request.agent, attestation));
    }

    for (agent, attestation) in accepted {
        let bound = bound_agents
            .iter_mut()
            .find(|bound| bound.agent.name == agent)
            .expect("BindRequest is constructed from BoundAgent");
        bound
            .set_attestation(attestation)
            .map_err(|error| BindingProviderError::Digest(error.to_string()))?;
    }
    Ok(())
}

fn validate_receipt(
    request: &BindRequest,
    receipt: BindReceipt,
) -> Result<BindingAttestation, BindingProviderError> {
    if receipt.request_digest != request.request_digest {
        return Err(BindingProviderError::RequestDigestMismatch {
            agent: request.agent.clone(),
            requested: request.request_digest.clone(),
            effective: receipt.request_digest,
        });
    }
    if receipt.provider_id.trim().is_empty() {
        return Err(BindingProviderError::EmptyProviderId {
            agent: request.agent.clone(),
        });
    }
    if let Some(requested) = &request.requested_model {
        if receipt
            .resolved_model
            .as_deref()
            .map_or(true, str::is_empty)
        {
            return Err(BindingProviderError::MissingResolvedModel {
                agent: request.agent.clone(),
                requested: requested.clone(),
            });
        }
    }

    let effective_tools = canonical_tools(&receipt.effective_tools);
    let effective_set: BTreeSet<&str> = effective_tools.iter().map(String::as_str).collect();
    let missing: Vec<String> = request
        .requested_tools
        .iter()
        .filter(|tool| !effective_set.contains(tool.as_str()))
        .cloned()
        .collect();
    if !missing.is_empty() {
        return Err(BindingProviderError::MissingTools {
            agent: request.agent.clone(),
            missing,
        });
    }
    if let Some(requested) = &request.launch_variant {
        if receipt.launch_variant.as_ref() != Some(requested) {
            return Err(BindingProviderError::VariantMismatch {
                agent: request.agent.clone(),
                requested: requested.clone(),
                effective: receipt.launch_variant,
            });
        }
    }

    Ok(BindingAttestation {
        request_digest: request.request_digest.clone(),
        provider_id: receipt.provider_id,
        provider_revision: receipt.provider_revision,
        resolved_model: receipt.resolved_model,
        effective_tools,
        launch_variant: receipt.launch_variant,
        evidence_digest: receipt.evidence_digest,
    })
}

fn canonical_tools(tools: &[String]) -> Vec<String> {
    tools
        .iter()
        .filter(|tool| !tool.is_empty())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blueprint::{
        current_schema_version, resolve_bound_agents, AgentDef, AgentKind, AgentProfile, Blueprint,
        BlueprintMetadata, CompilerHints, CompilerStrategy,
    };
    use mlua_flow_ir::Node as FlowNode;
    use serde_json::json;

    struct ReceiptProvider(Vec<BindReceipt>);

    #[async_trait]
    impl AgentBindingProvider for ReceiptProvider {
        async fn bind(
            &self,
            _requests: &[BindRequest],
        ) -> Result<Vec<BindReceipt>, BindingProviderError> {
            Ok(self.0.clone())
        }
    }

    fn bound() -> Vec<BoundAgent> {
        let mut bp = Blueprint {
            schema_version: current_schema_version(),
            id: "binding-test".into(),
            flow: FlowNode::Seq { children: vec![] },
            agents: vec![],
            operators: vec![],
            metas: vec![],
            hints: CompilerHints::default(),
            strategy: CompilerStrategy::default(),
            metadata: BlueprintMetadata::default(),
            spawner_hints: Default::default(),
            default_agent_kind: AgentKind::Operator,
            default_operator_kind: None,
            default_init_ctx: None,
            default_agent_ctx: None,
            default_context_policy: None,
            projection_placement: None,
            audits: vec![],
            degradation_policy: None,
            runners: vec![],
            default_runner: None,
            check_policy: None,
            blueprint_ref_includes: vec![],
        };
        bp.agents.push(AgentDef {
            name: "coder".to_string(),
            kind: AgentKind::Operator,
            spec: json!({}),
            profile: Some(AgentProfile {
                model: Some("sonnet".to_string()),
                ..Default::default()
            }),
            meta: None,
            runner: Some(Runner::WsClaudeCode {
                variant: "mse-coder".to_string(),
                tools: vec!["Write".to_string(), "Read".to_string()],
            }),
            runner_ref: None,
            verdict: None,
        });
        resolve_bound_agents(&bp).unwrap()
    }

    fn receipt() -> BindReceipt {
        BindReceipt {
            agent: "coder".to_string(),
            request_digest: bound()[0].binding_digest.clone(),
            provider_id: "operator-main-ai".to_string(),
            provider_revision: Some("1".to_string()),
            resolved_model: Some("claude-sonnet-4".to_string()),
            effective_tools: vec!["Write".to_string(), "Read".to_string()],
            launch_variant: Some("mse-coder".to_string()),
            evidence_digest: None,
        }
    }

    #[test]
    fn requests_are_canonical_and_include_declaration_digest() {
        let bound = bound();
        let requests = binding_requests(&bound);
        assert_eq!(requests[0].agent, "coder");
        assert_eq!(requests[0].request_digest, bound[0].binding_digest);
        assert_eq!(requests[0].requested_model.as_deref(), Some("sonnet"));
        assert_eq!(requests[0].requested_tools, ["Read", "Write"]);
        assert_eq!(requests[0].launch_variant.as_deref(), Some("mse-coder"));
    }

    #[tokio::test]
    async fn accepted_receipt_is_attested_and_changes_digest() {
        let mut bound = bound();
        let declaration_digest = bound[0].binding_digest.clone();
        attest_bound_agents(&ReceiptProvider(vec![receipt()]), &mut bound)
            .await
            .unwrap();
        assert_ne!(bound[0].binding_digest, declaration_digest);
        assert_eq!(
            bound[0].attestation.as_ref().unwrap().effective_tools,
            ["Read", "Write"]
        );
    }

    #[tokio::test]
    async fn missing_tool_fails_closed() {
        let mut bound = bound();
        let mut receipt = receipt();
        receipt.effective_tools = vec!["Read".to_string()];
        let error = attest_bound_agents(&ReceiptProvider(vec![receipt]), &mut bound)
            .await
            .unwrap_err();
        assert_eq!(
            error,
            BindingProviderError::MissingTools {
                agent: "coder".to_string(),
                missing: vec!["Write".to_string()],
            }
        );
    }

    #[tokio::test]
    async fn missing_receipt_fails_closed() {
        let mut bound = bound();
        let error = attest_bound_agents(&ReceiptProvider(vec![]), &mut bound)
            .await
            .unwrap_err();
        assert_eq!(
            error,
            BindingProviderError::MissingReceipt {
                agent: "coder".to_string(),
            }
        );
        assert!(bound[0].attestation.is_none());
    }

    #[tokio::test]
    async fn stale_request_digest_fails_closed() {
        let mut bound = bound();
        let mut receipt = receipt();
        receipt.request_digest = crate::blueprint::BindingDigest::sha256("stale");
        let error = attest_bound_agents(&ReceiptProvider(vec![receipt]), &mut bound)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            BindingProviderError::RequestDigestMismatch { .. }
        ));
        assert!(bound[0].attestation.is_none());
    }

    #[tokio::test]
    async fn variant_mismatch_fails_closed() {
        let mut bound = bound();
        let mut receipt = receipt();
        receipt.launch_variant = Some("other".to_string());
        let error = attest_bound_agents(&ReceiptProvider(vec![receipt]), &mut bound)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            BindingProviderError::VariantMismatch { .. }
        ));
    }
}
