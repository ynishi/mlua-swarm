//! Platform-neutral execution binding boundary.
//!
//! Swarm owns request construction, requested/effective validation, and
//! immutable Run snapshots. Execution environments implement
//! [`AgentBindingProvider`] and report what they can actually enforce; an
//! official platform adapter is one implementation of this same interface.

use crate::blueprint::{
    AgentProviderManifest, BindOutcome, BindReceipt, BindRequest, BindingAttestation,
    BindingBackend, BoundAgent, Runner,
};
use async_trait::async_trait;
use serde::Serialize;
use std::collections::{BTreeSet, HashMap, HashSet};
use thiserror::Error;

/// Migration policy for the deprecated `AgentProfile.worker_binding` Runner
/// fallback. It applies only to fresh declaration resolution; persisted
/// snapshots keep their pinned Runner and remain readable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyWorkerBindingPolicy {
    /// Preserve pre-Runner Blueprint compatibility and mark the snapshot with
    /// `runner_source = legacy_worker_binding`.
    #[default]
    Allow,
    /// Reject the fallback and require an explicit `runner` or `runner_ref`.
    Reject,
}

/// Execution-environment provider for effective agent capabilities.
#[async_trait]
pub trait AgentBindingProvider: Send + Sync {
    /// Resolve all requested bindings as one launch-time transaction.
    ///
    /// The provider returns exactly one [`BindOutcome`] per requested agent
    /// — `Bound` with an (untrusted) receipt Core still validates, or
    /// `Unbound` when the execution environment currently offers no matching
    /// capability. Returning fewer, extra, or duplicate outcomes is rejected
    /// by Core in [`attest_bound_agents`]. Whether an `Unbound` outcome fails
    /// the launch is the caller's `strict` decision, not the provider's.
    async fn bind(
        &self,
        requests: &[BindRequest],
    ) -> Result<Vec<BindOutcome>, BindingProviderError>;
}

/// Reference provider backed by an execution-environment capability manifest.
///
/// Claude Code, Codex, and other official plugins can inspect their own
/// platform state, construct [`AgentProviderManifest`], and delegate the common
/// request-to-receipt mapping here. Core still validates every returned receipt
/// through [`attest_bound_agents`].
#[derive(Debug, Clone)]
pub struct ManifestBindingProvider {
    manifest: AgentProviderManifest,
}

impl ManifestBindingProvider {
    /// Wrap one provider-owned capability manifest.
    pub fn new(manifest: AgentProviderManifest) -> Self {
        Self { manifest }
    }

    /// Borrow the provider-owned manifest used for resolution.
    pub fn manifest(&self) -> &AgentProviderManifest {
        &self.manifest
    }

    fn outcome_for(&self, request: &BindRequest) -> Result<BindOutcome, BindingProviderError> {
        if request.backend == BindingBackend::AgentBlockInProcess {
            return Err(BindingProviderError::Provider(format!(
                "manifest provider '{}' cannot bind in-process agent '{}'",
                self.manifest.provider_id, request.agent
            )));
        }
        let mut matches = self
            .manifest
            .capabilities
            .iter()
            .filter(|capability| capability.launch_variant == request.launch_variant);
        let Some(capability) = matches.next() else {
            // No capability for the requested variant is an attestation gap,
            // not a provider fault: report `Unbound` and let the caller's
            // `strict` decision (in `attest_bound_agents`) choose whether the
            // launch fails.
            return Ok(BindOutcome::Unbound {
                agent: request.agent.clone(),
                reason: format!(
                    "manifest provider '{}' has no capability for launch variant {:?}",
                    self.manifest.provider_id, request.launch_variant
                ),
            });
        };
        if matches.next().is_some() {
            // A manifest that declares the same variant twice is ambiguous:
            // this is a provider configuration bug, so it stays fail-closed
            // in every mode.
            return Err(BindingProviderError::Provider(format!(
                "manifest provider '{}' declares duplicate capabilities for launch variant {:?}",
                self.manifest.provider_id, request.launch_variant
            )));
        }
        Ok(BindOutcome::Bound {
            receipt: BindReceipt {
                agent: request.agent.clone(),
                request_digest: request.request_digest.clone(),
                provider_id: self.manifest.provider_id.clone(),
                provider_revision: self.manifest.provider_revision.clone(),
                resolved_model: capability.resolved_model.clone(),
                effective_tools: capability.effective_tools.clone(),
                launch_variant: capability.launch_variant.clone(),
                capability_snapshot_digest: capability.capability_snapshot_digest.clone(),
            },
        })
    }
}

#[async_trait]
impl AgentBindingProvider for ManifestBindingProvider {
    async fn bind(
        &self,
        requests: &[BindRequest],
    ) -> Result<Vec<BindOutcome>, BindingProviderError> {
        requests
            .iter()
            .map(|request| self.outcome_for(request))
            .collect()
    }
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
    /// `strict_binding` is set but the provider left a Runner-backed agent
    /// `Unbound`. The message lists what an execution environment would have
    /// to attest (launch variant, requested tools, requested model) so an
    /// Operator can generate a satisfying capability manifest.
    #[error(
        "strict_binding requires an attestation for agent '{agent}' \
         (requested launch variant {variant:?}, tools {tools:?}, model {model:?}) \
         but the provider returned Unbound: {reason}"
    )]
    AttestationRequired {
        /// Logical agent name.
        agent: String,
        /// Provider-reported reason the agent could not be bound.
        reason: String,
        /// Requested launch variant from the resolved `Runner`.
        variant: Option<String>,
        /// Requested minimum tool grant from the resolved `Runner`.
        tools: Vec<String>,
        /// Requested model alias or tier from `AgentProfile.model`.
        model: Option<String>,
    },
}

/// One Runner-backed agent the provider could not attest, returned by
/// [`attest_bound_agents`] in non-strict mode. Purely observational: the
/// `reason` never enters the [`BoundAgent`] snapshot or its digest lineage —
/// the agent stays `DeclarationOnly` and callers record the gap out of band
/// (tracing warn + `RunRecord.degradations`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnboundAgent {
    /// Logical agent name that was left unattested.
    pub agent: String,
    /// Provider-reported reason the agent could not be bound.
    pub reason: String,
}

/// Build platform-neutral requests for every Runner-bound agent.
pub fn binding_requests(bound_agents: &[BoundAgent]) -> Vec<BindRequest> {
    bound_agents
        .iter()
        .filter_map(binding_request_for_snapshot)
        .collect()
}

/// Reconstruct the platform-neutral request pinned by one immutable snapshot.
///
/// Before attestation, the snapshot's `binding_digest` is the declaration
/// request digest. After attestation changes the final replay identity, the
/// original request digest remains pinned inside the accepted attestation.
/// This makes the helper safe for both launch-time provider calls and
/// after-the-fact Run explain surfaces.
pub fn binding_request_for_snapshot(bound: &BoundAgent) -> Option<BindRequest> {
    let runner = bound.runner.as_ref()?;
    let (backend, requested_tools, launch_variant) = match runner {
        Runner::WsOperator { variant, tools } => (
            BindingBackend::WsOperator,
            canonical_tools(tools),
            Some(variant.clone()),
        ),
        Runner::WsClaudeCode { variant, tools } => (
            BindingBackend::WsClaudeCode,
            canonical_tools(tools),
            Some(variant.clone()),
        ),
        Runner::AgentBlockInProcess { tools } => (
            BindingBackend::AgentBlockInProcess,
            canonical_tools(tools),
            None,
        ),
    };
    Some(BindRequest {
        agent: bound.agent.name.clone(),
        request_digest: bound.attestation.as_ref().map_or_else(
            || bound.binding_digest.clone(),
            |attestation| attestation.request_digest.clone(),
        ),
        backend,
        binding_target: bound
            .agent
            .spec
            .get("operator_ref")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        requested_model: bound
            .agent
            .profile
            .as_ref()
            .and_then(|profile| profile.model.clone()),
        requested_tools,
        launch_variant,
    })
}

#[derive(Serialize)]
struct LegacyEvidenceAttestation<'a> {
    request_digest: &'a crate::blueprint::BindingDigest,
    provider_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_revision: &'a Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resolved_model: &'a Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    effective_tools: &'a Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    launch_variant: &'a Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    evidence_digest: &'a Option<crate::blueprint::BindingDigest>,
}

#[derive(Serialize)]
struct LegacyEvidenceBoundAgentDigestInput<'a> {
    agent: &'a crate::blueprint::AgentDef,
    runner: &'a Option<Runner>,
    context_policy: &'a Option<crate::core::agent_context::ContextPolicy>,
    runner_source: crate::blueprint::RunnerResolutionSource,
    attestation: Option<LegacyEvidenceAttestation<'a>>,
}

fn legacy_evidence_binding_digest(
    bound: &BoundAgent,
) -> Result<crate::blueprint::BindingDigest, BindingProviderError> {
    let attestation = bound
        .attestation
        .as_ref()
        .map(|attestation| LegacyEvidenceAttestation {
            request_digest: &attestation.request_digest,
            provider_id: &attestation.provider_id,
            provider_revision: &attestation.provider_revision,
            resolved_model: &attestation.resolved_model,
            effective_tools: &attestation.effective_tools,
            launch_variant: &attestation.launch_variant,
            evidence_digest: &attestation.capability_snapshot_digest,
        });
    let bytes = serde_json::to_vec(&LegacyEvidenceBoundAgentDigestInput {
        agent: &bound.agent,
        runner: &bound.runner,
        context_policy: &bound.context_policy,
        runner_source: bound.runner_source,
        attestation,
    })
    .map_err(|error| BindingProviderError::Digest(error.to_string()))?;
    Ok(crate::blueprint::BindingDigest::sha256(bytes))
}

/// Validate one persisted [`BoundAgent`] before it is reused or explained.
///
/// The digest is recomputed from the snapshot body, then an attested snapshot
/// is checked again against its declaration-only request. This detects store
/// corruption and schema-inconsistent mutations without consulting a Provider,
/// the current Blueprint, or any mutable execution-environment registry.
pub fn validate_bound_agent_snapshot(bound: &BoundAgent) -> Result<(), BindingProviderError> {
    let mut expected = bound.clone();
    expected
        .recompute_binding_digest()
        .map_err(|error| BindingProviderError::Digest(error.to_string()))?;
    let legacy_digest = legacy_evidence_binding_digest(bound)?;
    if expected.binding_digest != bound.binding_digest && legacy_digest != bound.binding_digest {
        return Err(BindingProviderError::Digest(format!(
            "stored BoundAgent '{}' has binding digest '{}', recomputed '{}'",
            bound.agent.name, bound.binding_digest, expected.binding_digest
        )));
    }

    let Some(attestation) = bound.attestation.as_ref() else {
        return Ok(());
    };

    let mut declaration = bound.clone();
    declaration.attestation = None;
    declaration
        .recompute_binding_digest()
        .map_err(|error| BindingProviderError::Digest(error.to_string()))?;
    let request = binding_request_for_snapshot(&declaration).ok_or_else(|| {
        BindingProviderError::Provider(format!(
            "stored BoundAgent '{}' has an attestation but no Runner declaration",
            bound.agent.name
        ))
    })?;
    let validated = validate_receipt(
        &request,
        BindReceipt {
            agent: bound.agent.name.clone(),
            request_digest: attestation.request_digest.clone(),
            provider_id: attestation.provider_id.clone(),
            provider_revision: attestation.provider_revision.clone(),
            resolved_model: attestation.resolved_model.clone(),
            effective_tools: attestation.effective_tools.clone(),
            launch_variant: attestation.launch_variant.clone(),
            capability_snapshot_digest: attestation.capability_snapshot_digest.clone(),
        },
    )?;
    if &validated != attestation {
        return Err(BindingProviderError::Provider(format!(
            "stored BoundAgent '{}' contains a non-canonical attestation",
            bound.agent.name
        )));
    }
    Ok(())
}

/// Validate a complete persisted binding snapshot without partially accepting
/// any entry.
pub fn validate_bound_agent_snapshots(
    bound_agents: &[BoundAgent],
) -> Result<(), BindingProviderError> {
    for bound in bound_agents {
        validate_bound_agent_snapshot(bound)?;
    }
    Ok(())
}

/// Ask `provider` to bind all Runner-backed agents, validate every returned
/// receipt, and pin accepted attestations into the snapshots.
///
/// `strict` decides how an `Unbound` outcome is treated (never how a `Bound`
/// one is validated — "attestation is optional, but never wrong"):
///
/// - A `Bound` outcome is always validated through [`validate_receipt`]; a
///   receipt that is present but contradicts the request (missing tools,
///   variant mismatch, stale digest, empty resolved model) is an error in
///   BOTH modes.
/// - An `Unbound` outcome fails the call with
///   [`BindingProviderError::AttestationRequired`] when `strict`, or is
///   collected into the returned [`UnboundAgent`] list when not — the agent
///   stays `DeclarationOnly`.
///
/// Per-agent outcome completeness (exactly one outcome per requested agent,
/// no missing / duplicate / unexpected entries) stays fail-closed in both
/// modes.
pub async fn attest_bound_agents(
    provider: &dyn AgentBindingProvider,
    bound_agents: &mut [BoundAgent],
    strict: bool,
) -> Result<Vec<UnboundAgent>, BindingProviderError> {
    let requests = binding_requests(bound_agents);
    if requests.is_empty() {
        return Ok(Vec::new());
    }

    let outcomes = provider.bind(&requests).await?;
    let requested_names: HashSet<&str> = requests.iter().map(|r| r.agent.as_str()).collect();
    let mut by_agent = HashMap::with_capacity(outcomes.len());
    for outcome in outcomes {
        let agent = match &outcome {
            BindOutcome::Bound { receipt } => receipt.agent.clone(),
            BindOutcome::Unbound { agent, .. } => agent.clone(),
        };
        if !requested_names.contains(agent.as_str()) {
            return Err(BindingProviderError::UnexpectedReceipt { agent });
        }
        if by_agent.insert(agent.clone(), outcome).is_some() {
            return Err(BindingProviderError::DuplicateReceipt { agent });
        }
    }

    let mut accepted = Vec::with_capacity(requests.len());
    let mut unbound = Vec::new();
    for request in requests {
        let outcome = by_agent.remove(&request.agent).ok_or_else(|| {
            BindingProviderError::MissingReceipt {
                agent: request.agent.clone(),
            }
        })?;
        match outcome {
            BindOutcome::Bound { receipt } => {
                let attestation = validate_receipt(&request, receipt)?;
                accepted.push((request.agent, attestation));
            }
            BindOutcome::Unbound { reason, .. } => {
                if strict {
                    return Err(BindingProviderError::AttestationRequired {
                        agent: request.agent,
                        reason,
                        variant: request.launch_variant,
                        tools: request.requested_tools,
                        model: request.requested_model,
                    });
                }
                unbound.push(UnboundAgent {
                    agent: request.agent,
                    reason,
                });
            }
        }
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
    Ok(unbound)
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
        capability_snapshot_digest: receipt.capability_snapshot_digest,
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
        ) -> Result<Vec<BindOutcome>, BindingProviderError> {
            Ok(self
                .0
                .iter()
                .cloned()
                .map(|receipt| BindOutcome::Bound { receipt })
                .collect())
        }
    }

    /// Provider that reports every request as `Unbound` with a fixed reason —
    /// exercises the `strict` gate in [`attest_bound_agents`].
    struct UnboundProvider(&'static str);

    #[async_trait]
    impl AgentBindingProvider for UnboundProvider {
        async fn bind(
            &self,
            requests: &[BindRequest],
        ) -> Result<Vec<BindOutcome>, BindingProviderError> {
            Ok(requests
                .iter()
                .map(|request| BindOutcome::Unbound {
                    agent: request.agent.clone(),
                    reason: self.0.to_string(),
                })
                .collect())
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
            spec: json!({ "operator_ref": "main-ai" }),
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
            capability_snapshot_digest: None,
        }
    }

    #[test]
    fn requests_are_canonical_and_include_declaration_digest() {
        let bound = bound();
        let requests = binding_requests(&bound);
        assert_eq!(requests[0].agent, "coder");
        assert_eq!(requests[0].request_digest, bound[0].binding_digest);
        assert_eq!(requests[0].backend, BindingBackend::WsClaudeCode);
        assert_eq!(requests[0].binding_target.as_deref(), Some("main-ai"));
        assert_eq!(requests[0].requested_model.as_deref(), Some("sonnet"));
        assert_eq!(requests[0].requested_tools, ["Read", "Write"]);
        assert_eq!(requests[0].launch_variant.as_deref(), Some("mse-coder"));
    }

    #[tokio::test]
    async fn accepted_receipt_is_attested_and_changes_digest() {
        let mut bound = bound();
        let declaration_digest = bound[0].binding_digest.clone();
        let unbound = attest_bound_agents(&ReceiptProvider(vec![receipt()]), &mut bound, false)
            .await
            .unwrap();
        assert!(unbound.is_empty());
        assert_ne!(bound[0].binding_digest, declaration_digest);
        validate_bound_agent_snapshot(&bound[0]).unwrap();
        assert_eq!(
            bound[0].attestation.as_ref().unwrap().effective_tools,
            ["Read", "Write"]
        );
    }

    #[test]
    fn persisted_snapshot_rejects_binding_digest_drift() {
        let mut bound = bound().remove(0);
        bound.agent.profile.as_mut().unwrap().system_prompt = "mutated after persistence".into();

        let error = validate_bound_agent_snapshot(&bound).unwrap_err();
        assert!(matches!(error, BindingProviderError::Digest(_)));
    }

    #[tokio::test]
    async fn persisted_snapshot_rejects_attestation_for_a_different_declaration() {
        let mut bound = bound();
        attest_bound_agents(&ReceiptProvider(vec![receipt()]), &mut bound, false)
            .await
            .unwrap();
        bound[0].attestation.as_mut().unwrap().request_digest =
            crate::blueprint::BindingDigest::sha256("other-declaration");
        bound[0].recompute_binding_digest().unwrap();

        let error = validate_bound_agent_snapshot(&bound[0]).unwrap_err();
        assert!(matches!(
            error,
            BindingProviderError::RequestDigestMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn persisted_snapshot_accepts_the_legacy_evidence_digest_identity() {
        let mut receipt = receipt();
        receipt.capability_snapshot_digest = Some(crate::blueprint::BindingDigest::sha256(
            "legacy-capabilities",
        ));
        let mut bound = bound();
        attest_bound_agents(&ReceiptProvider(vec![receipt]), &mut bound, false)
            .await
            .unwrap();
        let new_digest = bound[0].binding_digest.clone();
        let legacy_digest = legacy_evidence_binding_digest(&bound[0]).unwrap();
        assert_ne!(legacy_digest, new_digest);

        bound[0].binding_digest = legacy_digest;
        validate_bound_agent_snapshot(&bound[0]).unwrap();
    }

    #[tokio::test]
    async fn missing_tool_fails_closed() {
        let mut bound = bound();
        let mut receipt = receipt();
        receipt.effective_tools = vec!["Read".to_string()];
        // A receipt that IS present but contradicts the request fails in
        // non-strict mode too — "attestation is optional, but never wrong".
        let error = attest_bound_agents(&ReceiptProvider(vec![receipt]), &mut bound, false)
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
        let error = attest_bound_agents(&ReceiptProvider(vec![]), &mut bound, false)
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
        let error = attest_bound_agents(&ReceiptProvider(vec![receipt]), &mut bound, false)
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
        let error = attest_bound_agents(&ReceiptProvider(vec![receipt]), &mut bound, false)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            BindingProviderError::VariantMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn unbound_outcome_is_collected_in_non_strict_mode() {
        let mut bound = bound();
        let unbound = attest_bound_agents(
            &UnboundProvider("no capability for launch variant"),
            &mut bound,
            false,
        )
        .await
        .expect("non-strict must not fail on Unbound");
        assert_eq!(unbound.len(), 1);
        assert_eq!(unbound[0].agent, "coder");
        assert_eq!(unbound[0].reason, "no capability for launch variant");
        // The agent stays DeclarationOnly — no attestation is pinned.
        assert!(bound[0].attestation.is_none());
    }

    #[tokio::test]
    async fn unbound_outcome_fails_closed_in_strict_mode_with_requirements() {
        let mut bound = bound();
        let error = attest_bound_agents(
            &UnboundProvider("role main-ai has not joined"),
            &mut bound,
            true,
        )
        .await
        .unwrap_err();
        match &error {
            BindingProviderError::AttestationRequired {
                agent,
                variant,
                tools,
                ..
            } => {
                assert_eq!(agent, "coder");
                assert_eq!(variant.as_deref(), Some("mse-coder"));
                assert_eq!(tools, &["Read".to_string(), "Write".to_string()]);
            }
            other => panic!("expected AttestationRequired, got {other:?}"),
        }
        // The message must name the agent and the requested variant/tools so
        // an Operator can generate a satisfying manifest.
        let message = error.to_string();
        assert!(message.contains("coder"), "message: {message}");
        assert!(message.contains("mse-coder"), "message: {message}");
        assert!(message.contains("Read"), "message: {message}");
        assert!(bound[0].attestation.is_none());
    }
}
