//! Server-side implementation of the platform-neutral agent binding IF.
//!
//! Operator/MainAI manifests are looked up through logical role aliases.
//! The provider returns untrusted receipts; validation and digest ownership
//! remain in `mlua-swarm` Core.

use crate::operator_ws::login::OperatorSessionEntry;
use async_trait::async_trait;
use mlua_swarm::{
    AgentBindingProvider, BindOutcome, BindReceipt, BindRequest, BindingBackend,
    BindingProviderError, ManifestBindingProvider, SessionId,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Binding provider backed by live Operator login records.
pub struct OperatorSessionBindingProvider {
    operator_sessions: Arc<Mutex<HashMap<SessionId, Arc<OperatorSessionEntry>>>>,
    roles_to_sid: Arc<Mutex<HashMap<String, SessionId>>>,
}

impl OperatorSessionBindingProvider {
    /// Bind the provider to the same session and role maps used by the
    /// Operator REST/WebSocket login flow.
    pub fn new(
        operator_sessions: Arc<Mutex<HashMap<SessionId, Arc<OperatorSessionEntry>>>>,
        roles_to_sid: Arc<Mutex<HashMap<String, SessionId>>>,
    ) -> Self {
        Self {
            operator_sessions,
            roles_to_sid,
        }
    }

    async fn bind_operator(
        &self,
        request: &BindRequest,
    ) -> Result<BindOutcome, BindingProviderError> {
        // A WS-backed agent with no logical binding target is a Blueprint
        // declaration error, not a transient capability gap — keep it
        // fail-closed rather than reporting `Unbound`.
        let target = request.binding_target.as_deref().ok_or_else(|| {
            BindingProviderError::Provider(format!(
                "agent '{}' uses {:?} but declares no logical binding target",
                request.agent, request.backend
            ))
        })?;
        // (a) role not joined, (b) session gone, (c) no capability_manifest:
        // the execution environment simply has nothing to attest yet. These
        // are `Unbound` (observed, not fatal) — the non-strict launch runs
        // DeclarationOnly and `strict_binding` decides whether they fail.
        // (a)/(b) would fail again at real spawn-time routing anyway, so the
        // binding stage does not pre-gate them.
        let Some(sid) = self.roles_to_sid.lock().await.get(target).cloned() else {
            return Ok(BindOutcome::Unbound {
                agent: request.agent.clone(),
                reason: format!("no Operator session owns binding target '{target}'"),
            });
        };
        let Some(entry) = self.operator_sessions.lock().await.get(&sid).cloned() else {
            return Ok(BindOutcome::Unbound {
                agent: request.agent.clone(),
                reason: format!(
                    "Operator session '{sid}' for binding target '{target}' disappeared"
                ),
            });
        };
        let Some(manifest) = entry.capability_manifest.as_ref() else {
            return Ok(BindOutcome::Unbound {
                agent: request.agent.clone(),
                reason: format!(
                    "Operator session '{sid}' for binding target '{target}' supplied no capability_manifest"
                ),
            });
        };
        // (d) manifest lacks the requested variant surfaces as `Unbound` from
        // the delegated `ManifestBindingProvider`; a duplicate variant stays
        // an error there. Either way the single outcome is passed straight
        // through.
        ManifestBindingProvider::new(manifest.clone())
            .bind(std::slice::from_ref(request))
            .await?
            .pop()
            .ok_or_else(|| {
                BindingProviderError::Provider(format!(
                    "Operator provider '{}' returned no outcome for agent '{}'",
                    manifest.provider_id, request.agent
                ))
            })
    }
}

#[async_trait]
impl AgentBindingProvider for OperatorSessionBindingProvider {
    async fn bind(
        &self,
        requests: &[BindRequest],
    ) -> Result<Vec<BindOutcome>, BindingProviderError> {
        let mut outcomes = Vec::with_capacity(requests.len());
        for request in requests {
            let outcome = match request.backend {
                BindingBackend::WsOperator | BindingBackend::WsClaudeCode => {
                    self.bind_operator(request).await?
                }
                // In-process AgentBlock still echoes a receipt (Core
                // validates it); the registry-backed real attest is a future
                // follow-up.
                BindingBackend::AgentBlockInProcess => BindOutcome::Bound {
                    receipt: BindReceipt {
                        agent: request.agent.clone(),
                        request_digest: request.request_digest.clone(),
                        provider_id: "mse-agent-block-in-process".to_string(),
                        provider_revision: Some(env!("CARGO_PKG_VERSION").to_string()),
                        resolved_model: request.requested_model.clone(),
                        effective_tools: request.requested_tools.clone(),
                        launch_variant: None,
                        capability_snapshot_digest: None,
                    },
                },
            };
            outcomes.push(outcome);
        }
        Ok(outcomes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mlua_swarm::{AgentProviderCapability, AgentProviderManifest, BindingDigest};

    fn request() -> BindRequest {
        BindRequest {
            agent: "coder".to_string(),
            request_digest: BindingDigest::sha256("request"),
            backend: BindingBackend::WsOperator,
            binding_target: Some("main-ai".to_string()),
            requested_model: Some("sonnet".to_string()),
            requested_tools: vec!["Read".to_string()],
            launch_variant: Some("mse-coder".to_string()),
        }
    }

    async fn provider(manifest: Option<AgentProviderManifest>) -> OperatorSessionBindingProvider {
        let sid = SessionId::new();
        let entry = Arc::new(OperatorSessionEntry {
            sid: sid.clone(),
            token: "token".to_string(),
            roles: vec!["main-ai".to_string()],
            capability_manifest: manifest,
            ws_session: Mutex::new(None),
        });
        let sessions = Arc::new(Mutex::new(HashMap::from([(sid.clone(), entry)])));
        let roles = Arc::new(Mutex::new(HashMap::from([("main-ai".to_string(), sid)])));
        OperatorSessionBindingProvider::new(sessions, roles)
    }

    fn expect_bound(outcome: &BindOutcome) -> &mlua_swarm::BindReceipt {
        match outcome {
            BindOutcome::Bound { receipt } => receipt,
            BindOutcome::Unbound { agent, reason } => {
                panic!("expected Bound, got Unbound({agent}): {reason}")
            }
        }
    }

    #[tokio::test]
    async fn operator_manifest_resolves_to_untrusted_receipt() {
        let manifest = AgentProviderManifest {
            provider_id: "main-ai-self-report".to_string(),
            provider_revision: Some("1".to_string()),
            capabilities: vec![AgentProviderCapability {
                launch_variant: Some("mse-coder".to_string()),
                resolved_model: Some("claude-sonnet-4".to_string()),
                effective_tools: vec!["Read".to_string(), "Write".to_string()],
                capability_snapshot_digest: Some(BindingDigest::sha256("manifest")),
            }],
        };
        let outcomes = provider(Some(manifest))
            .await
            .bind(&[request()])
            .await
            .unwrap();
        assert_eq!(outcomes.len(), 1);
        let receipt = expect_bound(&outcomes[0]);
        assert_eq!(receipt.provider_id, "main-ai-self-report");
        assert_eq!(receipt.request_digest, request().request_digest);
        assert_eq!(receipt.effective_tools, ["Read", "Write"]);
    }

    #[tokio::test]
    async fn missing_manifest_reports_unbound() {
        let outcomes = provider(None).await.bind(&[request()]).await.unwrap();
        assert_eq!(outcomes.len(), 1);
        match &outcomes[0] {
            BindOutcome::Unbound { agent, reason } => {
                assert_eq!(agent, "coder");
                assert!(
                    reason.contains("supplied no capability_manifest"),
                    "reason: {reason}"
                );
            }
            BindOutcome::Bound { .. } => panic!("expected Unbound when no manifest was submitted"),
        }
    }

    #[tokio::test]
    async fn missing_role_reports_unbound() {
        // A provider whose role maps are empty: the requested binding target
        // has not joined, so the agent is Unbound (not a hard error).
        let sessions = Arc::new(Mutex::new(HashMap::new()));
        let roles = Arc::new(Mutex::new(HashMap::new()));
        let provider = OperatorSessionBindingProvider::new(sessions, roles);
        let outcomes = provider.bind(&[request()]).await.unwrap();
        match &outcomes[0] {
            BindOutcome::Unbound { agent, reason } => {
                assert_eq!(agent, "coder");
                assert!(
                    reason.contains("no Operator session owns"),
                    "reason: {reason}"
                );
            }
            BindOutcome::Bound { .. } => panic!("expected Unbound when the role has not joined"),
        }
    }

    #[tokio::test]
    async fn in_process_backend_is_attested_by_server_registry() {
        let mut request = request();
        request.backend = BindingBackend::AgentBlockInProcess;
        request.binding_target = None;
        request.launch_variant = None;
        let outcomes = provider(None).await.bind(&[request.clone()]).await.unwrap();
        let receipt = expect_bound(&outcomes[0]);
        assert_eq!(receipt.provider_id, "mse-agent-block-in-process");
        assert_eq!(receipt.effective_tools, request.requested_tools);
    }
}
