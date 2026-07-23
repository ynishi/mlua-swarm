//! Server-side implementation of the platform-neutral agent binding IF.
//!
//! Operator/MainAI manifests are looked up through logical role aliases.
//! The provider returns untrusted receipts; validation and digest ownership
//! remain in `mlua-swarm` Core.

use crate::operator_ws::login::OperatorSessionEntry;
use async_trait::async_trait;
use mlua_swarm::{
    AgentBindingProvider, BindReceipt, BindRequest, BindingBackend, BindingProviderError,
    ManifestBindingProvider, SessionId,
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
    ) -> Result<BindReceipt, BindingProviderError> {
        let target = request.binding_target.as_deref().ok_or_else(|| {
            BindingProviderError::Provider(format!(
                "agent '{}' uses {:?} but declares no logical binding target",
                request.agent, request.backend
            ))
        })?;
        let sid = self
            .roles_to_sid
            .lock()
            .await
            .get(target)
            .cloned()
            .ok_or_else(|| {
                BindingProviderError::Provider(format!(
                    "no Operator session owns binding target '{target}' for agent '{}'",
                    request.agent
                ))
            })?;
        let entry = self
            .operator_sessions
            .lock()
            .await
            .get(&sid)
            .cloned()
            .ok_or_else(|| {
                BindingProviderError::Provider(format!(
                    "Operator session '{sid}' for binding target '{target}' disappeared"
                ))
            })?;
        let manifest = entry.capability_manifest.as_ref().ok_or_else(|| {
            BindingProviderError::Provider(format!(
                "Operator session '{sid}' for binding target '{target}' supplied no capability_manifest"
            ))
        })?;
        ManifestBindingProvider::new(manifest.clone())
            .bind(std::slice::from_ref(request))
            .await?
            .pop()
            .ok_or_else(|| {
                BindingProviderError::Provider(format!(
                    "Operator provider '{}' returned no receipt for agent '{}'",
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
    ) -> Result<Vec<BindReceipt>, BindingProviderError> {
        let mut receipts = Vec::with_capacity(requests.len());
        for request in requests {
            let receipt = match request.backend {
                BindingBackend::WsOperator | BindingBackend::WsClaudeCode => {
                    self.bind_operator(request).await?
                }
                BindingBackend::AgentBlockInProcess => BindReceipt {
                    agent: request.agent.clone(),
                    request_digest: request.request_digest.clone(),
                    provider_id: "mse-agent-block-in-process".to_string(),
                    provider_revision: Some(env!("CARGO_PKG_VERSION").to_string()),
                    resolved_model: request.requested_model.clone(),
                    effective_tools: request.requested_tools.clone(),
                    launch_variant: None,
                    capability_snapshot_digest: None,
                },
            };
            receipts.push(receipt);
        }
        Ok(receipts)
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
        let receipts = provider(Some(manifest))
            .await
            .bind(&[request()])
            .await
            .unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].provider_id, "main-ai-self-report");
        assert_eq!(receipts[0].request_digest, request().request_digest);
        assert_eq!(receipts[0].effective_tools, ["Read", "Write"]);
    }

    #[tokio::test]
    async fn missing_manifest_fails_closed() {
        let error = provider(None).await.bind(&[request()]).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("supplied no capability_manifest"));
    }

    #[tokio::test]
    async fn in_process_backend_is_attested_by_server_registry() {
        let mut request = request();
        request.backend = BindingBackend::AgentBlockInProcess;
        request.binding_target = None;
        request.launch_variant = None;
        let receipts = provider(None).await.bind(&[request.clone()]).await.unwrap();
        assert_eq!(receipts[0].provider_id, "mse-agent-block-in-process");
        assert_eq!(receipts[0].effective_tools, request.requested_tools);
    }
}
