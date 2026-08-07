//! Server-side implementation of the platform-neutral agent binding IF.
//!
//! Operator/MainAI manifests are looked up through logical role aliases.
//! The provider returns untrusted receipts; validation and digest ownership
//! remain in `mlua-swarm` Core.

use crate::operator_ws::login::OperatorSessionEntry;
use async_trait::async_trait;
use mlua_swarm::{
    AgentBindingProvider, BindOutcome, BindReceipt, BindRequest, BindingBackend,
    BindingProviderError, ManifestBindingProvider, OperatorRef, SessionId,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Binding provider backed by live Operator login records.
pub struct OperatorSessionBindingProvider {
    operator_sessions: Arc<Mutex<HashMap<SessionId, Arc<OperatorSessionEntry>>>>,
    roles_to_sid: Arc<Mutex<HashMap<OperatorRef, SessionId>>>,
    /// Run-scoped session pin (`operator_sid` on the launch). `Some`
    /// resolves manifests through this session and never consults
    /// `roles_to_sid`; `None` is the process-global role lookup every
    /// unpinned launch keeps using.
    pinned_sid: Option<SessionId>,
}

impl OperatorSessionBindingProvider {
    /// Bind the provider to the same session and role maps used by the
    /// Operator REST/WebSocket login flow.
    pub fn new(
        operator_sessions: Arc<Mutex<HashMap<SessionId, Arc<OperatorSessionEntry>>>>,
        roles_to_sid: Arc<Mutex<HashMap<OperatorRef, SessionId>>>,
    ) -> Self {
        Self {
            operator_sessions,
            roles_to_sid,
            pinned_sid: None,
        }
    }

    /// Resolve the session this request attests through: the launch pin when
    /// there is one, otherwise the session currently holding the request's
    /// logical `target` role.
    ///
    /// The pinned arm reports `Unbound` (not an error) when the sid names no
    /// live session — same tier as an unjoined role, and the launch's own
    /// fail-loud line is the compiler's pinned spawner lookup, which rejects
    /// the same condition outright.
    async fn resolve_sid(&self, target: &OperatorRef) -> Result<SessionId, String> {
        match &self.pinned_sid {
            Some(sid) => Ok(sid.clone()),
            None => self
                .roles_to_sid
                .lock()
                .await
                .get(target.as_str())
                .cloned()
                .ok_or_else(|| format!("no Operator session owns binding target '{target}'")),
        }
    }

    async fn bind_operator(
        &self,
        request: &BindRequest,
    ) -> Result<BindOutcome, BindingProviderError> {
        // A WS-backed agent with no logical binding target is a Blueprint
        // declaration error, not a transient capability gap — keep it
        // fail-closed rather than reporting `Unbound`.
        let target = request.binding_target.as_ref().ok_or_else(|| {
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
        let sid = match self.resolve_sid(target).await {
            Ok(sid) => sid,
            Err(reason) => {
                return Ok(BindOutcome::Unbound {
                    agent: request.agent.clone(),
                    reason,
                });
            }
        };
        // How this run reached that sid — a launch pin or the role map.
        // Named in every `Unbound` reason below so a driver reading a
        // degradation entry can tell "my pinned session is gone" from "the
        // role has no holder".
        let via = match &self.pinned_sid {
            Some(_) => format!("run-scoped pin (declared binding target '{target}')"),
            None => format!("binding target '{target}'"),
        };
        let Some(entry) = self.operator_sessions.lock().await.get(&sid).cloned() else {
            return Ok(BindOutcome::Unbound {
                agent: request.agent.clone(),
                reason: format!("Operator session '{sid}' for {via} disappeared"),
            });
        };
        let Some(manifest) = entry.capability_manifest.as_ref() else {
            return Ok(BindOutcome::Unbound {
                agent: request.agent.clone(),
                reason: format!(
                    "Operator session '{sid}' for {via} supplied no capability_manifest"
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

    /// Launch-scoped clone pinned to `session_id`, sharing the same live
    /// session / role maps. An id that does not parse as a `SessionId`
    /// cannot name a live session, so it yields `None` and the launch keeps
    /// the unpinned provider — the pin's fail-loud line is the compiler's
    /// pinned spawner lookup, which rejects that same id there.
    fn pinned_to_session(&self, session_id: &str) -> Option<Arc<dyn AgentBindingProvider>> {
        let sid = SessionId::parse(session_id.to_string()).ok()?;
        Some(Arc::new(Self {
            operator_sessions: self.operator_sessions.clone(),
            roles_to_sid: self.roles_to_sid.clone(),
            pinned_sid: Some(sid),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mlua_swarm::store::operator_session::OperatorSessionRecord;
    use mlua_swarm::{AgentProviderCapability, AgentProviderManifest, BindingDigest};

    /// convention-token-ok: mlua-swarm public operator role literal.
    fn role(name: &str) -> OperatorRef {
        OperatorRef::new(name).expect("test role literal is never empty")
    }

    fn request() -> BindRequest {
        BindRequest {
            agent: "coder".to_string(),
            request_digest: BindingDigest::sha256("request"),
            backend: BindingBackend::WsOperator,
            binding_target: Some(role("main-ai")),
            requested_model: Some("sonnet".to_string()),
            requested_tools: vec!["Read".to_string()],
            launch_variant: Some("mse-coder".to_string()),
        }
    }

    async fn provider(manifest: Option<AgentProviderManifest>) -> OperatorSessionBindingProvider {
        let sid = SessionId::new();
        let entry = Arc::new(OperatorSessionEntry {
            sid: sid.clone(),
            token_digest: OperatorSessionRecord::digest_of("token"),
            roles: vec![role("main-ai")],
            capability_manifest: manifest,
            joined_at_secs: 0,
            ws_session: Mutex::new(None),
        });
        let sessions = Arc::new(Mutex::new(HashMap::from([(sid.clone(), entry)])));
        let roles = Arc::new(Mutex::new(HashMap::from([(role("main-ai"), sid)])));
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

    /// Two sessions, one role: the role is held by a session with no
    /// manifest (another driver's, as far as this launch is concerned) while
    /// the pinned session carries the manifest. The pinned provider must
    /// attest through the pin — this is the strict_binding path staying
    /// `Bound` under run-scoped pinning.
    #[tokio::test]
    async fn pinned_provider_attests_through_the_pin_not_the_role_holder() {
        let manifest = AgentProviderManifest {
            provider_id: "pinned-session".to_string(),
            provider_revision: None,
            capabilities: vec![AgentProviderCapability {
                launch_variant: Some("mse-coder".to_string()),
                resolved_model: Some("claude-sonnet-4".to_string()),
                effective_tools: vec!["Read".to_string()],
                capability_snapshot_digest: None,
            }],
        };
        let role_holder_sid = SessionId::new();
        let pinned_sid = SessionId::new();
        let role_holder = Arc::new(OperatorSessionEntry {
            sid: role_holder_sid.clone(),
            token_digest: OperatorSessionRecord::digest_of("token"),
            roles: vec![role("main-ai")],
            capability_manifest: None,
            joined_at_secs: 0,
            ws_session: Mutex::new(None),
        });
        let pinned = Arc::new(OperatorSessionEntry {
            sid: pinned_sid.clone(),
            token_digest: OperatorSessionRecord::digest_of("token"),
            roles: Vec::new(),
            capability_manifest: Some(manifest),
            joined_at_secs: 0,
            ws_session: Mutex::new(None),
        });
        let sessions = Arc::new(Mutex::new(HashMap::from([
            (role_holder_sid.clone(), role_holder),
            (pinned_sid.clone(), pinned),
        ])));
        let roles = Arc::new(Mutex::new(HashMap::from([(
            role("main-ai"),
            role_holder_sid,
        )])));
        let provider = OperatorSessionBindingProvider::new(sessions, roles);

        // Unpinned, the role's holder answers — and it has nothing to attest.
        let outcomes = provider.bind(&[request()]).await.unwrap();
        assert!(
            matches!(&outcomes[0], BindOutcome::Unbound { .. }),
            "the role's holder supplied no manifest, so the unpinned bind is Unbound"
        );

        // Pinned, the pinned session's manifest resolves the receipt. The
        // agent keeps declaring the same logical role throughout.
        let pinned_provider = provider
            .pinned_to_session(pinned_sid.as_str())
            .expect("a live sid must yield a pinned provider");
        let outcomes = pinned_provider.bind(&[request()]).await.unwrap();
        let receipt = expect_bound(&outcomes[0]);
        assert_eq!(receipt.provider_id, "pinned-session");
        assert_eq!(receipt.effective_tools, ["Read"]);
    }

    /// A pin naming no live session reports `Unbound` (the launch's loud
    /// failure is the compiler's pinned spawner lookup), and the reason says
    /// the pin — not the role — is what went missing.
    #[tokio::test]
    async fn pin_to_a_dead_session_reports_unbound_naming_the_pin() {
        let provider = provider(None).await;
        let gone = SessionId::new();
        let pinned = provider
            .pinned_to_session(gone.as_str())
            .expect("pinned provider");
        let outcomes = pinned.bind(&[request()]).await.unwrap();
        match &outcomes[0] {
            BindOutcome::Unbound { reason, .. } => {
                assert!(
                    reason.contains("run-scoped pin"),
                    "reason must attribute the gap to the pin: {reason}"
                );
                assert!(
                    reason.contains(gone.as_str()),
                    "reason must name the pinned sid: {reason}"
                );
            }
            BindOutcome::Bound { .. } => panic!("a pin to a dead session cannot be Bound"),
        }
    }

    /// An id that is not a `SessionId` at all cannot name a live session:
    /// no pinned provider is produced, so the launch keeps the unpinned one
    /// and fails loudly at the compiler instead.
    #[test]
    fn unparseable_pin_yields_no_pinned_provider() {
        let sessions = Arc::new(Mutex::new(HashMap::new()));
        let roles = Arc::new(Mutex::new(HashMap::new()));
        let provider = OperatorSessionBindingProvider::new(sessions, roles);
        assert!(provider.pinned_to_session("not-a-session-id").is_none());
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
