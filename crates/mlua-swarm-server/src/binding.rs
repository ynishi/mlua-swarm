//! Server-side implementation of the platform-neutral agent binding IF.
//!
//! Operator/MainAI manifests are looked up through the launch's own
//! Operator session pin. The provider returns untrusted receipts;
//! validation and digest ownership remain in `mlua-swarm` Core.

use crate::operator_ws::login::LoginSession;
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
    operator_sessions: Arc<Mutex<HashMap<SessionId, Arc<LoginSession>>>>,
    /// Run-scoped pin (`operator_sid` on the launch), as the model's
    /// `OperatorId` — which is now a session id (`S-<hex>`) and nothing
    /// else, since a join no longer claims a name to be addressed by.
    ///
    /// `Some` resolves manifests through the session that id names, for
    /// this launch only. `None` has nothing to resolve through at all —
    /// see [`Self::resolve_sid`], which is where that judgment is made.
    ///
    /// Still held as the raw string rather than a [`SessionId`]: a pin that
    /// does not parse has to be *reported* naming the value the caller
    /// sent, and narrowing the type at the door is what previously made an
    /// unusable pin vanish into an unpinned launch.
    pinned_op: Option<String>,
}

impl OperatorSessionBindingProvider {
    /// Bind the provider to the same session map used by the Operator
    /// REST/WebSocket login flow.
    pub fn new(operator_sessions: Arc<Mutex<HashMap<SessionId, Arc<LoginSession>>>>) -> Self {
        Self {
            operator_sessions,
            pinned_op: None,
        }
    }

    /// Resolve the session this request attests through: the launch pin,
    /// and only the launch pin.
    ///
    /// # What an unpinned launch attests through: nothing
    ///
    /// This used to fall back to "the session currently holding the
    /// request's `binding_target` role", which worked because a join
    /// claimed role names and `roles_to_sid` mapped one to a session. Role
    /// declaration has moved onto the Run, so that map is gone and the
    /// question is what an unpinned bind resolves against instead. The
    /// answer is that it cannot resolve against anything, and the reason is
    /// worth stating rather than working out again later:
    ///
    /// - `binding_target` is `AgentDef.spec.operator_ref` — a **seat**, a
    ///   position in the Blueprint's `operators[]`. It names a lane of the
    ///   flow, never a session, so it cannot be turned into one.
    /// - The seat's *holder* could be looked up (`Run.current[slot]`), but
    ///   that is a per-Run fact and this provider is per-launch: it is
    ///   built before the Run exists, holds no `RunId` and no `RunStore`,
    ///   and a seat's holder changes under it at every handover (**A8**),
    ///   so a receipt stamped from it would attest to whoever happened to
    ///   hold the lane at bind time.
    /// - An unpinned launch carries no operator identity of any kind (see
    ///   `tasks::seat_declared_operators`), so there is no third candidate.
    ///
    /// So an unpinned bind reports `Unbound` naming the target, and the
    /// only way to attest a manifest is to pin the launch to the session
    /// whose manifest it is. That is the honest shape: attestation is a
    /// claim about one specific execution environment, and a pin is the
    /// only thing that names one. `strict_binding` launches must therefore
    /// carry `operator_sid` — which `mse mcp` already sends on every launch
    /// it makes (explicitly, or auto-pinned from its sole live session).
    ///
    /// A pin that resolves to nothing is likewise reported (`Unbound`,
    /// naming the pin) rather than dropped. It stays a report rather than
    /// an error because the launch's hard failure line is elsewhere — the
    /// compiler's pinned spawner lookup rejects that same id outright.
    async fn resolve_sid(&self, target: &OperatorRef) -> Result<SessionId, String> {
        let Some(pin) = &self.pinned_op else {
            return Err(format!(
                "binding target '{target}' names a Blueprint Operator seat, not a session, and \
                 this launch carries no operator_sid to attest through"
            ));
        };
        SessionId::parse(pin.clone())
            .map_err(|_| format!("run-scoped pin '{pin}' is not an Operator session id"))
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
        // (a) no usable pin, (b) session gone, (c) no capability_manifest:
        // the execution environment simply has nothing to attest yet. These
        // are `Unbound` (observed, not fatal) — the non-strict launch runs
        // DeclarationOnly and `strict_binding` decides whether they fail.
        // (b) would fail again at real spawn-time routing anyway, so the
        // binding stage does not pre-gate it.
        let sid = match self.resolve_sid(target).await {
            Ok(sid) => sid,
            Err(reason) => {
                return Ok(BindOutcome::Unbound {
                    agent: request.agent.clone(),
                    reason,
                });
            }
        };
        // How this run reached that sid. Only one way remains, but the
        // phrase is kept in the `Unbound` reasons below so a driver reading
        // a degradation entry sees which pin went missing and which seat
        // the agent declared, rather than a bare sid.
        let via = format!("run-scoped pin (declared binding target '{target}')");
        let Some(live) = self.operator_sessions.lock().await.get(&sid).cloned() else {
            return Ok(BindOutcome::Unbound {
                agent: request.agent.clone(),
                reason: format!("Operator session '{sid}' for {via} disappeared"),
            });
        };
        let Some(manifest) = live.record().capability_manifest.as_ref() else {
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
    /// session map.
    ///
    /// Always `Some`: deciding here that some ids are not worth pinning is
    /// what silently reverted a launch to the unpinned provider, which now
    /// attests nothing at all. Whether the id names a live session is
    /// [`Self::resolve_sid`]'s question, and an unusable one is reported
    /// there instead of disappearing here.
    fn pinned_to_session(&self, session_id: &str) -> Option<Arc<dyn AgentBindingProvider>> {
        Some(Arc::new(Self {
            operator_sessions: self.operator_sessions.clone(),
            pinned_op: Some(session_id.to_string()),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mlua_swarm::store::operator_session::OperatorSessionRecord;
    use mlua_swarm::{AgentProviderCapability, AgentProviderManifest, BindingDigest};

    /// The Blueprint Operator seat the agent under test dispatches through.
    /// A lane of the flow, not a job title and not a session.
    const SEAT: &str = "phase-a-op";

    fn seat(name: &str) -> OperatorRef {
        OperatorRef::new(name).expect("test seat literal is never empty")
    }

    fn request() -> BindRequest {
        BindRequest {
            agent: "coder".to_string(),
            request_digest: BindingDigest::sha256("request"),
            backend: BindingBackend::WsOperator,
            binding_target: Some(seat(SEAT)),
            requested_model: Some("sonnet".to_string()),
            requested_tools: vec!["Read".to_string()],
            launch_variant: Some("mse-coder".to_string()),
        }
    }

    /// One live session carrying `manifest`, and a provider over it.
    /// Returns the sid, because pinning to it is now the only way a bind
    /// reaches a manifest at all.
    async fn provider(
        manifest: Option<AgentProviderManifest>,
    ) -> (OperatorSessionBindingProvider, SessionId) {
        let sid = SessionId::new();
        let live = LoginSession::new(
            OperatorSessionRecord {
                sid: sid.clone(),
                token_digest: OperatorSessionRecord::digest_of("token"),
                capability_manifest: manifest,
                joined_at_secs: 0,
                desc: None,
                observed: Vec::new(),
                observed_total: 0,
            },
            None,
        );
        let sessions = Arc::new(Mutex::new(HashMap::from([(sid.clone(), live)])));
        (OperatorSessionBindingProvider::new(sessions), sid)
    }

    /// The provider a pinned launch actually binds through.
    async fn pinned_provider(
        manifest: Option<AgentProviderManifest>,
    ) -> Arc<dyn AgentBindingProvider> {
        let (provider, sid) = provider(manifest).await;
        provider
            .pinned_to_session(sid.as_str())
            .expect("a live sid must yield a pinned provider")
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
        let outcomes = pinned_provider(Some(manifest))
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
        let outcomes = pinned_provider(None)
            .await
            .bind(&[request()])
            .await
            .unwrap();
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

    /// **The judgment an unpinned bind rests on.** `binding_target` names a
    /// Blueprint seat, and a seat is not a session — with role declaration
    /// moved onto the Run there is nothing left for an unpinned launch to
    /// attest through, so it reports `Unbound` and says which of the two
    /// halves it was missing. It must not quietly pick a session.
    #[tokio::test]
    async fn an_unpinned_bind_attests_through_nothing_and_says_so() {
        // The one live session even carries a matching manifest, so the
        // only thing making this `Unbound` is the refusal to guess.
        let manifest = AgentProviderManifest {
            provider_id: "some-live-session".to_string(),
            provider_revision: None,
            capabilities: vec![AgentProviderCapability {
                launch_variant: Some("mse-coder".to_string()),
                resolved_model: None,
                effective_tools: vec!["Read".to_string()],
                capability_snapshot_digest: None,
            }],
        };
        let (provider, _sid) = provider(Some(manifest)).await;
        let outcomes = provider.bind(&[request()]).await.unwrap();
        match &outcomes[0] {
            BindOutcome::Unbound { agent, reason } => {
                assert_eq!(agent, "coder");
                assert!(
                    reason.contains(SEAT) && reason.contains("operator_sid"),
                    "the reason must name the seat that could not be resolved and the pin \
                     that would have resolved it: {reason}"
                );
            }
            BindOutcome::Bound { receipt } => panic!(
                "an unpinned launch must not attest through whichever session happens to be \
                 live (got provider_id {})",
                receipt.provider_id
            ),
        }
    }

    /// Two live sessions, one of them carrying the manifest: the pinned
    /// provider must attest through the pin and not through the other
    /// session. This is the strict_binding path staying `Bound` under
    /// run-scoped pinning.
    #[tokio::test]
    async fn pinned_provider_attests_through_the_pin_not_another_live_session() {
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
        let other_sid = SessionId::new();
        let pinned_sid = SessionId::new();
        let other = LoginSession::new(
            OperatorSessionRecord {
                sid: other_sid.clone(),
                token_digest: OperatorSessionRecord::digest_of("token"),
                capability_manifest: None,
                joined_at_secs: 0,
                desc: None,
                observed: Vec::new(),
                observed_total: 0,
            },
            None,
        );
        let pinned = LoginSession::new(
            OperatorSessionRecord {
                sid: pinned_sid.clone(),
                token_digest: OperatorSessionRecord::digest_of("token"),
                capability_manifest: Some(manifest),
                joined_at_secs: 0,
                desc: None,
                observed: Vec::new(),
                observed_total: 0,
            },
            None,
        );
        let sessions = Arc::new(Mutex::new(HashMap::from([
            (other_sid.clone(), other),
            (pinned_sid.clone(), pinned),
        ])));
        let provider = OperatorSessionBindingProvider::new(sessions);

        // Pinned to the other session, there is nothing to attest.
        let outcomes = provider
            .pinned_to_session(other_sid.as_str())
            .expect("a live sid must yield a pinned provider")
            .bind(&[request()])
            .await
            .unwrap();
        assert!(
            matches!(&outcomes[0], BindOutcome::Unbound { .. }),
            "the other session supplied no manifest, so a bind pinned to it is Unbound"
        );

        // Pinned to the right one, its manifest resolves the receipt. The
        // agent keeps declaring the same seat throughout.
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
    /// the pin is what went missing.
    #[tokio::test]
    async fn pin_to_a_dead_session_reports_unbound_naming_the_pin() {
        let (provider, _sid) = provider(None).await;
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

    /// A pin that is not a session id is reported, naming it. Nothing else
    /// is a pinnable `OperatorId` any more: a role alias used to share the
    /// key space and no longer exists, so a pin that does not parse is a
    /// caller error rather than a name to look up somewhere else.
    #[tokio::test]
    async fn a_pin_that_is_not_a_session_id_reports_unbound_naming_it() {
        let manifest = AgentProviderManifest {
            provider_id: "the-live-session".to_string(),
            provider_revision: None,
            capabilities: vec![AgentProviderCapability {
                launch_variant: Some("mse-coder".to_string()),
                resolved_model: None,
                effective_tools: vec!["Read".to_string()],
                capability_snapshot_digest: None,
            }],
        };
        let (provider, sid) = provider(Some(manifest)).await;
        assert!(
            matches!(
                &provider
                    .pinned_to_session(sid.as_str())
                    .expect("pinned provider")
                    .bind(&[request()])
                    .await
                    .unwrap()[0],
                BindOutcome::Bound { .. }
            ),
            "precondition: pinned to the live sid, this request binds"
        );

        let pinned = provider
            .pinned_to_session("main-ai")
            .expect("every pin yields a pinned provider");
        match &pinned.bind(&[request()]).await.unwrap()[0] {
            BindOutcome::Unbound { reason, .. } => {
                assert!(
                    reason.contains("main-ai"),
                    "reason must name the pin: {reason}"
                );
            }
            BindOutcome::Bound { receipt } => panic!(
                "an unusable pin must not silently attest through some other live session \
                 (got provider_id {})",
                receipt.provider_id
            ),
        }
    }

    #[tokio::test]
    async fn in_process_backend_is_attested_by_server_registry() {
        let mut request = request();
        request.backend = BindingBackend::AgentBlockInProcess;
        request.binding_target = None;
        request.launch_variant = None;
        let outcomes = provider(None)
            .await
            .0
            .bind(&[request.clone()])
            .await
            .unwrap();
        let receipt = expect_bound(&outcomes[0]);
        assert_eq!(receipt.provider_id, "mse-agent-block-in-process");
        assert_eq!(receipt.effective_tools, request.requested_tools);
    }
}
