//! Cross-platform AgentProvider contract: the same logical Agent and result
//! contract bind through Claude Code and Codex without platform data entering
//! the Blueprint.

use mlua_swarm::blueprint::resolve_bound_agents;
use mlua_swarm::{
    attest_bound_agents, binding_requests, AgentProviderCapability, AgentProviderManifest,
    BindingDigest, Blueprint, ManifestBindingProvider,
};

fn shared_blueprint() -> Blueprint {
    serde_json::from_value(serde_json::json!({
        "schema_version": "0.1.0",
        "id": "cross-platform-provider",
        "flow": {"kind": "seq", "children": []},
        "agents": [{
            "name": "reviewer",
            "kind": "operator",
            "spec": {"operator_ref": "main-ai"},
            "profile": {
                "system_prompt": "Review the change against the declared contract.",
                "model": "reasoning-medium"
            },
            "runner": {
                "backend": "ws_operator",
                "variant": "mse-reviewer",
                "tools": ["Read", "Grep"]
            },
            "verdict": {
                "channel": "part",
                "values": ["PASS", "BLOCKED"]
            }
        }]
    }))
    .expect("shared Blueprint must deserialize")
}

fn provider(
    provider_id: &str,
    revision: &str,
    resolved_model: &str,
    effective_tools: &[&str],
) -> ManifestBindingProvider {
    ManifestBindingProvider::new(AgentProviderManifest {
        provider_id: provider_id.to_string(),
        provider_revision: Some(revision.to_string()),
        capabilities: vec![AgentProviderCapability {
            launch_variant: Some("mse-reviewer".to_string()),
            resolved_model: Some(resolved_model.to_string()),
            effective_tools: effective_tools
                .iter()
                .map(|tool| tool.to_string())
                .collect(),
            evidence_digest: Some(BindingDigest::sha256(format!("{provider_id}:{revision}"))),
        }],
    })
}

#[tokio::test]
async fn claude_code_and_codex_bind_the_same_agent_contract_through_one_if() {
    let blueprint = shared_blueprint();
    let mut claude_bound = resolve_bound_agents(&blueprint).expect("resolve Claude snapshot");
    let mut codex_bound = resolve_bound_agents(&blueprint).expect("resolve Codex snapshot");

    let claude_request = binding_requests(&claude_bound);
    let codex_request = binding_requests(&codex_bound);
    assert_eq!(claude_request, codex_request);

    attest_bound_agents(
        &provider(
            "mse-provider-claude-code",
            "claude-plugin-1",
            "claude-sonnet-4",
            &["Grep", "Read", "Write"],
        ),
        &mut claude_bound,
    )
    .await
    .expect("Claude Code provider must satisfy the shared request");
    attest_bound_agents(
        &provider(
            "mse-provider-codex",
            "codex-plugin-1",
            "gpt-5-codex",
            &["Grep", "Read", "Shell"],
        ),
        &mut codex_bound,
    )
    .await
    .expect("Codex provider must satisfy the shared request");

    let claude = &claude_bound[0];
    let codex = &codex_bound[0];
    assert_eq!(claude.agent, codex.agent);
    assert_eq!(claude.agent.verdict, codex.agent.verdict);
    assert_eq!(
        claude.attestation.as_ref().unwrap().request_digest,
        codex.attestation.as_ref().unwrap().request_digest
    );
    assert_eq!(
        claude.attestation.as_ref().unwrap().provider_id,
        "mse-provider-claude-code"
    );
    assert_eq!(
        codex.attestation.as_ref().unwrap().provider_id,
        "mse-provider-codex"
    );
    assert_ne!(claude.binding_digest, codex.binding_digest);
}

#[tokio::test]
async fn either_platform_still_fails_closed_when_a_shared_tool_is_missing() {
    let blueprint = shared_blueprint();
    let mut bound = resolve_bound_agents(&blueprint).expect("resolve shared snapshot");
    let error = attest_bound_agents(
        &provider(
            "mse-provider-codex",
            "codex-plugin-1",
            "gpt-5-codex",
            &["Read"],
        ),
        &mut bound,
    )
    .await
    .expect_err("missing Grep must be rejected by common Core validation");
    assert!(error.to_string().contains("missing requested tools"));
    assert!(bound[0].attestation.is_none());
}
