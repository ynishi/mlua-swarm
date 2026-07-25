//! GH #81 Layer 2 integration coverage: the new operator-recovery
//! surfaces that close the pre-#81 gap where a stale session could only
//! be cleared by a full server restart.
//!
//! Three surfaces exercised against a live `axum::serve` instance:
//!
//! 1. `POST /v1/operators` with a role already held returns 409, and the
//!    body carries the additive `conflicts_detail: [{role, sid}]` array
//!    identifying the holding session (Layer 2 (a)).
//! 2. `GET /v1/operators` enumerates every live session's
//!    `{sid, roles, joined_at_secs, connected}` without requiring a
//!    Bearer (Layer 2 (b)).
//! 3. `DELETE /v1/operators/by-role/:role` releases the stale role
//!    holder without knowing the sid or its Bearer, and a subsequent
//!    `POST /v1/operators` with the same role succeeds (Layer 2 (c)).

use mlua_swarm::core::config::EngineCfg;
use mlua_swarm::core::engine::Engine;
use serde_json::json;
use tokio::task::JoinHandle;

struct ServerHandle {
    base_url: String,
    task: JoinHandle<()>,
}

impl ServerHandle {
    fn shutdown(self) {
        self.task.abort();
    }
}

async fn spawn_server() -> ServerHandle {
    let engine = Engine::new_with_layers(
        EngineCfg::default(),
        mlua_swarm_server::default_layer_registry(),
    );
    let router = mlua_swarm_server::build_router_full(
        engine,
        mlua_swarm_server::default_registry(),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        300,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let task = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    ServerHandle {
        base_url: format!("http://{addr}"),
        task,
    }
}

async fn mint(client: &reqwest::Client, base_url: &str, role: &str) -> serde_json::Value {
    // convention-token-ok: role names are mlua-swarm public operator role literals.
    let resp = client
        .post(format!("{base_url}/v1/operators"))
        .json(&json!({ "roles": [role] }))
        .send()
        .await
        .expect("mint request");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "POST /v1/operators must mint successfully with a free role"
    );
    resp.json().await.expect("mint json")
}

#[tokio::test]
async fn conflict_body_names_the_holding_session_id() {
    let server = spawn_server().await;
    let client = reqwest::Client::new();

    // convention-token-ok: mlua-swarm public operator role literal.
    let holder = mint(&client, &server.base_url, "main-ai").await;
    let holder_sid = holder["sid"].as_str().expect("holder sid").to_string();

    // Second mint with the same role → 409 with conflicts_detail carrying
    // the holder sid (GH #81 Layer 2 (a)).
    let conflict = client
        .post(format!("{}/v1/operators", server.base_url))
        // convention-token-ok: mlua-swarm public operator role literal.
        .json(&json!({ "roles": ["main-ai"] }))
        .send()
        .await
        .expect("conflict request");
    assert_eq!(conflict.status(), reqwest::StatusCode::CONFLICT);
    let body: serde_json::Value = conflict.json().await.expect("conflict json");
    assert_eq!(body["error"], "roles conflict");
    // Pre-#81 wire shape preserved.
    assert_eq!(body["conflicts"], serde_json::json!(["main-ai"]));
    // New Layer 2 (a) field.
    let detail = body["conflicts_detail"]
        .as_array()
        .expect("conflicts_detail must be an array");
    assert_eq!(detail.len(), 1);
    assert_eq!(detail[0]["role"], "main-ai");
    assert_eq!(detail[0]["sid"], holder_sid);

    server.shutdown();
}

#[tokio::test]
async fn list_route_enumerates_live_sessions_without_bearer() {
    let server = spawn_server().await;
    let client = reqwest::Client::new();

    // convention-token-ok: mlua-swarm public operator role literals.
    let a = mint(&client, &server.base_url, "main-ai").await;
    let b = mint(&client, &server.base_url, "auditor").await;
    let sid_a = a["sid"].as_str().unwrap().to_string();
    let sid_b = b["sid"].as_str().unwrap().to_string();

    let list = client
        .get(format!("{}/v1/operators", server.base_url))
        .send()
        .await
        .expect("list request");
    assert_eq!(list.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = list.json().await.expect("list json");
    let ops = body["operators"]
        .as_array()
        .expect("operators must be an array");
    assert_eq!(ops.len(), 2);
    let sids: Vec<&str> = ops.iter().map(|e| e["sid"].as_str().unwrap()).collect();
    assert!(sids.contains(&sid_a.as_str()));
    assert!(sids.contains(&sid_b.as_str()));
    // Every entry must expose the identity fields the guide names.
    for entry in ops {
        assert!(entry["roles"].is_array());
        assert!(entry["joined_at_secs"].as_u64().is_some());
        assert!(entry["connected"].as_bool().is_some());
        // Bearer secrets must never surface on the list route.
        assert!(entry.get("token").is_none());
    }

    server.shutdown();
}

#[tokio::test]
async fn by_role_delete_releases_stale_session_and_role_reopens() {
    let server = spawn_server().await;
    let client = reqwest::Client::new();

    // A pretend-crashed driver: mint, then discard the sid/token (simulating
    // a driver that lost its local state).
    // convention-token-ok: mlua-swarm public operator role literal.
    let _stale = mint(&client, &server.base_url, "main-ai").await;

    let release = client
        .delete(format!("{}/v1/operators/by-role/main-ai", server.base_url))
        .send()
        .await
        .expect("by-role delete request");
    assert_eq!(
        release.status(),
        reqwest::StatusCode::NO_CONTENT,
        "DELETE /v1/operators/by-role/:role must return 204 on successful teardown"
    );

    // The role is now open — a fresh mint succeeds with a different sid.
    // convention-token-ok: mlua-swarm public operator role literal.
    let remint = mint(&client, &server.base_url, "main-ai").await;
    assert!(remint["sid"].as_str().is_some());

    server.shutdown();
}

#[tokio::test]
async fn by_role_delete_returns_404_when_no_session_holds_the_role() {
    let server = spawn_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .delete(format!(
            "{}/v1/operators/by-role/no-such-role",
            server.base_url
        ))
        .send()
        .await
        .expect("by-role delete request");
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    let body: serde_json::Value = resp.json().await.expect("404 body json");
    assert_eq!(body["error"], "no session holds this role");
    assert_eq!(body["role"], "no-such-role");

    server.shutdown();
}
