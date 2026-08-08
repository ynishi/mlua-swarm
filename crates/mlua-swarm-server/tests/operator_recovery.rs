//! GH #81 Layer 2 integration coverage: the new operator-recovery
//! surfaces that close the pre-#81 gap where a stale session could only
//! be cleared by a full server restart.
//!
//! Three surfaces exercised against a live `axum::serve` instance:
//!
//! 1. `POST /v1/operators` with a role already held returns 409, and the
//!    body carries the additive `conflicts_detail: [{role, sid}]` array
//!    identifying the holding session (Layer 2 (a)).
//! 2. `GET /v1/operators` enumerates every live session's identity and
//!    its 記名 (model §4.2), and is **Bearer-gated** (**D3**) — a
//!    breaking change from the Layer 2 (b) shape, which answered
//!    anonymously. Any live session's token opens it (**W5**).
//! 3. `DELETE /v1/operators/by-role/:role` releases the stale role
//!    holder without knowing the sid or its Bearer, and a subsequent
//!    `POST /v1/operators` with the same role succeeds (Layer 2 (c)).
//!
//! Plus the two teardown-lifecycle regressions that live on the same
//! `teardown_operator_session` path:
//!
//! 4. Teardown closes the holder's WebSocket (a session torn down out from
//!    under a third party must not leave that client parked on a socket
//!    nothing will ever speak on again).
//! 5. A persisted-row delete failure aborts the teardown instead of being
//!    swallowed, so the in-memory maps and the store can never disagree.

use futures_util::StreamExt;
use mlua_swarm::blueprint::{
    current_schema_version, AgentDef, AgentKind, Blueprint, BlueprintMetadata, CompilerHints,
    CompilerStrategy, OperatorDef,
};
use mlua_swarm::core::config::EngineCfg;
use mlua_swarm::core::engine::Engine;
use mlua_swarm::store::operator_session::{
    InMemoryOperatorSessionStore, OperatorSessionRecord, OperatorSessionStore,
    OperatorSessionStoreError,
};
use mlua_swarm::SessionId;
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

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
    mint_with_desc(client, base_url, role, None).await
}

/// Mint carrying the 記名's confirmed part (**D1**). `None` exercises the
/// join that writes none — which the server accepts, and the list reports
/// as `desc: null`.
async fn mint_with_desc(
    client: &reqwest::Client,
    base_url: &str,
    role: &str,
    desc: Option<&str>,
) -> serde_json::Value {
    // convention-token-ok: role names are mlua-swarm public operator role literals.
    let resp = client
        .post(format!("{base_url}/v1/operators"))
        .json(&json!({ "roles": [role], "desc": desc }))
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

/// The 記名 list, and the breaking change that came with it: this route
/// used to answer anyone (GH #81 Layer 2 (b), `GET /v1/status` trust
/// tier). **D3** gates it, and any live session's token opens it.
#[tokio::test]
async fn list_route_enumerates_live_sessions_with_any_live_bearer() {
    let server = spawn_server().await;
    let client = reqwest::Client::new();

    // convention-token-ok: mlua-swarm public operator role literals.
    let a = mint_with_desc(
        &client,
        &server.base_url,
        "main-ai",
        Some("  rewriting the seat resolver in mlua-swarm-server  "),
    )
    .await;
    let b = mint(&client, &server.base_url, "auditor").await;
    let sid_a = a["sid"].as_str().unwrap().to_string();
    let sid_b = b["sid"].as_str().unwrap().to_string();
    let token_b = b["token"].as_str().unwrap().to_string();

    // D3: no Bearer is a 401 now, where it used to be the whole answer.
    let anonymous = client
        .get(format!("{}/v1/operators", server.base_url))
        .send()
        .await
        .expect("anonymous list request");
    assert_eq!(
        anonymous.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "D3: the 記名 list is Bearer-gated"
    );

    // A token no live session answers to is a 401 as well.
    let wrong = client
        .get(format!("{}/v1/operators", server.base_url))
        .bearer_auth("not-a-minted-token")
        .send()
        .await
        .expect("bad-token list request");
    assert_eq!(wrong.status(), reqwest::StatusCode::UNAUTHORIZED);

    // W5: the reader is any Assignee — B's token reads A's entry.
    let list = client
        .get(format!("{}/v1/operators", server.base_url))
        .bearer_auth(&token_b)
        .send()
        .await
        .expect("list request");
    assert_eq!(list.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = list.json().await.expect("list json");
    let ops = body["operators"]
        .as_array()
        .expect("operators must be an array");
    assert_eq!(ops.len(), 2);
    assert_eq!(body["total"], 2);
    assert!(
        body["limit"].as_u64().is_some(),
        "D5: the list reports the count limit it applied"
    );
    let sids: Vec<&str> = ops.iter().map(|e| e["sid"].as_str().unwrap()).collect();
    assert!(sids.contains(&sid_a.as_str()));
    assert!(sids.contains(&sid_b.as_str()));
    // Every entry must expose the identity fields the guide names.
    for entry in ops {
        assert!(entry["roles"].is_array());
        assert!(entry["joined_at_secs"].as_u64().is_some());
        assert!(entry["connected"].as_bool().is_some());
        assert!(entry["last_activity_secs"].as_u64().is_some());
        assert!(entry["observed"].is_array());
        assert_eq!(entry["observed_total"], 0, "nothing assigned yet");
        // D1's absence is a value, not a missing key.
        assert!(
            entry.get("desc").is_some(),
            "the desc key is always present: {entry}"
        );
        // Bearer secrets must never surface on the list route.
        assert!(entry.get("token").is_none());
    }

    let entry_a = ops
        .iter()
        .find(|e| e["sid"] == sid_a.as_str())
        .expect("A's entry");
    assert_eq!(
        entry_a["desc"], "rewriting the seat resolver in mlua-swarm-server",
        "D1: the join-time description is kept, trimmed"
    );
    let entry_b = ops
        .iter()
        .find(|e| e["sid"] == sid_b.as_str())
        .expect("B's entry");
    assert!(
        entry_b["desc"].is_null(),
        "a session that wrote nothing reports null rather than dropping the key"
    );

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

// ─── teardown closes the holder's WebSocket ─────────────────────────────────

/// Open an operator WS with the session's Bearer, exactly as a driver would.
async fn connect_ws(
    base_url: &str,
    sid: &str,
    token: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let ws_url = format!(
        "{}/v1/operators/{sid}/ws",
        base_url.replace("http://", "ws://")
    );
    let mut request = ws_url.into_client_request().expect("ws request");
    request.headers_mut().insert(
        "authorization",
        HeaderValue::from_str(&format!("Bearer {token}")).expect("bearer header"),
    );
    let (socket, _response) = tokio_tungstenite::connect_async(request)
        .await
        .expect("ws connect");
    socket
}

/// Block until the server reports the socket as attached. The upgrade
/// completing on the client says nothing about the server-side handler
/// having bound the session yet, and this test is about what teardown does
/// to an *attached* socket.
async fn wait_until_connected(client: &reqwest::Client, base_url: &str, sid: &str, token: &str) {
    for _ in 0..200 {
        let info: serde_json::Value = client
            .get(format!("{base_url}/v1/operators/{sid}"))
            .bearer_auth(token)
            .send()
            .await
            .expect("info request")
            .json()
            .await
            .expect("info json");
        if info["connected"] == json!(true) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the WS never became `connected` on the server side");
}

/// A session torn down by a third party (`by-role`, no Bearer needed) must
/// have its socket **closed**, not merely unregistered.
///
/// Before this fix teardown dropped the session's own sender and removed
/// the `operator_sessions` entry, but `handle_operator_socket`'s local
/// sender kept the mpsc channel — and therefore the write task and the
/// socket — alive. The client sat on a live WebSocket that no frame would
/// ever arrive on again, with no error to notice: the session it belonged
/// to was already gone.
#[tokio::test]
async fn by_role_delete_closes_the_holders_socket() {
    let server = spawn_server().await;
    let client = reqwest::Client::new();

    // convention-token-ok: mlua-swarm public operator role literal.
    let holder = mint(&client, &server.base_url, "main-ai").await;
    let sid = holder["sid"].as_str().expect("sid").to_string();
    let token = holder["token"].as_str().expect("token").to_string();

    let mut socket = connect_ws(&server.base_url, &sid, &token).await;
    wait_until_connected(&client, &server.base_url, &sid, &token).await;

    let release = client
        // convention-token-ok: mlua-swarm public operator role literal.
        .delete(format!("{}/v1/operators/by-role/main-ai", server.base_url))
        .send()
        .await
        .expect("by-role delete request");
    assert_eq!(release.status(), reqwest::StatusCode::NO_CONTENT);

    let next = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .expect(
            "the torn-down session's socket must be closed by the server; \
             it stayed open instead (nothing was ever delivered on it again)",
        );
    match next {
        Some(Ok(Message::Close(frame))) => {
            let reason = frame.map(|f| f.reason.to_string()).unwrap_or_default();
            assert!(
                reason.contains("torn down"),
                "the Close frame should say why the session went away, got: {reason:?}"
            );
        }
        other => panic!("expected a WS Close frame after teardown, got: {other:?}"),
    }

    server.shutdown();
}

// ─── a persisted-row delete failure aborts the teardown ─────────────────────

/// What [`FaultyDeleteStore::delete`] does on the next call.
#[derive(Clone, Copy, PartialEq, Debug)]
enum DeleteMode {
    /// Delegate to the wrapped in-memory store (the honest path).
    Pass,
    /// A backend failure — store outage, disk error. The case whose
    /// swallowing left the in-memory maps and the store disagreeing.
    Fail,
    /// The row is already gone (a concurrent delete won). Idempotent, and
    /// therefore still a *success*.
    NotFound,
}

/// In-memory session store with an injectable `delete` failure. `put` /
/// `list` are always honest, so boot-time restore behaves normally and only
/// the teardown write is under test.
struct FaultyDeleteStore {
    inner: InMemoryOperatorSessionStore,
    mode: Mutex<DeleteMode>,
}

impl FaultyDeleteStore {
    fn new() -> Self {
        Self {
            inner: InMemoryOperatorSessionStore::new(),
            mode: Mutex::new(DeleteMode::Pass),
        }
    }

    fn set_mode(&self, mode: DeleteMode) {
        *self.mode.lock().expect("delete mode lock") = mode;
    }
}

#[async_trait::async_trait]
impl OperatorSessionStore for FaultyDeleteStore {
    fn name(&self) -> &str {
        "faulty-delete"
    }

    async fn put(&self, record: OperatorSessionRecord) -> Result<(), OperatorSessionStoreError> {
        self.inner.put(record).await
    }

    async fn delete(&self, sid: &SessionId) -> Result<(), OperatorSessionStoreError> {
        let mode = *self.mode.lock().expect("delete mode lock");
        match mode {
            DeleteMode::Pass => self.inner.delete(sid).await,
            DeleteMode::Fail => Err(OperatorSessionStoreError::Other(
                "injected persisted-row delete failure".to_string(),
            )),
            DeleteMode::NotFound => Err(OperatorSessionStoreError::NotFound(sid.clone())),
        }
    }

    async fn list(&self) -> Result<Vec<OperatorSessionRecord>, OperatorSessionStoreError> {
        self.inner.list().await
    }
}

/// Bearer for the seeded session below. Not a secret: it exists only inside
/// this test binary, and what the store holds is its digest.
const SEEDED_BEARER: &str = "operator-recovery-test-bearer"; // allowlist-secret: test-local literal.

/// Boot a server from `store`, restoring whatever it already holds — the
/// path that puts a session into the engine registries with no WS connect
/// anywhere. The returned `Engine` is the same handle the router got, so a
/// test can watch registry membership directly.
async fn spawn_server_with_session_store(
    store: Arc<dyn OperatorSessionStore>,
) -> (ServerHandle, Engine) {
    let engine = Engine::new_with_layers(
        EngineCfg::default(),
        mlua_swarm_server::default_layer_registry(),
    );
    let persistence =
        mlua_swarm_server::OperatorSessionPersistence::restore(store, &engine, None, None)
            .await
            .expect("operator session restore");
    let router = mlua_swarm_server::build_router_full_with_operator_session_persistence(
        engine.clone(),
        mlua_swarm_server::default_registry(),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(persistence),
        300,
        mlua_swarm::LegacyWorkerBindingPolicy::Allow,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let task = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (
        ServerHandle {
            base_url: format!("http://{addr}"),
            task,
        },
        engine,
    )
}

/// Seed one persisted session for [`spawn_server_with_session_store`] to
/// restore, authenticated by [`SEEDED_BEARER`].
async fn seed_persisted_session(store: &FaultyDeleteStore, role: &str) -> SessionId {
    let sid = SessionId::new();
    store
        .put(OperatorSessionRecord {
            sid: sid.clone(),
            token_digest: OperatorSessionRecord::digest_of(SEEDED_BEARER),
            roles: vec![
                mlua_swarm::OperatorRef::new(role).expect("test role literal is never empty")
            ],
            capability_manifest: None,
            joined_at_secs: 0,
            desc: None,
            observed: Vec::new(),
            observed_total: 0,
        })
        .await
        .expect("seed the persisted row");
    sid
}

/// When the persisted row cannot be dropped, the teardown must not happen
/// at all.
///
/// Previously the store failure was logged and swallowed *after* the
/// in-memory teardown had already run: the caller got its `204`, the
/// session vanished from the engine, and the row stayed on disk — so the
/// next boot restored a session the operator had deliberately released.
/// Dropping the row first makes that state unreachable: either both go, or
/// neither does.
#[tokio::test]
async fn persisted_row_delete_failure_aborts_the_teardown() {
    let store = Arc::new(FaultyDeleteStore::new());
    // convention-token-ok: mlua-swarm public operator role literal.
    let sid = seed_persisted_session(&store, "main-ai").await;

    let (server, engine) = spawn_server_with_session_store(store.clone()).await;
    let client = reqwest::Client::new();
    assert!(
        engine.list_operator_ids().await.contains(&sid.to_string()),
        "precondition: restoring the persisted row registers the session with the engine"
    );

    store.set_mode(DeleteMode::Fail);
    let delete = client
        .delete(format!("{}/v1/operators/{sid}", server.base_url))
        .bearer_auth(SEEDED_BEARER)
        .send()
        .await
        .expect("delete request");
    assert!(
        delete.status().is_server_error(),
        "a persisted-row delete failure must reach the caller as 5xx, got {}",
        delete.status()
    );

    // The session is untouched on every axis the teardown would have cleared.
    let info = client
        .get(format!("{}/v1/operators/{sid}", server.base_url))
        .bearer_auth(SEEDED_BEARER)
        .send()
        .await
        .expect("info request");
    assert_eq!(
        info.status(),
        reqwest::StatusCode::OK,
        "an aborted teardown must leave the session in `operator_sessions`"
    );
    assert!(
        engine.list_operator_ids().await.contains(&sid.to_string()),
        "an aborted teardown must leave the engine registration in place"
    );
    let conflict = client
        .post(format!("{}/v1/operators", server.base_url))
        // convention-token-ok: mlua-swarm public operator role literal.
        .json(&json!({ "roles": ["main-ai"] }))
        .send()
        .await
        .expect("conflicting mint request");
    assert_eq!(
        conflict.status(),
        reqwest::StatusCode::CONFLICT,
        "an aborted teardown must not release the role"
    );

    // ...and once the store is healthy again the same DELETE goes through,
    // so the refusal is a retryable failure, not a wedged session.
    store.set_mode(DeleteMode::Pass);
    let retry = client
        .delete(format!("{}/v1/operators/{sid}", server.base_url))
        .bearer_auth(SEEDED_BEARER)
        .send()
        .await
        .expect("retried delete request");
    assert_eq!(retry.status(), reqwest::StatusCode::NO_CONTENT);
    assert!(
        !engine.list_operator_ids().await.contains(&sid.to_string()),
        "the successful retry must complete the teardown"
    );

    server.shutdown();
}

// ─── registration is owned by the mint, not by the WS connect ───────────────

/// A server on an empty in-memory session store, handing back the same
/// `Engine` the router got so a test can watch registry membership.
async fn spawn_server_with_engine() -> (ServerHandle, Engine) {
    spawn_server_with_session_store(Arc::new(InMemoryOperatorSessionStore::new())).await
}

/// Engine-registered ids, sorted — comparable across two points in time.
async fn registered_ids(engine: &Engine) -> Vec<String> {
    let mut ids = engine.list_operator_ids().await;
    ids.sort();
    ids
}

/// Attaching a WebSocket must not add anything to the engine registry.
///
/// Registration belongs to `POST /v1/operators` now; the connect path only
/// swaps a sender into the session the sid already owns. Before this
/// change the mint registered nothing and the first connect registered
/// everything, which is what made a connect that raced a teardown able to
/// re-create registrations behind it.
#[tokio::test]
async fn connecting_a_socket_adds_nothing_to_the_engine_registry() {
    let (server, engine) = spawn_server_with_engine().await;
    let client = reqwest::Client::new();

    // convention-token-ok: mlua-swarm public operator role literal.
    let holder = mint(&client, &server.base_url, "main-ai").await;
    let sid = holder["sid"].as_str().expect("sid").to_string();
    let token = holder["token"].as_str().expect("token").to_string();

    let before = registered_ids(&engine).await;
    assert!(
        before.contains(&sid),
        "the mint alone must register the sid: {before:?}"
    );
    assert!(
        // convention-token-ok: mlua-swarm public operator role literal.
        before.contains(&"main-ai".to_string()),
        "the mint alone must register the role alias: {before:?}"
    );

    let _socket = connect_ws(&server.base_url, &sid, &token).await;
    wait_until_connected(&client, &server.base_url, &sid, &token).await;

    assert_eq!(
        registered_ids(&engine).await,
        before,
        "the WS connect path must leave registry membership exactly as it found it"
    );

    server.shutdown();
}

/// The bug this unit exists for: a teardown landing between a socket's
/// upgrade and its server-side bind.
///
/// `operators_ws_connect` returns `ws.on_upgrade(...)`, so the handler runs
/// only after the `101` has been written — `connect_ws` below can return
/// while the server has not bound anything yet. Tearing the session down
/// right there used to leave the late handler looking at an entry with no
/// session, which it answered by minting one and **registering** it: a
/// registration under a sid already gone from `operator_sessions`, so
/// `DELETE /v1/operators/:sid` could only `404` and the by-role route could
/// only clear the role map. It survived until the process exited, and the
/// client sat on a socket that would never be spoken on or closed.
///
/// Both interleavings now land in the same place, which is what makes this
/// test stable: if the bind wins, teardown closes an attached socket; if
/// the teardown wins, the late bind finds the closed session, swaps its
/// sender in, and is answered by the already-latched close. Neither adds a
/// registration. (Under the old code the teardown-first ordering both
/// re-registered and never closed, so this test hangs to its timeout there.)
#[tokio::test]
async fn a_teardown_racing_a_connect_registers_nothing_and_still_closes_the_socket() {
    let (server, engine) = spawn_server_with_engine().await;
    let client = reqwest::Client::new();

    // convention-token-ok: mlua-swarm public operator role literal.
    let holder = mint(&client, &server.base_url, "main-ai").await;
    let sid = holder["sid"].as_str().expect("sid").to_string();
    let token = holder["token"].as_str().expect("token").to_string();

    // Deliberately NO `wait_until_connected` here — waiting would close
    // the very window this test is about.
    let mut socket = connect_ws(&server.base_url, &sid, &token).await;

    let release = client
        // convention-token-ok: mlua-swarm public operator role literal.
        .delete(format!("{}/v1/operators/by-role/main-ai", server.base_url))
        .send()
        .await
        .expect("by-role delete request");
    assert_eq!(release.status(), reqwest::StatusCode::NO_CONTENT);

    // The socket must be closed either way. Reaching this point also means
    // the handler has run, so the registry assertion below is not racing it.
    let next = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .expect(
            "the socket must be closed whether it bound before or after the \
             teardown; it stayed open, which is the late-bind path having \
             created a fresh session with no close latch on it",
        );
    match next {
        Some(Ok(Message::Close(frame))) => {
            let reason = frame.map(|f| f.reason.to_string()).unwrap_or_default();
            assert!(
                reason.contains("torn down"),
                "the Close frame should say why the session went away, got: {reason:?}"
            );
        }
        other => panic!("expected a WS Close frame after teardown, got: {other:?}"),
    }

    let after = registered_ids(&engine).await;
    assert!(
        !after.contains(&sid),
        "a connect completing after the teardown must not re-register the sid — \
         nothing could ever unregister it again: {after:?}"
    );
    assert!(
        // convention-token-ok: mlua-swarm public operator role literal.
        !after.contains(&"main-ai".to_string()),
        "...nor its role alias: {after:?}"
    );

    server.shutdown();
}

/// Single-step `identity` RustFn Blueprint for the pin test below. Routes
/// through no Operator agent, so the only thing the pin exercises is
/// launch-time `operator_sid` resolution. (Duplicated rather than shared:
/// an integration test cannot reach the crate-private original, the same
/// reason `restart_operator_sid.rs` carries its own copy.)
fn identity_blueprint() -> Blueprint {
    Blueprint {
        schema_version: current_schema_version(),
        id: "operator-recovery-mint-pin-bp".into(),
        flow: serde_json::from_value(json!({
            "kind": "step",
            "ref": mlua_swarm::worker::baseline::AG_IDENTITY,
            "in": {"op": "lit", "value": "hello"},
            "out": {"op": "path", "at": "$.out"},
        }))
        .expect("flow parse"),
        agents: vec![AgentDef {
            name: mlua_swarm::worker::baseline::AG_IDENTITY.into(),
            kind: AgentKind::RustFn,
            spec: json!({"fn_id": mlua_swarm::worker::baseline::AG_IDENTITY}),
            profile: None,
            meta: None,
            runner: None,
            runner_ref: None,
            verdict: None,
            lints: None,
        }],
        // A launch pin assigns the Run to one Blueprint-declared Operator
        // seat, so the Blueprint has to declare one for the pin below to
        // resolve. Exactly one — that makes it implicit, and the launch
        // payload stays the pre-seat one (no `operator_slot`).
        operators: vec![OperatorDef {
            // convention-token-ok: mlua-swarm public operator role literal.
            name: "main-ai".into(),
            display_name: None,
            kind: None,
            spec: json!(null),
            profile: None,
            meta: None,
        }],
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
        subprocesses: vec![],
        check_policy: None,
        blueprint_ref_includes: Vec::new(),
    }
}

/// The guarantee `restart_operator_sid.rs` established for the *restore*
/// path, now held for the *mint* path: a sid is a usable `operator_sid` pin
/// from the moment it is minted, with no WebSocket anywhere.
///
/// And the other half of it — being registered is not being reachable, so
/// the same session still reports `connected: false`.
#[tokio::test]
async fn minted_session_is_launch_pinnable_before_any_ws_connect() {
    let (server, _engine) = spawn_server_with_engine().await;
    let client = reqwest::Client::new();

    // convention-token-ok: mlua-swarm public operator role literal.
    let holder = mint(&client, &server.base_url, "main-ai").await;
    let sid = holder["sid"].as_str().expect("sid").to_string();
    let token = holder["token"].as_str().expect("token").to_string();

    let launch = client
        .post(format!("{}/v1/tasks", server.base_url))
        .json(&json!({
            "blueprint": { "kind": "inline", "value": identity_blueprint() },
            // convention-token-ok: mlua-swarm public `POST /v1/tasks` payload field.
            "init_ctx": {},
            "operator_sid": sid,
            // A pin assigns the Run to that operator, and an assignment
            // records why (model A9) — a pinned launch without this is a
            // `400` on its own, which would mask what this test measures.
            "operator_desc": "minted session pinned by this test",
            "goal": "minted operator session pin",
        }))
        .send()
        .await
        .expect("pinned launch request");
    let launch_status = launch.status();
    let launch_body = launch.text().await.expect("launch body");
    assert_ne!(
        launch_status,
        reqwest::StatusCode::BAD_REQUEST,
        "a minted sid must be a usable pin before any WS connect \
         (the 400 here is the engine registry not carrying it): {launch_body}"
    );
    assert_eq!(
        launch_status,
        reqwest::StatusCode::OK,
        "the pinned launch must run to completion: {launch_body}"
    );

    let info: serde_json::Value = client
        .get(format!("{}/v1/operators/{sid}", server.base_url))
        .bearer_auth(&token)
        .send()
        .await
        .expect("info request")
        .json()
        .await
        .expect("info json");
    assert_eq!(
        info["connected"], false,
        "registering at mint must not make the session claim to be connected"
    );

    server.shutdown();
}

/// `NotFound` keeps its pre-existing success semantics: a row a concurrent
/// delete already removed is not a failure, and the in-memory teardown
/// still runs. (Green before the ordering change as well — this locks the
/// idempotent case in against the stricter error handling next to it.)
#[tokio::test]
async fn persisted_row_already_gone_is_still_a_successful_teardown() {
    let store = Arc::new(FaultyDeleteStore::new());
    // convention-token-ok: mlua-swarm public operator role literal.
    let sid = seed_persisted_session(&store, "main-ai").await;

    let (server, engine) = spawn_server_with_session_store(store.clone()).await;
    let client = reqwest::Client::new();

    store.set_mode(DeleteMode::NotFound);
    let delete = client
        .delete(format!("{}/v1/operators/{sid}", server.base_url))
        .bearer_auth(SEEDED_BEARER)
        .send()
        .await
        .expect("delete request");
    assert_eq!(
        delete.status(),
        reqwest::StatusCode::NO_CONTENT,
        "an already-deleted row is the idempotent concurrent-delete case, not a failure"
    );

    let info = client
        .get(format!("{}/v1/operators/{sid}", server.base_url))
        .bearer_auth(SEEDED_BEARER)
        .send()
        .await
        .expect("info request");
    assert_eq!(info.status(), reqwest::StatusCode::NOT_FOUND);
    assert!(
        !engine.list_operator_ids().await.contains(&sid.to_string()),
        "the in-memory teardown must still have run"
    );

    server.shutdown();
}
