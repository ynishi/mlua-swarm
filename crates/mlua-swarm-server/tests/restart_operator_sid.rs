//! Positive-path integration test: operator login sessions survive an
//! `mse serve` restart when an `OperatorSessionStore` is wired in.
//!
//! This *reverses* the pre-persistence policy this file used to lock in
//! ("Adapter 外部 state は保存しない — the supervisor re-mints after every
//! restart"). That policy stranded real state: every other store persists,
//! and `RunRecord.operator_sid` (SQLite, `runs.operator_sid`) persists a
//! *pointer* into the session space — so after a restart a restored run's
//! pin pointed at a sid the server no longer knew, `GET /v1/operators/:sid`
//! answered `404 unknown sid`, and the sole recovery was a forced re-login
//! that still could not reclaim the old pin. Persisting the login record
//! (sid / token / manifest / 記名) closes that asymmetry.
//!
//! What is still deliberately NOT persisted: the WS adapter state
//! (`tx` sender / `pending` oneshot map) — process-lifetime objects whose
//! correct restoration is an empty rebuild on the client's next WS connect.
//! The client keeps its sid + token across the server restart and
//! reconnects on the existing first-connect path; no re-mint involved.
//!
//! Nor is the bearer itself: only `hex(SHA-256(bearer))` reaches the file,
//! which the first test asserts directly against the database bytes. A
//! restart-survivable session must not become a credential lying on disk.
//!
//! The SQLite bundle is re-used across the two servers for realism —
//! `replay_e2e.rs`'s success path for `Run`s runs against the exact same
//! shared-file setup; this test proves the operator sessions ride along.

use mlua_swarm::blueprint::{
    current_schema_version, AgentDef, AgentKind, Blueprint, BlueprintMetadata, CompilerHints,
    CompilerStrategy, OperatorDef,
};
use mlua_swarm::core::config::EngineCfg;
use mlua_swarm::core::engine::Engine;
use mlua_swarm::store::operator_session::{OperatorSessionStore, SqliteOperatorSessionStore};
use mlua_swarm::store::replay::{ReplayStore, SqliteReplayStore};
use mlua_swarm::store::run::{RunStore, SqliteRunStore};
use mlua_swarm::store::task::{SqliteTaskStore, TaskStore};
use rusqlite_isle::AsyncIsleDriver;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::task::JoinHandle;

/// Shared SQLite bundle — mirrors `replay_e2e.rs::StoreBundle` plus the
/// operator-session store, so both servers open the exact same files
/// without re-implementing the pattern.
struct StoreBundle {
    task_store: Arc<dyn TaskStore>,
    run_store: Arc<dyn RunStore>,
    replay_store: Arc<dyn ReplayStore>,
    operator_session_store: Arc<dyn OperatorSessionStore>,
    drivers: Vec<AsyncIsleDriver>,
}

impl StoreBundle {
    async fn open(dir: &Path) -> Self {
        // allowlist-secret: runtime tempdir file names, not committed .sqlite files.
        let (task_store, task_driver) = SqliteTaskStore::open(dir.join("task.sqlite"))
            .await
            .expect("task store open");
        // allowlist-secret: runtime tempdir file names, not committed .sqlite files.
        let (run_store, run_driver) = SqliteRunStore::open(dir.join("run.sqlite"))
            .await
            .expect("run store open");
        // allowlist-secret: runtime tempdir file names, not committed .sqlite files.
        let (replay_store, replay_driver) = SqliteReplayStore::open(dir.join("replay.sqlite"))
            .await
            .expect("replay store open");
        let (operator_session_store, operator_session_driver) =
            // allowlist-secret: runtime tempdir file names, not committed .sqlite files.
            SqliteOperatorSessionStore::open(dir.join("operator_session.sqlite"))
                .await
                .expect("operator session store open");
        Self {
            task_store: Arc::new(task_store),
            run_store: Arc::new(run_store),
            replay_store: Arc::new(replay_store),
            operator_session_store: Arc::new(operator_session_store),
            drivers: vec![
                task_driver,
                run_driver,
                replay_driver,
                operator_session_driver,
            ],
        }
    }

    async fn shutdown(self) {
        for driver in self.drivers {
            let _ = driver.shutdown().await;
        }
    }
}

struct ServerHandle {
    base_url: String,
    task: JoinHandle<()>,
}

impl ServerHandle {
    fn shutdown(self) {
        self.task.abort();
    }
}

async fn spawn_server(bundle: &StoreBundle) -> ServerHandle {
    let engine = Engine::new_with_layers(
        EngineCfg::default(),
        mlua_swarm_server::default_layer_registry(),
    );
    // Same boot sequence as `mse serve`: restore the persisted sessions
    // once (which also registers them with this engine), then hand the
    // bundle to the terminal builder for map rehydration.
    let persistence = mlua_swarm_server::OperatorSessionPersistence::restore(
        bundle.operator_session_store.clone(),
        &engine,
        None,
        None,
    )
    .await
    .expect("operator session restore");
    let router = mlua_swarm_server::build_router_full_with_operator_session_persistence(
        engine,
        mlua_swarm_server::default_registry(),
        None,
        None,
        None,
        None,
        Some(bundle.task_store.clone()),
        Some(bundle.run_store.clone()),
        Some(bundle.replay_store.clone()),
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
    ServerHandle {
        base_url: format!("http://{addr}"),
        task,
    }
}

#[tokio::test]
async fn operator_sid_survives_server_restart() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let shared_dir: PathBuf = tmp.path().join("shared");
    std::fs::create_dir_all(&shared_dir).expect("mkdir shared");

    // ─── Server A ──────────────────────────────────────────────────────
    let bundle_a = StoreBundle::open(&shared_dir).await;
    let server_a = spawn_server(&bundle_a).await;
    let client = reqwest::Client::new();

    let mint = client
        .post(format!("{}/v1/operators", server_a.base_url))
        .json(&json!({ "desc": "the session this restart has to carry over" }))
        .send()
        .await
        .expect("mint request");
    assert_eq!(
        mint.status(),
        reqwest::StatusCode::OK,
        "POST /v1/operators must succeed on server A"
    );
    let mint_body: serde_json::Value = mint.json().await.expect("mint json");
    let sid = mint_body["sid"]
        .as_str()
        .expect("mint response missing sid")
        .to_string();
    let token = mint_body["token"]
        .as_str()
        .expect("mint response missing token")
        .to_string();

    // Sanity: the sid is live on Server A while it is running.
    let get_alive = client
        .get(format!("{}/v1/operators/{sid}", server_a.base_url))
        .bearer_auth(&token)
        .send()
        .await
        .expect("get request on server A");
    assert_eq!(
        get_alive.status(),
        reqwest::StatusCode::OK,
        "GET /v1/operators/:sid must return 200 on server A while the session is live"
    );

    // The durability this test is about must not come at the cost of
    // writing the bearer down: what persists is `hex(SHA-256(bearer))`.
    let persisted = bundle_a
        .operator_session_store
        .list()
        .await
        .expect("list persisted sessions");
    assert_eq!(persisted.len(), 1, "the mint must have written through");
    assert!(
        persisted[0].verify_bearer(&token),
        "the persisted digest must verify the minted bearer"
    );
    assert_ne!(
        persisted[0].token_digest, token,
        "the bearer itself must never be what is stored"
    );
    // allowlist-secret: runtime tempdir file name, not a committed .sqlite file.
    let db_bytes = std::fs::read(shared_dir.join("operator_session.sqlite")).expect("read db");
    assert!(
        !String::from_utf8_lossy(&db_bytes).contains(&token),
        "the plaintext bearer must not appear anywhere in the database file"
    );

    // ─── Server A shutdown ─────────────────────────────────────────────
    server_a.shutdown();
    bundle_a.shutdown().await;

    // ─── Server B (re-open SAME SQLite files) ──────────────────────────
    let bundle_b = StoreBundle::open(&shared_dir).await;
    let server_b = spawn_server(&bundle_b).await;

    // The session must be back: same sid, same token, 記名 intact,
    // `connected: false` (the WS adapter state is process-lifetime and is
    // correctly rebuilt empty — the client reconnects, it does not re-mint).
    let get_after = client
        .get(format!("{}/v1/operators/{sid}", server_b.base_url))
        .bearer_auth(&token)
        .send()
        .await
        .expect("get request on server B");
    assert_eq!(
        get_after.status(),
        reqwest::StatusCode::OK,
        "GET /v1/operators/:sid must return 200 on server B: operator \
         sessions persist across restart"
    );
    let get_body: serde_json::Value = get_after.json().await.expect("get json");
    assert_eq!(get_body["sid"], sid.as_str());
    assert_eq!(
        get_body["desc"], "the session this restart has to carry over",
        "D1: the 記名's confirmed part is what identifies a restored session"
    );
    assert_eq!(
        get_body["connected"], false,
        "no WS is attached yet on server B — the adapter state is rebuilt empty"
    );

    // A second mint alongside the restored one is not a conflict: a
    // session claims no name, so nothing about the restore can be
    // double-claimed.
    let second_mint = client
        .post(format!("{}/v1/operators", server_b.base_url))
        .json(&json!({ "desc": "a second driver, after the restart" }))
        .send()
        .await
        .expect("second mint request");
    assert_eq!(
        second_mint.status(),
        reqwest::StatusCode::OK,
        "a restored session blocks nobody else's join"
    );

    // DELETE with the pre-restart token must work — and must also remove
    // the persisted row.
    let delete_after = client
        .delete(format!("{}/v1/operators/{sid}", server_b.base_url))
        .bearer_auth(&token)
        .send()
        .await
        .expect("delete request on server B");
    assert_eq!(
        delete_after.status(),
        reqwest::StatusCode::NO_CONTENT,
        "DELETE /v1/operators/:sid must tear down the restored session"
    );

    let gone = client
        .get(format!("{}/v1/operators/{sid}", server_b.base_url))
        .bearer_auth(&token)
        .send()
        .await
        .expect("get after delete");
    assert_eq!(
        gone.status(),
        reqwest::StatusCode::NOT_FOUND,
        "the torn-down session must not answer any more"
    );

    server_b.shutdown();
    bundle_b.shutdown().await;
}

/// The teardown write-through must reach the store: a session deleted on
/// Server A stays gone on Server B (no zombie resurrection from a stale
/// persisted row).
#[tokio::test]
async fn deleted_session_does_not_resurrect_on_restart() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let shared_dir: PathBuf = tmp.path().join("shared");
    std::fs::create_dir_all(&shared_dir).expect("mkdir shared");

    let bundle_a = StoreBundle::open(&shared_dir).await;
    let server_a = spawn_server(&bundle_a).await;
    let client = reqwest::Client::new();

    let mint = client
        .post(format!("{}/v1/operators", server_a.base_url))
        .json(&json!({ "desc": "a session that leaves before the restart" }))
        .send()
        .await
        .expect("mint request");
    let mint_body: serde_json::Value = mint.json().await.expect("mint json");
    let sid = mint_body["sid"].as_str().expect("sid").to_string();
    let token = mint_body["token"].as_str().expect("token").to_string();

    let delete = client
        .delete(format!("{}/v1/operators/{sid}", server_a.base_url))
        .bearer_auth(&token)
        .send()
        .await
        .expect("delete request on server A");
    assert_eq!(delete.status(), reqwest::StatusCode::NO_CONTENT);

    server_a.shutdown();
    bundle_a.shutdown().await;

    let bundle_b = StoreBundle::open(&shared_dir).await;
    let server_b = spawn_server(&bundle_b).await;

    let get_after = client
        .get(format!("{}/v1/operators/{sid}", server_b.base_url))
        .bearer_auth(&token)
        .send()
        .await
        .expect("get request on server B");
    assert_eq!(
        get_after.status(),
        reqwest::StatusCode::NOT_FOUND,
        "a session torn down before the restart must not come back"
    );

    server_b.shutdown();
    bundle_b.shutdown().await;
}

/// Single-step `identity` RustFn Blueprint — the launch payload used by the
/// pin test below. Same shape as `replay_e2e.rs`'s helper (an integration
/// test cannot reach that crate-private one); it deliberately routes through
/// no Operator agent, so the only thing the pin exercises is the launch-time
/// `operator_sid` resolution.
fn identity_blueprint() -> Blueprint {
    Blueprint {
        schema_version: current_schema_version(),
        id: "restart-operator-sid-pin-bp".into(),
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

/// Restoring the login record is only half of a restart: the sid must also
/// be resolvable from the **engine** registry the moment the server is up.
///
/// `POST /v1/tasks` validates `operator_sid` against
/// `engine.list_operator_ids()` and answers `400` when the sid is not there
/// — and before this fix the three `register_*` calls ran only inside
/// `handle_operator_socket`'s first-connect arm, so between boot and the
/// owning client's WS reconnect a restored sid was known to
/// `GET /v1/operators/:sid` yet unusable as a pin. That window is what this
/// test closes: **no WebSocket is connected anywhere in it**.
#[tokio::test]
async fn restored_session_is_launch_pinnable_before_any_ws_connect() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let shared_dir: PathBuf = tmp.path().join("shared");
    std::fs::create_dir_all(&shared_dir).expect("mkdir shared");

    // ─── Server A: mint, never connect a WS ────────────────────────────
    let bundle_a = StoreBundle::open(&shared_dir).await;
    let server_a = spawn_server(&bundle_a).await;
    let client = reqwest::Client::new();

    let mint = client
        .post(format!("{}/v1/operators", server_a.base_url))
        .json(&json!({ "desc": "pinnable before any WS connect" }))
        .send()
        .await
        .expect("mint request");
    assert_eq!(mint.status(), reqwest::StatusCode::OK);
    let mint_body: serde_json::Value = mint.json().await.expect("mint json");
    let sid = mint_body["sid"].as_str().expect("sid").to_string();
    let token = mint_body["token"].as_str().expect("token").to_string();

    server_a.shutdown();
    bundle_a.shutdown().await;

    // ─── Server B: same store, still no WS ─────────────────────────────
    let bundle_b = StoreBundle::open(&shared_dir).await;
    let server_b = spawn_server(&bundle_b).await;

    let launch = client
        .post(format!("{}/v1/tasks", server_b.base_url))
        .json(&json!({
            "blueprint": { "kind": "inline", "value": identity_blueprint() },
            "init_ctx": {},
            "operator_sid": sid,
            // A pin assigns the Run to that operator, and an assignment
            // records why (model A9) — a pinned launch without this is a
            // `400` on its own, which would mask what this test measures.
            "operator_desc": "restored session pinned by this test",
            "goal": "restored operator session pin",
        }))
        .send()
        .await
        .expect("pinned launch request");
    let launch_status = launch.status();
    let launch_body = launch.text().await.expect("launch body");
    assert_ne!(
        launch_status,
        reqwest::StatusCode::BAD_REQUEST,
        "a restored sid must be a usable pin before any WS reconnect \
         (the 400 here is the engine registry not carrying it): {launch_body}"
    );
    assert_eq!(
        launch_status,
        reqwest::StatusCode::OK,
        "the pinned launch must run to completion: {launch_body}"
    );

    // Registry membership is not connectivity: the restored session is
    // registered, and still reports itself as having no live socket.
    let info = client
        .get(format!("{}/v1/operators/{sid}", server_b.base_url))
        .bearer_auth(&token)
        .send()
        .await
        .expect("get request on server B");
    assert_eq!(info.status(), reqwest::StatusCode::OK);
    let info_body: serde_json::Value = info.json().await.expect("get json");
    assert_eq!(
        info_body["connected"], false,
        "being registered must not be reported as being connected"
    );

    server_b.shutdown();
    bundle_b.shutdown().await;
}
