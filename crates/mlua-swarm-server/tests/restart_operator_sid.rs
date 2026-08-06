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
//! (sid / token / roles / manifest) closes that asymmetry.
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
    // Same boot sequence as `mse serve`: list the persisted sessions once,
    // hand the bundle to the terminal builder for map rehydration.
    let persistence = mlua_swarm_server::OperatorSessionPersistence::restore(
        bundle.operator_session_store.clone(),
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
        // convention-token-ok: "main-ai" is a mlua-swarm public operator role name.
        .json(&json!({ "roles": ["main-ai"] }))
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

    // The session must be back: same sid, same token, roles intact,
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
    assert_eq!(get_body["roles"], json!(["main-ai"]));
    assert_eq!(
        get_body["connected"], false,
        "no WS is attached yet on server B — the adapter state is rebuilt empty"
    );

    // Roles exclusivity must be rehydrated too: the restored session still
    // owns `main-ai`, so a competing mint conflicts instead of silently
    // double-claiming the role.
    let conflicting_mint = client
        .post(format!("{}/v1/operators", server_b.base_url))
        // convention-token-ok: "main-ai" is a mlua-swarm public operator role name.
        .json(&json!({ "roles": ["main-ai"] }))
        .send()
        .await
        .expect("conflicting mint request");
    assert_eq!(
        conflicting_mint.status(),
        reqwest::StatusCode::CONFLICT,
        "the restored session must still hold the role after restart"
    );

    // DELETE with the pre-restart token must work — and must also remove
    // the persisted row, re-opening the role for a fresh mint.
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

    let remint = client
        .post(format!("{}/v1/operators", server_b.base_url))
        // convention-token-ok: "main-ai" is a mlua-swarm public operator role name.
        .json(&json!({ "roles": ["main-ai"] }))
        .send()
        .await
        .expect("remint request");
    assert_eq!(
        remint.status(),
        reqwest::StatusCode::OK,
        "after teardown the role is claimable again"
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
        .json(&json!({ "roles": [] }))
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
