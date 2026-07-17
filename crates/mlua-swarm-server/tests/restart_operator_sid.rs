//! Negative-path integration test: operator sessions are *not* persisted
//! across an `mse serve` restart. The supervisor is expected to re-mint via
//! `POST /v1/operators` after every restart; `GET`/`DELETE` against a
//! pre-restart sid must return `404 unknown sid`.
//!
//! This is a *decided* policy lock-in, not a bug report:
//!
//! > Adapter 外部 state (WS session / sid / worker_handle) は保存しない
//! > (User direction, β 案 = WS session + sid 永続化 は overengineered として
//! > drop). Why = `Ctx` struct は既に `#[serde(skip)] operator` で codebase 上
//! > 「Adapter concern は Ctx snapshot から自然に skip、 restart 後は fresh
//! > factory から re-mint」 が埋め込まれていた。 restart 後 MainAI が再 join
//! > (`operators_create`) で新 sid を持てば旧 handle は 410 Gone reject で
//! > 自然解消。
//!
//! Because `AppState.operator_sessions` is an in-memory `Mutex<HashMap>`
//! (`crates/mlua-swarm-server/src/lib.rs` line ~166), a fresh `axum::serve`
//! boot starts with an empty map even when the SQLite task/run/replay files
//! are re-opened on the same tempdir. A future change that begins persisting
//! sessions to the store would flip these asserts from `404` to `200`/`204`,
//! catching the drift before it lands in production.
//!
//! The SQLite bundle is re-used across the two servers for realism —
//! `replay_e2e.rs`'s success path for `Run`s runs against the exact same
//! shared-file setup, and this test proves that *sharing the run/replay
//! stores does not implicitly restore operator sessions*.

use mlua_swarm::core::config::EngineCfg;
use mlua_swarm::core::engine::Engine;
use mlua_swarm::store::replay::{ReplayStore, SqliteReplayStore};
use mlua_swarm::store::run::{RunStore, SqliteRunStore};
use mlua_swarm::store::task::{SqliteTaskStore, TaskStore};
use rusqlite_isle::AsyncIsleDriver;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::task::JoinHandle;

/// Shared SQLite bundle — mirrors `replay_e2e.rs::StoreBundle` so both
/// servers open the exact same files without re-implementing the pattern.
struct StoreBundle {
    task_store: Arc<dyn TaskStore>,
    run_store: Arc<dyn RunStore>,
    replay_store: Arc<dyn ReplayStore>,
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
        Self {
            task_store: Arc::new(task_store),
            run_store: Arc::new(run_store),
            replay_store: Arc::new(replay_store),
            drivers: vec![task_driver, run_driver, replay_driver],
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
    let router = mlua_swarm_server::build_router_full(
        engine,
        mlua_swarm_server::default_registry(),
        None,
        None,
        None,
        None,
        Some(bundle.task_store.clone()),
        Some(bundle.run_store.clone()),
        Some(bundle.replay_store.clone()),
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

#[tokio::test]
async fn operator_sid_does_not_survive_server_restart() {
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

    // ─── Server A shutdown ─────────────────────────────────────────────
    server_a.shutdown();
    bundle_a.shutdown().await;

    // ─── Server B (re-open SAME SQLite files) ──────────────────────────
    let bundle_b = StoreBundle::open(&shared_dir).await;
    let server_b = spawn_server(&bundle_b).await;

    // GET must return 404 — the operator session is not persisted, so a
    // fresh AppState boot has no record of this sid even though the same
    // task/run/replay SQLite files were re-opened.
    let get_after = client
        .get(format!("{}/v1/operators/{sid}", server_b.base_url))
        .bearer_auth(&token)
        .send()
        .await
        .expect("get request on server B");
    assert_eq!(
        get_after.status(),
        reqwest::StatusCode::NOT_FOUND,
        "GET /v1/operators/:sid must return 404 on server B: operator \
         sessions are intentionally not persisted across restart \
         (decided policy: Adapter 外部 state は保存しない)"
    );

    // DELETE must also return 404 — this is the exact call the supervisor
    // ran in the issue 6e509662 副次観察 that motivated this regression
    // lock-in.
    let delete_after = client
        .delete(format!("{}/v1/operators/{sid}", server_b.base_url))
        .bearer_auth(&token)
        .send()
        .await
        .expect("delete request on server B");
    assert_eq!(
        delete_after.status(),
        reqwest::StatusCode::NOT_FOUND,
        "DELETE /v1/operators/:sid must return 404 on server B: same \
         non-persistence invariant"
    );

    // The supervisor's supported recovery path is to re-mint. Prove that
    // a fresh `POST /v1/operators` on Server B succeeds and hands back a
    // *different* sid — the same role name is now claimable because the
    // pre-restart sid never carried into Server B's `roles_to_sid`.
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
        "supervisor's supported recovery is to re-mint on Server B"
    );
    let remint_body: serde_json::Value = remint.json().await.expect("remint json");
    let remint_sid = remint_body["sid"]
        .as_str()
        .expect("remint response missing sid");
    assert_ne!(
        remint_sid, sid,
        "re-mint must issue a fresh sid; Server B has no way to know the pre-restart sid"
    );

    server_b.shutdown();
    bundle_b.shutdown().await;
}
