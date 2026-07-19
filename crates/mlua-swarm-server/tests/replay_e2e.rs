//! In-process integration test for restart-crossing resume of an
//! `Interrupted` Run under the SAME `run_id`.
//!
//! Simulates a supervisor restart by driving two `axum::serve`
//! instances back to back against the SAME on-disk SQLite files
//! (`SqliteTaskStore`, `SqliteRunStore`, `SqliteReplayStore`). The
//! subprocess variant (a real `mse serve` binary via
//! `std::process::Command::spawn`) and boot-time auto-respawn are both
//! deferred to a separate carry — this file exercises only the
//! in-process cut of the flow.
//!
//! Scenario:
//!
//! 1. **Server A** launches an `identity` RustFn Blueprint against the
//!    shared SQLite files through `POST /v1/tasks`, lets it run to
//!    `Done`, then forces the `RunRecord` back to `Interrupted` via
//!    the store handle (the cleanest way to stage a mid-flight restart
//!    without hooking dispatch cancellation).
//! 2. **Real replay append** — `EngineDispatcher::dispatch`
//!    (`src/blueprint.rs`) routes through
//!    `Engine::dispatch_attempt_with_run_ctx`, so Server A's successful
//!    dispatch persists one `ReplayEntry` per step through the shared
//!    `SqliteReplayStore` handle. The test asserts the row appears in
//!    the store before forcing `Interrupted`, so Server B has real
//!    replay content to resume against (no test-side seeding).
//! 3. **Server A shutdown** — the `axum::serve` join handle is aborted
//!    and every `AsyncIsleDriver` is `.shutdown().await`ed so the
//!    SQLite writer threads flush cleanly (dropping without shutdown
//!    would leave those threads dangling until the test process
//!    exited).
//! 4. **Server B** re-opens the same SQLite files (proving the roundtrip
//!    survives a fresh `SqliteRunStore::open`) and boots a second
//!    router on a fresh ephemeral port. The replay log's row count
//!    and the `RunRecord.status` = `Interrupted` are re-checked
//!    directly on Server B's fresh store handle plus over HTTP via
//!    `GET /v1/runs/:id`.
//! 5. `POST /v1/runs/:id/resume` returns `202` with `replayed_steps: 1`
//!    (the row Server A appended). Polling `GET /v1/runs/:id` settles
//!    on `Done`, and the resumed run persists a `result_ref` — the
//!    persisted launch-input snapshot + replay cursor are what let
//!    Server B complete the same `run_id`.
//!
//! The Core "identical final Ctx across a restart" invariant already
//! has a byte-exact assertion at the in-process layer
//! (`tests/replay_ctx_reconstruct.rs` in the root crate — same
//! `ReplayCursor` code path). This integration test's contribution is
//! the *SQLite + HTTP + resume-endpoint* roundtrip: same-`run_id`
//! resume completes to `Done` across a real server restart, with the
//! `replayed_steps` count sourced from `SqliteReplayStore::list_by_run`
//! on the fresh Server B handle. Duplicating a per-field Ctx compare
//! through the extra HTTP layer would add fragility without additional
//! coverage.

use mlua_swarm::blueprint::{
    current_schema_version, AgentDef, AgentKind, Blueprint, BlueprintMetadata, CompilerHints,
    CompilerStrategy,
};
use mlua_swarm::core::config::EngineCfg;
use mlua_swarm::core::engine::Engine;
use mlua_swarm::store::replay::{ReplayStore, SqliteReplayStore};
use mlua_swarm::store::run::{RunStatus, RunStore, SqliteRunStore};
use mlua_swarm::store::task::{SqliteTaskStore, TaskStore};
use mlua_swarm::RunId;
use rusqlite_isle::AsyncIsleDriver;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// A single-step RustFn `identity` Blueprint — same shape as
/// `resume.rs`'s helper, self-contained here because an integration
/// test cannot reach that crate-private helper.
fn identity_blueprint() -> Blueprint {
    Blueprint {
        schema_version: current_schema_version(),
        id: "replay-e2e-bp".into(),
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
        }],
        operators: vec![],
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
        check_policy: None,
        blueprint_ref_includes: Vec::new(),
    }
}

/// SQLite-backed store bundle — a single tempdir carries all three
/// files, so re-opening them for "Server B" boils down to re-running
/// the same `open` calls against the same paths.
struct StoreBundle {
    task_store: Arc<dyn TaskStore>,
    run_store: Arc<dyn RunStore>,
    replay_store: Arc<dyn ReplayStore>,
    drivers: Vec<AsyncIsleDriver>,
}

impl StoreBundle {
    async fn open(dir: &Path) -> Self {
        let (task_store, task_driver) = SqliteTaskStore::open(dir.join("task.sqlite"))
            .await
            .expect("task store open");
        let (run_store, run_driver) = SqliteRunStore::open(dir.join("run.sqlite"))
            .await
            .expect("run store open");
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

    /// Flush every SQLite writer thread. Mirrors `mse serve`'s own
    /// shutdown path.
    async fn shutdown(self) {
        for driver in self.drivers {
            let _ = driver.shutdown().await;
        }
    }
}

/// A running `axum::serve` handle plus the caller-facing base URL.
struct ServerHandle {
    base_url: String,
    task: JoinHandle<()>,
}

impl ServerHandle {
    fn shutdown(self) {
        self.task.abort();
    }
}

/// Boot a router configured with the caller-supplied SQLite-backed
/// stores. Mirrors `resume.rs`'s `spawn_server` pattern but threads
/// the caller's `task_store` in as well (Server A / B share all three
/// SQLite files, not just the run + replay pair).
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

/// Drive `POST /v1/tasks` with the `identity` Blueprint and return the
/// launched `RunId`. Blocks until the server replies (the entry is
/// synchronous end to end — no polling needed to reach `Done` here).
async fn launch_run(base_url: &str) -> (RunId, serde_json::Value) {
    let resp = reqwest::Client::new()
        .post(format!("{base_url}/v1/tasks"))
        .json(&json!({
            "blueprint": { "kind": "inline", "value": identity_blueprint() },
            "init_ctx": { "in": "hello" },
            "goal": "replay e2e",
        }))
        .send()
        .await
        .expect("launch request");
    let status = resp.status();
    let text = resp.text().await.expect("launch body");
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "POST /v1/tasks must return 200 (body={text})"
    );
    let body: serde_json::Value =
        serde_json::from_str(&text).expect("launch response is not valid JSON");
    let run_id_str = body["run_id"]
        .as_str()
        .unwrap_or_else(|| panic!("launch response missing run_id string (body={body})"));
    let run_id = RunId::parse(run_id_str).expect("run_id parse");
    (run_id, body)
}

/// Poll `RunStore::get` until the Run leaves `Pending`/`Running` or a
/// generous ~15s ceiling elapses (deliberately loose — the two-server
/// restart flow doubles SQLite bookkeeping, and false-timing this
/// would make the assertion diagnostic harder to reason about than
/// the actual pass/fail).
async fn wait_for_terminal(
    run_store: &Arc<dyn RunStore>,
    run_id: &RunId,
) -> mlua_swarm::store::run::RunRecord {
    for _ in 0..150 {
        let rec = run_store.get(run_id).await.expect("run get");
        if !matches!(rec.status, RunStatus::Pending | RunStatus::Running) {
            return rec;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let latest = run_store.get(run_id).await.expect("run get");
    panic!(
        "run {run_id} never reached a terminal status within ~15s \
         (last status={:?}, result_ref={:?})",
        latest.status, latest.result_ref
    );
}

#[tokio::test]
async fn restart_across_server_processes_resumes_interrupted_run_under_same_id() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let shared_dir: PathBuf = tmp.path().join("shared");
    std::fs::create_dir_all(&shared_dir).expect("mkdir shared");

    // ─── Server A ──────────────────────────────────────────────────
    let bundle_a = StoreBundle::open(&shared_dir).await;
    let server_a = spawn_server(&bundle_a).await;
    let (run_id, launch_body) = launch_run(&server_a.base_url).await;

    // Wait for Server A's synchronous launch to settle at Done.
    let terminal_a = wait_for_terminal(&bundle_a.run_store, &run_id).await;
    assert_eq!(
        terminal_a.status,
        RunStatus::Done,
        "server A run must complete to Done before we force it back to \
         Interrupted (launch_body={launch_body}, terminal_a={terminal_a:?})"
    );

    // Server A's dispatch is expected to have appended one Ctx-snapshot
    // row to the replay log via `EngineDispatcher::dispatch` →
    // `Engine::dispatch_attempt_with_run_ctx`. No test-side seeding —
    // we assert the row appears on the shared SQLite handle so that
    // Server B (fresh process, same file) has real replay content to
    // resume against.
    let logged_a = bundle_a
        .replay_store
        .list_by_run(&run_id)
        .await
        .expect("list_by_run on server A");
    assert_eq!(
        logged_a.len(),
        1,
        "dispatch on server A must append exactly one ReplayEntry \
         for the identity step (got {:?})",
        logged_a
    );

    // Force `Interrupted` via the store handle — the cleanest way to
    // simulate a mid-flight restart without hooking dispatch
    // cancellation.
    bundle_a
        .run_store
        .update_status(&run_id, RunStatus::Interrupted)
        .await
        .expect("force interrupted");

    // ─── Server A shutdown ─────────────────────────────────────────
    server_a.shutdown();
    bundle_a.shutdown().await;

    // ─── Server B (re-open SAME SQLite files) ──────────────────────
    let bundle_b = StoreBundle::open(&shared_dir).await;
    let server_b = spawn_server(&bundle_b).await;

    let logged_b = bundle_b
        .replay_store
        .list_by_run(&run_id)
        .await
        .expect("list_by_run on server B");
    assert_eq!(
        logged_b.len(),
        1,
        "replay log must survive the server A → B restart roundtrip"
    );

    let get_before_status = reqwest::Client::new()
        .get(format!("{}/v1/runs/{run_id}", server_b.base_url))
        .send()
        .await
        .expect("get run request")
        .status();
    assert_eq!(
        get_before_status,
        reqwest::StatusCode::OK,
        "GET /v1/runs/:id on server B must return 200 before resume"
    );

    let interrupted_before = bundle_b.run_store.get(&run_id).await.expect("run get");
    assert_eq!(
        interrupted_before.status,
        RunStatus::Interrupted,
        "server B must see the run as Interrupted before resume"
    );

    // ─── Resume ────────────────────────────────────────────────────
    let resume_resp = reqwest::Client::new()
        .post(format!("{}/v1/runs/{run_id}/resume", server_b.base_url))
        .send()
        .await
        .expect("resume request");
    let resume_status = resume_resp.status();
    let resume_text = resume_resp.text().await.expect("resume body");
    assert_eq!(
        resume_status,
        reqwest::StatusCode::ACCEPTED,
        "POST /v1/runs/:id/resume must return 202 (body={resume_text})"
    );
    let resume: serde_json::Value =
        serde_json::from_str(&resume_text).expect("resume response is not valid JSON");
    assert_eq!(
        resume["run_id"].as_str(),
        Some(run_id.to_string().as_str()),
        "resume must not mint a new run_id (resume body={resume})"
    );
    assert_eq!(
        resume["replayed_steps"].as_u64(),
        Some(1),
        "resume must report one replayed step from the server A run \
         (resume body={resume})"
    );

    // ─── Terminal on Server B ──────────────────────────────────────
    let terminal_b = wait_for_terminal(&bundle_b.run_store, &run_id).await;
    assert_eq!(
        terminal_b.status,
        RunStatus::Done,
        "resumed run must complete to Done (terminal_b={terminal_b:?})"
    );
    assert!(
        terminal_b.result_ref.is_some(),
        "resumed run's finalize_run must persist a final_ctx (terminal_b={terminal_b:?})"
    );

    // Replay log still has exactly one entry — a replay hit does not
    // re-append (the entry already carries `step-a`; no new dispatch
    // happened for it on Server B).
    let logged_final = bundle_b
        .replay_store
        .list_by_run(&run_id)
        .await
        .expect("list_by_run final");
    assert_eq!(
        logged_final.len(),
        1,
        "replay-hit path must not append duplicate rows (got {:?})",
        logged_final
    );

    server_b.shutdown();
    bundle_b.shutdown().await;
}
