//! Regression lock-in for the sync-launch driver lifetime: a synchronous `POST /v1/tasks`
//! launch must keep driving its Run after the HTTP client disconnects.
//!
//! Before the fix the driver future was awaited **inside** the axum
//! handler, so a client disconnect (a `curl` timeout, an aborted request
//! task) dropped the handler future and the run driver with it. Nothing
//! runs on a drop — the panic guard only intercepts unwinds and the
//! timeout ceiling lived inside the very future that went away — so the
//! Run stayed `Running` with no reader left for any late worker output,
//! until the stale-run sweeper reaped it ~3900s later.
//!
//! The flow here:
//!
//! 1. A single-step Blueprint dispatches a `RustFn` agent that reports it
//!    is mid-flight and then blocks on a `Notify` the test owns — the
//!    in-process stand-in for a worker the run is waiting on (a poll-style
//!    `/v1/worker/submit`, an operator ack).
//! 2. The launch request runs in its own task, which is aborted once the
//!    agent reports in. Dropping the `reqwest` future closes the
//!    connection, which is what makes axum drop the handler future.
//! 3. The agent is then released. A driver bound to the request future
//!    would be gone by now and the Run would sit `Running` forever; a
//!    spawned driver folds the result and runs `finalize_run`, so the Run
//!    reaches `Done` with a persisted `result_ref`.

use mlua_swarm::blueprint::{
    current_schema_version, AgentDef, AgentKind, Blueprint, BlueprintMetadata, CompilerHints,
    CompilerStrategy,
};
use mlua_swarm::core::config::EngineCfg;
use mlua_swarm::core::engine::Engine;
use mlua_swarm::store::replay::{InMemoryReplayStore, ReplayStore};
use mlua_swarm::store::run::{InMemoryRunStore, RunRecord, RunStatus, RunStore};
use mlua_swarm::store::task::{InMemoryTaskStore, TaskStore};
use mlua_swarm::worker::adapter::WorkerResult;
use mlua_swarm::{RustFnInProcessSpawnerFactory, SpawnerRegistry};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Notify};

/// Agent name + `fn_id` of the gated stand-in worker.
const AG_GATED: &str = "gated";

/// Single-step Blueprint over [`AG_GATED`] — same shape as
/// `replay_e2e.rs`'s `identity_blueprint`, pointed at the gated worker.
fn gated_blueprint() -> Blueprint {
    Blueprint {
        schema_version: current_schema_version(),
        id: "sync-launch-driver-lifetime-bp".into(),
        flow: serde_json::from_value(json!({
            "kind": "step",
            "ref": AG_GATED,
            "in": {"op": "lit", "value": "hello"},
            "out": {"op": "path", "at": "$.out"},
        }))
        .expect("flow parse"),
        agents: vec![AgentDef {
            name: AG_GATED.into(),
            kind: AgentKind::RustFn,
            spec: json!({"fn_id": AG_GATED}),
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
        subprocesses: vec![],
        check_policy: None,
        blueprint_ref_includes: Vec::new(),
    }
}

/// Registry whose only worker announces its start on `started` and then
/// parks until `release` fires — the test's handle on "the run is
/// mid-flight and cannot finish on its own".
fn gated_registry(started: mpsc::UnboundedSender<()>, release: Arc<Notify>) -> SpawnerRegistry {
    let factory = RustFnInProcessSpawnerFactory::new().register_fn(AG_GATED, move |_inv| {
        let started = started.clone();
        let release = release.clone();
        async move {
            // Send failure only means the test already gave up waiting;
            // the park below still decides when this worker returns.
            let _ = started.send(());
            release.notified().await;
            Ok(WorkerResult {
                value: json!({"result": "released"}),
                ok: true,
                stats: None,
            })
        }
    });
    let mut reg = SpawnerRegistry::new();
    reg.register::<RustFnInProcessSpawnerFactory>(Arc::new(factory));
    reg
}

/// Poll until the Run leaves `Pending`/`Running`, or ~15s elapse.
async fn wait_for_terminal(run_store: &Arc<dyn RunStore>, run_id: &mlua_swarm::RunId) -> RunRecord {
    for _ in 0..150 {
        let rec = run_store.get(run_id).await.expect("run get");
        if !matches!(rec.status, RunStatus::Pending | RunStatus::Running) {
            return rec;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let latest = run_store.get(run_id).await.expect("run get");
    panic!(
        "run {run_id} never reached a terminal status within ~15s: the driver did not survive \
         the client disconnect (last status={:?}, result_ref={:?})",
        latest.status, latest.result_ref
    );
}

#[tokio::test]
async fn sync_launch_driver_survives_client_disconnect_and_finalizes_the_run() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());

    let engine = Engine::new_with_layers(
        EngineCfg::default(),
        mlua_swarm_server::default_layer_registry(),
    );
    let task_store: Arc<dyn TaskStore> = Arc::new(InMemoryTaskStore::new());
    let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
    let replay_store: Arc<dyn ReplayStore> = Arc::new(InMemoryReplayStore::new());
    let router = mlua_swarm_server::build_router_full(
        engine,
        gated_registry(started_tx, release.clone()),
        None,
        None,
        None,
        None,
        Some(task_store.clone()),
        Some(run_store.clone()),
        Some(replay_store),
        300,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });

    // The launch lives in its own task so the test can drop it mid-flight;
    // the client is built inside it so the abort takes the connection down
    // with it.
    let base_url = format!("http://{addr}");
    let launch = tokio::spawn(async move {
        reqwest::Client::new()
            .post(format!("{base_url}/v1/tasks"))
            .json(&json!({
                "blueprint": { "kind": "inline", "value": gated_blueprint() },
                "init_ctx": { "in": "hello" },
                "goal": "sync launch driver lifetime",
            }))
            .send()
            .await
    });

    // The worker is dispatched, so the Run exists and is mid-flight.
    tokio::time::timeout(Duration::from_secs(10), started_rx.recv())
        .await
        .expect("gated worker must be dispatched within 10s")
        .expect("gated worker start signal");
    let running = run_store.list_running().await.expect("list_running");
    assert_eq!(
        running.len(),
        1,
        "exactly one Run is in flight at this point (got {running:?})"
    );
    let run_id = running[0].id.clone();

    // Client disconnect.
    launch.abort();
    assert!(
        launch.await.is_err(),
        "the launch request must be cancelled, not completed — otherwise the run finished \
         before the disconnect and this test proves nothing"
    );
    // Let the server observe the closed connection and drop the handler
    // future before the worker is released.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Release the worker. Only a driver that outlived the request future
    // can still consume this and finalize the Run.
    release.notify_one();

    let terminal = wait_for_terminal(&run_store, &run_id).await;
    assert_eq!(
        terminal.status,
        RunStatus::Done,
        "the spawned driver must fold the released worker result and finalize the Run \
         (terminal={terminal:?})"
    );
    assert!(
        terminal.result_ref.is_some(),
        "finalize_run must persist the final ctx (terminal={terminal:?})"
    );

    server.abort();
}
