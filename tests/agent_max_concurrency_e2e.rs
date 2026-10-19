//! `AgentDef.max_concurrency` end to end: a `fanout` whose lanes all
//! dispatch one Subprocess agent never runs more children of it at once
//! than the agent's limit — within one run and across runs on the same
//! engine.
//!
//! Each child registers itself in a shared directory, records how many
//! children are registered at that moment, sleeps, and unregisters. The
//! largest recorded count is the observed concurrency.
//!
//! `#[cfg(unix)]`: the children are `sh -c` scripts, like the other
//! Subprocess e2e tests.

#![cfg(unix)]

use mlua_swarm::{
    Compiler, Engine, EngineCfg, Role, SpawnerRegistry, SubprocessProcessSpawnerFactory,
    TaskLaunchInput, TaskLaunchService,
};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

fn service() -> TaskLaunchService {
    let mut reg = SpawnerRegistry::new();
    reg.register::<SubprocessProcessSpawnerFactory>(Arc::new(SubprocessProcessSpawnerFactory));
    TaskLaunchService::new(Engine::new(EngineCfg::default()), Compiler::new(reg))
}

fn launch_input(bp: Value) -> TaskLaunchInput {
    TaskLaunchInput::automate(
        serde_json::from_value(bp).expect("blueprint deserializes"),
        "max-concurrency-e2e-op",
        Role::Operator,
        Duration::from_secs(30),
        json!({}),
    )
}

/// A Blueprint that fans `lanes` items out to one Subprocess agent whose
/// child registers in `dir`, appends the live count to `dir/counts`, and
/// sleeps 300 ms. `limit = None` leaves the agent unlimited.
fn fanout_bp(id: &str, dir: &Path, lanes: usize, limit: Option<u32>) -> Value {
    let d = dir.display();
    let script = format!(
        "mkdir -p '{d}/live'; touch \"{d}/live/$$\"; \
         ls '{d}/live' | wc -l | tr -d ' ' >> '{d}/counts'; \
         sleep 0.3; rm -f \"{d}/live/$$\"; printf '{{\"result\": 1}}'"
    );
    let mut agent = json!({
        "name": "worker",
        "kind": "subprocess",
        "spec": {},
        "runner": {"backend": "subprocess", "template": "probe"}
    });
    if let Some(n) = limit {
        agent["max_concurrency"] = json!(n);
    }
    json!({
        "schema_version": "0.1.0",
        "id": id,
        "flow": {
            "kind": "fanout",
            "items": {"op": "lit", "value": (0..lanes).collect::<Vec<_>>()},
            "bind": {"op": "path", "at": "$.item"},
            "join": "all",
            "out": {"op": "path", "at": "$.results"},
            "body": {
                "kind": "step",
                "ref": "worker",
                "in": {"op": "path", "at": "$.item"},
                "out": {"op": "path", "at": "$.lane"}
            }
        },
        "agents": [agent],
        "subprocesses": [{
            "name": "probe",
            "argv": ["sh", "-c", script],
            "output": {"format": "json", "result_ptr": "/result"}
        }]
    })
}

/// The largest live count any child recorded.
fn max_observed(dir: &Path) -> usize {
    std::fs::read_to_string(dir.join("counts"))
        .expect("children wrote their counts")
        .lines()
        .map(|l| l.trim().parse::<usize>().expect("a count"))
        .max()
        .expect("at least one count")
}

#[tokio::test]
async fn fanout_lanes_never_exceed_the_agent_limit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = service()
        .launch(launch_input(fanout_bp("mc-limit", dir.path(), 6, Some(2))))
        .await
        .expect("launch must complete");
    assert_eq!(out.final_ctx["results"].as_array().map(Vec::len), Some(6));
    assert_eq!(
        max_observed(dir.path()),
        2,
        "limit 2 must cap, and reach, 2"
    );
}

#[tokio::test]
async fn the_limit_is_shared_by_concurrent_runs_of_the_same_blueprint() {
    let dir = tempfile::tempdir().expect("tempdir");
    let svc = service();
    let (a, b) = tokio::join!(
        svc.launch(launch_input(fanout_bp("mc-shared", dir.path(), 3, Some(2)))),
        svc.launch(launch_input(fanout_bp("mc-shared", dir.path(), 3, Some(2)))),
    );
    a.expect("run a must complete");
    b.expect("run b must complete");
    assert!(
        max_observed(dir.path()) <= 2,
        "two runs of one Blueprint share its limit of 2"
    );
}

/// Control: without a limit the same fanout does run more than two
/// children at once, so the assertions above are measuring the limit and
/// not the test's own timing.
#[tokio::test]
async fn without_a_limit_the_lanes_run_together() {
    let dir = tempfile::tempdir().expect("tempdir");
    service()
        .launch(launch_input(fanout_bp("mc-unlimited", dir.path(), 6, None)))
        .await
        .expect("launch must complete");
    assert!(
        max_observed(dir.path()) > 2,
        "an unlimited fanout must overlap more than 2 children"
    );
}

#[tokio::test]
async fn zero_limit_is_refused_at_compile_time() {
    let dir = tempfile::tempdir().expect("tempdir");
    let err = service()
        .launch(launch_input(fanout_bp("mc-zero", dir.path(), 2, Some(0))))
        .await
        .expect_err("max_concurrency = 0 must be refused");
    let msg = err.to_string();
    assert!(msg.contains("max_concurrency = 0"), "{msg}");
}

/// The wait for a slot is recorded as the step entry's `queued_ms` and is
/// kept out of its `duration_ms`: with a limit of 1, four 300 ms lanes
/// run one after another, so the last one waits roughly 900 ms while each
/// lane's own duration stays near 300 ms.
#[tokio::test]
async fn the_wait_is_recorded_as_queued_ms_not_duration() {
    use mlua_swarm::store::run::{InMemoryRunStore, RunContext, RunRecord, RunStatus, RunStore};
    use mlua_swarm::types::{RunId, TaskId};

    let dir = tempfile::tempdir().expect("tempdir");
    let run_store: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
    let run_id = RunId::new();
    run_store
        .create(RunRecord {
            id: run_id.clone(),
            task_id: TaskId::new(),
            status: RunStatus::Running,
            step_entries: Vec::new(),
            degradations: Vec::new(),
            operator_sid: None,
            current: Default::default(),
            next_generation: 0,
            result_ref: None,
            input_json: Some("{}".to_string()),
            created_at: 0,
            updated_at: 0,
        })
        .await
        .expect("seed RunRecord");

    let mut input = launch_input(fanout_bp("mc-queued", dir.path(), 4, Some(1)));
    input.run_ctx = Some(RunContext::new(run_id.clone(), run_store.clone()));
    service().launch(input).await.expect("launch must complete");

    let run = run_store.get(&run_id).await.expect("run present");
    let worker: Vec<_> = run
        .step_entries
        .iter()
        .filter(|e| e.step_ref.as_deref() == Some("worker"))
        .collect();
    assert_eq!(worker.len(), 4, "{:?}", run.step_entries);
    let max_queued = worker
        .iter()
        .map(|e| e.queued_ms.expect("a limited agent records queued_ms"))
        .max()
        .unwrap();
    assert!(
        max_queued >= 600,
        "the last lane must have waited: {max_queued} ms"
    );
    // Relative, not absolute, so a slow CI runner cannot flake it: had the
    // wait been folded into `duration_ms`, the last lane's duration would
    // be at least its wait.
    for e in &worker {
        let d = e.duration_ms.expect("duration_ms");
        assert!(
            d < max_queued,
            "duration must exclude the wait: duration {d} ms, longest wait {max_queued} ms"
        );
    }
}
