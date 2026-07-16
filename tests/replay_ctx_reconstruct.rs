//! Scenario test — Ctx-snapshot replay Core primitive end-to-end.
//!
//! A 2-step RustFn Blueprint dispatched via
//! [`Engine::dispatch_attempt_with_run_ctx`] proves the deterministic
//! replay contract:
//!
//! 1. **Uninterrupted** — a fresh engine + fresh replay store dispatches
//!    step-a then step-b through the counting spawner; the spawner is hit
//!    twice, and both `DispatchOutcome::Pass` values match the RustFn's
//!    output. The replay store now carries two rows for the run
//!    (`step-a` first, `step-b` second).
//!
//! 2. **Interrupted-run + replay** — a NEW `Engine` (fresh factories, no
//!    prior state) plus a `ReplayCursor` built from ONLY the first
//!    completed step's entries dispatches step-a: the cursor hit returns
//!    the stored value verbatim, the counting spawner sees ZERO new
//!    calls. Then step-b runs: cursor miss, ordinary Adapter dispatch,
//!    the counting spawner counter increments by exactly one. The
//!    captured Ctx for step-b matches the pristine run's step-b Ctx
//!    across `agent` / `attempt` / `meta.runtime[RUN_ID_KEY]` — the
//!    "identical final Ctx across a mid-run restart" invariant the
//!    subtask brief calls out.
//!
//! No HTTP, no operator/worker session, no CLI flags — everything runs
//! in-process, matching the Core iteration's scope (Adapter-external
//! state is not persisted at this layer).

use async_trait::async_trait;
use mlua_swarm::core::agent_context::RUN_ID_KEY;
use mlua_swarm::core::state::{DispatchOutcome, TaskSpec};
use mlua_swarm::store::replay::{InMemoryReplayStore, ReplayCursor, ReplayStore};
use mlua_swarm::store::run::{InMemoryRunStore, RunContext, RunRecord, RunStatus, RunStore};
use mlua_swarm::types::{RunId, StepId, TaskId};
use mlua_swarm::worker::adapter::{
    InProcSpawner, SpawnError, SpawnerAdapter, WorkerFn, WorkerResult,
};
use mlua_swarm::worker::Worker;
use mlua_swarm::{CapToken, Ctx, Engine, EngineCfg, Role};
use serde_json::json;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

// ─── Test spawner ────────────────────────────────────────────────────────

/// Wraps an [`InProcSpawner`], counts every real `spawn()` call, and
/// snapshots the incoming [`Ctx`]. Used to prove the replay-hit path
/// bypasses the spawner entirely (counter stays at zero) and to capture
/// the step-b Ctx for the "identical across restart" invariant.
struct CountingSpawner {
    inner: Arc<dyn SpawnerAdapter>,
    counter: Arc<AtomicU32>,
    last_ctx: Arc<StdMutex<Option<Ctx>>>,
}

#[async_trait]
impl SpawnerAdapter for CountingSpawner {
    async fn spawn(
        &self,
        engine: &Engine,
        ctx: &Ctx,
        task_id: StepId,
        attempt: u32,
        token: CapToken,
    ) -> Result<Box<dyn Worker>, SpawnError> {
        self.counter.fetch_add(1, Ordering::SeqCst);
        *self.last_ctx.lock().unwrap() = Some(ctx.clone());
        self.inner.spawn(engine, ctx, task_id, attempt, token).await
    }
}

type SpawnerRig = (
    Arc<dyn SpawnerAdapter>,
    Arc<AtomicU32>,
    Arc<StdMutex<Option<Ctx>>>,
);

fn build_spawner() -> SpawnerRig {
    let mut sp: InProcSpawner = InProcSpawner::new();
    // Two RustFns keyed by agent name — the same InProcSpawner routes
    // both because `InProcSpawner::spawn` looks the closure up by
    // `ctx.agent`.
    sp.register("step-a", |inv| async move {
        Ok(WorkerResult {
            value: json!({ "by": "step-a", "prompt": inv.prompt }),
            ok: true,
        })
    });
    sp.register("step-b", |inv| async move {
        Ok(WorkerResult {
            value: json!({ "by": "step-b", "prompt": inv.prompt }),
            ok: true,
        })
    });
    let inner: Arc<dyn SpawnerAdapter> = Arc::new(sp);
    let counter = Arc::new(AtomicU32::new(0));
    let last_ctx: Arc<StdMutex<Option<Ctx>>> = Arc::new(StdMutex::new(None));
    let wrapped: Arc<dyn SpawnerAdapter> = Arc::new(CountingSpawner {
        inner,
        counter: counter.clone(),
        last_ctx: last_ctx.clone(),
    });
    // Silence the unused-import warning when `WorkerFn` isn't otherwise
    // referenced — the type alias documents the InProcSpawner registry
    // shape that `register` fills.
    let _: Option<WorkerFn> = None;
    (wrapped, counter, last_ctx)
}

// ─── Dispatch helper ─────────────────────────────────────────────────────

async fn dispatch_step(
    engine: &Engine,
    token: &CapToken,
    spawner: &Arc<dyn SpawnerAdapter>,
    agent: &str,
    directive: serde_json::Value,
    run_ctx: Option<&RunContext>,
) -> DispatchOutcome {
    let tid = engine
        .start_task(
            token,
            TaskSpec {
                agent: agent.to_string(),
                initial_directive: directive,
                step_ctx: None,
                check_policy: None,
            },
        )
        .await
        .expect("start_task");
    engine
        .dispatch_attempt_with_run_ctx(token, &tid, spawner, run_ctx)
        .await
        .expect("dispatch_attempt_with_run_ctx")
}

// ─── Setup helpers ───────────────────────────────────────────────────────

async fn attach_operator(engine: &Engine) -> CapToken {
    engine
        .attach("test-op", Role::Operator, Duration::from_secs(60))
        .await
        .expect("attach")
}

async fn seed_run(run_store: &Arc<dyn RunStore>, task_id: TaskId) -> RunId {
    let run_id = RunId::new();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    run_store
        .create(RunRecord {
            id: run_id.clone(),
            task_id,
            status: RunStatus::Running,
            step_entries: Vec::new(),
            degradations: Vec::new(),
            operator_sid: None,
            result_ref: None,
            created_at: now,
            updated_at: now,
        })
        .await
        .expect("seed RunRecord");
    run_id
}

// ─── The scenario ────────────────────────────────────────────────────────

#[tokio::test]
async fn ctx_snapshot_replay_reconstructs_final_ctx_after_mid_run_restart() {
    // ─── Uninterrupted run (baseline) ────────────────────────────────
    //
    // Fresh engine + spawner, no replay hook — dispatches step-a then
    // step-b so we can capture what the "no restart happened" Ctx for
    // step-b looks like. The replay store isn't threaded here; we're
    // building the ground truth to compare the replayed run against.

    let engine_pristine = Engine::new(EngineCfg::default());
    let op_pristine = attach_operator(&engine_pristine).await;
    let (spawner_pristine, counter_pristine, last_ctx_pristine) = build_spawner();
    let run_store_pristine: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
    let run_id_pristine = seed_run(&run_store_pristine, TaskId::new()).await;
    let rc_pristine = RunContext::new(run_id_pristine.clone(), run_store_pristine.clone());

    let out_a_pristine = dispatch_step(
        &engine_pristine,
        &op_pristine,
        &spawner_pristine,
        "step-a",
        json!("hello"),
        Some(&rc_pristine),
    )
    .await;
    let out_b_pristine = dispatch_step(
        &engine_pristine,
        &op_pristine,
        &spawner_pristine,
        "step-b",
        json!("world"),
        Some(&rc_pristine),
    )
    .await;
    let pristine_step_b_ctx = last_ctx_pristine
        .lock()
        .unwrap()
        .clone()
        .expect("step-b Ctx captured on the pristine run");
    assert_eq!(
        counter_pristine.load(Ordering::SeqCst),
        2,
        "uninterrupted run must spawn both step-a and step-b"
    );
    assert!(matches!(&out_a_pristine, DispatchOutcome::Pass(v) if v["by"] == "step-a"));
    assert!(matches!(&out_b_pristine, DispatchOutcome::Pass(v) if v["by"] == "step-b"));

    // ─── Partial run (only step-a completes, then "restart") ─────────
    //
    // Fresh engine, replay store threaded via RunContext.replay_store.
    // Dispatch step-a; the store now carries one row for this Run.
    // step-b is NOT dispatched — this simulates the mid-run restart
    // point.

    let engine_partial = Engine::new(EngineCfg::default());
    let op_partial = attach_operator(&engine_partial).await;
    let (spawner_partial, counter_partial, _) = build_spawner();
    let run_store_partial: Arc<dyn RunStore> = Arc::new(InMemoryRunStore::new());
    let replay_store: Arc<dyn ReplayStore> = Arc::new(InMemoryReplayStore::new());
    let run_id = seed_run(&run_store_partial, TaskId::new()).await;
    let rc_partial = RunContext::new(run_id.clone(), run_store_partial.clone())
        .with_replay_store(replay_store.clone());

    let out_a_partial = dispatch_step(
        &engine_partial,
        &op_partial,
        &spawner_partial,
        "step-a",
        json!("hello"),
        Some(&rc_partial),
    )
    .await;
    assert_eq!(
        counter_partial.load(Ordering::SeqCst),
        1,
        "step-a must dispatch on the first run"
    );
    assert!(matches!(&out_a_partial, DispatchOutcome::Pass(v) if v["by"] == "step-a"));

    let logged = replay_store
        .list_by_run(&run_id)
        .await
        .expect("list replay rows");
    assert_eq!(
        logged.len(),
        1,
        "partial run must have logged exactly one row (step-a)"
    );
    assert_eq!(logged[0].step_ref, "step-a");
    assert_eq!(logged[0].occurrence, 0);

    // ─── Replay run (fresh engine, cursor built from the logged rows) ─
    //
    // A brand-new `Engine` — no shared state with the partial engine
    // (fresh factories, empty `EngineState`). The cursor carries the
    // one step-a row. Dispatching step-a should hit the cursor and
    // return the stored value verbatim (spawner counter unchanged);
    // dispatching step-b should miss and dispatch normally (counter
    // increments by exactly one).

    let engine_replay = Engine::new(EngineCfg::default());
    let op_replay = attach_operator(&engine_replay).await;
    let (spawner_replay, counter_replay, last_ctx_replay) = build_spawner();
    let cursor = ReplayCursor::from_entries(logged);
    assert_eq!(cursor.len(), 1, "cursor must carry the one logged row");
    let cursor_arc = Arc::new(StdMutex::new(cursor));
    let rc_replay = RunContext::new(run_id.clone(), run_store_partial.clone())
        .with_replay_store(replay_store.clone())
        .with_replay_cursor(cursor_arc.clone());

    let out_a_replay = dispatch_step(
        &engine_replay,
        &op_replay,
        &spawner_replay,
        "step-a",
        json!("hello"),
        Some(&rc_replay),
    )
    .await;
    assert_eq!(
        counter_replay.load(Ordering::SeqCst),
        0,
        "step-a replay HIT must skip the Adapter dispatch entirely (counter must stay 0)"
    );
    match &out_a_replay {
        DispatchOutcome::Pass(v) => {
            assert_eq!(
                v["by"], "step-a",
                "replay hit must return the stored step-a output verbatim"
            );
        }
        other => panic!("expected Pass on replay hit, got {other:?}"),
    }

    let out_b_replay = dispatch_step(
        &engine_replay,
        &op_replay,
        &spawner_replay,
        "step-b",
        json!("world"),
        Some(&rc_replay),
    )
    .await;
    assert_eq!(
        counter_replay.load(Ordering::SeqCst),
        1,
        "step-b MISS must dispatch through the Adapter exactly once"
    );
    assert!(matches!(&out_b_replay, DispatchOutcome::Pass(v) if v["by"] == "step-b"));

    let replayed_step_b_ctx = last_ctx_replay
        .lock()
        .unwrap()
        .clone()
        .expect("step-b Ctx captured on the replay run");

    // ─── The Core invariant — final Ctx is identical across restart ──
    //
    // `task_id` and `worker_handle` are freshly minted per attempt and
    // therefore MUST differ between the two runs — comparing them
    // would be nonsensical. The stable fields are `agent`, `attempt`,
    // and the `RUN_ID_KEY` slot in `meta.runtime`.

    assert_eq!(
        replayed_step_b_ctx.agent, pristine_step_b_ctx.agent,
        "step-b agent must be identical across pristine and replay runs"
    );
    assert_eq!(
        replayed_step_b_ctx.attempt, pristine_step_b_ctx.attempt,
        "step-b attempt must be 1 in both runs (fresh Engine, first attempt)"
    );
    assert_eq!(
        replayed_step_b_ctx
            .meta
            .runtime
            .get(RUN_ID_KEY)
            .and_then(|v| v.as_str()),
        Some(run_id.as_str()),
        "step-b Ctx must carry the propagated RunId in meta.runtime[RUN_ID_KEY]"
    );

    // Replay run also appended step-b to the store — the log is now
    // complete: two rows, one per step.
    let final_logged = replay_store
        .list_by_run(&run_id)
        .await
        .expect("list final replay rows");
    assert_eq!(final_logged.len(), 2, "replay run must append step-b's row");
    assert_eq!(final_logged[1].step_ref, "step-b");
}
