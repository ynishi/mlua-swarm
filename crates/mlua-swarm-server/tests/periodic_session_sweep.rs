//! The `operator-session-expiry` job as the server actually wires it:
//! registered by the terminal router builder, visible on `GET /v1/status`,
//! and releasing a real session against a live `axum::serve` instance.
//!
//! The horizon's *behaviour* is pinned by unit tests next to the code
//! (`operator_ws::login::tests::o1_expiry`), which can place a session's
//! access clock anywhere. What only an integration test can show is the
//! wiring: that the job the builder registers is bound to the state this
//! server serves, that its period reaches it from config, and that an
//! operator can see it ran.
//!
//! Why the job exists at all, given the reads already expire what they
//! touch: the reads need a caller. A driver that crashed on a server nobody
//! is listing leaves a session that stays registered — and a dispatch
//! routed at its sid resolves through those registrations and parks, which
//! is not a path that judges the horizon. See `mlua_swarm_server::periodic`
//! for the rule that lets this be scheduled at all (the timer supplies the
//! arrival, never the predicate).

use mlua_swarm::core::config::EngineCfg;
use mlua_swarm::core::engine::Engine;
use mlua_swarm::store::operator_session::{
    InMemoryOperatorSessionStore, OperatorSessionRecord, OperatorSessionStore,
    OPERATOR_SESSION_MAX_IDLE_SECS,
};
use mlua_swarm::SessionId;
use mlua_swarm_server::periodic::PeriodicJobsHandle;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

const JOB: &str = "operator-session-expiry";

struct ServerHandle {
    base_url: String,
    task: JoinHandle<()>,
    /// Held: dropping it would stop every job this test is about.
    jobs: PeriodicJobsHandle,
}

impl ServerHandle {
    fn shutdown(self) {
        self.jobs.shutdown();
        self.task.abort();
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs()
}

/// A persisted session whose last access was `idle_secs` ago — the row a
/// driver that crashed that long ago leaves behind.
fn record_idle_for(sid: &SessionId, idle_secs: u64, desc: &str) -> OperatorSessionRecord {
    let accessed_at = now_secs() - idle_secs;
    OperatorSessionRecord {
        sid: sid.clone(),
        token_digest: OperatorSessionRecord::digest_of("crashed-driver-bearer"),
        capability_manifest: None,
        joined_at_secs: accessed_at,
        last_access_secs: accessed_at,
        desc: Some(desc.to_string()),
        observed: Vec::new(),
        observed_total: 0,
    }
}

/// Boots a server the way `mse serve` does — restore, then the terminal
/// builder — with `sweep_secs` as the job's period, and returns the handle
/// plus the store so the test can read the durable side.
async fn spawn_server(
    seed: Vec<OperatorSessionRecord>,
    sweep_secs: u64,
) -> (ServerHandle, Arc<dyn OperatorSessionStore>) {
    let store: Arc<dyn OperatorSessionStore> = Arc::new(InMemoryOperatorSessionStore::new());
    for record in seed {
        store.put(record).await.expect("seed the persisted row");
    }
    let engine = Engine::new_with_layers(
        EngineCfg::default(),
        mlua_swarm_server::default_layer_registry(),
    );
    let persistence =
        mlua_swarm_server::OperatorSessionPersistence::restore(store.clone(), &engine, None, None)
            .await
            .expect("operator session restore");
    let (router, jobs) = mlua_swarm_server::build_router_full_with_operator_session_persistence(
        engine,
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
        sweep_secs,
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
            jobs,
        },
        store,
    )
}

/// The `periodic_jobs` entry for `name` as `GET /v1/status` serves it.
///
/// Read off the JSON rather than deserialised into `JobReport`: the report
/// is a response type, and the point of asking over HTTP is to pin what a
/// caller who only has the wire format can see.
async fn status_job(
    client: &reqwest::Client,
    base_url: &str,
    name: &str,
) -> Option<serde_json::Value> {
    let body: serde_json::Value = client
        .get(format!("{base_url}/v1/status"))
        .send()
        .await
        .expect("status request")
        .json()
        .await
        .expect("status json");
    body["periodic_jobs"]
        .as_array()
        .expect("status must carry a periodic_jobs array")
        .iter()
        .find(|j| j["name"] == name)
        .cloned()
}

/// The wiring, end to end: a session past the horizon that **nothing
/// reads** is released by the job, and the release is the full teardown —
/// the durable row goes with it, so a restart does not bring it back.
///
/// The session is seeded a few seconds short of the horizon and crosses it
/// while the test waits, because the horizon is wall-clock and model-fixed
/// (24h) — there is no knob to shorten it, and shortening it is not what
/// this job does.
#[tokio::test]
async fn the_job_releases_a_session_that_crosses_the_horizon_unread() {
    let sid = SessionId::new();
    // Restore judges the row too, so it must still be inside the horizon at
    // boot — the assertion below fails loudly if this machine took longer
    // than the margin to get there.
    let margin_secs = 3;
    let (server, store) = spawn_server(
        vec![record_idle_for(
            &sid,
            OPERATOR_SESSION_MAX_IDLE_SECS - margin_secs,
            "about to cross the horizon with nobody watching",
        )],
        // Scheduled, but far enough out that the run below is the one this
        // test is measuring rather than a tick that happened to land.
        3600,
    )
    .await;
    let client = reqwest::Client::new();

    assert!(
        store.get(&sid).await.expect("store get").is_some(),
        "the seeded session must survive the boot restore, or this test is \
         measuring the restore path instead of the job"
    );

    tokio::time::sleep(Duration::from_millis(margin_secs * 1000 + 300)).await;

    // Nothing has read the session in between: `run_now` drives the same
    // body the schedule drives, without waiting an hour for the tick.
    let released = server
        .jobs
        .run_now(JOB)
        .await
        .expect("the terminal builder must register the job");
    assert_eq!(
        released,
        Ok(1),
        "the job must release the session that crossed the horizon"
    );

    assert!(
        store.get(&sid).await.expect("store get").is_none(),
        "expiry is the same teardown a leave performs: the persisted row goes too"
    );
    let info = client
        .get(format!("{}/v1/operators/{sid}", server.base_url))
        .bearer_auth("crashed-driver-bearer")
        .send()
        .await
        .expect("info request");
    assert_eq!(
        info.status(),
        reqwest::StatusCode::NOT_FOUND,
        "and the session is gone from the server, not merely hidden from one route — \
         the sid reads as unknown, which is what a released session is"
    );

    let report = status_job(&client, &server.base_url, JOB)
        .await
        .expect("the job must be reported on /v1/status");
    assert_eq!(report["runs"], 1, "the run is counted: {report}");
    assert_eq!(report["acted_total"], 1, "and so is what it released");
    assert_eq!(report["last_outcome"], "ok");

    server.shutdown();
}

/// The job's period comes from config and is reported as configured — the
/// half of the wiring a behavioural test cannot see, since a job bound to
/// the wrong state or scheduled at the wrong period still reaps correctly
/// when driven by hand.
#[tokio::test]
async fn the_configured_period_reaches_the_job_and_is_reported() {
    let (server, _store) = spawn_server(Vec::new(), 60).await;
    let client = reqwest::Client::new();

    let report = status_job(&client, &server.base_url, JOB)
        .await
        .expect("registered");
    assert_eq!(report["enabled"], true, "a non-zero period schedules it");
    assert_eq!(report["period_secs"], 60);
    assert_eq!(
        report["runs"], 0,
        "and the first tick is one period in, not at boot"
    );

    server.shutdown();
}

/// Period `0` is the off switch, and an off job is **reported as off**
/// rather than omitted: "nothing is sweeping" and "nothing is registered to
/// sweep" are different faults, and an operator reading this list is
/// entitled to tell them apart. Expiry still happens at every read.
#[tokio::test]
async fn a_zero_period_reports_the_job_as_disabled_rather_than_hiding_it() {
    let (server, _store) = spawn_server(Vec::new(), 0).await;
    let client = reqwest::Client::new();

    let report = status_job(&client, &server.base_url, JOB)
        .await
        .expect("an unscheduled job is still registered and still reported");
    assert_eq!(report["enabled"], false);
    assert_eq!(report["period_secs"], 0);

    // The manual verb survives the schedule being off.
    assert_eq!(server.jobs.run_now(JOB).await, Some(Ok(0)));

    server.shutdown();
}
