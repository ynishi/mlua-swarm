//! Periodic jobs — the one place scheduled work runs in the server process.
//!
//! A [`PeriodicJob`] is a name, a period, and an async closure. [`PeriodicJobs`]
//! collects them at boot, [`PeriodicJobs::start`] gives each one a task, and the
//! returned [`PeriodicJobsHandle`] is what keeps them alive: drop it and every
//! job stops. Their state is readable from outside the process
//! ([`JobReports::snapshot`], surfaced on `GET /v1/status`), and any of them can
//! be kicked by hand ([`PeriodicJobsHandle::run_now`]) without waiting for a
//! tick — which is also how they are tested, so no test sleeps on a wall clock.
//!
//! # What may be registered here
//!
//! **A job may only apply a predicate that some non-timer path already
//! applies.** The timer decides *when* a rule is noticed. It never decides
//! *what counts* as a hit. If the only code that can say a thing is dead is the
//! job that reaps it, the job is a guess wearing a schedule, and this module is
//! the wrong home for it.
//!
//! That rule is written from the one that was removed. `31fefc1` deleted a
//! periodic stale-`Run` sweeper because its predicate — "a Run still `Running`
//! whose row nobody has written to for 3900s has lost its driver" — was stated
//! nowhere else in the system and was false: every driver self-bounds well
//! inside that threshold, so the only Runs the threshold could reach were
//! healthy ones. It generated false positives because it was **inventing a
//! liveness rule**, not because it woke up on a timer.
//!
//! Read as "we removed a sweeper, so we do not do scheduled work", that
//! removal quietly forbids a whole mechanism on the strength of one bad
//! predicate — and leaves rules that *are* stated in the model with nothing
//! implementing them. This module exists so the distinction is enforceable
//! rather than remembered: bring a predicate that already has a home, and the
//! schedule is free.
//!
//! The first resident meets that bar exactly. The 24h Operator-session horizon
//! is stated by model §4.1's state diagram, applied by
//! [`crate::operator_ws::login`] at every read of a session, and executed by the
//! same teardown a `DELETE` performs. The job adds no judgment of its own: it
//! calls that same code on the sessions nobody happened to read.
//!
//! # What the runner takes care of
//!
//! Each of these is a way a hand-rolled `tokio::spawn` loop goes wrong quietly,
//! which is the other half of why scheduled work belongs in one audited place:
//!
//! - **A tick backlog after a stall.** `tokio::time::interval` defaults to
//!   [`MissedTickBehavior::Burst`], so a laptop resuming from sleep fires one
//!   tick per missed period back-to-back. Every job here runs with
//!   [`MissedTickBehavior::Delay`]: a stall costs one late run, never a burst.
//! - **Overlapping runs.** A tick waits for the previous run of the same job —
//!   scheduled or manual — through a per-job gate, so a job that occasionally
//!   runs longer than its period degrades to "runs back to back" instead of
//!   racing itself.
//! - **A panic killing the loop forever.** A panicking tick would end its task
//!   and the job would silently never run again. Each tick is wrapped in
//!   [`AssertUnwindSafe`] + `catch_unwind`, recorded as a panic, and the loop
//!   continues. (Like the run-driver guard in [`crate::tasks`], this relies on
//!   unwinding; `panic = "abort"` would make it a no-op.)
//! - **Being invisible.** Every job reports its last start, duration, outcome
//!   and lifetime counters, including one that has never run and one that is
//!   disabled — "off" and "gone" have to look different when you are asking why
//!   nothing was cleaned up.
//! - **Being unstoppable.** The handle owns the tasks and aborts them on drop,
//!   so a job cannot outlive the server that registered it.
//!
//! # Disabling
//!
//! A period of zero registers the job and starts no task. It still appears in
//! [`JobReports::snapshot`] with `enabled: false`, and
//! [`PeriodicJobsHandle::run_now`] still runs it — an operator who turned the
//! schedule off keeps the manual verb.

use futures_util::FutureExt;
use serde::Serialize;
use std::collections::BTreeMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

/// What one run of a job produced: how many items it acted on, or why the run
/// as a whole failed.
///
/// "Acted on" is the count the job's own domain cares about (sessions reaped,
/// rows pruned) and is what the reports total up — a job that keeps finding
/// nothing to do is the normal, quiet case, and a job whose count suddenly
/// jumps is the interesting one.
///
/// `Err` is for a run that could not be completed, not for a single item that
/// could not be handled. A job that processes items independently logs the item
/// failure, leaves it for the next run, and still returns `Ok`.
pub type JobOutcome = Result<u64, String>;

/// Boxed future returned by a job body.
pub type JobFuture = Pin<Box<dyn Future<Output = JobOutcome> + Send>>;

/// A job body: called once per run, produces the future for that run.
pub type JobFn = Arc<dyn Fn() -> JobFuture + Send + Sync>;

/// One registered job: a stable name, a period, and the work.
#[derive(Clone)]
pub struct PeriodicJob {
    name: &'static str,
    period: Duration,
    run: JobFn,
    /// Held for the duration of a run so a scheduled tick and a
    /// [`PeriodicJobsHandle::run_now`] can never be inside the body together.
    gate: Arc<tokio::sync::Mutex<()>>,
}

impl PeriodicJob {
    /// Registers `f` to run every `period`.
    ///
    /// `name` is the identity the logs, the reports and `run_now` use, so it is
    /// `&'static str` on purpose: job names are wiring, not data. A zero
    /// `period` means "registered but not scheduled" (see the module doc).
    pub fn new<F, Fut>(name: &'static str, period: Duration, f: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = JobOutcome> + Send + 'static,
    {
        Self {
            name,
            period,
            run: Arc::new(move || Box::pin(f()) as JobFuture),
            gate: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// The name this job is logged, reported and kicked under.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// How long the runner waits between the end of one run and the start of
    /// the next (the wait is measured from completion — see
    /// [`MissedTickBehavior::Delay`] in the module doc).
    pub fn period(&self) -> Duration {
        self.period
    }

    /// Whether [`PeriodicJobs::start`] will give this job a task. A zero period
    /// is registered, reported and manually runnable, but never scheduled.
    pub fn is_scheduled(&self) -> bool {
        !self.period.is_zero()
    }
}

impl std::fmt::Debug for PeriodicJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeriodicJob")
            .field("name", &self.name)
            .field("period", &self.period)
            .finish_non_exhaustive()
    }
}

/// What one job has done so far, as served on `GET /v1/status`.
///
/// Every field is either a count or a wall-clock second, so the snapshot
/// survives serialisation to a caller that has no idea what the job does. A job
/// that has never run reports its declaration and `None` for everything
/// observational — that is the shape that tells you a job exists and has not
/// fired, which is different from the job being absent.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct JobReport {
    /// The registered name.
    pub name: String,
    /// The configured period in seconds; `0` when the job is not scheduled.
    pub period_secs: u64,
    /// Whether a task is ticking this job. `false` = registered with a zero
    /// period (deliberately off), not missing.
    pub enabled: bool,
    /// Runs completed, including failed and panicking ones.
    pub runs: u64,
    /// Runs that returned `Err`.
    pub errors: u64,
    /// Runs whose body panicked (the loop survived each one).
    pub panics: u64,
    /// Sum of the counts successful runs reported acting on.
    pub acted_total: u64,
    /// Unix seconds when the most recent run started.
    pub last_started_secs: Option<u64>,
    /// Unix seconds when the most recent run finished.
    pub last_finished_secs: Option<u64>,
    /// How long the most recent run took.
    pub last_duration_ms: Option<u64>,
    /// The most recent run's outcome, rendered: `"ok"` / `"error: <msg>"` /
    /// `"panic: <payload>"`.
    pub last_outcome: Option<String>,
}

impl JobReport {
    fn declared(job: &PeriodicJob) -> Self {
        Self {
            name: job.name.to_string(),
            period_secs: job.period.as_secs(),
            enabled: job.is_scheduled(),
            runs: 0,
            errors: 0,
            panics: 0,
            acted_total: 0,
            last_started_secs: None,
            last_finished_secs: None,
            last_duration_ms: None,
            last_outcome: None,
        }
    }
}

/// Live report state for every registered job.
///
/// Shared as an `Arc` between the runner tasks (writers) and whoever reads
/// them — `GET /v1/status` holds one through `AppState`. The lock is a plain
/// `std::sync::Mutex` because nothing awaits while holding it.
#[derive(Default)]
pub struct JobReports {
    inner: std::sync::Mutex<BTreeMap<&'static str, JobReport>>,
}

impl JobReports {
    fn declare(&self, job: &PeriodicJob) {
        self.with(|m| {
            m.insert(job.name, JobReport::declared(job));
        });
    }

    fn record_start(&self, name: &'static str, at_secs: u64) {
        self.with(|m| {
            if let Some(r) = m.get_mut(name) {
                r.last_started_secs = Some(at_secs);
            }
        });
    }

    fn record_finish(
        &self,
        name: &'static str,
        at_secs: u64,
        took: Duration,
        outcome: &JobOutcome,
    ) {
        self.with(|m| {
            let Some(r) = m.get_mut(name) else { return };
            r.runs += 1;
            r.last_finished_secs = Some(at_secs);
            r.last_duration_ms = Some(took.as_millis() as u64);
            match outcome {
                Ok(acted) => {
                    r.acted_total += acted;
                    r.last_outcome = Some("ok".to_string());
                }
                Err(msg) => {
                    r.errors += 1;
                    r.last_outcome = Some(format!("error: {msg}"));
                }
            }
        });
    }

    fn record_panic(&self, name: &'static str, payload: &str) {
        self.with(|m| {
            if let Some(r) = m.get_mut(name) {
                r.panics += 1;
                r.last_outcome = Some(format!("panic: {payload}"));
            }
        });
    }

    /// Every registered job's report, ordered by name.
    pub fn snapshot(&self) -> Vec<JobReport> {
        self.inner
            .lock()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    /// One job's report, or `None` when nothing is registered under that name.
    pub fn get(&self, name: &str) -> Option<JobReport> {
        self.inner.lock().ok().and_then(|m| m.get(name).cloned())
    }

    /// Runs `f` against the map, doing nothing if the lock was poisoned by a
    /// panic elsewhere. Losing a counter is not worth propagating a panic into
    /// a job runner whose entire purpose is to survive them.
    fn with<F: FnOnce(&mut BTreeMap<&'static str, JobReport>)>(&self, f: F) {
        if let Ok(mut m) = self.inner.lock() {
            f(&mut m);
        }
    }
}

/// Boot-time collection of jobs, before any of them is running.
///
/// Build it, hand [`Self::reports`] to whatever surfaces them, register the
/// jobs, then [`Self::start`].
#[derive(Default)]
pub struct PeriodicJobs {
    jobs: Vec<PeriodicJob>,
    reports: Arc<JobReports>,
}

impl PeriodicJobs {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// The report handle, available before `start` so it can be wired into
    /// state that has to exist before the jobs themselves do.
    pub fn reports(&self) -> Arc<JobReports> {
        self.reports.clone()
    }

    /// Adds a job.
    ///
    /// # Panics
    ///
    /// If `job`'s name is already registered. Registration is boot wiring and a
    /// duplicate name would make the reports and `run_now` ambiguous about
    /// which job they mean; failing at startup is the only place that mistake
    /// is cheap.
    pub fn register(&mut self, job: PeriodicJob) {
        assert!(
            !self.jobs.iter().any(|j| j.name == job.name),
            "periodic job {:?} registered twice",
            job.name
        );
        self.reports.declare(&job);
        self.jobs.push(job);
    }

    /// Spawns a task per scheduled job and returns the handle that owns them.
    ///
    /// Must be called from inside a Tokio runtime. The first tick of each job
    /// lands one period in, not immediately: the paths that run at boot (store
    /// recovery, session restore) have just done their own pass, so an
    /// immediate tick would only duplicate it.
    pub fn start(self) -> PeriodicJobsHandle {
        let mut tasks = Vec::new();
        for job in &self.jobs {
            if !job.is_scheduled() {
                tracing::info!(
                    job = job.name,
                    "periodic job registered but not scheduled (period 0); \
                     it can still be run on demand"
                );
                continue;
            }
            tracing::info!(
                job = job.name,
                period_secs = job.period.as_secs(),
                "periodic job scheduled"
            );
            let job = job.clone();
            let reports = self.reports.clone();
            tasks.push(tokio::spawn(async move {
                let mut ticker = tokio::time::interval(job.period);
                ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
                // `interval`'s first tick completes immediately; consume it so
                // the first run lands one period in.
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    // The outcome is already recorded and logged by `run_tick`;
                    // the schedule itself does not branch on it.
                    let _ = run_tick(&job, &reports).await;
                }
            }));
        }
        PeriodicJobsHandle {
            jobs: self.jobs,
            tasks,
            reports: self.reports,
        }
    }
}

/// Owns the running jobs. Dropping it stops every one of them.
pub struct PeriodicJobsHandle {
    jobs: Vec<PeriodicJob>,
    tasks: Vec<JoinHandle<()>>,
    reports: Arc<JobReports>,
}

impl PeriodicJobsHandle {
    /// The shared report state (same `Arc` [`PeriodicJobs::reports`] returned).
    pub fn reports(&self) -> Arc<JobReports> {
        self.reports.clone()
    }

    /// Every registered job's report, ordered by name.
    pub fn snapshot(&self) -> Vec<JobReport> {
        self.reports.snapshot()
    }

    /// Runs `name` once, now, and returns its outcome — `None` if no job is
    /// registered under that name.
    ///
    /// Waits for any in-flight run of the same job rather than running beside
    /// it, and is recorded in the reports exactly like a scheduled run. Works
    /// on an unscheduled (period 0) job too: turning the schedule off does not
    /// take the manual verb away.
    ///
    /// A panicking body is caught here as well, and surfaces as
    /// `Some(Err("panicked: ..."))` rather than unwinding into the caller.
    pub async fn run_now(&self, name: &str) -> Option<JobOutcome> {
        let job = self.jobs.iter().find(|j| j.name == name)?;
        Some(run_tick(job, &self.reports).await)
    }

    /// Aborts every running job. Idempotent, and implied by drop.
    pub fn shutdown(&self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Drop for PeriodicJobsHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// One run of `job`: gate, time, catch, record, log.
async fn run_tick(job: &PeriodicJob, reports: &JobReports) -> JobOutcome {
    let _gate = job.gate.lock().await;
    reports.record_start(job.name, now_secs());
    let started = Instant::now();
    let outcome = match AssertUnwindSafe((job.run)()).catch_unwind().await {
        Ok(outcome) => outcome,
        Err(payload) => {
            let payload = crate::tasks::panic_payload_to_string(payload);
            tracing::error!(
                job = job.name,
                %payload,
                "periodic job panicked; the schedule continues and the next run is unaffected"
            );
            reports.record_panic(job.name, &payload);
            Err(format!("panicked: {payload}"))
        }
    };
    let took = started.elapsed();
    reports.record_finish(job.name, now_secs(), took, &outcome);
    match &outcome {
        Ok(0) => tracing::debug!(
            job = job.name,
            took_ms = took.as_millis() as u64,
            "periodic job ran, nothing to do"
        ),
        Ok(acted) => tracing::info!(
            job = job.name,
            acted = acted,
            took_ms = took.as_millis() as u64,
            "periodic job ran"
        ),
        Err(error) => tracing::warn!(
            job = job.name,
            %error,
            took_ms = took.as_millis() as u64,
            "periodic job failed; it will be tried again on the next tick"
        ),
    }
    outcome
}

/// Wall-clock seconds since the epoch, or `0` from a clock that cannot answer.
/// Only ever reported, never compared against a threshold here.
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn counting_job(name: &'static str, period: Duration, calls: Arc<AtomicU64>) -> PeriodicJob {
        PeriodicJob::new(name, period, move || {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(1)
            }
        })
    }

    #[tokio::test]
    async fn scheduled_job_runs_repeatedly_and_reports_each_run() {
        let calls = Arc::new(AtomicU64::new(0));
        let mut jobs = PeriodicJobs::new();
        jobs.register(counting_job(
            "tick",
            Duration::from_millis(10),
            calls.clone(),
        ));
        let handle = jobs.start();

        tokio::time::sleep(Duration::from_millis(120)).await;
        handle.shutdown();

        let runs = calls.load(Ordering::SeqCst);
        assert!(runs >= 2, "expected repeated runs, saw {runs}");
        let report = handle.reports().get("tick").expect("declared");
        assert!(report.enabled);
        assert_eq!(report.runs, runs);
        assert_eq!(report.acted_total, runs);
        assert_eq!(report.errors, 0);
        assert_eq!(report.last_outcome.as_deref(), Some("ok"));
        assert!(report.last_started_secs.is_some());
        assert!(report.last_duration_ms.is_some());
    }

    #[tokio::test]
    async fn zero_period_registers_without_scheduling_and_still_runs_on_demand() {
        let calls = Arc::new(AtomicU64::new(0));
        let mut jobs = PeriodicJobs::new();
        jobs.register(counting_job("off", Duration::ZERO, calls.clone()));
        let handle = jobs.start();

        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "a zero period must not tick"
        );

        let report = handle.reports().get("off").expect("declared even when off");
        assert!(!report.enabled, "an off job is reported, not hidden");
        assert_eq!(report.runs, 0);

        assert_eq!(handle.run_now("off").await, Some(Ok(1)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(handle.reports().get("off").expect("declared").runs, 1);
    }

    #[tokio::test]
    async fn a_panicking_run_is_recorded_and_the_schedule_survives_it() {
        let calls = Arc::new(AtomicU64::new(0));
        let job = PeriodicJob::new("panicky", Duration::from_millis(10), {
            let calls = calls.clone();
            move || {
                let calls = calls.clone();
                async move {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        panic!("first run explodes");
                    }
                    Ok(0)
                }
            }
        });
        let mut jobs = PeriodicJobs::new();
        jobs.register(job);
        let handle = jobs.start();

        tokio::time::sleep(Duration::from_millis(120)).await;
        handle.shutdown();

        let report = handle.reports().get("panicky").expect("declared");
        assert_eq!(report.panics, 1, "the panic is counted");
        assert!(
            calls.load(Ordering::SeqCst) >= 2,
            "the loop must keep ticking after a panicking run"
        );
        assert_eq!(report.last_outcome.as_deref(), Some("ok"));
    }

    #[tokio::test]
    async fn runs_of_one_job_never_overlap() {
        let inside = Arc::new(AtomicU64::new(0));
        let max_inside = Arc::new(AtomicU64::new(0));
        let job = PeriodicJob::new("slow", Duration::from_millis(5), {
            let inside = inside.clone();
            let max_inside = max_inside.clone();
            move || {
                let inside = inside.clone();
                let max_inside = max_inside.clone();
                async move {
                    let concurrent = inside.fetch_add(1, Ordering::SeqCst) + 1;
                    max_inside.fetch_max(concurrent, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    inside.fetch_sub(1, Ordering::SeqCst);
                    Ok(0)
                }
            }
        });
        let mut jobs = PeriodicJobs::new();
        jobs.register(job);
        let handle = jobs.start();

        // A manual kick races the ticker on purpose: the gate covers both.
        let manual = handle.run_now("slow");
        tokio::time::sleep(Duration::from_millis(90)).await;
        let _ = manual.await;
        handle.shutdown();

        assert_eq!(
            max_inside.load(Ordering::SeqCst),
            1,
            "two runs of the same job were inside the body at once"
        );
    }

    #[tokio::test]
    async fn a_failing_run_is_counted_and_does_not_stop_the_job() {
        let calls = Arc::new(AtomicU64::new(0));
        let job = PeriodicJob::new("failing", Duration::from_millis(10), {
            let calls = calls.clone();
            move || {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err("store unavailable".to_string())
                }
            }
        });
        let mut jobs = PeriodicJobs::new();
        jobs.register(job);
        let handle = jobs.start();

        tokio::time::sleep(Duration::from_millis(120)).await;
        handle.shutdown();

        let report = handle.reports().get("failing").expect("declared");
        assert!(report.errors >= 2, "every failed run counts: {report:?}");
        assert_eq!(report.acted_total, 0);
        assert_eq!(
            report.last_outcome.as_deref(),
            Some("error: store unavailable")
        );
    }

    #[tokio::test]
    async fn dropping_the_handle_stops_the_jobs() {
        let calls = Arc::new(AtomicU64::new(0));
        let mut jobs = PeriodicJobs::new();
        jobs.register(counting_job(
            "dropped",
            Duration::from_millis(10),
            calls.clone(),
        ));
        let handle = jobs.start();
        tokio::time::sleep(Duration::from_millis(60)).await;
        let after_start = calls.load(Ordering::SeqCst);
        assert!(after_start >= 1);

        drop(handle);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            after_start,
            "no job may outlive the handle that owns it"
        );
    }

    #[tokio::test]
    async fn run_now_on_an_unknown_job_answers_none() {
        let jobs = PeriodicJobs::new();
        let handle = jobs.start();
        assert!(handle.run_now("nope").await.is_none());
        assert!(handle.snapshot().is_empty());
    }

    #[tokio::test]
    #[should_panic(expected = "registered twice")]
    async fn a_duplicate_job_name_fails_at_registration() {
        let calls = Arc::new(AtomicU64::new(0));
        let mut jobs = PeriodicJobs::new();
        jobs.register(counting_job("dup", Duration::from_secs(1), calls.clone()));
        jobs.register(counting_job("dup", Duration::from_secs(1), calls));
    }
}
