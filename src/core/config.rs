//! EngineCfg + LongHoldConfig.

use std::time::Duration;

fn random_token_secret() -> Vec<u8> {
    let mut buf = vec![0u8; 32];
    getrandom::fill(&mut buf).expect("OS RNG unavailable");
    buf
}

/// Lock acquisition + max-hold guard configuration.
#[derive(Debug, Clone)]
pub struct EngineCfg {
    /// When `true`, `Engine::with_state` never retries on a busy lock — it
    /// fails fast with `EngineError::LockBusy` on the very first
    /// `try_lock` miss instead of backing off and retrying.
    pub try_only: bool,
    /// Max number of `try_lock` retries before giving up with
    /// `EngineError::LockBusyAfterRetry` (ignored when `try_only = true`).
    pub max_retry: u32,
    /// Linear backoff step (ms) between retries; the sleep duration for
    /// attempt `n` is `backoff_ms_step * (n + 1)`.
    pub backoff_ms_step: u64,
    /// R4 guard threshold: if a single `with_state` closure holds the lock
    /// longer than this (ms), `with_state` panics — a signal that a long
    /// operation leaked inside the lock in violation of the R3 discipline.
    pub max_hold_ms: u128,
    /// HMAC secret used by `TokenSigner` to sign/verify `CapToken`s.
    ///
    /// `Default` generates this fresh (32 random bytes from the OS RNG) on
    /// every call when the caller does not supply one — set it explicitly
    /// for tokens to stay valid across restarts, or when multiple
    /// independently-constructed engines/signers must accept each other's
    /// tokens.
    pub token_secret: Vec<u8>,
    /// Long-hold session tuning (idle keepalive, heartbeat cadence).
    pub long_hold: LongHoldConfig,
    /// Worker recursive spawn depth ceiling (guards against unbounded spawn).
    ///
    /// When `Ctx.meta.runtime["spawn_depth"]` has already reached this value
    /// and a Worker token tries to call `start_task`, the engine raises
    /// `EngineError::SpawnDepthExceeded`. `0` = root (a task launched
    /// directly by an Operator); `4` = default, which allows four levels of
    /// nested sub-tasks.
    pub max_spawn_depth: u32,
    /// GH #31: threshold/mode/storage tuning for delivering an oversized
    /// baked `system_prompt` by reference (`WorkerPayload.system_ref`)
    /// instead of inline (`WorkerPayload.system`). See
    /// [`SystemRefConfig`].
    pub system_ref: SystemRefConfig,
    /// Policy that governs how submit-time projection sinks react when a
    /// fail-open condition is encountered (missing `work_dir`/
    /// `project_root`, `OutputStore` write error, adapter materialize
    /// error, state lookup error). Default [`CheckPolicy::Warn`]
    /// preserves the pre-existing warn-and-continue behaviour of every
    /// call site — see [`CheckPolicy`] for the semantics of the other
    /// two modes and [`apply_check_policy`](crate::core::engine::apply_check_policy)
    /// for the shared decision helper. Per-run override is threaded via
    /// `POST /v1/tasks` (subtask-1c).
    pub check_policy: CheckPolicy,
}

/// How a submit-time projection sink reacts when a fail-open condition
/// is encountered.
///
/// Fail-open conditions include: `work_dir` / `project_root` unresolved,
/// `OutputStore` write error, `FileProjectionAdapter::materialize_submission`
/// error, and state lookup error. Each call site inside
/// `Engine::materialize_final_submission` /
/// `Engine::materialize_artifact_submission` currently logs a
/// `tracing::warn!` and returns without materializing the file /
/// dual-write; `CheckPolicy` is the first-class knob that lets a caller
/// opt into a different reaction without changing that behaviour by
/// default.
///
/// The three modes are (a) [`CheckPolicy::Silent`] — no log, no error,
/// operation continues; (b) [`CheckPolicy::Warn`] — log warn (existing
/// message literal preserved), no error, operation continues (the
/// default = pre-existing behaviour); (c) [`CheckPolicy::Strict`] — log
/// the same warn AND return
/// [`EngineError::CheckPolicyStrict`](crate::core::errors::EngineError::CheckPolicyStrict)
/// so the caller can fail the step / launch fast. When Strict returns
/// an error, the underlying `OutputStore` may already have appended
/// (dual-write side-effect is not rolled back) — this "state dirty on
/// fail" semantics is intentional: the append happens **before** the
/// fail-open branch runs, so Strict surfaces the mismatch instead of
/// hiding it.
#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Default,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum CheckPolicy {
    /// Skip both the log warn and the error path — completely silent.
    /// The operation continues (fail-open is still in effect).
    Silent,
    /// Log a `tracing::warn!` with the call site's existing message and
    /// continue (fail-open). Default — byte-identical to the
    /// pre-`CheckPolicy` behaviour of every submit-time projection sink
    /// code path.
    #[default]
    Warn,
    /// Log the same warn AND return
    /// [`EngineError::CheckPolicyStrict`](crate::core::errors::EngineError::CheckPolicyStrict).
    /// A caller that has opted in can fail the step / launch fast
    /// instead of proceeding with a partially-realized submission.
    Strict,
}

impl EngineCfg {
    /// Strict variant: `try_only = true` (no retry/backoff) and a tight
    /// `max_hold_ms = 10`. Useful for tests that want lock contention to
    /// fail fast rather than silently wait.
    pub fn strict() -> Self {
        Self {
            try_only: true,
            max_retry: 0,
            backoff_ms_step: 0,
            max_hold_ms: 10,
            ..Self::default()
        }
    }

    /// Relaxed variant: higher `max_retry` / `backoff_ms_step` /
    /// `max_hold_ms` than the default, for environments where lock
    /// contention is expected to be more frequent or operations slower.
    pub fn relaxed() -> Self {
        Self {
            try_only: false,
            max_retry: 10,
            backoff_ms_step: 50,
            max_hold_ms: 200,
            ..Self::default()
        }
    }
}

impl Default for EngineCfg {
    /// Baseline configuration: bounded retry with backoff, a generous
    /// (but non-zero) `max_hold_ms`, and `max_spawn_depth = 4`.
    /// `token_secret` is generated fresh per call — see the field doc.
    fn default() -> Self {
        Self {
            try_only: false,
            max_retry: 3,
            backoff_ms_step: 10,
            max_hold_ms: 50,
            token_secret: random_token_secret(),
            long_hold: LongHoldConfig::default(),
            max_spawn_depth: 4,
            system_ref: SystemRefConfig::default(),
            check_policy: CheckPolicy::default(),
        }
    }
}

/// GH #31: server-side config for how a baked `system_prompt` too large
/// to inline is delivered instead — see `Engine::fetch_worker_payload`'s
/// threshold branch (`crate::core::engine`) for where this is consumed.
/// Single server-side setting, not per-request: every fetch of a given
/// `(task_id, attempt)` sees the same `mode` and `threshold_bytes`.
#[derive(Debug, Clone)]
pub struct SystemRefConfig {
    /// Byte length of the baked `system` string above which
    /// `Engine::fetch_worker_payload{,_trusted}` switches from inlining
    /// the value (`WorkerPayload.system`) to a reference
    /// (`WorkerPayload.system_ref`). Default (`25 * 1024`, 25 KiB)
    /// matches `bp_doctor`'s existing WARN threshold
    /// (`AGENT_MD_DEFAULT_WARN_BYTES` in
    /// `crates/mlua-swarm-cli/src/mcp.rs`) — the same SubAgent
    /// context-window headroom rationale that threshold documents
    /// applies here to inline-vs-reference delivery.
    pub threshold_bytes: usize,
    /// Which [`crate::types::SystemRefMode`] an over-threshold response
    /// uses to deliver its content.
    pub mode: crate::types::SystemRefMode,
    /// Directory `SystemRefMode::File` writes rendered `system` bodies
    /// into (`<store_dir>/<task_id>-<attempt>.md`). No eviction policy —
    /// files accumulate for the process lifetime (explicitly out of
    /// scope; see the risk note on `Engine::fetch_worker_payload`).
    pub store_dir: std::path::PathBuf,
}

impl Default for SystemRefConfig {
    /// `threshold_bytes = 25 * 1024`, `mode = SystemRefMode::File`,
    /// `store_dir = std::env::temp_dir().join("mse-system-ref")`.
    fn default() -> Self {
        Self {
            threshold_bytes: 25 * 1024,
            mode: crate::types::SystemRefMode::File,
            store_dir: std::env::temp_dir().join("mse-system-ref"),
        }
    }
}

/// Tuning for long-running (suspend/resume-capable) sessions and tasks —
/// how long a poll/suspend may hold, how often heartbeats are expected,
/// and whether idle tasks are kept alive across a detach.
#[derive(Debug, Clone)]
pub struct LongHoldConfig {
    /// Default wait duration used by long-poll style waits when the
    /// caller does not specify one explicitly.
    pub default_hold: Duration,
    /// Upper bound on how long a single suspend/poll wait may block.
    pub max_hold: Duration,
    /// Expected cadence of `Engine::heartbeat` calls from an attached
    /// session; consumed by `Engine::start_detach_loop`.
    pub heartbeat_interval: Duration,
    /// Number of missed heartbeat intervals tolerated before the detach
    /// loop flips a session's `attached` flag to `false`.
    pub heartbeat_miss_threshold: u32,
    /// When `true`, a task survives (its state is retained) across a
    /// session detach, so a later reattach can resume it in place.
    pub keepalive_on_idle: bool,
}

impl Default for LongHoldConfig {
    fn default() -> Self {
        Self {
            default_hold: Duration::from_secs(3600),
            max_hold: Duration::from_secs(48 * 3600),
            heartbeat_interval: Duration::from_secs(300),
            heartbeat_miss_threshold: 3,
            keepalive_on_idle: true,
        }
    }
}
