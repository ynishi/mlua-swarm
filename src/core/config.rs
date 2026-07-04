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
