//! Engine error type.

use crate::types::{Role, Verb};
use thiserror::Error;

/// All ways an engine operation can fail.
#[derive(Debug, Error)]
pub enum EngineError {
    /// A required lock was busy and the operation gave up without retrying.
    #[error("lock busy ({0})")]
    LockBusy(&'static str),

    /// A required lock was still busy after the configured retry budget was
    /// exhausted.
    #[error("lock busy after retry ({0})")]
    LockBusyAfterRetry(&'static str),

    /// The presented `CapToken`'s HMAC signature did not verify.
    #[error("token signature invalid")]
    BadSignature,

    /// The presented `CapToken` is past its `expire_at`.
    #[error("token expired")]
    TokenExpired,

    /// The presented `CapToken` has no uses left (`max_uses` budget spent).
    #[error("token uses exhausted")]
    TokenUsesExhausted,

    /// No server-side record exists for the token. Carries the token
    /// **fingerprint** (SHA-256 of the nonce, see `CapToken::fingerprint`)
    /// — never the nonce itself, since this message can surface in HTTP
    /// error bodies and logs (issue #14).
    #[error("token not found in store (fp={0})")]
    TokenNotFound(String),

    /// The token's `Role` is not allow-listed for the requested `Verb` (see
    /// `RoleVerbGate`).
    #[error("role violation: role={role:?} verb={verb:?}")]
    RoleViolation {
        /// The role the token was minted for.
        role: Role,
        /// The verb that was rejected.
        verb: Verb,
    },

    /// No task exists with the given id.
    #[error("task not found: {0}")]
    TaskNotFound(String),

    /// No session is attached to the task.
    #[error("session not found")]
    SessionNotFound,

    /// The resume key presented does not match any pending resume point.
    #[error("resume key not found")]
    ResumeKeyNotFound,

    /// A generic named resource (other than task/session/token) was not
    /// found.
    #[error("resource not found: {0}")]
    ResourceNotFound(String),

    /// The requested state transition is not valid from the task's current
    /// state.
    #[error("invalid state transition: {0}")]
    InvalidTransition(String),

    /// Dispatching an attempt failed; the string carries the underlying
    /// reason.
    #[error("dispatch failed: {0}")]
    DispatchFailed(String),

    /// A poll operation exceeded its deadline without observing completion.
    #[error("poll timeout")]
    PollTimeout,

    /// The task was cancelled.
    #[error("cancelled")]
    Cancelled,

    /// A sub-task spawn would exceed the configured `max_spawn_depth`.
    #[error("spawn depth exceeded: {current} >= max {max}")]
    SpawnDepthExceeded {
        /// The depth that would result from this spawn.
        current: u32,
        /// The configured maximum allowed depth.
        max: u32,
    },

    /// The presented token is bound to a different task than the one
    /// referenced by the call.
    #[error("token task mismatch: token bound to {bound}, arg was {arg}")]
    TokenTaskMismatch {
        /// The task id the token is actually bound to.
        bound: String,
        /// The task id that was passed in the call.
        arg: String,
    },

    /// Catch-all for invariant violations that don't have a dedicated
    /// variant yet.
    #[error("internal: {0}")]
    Internal(String),

    /// GH #31: writing a `SystemRefMode::File` body to
    /// `SystemRefConfig.store_dir` failed (directory creation or file
    /// write, both via `tokio::fs`).
    #[error("system_ref store write failed: {0}")]
    SystemRefWrite(#[from] std::io::Error),

    /// GH #51 — completion-time verdict-contract enforcement: a
    /// `channel: "body"` contract's completing value (or a `channel:
    /// "part"` contract's staged `"verdict"` artifact value) is not a
    /// member of the declared `values` set. Raised by the shared
    /// completion-check embedded inside `Engine::submit_worker_result_trusted`
    /// / `Engine::submit_output` — the single choke point every
    /// completion route passes through.
    #[error(
        "verdict contract violation: {value:?} is not a member of the declared values {allowed:?}"
    )]
    VerdictValueRejected {
        /// The value that failed membership.
        value: String,
        /// The contract's declared token set.
        allowed: Vec<String>,
    },

    /// GH #51 — completion-time verdict-contract enforcement: a
    /// `channel: "part"` contract's attempt completed without ever
    /// staging a `"verdict"` artifact (presence, not just membership, is
    /// checked at completion — the staging-time check only covers
    /// membership for parts that WERE staged).
    #[error("verdict contract violation: no staged \"verdict\" part found for this attempt; declared values {allowed:?}")]
    VerdictPartMissing {
        /// The contract's declared token set.
        allowed: Vec<String>,
    },
}
