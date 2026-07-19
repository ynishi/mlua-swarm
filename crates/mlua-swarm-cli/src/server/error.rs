//! Structured error type for the `mse server` subcommand family.
//!
//! Shared by the launchd lifecycle wrappers in
//! [`launchd`](crate::server::launchd) — every fallible pub fn returns
//! `Result<T, ServerError>`, giving the crate a single internal error
//! currency for the `install` / `uninstall` / `bootstrap` / `bootout` /
//! `start` / `stop` / `restart` / `logs` family plus the infallible
//! `status` reader.
//!
//! MCP tool handlers absorb `ServerError` via `.to_string()` — the
//! wire-visible payload is the `Display` rendering. See `crate::mcp`
//! server_control tool arms for the `map_err(|e| McpError::internal_error(
//! e.to_string(), None))` pattern.

use std::time::Duration;

/// Structured error for the `mse server` subcommand family and the shared
/// [`launchd`](crate::server::launchd) module. Every variant carries
/// enough context to render a self-explanatory `Display` line — MCP tool
/// handlers rely on that: they surface `ServerError` back to the client
/// as `e.to_string()`.
#[derive(thiserror::Error, Debug)]
pub enum ServerError {
    /// `tokio::process::Command::output().await` itself failed (missing
    /// `launchctl` binary, exec permission denied, EAGAIN, ...). Distinct
    /// from [`Self::LaunchctlFailed`] which is a non-zero exit *from*
    /// launchctl.
    #[error("launchctl exec failed: {0}")]
    LaunchctlExec(#[from] std::io::Error),

    /// `launchctl <op>` exited non-zero. `stderr` holds the combined
    /// stdout+stderr text so callers can log the full launchctl message.
    #[error("launchctl {op} failed: {stderr}")]
    LaunchctlFailed {
        /// The launchctl subcommand name (`"bootstrap"` / `"bootout"` /
        /// `"kickstart"` / `"kickstart -k"` / `"print"`).
        op: &'static str,
        /// Combined stdout+stderr from the failed launchctl invocation.
        stderr: String,
    },

    /// The launchd job is not bootstrapped (or the plist is missing). Hint
    /// text follows the `mse server install` literal per Crux #1.
    #[error("launchd job '{label}' is not bootstrapped. run `mse server install` first, then `mse server bootstrap`.")]
    MissingJob {
        /// The launchd label (e.g. `"com.mse.server"`).
        label: String,
    },

    /// healthz never came back up within the poll window after a launchctl
    /// operation that was supposed to bring it up (`start` / `restart` /
    /// `bootstrap`-through-`install`).
    #[error("healthz did not respond within {duration:?} after launchctl {op}")]
    HealthzTimeout {
        /// The launchctl subcommand name whose completion should have
        /// brought healthz up.
        op: &'static str,
        /// The poll window that expired without a healthz response.
        duration: Duration,
    },

    /// Plist template render failed (non-UTF-8 path input, or the
    /// post-render guard caught an unresolved `{{...}}` placeholder).
    #[error("render plist failed: {0}")]
    Render(String),

    /// Miscellaneous local IO failure (`create_dir_all` / `write` /
    /// `read_to_string` / `remove_file`) that we do not want to funnel
    /// through the `LaunchctlExec` variant (that one is reserved for
    /// launchctl process exec, semantic differentiation on `Display`).
    #[error("io: {0}")]
    Io(std::io::Error),

    /// The current platform is not macOS. `mse server` lifecycle
    /// operations require launchd; on Linux / Windows the top-level
    /// dispatcher returns this variant instead of attempting to shell out
    /// to `launchctl`. Only constructed under
    /// `#[cfg(not(target_os = "macos"))]`, hence the `allow(dead_code)`
    /// for macOS builds where the variant is legitimately unreachable.
    #[error("unsupported platform: mse server requires macOS launchd")]
    #[cfg_attr(target_os = "macos", allow(dead_code))]
    UnsupportedPlatform,
}
