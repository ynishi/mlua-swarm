//! `mse server <subcommand>` — CLI front-end to the `mse serve` HTTP
//! daemon's lifecycle (launchd-owned).
//!
//! Thin adapter over [`launchd`]: each subcommand forwards to the matching
//! `launchd::*` async function and renders the returned outcome as either
//! a one-line human-readable summary (default) or pretty-printed JSON
//! (`--json`). Trace / diagnostic output goes to stderr via
//! `tracing_subscriber` (initialized in `main.rs`); stdout carries only
//! the subcommand's outcome payload.
//!
//! Ships the full 9-subcommand lifecycle family (`install` / `uninstall`
//! / `bootstrap` / `bootout` / `start` / `stop` / `restart` / `status` /
//! `logs`) forwarding to the [`launchd`] module. Non-macOS callers hit
//! the `#[cfg(not(target_os = "macos"))]` fast path in [`run`] and
//! receive `ServerError::UnsupportedPlatform` — launchd is a macOS-only
//! service manager, so the whole family is macOS-scoped by design.

pub mod error;
pub mod launchd;

use anyhow::Result;
use clap::Subcommand;
#[cfg(target_os = "macos")]
use serde::Serialize;

/// `mse server <subcommand>`.
///
/// `--bind` and `--json` are declared `global` so they can be passed
/// after either `server` or the inner subcommand token (e.g.
/// `mse server --json status` and `mse server status --json` both work).
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(subcommand)]
    sub: ServerSub,
    /// `host:port` the `mse serve` daemon is expected to be listening on.
    /// Defaults to [`launchd::DEFAULT_BIND`].
    #[clap(long, global = true)]
    bind: Option<String>,
    /// Emit the subcommand's outcome as pretty-printed JSON on stdout
    /// instead of the default one-line human-readable summary.
    #[clap(long, global = true)]
    json: bool,
}

/// `mse server` subcommands — the launchd lifecycle family.
///
/// - `install` / `uninstall` — one-shot plist install / removal
///   (idempotent).
/// - `bootstrap` / `bootout` — load / unload the LaunchAgent without
///   touching the plist file (idempotent).
/// - `start` / `stop` / `restart` — MCP-mapped lifecycle operations
///   (`swarm.server.start` / `stop` / `restart`).
/// - `status` — healthz + `launchctl print` summary.
/// - `logs` — tail the `/tmp/mse-server.{stdout,stderr}` sinks.
#[derive(Debug, Subcommand)]
enum ServerSub {
    /// Render the baked plist template and install it as the
    /// `com.mse.server` LaunchAgent (idempotent — re-installs cleanly
    /// over an already-loaded job).
    Install {
        /// Cargo install target dir (default: `$CARGO_BIN` env, else
        /// `$HOME/.cargo/bin`).
        #[clap(long)]
        cargo_bin: Option<std::path::PathBuf>,
        /// `WorkingDirectory` for the daemon (default: `$PWD` env, else
        /// the process's current working directory).
        #[clap(long)]
        project_root: Option<std::path::PathBuf>,
    },
    /// `bootout` the job + remove the installed plist file (idempotent —
    /// missing job / missing plist both tolerated).
    Uninstall,
    /// `launchctl bootstrap gui/<uid> <plist>` — load the LaunchAgent
    /// (idempotent — already-loaded returns success).
    Bootstrap,
    /// `launchctl bootout gui/<uid>/com.mse.server` — unload the
    /// LaunchAgent (idempotent — missing job returns success).
    Bootout,
    /// Start the `mse serve` daemon via `launchctl kickstart`.
    Start,
    /// Stop the `mse serve` daemon via `launchctl bootout`.
    Stop,
    /// Restart the `mse serve` daemon via `launchctl kickstart -k`.
    Restart,
    /// Report the `mse serve` daemon's healthz + `launchctl print` summary.
    Status,
    /// Tail the launchd-managed log sinks
    /// (`/tmp/mse-server.{stdout,stderr}`).
    Logs {
        /// Number of trailing lines to include from each sink (default:
        /// 20).
        #[clap(long, short = 'n')]
        tail: Option<usize>,
    },
}

/// Entry point wired from `main.rs`'s `Cmd::Server` arm.
pub async fn run(args: Args) -> Result<()> {
    #[cfg(not(target_os = "macos"))]
    {
        // launchd is macOS-only. Rather than pretending to work by
        // shelling out to a non-existent `launchctl`, fail fast with a
        // structured error the caller can surface as-is.
        let _ = &args;
        return Err(error::ServerError::UnsupportedPlatform.into());
    }
    #[cfg(target_os = "macos")]
    {
        run_macos(args).await
    }
}

#[cfg(target_os = "macos")]
async fn run_macos(args: Args) -> Result<()> {
    let bind = args
        .bind
        .unwrap_or_else(|| launchd::DEFAULT_BIND.to_string());
    match args.sub {
        ServerSub::Install {
            cargo_bin,
            project_root,
        } => {
            let outcome = launchd::install(cargo_bin.as_deref(), project_root.as_deref())
                .await
                .map_err(anyhow::Error::from)?;
            emit(&outcome, args.json, human_install(&outcome))
        }
        ServerSub::Uninstall => {
            let outcome = launchd::uninstall().await.map_err(anyhow::Error::from)?;
            emit(&outcome, args.json, human_uninstall(&outcome))
        }
        ServerSub::Bootstrap => {
            let outcome = launchd::bootstrap().await.map_err(anyhow::Error::from)?;
            emit(&outcome, args.json, human_bootstrap(&outcome))
        }
        ServerSub::Bootout => {
            let outcome = launchd::bootout(&bind).await.map_err(anyhow::Error::from)?;
            emit(&outcome, args.json, human_stop(&outcome))
        }
        ServerSub::Start => {
            let outcome = launchd::start(&bind).await.map_err(anyhow::Error::from)?;
            emit(&outcome, args.json, human_start(&outcome))
        }
        ServerSub::Stop => {
            let outcome = launchd::shutdown(&bind)
                .await
                .map_err(anyhow::Error::from)?;
            emit(&outcome, args.json, human_stop(&outcome))
        }
        ServerSub::Restart => {
            let outcome = launchd::restart(&bind).await.map_err(anyhow::Error::from)?;
            emit(&outcome, args.json, human_start(&outcome))
        }
        ServerSub::Status => {
            let outcome = launchd::status(&bind).await;
            emit(&outcome, args.json, human_status(&outcome))
        }
        ServerSub::Logs { tail } => {
            let outcome = launchd::logs(tail).await.map_err(anyhow::Error::from)?;
            emit(&outcome, args.json, human_logs(&outcome))
        }
    }
}

/// Serialize `outcome` as pretty JSON when `json` is `true`, otherwise
/// print the pre-rendered one-line human summary. Both flavors go to
/// stdout; trace / diagnostics stay on stderr via `tracing`.
#[cfg(target_os = "macos")]
fn emit<T: Serialize>(outcome: &T, json: bool, human: String) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(outcome)?);
    } else {
        println!("{human}");
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn human_start(outcome: &launchd::StartOutcome) -> String {
    match outcome {
        launchd::StartOutcome::AlreadyRunning { bind } => {
            format!("bind={bind} state=already_running")
        }
        launchd::StartOutcome::Started { bind } => format!("bind={bind} state=started"),
    }
}

#[cfg(target_os = "macos")]
fn human_stop(outcome: &launchd::StopOutcome) -> String {
    format!("bind={} stopped={}", outcome.bind, outcome.stopped)
}

#[cfg(target_os = "macos")]
fn human_status(outcome: &launchd::StatusOutcome) -> String {
    let state = outcome.launchd_state.as_deref().unwrap_or("unknown");
    let pid = outcome
        .launchd_pid
        .map(|p| p.to_string())
        .unwrap_or_else(|| "none".to_string());
    let last_exit = outcome
        .launchd_last_exit_code
        .map(|c| c.to_string())
        .unwrap_or_else(|| "none".to_string());
    format!(
        "bind={} up={} state={state} pid={pid} last_exit={last_exit}",
        outcome.bind, outcome.up
    )
}

#[cfg(target_os = "macos")]
fn human_install(outcome: &launchd::InstallOutcome) -> String {
    let bootstrap_state = match &outcome.bootstrap {
        launchd::BootstrapOutcome::Bootstrapped { .. } => "bootstrapped",
        launchd::BootstrapOutcome::AlreadyLoaded { .. } => "already_loaded",
    };
    format!(
        "installed: {} (bootstrap={bootstrap_state})",
        outcome.plist_path.display()
    )
}

#[cfg(target_os = "macos")]
fn human_uninstall(outcome: &launchd::UninstallOutcome) -> String {
    format!("uninstalled: {}", outcome.plist_path.display())
}

#[cfg(target_os = "macos")]
fn human_bootstrap(outcome: &launchd::BootstrapOutcome) -> String {
    match outcome {
        launchd::BootstrapOutcome::Bootstrapped { plist_path } => {
            format!("bootstrapped: {}", plist_path.display())
        }
        launchd::BootstrapOutcome::AlreadyLoaded { plist_path } => {
            format!("already_loaded: {}", plist_path.display())
        }
    }
}

#[cfg(target_os = "macos")]
fn human_logs(outcome: &launchd::LogsOutcome) -> String {
    format!(
        "stdout={} ({} lines) stderr={} ({} lines)",
        outcome.stdout_path.display(),
        outcome.stdout_tail.len(),
        outcome.stderr_path.display(),
        outcome.stderr_tail.len()
    )
}
