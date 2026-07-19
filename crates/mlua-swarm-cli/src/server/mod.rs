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
//! Ships the skeleton + 4 MCP-mapped subcommands (`start` / `stop` /
//! `restart` / `status`) forwarding to the relocated [`launchd`] module.
//! Additional lifecycle subcommands (`install` / `uninstall` /
//! `bootstrap` / `bootout` / `logs`) and the
//! `#[cfg(not(target_os = "macos"))]` fast path arrive in a follow-up.

pub mod launchd;

use anyhow::Result;
use clap::Subcommand;
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

/// `mse server` subcommands.
///
/// The four variants below are the MCP-mapped lifecycle operations
/// (`swarm.server.start` / `stop` / `restart` / `status`); additional
/// lifecycle subcommands (`install` / `uninstall` / `bootstrap` /
/// `bootout` / `logs`) arrive in a follow-up.
#[derive(Debug, Subcommand)]
enum ServerSub {
    /// Start the `mse serve` daemon via `launchctl kickstart`.
    Start,
    /// Stop the `mse serve` daemon via `launchctl bootout`.
    Stop,
    /// Restart the `mse serve` daemon via `launchctl kickstart -k`.
    Restart,
    /// Report the `mse serve` daemon's healthz + `launchctl print` summary.
    Status,
}

/// Entry point wired from `main.rs`'s `Cmd::Server` arm.
pub async fn run(args: Args) -> Result<()> {
    let bind = args.bind.unwrap_or_else(|| launchd::DEFAULT_BIND.to_string());
    match args.sub {
        ServerSub::Start => {
            let outcome = launchd::start(&bind)
                .await
                .map_err(|s| anyhow::anyhow!("{}", s))?;
            emit(&outcome, args.json, human_start(&outcome))
        }
        ServerSub::Stop => {
            let outcome = launchd::shutdown(&bind)
                .await
                .map_err(|s| anyhow::anyhow!("{}", s))?;
            emit(&outcome, args.json, human_stop(&outcome))
        }
        ServerSub::Restart => {
            let outcome = launchd::restart(&bind)
                .await
                .map_err(|s| anyhow::anyhow!("{}", s))?;
            emit(&outcome, args.json, human_start(&outcome))
        }
        ServerSub::Status => {
            let outcome = launchd::status(&bind).await;
            emit(&outcome, args.json, human_status(&outcome))
        }
    }
}

/// Serialize `outcome` as pretty JSON when `json` is `true`, otherwise
/// print the pre-rendered one-line human summary. Both flavors go to
/// stdout; trace / diagnostics stay on stderr via `tracing`.
fn emit<T: Serialize>(outcome: &T, json: bool, human: String) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(outcome)?);
    } else {
        println!("{human}");
    }
    Ok(())
}

fn human_start(outcome: &launchd::StartOutcome) -> String {
    match outcome {
        launchd::StartOutcome::AlreadyRunning { bind } => {
            format!("bind={bind} state=already_running")
        }
        launchd::StartOutcome::Started { bind } => format!("bind={bind} state=started"),
    }
}

fn human_stop(outcome: &launchd::StopOutcome) -> String {
    format!("bind={} stopped={}", outcome.bind, outcome.stopped)
}

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
