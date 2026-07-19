//! `mse` — multi-call CLI for mlua-swarm.
//!
//! Subcommands:
//!
//! - `mse serve` — start the HTTP + WS server (Task + Enhance + Operator
//!   dispatch in one process).
//! - `mse mcp` — run the MCP adapter (stdio transport, exposes the server's
//!   task / blueprint / operator surface as MCP tools).
//! - `mse bp build` — compile-lint + emit (+ optionally register) a
//!   `.bp.lua` DSL script's built Blueprint JSON (see `bp` module doc).
//! - `mse server <subcmd>` — control the `mse serve` daemon lifecycle via
//!   launchd (`start` / `stop` / `restart` / `status`; see `server`
//!   module doc). Additional lifecycle subcommands (`install` /
//!   `uninstall` / `bootstrap` / `bootout` / `logs`) arrive in a
//!   follow-up.
//!
//! Each subcommand carries its own flag surface (see `mse <cmd> --help`).

mod bp;
mod mcp;
mod serve;
mod server;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "mse", about = "mlua-swarm CLI (serve / mcp / bp / server).", version)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Run the HTTP + WS server.
    Serve(Box<serve::Args>),
    /// Run the MCP adapter (stdio transport).
    Mcp,
    /// Build a `.bp.lua` DSL script into Blueprint JSON.
    Bp(bp::BpArgs),
    /// Control the `mse serve` daemon lifecycle (launchd-owned).
    Server(server::Args),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Serve(args) => serve::run(*args).await,
        Cmd::Mcp => mcp::run().await,
        Cmd::Bp(args) => bp::run(args).await,
        Cmd::Server(args) => server::run(args).await,
    }
}
