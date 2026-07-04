//! `mse` — multi-call CLI for mlua-swarm.
//!
//! Subcommands:
//!
//! - `mse serve` — start the HTTP + WS server (Task + Enhance + Operator
//!   dispatch in one process).
//! - `mse mcp` — run the MCP adapter (stdio transport, exposes the server's
//!   task / blueprint / operator surface as MCP tools).
//!
//! Each subcommand carries its own flag surface (see `mse <cmd> --help`).

mod mcp;
mod serve;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "mse", about = "mlua-swarm CLI (serve / mcp).", version)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Run the HTTP + WS server.
    Serve(serve::Args),
    /// Run the MCP adapter (stdio transport).
    Mcp,
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
        Cmd::Serve(args) => serve::run(args).await,
        Cmd::Mcp => mcp::run().await,
    }
}
