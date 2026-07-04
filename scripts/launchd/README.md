# Running `mse serve` under launchd

`com.mse.server` is a user-level LaunchAgent that daemonizes `mse serve`, so
the HTTP server's lifecycle is independent of any client (Claude Code, curl,
etc.) and launchd's `KeepAlive` restarts it after crashes.

## Install

```bash
scripts/launchd/install.sh
```

The script:

- expands `scripts/launchd/com.mse.server.plist.template` (`{{HOME}}` / `{{CARGO_BIN}}` /
  `{{PROJECT_ROOT}}`) to concrete absolute paths and writes the result to
  `~/Library/LaunchAgents/com.mse.server.plist`;
- is idempotent — an already-loaded job is booted out and re-bootstrapped;
- assumes `mse` is on `$CARGO_BIN` (default `$HOME/.cargo/bin`), i.e.
  `cargo install --path crates/mlua-swarm-cli` has been run at least once.

```bash
# Verify
launchctl print gui/$(id -u)/com.mse.server | head -20
curl -s http://127.0.0.1:7777/v1/healthz
```

## Reload (after a config or binary change)

```bash
launchctl kickstart -k gui/$(id -u)/com.mse.server
```

Server settings live in `~/.mse/config.toml`, not in the plist; editing that
file followed by the kickstart above is enough for a config reload.

## Uninstall

```bash
scripts/launchd/install.sh --uninstall
```

## MCP shortcuts

If you have `mse mcp` running as an MCP server, the same lifecycle actions
are exposed as tools: `mlua_swarm_server_start`, `mlua_swarm_server_status`,
`mlua_swarm_server_shutdown`, `mlua_swarm_server_restart`. The MCP tools
manage a job that is already installed; the initial plist install (this
directory's script) is still a shell operation.
