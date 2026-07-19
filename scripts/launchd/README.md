# Running `mse serve` under launchd

`com.mse.server` is a user-level LaunchAgent that daemonizes `mse serve`, so
the HTTP server's lifecycle is independent of any client (Claude Code, curl,
etc.) and launchd's `KeepAlive` restarts it after crashes.

Since GH #69 the plist template and every install / lifecycle operation are
baked into the `mse` binary itself. There are no shell scripts left in this
directory — every step below is a first-class `mse server <subcmd>`
invocation. This README exists only to describe the end-user workflow; the
canonical reference (with recovery SOPs and the MCP-tool ↔ subcommand
mapping) is the `mse://guides/server-management` MCP resource.

## Install

```bash
mse server install
```

`mse server install`:

- renders the baked plist template (with `{{HOME}}` / `{{CARGO_BIN}}` /
  `{{PROJECT_ROOT}}` substituted) and writes it to
  `~/Library/LaunchAgents/com.mse.server.plist`;
- is idempotent — an already-loaded job is booted out and re-bootstrapped
  cleanly;
- assumes `mse` is on `$CARGO_BIN` (default `$HOME/.cargo/bin`), i.e.
  `cargo install --path crates/mlua-swarm-cli` has been run at least once.

Override the two paths that vary per environment with the flags
`--cargo-bin <dir>` and `--project-root <dir>` if the defaults are wrong.

```bash
# Verify
mse server status
curl -s http://127.0.0.1:7777/v1/healthz
```

## Reload (after a config or binary change)

```bash
mse server restart
```

Server settings live in `~/.mse/config.toml`, not in the plist; editing that
file followed by `mse server restart` is enough for a config reload.

## Uninstall

```bash
mse server uninstall
```

`mse server uninstall` boots the job out and removes the installed plist
file (idempotent — missing job / missing plist both tolerated).

## MCP shortcuts

If you have `mse mcp` running as an MCP server, the same lifecycle actions
are exposed as tools:

| MCP tool                          | equivalent CLI            |
|-----------------------------------|---------------------------|
| `mlua_swarm_server_start`         | `mse server start`        |
| `mlua_swarm_server_status`        | `mse server status`       |
| `mlua_swarm_server_shutdown`      | `mse server bootout`      |
| `mlua_swarm_server_restart`       | `mse server restart`      |
| `mlua_swarm_server_bootstrap`     | `mse server bootstrap`    |
| `mlua_swarm_server_install`       | `mse server install`      |
| `mlua_swarm_server_uninstall`     | `mse server uninstall`    |

For the full subcommand reference, recovery SOPs (throttle backoff /
booted-out / uninstalled), and the idempotency contract, read
`mse://guides/server-management` via any MCP client (or
`mse mcp` `resources/read`).
