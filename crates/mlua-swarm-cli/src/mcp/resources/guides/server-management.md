# `mse server` — Server lifecycle management

`mse server <subcmd>` is the CLI front-end to the `mse serve` HTTP
daemon's lifecycle (launchd-owned). It replaces the shell installer
that predates the `mse` binary: the plist template is now baked into
the binary and every install / lifecycle step is a first-class
subcommand.

The same operations are also exposed as `mse mcp` tools, so an MCP
client can drive recovery end-to-end without shelling out. See
[`## MCP tool ↔ mse server mapping`](#mcp-tool--mse-server-mapping)
below.

## Platform note

launchd is macOS-only, and this whole family is macOS-scoped by
design. Non-macOS callers receive `ServerError::UnsupportedPlatform`
from every subcommand and every MCP tool. Linux (systemd / OpenRC) and
Windows (Service Control Manager) integrations are Non-goals for this
release; they may land later behind different subcommand families.

## Subcommand reference

Nine subcommands cover the full lifecycle. Each subcommand accepts
`--json` (pretty-printed JSON on stdout instead of the one-line human
summary) and `--bind host:port` (override the healthz endpoint; default
is the value baked into `launchd::DEFAULT_BIND`). Both are `global` on
`clap`, so `mse server --json status` and `mse server status --json`
are both valid.

### `install`

Render the baked plist template and install it as the
`com.mse.server` LaunchAgent at
`~/Library/LaunchAgents/com.mse.server.plist`. Idempotent: an
already-loaded job is booted out and re-bootstrapped cleanly. Flags:
`--cargo-bin <dir>` overrides the daemon's binary directory
(default: `$CARGO_BIN` env, else `$HOME/.cargo/bin`);
`--project-root <dir>` overrides `WorkingDirectory` (default: `$PWD`).

```
mse server install
mse server install --cargo-bin ~/.cargo/bin --project-root ~/projects/mlua-swarm
```

### `uninstall`

Boot the job out and remove the installed plist file. Idempotent:
missing job and missing plist are both tolerated.

```
mse server uninstall
```

### `bootstrap`

`launchctl bootstrap gui/<uid> <plist>` — load the LaunchAgent from
the already-installed plist without re-writing it. Idempotent: an
already-loaded job returns success.

```
mse server bootstrap
```

### `bootout`

`launchctl bootout gui/<uid>/com.mse.server` — unload the LaunchAgent
without removing the plist file. Idempotent: a missing job returns
success. Useful when you want the plist to stay on disk (so the next
`bootstrap` is a no-op) but want the daemon fully stopped.

```
mse server bootout
```

### `start`

Start the daemon via `launchctl kickstart`. If the LaunchAgent is not
currently loaded, `start` transparently bootstraps it first
(auto-recovery for the "booted-out but not uninstalled" state; see
[`Recovery SOPs`](#recovery-sops) below).

```
mse server start
```

### `stop`

Stop the daemon via `launchctl bootout` (same as `bootout`, exposed
under the `stop` verb for symmetry with `start`).

```
mse server stop
```

### `restart`

Restart the daemon via `launchctl kickstart -k`. Like `start`, this
auto-bootstraps first if the job is not currently loaded.

```
mse server restart
```

### `status`

Report the daemon's health: reachability of `GET /v1/healthz` on
`--bind`, plus the `launchctl print` summary
(`launchd_state` / `launchd_pid` / `launchd_last_exit_code`). The
one-line human summary is
`bind=127.0.0.1:7777 up=<bool> state=<state> pid=<pid> last_exit=<code>`;
the `--json` flavor carries the full structured payload.

```
mse server status
mse server status --json
```

### `logs`

Tail the launchd-managed log sinks
(`/tmp/mse-server.stdout` and `/tmp/mse-server.stderr`). Flag: `-n /
--tail <N>` sets the number of trailing lines to include from each
sink (default: 20).

```
mse server logs
mse server logs --tail 100
```

## MCP tool ↔ mse server mapping

Every lifecycle operation exposed as an MCP tool has a matching
`mse server` subcommand. The mapping is one-to-one, so an MCP client
can compose the same recovery flowcharts a human operator would run
from the shell:

| MCP tool                       | `mse server` subcmd     |
|--------------------------------|-------------------------|
| `mlua_swarm_server_start`      | `mse server start`      |
| `mlua_swarm_server_status`     | `mse server status`     |
| `mlua_swarm_server_shutdown`   | `mse server bootout`    |
| `mlua_swarm_server_restart`    | `mse server restart`    |
| `mlua_swarm_server_bootstrap`  | `mse server bootstrap`  |
| `mlua_swarm_server_install`    | `mse server install`    |
| `mlua_swarm_server_uninstall`  | `mse server uninstall`  |

The MCP tools are thin forwarders over the same `launchd::*` async
functions the CLI dispatches to — they emit the same structured
outcomes, so the recovery SOPs below apply identically whether you
drive them from a shell or from an MCP client.

## Idempotency guarantee

Every subcommand (and every matching MCP tool) is idempotent:

- `install` over an already-installed plist re-installs cleanly
  (bootout + write + bootstrap) rather than erroring;
- `uninstall` on an already-uninstalled system succeeds with a "no-op"
  outcome — missing plist and missing job are both tolerated;
- `bootstrap` on an already-loaded job returns `AlreadyLoaded` rather
  than the raw `launchctl` "Bootstrap failed: 37" error;
- `bootout` on a missing job returns success;
- `start` / `restart` auto-bootstrap first if the job is not currently
  loaded, so the "installed but booted out" recovery path is a single
  MCP call.

This means recovery from any transient failure state can be attempted
by re-running the same tool; you do not need to inspect the current
state and pick a different subcommand.

## Recovery SOPs

These SOPs cover the three states an MCP client (or human operator) is
most likely to hit. Each SOP is closed under the MCP tool surface —
you do not need shell access for recovery.

### Throttle backoff (state=spawn scheduled, `last_exit_code=null`)

Symptom
: `mlua_swarm_server_status` reports `up: false`,
  `launchd_state: "spawn scheduled"`, and `launchd_last_exit_code:
  null`. The daemon just exited and launchd is waiting on
  `ThrottleInterval` before respawning.

Cause
: The plist declares `ThrottleInterval=10`, so launchd enforces at
  least ten seconds between spawn attempts. This is intentional (it
  keeps a crash-looping daemon from monopolizing the machine) and is a
  Non-goal to change from an MCP recovery path.

Recovery (MCP-only)
: Wait ten seconds, then call `mlua_swarm_server_restart`:
    1. Call `mlua_swarm_server_status` — confirm
       `state: "spawn scheduled"` and
       `last_exit_code: null`.
    2. Wait ten seconds (or a little longer for safety).
    3. Call `mlua_swarm_server_restart` — `launchctl kickstart -k`
       forces the respawn.
    4. Call `mlua_swarm_server_status` — confirm `up: true` and
       `state: "running"`.

If the state persists after `restart`, tail the log sinks with
`mse server logs --tail 100` to inspect the crash reason before
retrying.

### Booted-out (`Could not find service` error)

Symptom
: `mlua_swarm_server_start` or `mlua_swarm_server_restart` fails with
  `Could not find service "com.mse.server" in domain for port`
  (or the structured equivalent from the MCP tool). The plist is
  still on disk but the LaunchAgent is not currently loaded.

Cause
: A previous `mlua_swarm_server_shutdown` (or `mse server bootout`,
  or a `launchctl bootout` from the shell) unloaded the job. The
  plist file was left in place, but `launchctl kickstart` cannot
  reach a job that is not loaded.

Recovery (MCP-only)
: Either single-step via the auto-bootstrap fallback, or two-step
  explicitly:
    - **Single-step**: Call `mlua_swarm_server_start` — the start
      path transparently bootstraps first when the job is missing,
      then kicks it.
    - **Two-step**: Call `mlua_swarm_server_bootstrap` (returns
      `Bootstrapped` or `AlreadyLoaded`), then
      `mlua_swarm_server_start`.

Both paths converge on the same running state; the single-step is
preferred for concise recovery flowcharts.

### Uninstalled (plist missing)

Symptom
: `mlua_swarm_server_bootstrap` or `mlua_swarm_server_start` fails
  with a plist-not-found error (or `mlua_swarm_server_status`
  reports `state: null` and `up: false`).

Cause
: `mse server uninstall` (or `mlua_swarm_server_uninstall`) removed
  the plist, or the system was never installed. Nothing on disk for
  launchd to load.

Recovery (MCP-only)
: Call `mlua_swarm_server_install`. This tool is idempotent and
  handles the full install-and-bootstrap sequence in one call:
    1. Render the baked plist template with the current
       `$CARGO_BIN` / `$PWD` (override with the `cargo_bin` /
       `project_root` request fields).
    2. Write it to `~/Library/LaunchAgents/com.mse.server.plist`.
    3. Bootstrap the LaunchAgent.
    4. Return `InstallOutcome` with the resolved `plist_path` and
       an inner `bootstrap` field of either `Bootstrapped` or
       `AlreadyLoaded`.

After `install` returns, call `mlua_swarm_server_start` (or
`mlua_swarm_server_status`) to verify the daemon is reachable.

## Configuration reload

Runtime configuration lives in `~/.mse/config.toml`, not in the plist
file. To pick up config changes, call
`mlua_swarm_server_restart` (or `mse server restart` from the
shell) — the daemon rereads the file on start-up. The plist file only
carries process-level knobs (working directory, `KeepAlive`,
`ThrottleInterval`, log sinks) and does not need re-installing for a
config-only change.

## See also

- `mse://guides/getting-started` — top-level entry point (serve /
  mcp / run) and quickstart snippets.
- `mse://guides/mcp-tool-reference` — every `mse mcp` tool grouped by
  family (`mlua_swarm_server_*` is one of them).
- `mse://api/mcp-tools` — live JSON Schemas for each MCP tool's
  request body, including the seven `mlua_swarm_server_*` tools.
- `mse://api/http-endpoints` — HTTP wire-body JSON Schemas (`GET
  /v1/healthz` is what `mse server status` probes under the hood).
