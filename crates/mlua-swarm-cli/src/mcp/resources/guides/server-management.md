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
`--working-dir <dir>` overrides `WorkingDirectory` (default: `~/.mse`,
the service's own state directory; created if missing;
`--project-root` is the pre-GH-#97 alias).

The default is deliberately **not** the installer's `$PWD`: a
`WorkingDirectory` that names a directory the service does not own
(typically a source checkout) makes the daemon permanently
unstartable once that directory moves — launchd fails the spawn with
`EX_CONFIG` (78) before the log sinks open, a zero-log crash loop
(GH #97). The daemon itself resolves everything it needs against
`~/.mse` and absolute config paths, so it has no use for a checkout
CWD.

That `--cargo-bin` directory is also prepended to the daemon's
`EnvironmentVariables.PATH`, which is what an in-process `agent_block`
worker's `spec.mcp_servers[].command` resolves against (the MCP server
is a child of the daemon, not of your shell) — see
`mse://guides/blueprint-authoring` § In-process agents.

```
mse server install
mse server install --cargo-bin ~/.cargo/bin --working-dir ~/.mse
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
(`launchd_state` / `launchd_pid` / `launchd_last_exit_code`), plus the
installed plist's `WorkingDirectory` probe
(`plist_working_directory` / `plist_working_directory_exists`). The
one-line human summary is
`bind=127.0.0.1:7777 up=<bool> state=<state> pid=<pid> last_exit=<code>`
(with a `working_dir=... (MISSING ...)` suffix only when the probe
fails); the `--json` flavor carries the full structured payload.

`launchd_last_exit_code` understands launchctl's annotated form
(`78: EX_CONFIG`) and reports the numeric code; `null` means launchd
has not recorded an exit (fresh bootstrap, or the job was never
spawned — e.g. `(never exited)`).

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

### Missing WorkingDirectory (`plist_working_directory_exists: false`)

Symptom
: `mlua_swarm_server_status` reports `up: false`,
  `launchd_state: "spawn scheduled"`, and
  `plist_working_directory_exists: false` (often with
  `launchd_last_exit_code: 78` — `EX_CONFIG`). `mse server logs` shows
  nothing new: launchd fails the spawn **before** the daemon's log
  sinks open, so the crash loop writes zero bytes of log.

Cause
: The plist's `WorkingDirectory` names a directory that no longer
  exists, so launchd cannot chdir and never execs the binary. Installs
  made before GH #97 baked the installer's `$PWD` (typically a source
  checkout) into this key; moving or deleting that checkout produces
  exactly this state.

Recovery (MCP-only)
: Call `mlua_swarm_server_install` — the re-rendered plist points
  `WorkingDirectory` at `~/.mse` (created if missing), and install
  boot-outs / re-bootstraps the job in the same call. Then
  `mlua_swarm_server_start`.

### Throttle backoff (state=spawn scheduled, `last_exit_code=null`)

Symptom
: `mlua_swarm_server_status` reports `up: false`,
  `launchd_state: "spawn scheduled"`, and `launchd_last_exit_code:
  null`. The daemon just exited and launchd is waiting on
  `ThrottleInterval` before respawning. (A non-null
  `launchd_last_exit_code` names the crash reason as a sysexits code —
  check it, and check `plist_working_directory_exists`, before
  assuming a plain throttle wait.)

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
       `$CARGO_BIN` and a `~/.mse` working directory (override with
       the `cargo_bin` / `working_dir` request fields).
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

Unknown keys in `config.toml` are a hard error (typo guard), so a
key must not be added before the binary that understands it is
installed — otherwise the daemon refuses to start. Order for a
key introduced by a new release: upgrade the binary first, then
add the key, then restart.

The same guard applies in reverse to a key a release **removes**:
it becomes an unknown key on upgrade, so drop it from
`config.toml` before restarting the daemon on the new binary.

### Recently added keys

| Key | CLI flag | Default | Effect |
|---|---|---|---|
| `engine_max_hold_ms` | `--engine-max-hold-ms` | `50` | Engine lock-hold guard threshold in milliseconds: how long a single state-lock operation may run before the engine reports a suspected long operation inside the lock. Raise it on a loaded host where the warning fires on healthy runs. |
| `worker_token_ttl_secs` | `--worker-token-ttl-secs` | `1800` | TTL in seconds for the worker capability tokens handed to SubAgents. A Step whose SubAgent runs longer than this fails authentication mid-flight, so raise it alongside the run TTL when running long Steps. The token leaves the process and cannot be revoked, so this TTL is the only bound on the capability — keep it as short as the workload allows. `0` is refused at startup (it would mint already-expired tokens). |
| `operator_session_sweep_secs` | `--operator-session-sweep-secs` | `300` | How often the `operator-session-expiry` job sweeps Operator sessions past the 24h horizon (`mse://guides/operator-execution-model`, "It goes on its own after 24 hours"). This is the sweep's **period, not the horizon** — turning it down does not expire sessions sooner, it only shortens how long a release waits, and every read of a session applies the horizon regardless. `0` leaves the job registered but unscheduled, which is the read-time-only behaviour. |

## Periodic jobs

`GET /v1/status` carries a `periodic_jobs` array — one entry per scheduled
job in the running server, with its period, whether it is scheduled, how
many times it has run, and the outcome and duration of its last run. A job
that has never run appears with `runs: 0`, and one turned off appears with
`enabled: false`; a name missing from the array means nothing registered
it, which is a different fault from a job that is not firing.

There is one job today, `operator-session-expiry`. A job is only allowed on
this runner if it applies a rule some non-timer path already applies — the
schedule changes *when* something is noticed, never *what counts* — so a
new entry appearing here should always be traceable to a rule stated
elsewhere.

## Remote hosting

`mse serve` is loopback-first: binding anything other than `127.0.0.1` /
`::1` (so `--bind 0.0.0.0:7777`, an external IP, …) requires the L0
perimeter access token, and the server **refuses to start** without one:

```bash
mse serve --bind 0.0.0.0:7777 --access-token "$TOKEN"   # or MSE_ACCESS_TOKEN / config `access_token`
```

Every client then needs `MSE_ACCESS_TOKEN` set (the mse-mcp tools,
`mse bp push`, the operator WS client, and worker fetch/submit attach the
`X-MSE-Access-Token` header automatically). Two more knobs matter on a
remote host:

- **Pin `token_secret`** (config, `--token-secret`, or env
  `MSE_TOKEN_SECRET`): unpinned, it is regenerated every boot and a
  restart invalidates all outstanding worker CapTokens. The server warns
  about this on non-loopback binds.
- **TLS terminates at the platform edge / reverse proxy** — the server
  speaks plain HTTP behind it. The operator WS client maps `https://` in
  `MSE_HTTP` to `wss://` on its own.

Container platforms that cannot pass flags can drive everything through
env: `MSE_BIND`, `MSE_ACCESS_TOKEN`, `MSE_TOKEN_SECRET` (each is
overridden by its flag, and overrides the config file). A complete
single-machine reference deployment (Fly.io: volume, secrets, TLS edge,
autostop off) ships in the repo under `contrib/fly/`.

Credential vocabulary, the route × layer matrix, and the full rationale
live in `mse://guides/auth-token-model`.

## See also

- `mse://guides/auth-token-model` — the three credential layers (L0
  access token / L1 identity / L2 capability) behind the remote-hosting
  rules above.
- `mse://guides/getting-started` — top-level entry point (serve /
  mcp / run) and quickstart snippets.
- `mse://guides/mcp-tool-reference` — every `mse mcp` tool grouped by
  family (`mlua_swarm_server_*` is one of them).
- `mse://api/mcp-tools` — live JSON Schemas for each MCP tool's
  request body, including the seven `mlua_swarm_server_*` tools.
- `mse://api/http-endpoints` — HTTP wire-body JSON Schemas for the
  `/v1/blueprints`, `/v1/tasks`, `/v1/runs`, and `/v1/worker` families
  (`GET /v1/healthz` is what `mse server status` probes under the hood).
