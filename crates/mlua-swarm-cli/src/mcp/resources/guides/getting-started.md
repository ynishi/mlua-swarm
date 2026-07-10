# mse — Getting started

`mse` is the command line interface for **mlua-swarm**, a long-running swarm
engine host that compiles `flow.ir` Blueprints and dispatches their agent
steps to workers. A Blueprint declares a `flow` (step / seq / branch / loop /
fanout / try / assign nodes) plus the `agents` it references; the engine
resolves each agent to a backend (in-process Lua, a Rust function, a child
process, or an interactive Operator) and drives the flow while recording task
state.

## Entry points

`mse` has two subcommands, each with its own flag surface (`mse <cmd>
--help`):

| subcommand   | what it does                                                                 | when to use it                                                                 |
|--------------|-------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| `mse serve`  | Starts the HTTP + WS server (Task + Enhance + Operator dispatch, one process). | Long-running deployment: multiple clients, Blueprint registration/versioning, WS Operator sessions. |
| `mse mcp`    | Runs the MCP adapter over stdio, exposing Blueprint / task / Operator tools.   | Wiring mlua-swarm into an AI agent (Claude Code or any other MCP client).        |

One-shot Blueprint execution (no server, no persistence) is available through
the `swarm_run` tool exposed by `mse mcp` — it runs through the canonical
`TaskApplication.handle` / `TaskLaunchService::launch` path.

## Quickstart

Install the binary:

```bash
cargo install mlua-swarm-cli
```

This installs the `mse` binary (subcommands `serve` / `mcp`).

### `mse serve`

Settings load from a TOML config file (`~/.mse/config.toml` by default; a
missing file is not an error, built-in defaults apply) with the precedence
**CLI flag > config file > built-in default**. The built-in default bind
address is `127.0.0.1:7777`.

```bash
mse serve --bind 127.0.0.1:7777
```

Notable flags (see `mse serve --help` for the full set):

- `--config <path>` — TOML config file path.
- `--enable-enhance-flow` — merge the enhance-flow workers (patch-spawner /
  patch-applier / verifier-router / committer) into the registry.
- `--git-store-path <path>` — use a Git2-backed `BlueprintStore` instead of
  the default in-memory store (lost on restart).
- `--issue-store-path <path>` / `--enhance-setting-store-path <path>` /
  `--enhance-log-store-path <path>` / `--output-store-path <path>` —
  use a SQLite backend (via `rusqlite-isle`, thread-isolated `Connection`)
  for that store at the given path. Omit for the default in-memory
  store (lost on restart). Each flag is independent.
- `--task-store-path <path>` / `--run-store-path <path>` — override the
  SQLite path for the `TaskStore` / `RunStore`. Unlike the stores above,
  these two are **persisted by default**: omitting both flags does not
  fall back to in-memory — `mse serve` writes to
  `~/.mse/store/task.sqlite` / `~/.mse/store/run.sqlite` unless
  `--ephemeral` is set. An explicit path here always wins over
  `--ephemeral`.
- `--ephemeral` — restore the pre-issue-#35 in-memory `TaskStore`/`RunStore`
  default (records are lost on restart). Has no effect when
  `--task-store-path`/`--run-store-path` is set explicitly.
- `--blueprint-ref-base <path>` — base dir for expanding `$file` /
  `$agent_md` refs in seeded Blueprint bodies.

On boot, `mse serve` sweeps any `Run`/`Task` left `Running` from a prior
process into a terminal `Interrupted` status (reason `"server restart"`) —
persisted state never resumes an in-flight run, it is only marked so
`GET /v1/runs/:id` reports the truth after a crash or restart.

Routes served: `/v1/tasks`, `/v1/tasks/:id/runs` (re-kick), `/v1/status`
(occupancy: `{running_runs, attached_operators}`, used by the
`mlua_swarm_server_restart`/`_shutdown` MCP tools' busy-refusal guard),
`/v1/operators` (WS login flow), `/v1/blueprints`, `/v1/issues`,
`/v1/enhance-settings`, `/v1/worker/*`.

### `mse mcp`

Runs over stdio — no bind address, no flags. Point an MCP client at the
binary directly:

```json
{
  "mcpServers": {
    "mse": {
      "command": "mse",
      "args": ["mcp"]
    }
  }
}
```

Once connected, list the bundled resources (`resources/list`) or fetch one
directly, e.g. `mse://guides/blueprint-authoring` or
`mse://api/blueprint-schema`.

## Where to go next

- Sample Blueprints ready to adapt: `mse://blueprints/samples/01-pure-ctx-eval`,
  `mse://blueprints/samples/02-verdict-loop`, `mse://blueprints/samples/03-fn-override`.
- Full Blueprint authoring reference: `mse://guides/blueprint-authoring`.
- The live Blueprint JSON Schema (always in sync with the running binary):
  `mse://api/blueprint-schema`.
- All `mse mcp` tools grouped by family: `mse://guides/mcp-tool-reference`.
- Deep API docs (types, traits, module map): the crate's docs.rs page.
