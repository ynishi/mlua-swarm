# mlua-swarm

A long-running swarm engine host that compiles `flow.ir` Blueprints and
dispatches their agent steps to workers. A Blueprint declares a `flow`
(step / seq / branch / loop / fanout / try / assign nodes) plus the
`agents` it references; the engine resolves each agent to a backend
(in-process Lua, a Rust function, a child process, or an interactive
Operator) and drives the flow while recording task state.

## Install

```bash
cargo install mlua-swarm-cli
```

This installs the `mse` binary with two subcommands:

```bash
mse serve   # HTTP + WS server (tasks, Blueprint store, WS Operator sessions)
mse mcp     # MCP adapter over stdio, for AI agents (Claude Code etc.)
```

One-shot Blueprint execution is available through the `swarm_run` MCP tool
(exposed by `mse mcp`), which runs through the canonical
`TaskApplication.handle` / `TaskLaunchService::launch` path.

MCP client config:

```json
{ "command": "mse", "args": ["mcp"] }
```

#### Docker (no Rust toolchain required)

Also listed on the [MCP Registry](https://registry.modelcontextprotocol.io)
as `io.github.ynishi/mlua-swarm`:

```json
{
  "mcpServers": {
    "mlua-swarm": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm",
        "ghcr.io/ynishi/mse:latest",
        "mcp"
      ]
    }
  }
}
```

## Documentation

Documentation is served from the code itself:

- **API / architecture** — rustdoc on [docs.rs](https://docs.rs/mlua-swarm)
  (the crate root doc is the architecture overview).
- **Guides / samples / schema** — bundled MCP resources under `mse://`,
  served by `mse mcp` and always version-matched to the binary:
  `mse://guides/getting-started`, `mse://guides/blueprint-authoring`,
  `mse://guides/mcp-tool-reference`, `mse://blueprints/samples/*`, and
  the live Blueprint JSON Schema at `mse://api/blueprint-schema`
  (also available as the `bp_schema` tool).

## Workspace crates

| crate | role |
|---|---|
| `mlua-swarm` | engine core (workspace root package) |
| `mlua-swarm-schema` | Blueprint schema types |
| `mlua-swarm-server` | HTTP + WS server library |
| `mlua-swarm-cli` | the `mse` binary |

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or
[MIT License](LICENSE-MIT) at your option.
