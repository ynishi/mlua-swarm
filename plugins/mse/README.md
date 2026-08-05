# mse — Blueprint-authoring DX plugin

Claude Code plugin bundle for [mlua-swarm](https://github.com/ynishi/mlua-swarm):
a Blueprint-authoring DX set (2 agents + 2 skills) that rides the `mse`
MCP server to make Blueprint design and implementation reproducible.

- **`@mse-adviser`** — read-only design consultant. Answers "how do I
  express X in a Blueprint" grounded in `mse://api/blueprint-schema`,
  `mse://guides/*`, and `mse://blueprints/samples/*`.
- **`@bp-coder`** — isolated implementation worker. Turns a mature design
  paragraph into a Blueprint file, uses `bp_doctor` as the verify gate,
  retries up to 3 times.
- **`/mse-wake`** — load-only. Injects schema + guide list + sample
  inventory into the main-thread context.
- **`/bp-build`** — kicker. Dispatches `@bp-coder` with the finalized
  design paragraph and output path.

## Install

### Marketplace auto-discovery (recommended)

Add the mlua-swarm marketplace once:

```
/plugin marketplace add ynishi/mlua-swarm
```

Then install:

```
/plugin install mse@mlua-swarm
```

The plugin's `mcp.json` wires the `mse` MCP server automatically — you
only need the `mse` binary on `PATH`:

```bash
cargo install mlua-swarm-cli
# or:  brew install ynishi/tap/mse
# or:  https://github.com/ynishi/mlua-swarm/releases
```

### Manual install

Clone the repo and point Claude Code at the local marketplace:

```
/plugin marketplace add /path/to/mlua-swarm
/plugin install mse@mlua-swarm
```

## Workflow: wake → build → doctor

```
User idea
   │
   ▼
/mse-wake                          ← load schema + guides + samples
   │
   ├── (optional) @mse-adviser     ← schema-grounded design consultation
   │
   ▼
main-thread design conversation    ← condense into one paragraph
   │
   ▼
/bp-build "<paragraph>" --out=…    ← dispatch @bp-coder
   │
   ▼
@bp-coder                          ← draft → bp_doctor → fix (≤3 retries)
   │
   ▼
Result / Artifacts / Key observations  ← main-thread reads and decides
```

The doctor gate (`bp_doctor` diagnostics empty) is the contract. Optional
`smoke: true` on `/bp-build` runs one `swarm_run` after the doctor clears
to prove end-to-end dispatch.

## Requirements

- **`mse` binary on `PATH`** — the plugin's `mcp.json` runs `mse mcp`
  (stdio transport) as the MCP server. See the
  [mlua-swarm README](https://github.com/ynishi/mlua-swarm) for install
  options.
- **Claude Code** with plugin marketplace support.

## Notes

- Plugin version tracks independently of the `mse` crate version. The
  plugin ships from this repo (`plugins/mse/`) and is discovered via
  `.claude-plugin/marketplace.json` at the repo root.
- No new CI jobs are required: distribution rides the existing repo
  release flow, and the plugin bundle is discovered directly from GitHub.
- A refiner-style third agent (journal-driven improvement proposals) is
  deliberately out of scope for v1 — will be added once the coder loop
  has real usage traces.

## Related resources

- Lifecycle guide (Develop → Trial-run → Operate):
  `mse://guides/bp-lifecycle`
- Blueprint authoring reference: `mse://guides/blueprint-authoring`
- MCP tool reference (`bp_build` / `bp_doctor` / `swarm_run` /
  `mse_operator_*`): `mse://guides/mcp-tool-reference`
- Agent-md authoring (for `$agent_md` refs inside a Blueprint):
  `mse://guides/agent-md-authoring`

## License

MIT OR Apache-2.0 (see `LICENSE-MIT` / `LICENSE-APACHE`).
