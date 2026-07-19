# Blueprint ref paths (`$file` / `$agent_md`)

How the linker resolves the two ref forms inside a Blueprint, the 6-tier
include cascade, and the strict opt-in at each authoring layer.

Read this before wiring `$agent_md` / `$file` refs into a `.bp.lua` or
tweaking a `mse serve` deployment's ref-resolution knobs.

## The two ref forms

Both live in the Blueprint wire JSON (`.bp.lua` output, `.json` on
disk, HTTP body to `POST /v1/blueprints/:id`). Neither survives typed
parse — the linker replaces them before `serde_json::from_value::<Blueprint>`
runs, so `Blueprint.agents` / `AgentDef` never carry a `$agent_md` /
`$file` field.

### `{"$file": "<path>"}`

Anywhere inside the Blueprint JSON, an object whose single key is
`$file` is replaced with the referenced file's contents **as a raw
string**. Typical use:

```lua
-- .bp.lua fragment
{ op = "lit", value = { ["$file"] = "prompts/system-writer.md" } }
```

- `<path>` is relative — see the cascade below for how it is resolved.
- Absolute paths and `..` parent escapes are rejected (path hygiene).

### `{"$agent_md": "<path>"}`

A structured ref that expands into a fully-populated `AgentDef` object,
not a string. Runs the referenced file through the `agent.md` loader
(`mse://guides/agent-md-authoring`) so the YAML frontmatter becomes
`AgentDef.name` / `AgentDef.profile` / `AgentDef.meta`, and the body
becomes `profile.system_prompt` verbatim.

```lua
-- .bp.lua fragment
agents = {
  {
    ["$agent_md"] = "agents/researcher.md",
    spec = { operator_ref = "main-ai" },
    verdict = { channel = "part", values = { "PASS", "BLOCKED" } },
  },
}
```

Sibling keys are shallow-merged onto the expanded `AgentDef` — so
`spec` / `verdict` above override anything the `agent.md` supplied.
Path hygiene matches `$file`: absolute paths and `..` are rejected.

## Include cascade — 6 tiers, first-hit-wins

The linker walks these directories in order for every ref path it
encounters. The first tier that contains a file at `<dir>/<path>`
wins; the error on a total miss names every tier searched, so you can
tell which one you meant to add.

| tier | source | typical author use |
|---|---|---|
| 1 | bp.lua parent directory | Files sitting next to the Blueprint. Always first. |
| 2 | in-bp `blueprint_ref_includes = { … }` | Extra dirs declared *inside* the Blueprint (relative to tier 1). Self-contained authoring — the Blueprint travels with its own resolution config. |
| 3 | env `MSE_BLUEPRINT_INCLUDES` | `:`-separated (Unix) / `;`-separated (Windows) list of absolute paths. Session-scoped override. |
| 4 | CLI `--include <DIR>` | Repeatable on `mse bp build`, `mse bp lint`, and `mse serve`. |
| 5 | server config `blueprint_ref_includes` | Set in `mlua_swarm_server.toml` under `[server]`, extended by any `--include` passed to `mse serve`. |
| 6 | bundled default (`mse` samples) | The `samples/agents/*.md` files shipped inside `mlua-swarm-cli`. CLI only — the server binary does not register a bundled default. |

Tier 5 is server-only (kicks in during `POST /v1/blueprints/:id`).
Tiers 1-4 apply to `mse bp build` and `mse bp lint`; tier 6 applies to
CLI lint / build, so an author on a fresh checkout can reference the
bundled samples (`$agent_md = "researcher.md"`) without any config.

Backward compat: `mse serve`'s pre-existing `--blueprint-ref-base`
flag stays supported — it is prepended into the tier-4 CLI include
list, so no existing deployment breaks.

## Behavior on unresolved refs

Typed `Blueprint` cannot hold refs (they are wire sugar; `AgentDef` is
`deny_unknown_fields`). So an unresolved ref is a wire-layer problem
only; each layer chooses its own strictness.

| layer | default | strict opt-in |
|---|---|---|
| `mse bp lint` | linker best-effort, verdict `WARN`, exit 0 | `--strict` — non-zero exit on any WARN/ERROR |
| `mse bp build` | linker; on fail, emit raw wire JSON with refs preserved and print WARN | `--strict-embed` — hard-fail (non-zero exit, no JSON emitted) |
| server register (`POST /v1/blueprints/:id`) | linker; on fail, HTTP 400 with a fix hint naming every include-cascade knob | server config `blueprint_strict_embed = true` — reject any request body that still carries `$file` / `$agent_md` before the linker even runs |
| dispatch (worker prompt build) | n/a — typed `Blueprint` is resolved-only by the time the compiler sees it | n/a |

= wire-layer partial preserve is legal, so a caller can hand the raw
JSON to the server and let the server's cascade do the resolution.
Typed layer, storage, and dispatch never see refs.

## `--strict-embed` naming

The flag semantic is "require refs to be **embedded** at build time,
hard-fail if any are unresolved." Not `--strict-refs`, which would
suggest refs themselves are disallowed — they are not. `mse bp build`
still accepts a Blueprint with refs by default; `--strict-embed` is
the switch that promotes the default WARN to an error.

## Path hygiene

Regardless of which tier resolves a ref, two shapes are always
rejected before any tier is walked:

- **Absolute paths** — `$file = "/etc/passwd"` errors immediately.
- **`..` parent escapes** — `$file = "../secrets.env"` errors
  immediately.

Sandboxes every ref to the subtree(s) the cascade explicitly names.
Combine with `blueprint_strict_embed = true` on the server to fully
close the "raw-ref inflight to the server" surface — refs must embed
at build time, and the server rejects the wire body as-is.

## Related resources

- `mse://guides/blueprint-authoring` — Blueprint shape, flow node
  kinds, verdict contracts.
- `mse://guides/agent-md-authoring` — `agent.md` frontmatter fields,
  size targets, `$agent_md` expansion contract.
- `mse://blueprints/samples/08-bundled-refs` — round-trip sample
  wiring the cascade end-to-end via the bundled tier.
