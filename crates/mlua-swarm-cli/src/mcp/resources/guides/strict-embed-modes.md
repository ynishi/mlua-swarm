# mse — The two `strict-embed` layers (build-time vs register-time)

Two independent switches share the token `strict-embed`. Both concern
`$file` / `$agent_md` refs; they live in different layers, trigger at
different points, and neither implies the other. Setting one and
expecting the other's effect is the misconfiguration this guide exists
to prevent (GH #78 P1b).

## Side-by-side

| | client-side (build) | server-side (register) |
|---|---|---|
| **flag surface** | `mse bp build --strict-embed` / MCP `bp_build` `strict_embed: true` | `mse serve --blueprint-strict-embed` / config-file key `blueprint_strict_embed` |
| **layer** | client (authoring CLI / MCP tool) | server (`mse serve`) |
| **trigger point** | build time — while compiling `.bp.lua` → Blueprint JSON | register time — `POST /v1/blueprints/:id` |
| **effect** | require every ref to **embed at build time**: an unresolved ref hard-fails the build (non-zero exit / `stage: "lint"` error), no JSON emitted | **reject raw refs**: a body still carrying `$file` / `$agent_md` refs is refused with 400; the server never runs its own linker for that request |
| **default (off) behavior** | unresolved refs emit the raw wire JSON with a WARN — the server resolves them itself at register time | the server accepts raw refs and resolves them against its own include cascade (`--blueprint-ref-base` + config includes) |
| **failure mode when set** | build exits non-zero; nothing to register | register returns 400 with a hint pointing at `mse bp build --strict-embed` |

## How they compose

The two switches implement one policy split: **who resolves refs — the
client at build time, or the server at register time?**

- **Neither set (default)**: refs may travel raw over the wire; the
  server resolves them. Most forgiving; resolution failures surface at
  register time from the server's cascade.
- **Client only** (`mse bp build --strict-embed`): the emitted JSON is
  guaranteed fully embedded, but the server still *accepts* raw-ref
  bodies from other clients.
- **Server only** (`--blueprint-strict-embed`): the server refuses raw
  refs — but nothing pre-embeds them for you. Pair it with the client
  flag (or ensure your build pipeline embeds) or every raw-ref register
  fails with 400.
- **Both set**: the strict end-to-end posture — clients embed, the
  server verifies nothing raw slips through. Recommended once a
  Blueprint pipeline is in CI.

The server's 400 hint names the client flag precisely because the
server cannot embed on the client's behalf under this posture — the fix
for a rejected register lives on the build side.

## Which one do I want?

- "My build should fail fast if a ref doesn't resolve locally" →
  client `--strict-embed`.
- "My server should never run the linker / see unresolved refs" →
  server `--blueprint-strict-embed` (and embed client-side).
- "I want the default layered behavior" (client emits raw, server
  resolves) → neither.

## Where to go next

- The 6-tier include cascade both linkers walk, and the Warn-default
  behavior on unresolved refs: `mse://guides/blueprint-ref-paths`
- `mse bp build` / `bp_build` reference: `mse://guides/dsl-authoring`,
  `bp_build` entry in `mse://guides/mcp-tool-reference`
- Server flags and config file: `mse://guides/getting-started`
  (`mse serve` section), `mse://guides/server-management`
