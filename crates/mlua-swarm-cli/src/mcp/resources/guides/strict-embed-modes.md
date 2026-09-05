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
| **effect** | require every ref to **embed at build time**, and emit it embedded: a resolved ref is replaced by what it referenced in the emitted JSON (`-o`, stdout, `--register` body alike), an unresolved one hard-fails the build (non-zero exit / `stage: "lint"` error), no JSON emitted | **reject raw refs**: a body still carrying `$file` / `$agent_md` refs is refused with 400; the server never runs its own linker for that request |
| **default (off) behavior** | refs are emitted as they were written — resolved or not — and the server resolves them itself at register time; an unresolved one also prints a WARN | the server accepts raw refs and resolves them against its own include cascade (`--blueprint-ref-base` + config includes) |
| **failure mode when set** | build exits non-zero; nothing to register | register returns 400 with a hint pointing at `mse bp build --strict-embed` |

## How they compose

The two switches implement one policy split: **who resolves refs — the
client at build time, or the server at register time?**

- **Neither set (default)**: refs may travel raw over the wire; the
  server resolves them. Most forgiving; resolution failures surface at
  register time from the server's cascade. Requires the server to be
  able to open the referenced files — true for a server on the author's
  own machine, false for a hosted one.
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

## What "self-contained" adds under the client flag

An embedded Blueprint is the whole document: nothing about it is left
for the registering server to fill in. Two consequences, both only
under `--strict-embed` / `strict_embed: true`:

- **Kinds must be declared by the Blueprint.** A `$agent_md` with no
  sibling `kind`, in a Blueprint with no top-level `default_agent_kind`,
  is a build error — the message names both places. Without the flag
  the server's own `mse serve --default-agent-kind` would have decided;
  an embedded body carries the `kind` literal itself, so pinning the
  schema default silently would bypass that server setting.
- **Every embedded agent records where it came from.**
  `profile.extras.embed = {source, repo, rev}` — the resolved file's
  path relative to its git work tree (never absolute), the work tree's
  directory name, and its `HEAD` sha (`-dirty` suffixed when the tree
  has uncommitted changes; `rev` absent when the file is not under git).
  Together with the loader's `version_hash` (blake3 of the body) this
  is what lets a registered Blueprint be compared against the
  `agent.md` it was built from. `extras` is the schema's carry for
  unmodelled keys, so the record registers with servers that predate
  it.

The MCP twin writes the built JSON to a file on every call (`out`, or
`$MSE_HOME/bp/<bp id>.json`) and names it as `blueprint_file`; the
inline `blueprint` is present only under 16 KiB, which an embedded
Blueprint is not. Pass `include` to add tier-4 directories the way the
CLI's `--include` does.

## Which one do I want?

- "My build should fail fast if a ref doesn't resolve locally" →
  client `--strict-embed`.
- "My `mse serve` runs somewhere else and cannot see my `agent.md`
  files" → client `--strict-embed`. This is the only posture that gets
  the prompts to a hosted server: the default hands it paths, and the
  paths are yours, not its.
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
