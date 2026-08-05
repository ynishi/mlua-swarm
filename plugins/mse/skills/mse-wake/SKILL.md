---
name: mse-wake
description: Load the mse Blueprint JSON Schema, the bundled guide inventory, and the sample inventory into the main-thread context so subsequent design conversation can name Blueprint concepts with grounded vocabulary. Read-only load; does not spawn any agent.
---

# /mse-wake — Load mse Blueprint authoring context

Fetches the schema + guides + sample inventory once, at the start of a
Blueprint design conversation, so the main thread has the right vocabulary
before the User asks (or the AI decides) to invoke `@mse-adviser` or
`/bp-build`.

## When to run

- Start of a session where the User wants to design or discuss a mlua-swarm
  Blueprint.
- Before the AI reasons about `flow` nodes, worker binding, projection
  placement, verdict contracts, or operator kinds — anything where the
  schema field names matter.

Runs load-only. If the User just wants an answer to one question, prefer
`@mse-adviser` directly (it does its own grounding fetches).

## What it loads

1. **Blueprint JSON Schema** — `mse://api/blueprint-schema` via
   `ReadMcpResourceTool`.
2. **Guide inventory** — list `mse://guides/*` via `ListMcpResourcesTool`
   and read the two lifecycle entry points:
   `mse://guides/getting-started` and `mse://guides/bp-lifecycle`.
3. **Sample inventory** — list `mse://blueprints/samples/*` via
   `ListMcpResourcesTool`. Do not read every sample body; a single
   representative sample body (`mse://blueprints/samples/07-dsl-pipeline`)
   is enough to seed the DSL vocabulary.
4. **MCP tool reference** — `mse://guides/mcp-tool-reference` so the
   `bp_build` / `bp_doctor` / `swarm_run` / `mse_operator_*` shapes are
   accessible.

## What it does not do

- Does **not** spawn an agent (`@mse-adviser` / `@bp-coder`).
- Does **not** write files.
- Does **not** register or run a Blueprint.

## After it runs

The main thread continues normally. Typical next moves:

- Design consultation → `@mse-adviser` with a specific question.
- Kick implementation once design is mature → `/bp-build "<design paragraph>"`.
- Peek a specific sample → `ReadMcpResourceTool` on the URI listed in the
  sample inventory.
