# Bundled sample agents

These `*.md` files ship with the `mse` binary as tier-6 (`bundled_default`)
of the Blueprint include cascade — the last-resort fallback the
`mlua-swarm-compile` linker walks when resolving `$agent_md` / `$file`
refs. See `mse://guides/blueprint-ref-paths` for the full cascade.

## Purpose

Two audiences read these files:

- **Authors** — bundled examples of the `agent.md` frontmatter shape
  (`name` / `description` / `model` / `effort` / `tools` /
  `worker_binding`) plus a body a `system_prompt` bake looks
  reasonable on.
- **Tests** — a stable set of resolvable `$agent_md` refs
  authoring-time integrations can pin against without needing the
  caller repo to ship its own agent.md files.

## Non-purposes

- These are not production Agents. `mse` never dispatches from them
  directly at runtime — they exist so an author writing a `.bp.lua`
  sample can refer to a real, resolvable `$agent_md` without pointing
  outside the `mse` workspace.
- No private-repo agent-profile literal ever appears here (design.md
  §Doc rule / project `CLAUDE.md`).

## Add another sample

Drop a new `<name>.md` beside these. The `mlua-swarm-compile`
`agent_md::load_file` loader accepts YAML frontmatter delimited by
`---`; see `mse://guides/agent-md-authoring` for the field inventory
and size targets.
