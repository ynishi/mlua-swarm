-- Sample: a two-stage pipeline whose agents are supplied by `$agent_md`
-- refs pointing at the bundled `agents/*.md` files (`researcher.md` +
-- `reviewer.md`). Demonstrates the tier-1 (bp.lua parent) and tier-6
-- (bundled default) knobs of the Blueprint include cascade in one shot —
-- as authored, refs resolve via tier 1 (this file's parent contains
-- `agents/`), and dropping the `agents/` prefix would still resolve
-- because tier 6 points at the same bundled dir.
--
-- ## Blueprint ref-path cascade (order = first-hit-wins)
--
-- 1. bp.lua parent directory (this script's dir).
-- 2. `blueprint_ref_includes` declared inside this script (relative to 1).
-- 3. `MSE_BLUEPRINT_INCLUDES` env var (`:`-separated paths).
-- 4. CLI `--include <DIR>` flags (`mse bp build` / `mse bp lint` / `mse serve`).
-- 5. server config `blueprint_ref_includes` (mlua_swarm_server.toml).
-- 6. Bundled default — `mse`'s built-in `samples/agents/` directory.
--
-- Full guide: `mse://guides/blueprint-ref-paths`.
-- Strict opt-in (require every ref to be embedded at build time):
--   `mse bp build --strict-embed <this-file>`.

local F = require("flow_dsl")
local B = require("bp_dsl")

local flow = B.pipeline({
  B.stage "research" { agent = "researcher" },
  B.stage "review" { agent = "reviewer" },
  halt_on = { "BLOCKED" },
  halted_at = "$.halted_at",
  done = "$.pipeline_complete",
})

return {
  id = "sample-bundled-refs",
  flow = flow,
  agents = {
    {
      ["$agent_md"] = "agents/researcher.md",
      spec = { operator_ref = "main-ai" },
    },
    {
      ["$agent_md"] = "agents/reviewer.md",
      spec = { operator_ref = "main-ai" },
      verdict = { channel = "part", values = { "PASS", "BLOCKED" } },
    },
  },
  operators = {
    { name = "main-ai" },
  },
  strategy = { strict_refs = true, strict_kind = true },
  metadata = {
    description = "Two-stage research-then-review pipeline whose agents are pulled from the bundled `samples/agents/*.md` files via `$agent_md` refs. Seed with init_ctx={\"d\":{\"research\":\"<your topic>\"}}. Both agents point at the \"main-ai\" logical role; join with mse_operator_join(roles=[\"main-ai\"]) before dispatch.",
  },
}
