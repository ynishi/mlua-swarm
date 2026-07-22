-- Sample: a bp_dsl `B.pipeline{}` Blueprint. Three sequential stages —
-- analyze -> review -> publish. Stage ids double as the wiring defaults
-- (input `$.d.{stage_id}`, output `$.{stage_id}`). The `review` stage
-- declares a verdict contract and retries through a `fix` stage while it
-- keeps returning BLOCKED (bounded); a stage that stays BLOCKED halts the
-- pipeline and records where it stopped in `$.halted_at`.
--
-- Post-bafe47d4 gate semantics: only `review` emits a verdict gate here,
-- because it is the stage that actually emits verdict — `retry = {...}`
-- opts it in implicitly (a retry loop reads verdict, so the post-retry
-- gate makes sense). `analyze` and `publish` produce no verdict and get
-- no gate. Pipeline-level `halt_on = { "BLOCKED" }` becomes the shared
-- default value used by the opted-in stage rather than a cascade that
-- forces every stage to gate. Add `gate = true` (or `halt_on = {...}`)
-- on a stage record to opt in explicitly; set `gate_default = "auto"`
-- at the pipeline level for the pre-fix cascade shape.
--
-- Agents are declared inline (no `$agent_md` refs). For a sample that pulls
-- agents from `.md` files via the Blueprint include cascade — bp.lua parent
-- → in-bp `blueprint_ref_includes` → `MSE_BLUEPRINT_INCLUDES` → CLI
-- `--include` → server config → bundled default — see
-- `mse://blueprints/samples/08-bundled-refs`. Full cascade guide:
-- `mse://guides/blueprint-ref-paths`. Strict opt-in (require every ref to
-- embed at build time): `mse bp build --strict-embed <this-file>`.

local F = require("flow_dsl")
local B = require("bp_dsl")

local flow = B.pipeline({
  B.stage "analyze" { agent = "analyzer" },
  B.stage "review" {
    agent = "reviewer",
    retry = {
      max = 2,
      fix = B.stage "fix" { agent = "fixer", input = B.from "review" },
    },
  },
  B.stage "publish" { agent = "publisher" },
  halt_on = { "BLOCKED" },
  halted_at = "$.halted_at",
  done = "$.pipeline_complete",
})

return {
  id = "sample-dsl-pipeline",
  flow = flow,
  agents = {
    {
      name = "analyzer",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = { system_prompt = "Always reply `ANALYZED`", tools = {}, worker_binding = "claude" },
    },
    {
      name = "reviewer",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = {
        system_prompt = "Stage a named `verdict` part (`PASS` or `BLOCKED`), then finish with a report body",
        tools = {},
        worker_binding = "claude",
      },
      verdict = { channel = "part", values = { "PASS", "BLOCKED" } },
    },
    {
      name = "fixer",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = { system_prompt = "Always reply `FIXED`", tools = {}, worker_binding = "claude" },
    },
    {
      name = "publisher",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = { system_prompt = "Always reply `PUBLISHED`", tools = {}, worker_binding = "claude" },
    },
  },
  operators = {
    { name = "main-ai" },
  },
  strategy = { strict_refs = true, strict_kind = true },
  metadata = {
    description = "Verdict-gated three-stage pipeline built with bp_dsl's B.pipeline{} sugar: analyze -> review (retries a bounded fix-and-regate loop while its staged verdict part reads BLOCKED) -> publish. Seed with init_ctx={\"d\":{\"analyze\":\"issue\"}}. All operator agents point at the \"main-ai\" logical role; join with mse_operator_join using that role and a capability_manifest covering the declared variants before dispatch.",
  },
}
