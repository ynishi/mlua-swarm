-- Sample: a bp_dsl `B.pipeline{}` Blueprint. Three verdict-gated stages run
-- in sequence: analyze -> review -> publish. Stage ids double as the wiring
-- defaults (input `$.d.{stage_id}`, output `$.{stage_id}`). The `review`
-- stage declares a verdict contract and retries through a `fix` stage while
-- it keeps returning BLOCKED (bounded); a stage that stays BLOCKED halts the
-- pipeline and records where it stopped in `$.halted_at`.

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
    description = "Verdict-gated three-stage pipeline built with bp_dsl's B.pipeline{} sugar: analyze -> review (retries a bounded fix-and-regate loop while its staged verdict part reads BLOCKED) -> publish. Seed with init_ctx={\"d\":{\"analyze\":\"issue\"}}. All operator agents point at the \"main-ai\" logical role; join with mse_operator_join(roles=[\"main-ai\"]) before dispatch.",
  },
}
