-- Reproduces mse://blueprints/samples/02-verdict-loop verbatim, using
-- flow_dsl directly (this sample's loop/branch shape is hand-written, not
-- bp_dsl's opinionated gate/retry sugar shape — see
-- dsl_json_equivalence_verdict_loop.rs for why this fixture goes through F,
-- not B.pipeline).

local F = require("flow_dsl")

local flow = F.seq({
  F.step({ id = "scout", agent = "mock-scout", input = F.lit("issue"), out = F.p("$.scout") }),
  F.step({ id = "planner", agent = "mock-planner", input = F.p("$.scout"), out = F.p("$.plan") }),
  F.loop_({
    counter = F.p("$.n"),
    cond = F.p("$.verdict"):eq("BLOCKED"),
    max = 3,
    body = F.seq({
      F.step({ id = "resolver", agent = "mock-resolver", input = F.p("$.plan"), out = F.p("$.fix") }),
      F.step({ id = "gate", agent = "mock-gate", input = F.p("$.fix"), out = F.p("$.verdict") }),
    }),
  }),
  F.branch({
    cond = F.p("$.verdict"):eq("PASS"),
    on_true = F.step({ id = "commit", agent = "mock-commit", input = F.p("$.fix"), out = F.p("$.commit") }),
    on_false = F.step({ id = "escalate", agent = "mock-escalate", input = F.p("$.fix"), out = F.p("$.escalated") }),
  }),
})

return {
  id = "sample-verdict-loop",
  -- Top-level check_policy passes straight through the bp_dsl Lua->JSON
  -- table conversion (build_bp_from_script); this fixture proves the
  -- passthrough by matching the JSON sample's `"check_policy": "strict"`.
  check_policy = "strict",
  flow = flow,
  agents = {
    {
      name = "mock-scout",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = { system_prompt = "Always reply `SCOUT_OK`", tools = {}, worker_binding = "claude" },
    },
    {
      name = "mock-planner",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = { system_prompt = "Always reply `PLAN_OK`", tools = {}, worker_binding = "claude" },
    },
    {
      name = "mock-resolver",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = { system_prompt = "Always reply `FIX_OK`", tools = {}, worker_binding = "claude" },
    },
    {
      name = "mock-gate",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = {
        system_prompt = "Always reply `PASS` (change to `BLOCKED` if you want to exercise the retry path)",
        tools = {},
        worker_binding = "claude",
      },
    },
    {
      name = "mock-commit",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = { system_prompt = "Always reply `COMMITTED`", tools = {}, worker_binding = "claude" },
    },
    {
      name = "mock-escalate",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = { system_prompt = "Always reply `ESCALATED`", tools = {}, worker_binding = "claude" },
    },
  },
  operators = {
    { name = "main-ai" },
  },
  strategy = { strict_refs = true, strict_kind = true },
  metadata = {
    description = "Verdict retry loop with a self-managed counter. Seed with init_ctx={\"verdict\":\"BLOCKED\"}. All operator agents point at the \"main-ai\" logical role; join with mse_operator_join(roles=[\"main-ai\"]) before dispatch. Declares check_policy=\"strict\" at the top level (cascade tier 2: launch request > blueprint > server config), so submit-time projection fail-open conditions surface as errors.",
  },
}
