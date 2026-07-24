-- Sample (GH #82): `F.fanout` — parallel branch dispatch + aggregate.
--
-- Three independent checkers (`lint`, `test`, `build`) dispatch in
-- parallel via the flow-ir `fanout` node, each reading its own input
-- from `$.d.<checker>`. The `join = "all"` mode gathers every
-- branch's final ctx into an ordered array at `$.results`; the
-- `aggregate` stage consumes that array and produces the final
-- decision.
--
-- Before GH #82, this shape required `F.raw()` (the flow_dsl had no
-- `fanout` builder). `F.fanout{items, bind, body, join, out}` is the
-- 7th and final Node builder — flow.ir's full Node grammar is now
-- reachable through the DSL.
--
-- Seed with `init_ctx = { d = { lint = "...", test = "...", build = "..." } }`.
--
-- See `mse://guides/blueprint-authoring` § "Flow node kinds" for the
-- four `join` modes (`all` / `any` / `race` / `all_settled`) and
-- `mse://guides/bp-dsl-templates` for the `fanout` template shape.

local F = require("flow_dsl")

local flow = F.seq({
  F.assign({ at = F.p("$.checkers"), value = F.lit({ "lint", "test", "build" }) }),
  F.fanout({
    items = F.p("$.checkers"),
    bind  = F.p("$.item"),
    join  = "all",
    out   = F.p("$.results"),
    body  = F.seq({
      F.step({ agent = "lint",  input = F.p("$.d.lint"),  out = F.p("$.branch_out") }),
      F.step({ agent = "test",  input = F.p("$.d.test"),  out = F.p("$.branch_out") }),
      F.step({ agent = "build", input = F.p("$.d.build"), out = F.p("$.branch_out") }),
    }),
  }),
  F.step({ agent = "aggregate", input = F.p("$.results"), out = F.p("$.aggregate") }),
})

return {
  id = "sample-fanout",
  flow = flow,
  agents = {
    {
      name = "lint",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = {
        system_prompt = "Lint the input; reply with a one-paragraph report of what you found.",
        tools = {},
      },
      runner = { backend = "ws_operator", variant = "claude", tools = {} },
    },
    {
      name = "test",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = {
        system_prompt = "Run the tests described by the input; reply with a one-paragraph pass/fail summary.",
        tools = {},
      },
      runner = { backend = "ws_operator", variant = "claude", tools = {} },
    },
    {
      name = "build",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = {
        system_prompt = "Verify the build described by the input; reply with a one-paragraph build report.",
        tools = {},
      },
      runner = { backend = "ws_operator", variant = "claude", tools = {} },
    },
    {
      name = "aggregate",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = {
        system_prompt = "Read the array of parallel checker results at `$.results` and produce a single overall verdict + one-paragraph summary.",
        tools = {},
      },
      runner = { backend = "ws_operator", variant = "claude", tools = {} },
    },
  },
  operators = {
    { name = "main-ai", kind = "main_ai" },
  },
  strategy = { strict_refs = true, strict_kind = true },
  metadata = {
    description = "Three independent checkers dispatched in parallel via F.fanout (join = \"all\"); an aggregate stage consumes the collected `$.results` array. Demonstrates the flow_dsl `F.fanout` builder introduced by GH #82 (the shape used to require F.raw()). Seed with init_ctx={\"d\":{\"lint\":\"...\",\"test\":\"...\",\"build\":\"...\"}}. See mse://guides/blueprint-authoring for the four join modes and mse://guides/bp-dsl-templates for the fanout scaffold template.",
  },
}
