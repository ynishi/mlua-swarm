-- Sample (GH #82): `F.fanout` — one agent, N item payloads, aggregate.
--
-- The fanout `body` runs ONCE PER ITEM. This sample is the homogeneous
-- shape: a single `check` agent, dispatched once per element of
-- `$.d.targets`, each lane reading its own element through the bound
-- `$.item`. K steps in the body would mean K x N dispatches, so the
-- canonical body is exactly one step.
--
-- The heterogeneous shape — N differently-named checkers, one per lane —
-- selects its agent by branching on the bound `$.item`. That is what
-- `mse bp new fanout --stages lint,test,build` scaffolds; see
-- `mse://guides/bp-dsl-templates` for the rendered flow.
--
-- `join = "all"` gathers every lane's final ctx into an ordered array at
-- `$.results`. Each element is the whole lane ctx, not the step output —
-- and flow.ir paths cannot index an array, so the `aggregate` step is how
-- a downstream gate reads a fanout result. See
-- `mse://guides/blueprint-authoring` § "Fanout lanes, `$.results`, and the
-- aggregate gate".
--
-- Seed with `init_ctx = { d = { targets = { "core", "server", "cli" } } }`.

local F = require("flow_dsl")

local flow = F.seq({
  F.fanout({
    items = F.p("$.d.targets"),
    bind  = F.p("$.item"),
    join  = "all",
    out   = F.p("$.results"),
    -- One step, one dispatch per item. `$.item` is this lane's element.
    body  = F.step({ agent = "check", input = F.p("$.item"), out = F.p("$.branch_out") }),
  }),
  F.step({ agent = "aggregate", input = F.p("$.results"), out = F.p("$.aggregate") }),
})

return {
  id = "sample-fanout",
  flow = flow,
  agents = {
    {
      name = "check",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = {
        system_prompt = "Check the single target named in the input; reply with a one-paragraph report of what you found.",
        tools = {},
      },
      runner = { backend = "ws_operator", variant = "claude", tools = {} },
    },
    {
      name = "aggregate",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = {
        system_prompt = "Read the array of per-lane checker results at `$.results` and produce a single overall verdict + one-paragraph summary.",
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
    description = "One `check` agent fanned out over the `$.d.targets` array via F.fanout (join = \"all\"), one dispatch per item, with an aggregate stage consuming the collected `$.results`. The fanout body runs once per item, so it holds exactly one step. Seed with init_ctx={\"d\":{\"targets\":[\"core\",\"server\",\"cli\"]}}. Heterogeneous lanes (one agent per lane) are the `mse bp new fanout` scaffold's shape; see mse://guides/bp-dsl-templates. Gating on the result: mse://guides/blueprint-authoring.",
  },
}
