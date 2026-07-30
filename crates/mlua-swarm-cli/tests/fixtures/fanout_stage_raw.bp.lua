-- Fixture: the flow of `tests/fixtures/fanout_stage.bp.lua`, hand-built out
-- of `flow_dsl` nodes — the shape an author had to write before the
-- `B.pipeline` fanout-stage sugar existed. `tests/dsl_fanout_stage.rs`
-- asserts the two fixtures build to the same JSON.

local F = require("flow_dsl")

local flow = F.seq({
  F.step({ agent = "planner", input = F.p("$.d.plan"), out = F.p("$.plan") }),
  F.seq({
    F.fanout({
      items = F.lit({ "danger", "leak", "hygiene" }),
      bind = F.p("$.item"),
      join = "all",
      out = F.p("$.gates"),
      -- One dispatch per lane: the body branches on the bound item and the
      -- last lane is the terminal `else` (the item set is the literal array
      -- above, so it is closed by construction).
      body = F.branch({
        cond = F.p("$.item"):eq("danger"),
        on_true = F.step({
          agent = "gate-danger",
          input = F.p("$.d.danger"),
          out = F.p("$.lane.danger"),
        }),
        on_false = F.branch({
          cond = F.p("$.item"):eq("leak"),
          on_true = F.step({
            agent = "gate-leak",
            input = F.p("$.d.leak"),
            out = F.p("$.lane.leak"),
          }),
          on_false = F.step({
            agent = "gate-hygiene",
            input = F.p("$.d.hygiene"),
            out = F.p("$.lane.hygiene"),
          }),
        }),
      }),
    }),
    F.seq({
      F.step({
        agent = "aggregate",
        input = F.p("$.gates"),
        out = F.p("$.aggregate"),
      }),
      F.branch({
        cond = F.p('$.aggregate.parts["verdict"]'):eq("BLOCKED"),
        on_true = F.assign({ at = F.p("$.halted_at"), value = F.lit("aggregate") }),
        on_false = F.assign({ at = F.p("$.gates_ok"), value = F.lit(true) }),
      }),
    }),
  }),
})

return { id = "fanout-stage-fixture", flow = flow }
