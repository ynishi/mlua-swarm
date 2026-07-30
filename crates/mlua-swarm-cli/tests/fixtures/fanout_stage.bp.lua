-- Fixture: a fanout stage written with the `B.pipeline` sugar.
--
-- `tests/fixtures/fanout_stage_raw.bp.lua` is the same flow hand-built out
-- of `flow_dsl` nodes; `tests/dsl_fanout_stage.rs` asserts the two build to
-- the same JSON, which is what pins the sugar's expansion.

local B = require("bp_dsl")

local flow = B.pipeline({
  B.stage "plan" { agent = "planner" },
  B.stage "gates" {
    fanout = {
      lanes = {
        { lane = "danger", agent = "gate-danger" },
        { lane = "leak", agent = "gate-leak" },
        { lane = "hygiene", agent = "gate-hygiene" },
      },
    },
  },
  B.stage "aggregate" {
    agent = "aggregate",
    input = B.from "gates",
    gate = true,
  },
  halt_on = { "BLOCKED" },
  halted_at = "$.halted_at",
  done = "$.gates_ok",
})

return { id = "fanout-stage-fixture", flow = flow }
