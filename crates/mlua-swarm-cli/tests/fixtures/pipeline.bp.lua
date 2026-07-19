-- Fixture script: rebuilds `tests/fixtures/pipeline.json` (a nine-agent
-- pipeline shape) using `B.pipeline{}` for the flow and `B.agent` for the
-- `$agent_md` agent entries. The fixture deliberately gives every stage an
-- `input=`/`out=` override (its path names don't follow `bp_dsl`'s default
-- `$.d.{stage_id}` / `$.{stage_id}` wiring) and overrides the retry counter
-- path, so the explicit-override code paths stay exercised. Stage ids are
-- set to the dispatch agent name itself so the automatic verdict-gate's
-- `halted_at` assign (which always writes `F.lit(stage_id)`) matches the
-- JSON fixture's literal halt values without a separate override.

local F = require("flow_dsl")
local B = require("bp_dsl")

local flow = B.pipeline({
  B.stage "prepare" {
    agent = "prepare",
    input = "$.d.prep",
    out = "$.prepared",
  },
  B.stage "setup" {
    agent = "setup",
    input = "$.d.setup_dir",
    out = "$.workspace",
  },
  B.stage "gather" {
    agent = "gather",
    input = "$.d.gather_ctx",
    out = "$.context",
  },
  B.stage "implement" {
    agent = "implement",
    input = "$.d.impl",
    out = "$.implemented",
  },
  B.stage "polish" {
    agent = "polish",
    input = "$.d.polish_rules",
    out = "$.polished",
  },
  B.stage "review" {
    agent = "review",
    input = "$.d.review",
    out = "$.review_verdict",
    retry = {
      max = 2,
      counter = "$.rv_n",
      fix = B.stage "fix" { agent = "fixer", input = B.from "review" },
    },
  },
  B.stage "record" {
    agent = "record",
    input = "$.d.record",
    out = "$.recorded",
  },
  B.stage "merge" {
    agent = "merge",
    input = "$.d.merge_target",
    out = "$.merged",
  },
  halt_on = { "BLOCKED" },
  halted_at = "$.halted_at",
  done = "$.pipeline_complete",
  -- bafe47d4: the shipped `pipeline.json` snapshot expects the pre-fix
  -- cascade shape (every stage emits a gate whose cond compares against
  -- pipeline-level halt_on). Under the new opt-in default this fixture
  -- would drop the dead branches on non-verdict stages and no longer
  -- byte-match. `gate_default = "auto"` restores the legacy shape so the
  -- JSON snapshot stays a stable regression fixture.
  gate_default = "auto",
})

local agents = {
  B.agent({ md = "prepare.md", spec = { operator_ref = "main-ai" } }),
  B.agent({ md = "setup.md", spec = { operator_ref = "main-ai" } }),
  B.agent({ md = "gather.md", spec = { operator_ref = "main-ai" } }),
  B.agent({ md = "implement.md", spec = { operator_ref = "main-ai" } }),
  B.agent({ md = "polish.md", spec = { operator_ref = "main-ai" } }),
  B.agent({
    md = "review.md",
    verdict = { channel = "part", values = { "PASS", "BLOCKED" } },
    spec = { operator_ref = "main-ai" },
  }),
  B.agent({ md = "record.md", spec = { operator_ref = "main-ai" } }),
  B.agent({ md = "merge.md", spec = { operator_ref = "main-ai" } }),
  B.agent({ md = "fixer.md", spec = { operator_ref = "main-ai" } }),
}

-- `operators[1].spec` is `{}` (an empty JSON object) in the JSON
-- fixture; `F.obj()` is the DSL marker for that exact shape (see
-- `flow_dsl.lua`'s `M.obj` doc comment).
local operators = {
  {
    name = "main-ai",
    display_name = "Main operator",
    kind = "main_ai",
    spec = F.obj(),
  },
}

return {
  id = "sample-pipeline",
  flow = flow,
  agents = agents,
  strategy = { strict_refs = true, strict_kind = true },
  metadata = {
    default_run_ttl_secs = 5400,
    description = "Nine verdict-gated stages in sequence with a bounded fix-and-regate retry loop around the review stage. Callers seed per-stage directives under init_ctx.d and the retry counter rv_n=0. The review agent declares a verdict contract (channel=part, values=[PASS, BLOCKED]) so compile-time cond lint and submit-time gating protect the loop shape.",
  },
  operators = operators,
}
