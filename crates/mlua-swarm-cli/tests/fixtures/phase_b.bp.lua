-- Golden-fixture script: rebuilds `tests/fixtures/phase-b-real-agents.json`
-- (a real-world nine-agent pipeline shape) using flow_dsl (F) directly for
-- the flow and bp_dsl's `B.agent` for the `$agent_md` agent entries. The
-- flow is hand-assembled rather than built through `B.pipeline`'s
-- gate/retry sugar because the fixture's historical names don't match the
-- pipeline's derived defaults everywhere (e.g. stage input
-- `$.d.topic_setup` vs the `$.d.{stage_id}` default, retry counter
-- `$.qg_n` vs `$.{stage_id}_n`) and the retry counter path is not yet
-- overridable — see GH #52. Building the tree directly with F mirrors
-- `verdict_loop.bp.lua`'s convention for hand-written flow shapes.

local F = require("flow_dsl")
local B = require("bp_dsl")

-- Deepest: worktree-merge step + its gate (else = phase_b_complete assign).
local worktree_merge_step = F.step({
  agent = "worktree-merge",
  input = F.p("$.d.merge"),
  out = F.p("$.merge"),
})
local worktree_merge_gate = F.branch({
  cond = F.p('$.merge.parts["verdict"]'):eq("BLOCKED"),
  on_true = F.assign({ at = F.p("$.halted_at"), value = F.lit("worktree-merge") }),
  on_false = F.assign({ at = F.p("$.phase_b_complete"), value = F.lit(true) }),
})

-- committer step + gate (else = seq[worktree_merge_step, worktree_merge_gate]).
local committer_step = F.step({
  agent = "committer",
  input = F.p("$.d.commit"),
  out = F.p("$.commit"),
})
local committer_gate = F.branch({
  cond = F.p('$.commit.parts["verdict"]'):eq("BLOCKED"),
  on_true = F.assign({ at = F.p("$.halted_at"), value = F.lit("committer") }),
  on_false = F.seq({ worktree_merge_step, worktree_merge_gate }),
})

-- quality-gate first run + verdict retry loop + final gate.
local qgate_step = F.step({
  agent = "quality-gate",
  input = F.p("$.d.qgate"),
  out = F.p("$.qgate_verdict"),
})
local build_resolver_step = F.step({
  agent = "build-resolver",
  input = F.p("$.qgate_verdict"),
  out = F.p("$.resolve"),
})
local qgate_rerun_step = F.step({
  agent = "quality-gate",
  input = F.p("$.d.qgate"),
  out = F.p("$.qgate_verdict"),
})
local qgate_loop = F.loop_({
  counter = F.p("$.qg_n"),
  cond = F.p("$.qg_n"):lt(2):And(F.p('$.qgate_verdict.parts["verdict"]'):eq("BLOCKED")),
  max = 3,
  body = F.seq({ build_resolver_step, qgate_rerun_step }),
})
local qgate_final_gate = F.branch({
  cond = F.p('$.qgate_verdict.parts["verdict"]'):eq("BLOCKED"),
  on_true = F.assign({ at = F.p("$.halted_at"), value = F.lit("quality-gate") }),
  on_false = F.seq({ committer_step, committer_gate }),
})

-- quality-coding step + gate.
local qcode_step = F.step({
  agent = "quality-coding",
  input = F.p("$.d.qcode"),
  out = F.p("$.qcode"),
})
local qcode_gate = F.branch({
  cond = F.p('$.qcode.parts["verdict"]'):eq("BLOCKED"),
  on_true = F.assign({ at = F.p("$.halted_at"), value = F.lit("quality-coding") }),
  on_false = F.seq({ qgate_step, qgate_loop, qgate_final_gate }),
})

-- impl-lead step + gate.
local impl_step = F.step({
  agent = "impl-lead",
  input = F.p("$.d.impl"),
  out = F.p("$.impl"),
})
local impl_gate = F.branch({
  cond = F.p('$.impl.parts["verdict"]'):eq("BLOCKED"),
  on_true = F.assign({ at = F.p("$.halted_at"), value = F.lit("impl-lead") }),
  on_false = F.seq({ qcode_step, qcode_gate }),
})

-- context-librarian step + gate.
local context_step = F.step({
  agent = "context-librarian",
  input = F.p("$.d.context"),
  out = F.p("$.context"),
})
local context_gate = F.branch({
  cond = F.p('$.context.parts["verdict"]'):eq("BLOCKED"),
  on_true = F.assign({ at = F.p("$.halted_at"), value = F.lit("context-librarian") }),
  on_false = F.seq({ impl_step, impl_gate }),
})

-- workspace-setup step + gate.
local workspace_step = F.step({
  agent = "workspace-setup",
  input = F.p("$.d.workspace_setup"),
  out = F.p("$.workspace"),
})
local workspace_gate = F.branch({
  cond = F.p('$.workspace.parts["verdict"]'):eq("BLOCKED"),
  on_true = F.assign({ at = F.p("$.halted_at"), value = F.lit("workspace-setup") }),
  on_false = F.seq({ context_step, context_gate }),
})

-- topic-setup step + gate (the flow's top-level seq).
local topic_step = F.step({
  agent = "topic-setup",
  input = F.p("$.d.topic_setup"),
  out = F.p("$.topic"),
})
local topic_gate = F.branch({
  cond = F.p('$.topic.parts["verdict"]'):eq("BLOCKED"),
  on_true = F.assign({ at = F.p("$.halted_at"), value = F.lit("topic-setup") }),
  on_false = F.seq({ workspace_step, workspace_gate }),
})

local flow = F.seq({ topic_step, topic_gate })

local agents = {
  B.agent({ md = "topic-setup.md", spec = { operator_ref = "main-ai" } }),
  B.agent({ md = "workspace-setup.md", spec = { operator_ref = "main-ai" } }),
  B.agent({ md = "context-librarian.md", spec = { operator_ref = "main-ai" } }),
  B.agent({ md = "impl-lead.md", spec = { operator_ref = "main-ai" } }),
  B.agent({ md = "quality-coding.md", spec = { operator_ref = "main-ai" } }),
  B.agent({
    md = "quality-gate.md",
    verdict = { channel = "part", values = { "PASS", "BLOCKED" } },
    spec = { operator_ref = "main-ai" },
  }),
  B.agent({ md = "committer.md", spec = { operator_ref = "main-ai" } }),
  B.agent({ md = "worktree-merge.md", spec = { operator_ref = "main-ai" } }),
  B.agent({ md = "build-resolver.md", spec = { operator_ref = "main-ai" } }),
}

-- NOTE: `operators[1].spec` is `{}` (an empty JSON OBJECT) in the JSON
-- fixture. `dsl::build_bp_from_script` converts every zero-length Lua
-- table to a JSON empty ARRAY (`encode_empty_tables_as_array(true)` —
-- Lua's table type cannot itself distinguish an empty array from an
-- empty object), so this one leaf cannot be reproduced byte-for-byte
-- from Lua today; `dsl_golden_phase_b.rs` normalizes this single,
-- semantically-empty leaf before comparing (see GH #52 for the planned
-- empty-object marker).
local operators = {
  {
    name = "main-ai",
    display_name = "Main AI (Coding Orch dispatcher)",
    kind = "main_ai",
    spec = {},
  },
}

return {
  id = "coding-orch-phase-b",
  flow = flow,
  agents = agents,
  strategy = { strict_refs = true, strict_kind = true },
  metadata = {
    default_run_ttl_secs = 5400,
    description = "Real-world coding-pipeline shape: nine verdict-gated stages in sequence with a bounded fix-and-regate retry loop around the quality gate. Callers seed per-stage directives under init_ctx.d and the retry counter qg_n=0. The quality-gate agent declares a verdict contract (channel=part, values=[PASS, BLOCKED]) so compile-time cond lint and submit-time gating protect the loop shape.",
  },
  operators = operators,
}
