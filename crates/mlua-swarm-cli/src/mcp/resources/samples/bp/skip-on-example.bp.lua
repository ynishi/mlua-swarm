-- Sample (GH #76 DSL sugar): `skip_on = { ... }` DSL sugar for the Skip tier.
--
-- Three-stage analyst chain — triage -> analyze -> summarize.
--
-- The middle `analyze` stage carries `skip_on = { "NOT_APPLICABLE" }`.
-- When `triage`'s staged verdict part reads `NOT_APPLICABLE`, the
-- `analyze` stage's body is elided (its worker is never
-- dispatched); flow continues straight to `summarize`, which reads
-- whatever pre-existing value sat at `$.analyze` (typically
-- absent). When `triage` returns any other verdict, `analyze`
-- runs as usual.
--
-- The runtime-path Skip (agent submits `--verdict=skip` at
-- `mse_worker_submit` time — see `mse://guides/skip-tier-and-skip-on`
-- for the wire semantics) coexists with this DSL sugar: an agent that
-- ran but decided its output is not applicable can declare so at
-- submit time; both paths land on `DispatchOutcome::Skip`.
--
-- Agents are declared inline (no `$agent_md` refs). For a sample that
-- pulls agents from bundled `.md` files see
-- `mse://blueprints/samples/08-bundled-refs`. The full cascade guide
-- is `mse://guides/blueprint-ref-paths`.
--
-- Seed with `init_ctx = { d = { triage = "<incoming issue>" } }`.

local B = require("bp_dsl")

local flow = B.pipeline({
  B.stage "triage" { agent = "triager" },
  B.stage "analyze" {
    agent = "analyzer",
    input = B.from "triage",
    skip_on = { "NOT_APPLICABLE" },
  },
  B.stage "summarize" {
    agent = "summarizer",
    input = B.from "triage",
  },
  halted_at = "$.halted_at",
  done = "$.pipeline_complete",
})

return {
  id = "sample-skip-on-example",
  flow = flow,
  agents = {
    {
      name = "triager",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = {
        system_prompt = "Classify the incoming issue. Stage a named `verdict` part reading either `APPLICABLE` (the analyze stage should run) or `NOT_APPLICABLE` (analyze should be skipped), then finish with a short report body.",
        tools = {},
      },
      runner = { backend = "ws_operator", variant = "claude", tools = {} },
      verdict = { channel = "part", values = { "APPLICABLE", "NOT_APPLICABLE" } },
    },
    {
      name = "analyzer",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = {
        system_prompt = "Run deep analysis on the triaged issue and reply with the analysis body.",
        tools = {},
      },
      runner = { backend = "ws_operator", variant = "claude", tools = {} },
    },
    {
      name = "summarizer",
      kind = "operator",
      spec = { operator_ref = "main-ai" },
      profile = {
        system_prompt = "Summarize the triage outcome (and the deep analysis, if any) into a single-paragraph report.",
        tools = {},
      },
      runner = { backend = "ws_operator", variant = "claude", tools = {} },
    },
  },
  operators = {
    { name = "main-ai" },
  },
  strategy = { strict_refs = true, strict_kind = true },
  metadata = {
    description = "Three-stage analyst chain built with bp_dsl's B.pipeline{} sugar: triage -> analyze -> summarize. The middle stage's `skip_on = { \"NOT_APPLICABLE\" }` elides its body when triage's staged verdict part reads NOT_APPLICABLE, and the pipeline continues to summarize. Seed with init_ctx={\"d\":{\"triage\":\"<incoming issue>\"}}. All operator agents point at the \"main-ai\" logical role; join with mse_operator_join using that role. See mse://guides/skip-tier-and-skip-on for the Skip tier semantics and the skip_on DSL surface.",
  },
}
