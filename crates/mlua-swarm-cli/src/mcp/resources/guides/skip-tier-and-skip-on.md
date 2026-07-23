# Skip tier & `skip_on` DSL sugar (GH #76)

Skip is a sibling tier of `Pass` / `Blocked` in the engine's
`DispatchOutcome` enum. It represents "the worker ran successfully but
its output is not applicable to the surrounding flow" — a completing
outcome that continues flow evaluation without propagating an error and
without writing the worker's output to the step's declared `out`
binding. Downstream `$.<step_id>` references see whatever pre-existing
value the binding held (typically absent).

This document covers:

- Skip tier semantics (engine + wire).
- Two entry paths (runtime `--verdict=skip` vs. `skip_on` DSL sugar).
- `bp_doctor` `skip_on_lint` family.
- Error surface when a step legitimately fails (`TaskLaunchError::FlowEval`).

Related: `mse://guides/blueprint-authoring` for the Blueprint document
shape, `mse://guides/dsl-authoring` for the general
`flow_dsl` / `bp_dsl` surface, and
`mse://blueprints/samples/09-skip-on-example` for a runnable Blueprint
using the sugar.

## Wire semantics

A Skip outcome maps to `Final { ok: true, content: <skip_marker> }` on
the worker's output tail. The marker is a reserved-key sentinel of
shape `{ "__mse_skip": true, "value": <the agent's verdict payload> }`.
The engine dispatcher recognises the marker and:

1. Returns `Ok(...)` to the enclosing flow — Skip is **not** a
   flow-level failure. The `try` node's catch arm is not entered.
2. Short-circuits the write to the step's declared `out` binding.
   Downstream steps that reference `$.<step_id>` see the pre-existing
   value at that path (typically absent), never the skip marker.
3. Records the step's `status` as `"skipped"` in the run's step-entry
   log, so replay / audit surfaces can distinguish Skip from Pass.

The verdict-contract completion check is deliberately skipped for a
Skip outcome — Skip means "no verdict applied", not "a specific
verdict value in the declared enum". Agents do not need to add a
`SKIP` literal to their `verdict.values` for the Skip path to work.

## Entry paths

### 1. Runtime path — `mse_worker_submit --verdict=skip`

An agent that has already been dispatched can declare its own output
non-applicable by passing `verdict=skip` on the final
`mse_worker_submit` call. This routes through
`Engine::submit_worker_result_trusted(.., outcome: SubmitOutcome::Skip)`
on the server side, which wraps the value in the skip marker and hands
it back to the dispatcher.

This is the **self-declared** path: the agent runs, decides, and
declares. Use this when the applicability test requires the agent's
own reasoning (fetching data, running a check, etc.).

### 2. DSL path — `skip_on = { ... }` on a `B.stage`

Static pre-emptive skip based on an upstream verdict. On a
`B.stage` record inside a `B.pipeline{}`:

```lua
local B = require("bp_dsl")

return B.pipeline({
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
})
```

The `analyze` stage compiles to:

```
Branch {
  cond = in(
    needle   = path("$.triage.parts[\"verdict\"]"),
    haystack = lit(["NOT_APPLICABLE"]),
  ),
  then = Seq { children = [] },
  else = Seq { children = [<analyze step>] },
}
```

When `triage`'s staged verdict part reads `NOT_APPLICABLE`, the
`analyze` step is never dispatched; the pipeline continues to
`summarize` unchanged. When it reads anything else, `analyze` runs as
usual.

The verdict path checked by `skip_on` is
`<input_path>.parts["verdict"]` — the stage's own INPUT path,
`.parts["verdict"]`. In a chained pipeline (`chain = true`, or an
explicit `input = B.from "prev"`), this resolves to the previous
stage's own verdict, which is the intended shape. In the R1-default
case (no chain, no `input =`) the input is `$.d.<stage_id>`, where
`.parts.verdict` is typically absent — the `in` check evaluates false
and the guard never fires (safe by construction; skip_on with no
upstream verdict is a no-op).

`skip_on = {}` is a no-op (identical to omitting the option).
`skip_on` may coexist with `halt_on` / `retry` / `gate` on the same
stage: the skip guard wraps the stage's own body and sits INSIDE the
enclosing gate/rest chain, so a skipped stage still lets later stages
run.

Use this path when applicability is decidable from an upstream
verdict alone (a router / triage stage classified the input, and
subsequent stages want to opt out on specific classifications).

### Choosing between the two

Both paths land on `DispatchOutcome::Skip`. Prefer the DSL path when
the applicability check is a lookup against an upstream verdict —
skipping saves the entire agent dispatch. Prefer the runtime path when
the agent itself needs to run in order to know whether its output is
applicable.

## `bp_doctor` `skip_on_lint` family

The `bp_doctor` MCP tool (`mcp__mse__bp_doctor`) applies a
Blueprint-scoped `skip_on_lint` family (default enabled; disable via
`disable_skip_on_lint`). It emits three checks:

- `skip_on_missing_for_skip_like_verdict_value` (WARN) — an agent
  declares a `verdict.values` entry that reads like a Skip signal
  (`SKIP`, `NOT_APPLICABLE`, `N/A`, case-insensitive), but no
  compiled Branch in the flow uses that value in a `skip_on` list.
  Points at either the missing DSL sugar or a redundant verdict
  value.
- `skip_on_declared_but_no_matching_verdict_value` (WARN) — a
  `skip_on` list carries a value that appears in no agent's
  `verdict.values`. Points at a stale skip_on list — the upstream
  agent no longer emits that verdict, so the guard can never fire
  (dead branch).
- `skip_on_pattern_conflicts_with_halt_on` (WARN) — the same
  verdict value appears in a `skip_on` list AND in a `halt_on` /
  gate check somewhere in the flow. Only one of the two guards
  will actually fire on that verdict (the first one the flow
  reaches), so the overlap is at best confusing and at worst a
  logic bug.

Every finding is report-only — `bp_doctor` never blocks a dispatch,
and the `skip_on_lint` family is BLOCK-disabled by default (WARN is
the maximum severity emitted). Findings feed the aggregate `verdict`
via the same OK/WARN/BLOCK precedence sibling families use.

## Error surface

A step that legitimately fails (dispatch error, `Blocked` outcome,
flow-eval failure) surfaces as `TaskLaunchError::FlowEval` on the
launch path. The variant carries structured fields —
`failed_step` (the step id that failed), `verdict_value` (the last
observed verdict, if any), and `partial_ctx` (the step-entry-level
partial context reconstructed from the run's step-entry log). The
HTTP surface exposes these as top-level fields on the failed launch
response, and `finalize_run` populates `RunRecord.result_ref` with an
envelope of shape `{ "error": { message, failed_step, verdict_value },
"partial_ctx": ... }` so a post-mortem tool can retrieve the same
diagnostics without re-parsing the error message.

Skip does **not** raise `FlowEval` — Skip is a completing outcome and
the flow continues normally. Only real failures (`Blocked`,
dispatcher errors, timeouts) surface via the error path.

## See also

- `mse://guides/blueprint-authoring` — Blueprint document shape,
  `agents[].verdict` contract, flow node kinds.
- `mse://guides/dsl-authoring` — `flow_dsl` / `bp_dsl` reference,
  pipeline default wiring, gate / retry expansion.
- `mse://guides/agent-md-authoring` — SubAgent prompt shape,
  verdict tiers (Pass / Blocked / Skip), Output contract.
- `mse://blueprints/samples/09-skip-on-example` — runnable
  `.bp.lua` demonstrating the middle-stage `skip_on` idiom.
- `mse://blueprints/samples/07-dsl-pipeline` — sibling pipeline
  sample (no skip_on, with `halt_on` + retry).
