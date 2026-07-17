--- bp_dsl.lua — pure-Lua internal DSL for the Blueprint vocabulary.
---
--- `B = require("bp_dsl")`. Depends on `flow_dsl` (`F`) for Expr / Node
--- construction; `flow_dsl` does NOT depend on `bp_dsl` (one-way
--- dependency).
---
--- `B.pipeline{}` is the authoring sugar this module exists for: default
--- `in` / `out` wiring, automatic per-stage verdict-gate insertion, and a
--- 3-part retry-loop expansion. `B.stage` is a curried 2-arg constructor
--- (`B.stage "id" { ... }`) that returns a plain "stage record" — NOT yet
--- an AST Node; `B.pipeline` is what turns stage records into flow.ir
--- Nodes.

local F = require("flow_dsl")

local M = {}

--- `B.bp{ id=, agents=, flow=, ... }` — the whole Blueprint table.
--- bp_dsl does not gate-keep field names: anything beyond `id` / `flow`
--- (e.g. `agents`, `operators`, `strategy`, `metadata`) passes through
--- verbatim — the Blueprint schema itself is the source of truth for
--- what's valid there.
function M.bp(t)
  return t
end

--- `B.agent{ md=, verdict=, ... }` — an `$agent_md` file-ref `AgentDef`
--- entry: `{ ["$agent_md"] = md, verdict = verdict, ... }`. Every sibling
--- field besides `md` passes through verbatim, mirroring the loader's own
--- shallow-merge-onto-`$agent_md` semantics (see the guide's `$agent_md`
--- file-ref expansion section).
function M.agent(t)
  local out = {}
  for k, v in pairs(t) do
    if k ~= "md" then
      out[k] = v
    end
  end
  out["$agent_md"] = t.md
  return out
end

-- ── Stage records + B.from placeholders ──────────────────────────────────

local Placeholder = {}
Placeholder.__index = Placeholder

local function is_placeholder(v)
  return type(v) == "table" and getmetatable(v) == Placeholder
end

--- `B.from "stage_id"` — an unresolved reference to another stage's `out`
--- path. Resolved by `B.pipeline` once every stage's `out` is known (so
--- forward references — a stage referencing one declared later in the
--- same pipeline — work too); referencing an undefined stage id is an
--- `error()` at `B.pipeline` time.
function M.from(stage_id)
  return setmetatable({ stage_id = stage_id }, Placeholder)
end

--- `B.stage "id" { agent=, input=, out=, gate=, halt_on=, retry= }` —
--- curried 2-arg stage constructor. Returns a plain "stage record" table
--- (NOT an AST Node yet — `B.pipeline` expands stage records into
--- flow.ir Nodes, applying the default-wiring + gate + retry rules
--- below). `halt_on` here overrides `B.pipeline`'s pipeline-wide default
--- for this stage only.
function M.stage(id)
  return function(t)
    t.id = id
    return t
  end
end

-- ── B.pipeline: default wiring + gate + retry expansion ─────────────────

-- A stage's default `in` path (used when the stage record's own `input`
-- is nil and it isn't a `B.from` placeholder either).
local function default_input_path(stage_id)
  return "$.d." .. stage_id
end

-- A stage's default `out` path.
local function default_out_path(stage_id)
  return "$." .. stage_id
end

-- A stage's default retry-loop counter path (used when the stage's own
-- `retry.counter` is nil).
local function default_counter_path(stage_id)
  return "$." .. stage_id .. "_n"
end

-- The verdict-gate condition for one stage: `eq(<out>.parts["verdict"],
-- lit(v))` for a single halt_on value, or an N-ary `or` of that shape
-- across every halt_on value when there's more than one.
local function gate_cond(out_path, halt_on_values)
  local verdict_path = out_path .. '.parts["verdict"]'
  if #halt_on_values == 1 then
    return F.p(verdict_path):eq(halt_on_values[1])
  end
  local eqs = {}
  for i, v in ipairs(halt_on_values) do
    eqs[i] = F.p(verdict_path):eq(v)
  end
  return F.any(eqs)
end

-- Resolve a stage's `input` field to a path string: `B.from "x"`
-- placeholders resolve against `outs` (populated for every stage, and
-- every retry `fix` stage, before any Node is built — so both forward and
-- backward references work); `nil` falls back to the R1 default; any
-- other value is assumed to already be a path string.
local function resolve_input_path(input, stage_id, outs)
  if input == nil then
    return default_input_path(stage_id)
  end
  if is_placeholder(input) then
    local target = outs[input.stage_id]
    if target == nil then
      error(
        'bp_dsl: B.from("' .. tostring(input.stage_id) .. '") references an undefined stage',
        0
      )
    end
    return target
  end
  return input
end

-- Build the `step` Node for one stage record. `outs` must already carry
-- this stage's own resolved `out` path (`rec._out`, set by the
-- register-outs pass below) so R6 `B.from` references to THIS stage
-- resolve correctly even from earlier stages in the list.
local function build_step(rec, outs)
  local input_path = resolve_input_path(rec.input, rec.id, outs)
  return F.step({
    id = rec.id,
    agent = rec.agent,
    input = F.p(input_path),
    out = F.p(rec._out),
  })
end

--- `B.pipeline{ stage..., halt_on={"BLOCKED"}, halted_at="$.halted_at",
--- done="$.xxx" }` — the default-wiring authoring sugar. Positional
--- entries are stage records (`B.stage "id" {...}`); `halt_on` /
--- `halted_at` / `done` are pipeline-wide options. Returns a `seq` Node
--- (a raw flow.ir table).
---
--- ## Default in/out
---
--- A stage's `input` defaults to `$.d.{stage_id}`; its `out` defaults to
--- `$.{stage_id}`. An explicit `input` / `out` on the stage record
--- overrides the default (per-stage `input` may also be a `B.from`
--- placeholder — see R6 below).
---
--- ## Automatic verdict gate
---
--- Immediately after each stage's `step` Node, a `branch` Node is
--- inserted whose `cond` is `eq(path(<out>.parts["verdict"]),
--- lit(halt_on_value))` (`or`-combined across every `halt_on` value when
--- there's more than one; a stage's own `halt_on` field overrides the
--- pipeline-wide default). The gate's `then` (halt) branch is
--- `assign{at=halted_at, value=lit(stage_id)}` only — every remaining
--- stage is skipped. The gate's `else` branch nests the rest of the
--- pipeline (the following stage's step + its own gate, and so on); the
--- innermost `else` (after the LAST stage's gate) is
--- `assign{at=done, value=lit(true)}` when `done` was given, or an empty
--- `seq{}` otherwise. `gate = false` on a stage record opts that one
--- stage out of gate insertion entirely — its step Node is spliced
--- directly into the enclosing `seq`, and the rest of the pipeline
--- continues unconditionally (NOT nested under an `else`).
---
--- ## Retry
---
--- `retry = { max = N, fix = <stage record>, counter = "$.path" }` on a
--- stage record expands to 3 parts, in order: (1) the stage's own `step`
--- Node; (2) `loop_{counter = <counter path>, cond = <lt(counter, max)
--- AND <the gate cond above>>, max = max + 1, body = seq{fix step, stage
--- step re-run}}`; (3) the ordinary verdict gate (evaluated once more,
--- after the loop settles — or spliced in directly if `gate = false`).
--- `counter` is optional; when omitted the loop counter path defaults to
--- `"$.{stage_id}_n"`. The `fix` stage record goes through the same
--- default in/out wiring as any other stage.
---
--- ## `B.from`
---
--- Resolved against every stage's `out` path (including retry `fix`
--- stages) before any Node is assembled, so forward references work; an
--- unresolved reference is an `error()`.
function M.pipeline(spec)
  local halt_on = spec.halt_on or {}
  -- Default `halted_at` so a pipeline without an explicit halt-site knob
  -- still compiles: the per-stage gate always emits
  -- `assign{at=F.p(halted_at), value=lit(stage_id)}` on its `then` branch,
  -- and a nil target passes through as an empty `path{}` node that fails
  -- shape validation downstream. `"$.halted_at"` matches the bundled
  -- sample's convention (see mse://blueprints/samples/07-dsl-pipeline);
  -- authors who care can still override via `halted_at = "$.custom"`.
  local halted_at = spec.halted_at or "$.halted_at"
  local done = spec.done

  local stages = {}
  for i, rec in ipairs(spec) do
    stages[i] = rec
  end

  -- Pass 1: resolve every stage's (and retry fix stage's) `out` path
  -- up front, so B.from() references work regardless of declaration
  -- order.
  local outs = {}
  local function register_out(rec)
    rec._out = rec.out or default_out_path(rec.id)
    outs[rec.id] = rec._out
  end
  for _, rec in ipairs(stages) do
    register_out(rec)
    if rec.retry ~= nil then
      register_out(rec.retry.fix)
    end
  end

  -- Pass 2: build, from the first stage forward, the step (+ retry loop)
  -- + gate chain, threading `rest_else` (the tail of the pipeline) into
  -- each gate's `else`.
  local function build_from(idx, rest_else)
    if idx > #stages then
      return rest_else
    end
    local rec = stages[idx]
    local this_halt_on = rec.halt_on or halt_on
    local step_node = build_step(rec, outs)
    local rest = build_from(idx + 1, rest_else)

    local children = { step_node }

    if rec.retry ~= nil then
      local fix_step = build_step(rec.retry.fix, outs)
      local max = rec.retry.max
      local counter_path = rec.retry.counter or default_counter_path(rec.id)
      local loop_cond = F.p(counter_path):lt(max):And(gate_cond(rec._out, this_halt_on))
      children[#children + 1] = F.loop_({
        counter = F.p(counter_path),
        cond = loop_cond,
        max = max + 1,
        body = F.seq({ fix_step, step_node }),
      })
    end

    if rec.gate == false then
      children[#children + 1] = rest
      return F.seq(children)
    end

    children[#children + 1] = F.branch({
      cond = gate_cond(rec._out, this_halt_on),
      on_true = F.assign({ at = F.p(halted_at), value = F.lit(rec.id) }),
      on_false = rest,
    })
    return F.seq(children)
  end

  local final_else
  if done ~= nil then
    final_else = F.assign({ at = F.p(done), value = F.lit(true) })
  else
    final_else = F.seq({})
  end

  return build_from(1, final_else)
end

return M
