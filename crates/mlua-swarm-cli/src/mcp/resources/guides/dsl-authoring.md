# mse — Blueprint DSL (`flow_dsl` / `bp_dsl`) authoring guide

This guide covers the pure-Lua internal DSL for authoring a Blueprint's
`flow` (and, for `bp_dsl`, common multi-stage pipeline shapes) without
hand-writing the raw `flow.ir` JSON. For the Blueprint document shape itself
(top-level fields, `AgentDef`, versioning) start at
`mse://guides/blueprint-authoring` — this guide assumes that vocabulary and
only documents the Lua authoring layer on top of it.

A `.bp.lua` script is plain Lua that `require`s `flow_dsl` and/or `bp_dsl`
and `return`s a Blueprint-shaped table as its last expression. `mse bp build
<script>.bp.lua` runs the script, resolves `$file`/`$agent_md` refs, runs a
best-effort compile lint, and emits the built Blueprint JSON — see
§ Migrating a hand-written JSON Blueprint below for the full workflow.

`flow_dsl` (`F = require("flow_dsl")`) wraps the `flow.ir` Expr/Node
vocabulary; `bp_dsl` (`B = require("bp_dsl")`) depends on `flow_dsl` and adds
Blueprint-level pipeline sugar. `flow_dsl` has zero dependencies — it does
not `require` `bp_dsl`.

## Expr ops

Every `flow_dsl` Expr wrapper (`F.p(...)` / `F.lit(...)`, or anything a
method chain returns) emits a plain Lua table shaped like a `flow.ir` `Expr`
(the same 20-op vocabulary `mse://guides/blueprint-authoring` § Expr ops
documents). `E` below stands for any Expr wrapper; anywhere an Expr is
expected, a raw Lua value is also accepted (auto-wrapped as `lit` by
`F.unwrap`).

| op            | flow_dsl call form                          | notes                                                                 |
|----------------|----------------------------------------------|------------------------------------------------------------------------|
| `path`         | `F.p("$.x")`                                 | Read a value from ctx.                                                 |
| `lit`          | `F.lit(v)`                                   | A literal JSON-serializable value.                                     |
| `eq`           | `E:eq(other)`                                | Structural equality.                                                    |
| `ne`           | `E:ne(other)`                                | Structural inequality.                                                  |
| `lt`           | `E:lt(other)`                                | `<` comparison.                                                         |
| `lte`          | `E:lte(other)`                               | `<=` comparison.                                                        |
| `gt`           | `E:gt(other)`                                | `>` comparison.                                                         |
| `gte`          | `E:gte(other)`                               | `>=` comparison.                                                        |
| `not`          | `E:Not()`                                    | Boolean negation.                                                       |
| `and`          | `E:And(other)` (pairwise) / `F.all{e1, e2, ...}` (N-ary) | `:And()` always emits a 2-element `args` (chain for nested `and`s); `F.all{...}` emits one flat N-ary node regardless of list length. |
| `or`           | `E:Or(other)` (pairwise) / `F.any{e1, e2, ...}` (N-ary) | Same split as `and`/`F.all` — `:Or()` pairwise, `F.any{...}` flat N-ary. |
| `exists`       | `E:exists()`                                 | `true` iff `E` resolves to a non-null value.                            |
| `add`          | `E + other`                                  | Arithmetic operator overload (`Expr.__add`).                            |
| `sub`          | `E - other`                                  | `Expr.__sub`.                                                           |
| `mul`          | `E * other`                                  | `Expr.__mul`.                                                           |
| `div`          | `E / other`                                  | `Expr.__div`.                                                           |
| `mod`          | `E % other`                                  | `Expr.__mod`.                                                           |
| `len`          | `E:len()`                                    | Element/char/key count.                                                 |
| `in`           | `E:contains(needle)`                         | `E` is the haystack; `true` iff `needle` is a member.                   |
| `call_extern`  | `F.call_extern(name, {arg1, arg2, ...})`     | `name` -> wire `ref` (the extern registry key); `args` accepts Expr wrappers or raw values (auto-`lit`, same convention as `F.all`/`F.any`). |

Comparison operators (`< <= == ...`) are deliberately **not** overloaded on
the Expr metatable: the Lua VM coerces their result to a VM-level boolean
before any metamethod return value could survive as an AST table, so
`:lt()` / `:eq()` / ... method-chain calls are the only way to build a
comparison Expr — there is no `E1 < E2` shorthand.

## Node builders

`flow_dsl` exposes exactly **6** Node builders, matching 6 of the 7
`flow.ir` Node kinds (`step` / `seq` / `branch` / `loop` / `assign` / `try`).
Each returns a plain Lua table (no metatable) shaped like a `flow.ir` `Node`.

| kind     | flow_dsl builder                                   | field mapping                                                                                     |
|----------|-------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `step`   | `F.step{id=, agent=, input=, out=}`                    | `agent` -> wire `ref`; `input` -> wire `in` (`in` is a Lua reserved word); `out` -> wire `out`. `id` is an author-facing label only, discarded — `flow.ir` steps have no identity of their own. |
| `seq`    | `F.seq{node1, node2, ...}`                             | The list -> wire `children`, evaluated in order.                                                    |
| `branch` | `F.branch{cond=, on_true=, on_false=}`                 | `cond` -> wire `cond`; `on_true` -> wire `then`; `on_false` -> wire `else` (`then`/`else` are Lua reserved words). |
| `loop_`  | `F.loop_{counter=, cond=, max=, body=}`                | `counter` -> wire `counter`; `cond` -> wire `cond`; `max` -> wire `max`; `body` -> wire `body` (`loop` is a Lua reserved word, hence the trailing underscore). |
| `assign` | `F.assign{at=, value=}`                                | `at` -> wire `at`; `value` -> wire `value`. Pure ctx transform, no agent dispatch.                   |
| `try_`   | `F.try_{body=, catch=, err_at=}`                       | `body` -> wire `body`; `catch` -> wire `catch`; `err_at` -> wire `err_at` (optional — omitted entirely from the emitted table when not given, matching the wire schema's default). `try` is a Lua reserved word, hence the trailing underscore. |

**`fanout` has no `flow_dsl` builder.** `flow.ir`'s `Node` schema has a
7th kind, `fanout` (`items` / `bind` / `body` / `join` / `out`, evaluated
per-item over an array with `all` / `any` / `race` / `all_settled` join
modes — see `mse://guides/blueprint-authoring` § Flow node kinds), but
`flow_dsl` does not currently expose a dedicated builder for it. Author a
`fanout` Node by hand with `F.raw(t)`, the general escape hatch that
treats an arbitrary raw AST table as an Expr/Node wrapper passthrough (no
validation — `t` is emitted verbatim). This is a current gap in `flow_dsl`'s
surface, not an intentional omission from the `flow.ir` vocabulary; do not
read the absence of a `fanout` row above as `fanout` itself being
deprecated or unsupported by the engine.

## `bp_dsl` pipeline conventions

`bp_dsl` (`B = require("bp_dsl")`) adds Blueprint-level authoring sugar on
top of `flow_dsl`. It does not gate-keep the Blueprint document's field
names — the Blueprint schema itself remains the source of truth for what is
valid there.

- **`B.bp{ id=, agents=, flow=, ... }`** — a passthrough: the whole table is
  returned verbatim. Anything beyond `id`/`flow` (`agents`, `operators`,
  `strategy`, `metadata`, ...) passes through unchanged.
- **`B.agent{ md=, verdict=, ... }`** — an `$agent_md` file-ref `AgentDef`
  entry: `{ ["$agent_md"] = md, verdict = verdict, ... }`. Every sibling
  field besides `md` passes through verbatim, mirroring the loader's own
  shallow-merge-onto-`$agent_md` semantics (see
  `mse://guides/blueprint-authoring` § `$agent_md` file-ref expansion).
- **`B.stage "id" { agent=, input=, out=, gate=, halt_on=, retry= }`** — a
  curried 2-arg stage record constructor. Returns a plain "stage record"
  table, **not** yet an AST Node — `B.pipeline` is what turns stage records
  into `flow.ir` Nodes. A stage's own `halt_on` overrides `B.pipeline`'s
  pipeline-wide default for that stage only.
- **`B.pipeline{ stage..., halt_on={...}, halted_at="$...", done="$..." }`**
  — the default-wiring authoring sugar this module exists for. Positional
  entries are stage records; `halt_on` / `halted_at` / `done` are
  pipeline-wide options. Returns a `seq` Node.

### Default in/out

A stage's `input` defaults to `$.d.{stage_id}`; its `out` defaults to
`$.{stage_id}`. An explicit `input`/`out` on the stage record overrides the
default (per-stage `input` may also be a `B.from` placeholder — see
§ `B.from` below).

### Chained pipelines

`chain = true` at the top level of the `B.pipeline{}` spec opts the pipeline
into stage-to-stage chaining: stage N (N ≥ 2) whose `input` is nil defaults
to `$.{stage[N-1]_id}` — the previous stage's own `out` — instead of the
`$.d.{stage_id}` default. Stage 1's default is unchanged (still
`$.d.{stage_1_id}`), because there is no earlier stage to chain from; seed it
via the launcher's `init_ctx` as usual (`init_ctx = { d = { {stage_1_id} =
... } }`).

```lua
local flow = B.pipeline({
  B.stage "ingest"    { agent = "ingest" },
  B.stage "transform" { agent = "transform" },
  B.stage "emit"      { agent = "emit" },
  chain = true,
  halted_at = "$.halted_at",
  done      = "$.pipeline_complete",
})
-- ingest:    in = $.d.ingest, out = $.ingest        (stage 1 unchanged)
-- transform: in = $.ingest,   out = $.transform     (chained from ingest)
-- emit:      in = $.transform, out = $.emit         (chained from transform)
```

An explicit per-stage `input` (either a path string or a `B.from`
placeholder) still overrides the chained default. Retry `fix` stages are not
chained: they keep the R1 default so the fixer's input can be seeded
independently of the review stage's output. Omitting `chain` (or setting it
to `false`) preserves the R1 default (`$.d.{stage_id}`) in every position —
the pre-existing behavior.

### Automatic verdict gate

Immediately after each stage's `step` Node, a `branch` Node is inserted
whose `cond` is `eq(path(<out>.parts["verdict"]), lit(halt_on_value))`
(`or`-combined across every `halt_on` value when there's more than one — a
stage's own `halt_on` field overrides the pipeline-wide default). The
gate's `then` (halt) branch is `assign{at=halted_at, value=lit(stage_id)}`
only — every remaining stage is skipped. The gate's `else` branch nests the
rest of the pipeline (the following stage's step + its own gate, and so on);
the innermost `else` (after the last stage's gate) is
`assign{at=done, value=lit(true)}` when `done` was given, or an empty
`seq{}` otherwise. `gate = false` on a stage record opts that one stage out
of gate insertion entirely — its step Node is spliced directly into the
enclosing `seq`, and the rest of the pipeline continues unconditionally
(not nested under an `else`).

Because the gate always addresses `<out>.parts["verdict"]`, a stage whose
verdict drives a `B.pipeline` gate must stage its verdict as a **named
part** called `verdict` (Pattern B in `mse://guides/blueprint-authoring` §
Returning verdicts to drive BP flow) — `mse_worker_submit(name="verdict",
body=...)` — not as the plain step body (Pattern A). Declaring
`verdict = { channel = "part", values = {...} }` on that stage's `AgentDef`
turns this convention into a compile-time + completion-time check (see the
same guide section, § Enforcing verdict contracts).

### Retry

`retry = { max=N, fix=<stage record>, counter="$.path" }` on a stage record
expands to 3 parts, in order: (1) the stage's own `step` Node; (2)
`loop_{counter=<counter path>, cond=<lt(counter,max) AND <the gate cond
above>>, max=max+1, body=seq{fix step, stage step re-run}}`; (3) the
ordinary verdict gate (evaluated once more after the loop settles, or
spliced in directly if `gate=false`). `counter` is optional; when omitted
the loop counter path defaults to `"$.{stage_id}_n"`. The `fix` stage record
goes through the same default in/out wiring as any other stage.

### `B.from`

`B.from "stage_id"` is an unresolved reference to another stage's `out`
path. `B.pipeline` resolves every stage's (and every retry `fix` stage's)
`out` path up front, before any Node is assembled, so both forward and
backward references work — a stage may reference one declared later in the
same pipeline. Referencing an undefined stage id is an `error()` at
`B.pipeline` time.

## Migrating a hand-written JSON Blueprint to `.bp.lua`

1. **Rewrite**: translate the existing JSON `flow` (and any `agents[]` /
   `operators[]`) into flow_dsl/bp_dsl calls, stage by stage. When a stage's
   `in`/`out` path matches bp_dsl's defaults (`$.d.{stage_id}` / `$.{stage_id}`),
   omit the explicit `input=`/`out=` override; otherwise carry the original
   path over verbatim as an explicit override (pre-existing Blueprints often
   use historical path names that predate the defaults — overriding is the
   expected, fully supported case).
2. **Build**: `mse bp build <script>.bp.lua -o rebuilt.json` (or omit `-o` for
   stdout). This runs `dsl::build_bp_from_script`, a best-effort compile lint
   (step 3 below), and JSON emission in one pass.
3. **AST-equality check**: diff `rebuilt.json` against the original
   hand-authored JSON with a `serde_json::Value` equality assertion
   (key-order-insensitive) — the same technique the repo's DSL
   JSON-equivalence tests use as a permanent regression guard. A one-off
   migration should add (or extend) an equivalence regression test like
   these rather than eyeballing the diff once.
4. **Compile lint**: `mse bp build`'s step 2 already runs this automatically —
   it resolves `$file`/`$agent_md` refs relative to the script's directory and
   runs `Compiler::compile` against a lint registry, surfacing
   `CompileError::VerdictChannelMismatch` / `VerdictValueNotInContract` (GH #50)
   as a hard CLI error. When refs can't be resolved (they may live outside the
   script's own tree), the lint is explicitly reported as skipped — never
   silently dropped. A hard compile-lint failure here means the DSL rewrite
   introduced a real contract violation, not just a shape mismatch.
5. **Smoke run**: either `mse bp build --register` against a running
   `mse serve` (full worker dispatch), or — offline, no server — the
   `flow.eval` Lua binding smoke pattern (see
   `tests/dsl_pipeline.rs`'s `eval_smoke_runs_a_small_flow_via_flow_eval`):
   preload `flow_dsl`, build the flow, and run it end-to-end through
   `mlua_flow_ir::module(&lua)`'s `flow.eval` with a stub dispatcher function
   — proves the flow shape evaluates correctly with zero network/process
   dependencies.

## Samples

Two `.bp.lua` samples are bundled as MCP resources, both build-tested
against `dsl::build_bp_from_script` in CI so they cannot silently drift from
what the DSL actually compiles.

### `mse://blueprints/samples/06-dsl-verdict-loop`

A hand-written `flow_dsl`-only reproduction of
`mse://blueprints/samples/02-verdict-loop` — this sample's loop/branch shape
is written directly with `F.seq`/`F.loop_`/`F.branch`, not `bp_dsl`'s
opinionated gate/retry sugar:

```lua
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
```

### `mse://blueprints/samples/07-dsl-pipeline`

A `bp_dsl` `B.pipeline{}` Blueprint — three verdict-gated stages
(`analyze` -> `review` -> `publish`) wired entirely from default in/out
derivation, with a bounded fix-and-regate retry loop on the middle stage:

```lua
local F = require("flow_dsl")
local B = require("bp_dsl")

local flow = B.pipeline({
  B.stage "analyze" { agent = "analyzer" },
  B.stage "review" {
    agent = "reviewer",
    retry = {
      max = 2,
      fix = B.stage "fix" { agent = "fixer", input = B.from "review" },
    },
  },
  B.stage "publish" { agent = "publisher" },
  halt_on = { "BLOCKED" },
  halted_at = "$.halted_at",
  done = "$.pipeline_complete",
})
```

Note that `B.from "review"` here is a **forward reference within the
retry block** — the `fix` stage's `input` reads the `review` stage's own
`out` path — resolved by `B.pipeline`'s stage-registration pass before any
Node is built, so declaration order inside the `retry` table does not
matter.

## Where to go next

- The Blueprint document shape this DSL emits (top-level fields, `AgentDef`,
  `flow.ir` Node/Expr vocabulary in raw JSON form, versioning):
  `mse://guides/blueprint-authoring`.
- The live Blueprint JSON Schema: `mse://api/blueprint-schema`.
- Both samples above, ready to run or adapt: `mse://blueprints/samples/06-dsl-verdict-loop`,
  `mse://blueprints/samples/07-dsl-pipeline`.
