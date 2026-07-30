# bp_dsl authoring templates (`mse bp new` / `bp_new`)

Scaffold a `.bp.lua` from a bundled template with every currently-mandatory
field pre-filled, so new pipelines compile-lint OK on first `mse bp build`
instead of round-tripping through dispatch-time failures.

- Sibling CLI: `mse bp new <template> <name> [flags...]`
- Sibling MCP tool: `bp_new` (same flag surface, plus `out`)
- GH #62 Axis A. Axis B (lint→patch mapping / `mse bp fix`) is a
  follow-up; the DSL parser stays strict — the "fuzzy" scope is only the
  lint failure → concrete fix hint layer.

## Why templates

Sibling fixes (`halted_at` default = GH #60, explicit Runner compile-lint
= GH #61) tightened the DSL's mandatory-field contract. Templates
front-load those fields into a legal shape so an author never has to
learn each one by hitting the failure. Every rendered `.bp.lua`
round-trips through `mse bp build` on first run.

## Templates

### `pipeline` — N-stage main-ai pipeline

Minimal `B.pipeline{}` with one operator agent per stage, `halted_at` and
`done` set, `strict_refs` + `strict_kind` on.

```
mse bp new pipeline hello --stages greet,echo
mse bp new pipeline hello --stages greet,echo,farewell --operator main-ai --binding claude
mse bp new pipeline hello -o hello.bp.lua
```

Flags:

| flag | meaning | default |
|---|---|---|
| `--stages` | Comma-separated stage names — one agent emitted per stage. | `stage1,stage2` |
| `--operator` | Operator role name every agent points at. | `main-ai` |
| `--binding` | Every operator agent's `ws_operator` Runner variant. | `claude` |
| `-o` / `--out` | Write to this path instead of stdout. | stdout |

### `single` — one-agent one-step

Minimal `F.step{}` shape (no `bp_dsl` pipeline sugar). Useful when the
whole Blueprint is one agent's job.

```
mse bp new single solo-run --agent solo
mse bp new single solo-run --agent solo --binding claude -o solo.bp.lua
```

Flags:

| flag | meaning | default |
|---|---|---|
| `--agent` | Sole agent's name (also the step's `id` and `out` key). | `solo` |
| `--operator` | Operator role name. | `main-ai` |
| `--binding` | The agent's `ws_operator` Runner variant. | `claude` |
| `-o` / `--out` | Write to this path instead of stdout. | stdout |

### `verdict` — 3-stage verdict-gated with retry-through-fixer

Fixed 3-stage shape (mirrors `mse://blueprints/samples/07-dsl-pipeline`):

- Stage 1 (analyze) — produces the input for the reviewer.
- Stage 2 (review) — verdict-gated with `channel = "part", values = ["PASS", "BLOCKED"]`; on BLOCKED, retries a bounded fix loop through a `fixer` agent.
- Stage 3 (publish) — runs only when review reaches PASS.

```
mse bp new verdict review-loop
mse bp new verdict review-loop --stages analyze,review,publish
mse bp new verdict review-loop -o review-loop.bp.lua
```

Flags:

| flag | meaning | default |
|---|---|---|
| `--stages` | 3-slot positional override — analyze / review / publish role names. Fewer than 3 → remaining slots use defaults; more than 3 → tail ignored. | `analyze,review,publish` |
| `--operator` | Operator role name. | `main-ai` |
| `--binding` | Every operator agent's `ws_operator` Runner variant. | `claude` |
| `-o` / `--out` | Write to this path instead of stdout. | stdout |

The `verdict` template's 3-stage count is deliberate: stage identity ties
to role (analyzer produces input, reviewer issues the verdict, publisher
consumes on PASS). Variable stage counts would change the flow shape,
not just role names — use `pipeline` if you need N stages without
verdict gating.

### `fanout` — N parallel checkers + aggregate (GH #82)

`F.fanout` shape: N independent checker agents, one dispatch per lane,
their lane results collecting into `$.results` (`join = "all"`), and a
fixed `aggregate` stage consuming that array. This is the shape
`bp_dsl` used to require `F.raw()` for; GH #82 added the 7th (and
final) Node builder to `flow_dsl` so the whole flow.ir Node grammar
is now reachable from the DSL.

```
mse bp new fanout ci-gate
mse bp new fanout ci-gate --stages lint,test,build
mse bp new fanout ci-gate -o ci-gate.bp.lua
```

Flags:

| flag | meaning | default |
|---|---|---|
| `--stages` | Comma-separated names for the parallel checkers — one agent emitted per checker (the aggregate stage's id is fixed at `aggregate`). | `checker1,checker2` |
| `--operator` | Operator role name every agent points at. | `main-ai` |
| `--binding` | Every operator agent's `ws_operator` Runner variant. | `claude` |
| `-o` / `--out` | Write to this path instead of stdout. | stdout |

Each checker reads its own `$.d.<checker>` slot at launch time (the
`$.d.<stage>` seeding convention `pipeline` and `verdict` use), so
seed every checker under `d` when starting a run:

```
swarm_run(blueprint = ..., init_ctx = { d = { lint = "...", test = "...", build = "..." } })
```

**Lane arithmetic — why the body is a branch cascade.** A fanout `body`
runs once *per item*, whole. A body listing all N checkers in a `seq`
would therefore run every checker for every item: N x N dispatches. The
`--stages` list means N *different* agents, one per lane, and
`Step.ref` is a static string on the wire — so per-item agent selection
is a `branch` on the bound `$.item`. That is what the template renders:
N checkers produce N-1 nested branches, each lane dispatches exactly one
step, and the last checker is the terminal `else` (the item set is
closed — it is the literal array the template emits alongside the
fanout). `--stages solo` degenerates to a bare `F.step` body.

```lua
body = F.branch({
  cond     = F.p("$.item"):eq("lint"),
  on_true  = F.step({ agent = "lint", input = F.p("$.d.lint"), out = F.p("$.branch_out") }),
  on_false = F.branch({
    cond     = F.p("$.item"):eq("test"),
    on_true  = F.step({ agent = "test", input = F.p("$.d.test"), out = F.p("$.branch_out") }),
    -- Closed item set: the last checker is the fallthrough.
    on_false = F.step({ agent = "build", input = F.p("$.d.build"), out = F.p("$.branch_out") }),
  }),
}),
```

The rendered flow uses `F.fanout{items, bind, body, join, out}` with
`join = "all"` (every lane runs, results collect in order). To
short-circuit on the first success, first settlement, or gather
per-item status without raising, switch `join` to `"any"` /
`"race"` / `"all_settled"` respectively — the runtime already
supports all four modes; see `mse://guides/blueprint-authoring`
§ "Flow node kinds" for the semantics of each, and § "Fanout lanes,
`$.results`, and the aggregate gate" for what `$.results` actually
holds and why the `aggregate` stage is the only way to gate on it.

Live sample: `mse://blueprints/samples/10-fanout` — the *homogeneous*
variant of this shape (one agent fanned out over an item array, no
branch cascade), which is what you want when every lane runs the same
checker over a different payload.

## Rendered shape guarantees

Every template's output:

- Passes `mse bp build` compile-lint on first run (including the GH #61
  explicit Runner gate and the GH #60 `halted_at` default).
- Uses `require("bp_dsl")` (`pipeline` / `verdict`) or `require("flow_dsl")`
  (`single` / `fanout`) — no other DSL crates.
- Sets every operator agent's platform-neutral `ws_operator` Runner variant to `--binding`.
- Sets `strategy = { strict_refs = true, strict_kind = true }`.
- Ships `TODO:` markers in every `system_prompt` and `metadata.description` —
  intentional: the author fills these in, and a stray `TODO:` in a
  registered Blueprint is a visible reminder.

## MCP `bp_new` tool

The MCP twin has the same flag surface plus an `out` path (writes the
rendered `.bp.lua` server-side, relative to the mse-mcp process CWD).

- `out` set: response is `{status: "scaffolded", template, name, out, bytes, guide_ref}`.
- `out` omitted: response is `{status: "scaffolded", template, name, bytes, script, guide_ref}` — the rendered `.bp.lua` text lives on `script`.
- Unknown template / render failure: `{status: "error", stage: "render", template, name, error}` with the accepted-template list in `error`.
- `out` write failure: `{status: "error", stage: "write_out", template, name, out, error}`.

## Non-goals (deferred to Axis B)

The `mse bp new` surface is prevention-only. Curing an existing `.bp.lua`
that fails compile-lint is out of scope here — that's Axis B (lint
failures gain concrete `fix_hint` payloads and, where safe, an `mse bp
fix <file> --lint <key>` auto-apply). Axis B rides on top of each
sibling lint kind as it lands; scaffolding closes the on-first-write
gap, Axis B closes the on-edit gap.
