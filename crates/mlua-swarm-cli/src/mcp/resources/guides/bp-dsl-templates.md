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

Sibling fixes (`halted_at` default = GH #60, `worker_binding` compile-lint
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
| `--binding` | Every operator agent's `profile.worker_binding`. | `claude` |
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
| `--binding` | The agent's `profile.worker_binding`. | `claude` |
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
| `--binding` | Every operator agent's `profile.worker_binding`. | `claude` |
| `-o` / `--out` | Write to this path instead of stdout. | stdout |

The `verdict` template's 3-stage count is deliberate: stage identity ties
to role (analyzer produces input, reviewer issues the verdict, publisher
consumes on PASS). Variable stage counts would change the flow shape,
not just role names — use `pipeline` if you need N stages without
verdict gating.

## Rendered shape guarantees

Every template's output:

- Passes `mse bp build` compile-lint on first run (including the GH #61
  `worker_binding` gate and the GH #60 `halted_at` default).
- Uses `require("bp_dsl")` (`pipeline` / `verdict`) or `require("flow_dsl")`
  (`single`) — no other DSL crates.
- Sets every operator agent's `profile.worker_binding` to `--binding`.
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
