# mse — Blueprint authoring guide

A **Blueprint** is the unified package of a `flow.ir` program plus the Swarm
extension layers (agent bindings, Operator role definitions, compiler
hints/strategy, metadata). This guide covers the shape you write by hand or
generate programmatically; for the exact, always-current JSON Schema fetch
`mse://api/blueprint-schema`.

## Top-level shape

```jsonc
{
  "schema_version": "0.1.0",          // optional, defaults to the current schema version
  "id": "my-blueprint",               // required, unique within your namespace
  "flow": { "kind": "seq", "children": [] }, // required, a flow.ir Node (see below)
  "agents": [ /* AgentDef[] */ ],      // optional, default []
  "operators": [ /* OperatorDef[] */ ], // optional, default []
  "hints": { "per_agent": {}, "global": {} }, // optional
  "strategy": { "strict_refs": true, "strict_kind": true }, // optional
  "metadata": { "description": "...", "tags": [] }, // optional
  "spawner_hints": { "layers": [] },   // optional, middleware capability keys
  "default_agent_kind": "operator",    // optional, defaults to "operator"
  "default_operator_kind": "automate", // optional, no default (falls through the cascade)
  "degradation_policy": "warn"         // optional, "warn" (default) | "fail" (opt-in, schema-only today — see the worker degradation reporting section in `mse://guides/operator-execution-model`)
}
```

All fields except `id` and `flow` are optional and fall back to sensible
defaults. `deny_unknown_fields` is enforced throughout the schema — a typo in
a field name is a hard parse error, not a silently-ignored key.

## Flow node kinds (`flow.ir` `Node`)

Every node is tagged with a `kind` discriminator:

| kind      | fields                                             | behavior                                                                                     |
|-----------|------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| `step`    | `ref`, `in`, `out`                                    | Dispatch the agent named `ref` with the evaluated `in` expr as input; write the result to `out` (must be a `path` expr). |
| `seq`     | `children` (`Node[]`)                                 | Evaluate children in order, threading the ctx through each.                                    |
| `branch`  | `cond`, `then`, `else`                                | Evaluate `cond` (must resolve to a JSON bool); run `then` if true, `else` if false.             |
| `loop`    | `counter`, `cond`, `body`, `max`                      | Writes `0` to `counter`, then repeats `body` while `cond` is truthy and `counter < max`, incrementing `counter` after each iteration. |
| `fanout`  | `items`, `bind`, `body`, `join`, `out`                | Evaluate `items` to an array; run `body` once per element (bound to `bind` in a branch-local ctx); aggregate into `out` per `join` mode: `all` (every branch runs, array of final ctx), `any` (first success wins), `race` (first to settle wins), `all_settled` (never raises, per-item `{status, value|reason}` record). |
| `try`     | `body`, `catch`, `err_at?`                            | Run `body`; on error, roll back ctx writes, optionally write the error message to `err_at`, then run `catch`. |
| `assign`  | `at`, `value`                                         | Pure ctx transform: evaluate `value` against the ctx snapshot and write it to `at`. No agent dispatch. |

`out` / `at` / `counter` must always be `path` exprs (write targets).

## Worker output: `out` vs named parts (GH #36)

A `step` node's OUTPUT is normally a single JSON value — the worker's
final `mse_worker_submit` `body` — addressable downstream via `{"op":
"path", "at": "$.<step>"}`.

A worker may additionally stage any number of *named* output parts
before completing the attempt: call `mse_worker_submit` with `name` set
(see `mse://guides/mcp-tool-reference` § Named multi-part output) once
per part, then finish with an ordinary plain (no-`name`) submit. The
set of `name` values a worker submits is its **staged-names allowlist**
— that allowlist alone determines what the engine folds into the
`parts` map on that step's OUTPUT. Any other artifact the worker (or a
middleware) emits — most notably the after-run audit sidecar
`audit:<step_ref>` (see § Reading prior-step OUTPUT below) — bypasses
the fold and is only reachable via the Worker axis. A step that staged
at least one part ends up with OUTPUT shape

```jsonc
{ "out": /* the final plain-submit body */ "...", "parts": { "plan.md": "...", "notes": { "todo": "..." } } }
```

instead of the plain final-submit body alone. A downstream step reads a
part with RFC 9535-style bracket-notation path syntax — required for any
key containing a literal `.`, like a filename:

```jsonc
{ "op": "path", "at": "$.<step>.parts[\"plan.md\"]" }
```

Bracket segments chain directly (`$.<step>.parts["a"]["b"]`) or combine
with dot segments in either order (`$.<step>.parts["notes"].todo`); keys
support no escaping (a literal `"` inside a name cannot be represented).

**Author caution**: once a step stages any parts, its OUTPUT becomes an
Object (`{"out": ..., "parts": {...}}`) instead of the plain final-submit
value — a downstream `eq`/`ne` expr comparing `$.<step>` directly against
a string (or other scalar) no longer matches; address `$.<step>.out`
instead (or a `parts[...]` entry). Keeping a worker's staging behavior in
sync with the Blueprint's `in` exprs that read its output is the
Blueprint author's responsibility — nothing in the schema enforces it
automatically.

## Reading prior-step OUTPUT (Worker axis): `context.steps`

The `$.<step>` / `$.<step>.parts[...]` paths above are the **BP axis** —
a downstream step declares what to read at Blueprint-authoring time and
the folded value flows into its `in`. Fine when the caller knows the
shape in advance.

The **Worker axis** is the complementary read-back path for a SubAgent
that decides at runtime which prior-step OUTPUT to pull. Every step's
OUTPUT is dual-recorded in the engine's `OutputStore` and surfaced via
`WorkerPayload.context.steps` as a `StepPointer` (GH #20 Contract C; see
`mse://guides/operator-execution-model` § Hop 4), filtered by the
current step's `ContextPolicy.steps` allowlist / `steps_exclude`
denylist:

```jsonc
"context": {
  "steps": {
    "planner":       { "name": "planner",       "size_bytes": 1834, "file_path": "/…/ctx/planner.json",       "content_url": "…", "sha256": "…" },
    "audit:planner": { "name": "audit:planner", "size_bytes":  412, "file_path": "/…/ctx/audit-planner.json", "content_url": "…", "sha256": "…" }
  }
}
```

Key facts:

- **Pointer-only invariant** — a `StepPointer` never carries the OUTPUT
  content itself (no preview, no content bytes inline). The SubAgent
  fetches the actual bytes via `file_path` (local FS `Read`) or
  `content_url` (server HTTP GET, verifiable against `sha256`). Choose
  `file_path` on same-host SubAgents (the common case); choose
  `content_url` when the SubAgent runs elsewhere.
- **`audit:<step_ref>` is a first-class entry** — after-run audit
  artifacts (GH #34) surface as top-level `context.steps` keys named
  `audit:<step_ref>`, **not** as a nested field of the audited step. The
  BP axis (`$.<step>.audit[...]` or similar) does not reach them — audit
  findings are observational sidecars that the fold-final path drops
  from the BP-chain value but the Data-plane dual-write preserves for
  Worker-axis consumers. See `mse://guides/operator-execution-model` §
  After-run audits.
- **Keys are canonical step names** — the map key is the step's
  canonical name resolved from `AgentMeta.projection_name` (GH #23), so
  `ContextPolicy.steps` / `steps_exclude` entries are matched against
  canonical names, not any renamed alias.
- **BP-chain scope vs Data-plane scope** — the fold-final path
  (`fold_final_and_parts`, `src/core/engine.rs`) only stages a step's
  `mse_worker_submit`-with-`name` artifacts (the `staged_names`
  allowlist from § Worker output) into the BP-chain value. Other
  artifacts (audits, out-of-band submissions) bypass fold but remain
  reachable via `context.steps`.

## Returning verdicts to drive BP flow (canonical pattern)

A **verdict** is a small scalar (e.g. `"PASS"`, `"BLOCKED"`, `"ALLOW"`) an
agent emits so that a downstream `branch` or `loop` node can compare it
via `eq($.<step>, lit("BLOCKED"))` and pick a path. `eq` is a
**structural** compare — the whole value at `$.<step>` must equal the
whole `lit(...)` value. That constraint decides how the agent shapes its
submit body.

Two shapes are canonical; a third is a frequently-attempted anti-pattern.

### Pattern A — plain body carries the verdict scalar

The agent's `mse_worker_submit` body is the verdict literal, nothing
else:

```
mse_worker_submit(body="BLOCKED")
```

- Step OUTPUT is exactly the string `"BLOCKED"` — no `parts` field.
- BP-side: a downstream `"in": {"op": "path", "at": "$.gate"}` observes
  the scalar directly, and
  `{"op": "eq", "lhs": {"op": "path", "at": "$.gate"}, "rhs": {"op": "lit", "value": "BLOCKED"}}`
  matches.
- Trade-off: the submit body has room for the verdict only — no
  human-readable report co-exists with it on that step.
- When to use: pure gates where the verdict is the whole point. Working
  example: `mse://blueprints/samples/03-fn-override` — the `mock-gate`
  agent's system prompt is literally `` Always reply `BLOCKED` ``, and
  the top-level `branch` fires on `eq($.verdict, lit("BLOCKED"))`.

### Pattern B — named part carries the verdict, plain body carries the report

The agent stages the verdict as a named part first, then finishes the
attempt with a plain (unnamed) submit whose body is the human-readable
report:

```
mse_worker_submit(name="verdict", body="BLOCKED")     # stage the verdict
mse_worker_submit(body=<full YAML / markdown report>) # finish the attempt
```

- Step OUTPUT shape becomes
  `{"out": <the full report>, "parts": {"verdict": "BLOCKED"}}`.
- BP-side: the verdict is addressed with bracket notation —
  `{"op": "eq", "lhs": {"op": "path", "at": "$.gate.parts[\"verdict\"]"}, "rhs": {"op": "lit", "value": "BLOCKED"}}`
  — while the full report stays reachable as `$.gate.out` for
  downstream consumers (a resolver agent that needs the failure detail,
  or a report artifact for humans).
- Trade-off: the agent issues two `mse_worker_submit` calls, and the BP
  author must know to address the `parts["verdict"]` entry, not the
  plain step name.
- When to use: gates whose verdict must drive flow *and* whose full
  report is a first-class artifact.

### Anti-pattern — full report in the plain body, `eq` against the step name

An agent that submits a full YAML/markdown report as its only submit
body and expects `eq($.gate, lit("BLOCKED"))` to fire **cannot work**:
`$.gate` resolves to the whole report string; `lit("BLOCKED")` is the
four-character literal; they never compare equal. The `branch`'s `else`
path fires on every dispatch, which reads to a caller like the verdict
path was silently swallowed even though the agent said `BLOCKED`.

Debug rule: if a gate's `then` path never fires while the agent visibly
outputs a verdict word, check the submit shape first. It must be a
scalar (Pattern A) or a named part (Pattern B) — `$.gate` cannot be a
report body that *contains* the verdict word.

### Enforcing verdict contracts (opt-in)

Pattern A/B above are conventions — until an agent opts in, nothing
checks that its submit shape actually matches how a downstream `cond`
addresses it. `AgentDef.verdict` is an **optional** field that turns
that convention into two machine checks. It is strictly additive: an
agent that declares no `verdict` behaves exactly as before, byte for
byte, at both boundaries described below. The working sample
`mse://blueprints/samples/02-verdict-loop` is a live example of this —
its `mock-gate` agent declares no `verdict` field and continues to
register and run unchanged; Pattern A's convention alone is still
enough for it.

Declare a contract on the agent whose output a `cond` will compare:

```jsonc
// channel: "body" — Pattern A, the plain step OUTPUT IS the verdict
"agents": [{
  "name": "gate",
  "verdict": {
    "channel": "body",
    "values": ["PASS", "BLOCKED"]
  }
}]
```

```jsonc
// channel: "part" — Pattern B, the verdict is staged as the named part
"agents": [{
  "name": "gate",
  "verdict": {
    "channel": "part",
    "values": ["PASS", "BLOCKED"]
  }
}]
```

`channel: "part"` addresses one literal part name only —
`mse_worker_submit(name="verdict", body=...)` / `$.gate.parts["verdict"]`
— the way Pattern B is documented above. `values` is a closed set of
tokens; a comparison against anything outside it is a violation.

**Register time (compile, read-only lint).** `Compiler::compile` walks
every `Branch`/`Loop` `cond`'s `Eq`/`Ne`/`In` comparisons of a step
output `Path` against a literal, resolves the `Path` back to its
producing agent, and — only for agents that declared a `verdict` —
checks two things:

- The `Path` addresses the channel the agent declared (bare `$.<step>`
  for `channel: "body"`, `$.<step>.parts.verdict` /
  `$.<step>.parts["verdict"]` for `channel: "part"`). A mismatch fails
  the compile with `CompileError::VerdictChannelMismatch`, naming the
  step, the declared channel, and the channel the `cond` actually
  addressed.
- Every literal compared against that `Path` (including every entry of
  an `In` haystack) is a member of the declared `values`. A literal
  outside the set fails the compile with
  `CompileError::VerdictValueNotInContract`, naming the offending
  literal and the declared set.

Compile fails on the **first** violation found (same posture as the
compiler's other static checks). If the `cond` references an agent
that declared **no** `verdict` field, nothing is rejected — at most a
`tracing::warn!` is emitted, and compilation still succeeds. This is
what keeps every pre-existing Blueprint, and every Blueprint whose
authors haven't opted in yet, compiling unchanged.

**Completion time (server, fail-loud producer gate — all 3 completion
routes).** A contract-bearing agent's attempt can complete through 3
different routes: `POST /v1/worker/submit`, the older
`POST /v1/worker/result`, or the WS Operator fallback (a worker
process that never POSTs at all). GH #50 originally gated only the
first of these, and only for `channel: "body"`; GH #51 closes the
remaining gaps by moving the check to the single choke point every
route funnels through — `Engine::submit_worker_result_trusted` /
`Engine::submit_output`, embedded immediately before the value is
written to `output_tail`, not re-implemented per route handler:

- `channel: "body"` — the completing value must be a member of
  `values`.
- `channel: "part"` — a staged `"verdict"` artifact must exist for the
  attempt (**presence**, not just membership — a worker that never
  calls `POST /v1/worker/artifact?name=verdict` at all is now rejected,
  the gap GH #51 exists to close) AND its value must be a member of
  `values`.
- `ok=false` completions are exempt on every route, identically — a
  transport-level failure (`DispatchOutcome::Blocked`, the flow.ir Try
  path) is not a verdict and is never validated against the contract.
- An agent that declared no contract, or declared a contract for the
  other channel, sees this gate as a no-op — behavior unchanged from
  before GH #50/#51.

A violation is rejected **before** the value reaches `output_tail` / the
flow ctx: `POST /v1/worker/submit` and `POST /v1/worker/result` both
surface HTTP 422, echoing the declared `values` (`channel: "body"`
violations) or naming the missing `"verdict"` part (`channel: "part"`
violations). The WS Operator route has no HTTP response to return a 422
on — a rejected completion there simply never writes its `Final`; the
attempt's `output_tail` has no `Final` and the downstream dispatch path
naturally treats it as incomplete (a `tracing::warn!` is logged
server-side). No new WS protocol message is introduced for this — the
deliberate "zero flow-ir changes" design choice, not a gap left to
fill.

The staging-time check at `POST /v1/worker/artifact?name=verdict`
(`channel: "part"` membership only, not presence) still runs — it gives
the worker the fastest possible feedback the moment it stages a bad
token. The completion-time check above is the backstop that guarantees
enforcement no matter which of the 3 routes an agent's attempt actually
completes through.

Together, the register-time and completion-time boundaries turn both
halves of the silent never-match anti-pattern above into loud
failures: an authoring mistake (`cond` addressing the wrong channel, or
comparing against a token the agent will never emit) stops at register
time; a worker that emits a full report where a token was expected, or
skips staging the verdict part entirely, stops at completion time on
every route it could have completed through. Neither boundary touches
`flow.ir` itself — no new `Expr` forms, no eval hooks, no `FlowNode`
rewriting; `Blueprint.flow` stays exactly what the author wrote, and
the contract lives entirely in the Blueprint/schema/compiler/server
layers described here.

### Declared verdict values must be handled downstream (opt-in strict mode)

The two register-time checks above are the **forward-direction** lint:
"every `Lit` a `cond` compares against must be a member of the agent's
declared `verdict.values`" and "every `cond` must address the declared
channel." The **reverse-direction** lint — "every entry of the declared
`verdict.values` set must be referenced by at least one downstream
`Branch`/`Loop` `cond`" — catches the complementary drift where a flow
author declares a verdict value (e.g. `"BLOCKED"`) but forgets to write
a branch that handles it.

By default the reverse-direction lint only surfaces
`tracing::warn!` — the compile still succeeds. This preserves back-
compat with existing Blueprints that intentionally leave some declared
values as silent-pass informational tokens (an agent may want to
document "we may emit `INFO` as well" without demanding every caller
branch on it).

To promote the warning to a hard `CompileError::VerdictValueUnhandled`,
opt in via `Blueprint.metadata`:

```json
{
  "metadata": {
    "strict_verdict_handling": true
  }
}
```

Under `strict_verdict_handling: true`, `Compiler::compile` rejects any
Blueprint where a contract-bearing agent declares a `verdict.values`
entry that no downstream `Branch`/`Loop` `cond` references. The
diagnostic names the agent, the unhandled value, the full declared
`values` set (so the fix is unambiguous — either add a handler branch
or drop the value from the declaration), and the `Step.ref_` where the
agent is invoked (best-effort — when the agent is invoked at multiple
sites, the first-encountered site is reported).

Two ways to satisfy the strict lint:

1. **Add a branch per declared value.** The canonical shape — one
   `Branch` per value, or one `Branch` per value pair (e.g. `"PASS"` in
   the `then_`, `"BLOCKED"` in the `else_`).
2. **Cover the whole set with one `In`.** An `In` cond whose `Lit`
   haystack lists every declared value counts every entry as handled in
   one node — useful when the flow author wants a single "any of these
   verdict values ⇒ proceed" branch.

The forward and reverse lints run in the same walk, so the strict
setting has no extra runtime cost; either both fire or neither does.
The setting is a Blueprint-level opt-in (per BP, not per agent), so a
flow author who wants the strict check on some agents but not others
can either split those agents into a separate Blueprint or leave the
setting off and rely on the default `tracing::warn!` output.

### Cross-links

- Named-parts wire format and OUTPUT shape: § Worker output: `out` vs
  named parts (above).
- Working samples that exercise Pattern A end-to-end:
  `mse://blueprints/samples/02-verdict-loop` (a `loop` that retries
  while `$.verdict == "BLOCKED"`) and
  `mse://blueprints/samples/03-fn-override` (a `branch` that hands a
  BLOCKED gate result to an approver step).
- Agent-side declaration (which pattern the agent's own Output format
  section commits to): `mse://guides/agent-md-authoring` § Output
  contract: inline body vs `@file:` sentinel.
- The static verifier that surfaces some verdict-related drift
  (`declared_tools` vs wrapper grants, projection-name/parts-shape
  changes downstream): `mse://guides/agent-md-authoring` § Verifying
  how your agent materializes.

## Expr ops (`flow.ir` `Expr`)

Every expr is tagged with an `op` discriminator:

| op       | fields                    | result                                                                 |
|----------|---------------------------|-------------------------------------------------------------------------|
| `path`   | `at` (e.g. `"$.x.y"`)      | Read a value from ctx. Raises if the path is missing.                   |
| `lit`    | `value`                    | A literal JSON value.                                                   |
| `eq`     | `lhs`, `rhs`               | Structural equality.                                                     |
| `ne`     | `lhs`, `rhs`               | Structural inequality.                                                   |
| `lt` / `lte` / `gt` / `gte` | `lhs`, `rhs` | Comparison: both numbers (`f64`) or both strings (lexicographic, Lua `<` parity). Mixed types raise. |
| `not`    | `arg`                      | Boolean negation (truthy-based).                                        |
| `and`    | `args` (array)             | Short-circuit conjunction; empty array → `true`.                        |
| `or`     | `args` (array)             | Short-circuit disjunction; empty array → `false`.                       |
| `exists` | `arg` (expr)               | `true` iff `arg` resolves to a non-`null` value (missing path → `false`, present-but-`null` → `false`). |
| `add` / `sub` / `mul` / `div` / `mod` | `lhs`, `rhs` | Numeric arithmetic (`f64`); `div` / `mod` by zero raises. `mod` follows Lua `%` (result takes the sign of `rhs`). |
| `len`    | `arg`                      | Element count (array), char count (string), or key count (object).      |
| `in`     | `needle`, `haystack`       | `true` if `needle` equals any element of the `haystack` array.          |
| `call_extern` | `ref`, `args` (array) | Invoke a host-registered pure function (`Externs` registry) with the evaluated `args`. Unregistered `ref` raises. Value-shape only — no side effects, no flow control. |

`call_extern` requires the host to register an externs registry
(`TaskLaunchService::with_externs`); without one every `call_extern`
raises an extern error.

Truthy semantics match Lua/JS: `null`/`false` are falsy, everything else
(including `0` and `""`) is truthy.

## Agents (`AgentDef`) and kind resolution

### Two authoring paths

An `AgentDef` can be written in two places, and either is fine:

- **Direct JSON literal (this guide's default form)** — the
  `AgentDef` object appears inline inside the Blueprint JSON. All
  fields (`name`, `kind`, `spec`, `profile.system_prompt`,
  `profile.worker_binding`, `profile.tools`, `meta`, …) are set
  literally in the JSON tree. This is the default authoring shape
  for the samples under `mse://blueprints/samples/*` and for
  programmatic authoring (algocline strategies, skills, dogfood
  harnesses).
- **`$agent_md` file ref** — the entry is a single-key object
  `{ "$agent_md": "agents/foo.md" }` and the loader parses the
  target file's frontmatter (+ Markdown body) into a
  fully-populated `AgentDef`. See the `$agent_md file-ref
  expansion` section below.

Compile-time error messages that name a field (e.g.
`profile.worker_binding`) are actionable on either path — for JSON
authors, add the field to the JSON literal; for `$agent_md` authors,
add it to the `.md` frontmatter. The messages themselves spell both
paths out.

### `AgentDef` shape (JSON-direct form)

Each entry in `agents` maps a name (referenced from `flow.Step.ref`) to a
backend:

```jsonc
{
  "name": "my-agent",
  "kind": "rust_fn",           // lua | rust_fn | agent_block | subprocess | operator
  "spec": { "fn_id": "..." },  // free-form, interpreted per kind
  "profile": { "system_prompt": "...", "model": "...", "tools": [] }, // optional
  "meta": { "description": "...", "tags": [] } // optional
}
```

`AgentKind` is a closed enum (`lua`, `rust_fn`, `agent_block`, `subprocess`,
`operator`) — there is no string-escape-hatch variant. When an agent omits
`kind`, resolution falls through a four-tier cascade (highest to lowest
priority): (1) per-`AgentDef.kind` literal, (2) the Blueprint's top-level
`default_agent_kind`, (3) a CLI-level default (e.g. `mse serve
--default-agent-kind`), (4) the schema `Default` impl (`operator`).

### `$agent_md` file-ref expansion

Instead of writing an `AgentDef` object inline, you can reference an
`agent.md` file (frontmatter + Markdown body) and let the loader expand it:

```jsonc
{ "agents": [ { "$agent_md": "agents/domain-researcher.md" } ] }
```

This parses the file's frontmatter + body into a fully-populated `AgentDef`
(`profile.system_prompt`, `meta`, `spec`, etc.). Sibling keys alongside
`$agent_md` are shallow-merged onto the expanded object afterward — handy for
overriding just `spec.operator_ref` or `meta` while keeping the rest of the
`agent.md` content:

```jsonc
{ "$agent_md": "agents/domain-researcher.md", "spec": { "operator_ref": "role-a" } }
```

**Path hygiene**: refs are resolved relative to the Blueprint file's own
directory. Absolute paths and any `..` parent-directory component are
rejected — refs are sandboxed inside the Blueprint's base-directory subtree.
The same rule applies to the more general `$file` ref (`{"$file": "path"}`),
which substitutes a referenced file's raw string contents anywhere in the
JSON tree (e.g. externalizing a large prompt out of a `Step.in` literal).

### Runners (GH #46): `Blueprint.runners` / `AgentDef.runner` / `runner_ref`

A `Runner` declares the execution shell an agent's Worker IMPL dispatches
into — tool grant, model selection, and runtime capabilities for the
backend it targets. Two variants exist today: `ws_claude_code` (Claude
Code subagent wrapper; `variant` = the wrapper's `subagent_type`, `tools`
mirrors the wrapper frontmatter) and `agent_block_in_process`
(agent-block in-process runtime; `tools` is the effective, enforced tool
set). `AgentDef.kind = agent_block` pairs with an `agent_block_in_process`
Runner; every other `AgentDef.kind` pairs with `ws_claude_code`.

Runners are declared through a named, BP-level registry
(`Blueprint.runners: [{ "name": ..., "runner": {...} }]`) — the same
registry shape as `Blueprint.metas` — and resolved per-agent through a
5-tier cascade (highest priority first):

1. `AgentDef.runner` — an inline `Runner` object on the agent itself.
2. `AgentDef.runner_ref` — a name looked up in `Blueprint.runners`.
3. Legacy fallback: `profile.worker_binding` synthesizes a
   `ws_claude_code` Runner from `{ variant: worker_binding, tools:
   profile.tools }` — **deprecated, kept for one release cycle** while
   Blueprints migrate onto `runner` / `runner_ref`.
4. `Blueprint.default_runner` — a BP-wide registry name, used only when
   no tier above (1–3) applies to this agent.
5. No Runner declared through any tier — the agent has none.

Note tier 3 outranks tier 4: an agent's own `profile.worker_binding`
still wins over the Blueprint's `default_runner`, mirroring the
`AgentInline > MetaRef > BpGlobal` precedence the ctx-supply cascade
already follows (agent-level declarations always beat BP-global ones).

```jsonc
{
  "runners": [
    { "name": "claude-worker", "runner": {
        "backend": "ws_claude_code", "variant": "mse-worker-coder", "tools": ["Read", "Edit"]
    } }
  ],
  "default_runner": "claude-worker",
  "agents": [
    { "name": "coder", "kind": "operator", "spec": { "operator_ref": "role-a" }, "runner_ref": "claude-worker" }
  ]
}
```

At Run start MSE resolves this cascade once into an immutable `BoundAgent`
snapshot. The snapshot pins the full Agent definition (including role prompt
and verdict contract), the resolved Runner, and the effective static context
policy. Its `binding_digest` is persisted with the Run launch snapshot and
copied onto each step trace; replay keys include the digest, so identical
step input under a different binding is not treated as the same execution.

The legacy `profile.worker_binding` tier is projected only at the Claude Code
compatibility boundary. New Blueprints should use `runner` or `runner_ref`.
The Runner's `tools` remain requested/declarative for `ws_claude_code` until
an injected `AgentBindingProvider` attests the execution environment's
effective grant. The generic path is for the Operator/MainAI to implement
that interface; platform-specific official plugins may implement the same
interface when a host needs a stabilizing adapter. The standard Server maps
`AgentDef.spec.operator_ref` to the role claimed by `mse_operator_join` and
resolves the submitted `capability_manifest`; it never reads wrapper files
from the Server filesystem. Core validates one receipt
per requested agent, requires every requested tool and the exact launch
variant, then pins the accepted model, tools, provider revision, and optional
evidence digest as `BindingAttestation`. That attestation is included in the
final `binding_digest` and persisted in the Run snapshot. Resume and replay
reuse it without asking the provider to resolve mutable environment state
again. MSE does not misreport declaration data as an enforced capability.

## Versioning

`metadata.version_label` is an optional free-form SemVer string (e.g.
`"1.2.3"`) used as the match target when reading a stored Blueprint by
version. Store readers select a version via one of three selectors:

- `Latest` — the store's current head (the default when unspecified).
- `Fixed { value }` — one exact, previously-committed version.
- `SemverReq { req }` — resolve to the newest stored version whose
  `version_label` satisfies a `semver::VersionReq` (e.g. `"^1.2"`).

`version_label` is rewritten automatically by the Enhance loop on
PATCH/MINOR/MAJOR bumps; you do not need to hand-maintain it once a
Blueprint is under Enhance management.

## Where to go next

- Three worked examples: `mse://blueprints/samples/01-pure-ctx-eval` (zero
  agent dispatch, pure ctx math), `mse://blueprints/samples/02-verdict-loop`
  (retry loop with a self-managed counter), `mse://blueprints/samples/03-fn-override`
  (a blocked verdict overridden by an approver step).
- The exact, always-current JSON Schema: `mse://api/blueprint-schema` (note:
  `flow` itself is opaque in the schema — its grammar is owned by the
  `mlua-flow-ir` crate, referenced above).
- Tool-level operations (running, archiving, schema fetch): `mse://guides/mcp-tool-reference`.
- Verifying an `AgentDef`'s materialized tools/ctx/output before a run
  (`bp_explain_agent`): `mse://guides/agent-md-authoring` §
  Verifying how your agent materializes.
- The DSL surface for authoring Blueprints directly in Lua (Expr method chains, Node builders, bp_dsl pipeline sugar): mse://guides/dsl-authoring, with two DSL samples: mse://blueprints/samples/06-dsl-verdict-loop and mse://blueprints/samples/07-dsl-pipeline.
