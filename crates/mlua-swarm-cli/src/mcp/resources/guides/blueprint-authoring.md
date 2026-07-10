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
  "default_operator_kind": "automate"  // optional, no default (falls through the cascade)
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
per part, then finish with an ordinary plain (no-`name`) submit. A step
that staged at least one part ends up with OUTPUT shape

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
