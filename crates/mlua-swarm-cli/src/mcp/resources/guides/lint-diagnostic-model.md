# mse — Lint & Diagnostic model (GH #79)

How MSE's authoring-time feedback surface is unified: every
diagnostic-producing stage — compile-lint (`mse bp build` / `bp_build` /
`mse bp lint`), post-register lint (`bp_doctor`), launch pre-flight,
runtime, ref resolution — projects its findings into one Clippy-style
`Diagnostic` shape, declared once in the `mlua-swarm-diag` vocabulary
crate. This guide documents the model, how to turn individual lints
down or up (allow / warn / deny), and the 4-step recipe for adding a new
lint.

## The model

| Clippy          | MSE (`mlua-swarm-diag`)     |
|-----------------|-----------------------------|
| `Level`         | `DiagLevel` (Info / Warn / Error) |
| `Diagnostic`    | `Diagnostic`                |
| `Suggestion`    | `Suggestion` (msg / patch)  |
| `Applicability` | `Applicability` (MachineApplicable / MaybeIncorrect / HasPlaceholders / Unspecified) |
| `Lint` registry | `LintDecl` / `LINT_DECLS`   |

One `Diagnostic` (wire shape):

```json
{
  "kind": "worker-binding-missing",
  "stage": {"type": "BpDoctor", "family": "WorkerBindingLint"},
  "level": "Warn",
  "message": "operator agent 'greeter' lacks worker_binding",
  "notes": ["profile.worker_binding is required for the WS thin-path backend"],
  "help": "the WS thin-path operator backend requires a Runner at dispatch",
  "suggestion": {
    "msg": "add an explicit Runner (or legacy profile.worker_binding)",
    "patch": "runner = { backend = \"ws_operator\", variant = \"claude\", tools = {} }",
    "applicability": "HasPlaceholders"
  },
  "docs_ref": {"uri": "mse://guides/bp-dsl-templates", "anchor": null},
  "span": {
    "element": {"type": "Agent", "name": "greeter"},
    "json_path": "$.agents[?(@.name=='greeter')].profile.worker_binding"
  }
}
```

Reading rules for consumers:

- **Switch on `kind`**, never on message text. Every `kind` is declared
  exactly once in `LINT_DECLS` (the static registry), whatever stages
  it fires at.
- **`stage` names the producer** — internally tagged, so a renderer
  that only knows `type` still routes correctly when new families land.
- **`level` is per-stage**, not per-kind: `worker-binding-missing` is
  `Error` at `{"type": "CompileLint"}` (the compile refuses) and `Warn`
  at `{"type": "BpDoctor", "family": "WorkerBindingLint"}`
  (report-only). One lint, one docs anchor, two enforcement points.
  `verdict-value-unhandled` is the same shape with the stages weighted
  differently: `Error` at `{"type": "CompileLint"}` but **only under**
  `metadata.lints = {"verdict-value-unhandled": "deny"}` (or its legacy
  spelling `metadata.strict_verdict_handling`), and `Warn` at `{"type":
  "BpDoctor", "family": "VerdictContractLint"}` unconditionally — the
  report-only stage is the one that always runs, so an author who never
  denies the kind still sees the finding.
- **`suggestion.applicability` is the auto-apply gate**: only
  `MachineApplicable` patches are candidates for unreviewed
  application; `HasPlaceholders` patches contain tokens the author must
  fill in first.
- **A missing finding is an empty array, not an absent field** — an
  all-clear `bp_doctor` run reports `diagnostics: []`.

## Where diagnostics surface today

| surface | field | notes |
|---|---|---|
| `bp_build` MCP tool (`stage: "lint"` failures) | `diagnostic` (single object or `null`) | Typed projection of the Compiler's `CompileError` — no substring re-parse. The legacy `fix_hint` field derives from the same diagnostic and stays for back-compat. |
| `bp_doctor` MCP tool | `diagnostics` (array) | One entry per finding across every lint family, alongside the family-specific fields (which remain until a future major bump removes them). Findings an `allow` took out move to the sibling `suppressed` array — see "Controlling lint levels". |
| `mse bp build` / `mse bp lint` (CLI stderr) | rendered as the `fix hint (…)` block | Same diagnostic, prose-rendered. |

## The registry: `LINT_DECLS`

`mlua-swarm-diag` declares one `LintDecl` per lint kind:

```rust
LintDecl {
    kind: "worker-binding-missing",
    default_level: DiagLevel::Error,
    category: LintCategory::Contract,   // Correctness / Suspicious / Style / Contract / Migration
    desc: "An operator-kind agent lacks the worker binding its backend requires.",
    docs_ref: DocsRef { uri: "mse://guides/bp-dsl-templates", anchor: None },
}
```

The registry is the single enumeration point: docs generation,
`--help lints`-style listings, and future opt-out surfaces all read it.
Producers must only emit `kind` values that resolve via
`mlua_swarm_diag::lint_decl(kind)` — the producing crates' tests assert
this.

## Controlling lint levels

Any lint `bp_doctor` reports can be turned down or up, Clippy-style, in
three places. The declaration is a `{key: level}` map:

| layer | where | scope |
|---|---|---|
| call-site | `bp_doctor` request field `lints` | this invocation only |
| per-agent | `agents[].lints` in the Blueprint | findings that span that agent (or a step referencing it) |
| blueprint | `metadata.lints` in the Blueprint | every finding |

The per-agent layer is authorable from an `agent.md` frontmatter `lints:`
map too, not just from Blueprint JSON — see
`mse://guides/agent-md-authoring`.

**Precedence: call-site > agent > blueprint > the kind's registry
default.** The first layer that has *any* matching key wins outright —
no merging across layers. This is rustc's attribute-proximity model: the
innermost `#[allow]` decides, and a broad `all` key on a nearer layer
beats an exact-kind key on a farther one.

Keys address one kind, one category, or everything:

| key form | example | matches |
|---|---|---|
| kind literal | `"agent-md-size"` | exactly that `LINT_DECLS` kind |
| category group | `"category:style"` | every kind in that category — `correctness`, `suspicious`, `style`, `contract`, `migration` |
| `all` | `"all"` | every kind |

**Within one layer, specificity orders the keys**: exact kind >
`category:<cat>` > `all`. So a layer saying
`{"all": "allow", "agent-md-size": "deny"}` denies that one kind and
allows the rest.

Values are `allow`, `warn`, `deny`:

- `allow` — the finding does not fold into the verdict and does not
  appear in `diagnostics[]`.
- `warn` — reported at `Warn`.
- `deny` — reported at `Error`, and the `bp_doctor` verdict label
  escalates to `BLOCK`. A WARN-only family can therefore become a BLOCK
  verdict. The verdict stays a report label either way: `bp_doctor`
  blocks nothing, it only names what it found.

An allowed finding is never dropped silently. It moves to the top-level
`suppressed` array, which is **always present** (empty when nothing was
allowed) — omitted ≠ passed, the same discipline the family fields
follow:

```json
{
  "suppressed": [
    {
      "kind": "agent-md-size",
      "span": {"element": {"type": "Agent", "name": "researcher"}, "json_path": null},
      "source": "agent:researcher",
      "message": "agent 'researcher' system_prompt is 24680 bytes / 412 lines — over the authoring-guide size target"
    }
  ]
}
```

`source` names the layer that allowed it: `"call-site"`,
`"agent:<name>"`, or `"blueprint"`.

### The non-suppressible boundary is per stage, not per kind

At the **compile** stage a finding that fires at `Error` is a hard
error, not a lint: `Compiler::compile` refuses the Blueprint and no
`allow` / `warn` changes that. At **`bp_doctor`** (report-only)
everything the stage emits is suppressible and escalatable — including
the dual-stage kinds that are compile hard errors, such as
`worker-binding-missing` and `verdict-value-unhandled`. Suppressing one
at `bp_doctor` says "do not report this here"; it does not make the
Blueprint compile.

**The one compile-stage exception**: a `lints` map can change the
compile behavior of exactly one kind, `verdict-value-unhandled`.
`deny` (via the kind literal, `category:suspicious`, or `all`) rejects
the compile with the same error `metadata.strict_verdict_handling: true`
produces — that flag is now the legacy sugar for
`{"verdict-value-unhandled": "deny"}` — and `allow` silences its
`tracing::warn!`. The two spellings union toward deny: either one saying
deny denies, and the flag wins over an `allow` at any layer. No other
kind's compile behavior can be changed.

Both Blueprint layers are read at compile, per agent and in the same
proximity order as at `bp_doctor` — `agents[].lints`, then
`metadata.lints` (there is no call-site layer at compile). So one agent
can be exempted without touching its siblings:

```json
{
  "agents": [
    {"name": "researcher", "lints": {"verdict-value-unhandled": "allow"}},
    {"name": "reviewer"}
  ],
  "metadata": {"lints": {"verdict-value-unhandled": "deny"}}
}
```

`researcher`'s unhandled declared verdict values stay silent; an
unhandled value on `reviewer` still rejects the compile.

### Two meta-lints about the declaration itself

Both fire at `Warn` from `bp_doctor`, with a `BlueprintRoot` span and a
`declared by: <layer>` note — a typo degrades to a diagnostic, never to
a rejected request or a failed register:

| kind | fires when |
|---|---|
| `unknown-lint-kind` | a key matches no kind, no `category:<cat>` group and is not `all` — or its value is not `allow` / `warn` / `deny` |
| `non-suppressible-lint` | an exact-kind `allow` / `warn` targets a compile hard error `bp_doctor` never emits (e.g. `duplicate-agent-name`) — the setting is ignored at every stage. `category:` / `all` keys never raise it: addressing whole sets is expected to cover such kinds |

### Legacy `disable_*_lint` flags

The seven `bp_doctor` request flags keep their exact current semantics
(the family's field is omitted from the response entirely). They are
call-site `allow` on a fixed kind set:

| flag | equivalent to `allow` on |
|---|---|
| `disable_tool_lint` | `tool-unknown-mcp-ref` |
| `disable_output_contract_lint` | `output-contract-missing` |
| `disable_worker_binding_lint` | `worker-binding-missing` |
| `disable_binding_lint` | `binding-requirements-info`, `strict-binding-without-runners`, `legacy-worker-binding`, `binding-resolution-error` |
| `disable_skip_on_lint` | `skip-on-missing-for-skip-like-verdict-value`, `skip-on-declared-but-no-matching-verdict-value`, `skip-on-pattern-conflicts-with-halt-on` |
| `disable_context_policy_lint` | `context-policy-strips-projection-roots`, `projection-root-seed-missing` |
| `disable_verdict_contract_lint` | `verdict-value-unhandled`, `verdict-contract-never-read` |

New authoring uses `lints`: it is per-kind rather than per-family, it
survives in the artifact, and an allowed finding stays visible in
`suppressed[]` instead of vanishing with its whole family. There is no
`disable_agent_md_lint` — `"agent-md-size": "allow"` covers it, at
whichever layer is right.

### Worked example: one legitimately large agent

One agent's system prompt is over the size target on purpose. Instead of
disabling the size family for every agent, allow the kind on that one
agent:

```json
{
  "agents": [
    {
      "name": "researcher",
      "kind": "operator",
      "lints": {"agent-md-size": "allow"}
    }
  ]
}
```

Every other agent keeps the size lint. The finding still appears in
`suppressed[]` on each run, with `source: "agent:researcher"`, so
the exemption stays auditable — and it is declared in the Blueprint, so
a reader can see *that* it was exempted without re-deriving it from a
caller's flags.

## Adding a new lint: the 4-step recipe

Worked example: the planned `context_policy_lint` family (GH #78).

1. **Declare** — add one `LintDecl` entry to `LINT_DECLS` in
   `crates/mlua-swarm-diag/src/lib.rs` (kind, default level, category,
   one-line desc, docs_ref). If it is a new `bp_doctor` family, also
   add the `BpDoctorFamily` variant. The crate's unit tests
   (kind uniqueness, `mse://` scheme guard) cover the new entry
   automatically.
2. **Document** — make sure the `docs_ref` target actually explains the
   contract being checked (add a section to the relevant
   `mse://guides/*` file if none does). The docs anchor is part of the
   lint's public surface: every stage that fires the kind points at the
   same guide.
3. **Produce** — write the producer at the stage that checks the
   invariant:
   - compile stage → a new `CompileError` variant plus its arm in
     `impl From<&CompileError> for Diagnostic`
     (`src/blueprint/compiler.rs` — the `match` has no wildcard, so the
     compiler forces the new arm);
   - `bp_doctor` family → a `classify_*` function returning the
     family's JSON verdict plus its `diag_from_*` sibling projecting
     `Vec<Diagnostic>` (`crates/mlua-swarm-cli/src/mcp.rs`), wired into
     the `bp_doctor` loop and the `diagnostics` array;
   - launch pre-flight → produce diagnostics with
     `stage: LaunchPreflight` (first producers land with GH #78).
4. **Test** — assert (a) the finding case emits your kind with the
   expected level / span / suggestion, (b) the all-clear case emits
   nothing, and (c) the kind resolves via `lint_decl(kind)` (add it to
   the producing crate's declared-kind list test).

## Boundary discipline (why a separate crate)

`mlua-swarm-diag` depends on no other mlua-swarm crate. Producers
(`mlua-swarm`) and consumers (`mlua-swarm-cli`, external renderers)
meet at this vocabulary crate instead of coupling to each other's
internals — the CLI's old approach was substring-matching the
compiler's pre-formatted error strings (`err_msg.contains(...)`), which
broke silently whenever a message was reworded. The typed seam replaces
that: `compile_lint` keeps the `CompileError` in its error chain, the
CLI downcasts it back out, and `Diagnostic::from(&err)` reads the
variant's typed fields directly. The one string literal both sides
still share (`profile.worker_binding is required…`) is a `pub const`
in `mlua-swarm` used by both the message constructor and the matcher,
so it cannot drift.

## Non-goals (follow-up candidates)

- `mse bp fix` auto-apply — `Applicability` is declared here; the apply
  loop is not.
- More compile-stage kinds — the compile stage reads both Blueprint
  layers (`agents[].lints`, then `metadata.lints`) but still for
  `verdict-value-unhandled` only; every other compile finding is a hard
  error, not a lint (see "Controlling lint levels").
- Runtime error-path unification — `DiagStage::Runtime` is declared,
  unconsumed.
- Colorized terminal rendering — the model is data-only.

## Where to go next

- The lifecycle stage this belongs to (develop): `mse://guides/bp-lifecycle`
- `bp_doctor` / `bp_build` tool reference: `mse://guides/mcp-tool-reference`
- Skip-tier lint family semantics: `mse://guides/skip-tier-and-skip-on`
- API docs: `mlua-swarm-diag` on docs.rs
