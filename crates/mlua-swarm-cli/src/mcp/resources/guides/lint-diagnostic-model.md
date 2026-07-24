# mse — Lint & Diagnostic model (GH #79)

How MSE's authoring-time feedback surface is unified: every
diagnostic-producing stage — compile-lint (`mse bp build` / `bp_build` /
`mse bp lint`), post-register lint (`bp_doctor`), launch pre-flight,
runtime, ref resolution — projects its findings into one Clippy-style
`Diagnostic` shape, declared once in the `mlua-swarm-diag` vocabulary
crate. This guide documents the model and the 4-step recipe for adding
a new lint.

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
| `bp_doctor` MCP tool | `diagnostics` (array) | One entry per finding across all six lint families, alongside the family-specific fields (which remain until a future major bump removes them). |
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
- User-level `#[allow]`-style lint control — the 5 categories are the
  baseline; per-lint opt-out surfaces would read `LINT_DECLS`.
- Runtime error-path unification — `DiagStage::Runtime` is declared,
  unconsumed.
- Colorized terminal rendering — the model is data-only.

## Where to go next

- The lifecycle stage this belongs to (develop): `mse://guides/bp-lifecycle`
- `bp_doctor` / `bp_build` tool reference: `mse://guides/mcp-tool-reference`
- Skip-tier lint family semantics: `mse://guides/skip-tier-and-skip-on`
- API docs: `mlua-swarm-diag` on docs.rs
