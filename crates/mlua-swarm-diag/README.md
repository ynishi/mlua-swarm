# mlua-swarm-diag

Unified Clippy-style diagnostic model for the
[mlua-swarm](https://github.com/ynishi/mlua-swarm) authoring-time
feedback surface (GH #79).

Every diagnostic-producing stage — compile-lint (`mse bp build`),
post-register lint (`bp_doctor`), launch pre-flight, runtime, ref
resolution — emits the same [`Diagnostic`] shape, mirroring Clippy's
`Level` / `Diagnostic` / `Suggestion` / `Applicability` / `Lint`
design:

- [`Diagnostic`] — one finding: stable `kind` key, producing
  [`DiagStage`], [`DiagLevel`], message / notes / help, optional
  [`Suggestion`] (with [`Applicability`] confidence), optional
  [`DocsRef`] (`mse://` guide pointer), optional [`DiagSpan`]
  (which Blueprint element, plus a JSONPath).
- [`LintDecl`] / [`LINT_DECLS`] — the static lint registry: one entry
  per lint kind, the single source that enumerates every lint for
  docs generation / opt-out surfaces.

This crate is the shared vocabulary boundary: it depends on no other
mlua-swarm crate, so producers (`mlua-swarm`) and consumers
(`mlua-swarm-cli`, renderers, `mse bp fix`-style tooling) can meet
here without coupling to each other's internals.

The "add a new lint" recipe is documented in the bundled MCP resource
`mse://guides/lint-diagnostic-model` (served by `mse mcp`).

## License

Licensed under either of Apache License 2.0 or MIT License at your
option.
