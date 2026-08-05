---
name: bp-build
description: Delegates Blueprint implementation to @bp-coder. Invoke (either by the User typing `/bp-build "<design_para>" --out=<path>` directly, or by the AI invoking it on the User's behalf) only once the design paragraph is mature — flow shape, worker binding, verdict / projection expectations, and output path are all decided.
---

# /bp-build — Blueprint Implementation Kick

Hands a mature `design_para` and a resolved `output_path` to `@bp-coder`
and lets it write the Blueprint in an isolated context, using `bp_doctor`
as the verify gate.

Either the User invokes `/bp-build "<design_para>" --out=<path>` literally,
or the AI invokes it on the User's behalf once the design conversation has
matured (see "Maturity Self-check" below).

Run only when main-thread dialogue (User ↔ AI) has condensed the design
into a single paragraph. Never trigger mid-conversation with a vague
intent, unnamed flow shape, or missing pass conditions.

## Maturity Self-check (AI-invoked path)

Before assembling the kick prompt on the User's behalf, verify all of the
following from the conversation context. If any item is missing, do not
invoke — return one short question to the User to fill the gap, then
resume.

1. **Blueprint name / id** is decided (kebab-case, no collision concern).
2. **Flow shape** is stated (which node kinds compose the top-level flow —
   `step` / `seq` / `branch` / `loop` / `fanout` / `try` / `assign`).
3. **Worker binding** is stated (each agent's kind: `Lua` / `Automate` /
   `MainAi` / `AgentBlock` / subprocess backend — or an explicit "reuse
   existing agent id" declaration).
4. **Verdict / projection expectations** are stated (what shape lands in
   `final_ctx`; which per-step outputs are named parts vs. plain body).
5. **Output path** is decided (absolute path ending `.json` or `.bp.lua`).
6. **Smoke intent** is decided (`smoke: true` = run once after doctor
   clears with a minimal `init_ctx`; default `false`).

If any of 1–6 is missing, ask one short question rather than guessing.

## Kick prompt shape

Dispatched to `@bp-coder` with the following literal template:

```
Blueprint output path: <absolute path, .json or .bp.lua>
Format: <json|lua>       # derived from the path suffix
Smoke: <true|false>      # default false

Design paragraph:
<design_para verbatim>

Optional grounding refs (Read before writing):
- <mse://guides/... URIs the design references, when applicable>
- <mse://blueprints/samples/... URIs the design references, when applicable>
```

## After @bp-coder returns

`@bp-coder` returns a Result / Artifacts / Key observations report. The
main thread reads it and decides:

- `Result: clean` → accept, proceed to Trial-run stage (see
  `mse://guides/bp-lifecycle` Trial-run).
- `Result: max_retries_hit` → discuss the hypothesis with the User, refine
  the design paragraph, re-kick.
- `Result: smoke_failed` → inspect the `run_id`; the doctor gate passed
  but the actual dispatch failed — likely a runtime binding / operator
  contract gap. Read `mse://guides/operator-execution-model` and revise.

`/bp-build` never edits the design paragraph itself — that is the
main-thread's domain. Its scope is exactly the kick and the return-relay.
