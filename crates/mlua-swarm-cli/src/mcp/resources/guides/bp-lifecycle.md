# mse — Blueprint lifecycle (develop → trial-run → operate)

A workflow-oriented map of the Blueprint authoring loop. Every feature
named below already exists and has its own reference doc — this guide's
job is routing: given the stage you are in, it names the surface built
for that stage, so you never re-invent a manual driver loop for a
problem the engine already solves.

| stage | goal | key surfaces |
|---|---|---|
| 1. Develop | get a Blueprint that compiles clean | `mse bp new` / `bp_new`, `mse bp lint`, `bp_build` with `register=false`, `bp_doctor` |
| 2. Trial-run | run it end-to-end cheaply, iterate | `OperatorKind::Automate` (the default), `swarm_run` (+ `detach`), `swarm_status`, rekick / `resume` / `rerun-from` |
| 3. Operate | run it for real, under the operator model you chose | `MainAi` contract (`mse_pending_wait` → dispatch → `mse_ack`), launchd lifecycle, boot recovery sweep |

---

## Stage 1 — Develop

### Author

Scaffold instead of hand-writing: `mse bp new` (CLI) / `bp_new` (MCP)
emits a compile-lint-legal `.bp.lua` from a bundled template with every
currently-mandatory field pre-filled — see
`mse://guides/bp-dsl-templates` for the template inventory and
`mse://guides/dsl-authoring` for the `flow_dsl` / `bp_dsl` surface
itself.

### Static lint, before anything is registered

Two surfaces run the real Compiler's lint without touching a server:

- `mse bp lint [--strict]` — lint-only verdict (OK / WARN / ERROR), no
  JSON emitted. `--strict` exits non-zero on any WARN/ERROR, so it
  slots into CI as-is (precedent: `cargo check`, `tsc --noEmit`).
- `mse bp build` without `--register` (CLI) / `bp_build` with
  `register=false` (MCP) — build + lint dry run that also returns the
  built Blueprint JSON for inspection. On lint failures whose Compiler
  message matches a known kind (worker-binding-missing /
  verdict-value-not-in-contract / halted-at-missing), the MCP response
  carries a structured `fix_hint` (`{kind, reason, patch_suggestion,
  docs_ref}`) — a Clippy-style recovery hint an authoring loop can
  apply and re-call (GH #62).

### Deeper lint, once registered

`bp_doctor` inspects a registered Blueprint head and reports per-agent
and Blueprint-scoped findings across six families: agent.md size
(bytes / lines vs. the `mse://guides/agent-md-authoring` targets),
`tool_lint` (phantom MCP tool refs), `output_contract_lint` (missing /
malformed `expected_output`), `worker_binding_lint` (operator-kind
agents without a Runner), `binding_lint` (operator-binding advisories,
incl. `binding_requirements_info`), and `skip_on_lint`
(`mse://guides/skip-tier-and-skip-on`). The verdict is a report label
only — `bp_doctor` never blocks a dispatch, so running it early and
often is free.

---

## Stage 2 — Trial-run

### Let the engine drive: `Automate` is the default

`OperatorKind` has three values — `MainAi`, `Automate`, `Composite` —
and **`Automate` is the hardcoded default** (`OperatorKind::default()`).
The kind resolves through a cascade (highest priority first): runtime
agent-level override → runtime global (`operator_kind` on the launch
request) → BP agent-level (`OperatorDef.kind`) → BP global
(`Blueprint.default_operator_kind`) → the `Automate` fallback.

For a trial run you almost never want `MainAi`: that kind exists for
interactive management (Stage 3), and picking it means *you* now own
the dispatch loop. Leave the kind unset — or set `operator_kind:
"automate"` explicitly — and the engine runs the flow to completion
with no frame-by-frame intervention.

### Run to completion: `swarm_run`

`swarm_run` is blocking by default: it returns `run_id` + `final_ctx` +
`bound_version` when the flow finishes. It accepts a
`BlueprintSelector` (`inline` / `id` / `file`), so the edit-run loop on
a `.bp.lua` under development is `bp_build` → `swarm_run {kind: "id"}`
(or `{kind: "file"}` with no server at all).

For long runs, pass `detach: true` — the tool returns
`{run_id, task_id, status: "running"}` immediately, and `swarm_status`
polls the result (it folds in `GET /v1/runs/:id` server-side state for
detach runs, GH #67).

### Iterate: rekick, resume, rerun-from

Three run-again surfaces with distinct semantics — pick by what
happened:

- **Rekick** — `POST /v1/tasks/:id/runs`. Fresh `RunId`, runs the Task
  from scratch. The plain "run it again" loop.
- **Resume** — `POST /v1/runs/:id/resume`. Recovery for a Run whose
  supervisor died mid-flight (status `Interrupted`). Same `RunId`; the
  Ctx-snapshot replay log short-circuits every step that already
  produced a `Pass`, so only the unfinished tail re-executes.
- **Rerun-from** — `POST /v1/runs/:id/rerun-from` (GH #71). Re-executes
  a chosen step *and everything downstream* of a terminal Run (`Done` /
  `Failed` / `Interrupted`), same `RunId`, truncating the replay log at
  the cut point. This is the debug loop for "step 4 of 6 is wrong; fix
  the agent and re-run from there without paying for steps 1–3 again".
  Caveat for iterating on the Blueprint itself: an inline-launched Run
  freezes the BP in its launch snapshot, so register the BP and launch
  by id (`BlueprintRef::Id`) when you want agent edits picked up by the
  rerun.

The replay store backing resume / rerun-from is **persistent by
default** (`~/.mse/store/replay.<db>`; `--ephemeral` opts out). Full
wire narrative: `mse://guides/replay-and-resume`.

---

## Stage 3 — Operate

### The Operator contract: `MainAi` means *you* drive

Choosing `MainAi` is signing up for interactive management — that is
the feature, not a limitation. The attached operator (a main AI over
the WS client, or any MCP client) owns the loop:

```
mse_operator_join → mse_pending_wait → dispatch the frame's work → mse_ack → (repeat) → mse_operator_leave
```

Nothing runs hands-off under `MainAi` **by design**: every Spawn frame
waits for the operator to pop and ack it. If you find yourself
hand-driving this loop for a run that needs no human/AI judgment, the
fix is not a driver script — it is switching the kind to `Automate`
(or leaving it unset, since `Automate` is the default). `Composite`
runs both side by side. The full three-hop responsibility model is
`mse://guides/operator-execution-model`.

### Production posture

- **Server lifecycle** — `mse serve` under launchd, managed via the
  `mse server <subcmd>` family / `mlua_swarm_server_*` MCP tools, with
  occupancy-guarded shutdown/restart: `mse://guides/server-management`.
- **Crash recovery** — on boot, the recovery sweep marks mid-flight
  Runs `Interrupted` and logs the ones with replay entries as
  resumable candidates (`resume_url` hint at `info!` level); kicking
  the resume is the attached operator's call:
  `mse://guides/replay-and-resume`.
- **Health & drift** — `mse_doctor` for a combined in-process + server
  snapshot (incl. after-run audit findings and worker degradation
  counts); `bp_doctor` stays useful post-register whenever a Blueprint
  head changes.
- **Recovery from blocked resources (GH #81)** — error bodies name the
  fix, not just the blocking condition:
  - **Archived Blueprint on launch/rekick.** Launching or rekicking a
    task against an archived Blueprint now returns `409 CONFLICT` with
    the same wording the register path already emits: `bp resolve:
    blueprint {id} is archived; POST /v1/blueprints/{id}/unarchive
    first`. Pre-#81 this fell through to a generic `400` with no
    recovery hint. `bp_unarchive` is the corresponding MCP tool.
  - **Stale operator session** (a driver crashed after
    `mse_operator_join`). `GET /v1/operators` (MCP:
    `mse_operator_list`) enumerates every live session's
    `{sid, joined_at_secs, connected}` plus its 記名 — Bearer
    required, any live session's token — so you can tell which one it
    is by what it wrote at join. It blocks nothing in the meantime: a
    session claims no name, so your own join, your pins and your
    acquires are all unaffected by it. Full contract:
    `mse://guides/operator-execution-model` § Recovery.

---

## Where to go next

- Authoring surface: `mse://guides/dsl-authoring`,
  `mse://guides/bp-dsl-templates`, `mse://guides/blueprint-authoring`
- Lint / diagnosis: `bp_build` / `bp_doctor` entries in
  `mse://guides/mcp-tool-reference`; the unified diagnostic model and
  the add-a-lint recipe: `mse://guides/lint-diagnostic-model`
- Run / iterate: `swarm_run` / `swarm_status` entries in
  `mse://guides/mcp-tool-reference`; `mse://guides/replay-and-resume`
- Operator model: `mse://guides/operator-execution-model`
- Server operations: `mse://guides/server-management`
