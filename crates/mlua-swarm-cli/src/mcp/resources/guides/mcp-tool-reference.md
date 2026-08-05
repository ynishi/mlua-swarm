# mse — MCP tool reference

All tools exposed by `mse mcp` (stdio transport), grouped by family. "Side
effect" notes whether the call is read-only, mutates local/in-process state,
or requires `mse serve` to be reachable at `bind` (default `127.0.0.1:7777`).

## ID hierarchy (issue #13)

One kick flows through five identity layers:

| layer | id | prefix | scope |
|---|---|---|---|
| Blueprint | `blueprint_id` | — (user-supplied) | reusable pipeline definition |
| Task | `task_id` | `T-<hex>` | one work item (goal + Blueprint ref + input ctx); persisted, kickable N times |
| Run | `run_id` | `R-<hex>` | one kick of a Task; carries the step trace |
| Step | `step_id` | `ST-<hex>` | one dispatched Blueprint step (stable across retries) |
| Attempt | `attempt` | (counter) | retry counter inside a step |

`run_id` is minted server-side at `POST /v1/tasks` and propagated into every
worker ctx (`ctx.meta.runtime.run_id`), pending-wait payload, and spawn
directive, so wire frames and outputs correlate back to one Run. Server
drill-down: `GET /v1/tasks` → `GET /v1/tasks/:id` (runs included) →
`GET /v1/runs/:id` (step trace). `GET /v1/runs/:id/bindings` explains the
immutable Runner request and Core-validated provider result, including
provider revision, capability snapshot digest, and a mechanical requested/effective
difference. It reads only the Run launch snapshot and returns `422` for older
Runs instead of consulting a changed Blueprint. `POST /v1/tasks/:id/runs` re-kicks an
existing Task with a fresh `RunId`, while `POST /v1/runs/:id/resume`
resumes an `Interrupted` Run under the *same* `RunId` via the
Ctx-snapshot replay log (state-driven: `404` unknown / `409` wrong
status / `422` no launch snapshot / `202` accepted). Full replay wire
narrative including store config, schema versioning, boot recovery
sweep, and the resumable-log hint: `mse://guides/replay-and-resume`.
Full ID inventory (sid / worker_handle / req_id / capability_token
included): `mse://guides/id-lifecycle`. HTTP wire body schemas for the
`POST /v1/tasks` / `GET /v1/tasks/:id` / `POST /v1/tasks/:id/runs` /
`GET /v1/runs/:id/bindings`
request/response shapes above (and `POST /v1/blueprints/:id`, the run-read
family `GET /v1/runs` / `GET /v1/runs/:id` / `GET /v1/runs/:id/steps`, and
the two worker report routes `POST /v1/worker/stats` /
`POST /v1/worker/degradation`): `mse://api/http-endpoints`.

## Blueprint run / schema

| tool | purpose | side effect |
|---|---|---|
| `swarm_run` | Run a Blueprint. Blocking by default: returns `run_id` (`R-<hex>`) + `task_id` (`T-<hex>`) + `final_ctx` + `bound_version` on completion. Pass `detach: true` for the asynchronous launch — returns `{run_id, task_id, status: "running"}` immediately (the eval continues in the background bounded by `timeout_secs` in-process / the server run TTL for `kind: "id"`); poll `swarm_status` for the terminal status and result. `blueprint` accepts a `BlueprintSelector` — `{kind: "inline", blueprint: {...}}`, `{kind: "id", id: "..."}` (proxies to `POST /v1/tasks` on the server-side store), or `{kind: "file", path: "..."}` (CWD-relative; `..` and absolute paths rejected). For backward compat a bare Blueprint object is treated as inline. Other params: `init_ctx?`, `timeout_secs?` (default 300), `operator_id?` (default `"mcp-run"`), `operator_kind?` (`main_ai`/`automate`/`composite`), `operator_kind_overrides?` (per-agent kind map), `detach?` (default false). | Mutating — registers an in-process run record; `kind: "inline"` and `kind: "file"` use the local `TaskApplication`, `kind: "id"` requires `mse serve` reachable at `bind`. |
| `swarm_status` | Peek at a known run by `run_id`; returns a status snapshot. In-process runs (`kind: "inline"` / `"file"`) also include `task_id` and the per-step trace (`step_entries`). Each entry carries `step_id` / `step_ref` / `status` / `at` plus `binding_digest`, and — when a worker boundary reported them — a per-attempt stats block (`attempt`, `started_at_ms`, `completed_at_ms`, `duration_ms`, `worker_kind`, `model`, `usage`, `num_turns`, `adapter_data`), every stats field optional. Authoritative field list: `mse://api/http-endpoints` § `GET /v1/runs/:id`. HTTP-proxied runs (`kind: "id"`) live on the server — drill down with `GET /v1/runs/:id` there. | Read-only. |
| `swarm_run_stats` | Cost/latency report for one run: reads `GET /v1/runs/:id` and folds the step trace into per-step rows (`step_ref` / `status` / `attempt` / `duration_ms` / `worker_kind` / `model` / `usage`), whole-run `totals` (`input_tokens`, `output_tokens`, `total_tokens`, `duration_ms_sum`, `steps_with_stats`, `steps_total`) and a `by_model` breakdown (`steps` + token counts per self-reported model). Needs no local run handle — a run launched by another driver session reports fine from its id alone. Reading it honestly: stats are optional at every worker boundary, so compare `steps_with_stats` against `steps_total` before treating the totals as the run's full cost, and note `duration_ms_sum` adds up per-step durations (concurrent `Fanout` steps each count in full, so it is not wall-clock). Unknown `run_id` → `invalid_params`; unreachable server → `internal_error`. Params: `run_id`, `bind?`. | Read-only; requires `mse serve` reachable at `bind`. |
| `swarm_cancel` | Mark a run cancelled in the local registry. Note: aborting an in-flight run handle is not implemented yet — this only flips the recorded status. | Mutating — local registry only. |
| `bp_schema` | Return the Blueprint JSON Schema (schemars-generated). Use before authoring/registering a Blueprint, or when a parse error points here. `flow` is opaque in the schema (owned by `mlua-flow-ir`). | Read-only, in-process (no `mse serve` needed). Identical body to the `mse://api/blueprint-schema` resource. |

## Blueprint lifecycle (requires `mse serve`)

| tool | purpose | side effect |
|---|---|---|
| `bp_archive` | Archive a Blueprint (logical soft-delete via marker commit; reversible via `bp_unarchive`). Filters the id from `list_ids` default and hard-rejects downstream resolvers. Params: `id`, `bind?`, `confirm` (default `false` = dry-run report). | Mutating when `confirm=true`; requires `mse serve` reachable at `bind`. |
| `bp_unarchive` | Reverse of `bp_archive` — appends an `archive: false` marker commit, re-exposing the id. Params: `id`, `bind?`. | Mutating; requires `mse serve` reachable at `bind`. |

## Operator WS client (multi-session)

| tool | purpose | side effect |
|---|---|---|
| `mse_operator_join` | Join as an Operator session: `POST /v1/operators` (mint `sid`+token and pin `capability_manifest`) then connect `WS /v1/operators/:sid/ws` with the Bearer token (kept process-local, never returned). Returns `{sid, roles}`. Params: `roles?`, `capability_manifest?`. The manifest is optional: Runner-backed launches proceed declaration-only without one (the operator self-checks); a manifest entry matching each launch variant is required only when the Blueprint sets `strict_binding = true` (recommended otherwise for verification). GH #81 Layer 2 (a): on 409 `roles conflict` the response body additionally carries `conflicts_detail: [{role, sid}]` identifying the holding session per conflicted role (the pre-#81 `conflicts: [<role>]` array stays unchanged). | Mutating — opens a WS session; requires `mse serve` reachable. |
| `mse_pending_wait` | Pop one pending server frame (`ask`/`hook_before`/`hook_after`/`spawn`) for `sid`, long-polling up to `timeout_ms` (default 30000). Returns `{timed_out}` or the frame. Params: `sid`, `timeout_ms?`. | Read-only (blocks up to the timeout); requires an active `sid` from `mse_operator_join`. |
| `mse_ack` | Ack a pending frame popped via `mse_pending_wait`. Params: `sid`, `req_id`, `kind` (`answer` / `hook_ack` / `spawn_ack` / `spawn_halt`), `value?`, `ok` (default `true`), `error?`. `spawn_halt` (issue #7) is a controlled halt for the current spawn — pass optional `value` (partial ctx merged into the halt marker) and optional `error` (halt reason, reused as the human-readable log line). The step lands as `WorkerResult { ok: true, value: {halted: true, reason, value} }` — a normal termination, not a worker error (distinct from `spawn_ack ok=false`, which stays the fail-loud path). Scope: `spawn_halt` halts the current spawn only; use `swarm_cancel` for swarm-wide cancellation. | Mutating — sends a `ClientMsg` over the session's WS connection. |
| `mse_operator_leave` | Leave an Operator session: `DELETE /v1/operators/:sid`, abort the WS reader task, drop the local `sid` entry. Params: `sid`. | Mutating — closes the WS session. |
| `mse_operator_leave_by_role` | GH #81 Layer 2 (c): release the session currently holding `role` without knowing the sid or its Bearer token — the recovery route for a stale session whose driver crashed after minting. Proxies `DELETE /v1/operators/by-role/:role` (same trust tier as `mlua_swarm_server_shutdown`, no Bearer). Returns `{removed: true, role}` on 204; server-side 404 (no session holds this role) surfaces as `invalid_params`. Companion HTTP route `GET /v1/operators` enumerates every live session's `{sid, roles, joined_at_secs, connected}` for identifying which role to release; not exposed as an MCP tool — call the HTTP endpoint directly. Params: `role`. | Mutating — closes the WS session server-side; local state is not tracked here. |

## Worker HTTP client

Pure-MCP replacements for the two `curl` steps a spawned worker performs,
so worker-side wrapper agents don't need shell access at all.

Route auto-resolution: when a Spawn frame is popped via `mse_pending_wait`,
this process records `worker_handle → {base_url, task_id}`. Both tools
resolve those fields from the handle alone, so the MainAI only has to
relay `worker_handle` to the SubAgent; explicit params override the
recorded route (and are required when the Bearer is a full
`capability_token`, which has no recorded route).

<!-- convention-token-ok: mse_worker_fetch / mse_worker_submit are mlua-swarm public MCP tool names. -->

| tool | purpose | side effect |
|---|---|---|
| `mse_worker_fetch` | `GET <base_url>/v1/worker/prompt?task_id=<task_id>` with `Authorization: Bearer <worker_handle>` (the `wh-` short handle from the Spawn frame, or the full `capability_token`). Returns the server's `WorkerPayload` JSON verbatim (`{task_id, attempt, agent, prompt, system?}`). Params: `worker_handle`, `base_url?`, `task_id?` (`ST-<hex>`; validated before any network I/O; both auto-resolve from the recorded route). | Read-only; requires the `mse serve` at `base_url` reachable. |
| `mse_worker_submit` | `POST <base_url>/v1/worker/submit` with the same Bearer and the raw `body` as `text/plain` (`task_id` resolves server-side from the Bearer). `ok=false` marks the attempt failed (`?ok=false`, the flow.ir Try catch path). Expects HTTP 204; returns `{submitted: true}`. Params: `worker_handle`, `body`, `base_url?` (auto-resolves from the recorded route), `ok?`, `name?` (GH #36, see below; mutually exclusive with `ok=false`), `degradations?` (GH #32, see below), `stats?` (see below). | Mutating — lands the attempt result (`submit_output` + `post_result`), or (with `name`) stages one output part without landing the attempt; a non-empty `degradations` array is POSTed to `/v1/worker/degradation` per entry, then `stats` (when given) to `/v1/worker/stats`, before either path runs. |

### Named multi-part output (`name`, GH #36)

Pass `name` on `mse_worker_submit` to POST `<base_url>/v1/worker/artifact?name=<name>` instead of `/v1/worker/submit` — the task stays open, and this stages one named part rather than completing the attempt. Call again (same or a different `name`) for more parts; re-staging the same `name` within one attempt replaces the earlier value (last write wins). Finish the attempt with an ordinary plain (no-`name`) `mse_worker_submit` call, unchanged.

A step that staged any parts ends up with output shape `{"out": <the final plain-submit body>, "parts": {<name>: <value>, ...}}`. A downstream step reads a part via bracket-notation path syntax, e.g. `"in": "$.<step>.parts[\"plan.md\"]"` — see `mse://guides/blueprint-authoring` for the full `Step.in` addressing example. `name` and `ok=false` are mutually exclusive (`invalid_params` if both are given): a named part has no pass/fail state of its own, only the attempt (completed via a later `ok=false`-capable plain submit) does.

### Worker degradation reporting (`degradations`, GH #32)

Pass a non-empty `degradations` array (each entry `{tool, error, fallback, note?}`) on `mse_worker_submit` to report a tool failure the worker worked around instead of silently substituting it away. Each entry is POSTed to `/v1/worker/degradation` — server-injected `step_ref`/`attempt`/`at`, an independent channel from the `body`/`name` fold path — before this call's own submit/artifact POST runs. Omitted (`None`) is unchanged pre-#32 behavior. See `mse://guides/operator-execution-model` § Worker degradation reporting for the endpoint's HTTP shape and the persistence / `mse_doctor` surface.

### Worker stats reporting (`stats`)

Pass a `stats` object (`{worker_kind?, model?, usage?: {input_tokens?, output_tokens?, total_tokens?}, num_turns?, adapter_data?}`) on `mse_worker_submit` to report what the attempt cost. Each token field is optional on its own: report `{"total_tokens": N}` alone when that is all the worker knows (the splits read as `0`), or the splits alone and the total is derived as `input + output`. It is POSTed to `/v1/worker/stats` after any `degradations` entries and **before** this call's own submit/artifact POST, which is what makes it land: the dispatcher folds recorded stats at outcome time — the moment the final submit settles the dispatch — so a report arriving afterwards is dropped with the attempt's cleanup.

Report on the **final** (plain, no-`name`) submit. Stats sent on an earlier artifact-staging call are held per `(task_id, attempt)` and overwritten by a later report (last write wins), so a worker that learns its usage incrementally can simply report again. Every field is optional; omitting `stats` entirely POSTs nothing. A non-`204` response is an error rather than a silent drop — the caller opted into reporting.

Reported stats land on the attempt's `StepEntry` and read back via `swarm_run_stats` (aggregate) or `swarm_status` / `GET /v1/runs/:id` (raw entries). Direct `POST /v1/worker/stats` remains available for workers not going through this tool — see `mse://guides/operator-execution-model` § Worker stats reporting for the HTTP shape and the fold timing in full.

## Server control (launchd wrapper)

`mse serve` lifecycle is owned by macOS launchd (`Label = com.mse.server`);
these tools are thin wrappers, not a second process-management layer.

Before killing the process, `mlua_swarm_server_shutdown` and
`mlua_swarm_server_restart` poll `GET /v1/status` (`{running_runs,
attached_operators}`) on the target `bind`. If either count is nonzero the
call is refused with a structured error listing the counts; pass
`force=true` to skip the check and kill unconditionally. When healthz is
down, or the occupancy check itself errors (network hiccup, or a
pre-issue-#35 server binary with no `/v1/status` route), the check is
skipped and the call proceeds — the guard fails open rather than blocking a
legitimate shutdown/restart indefinitely.

| tool | purpose | side effect |
|---|---|---|
| `mse_doctor` | Combined snapshot: `mse mcp`'s own state (`version` = the `mse` build this MCP process is running, in-memory Blueprint store, in-flight run count) + the server-side config/BP list fetched from `GET /v1/doctor` (including its own `server_version`) + a `version_drift` section — `{mse_mcp, mlua_swarm_server, drift}` where `drift` is **tri-state**: `true` (the two running processes are different builds), `false` (same build), `null` (**could not compare** — server down, or its doctor payload predates `server_version`; `null` is not a no-drift verdict). Each process keeps the version it was started with, so a `cargo install` leaves the launchd `mse serve` and the session-lived `mse mcp` on their old vintage until each is restarted — this is the only surface that shows that. Plus an `audit_findings` section (GH #34): for every run this process is tracking with a known `task_id`, fetches `GET /v1/tasks/:id/runs/:run/steps` and flags entries whose `name` starts with `audit:`, reporting `{task_id, run_id, step, artifact_name}` per hit plus a `count`. Zero hits is an empty section, not an error; a steps-fetch failure for one run becomes a `notes` entry and never fails the whole call. Also a `degradations` section (GH #32): for every tracked run, fetches `GET /v1/runs/:id` and sums its `degradations` array length, reporting `{run_id, task_id, count}` per non-empty run plus an overall `count` — same fail-safe contract as `audit_findings` (a fetch/decode failure becomes a `notes` entry, never fails the call). Params: `bind?`. | Read-only; degrades gracefully if the server is down. |
| `mlua_swarm_server_start` | `launchctl kickstart gui/<uid>/com.mse.server`, then healthz-polls up to 30s. No-op if already running. Errors with install instructions if the launchd job isn't bootstrapped. Params: `bind?`. | Mutating — starts a system service. |
| `mlua_swarm_server_status` | healthz + a `launchctl print` summary (state/pid/last exit code). Params: `bind?`. | Read-only. |
| `mlua_swarm_server_shutdown` | `launchctl bootout gui/<uid>/com.mse.server` (unloads the job; won't restart until the next start/restart). Refuses if the server reports in-flight runs or attached operators (see the occupancy-guard paragraph above). Params: `bind?`, `force?` (default `false`; skip the occupancy check and kill unconditionally). | Mutating — stops a system service. |
| `mlua_swarm_server_restart` | `launchctl kickstart -k gui/<uid>/com.mse.server`, then healthz-polls up to 30s. Use after editing `~/.mse/config.toml`. Refuses if the server reports in-flight runs or attached operators (see the occupancy-guard paragraph above). Params: `bind?`, `force?` (default `false`; skip the occupancy check and kill unconditionally). | Mutating — restarts a system service. |

## Where to go next

- Which of these tools belongs to which stage of Blueprint work
  (develop → trial-run → operate) — e.g. `bp_build` / `bp_doctor` are
  develop-stage lint, `swarm_run` + `detach` is the trial-run loop, the
  `mse_operator_*` family is the `MainAi` operate contract:
  `mse://guides/bp-lifecycle`.
- Blueprint shape reference: `mse://guides/blueprint-authoring`.
- Worked samples to feed straight into `swarm_run`: `mse://blueprints/samples/*`.
- Entry points and quickstart: `mse://guides/getting-started`.
- Blueprint JSON Schema: `mse://api/blueprint-schema`.
- HTTP endpoint wire-body JSON Schemas (`/v1/blueprints`, `/v1/tasks`,
  `/v1/tasks/:id/runs`, the `/v1/runs` read family including the per-step
  stats trace, and the `/v1/worker` stats / degradation report routes):
  `mse://api/http-endpoints`.
- The three-hop execution model for the WS thin-path (`AgentKind::Operator`
  → MainAI → SubAgent) and its responsibility boundary:
  `mse://guides/operator-execution-model`.
