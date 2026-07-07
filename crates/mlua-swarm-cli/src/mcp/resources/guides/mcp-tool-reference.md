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
`GET /v1/runs/:id` (step trace); `POST /v1/tasks/:id/runs` re-kicks an
existing Task. Full inventory (sid / worker_handle / req_id /
capability_token included): `mse://guides/id-lifecycle`.

## Blueprint run / schema

| tool | purpose | side effect |
|---|---|---|
| `swarm_run` | Run a Blueprint to completion. Blocking; returns `run_id` (`R-<hex>`) + `task_id` (`T-<hex>`) + `final_ctx` + `bound_version`. `blueprint` accepts a `BlueprintSelector` — `{kind: "inline", blueprint: {...}}`, `{kind: "id", id: "..."}` (proxies to `POST /v1/tasks` on the server-side store), or `{kind: "file", path: "..."}` (CWD-relative; `..` and absolute paths rejected). For backward compat a bare Blueprint object is treated as inline. Other params: `init_ctx?`, `timeout_secs?` (default 300), `operator_id?` (default `"mcp-run"`), `operator_kind?` (`main_ai`/`automate`/`composite`), `operator_kind_overrides?` (per-agent kind map). | Mutating — registers an in-process run record; `kind: "inline"` and `kind: "file"` use the local `TaskApplication`, `kind: "id"` requires `mse serve` reachable at `bind`. |
| `swarm_status` | Peek at a known run by `run_id`; returns a status snapshot. In-process runs (`kind: "inline"` / `"file"`) also include `task_id` and the per-step trace (`step_entries`, each entry = `{step_id, step_ref, status, at}`). HTTP-proxied runs (`kind: "id"`) live on the server — drill down with `GET /v1/runs/:id` there. | Read-only. |
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
| `mse_operator_join` | Join as an Operator session: `POST /v1/operators` (mint `sid`+token) then connect `WS /v1/operators/:sid/ws` with the Bearer token (kept process-local, never returned). Returns `{sid, roles}`. Params: `roles?`. | Mutating — opens a WS session; requires `mse serve` reachable. |
| `mse_pending_wait` | Pop one pending server frame (`ask`/`hook_before`/`hook_after`/`spawn`) for `sid`, long-polling up to `timeout_ms` (default 30000). Returns `{timed_out}` or the frame. Params: `sid`, `timeout_ms?`. | Read-only (blocks up to the timeout); requires an active `sid` from `mse_operator_join`. |
| `mse_ack` | Ack a pending frame popped via `mse_pending_wait`. Params: `sid`, `req_id`, `kind` (`answer` / `hook_ack` / `spawn_ack` / `spawn_halt`), `value?`, `ok` (default `true`), `error?`. `spawn_halt` (issue #7) is a controlled halt for the current spawn — pass optional `value` (partial ctx merged into the halt marker) and optional `error` (halt reason, reused as the human-readable log line). The step lands as `WorkerResult { ok: true, value: {halted: true, reason, value} }` — a normal termination, not a worker error (distinct from `spawn_ack ok=false`, which stays the fail-loud path). Scope: `spawn_halt` halts the current spawn only; use `swarm_cancel` for swarm-wide cancellation. | Mutating — sends a `ClientMsg` over the session's WS connection. |
| `mse_operator_leave` | Leave an Operator session: `DELETE /v1/operators/:sid`, abort the WS reader task, drop the local `sid` entry. Params: `sid`. | Mutating — closes the WS session. |

## Worker HTTP client

Pure-MCP replacements for the two `curl` steps a spawned worker performs,
so worker-side wrapper agents don't need shell access at all.

Route auto-resolution: when a Spawn frame is popped via `mse_pending_wait`,
this process records `worker_handle → {base_url, task_id}`. Both tools
resolve those fields from the handle alone, so the MainAI only has to
relay `worker_handle` to the SubAgent; explicit params override the
recorded route (and are required when the Bearer is a full
`capability_token`, which has no recorded route).

| tool | purpose | side effect |
|---|---|---|
| `mse_worker_fetch` | `GET <base_url>/v1/worker/prompt?task_id=<task_id>` with `Authorization: Bearer <worker_handle>` (the `wh-` short handle from the Spawn frame, or the full `capability_token`). Returns the server's `WorkerPayload` JSON verbatim (`{task_id, attempt, agent, prompt, system?}`). Params: `worker_handle`, `base_url?`, `task_id?` (`ST-<hex>`; validated before any network I/O; both auto-resolve from the recorded route). | Read-only; requires the `mse serve` at `base_url` reachable. |
| `mse_worker_submit` | `POST <base_url>/v1/worker/submit` with the same Bearer and the raw `body` as `text/plain` (`task_id` resolves server-side from the Bearer). `ok=false` marks the attempt failed (`?ok=false`, the flow.ir Try catch path). Expects HTTP 204; returns `{submitted: true}`. Params: `worker_handle`, `body`, `base_url?` (auto-resolves from the recorded route), `ok?`. | Mutating — lands the attempt result (`submit_output` + `post_result`). |

## Server control (launchd wrapper)

`mse serve` lifecycle is owned by macOS launchd (`Label = com.mse.server`);
these tools are thin wrappers, not a second process-management layer.

| tool | purpose | side effect |
|---|---|---|
| `mse_doctor` | Combined snapshot: `mse mcp`'s own in-process state (in-memory Blueprint store, in-flight run count) + the server-side config/BP list fetched from `GET /v1/doctor`. Params: `bind?`. | Read-only; degrades gracefully if the server is down. |
| `mlua_swarm_server_start` | `launchctl kickstart gui/<uid>/com.mse.server`, then healthz-polls up to 30s. No-op if already running. Errors with install instructions if the launchd job isn't bootstrapped. Params: `bind?`. | Mutating — starts a system service. |
| `mlua_swarm_server_status` | healthz + a `launchctl print` summary (state/pid/last exit code). Params: `bind?`. | Read-only. |
| `mlua_swarm_server_shutdown` | `launchctl bootout gui/<uid>/com.mse.server` (unloads the job; won't restart until the next start/restart). Params: `bind?`. | Mutating — stops a system service. |
| `mlua_swarm_server_restart` | `launchctl kickstart -k gui/<uid>/com.mse.server`, then healthz-polls up to 30s. Use after editing `~/.mse/config.toml`. Params: `bind?`. | Mutating — restarts a system service. |

## Where to go next

- Blueprint shape reference: `mse://guides/blueprint-authoring`.
- Worked samples to feed straight into `swarm_run`: `mse://blueprints/samples/*`.
- Entry points and quickstart: `mse://guides/getting-started`.
