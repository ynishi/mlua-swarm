# mse — MCP tool reference

All tools exposed by `mse mcp` (stdio transport), grouped by family. "Side
effect" notes whether the call is read-only, mutates local/in-process state,
or requires `mse serve` to be reachable at `bind` (default `127.0.0.1:7777`).

## Blueprint run / schema

| tool | purpose | side effect |
|---|---|---|
| `swarm_run` | Run a Blueprint to completion via `TaskApplication.handle`. Blocking; returns `run_id` + `final_ctx` + `bound_version`. Params: `blueprint` (JSON object), `init_ctx?`, `timeout_secs?` (default 300), `operator_id?` (default `"mcp-run"`), `operator_kind?` (`main_ai`/`automate`/`composite`), `operator_kind_overrides?` (per-agent kind map). | Mutating — registers an in-process run record; may spawn workers/operators depending on the Blueprint's agents. |
| `swarm_status` | Peek at a known run by `run_id`; returns a status snapshot. | Read-only. |
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
| `mse_ack` | Ack a pending frame popped via `mse_pending_wait`. Params: `sid`, `req_id`, `kind` (`answer`/`hook_ack`/`spawn_ack`), `value?`, `ok` (default `true`), `error?`. | Mutating — sends a `ClientMsg` over the session's WS connection. |
| `mse_operator_leave` | Leave an Operator session: `DELETE /v1/operators/:sid`, abort the WS reader task, drop the local `sid` entry. Params: `sid`. | Mutating — closes the WS session. |

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
