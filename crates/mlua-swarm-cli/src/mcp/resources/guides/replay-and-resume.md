# Replay & Resume

How mse recovers an in-flight Run across a supervisor restart, and how
an operator kicks that recovery from the outside.

The wire is built on a single Core primitive — the Ctx-snapshot
replay log — plus a state-driven HTTP endpoint that reuses the
original `run_id`. Everything below is opt-in: with the store
disabled the whole feature is dormant and the server behaves exactly
like the pre-replay build.

## When resume applies

Resume covers exactly one scenario: a Run whose supervisor process
died mid-flight (crash, `launchctl kickstart -k`, host reboot). On
the next `mse serve` boot the recovery sweep marks such Runs as
`Interrupted`, and an attached operator can then re-enter each of
them via `POST /v1/runs/:id/resume`. The Run keeps its original
`run_id` and the replay log short-circuits every step that had
already produced a `Pass`, so the resumed Run reaches the same
final Ctx it would have reached without the restart.

Resume does *not* re-kick a Task from scratch — that is the job of
`POST /v1/tasks/:id/runs` (rekick), which always mints a fresh
`RunId`. Resume and rekick are separate endpoints with separate
semantics; nothing in the wire chooses between them for you.

## The Ctx-snapshot replay log

Every successful dispatch pass through the replay-aware sibling of
the engine dispatcher appends one row to the run's replay log:

- `run_id` — the Run this row belongs to.
- `step_ref` — the agent name being dispatched.
- `input_hash` — SHA-256 over the canonicalized `initial_directive`.
- `occurrence` — per `(step_ref, input_hash)` counter, so a loop
  that revisits the same step with the same input records 0, 1, 2,
  … distinct rows.
- `ctx_snapshot_json` — the whole `Ctx` value the spawner would have
  seen, minus the `operator` field (dropped by `#[serde(skip)]` on
  `Ctx.operator`).
- `step_output_json` — the `Pass` value returned by the step.

`Blocked` and `Err` outcomes are deliberately **not** logged.
Persisting a mid-failure Ctx would poison later replays: a resume
would happily short-circuit through the poisoned row and the
recovered Run would see a Ctx no successful path ever produced.
Only `Pass` rows accumulate.

On resume the endpoint hands the engine a `ReplayCursor` built
from all rows belonging to that Run. The dispatcher checks the
cursor before every dispatch and, on a hit, returns the stored
value immediately without touching the spawner. On a miss the
usual spawn path runs, and if the outcome is `Pass` a new row is
appended.

## Store backend

`ReplayStore` is a trait; two implementations ship in-tree:

- `InMemoryReplayStore` — process-volatile, useful for tests and
  strictly in-process runs (`swarm_run` inline).
- `SqliteReplayStore` — one SQLite file, opened via `rusqlite-isle`
  so all access is confined to a dedicated OS thread. This is what
  makes restart-crossing resume possible.

Selection is driven by `mse serve` flags and the config file:

- `mse serve --replay-store-path <path>` picks a file explicitly.
- `$HOME/.mse/config.toml` `replay_store_path = "..."` does the
  same in config form.
- With neither set the default is
  `$HOME/.mse/store/replay.<db>` — replay is **persistent by
  default**, matching the RunStore / TaskStore persist-by-default
  convention that landed with the SQLite migration work.
- Passing `--ephemeral` disables persistence for all three stores
  and falls back to the in-memory backend.

### Schema versioning

`SqliteReplayStore::open` uses `PRAGMA user_version` as its schema
state machine:

- `0` (fresh file, or any pre-versioned file). If a `replay_log`
  table exists but lacks a `ctx_snapshot_json` column the store is
  from a pre-Ctx-snapshot build; the table is dropped and rebuilt
  in the current shape, then `user_version` is stamped to `1`.
  Dropping legacy rows is safe by construction: they carry no Ctx
  snapshot and would never be usable as a cursor hit anyway.
- `1` — current shape. `CREATE TABLE IF NOT EXISTS` runs as a
  defensive no-op.
- `> 1` — a store written by a newer mse binary. `open` refuses,
  because silently downgrading the schema would corrupt the newer
  format.

Adding a v2 will just be another `1 => migrate_v1_to_v2` arm in
the same function.

## Resume endpoint

```
POST /v1/runs/:run_id/resume
```

- Request body: none. The `run_id` in the path is the only key.
- Response on success: `202 Accepted` with
  `{"run_id": "...", "task_id": "...", "replayed_steps": N}` —
  the flow eval continues in the background (detached).
- `404 Not Found` — no such `run_id` in the RunStore.
- `409 Conflict` — the Run is not `Interrupted` (already
  `Running` / `Done` / `Failed` / `Pending`), or a concurrent
  resume already won the `Interrupted -> Running`
  compare-and-set. The response body carries the current status
  so the caller can decide whether to retry, poll, or move on.
- `422 Unprocessable Entity` — the Run has no recorded launch
  input. Older rows written before RunRecord grew its
  `input_json` column fall in this bucket; they cannot be
  resumed and the endpoint says so explicitly.

The endpoint holds the state machine invariant end to end: the
transition to `Running` is a compare-and-set, so two clients
racing on the same Run do not both kick the dispatcher. The
`run_id` is reused, which keeps `Ctx.meta.runtime.run_id`
consistent with the replay-log rows and with every
`Ctx.meta.runtime.step_ctx` the flow has already written.

## Launch-input snapshot

For a resume to be meaningful the server has to know what the
original launch looked like, so `RunRecord` carries an
`input_json` column that snapshots the `TaskApplicationInput`
verbatim at kick time. Both `POST /v1/tasks` (fresh) and
`POST /v1/tasks/:id/runs` (rekick) persist this snapshot; the
resume endpoint deserializes it back to rebuild the Task
context. Runs without a snapshot get `422`.

## Boot recovery sweep

`mse serve`'s boot flow runs `recover_interrupted_runs` before it
starts accepting HTTP traffic:

1. `RunStore::list_running` walks every Run still in the
   `Running` state — the ones a previous supervisor left mid-flight.
2. Each such Run gets its `result_ref` set to
   `{"error":"server restart"}` and its status flipped to
   `Interrupted`. The owning Task follows.
3. For each newly-interrupted Run the sweep consults the replay
   log:
   - `replayed_steps > 0` — emits at `tracing::info!` level with
     fields `run_id`, `task_id`, `replayed_steps`, and
     `resume_url = "POST /v1/runs/<id>/resume"`. This is the
     "resumable" hint the attached operator watches for.
   - `replayed_steps == 0` — emits at `tracing::debug!` level;
     the Run is marked Interrupted but there is nothing to
     replay against, so a resume would be equivalent to a
     rekick.

The sweep never re-dispatches on its own. An operator that has
not yet attached would have its handle burn out while the Run
waits for it, so the actual resume kick is the operator's
responsibility (see the "Deferred" note below).

## Deferred pieces

Two natural extensions ride on the same wire but are explicitly
out of scope of the initial land:

- **Boot-time auto-respawn.** The sweep only logs; the server
  never kicks resume itself. Adding a "resume every logged
  candidate on boot" option is safe once there is a way to
  guarantee an operator will be there to pick the newly-dispatched
  work up.
- **Subprocess-mode E2E.** The in-tree end-to-end test spins two
  `axum::serve` instances back to back inside one test process,
  which proves the SQLite roundtrip and the endpoint semantics.
  A driver that actually spawns `mse serve` as a subprocess and
  restarts it via `launchctl` would push the coverage all the
  way to the real deployment loop; the fixture is in place for
  that follow-up when it arrives.

## References

- `crate::store::replay` — the trait, `ReplayEntry`,
  `ReplayCursor`, `hash_input_value`, the InMemory / SQLite
  backends.
- `crate::store::run::RunContext::with_replay_store` /
  `with_replay_cursor` — how a `RunContext` opts into the wire.
- `Engine::dispatch_attempt_with_run_ctx` — the dispatch sibling
  that checks the cursor and appends on Pass.
- `POST /v1/runs/:id/resume` — the state-driven endpoint on the
  server side.
- `crates/mlua-swarm-cli/src/serve.rs::recover_interrupted_runs`
  — the boot-time sweep and the resumable-log emission point.
