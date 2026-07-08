# mse — Operator Execution Model

Canonical execution model for the `AgentKind::Operator` path — the WS
thin-path where a MainAI (WS Client) sits between the engine and the
final worker (SubAgent). Explains the three-hop flow, the responsibility
boundary at each hop, and how the Task-level canonical fields
(`project_root` / `work_dir` / `task_metadata`) reach the SubAgent.

**Scope**: only `AgentKind::Operator` (`MainAi` / `Automate` / `Composite`).
The other AgentKinds — `Lua`, `RustFn`, `AgentBlock`, `Subprocess` —
run inside the engine process. Their worker code reads the ctx it needs
directly (via `WorkerInvocation` / `Engine::fetch_prompt` etc.) and the
material below does not apply.

---

## The three-hop flow

```
Task IF                 mse-server                 MainAI                 SubAgent
 (POST /v1/tasks         ctx.meta.runtime           (WS Client,             (self-fetches
  + BP defaults           injection +               reads Spawn frame,       system + prompt,
  + Run override)         Spawn.directive           builds SubAgent          runs the task,
                          text render)              launch prompt)           POSTs result)
        │                       │                       │                       │
        ▼                       ▼                       ▼                       ▼
  Task IF fields ──► ctx.meta.runtime ──► Spawn.directive ──► SubAgent prompt ──► /v1/worker/{prompt,submit}
  (canonical seed)     (Value bag)         (rendered text)     (MainAI-owned)      (SubAgent-owned)
```

Each hop has a fixed owner. The seed at hop 1 (Task IF) and the pull at
hop 4 (SubAgent HTTP) are the two ends the design fixes; the two hops in
between are where the engine and the MainAI cooperate.

---

## Hop 1 — Task IF → ctx.meta.runtime (engine-owned)

The caller of `POST /v1/tasks` seeds the Task-level execution context.
Three canonical top-level sibling fields carry the context:

- `project_root: Option<String>` — the project's root path.
- `work_dir: Option<String>` — the task's working directory.
- `task_metadata: Option<Value>` — an opaque JSON bag for anything else
  the caller wants attached to the Task.

Blueprint-level defaults (`Blueprint.default_init_ctx`) and Run-level
overrides (`POST /v1/tasks/:id/runs` body's `init_ctx_override` /
`task_input_override`) merge into these three fields — the full precedence
is Run > Task > BP default, shallow-object-merge with non-object-wins-fully.

The engine writes the resolved values into `Ctx.meta.runtime` under the
canonical keys, via `TaskInputMiddleware` (see
`src/middleware/task_input.rs`). Reading from `Ctx.meta.runtime` at
dispatch time is the single source of truth for every downstream layer.

Related schemas: `mse://api/http-endpoints` for the wire bodies,
`mse://api/blueprint-schema` for `Blueprint.default_init_ctx`.

## Hop 2 — ctx.meta.runtime → Spawn.directive text (server-side render)

When the engine dispatches an Operator agent, the WS operator session
renders a `Spawn.directive` string for the MainAI. Its job is to
translate the runtime context into a header the MainAI (an LLM) can read
straight through, alongside the routing fields the SubAgent needs to
self-fetch (`worker_handle`, `base_url`, `task_id`).

Splice source is `Ctx.meta.runtime`; the renderer lives in
`crates/mlua-swarm-server/src/operator_ws/session.rs`
(`default_spawn_directive_with_task_directive`). Every key it splices
comes out as a `key: value` header line at the top of the directive.

Currently spliced (as of this doc's write date):

- `project_root: <value>` — from `Ctx.meta.runtime["project_root"]`.
- `work_dir: <value>` — from `Ctx.meta.runtime["work_dir"]`.
- `run_id: <value>` — from `Ctx.meta.runtime["run_id"]`.
- `project_name_alias: <value>` — from `Blueprint.metadata.project_name_alias`.

**Gap (open follow-up)**: `task_metadata`'s inner keys are not spliced
yet. A MainAI reading a directive today cannot see them, and neither can
the SubAgent (see hop 3). The splice is a straightforward extension
of `default_spawn_directive_with_task_directive`; until then MainAIs and
SubAgents recover the missing fields the same way they did before
`task_metadata` was formalized — from convention (`work_dir` via the
workspace layout rules) or from `issue.md`'s body.

## Hop 3 — Spawn.directive → SubAgent launch prompt (MainAI-owned)

The MainAI receives the `Spawn` frame via `mse_pending_wait`. Its job is
to launch a SubAgent (typically `mse-worker`) with a prompt that lets
the SubAgent do its `/v1/worker/prompt` fetch — and, when relevant, to
relay the header lines it just read.

The minimum contract the SubAgent's fetch depends on is a four-line body
consisting of `agent_id`, `worker_handle`, `base_url`, and `task_id` (in
that literal shape). This is documented in the orch driver guides
(`sets/coding/skills/orch-mse/SKILL.md`, sec. Step 4) and in the
`mse-worker` agent definition.

Beyond that four-line minimum, the MainAI is expected to forward whatever
header lines the SubAgent needs to do its work end-to-end. That is a
responsibility boundary, not a fixed list — the MainAI is the layer that
decides. Two conventions worth noting:

- Task-level path fields (`project_root`, `work_dir`) are typically
  relayed verbatim so the SubAgent starts from the right working
  directory without having to derive it.
- Task-level metadata that a specific SubAgent needs (once splice
  completes) is relayed in a form the SubAgent's agent definition
  expects (typically `key: value` lines matching the directive header).

## Hop 4 — SubAgent self-fetch + submit (SubAgent-owned)

The SubAgent (`mse-worker`) does not read the directive text itself. Its
own contract is documented in `mse-worker.md`:

1. `GET <base_url>/v1/worker/prompt?task_id=<task_id>` with
   `Authorization: Bearer <worker_handle>` — returns
   `{system, prompt, agent, ...}` where `system` is the agent persona
   and `prompt` is `TaskSpec.initial_directive` rendered to a string.
2. Adopt `system` as its role, take `prompt` as the task input, run.
3. `POST <base_url>/v1/worker/submit` with the raw output body.
4. Reply `OUTPUT` on stdin and stop.

Anything the SubAgent needs beyond `system` and `prompt` must come from
the MainAI's launch prompt (hop 3) or from files inside `work_dir`
(the classic `issue.md` pattern). The SubAgent never talks to the WS
session and never sees the `Spawn.directive` text.

---

## Responsibility summary

| Hop | Owner       | Reads from                     | Writes to                      |
|----:|-------------|--------------------------------|--------------------------------|
|   1 | mse-server  | `POST /v1/tasks` body + BP + Run override | `Ctx.meta.runtime` (Value)     |
|   2 | mse-server  | `Ctx.meta.runtime` (session.rs) | `Spawn.directive` (String)     |
|   3 | MainAI      | `Spawn.directive` (WS frame)    | SubAgent launch prompt         |
|   4 | SubAgent    | `/v1/worker/prompt` HTTP payload | `/v1/worker/submit` HTTP body  |

## Related

- `mse://api/http-endpoints` — HTTP wire body schemas for the Task IF surface.
- `mse://api/blueprint-schema` — Blueprint schema, including `default_init_ctx`.
- `mse://guides/id-lifecycle` — the five ID layers (Blueprint, Task, Run, Step, Attempt).
- `mse://guides/mcp-tool-reference` — `mse_operator_join` / `mse_pending_wait` / `mse_ack` details.
