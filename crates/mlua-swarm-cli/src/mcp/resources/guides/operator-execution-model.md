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

**Resolved by GH #20 (Contract C — `AgentContextView`)**: the splice
source is now one materialized view, not individual `Ctx.meta.runtime`
reads. `AgentContextMiddleware` (`src/middleware/agent_context.rs`, the
innermost spawner layer) builds an `AgentContextView`
(`src/core/agent_context.rs`) from `Ctx` exactly once per spawn and fans
it out on two rails: (a) `EngineState.agent_ctx[(task_id, attempt)].view`
— the Worker axis source (hop 4 below), and (b)
`ctx.meta.runtime[AGENT_CONTEXT_KEY]` (JSON-serialized) — the Spawner
axis source this hop reads back via `AgentContextView::materialized_or_from_ctx`.

The renderer lives in
`crates/mlua-swarm-server/src/operator_ws/session.rs`
(`default_spawn_directive_with_task_directive`, taking `view:
&AgentContextView` in place of the old individual params). Header lines
come from `AgentContextView::to_directive_header`, which renders one
`key: value` line per present field, in this order:

- `project_name_alias: <value>` — from `Blueprint.metadata.project_name_alias`.
- `project_root: <value>` — from `Ctx.meta.runtime["project_root"]`.
- `work_dir: <value>` — from `Ctx.meta.runtime["work_dir"]`.
- `task_metadata: <compact-json>` — from `Ctx.meta.runtime["task_metadata"]`
  (the F2 gap this section used to track — closed as of GH #20: a
  MainAI reading the directive can now see `task_metadata`'s inner keys
  directly, without falling back to convention or `issue.md`'s body).
- One `<extra key>: <compact-json>` line per `AgentContextView.extra`
  entry — the injectable surface future supply-axis fields (FlowIr ctx /
  StepMeta) land on. A field added there reaches this splice with no
  further wiring.

`run_id: <value>` (from `Ctx.meta.runtime["run_id"]`) is rendered
separately, into the observation route hint (`GET /v1/runs/{run_id}`) —
not part of the task-level context header above.

## Supply tiers (GH #21 Phase 1)

Before hop 2 renders the directive header, `AgentContextMiddleware`
(`src/middleware/agent_context.rs`) resolves *where the `AgentContextView`
values it materializes come from* — the agent-context supply axis. Each
tier is declared at a different place and the tiers stack, highest
priority first:

| Tier | Declared | Mechanism |
|---|---|---|
| Run | `POST /v1/tasks/:id/runs` body (`init_ctx_override` / `task_input_override`) | Explicit per-run override |
| Task | `POST /v1/tasks` body (`project_root` / `work_dir` / `task_metadata`) | `TaskInputMiddleware` inserts into `ctx.meta.runtime` |
| Step | `$step_meta` envelope in `Step.in` (`{"ref": "<MetaDef.name>", "inline": {...}}`) + `Blueprint.metas` pool | `EngineDispatcher::dispatch` strips the envelope before `start_task`, resolves it against the pool, and threads it through `TaskSpec.step_ctx` |
| Agent | `AgentMeta.ctx` / `AgentMeta.meta_ref` / `AgentMeta.context_policy` | `AgentContextMiddleware`, only-if-absent |
| BP-global | `Blueprint.default_agent_ctx` / `default_context_policy` | `AgentContextMiddleware`, only-if-absent |

The precedence needs no priority code: `AgentContextMiddleware` is layered
**innermost** (see `service::task_launch::TaskLaunchService::launch`), so
it always runs *after* every outer tier (Run / Task / Step) has already
inserted its keys into `ctx.meta.runtime`. It merges the Agent and
BP-global tiers itself (agent wins on collision) and inserts the result
only-if-absent — a key an outer tier already set is never overwritten.
Keys matching one of the five named `AgentContextView` fields become
design-time defaults for those fields; any other key lands in
`AgentContextView.extra` (and, for in-process workers that read `ctx`
directly, in `ctx.meta.runtime` too) with no further wiring.

**`default_agent_ctx` vs `default_init_ctx`**: both are BP-global JSON
defaults, but they feed different things. `Blueprint.default_init_ctx`
seeds the flow-ir eval `ctx` exactly once at flow start (a pure eval seed
— see `service::task_launch::merge_init_ctx`). `Blueprint.default_agent_ctx`
is consumed per-spawn by `AgentContextMiddleware` and lands in the
Agent/LLM-boundary runtime bag (`ctx.meta.runtime` / `AgentContextView`) —
it never touches flow-ir eval at all.

### Step projection naming (GH #23): `AgentMeta.projection_name`

A dispatched Step's OUTPUT used to be addressable under two independent
names — the flow.ir data-plane producer name (`Step.ref`) and the
`result_ref` ctx-path key (`Step.out`'s top-level path segment) — with
consumers (the `ContextPolicy.steps` filter, `StepPointer.name`, the REST
`:step` resolver, and the materialized-file stem) resolving the union of
both, data-plane winning on a name collision.
`AgentMeta.projection_name: Option<String>` lets a Blueprint author
collapse that union into one name declared up front, on the Agent tier:

```jsonc
{
  "agents": [
    { "name": "planner", "kind": "operator", "spec": { /* ... */ },
      "meta": { "projection_name": "plan" } }
  ]
}
```

- **Declared** (`meta.projection_name = "plan"`): every consumer converges
  on `"plan"` as the ONE canonical name for that step's OUTPUT — the
  `ContextPolicy.steps` filter, `StepPointer.name`, the REST `:step` path,
  and the materialized `<name>.md` file stem all use it. The step stays
  reachable under its `Step.ref` (`"planner"`) and its `out` ctx-path's
  top-level segment too — both become aliases — so a filter written
  against either of those names keeps matching.
- **Undeclared** (`meta.projection_name` absent, the default): the step's
  canonical name stays its `Step.ref`, and its aliases are `{Step.ref,
  out-top-segment}` — byte-identical to the pre-GH-#23 union behavior. No
  Blueprint change is required for this to keep working.
- **Collision at register time**: a declared name (or alias) that clashes
  with another step's DECLARED name is rejected — `Compiler::compile`
  fails fast with a `StepNamingError` naming both contending steps. A
  clash between two UNDECLARED steps still registers (the pre-GH-#23
  collision case) with a `tracing::warn!`, resolving data-plane-first —
  unchanged from before.

See `crate::core::step_naming`'s module doc for the full addressing-space
design this table backs.

### Projection placement (GH #27, follow-up to #23): `Blueprint.projection_placement`

A Step's materialized OUTPUT file — the file the submit-time projection
sink writes, the REST metadata/content routes' `file_path` reconstructs,
and the spawn-time `ctx_projection` pointer addresses (the "3 path"
convergence a single
`mlua_swarm::core::projection_placement::ProjectionPlacement` resolver now
owns) — lives at a location resolved from two independent choices:

- **Root preference**: which of the spawn-time `work_dir` / `project_root`
  to prefer as the materialize root, falling back to the other when the
  preferred one is absent. `work_dir` is the per-task working directory
  supplied on the task — typically a git worktree cut from the main
  checkout; `project_root` is that main checkout itself. Default (and
  every pre-GH-#27 Blueprint's behavior): prefer `work_dir`, falling back
  to `project_root`.
- **Directory template**: a `{task_id}`-templated path, relative to the
  resolved root, under which the file is written (the file name itself is
  unchanged — the canonical agent / projection name, `.md`-suffixed).
  Default: `"workspace/tasks/{task_id}/ctx"`.

Declare either or both on `Blueprint.projection_placement`:

```jsonc
{
  "projection_placement": {
    "root": "project_root",
    "dir_template": "artifacts/{task_id}/projections"
  }
}
```

- **Undeclared** (`projection_placement` absent, the default): both
  choices resolve to their defaults above — byte-identical to every
  pre-GH-#27 Blueprint's materialize location.
- **Partially declared**: an omitted field (`root` or `dir_template`)
  resolves to its own default independently — declaring only `root`
  leaves `dir_template` at `"workspace/tasks/{task_id}/ctx"`.
- **Invalid `dir_template`**: empty, missing the `{task_id}` placeholder,
  absolute, or containing a `..` path segment is rejected at
  `Compiler::compile` time (fails fast, same class as the Step-naming
  collision above).

See `crate::core::projection_placement`'s module doc for the resolver's
full API and the "3 path" convergence this collapses.

### Per-Step meta: `$step_meta` envelope, and the dedicated-agent pattern

Besides the `$step_meta` envelope (the Step tier row above, detailed
below), per-Step context is also expressible
**through the Step → Agent binding the Blueprint author controls**: a flow
step names its agent via `{"kind": "step", "ref": "<agent name>"}`, so
giving each step its own `AgentDef` entry gives each step its own
`AgentMeta.ctx`. Two agents may share the same `kind` / `spec` / `profile`
and differ only in `name` + `meta.ctx`:

```jsonc
{
  "flow": { "kind": "seq", "nodes": [
    { "kind": "step", "ref": "scout-repo", "in": ..., "out": ... },
    { "kind": "step", "ref": "scout-docs", "in": ..., "out": ... }
  ]},
  "agents": [
    { "name": "scout-repo", "kind": "operator", "spec": { /* same */ },
      "meta": { "ctx": { "work_dir": "/repo/service-a" } } },
    { "name": "scout-docs", "kind": "operator", "spec": { /* same */ },
      "meta": { "ctx": { "work_dir": "/repo/docs" } } }
  ]
}
```

Each spawn resolves `ctx.agent` to its own `AgentMeta.ctx`, so the two
steps see different `work_dir` (and any `extra` keys) with nothing else
wired. The Step tier is now wired **BP-side** (GH #21 Phase 2), so a
per-Step context no longer requires a dedicated `AgentDef` — though the
pattern above stays fully valid as the alternative for whenever you would
rather not touch `Step.in` (and `AgentMeta.meta_ref`, below, now lets a
whole family of those thin per-step agents share one `MetaDef` instead of
repeating the same `meta.ctx` object on each).

**`Blueprint.metas` pool.** A Blueprint declares a named, shared pool of
`MetaDef` entries (`{"name": "<logical name>", "ctx": {...}}`) at
`Blueprint.metas`. Two independent consumers resolve names against this
pool:

- a `$step_meta` envelope embedded in a Step's evaluated `in` value (this
  section), and
- `AgentMeta.meta_ref` (the Agent tier — resolves the same pool as the
  base layer UNDER the agent's own inline `AgentMeta.ctx`, inline wins on
  key collision).

**The `$step_meta` envelope.** Wrap the Step's real input under `$in`
alongside a `$step_meta` key naming (and/or inlining) the context:

```jsonc
{
  "op": "lit",
  "value": {
    "$step_meta": {
      "ref": "heavy-scan",
      "inline": { "work_dir": "/x" }
    },
    "$in": "do the thing"
  }
}
```

`EngineDispatcher::dispatch` (`src/blueprint.rs`) strips `$step_meta`
before calling `Engine::start_task` — it never leaks into
`prompts[(tid,1)]` or the WS directive text. `ref` resolves against the
`Blueprint.metas` pool (an unresolved name is a loud dispatch-time error,
naming the unresolved ref and the defined names — no silent skip);
`inline` shallow-merges on top (**inline wins** key collisions). The
resolved object is threaded through as `TaskSpec.step_ctx` and inserted
into `ctx.meta.runtime` by `AgentContextMiddleware`, only-if-absent,
**before** the Agent and BP-global tiers (full precedence Run > Task >
Step > Agent > BP-global — see the table above).

**The `$in` / remainder rule.** After `$step_meta` is stripped, if the
remaining object still has an `$in` key, that value becomes
`TaskSpec.initial_directive` (any other sibling keys are ignored for the
directive). Otherwise the whole remainder becomes the directive; an empty
remainder (envelope-only input, e.g. `{"$step_meta": {"ref": "..."}}`)
becomes `""`. Inputs with no `$step_meta` key at all (plain strings,
plain objects) flow through unchanged — pre-#21-Phase-2 Blueprints are
byte-identical.

Values that vary **per iteration of a dynamic loop** are still runtime
data, not design-time meta — they belong in the flow ctx and reach the
worker through `Step.in` (the prompt), which is already wired. Composing
a *different* `$step_meta` envelope per iteration depends on what the
flow-ir `Expr` grammar can express at `Step.in`: only a literal
`Expr::Lit` is visible to the compiler's best-effort static `meta_ref`
check (`Compiler::compile`); a computed/`Path`-derived envelope is
invisible statically and validated only at dispatch time. That
composition is out of this section's scope.

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
- Task-level metadata that a specific SubAgent needs is relayed in a
  form the SubAgent's agent definition expects (typically `key: value`
  lines matching the directive header — `task_metadata:` included, as
  of GH #20).

## Hop 4 — SubAgent self-fetch + submit (SubAgent-owned)

The SubAgent (`mse-worker`) does not read the directive text itself. Its
own contract is documented in `mse-worker.md`:

1. `GET <base_url>/v1/worker/prompt?task_id=<task_id>` with
   `Authorization: Bearer <worker_handle>` — returns a `WorkerPayload`
   JSON body: `{system, prompt, agent, ..., context?}` where `system` is
   the agent persona, `prompt` is `TaskSpec.initial_directive` rendered
   to a string, and `context` (GH #20 Contract C, optional — present
   whenever `AgentContextMiddleware` was layered onto the dispatching
   spawner stack) carries the same materialized `AgentContextView` hop 2
   splices into the directive text, as structured JSON instead of
   header lines — the Worker axis's read-back source, keyed by
   `(task_id, attempt)` in `EngineState.agent_ctx`. In practice
   the SubAgent has already been handed whatever it needs as prompt
   text via hop 3, so consuming `context` here is optional; it exists
   as a structured fallback for a SubAgent that wants
   `task_metadata` / `project_root` / `work_dir` as JSON rather than
   re-parsing header lines out of the prompt it was launched with.
2. Adopt `system` as its role, take `prompt` as the task input, run.
3. `POST <base_url>/v1/worker/submit` with the raw output body.
4. Reply `OUTPUT` on stdin and stop.

Anything the SubAgent needs beyond `system` and `prompt` must come from
the MainAI's launch prompt (hop 3) or from files inside `work_dir`
(the classic `issue.md` pattern). The SubAgent never talks to the WS
session and never sees the `Spawn.directive` text.

## Prompt delivery modes (GH #31)

Hop 4's `WorkerPayload` carries the baked `system` prompt in one of two
mutually-exclusive modes, decided server-side (per-config, not
per-request) against a size threshold (default 25 KiB, matching
`bp_doctor`'s existing WARN threshold):

- **`system: Some(...)` (inline)** — the default for prompts under the
  threshold. Unchanged from the pre-GH #31 contract: the fetched
  `WorkerPayload` carries the full rendered string directly, and a
  SubAgent (or MCP tool relaying the fetch) has the text the moment the
  fetch call returns.
- **`system_ref: Some(...)` (by-reference)** — used instead of `system`
  once the rendered prompt exceeds the threshold. The payload carries a
  `SystemRef { uri, sha256, size_bytes, mode }` pointer rather than the
  text itself:
  - `mode: Http` — `uri` is a bare path
    (`/v1/worker/prompt/system?task_id=...&attempt=...`); the resolving
    caller `GET`s it (prefixed with the same `base_url` the main fetch
    used) to retrieve the raw bytes.
  - `mode: File` — `uri` is a `file://<path>` URI; the resolving caller
    reads that path directly.
  - Either way, the caller sha256-verifies the retrieved bytes against
    `system_ref.sha256` before trusting them.

**SubAgent-side flow, inline mode**: fetch → `system` is already the
persona text → adopt it as system prompt → proceed. No extra step.

**SubAgent-side flow, by-reference mode**: fetch → `system` is absent,
`system_ref` is populated → resolve `system_ref` (download/read,
sha256-verify with one retry on mismatch, write the verified bytes to a
local file, read the file back to confirm the write landed) → **only
then** load the file's contents as the system prompt and proceed.
`mse_worker_fetch` (the MCP tool wrapping hop 4 for MCP-based SubAgents)
performs this resolution automatically and returns the original payload
plus a `system_ref_resolution: {ok, path, sha256, size_bytes}` (or
`{ok: false, stage, error}` on failure) companion value.

> **This caveat is load-bearing, not optional colour**: a
> `system_ref_resolution.ok: true` (or any successful by-reference
> resolution, MCP-tool-mediated or not) means only that **the referenced
> file was written to disk intact and its bytes match the advertised
> sha256** — it does **not** mean the SubAgent has loaded that file's
> contents into its own LLM context yet. Verifying the file on disk and
> adopting its contents as the running persona are two separate steps;
> a caller that stops at "the tool returned `ok: true`" without also
> reading the file and using it as the system prompt has not actually
> completed hop 4.

## After-run audits (GH #34)

`Blueprint.audits: Vec<AuditDef>` declares agents the engine auto-kicks
**after** a matching step settles, purely for observation — see
`mse://api/blueprint-schema`'s `AuditDef` for the field shape (`agent` /
`steps` / `mode`), and `mse://blueprints/samples/*after-run-audit*` for
worked samples.

**From the operator's point of view.** When `AuditDef.agent` names an
`AgentDef` whose `kind` is `operator`, the audit's dispatch reuses hops
1-4 above unmodified — the operator receives an ordinary `ServerMsg::Spawn`
frame via `mse_pending_wait`, exactly like any other Operator-kind step.
There is no new frame kind and no special-casing required on the WS thin
path. The only two differences from a normal step spawn:

- **Timing**: the Spawn fires *after* another step's own spawn has
  already settled — it is not part of the flow's own step sequence.
- **Directive content**: instead of asking the operator to do the
  audited step's own work, the rendered `Spawn.directive` instructs it to
  **audit** that step — inspect the step's transcript/output (via
  `agent-inspect`, or by reading the worker's own submitted result through
  the normal read paths), then report findings as structured JSON.

Launch the audit exactly as hop 3 launches any worker (a SubAgent whose
prompt is the rendered directive text), and submit its findings through
the normal worker path (`POST /v1/worker/submit`, hop 4) — no dedicated
audit endpoint exists or is needed.

**Observational only — binding invariant.** An audit's verdict, findings,
or even its own failure or crash NEVER change the audited step's or run's
outcome, and never gate the flow (`Blueprint.audits`'s binding invariant,
enforced by `mlua-swarm` core's `AfterRunAuditMiddleware`). A worker that
warned-and-fell-back on its own step still completes normally; the audit
trail exists so degradations are visible **after the fact**, not so they
can be blocked in the moment. `mode: async` (the default) fires the audit
in the background without the audited step waiting on it; `mode: sync`
awaits the audit before the step settles, but still never alters the
outcome either way.

**Finding the results.** The audit agent's own submitted output is
persisted as an `OutputEvent::Artifact` named `audit:<step_ref>` on the
AUDITED step's own output tail — no new endpoint or schema change: it
shows up alongside that step's other output in
`GET /v1/tasks/:id/runs/:run/steps` (an entry whose `name` starts with
`audit:`), and `mse_doctor`'s `audit_findings` section (see
`mse://guides/mcp-tool-reference`) flags it across every run the `mse mcp`
process is tracking.

**agent-block-backed audits.** When `AuditDef.agent` names a `kind:
agent_block` `AgentDef` instead, the audit runs entirely in-process via
the existing AgentBlock factory — no operator round-trip, so hops 2-4
above do not apply. The audit agent runs and submits its finding the same
way any other in-process AgentBlock worker does; the observational
invariant and the `audit:<step_ref>` artifact naming are identical either
way.

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
