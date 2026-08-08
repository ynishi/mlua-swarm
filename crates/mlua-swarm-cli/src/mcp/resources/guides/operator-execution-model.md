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

### `allow_file_submit` opt-in for the `@file:` sentinel (GH #43)

The `POST /v1/worker/submit` and `POST /v1/worker/artifact` endpoints
accept an `@file:<abs-path>` sentinel body — the SubAgent writes a large
payload under its task `work_dir` and submits the one-line sentinel
instead of streaming the payload back through the LLM. See hop 4 below
for the SubAgent-side contract.

The sentinel is **opt-in per step (default-deny)**: without an opt-in the
server rejects the sentinel body with `400`. The opt-in rides the supply
tiers above — declare `allow_file_submit: true` at any tier, and
`AgentContextMiddleware` folds it into `AgentContextView.extra` at spawn
time, where the sentinel resolver reads it:

| Tier | Declaration |
|---|---|
| Step | `Step.in.$step_meta.inline = {"allow_file_submit": true, ...}` (per-step, overrides Agent / BP-global) |
| Agent | `AgentMeta.ctx = {"allow_file_submit": true, ...}` (all dispatches of this agent) |
| BP-global | `Blueprint.default_agent_ctx = {"allow_file_submit": true, ...}` (all agents unless overridden) |

The value is checked with strict-equality against the JSON boolean
`true`: a string `"true"`, the integer `1`, or `false` all reject with
`400`, mirroring how the other named `AgentContextView` fields are typed.

The path guards (`work_dir` allowlist, ≤ 2 MiB, `404` on missing file)
apply on top of the opt-in check — they are independent gates. Pre-#43
Blueprints that don't declare the key still work byte-for-byte for
inline bodies; only sentinel bodies now require the opt-in.

For the agent-side (agent.md) contract around choosing inline vs
sentinel, see `mse://guides/agent-md-authoring` § Output contract.

### `submit_format` and the structured fold

A sibling key on the same tiers. By default a final body or staged part
whose bytes parse as a JSON **object or array** already folds structured
into the flow ctx (lenient, containers only — scalars and prose stay
strings), so a downstream `fanout` `items` or `branch` cond can address
fields inside it (`$.<step>.lanes`) with no declaration.
`submit_format` adjusts that default per step:

| Tier | Declaration |
|---|---|
| Step | `Step.in.$step_meta.inline = {"submit_format": "json", ...}` |
| Agent | `AgentMeta.ctx = {"submit_format": "json", ...}` |
| BP-global | `Blueprint.default_agent_ctx = {"submit_format": "json", ...}` |

`"json"` is the strict form (body only): an unparseable final body is
rejected with `422` instead of degrading to a string. `"text"` opts the
step's fold out of the lenient parse entirely (body and parts). Any
other value falls back to the default with a warning. The full rules
(scalar handling, `@file:` composition, verdict contracts, verbatim part
files) live in `mse://guides/worker-io-contract` § Structured worker
output.

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

**Steps sharing a nesting root claim it weakly.** Writing one lane each
under a common root (`"out": "$.review.style"` / `"$.review.perf"` / …)
is ordinary Blueprint shape, not an ambiguity, so those steps do not
compete for the root name `review`. A name claimed only as the top
segment of a path written *underneath* it is a **weak** claim; the
`Step.ref`, a declared `projection_name`, and an `out` that is exactly
`$.review` are **strong** claims. A weak claim registers only while
nobody else claims that name — the moment a second step claims it,
strongly or weakly, every weak claim on it is dropped:

| Blueprint shape | result |
|---|---|
| `$.r.a` / `$.r.b` / … written by different steps (a shared nesting root) | `"r"` is nobody's alias; no warning. Address each lane by its own ref / `projection_name` |
| two steps whose `out` is the identical `$.r` | warning + data-plane priority, as before (a genuine ambiguity) |
| `ref: "r"` on one step, `$.r.x` on another | the weak claim yields: no warning, `"r"` still resolves to the `"r"` step |
| `projection_name: "r"` on one step, `$.r.x` on another | the weak claim yields: compiles (this used to be a hard error) |
| `$.r.a` written by exactly one step | `"r"` stays that step's alias, unchanged |

A dropped root name resolves to nothing rather than to some lane: a
`ContextPolicy.steps: ["review"]` filter or a REST `.../steps/review`
lookup against a *shared* root now misses explicitly instead of silently
returning whichever lane happened to be registered first. Name the lane
you mean (its `Step.ref`, or a `projection_name`) — a root with a single
writer is unaffected.

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

#### Fail-open discipline and `CheckPolicy`

The submit-time projection sink is fail-open by default: an unresolved
root, a Data-plane `OutputStore` write error, an
`AgentContextView` state lookup error, or a
`FileProjectionAdapter::materialize_submission` error all only log a
`tracing::warn!` and let the submit itself succeed. That default
preserves Invariant 1 (a submit that reached the domain-plane append
never gets turned into a step failure by the projection half) but
silently hides a partially-realized submission from a caller that would
prefer to fail loudly.

`EngineCfg.check_policy: CheckPolicy` selects one of three modes,
server-wide (per-run override plumbing is a follow-up):

- `Warn` (default) — log the warn, continue. Byte-identical to the
  pre-`CheckPolicy` behaviour every existing caller relies on.
- `Silent` — skip the warn, continue. Useful for a caller that has
  already verified upstream invariants and wants the fail-open branch
  to run without log noise.
- `Strict` — log the warn AND return `EngineError::CheckPolicyStrict`,
  so the caller can fail the step / launch fast. When Strict returns
  an error, the underlying `OutputStore` may already have appended (the
  domain-plane / data-plane append happens before the fail-open
  branch runs) — this "state dirty on fail" is intentional, surfacing
  the mismatch instead of hiding it.

A launch whose Blueprint materializes files must always seed
`init_ctx.project_root` (or `work_dir`) so the resolver above yields a
usable root; under `Strict`, omitting both is what surfaces as a step
error, not a silent skip.

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
that literal shape). Orch drivers that ship as separate distributions
document this in their own Step 4 guide; the `mse-worker` agent
<!-- convention-token-ok: mse-worker is a mlua-swarm public agent kind. -->
definition also carries it verbatim.

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

## Worker degradation reporting

Hop 4's raw `body` carries the attempt's result; it says nothing about
*how* that result was produced. A worker that hits a tool failure mid-task
often has a cheap escape hatch — fall back to a weaker method and still
submit a plausible-looking result — and the submit-side contract gives it
no way to say so. `POST /v1/worker/degradation` (GH #32) is a **separate
observational channel**, sibling of the after-run audit sidecar above:
both keep execution-quality signal off the BP-chain value, so a
`$.<step>` read never sees anything but the worker's own result.

A worker (or the MainAI harness driving it) has two entry points:

- **Direct HTTP** — `POST <base_url>/v1/worker/degradation` with
  `Authorization: Bearer <worker_handle>` and `Content-Type:
  application/json`:

  ```jsonc
  {
    "tool": "code-index",
    "error": "project-binding mismatch; empty result set",
    "fallback": "grep + manual read",
    "note": "index scoped to the wrong worktree" // optional
  }
  ```

  The server injects `step_ref`, `attempt`, and `at` on the persisted
  entry — never trust the client body for those.

- **`mse_worker_submit`'s `degradations` array** (`mse://guides/mcp-tool-reference`)
  — pass one entry per tool failure alongside the ordinary submit call:

  ```jsonc
  {
    "worker_handle": "wh-...",
    "body": "<the actual result>",
    "degradations": [
      { "tool": "code-index", "error": "project-binding mismatch",
        "fallback": "grep + manual read" }
    ]
  }
  ```

  Each entry is POSTed to `/v1/worker/degradation` before the call's own
  submit body lands. An absent (or omitted) `degradations` field is
  pre-#32 behavior — nothing changes.

Entries land on `RunRecord.degradations` — a flat list at the Run level,
each entry carrying its own `step_ref` for locality — and surface via
`GET /v1/runs/:id`, the same read path that already returns
`step_entries`. `mse_doctor` reports a `degradations` section counting
non-empty runs, so an operator or MainAI can spot a degraded run without
walking the full run record.

## Worker stats reporting

The same asymmetry the degradation channel closes exists for *cost*: an
in-process or subprocess worker reports its own token usage at its fold
site, but a WS-operator SubAgent's spawn path has no such site — nothing
in the three hops above knows what the attempt cost. `POST
/v1/worker/stats` is that boundary's self-report, a sibling of the
degradation channel on the same observational plane (it never touches
step OUTPUT, so `$.<step>` is unaffected).

There are two entry points, exactly as for degradations:

- **Direct HTTP** — `POST <base_url>/v1/worker/stats` with
  `Authorization: Bearer <worker_handle>` (or the full
  `capability_token`) and `Content-Type: application/json`:

  ```jsonc
  {
    "worker_kind": "operator",              // optional, defaults to "operator"
    "model": "<the model that served the attempt>",
    "usage": { "input_tokens": 1200, "output_tokens": 340, "total_tokens": 1540 },
    "num_turns": 3,
    "adapter_data": { "…": "free-form, size-capped, never interpreted" }
  }
  ```

- **`mse_worker_submit`'s `stats` object** (`mse://guides/mcp-tool-reference`)
  — the same body, passed alongside the submit that ends the attempt:

  ```jsonc
  {
    "worker_handle": "wh-...",
    "body": "<the actual result>",
    "stats": {
      "model": "<the model that served the attempt>",
      "usage": { "input_tokens": 1200, "output_tokens": 340, "total_tokens": 1540 },
      "num_turns": 3
    }
  }
  ```

  It is POSTed to `/v1/worker/stats` after any `degradations` entries and
  before the call's own submit body lands — the ordering the fold below
  requires. An omitted `stats` field POSTs nothing.

Every field is optional — **including each of the three token fields**.
A reporter that only knows one number sends `"usage": {"total_tokens":
198471}` and the splits read as `0`; a reporter that only knows the
splits has its total derived as `input + output`. An all-empty body is
accepted and dropped. The response is `204`, or `410 Gone` once the
addressed Run is terminal (the same guard the submit / artifact /
degradation routes apply).

That partial shape is the normal case on the Operator axis: a harness
completion notification typically surfaces a single token total for the
SubAgent, not a prompt/completion split.

**Call it before the attempt's final submit.** The engine holds the
reported stats per `(task_id, attempt)`, and the dispatcher drains them
at outcome time — the moment the dispatch settles, which is what the
final `mse_worker_submit` triggers. Stats that arrive after that fold are
never folded into this attempt's record. Re-reporting within one attempt
is last-write-wins, so a worker that learns its usage incrementally can
just POST again before finishing.

Reported stats land on the attempt's `StepEntry` — `worker_kind`,
`model`, `usage`, `num_turns`, `adapter_data`, beside the
dispatcher-measured `started_at_ms` / `completed_at_ms` / `duration_ms` —
and surface on both `GET /v1/runs/:id` (inside `step_entries`) and
`GET /v1/runs/:id/steps`. Wire schemas for all three:
`mse://api/http-endpoints`. The `swarm_run_stats` tool folds that trace
into per-step rows, whole-run `totals`, and a per-model breakdown for one
`run_id`, without needing the run to have been launched by the calling
process. Because reporting is per-boundary and optional, its
`steps_with_stats` / `steps_total` pair is the honest coverage figure —
read the totals against it rather than as a run's full cost.

Runner capability resolution has a separate Run-scoped explain surface:
`GET /v1/runs/:id/bindings`. Each entry returns the pinned declaration as
`requested`, the Core-validated provider attestation as `effective`, and a
mechanical `difference` (model, tools, and launch variant). Provider id,
provider revision, capability snapshot digest, declaration request digest, and final
binding digest remain visible after execution. The route reads only
`RunRecord.input_json.bound_agents`; it never re-resolves the Blueprint or
reads platform wrapper files. A pre-snapshot Run returns `422`, preserving the
distinction between “not recorded” and “currently resolvable.”

**Resuming a Run created before binding snapshots existed.** Such a Run has no
`bound_agents` to restore, so resume (and rerun-from) backfills the snapshot
from the *current* Blueprint at resume time. The explain response marks this
with `snapshot_origin`: `"launch"` when the bindings were pinned at the Run's
initial launch, `"resume_backfill"` when they were re-derived on resume (a
snapshot that carries `bound_agents` but no origin marker also reports
`"resume_backfill"` — the safe side). A backfilled Run's binding identity is
therefore *not* a launch-time pin, and its resume also records a
`binding` / `resume_backfill` degradation. To keep the pre-upgrade replay log
usable, a `resume_backfill` Run deliberately does **not** mix binding digests
into its replay keys (an initial `launch` Run does), so its previously logged
steps still replay verbatim instead of re-executing.

Legacy `profile.worker_binding` conversion is controlled at server startup by
`legacy_worker_binding_policy = "allow" | "reject"` (or CLI
`--legacy-worker-binding-policy`). `allow` is the compatibility default and
records `runner_source=legacy_worker_binding`; `reject` requires an explicit
`runner` or `runner_ref`. This switch affects fresh resolution only—persisted
Run snapshots are never rewritten or re-resolved.

**The contract**: a worker SHOULD report every tool failure it works
around through this channel rather than silently substituting it away.
Honesty becomes cheap, and downstream gates get a machine-checkable
signal that the execution path was compromised — the same motivation as
the audit sidecar above, from the worker's own side instead of an
after-the-fact observer's.

`Blueprint.degradation_policy` (`mse://guides/blueprint-authoring`) is
schema-only today: `warn` (the default) and `fail` both record author
intent, but neither currently changes a Run's outcome — engine
enforcement of `fail` is a follow-up.

---

## Operator naming: three layers, one string

The BP's `OperatorDef.name`, the mint-time `roles: [...]` alias, and the
engine's `register_operator(id, ...)` key are three separate layers that
are only connected by **string equality**. Getting this wrong (or thinking
`main-ai` is a hard-coded system name) is the usual source of "why can't
I run two MainAIs in parallel?".

```
BP (design-time)                    Runtime (per Operator process)
─────────────────────               ──────────────────────────────
operators:                          POST /v1/operators
  - name: "planner_bot"    ◄──┐       { roles: ["planner_bot"],
    kind: MainAi              │         capability_manifest: {...} }  # optional
                              │        → mints sid, reserves alias (+ manifest if sent)
                              │        → register_operator("planner_bot", ...)
agents:                       │
  - name: task-planner        │     WS /v1/operators/:sid/ws
    spec:                     │        → attaches the socket (no registration)
      operator_ref: ──────────┘
        "planner_bot"
```

Rules that fall out of this:

1. **The name is arbitrary.** `"planner_bot"`, `"XXX"`, `"main-ai"` are
   all valid — nothing in the engine treats `main-ai` specially. It only
   became a convention because the default scaffold uses it.
2. **The three sites must be the same literal.** `OperatorDef.name` ==
   the mint's `roles[]` entry == the `register_operator` id. Under
   `strict_binding = true` a typo in the binding target is rejected before
   Spawn (no manifest-owning session resolves for that role). In the default
   non-strict mode the mismatch is not a pre-Spawn gate — the agent binds
   `DeclarationOnly`, and the missing role instead surfaces when the Spawn's
   own routing finds no session claiming it.
   A **run-scoped pin** (§ Two drivers, one role) changes only the third
   site: the launch names the session, so the run resolves against that sid
   instead of the role's current holder. The BP-side pair
   (`OperatorDef.name` == `spec.operator_ref`) is unaffected, and the
   mint's `roles[]` becomes optional on pinned runs.
3. **`kind: MainAi` is the *type*, not the *name*.** It says "when an
   agent references this role, dispatch via the WS thin-path". Multiple
   `OperatorDef`s can have `kind: MainAi` under different names.
4. **The "1 role = 1 sid" exclusivity is per-alias, not global.**
   `POST /v1/operators` returns `409 CONFLICT` only when the same alias
   string is already claimed by a live session (`login.rs` role check
   under `roles_to_sid`). Distinct alias strings never conflict.
5. **The 409 conflict body names the holding session (GH #81 Layer 2 (a)).**
   Alongside the pre-#81 `conflicts: [<role>]` array, the response now
   carries `conflicts_detail: [{role, sid}]` so a recovery driver can
   read the holder sid directly:
   ```json
   {
     "error": "roles conflict",
     "conflicts": ["main-ai"],
     "conflicts_detail": [{"role": "main-ai", "sid": "S-..."}]
   }
   ```
   Existing clients that only read `conflicts` are unaffected.

### Recovery: enumerate and release stale sessions (GH #81 Layer 2)

Two surfaces close the pre-#81 recovery gap where a driver that crashed
after minting a session could only be recovered by a full `mse serve`
restart (which also dropped every OTHER live session):

- **`GET /v1/operators`** — every live session's
  `{sid, roles, joined_at_secs, connected}` plus its 記名 (see below).
  Answers "which sid holds `main-ai`?" without probing every sid
  individually. **Bearer required** — any live session's token opens it.
  This is a breaking change: the route answered anonymously up to
  v0.24.0. The token itself never surfaces in the response.
  MCP counterpart: `mse_operator_list(sid?, limit?)`.
- **`DELETE /v1/operators/by-role/:role`** — release the session
  currently holding `role` without knowing the sid or its Bearer token.
  `404` when no session holds the role, `204` on successful teardown.
  Same trust tier as `mlua_swarm_server_shutdown` (no Bearer — admin
  observability + recovery for stale-session drift). MCP counterpart:
  `mse_operator_leave_by_role(role)`.
  Teardown fails every spawn parked on the session, and a role name is
  process-global — so the holder may be a working session rather than the
  stale one you meant. When at least one `Running` run is pinned to the
  holder, the route refuses with `409` and lists them:
  ```json
  {
    "error": "session is driving in-flight runs; ...",
    "role": "main-ai",
    "sid": "S-...",
    "active_runs": ["R-..."]
  }
  ```
  Repeat with `?force=true` to tear down anyway — the escape hatch for a
  genuinely wedged session whose runs will never finish.

The pre-#81 `DELETE /v1/operators/:sid` (Bearer required) is unchanged
and remains the correct path for a session's own driver to leave
cleanly. `by-role` is the recovery escape hatch when the sid is
unknown, not a replacement for the sid-scoped route.

### Telling parallel sessions apart: the 記名 and the holder list

Taking a Run's Operator seat does not exclude anyone — the last acquire
wins. Two things are therefore what stand between an incoming Assignee
and somebody else's Run, and both are Bearer-gated (any live session's
token):

- **`GET /v1/operators`** — the 記名 list. Each session carries a
  **confirmed part** and an **observed part**.
  - *Confirmed*: the `desc` the session wrote at join, fixed for its
    lifetime. Pass it as `desc` on `POST /v1/operators`, or as the
    mandatory `desc` argument of `mse_operator_join`. About 50
    characters, describing what you are touching and what you are doing
    to it — *not* the repo path, worktree path, Run id, goal or start
    time, all of which are recorded automatically. A session that wrote
    none reports `desc: null`.
  - *Observed*: one `observed[]` entry per Operator seat the session has
    been assigned, written by the server at the moment of the assignment
    — `{run_id, slot, goal, project_root, work_dir, task_metadata,
    at_secs}`. The paths come from the launch's Task-level input and are
    `null` when the launch carried none; nothing is substituted. There
    is no route that deletes an entry. The list is a bounded window: the
    newest 32 per session, with `observed_total` reporting how many
    assignments there really were. Every field of an entry is bounded
    too — `task_metadata` above 4 KiB is dropped with
    `task_metadata_omitted: true`, and `goal` / `project_root` /
    `work_dir` above 1 KiB are cut to fit, ending in `…` with
    `text_truncated: true`. That keeps the ring's serialized size a
    number rather than a hope.

  The confirmed part is what the observed part cannot supply: two
  drivers in the same worktree produce identical paths, and only the
  sentence one of them wrote at join exists nowhere else.

  Ordered by most recent activity first, capped (`?limit=`, default 50,
  ceiling 200), with `total` reporting the count before the cut.

- **`GET /v1/runs/:id/assignees`** — the holder list of one Run. Every
  Operator seat the Blueprint declares plus any the Run holds, each with
  `vacant` and `holder`. A seat nobody holds is **present** and says so
  (`vacant: true`, `holder: null`) rather than being left out, so
  "nobody is on this" is distinguishable from "this response did not
  report holders". `GET /v1/runs/:id` answers the same question in its
  own shape: `current` is now written out as `{}` on a Run holding
  nothing, where it used to vanish from the wire.

  Note the asymmetry with `POST /v1/runs/:id/acquire`, which needs no
  Bearer at all: taking a seat is deliberately ungated (a Bearer must
  not decide assignment), and it is *reading who is on it* that
  requires one.

### Deciding what to do next: the four-axis read

Recognising the Run is one question; knowing what state it is in is
another, and it is the one an Assignee asks constantly rather than only
at a handover. Two more Bearer-gated reads answer it.

- **`GET /v1/runs/:id/handover`** — four axes in one call:

  | axis | where it is in the body |
  |---|---|
  | what has been done | `trace` — a `{route, latest_seq}` reference, not the events |
  | who holds what | `seats` / `seats_source` / `note`, the `/assignees` body verbatim |
  | what is in the air | `unanswered[]` |
  | what to do next | `unanswered[].final_present` / `final_ok`, plus `material_route` |

  `latest_seq` is a watermark: a trace event with a higher `seq` happened
  after this snapshot was taken, so "the picture moved while I was
  reading it" is detectable rather than silent.

  Each `unanswered[]` entry is one request a current holder has been given
  and has not answered, listed **once**. `slot` / `op` / `generation` name
  the Operator seat it was dispatched through and whoever holds that seat
  now. All three are `null` when the request belongs to no seat — a
  `hook_before` goes to the session directly rather than through a seat,
  so there is none to name, and naming the one that happened to answer
  would be a guess. (One driver can hold several seats of a Run: a session
  is registered under its sid *and* under each of its roles, and a launch
  seats each declared slot from whoever answers to that slot's name.)

  Nothing on an entry grades the wait — no age, no deadline, no
  sent/unsent split. A step whose driver went away is *waiting*, not
  broken, and the next action is an ordinary acquire followed by an
  ordinary dispatch. There is no resume, skip or retry verb anywhere on
  this surface.

  `final_present` is the field that prevents the two mistakes worth
  preventing: re-running an attempt that already produced a value (and
  doubling its side effect), or treating an attempt with no value as
  finished. `unread_seats[]` names a held seat whose holder could not be
  asked, so an empty `unanswered` always means "everyone was asked and
  owed nothing".

- **`GET /v1/runs/:id/material?step_id=<id>`** — the material for one
  step: the same `WorkerPayload` a SubAgent self-fetches, plus that
  attempt's `final_present` / `final_ok`. `run_link` is `confirmed` when
  the payload's own context names the Run in the path, `unconfirmed` when
  it carries no Run identity to check against. The `Final`'s **value** is
  not here on purpose — presence and the `ok` flag are what the decision
  needs, and the value is unbounded.

  This route exists next to `GET /v1/worker/prompt` because the gate
  differs, not the payload: the worker route is held by a per-task
  CapToken an Assignee does not have and must not be issued. Note what
  that makes the operator Bearer worth here — `POST /v1/operators` needs
  no credential, so any caller that can reach the server can mint one.
  The gate is a shape check, not confidentiality; bind the server
  accordingly.

### Capability manifest at join

The manifest is **OPTIONAL**. An Operator/MainAI *may* submit what its
execution environment can actually enforce, but the default Blueprint
(`strategy.strict_binding = false`, see
`mse://guides/blueprint-authoring` § Execution assurance) never requires it:
without a manifest, Runner-backed spawns proceed **declaration-only** — the
`runner.tools` / `model` stay requested/declarative and the Operator
self-checks its own environment (§ Operator self-check below). The manifest
becomes **mandatory** only when the Blueprint sets `strict_binding = true`;
there a missing or insufficient manifest fails the launch before any Spawn.

When a manifest *is* submitted it is not copied blindly into a Run. The Server
selects exactly one capability by role alias and `launch_variant`, returns a
`BindReceipt`, and Core checks the requested tool subset, model presence,
variant equality, and request digest before creating the final
`BindingAttestation`.

```json
{
  "roles": ["planner_bot"],
  "capability_manifest": {
    "provider_id": "main-ai-self-report",
    "provider_revision": "2026-07-22",
    "capabilities": [{
      "launch_variant": "code-worker",
      "resolved_model": "claude-sonnet-4",
      "effective_tools": ["Read", "Edit"]
    }]
  }
}
```

The manifest is owned by the joining execution environment; Swarm does not
read platform wrapper files from the Server filesystem. The resolution chain
per Runner-backed agent is compact:

- **manifest present & consistent** → validated `BindingAttestation`.
- **manifest absent** (or no matching variant / role not joined) →
  `DeclarationOnly`; the Run launches and the unattested state is recorded on
  `RunRecord.degradations`. Under `strict_binding = true` this absence is
  instead a launch error before Spawn, naming the agent and its requested
  variant/tools.
- **manifest present & contradicting** (a tool short of the grant, wrong
  variant, digest or model mismatch) → **always an error, in both modes**.

That last line is the invariant: **attestation is optional, but never wrong —
a receipt that contradicts the request fails in both modes.** `strict_binding`
controls only whether an *absent* attestation is tolerated. Any accepted
attestation is persisted in the Run's `BoundAgent` snapshot, so resume never
re-resolves mutable capabilities.

New cross-platform Blueprints use `runner.backend = "ws_operator"` rather
than naming either host. `ManifestBindingProvider` is the reference
implementation of the same `AgentBindingProvider` IF used by the Server:
a Claude Code plugin may derive its manifest from wrapper frontmatter, while
a Codex plugin may derive it from the active model/tool sandbox. Both return
the same `BindReceipt` shape and pass through the same Core validation. The
logical Agent, role prompt, verdict/result contract, and BindRequest therefore
stay identical; only provider provenance and effective platform values differ.

### Operator self-check (non-strict mode)

When the Blueprint is not `strict_binding`, the Server does **not** pre-verify
the Operator's environment: a missing manifest leaves the agent
`DeclarationOnly` and the Spawn still lands. In that mode the requesting side's
declaration is instead carried into the spawn frame so the Operator can check
itself. The `WorkerBinding` on `ctx.meta.runtime` (see Hop 1) now also carries:

- `request_digest` — the immutable declaration-only `BoundAgent` snapshot
  digest (`sha256:<hex>`), a correlation key back to what Core resolved.
- `requested_model` — the model declared in `AgentProfile.model`.

alongside the existing `variant` and `tools`. These are informational
self-check inputs — the Server enforces nothing off them. The Operator SHOULD
compare the spawn frame's requested `variant` / `tools` / `requested_model`
against what its own environment actually runs and, on a mismatch, report it
through the existing degradation channel (`RunRecord.degradations`, see
[Worker degradation reporting](#worker-degradation-reporting)) rather than
silently running a substitute. A receipt that *exists* and contradicts the
request still fails under both strict and non-strict — strictness only controls
whether an absent attestation is tolerated.

### Running multiple MainAI sessions in parallel

The exclusivity above is the only structural constraint — split the role
into per-lane aliases and each lane gets its own MainAI:

```lua
operators = {
  { name = "phase_a_op", kind = "main_ai" },
  { name = "phase_b_op", kind = "main_ai" },
},
agents = {
  { name = "planner",  spec = { operator_ref = "phase_a_op" }, ... },
  { name = "impl",     spec = { operator_ref = "phase_b_op" }, ... },
},
```

Then two Operator processes join independently:

```
Process A:  mse_operator_join(roles={"phase_a_op"}, capability_manifest={...}) → sid=S-aaa
Process B:  mse_operator_join(roles={"phase_b_op"}, capability_manifest={...}) → sid=S-bbb
```

Spawns on the `planner` agent land on process A; spawns on `impl` land
on process B. No lock, no queue, no conflict — the two aliases are
independent registry keys.

Within **one** MainAI session, concurrent Spawns are already multiplexed
over the single WS by `req_id` (see `WSOperatorSession.pending` in
`session.rs`). The practical throughput limit there is on the client
side: the reference `mse_pending_wait` loop pops one frame at a time
(`operator_client.rs::pending_wait`), so if you want a single Operator
to drive many concurrent spawns you need to fan out that pop loop
yourself.

### Two drivers, one role: run-scoped session pins

Splitting the role per lane (above) works when the lanes are part of one
design. It does not cover the other common shape: **two independent
drivers running the same Blueprint against one `mse serve`**. Both want
`main-ai`, and a role is a process-global claim — so without a pin:

- the second driver's `POST /v1/operators` is a `409`, and if it takes
  the role instead, the first driver's next launch compiles against
  *its* session — Spawn frames leave for the wrong process, silently;
- the usual recovery, `DELETE /v1/operators/by-role/main-ai`, fails
  every spawn parked on whoever holds it, including runs that were
  perfectly healthy.

The fix is a launch-time fact rather than a naming scheme: **the
Blueprint declares the logical role; the launch declares which session
that role means for this run.**

```
Blueprint (design-time)         Launch (per run)              Run
────────────────────────        ─────────────────────         ──────────────
operators:                      POST /v1/tasks
  - name: "main-ai"               { blueprint: {...},
agents:                             operator_sid:  "S-aaa",   RunRecord
  - spec:                           operator_desc: "why" }      .operator_sid
      operator_ref: "main-ai"              │                    = "S-aaa"
                                           ▼                    .current
                                  the seat's holder becomes     = { "main-ai":
                                  S-aaa, manifests attest          { op: "S-aaa",
                                  through it                         desc: "why",
                                                                     gen: 1 } }
```

`operator_sid` is accepted by `POST /v1/tasks` and `POST
/v1/tasks/:id/runs`, validated against the live session registry at
request time (an unknown sid is a `400`, before any Task/Run row is
written), and recorded on `RunRecord.operator_sid`. It binds the run's
whole Spawn stream:

- **routing** — the pin makes the pinned session the *holder* of the
  Operator seat named by `spec.operator_ref`, recorded on
  `RunRecord.current`. Every dispatch through a `kind = Operator` agent
  reads that seat's holder afresh, so the run goes to the pinned session
  rather than to whoever else holds the role — and re-assigning the seat
  mid-run moves the next dispatch with it. Nothing about the session is
  baked into the compiled Blueprint;
- **attestation** — capability manifests resolve through the pinned
  session too, so `strict_binding` Blueprints stay `Bound` under pinning;
- **resume** — the pin travels in the run's launch snapshot, so
  `POST /v1/runs/:id/resume` continues on the same session.

A pin that names no live session **fails the launch**. There is
deliberately no fallback to the role: falling back is how a run ends up
on another driver's session in the first place, and it does so silently.

Unpinned launches still reach the role's holder, but they now reach it
*through* `RunRecord.current` rather than beside it. At launch, every
Operator seat the Blueprint declares is filled from its own name: a
session is registered under its sid **and** under each role it claims, so
if anyone holds the role `operators[].name` spells, that holder is
assigned the seat at generation 1 — same holder the old role lookup
found, now recorded where every dispatch reads it. What it is *not* is
byte-for-byte: the assignment is a real `Assign`, so it carries a
generation and a `desc`, and a later handover moves it like any other.

The `desc` is server-authored, because an unpinned launch has no caller
text to use:

```
"auto-seated at launch from the Blueprint-declared operator role
 'main-ai' (no operator_sid pin in the launch request)"
```

Read `GET /v1/runs/:id` and that opening — `auto-seated at launch` — is
how you tell a seat nobody chose from one a caller pinned; a pin's `desc`
is whatever the caller wrote in `operator_desc`.

A seat whose role nobody holds at launch stays `Vacant`. Nothing is
invented for it, and the first dispatch that needs it fails naming the
seat.

##### `operator_desc` — pinning assigns, and an assignment is recorded

A pinned launch does not merely note which session it prefers: it
**assigns** the run to that operator, and the run carries that holder in
`RunRecord.current` (`{ op, desc, gen }`) from then on. So the launch has
to say why, and `operator_desc` is mandatory whenever `operator_sid` is
given — absent, empty, or whitespace-only is a `400`, refused before any
Task/Run row is written. Without `operator_sid` the field is ignored:
there is no assignment for it to describe.

Write it for whoever reads `GET /v1/runs/:id` later and has to work out
why this run went to that session — `"pinned by the launch request"`,
`"mse-mcp auto-pin: this process's sole live operator session"`. The two
`swarm_run` paths below send different text for exactly that reason.

`current` is the run's **live** holder; `operator_sid` is the launch-time
snapshot of the same fact. They agree at launch and diverge afterwards:
re-assigning a run rewrites `current` (at the next generation) and leaves
`operator_sid` as the record of how the run started.

##### `operator_slot` — which declared Operator the pin assigns

A Blueprint may declare several Operators (`operators[]`, one per lane —
see the per-lane alias section above), and `RunRecord.current` holds one
holder **per declared Operator**. So a pin has to land in a named seat,
and the Blueprint is what supplies the names:

| Blueprint `operators[]` | `operator_slot` | result |
|---|---|---|
| exactly one | omitted | that one — nothing to disambiguate |
| two or more | omitted | `400`, listing the declared names |
| any | a declared name | that seat |
| any | a name not declared | `400`, listing the declared names |
| none | any | `400` — no seat to assign to |

`operator_slot` is accepted by `POST /v1/tasks` and
`POST /v1/tasks/:id/runs` alongside `operator_sid`, and — like
`operator_desc` — is read only when a pin is actually being made. The
undeclared-name case is a `400` rather than a new seat on purpose: a
holder filed under a name no agent dispatches through would leave the run
addressing a `Vacant` seat while the pin looked like it took.

Single-Operator Blueprints (every bundled sample) therefore send exactly
the pre-`operator_slot` body. Multi-lane Blueprints name the lane they are
assigning, and the lanes hand over independently: re-assigning
`phase-a-op` moves only the dispatches that resolve through `phase-a-op`.

A pin names **one** lane, but it is not the only lane a launch fills: the
seats it does not name are auto-seated from their own roles, exactly as in
an unpinned launch. So a two-lane Blueprint launched with a driver holding
`phase-a-op` and another holding `phase-b-op` comes up with both lanes
dispatchable, whether the launch pins one of them or neither. A pin always
wins the seat it names — the role holder for that lane is not seated over
it. A lane whose role nobody holds, and which no pin names, stays `Vacant`
until it is assigned.

#### Auto-pin from `mse mcp`

A driver rarely has to name the sid. `swarm_run` on the
`{kind: "id"}` selector pins:

1. the `operator_sid` argument, when given;
2. otherwise this mcp process's **sole** live Operator session (joined
   via `mse_operator_join`), and only when the run targets the server
   that session is joined to.

Zero or several live sessions auto-pin nothing — with several, the
process would have to guess, which is the failure the pin exists to
prevent; name one explicitly there. Inline / file Blueprints run inside
the mcp process, which holds no Operator sessions at all, so an explicit
`operator_sid` on those selectors is rejected rather than ignored.

The auto-pin answers "which session", never "which lane": `swarm_run`
passes `operator_slot` through when the caller gives one, and omits it
otherwise, so a multi-Operator Blueprint gets the server's
candidate-listing `400` rather than a lane this process picked.

So the everyday driver loop is unchanged: join, launch, `mse_pending_wait`
— and the frames come back to the driver that launched, even with a
second driver on the same server.

#### `roles: []` is the canonical join under pinning

Because a pinned run resolves by sid, a driver that always pins does not
need the role claim at all:

```
mse_operator_join(roles={}, capability_manifest={...})  → sid=S-aaa
```

An empty `roles` never conflicts (nothing is claimed), so any number of
drivers can join the same server for the same Blueprint. The Blueprint
still declares `operators[].name` / `spec.operator_ref` — those are
design-time symbols and unaffected. What the empty claim gives up is the
*unpinned* path: launch-time seating looks for a holder of the seat's own
role and an empty claim registers none, so an unpinned launch of that
Blueprint compiles fine, leaves the seat `Vacant`, and then fails on its
first Operator dispatch — loudly, naming the seat. Claim the role and
pin to it when you want a launch-time answer; leave it empty when every
launch carries its own `operator_sid`.

#### Relationship to the delegate layer

`operator_sid` keeps its original meaning for the opt-in
`spawner_hints.layers = ["operator_delegate"]` path — it is that layer's
session backend. The two axes do not interfere: where the delegate layer
applies it bypasses the per-agent spawners entirely (see
`OperatorSpawnerFactory`'s exclusivity note), and where it does not, the
pin decides the AgentSpec axis underneath. One sid, both axes, same
session.

## Responsibility summary

| Hop | Owner       | Reads from                     | Writes to                      |
|----:|-------------|--------------------------------|--------------------------------|
|   1 | mse-server  | `POST /v1/tasks` body + BP + Run override | `Ctx.meta.runtime` (Value)     |
|   2 | mse-server  | `Ctx.meta.runtime` (session.rs) | `Spawn.directive` (String)     |
|   3 | MainAI      | `Spawn.directive` (WS frame)    | SubAgent launch prompt         |
|   4 | SubAgent    | `/v1/worker/prompt` HTTP payload | `/v1/worker/submit` HTTP body  |

## Related

- `mse://guides/bp-lifecycle` — where this model sits in the develop →
  trial-run → operate lifecycle. The `mse_pending_wait` → dispatch →
  `mse_ack` loop described here is the **operate-stage `MainAi`
  contract**; for trial runs prefer `OperatorKind::Automate` (the
  default) so the engine drives the flow without an attached operator.
- `mse://api/http-endpoints` — HTTP wire body schemas for the Task IF surface.
- `mse://api/blueprint-schema` — Blueprint schema, including `default_init_ctx`.
- `mse://guides/id-lifecycle` — the five ID layers (Blueprint, Task, Run, Step, Attempt).
- `mse://guides/agent-md-authoring` — SubAgent (agent.md) canonical shape, size targets, and the agent-side Output contract (inline body vs `@file:` sentinel).
- `mse://guides/mcp-tool-reference` — `mse_operator_join` / `mse_pending_wait` / `mse_ack` details.
- `mse://blueprints/samples/07-dsl-pipeline` — the scaffold shape (`operators = { { name = ..., kind = "main_ai" } }` + agents referencing it via `operator_ref`) the "Operator naming" section above generalizes from.
