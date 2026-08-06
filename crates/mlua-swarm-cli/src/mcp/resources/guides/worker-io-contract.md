# Worker I/O contract

How a worker step receives its input and returns its output, and — more
importantly — *why* the two sides are shaped differently. Read this before
authoring an agent (`agent.md`), a worker binding, or a Blueprint that
consumes another step's output.

## The shape

```
            IN (fetch)                     OUT (tool call)
  ┌────────────────────────┐      ┌────────────────────────────┐
  │ GET /v1/worker/prompt  │      │ POST /v1/worker/submit     │  final body
  │   Bearer = capability  │      │ POST /v1/worker/artifact   │  per named part
  │   token / handle       │      │        ?name=<name>        │
  └───────────┬────────────┘      └──────────────┬─────────────┘
              │                                  │ projection sink
        worker executes                          ▼ (server side)
                                   <ctx-dir>/<step>.md   (final, wrapped)
                                   <ctx-dir>/<name>      (each part, raw)
                                           = the NEXT step's IN files
```

One fetch in, one (or a few) tool calls out. The worker never chooses a
file path and never writes its result to disk itself.

## Why IN is an HTTP fetch

- **The server owns the assembly.** System prompt, task directive,
  `AgentContextView` (`project_root` / `work_dir` / `task_metadata`), and
  pointers to prior steps' OUTPUT are put together *at fetch time*, per
  attempt. A fetch always returns the current attempt's truth; files
  written ahead of a spawn would need pre-resolution, cleanup, and would
  go stale on rekick.
- **The fetch is the trust handshake.** The Bearer value is a capability
  token (TTL'd, role × verb gated) minted at dispatch. Only this task's
  worker can read this task's IN. A file on disk cannot enforce that.
- **The relay stays thin.** The operator that spawns the worker forwards
  only a short handle (`wh-XXXXXXXX`) — the prompt bytes never pass
  through the orchestrator's own context window.
- IN is a small, read-once payload, so none of the file-side ergonomics
  (partial reads, grep) are needed on this side.

## Why OUT is a tool call — never a self-written file

Producing OUT happens at the **end** of a worker's run. For an LLM that
has just consumed a long context, that is the single least reliable
moment to be choosing file names and formats: the failure mode is not an
error, it is a *plausible-looking file in the wrong place* — a
hallucinated path, or "wrote something file-shaped and called it done".

So the exit is pinned to tool calls that remove every degree of freedom:

- `POST /v1/worker/submit` — raw body, no path, no format decision. The
  server resolves the task from the Bearer handle.
- `POST /v1/worker/artifact?name=<name>` — one named part per call
  (`plan.md`, `notes.md`, ...). The name is a *call argument*, not a
  path the model composes; re-staging a name overwrites (last write
  wins).
- Large or file-shaped content can use the `@file:<abs-path>` sentinel
  body, resolved server-side.
- A body or part whose bytes are a JSON object / array folds into the
  flow ctx as the parsed structure by default — see § Structured worker
  output below.

Staging never completes the attempt; the final `submit` does. At
final-pull the server folds staged parts into
`{"out": <final>, "parts": {<name>: <value>, ...}}` for the Blueprint
flow (`$.parts["plan.md"]` addressing).

## Structured worker output: the fold and `submit_format`

The submit body and every staged part travel as raw text. What the
Blueprint flow sees is decided at the **fold** (the final-pull that
assembles the step's ctx value), in one of three modes:

- **Default — lenient, containers only.** A final body or staged part
  whose bytes parse as a JSON **object or array** folds as the parsed
  structure, with no declaration, uniformly across the HTTP and
  in-process lanes. `$.<step>.lanes`, a fanout
  `items = $.<step>.parts["plan-meta.json"].lanes`, and a branch cond
  all resolve into it. A container the model wrapped in a markdown code
  fence folds the same way: when the body *starts* with a fence, the
  fence is stripped and the inner bytes are reparsed, so a step that
  asked for bare JSON and got ```` ```json {...} ``` ```` back still
  lands structured. Scalar JSON (`true`, `42`, `"quoted"`, `null`)
  and prose stay strings: a scalar has no addressable interior, and
  parsing it would silently change `Eq` conds and verdict comparisons
  for tokens that happen to be valid JSON. Anything that fails to parse
  also stays a string — a JSON-looking prose body degrades to exactly
  the old behavior, and a fence whose content is not a parseable
  container folds as the original text, fence included.
- **`submit_format: "json"` — strict, body only.** The step *promises*
  JSON: the server parses the final body at submit time (any JSON
  value, scalars included) and rejects an unparseable body with `422`
  (the message names the agent and echoes the start of the body) so
  the contract fails loud instead of folding a malformed string.
  Deliberately body-only — a planner whose body is `plan-meta.json`
  still stages markdown parts (`plan.md`), which must not 422.
- **`submit_format: "text"` — opt-out.** Every string the step submits
  — body and parts alike — folds as itself, even when its bytes are a
  JSON container and even when they are fenced (no parsing, no fence
  stripping). The escape hatch for a step whose downstream wants the
  raw text of JSON-looking output.

`submit_format` is declared on the same meta channels the `@file:`
opt-in uses:

| Tier | Declaration |
|---|---|
| Step | `Step.in.$step_meta.inline = {"submit_format": "json", ...}` |
| Agent | `AgentMeta.ctx = {"submit_format": "json", ...}` |
| BP-global | `Blueprint.default_agent_ctx = {"submit_format": "json", ...}` |

Notes that hold in every mode:

- **Unknown values fall back.** Any value other than `"json"` / `"text"`
  logs a warning and behaves like the default, so a typo degrades
  visibly rather than failing the run.
- **It composes with `@file:`.** The sentinel resolves the file first
  and the mode applies to its contents, exactly as if the same bytes
  had been posted inline.
- **Verdict contracts are unaffected.** Both verdict checks compare the
  submitted string *before* the fold, and a bare verdict token is a
  scalar the lenient fold never touches.
- **Part FILES are always verbatim.** The lenient parse applies to the
  ctx fold only; the materialized part file keeps the submitted bytes
  (see § Files are the server's job).
- **The subprocess lane differs on scalars.** `kind: subprocess` parses
  its stdout as any JSON value (scalars included) at its own boundary —
  a pre-existing contract this fold does not change.

## Files are the server's job (the Adapter half)

The *next* step's cheapest, most reliable primitive is `Read` on a known
path — harness-native, partial reads and grep for free. So the contract
completes on the server: the submit-time projection sink materializes
what the worker submitted into the files the next step (or a human, or a
gate) reads:

- the final body lands as `<ctx-dir>/<step>.md` (front-matter wrapped,
  round-trippable);
- each staged part lands **raw** as `<ctx-dir>/<name>` — a part named
  `plan.md` *is* the plan document on disk, not a JSON envelope;
- part names must be plain file names — anything containing `/`, `\`,
  or `..` is rejected at the adapter (the data-plane copy is kept, the
  file half is skipped fail-open).

`<ctx-dir>` resolves from the launch-supplied `work_dir` /
`project_root` through the Blueprint's projection placement (default
`workspace/tasks/{task_id}/ctx`). When neither root resolves, file
materialization skips fail-open (WARN) — or fails the step under
`check_policy: strict`.

## The in-process twin

Everything above describes a worker in **another process** — a WebSocket
Operator reaching back over HTTP. A worker that runs *inside* the server
(`kind: agent_block`, `kind: lua`, `kind: rust_fn`) has the same contract
with the transport removed:

| | Out-of-process | In-process |
|---|---|---|
| IN | `GET /v1/worker/prompt` → `WorkerPayload` | `WorkerInvocation`, handed to the worker directly |
| task context | `WorkerPayload.context` (the `AgentContextView`) | `WorkerInvocation.context` — the same view, same middleware, same policy filtering |
| final OUT | `POST /v1/worker/submit` | the worker's return value (`agent_block`: `bus.emit(<any kind>, ...)`, first emit wins) |
| named part | `POST /v1/worker/artifact?name=<name>` | `agent_block`: `bus.emit("artifact", {name = ..., content = ...})` |
| structured final OUT | JSON-container bodies fold structured by default (§ Structured worker output) | a Lua table is already structured; a Lua STRING of a JSON container folds structured at the same fold |

Staged parts fold identically on both lanes: stage at least one and the
step's value becomes `{"out": <final>, "parts": {<name>: <value>}}`, so a
downstream cond reads `$.<step>.parts["verdict"]` and a downstream `in`
reads `$.<step>.out`. Stage none and the value stays the plain final body.
Only the worker's OWN parts fold — an `Artifact` another producer appends
to the same attempt's tail (`AfterRunAuditMiddleware`'s `audit:<step_ref>`
sidecar) is deliberately invisible to the BP chain.

Both verdict channels work on both lanes: `channel: "body"` compares the
final body, `channel: "part"` compares a staged `verdict` part.

The rejection is symmetric too, but the *diagnostic* is not, so know
which lane you are on. Out-of-process, a contract violation answers the
submit with HTTP 422. In-process, there is no HTTP response to carry it:
the rejected `Final` never reaches `output_tail`, and the reason is
folded into the attempt's failure instead (`Final rejected before
output_tail: verdict contract violation: …`). The WS Operator fallback
emit is the one route that keeps only a server-side `tracing::warn!`, so
there the caller sees the bare `no Final in output_tail`. Reverse lookup
for that symptom: `mse://guides/blueprint-authoring` § "Symptom → cause".

For a Lua-visible worker (`agent_block` script mode, or `kind: lua`) the
context view arrives as globals rather than as a JSON field —
`_TASK_METADATA` (the launch's `init_ctx.task_metadata`) and `_AGENT_CTX`
(the Blueprint's `default_agent_ctx` / `AgentMeta.ctx`), both real Lua
tables, both `nil` when the field is absent. See
`mse://guides/blueprint-authoring` § "In-process agents" for the full
list.

One asymmetry is deliberate: the pointer list to prior steps' OUTPUT is
out-of-process only. It exists because a worker in another process cannot
read the flow ctx and needs somewhere to fetch from. An in-process worker
declares what it needs in its own `in` expression (`in: $.<prior_step>`)
and gets the value directly — no pointer, no fetch.

## Keep worker defaults generic

None of the above lives in a worker's own prompt or defaults. Placement,
naming, wrapping, and supply policy are adapter/middleware concerns
(`FileProjectionAdapter`, projection placement, context-supply tiers), so:

- an agent author writes *what the agent does*, not where its output
  goes;
- a Blueprint author picks placement/policy declaratively;
- swapping storage or layout policy touches the adapter, not every
  agent.

## Authoring checklist

- Agent prompts: end with the submit tool call. Never instruct an agent
  to `Write` its deliverable to a path it composes itself.
- Multi-file producers: stage each file as a named part
  (`?name=plan.md`), then submit the final body.
- Consumers: read the previous step's files from `<ctx-dir>`, or address
  parts in the flow with `$.parts["<name>"]`.

Related resources: `mse://guides/operator-execution-model` (the 3-hop
spawn relay), `mse://guides/agent-md-authoring` (agent prompt shape,
inline body vs `@file:` sentinel).
