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
- A body that is meant to be structured data rather than prose can be
  parsed server-side instead of folded as a string — see § Structured
  final bodies below.

Staging never completes the attempt; the final `submit` does. At
final-pull the server folds staged parts into
`{"out": <final>, "parts": {<name>: <value>, ...}}` for the Blueprint
flow (`$.parts["plan.md"]` addressing).

## Structured final bodies: `submit_format: "json"`

The submit body is raw text, so a step's OUTPUT is a single string by
default — a downstream `fanout` `items` or `branch` cond cannot reach
inside it. A Blueprint opts one step out of that by declaring
`submit_format: "json"` on the same meta channels the `@file:` opt-in
uses:

| Tier | Declaration |
|---|---|
| Step | `Step.in.$step_meta.inline = {"submit_format": "json", ...}` |
| Agent | `AgentMeta.ctx = {"submit_format": "json", ...}` |
| BP-global | `Blueprint.default_agent_ctx = {"submit_format": "json", ...}` |

The server parses that step's final body and folds the parsed value, so
`$.<step>.lanes` resolves and a fanout can take
`{"op": "path", "at": "$.<step>.lanes"}` as its `items`.

What the declaration commits you to:

- **Default-deny.** Without it the body folds as a string, byte for
  byte as before — the server never sniffs a JSON-looking body.
- **Declared-strict.** A declared step whose body does not parse is
  rejected with `422` (the message names the agent and echoes the start
  of the body) and the attempt records no OUTPUT. Declare it only for
  an agent whose prompt commits to emitting JSON and nothing else.
- **Unknown values fall back.** Any value other than `"json"` folds the
  body as a string and logs a warning, so a typo degrades visibly
  rather than failing the run.
- **It composes with `@file:`.** The sentinel resolves the file first
  and the parse runs on its contents, so a large structured payload can
  take the file lane without losing its shape.
- **Verdict contracts are unaffected.** A `channel: "body"` contract
  still compares the submitted body; a bare-token gate agent declares no
  `submit_format` and behaves exactly as before.
- **Parts stay strings.** The parse applies to the final body only —
  a staged part (`?name=<name>`) is always folded raw.

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
| structured final OUT | raw text unless the step declares `submit_format: "json"` (above) | the return value is already a JSON value — nothing to declare |

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
