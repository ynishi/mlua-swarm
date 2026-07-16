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

Staging never completes the attempt; the final `submit` does. At
final-pull the server folds staged parts into
`{"out": <final>, "parts": {<name>: <value>, ...}}` for the Blueprint
flow (`$.parts["plan.md"]` addressing).

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
