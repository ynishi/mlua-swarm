# Enhance flow — a natural-language issue into a verified Blueprint commit

The enhance flow is a self-improvement loop over Blueprints. You post a
plain-language issue against a Blueprint; an LLM turns it into an RFC 6902
JSON Patch; the patch is applied; N verifiers judge the result in parallel;
a committer either writes the new Blueprint version or rejects it with
reasons. The document being patched is an ordinary Blueprint in the
`BlueprintStore` — including, if you aim it there, the enhance flow's own.

The flow is **off by default** and needs two things in place before the first
issue can land. Both are below.

## The flow

The bundled `enhance-default` Blueprint is four steps:

```text
patch-spawner  ->  patch-applier  ->  fanout(verifier-router x N)  ->  committer
   $.patch           $.applied              $.verdicts                  $.commit
```

Every step reads `$` (the whole ctx) and writes its own key, so each step sees
everything the earlier ones produced. The fanout's `items` is `$.verifiers`
and its `bind` is `$.axis`, which makes the verifier count data rather than
structure: the same Blueprint runs one lane per configured axis.

| step | backend | what it does |
|---|---|---|
| `patch-spawner` | `agent_block` (LLM) | turns `intent` into `{ops, bump, rationale}` |
| `patch-applier` | `lua` | applies the ops, recomputes touched agent hashes, bumps `metadata.version_label` |
| `verifier-router` | `lua` | one lane per axis, each returning `{axis, outcome}` (Pass or Deny) |
| `committer` | `lua` | any Deny rejects with reasons; otherwise carries the new document out for commit |

The three `lua` workers are embedded in the binary, so they need nothing on
disk. `patch-spawner` is the only step that calls a model, and the only one
you are expected to swap.

The four built-in verifier axes are `des` (the patched document still has
`id` / `flow` / `agents`), `canonical` (re-canonicalizing the document
reproduces the hash the applier computed), `noop` (the patch actually changed
something), and `agent-ref` (every `Step.ref` resolves to a declared agent).
Which axes run is the setting's `verifier_axes`; an empty array skips
verification entirely and the committer commits unconditionally.

## What it takes to run

**1. Start the server with the flow enabled.**

```bash
mse serve --enable-enhance-flow
```

The built-in default is `false` (config-file key: `enable_enhance_flow`). With
the flow off, the three Lua worker ids are absent from the spawner registry
and compiling the Blueprint fails with
`agent 'patch-applier' spec invalid: fn_id 'patch-applier' not registered in factory`.

**2. Create the `default` enhance setting.**

Nothing is seeded at boot. The enhance application reads exactly the setting
whose id is `"default"`, so until you POST one under that id every tick errors
out and every issue you post ends up `rejected` with reason
`dispatch failed: ...`.

A third, softer prerequisite: the spawner has to be able to reach a model. The
bundled default is an `agent_block` agent carrying `spec.project_root` — the
compile-time fallback working directory, which a task-level `work_dir` /
`project_root` in the context view outranks. If that backend is not usable in
your deployment, swap the agent instead (see below).

## Driving it

Create the setting. `blueprint` is the orbit Blueprint inline and complete;
the server commits it to the `BlueprintStore` first and keeps only an id ref,
so the response body is the ref form, not what you sent:

```jsonc
// POST /v1/enhance-settings  ->  201
{
  "id": "default",
  "ttl_secs": 600,                    // how long the drain waits for one flow
                                      // before giving up on it — read "The epoch
                                      // ceiling" below before you rely on it.
                                      // Must be greater than 0.
  "blueprint": { /* full Blueprint document — schema: mse://api/blueprint-schema */ },
  "verifier_axes": ["des", "canonical", "noop", "agent-ref"]  // optional; this is the default
}
```

Post an issue against the Blueprint you want changed:

```jsonc
// POST /v1/issues  ->  201 {"issue_id": "h-<uuid>", "status": "pending"}
{
  "blueprint_id": "main",
  "intent": "Add a 'smoke' tag to the Blueprint metadata."
}
```

`intent` must be non-empty (`400` otherwise). Then poll `GET
/v1/issues/:issue_id`, whose body is
`{issue_id, status, blueprint_id, intent, reason, new_version}`. `status` is
one of `pending` / `in_flight` / `applied` / `rejected`; `new_version` is
present only when applied and `reason` only when rejected. A run that reached
the committer — applied or verifier-rejected — also appends exactly one entry
to `GET /v1/enhance/log` (and `/v1/enhance/log/:issue_id`), carrying the
per-axis verdicts. A run that never got that far (an infra fault, e.g. a
missing setting) only shows up as the issue's `rejected` reason.

## The epoch ceiling

One tick is one epoch — one issue, popped, dispatched, and driven to a
commit decision. `ttl_secs` bounds **how long the drain waits** for that
flow to come back, and nothing else. It does not bound the epoch (the store
calls on either side of the flow sit outside it), and it does not stop the
work (a worker already running keeps running). Read both as the definition,
not as caveats; the rest of this section is what follows from them.

The knob exists to let the Swarm give up on an Operator that has stopped
answering. That wait is the thing it bounds, and it bounds it exactly.

The drain is single-threaded, so a `patch-spawner` whose model call hangs
stalls not just its own issue but every issue queued behind it. `ttl_secs`
is what gets the drain moving again in that case. It is not a bound on
anything else: the store calls on either side of the flow (reading the
setting, resolving the orbit and target Blueprints, writing the new
version, appending the log, updating issue status) sit outside it and are
bounded by nothing. A `BlueprintStore` blocked on git index-lock
contention still wedges the drain.

`ttl_secs` is also stamped on the epoch's operator session token, where it
does nothing: Operator tokens are exempt from the expiry check. Bounding
the drain's wait is the field's only effect. Set it to the number of
seconds you are willing to wait for a whole flow — spawner call, patch
apply, every verifier lane, committer — not to the length of the model
call alone. `0` is refused at write time (`POST` / `PUT
/v1/enhance-settings` answers `400`); it is a typo, not a way to say
"unbounded".

When `ttl_secs` elapses, the drain stops waiting and:

- **Nothing is committed.** The epoch's only write to the `BlueprintStore`
  happens after the flow completes, so a fired ceiling always precedes it.
  The target Blueprint's head is byte-identical to what it was when the
  issue was popped — there is no half-written version, and no `prev_hash`
  drift for a later epoch to trip over.
- **The issue is terminal `rejected`**, with a reason naming the ceiling it
  blew through and the setting field to raise.
- **Nothing is appended to `/v1/enhance/log`.** The log records epochs that
  reached the committer, and carries their per-axis verdicts; a timed-out
  epoch has none. Same treatment as every other infra fault.
- **The worker is not stopped.** See below before you re-post.

### The worker keeps running (known limitation)

Giving up on the wait does not reach the work. A worker running in one of
the in-process lanes lives in its own task, not inside the future the drain
dropped, and nothing in the server signals it to stop:

| spawner | what a fired ceiling does to it |
|---|---|
| in-process (Lua / Rust fn) | nothing — it runs to its own end and submits into an epoch nobody reads |
| `agent-block` | nothing — the SDK run, its Lua VM and the MCP servers it drives all continue |
| subprocess | nothing — the child process keeps running |

This is not a bug in the ceiling. `ttl_secs` bounds a wait on an Operator,
and none of the lanes above is a wait: they are computations running in
this process. Bounding *them* would mean a different mechanism (a timeout
inside the agent, or a subprocess wrapper that enforces one).

**Do not re-post on reflex.** The issue going terminal does not mean the
work stopped, and a re-post while the previous worker is alive puts two
uncoordinated writers under the same `project_root`. Recovery order:

1. **Check the previous worker has exited.** If the agent writes to a
   working tree, check that tree too — see below.
2. **Raise `ttl_secs`** if the epoch legitimately needs longer. A ceiling
   that trips routinely is cutting real work loose each time.
3. **Re-post.** There is no run record or replay log on this path, so
   nothing is resumable — a re-post is a fresh epoch, and because nothing
   was committed it starts from exactly the same `prev_hash`.

### What a fired ceiling does not undo

The ceiling ends a wait. It does not roll anything back:

- **Whatever the worker wrote is still there**, and it is still being
  written to while the worker runs. The next epoch reads that state as if
  it were yours.
- **The worker's result still arrives.** It submits against an epoch nobody
  reads. Harmless on this path (no files are materialized for an `automate`
  launch), but it is not evidence that the epoch continued.
- **The operator session attached for the epoch is not detached** — shared
  with the success path, which does not detach either, so it is not
  specific to a fired ceiling.

## The spawner's output contract

The spawner's `in` is `$`, so its prompt is the whole init ctx:

```jsonc
{
  "issue":        {"issue_id": "...", "blueprint_id": "...", "intent": "..."},
  "prev_bp_yaml": "<the target Blueprint as YAML — this is what you patch>",
  "prev_hash":    "<hex>",
  "epoch_id":     { /* optimistic-concurrency token, traceability only */ },
  "verifiers":    ["des", "canonical", "noop", "agent-ref"]
}
```

Its result must fold into a JSON object. Who reads what:

| reader | key | required? |
|---|---|---|
| `patch-applier` | `ops` — array of RFC 6902 operations | falls back to `[]` |
| `patch-applier` | `bump` — `major` / `minor` / `patch` | falls back to `patch` |
| `committer` | the value itself, which must be an object | **required** — otherwise `committer: ctx.patch must be a table` |
| `committer` | `rationale` | **required**, missing raises |
| `committer` | `bump` | **required**, missing raises |

The applier's fallbacks are not a licence to omit `bump`: the committer raises
on it two steps later. An empty `ops` array is a legal answer — the `noop`
axis then denies it, which is the intended way to say "nothing to do here".

Each op's `path` is an RFC 6901 pointer into the JSON representation of
`prev_bp_yaml`, and array indices are 0-based (`/agents/0/profile/system_prompt`).
Do not patch `version_hash`: replacing or adding an agent's
`profile.system_prompt` makes the applier recompute that agent's
`profile.version_hash`, so an explicit hash op can only conflict with it.

A reply wrapped in a markdown code fence is also folded: when a submitted body
leads with a fence, the fence is stripped and the inner bytes are reparsed, so
a model that fences its JSON despite being told not to still lands structured.
That fallback does not apply to a step declaring `submit_format: "text"`,
which folds every string as itself, fence included. Full folding rules:
`mse://guides/worker-io-contract`.

## Swapping the spawner

`EnhanceSetting.spawner` takes an `AgentDef`. At dispatch time it replaces the
orbit Blueprint's `patch-spawner` agent in the in-memory copy used for that
run:

- **Omitted** — whatever the Blueprint declares runs, byte-for-byte the
  pre-override behavior.
- **Present** — the definition replaces the matching agent, and its `name` is
  forced back to `patch-spawner` so the flow's `Step.ref` keeps resolving.
  Nothing is written back to the store: the override is a setting-level knob,
  so editing the setting re-swaps the spawner without cutting a new Blueprint
  version.
- **Present, but the Blueprint declares no `patch-spawner`** — dispatch fails
  loud with `spawner override: orbit blueprint declares no agent named
  "patch-spawner"`. A silently ignored override would keep running the
  Blueprint's own spawner while you believe the swap took effect.

The point is to change the execution backend without rewriting the Blueprint.
Any `AgentKind` the registry can build is fair game — `agent_block`,
`subprocess`, `operator`.

The spec-based subprocess form needs no template:

```jsonc
// POST /v1/enhance-settings
{
  "id": "default",
  "ttl_secs": 600,
  "blueprint": { /* ... */ },
  "spawner": {
    "name": "patch-spawner",
    "kind": "subprocess",
    "spec": {"program": "sh", "args": ["-c", "<your invocation>"], "use_stdin": true}
  }
}
```

The template form (`"runner": {"backend": "subprocess", "template": "<name>"}`)
resolves the template out of the **orbit Blueprint's** `subprocesses`
registry, so that Blueprint has to declare it. In exchange you get the closed
placeholder set (`{system_file}` / `{prompt}` / `{model}` and friends) and
declarative stdout normalization: `mse://guides/subprocess-backends`.

Whichever backend you pick, the swapped-in spawner still owes the output
contract above — the downstream Lua steps are unchanged. `mse` ships no
vendor-specific templates; the examples here use neutral binaries only.

## Failure modes

| symptom | cause |
|---|---|
| `committer: ctx.patch must be a table` | the spawner's reply did not fold into a JSON object. The fence fallback covers a wrapped container, but prose before or after the JSON still leaves it a string. |
| `verifier deny: noop: patch is no-op (new_hash == prev_hash)` | `ops` was empty, or applying it changed nothing. Not a defect — the axis doing its job. |
| every issue goes straight to `rejected` with `dispatch failed: ...` | one of the two prerequisites is missing: the flow is not enabled, or there is no setting under id `default`. |
| `fn_id '<id>' not registered in factory` at compile time | the server was started without `--enable-enhance-flow`. |
| `spawner override: orbit blueprint declares no agent named "patch-spawner"` | the setting carries a `spawner`, but the orbit Blueprint has no agent under that name. |
| `enhance epoch exceeded the <N>s ceiling ...` | the drain stopped waiting after `ttl_secs`. Nothing was committed, but **only the wait ended** — a worker already running keeps running, so re-posting now would put a second writer under the same `project_root`. Check it has exited (and what it left there), raise `ttl_secs` if the epoch needs longer, then re-post. See "The epoch ceiling". |
| `enhance setting "default" declares ttl_secs: 0 ...` | `ttl_secs` must be greater than 0; zero is not a way to say "unbounded". |
