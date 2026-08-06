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
  "ttl_secs": 600,                    // operator-session TTL for the run
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
