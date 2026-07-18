# mse — Agent (agent.md) authoring guide

An **agent.md** file is the durable system prompt for a Claude Code SubAgent
(built-in or custom). It is what gets fetched and pushed into the SubAgent's
context window every time the agent is invoked. Because that context window is
finite and shared with the runtime task payload (fetched Task IF, `Read`
results, `tool_result` bodies, MCP resource fetches, `PreOut` file contents),
**an oversized agent.md leaves no room to actually do the work**.

This guide states the canonical shape, size targets, and anti-patterns for
SubAgent prompts used from mse — the Operator wrapper agents, the mse-worker
family, and any custom SubAgent that participates in a Blueprint dispatch. It
follows the Claude Code SubAgent conventions; see the References section for
primary sources.

## Canonical shape (4 sections)

The Anthropic official examples (`code-reviewer`, `debugger`, `data-scientist`,
`db-reader`) all converge on the same 4-layer structure. Reproduce it:

1. **Role — 1 sentence.** What this agent is and what it excels at. No
   preamble, no background, no history. If it takes more than one sentence, the
   agent is doing too much and should be split.

2. **`When invoked:` — numbered workflow.** The exact steps this agent runs,
   in order. Prefer 3–8 steps. Each step is imperative ("Read X", "Post to
   /v1/…", "Return Y"). No decision trees, no exceptional paths described in
   prose — put those into the step body as sub-bullets if truly needed.

3. **Tool guidance / key practices — checklist.** Which tools to reach for at
   which step, plus non-obvious constraints (auth headers, ordering rules,
   idempotency). Keep it to items that *change what the agent does*; do not
   describe every tool from scratch (the tool schemas are already loaded).

4. **Output format — explicit.** What the caller expects back: the exact
   `return` literal shape, the JSON schema, or the human-readable format. If
   the caller parses the output, put a fenced example. If the agent hands off
   to another SubAgent, state the handoff artifact path.

**Input is not a section.** The delegation prompt (Task message) carries the
input. Do not enumerate example inputs in the durable prompt — that content is
per-invocation and belongs in the caller's `Task` dispatch, not in agent.md.

## Frontmatter shape (how mse's `$agent_md` loader parses this file)

The Blueprint `$agent_md` ref (`{"$agent_md": "path/to/agent.md"}`) resolves the
file through mse's `agent_md_loader`. The loader is a thin adaptor around the
same Claude Code SubAgent shape — YAML frontmatter between `---` fences, then a
Markdown body — so the on-disk shape you already use with Claude Code is what
mse consumes. There's no separate mse-specific frontmatter dialect.

- **Frontmatter is required.** The file must start with `---\n`, contain YAML
  key/value pairs, and close with a `---\n` line. Files with no frontmatter are
  **silently skipped** in a directory scan (they're treated as explanatory
  docs, not agents), and rejected outright when loaded by explicit path with a
  `NoFrontmatter` error. A leading blank line before the opening `---` breaks
  detection — start the file with `---` on line 1.

- **`name:` is the only required field.** Missing `name` fails with
  `MissingName`. Every other key is optional; missing keys default to nothing
  and are not reported as errors.

- **First-class fields.** These six keys are extracted into typed
  `AgentProfile` slots. The names and semantics are Claude Code's:

  | key              | shape                              | slot                          |
  |------------------|------------------------------------|-------------------------------|
  | `name`           | string (required)                  | `AgentDef.name`               |
  | `description`    | string                             | `profile.description` (trimmed) |
  | `model`          | string (`sonnet` / `haiku` / …)    | `profile.model`               |
  | `effort`         | string (`low` / `medium` / `high` / …) | `profile.effort`          |
  | `tools`          | CSV string **or** YAML array       | `profile.tools` (normalized `Vec<String>`) |
  | `worker_binding` | string (SubAgent variant name)     | `profile.worker_binding`      |

  `tools` accepts either form — `tools: "Read, Edit, Grep"` and
  `tools: [Read, Edit, Grep]` produce the same `Vec<String>`. Whitespace and
  empty entries are trimmed.

- **Everything else is dumped into `extras`.** Any key outside the first-class
  set (`permissionMode`, `memory`, `abtest`, arbitrary custom keys, …) is
  preserved verbatim as JSON under `profile.extras`. This is a future-proof
  carry — Claude Code frontmatter keys mse doesn't know about yet still round-
  trip through the loader unchanged. They're **not** injected into the SubAgent
  system prompt on the mse side; if a key like `permissionMode` needs to affect
  the actual SubAgent grant, that's the wrapper file's job (mse's binding
  target), not the loader's.

- **Body is verbatim.** Everything after the closing `---\n` becomes
  `profile.system_prompt` as-is — no heading normalization, no truncation, no
  hidden preface. The body is what the SubAgent sees; the frontmatter never
  bleeds into the prompt.

- **Content hash.** `profile.version_hash` is `blake3(body)` (hex-encoded), so
  frontmatter edits don't invalidate the hash — only body changes do. This
  matches how the compiler's post-hook recomputes the hash for
  `/agents/N/profile/system_prompt` replacements.

**Practical implication for authoring.** You can write agent.md files in the
same shape you already use with Claude Code and drop them straight into a
mse Blueprint with `$agent_md`. mse-only fields (`worker_binding`) go in the
same frontmatter block; Claude Code-only fields (`permissionMode`, `memory`,
…) survive round-trip but don't affect mse's dispatch — they belong to the
wrapper's grant, not to this durable prompt.

## Output contract: inline body vs `@file:` sentinel

`POST /v1/worker/submit` accepts two body forms. Declare **exactly one** in
the agent's Output format section — never present both as alternatives in a
single agent.md (an LLM given a choice tends to defensively do both, writing
the file *and* re-emitting the full body inline):

- **Inline body (the default).** The submit body is the output itself — a
  short literal (`DONE ...`, `PASS|BLOCKED`) or a markdown report. Use this
  unless the payload is genuinely too large to re-emit.
- **`@file:<abs-path>` sentinel.** The worker `Write`s the payload to a file
  under its task `work_dir` and submits the single line
  `@file:<abs-path>`; the server resolves the file into the body. The step
  must be opted in on the Blueprint side — see
  `mse://guides/operator-execution-model` § `allow_file_submit` opt-in for
  the declaration form and precedence. Without the opt-in the server
  rejects the sentinel with `400`. Path guards apply either way (absolute
  path, under `work_dir`, ≤ 2 MiB).

**Self-report convention.** If the agent uses a tool outside the set its
prompt intends, or falls back to another tool after a failure, it should
report that via `POST /v1/worker/degradation` (`{tool, error, fallback,
note}`) instead of silently working around it. Reports surface in the run
record and in `mse_doctor`'s degradations section, which is the standard
post-run checkpoint for contract deviations.

## Size targets

There is **no official byte limit** in the Claude Code SubAgent docs, but
neighboring numbers make the working range obvious:

- Official example agents: **20–40 lines** each (measured across the 4
  reference agents in `code.claude.com/docs/en/sub-agents §Example subagents`).
- `MEMORY.md` injection threshold: **first 200 lines or 25 KB, whichever is
  first**; beyond that the runtime injects a curation instruction instead of
  the content ([sub-agents §Enable persistent memory][mem]).
- Windows delegation hard cap: **8191 characters** in the delegation prompt
  string; longer prompts fail to dispatch ([agent-sdk/subagents §Long prompt
  failures on Windows][winlim]).

Target: **≤ 200 lines / ≤ 25 KB** for any agent.md. Treat 500+ lines / 50 KB+
as a defect — at that size the fetched system_prompt alone will consume a
significant fraction of a 200 K context window, leaving no headroom for the
task payload, and the SubAgent will fail with *Prompt is too long* the first
time it hits a real tool_result of any size.

These thresholds are observed by the `bp_doctor` MCP tool, which fetches a
Blueprint from `GET /v1/blueprints/:id/head`, measures every agent's
`profile.system_prompt` bytes/lines, and returns per-agent severity
(`OK` / `WARN` / `BLOCK`) plus an aggregate verdict. The default check is:

- **WARN** if `system_prompt` bytes ≥ 25 KB **or** lines ≥ 200.
- **BLOCK** if `system_prompt` bytes ≥ 50 KB **or** lines ≥ 500.

The verdict is a report label only — `bp_doctor` never prevents any
dispatch. **BLOCK is disabled by default**: modern Claude models (Opus-tier
and long-window Fable variants) tolerate large system prompts, so an
over-block-threshold agent falls back to WARN. Callers running against a
strict 200 K-window model can pass `disable_block=false` to opt into the
BLOCK band. Any of the four thresholds (`warn_bytes` / `warn_lines` /
`block_bytes` / `block_lines`) can also be overridden per call to fit the
model actually in play.

Agents whose backend does not carry an `agent.md` body (`RustFn` and other
spec-only agents where `profile` is `None`) are reported with size 0 and
severity `OK`.

## What to remove (anti-patterns)

Everything in this list is already available to the SubAgent from another
channel; re-stating it in agent.md is dead weight that costs tokens on every
invocation.

- **Content of `CLAUDE.md` / `~/.claude/CLAUDE.md` / project rules / managed
  policy files.** The full memory hierarchy is auto-loaded into every SubAgent
  except the built-in `Explore` / `Plan` ([sub-agents §What loads at
  startup][load]). Re-quoting a rule your agent is expected to follow just
  duplicates it.
- **Accident logs, retrospectives, "why this exists" narratives.** SubAgents
  do not inherit the parent's conversation history or tool results ([agent-sdk
  §What subagents inherit][inh]). A long historical explanation gives the
  SubAgent no leverage it can use at runtime; it only bloats the fetch.
- **Full copies of upstream doc pages.** Link to `mse://guides/<slug>` or the
  canonical URL and let the agent fetch on demand if truly needed.
- **Input examples / fixture blobs.** These belong in the delegation prompt
  (Task message), which is per-invocation and correctly sized to the specific
  work.
- **Exhaustive tool schemas.** Tool schemas are loaded automatically; the
  agent only needs *which tool to reach for and why*, not the JSON Schema.
- **Persona / register decoration for agents that are pure workers.** Voice
  and register belong to persona-facing agents; worker SubAgents that just
  return structured output do not benefit from them.

## Fetch-vs-embed policy (mse SubAgent specifics)

When an mse SubAgent needs data at runtime, use the right channel for each
kind of payload:

| Payload kind | Delivery channel | Why |
|---|---|---|
| **System prompt (agent role definition)** | `fetch` (mse-worker `fetch` MCP tool) | Literal, hard guarantee. FilePath-only delivery fails on cwd drift / relative-path ambiguity ~ 100% of the time in some environments; embed as literal to eliminate that class. |
| **Lightweight context (FilePath, short config, env-derived values)** | Embed in system prompt OR pass via Task IF from MainAI | Small enough to co-locate; passing through Task IF keeps agent.md invocation-agnostic. |
| **PreOut (upstream phase artifacts, large fixtures)** | Pass the path in Task IF; SubAgent runs `Read` itself | Keeps agent.md size bounded and lets the SubAgent decide what to load. |
| **Anything else** | Assume it will fail. | These three channels cover the reliable delivery paths for mse SubAgents. |

The size discipline in the previous section exists to preserve headroom for
the "PreOut via `Read`" case: if agent.md consumes 77 K of the 200 K window
just for the system prompt, `Read` results have nowhere to go and the agent
crashes on the first non-trivial file.

## Verifying how your agent materializes (`bp_explain_agent`)

Once an agent.md is wired into a Blueprint (`profile.worker_binding` set,
or reachable via `$agent_md`), do not guess whether the definition
survived the compile/spawn pipeline intact — check it. Two faces expose
the same read-only, dry-run view, resolved statically from the
Blueprint's head commit alone (no engine state touched, nothing
dispatched):

- **HTTP**: `GET /v1/blueprints/:bp_id/agents/:agent/explain` on
  `mse serve` — the source of truth. Same unauthenticated trust tier and
  404 mapping as `GET /v1/agents/:name/render-size`.
- **MCP**: `bp_explain_agent {bp_id, agent, bind?, wrapper_dir?}` —
  proxies the HTTP response, then adds the one check the server cannot do
  itself: when the agent has a `worker_binding`, it reads the worker
  wrapper `.claude/agents/<variant>.md` (override with `wrapper_dir`,
  default `.claude/agents`) and diffs its frontmatter `tools` against
  `declared_tools.tools`, returned as `tool_drift: {matched,
  declared_only, wrapper_only, wrapper_only_contract,
  wrapper_only_meaningful}`. `wrapper_only_contract` is the mse-worker
  fetch/submit tools (`mse_worker_fetch` / `mse_worker_submit`) every
  wrapper carries regardless of author intent — noise, not signal.
  `wrapper_only_meaningful` is everything else in `wrapper_only`: tools
  the wrapper actually grants that the agent.md never declared.
  `wrapper_only` (flat, unsplit) is retained for one release cycle for
  backward compat and may be removed after that; `declared_only` stays
  the primary drift signal either way (see point 1 below).

The response covers `blueprint` (id/version), `agent` (name/kind),
`worker_binding`/`binding_note`, `declared_tools`, `system_prompt`
(bytes/lines/`template_variables`/`template_syntax_error`),
`effective_ctx`, and `output`
(`projection_name`/`naming_warnings`/`parts_note`). Three things matter
most when reading it:

1. **`declared_tools` never grants anything.** It is `profile.tools`,
   verbatim, marked `informational: true` — the effective tool surface an
   Operator-dispatched SubAgent actually gets is whatever the worker
   wrapper's frontmatter declares. `bp_explain_agent`'s
   `tool_drift.declared_only` is the single most useful field this whole
   view produces: a tool your agent.md lists but the wrapper does not
   grant is silently unusable, and neither a successful compile nor a
   successful spawn tells you that on its own. `wrapper_missing: true`
   (+ `wrapper_error`) means the drift check could not run at all —
   treat that result as "unverified", not "clean".
2. **`effective_ctx` only resolves the 3 static tiers** —
   `AgentInline > MetaRef > BpGlobal` (`AgentMeta.ctx` beats
   `AgentMeta.meta_ref` beats `Blueprint.default_agent_ctx`), reusing the
   exact merge `AgentContextMiddleware` runs at spawn time rather than
   re-implementing it. The full runtime cascade is
   `Run > Task > Step > Agent > BP-global`; the Run/Task/Step tiers only
   exist once a launch or a dispatched Step supplies them, so this static
   view has nothing to show for them (`effective_ctx.note` states this).
3. **`output.projection_name` and the part-staging shape change.** If the
   agent's step ever stages named output parts
   (`mse_worker_submit` with `name`), the step's OUTPUT changes shape to
   `{"out", "parts"}` — see `output.parts_note`. A downstream `Step.in`
   written against the plain (non-parts) shape breaks the moment staging
   is introduced for that step; check this before wiring a consumer.

**Phase 2 carry**: this view does not analyze flow expr consumers — which
`$.step` references read this agent's output, and in what shape. That
requires walking the `mlua-flow-ir` `flow` tree and is out of scope for
the current implementation; cross-reference `output.projection_name`
against the Blueprint's `flow` manually until that lands.

### Sweeping a whole Blueprint (`bp_explain_agents`)

Checking one agent at a time does not scale once a Blueprint has more
than a handful of agents. `bp_explain_agents {bp_id, bind?,
wrapper_dir?}` sweeps every agent in one call and reports a compact
per-agent row — counts, not full lists, since a whole-Blueprint response
has to stay small:

```json
{
  "blueprint": { "id": "...", "version": "..." },
  "agents": [
    {
      "name": "...",
      "variant": "...",
      "wrapper_missing": false,
      "wrapper_error": null,
      "declared_only_count": 2,
      "wrapper_only_contract_count": 2,
      "wrapper_only_meaningful_count": 1
    }
  ]
}
```

Agents without a `worker_binding` get `variant: null` and every
wrapper-side field `null` — there is nothing to diff. A missing or
unparsable wrapper file sets `wrapper_missing: true` + `wrapper_error`,
with every count at `0`. Once a row's `declared_only_count` or
`wrapper_only_meaningful_count` is non-zero, drill down with
`bp_explain_agent {bp_id, agent}` for the full `tool_drift` detail
(which tools, not just how many).

## Quick self-check before you commit an agent.md

1. Is the file **≤ 200 lines / ≤ 25 KB**?
2. Does it have exactly the **4 canonical sections** (Role / When invoked /
   Tool guidance / Output format)?
3. Does it **avoid re-stating** anything already in `CLAUDE.md`, user-global
   rules, or tool schemas?
4. If the agent takes runtime input, does the prompt describe **the shape**
   (not example values)?
5. Does it declare its **Output format** concretely enough that the caller
   can parse or verify the return?
6. Does it commit to **one submit form** (inline body *or* `@file:` sentinel,
   never both) — and if `@file:`, does the Blueprint declare
   `allow_file_submit: true` for the step?

Six yeses → ship it. Any no → shrink and re-check.

## References

- [Create custom subagents — Claude Code Docs][sub-agents]
- [Subagents in the SDK — Claude Agent SDK Docs][sdk-subagents]
- [Effective context engineering for AI agents — Anthropic Engineering][context]
- [Writing effective tools for AI agents — Anthropic Engineering][tools]
- `mse://guides/operator-execution-model` — Operator-kind 3-hop execution model this SubAgent is dispatched under (Spawn.directive rendering, supply tiers, `allow_file_submit` opt-in).
- `mse://guides/blueprint-authoring` — Blueprint document shape that names this agent.

[sub-agents]: https://code.claude.com/docs/en/sub-agents
[sdk-subagents]: https://code.claude.com/docs/en/agent-sdk/subagents
[mem]: https://code.claude.com/docs/en/sub-agents#enable-persistent-memory
[winlim]: https://code.claude.com/docs/en/agent-sdk/subagents
[load]: https://code.claude.com/docs/en/sub-agents
[inh]: https://code.claude.com/docs/en/agent-sdk/subagents
[context]: https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents
[tools]: https://www.anthropic.com/engineering/writing-tools-for-agents
