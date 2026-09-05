# Running blocks on the caller's host (`agent-block` launch variant)

A *block* is a deterministic Lua step — a worktree setup, a merge, a
pre-commit scan — that touches the caller's repository and needs no
model. Until now the only way to run one without paying a SubAgent
spawn was `AgentKind::AgentBlock` + `Runner::AgentBlockInProcess`: the
**server** loads `spec.script_path` from its own filesystem and runs it
inside its own process, with `init_ctx.work_dir` as the working directory.

That works exactly as long as the server and the repository share a
host. A hosted `mse serve` has none of the three things a block needs —
the script, `git`, the working tree — and the last one cannot be shipped
to it: the point of the block is to change the caller's tree.

The invariant is therefore **a block runs on the host that has the
repository**. The stdio MCP (`mse mcp`) already runs there, and the `mse`
binary already links the same `agent-block-core` runtime the server
uses. So the MCP runs the block itself.

## Declaring a block step

Bind the agent to the `agent-block` launch variant of the ordinary
platform-neutral Operator runner. No `script_path`, no path of any kind:

```lua
{
  name = "checkout-prep",                       -- = the block's directory name
  kind = "operator",
  runner = { backend = "ws_operator", variant = "agent-block" },
  spec = { operator_ref = "main-ai" },
  profile = { system_prompt = "" },           -- or an $agent_md ref
}
```

The block is addressed **by the agent's name**: the MCP resolves
`$MSE_BLOCKS_DIR/<name>/init.lua` on its own host. Where the blocks live
is that host's business, not the Blueprint's — which is also why the
Blueprint no longer carries an absolute path that is only true on the
author's machine.

To the server this is an ordinary Operator step: it dispatches a `Spawn`
frame with `worker.variant = "agent-block"` and waits for the worker
endpoints. A server that predates this guide dispatches it just the same
— the variant is a string to it.

## What the MCP does with the frame

The WS reader sees the variant as the frame arrives and **diverts it**:
it never enters the queue `mse_pending_wait` pops from, so the block
runs whether or not anyone is polling — during a blocking `swarm_run`,
or in a Blueprint made only of blocks. A background task then:

1. resolves `$MSE_BLOCKS_DIR/<agent>/init.lua` (a name that is not one
   plain path component is refused — no `/`, no `..`, not hidden);
2. `GET /v1/worker/prompt` with the spawn's worker handle, exactly as a
   SubAgent would (a `system_ref` is resolved the same way);
3. runs the script with the launch's `work_dir` (else `project_root`,
   else the MCP's cwd) as the project root;
4. POSTs every staged part to `/v1/worker/artifact` and the body to
   `/v1/worker/submit` (`?ok=false` when the script said so, or when
   anything above failed — the reason is the body);
5. acks the spawn.

`mse_pending_wait` reports `blocks_dispatched` — how many spawns were
diverted since its previous call — so a driver can see that frames were
taken. The MainAI never gets a turn for a block, which is the whole cost
model: zero model calls, same as in-process.

The only thing a block step needs from the driver is a joined session
for the run to be pinned to (`mse_operator_join`, then `swarm_run` on the
Blueprint id): the spawn is delivered over that session's socket.

`MSE_BLOCKS_DIR` is read per spawn, from the MCP process's environment.
Set it where the MCP is configured (the `env` block of the server entry
in `mcp.json`, or the shell that launches it).

## Script contract

Identical to the in-process runtime, so a block is portable between the
two:

| global | content |
|---|---|
| `_PROMPT` | the step's evaluated `in` |
| `_CONTEXT` | `profile.system_prompt` |
| `_TASK_METADATA` | the launch's `init_ctx.task_metadata` (absent when none) |
| `_AGENT_CTX` | the agent's declared context bag (absent when empty) |

A script returns by `bus.emit(<kind>, payload)`: `payload.content`, else
`payload.response`, else the whole payload is the body; `payload.ok =
false` fails the attempt. `bus.emit("artifact", {name, content})` stages a
named part and leaves the script running. A script that finishes without
emitting is a failed attempt.

## Joining with a manifest

`mse_operator_join` adds an `agent-block` capability to any manifest it
submits that does not already declare one, so a `strict_binding`
Blueprint resolves its block steps to this session without the caller
listing the variant by hand.

## Choosing between the two runtimes

| | in-process (`agent_block_in_process`) | caller-side (`ws_operator` + `agent-block`) |
|---|---|---|
| where the script runs | the server process | the `mse mcp` process |
| what it needs on that host | script, `git`, the repository | the same |
| script addressing | `spec.script_path` (a path on the server) | agent name under `MSE_BLOCKS_DIR` |
| server host = repository host | required | not required |
| model calls | none | none |
| needs an operator session | no | yes — the spawn arrives over the session's socket; no polling required |

Keep in-process for a server you run next to the repository; use the
caller-side variant for a hosted one. A Blueprint can carry both kinds
of step.
