# Subprocess backends — declarative CLI invocation templates (`SubprocessDef`)

`AgentKind::Subprocess` runs a step's worker as a headless child process.
Historically the only way to describe that invocation was the spec-based
shape (`spec.program` + `spec.args` literals), which meant every CLI's
flag surface, prompt plumbing, and stdout parsing had to be hand-composed
per Blueprint. GH #83 adds the declarative alternative: a **`SubprocessDef`
template** — data that describes how the materialized worker payload
(system prompt + task + model/tools/cwd) is rendered into the child's
argv / stdin / env / cwd, and how its stdout is normalized back into the
worker-result shape.

Adding support for a new CLI backend means adding **one more named
template to `Blueprint.subprocesses`** — no spawner code changes, no new
enum arms. Templates are deliberately vendor-neutral: this guide (and the
bundled sample) uses only neutral binaries (`sh`, `cat`); shipping
templates for specific agent CLIs is out of scope for the engine.

## The two Subprocess paths

| | spec-based (historical) | template (EmbedAgent, GH #83) |
|---|---|---|
| declared by | `spec.program` / `spec.args` / `spec.use_stdin` / `spec.stream_mode` | `Blueprint.subprocesses[]` + `runner: {backend: "subprocess", template: "<name>"}` |
| worker input | one directive string (stdin or trailing arg) | full worker payload: `{system}` / `{system_file}` / `{prompt}` + model/tools/cwd |
| stdout | lenient JSON-or-raw | declarative `output: {format, result_ptr, ok_from}` (or the same lenient default) |
| existing BPs | untouched, byte-for-byte | opt-in per agent |

Both paths export the same `MSE_TOKEN_AGENT_ID` / `MSE_TOKEN_NONCE` /
`MSE_TASK_ID` / `MSE_ATTEMPT` / `MSE_CTX_AGENT` environment variables, so
a child that prefers to *pull* its payload itself (self-fetch, the
`mse-worker` pattern) can still do so — the template path is the *push*
sibling, not a replacement.

## `SubprocessDef` fields

Declared in the `Blueprint.subprocesses` registry (same shape as
`runners` / `metas` — a named list):

```jsonc
{
  "name": "probe",                    // registry key
  "argv": ["sh", "-c", "..."],        // argv[0] = binary; every element may carry placeholders
  "stdin": "{prompt}",                // optional; rendered + piped to stdin (absent = immediate EOF)
  "env": {"SYS_FILE": "{system_file}"}, // optional; appended to the MSE_* exports
  "cwd": "{work_dir}",                // optional; child working directory
  "output": {                          // optional; stdout normalization (plain mode only)
    "format": "json",                  // "json" = unparsable stdout fails the step
    "result_ptr": "/result",           // RFC 6901 JSON Pointer into the parsed stdout
    "ok_from": "exit_code"             // "exit_code" (default) or a JSON Pointer to a boolean
  },
  "stream_mode": null                  // "ndjson_lines" | "sse_events" | "length_prefixed" | absent
}
```

`output` and `stream_mode` are mutually exclusive (normalization is a
plain-mode declaration; streaming keeps the event protocol untouched) —
declaring both is a compile error.

## The closed placeholder set

Template strings (`argv` elements, `stdin`, `env` values, `cwd`) may
reference exactly these tokens. Rendering is **pure string substitution**
— no conditionals, no loops, no expression language. Any other
`{lowercase_token}` is rejected at compile time (`InvalidSpec`); brace
text that is not a lowercase identifier (e.g. JSON literals inside a
`sh -c` one-liner) is left alone.

| token | value source | when absent |
|---|---|---|
| `{system}` | the agent's rendered `profile.system_prompt` | renders as empty (a profile-less agent is legal) |
| `{system_file}` | path to the system prompt materialized to a file (unconditional — written regardless of the GH #31 size threshold) | spawn fails loud |
| `{prompt}` | the step's task directive (`WorkerPayload.prompt`) | always present |
| `{model}` | `overrides.model`, else `profile.model` | spawn fails loud |
| `{tools_csv}` | `overrides.tools`, else `profile.tools`, CSV-joined | renders as empty |
| `{work_dir}` | the task-level context view (`work_dir`, falling back to `project_root`) | spawn fails loud |
| `{task_id}` | the step's task id | always present |
| `{attempt}` | the attempt number | always present |

Prefer `{system_file}` over `{system}` for anything that lands in argv:
system prompts can exceed OS argv limits, and the file path never does.

## Binding an agent to a template

`Runner::Subprocess` is the Runner-axis face of `AgentKind::Subprocess`
(1:1 name symmetry). It participates in the standard `resolve_runner`
cascade — inline `runner`, `runner_ref` into `Blueprint.runners`, or
`default_runner`:

```jsonc
{
  "name": "headless",
  "kind": "subprocess",
  "spec": {},
  "profile": {"system_prompt": "You are a headless reviewer.", "model": "example-model"},
  "runner": {
    "backend": "subprocess",
    "template": "probe",              // → Blueprint.subprocesses[name == "probe"]
    "overrides": {                     // optional, all fields optional
      "model": "other-model",          // wins over profile.model for {model}
      "tools": ["Read"],               // wins over profile.tools for {tools_csv}
      "cwd": "/somewhere"              // wins over the template's cwd
    }
  }
}
```

Overrides live on the Runner variant — the template itself stays flat and
shareable across agents. Workers backed by *different* templates coexist
freely in one Blueprint.

## Failure semantics

| case | outcome |
|---|---|
| non-zero exit | failed step (`ok = false` — dispatch outcome Blocked) |
| `format: "json"` + unparsable stdout | failed step; value carries `{"raw", "stderr", "parse_error"}` |
| `result_ptr` not found in the parsed stdout | failed step; value carries an actionable `error` |
| `ok_from` pointer missing / not boolean `true` | failed step |
| timeout / cancel | existing run-TTL / cancellation machinery, unchanged |
| referenced placeholder with no source | spawn fails loud (`SpawnError`) before the child ever starts |

## Complete neutral-binary example

The bundled sample `mse://blueprints/samples/11-subprocess-embed` is
runnable end-to-end with nothing but `sh`: one worker whose template
pipes `{prompt}` to stdin, reads the system prompt back out of
`{system_file}`, and answers with JSON that `result_ptr` extracts. The
same shape with a real agent CLI is purely a data change — swap the
`argv` for the CLI's non-interactive form, point its model/tool flags at
`{model}` / `{tools_csv}`, and declare where its output format puts the
result.
