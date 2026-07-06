# mse — ID lifecycle

Canonical inventory of every identifier that flows through a swarm run,
with mint sites, lifecycle scopes, and reference chains. This is the
authoritative answer to "which ID means what" (issues #11 / #13).

## The five-layer hierarchy

One kick flows through five identity layers:

```
Blueprint (blueprint_id, user-supplied)      reusable pipeline definition
    │  referenced via BlueprintSelector (id | inline | file)
Task (T-<hex>)                               one work item; persisted, CRUD
    │  kicked 1..N times
Run (R-<hex>)                                one kick; carries the step trace
    │  one per dispatched Blueprint step
Step (ST-<hex>)                              one step execution
    │  retries bump a counter, same StepId
Attempt (u32 counter)                        retry counter inside a step
```

## Inventory

| ID | type | prefix / shape | minted at | scope | referenced by |
|---|---|---|---|---|---|
| `blueprint_id` | `BlueprintId` newtype (store) / plain `String` (schema) | user-supplied (default `"main"`) | Blueprint registration | as long as the Blueprint exists | `TaskRecord.blueprint_ref`, version history |
| `BlueprintVersion` | `ContentHash` (blake3) | content hash | git-backed store commit | one immutable Blueprint revision | `swarm_run` response `bound_version` |
| `task_id` | `TaskId` newtype | `T-<hex>` | `POST /v1/tasks` (server); ad hoc in `mse mcp` in-process runs | work item; survives restarts (task store) | `RunRecord.task_id`, `GET /v1/tasks/:id` |
| `run_id` | `RunId` newtype | `R-<hex>` | `POST /v1/tasks` / `POST /v1/tasks/:id/runs` (server); `swarm_run` (in-process) | one kick | `ctx.meta.runtime.run_id`, pending-wait payloads, spawn directives, `GET /v1/runs/:id` |
| `step_id` | `StepId` newtype | `ST-<hex>` | engine `start_task`, one per dispatched `Step.ref` | one step execution (stable across retries) | `StepEntry.step_id`, worker endpoints, `worker-of-<step_id>` token agent_id |
| `attempt` | `u32` counter | 1-based | bumped by each `dispatch_attempt` | one dispatch of a step | `TaskState.attempt` |
| `sid` | `String` (SessionId shape) | `S-<hex>` | `POST /v1/operators` | one WS operator session | WS URL path, `req_id` prefix, `RunRecord.operator_sid` |
| `SessionId` | newtype | `S-<hex>` | engine `attach` paths | one attached engine operator session | `EngineState.sessions` key |
| `worker_id` | `WorkerId` newtype | `W-<hex>` | each spawner at spawn time | one spawned worker (observability only) | trace log (`worker spawned` events) |
| `worker_handle` | `String` | `wh-<hex>` | engine at dispatch (short handle → token nonce) | one worker's Bearer session | `ctx.meta.runtime.worker_handle`, `/v1/worker/*` Bearer |
| `req_id` | `String` | `<sid>-<ask\|hb\|ha\|spawn>-<uuid>` | server per outbound operator frame | one server→operator request | echoed back in `mse_ack`; `parent_req_id` chains |
| `capability_token` | `CapToken` (base64 JSON) | opaque | `TokenSigner::mint` (HMAC-SHA256) | until `expire_at` / `max_uses` | spawn frames; full-token Bearer on `/v1/worker/*` |
| `agent_id` | `String` (inside `CapToken`) | free-form (`worker-of-<step_id>` for workers) | token mint | token lifetime | role × verb gate, task-ownership check |
| `br-` / `hk-` / `ob-` ids | `String` | `br-<hex>` / `hk-<hex>` / `ob-<hex>` | engine `attach_with` inline registration | process lifetime | bridge / hook / operator-backend registries |

## Session ids: one shape

The WS operator `sid` and the engine-side `SessionId` are two registries
for the same concept — "an attached operator session" — and both mint the
`S-<hex>` shape (`sid` used to be `op-<uuid>`; unified in issue #11). They
are not the same value: a WS login does not create an engine session. The
`sid` is an identifier, not a credential — the 10-hex `token` returned by
`POST /v1/operators` is the sole bearer secret on that path.

The engine's operator-backend registry uses `ob-<hex>` (renamed from
`op-<hex>` so no registry shares a prefix with the session ids).

## Entropy: two generators

- `uid_hex` — process-unique (counter XOR per-process random salt).
  **Not unguessable.** Used for every identifier above (`T-` / `R-` /
  `ST-` / `S-` / `W-` / `wh-` / `br-` / `hk-` / `ob-`).
- `secure_hex` — OS-RNG. Used for bearer secrets only: the operator login
  `token` and the `CapToken.nonce`.

Rule of thumb: if it names a thing, it's `uid_hex`; if holding it grants
access, it's `secure_hex`.

## capability_token lifecycle

```
mint    TokenSigner::mint — agent_id / role / scopes / issued_at /
        expire_at (worker tokens: 1800s TTL) / max_uses / nonce
        (secure_hex) / sig_hex (HMAC-SHA256 over the signing input)
   │
carry   Spawn frames carry the encoded token (URL-safe base64 JSON) and a
        short handle `wh-<hex>` that the server maps to the token nonce.
        /v1/worker/* accepts either Bearer form (short handle or full token).
   │
verify  Engine::verify_token — 4 steps: (1) constant-time signature check,
        (2) expiry, (3) role × verb gate, (4) server-side uses_left
        consume. Task-ownership additionally via verify_token_for_task.
   │
expire  expire_at passes or max_uses exhausts; worker handles die with the
        server process (in-memory map).
```

## req_id

Minted server-side, one per outbound operator frame, as
`<sid>-<verb>-<uuid>` where `verb` ∈ `ask` / `hb` (hook_before) /
`ha` (hook_after) / `spawn`. The operator echoes it in `mse_ack` to
correlate the reply; `parent_req_id` chains follow-ups to the frame that
caused them.

## Known limitations

- `CapToken.nonce` doubles as the token's server-side lookup key
  (`CapToken::id()`) — token ID and secret are the same value. Tracked
  for a follow-up split.
- `BlueprintId` exists as a newtype in the store layer while the schema
  `Blueprint.id` is a plain `String` — two representations of one concept.
- Newtype constructors accept arbitrary strings (no prefix validation);
  prefix validators are a follow-up.
