# Auth & token model — the three credential layers

`mse serve` uses several credentials that all travel as opaque strings, so
this guide pins each one to exactly one concern. Every API header, config
key, and guide sentence follows this vocabulary (GH #101).

## The three concerns

Every credential answers exactly one of these questions, and no credential
answers two:

| layer | question | credential | transport |
|---|---|---|---|
| **L0 — perimeter** | may you talk to this server at all? | **access token** | `X-MSE-Access-Token` header |
| **L1 — identity** | who are you? | operator session token · worker `CapToken` · `wh-` handle | `Authorization: Bearer` |
| **L2 — capability** | what may you do? | `role` × `scopes` inside the `CapToken`, the role × verb gate, the Run seat check | server-side (never a wire credential) |

One more string exists that is **not** a token: `token_secret` is the
HMAC-SHA256 *signing key* behind `CapToken`. It guards nothing on the wire;
it lets the server verify what it minted.

```
        client
            │  X-MSE-Access-Token: <access token>        L0 perimeter
            ▼
┌─ full /v1 surface ─────────────────────────────────────┐
│ GET  /v1/healthz          L0 exempt (health checks)     │
│ POST /v1/operators        L0 only (issues the operator  │
│ POST /v1/sessions         L0 only  session / CapToken)  │
│ /v1/operators/:sid/*      L0 + Bearer <session token>   │  L1 (operator)
│ /v1/tasks /v1/runs/* /v1/blueprints/* /v1/issues* …     │
│                           L0                            │
│ /v1/worker/*              L0 + Bearer <CapToken | wh->  │  L1 (SubAgent)
└────────────────────────────────────────────────────────┘
  L2 = role × verbs + scopes + seat, evaluated server-side
```

## L0 — the access token

A single static secret, operator-managed. It says nothing about *who* you
are — it only opens the perimeter.

- **Server side**: config key `access_token`, env `MSE_ACCESS_TOKEN`, or
  `mse serve --access-token`. When set, every `/v1` request (including the
  WebSocket upgrade) must carry `X-MSE-Access-Token: <value>`; a missing or
  wrong value is a `401` with no detail. `GET /v1/healthz` is the only
  exemption, and its body stays minimal. An empty token is refused at
  config resolution (an unset `$TOKEN` in a copy-pasted command must not
  produce a gate that accepts an empty header). On shared hosts prefer the
  env var or the config file — a `--access-token` flag value is visible to
  every local user via `ps`.
- **Client side**: set env `MSE_ACCESS_TOKEN` and the whole client surface
  (mse-mcp HTTP tools, `mse bp push`, the operator WS client, worker
  fetch/submit) attaches the header automatically. Unset ⇒ byte-identical
  behavior to a server without a token.
- **Fail-closed startup**: `mse serve` binding a non-loopback address
  (`0.0.0.0`, `[::]`, any external IP) with no access token configured
  refuses to start. Loopback binds (`127.0.0.1`, `::1`) keep working with
  zero new configuration.
- **Why a dedicated header**: `Authorization: Bearer` is already taken —
  worker routes read it as a `CapToken` / `wh-` handle, operator routes as
  the session token. Reusing it would make one header mean different things
  per route. A custom header for a static perimeter key is the established
  self-hosted pattern (cf. Syncthing's `X-API-Key`).
- **Rotation**: swap the secret in config / your platform's secret store,
  restart the server, update clients' `MSE_ACCESS_TOKEN`. There is no
  dual-token grace window.
- The token travels in the header only — never in a URL or query
  parameter (URLs end up in access and proxy logs).

## L1 — identity credentials

- **Operator session token** — issued by `POST /v1/operators` (join):
  `{ sid: "S-<hex>", token }`. The server stores only the SHA-256 digest.
  Guards the operator WS and session CRUD. Join itself needs only L0 —
  putting issuance behind the perimeter is what closes the
  "anyone on the network can mint a bearer" hole.
- **Attach session** (`POST /v1/sessions`) — the second mint path: hands
  out a `CapToken` for a requested role. Behind L0 like join.
- **Worker token** (`CapToken`) — minted at dispatch, self-contained
  (HMAC-signed `agent_id` / `role` / `scopes` / TTL). This is how the
  server knows *which* dispatch/agent a worker call belongs to.
- **Worker handle** (`wh-<8 hex>`) — a short server-side alias for the
  above, the recommended Bearer form for SubAgents. Its 2^32 space is
  brute-forceable on an open network, which is exactly why it is only
  valid *inside* L0 — the fail-closed startup rule is what keeps this
  convenience safe. There is no throttle on failed lookups (known
  limitation; single-tenant posture).

Spawned workers receive `MSE_ACCESS_TOKEN` alongside the existing
`MSE_TOKEN_*` CapToken material, so they can cross the perimeter. Workers
are therefore perimeter-trusted by design — they already hold token secret
material in their environment.

## L2 — capability

The role × verb allow-list, the token's `scopes`, and the Run seat check
stay server-side. A worker's `CapToken` carries both its identity (L1) and
its capability claims (L2) in one object on purpose: both are minted at
dispatch and die with the token's TTL.

## `token_secret` — a key, not a token

When unset it is regenerated from the OS RNG on every boot, which
invalidates all outstanding CapTokens across a restart. That is an
availability concern, not a confidentiality one — so a remote deployment
should pin it (config / platform secret), and the server warns (but does
not refuse) when binding non-loopback with an unpinned key.

## Remote hosting posture

- TLS terminates at the platform edge or a reverse proxy; the server
  speaks plain HTTP behind it.
- Browser WebSocket clients cannot set custom headers and are unsupported.
- Per-operator credentials, scoped access tokens, and multi-node operation
  are out of scope; they would build on this vocabulary, not replace it.

See `mse://guides/server-management` for the operational side (install,
status, recovery).
