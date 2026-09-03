# Running `mse serve` on Fly.io

Single-machine reference deployment for remote self-hosting (GH #101
Layer 3). The security model behind every step here — what the access
token is, what `token_secret` is, why the server refuses to start
half-configured — is `mse://guides/auth-token-model`.

What you get: one always-on machine running `mse serve` behind Fly's
TLS edge, state on a volume, the whole `/v1` surface behind the L0
access token. TLS terminates at the edge; the server speaks plain HTTP
inside, and clients reach it as `https://` (the operator WS client maps
that to `wss://` on its own).

## Prerequisites

- `flyctl` authenticated (`fly auth login`)
- this directory (`contrib/fly/`) as your working directory

## 1. Create the app and volume

```bash
fly launch --no-deploy --copy-config   # accepts fly.toml, asks for the app name
fly volumes create mse_data --size 1   # same region as primary_region
```

Edit `fly.toml`: set `app`, `primary_region`, and (optionally) pin a
newer image tag.

## 2. Set the two secrets

```bash
fly secrets set \
  MSE_ACCESS_TOKEN="$(openssl rand -hex 32)" \
  MSE_TOKEN_SECRET="$(openssl rand -hex 32)"
```

- `MSE_ACCESS_TOKEN` — the L0 perimeter token. Mandatory: binding
  `0.0.0.0` without it, the server exits at startup with a clear error
  (fail-closed), so forgetting this step breaks the deploy instead of
  exposing an open server.
- `MSE_TOKEN_SECRET` — the CapToken signing key (hex). Optional but
  strongly recommended: unpinned, it is regenerated every boot and a
  machine restart invalidates all outstanding worker tokens (the server
  warns about this on non-loopback binds).

Keep the access-token value — every client needs it.

## 3. Deploy and verify

```bash
fly deploy
APP=https://<your-app>.fly.dev
curl -s -o /dev/null -w '%{http_code}\n' $APP/v1/healthz    # 200 (exempt)
curl -s -o /dev/null -w '%{http_code}\n' $APP/v1/status     # 401 (gate works)
curl -s -o /dev/null -w '%{http_code}\n' \
  -H "X-MSE-Access-Token: <token>" $APP/v1/status           # 200
```

Restart survival check: `fly machine restart <id>`, then repeat the
authorized `/v1/status` call — state (Blueprints, runs, operator
sessions) persists on the volume, and outstanding CapTokens stay valid
because `token_secret` is pinned.

## 4. Point clients at it

```bash
export MSE_HTTP=https://<your-app>.fly.dev
export MSE_ACCESS_TOKEN=<token>
```

With those two set, the whole client surface works unchanged: the
operator WS client connects through the TLS edge (`wss://`), and worker
fetch/submit and `mse bp build --register --server ...` attach the
`X-MSE-Access-Token` header automatically. For an MCP client (e.g.
Claude Code), put both variables in the `mse` server's `env` block.

## Constraints (by design)

- **One machine, one volume.** SQLite single-writer + in-process engine
  state — do not add machines or regions to this app.
- **Autostop stays off** (`auto_stop_machines = "off"` in fly.toml):
  the operator WS is long-lived and Fly does not guarantee active
  WebSockets survive autostop.
- **Browser WebSocket clients are unsupported** (they cannot send the
  custom access-token header); clients are the native `mse` tooling.
- The access token is a single shared static secret — rotation is
  `fly secrets set MSE_ACCESS_TOKEN=<new>` (triggers a restart) plus
  updating every client. Per-operator credentials are out of scope
  (see the guide's non-goals).
