#!/usr/bin/env bash
# One-shot Fly.io deploy driver for `mse serve`.
#
# Idempotent: safe to re-run — existing app / volume / secrets / IPs are
# reused. Walkthrough: contrib/fly/README.md; security model:
# mse://guides/auth-token-model. Same driver shape as the sibling
# self-hosted daemons (journal-mcp / mini-app-mcp / outline-mcp).
#
# Usage (app name is yours to choose; no default):
#   MSE_FLY_APP=<your-app-name> bash contrib/fly/deploy.sh
#   MSE_FLY_APP=<app> MSE_ACCESS_TOKEN=<token> MSE_TOKEN_SECRET=<hex> bash contrib/fly/deploy.sh
set -euo pipefail

APP="${MSE_FLY_APP:-}"
if [ -z "$APP" ]; then
  echo "set MSE_FLY_APP to your Fly app name, e.g.:"
  echo "  MSE_FLY_APP=my-mse-server bash contrib/fly/deploy.sh"
  exit 1
fi
REGION="${MSE_FLY_REGION:-nrt}"
VOLUME="mse_data"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

echo "== 0. prerequisites =="
command -v fly >/dev/null || { echo "flyctl not installed (https://fly.io/docs/flyctl/install/)"; exit 1; }
fly auth whoami >/dev/null || { echo "not authenticated — run: fly auth login"; exit 1; }

echo "== 1. app ($APP) =="
if fly status --app "$APP" >/dev/null 2>&1; then
  echo "app $APP exists — reuse"
else
  fly apps create "$APP"
fi

echo "== 2. volume =="
if fly volumes list --app "$APP" | grep -q "$VOLUME"; then
  echo "volume $VOLUME exists — reuse"
else
  fly volumes create "$VOLUME" --app "$APP" --region "$REGION" --size 1 --yes
fi

echo "== 3. secrets =="
if fly secrets list --app "$APP" | grep -q MSE_ACCESS_TOKEN; then
  echo "MSE_ACCESS_TOKEN already set — reuse (clients need the same value)"
  ACCESS_TOKEN="${MSE_ACCESS_TOKEN:-}"
else
  ACCESS_TOKEN="${MSE_ACCESS_TOKEN:-$(openssl rand -hex 32)}"
  fly secrets set --app "$APP" --stage "MSE_ACCESS_TOKEN=$ACCESS_TOKEN"
  echo "access token set. SAVE THIS VALUE (every client needs it):"
  echo "  $ACCESS_TOKEN"
fi
if fly secrets list --app "$APP" | grep -q MSE_TOKEN_SECRET; then
  echo "MSE_TOKEN_SECRET already set — reuse"
else
  fly secrets set --app "$APP" --stage "MSE_TOKEN_SECRET=${MSE_TOKEN_SECRET:-$(openssl rand -hex 32)}"
fi

echo "== 4. public IPs =="
# First deploys have been observed to fail the automatic allocation
# ("error allocating ipv6") and come up unreachable (curl 000, empty
# `fly ips list`) — allocate explicitly so the app is reachable.
IPS="$(fly ips list --app "$APP")"
echo "$IPS" | grep -q " v4 " || fly ips allocate-v4 --shared --app "$APP"
echo "$IPS" | grep -q " v6 " || fly ips allocate-v6 --app "$APP" || \
  echo "(v6 allocation failed — shared v4 alone is enough for <app>.fly.dev)"

echo "== 5. deploy =="
fly deploy --config contrib/fly/fly.toml --app "$APP"

echo "== 6. smoke (verification ladder) =="
BASE="https://$APP.fly.dev"
code() { curl -s -m 15 -o /dev/null -w '%{http_code}' "$@"; }
echo "healthz no-token   (expect 200): $(code "$BASE/v1/healthz")"
echo "status  no-token   (expect 401): $(code "$BASE/v1/status")"
if [ -n "${ACCESS_TOKEN:-}" ]; then
  echo "status  with-token (expect 200): $(code -H "X-MSE-Access-Token: $ACCESS_TOKEN" "$BASE/v1/status")"
else
  echo "status  with-token: skipped (token pre-existed and was not exported — re-run with MSE_ACCESS_TOKEN=<value>)"
fi

echo
echo "client wiring:"
echo "  export MSE_HTTP=$BASE"
echo "  export MSE_ACCESS_TOKEN=<the value above>"
