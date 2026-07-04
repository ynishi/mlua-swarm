#!/usr/bin/env bash
# Expand scripts/launchd/com.mse.server.plist.template with the current
# user's absolute paths and install it as a LaunchAgent for `mse serve`.
#
# Idempotent: if the LaunchAgent is already loaded it is booted out first
# so that the new plist replaces the old one cleanly.
#
# Usage:
#   scripts/launchd/install.sh                # install / re-install
#   scripts/launchd/install.sh --uninstall    # bootout + remove
#   scripts/launchd/install.sh --render       # print expanded plist to stdout
#
# Env overrides (rarely needed):
#   CARGO_BIN     defaults to "$HOME/.cargo/bin"
#   PROJECT_ROOT  defaults to the repo root inferred from this script's location

set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

template="$script_dir/com.mse.server.plist.template"
label="com.mse.server"
launch_agents_dir="$HOME/Library/LaunchAgents"
installed_plist="$launch_agents_dir/$label.plist"
domain="gui/$(id -u)"

: "${CARGO_BIN:=$HOME/.cargo/bin}"
: "${PROJECT_ROOT:=$repo_root}"

if [[ ! -f "$template" ]]; then
  echo "ERROR: template not found: $template" >&2
  exit 1
fi

render() {
  sed \
    -e "s|{{HOME}}|$HOME|g" \
    -e "s|{{CARGO_BIN}}|$CARGO_BIN|g" \
    -e "s|{{PROJECT_ROOT}}|$PROJECT_ROOT|g" \
    "$template"
}

uninstall() {
  if launchctl print "$domain/$label" >/dev/null 2>&1; then
    launchctl bootout "$domain/$label" || true
  fi
  rm -f "$installed_plist"
  echo "uninstalled: $installed_plist"
}

case "${1:-install}" in
  --render)
    render
    ;;
  --uninstall)
    uninstall
    ;;
  install)
    mkdir -p "$launch_agents_dir"
    tmp="$(mktemp)"
    trap 'rm -f "$tmp"' EXIT
    render > "$tmp"
    if launchctl print "$domain/$label" >/dev/null 2>&1; then
      launchctl bootout "$domain/$label" || true
    fi
    mv "$tmp" "$installed_plist"
    trap - EXIT
    launchctl bootstrap "$domain" "$installed_plist"
    echo "installed: $installed_plist"
    echo "status:    launchctl print $domain/$label"
    echo "config:    edit ~/.mse/config.toml then launchctl kickstart -k $domain/$label"
    ;;
  *)
    echo "unknown arg: $1" >&2
    echo "usage: $0 [install|--uninstall|--render]" >&2
    exit 2
    ;;
esac
