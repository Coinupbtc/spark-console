#!/usr/bin/env bash
# Spark Console (DGX ops control panel) — tailnet bind, PWA.
#
# Binds to the TAILSCALE address only — never 0.0.0.0. This is the control
# panel that can stop models and restart services, so it must stay off the
# open LAN/internet. Mirror of pokemon-arb/scripts/run_mobile_api.sh.
#   iPhone: http://sparkmax-10ef.tail6cfceb.ts.net:8085/
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
PORT="${PORT:-8085}"

# Resolve our own tailnet v4 address rather than hard-coding it: it survives a
# tailnet re-key, and if tailscaled is down we want a loud failure, not a
# silent bind to something world-reachable.
BIND="$(tailscale ip -4 2>/dev/null | head -1 || true)"
if [[ -z "$BIND" ]]; then
  echo "FATAL: tailscale has no IPv4 address — refusing to start." >&2
  echo "       (binding 0.0.0.0 would expose the control panel to the LAN)" >&2
  if [[ -x "$HOME/.hermes/scripts/alertbot-send.sh" ]]; then
    "$HOME/.hermes/scripts/alertbot-send.sh" \
      "🔴 Spark Console refused to start: tailscale has no IPv4 address" || true
  fi
  exit 1
fi

cd "$ROOT"
export HOST="$BIND"
export PORT="$PORT"
# TrustedHostMiddleware must accept the phone's Host header (tailnet IP or
# MagicDNS FQDN) in addition to loopback. Loopback stays allowed so agents +
# the console watchdog (curl 127.0.0.1:8085) keep working after the rebind.
export CONSOLE_ALLOWED_HOSTS="${CONSOLE_ALLOWED_HOSTS:-$BIND,sparkmax-10ef.tail6cfceb.ts.net}"
exec "$ROOT/.venv/bin/python" "$ROOT/server.py"
