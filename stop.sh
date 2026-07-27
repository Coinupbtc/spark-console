#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(pwd)"

if [[ -f data/server.pid ]]; then
  kill "$(cat data/server.pid)" 2>/dev/null || true
  rm -f data/server.pid
fi
# Portable: only match this repo's server process (no homelab pathnames).
pkill -f "${ROOT}/.venv/bin/python server.py" 2>/dev/null || true
pkill -f "${ROOT}/server.py" 2>/dev/null || true
echo "Dashboard stopped."
