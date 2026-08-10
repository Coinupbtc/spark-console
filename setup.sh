#!/usr/bin/env bash
# One-command setup + start for Spark Console (portable core)
set -euo pipefail
cd "$(dirname "$0")"

echo "==> Spark Console setup"
python3 -m venv .venv
./.venv/bin/pip -q install -U pip
./.venv/bin/pip -q install -r requirements.txt

export PORT="${PORT:-8085}"
export HOST="${HOST:-127.0.0.1}"

echo
echo "Starting on http://${HOST}:${PORT}/"
exec ./start.sh
