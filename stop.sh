#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if [[ -f data/server.pid ]]; then
  kill "$(cat data/server.pid)" 2>/dev/null || true
  rm -f data/server.pid
fi
pkill -f "dgx-spark-gpu-monitor/.venv/bin/python server.py" 2>/dev/null || true
echo "Dashboard stopped."