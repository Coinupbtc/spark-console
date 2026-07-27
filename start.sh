#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

PORT="${PORT:-8085}"
HOST="${HOST:-127.0.0.1}"

if [[ ! -x .venv/bin/python ]]; then
  echo "No .venv yet — run ./setup.sh first (or: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt)"
  exit 1
fi

if ss -tlnp 2>/dev/null | grep -q ":${PORT}"; then
  echo "Already running → http://${HOST}:${PORT}/"
  exit 0
fi

mkdir -p data
./.venv/bin/python collector.py
nohup env PORT="$PORT" HOST="$HOST" .venv/bin/python server.py > data/server.log 2>&1 &
echo $! > data/server.pid
sleep 1
echo "Spark Console → http://${HOST}:${PORT}/"
echo "Logs          → data/server.log"
echo "Stop with     → ./stop.sh"
