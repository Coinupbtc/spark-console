#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if ss -tlnp 2>/dev/null | grep -q ':8085'; then
  echo "Already running → http://localhost:8085"
  exit 0
fi

mkdir -p data
.venv/bin/python collector.py
nohup .venv/bin/python server.py > data/server.log 2>&1 &
echo $! > data/server.pid
sleep 1
echo "Dashboard → http://localhost:8085"
echo "Logs      → data/server.log"
echo "Stop with → ./stop.sh"