#!/usr/bin/env bash
# Keep timer failures visible; systemd journal alone is not a human notification path.
set -euo pipefail

PROJECT_DIR="/home/coinupbtc/Documents/projects/dgx-spark-gpu-monitor"
ALERTBOT="/home/coinupbtc/.hermes/scripts/alertbot-send.sh"

on_error() {
  local status=$?
  "${ALERTBOT}" --plain "DGX performance collector failed (exit ${status}). Check: journalctl --user -u dgx-performance-collector.service"
  exit "${status}"
}
trap on_error ERR

# This opt-in path proves alerting without corrupting the real collector command.
if [[ "${DGX_COLLECTOR_FORCE_FAIL:-0}" == "1" ]]; then
  false
fi

"${PROJECT_DIR}/.venv/bin/python" "${PROJECT_DIR}/collector.py"
