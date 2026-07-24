#!/usr/bin/env bash
# Stop the temporary sampler, preserve its report, and notify the operator.
set -euo pipefail

PROJECT_DIR="/home/coinupbtc/Documents/projects/dgx-spark-gpu-monitor"
ALERTBOT="/home/coinupbtc/.hermes/scripts/alertbot-send.sh"

on_error() {
  local status=$?
  "${ALERTBOT}" --plain "DGX 72-hour baseline finalization failed (exit ${status}). Check: journalctl --user -u dgx-performance-baseline-finish.service"
  exit "${status}"
}
trap on_error ERR

summary="$("${PROJECT_DIR}/.venv/bin/python" "${PROJECT_DIR}/baseline_summary.py" --hours 72)"
systemctl --user disable --now \
  dgx-performance-collector.timer \
  dgx-performance-baseline-finish.timer
"${ALERTBOT}" --plain "DGX 72-hour baseline complete.
${summary}"
