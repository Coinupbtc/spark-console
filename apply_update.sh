#!/usr/bin/env bash
# Allowlisted Spark Console update worker. One key per run. Never restarts
# DS4F / H3 / helper. Hermes path restarts gateways via hermes update +
# house reapply scripts.
set -euo pipefail

HOME="${HOME:-/home/coinupbtc}"
KEY="${1:-}"
HERMES_AGENT="${HERMES_AGENT_DIR:-$HOME/.hermes/hermes-agent}"
HERMES_BIN="${HERMES_AGENT}/venv/bin/hermes"
INJECT="${HOME}/.hermes/scripts/inject-media-wizard.py"
REAPPLY_MEDIA="${HOME}/.hermes/scripts/reapply-media-wizard.sh"
REAPPLY_DET="${HOME}/.hermes/scripts/reapply-deterministic-exit.sh"
DSPARK_FF="${HOME}/scripts/dgx/dspark-ff-pull.sh"
RECIPES="${HOME}/.hermes/scripts/recipe-autoupdate.sh"
ALERT="${HOME}/.hermes/scripts/alertbot-send.sh"

log() { printf '%s\n' "$*"; }

ff_one() {
  local path="$1" ref="${2:-main}"
  [[ -d "$path/.git" || -f "$path/.git" ]] || { log "missing git: $path"; return 1; }
  timeout 45 git -C "$path" fetch --quiet origin
  local before behind ahead
  before=$(git -C "$path" rev-parse --short HEAD)
  behind=$(git -C "$path" rev-list --count "HEAD..origin/${ref}")
  ahead=$(git -C "$path" rev-list --count "origin/${ref}..HEAD")
  if [[ "$behind" == "0" ]]; then
    log "already current @ ${before}"
    return 0
  fi
  if [[ "$ahead" != "0" ]]; then
    log "DIVERGED ahead=${ahead} behind=${behind} @ ${before} — skipped (not a force merge)"
    return 2
  fi
  if git -C "$path" status --porcelain --untracked-files=no | grep -q .; then
    log "DIRTY behind=${behind} @ ${before} — skipped (local tracked changes)"
    git -C "$path" status --porcelain --untracked-files=no
    return 3
  fi
  git -C "$path" merge --ff-only "origin/${ref}"
  log "pulled ${before} -> $(git -C "$path" rev-parse --short HEAD) (+${behind})"
}

apply_hermes() {
  local before after
  [[ -x "$HERMES_BIN" ]] || { log "missing $HERMES_BIN"; return 1; }
  cd "$HERMES_AGENT"
  before=$(git rev-parse --short HEAD)
  log "hermes HEAD ${before}"
  timeout 45 git fetch --quiet origin
  local behind ahead
  behind=$(git rev-list --count HEAD..origin/main)
  ahead=$(git rev-list --count origin/main..HEAD)
  log "origin/main behind=${behind} ahead=${ahead}"
  if [[ "$behind" == "0" ]]; then
    log "already current @ ${before} — still running hermes update -y for dep/gateway check"
  elif [[ "$ahead" != "0" ]]; then
    log "diverged (local reapply commits). Resetting to origin/main; reapply scripts run after."
    git reset --hard origin/main
  fi
  PYTHONUNBUFFERED=1 "$HERMES_BIN" update -y
  after=$(git rev-parse --short HEAD)
  log "hermes now ${after}"
  if [[ -f "$INJECT" ]]; then
    python3 "$INJECT" --agent "$HERMES_AGENT" || {
      log "inject-media-wizard failed — trying reapply-media-wizard.sh"
      bash "$REAPPLY_MEDIA"
    }
  elif [[ -x "$REAPPLY_MEDIA" ]]; then
    bash "$REAPPLY_MEDIA"
  fi
  if [[ -x "$REAPPLY_DET" ]]; then
    bash "$REAPPLY_DET" --check || bash "$REAPPLY_DET"
  fi
  log "hermes update done ${before} -> $(git rev-parse --short HEAD)"
  if [[ -x "$ALERT" ]]; then
    "$ALERT" "✅ Hermes updated ${before} → $(git -C "$HERMES_AGENT" rev-parse --short HEAD) from Spark Console" >/dev/null 2>&1 || true
  fi
}

apply_ds4f() {
  bash "$DSPARK_FF"
  bash "$DSPARK_FF" --spark2
}

case "$KEY" in
  hermes) apply_hermes ;;
  ds4f) apply_ds4f ;;
  recipes) bash "$RECIPES" ;;
  git)
    # apply_update.sh git <path> <ref>
    ff_one "${2:?path}" "${3:-main}"
    ;;
  *)
    log "unknown key: ${KEY}"
    exit 1
    ;;
esac
