#!/usr/bin/env python3
"""
Backup freshness across every tier, read from the artifacts the backup scripts
already leave behind (cron logs + remote marker files). No restic/ssh calls of
its own, so the panel is instant and cannot hang the dashboard.

Tiers:
  1  restic → node2 sftp        backup-restic-spark.sh   (encrypted, CRITICAL)
  2  Hermes state.db snapshots  backup-state-db.sh
  3  Start9 rsync (3.7 TB)      backup-to-start9.sh      + remote marker file
  4  Gitea code push            gitea-push-all-projects.sh
  5  Pi mirror                  read from the Pi poller
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

HOME = Path.home()
LOG_DIR = HOME / "logs/cron"

# script → (label, tier, detail, expected cadence in hours, success regex, fail regex)
JOBS = [
    ("backup-restic-spark.sh", "Restic → node2", 1,
     "Encrypted sftp:spark2 restic repo", 30,
     r"✅ Encrypted backup complete", r"(❌|ERROR|failed with exit code [1-9])"),
    ("backup-state-db.sh", "Hermes state.db", 2,
     "Per-profile agent DB snapshots", 30,
     r"state\.db backup complete", r"(❌|ERROR)"),
    ("backup-to-start9.sh", "Start9 rsync", 3,
     "Tier-3 bulk → package-data 3.7 TB", 30,
     r"Done\. ok=\d+ fail=0", r"fail=[1-9]"),
    ("gitea-push-all-projects.sh", "Gitea push", 4,
     "Code repos → Start9 Gitea", 30,
     r"(✅|pushed|up to date)", r"(❌|error:|rejected)"),
]


def _ago(ts: float | None) -> str:
    if not ts:
        return "never"
    d = time.time() - ts
    if d < 5400:
        return f"{int(d / 60)}m ago"
    if d < 172800:
        return f"{d / 3600:.1f}h ago"
    return f"{int(d / 86400)}d ago"


def _newest_log(script: str) -> Path | None:
    logs = sorted(LOG_DIR.glob(f"{script}.*.log"), key=lambda p: p.stat().st_mtime)
    return logs[-1] if logs else None


def _tail(path: Path, n: int = 262144) -> str:
    """Last N bytes of a log.

    Rsync jobs print huge path lists (and optional dry-runs append after a real
    Done.), so a 4KB tail used to miss `Done. ok=` and false-warn every morning.
    """
    try:
        with path.open("rb") as f:
            f.seek(0, 2)
            f.seek(max(0, f.tell() - n))
            return f.read().decode("utf-8", "replace")
    except OSError:
        return ""


def _latest_match(text: str, pattern: str) -> re.Match | None:
    """Rightmost regex match — the newest signal in an appending daily log."""
    last = None
    for m in re.finditer(pattern, text):
        last = m
    return last


def query_backups(pi: dict | None = None, start9: dict | None = None) -> dict:
    now = time.time()
    entries: list[dict] = []

    for script, label, tier, detail, cadence_h, ok_re, fail_re in JOBS:
        log = _newest_log(script)
        entry = {"id": script, "label": label, "tier": tier, "detail": detail,
                 "target": "local log", "cadence_h": cadence_h}
        if not log:
            entry.update({"state": "unknown", "ago": "no log found", "age_h": None,
                          "note": f"no {script} log in ~/logs/cron"})
            entries.append(entry)
            continue
        mtime = log.stat().st_mtime
        text = _tail(log)
        age_h = (now - mtime) / 3600
        ok_m = _latest_match(text, ok_re)
        fail_m = _latest_match(text, fail_re)
        # backup-to-start9 can append a multi-MB dry-run after Done — deepen scan
        if not ok_m:
            deep = _tail(log, 3_000_000)
            ok_m = _latest_match(deep, ok_re)
            fail_m = _latest_match(deep, fail_re) or fail_m
            if ok_m:
                text = deep
        succeeded = bool(ok_m)
        # A fail after the last success wins; a fail before it is stale noise.
        failed = bool(fail_m) and (not ok_m or fail_m.start() > ok_m.start())
        skipped = "already running, exiting" in text
        if failed:
            state, note = "fail", "last run reported an error"
        elif not succeeded:
            state, note = "warn", "no success marker in last log"
        elif age_h > cadence_h:
            state, note = "warn", f"stale — expected every {cadence_h}h"
        else:
            state, note = "ok", ""
            # Dry-run / second pass after Done can leave a huge unfinished tail —
            # that is noise, not a failed backup, when Done already landed.
            if "Dry run: true" in text:
                note = "ok · later dry-run in same log"
        if skipped and succeeded and not failed:
            note = (note + " · a later run exited on flock (already running)").strip(" ·")
        snaps = re.search(r"(\d+) snapshots", text)
        if snaps:
            entry["extra"] = f"{snaps.group(1)} snapshots"
        size = re.search(r"total size is ([\d.]+[KMGT])", text)
        if size:
            entry["extra"] = f"{size.group(1)}B transferred set"
        entry.update({"state": state, "note": note, "age_h": round(age_h, 1),
                      "ago": _ago(mtime), "ts": mtime, "log": str(log)})
        entries.append(entry)

    # ---- remote confirmations: the copy that actually landed off-box
    if start9 and start9.get("reachable"):
        bk = start9.get("backup") or {}
        age_h = bk.get("age_h")
        entries.append({
            "id": "start9-marker", "label": "Start9 marker", "tier": 3,
            "detail": bk.get("label", "/mnt/backup/hermes/last-backup.txt"),
            "target": "cosmic-charcoal", "cadence_h": 30,
            "state": "ok" if age_h is not None and age_h <= 30 else "warn",
            "note": "" if age_h is not None and age_h <= 30 else "marker older than daily cadence",
            "age_h": age_h, "ago": bk.get("ago", "?"), "ts": bk.get("ts"),
        })
    elif start9:
        entries.append({"id": "start9-marker", "label": "Start9 marker", "tier": 3,
                        "detail": "host unreachable", "target": "cosmic-charcoal",
                        "state": "unknown", "note": start9.get("error", "")[:80],
                        "age_h": None, "ago": "?", "cadence_h": 30})

    if pi and pi.get("reachable"):
        mirror = pi.get("mirror") or {}
        age_h = mirror.get("age_h")
        entries.append({
            "id": "pi-mirror", "label": "Pi mirror", "tier": 5,
            "detail": f"{mirror.get('size_gb', '?')} GB · {mirror.get('files', '?')} files",
            "target": "raspberrypi", "cadence_h": 48,
            "state": "ok" if age_h is not None and age_h <= 48 else "warn",
            "note": "" if age_h is not None and age_h <= 48 else "mirror not refreshed in 48h",
            "age_h": age_h, "ago": mirror.get("newest_ago", "?"), "ts": mirror.get("newest_ts"),
        })
    elif pi:
        entries.append({"id": "pi-mirror", "label": "Pi mirror", "tier": 5,
                        "detail": "host unreachable", "target": "raspberrypi",
                        "state": "unknown", "note": pi.get("error", "")[:80],
                        "age_h": None, "ago": "?", "cadence_h": 48})

    entries.sort(key=lambda e: (e["tier"], e["label"]))
    bad = [e for e in entries if e["state"] == "fail"]
    warn = [e for e in entries if e["state"] == "warn"]
    freshest = min((e["age_h"] for e in entries if e.get("age_h") is not None), default=None)
    return {
        "iso_ts": datetime.now(timezone.utc).isoformat(),
        "entries": entries,
        "counts": {"total": len(entries), "ok": sum(1 for e in entries if e["state"] == "ok"),
                   "warn": len(warn), "fail": len(bad)},
        "newest_age_h": freshest,
        "issues": [{"level": "critical" if e["state"] == "fail" else "warning",
                    "message": f"backup {e['label']}: {e['note'] or e['state']} ({e['ago']})"}
                   for e in bad + warn],
    }


if __name__ == "__main__":
    import fleet_nodes
    data = query_backups(fleet_nodes.query_pi(), fleet_nodes.query_start9())
    print(json.dumps(data["counts"], indent=2))
    for e in data["entries"]:
        print(f"  t{e['tier']} {e['label']:16} {e['state']:8} {e['ago']:>10}  "
              f"{e.get('extra', '')} {e['note']}")
