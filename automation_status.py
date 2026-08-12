#!/usr/bin/env python3
"""
Unified view of all THREE scheduling layers on this box, which is the thing
you otherwise need three commands to see:

  1. Hermes agent jobs   ~/.hermes/profiles/*/cron/jobs.json   (`hermes cron list`)
  2. OS crontab          crontab -l
  3. systemd user timers systemctl --user list-timers

Read from the source files/commands directly (no agent, no LLM) so the panel
costs milliseconds and cannot wedge the dashboard.
"""
from __future__ import annotations

import json
import re
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

HOME = Path.home()
HERMES_PROFILES = HOME / ".hermes/profiles"
LOG_DIR = HOME / "logs/cron"


def _run(cmd: list[str], timeout: int = 10) -> str:
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return out.stdout if out.returncode == 0 else ""
    except (subprocess.TimeoutExpired, OSError):
        return ""


def _ago(ts: float | None) -> str:
    if not ts:
        return "never"
    d = time.time() - ts
    if d < 0:
        return "just now"
    if d < 90:
        return f"{int(d)}s ago"
    if d < 5400:
        return f"{int(d / 60)}m ago"
    if d < 172800:
        return f"{d / 3600:.1f}h ago"
    return f"{int(d / 86400)}d ago"


def _in(ts: float | None) -> str:
    if not ts:
        return "—"
    d = ts - time.time()
    if d < 0:
        return "due"
    if d < 90:
        return f"in {int(d)}s"
    if d < 5400:
        return f"in {int(d / 60)}m"
    if d < 172800:
        return f"in {d / 3600:.1f}h"
    return f"in {int(d / 86400)}d"


# ------------------------------------------------------------------ cron parsing

def _field(spec: str, lo: int, hi: int) -> set[int]:
    """Expand one crontab field into the set of values it matches."""
    values: set[int] = set()
    for part in spec.split(","):
        step = 1
        if "/" in part:
            part, step_s = part.split("/", 1)
            step = int(step_s) if step_s.isdigit() else 1
        if part in ("*", ""):
            start, end = lo, hi
        elif "-" in part:
            a, b = part.split("-", 1)
            start, end = int(a), int(b)
        else:
            start = end = int(part)
        values.update(range(start, end + 1, step))
    return {v for v in values if lo <= v <= hi}


def next_cron_run(expr: str, now: datetime | None = None) -> float | None:
    """Next fire time (epoch) for a standard 5-field crontab expression.

    Minute-stepping with an early exit; typical schedules resolve in well under
    a thousand iterations. Returns None for expressions we do not parse
    (@reboot, 6-field, malformed) rather than guessing.
    """
    parts = expr.split()
    if len(parts) != 5:
        return None
    try:
        minutes = _field(parts[0], 0, 59)
        hours = _field(parts[1], 0, 23)
        doms = _field(parts[2], 1, 31)
        months = _field(parts[3], 1, 12)
        dows = {d % 7 for d in _field(parts[4], 0, 7)}
    except (ValueError, TypeError):
        return None
    if not (minutes and hours and doms and months and dows):
        return None
    dom_restricted = parts[2] != "*"
    dow_restricted = parts[4] != "*"

    cur = (now or datetime.now()).replace(second=0, microsecond=0) + timedelta(minutes=1)
    for _ in range(367 * 24 * 60):
        if cur.month in months and cur.hour in hours and cur.minute in minutes:
            dom_ok = cur.day in doms
            dow_ok = (cur.weekday() + 1) % 7 in dows
            # crontab semantics: with both restricted it is an OR, not an AND
            if (dom_ok and dow_ok) if not (dom_restricted and dow_restricted) else (dom_ok or dow_ok):
                return cur.timestamp()
        cur += timedelta(minutes=1)
    return None


_REDIRECT = re.compile(r">>?\s*(\S+\.log)")


def _log_mtime(path: str) -> float | None:
    try:
        return Path(path).stat().st_mtime
    except OSError:
        return None


# ------------------------------------------------------------------ layer 1: Hermes

def hermes_jobs() -> list[dict]:
    jobs: list[dict] = []
    for jobs_file in sorted(HERMES_PROFILES.glob("*/cron/jobs.json")):
        profile = jobs_file.parent.parent.name
        try:
            data = json.loads(jobs_file.read_text())
        except (OSError, ValueError):
            continue
        for job in data.get("jobs", []):
            next_ts = last_ts = None
            for key, target in (("next_run_at", "next"), ("last_run_at", "last")):
                raw = job.get(key)
                if not raw:
                    continue
                try:
                    ts = datetime.fromisoformat(raw).timestamp()
                except (ValueError, TypeError):
                    continue
                if target == "next":
                    next_ts = ts
                else:
                    last_ts = ts
            status = (job.get("last_status") or "unknown").lower()
            enabled = job.get("enabled", True)
            if not enabled:
                state = "paused"
            elif status in ("ok", "success", "completed"):
                state = "ok"
            elif status in ("error", "failed", "fail"):
                state = "fail"
            else:
                state = "unknown"
            jobs.append({
                "layer": "hermes",
                "id": job.get("id", "")[:12],
                "name": job.get("name", "(unnamed)"),
                "where": profile,
                "schedule": job.get("schedule_display")
                            or (job.get("schedule") or {}).get("display") or "?",
                "next_ts": next_ts, "next_in": _in(next_ts),
                "last_ts": last_ts, "last_ago": _ago(last_ts),
                "state": state,
                "detail": ("no-agent script" if job.get("no_agent") else "agent")
                          + (f" · {job['script']}" if job.get("script") else "")
                          + (f" → {job['deliver']}" if job.get("deliver") else ""),
                "error": (job.get("last_error") or job.get("last_delivery_error") or "")[:160],
            })
    jobs.sort(key=lambda j: (j["next_ts"] is None, j["next_ts"] or 0))
    return jobs


# ------------------------------------------------------------------ layer 2: OS crontab

def os_cron_jobs() -> list[dict]:
    out = _run(["crontab", "-l"])
    jobs: list[dict] = []
    for line in out.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" in line.split()[0]:
            continue
        parts = line.split(None, 5)
        if len(parts) < 6:
            continue
        expr = " ".join(parts[:5])
        command = parts[5]
        next_ts = next_cron_run(expr)
        m = _REDIRECT.search(command)
        last_ts = _log_mtime(m.group(1)) if m else None
        name = command
        for token in command.split():
            if "/" in token and not token.startswith(("-", ">")):
                name = token.rstrip(";").split("/")[-1]
                break
        jobs.append({
            "layer": "crontab",
            "id": "",
            "name": name,
            "where": "OS cron",
            "schedule": expr,
            "next_ts": next_ts, "next_in": _in(next_ts),
            "last_ts": last_ts, "last_ago": _ago(last_ts) if last_ts else "no log",
            "state": "ok" if last_ts else "unknown",
            "detail": command[:150],
            "error": "",
        })
    jobs.sort(key=lambda j: (j["next_ts"] is None, j["next_ts"] or 0))
    return jobs


# ------------------------------------------------------------------ layer 3: systemd timers

def _unit_cleanly_inactive(unit: str) -> bool:
    """True when unit is stopped by owner (inactive), not crashed (failed)."""
    show = _run(["systemctl", "--user", "show", unit,
                 "-p", "ActiveState", "-p", "Result", "--value"])
    vals = [v.strip() for v in show.splitlines() if v.strip()]
    active = (vals[0] if vals else "").lower()
    return active in ("inactive", "dead")


def _optional_app_parked(job_name: str) -> bool:
    """Apps the owner parks from Spark Console must not red-light Jobs.

    BetIntel timers keep firing while backend is off; that is intentional
    parking, not a stack failure. Mirror of server.py optional_noise.
    """
    name = (job_name or "").lower()
    if name.startswith("betintel-") or name.startswith("betintel."):
        return _unit_cleanly_inactive("betintel-backend.service")
    return False


def systemd_timers() -> list[dict]:
    raw = _run(["systemctl", "--user", "list-timers", "--all", "--output=json", "--no-pager"])
    try:
        rows = json.loads(raw) if raw.strip() else []
    except ValueError:
        rows = []
    # Probe both the .timer and the activated .service — masked vendor
    # units (e.g. launchpadlib-cache-clean) still appear in list-timers --all
    # with no next/last and look like "paused" problems on Spark Console.
    probe_ids = []
    for r in rows:
        if r.get("unit"):
            probe_ids.append(r["unit"])
        if r.get("activates"):
            probe_ids.append(r["activates"])
    results: dict[str, dict] = {}
    if probe_ids:
        show = _run(["systemctl", "--user", "show", *probe_ids,
                     "-p", "Id", "-p", "Result", "-p", "ActiveState",
                     "-p", "ExecMainStatus", "-p", "UnitFileState", "-p", "LoadState"])
        block: dict[str, str] = {}
        for line in show.splitlines():
            if not line.strip():
                if block.get("Id"):
                    results[block["Id"]] = dict(block)
                block = {}
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                block[k] = v
        if block.get("Id"):
            results[block["Id"]] = dict(block)

    jobs: list[dict] = []
    for r in rows:
        unit = r.get("unit", "")
        activates = r.get("activates") or ""
        # Intentionally killed (mask → /dev/null): omit from console inventory
        timer_info = results.get(unit, {})
        svc_info = results.get(activates, {})
        if (timer_info.get("UnitFileState") or "").lower() == "masked" or \
           (timer_info.get("LoadState") or "").lower() == "masked" or \
           (svc_info.get("UnitFileState") or "").lower() == "masked" or \
           (svc_info.get("LoadState") or "").lower() == "masked":
            continue
        next_us, last_us = r.get("next"), r.get("last")
        next_ts = next_us / 1e6 if next_us else None
        last_ts = last_us / 1e6 if last_us else None
        info = svc_info or timer_info
        result = (info.get("Result") or "").lower()
        active = (info.get("ActiveState") or "").lower()
        name = unit.replace(".timer", "")
        if result in ("", "success"):
            state = "running" if active == "activating" else "ok"
        else:
            state = "fail"
        if next_ts is None and last_ts is None:
            state = "paused"
        # Owner parked the app → show paused, not fail (Jobs badge / Problems)
        if state == "fail" and _optional_app_parked(name):
            state = "paused"
            result = "parked"
        jobs.append({
            "layer": "timer",
            "id": "",
            "name": name,
            "where": "systemd --user",
            "schedule": "timer",
            "next_ts": next_ts, "next_in": _in(next_ts),
            "last_ts": last_ts, "last_ago": _ago(last_ts),
            "state": state,
            "detail": activates or unit,
            "error": "" if state != "fail" else f"last result: {result}",
        })
    jobs.sort(key=lambda j: (j["next_ts"] is None, j["next_ts"] or 0))
    return jobs


def failed_units() -> list[str]:
    """User units currently in failed state, skipping masked/static junk.

    Masked units (e.g. GNOME's update-notifier-crash) can sit in `failed`
    forever after one bad boot attempt — they are not actionable stack failures
    and must not page the Needs attention panel as critical.
    """
    out = _run(["systemctl", "--user", "list-units", "--state=failed",
                "--no-legend", "--plain", "--no-pager"])
    names = [line.split()[0] for line in out.splitlines() if line.strip()]
    actionable = []
    for name in names:
        show = _run(["systemctl", "--user", "show", name,
                     "-p", "LoadState", "-p", "UnitFileState", "--value"])
        vals = [v.strip() for v in show.splitlines() if v.strip()]
        # show --value prints LoadState then UnitFileState
        load = vals[0] if vals else ""
        ufs = vals[1] if len(vals) > 1 else ""
        if load == "masked" or ufs == "masked":
            continue
        # Same parked-app rule as timers — don't inventory owner-stop noise
        if _optional_app_parked(name.replace(".service", "").replace(".timer", "")):
            continue
        # Desktop portal timeouts are normal on this box (no full GNOME session
        # for portals). They sticky-fail and paint Needs attention red without
        # being stack-actionable — same class as masked update-notifier junk.
        base = name.rsplit(".", 1)[0].lower()
        if base.startswith("xdg-desktop-portal"):
            continue
        actionable.append(name)
    return actionable


def query_automation() -> dict:
    hermes = hermes_jobs()
    cron = os_cron_jobs()
    timers = systemd_timers()
    all_jobs = hermes + cron + timers
    failing = [j for j in all_jobs if j["state"] == "fail"]
    # A paused job keeps its stale next_run_at and would otherwise sit at the top
    # of "next up" reading DUE forever — it is never going to fire.
    upcoming = sorted((j for j in all_jobs if j["next_ts"] and j["state"] != "paused"),
                      key=lambda j: j["next_ts"])[:10]
    return {
        "iso_ts": datetime.now(timezone.utc).isoformat(),
        "jobs": all_jobs,
        "counts": {
            "hermes": len(hermes), "crontab": len(cron), "timer": len(timers),
            "total": len(all_jobs), "failing": len(failing),
            "paused": sum(1 for j in all_jobs if j["state"] == "paused"),
        },
        "failing": failing,
        "upcoming": upcoming,
        "failed_units": failed_units(),
    }


if __name__ == "__main__":
    d = query_automation()
    print(json.dumps(d["counts"], indent=2))
    for j in d["upcoming"]:
        print(f"  {j['next_in']:>10}  [{j['layer']:8}] {j['name'][:44]:44} {j['state']}")
    if d["failing"]:
        print("\nFAILING:")
        for j in d["failing"]:
            print(f"  [{j['layer']}] {j['name']} — {j['error']}")
    if d["failed_units"]:
        print("failed units:", d["failed_units"])
