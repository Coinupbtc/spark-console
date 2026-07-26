#!/usr/bin/env python3
"""Best-effort 'last real use' signals for Spark Console services.

Health probes (GET /v1/models, etc.) are excluded so the console's own
polling does not look like user traffic.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

HOME = Path.home()

# Only mutation / generation traffic counts as "use". GET is almost always
# health probes (this console), frontend polls, or static assets — counting
# GET made every service look "just now" while the dashboard was open.
_USE_RE = re.compile(r'"(POST|PUT|DELETE|PATCH) ([^"\s]+)', re.I)


def _ago_label(age_s: float | None) -> str:
    if age_s is None:
        return "unknown"
    if age_s < 90:
        return "just now"
    if age_s < 3600:
        return f"{int(age_s // 60)}m ago"
    if age_s < 86400:
        h = age_s / 3600
        return f"{h:.1f}h ago" if h < 10 else f"{int(h)}h ago"
    return f"{age_s / 86400:.1f}d ago"


def _idle_level(age_s: float | None, active: bool, critical: bool) -> str:
    """ok | warm | idle | waste | off | unknown"""
    if not active:
        return "off"
    if age_s is None:
        return "unknown"
    # Heavy residents: flag sooner
    waste_h = 12 if critical else 6
    idle_h = 4 if critical else 2
    if age_s >= waste_h * 3600:
        return "waste"
    if age_s >= idle_h * 3600:
        return "idle"
    if age_s >= 30 * 60:
        return "warm"
    return "ok"


def _pack(epoch: float | None, source: str, active: bool, critical: bool = False) -> dict:
    now = time.time()
    age = (now - epoch) if epoch else None
    return {
        "last_used_epoch": epoch,
        "last_used_iso": (
            datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat() if epoch else None
        ),
        "last_used_ago": _ago_label(age),
        "last_used_source": source,
        "idle": _idle_level(age, active, critical),
        "idle_seconds": int(age) if age is not None else None,
    }


def _mtime_newest(paths: list[Path], pattern: str | None = None) -> float | None:
    best: float | None = None
    for p in paths:
        try:
            if not p.exists():
                continue
            if p.is_file():
                m = p.stat().st_mtime
                best = m if best is None else max(best, m)
                continue
            if p.is_dir():
                # shallow + one level deep (fast)
                for root, dirs, files in os.walk(p):
                    depth = Path(root).relative_to(p).parts
                    if len(depth) > 2:
                        dirs.clear()
                        continue
                    for fn in files:
                        if pattern and pattern not in fn:
                            continue
                        try:
                            m = (Path(root) / fn).stat().st_mtime
                            best = m if best is None else max(best, m)
                        except OSError:
                            pass
                    break  # only top-level for large trees unless pattern
        except OSError:
            continue
    return best


def _mtime_shallow(dir_path: Path, max_files: int = 200) -> float | None:
    if not dir_path.is_dir():
        return None
    best = None
    n = 0
    try:
        with os.scandir(dir_path) as it:
            for ent in it:
                n += 1
                if n > max_files:
                    break
                try:
                    m = ent.stat().st_mtime
                    best = m if best is None else max(best, m)
                except OSError:
                    pass
    except OSError:
        return None
    return best


def _parse_unix_or_float(text: str) -> float | None:
    text = (text or "").strip()
    if not text:
        return None
    try:
        v = float(text.split()[0])
        # reject tiny numbers
        if v > 1_000_000_000:
            return v
    except ValueError:
        pass
    return None


def _journal_last_access(unit: str, since: str = "14 days ago") -> float | None:
    """Last non-health HTTP access line from a user unit journal."""
    try:
        out = subprocess.run(
            [
                "journalctl", "--user", "-u", unit,
                "--since", since, "-n", "80", "--no-pager",
                "-o", "short-iso",
            ],
            capture_output=True, text=True, timeout=8,
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    best = None
    for line in reversed((out.stdout or "").splitlines()):
        if not _USE_RE.search(line):
            continue
        try:
            m2 = re.match(
                r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2})", line
            )
            if m2:
                dt = datetime.fromisoformat(m2.group(1))
            else:
                m = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", line)
                if not m:
                    continue
                dt = datetime.fromisoformat(m.group(1)).replace(tzinfo=timezone.utc)
            best = dt.timestamp()
            break
        except ValueError:
            continue
    return best


def _unit_active_enter(unit: str) -> float | None:
    try:
        out = subprocess.run(
            ["systemctl", "--user", "show", unit, "-p", "ActiveEnterTimestamp", "--value"],
            capture_output=True, text=True, timeout=5,
        )
        raw = (out.stdout or "").strip()
        if not raw or raw == "n/a":
            return None
        # e.g. Mon 2026-07-20 16:40:48 CDT
        # fall back: use Invoked path via show ActiveEnterTimestampMonotonic — skip
        # parse with date
        p = subprocess.run(
            ["date", "-d", raw, "+%s"],
            capture_output=True, text=True, timeout=3,
        )
        if p.returncode == 0:
            return float(p.stdout.strip())
    except Exception:
        pass
    return None


def probe_last_used(service_id: str, meta: dict, active: bool) -> dict:
    """Return last_used fields for a service catalog entry."""
    critical = bool(meta.get("critical"))
    kind = meta.get("kind")
    sources: list[tuple[float, str]] = []

    # Explicit touch file (optional future / manual)
    touch = DATA_TOUCH / f"{service_id}.touch"
    if touch.is_file():
        try:
            sources.append((touch.stat().st_mtime, "touch"))
        except OSError:
            pass

    # Catalog-driven hints
    for path in meta.get("activity_files") or []:
        p = Path(path).expanduser()
        if p.is_file():
            try:
                if p.name == "last-activity":
                    ep = _parse_unix_or_float(p.read_text(errors="replace"))
                    if ep:
                        sources.append((ep, "last-activity file"))
                        continue
                sources.append((p.stat().st_mtime, f"file:{p.name}"))
            except OSError:
                pass

    for d in meta.get("activity_dirs") or []:
        p = Path(d).expanduser()
        m = _mtime_shallow(p)
        if m:
            sources.append((m, f"dir:{p.name}"))

    unit = meta.get("unit")
    if unit and meta.get("activity_journal", True):
        j = _journal_last_access(unit)
        if j:
            sources.append((j, "http access log"))

    if kind == "node2-deep":
        # Do NOT use status.json — our own status polls rewrite it every refresh.
        for p in (
            Path("/tmp/node2-deep-lane-llama.log"),
            HOME / ".local/state/hermes/node2-deep-lane/active-model",
            HOME / ".local/state/hermes/node2-deep-lane/desired",
        ):
            if p.is_file():
                try:
                    sources.append((p.stat().st_mtime, p.name))
                except OSError:
                    pass

    if service_id == "bakeoff-ui":
        st = HOME / ".local/state/hermes/bakeoff"
        for name in ("progress.json", "history.jsonl", "latest.json"):
            p = st / name
            if p.is_file():
                try:
                    sources.append((p.stat().st_mtime, name))
                except OSError:
                    pass
        runs = st / "runs"
        m = _mtime_shallow(runs) if runs.is_dir() else None
        if m:
            sources.append((m, "bakeoff runs"))

    if service_id.startswith("hermes-"):
        # Telegram/Kanban do NOT reliably update sessions/*.json dumps.
        # Live work lands in state.db-wal, agent.log, and kanban assignee pings.
        profile = service_id.replace("hermes-", "")
        # legacy rename: coder → dobby
        aliases = {profile}
        if profile == "dobby":
            aliases.add("coder")
        base = HOME / f".hermes/profiles/{profile}"
        for rel, label in (
            ("logs/agent.log", "agent.log"),
            ("state.db-wal", "state.db-wal"),
            ("state.db", "state.db"),
            ("auth.json", "auth.json"),
            # gateway.heartbeat = process alive, not user work — skip
        ):
            p = base / rel
            if p.is_file():
                try:
                    sources.append((p.stat().st_mtime, label))
                except OSError:
                    pass
        # Kanban: progress pings keyed by assignee profile name
        ping = HOME / ".local/state/hermes/kanban-progress-ping.json"
        if ping.is_file():
            try:
                data = json.loads(ping.read_text())
                best_ping = None
                for _tid, row in (data or {}).items():
                    if not isinstance(row, dict):
                        continue
                    who = str(row.get("assignee") or "").lower()
                    if who in aliases or who == profile:
                        lp = row.get("last_ping")
                        if isinstance(lp, (int, float)) and lp > 1_000_000_000:
                            best_ping = lp if best_ping is None else max(best_ping, float(lp))
                if best_ping:
                    sources.append((float(best_ping), "kanban ping"))
            except Exception:
                pass
        # Kanban task logs (mtime of logs while worker is assigned is imperfect;
        # still useful when pings missing). Prefer logs modified in last 2d only
        # if assignee appears in the log tail — skip full scan; use ping+agent.
        klogs = HOME / ".hermes/kanban/logs"
        if klogs.is_dir() and ping.is_file():
            try:
                data = json.loads(ping.read_text())
                for tid, row in (data or {}).items():
                    if not isinstance(row, dict):
                        continue
                    who = str(row.get("assignee") or "").lower()
                    if who not in aliases and who != profile:
                        continue
                    lp = klogs / f"{tid}.log"
                    if lp.is_file():
                        sources.append((lp.stat().st_mtime, f"kanban:{tid}"))
            except Exception:
                pass

    if service_id == "llama-miaai35":
        # Prefer real completions over health probes: journal lines with chat/completions
        try:
            out = subprocess.run(
                [
                    "journalctl", "--user", "-u", "llama-miaai35.service",
                    "--since", "14 days ago", "-n", "100", "--no-pager", "-o", "short-iso",
                ],
                capture_output=True, text=True, timeout=8,
            )
            for line in reversed((out.stdout or "").splitlines()):
                if any(x in line for x in (
                    "chat/completions", "completions", "slot update",
                    "prompt eval", "sampling", "request:",
                )):
                    m2 = re.match(
                        r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2})", line
                    )
                    if m2:
                        sources.append(
                            (datetime.fromisoformat(m2.group(1)).timestamp(), "inference req")
                        )
                        break
        except Exception:
            pass

    if not sources and unit and active:
        # Fall back: when the unit last entered active (weak signal)
        ent = _unit_active_enter(unit)
        if ent:
            sources.append((ent, "unit start (no traffic seen)"))

    if not sources:
        return _pack(None, "none", active, critical)

    sources.sort(key=lambda x: x[0], reverse=True)
    epoch, src = sources[0]
    return _pack(epoch, src, active, critical)


DATA_TOUCH = Path(__file__).resolve().parent / "data" / "last_used"
DATA_TOUCH.mkdir(parents=True, exist_ok=True)


def touch_used(service_id: str) -> None:
    p = DATA_TOUCH / f"{service_id}.touch"
    p.write_text(str(time.time()))
