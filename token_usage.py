#!/usr/bin/env python3
"""
Token Usage panel — how many LLM tokens every Hermes profile has consumed.

Reads the per-profile Hermes `state.db` `session_model_usage` tables directly
(no agent, no LLM) so this panel costs a few ms and cannot wedge the console.
Every gateway profile writes rows there per session+model: input/output tokens,
cache reads/writes, reasoning tokens, api_call_count, and per-call cost.

Data source:
  ~/.hermes/state.db                                  (root/default profile)
  ~/.hermes/profiles/<name>/state.db                  (orchestrator, dobby,
                                                       light, smeagle, freegle)

Returns:
  * grand totals + last-24h (existing)
  * per-profile breakdown (existing)
  * series_14d — daily input/output/calls across all profiles (for the graph)
  * stats — mean / median / mode of tokens-per-session and tokens-per-call
    (mode uses rounded buckets so continuous token counts still have a mode)
"""
from __future__ import annotations

import math
import sqlite3
import statistics
import time
from collections import Counter
from pathlib import Path

HOME = Path.home()
HERMES_HOME = HOME / ".hermes"
ROOT_DB = HERMES_HOME / "state.db"
PROFILES_DIR = HERMES_HOME / "profiles"

# The "live gateway" profiles in the standard install.
PROFILE_NAMES = ["orchestrator", "dobby", "light", "smeagle", "freegle"]

_DAY = 24 * 3600
_SERIES_DAYS = 14
# Mode on raw token counts is almost always unique; bucket so "mode" is useful.
_SESSION_MODE_BUCKET = 10_000   # tokens (in+out) per session
_CALL_MODE_BUCKET = 1_000       # tokens (in+out) per API call


def _db_list() -> list[tuple[str, Path]]:
    """[(label, db_path)] — root db plus every profile db that exists."""
    dbs: list[tuple[str, Path]] = []
    if ROOT_DB.exists():
        dbs.append(("root", ROOT_DB))
    if PROFILES_DIR.is_dir():
        for p in sorted(PROFILES_DIR.iterdir()):
            if p.is_dir() and (p / "state.db").exists():
                dbs.append((p.name, p / "state.db"))
    return dbs


def _total(db: Path, since: float | None = None) -> dict:
    """Aggregate one state.db's session_model_usage."""
    agg = {
        "input_tokens": 0, "output_tokens": 0, "cache_read_tokens": 0,
        "cache_write_tokens": 0, "reasoning_tokens": 0,
        "api_calls": 0, "sessions": 0,
    }
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        cur = con.cursor()
        if since:
            cur.execute(
                "SELECT COUNT(DISTINCT session_id), "
                "COALESCE(SUM(api_call_count),0), COALESCE(SUM(input_tokens),0), "
                "COALESCE(SUM(output_tokens),0), COALESCE(SUM(cache_read_tokens),0), "
                "COALESCE(SUM(cache_write_tokens),0), COALESCE(SUM(reasoning_tokens),0) "
                "FROM session_model_usage WHERE first_seen >= ?",
                (since,),
            )
        else:
            cur.execute(
                "SELECT COUNT(DISTINCT session_id), "
                "COALESCE(SUM(api_call_count),0), COALESCE(SUM(input_tokens),0), "
                "COALESCE(SUM(output_tokens),0), COALESCE(SUM(cache_read_tokens),0), "
                "COALESCE(SUM(cache_write_tokens),0), COALESCE(SUM(reasoning_tokens),0) "
                "FROM session_model_usage"
            )
        row = cur.fetchone()
        con.close()
        if row:
            agg["sessions"] = row[0] or 0
            agg["api_calls"] = row[1] or 0
            agg["input_tokens"] = row[2] or 0
            agg["output_tokens"] = row[3] or 0
            agg["cache_read_tokens"] = row[4] or 0
            agg["cache_write_tokens"] = row[5] or 0
            agg["reasoning_tokens"] = row[6] or 0
    except Exception:
        pass
    return agg


def _session_rows(db: Path) -> list[tuple[float, int, int]]:
    """Per-session (in+out tokens, api_calls) for stats. Skips empty sessions."""
    out: list[tuple[float, int, int]] = []
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        cur = con.cursor()
        cur.execute(
            "SELECT session_id, "
            "COALESCE(SUM(input_tokens),0)+COALESCE(SUM(output_tokens),0), "
            "COALESCE(SUM(api_call_count),0) "
            "FROM session_model_usage "
            "GROUP BY session_id"
        )
        for _sid, toks, calls in cur.fetchall():
            toks_i = int(toks or 0)
            calls_i = int(calls or 0)
            if toks_i > 0 or calls_i > 0:
                # first element unused by callers today; keep shape extensible
                out.append((0.0, toks_i, calls_i))
        con.close()
    except Exception:
        pass
    return out


def _daily_rows(db: Path, since: float) -> list[tuple[str, int, int, int, int]]:
    """[(day_iso, input, output, calls, session_rows)] from one db since `since`."""
    rows: list[tuple[str, int, int, int, int]] = []
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        cur = con.cursor()
        # Group by UTC calendar day of first_seen (Hermes stores unix floats).
        cur.execute(
            "SELECT date(first_seen, 'unixepoch') AS d, "
            "COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0), "
            "COALESCE(SUM(api_call_count),0), COUNT(DISTINCT session_id) "
            "FROM session_model_usage "
            "WHERE first_seen >= ? AND first_seen IS NOT NULL "
            "GROUP BY d ORDER BY d",
            (since,),
        )
        for d, inn, out, calls, sess in cur.fetchall():
            if not d:
                continue
            rows.append((str(d), int(inn or 0), int(out or 0),
                         int(calls or 0), int(sess or 0)))
        con.close()
    except Exception:
        pass
    return rows


def _median(vals: list[float]) -> float | None:
    if not vals:
        return None
    return float(statistics.median(vals))


def _mean(vals: list[float]) -> float | None:
    if not vals:
        return None
    return float(statistics.fmean(vals))


def _mode_bucketed(vals: list[float], bucket: int) -> dict:
    """Most common bucket center. Returns {value, count, bucket} or empty."""
    if not vals or bucket <= 0:
        return {"value": None, "count": 0, "bucket": bucket}
    # Round each value to nearest bucket center (0, bucket, 2*bucket, …).
    centers = [int(round(v / bucket) * bucket) for v in vals]
    counts = Counter(centers)
    # Prefer the most frequent; ties → smallest center (deterministic).
    best_val, best_n = min(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return {"value": int(best_val), "count": int(best_n), "bucket": bucket}


def _p90(vals: list[float]) -> float | None:
    if not vals:
        return None
    s = sorted(vals)
    # nearest-rank
    idx = min(len(s) - 1, max(0, math.ceil(0.9 * len(s)) - 1))
    return float(s[idx])


def _build_stats(session_rows: list[tuple[float, int, int]]) -> dict:
    """Mean/median/mode (+n, p90) for tokens/session and tokens/call."""
    sess_toks = [float(t) for _, t, _ in session_rows if t > 0]
    # tokens per call: only sessions with at least one API call
    call_toks: list[float] = []
    for _, t, c in session_rows:
        if c and c > 0 and t > 0:
            call_toks.append(float(t) / float(c))

    def pack(vals: list[float], bucket: int) -> dict:
        mode = _mode_bucketed(vals, bucket)
        return {
            "n": len(vals),
            "mean": _round_or_none(_mean(vals)),
            "median": _round_or_none(_median(vals)),
            "mode": mode["value"],
            "mode_count": mode["count"],
            "mode_bucket": mode["bucket"],
            "p90": _round_or_none(_p90(vals)),
            "min": _round_or_none(min(vals) if vals else None),
            "max": _round_or_none(max(vals) if vals else None),
        }

    return {
        "unit": "tokens (input+output)",
        "per_session": pack(sess_toks, _SESSION_MODE_BUCKET),
        "per_call": pack(call_toks, _CALL_MODE_BUCKET),
    }


def _round_or_none(v) -> float | None:
    if v is None:
        return None
    try:
        return round(float(v), 1)
    except (TypeError, ValueError):
        return None


def _build_series(days: int = _SERIES_DAYS) -> list[dict]:
    """Daily totals across all profile DBs for the last `days` calendar days."""
    now = time.time()
    since = now - days * _DAY
    # Accumulate by day iso
    by_day: dict[str, dict] = {}
    for _label, db in _db_list():
        for d, inn, out, calls, sess in _daily_rows(db, since):
            slot = by_day.setdefault(d, {
                "day": d, "input_tokens": 0, "output_tokens": 0,
                "api_calls": 0, "sessions": 0,
            })
            slot["input_tokens"] += inn
            slot["output_tokens"] += out
            slot["api_calls"] += calls
            slot["sessions"] += sess

    # Fill missing days so the chart has a continuous X axis
    out: list[dict] = []
    # Build last N UTC midnights via local date math from unix
    for i in range(days - 1, -1, -1):
        t = now - i * _DAY
        day = time.strftime("%Y-%m-%d", time.gmtime(t))
        slot = by_day.get(day) or {
            "day": day, "input_tokens": 0, "output_tokens": 0,
            "api_calls": 0, "sessions": 0,
        }
        out.append(slot)
    return out


_SUMMARY_CACHE: dict = {"ts": 0.0, "payload": None}
_SUMMARY_TTL = 20.0  # console Jobs tab polls often; SQLite scan is light but avoid stampede


def token_summary() -> dict:
    now = time.time()
    cached = _SUMMARY_CACHE.get("payload")
    if cached and (now - float(_SUMMARY_CACHE.get("ts") or 0)) < _SUMMARY_TTL:
        out = dict(cached)
        out["cached"] = True
        return out

    since_24h = now - _DAY

    profiles: list[dict] = []
    session_rows: list[tuple[float, int, int]] = []
    for label, db in _db_list():
        total = _total(db)
        recent = _total(db, since=since_24h)
        profiles.append({
            "profile": label,
            "db": str(db),
            "total": total,
            "last_24h": recent,
        })
        session_rows.extend(_session_rows(db))

    def _sum(key):
        return sum(p["total"][key] for p in profiles)

    def _sum24(key):
        return sum(p["last_24h"][key] for p in profiles)

    series = _build_series(_SERIES_DAYS)
    stats = _build_stats(session_rows)

    payload = {
        "updated_unix": now,
        "cached": False,
        "profiles": profiles,
        "counts": {
            "input_tokens": _sum("input_tokens"),
            "output_tokens": _sum("output_tokens"),
            "cache_read_tokens": _sum("cache_read_tokens"),
            "cache_write_tokens": _sum("cache_write_tokens"),
            "reasoning_tokens": _sum("reasoning_tokens"),
            "api_calls": _sum("api_calls"),
            "sessions": _sum("sessions"),
        },
        "last_24h": {
            "input_tokens": _sum24("input_tokens"),
            "output_tokens": _sum24("output_tokens"),
            "cache_read_tokens": _sum24("cache_read_tokens"),
            "reasoning_tokens": _sum24("reasoning_tokens"),
            "api_calls": _sum24("api_calls"),
            "sessions": _sum24("sessions"),
        },
        "series_14d": series,
        "stats": stats,
    }
    _SUMMARY_CACHE["ts"] = now
    _SUMMARY_CACHE["payload"] = payload
    return dict(payload)


if __name__ == "__main__":
    import json
    print(json.dumps(token_summary(), indent=2, default=str))
