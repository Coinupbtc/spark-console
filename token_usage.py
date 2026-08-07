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

We return, per profile and as a grand total: input tokens, output tokens,
cache-read tokens, reasoning tokens, api calls, distinct sessions, plus a
"last 24h" window (sessions with first_seen in the last day).
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

HOME = Path.home()
HERMES_HOME = HOME / ".hermes"
ROOT_DB = HERMES_HOME / "state.db"
PROFILES_DIR = HERMES_HOME / "profiles"

# The "live gateway" profiles in the standard install.
PROFILE_NAMES = ["orchestrator", "dobby", "light", "smeagle", "freegle"]

_DAY = 24 * 3600


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


def _fmt_token_catalog() -> dict:
    """Full per-model catalog is heavy; just return counts. Kept light."""
    return {}


def token_summary() -> dict:
    now = time.time()
    since_24h = now - _DAY

    profiles: list[dict] = []
    for label, db in _db_list():
        total = _total(db)
        recent = _total(db, since=since_24h)
        profiles.append({
            "profile": label,
            "db": str(db),
            "total": total,
            "last_24h": recent,
        })

    def _sum(key):
        return sum(p["total"][key] for p in profiles)

    def _sum24(key):
        return sum(p["last_24h"][key] for p in profiles)

    return {
        "updated_unix": now,
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
    }


if __name__ == "__main__":
    import json
    print(json.dumps(token_summary(), indent=2, default=str))
