#!/usr/bin/env python3
"""
Portable, read-only service board.

Describe whatever you want watched in `services.json` (see
services.example.json) and this reports each entry's systemd state and/or HTTP
reachability. Deliberately read-only: the public tree ships no start/stop path,
so there is nothing here for a hostile page to trigger.

Two design notes worth keeping:

* Unit states come from ONE `systemctl show` call for the whole board, not a
  `cat` + `is-active` pair per unit. At 17 services that is 1 process spawn
  instead of 34, on every single poll.
* A missing unit still produces its own output block (LoadState=not-found), so
  one bad entry in services.json cannot blank the entire board.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
CATALOG = Path(os.environ.get("SPARK_CONSOLE_SERVICES", HERE / "services.json"))

# Same shape the private control-plane uses, so the UI needs no special-casing.
_ID_RE = re.compile(r"^[a-z][a-z0-9_-]{1,48}$")
_SCOPE = os.environ.get("SPARK_CONSOLE_SYSTEMD_SCOPE", "--user")
PROBE_TIMEOUT = float(os.environ.get("SPARK_CONSOLE_PROBE_TIMEOUT", "2"))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _systemctl(*args: str, timeout: int = 15) -> subprocess.CompletedProcess:
    return subprocess.run(["systemctl", _SCOPE, *args],
                          capture_output=True, text=True, timeout=timeout)


def _unit_states_bulk(units: list[str]) -> dict[str, tuple[bool, str]]:
    """{unit: (exists, active_state)} for every unit in a single systemctl call."""
    states: dict[str, tuple[bool, str]] = {}
    if not units:
        return states
    try:
        out = _systemctl("show", "--no-pager",
                         "--property=Id,LoadState,ActiveState", *units)
        if out.returncode != 0:
            return states
        for block in (out.stdout or "").split("\n\n"):
            fields = dict(line.split("=", 1)
                          for line in block.strip().splitlines() if "=" in line)
            uid = fields.get("Id")
            if uid:
                states[uid] = (fields.get("LoadState") != "not-found",
                               fields.get("ActiveState") or "unknown")
    except (OSError, subprocess.SubprocessError):
        pass
    return states


def _probe(url: str, timeout: float = PROBE_TIMEOUT) -> tuple[bool, str | None]:
    """GET a URL; on an OpenAI-style /v1/models body, return the first model id."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            if r.status >= 400:
                return False, None
            body = r.read(8000).decode("utf-8", errors="replace")
    except (urllib.error.URLError, OSError, ValueError):
        return False, None
    try:
        data = json.loads(body)
        models = data.get("data") if isinstance(data, dict) else None
        if models:
            return True, str(models[0].get("id") or "")[:60] or "ok"
    except ValueError:
        pass
    return True, "ok"


def load_catalog() -> dict[str, dict]:
    """Read services.json. A malformed file yields an empty board, never a 500."""
    if not CATALOG.is_file():
        return {}
    try:
        raw = json.loads(CATALOG.read_text())
    except (ValueError, OSError):
        return {}
    entries = raw.get("services", raw) if isinstance(raw, dict) else raw
    catalog: dict[str, dict] = {}
    if isinstance(entries, dict):
        entries = [{**v, "id": k} for k, v in entries.items()]
    for item in entries or []:
        if not isinstance(item, dict):
            continue
        sid = str(item.get("id") or "").strip()
        if not _ID_RE.match(sid):
            continue
        unit = str(item.get("unit") or "").strip()
        # Reject path traversal / non-service units before they reach systemctl
        if unit and (not unit.endswith(".service") or "/" in unit or ".." in unit):
            unit = ""
        probe_url = str(item.get("probe_url") or "").strip()
        port = item.get("port")
        if not probe_url and port:
            probe_url = f"http://127.0.0.1:{int(port)}/"
        if not unit and not probe_url:
            continue
        # Optional public/UI URL when probe is loopback-only or Tailscale-served
        open_url = str(item.get("open_url") or "").strip()
        if open_url and not (open_url.startswith("http://") or open_url.startswith("https://")):
            open_url = ""
        path = str(item.get("path") or "/").strip() or "/"
        if not path.startswith("/"):
            path = "/" + path
        catalog[sid] = {
            "label": str(item.get("label") or sid)[:80],
            "detail": str(item.get("detail") or "")[:160],
            "group": str(item.get("group") or "apps")[:32],
            "unit": unit,
            "probe_url": probe_url,
            "port": port,
            "path": path,
            "open_url": open_url[:400],
            "no_open": bool(item.get("no_open")),
            "critical": bool(item.get("critical")),
        }
    return catalog


def list_services() -> dict:
    catalog = load_catalog()
    unit_states = _unit_states_bulk([m["unit"] for m in catalog.values() if m["unit"]])

    services = []
    for sid, meta in catalog.items():
        row = {
            "id": sid, "label": meta["label"], "detail": meta["detail"],
            "group": meta["group"], "critical": meta["critical"],
            "no_stop": True,           # read-only tree: nothing is stoppable here
            "hint": "", "models": None, "default_model": None, "source": "config",
            "state": "unknown", "active": False, "serving": None,
            "extra": "", "unit_missing": False,
        }
        # So the UI can swap Start → Open when a service is up
        if meta.get("port") is not None:
            try:
                row["port"] = int(meta["port"])
            except (TypeError, ValueError):
                pass
        if meta.get("path"):
            row["path"] = meta["path"]
        if meta.get("open_url"):
            row["open_url"] = meta["open_url"]
        if meta.get("no_open"):
            row["no_open"] = True
        if meta["unit"]:
            exists, state = unit_states.get(meta["unit"], (False, "unknown"))
            if not exists:
                row["state"] = "missing"
                row["unit_missing"] = True
                row["extra"] = f"no unit {meta['unit']}"
            else:
                row["state"] = state
                row["active"] = state == "active"
                row["extra"] = state
        if meta["probe_url"]:
            ok, mid = _probe(meta["probe_url"])
            if not meta["unit"]:
                row["active"] = ok
                row["state"] = "active" if ok else "inactive"
            if ok:
                row["serving"] = mid if mid and mid != "ok" else None
                row["extra"] = f"up · {mid}" if mid and mid != "ok" else "up"
            elif row["active"]:
                row["extra"] = "unit up, endpoint lag"
            else:
                row["extra"] = "down"
        services.append(row)

    group_order = {"inference": 0, "agent": 1, "apps": 2, "meta": 3}
    services.sort(key=lambda s: (group_order.get(s["group"], 9),
                                 not s["active"], s["label"]))
    return {
        "iso_ts": _now_iso(),
        "services": services,
        "active_operation": None,
        "waste_ids": [],
        "idle_ids": [],
        "catalog_path": str(CATALOG),
        "configured": bool(catalog),
    }


if __name__ == "__main__":
    print(json.dumps(list_services(), indent=2)[:3000])
