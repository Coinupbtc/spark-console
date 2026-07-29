#!/usr/bin/env python3
"""Allowlisted service status + start/stop for Spark Console.

Controls only known units/scripts — never arbitrary shell. Long starts
(llama load, node2 deep) run async with a short op log.

Catalog layers (merged each request; later wins only for non-builtin keys):
  1. BUILTIN_SERVICES  — hard-coded core stack
  2. data/services_extra.json — agent/register CLI overlay (persistent)
  3. data/services.d/*.json — optional drop-in fragments
  4. X-Spark-Console-* markers in ~/.config/systemd/user/*.service

Agents that create a new HTTP server MUST register it (no human prompt):
  bash ~/scripts/dgx/register-console-service.sh --id … --label … --port …
"""
from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import threading
import time
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

from last_activity import probe_last_used, touch_used

DATA_DIR = Path(__file__).resolve().parent / "data"
OPS_FILE = DATA_DIR / "service_operations.json"
EXTRA_CATALOG = DATA_DIR / "services_extra.json"
EXTRA_DIR = DATA_DIR / "services.d"
USER_UNITS = Path.home() / ".config/systemd/user"
NODE2_SCRIPT = Path.home() / "scripts/dgx/node2-deep-lane.sh"
NODE2_ENDPOINT = "http://192.168.100.11:8100/v1/models"
HOME = Path.home()

# Safe fields agents may set on registered entries
_REGISTER_ALLOWED = frozenset({
    "label", "detail", "group", "kind", "unit", "probe", "critical",
    "no_stop", "hint", "activity_files", "activity_dirs", "activity_journal",
    "models", "default_model", "ollama_status", "source", "registered_at",
    "probe_url", "probe_timeout", "port", "show_projects_tile",
})
_SAFE_GROUPS = frozenset({"inference", "hermes", "apps", "meta", "other"})
_SAFE_KINDS = frozenset({"systemd", "node2-deep", "probe-only"})
_ID_RE = re.compile(r"^[a-z][a-z0-9_-]{1,48}$")

# id -> catalog entry (core stack; never removed by register/unregister)
BUILTIN_SERVICES: dict[str, dict] = {
    # ---- inference ----
    "llama-miaai35": {
        "label": "Hermes 35B",
        "detail": "llama.cpp :8889",
        "group": "inference",
        "kind": "systemd",
        "unit": "llama-miaai35.service",
        "probe": ("http://127.0.0.1:8889/v1/models", 2),
        "critical": True,
        "hint": "Primary Hermes model. Stop only for maintenance.",
        "activity_journal": True,
    },
    "ollama": {
        "label": "Ollama",
        "detail": "VLM / misc :11434",
        "group": "inference",
        "kind": "systemd",
        "unit": "ollama.service",
        # Custom status via _ollama_status (tags ≠ loaded)
        "probe": None,
        "ollama_status": True,
        "critical": False,
        "hint": "Daemon often idle. Models only use RAM when loaded (KEEP_ALIVE=8m). Default Hermes text is llama :8889, not this.",
    },
    "node2-deep": {
        "label": "Node2 deep lane",
        "detail": "spark2 :8100 (CX7)",
        "group": "inference",
        "kind": "node2-deep",
        "critical": False,
        # Keys must match node2-deep-lane.sh / switch-deep-lane-trial.sh.
        # qwen27 LIVE deep trial 2026-07-28 (quality-lab 97.5 vs Puzzle 68.5). MiniMax deleted 2026-07-24.
        "models": ["qwen27", "puzzle", "mimo", "laguna", "qwen122"],
        "default_model": "qwen27",  # start-when-stopped fallback; UI prefers live model_key
        "hint": "Deep-lane TRIAL switchable. Start dropdown = key. Full swap (serve+wire TG): switch-deep-lane-trial.sh.",
    },
    "comfyui": {
        "label": "ComfyUI",
        "detail": "image/video :8188",
        "group": "inference",
        "kind": "systemd",
        "unit": "comfyui.service",
        "probe": ("http://127.0.0.1:8188/", 2),
        "critical": False,
        "hint": "On-demand; idle timer stops after ~20m.",
        "activity_files": [
            str(HOME / ".local/state/comfyui/last-activity"),
            str(HOME / ".local/state/comfyui/comfyui.log"),
        ],
        "activity_dirs": [str(HOME / "comfy/ComfyUI/output")],
        "activity_journal": False,
    },
    # ---- hermes ----
    "hermes-orchestrator": {
        "label": "Hermes · orchestrator",
        "detail": "Telegram + cron",
        "group": "hermes",
        "kind": "systemd",
        "unit": "hermes-gateway-orchestrator.service",
        "critical": True,
        "activity_journal": False,
    },
    "hermes-light": {
        "label": "Hermes · light",
        "detail": "deep TG bot",
        "group": "hermes",
        "kind": "systemd",
        "unit": "hermes-gateway-light.service",
        "critical": False,
        "activity_journal": False,
        "hint": "Telegram #2 deep chat → whatever is wired on node2 :8100 (trial; switch-deep-lane-trial.sh).",
        "deep_lane_label": True,
    },
    "hermes-dobby": {
        "label": "Hermes · dobby",
        "detail": "coding / agent",
        "group": "hermes",
        "kind": "systemd",
        "unit": "hermes-gateway-dobby.service",
        "critical": False,
        "activity_journal": False,
    },
    "hermes-smeagle": {
        "label": "Hermes · smeagle",
        "detail": "deep Kanban",
        "group": "hermes",
        "kind": "systemd",
        "unit": "hermes-gateway-smeagle.service",
        "critical": False,
        "activity_journal": False,
        "hint": "Heavy Kanban → node2 deep lane (trial key switchable).",
        "deep_lane_label": True,
    },
    # ---- apps / websites ----
    "stl-sandbox": {
        "label": "STL Sandbox",
        "detail": "3D print UI :8050",
        "group": "apps",
        "kind": "systemd",
        "unit": "stl-sandbox.service",
        "probe": ("http://127.0.0.1:8050/", 2),
        "critical": False,
        "hint": "Safe to stop when not designing.",
        "activity_dirs": [
            str(HOME / "Documents/sandbox-stl/stl-sandbox/output"),
            str(HOME / "Documents/sandbox-stl/stl-sandbox/data"),
        ],
        "activity_journal": True,
    },
    "betintel-backend": {
        "label": "BetIntel API",
        "detail": "sports FastAPI :8000",
        "group": "apps",
        "kind": "systemd",
        "unit": "betintel-backend.service",
        "probe": ("http://127.0.0.1:8000/api/status", 2),
        "critical": False,
        "activity_journal": True,
    },
    "betintel-frontend": {
        "label": "BetIntel UI",
        "detail": "Vite :5173",
        "group": "apps",
        "kind": "systemd",
        "unit": "betintel-frontend.service",
        "probe": ("http://127.0.0.1:5173/", 2),
        "critical": False,
        "activity_journal": True,
    },
    "blockfield": {
        "label": "Blockfield",
        "detail": "Bitcoin viz :8080",
        "group": "apps",
        "kind": "systemd",
        "unit": "bitcoin-blockfield.service",
        "probe": ("http://127.0.0.1:8080/", 2),
        "critical": False,
        "hint": "Static http.server — stop if unused.",
        "activity_journal": True,
    },
    "bakeoff-ui": {
        "label": "Bakeoff UI",
        "detail": "model bakeoff :8765",
        "group": "apps",
        "kind": "systemd",
        "unit": "bakeoff-ui.service",
        "probe": ("http://127.0.0.1:8765/", 2),
        "critical": False,
        "hint": "UI only; runs are CLI. Stop UI when done.",
        "activity_files": [
            str(HOME / ".local/state/hermes/bakeoff/progress.json"),
            str(HOME / ".local/state/hermes/bakeoff/history.jsonl"),
            str(HOME / ".local/state/hermes/bakeoff/latest.json"),
        ],
        "activity_dirs": [str(HOME / ".local/state/hermes/bakeoff/runs")],
        "activity_journal": True,
    },
    # ---- meta ----
    "dashboard": {
        "label": "Spark Console",
        "detail": "control panel :8085",
        "group": "meta",
        "kind": "systemd",
        "unit": "dgx-performance-dashboard.service",
        "critical": True,
        "no_stop": True,
        "hint": "Always-on control panel. Restart=always.",
        "activity_journal": True,
    },
}

# Back-compat alias: prefer get_services_catalog() for live merges
SERVICES = BUILTIN_SERVICES

_ops_lock = threading.Lock()
_catalog_lock = threading.Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_entry(sid: str, raw: dict, source: str) -> dict | None:
    """Validate and normalize one catalog entry. Returns None if invalid."""
    if not isinstance(raw, dict):
        return None
    if not _ID_RE.match(sid):
        return None
    label = str(raw.get("label") or sid).strip()[:80]
    if not label:
        return None
    kind = str(raw.get("kind") or "systemd").strip()
    if kind not in _SAFE_KINDS:
        return None
    group = str(raw.get("group") or "apps").strip()
    if group not in _SAFE_GROUPS:
        group = "apps"

    entry: dict = {
        "label": label,
        "detail": str(raw.get("detail") or "")[:120],
        "group": group,
        "kind": kind,
        "critical": bool(raw.get("critical", False)),
        "no_stop": bool(raw.get("no_stop", False)),
        "hint": str(raw.get("hint") or "")[:240],
        "source": source,
    }
    # Registered apps never elevate to critical/no_stop via overlay alone
    if source != "builtin":
        entry["critical"] = False
        if kind != "systemd" or not raw.get("no_stop"):
            entry["no_stop"] = False

    unit = raw.get("unit")
    if unit:
        unit_s = str(unit).strip()
        if not unit_s.endswith(".service") or "/" in unit_s or ".." in unit_s:
            return None
        entry["unit"] = unit_s

    if kind == "systemd" and not entry.get("unit"):
        # Default unit name from id
        entry["unit"] = f"{sid}.service"

    # probe: tuple (url, timeout) or probe_url + probe_timeout
    probe = raw.get("probe")
    if isinstance(probe, (list, tuple)) and len(probe) >= 1:
        url = str(probe[0]).strip()
        to = float(probe[1]) if len(probe) > 1 else 2.0
        if url.startswith("http://127.0.0.1") or url.startswith("http://localhost"):
            entry["probe"] = (url, max(0.5, min(to, 10.0)))
    elif raw.get("probe_url"):
        url = str(raw["probe_url"]).strip()
        if url.startswith("http://127.0.0.1") or url.startswith("http://localhost"):
            to = float(raw.get("probe_timeout") or 2.0)
            entry["probe"] = (url, max(0.5, min(to, 10.0)))
    elif raw.get("port"):
        try:
            port = int(raw["port"])
            if 1 <= port <= 65535:
                entry["probe"] = (f"http://127.0.0.1:{port}/", 2.0)
                if not entry.get("detail"):
                    entry["detail"] = f":{port}"
                entry["port"] = port
        except (TypeError, ValueError):
            pass

    if kind == "probe-only":
        entry["no_stop"] = True  # no unit control
        if not entry.get("probe"):
            return None

    for key in ("activity_files", "activity_dirs"):
        if isinstance(raw.get(key), list):
            entry[key] = [str(p)[:400] for p in raw[key][:20]]
    if "activity_journal" in raw:
        entry["activity_journal"] = bool(raw["activity_journal"])
    if raw.get("show_projects_tile"):
        entry["show_projects_tile"] = True
    if raw.get("registered_at"):
        entry["registered_at"] = str(raw["registered_at"])[:40]
    return entry


def _load_extra_json() -> dict[str, dict]:
    out: dict[str, dict] = {}
    if EXTRA_CATALOG.is_file():
        try:
            data = json.loads(EXTRA_CATALOG.read_text())
            services = data.get("services", data) if isinstance(data, dict) else {}
            if isinstance(services, dict):
                for sid, meta in services.items():
                    norm = _normalize_entry(sid, meta, source="extra")
                    if norm:
                        out[sid] = norm
        except (json.JSONDecodeError, OSError):
            pass
    if EXTRA_DIR.is_dir():
        for path in sorted(EXTRA_DIR.glob("*.json")):
            try:
                data = json.loads(path.read_text())
                services = data.get("services", data) if isinstance(data, dict) else {}
                if not isinstance(services, dict):
                    continue
                for sid, meta in services.items():
                    norm = _normalize_entry(sid, meta, source=f"services.d/{path.name}")
                    if norm:
                        out[sid] = norm
            except (json.JSONDecodeError, OSError):
                continue
    return out


def _parse_unit_spark_markers(text: str) -> dict:
    """Parse X-Spark-Console-* lines from a unit file [Unit]/[Service] comments or free keys."""
    found: dict = {}
    # Comment form: # X-Spark-Console: id=foo group=apps label=Foo port=8099
    for m in re.finditer(
        r"(?m)^\s*(?:#\s*)?X-Spark-Console(?:-([A-Za-z0-9_-]+))?\s*[:=]\s*(.+?)\s*$",
        text,
    ):
        key, val = m.group(1), m.group(2).strip()
        if key is None or key == "":
            # key=value pairs in one line
            for part in re.split(r"\s+", val):
                if "=" in part:
                    k, v = part.split("=", 1)
                    found[k.strip().lower()] = v.strip().strip('"')
        else:
            found[key.lower().replace("-", "_")] = val.strip().strip('"')
    return found


def _discover_unit_markers() -> dict[str, dict]:
    """Units that declare X-Spark-Console markers auto-join the catalog."""
    out: dict[str, dict] = {}
    if not USER_UNITS.is_dir():
        return out
    for path in USER_UNITS.glob("*.service"):
        # Skip templates / bak / disabled copies
        if ".bak" in path.name or path.name.endswith(".disabled"):
            continue
        try:
            text = path.read_text(errors="replace")
        except OSError:
            continue
        if "X-Spark-Console" not in text:
            continue
        marks = _parse_unit_spark_markers(text)
        sid = marks.get("id") or path.stem
        if sid in BUILTIN_SERVICES:
            continue
        raw = {
            "label": marks.get("label") or path.stem.replace("-", " ").title(),
            "detail": marks.get("detail") or "",
            "group": marks.get("group") or "apps",
            "kind": marks.get("kind") or "systemd",
            "unit": path.name,
            "hint": marks.get("hint") or "Auto-discovered from unit X-Spark-Console marker.",
            "show_projects_tile": marks.get("projects", "").lower() in ("1", "true", "yes"),
        }
        if marks.get("port"):
            raw["port"] = marks["port"]
        if marks.get("probe") or marks.get("probe_url"):
            raw["probe_url"] = marks.get("probe") or marks.get("probe_url")
        norm = _normalize_entry(sid, raw, source=f"unit:{path.name}")
        if norm:
            out[sid] = norm
    return out


def get_services_catalog() -> dict[str, dict]:
    """Merged live catalog: builtin + extra JSON + unit markers.

    Builtin ids are never overridden (safety).
    """
    merged: dict[str, dict] = {k: dict(v) for k, v in BUILTIN_SERVICES.items()}
    for k, v in merged.items():
        v.setdefault("source", "builtin")
    for sid, meta in _load_extra_json().items():
        if sid in BUILTIN_SERVICES:
            continue
        merged[sid] = meta
    for sid, meta in _discover_unit_markers().items():
        if sid in BUILTIN_SERVICES:
            continue
        # Explicit extra file wins over bare unit marker
        if sid in merged and merged[sid].get("source", "").startswith("extra"):
            continue
        if sid in merged and merged[sid].get("source", "").startswith("services.d"):
            continue
        merged[sid] = meta
    return merged


def _save_extra_catalog(services: dict[str, dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "updated_at": _now_iso(),
        "services": services,
    }
    tmp = EXTRA_CATALOG.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n")
    os.replace(tmp, EXTRA_CATALOG)


def load_extra_services_raw() -> dict[str, dict]:
    if not EXTRA_CATALOG.is_file():
        return {}
    try:
        data = json.loads(EXTRA_CATALOG.read_text())
        services = data.get("services", {}) if isinstance(data, dict) else {}
        return services if isinstance(services, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def register_service(entry: dict, *, replace: bool = True) -> dict:
    """Register or update a non-builtin service in services_extra.json.

    entry must include at least: id, label; and either unit+kind=systemd,
    or port / probe_url for probe-only.
    """
    sid = str(entry.get("id") or "").strip()
    if not _ID_RE.match(sid):
        return {"ok": False, "error": f"Invalid id '{sid}' (use [a-z][a-z0-9_-]{{1,48}})"}
    if sid in BUILTIN_SERVICES:
        return {"ok": False, "error": f"'{sid}' is builtin — edit service_control.py, not the overlay"}

    raw = {k: v for k, v in entry.items() if k in _REGISTER_ALLOWED or k in ("port", "probe_url")}
    raw.setdefault("label", entry.get("label") or sid)
    raw.setdefault("kind", entry.get("kind") or ("systemd" if entry.get("unit") else "probe-only"))
    raw.setdefault("group", entry.get("group") or "apps")
    if entry.get("port") is not None:
        raw["port"] = entry["port"]
    if entry.get("unit"):
        raw["unit"] = entry["unit"]
    if entry.get("probe_url"):
        raw["probe_url"] = entry["probe_url"]
    raw["registered_at"] = _now_iso()
    raw["show_projects_tile"] = bool(entry.get("show_projects_tile", True))

    norm = _normalize_entry(sid, raw, source="extra")
    if not norm:
        return {"ok": False, "error": "Entry failed validation (need label + unit or localhost probe/port)"}

    with _catalog_lock:
        services = load_extra_services_raw()
        if sid in services and not replace:
            return {"ok": False, "error": f"Already registered: {sid}", "service": sid}
        # Store JSON-serializable form (probe as list)
        store = dict(norm)
        if "probe" in store and isinstance(store["probe"], tuple):
            store["probe"] = list(store["probe"])
        store["source"] = "extra"
        services[sid] = store
        _save_extra_catalog(services)

    return {"ok": True, "service": sid, "entry": norm, "catalog": str(EXTRA_CATALOG)}


def unregister_service(service_id: str) -> dict:
    sid = str(service_id).strip()
    if sid in BUILTIN_SERVICES:
        return {"ok": False, "error": "Cannot unregister builtin service"}
    with _catalog_lock:
        services = load_extra_services_raw()
        if sid not in services:
            return {"ok": False, "error": f"Not in extra catalog: {sid}"}
        del services[sid]
        _save_extra_catalog(services)
    return {"ok": True, "service": sid, "removed": True}


def project_tiles_from_catalog() -> list[dict]:
    """Port tiles for projects panel from registered entries with show_projects_tile."""
    tiles = []
    for sid, meta in get_services_catalog().items():
        if meta.get("source") == "builtin":
            continue
        if not meta.get("show_projects_tile", False):
            continue
        port = meta.get("port")
        if not port and meta.get("probe"):
            # extract port from probe url
            m = re.search(r":(\d+)", meta["probe"][0])
            if m:
                port = int(m.group(1))
        if not port:
            continue
        tiles.append({
            "id": sid,
            "label": meta.get("label") or sid,
            "port": int(port),
            "unit": meta.get("unit"),
        })
    return tiles


def _systemctl(*args: str, timeout: int = 15) -> subprocess.CompletedProcess:
    env = {**os.environ, "HOME": str(Path.home()),
           "XDG_RUNTIME_DIR": f"/run/user/{os.getuid()}",
           "DBUS_SESSION_BUS_ADDRESS": f"unix:path=/run/user/{os.getuid()}/bus"}
    return subprocess.run(
        ["systemctl", "--user", *args],
        capture_output=True, text=True, timeout=timeout, env=env,
    )


def _short_model_id(raw: str) -> str:
    if not raw:
        return ""
    name = raw.rstrip("/").split("/")[-1]
    if name.endswith(".gguf"):
        name = name[: -len(".gguf")]
    return name[-56:]


def _probe(url: str, timeout: float = 2.0) -> tuple[bool, str]:
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read(8000).decode("utf-8", errors="replace")
        mid = ""
        try:
            data = json.loads(body)
            if data.get("data"):
                mid = _short_model_id(data["data"][0].get("id") or "")
            if not mid and data.get("models"):
                m0 = data["models"][0]
                mid = _short_model_id(m0.get("name") or m0.get("model") or m0.get("id") or "")
            if not mid and isinstance(data, dict):
                mid = "ok"
        except Exception:
            mid = "ok" if body or True else "ok"
        return True, mid or "ok"
    except Exception as e:
        return False, str(e)[:80]


def _ollama_status() -> tuple[bool, str, str]:
    """Return (daemon_up, serving_label, extra).

    /api/tags lists *installed* weights on disk — not VRAM residents.
    /api/ps is what is actually loaded (keep-alive). Empty ps = idle daemon.
    """
    ok_tags, _ = _probe("http://127.0.0.1:11434/api/tags", 2)
    if not ok_tags:
        return False, None, "daemon down"
    loaded = []
    try:
        req = urllib.request.Request("http://127.0.0.1:11434/api/ps", method="GET")
        with urllib.request.urlopen(req, timeout=2) as r:
            data = json.loads(r.read(8000).decode("utf-8", errors="replace"))
        for m in data.get("models") or []:
            name = m.get("name") or m.get("model") or "?"
            loaded.append(str(name).split(":")[0][:40] if ":" in str(name) else str(name)[:40])
            # keep full short name
            loaded[-1] = str(name)[:48]
    except Exception:
        pass
    if loaded:
        return True, loaded[0], f"loaded · {', '.join(loaded)}"
    # installed inventory (disk only)
    installed = []
    try:
        req = urllib.request.Request("http://127.0.0.1:11434/api/tags", method="GET")
        with urllib.request.urlopen(req, timeout=2) as r:
            data = json.loads(r.read(8000).decode("utf-8", errors="replace"))
        for m in data.get("models") or []:
            installed.append(str(m.get("name") or "?")[:40])
    except Exception:
        pass
    inv = ", ".join(installed[:3]) if installed else "no models"
    if len(installed) > 3:
        inv += f" +{len(installed)-3}"
    return True, None, f"idle daemon · on disk: {inv}"


def _unit_active(unit: str) -> str:
    try:
        out = _systemctl("is-active", unit, timeout=5)
        return (out.stdout or out.stderr or "unknown").strip().splitlines()[0]
    except Exception:
        return "unknown"


def _unit_exists(unit: str) -> bool:
    try:
        out = _systemctl("cat", unit, timeout=5)
        return out.returncode == 0
    except Exception:
        return False


def _unit_states_bulk(units: list[str]) -> dict[str, tuple[bool, str]]:
    """Resolve every unit's existence + active state in ONE systemctl call.

    Returns {unit: (exists, active_state)}. Replaces the per-unit
    `systemctl cat` + `systemctl is-active` pair, which cost two process spawns
    per service — 34 spawns for a 17-service catalog, on every single poll.

    A missing unit still gets its own output block (LoadState=not-found) rather
    than failing the whole command, so one bad catalog entry cannot blank the
    board. Blocks are keyed by the Id systemd echoes back; callers fall back to
    the single-unit helpers for anything absent from the result (e.g. aliases).
    """
    states: dict[str, tuple[bool, str]] = {}
    if not units:
        return states
    try:
        out = _systemctl("show", "--no-pager",
                         "--property=Id,LoadState,ActiveState", *units, timeout=15)
        if out.returncode != 0:
            return states
        for block in (out.stdout or "").split("\n\n"):
            fields = dict(line.split("=", 1)
                          for line in block.strip().splitlines() if "=" in line)
            uid = fields.get("Id")
            if uid:
                states[uid] = (fields.get("LoadState") != "not-found",
                               fields.get("ActiveState") or "unknown")
    except Exception:
        pass
    return states


_NODE2_STATE = Path.home() / ".local/state/hermes/node2-deep-lane"
_NODE2_ACTIVE_FILE = _NODE2_STATE / "active-model"
_NODE2_TRIAL_FILE = _NODE2_STATE / "trial-key"


def _read_node2_active_key() -> str | None:
    for p in (_NODE2_ACTIVE_FILE, _NODE2_TRIAL_FILE):
        try:
            if p.is_file():
                k = p.read_text().strip()
                if k:
                    return k
        except OSError:
            continue
    return None


_N2_STATUS_TTL = 15.0
_n2_status_lock = threading.Lock()
_n2_status_cache: dict = {"at": 0.0, "data": None}


def invalidate_node2_status() -> None:
    """Drop the cached deep-lane status — call after mutating the lane."""
    with _n2_status_lock:
        _n2_status_cache["at"] = 0.0


def _node2_status() -> dict:
    """Deep-lane status, cached for _N2_STATUS_TTL seconds.

    Each uncached call shells out to node2-deep-lane.sh, which SSHes to node2 —
    measured at 0.42s, i.e. ~72% of the entire service-board sweep. The lane
    changes state on the order of minutes (a cold model load is several), so a
    few seconds of staleness costs nothing, and start/stop/restart invalidate
    the cache explicitly so the UI never shows pre-click state.
    """
    with _n2_status_lock:
        cached = _n2_status_cache["data"]
        if cached is not None and (time.time() - _n2_status_cache["at"]) < _N2_STATUS_TTL:
            return dict(cached)
    fresh = _node2_status_uncached()
    with _n2_status_lock:
        _n2_status_cache.update({"at": time.time(), "data": dict(fresh)})
    return fresh


def _node2_status_uncached() -> dict:
    st = {
        "active": False,
        "state": "inactive",
        "detail": "not running",
        "serving": None,
        "model_key": None,
        "up": False,
    }
    # Fast path: state files (avoid SSH/status lag on every poll)
    file_key = _read_node2_active_key()
    if file_key:
        st["model_key"] = file_key
    if NODE2_SCRIPT.is_file():
        try:
            env = {**os.environ, "HOME": str(Path.home())}
            env.pop("SSH_AUTH_SOCK", None)
            out = subprocess.run(
                ["bash", str(NODE2_SCRIPT), "status"],
                capture_output=True, text=True, timeout=20, env=env,
            )
            text = (out.stdout or "") + "\n" + (out.stderr or "")
            for line in text.splitlines():
                low = line.lower().strip()
                if low.startswith("up:"):
                    st["up"] = low.split(":", 1)[1].strip() in ("1", "true", "yes")
                if low.startswith("active-key:"):
                    k = line.split(":", 1)[1].strip() or None
                    if k and k != "?":
                        st["model_key"] = k
                if low.startswith("serving-id:"):
                    st["serving"] = line.split(":", 1)[1].strip() or None
        except Exception as e:
            st["detail"] = f"status script: {e}"[:120]

    ok, probe = _probe(NODE2_ENDPOINT, 3.0)
    if ok:
        st["active"] = True
        st["state"] = "active"
        key = st.get("model_key") or ""
        serve = st["serving"] or (probe if probe != "ok" else None)
        if serve and (not st.get("serving") or st["serving"] == "?"):
            st["serving"] = serve
        # Prefer "laguna · Laguna-S-…" style detail so UI never freezes on old name
        if key and serve:
            st["detail"] = f"{key} · {serve}"
        else:
            st["detail"] = serve or key or "serving"
    elif st.get("up"):
        st["active"] = True
        st["state"] = "activating"
        st["detail"] = st["serving"] or st["model_key"] or "process up, endpoint lag"
    else:
        st["state"] = "inactive"
        st["detail"] = "stopped"
    return st


def list_services() -> dict:
    catalog = get_services_catalog()
    # One node2 status for deep-lane rows + dynamic hermes light/smeagle labels
    n2_cache = None

    def n2() -> dict:
        nonlocal n2_cache
        if n2_cache is None:
            n2_cache = _node2_status()
        return n2_cache

    # One systemctl call up front for the whole board (see _unit_states_bulk)
    unit_states = _unit_states_bulk([
        meta["unit"] for meta in catalog.values()
        if meta.get("kind") == "systemd" and meta.get("unit")
    ])

    services = []
    for sid, meta in catalog.items():
        row = {
            "id": sid,
            "label": meta["label"],
            "detail": meta.get("detail", ""),
            "group": meta.get("group", "other"),
            "critical": bool(meta.get("critical")),
            "no_stop": bool(meta.get("no_stop")),
            "hint": meta.get("hint", ""),
            "models": meta.get("models"),
            "default_model": meta.get("default_model"),
            "source": meta.get("source", "builtin"),
            "state": "unknown",
            "active": False,
            "serving": None,
            "extra": "",
            "unit_missing": False,
        }
        # Live deep-lane key for labels that follow the trial (not hard-coded MiMo/Laguna)
        if meta.get("deep_lane_label"):
            mk = n2().get("model_key") or _read_node2_active_key() or "?"
            row["detail"] = f"deep · {mk}"
            row["model_key"] = mk if mk != "?" else None
        kind = meta["kind"]
        if kind == "systemd":
            unit = meta["unit"]
            exists, state = unit_states.get(unit, (None, None))
            if exists is None:  # absent from the bulk result — ask about it directly
                exists, state = _unit_exists(unit), _unit_active(unit)
            if not exists:
                row["state"] = "missing"
                row["unit_missing"] = True
                row["extra"] = f"no unit {unit}"
            else:
                row["state"] = state
                row["active"] = state == "active"
                if meta.get("ollama_status"):
                    up, mid, extra = _ollama_status()
                    if up and row["active"]:
                        row["serving"] = mid  # only when actually loaded
                        row["extra"] = extra
                        # Empty daemon is intentional standby (KEEP_ALIVE=8m) — not waste.
                        row["ollama_loaded"] = bool(mid)
                    elif row["active"]:
                        row["extra"] = "unit up, endpoint lag"
                        row["ollama_loaded"] = False
                    else:
                        row["extra"] = "stopped"
                        row["ollama_loaded"] = False
                else:
                    probe = meta.get("probe")
                    if probe:
                        ok, mid = _probe(probe[0], probe[1])
                        if ok:
                            row["serving"] = mid if mid != "ok" else None
                            row["extra"] = f"up · {mid}" if mid else "up"
                        elif row["active"]:
                            row["extra"] = "unit up, endpoint lag"
                        else:
                            row["extra"] = "stopped"
                    else:
                        row["extra"] = state
        elif kind == "probe-only":
            probe = meta.get("probe")
            if probe:
                ok, mid = _probe(probe[0], probe[1])
                row["active"] = ok
                row["state"] = "active" if ok else "inactive"
                row["extra"] = (f"up · {mid}" if mid and mid != "ok" else "up") if ok else "down"
                row["no_stop"] = True
            else:
                row["state"] = "missing"
                row["extra"] = "no probe"
        elif kind == "node2-deep":
            n2s = n2()
            row["state"] = n2s["state"]
            row["active"] = n2s["active"]
            row["serving"] = n2s.get("serving") or n2s.get("model_key")
            row["extra"] = n2s.get("detail") or ""
            row["model_key"] = n2s.get("model_key")
            # Dropdown selected value = live key when up, else catalog default
            if n2s.get("model_key"):
                row["default_model"] = n2s["model_key"]

        act = probe_last_used(sid, meta, row["active"])
        row.update(act)
        # Ollama with nothing loaded: cheap daemon, never yellow/waste
        if sid == "ollama" and row.get("active") and not row.get("ollama_loaded"):
            row["idle"] = "ok"
            if not row.get("serving"):
                row["last_used_ago"] = row.get("last_used_ago") or "standby"
                if row.get("last_used_source") in (None, "none", "unit start (no traffic seen)"):
                    row["last_used_source"] = "daemon only (no model loaded)"
                    row["last_used_ago"] = "standby"
        # Ollama with a model resident: keep real idle signal (RAM is in use)
        services.append(row)

    # Sort: group order inference, hermes, apps, meta — idle waste first within group
    group_order = {"inference": 0, "hermes": 1, "apps": 2, "meta": 3}
    idle_order = {"waste": 0, "idle": 1, "warm": 2, "ok": 3, "unknown": 4, "off": 5}

    def _key(s: dict):
        return (
            group_order.get(s.get("group"), 9),
            idle_order.get(s.get("idle"), 9) if s.get("active") else 20,
            s.get("label") or "",
        )

    services.sort(key=_key)

    with _ops_lock:
        active_op = _active_op_unlocked(_load_ops())
    waste = [s["id"] for s in services if s.get("idle") == "waste" and s.get("active")]
    idle = [s["id"] for s in services if s.get("idle") == "idle" and s.get("active")]
    return {
        "iso_ts": _now_iso(),
        "services": services,
        "active_operation": active_op,
        "waste_ids": waste,
        "idle_ids": idle,
    }


def _load_ops() -> dict:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if OPS_FILE.is_file():
        try:
            return json.loads(OPS_FILE.read_text())
        except json.JSONDecodeError:
            pass
    return {"operations": {}}


def _save_ops(data: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OPS_FILE.write_text(json.dumps(data, indent=2))


def _active_op_unlocked(data: dict) -> dict | None:
    for op in data.get("operations", {}).values():
        if op.get("status") != "running":
            continue
        pid = op.get("pid")
        if pid:
            try:
                os.kill(pid, 0)
                return op
            except OSError:
                op["status"] = "failed"
                op["finished_at"] = _now_iso()
                continue
        return op
    return None


def get_operation(op_id: str) -> dict | None:
    with _ops_lock:
        data = _load_ops()
        op = data["operations"].get(op_id)
        if not op:
            return None
        if op.get("status") == "running" and op.get("pid"):
            try:
                os.kill(op["pid"], 0)
            except OSError:
                op["status"] = "failed"
                op["finished_at"] = _now_iso()
                data["operations"][op_id] = op
                _save_ops(data)
        return op


def _spawn(cmd: list[str], op_type: str, key: str, message: str) -> dict:
    with _ops_lock:
        data = _load_ops()
        running = _active_op_unlocked(data)
        if running:
            return {
                "ok": False,
                "error": f"Busy: {running.get('type')} {running.get('key', '')}",
                "operation": running,
            }

    op_id = uuid.uuid4().hex[:12]
    log_file = DATA_DIR / f"svc_op_{op_id}.log"
    env = {
        **os.environ,
        "HOME": str(Path.home()),
        "XDG_RUNTIME_DIR": f"/run/user/{os.getuid()}",
        "DBUS_SESSION_BUS_ADDRESS": f"unix:path=/run/user/{os.getuid()}/bus",
        "PATH": f"{Path.home()}/.local/bin:/usr/local/bin:/usr/bin:/bin",
    }
    env.pop("SSH_AUTH_SOCK", None)

    with open(log_file, "w") as logfh:
        proc = subprocess.Popen(
            cmd, stdout=logfh, stderr=subprocess.STDOUT, env=env,
        )

    op = {
        "id": op_id,
        "type": op_type,
        "key": key,
        "status": "running",
        "message": message,
        "pid": proc.pid,
        "log_file": str(log_file),
        "command": " ".join(cmd),
        "started_at": _now_iso(),
        "finished_at": None,
        "returncode": None,
    }

    def _waiter():
        rc = proc.wait()
        with _ops_lock:
            data = _load_ops()
            cur = data["operations"].get(op_id, op)
            cur["returncode"] = rc
            try:
                cur["log_tail"] = "\n".join(
                    Path(log_file).read_text(errors="replace").splitlines()[-40:]
                )
            except OSError:
                cur["log_tail"] = ""
            cur["status"] = "completed" if rc == 0 else "failed"
            cur["message"] = "Done." if rc == 0 else f"Failed (exit {rc})."
            cur["finished_at"] = _now_iso()
            data["operations"][op_id] = cur
            ops = data["operations"]
            if len(ops) > 40:
                keys = sorted(ops, key=lambda k: ops[k].get("started_at", ""))
                for k in keys[:-30]:
                    del ops[k]
            _save_ops(data)

    with _ops_lock:
        data = _load_ops()
        data["operations"][op_id] = op
        _save_ops(data)
    threading.Thread(target=_waiter, daemon=True).start()
    return {"ok": True, "operation": op}


def service_action(service_id: str, action: str, model: str | None = None) -> dict:
    catalog = get_services_catalog()
    if service_id not in catalog:
        return {"ok": False, "error": f"Unknown service: {service_id}"}
    if action not in ("start", "stop", "restart"):
        return {"ok": False, "error": f"Bad action: {action}"}
    meta = catalog[service_id]
    if action == "stop" and meta.get("no_stop"):
        return {"ok": False, "error": "Cannot stop this service from the console."}

    kind = meta["kind"]
    if kind == "probe-only":
        return {"ok": False, "error": "Probe-only entry (no start/stop). Add a systemd unit to control it."}

    if kind == "systemd":
        unit = meta["unit"]
        if not _unit_exists(unit):
            return {"ok": False, "error": f"Unit missing: {unit}. Install unit first."}
        long = service_id in ("llama-miaai35", "comfyui") and action in ("start", "restart")
        cmd = ["systemctl", "--user", action, unit]
        if long:
            return _spawn(
                cmd, action, service_id,
                f"{action} {meta['label']} (may take a while)",
            )
        try:
            out = _systemctl(action, unit, timeout=60)
            ok = out.returncode == 0
            if ok and action == "start":
                touch_used(service_id)
            return {
                "ok": ok,
                "service": service_id,
                "action": action,
                "message": (out.stdout or out.stderr or "").strip()[:300] or (
                    "ok" if ok else f"exit {out.returncode}"
                ),
                "state": _unit_active(unit),
            }
        except Exception as e:
            return {"ok": False, "error": str(e)[:200]}

    if kind == "node2-deep":
        if not NODE2_SCRIPT.is_file():
            return {"ok": False, "error": f"Missing {NODE2_SCRIPT}"}
        invalidate_node2_status()  # lane is about to change; don't serve the TTL copy
        if action == "stop":
            return _spawn(
                ["bash", str(NODE2_SCRIPT), "stop"],
                "stop", service_id, "Stopping node2 deep lane",
            )
        if action == "restart":
            m = model or meta.get("default_model") or _read_node2_active_key() or "laguna"
            if m not in meta.get("models", [m]):
                return {"ok": False, "error": f"Unknown model {m}"}
            return _spawn(
                ["bash", "-c",
                 f"bash '{NODE2_SCRIPT}' stop; bash '{NODE2_SCRIPT}' start {m}"],
                "restart", service_id, f"Restart node2 deep → {m}",
            )
        m = model or meta.get("default_model") or _read_node2_active_key() or "laguna"
        if m not in meta.get("models", [m]):
            return {"ok": False, "error": f"Unknown model {m}. Allowed: {meta.get('models')}"}
        return _spawn(
            ["bash", str(NODE2_SCRIPT), "start", m],
            "start", service_id,
            f"Start node2 deep lane ({m}) — cold load can take several minutes. "
            f"For Telegram rewire: switch-deep-lane-trial.sh {m}",
        )

    return {"ok": False, "error": f"Unhandled kind {kind}"}
