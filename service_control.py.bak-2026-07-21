#!/usr/bin/env python3
"""Allowlisted service status + start/stop for Spark Console.

Controls only known units/scripts — never arbitrary shell. Long starts
(llama load, node2 deep) run async with a short op log.
"""
from __future__ import annotations

import json
import os
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
NODE2_SCRIPT = Path.home() / "scripts/dgx/node2-deep-lane.sh"
NODE2_ENDPOINT = "http://192.168.100.11:8100/v1/models"
HOME = Path.home()

# id -> catalog entry
SERVICES: dict[str, dict] = {
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
        "models": ["qwen122", "minimax", "mimo", "minimax-reap"],
        "default_model": "mimo",
        "hint": "Heavy GGUF on node2 (~80–110G). Stop if idle.",
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
        "detail": "light bot",
        "group": "hermes",
        "kind": "systemd",
        "unit": "hermes-gateway-light.service",
        "critical": False,
        "activity_journal": False,
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
        "detail": "MiMo profile",
        "group": "hermes",
        "kind": "systemd",
        "unit": "hermes-gateway-smeagle.service",
        "critical": False,
        "activity_journal": False,
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

_ops_lock = threading.Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _node2_status() -> dict:
    st = {
        "active": False,
        "state": "inactive",
        "detail": "not running",
        "serving": None,
        "model_key": None,
        "up": False,
    }
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
                    st["model_key"] = line.split(":", 1)[1].strip() or None
                if low.startswith("serving-id:"):
                    st["serving"] = line.split(":", 1)[1].strip() or None
        except Exception as e:
            st["detail"] = f"status script: {e}"[:120]

    ok, probe = _probe(NODE2_ENDPOINT, 3.0)
    if ok:
        st["active"] = True
        st["state"] = "active"
        st["detail"] = st["serving"] or probe or "serving"
        if probe and probe != "ok" and not st["serving"]:
            st["serving"] = probe
    elif st.get("up"):
        st["active"] = True
        st["state"] = "activating"
        st["detail"] = st["serving"] or st["model_key"] or "process up, endpoint lag"
    else:
        st["state"] = "inactive"
        st["detail"] = "stopped"
    return st


def list_services() -> dict:
    services = []
    for sid, meta in SERVICES.items():
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
            "state": "unknown",
            "active": False,
            "serving": None,
            "extra": "",
            "unit_missing": False,
        }
        kind = meta["kind"]
        if kind == "systemd":
            unit = meta["unit"]
            if not _unit_exists(unit):
                row["state"] = "missing"
                row["unit_missing"] = True
                row["extra"] = f"no unit {unit}"
            else:
                state = _unit_active(unit)
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
        elif kind == "node2-deep":
            n2 = _node2_status()
            row["state"] = n2["state"]
            row["active"] = n2["active"]
            row["serving"] = n2.get("serving") or n2.get("model_key")
            row["extra"] = n2.get("detail") or ""
            row["model_key"] = n2.get("model_key")

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
    if service_id not in SERVICES:
        return {"ok": False, "error": f"Unknown service: {service_id}"}
    if action not in ("start", "stop", "restart"):
        return {"ok": False, "error": f"Bad action: {action}"}
    meta = SERVICES[service_id]
    if action == "stop" and meta.get("no_stop"):
        return {"ok": False, "error": "Cannot stop this service from the console."}

    kind = meta["kind"]
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
        if action == "stop":
            return _spawn(
                ["bash", str(NODE2_SCRIPT), "stop"],
                "stop", service_id, "Stopping node2 deep lane",
            )
        if action == "restart":
            m = model or meta.get("default_model") or "mimo"
            if m not in meta.get("models", [m]):
                return {"ok": False, "error": f"Unknown model {m}"}
            return _spawn(
                ["bash", "-c",
                 f"bash '{NODE2_SCRIPT}' stop; bash '{NODE2_SCRIPT}' start {m}"],
                "restart", service_id, f"Restart node2 deep → {m}",
            )
        m = model or meta.get("default_model") or "mimo"
        if m not in meta.get("models", [m]):
            return {"ok": False, "error": f"Unknown model {m}. Allowed: {meta.get('models')}"}
        return _spawn(
            ["bash", str(NODE2_SCRIPT), "start", m],
            "start", service_id,
            f"Start node2 deep lane ({m}) — cold load can take several minutes",
        )

    return {"ok": False, "error": f"Unhandled kind {kind}"}
