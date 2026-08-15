"""Named Spark stack switcher for the console (prime / dream / qwen38 / setup / video / music).

The heavy lifting lives in ~/scripts/dgx/spark-stack.sh — this module is the
allowlisted API: detect what's up, spawn one switch, poll the log.
"""
from __future__ import annotations

import json
import os
import subprocess
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

DATA_DIR = Path(__file__).resolve().parent / "data"
OPS_FILE = DATA_DIR / "stack_operations.json"
STATE_JSON = Path.home() / ".local/state/hermes/spark-stack.json"
STACK_SCRIPT = Path.home() / "scripts/dgx/spark-stack.sh"
H3_FABRIC = os.environ.get("H3_API_BASE", "http://192.168.100.10:8800").rstrip("/")

# UI copy — keep keys in lockstep with spark-stack.sh
PRESETS: dict[str, dict] = {
    "prime": {
        "label": "Prime",
        "short": "DS4F 0731 · 500k · vision",
        "detail": "DeepSeek-V4-Flash 0731 on both Sparks (TP2) with the Qwen vision sidecar. Chat stays local.",
        "eta": "10–15 min",
        "stops": "Music3, helper 35B, MiniMax H3, Qwen 3.8",
        "starts": "DS4F :8888 + vision :8890",
    },
    "dream": {
        "label": "Dream",
        "short": "0731 348k · Qwen 88k MTP3 · pics n1",
        "detail": "0731 on both Sparks at 348k, 4B pictures on this box, Qwen 3.8 GGUF 88k + MTP3 on node2 :8100. Orch/dobby = 0731. Smeagle = Qwen (max_tokens 20k).",
        "eta": "10–20 min",
        "stops": "Music3, helper 35B, MiniMax H3, Qwen 3.8 NVFP4",
        "starts": "DS4F :8888 + vision n1 + Qwen GGUF :8100",
    },
    "qwen38": {
        "label": "Qwen 3.8",
        "short": "Mia NVFP4 · both Sparks",
        "detail": "Qwen3.8-27B Unsloth NVFP4 (Mia recipe) on each Spark. Orch/dobby/light on this box; smeagle on node2.",
        "eta": "10–20 min",
        "stops": "DS4F, helper 35B, MiniMax H3, Music3, vision sidecar",
        "starts": "NVFP4 :8888 node1 + :8888 node2",
    },
    "setup": {
        "label": "Setup",
        "short": "Helper 35B only",
        "detail": "Daily chat: Qwen 35B on this Spark. Stops Music3, DS4F, and H3 so UMA is free for agents.",
        "eta": "2–15 min",
        "stops": "DS4F, MiniMax H3, Music3, vision sidecar, Qwen 3.8",
        "starts": "helper :8889",
    },
    "video": {
        "label": "Videos",
        "short": "MiniMax H3 TP2",
        "detail": "MiniMax H3 on both Sparks for video. Telegram chat moves to Nous until you leave this setup.",
        "eta": "10–15 min",
        "stops": "DS4F, Music3, vision sidecar, Qwen 3.8",
        "starts": "H3 :8800 · chat → Nous",
    },
    "music": {
        "label": "Music",
        "short": "Helper 35B · Music3",
        "detail": "Qwen 35B chat on this Spark plus AIM Music3 (and the spark2 replica). Vision sidecar stays off.",
        "eta": "10–15 min",
        "stops": "DS4F, MiniMax H3, vision sidecar, Qwen 3.8",
        "starts": "helper :8889 + Music3 :8801",
    },
}

_ops_lock = threading.Lock()
_detect_lock = threading.Lock()
_detect_cache: tuple[float, dict] | None = None
_DETECT_TTL = 4.0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _model_ids(url: str, timeout: float = 1.5) -> list[str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace")[:12000])
        return [
            str(m.get("id") or "").lower()
            for m in (data.get("data") or [])
        ]
    except Exception:
        return []


def _probe(url: str, timeout: float = 1.5) -> bool:
    """True when /v1/models answers with JSON. Never raises."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace")[:8000])
        return bool(data.get("data") or data.get("id") or data.get("object"))
    except Exception:
        return False


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


def _tail_file(path: Path, lines: int = 24) -> str:
    if not path.is_file():
        return ""
    try:
        return "\n".join(path.read_text(errors="replace").splitlines()[-lines:])
    except OSError:
        return ""


def _refresh_op(op: dict) -> dict:
    if op.get("status") in ("completed", "failed"):
        return op
    pid = op.get("pid")
    if not pid:
        return op
    try:
        os.kill(pid, 0)
        op["status"] = "running"
        op["log_tail"] = _tail_file(Path(op.get("log_file") or ""), 16)
        return op
    except OSError:
        log_path = Path(op.get("log_file") or "")
        tail = _tail_file(log_path, 24)
        op["log_tail"] = tail
        rc = op.get("returncode")
        if rc == 0:
            op["status"] = "completed"
            op["message"] = op.get("message") or "Setup is live."
        else:
            op["status"] = "failed"
            op["message"] = op.get("message") or "Switch failed — see log."
        op["finished_at"] = op.get("finished_at") or _now_iso()
        return op


def active_operation() -> dict | None:
    with _ops_lock:
        data = _load_ops()
        for op in data.get("operations", {}).values():
            op = _refresh_op(op)
            if op.get("status") == "running":
                data["operations"][op["id"]] = op
                _save_ops(data)
                return op
        _save_ops(data)
    return None


def get_operation(op_id: str) -> dict | None:
    with _ops_lock:
        data = _load_ops()
        op = data.get("operations", {}).get(op_id)
        if not op:
            return None
        op = _refresh_op(op)
        data["operations"][op_id] = op
        _save_ops(data)
        return op


def _read_saved_state() -> dict:
    if not STATE_JSON.is_file():
        return {}
    try:
        return json.loads(STATE_JSON.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def classify(probes: dict[str, bool]) -> str:
    """Map endpoint booleans to a preset key (or mixed/none)."""
    ds4f = probes.get("ds4f", False)
    qwen38 = probes.get("qwen38", False)
    dream = probes.get("dream", False)
    helper = probes.get("helper", False)
    h3 = probes.get("h3", False)
    music = probes.get("music", False)
    if h3 and (ds4f or qwen38 or dream):
        return "mixed"
    if h3:
        return "video"
    if qwen38:
        return "qwen38"
    if dream:
        return "dream"
    if ds4f:
        return "prime"
    if helper and music:
        return "music"
    if helper:
        return "setup"
    return "none"


def _probes_now() -> dict[str, bool]:
    targets = {
        "ds4f": "http://127.0.0.1:8888/v1/models",
        "helper": "http://127.0.0.1:8889/v1/models",
        "h3": f"{H3_FABRIC}/v1/models",
        "music": "http://127.0.0.1:8801/v1/models",
        "vision": "http://127.0.0.1:8890/v1/models",
        "vision_n2": "http://192.168.100.11:8891/v1/models",
        "qwen_gguf": "http://192.168.100.11:8100/v1/models",
    }
    found: dict[str, bool] = {k: False for k in targets}
    with ThreadPoolExecutor(max_workers=8) as pool:
        fut = {pool.submit(_probe, url): key for key, url in targets.items()}
        for f in as_completed(fut):
            found[fut[f]] = bool(f.result())
    found["vision"] = found["vision"] or found.pop("vision_n2")
    ids = _model_ids("http://127.0.0.1:8888/v1/models")
    found["qwen38"] = any("qwen38-27b-unsloth-nvfp4" in i for i in ids)
    found["ds4f"] = any("deepseek" in i for i in ids) and not found["qwen38"]
    n2_ids = _model_ids("http://192.168.100.11:8100/v1/models")
    found["dream"] = bool(found["ds4f"] and any("qwen3.8-27b" in i.lower() for i in n2_ids))
    found.pop("qwen_gguf", None)
    return found


def detect_stack(*, force: bool = False) -> dict:
    """Cached probe of the three setups. Safe to call from /api/overview."""
    global _detect_cache
    now = time.time()
    with _detect_lock:
        if not force and _detect_cache and (now - _detect_cache[0]) < _DETECT_TTL:
            return dict(_detect_cache[1])
    probes = _probes_now()
    detected = classify(probes)
    saved = _read_saved_state()
    op = active_operation()
    busy = bool(op and op.get("status") == "running")
    payload = {
        "detected": detected,
        "desired": saved.get("desired") or detected,
        "phase": "switching" if busy else "idle",
        "message": (op or {}).get("message") or saved.get("message") or "",
        "updated_at": saved.get("updated_at"),
        "probes": probes,
        "active_operation": op,
        "presets": [
            {
                "key": key,
                **meta,
                "active": detected == key,
                "can_switch": (not busy) and detected != key and detected != "mixed",
            }
            for key, meta in PRESETS.items()
        ],
    }
    # mixed / none still allow leaving for a named preset
    if detected in ("mixed", "none"):
        for row in payload["presets"]:
            row["can_switch"] = not busy
            row["active"] = False
    with _detect_lock:
        _detect_cache = (time.time(), payload)
    return dict(payload)


def _stack_env() -> dict:
    """Console unit sets PORT=8085. DSpark start would steal that as vLLM port."""
    env = {**os.environ, "HOME": str(Path.home())}
    env.pop("PORT", None)
    env.pop("VLLM_PORT", None)
    return env


def switch_stack(key: str) -> dict:
    key = (key or "").strip().lower()
    if key not in PRESETS:
        return {"ok": False, "error": f"Unknown setup: {key}"}
    if not STACK_SCRIPT.is_file():
        return {"ok": False, "error": f"Missing {STACK_SCRIPT}"}

    try:
        from model_control import active_operation as model_op
        other = model_op()
        if other:
            return {
                "ok": False,
                "error": f"A model operation is already running ({other.get('type')} {other.get('key')}).",
            }
    except Exception:
        pass

    running = active_operation()
    if running:
        return {
            "ok": False,
            "error": f"Already switching to {running.get('key')}.",
            "operation": running,
        }

    current = detect_stack(force=True)
    if current.get("detected") == key:
        return {
            "ok": True,
            "already": True,
            "message": f"{PRESETS[key]['label']} is already live.",
            "stack": current,
        }

    op_id = uuid4().hex[:12]
    log_file = DATA_DIR / f"stack_op_{op_id}.log"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    meta = PRESETS[key]
    with open(log_file, "w") as logfh:
        proc = subprocess.Popen(
            ["bash", str(STACK_SCRIPT), key],
            stdout=logfh,
            stderr=subprocess.STDOUT,
            env=_stack_env(),
        )
    op = {
        "id": op_id,
        "type": "stack",
        "key": key,
        "status": "running",
        "message": f"Switching to {meta['label']}… ({meta['eta']})",
        "pid": proc.pid,
        "log_file": str(log_file),
        "started_at": _now_iso(),
        "finished_at": None,
        "returncode": None,
    }

    def _waiter() -> None:
        rc = proc.wait()
        with _ops_lock:
            data = _load_ops()
            cur = data["operations"].get(op_id, op)
            cur["returncode"] = rc
            cur["log_tail"] = _tail_file(log_file, 24)
            if rc == 0:
                cur["status"] = "completed"
                cur["message"] = f"{meta['label']} is live."
            else:
                cur["status"] = "failed"
                cur["message"] = f"{meta['label']} switch failed (exit {rc})."
            cur["finished_at"] = _now_iso()
            data["operations"][op_id] = cur
            _save_ops(data)
        detect_stack(force=True)

    threading.Thread(target=_waiter, daemon=True).start()
    with _ops_lock:
        data = _load_ops()
        data.setdefault("operations", {})[op_id] = op
        _save_ops(data)
    return {"ok": True, "operation": op}
