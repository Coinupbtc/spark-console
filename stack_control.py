"""Named Spark stack switcher for the console (prime / dream / twins / flashnext / glm53keys / video / music).

The heavy lifting lives in ~/scripts/dgx/spark-stack.sh — this module is the
allowlisted API: detect what's up, spawn one switch, poll the log.

Retired 2026-08-15: exclusive qwen38 NVFP4 chip and helper-only setup chip.
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
STACK_LOCK = Path(os.environ.get("XDG_RUNTIME_DIR", "/tmp")) / "spark-stack.lock"
H3_FABRIC = os.environ.get("H3_API_BASE", "http://192.168.100.10:8800").rstrip("/")

# UI copy — keep keys in lockstep with spark-stack.sh
PRESETS: dict[str, dict] = {
    "prime": {
        "label": "Prime",
        "short": "DS4F 0731 · 500k · vision",
        "detail": "DeepSeek-V4-Flash 0731 on both Sparks (TP2) with the Qwen vision sidecar. Chat stays local.",
        "eta": "10–15 min",
        "stops": "Music3, helper 35B, MiniMax H3",
        "starts": "DS4F :8888 + vision :8890",
    },
    "dream": {
        "label": "Dream",
        "short": "0731 348k · Qwen VL 116k (95k+20k) · baton",
        "detail": "Both Sparks: 0731 TP2 348k. Node2: Qwen 27B GGUF+mmproj 116k engine (Hermes prompt ~95k + 20k out share that slot; baton clamps max_tokens). No 4B sidecar. Chat = baton :8877.",
        "eta": "10–20 min",
        "stops": "Music3, helper 35B, MiniMax H3, 4B vision sidecar",
        "starts": "DS4F :8888 + Qwen VL 192.168.100.11:8100 (never node1 :8100) + baton :8877",
    },
    "twins": {
        "label": "Qwen Twins",
        "short": "Mia SGLang DSpark NVFP4 ×2",
        "detail": "Park Dream/DS4F. MiaAI Qwen3.8-27B-SGLang-DGX-Spark (DSpark NVFP4, 262k) — one replica per Spark on :8888. Orch=n1, smeagle=n2. First boot may pull ~25GB.",
        "eta": "10–20 min",
        "stops": "DS4F 0731, GGUF Qwen, baton, Music3, H3, 4B sidecar",
        "starts": "SGLang qwen3.8-27b-sglang n1+n2 :8888",
    },
    "flashnext": {
        "label": "Qwen3.8-Flash",
        "short": "Flash-Next FP8 TP2 · 131k",
        "detail": "Qwen3.8-Flash-Next-FP8 on both Sparks (SGLang TP2/EP2). Parks Dream/0731. Chat stays local on :8888. First cold load ~10–20 min.",
        "eta": "10–20 min",
        "stops": "DS4F 0731, Dream Qwen, Twins, baton, Music3, H3, 4B sidecar",
        "starts": "qwen38-flash-next :8888 TP2",
    },
    "glm53keys": {
        "label": "GLM-5.3",
        "short": "Tony DFlash2 · fp8 KV · 262k",
        "detail": "Tony GLM-5.3-Flash NVFP4 + DFlash2 on both Sparks (vLLM TP2, fp8 KV, 262k). Parks Flash-Next and Dream. Chat stays local on :8888. Cold load ~15–30 min.",
        "eta": "15–30 min",
        "stops": "DS4F 0731, Dream Qwen, Flash-Next, Twins, baton, Music3, H3, 4B sidecar",
        "starts": "glm53_nvfp4_tp2 :8888 glm-5.3-flash-nvfp4 DFlash2",
    },
    "video": {
        "label": "Videos",
        "short": "MiniMax H3 TP2",
        "detail": "MiniMax H3 on both Sparks for video. Telegram chat moves to Nous until you leave this setup.",
        "eta": "10–15 min",
        "stops": "DS4F, Music3, vision sidecar",
        "starts": "H3 :8800 · chat → Nous",
    },
    "music": {
        "label": "Music",
        "short": "Helper 35B · Music3",
        "detail": "Qwen 35B chat on this Spark plus AIM Music3 (and the spark2 replica). Stops Dream Qwen on n2 :8100. Vision sidecar stays off. Nous :free stays on Freegle only.",
        "eta": "10–15 min",
        "stops": "DS4F, MiniMax H3, n2 Qwen 3.8, vision sidecar",
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
    """True when /v1/models or /health answers with JSON. Never raises."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace")[:8000])
        if not isinstance(data, dict):
            return False
        return bool(
            data.get("data")
            or data.get("id")
            or data.get("object")
            or data.get("ok") is True
            or data.get("qwen")
        )
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


def _lock_held(path: Path) -> bool:
    """True when another process has flock on spark-stack.lock (CLI / remake / console)."""
    import fcntl

    if not path.is_file():
        return False
    try:
        fd = os.open(path, os.O_RDWR)
    except OSError:
        return False
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    except BlockingIOError:
        return True
    except OSError:
        return False
    finally:
        os.close(fd)


def _pgrep_stack() -> list[int]:
    try:
        out = subprocess.check_output(
            ["pgrep", "-f", r"bash .*/spark-stack\.sh"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except (subprocess.CalledProcessError, FileNotFoundError, OSError):
        return []
    pids = []
    for line in out.split():
        try:
            pids.append(int(line))
        except ValueError:
            continue
    return pids


def external_switch_busy() -> dict | None:
    """spark-stack.sh running outside this console (remake, cron, CLI)."""
    saved = _read_saved_state()
    pids = _pgrep_stack()
    held = _lock_held(STACK_LOCK)
    if not held and not pids:
        return None
    desired = saved.get("desired") or "a setup"
    msg = saved.get("message") or f"spark-stack is already switching ({desired})"
    return {
        "busy": True,
        "desired": desired,
        "message": msg,
        "pids": pids,
        "lock_held": held,
    }


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
    twins = probes.get("twins", False)
    flashnext = probes.get("flashnext", False)
    glm53keys = probes.get("glm53keys", False)
    helper = probes.get("helper", False)
    h3 = probes.get("h3", False)
    music = probes.get("music", False)
    if h3 and (ds4f or qwen38 or dream or twins or flashnext or glm53keys):
        return "mixed"
    if h3:
        return "video"
    if qwen38:
        # Retired exclusive chip — leftover NVFP4 is not a preset.
        return "mixed"
    if glm53keys:
        return "glm53keys"
    if flashnext:
        return "flashnext"
    if twins:
        return "twins"
    if dream:
        return "dream"
    if ds4f:
        return "prime"
    if helper and music:
        return "music"
    if helper:
        return "none"
    return "none"


def _probes_now() -> dict[str, bool]:
    targets = {
        "ds4f": "http://127.0.0.1:8888/v1/models",
        "helper": "http://127.0.0.1:8889/v1/models",
        "h3": f"{H3_FABRIC}/v1/models",
        "music": "http://127.0.0.1:8801/v1/models",
        "vision_proxy": "http://127.0.0.1:8890/v1/models",
        "vision_n1": "http://127.0.0.1:8891/v1/models",
        "vision_n2": "http://192.168.100.11:8891/v1/models",
        "qwen_n2": "http://192.168.100.11:8100/v1/models",
        "qwen_twin_n2": "http://192.168.100.11:8888/v1/models",
        "baton": "http://127.0.0.1:8877/health",
    }
    found: dict[str, bool] = {k: False for k in targets}
    with ThreadPoolExecutor(max_workers=8) as pool:
        fut = {pool.submit(_probe, url): key for key, url in targets.items()}
        for f in as_completed(fut):
            found[fut[f]] = bool(f.result())
    ids = _model_ids("http://127.0.0.1:8888/v1/models")
    found["qwen38"] = any("qwen38-27b-unsloth-nvfp4" in i for i in ids)
    found["flashnext"] = any("qwen38-flash-next" in i for i in ids)
    found["glm53keys"] = any("glm-5.3-flash" in i for i in ids)
    found["ds4f"] = any("deepseek" in i for i in ids) and not found["qwen38"]
    n2_ids = _model_ids("http://192.168.100.11:8100/v1/models")
    n2_8888 = _model_ids("http://192.168.100.11:8888/v1/models")
    found["dream"] = bool(found["ds4f"] and any("qwen3.8-27b" in i.lower() for i in n2_ids))
    gguf_n1 = any("qwen3.8-27b" in i.lower() or i == "qwen3.8-27b" for i in ids) and not found["qwen38"] and not found["flashnext"] and not found["glm53keys"]
    gguf_n2 = any("qwen3.8-27b" in i.lower() for i in n2_8888)
    found["twins"] = bool(gguf_n1 and gguf_n2 and not found["ds4f"] and not found["flashnext"] and not found["glm53keys"])
    # Do not OR n1/n2 into one "vision" bit — Dream is n1, Prime is n2.
    found["vision"] = bool(found["vision_proxy"])
    # Keep qwen_n2 + baton in probes so the console never confuses
    # node1 127.0.0.1:8100 (usually empty) with Dream Qwen on node2.
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
    ext = external_switch_busy()
    busy = bool(op and op.get("status") == "running") or bool(ext)
    message = (op or {}).get("message") or (ext or {}).get("message") or saved.get("message") or ""
    payload = {
        "detected": detected,
        "desired": saved.get("desired") or detected,
        "phase": "switching" if busy else "idle",
        "message": message,
        "updated_at": saved.get("updated_at"),
        "probes": probes,
        "active_operation": op,
        "external_switch": ext,
        "presets": [
            {
                "key": key,
                **meta,
                "active": detected == key,
                # Re-click refreshes that setup's own wiring (Dream 88k / n1 4B).
                "can_switch": (not busy) and detected != "mixed",
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

    ext = external_switch_busy()
    if ext:
        return {
            "ok": False,
            "error": ext.get("message")
            or "Another spark-stack switch is already running. Wait until Setup is idle.",
            "external_switch": ext,
        }

    current = detect_stack(force=True)
    refreshing = current.get("detected") == key

    op_id = uuid4().hex[:12]
    log_file = DATA_DIR / f"stack_op_{op_id}.log"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    meta = PRESETS[key]
    verb = "Refreshing" if refreshing else "Switching to"
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
        "message": f"{verb} {meta['label']}… ({meta['eta'] if not refreshing else 'refresh'})",
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
