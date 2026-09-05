"""Named Spark stack switcher for the console.

TP2 chips (ds4f / flashnext / glm53keys / video) still call
~/scripts/dgx/spark-stack.sh and consume both Sparks.

TP1 occupants (flash1 / music3 / h3) load on one node via
~/scripts/dgx/spark-occupy.sh. Telegram chat is derived from occupancy.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
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

# UI copy — keep TP2 keys in lockstep with spark-stack.sh
# Music3 is a TP1 occupant (pair it with Qwen Flash on the other Spark).
PRESETS: dict[str, dict] = {
    "ds4f": {
        "label": "DeepSeek Vision",
        "short": "Vision-Exp DSpark · 1M · native images",
        "detail": "Mia DeepSeek-V4-Flash-Vision-Exp on both Sparks (TP2, 1M, native OpenAI image_url). No 4B sidecar. Chat stays local on :8888.",
        "eta": "10–20 min",
        "stops": "Music3, MiniMax H3, Flash-Next, GLM",
        "starts": "Vision-Exp :8888 (deepseek-v4-flash-vision-exp)",
    },
    "flashnext": {
        "label": "Qwen3.8-Flash",
        "short": "Flash-Next vLLM TP2 · 1M",
        "detail": "Qwen3.8-Flash-Next-NVFP4 on both Sparks (vLLM TP2+EP+MTP3, YaRN 1M, bf16 KV). Parks DeepSeek Vision. Chat stays local on :8888. First cold load ~10–20 min.",
        "eta": "10–20 min",
        "stops": "DeepSeek Vision, GLM, Music3, H3",
        "starts": "vllm-fn :8888 TP2 (qwen38-flash-next)",
    },
    "glm53keys": {
        "label": "GLM-5.3",
        "short": "Mia EXL3 · DFlash2 · 1.05M · orch+dobby",
        "detail": "Mia GLM-5.3-Flash EXL3 4bpw + DFlash2 on both Sparks (vLLM TP2, fp8 KV, 1.05M). Parks Flash-Next and DeepSeek Vision. Chat is orch+dobby on :8888 — light/smeagle/freegle stay parked. First boot downloads ~164 GiB.",
        "eta": "15–30 min",
        "stops": "DeepSeek Vision, Flash-Next, Music3, H3",
        "starts": "glm53-exl3-head :8888 GLM-5.3-Flash-EXL3 DFlash2",
    },
    "video": {
        "label": "Videos",
        "short": "MiniMax H3 TP2 RoCE",
        "detail": "MiniMax H3 on both Sparks over CX7 RoCEv2 (GID 3, matched 1500 MTU, unlimited memlock). Same-quality 20-step 768×448 measured 55.5s vs 90.5s Socket. Telegram chat moves to Nous until you leave this setup.",
        "eta": "10–15 min",
        "stops": "DS4F, Music3, vision sidecar",
        "starts": "H3 :8800 RoCE IB · chat → Freegle",
    },
}

OCCUPY_SCRIPT = Path.home() / "scripts/dgx/spark-occupy.sh"

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
    if qwen38 or twins or dream or helper:
        # Retired chips (Twins / Dream / 35B / exclusive NVFP4) are leftovers.
        return "mixed"
    if glm53keys:
        return "glm53keys"
    if flashnext:
        return "flashnext"
    if ds4f:
        return "ds4f"
    if music:
        # Music3 is TP1; exclusive helper+music setup is gone.
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
    found["flashnext"] = any("qwen38-flash-next" in i or "qwen3.8-flash-next" in i for i in ids)
    # TP1 recipe serves the same model id from vllm-fn-tp1 — do not light the TP2 chip.
    try:
        names = subprocess.check_output(
            ["docker", "ps", "--format", "{{.Names}}"],
            text=True,
            timeout=2,
            stderr=subprocess.DEVNULL,
        )
        tp1_n1 = "vllm-fn-tp1" in names.split()
    except (subprocess.CalledProcessError, FileNotFoundError, OSError):
        tp1_n1 = False
    if tp1_n1:
        found["flashnext"] = False
        found["flash1"] = True
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


def _occupancy_mod():
    scripts = Path.home() / "scripts" / "dgx"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))
    import spark_occupancy as occ  # type: ignore

    return occ


def occupancy_view(detected: str) -> dict:
    """Who is on which Spark + who talks on Telegram."""
    occ = _occupancy_mod()
    state = occ.load_state()
    if not state.get("tp2") and not state.get("n1") and not state.get("n2"):
        state = occ.from_detected_tp2(detected)
        occ.apply_chat(state)
    else:
        occ.apply_chat(state)
    cat = occ.catalog()
    n1, n2 = state.get("n1"), state.get("n2")
    tp1 = []
    for row in cat["tp1"]:
        key = row["key"]
        on = None
        if n1 == key:
            on = "n1"
        elif n2 == key:
            on = "n2"
        tp1.append({**row, "active_on": on, "active": on is not None})
    return {
        "mode": state.get("mode"),
        "tp2": state.get("tp2"),
        "n1": n1,
        "n2": n2,
        "chat": state.get("chat"),
        "chat_label": state.get("chat_label") or occ.pick_chat(state)["chat_label"],
        "tp1": tp1,
    }


def recommend_tp1(key: str) -> dict:
    occ = _occupancy_mod()
    detected = classify(_probes_now())
    state = occupancy_view(detected)
    occ_state = occ.load_state()
    if not occ_state.get("tp2") and not occ_state.get("n1") and not occ_state.get("n2"):
        occ_state = occ.from_detected_tp2(detected)
    rec = occ.recommend(key, occ_state)
    rec["occupancy"] = {
        "n1": state.get("n1"),
        "n2": state.get("n2"),
        "tp2": state.get("tp2"),
        "chat_label": state.get("chat_label"),
    }
    return rec


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
    occu = occupancy_view(detected)
    payload["lane"] = "tp2" if occu.get("tp2") else ("tp1-mix" if occu.get("n1") or occu.get("n2") else "empty")
    payload["occupancy"] = occu
    payload["chat"] = {"key": occu.get("chat"), "label": occu.get("chat_label")}
    payload["tp1"] = occu.get("tp1") or []
    for row in payload["presets"]:
        row["lane"] = "tp2"
        row["consumes"] = "both Sparks"
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


def occupy_tp1(key: str, node: str) -> dict:
    """Load a TP1 occupant onto n1 or n2. Parks TP2 first if one is live."""
    key = (key or "").strip().lower()
    node = (node or "").strip().lower()
    if node in ("1", "local", "this"):
        node = "n1"
    if node in ("2", "spark2", "peer"):
        node = "n2"
    rec = recommend_tp1(key)
    if not rec.get("ok"):
        return rec
    if node == "both":
        occ = _occupancy_mod()
        if not (occ.TP1.get(key) or {}).get("replica_pair"):
            return {
                "ok": False,
                "error": "Both Sparks here is two copies, not a faster pair. Use Videos TP2 for one-clip speed. Music has no tensor-parallel one-song mode.",
            }
    else:
        view = (rec.get("nodes") or {}).get(node) or {}
        if view.get("disabled"):
            return {"ok": False, "error": view.get("reason") or f"{key} cannot load on {node}"}
        if node not in ("n1", "n2"):
            return {"ok": False, "error": "Pick Node 1, Node 2, or Both (two copies)"}
    if not OCCUPY_SCRIPT.is_file():
        return {"ok": False, "error": f"Missing {OCCUPY_SCRIPT}"}

    running = active_operation()
    if running:
        return {"ok": False, "error": f"Already switching ({running.get('key')}).", "operation": running}
    ext = external_switch_busy()
    if ext:
        return {
            "ok": False,
            "error": ext.get("message") or "Another switch is already running.",
            "external_switch": ext,
        }

    op_id = uuid4().hex[:12]
    log_file = DATA_DIR / f"stack_op_{op_id}.log"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    label = rec.get("label") or key
    with open(log_file, "w") as logfh:
        proc = subprocess.Popen(
            ["bash", str(OCCUPY_SCRIPT), "load", key, "--node", node],
            stdout=logfh,
            stderr=subprocess.STDOUT,
            env=_stack_env(),
        )
    op = {
        "id": op_id,
        "type": "occupy",
        "key": key,
        "node": node,
        "status": "running",
        "message": f"Loading {label} on {node}… ({rec.get('eta') or 'several min'})",
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
                cur["message"] = f"{label} live on {node}."
            else:
                cur["status"] = "failed"
                cur["message"] = f"{label} on {node} failed (exit {rc})."
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
