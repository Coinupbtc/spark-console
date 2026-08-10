"""Async model switch / stop controls for the DGX Spark dashboard."""
from __future__ import annotations

import json
import os
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from model_inventory import BUNDLE_DIR, query_inventory, _model_installed

DATA_DIR = Path(__file__).resolve().parent / "data"
OPS_FILE = DATA_DIR / "model_operations.json"
SWITCH_SCRIPT = Path.home() / ".hermes/scripts/switch-local-model.sh"
START_VLLM = BUNDLE_DIR / "start-vllm-model.sh"
SYNC_PROVIDERS = Path.home() / ".hermes/scripts/sync-vllm-providers.py"

# manifest key -> short switch key
KEY_ALIASES: dict[str, str] = {
    "qwen-nvfp4": "qwen",
    "gemma-nvfp4": "gemma",
    "ornith-35b": "ornith",
    "nemotron-nvfp4": "nemotron",
    "mistral-nvfp4": "mistral",
}

SWITCH_MODELS: dict[str, dict] = {
    "qwen": {
        "label": "Qwen 3.6 35B",
        "tier": "daily",
        "port": 8001,
        "dir": "qwen-nvfp4",
        "heavy": False,
        "hint": "Default daily driver — fast, safe on desktop.",
    },
    "gemma": {
        "label": "Gemma 4 26B",
        "tier": "light",
        "port": 8003,
        "dir": "gemma-nvfp4",
        "heavy": False,
        "hint": "Light writer model.",
    },
    "ornith": {
        "label": "Ornith 1.0 35B",
        "tier": "agent",
        "port": 8006,
        "dir": "ornith-35b",
        "heavy": False,
        "hint": "Agent/coding MoE — same class as Qwen.",
    },
    "nemotron": {
        "label": "Nemotron 3 Super",
        "tier": "frontier",
        "port": 8002,
        "dir": "nemotron-nvfp4",
        "heavy": True,
        "min_free_gb": 105,
        "hint": "Largest single-Spark model. Stop everything else first; 8K ctx only.",
    },
    "mistral": {
        "label": "Mistral Medium 3.5",
        "tier": "frontier",
        "port": 8005,
        "dir": "mistral-nvfp4",
        "heavy": True,
        "min_free_gb": 105,
        "hint": "Not downloaded — needs 2× Spark when available.",
    },
}

_ops_lock = threading.Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def mem_avail_gb() -> int | None:
    try:
        out = subprocess.run(
            ["free", "-g"],
            capture_output=True, text=True, timeout=5,
        )
        for line in out.stdout.splitlines():
            if line.startswith("Mem:"):
                parts = line.split()
                if len(parts) >= 7:
                    return int(parts[6])
    except (subprocess.TimeoutExpired, OSError, ValueError):
        pass
    return None


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


def _tail_file(path: Path, lines: int = 30) -> str:
    if not path.is_file():
        return ""
    try:
        content = path.read_text(errors="replace").splitlines()
        return "\n".join(content[-lines:])
    except OSError:
        return ""


def _refresh_op_status(op: dict) -> dict:
    pid = op.get("pid")
    if op.get("status") in ("completed", "failed", "cancelled"):
        return op
    if not pid:
        return op
    try:
        os.kill(pid, 0)
        op["status"] = "running"
        return op
    except OSError:
        log_path = Path(op.get("log_file", ""))
        tail = _tail_file(log_path, 40)
        op["log_tail"] = tail
        rc = op.get("returncode")
        if rc is None and log_path.is_file():
            # subprocess finished; infer from log
            if "READY on :" in tail or "✓" in tail:
                rc = 0
            elif "REFUSE:" in tail or "Server exited early" in tail or "Unknown model" in tail:
                rc = 1
        if rc == 0:
            op["status"] = "completed"
            op["message"] = op.get("message") or "Operation finished successfully."
        else:
            op["status"] = "failed"
            op["message"] = op.get("message") or "Operation failed — see log."
        op["finished_at"] = _now_iso()
        return op


def get_operation(op_id: str) -> dict | None:
    with _ops_lock:
        data = _load_ops()
        op = data["operations"].get(op_id)
        if not op:
            return None
        op = _refresh_op_status(op)
        data["operations"][op_id] = op
        _save_ops(data)
        return op


def list_operations(limit: int = 10) -> list[dict]:
    with _ops_lock:
        data = _load_ops()
        ops = []
        for op_id, op in data.get("operations", {}).items():
            op = _refresh_op_status(op)
            data["operations"][op_id] = op
            ops.append(op)
        _save_ops(data)
    ops.sort(key=lambda o: o.get("started_at", ""), reverse=True)
    return ops[:limit]


def _active_operation_unlocked(data: dict) -> dict | None:
    for op in data.get("operations", {}).values():
        if op.get("status") == "running":
            pid = op.get("pid")
            if pid:
                try:
                    os.kill(pid, 0)
                    return op
                except OSError:
                    pass
            else:
                return op
    return None


def active_operation() -> dict | None:
    with _ops_lock:
        return _active_operation_unlocked(_load_ops())


def _spawn_operation(op_type: str, key: str, cmd: list[str], message: str) -> dict:
    with _ops_lock:
        running = _active_operation_unlocked(_load_ops())
        if running:
            return {
                "ok": False,
                "error": f"Another operation is running: {running.get('type')} {running.get('key', '')}",
                "operation": running,
            }

    op_id = uuid.uuid4().hex[:12]
    log_file = DATA_DIR / f"model_op_{op_id}.log"
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with open(log_file, "w") as logfh:
        proc = subprocess.Popen(
            cmd,
            stdout=logfh,
            stderr=subprocess.STDOUT,
            cwd=str(BUNDLE_DIR),
            env={**os.environ, "HOME": str(Path.home())},
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
            cur["log_tail"] = _tail_file(log_file, 40)
            if rc == 0:
                cur["status"] = "completed"
                cur["message"] = cur.get("message") or "Done."
            else:
                cur["status"] = "failed"
                cur["message"] = cur.get("message") or f"Exited with code {rc}."
            cur["finished_at"] = _now_iso()
            data["operations"][op_id] = cur
            _save_ops(data)

    threading.Thread(target=_waiter, daemon=True).start()

    with _ops_lock:
        data = _load_ops()
        data.setdefault("operations", {})[op_id] = op
        _save_ops(data)

    return {"ok": True, "operation": op}


def _resolve_key(key: str) -> str | None:
    key = (key or "").strip().lower()
    if key in SWITCH_MODELS:
        return key
    if key in KEY_ALIASES:
        return KEY_ALIASES[key]
    return None


def can_switch(key: str) -> dict:
    resolved = _resolve_key(key)
    if not resolved:
        return {"ok": False, "error": f"Unknown model key: {key}"}

    meta = SWITCH_MODELS[resolved]
    dir_key = meta["dir"]
    installed = _model_installed(dir_key)
    avail = mem_avail_gb()
    inv = query_inventory()
    active = inv.get("active_vllm") or []
    running_key = active[0]["key"] if len(active) == 1 else None
    running_switch = KEY_ALIASES.get(running_key or "", running_key)

    blockers: list[str] = []
    if not installed:
        blockers.append(f"Not installed — download {dir_key} first.")
    if active_operation():
        blockers.append("Another dashboard operation is already running.")
    if meta.get("heavy"):
        need = meta.get("min_free_gb", 105)
        # If same model already loading on port, allow retry/wait
        if running_switch != resolved and (avail is None or avail < need):
            blockers.append(
                f"Needs ≥{need}GB free unified memory (currently {avail if avail is not None else '?'}GB). "
                "Stop the active model first."
            )

    return {
        "ok": not blockers,
        "key": resolved,
        "label": meta["label"],
        "installed": installed,
        "mem_avail_gb": avail,
        "heavy": meta.get("heavy", False),
        "running": running_switch,
        "blockers": blockers,
        "hint": meta.get("hint", ""),
    }


def switch_model(key: str, fast: bool = False) -> dict:
    check = can_switch(key)
    if not check.get("ok"):
        return {"ok": False, "error": "; ".join(check.get("blockers") or ["Cannot switch"]), **check}

    resolved = check["key"]
    if not SWITCH_SCRIPT.is_file():
        return {"ok": False, "error": f"Missing switch script: {SWITCH_SCRIPT}"}

    cmd = ["bash", str(SWITCH_SCRIPT), resolved]
    if fast and resolved == "qwen":
        cmd.append("--fast")

    return _spawn_operation(
        "switch",
        resolved,
        cmd,
        f"Switching to {check['label']}… (may take 10–15 min for large models)",
    )


def stop_models() -> dict:
    if not START_VLLM.is_file():
        return {"ok": False, "error": f"Missing start script: {START_VLLM}"}
    if active_operation():
        return {"ok": False, "error": "Cannot stop while a switch operation is running."}
    return _spawn_operation(
        "stop",
        "stop",
        ["bash", str(START_VLLM), "stop"],
        "Stopping all vLLM servers and freeing GPU memory…",
    )


def _vllm_port_healthy() -> bool:
    """True when any NVFP4 vLLM port is serving /v1/models."""
    for port in (8001, 8002, 8003, 8005, 8006):
        try:
            out = subprocess.run(
                ["curl", "-sf", "--max-time", "2", f"http://127.0.0.1:{port}/v1/models"],
                capture_output=True,
                timeout=5,
            )
            if out.returncode == 0:
                return True
        except (subprocess.TimeoutExpired, OSError):
            pass
    return False


def kill_orphans() -> dict:
    """Kill VLLM::EngineCore only when no healthy api_server is running."""
    try:
        if _vllm_port_healthy():
            return {
                "ok": True,
                "killed": 0,
                "message": "vLLM server is responding on a local port — use Stop vLLM instead of Kill Orphans.",
            }
        api = subprocess.run(
            ["pgrep", "-af", r"python.*vllm\.entrypoints\.openai\.api_server"],
            capture_output=True, text=True, timeout=5,
        )
        if api.stdout.strip():
            return {
                "ok": True,
                "killed": 0,
                "message": "vLLM server process is running (still loading?) — wait or use Stop vLLM.",
            }
        out = subprocess.run(
            ["pgrep", "-af", "VLLM::EngineCore"],
            capture_output=True, text=True, timeout=5,
        )
        if not out.stdout.strip():
            return {"ok": True, "killed": 0, "message": "No orphaned EngineCore processes."}
        subprocess.run(["pkill", "-f", "VLLM::EngineCore"], timeout=5)
        time.sleep(1)
        subprocess.run(["pkill", "-9", "-f", "VLLM::EngineCore"], timeout=5)
        return {
            "ok": True,
            "killed": len(out.stdout.strip().splitlines()),
            "message": "Killed orphaned VLLM::EngineCore process(es).",
            "before": out.stdout.strip(),
        }
    except (subprocess.TimeoutExpired, OSError) as exc:
        return {"ok": False, "error": str(exc)}


def unload_ollama() -> dict:
    try:
        ps = subprocess.run(
            ["ollama", "ps", "--format", "{{.Name}}"],
            capture_output=True, text=True, timeout=10,
        )
        names = [n.strip() for n in ps.stdout.splitlines() if n.strip()]
        stopped = []
        for name in names:
            subprocess.run(["ollama", "stop", name], capture_output=True, timeout=30)
            stopped.append(name)
        return {
            "ok": True,
            "stopped": stopped,
            "message": f"Unloaded {len(stopped)} Ollama model(s)." if stopped else "No Ollama models loaded.",
        }
    except (subprocess.TimeoutExpired, OSError) as exc:
        return {"ok": False, "error": str(exc)}


def sync_hermes() -> dict:
    if not SYNC_PROVIDERS.is_file():
        return {"ok": False, "error": f"Missing sync script: {SYNC_PROVIDERS}"}
    return _spawn_operation(
        "sync",
        "sync",
        ["python3", str(SYNC_PROVIDERS)],
        "Syncing Hermes vLLM provider configs…",
    )


def control_status() -> dict:
    inv = query_inventory()
    avail = mem_avail_gb()
    active = active_operation()
    models = []
    for key, meta in SWITCH_MODELS.items():
        check = can_switch(key)
        models.append({
            "key": key,
            "label": meta["label"],
            "tier": meta["tier"],
            "port": meta["port"],
            "heavy": meta.get("heavy", False),
            "installed": check.get("installed", False),
            "can_switch": check.get("ok", False),
            "blockers": check.get("blockers", []),
            "hint": meta.get("hint", ""),
            "status": next(
                (m["status"] for m in inv.get("models", []) if KEY_ALIASES.get(m["key"]) == key or m["key"] == meta["dir"]),
                "missing",
            ),
        })
    return {
        "mem_avail_gb": avail,
        "mem_total_gb": 121,
        "active_operation": active,
        "active_vllm": inv.get("active_vllm") or [],
        "models": models,
        "switch_script": str(SWITCH_SCRIPT),
    }