"""Spark Console LoRA Train — extract QLoRA on Qwen 3.8.

Not a named setup. Lives under Control → Setup. Occupancy: 0731 :8888 must
be down (or the operator confirms stop). Does not restart Dream or wire :8100.
"""
from __future__ import annotations

import json
import os
import subprocess
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

HOME = Path.home()
DATA_DIR = Path(__file__).resolve().parent / "data"
OPS_FILE = DATA_DIR / "lora_operations.json"
LAB = HOME / "Documents/projects/spark-training-lab"
STATUS_JSON = LAB / "datasets/improve/STATUS.json"
ADAPTER = LAB / "adapters/qwen38-extract-v1/adapter_config.json"
BENCH_STOCK = LAB / "runs/bench_extract_qwen38_stock.json"
BENCH_LORA = LAB / "runs/bench_extract_qwen38_lora.json"
TRAIN_SH = LAB / "scripts/console-lora-train.sh"
ADMIT_SH = HOME / ".hermes/scripts/heavy-job-admit.sh"
STACK_LOCK = Path(os.environ.get("XDG_RUNTIME_DIR", "/tmp")) / "spark-stack.lock"

_ops_lock = threading.Lock()
_cache_lock = threading.Lock()
_status_cache: tuple[float, dict] | None = None
_CACHE_TTL = 6.0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _probe(url: str, timeout: float = 2.0) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            json.loads(resp.read().decode("utf-8", errors="replace")[:4000])
        return True
    except Exception:
        return False


def _read_json(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _mem() -> tuple[int, int]:
    """Return (available_g, swap_used_g) from free -g."""
    try:
        out = subprocess.check_output(["free", "-g"], text=True, timeout=3)
    except (OSError, subprocess.SubprocessError):
        return 0, 99
    avail = swap = 0
    for line in out.splitlines():
        parts = line.split()
        if parts and parts[0] == "Mem:" and len(parts) >= 7:
            avail = int(parts[6])
        elif parts and parts[0] == "Swap:" and len(parts) >= 3:
            swap = int(parts[2])
    return avail, swap


def _admit_ok() -> tuple[bool, str]:
    if not ADMIT_SH.is_file():
        avail, swap = _mem()
        ok = avail >= 40 and swap <= 8
        return ok, f"avail {avail}G swap {swap}G (no admit script)"
    try:
        r = subprocess.run(
            ["bash", str(ADMIT_SH), "check", "--min-free-g", "40",
             "--max-swap-g", "8", "--label", "improve-train"],
            capture_output=True, text=True, timeout=8,
        )
        msg = (r.stderr or r.stdout or "").strip().splitlines()[-1:] or [""]
        return r.returncode == 0, msg[0][:200]
    except (OSError, subprocess.SubprocessError) as e:
        return False, str(e)[:200]


def _lock_busy() -> bool:
    if not STACK_LOCK.exists():
        return False
    try:
        import fcntl
        with open(STACK_LOCK, "a+") as fh:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                return False
            except OSError:
                return True
    except OSError:
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
        op["log_tail"] = _tail_file(Path(op.get("log_file") or ""), 20)
        return op
    except OSError:
        log_path = Path(op.get("log_file") or "")
        tail = _tail_file(log_path, 32)
        op["log_tail"] = tail
        rc = op.get("returncode")
        if rc == 0:
            op["status"] = "completed"
            op["message"] = op.get("message") or "LoRA train finished."
        else:
            op["status"] = "failed"
            op["message"] = op.get("message") or "LoRA train failed — see log."
        op["finished_at"] = op.get("finished_at") or _now_iso()
        return op


def active_operation() -> dict | None:
    with _ops_lock:
        data = _load_ops()
        running = None
        for op in data.get("operations", {}).values():
            op = _refresh_op(op)
            data["operations"][op["id"]] = op
            if op.get("status") == "running":
                running = op
        _save_ops(data)
        return running


def last_operation() -> dict | None:
    with _ops_lock:
        data = _load_ops()
        ops = list(data.get("operations", {}).values())
        if not ops:
            return None
        ops.sort(key=lambda o: o.get("started_at") or "", reverse=True)
        op = _refresh_op(ops[0])
        data["operations"][op["id"]] = op
        _save_ops(data)
        return op


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


def can_start(*, stop_0731: bool, ds4f: bool, train_n: int, admit: bool,
              busy: bool, lock: bool, dual: bool = True) -> tuple[bool, str]:
    if busy:
        return False, "a LoRA train is already running"
    if lock:
        return False, "a setup switch is running — wait"
    if train_n < 100:
        return False, f"gold too small ({train_n}) — harvest first"
    if ds4f and not stop_0731:
        return False, "0731 is LIVE — confirm Stop 0731 + Train"
    if not admit:
        return False, "RAM admit refused (≥40G free, swap ≤8G)"
    if dual:
        if ds4f and stop_0731:
            return True, "will stop 0731 + :8100 then dual QLoRA ~2–4h; Dream does not auto-return"
        return True, "dual QLoRA both Sparks ~2–4h; stops :8100; does not wire adapter"
    if ds4f and stop_0731:
        return True, "will stop 0731 then train n1 ~4–8h; Dream does not auto-return"
    return True, "n1-only QLoRA; :8100 stays; does not wire adapter"


def status() -> dict:
    global _status_cache
    now = time.time()
    with _cache_lock:
        if _status_cache and (now - _status_cache[0]) < _CACHE_TTL:
            return dict(_status_cache[1])
    gold = _read_json(STATUS_JSON)
    train_n = int(gold.get("train_n") or 0)
    holdout_n = int(gold.get("holdout_n") or 0)
    ds4f = _probe("http://127.0.0.1:8888/v1/models")
    qwen = _probe("http://192.168.100.11:8100/v1/models")
    avail, swap = _mem()
    admit, admit_msg = _admit_ok()
    busy_op = active_operation()
    busy = bool(busy_op and busy_op.get("status") == "running")
    lock = _lock_busy()
    ok, reason = can_start(
        stop_0731=False, ds4f=ds4f, train_n=train_n, admit=admit,
        busy=busy, lock=lock,
    )
    # 0731 holding RAM is expected — admit after stop. Gold + not-busy is enough.
    ok_stop, reason_stop = can_start(
        stop_0731=True, ds4f=ds4f, train_n=train_n,
        admit=True if ds4f else admit,
        busy=busy, lock=lock,
    )
    stock = _read_json(BENCH_STOCK)
    lora = _read_json(BENCH_LORA)
    payload = {
        "skill": gold.get("skill") or "card_listing_json_extract",
        "student": gold.get("student") or "Qwen3.8-27B",
        "train_n": train_n,
        "holdout_n": holdout_n,
        "ready_gold": train_n >= 100,
        "ds4f_up": ds4f,
        "qwen_up": qwen,
        "avail_g": avail,
        "swap_g": swap,
        "admit_ok": admit,
        "admit_msg": admit_msg,
        "adapter": ADAPTER.is_file(),
        "stock_acc": stock.get("field_acc"),
        "lora_acc": lora.get("field_acc"),
        "can_train": ok,
        "can_stop_and_train": ok_stop,
        "reason": reason if not ok else reason_stop if ds4f else reason,
        "eta": "2–4 hours both Sparks",
        "eta_single": "4–8 hours n1 only",
        "dual_default": True,
        "wired_8100": False,
        "warn": (
            "Default is both Sparks (torchrun DDP). Stops 0731 if up and stops :8100. "
            "Does not bring Dream back. Does not load the adapter on :8100. "
            "Discord #training pings when done."
        ),
        "active_operation": busy_op,
        "last_operation": busy_op or last_operation(),
        "harvested_at": gold.get("harvested_at"),
    }
    with _cache_lock:
        _status_cache = (time.time(), payload)
    return dict(payload)


def start(*, stop_0731: bool = False, dual: bool = True) -> dict:
    st = status()
    ok, reason = can_start(
        stop_0731=stop_0731,
        ds4f=bool(st["ds4f_up"]),
        train_n=int(st["train_n"]),
        admit=bool(st["admit_ok"]) or (stop_0731 and st["ds4f_up"]),
        busy=bool(st.get("active_operation")),
        lock=_lock_busy(),
        dual=dual,
    )
    # After stopping 0731, admit may currently fail because 0731 still holds RAM.
    # Allow stop_0731 when gold is ready and not otherwise busy; admit re-checked
    # inside improve-train.sh after :8888 is down.
    if st["ds4f_up"] and stop_0731 and st["ready_gold"] and not st.get("active_operation") and not _lock_busy():
        ok, reason = True, (
            "will stop 0731 + :8100 then dual QLoRA ~2–4h; Dream does not auto-return"
            if dual else
            "will stop 0731 then train n1 ~4–8h; Dream does not auto-return"
        )
    if not ok:
        return {"ok": False, "error": reason, "status": st}
    if not TRAIN_SH.is_file():
        return {"ok": False, "error": f"missing {TRAIN_SH}"}

    op_id = uuid4().hex[:12]
    log_file = DATA_DIR / f"lora_op_{op_id}.log"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "HOME": str(HOME), "PYTHONUNBUFFERED": "1",
           "LORA_STOP_0731": "1" if stop_0731 else "0",
           "LORA_DUAL": "1" if dual else "0"}
    env.pop("PORT", None)
    with open(log_file, "w") as logfh:
        proc = subprocess.Popen(
            ["bash", str(TRAIN_SH)],
            stdout=logfh, stderr=subprocess.STDOUT, env=env,
        )
    if dual:
        label = "Stop 0731 + dual LoRA" if stop_0731 else "Dual LoRA (both Sparks)"
        eta = st.get("eta") or "2–4 hours both Sparks"
    else:
        label = "Stop 0731 + n1 LoRA" if stop_0731 else "n1 LoRA"
        eta = st.get("eta_single") or "4–8 hours n1 only"
    op = {
        "id": op_id,
        "type": "lora",
        "status": "running",
        "message": f"{label} started ({eta})",
        "pid": proc.pid,
        "log_file": str(log_file),
        "stop_0731": bool(stop_0731),
        "dual": bool(dual),
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
            cur["log_tail"] = _tail_file(log_file, 32)
            if rc == 0:
                cur["status"] = "completed"
                cur["message"] = "LoRA train finished. Rematch holdout before wiring :8100."
            else:
                cur["status"] = "failed"
                cur["message"] = f"LoRA train failed (exit {rc})."
            cur["finished_at"] = _now_iso()
            data["operations"][op_id] = cur
            _save_ops(data)
        with _cache_lock:
            _status_cache = None

    threading.Thread(target=_waiter, daemon=True).start()
    with _ops_lock:
        data = _load_ops()
        data.setdefault("operations", {})[op_id] = op
        _save_ops(data)
    with _cache_lock:
        _status_cache = None
    return {"ok": True, "operation": op, "reason": reason}
