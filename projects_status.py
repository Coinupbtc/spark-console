#!/usr/bin/env python3
"""
Project status tiles + todo list for the Spark Console.

Projects: read-only, cheap checks (file mtimes, ports, systemd states) — no
side effects, no state mutation, safe to run every minute.
Todos: plain JSON file at data/todos.json (user-approved store, 2026-07-16).
"""
from __future__ import annotations

import json
import os
import re
import socket
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone

HOME = os.path.expanduser("~")
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
TODO_FILE = os.path.join(DATA_DIR, "todos.json")
_todo_lock = threading.Lock()

POKEMON_LOG = f"{HOME}/Documents/projects/pokemon-arb/logs/cron_scan.log"
CRYPTO_CYCLES = f"{HOME}/crypto-machine/data/cycles.jsonl"
BACKUP_LOG = f"{HOME}/logs/daily-backup.log"
PI_IP = "192.168.50.152"


def _ago(epoch: float) -> str:
    s = max(0, time.time() - epoch)
    if s < 90:
        return "just now"
    if s < 3600:
        return f"{int(s // 60)}m ago"
    if s < 86400:
        return f"{s / 3600:.1f}h ago"
    return f"{s / 86400:.1f}d ago"


def _port_open(port: int, host: str = "127.0.0.1", timeout: float = 1.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _tail(path: str, n: int = 40) -> list[str]:
    try:
        out = subprocess.run(["tail", "-n", str(n), path],
                             capture_output=True, text=True, timeout=5)
        return (out.stdout or "").splitlines()
    except (subprocess.TimeoutExpired, OSError):
        return []


def _systemd_active(*units: str) -> list[str]:
    try:
        out = subprocess.run(["systemctl", "--user", "is-active", *units],
                             capture_output=True, text=True, timeout=5)
        return (out.stdout or "").strip().splitlines()
    except (subprocess.TimeoutExpired, OSError):
        return ["unknown"] * len(units)


def _proj(name: str, status: str, detail: str, ago: str) -> dict:
    return {"name": name, "status": status, "detail": detail, "ago": ago}


def _hermes_tile() -> dict:
    units = ["hermes-gateway-orchestrator.service", "hermes-gateway-light.service",
             "hermes-gateway-dobby.service"]
    states = _systemd_active(*units)
    up = sum(1 for x in states if x == "active")
    status = "ok" if up == 3 else ("warn" if up > 0 else "bad")
    return _proj("Hermes gateways", status, f"{up}/3 active", "live")


def _pokemon_tile() -> dict:
    if not os.path.exists(POKEMON_LOG):
        return _proj("Pokemon Arb", "stale", "no scan log", "—")
    mtime = os.path.getmtime(POKEMON_LOG)
    detail = "scan log updated"
    for line in reversed(_tail(POKEMON_LOG, 60)):
        m = re.search(r"(\d+)\s+(?:hits?|matches)", line, re.I)
        if m:
            detail = f"last scan: {m.group(1)} hits"
            break
    age_h = (time.time() - mtime) / 3600
    status = "ok" if age_h < 6 else ("warn" if age_h < 12 else "bad")
    return _proj("Pokemon Arb", status, detail, _ago(mtime))


def _crypto_tile() -> dict:
    if not os.path.exists(CRYPTO_CYCLES):
        return _proj("Crypto Machine", "stale", "no cycles.jsonl", "—")
    mtime = os.path.getmtime(CRYPTO_CYCLES)
    detail = "cycle logged"
    lines = _tail(CRYPTO_CYCLES, 2)
    if lines:
        try:
            last = json.loads(lines[-1])
            opens = last.get("opened", last.get("opens"))
            closes = last.get("closed", last.get("closes"))
            eq = last.get("equity") or last.get("total_equity")
            bits = []
            if opens is not None:
                bits.append(f"{opens} open / {closes} close")
            if eq is not None:
                bits.append(f"eq ${float(eq):,.0f}")
            if bits:
                detail = " · ".join(bits)
        except (ValueError, TypeError):
            pass
    age_h = (time.time() - mtime) / 3600
    status = "ok" if age_h < 5 else ("warn" if age_h < 9 else "bad")
    return _proj("Crypto Machine", status, detail, _ago(mtime))


def _backup_tile() -> dict:
    if not os.path.exists(BACKUP_LOG):
        return _proj("Backups (restic)", "stale", "no log", "—")
    mtime = os.path.getmtime(BACKUP_LOG)
    status, detail = "warn", "no result line found"
    for line in reversed(_tail(BACKUP_LOG, 80)):
        if "🔴" in line or "FAILED" in line:
            status, detail = "bad", line.split("]")[-1].strip()[:60]
            break
        if "✅" in line:
            status, detail = "ok", line.split("]")[-1].strip()[:60]
            break
    if status == "ok" and (time.time() - mtime) > 2 * 86400:
        status, detail = "warn", f"last success stale: {detail}"
    return _proj("Backups (restic)", status, detail, _ago(mtime))


def _port_tile(name: str, port: int, unit: str | None = None) -> dict:
    if _port_open(port):
        return _proj(name, "ok", f":{port} up", "live")
    state = _systemd_active(unit)[0] if unit else "?"
    return _proj(name, "bad" if state == "active" else "stale",
                 f":{port} down (unit {state})", "—")


def _pi_tile() -> dict:
    try:
        out = subprocess.run(["ping", "-c", "1", "-W", "1", PI_IP],
                             capture_output=True, timeout=4)
        if out.returncode == 0:
            return _proj("Pi mirror", "ok", f"{PI_IP} reachable", "live")
    except (subprocess.TimeoutExpired, OSError):
        pass
    return _proj("Pi mirror", "bad", f"{PI_IP} unreachable", "—")


def _start9_tile() -> dict:
    if _port_open(18080):
        return _proj("Start9", "ok", "proxy :18080 up", "live")
    return _proj("Start9", "warn", "proxy :18080 down", "—")


def query_projects() -> dict:
    tiles = [
        _hermes_tile(),
        _pokemon_tile(),
        _crypto_tile(),
        _backup_tile(),
        _port_tile("STL Sandbox", 8050, "stl-sandbox.service"),
        _port_tile("Blockfield", 8080, "bitcoin-blockfield.service"),
        _pi_tile(),
        _start9_tile(),
    ]
    return {"iso_ts": datetime.now(timezone.utc).isoformat(), "projects": tiles}


# ---------------- todos (plain JSON file) ----------------

def _load_todos() -> list[dict]:
    if not os.path.exists(TODO_FILE):
        return []
    try:
        with open(TODO_FILE) as f:
            return json.load(f)
    except (ValueError, OSError):
        return []


def _save_todos(todos: list[dict]) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = TODO_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(todos, f, indent=1)
    os.replace(tmp, TODO_FILE)


def get_todos() -> list[dict]:
    with _todo_lock:
        return _load_todos()


def add_todo(text: str, tag: str = "home") -> list[dict]:
    with _todo_lock:
        todos = _load_todos()
        todos.insert(0, {
            "id": uuid.uuid4().hex[:8],
            "text": text.strip()[:300],
            "tag": (tag or "home").strip()[:20],
            "done": False,
            "created": datetime.now(timezone.utc).isoformat(),
        })
        _save_todos(todos)
        return todos


def toggle_todo(todo_id: str) -> list[dict]:
    with _todo_lock:
        todos = _load_todos()
        for t in todos:
            if t.get("id") == todo_id:
                t["done"] = not t.get("done")
        _save_todos(todos)
        return todos


def delete_todo(todo_id: str) -> list[dict]:
    with _todo_lock:
        todos = [t for t in _load_todos() if t.get("id") != todo_id]
        _save_todos(todos)
        return todos


if __name__ == "__main__":
    print(json.dumps(query_projects(), indent=2))
