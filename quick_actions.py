#!/usr/bin/env python3
"""
Allowlisted one-click operations for the console.

Deliberately tiny and closed: only the commands in ACTIONS can ever run, no
arguments come from the request, and each runs in a worker thread with a hard
timeout so a hung script cannot wedge the dashboard. Nothing here mutates
trading, backups, or model state — those already have their own controls in
service_control.py / model_control.py.
"""
from __future__ import annotations

import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

HOME = Path.home()

ACTIONS: dict[str, dict] = {
    "test-alert": {
        "label": "Send test alert",
        "detail": "Proves the Telegram alert path end-to-end (@alerthermesbot)",
        "cmd": ["bash", str(HOME / ".hermes/scripts/alertbot-send.sh"),
                "🔔 Spark Console test alert — alert path is working"],
        "timeout": 30,
    },
    "stack-health": {
        "label": "Run stack health",
        "detail": "Full stack-health.sh sweep — inference, gateways, disks, backups",
        "cmd": ["bash", str(HOME / "scripts/data/stack-health.sh")],
        "timeout": 180,
    },
    "vault-lint": {
        "label": "Lint Obsidian vault",
        "detail": "vault-lint.py — orphans, duplicate hubs, broken links",
        "cmd": ["python3", str(HOME / "scripts/obsidian/vault-lint.py")],
        "timeout": 120,
    },
    "inference-check": {
        "label": "Check inference",
        "detail": "curl the default text model endpoint :8889 (crash-cascade check #1)",
        "cmd": ["curl", "-s", "-m", "5", "http://127.0.0.1:8889/v1/models"],
        "timeout": 15,
    },
}

_lock = threading.Lock()
_runs: dict[str, dict] = {}
_recent: list[str] = []


def list_actions() -> list[dict]:
    with _lock:
        last = {}
        for run_id in reversed(_recent[-40:]):
            run = _runs.get(run_id)
            if run and run["action"] not in last:
                last[run["action"]] = run
    return [{"id": key, "label": spec["label"], "detail": spec["detail"],
             "last": _public(last[key]) if key in last else None}
            for key, spec in ACTIONS.items()]


def _public(run: dict) -> dict:
    return {k: v for k, v in run.items() if k != "_thread"}


def get_run(run_id: str) -> dict | None:
    with _lock:
        run = _runs.get(run_id)
        return _public(run) if run else None


def _worker(run_id: str, key: str) -> None:
    spec = ACTIONS[key]
    started = time.time()
    try:
        out = subprocess.run(spec["cmd"], capture_output=True, text=True,
                             timeout=spec["timeout"])
        text = (out.stdout or "").strip() or (out.stderr or "").strip()
        status = "ok" if out.returncode == 0 else "failed"
        code = out.returncode
    except subprocess.TimeoutExpired:
        text, status, code = f"timed out after {spec['timeout']}s", "failed", -1
    except OSError as e:
        text, status, code = f"{type(e).__name__}: {e}", "failed", -1
    with _lock:
        run = _runs[run_id]
        run.update({
            "status": status, "exit_code": code,
            "output": text[-4000:] if text else "(no output)",
            "took_s": round(time.time() - started, 1),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })


def run_action(key: str) -> dict:
    if key not in ACTIONS:
        return {"ok": False, "error": f"unknown action: {key}"}
    run_id = uuid.uuid4().hex[:12]
    run = {"id": run_id, "action": key, "label": ACTIONS[key]["label"],
           "status": "running", "output": "", "exit_code": None,
           "started_at": datetime.now(timezone.utc).isoformat()}
    with _lock:
        _runs[run_id] = run
        _recent.append(run_id)
        if len(_recent) > 200:
            for stale in _recent[:100]:
                _runs.pop(stale, None)
            del _recent[:100]
    threading.Thread(target=_worker, args=(run_id, key), daemon=True).start()
    return {"ok": True, "run": _public(run)}


if __name__ == "__main__":
    import json
    result = run_action("inference-check")
    rid = result["run"]["id"]
    for _ in range(20):
        time.sleep(0.5)
        run = get_run(rid)
        if run and run["status"] != "running":
            print(json.dumps(run, indent=2)[:600])
            break
