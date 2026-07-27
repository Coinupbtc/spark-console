#!/usr/bin/env python3
"""
Local scratch todo list for the console panel.

Plain JSON under data/, no database and nothing remote. Writes go through a
lock and a temp-file rename so a crash mid-save cannot truncate the list.
"""
from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
STORE = Path(os.environ.get("SPARK_CONSOLE_TODOS", HERE / "data" / "todos.json"))
MAX_TODOS = 200

_lock = threading.Lock()


def _load() -> list[dict]:
    try:
        data = json.loads(STORE.read_text())
        return [t for t in data.get("todos", []) if isinstance(t, dict)]
    except (OSError, ValueError):
        return []


def _save(todos: list[dict]) -> None:
    STORE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STORE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps({"todos": todos}, indent=2))
    tmp.replace(STORE)  # atomic: never leave a half-written list on disk


def get_todos() -> list[dict]:
    with _lock:
        return _load()


def apply(action: str, text: str = "", tag: str = "home", tid: str = "") -> list[dict]:
    """add | toggle | delete. Unknown actions are a no-op returning current state."""
    with _lock:
        todos = _load()
        if action == "add":
            text = (text or "").strip()[:300]
            if text:
                todos.insert(0, {
                    "id": uuid.uuid4().hex[:10],
                    "text": text,
                    "tag": (tag or "home").strip()[:24],
                    "done": False,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                })
                del todos[MAX_TODOS:]
        elif action == "toggle":
            for t in todos:
                if t.get("id") == tid:
                    t["done"] = not t.get("done")
                    break
        elif action == "delete":
            todos = [t for t in todos if t.get("id") != tid]
        _save(todos)
        return todos
