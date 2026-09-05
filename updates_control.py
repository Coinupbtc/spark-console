"""Allowlisted git / Hermes updates for Spark Console.

Telegram ``/update`` runs ``hermes update --gateway``. That path is currently
dead on this box because the Hermes CLI used to crash at import. This module
is the console replacement: inspect tracked checkouts, apply one allowlisted
key, poll a log. Recipe pulls stay ff-only (never overwrite dirty DS4F
hotfixes). Hermes may reset-to-origin then re-apply house patches.
"""
from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

HOME = Path.home()
DATA_DIR = Path(__file__).resolve().parent / "data"
OPS_FILE = DATA_DIR / "update_operations.json"
CACHE_FILE = DATA_DIR / "updates_status.json"
APPLY_SH = Path(__file__).resolve().parent / "apply_update.sh"
APT_SH = HOME / "scripts/dgx/apply-system-packages.sh"
RECIPES_SH = HOME / ".hermes/scripts/recipe-autoupdate.sh"
DSPARK_FF = HOME / "scripts/dgx/dspark-ff-pull.sh"

# Closed list. Keys are the only strings POST /api/updates/apply will accept.
TARGETS: dict[str, dict] = {
    "apt-node1": {
        "label": "Update System Package Node1",
        "short": "apt dist-upgrade this Spark",
        "path": None,
        "ref": "main",
        "kind": "apt",
        "eta": "2–15 min",
        "warn": "apt-get dist-upgrade on sparkmax-10ef. Does not reboot. Kernel packages wait until you reboot. Dream/0731 can stay up (same as Software Updater).",
        "node": "node1",
    },
    "apt-node2": {
        "label": "Update System Package Node2",
        "short": "apt dist-upgrade spark2",
        "path": None,
        "ref": "main",
        "kind": "apt",
        "eta": "2–15 min",
        "warn": "apt-get dist-upgrade on sparkymaxxx-12ef over CX7. Does not reboot. Qwen :8100 can stay up.",
        "node": "node2",
    },
    "hermes": {
        "label": "Hermes Agent",
        "short": "Telegram /update replacement",
        "path": HOME / ".hermes/hermes-agent",
        "ref": "main",
        "kind": "hermes",
        "eta": "2–8 min",
        "warn": "Pulls Nous origin/main, then re-applies /image /video and the kanban sentinel. Restarts gateways. Does not switch inference.",
    },
    "ds4f": {
        "label": "DS4F recipe (both Sparks)",
        "short": "Mia live checkout",
        "path": HOME / "Documents/projects/DeepSeek-v4-Flash-DSpark-2x-DGX-Spark",
        "ref": "main",
        "kind": "ds4f",
        "eta": "under 1 min",
        "warn": "Checkout only — does not restart :8888. Behind/diverged: merge origin/main (keeps local commits). Dirty tracked files still skip.",
    },
    "h3-2x": {
        "label": "MiniMax H3 2x",
        "short": "video recipe · RoCE IB",
        "path": HOME / "Documents/projects/MiniMax-H3-2x-DGX-Spark",
        "ref": "main",
        "kind": "git",
        "eta": "under 1 min",
        "warn": "ff-only from the public fork. Does not start Videos setup. Live Videos uses CX7 RoCEv2 (GID 3, MTU 1500, memlock unlimited).",
    },
    "h3-1x": {
        "label": "MiniMax H3 1x",
        "short": "single-spark recipe",
        "path": HOME / "Documents/projects/MiniMax-H3-DGX-Spark",
        "ref": "main",
        "kind": "git",
        "eta": "under 1 min",
        "warn": "ff-only.",
    },
    # puzzle / laguna / mimo: leftover trial recipes (~400K git, no weights).
    # Dropped 2026-09-02 so Console + nightly jobs stop paging unused trees.
    "qwen35": {
        "label": "Qwen 3.6 35B recipe",
        "short": "helper weights recipe",
        "path": HOME / "models/dgx_bundle/qwen3.6-35b-a3b-ud",
        "ref": "main",
        "kind": "git",
        "eta": "under 1 min",
        "warn": "ff-only. Does not download HF weights.",
    },
    "recipes": {
        "label": "All recipes (safe)",
        "short": "nightly ff-only job, on demand",
        "path": None,
        "ref": "main",
        "kind": "bundle",
        "eta": "1–2 min",
        "warn": "Same as recipe-autoupdate.sh. Skips dirty and diverged trees. Never restarts engines.",
    },
}

_ops_lock = threading.Lock()
_cache_lock = threading.Lock()
_status_cache: tuple[float, dict] | None = None
_CACHE_TTL = 20.0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _git(path: Path, *args: str, timeout: float = 20.0) -> tuple[int, str]:
    try:
        out = subprocess.run(
            ["git", "-C", str(path), *args],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        text = (out.stdout or out.stderr or "").strip()
        return out.returncode, text
    except (subprocess.TimeoutExpired, OSError) as e:
        return 1, f"{type(e).__name__}: {e}"


def inspect_repo(path: Path | None, ref: str = "main") -> dict:
    """Local git math only — no network. origin/<ref> may be stale."""
    if path is None:
        return {
            "exists": False,
            "head": None,
            "remote": None,
            "behind": 0,
            "ahead": 0,
            "dirty": False,
            "dirty_files": [],
            "state": "bundle",
            "error": None,
        }
    path = Path(path)
    if not ((path / ".git").exists()):
        return {
            "exists": False,
            "head": None,
            "remote": None,
            "behind": None,
            "ahead": None,
            "dirty": False,
            "dirty_files": [],
            "state": "missing",
            "error": f"no git checkout at {path}",
        }
    rc_h, head = _git(path, "rev-parse", "--short", "HEAD")
    rc_r, remote = _git(path, "rev-parse", "--short", f"origin/{ref}")
    behind = ahead = None
    if rc_h == 0 and rc_r == 0:
        _, btxt = _git(path, "rev-list", "--count", f"HEAD..origin/{ref}")
        _, atxt = _git(path, "rev-list", "--count", f"origin/{ref}..HEAD")
        try:
            behind = int(btxt)
            ahead = int(atxt)
        except ValueError:
            behind = ahead = None
    rc_d, porcelain = _git(path, "status", "--porcelain", "--untracked-files=no")
    dirty_files = []
    dirty = False
    if rc_d == 0 and porcelain:
        dirty = True
        for line in porcelain.splitlines()[:8]:
            parts = line.strip().split(maxsplit=1)
            dirty_files.append(parts[-1] if parts else line)
    if rc_h != 0:
        state = "error"
        error = head
        head = None
    elif rc_r != 0:
        state = "unknown"
        error = f"no origin/{ref} — tap Check"
        remote = None
    elif dirty:
        state = "dirty"
        error = None
    elif ahead and behind:
        state = "diverged"
        error = None
    elif behind:
        state = "behind"
        error = None
    else:
        state = "current"
        error = None
    return {
        "exists": True,
        "head": head if rc_h == 0 else None,
        "remote": remote if rc_r == 0 else None,
        "behind": behind,
        "ahead": ahead,
        "dirty": dirty,
        "dirty_files": dirty_files,
        "state": state,
        "error": error,
    }


def can_apply(kind: str, info: dict, busy: bool) -> tuple[bool, str]:
    if busy:
        return False, "another update is running"
    if kind == "bundle":
        return True, "safe ff-only of every recipe repo"
    if kind == "apt":
        return True, "apt dist-upgrade (no reboot)"
    if not info.get("exists"):
        return False, info.get("error") or "missing checkout"
    if info.get("state") == "unknown":
        return False, info.get("error") or "tap Check first"
    behind = int(info.get("behind") or 0)
    ahead = int(info.get("ahead") or 0)
    if kind == "hermes":
        if behind <= 0:
            return False, "already current"
        if ahead:
            return True, f"{behind} upstream, {ahead} local reapply — will reset then re-apply patches"
        return True, f"{behind} commits behind"
    if info.get("dirty"):
        return False, "dirty tracked files — skipped so local patches stay"
    if ahead and behind:
        if kind == "ds4f":
            return True, f"diverged — merge GitHub ({behind} behind, keep {ahead} local)"
        return False, f"diverged (ahead {ahead}, behind {behind}) — not a force merge"
    if ahead:
        return False, f"already current ({ahead} local commit{'s' if ahead!=1 else ''})"
    if behind <= 0:
        return False, "already current"
    return True, f"{behind} commits behind"


def _fetch_one(path: Path, ref: str) -> None:
    _git(path, "fetch", "--quiet", "origin", timeout=45.0)


def _public_target(key: str, spec: dict, info: dict, busy: bool) -> dict:
    ok, reason = can_apply(spec["kind"], info, busy)
    return {
        "key": key,
        "label": spec["label"],
        "short": spec["short"],
        "kind": spec["kind"],
        "eta": spec["eta"],
        "warn": spec["warn"],
        "can_apply": ok,
        "reason": reason,
        **{k: info[k] for k in (
            "exists", "head", "remote", "behind", "ahead",
            "dirty", "dirty_files", "state", "error",
        )},
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
            op["message"] = op.get("message") or "Update finished."
        else:
            op["status"] = "failed"
            op["message"] = op.get("message") or "Update failed — see log."
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


def _build_status(*, fetched: bool) -> dict:
    busy_op = active_operation()
    busy = bool(busy_op and busy_op.get("status") == "running")
    items = []
    behind_n = 0
    blocked_n = 0
    for key, spec in TARGETS.items():
        if spec["kind"] == "apt":
            info = {
                "exists": True,
                "head": spec.get("node"),
                "remote": None,
                "behind": 0,
                "ahead": 0,
                "dirty": False,
                "dirty_files": [],
                "state": "current",
                "error": None,
            }
        else:
            info = inspect_repo(spec.get("path"), spec.get("ref") or "main")
        row = _public_target(key, spec, info, busy)
        items.append(row)
        if key != "recipes":
            behind_n += int(row.get("behind") or 0)
            if row["state"] in ("dirty", "diverged") and int(row.get("behind") or 0) > 0:
                blocked_n += 1
    payload = {
        "fetched_at": _now_iso() if fetched else None,
        "cached_at": _now_iso(),
        "behind_total": behind_n,
        "blocked": blocked_n,
        "active_operation": busy_op,
        "last_operation": busy_op or last_operation(),
        "targets": items,
        "needs": [
            {
                "key": t["key"],
                "label": t["label"],
                "state": t["state"],
                "behind": t.get("behind") or 0,
                "reason": t.get("reason"),
            }
            for t in items
            if t["key"] != "recipes" and (
                int(t.get("behind") or 0) > 0 or t.get("state") in ("dirty", "diverged")
            )
        ],
    }
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_FILE.write_text(json.dumps(payload, indent=2, default=str))
    return payload


def status(*, refresh: bool = False) -> dict:
    """refresh=True fetches origin for every git target (slow, ~seconds)."""
    global _status_cache
    now = time.time()
    with _cache_lock:
        if not refresh and _status_cache and (now - _status_cache[0]) < _CACHE_TTL:
            return dict(_status_cache[1])
        if not refresh and CACHE_FILE.is_file() and not _status_cache:
            try:
                cached = json.loads(CACHE_FILE.read_text())
                if isinstance(cached, dict) and cached.get("targets") and "needs" in cached:
                    _status_cache = (now, cached)
                    return dict(cached)
            except (OSError, json.JSONDecodeError):
                pass
    if refresh:
        for spec in TARGETS.values():
            path = spec.get("path")
            if path and Path(path).exists():
                _fetch_one(Path(path), spec.get("ref") or "main")
    payload = _build_status(fetched=refresh)
    with _cache_lock:
        _status_cache = (time.time(), payload)
    return dict(payload)


def summary_for_overview() -> dict:
    """Cheap: last cache only. Never fetches. Safe on /api/overview."""
    st = status(refresh=False)
    return {
        "behind_total": st.get("behind_total") or 0,
        "blocked": st.get("blocked") or 0,
        "cached_at": st.get("cached_at"),
        "active_operation": st.get("active_operation"),
        "last_operation": st.get("last_operation"),
        "needs": [
            {
                "key": t["key"],
                "label": t["label"],
                "state": t["state"],
                "behind": t.get("behind") or 0,
                "reason": t.get("reason"),
            }
            for t in st.get("targets") or []
            if t["key"] != "recipes" and (
                int(t.get("behind") or 0) > 0 or t.get("state") in ("dirty", "diverged")
            )
        ],
    }


def _cmd_for(key: str) -> list[str] | None:
    spec = TARGETS.get(key)
    if not spec:
        return None
    kind = spec["kind"]
    if kind == "apt":
        node = spec.get("node") or "node1"
        return ["bash", str(APT_SH), node]
    if kind == "hermes":
        return ["bash", str(APPLY_SH), "hermes"]
    if kind == "ds4f":
        return ["bash", str(APPLY_SH), "ds4f"]
    if kind == "bundle":
        return ["bash", str(RECIPES_SH)]
    path = spec.get("path")
    ref = spec.get("ref") or "main"
    return ["bash", str(APPLY_SH), "git", str(path), str(ref)]


def apply(key: str) -> dict:
    key = (key or "").strip()
    if key not in TARGETS:
        return {"ok": False, "error": f"Unknown update: {key}"}
    if not APPLY_SH.is_file():
        return {"ok": False, "error": f"Missing {APPLY_SH}"}

    running = active_operation()
    if running:
        return {
            "ok": False,
            "error": f"Already updating {running.get('key')}.",
            "operation": running,
        }

    spec = TARGETS[key]
    info = inspect_repo(spec.get("path"), spec.get("ref") or "main")
    ok, reason = can_apply(spec["kind"], info, False)
    if not ok:
        return {"ok": False, "error": reason}

    cmd = _cmd_for(key)
    if not cmd:
        return {"ok": False, "error": f"No command for {key}"}

    op_id = uuid4().hex[:12]
    log_file = DATA_DIR / f"update_op_{op_id}.log"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "HOME": str(HOME), "PYTHONUNBUFFERED": "1"}
    env.pop("PORT", None)
    with open(log_file, "w") as logfh:
        proc = subprocess.Popen(cmd, stdout=logfh, stderr=subprocess.STDOUT, env=env)
    op = {
        "id": op_id,
        "type": "update",
        "key": key,
        "status": "running",
        "message": f"Updating {spec['label']}… ({spec['eta']})",
        "pid": proc.pid,
        "log_file": str(log_file),
        "started_at": _now_iso(),
        "finished_at": None,
        "returncode": None,
    }

    def _waiter() -> None:
        global _status_cache
        rc = proc.wait()
        with _ops_lock:
            data = _load_ops()
            cur = data["operations"].get(op_id, op)
            cur["returncode"] = rc
            cur["log_tail"] = _tail_file(log_file, 32)
            if rc == 0:
                cur["status"] = "completed"
                cur["message"] = f"{spec['label']} updated."
            else:
                cur["status"] = "failed"
                cur["message"] = f"{spec['label']} update failed (exit {rc})."
            cur["finished_at"] = _now_iso()
            data["operations"][op_id] = cur
            _save_ops(data)
        with _cache_lock:
            _status_cache = None
        _build_status(fetched=False)

    threading.Thread(target=_waiter, daemon=True).start()
    with _ops_lock:
        data = _load_ops()
        data.setdefault("operations", {})[op_id] = op
        _save_ops(data)
    return {"ok": True, "operation": op, "reason": reason}
