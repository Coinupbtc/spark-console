#!/usr/bin/env python3
"""ComfyUI job-monitor panel — live job queue + VRAM footprint for Spark Console.

Polls the local ComfyUI server (127.0.0.1:8188) with standard HTTP endpoints
and returns a flat, cached JSON snapshot. ComfyUI is on-demand (systemd user
timer, 20 min idle), so it is frequently DOWN — this module MUST degrade to a
clean "offline" state without erroring, spam, or wedging the console.

Data sources (all short-timeout, per-source isolated):
  GET /system_stats -> system/comfyui_version; devices[{name,vram_total,vram_free}]
  GET /prompt  (same shape as /queue) -> {queue_running:[...], queue_pending:[...]}
  GET /history?max_items=N -> last finished prompts: {pid:{prompt, outputs, status}}

Queue item shape (after ComfyUI strips index-5 sensitive data):
  [number, prompt_id, prompt_graph(dict node->{class_type,inputs,...}),
   extra_data, outputs_to_ui]
No live step counter is exposed over REST — ComfyUI pushes progress over its
WebSocket instead. So "progress" here means the running job's sampler `steps`
(total) plus an indeterminate bar while generating; `current` is filled only
when a node output already reports a value.

No secrets on this path: it only ever talks to the loopback ComfyUI server.

Design notes (mirrors the repo's /api/tick + _services_cache conventions):
  * the server's _comfy_refresher daemon thread calls query_comfy() and caches
    under a lock; GET /api/comfy returns the flat cache. Fast for the phone.
  * query_comfy() itself owns a tiny TTL cache so the cold-start path and the
    refresher don't stampede ComfyUI when it is actually running a job.
"""
from __future__ import annotations

import json
import time

try:
    import urllib.request
    import urllib.error
except Exception:  # pragma: no cover - never expected
    urllib = urllib.request = urllib.error = None

BASE = "http://127.0.0.1:8188"
TIMEOUT = 2          # short — never let a dead ComfyUI block the panel
REFRESH_TTL = 4.0    # seconds between refresher-thread re-polls of a live server
_HISTORY_ITEMS = 3   # how many finished jobs to retain for the "last finished" row

# Query string keys (see model_inventory.py style). Kept importable publicly in
# case a future /api route needs them.

_LAST: dict = {
    "ok": False,
    "offline": True,
    "error": "not polled yet",
    "ts": 0.0,
}
_LAST_LOCK = None
try:
    import threading
    _LAST_LOCK = threading.Lock()
except Exception:
    pass

# ---- small http helpers -----------------------------------------------------


def _get_json(path: str, timeout: float = TIMEOUT):
    """GET `path` on ComfyUI and return parsed JSON. Raises on any failure."""
    if urllib is None:
        raise RuntimeError("urllib unavailable")
    req = urllib.request.Request(BASE + path, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def _fmt_gb(n):
    """Pretty bytes->GB string. Defensive: None/bad -> ''."""
    try:
        n = float(n)
        if n < 0:
            return ""
        return f"{n/1024**3:.1f}G"
    except (TypeError, ValueError):
        return ""


# ---- per-source parsers (each isolated so one bad feed can't kill the rest) ---


def _parse_system_stats(raw) -> dict:
    """VRAM footprint + version from /system_stats."""
    stats = raw.get("system") or {}
    devices = raw.get("devices") or []
    dev = devices[0] if devices else {}
    return {
        "version": str(stats.get("comfyui_version") or "unknown")[:40],
        "vram_total": _fmt_gb(dev.get("vram_total")),
        "vram_free": _fmt_gb(dev.get("vram_free")),
        "device_name": str(dev.get("name") or "n/a")[:60],
        "device_count": len(devices),
    }


def _node_inputs(node) -> dict:
    in_ = node.get("inputs") or {}
    return in_ if isinstance(in_, dict) else {}


def _pick_running_job(item) -> dict:
    """Best-effort label for one queue item (running or pending)."""
    prompt_graph = None
    nodes = None
    pid = None
    # item is [number, prompt_id, prompt_graph, extra_data, outputs_to_ui]
    if isinstance(item, (list, tuple)) and len(item) >= 3:
        pid = item[1] if len(item) >= 2 else None
        prompt_graph = item[2] if len(item) >= 3 else None
    elif isinstance(item, dict):
        pid = item.get("prompt_id")
        prompt_graph = item.get("prompt")
    nodes = prompt_graph if isinstance(prompt_graph, dict) else {}

    sampler_steps = None
    ckpt = None
    vaes = []
    title = ""
    for nid, node in nodes.items():
        if not isinstance(node, dict):
            continue
        cls = str(node.get("class_type") or "")
        ins = _node_inputs(node)
        # sampler: KSampler / KSamplerAdvanced / SamplerCustom / ltx/wan samplers
        if any(k in cls.lower() for k in ("sampler", "ltxv", "ltx-video")) and ins:
            st = ins.get("steps")
            if isinstance(st, (int, float)):
                sampler_steps = int(st)
        if cls in ("CheckpointLoaderSimple", "CheckpointLoader", "UNETLoader"):
            ckpt = ckpt or str(ins.get("ckpt_name") or ins.get("unet_name") or "")[:60]
        if "vae" in cls.lower() and ins:
            n = ins.get("vae_name") or ins.get("vae")
            if n:
                vaes.append(str(n))
        if not title and ins.get("text"):
            t = str(ins["text"]).strip().replace("\n", " ")
            if t:
                title = t[:48]
        if not title and ins.get("ckpt_name"):
            title = str(ins["ckpt_name"])[:48]

    return {
        "prompt_id": str(pid or "")[:36],
        "label": title or ckpt or (list(nodes.keys())[0] if nodes else "job"),
        "steps": sampler_steps,
        "model": ckpt,
        "nodes": len(nodes),
    }


def _parse_history(raw) -> dict:
    """Last finished jobs: label + duration from /history.

    Handles both shapes seen across ComfyUI versions:
      * status.start/status.end (older) as epoch-ms
      * status.messages[] with execution_start / execution_success timestamps
        (>=0.27) — elapsed = success - start.
    """
    hist = raw if isinstance(raw, dict) else {}
    done = []
    for pid, entry in list(hist.items()):
        if not isinstance(entry, dict):
            continue
        outputs = entry.get("outputs") or {}
        status = entry.get("status") or {}
        status_str = str(status.get("status_str") or "success")
        completed = status.get("completed")

        # duration, in ms, from whichever source the server exposes
        start_ms = status.get("start")
        end_ms = status.get("end")
        if not (isinstance(start_ms, (int, float)) and isinstance(end_ms, (int, float))):
            # >=0.27: timestamps live in status.messages[{msg_type,timestamp}]
            ev = {}
            for msg in status.get("messages") or []:
                if isinstance(msg, (list, tuple)) and len(msg) >= 2:
                    ev[msg[0]] = msg[1]
            s = ev.get("execution_start") or {}
            e = ev.get("execution_success") or {}
            if isinstance(s, dict):
                start_ms = s.get("timestamp")
            if isinstance(e, dict):
                end_ms = e.get("timestamp")
        dur_ms = None
        if isinstance(start_ms, (int, float)) and isinstance(end_ms, (int, float)):
            _d = int(end_ms - start_ms)
            dur_ms = _d if _d >= 0 else None

        job = _pick_running_job(entry.get("prompt") or {})
        job.update({
            "prompt_id": str(pid or "")[:36],
            "status": status_str,
            "finished": bool(completed) if isinstance(completed, bool) else bool(completed),
            "duration_ms": dur_ms,
            "duration_s": round(dur_ms / 1000.0, 1) if dur_ms else None,
            "output_count": len(outputs) if isinstance(outputs, dict) else 0,
            "completed_iso": "",
        })
        done.append(job)
        if len(done) >= _HISTORY_ITEMS:
            break
    return {"last_finished": done}


# ---- public snapshot --------------------------------------------------------


def query_comfy() -> dict:
    """Return a flat ComfyUI snapshot, cached ~REFRESH_TTL to avoid stampeding
    a live generation. Never raises: any failure -> offline state."""
    now = time.time()
    # TTL cache: if we polled a LIVE ComfyUI < REFRESH_TTL ago, reuse it.
    if _LAST.get("offline") is False and (now - _LAST.get("ts", 0)) < REFRESH_TTL:
        with (_LAST_LOCK or _noop()):
            return dict(_LAST)

    try:
        sysraw = _get_json("/system_stats")
        # NOTE: this ComfyUI (0.27) does NOT expose queue_running/queue_pending
        # on GET /prompt (it returns only exec_info.queue_remaining). The real
        # queue lives on GET /queue — the reliable cross-version source.
        queueraw = _get_json("/queue")
        histraw = _get_json(f"/history?max_items={_HISTORY_ITEMS}")
    except Exception as e:  # offline / refused / timeout — the common case
        out = {
            "ok": False,
            "offline": True,
            "error": _short_err(e),
            "version": None,
            "vram_total": "", "vram_free": "", "device_name": "", "device_count": 0,
            "state": "offline",
            "chip": "offline",
            "running": None,
            "pending_count": 0,
            "queue_total": 0,
            "models": [],
            "last_finished": [],
        }
        with (_LAST_LOCK or _noop()):
            _LAST.update(out)
            _LAST["ts"] = now
        return dict(_LAST)

    return _build_snapshot(sysraw, queueraw, histraw, now)


def _noop():
    class _C:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    return _C()


def _build_snapshot(sysraw, promptraw, histraw, now):
    sys_ = _parse_system_stats(sysraw)

    running = promptraw.get("queue_running") or []
    pending = promptraw.get("queue_pending") or []
    running_job = {}
    if running:
        try:
            running_job = _pick_running_job(running[0])
        except Exception:
            running_job = {}

    pending_labels = []
    for p in pending:
        try:
            pending_labels.append(_pick_running_job(p))
        except Exception:
            continue

    # Models in VRAM: combine ckpt/vae from running + last finished, deduped.
    models = []
    _seen = set()
    for j in ([running_job] if running else []) + (pending_labels or []):
        for m in (j.get("model"),):
            if m and m not in _seen:
                _seen.add(m)
                models.append(m)
    for j in (_parse_history(histraw).get("last_finished") or []):
        m = j.get("model")
        if m and m not in _seen:
            _seen.add(m)
            models.append(m)

    hist = _parse_history(histraw)
    last_finished = hist.get("last_finished") or []

    state = "running" if running else ("queued" if pending else "idle")
    chip = "run" if running else (f"{len(pending)}q" if pending else "idle")

    out = {
        "ok": True,
        "offline": False,
        "error": None,
        **sys_,
        "state": state,
        "chip": chip,
        "running": running_job or None,
        "running_steps": (running_job or {}).get("steps"),
        "pending_count": len(pending),
        "queue_total": len(running) + len(pending),
        "pending": pending_labels,
        "models": models,
        "vram_used_pct": _vram_pct(sys_),
        "last_finished": last_finished,
    }
    with (_LAST_LOCK or _noop()):
        _LAST.clear()
        _LAST.update(out)
        _LAST["ts"] = now
    return dict(_LAST)


def _vram_pct(sys_) -> float | None:
    """VRAM used % from total/free strings; None if unknowable."""
    tot = sys_.get("vram_total") or ""
    free = sys_.get("vram_free") or ""
    try:
        tot_f = float(tot.rstrip("G"))
        free_f = float(free.rstrip("G"))
        if tot_f <= 0:
            return None
        used = max(0.0, min(100.0, (tot_f - free_f) / tot_f * 100.0))
        return round(used, 1)
    except (ValueError, AttributeError):
        return None


def _short_err(e) -> str:
    """One-line, safe error description for the frontend."""
    try:
        reason = getattr(e, "reason", None)
        if reason is not None:
            return f"Cannot reach ComfyUI ({reason})"
        return f"Cannot reach ComfyUI ({type(e).__name__})"
    except Exception:
        return "Cannot reach ComfyUI"


if __name__ == "__main__":
    import sys
    print(json.dumps(query_comfy(), indent=2, default=str))
    sys.exit(0)
