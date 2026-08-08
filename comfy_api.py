#!/usr/bin/env python3
"""ComfyUI job-monitor panel — live job queue + footprint for Spark Console.

Polls the local ComfyUI server (127.0.0.1:8188) with standard HTTP endpoints
and returns a flat, cached JSON snapshot. ComfyUI is on-demand (systemd user
timer, 20 min idle), so it is frequently DOWN — this module MUST degrade to a
clean "offline" state without erroring, spam, or wedging the console.

Aligned with MiaAI sparkDash 1.6 Comfy panel (job-centric):
  * model / LoRA footprint from the prompt graph (any *.safetensors etc.)
  * res · steps · sampler · batch · node count
  * live queue (running + pending) with cancel/remove
  * elapsed clock for the running job (no invented % — REST has no step
    counter; ComfyUI pushes that over websocket to the submitting client)
  * last finished + duration, coarse queue ETA from finished-job averages,
    installed checkpoints/LoRAs

Data sources (all short-timeout, per-source isolated):
  GET /system_stats
  GET /queue
  GET /history?max_items=N  (and optional /api/jobs when present)
  GET /models/checkpoints , /models/loras
  POST /interrupt + POST /queue {delete:[id]}  (cancel / remove)

No secrets on this path: it only ever talks to the loopback ComfyUI server.
"""
from __future__ import annotations

import json
import re
import time

try:
    import urllib.request
    import urllib.error
except Exception:  # pragma: no cover - never expected
    urllib = urllib.request = urllib.error = None

BASE = "http://127.0.0.1:8188"
TIMEOUT = 2          # short — never let a dead ComfyUI block the panel
REFRESH_TTL = 4.0    # seconds between refresher-thread re-polls of a live server
_HISTORY_ITEMS = 8   # enough samples for a useful avg duration / ETA
_ETA_FALLBACK_MS = 60_000
_MODELS_TTL = 45.0    # installed inventory changes rarely
_MODEL_FILE_RE = re.compile(r"\.(safetensors|sft|ckpt|pt|bin|gguf|pth|onnx)$", re.I)

_LAST: dict = {
    "ok": False,
    "offline": True,
    "error": "not polled yet",
    "ts": 0.0,
}
_MODELS_CACHE: dict = {"fetched_at": 0.0, "checkpoints": [], "loras": []}
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


def _post_json(path: str, body: dict, timeout: float = TIMEOUT):
    """POST JSON to ComfyUI. Returns (ok, status_code, text_snip)."""
    if urllib is None:
        raise RuntimeError("urllib unavailable")
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method="POST",
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
            return True, getattr(resp, "status", 200), raw[:200]
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace") if hasattr(e, "read") else ""
        return False, e.code, raw[:200]
    except Exception as e:
        return False, 0, _short_err(e)


def _fmt_gb(n):
    """Pretty bytes->GB string. Defensive: None/bad -> ''."""
    try:
        n = float(n)
        if n < 0:
            return ""
        return f"{n/1024**3:.1f}G"
    except (TypeError, ValueError):
        return ""


def _basename(path: str) -> str:
    s = str(path)
    i = max(s.rfind("/"), s.rfind("\\"))
    return s[i + 1:] if i >= 0 else s


def _num(v):
    """Coerce to finite number or None."""
    try:
        if isinstance(v, str) and v.strip():
            v = float(v)
        if isinstance(v, (int, float)) and v == v:  # not NaN
            return v
    except (TypeError, ValueError):
        pass
    return None


# ---- per-source parsers (each isolated so one bad feed can't kill the rest) ---


def _parse_system_stats(raw) -> dict:
    """Version + device type from /system_stats (VRAM kept for the card footer)."""
    stats = raw.get("system") or {}
    devices = raw.get("devices") or []
    dev = devices[0] if devices else {}
    return {
        "version": str(stats.get("comfyui_version") or "unknown")[:40],
        "pytorch_version": str(stats.get("pytorch_version") or "")[:40] or None,
        "vram_total": _fmt_gb(dev.get("vram_total")),
        "vram_free": _fmt_gb(dev.get("vram_free")),
        "device_name": str(dev.get("name") or "n/a")[:60],
        "device_type": str(dev.get("type") or "")[:40] or None,
        "device_count": len(devices),
    }


def _node_inputs(node) -> dict:
    in_ = node.get("inputs") or {}
    return in_ if isinstance(in_, dict) else {}


def _summarize_prompt(prompt) -> dict:
    """Extract human-facing workload fields from a ComfyUI prompt graph.

    Mirrors sparkDash summarizeComfyPrompt: any model-looking filename (ckpt,
    unet, lora, vae, …), first steps/width/height/batch/sampler_name found.
    """
    models: list[str] = []
    steps = width = height = batch = None
    sampler = None
    node_count = 0
    class_counts: dict[str, int] = {}

    if not isinstance(prompt, dict):
        return {
            "models": [], "node_count": 0, "steps": None, "width": None,
            "height": None, "batch_size": None, "sampler": None, "class_counts": {},
        }

    for _nid, node in prompt.items():
        if not isinstance(node, dict):
            continue
        node_count += 1
        cls = str(node.get("class_type") or "Unknown")
        class_counts[cls] = class_counts.get(cls, 0) + 1
        ins = _node_inputs(node)
        for key, val in ins.items():
            if isinstance(val, str) and _MODEL_FILE_RE.search(val):
                models.append(_basename(val))
                continue
            if key == "steps" and steps is None:
                n = _num(val)
                if n is not None:
                    steps = int(n)
            elif key == "width" and width is None:
                n = _num(val)
                if n is not None:
                    width = int(n)
            elif key == "height" and height is None:
                n = _num(val)
                if n is not None:
                    height = int(n)
            elif key == "batch_size" and batch is None:
                n = _num(val)
                if n is not None:
                    batch = int(n)
            elif key == "sampler_name" and sampler is None and isinstance(val, str):
                sampler = val[:40]

    # unique, order-preserving
    seen: set[str] = set()
    uniq = []
    for m in models:
        if m not in seen:
            seen.add(m)
            uniq.append(m)

    return {
        "models": uniq,
        "node_count": node_count,
        "steps": steps,
        "width": width,
        "height": height,
        "batch_size": batch,
        "sampler": sampler,
        "class_counts": class_counts,
    }


def _workflow_title(extra) -> str | None:
    """Pull a human title from extra_pnginfo.workflow when present."""
    if not isinstance(extra, dict):
        return None
    try:
        png = extra.get("extra_pnginfo") or {}
        wf = png.get("workflow") if isinstance(png, dict) else None
        if isinstance(wf, dict):
            for k in ("title", "name"):
                t = wf.get(k)
                if t is not None and str(t).strip():
                    return str(t).strip()[:80]
    except Exception:
        pass
    return None


def _pick_running_job(item, status: str = "running") -> dict:
    """Best-effort label + footprint for one queue item (running or pending)."""
    prompt_graph = None
    pid = None
    extra = {}
    # item is [number, prompt_id, prompt_graph, extra_data, outputs_to_ui]
    if isinstance(item, (list, tuple)) and len(item) >= 3:
        pid = item[1] if len(item) >= 2 else None
        prompt_graph = item[2] if len(item) >= 3 else None
        if len(item) >= 4 and isinstance(item[3], dict):
            extra = item[3]
    elif isinstance(item, dict):
        # /history entry shape: {prompt: [...], outputs, status} OR flat
        if isinstance(item.get("prompt"), (list, tuple)) and len(item["prompt"]) >= 3:
            # history wraps the queue tuple under "prompt"
            return _pick_running_job(item["prompt"], status)
        pid = item.get("prompt_id") or item.get("id")
        prompt_graph = item.get("prompt")
        extra = item.get("extra_data") or {}
    else:
        return {
            "prompt_id": "", "id": "", "status": status, "label": "job",
            "title": None, "models": [], "model": None, "steps": None,
            "width": None, "height": None, "batch_size": None, "sampler": None,
            "nodes": 0, "node_count": 0, "create_time": None, "footprint": "",
        }

    nodes = prompt_graph if isinstance(prompt_graph, dict) else {}
    summary = _summarize_prompt(nodes)
    title = _workflow_title(extra)

    # fallback label: workflow title → first model → first CLIP text → "job"
    label = title or ""
    if not label and summary["models"]:
        label = summary["models"][0]
    if not label:
        for _nid, node in nodes.items():
            if not isinstance(node, dict):
                continue
            ins = _node_inputs(node)
            if ins.get("text"):
                t = str(ins["text"]).strip().replace("\n", " ")
                if t:
                    label = t[:48]
                    break
    if not label:
        label = "job"

    create_time = _num(extra.get("create_time")) if isinstance(extra, dict) else None
    footprint = _footprint_str(summary)

    return {
        "prompt_id": str(pid or "")[:64],
        "id": str(pid or "")[:64],
        "status": status,
        "label": label[:80],
        "title": title,
        "models": summary["models"],
        "model": summary["models"][0] if summary["models"] else None,
        "steps": summary["steps"],
        "width": summary["width"],
        "height": summary["height"],
        "batch_size": summary["batch_size"],
        "sampler": summary["sampler"],
        "nodes": summary["node_count"],
        "node_count": summary["node_count"],
        "create_time": create_time,
        "footprint": footprint,
    }


def _footprint_str(summary: dict) -> str:
    """`1024×1024 · 28 steps · euler · 12 nodes` — sparkDash jobFootprint."""
    parts = []
    w, h = summary.get("width"), summary.get("height")
    if w is not None and h is not None:
        parts.append(f"{int(w)}×{int(h)}")
    if summary.get("steps") is not None:
        parts.append(f"{int(summary['steps'])} steps")
    bs = summary.get("batch_size")
    if bs is not None and bs != 1:
        parts.append(f"batch {int(bs)}")
    if summary.get("sampler"):
        parts.append(str(summary["sampler"]))
    nc = summary.get("node_count") or 0
    if nc > 0:
        parts.append(f"{nc} nodes")
    return " · ".join(parts)


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
            e = (ev.get("execution_success")
                 or ev.get("execution_error")
                 or ev.get("execution_interrupted")
                 or {})
            if isinstance(s, dict):
                start_ms = s.get("timestamp")
            if isinstance(e, dict):
                end_ms = e.get("timestamp")
            if "execution_error" in ev:
                status_str = "failed"
            elif "execution_interrupted" in ev:
                status_str = "cancelled"
        dur_ms = None
        if isinstance(start_ms, (int, float)) and isinstance(end_ms, (int, float)):
            _d = int(end_ms - start_ms)
            dur_ms = _d if _d >= 0 else None

        job = _pick_running_job(entry.get("prompt") or {}, status="completed")
        job.update({
            "prompt_id": str(pid or "")[:64],
            "id": str(pid or "")[:64],
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


def _avg_duration_ms(last_finished: list) -> float | None:
    """Mean finished duration, or None when we have no real samples (no fallback)."""
    durs = [j["duration_ms"] for j in last_finished
            if isinstance(j.get("duration_ms"), (int, float)) and j["duration_ms"] > 0]
    if not durs:
        return None
    return max(1000.0, sum(durs) / len(durs))


def _estimate_progress(job: dict | None, avg_ms: float | None) -> dict | None:
    """Elapsed clock only — no fake % (REST has no live step counter)."""
    if not job:
        return None
    create = job.get("create_time")
    now_ms = time.time() * 1000.0
    elapsed = None
    if create is not None:
        ms = float(create) * 1000.0 if float(create) < 1e12 else float(create)
        elapsed = int(max(0.0, now_ms - ms))
    return {
        "prompt_id": job.get("prompt_id"),
        "percent": None,  # never invent a completion %
        "value": 0,
        "max": 0,
        "source": "elapsed",
        "updated_at": now_ms,
        "elapsed_ms": elapsed,
    }


def _estimate_queue_eta_ms(avg_ms: float | None, pending: int, running: bool,
                           progress_pct) -> int | None:
    """ETA only from real finished-job averages — never a hardcoded guess."""
    if avg_ms is None or avg_ms <= 0:
        return None
    running_remaining = (avg_ms * 0.5) if running else 0.0
    return int(round(running_remaining + max(0, pending) * avg_ms))


def _fetch_models_installed() -> dict:
    """Throttled GET /models/checkpoints + /models/loras (sparkDash inventory)."""
    now = time.time()
    if (now - _MODELS_CACHE.get("fetched_at", 0)) < _MODELS_TTL:
        return {
            "checkpoints": list(_MODELS_CACHE.get("checkpoints") or []),
            "loras": list(_MODELS_CACHE.get("loras") or []),
        }

    def _list(path):
        try:
            raw = _get_json(path)
        except Exception:
            return []
        if not isinstance(raw, list):
            return []
        out = []
        for x in raw:
            if isinstance(x, str) and x.strip():
                out.append(_basename(x))
            if len(out) >= 30:
                break
        return out

    ckpts = _list("/models/checkpoints")
    loras = _list("/models/loras")
    _MODELS_CACHE.update({"fetched_at": now, "checkpoints": ckpts, "loras": loras})
    return {"checkpoints": ckpts, "loras": loras}


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
        # NOTE: this ComfyUI (0.27+) does NOT expose queue_running/queue_pending
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
            "pytorch_version": None,
            "vram_total": "", "vram_free": "", "device_name": "",
            "device_type": None, "device_count": 0,
            "state": "offline",
            "chip": "offline",
            "running": None,
            "active_job": None,
            "pending_count": 0,
            "queue_total": 0,
            "pending": [],
            "pending_jobs": [],
            "models": [],
            "models_installed": None,
            "progress": None,
            "queue_eta_ms": None,
            "open_url": f"{BASE}/",
            "last_finished": [],
            "last_job": None,
        }
        with (_LAST_LOCK or _noop()):
            _LAST.update(out)
            _LAST["ts"] = now
        return dict(_LAST)

    return _build_snapshot(sysraw, queueraw, histraw, now)


def cancel_job(prompt_id: str) -> dict:
    """Cancel a running job or remove a pending one (sparkDash cancel/remove).

    Prefers modern /api/jobs/:id/cancel; falls back to interrupt + queue delete.
    Never raises — returns {ok, method, message}. Returns ok=False when ComfyUI
    is unreachable so the UI doesn't claim a cancel that never left the box.
    """
    pid = (prompt_id or "").strip()
    if not pid:
        return {"ok": False, "method": "none", "message": "prompt_id required"}

    # 1) Modern jobs API (newer ComfyUI builds)
    try:
        ok, code, text = _post_json(f"/api/jobs/{pid}/cancel", {})
        if ok or code in (200, 204):
            _invalidate_cache()
            return {"ok": True, "method": "api_jobs_cancel", "message": "cancelled"}
        if code >= 500:
            return {"ok": False, "method": "api_jobs_cancel",
                    "message": f"HTTP {code} {text}"}
        # code==0 → connection refused / timeout — ComfyUI is down
        if code == 0:
            return {"ok": False, "method": "api_jobs_cancel", "message": text or "ComfyUI unreachable"}
    except Exception as e:
        return {"ok": False, "method": "api_jobs_cancel", "message": _short_err(e)}

    # 2) Interrupt running (targeted by prompt_id when supported)
    try:
        _post_json("/interrupt", {"prompt_id": pid})
    except Exception:
        pass

    # 3) Delete from pending queue
    try:
        ok, code, text = _post_json("/queue", {"delete": [pid]})
        if ok:
            _invalidate_cache()
            return {"ok": True, "method": "queue_delete",
                    "message": "removed from queue / interrupted"}
        if code == 0:
            return {"ok": False, "method": "queue_delete",
                    "message": text or "ComfyUI unreachable"}
        if code:
            # interrupt alone may have worked for a running job
            _invalidate_cache()
            return {"ok": True, "method": "interrupt",
                    "message": f"interrupt requested (queue delete HTTP {code})"}
    except Exception as e:
        return {"ok": False, "method": "queue_delete", "message": _short_err(e)}

    _invalidate_cache()
    return {"ok": True, "method": "interrupt", "message": "interrupt requested"}


def _invalidate_cache():
    """Force next query_comfy() to re-hit ComfyUI after a mutation."""
    with (_LAST_LOCK or _noop()):
        _LAST["ts"] = 0.0


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
    running_job = None
    if running:
        try:
            running_job = _pick_running_job(running[0], "running")
        except Exception:
            running_job = None

    pending_jobs = []
    for p in pending:
        try:
            pending_jobs.append(_pick_running_job(p, "pending"))
        except Exception:
            continue

    hist = _parse_history(histraw)
    last_finished = hist.get("last_finished") or []
    last_job = last_finished[0] if last_finished else None

    # Models in the active graph (running + pending) — ckpt + lora + anything
    models = []
    _seen = set()
    for j in ([running_job] if running_job else []) + pending_jobs:
        for m in (j.get("models") or []):
            if m and m not in _seen:
                _seen.add(m)
                models.append(m)

    avg_ms = _avg_duration_ms(last_finished)
    progress = _estimate_progress(running_job, avg_ms) if running_job else None
    busy = bool(running_job) or bool(pending_jobs)
    eta_ms = (_estimate_queue_eta_ms(
        avg_ms, len(pending_jobs), bool(running_job),
        (progress or {}).get("percent"),
    ) if busy else None)

    models_installed = None
    try:
        models_installed = _fetch_models_installed()
        # hide empty inventory (matches sparkDash: section hidden when both empty)
        if not models_installed.get("checkpoints") and not models_installed.get("loras"):
            models_installed = None
    except Exception:
        models_installed = None

    state = "running" if running_job else ("queued" if pending_jobs else "idle")
    chip = "run" if running_job else (f"{len(pending_jobs)}q" if pending_jobs else "idle")

    out = {
        "ok": True,
        "offline": False,
        "error": None,
        **sys_,
        "state": state,
        "chip": chip,
        "running": running_job,
        "active_job": running_job,          # sparkDash-shaped alias
        "running_steps": (running_job or {}).get("steps"),
        "pending_count": len(pending_jobs),
        "queue_total": (1 if running_job else 0) + len(pending_jobs),
        "pending": pending_jobs,
        "pending_jobs": pending_jobs[:5],   # sparkDash-shaped alias (capped)
        "models": models,
        "models_installed": models_installed,
        "vram_used_pct": _vram_pct(sys_),
        "progress": progress,
        "queue_eta_ms": eta_ms,
        "queue_eta_source": "finished_avg" if eta_ms is not None else None,
        "avg_duration_ms": int(avg_ms) if avg_ms is not None else None,
        "open_url": f"{BASE}/",
        "last_finished": last_finished[:3],
        "last_job": last_job,
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
