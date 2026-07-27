#!/usr/bin/env python3
"""
Spark Console — portable core server.

Read-only local dashboard for a DGX Spark (or any Linux + NVIDIA box):
GPU/CPU/memory live graphs, model inventory, endpoint health, a configurable
service board, and an optional second node over SSH.

  GET  /                  console UI
  GET  /api/overview      everything the UI needs in one call
  GET  /api/latest        last collector snapshot
  GET  /api/live          fresh snapshot (bypasses the collector file)
  GET  /api/sparks        1 Hz rolling history for the graphs
  GET  /api/system        system metrics + local endpoints
  GET  /api/gpu-stats     GPU + process detail
  GET  /api/models        model inventory
  GET  /api/diagnostics   alerts and recommendations
  GET  /api/services      service board from services.json
  GET  /api/node2         optional second node (see remote_node.py)
  GET  /api/history       CSV rows for the last N hours
  GET  /api/summary       min/max/avg over the window
  GET  /api/baseline      reproducible baseline report
  GET  /api/csv           raw time-series CSV
  GET  /api/todos         scratch list  (POST to mutate)
  POST /api/refresh       re-run the collector now

The control plane (start/stop services, switch models, run scripts) is NOT part
of this tree — see the README. Everything here only reads.

Env: PORT, HOST, NODE2_HOST, CONSOLE_ALLOWED_HOSTS, SPARK_CONSOLE_SERVICES.
"""
from __future__ import annotations

import collections
import csv
import json
import os
import subprocess
import sys
import threading
import time as _time
from datetime import datetime, timezone
from urllib.parse import urlsplit

DATA_DIR = os.getenv("DGX_DATA_DIR",
                     os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
CSV_FILE = os.path.join(DATA_DIR, "performance_timeseries.csv")
JSON_SNAP = os.path.join(DATA_DIR, "latest_snapshot.json")
HERE = os.path.dirname(os.path.abspath(__file__))
CONSOLE_HTML = os.path.join(HERE, "console.html")

try:
    import psutil
    from fastapi import FastAPI, Request
    from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
    from pydantic import BaseModel
    from starlette.middleware.trustedhost import TrustedHostMiddleware
except ImportError:
    print("ERROR: pip install -r requirements.txt", file=sys.stderr)
    sys.exit(1)

sys.path.insert(0, HERE)
from collector import (  # noqa: E402
    diagnose, query_agent, query_endpoints, query_gpu, query_ollama, query_procs,
    system_metrics,
)
from model_inventory import query_inventory  # noqa: E402
from remote_node import node2_target, query_node2  # noqa: E402
import services_lite  # noqa: E402
import todos_lite  # noqa: E402

app = FastAPI(title="Spark Console")

# NOTE: every route handler below is a plain `def`, never `async def`. They all
# do blocking work (subprocess/systemctl/SSH/file reads), and an `async def`
# handler runs *on* the event loop, so one slow call would stall every other
# request. As plain `def`, Starlette runs them in its threadpool instead.
# Do not add `async` to these.

# --- hardening ---------------------------------------------------------------
# Binding loopback is not sufficient on its own. Two browser-borne attacks reach
# a localhost HTTP server:
#
#   DNS rebinding — a page on evil.com re-points its own hostname at 127.0.0.1,
#     which makes it same-origin with this server and lets it read every
#     endpoint. TrustedHostMiddleware rejects the forged Host header.
#   CSRF — a cross-origin POST with no custom headers is a "simple request", so
#     no preflight blocks it. _refuse_foreign_origin drops foreign Origins.
#
# Requests with no Origin at all are allowed: that is the scripted path (curl),
# not a browser one, and browsers always attach Origin to cross-origin writes.
# Reach it remotely with an SSH tunnel (-L 8085:127.0.0.1:8085), which keeps the
# Host as localhost. To serve a real hostname, list it in CONSOLE_ALLOWED_HOSTS.
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}
ALLOWED_HOSTS = _LOCAL_HOSTS | {
    h.strip().lower()
    for h in os.environ.get("CONSOLE_ALLOWED_HOSTS", "").split(",") if h.strip()
}

app.add_middleware(TrustedHostMiddleware, allowed_hosts=sorted(ALLOWED_HOSTS))


@app.middleware("http")
async def _refuse_foreign_origin(request: Request, call_next):
    """Block cross-origin writes. Async by design — header work only, no I/O."""
    if request.method in ("POST", "PUT", "PATCH", "DELETE"):
        origin = request.headers.get("origin")
        if origin and (urlsplit(origin).hostname or "").lower() not in ALLOWED_HOSTS:
            return JSONResponse(
                {"ok": False,
                 "error": f"cross-origin {request.method} refused (origin: {origin})"},
                status_code=403,
            )
    return await call_next(request)


class TodoRequest(BaseModel):
    action: str  # add | toggle | delete
    text: str = ""
    tag: str = "home"
    id: str = ""


# --- caches ------------------------------------------------------------------
# Anything slow (SSH, systemctl sweeps, endpoint probes) is refreshed on a
# thread and served from memory, so a page load never waits on it.
_cache_lock = threading.Lock()
_node2_cache: dict = {"reachable": False, "error": "not polled yet"}
_services_cache: dict = {}
_services_refresh = threading.Event()
_n1_hist: collections.deque = collections.deque(maxlen=90)  # 1s samples, 90s window
_n2_hist: collections.deque = collections.deque(maxlen=90)


def _gpu_quick() -> tuple[float, float] | None:
    """(util_pct, power_w) from one nvidia-smi call; None if unavailable."""
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=utilization.gpu,power.draw",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=3)
        parts = [p.strip() for p in out.stdout.strip().splitlines()[0].split(",")]
        blank = ("", "N/A", "[N/A]")
        util = float(parts[0]) if parts[0] not in blank else 0.0
        # GB10 reports instantaneous draw; the power.limit fields read N/A there
        pwr = float(parts[1]) if len(parts) > 1 and parts[1] not in blank else 0.0
        return util, pwr
    except Exception:
        return None


def _node1_sampler() -> None:
    """1 Hz local sampler feeding the rolling history graphs."""
    psutil.cpu_percent(None)  # prime the interval
    prev_net = psutil.net_io_counters()
    prev_t = _time.time()
    gpu = pwr = 0.0
    tick = 0
    while True:
        _time.sleep(1)
        try:
            now = _time.time()
            cpu = psutil.cpu_percent(None)
            vm, sw = psutil.virtual_memory(), psutil.swap_memory()
            net = psutil.net_io_counters()
            dt = max(now - prev_t, 0.1)
            rx = (net.bytes_recv - prev_net.bytes_recv) / dt / 1024
            tx = (net.bytes_sent - prev_net.bytes_sent) / dt / 1024
            prev_net, prev_t = net, now
            if tick % 2 == 0:  # nvidia-smi spawn costs ~50ms; every other tick
                g = _gpu_quick()
                if g is not None:
                    gpu, pwr = g
            tick += 1
            with _cache_lock:
                _n1_hist.append({"t": round(now), "cpu": cpu, "gpu": gpu,
                                 "mem": vm.percent, "swap": sw.percent,
                                 "rx": round(rx, 1), "tx": round(tx, 1),
                                 "pwr": round(pwr, 1)})
        except Exception:
            pass  # a sampling hiccup must not kill the thread


def _node2_refresher() -> None:
    """Poll the optional second node. Idles cheaply when none is configured."""
    while True:
        if not node2_target():
            with _cache_lock:
                _node2_cache.clear()
                _node2_cache.update({
                    "reachable": False,
                    "error": "not configured (set NODE2_HOST or NODE2_SSH_ALIAS)",
                })
            _time.sleep(60)
            continue
        try:
            n2 = query_node2()
            samples = n2.pop("samples", [])
            with _cache_lock:
                _node2_cache.clear()
                _node2_cache.update(n2)
                _n2_hist.extend(samples)
        except Exception as e:
            with _cache_lock:
                _node2_cache.update({"reachable": False, "error": str(e)[:200]})
            _time.sleep(10)
        _time.sleep(1)


def _services_snapshot() -> dict:
    """Cached service board; the first caller pays the sweep rather than being
    served an empty board that would look like "nothing is running"."""
    with _cache_lock:
        if _services_cache.get("iso_ts"):
            return dict(_services_cache)
    try:
        data = services_lite.list_services()
    except Exception as e:
        return {"services": [], "error": str(e)[:200], "active_operation": None}
    with _cache_lock:
        _services_cache.clear()
        _services_cache.update(data)
    return data


def _services_refresher() -> None:
    while True:
        try:
            data = services_lite.list_services()
            with _cache_lock:
                _services_cache.clear()
                _services_cache.update(data)
        except Exception as e:
            with _cache_lock:
                _services_cache["error"] = str(e)[:200]
        _services_refresh.wait(timeout=5)
        _services_refresh.clear()


threading.Thread(target=_node1_sampler, daemon=True).start()
threading.Thread(target=_node2_refresher, daemon=True).start()
threading.Thread(target=_services_refresher, daemon=True).start()


# --- snapshot helpers --------------------------------------------------------
def live_snapshot() -> dict:
    sys_m = system_metrics()
    gpus = query_gpu()
    procs = query_procs()
    ollama = query_ollama()
    agent = query_agent()
    inv = query_inventory()
    inv["agent"] = agent
    alerts = diagnose(sys_m, gpus, ollama, procs, inv)
    now = datetime.now(timezone.utc)
    return {
        "timestamp_utc": now.strftime("%Y-%m-%d %H:%M:%S"),
        "iso_ts": now.isoformat(),
        "hostname": os.uname().nodename,
        "system": sys_m,
        "num_gpus": len(gpus),
        "avg_util_pct": round(sum(g["util_gpu"] for g in gpus) / len(gpus), 1) if gpus else 0,
        "total_power_watts": round(sum(g["power_w"] for g in gpus), 2),
        "avg_temp_c": round(sum(g["temp_c"] for g in gpus) / len(gpus), 1) if gpus else 0,
        "gpus": gpus,
        "processes": procs,
        "ollama": ollama,
        "agent": agent,
        "endpoints": query_endpoints(),
        "models": inv,
        "alerts": alerts,
        "alert_count": {
            lvl: sum(1 for a in alerts if a["level"] == lvl)
            for lvl in ("critical", "warning", "info")
        },
    }


def _load_history_rows(hours: int) -> list[dict]:
    if not os.path.exists(CSV_FILE):
        return []
    cutoff = datetime.now(timezone.utc).timestamp() - hours * 3600
    rows = []
    with open(CSV_FILE) as f:
        for row in csv.DictReader(f):
            try:
                ts = datetime.fromisoformat(row["iso_ts"].replace("Z", "+00:00"))
                if ts.timestamp() >= cutoff:
                    rows.append(row)
            except (KeyError, ValueError, AttributeError):
                continue
    return rows


def _series_stats(values: list[float]) -> dict:
    if not values:
        return {"min": None, "max": None, "avg": None, "last": None}
    return {"min": round(min(values), 1), "max": round(max(values), 1),
            "avg": round(sum(values) / len(values), 1), "last": round(values[-1], 1)}


def _fleet_alerts(node1: dict, node2: dict, services: dict) -> list[dict]:
    """One ranked list, so the UI can honestly say whether anything needs you."""
    out = [{"level": a.get("level", "info"), "host": "node1",
            "message": a.get("message", "")} for a in (node1.get("alerts") or [])]
    # A first poll after startup is not an outage — do not report it as one.
    booting = "not polled yet" in str(node2.get("error", ""))
    configured = "not configured" not in str(node2.get("error", ""))
    if configured and not node2.get("reachable") and not booting:
        out.append({"level": "critical", "host": "node2",
                    "message": f"unreachable ({str(node2.get('error', '?'))[:70]})"})
    for svc in (services.get("services") or []):
        if svc.get("unit_missing"):
            out.append({"level": "warning", "host": "services",
                        "message": f"{svc['label']}: {svc.get('extra') or 'unit missing'}"})
        elif svc.get("critical") and not svc.get("active"):
            out.append({"level": "critical", "host": "services",
                        "message": f"{svc['label']} is not running"})
    rank = {"critical": 0, "warning": 1, "info": 2}
    out.sort(key=lambda a: rank.get(a["level"], 3))
    return out


# --- routes ------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def console():
    if os.path.exists(CONSOLE_HTML):
        return HTMLResponse(content=open(CONSOLE_HTML).read())
    return HTMLResponse(content="<h1>console.html missing</h1>", status_code=500)


@app.get("/api/latest")
def api_latest():
    if os.path.exists(JSON_SNAP):
        try:
            return json.load(open(JSON_SNAP))
        except ValueError:
            pass  # half-written snapshot — fall through to a live read
    return live_snapshot()


@app.get("/api/live")
def api_live():
    return live_snapshot()


@app.get("/api/csv")
def api_csv():
    if not os.path.exists(CSV_FILE):
        return PlainTextResponse("No data yet — run collector.py first", status_code=404)
    return PlainTextResponse(content=open(CSV_FILE).read(), media_type="text/csv")


@app.get("/api/gpu-stats")
def api_gpu_stats():
    gpus = query_gpu()
    now = datetime.now(timezone.utc)
    return {
        "timestamp_utc": now.strftime("%Y-%m-%d %H:%M:%S"),
        "iso_ts": now.isoformat(),
        "gpus": gpus,
        "processes": query_procs(),
        "avg_util_pct": round(sum(g["util_gpu"] for g in gpus) / len(gpus), 1) if gpus else 0,
        "total_power_watts": round(sum(g["power_w"] for g in gpus), 2),
        "avg_temp_c": round(sum(g["temp_c"] for g in gpus) / len(gpus), 1) if gpus else 0,
    }


@app.get("/api/system")
def api_system():
    now = datetime.now(timezone.utc)
    return {
        "timestamp_utc": now.strftime("%Y-%m-%d %H:%M:%S"),
        "iso_ts": now.isoformat(),
        "system": system_metrics(),
        "ollama": query_ollama(),
        "agent": query_agent(),
        "endpoints": query_endpoints(),
    }


@app.get("/api/models")
def api_models():
    inv = query_inventory()
    inv["agent"] = query_agent()
    return inv


@app.get("/api/diagnostics")
def api_diagnostics():
    sys_m = system_metrics()
    gpus = query_gpu()
    ollama = query_ollama()
    inv = query_inventory()
    inv["agent"] = query_agent()
    alerts = diagnose(sys_m, gpus, ollama, query_procs(), inv)
    return {
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "alerts": alerts,
        "alert_count": {
            lvl: sum(1 for a in alerts if a["level"] == lvl)
            for lvl in ("critical", "warning", "info")
        },
        "ollama": ollama,
        "models": inv,
    }


@app.get("/api/sparks")
def api_sparks():
    with _cache_lock:
        return {"node1": list(_n1_hist), "node2": list(_n2_hist)}


@app.get("/api/node2")
def api_node2():
    with _cache_lock:
        return dict(_node2_cache)


@app.get("/api/services")
def api_services():
    return _services_snapshot()


@app.get("/api/todos")
def api_todos():
    return {"todos": todos_lite.get_todos()}


@app.post("/api/todos")
def api_todos_post(req: TodoRequest):
    return {"todos": todos_lite.apply(req.action, text=req.text, tag=req.tag,
                                      tid=req.id)}


@app.get("/api/actions")
def api_actions():
    """Empty by design: one-click scripts are part of the private control plane."""
    return {"actions": []}


@app.get("/api/overview")
def api_overview():
    """Everything the console page needs in one call."""
    snap = live_snapshot()
    with _cache_lock:
        node2 = dict(_node2_cache)
    services = _services_snapshot()
    return {
        "node1": snap,
        "node2": node2,
        "services": services,
        "todos": todos_lite.get_todos(),
        "actions": [],
        # Panels below are supplied by the private control-plane modules; the UI
        # renders them empty rather than erroring (it defaults with `|| {}`).
        "projects": [],
        "automation": {"jobs": [], "counts": {}, "upcoming": [], "failing": []},
        "backups": {"entries": [], "counts": {}, "issues": []},
        "links": {"groups": [], "counts": {}},
        "alerts": _fleet_alerts(snap, node2, services),
    }


@app.get("/api/history")
def api_history(hours: int = 24):
    rows = _load_history_rows(hours)
    return {"rows": rows, "hours": hours, "count": len(rows)}


@app.get("/api/summary")
def api_summary(hours: int = 24):
    rows = _load_history_rows(hours)

    def col(name: str) -> list[float]:
        vals = []
        for r in rows:
            try:
                vals.append(float(r[name]))
            except (KeyError, TypeError, ValueError):
                continue
        return vals

    return {
        "hours": hours,
        "samples": len(rows),
        "cpu_pct": _series_stats(col("cpu_pct")),
        "mem_used_gb": _series_stats(col("mem_used_gb")),
        "gpu_util_pct": _series_stats(col("gpu_util_pct")),
        "gpu_power_w": _series_stats(col("gpu_power_w")),
    }


@app.get("/api/baseline")
def api_baseline(hours: int = 24):
    """Reproducible baseline report (percentiles, availability, longest runs)."""
    try:
        import baseline_summary
        rows = baseline_summary.load_rows(hours)
        return {"ok": True, "report": baseline_summary.summarize(rows, hours)}
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"[:200]},
                            status_code=500)


@app.post("/api/refresh")
def api_refresh():
    """Re-run the collector now and return the fresh snapshot."""
    script = os.path.join(HERE, "collector.py")
    try:
        result = subprocess.run([sys.executable, script], capture_output=True,
                                text=True, timeout=60)
        if result.returncode != 0:
            return JSONResponse(
                {"ok": False, "error": (result.stderr or "collector failed").strip()[:400]},
                status_code=500)
        snap = json.load(open(JSON_SNAP)) if os.path.exists(JSON_SNAP) else None
        return {"ok": True, "snapshot": snap}
    except subprocess.TimeoutExpired:
        return JSONResponse({"ok": False, "error": "collector timed out"},
                            status_code=504)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8085))
    host = os.environ.get("HOST", "127.0.0.1")
    print(f"Spark Console → http://{host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="info")
