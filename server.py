#!/usr/bin/env python3
"""
DGX Spark Performance Dashboard — FastAPI server.
  GET /                  → dashboard HTML
  GET /api/latest        → latest snapshot JSON
  GET /api/csv           → time-series CSV
  GET /api/gpu-stats     → live GPU data
  GET /api/system        → live system metrics
  GET /api/diagnostics   → live alerts + recommendations
Runs on port 8085 by default.
"""
from __future__ import annotations

import csv
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from io import StringIO

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
CSV_FILE = os.path.join(DATA_DIR, "performance_timeseries.csv")
JSON_SNAP = os.path.join(DATA_DIR, "latest_snapshot.json")
DASHBOARD_HTML = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
CONSOLE_HTML = os.path.join(os.path.dirname(os.path.abspath(__file__)), "console.html")

try:
    from fastapi import FastAPI, Request
    from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse, JSONResponse
    from pydantic import BaseModel
    from starlette.middleware.trustedhost import TrustedHostMiddleware
except ImportError:
    print("ERROR: pip install fastapi uvicorn", file=sys.stderr)
    sys.exit(1)

from urllib.parse import urlsplit  # noqa: E402

# Import collector functions for live queries
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from collector import (  # noqa: E402
    diagnose, query_endpoints, query_gpu, query_hermes, query_ollama,
    query_procs, system_metrics,
)
from model_inventory import query_inventory  # noqa: E402
from remote_node import query_node2  # noqa: E402
import projects_status  # noqa: E402
from model_control import (  # noqa: E402
    can_switch,
    control_status,
    get_operation,
    kill_orphans,
    list_operations,
    stop_models,
    switch_model,
    sync_hermes,
    unload_ollama,
)
from service_control import (  # noqa: E402
    get_operation as get_svc_operation,
    list_services,
    register_service,
    service_action,
    unregister_service,
)
import automation_status  # noqa: E402
import backups_status  # noqa: E402
import token_usage  # noqa: E402
import fleet_links  # noqa: E402
import fleet_nodes  # noqa: E402
import quick_actions  # noqa: E402
import desktop_launch  # noqa: E402
import comfy_api  # noqa: E402
import energy_cost  # noqa: E402

app = FastAPI(title="DGX Spark Performance Dashboard")

# NOTE: every route handler below is a plain `def`, never `async def`. They all do
# blocking work (subprocess/systemctl/SSH/file reads), and an `async def` handler
# runs *on* the event loop — one slow call freezes every other request. Measured
# before this was fixed: /api/latest went 1.6ms -> 1.36s while a single
# /api/overview was in flight, and a service restart could stall the whole panel
# for the 60s systemctl timeout. As plain `def`, Starlette runs them in its
# threadpool and slow calls stay isolated. Do not add `async` to these.

# --- control-plane hardening -------------------------------------------------
# This is not just a dashboard: it can stop inference and restart services. Two
# browser-borne attacks reach it even though it binds loopback only.
#
#   DNS rebinding — a page on evil.com re-points its own hostname at 127.0.0.1,
#     becoming same-origin with the console, and can then READ every endpoint
#     and POST to /api/models/stop. TrustedHostMiddleware rejects the forged
#     Host header before any handler runs.
#   CSRF — several mutating routes take no request body, so a plain cross-origin
#     POST (a "simple request", no preflight to block it) used to reach the
#     handler. _refuse_foreign_origin rejects any Origin we don't own.
#
# Requests carrying no Origin at all are still allowed: that is the scripted
# path (curl, register-console-service.sh, Hermes agents), not a browser one,
# and browsers always attach Origin to cross-origin mutating requests.
# Remote access is expected to be an SSH tunnel (-L 8085:127.0.0.1:8085), which
# keeps the Host as localhost; set CONSOLE_ALLOWED_HOSTS=a,b to widen it.
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}
ALLOWED_HOSTS = _LOCAL_HOSTS | {
    h.strip().lower()
    for h in os.environ.get("CONSOLE_ALLOWED_HOSTS", "").split(",") if h.strip()
}

app.add_middleware(TrustedHostMiddleware, allowed_hosts=sorted(ALLOWED_HOSTS))


@app.middleware("http")
async def _refuse_foreign_origin(request: Request, call_next):
    """Block cross-origin writes. Async by design — pure header work, no I/O."""
    if request.method in ("POST", "PUT", "PATCH", "DELETE"):
        origin = request.headers.get("origin")
        if origin and (urlsplit(origin).hostname or "").lower() not in ALLOWED_HOSTS:
            return JSONResponse(
                {"ok": False,
                 "error": f"cross-origin {request.method} refused (origin: {origin})"},
                status_code=403,
            )
    return await call_next(request)


class SwitchRequest(BaseModel):
    key: str
    fast: bool = False


class TodoRequest(BaseModel):
    action: str  # add | toggle | delete
    text: str = ""
    tag: str = "home"
    id: str = ""


class ServiceActionRequest(BaseModel):
    action: str  # start | stop | restart
    model: str | None = None  # node2-deep only


class ServiceRegisterRequest(BaseModel):
    """Register a new app/server on the Spark Console Services board.

    Agents (Hermes/Telegram/Grok) MUST call this when they create a new HTTP
    server — do not wait for the human to ask.
    """
    id: str
    label: str
    port: int | None = None
    unit: str | None = None
    detail: str = ""
    group: str = "apps"
    kind: str | None = None  # systemd | probe-only
    probe_url: str | None = None
    hint: str = ""
    show_projects_tile: bool = True
    replace: bool = True


class ComfyCancelRequest(BaseModel):
    """Cancel a running ComfyUI job or remove a pending one (sparkDash 1.6)."""
    prompt_id: str


# ---- background refreshers: node2 + projects cached so pages never block ----
import collections  # noqa: E402
import threading  # noqa: E402
import time as _time  # noqa: E402

import psutil  # noqa: E402

_cache_lock = threading.Lock()
_node2_cache: dict = {"reachable": False, "error": "not polled yet"}
_projects_cache: dict = {"projects": []}
_n1_hist: collections.deque = collections.deque(maxlen=90)   # 1s samples, 90s window
_n2_hist: collections.deque = collections.deque(maxlen=90)
# Fleet appliances + control-center panels: all served from caches so a slow
# SSH hop or a stalled script can never block a page load.
_pi_cache: dict = {"reachable": False, "error": "not polled yet", "id": "pi"}
_start9_cache: dict = {"reachable": False, "error": "not polled yet", "id": "start9"}
_automation_cache: dict = {"jobs": [], "counts": {}, "upcoming": [], "failing": []}
_backups_cache: dict = {"entries": [], "counts": {}, "issues": []}
_links_cache: dict = {"groups": [], "counts": {}}
# Service board: one systemctl sweep + an endpoint probe per row, so it is polled
# on a thread like every other panel instead of rebuilt on the request path.
_services_cache: dict = {}
_services_refresh = threading.Event()
# ComfyUI job panel: on-demand (20 min idle timer) so usually offline. Polled on
# a thread so GET /api/comfy is a fast cache read; the refresher backs off hard
# while ComfyUI is down so a dead box can't hammer loopback.
_comfy_cache: dict = {"offline": True, "state": "offline", "error": "not polled yet"}
_comfy_offline_backoff = 10.0  # seconds between polls while ComfyUI is down


def _gpu_quick() -> tuple[float, float, float] | None:
    """Return (util_pct, power_w, temp_c) from one nvidia-smi call; None on failure.

    Prefer average power draw for energy; Instantaneous undersamples on GB10.
    Temperature rides along on a call we already make every other tick, so the
    1 Hz ring buffer can feed /api/tick a complete host reading without the
    phone ever paying for the full (~1 s) live snapshot.
    """
    try:
        out = subprocess.run(
            ["nvidia-smi",
             "--query-gpu=utilization.gpu,power.draw.average,power.draw.instant,temperature.gpu",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=3)
        if out.returncode != 0 or not out.stdout.strip():
            out = subprocess.run(
                ["nvidia-smi",
                 "--query-gpu=utilization.gpu,power.draw,temperature.gpu",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=3)
            line = out.stdout.strip().splitlines()[0]
            parts = [p.strip() for p in line.split(",")]
            num = lambda i: (float(parts[i])
                             if len(parts) > i and parts[i] not in ("", "N/A", "[N/A]")
                             else 0.0)
            return num(0), num(1), num(2)
        line = out.stdout.strip().splitlines()[0]
        parts = [p.strip() for p in line.split(",")]
        num = lambda i: (float(parts[i])
                         if len(parts) > i and parts[i] not in ("", "N/A", "[N/A]")
                         else 0.0)
        util, avg, inst, tmp = num(0), num(1), num(2), num(3)
        return util, (avg if avg > 0 else inst), tmp
    except Exception:
        return None


def _energy_tick_from_caches(t: float | None = None) -> None:
    """Fold already-paid live watts into the energy log — no nvidia-smi / SSH."""
    try:
        with _cache_lock:
            n1 = _n1_hist[-1] if _n1_hist else {}
            n2 = dict(_node2_cache)
            n2_hist_last = _n2_hist[-1] if _n2_hist else {}
            pi = dict(_pi_cache)
            s9 = dict(_start9_cache)
        n1_pwr = n1.get("pwr") if n1 else None
        n2_pwr = None
        if n2.get("reachable"):
            g2 = (n2.get("gpus") or [{}])[0]
            n2_pwr = g2.get("power_w")
            if n2_pwr is None:
                n2_pwr = n2_hist_last.get("pwr")
        energy_cost.record_sample({
            "node1": n1_pwr,
            "node2": n2_pwr,
            "pi": pi.get("power_w") if pi.get("reachable") else None,
            "start9": s9.get("power_w") if s9.get("reachable") else None,
        }, t=t)
    except Exception:
        pass


def _node1_sampler():
    """1 Hz local sampler for the System Monitor-style history graphs.

    Every ~30s, folds all live cache watts into the electricity log using power
    we already paid for — no extra nvidia-smi for energy.
    """
    psutil.cpu_percent(None)  # prime
    prev_net = psutil.net_io_counters()
    prev_t = _time.time()
    gpu: float = 0.0
    pwr: float = 0.0
    tmp: float = 0.0
    tick = 0
    while True:
        _time.sleep(1)
        try:
            now = _time.time()
            cpu = psutil.cpu_percent(None)
            vm = psutil.virtual_memory()
            sw = psutil.swap_memory()
            net = psutil.net_io_counters()
            dt = max(now - prev_t, 0.1)
            rx = (net.bytes_recv - prev_net.bytes_recv) / dt / 1024
            tx = (net.bytes_sent - prev_net.bytes_sent) / dt / 1024
            prev_net, prev_t = net, now
            if tick % 2 == 0:  # nvidia-smi spawn is ~50ms; every other tick
                g = _gpu_quick()
                if g is not None:
                    gpu, pwr, tmp = g
            tick += 1
            with _cache_lock:
                # pwr = GPU draw watts (not wall); GB10 has no RAPL/module meter
                _n1_hist.append({"t": round(now), "cpu": cpu, "gpu": gpu,
                                 "mem": vm.percent, "swap": sw.percent,
                                 "rx": round(rx, 1), "tx": round(tx, 1),
                                 "pwr": round(pwr, 1), "temp": round(tmp)})
            if tick % 30 == 0:
                _energy_tick_from_caches(t=now)
        except Exception:
            pass


def _node2_refresher():
    """Continuous SSH loop; each call carries 10×1s samples for the graphs."""
    while True:
        try:
            n2 = query_node2()
            samples = n2.pop("samples", [])
            with _cache_lock:
                _node2_cache.clear()
                _node2_cache.update(n2)
                _n2_hist.extend(samples)
        except Exception as e:  # never let the thread die
            with _cache_lock:
                _node2_cache.update({"reachable": False, "error": str(e)[:200]})
            _time.sleep(10)
        _time.sleep(1)


def _services_snapshot() -> dict:
    """Cached service board, computed synchronously on the very first read.

    The cold-start path matters: the console watchdog curls /api/services every
    2 minutes and an empty-but-200 board would look healthy while telling the UI
    nothing is running. So the first caller pays the full sweep rather than
    being served an empty cache.
    """
    with _cache_lock:
        if _services_cache.get("iso_ts"):
            return dict(_services_cache)
    try:
        data = list_services()
    except Exception as e:
        return {"services": [], "error": str(e)[:200], "active_operation": None}
    with _cache_lock:
        _services_cache.clear()
        _services_cache.update(data)
    return data


def _services_refresher():
    """Re-sweep the board every 5s, or immediately when an action asks for it.

    start/stop/restart set _services_refresh so the UI reflects the click on the
    next poll instead of showing pre-click state for up to a full interval.
    """
    while True:
        try:
            data = list_services()
            with _cache_lock:
                _services_cache.clear()
                _services_cache.update(data)
        except Exception as e:
            with _cache_lock:
                _services_cache["error"] = str(e)[:200]
        _services_refresh.wait(timeout=5)
        _services_refresh.clear()


def _comfy_refresher():
    """Poll ComfyUI on a thread so GET /api/comfy is a fast cache read.

    ComfyUI is on-demand (20 min idle timer) so it is usually DOWN. When down we
    back off to _comfy_offline_backoff seconds between polls — a ConnectionRefused
    is near-instant but we don't want to spin against loopback all day. When it
    comes alive the module's own live snapshot fans out and state flips cleanly.
    """
    while True:
        try:
            snap = comfy_api.query_comfy()
        except Exception:  # never let the thread die
            snap = {"offline": True, "state": "offline", "error": "comfy poll errored"}
        was_offline = bool(snap.get("offline"))
        with _cache_lock:
            _comfy_cache.clear()
            _comfy_cache.update(snap)
        _time.sleep(_comfy_offline_backoff if was_offline else 5)


def _projects_refresher():
    while True:
        try:
            pr = projects_status.query_projects()
            with _cache_lock:
                _projects_cache.clear()
                _projects_cache.update(pr)
        except Exception:
            pass
        _time.sleep(60)


def _fleet_refresher():
    """Pi + Start9 over LAN SSH. Slow-changing appliances — 30s is plenty.

    Feeds Pi/Start9 (and any Spark watts already in live caches) into the
    electricity log — never an extra nvidia-smi; Sparks are sampled from hist
    by the node1/node2 threads.
    """
    while True:
        for query, cache in ((fleet_nodes.query_pi, _pi_cache),
                             (fleet_nodes.query_start9, _start9_cache)):
            try:
                data = query()
                with _cache_lock:
                    cache.clear()
                    cache.update(data)
            except Exception as e:  # never let the thread die
                with _cache_lock:
                    cache.update({"reachable": False, "error": str(e)[:200]})
        # Energy: appliances from this poll; Sparks from hist/cache (free).
        _energy_tick_from_caches()
        _time.sleep(30)


def _panels_refresher():
    """Automation (file reads, ms) and backups (log tails) + launcher catalog.

    Backups and the launcher both read the fleet caches, so give the first SSH
    poll a head start — otherwise the first pass records every remote tier as
    "unknown" and the UI shows a cold-start lie for 30s.
    """
    _time.sleep(6)
    tick = 0
    while True:
        try:
            auto = automation_status.query_automation()
            with _cache_lock:
                _automation_cache.clear()
                _automation_cache.update(auto)
        except Exception:
            pass
        with _cache_lock:
            pi, s9 = dict(_pi_cache), dict(_start9_cache)
        try:
            back = backups_status.query_backups(pi, s9)
            with _cache_lock:
                _backups_cache.clear()
                _backups_cache.update(back)
        except Exception:
            pass
        try:
            links = fleet_links.query_links(_services_snapshot(), s9, pi)
            with _cache_lock:
                _links_cache.clear()
                _links_cache.update(links)
        except Exception:
            pass
        tick += 1
        _time.sleep(30)


def _inference_up_now() -> bool:
    """True when any local/fabric chat endpoint answers /v1/models right now.

    Used to demote sticky overnight Connection-error job fails while the
    owner's chosen mode is already healthy again.
    """
    import urllib.request
    fabric = os.environ.get("SPARK_FABRIC_IP", "192.168.100.10").strip() or "192.168.100.10"
    for url in (
        "http://127.0.0.1:8888/v1/models",
        "http://127.0.0.1:8889/v1/models",
        f"http://{fabric}:8800/v1/models",
    ):
        try:
            with urllib.request.urlopen(url, timeout=2) as r:
                if r.status == 200:
                    return True
        except Exception:
            continue
    return False


def _fleet_alerts(node1: dict, node2: dict, pi: dict, start9: dict,
                  automation: dict, backups: dict, projects: list) -> list[dict]:
    """One ranked alert list for the whole fleet — the console's headline claim
    is 'nothing needs you right now', so every source has to feed this."""
    out: list[dict] = []

    def add(level, host, message):
        out.append({"level": level, "host": host, "message": message})

    def booting(host: dict) -> bool:
        # First poll after a restart is not an outage — do not page for it.
        return "not polled yet" in str(host.get("error", ""))

    # Optional app watchdogs (e.g. BetIntel) fail loudly when you intentionally
    # stop the app. That is Jobs-tab noise, not "Needs you" critical.
    def optional_noise(name: str) -> bool:
        # Prefix match covers betintel-watchdog.service / .timer job names
        return (name or "").lower().startswith("betintel-")

    for a in (node1.get("alerts") or []):
        add(a.get("level", "info"), "node1", a.get("message", ""))
    if node2 and not node2.get("reachable") and not booting(node2):
        add("critical", "node2", f"unreachable over fabric ({node2.get('error', '?')[:70]})")
    elif node2 and (node2.get("swap") or {}).get("used_gb", 0) >= 2:
        # Same rule as node1 diagnose: swap while a peer model is up is expected.
        # Only surface when swap is near full (thrash), not every multi-GB spill.
        sw = node2.get("swap") or {}
        used = float(sw.get("used_gb") or 0)
        total = float(sw.get("total_gb") or 0) or 16.0
        if (used / total) * 100 >= 90:
            add("warning", "node2", f"swap {used} G ({int(used / total * 100)}%) — thrash risk")
    for host, data in (("pi", pi), ("start9", start9)):
        if booting(data):
            continue
        for issue in (data.get("issues") or []):
            add(issue.get("level", "warning"), host, issue.get("message", ""))
    for job in (automation.get("failing") or []):
        if optional_noise(str(job.get("name") or "")):
            continue
        err = str(job.get("error") or "")
        # Overnight Connection errors stick on last_status until next schedule.
        # When a resident engine is answering NOW, that is stale — warn, don't
        # paint the whole console red for a cold-load blip.
        stale_conn = bool(
            re.search(r"Connection error|APIConnectionError|ConnectError", err, re.I)
        )
        if stale_conn and _inference_up_now():
            add("warning", "automation",
                f"{job['name']} ({job['layer']}) stale fail — {err[:100]} "
                f"(inference up now; clears next run)"[:150])
            continue
        add("critical", "automation", f"{job['name']} ({job['layer']}) failed — {job['error']}"[:150])
    for unit in (automation.get("failed_units") or []):
        if optional_noise(str(unit)):
            continue
        add("critical", "automation", f"systemd unit failed: {unit}")
    for issue in (backups.get("issues") or []):
        add(issue.get("level", "warning"), "backups", issue.get("message", ""))
    for p in (projects or []):
        if p.get("status") == "bad":
            add("critical", "projects", f"{p.get('name')}: {p.get('detail')}")
    rank = {"critical": 0, "warning": 1, "info": 2}
    out.sort(key=lambda a: rank.get(a["level"], 3))
    return out


threading.Thread(target=_node1_sampler, daemon=True).start()
threading.Thread(target=_node2_refresher, daemon=True).start()
threading.Thread(target=_projects_refresher, daemon=True).start()
# Seed Sparks energy history from the 5‑min CSV once, then keep logging.
try:
    energy_cost.ensure_ready()
except Exception:
    pass

threading.Thread(target=_fleet_refresher, daemon=True).start()
threading.Thread(target=_panels_refresher, daemon=True).start()
threading.Thread(target=_services_refresher, daemon=True).start()
threading.Thread(target=_comfy_refresher, daemon=True).start()


# iPhone PWA will otherwise keep a stale console.html forever. HTML is the
# app — never let WKWebView treat it as cacheable.
_NO_STORE = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}


def console_build_info() -> dict:
    """Visible page version: mtime stamp + last git hash that touched console.html.

    mtime changes on every save so a dirty tree still bumps the label; git
    hash is the durable compare against another device.
    """
    mtime = os.path.getmtime(CONSOLE_HTML) if os.path.exists(CONSOLE_HTML) else 0
    stamp = datetime.fromtimestamp(mtime).strftime("%m%d-%H%M") if mtime else "dev"
    git = ""
    try:
        git = subprocess.check_output(
            [
                "git", "-C", os.path.dirname(CONSOLE_HTML),
                "log", "-1", "--format=%h", "--", "console.html",
            ],
            timeout=1.5,
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        git = ""
    build = f"{stamp}-{git}" if git else stamp
    return {"build": build, "git": git or None, "html_mtime": int(mtime), "stamp": stamp}


def _serve_console_html(path: str) -> HTMLResponse:
    if not os.path.exists(path):
        return HTMLResponse(content="<h1>console.html missing</h1>", status_code=500, headers=_NO_STORE)
    html = open(path, encoding="utf-8").read()
    html = html.replace("{{CONSOLE_BUILD}}", console_build_info()["build"])
    return HTMLResponse(content=html, headers=_NO_STORE)


@app.get("/", response_class=HTMLResponse)
def console():
    return _serve_console_html(CONSOLE_HTML)


@app.get("/api/version")
def api_version():
    """What the Spark is serving right now — iPhone compares this to its baked-in build."""
    return JSONResponse(console_build_info(), headers=_NO_STORE)


_PWA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pwa")


@app.get("/manifest.webmanifest")
@app.get("/manifest.json")
def pwa_manifest():
    manifest = os.path.join(_PWA_DIR, "manifest.webmanifest")
    if not os.path.exists(manifest):
        return JSONResponse({"error": "manifest missing"}, status_code=404)
    return FileResponse(manifest, media_type="application/manifest+json")


@app.get("/icon-192.png")
def icon_192():
    return pwa_file("icon-192.png")


@app.get("/icon-512.png")
def icon_512():
    return pwa_file("icon-512.png")


@app.get("/icon-maskable-512.png")
def icon_maskable():
    return pwa_file("icon-maskable-512.png")


@app.get("/sw.js")
def sw_js():
    return pwa_file("sw.js")


def pwa_file(name: str):
    """Serve a file from the pwa/ dir (icons/worker) for Add-to-Home-Screen."""
    fp = os.path.join(_PWA_DIR, name)
    if not os.path.exists(fp):
        return JSONResponse({"error": f"{name} missing"}, status_code=404)
    return FileResponse(fp)


@app.get("/v1", response_class=HTMLResponse)
def console_v1():
    """Pre-redesign console (6 tabs, 3 themes), kept for A/B compare and instant fallback.

    Same live APIs as `/` — only the page differs. Retire once v2 has proven itself.
    """
    legacy = CONSOLE_HTML + ".bak-2026-08-02-pre-v2"
    if os.path.exists(legacy):
        return HTMLResponse(content=open(legacy).read())
    return HTMLResponse(content="<h1>v1 console not present</h1>", status_code=404)


@app.get("/classic", response_class=HTMLResponse)
def dashboard_retired():
    """Classic Plotly UI retired 2026-07-20 — redirect-style notice."""
    return HTMLResponse(
        content=(
            "<!doctype html><meta charset=utf-8>"
            "<title>Classic view retired</title>"
            "<body style='font:16px system-ui;background:#0d1117;color:#e9eef4;"
            "display:flex;min-height:100vh;align-items:center;justify-content:center'>"
            "<div style='max-width:28rem;text-align:center;line-height:1.5'>"
            "<p><strong>Classic view is gone.</strong></p>"
            "<p style='color:#93a0ad'>Use the Spark Console control panel.</p>"
            "<p><a href='/' style='color:#5aa9ff'>→ Spark Console</a></p>"
            "</div></body>"
        ),
        status_code=410,
    )


@app.get("/api/latest")
def api_latest():
    if os.path.exists(JSON_SNAP):
        return json.load(open(JSON_SNAP))
    return api_live_snapshot()


@app.get("/api/csv")
def api_csv():
    if not os.path.exists(CSV_FILE):
        legacy = os.path.join(DATA_DIR, "gpu_timeseries.csv")
        if os.path.exists(legacy):
            return PlainTextResponse(content=open(legacy).read(), media_type="text/csv")
        return PlainTextResponse("No data yet — run collector.py first", status_code=404)
    return PlainTextResponse(content=open(CSV_FILE).read(), media_type="text/csv")


@app.get("/api/gpu-stats")
def api_gpu_stats():
    gpus = query_gpu()
    procs = query_procs()
    now = datetime.now(timezone.utc)
    avg_u = round(sum(g["util_gpu"] for g in gpus) / len(gpus), 1) if gpus else 0
    return {
        "timestamp_utc": now.strftime("%Y-%m-%d %H:%M:%S"),
        "iso_ts": now.isoformat(),
        "gpus": gpus,
        "processes": procs,
        "avg_util_pct": avg_u,
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
        "hermes": query_hermes(),
    }


@app.get("/api/models")
def api_models():
    inv = query_inventory()
    inv["hermes"] = query_hermes()
    return inv


@app.get("/api/diagnostics")
def api_diagnostics():
    sys_m = system_metrics()
    gpus = query_gpu()
    procs = query_procs()
    ollama = query_ollama()
    inv = query_inventory()
    inv["hermes"] = query_hermes()
    # Pass live endpoints so the memory alert knows a resident engine explains
    # high RAM (see diagnose()). Sync handler → FastAPI threadpool, so the
    # blocking probe cannot stall the event loop.
    alerts = diagnose(sys_m, gpus, ollama, procs, inv, query_endpoints())
    return {
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "alerts": alerts,
        "alert_count": {
            "critical": sum(1 for a in alerts if a["level"] == "critical"),
            "warning": sum(1 for a in alerts if a["level"] == "warning"),
            "info": sum(1 for a in alerts if a["level"] == "info"),
        },
        "ollama": ollama,
        "hermes": inv["hermes"],
        "models": inv,
        "vllm": inv.get("active_vllm") or [],
    }


_LIVE_SNAP_CACHE: dict = {"ts": 0.0, "payload": None}
_LIVE_SNAP_TTL = 2.5  # overview can stampede on focus; reuse a fresh snap


def api_live_snapshot(force: bool = False):
    """Full node1 snapshot (smi + procs + inventory). Short-TTL cached.

    The 1 Hz hist already owns present-tense GPU/CPU/mem for the UI; this
    path is for structure (endpoints, models, alerts). Caching 2.5s avoids
    back-to-back nvidia-smi when refresh + visibilitychange collide.
    """
    now = _time.time()
    cached = _LIVE_SNAP_CACHE.get("payload")
    if (not force and cached
            and (now - float(_LIVE_SNAP_CACHE.get("ts") or 0)) < _LIVE_SNAP_TTL):
        return _snap_copy(cached)

    sys_m = system_metrics()
    gpus = query_gpu()
    procs = query_procs()
    ollama = query_ollama()
    hermes = query_hermes()
    inv = query_inventory()
    inv["hermes"] = hermes
    alerts = diagnose(sys_m, gpus, ollama, procs, inv, query_endpoints())
    now_dt = datetime.now(timezone.utc)
    avg_util = round(sum(g["util_gpu"] for g in gpus) / len(gpus), 1) if gpus else 0
    result = {
        "timestamp_utc": now_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "iso_ts": now_dt.isoformat(),
        "hostname": os.uname().nodename,
        "system": sys_m,
        "num_gpus": len(gpus),
        "avg_util_pct": avg_util,
        "total_power_watts": round(sum(g["power_w"] for g in gpus), 2),
        "avg_temp_c": round(sum(g["temp_c"] for g in gpus) / len(gpus), 1) if gpus else 0,
        "gpus": gpus,
        "processes": procs,
        "ollama": ollama,
        "hermes": hermes,
        "endpoints": query_endpoints(),
        "models": inv,
        "vllm": inv.get("active_vllm") or [],
        "alerts": alerts,
        "alert_count": {
            "critical": sum(1 for a in alerts if a["level"] == "critical"),
            "warning": sum(1 for a in alerts if a["level"] == "warning"),
            "info": sum(1 for a in alerts if a["level"] == "info"),
        },
    }
    _LIVE_SNAP_CACHE["ts"] = now
    _LIVE_SNAP_CACHE["payload"] = result
    return _snap_copy(result)


def _snap_copy(snap: dict) -> dict:
    """Copy fields overview may mutate (gpus/system) without a full deepcopy."""
    out = dict(snap)
    out["gpus"] = [dict(g) for g in (snap.get("gpus") or [])]
    if isinstance(snap.get("system"), dict):
        out["system"] = dict(snap["system"])
    return out


def _overlay_hist_metrics(snap: dict, node2: dict) -> None:
    """Replace overview live gauges with 1 Hz hist (present-tense, already paid)."""
    with _cache_lock:
        n1 = dict(_n1_hist[-1]) if _n1_hist else {}
        n2s = dict(_n2_hist[-1]) if _n2_hist else {}
    if n1:
        snap["metrics_as_of"] = n1.get("t")
        gpus = snap.get("gpus") or []
        if gpus:
            g = gpus[0]
            if n1.get("gpu") is not None:
                g["util_gpu"] = n1["gpu"]
            if n1.get("pwr") is not None:
                g["power_w"] = n1["pwr"]
            if n1.get("temp") is not None:
                g["temp_c"] = n1["temp"]
            snap["avg_util_pct"] = g.get("util_gpu")
            snap["total_power_watts"] = g.get("power_w")
            snap["avg_temp_c"] = g.get("temp_c")
        sys_m = snap.setdefault("system", {})
        if n1.get("cpu") is not None:
            sys_m["cpu_pct"] = n1["cpu"]
        if n1.get("mem") is not None:
            sys_m["mem_pct"] = n1["mem"]
    if n2s and node2.get("reachable"):
        node2["metrics_as_of"] = n2s.get("t")
        # Don't mutate the live node2 cache — copy nested metrics first.
        if node2.get("gpus"):
            node2["gpus"] = [dict(g) for g in node2["gpus"]]
        if isinstance(node2.get("mem"), dict):
            node2["mem"] = dict(node2["mem"])
        if isinstance(node2.get("swap"), dict):
            node2["swap"] = dict(node2["swap"])
        gpus = node2.get("gpus") or []
        if gpus:
            g = gpus[0]
            if n2s.get("gpu") is not None:
                g["util_gpu"] = n2s["gpu"]
            if n2s.get("pwr") is not None:
                g["power_w"] = n2s["pwr"]
        if n2s.get("cpu") is not None:
            node2["cpu_pct"] = n2s["cpu"]
        mem = node2.get("mem")
        if isinstance(mem, dict) and n2s.get("mem") is not None:
            mem["pct"] = n2s["mem"]
        swap = node2.get("swap")
        if isinstance(swap, dict) and n2s.get("swap") is not None:
            swap["pct"] = n2s["swap"]


def _load_history_rows(hours: int) -> list[dict]:
    if not os.path.exists(CSV_FILE):
        return []
    cutoff = datetime.now(timezone.utc).timestamp() - hours * 3600
    rows = []
    with open(CSV_FILE) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                ts = datetime.fromisoformat(row["iso_ts"].replace("Z", "+00:00"))
                if ts.timestamp() >= cutoff:
                    rows.append(row)
            except (KeyError, ValueError):
                continue
    return rows


def _series_stats(values: list[float]) -> dict:
    if not values:
        return {"min": None, "max": None, "avg": None, "last": None}
    return {
        "min": round(min(values), 1),
        "max": round(max(values), 1),
        "avg": round(sum(values) / len(values), 1),
        "last": round(values[-1], 1),
    }


@app.get("/api/live")
def api_live():
    return api_live_snapshot()


@app.get("/api/sparks")
def api_sparks():
    """1s-resolution rolling history for the System Monitor-style graphs."""
    with _cache_lock:
        return {"node1": list(_n1_hist), "node2": list(_n2_hist)}


@app.get("/api/tick")
def api_tick():
    """Tiny live heartbeat — every number the console animates, nothing else.

    WHY THIS EXISTS: /api/overview is ~62 kB and ~1 s (it calls nvidia-smi and
    sweeps systemd on the request path), so a phone can only afford it every
    ~10-15 s. This route reads ONLY already-populated caches — measured well
    under 5 ms and ~400 bytes — so the gauges, bars and sparklines can run at
    1 Hz on an iPhone over the tailnet without loading the box at all.

    Structure (host set, alerts, service/job state) still comes from
    /api/overview; this is values-only.
    """
    with _cache_lock:
        n1 = _n1_hist[-1] if _n1_hist else {}
        n2s = _n2_hist[-1] if _n2_hist else {}
        n2 = dict(_node2_cache)
        pi = dict(_pi_cache)
        s9 = dict(_start9_cache)
    n2gpu = (n2.get("gpus") or [{}])[0]
    # Prefer 1 Hz hist watts when present — SSH cache snapshot can lag a cycle.
    n2_pwr = n2s.get("pwr")
    if n2_pwr is None:
        n2_pwr = n2gpu.get("power_w")
    n2_temp = n2gpu.get("temp_c")  # samples loop has no temp field

    def appliance(h: dict) -> dict:
        return {"reachable": bool(h.get("reachable")),
                "t": h.get("polled_at") or h.get("metrics_as_of"),
                "cpu": h.get("cpu_pct"), "mem": (h.get("mem") or {}).get("pct"),
                "temp": h.get("temp_c"), "pwr": h.get("power_w")}

    return {
        "t": _time.time(),
        "node1": {"reachable": True, "t": n1.get("t"),
                  "cpu": n1.get("cpu"), "gpu": n1.get("gpu"), "mem": n1.get("mem"),
                  "swap": n1.get("swap"), "pwr": n1.get("pwr"), "temp": n1.get("temp"),
                  "rx": n1.get("rx"), "tx": n1.get("tx")},
        "node2": {"reachable": bool(n2.get("reachable")), "t": n2s.get("t") or n2.get("metrics_as_of"),
                  "cpu": n2s.get("cpu", n2.get("cpu_pct")),
                  "gpu": n2s.get("gpu", n2gpu.get("util_gpu")),
                  "mem": n2s.get("mem", (n2.get("mem") or {}).get("pct")),
                  "swap": n2s.get("swap"), "pwr": n2_pwr,
                  "temp": n2_temp},
        "pi": appliance(pi),
        "start9": appliance(s9),
    }


@app.get("/api/node2")
def api_node2():
    with _cache_lock:
        return dict(_node2_cache)


@app.get("/api/projects")
def api_projects():
    with _cache_lock:
        return dict(_projects_cache)


@app.get("/api/todos")
def api_todos():
    return {"todos": projects_status.get_todos()}


@app.post("/api/todos")
def api_todos_post(req: TodoRequest):
    if req.action == "add" and req.text.strip():
        return {"todos": projects_status.add_todo(req.text, req.tag)}
    if req.action == "toggle" and req.id:
        return {"todos": projects_status.toggle_todo(req.id)}
    if req.action == "delete" and req.id:
        return {"todos": projects_status.delete_todo(req.id)}
    return JSONResponse({"ok": False, "error": "bad action"}, status_code=400)


@app.get("/api/pi")
def api_pi():
    with _cache_lock:
        return dict(_pi_cache)


@app.get("/api/start9")
def api_start9():
    with _cache_lock:
        return dict(_start9_cache)


@app.get("/api/automation")
def api_automation():
    with _cache_lock:
        return dict(_automation_cache)


@app.get("/api/token-usage")
def api_token_usage():
    # Cheap direct DB reads; 20s cache keeps Jobs-tab polls from re-scanning.
    return token_usage.token_summary()


@app.get("/api/backups")
def api_backups():
    with _cache_lock:
        return dict(_backups_cache)


@app.get("/api/comfy")
def api_comfy():
    """ComfyUI job-monitor snapshot. Plain def, reads the thread-populated cache.

    ComfyUI is on-demand, so when it is DOWN this returns a clean offline state
    instead of erroring: {"offline": true, "state": "offline", "chip": "offline"}
    with all counts zeroed. See _comfy_refresher for the backoff policy.
    """
    with _cache_lock:
        return dict(_comfy_cache)


@app.get("/api/energy-cost")
def api_energy_cost(request: Request):
    """Historical electricity: trailing 24h/30d kWh from integrated watt samples.

    Default mode=wall (Spark wall estimate — bill-like). mode=sensor is raw
    GPU/PMIC/RAPL only (understates AC wall). Returns kWh; UI multiplies by
    editable $/kWh. Also returns pace_30d scaled from trailing 24h average.

    Query params:
      mode         wall|sensor (default wall)
      idle_wall_w  default 50 — only used when mode=wall
      gpu_floor_w  default 11
      slope        default 1.15
    """
    q = request.query_params
    def _f(name, default):
        raw = q.get(name)
        if raw is None or raw == "":
            return default
        try:
            return float(raw)
        except ValueError:
            return default
    return energy_cost.energy_summary(
        idle_wall_w=_f("idle_wall_w", energy_cost.DEFAULT_IDLE_WALL_W),
        gpu_floor_w=_f("gpu_floor_w", energy_cost.DEFAULT_GPU_FLOOR_W),
        slope=_f("slope", energy_cost.DEFAULT_SLOPE),
        mode=(q.get("mode") or "wall"),
    )


@app.post("/api/comfy/cancel")
def api_comfy_cancel(req: ComfyCancelRequest):
    """Cancel / remove a ComfyUI job by prompt_id (running → interrupt, pending → dequeue).

    Proxies to loopback ComfyUI only — never accepts a host. After success the
    refresher cache is invalidated so the next GET /api/comfy reflects the queue.
    """
    result = comfy_api.cancel_job(req.prompt_id)
    if not result.get("ok"):
        return JSONResponse(result, status_code=400)
    # Push a fresh snapshot into the cache so the UI updates without waiting
    # for the next refresher tick (up to 5s while live / 10s while offline).
    try:
        snap = comfy_api.query_comfy()
        with _cache_lock:
            _comfy_cache.clear()
            _comfy_cache.update(snap)
    except Exception:
        pass
    return result


@app.get("/api/links")
def api_links():
    with _cache_lock:
        return dict(_links_cache)


@app.get("/api/actions")
def api_actions():
    return {"actions": quick_actions.list_actions()}


@app.post("/api/actions/{action_id}")
def api_action_run(action_id: str):
    result = quick_actions.run_action(action_id)
    if not result.get("ok"):
        return JSONResponse(result, status_code=400)
    return result


@app.get("/api/actions/runs/{run_id}")
def api_action_run_status(run_id: str):
    run = quick_actions.get_run(run_id)
    if not run:
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)
    return {"ok": True, "run": run}


@app.get("/api/launch")
def api_launch_list():
    return {"apps": desktop_launch.list_apps()}


@app.post("/api/launch/{app_id}")
def api_launch(app_id: str):
    result = desktop_launch.launch_app(app_id)
    if not result.get("ok"):
        return JSONResponse(result, status_code=400)
    return result


@app.get("/api/overview")
def api_overview():
    """Everything the console page needs in one call.

    Live gauges (GPU/CPU/mem/power) are overlaid from the 1 Hz hist so Fleet
    matches Pulse — overview still carries structure (services, alerts, models).
    """
    snap = api_live_snapshot()
    with _cache_lock:
        node2 = dict(_node2_cache)
        projects = dict(_projects_cache)
        pi = dict(_pi_cache)
        start9 = dict(_start9_cache)
        automation = dict(_automation_cache)
        backups = dict(_backups_cache)
        links = dict(_links_cache)
    _overlay_hist_metrics(snap, node2)
    try:
        services = _services_snapshot()
    except Exception as e:
        services = {"services": [], "error": str(e)[:200], "active_operation": None}
    project_list = projects.get("projects", [])
    return {
        "node1": snap,
        "node2": node2,
        "pi": pi,
        "start9": start9,
        "projects": project_list,
        "projects_ts": projects.get("iso_ts"),
        "todos": projects_status.get_todos(),
        "services": services,
        "automation": automation,
        "backups": backups,
        "links": links,
        "actions": quick_actions.list_actions(),
        "launch": desktop_launch.list_apps(),
        "alerts": _fleet_alerts(snap, node2, pi, start9, automation, backups, project_list),
    }


@app.get("/api/services")
def api_services():
    return _services_snapshot()


@app.post("/api/services/register")
def api_services_register(req: ServiceRegisterRequest):
    """Add/update a non-builtin service on the console (no restart needed)."""
    payload = req.model_dump()
    replace = payload.pop("replace", True)
    result = register_service(payload, replace=replace)
    if not result.get("ok"):
        return JSONResponse(result, status_code=400)
    _services_refresh.set()  # new row must appear without waiting out the interval
    return result


@app.delete("/api/services/register/{service_id}")
def api_services_unregister(service_id: str):
    result = unregister_service(service_id)
    if not result.get("ok"):
        return JSONResponse(result, status_code=400)
    _services_refresh.set()
    return result


@app.post("/api/services/{service_id}")
def api_services_action(service_id: str, req: ServiceActionRequest):
    if service_id == "register":
        return JSONResponse(
            {"ok": False, "error": "Use POST /api/services/register with a body"},
            status_code=400,
        )
    result = service_action(service_id, req.action, model=req.model)
    _services_refresh.set()  # reflect the click even if the action itself failed
    if not result.get("ok"):
        return JSONResponse(result, status_code=400)
    return result


@app.get("/api/services/operations/{op_id}")
def api_services_operation(op_id: str):
    op = get_svc_operation(op_id)
    if not op:
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404)
    return {"ok": True, "operation": op}


@app.get("/api/history")
def api_history(hours: int = 24):
    rows = _load_history_rows(hours)
    return {"rows": rows, "hours": hours, "count": len(rows)}


@app.get("/api/summary")
def api_summary(hours: int = 24):
    rows = _load_history_rows(hours)
    if not rows:
        return {"hours": hours, "count": 0, "metrics": {}}

    def col(name: str) -> list[float]:
        out = []
        for row in rows:
            try:
                out.append(float(row[name]))
            except (KeyError, ValueError, TypeError):
                continue
        return out

    metrics = {
        "gpu_util": _series_stats(col("gpu_util")),
        "cpu_pct": _series_stats(col("cpu_pct")),
        "mem_pct": _series_stats(col("mem_pct")),
        "mem_avail_gb": _series_stats(col("mem_avail_gb")),
        "gpu_power_w": _series_stats(col("gpu_power_w")),
        "gpu_temp_c": _series_stats(col("gpu_temp_c")),
        "swap_used_gb": _series_stats(col("swap_used_gb")),
    }
    return {
        "hours": hours,
        "count": len(rows),
        "first_ts": rows[0].get("iso_ts"),
        "last_ts": rows[-1].get("iso_ts"),
        "metrics": metrics,
    }


@app.get("/api/models/control")
def api_models_control():
    return control_status()


@app.get("/api/models/can-switch/{key}")
def api_models_can_switch(key: str):
    return can_switch(key)


@app.get("/api/models/operations")
def api_models_operations(limit: int = 10):
    return {"operations": list_operations(limit=limit)}


@app.get("/api/models/operations/{op_id}")
def api_models_operation(op_id: str):
    op = get_operation(op_id)
    if not op:
        return JSONResponse({"ok": False, "error": "Operation not found"}, status_code=404)
    return {"ok": True, "operation": op}


@app.post("/api/models/switch")
def api_models_switch(req: SwitchRequest):
    result = switch_model(req.key, fast=req.fast)
    if not result.get("ok"):
        return JSONResponse(result, status_code=400)
    return result


@app.post("/api/models/stop")
def api_models_stop():
    result = stop_models()
    if not result.get("ok"):
        return JSONResponse(result, status_code=400)
    return result


@app.post("/api/models/kill-orphans")
def api_models_kill_orphans():
    return kill_orphans()


@app.post("/api/models/unload-ollama")
def api_models_unload_ollama():
    return unload_ollama()


@app.post("/api/models/sync-hermes")
def api_models_sync_hermes():
    result = sync_hermes()
    if not result.get("ok"):
        return JSONResponse(result, status_code=400)
    return result


@app.post("/api/refresh")
def api_refresh():
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "collector.py")
    try:
        result = subprocess.run(
            [sys.executable, script],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            return JSONResponse(
                {"ok": False, "error": result.stderr.strip() or "collector failed"},
                status_code=500,
            )
        snap = json.load(open(JSON_SNAP)) if os.path.exists(JSON_SNAP) else None
        return {"ok": True, "snapshot": snap}
    except subprocess.TimeoutExpired:
        return JSONResponse({"ok": False, "error": "collector timed out"}, status_code=504)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8085))
    host = os.environ.get("HOST", "127.0.0.1")
    print(f"DGX Spark Performance Dashboard → http://{host}:{port}")
    uvicorn.run(app, host=host, port=port)