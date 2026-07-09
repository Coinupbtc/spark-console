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
import subprocess
import sys
from datetime import datetime, timezone
from io import StringIO

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
CSV_FILE = os.path.join(DATA_DIR, "performance_timeseries.csv")
JSON_SNAP = os.path.join(DATA_DIR, "latest_snapshot.json")
DASHBOARD_HTML = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")

try:
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse, PlainTextResponse, JSONResponse
    from pydantic import BaseModel
except ImportError:
    print("ERROR: pip install fastapi uvicorn", file=sys.stderr)
    sys.exit(1)

# Import collector functions for live queries
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from collector import (  # noqa: E402
    diagnose, query_gpu, query_hermes, query_ollama, query_procs, system_metrics,
)
from model_inventory import query_inventory  # noqa: E402
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

app = FastAPI(title="DGX Spark Performance Dashboard")


class SwitchRequest(BaseModel):
    key: str
    fast: bool = False


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    if os.path.exists(DASHBOARD_HTML):
        return HTMLResponse(content=open(DASHBOARD_HTML).read())
    return HTMLResponse(content="<h1>dashboard.html missing</h1>", status_code=500)


@app.get("/api/latest")
async def api_latest():
    if os.path.exists(JSON_SNAP):
        return json.load(open(JSON_SNAP))
    return await api_live_snapshot()


@app.get("/api/csv")
async def api_csv():
    if not os.path.exists(CSV_FILE):
        legacy = os.path.join(DATA_DIR, "gpu_timeseries.csv")
        if os.path.exists(legacy):
            return PlainTextResponse(content=open(legacy).read(), media_type="text/csv")
        return PlainTextResponse("No data yet — run collector.py first", status_code=404)
    return PlainTextResponse(content=open(CSV_FILE).read(), media_type="text/csv")


@app.get("/api/gpu-stats")
async def api_gpu_stats():
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
async def api_system():
    now = datetime.now(timezone.utc)
    return {
        "timestamp_utc": now.strftime("%Y-%m-%d %H:%M:%S"),
        "iso_ts": now.isoformat(),
        "system": system_metrics(),
        "ollama": query_ollama(),
        "hermes": query_hermes(),
    }


@app.get("/api/models")
async def api_models():
    inv = query_inventory()
    inv["hermes"] = query_hermes()
    return inv


@app.get("/api/diagnostics")
async def api_diagnostics():
    sys_m = system_metrics()
    gpus = query_gpu()
    procs = query_procs()
    ollama = query_ollama()
    inv = query_inventory()
    inv["hermes"] = query_hermes()
    alerts = diagnose(sys_m, gpus, ollama, procs, inv)
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


async def api_live_snapshot():
    sys_m = system_metrics()
    gpus = query_gpu()
    procs = query_procs()
    ollama = query_ollama()
    hermes = query_hermes()
    inv = query_inventory()
    inv["hermes"] = hermes
    alerts = diagnose(sys_m, gpus, ollama, procs, inv)
    now = datetime.now(timezone.utc)
    avg_util = round(sum(g["util_gpu"] for g in gpus) / len(gpus), 1) if gpus else 0
    return {
        "timestamp_utc": now.strftime("%Y-%m-%d %H:%M:%S"),
        "iso_ts": now.isoformat(),
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
        "models": inv,
        "vllm": inv.get("active_vllm") or [],
        "alerts": alerts,
        "alert_count": {
            "critical": sum(1 for a in alerts if a["level"] == "critical"),
            "warning": sum(1 for a in alerts if a["level"] == "warning"),
            "info": sum(1 for a in alerts if a["level"] == "info"),
        },
    }


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
async def api_live():
    return await api_live_snapshot()


@app.get("/api/history")
async def api_history(hours: int = 24):
    rows = _load_history_rows(hours)
    return {"rows": rows, "hours": hours, "count": len(rows)}


@app.get("/api/summary")
async def api_summary(hours: int = 24):
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
async def api_models_control():
    return control_status()


@app.get("/api/models/can-switch/{key}")
async def api_models_can_switch(key: str):
    return can_switch(key)


@app.get("/api/models/operations")
async def api_models_operations(limit: int = 10):
    return {"operations": list_operations(limit=limit)}


@app.get("/api/models/operations/{op_id}")
async def api_models_operation(op_id: str):
    op = get_operation(op_id)
    if not op:
        return JSONResponse({"ok": False, "error": "Operation not found"}, status_code=404)
    return {"ok": True, "operation": op}


@app.post("/api/models/switch")
async def api_models_switch(req: SwitchRequest):
    result = switch_model(req.key, fast=req.fast)
    if not result.get("ok"):
        return JSONResponse(result, status_code=400)
    return result


@app.post("/api/models/stop")
async def api_models_stop():
    result = stop_models()
    if not result.get("ok"):
        return JSONResponse(result, status_code=400)
    return result


@app.post("/api/models/kill-orphans")
async def api_models_kill_orphans():
    return kill_orphans()


@app.post("/api/models/unload-ollama")
async def api_models_unload_ollama():
    return unload_ollama()


@app.post("/api/models/sync-hermes")
async def api_models_sync_hermes():
    result = sync_hermes()
    if not result.get("ok"):
        return JSONResponse(result, status_code=400)
    return result


@app.post("/api/refresh")
async def api_refresh():
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