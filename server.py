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
except ImportError:
    print("ERROR: pip install fastapi uvicorn", file=sys.stderr)
    sys.exit(1)

# Import collector functions for live queries
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from collector import (  # noqa: E402
    diagnose, query_gpu, query_hermes, query_ollama, query_procs, system_metrics,
)

app = FastAPI(title="DGX Spark Performance Dashboard")


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


@app.get("/api/diagnostics")
async def api_diagnostics():
    sys_m = system_metrics()
    gpus = query_gpu()
    procs = query_procs()
    ollama = query_ollama()
    alerts = diagnose(sys_m, gpus, ollama, procs)
    return {
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "alerts": alerts,
        "alert_count": {
            "critical": sum(1 for a in alerts if a["level"] == "critical"),
            "warning": sum(1 for a in alerts if a["level"] == "warning"),
            "info": sum(1 for a in alerts if a["level"] == "info"),
        },
        "ollama": ollama,
        "hermes": query_hermes(),
    }


async def api_live_snapshot():
    sys_m = system_metrics()
    gpus = query_gpu()
    procs = query_procs()
    ollama = query_ollama()
    hermes = query_hermes()
    alerts = diagnose(sys_m, gpus, ollama, procs)
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
        "alerts": alerts,
        "alert_count": {
            "critical": sum(1 for a in alerts if a["level"] == "critical"),
            "warning": sum(1 for a in alerts if a["level"] == "warning"),
            "info": sum(1 for a in alerts if a["level"] == "info"),
        },
    }


@app.get("/api/history")
async def api_history(hours: int = 24):
    if not os.path.exists(CSV_FILE):
        return JSONResponse({"rows": [], "hours": hours})
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
    return {"rows": rows, "hours": hours, "count": len(rows)}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8085))
    print(f"DGX Spark Performance Dashboard → http://0.0.0.0:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port)