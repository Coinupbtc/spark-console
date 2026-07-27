#!/usr/bin/env python3
"""
DGX Spark Performance Collector
Samples GPU, CPU, memory, disk, Ollama, and Agent gateway state.
Appends one CSV row per run and overwrites latest_snapshot.json.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone

from cluster_metrics import (
    CLUSTER_CSV_HEADERS,
    cluster_csv_values,
    collect_cluster,
    notify_node2_state,
)
from model_inventory import query_inventory, sync_manifest_from_disk
from timeseries_schema import SCHEMA_VERSION, append_row

try:
    import psutil
except ImportError:
    print("ERROR: pip install psutil", file=sys.stderr)
    sys.exit(1)

# Tests can isolate writes while exercising the real collector and alert path.
DATA_DIR = os.getenv(
    "DGX_DATA_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"),
)
CSV_FILE = os.path.join(DATA_DIR, "performance_timeseries.csv")
JSON_SNAP = os.path.join(DATA_DIR, "latest_snapshot.json")

BASE_CSV_HEADERS = [
    "timestamp_utc", "iso_ts", "hour", "dow",
    "cpu_pct", "load_1", "load_5", "load_15",
    "mem_used_gb", "mem_total_gb", "mem_avail_gb", "mem_pct",
    "swap_used_gb", "swap_total_gb", "swap_pct",
    "disk_used_pct", "disk_free_tb",
    "gpu_util", "gpu_power_w", "gpu_temp_c", "gpu_mem_used_mb", "gpu_mem_total_mb",
    "ollama_models_json", "gpu_procs_json", "alerts_json",
]
CSV_HEADERS = BASE_CSV_HEADERS + CLUSTER_CSV_HEADERS


def _safe_int(v: str | None, default: int = 0) -> int:
    if not v or v.strip() in ("", "N/A", "[N/A]"):
        return default
    v = v.strip().strip("[]").split()[0]
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return default


def _safe_float(v: str | None, default: float = 0.0) -> float:
    if not v or v.strip() in ("", "N/A", "[N/A]"):
        return default
    v = v.strip().strip("[]").split()[0]
    try:
        return round(float(v), 2)
    except (ValueError, TypeError):
        return default


def query_gpu() -> list[dict]:
    out = subprocess.run(
        ["nvidia-smi",
         "--query-gpu=index,name,power.draw,temperature.gpu,"
         "utilization.gpu,memory.used,memory.total,fan.speed,pstate",
         "--format=csv,noheader,nounits"],
        capture_output=True, text=True, timeout=10,
    )
    gpus = []
    for line in (out.stdout or "").strip().splitlines():
        p = [x.strip() for x in line.split(",")]
        if len(p) < 7:
            continue
        gpus.append({
            "index": _safe_int(p[0]),
            "name": p[1] or "unknown",
            "power_w": _safe_float(p[2]),
            "temp_c": _safe_int(p[3]),
            "util_gpu": _safe_float(p[4]),
            "mem_used_mb": _safe_int(p[5]),
            "mem_total_mb": _safe_int(p[6]),
            "fan_pct": _safe_int(p[7]) if len(p) > 7 else 0,
            "pstate": p[8] if len(p) > 8 else "unknown",
        })
    return gpus


def query_procs() -> list[dict]:
    out = subprocess.run(
        ["nvidia-smi", "--query-compute-apps=name,used_memory",
         "--format=csv,noheader,nounits"],
        capture_output=True, text=True, timeout=10,
    )
    procs = []
    for line in (out.stdout or "").strip().splitlines():
        p = [x.strip() for x in line.split(",")]
        if len(p) >= 2 and p[0] and p[0] not in ("", "N/A", "[No data]"):
            procs.append({"name": p[0], "mem_mb": _safe_int(p[1])})
    return procs


OLLAMA_BIN = os.path.expanduser("~/.local/bin/ollama")
if not os.path.exists(OLLAMA_BIN):
    OLLAMA_BIN = "ollama"


def query_ollama() -> list[dict]:
    # bare "ollama" fails under systemd (PATH lacks ~/.local/bin → errno 13)
    try:
        out = subprocess.run(
            [OLLAMA_BIN, "ps"], capture_output=True, text=True, timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    models = []
    for line in (out.stdout or "").strip().splitlines()[1:]:
        parts = line.split()
        if len(parts) < 4:
            continue
        name = parts[0]
        size = parts[2] if len(parts) > 2 else "?"
        processor = parts[3] if len(parts) > 3 else "?"
        until = " ".join(parts[5:]) if len(parts) > 5 else ""
        models.append({
            "name": name,
            "size": size,
            "processor": processor,
            "until": until,
            "stuck": "stopping" in until.lower(),
        })
    return models


def query_agent() -> dict:
    out = subprocess.run(
        ["systemctl", "--user", "is-active",
         "agent-gateway-orchestrator.service",
         "agent-gateway-light.service",
         "agent-gateway-dobby.service",
         "agent-gateway-smeagle.service",
         "dgx-performance-dashboard.service",
         "llama-miaai35.service",
         "ollama.service"],
        capture_output=True, text=True, timeout=5,
    )
    lines = (out.stdout or "").strip().splitlines()
    return {
        "orchestrator": lines[0] if len(lines) > 0 else "unknown",
        "light": lines[1] if len(lines) > 1 else "unknown",
        "dobby": lines[2] if len(lines) > 2 else "unknown",
        "smeagle": lines[3] if len(lines) > 3 else "unknown",
        "dashboard": lines[4] if len(lines) > 4 else "unknown",
        "llama_miaai35": lines[5] if len(lines) > 5 else "unknown",
        "ollama": lines[6] if len(lines) > 6 else "unknown",
    }


def query_endpoints() -> list[dict]:
    """Local inference endpoints on this node (llama.cpp :8889 default)."""
    import urllib.request
    eps = []
    for port, engine in ((8889, "llama.cpp"), (8888, "vLLM")):
        try:
            with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/v1/models", timeout=2) as r:
                data = json.load(r)
            for m in data.get("data") or []:
                mid = (m.get("id") or "?").rstrip("/").split("/")[-1]
                eps.append({"id": mid, "port": port,
                            "engine": engine, "status": "ok"})
        except Exception:
            continue
    return eps


def system_metrics() -> dict:
    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()
    disk = psutil.disk_usage("/")
    load = os.getloadavg()
    return {
        "cpu_pct": psutil.cpu_percent(interval=0.5),
        "load_1": round(load[0], 2),
        "load_5": round(load[1], 2),
        "load_15": round(load[2], 2),
        "mem_used_gb": round(mem.used / (1024 ** 3), 2),
        "mem_total_gb": round(mem.total / (1024 ** 3), 2),
        "mem_avail_gb": round(mem.available / (1024 ** 3), 2),
        "mem_pct": round(mem.percent, 1),
        "swap_used_gb": round(swap.used / (1024 ** 3), 2),
        "swap_total_gb": round(swap.total / (1024 ** 3), 2),
        "swap_pct": round(swap.percent, 1),
        "disk_used_pct": round(disk.percent, 1),
        "disk_free_tb": round(disk.free / (1024 ** 4), 2),
    }


def diagnose(
    sys_m: dict,
    gpus: list[dict],
    ollama: list[dict],
    procs: list[dict],
    inventory: dict | None = None,
) -> list[dict]:
    alerts: list[dict] = []

    if sys_m["mem_pct"] >= 85:
        alerts.append({
            "level": "critical",
            "category": "memory",
            "message": f"RAM at {sys_m['mem_pct']}% — only {sys_m['mem_avail_gb']} GB free",
            "action": "Stop vLLM or Ollama: switch-model qwen OR ollama stop <model>",
        })
    elif sys_m["mem_pct"] >= 70:
        alerts.append({
            "level": "warning",
            "category": "memory",
            "message": f"RAM at {sys_m['mem_pct']}% — {sys_m['mem_avail_gb']} GB available",
            "action": "Review vLLM (nvfp4-status.sh) and `ollama ps`",
        })

    if sys_m["swap_used_gb"] >= 2:
        alerts.append({
            "level": "warning",
            "category": "swap",
            "message": f"Swap usage {sys_m['swap_used_gb']} GB — memory pressure",
            "action": "Free RAM by stopping large models",
        })

    for m in ollama:
        size_gb = 0.0
        m_size = re.search(r"([\d.]+)\s*GB", m.get("size", ""))
        if m_size:
            size_gb = float(m_size.group(1))
        if m.get("stuck"):
            alerts.append({
                "level": "critical",
                "category": "ollama",
                "message": f"Model {m['name']} stuck in Stopping... ({m['size']})",
                "action": f"Run: ollama stop {m['name']}",
            })
        elif size_gb >= 60:
            alerts.append({
                "level": "warning",
                "category": "ollama",
                "message": f"Large model loaded: {m['name']} ({m['size']})",
                "action": "Unload after use to free ~87 GB for other tasks",
            })

    for g in gpus:
        if g.get("util_gpu", 0) >= 80 and g.get("temp_c", 0) >= 75:
            alerts.append({
                "level": "warning",
                "category": "gpu",
                "message": f"GPU {g['index']} hot: {g['util_gpu']}% util, {g['temp_c']}°C",
                "action": "Check if workload is expected; consider smaller model",
            })

    for p in procs:
        if "gnome-remote-desktop" in p.get("name", ""):
            alerts.append({
                "level": "info",
                "category": "gpu",
                "message": f"gnome-remote-desktop using {p.get('mem_mb', 0)} MB VRAM",
                "action": "Disable remote desktop if not needed to reclaim VRAM",
            })

    if sys_m["disk_used_pct"] >= 80:
        alerts.append({
            "level": "warning",
            "category": "disk",
            "message": f"Disk {sys_m['disk_used_pct']}% full — {sys_m['disk_free_tb']} TB free",
            "action": "Review ~/models/dgx_bundle and Ollama cache for unused models",
        })

    if inventory:
        agent_dash = (inventory.get("agent") or {}).get("dashboard")
        if agent_dash == "inactive":
            alerts.append({
                "level": "warning",
                "category": "dashboard",
                "message": "Performance dashboard service is not running",
                "action": "systemctl --user restart dgx-performance-dashboard.service",
            })
        active = inventory.get("active_vllm") or []
        if len(active) > 1:
            alerts.append({
                "level": "critical",
                "category": "vllm",
                "message": f"Multiple vLLM servers active ({len(active)}) — OOM risk on 121GB Spark",
                "action": "switch-model stop  # then start one model only",
            })
        for m in inventory.get("models") or []:
            if m.get("status") == "loading":
                alerts.append({
                    "level": "info",
                    "category": "vllm",
                    "message": f"vLLM loading {m.get('label')} on :{m.get('port')}",
                    "action": f"tail -f ~/models/dgx_bundle/vllm-{m.get('key', '').replace('-nvfp4', '').replace('-35b', '')}.log",
                })

    return alerts


def collect() -> dict | None:
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%d %H:%M:%S")
    iso = now.isoformat()

    sync_manifest_from_disk()
    sys_m = system_metrics()
    gpus = query_gpu()
    procs = query_procs()
    ollama = query_ollama()
    agent = query_agent()
    inv = query_inventory()
    inv["agent"] = agent
    alerts = diagnose(sys_m, gpus, ollama, procs, inv)
    cluster = collect_cluster(sys_m, gpus)
    node2_alert_state = notify_node2_state(cluster["node2"])

    gpu = gpus[0] if gpus else {}
    avg_util = round(sum(g["util_gpu"] for g in gpus) / len(gpus), 1) if gpus else 0

    snap = {
        "schema_version": SCHEMA_VERSION,
        "timestamp_utc": ts,
        "iso_ts": iso,
        "hour": now.hour,
        "dow": now.weekday(),
        "hostname": os.uname().nodename,
        "system": sys_m,
        "num_gpus": len(gpus),
        "avg_util_pct": avg_util,
        "total_power_watts": round(sum(g["power_w"] for g in gpus), 2),
        "avg_temp_c": round(sum(g["temp_c"] for g in gpus) / len(gpus), 1) if gpus else 0,
        "gpus": gpus,
        "processes": procs,
        "ollama": ollama,
        "agent": agent,
        "models": inv,
        "vllm": inv.get("active_vllm") or [],
        "nodes": cluster,
        "endpoint_health": {
            "node1": cluster["node1"]["endpoints"],
            "node2": cluster["node2"]["endpoints"],
        },
        "workloads": {
            "node1": cluster["node1"]["workloads"],
            "node2": cluster["node2"]["workloads"],
        },
        "node2_alert_state": node2_alert_state,
        "alerts": alerts,
        "alert_count": {"critical": sum(1 for a in alerts if a["level"] == "critical"),
                        "warning": sum(1 for a in alerts if a["level"] == "warning"),
                        "info": sum(1 for a in alerts if a["level"] == "info")},
    }

    os.makedirs(DATA_DIR, exist_ok=True)

    row = {
        "timestamp_utc": ts,
        "iso_ts": iso,
        "hour": now.hour,
        "dow": now.weekday(),
        **sys_m,
        "gpu_util": gpu.get("util_gpu", 0),
        "gpu_power_w": gpu.get("power_w", 0),
        "gpu_temp_c": gpu.get("temp_c", 0),
        "gpu_mem_used_mb": gpu.get("mem_used_mb", 0),
        "gpu_mem_total_mb": gpu.get("mem_total_mb", 0),
        "ollama_models_json": json.dumps(ollama),
        "gpu_procs_json": json.dumps(procs),
        "alerts_json": json.dumps(alerts),
        **cluster_csv_values(cluster),
    }
    append_row(CSV_FILE, CSV_HEADERS, row)

    with open(JSON_SNAP, "w") as f:
        json.dump(snap, f, indent=2)

    print(f"[{ts}] spark-node performance snapshot")
    print(f"  CPU {sys_m['cpu_pct']}% | RAM {sys_m['mem_pct']}% ({sys_m['mem_avail_gb']}G free) | "
          f"GPU {avg_util}% {snap['total_power_watts']}W")
    node2 = cluster["node2"]
    if node2["reachable"]:
        node2_gpu = (node2["gpus"] or [{}])[0]
        print(
            f"  Node2 CPU {node2.get('cpu_pct', 0)}% | "
            f"RAM {node2.get('mem', {}).get('avail_gb', 0)}G free | "
            f"GPU {node2_gpu.get('util_gpu', 0)}% {node2_gpu.get('power_w', 0)}W"
        )
    elif "not configured" in str(node2.get("error") or ""):
        # Single-node is the default shape, not a degradation worth warning about.
        print("  Node2: not configured (single-node mode)")
    else:
        print(f"  [WARNING] Node2 degraded: {node2.get('error', 'unreachable')}")
    if inv.get("active_vllm"):
        for m in inv["active_vllm"]:
            print(f"  vLLM: {m['label']} :{m['port']} ({m['status']})")
    if ollama:
        for m in ollama:
            flag = " STUCK" if m.get("stuck") else ""
            print(f"  Ollama: {m['name']} {m['size']}{flag}")
    if alerts:
        for a in alerts:
            print(f"  [{a['level'].upper()}] {a['message']}")
    return snap


if __name__ == "__main__":
    collect()