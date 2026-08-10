"""Cluster-wide metrics and state-gated node2 reliability alerts."""

from __future__ import annotations

import json
import os
import subprocess
import urllib.request
from pathlib import Path

import psutil

from remote_node import query_node2


ALERTBOT = Path.home() / ".hermes/scripts/alertbot-send.sh"
NODE2_ALERT_STATE = Path(
    os.getenv(
        "DGX_NODE2_ALERT_STATE",
        str(Path(__file__).resolve().parent / "data/node2-degraded-alerted"),
    )
)
CLUSTER_CSV_HEADERS = [
    "schema_version",
    "node1_endpoint_8889_ok",
    "node1_active_model",
    "node2_reachable",
    "node2_cpu_pct",
    "node2_mem_avail_gb",
    "node2_swap_used_gb",
    "node2_swap_pct",
    "node2_gpu_util",
    "node2_gpu_power_w",
    "node2_gpu_temp_c",
    "node2_active_model",
    "node2_endpoint_8100_ok",
    "workloads_json",
]

_WORKLOAD_PATTERNS = {
    "pokemon": ("daily_scan", "pokemon-arb"),
    "crypto": ("run_cycle", "crypto-machine"),
    "comfyui": ("comfyui",),
    "download": ("huggingface-cli download", "hf download", "aria2c", "wget "),
    "bakeoff": ("bakeoff.py",),
    "agent-cron": ("cron run",),
    "llama": ("llama-server",),
    "vllm": ("vllm",),
    "ollama": ("ollama",),
}


def query_local_endpoints() -> list[dict]:
    """Record endpoint health even when no model payload is returned."""
    endpoints = []
    for port, url, engine in (
        (8889, "http://127.0.0.1:8889/v1/models", "llama.cpp"),
        (8888, "http://127.0.0.1:8888/v1/models", "vLLM"),
        (11434, "http://127.0.0.1:11434/api/tags", "Ollama"),
    ):
        item = {"port": port, "engine": engine, "status": "down", "models": []}
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                payload = json.load(response)
            models = payload.get("data") or payload.get("models") or []
            item["models"] = [
                (model.get("id") or model.get("name") or "?").rstrip("/").split("/")[-1]
                for model in models
            ]
            item["status"] = "ok"
        except Exception as error:
            item["error"] = f"{type(error).__name__}: {error}"[:200]
        endpoints.append(item)
    return endpoints


def query_local_workloads() -> list[str]:
    """Classify relevant processes without persisting full command lines."""
    labels: set[str] = set()
    for process in psutil.process_iter(("name", "cmdline")):
        try:
            text = " ".join([process.info.get("name") or "", *(process.info.get("cmdline") or [])]).lower()
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue
        for label, patterns in _WORKLOAD_PATTERNS.items():
            if any(pattern in text for pattern in patterns):
                labels.add(label)
    try:
        jobs_path = Path.home() / ".hermes/profiles/orchestrator/cron/jobs.json"
        jobs = json.loads(jobs_path.read_text()).get("jobs") or []
        if any(job.get("fire_claim") or job.get("state") == "running" for job in jobs):
            labels.add("agent-cron")
    except (OSError, TypeError, ValueError):
        # Process labels remain useful when the scheduler ledger is unavailable.
        pass
    return sorted(labels)


def normalize_node2(raw: dict) -> dict:
    """Keep the durable node2 payload compact and schema-stable."""
    node = {
        "name": raw.get("name", "sparkymaxxx-12ef"),
        "role": "node2",
        "iso_ts": raw.get("iso_ts"),
        "reachable": bool(raw.get("reachable")),
        "error": raw.get("error") or raw.get("parse_error"),
        "cpu_pct": raw.get("cpu_pct"),
        "mem": raw.get("mem") or {},
        "swap": raw.get("swap") or {},
        "gpus": raw.get("gpus") or [],
        "models": raw.get("models") or [],
        "endpoints": raw.get("endpoints") or [],
        "workloads": raw.get("workloads") or [],
        "top_procs": raw.get("top_procs") or [],
    }
    return node


def collect_cluster(local_system: dict, local_gpus: list[dict]) -> dict:
    """Collect both nodes and preserve node1 fields for existing consumers."""
    endpoints = query_local_endpoints()
    workloads = query_local_workloads()
    if any(gpu.get("util_gpu", 0) >= 5 for gpu in local_gpus):
        workloads.append("inference-busy")
    node2 = normalize_node2(query_node2())
    node1 = {
        "name": os.uname().nodename,
        "role": "node1",
        "reachable": True,
        "system": local_system,
        "gpus": local_gpus,
        "endpoints": endpoints,
        "workloads": workloads,
    }
    return {"node1": node1, "node2": node2}


def notify_node2_state(node2: dict, state_path: Path = NODE2_ALERT_STATE) -> str:
    """Page once per degraded episode and re-arm after a healthy sample."""
    if node2.get("reachable"):
        if state_path.exists():
            state_path.unlink()
        return "healthy"

    if state_path.exists():
        return "already-alerted"

    detail = str(node2.get("error") or "node2 unreachable")[:300]
    message = f"DGX baseline degraded: node2 collection failed. {detail}"
    result = subprocess.run(
        [str(ALERTBOT), "--plain", message],
        capture_output=True,
        text=True,
        timeout=20,
    )
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout or "alertbot failed").strip())
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(message + "\n")
    return "alerted"


def cluster_csv_values(cluster: dict) -> dict:
    """Flatten additive cluster fields into the existing time-series row."""
    node1 = cluster["node1"]
    node2 = cluster["node2"]
    node1_8889 = next((endpoint for endpoint in node1["endpoints"] if endpoint["port"] == 8889), {})
    node2_8100 = next((endpoint for endpoint in node2["endpoints"] if endpoint["port"] == 8100), {})
    node2_gpu = (node2.get("gpus") or [{}])[0]
    deep_model = next((model for model in node2["models"] if model.get("port") == 8100), {})
    workloads = {
        "node1": node1.get("workloads") or [],
        "node2": node2.get("workloads") or [],
    }
    return {
        "schema_version": 2,
        "node1_endpoint_8889_ok": node1_8889.get("status") == "ok",
        "node1_active_model": ",".join(node1_8889.get("models") or []),
        "node2_reachable": node2.get("reachable", False),
        "node2_cpu_pct": node2.get("cpu_pct", ""),
        "node2_mem_avail_gb": node2.get("mem", {}).get("avail_gb", ""),
        "node2_swap_used_gb": node2.get("swap", {}).get("used_gb", ""),
        "node2_swap_pct": node2.get("swap", {}).get("pct", ""),
        "node2_gpu_util": node2_gpu.get("util_gpu", ""),
        "node2_gpu_power_w": node2_gpu.get("power_w", ""),
        # Persist node2 GPU temp so cooling A/B (case/fan) can use both nodes.
        "node2_gpu_temp_c": node2_gpu.get("temp_c", ""),
        "node2_active_model": deep_model.get("id", ""),
        "node2_endpoint_8100_ok": node2_8100.get("status") == "ok",
        "workloads_json": json.dumps(workloads, separators=(",", ":")),
    }
