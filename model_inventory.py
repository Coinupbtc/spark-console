"""NVFP4 / local model inventory for DGX Spark dashboard.

Canonical source: ~/models/dgx_bundle/manifest.txt (maintained with dgx_bundle).
"""
from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path

BUNDLE_DIR = Path.home() / "models" / "dgx_bundle"
MANIFEST = BUNDLE_DIR / "manifest.txt"

# key -> (port, label, tier)
VLLM_PORTS: dict[str, tuple[int, str, str]] = {
    "qwen-nvfp4": (8001, "Qwen 3.6 35B NVFP4", "daily"),
    "nemotron-nvfp4": (8002, "Nemotron 3 Super NVFP4", "frontier"),
    "gemma-nvfp4": (8003, "Gemma 4 26B NVFP4", "light"),
    "ornith-35b": (8006, "Ornith 1.0 35B", "agent"),
    "mistral-nvfp4": (8005, "Mistral Medium 3.5 NVFP4", "frontier"),
}

REMOVED_MODELS = {"qwen35-nvfp4"}


def _dir_size_gb(path: Path) -> str | None:
    if not path.is_dir():
        return None
    try:
        out = subprocess.run(
            ["du", "-sh", str(path)],
            capture_output=True, text=True, timeout=30,
        )
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip().split()[0]
    except (subprocess.TimeoutExpired, OSError):
        pass
    return None


def _model_installed(key: str) -> bool:
    d = BUNDLE_DIR / key
    if not d.is_dir():
        return False
    if key == "nemotron-nvfp4":
        shards = list(d.glob("model-*.safetensors"))
        return len(shards) >= 17 and (d / "super_v3_reasoning_parser.py").is_file()
    return any(d.rglob("*.safetensors")) or any(d.rglob("*.gguf"))


def _probe_vllm(port: int) -> dict | None:
    try:
        out = subprocess.run(
            [
                "curl", "-sf", "--max-time", "2",
                f"http://127.0.0.1:{port}/v1/models",
            ],
            capture_output=True, text=True, timeout=5,
        )
        if out.returncode != 0 or not out.stdout.strip():
            return None
        data = json.loads(out.stdout)
        models = data.get("data") or []
        if not models:
            return {"port": port, "id": "unknown", "ready": True}
        return {"port": port, "id": models[0].get("id", "unknown"), "ready": True}
    except (json.JSONDecodeError, subprocess.TimeoutExpired, OSError):
        return None


def _vllm_loading(port: int) -> bool:
    try:
        out = subprocess.run(
            ["pgrep", "-af", f"vllm.entrypoints.openai.api_server.*--port {port}"],
            capture_output=True, text=True, timeout=5,
        )
        return bool(out.stdout.strip())
    except (subprocess.TimeoutExpired, OSError):
        return False


def parse_manifest() -> list[dict]:
    """Parse manifest.txt rows; fall back to disk scan if missing."""
    entries: list[dict] = []
    if MANIFEST.is_file():
        for line in MANIFEST.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("Total") or line.startswith("-"):
                continue
            if "|" not in line:
                continue
            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 6:
                continue
            key, size, shards, repo, port_s, spark = parts[:6]
            if key in REMOVED_MODELS:
                continue
            port = int(port_s) if port_s.isdigit() else VLLM_PORTS.get(key, (0, "", ""))[0]
            meta = VLLM_PORTS.get(key, (port, key, "other"))
            entries.append({
                "key": key,
                "label": meta[1],
                "tier": meta[2],
                "size_manifest": size,
                "shards": shards,
                "repo": repo,
                "port": port or meta[0],
                "single_spark": spark,
            })
    if entries:
        return entries

    # Fallback: scan bundle dirs
    for key, (port, label, tier) in VLLM_PORTS.items():
        entries.append({
            "key": key,
            "label": label,
            "tier": tier,
            "size_manifest": _dir_size_gb(BUNDLE_DIR / key) or "--",
            "shards": "--",
            "repo": "",
            "port": port,
            "single_spark": "✅" if _model_installed(key) else "--",
        })
    return entries


def query_vllm() -> list[dict]:
    active: list[dict] = []
    for key, (port, label, tier) in VLLM_PORTS.items():
        probe = _probe_vllm(port)
        if probe:
            active.append({
                "key": key,
                "label": label,
                "tier": tier,
                "port": port,
                "model_id": probe["id"],
                "status": "ready",
            })
        elif _vllm_loading(port):
            active.append({
                "key": key,
                "label": label,
                "tier": tier,
                "port": port,
                "model_id": None,
                "status": "loading",
            })
    return active


def query_inventory() -> dict:
    manifest_mtime = MANIFEST.stat().st_mtime if MANIFEST.is_file() else None
    models = []
    for row in parse_manifest():
        key = row["key"]
        path = BUNDLE_DIR / key
        installed = _model_installed(key)
        size_disk = _dir_size_gb(path) if installed else None
        port = row["port"]
        probe = _probe_vllm(port) if port else None
        status = "running" if probe else ("loading" if port and _vllm_loading(port) else (
            "installed" if installed else "missing"
        ))
        models.append({
            **row,
            "installed": installed,
            "size_disk": size_disk or row["size_manifest"],
            "status": status,
            "active_model_id": probe["id"] if probe else None,
        })
    return {
        "bundle_dir": str(BUNDLE_DIR),
        "manifest": str(MANIFEST),
        "manifest_mtime": manifest_mtime,
        "models": models,
        "active_vllm": query_vllm(),
        "removed": sorted(REMOVED_MODELS),
    }


def sync_manifest_from_disk() -> bool:
    """Rewrite manifest size/install rows from on-disk state."""
    if not MANIFEST.is_file():
        return False
    lines = MANIFEST.read_text().splitlines()
    out: list[str] = []
    changed = False
    row_re = re.compile(
        r"^(\S+)\s*\|\s*([^|]+)\|\s*(\d+|--)\s*\|\s*([^|]+)\|\s*(\d+|--)\s*\|\s*(.+)$"
    )
    for line in lines:
        m = row_re.match(line.strip())
        if not m:
            out.append(line)
            continue
        key, size, shards, repo, port, spark = m.groups()
        key = key.strip()
        if key in REMOVED_MODELS:
            changed = True
            continue
        path = BUNDLE_DIR / key
        if path.is_dir():
            disk = _dir_size_gb(path)
            sf = list(path.glob("model-*.safetensors"))
            if disk and disk != size.strip():
                size = f" {disk} "
                changed = True
            if sf and str(len(sf)) != shards.strip():
                shards = f" {len(sf)} "
                changed = True
        out.append(f"{key} |{size}|{shards}| {repo.strip()} |{port.strip()}| {spark.strip()}")
    if changed:
        MANIFEST.write_text("\n".join(out) + "\n")
    return changed