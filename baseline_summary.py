#!/usr/bin/env python3
"""Summarize schema-v2 dual-node samples from the temporary baseline."""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import math
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent
CSV_FILE = PROJECT_DIR / "data/performance_timeseries.csv"
SUMMARY_DIR = PROJECT_DIR / "data/baselines"


def _number(value: str | None) -> float | None:
    """Parse measured values while treating legacy blanks as unavailable."""
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _percentile(values: list[float], percentile: float) -> float | None:
    """Use linear interpolation so small early baselines remain readable."""
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * percentile
    low = math.floor(index)
    high = math.ceil(index)
    if low == high:
        return round(ordered[low], 2)
    weight = index - low
    return round(ordered[low] * (1 - weight) + ordered[high] * weight, 2)


def _stats(rows: list[dict], key: str) -> dict:
    """Return p50/p95/max for one numeric CSV column."""
    values = [value for row in rows if (value := _number(row.get(key))) is not None]
    return {
        "p50": _percentile(values, 0.50),
        "p95": _percentile(values, 0.95),
        "max": round(max(values), 2) if values else None,
    }


def _availability(rows: list[dict], key: str) -> float | None:
    """Calculate the percentage of samples with a truthy endpoint flag."""
    values = [row.get(key, "").lower() for row in rows if row.get(key) not in (None, "")]
    if not values:
        return None
    successes = sum(value in {"true", "1", "yes"} for value in values)
    return round(successes / len(values) * 100, 2)


def _longest_run(rows: list[dict], key: str, threshold: float) -> int:
    """Count consecutive 5-minute samples above a policy threshold."""
    longest = current = 0
    for row in rows:
        value = _number(row.get(key))
        current = current + 1 if value is not None and value > threshold else 0
        longest = max(longest, current)
    return longest


def _workload_overlaps(rows: list[dict]) -> dict[str, int]:
    """Count pairwise overlap windows after grouping inference runtimes."""
    counts: Counter[str] = Counter()
    for row in rows:
        try:
            payload = json.loads(row.get("workloads_json") or "{}")
        except (TypeError, ValueError):
            continue
        labels = set(payload.get("node1") or []) | set(payload.get("node2") or [])
        if "inference-busy" in labels:
            labels.add("inference")
        labels &= {"inference", "pokemon", "comfyui", "download", "agent-cron"}
        for left, right in itertools.combinations(sorted(labels), 2):
            counts[f"{left}+{right}"] += 1
    return dict(sorted(counts.items()))


def load_rows(hours: int, now: datetime | None = None) -> list[dict]:
    """Load only schema-v2 rows inside the requested UTC measurement window."""
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours)
    rows = []
    with CSV_FILE.open(newline="") as source:
        for row in csv.DictReader(source):
            if row.get("schema_version") != "2":
                continue
            try:
                timestamp = datetime.fromisoformat(row["iso_ts"])
            except (KeyError, TypeError, ValueError):
                continue
            if timestamp >= cutoff:
                rows.append(row)
    return rows


def summarize(rows: list[dict], hours: int) -> dict:
    """Build the reproducible baseline report used by the approval gate."""
    node1_ram = [value for row in rows if (value := _number(row.get("mem_avail_gb"))) is not None]
    node2_ram = [value for row in rows if (value := _number(row.get("node2_mem_avail_gb"))) is not None]
    node1_swap = [value for row in rows if (value := _number(row.get("swap_used_gb"))) is not None]
    node2_swap = [value for row in rows if (value := _number(row.get("node2_swap_used_gb"))) is not None]
    return {
        "schema_version": 2,
        "window_hours": hours,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sample_count": len(rows),
        "expected_samples": hours * 12,
        "utilization": {
            "node1_cpu_pct": _stats(rows, "cpu_pct"),
            "node1_gpu_pct": _stats(rows, "gpu_util"),
            "node2_cpu_pct": _stats(rows, "node2_cpu_pct"),
            "node2_gpu_pct": _stats(rows, "node2_gpu_util"),
        },
        "memory": {
            "node1_min_available_gb": round(min(node1_ram), 2) if node1_ram else None,
            "node2_min_available_gb": round(min(node2_ram), 2) if node2_ram else None,
            "node1_max_swap_gb": round(max(node1_swap), 2) if node1_swap else None,
            "node2_max_swap_gb": round(max(node2_swap), 2) if node2_swap else None,
            "samples_below_15gb": sum(
                any(value is not None and value < 15 for value in (
                    _number(row.get("mem_avail_gb")),
                    _number(row.get("node2_mem_avail_gb")),
                ))
                for row in rows
            ),
            "node1_longest_swap_over_8gb_samples": _longest_run(rows, "swap_used_gb", 8),
            "node2_longest_swap_over_8gb_samples": _longest_run(rows, "node2_swap_used_gb", 8),
        },
        "endpoint_availability_pct": {
            "node1_8889": _availability(rows, "node1_endpoint_8889_ok"),
            "node2_8100": _availability(rows, "node2_endpoint_8100_ok"),
            "node2_ssh": _availability(rows, "node2_reachable"),
        },
        "overlap_samples": _workload_overlaps(rows),
    }


def _human_summary(report: dict, output_path: Path) -> str:
    """Keep the completion page short enough for Telegram."""
    memory = report["memory"]
    endpoints = report["endpoint_availability_pct"]
    return (
        f"DGX baseline: {report['sample_count']}/{report['expected_samples']} samples\n"
        f"Min available RAM: node1 {memory['node1_min_available_gb']}GB, "
        f"node2 {memory['node2_min_available_gb']}GB\n"
        f"Max swap: node1 {memory['node1_max_swap_gb']}GB, "
        f"node2 {memory['node2_max_swap_gb']}GB\n"
        f"Availability: :8889 {endpoints['node1_8889']}%, "
        f"node2 :8100 {endpoints['node2_8100']}%, SSH {endpoints['node2_ssh']}%\n"
        f"15GB-gate samples: {memory['samples_below_15gb']}\n"
        f"Report: {output_path}"
    )


def main() -> int:
    """Generate JSON and print a concise durable-report pointer."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=72)
    args = parser.parse_args()

    rows = load_rows(args.hours)
    report = summarize(rows, args.hours)
    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = SUMMARY_DIR / f"baseline-{stamp}.json"
    output_path.write_text(json.dumps(report, indent=2) + "\n")
    print(_human_summary(report, output_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
