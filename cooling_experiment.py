#!/usr/bin/env python3
"""
Case/fan cooling A/B (and multi-phase) helper.

Marks time windows against the 5-minute performance_timeseries.csv and
compares GPU temps with util buckets so load differences don't fake a win.

Phases (typical):
  no_fan  →  fan_1700  →  fan_3000

Usage:
  python3 cooling_experiment.py mark-before [--note "..."] [--nodes node1,node2]
  python3 cooling_experiment.py mark-after  [--note "..."]   # 2-phase legacy
  python3 cooling_experiment.py mark-phase --id fan_3000 --label "3000 RPM" [--note "..."]
  python3 cooling_experiment.py status
  python3 cooling_experiment.py compare [--min-hours 0] [--phases no_fan,fan_1700,fan_3000]
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent
CSV_FILE = PROJECT_DIR / "data/performance_timeseries.csv"
EXPERIMENT_FILE = PROJECT_DIR / "data/cooling-experiment.json"
REPORT_DIR = PROJECT_DIR / "data/cooling-reports"

# Util buckets keep phases comparable under different inference load.
UTIL_BUCKETS = (
    ("idle", 0.0, 10.0),
    ("mid", 10.0, 70.0),
    ("high", 70.0, 100.1),
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def _number(value: str | None) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _percentile(values: list[float], percentile: float) -> float | None:
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


def _stats(values: list[float]) -> dict:
    if not values:
        return {"n": 0, "min": None, "p50": None, "mean": None, "p95": None, "max": None}
    return {
        "n": len(values),
        "min": round(min(values), 2),
        "p50": _percentile(values, 0.50),
        "mean": round(statistics.mean(values), 2),
        "p95": _percentile(values, 0.95),
        "max": round(max(values), 2),
    }


def _load_experiment() -> dict:
    if EXPERIMENT_FILE.exists():
        return json.loads(EXPERIMENT_FILE.read_text())
    return {}


def _save_experiment(payload: dict) -> None:
    EXPERIMENT_FILE.parent.mkdir(parents=True, exist_ok=True)
    EXPERIMENT_FILE.write_text(json.dumps(payload, indent=2) + "\n")


def _ensure_phases(payload: dict) -> dict:
    """Migrate legacy before/after fields into a phases list (in place)."""
    if payload.get("phases"):
        return payload
    phases: list[dict] = []
    if payload.get("before_start"):
        phases.append(
            {
                "id": "no_fan",
                "label": "no fan",
                "start": payload["before_start"],
                "end": payload.get("before_end"),
                "note": (payload.get("notes") or {}).get("before", "pre case+fan"),
            }
        )
    if payload.get("after_start"):
        phases.append(
            {
                "id": "fan_1700",
                "label": "1700 RPM",
                "start": payload["after_start"],
                "end": payload.get("after_end"),
                "note": (payload.get("notes") or {}).get("after", "case+fan installed"),
            }
        )
    if phases:
        payload["phases"] = phases
        payload["schema"] = "phases-v1"
    return payload


def mark_before(note: str, nodes: list[str]) -> dict:
    """Open the pre-case (no fan) window at now."""
    now = _iso(_now())
    payload = {
        "name": "case-fan-cooling",
        "schema": "phases-v1",
        "nodes": nodes,
        "created_at": now,
        # legacy mirrors for older tooling/status
        "before_start": now,
        "before_end": None,
        "after_start": None,
        "after_end": None,
        "notes": {"before": note},
        "phases": [
            {
                "id": "no_fan",
                "label": "no fan",
                "start": now,
                "end": None,
                "note": note,
            }
        ],
        "collector": {
            "csv": str(CSV_FILE),
            "interval_hint_s": 300,
            "node1_temp_col": "gpu_temp_c",
            "node2_temp_col": "node2_gpu_temp_c",
        },
    }
    _save_experiment(payload)
    return payload


def mark_after(note: str, phase_id: str = "fan_1700", label: str = "1700 RPM") -> dict:
    """Close open phase and start fan phase (legacy 2-window entry)."""
    return mark_phase(phase_id=phase_id, label=label, note=note)


def mark_phase(phase_id: str, label: str, note: str) -> dict:
    """Close the current open phase and start a new named phase."""
    payload = _ensure_phases(_load_experiment())
    if not payload.get("phases"):
        raise SystemExit("No experiment — run mark-before first.")

    now = _iso(_now())
    phases: list[dict] = payload["phases"]

    # refuse duplicate ids
    if any(p.get("id") == phase_id for p in phases):
        raise SystemExit(
            f"Phase id '{phase_id}' already exists. Pick a new --id or edit {EXPERIMENT_FILE}."
        )

    # close any open phase (end is None)
    open_phases = [p for p in phases if not p.get("end")]
    if not open_phases:
        # also allow if last phase has end but user wants another phase after a gap
        pass
    else:
        for p in open_phases:
            p["end"] = now

    phases.append(
        {
            "id": phase_id,
            "label": label,
            "start": now,
            "end": None,
            "note": note,
        }
    )
    payload["phases"] = phases
    payload["schema"] = "phases-v1"

    # keep legacy mirrors: first phase = before, second = after when present
    if len(phases) >= 1:
        payload["before_start"] = phases[0]["start"]
        payload["before_end"] = phases[0].get("end")
    if len(phases) >= 2:
        payload["after_start"] = phases[1]["start"]
        payload["after_end"] = phases[1].get("end")
    # if 3+ phases, after_* tracks the latest non-baseline phase end only for legacy;
    # compare uses phases.

    payload.setdefault("notes", {})
    payload["notes"][phase_id] = note
    if phase_id.startswith("fan") or "fan" in phase_id:
        payload["notes"]["after"] = note

    _save_experiment(payload)
    return payload


def _load_rows(start: datetime, end: datetime | None = None) -> list[dict]:
    end = end or _now()
    rows: list[dict] = []
    with CSV_FILE.open(newline="") as source:
        for row in csv.DictReader(source):
            ts = _parse_iso(row.get("iso_ts"))
            if ts is None or ts < start or ts > end:
                continue
            rows.append(row)
    return rows


def _bucket_name(util: float | None) -> str | None:
    if util is None:
        return None
    for name, lo, hi in UTIL_BUCKETS:
        if lo <= util < hi:
            return name
    return None


def _node_series(rows: list[dict], node: str) -> dict[str, list[float]]:
    """Temps + util for one node, plus per-bucket temp lists."""
    if node == "node1":
        temp_key, util_key, power_key = "gpu_temp_c", "gpu_util", "gpu_power_w"
    else:
        temp_key, util_key, power_key = "node2_gpu_temp_c", "node2_gpu_util", "node2_gpu_power_w"

    temps: list[float] = []
    utils: list[float] = []
    powers: list[float] = []
    buckets: dict[str, list[float]] = {name: [] for name, _, _ in UTIL_BUCKETS}

    for row in rows:
        temp = _number(row.get(temp_key))
        util = _number(row.get(util_key))
        power = _number(row.get(power_key))
        if temp is None:
            continue
        temps.append(temp)
        if util is not None:
            utils.append(util)
            bucket = _bucket_name(util)
            if bucket:
                buckets[bucket].append(temp)
        if power is not None:
            powers.append(power)

    return {
        "temp": temps,
        "util": utils,
        "power": powers,
        "buckets": buckets,
    }


def _delta(a: dict, b: dict) -> dict:
    """b − a (later − earlier). Negative temp = cooler."""
    out = {}
    for key in ("min", "p50", "mean", "p95", "max"):
        left, right = a.get(key), b.get(key)
        out[key] = round(right - left, 2) if left is not None and right is not None else None
    return out


def _phase_window(phase: dict) -> tuple[datetime, datetime, float, list[dict]]:
    start = _parse_iso(phase["start"])
    if start is None:
        raise SystemExit(f"Phase {phase.get('id')} has bad start")
    end = _parse_iso(phase.get("end")) or _now()
    hours = (end - start).total_seconds() / 3600.0
    rows = _load_rows(start, end)
    return start, end, hours, rows


def compare(
    min_hours: float = 0.0,
    phase_ids: list[str] | None = None,
) -> dict:
    """Compare named phases; default = all phases in order."""
    payload = _ensure_phases(_load_experiment())
    phases = payload.get("phases") or []
    if len(phases) < 2:
        raise SystemExit("Need at least 2 phases. mark-before + mark-phase (or mark-after).")

    if phase_ids:
        by_id = {p["id"]: p for p in phases}
        missing = [pid for pid in phase_ids if pid not in by_id]
        if missing:
            raise SystemExit(f"Unknown phase ids: {missing}. Have: {list(by_id)}")
        selected = [by_id[pid] for pid in phase_ids]
    else:
        selected = phases

    nodes = payload.get("nodes") or ["node1", "node2"]
    phase_data: dict[str, dict] = {}

    for phase in selected:
        start, end, hours, rows = _phase_window(phase)
        if hours < min_hours and phase.get("end"):
            # only enforce min hours on closed phases if user set min; open phases always included with override
            pass
        if hours < min_hours:
            raise SystemExit(
                f"Phase '{phase['id']}' only {hours:.2f}h; need >= {min_hours}h "
                f"(pass --min-hours 0 to override)."
            )
        per_node = {}
        for node in nodes:
            series = _node_series(rows, node)
            per_node[node] = {
                "temp": _stats(series["temp"]),
                "util": _stats(series["util"]),
                "power_w": _stats(series["power"]),
                "by_util_bucket": {
                    name: _stats(series["buckets"][name]) for name, _, _ in UTIL_BUCKETS
                },
            }
        phase_data[phase["id"]] = {
            "label": phase.get("label") or phase["id"],
            "note": phase.get("note"),
            "start": phase["start"],
            "end": phase.get("end") or _iso(end),
            "hours": round(hours, 2),
            "samples": len(rows),
            "nodes": per_node,
        }

    # pairwise deltas: each phase vs first (baseline), and consecutive
    baseline_id = selected[0]["id"]
    deltas: dict[str, dict] = {}
    for phase in selected[1:]:
        pid = phase["id"]
        node_deltas = {}
        for node in nodes:
            base = phase_data[baseline_id]["nodes"][node]
            cur = phase_data[pid]["nodes"][node]
            bucket_d = {}
            for name, _, _ in UTIL_BUCKETS:
                bucket_d[name] = {
                    "baseline": base["by_util_bucket"][name],
                    "phase": cur["by_util_bucket"][name],
                    "temp_delta_c": _delta(base["by_util_bucket"][name], cur["by_util_bucket"][name]),
                }
            node_deltas[node] = {
                "temp_delta_c": _delta(base["temp"], cur["temp"]),
                "by_util_bucket": bucket_d,
                "util_mean": {
                    "baseline": base["util"]["mean"],
                    "phase": cur["util"]["mean"],
                },
                "power_mean": {
                    "baseline": base["power_w"]["mean"],
                    "phase": cur["power_w"]["mean"],
                },
            }
        deltas[pid] = {
            "vs": baseline_id,
            "nodes": node_deltas,
        }

    # consecutive chain (e.g. 1700 vs 3000)
    chain: list[dict] = []
    for i in range(1, len(selected)):
        prev_id = selected[i - 1]["id"]
        cur_id = selected[i]["id"]
        node_deltas = {}
        for node in nodes:
            prev = phase_data[prev_id]["nodes"][node]
            cur = phase_data[cur_id]["nodes"][node]
            bucket_d = {}
            for name, _, _ in UTIL_BUCKETS:
                bucket_d[name] = _delta(
                    prev["by_util_bucket"][name], cur["by_util_bucket"][name]
                )
            node_deltas[node] = {
                "temp_delta_c": _delta(prev["temp"], cur["temp"]),
                "by_util_bucket": bucket_d,
            }
        chain.append({"from": prev_id, "to": cur_id, "nodes": node_deltas})

    report = {
        "experiment": payload.get("name"),
        "generated_at": _iso(_now()),
        "phases": phase_data,
        "deltas_vs_baseline": deltas,
        "deltas_chain": chain,
        "reading_guide": (
            "delta = later − earlier (°C). Negative = cooler. "
            "Prefer util-bucket deltas over overall mean when loads differ. "
            "Video/diffusion peaks are a hotter class than light LLM decode."
        ),
    }

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = _now().strftime("%Y%m%dT%H%M%SZ")
    ids = "-".join(p["id"] for p in selected)
    out_path = REPORT_DIR / f"cooling-compare-{ids}-{stamp}.json"
    out_path.write_text(json.dumps(report, indent=2) + "\n")
    report["report_path"] = str(out_path)
    return report


def _fmt_stat(s: dict, key: str = "mean") -> str:
    v = s.get(key)
    return "—" if v is None else f"{v}"


def _print_compare(report: dict) -> None:
    print(report["reading_guide"])
    print()
    # phase summary table
    print("=== PHASES ===")
    for pid, body in report["phases"].items():
        print(
            f"  {pid:12s}  {body['label']:12s}  "
            f"{body['hours']:6.2f}h  n={body['samples']:4d}  "
            f"{body['start'][:16]} → {(body['end'] or 'open')[:16]}"
        )

    # per-node summary across phases
    phase_ids = list(report["phases"].keys())
    nodes = list(next(iter(report["phases"].values()))["nodes"].keys())
    for node in nodes:
        print(f"\n=== {node} temps (°C) ===")
        header = f"{'metric':12s}" + "".join(f"  {pid:>12s}" for pid in phase_ids)
        print(header)
        for metric in ("mean", "p95", "max"):
            row = f"{metric:12s}"
            for pid in phase_ids:
                row += f"  {_fmt_stat(report['phases'][pid]['nodes'][node]['temp'], metric):>12s}"
            print(row)
        # high-util mean
        row = f"{'high mean':12s}"
        for pid in phase_ids:
            row += f"  {_fmt_stat(report['phases'][pid]['nodes'][node]['by_util_bucket']['high'], 'mean'):>12s}"
        print(row)
        row = f"{'high max':12s}"
        for pid in phase_ids:
            row += f"  {_fmt_stat(report['phases'][pid]['nodes'][node]['by_util_bucket']['high'], 'max'):>12s}"
        print(row)
        row = f"{'idle mean':12s}"
        for pid in phase_ids:
            row += f"  {_fmt_stat(report['phases'][pid]['nodes'][node]['by_util_bucket']['idle'], 'mean'):>12s}"
        print(row)
        # load check
        row = f"{'util mean':12s}"
        for pid in phase_ids:
            row += f"  {_fmt_stat(report['phases'][pid]['nodes'][node]['util'], 'mean'):>12s}"
        print(row)
        row = f"{'power mean':12s}"
        for pid in phase_ids:
            row += f"  {_fmt_stat(report['phases'][pid]['nodes'][node]['power_w'], 'mean'):>12s}"
        print(row)

    print("\n=== DELTAS vs baseline (negative = cooler) ===")
    for pid, body in report["deltas_vs_baseline"].items():
        print(f"\n{pid} vs {body['vs']}:")
        for node, nd in body["nodes"].items():
            d = nd["temp_delta_c"]
            print(
                f"  {node}: mean Δ {d['mean']}  p95 Δ {d['p95']}  max Δ {d['max']}  "
                f"(util {nd['util_mean']['baseline']}→{nd['util_mean']['phase']}, "
                f"power {nd['power_mean']['baseline']}→{nd['power_mean']['phase']})"
            )
            for bucket, stats in nd["by_util_bucket"].items():
                bd = stats["temp_delta_c"]
                bn, an = stats["baseline"]["n"], stats["phase"]["n"]
                if bn == 0 and an == 0:
                    continue
                print(
                    f"    {bucket:4s}: n {bn}→{an}  mean Δ {bd['mean']}  "
                    f"p95 Δ {bd['p95']}  max Δ {bd['max']}"
                )

    if len(report["deltas_chain"]) > 1:
        print("\n=== CHAIN (consecutive phases) ===")
        for step in report["deltas_chain"]:
            print(f"\n{step['from']} → {step['to']}:")
            for node, nd in step["nodes"].items():
                d = nd["temp_delta_c"]
                print(f"  {node}: mean Δ {d['mean']}  p95 Δ {d['p95']}  max Δ {d['max']}")
                for bucket, bd in nd["by_util_bucket"].items():
                    if bd.get("mean") is None and bd.get("max") is None:
                        continue
                    print(
                        f"    {bucket:4s}: mean Δ {bd['mean']}  p95 Δ {bd['p95']}  max Δ {bd['max']}"
                    )

    print(f"\nReport: {report['report_path']}")


def status() -> None:
    payload = _ensure_phases(_load_experiment())
    if not payload:
        print("No cooling experiment marked yet.")
        return
    # persist migration if we upgraded
    if payload.get("phases") and payload.get("schema") != "phases-v1":
        payload["schema"] = "phases-v1"
    if payload.get("phases") and "schema" in payload:
        _save_experiment(payload)

    print(json.dumps(payload, indent=2))
    phases = payload.get("phases") or []
    if not phases:
        return
    print("\n=== phase sample counts ===")
    for phase in phases:
        start, end, hours, rows = _phase_window(phase)
        open_tag = " OPEN" if not phase.get("end") else ""
        n2 = sum(1 for r in rows if _number(r.get("node2_gpu_temp_c")) is not None)
        print(
            f"  {phase['id']:12s}  {phase.get('label', ''):12s}  "
            f"{hours:6.2f}h  n={len(rows):4d}  n2={n2:4d}{open_tag}"
        )
        if phase.get("note"):
            print(f"               note: {phase['note']}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_before = sub.add_parser("mark-before", help="Start no-fan baseline now")
    p_before.add_argument("--note", default="pre case+fan")
    p_before.add_argument(
        "--nodes",
        default="node1,node2",
        help="Comma list: node1,node2 (default both)",
    )

    p_after = sub.add_parser(
        "mark-after",
        help="Legacy: close baseline, start fan phase (default fan_1700)",
    )
    p_after.add_argument("--note", default="case+fan installed")
    p_after.add_argument("--id", default="fan_1700", help="Phase id (default fan_1700)")
    p_after.add_argument("--label", default="1700 RPM", help="Human label")

    p_phase = sub.add_parser(
        "mark-phase",
        help="Close current open phase and start a new named phase (e.g. fan_3000)",
    )
    p_phase.add_argument("--id", required=True, help="Phase id, e.g. fan_3000")
    p_phase.add_argument("--label", default="", help="Human label, e.g. '3000 RPM'")
    p_phase.add_argument("--note", default="")

    sub.add_parser("status", help="Show experiment markers + sample counts")

    p_cmp = sub.add_parser("compare", help="Print multi-phase temp table + deltas")
    p_cmp.add_argument(
        "--min-hours",
        type=float,
        default=0.0,
        help="Refuse if any selected phase has fewer hours (default 0)",
    )
    # backward-compat alias
    p_cmp.add_argument(
        "--min-hours-after",
        type=float,
        default=None,
        help=argparse.SUPPRESS,
    )
    p_cmp.add_argument(
        "--phases",
        default="",
        help="Comma phase ids to include (default: all, in order)",
    )

    args = parser.parse_args()

    if args.cmd == "mark-before":
        nodes = [n.strip() for n in args.nodes.split(",") if n.strip()]
        payload = mark_before(args.note, nodes)
        print("BEFORE / no_fan marked at", payload["before_start"])
        print("Next fan install: python3 cooling_experiment.py mark-phase --id fan_1700 --label '1700 RPM'")
        print("Marker:", EXPERIMENT_FILE)
        return 0

    if args.cmd == "mark-after":
        payload = mark_after(args.note, phase_id=args.id, label=args.label)
        print(f"PHASE {args.id} marked at", payload["phases"][-1]["start"])
        print("Soak, then: python3 cooling_experiment.py compare")
        return 0

    if args.cmd == "mark-phase":
        label = args.label or args.id
        note = args.note or label
        payload = mark_phase(phase_id=args.id, label=label, note=note)
        print(f"PHASE {args.id} ({label}) marked at", payload["phases"][-1]["start"])
        print("Prior open phase closed. Soak, then: python3 cooling_experiment.py compare")
        return 0

    if args.cmd == "status":
        status()
        return 0

    if args.cmd == "compare":
        min_hours = args.min_hours
        if args.min_hours_after is not None:
            min_hours = args.min_hours_after
        phase_ids = [p.strip() for p in args.phases.split(",") if p.strip()] or None
        report = compare(min_hours=min_hours, phase_ids=phase_ids)
        _print_compare(report)
        return 0

    raise SystemExit(f"unknown command: {args.cmd}")


if __name__ == "__main__":
    raise SystemExit(main())
