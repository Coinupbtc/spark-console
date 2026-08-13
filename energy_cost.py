#!/usr/bin/env python3
"""Historical electricity energy log — watts × time → kWh, then $ = kWh × rate.

Why this exists
---------------
The console used to show live watt *projections* ($/hr extrapolated). Those are
not a bill. This module integrates real power samples over time so the UI can
show an actual trailing 24h cost, and a trailing 30d cost once enough history
exists.

IMPORTANT — Sparks are NOT wall-metered
---------------------------------------
nvidia-smi on GB10 reports GPU / board-slice draw, not AC wall watts. A DGX
Spark idles ~22–45 W at the outlet (higher with CX7 fabric up) and much more
under load, while GPU draw alone often sits ~10–20 W. Billing GPU-only made
the 30d total look like ~$2.60 — that was the bug the owner smelled (real
fleet wall is more like ~$10–40/mo at residential rates, higher under load).

Default bill view is **wall estimate** for Sparks:
      wall ≈ idle_wall_w + slope * max(0, gpu_w − gpu_floor_w)   capped at 240 W PSU
(editable idle W after a USB-C / Kill-A-Watt reading). Pi applies a USB-C PSU
factor; Start9 stays RAPL (CPU+DRAM, still understates the outlet).
`mode=sensor` is still available for raw GPU/PMIC/RAPL integration.

This cluster runs CX7 fabric for DS4F, so idle wall is NOT the 22 W headless
CX7-down figure. NVIDIA/Toms: ~37 W idle with CX7 always-on; 240 W adapter.

Sources
-------
  * Sparks (node1/node2): GPU power.draw.average from live hist + 5‑min CSV
  * Pi: board PMIC watts from fleet SSH polls
  * Start9: RAPL package+DRAM watts from fleet SSH polls

Durable files (under data/)
---------------------------
  energy_samples.jsonl  — throttled watt snapshots (pruned ~45d)
  energy_daily.json     — per‑UTC‑day sensor kWh rollups (kept forever)
"""
from __future__ import annotations

import csv
import fcntl
import json
import os
import threading
import time
from pathlib import Path

NODES = ("node1", "node2", "pi", "start9")
SPARK_NODES = ("node1", "node2")
NODE_SOURCE = {
    "node1": "gpu_draw",
    "node2": "gpu_draw",
    "pi": "board_pmic",
    "start9": "rapl_pkg_dram",
}

# Default wall calibration for Sparks (CX7 fabric typically up on this cluster).
# Override via energy_summary(idle_wall_w=…) / console localStorage.
# gpu_floor matches this fleet's measured idle nvidia-smi (~11 W median), so
# idle_wall_w is "outlet watts at that GPU reading" — not double-counted.
DEFAULT_IDLE_WALL_W = 50.0   # outlet watts at idle-ish with CX7 up (conservative)
DEFAULT_GPU_FLOOR_W = 11.0   # GPU draw treated as that idle point
DEFAULT_SLOPE = 1.15         # wall rises ~1.15 W per extra GPU watt above floor
WALL_CAP_W = 240.0           # DGX Spark external PSU rating
PI_PSU_EFF = 1.12            # USB-C brick; PMIC is board DC, not wall

PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
SAMPLES_PATH = DATA_DIR / "energy_samples.jsonl"
DAILY_PATH = DATA_DIR / "energy_daily.json"
CSV_FILE = DATA_DIR / "performance_timeseries.csv"
BACKFILL_MARKER = DATA_DIR / "energy_backfill.done"

# Don't invent energy across long outages / missing polls.
MAX_GAP_S = 2 * 3600
# One-sided hold (node missing from one endpoint) only across a couple of
# missed 30 s ticks. A 1 h hold after a node goes unreachable used to bill
# last-known watts as if it were still on.
HOLD_MAX_S = 90.0
# Keep raw samples long enough for a rolling 30d window + slack.
SAMPLE_RETENTION_S = 45 * 86400
# Reuse live hist (~1 Hz already paid for). 30s is dense enough for $ without
# extra nvidia-smi; collector CSV still backfills gaps.
MIN_SAMPLE_INTERVAL_S = 30.0

_lock = threading.Lock()
_last_sample_t = 0.0


# ---- file helpers -----------------------------------------------------------


def _atomic_write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w") as f:
        json.dump(obj, f, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _load_daily() -> dict:
    if not DAILY_PATH.exists():
        return {}
    try:
        return json.loads(DAILY_PATH.read_text())
    except Exception:
        return {}


def _save_daily(daily: dict) -> None:
    _atomic_write_json(DAILY_PATH, daily)


# ---- wall estimate ----------------------------------------------------------


def gpu_to_wall(
    gpu_w,
    idle_wall_w: float = DEFAULT_IDLE_WALL_W,
    gpu_floor_w: float = DEFAULT_GPU_FLOOR_W,
    slope: float = DEFAULT_SLOPE,
) -> float | None:
    """Estimate AC wall watts from GPU draw for one Spark.

    wall ≈ min(idle_wall + slope * max(0, gpu − floor), 240 W PSU)

    This is a calibration curve, not a meter. Tune idle_wall_w after a USB-C /
    Kill-A-Watt reading at idle with your normal fabric/display config.
    gpu_floor should be nvidia-smi at that same idle reading (this fleet ~11 W).
    """
    if gpu_w is None:
        return None
    try:
        g = float(gpu_w)
    except (TypeError, ValueError):
        return None
    if g != g or g < 0:  # NaN
        return None
    wall = float(idle_wall_w) + float(slope) * max(0.0, g - float(gpu_floor_w))
    return min(wall, float(WALL_CAP_W))


def _finite_w(v) -> float | None:
    if v is None or v == "":
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f or f < 0:
        return None
    return f


def _billable_w(node: str, sensor_w, calib: dict) -> float | None:
    """Watts we integrate for $ — wall estimate on Sparks, PSU-adjusted Pi."""
    if node in SPARK_NODES:
        return gpu_to_wall(
            sensor_w,
            idle_wall_w=calib.get("idle_wall_w", DEFAULT_IDLE_WALL_W),
            gpu_floor_w=calib.get("gpu_floor_w", DEFAULT_GPU_FLOOR_W),
            slope=calib.get("slope", DEFAULT_SLOPE),
        )
    v = _finite_w(sensor_w)
    if v is None:
        return None
    if node == "pi":
        return v * float(calib.get("pi_psu_eff", PI_PSU_EFF))
    return v


# ---- integration ------------------------------------------------------------


def _trap_kwh(t0: float, w0, t1: float, w1,
              hold_max_s: float = HOLD_MAX_S) -> float | None:
    """Trapezoidal kWh for one interval. None if gap too large or both missing.

    One-sided (a node present on only one endpoint) is allowed only for
    hold_max_s — a couple of missed 30 s ticks, not an hour of invented draw.
    """
    dt = t1 - t0
    if dt <= 0 or dt > MAX_GAP_S:
        return None
    a = _finite_w(w0)
    b = _finite_w(w1)
    if a is not None and b is not None:
        avg = (a + b) / 2.0
    elif a is not None or b is not None:
        if dt > float(hold_max_s):
            return None
        avg = a if a is not None else b
    else:
        return None
    if avg < 0:
        return None
    return (avg / 1000.0) * (dt / 3600.0)


def _integrate(samples: list[dict], t_from: float, t_to: float,
               calib: dict | None = None, mode: str = "billable") -> dict:
    """Integrate samples in [t_from, t_to] → per-node kWh + covered seconds.

    mode='billable' uses wall estimate for Sparks; mode='sensor' uses raw GPU/PMIC/RAPL.
    """
    calib = calib or {}
    kwh = {n: 0.0 for n in NODES}
    covered = {n: 0.0 for n in NODES}
    pts = [s for s in samples if t_from <= float(s.get("t", 0)) <= t_to]
    pts.sort(key=lambda s: float(s["t"]))
    for i in range(1, len(pts)):
        a, b = pts[i - 1], pts[i]
        t0, t1 = float(a["t"]), float(b["t"])
        wa, wb = a.get("w") or {}, b.get("w") or {}
        for n in NODES:
            if mode == "sensor":
                w0, w1 = wa.get(n), wb.get(n)
            else:
                w0 = _billable_w(n, wa.get(n), calib)
                w1 = _billable_w(n, wb.get(n), calib)
            piece = _trap_kwh(t0, w0, t1, w1)
            if piece is None:
                continue
            kwh[n] += piece
            covered[n] += (t1 - t0)
    return {"kwh": kwh, "covered_s": covered}


def _read_samples_since(since: float) -> list[dict]:
    """Read jsonl samples with t >= since. Tolerates partial last line."""
    if not SAMPLES_PATH.exists():
        return []
    out = []
    try:
        with SAMPLES_PATH.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                t = row.get("t")
                if t is None:
                    continue
                if float(t) >= since:
                    out.append(row)
    except Exception:
        return []
    return out


# ---- recording --------------------------------------------------------------


def record_sample(watts: dict, t: float | None = None, force: bool = False) -> bool:
    """Append a watt snapshot. Throttled. Also folds energy into energy_daily.json.

    `watts` keys are node ids; missing/None means "no reading this tick" (not 0).
    Values are **sensor** watts (GPU/PMIC/RAPL). Wall estimate is applied at
    summary time so calibration changes reprice history without rewriting logs.
    Returns True if a sample was written.
    """
    global _last_sample_t
    now = float(t if t is not None else time.time())
    clean = {}
    for n in NODES:
        v = watts.get(n) if isinstance(watts, dict) else None
        if v is None or v == "":
            continue
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if fv < 0 or fv != fv:  # NaN
            continue
        clean[n] = round(fv, 3)
    if not clean:
        return False

    with _lock:
        if not force and _last_sample_t and (now - _last_sample_t) < MIN_SAMPLE_INTERVAL_S:
            return False

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        lock_path = SAMPLES_PATH.with_suffix(SAMPLES_PATH.suffix + ".lock")
        with lock_path.open("a+") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)

            # Integrate against the previous sample into the daily rollup (sensor kWh).
            prev = _last_jsonl_row()
            if prev:
                _fold_interval_into_daily(prev, {"t": now, "w": clean})

            with SAMPLES_PATH.open("a") as f:
                f.write(json.dumps({"t": now, "w": clean}, separators=(",", ":")) + "\n")
                f.flush()
                os.fsync(f.fileno())

        _last_sample_t = now
        # Fresh sample → drop summary cache so the next poll recomputes.
        _SUMMARY_CACHE["ts"] = 0.0
    return True


def _last_jsonl_row() -> dict | None:
    """Tail-read the last complete JSONL object (best-effort)."""
    if not SAMPLES_PATH.exists() or SAMPLES_PATH.stat().st_size == 0:
        return None
    try:
        with SAMPLES_PATH.open("rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - 4096))
            chunk = f.read().decode("utf-8", "replace")
        lines = [ln for ln in chunk.splitlines() if ln.strip()]
        if not lines:
            return None
        return json.loads(lines[-1])
    except Exception:
        return None


def _fold_interval_into_daily(prev: dict, cur: dict) -> None:
    """Add one interval's sensor kWh into the UTC day bucket of cur['t']."""
    t0, t1 = float(prev["t"]), float(cur["t"])
    wa, wb = prev.get("w") or {}, cur.get("w") or {}
    day = time.strftime("%Y-%m-%d", time.gmtime(t1))
    daily = _load_daily()
    slot = daily.setdefault(day, {n: 0.0 for n in NODES})
    slot.setdefault("samples", 0)
    for n in NODES:
        piece = _trap_kwh(t0, wa.get(n), t1, wb.get(n))
        if piece is None:
            continue
        slot[n] = round(float(slot.get(n) or 0.0) + piece, 6)
    slot["samples"] = int(slot.get("samples") or 0) + 1
    cutoff = time.strftime("%Y-%m-%d", time.gmtime(t1 - 3 * 365 * 86400))
    daily = {k: v for k, v in daily.items() if k >= cutoff}
    _save_daily(daily)


def prune_samples(now: float | None = None) -> int:
    """Drop samples older than SAMPLE_RETENTION_S. Returns lines removed."""
    now = float(now if now is not None else time.time())
    cutoff = now - SAMPLE_RETENTION_S
    if not SAMPLES_PATH.exists():
        return 0
    with _lock:
        lock_path = SAMPLES_PATH.with_suffix(SAMPLES_PATH.suffix + ".lock")
        with lock_path.open("a+") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            kept = []
            removed = 0
            with SAMPLES_PATH.open() as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                        if float(row.get("t", 0)) >= cutoff:
                            kept.append(line)
                        else:
                            removed += 1
                    except Exception:
                        removed += 1
            tmp = SAMPLES_PATH.with_suffix(".jsonl.tmp")
            with tmp.open("w") as f:
                for ln in kept:
                    f.write(ln + "\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, SAMPLES_PATH)
    return removed


# ---- CSV backfill (Sparks) --------------------------------------------------


def backfill_from_csv(force: bool = False) -> dict:
    """One-shot: seed samples + daily rollups from performance_timeseries.csv.

    Gives node1/node2 an immediate real 24h (and ~30d) history. Pi/Start9 start
    accumulating from live fleet polls onward. Stored values are GPU sensor W;
    wall estimate is applied at summary time.
    """
    if BACKFILL_MARKER.exists() and not force:
        return {"ok": True, "skipped": True, "reason": "already done"}
    if not CSV_FILE.exists():
        return {"ok": False, "error": "no performance_timeseries.csv"}

    rows = []
    with CSV_FILE.open(newline="") as f:
        for r in csv.DictReader(f):
            iso = r.get("iso_ts") or ""
            if not iso:
                continue
            try:
                from datetime import datetime
                t = datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()
            except Exception:
                continue
            w = {}
            try:
                if r.get("gpu_power_w") not in (None, ""):
                    w["node1"] = float(r["gpu_power_w"])
            except (TypeError, ValueError):
                pass
            try:
                if r.get("node2_gpu_power_w") not in (None, ""):
                    w["node2"] = float(r["node2_gpu_power_w"])
            except (TypeError, ValueError):
                pass
            if w:
                rows.append({"t": t, "w": w})
    rows.sort(key=lambda x: x["t"])
    if not rows:
        return {"ok": False, "error": "no power rows in csv"}

    csv_end = rows[-1]["t"]
    live_extra = [s for s in _read_samples_since(csv_end + 0.1)
                  if any(k in (s.get("w") or {}) for k in ("pi", "start9"))]

    with _lock:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        lock_path = SAMPLES_PATH.with_suffix(SAMPLES_PATH.suffix + ".lock")
        with lock_path.open("a+") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            tmp = SAMPLES_PATH.with_suffix(".jsonl.tmp")
            with tmp.open("w") as f:
                for row in rows:
                    f.write(json.dumps(row, separators=(",", ":")) + "\n")
                for row in live_extra:
                    f.write(json.dumps(row, separators=(",", ":")) + "\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, SAMPLES_PATH)

        daily: dict = {}
        merged = rows + live_extra
        for i in range(1, len(merged)):
            a, b = merged[i - 1], merged[i]
            t0, t1 = float(a["t"]), float(b["t"])
            wa, wb = a.get("w") or {}, b.get("w") or {}
            day = time.strftime("%Y-%m-%d", time.gmtime(t1))
            slot = daily.setdefault(day, {n: 0.0 for n in NODES})
            slot.setdefault("samples", 0)
            for n in NODES:
                piece = _trap_kwh(t0, wa.get(n), t1, wb.get(n))
                if piece is None:
                    continue
                slot[n] = round(float(slot.get(n) or 0.0) + piece, 6)
            slot["samples"] = int(slot.get("samples") or 0) + 1
        _save_daily(daily)
        BACKFILL_MARKER.write_text(
            json.dumps({"ok": True, "rows": len(rows), "ts": time.time(),
                        "note": "sensor=gpu; wall applied at summary"}) + "\n"
        )

    return {"ok": True, "rows": len(rows), "days": len(daily), "live_extra": len(live_extra)}


# ---- public summary ---------------------------------------------------------


def _window_payload(samples: list[dict], t_from: float, t_to: float,
                    target_s: float, calib: dict, mode: str = "sensor") -> dict:
    # Default mode=sensor (measured). mode=wall applies Spark wall estimate.
    bill = _integrate(samples, t_from, t_to, calib, mode="billable" if mode == "wall" else "sensor")
    sens = _integrate(samples, t_from, t_to, calib, mode="sensor")
    kwh, covered = bill["kwh"], bill["covered_s"]
    skwh = sens["kwh"]
    nodes = {}
    for n in NODES:
        hrs = covered[n] / 3600.0
        src = NODE_SOURCE[n]
        if mode == "wall" and n in SPARK_NODES:
            src = "wall_est←gpu"
        elif mode == "wall" and n == "pi":
            src = "board_pmic×psu"
        elif mode != "wall" and n in SPARK_NODES:
            src = "gpu_draw"  # honest: measured sensor, not wall estimate
        target_h = target_s / 3600.0
        nodes[n] = {
            "kwh": round(kwh[n], 5),
            "sensor_kwh": round(skwh[n], 5),
            "hours_covered": round(hrs, 2),
            "complete": hrs >= target_h * 0.85,
            "source": src,
            # Clock-window average (missing samples count as 0 W — meter-like).
            "avg_w": round(kwh[n] / target_h * 1000.0, 1) if target_h else None,
        }
    sparks = kwh["node1"] + kwh["node2"]
    fleet = sum(kwh[n] for n in NODES)
    fleet_hrs = max((covered[n] / 3600.0) for n in NODES)
    sparks_hrs = max(covered["node1"], covered["node2"]) / 3600.0
    target_h = target_s / 3600.0
    return {
        "nodes": nodes,
        "sparks_kwh": round(sparks, 5),
        "sparks_sensor_kwh": round(skwh["node1"] + skwh["node2"], 5),
        "fleet_kwh": round(fleet, 5),
        "sparks_hours": round(sparks_hrs, 2),
        "fleet_hours": round(fleet_hrs, 2),
        "sparks_complete": sparks_hrs >= target_h * 0.85,
        # Never treat "no samples → 0 kWh" as a complete window.
        "fleet_complete": all(nodes[n]["complete"] for n in NODES),
        "target_hours": round(target_h, 1),
        "mode": mode,
        "avg_w": round(fleet / target_h * 1000.0, 1) if target_h else None,
        "sparks_avg_w": round(sparks / target_h * 1000.0, 1) if target_h else None,
    }


_SUMMARY_CACHE: dict = {"ts": 0.0, "key": "", "payload": None}
_SUMMARY_TTL = 20.0  # seconds — console polls ~10–20s; avoid re-scanning jsonl


def _pace_from_24h(w24: dict, days: float = 30.0) -> dict:
    """Scale trailing-24h kWh to a full-month pace (clock hours, gaps = 0 W).

    Uses last-24h kWh × 30 d — the last day's meter-like total, not
    (kWh / hours_sampled) which would treat downtime as "still drawing."
    Fleet pace is the sum of per-node paces so a short-coverage node cannot
    steal the window length from a complete sibling.
    """
    min_hrs = 1.0

    def _scale_clock(kwh, hrs):
        try:
            kwh_f = float(kwh)
            hrs_f = float(hrs)
        except (TypeError, ValueError):
            return None
        if hrs_f < min_hrs or kwh_f < 0:
            return None
        # Window is trailing 24 clock hours. Monthly = that day's kWh × 30.
        return round(kwh_f * float(days), 5)

    nodes_out = {}
    for n, row in (w24.get("nodes") or {}).items():
        pk = _scale_clock(row.get("kwh"), row.get("hours_covered"))
        if pk is not None:
            nodes_out[n] = pk
    sparks_parts = [nodes_out[n] for n in SPARK_NODES if n in nodes_out]
    fleet_parts = list(nodes_out.values())
    return {
        "days": float(days),
        "basis": "trailing_24h_clock",
        "fleet_kwh": round(sum(fleet_parts), 5) if fleet_parts else None,
        "sparks_kwh": round(sum(sparks_parts), 5) if sparks_parts else None,
        "nodes": nodes_out,
        "basis_hours": 24.0,
    }


def energy_summary(
    idle_wall_w: float = DEFAULT_IDLE_WALL_W,
    gpu_floor_w: float = DEFAULT_GPU_FLOOR_W,
    slope: float = DEFAULT_SLOPE,
    mode: str = "wall",
) -> dict:
    """Trailing 24h + 30d kWh + 30d pace from 24h average.

    mode='wall' (default): Spark wall estimate curve (bill-like; not a meter).
    mode='sensor': integrate measured watts only (GPU / PMIC / RAPL) — understates
    AC wall for Sparks (often ~$2–4/mo vs ~$10–40 real).
    """
    if not BACKFILL_MARKER.exists():
        try:
            backfill_from_csv()
        except Exception as e:
            return {"ok": False, "error": f"backfill failed: {e}"}

    mode = "wall" if str(mode).lower() == "wall" else "sensor"
    try:
        idle_wall_w = float(idle_wall_w)
        gpu_floor_w = float(gpu_floor_w)
        slope = float(slope)
    except (TypeError, ValueError):
        idle_wall_w, gpu_floor_w, slope = (
            DEFAULT_IDLE_WALL_W, DEFAULT_GPU_FLOOR_W, DEFAULT_SLOPE)

    calib = {
        "idle_wall_w": idle_wall_w,
        "gpu_floor_w": gpu_floor_w,
        "slope": slope,
        "pi_psu_eff": PI_PSU_EFF,
        "wall_cap_w": WALL_CAP_W,
    }
    cache_key = f"{mode}|{idle_wall_w}|{gpu_floor_w}|{slope}|{PI_PSU_EFF}"
    now = time.time()
    cached = _SUMMARY_CACHE.get("payload")
    if (cached and _SUMMARY_CACHE.get("key") == cache_key
            and (now - float(_SUMMARY_CACHE.get("ts") or 0)) < _SUMMARY_TTL):
        out = dict(cached)
        out["cached"] = True
        out["updated_unix"] = _SUMMARY_CACHE["ts"]
        return out

    since = now - SAMPLE_RETENTION_S
    samples = _read_samples_since(since)
    w24 = _window_payload(samples, now - 86400, now, 86400, calib, mode=mode)
    w30 = _window_payload(samples, now - 30 * 86400, now, 30 * 86400, calib, mode=mode)
    pace = _pace_from_24h(w24, days=30.0)
    daily = _load_daily()
    days = []
    for i in range(29, -1, -1):
        day = time.strftime("%Y-%m-%d", time.gmtime(now - i * 86400))
        slot = daily.get(day) or {}
        days.append({
            "day": day,
            "node1_kwh": round(float(slot.get("node1") or 0), 5),
            "node2_kwh": round(float(slot.get("node2") or 0), 5),
            "pi_kwh": round(float(slot.get("pi") or 0), 5),
            "start9_kwh": round(float(slot.get("start9") or 0), 5),
            "samples": int(slot.get("samples") or 0),
        })

    if mode == "wall":
        note = (
            "Sparks $ use WALL ESTIMATE (idle_wall + slope×GPU above 11 W floor, "
            "capped at 240 W PSU) — not an outlet meter. Tune idle wall W after "
            "a Kill-A-Watt reading with CX7 in its usual state. Pi = PMIC×1.12 "
            "USB-C PSU. Start9 = RAPL CPU+DRAM (still understates wall). "
            "Gaps >90s with a node missing are not filled; gaps >2h never are."
        )
    else:
        note = (
            "Measured sensors only: Sparks = GPU draw (nvidia-smi), Pi = board "
            "PMIC, Start9 = RAPL pkg+DRAM. UNDERSTATES AC wall (GPU-only often "
            "~$2–4/mo). Switch to Wall estimate for bill-like $."
        )

    payload = {
        "ok": True,
        "updated_unix": now,
        "cached": False,
        "sample_count": len(samples),
        "mode": mode,
        "calibration": calib,
        "window_24h": w24,
        "window_30d": w30,
        "pace_30d": pace,
        "daily": days,
        "note": note,
    }
    _SUMMARY_CACHE["ts"] = now
    _SUMMARY_CACHE["key"] = cache_key
    _SUMMARY_CACHE["payload"] = payload
    return dict(payload)


def ensure_ready() -> None:
    """Startup hook: backfill once, prune old samples."""
    try:
        if not BACKFILL_MARKER.exists():
            backfill_from_csv()
    except Exception:
        pass
    try:
        prune_samples()
    except Exception:
        pass


if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "summary"
    if cmd == "backfill":
        print(json.dumps(backfill_from_csv(force="--force" in sys.argv), indent=2))
    elif cmd == "prune":
        print(json.dumps({"removed": prune_samples()}, indent=2))
    else:
        print(json.dumps(energy_summary(), indent=2))
