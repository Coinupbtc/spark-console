"""Tests for historical energy integration (energy_cost)."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import energy_cost


class EnergyCostTests(unittest.TestCase):
    def test_trap_kwh_average(self) -> None:
        # 100 W for 1 hour = 0.1 kWh
        k = energy_cost._trap_kwh(0, 100, 3600, 100)
        self.assertAlmostEqual(k, 0.1, places=6)

    def test_gpu_to_wall_idle_and_load(self) -> None:
        # At GPU floor: just idle wall
        self.assertAlmostEqual(energy_cost.gpu_to_wall(8, idle_wall_w=50, gpu_floor_w=8, slope=1.15), 50.0)
        # 20 W GPU → 50 + 1.15*(20-8) = 63.8
        self.assertAlmostEqual(energy_cost.gpu_to_wall(20, idle_wall_w=50, gpu_floor_w=8, slope=1.15), 63.8)
        self.assertIsNone(energy_cost.gpu_to_wall(None))

    def test_billable_integrate_higher_than_sensor(self) -> None:
        samples = [
            {"t": 0, "w": {"node1": 18, "pi": 2}},
            {"t": 3600, "w": {"node1": 18, "pi": 2}},
        ]
        calib = {"idle_wall_w": 50, "gpu_floor_w": 8, "slope": 1.15}
        bill = energy_cost._integrate(samples, 0, 3600, calib, mode="billable")
        sens = energy_cost._integrate(samples, 0, 3600, calib, mode="sensor")
        # wall ≈ 50 + 1.15*10 = 61.5 W → 0.0615 kWh vs GPU 0.018
        self.assertGreater(bill["kwh"]["node1"], sens["kwh"]["node1"] * 2)
        self.assertAlmostEqual(bill["kwh"]["pi"], sens["kwh"]["pi"], places=5)

    def test_integrate_window(self) -> None:
        samples = [
            {"t": 0, "w": {"node1": 100, "node2": 50}},
            {"t": 3600, "w": {"node1": 100, "node2": 50}},
            {"t": 7200, "w": {"node1": 100}},
        ]
        out = energy_cost._integrate(samples, 0, 7200, mode="sensor")
        self.assertAlmostEqual(out["kwh"]["node1"], 0.2, places=5)
        # node2: 50W for hour1, then held at 50W for hour2 (one-sided) → 0.1 kWh
        self.assertAlmostEqual(out["kwh"]["node2"], 0.1, places=5)

    def test_pace_from_24h(self) -> None:
        w24 = {
            "fleet_kwh": 2.4, "fleet_hours": 24.0,
            "sparks_kwh": 2.0, "sparks_hours": 24.0,
            "nodes": {
                "node1": {"kwh": 1.0, "hours_covered": 24.0},
                "pi": {"kwh": 0.4, "hours_covered": 24.0},
            },
        }
        pace = energy_cost._pace_from_24h(w24, days=30.0)
        # 2.4 kWh/day * 30 = 72
        self.assertAlmostEqual(pace["fleet_kwh"], 72.0, places=4)
        self.assertAlmostEqual(pace["sparks_kwh"], 60.0, places=4)
        self.assertAlmostEqual(pace["nodes"]["node1"], 30.0, places=4)

    def test_record_and_summary(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            with patch.object(energy_cost, "DATA_DIR", root), \
                 patch.object(energy_cost, "SAMPLES_PATH", root / "energy_samples.jsonl"), \
                 patch.object(energy_cost, "DAILY_PATH", root / "energy_daily.json"), \
                 patch.object(energy_cost, "BACKFILL_MARKER", root / "energy_backfill.done"), \
                 patch.object(energy_cost, "_last_sample_t", 0.0), \
                 patch.object(energy_cost, "_SUMMARY_CACHE", {"ts": 0.0, "key": "", "payload": None}):
                # Pretend backfill already done so summary doesn't try CSV.
                energy_cost.BACKFILL_MARKER.write_text("{}\n")
                t0 = 1_700_000_000.0
                energy_cost.record_sample({"node1": 200, "pi": 2}, t=t0, force=True)
                energy_cost.record_sample({"node1": 200, "pi": 2}, t=t0 + 3600, force=True)
                # summary uses time.time() windows — inject samples spanning "now"
                now = t0 + 3600
                with patch("energy_cost.time.time", return_value=now):
                    # Re-read: samples are absolute; window is now-86400..now
                    s = energy_cost.energy_summary(mode="sensor")
                    w = energy_cost.energy_summary(mode="wall", idle_wall_w=50)
                    dflt = energy_cost.energy_summary()
                self.assertTrue(s["ok"])
                self.assertEqual(s["mode"], "sensor")
                self.assertEqual(dflt["mode"], "wall")  # bill-like default
                self.assertGreater(s["window_24h"]["nodes"]["node1"]["kwh"], 0)
                self.assertGreater(s["window_24h"]["nodes"]["pi"]["kwh"], 0)
                # Wall estimate for Sparks must exceed measured GPU kWh
                self.assertGreater(
                    w["window_24h"]["nodes"]["node1"]["kwh"],
                    s["window_24h"]["nodes"]["node1"]["kwh"],
                )
                self.assertIn("pace_30d", w)
                self.assertIsNotNone(w["pace_30d"].get("fleet_kwh"))
                daily = json.loads((root / "energy_daily.json").read_text())
                self.assertTrue(any(v.get("node1", 0) > 0 for v in daily.values()))


if __name__ == "__main__":
    unittest.main()
