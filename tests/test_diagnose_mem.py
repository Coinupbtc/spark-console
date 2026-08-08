"""Needs-you memory alerts must not page expected model loads."""
from __future__ import annotations

import unittest

from collector import diagnose, resident_engine


def _sys(**over):
    base = {
        "mem_pct": 93.5, "mem_avail_gb": 7.8,
        "swap_used_gb": 11.5, "swap_pct": 71.5,
        "disk_used_pct": 50.0, "disk_free_tb": 1.0,
    }
    base.update(over)
    return base


class DiagnoseMemTests(unittest.TestCase):
    def test_gpu_worker_explains_ram_and_silences_swap(self):
        procs = [{"name": "ray::RayDiffusionWorker", "mem_mb": 103799}]
        eng = resident_engine([], procs)
        self.assertIsNotNone(eng)
        alerts = diagnose(_sys(), [], [], procs, None, [])
        kinds = {(a["category"], a["level"]) for a in alerts}
        self.assertNotIn(("memory", "critical"), kinds)
        self.assertNotIn(("swap", "warning"), kinds)

    def test_unexplained_high_ram_still_critical(self):
        alerts = diagnose(_sys(), [], [], [], None, [])
        mem = [a for a in alerts if a["category"] == "memory"]
        self.assertTrue(mem)
        self.assertEqual(mem[0]["level"], "critical")
        self.assertIn("no inference engine", mem[0]["message"])

    def test_endpoint_ok_explains_ram(self):
        eps = [{"status": "ok", "engine": "vLLM-fabric", "port": 8800, "id": "MiniMax"}]
        alerts = diagnose(_sys(mem_pct=93.0), [], [], [], None, eps)
        self.assertFalse(any(a["category"] == "memory" for a in alerts))
        self.assertFalse(any(a["category"] == "swap" for a in alerts))

    def test_remote_desktop_stays_info(self):
        procs = [
            {"name": "ray::RayDiffusionWorker", "mem_mb": 103799},
            {"name": "/usr/libexec/gnome-remote-desktop-daemon", "mem_mb": 279},
        ]
        alerts = diagnose(_sys(), [], [], procs, None, [])
        info = [a for a in alerts if a["level"] == "info"]
        self.assertTrue(any("gnome-remote-desktop" in a["message"] for a in info))


if __name__ == "__main__":
    unittest.main()
