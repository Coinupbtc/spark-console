"""Focused tests for additive dual-node collection and alert gating."""

from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import cluster_metrics
from cluster_metrics import cluster_csv_values, normalize_node2, notify_node2_state
from timeseries_schema import append_row


class ClusterMetricsTests(unittest.TestCase):
    """Protect the additive schema and degraded-node behavior."""

    def test_normalize_unreachable_node2_preserves_degraded_state(self) -> None:
        node = normalize_node2({"reachable": False, "error": "ssh unavailable"})

        self.assertFalse(node["reachable"])
        self.assertEqual(node["error"], "ssh unavailable")
        self.assertEqual(node["endpoints"], [])
        self.assertEqual(node["workloads"], [])

    def test_cluster_csv_values_are_additive_and_role_scoped(self) -> None:
        cluster = {
            "node1": {
                "endpoints": [{"port": 8889, "status": "ok", "models": ["qwen"]}],
                "workloads": ["pokemon"],
            },
            "node2": {
                "reachable": True,
                "cpu_pct": 12.5,
                "mem": {"avail_gb": 70.0},
                "swap": {"used_gb": 1.0, "pct": 2.0},
                "gpus": [{"util_gpu": 44.0, "power_w": 65.0}],
                "models": [{"port": 8100, "id": "mimo"}],
                "endpoints": [{"port": 8100, "status": "ok"}],
                "workloads": ["llama"],
            },
        }

        values = cluster_csv_values(cluster)

        self.assertEqual(values["schema_version"], 2)
        self.assertEqual(values["node1_active_model"], "qwen")
        self.assertEqual(values["node2_active_model"], "mimo")
        self.assertTrue(values["node2_endpoint_8100_ok"])
        self.assertIn('"pokemon"', values["workloads_json"])

    def test_node2_alert_pages_once_and_rearms_after_recovery(self) -> None:
        calls = []

        def fake_run(argv, **_kwargs):
            calls.append(argv)
            return SimpleNamespace(returncode=0, stdout="sent", stderr="")

        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "node2-alerted"
            degraded = {"reachable": False, "error": "forced test failure"}
            with patch.object(cluster_metrics.subprocess, "run", side_effect=fake_run):
                self.assertEqual(notify_node2_state(degraded, state), "alerted")
                self.assertEqual(notify_node2_state(degraded, state), "already-alerted")
                self.assertEqual(len(calls), 1)
            self.assertEqual(notify_node2_state({"reachable": True}, state), "healthy")
            self.assertFalse(state.exists())

    def test_csv_header_migration_preserves_legacy_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "metrics.csv"
            csv_path.write_text("timestamp_utc,cpu_pct\nold,5\n")

            append_row(
                str(csv_path),
                ["timestamp_utc", "cpu_pct", "schema_version"],
                {"timestamp_utc": "new", "cpu_pct": 7, "schema_version": 2},
            )

            with csv_path.open(newline="") as source:
                rows = list(csv.DictReader(source))
        self.assertEqual(
            rows,
            [
                {"timestamp_utc": "old", "cpu_pct": "5", "schema_version": ""},
                {"timestamp_utc": "new", "cpu_pct": "7", "schema_version": "2"},
            ],
        )


if __name__ == "__main__":
    unittest.main()
