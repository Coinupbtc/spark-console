"""Deterministic checks for the 72-hour baseline report."""

from __future__ import annotations

import json
import unittest

from baseline_summary import summarize


class BaselineSummaryTests(unittest.TestCase):
    """Protect policy gates, percentiles, availability, and overlap counts."""

    def test_summary_reports_policy_and_overlap_metrics(self) -> None:
        rows = [
            {
                "cpu_pct": "10",
                "gpu_util": "20",
                "mem_avail_gb": "20",
                "swap_used_gb": "9",
                "node2_cpu_pct": "30",
                "node2_gpu_util": "40",
                "node2_mem_avail_gb": "14",
                "node2_swap_used_gb": "1",
                "node1_endpoint_8889_ok": "True",
                "node2_endpoint_8100_ok": "True",
                "node2_reachable": "True",
                "workloads_json": json.dumps(
                    {"node1": ["scan"], "node2": ["llama", "inference-busy"]}
                ),
            },
            {
                "cpu_pct": "50",
                "gpu_util": "60",
                "mem_avail_gb": "30",
                "swap_used_gb": "10",
                "node2_cpu_pct": "70",
                "node2_gpu_util": "80",
                "node2_mem_avail_gb": "25",
                "node2_swap_used_gb": "2",
                "node1_endpoint_8889_ok": "False",
                "node2_endpoint_8100_ok": "True",
                "node2_reachable": "True",
                "workloads_json": json.dumps(
                    {"node1": ["agent-cron"], "node2": ["llama", "inference-busy"]}
                ),
            },
        ]

        report = summarize(rows, 72)

        self.assertEqual(report["sample_count"], 2)
        self.assertEqual(report["utilization"]["node1_cpu_pct"]["p50"], 30.0)
        self.assertEqual(report["memory"]["samples_below_15gb"], 1)
        self.assertEqual(report["memory"]["node1_longest_swap_over_8gb_samples"], 2)
        self.assertEqual(report["endpoint_availability_pct"]["node1_8889"], 50.0)
        self.assertEqual(report["overlap_samples"]["inference+scan"], 1)
        self.assertEqual(report["overlap_samples"]["agent-cron+inference"], 1)


if __name__ == "__main__":
    unittest.main()
