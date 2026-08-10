"""Tests for token_usage mean/median/mode + 14-day series."""

from __future__ import annotations

import unittest
from unittest.mock import patch

import token_usage
from token_usage import _build_stats, _build_series, _mode_bucketed, _median, _mean


class TokenUsageStatsTests(unittest.TestCase):
    def test_mean_median(self) -> None:
        vals = [10.0, 20.0, 30.0, 40.0, 100.0]
        self.assertEqual(_mean(vals), 40.0)
        self.assertEqual(_median(vals), 30.0)
        self.assertIsNone(_mean([]))
        self.assertIsNone(_median([]))

    def test_mode_bucketed_picks_most_frequent_center(self) -> None:
        # 12k, 13k, 48k → buckets of 10k → 10k, 10k, 50k → mode 10k ×2
        out = _mode_bucketed([12000, 13000, 48000], 10000)
        self.assertEqual(out["value"], 10000)
        self.assertEqual(out["count"], 2)
        self.assertEqual(out["bucket"], 10000)

    def test_build_stats_shape(self) -> None:
        rows = [
            (0.0, 50_000, 5),
            (0.0, 55_000, 5),
            (0.0, 200_000, 10),
            (0.0, 0, 0),  # skipped (empty)
        ]
        st = _build_stats(rows)
        self.assertEqual(st["unit"], "tokens (input+output)")
        self.assertEqual(st["per_session"]["n"], 3)
        self.assertIsNotNone(st["per_session"]["mean"])
        self.assertIsNotNone(st["per_session"]["median"])
        self.assertIsNotNone(st["per_session"]["mode"])
        self.assertEqual(st["per_call"]["n"], 3)

    def test_series_fills_14_days(self) -> None:
        with patch("token_usage._db_list", return_value=[]):
            series = _build_series(14)
        self.assertEqual(len(series), 14)
        self.assertTrue(all("day" in d and "input_tokens" in d for d in series))
        # continuous ascending days
        days = [d["day"] for d in series]
        self.assertEqual(days, sorted(days))


if __name__ == "__main__":
    unittest.main()
