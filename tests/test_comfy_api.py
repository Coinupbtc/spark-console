"""Tests for the ComfyUI job-monitor module (comfy_api).

Covers the states the Spark Console card must render: offline (ComfyUI down,
the common case — it is on-demand), idle, running, and queued, plus the
versioned /history duration parsing seen on ComfyUI 0.27 (timestamps live in
status.messages, not status.start/end).
"""

from __future__ import annotations

import unittest
from unittest.mock import patch

import comfy_api
from comfy_api import _build_snapshot, _parse_history, _pick_running_job, _vram_pct


def _running_item(prompt_id="abc123", steps=30, ckpt="sd_xl_base_1.0.safetensors"):
    return [7, prompt_id, {
        "3": {"class_type": "KSampler", "inputs": {
            "cfg": 2.0, "steps": steps, "model": ["4", 0], "seed": 3}},
        "4": {"class_type": "CheckpointLoaderSimple", "inputs": {"ckpt_name": ckpt}},
    }, {"create_time": 1000}, ["9"]]


class ComfyApiTests(unittest.TestCase):
    def test_offline_state_when_comfy_down(self) -> None:
        with patch("comfy_api._get_json", side_effect=ConnectionRefusedError()):
            out = comfy_api.query_comfy()
        self.assertFalse(out["ok"])
        self.assertTrue(out["offline"])
        self.assertEqual(out["state"], "offline")
        self.assertEqual(out["chip"], "offline")
        self.assertEqual(out["queue_total"], 0)
        self.assertIs(out["running"], None)
        # clean, JSON-serialisable, no exception leak
        self.assertIsInstance(out["error"], str)

    def test_history_duration_from_messages_on_027(self) -> None:
        raw = {
            "pidX": {
                "prompt": _running_item("pidX", steps=4),
                "outputs": {"9": {}},
                "status": {"status_str": "success", "completed": True,
                           "messages": [
                               ["execution_start", {"timestamp": 1000}],
                               ["execution_success", {"timestamp": 51000}],
                           ]},
            }
        }
        hist = _parse_history(raw)["last_finished"]
        self.assertEqual(len(hist), 1)
        j = hist[0]
        self.assertEqual(j["status"], "success")
        self.assertTrue(j["finished"])
        self.assertEqual(j["duration_ms"], 50000)
        self.assertEqual(j["duration_s"], 50.0)
        self.assertEqual(j["label"], "sd_xl_base_1.0.safetensors")

    def test_history_duration_from_legacy_start_end(self) -> None:
        raw = {"pidY": {"prompt": _running_item("pidY"), "outputs": {},
                         "status": {"status_str": "success", "start": 2000, "end": 12000}}}
        hist = _parse_history(raw)["last_finished"]
        self.assertEqual(hist[0]["duration_ms"], 10000)
        self.assertEqual(hist[0]["duration_s"], 10.0)

    def test_running_snapshot_state_and_fields(self) -> None:
        sysraw = {"system": {"comfyui_version": "0.27.0"},
                  "devices": [{"name": "cuda:0 GB10", "vram_total": 130 * 1024**3,
                                "vram_free": 10 * 1024**3}]}
        queueraw = {"queue_running": [_running_item("run1", steps=50)],
                    "queue_pending": [_running_item("pend1", steps=25)]}
        raw = _build_snapshot(sysraw, queueraw, {}, 0)
        self.assertEqual(raw["state"], "running")
        self.assertEqual(raw["chip"], "run")
        self.assertEqual(raw["queue_total"], 2)
        self.assertEqual(raw["pending_count"], 1)
        self.assertEqual(raw["running_steps"], 50)
        self.assertEqual(raw["running"]["prompt_id"], "run1")
        self.assertEqual(raw["version"], "0.27.0")

    def test_queued_state_when_only_pending(self) -> None:
        sysraw = {"system": {"comfyui_version": "0.27.0"}, "devices": []}
        queueraw = {"queue_running": [], "queue_pending": [_running_item("q1")]}
        raw = _build_snapshot(sysraw, queueraw, {}, 0)
        self.assertEqual(raw["state"], "queued")
        self.assertEqual(raw["chip"], "1q")
        self.assertEqual(raw["queue_total"], 1)

    def test_vram_pct_from_string_gb(self) -> None:
        pct = _vram_pct({"vram_total": "121.7G", "vram_free": "1.0G"})
        self.assertIsNotNone(pct)
        p = float(pct or 0)
        self.assertGreater(p, 95)
        self.assertLessEqual(p, 100)
        self.assertIsNone(_vram_pct({"vram_total": "", "vram_free": ""}))

    def test_pick_running_job_defensive_types(self) -> None:
        # must not throw on malformed queue entries (SPL: type-validate before ops)
        self.assertEqual(_pick_running_job(None)["label"], "job")
        self.assertEqual(_pick_running_job({})["label"], "job")
        self.assertEqual(_pick_running_job("junk")["label"], "job")
        self.assertEqual(_pick_running_job([1, 2, {"1": {"class_type": "X",
                                                        "inputs": {"text": "hello world"}}}])["label"], "hello world")

    def test_isolated_bad_history_does_not_crash_snapshot(self) -> None:
        # one malformed history entry must not break the whole snapshot
        histraw = {"bad": "not-a-dict", "also": None}
        sysraw = {"system": {"comfyui_version": "0.27.0"}, "devices": []}
        queueraw = {"queue_running": [], "queue_pending": []}
        raw = _build_snapshot(sysraw, queueraw, histraw, 0)
        self.assertEqual(raw["last_finished"], [])
        self.assertTrue(raw["ok"])


if __name__ == "__main__":
    unittest.main()
