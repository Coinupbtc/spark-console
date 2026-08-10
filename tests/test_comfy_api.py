"""Tests for the ComfyUI job-monitor module (comfy_api).

Covers the states the Spark Console card must render: offline (ComfyUI down,
the common case — it is on-demand), idle, running, and queued, plus the
versioned /history duration parsing seen on ComfyUI 0.27 (timestamps live in
status.messages, not status.start/end).

Also covers sparkDash 1.6-shaped fields: footprint (res · steps · sampler ·
nodes), model/LoRA list from the graph, queue ETA, and cancel_job.
"""

from __future__ import annotations

import unittest
from unittest.mock import patch

import comfy_api
from comfy_api import (
    _build_snapshot,
    _estimate_queue_eta_ms,
    _footprint_str,
    _parse_history,
    _pick_running_job,
    _summarize_prompt,
    _vram_pct,
)


def _running_item(
    prompt_id="abc123",
    steps=30,
    ckpt="sd_xl_base_1.0.safetensors",
    lora="style_lora.safetensors",
    width=1024,
    height=1024,
    sampler="euler",
    title=None,
):
    extra = {"create_time": 1_700_000_000}
    if title:
        extra["extra_pnginfo"] = {"workflow": {"title": title}}
    return [7, prompt_id, {
        "3": {"class_type": "KSampler", "inputs": {
            "cfg": 2.0, "steps": steps, "sampler_name": sampler,
            "model": ["4", 0], "seed": 3}},
        "4": {"class_type": "CheckpointLoaderSimple", "inputs": {"ckpt_name": ckpt}},
        "5": {"class_type": "EmptyLatentImage", "inputs": {
            "width": width, "height": height, "batch_size": 1}},
        "6": {"class_type": "LoraLoader", "inputs": {
            "lora_name": lora, "strength_model": 0.8}},
    }, extra, ["9"]]


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
        # label prefers first model filename when no workflow title
        self.assertIn("sd_xl_base_1.0.safetensors", j["label"])

    def test_history_duration_from_legacy_start_end(self) -> None:
        raw = {"pidY": {"prompt": _running_item("pidY"), "outputs": {},
                         "status": {"status_str": "success", "start": 2000, "end": 12000}}}
        hist = _parse_history(raw)["last_finished"]
        self.assertEqual(hist[0]["duration_ms"], 10000)
        self.assertEqual(hist[0]["duration_s"], 10.0)

    def test_running_snapshot_state_and_fields(self) -> None:
        sysraw = {"system": {"comfyui_version": "0.27.0", "pytorch_version": "2.13.0"},
                  "devices": [{"name": "cuda:0 GB10", "type": "cuda",
                               "vram_total": 130 * 1024**3,
                               "vram_free": 10 * 1024**3}]}
        queueraw = {"queue_running": [_running_item("run1", steps=50, title="Portrait run")],
                    "queue_pending": [_running_item("pend1", steps=25, title="Upscale pass")]}
        with patch("comfy_api._fetch_models_installed", return_value={
            "checkpoints": ["sd_xl_base_1.0.safetensors"],
            "loras": ["style_lora.safetensors"],
        }):
            raw = _build_snapshot(sysraw, queueraw, {}, 0)
        self.assertEqual(raw["state"], "running")
        self.assertEqual(raw["chip"], "run")
        self.assertEqual(raw["queue_total"], 2)
        self.assertEqual(raw["pending_count"], 1)
        self.assertEqual(raw["running_steps"], 50)
        self.assertEqual(raw["running"]["prompt_id"], "run1")
        self.assertEqual(raw["running"]["title"], "Portrait run")
        self.assertEqual(raw["version"], "0.27.0")
        # footprint: res · steps · sampler · nodes
        fp = raw["running"]["footprint"]
        self.assertIn("1024×1024", fp)
        self.assertIn("50 steps", fp)
        self.assertIn("euler", fp)
        self.assertIn("nodes", fp)
        # models include checkpoint AND lora
        self.assertIn("sd_xl_base_1.0.safetensors", raw["running"]["models"])
        self.assertIn("style_lora.safetensors", raw["running"]["models"])
        # pending job preserved with title
        self.assertEqual(raw["pending_jobs"][0]["title"], "Upscale pass")
        # No finished-job history → no invented queue ETA
        self.assertIsNone(raw["queue_eta_ms"])
        self.assertIsNotNone(raw["progress"])
        # No invented completion % — elapsed only
        self.assertIsNone(raw["progress"].get("percent"))
        self.assertEqual(raw["progress"].get("source"), "elapsed")
        self.assertEqual(raw["models_installed"]["loras"][0], "style_lora.safetensors")

    def test_queue_eta_from_finished_history(self) -> None:
        sysraw = {"system": {}, "devices": []}
        queueraw = {"queue_running": [_running_item("run1")], "queue_pending": []}
        histraw = {"done1": {
            "prompt": _running_item("done1"), "outputs": {},
            "status": {"status_str": "success",
                       "messages": [["execution_start", {"timestamp": 0}],
                                    ["execution_success", {"timestamp": 30_000}]]},
        }}
        with patch("comfy_api._fetch_models_installed", return_value={"checkpoints": [], "loras": []}):
            raw = _build_snapshot(sysraw, queueraw, histraw, 0)
        self.assertEqual(raw["queue_eta_source"], "finished_avg")
        self.assertEqual(raw["queue_eta_ms"], 15_000)  # 0.5 × 30s while running, 0 pending


    def test_summarize_prompt_footprint(self) -> None:
        item = _running_item(steps=28, width=768, height=512, sampler="dpmpp_2m")
        summary = _summarize_prompt(item[2])
        self.assertEqual(summary["steps"], 28)
        self.assertEqual(summary["width"], 768)
        self.assertEqual(summary["height"], 512)
        self.assertEqual(summary["sampler"], "dpmpp_2m")
        self.assertGreaterEqual(summary["node_count"], 4)
        self.assertEqual(len(summary["models"]), 2)
        self.assertIn("1024×1024", _footprint_str(
            {"width": 1024, "height": 1024, "steps": 28, "sampler": "euler",
             "node_count": 12, "batch_size": 1}))

    def test_queue_eta_half_avg_while_running(self) -> None:
        # Running contributes 0.5×avg + 1 pending × avg when we have a real avg
        eta = _estimate_queue_eta_ms(60_000, pending=1, running=True, progress_pct=50)
        self.assertEqual(eta, 90_000)

    def test_queue_eta_none_without_avg(self) -> None:
        self.assertIsNone(_estimate_queue_eta_ms(None, pending=1, running=True, progress_pct=None))

    def test_queued_state_when_only_pending(self) -> None:
        sysraw = {"system": {"comfyui_version": "0.27.0"}, "devices": []}
        queueraw = {"queue_running": [], "queue_pending": [_running_item("q1")]}
        with patch("comfy_api._fetch_models_installed", return_value={
            "checkpoints": [], "loras": [],
        }):
            raw = _build_snapshot(sysraw, queueraw, {}, 0)
        self.assertEqual(raw["state"], "queued")
        self.assertEqual(raw["chip"], "1q")
        self.assertEqual(raw["queue_total"], 1)
        self.assertIsNone(raw["models_installed"])  # both empty → hidden

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
        with patch("comfy_api._fetch_models_installed", return_value={
            "checkpoints": [], "loras": [],
        }):
            raw = _build_snapshot(sysraw, queueraw, histraw, 0)
        self.assertEqual(raw["last_finished"], [])
        self.assertTrue(raw["ok"])

    def test_cancel_job_requires_id(self) -> None:
        out = comfy_api.cancel_job("")
        self.assertFalse(out["ok"])
        self.assertEqual(out["method"], "none")

    def test_cancel_job_queue_delete_fallback(self) -> None:
        # modern cancel 404s → interrupt + queue delete succeeds
        calls = []

        def fake_post(path, body, timeout=2):
            calls.append((path, body))
            if path.startswith("/api/jobs/"):
                return False, 404, "not found"
            if path == "/interrupt":
                return True, 200, "{}"
            if path == "/queue":
                return True, 200, "{}"
            return False, 0, "nope"

        with patch("comfy_api._post_json", side_effect=fake_post):
            out = comfy_api.cancel_job("pid-42")
        self.assertTrue(out["ok"])
        self.assertEqual(out["method"], "queue_delete")
        self.assertTrue(any(p == "/queue" for p, _ in calls))

    def test_cancel_job_unreachable_comfy(self) -> None:
        with patch("comfy_api._post_json", return_value=(False, 0, "Cannot reach ComfyUI")):
            out = comfy_api.cancel_job("pid-42")
        self.assertFalse(out["ok"])
        self.assertIn("ComfyUI", out["message"])


if __name__ == "__main__":
    unittest.main()
