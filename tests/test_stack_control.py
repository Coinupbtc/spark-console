"""Unit tests for Spark stack detect/classify (no live switch)."""
from __future__ import annotations

import unittest
from unittest.mock import patch

import stack_control


class ClassifyTests(unittest.TestCase):
    def test_ds4f_when_deepseek_up(self) -> None:
        self.assertEqual(
            stack_control.classify({"ds4f": True, "helper": False, "h3": False, "music": False}),
            "ds4f",
        )

    def test_dream_leftover_is_mixed(self) -> None:
        self.assertEqual(
            stack_control.classify(
                {"ds4f": True, "dream": True, "helper": False, "h3": False, "music": False}
            ),
            "mixed",
        )

    def test_video_when_h3_up(self) -> None:
        self.assertEqual(
            stack_control.classify({"ds4f": False, "helper": False, "h3": True, "music": False}),
            "video",
        )

    def test_music_alone_is_not_a_tp2_chip(self) -> None:
        self.assertEqual(
            stack_control.classify({"ds4f": False, "helper": False, "h3": False, "music": True}),
            "none",
        )
        self.assertEqual(
            stack_control.classify({"ds4f": False, "helper": True, "h3": False, "music": True}),
            "mixed",
        )

    def test_retired_qwen38_nvfp4_is_mixed(self) -> None:
        self.assertEqual(
            stack_control.classify(
                {"qwen38": True, "ds4f": False, "helper": False, "h3": False, "music": False}
            ),
            "mixed",
        )

    def test_mixed_ds4f_and_h3(self) -> None:
        self.assertEqual(
            stack_control.classify({"ds4f": True, "helper": False, "h3": True, "music": False}),
            "mixed",
        )

    def test_twins_leftover_is_mixed(self) -> None:
        self.assertEqual(
            stack_control.classify(
                {
                    "ds4f": False,
                    "twins": True,
                    "dream": False,
                    "helper": False,
                    "h3": False,
                    "music": False,
                }
            ),
            "mixed",
        )

    def test_flashnext_before_ds4f(self) -> None:
        self.assertEqual(
            stack_control.classify(
                {
                    "ds4f": False,
                    "flashnext": True,
                    "twins": False,
                    "dream": False,
                    "helper": False,
                    "h3": False,
                    "music": False,
                }
            ),
            "flashnext",
        )

    def test_glm53keys_before_flashnext(self) -> None:
        self.assertEqual(
            stack_control.classify(
                {
                    "ds4f": False,
                    "glm53keys": True,
                    "flashnext": False,
                    "twins": False,
                    "dream": False,
                    "helper": False,
                    "h3": False,
                    "music": False,
                }
            ),
            "glm53keys",
        )

    def test_none(self) -> None:
        self.assertEqual(
            stack_control.classify({"ds4f": False, "helper": False, "h3": False, "music": False}),
            "none",
        )


class PresetTests(unittest.TestCase):
    def test_named_setups(self) -> None:
        self.assertEqual(
            list(stack_control.PRESETS),
            ["ds4f", "flashnext", "glm53keys", "video"],
        )
        for meta in stack_control.PRESETS.values():
            for field in ("label", "short", "detail", "eta", "stops", "starts"):
                self.assertTrue(meta.get(field), f"missing {field}")
        self.assertEqual(stack_control.PRESETS["ds4f"]["label"], "DeepSeek Vision")
        self.assertIn("image_url", stack_control.PRESETS["ds4f"]["detail"])
        self.assertIn("qwen38-flash-next", stack_control.PRESETS["flashnext"]["starts"])
        self.assertEqual(stack_control.PRESETS["flashnext"]["label"], "Qwen3.8-Flash")
        self.assertEqual(stack_control.PRESETS["glm53keys"]["label"], "GLM-5.3")
        self.assertIn("GLM-5.3-Flash-EXL3", stack_control.PRESETS["glm53keys"]["starts"])

    def test_both_refused_for_h3(self) -> None:
        result = stack_control.occupy_tp1("h3", "both")
        self.assertFalse(result["ok"])
        self.assertIn("two copies", result["error"].lower() + result.get("error", ""))
        result = stack_control.switch_stack("nemotron")
        self.assertFalse(result["ok"])
        self.assertIn("Unknown", result["error"])


class DetectTests(unittest.TestCase):
    def test_detect_uses_probes(self) -> None:
        probes = {
            "ds4f": False, "helper": False, "h3": False,
            "music": False, "vision": False,
        }
        with patch.object(stack_control, "_probes_now", return_value=probes), \
             patch.object(stack_control, "active_operation", return_value=None), \
             patch.object(stack_control, "external_switch_busy", return_value=None), \
             patch.object(stack_control, "_read_saved_state", return_value={}), \
             patch.object(stack_control, "occupancy_view", return_value={
                 "tp2": None, "n1": None, "n2": None, "chat": "freegle",
                 "chat_label": "Freegle (Nous :free)", "tp1": [],
             }):
            stack_control._detect_cache = None
            out = stack_control.detect_stack(force=True)
        self.assertEqual(out["detected"], "none")
        self.assertTrue(any(p["key"] == "video" for p in out["presets"]))
        ds4f = next(p for p in out["presets"] if p["key"] == "ds4f")
        self.assertTrue(ds4f["can_switch"])

    def test_external_lock_disables_chips(self) -> None:
        probes = {
            "ds4f": False, "helper": False, "h3": False,
            "music": False, "vision": False,
        }
        ext = {"busy": True, "desired": "ds4f", "message": "switching to DeepSeek Vision"}
        with patch.object(stack_control, "_probes_now", return_value=probes), \
             patch.object(stack_control, "active_operation", return_value=None), \
             patch.object(stack_control, "external_switch_busy", return_value=ext), \
             patch.object(stack_control, "_read_saved_state", return_value={"desired": "ds4f"}), \
             patch.object(stack_control, "occupancy_view", return_value={
                 "tp2": None, "n1": None, "n2": None, "chat": "freegle",
                 "chat_label": "Freegle (Nous :free)", "tp1": [],
             }):
            stack_control._detect_cache = None
            out = stack_control.detect_stack(force=True)
        self.assertEqual(out["phase"], "switching")
        self.assertFalse(any(p["can_switch"] for p in out["presets"]))

    def test_switch_refuses_when_lock_held(self) -> None:
        ext = {"busy": True, "message": "switching to DeepSeek Vision"}
        with patch.object(stack_control, "active_operation", return_value=None), \
             patch.object(stack_control, "external_switch_busy", return_value=ext):
            result = stack_control.switch_stack("video")
        self.assertFalse(result["ok"])
        self.assertIn("DeepSeek Vision", result["error"])


if __name__ == "__main__":
    unittest.main()
