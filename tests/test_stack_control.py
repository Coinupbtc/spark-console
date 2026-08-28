"""Unit tests for Spark stack detect/classify (no live switch)."""
from __future__ import annotations

import unittest
from unittest.mock import patch

import stack_control


class ClassifyTests(unittest.TestCase):
    def test_prime_when_ds4f_up(self) -> None:
        self.assertEqual(
            stack_control.classify({"ds4f": True, "helper": False, "h3": False, "music": False}),
            "prime",
        )

    def test_dream_when_ds4f_and_qwen_gguf(self) -> None:
        self.assertEqual(
            stack_control.classify(
                {"ds4f": True, "dream": True, "helper": False, "h3": False, "music": False}
            ),
            "dream",
        )

    def test_video_when_h3_up(self) -> None:
        self.assertEqual(
            stack_control.classify({"ds4f": False, "helper": False, "h3": True, "music": False}),
            "video",
        )

    def test_music_needs_helper_and_music3(self) -> None:
        self.assertEqual(
            stack_control.classify({"ds4f": False, "helper": True, "h3": False, "music": True}),
            "music",
        )
        self.assertEqual(
            stack_control.classify({"ds4f": False, "helper": True, "h3": False, "music": False}),
            "none",
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

    def test_twins_when_gguf_both_8888(self) -> None:
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
            "twins",
        )

    def test_flashnext_before_twins(self) -> None:
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
            ["prime", "dream", "twins", "flashnext", "glm53keys", "video", "music"],
        )
        for meta in stack_control.PRESETS.values():
            for field in ("label", "short", "detail", "eta", "stops", "starts"):
                self.assertTrue(meta.get(field), f"missing {field}")
        dream = stack_control.PRESETS["dream"]
        self.assertIn("116k", dream["short"])
        self.assertIn("95k", dream["short"])
        self.assertIn("20k", dream["detail"])
        self.assertIn("baton", dream["detail"])
        self.assertIn("sglang", stack_control.PRESETS["twins"]["starts"])
        self.assertIn("qwen38-flash-next", stack_control.PRESETS["flashnext"]["starts"])
        self.assertEqual(stack_control.PRESETS["flashnext"]["label"], "Qwen3.8-Flash")
        self.assertEqual(stack_control.PRESETS["glm53keys"]["label"], "GLM-5.3")
        self.assertIn("glm-5.3-flash-nvfp4", stack_control.PRESETS["glm53keys"]["starts"])

    def test_unknown_key_refused(self) -> None:
        result = stack_control.switch_stack("nemotron")
        self.assertFalse(result["ok"])
        self.assertIn("Unknown", result["error"])


class DetectTests(unittest.TestCase):
    def test_detect_uses_probes(self) -> None:
        probes = {
            "ds4f": False, "helper": True, "h3": False,
            "music": True, "vision": False,
        }
        with patch.object(stack_control, "_probes_now", return_value=probes), \
             patch.object(stack_control, "active_operation", return_value=None), \
             patch.object(stack_control, "external_switch_busy", return_value=None), \
             patch.object(stack_control, "_read_saved_state", return_value={}):
            stack_control._detect_cache = None
            out = stack_control.detect_stack(force=True)
        self.assertEqual(out["detected"], "music")
        music = next(p for p in out["presets"] if p["key"] == "music")
        self.assertTrue(music["active"])
        self.assertTrue(music["can_switch"])  # re-apply / refresh
        prime = next(p for p in out["presets"] if p["key"] == "prime")
        self.assertTrue(prime["can_switch"])

    def test_external_lock_disables_chips(self) -> None:
        probes = {
            "ds4f": False, "helper": False, "h3": False,
            "music": False, "vision": False,
        }
        ext = {"busy": True, "desired": "dream", "message": "switching to Dream"}
        with patch.object(stack_control, "_probes_now", return_value=probes), \
             patch.object(stack_control, "active_operation", return_value=None), \
             patch.object(stack_control, "external_switch_busy", return_value=ext), \
             patch.object(stack_control, "_read_saved_state", return_value={"desired": "dream"}):
            stack_control._detect_cache = None
            out = stack_control.detect_stack(force=True)
        self.assertEqual(out["phase"], "switching")
        self.assertFalse(any(p["can_switch"] for p in out["presets"]))

    def test_switch_refuses_when_lock_held(self) -> None:
        ext = {"busy": True, "message": "switching to Dream"}
        with patch.object(stack_control, "active_operation", return_value=None), \
             patch.object(stack_control, "external_switch_busy", return_value=ext):
            result = stack_control.switch_stack("music")
        self.assertFalse(result["ok"])
        self.assertIn("Dream", result["error"])


if __name__ == "__main__":
    unittest.main()
