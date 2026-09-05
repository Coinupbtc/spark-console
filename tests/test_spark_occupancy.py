"""TP1 / TP2 occupancy picker — who talks on Telegram."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path.home() / "scripts" / "dgx"))
import spark_occupancy as occ  # noqa: E402


class ChatPickerTests(unittest.TestCase):
    def test_h3_tp2_is_freegle(self) -> None:
        state = {"tp2": "video", "n1": None, "n2": None}
        picked = occ.pick_chat(state)
        self.assertEqual(picked["chat"], "freegle")
        self.assertEqual(picked["wire"], "nous-free")

    def test_music_plus_flash_is_qwen(self) -> None:
        state = {"tp2": None, "n1": "flash1", "n2": "music3"}
        picked = occ.pick_chat(state)
        self.assertEqual(picked["chat"], "flash1")
        self.assertIn("Qwen", picked["chat_label"])

    def test_flash_plus_h3_tp1_is_qwen(self) -> None:
        state = {"tp2": None, "n1": "flash1", "n2": "h3"}
        picked = occ.pick_chat(state)
        self.assertEqual(picked["chat"], "flash1")

    def test_music_plus_video_tp1_is_freegle(self) -> None:
        state = {"tp2": None, "n1": "music3", "n2": "h3"}
        picked = occ.pick_chat(state)
        self.assertEqual(picked["chat"], "freegle")

    def test_ds4f_tp2_is_deepseek(self) -> None:
        state = {"tp2": "ds4f", "n1": None, "n2": None}
        picked = occ.pick_chat(state)
        self.assertEqual(picked["chat"], "ds4f")
        self.assertIn("DeepSeek", picked["chat_label"])


class RecommendTests(unittest.TestCase):
    def test_flash_prefers_n1_when_empty(self) -> None:
        rec = occ.recommend("flash1", occ.empty_state())
        self.assertTrue(rec["ok"])
        self.assertEqual(rec["recommended"], "n1")

    def test_music_prefers_n2_when_empty(self) -> None:
        rec = occ.recommend("music3", occ.empty_state())
        self.assertEqual(rec["recommended"], "n2")

    def test_helper_removed(self) -> None:
        rec = occ.recommend("helper", occ.empty_state())
        self.assertFalse(rec["ok"])

    def test_flash_goes_n2_if_n1_busy(self) -> None:
        state = occ.empty_state()
        state["n1"] = "music3"
        rec = occ.recommend("flash1", state)
        self.assertEqual(rec["recommended"], "n2")

    def test_tp2_live_warns_park(self) -> None:
        state = occ.empty_state()
        state["tp2"] = "video"
        rec = occ.recommend("flash1", state)
        self.assertEqual(rec["parks_tp2"], "video")
        self.assertIn("Parks", rec["reason"])

    def test_replica_pair_flags(self) -> None:
        self.assertTrue(occ.TP1["flash1"]["replica_pair"])
        self.assertTrue(occ.TP1["music3"]["replica_pair"])
        self.assertFalse(occ.TP1["h3"]["replica_pair"])
        rec = occ.recommend("music3", occ.empty_state())
        self.assertTrue(rec["replica_pair"])
        rec_h = occ.recommend("h3", occ.empty_state())
        self.assertFalse(rec_h["replica_pair"])
        cat = occ.catalog()
        self.assertEqual([r["key"] for r in cat["tp2"]], ["ds4f", "flashnext", "glm53keys", "video"])
        self.assertEqual([r["key"] for r in cat["tp1"]], ["flash1", "music3", "h3"])


if __name__ == "__main__":
    unittest.main()
