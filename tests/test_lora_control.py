"""Unit tests for LoRA Train occupancy gates (no live GPU train)."""
from __future__ import annotations

import unittest
from unittest.mock import patch

import lora_control as lc


class CanStartTests(unittest.TestCase):
    def test_0731_up_without_confirm_blocked(self) -> None:
        ok, reason = lc.can_start(
            stop_0731=False, ds4f=True, train_n=931, admit=True,
            busy=False, lock=False,
        )
        self.assertFalse(ok)
        self.assertIn("LIVE", reason)

    def test_0731_up_with_confirm_allowed(self) -> None:
        ok, reason = lc.can_start(
            stop_0731=True, ds4f=True, train_n=931, admit=True,
            busy=False, lock=False,
        )
        self.assertTrue(ok)
        self.assertIn("stop", reason.lower())

    def test_window_open(self) -> None:
        ok, _ = lc.can_start(
            stop_0731=False, ds4f=False, train_n=931, admit=True,
            busy=False, lock=False,
        )
        self.assertTrue(ok)

    def test_gold_too_small(self) -> None:
        ok, reason = lc.can_start(
            stop_0731=False, ds4f=False, train_n=24, admit=True,
            busy=False, lock=False,
        )
        self.assertFalse(ok)
        self.assertIn("gold", reason.lower())

    def test_busy_blocks(self) -> None:
        ok, reason = lc.can_start(
            stop_0731=True, ds4f=True, train_n=931, admit=True,
            busy=True, lock=False,
        )
        self.assertFalse(ok)
        self.assertIn("already", reason.lower())

    def test_stack_lock_blocks(self) -> None:
        ok, reason = lc.can_start(
            stop_0731=False, ds4f=False, train_n=931, admit=True,
            busy=False, lock=True,
        )
        self.assertFalse(ok)
        self.assertIn("setup", reason.lower())

    def test_admit_refused_when_0731_already_down(self) -> None:
        ok, reason = lc.can_start(
            stop_0731=False, ds4f=False, train_n=931, admit=False,
            busy=False, lock=False,
        )
        self.assertFalse(ok)
        self.assertIn("admit", reason.lower())

    def test_dual_copy_mentions_both_sparks(self) -> None:
        ok, reason = lc.can_start(
            stop_0731=False, ds4f=False, train_n=931, admit=True,
            busy=False, lock=False, dual=True,
        )
        self.assertTrue(ok)
        self.assertIn("dual", reason.lower())

    def test_single_copy_mentions_n1(self) -> None:
        ok, reason = lc.can_start(
            stop_0731=False, ds4f=False, train_n=931, admit=True,
            busy=False, lock=False, dual=False,
        )
        self.assertTrue(ok)
        self.assertIn("n1", reason.lower())


    def test_0731_student_single_blocked(self) -> None:
        ok, reason = lc.can_start(
            stop_0731=False, ds4f=False, train_n=2016, admit=True,
            busy=False, lock=False, dual=False, student="ds0731",
        )
        self.assertFalse(ok)
        self.assertIn("both Sparks", reason)

    def test_0731_student_dual_ok(self) -> None:
        ok, reason = lc.can_start(
            stop_0731=False, ds4f=False, train_n=2016, admit=True,
            busy=False, lock=False, dual=True, student="0731",
        )
        self.assertTrue(ok)
        self.assertIn("0731", reason)

    def test_normalize_student(self) -> None:
        self.assertEqual(lc.normalize_student("0731"), "ds0731")
        self.assertEqual(lc.normalize_student("qwen38"), "qwen38")


class StartGuardTests(unittest.TestCase):
    def test_unknown_busy_does_not_spawn(self) -> None:
        fake = {
            "train_n": 931, "ready_gold": True, "ds4f_up": True,
            "admit_ok": False, "active_operation": {"id": "x", "status": "running"},
            "eta": "1–3 hours",
            "students": {"qwen38": {"train_n": 931}, "ds0731": {"train_n": 2016}},
        }
        with patch.object(lc, "status", return_value=fake), \
             patch.object(lc, "_lock_busy", return_value=False):
            result = lc.start(stop_0731=True)
        self.assertFalse(result["ok"])

    def test_start_0731_sets_student_env(self) -> None:
        fake = {
            "train_n": 931, "ready_gold": True, "ds4f_up": False,
            "admit_ok": True, "active_operation": None,
            "eta": "2–4 hours both Sparks",
            "students": {"qwen38": {"train_n": 931}, "ds0731": {"train_n": 2016}},
        }
        captured = {}

        class FakeProc:
            pid = 4242
            def wait(self):
                return 0

        def fake_popen(cmd, stdout=None, stderr=None, env=None):
            captured["env"] = env
            captured["cmd"] = cmd
            return FakeProc()

        with patch.object(lc, "status", return_value=fake), \
             patch.object(lc, "_lock_busy", return_value=False), \
             patch.object(lc, "can_start", return_value=(True, "dual 0731")), \
             patch.object(lc, "TRAIN_SH", lc.Path("/bin/true")), \
             patch("lora_control.subprocess.Popen", side_effect=fake_popen), \
             patch("lora_control.threading.Thread"):
            result = lc.start(stop_0731=False, dual=True, student="ds0731")
        self.assertTrue(result["ok"])
        self.assertEqual(result["student"], "ds0731")
        self.assertEqual(captured["env"].get("LORA_STUDENT"), "ds0731")
        self.assertEqual(captured["env"].get("SPARK_0731_QLORA"), "1")
        self.assertEqual(captured["env"].get("LORA_DUAL"), "1")
