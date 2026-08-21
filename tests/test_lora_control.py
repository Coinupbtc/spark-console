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


class StartGuardTests(unittest.TestCase):
    def test_unknown_busy_does_not_spawn(self) -> None:
        fake = {
            "train_n": 931, "ready_gold": True, "ds4f_up": True,
            "admit_ok": False, "active_operation": {"id": "x", "status": "running"},
            "eta": "1–3 hours",
        }
        with patch.object(lc, "status", return_value=fake), \
             patch.object(lc, "_lock_busy", return_value=False):
            result = lc.start(stop_0731=True)
        self.assertFalse(result["ok"])
