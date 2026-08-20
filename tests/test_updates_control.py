"""Unit tests for Spark Console updates (no live git fetch / apply)."""
from __future__ import annotations

import unittest
from unittest.mock import patch

import updates_control as uc


class CanApplyTests(unittest.TestCase):
    def test_hermes_behind_with_local_reapply_is_allowed(self) -> None:
        info = {
            "exists": True, "behind": 127, "ahead": 3, "dirty": False,
            "state": "diverged", "error": None,
        }
        ok, reason = uc.can_apply("hermes", info, False)
        self.assertTrue(ok)
        self.assertIn("127", reason)

    def test_hermes_current_blocked(self) -> None:
        info = {
            "exists": True, "behind": 0, "ahead": 0, "dirty": False,
            "state": "current", "error": None,
        }
        ok, reason = uc.can_apply("hermes", info, False)
        self.assertFalse(ok)
        self.assertIn("current", reason)

    def test_recipe_dirty_blocked(self) -> None:
        info = {
            "exists": True, "behind": 11, "ahead": 0, "dirty": True,
            "state": "dirty", "error": None,
        }
        ok, reason = uc.can_apply("git", info, False)
        self.assertFalse(ok)
        self.assertIn("dirty", reason.lower())

    def test_recipe_diverged_blocked(self) -> None:
        info = {
            "exists": True, "behind": 30, "ahead": 1, "dirty": False,
            "state": "diverged", "error": None,
        }
        ok, reason = uc.can_apply("ds4f", info, False)
        self.assertFalse(ok)
        self.assertIn("diverged", reason.lower())

    def test_recipe_ahead_only_is_current(self) -> None:
        info = {
            "exists": True, "behind": 0, "ahead": 2, "dirty": False,
            "state": "current", "error": None,
        }
        ok, reason = uc.can_apply("ds4f", info, False)
        self.assertFalse(ok)
        self.assertIn("local", reason)

    def test_recipe_behind_clean_allowed(self) -> None:
        info = {
            "exists": True, "behind": 4, "ahead": 0, "dirty": False,
            "state": "behind", "error": None,
        }
        ok, _ = uc.can_apply("git", info, False)
        self.assertTrue(ok)

    def test_busy_blocks_everything(self) -> None:
        info = {
            "exists": True, "behind": 4, "ahead": 0, "dirty": False,
            "state": "behind", "error": None,
        }
        ok, reason = uc.can_apply("hermes", info, True)
        self.assertFalse(ok)
        self.assertIn("running", reason)

    def test_bundle_always_allowed_when_idle(self) -> None:
        ok, _ = uc.can_apply("bundle", {"exists": False}, False)
        self.assertTrue(ok)


class ApplyGuardTests(unittest.TestCase):
    def test_unknown_key_refused(self) -> None:
        result = uc.apply("not-a-real-target")
        self.assertFalse(result["ok"])
        self.assertIn("Unknown", result["error"])

    def test_named_keys(self) -> None:
        self.assertIn("hermes", uc.TARGETS)
        self.assertIn("ds4f", uc.TARGETS)
        self.assertIn("recipes", uc.TARGETS)
        for spec in uc.TARGETS.values():
            for field in ("label", "kind", "eta", "warn"):
                self.assertTrue(spec.get(field), f"missing {field}")

    def test_cmd_for_hermes_uses_wrapper(self) -> None:
        cmd = uc._cmd_for("hermes")
        self.assertEqual(cmd[:3], ["bash", str(uc.APPLY_SH), "hermes"])

    def test_cmd_for_git_passes_path(self) -> None:
        cmd = uc._cmd_for("h3-2x")
        self.assertEqual(cmd[0:3], ["bash", str(uc.APPLY_SH), "git"])
        self.assertIn("MiniMax-H3-2x-DGX-Spark", cmd[3])


class InspectTests(unittest.TestCase):
    def test_missing_path(self) -> None:
        info = uc.inspect_repo("/tmp/definitely-not-a-hermes-repo-xyz")
        self.assertFalse(info["exists"])
        self.assertEqual(info["state"], "missing")

    def test_bundle_placeholder(self) -> None:
        info = uc.inspect_repo(None)
        self.assertEqual(info["state"], "bundle")


class StatusUsesInspect(unittest.TestCase):
    def test_status_shapes_targets(self) -> None:
        fake = {
            "exists": True, "head": "abc", "remote": "def",
            "behind": 2, "ahead": 0, "dirty": False, "dirty_files": [],
            "state": "behind", "error": None,
        }
        import tempfile
        from pathlib import Path
        tmp = Path(tempfile.mkdtemp())
        with patch.object(uc, "inspect_repo", return_value=fake), \
             patch.object(uc, "active_operation", return_value=None), \
             patch.object(uc, "DATA_DIR", tmp), \
             patch.object(uc, "CACHE_FILE", tmp / "updates_status.json"):
            uc._status_cache = None
            out = uc._build_status(fetched=False)
        keys = [t["key"] for t in out["targets"]]
        self.assertEqual(keys, list(uc.TARGETS))
        hermes = next(t for t in out["targets"] if t["key"] == "hermes")
        self.assertTrue(hermes["can_apply"])
        self.assertEqual(out["behind_total"], 2 * (len(uc.TARGETS) - 1))


if __name__ == "__main__":
    unittest.main()
