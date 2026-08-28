"""Guard the published tree: read-only HTTP API, no house-lab map."""
from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Routes that would make this repo a remote control plane.
_CONTROL_ROUTES = (
    "/api/models/stop",
    "/api/models/switch",
    "/api/models/kill",
    "/api/models/unload",
    "/api/launch/",
    "/api/comfy/cancel",
    'service_action',
)

# House-lab fingerprints that must not ship in the public default.
_LAB_PATTERNS = (
    re.compile(r"192\.168\."),
    re.compile(r"10\.(?:[0-9]{1,3}\.){2}[0-9]{1,3}"),
    re.compile(r"172\.(?:1[6-9]|2[0-9]|3[0-1])\."),
    re.compile(r"100\.(?:6[4-9]|[7-9][0-9]|1[01][0-9]|12[0-7])\."),
    re.compile(r"id_ed25519_shared"),
    re.compile(r"coffee-house"),
    re.compile(r"cosmic-charcoal"),
    re.compile(r"sparkmax-10ef"),
    re.compile(r"sparkymaxxx-12ef"),
    re.compile(r"\bbitcoind\b", re.I),
    re.compile(r"vaultwarden", re.I),
    re.compile(r"\blnbits\b", re.I),
    re.compile(r"btcpay", re.I),
    re.compile(r"\bimmich\b", re.I),
)

_SKIP_DIRS = {".git", ".venv", "venv", "__pycache__", "data", "docs"}
_TEXT_SUFFIXES = {".py", ".html", ".sh", ".md", ".json", ".yml", ".yaml", ".txt", ".webmanifest"}


def _iter_text_files():
    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue
        if any(part in _SKIP_DIRS for part in path.parts):
            continue
        if path.suffix.lower() not in _TEXT_SUFFIXES and path.name not in {
            "start.sh", "stop.sh", "setup.sh",
        }:
            continue
        if path.name == "test_public_tree.py":
            continue
        yield path


class PublicTreeTests(unittest.TestCase):
    def test_server_has_no_control_routes(self) -> None:
        source = (ROOT / "server.py").read_text()
        for needle in _CONTROL_ROUTES:
            self.assertNotIn(needle, source, f"control route leaked: {needle}")
        self.assertNotIn("@app.post(\"/api/services/", source)
        self.assertIn("@app.get(\"/api/services\")", source)
        self.assertIn("@app.get(\"/api/overview\")", source)

    def test_tree_has_no_house_lab_map(self) -> None:
        hits: list[str] = []
        for path in _iter_text_files():
            text = path.read_text(errors="ignore")
            for pattern in _LAB_PATTERNS:
                if pattern.search(text):
                    hits.append(f"{path.relative_to(ROOT)}: {pattern.pattern}")
        self.assertEqual(hits, [], "house-lab fingerprints still in the tree")


if __name__ == "__main__":
    unittest.main()
