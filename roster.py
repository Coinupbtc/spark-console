#!/usr/bin/env python3
"""Hermes gateway roster for Spark Console.

Keep in lockstep with ~/.hermes/scripts/agent-roster.sh.
Exclusive one-engine setups (GLM TP2, Flash-Next, Prime, helper, video, music)
run orch + dobby only. Dual-brain setups unpark light / smeagle / freegle.
"""
from __future__ import annotations

import json
from pathlib import Path

STATE_JSON = Path.home() / ".local/state/hermes/spark-stack.json"
ALL = ("orchestrator", "dobby", "light", "smeagle", "freegle")
DUAL_SETUPS = frozenset({"dream", "twins", "qwen38"})


def kind_for_setup(setup: str) -> str:
    return "dual" if (setup or "") in DUAL_SETUPS else "exclusive"


def live_for_setup(setup: str) -> tuple[str, ...]:
    return ALL if kind_for_setup(setup) == "dual" else ("orchestrator", "dobby")


def parked_for_setup(setup: str) -> tuple[str, ...]:
    return () if kind_for_setup(setup) == "dual" else ("light", "smeagle", "freegle")


def telegram_for_setup(setup: str) -> tuple[str, ...]:
    return ("orchestrator", "light") if kind_for_setup(setup) == "dual" else ("orchestrator",)


def service_id(profile: str) -> str:
    return f"hermes-{profile}"


def from_state(path: Path | None = None) -> dict:
    """Live roster snapshot — cheap JSON read, no systemd."""
    setup = ""
    p = path or STATE_JSON
    try:
        if p.is_file():
            setup = str(json.loads(p.read_text()).get("desired") or "")
    except (OSError, ValueError):
        setup = ""
    kind = kind_for_setup(setup)
    live = list(live_for_setup(setup))
    parked = list(parked_for_setup(setup))
    return {
        "setup": setup or "unknown",
        "kind": kind,
        "live": live,
        "parked": parked,
        "telegram": list(telegram_for_setup(setup)),
        "live_ids": [service_id(x) for x in live],
        "parked_ids": [service_id(x) for x in parked],
        # Stranger copy for Pulse — not a dump of ports.
        "how": (
            "Pulse = is the house OK. Fleet = Spark / Pi / Start9. "
            "Control = switch the one live setup. Jobs = broken crons. "
            "Chat is Telegram orch."
            + (
                " Dual-brain: orch+dobby+light+smeagle+freegle."
                if kind == "dual"
                else " Exclusive TP2: orch + dobby only — do not start light/smeagle."
            )
        ),
    }
