#!/usr/bin/env python3
"""
Allowlisted desktop-app launchers for the console.

Same posture as quick_actions.py, with one difference: these are GUI apps that
run until the user closes them, so each is spawned **detached** rather than
waited on. Only the apps in APPS can ever be launched, no argument ever comes
from the request, and a launch is refused if the app is already up (so the
button cannot spawn ten Blenders).

These are deliberately separate from the Services board: `blender-mcp.service`
is the headless, agent-facing bridge (start/stop/restart there), while these
open a real window on the desktop for a human to work in.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import threading
import time
from pathlib import Path

HOME = Path.home()

# The X display the desktop session actually runs on. The console runs as a
# systemd --user service with no DISPLAY of its own, so it must be set here.
DISPLAY = os.environ.get("CONSOLE_DESKTOP_DISPLAY", ":0")

# Inert argv marker so a desktop Blender window is distinguishable from the
# headless blender-mcp.service, which otherwise has a byte-identical cmdline.
GUI_MARKER = "spark-console-desktop-gui"

APPS: dict[str, dict] = {
    "blender-gui": {
        "label": "Blender GUI",
        "icon": "🎨",
        "detail": "Blender 5.1 on the desktop, MCP addon connected automatically",
        # The trailing marker is inert to Blender (everything after `--` is
        # ignored) but makes this window distinguishable from the identical
        # cmdline that blender-mcp.service runs under xvfb-run.
        "cmd": [str(HOME / ".local/bin/blender"),
                "--python", str(HOME / ".hermes/blender/start_with_mcp.py"),
                "--", GUI_MARKER],
        "bin": str(HOME / ".local/bin/blender"),
        "match": GUI_MARKER,
        "note": "Shares 127.0.0.1:9876 with blender-mcp.service — "
                "stop that service first, or this window's bridge will not bind.",
    },
    "godot-editor": {
        "label": "Godot Editor",
        "icon": "🎮",
        "detail": "Godot 4.7.1 editor on ~/mega-tester (project manager if missing)",
        "cmd": None,          # built at launch time by _godot_cmd()
        "bin": str(HOME / ".local/bin/godot"),
        "match": "bin/godot",
    },
}

_lock = threading.Lock()
_last: dict[str, dict] = {}


def _godot_cmd() -> list[str]:
    """Open the editor on mega-tester when it exists, else the project manager."""
    godot = str(HOME / ".local/bin/godot")
    project = HOME / "mega-tester"
    if (project / "project.godot").is_file():
        return [godot, "--editor", "--path", str(project)]
    return [godot, "--project-manager"]


def _cmdline(pid: int) -> list[str]:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except Exception:
        return []
    return [a for a in raw.decode(errors="replace").split("\0") if a]


def _running(spec: dict) -> int | None:
    """PID of a live instance of this app, else None.

    pgrep alone is not enough: a shell whose argv merely *contains* the pattern
    matches too. So every candidate is confirmed against /proc — argv[0] must be
    the app's own binary, and the marker (if any) must be a real argument.
    """
    if not shutil.which("pgrep"):
        return None
    match, binary = spec["match"], spec.get("bin", "")
    try:
        out = subprocess.run(["pgrep", "-f", match], capture_output=True,
                             text=True, timeout=5)
    except Exception:
        return None
    for tok in (out.stdout or "").split():
        try:
            pid = int(tok)
        except ValueError:
            continue
        if pid == os.getpid():
            continue
        argv = _cmdline(pid)
        if not argv:
            continue
        if binary and argv[0] != binary:
            continue          # a shell or wrapper that merely mentions the name
        if match == GUI_MARKER and GUI_MARKER not in argv[1:]:
            continue          # headless service shares argv[0]; marker decides
        if spec is APPS.get("godot-editor") and "--editor" not in argv:
            continue
        return pid
    return None


def list_apps() -> list[dict]:
    rows = []
    for key, spec in APPS.items():
        pid = _running(spec)
        with _lock:
            last = _last.get(key)
        rows.append({
            "id": key,
            "label": spec["label"],
            "icon": spec["icon"],
            "detail": spec["detail"],
            "note": spec.get("note"),
            "running": pid is not None,
            "pid": pid,
            "last": last,
        })
    return rows


def launch_app(key: str) -> dict:
    if key not in APPS:
        return {"ok": False, "error": f"Unknown app: {key}"}
    spec = APPS[key]

    pid = _running(spec)
    if pid is not None:
        return {"ok": False, "app": key, "running": True, "pid": pid,
                "error": f"{spec['label']} is already running (pid {pid})."}

    cmd = spec["cmd"] or _godot_cmd()
    binary = Path(cmd[0])
    if not binary.exists():
        return {"ok": False, "error": f"Not installed: {binary}"}

    env = dict(os.environ)
    env["DISPLAY"] = DISPLAY
    env.pop("WAYLAND_DISPLAY", None)     # force X11; the session is XDG_SESSION_TYPE=x11

    log_dir = HOME / "logs/desktop-launch"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{key}-{time.strftime('%Y%m%d-%H%M%S')}.log"

    try:
        with open(log_path, "wb") as log:
            proc = subprocess.Popen(
                cmd, env=env, stdout=log, stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,   # detach: survives a console restart
                cwd=str(HOME),
            )
    except Exception as e:
        return {"ok": False, "error": f"launch failed: {e}"[:200]}

    # Give it a moment to fall over on its own (bad DISPLAY, missing lib, …).
    time.sleep(1.5)
    if proc.poll() is not None:
        tail = ""
        try:
            tail = log_path.read_text(errors="replace")[-400:]
        except Exception:
            pass
        return {"ok": False, "app": key,
                "error": f"{spec['label']} exited immediately (rc={proc.returncode}). {tail}"[:400]}

    result = {"ok": True, "app": key, "pid": proc.pid,
              "message": f"{spec['label']} launched on {DISPLAY}",
              "log": str(log_path),
              "at": time.strftime("%Y-%m-%d %H:%M:%S")}
    with _lock:
        _last[key] = {k: result[k] for k in ("ok", "pid", "message", "at")}
    return result
