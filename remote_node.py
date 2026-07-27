#!/usr/bin/env python3
"""
Optional second-node poller — one batched SSH call, parsed into the same shape
as the local snapshot.

Disabled unless you point it at a host. Set either:

    NODE2_HOST=user@second-node.example      # anything ssh(1) accepts
    NODE2_SSH_ALIAS=mynode        # or a ~/.ssh/config alias

Optional:
    NODE2_SSH_KEY=~/.ssh/id_ed25519   # pin an identity (needed under systemd,
                                      # where no SSH agent is available)
    NODE2_ENDPOINTS=8000,8080         # extra local ports to probe on the node
    NODE2_SSH_TIMEOUT=25

With nothing configured every call returns a cheap "not configured" result, so
the console runs fine as a single-node dashboard. Callers cache this behind a
background thread — an SSH round-trip must never sit on a request path.
"""
from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

SSH_TIMEOUT = int(os.environ.get("NODE2_SSH_TIMEOUT", "25"))

# The remote half. Tagged sections keep parsing dumb and resilient: an
# unrecognised or empty section is skipped rather than breaking the payload.
# The trailing `echo` after each curl matters — JSON without a newline would
# otherwise glue itself onto the next @TAG line.
REMOTE_SCRIPT = r"""
echo @HOST; hostname
echo @LOAD; cat /proc/loadavg
echo @MEM; free -b | sed -n '2p;3p'
echo @DISK; df -B1 / | tail -1
echo @GPU; nvidia-smi --query-gpu=index,name,power.draw,temperature.gpu,utilization.gpu --format=csv,noheader,nounits 2>/dev/null
echo @TOPMEM; ps -eo comm,rss --sort=-rss --no-headers | head -5
echo @ENDPOINTS
for p in __PORTS__; do
  body=$(curl -s -m 3 "http://127.0.0.1:${p}/v1/models" 2>/dev/null || true)
  [ -n "$body" ] && echo "${p} ${body}"
done
echo @SAMPLES
read tprev iprev < <(awk '/^cpu /{print $2+$3+$4+$5+$6+$7+$8+$9, $5+$6}' /proc/stat)
read rxp txp < <(awk '/:/{sub(/^[^:]*:/,"");rx+=$1;tx+=$9} END{print rx,tx}' /proc/net/dev)
for i in 1 2 3 4 5; do
  sleep 1
  read tcur icur < <(awk '/^cpu /{print $2+$3+$4+$5+$6+$7+$8+$9, $5+$6}' /proc/stat)
  read rxc txc < <(awk '/:/{sub(/^[^:]*:/,"");rx+=$1;tx+=$9} END{print rx,tx}' /proc/net/dev)
  read gpu pwr < <(nvidia-smi --query-gpu=utilization.gpu,power.draw --format=csv,noheader,nounits 2>/dev/null | head -1 | tr ',' ' ')
  read mem swap < <(awk '/MemTotal/{mt=$2}/MemAvailable/{ma=$2}/SwapTotal/{st=$2}/SwapFree/{sf=$2} END{printf "%.1f %.1f", (mt-ma)/mt*100, (st ? (st-sf)/st*100 : 0)}' /proc/meminfo)
  cpu=$(awk -v t=$((tcur-tprev)) -v i=$((icur-iprev)) 'BEGIN{if(t>0)printf "%.1f",(t-i)/t*100; else print 0}')
  rxk=$(awk -v a="$rxc" -v b="$rxp" 'BEGIN{printf "%.1f",(a-b)/1024}')
  txk=$(awk -v a="$txc" -v b="$txp" 'BEGIN{printf "%.1f",(a-b)/1024}')
  echo "$(date +%s) $cpu ${gpu:-0} $mem $swap $rxk $txk ${pwr:-0}"
  tprev=$tcur; iprev=$icur; rxp=$rxc; txp=$txc
done
echo @END
"""

# Ports probed for an OpenAI-compatible /v1/models on the remote node.
DEFAULT_PORTS = ["8000", "8080", "11434"]


def node2_target() -> str | None:
    """The ssh destination, or None when no second node is configured."""
    return (os.environ.get("NODE2_HOST")
            or os.environ.get("NODE2_SSH_ALIAS")
            or None)


def _ssh_cmd(target: str) -> list[str]:
    cmd = [
        "ssh", "-o", "BatchMode=yes",              # never prompt; fail instead
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", f"ConnectTimeout={min(SSH_TIMEOUT, 10)}",
    ]
    key = os.environ.get("NODE2_SSH_KEY")
    if key:
        key_path = Path(key).expanduser()
        if key_path.is_file():
            # IdentitiesOnly stops ssh from trying agent keys first
            cmd += ["-o", "IdentitiesOnly=yes", "-i", str(key_path)]
    return cmd + [target, "bash -s"]


def _blank(error: str | None = None) -> dict:
    return {
        "name": "node2",
        "role": "node2",
        "iso_ts": datetime.now(timezone.utc).isoformat(),
        "reachable": False,
        "error": error,
        "cpu_pct": None,
        "mem": {}, "swap": {}, "gpus": [], "models": [],
        "endpoints": [], "workloads": [], "top_procs": [], "samples": [],
    }


def _parse(text: str) -> dict:
    """Split the tagged transcript into sections, then parse each one."""
    out = _blank()
    out["reachable"] = True
    sections: dict[str, list[str]] = {}
    current = None
    for line in text.splitlines():
        if line.startswith("@"):
            current = line[1:].strip()
            sections[current] = []
        elif current:
            sections[current].append(line)

    def sec(name: str) -> list[str]:
        return [ln for ln in sections.get(name, []) if ln.strip()]

    host = sec("HOST")
    if host:
        out["name"] = host[0].strip()

    load = sec("LOAD")
    if load:
        parts = load[0].split()
        if parts:
            try:
                out["load_1"] = float(parts[0])
                # Rough CPU proxy; the SAMPLES block below is the accurate one.
                out["cpu_pct"] = None
            except ValueError:
                pass

    mem = sec("MEM")
    for row in mem:
        cols = row.split()
        if len(cols) >= 3 and cols[0].lower().startswith("mem"):
            total, used = int(cols[1]), int(cols[2])
            out["mem"] = {"total_gb": round(total / 1e9, 1),
                          "used_gb": round(used / 1e9, 1),
                          "pct": round(used / total * 100, 1) if total else 0}
        elif len(cols) >= 3 and cols[0].lower().startswith("swap"):
            total, used = int(cols[1]), int(cols[2])
            out["swap"] = {"total_gb": round(total / 1e9, 1),
                           "used_gb": round(used / 1e9, 1),
                           "pct": round(used / total * 100, 1) if total else 0}

    disk = sec("DISK")
    if disk:
        cols = disk[0].split()
        if len(cols) >= 5:
            try:
                out["disk"] = {"total_gb": round(int(cols[1]) / 1e9, 1),
                               "used_gb": round(int(cols[2]) / 1e9, 1),
                               "pct": float(cols[4].rstrip("%"))}
            except ValueError:
                pass

    for row in sec("GPU"):
        cols = [c.strip() for c in row.split(",")]
        if len(cols) >= 5:
            try:
                out["gpus"].append({
                    "index": int(cols[0]), "name": cols[1],
                    "power_w": float(cols[2]) if cols[2] not in ("N/A", "[N/A]") else 0.0,
                    "temp_c": float(cols[3]) if cols[3] not in ("N/A", "[N/A]") else 0.0,
                    "util_gpu": float(cols[4]) if cols[4] not in ("N/A", "[N/A]") else 0.0,
                })
            except ValueError:
                continue

    for row in sec("TOPMEM"):
        cols = row.split()
        if len(cols) >= 2:
            try:
                out["top_procs"].append({"name": cols[0],
                                         "rss_gb": round(int(cols[1]) / 1e6, 2)})
            except ValueError:
                continue

    for row in sec("ENDPOINTS"):
        port, _, body = row.partition(" ")
        entry = {"port": port, "ok": True, "models": []}
        try:
            data = json.loads(body)
            entry["models"] = [m.get("id") for m in data.get("data", []) if m.get("id")]
        except (ValueError, AttributeError):
            pass
        out["endpoints"].append(entry)
        out["models"].extend(entry["models"])

    for row in sec("SAMPLES"):
        cols = row.split()
        if len(cols) == 8:
            try:
                out["samples"].append({
                    "t": int(cols[0]), "cpu": float(cols[1]), "gpu": float(cols[2]),
                    "mem": float(cols[3]), "swap": float(cols[4]),
                    "rx": float(cols[5]), "tx": float(cols[6]), "pwr": float(cols[7]),
                })
            except ValueError:
                continue
    if out["samples"]:
        out["cpu_pct"] = out["samples"][-1]["cpu"]

    return out


def query_node2() -> dict:
    """Poll the configured second node. Never raises — always returns a dict."""
    target = node2_target()
    if not target:
        return _blank("not configured (set NODE2_HOST or NODE2_SSH_ALIAS)")

    ports = os.environ.get("NODE2_ENDPOINTS", "")
    port_list = [p.strip() for p in ports.split(",") if p.strip()] or DEFAULT_PORTS
    script = REMOTE_SCRIPT.replace("__PORTS__", " ".join(port_list))

    try:
        out = subprocess.run(_ssh_cmd(target), input=script, capture_output=True,
                             text=True, timeout=SSH_TIMEOUT)
    except subprocess.TimeoutExpired:
        return _blank(f"ssh timed out after {SSH_TIMEOUT}s")
    except OSError as e:
        return _blank(f"{type(e).__name__}: {e}"[:200])

    if out.returncode != 0:
        return _blank((out.stderr or "ssh failed").strip()[:200])
    try:
        return _parse(out.stdout)
    except Exception as e:  # a parse bug must not take the dashboard down
        blank = _blank(f"parse error: {type(e).__name__}: {e}"[:200])
        blank["reachable"] = True
        return blank


if __name__ == "__main__":
    print(json.dumps(query_node2(), indent=2)[:2000])
