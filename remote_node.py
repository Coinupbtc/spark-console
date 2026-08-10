#!/usr/bin/env python3
"""
Node2 (sparkymaxxx-12ef) poller — one batched SSH call over the CX7 fabric,
parsed into the same shape as the local node snapshot. Results are cached by
a background refresher thread in server.py so page loads never wait on SSH.
"""
from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

NODE2_ALIAS = "spark2"  # ~/.ssh/config → 192.168.100.11 over CX7
SSH_TIMEOUT = 25  # remote script includes a 10×1s sampling loop
# Pin identity so BatchMode works under systemd without SSH agent
_SHARED_KEY = Path.home() / ".ssh/id_ed25519_shared"
_CX7_IP = "192.168.100.11"
_DEEP_PORT = 8100

REMOTE_SCRIPT = r"""
echo @HOST; hostname
echo @UPTIME; cat /proc/uptime
echo @LOAD; cat /proc/loadavg
echo @MEM; free -b | sed -n '2p;3p'
echo @DISK; df -B1 / | tail -1
# Prefer average draw (matches node1 / energy log); fall back to instant.
echo @GPU; (nvidia-smi --query-gpu=index,name,power.draw.average,temperature.gpu,utilization.gpu --format=csv,noheader,nounits 2>/dev/null || nvidia-smi --query-gpu=index,name,power.draw,temperature.gpu,utilization.gpu --format=csv,noheader,nounits 2>/dev/null)
echo @TOPMEM; ps -eo comm,rss --sort=-rss --no-headers | head -5
echo @WORKLOADS
ps -eo comm,args --no-headers | awk '
BEGIN { IGNORECASE=1 }
($1 == "awk" || $1 == "ps") { next }
/daily_scan|pokemon-arb/ { seen["pokemon"]=1 }
/run_cycle|crypto-machine/ { seen["crypto"]=1 }
/ComfyUI/ { seen["comfyui"]=1 }
/huggingface-cli download|hf download|aria2c|wget / { seen["download"]=1 }
/bakeoff.py/ { seen["bakeoff"]=1 }
/llama-server/ { seen["llama"]=1 }
/vllm/ { seen["vllm"]=1 }
/ollama/ { seen["ollama"]=1 }
END { for (label in seen) print label }
'
# Always force a trailing newline after curl (JSON often has none → glues onto next @TAG)
echo @VLLM; curl -s -m 3 http://127.0.0.1:8888/v1/models 2>/dev/null || true; echo
echo @LLAMA; curl -s -m 3 http://127.0.0.1:8889/v1/models 2>/dev/null || true; echo
# deep lane binds CX7 IP only (not always localhost)
echo @DEEP; curl -s -m 3 http://192.168.100.11:8100/v1/models 2>/dev/null || curl -s -m 3 http://127.0.0.1:8100/v1/models 2>/dev/null || true; echo
echo @SAMPLES
read tprev iprev < <(awk '/^cpu /{print $2+$3+$4+$5+$6+$7+$8+$9, $5+$6}' /proc/stat)
read rxp txp < <(awk '/:/{sub(/^[^:]*:/,"");rx+=$1;tx+=$9} END{print rx,tx}' /proc/net/dev)
for i in 1 2 3 4 5 6 7 8 9 10; do
  sleep 1
  read tcur icur < <(awk '/^cpu /{print $2+$3+$4+$5+$6+$7+$8+$9, $5+$6}' /proc/stat)
  read rxc txc < <(awk '/:/{sub(/^[^:]*:/,"");rx+=$1;tx+=$9} END{print rx,tx}' /proc/net/dev)
  # Prefer average draw for energy accuracy; fall back to instant.
  read gpu pwr < <( (nvidia-smi --query-gpu=utilization.gpu,power.draw.average --format=csv,noheader,nounits 2>/dev/null || nvidia-smi --query-gpu=utilization.gpu,power.draw --format=csv,noheader,nounits 2>/dev/null) | head -1 | tr ',' ' ')
  read mem swap < <(awk '/MemTotal/{mt=$2}/MemAvailable/{ma=$2}/SwapTotal/{st=$2}/SwapFree/{sf=$2} END{printf "%.1f %.1f", (mt-ma)/mt*100, (st ? (st-sf)/st*100 : 0)}' /proc/meminfo)
  cpu=$(awk -v t=$((tcur-tprev)) -v i=$((icur-iprev)) 'BEGIN{if(t>0)printf "%.1f",(t-i)/t*100; else print 0}')
  rxk=$(awk -v a="$rxc" -v b="$rxp" 'BEGIN{printf "%.1f",(a-b)/1024}')
  txk=$(awk -v a="$txc" -v b="$txp" 'BEGIN{printf "%.1f",(a-b)/1024}')
  echo "$(date +%s) $cpu ${gpu:-0} $mem $swap $rxk $txk ${pwr:-0}"
  tprev=$tcur; iprev=$icur; rxp=$rxc; txp=$txc
done
echo @END
"""


def _ssh_base() -> list[str]:
    cmd = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=5",
        "-o", "StrictHostKeyChecking=accept-new",
    ]
    if _SHARED_KEY.is_file():
        cmd += ["-o", "IdentitiesOnly=yes", "-i", str(_SHARED_KEY)]
    # Avoid broken gpg-agent sockets under systemd when key file is enough
    return cmd


def _uptime_human(seconds: float) -> str:
    d, rem = divmod(int(seconds), 86400)
    h, rem = divmod(rem, 3600)
    m = rem // 60
    return f"{d}d {h}h" if d else f"{h}h {m}m"


def _parse_sections(raw: str) -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {}
    key = None
    for line in raw.splitlines():
        if line.startswith("@"):
            key = line.strip()[1:]
            sections[key] = []
        elif key:
            sections[key].append(line)
    return sections


def _parse_models_json(lines: list[str], port: int, engine: str) -> list[dict]:
    text = "\n".join(lines).strip()
    if not text:
        return []
    # Prefer first JSON object if noise trailed (legacy glue bug)
    try:
        data = json.loads(text)
    except (ValueError, TypeError):
        data = None
        for i, ch in enumerate(text):
            if ch != "{":
                continue
            try:
                data, _ = json.JSONDecoder().raw_decode(text[i:])
                break
            except ValueError:
                continue
        if data is None:
            return []
    out = []
    seen = set()
    for m in (data.get("data") or []) + (data.get("models") or []):
        mid = (m.get("id") or m.get("name") or m.get("model") or "?").rstrip("/").split("/")[-1]
        if mid in seen:
            continue
        seen.add(mid)
        out.append({"id": mid, "port": port, "engine": engine, "status": "ok"})
    return out


def query_node2() -> dict:
    now = datetime.now(timezone.utc).isoformat()
    base: dict = {"name": "sparkymaxxx-12ef", "role": "node2", "iso_ts": now}
    if os.getenv("DGX_NODE2_FORCE_FAILURE") == "1":
        # Isolate an end-to-end degraded-path test without changing SSH or node2.
        base.update({"reachable": False, "error": "forced node2 collection test"})
        return base
    env = {**os.environ, "HOME": str(Path.home())}
    # Prefer file key over a flaky agent socket
    env.pop("SSH_AUTH_SOCK", None)
    try:
        out = subprocess.run(
            _ssh_base() + [NODE2_ALIAS, "bash", "-s"],
            input=REMOTE_SCRIPT, capture_output=True, text=True,
            timeout=SSH_TIMEOUT, env=env,
        )
        if out.returncode != 0 or "@END" not in (out.stdout or ""):
            base.update({"reachable": False,
                         "error": (out.stderr or out.stdout or "ssh failed").strip()[:200]})
            return base
    except (subprocess.TimeoutExpired, OSError) as e:
        base.update({"reachable": False, "error": str(e)[:200]})
        return base

    s = _parse_sections(out.stdout)
    base["reachable"] = True
    try:
        base["hostname"] = s["HOST"][0].strip()
        base["uptime"] = _uptime_human(float(s["UPTIME"][0].split()[0]))
        load = s["LOAD"][0].split()
        base["load"] = f"{load[0]} / {load[1]} / {load[2]}"

        mem = s["MEM"][0].split()   # Mem: total used free shared buff avail
        swap = s["MEM"][1].split()  # Swap: total used free
        gib = 1024 ** 3
        base["mem"] = {
            "used_gb": round(int(mem[2]) / gib, 1),
            "total_gb": round(int(mem[1]) / gib, 1),
            "avail_gb": round(int(mem[6]) / gib, 1),
            "pct": round(int(mem[2]) / int(mem[1]) * 100, 1),
        }
        st = int(swap[1])
        base["swap"] = {
            "used_gb": round(int(swap[2]) / gib, 1),
            "total_gb": round(st / gib, 1),
            "pct": round(int(swap[2]) / st * 100, 1) if st else 0.0,
        }
        disk = s["DISK"][0].split()
        base["disk"] = {
            "pct": float(disk[4].rstrip("%")),
            "free_tb": round(int(disk[3]) / 1024 ** 4, 2),
        }
        base["cpu_pct"] = round(min(float(load[0]) / 20 * 100, 100), 1)

        gpus = []
        for line in s.get("GPU", []):
            p = [x.strip() for x in line.split(",")]
            if len(p) >= 5:
                gpus.append({
                    "index": int(p[0]), "name": p[1],
                    "power_w": float(p[2]) if p[2] not in ("N/A", "[N/A]") else 0.0,
                    "temp_c": int(float(p[3])) if p[3] not in ("N/A", "[N/A]") else 0,
                    "util_gpu": float(p[4]) if p[4] not in ("N/A", "[N/A]") else 0.0,
                })
        base["gpus"] = gpus

        top = []
        for line in s.get("TOPMEM", []):
            p = line.split()
            if len(p) >= 2:
                top.append({"name": p[0], "mem_gb": round(int(p[1]) * 1024 / 1024 ** 3, 1)})
        base["top_procs"] = top

        deep_models = _parse_models_json(s.get("DEEP", []), _DEEP_PORT, "llama.cpp-deep")
        llama_models = _parse_models_json(s.get("LLAMA", []), 8889, "llama.cpp")
        vllm_models = _parse_models_json(s.get("VLLM", []), 8888, "vLLM")
        models = deep_models + llama_models + vllm_models
        base["models"] = models
        base["endpoints"] = [
            {"port": _DEEP_PORT, "engine": "llama.cpp-deep",
             "status": "ok" if deep_models else "down"},
            {"port": 8889, "engine": "llama.cpp",
             "status": "ok" if llama_models else "down"},
            {"port": 8888, "engine": "vLLM",
             "status": "ok" if vllm_models else "down"},
        ]
        base["workloads"] = sorted(set(s.get("WORKLOADS", [])))
        if any(gpu.get("util_gpu", 0) >= 5 for gpu in gpus) and models:
            base["workloads"].append("inference-busy")

        samples = []
        for line in s.get("SAMPLES", []):
            p = line.split()
            # 7 fields = legacy (pre-power); 8 = util+power.draw watts
            if len(p) in (7, 8):
                try:
                    samples.append({
                        "t": int(p[0]), "cpu": float(p[1]), "gpu": float(p[2]),
                        "mem": float(p[3]), "swap": float(p[4]),
                        "rx": float(p[5]), "tx": float(p[6]),
                        "pwr": float(p[7]) if len(p) == 8 else 0.0,
                    })
                except ValueError:
                    continue
        base["samples"] = samples
        if samples:
            # The 10-second sampled CPU value is more representative than
            # converting load average with an arbitrary core-count divisor.
            base["cpu_pct"] = round(
                sum(sample["cpu"] for sample in samples) / len(samples), 1
            )
    except (KeyError, IndexError, ValueError) as e:
        base["parse_error"] = f"{type(e).__name__}: {e}"
    return base


if __name__ == "__main__":
    print(json.dumps(query_node2(), indent=2))
