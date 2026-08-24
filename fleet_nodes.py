#!/usr/bin/env python3
"""
Fleet appliance pollers — Raspberry Pi 5 (mirror/watchdog) and Start9 server
(cosmic-charcoal, self-hosted Bitcoin/Nextcloud/Gitea stack).

Same shape as remote_node.query_node2(): ONE batched SSH call per host parsed
by @SECTION markers, driven by a background thread in server.py so page loads
never wait on SSH. Both hosts authenticate with ~/.ssh/id_ed25519_shared so
BatchMode works under systemd with no agent.

Read-only by design: these are appliances holding money-adjacent services
(bitcoind/lnd) and the tier-3 backup — the console reports, it does not
start/stop containers.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

SHARED_KEY = Path.home() / ".ssh/id_ed25519_shared"
PI_ALIAS = "pi5"           # ~/.ssh/config → coffee-house@192.168.50.152:2222
# Must track PI_MIRROR_KEEP_DAYS in ~/scripts/data/pi-setup-mirror.sh (keep-daily=14,
# same policy as restic tier-1). Used to detect a prune that has stopped running.
PI_MIRROR_KEEP_DAYS = 14
START9_ALIAS = "start9"    # ~/.ssh/config → start9@192.168.50.119
SSH_TIMEOUT = 30           # remote scripts include a 1s CPU sampling delta
GIB = 1024 ** 3

# ---------------------------------------------------------------- remote scripts

PI_SCRIPT = r"""
echo @HOST; hostname
echo @UPTIME; cat /proc/uptime
echo @LOAD; cat /proc/loadavg
echo @NPROC; nproc
echo @CPU
read t1 i1 < <(awk '/^cpu /{print $2+$3+$4+$5+$6+$7+$8+$9, $5+$6}' /proc/stat)
sleep 1
read t2 i2 < <(awk '/^cpu /{print $2+$3+$4+$5+$6+$7+$8+$9, $5+$6}' /proc/stat)
awk -v t=$((t2-t1)) -v i=$((i2-i1)) 'BEGIN{if(t>0)printf "%.1f\n",(t-i)/t*100; else print 0}'
echo @MEM; free -b | sed -n '2p;3p'
echo @DISK; df -B1 / | tail -1
echo @TEMP; vcgencmd measure_temp 2>/dev/null || awk '{printf "temp=%.1f'\''C\n",$1/1000}' /sys/class/thermal/thermal_zone0/temp 2>/dev/null
echo @THROTTLE; vcgencmd get_throttled 2>/dev/null || echo throttled=0x0
# Pi 5 PMIC rails (I×V sum ≈ board draw; EXT5V has V only — not wall meter)
echo @PMIC; vcgencmd pmic_read_adc 2>/dev/null
echo @SVC
for u in ssh cron tailscaled docker; do echo "$u $(systemctl is-active $u 2>/dev/null || echo unknown)"; done
# Syncthing runs as a TEMPLATED unit here (syncthing@coffee-house), not plain "syncthing".
# Probing the bare name reported inactive forever — check both, report under one name.
st=$(systemctl is-active syncthing 2>/dev/null || true)
[ "$st" = active ] || st=$(systemctl is-active syncthing@coffee-house 2>/dev/null || echo unknown)
echo "syncthing $st"
echo @MIRROR
if [ -d /home/coffee-house/spark-mirror ]; then
  find /home/coffee-house/spark-mirror -type f -printf '%T@\n' 2>/dev/null | sort -n | tail -1
  du -sb /home/coffee-house/spark-mirror 2>/dev/null | cut -f1
  find /home/coffee-house/spark-mirror -type f 2>/dev/null | wc -l
  # line 4 = oldest file: the mirror tier has no prune rule, so retention span is the
  # only way to see unbounded growth on a 57G SD card before it bites.
  find /home/coffee-house/spark-mirror -type f -printf '%T@\n' 2>/dev/null | sort -n | head -1
fi
# Headless-overhead: this box has no monitor but runs LXDE+VNC+telegram-desktop.
# Reported so the console can show reclaimable RAM instead of just "18% used".
echo @OVERHEAD
ps -eo rss,comm --no-headers 2>/dev/null | awk '$2 ~ /telegram|Xorg|lxpanel|pcmanfm|lightdm|gtk-nop|vnc/ {s+=$1} END{print (s?s:0)}'
echo @WATCHDOG
stat -c %Y /home/coffee-house/logs/stack-watchdog.log 2>/dev/null
tail -2 /home/coffee-house/logs/stack-watchdog.log 2>/dev/null
echo @TAILSCALE; tailscale ip -4 2>/dev/null | head -1
echo @END
"""

# StartOS 0.4.0 (2026-07-26): services are LXC under startd, not host podman.
# Paths moved /embassy-data → /media/startos/data; tier-3 marker lives in filebrowser.
START9_SCRIPT = r"""
echo @HOST; hostname
echo @UPTIME; cat /proc/uptime
echo @LOAD; cat /proc/loadavg
echo @NPROC; nproc
echo @CPU
# RAPL energy_uj is root-only; sudo -n works. Sample across the same 1s window
# used for CPU % — package watts, not wall (no BMC meter here).
E1=$(sudo -n cat /sys/class/powercap/intel-rapl:0/energy_uj 2>/dev/null || echo)
D1=$(sudo -n cat /sys/class/powercap/intel-rapl:0:2/energy_uj 2>/dev/null || echo)
read t1 i1 < <(awk '/^cpu /{print $2+$3+$4+$5+$6+$7+$8+$9, $5+$6}' /proc/stat)
sleep 1
read t2 i2 < <(awk '/^cpu /{print $2+$3+$4+$5+$6+$7+$8+$9, $5+$6}' /proc/stat)
E2=$(sudo -n cat /sys/class/powercap/intel-rapl:0/energy_uj 2>/dev/null || echo)
D2=$(sudo -n cat /sys/class/powercap/intel-rapl:0:2/energy_uj 2>/dev/null || echo)
awk -v t=$((t2-t1)) -v i=$((i2-i1)) 'BEGIN{if(t>0)printf "%.1f\n",(t-i)/t*100; else print 0}'
echo @POWER
# lines: package_uj_delta dram_uj_delta  (÷1e6 → watts over the 1s sleep)
echo "${E1} ${E2} ${D1} ${D2}"
echo @MEM; free -b | sed -n '2p;3p'
echo @DISKROOT; df -B1 / | tail -1
# Prefer 0.4 mount; fall back to 0.3 path so a mid-migrate box still reports.
echo @DISKDATA
df -B1 /media/startos/data/package-data 2>/dev/null | tail -1
df -B1 /embassy-data/package-data 2>/dev/null | tail -1
echo @TEMP
for z in /sys/class/thermal/thermal_zone*; do
  echo "$(cat $z/type 2>/dev/null) $(cat $z/temp 2>/dev/null)"
done 2>/dev/null
echo @STARTD; systemctl is-active startd 2>/dev/null || echo unknown
echo @OSVER; start-cli git-info 2>/dev/null | head -1
echo @LXC
# Prefer package stats. On 0.4, stats can fail entirely if ANY LXC is stopped
# (lxc-attach error) — fall back to mountinfo mapping of RUNNING containers.
if start-cli package stats >/tmp/s9-stats.txt 2>/tmp/s9-stats.err; then
  awk -F'|' '
    NR<=3 { next }
    {
      name=$2; id=$3;
      gsub(/^ +| +$/,"",name); gsub(/^ +| +$/,"",id);
      if (name=="" || name ~ /^Name/ || name ~ /^-/) next;
      total++;
      up = (id!="" && id!="N/A") ? 1 : 0;
      if (up) run++;
      print name "|" up "|" id
    }
    END { print "__COUNTS__|" (run+0) "|" (total+0) }
  ' /tmp/s9-stats.txt
else
  # Fallback: only RUNNING LXCs (stopped packages omitted — console shows live set)
  run=0
  for cid in $(sudo -n lxc-ls -1 2>/dev/null); do
    st=$(sudo -n lxc-info -n "$cid" -sH 2>/dev/null || echo ?)
    [ "$st" = "RUNNING" ] || continue
    pid=$(sudo -n lxc-info -n "$cid" -pH 2>/dev/null || true)
    [ -n "$pid" ] || continue
    pkg=$(sudo -n grep -oE '/volumes/[a-zA-Z0-9_-]+/' /proc/$pid/mountinfo 2>/dev/null | head -1 | cut -d/ -f3)
    [ -z "$pkg" ] && pkg=$(sudo -n grep -oE '/logs/[a-zA-Z0-9_-]+' /proc/$pid/mountinfo 2>/dev/null | head -1 | cut -d/ -f3)
    [ -z "$pkg" ] && continue
    echo "${pkg}|1|${cid}"
    run=$((run+1))
  done
  # total ≈ installed count from package list when stats broken
  total=$(start-cli package list 2>/dev/null | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))' 2>/dev/null || echo "$run")
  echo "__COUNTS__|${run}|${total}"
fi
echo @BACKUP
BK=/media/startos/data/package-data/volumes/filebrowser/data/data/sparkmax-backup/hermes/last-backup.txt
# legacy bind-mount path used before 0.4 migrate
[ -f "$BK" ] || BK=/mnt/backup/hermes/last-backup.txt
head -2 "$BK" 2>/dev/null
stat -c %Y "$BK" 2>/dev/null
echo @END
"""

# Containers whose absence is worth an alert (core of the self-hosted stack).
# Lightning (lnd) and Fulcrum are optional — many installs run electrs-only or
# no Lightning at all; treating them as core produced permanent false warnings.
# nextcloud added to KEEP intent 2026-07-26 but not required for critical pages.
START9_CORE = ("bitcoind", "electrs", "nextcloud",
               "gitea", "syncthing", "searxng", "vaultwarden", "mempool")


def _ssh(alias: str, script: str, timeout: int = SSH_TIMEOUT) -> tuple[bool, str, str]:
    cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=6",
           "-o", "StrictHostKeyChecking=accept-new"]
    if SHARED_KEY.is_file():
        cmd += ["-o", "IdentitiesOnly=yes", "-i", str(SHARED_KEY)]
    env = {**os.environ, "HOME": str(Path.home())}
    env.pop("SSH_AUTH_SOCK", None)  # a stale agent socket breaks BatchMode
    try:
        out = subprocess.run(cmd + [alias, "bash", "-s"], input=script,
                             capture_output=True, text=True, timeout=timeout, env=env)
    except (subprocess.TimeoutExpired, OSError) as e:
        return False, "", f"{type(e).__name__}: {str(e)[:160]}"
    if out.returncode != 0 or "@END" not in (out.stdout or ""):
        return False, out.stdout or "", (out.stderr or out.stdout or "ssh failed").strip()[:200]
    return True, out.stdout, ""


def _sections(raw: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    key = None
    for line in raw.splitlines():
        if line.startswith("@"):
            key = line.strip()[1:]
            out[key] = []
        elif key:
            out[key].append(line.rstrip())
    return out


def _first(sec: dict, key: str, default: str = "") -> str:
    vals = [v for v in sec.get(key, []) if v.strip()]
    return vals[0].strip() if vals else default


def _uptime_human(seconds: float) -> str:
    d, rem = divmod(int(seconds), 86400)
    h, rem = divmod(rem, 3600)
    return f"{d}d {h}h" if d else f"{h}h {rem // 60}m"


def ago_human(epoch: float | None) -> str:
    if not epoch:
        return "never"
    delta = datetime.now(timezone.utc).timestamp() - epoch
    if delta < 90:
        return f"{int(delta)}s ago"
    if delta < 5400:
        return f"{int(delta / 60)}m ago"
    if delta < 172800:
        return f"{delta / 3600:.1f}h ago"
    return f"{int(delta / 86400)}d ago"


def _mem_block(lines: list[str]) -> tuple[dict, dict]:
    mem = lines[0].split()   # Mem: total used free shared buff avail
    swap = lines[1].split()  # Swap: total used free
    total, used, avail = int(mem[1]), int(mem[2]), int(mem[6])
    st, su = int(swap[1]), int(swap[2])
    return (
        {"used_gb": round(used / GIB, 1), "total_gb": round(total / GIB, 1),
         "avail_gb": round(avail / GIB, 1), "pct": round(used / total * 100, 1)},
        {"used_gb": round(su / GIB, 1), "total_gb": round(st / GIB, 1),
         "pct": round(su / st * 100, 1) if st else 0.0},
    )


def temp_band(c: float | None, kind: str = "gpu") -> str | None:
    """Map °C → cool|ok|warm|hot for console color (blue/green/yellow/red).

    Cutoffs match each device's existing alert thresholds so the UI band and
    the issues[] warning trip at the same heat, not two different stories.
      pi  → warn ≥75   (throttle risk)
      pkg → warn ≥90   (x86 package / Start9)
      gpu → hot  ≥80   (GB10 comfort band)
      asic → warn ≥62  (Bitaxe/NerdQAxe abort band)
    """
    if c is None:
        return None
    try:
        t = float(c)
    except (TypeError, ValueError):
        return None
    # (cool_lt, ok_lt, warm_lt) — anything ≥ warm_lt is hot
    cuts = {
        "pi": (45.0, 60.0, 75.0),
        "pkg": (55.0, 75.0, 90.0),
        "gpu": (50.0, 70.0, 80.0),
        "asic": (48.0, 56.0, 62.0),
    }.get(kind, (50.0, 70.0, 85.0))
    if t < cuts[0]:
        return "cool"
    if t < cuts[1]:
        return "ok"
    if t < cuts[2]:
        return "warm"
    return "hot"


def _disk_block(line: str) -> dict:
    p = line.split()
    total, used, free = int(p[1]), int(p[2]), int(p[3])
    return {"pct": float(p[4].rstrip("%")),
            "used_gb": round(used / GIB, 1), "total_gb": round(total / GIB, 1),
            "free_gb": round(free / GIB, 1),
            "free_tb": round(free / 1024 ** 4, 2), "mount": p[5] if len(p) > 5 else "/"}


def _pi_pmic_power_w(lines: list[str]) -> dict | None:
    """Sum Pi 5 PMIC rail I×V from `vcgencmd pmic_read_adc` (board estimate, not wall)."""
    curr: dict[str, float] = {}
    volt: dict[str, float] = {}
    # "VDD_CORE_A current(7)=0.56A" / "VDD_CORE_V volt(15)=0.75V"
    pat = re.compile(r"^\s*(\S+)\s+(current|volt)\(\d+\)=([0-9.]+)")
    for line in lines:
        m = pat.match(line)
        if not m:
            continue
        name, kind, val = m.group(1), m.group(2), float(m.group(3))
        # Only strip the trailing _A / _V suffix — mid-name "_V" (DDR_VDD2) must stay
        if kind == "current" and name.endswith("_A"):
            curr[name[:-2]] = val
        elif kind == "volt" and name.endswith("_V"):
            volt[name[:-2]] = val
    if not curr:
        return None
    rails = []
    total = 0.0
    for rail, amps in curr.items():
        v = volt.get(rail)
        if v is None:
            continue
        watts = amps * v
        total += watts
        rails.append({"rail": rail, "a": round(amps, 4), "v": round(v, 4),
                      "w": round(watts, 3)})
    rails.sort(key=lambda r: -r["w"])
    return {
        "power_w": round(total, 2),
        "power_source": "pmic-rails",
        "power_label": "board",
        "power_scale_w": 15.0,  # Pi 5 typical peak board draw for bar fill
        "ext5v_v": round(volt["EXT5V"], 3) if "EXT5V" in volt else None,
        "rails_top": rails[:5],
    }


def _start9_rapl_power_w(line: str) -> dict | None:
    """Parse `E1 E2 D1 D2` µJ samples taken 1s apart → package (+DRAM) watts."""
    parts = line.split()
    if len(parts) < 2:
        return None
    try:
        e1, e2 = float(parts[0]), float(parts[1])
    except ValueError:
        return None
    if e2 < e1:  # counter wrap — skip this sample
        return None
    pkg_w = (e2 - e1) / 1e6  # 1-second window → watts
    dram_w = None
    if len(parts) >= 4 and parts[2] and parts[3]:
        try:
            d1, d2 = float(parts[2]), float(parts[3])
            if d2 >= d1:
                dram_w = (d2 - d1) / 1e6
        except ValueError:
            dram_w = None
    # On this Cannon Lake box DRAM is a sibling domain, not inside package-0
    total = pkg_w + (dram_w or 0.0)
    return {
        "power_w": round(total, 2),
        "power_pkg_w": round(pkg_w, 2),
        "power_dram_w": round(dram_w, 2) if dram_w is not None else None,
        "power_source": "rapl-package+dram",
        "power_label": "CPU+DRAM",
        "power_scale_w": 65.0,  # modest NUC-class package headroom for bar fill
    }


# ---------------------------------------------------------------- Raspberry Pi

def query_pi() -> dict:
    now = datetime.now(timezone.utc)
    base: dict = {"id": "pi", "name": "raspberrypi", "role": "Pi 5 · mirror + watchdog",
                  "kind": "appliance", "iso_ts": now.isoformat(), "ts": now.timestamp()}
    ok, raw, err = _ssh(PI_ALIAS, PI_SCRIPT)
    if not ok:
        base.update({"reachable": False, "error": err, "issues": [
            {"level": "critical", "message": f"Pi unreachable — {err[:90]}"}]})
        return base
    s = _sections(raw)
    base["reachable"] = True
    issues: list[dict] = []
    try:
        base["hostname"] = _first(s, "HOST", "raspberrypi")
        base["uptime"] = _uptime_human(float(_first(s, "UPTIME", "0").split()[0]))
        load = _first(s, "LOAD").split()
        base["load"] = " / ".join(load[:3]) if load else "?"
        base["cores"] = int(_first(s, "NPROC", "4") or 4)
        base["cpu_pct"] = float(_first(s, "CPU", "0") or 0)
        base["mem"], base["swap"] = _mem_block([l for l in s.get("MEM", []) if l.strip()])
        base["disk"] = _disk_block(_first(s, "DISK"))

        temp_raw = _first(s, "TEMP")  # temp=43.3'C
        temp = None
        if "=" in temp_raw:
            try:
                temp = float(temp_raw.split("=")[1].rstrip("'C").rstrip("C"))
            except ValueError:
                temp = None
        base["temp_c"] = temp
        # cool/ok/warm/hot — console paints blue/green/yellow/red from this
        base["temp_band"] = temp_band(temp, "pi")
        throttled = _first(s, "THROTTLE", "throttled=0x0").split("=")[-1]
        base["throttled"] = throttled
        base["throttled_ok"] = throttled in ("0x0", "")

        pmic = _pi_pmic_power_w(s.get("PMIC", []))
        if pmic:
            base.update(pmic)

        services = []
        for line in s.get("SVC", []):
            p = line.split()
            if len(p) == 2:
                services.append({"name": p[0], "state": p[1]})
        base["services"] = services

        mirror_lines = [l for l in s.get("MIRROR", []) if l.strip()]
        if len(mirror_lines) >= 2:
            newest = float(mirror_lines[0])
            base["mirror"] = {
                "newest_ts": newest, "newest_ago": ago_human(newest),
                "size_gb": round(int(mirror_lines[1]) / GIB, 2),
                "files": int(mirror_lines[2]) if len(mirror_lines) > 2 else None,
                "age_h": round((now.timestamp() - newest) / 3600, 1),
            }
            if len(mirror_lines) > 3:
                oldest = float(mirror_lines[3])
                span_d = max((newest - oldest) / 86400, 0)
                base["mirror"]["oldest_ts"] = oldest
                base["mirror"]["oldest_ago"] = ago_human(oldest)
                base["mirror"]["span_days"] = round(span_d, 1)
                # No prune rule exists in pi-setup-mirror.sh, so growth is linear in
                # retention. Project a year out to make the SD-card ceiling concrete.
                if span_d >= 1:
                    base["mirror"]["growth_gb_per_yr"] = round(
                        base["mirror"]["size_gb"] / span_d * 365, 1)
        wd = [l for l in s.get("WATCHDOG", []) if l.strip()]
        if wd:
            try:
                wts = float(wd[0])
                base["watchdog"] = {"last_ts": wts, "last_ago": ago_human(wts),
                                    "tail": wd[-1][:120] if len(wd) > 1 else "",
                                    "age_m": round((now.timestamp() - wts) / 60, 1)}
            except ValueError:
                pass
        base["tailscale_ip"] = _first(s, "TAILSCALE")

        # ---- headroom: this box is a mirror+watchdog appliance, so the interesting
        # number is how much of it is unused, not how much is used.
        try:
            overhead_mb = round(int(_first(s, "OVERHEAD", "0") or 0) / 1024, 1)
        except ValueError:
            overhead_mb = 0.0
        base["overhead_mb"] = overhead_mb
        base["headroom"] = {
            "cpu_idle_pct": round(100 - base["cpu_pct"], 1),
            "mem_free_gb": base["mem"].get("avail_gb"),
            "disk_free_gb": base["disk"].get("free_gb"),
            # Desktop stack is reclaimable on a box with no monitor attached.
            "reclaimable_mb": overhead_mb,
        }

        # ---- judgments (the console's job is to say what is wrong, not just show numbers)
        if temp is not None and temp >= 75:
            issues.append({"level": "warning", "message": f"Pi CPU {temp}°C — throttle risk"})
        if not base["throttled_ok"]:
            issues.append({"level": "warning",
                           "message": f"Pi throttled flags {throttled} (power/heat)"})
        if base["disk"]["pct"] >= 85:
            issues.append({"level": "warning",
                           "message": f"Pi disk {base['disk']['pct']}% full"})
        for svc in services:
            if svc["name"] in ("ssh", "cron") and svc["state"] != "active":
                issues.append({"level": "critical",
                               "message": f"Pi {svc['name']} is {svc['state']}"})
            # Syncthing is the vault replica leg — degraded, not fatal.
            elif svc["name"] == "syncthing" and svc["state"] != "active":
                issues.append({"level": "warning",
                               "message": f"Pi syncthing is {svc['state']} — vault replica stalled"})
        wdog = base.get("watchdog")
        if wdog and wdog["age_m"] > 25:
            issues.append({"level": "warning",
                           "message": f"Pi stack-watchdog silent {wdog['last_ago']} (runs */10m)"})
        mirror = base.get("mirror")
        if mirror and mirror["age_h"] > 48:
            issues.append({"level": "warning",
                           "message": f"Pi mirror stale — newest file {mirror['newest_ago']}"})
        # pi-setup-mirror.sh prunes to keep-daily=14 (matching restic tier-1). If the
        # retention span drifts well past that, the prune has silently stopped and the
        # mirror is back to growing unbounded on a 57G SD card.
        if mirror and mirror.get("span_days") is not None:
            if mirror["span_days"] > PI_MIRROR_KEEP_DAYS + 7:
                issues.append({
                    "level": "warning",
                    "message": (f"Pi mirror retention {mirror['span_days']}d "
                                f"> {PI_MIRROR_KEEP_DAYS}d policy — prune not running")})
    except (KeyError, IndexError, ValueError) as e:
        base["parse_error"] = f"{type(e).__name__}: {e}"
        issues.append({"level": "warning", "message": f"Pi parse error: {e}"})
    base["issues"] = issues
    return base


# ---------------------------------------------------------------- Start9

def query_start9() -> dict:
    now = datetime.now(timezone.utc)
    base: dict = {"id": "start9", "name": "cosmic-charcoal", "role": "Start9 · self-hosted stack",
                  "kind": "appliance", "iso_ts": now.isoformat(), "ts": now.timestamp()}
    ok, raw, err = _ssh(START9_ALIAS, START9_SCRIPT)
    if not ok:
        base.update({"reachable": False, "error": err, "issues": [
            {"level": "critical", "message": f"Start9 unreachable — {err[:90]}"}]})
        return base
    s = _sections(raw)
    base["reachable"] = True
    issues: list[dict] = []
    try:
        base["hostname"] = _first(s, "HOST", "cosmic-charcoal")
        base["uptime"] = _uptime_human(float(_first(s, "UPTIME", "0").split()[0]))
        load = _first(s, "LOAD").split()
        base["load"] = " / ".join(load[:3]) if load else "?"
        base["cores"] = int(_first(s, "NPROC", "8") or 8)
        base["cpu_pct"] = float(_first(s, "CPU", "0") or 0)
        rapl = _start9_rapl_power_w(_first(s, "POWER"))
        if rapl:
            base.update(rapl)
        base["mem"], base["swap"] = _mem_block([l for l in s.get("MEM", []) if l.strip()])
        base["disk_root"] = _disk_block(_first(s, "DISKROOT"))
        data_line = _first(s, "DISKDATA")
        base["disk_data"] = _disk_block(data_line) if data_line else None

        # Hottest thermal zone wins, but keep its name — "87°C" means nothing
        # until you know it is the CPU package and not a chipset sensor.
        hottest, hot_name = None, ""
        for line in s.get("TEMP", []):
            parts = line.split()
            if len(parts) != 2:
                continue
            try:
                milli = float(parts[1])
            except ValueError:
                continue
            if hottest is None or milli > hottest:
                hottest, hot_name = milli, parts[0]
        base["temp_c"] = round(hottest / 1000, 1) if hottest else None
        base["temp_source"] = hot_name
        # x86 package runs hotter than Pi/GPU; band uses pkg cutoffs (≥90 = hot)
        base["temp_band"] = temp_band(base["temp_c"], "pkg")

        base["startd"] = _first(s, "STARTD", "unknown")
        base["os_git"] = _first(s, "OSVER", "")[:12] or None
        # Prefer @LXC (0.4). Fall back to legacy @PODMAN/@NAMES if an old
        # script somehow still runs against a 0.3 box.
        svc_list = []
        running = total = 0
        lxc_lines = [l for l in s.get("LXC", []) if l.strip()]
        if lxc_lines:
            for line in lxc_lines:
                parts = line.split("|")
                if len(parts) < 2:
                    continue
                if parts[0] == "__COUNTS__":
                    try:
                        running, total = int(parts[1]), int(parts[2])
                    except (ValueError, IndexError):
                        pass
                    continue
                name = parts[0].strip()
                try:
                    up = parts[1].strip() == "1"
                except IndexError:
                    up = False
                cid = parts[2].strip() if len(parts) > 2 else ""
                status = f"LXC {cid}" if up else "stopped"
                svc_list.append({"name": name, "status": status, "up": up,
                                 "core": name in START9_CORE, "lxc_id": cid or None})
            if not total and svc_list:
                total = len(svc_list)
                running = sum(1 for x in svc_list if x["up"])
        else:
            # 0.3.x podman path (kept for rollback diagnostics only)
            pod = [l for l in s.get("PODMAN", []) if l.strip()]
            running = int(pod[0]) if pod else 0
            total = int(pod[1]) if len(pod) > 1 else running
            for line in s.get("NAMES", []):
                if "|" not in line:
                    continue
                name, status = line.split("|", 1)
                short = name.replace(".embassy", "").strip()
                up = status.strip().lower().startswith("up")
                svc_list.append({"name": short, "status": status.strip(), "up": up,
                                 "core": short in START9_CORE})
        svc_list.sort(key=lambda x: (not x["core"], not x["up"], x["name"]))
        base["containers"] = {"running": running, "total": total, "runtime": "lxc"}
        # lxc_services is canonical; podman_services kept so fleet_links/console
        # keep working without a coordinated rename.
        base["lxc_services"] = svc_list
        base["podman_services"] = svc_list
        base["core_down"] = [x["name"] for x in svc_list if x["core"] and not x["up"]]
        missing = [c for c in START9_CORE if not any(x["name"] == c for x in svc_list)]
        base["core_missing"] = missing

        bk = [l for l in s.get("BACKUP", []) if l.strip()]
        if bk:
            backup = {"label": bk[0].strip()}
            if len(bk) > 1:
                backup["stamp"] = bk[1].strip()
            try:
                bts = float(bk[-1])
                backup["ts"] = bts
                backup["ago"] = ago_human(bts)
                backup["age_h"] = round((now.timestamp() - bts) / 3600, 1)
            except ValueError:
                pass
            base["backup"] = backup

        # ---- judgments
        if base["startd"] != "active":
            issues.append({"level": "critical",
                           "message": f"Start9 startd {base['startd']} — services will not run"})
        if running == 0:
            issues.append({"level": "critical", "message": "Start9: no containers running"})
        elif total and running < total:
            # KEEP pause intentionally leaves many installed packages stopped.
            # Only warn when even the core set is incomplete — not on lean installs.
            core_up = sum(1 for x in svc_list if x.get("core") and x.get("up"))
            if core_up < len(START9_CORE) and running < max(8, len(START9_CORE)):
                issues.append({"level": "warning",
                               "message": f"Start9 only {running}/{total} containers up "
                                          f"(core {core_up}/{len(START9_CORE)})"})
        for name in base["core_down"]:
            issues.append({"level": "critical", "message": f"Start9 core service down: {name}"})
        for name in missing:
            # Missing from live list is OK when package stats fallback only lists RUNNING;
            # only warn if startd is up and we expected them in svc_list with up=False.
            if any(x["name"] == name for x in svc_list):
                issues.append({"level": "warning", "message": f"Start9 core service missing: {name}"})
            elif running == 0:
                issues.append({"level": "warning", "message": f"Start9 core service missing: {name}"})
        dd = base.get("disk_data")
        if dd and dd["pct"] >= 85:
            issues.append({"level": "warning",
                           "message": f"Start9 package-data {dd['pct']}% full "
                                      f"({dd['free_tb']} TB free)"})
        if base["disk_root"]["pct"] >= 80:
            issues.append({"level": "warning",
                           "message": f"Start9 root overlay {base['disk_root']['pct']}% "
                                      f"— only {base['disk_root']['total_gb']} G total"})
        if base["swap"]["used_gb"] >= 6:
            issues.append({"level": "warning",
                           "message": f"Start9 swap {base['swap']['used_gb']} G in use"})
        if base["temp_c"] and base["temp_c"] >= 90:
            issues.append({"level": "warning",
                           "message": f"Start9 {base['temp_source'] or 'CPU'} "
                                      f"{base['temp_c']}°C — near throttle"})
        bkp = base.get("backup")
        if bkp and bkp.get("age_h") is not None and bkp["age_h"] > 36:
            issues.append({"level": "warning",
                           "message": f"Start9 tier-3 backup stale — {bkp['ago']}"})
    except (KeyError, IndexError, ValueError) as e:
        base["parse_error"] = f"{type(e).__name__}: {e}"
        issues.append({"level": "warning", "message": f"Start9 parse error: {e}"})
    base["issues"] = issues
    return base


# ---------------------------------------------------------------- Bitaxe / NerdQAxe (AxeOS HTTP)

FLEET_JSON = Path.home() / "scripts/nerdqaxe/fleet.json"
AXE_TIMEOUT = 4.0
# Known ASICs (wiki/projects/home-bitcoin-miners.md) — IPs move after mesh/DHCP.
MINER_MAC = {
    "Tantalizing": "f0:9e:9e:20:91:30",
    "Tantalizing-2": "f0:f5:bd:4b:d6:10",
}
# Abort bands from wiki/projects/home-bitcoin-miners.md (2026-08-19)
MINER_GUARDS = {
    "Tantalizing": {"temp": 64.0, "power": 95.0, "rssi": -91},
    "Tantalizing-2": {"temp": 62.0, "power": 16.0, "rssi": -80},
}


def _load_miner_catalog() -> list[dict]:
    try:
        data = json.loads(FLEET_JSON.read_text())
        return list(data.get("miners") or [])
    except (OSError, json.JSONDecodeError):
        return [
            {"name": "Tantalizing", "model": "NerdQAxe++", "last_ip": "192.168.50.66"},
            {"name": "Tantalizing-2", "model": "Bitaxe Ultra", "last_ip": "192.168.50.104"},
        ]


def _arp_ip_for_mac(mac: str) -> str | None:
    want = mac.lower().replace("-", ":")
    try:
        raw = Path("/proc/net/arp").read_text()
    except OSError:
        return None
    for line in raw.splitlines()[1:]:
        parts = line.split()
        if len(parts) >= 4 and parts[3].lower() == want:
            return parts[0]
    return None


def _axe_get(url: str) -> dict | None:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=AXE_TIMEOUT) as r:
            return json.loads(r.read().decode("utf-8", errors="replace"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError, ValueError):
        return None


def _fmt_hashrate(ghs: float | None) -> str | None:
    if ghs is None:
        return None
    try:
        v = float(ghs)
    except (TypeError, ValueError):
        return None
    if v >= 1000:
        return f"{v / 1000.0:.2f} TH/s"
    return f"{v:.0f} GH/s"


def _worker_short(user: str | None, hostname: str | None) -> str | None:
    if hostname:
        return hostname
    if not user:
        return None
    # wallet.Worker.suffix@pool → Worker
    core = user.split("@", 1)[0]
    parts = core.split(".")
    if len(parts) >= 2:
        return parts[1]
    return core[-18:]


def query_miner(spec: dict) -> dict:
    now = datetime.now(timezone.utc)
    name = spec.get("name") or "miner"
    ip = spec.get("last_ip") or ""
    mac = (spec.get("mac") or MINER_MAC.get(name) or "").lower()
    found = _arp_ip_for_mac(mac) if mac else None
    if found:
        ip = found
    sid = "miner-" + re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    base: dict = {
        "id": sid, "name": name, "role": spec.get("model") or "AxeOS miner",
        "kind": "miner", "ip": ip, "iso_ts": now.isoformat(), "ts": now.timestamp(),
        "open_url": f"http://{ip}/" if ip else None,
    }
    issues: list[dict] = []
    info = _axe_get(f"http://{ip}/api/system/info") if ip else None
    # NerdQAxe++ AxeOS UI is often too slow; Spark proxy talks to the same API.
    if info is None and name == "Tantalizing":
        info = _axe_get("http://127.0.0.1:8766/api/system/info")
        if info:
            base["via"] = "nerdqaxe-proxy"
    if not info:
        base.update({"reachable": False, "error": f"AxeOS {ip or '?'} no /api/system/info",
                     "issues": [{"level": "warning",
                                 "message": f"{name} unreachable — {ip or 'no IP'}"}]})
        return base
    base["reachable"] = True
    hr = info.get("hashRate")
    try:
        base["hashrate_ghs"] = round(float(hr), 1) if hr is not None else None
    except (TypeError, ValueError):
        base["hashrate_ghs"] = None
    base["hashrate"] = _fmt_hashrate(base.get("hashrate_ghs"))
    try:
        base["temp_c"] = float(info["temp"]) if info.get("temp") is not None else None
    except (TypeError, ValueError):
        base["temp_c"] = None
    base["temp_band"] = temp_band(base["temp_c"], "asic")
    try:
        base["power_w"] = round(float(info["power"]), 1) if info.get("power") is not None else None
    except (TypeError, ValueError):
        base["power_w"] = None
    try:
        base["wifi_rssi"] = int(info["wifiRSSI"]) if info.get("wifiRSSI") is not None else None
    except (TypeError, ValueError):
        base["wifi_rssi"] = None
    base["ssid"] = info.get("ssid") or None
    base["pool"] = info.get("stratumURL") or None
    base["worker"] = _worker_short(info.get("stratumUser"), info.get("hostname"))
    base["hostname"] = info.get("hostname") or name
    base["frequency"] = info.get("frequency")
    base["core_mv"] = info.get("coreVoltage")
    base["fan_rpm"] = info.get("fanrpm")
    base["fw"] = info.get("version") or None
    try:
        v = info.get("voltage")
        base["rail_mv"] = round(float(v), 0) if v is not None else None
    except (TypeError, ValueError):
        base["rail_mv"] = None
    guard = MINER_GUARDS.get(name, {"temp": 64.0, "power": 95.0, "rssi": -90})
    if base["temp_c"] is not None and base["temp_c"] >= guard["temp"]:
        issues.append({"level": "warning",
                       "message": f"{name} ASIC {base['temp_c']}°C ≥ {guard['temp']} abort"})
    if base["power_w"] is not None and base["power_w"] >= guard["power"]:
        issues.append({"level": "warning",
                       "message": f"{name} {base['power_w']} W ≥ {guard['power']} abort"})
    if base["wifi_rssi"] is not None and base["wifi_rssi"] <= -88:
        issues.append({"level": "warning",
                       "message": f"{name} Wi‑Fi {base['wifi_rssi']} dBm — dropouts likely"})
    pool = (base.get("pool") or "").lower()
    if pool and "parasite" not in pool and "public-pool" not in pool:
        issues.append({"level": "warning", "message": f"{name} pool {base['pool']}"})
    if base.get("hashrate_ghs") is not None and base["hashrate_ghs"] < 50:
        issues.append({"level": "warning", "message": f"{name} hashrate {base['hashrate']} (stalled?)"})
    base["issues"] = issues
    return base


PARASITE_API = "https://parasite.space/api/user/"
PARASITE_PAGE = "https://parasite.space/user/"


def _fmt_hs(hs) -> str | None:
    try:
        v = float(hs)
    except (TypeError, ValueError):
        return None
    if v >= 1e12:
        return f"{v / 1e12:.2f} TH/s"
    if v >= 1e9:
        return f"{v / 1e9:.0f} GH/s"
    if v >= 1e6:
        return f"{v / 1e6:.0f} MH/s"
    return f"{v:.0f} H/s"


def _fmt_diff(v) -> str | None:
    if v is None or v == "":
        return None
    if isinstance(v, str) and not v.replace(".", "", 1).isdigit():
        return v
    try:
        n = float(v)
    except (TypeError, ValueError):
        return str(v)
    if n >= 1e9:
        return f"{n / 1e9:.2f}G"
    if n >= 1e6:
        return f"{n / 1e6:.2f}M"
    if n >= 1e3:
        return f"{n / 1e3:.2f}k"
    return f"{n:.0f}"


def _parasite_user(wallet: str) -> dict | None:
    if not wallet:
        return None
    url = PARASITE_API + wallet
    req = urllib.request.Request(
        url, headers={"Accept": "application/json", "User-Agent": "spark-console/miners"}
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read().decode("utf-8", errors="replace"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    workers = []
    for w in data.get("workerData") or []:
        if not isinstance(w, dict):
            continue
        last = w.get("lastSubmission")
        last_ts = None
        try:
            last_ts = float(last)
        except (TypeError, ValueError):
            last_ts = None
        workers.append({
            "name": w.get("name") or "",
            "id": w.get("id") or "",
            "hashrate_hs": _float_or_none(w.get("hashrate")),
            "hashrate": _fmt_hs(w.get("hashrate")),
            "best_diff": _fmt_diff(w.get("bestDifficulty")),
            "last_share_ts": last_ts,
            "last_share": ago_human(last_ts) if last_ts else (str(last) if last else None),
            "uptime": None if w.get("uptime") in (None, "N/A", "") else str(w.get("uptime")),
        })
    return {
        "wallet": wallet,
        "url": PARASITE_PAGE + wallet,
        "hashrate_hs": _float_or_none(data.get("hashrate")),
        "hashrate": _fmt_hs(data.get("hashrate")),
        "workers_n": data.get("workers"),
        "last_share": data.get("lastSubmission"),
        "best_diff": _fmt_diff(data.get("bestDifficulty")) or data.get("bestDifficulty"),
        "uptime": data.get("uptime"),
        "workers": workers,
    }


def _float_or_none(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _match_parasite_worker(farm: dict | None, name: str) -> dict | None:
    if not farm:
        return None
    want = (name or "").lower()
    for w in farm.get("workers") or []:
        if (w.get("name") or "").lower() == want:
            return w
    for w in farm.get("workers") or []:
        ident = (w.get("id") or "").lower()
        if want and want in ident:
            return w
    return None


def query_miners() -> dict:
    now = datetime.now(timezone.utc)
    catalog = _load_miner_catalog()
    try:
        wallet = json.loads(FLEET_JSON.read_text()).get("wallet") or ""
    except (OSError, json.JSONDecodeError):
        wallet = "bc1qv2n8e0d6nq9rafwyqxy44r3d6mj8e2zazy0psx"
    farm = _parasite_user(wallet)
    rows = []
    for spec in catalog:
        m = query_miner(spec)
        w = _match_parasite_worker(farm, spec.get("name") or m.get("name") or "")
        if w:
            m["parasite"] = w
            issues = list(m.get("issues") or [])
            last_ts = w.get("last_share_ts")
            if last_ts and (now.timestamp() - last_ts) > 600:
                issues.append({
                    "level": "warning",
                    "message": f"{m.get('name')} last Parasite share {w.get('last_share')}",
                })
            m["issues"] = issues
        rows.append(m)
    out = {
        "iso_ts": now.isoformat(),
        "miners": rows,
        "reachable": any(m.get("reachable") for m in rows),
        "parasite": farm,
        "parasite_url": (farm or {}).get("url") or (PARASITE_PAGE + wallet if wallet else None),
    }
    return out


if __name__ == "__main__":
    import sys
    which = sys.argv[1] if len(sys.argv) > 1 else "both"
    if which in ("pi", "both"):
        print(json.dumps(query_pi(), indent=2))
    if which in ("start9", "both"):
        print(json.dumps(query_start9(), indent=2))
    if which in ("miners", "both"):
        print(json.dumps(query_miners(), indent=2))
