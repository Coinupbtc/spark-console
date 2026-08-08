#!/usr/bin/env python3
"""
Launcher catalog — every web UI on the fleet, in one clickable grid.

Local apps are derived from the SAME registry the Services board uses
(service_control.get_services_catalog), so anything an agent registers with
register-console-service.sh shows up here automatically — no second list to
keep in sync. Start9/Pi links are composed from the live pollers so a tile is
only advertised when the container behind it is actually up.

Local links are returned as {port, path} and the browser builds the URL from
its own hostname: the console is reached over the LAN (192.168.50.123) and
127.0.0.1 links would break for every viewer.
"""
from __future__ import annotations

import json

from service_control import get_services_catalog

START9_WEB = "https://cosmic-charcoal.local"
PI_LAN = "192.168.50.152"

# Start9 package id → (label, path under startd reverse proxy).
# 0.4 also exposes unique ports on cosmic-charcoal.local — path proxies may
# 404; Interfaces tab is authoritative. Keep common paths for one-click try.
START9_UI = {
    "mempool": ("Mempool", "/mempool/"),
    "gitea": ("Gitea", "/gitea/"),
    "nextcloud": ("Nextcloud", "/nextcloud/"),
    "vaultwarden": ("Vaultwarden", "/vaultwarden/"),
    "syncthing": ("Syncthing (S9)", "/syncthing/"),
    "searxng": ("SearXNG", "/searxng/"),
    "jellyfin": ("Jellyfin", "/jellyfin/"),
    "photoview": ("Photoview", "/photoview/"),
    "immich": ("Immich", "/immich/"),
    "cryptpad": ("CryptPad", "/cryptpad/"),
    "filebrowser": ("File Browser", "/filebrowser/"),
    "lnbits": ("LNbits", "/lnbits/"),
    "btcpayserver": ("BTCPay", "/btcpayserver/"),
    "ghost-legacy": ("Ghost (legacy)", "/ghost/"),
    "lndg": ("LNDg", "/lndg/"),
    "albyhub": ("Alby Hub", "/albyhub/"),
    "jam": ("JoinMarket Jam", "/jam/"),
    "sparrow-webtop": ("Sparrow", "/sparrow-webtop/"),
    "nostr-rs-relay": ("Nostr relay", "/nostr-rs-relay/"),
    "ntfy": ("ntfy", "/ntfy/"),
    "start9-pages": ("Start9 Pages", "/start9-pages/"),
    "robosats": ("RoboSats", "/robosats/"),
    "stash": ("Stash", "/stash/"),
    "hermes-agent": ("Hermes Agent (S9)", "/hermes-agent/"),
    "uptime-kuma": ("Uptime Kuma", "/uptime-kuma/"),
    "my-speed": ("MySpeed", "/my-speed/"),
    "lightning-jet": ("Lightning Jet", "/lightning-jet/"),
    "balanceofsatoshis": ("BoS", "/balanceofsatoshis/"),
}

# Local extras that are not systemd-registered services
LOCAL_EXTRA = [
    {"label": "Syncthing (node1)", "port": 8384, "path": "/", "detail": "vault sync UI"},
]


def query_links(services: dict | None = None, start9: dict | None = None,
                pi: dict | None = None) -> dict:
    groups: list[dict] = []

    # ---- this Spark (node1): registry entries that expose a port
    live = {s["id"]: s for s in (services or {}).get("services", [])}
    local: list[dict] = []
    for sid, meta in get_services_catalog().items():
        port = meta.get("port")
        if not port:
            probe = meta.get("probe")
            if isinstance(probe, (list, tuple)) and probe and "://" in str(probe[0]):
                try:
                    port = int(str(probe[0]).split(":")[2].split("/")[0])
                except (IndexError, ValueError):
                    port = None
        open_url = meta.get("open_url")
        # Need a local port OR an explicit public URL (Tailscale serve, etc.)
        if not port and not open_url:
            continue
        state = live.get(sid, {})
        tile = {
            "id": sid,
            "label": meta.get("label", sid),
            "detail": meta.get("detail", ""),
            "status": "up" if state.get("active") else ("down" if state else "unknown"),
            "group": meta.get("group", "apps"),
        }
        if open_url:
            tile["url"] = open_url
        if port:
            tile["port"] = int(port)
            tile["path"] = "/"
        local.append(tile)
    for extra in LOCAL_EXTRA:
        local.append({"id": f"extra-{extra['port']}", "label": extra["label"],
                      "detail": extra["detail"], "port": extra["port"],
                      "path": extra["path"], "status": "unknown", "group": "apps"})
    local.sort(key=lambda x: (x["status"] != "up", x["label"].lower()))
    groups.append({"host": "sparkmax-10ef", "kind": "local", "label": "This Spark · node1",
                   "links": local})

    # ---- Start9
    s9: list[dict] = []
    running = {}
    if start9 and start9.get("reachable"):
        running = {s["name"]: s for s in (
            start9.get("lxc_services") or start9.get("podman_services") or [])}
        s9.append({"id": "start9-dash", "label": "StartOS dashboard", "detail": "cosmic-charcoal",
                   "url": START9_WEB + "/", "status": "up" if start9.get("startd") == "active"
                   else "down", "group": "start9"})
    for name, (label, path) in START9_UI.items():
        if running and name not in running:
            continue
        up = running.get(name, {}).get("up") if running else None
        s9.append({"id": f"s9-{name}", "label": label, "detail": name,
                   "url": START9_WEB + path,
                   "status": "up" if up else ("down" if up is False else "unknown"),
                   "group": "start9"})
    s9.sort(key=lambda x: (x["status"] != "up", x["label"].lower()))
    groups.append({"host": "cosmic-charcoal", "kind": "start9", "label": "Start9 · self-hosted",
                   "links": s9,
                   "note": None if start9 and start9.get("reachable") else "host unreachable"})

    # ---- Pi
    pi_links: list[dict] = []
    if pi:
        svc = {s["name"]: s["state"] for s in pi.get("services", [])}
        pi_links.append({"id": "pi-syncthing", "label": "Syncthing (Pi)",
                         "detail": "mirror node UI",
                         "url": f"http://{PI_LAN}:8384/",
                         "status": "up" if svc.get("syncthing") == "active" else "down",
                         "group": "pi"})
        if pi.get("tailscale_ip"):
            pi_links.append({"id": "pi-tailscale", "label": "Pi over Tailscale",
                             "detail": pi["tailscale_ip"],
                             "url": f"http://{pi['tailscale_ip']}:8384/",
                             "status": "unknown", "group": "pi"})
    groups.append({"host": "raspberrypi", "kind": "pi", "label": "Raspberry Pi 5",
                   "links": pi_links,
                   "note": None if pi and pi.get("reachable") else "host unreachable"})

    total = sum(len(g["links"]) for g in groups)
    up = sum(1 for g in groups for l in g["links"] if l["status"] == "up")
    return {"groups": groups, "counts": {"total": total, "up": up}}


if __name__ == "__main__":
    import fleet_nodes
    from service_control import list_services
    data = query_links(list_services(), fleet_nodes.query_start9(), fleet_nodes.query_pi())
    print(json.dumps(data["counts"]))
    for g in data["groups"]:
        print(f"\n== {g['label']} ({len(g['links'])}) {g.get('note') or ''}")
        for l in g["links"][:30]:
            target = l.get("url") or f":{l['port']}{l['path']}"
            print(f"   {l['status']:8} {l['label'][:28]:28} {target}")
