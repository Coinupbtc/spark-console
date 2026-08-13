# Feel-pass — Spark Console layout + look (2026-08-12)

Track: feel-pass · desktop 1600×1000 + iPhone 390×844 + landscape 844×390

## 5 bullets

1. **Cold open:** Pulse still leads with the story (“N things need you” / quiet watch). Header chip names what’s actually serving (`DS4F 0731`). Brand mark has a quiet spark, not a new theme.
2. **Primary path:** Control is grouped by function — Inference / Hermes / Apps / Meta — with This desk (Blender/Godot) pinned on top. Jobs opens on the job list, not backups. Fleet opens on the machines; the power bill is last.
3. **Break / filter:** `/` focuses Find a service / Find a job. Empty filter copy is “no services match”. Start / Open / Stop colors unchanged. Stopped tiles sit at 70% opacity so running ones pop.
4. **Loading / empty / phone:** LIVE pill still goes `…` → `live`. Quiet watch empty state on Needs you. iPhone: bottom tabs, 2-col hosts, grouped Control bays, 44px actions. Landscape 844×390 uses the phone chrome (not the desktop header).
5. **Would a stranger keep this?** **yes** — same instrument panel, but you can find a function without scanning 20 identical tiles. Residual: landscape 390px-tall still clips host gauges below the fold (scroll); Jobs backups sit under the table so they are one scroll away.

## States

- loading: live pill → “…” then “live”; hero “Checking the fleet…”
- empty: Quiet watch / no services match / no jobs match / ComfyUI on-demand
- error: header err chip (phone: toast above tab bar)
- success: serving chip + grouped bays + job list first

## Evidence

- `data/screenshots/ux-desktop-pulse-2026-08-12.png`
- `data/screenshots/ux-desktop-control-2026-08-12.png`
- `data/screenshots/ux-desktop-jobs-2026-08-12.png`
- `data/screenshots/ux-desktop-fleet-2026-08-12.png`
- `data/screenshots/ux-iphone-pulse-2026-08-12.png`
- `data/screenshots/ux-iphone-control-tall-2026-08-12.png`
- `data/screenshots/ux-iphone-landscape-pulse-2026-08-12.png`

## Direction shipped

Function layout: Control bays + desk dock; Jobs list first; Fleet machines then bill.
Look: same Sora / IBM Plex / glyphs; serving chip; spark on the bolt; group color rails.
Backup: `console.html.bak-2026-08-12-pre-layout`
