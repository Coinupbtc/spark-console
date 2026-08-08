# Feel-pass — Control Start→Open swap (2026-08-08)

Track: feel-pass · desktop 1440 + iPhone 390

## 5 bullets

1. Cold open Control: service tiles still scannable; primary actions sit in the same bottom-left slot.
2. Primary path: stopped tile shows mint **Start**; after start (or already up with a site), that slot becomes sky **Open ↗** — one click to the site; **Stop** stays beside it.
3. No-site daemons (Ollama, Hermes gateways): running shows **Stop** only — no fake Open.
4. Loading: after Start, board refreshes and Start flips to Open when the catalog has a URL/port.
5. Plain English: labels are Start / Open / Stop — no jargon.

## States

- loading: op banner while start/stop runs
- empty: filter “no services match”
- error: header err chip on failed action
- success: Open appears for up+URL services (incl. CardArb via open_url)

## Evidence

- `data/screenshots/ux-control-open-desktop-2026-08-08.png`
- `data/screenshots/ux-control-open-iphone-2026-08-08.png`
