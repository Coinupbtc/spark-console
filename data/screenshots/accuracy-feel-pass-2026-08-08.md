# Feel-pass — console numbers accuracy (2026-08-08)

Track: feel-pass (~10 min cold open as stranger)

## 5 bullets

1. Electricity defaults to **Measured** (GPU / PMIC / RAPL) — dollar figures match sensors, not an outlet guess.
2. **Bill basis** select makes Wall estimate optional; idle wall W only appears in that mode.
3. Note shows age (“just now” / Ns ago) so the panel feels present-tense.
4. Live tick power (node1 ~39–67 W) feeds the energy log every ~30s without an extra nvidia-smi.
5. Comfy running bar no longer invents a completion % — indeterminate + elapsed only.

## States

- loading: “fetching /api/energy-cost”
- empty: “no history yet” per cell
- error: API error string in cell
- success: per-node 24h/30d $ + coverage hours

## Load

- console RSS ~55–70 MB, ~1–2% CPU after restart
- `/api/energy-cost` ~50ms cold / ~4ms cached (20s TTL)
- no extra GPU poll for energy (reuses 1 Hz hist)

## Screenshot

See `accuracy-feel-pass-2026-08-08.png` if headless chromium produced one.

## Follow-up (whole console, not just $)

1. Pulse + Fleet gauges share 1 Hz hist; Fleet patches in place each second.
2. Overview overlays hist so structure refresh doesn’t flash stale GPU W/%.
3. Comfy ETA only when finished-job averages exist; no fake %.
4. Polls split: overview 10s, comfy 10s, tokens/energy 30s, tick 1s.
5. Live ages (“now” / “Ns ago”) on Pulse feet and Fleet Spark cards.

