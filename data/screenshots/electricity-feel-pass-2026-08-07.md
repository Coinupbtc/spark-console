# Feel-pass — Electricity cost panel — 2026-08-07

Track: feel-pass (Fleet tab)

## Bullets
1. Rate editor is obvious: `$/kWh` number field defaults to 0.145, edits live-update the card.
2. Per-node cells show W + $/hr · $/d · $/mo with source labels (GPU draw / board PMIC / RAPL).
3. Sparks total + Fleet total sit as highlighted summary cells.
4. Power rows on each node card also carry `· $X/d` so you see cost without scrolling to the top.
5. Empty/offline nodes say “offline / no meter” instead of $0 lies.

## States
- loading/connecting: offline / no meter
- empty: — when no watts
- success: live projections at current rate
- edit: type new rate → card updates on input; Pulse/Fleet suffixes refresh on change

## Screenshot
- `electricity-2026-08-07.png`
