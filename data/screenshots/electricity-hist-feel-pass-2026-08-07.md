# Feel-pass — Historical electricity (24h/30d) — 2026-08-07

Track: feel-pass (Fleet tab)

## Bullets
1. Card shows trailing **24h $** and **30d $** from integrated kWh — not live W×24.
2. Coverage hours + “partial/building” make incomplete windows honest.
3. $/kWh still editable live; $ recalculates client-side from kWh.
4. Sparks total + Fleet total highlighted; Pi/Start9 start at $0 until samples accumulate.
5. Power rows show `· $X/24h` from the same historical window.

## States
- loading: “fetching /api/energy-cost”
- empty/building: partial hours marked
- success: complete 24h Sparks from CSV backfill; 30d already complete (~701h)
- rate edit: instant $ refresh, no refetch required

## Screenshot
- `electricity-hist-2026-08-07.png`
