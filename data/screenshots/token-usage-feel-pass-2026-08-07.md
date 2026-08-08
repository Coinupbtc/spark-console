# Feel-pass — LLM token usage graph + stats — 2026-08-07

Track: feel-pass (Jobs tab cold open)

## Bullets
1. Graph reads first: 14-day in/out area chart with MM-DD ticks and period totals.
2. Stats are plain English blocks — Per session / Per API call with mean, median, mode, p90/n.
3. Mode shows `~50k (×95)` so it’s obvious it’s a bucket center, not a fake exact mode.
4. Loading: `…` placeholders → filled on `/api/token-usage`; chart says “collecting…” if <2 days.
5. Existing totals (24h / all-time / cache / calls) still sit under the new stats — no lost info.

## States
- loading: placeholders + empty chart text
- empty: zeros / — when no sessions
- error: poll fails silently leave prior paint (same as before)
- success: live series + stats

## Screenshot
- `token-usage-2026-08-07.png`
