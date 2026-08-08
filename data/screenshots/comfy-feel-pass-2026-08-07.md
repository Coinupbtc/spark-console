# Feel-pass — ComfyUI card (sparkDash 1.6 parity) — 2026-08-07

Track: feel-pass (~cold open Jobs tab)

## Bullets
1. Offline empty state is calm: chip `offline`, copy says on-demand :8188 — not an error scream.
2. Open link still one click (`ComfyUI UI` → loopback :8188).
3. When live, card shows job blocks (Running / Queued) with Cancel/Remove, footprint `res · steps · sampler · nodes`, model+LoRA list, progress %, queue ETA, last finished, installed inventory.
4. Loading: chip `…` → filled on first `/api/comfy` poll (~5s refresher / 10s client poll).
5. Error: cancel against down Comfy returns clear “Cannot reach ComfyUI” (no false success).

## States
- loading: placeholders until poll
- empty/offline: “Not reachable — ComfyUI is on-demand”
- error: cancel alert / API `{ok:false}`
- success: live job blocks + progress

## Screenshots
- `comfy-jobs-tab-2026-08-07.png` (Jobs tab, Comfy offline)
- source tweet reference: MiaAI sparkDash 1.6 ComfyUI monitor
