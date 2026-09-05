# TP2 vs TP1 setup lanes

Two lanes. TP2 occupies **both** GPUs as one named job. TP1 occupies **one** GPU; mix the other GPU independently.

## TP2 — both GPUs, one job

| Chip | Job |
|------|-----|
| DeepSeek Vision | Chat + native images, tensor-parallel |
| Qwen3.8-Flash | Chat + vision, tensor-parallel |
| GLM-5.3 | Chat, tensor-parallel |
| Videos | MiniMax H3 one clip across both GPUs (faster wall time) |

A TP2 switch parks TP1 occupants. A TP1 load parks a live TP2 job.

## TP1 — pick a node, then the other

Load one occupant per GPU. Telegram talks to a **local chat LLM** if one is loaded (Qwen Flash wins). Music + video with no chat LLM → cloud fallback.

| Mix | Result |
|-----|--------|
| Qwen Flash + Music | Chat + one singer |
| Qwen Flash + Videos TP1 | Chat + one-Spark H3 (slower than Videos TP2) |
| Videos TP1 + Music | Two media engines, no local chat |
| Two Qwen Flash TP1 | Two independent chats. **Faster pair = TP2 Flash**, not this |
| Two Music | Two singers, two songs at once. **Not** a faster one song |
| Two Videos TP1 | Two independent H3s. **Faster one clip = TP2 Videos** |

CLI: `spark-occupy.sh load flash1 --node n1` then `load music3 --node n2`. Two copies: `--node both` (Flash or Music only).

## Music: no Videos-style TP2

Official MiniMax Music 3 serve is **pipeline** on one box with two CUDA devices: GPU 0 = AR/RVQ, GPU 1 = flow-matching DiT + decode. Tensor-parallel (`tp_size≠1`) **refuses to boot**.

A DGX Spark has **one** GPU. Colocating both stages on that GPU is the working recipe (~27 GiB). Two Sparks as two copies doubles **throughput** (two songs), not **latency** of one song.

The only plausible one-song speedup on two hosts is **pipeline-parallel** (AR on Spark 1, DiT on Spark 2 over the fabric). That overlay is not shipped: `start-two-sparks.sh` syncs weights and exits until a cross-node rank spawn exists. It would not be H3 Ulysses TP2.

Do not add a Music TP2 chip that pretends to be Videos TP2.
