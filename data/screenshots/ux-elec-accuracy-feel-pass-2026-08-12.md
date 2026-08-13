# Feel-pass — Electricity accuracy (2026-08-12)

Track: feel-pass (Fleet tab)

## 5 bullets

1. **Cold open:** Rate default is Ameren MO summer energy **15.24¢/kWh** (Jun 1 2026 tariff), not the old 14.5¢ guess. Note names the tariff. $9.19/mo customer charge is not allocated to the lab.
2. **Primary path:** Wall estimate uses this fleet’s idle GPU floor (**11 W** median nvidia-smi), 240 W PSU cap, Pi PMIC×1.12. Each Spark cell shows ≈W avg and GPU kWh so you can sanity-check against a Kill-A-Watt.
3. **Break / incomplete:** 30d node2/Pi/Start9 stay **building** when coverage is short. Pace is last-24h × 30 with gaps = 0 W (does not invent draw for offline hours).
4. **Loading / empty:** still “fetching /api/energy-cost”; Start9 labeled **RAPL (not wall)**.
5. **Would a stranger keep this?** **yes** for a bill-shaped number. Residual: Start9 RAPL still understates the outlet (disks/PSU); Spark idle 50 W is CX7-up conservative until a USB-C meter reading.

## States

- loading: fetching /api/energy-cost
- empty/building: partial hours + “building” on 30d
- success: 24h complete fleet; pace from clock 24h
- error: API error string in cell

## Evidence

- `data/screenshots/ux-elec-accuracy-fleet-2026-08-12.png`
- Live wall 24h: fleet **3.06 kWh ≈ 128 W** · **$0.47/24h** · **≈$14/mo pace** @ $0.1524/kWh
