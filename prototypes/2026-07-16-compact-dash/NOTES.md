# Compact dashboard prototype — reactions log (2026-07-16)

Prototype: single-screen, no-scroll console for the dual-Spark stack. FAKE DATA.
Serve: `python3 -m http.server 8899` in this dir → http://192.168.50.123:8899

## Requirements so far
- REQ: single screen, zero scroll ("No scroll compact, useful DASHBOARD")
- REQ: both Sparks — GPU, CPU, memory, swap
- REQ: models running (what/where/RAM/speed)
- REQ: projects in one place
- SCOPE: real backend = extend existing dgx-performance-dashboard (:8085), not new service

## Directions
- A: dense grid — node meter strips top, models+alerts middle, project tiles bottom
- B: two node columns with fat bars, project pills strip on bottom
- C: 12-cell KPI strip on top, two ledgers (models/alerts | projects/crons) below

## Reactions
- 2026-07-16: picked **B** (node columns). REQ: use the entire screen space. REQ: readable fonts/colors (bumped base 12→14px, fatter bars). REQ: keep Alerts panel (was only in A/C — merged into B). UK: wants a **Todo / Home list** on the dashboard — personal items (home chores) mixed with stack items. SCOPE: todo backing store TBD — options: simple JSON file the dashboard edits, vault page, or Hermes kanban.db (~/.hermes/kanban.db exists, SQLite). SCOPE: alerts source = alertbot/journal tail (define during real wiring).
- Layout now: header · two node columns (bars + models) · bottom row = Alerts | Todo·Home | Projects grid.

## Status: disposable — delete after design approved
