---
name: p7-chaos-benchmarks
description: Phase 7 of AgriPulse — inject data faults to measure quality-gate detection rate and benchmark incremental vs full-refresh runtime and optimisation.
---
# P7 — Chaos data test + benchmarks (turn "we have DQ" into a measured result)

## Fault injection (src/agripulse/chaos/fault_injector.py)
Write to a separate dev schema copy (never prod). Inject 50 faults across 8 types, each logged in `ops.chaos_log(fault_id, type, table, expected_detector)`:
nulls in keys · tmax<tmin · negative rain · NASA -999 · price order violation · 100× price spike · duplicate rows · schema drift (renamed/dropped column) · stale feed (stop a source 3 days) · volume drop (90% rows missing).
Run pipeline → match detections (expectations/quarantine/freshness/volume alerts) to faults → `docs/results.md`: detected X/50 by type, bad rows reaching gold = N. Fix any miss and re-run (report before/after).

## Benchmarks (notebooks/90_benchmarks.py)
- Full refresh vs incremental daily run: duration, rows processed (from ops.pipeline_runs).
- Delta optimisations: liquid clustering / OPTIMIZE + ZORDER on (district_id, date) — query latency of a typical DS query before/after.
- API ingestion: sequential vs batched multi-coordinate + threadpool calls → wall time.
Record all in results.md table with method.

## Done when
Detection table + benchmark table in results.md with real numbers. Commit `test(chaos): fault injection and performance benchmarks`.
