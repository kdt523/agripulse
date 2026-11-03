---
name: p2-silver-quality
description: Phase 2 of AgriPulse — Lakeflow Declarative Pipeline (DLT) silver layer that cleans/standardises bronze data with expectations, quarantine tables and a data-quality scorecard.
---
# P2 — Silver + data quality

## Pipeline
`pipelines/dlt_silver.py` (Lakeflow Declarative Pipelines, Python API `import dlt` / `pyspark.pipelines` whichever the workspace supports), deployed via `resources/pipelines.yml`, serverless, target catalog agripulse schema silver.

Tables:
- silver.weather_daily(district_id, date, tmax_c, tmin_c, precip_mm, et0_mm, rh_pct, wind_kmh, is_forecast BOOL)
- silver.agro_climate_daily(district_id, date, solar_mj_m2, soil_wetness_root, soil_wetness_top, t2m_c, precip_mm)
- silver.mandi_prices_daily(district_id, market_id, commodity_std, date, min_price_inr_qtl, max_price_inr_qtl, modal_price_inr_qtl, arrivals_t)
- silver.districts(district_id, district_name_std, state_std, lat, lon, area_km2, crop mix columns)

Standardisation: units (°C, mm, INR/quintal, tonnes), commodity name normalisation map, district_id via ops.district_name_map, dates to DATE, trim/upper names, dedupe on keys keeping latest _ingested_at.

## Expectations (≥10, mix of warn / drop / fail)
- valid_key: district_id IS NOT NULL AND date IS NOT NULL → drop
- tmax_ge_tmin: tmax_c >= tmin_c → drop
- temp_range: tmax_c BETWEEN -10 AND 55 → drop
- precip_nonneg: precip_mm >= 0 → drop
- nasa_fill: solar_mj_m2 != -999 → drop (quarantine)
- price_order: min_price <= modal_price <= max_price → drop
- price_positive: modal_price > 0 → drop
- price_spike: modal_price < 20 × rolling median (computed in a view) → warn
- date_not_future (except forecast rows) → drop
- schema contract: expected columns present → fail pipeline
- uniqueness check via aggregation view → warn

Quarantine: for each silver table build `silver.<t>_quarantine` = rows failing rules with `failed_rules ARRAY<STRING>` (implement by computing rule flags in a view, then routing). Never silently lose data.

## Scorecard
`src/agripulse/quality/scorecard.py` → `ops.dq_scorecard(table, date, rows, pass_rows, fail_rows, pass_rate, rules_failed MAP<STRING,LONG>, completeness_pct)`; also read DLT event log (`event_log()` TVF) for expectation metrics.

## Done when
- Pipeline runs green; quarantine has real rows (there will be — NASA -999s, Agmarknet price-order errors); scorecard populated; numbers noted in docs/results.md (e.g. "% Agmarknet rows quarantined").
- Commit `feat(silver): DLT pipeline with expectations, quarantine, DQ scorecard`.
