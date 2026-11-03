---
name: p1-bronze-ingestion
description: Phase 1 of AgriPulse — PySpark/Python ingestion of 4 external sources (Open-Meteo, NASA POWER, Agmarknet, geo/crop) into bronze Delta tables with incremental watermarks and backfill.
---
# P1 — Bronze ingestion (biggest gap closer: PySpark, Delta Lake, APIs, external data)

## Design rules
- Bronze = raw but typed-enough: keep original payload columns + `_ingested_at`, `_source`, `_run_id`, `_request_params`.
- Land raw JSON/CSV into Volume `agripulse.bronze.landing/<source>/dt=YYYY-MM-DD/` first, then load to Delta (replayable).
- Write with Delta MERGE on natural keys. Partition/cluster: use liquid clustering (`CLUSTER BY`) on (district_id, date) where supported.
- Incremental: read `ops.ingest_watermarks`, fetch from last_date+1 to today; `--backfill start end` overrides.
- Parallelism: batch API calls in Python with a ThreadPool (respect rate limits); build Spark DF from results. Don't call APIs inside Spark UDFs.

## Step 1 — geo + crop (do first: gives district list)
- Download DataMeet India districts GeoJSON → compute centroid lat/lon per district (shapely) → `bronze.geo_districts(district_id, district_name, state_name, lat, lon, area_km2, geometry_wkt)`.
- ICRISAT district-level crop area/production CSV → `bronze.crop_area_raw`.
- Build `silver`-ready mapping table `ops.district_name_map` (fuzzy match names across sources with rapidfuzz; manual overrides CSV in repo). Name mismatches across Indian datasets are the #1 real-world issue — document it.

## Step 2 — Open-Meteo weather
- Archive API for history, Forecast API for next 16 days. Variables: temperature_2m_max/min, precipitation_sum, rain_sum, et0_fao_evapotranspiration, relative_humidity_2m_mean, wind_speed_10m_max, soil_moisture (forecast hourly→daily agg if available).
- Multi-coordinate requests (comma-separated lat/lon lists) to cut call count. Table `bronze.weather_daily_raw` + `bronze.weather_forecast_raw`.

## Step 3 — NASA POWER agro-climate (remote sensing/agronomic indicators)
- Daily point API, community=AG, params: ALLSKY_SFC_SW_DWN (solar), GWETROOT (root-zone soil wetness), GWETTOP, T2M, PRECTOTCORR, RH2M. Fill value -999 → keep raw, flag in silver.
- `bronze.nasa_power_raw`.

## Step 4 — Agmarknet mandi prices (market intelligence)
- data.gov.in API (paged, offset/limit, api-key from secret). Plus historical CSV backfill if API depth limited.
- Fields: state, district, market, commodity, variety, arrival_date, min_price, max_price, modal_price (+ arrivals_tonnes when available).
- `bronze.mandi_prices_raw`. Focus commodities relevant to agri-inputs demand: paddy, wheat, maize, soybean, cotton, tomato, onion, potato (configurable).

## Step 5 — ingestion framework polish
- One `BaseIngestor` class: fetch() → land() → load_bronze() → update_watermark() → log_run(). Each source subclasses.
- Notebook/job entry: `notebooks/10_ingest.py` with widget `source`, `mode` (incremental|backfill), `start`, `end`.

## Tests
- unit: response parsing per source using saved fixture JSON in tests/fixtures; watermark logic; MERGE idempotency (local Delta).
- integration (marked, runs on Databricks): ingest 3 districts × 7 days end-to-end.

## Done when
- Backfill complete: weather + NASA ≥5 yrs × ~700 districts; mandi ≥3 yrs; record row counts in docs/results.md.
- Running incremental twice in a row → second run inserts 0 duplicate rows (prove with counts).
- ops.pipeline_runs populated for each source. Commit: `feat(ingest): bronze ingestion for 4 external sources`.
