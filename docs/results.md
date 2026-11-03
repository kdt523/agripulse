# Results (measured numbers only)

Every number below was measured on the Databricks workspace (Free Edition, serverless). Rows marked **TBD** have not been measured yet.

## Scope decisions forced by Free Edition / data access (honest notes)
| Constraint | Measured / observed | What was done |
|---|---|---|
| `api.data.gov.in` unreachable from Databricks serverless (egress probe, `ConnectionError`) | probe job `agripulse_probe` | Mandi history loaded from the public Agmarknet archive (iancovert/Agmarknet on GitHub, 2008–Jun 2017, real government data incl. arrivals). API ingestor kept for ADF/local use. |
| Open-Meteo free quota (~10k weighted calls/day; weight = locations × days/14) | 25-location × 1-year request timed out / 429; 10 × 1-year works (3,650 rows in ~60 s) | Quota-aware, resumable catch-up with per-district watermarks, 10-district batches, hourly throttle. Starts 2026-05-01; NASA POWER carries the multi-year history. |
| Mandi archive ends June 2017 | max(arrival_date) = 2017-06-22 | Forecast-lift experiment runs on the 2013–2017 window, where mandi, NASA POWER (2009–2017) and geo overlap. |
| ICRISAT crop-area CSV needs a manual form export | not downloaded | Crop mix is derived from mandi arrivals shares (`commodity_share_52w`, `share_<crop>` in `gold.district_profile`). Loader `load_crop_area_csv` is ready for the export. |

## Bronze ingestion (P1)
| Table | Rows | Districts | Date range |
|---|---|---|---|
| bronze.geo_districts | 641 | 641 | static (Census 2011) |
| bronze.nasa_power_raw | 3,471,656 | 641 | 2009-01-01 → 2026-10-30 (2009–2017 + 2021–2026) |
| bronze.mandi_prices_raw | 1,776,026 | 468 (source spellings) | 2013-01-01 → 2017-06-22, 6 commodities |
| bronze.weather_daily_raw (Open-Meteo) | in progress (catch-up job) | see below | 2023+ test slices, then 2026-05-01 → |
| bronze.weather_forecast_raw | 48 (3-district test) | 3 | 16-day forecast |

**Idempotency proof (re-running the same window):** weather 3 districts × 7 days: run 1 inserted 21, run 2 inserted **0** (total stayed 21). NASA: 21 → 0. Forecast: 48 → 0. Weather 10 districts × 2023: re-run inserted **0**.
NASA POWER backfill throughput: 1,364,689 rows (641 districts, 2021-01-01 → 2026-10-30) in ~4 min; 2,106,967 rows (2009–2017) in one job run.

### District name matching (the #1 real-world issue)
Distinct mandi (state, district) spellings: 472 → exact 337, fuzzy-in-state 69, unmatched 66. After adding 24 manual overrides (`docs/district_overrides.csv`): exact 337, fuzzy 69, override 24, **unmatched 42**. Share of mandi **rows** with no district: **14.1% → 2.85%** after overrides.

## Silver + data quality (P2)
Final pipeline (all rules below live). Counts after the last full run:

| Table | Silver rows | Quarantined | Pass rate |
|---|---|---|---|
| districts | 641 | 0 | 100% |
| weather_daily | 3,472,561 | 3 | ~100% |
| agro_climate_daily | 3,471,015 | 641 | 99.98% |
| mandi_prices_daily | 1,604,109 | 171,917 | **90.32%** |

Rules that fired in quarantine: mandi `price_order` 125,114 · `valid_key` 50,668 (district not matched, 2.85% of rows) · `price_positive` 3,828 · `arrivals_plausible` 51 · `price_spike_extreme` 6 · `date_not_future` 4 · agro `nasa_fill` 641 (NASA -999 fill, one date per district) · weather `tmax_ge_tmin` 3, `temp_present` 3, `temp_range` 3. A row can fail several rules, so counts overlap.
Expectations live: weather 6, agro-climate 5, mandi 6 hard + 1 warn, districts 3, plus a bronze uniqueness warn and the bronze schema contract (fails the update) = **23**.

**Three rules were corrected after looking at the data:**
1. The plan's first-draft `temp_range` (-10...55 degC for both tmax and tmin) quarantined **21,502 real rows** from Himalayan districts (J&K 14,168 - Himachal 5,480 - Uttarakhand 1,671 - Sikkim 176 - Arunachal 7; tmin to -45 degC). Bounds widened to physical limits (tmax -50...55, tmin -60...50): weather quarantine 21,502 -> 3.
2. `arrivals_plausible` added: 43 archive rows report more than 100,000 t/day in one market (99.9th percentile is ~11,400 t; one maize row says 716,144 t). 51 rows quarantined.
3. `price_spike_extreme` (>=50x the trailing median -> quarantine) added after the chaos test; its first version also quarantined 2,568 real rows because the rolling median included zero prices (median 0 made every price look like a spike). Computing the median over positive prices only cut that to **6 rows, all genuine entry errors** (e.g. rice at 200,000 INR/quintal against a 1,400 median).
Scorecard table: `ops.dq_scorecard` (per table per day).

## Gold (P3)
| Metric | Value |
|---|---|
| gold.district_week rows | 177,083 (427 districts × 6 commodities, 2012-12-31 → 2017-06-19) |
| null keys / duplicate grain keys | 0 / 0 |
| gold.district_profile rows | 641 |
| gold.v_ds_training rows (with next-week target) | 160,263 |
| Full build time (serverless) | 56.8 s |

Feature null rates (%): precip_sum_7d 0.0 · precip_anomaly 0.5 · soil_moisture_anomaly 0.5 · arrivals_lag_1w 9.5 · arrivals_lag_52w 37.0 (first year + sparse reporting) · commodity_share_52w 1.0 · et0_sum 100 (NASA-only period has no ET0) · precip_forecast_next_7d 100 (historical weeks; forecast only for current week by design).
Dictionary: `docs/feature_dictionary.md`.

## Orchestration, monitoring, alerting (P4)
| Item | Result |
|---|---|
| Daily workflow `agripulse_daily` | geo gate (Mondays) -> ingest weather / NASA / forecast / mandi-landing (parallel, 2 retries, timeouts) -> name map -> silver pipeline -> gold -> DQ scorecard -> freshness check. Schedule 06:30 IST (quartz cron, `Asia/Kolkata`), failure email, queueing on. Deployed and validated; **not yet run on its schedule, so 0 scheduled days measured (target was >=7)**. |
| Freshness SLAs (days) | weather 3 · nasa_power 5 · forecast 1 · mandi static archive (NULL, never breaches). The plan's 1/3/2/0 would have breached on healthy data because the sources publish with a 2-4 day lag. |
| Normal check | Real finding: `dq` alert for `mandi_prices_daily` - pass rate 90.3% < 95% gate. |
| **Forced stale-feed test** (`sla_override=weather=-1`) | **Fired:** `freshness / weather / lag 2d exceeds SLA -1d` written to `ops.alert_log`. Slack delivery: **not delivered (no webhook configured)** - `delivered=false` is recorded honestly. |
| Airflow | `airflow/docker-compose.yml` + DAG `agripulse_daily` (DatabricksRunNowOperator -> PythonSensor freshness gate -> SLA-miss callback). Syntax validated; see run status below. |
| Azure Data Factory | `adf/` pipeline `pl_agmarknet_copy` (REST -> ADLS Gen2 CSV -> Databricks run-now Web activity), linked services, daily trigger. JSON validated; **not deployed (needs an Azure subscription)**. |

## Catalogue (P5)
Column comments applied: **273**; tables/columns without a comment (system-managed objects excluded): **0**. Tags set per table: `layer`, `source`, `refresh`, `pii=false`, `owner=kd`. `docs/DATA_CATALOG.md` generated from Unity Catalog `information_schema`.

## Proof of value (P6)
Setup: LightGBM, next-week arrivals per district x commodity, 4-fold rolling-origin backtest (13-week validation + 13-week test per fold, expanding train; the last two folds are the final 26-week holdout), 37,370 test rows, WAPE primary. Window 2013-2017 (the span where mandi, NASA POWER and geo overlap). Numbers are from the final pipeline.

| Model | WAPE | MAPE | RMSE |
|---|---|---|---|
| A seasonal naive | 72.8% | - | - |
| B LightGBM market lags + calendar | 49.0% | 165.2% | 6,000 |
| C B + weather + agro-climate + geo | **48.5%** | 169.3% | 5,984 |

**B -> C WAPE lift: +0.9% (95% bootstrap CI over district x commodity series: -1.1% to +3.0%). The effect is not distinguishable from zero.** Per fold: -0.5%, +4.2%, 0.0%, +0.3%. Both LightGBM models cut error by about a third versus seasonal naive (72.8% -> 49.0%), and that gain comes from the market lags, not from external data.
By commodity (lift %): soybean +4.4 - wheat +0.7 - maize +0.5 - moong +0.3 - tur -0.4 - rice -1.2. By season: kharif +3.7 - zaid +0.8 - rabi -1.1. By rainfall tercile: mid-rain +1.3 - low-rain +0.8 - high-rain -0.1. These slices were named in the plan (kharif / rain-fed), but they are several comparisons on one dataset and are all inside plausible noise, so treat them as hypotheses. The one pattern that is consistent across all runs is kharif > rabi.
Top SHAP features (model C): arrivals_t_sum, arrivals_lag_1w, arrivals_lag_2w, next_week_arrivals_lag_52w, arrivals_lag_4w, then `state`, commodity_share_52w. External weather features rank below the market lags.
**How stable is the estimate?** The point estimate moved with data-cleaning rules: -0.35% (before the `arrivals_plausible` rule), +2.3% (after it), +0.9% (final, after the `price_spike_extreme` fix). Every one of those has a confidence interval that includes zero, so the honest reading is "no detectable lift in this window", and the sensitivity itself is evidence that outliers in the target matter more than the external features do.
Caveats: the weather source in this window is NASA POWER (0.5 degree grid, no ET0), which is coarse for district-level rainfall; the mandi arrivals series is noisy (WAPE ~49% even for the best model); 2013-2017 is the only window with all sources, so there is no out-of-period check.

### Segmentation
**8 segments** (k chosen by silhouette over k=4..10; **silhouette 0.287 at k=8**, weak-to-moderate separation, and k=6 and k=10 score within 0.01, so k is not sharply identified). 641 districts; PCA explains 25% + 13% of variance. Sizes 42 / 218 / 71 / 140 / 43 / 98 / 10 / 19. Segments are named from their centroids with a suggested marketing use each: hot arid wheat belt (218), high-rainfall mixed-crop (140), high-rainfall rice belt (98), semi-arid maize belt (71), semi-arid soybean belt (43), cool semi-arid with no mandi data (42), arid green-gram belt (19), semi-arid tur belt (10). See `docs/img/segment_profiles.csv` and `docs/img/segment_map.png`. Crop mix is mandi-arrivals based (ICRISAT not loaded), so the 42 districts without mandi data are grouped by climate only. The chosen k changed between runs (7, 6, 8) as the input data was cleaned, so the segmentation is sensitive to the data.

## Chaos data test (P7)
Method: a 30-district sandbox copy of bronze (`chaos_bronze`, 2015-07..2016-12; the real tables are never touched), the silver pipeline runs on the copy, and 50 faults (10 types x 5) are injected and traced by a marker to quarantine rows, silver rows or a failed update. Schema drift runs the pipeline a second time and must fail. Stale-feed and volume-drop faults are checked at **logic level** (the same pure functions the workflow uses, plus 3 negative controls each with 0 false positives), not through a live pipeline run.

| Run | Detected | Bad rows reaching gold | Missed |
|---|---|---|---|
| **before** fixes | **49 / 50** | **6** | null `market` key (1) - flowed into silver; 5 x 100x price spikes - only flagged by a warn rule, so all 5 reached silver |
| **after** fixes (final) | **50 / 50** | **0** | none |

Detected by type (after): null_key 5/5 - tmax_lt_tmin 5/5 - negative_rain 5/5 - nasa_fill 5/5 - price_order 5/5 - price_spike_100x 5/5 - duplicate_row 5/5 - schema_drift 5/5 - stale_feed 5/5 (logic) - volume_drop 5/5 (logic).
Fixes: `market_id` is now NULL when the market name is missing (`concat_ws` silently skipped NULLs, so `valid_key` could not fire) and a hard `price_spike_extreme` rule was added (see Silver). Clean-sandbox quarantine is unchanged by the fixes (774 mandi rows before and after), i.e. no new false positives there. "Reached gold" means the bad row remained in silver un-quarantined and not deduplicated; gold consumes silver as is. Duplicates are caught by `dq_duplicate_keys` and removed by the dedupe step (0 survive).
