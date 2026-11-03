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
| Table | Silver rows | Quarantined | Pass rate |
|---|---|---|---|
| districts | 641 | 0 | 100% |
| weather_daily | 3,471,861 | 3 | ~100% |
| agro_climate_daily | 3,471,015 | 641 | 99.98% |
| mandi_prices_daily | 1,604,158 | 171,868 | **90.32%** |

Rules that fired in quarantine: mandi `price_order` 125,114 · `valid_key` 50,668 (district not matched) · `price_positive` 3,828 · `date_not_future` 4 · agro `nasa_fill` 641 (NASA −999 fill, one date per district) · weather `tmax_ge_tmin` 3, `temp_present` 3.
**Rule bug found by looking at the data:** the plan's first-draft `temp_range` (−10…55 °C for both tmax and tmin) quarantined **21,502 real rows** from Himalayan districts (J&K 14,168 · Himachal 5,480 · Uttarakhand 1,671 · Sikkim 176 · Arunachal 7; tmin to −45 °C). Bounds widened to physical limits (tmax −50…55, tmin −60…50) → weather quarantine 21,502 → 3.
Expectations live: 6 (weather) + 5 (agro) + 4 (mandi) + 1 warn (price_spike) + 3 (districts) + 1 uniqueness warn + schema contract (fails the update) = 20.
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
