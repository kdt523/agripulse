---
name: p3-gold-features
description: Phase 3 of AgriPulse — build gold.district_week feature table joining weather, agro-climate, mandi and geo data for the data-science team, incrementally, with a feature dictionary.
---
# P3 — Gold feature table (the "collaborate with Data Science" proof)

## gold.district_week (grain: district_id × iso_week × commodity)
Target: `arrivals_t_sum` (weekly mandi arrivals, demand proxy) and `modal_price_avg`.
Features:
- weather: precip_sum_7d/28d, precip_anomaly vs 5-yr same-week mean, tmax_avg, heat_days(>40°C), dry_spell_len, et0_sum, gdd (growing degree days)
- agro: soil_wetness_root_avg, soil_moisture_anomaly, solar_sum
- forecast: precip_forecast_next_7d (from is_forecast rows — leakage-safe only for future weeks)
- market: lag prices/arrivals 1,2,4,52 weeks, price volatility 4w, n_markets reporting
- geo/crop: crop share of commodity in district, area_km2, state
- calendar: week_of_year, season (kharif/rabi/zaid)
Build with Spark SQL window functions; write via MERGE; incremental by recomputing only weeks touched since last watermark (+ lookback for rolling windows).

Also `gold.district_profile` (one row per district, long-run climate + crop mix) for segmentation.

## Leakage rules (write them in docs)
Rolling features use only past data; forecast features use forecast issued before the week; target never in features.

## Done when
- No null keys; row counts + feature null-rate table in docs/results.md
- `docs/feature_dictionary.md` (name, description, source, formula) — generate from table comments.
- SQL view `gold.v_ds_training` ready for P6. Commit `feat(gold): district_week feature table`.
