---
name: p6-ml-proof
description: Phase 6 of AgriPulse — prove external-data value with a LightGBM demand-forecast ablation (with vs without external features) and KMeans agro-climatic district segmentation, tracked in MLflow.
---
# P6 — Proof of value (the impressive headline)

## Forecast lift experiment (src/agripulse/ml/forecast_lift.py)
- Data: gold.v_ds_training. Target: next-week arrivals_t_sum per district×commodity (also try modal price).
- Split: time-based — train up to T−52w, validate next 26w, test last 26w. No random split.
- Models: (A) baseline seasonal-naive (same week last year), (B) LightGBM with market lags + calendar only, (C) LightGBM + weather + agro + forecast + geo features.
- Metrics: MAPE/WAPE (WAPE primary — robust to zeros), RMSE, per-commodity and per-state breakdown. Bootstrap CI or rolling-origin backtest (≥4 folds) so the lift is credible.
- SHAP top features plot for (C) → shows which external sources matter.
- Track all runs in MLflow (Databricks built-in) experiment `/agripulse/forecast_lift`; log params, metrics, plots.
- Output table `ml.forecast_lift_results` + chart into docs/results.md: "WAPE B → C = X% lift".
- If lift is small, report honestly and analyse where it helps (e.g. rain-fed districts, kharif crops) — still a strong story.

## Segmentation (src/agripulse/ml/segmentation.py)
- Input gold.district_profile: long-run rainfall, rainfall variability, heat days, soil wetness, crop mix shares, market price volatility.
- Standardise → KMeans k=4..10 → choose by silhouette + interpretability; also try PCA viz.
- Name segments (e.g. "rain-fed cotton belt, high drought risk"), profile table `ml.district_segments`, map plot with geopandas choropleth (PNG in docs).
- Write 1-paragraph "how marketing would use this" per segment.

## Done when
results.md has: WAPE A/B/C, lift %, folds, top SHAP features, k chosen + silhouette, segment map PNG. Commit `feat(ml): forecast lift ablation and district segmentation`.
