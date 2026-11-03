"""Feature groups for the forecast-lift ablation. B = market + calendar; C = B + external (weather, agro, geo)."""

from __future__ import annotations

TARGET = "target_arrivals_next_week"

MARKET = [
    "arrivals_t_sum", "arrivals_lag_1w", "arrivals_lag_2w", "arrivals_lag_4w", "arrivals_lag_52w",
    "modal_price_avg", "price_lag_1w", "price_lag_2w", "price_lag_4w", "price_lag_52w",
    "price_volatility_4w", "n_markets", "commodity_share_52w", "next_week_arrivals_lag_52w",
]  # fmt: skip
CALENDAR = ["week_of_year"]  # target week is week_of_year + 1; the model learns this offset
CATEGORICAL_BASE = ["commodity_std"]

EXTERNAL_WEATHER = ["precip_sum_7d", "precip_sum_28d", "precip_anomaly", "tmax_avg", "heat_days", "dry_spell_len", "gdd"]
EXTERNAL_AGRO = ["soil_wetness_root_avg", "soil_moisture_anomaly", "solar_sum"]
EXTERNAL_GEO = ["area_km2"]
CATEGORICAL_GEO = ["state"]

# Dropped because they are structurally NULL in the 2013-2017 window: et0_sum (NASA has no ET0), precip_forecast_next_7d.
DROPPED_ALL_NULL = ["et0_sum", "precip_forecast_next_7d"]

FEATURES_B = MARKET + CALENDAR + CATEGORICAL_BASE
FEATURES_C = FEATURES_B + EXTERNAL_WEATHER + EXTERNAL_AGRO + EXTERNAL_GEO + CATEGORICAL_GEO
EXTERNAL_ONLY = EXTERNAL_WEATHER + EXTERNAL_AGRO + EXTERNAL_GEO + CATEGORICAL_GEO

CATEGORICAL = CATEGORICAL_BASE + CATEGORICAL_GEO
