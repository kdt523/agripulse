"""Data-quality rules as SQL expressions: single source of truth for the DLT pipeline, quarantine and tests.

Each table has a dict {rule_name: SQL predicate that is TRUE for a good row}. `severity` says how the DLT
pipeline reacts: drop (row goes to quarantine), warn (kept, counted), fail (pipeline stops).
"""

from __future__ import annotations

# --- silver.weather_daily -------------------------------------------------------------------------------------
WEATHER_RULES = {
    "valid_key": "district_id IS NOT NULL AND date IS NOT NULL",
    "temp_present": "tmax_c IS NOT NULL AND tmin_c IS NOT NULL",
    "tmax_ge_tmin": "tmax_c >= tmin_c",
    # Bounds are physical limits for India, not the plan's first-draft [-10, 55]: that range rejected 21,502 real
    # Himalayan winter rows (J&K, Himachal, Uttarakhand, Sikkim; tmin down to -45 C). See docs/results.md.
    "temp_range": "tmax_c BETWEEN -50 AND 55 AND tmin_c BETWEEN -60 AND 50",
    "precip_nonneg": "precip_mm IS NULL OR precip_mm >= 0",
    "date_not_future": "is_forecast OR date <= current_date()",
}

# --- silver.agro_climate_daily --------------------------------------------------------------------------------
AGRO_RULES = {
    "valid_key": "district_id IS NOT NULL AND date IS NOT NULL",
    "nasa_fill": "solar_mj_m2 IS NOT NULL AND solar_mj_m2 != -999",  # -999 is POWER's fill value
    "soil_wetness_range": "soil_wetness_root IS NULL OR soil_wetness_root BETWEEN 0 AND 1",
    "precip_nonneg": "precip_mm IS NULL OR precip_mm >= 0",
    "date_not_future": "date <= current_date()",
}

# --- silver.mandi_prices_daily --------------------------------------------------------------------------------
MANDI_RULES = {
    "valid_key": "district_id IS NOT NULL AND market_id IS NOT NULL AND commodity_std IS NOT NULL AND date IS NOT NULL",
    "price_positive": "modal_price_inr_qtl > 0",
    "price_order": "min_price_inr_qtl <= modal_price_inr_qtl AND modal_price_inr_qtl <= max_price_inr_qtl",
    "date_not_future": "date <= current_date()",
}
# warn-only (rows kept, but counted): need a rolling median computed in a view
MANDI_WARN_RULES = {
    "price_spike": "rolling_median_price IS NULL OR modal_price_inr_qtl < 20 * rolling_median_price",
}

# --- silver.districts -----------------------------------------------------------------------------------------
DISTRICT_RULES = {
    "valid_key": "district_id IS NOT NULL",
    "valid_coords": "lat BETWEEN 6 AND 38 AND lon BETWEEN 68 AND 98",  # India bounding box
    "area_positive": "area_km2 > 0",
}

# --- schema contracts: pipeline FAILS if a bronze table lost one of these columns (schema drift) -------------
BRONZE_CONTRACTS = {
    "weather_daily_raw": ["district_id", "date", "temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
    "weather_forecast_raw": ["district_id", "date", "issued_date", "temperature_2m_max", "precipitation_sum"],
    "nasa_power_raw": ["district_id", "date", "allsky_sfc_sw_dwn", "gwetroot", "t2m", "prectotcorr"],
    "mandi_prices_raw": ["state", "district", "market", "commodity", "arrival_date", "min_price", "max_price", "modal_price"],
    "geo_districts": ["district_id", "district_name", "state_name", "lat", "lon", "area_km2"],
}


def missing_columns(table: str, columns: list[str]) -> list[str]:
    """Columns the contract requires but the table does not have."""
    return [c for c in BRONZE_CONTRACTS[table] if c not in set(columns)]


def failed_rules_sql(rules: dict[str, str]) -> str:
    """SQL expression -> ARRAY<STRING> of rule names the row violates (NULL-safe: unknown counts as failed)."""
    parts = ", ".join(f"CASE WHEN coalesce({expr}, false) THEN NULL ELSE '{name}' END" for name, expr in rules.items())
    return f"filter(array({parts}), x -> x IS NOT NULL)"


def with_rule_flags(df, rules: dict[str, str]):
    """Add `failed_rules` (array<string>) to a DataFrame."""
    from pyspark.sql import functions as F

    return df.withColumn("failed_rules", F.expr(failed_rules_sql(rules)))
