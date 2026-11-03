"""Feature dictionary for gold.district_week: single source for UC column comments and docs/feature_dictionary.md."""

from __future__ import annotations

# (column, description, source, formula)
FEATURES = [
    ("district_id", "Census-2011 district key (D + 4-digit census code)", "bronze.geo_districts", "'D' || lpad(censuscode,4,'0')"),
    ("week_start", "Monday of the ISO week (grain key)", "calendar", "date_trunc('week', date)"),
    ("commodity_std", "Standardised commodity name (grain key)", "silver.mandi_prices_daily", "COMMODITY_MAP(commodity)"),
    ("arrivals_t_sum", "Weekly mandi arrivals in tonnes: the demand proxy", "Agmarknet", "sum(arrivals_t) over district's markets in the week"),
    ("modal_price_avg", "Average modal price, INR per quintal", "Agmarknet", "avg(modal_price_inr_qtl)"),
    ("precip_sum_7d", "Rainfall in the week (mm)", "Open-Meteo / NASA POWER", "sum(precip_mm) over 7 days"),
    ("precip_sum_28d", "Rainfall over this + previous 3 weeks (mm)", "Open-Meteo / NASA POWER", "sum(precip_sum_7d) over weeks t-3..t"),
    ("precip_anomaly", "Weekly rainfall minus mean of same week-of-year in the previous 5 years", "Open-Meteo / NASA POWER",
     "precip_sum_7d - avg(precip_sum_7d, same week, years y-5..y-1)"),
    ("tmax_avg", "Mean daily maximum temperature in the week (degC)", "Open-Meteo / NASA POWER", "avg(tmax_c)"),
    ("heat_days", "Days in the week with tmax above 40 degC", "Open-Meteo / NASA POWER", "count(tmax_c > 40)"),
    ("dry_spell_len", "Longest run of consecutive dry days (<1 mm) ending in the week; run may start in an earlier week",
     "Open-Meteo / NASA POWER", "max(running count of consecutive days with precip_mm < 1)"),
    ("et0_sum", "Reference evapotranspiration in the week (mm); NULL when only NASA data exists", "Open-Meteo", "sum(et0_mm)"),
    ("gdd", "Growing degree days (base 10 degC)", "Open-Meteo / NASA POWER", "sum(max(0, (tmax+tmin)/2 - 10))"),
    ("soil_wetness_root_avg", "Mean root-zone soil wetness (0-1)", "NASA POWER GWETROOT", "avg(soil_wetness_root)"),
    ("soil_moisture_anomaly", "Root-zone soil wetness minus same-week mean of previous 5 years", "NASA POWER GWETROOT",
     "soil_wetness_root_avg - avg(same week, years y-5..y-1)"),
    ("solar_sum", "Total incoming shortwave radiation in the week (MJ/m2)", "NASA POWER ALLSKY_SFC_SW_DWN", "sum(solar_mj_m2)"),
    ("precip_forecast_next_7d", "Forecast rainfall for the 7 days after the build date; only set for the current week",
     "Open-Meteo forecast", "sum(precip_mm) on forecast rows in (today, today+7]"),
    ("arrivals_lag_1w", "Arrivals 1 week earlier (NULL if no report that week)", "Agmarknet", "arrivals_t_sum at week_start - 7d"),
    ("arrivals_lag_2w", "Arrivals 2 weeks earlier", "Agmarknet", "arrivals_t_sum at week_start - 14d"),
    ("arrivals_lag_4w", "Arrivals 4 weeks earlier", "Agmarknet", "arrivals_t_sum at week_start - 28d"),
    ("arrivals_lag_52w", "Arrivals same week last year", "Agmarknet", "arrivals_t_sum at week_start - 364d"),
    ("price_lag_1w", "Modal price 1 week earlier", "Agmarknet", "modal_price_avg at week_start - 7d"),
    ("price_lag_2w", "Modal price 2 weeks earlier", "Agmarknet", "modal_price_avg at week_start - 14d"),
    ("price_lag_4w", "Modal price 4 weeks earlier", "Agmarknet", "modal_price_avg at week_start - 28d"),
    ("price_lag_52w", "Modal price same week last year", "Agmarknet", "modal_price_avg at week_start - 364d"),
    ("price_volatility_4w", "Std-dev of weekly modal price over the last 4 weeks (reported weeks only)", "Agmarknet",
     "stddev(modal_price_avg) over week index t-3..t"),
    ("n_markets", "Number of distinct markets reporting in the week", "Agmarknet", "count(distinct market_id)"),
    ("commodity_share_52w", "Share of this commodity in the district's total arrivals over the previous 52 weeks (mandi-derived crop-mix proxy)",
     "Agmarknet", "sum(arrivals commodity, t-52..t-1) / sum(arrivals all, t-52..t-1)"),
    ("area_km2", "District area (km2)", "DataMeet Census-2011", "geodesic-approximated polygon area"),
    ("state", "Standardised state name", "DataMeet Census-2011", "upper(trim(ST_NM))"),
    ("week_of_year", "ISO week number", "calendar", "weekofyear(week_start)"),
    ("year", "Calendar year of week_start", "calendar", "year(week_start)"),
    ("season", "Indian cropping season of the week's month: kharif Jun-Oct, rabi Nov-Mar, zaid Apr-May", "calendar", "month(week_start) rule"),
]  # fmt: skip

TABLE_COMMENT = (
    "Feature table for demand forecasting: one row per district x ISO week x commodity, joining mandi demand proxy, "
    "weather, agro-climate, forecast and geo features. Leakage-safe: features use data up to the end of the week only."
)


def apply_comments(spark, catalog: str = "agripulse") -> None:
    t = f"{catalog}.gold.district_week"
    spark.sql(f"COMMENT ON TABLE {t} IS '{TABLE_COMMENT}'")
    for col, desc, _src, _f in FEATURES:
        spark.sql(f"ALTER TABLE {t} ALTER COLUMN {col} COMMENT '{desc.replace(chr(39), '')}'")


def render_markdown() -> str:
    lines = [
        "# Feature dictionary: `gold.district_week`",
        "",
        "Grain: `district_id` x `week_start` x `commodity_std`. Generated from `src/agripulse/transform/gold_docs.py`.",
        "",
        "## Leakage rules",
        "1. Rolling and lag features at week *t* use data up to the end of week *t* only.",
        "2. Anomaly baselines use the 5 *previous* years' same-week values (current year excluded).",
        "3. Forecast features exist only for the current build week, from the latest forecast issued so far.",
        "4. The training target is next week's arrivals (`target_arrivals_next_week` in `gold.v_ds_training`); "
        "week *t* features never contain week *t+1* information.",
        "",
        "| Column | Description | Source | Formula |",
        "|---|---|---|---|",
    ]
    lines += [f"| `{c}` | {d} | {s} | {f} |" for c, d, s, f in FEATURES]
    return "\n".join(lines) + "\n"
