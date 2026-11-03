"""Lakeflow Declarative Pipeline: silver layer (clean, standardise, enforce quality, quarantine).

Pattern per table:
  <t>_flagged  (view)   : standardised rows + `failed_rules` array computed from agripulse.quality.expectations
  <t>          (table)  : ALL flagged rows with expect_all_or_drop(rules) -> bad rows dropped AND counted in the event log
  <t>_quarantine(table) : the rows that failed >=1 rule, with failed_rules   -> nothing is silently lost
"""

import os
import sys

import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F

# make `agripulse` importable (path is passed as a pipeline configuration by the bundle)
_src = spark.conf.get("agripulse.src_path", None)  # noqa: F821
if _src and _src not in sys.path:
    sys.path.insert(0, _src)
elif not _src:
    sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from agripulse.quality.expectations import (  # noqa: E402
    AGRO_RULES,
    DISTRICT_RULES,
    MANDI_RULES,
    MANDI_WARN_RULES,
    WEATHER_RULES,
    missing_columns,
    with_rule_flags,
)
from agripulse.transform.standardise import KMH_PER_MS, commodity_map_expr, nasa_null_if_fill, norm_name  # noqa: E402

CATALOG = spark.conf.get("agripulse.catalog", "agripulse")  # noqa: F821


def bronze(name: str):
    return spark.read.table(f"{CATALOG}.bronze.{name}")  # noqa: F821


# ---- schema contract: fail the whole update if bronze drifted ------------------------------------------------
for _t in ["weather_daily_raw", "weather_forecast_raw", "nasa_power_raw", "mandi_prices_raw", "geo_districts"]:
    _missing = missing_columns(_t, bronze(_t).columns)
    if _missing:
        raise Exception(f"SCHEMA CONTRACT VIOLATION: bronze.{_t} is missing columns {_missing}")


def dedupe_latest(df, keys):
    w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def publish(name, flagged_view, rules, comment, warn_rules=None, cluster_by=None):
    """Create <name> (expectations applied) and <name>_quarantine from a flagged view."""

    @dlt.table(name=name, comment=comment, cluster_by=cluster_by)
    @dlt.expect_all_or_drop(rules)
    @dlt.expect_all(warn_rules or {})
    def _silver():
        return dlt.read(flagged_view).drop("failed_rules")

    @dlt.table(name=f"{name}_quarantine", comment=f"Rows rejected from silver.{name}, with the rules they failed")
    def _quarantine():
        return dlt.read(flagged_view).filter(F.size("failed_rules") > 0)


# ================================================================ districts
@dlt.view(name="districts_flagged")
def districts_flagged():
    g = bronze("geo_districts")
    df = g.select(
        "district_id",
        norm_name("district_name").alias("district_name_std"),
        norm_name("state_name").alias("state_std"),
        F.col("lat").cast("double"),
        F.col("lon").cast("double"),
        F.col("area_km2").cast("double"),
    )
    return with_rule_flags(dedupe_latest_simple(df), DISTRICT_RULES)


def dedupe_latest_simple(df):
    return df.dropDuplicates(["district_id"])


publish("districts", "districts_flagged", DISTRICT_RULES,
        "One row per Census-2011 district: standardised names, centroid, area (km2). Source: DataMeet.")  # fmt: skip


# ================================================================ weather_daily
@dlt.view(name="weather_daily_flagged")
def weather_daily_flagged():
    om = dedupe_latest(bronze("weather_daily_raw"), ["district_id", "date"]).select(
        "district_id", "date",
        F.col("temperature_2m_max").alias("tmax_c"),
        F.col("temperature_2m_min").alias("tmin_c"),
        F.col("precipitation_sum").alias("precip_mm"),
        F.col("et0_fao_evapotranspiration").alias("et0_mm"),
        F.col("relative_humidity_2m_mean").alias("rh_pct"),
        F.col("wind_speed_10m_max").alias("wind_kmh"),
        F.lit(False).alias("is_forecast"),
        F.lit("open_meteo").alias("weather_source"),
        F.lit(1).alias("_prio"),
    )  # fmt: skip
    nasa = dedupe_latest(bronze("nasa_power_raw"), ["district_id", "date"]).select(
        "district_id", "date",
        nasa_null_if_fill("t2m_max").alias("tmax_c"),
        nasa_null_if_fill("t2m_min").alias("tmin_c"),
        nasa_null_if_fill("prectotcorr").alias("precip_mm"),
        F.lit(None).cast("double").alias("et0_mm"),
        nasa_null_if_fill("rh2m").alias("rh_pct"),
        (nasa_null_if_fill("ws10m") * KMH_PER_MS).alias("wind_kmh"),
        F.lit(False).alias("is_forecast"),
        F.lit("nasa_power").alias("weather_source"),
        F.lit(2).alias("_prio"),
    )  # fmt: skip
    fc_raw = bronze("weather_forecast_raw")
    latest_issue = fc_raw.groupBy("district_id", "date").agg(F.max("issued_date").alias("issued_date"))
    fc = fc_raw.join(latest_issue, ["district_id", "date", "issued_date"]).select(
        "district_id", "date",
        F.col("temperature_2m_max").alias("tmax_c"),
        F.col("temperature_2m_min").alias("tmin_c"),
        F.col("precipitation_sum").alias("precip_mm"),
        F.col("et0_fao_evapotranspiration").alias("et0_mm"),
        F.col("relative_humidity_2m_mean").alias("rh_pct"),
        F.col("wind_speed_10m_max").alias("wind_kmh"),
        F.lit(True).alias("is_forecast"),
        F.lit("open_meteo_forecast").alias("weather_source"),
        F.lit(0).alias("_prio"),
    )  # fmt: skip
    obs = om.unionByName(nasa)
    w = Window.partitionBy("district_id", "date").orderBy("_prio")  # Open-Meteo wins over NASA fallback
    obs = obs.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    allw = obs.unionByName(fc).drop("_prio")
    return with_rule_flags(allw, WEATHER_RULES)


publish("weather_daily", "weather_daily_flagged", WEATHER_RULES,
        "Daily district weather in SI-ish units (degC, mm, %, km/h). Open-Meteo preferred, NASA POWER fallback "
        "(weather_source). is_forecast marks forecast rows.", cluster_by=["district_id", "date"])  # fmt: skip


# ================================================================ agro_climate_daily
@dlt.view(name="agro_climate_daily_flagged")
def agro_climate_daily_flagged():
    n = dedupe_latest(bronze("nasa_power_raw"), ["district_id", "date"])
    df = n.select(
        "district_id", "date",
        F.col("allsky_sfc_sw_dwn").alias("solar_mj_m2"),  # raw: -999 kept so the nasa_fill rule can quarantine it
        nasa_null_if_fill("gwetroot").alias("soil_wetness_root"),
        nasa_null_if_fill("gwettop").alias("soil_wetness_top"),
        nasa_null_if_fill("t2m").alias("t2m_c"),
        nasa_null_if_fill("prectotcorr").alias("precip_mm"),
    )  # fmt: skip
    return with_rule_flags(df, AGRO_RULES)


publish("agro_climate_daily", "agro_climate_daily_flagged", AGRO_RULES,
        "NASA POWER daily agro-climate per district centroid: solar radiation (MJ/m2/day), root/top-zone soil "
        "wetness (0-1), 2m temperature (degC), corrected precipitation (mm).", cluster_by=["district_id", "date"])  # fmt: skip


# ================================================================ mandi_prices_daily
@dlt.view(name="mandi_prices_daily_flagged")
def mandi_prices_daily_flagged():
    m = dedupe_latest(
        bronze("mandi_prices_raw"),
        ["state", "district", "market", "commodity", "variety", "grade", "arrival_date"],
    )
    nm = spark.read.table(f"{CATALOG}.ops.district_name_map").select(  # noqa: F821
        F.col("src_state").alias("state"), F.col("src_district").alias("district"), "district_id"
    )
    df = (
        m.join(nm, ["state", "district"], "left")
        .select(
            "district_id",
            F.concat_ws("|", F.col("district_id"), norm_name("market")).alias("market_id"),
            commodity_map_expr("commodity").alias("commodity_std"),
            norm_name("variety").alias("variety_std"),
            F.col("arrival_date").alias("date"),
            F.col("min_price").alias("min_price_inr_qtl"),
            F.col("max_price").alias("max_price_inr_qtl"),
            F.col("modal_price").alias("modal_price_inr_qtl"),
            F.col("arrivals_tonnes").alias("arrivals_t"),
        )
    )  # fmt: skip
    # trailing 30-observation median per market x commodity x variety (excludes the current row)
    w = Window.partitionBy("market_id", "commodity_std", "variety_std").orderBy("date").rowsBetween(-30, -1)
    df = df.withColumn("rolling_median_price", F.percentile_approx("modal_price_inr_qtl", 0.5).over(w))
    return with_rule_flags(df, {**MANDI_RULES, **MANDI_WARN_RULES}).withColumn(
        # warn-only rules must not quarantine: keep only hard-rule failures in failed_rules
        "failed_rules", F.array_except("failed_rules", F.array(*[F.lit(k) for k in MANDI_WARN_RULES]))
    )


publish("mandi_prices_daily", "mandi_prices_daily_flagged", MANDI_RULES,
        "Daily mandi (APMC market) prices in INR per quintal and arrivals in tonnes, standardised commodity names, "
        "district_id via ops.district_name_map. price_spike is a warn-only rule.",
        warn_rules=MANDI_WARN_RULES, cluster_by=["date"])  # fmt: skip


# ================================================================ uniqueness check (warn)
@dlt.table(name="dq_duplicate_keys", comment="Natural keys that appear more than once in bronze mandi data (should be empty)")
@dlt.expect_all({"unique_keys": "n = 1"})
def dq_duplicate_keys():
    keys = ["state", "district", "market", "commodity", "variety", "grade", "arrival_date"]
    return bronze("mandi_prices_raw").groupBy(*keys).agg(F.count("*").alias("n")).filter("n > 1")
