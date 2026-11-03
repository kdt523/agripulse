"""Daily data-quality scorecard -> ops.dq_scorecard (one row per silver table per day)."""

from __future__ import annotations

from datetime import date

from agripulse.common.config import Settings, get_settings

# silver table -> columns whose completeness we report
SCORECARD_TABLES = {
    "districts": ["district_id", "lat", "lon", "area_km2"],
    "weather_daily": ["district_id", "date", "tmax_c", "tmin_c", "precip_mm"],
    "agro_climate_daily": ["district_id", "date", "solar_mj_m2", "soil_wetness_root"],
    "mandi_prices_daily": ["district_id", "market_id", "commodity_std", "date", "modal_price_inr_qtl", "arrivals_t"],
}


def pass_rate(pass_rows: int, fail_rows: int) -> float:
    total = pass_rows + fail_rows
    return 1.0 if total == 0 else pass_rows / total


def build_scorecard(spark, settings: Settings | None = None, as_of: date | None = None):
    """Return a DataFrame with the scorecard schema (does not write)."""
    from pyspark.sql import functions as F

    s = settings or get_settings()
    as_of = as_of or date.today()
    out = None
    for table, cols in SCORECARD_TABLES.items():
        silver = spark.table(s.table("silver", table))
        quar = spark.table(s.table("silver", f"{table}_quarantine"))
        pass_n, fail_n = silver.count(), quar.count()
        # completeness: share of non-null cells across the key columns of the PASSING rows
        nn = silver.select([F.avg(F.col(c).isNotNull().cast("double")).alias(c) for c in cols]).collect()[0]
        completeness = 100.0 * sum((nn[c] or 0.0) for c in cols) / len(cols) if pass_n else 0.0
        by_rule = quar.select(F.explode("failed_rules").alias("rule")).groupBy("rule").count().collect()
        rules = {r["rule"]: int(r["count"]) for r in by_rule}
        row = spark.createDataFrame(
            [(table, as_of, pass_n + fail_n, pass_n, fail_n, pass_rate(pass_n, fail_n), rules, completeness)],
            "table string, date date, rows long, pass_rows long, fail_rows long, pass_rate double, "
            "rules_failed map<string,long>, completeness_pct double",
        )
        out = row if out is None else out.unionByName(row)
    return out


def write_scorecard(spark, settings: Settings | None = None, as_of: date | None = None) -> dict:
    s = settings or get_settings()
    df = build_scorecard(spark, s, as_of)
    target = s.table("ops", "dq_scorecard")
    if not spark.catalog.tableExists(target):
        df.limit(0).write.format("delta").saveAsTable(target)
        comment = "Daily data-quality scorecard per silver table (pass rate, failed rules, completeness)"
        spark.sql(f"COMMENT ON TABLE {target} IS '{comment}'")
    df.createOrReplaceTempView("_dq_src")
    spark.sql(
        f"MERGE INTO {target} t USING _dq_src s ON t.`table` = s.`table` AND t.date = s.date "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
    return {r["table"]: round(r["pass_rate"], 4) for r in df.collect()}
