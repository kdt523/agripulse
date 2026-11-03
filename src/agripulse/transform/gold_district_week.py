"""gold.district_week: grain = district_id x week_start (Monday) x commodity_std.

Pure DataFrame functions (testable locally); `build()` wires them to the silver tables.

LEAKAGE RULES (also in docs/feature_dictionary.md):
 1. Every rolling/lag feature at week t uses data up to the END of week t only (past + current), never later weeks.
 2. Anomaly baselines use the 5 PRIOR years' same-week values, excluding the current year.
 3. Forecast features exist only for the current/future weeks and use the latest forecast issued so far.
 4. The training target is the NEXT week's arrivals (`target_arrivals_next_week`, view gold.v_ds_training);
    features of week t never contain week t+1 information.
"""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

HEAT_THRESHOLD_C = 40.0
DRY_DAY_MM = 1.0
GDD_BASE_C = 10.0
ANOMALY_YEARS = 5
LOOKBACK_DAYS = 6 * 366  # inputs needed to compute 5-year anomalies for any output week


def week_start(col: str = "date"):
    return F.date_trunc("week", F.col(col)).cast("date")  # Monday


# --------------------------------------------------------------------------------------------- mandi (market)
def weekly_mandi(mandi: DataFrame) -> DataFrame:
    """Daily mandi rows -> district x week x commodity demand proxy and price."""
    m = mandi.filter("district_id IS NOT NULL AND commodity_std IS NOT NULL")
    return (
        m.withColumn("week_start", week_start("date"))
        .groupBy("district_id", "week_start", "commodity_std")
        .agg(
            F.sum("arrivals_t").alias("arrivals_t_sum"),
            F.avg("modal_price_inr_qtl").alias("modal_price_avg"),
            F.countDistinct("market_id").alias("n_markets"),
        )
    )


def add_market_features(wk: DataFrame) -> DataFrame:
    """Calendar-correct lags (1,2,4,52 weeks; NULL if that week has no report), volatility, trailing commodity share."""
    base = wk.select("district_id", "commodity_std", "week_start", "arrivals_t_sum", "modal_price_avg")
    out = wk
    for k in (1, 2, 4, 52):
        lagged = base.select(
            "district_id",
            "commodity_std",
            (F.col("week_start") + F.expr(f"INTERVAL {7 * k} DAYS")).cast("date").alias("week_start"),
            F.col("arrivals_t_sum").alias(f"arrivals_lag_{k}w"),
            F.col("modal_price_avg").alias(f"price_lag_{k}w"),
        )
        out = out.join(lagged, ["district_id", "commodity_std", "week_start"], "left")

    idx = F.datediff("week_start", F.lit(date(1970, 1, 5))) / 7  # integer week index (1970-01-05 is a Monday)
    out = out.withColumn("_widx", idx.cast("long"))
    w4 = Window.partitionBy("district_id", "commodity_std").orderBy("_widx").rangeBetween(-3, 0)
    out = out.withColumn("price_volatility_4w", F.stddev("modal_price_avg").over(w4))

    # trailing-52-week share of this commodity in the district's total arrivals, EXCLUDING the current week
    w_c = Window.partitionBy("district_id", "commodity_std").orderBy("_widx").rangeBetween(-52, -1)
    w_all = Window.partitionBy("district_id").orderBy("_widx").rangeBetween(-52, -1)
    tot = wk.groupBy("district_id", "week_start").agg(F.sum("arrivals_t_sum").alias("_tot")).withColumn("_widx", idx.cast("long"))
    tot = tot.withColumn("_tot_52", F.sum("_tot").over(w_all)).select("district_id", "week_start", "_tot_52")
    out = out.withColumn("_c52", F.sum("arrivals_t_sum").over(w_c)).join(tot, ["district_id", "week_start"], "left")
    out = out.withColumn("commodity_share_52w", F.col("_c52") / F.when(F.col("_tot_52") > 0, F.col("_tot_52")))
    return out.drop("_widx", "_c52", "_tot_52")


# --------------------------------------------------------------------------------------------- weather
def weekly_weather(weather: DataFrame) -> DataFrame:
    """Observed (non-forecast) daily weather -> district x week features."""
    d = weather.filter("NOT is_forecast").select("district_id", "date", "tmax_c", "tmin_c", "precip_mm", "et0_mm")
    # dry spell: running count of consecutive dry days (precip < 1 mm), via gaps-and-islands
    wd = Window.partitionBy("district_id").orderBy("date")
    d = d.withColumn("_dry", (F.coalesce(F.col("precip_mm"), F.lit(0.0)) < DRY_DAY_MM).cast("int"))
    d = d.withColumn("_island", F.sum(1 - F.col("_dry")).over(wd))
    d = d.withColumn("_run", F.sum("_dry").over(wd.partitionBy("district_id", "_island").rowsBetween(Window.unboundedPreceding, 0)))
    gdd = F.greatest(F.lit(0.0), (F.col("tmax_c") + F.col("tmin_c")) / 2 - F.lit(GDD_BASE_C))
    return (
        d.withColumn("week_start", week_start("date"))
        .groupBy("district_id", "week_start")
        .agg(
            F.sum("precip_mm").alias("precip_sum_7d"),
            F.avg("tmax_c").alias("tmax_avg"),
            F.sum((F.col("tmax_c") > HEAT_THRESHOLD_C).cast("int")).alias("heat_days"),
            F.max("_run").alias("dry_spell_len"),
            F.sum("et0_mm").alias("et0_sum"),
            F.sum(gdd).alias("gdd"),
            F.count("*").alias("_days_observed"),
        )
    )


def _anomaly(df: DataFrame, col: str, name: str) -> DataFrame:
    """col minus the mean of the same ISO week over the previous 5 years (current year excluded)."""
    df = df.withColumn("_woy", F.weekofyear("week_start")).withColumn("_yr", F.year("week_start"))
    w = Window.partitionBy("district_id", "_woy").orderBy("_yr").rangeBetween(-ANOMALY_YEARS, -1)
    base = F.avg(col).over(w)
    return df.withColumn(name, F.col(col) - base).drop("_woy", "_yr")


def add_weather_features(wk: DataFrame) -> DataFrame:
    w28 = Window.partitionBy("district_id").orderBy(F.expr("unix_date(week_start)")).rangeBetween(-21, 0)  # days
    out = wk.withColumn("precip_sum_28d", F.sum("precip_sum_7d").over(w28))
    out = _anomaly(out, "precip_sum_7d", "precip_anomaly")
    return out.filter(F.col("_days_observed") >= 5).drop("_days_observed")  # drop weeks with mostly missing days


# --------------------------------------------------------------------------------------------- agro-climate
def weekly_agro(agro: DataFrame) -> DataFrame:
    wk = (
        agro.withColumn("week_start", week_start("date"))
        .groupBy("district_id", "week_start")
        .agg(
            F.avg("soil_wetness_root").alias("soil_wetness_root_avg"),
            F.sum("solar_mj_m2").alias("solar_sum"),
            F.count("*").alias("_n"),
        )
        .filter("_n >= 5")
        .drop("_n")
    )
    return _anomaly(wk, "soil_wetness_root_avg", "soil_moisture_anomaly")


# --------------------------------------------------------------------------------------------- forecast
def forecast_next7(weather: DataFrame, today: date) -> DataFrame:
    """Sum of forecast precip for the 7 days after `today`, attached to the week containing `today` only."""
    f = weather.filter(F.col("is_forecast") & (F.col("date") > F.lit(today)) & (F.col("date") <= F.lit(today + timedelta(days=7))))
    return (
        f.groupBy("district_id")
        .agg(F.sum("precip_mm").alias("precip_forecast_next_7d"))
        .withColumn("week_start", week_start_lit(today))
    )


def week_start_lit(d: date):
    return F.lit(d - timedelta(days=d.weekday())).cast("date")


# --------------------------------------------------------------------------------------------- assemble
def season_expr():
    m = F.month("week_start")
    return F.when(m.between(6, 10), "kharif").when((m >= 11) | (m <= 3), "rabi").otherwise("zaid")


def assemble(mandi_w: DataFrame, weather_w: DataFrame, agro_w: DataFrame, fc: DataFrame, districts: DataFrame) -> DataFrame:
    keys = ["district_id", "week_start"]
    df = (
        add_market_features(mandi_w)
        .join(weather_w, keys, "left")
        .join(agro_w, keys, "left")
        .join(fc, keys, "left")
        .join(districts.select("district_id", "area_km2", F.col("state_std").alias("state")), "district_id", "left")
        .withColumn("week_of_year", F.weekofyear("week_start"))
        .withColumn("year", F.year("week_start"))
        .withColumn("season", season_expr())
    )
    return df.select(
        "district_id", "week_start", "commodity_std",
        "arrivals_t_sum", "modal_price_avg",
        "precip_sum_7d", "precip_sum_28d", "precip_anomaly", "tmax_avg", "heat_days", "dry_spell_len", "et0_sum", "gdd",
        "soil_wetness_root_avg", "soil_moisture_anomaly", "solar_sum",
        "precip_forecast_next_7d",
        "arrivals_lag_1w", "arrivals_lag_2w", "arrivals_lag_4w", "arrivals_lag_52w",
        "price_lag_1w", "price_lag_2w", "price_lag_4w", "price_lag_52w",
        "price_volatility_4w", "n_markets",
        "commodity_share_52w", "area_km2", "state",
        "week_of_year", "year", "season",
    )  # fmt: skip


# --------------------------------------------------------------------------------------------- district profile
def build_profile(weather: DataFrame, agro: DataFrame, mandi_w: DataFrame, districts: DataFrame) -> DataFrame:
    obs = weather.filter("NOT is_forecast").withColumn("yr", F.year("date"))
    yearly = obs.groupBy("district_id", "yr").agg(
        F.sum("precip_mm").alias("p"), F.count("*").alias("n"),
        F.sum((F.col("tmax_c") > HEAT_THRESHOLD_C).cast("int")).alias("hd"), F.avg("tmax_c").alias("tm"),
    ).filter("n >= 330")  # fmt: skip
    clim = yearly.groupBy("district_id").agg(
        F.avg("p").alias("annual_precip_mm"),
        (F.stddev("p") / F.avg("p")).alias("precip_cv"),
        F.avg("hd").alias("heat_days_per_year"),
        F.avg("tm").alias("tmax_mean_c"),
        F.count("*").alias("years_used"),
    )
    soil = agro.groupBy("district_id").agg(F.avg("soil_wetness_root").alias("soil_wetness_root_mean"))
    vol = (
        mandi_w.withColumn("cv", F.col("modal_price_avg"))
        .groupBy("district_id", "commodity_std").agg((F.stddev("modal_price_avg") / F.avg("modal_price_avg")).alias("pcv"))
        .groupBy("district_id").agg(F.avg("pcv").alias("price_volatility_cv"))
    )  # fmt: skip
    mix = mandi_w.groupBy("district_id", "commodity_std").agg(F.sum("arrivals_t_sum").alias("a"))
    tot = mix.groupBy("district_id").agg(F.sum("a").alias("t"))
    shares = (
        mix.join(tot, "district_id").withColumn("share", F.col("a") / F.col("t"))
        .groupBy("district_id").pivot("commodity_std").agg(F.first("share"))
    )  # fmt: skip
    for c in shares.columns:
        if c != "district_id":
            shares = shares.withColumnRenamed(c, f"share_{c.lower()}")
    out = districts.select("district_id", F.col("district_name_std"), F.col("state_std"), "lat", "lon", "area_km2")
    for part in (clim, soil, vol, shares):
        out = out.join(part, "district_id", "left")
    return out


# --------------------------------------------------------------------------------------------- wiring
def build(spark, catalog: str = "agripulse", since: date | None = None, today: date | None = None) -> dict:
    """Full refresh (since=None) or incremental (rebuild weeks >= since, reading enough history for lags/anomalies)."""
    today = today or date.today()
    t = lambda s, n: f"{catalog}.{s}.{n}"  # noqa: E731
    floor = (since - timedelta(days=LOOKBACK_DAYS)) if since else None

    def read(schema, name, col="date"):
        df = spark.table(t(schema, name))
        return df.filter(F.col(col) >= F.lit(floor)) if floor else df

    mandi = read("silver", "mandi_prices_daily")
    weather = read("silver", "weather_daily")
    agro = read("silver", "agro_climate_daily")
    districts = spark.table(t("silver", "districts"))

    mw = weekly_mandi(mandi)
    gold = assemble(mw, add_weather_features(weekly_weather(weather)), weekly_agro(agro), forecast_next7(weather, today), districts)
    if since:
        gold = gold.filter(F.col("week_start") >= F.lit(since))

    target = t("gold", "district_week")
    if not spark.catalog.tableExists(target):
        gold.limit(0).write.format("delta").saveAsTable(target)
    gold.createOrReplaceTempView("_gold_src")
    spark.sql(
        f"MERGE INTO {target} t USING _gold_src s ON t.district_id = s.district_id AND t.week_start = s.week_start "
        "AND t.commodity_std = s.commodity_std WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
    profile = build_profile(weather if not since else spark.table(t("silver", "weather_daily")),
                            spark.table(t("silver", "agro_climate_daily")), weekly_mandi(spark.table(t("silver", "mandi_prices_daily"))), districts)  # fmt: skip
    profile.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold", "district_profile"))
    return {
        "district_week_rows": spark.table(target).count(),
        "district_profile_rows": spark.table(t("gold", "district_profile")).count(),
    }
