"""Chaos data test: inject 50 faults (10 types x 5) into a SANDBOX copy of bronze and measure what the quality gates catch.

Never touches the real bronze/silver/ops schemas: everything lives in chaos_bronze / chaos_silver / chaos_ops.
Each fault carries a marker that survives into silver (mandi: arrivals_tonnes = 7000 + n/10; NASA: (district_id, date) key or
gwettop = 0.5 + n*1e-5 for null-key rows), so every fault can be traced to a quarantine row, a silver row or a failed update.
"""

from __future__ import annotations

import datetime
import random
from datetime import date

import pandas as pd

TYPES = {
    "null_key": "valid_key",
    "tmax_lt_tmin": "tmax_ge_tmin",
    "negative_rain": "precip_nonneg",
    "nasa_fill": "nasa_fill",
    "price_order": "price_order",
    "price_spike_100x": "price_spike (warn) / price_spike_extreme (drop)",
    "duplicate_row": "dq_duplicate_keys + dedupe",
    "schema_drift": "schema contract (fails update)",
    "stale_feed": "freshness SLA",
    "volume_drop": "volume anomaly",
}
PER_TYPE = 5
SCHEMAS = ("chaos_bronze", "chaos_silver", "chaos_ops")
MANDI_KEYS = ["state", "district", "market", "commodity", "variety", "grade", "arrival_date"]


def lit(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("'", "''") + "'"


def where_eq(keys: dict) -> str:
    return " AND ".join(f"`{k}` <=> {lit(v)}" for k, v in keys.items())


def marker(n: int) -> float:
    return round(7000 + n / 10.0, 1)


# --------------------------------------------------------------------------------------------- sandbox
def build_sandbox(spark, catalog: str, n_districts: int = 30) -> list[str]:
    for s in SCHEMAS:  # chaos_silver is the pipeline target: create it but never drop it
        if s != "chaos_silver":
            spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{s} CASCADE")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{s}")
    top = spark.sql(
        f"SELECT district_id FROM {catalog}.silver.mandi_prices_daily WHERE district_id IS NOT NULL "
        f"GROUP BY district_id ORDER BY count(*) DESC LIMIT {n_districts}"
    ).collect()
    ids = [r["district_id"] for r in top]
    in_list = ",".join(lit(i) for i in ids)
    b = f"{catalog}.chaos_bronze"
    spark.sql(f"CREATE OR REPLACE TABLE {b}.geo_districts AS SELECT * FROM {catalog}.bronze.geo_districts")
    spark.sql(
        f"CREATE OR REPLACE TABLE {b}.nasa_power_raw AS SELECT * FROM {catalog}.bronze.nasa_power_raw "
        f"WHERE district_id IN ({in_list}) AND date BETWEEN '2015-07-01' AND '2016-12-31'"
    )
    spark.sql(
        f"CREATE OR REPLACE TABLE {b}.mandi_prices_raw AS SELECT m.* FROM {catalog}.bronze.mandi_prices_raw m "
        f"JOIN {catalog}.ops.district_name_map n ON m.state = n.src_state AND m.district = n.src_district "
        f"WHERE n.district_id IN ({in_list}) AND m.arrival_date BETWEEN '2015-07-01' AND '2016-12-31'"
    )
    spark.sql(f"CREATE OR REPLACE TABLE {b}.weather_daily_raw AS SELECT * FROM {catalog}.bronze.weather_daily_raw LIMIT 3000")
    spark.sql(f"CREATE OR REPLACE TABLE {b}.weather_forecast_raw AS SELECT * FROM {catalog}.bronze.weather_forecast_raw")
    spark.sql(f"CREATE OR REPLACE TABLE {catalog}.chaos_ops.district_name_map AS SELECT * FROM {catalog}.ops.district_name_map")
    return ids


# --------------------------------------------------------------------------------------------- injection
def inject(spark, catalog: str, seed: int = 11) -> pd.DataFrame:
    """Inject 40 row-level faults (8 types x 5). Returns the fault log; schema/stale/volume are added later."""
    rng = random.Random(seed)
    b = f"{catalog}.chaos_bronze"
    nasa = spark.table(f"{b}.nasa_power_raw").select("district_id", "date").toPandas()
    nasa = nasa.sample(n=60, random_state=seed).reset_index(drop=True)
    mandi = spark.table(f"{b}.mandi_prices_raw").select(*MANDI_KEYS).toPandas()
    mandi["grp"] = mandi[["state", "district", "market", "commodity", "variety"]].astype(str).agg("|".join, axis=1)
    mandi = mandi.sort_values(["grp", "arrival_date"])
    mandi["rank"] = mandi.groupby("grp").cumcount()
    sizes = mandi.groupby("grp")["rank"].transform("max")
    used_groups: set[str] = set()

    def pick_mandi(min_rank: int = 1) -> dict:
        cand = mandi[(mandi["rank"] >= min_rank) & (sizes >= min_rank) & (~mandi["grp"].isin(used_groups))]
        row = cand.iloc[rng.randrange(len(cand))]
        used_groups.add(row["grp"])
        return {k: (None if pd.isna(row[k]) else (row[k].date() if hasattr(row[k], "date") and not isinstance(row[k], str) else row[k])) for k in MANDI_KEYS}

    log, n, nasa_i = [], 0, 0

    def next_nasa() -> dict:
        nonlocal nasa_i
        r = nasa.iloc[nasa_i]
        nasa_i += 1
        return {"district_id": r["district_id"], "date": r["date"]}

    def add(type_, table, key, sql_after=None, **extra):
        nonlocal n
        n += 1
        log.append({"fault_id": f"F{n:03d}", "type": type_, "table": table, "expected_detector": TYPES[type_],
                    "key": repr(key), "marker": extra.get("marker"), "variant": extra.get("variant")})  # fmt: skip
        return n

    t_nasa, t_mandi = f"{b}.nasa_power_raw", f"{b}.mandi_prices_raw"

    # 1. null keys: 2 mandi commodity, 1 mandi market, 1 NASA district_id, 1 NASA date
    for variant in ("commodity", "commodity", "market"):
        k = pick_mandi()
        fid = add("null_key", "mandi_prices_raw", k, marker=marker(n + 1), variant=variant)
        spark.sql(f"UPDATE {t_mandi} SET `{variant}` = NULL, arrivals_tonnes = {marker(fid)} WHERE {where_eq(k)}")
    for variant in ("district_id", "date"):
        k = next_nasa()
        m = round(0.5 + (n + 1) * 1e-5, 6)
        add("null_key", "nasa_power_raw", k, marker=m, variant=variant)
        spark.sql(f"UPDATE {t_nasa} SET `{variant}` = NULL, gwettop = {m} WHERE {where_eq(k)}")
    # 2. tmax < tmin, 3. negative rain, 4. NASA -999
    for _ in range(PER_TYPE):
        k = next_nasa()
        add("tmax_lt_tmin", "nasa_power_raw", k)
        spark.sql(f"UPDATE {t_nasa} SET t2m_max = t2m_min - 3 WHERE {where_eq(k)}")
    for _ in range(PER_TYPE):
        k = next_nasa()
        add("negative_rain", "nasa_power_raw", k)
        spark.sql(f"UPDATE {t_nasa} SET prectotcorr = -4.2 WHERE {where_eq(k)}")
    for _ in range(PER_TYPE):
        k = next_nasa()
        add("nasa_fill", "nasa_power_raw", k)
        spark.sql(f"UPDATE {t_nasa} SET allsky_sfc_sw_dwn = -999 WHERE {where_eq(k)}")
    # 5. price order violation (min > modal)
    for _ in range(PER_TYPE):
        k = pick_mandi()
        fid = add("price_order", "mandi_prices_raw", k, marker=marker(n + 1))
        spark.sql(f"UPDATE {t_mandi} SET min_price = modal_price * 1.5, arrivals_tonnes = {marker(fid)} WHERE {where_eq(k)}")
    # 6. 100x spike on all three prices (keeps min<=modal<=max, so only the spike rule can catch it)
    for _ in range(PER_TYPE):
        k = pick_mandi(min_rank=10)
        fid = add("price_spike_100x", "mandi_prices_raw", k, marker=marker(n + 1))
        spark.sql(
            f"UPDATE {t_mandi} SET min_price = min_price * 100, max_price = max_price * 100, modal_price = modal_price * 100, "
            f"arrivals_tonnes = {marker(fid)} WHERE {where_eq(k)}"
        )
    # 7. duplicate rows: second physical copy with a later _ingested_at
    for _ in range(PER_TYPE):
        k = pick_mandi()
        fid = add("duplicate_row", "mandi_prices_raw", k, marker=marker(n + 1))
        spark.sql(f"UPDATE {t_mandi} SET arrivals_tonnes = {marker(fid)} WHERE {where_eq(k)}")
        cols = [c.name for c in spark.table(t_mandi).schema]
        sel = ", ".join("current_timestamp()" if c == "_ingested_at" else f"`{c}`" for c in cols)
        spark.sql(f"INSERT INTO {t_mandi} SELECT {sel} FROM {t_mandi} WHERE {where_eq(k)}")
    return pd.DataFrame(log)


def inject_schema_drift(spark, catalog: str) -> pd.DataFrame:
    """5 drift faults: rename/drop a contract column in each of the 5 bronze sandbox tables."""
    b = f"{catalog}.chaos_bronze"
    plan = [
        ("weather_daily_raw", "temperature_2m_max", "tmax_renamed"),
        ("weather_forecast_raw", "precipitation_sum", None),
        ("nasa_power_raw", "allsky_sfc_sw_dwn", "solar_renamed"),
        ("mandi_prices_raw", "modal_price", "modal_price_inr"),
        ("geo_districts", "area_km2", None),
    ]
    rows = []
    for i, (table, col, new) in enumerate(plan, start=36):  # F036-F040
        cols = [c.name for c in spark.table(f"{b}.{table}").schema]
        sel = ", ".join(f"`{c}` AS `{new}`" if (c == col and new) else f"`{c}`" for c in cols if not (c == col and new is None))
        spark.sql(f"CREATE OR REPLACE TABLE {b}.{table} AS SELECT {sel} FROM {b}.{table}")
        rows.append({"fault_id": f"F{i:03d}", "type": "schema_drift", "table": table, "expected_detector": TYPES["schema_drift"],
                     "key": repr({"column": col, "action": f"rename->{new}" if new else "drop"}), "marker": None, "variant": col})  # fmt: skip
    return pd.DataFrame(rows)


def logic_faults() -> pd.DataFrame:
    """F041-F045 (stale feed) and F046-F050 (volume drop) are evaluated at logic level (no pipeline): see evaluate_logic()."""
    rows = []
    for i in range(5):
        rows.append({"fault_id": f"F{41 + i:03d}", "type": "stale_feed", "table": "freshness", "expected_detector": TYPES["stale_feed"],
                     "key": repr({"source_stopped_days": 3 + i}), "marker": None, "variant": str(3 + i)})  # fmt: skip
    for i in range(5):
        rows.append({"fault_id": f"F{46 + i:03d}", "type": "volume_drop", "table": "pipeline_runs", "expected_detector": TYPES["volume_drop"],
                     "key": repr({"missing_pct": 90}), "marker": None, "variant": str(i)})  # fmt: skip
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------------------------- evaluation
def evaluate_rows(spark, catalog: str, log: pd.DataFrame) -> pd.DataFrame:
    """Match injected row-level faults to quarantine rows / silver rows after a successful pipeline update."""
    s = f"{catalog}.chaos_silver"
    wq = spark.table(f"{s}.weather_daily_quarantine").select("district_id", "date", "failed_rules").toPandas()
    aq = spark.table(f"{s}.agro_climate_daily_quarantine").select("district_id", "date", "soil_wetness_top", "failed_rules").toPandas()
    mq = spark.table(f"{s}.mandi_prices_daily_quarantine").select("arrivals_t", "failed_rules").toPandas()
    ws = spark.table(f"{s}.weather_daily").filter("NOT is_forecast").select("district_id", "date", "tmax_c", "tmin_c", "precip_mm").toPandas()
    asil = spark.table(f"{s}.agro_climate_daily").select("district_id", "date", "solar_mj_m2", "precip_mm").toPandas()
    ms = spark.table(f"{s}.mandi_prices_daily").select("arrivals_t", "modal_price_inr_qtl", "rolling_median_price").toPandas()
    dup = spark.table(f"{s}.dq_duplicate_keys").select("arrival_date", "market").toPandas()

    def rules(df, mask):
        out = set()
        for r in df[mask]["failed_rules"]:
            out |= set(r)
        return out

    res = []
    for _, f in log.iterrows():
        key, mk, t = eval(f["key"], {"datetime": datetime}), f["marker"], f["type"]  # our own repr() of plain dicts
        detected_by, in_silver = None, False
        if t == "null_key":
            if f["table"] == "mandi_prices_raw":
                hit = mq["arrivals_t"].round(1) == mk
                in_silver = bool((ms["arrivals_t"].round(1) == mk).any())
                if "valid_key" in rules(mq, hit):
                    detected_by = "valid_key"
            else:
                hit = (aq["soil_wetness_top"] - mk).abs() < 1e-7
                if "valid_key" in rules(aq, hit):
                    detected_by = "valid_key"
        elif t in ("tmax_lt_tmin", "negative_rain", "nasa_fill"):
            d = pd.Timestamp(key["date"]).date()
            wm = (wq["district_id"] == key["district_id"]) & (wq["date"] == d)
            am = (aq["district_id"] == key["district_id"]) & (aq["date"] == d)
            want = {"tmax_lt_tmin": "tmax_ge_tmin", "negative_rain": "precip_nonneg", "nasa_fill": "nasa_fill"}[t]
            if want in rules(wq, wm) | rules(aq, am):
                detected_by = want
            wsm = (ws["district_id"] == key["district_id"]) & (ws["date"] == d)
            asm = (asil["district_id"] == key["district_id"]) & (asil["date"] == d)
            if t == "tmax_lt_tmin":
                in_silver = bool((ws[wsm]["tmax_c"] < ws[wsm]["tmin_c"]).any())
            elif t == "negative_rain":
                in_silver = bool((ws[wsm]["precip_mm"] < 0).any() or (asil[asm]["precip_mm"] < 0).any())
            else:
                in_silver = bool((asil[asm]["solar_mj_m2"] == -999).any())
        elif t == "price_order":
            if "price_order" in rules(mq, mq["arrivals_t"].round(1) == mk):
                detected_by = "price_order"
            in_silver = bool((ms["arrivals_t"].round(1) == mk).any())
        elif t == "price_spike_100x":
            q = rules(mq, mq["arrivals_t"].round(1) == mk)
            row = ms[ms["arrivals_t"].round(1) == mk]
            in_silver = len(row) > 0
            if "price_spike_extreme" in q:
                detected_by = "price_spike_extreme (quarantined)"
            elif in_silver and len(row) and row["rolling_median_price"].notna().any() and \
                    (row["modal_price_inr_qtl"] >= 20 * row["rolling_median_price"]).any():  # fmt: skip
                detected_by = "price_spike (warn flag)"
        elif t == "duplicate_row":
            copies = int((ms["arrivals_t"].round(1) == mk).sum())
            in_silver = copies > 1  # a bad row only "reaches gold" if the duplicate survives dedupe
            if len(dup) > 0 and (dup["market"] == key["market"]).any():
                detected_by = "dq_duplicate_keys + dedupe"
        res.append({**f.to_dict(), "detected": detected_by is not None, "detected_by": detected_by, "reached_silver_gold": in_silver})
    return pd.DataFrame(res)


def evaluate_drift(log: pd.DataFrame, update_failed: bool, error_text: str) -> pd.DataFrame:
    rows = []
    for _, f in log.iterrows():
        hit = update_failed and f"bronze.{f['table']}" in error_text
        rows.append({**f.to_dict(), "detected": bool(hit), "detected_by": "schema contract" if hit else None, "reached_silver_gold": not update_failed})
    return pd.DataFrame(rows)


def evaluate_logic(log: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Stale-feed and volume-drop faults against the same pure functions the workflow uses (+ negative controls)."""
    from agripulse.quality.freshness import is_breached, volume_anomaly

    out = []
    for _, f in log.iterrows():
        if f["type"] == "stale_feed":
            sla = 3  # weather SLA
            stopped = int(f["variant"])
            hit = is_breached(lag=2 + stopped, sla_days=sla)  # healthy lag is ~2 days; the feed then stops for `stopped` days
        else:
            hit = volume_anomaly(today_rows=100, previous_rows=[1000] * 7)  # 90% of rows missing
        out.append({**f.to_dict(), "detected": bool(hit), "detected_by": "logic test", "reached_silver_gold": False})
    controls = {
        "stale_false_positives": sum(is_breached(lag=lag, sla_days=3) for lag in (1, 2, 3)),
        "volume_false_positives": sum(volume_anomaly(today_rows=rows_today, previous_rows=[1000] * 7) for rows_today in (700, 900, 1100)),
    }
    return pd.DataFrame(out), controls


def summarize(results: pd.DataFrame) -> dict:
    by_type = results.groupby("type").agg(injected=("fault_id", "count"), detected=("detected", "sum"), reached=("reached_silver_gold", "sum"))
    return {
        "injected": int(len(results)),
        "detected": int(results["detected"].sum()),
        "bad_rows_reaching_gold": int(results["reached_silver_gold"].sum()),
        "by_type": {k: {"injected": int(v.injected), "detected": int(v.detected), "reached_gold": int(v.reached)} for k, v in by_type.iterrows()},
        "run_date": date.today().isoformat(),
    }
