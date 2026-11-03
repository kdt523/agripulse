# Databricks notebook source
# MAGIC %md
# MAGIC # 20 — District name map
# MAGIC Reconciles mandi (state, district) spellings to Census-2011 `district_id`s -> `ops.district_name_map`.
# MAGIC Overrides live in `docs/district_overrides.csv`. Unmatched rows are kept (district_id NULL) and reported.

# COMMAND ----------
# MAGIC %pip install -q rapidfuzz
# MAGIC %restart_python

# COMMAND ----------
import os
import sys

sys.path.insert(0, os.path.abspath("../src"))
dbutils.widgets.text("catalog", "agripulse")
catalog = dbutils.widgets.get("catalog")

from agripulse.ingest.name_map import load_overrides, match_names

geo = [r.asDict() for r in spark.table(f"{catalog}.bronze.geo_districts").select("district_id", "district_name", "state_name").collect()]
src = [
    (r["state"], r["district"])
    for r in spark.table(f"{catalog}.bronze.mandi_prices_raw").select("state", "district").distinct().collect()
    if r["district"]
]
overrides = load_overrides(os.path.abspath("../docs/district_overrides.csv"))
matches = match_names(src, geo, overrides)

rows = [(m.src_state, m.src_district, m.district_id, m.score, m.method) for m in matches]
df = spark.createDataFrame(rows, "src_state string, src_district string, district_id string, score double, method string")
(df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.ops.district_name_map"))
spark.sql(f"COMMENT ON TABLE {catalog}.ops.district_name_map IS 'Mandi (state,district) spellings mapped to Census-2011 district_id; method = override|exact|fuzzy_state|fuzzy_any_state|unmatched'")

summary = spark.sql(f"SELECT method, count(*) AS n FROM {catalog}.ops.district_name_map GROUP BY method ORDER BY n DESC").collect()
print({r["method"]: r["n"] for r in summary})
import json

dbutils.notebook.exit(json.dumps({r["method"]: r["n"] for r in summary}))
