# Databricks notebook source
# MAGIC %md
# MAGIC # 40 — Gold: district_week + district_profile
# MAGIC `mode=full` rebuilds everything; `mode=incremental` recomputes weeks since the watermark (minus 8 weeks) and MERGEs.

# COMMAND ----------
# MAGIC %pip install -q pydantic-settings tenacity
# MAGIC %restart_python

# COMMAND ----------
import json
import os
import sys
import time
from datetime import timedelta

sys.path.insert(0, os.path.abspath("../src"))
dbutils.widgets.text("catalog", "agripulse")
dbutils.widgets.dropdown("mode", "full", ["full", "incremental"])
catalog, mode = dbutils.widgets.get("catalog"), dbutils.widgets.get("mode")
os.environ["AGRIPULSE_CATALOG"] = catalog

from agripulse.common.config import get_settings
from agripulse.common.delta_io import get_watermark, set_watermark
from agripulse.common.runlog import logged_run
from agripulse.transform.gold_district_week import build
from agripulse.transform.gold_docs import apply_comments

settings = get_settings()
since = None
if mode == "incremental":
    wm = get_watermark(spark, "gold_district_week", settings=settings)
    since = (wm - timedelta(weeks=8)) if wm else None

# COMMAND ----------
t0 = time.time()
with logged_run(spark, "gold_district_week", "gold", settings) as rec:
    res = build(spark, catalog, since=since)
    rec.rows_out = res["district_week_rows"]

apply_comments(spark, catalog)
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.gold.v_ds_training
COMMENT 'Training view for the forecast-lift experiment: features at week t plus next-week arrivals/price as targets'
AS
SELECT f.*, n.arrivals_t_sum AS target_arrivals_next_week, n.modal_price_avg AS target_price_next_week
FROM {catalog}.gold.district_week f
JOIN {catalog}.gold.district_week n
  ON n.district_id = f.district_id AND n.commodity_std = f.commodity_std AND n.week_start = f.week_start + INTERVAL 7 DAYS
""")
mx = spark.sql(f"SELECT max(week_start) m FROM {catalog}.gold.district_week WHERE arrivals_t_sum IS NOT NULL").collect()[0]["m"]
if mx:
    set_watermark(spark, "gold_district_week", mx, settings=settings)
res["seconds"] = round(time.time() - t0, 1)
res["mode"] = mode
print(res)
dbutils.notebook.exit(json.dumps(res, default=str))
