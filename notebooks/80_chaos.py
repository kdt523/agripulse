# Databricks notebook source
# MAGIC %md
# MAGIC # 80 — Chaos data test
# MAGIC Injects 50 faults (10 types x 5) into a sandbox copy (`chaos_bronze`), runs the silver pipeline on the copy and measures
# MAGIC detection. `label` = before / after (compare before and after fixing any miss). Never touches the real schemas.

# COMMAND ----------
# MAGIC %pip install -q pydantic-settings tenacity
# MAGIC %restart_python

# COMMAND ----------
import json
import os
import sys
import time

import pandas as pd

sys.path.insert(0, os.path.abspath("../src"))
dbutils.widgets.text("catalog", "agripulse")
dbutils.widgets.text("label", "before")
dbutils.widgets.text("pipeline_id", "")
catalog, label, pipeline_id = (dbutils.widgets.get(k) for k in ("catalog", "label", "pipeline_id"))
os.environ["AGRIPULSE_CATALOG"] = catalog

from databricks.sdk import WorkspaceClient

from agripulse.chaos import fault_injector as fi

w = WorkspaceClient()


def run_pipeline(full_refresh=True):
    upd = w.pipelines.start_update(pipeline_id, full_refresh=full_refresh)
    while True:
        u = w.pipelines.get_update(pipeline_id, upd.update_id).update
        if u.state.value in ("COMPLETED", "FAILED", "CANCELED"):
            break
        time.sleep(10)
    err = ""
    if u.state.value != "COMPLETED":
        evs = w.pipelines.list_pipeline_events(pipeline_id, filter="level='ERROR'", max_results=25)
        err = " ".join(f"{e.message or ''} {e.error or ''}" for e in evs)
    return u.state.value, err


# COMMAND ----------
ids = fi.build_sandbox(spark, catalog)
t0 = time.time()
state0, err0 = run_pipeline()
assert state0 == "COMPLETED", f"baseline pipeline failed: {err0[:500]}"
baseline = {
    t: spark.table(f"{catalog}.chaos_silver.{t}_quarantine").count()
    for t in ("weather_daily", "agro_climate_daily", "mandi_prices_daily")
}
baseline_rows = spark.table(f"{catalog}.chaos_silver.mandi_prices_daily").count()
print("baseline (clean sandbox) quarantine:", baseline)

# COMMAND ----------
row_log = fi.inject(spark, catalog)
state1, err1 = run_pipeline()
assert state1 == "COMPLETED", f"fault run failed unexpectedly: {err1[:500]}"
row_res = fi.evaluate_rows(spark, catalog, row_log)

# COMMAND ----------
drift_log = fi.inject_schema_drift(spark, catalog)
state2, err2 = run_pipeline()
drift_res = fi.evaluate_drift(drift_log, state2 != "COMPLETED", err2)
logic_res, controls = fi.evaluate_logic(fi.logic_faults())

# COMMAND ----------
results = pd.concat([row_res, drift_res, logic_res], ignore_index=True)
results["label"] = label
summary = fi.summarize(results)
summary.update({"label": label, "districts_in_sandbox": len(ids), "baseline_quarantine": baseline, "controls": controls,
                "drift_update_state": state2, "seconds": round(time.time() - t0)})  # fmt: skip
sdf = spark.createDataFrame(results.astype(str))
sdf.write.format("delta").mode("append").saveAsTable(f"{catalog}.chaos_ops.chaos_results") if spark.catalog.tableExists(f"{catalog}.chaos_ops.chaos_results") else sdf.write.format("delta").saveAsTable(f"{catalog}.chaos_ops.chaos_results")
missed = results[~results.detected][["fault_id", "type", "variant", "expected_detector"]].to_dict("records")
summary["missed"] = missed
print(json.dumps(summary, indent=2, default=str))
dbutils.notebook.exit(json.dumps(summary, default=str))
