# Databricks notebook source
# MAGIC %md
# MAGIC # 60 — Freshness / DQ / volume checks and alerting
# MAGIC `sla_override` (e.g. `weather=-1`) is for the forced stale-feed test: the alert must fire.

# COMMAND ----------
# MAGIC %pip install -q pydantic-settings tenacity
# MAGIC %restart_python

# COMMAND ----------
import json
import os
import sys

sys.path.insert(0, os.path.abspath("../src"))
dbutils.widgets.text("catalog", "agripulse")
dbutils.widgets.text("sla_override", "")
dbutils.widgets.text("run_url", "")
os.environ["AGRIPULSE_CATALOG"] = dbutils.widgets.get("catalog")

from agripulse.quality.freshness import run_checks

override = {}
for part in filter(None, dbutils.widgets.get("sla_override").split(",")):
    k, v = part.split("=")
    override[k.strip()] = int(v)

for ddl in [
    "CREATE TABLE IF NOT EXISTS {c}.ops.freshness_status (source STRING COMMENT 'Data source', checked_at TIMESTAMP COMMENT 'When checked', max_date DATE COMMENT 'Latest data date', lag_days INT COMMENT 'today - max_date', sla_days INT COMMENT 'Allowed lag (NULL = static archive)', breached BOOLEAN COMMENT 'lag > sla') COMMENT 'Freshness check history'",
    "CREATE TABLE IF NOT EXISTS {c}.ops.alert_log (alert_ts TIMESTAMP COMMENT 'When raised', kind STRING COMMENT 'freshness|dq|volume', source STRING COMMENT 'Source or table', message STRING COMMENT 'Human-readable detail', delivered BOOLEAN COMMENT 'Webhook delivered') COMMENT 'Every alert raised, whether or not Slack delivery worked'",
]:
    spark.sql(ddl.format(c=os.environ["AGRIPULSE_CATALOG"]))

res = run_checks(spark, sla_override=override, run_url=dbutils.widgets.get("run_url") or None)
print(res)
dbutils.notebook.exit(json.dumps(res, default=str))
