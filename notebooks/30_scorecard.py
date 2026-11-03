# Databricks notebook source
# MAGIC %md
# MAGIC # 30 — DQ scorecard
# MAGIC Writes today's row per silver table to `ops.dq_scorecard`.

# COMMAND ----------
# MAGIC %pip install -q pydantic-settings tenacity
# MAGIC %restart_python

# COMMAND ----------
import json
import os
import sys

sys.path.insert(0, os.path.abspath("../src"))
dbutils.widgets.text("catalog", "agripulse")
os.environ["AGRIPULSE_CATALOG"] = dbutils.widgets.get("catalog")

from agripulse.quality.scorecard import write_scorecard

result = write_scorecard(spark)
print(result)
dbutils.notebook.exit(json.dumps(result))
