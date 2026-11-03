# Databricks notebook source
# MAGIC %md
# MAGIC # 50 — Apply Unity Catalog metadata and report coverage
# MAGIC Comments + tags come from `docs/catalog.yml`. The notebook exits with the list of anything still undocumented.

# COMMAND ----------
# MAGIC %pip install -q pyyaml
# MAGIC %restart_python

# COMMAND ----------
import json
import os
import sys

sys.path.insert(0, os.path.abspath("../src"))
dbutils.widgets.text("catalog", "agripulse")
dbutils.widgets.text("write_markdown_to", "")  # e.g. /Volumes/agripulse/bronze/landing/exports/DATA_CATALOG.md
catalog = dbutils.widgets.get("catalog")

from agripulse.quality.catalog_export import apply_metadata, load, missing_comments, render_markdown

cfg = load(os.path.abspath("../docs/catalog.yml"))
res = apply_metadata(spark, cfg, catalog)
missing = missing_comments(spark, catalog)
res["missing_comments"] = missing
res["missing_count"] = len(missing)

out = dbutils.widgets.get("write_markdown_to")
if out:
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(render_markdown(spark, cfg, catalog))
print(res)
dbutils.notebook.exit(json.dumps(res))
