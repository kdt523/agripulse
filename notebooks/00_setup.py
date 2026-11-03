# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — AgriPulse setup
# MAGIC Creates the catalog, schemas, landing volume and ops tables. Idempotent.

# COMMAND ----------
dbutils.widgets.text("catalog", "agripulse")  # noqa: F821
catalog = dbutils.widgets.get("catalog")  # noqa: F821

# COMMAND ----------
# Free Edition may not allow CREATE CATALOG; fall back to using an existing one and say so.
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")  # noqa: F821
except Exception as exc:  # pragma: no cover - workspace dependent
    print(f"WARN: could not create catalog {catalog}: {exc}. Create it in the UI (see docs/runbook.md).")

spark.sql(f"USE CATALOG {catalog}")  # noqa: F821
for schema in ["bronze", "silver", "gold", "ops", "ml"]:
    spark.sql(  # noqa: F821
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT 'AgriPulse {schema} layer'"
    )

spark.sql(  # noqa: F821
    f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.landing COMMENT 'Raw landed files per source (replayable)'"
)

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.ops.pipeline_runs (
  run_id STRING COMMENT 'Unique id of a pipeline run',
  source STRING COMMENT 'Data source or pipeline name',
  layer STRING COMMENT 'bronze|silver|gold|ml',
  started_at TIMESTAMP COMMENT 'Run start (UTC)',
  duration_s DOUBLE COMMENT 'Wall time in seconds',
  rows_in LONG COMMENT 'Rows read',
  rows_out LONG COMMENT 'Rows written',
  rows_quarantined LONG COMMENT 'Rows routed to quarantine',
  status STRING COMMENT 'success|failed',
  error STRING COMMENT 'Error message if failed'
) COMMENT 'One row per pipeline run'
""")  # noqa: F821

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.ops.ingest_watermarks (
  source STRING COMMENT 'Data source',
  key STRING COMMENT 'Sub-key (e.g. commodity or global)',
  last_date DATE COMMENT 'Last date successfully ingested',
  updated_at TIMESTAMP COMMENT 'When the watermark was updated'
) COMMENT 'Incremental-load watermarks per source'
""")  # noqa: F821

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.ops.freshness_sla (
  source STRING COMMENT 'Data source',
  expected_lag_days INT COMMENT 'Max allowed lag in days',
  owner STRING COMMENT 'Owner of the feed'
) COMMENT 'Freshness SLA per source'
""")  # noqa: F821

# COMMAND ----------
spark.sql(f"""
MERGE INTO {catalog}.ops.freshness_sla t
USING (SELECT * FROM VALUES
  ('weather', 1, 'kd'), ('nasa_power', 3, 'kd'), ('mandi', 2, 'kd'), ('forecast', 0, 'kd')
  AS s(source, expected_lag_days, owner)) s
ON t.source = s.source
WHEN NOT MATCHED THEN INSERT *
""")  # noqa: F821

display(spark.sql(f"SHOW TABLES IN {catalog}.ops"))  # noqa: F821,F841
