"""Delta helpers: idempotent MERGE on natural keys and watermark get/set (CLAUDE.md rules 3-4)."""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timezone

from agripulse.common.config import Settings, get_settings


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _table_exists(spark, table: str) -> bool:
    try:
        return spark.catalog.tableExists(table)
    except Exception:
        return False


def merge_into(spark, df, table: str, keys: list[str], cluster_by: list[str] | None = None) -> dict:
    """MERGE df into table on natural keys. Re-running the same data never duplicates rows.

    Source is de-duplicated on keys first (latest `_ingested_at` wins). Returns counts.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if "_ingested_at" in df.columns:
        w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc())
        df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    else:
        df = df.dropDuplicates(keys)

    if not _table_exists(spark, table):
        writer = df.limit(0).write.format("delta")
        if cluster_by and on_databricks():
            writer = writer.clusterBy(*cluster_by)
        writer.saveAsTable(table)

    rows_in = df.count()
    before = spark.table(table).count()
    view = f"_src_{uuid.uuid4().hex[:8]}"
    df.createOrReplaceTempView(view)
    cond = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    spark.sql(
        f"MERGE INTO {table} t USING {view} s ON {cond} "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
    after = spark.table(table).count()
    return {"rows_in": rows_in, "inserted": after - before, "total": after}


def get_watermark(spark, source: str, key: str = "global", settings: Settings | None = None) -> date | None:
    s = settings or get_settings()
    tbl = s.table("ops", "ingest_watermarks")
    rows = spark.sql(
        f"SELECT last_date FROM {tbl} WHERE source = '{source}' AND key = '{key}'"
    ).collect()
    return rows[0]["last_date"] if rows else None


def set_watermark(spark, source: str, last_date: date, key: str = "global", settings: Settings | None = None) -> None:
    s = settings or get_settings()
    tbl = s.table("ops", "ingest_watermarks")
    now = datetime.now(timezone.utc)
    df = spark.createDataFrame(
        [(source, key, last_date, now)], "source string, key string, last_date date, updated_at timestamp"
    )
    df.createOrReplaceTempView("_wm_src")
    spark.sql(
        f"MERGE INTO {tbl} t USING _wm_src s ON t.source = s.source AND t.key = s.key "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
