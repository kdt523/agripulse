"""BaseIngestor: fetch() -> land() -> load_bronze() -> update_watermark() -> log_run()."""

from __future__ import annotations

import json
import os
import time
import uuid
from abc import ABC, abstractmethod
from datetime import date, datetime, timezone

from agripulse.common.config import Settings, get_settings
from agripulse.common.delta_io import get_watermark, merge_into, set_watermark
from agripulse.common.logging import get_logger
from agripulse.common.runlog import RunRecord, write_run
from agripulse.ingest.windows import resolve_window

log = get_logger("agripulse.ingest")


class BaseIngestor(ABC):
    source: str = ""  # also the watermark / landing folder name
    table: str = ""  # bronze table name (without catalog.schema)
    keys: list[str] = []
    cluster_by: list[str] = []
    lag_days: int = 0
    default_start: date = date(2020, 1, 1)

    def __init__(self, spark, settings: Settings | None = None, landing_root: str | None = None):
        self.spark = spark
        self.settings = settings or get_settings()
        self.landing_root = landing_root or self.settings.landing_root
        self.run_id = uuid.uuid4().hex

    # ---- to implement per source -------------------------------------------------
    @abstractmethod
    def fetch(self, start: date, end: date) -> list[dict]:
        """Return a list of flat row dicts (already typed enough: ISO dates as strings, numbers as floats)."""

    # ---- shared steps ------------------------------------------------------------
    def land(self, rows: list[dict], start: date, end: date) -> str | None:
        """Write raw rows as JSON-lines to the landing volume (replayable)."""
        if not rows:
            return None
        folder = os.path.join(self.landing_root, self.source, f"dt={date.today().isoformat()}")
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, f"{start.isoformat()}_{end.isoformat()}_{self.run_id}.jsonl")
        with open(path, "w", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, default=str) + "\n")
        return path

    def load_bronze(self, path: str) -> dict:
        from pyspark.sql import functions as F

        df = self.spark.read.json(path)
        df = self.cast(df)
        df = (
            df.withColumn("_ingested_at", F.lit(datetime.now(timezone.utc)).cast("timestamp"))
            .withColumn("_source", F.lit(self.source))
            .withColumn("_run_id", F.lit(self.run_id))
        )
        return merge_into(
            self.spark, df, self.settings.table("bronze", self.table), self.keys, self.cluster_by or None
        )

    def cast(self, df):
        """Override to cast columns (e.g. date strings -> DATE)."""
        return df

    def run(self, mode: str = "incremental", start: date | None = None, end: date | None = None) -> dict:
        t0 = time.monotonic()
        rec = RunRecord(source=self.source, layer="bronze", run_id=self.run_id)
        try:
            wm = get_watermark(self.spark, self.source, settings=self.settings)
            win = resolve_window(
                mode=mode,
                watermark=wm,
                today=date.today(),
                lag_days=self.lag_days,
                default_start=self.default_start,
                start=start,
                end=end,
            )
            if win is None:
                log.info("nothing to load", extra={"ctx": {"source": self.source}})
                rec.status = "success"
                return {"rows_in": 0, "inserted": 0, "window": None}
            s, e = win
            rows = self.fetch(s, e)
            rec.rows_in = len(rows)
            path = self.land(rows, s, e)
            result = self.load_bronze(path) if path else {"rows_in": 0, "inserted": 0}
            rec.rows_out = result["inserted"]
            if path and (wm is None or e > wm):  # watermark only ever advances
                set_watermark(self.spark, self.source, e, settings=self.settings)
            rec.status = "success"
            result["window"] = (s.isoformat(), e.isoformat())
            return result
        except Exception as exc:
            rec.status, rec.error = "failed", str(exc)[:2000]
            raise
        finally:
            rec.duration_s = time.monotonic() - t0
            write_run(self.spark, rec, self.settings)
