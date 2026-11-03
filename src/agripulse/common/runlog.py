"""Run logger: every pipeline run is recorded in ops.pipeline_runs (CLAUDE.md rule 6)."""

from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone

from agripulse.common.config import Settings, get_settings

RUN_COLUMNS = (
    "run_id", "source", "layer", "started_at", "duration_s",
    "rows_in", "rows_out", "rows_quarantined", "status", "error",
)  # fmt: skip


@dataclass
class RunRecord:
    source: str
    layer: str
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    duration_s: float = 0.0
    rows_in: int = 0
    rows_out: int = 0
    rows_quarantined: int = 0
    status: str = "running"
    error: str | None = None

    def as_row(self) -> tuple:
        return (
            self.run_id, self.source, self.layer, self.started_at, self.duration_s,
            self.rows_in, self.rows_out, self.rows_quarantined, self.status, self.error,
        )  # fmt: skip


def write_run(spark, record: RunRecord, settings: Settings | None = None) -> None:
    """Append a finished run record to ops.pipeline_runs."""
    settings = settings or get_settings()
    schema = (
        "run_id string, source string, layer string, started_at timestamp, duration_s double, "
        "rows_in long, rows_out long, rows_quarantined long, status string, error string"
    )
    df = spark.createDataFrame([record.as_row()], schema=schema)
    df.write.mode("append").saveAsTable(settings.table("ops", "pipeline_runs"))


@contextmanager
def logged_run(spark, source: str, layer: str, settings: Settings | None = None):
    """Context manager: yields a RunRecord; on exit writes success/failed with duration."""
    rec = RunRecord(source=source, layer=layer)
    t0 = time.monotonic()
    try:
        yield rec
        rec.status = "success"
    except Exception as exc:
        rec.status = "failed"
        rec.error = str(exc)[:2000]
        raise
    finally:
        rec.duration_s = time.monotonic() - t0
        write_run(spark, rec, settings)
