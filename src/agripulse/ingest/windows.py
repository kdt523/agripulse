"""Pure date-window logic for incremental vs backfill loads (unit-testable, no Spark)."""

from __future__ import annotations

from datetime import date, timedelta


def resolve_window(
    *,
    mode: str,
    watermark: date | None,
    today: date,
    lag_days: int,
    default_start: date,
    start: date | None = None,
    end: date | None = None,
) -> tuple[date, date] | None:
    """Return (start, end) inclusive, or None when there is nothing to load.

    - backfill: explicit start/end (end defaults to today - lag).
    - incremental: watermark + 1 .. today - lag; first run starts at default_start.
    """
    latest_available = today - timedelta(days=lag_days)
    if mode == "backfill":
        if start is None:
            raise ValueError("backfill mode needs a start date")
        s, e = start, min(end or latest_available, latest_available)
    elif mode == "incremental":
        s = (watermark + timedelta(days=1)) if watermark else default_start
        e = latest_available
    else:
        raise ValueError(f"unknown mode {mode!r}")
    return (s, e) if s <= e else None


def chunk_dates(start: date, end: date, days: int) -> list[tuple[date, date]]:
    """Split [start, end] into consecutive inclusive chunks of at most `days` days."""
    out, cur = [], start
    while cur <= end:
        nxt = min(cur + timedelta(days=days - 1), end)
        out.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return out
