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


def plan_catchup(
    district_ids: list[str],
    watermarks: dict[str, date],
    target: date,
    default_start: date,
    batch_size: int,
    chunk_days: int,
) -> list[tuple[list[str], date, date]]:
    """Work list of (district batch, chunk_start, chunk_end) still missing up to `target`.

    Districts sharing the same resume date are batched together, oldest data first, so a
    budget-limited run always leaves a consistent, resumable state.
    """
    groups: dict[date, list[str]] = {}
    for did in sorted(district_ids):
        wm = watermarks.get(did)
        start = wm + timedelta(days=1) if wm else default_start
        if start <= target:
            groups.setdefault(start, []).append(did)
    plan: list[tuple[list[str], date, date]] = []
    for start in sorted(groups):
        ids = groups[start]
        for i in range(0, len(ids), batch_size):
            batch = ids[i : i + batch_size]
            plan.append((batch, *chunk_dates(start, target, chunk_days)[0]))
    return plan


def chunk_dates(start: date, end: date, days: int) -> list[tuple[date, date]]:
    """Split [start, end] into consecutive inclusive chunks of at most `days` days."""
    out, cur = [], start
    while cur <= end:
        nxt = min(cur + timedelta(days=days - 1), end)
        out.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return out
