"""Open-Meteo archive (history) + forecast ingestors. Multi-coordinate requests + thread pool."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

from agripulse.common.config import Settings
from agripulse.common.http import HttpClient
from agripulse.ingest.base import BaseIngestor
from agripulse.ingest.windows import chunk_dates

DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "rain_sum",
    "et0_fao_evapotranspiration",
    "relative_humidity_2m_mean",
    "wind_speed_10m_max",
]


def parse_response(payload, district_ids: list[str], extra: dict | None = None) -> list[dict]:
    """Open-Meteo returns a dict for 1 location and a list for several; normalise to flat rows."""
    items = payload if isinstance(payload, list) else [payload]
    if len(items) != len(district_ids):
        raise ValueError(f"expected {len(district_ids)} locations, got {len(items)}")
    rows = []
    for did, item in zip(district_ids, items, strict=True):
        daily = item.get("daily") or {}
        times = daily.get("time") or []
        for i, t in enumerate(times):
            row = {"district_id": did, "date": t}
            for v in DAILY_VARS:
                vals = daily.get(v)
                row[v] = vals[i] if vals is not None else None
            if extra:
                row.update(extra)
            rows.append(row)
    return rows


class OpenMeteoArchiveIngestor(BaseIngestor):
    source = "weather"
    table = "weather_daily_raw"
    keys = ["district_id", "date"]
    cluster_by = ["district_id", "date"]
    lag_days = 2  # ERA5 archive lags ~2 days
    # Free-tier quota (~10k weighted calls/day) fits ~6 months x 641 districts per day; NASA POWER holds the
    # 5-year history (see docs/runbook.md). Catch-up extends this as quota allows.
    default_start = date(2026, 5, 1)

    def __init__(self, spark, districts: list[dict], batch_size: int = 10, chunk_days: int = 366,
                 workers: int = 3, settings: Settings | None = None, landing_root: str | None = None,
                 client: HttpClient | None = None):  # fmt: skip
        super().__init__(spark, settings, landing_root)
        self.districts = districts  # [{district_id, lat, lon}]
        self.batch_size, self.chunk_days, self.workers = batch_size, chunk_days, workers
        self.client = client or HttpClient(self.settings)

    def _one(self, batch: list[dict], s: date, e: date) -> list[dict]:
        params = {
            "latitude": ",".join(str(d["lat"]) for d in batch),
            "longitude": ",".join(str(d["lon"]) for d in batch),
            "start_date": s.isoformat(),
            "end_date": e.isoformat(),
            "daily": ",".join(DAILY_VARS),
            "timezone": "Asia/Kolkata",
        }
        payload = self.client.get_json(self.settings.open_meteo_archive_url, params)
        rows = parse_response(payload, [d["district_id"] for d in batch])
        req = f"{s}..{e}"
        for r in rows:
            r["_request_params"] = req
        return rows

    def fetch(self, start: date, end: date) -> list[dict]:
        jobs = []
        for cs, ce in chunk_dates(start, end, self.chunk_days):
            for i in range(0, len(self.districts), self.batch_size):
                jobs.append((self.districts[i : i + self.batch_size], cs, ce))
        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            results = list(ex.map(lambda j: self._one(*j), jobs))
        return [r for chunk in results for r in chunk]

    def cast(self, df):
        from pyspark.sql import functions as F

        return df.withColumn("date", F.to_date("date"))

    # ---- quota-aware, resumable catch-up --------------------------------------------------------
    @staticmethod
    def request_weight(n_locations: int, n_days: int) -> float:
        """Open-Meteo bills locations x max(1, days/14) (variables < 10 add nothing)."""
        return n_locations * max(1.0, n_days / 14.0)

    def run_catchup(self, end: date | None = None, daily_budget: float = 9000.0,
                    weight_per_hour: float = 4500.0, max_runtime_s: float = 3.2 * 3600) -> dict:  # fmt: skip
        """Per-district watermarks (ops.ingest_watermarks key=district_id). Each call pulls as many
        (batch x year) requests as the free-tier budget allows, then stops; the next call resumes.
        Throttles to stay under the hourly weight cap. Safe to re-run: MERGE is idempotent."""
        import time

        from agripulse.common.delta_io import district_watermarks, set_district_watermarks
        from agripulse.ingest.windows import plan_catchup

        t0 = time.monotonic()
        target = end or (date.today() - timedelta(days=self.lag_days))
        wms = district_watermarks(self.spark, self.source, self.settings)
        plan = plan_catchup([d["district_id"] for d in self.districts], wms, target, self.default_start,
                            self.batch_size, self.chunk_days)  # fmt: skip
        by_id = {d["district_id"]: d for d in self.districts}
        all_ids = list(by_id)
        total_planned = len(plan)
        spent, requests_done, rows_in, inserted = 0.0, 0, 0, 0
        while plan:
            ids, cs, ce = plan[0]
            weight = self.request_weight(len(ids), (ce - cs).days + 1)
            if spent + weight > daily_budget or time.monotonic() - t0 > max_runtime_s:
                break
            batch = [by_id[i] for i in ids]
            rows = self._one(batch, cs, ce)
            path = self.land(rows, cs, ce)
            res = self.load_bronze(path) if path else {"rows_in": 0, "inserted": 0}
            set_district_watermarks(self.spark, self.source, ids, ce, self.settings)
            spent += weight
            requests_done += 1
            rows_in += res["rows_in"]
            inserted += res["inserted"]
            for i in ids:
                wms[i] = ce
            plan = plan_catchup(all_ids, wms, target, self.default_start, self.batch_size, self.chunk_days)
            time.sleep(weight / weight_per_hour * 3600.0)  # honour the hourly cap
        pending = {i for i in all_ids if wms.get(i) is None or wms[i] < target}
        return {"requests": requests_done, "weight_spent": round(spent, 1), "rows_in": rows_in,
                "inserted": inserted, "districts_pending": len(pending), "districts_total": len(all_ids),
                "initial_plan_requests": total_planned, "target": target.isoformat()}  # fmt: skip


class OpenMeteoForecastIngestor(OpenMeteoArchiveIngestor):
    """Next-16-day forecast, keyed by issue date so we never overwrite what was known at the time."""

    source = "forecast"
    table = "weather_forecast_raw"
    keys = ["district_id", "date", "issued_date"]
    lag_days = 0

    def fetch(self, start: date, end: date) -> list[dict]:  # window is ignored: forecast is "now"
        issued = datetime.now(timezone.utc).date()
        rows: list[dict] = []
        for i in range(0, len(self.districts), self.batch_size):
            batch = self.districts[i : i + self.batch_size]
            params = {
                "latitude": ",".join(str(d["lat"]) for d in batch),
                "longitude": ",".join(str(d["lon"]) for d in batch),
                "daily": ",".join(DAILY_VARS),
                "forecast_days": 16,
                "timezone": "Asia/Kolkata",
            }
            payload = self.client.get_json(self.settings.open_meteo_forecast_url, params)
            rows += parse_response(
                payload, [d["district_id"] for d in batch],
                extra={"issued_date": issued.isoformat(), "_request_params": "forecast_days=16"},
            )  # fmt: skip
        return rows

    def run(self, mode: str = "incremental", start=None, end=None) -> dict:
        today = date.today()
        return super().run("backfill", start=today, end=today + timedelta(days=16))

    def cast(self, df):
        from pyspark.sql import functions as F

        return df.withColumn("date", F.to_date("date")).withColumn("issued_date", F.to_date("issued_date"))
