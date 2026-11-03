"""NASA POWER daily agro-climate (community=AG) per district centroid.

Fill value -999 is kept raw in bronze and flagged/quarantined in silver (design rule).
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime

from agripulse.common.config import Settings
from agripulse.common.http import HttpClient
from agripulse.ingest.base import BaseIngestor

PARAMS = ["ALLSKY_SFC_SW_DWN", "GWETROOT", "GWETTOP", "T2M", "PRECTOTCORR", "RH2M"]


def parse_response(payload: dict, district_id: str) -> list[dict]:
    """{'properties': {'parameter': {P: {YYYYMMDD: v}}}} -> flat rows keyed by district/date."""
    param_block = payload["properties"]["parameter"]
    dates = sorted({d for p in param_block.values() for d in p})
    rows = []
    for d in dates:
        row = {"district_id": district_id, "date": datetime.strptime(d, "%Y%m%d").date().isoformat()}
        for p in PARAMS:
            row[p.lower()] = param_block.get(p, {}).get(d)
        rows.append(row)
    return rows


class NasaPowerIngestor(BaseIngestor):
    source = "nasa_power"
    table = "nasa_power_raw"
    keys = ["district_id", "date"]
    cluster_by = ["district_id", "date"]
    lag_days = 4  # POWER lags real time by a few days
    default_start = date(2020, 1, 1)

    def __init__(self, spark, districts: list[dict], workers: int = 4, settings: Settings | None = None,
                 landing_root: str | None = None, client: HttpClient | None = None):  # fmt: skip
        super().__init__(spark, settings, landing_root)
        self.districts, self.workers = districts, workers
        self.client = client or HttpClient(self.settings)

    def _one(self, d: dict, s: date, e: date) -> list[dict]:
        params = {
            "parameters": ",".join(PARAMS),
            "community": "AG",
            "longitude": d["lon"],
            "latitude": d["lat"],
            "start": s.strftime("%Y%m%d"),
            "end": e.strftime("%Y%m%d"),
            "format": "JSON",
        }
        payload = self.client.get_json(self.settings.nasa_power_url, params)
        rows = parse_response(payload, d["district_id"])
        for r in rows:
            r["_request_params"] = f"{s}..{e}"
        return rows

    def fetch(self, start: date, end: date) -> list[dict]:
        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            results = list(ex.map(lambda d: self._one(d, start, end), self.districts))
        return [r for chunk in results for r in chunk]

    def cast(self, df):
        from pyspark.sql import functions as F

        return df.withColumn("date", F.to_date("date"))
