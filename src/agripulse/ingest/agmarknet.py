"""Agmarknet mandi prices.

Two paths, same bronze contract:
- API (data.gov.in resource, needs key; api.data.gov.in is NOT reachable from Databricks Free Edition, so this
  path is meant for ADF / local runs - see docs/runbook.md)
- Historical CSV dumps landed in the Volume (the path used on Databricks).
"""

from __future__ import annotations

import os
import re
from datetime import date, datetime
from typing import Any

from agripulse.common.config import Settings, get_secret
from agripulse.common.http import HttpClient
from agripulse.ingest.base import BaseIngestor

# raw header (normalised) -> canonical bronze column
ALIASES = {
    "state": "state", "state_name": "state",
    "district": "district", "district_name": "district",
    "market": "market", "market_name": "market",
    "commodity": "commodity", "commodity_name": "commodity",
    "variety": "variety",
    "grade": "grade",
    "arrival_date": "arrival_date", "date": "arrival_date", "price_date": "arrival_date",
    "min_price": "min_price", "min_x0020_price": "min_price", "minimum_price": "min_price",
    "max_price": "max_price", "max_x0020_price": "max_price", "maximum_price": "max_price",
    "modal_price": "modal_price", "modal_x0020_price": "modal_price",
    "arrivals": "arrivals_tonnes", "arrivals_tonnes": "arrivals_tonnes", "arrival_tonnes": "arrivals_tonnes",
}  # fmt: skip

CANONICAL = [
    "state", "district", "market", "commodity", "variety", "grade",
    "arrival_date", "min_price", "max_price", "modal_price", "arrivals_tonnes",
]  # fmt: skip

DATE_FORMATS = ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d-%b-%Y", "%d %b %Y", "%m/%d/%Y")


def _norm(h: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", h.strip().lower()).strip("_")


def parse_date(v: Any) -> str | None:
    if v is None or str(v).strip() == "":
        return None
    for f in DATE_FORMATS:
        try:
            return datetime.strptime(str(v).strip(), f).date().isoformat()
        except ValueError:
            continue
    return None


def _num(v: Any) -> float | None:
    try:
        return float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def canonicalise(raw: dict) -> dict:
    """Map one raw record (API or CSV row) to the canonical bronze row. Unknown columns are dropped."""
    out: dict[str, Any] = {c: None for c in CANONICAL}
    for k, v in raw.items():
        c = ALIASES.get(_norm(k))
        if c:
            out[c] = v
    out["arrival_date"] = parse_date(out["arrival_date"])
    for c in ("min_price", "max_price", "modal_price", "arrivals_tonnes"):
        out[c] = _num(out[c])
    for c in ("state", "district", "market", "commodity", "variety", "grade"):
        out[c] = str(out[c]).strip() if out[c] is not None else None
    return out


def parse_api_page(payload: dict) -> list[dict]:
    return [canonicalise(r) for r in payload.get("records", [])]


class AgmarknetIngestor(BaseIngestor):
    """API ingestor (run from ADF/local, or Databricks if egress opens). Pages with offset/limit."""

    source = "mandi"
    table = "mandi_prices_raw"
    keys = ["state", "district", "market", "commodity", "variety", "grade", "arrival_date"]
    cluster_by = ["arrival_date"]
    lag_days = 1
    default_start = date(2023, 1, 1)

    def __init__(self, spark, settings: Settings | None = None, landing_root: str | None = None,
                 client: HttpClient | None = None, page_size: int = 1000, api_key: str | None = None):  # fmt: skip
        super().__init__(spark, settings, landing_root)
        self.client = client or HttpClient(self.settings)
        self.page_size, self._key = page_size, api_key

    def fetch(self, start: date, end: date) -> list[dict]:
        key = self._key or get_secret(self.settings.datagov_secret_key, self.settings.secret_scope)
        rows: list[dict] = []
        for commodity in self.settings.commodities:
            offset = 0
            while True:
                params = {
                    "api-key": key, "format": "json", "limit": self.page_size, "offset": offset,
                    "filters[commodity]": commodity,
                }  # fmt: skip
                recs = parse_api_page(self.client.get_json(self.settings.agmarknet_url, params))
                if not recs:
                    break
                rows += [r for r in recs if r["arrival_date"] and start.isoformat() <= r["arrival_date"] <= end.isoformat()]
                offset += self.page_size
        return rows

    def cast(self, df):
        from pyspark.sql import functions as F

        return df.withColumn("arrival_date", F.to_date("arrival_date"))


def load_mandi_csv(spark, path: str, settings: Settings | None = None, commodities: list[str] | None = None) -> dict:
    """Historical CSV dump(s) in the Volume -> canonical rows -> MERGE into bronze.mandi_prices_raw."""
    from pyspark.sql import functions as F

    from agripulse.common.config import get_settings

    s = settings or get_settings()
    df = spark.read.option("header", True).csv(path)
    mapping = {c: ALIASES[_norm(c)] for c in df.columns if _norm(c) in ALIASES}
    # keep first column per canonical name
    seen: dict[str, str] = {}
    for raw, canon in mapping.items():
        seen.setdefault(canon, raw)
    sel = []
    for c in CANONICAL:
        sel.append(F.col(f"`{seen[c]}`").alias(c) if c in seen else F.lit(None).cast("string").alias(c))
    df = df.select(*sel)

    return _standardise_and_merge(spark, df, s, "agmarknet_csv", path, commodities)


def _standardise_and_merge(spark, df, s, source: str, request_params: str, commodities: list[str] | None = None) -> dict:
    """df has canonical string columns -> typed bronze rows -> MERGE (shared by CSV and archive loaders)."""
    import uuid
    from datetime import timezone

    from pyspark.sql import functions as F

    from agripulse.common.delta_io import merge_into

    def spark_fmt(f: str) -> str:
        return f.replace("%d", "dd").replace("%m", "MM").replace("%Y", "yyyy").replace("%b", "MMM")

    date_expr = F.coalesce(*[F.try_to_timestamp(F.col("arrival_date"), F.lit(spark_fmt(f))).cast("date") for f in DATE_FORMATS])
    df = df.withColumn("arrival_date", date_expr)
    for c in ("min_price", "max_price", "modal_price", "arrivals_tonnes"):
        df = df.withColumn(c, F.regexp_replace(F.col(c).cast("string"), ",", "").cast("double"))
    for c in ("state", "district", "market", "commodity", "variety", "grade"):
        df = df.withColumn(c, F.trim(F.col(c)))
    if commodities:
        df = df.filter(F.col("commodity").isin(commodities))
    df = (
        df.withColumn("_ingested_at", F.lit(datetime.now(timezone.utc)).cast("timestamp"))
        .withColumn("_source", F.lit(source))
        .withColumn("_run_id", F.lit(uuid.uuid4().hex))
        .withColumn("_request_params", F.lit(request_params))
    )
    return merge_into(spark, df, s.table("bronze", "mandi_prices_raw"), AgmarknetIngestor.keys, ["arrival_date"])


# ---- public historical archive (iancovert/Agmarknet, 2008-Jun 2017): one headerless CSV per crop-year ------------
ARCHIVE_BASE = "https://raw.githubusercontent.com/iancovert/Agmarknet/master"
ARCHIVE_COLUMNS = ["state", "district", "market", "variety", "group", "arrivals_tonnes",
                   "min_price", "max_price", "modal_price", "arrival_date"]  # fmt: skip
ARCHIVE_CROPS = ["Maize", "Rice", "Soyabean", "Wheat", "Arhar (Tur-Red Gram)", "Green Gram (Moong)"]


def download_archive(landing_root: str, crops: list[str], years: list[int], client: HttpClient | None = None) -> list[str]:
    """Download crop-year CSVs into the landing volume (replayable). Returns local paths."""
    import os
    from urllib.parse import quote

    client = client or HttpClient()
    out = []
    for crop in crops:
        folder = os.path.join(landing_root, "mandi_archive", crop)
        os.makedirs(folder, exist_ok=True)
        for y in years:
            dest = os.path.join(folder, f"{y}.csv")
            if not os.path.exists(dest):
                try:
                    text = client.get_text(f"{ARCHIVE_BASE}/{quote(crop)}/{y}.csv")
                except Exception:  # not every crop has every year (e.g. White Peas)
                    continue
                with open(dest, "w", encoding="utf-8") as fh:
                    fh.write(text)
            out.append(dest)
    return out


def load_mandi_archive(spark, landing_root: str, crops: list[str], years: list[int], settings: Settings | None = None) -> dict:
    from pyspark.sql import functions as F

    from agripulse.common.config import get_settings

    s = settings or get_settings()
    paths = download_archive(landing_root, crops, years)
    schema = ", ".join(f"{c} string" for c in ARCHIVE_COLUMNS)
    df = None
    for crop in crops:
        crop_paths = [p for p in paths if f"mandi_archive{os.sep}{crop}{os.sep}" in p]
        if not crop_paths:
            continue
        part = spark.read.schema(schema).option("header", False).csv(crop_paths).withColumn("commodity", F.lit(crop))
        df = part if df is None else df.unionByName(part)
    df = df.withColumn("grade", F.col("group")).select(*CANONICAL)
    params = f"iancovert/Agmarknet {','.join(crops)} {years[0]}-{years[-1]}"
    return _standardise_and_merge(spark, df, s, "agmarknet_archive", params)
