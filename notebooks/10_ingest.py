# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Ingest (bronze)
# MAGIC Widgets: `source` (geo|weather|forecast|nasa_power|mandi_csv|crop_csv), `mode` (incremental|backfill),
# MAGIC `start`/`end` (YYYY-MM-DD, backfill only), `n_districts` (0 = all; use 3 for tests), `district_offset`.

# COMMAND ----------
# MAGIC %pip install -q shapely tenacity pydantic-settings
# MAGIC %restart_python

# COMMAND ----------
import json
import os
import sys
from datetime import date

sys.path.insert(0, os.path.abspath("../src"))

dbutils.widgets.text("catalog", "agripulse")  # noqa: F821
dbutils.widgets.dropdown(  # noqa: F821
    "source", "geo", ["geo", "weather", "forecast", "nasa_power", "mandi_csv", "mandi_landing", "mandi_archive", "mandi_api", "crop_csv"]
)
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "backfill", "catchup"])  # noqa: F821
dbutils.widgets.text("start", "")  # noqa: F821
dbutils.widgets.text("end", "")  # noqa: F821
dbutils.widgets.text("n_districts", "0")  # noqa: F821
dbutils.widgets.text("district_offset", "0")  # noqa: F821
dbutils.widgets.text("path", "")  # noqa: F821  (csv path for mandi_csv / crop_csv)

W = {k: dbutils.widgets.get(k) for k in ["catalog", "source", "mode", "start", "end", "n_districts", "district_offset", "path"]}  # noqa: F821
os.environ["AGRIPULSE_CATALOG"] = W["catalog"]

from agripulse.common.config import get_settings  # noqa: E402
from agripulse.common.runlog import logged_run  # noqa: E402

settings = get_settings()


def _d(v):
    return date.fromisoformat(v) if v else None


def load_districts():
    df = spark.table(settings.table("bronze", "geo_districts")).select("district_id", "lat", "lon").orderBy("district_id")  # noqa: F821
    rows = [r.asDict() for r in df.collect()]
    off, n = int(W["district_offset"]), int(W["n_districts"])
    rows = rows[off:]
    return rows[:n] if n else rows


# COMMAND ----------
src, result = W["source"], None
if src == "geo":
    from agripulse.ingest.geo import load_geo_districts

    with logged_run(spark, "geo", "bronze", settings) as rec:  # noqa: F821
        result = load_geo_districts(spark, settings)  # noqa: F821
        rec.rows_in, rec.rows_out = result["rows_in"], result["inserted"]
elif src == "crop_csv":
    from agripulse.ingest.geo import load_crop_area_csv

    with logged_run(spark, "crop_area", "bronze", settings) as rec:  # noqa: F821
        result = load_crop_area_csv(spark, W["path"], settings)  # noqa: F821
        rec.rows_in, rec.rows_out = result["rows_in"], result["inserted"]
elif src == "mandi_archive":  # public 2008-2017 archive; start/end are YEARS here (e.g. 2013 / 2017)
    from agripulse.ingest.agmarknet import ARCHIVE_CROPS, load_mandi_archive

    years = list(range(int(W["start"] or 2013), int(W["end"] or 2017) + 1))
    with logged_run(spark, "mandi_archive", "bronze", settings) as rec:
        result = load_mandi_archive(spark, settings.landing_root, ARCHIVE_CROPS, years, settings)
        rec.rows_in, rec.rows_out = result["rows_in"], result["inserted"]
elif src == "mandi_landing":  # ADF / manual drops: /Volumes/<cat>/bronze/landing/mandi_incoming/*.csv
    import shutil

    from agripulse.ingest.agmarknet import load_mandi_csv

    inbox = os.path.join(settings.landing_root, "mandi_incoming")
    os.makedirs(os.path.join(inbox, "processed"), exist_ok=True)
    files = [f for f in os.listdir(inbox) if f.endswith(".csv")]
    if not files:
        result = {"rows_in": 0, "inserted": 0, "note": "no new files"}
    else:
        with logged_run(spark, "mandi_landing", "bronze", settings) as rec:
            result = load_mandi_csv(spark, os.path.join(inbox, "*.csv"), settings, settings.commodities)
            rec.rows_in, rec.rows_out = result["rows_in"], result["inserted"]
        for f in files:
            shutil.move(os.path.join(inbox, f), os.path.join(inbox, "processed", f))
elif src == "mandi_csv":
    from agripulse.ingest.agmarknet import load_mandi_csv

    with logged_run(spark, "mandi_csv", "bronze", settings) as rec:  # noqa: F821
        result = load_mandi_csv(spark, W["path"], settings, settings.commodities)  # noqa: F821
        rec.rows_in, rec.rows_out = result["rows_in"], result["inserted"]
else:
    if src == "weather":
        from agripulse.ingest.openmeteo import OpenMeteoArchiveIngestor as Cls
    elif src == "forecast":
        from agripulse.ingest.openmeteo import OpenMeteoForecastIngestor as Cls
    elif src == "nasa_power":
        from agripulse.ingest.nasa_power import NasaPowerIngestor as Cls
    else:
        from agripulse.ingest.agmarknet import AgmarknetIngestor as Cls
    ing = Cls(spark, settings=settings) if src == "mandi_api" else Cls(spark, load_districts(), settings=settings)  # noqa: F821
    if W["mode"] == "catchup":  # quota-aware, resumable, per-district (Open-Meteo archive only)
        result = ing.run_catchup(end=_d(W["end"]))
    else:
        result = ing.run(W["mode"], _d(W["start"]), _d(W["end"]))

print(result)
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
