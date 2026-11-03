# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Network probe
# MAGIC Checks which external source domains are reachable from serverless compute (Free Edition restricts egress).

# COMMAND ----------
import requests

PROBES = {
    "open_meteo_archive": "https://archive-api.open-meteo.com/v1/archive?latitude=18.5&longitude=73.8&start_date=2024-01-01&end_date=2024-01-03&daily=temperature_2m_max",
    "open_meteo_forecast": "https://api.open-meteo.com/v1/forecast?latitude=18.5&longitude=73.8&daily=temperature_2m_max&forecast_days=2",
    "nasa_power": "https://power.larc.nasa.gov/api/temporal/daily/point?parameters=T2M&community=AG&longitude=73.8&latitude=18.5&start=20240101&end=20240103&format=JSON",
    "github_raw": "https://raw.githubusercontent.com/datameet/maps/master/Districts/Census_2011/2011_Dist.json",
    "data_gov_in": "https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070?format=json&limit=1",
    "pypi": "https://pypi.org/simple/shapely/",
}
results = {}
for name, url in PROBES.items():
    try:
        r = requests.get(url, timeout=30)
        results[name] = f"HTTP {r.status_code}, {len(r.content)} bytes"
    except Exception as exc:
        results[name] = f"FAIL: {type(exc).__name__}: {str(exc)[:150]}"
import json

for k, v in results.items():
    print(f"{k}: {v}")
dbutils.notebook.exit(json.dumps(results))  # noqa: F821
