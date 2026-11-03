# Databricks notebook source
# MAGIC %md
# MAGIC # 90 — Benchmarks
# MAGIC 1. Delta layout: unclustered copy vs liquid-clustered + OPTIMIZE copy, typical DS query latency.
# MAGIC 2. API ingestion: sequential vs batched multi-coordinate vs thread pool, wall time.
# MAGIC 3. Full vs incremental run time comes from `ops.pipeline_runs` (queried separately, see docs/results.md).

# COMMAND ----------
# MAGIC %pip install -q pydantic-settings tenacity
# MAGIC %restart_python

# COMMAND ----------
import json
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date

sys.path.insert(0, os.path.abspath("../src"))
dbutils.widgets.text("catalog", "agripulse")
catalog = dbutils.widgets.get("catalog")
os.environ["AGRIPULSE_CATALOG"] = catalog
out = {}

# COMMAND ----------
# 1) Delta layout benchmark -------------------------------------------------------------------------------------------
src = f"{catalog}.bronze.nasa_power_raw"
plain, clustered = f"{catalog}.ops.bench_nasa_plain", f"{catalog}.ops.bench_nasa_clustered"
spark.sql(f"CREATE OR REPLACE TABLE {plain} AS SELECT * FROM {src} ORDER BY rand()")  # deliberately unordered layout
spark.sql(f"CREATE OR REPLACE TABLE {clustered} CLUSTER BY (district_id, date) AS SELECT * FROM {src}")
t0 = time.time()
spark.sql(f"OPTIMIZE {clustered}")
out["optimize_seconds"] = round(time.time() - t0, 1)

ids = [r[0] for r in spark.sql(f"SELECT DISTINCT district_id FROM {src} ORDER BY district_id LIMIT 400").collect()][::80][:5]
q = (
    "SELECT district_id, avg(t2m) a, sum(prectotcorr) p FROM {t} WHERE district_id IN ({ids}) "
    "AND date BETWEEN '2016-01-01' AND '2016-12-31' GROUP BY district_id"
).format(ids=",".join(f"'{i}'" for i in ids), t="{t}")


def bench(table, runs=9):
    ts = []
    for _ in range(runs):
        t = time.time()
        spark.sql(q.format(t=table)).collect()
        ts.append(time.time() - t)
    return ts[1:]  # drop the first (cold) run


a, b = bench(plain), bench(clustered)
det = lambda t: spark.sql(f"DESCRIBE DETAIL {t}").collect()[0].asDict()  # noqa: E731
out["delta_layout"] = {
    "rows": spark.table(src).count(),
    "plain_median_s": round(statistics.median(a), 3), "clustered_median_s": round(statistics.median(b), 3),
    "plain_files": det(plain)["numFiles"], "clustered_files": det(clustered)["numFiles"],
    "speedup_x": round(statistics.median(a) / statistics.median(b), 2), "runs_each": len(a),
    "query": "5 districts x 1 year, avg/sum aggregate",
}  # fmt: skip
spark.sql(f"DROP TABLE IF EXISTS {plain}")
spark.sql(f"DROP TABLE IF EXISTS {clustered}")

# COMMAND ----------
# 2) API ingestion benchmark -------------------------------------------------------------------------------------------
from agripulse.common.config import get_settings
from agripulse.ingest.nasa_power import NasaPowerIngestor
from agripulse.ingest.openmeteo import OpenMeteoArchiveIngestor

s = get_settings()
dist = [r.asDict() for r in spark.table(f"{catalog}.bronze.geo_districts").select("district_id", "lat", "lon").orderBy("district_id").limit(60).collect()][20:]
d0, d1 = date(2016, 1, 1), date(2016, 12, 31)

nasa = NasaPowerIngestor(spark, dist[:24], settings=s)
t = time.time()
rows_seq = [r for d in dist[:24] for r in nasa._one(d, d0, d1)]
nasa_seq = time.time() - t
t = time.time()
with ThreadPoolExecutor(max_workers=4) as ex:
    rows_par = [r for chunk in ex.map(lambda d: nasa._one(d, d0, d1), dist[:24]) for r in chunk]
nasa_par = time.time() - t

# different districts for each approach and batched FIRST, so server-side response caching cannot flatter either side
set_a, set_b = dist[:10], dist[10:20]
om = OpenMeteoArchiveIngestor(spark, set_a + set_b, settings=s)
t = time.time()
om_batch = om._one(set_a, d0, d1)
om_batched = time.time() - t
t = time.time()
om_single = [r for d in set_b for r in om._one([d], d0, d1)]
om_seq = time.time() - t
out["api_ingestion"] = {
    "nasa_24_districts_1yr": {"sequential_s": round(nasa_seq, 1), "threadpool4_s": round(nasa_par, 1),
                              "speedup_x": round(nasa_seq / nasa_par, 2), "rows": len(rows_par)},
    "open_meteo_10_districts_1yr": {"single_location_sequential_s": round(om_seq, 1), "one_batched_request_s": round(om_batched, 1), "note": "different districts per approach, batched run first (no cache advantage)",
                                    "speedup_x": round(om_seq / om_batched, 2), "requests_single": 10, "requests_batched": 1,
                                    "rows_single": len(om_single), "rows_batched": len(om_batch)},
}  # fmt: skip
print(json.dumps(out, indent=2))
dbutils.notebook.exit(json.dumps(out))
