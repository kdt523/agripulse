from datetime import date

from agripulse.common import delta_io
from agripulse.common.config import get_settings


def _df(spark, rows):
    return spark.createDataFrame(rows, "district_id string, date date, v double, _ingested_at timestamp")


def test_merge_is_idempotent_and_updates(spark_delta):
    from datetime import datetime

    t = "bronze.t_merge"
    spark_delta.sql(f"DROP TABLE IF EXISTS {t}")
    d = date(2024, 1, 1)
    rows = [("D1", d, 1.0, datetime(2024, 1, 2)), ("D2", d, 2.0, datetime(2024, 1, 2))]
    r1 = delta_io.merge_into(spark_delta, _df(spark_delta, rows), t, ["district_id", "date"])
    assert r1["inserted"] == 2 and r1["total"] == 2
    r2 = delta_io.merge_into(spark_delta, _df(spark_delta, rows), t, ["district_id", "date"])
    assert r2["inserted"] == 0 and r2["total"] == 2  # re-running the same window adds nothing
    upd = [("D1", d, 9.0, datetime(2024, 1, 3))]
    r3 = delta_io.merge_into(spark_delta, _df(spark_delta, upd), t, ["district_id", "date"])
    assert r3["inserted"] == 0 and spark_delta.sql(f"SELECT v FROM {t} WHERE district_id='D1'").collect()[0][0] == 9.0


def test_source_duplicates_collapse_to_latest(spark_delta):
    from datetime import datetime

    t = "bronze.t_dups"
    spark_delta.sql(f"DROP TABLE IF EXISTS {t}")
    d = date(2024, 1, 1)
    rows = [("D1", d, 1.0, datetime(2024, 1, 1)), ("D1", d, 5.0, datetime(2024, 1, 2))]
    res = delta_io.merge_into(spark_delta, _df(spark_delta, rows), t, ["district_id", "date"])
    assert res["total"] == 1 and spark_delta.sql(f"SELECT v FROM {t}").collect()[0][0] == 5.0


def test_watermarks_advance_only(spark_delta):
    s = get_settings(catalog="spark_catalog")  # tables addressed as spark_catalog.ops.*
    spark_delta.sql("DROP TABLE IF EXISTS ops.ingest_watermarks")
    spark_delta.sql("CREATE TABLE ops.ingest_watermarks (source STRING, key STRING, last_date DATE, updated_at TIMESTAMP) USING delta")
    assert delta_io.get_watermark(spark_delta, "weather", settings=s) is None
    delta_io.set_watermark(spark_delta, "weather", date(2024, 1, 5), settings=s)
    assert delta_io.get_watermark(spark_delta, "weather", settings=s) == date(2024, 1, 5)
    delta_io.set_district_watermarks(spark_delta, "weather", ["D1", "D2"], date(2024, 2, 1), s)
    delta_io.set_district_watermarks(spark_delta, "weather", ["D1"], date(2023, 1, 1), s)  # older: must not regress
    wm = delta_io.district_watermarks(spark_delta, "weather", s)
    assert wm == {"D1": date(2024, 2, 1), "D2": date(2024, 2, 1)}
