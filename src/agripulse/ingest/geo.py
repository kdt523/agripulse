"""District geo (DataMeet Census-2011 GeoJSON) -> bronze.geo_districts, plus crop-area CSV loader."""

from __future__ import annotations

import math
import re
from typing import Any

from agripulse.common.config import Settings, get_settings
from agripulse.common.http import HttpClient

DISTRICTS_URL = (
    "https://raw.githubusercontent.com/datameet/maps/master/website/docs/data/geojson/dists11.geojson"
)


def district_id_from_census(censuscode: float | int) -> str:
    return f"D{int(censuscode):04d}"


def _area_km2(geom) -> float:
    """Approx. area in km2: degrees^2 scaled by (111.32 km)^2 * cos(lat). Good to ~1% at district scale."""
    c = geom.centroid
    return float(geom.area * (111.32**2) * math.cos(math.radians(c.y)))


def parse_districts(geojson: dict[str, Any]) -> list[dict]:
    """GeoJSON FeatureCollection -> one row per district with centroid lat/lon, area and WKT."""
    from shapely.geometry import shape

    rows = []
    for feat in geojson["features"]:
        p = feat["properties"]
        geom = shape(feat["geometry"])
        if not geom.is_valid:
            geom = geom.buffer(0)
        # representative_point lies inside the polygon (centroid can fall outside for odd shapes)
        c = geom.centroid if geom.contains(geom.centroid) else geom.representative_point()
        code = p.get("censuscode")
        if code is None:
            continue
        rows.append(
            {
                "district_id": district_id_from_census(code),
                "district_name": (p.get("DISTRICT") or "").strip(),
                "state_name": (p.get("ST_NM") or "").strip(),
                "census_code": int(code),
                "lat": round(c.y, 4),
                "lon": round(c.x, 4),
                "area_km2": round(_area_km2(geom), 2),
                "geometry_wkt": geom.simplify(0.01).wkt,
            }
        )
    return rows


def load_geo_districts(spark, settings: Settings | None = None, geojson: dict | None = None) -> dict:
    """Fetch + parse + MERGE into bronze.geo_districts."""
    from datetime import datetime, timezone

    from pyspark.sql import functions as F

    from agripulse.common.delta_io import merge_into

    s = settings or get_settings()
    if geojson is None:
        geojson = HttpClient(s).get_json(s.districts_geojson_url or DISTRICTS_URL)
    rows = parse_districts(geojson)
    df = spark.createDataFrame(rows).withColumns(
        {
            "_ingested_at": F.lit(datetime.now(timezone.utc)).cast("timestamp"),
            "_source": F.lit("datameet_districts_2011"),
        }
    )
    return merge_into(spark, df, s.table("bronze", "geo_districts"), ["district_id"])


# ---- ICRISAT crop area/production (CSV supplied by user, landed in the Volume) -----------------

def _norm(col: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", col.strip().lower()).strip("_")


def normalise_columns(columns: list[str]) -> dict[str, str]:
    """Map raw CSV headers to snake_case, preserving uniqueness."""
    out, seen = {}, set()
    for c in columns:
        n = _norm(c)
        while n in seen:
            n += "_x"
        seen.add(n)
        out[c] = n
    return out


def load_crop_area_csv(spark, path: str, settings: Settings | None = None) -> dict:
    """Load an ICRISAT district-level CSV as-is (snake_case headers) into bronze.crop_area_raw."""
    from datetime import datetime, timezone

    from pyspark.sql import functions as F

    s = settings or get_settings()
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    for old, new in normalise_columns(df.columns).items():
        df = df.withColumnRenamed(old, new)
    df = df.withColumn("_ingested_at", F.lit(datetime.now(timezone.utc)).cast("timestamp")).withColumn(
        "_source", F.lit("icrisat_dld")
    )
    n = df.count()
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        s.table("bronze", "crop_area_raw")
    )
    return {"rows_in": n, "inserted": n}
