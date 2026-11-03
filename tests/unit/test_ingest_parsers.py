import json
from datetime import date
from pathlib import Path

import pytest

from agripulse.ingest import agmarknet, geo, nasa_power, openmeteo
from agripulse.ingest.windows import chunk_dates, plan_catchup, resolve_window

FX = Path(__file__).parent.parent / "fixtures"


def load(name):
    return json.loads((FX / name).read_text())


def test_openmeteo_parse_multi_location():
    rows = openmeteo.parse_response(load("openmeteo_multi.json"), ["D1", "D2"])
    assert len(rows) == 4
    assert {r["district_id"] for r in rows} == {"D1", "D2"}
    assert rows[0]["temperature_2m_max"] == 30.1
    assert rows[3]["temperature_2m_max"] is None  # nulls preserved for silver to judge


def test_openmeteo_single_location_dict_and_mismatch():
    single = load("openmeteo_multi.json")[0]
    assert len(openmeteo.parse_response(single, ["D1"])) == 2
    with pytest.raises(ValueError):
        openmeteo.parse_response(load("openmeteo_multi.json"), ["D1"])


def test_nasa_parse_keeps_fill_value():
    rows = nasa_power.parse_response(load("nasa_power.json"), "D1")
    assert [r["date"] for r in rows] == ["2024-01-01", "2024-01-02"]
    assert rows[1]["allsky_sfc_sw_dwn"] == -999.0  # raw fill value kept for silver quarantine
    assert set(rows[0]) >= {"gwetroot", "t2m", "prectotcorr", "rh2m"}


def test_agmarknet_canonicalise():
    rows = agmarknet.parse_api_page(load("agmarknet_page.json"))
    assert rows[0]["arrival_date"] == "2024-01-02" and rows[0]["modal_price"] == 1500.0
    assert rows[1]["modal_price"] is None  # "NA" -> null
    assert rows[1]["min_price"] > rows[1]["max_price"]  # bad row kept raw; silver quarantines it


def test_agmarknet_header_aliases():
    out = agmarknet.canonicalise({"State": "X", "Min x0020 Price": "5", "Arrival_Date": "31/12/2023"})
    assert out["state"] == "X" and out["min_price"] == 5.0 and out["arrival_date"] == "2023-12-31"


def test_geo_parse_centroid_and_id():
    rows = geo.parse_districts(load("districts_mini.geojson"))
    r = rows[0]
    assert r["district_id"] == "D0532"
    assert (r["lat"], r["lon"]) == (18.5, 73.5)
    assert 11000 < r["area_km2"] < 12000  # ~1 deg x 1 deg at 18.5N is ~11,700 km2


def test_geo_normalise_columns_unique():
    m = geo.normalise_columns(["Dist Name", "dist_name", "Rice Area (1000 ha)"])
    assert len(set(m.values())) == 3


def test_windows():
    today = date(2024, 6, 10)
    kw = dict(today=today, lag_days=2, default_start=date(2020, 1, 1))
    assert resolve_window(mode="incremental", watermark=None, **kw) == (date(2020, 1, 1), date(2024, 6, 8))
    assert resolve_window(mode="incremental", watermark=date(2024, 6, 5), **kw) == (date(2024, 6, 6), date(2024, 6, 8))
    assert resolve_window(mode="incremental", watermark=date(2024, 6, 8), **kw) is None
    assert resolve_window(mode="backfill", watermark=None, start=date(2023, 1, 1), end=date(2023, 1, 31), **kw) == (
        date(2023, 1, 1), date(2023, 1, 31)
    )  # fmt: skip
    with pytest.raises(ValueError):
        resolve_window(mode="backfill", watermark=None, **kw)


def test_chunk_dates_covers_range_without_overlap():
    ch = chunk_dates(date(2020, 1, 1), date(2022, 12, 31), 366)
    assert ch[0][0] == date(2020, 1, 1) and ch[-1][1] == date(2022, 12, 31)
    for (_, e), (s, _) in zip(ch, ch[1:], strict=False):
        assert (s - e).days == 1


def test_plan_catchup_resumes_oldest_first_and_batches():
    ids = [f"D{i:04d}" for i in range(1, 6)]
    wms = {"D0001": date(2022, 12, 31), "D0002": date(2022, 12, 31)}
    plan = plan_catchup(ids, wms, date(2024, 6, 30), date(2020, 1, 1), batch_size=2, chunk_days=366)
    # untouched districts (start 2020-01-01) come first, in batches of 2; resumed ones start 2023-01-01
    assert plan[0] == (["D0003", "D0004"], date(2020, 1, 1), date(2020, 12, 31))
    assert plan[1] == (["D0005"], date(2020, 1, 1), date(2020, 12, 31))
    assert plan[2][0] == ["D0001", "D0002"] and plan[2][1] == date(2023, 1, 1)


def test_plan_catchup_empty_when_up_to_date():
    assert plan_catchup(["D0001"], {"D0001": date(2024, 6, 30)}, date(2024, 6, 30), date(2020, 1, 1), 10, 366) == []


def test_archive_date_format_and_columns():
    assert agmarknet.parse_date("01 Jan 2016") == "2016-01-01"
    assert agmarknet.ARCHIVE_COLUMNS[-1] == "arrival_date" and len(agmarknet.ARCHIVE_COLUMNS) == 10
