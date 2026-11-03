from agripulse.quality.expectations import (
    MANDI_RULES,
    WEATHER_RULES,
    failed_rules_sql,
    missing_columns,
)


def test_missing_columns_detects_drift():
    assert missing_columns("geo_districts", ["district_id", "district_name"]) == ["state_name", "lat", "lon", "area_km2"]
    assert missing_columns("nasa_power_raw", ["district_id", "date", "allsky_sfc_sw_dwn", "gwetroot", "t2m", "prectotcorr"]) == []


def test_failed_rules_sql_names_every_rule():
    sql = failed_rules_sql(MANDI_RULES)
    for name in MANDI_RULES:
        assert f"'{name}'" in sql
    assert sql.startswith("filter(array(")


def test_himalayan_winter_is_valid():
    # regression: a -45 C night / -36 C day at a high-altitude centroid must pass temp_range
    assert "-60" in WEATHER_RULES["temp_range"] and "-50" in WEATHER_RULES["temp_range"]
