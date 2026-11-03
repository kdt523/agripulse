from datetime import date, timedelta

from agripulse.transform import gold_district_week as g


def _mandi(spark, rows):
    return spark.createDataFrame(rows, "district_id string, commodity_std string, market_id string, date date, arrivals_t double, modal_price_inr_qtl double")


def test_weekly_mandi_and_calendar_lags(spark):
    mon = date(2016, 1, 4)  # Monday
    rows = []
    for w in range(60):
        d = mon + timedelta(weeks=w)
        rows.append(("D1", "WHEAT", "m1", d, 10.0 + w, 1000.0 + w))
        rows.append(("D1", "WHEAT", "m2", d + timedelta(days=2), 5.0, 1100.0 + w))
    wk = g.weekly_mandi(_mandi(spark, rows))
    r = {x["week_start"]: x for x in wk.collect()}
    assert r[mon]["arrivals_t_sum"] == 15.0 and r[mon]["n_markets"] == 2
    out = {x["week_start"]: x for x in g.add_market_features(wk).collect()}
    w10 = mon + timedelta(weeks=10)
    assert out[w10]["arrivals_lag_1w"] == 10.0 + 9 + 5  # week 9 total
    assert out[w10]["arrivals_lag_4w"] == 10.0 + 6 + 5
    assert out[mon]["arrivals_lag_1w"] is None  # nothing before the first week: no leakage / no invention
    w55 = mon + timedelta(weeks=55)
    assert out[w55]["arrivals_lag_52w"] == 10.0 + 3 + 5
    # share excludes the current week and has no history in week 0
    assert out[mon]["commodity_share_52w"] is None
    assert abs(out[w10]["commodity_share_52w"] - 1.0) < 1e-9  # only commodity in the district


def test_weather_weekly_features(spark):
    mon = date(2016, 6, 6)
    precip = [0.0, 0.0, 5.0, 0.0, 0.0, 0.0, 0.0]  # dry spell after the rain day: 4
    rows = [("D1", mon + timedelta(days=i), 41.0 if i < 2 else 30.0, 25.0, precip[i], 4.0, False) for i in range(7)]
    df = spark.createDataFrame(rows, "district_id string, date date, tmax_c double, tmin_c double, precip_mm double, et0_mm double, is_forecast boolean")
    r = g.weekly_weather(df).collect()[0]
    assert r["precip_sum_7d"] == 5.0 and r["heat_days"] == 2 and r["dry_spell_len"] == 4 and r["et0_sum"] == 28.0
    assert abs(r["gdd"] - (2 * (33.0 - 10) + 5 * (27.5 - 10))) < 1e-9
