import numpy as np
import pandas as pd

from agripulse.ml import forecast_lift as fl


def test_metrics():
    y = np.array([10.0, 0.0, 30.0])
    p = np.array([12.0, 1.0, 24.0])
    assert abs(fl.wape(y, p) - (2 + 1 + 6) / 40) < 1e-12
    assert abs(fl.mape(y, p) - np.mean([0.2, 0.2])) < 1e-12  # zero target excluded from MAPE
    assert fl.wape(np.zeros(3), p) != fl.wape(np.zeros(3), p)  # NaN when denominator is zero


def test_folds_are_time_ordered_and_non_overlapping():
    weeks = pd.Series(pd.date_range("2013-01-07", periods=120, freq="W-MON"))
    folds = fl.make_folds(weeks, n_folds=4)
    assert len(folds) == 4
    for f in folds:
        assert f["train_end"] < f["val_end"] < f["test_end"]
    for a, b in zip(folds, folds[1:], strict=False):
        assert a["test_end"] < b["val_end"] or a["test_end"] <= b["val_end"]  # test windows advance in time
    assert folds[-1]["test_end"] == weeks.max()


def test_backtest_prefers_informative_feature():
    rng = np.random.default_rng(0)
    weeks = pd.date_range("2013-01-07", periods=150, freq="W-MON")
    rows = []
    for d in range(30):
        for w in weeks:
            rain = rng.normal(0, 1)
            base = 100 + 20 * d
            y = base * np.exp(0.5 * rain)  # target driven by an external signal
            rows.append(dict(district_id=f"D{d}", commodity_std="WHEAT", state="S", week_start=w,
                             target_arrivals_next_week=y, arrivals_t_sum=base, arrivals_lag_1w=base, arrivals_lag_2w=base,
                             arrivals_lag_4w=base, arrivals_lag_52w=base, modal_price_avg=1000.0, price_lag_1w=1000.0,
                             price_lag_2w=1000.0, price_lag_4w=1000.0, price_lag_52w=1000.0, price_volatility_4w=5.0,
                             n_markets=2, commodity_share_52w=0.5, next_week_arrivals_lag_52w=base, week_of_year=int(w.isocalendar().week),
                             precip_sum_7d=rain, precip_sum_28d=rain, precip_anomaly=rain, tmax_avg=30.0, heat_days=0,
                             dry_spell_len=1, gdd=100.0, soil_wetness_root_avg=0.4, soil_moisture_anomaly=0.0,
                             solar_sum=100.0, area_km2=1000.0))  # fmt: skip
    df = pd.DataFrame(rows)
    # the signal is the *next* week's rain proxy - shift so features at t carry it
    df["precip_sum_7d"] = df.groupby("district_id")["target_arrivals_next_week"].transform(lambda s: np.log(s / s.mean()) / 0.5)
    res = fl.run_backtest(df, n_folds=4)
    table = fl.results_table(res["preds"])
    w = table[(table.fold == "all") & (table.slice == "overall")].set_index("model")["wape"]
    assert w["lgbm_external"] < w["lgbm_market"]
    point, lo, hi = fl.bootstrap_lift_ci(res["preds"], n=50)
    assert point > 0 and lo <= point <= hi
    imp = fl.shap_importance(res["model_C"], res["X_test_C"])
    assert {"precip_sum_7d", "precip_sum_28d", "precip_anomaly"} & set(imp.index[:3])  # the true driver ranks in the top 3
