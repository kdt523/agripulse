import numpy as np
import pandas as pd

from agripulse.ml import segmentation as seg


def _profile(n=60, seed=0):
    rng = np.random.default_rng(seed)
    rows = []
    for i in range(n):
        wet = i % 2 == 0
        rows.append({"district_id": f"D{i}", "annual_precip_mm": rng.normal(1500 if wet else 400, 40), "precip_cv": rng.normal(0.15 if wet else 0.4, 0.02),
                     "heat_days_per_year": rng.normal(5 if wet else 60, 2), "tmax_mean_c": rng.normal(30 if wet else 36, 0.5),
                     "soil_wetness_root_mean": rng.normal(0.7 if wet else 0.3, 0.02), "price_volatility_cv": rng.normal(0.2, 0.02),
                     "share_rice": 0.8 if wet else 0.0, "share_wheat": 0.0 if wet else 0.8, "share_maize": 0.1, "share_moong": 0.0, "share_soybean": 0.0, "share_tur": 0.0})
    return pd.DataFrame(rows)


def test_two_obvious_groups_are_found_and_named():
    p = _profile()
    res = seg.fit(p, k=2)
    labels = res["labels"]
    assert len(set(labels[::2])) == 1 and len(set(labels[1::2])) == 1 and labels[0] != labels[1]
    names = seg.name_segments(res["X"], labels)
    joined = " | ".join(names.segment_name)
    assert "rice belt" in joined and "wheat belt" in joined
    assert res["silhouette"][max(res["silhouette"], key=res["silhouette"].get)] > 0.4


def test_districts_without_mandi_data_are_imputed():
    p = _profile(10)
    p.loc[0, ["share_rice", "share_wheat", "price_volatility_cv"]] = np.nan
    x = seg.prepare(p)
    assert x.isna().sum().sum() == 0 and x.loc[0, "share_rice"] == 0.0
