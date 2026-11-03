"""Forecast-lift ablation: does adding external features cut next-week arrivals error?

Models: A seasonal-naive | B LightGBM (market lags + calendar) | C LightGBM (B + weather + agro + geo).
Split: strictly time-based. Rolling-origin backtest with expanding train window; each fold = 13 weeks of validation
(early stopping) followed by 13 weeks of test. The last two folds together are the final 26-week holdout.
WAPE is the primary metric (robust to zeros).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from agripulse.ml.features import CATEGORICAL, FEATURES_B, FEATURES_C, TARGET

PARAMS = {
    "objective": "regression", "learning_rate": 0.05, "num_leaves": 63, "min_data_in_leaf": 40,
    "feature_fraction": 0.8, "bagging_fraction": 0.8, "bagging_freq": 1, "lambda_l2": 1.0, "verbose": -1, "seed": 42,
}  # fmt: skip


def wape(y: np.ndarray, p: np.ndarray) -> float:
    d = float(np.abs(y).sum())
    return float("nan") if d == 0 else float(np.abs(y - p).sum() / d)


def mape(y: np.ndarray, p: np.ndarray) -> float:
    m = y > 0
    return float("nan") if not m.any() else float(np.mean(np.abs(y[m] - p[m]) / y[m]))


def rmse(y: np.ndarray, p: np.ndarray) -> float:
    return float(np.sqrt(np.mean((y - p) ** 2)))


def metrics(y, p) -> dict:
    y, p = np.asarray(y, dtype=float), np.asarray(p, dtype=float)
    return {"wape": wape(y, p), "mape": mape(y, p), "rmse": rmse(y, p), "n_rows": int(len(y))}


def make_folds(weeks: pd.Series, n_folds: int = 4, val_weeks: int = 13, test_weeks: int = 13) -> list[dict]:
    """Rolling-origin folds over sorted unique week_start values. Returns boundaries (inclusive/exclusive)."""
    w = sorted(pd.to_datetime(weeks).unique())
    folds = []
    for k in range(n_folds, 0, -1):  # oldest fold first
        test_end = len(w) - (k - 1) * test_weeks  # exclusive index
        test_start = test_end - test_weeks
        val_start = test_start - val_weeks
        if val_start <= 0:
            raise ValueError("not enough weeks for the requested folds")
        folds.append({
            "fold": n_folds - k + 1,
            "train_end": w[val_start], "val_end": w[test_start], "test_end": w[test_end - 1],
        })  # fmt: skip
    return folds


def split_fold(df: pd.DataFrame, f: dict):
    ws = pd.to_datetime(df["week_start"])
    train = df[ws < f["train_end"]]
    val = df[(ws >= f["train_end"]) & (ws < f["val_end"])]
    test = df[(ws >= f["val_end"]) & (ws <= f["test_end"])]
    return train, val, test


def _prep(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    x = df[cols].copy()
    for c in CATEGORICAL:
        if c in x:
            x[c] = x[c].astype("category")
    return x


def fit_predict(train, val, test, cols):
    import lightgbm as lgb

    cats = sorted(set(train["commodity_std"]) | set(val["commodity_std"]) | set(test["commodity_std"]))
    states = sorted(set(train["state"].dropna()) | set(val["state"].dropna()) | set(test["state"].dropna()))

    def prep(d):
        x = _prep(d, cols)
        if "commodity_std" in x:
            x["commodity_std"] = pd.Categorical(d["commodity_std"], categories=cats)
        if "state" in x:
            x["state"] = pd.Categorical(d["state"], categories=states)
        return x

    dtr = lgb.Dataset(prep(train), np.log1p(train[TARGET].clip(lower=0)))
    dva = lgb.Dataset(prep(val), np.log1p(val[TARGET].clip(lower=0)), reference=dtr)
    model = lgb.train(PARAMS, dtr, num_boost_round=1500, valid_sets=[dva],
                      callbacks=[lgb.early_stopping(50, verbose=False)])  # fmt: skip
    pred = np.expm1(model.predict(prep(test), num_iteration=model.best_iteration)).clip(min=0)
    return model, pred, prep(test)


def run_backtest(df: pd.DataFrame, n_folds: int = 4) -> dict:
    """Returns predictions for A/B/C on every test row (all folds) + models of the last fold."""
    df = df.dropna(subset=[TARGET]).copy()
    folds = make_folds(df["week_start"], n_folds)
    out, last = [], {}
    for f in folds:
        train, val, test = split_fold(df, f)
        t = test[["district_id", "commodity_std", "state", "week_start", TARGET]].copy()
        t["fold"] = f["fold"]
        t["pred_A"] = test["next_week_arrivals_lag_52w"].fillna(0.0).to_numpy()  # seasonal naive; no history -> 0
        mB, t["pred_B"], _ = fit_predict(train, val, test, FEATURES_B)
        mC, t["pred_C"], xC = fit_predict(train, val, test, FEATURES_C)
        out.append(t)
        last = {"model_B": mB, "model_C": mC, "X_test_C": xC, "fold": f, "n_train": len(train)}
    return {"preds": pd.concat(out, ignore_index=True), "folds": folds, **last}


def results_table(preds: pd.DataFrame, extra_keys: tuple[str, ...] = ()) -> pd.DataFrame:
    rows = []
    y_col = TARGET
    for model, col in (("seasonal_naive", "pred_A"), ("lgbm_market", "pred_B"), ("lgbm_external", "pred_C")):
        for fold, g in [("all", preds)] + [(str(k), g) for k, g in preds.groupby("fold")]:
            rows.append({"model": model, "fold": fold, "slice": "overall", **metrics(g[y_col], g[col])})
        for key in ("commodity_std", "state", *extra_keys):
            for val, g in preds.groupby(key):
                rows.append({"model": model, "fold": "all", "slice": f"{key.split('_')[0]}={val}", **metrics(g[y_col], g[col])})
    return pd.DataFrame(rows)


def lift(res: pd.DataFrame, base: str = "lgbm_market", new: str = "lgbm_external") -> pd.DataFrame:
    """Relative WAPE reduction base -> new per (fold, slice). Positive = external features help."""
    a = res[res.model == base].set_index(["fold", "slice"])["wape"]
    b = res[res.model == new].set_index(["fold", "slice"])["wape"]
    out = ((a - b) / a * 100).rename("lift_pct").reset_index()
    return out


def bootstrap_lift_ci(preds: pd.DataFrame, n: int = 300, seed: int = 7) -> tuple[float, float, float]:
    """Bootstrap over district x commodity series: (point, lo95, hi95) of WAPE lift B -> C in percent."""
    rng = np.random.default_rng(seed)
    p = preds.copy()
    p["series"] = p["district_id"] + "|" + p["commodity_std"]
    p["ae_B"] = (p[TARGET] - p["pred_B"]).abs()
    p["ae_C"] = (p[TARGET] - p["pred_C"]).abs()
    g = p.groupby("series").agg(y=(TARGET, "sum"), eB=("ae_B", "sum"), eC=("ae_C", "sum"))
    y, eb, ec = g["y"].to_numpy(), g["eB"].to_numpy(), g["eC"].to_numpy()

    def lift_of(ix):
        wb, wc = eb[ix].sum() / y[ix].sum(), ec[ix].sum() / y[ix].sum()
        return (wb - wc) / wb * 100

    point = lift_of(np.arange(len(g)))
    draws = [lift_of(rng.integers(0, len(g), len(g))) for _ in range(n)]
    return float(point), float(np.percentile(draws, 2.5)), float(np.percentile(draws, 97.5))


def shap_importance(model, X) -> pd.Series:
    """Mean |SHAP| per feature from LightGBM's native TreeSHAP (pred_contrib), excluding the bias column."""
    contrib = model.predict(X, pred_contrib=True)
    imp = np.abs(contrib[:, :-1]).mean(axis=0)
    return pd.Series(imp, index=X.columns).sort_values(ascending=False)
