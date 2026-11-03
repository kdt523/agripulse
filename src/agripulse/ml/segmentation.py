"""Agro-climatic district segmentation: standardise -> KMeans (k=4..10, silhouette) -> named, profiled segments."""

from __future__ import annotations

import numpy as np
import pandas as pd

CLIMATE = ["annual_precip_mm", "precip_cv", "heat_days_per_year", "tmax_mean_c", "soil_wetness_root_mean"]
MARKET = ["price_volatility_cv"]
SHARES = ["share_maize", "share_moong", "share_rice", "share_soybean", "share_tur", "share_wheat"]
FEATURES = CLIMATE + MARKET + SHARES
CROP_LABEL = {"share_maize": "maize", "share_moong": "green gram", "share_rice": "rice", "share_soybean": "soybean",
              "share_tur": "tur (pigeon pea)", "share_wheat": "wheat"}  # fmt: skip


def prepare(profile: pd.DataFrame) -> pd.DataFrame:
    """Impute: districts without mandi data get zero crop shares and median price volatility; climate NaNs -> median."""
    x = profile[FEATURES].copy()
    for c in SHARES:
        x[c] = x[c].fillna(0.0)
    return x.fillna(x.median(numeric_only=True))


def choose_k(x_std: np.ndarray, ks=range(4, 11), seed: int = 42) -> tuple[int, dict[int, float]]:
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score

    scores = {}
    for k in ks:
        labels = KMeans(n_clusters=k, n_init=10, random_state=seed).fit_predict(x_std)
        scores[k] = float(silhouette_score(x_std, labels))
    return max(scores, key=scores.get), scores


def fit(profile: pd.DataFrame, k: int | None = None, seed: int = 42) -> dict:
    from sklearn.cluster import KMeans
    from sklearn.decomposition import PCA
    from sklearn.preprocessing import StandardScaler

    x = prepare(profile)
    xs = StandardScaler().fit_transform(x)
    best_k, scores = choose_k(xs, seed=seed)
    k = k or best_k
    km = KMeans(n_clusters=k, n_init=10, random_state=seed).fit(xs)
    pca = PCA(n_components=2, random_state=seed).fit(xs)
    return {"k": k, "best_k": best_k, "silhouette": scores, "labels": km.labels_, "X": x, "Xs": xs,
            "pca": pca.transform(xs), "pca_var": pca.explained_variance_ratio_}  # fmt: skip


def name_segments(x: pd.DataFrame, labels: np.ndarray) -> pd.DataFrame:
    """Rule-based names from segment means relative to the national distribution (terciles)."""
    d = x.assign(segment=labels)
    prof = d.groupby("segment").mean()
    q = {c: (x[c].quantile(1 / 3), x[c].quantile(2 / 3)) for c in CLIMATE + MARKET}
    names, uses = {}, {}
    for s, r in prof.iterrows():
        rain = "arid" if r.annual_precip_mm <= q["annual_precip_mm"][0] else ("high-rainfall" if r.annual_precip_mm >= q["annual_precip_mm"][1] else "semi-arid")
        heat = "hot" if r.heat_days_per_year >= q["heat_days_per_year"][1] else ("cool" if r.heat_days_per_year <= q["heat_days_per_year"][0] else "")
        risk = "high rainfall variability" if r.precip_cv >= q["precip_cv"][1] else ("stable rainfall" if r.precip_cv <= q["precip_cv"][0] else "")
        shares = r[SHARES].sort_values(ascending=False)
        crop = f"{CROP_LABEL[shares.index[0]]} belt" if shares.iloc[0] >= 0.30 else ("no mandi data" if shares.sum() == 0 else "mixed-crop")
        names[s] = ", ".join(p for p in (f"{heat} {rain}".strip(), crop, risk) if p)
        uses[s] = _marketing(names[s], r, shares)
    return pd.DataFrame({"segment": list(names), "segment_name": list(names.values()), "marketing_use": list(uses.values())}).set_index("segment")


def _marketing(name: str, r: pd.Series, shares: pd.Series) -> str:
    crop = CROP_LABEL[shares.index[0]] if shares.iloc[0] >= 0.30 else "the local crop mix"
    if r.precip_cv >= 0 and "variability" in name:
        return f"Lead with resilience and timing: rainfall here swings year to year, so promote drought-tolerant options and flexible, staged input offers for {crop}, and time campaigns to the first reliable rains."
    if "hot" in name:
        return f"Lead with heat and water-stress protection for {crop}; schedule demand-generation before the pre-monsoon sowing window and watch soil-wetness signals for early orders."
    if "high-rainfall" in name:
        return f"Lead with disease and pest pressure in humid conditions for {crop}; plan crop-protection stock ahead of the main rainy-season peak."
    return f"Use steady-demand planning for {crop}: stable weather means forecasts can lean on market lags, so focus on distributor coverage and price-based promotions."
