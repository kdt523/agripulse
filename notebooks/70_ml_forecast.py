# Databricks notebook source
# MAGIC %md
# MAGIC # 70 — Forecast lift: do external features reduce next-week arrivals error?
# MAGIC A = seasonal naive, B = LightGBM market+calendar, C = B + weather + agro + geo. 4-fold rolling-origin backtest.

# COMMAND ----------
# MAGIC %pip install -q lightgbm matplotlib pydantic-settings tenacity
# MAGIC %restart_python

# COMMAND ----------
import json
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mlflow
import pandas as pd

sys.path.insert(0, os.path.abspath("../src"))
dbutils.widgets.text("catalog", "agripulse")
catalog = dbutils.widgets.get("catalog")
os.environ["AGRIPULSE_CATALOG"] = catalog

from agripulse.ml import forecast_lift as fl
from agripulse.ml.features import FEATURES_B, FEATURES_C

EXPORT = f"/Volumes/{catalog}/bronze/landing/exports"
os.makedirs(EXPORT, exist_ok=True)

# COMMAND ----------
df = spark.table(f"{catalog}.gold.v_ds_training").toPandas()
df["week_start"] = pd.to_datetime(df["week_start"])
print(df.shape, df["week_start"].min(), df["week_start"].max())

user = spark.sql("select current_user()").collect()[0][0]
mlflow.set_experiment(f"/Users/{user}/agripulse_forecast_lift")

# COMMAND ----------
res = fl.run_backtest(df, n_folds=4)
# pre-specified slices from the plan: season of the week, and rain-fed vs irrigated-ish by long-run rainfall tercile
prof = spark.table(f"{catalog}.gold.district_profile").select("district_id", "annual_precip_mm").toPandas()
prof["rain_regime"] = pd.qcut(prof["annual_precip_mm"], 3, labels=["low-rain", "mid-rain", "high-rain"]).astype(str)
meta = df[["district_id", "commodity_std", "week_start", "season"]]
res["preds"] = res["preds"].merge(meta, on=["district_id", "commodity_std", "week_start"], how="left").merge(
    prof[["district_id", "rain_regime"]], on="district_id", how="left")
table = fl.results_table(res["preds"], extra_keys=("season", "rain_regime"))
lift = fl.lift(table)
point, lo, hi = fl.bootstrap_lift_ci(res["preds"])
imp = fl.shap_importance(res["model_C"], res["X_test_C"])

overall = table[(table.fold == "all") & (table.slice == "overall")].set_index("model")
by_fold = lift[lift.slice == "overall"].set_index("fold")["lift_pct"]
print(overall[["wape", "mape", "rmse", "n_rows"]])
print("lift per fold:", by_fold.to_dict(), "pooled:", point, (lo, hi))

# COMMAND ----------
# charts
fig, ax = plt.subplots(figsize=(6, 4))
vals = overall.loc[["seasonal_naive", "lgbm_market", "lgbm_external"], "wape"] * 100
ax.bar(["A seasonal\nnaive", "B market +\ncalendar", "C + external\nfeatures"], vals, color=["#9aa5b1", "#4f7cac", "#2a9d8f"])
for i, v in enumerate(vals):
    ax.text(i, v + 0.5, f"{v:.1f}%", ha="center")
ax.set_ylabel("WAPE % (lower is better)")
ax.set_title(f"Next-week arrivals error, 4-fold backtest\nB to C lift {point:.1f}% (95% CI {lo:.1f} to {hi:.1f})")
fig.tight_layout()
fig.savefig(f"{EXPORT}/forecast_lift.png", dpi=150)

top = imp.head(15)[::-1]
fig2, ax2 = plt.subplots(figsize=(6, 5))
ext = set(FEATURES_C) - set(FEATURES_B)
ax2.barh(top.index, top.values, color=["#2a9d8f" if f in ext else "#4f7cac" for f in top.index])
ax2.set_xlabel("mean |SHAP| (log1p arrivals)")
ax2.set_title("Top features, model C (teal = external)")
fig2.tight_layout()
fig2.savefig(f"{EXPORT}/shap_top_features.png", dpi=150)

# COMMAND ----------
# MLflow: one run per model with params, metrics and artifacts
for model, col in (("seasonal_naive", "A"), ("lgbm_market", "B"), ("lgbm_external", "C")):
    with mlflow.start_run(run_name=model):
        mlflow.log_params({"model": model, "folds": 4, "features": len({"A": [], "B": FEATURES_B, "C": FEATURES_C}[col])})
        row = overall.loc[model]
        mlflow.log_metrics({"wape": float(row.wape), "mape": float(row.mape), "rmse": float(row.rmse)})
        if col == "C":
            mlflow.log_metrics({"lift_pct_vs_B": point, "lift_ci_lo": lo, "lift_ci_hi": hi})
            mlflow.log_artifact(f"{EXPORT}/forecast_lift.png")
            mlflow.log_artifact(f"{EXPORT}/shap_top_features.png")

# COMMAND ----------
out = table.assign(fold=table["fold"].astype(str))
(spark.createDataFrame(out[["model", "fold", "slice", "wape", "mape", "rmse", "n_rows"]])
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.ml.forecast_lift_results"))
spark.sql(f"COMMENT ON TABLE {catalog}.ml.forecast_lift_results IS 'Forecast-lift experiment metrics per model, fold and slice'")

summary = {
    "wape_A": float(overall.loc["seasonal_naive", "wape"]),
    "wape_B": float(overall.loc["lgbm_market", "wape"]),
    "wape_C": float(overall.loc["lgbm_external", "wape"]),
    "mape_B": float(overall.loc["lgbm_market", "mape"]), "mape_C": float(overall.loc["lgbm_external", "mape"]),
    "rmse_B": float(overall.loc["lgbm_market", "rmse"]), "rmse_C": float(overall.loc["lgbm_external", "rmse"]),
    "lift_pct": point, "lift_ci": [lo, hi], "lift_per_fold": {k: float(v) for k, v in by_fold.items() if k != "all"},
    "test_rows": int(overall.loc["lgbm_external", "n_rows"]), "folds": len(res["folds"]),
    "train_rows_last_fold": int(res["n_train"]),
    "top_features": {k: float(v) for k, v in imp.head(10).items()},
    "by_commodity_lift": {r.slice: float(r.lift_pct) for r in lift[lift.slice.str.startswith("commodity=")].itertuples()},
    "by_season_lift": {r.slice: float(r.lift_pct) for r in lift[lift.slice.str.startswith("season=")].itertuples()},
    "by_rain_lift": {r.slice: float(r.lift_pct) for r in lift[lift.slice.str.startswith("rain=")].itertuples()},
    "by_season_wape_B": {s: float(v) for s, v in table[(table.model == "lgbm_market") & table.slice.str.startswith("season=")].set_index("slice")["wape"].items()},
}
with open(f"{EXPORT}/forecast_lift_summary.json", "w") as fh:
    json.dump(summary, fh, indent=2)
dbutils.notebook.exit(json.dumps(summary))
