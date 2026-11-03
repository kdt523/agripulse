# Databricks notebook source
# MAGIC %md
# MAGIC # 71 — Agro-climatic district segmentation (KMeans)

# COMMAND ----------
# MAGIC %pip install -q matplotlib geopandas pydantic-settings tenacity
# MAGIC %restart_python

# COMMAND ----------
import json
import os
import sys

import matplotlib

matplotlib.use("Agg")
import geopandas as gpd
import matplotlib.pyplot as plt
import mlflow
from shapely import wkt

sys.path.insert(0, os.path.abspath("../src"))
dbutils.widgets.text("catalog", "agripulse")
catalog = dbutils.widgets.get("catalog")

from agripulse.ml import segmentation as seg

EXPORT = f"/Volumes/{catalog}/bronze/landing/exports"
os.makedirs(EXPORT, exist_ok=True)

# COMMAND ----------
prof = spark.table(f"{catalog}.gold.district_profile").toPandas()
prof[seg.SHARES] = prof[seg.SHARES].fillna(0.0)  # no mandi data for the crop = zero share (keeps profile means meaningful)
res = seg.fit(prof)
names = seg.name_segments(res["X"], res["labels"])
prof["segment"] = res["labels"]
prof = prof.join(names, on="segment")
print("silhouette:", res["silhouette"], "chosen k:", res["k"])

# COMMAND ----------
user = spark.sql("select current_user()").collect()[0][0]
mlflow.set_experiment(f"/Users/{user}/agripulse_segmentation")
with mlflow.start_run(run_name="kmeans"):
    mlflow.log_params({"k": res["k"], "features": ",".join(seg.FEATURES)})
    mlflow.log_metric("silhouette", res["silhouette"][res["k"]])
    for k, v in res["silhouette"].items():
        mlflow.log_metric("silhouette_by_k", v, step=k)

# segment map (choropleth from the stored simplified polygons)
geo = spark.table(f"{catalog}.bronze.geo_districts").select("district_id", "geometry_wkt").toPandas()
g = gpd.GeoDataFrame(prof.merge(geo, on="district_id"), geometry=geo.merge(prof[["district_id"]], on="district_id")["geometry_wkt"].map(wkt.loads), crs="EPSG:4326")
fig, ax = plt.subplots(figsize=(8, 9))
g.plot(column="segment", categorical=True, cmap="tab10", legend=True, linewidth=0.1, edgecolor="white", ax=ax,
       legend_kwds={"title": "segment", "loc": "lower left"})
ax.set_title(f"Agro-climatic district segments (k={res['k']}, silhouette {res['silhouette'][res['k']]:.2f})")
ax.set_axis_off()
fig.tight_layout()
fig.savefig(f"{EXPORT}/segment_map.png", dpi=140)

fig2, ax2 = plt.subplots(figsize=(6, 5))
sc = ax2.scatter(res["pca"][:, 0], res["pca"][:, 1], c=res["labels"], cmap="tab10", s=10)
ax2.set_xlabel(f"PC1 ({res['pca_var'][0]:.0%})")
ax2.set_ylabel(f"PC2 ({res['pca_var'][1]:.0%})")
ax2.set_title("Segments in PCA space")
fig2.tight_layout()
fig2.savefig(f"{EXPORT}/segment_pca.png", dpi=140)

# COMMAND ----------
cols = ["district_id", "district_name_std", "state_std", "segment", "segment_name"] + seg.FEATURES
out = prof[cols].copy()
spark.createDataFrame(out).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.ml.district_segments")
spark.sql(f"COMMENT ON TABLE {catalog}.ml.district_segments IS 'KMeans agro-climatic segment per district with profile features'")

profile_tbl = prof.groupby(["segment", "segment_name"]).agg(districts=("district_id", "count"), **{c: (c, "mean") for c in seg.FEATURES}).reset_index()
profile_tbl = profile_tbl.merge(names[["marketing_use"]], left_on="segment", right_index=True)
profile_tbl.to_csv(f"{EXPORT}/segment_profiles.csv", index=False)
summary = {"k": res["k"], "silhouette": {str(k): v for k, v in res["silhouette"].items()}, "best_k": res["best_k"],
           "districts": int(len(prof)), "sizes": prof["segment"].value_counts().sort_index().to_dict(),
           "pca_var": [float(v) for v in res["pca_var"]]}
with open(f"{EXPORT}/segmentation_summary.json", "w") as fh:
    json.dump(summary, fh, indent=2, default=str)
dbutils.notebook.exit(json.dumps(summary, default=str))
