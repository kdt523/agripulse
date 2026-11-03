"""Unity Catalog metadata: apply comments + tags from docs/catalog.yml, verify coverage, generate docs/DATA_CATALOG.md."""

from __future__ import annotations

import re

import yaml

from agripulse.quality import expectations as ex

LINEAGE = {
    "bronze.geo_districts": "DataMeet GeoJSON -> bronze.geo_districts -> silver.districts -> gold.district_week / district_profile",
    "bronze.weather_daily_raw": "Open-Meteo archive -> bronze.weather_daily_raw -> silver.weather_daily -> gold.district_week",
    "bronze.weather_forecast_raw": "Open-Meteo forecast -> bronze.weather_forecast_raw -> silver.weather_daily (is_forecast) -> gold.district_week",
    "bronze.nasa_power_raw": "NASA POWER -> bronze.nasa_power_raw -> silver.agro_climate_daily + silver.weather_daily (fallback) -> gold.district_week",
    "bronze.mandi_prices_raw": "Agmarknet (ADF / archive) -> bronze.mandi_prices_raw -> silver.mandi_prices_daily -> gold.district_week",
}
RULES = {
    "silver.districts": ex.DISTRICT_RULES,
    "silver.weather_daily": ex.WEATHER_RULES,
    "silver.agro_climate_daily": ex.AGRO_RULES,
    "silver.mandi_prices_daily": {**ex.MANDI_RULES, **{f"{k} (warn)": v for k, v in ex.MANDI_WARN_RULES.items()}},
}
SYSTEM_PREFIXES = ("__", "event_log_")


def load(path: str = "docs/catalog.yml") -> dict:
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _q(s: str) -> str:
    return s.replace("'", "''")


def _tag_value(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.@ -]", "", str(s).split(" (")[0])[:200].strip() or "n/a"


def _table_columns(spark, catalog: str, schema: str, table: str) -> list[str]:
    rows = spark.sql(f"SELECT column_name FROM {catalog}.information_schema.columns WHERE table_schema='{schema}' AND table_name='{table}' ORDER BY ordinal_position").collect()
    return [r["column_name"] for r in rows if not r["column_name"].startswith("__")]


def apply_metadata(spark, cfg: dict, catalog: str = "agripulse") -> dict:
    """COMMENT ON TABLE/COLUMN and tags for every catalogued table that exists. Returns counts and problems."""
    cols_doc, applied, problems = cfg["columns"], 0, []
    for fq, meta in cfg["tables"].items():
        schema, table = fq.split(".")
        if not spark.catalog.tableExists(f"{catalog}.{fq}"):
            continue
        spark.sql(f"COMMENT ON TABLE {catalog}.{fq} IS '{_q(meta['description'])}'")
        for col in _table_columns(spark, catalog, schema, table):
            text = (meta.get("column_overrides") or {}).get(col) or cols_doc.get(col)
            if not text:
                problems.append(f"{fq}.{col}: no description in docs/catalog.yml")
                continue
            spark.sql(f"COMMENT ON COLUMN {catalog}.{fq}.`{col}` IS '{_q(text)}'")
            applied += 1
        tags = {"layer": meta["layer"], "source": meta["source"], "refresh": _tag_value(meta["refresh"]), "pii": "false", "owner": cfg["owner"]}
        tag_sql = ", ".join(f"'{k}' = '{_q(v)}'" for k, v in tags.items())
        for stmt in ("ALTER TABLE", "ALTER VIEW", "ALTER MATERIALIZED VIEW"):
            try:
                spark.sql(f"{stmt} {catalog}.{fq} SET TAGS ({tag_sql})")
                break
            except Exception:
                continue
        else:
            problems.append(f"{fq}: could not set tags")
    return {"column_comments_applied": applied, "problems": problems}


def missing_comments(spark, catalog: str = "agripulse") -> list[str]:
    """Tables/columns in bronze/silver/gold/ops/ml without a comment (system-managed objects excluded)."""
    out = []
    tabs = spark.sql(f"SELECT table_schema, table_name, comment FROM {catalog}.information_schema.tables WHERE table_schema IN ('bronze','silver','gold','ops','ml')").collect()
    for t in tabs:
        if t["table_name"].startswith(SYSTEM_PREFIXES):
            continue
        if not t["comment"]:
            out.append(f"{t['table_schema']}.{t['table_name']} (table)")
    cols = spark.sql(f"SELECT table_schema, table_name, column_name, comment FROM {catalog}.information_schema.columns WHERE table_schema IN ('bronze','silver','gold','ops','ml')").collect()
    for c in cols:
        if c["table_name"].startswith(SYSTEM_PREFIXES) or c["column_name"].startswith("__"):
            continue
        if not c["comment"]:
            out.append(f"{c['table_schema']}.{c['table_name']}.{c['column_name']}")
    return out


def render_markdown(spark, cfg: dict, catalog: str = "agripulse") -> str:
    lines = ["# Data catalogue", "", "_Generated from `docs/catalog.yml` + Unity Catalog `information_schema` by `quality/catalog_export.py`. Do not edit by hand._", ""]
    for layer in ("bronze", "silver", "gold", "ops", "ml"):
        items = [(fq, m) for fq, m in cfg["tables"].items() if m["layer"] == layer and spark.catalog.tableExists(f"{catalog}.{fq}")]
        if not items:
            continue
        lines += [f"## {layer.capitalize()} layer", ""]
        for fq, m in items:
            schema, table = fq.split(".")
            lines += [f"### `{catalog}.{fq}`", "", m["description"], ""]
            lines += [
                f"- **Source:** {m.get('source_url', m['source'])}",
                f"- **Licence:** {m.get('licence', 'n/a')}",
                f"- **Refresh:** {m['refresh']}  |  **Freshness SLA:** {m['sla']}",
                f"- **Grain:** {m['grain']}  |  **Keys:** {', '.join(m['keys'])}  |  **Owner:** {cfg['owner']}  |  PII: false",
            ]
            if m.get("known_issues"):
                lines.append(f"- **Known issues:** {m['known_issues']}")
            if fq in LINEAGE:
                lines.append(f"- **Lineage:** {LINEAGE[fq]}")
            if fq in RULES:
                lines += ["", "**Quality rules** (violating rows go to the `_quarantine` table):", ""]
                lines += [f"- `{k}`: `{v}`" for k, v in RULES[fq].items()]
            rows = spark.sql(f"SELECT column_name, data_type, comment FROM {catalog}.information_schema.columns WHERE table_schema='{schema}' AND table_name='{table}' AND column_name NOT LIKE '\\_\\_%' ORDER BY ordinal_position").collect()
            lines += ["", "| Column | Type | Description |", "|---|---|---|"]
            lines += [f"| `{r['column_name']}` | {r['data_type']} | {r['comment'] or ''} |" for r in rows]
            lines += ["", "```sql", f"SELECT * FROM {catalog}.{fq} LIMIT 10;", "```", ""]
    return "\n".join(lines) + "\n"
