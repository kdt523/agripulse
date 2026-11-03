# AgriPulse — External-Data Lakehouse for Agri Demand Forecasting

## Why this project exists (read first)
Built by Krushna Datir (kd) to close resume gaps for the **Syngenta AMEA Data & AI — Data Engineering Intern (External Datasets & Pipelines)** role.
Every decision must serve the JD keywords. If a choice trades "cool" vs "covers a JD keyword", pick the JD keyword.

JD keywords this repo MUST visibly demonstrate (code + README):
Databricks · Delta Lake · PySpark · Azure Data Factory · Airflow · GitHub · Python (pandas, requests, APIs) · SQL ·
ingestion pipelines · external data sources · weather APIs · market data · remote sensing / agronomic indicators · geo-data ·
data cleaning & standardisation · data quality standards · data freshness alerting · pipeline reliability monitoring ·
data catalogue · documentation · marketing segmentation · sales forecasting · collaboration with data science (feature tables).

## One-line pitch
PySpark pipelines ingest 4 real external sources (weather, NASA agro-climate, mandi market prices, geo/crop data) for ~700 Indian districts
into a Databricks medallion lakehouse with enforced quality, freshness SLAs and a data catalog — then PROVE the value: external features
cut a LightGBM demand-forecast error by X% and power an agro-climatic district segmentation.

## Stack
- Databricks Free Edition (serverless compute, Unity Catalog, Workflows/Jobs, Lakeflow Declarative Pipelines a.k.a. DLT)
- PySpark, Delta Lake, Databricks SQL, Python 3.11 (pandas, requests, tenacity, pydantic)
- Databricks Asset Bundles (DAB) for deploy-as-code; Databricks CLI
- Apache Airflow (docker-compose, local) triggering Databricks jobs
- Azure Data Factory (Azure for Students) — one copy pipeline landing files to ADLS/Blob (optional phase, but do it if possible)
- LightGBM, scikit-learn (KMeans), MLflow (built into Databricks) for the downstream proof
- GitHub Actions CI: ruff, pytest (+ chispa for Spark DataFrame tests), bundle validate/deploy
- Alerts: Databricks SQL alerts + Slack/email webhook

## Sources (all free)
| Source | Endpoint | Granularity |
|---|---|---|
| Open-Meteo Historical + Forecast | archive-api.open-meteo.com/v1/archive, api.open-meteo.com/v1/forecast | daily per district centroid |
| NASA POWER (agro-climate) | power.larc.nasa.gov/api/temporal/daily/point (community=ag) | daily per centroid |
| Agmarknet mandi prices | api.data.gov.in resource 9ef84268-d588-465a-a308-a864a43d0070 (free API key) + historical CSV dumps | daily per market x commodity |
| District geo + crop area | DataMeet India district GeoJSON (github.com/datameet/maps) + ICRISAT district-level DB (CSV) | static / yearly |
If an endpoint changes, look it up, keep the same schema contract, and update docs/sources.md.

## Repo layout (target)
```
agripulse/
  CLAUDE.md  PLAN.md  README.md  databricks.yml  pyproject.toml
  src/agripulse/
    common/      config.py, http.py (retry/rate-limit), logging.py, schemas.py
    ingest/      openmeteo.py, nasa_power.py, agmarknet.py, geo.py   # bronze writers
    transform/   silver_*.py, gold_district_week.py
    quality/     expectations.py, scorecard.py, freshness.py
    ml/          features.py, forecast_lift.py, segmentation.py
    chaos/       fault_injector.py
  pipelines/     dlt_silver.py (Lakeflow declarative pipeline)
  notebooks/     00_setup.py ... 90_benchmarks.py (Databricks source format)
  resources/     jobs.yml, pipelines.yml, alerts.yml (DAB)
  airflow/       docker-compose.yml, dags/agripulse_daily.py
  adf/           pipeline_agmarknet_copy.json, linkedService*.json
  tests/         unit/, integration/
  docs/          architecture.md (mermaid), sources.md, DATA_CATALOG.md, runbook.md, results.md, resume_bullets.md
  .github/workflows/ci.yml, deploy.yml
```

## Unity Catalog naming
catalog `agripulse`; schemas `bronze`, `silver`, `gold`, `ops` (quality/freshness/run logs), `ml`.
Tables: snake_case; every table and column gets a COMMENT; tag PII=false, source=<name>, owner=kd.

## Working rules for the agent
1. Work phase by phase from PLAN.md. Before each phase, load the matching skill in `.claude/skills/`. Do not skip a phase's "Done when" checks.
2. All config via `src/agripulse/common/config.py` + bundle variables; secrets (data.gov.in key, Slack webhook) via Databricks secret scope `agripulse` — never hardcode.
3. Every ingest is idempotent and incremental (watermark in `ops.ingest_watermarks`), supports `--backfill start end`.
4. Every write is a Delta MERGE on natural keys — re-running a day must not duplicate.
5. Every table gets tests: schema test + at least one quality rule.
6. Log every run to `ops.pipeline_runs` (run_id, source, rows_in, rows_out, rows_quarantined, duration_s, status).
7. Record real measured numbers in `docs/results.md`. NEVER invent metrics. Resume bullets are generated from results.md only.
8. Keep Free Edition limits in mind: serverless only, limited concurrent jobs, small compute — batch API calls, cache, avoid huge shuffles. If a feature is unavailable on Free Edition, document the workaround in docs/runbook.md.
9. Commit after each phase with a clear message; keep main green in CI.
