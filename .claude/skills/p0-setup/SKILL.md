---
name: p0-setup
description: Phase 0 of AgriPulse — create accounts, repo skeleton, Databricks Asset Bundle, Unity Catalog objects and CI skeleton. Use at project start.
---
# P0 — Setup

## Human steps (ask kd to do these, then continue)
1. Sign up Databricks Free Edition (databricks.com/learn/free-edition). Create a personal access token.
2. Get a free data.gov.in API key (data.gov.in → sign up → My Account → API key).
3. Create Slack incoming webhook (or use email) for alerts.
4. (Optional, for P4 ADF) Azure for Students account.
5. Install locally: Python 3.11, Databricks CLI (`curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh`), Docker.

## Agent steps
1. Create repo layout from CLAUDE.md. `pyproject.toml` with deps: pyspark (dev only, local tests), delta-spark (dev), requests, tenacity, pydantic, pandas, lightgbm, scikit-learn, mlflow, chispa, pytest, pytest-cov, ruff.
2. `databricks configure` (profile `agripulse`). Write `databricks.yml` with targets `dev` (default, mode: development) and `prod` (mode: production), variables: catalog, alert_webhook_secret, schedule.
3. `notebooks/00_setup.py`: CREATE CATALOG IF NOT EXISTS agripulse; schemas bronze/silver/gold/ops/ml; CREATE VOLUME agripulse.bronze.landing (raw files); create ops tables:
   - ops.pipeline_runs(run_id STRING, source STRING, layer STRING, started_at TIMESTAMP, duration_s DOUBLE, rows_in LONG, rows_out LONG, rows_quarantined LONG, status STRING, error STRING)
   - ops.ingest_watermarks(source STRING, key STRING, last_date DATE, updated_at TIMESTAMP)
   - ops.freshness_sla(source STRING, expected_lag_days INT, owner STRING)
4. Secret scope: `databricks secrets create-scope agripulse`; put `datagov_api_key`, `slack_webhook`.
5. `src/agripulse/common/`: config (pydantic settings, reads bundle params/widgets), http client (requests Session + tenacity retry w/ exponential backoff + rate limit + timeout), structured logging, run logger writing to ops.pipeline_runs.
6. `.github/workflows/ci.yml`: ruff, pytest (local Spark), `databricks bundle validate` (secrets DATABRICKS_HOST/TOKEN).
7. Seed `docs/` files with headings.

## Done when
- `databricks bundle validate -t dev` OK, `databricks bundle deploy -t dev` OK
- 00_setup runs; catalog + schemas + ops tables visible in Catalog Explorer
- CI green. Commit: `chore: project skeleton, bundle, UC setup`.
