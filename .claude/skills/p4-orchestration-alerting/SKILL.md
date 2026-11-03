---
name: p4-orchestration-alerting
description: Phase 4 of AgriPulse — Databricks Workflows daily job, freshness SLA monitoring and alerting, Airflow DAG triggering Databricks, and an Azure Data Factory copy pipeline.
---
# P4 — Orchestration, monitoring, freshness alerting (JD: "monitor pipeline reliability and set up alerting for data freshness and quality")

## 1. Databricks Workflow (resources/jobs.yml)
Job `agripulse_daily`: tasks
ingest_geo(weekly only via condition) → [ingest_weather, ingest_nasa, ingest_mandi] (parallel) → silver_pipeline (pipeline task) → gold_build → dq_scorecard → freshness_check.
Schedule 06:30 IST daily (quartz cron, timezone Asia/Kolkata). Retries 2 with backoff; task timeouts; email_notifications on_failure; queueing on.

## 2. Freshness + health checks (src/agripulse/quality/freshness.py)
- For each source: lag_days = today − max(date) in silver; compare with ops.freshness_sla (weather 1, nasa 3 [NASA lags], mandi 2, forecast 0).
- Write `ops.freshness_status(source, checked_at, max_date, lag_days, sla_days, breached BOOL)`.
- On breach or DQ pass_rate < 95% or row-count drop > 50% vs 7-day avg (volume anomaly) → POST Slack webhook with table, metric, run link. Also Databricks SQL Alert on ops.freshness_status for redundancy.
- Test it: temporarily set SLA to −1 → alert must fire; screenshot for README.

## 3. Ops dashboard
Databricks AI/BI dashboard (or SQL dashboard) "AgriPulse Ops": runs per day, duration trend, rows by source, DQ pass rate, freshness lag, quarantine counts. Export JSON into repo `resources/dashboard.lvdash.json` if possible. Screenshot.

## 4. Airflow (nice-to-have keyword)
`airflow/docker-compose.yml` (official apache/airflow image, LocalExecutor). DAG `agripulse_daily.py` using `DatabricksRunNowOperator` (apache-airflow-providers-databricks) + a `PythonSensor`/SQL check on ops.freshness_status after run, with SLA miss callback. Document: Databricks Workflows is primary; Airflow shows cross-tool orchestration.

## 5. Azure Data Factory (core stack keyword)
Using Azure for Students: create Storage account (ADLS Gen2) + ADF. Pipeline `pl_agmarknet_copy`: HTTP/REST source (data.gov.in) → Copy activity → ADLS `raw/agmarknet/dt=...` CSV, parameterised by date, daily trigger; then a Web activity calling Databricks Jobs API run-now (or document Databricks reading via Volume/external location if Free Edition blocks external storage — then copy into Volume via `databricks fs cp` step). Export ARM/JSON to `adf/`. Screenshot of successful run. Tear down after screenshots to save credit.

## Done when
- ≥7 consecutive successful scheduled daily runs (record in results.md: SLA met X/Y days)
- Forced stale-feed alert received in Slack
- Airflow DAG run success screenshot; ADF run success screenshot
- Commit `feat(ops): workflows, freshness alerts, airflow, adf`.
