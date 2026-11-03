# Runbook

## Setup from a clean clone
1. Install Python 3.11 or 3.12, JDK 17 (PySpark 3.5 does not run on Java 23+), and the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html).
2. `pip install -e ".[dev]"`, then `pytest` and `ruff check .`
3. `databricks configure --profile agripulse` (host + personal access token).
4. `databricks secrets create-scope agripulse`, then put `slack_webhook` (and `datagov_api_key` if you have one).
5. `databricks bundle validate -t dev && databricks bundle deploy -t dev`
6. Run `agripulse_setup`, then `agripulse_ingest` with `source=geo`, then the other sources (below).

## Common operations
| Task | Command |
|---|---|
| Load geo | `databricks bundle run agripulse_ingest -t dev --params source=geo` |
| Backfill NASA POWER | `--params source=nasa_power,mode=backfill,start=2021-01-01` |
| Backfill / extend Open-Meteo (quota-aware, resumable) | `databricks bundle run agripulse_ingest_long -t dev --params source=weather,mode=catchup` |
| Re-run one day | `--params source=nasa_power,mode=backfill,start=2026-10-01,end=2026-10-01` (MERGE makes it safe) |
| Load mandi history archive | `--params source=mandi_archive,mode=backfill,start=2013,end=2017` |
| Load new mandi CSVs (from ADF) | drop files in `/Volumes/agripulse/bronze/landing/mandi_incoming/`, run `source=mandi_landing` |
| Rebuild district name map | `databricks bundle run agripulse_name_map -t dev` (edit `docs/district_overrides.csv` first) |
| Silver pipeline | `databricks bundle run agripulse_silver -t dev` |
| Gold full / incremental | `databricks bundle run agripulse_gold -t dev --params mode=full` |
| Freshness check | `databricks bundle run agripulse_freshness -t dev` |
| Forced stale-feed alert test | `... agripulse_freshness --params sla_override=weather=-1` (must raise an alert) |
| Apply catalog metadata | `databricks bundle run agripulse_catalog -t dev` |

## Alert types and what to do
| Alert | Meaning | Action |
|---|---|---|
| `freshness` | A source's newest data is older than its SLA | Check the latest `ops.pipeline_runs` row for that source; look for a failed ingest task; re-run with `mode=backfill` for the missing dates. NASA POWER lags ~4 days, so a 5-day SLA is normal. |
| `dq` | A silver table's pass rate fell below 95% | `SELECT r, count(*) FROM <table>_quarantine LATERAL VIEW explode(failed_rules) x AS r GROUP BY r` to see which rule. For mandi, check `ops.district_name_map` for new unmatched spellings and add overrides. |
| `volume` | Rows ingested fell more than 50% below the trailing average | Check the source's API status; check Open-Meteo quota (429s in the run log). |
| Pipeline update failed with `SCHEMA CONTRACT VIOLATION` | A bronze table lost a required column (schema drift) | Compare the source response to `tests/fixtures`; fix the ingestor; do not weaken the contract. |

## Rotate a secret
`databricks secrets put-secret agripulse slack_webhook` (paste the new value). Nothing in the repo changes. After rotating a Databricks token, run `databricks configure --profile agripulse` again.

## Add a new source
1. Add an ingestor in `src/agripulse/ingest/` that subclasses `BaseIngestor` (`fetch` returns flat dict rows).
2. Add a parser test with a saved fixture in `tests/fixtures/`.
3. Add the source to the `10_ingest.py` widgets and the daily job.
4. Add a bronze contract in `quality/expectations.py::BRONZE_CONTRACTS`, a silver table with rules, and a freshness query in `quality/freshness.py`.
5. Document it in `docs/catalog.yml` and `docs/sources.md`; run `agripulse_catalog`; confirm `missing_count` is 0.

## Free Edition workarounds (documented per project rule 8)
| Limitation | Workaround |
|---|---|
| `api.data.gov.in` blocked from serverless | Azure Data Factory copies the API to ADLS; files land in `mandi_incoming`; `mandi_landing` loads them. History comes from the public archive. |
| Open-Meteo free quota | Resumable per-district catch-up; NASA POWER holds the multi-year history. |
| Workspace files can fail to import (I/O error on a stale entry) | Rename the module (this happened to `run_logger.py`, now `runlog.py`). |
| Serverless only | No cluster config; notebooks `%pip install` their small dependencies. |
| `ALTER TABLE ... ALTER COLUMN` fails on materialized views | Use `COMMENT ON COLUMN` (what `50_catalog.py` does). |
| Databricks Free Edition has no per-job email/Slack integration guarantees | Alerts are written to `ops.alert_log` first, then pushed to Slack. |

## Production checklist
- [x] Idempotent reruns (Delta MERGE on natural keys; proven by zero-insert re-runs)
- [x] Backfill command (above)
- [x] Retries / timeouts on the daily job (2 retries, per-task timeouts)
- [x] Secrets in the `agripulse` scope, none in the repo
- [ ] Least-privilege grants (`GRANT SELECT ON SCHEMA gold TO data_science`): applies when a group exists
- [x] Alerting tested (forced stale-feed test fired in `ops.alert_log`; Slack delivery needs a webhook)
- [x] Logging to ops tables
- [x] Cost guard: serverless only, one scheduled run per day, no idle compute
- [ ] Data retention: `VACUUM` policy to be scheduled
- [x] Pinned dependency ranges in `pyproject.toml`

## CI/CD
- `ci.yml` (every PR/push): ruff, mypy (reported), pytest with a 70% coverage gate (Delta MERGE tests run here on Linux), `bundle validate` for dev and prod.
- `deploy.yml` (push to main): deploy dev -> integration test (3 districts, idempotency) -> regenerate `docs/DATA_CATALOG.md` and fail if stale -> **manual approval** (GitHub environment `production` with required reviewers) -> deploy prod.
- Required repo secrets: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`. Required environment: `production` (add reviewers).
- Grants: `scripts/grants.sql` (needs the `data_science` group, which Free Edition workspaces do not have by default).
