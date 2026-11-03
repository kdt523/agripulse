# AgriPulse — Build Plan (zero → production-ready)

Priority order = resume gap coverage. Est. 12–14 days. Each phase has a skill in `.claude/skills/<skill>/SKILL.md`.

| # | Phase | Skill | Days | JD gaps closed |
|---|---|---|---|---|
| 0 | Accounts, repo, bundle, CI skeleton | `p0-setup` | 1 | Databricks, GitHub |
| 1 | Bronze ingestion of 4 external sources + backfill | `p1-bronze-ingestion` | 3 | PySpark, Delta Lake, APIs, weather, market, remote sensing, geo, ingestion |
| 2 | Silver: clean, standardise, DLT expectations, quarantine | `p2-silver-quality` | 2 | cleaning, standardisation, data quality standards |
| 3 | Gold: district_week feature table for data science | `p3-gold-features` | 1 | collaboration with DS, SQL, columnar |
| 4 | Orchestration + monitoring + freshness alerting | `p4-orchestration-alerting` | 2 | Workflows, Airflow, ADF, alerting, freshness, reliability |
| 5 | Data catalog + documentation | `p5-catalog-docs` | 1 | data catalogue, documentation |
| 6 | Proof of value: forecast lift + segmentation | `p6-ml-proof` | 2 | sales forecasting, marketing segmentation |
| 7 | Chaos DQ test + performance benchmarks | `p7-chaos-benchmarks` | 1 | reliability, data quality (measured) |
| 8 | CI/CD hardening, tests, prod deploy | `p8-cicd-production` | 1 | GitHub, production-readiness |
| 9 | README, results, resume bullets, demo | `p9-showcase` | 1 | everything visible to recruiter |

## Phase exit criteria (summary — full checks in each skill)
- P0: `databricks bundle validate` passes; CI green on an empty test.
- P1: ≥5 yrs backfill for all 700 districts weather + NASA; ≥3 yrs Agmarknet; geo loaded; re-run is idempotent (row count unchanged).
- P2: ≥10 expectations live; quarantine table populated with real bad rows; `ops.dq_scorecard` per table per day.
- P3: `gold.district_week` with documented feature dictionary; no nulls in keys; refresh incremental.
- P4: Daily job runs on schedule ≥7 consecutive days; stale-feed alert fires in a forced test; Airflow DAG triggers the job; ADF pipeline lands Agmarknet file.
- P5: 100% tables/columns commented; lineage visible; DATA_CATALOG.md generated from UC metadata.
- P6: forecast MAPE with vs without external features on a time-based holdout; segmentation with silhouette score + profiles.
- P7: 50 injected faults → detection rate measured; full vs incremental runtime measured.
- P8: unit + integration tests ≥70% coverage on src/; deploy to `prod` target via GitHub Actions.
- P9: README with architecture diagram, results table, screenshots; resume_bullets.md filled from results.md.

## Final deliverable metrics to fill (docs/results.md)
rows ingested · districts · years · sources · forecast MAPE base vs enriched · % lift · faults detected/injected ·
bad rows in gold · incremental vs full runtime · freshness SLA met days/total · test coverage.
