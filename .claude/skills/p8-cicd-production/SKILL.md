---
name: p8-cicd-production
description: Phase 8 of AgriPulse — production hardening, test coverage, GitHub Actions CI/CD deploying Databricks Asset Bundles to dev and prod targets.
---
# P8 — CI/CD & production readiness

## Tests
- unit (local SparkSession + delta-spark): parsers, standardisation, expectations logic (as pure functions too), feature calcs vs hand-computed fixtures (chispa assert_df_equality), freshness logic, alert payload.
- integration (pytest -m integration, runs on Databricks via `databricks bundle run` in CI on main): small end-to-end with 3 districts.
- coverage ≥70% on src/ (pytest-cov), badge in README.

## GitHub Actions
- ci.yml (PR): ruff, mypy (light), pytest unit + coverage, `databricks bundle validate -t dev`.
- deploy.yml (push to main): bundle deploy -t dev → integration tests → manual approval environment → bundle deploy -t prod. Use repo secrets DATABRICKS_HOST/TOKEN (or OIDC if supported).
- Also regenerate DATA_CATALOG.md in CI and fail if stale.

## Production checklist (document in docs/runbook.md, tick each)
idempotent reruns · backfill command · retries/timeouts · secrets in scope · least-privilege grants (GRANT SELECT on gold to a `data_science` group) · alerting tested · logging to ops tables · cost guard (serverless, schedule once/day, no idle compute) · data retention (VACUUM policy) · pinned dependency versions · README setup works from clean clone.

## Done when
Main green; prod target deployed by Actions; grants applied; checklist ticked. Commit `ci: bundle deploy pipeline and production hardening`.
