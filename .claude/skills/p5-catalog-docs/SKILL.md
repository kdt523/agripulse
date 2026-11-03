---
name: p5-catalog-docs
description: Phase 5 of AgriPulse — Unity Catalog metadata (comments, tags, lineage) and generated data catalogue + documentation for every dataset.
---
# P5 — Data catalogue & documentation (JD: "Maintain data catalogue entries for all external datasets onboarded")

1. `notebooks/50_catalog.py`: apply COMMENT ON TABLE/COLUMN for every table from a single YAML `docs/catalog.yml` (source of truth: description, owner, source URL, licence, refresh cadence, SLA, grain, keys, known issues). Apply tags: `source`, `layer`, `refresh`, `pii=false`.
2. Generator `src/agripulse/quality/catalog_export.py`: query `information_schema.tables/columns` + tags → write `docs/DATA_CATALOG.md` (one section per dataset: purpose, source & licence, schema table, quality rules, freshness SLA, lineage bronze→silver→gold, sample query).
3. Lineage: screenshot Unity Catalog lineage graph for gold.district_week.
4. `docs/sources.md`: per source evaluation — why chosen, coverage, latency, limits, licence, alternatives considered (this shows "identify and evaluate external data sources").
5. `docs/runbook.md`: how to backfill, rerun a day, handle alert types, rotate API key, add a new source (step list).
6. `docs/architecture.md`: mermaid diagram sources → ADF/landing → bronze → DLT silver (+quarantine) → gold → ML/segmentation; ops plane (runs, DQ, freshness, alerts).

## Done when
100% tables & columns have comments (query check returns 0 missing); DATA_CATALOG.md regenerated in CI step. Commit `docs: unity catalog metadata and data catalogue`.
