---
name: p9-showcase
description: Phase 9 of AgriPulse — README, results, architecture diagram, screenshots, demo video and resume bullets generated strictly from measured results, targeted at the Syngenta Data Engineering Intern JD.
---
# P9 — Showcase (what the recruiter actually sees)

## README.md structure
1. Title + one-line pitch + badges (CI, coverage)
2. **Results table first** (from results.md): rows/districts/years/sources, forecast WAPE lift, faults detected, incremental speedup, freshness SLA days, test coverage
3. Architecture diagram (mermaid + PNG)
4. Data sources table with licences
5. Medallion layers + quality rules summary
6. Orchestration & monitoring (screenshots: Workflow run graph, Slack alert, Ops dashboard, Airflow DAG, ADF run, UC lineage)
7. Proof of value (lift chart, SHAP, segment map)
8. Quickstart (clone → configure → bundle deploy → run)
9. Design decisions & trade-offs (Free Edition limits, why DLT, why Workflows + Airflow, name-matching problem)
10. Roadmap

## resume_bullets.md (fill only with numbers from results.md)
Title line: `AgriPulse: External-Data Lakehouse for Agri Demand Forecasting | Databricks, PySpark, Delta Lake, Lakeflow, Azure Data Factory, Airflow, MLflow, LightGBM, GitHub Actions`
- Built Databricks lakehouse ingesting 4 external sources (weather API, NASA remote-sensing agro-climate, mandi market data, geo/crop data) for {N} districts / {R}M rows using PySpark + Delta Lake with incremental MERGE loads ({S}× faster than full refresh).
- Enforced data quality standards via {E} Lakeflow expectations, quarantine tables and freshness-SLA/volume alerting; detected {D}/50 injected faults with {G} bad rows reaching gold; documented all datasets in Unity Catalog data catalogue.
- Orchestrated with Databricks Workflows, Airflow and Azure Data Factory, deployed via Asset Bundles + GitHub Actions; external features cut LightGBM demand-forecast WAPE by {L}% and powered a {K}-segment agro-climatic district segmentation for marketing.
Also write a 2-line version for the Gen AI / SDE resumes and a 60-sec interview pitch + 5 likely interview Q&As (why Delta MERGE, how DLT expectations work, how you handle late data, name matching, how you'd scale to all AMEA countries).

## Demo
2-min Loom: architecture → trigger job → alert → dashboard → lift result.

## Done when
README renders cleanly on GitHub, all screenshots present, resume_bullets.md filled with no `{}` placeholders left. Final commit `docs: showcase, results and resume bullets`.
