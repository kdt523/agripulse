# Architecture

```mermaid
flowchart LR
  subgraph SRC[External sources]
    OM[Open-Meteo<br/>archive + forecast]
    NP[NASA POWER<br/>agro-climate]
    AG[Agmarknet<br/>mandi prices]
    GEO[DataMeet<br/>districts]
  end

  ADF[Azure Data Factory<br/>REST copy -> ADLS]:::cloud
  LAND[(UC Volume<br/>bronze.landing<br/>raw JSONL / CSV)]

  subgraph BRONZE[Bronze - Delta, MERGE on natural keys]
    B1[weather_daily_raw / forecast_raw]
    B2[nasa_power_raw]
    B3[mandi_prices_raw]
    B4[geo_districts]
  end

  subgraph SILVER[Silver - Lakeflow Declarative Pipeline]
    S1[weather_daily]
    S2[agro_climate_daily]
    S3[mandi_prices_daily]
    S4[districts]
    Q[(*_quarantine<br/>failed_rules)]
  end

  subgraph GOLD[Gold]
    G1[district_week<br/>features]
    G2[district_profile]
    G3[v_ds_training]
  end

  subgraph ML[ML proof of value]
    M1[LightGBM forecast lift<br/>MLflow]
    M2[KMeans segments]
  end

  OM --> LAND
  NP --> LAND
  GEO --> LAND
  AG -- api.data.gov.in --> ADF --> LAND
  LAND --> B1 & B2 & B3 & B4
  B1 & B2 --> S1
  B2 --> S2
  B3 --> S3
  B4 --> S4
  S1 & S2 & S3 & S4 --> G1 & G2
  S1 -.rejects.-> Q
  S2 -.rejects.-> Q
  S3 -.rejects.-> Q
  G1 --> G3 --> M1
  G2 --> M2

  subgraph OPS[Ops plane - schema ops]
    O1[pipeline_runs]
    O2[ingest_watermarks]
    O3[dq_scorecard]
    O4[freshness_status / alert_log]
  end
  WF[Databricks Workflow<br/>agripulse_daily 06:30 IST]:::cloud
  AF[Airflow DAG<br/>trigger + freshness gate]:::cloud
  SL[Slack webhook]:::cloud
  WF --> BRONZE & SILVER & GOLD
  AF --> WF
  BRONZE & SILVER & GOLD -.log.-> OPS
  O4 --> SL
  classDef cloud fill:#e8f1ff,stroke:#3b6fd4
```

## Layers
- **Bronze**: raw but typed-enough. Carries `_ingested_at`, `_source`, `_run_id`, `_request_params`. Raw files are first landed in the Volume, so any load is replayable. Every write is a Delta `MERGE` on natural keys, so re-running a window never duplicates rows.
- **Silver**: Lakeflow Declarative Pipeline. Standardises units and names, applies expectations, and routes every rejected row to a `_quarantine` table with the failed rule names. A schema-contract check fails the whole update if a bronze table lost a required column.
- **Gold**: `district_week` (district x ISO week x commodity) for data science, `district_profile` for segmentation, and `v_ds_training` with next-week targets. Leakage rules are in `feature_dictionary.md`.
- **Ops**: run log, watermarks, DQ scorecard, freshness status, alert log. Everything is queryable with SQL.

## Orchestration
Databricks Workflows is the primary scheduler. The daily DAG runs: geo gate (Mondays) -> weather / NASA / forecast / mandi ingest in parallel -> name map -> silver pipeline -> gold build -> DQ scorecard -> freshness check. Airflow triggers the same job and independently re-checks freshness, to show cross-tool orchestration.
