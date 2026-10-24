# Project plan — NYC Taxi Lakehouse Lite

This document mirrors the learning roadmap. Track progress by checking phases off in git or in your notes.

## Architecture

```mermaid
flowchart LR
  tlc[NYC TLC Parquet] --> dlt[dlt ingest]
  dlt --> bronze[Bronze Iceberg]
  bronze --> silver[Silver Iceberg]
  silver --> dbt[dbt-duckdb]
  dbt --> gold[Gold marts]
  gold --> bi[Evidence or Streamlit]
  duck[DuckDB engine] --- bronze
  duck --- silver
  duck --- gold
  dagster[Dagster] -.-> dlt
  dagster -.-> dbt
```

## Phase checklist

### Phase 0 — Scaffold (done)

- [x] New repo `nyc-taxi-lakehouse-lite`
- [x] uv + Python 3.11, ruff, pytest, pre-commit
- [x] `LakehouseConfig` + local/cloud profiles
- [x] Dagster `lakehouse_bootstrap` asset
- [x] dbt project skeleton
- [x] GitHub Actions CI

### Phase 1 — Bronze (done)

- [x] TLC yellow download helper and dlt source (`ingestion/tlc_yellow_dlt.py`); CLI default month `2024-01`
- [x] pyiceberg `bronze.yellow_taxi_trips`, identity-partitioned on `trip_month`
- [x] Idempotent re-runs documented (`ingestion/README.md`)

### Phase 2 — Silver (done)

- [x] Polars cleaning pipeline (`transforms/yellow_taxi_clean.py`)
- [x] pandera schema + range checks
- [x] Iceberg schema evolution (`pickup_date`, `_silver_version`)

### Phase 3 — Incremental (done)

- [x] Dagster monthly partitions `2024-01` through `2025-12` on bronze and silver
- [x] Month replace: delete that `trip_month`, then append (bronze and silver)
- [x] Backfill by materializing Dagster partitions, or `TLC_YELLOW_START` / `TLC_YELLOW_END` (`ingestion/README.md`)

### Phase 4 — Gold (done)

- [x] `fact_trips`, `dim_zone`, `dim_date`
- [x] dbt tests + `dbt docs generate` (`.\make.ps1 dbt-gold`)

### Phase 5 — Tuning

- [ ] File size / partition review
- [ ] Before/after query benchmarks (DuckDB EXPLAIN)

### Phase 6 — Serving

- [ ] Simple dashboard on gold marts

### Phase 7 — Cloud (optional)

- [ ] Object storage for `lake/`
- [ ] One distributed Spark exercise (free tier)

### Phase 8 — Polish

- [ ] Architecture write-up for interviews
- [ ] CI coverage for critical paths

## Senior skills map

| Skill | Where |
|-------|--------|
| Medallion design | Phases 1–4 |
| Iceberg format | Phases 1–3, 5 |
| Idempotent pipelines | Phases 1, 3 |
| Data contracts | Phase 2 |
| Analytics engineering | Phase 4 |
| Orchestration | Phase 3+ |
| Performance | Phase 5 |
| Distributed Spark | Phase 7 only |
