# NYC Taxi Lakehouse Lite — Master plan

**Source of truth** for this repository. In Agent chat: `@docs/ROADMAP.md`.

**Status:** Phase 0 complete. **Next:** Phase 1 (bronze ingestion with dlt + pyiceberg).

## Goal

Medallion **lakehouse** on NYC TLC trip data for senior data-engineering skills — **no Docker**, friendly to a **16GB Windows** laptop. Optional Phase 7 adds distributed Spark on a cloud free tier.

## Stack (all pip / uv)

| Layer | Tool |
|-------|------|
| Engine | DuckDB + Polars |
| Tables | Apache Iceberg (`pyiceberg`, SQLite catalog) |
| Ingest | dlt |
| Gold | dbt-duckdb |
| Orchestration | Dagster |
| Quality | pandera + dbt tests |
| Storage | `./lake/` on disk → S3/R2 in Phase 7 |
| BI (optional) | Evidence.dev or Streamlit |

## Phases

| # | Name | Outcome |
|---|------|---------|
| 0 | Scaffold | uv project, config profiles, Dagster bootstrap asset, CI |
| 1 | Bronze | dlt → partitioned Iceberg bronze |
| 2 | Silver | Clean + pandera contracts |
| 3 | Incremental | pyiceberg upserts, Dagster partitions/backfill |
| 4 | Gold | dbt-duckdb star schema + tests/docs |
| 5 | Tuning | Partitioning, file sizing, DuckDB EXPLAIN |
| 6 | Serving | Dashboard (Evidence/Streamlit) |
| 7 | Cloud (opt.) | S3/R2 + Databricks Community / EMR Serverless |
| 8 | Polish | Portfolio README, hardening |

## Repo map

- `lakehouse/` — shared config
- `ingestion/`, `transforms/` — pipeline code
- `orchestration/` — Dagster `definitions.py`
- `dbt/` — gold models (Phase 4)
- `config/profiles/` — `local` / `cloud`
- `lake/` — local data (gitignored)
- `docs/PLAN.md` — detailed plan checklist

## Decisions

- Iceberg via **pyiceberg** (not Delta) unless write limits force **delta-rs**.
- **Dagster** locally instead of Airflow (lighter RAM, asset lineage).
- **No Docker** in this repo — see separate `nyc-taxi-lakehouse` if you ever want the full cluster lab.
