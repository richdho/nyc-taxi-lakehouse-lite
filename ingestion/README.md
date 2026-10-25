# Ingestion (Phase 1)

**dlt** downloads NYC TLC yellow taxi Parquet; **pyiceberg** lands **`bronze.yellow_taxi_trips`**.

## Defaults

- Month range: **`2024-01`** only (`TLC_YELLOW_START` / `TLC_YELLOW_END` in `.env`).
- Staging downloads: `lake/staging/tlc/yellow/` (gitignored via `staging/`).
- Table location: `lake/bronze/yellow_taxi_trips/`.

## Run

```powershell
.\make.ps1 bootstrap
.\make.ps1 ingest-yellow
```

Or in Dagster: materialize **`lakehouse_bootstrap`**, then materialize **`bronze_yellow_taxi`** / **`silver_yellow_taxi`** for a **month partition** (e.g. `2024-01`).

## Backfill

Dagster partitions run from **`2024-01`** through **`2025-12`**. Materialize the months you want on **`bronze_yellow_taxi`**, then **`silver_yellow_taxi`**.

For the CLI, set `TLC_YELLOW_START` and `TLC_YELLOW_END` (inclusive) and run `.\make.ps1 ingest-yellow`, then `.\make.ps1 silver-yellow`. The default range is a single month.

## Idempotent re-runs

Re-running the same month **deletes that `trip_month` partition** in Iceberg, then appends fresh data. Row counts for that month should not double.

Appends are buffered up to the project write target (2 GiB of uncompressed Arrow, about one yellow-taxi month) so a month is not split into one Parquet file per 250k-row batch. `.\make.ps1 tune` rewrites any `trip_month` that still has more than one data file and prints DuckDB `EXPLAIN ANALYZE` for `2024-01`.

Set `TLC_FORCE_DOWNLOAD=1` to re-download Parquet even if a staging file exists.

## Config

| Variable | Default | Purpose |
|----------|---------|---------|
| `TLC_YELLOW_START` | `2024-01` | First month (inclusive) |
| `TLC_YELLOW_END` | `2024-01` | Last month (inclusive) |
| `TLC_FORCE_DOWNLOAD` | off | Re-fetch Parquet from TLC |
| `TLC_STAGING_DIR` | under `lake/staging/...` | Override staging path |

Catalog helpers: `lakehouse.iceberg_catalog.prepare_bronze_catalog()`.
