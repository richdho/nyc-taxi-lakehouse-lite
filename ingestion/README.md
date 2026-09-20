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

Or in Dagster: materialize **`lakehouse_bootstrap`**, then **`bronze_yellow_taxi`**.

## Idempotent re-runs

Re-running the same month **deletes that `trip_month` partition** in Iceberg, then appends fresh data. Row counts for that month should not double.

Set `TLC_FORCE_DOWNLOAD=1` to re-download Parquet even if a staging file exists.

## Config

| Variable | Default | Purpose |
|----------|---------|---------|
| `TLC_YELLOW_START` | `2024-01` | First month (inclusive) |
| `TLC_YELLOW_END` | `2024-01` | Last month (inclusive) |
| `TLC_FORCE_DOWNLOAD` | off | Re-fetch Parquet from TLC |
| `TLC_STAGING_DIR` | under `lake/staging/...` | Override staging path |

Catalog helpers: `lakehouse.iceberg_catalog.prepare_bronze_catalog()`.
