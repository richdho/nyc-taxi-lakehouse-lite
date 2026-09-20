# Transforms (Phase 2)

**Polars** cleans bronze TLC yellow taxi; **pandera** validates the contract; **pyiceberg** writes **`silver.yellow_taxi_trips`**.

## Run

Same month range as bronze (`TLC_YELLOW_START` / `TLC_YELLOW_END` in `.env`):

```powershell
.\make.ps1 silver-yellow
```

Or in Dagster: materialize **`silver_yellow_taxi`** (depends on **`bronze_yellow_taxi`**).

## Idempotent re-runs

Re-running a month **deletes that `trip_month` partition** in silver, then appends freshly cleaned rows.

## Quality rules (summary)

- Valid pickup/dropoff timestamps; dropoff ≥ pickup
- Non-negative `passenger_count`, `trip_distance`, `total_amount` (with upper caps)
- Taxi zone IDs in 1–263 when present
- Derived: `trip_duration_seconds`, `pickup_date`

## Schema evolution

New silver tables are created without `pickup_date` and `_silver_version`; `evolve_silver_yellow_table()` adds them before the first append (see `transforms/silver_yellow.py`).

## Layout

- `yellow_taxi_clean.py` — Polars rename, filter, derive
- `schemas/yellow_taxi_silver.py` — pandera `YellowTaxiSilverSchema`
- `silver_yellow.py` — bronze scan → silver append
