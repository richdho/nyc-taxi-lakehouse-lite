# NYC Taxi Lakehouse Lite

Senior-style **medallion lakehouse** learning project without Docker — built for a **16GB Windows** laptop.

**Stack:** DuckDB · Polars · Apache Iceberg (pyiceberg) · dlt · dbt-duckdb · Dagster · pandera

Full roadmap: [`docs/ROADMAP.md`](docs/ROADMAP.md) · Checklist: [`docs/PLAN.md`](docs/PLAN.md)

> The heavier Docker lab (Spark/Airflow/Trino/MinIO) lives in a separate repo: `nyc-taxi-lakehouse`.

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (`winget install astral-sh.uv`)
- Git

No Docker required.

## Quick start

```powershell
cd C:\Users\heyux\Projects\nyc-taxi-lakehouse-lite
Copy-Item .env.example .env
uv sync --extra dev
uv run pytest
.\make.ps1 dagster
```

Open **http://127.0.0.1:3000** (Dagster UI). Materialize **`lakehouse_bootstrap`**, then **`bronze_yellow_taxi`** (default month: 2024-01). Or: `.\make.ps1 ingest-yellow`.

Optional (Phase 4+):

```powershell
uv sync --extra dbt --extra dev
```

## Commands (Windows)

| Command | Action |
|---------|--------|
| `.\make.ps1 sync` | Install/update dependencies |
| `.\make.ps1 test` | pytest |
| `.\make.ps1 lint` | ruff |
| `.\make.ps1 dagster` | `dagster dev` |
| `.\make.ps1 bootstrap` | Materialize bootstrap asset via Python |

## Layout

```
lakehouse/          Shared config
orchestration/      Dagster definitions
ingestion/          Phase 1+ dlt / Iceberg bronze
transforms/         Phase 2+ silver
dbt/                Phase 4+ gold
config/profiles/    local.yaml, cloud.yaml
lake/               Local warehouse (gitignored)
docs/               ROADMAP + PLAN
```

## Open in Cursor

**File → Open Folder** → this directory. New Agent chats pick up `.cursor/rules/lakehouse-lite.mdc` and `@docs/ROADMAP.md` for full context.
