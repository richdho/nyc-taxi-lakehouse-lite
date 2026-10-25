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

Open **http://127.0.0.1:3000** (Dagster UI). Materialize **`lakehouse_bootstrap`**, then **`bronze_yellow_taxi`**, then **`silver_yellow_taxi`** (default month: 2024-01). Or: `.\make.ps1 ingest-yellow` then `.\make.ps1 silver-yellow`.

Gold marts (needs the dbt extra):

```powershell
uv sync --extra dbt --extra dev
.\make.ps1 dbt-gold
```

Dashboard on gold marts (Phase 6, Streamlit):

```powershell
uv sync --extra serving --extra dbt --extra dev
.\make.ps1 dashboard
```

Open **http://localhost:8501** after gold is built.

## Commands

Use `make <target>` on Linux/macOS/cloud, or `.\make.ps1 <target>` on Windows.

| Command | Action |
|---------|--------|
| `sync` | Install/update dependencies |
| `test` | pytest |
| `lint` | ruff |
| `dagster` | `dagster dev` |
| `bootstrap` | Lake dirs + Iceberg bronze catalog |
| `ingest-yellow` | TLC download → bronze Iceberg |
| `silver-yellow` | Bronze → silver transform |
| `register-silver-duckdb` | Refresh DuckDB view over silver Iceberg |
| `dbt-gold` | Seed zones, build gold marts, test, generate dbt docs |
| `tune` | Rewrite multi-file `trip_month` partitions and print DuckDB EXPLAIN |
| `dashboard` | Streamlit UI on dbt gold (`main_gold.fact_trips`, dims; read-only DuckDB) |

## Layout

```
lakehouse/          Shared config
orchestration/      Dagster definitions
ingestion/          TLC download and Iceberg bronze
transforms/         Silver cleaning and pandera
dbt/                Gold star schema
serving/            Streamlit dashboard (Phase 6)
config/profiles/    local.yaml, cloud.yaml
lake/               Local warehouse (gitignored)
docs/               ROADMAP + PLAN
```

## Open in Cursor

**File → Open Folder** → this directory. New Agent chats pick up `.cursor/rules/lakehouse-lite.mdc` and `@docs/ROADMAP.md` for full context.
