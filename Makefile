.PHONY: help env sync test lint fmt dagster bootstrap ingest-yellow silver-yellow register-silver-duckdb

ifneq (,$(wildcard .env))
UV_RUN := uv run --env-file .env
else
UV_RUN := uv run
endif

help:
	@echo "Targets: env, sync, test, lint, fmt, dagster, bootstrap, ingest-yellow, silver-yellow, register-silver-duckdb"

env:
	@test -f .env || cp .env.example .env

sync: env
	uv sync --extra dev

test:
	$(UV_RUN) pytest

lint:
	$(UV_RUN) ruff check .

fmt:
	$(UV_RUN) ruff format .

dagster: env
	mkdir -p .dagster_home
	DAGSTER_HOME=$$(pwd)/.dagster_home $(UV_RUN) dagster dev -m orchestration.definitions

bootstrap: env
	$(UV_RUN) python -c "from lakehouse.config import LakehouseConfig; from lakehouse.iceberg_catalog import prepare_bronze_catalog; c=LakehouseConfig.from_profile(); prepare_bronze_catalog(c); print('lake ready:', c.warehouse_root)"

ingest-yellow: env
	$(UV_RUN) python -c "from lakehouse.config import LakehouseConfig; from ingestion.bronze_yellow import run_bronze_yellow_ingest; c=LakehouseConfig.from_profile(); print(run_bronze_yellow_ingest(c))"

silver-yellow: env
	$(UV_RUN) python -c "from lakehouse.config import LakehouseConfig; from transforms.silver_yellow import run_silver_yellow_transform; c=LakehouseConfig.from_profile(); print(run_silver_yellow_transform(c))"

register-silver-duckdb: env
	$(UV_RUN) python -c "from lakehouse.config import LakehouseConfig; from lakehouse.duckdb_views import register_silver_yellow_duckdb; c=LakehouseConfig.from_profile(); print(register_silver_yellow_duckdb(c))"
