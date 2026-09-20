.PHONY: help sync test lint fmt dagster bootstrap

help:
	@echo "Targets: sync, test, lint, fmt, dagster, bootstrap"

sync:
	uv sync --extra dev

test:
	uv run pytest

lint:
	uv run ruff check .

fmt:
	uv run ruff format .

dagster:
	mkdir -p .dagster_home
	DAGSTER_HOME=$$(pwd)/.dagster_home uv run dagster dev -m orchestration.definitions

bootstrap:
	uv run python -c "from lakehouse.config import LakehouseConfig; c=LakehouseConfig.from_profile(); c.ensure_directories(); print('lake ready', c.warehouse_root)"
