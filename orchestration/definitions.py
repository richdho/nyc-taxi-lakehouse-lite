"""Dagster entrypoint — monthly partitioned bronze/silver yellow taxi assets."""

import os

from dagster import (
    AssetExecutionContext,
    Definitions,
    MaterializeResult,
    MonthlyPartitionsDefinition,
    asset,
)

from ingestion.bronze_yellow import run_bronze_yellow_ingest
from ingestion.tlc_urls import DEFAULT_YELLOW_START
from lakehouse.config import LakehouseConfig
from lakehouse.iceberg_catalog import (
    BRONZE_NAMESPACE,
    SILVER_NAMESPACE,
    prepare_bronze_catalog,
    prepare_silver_catalog,
)
from transforms.silver_yellow import run_silver_yellow_transform

# Partition keys match Iceberg ``trip_month`` (``YYYY-MM``). ``end_date`` is exclusive.
YELLOW_TAXI_MONTHLY_PARTITIONS = MonthlyPartitionsDefinition(
    start_date=DEFAULT_YELLOW_START,
    end_date="2026-01",
    fmt="%Y-%m",
)


@asset
def lakehouse_bootstrap(context: AssetExecutionContext) -> MaterializeResult:
    """Create local lake directories and Iceberg catalog namespace (Phase 0–1)."""
    cfg = LakehouseConfig.from_profile()
    prepare_bronze_catalog(cfg)
    prepare_silver_catalog(cfg)
    context.log.info(
        "Lake paths and Iceberg namespaces %r, %r ready (profile=%s)",
        BRONZE_NAMESPACE,
        SILVER_NAMESPACE,
        cfg.profile,
    )
    return MaterializeResult(
        metadata={
            "profile": cfg.profile,
            "warehouse_root": str(cfg.warehouse_root),
            "catalog_db": str(cfg.catalog_db),
            "bronze_namespace": BRONZE_NAMESPACE,
            "silver_namespace": SILVER_NAMESPACE,
        }
    )


@asset(deps=[lakehouse_bootstrap], partitions_def=YELLOW_TAXI_MONTHLY_PARTITIONS)
def bronze_yellow_taxi(context: AssetExecutionContext) -> MaterializeResult:
    """Download TLC yellow taxi Parquet for one month and land bronze Iceberg."""
    trip_month = context.partition_key
    cfg = LakehouseConfig.from_profile()
    force_download = os.getenv("TLC_FORCE_DOWNLOAD", "").lower() in {"1", "true", "yes"}
    stats = run_bronze_yellow_ingest(
        cfg,
        start=trip_month,
        end=trip_month,
        force_download=force_download,
    )
    context.log.info("Bronze ingest complete for %s: %s", trip_month, stats)
    rows_by_month = stats["rows_by_month"]
    return MaterializeResult(
        metadata={
            "trip_month": trip_month,
            "table": stats["table"],
            "table_location": stats["table_location"],
            "rows": rows_by_month.get(trip_month, 0),
        }
    )


@asset(deps=[bronze_yellow_taxi], partitions_def=YELLOW_TAXI_MONTHLY_PARTITIONS)
def silver_yellow_taxi(context: AssetExecutionContext) -> MaterializeResult:
    """Clean bronze yellow taxi trips for one month into validated silver Iceberg."""
    trip_month = context.partition_key
    cfg = LakehouseConfig.from_profile()
    stats = run_silver_yellow_transform(cfg, start=trip_month, end=trip_month)
    context.log.info("Silver transform complete for %s: %s", trip_month, stats)
    month_stats = stats["stats_by_month"].get(trip_month, {})
    metadata: dict[str, object] = {
        "trip_month": trip_month,
        "table": stats["table"],
        "table_location": stats["table_location"],
        "rows_in": month_stats.get("input_rows", 0),
        "rows_out": month_stats.get("output_rows", 0),
        "rows_rejected": month_stats.get("rejected_rows", 0),
    }
    duckdb_info = stats.get("duckdb")
    if duckdb_info:
        metadata["duckdb_path"] = duckdb_info["duckdb_path"]
        metadata["duckdb_view"] = duckdb_info["view"]
    return MaterializeResult(metadata=metadata)


defs = Definitions(assets=[lakehouse_bootstrap, bronze_yellow_taxi, silver_yellow_taxi])
