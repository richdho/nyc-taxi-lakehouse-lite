"""Dagster entrypoint — expanded in Phase 3 with partitioned assets."""

import os

from dagster import AssetExecutionContext, Definitions, MaterializeResult, asset

from ingestion.bronze_yellow import run_bronze_yellow_ingest
from lakehouse.config import LakehouseConfig
from lakehouse.iceberg_catalog import (
    BRONZE_NAMESPACE,
    SILVER_NAMESPACE,
    prepare_bronze_catalog,
    prepare_silver_catalog,
)
from transforms.silver_yellow import run_silver_yellow_transform


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


@asset(deps=[lakehouse_bootstrap])
def bronze_yellow_taxi(context: AssetExecutionContext) -> MaterializeResult:
    """Download TLC yellow taxi Parquet and land bronze Iceberg (default: 2024-01)."""
    cfg = LakehouseConfig.from_profile()
    force_download = os.getenv("TLC_FORCE_DOWNLOAD", "").lower() in {"1", "true", "yes"}
    stats = run_bronze_yellow_ingest(cfg, force_download=force_download)
    context.log.info("Bronze ingest complete: %s", stats)
    rows_by_month = stats["rows_by_month"]
    return MaterializeResult(
        metadata={
            "table": stats["table"],
            "table_location": stats["table_location"],
            "months": ", ".join(stats["months"]),
            "rows_total": sum(rows_by_month.values()),
            **{f"rows_{month}": count for month, count in rows_by_month.items()},
        }
    )


@asset(deps=[bronze_yellow_taxi])
def silver_yellow_taxi(context: AssetExecutionContext) -> MaterializeResult:
    """Clean bronze yellow taxi trips into validated silver Iceberg."""
    cfg = LakehouseConfig.from_profile()
    stats = run_silver_yellow_transform(cfg)
    context.log.info("Silver transform complete: %s", stats)
    by_month = stats["stats_by_month"]
    rows_in = sum(m.get("input_rows", 0) for m in by_month.values())
    rows_out = sum(m.get("output_rows", 0) for m in by_month.values())
    rejected = sum(m.get("rejected_rows", 0) for m in by_month.values())
    metadata: dict[str, object] = {
        "table": stats["table"],
        "table_location": stats["table_location"],
        "months": ", ".join(stats["months"]),
        "rows_in": rows_in,
        "rows_out": rows_out,
        "rows_rejected": rejected,
        **{
            f"rejected_{month}": month_stats.get("rejected_rows", 0)
            for month, month_stats in by_month.items()
        },
    }
    duckdb_info = stats.get("duckdb")
    if duckdb_info:
        metadata["duckdb_path"] = duckdb_info["duckdb_path"]
        metadata["duckdb_view"] = duckdb_info["view"]
    return MaterializeResult(metadata=metadata)


defs = Definitions(assets=[lakehouse_bootstrap, bronze_yellow_taxi, silver_yellow_taxi])
