"""Dagster entrypoint — expanded in Phase 3 with partitioned assets."""

import os

from dagster import AssetExecutionContext, Definitions, MaterializeResult, asset

from ingestion.bronze_yellow import run_bronze_yellow_ingest
from lakehouse.config import LakehouseConfig
from lakehouse.iceberg_catalog import BRONZE_NAMESPACE, prepare_bronze_catalog


@asset
def lakehouse_bootstrap(context: AssetExecutionContext) -> MaterializeResult:
    """Create local lake directories and Iceberg catalog namespace (Phase 0–1)."""
    cfg = LakehouseConfig.from_profile()
    prepare_bronze_catalog(cfg)
    context.log.info(
        "Lake paths and Iceberg namespace %r ready (profile=%s)",
        BRONZE_NAMESPACE,
        cfg.profile,
    )
    return MaterializeResult(
        metadata={
            "profile": cfg.profile,
            "warehouse_root": str(cfg.warehouse_root),
            "catalog_db": str(cfg.catalog_db),
            "bronze_namespace": BRONZE_NAMESPACE,
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


defs = Definitions(assets=[lakehouse_bootstrap, bronze_yellow_taxi])
