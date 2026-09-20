"""Dagster entrypoint — expanded in Phase 3 with partitioned assets."""

from dagster import AssetExecutionContext, Definitions, MaterializeResult, asset

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


defs = Definitions(assets=[lakehouse_bootstrap])
