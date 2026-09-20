"""Dagster entrypoint — expanded in Phase 3 with partitioned assets."""

from dagster import AssetExecutionContext, Definitions, MaterializeResult, asset

from lakehouse.config import LakehouseConfig


@asset
def lakehouse_bootstrap(context: AssetExecutionContext) -> MaterializeResult:
    """Create local lake directories (Phase 0 health check)."""
    cfg = LakehouseConfig.from_profile()
    cfg.ensure_directories()
    context.log.info("Lake paths ready under profile=%s", cfg.profile)
    return MaterializeResult(
        metadata={
            "profile": cfg.profile,
            "warehouse_root": str(cfg.warehouse_root),
            "catalog_db": str(cfg.catalog_db),
        }
    )


defs = Definitions(assets=[lakehouse_bootstrap])
