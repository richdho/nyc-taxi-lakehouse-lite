"""PyIceberg SQL catalog wired to :class:`LakehouseConfig` paths."""

from __future__ import annotations

from pathlib import Path

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError

from lakehouse.config import LakehouseConfig

CATALOG_NAME = "lakehouse"
BRONZE_NAMESPACE = "bronze"
SILVER_NAMESPACE = "silver"


def sqlite_uri(db_path: Path) -> str:
    """SQLAlchemy URI for a SQLite catalog database file."""
    return f"sqlite:///{db_path.resolve().as_posix()}"


def file_warehouse_uri(warehouse_root: Path) -> str:
    """Iceberg ``warehouse`` property for local filesystem storage."""
    return warehouse_root.resolve().as_uri()


def load_catalog_from_config(
    cfg: LakehouseConfig,
    *,
    name: str = CATALOG_NAME,
) -> Catalog:
    """Open the SQLite-backed Iceberg catalog for the active profile."""
    cfg.ensure_directories()
    return load_catalog(
        name,
        **{
            "type": "sql",
            "uri": sqlite_uri(cfg.catalog_db),
            "warehouse": file_warehouse_uri(cfg.warehouse_root),
        },
    )


def ensure_namespace(catalog: Catalog, namespace: str) -> None:
    """Create a namespace if it does not already exist."""
    try:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass


def prepare_bronze_catalog(cfg: LakehouseConfig) -> Catalog:
    """Ensure lake directories, catalog DB, and the bronze namespace exist."""
    catalog = load_catalog_from_config(cfg)
    ensure_namespace(catalog, BRONZE_NAMESPACE)
    return catalog


def prepare_silver_catalog(cfg: LakehouseConfig) -> Catalog:
    """Ensure lake directories, catalog DB, and the silver namespace exist."""
    catalog = load_catalog_from_config(cfg)
    ensure_namespace(catalog, SILVER_NAMESPACE)
    return catalog
