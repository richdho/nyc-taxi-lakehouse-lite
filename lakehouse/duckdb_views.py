"""Register Iceberg tables as views in the profile DuckDB file."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import duckdb
from pyiceberg.exceptions import NoSuchTableError

from ingestion.bronze_yellow import YELLOW_TAXI_TABLE
from lakehouse.config import LakehouseConfig
from lakehouse.iceberg_catalog import SILVER_NAMESPACE, prepare_silver_catalog

_SILVER_YELLOW_VIEW = "silver_yellow_taxi_trips"
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def register_iceberg_view(
    cfg: LakehouseConfig,
    *,
    view_name: str,
    metadata_path: str | Path,
    duckdb_path: Path | None = None,
) -> None:
    """Create or replace a DuckDB view over an Iceberg table metadata file."""
    if not _IDENTIFIER_RE.match(view_name):
        msg = f"invalid DuckDB view name: {view_name!r}"
        raise ValueError(msg)

    cfg.ensure_directories()
    db_path = duckdb_path or cfg.duckdb_path
    meta = Path(metadata_path).resolve().as_posix()

    con = duckdb.connect(str(db_path))
    try:
        con.execute("INSTALL iceberg; LOAD iceberg;")
        con.execute(
            f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT * FROM iceberg_scan('{meta}');
            """
        )
    finally:
        con.close()


def register_silver_yellow_duckdb(cfg: LakehouseConfig) -> dict[str, str]:
    """Point ``cfg.duckdb_path`` at ``silver.yellow_taxi_trips`` via a SQL view."""
    catalog = prepare_silver_catalog(cfg)
    try:
        table = catalog.load_table((SILVER_NAMESPACE, YELLOW_TAXI_TABLE))
    except NoSuchTableError:
        msg = "silver.yellow_taxi_trips does not exist; run the silver transform first"
        raise FileNotFoundError(msg) from None

    metadata = table.metadata_location
    if not metadata:
        msg = f"no metadata location for table at {table.location()}"
        raise FileNotFoundError(msg)

    register_iceberg_view(cfg, view_name=_SILVER_YELLOW_VIEW, metadata_path=metadata)
    return {
        "duckdb_path": str(cfg.duckdb_path),
        "view": _SILVER_YELLOW_VIEW,
        "iceberg_metadata": metadata,
    }


def silver_yellow_duckdb_registration(cfg: LakehouseConfig) -> dict[str, Any] | None:
    """Register silver yellow taxi in DuckDB, or return ``None`` if the table is missing."""
    try:
        return register_silver_yellow_duckdb(cfg)
    except FileNotFoundError:
        return None
