"""Bronze → silver transform for TLC yellow taxi trips."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo
from pyiceberg.table import Table
from pyiceberg.types import DateType, IntegerType

from ingestion.bronze_yellow import TABLE_IDENTIFIER as BRONZE_TABLE_ID
from ingestion.bronze_yellow import YELLOW_TAXI_TABLE
from ingestion.tlc_urls import iter_months_from_env
from lakehouse.config import LakehouseConfig
from lakehouse.duckdb_views import register_silver_yellow_duckdb
from lakehouse.iceberg_catalog import (
    SILVER_NAMESPACE,
    prepare_bronze_catalog,
    prepare_silver_catalog,
)
from transforms.schemas.yellow_taxi_silver import validate_yellow_taxi_silver
from transforms.yellow_taxi_clean import clean_yellow_taxi_bronze, project_silver_frame

SILVER_TABLE_ID = (SILVER_NAMESPACE, YELLOW_TAXI_TABLE)
SILVER_SCHEMA_VERSION = 1
EVOLUTION_COLUMNS = ("pickup_date", "_silver_version")


def run_silver_yellow_transform(
    cfg: LakehouseConfig,
    *,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    """Read bronze months, clean + validate, write silver Iceberg (idempotent per month)."""
    months = iter_months_from_env(start, end)
    prepare_bronze_catalog(cfg)
    catalog = prepare_silver_catalog(cfg)
    table_location = cfg.silver_path / YELLOW_TAXI_TABLE

    month_stats: dict[str, dict[str, Any]] = {}
    for month in months:
        bronze_arrow = _read_bronze_month(catalog, month)
        stats = _replace_silver_month(
            catalog, table_location, month=month, bronze_arrow=bronze_arrow
        )
        month_stats[month] = stats

    result: dict[str, Any] = {
        "months": months,
        "stats_by_month": month_stats,
        "table": ".".join(SILVER_TABLE_ID),
        "table_location": str(table_location),
    }
    try:
        result["duckdb"] = register_silver_yellow_duckdb(cfg)
    except FileNotFoundError:
        pass
    return result


def _read_bronze_month(catalog: Catalog, month: str) -> pa.Table:
    bronze = catalog.load_table(BRONZE_TABLE_ID)
    return bronze.scan(row_filter=EqualTo("trip_month", month)).to_arrow()


def _clear_silver_month(catalog: Catalog, month: str) -> None:
    try:
        table = catalog.load_table(SILVER_TABLE_ID)
    except NoSuchTableError:
        return
    table.delete(delete_filter=EqualTo("trip_month", month))


def _replace_silver_month(
    catalog: Catalog,
    table_location: Path,
    *,
    month: str,
    bronze_arrow: pa.Table,
) -> dict[str, Any]:
    if bronze_arrow.num_rows == 0:
        _clear_silver_month(catalog, month)
        return {"input_rows": 0, "output_rows": 0, "rejected_rows": 0}

    processed_at = datetime.now(tz=UTC).isoformat()
    bronze_df = pl.from_arrow(bronze_arrow)
    cleaned_df, clean_stats = clean_yellow_taxi_bronze(bronze_df)
    if cleaned_df.height == 0:
        return clean_stats

    silver_df = project_silver_frame(
        cleaned_df,
        processed_at=processed_at,
        silver_version=SILVER_SCHEMA_VERSION,
    )
    silver_df = validate_yellow_taxi_silver(silver_df)
    silver_arrow = silver_df.to_arrow()

    table = _ensure_silver_table(catalog, table_location, silver_arrow)
    evolve_silver_yellow_table(table)
    table = catalog.load_table(SILVER_TABLE_ID)

    table.delete(delete_filter=EqualTo("trip_month", month))
    table.append(_align_arrow_to_iceberg(table, silver_arrow))

    return clean_stats


def _ensure_silver_table(
    catalog: Catalog,
    table_location: Path,
    sample_arrow: pa.Table,
) -> Table:
    try:
        return catalog.load_table(SILVER_TABLE_ID)
    except NoSuchTableError:
        pass

    create_arrow = _arrow_without_columns(sample_arrow, EVOLUTION_COLUMNS)
    table_location.mkdir(parents=True, exist_ok=True)

    with catalog.create_table_transaction(
        identifier=SILVER_TABLE_ID,
        schema=create_arrow.schema,
        location=str(table_location),
    ) as txn:
        with txn.update_spec() as spec:
            spec.add_identity("trip_month")

    return catalog.load_table(SILVER_TABLE_ID)


def evolve_silver_yellow_table(table: Table) -> bool:
    """Add ``pickup_date`` and ``_silver_version`` if the table predates them."""
    field_names = {field.name for field in table.schema().fields}
    to_add: list[tuple[str, DateType | IntegerType]] = []
    if "pickup_date" not in field_names:
        to_add.append(("pickup_date", DateType()))
    if "_silver_version" not in field_names:
        to_add.append(("_silver_version", IntegerType()))

    if not to_add:
        return False

    with table.update_schema() as update:
        for name, iceberg_type in to_add:
            update.add_column(name, iceberg_type)
    return True


def _arrow_without_columns(table: pa.Table, columns: tuple[str, ...]) -> pa.Table:
    drop = set(columns)
    names = [name for name in table.column_names if name not in drop]
    return table.select(names)


def _align_arrow_to_iceberg(table: Table, arrow: pa.Table) -> pa.Table:
    """Cast columns and order fields to match the Iceberg table schema."""
    target = table.schema().as_arrow()
    columns = []
    for field in target:
        column = arrow.column(field.name)
        if column.type != field.type:
            column = column.cast(field.type)
        columns.append(column)
    return pa.table(columns, schema=target)
