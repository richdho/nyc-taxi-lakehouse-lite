"""Land TLC yellow taxi Parquet into bronze Iceberg ``yellow_taxi_trips``."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo
from pyiceberg.table import Table

from ingestion.tlc_urls import iter_months_from_env, yellow_taxi_parquet_url
from ingestion.tlc_yellow_dlt import (
    download_yellow_month_parquet,
    iter_yellow_month_arrow_batches,
)
from lakehouse.config import LakehouseConfig
from lakehouse.iceberg_catalog import BRONZE_NAMESPACE, prepare_bronze_catalog

YELLOW_TAXI_TABLE = "yellow_taxi_trips"
TABLE_IDENTIFIER = (BRONZE_NAMESPACE, YELLOW_TAXI_TABLE)


def run_bronze_yellow_ingest(
    cfg: LakehouseConfig,
    *,
    start: str | None = None,
    end: str | None = None,
    force_download: bool = False,
) -> dict[str, Any]:
    """Download TLC months and append to bronze Iceberg (idempotent per month)."""
    months = iter_months_from_env(start, end)
    catalog = prepare_bronze_catalog(cfg)
    table_location = cfg.bronze_path / YELLOW_TAXI_TABLE
    staging_dir = cfg.bronze_path.parent / "staging" / "tlc" / "yellow"

    month_stats: dict[str, int] = {}
    for month in months:
        source_url = yellow_taxi_parquet_url(month)
        parquet_path = download_yellow_month_parquet(
            staging_dir,
            month,
            force=force_download,
        )
        table = _ensure_yellow_table(catalog, table_location, parquet_path, month, source_url)
        rows = _replace_month_batches(
            table,
            month=month,
            source_url=source_url,
            parquet_path=parquet_path,
        )
        month_stats[month] = rows

    return {
        "months": months,
        "rows_by_month": month_stats,
        "table": ".".join(TABLE_IDENTIFIER),
        "table_location": str(table_location),
    }


def _ensure_yellow_table(
    catalog: Catalog,
    table_location: Path,
    sample_parquet: Path,
    month: str,
    source_url: str,
) -> Table:
    try:
        return catalog.load_table(TABLE_IDENTIFIER)
    except NoSuchTableError:
        pass

    sample_batch = next(iter_yellow_month_arrow_batches(sample_parquet, batch_size=10_000))
    sample_table = _enrich_batch(
        pa.Table.from_batches([sample_batch]),
        month=month,
        source_url=source_url,
    )
    table_location.mkdir(parents=True, exist_ok=True)

    with catalog.create_table_transaction(
        identifier=TABLE_IDENTIFIER,
        schema=sample_table.schema,
        location=str(table_location),
    ) as txn:
        with txn.update_spec() as spec:
            spec.add_identity("trip_month")

    return catalog.load_table(TABLE_IDENTIFIER)


def _replace_month_batches(
    table: Table,
    *,
    month: str,
    source_url: str,
    parquet_path: Path,
) -> int:
    table.delete(delete_filter=EqualTo("trip_month", month))

    total_rows = 0
    for batch in iter_yellow_month_arrow_batches(parquet_path):
        arrow_table = _enrich_batch(
            pa.Table.from_batches([batch]),
            month=month,
            source_url=source_url,
        )
        if arrow_table.num_rows == 0:
            continue
        table.append(arrow_table)
        total_rows += arrow_table.num_rows
    return total_rows


def _enrich_batch(table: pa.Table, *, month: str, source_url: str) -> pa.Table:
    loaded_at = datetime.now(tz=UTC).isoformat()
    row_count = table.num_rows
    table = table.append_column("trip_month", pa.array([month] * row_count, type=pa.string()))
    table = table.append_column("_loaded_at", pa.array([loaded_at] * row_count, type=pa.string()))
    source_col = pa.array([source_url] * row_count, type=pa.string())
    table = table.append_column("_source_file", source_col)
    return table
