"""Shared pytest fixtures (no TLC network downloads)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakehouse.config import LakehouseConfig


def write_minimal_yellow_parquet(path: Path, *, row_count: int = 2) -> Path:
    """Write a tiny TLC-shaped yellow parquet (bronze ingest reads this)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pickup = datetime(2024, 1, 15, 10, 0, 0)
    dropoff = datetime(2024, 1, 15, 10, 20, 0)

    def repeat(value, count: int):
        return [value] * count

    table = pa.table(
        {
            "VendorID": pa.array(repeat(1, row_count), type=pa.int64()),
            "tpep_pickup_datetime": pa.array(repeat(pickup, row_count), type=pa.timestamp("us")),
            "tpep_dropoff_datetime": pa.array(repeat(dropoff, row_count), type=pa.timestamp("us")),
            "passenger_count": pa.array(repeat(1, row_count), type=pa.int64()),
            "trip_distance": pa.array(repeat(2.5, row_count), type=pa.float64()),
            "RatecodeID": pa.array(repeat(1, row_count), type=pa.int64()),
            "store_and_fwd_flag": pa.array(repeat("N", row_count), type=pa.string()),
            "PULocationID": pa.array(repeat(161, row_count), type=pa.int64()),
            "DOLocationID": pa.array(repeat(162, row_count), type=pa.int64()),
            "payment_type": pa.array(repeat(1, row_count), type=pa.int64()),
            "fare_amount": pa.array(repeat(12.0, row_count), type=pa.float64()),
            "extra": pa.array(repeat(0.0, row_count), type=pa.float64()),
            "mta_tax": pa.array(repeat(0.5, row_count), type=pa.float64()),
            "tip_amount": pa.array(repeat(2.0, row_count), type=pa.float64()),
            "tolls_amount": pa.array(repeat(0.0, row_count), type=pa.float64()),
            "improvement_surcharge": pa.array(repeat(0.0, row_count), type=pa.float64()),
            "total_amount": pa.array(repeat(14.5, row_count), type=pa.float64()),
            "congestion_surcharge": pa.array(repeat(0.0, row_count), type=pa.float64()),
            "Airport_fee": pa.array(repeat(0.0, row_count), type=pa.float64()),
        }
    )
    pq.write_table(table, path)
    return path


@pytest.fixture
def lake_cfg(tmp_path, monkeypatch) -> LakehouseConfig:
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    return LakehouseConfig.from_profile("local")
