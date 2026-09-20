"""Polars cleaning for bronze TLC yellow taxi → silver shape."""

from __future__ import annotations

from typing import Any

import polars as pl

BRONZE_RENAME: dict[str, str] = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "RatecodeID": "ratecode_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "Airport_fee": "airport_fee",
}

MAX_TRIP_DISTANCE_MILES = 500.0
MAX_TOTAL_AMOUNT = 10_000.0
MIN_TAXI_ZONE_ID = 1
MAX_TAXI_ZONE_ID = 263


def clean_yellow_taxi_bronze(df: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Rename, derive fields, and drop rows that fail silver quality rules."""
    input_rows = df.height
    if input_rows == 0:
        return df, {"input_rows": 0, "output_rows": 0, "rejected_rows": 0}

    working = df.rename({k: v for k, v in BRONZE_RENAME.items() if k in df.columns})

    working = working.with_columns(
        (pl.col("dropoff_datetime") - pl.col("pickup_datetime"))
        .dt.total_seconds()
        .cast(pl.Float64)
        .alias("trip_duration_seconds"),
        pl.col("pickup_datetime").dt.date().alias("pickup_date"),
    )

    zone_ok = (
        pl.col("pu_location_id").is_null()
        | pl.col("pu_location_id").is_between(MIN_TAXI_ZONE_ID, MAX_TAXI_ZONE_ID)
    ) & (
        pl.col("do_location_id").is_null()
        | pl.col("do_location_id").is_between(MIN_TAXI_ZONE_ID, MAX_TAXI_ZONE_ID)
    )

    cleaned = working.filter(
        pl.col("pickup_datetime").is_not_null()
        & pl.col("dropoff_datetime").is_not_null()
        & (pl.col("dropoff_datetime") >= pl.col("pickup_datetime"))
        & pl.col("passenger_count").is_not_null()
        & (pl.col("passenger_count") >= 0)
        & pl.col("trip_distance").is_not_null()
        & (pl.col("trip_distance") >= 0)
        & (pl.col("trip_distance") <= MAX_TRIP_DISTANCE_MILES)
        & pl.col("total_amount").is_not_null()
        & (pl.col("total_amount") >= 0)
        & (pl.col("total_amount") <= MAX_TOTAL_AMOUNT)
        & zone_ok
    )

    output_rows = cleaned.height
    return cleaned, {
        "input_rows": input_rows,
        "output_rows": output_rows,
        "rejected_rows": input_rows - output_rows,
    }


def silver_output_columns() -> list[str]:
    """Column order written to silver Iceberg (excluding evolution-only fields)."""
    return [
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "ratecode_id",
        "store_and_fwd_flag",
        "pu_location_id",
        "do_location_id",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
        "trip_duration_seconds",
        "pickup_date",
        "trip_month",
        "_bronze_loaded_at",
        "_source_file",
        "_silver_processed_at",
        "_silver_version",
    ]


def project_silver_frame(
    df: pl.DataFrame, *, processed_at: str, silver_version: int
) -> pl.DataFrame:
    """Select silver columns and map bronze lineage fields."""
    projected = df.with_columns(
        pl.col("_loaded_at").alias("_bronze_loaded_at"),
        pl.lit(processed_at).alias("_silver_processed_at"),
        pl.lit(silver_version).cast(pl.Int32).alias("_silver_version"),
    )
    columns = [c for c in silver_output_columns() if c in projected.columns]
    return projected.select(columns)
