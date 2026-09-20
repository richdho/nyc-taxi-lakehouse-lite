"""Pandera contract for ``silver.yellow_taxi_trips``."""

from __future__ import annotations

import pandera.polars as pa
import polars as pl
from pandera.polars import DataFrameModel


class YellowTaxiSilverSchema(DataFrameModel):
    vendor_id: pl.Int64
    pickup_datetime: pl.Datetime(time_unit="us")
    dropoff_datetime: pl.Datetime(time_unit="us")
    passenger_count: pl.Int64 = pa.Field(ge=0)
    trip_distance: pl.Float64 = pa.Field(ge=0.0, le=500.0)
    ratecode_id: pl.Int64 = pa.Field(nullable=True)
    store_and_fwd_flag: pl.String = pa.Field(nullable=True)
    pu_location_id: pl.Int64 = pa.Field(nullable=True, ge=1, le=263)
    do_location_id: pl.Int64 = pa.Field(nullable=True, ge=1, le=263)
    payment_type: pl.Int64 = pa.Field(nullable=True)
    fare_amount: pl.Float64 = pa.Field(nullable=True)
    extra: pl.Float64 = pa.Field(nullable=True)
    mta_tax: pl.Float64 = pa.Field(nullable=True)
    tip_amount: pl.Float64 = pa.Field(nullable=True)
    tolls_amount: pl.Float64 = pa.Field(nullable=True)
    improvement_surcharge: pl.Float64 = pa.Field(nullable=True)
    total_amount: pl.Float64 = pa.Field(ge=0.0, le=10_000.0)
    congestion_surcharge: pl.Float64 = pa.Field(nullable=True)
    airport_fee: pl.Float64 = pa.Field(nullable=True)
    trip_duration_seconds: pl.Float64 = pa.Field(ge=0.0)
    pickup_date: pl.Date
    trip_month: pl.String
    bronze_loaded_at: pl.String = pa.Field(alias="_bronze_loaded_at")
    source_file: pl.String = pa.Field(alias="_source_file")
    silver_processed_at: pl.String = pa.Field(alias="_silver_processed_at")
    silver_version: pl.Int32 = pa.Field(alias="_silver_version", ge=1)

    class Config:
        strict = True
        coerce = True


def validate_yellow_taxi_silver(df: pl.DataFrame) -> pl.DataFrame:
    """Validate cleaned silver rows; raises ``SchemaError`` on failure."""
    return YellowTaxiSilverSchema.validate(df)
