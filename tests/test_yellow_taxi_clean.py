from datetime import datetime

import polars as pl

from transforms.schemas.yellow_taxi_silver import validate_yellow_taxi_silver
from transforms.yellow_taxi_clean import clean_yellow_taxi_bronze, project_silver_frame


def _sample_bronze_frame() -> pl.DataFrame:
    pickup = datetime(2024, 1, 15, 10, 0, 0)
    dropoff = datetime(2024, 1, 15, 10, 20, 0)
    bad_dropoff = datetime(2024, 1, 15, 9, 0, 0)
    return pl.DataFrame(
        {
            "VendorID": [1, 1, 2],
            "tpep_pickup_datetime": [pickup, pickup, pickup],
            "tpep_dropoff_datetime": [dropoff, bad_dropoff, dropoff],
            "passenger_count": [1, 1, -1],
            "trip_distance": [2.5, 3.0, 1.0],
            "RatecodeID": [1, 1, 1],
            "store_and_fwd_flag": ["N", "N", "N"],
            "PULocationID": [161, 999, 162],
            "DOLocationID": [162, 163, 163],
            "payment_type": [1, 1, 1],
            "fare_amount": [12.0, 11.0, 9.0],
            "extra": [0.0, 0.0, 0.0],
            "mta_tax": [0.5, 0.5, 0.5],
            "tip_amount": [2.0, 2.0, 1.0],
            "tolls_amount": [0.0, 0.0, 0.0],
            "improvement_surcharge": [0.0, 0.0, 0.0],
            "total_amount": [14.5, 13.5, 10.5],
            "congestion_surcharge": [0.0, 0.0, 0.0],
            "Airport_fee": [0.0, 0.0, 0.0],
            "trip_month": ["2024-01", "2024-01", "2024-01"],
            "_loaded_at": ["t0", "t0", "t0"],
            "_source_file": ["s", "s", "s"],
        }
    )


def test_clean_yellow_taxi_bronze_rejects_invalid_rows():
    cleaned, stats = clean_yellow_taxi_bronze(_sample_bronze_frame())

    assert stats["input_rows"] == 3
    assert stats["output_rows"] == 1
    assert stats["rejected_rows"] == 2
    assert cleaned.height == 1
    assert cleaned["vendor_id"][0] == 1


def test_project_and_validate_silver_frame():
    cleaned, _ = clean_yellow_taxi_bronze(_sample_bronze_frame())
    silver = project_silver_frame(
        cleaned, processed_at="2024-01-01T00:00:00+00:00", silver_version=1
    )

    validated = validate_yellow_taxi_silver(silver)

    assert validated.height == 1
    assert validated["_silver_version"][0] == 1
    assert validated["pickup_date"][0].isoformat() == "2024-01-15"
