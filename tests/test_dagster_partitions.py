"""Dagster partition wiring for yellow taxi assets."""

from orchestration.definitions import (
    YELLOW_TAXI_MONTHLY_PARTITIONS,
    bronze_yellow_taxi,
    silver_yellow_taxi,
)


def test_yellow_taxi_monthly_partition_keys_match_trip_month() -> None:
    keys = YELLOW_TAXI_MONTHLY_PARTITIONS.get_partition_keys()
    assert keys[0] == "2024-01"
    assert keys[-1] == "2025-12"
    assert all(len(k) == 7 and k[4] == "-" for k in keys)


def test_bronze_and_silver_share_partition_definition() -> None:
    assert bronze_yellow_taxi.partitions_def is YELLOW_TAXI_MONTHLY_PARTITIONS
    assert silver_yellow_taxi.partitions_def is YELLOW_TAXI_MONTHLY_PARTITIONS
