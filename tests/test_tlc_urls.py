import pytest

from ingestion.tlc_urls import iter_months, yellow_taxi_parquet_url


def test_yellow_taxi_parquet_url():
    url = yellow_taxi_parquet_url("2024-01")
    assert url.endswith("yellow_tripdata_2024-01.parquet")
    assert "cloudfront.net" in url


def test_iter_months_single():
    assert iter_months("2024-01", "2024-01") == ["2024-01"]


def test_iter_months_range():
    assert iter_months("2024-01", "2024-03") == ["2024-01", "2024-02", "2024-03"]


def test_iter_months_invalid_range():
    with pytest.raises(ValueError, match="after end"):
        iter_months("2024-06", "2024-01")
