"""Gold dashboard query helpers."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import duckdb

from serving.queries import (
    avg_fare_by_day_of_week,
    daily_trip_volume,
    gold_tables_present,
    list_trip_months,
    monthly_revenue,
    top_pickup_zones,
    trips_by_payment_type,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = PROJECT_ROOT / "dbt"


def _run_dbt(*args: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    cmd = ["uv", "run", "dbt", *args, "--project-dir", str(DBT_DIR)]
    return subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _fixture_db(tmp_path) -> duckdb.DuckDBPyConnection:
    db_path = tmp_path / "lakehouse.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        create table silver_yellow_taxi_trips (
            vendor_id bigint,
            pickup_datetime timestamp,
            dropoff_datetime timestamp,
            passenger_count bigint,
            trip_distance double,
            ratecode_id bigint,
            store_and_fwd_flag varchar,
            pu_location_id bigint,
            do_location_id bigint,
            payment_type bigint,
            fare_amount double,
            extra double,
            mta_tax double,
            tip_amount double,
            tolls_amount double,
            improvement_surcharge double,
            total_amount double,
            congestion_surcharge double,
            airport_fee double,
            trip_duration_seconds double,
            pickup_date date,
            trip_month varchar,
            _source_file varchar
        );
        insert into silver_yellow_taxi_trips values
        (
            1, '2024-01-15 08:00:00', '2024-01-15 08:20:00', 1, 3.2, 1, 'N',
            161, 236, 1, 12.5, 1.0, 0.5, 2.0, 0.0, 0.3, 16.3, 2.5, 0.0, 1200.0,
            date '2024-01-15', '2024-01', 'yellow_tripdata_2024-01.parquet'
        ),
        (
            1, '2024-01-16 09:00:00', '2024-01-16 09:25:00', 2, 5.0, 1, 'N',
            161, 237, 2, 18.0, 1.0, 0.5, 0.0, 0.0, 0.3, 20.0, 2.5, 0.0, 1500.0,
            date '2024-01-16', '2024-01', 'yellow_tripdata_2024-01.parquet'
        );
        """
    )
    con.close()

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(DBT_DIR)
    env["LAKEHOUSE_DUCKDB_PATH"] = str(db_path)
    assert _run_dbt("seed", env=env).returncode == 0
    assert _run_dbt("run", env=env).returncode == 0

    return duckdb.connect(str(db_path), read_only=True)


def test_gold_query_helpers(tmp_path):
    con = _fixture_db(tmp_path)
    assert gold_tables_present(con)
    assert list_trip_months(con) == ["2024-01"]

    daily = daily_trip_volume(con, trip_months=["2024-01"])
    assert daily.height == 2
    assert daily["trip_count"].sum() == 2

    revenue = monthly_revenue(con, trip_months=["2024-01"])
    assert revenue.height == 1
    assert float(revenue["total_revenue"][0]) == 36.3

    zones = top_pickup_zones(con, trip_months=["2024-01"], limit=5)
    assert zones.height >= 1

    payments = trips_by_payment_type(con, trip_months=["2024-01"])
    assert payments["trip_count"].sum() == 2

    dow = avg_fare_by_day_of_week(con, trip_months=["2024-01"])
    assert dow.height >= 1
    con.close()
