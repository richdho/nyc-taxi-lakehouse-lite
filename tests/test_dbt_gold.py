"""Compile and run gold dbt models against a minimal in-memory silver fixture."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import duckdb

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


def test_dbt_gold_builds_on_fixture_silver(tmp_path):
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
        insert into silver_yellow_taxi_trips values (
            1,
            '2024-01-15 08:00:00',
            '2024-01-15 08:20:00',
            1,
            3.2,
            1,
            'N',
            161,
            236,
            1,
            12.5,
            1.0,
            0.5,
            2.0,
            0.0,
            0.3,
            16.3,
            2.5,
            0.0,
            1200.0,
            date '2024-01-15',
            '2024-01',
            'yellow_tripdata_2024-01.parquet'
        );
        """
    )
    con.close()

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(DBT_DIR)
    env["LAKEHOUSE_DUCKDB_PATH"] = str(db_path)

    deps = _run_dbt("deps", env=env)
    assert deps.returncode == 0, deps.stderr

    seed = _run_dbt("seed", env=env)
    assert seed.returncode == 0, seed.stderr

    run = _run_dbt("run", env=env)
    assert run.returncode == 0, run.stderr

    test = _run_dbt("test", env=env)
    assert test.returncode == 0, test.stderr

    docs = _run_dbt("docs", "generate", env=env)
    assert docs.returncode == 0, docs.stderr
    assert (DBT_DIR / "target" / "manifest.json").is_file()
