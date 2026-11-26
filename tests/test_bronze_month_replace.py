"""Bronze ingest: Iceberg month replace (delete partition, then append)."""

from __future__ import annotations

from pathlib import Path

from pyiceberg.expressions import EqualTo

from ingestion.bronze_yellow import TABLE_IDENTIFIER, run_bronze_yellow_ingest
from lakehouse.config import LakehouseConfig
from lakehouse.iceberg_catalog import load_catalog_from_config, prepare_bronze_catalog
from tests.conftest import write_minimal_yellow_parquet


def _month_row_count(cfg: LakehouseConfig, month: str) -> int:
    catalog = load_catalog_from_config(cfg)
    table = catalog.load_table(TABLE_IDENTIFIER)
    return table.scan(row_filter=EqualTo("trip_month", month)).to_arrow().num_rows


def _patch_download(monkeypatch, paths_by_month: dict[str, Path]) -> None:
    def fake_download(staging_dir: Path, month: str, *, force: bool = False) -> Path:
        return paths_by_month[month]

    monkeypatch.setattr(
        "ingestion.bronze_yellow.download_yellow_month_parquet",
        fake_download,
    )


def test_bronze_rerun_same_month_does_not_double_rows(lake_cfg, tmp_path, monkeypatch):
    parquet = write_minimal_yellow_parquet(tmp_path / "yellow_tripdata_2024-01.parquet", row_count=2)
    _patch_download(monkeypatch, {"2024-01": parquet})

    run_bronze_yellow_ingest(lake_cfg, start="2024-01", end="2024-01")
    assert _month_row_count(lake_cfg, "2024-01") == 2

    run_bronze_yellow_ingest(lake_cfg, start="2024-01", end="2024-01")
    assert _month_row_count(lake_cfg, "2024-01") == 2


def test_bronze_rerun_replaces_row_count_for_month(lake_cfg, tmp_path, monkeypatch):
    path_v1 = write_minimal_yellow_parquet(tmp_path / "v1.parquet", row_count=2)
    path_v2 = write_minimal_yellow_parquet(tmp_path / "v2.parquet", row_count=5)
    current = {"path": path_v1}

    def fake_download(staging_dir: Path, month: str, *, force: bool = False) -> Path:
        assert month == "2024-01"
        return current["path"]

    monkeypatch.setattr(
        "ingestion.bronze_yellow.download_yellow_month_parquet",
        fake_download,
    )

    run_bronze_yellow_ingest(lake_cfg, start="2024-01", end="2024-01")
    assert _month_row_count(lake_cfg, "2024-01") == 2

    current["path"] = path_v2
    run_bronze_yellow_ingest(lake_cfg, start="2024-01", end="2024-01")
    assert _month_row_count(lake_cfg, "2024-01") == 5


def test_bronze_two_months_are_independent(lake_cfg, tmp_path, monkeypatch):
    paths = {
        "2024-01": write_minimal_yellow_parquet(tmp_path / "2024-01.parquet", row_count=2),
        "2024-02": write_minimal_yellow_parquet(tmp_path / "2024-02.parquet", row_count=3),
    }
    _patch_download(monkeypatch, paths)

    run_bronze_yellow_ingest(lake_cfg, start="2024-01", end="2024-02")

    assert _month_row_count(lake_cfg, "2024-01") == 2
    assert _month_row_count(lake_cfg, "2024-02") == 3


def test_bronze_creates_table_and_namespace(lake_cfg, tmp_path, monkeypatch):
    parquet = write_minimal_yellow_parquet(tmp_path / "yellow.parquet", row_count=1)
    _patch_download(monkeypatch, {"2024-01": parquet})

    result = run_bronze_yellow_ingest(lake_cfg, start="2024-01", end="2024-01")

    assert result["table"] == "bronze.yellow_taxi_trips"
    assert result["rows_by_month"]["2024-01"] == 1
    prepare_bronze_catalog(lake_cfg)
    catalog = load_catalog_from_config(lake_cfg)
    assert catalog.load_table(TABLE_IDENTIFIER) is not None
