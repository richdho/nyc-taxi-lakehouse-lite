"""Silver transform: month replace on bronze → silver."""

from __future__ import annotations

from pathlib import Path

from pyiceberg.expressions import EqualTo

from ingestion.bronze_yellow import run_bronze_yellow_ingest
from lakehouse.config import LakehouseConfig
from lakehouse.iceberg_catalog import load_catalog_from_config, prepare_bronze_catalog
from tests.conftest import write_minimal_yellow_parquet
from transforms.silver_yellow import SILVER_TABLE_ID, run_silver_yellow_transform


def _silver_month_row_count(cfg: LakehouseConfig, month: str) -> int:
    catalog = load_catalog_from_config(cfg)
    prepare_bronze_catalog(cfg)
    table = catalog.load_table(SILVER_TABLE_ID)
    return table.scan(row_filter=EqualTo("trip_month", month)).to_arrow().num_rows


def _seed_bronze(
    cfg: LakehouseConfig,
    tmp_path: Path,
    monkeypatch,
    *,
    month: str,
    row_count: int,
) -> None:
    parquet = write_minimal_yellow_parquet(
        tmp_path / f"yellow_{month}.parquet",
        row_count=row_count,
    )

    def fake_download(staging_dir: Path, m: str, *, force: bool = False) -> Path:
        assert m == month
        return parquet

    monkeypatch.setattr(
        "ingestion.bronze_yellow.download_yellow_month_parquet",
        fake_download,
    )
    run_bronze_yellow_ingest(cfg, start=month, end=month)


def test_silver_rerun_same_month_does_not_double_rows(lake_cfg, tmp_path, monkeypatch):
    _seed_bronze(lake_cfg, tmp_path, monkeypatch, month="2024-01", row_count=2)

    run_silver_yellow_transform(lake_cfg, start="2024-01", end="2024-01")
    assert _silver_month_row_count(lake_cfg, "2024-01") == 2

    run_silver_yellow_transform(lake_cfg, start="2024-01", end="2024-01")
    assert _silver_month_row_count(lake_cfg, "2024-01") == 2


def test_silver_replace_follows_bronze_row_count(lake_cfg, tmp_path, monkeypatch):
    _seed_bronze(lake_cfg, tmp_path, monkeypatch, month="2024-01", row_count=2)
    run_silver_yellow_transform(lake_cfg, start="2024-01", end="2024-01")
    assert _silver_month_row_count(lake_cfg, "2024-01") == 2

    _seed_bronze(lake_cfg, tmp_path, monkeypatch, month="2024-01", row_count=4)
    run_silver_yellow_transform(lake_cfg, start="2024-01", end="2024-01")
    assert _silver_month_row_count(lake_cfg, "2024-01") == 4
