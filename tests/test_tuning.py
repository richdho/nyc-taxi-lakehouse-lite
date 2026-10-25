"""File-layout helpers for Phase 5. No TLC download."""

import pyarrow as pa

from lakehouse.config import LakehouseConfig
from lakehouse.iceberg_catalog import prepare_bronze_catalog
from lakehouse.tuning import (
    append_batches,
    benchmark_sql,
    compact_small_partitions,
    duckdb_metadata_path,
    partition_file_stats,
)


class _RecordingTable:
    def __init__(self) -> None:
        self.calls: list[int] = []

    def append(self, frame: pa.Table) -> None:
        self.calls.append(frame.num_rows)


def test_append_batches_flushes_once_under_target() -> None:
    table = _RecordingTable()
    batches = [pa.record_batch({"n": pa.array([1, 2, 3])}) for _ in range(4)]

    rows = append_batches(table, batches, target_uncompressed_bytes=10**12)

    assert rows == 12
    assert table.calls == [12]


def test_append_batches_flushes_each_batch_when_target_is_tiny() -> None:
    table = _RecordingTable()
    batches = [pa.record_batch({"n": pa.array([1, 2, 3])}) for _ in range(3)]

    append_batches(table, batches, target_uncompressed_bytes=1)

    assert table.calls == [3, 3, 3]


def test_benchmark_sql_filters_trip_month() -> None:
    queries = dict(benchmark_sql("C:/lake/metadata.json", month="2024-01"))

    assert "iceberg_scan(" in queries["month_count"]
    assert "trip_month = '2024-01'" in queries["month_count"]
    assert "fare_amount" in queries["daily_fare"]


def test_duckdb_metadata_path_strips_file_uri(tmp_path) -> None:
    meta = tmp_path / "00001.metadata.json"
    meta.write_text("{}", encoding="utf-8")

    posix = duckdb_metadata_path(meta.resolve().as_uri())

    assert posix == meta.resolve().as_posix()


def test_compact_small_partitions_merges_one_month(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    cfg = LakehouseConfig.from_profile("local")
    catalog = prepare_bronze_catalog(cfg)
    sample = pa.table(
        {"trip_month": pa.array(["2024-01"], type=pa.string()), "n": pa.array([1], type=pa.int64())}
    )
    location = cfg.bronze_path / "tiny_trips"
    location.mkdir(parents=True)
    identifier = ("bronze", "tiny_trips")
    with catalog.create_table_transaction(
        identifier=identifier,
        schema=sample.schema,
        location=str(location),
    ) as txn:
        with txn.update_spec() as spec:
            spec.add_identity("trip_month")

    table = catalog.load_table(identifier)
    for i in range(4):
        table.append(
            pa.table(
                {
                    "trip_month": pa.array(["2024-01"] * 3, type=pa.string()),
                    "n": pa.array([i, i, i], type=pa.int64()),
                }
            )
        )

    before = partition_file_stats(table, table_name="bronze.tiny_trips")
    assert before[0].file_count == 4
    assert before[0].record_count == 12

    actions = compact_small_partitions(table, table_name="bronze.tiny_trips")

    assert len(actions) == 1
    after = partition_file_stats(table, table_name="bronze.tiny_trips")
    assert after[0].file_count == 1
    assert after[0].record_count == 12
    assert table.properties["write.target-file-size-bytes"] == str(2 * 1024 * 1024 * 1024)


def test_compact_skips_a_single_file_partition(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    cfg = LakehouseConfig.from_profile("local")
    catalog = prepare_bronze_catalog(cfg)
    sample = pa.table(
        {"trip_month": pa.array(["2024-02"], type=pa.string()), "n": pa.array([1], type=pa.int64())}
    )
    location = cfg.bronze_path / "wide_trips"
    location.mkdir(parents=True)
    identifier = ("bronze", "wide_trips")
    with catalog.create_table_transaction(
        identifier=identifier,
        schema=sample.schema,
        location=str(location),
    ) as txn:
        with txn.update_spec() as spec:
            spec.add_identity("trip_month")

    table = catalog.load_table(identifier)
    table.append(sample)

    actions = compact_small_partitions(table, table_name="bronze.wide_trips")

    assert actions == []
    assert partition_file_stats(table, table_name="bronze.wide_trips")[0].file_count == 1
