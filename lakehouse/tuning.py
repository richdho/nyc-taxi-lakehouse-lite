"""Phase 5: Iceberg file layout, compaction, and DuckDB EXPLAIN baselines.

``write.target-file-size-bytes`` is pyiceberg's uncompressed in-memory Arrow
budget, not the on-disk Parquet size. The Iceberg default (512 MiB) splits one
yellow-taxi month into two files and can leave a small tail file. This project
sets 2 GiB so a single ``trip_month`` (about 3 million rows, tens of megabytes
on disk) is one Parquet file. Compaction rewrites any month that still has
more than one data file.
"""

from __future__ import annotations

import argparse
import statistics
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

import duckdb
import pyarrow as pa
from pyiceberg.expressions import EqualTo
from pyiceberg.table import Table, TableProperties

from lakehouse.config import LakehouseConfig
from lakehouse.iceberg_catalog import load_catalog_from_config

PARTITION_COLUMN = "trip_month"
# Uncompressed Arrow bytes. A yellow month is above the 512 MiB Iceberg default
# and below 2 GiB, which is what produces one on-disk file per partition.
WRITE_TARGET_FILE_SIZE_BYTES = 2 * 1024 * 1024 * 1024
BENCHMARK_MONTH = "2024-01"

_TABLES = (
    ("bronze", "yellow_taxi_trips"),
    ("silver", "yellow_taxi_trips"),
)


@dataclass(frozen=True)
class PartitionFiles:
    """Data-file summary for one ``trip_month`` value in the current snapshot."""

    table: str
    partition_value: str
    file_count: int
    total_bytes: int
    min_bytes: int
    max_bytes: int
    median_bytes: int
    record_count: int


def ensure_write_target(
    table: Table, target_uncompressed_bytes: int = WRITE_TARGET_FILE_SIZE_BYTES
) -> None:
    """Record the Iceberg write target on the table when it is missing or different."""
    key = TableProperties.WRITE_TARGET_FILE_SIZE_BYTES
    desired = str(target_uncompressed_bytes)
    if table.properties.get(key) == desired:
        return
    with table.transaction() as tx:
        tx.set_properties(**{key: desired})
    table.refresh()


def append_batches(
    table: Table,
    batches: Iterable[pa.RecordBatch],
    *,
    target_uncompressed_bytes: int = WRITE_TARGET_FILE_SIZE_BYTES,
) -> int:
    """Append record batches, flushing once uncompressed bytes reach the write target.

    One flush becomes one ``table.append``. pyiceberg then bin-packs that Arrow
    payload at ``write.target-file-size-bytes``. With the 2 GiB project target,
    a yellow-taxi month lands as a single Parquet file.
    """
    if target_uncompressed_bytes < 1:
        msg = "target_uncompressed_bytes must be positive"
        raise ValueError(msg)

    buffered: list[pa.RecordBatch] = []
    buffered_bytes = 0
    total_rows = 0

    def flush() -> None:
        nonlocal buffered_bytes
        if not buffered:
            return
        table.append(pa.Table.from_batches(buffered))
        buffered.clear()
        buffered_bytes = 0

    for batch in batches:
        if batch.num_rows == 0:
            continue
        buffered.append(batch)
        buffered_bytes += batch.nbytes
        total_rows += batch.num_rows
        if buffered_bytes >= target_uncompressed_bytes:
            flush()
    flush()
    return total_rows


def partition_file_stats(table: Table, *, table_name: str) -> list[PartitionFiles]:
    """Group the current snapshot's data files by ``trip_month``."""
    files = table.inspect.data_files()
    if files.num_rows == 0:
        return []

    grouped: dict[str, list[tuple[int, int]]] = {}
    partitions = files.column("partition").to_pylist()
    sizes = files.column("file_size_in_bytes").to_pylist()
    counts = files.column("record_count").to_pylist()
    for partition, size, count in zip(partitions, sizes, counts, strict=True):
        value = str(partition[PARTITION_COLUMN])
        grouped.setdefault(value, []).append((int(size), int(count)))

    stats: list[PartitionFiles] = []
    for value, rows in sorted(grouped.items()):
        file_sizes = sorted(size for size, _ in rows)
        mid = len(file_sizes) // 2
        median = file_sizes[mid]
        if len(file_sizes) % 2 == 0:
            median = int(statistics.mean((file_sizes[mid - 1], file_sizes[mid])))
        stats.append(
            PartitionFiles(
                table=table_name,
                partition_value=value,
                file_count=len(file_sizes),
                total_bytes=sum(file_sizes),
                min_bytes=file_sizes[0],
                max_bytes=file_sizes[-1],
                median_bytes=median,
                record_count=sum(count for _, count in rows),
            )
        )
    return stats


def _needs_compaction(stats: PartitionFiles) -> bool:
    return stats.file_count > 1


def compact_small_partitions(
    table: Table,
    *,
    table_name: str,
) -> list[dict[str, PartitionFiles]]:
    """Rewrite each ``trip_month`` that has more than one data file.

    The write target is 2 GiB of uncompressed Arrow, so the rewrite emits one
    Parquet file for a yellow-taxi month. Row counts for that month stay the same.
    """
    ensure_write_target(table)
    before = partition_file_stats(table, table_name=table_name)
    rewritten: list[dict[str, PartitionFiles]] = []
    for stats in before:
        if not _needs_compaction(stats):
            continue
        month = stats.partition_value
        data = table.scan(row_filter=EqualTo(PARTITION_COLUMN, month)).to_arrow()
        table.overwrite(data, overwrite_filter=EqualTo(PARTITION_COLUMN, month))
        table.refresh()
        after = next(
            item
            for item in partition_file_stats(table, table_name=table_name)
            if item.partition_value == month
        )
        rewritten.append({"before": stats, "after": after, "rows": data.num_rows})
    return rewritten


def duckdb_metadata_path(metadata: str) -> str:
    """POSIX path DuckDB ``iceberg_scan`` can open from a pyiceberg metadata location."""
    if metadata.startswith("file:"):
        parsed = unquote(urlparse(metadata).path)
        if parsed.startswith("/") and len(parsed) > 2 and parsed[2] == ":":
            parsed = parsed[1:]
        return Path(parsed).resolve().as_posix()
    return Path(metadata).resolve().as_posix()


def benchmark_sql(metadata_path: str, *, month: str) -> tuple[tuple[str, str], ...]:
    """Month-pruned queries used for the DuckDB before/after plans."""
    scan = duckdb_metadata_path(metadata_path).replace("'", "''")
    source = f"iceberg_scan('{scan}')"
    month_literal = month.replace("'", "''")
    return (
        (
            "month_count",
            f"SELECT count(*) FROM {source} WHERE {PARTITION_COLUMN} = '{month_literal}'",
        ),
        (
            "daily_fare",
            f"""
            SELECT pickup_date, count(*) AS trips, avg(fare_amount) AS avg_fare
            FROM {source}
            WHERE {PARTITION_COLUMN} = '{month_literal}'
            GROUP BY 1
            """.strip(),
        ),
    )


def explain_analyze(sql: str) -> dict[str, object]:
    """Run the query, then ``EXPLAIN ANALYZE``, in a fresh in-memory DuckDB.

    DuckDB returns the plan in the ``explain_value`` column. The reported time
    is the direct ``SELECT``, so it is not mixed up with plan formatting.
    """
    con = duckdb.connect()
    try:
        con.execute("INSTALL iceberg; LOAD iceberg;")
        started = time.perf_counter()
        con.execute(sql).fetchall()
        elapsed = time.perf_counter() - started
        rows = con.execute(f"EXPLAIN ANALYZE {sql}").fetchall()
    finally:
        con.close()
    plan = "\n".join(str(row[1] if len(row) > 1 else row[0]) for row in rows)
    return {"elapsed_seconds": elapsed, "plan": plan}


def collect_layout(cfg: LakehouseConfig) -> list[PartitionFiles]:
    """File stats for bronze and silver yellow taxi in the current snapshot."""
    catalog = load_catalog_from_config(cfg)
    stats: list[PartitionFiles] = []
    for namespace, name in _TABLES:
        table = catalog.load_table((namespace, name))
        stats.extend(partition_file_stats(table, table_name=f"{namespace}.{name}"))
    return stats


def compact_lake(cfg: LakehouseConfig) -> dict[str, list[dict[str, PartitionFiles]]]:
    """Compact small-file partitions in bronze and silver."""
    catalog = load_catalog_from_config(cfg)
    rewritten: dict[str, list[dict[str, PartitionFiles]]] = {}
    for namespace, name in _TABLES:
        table = catalog.load_table((namespace, name))
        label = f"{namespace}.{name}"
        rewritten[label] = compact_small_partitions(table, table_name=label)
    return rewritten


def refresh_silver_duckdb_view(cfg: LakehouseConfig) -> str | None:
    """Point the silver DuckDB view at the current Iceberg metadata.

    Returns an error string when the DuckDB file is locked. Compaction still
    stands; re-run ``register-silver-duckdb`` after closing the other process.
    """
    from lakehouse.duckdb_views import register_silver_yellow_duckdb

    try:
        register_silver_yellow_duckdb(cfg)
    except duckdb.IOException as exc:
        return str(exc)
    return None


def explain_month(cfg: LakehouseConfig, month: str) -> dict[str, dict[str, dict[str, object]]]:
    """EXPLAIN ANALYZE the benchmark month against bronze and silver metadata."""
    catalog = load_catalog_from_config(cfg)
    results: dict[str, dict[str, dict[str, object]]] = {}
    for namespace, name in _TABLES:
        table = catalog.load_table((namespace, name))
        metadata = table.metadata_location
        if not metadata:
            msg = f"no metadata location for {namespace}.{name}"
            raise FileNotFoundError(msg)
        label = f"{namespace}.{name}"
        queries = benchmark_sql(metadata, month=month)
        if namespace == "bronze":
            queries = tuple(item for item in queries if item[0] == "month_count")
        results[label] = {query_name: explain_analyze(sql) for query_name, sql in queries}
    return results


def _format_stats(stats: Iterable[PartitionFiles]) -> str:
    lines = [
        f"{'table':<28} {'month':<8} {'files':>5} {'rows':>10} "
        f"{'bytes':>12} {'min':>10} {'median':>10} {'max':>10}"
    ]
    for item in stats:
        lines.append(
            f"{item.table:<28} {item.partition_value:<8} {item.file_count:5d} "
            f"{item.record_count:10d} {item.total_bytes:12d} {item.min_bytes:10d} "
            f"{item.median_bytes:10d} {item.max_bytes:10d}"
        )
    return "\n".join(lines)


def run_phase5(cfg: LakehouseConfig, *, month: str = BENCHMARK_MONTH, compact: bool = True) -> str:
    """Print layout, plans, and (by default) rewrite small bronze partitions."""
    sections = ["## Before", _format_stats(collect_layout(cfg)), "", "### EXPLAIN ANALYZE"]
    before_plans = explain_month(cfg, month)
    sections.append(_format_plans(before_plans))
    if compact:
        rewritten = compact_lake(cfg)
        view_error = refresh_silver_duckdb_view(cfg)
        sections.extend(["", "## Compaction"])
        if view_error:
            sections.append(f"silver DuckDB view not refreshed: {view_error}")
        for label, actions in rewritten.items():
            if not actions:
                sections.append(f"{label}: already one file per {PARTITION_COLUMN}")
                continue
            for action in actions:
                before = action["before"]
                after = action["after"]
                sections.append(
                    f"{label} {before.partition_value}: "
                    f"files {before.file_count} -> {after.file_count}, rows {action['rows']}"
                )
        sections.extend(
            ["", "## After", _format_stats(collect_layout(cfg)), "", "### EXPLAIN ANALYZE"]
        )
        sections.append(_format_plans(explain_month(cfg, month)))
    return "\n".join(sections)


def _format_plans(plans: dict[str, dict[str, dict[str, object]]]) -> str:
    chunks: list[str] = []
    for label, queries in plans.items():
        for name, result in queries.items():
            elapsed = float(result["elapsed_seconds"])
            chunks.append(f"#### {label} {name} ({elapsed:.3f}s)")
            chunks.append("```")
            chunks.append(str(result["plan"]).rstrip())
            chunks.append("```")
    return "\n".join(chunks)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Review Iceberg file layout and DuckDB plans.")
    parser.add_argument(
        "--month", default=BENCHMARK_MONTH, help="trip_month used in EXPLAIN queries"
    )
    parser.add_argument(
        "--no-compact",
        action="store_true",
        help="Report layout and plans without rewriting partitions",
    )
    args = parser.parse_args(argv)
    print(run_phase5(LakehouseConfig.from_profile(), month=args.month, compact=not args.no_compact))


if __name__ == "__main__":
    main()
