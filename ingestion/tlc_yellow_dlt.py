"""dlt source for NYC TLC yellow taxi monthly Parquet files."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from pathlib import Path

import dlt
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from ingestion.tlc_urls import yellow_taxi_parquet_url

CHUNK_SIZE = 1024 * 1024
ARROW_BATCH_SIZE = 250_000


@dlt.source(name="tlc_yellow")
def tlc_yellow_source(months: Sequence[str]):
    """One dlt resource per TLC month (download + local staging path)."""
    for month in months:
        yield yellow_taxi_month_file(month)


@dlt.resource(name="yellow_taxi_month_file")
def yellow_taxi_month_file(month: str):
    """Yield staging metadata after downloading the TLC Parquet for ``month``."""
    path = download_yellow_month_parquet(staging_dir=_default_staging_dir(), month=month)
    yield {
        "trip_month": month,
        "local_path": str(path),
        "source_url": yellow_taxi_parquet_url(month),
    }


def download_yellow_month_parquet(staging_dir: Path, month: str, *, force: bool = False) -> Path:
    """Download yellow taxi Parquet for ``month`` into ``staging_dir``."""
    staging_dir.mkdir(parents=True, exist_ok=True)
    dest = staging_dir / f"yellow_tripdata_{month}.parquet"
    if dest.exists() and dest.stat().st_size > 0 and not force:
        return dest

    url = yellow_taxi_parquet_url(month)
    with requests.get(url, stream=True, timeout=600) as response:
        response.raise_for_status()
        with dest.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    handle.write(chunk)
    return dest


def iter_yellow_month_arrow_batches(
    parquet_path: Path,
    *,
    batch_size: int = ARROW_BATCH_SIZE,
) -> Iterator[pa.RecordBatch]:
    parquet_file = pq.ParquetFile(parquet_path)
    yield from parquet_file.iter_batches(batch_size=batch_size)


def _default_staging_dir() -> Path:
    import os

    from lakehouse.config import LakehouseConfig

    cfg = LakehouseConfig.from_profile()
    root = cfg.bronze_path.parent
    override = os.getenv("TLC_STAGING_DIR")
    if override:
        return Path(override)
    return root / "staging" / "tlc" / "yellow"
