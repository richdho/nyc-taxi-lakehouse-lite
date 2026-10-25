# Phase 5 — Tuning

Measured on the local lake: yellow taxi `2024-01` through `2024-05`. Partition grain stays **one identity partition per `trip_month`**. DuckDB `EXPLAIN ANALYZE` shows a `trip_month = '2024-01'` filter reads only that month's files.

## What was wrong

Bronze appended every 250,000-row batch. pyiceberg treats `write.target-file-size-bytes` as **uncompressed Arrow bytes** (Iceberg default 512 MiB). Each batch was far under that, so a month became 12–15 Parquet files of about 4 MiB. A full month is also slightly over 512 MiB uncompressed, so silver (one append per month) landed as two files, sometimes with a small tail file.

## Write target

`lakehouse/tuning.py` sets `write.target-file-size-bytes` to **2 GiB** uncompressed. A yellow month is under that budget and about 50–65 MiB on disk, so it is written as **one Parquet file**. New bronze ingests buffer batches up to the same target instead of appending each 250k-row batch.

`.\make.ps1 tune` rewrites any `trip_month` that still has more than one data file, then runs the benchmark queries.

## File layout

Current snapshot only. Older snapshots still point at the small files, so the `data/` directories on disk are larger than these totals until snapshots are expired.

| Table | Month | Files before | Bytes before | Files after | Bytes after | Rows |
|-------|-------|-------------:|-------------:|------------:|------------:|-----:|
| bronze | 2024-01 | 12 | 54,155,732 | 1 | 51,101,757 | 2,964,624 |
| bronze | 2024-02 | 13 | 54,416,725 | 1 | 51,959,436 | 3,007,526 |
| bronze | 2024-03 | 15 | 65,261,647 | 1 | 62,161,246 | 3,582,628 |
| bronze | 2024-04 | 15 | 63,818,866 | 1 | 61,140,063 | 3,514,289 |
| bronze | 2024-05 | 15 | 67,419,156 | 1 | 64,989,696 | 3,723,833 |
| silver | 2024-01 | 2 | 51,752,072 | 1 | 51,179,595 | 2,758,732 |
| silver | 2024-02 | 2 | 51,309,338 | 1 | 51,037,532 | 2,757,856 |
| silver | 2024-03 | 3 | 57,942,822 | 1 | 56,853,960 | 3,078,543 |
| silver | 2024-04 | 2 | 56,792,534 | 1 | 56,095,485 | 3,027,919 |
| silver | 2024-05 | 3 | 60,668,176 | 1 | 59,873,483 | 3,233,236 |

Bronze went from **70 files to 5**. Row counts did not change. On-disk bytes dropped slightly because one file has less footer overhead than a dozen.

## DuckDB EXPLAIN ANALYZE (`2024-01`)

Plans were taken with `iceberg_scan` on the pre-compaction metadata and again on the current metadata. Both runs were against a warm file cache; times are sub-second. The stable signal is **Total Files Read**.

| Query | Files before | Total time before | Files after | Total time after |
|-------|-------------:|------------------:|------------:|-----------------:|
| bronze `count(*)` | 12 | 0.0368s | 1 | 0.0111s |
| silver `count(*)` | 2 | 0.0083s | 1 | 0.0089s |
| silver daily `avg(fare_amount)` | 2 | 0.0192s | 1 | 0.0168s |

Bronze count is the query that moved. Silver was already two files, so its times sit in the noise. Each plan's scan filter is `trip_month='2024-01'`, and the file list is only that partition.

Repeat with:

```powershell
.\make.ps1 tune
```
