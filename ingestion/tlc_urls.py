"""NYC TLC yellow taxi Parquet URL helpers."""

from __future__ import annotations

from datetime import date

TLC_YELLOW_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DEFAULT_YELLOW_START = "2024-01"
DEFAULT_YELLOW_END = "2024-01"


def yellow_taxi_parquet_url(month: str) -> str:
    """Return the TLC CloudFront URL for ``YYYY-MM`` yellow taxi Parquet."""
    year, month_num = _parse_month(month)
    return f"{TLC_YELLOW_BASE}/yellow_tripdata_{year}-{month_num:02d}.parquet"


def iter_months(start: str, end: str) -> list[str]:
    """Inclusive list of ``YYYY-MM`` strings from start through end."""
    start_date = _month_to_date(start)
    end_date = _month_to_date(end)
    if start_date > end_date:
        msg = f"start month {start!r} is after end month {end!r}"
        raise ValueError(msg)

    months: list[str] = []
    year, month_num = start_date.year, start_date.month
    while (year, month_num) <= (end_date.year, end_date.month):
        months.append(f"{year}-{month_num:02d}")
        month_num += 1
        if month_num > 12:
            year += 1
            month_num = 1
    return months


def iter_months_from_env(
    start: str | None = None,
    end: str | None = None,
) -> list[str]:
    import os

    start_val = start or os.getenv("TLC_YELLOW_START", DEFAULT_YELLOW_START)
    end_val = end or os.getenv("TLC_YELLOW_END", DEFAULT_YELLOW_END)
    return iter_months(start_val, end_val)


def _parse_month(month: str) -> tuple[int, int]:
    parts = month.split("-")
    if len(parts) != 2:
        raise ValueError(f"Expected YYYY-MM, got {month!r}")
    return int(parts[0]), int(parts[1])


def _month_to_date(month: str) -> date:
    year, month_num = _parse_month(month)
    return date(year, month_num, 1)
