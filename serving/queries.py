"""SQL helpers for the gold mart dashboard (read-only DuckDB)."""

from __future__ import annotations

import re
from typing import Any

import duckdb
import polars as pl

from lakehouse.config import LakehouseConfig

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DEFAULT_GOLD_SCHEMA = "main_gold"


def open_gold_connection(cfg: LakehouseConfig) -> duckdb.DuckDBPyConnection:
    """Open the profile DuckDB file read-only for dashboard queries."""
    path = cfg.duckdb_path
    if not path.is_file():
        msg = f"DuckDB database not found: {path}. Run .\\make.ps1 dbt-gold after ingesting silver."
        raise FileNotFoundError(msg)
    return duckdb.connect(str(path), read_only=True)


def gold_schema_name(con: duckdb.DuckDBPyConnection) -> str:
    """Resolve dbt gold schema (dbt-duckdb default: ``main_gold``)."""
    rows = con.execute(
        """
        select distinct table_schema
        from information_schema.tables
        where table_name = 'fact_trips'
        """
    ).fetchall()
    schemas = [str(r[0]) for r in rows]
    for candidate in ("gold", _DEFAULT_GOLD_SCHEMA):
        if candidate in schemas:
            return candidate
    if schemas:
        return schemas[0]
    return _DEFAULT_GOLD_SCHEMA


def _schema(con: duckdb.DuckDBPyConnection) -> str:
    name = gold_schema_name(con)
    if not _IDENTIFIER_RE.match(name):
        msg = f"invalid gold schema name: {name!r}"
        raise ValueError(msg)
    return name


def gold_tables_present(con: duckdb.DuckDBPyConnection) -> bool:
    schema = _schema(con)
    row = con.execute(
        """
        select count(*)::bigint
        from information_schema.tables
        where table_schema = ?
          and table_name in ('fact_trips', 'dim_zone', 'dim_date')
        """,
        [schema],
    ).fetchone()
    return row is not None and row[0] == 3


def list_trip_months(con: duckdb.DuckDBPyConnection) -> list[str]:
    gold = _schema(con)
    rows = con.execute(
        f"""
        select distinct trip_month
        from {gold}.fact_trips
        where trip_month is not null
        order by trip_month
        """
    ).fetchall()
    return [str(r[0]) for r in rows]


def _month_filter_clause(
    trip_months: list[str] | None,
    param_name: str = "trip_months",
) -> tuple[str, dict[str, Any]]:
    if not trip_months:
        return "", {}
    return f" and f.trip_month in (select unnest(${param_name}))", {param_name: trip_months}


def daily_trip_volume(
    con: duckdb.DuckDBPyConnection,
    *,
    trip_months: list[str] | None = None,
) -> pl.DataFrame:
    gold = _schema(con)
    extra, params = _month_filter_clause(trip_months)
    sql = f"""
        select
            f.date_key::date as trip_date,
            count(*)::bigint as trip_count
        from {gold}.fact_trips f
        where f.date_key is not null
        {extra}
        group by 1
        order by 1
    """
    return pl.from_arrow(con.execute(sql, params).arrow())


def monthly_revenue(
    con: duckdb.DuckDBPyConnection,
    *,
    trip_months: list[str] | None = None,
) -> pl.DataFrame:
    gold = _schema(con)
    extra, params = _month_filter_clause(trip_months)
    sql = f"""
        select
            f.trip_month,
            round(sum(f.total_amount), 2) as total_revenue,
            count(*)::bigint as trip_count
        from {gold}.fact_trips f
        where f.trip_month is not null
        {extra}
        group by 1
        order by 1
    """
    return pl.from_arrow(con.execute(sql, params).arrow())


def top_pickup_zones(
    con: duckdb.DuckDBPyConnection,
    *,
    trip_months: list[str] | None = None,
    limit: int = 10,
) -> pl.DataFrame:
    gold = _schema(con)
    extra, params = _month_filter_clause(trip_months)
    params["zone_limit"] = limit
    sql = f"""
        select
            coalesce(z.zone_name, 'Unknown') as zone_name,
            coalesce(z.borough, 'Unknown') as borough,
            count(*)::bigint as trip_count
        from {gold}.fact_trips f
        left join {gold}.dim_zone z on z.zone_key = f.pickup_zone_key
        where 1 = 1
        {extra}
        group by 1, 2
        order by trip_count desc
        limit $zone_limit
    """
    return pl.from_arrow(con.execute(sql, params).arrow())


def trips_by_payment_type(
    con: duckdb.DuckDBPyConnection,
    *,
    trip_months: list[str] | None = None,
) -> pl.DataFrame:
    gold = _schema(con)
    extra, params = _month_filter_clause(trip_months)
    sql = f"""
        select
            f.payment_type,
            count(*)::bigint as trip_count
        from {gold}.fact_trips f
        where f.payment_type is not null
        {extra}
        group by 1
        order by 1
    """
    return pl.from_arrow(con.execute(sql, params).arrow())


def avg_fare_by_day_of_week(
    con: duckdb.DuckDBPyConnection,
    *,
    trip_months: list[str] | None = None,
) -> pl.DataFrame:
    gold = _schema(con)
    extra, params = _month_filter_clause(trip_months)
    sql = f"""
        select
            d.day_name,
            d.iso_day_of_week,
            round(avg(f.total_amount), 2) as avg_total_amount,
            count(*)::bigint as trip_count
        from {gold}.fact_trips f
        join {gold}.dim_date d on d.date_key = f.date_key
        where 1 = 1
        {extra}
        group by 1, 2
        order by d.iso_day_of_week
    """
    return pl.from_arrow(con.execute(sql, params).arrow())
