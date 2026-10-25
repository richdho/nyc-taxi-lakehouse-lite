"""Streamlit dashboard on dbt gold marts (Phase 6)."""

from __future__ import annotations

import streamlit as st

from lakehouse.config import LakehouseConfig
from serving.queries import (
    avg_fare_by_day_of_week,
    daily_trip_volume,
    gold_tables_present,
    list_trip_months,
    monthly_revenue,
    open_gold_connection,
    top_pickup_zones,
    trips_by_payment_type,
)

PAYMENT_TYPE_LABELS: dict[int, str] = {
    0: "Flex fare",
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided",
}


def _payment_label(code: int | None) -> str:
    if code is None:
        return "Unknown"
    return PAYMENT_TYPE_LABELS.get(int(code), f"Type {int(code)}")


@st.cache_resource
def _cached_connection(duckdb_path: str):
    cfg = LakehouseConfig.from_profile()
    return open_gold_connection(cfg)


def main() -> None:
    st.set_page_config(page_title="NYC Yellow Taxi — Gold", layout="wide")
    st.title("NYC Yellow Taxi — Gold marts")
    st.caption("Read-only dashboard on dbt gold marts (`main_gold.*` with dbt-duckdb).")

    cfg = LakehouseConfig.from_profile()
    st.sidebar.markdown(f"**DuckDB:** `{cfg.duckdb_path}`")

    try:
        con = _cached_connection(str(cfg.duckdb_path.resolve()))
    except FileNotFoundError as exc:
        st.error(str(exc))
        st.stop()

    if not gold_tables_present(con):
        st.warning(
            "Gold tables are missing. Build them with: "
            "`uv sync --extra dbt --extra dev` then `.\\make.ps1 dbt-gold`."
        )
        st.stop()

    months = list_trip_months(con)
    if not months:
        st.info("No trips in `gold.fact_trips` yet. Ingest silver and run `dbt-gold`.")
        st.stop()

    selected = st.sidebar.multiselect("Trip months", months, default=months[-1:])
    filter_months = selected or None

    daily = daily_trip_volume(con, trip_months=filter_months)
    revenue = monthly_revenue(con, trip_months=filter_months)
    zones = top_pickup_zones(con, trip_months=filter_months)
    payments = trips_by_payment_type(con, trip_months=filter_months)
    dow = avg_fare_by_day_of_week(con, trip_months=filter_months)

    total_trips = int(daily["trip_count"].sum()) if daily.height else 0
    total_revenue = float(revenue["total_revenue"].sum()) if revenue.height else 0.0
    c1, c2, c3 = st.columns(3)
    c1.metric("Trips (filtered)", f"{total_trips:,}")
    c2.metric("Revenue (filtered)", f"${total_revenue:,.2f}")
    c3.metric("Months in filter", len(selected) if selected else len(months))

    st.subheader("1. Daily trip volume")
    if daily.height:
        chart_df = daily.sort("trip_date").to_pandas().set_index("trip_date")[["trip_count"]]
        st.line_chart(chart_df)
    else:
        st.write("No data for this filter.")

    st.subheader("2. Revenue by trip month")
    if revenue.height:
        chart_df = revenue.sort("trip_month").to_pandas().set_index("trip_month")[["total_revenue"]]
        st.bar_chart(chart_df)
    else:
        st.write("No data for this filter.")

    left, right = st.columns(2)

    with left:
        st.subheader("3. Top pickup zones")
        if zones.height:
            zones_pd = zones.to_pandas()
            zones_pd["zone"] = zones_pd["borough"] + " — " + zones_pd["zone_name"]
            chart_df = zones_pd.set_index("zone")[["trip_count"]]
            st.bar_chart(chart_df)
        else:
            st.write("No data for this filter.")

    with right:
        st.subheader("4. Trips by payment type")
        if payments.height:
            pay_pd = payments.to_pandas()
            pay_pd["payment_label"] = pay_pd["payment_type"].map(
                lambda x: _payment_label(int(x) if x is not None else None)
            )
            chart_df = pay_pd.set_index("payment_label")[["trip_count"]]
            st.bar_chart(chart_df)
        else:
            st.write("No data for this filter.")

    st.subheader("5. Average fare by day of week")
    if dow.height:
        chart_df = dow.to_pandas().set_index("day_name")[["avg_total_amount"]]
        st.bar_chart(chart_df)
    else:
        st.write("No data for this filter.")


if __name__ == "__main__":
    main()
