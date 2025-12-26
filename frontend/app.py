from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
import plotly.express as px
import streamlit as st

# Ensure the backend package is importable when running `streamlit run frontend/app.py`
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from backend.utils import query_tools  # noqa: E402

def _render_kpi(column, label: str, value, accent_class: str = "") -> None:
    with column:
        st.markdown(
            f"""
            <div class="kpi-card">
                <div class="kpi-label">{label}</div>
                <div class="kpi-value {accent_class}">{_format_value(value)}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _format_value(value) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if abs(value) >= 1000 and isinstance(value, int):
            return f"{value:,}"
        if isinstance(value, float):
            return f"{value:,.2f}"
    return str(value)


def _prepare_chart_data(df):
    return {
        "calls_logged": query_tools.prepare_calls_logged_per_day(df),
        "calls_closed": query_tools.prepare_calls_closed_per_day(df),
        "top_issues": query_tools.prepare_top_issues(df),
        "state_distribution": query_tools.prepare_distribution(df, "state"),
        "engineer_workload": query_tools.prepare_distribution(df, "visited_engineer_name", limit=15),
        "model_distribution": query_tools.prepare_distribution(df, "model", limit=15),
        "instrument_distribution": query_tools.prepare_distribution(df, "instrument_status"),
        "call_type_distribution": query_tools.prepare_distribution(df, "call_type"),
        "resolution_distribution": query_tools.prepare_resolution_distribution(df),
    }


def _render_time_series(chart_data):
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-header">Trend Analysis</div>', unsafe_allow_html=True)
    line_cols = st.columns(2)
    _plot_line_chart(line_cols[0], chart_data["calls_logged"], "date", "count", "Calls Logged Per Day")
    _plot_line_chart(line_cols[1], chart_data["calls_closed"], "date", "count", "Calls Closed Per Day")
    st.markdown("</div>", unsafe_allow_html=True)


def _render_distribution_charts(chart_data):
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-header">Distribution Insights</div>', unsafe_allow_html=True)
    row1 = st.columns(2)
    _plot_bar_chart(row1[0], chart_data["top_issues"], "issue", "count", "Top 10 Frequent Issues")
    _plot_bar_chart(
        row1[1],
        chart_data["state_distribution"],
        "state",
        "count",
        "State-wise Call Distribution",
        orientation="h",
    )

    row2 = st.columns(2)
    _plot_bar_chart(row2[0], chart_data["engineer_workload"], "visited_engineer_name", "count", "Engineer Workload")
    _plot_bar_chart(row2[1], chart_data["model_distribution"], "model", "count", "Model-wise Issues")

    row3 = st.columns(2)
    _plot_pie_chart(row3[0], chart_data["instrument_distribution"], "instrument_status", "count", "Instrument Status")
    _plot_bar_chart(row3[1], chart_data["call_type_distribution"], "call_type", "count", "Call Type Breakdown")
    st.markdown("</div>", unsafe_allow_html=True)


def _render_resolution_chart(chart_data):
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-header">Resolution Time Distribution</div>', unsafe_allow_html=True)
    _plot_histogram(chart_data["resolution_distribution"], "resolution_days", "Resolution Time (Days)")
    st.markdown("</div>", unsafe_allow_html=True)


def _plot_line_chart(column, data, x_field, y_field, title):
    with column:
        if data.empty:
            st.info(f"{title}: No data")
            return
        fig = px.line(data, x=x_field, y=y_field, markers=True, title=title)
        fig.update_traces(line=dict(width=3, color="#2c7be5"))
        fig.update_layout(margin=dict(l=0, r=0, t=60, b=0))
        st.plotly_chart(fig, use_container_width=True)


def _plot_bar_chart(column, data, x_field, y_field, title, orientation: str = "v"):
    with column:
        if data.empty:
            st.info(f"{title}: No data")
            return
        if orientation == "h":
            data = data.sort_values(y_field)
        fig = px.bar(
            data,
            x=x_field if orientation == "v" else y_field,
            y=y_field if orientation == "v" else x_field,
            orientation=orientation,
            title=title,
        )
        fig.update_layout(margin=dict(l=0, r=0, t=60, b=0))
        st.plotly_chart(fig, use_container_width=True)


def _plot_pie_chart(column, data, names_field, values_field, title):
    with column:
        if data.empty:
            st.info(f"{title}: No data")
            return
        fig = px.pie(data, names=names_field, values=values_field, title=title, hole=0.45)
        fig.update_traces(textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)


def _plot_histogram(data, x_field, title):
    if data.empty:
        st.info("No resolution data available.")
        return
    fig = px.histogram(data, x=x_field, nbins=20, title=title, color_discrete_sequence=["#2c7be5"])
    fig.update_layout(margin=dict(l=0, r=0, t=60, b=0), bargap=0.08)
    st.plotly_chart(fig, use_container_width=True)


def _render_map(df):
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-header">Field Deployment Map</div>', unsafe_allow_html=True)
    map_points = query_tools.prepare_map_points(df)
    if map_points.empty:
        st.info("No geographic coordinates available for the current filters.")
    else:
        st.map(map_points.rename(columns={"geo_loc_lat": "lat", "geo_loc_lon": "lon"}))
    st.markdown("</div>", unsafe_allow_html=True)


def _render_data_table(df):
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-header">Data Explorer</div>', unsafe_allow_html=True)
    search_query = st.text_input("Search calls", key="search", placeholder="Customer, issue, engineer...")
    table_df = df.copy()
    if search_query:
        search_cols = [
            "customer_name",
            "customer_complaint",
            "visited_engineer_name",
            "forward_employee_name",
            "state",
            "model",
            "instrument_status",
        ]
        mask = _build_search_mask(table_df, search_query, search_cols)
        table_df = table_df[mask]

    if table_df.empty:
        st.info("No rows to display.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    display_df = table_df.copy()
    for column in query_tools.DATE_COLUMNS:
        if column in display_df:
            display_df[column] = display_df[column].dt.strftime("%Y-%m-%d %H:%M")
    display_df = display_df[
        [
            "call_id",
            "call_entry_datetime",
            "call_solved_datetime",
            "state",
            "model",
            "customer_name",
            "customer_complaint",
            "visited_engineer_name",
            "forward_employee_name",
            "instrument_status",
            "status",
        ]
    ].rename(
        columns={
            "call_entry_datetime": "Call Logged",
            "call_solved_datetime": "Call Closed",
            "customer_name": "Customer",
            "customer_complaint": "Issue",
            "visited_engineer_name": "Engineer",
            "forward_employee_name": "Assigned To",
            "instrument_status": "Instrument",
            "status": "Status",
        }
    )

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)


def _build_search_mask(df, query: str, columns: Iterable[str]):
    query_lower = query.lower()
    mask = pd.Series(False, index=df.index)
    for column in columns:
        if column in df:
            mask = mask | df[column].fillna("").astype(str).str.lower().str.contains(query_lower)
    return mask


st.set_page_config(
    page_title="Service Call Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)


def _load_css() -> None:
    style_path = Path(__file__).with_name("style.css")
    if style_path.exists():
        st.markdown(f"<style>{style_path.read_text()}</style>", unsafe_allow_html=True)


_load_css()

st.markdown('<h1 class="page-title">Service Call Analytics Dashboard</h1>', unsafe_allow_html=True)
st.markdown(
    '<p class="page-subtitle">Full visibility into service performance, issue trends, and engineer productivity.</p>',
    unsafe_allow_html=True,
)

metadata = query_tools.get_filter_metadata()
default_start = metadata.date_min or (date.today() - timedelta(days=90))
default_end = metadata.date_max or date.today()

st.markdown('<div class="filter-card">', unsafe_allow_html=True)
st.markdown('<div class="section-header">Global Filters</div>', unsafe_allow_html=True)
filter_row1 = st.columns([1.2, 1, 1, 1])
filter_row2 = st.columns([1, 1, 1, 1])

date_range = filter_row1[0].date_input(
    "Call Logged Date Range",
    value=(default_start, default_end),
)
state_filter = filter_row1[1].multiselect("State", options=metadata.state_options)
model_filter = filter_row1[2].multiselect("Model", options=metadata.model_options)
assigned_filter = filter_row1[3].multiselect("Assigned To", options=metadata.assigned_to_options)

engineer_filter = filter_row2[0].multiselect("Visitor / Engineer", options=metadata.engineer_options)
issue_filter = filter_row2[1].multiselect("Issue Category", options=metadata.issue_category_options)
instrument_status_filter = filter_row2[2].multiselect(
    "Instrument Status",
    options=metadata.instrument_status_options,
)
status_filter = filter_row2[3].multiselect(
    "Call Status",
    options=metadata.status_options or ["Solved", "Processing", "UnSolved", "Pending"],
)
st.markdown("</div>", unsafe_allow_html=True)

if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range

filters = {
    "start_date": start_date,
    "end_date": end_date,
    "state": state_filter or None,
    "model": model_filter or None,
    "assigned_to": assigned_filter or None,
    "engineer": engineer_filter or None,
    "issue_category": issue_filter or None,
    "instrument_status": instrument_status_filter or None,
    "status": status_filter or None,
}

filtered_df = query_tools.fetch_filtered_calls(filters)

if filtered_df.empty:
    st.warning("No service calls match the selected filters.")
else:
    kpis = query_tools.compute_kpis(filtered_df)

    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-header">Key Performance Indicators</div>', unsafe_allow_html=True)
    kpi_cols = st.columns(6)
    _render_kpi(kpi_cols[0], "Total Calls Logged", kpis["total_calls"])
    _render_kpi(kpi_cols[1], "Total Calls Closed", kpis["closed_calls"])
    _render_kpi(kpi_cols[2], "Pending Calls", kpis["pending_calls"], accent_class="metric-warning")
    _render_kpi(kpi_cols[3], "Avg. Resolution (days)", kpis["avg_resolution_days"])
    _render_kpi(kpi_cols[4], "SLA Compliance", f"{kpis['sla_compliance']}%", accent_class="metric-positive")
    _render_kpi(kpi_cols[5], "Repeated Issue Count", kpis["repeated_issues"])
    st.markdown("</div>", unsafe_allow_html=True)

    chart_data = _prepare_chart_data(filtered_df)
    _render_time_series(chart_data)
    _render_distribution_charts(chart_data)
    _render_resolution_chart(chart_data)
    _render_map(filtered_df)
    _render_data_table(filtered_df)


# Run with:
#   streamlit run frontend/app.py

