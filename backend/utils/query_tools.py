from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
from dateutil import parser

from backend.database.db_connection import connection_scope

DATE_COLUMNS = [
    "call_entry_datetime",
    "start_call_datetime",
    "call_solved_datetime",
    "warranty_expiry_date",
]

FILTER_COLUMN_MAP = {
    "state": "state",
    "model": "model",
    "assigned_to": "forward_employee_name",
    "engineer": "visited_engineer_name",
    "issue_category": "nature_of_complaint",
    "instrument_status": "instrument_status",
    "status": "status",
}


@dataclass(frozen=True)
class FilterMetadata:
    date_min: Optional[date]
    date_max: Optional[date]
    state_options: List[str]
    model_options: List[str]
    assigned_to_options: List[str]
    engineer_options: List[str]
    issue_category_options: List[str]
    instrument_status_options: List[str]
    status_options: List[str]


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    return parser.parse(value).date()


@lru_cache(maxsize=1)
def get_filter_metadata() -> FilterMetadata:
    with connection_scope() as connection:
        min_max = connection.execute(
            """
            SELECT MIN(date(call_entry_datetime)), MAX(date(call_entry_datetime))
            FROM service_calls
            """
        ).fetchone()
        column_options: Dict[str, List[str]] = {}
        for key, column in FILTER_COLUMN_MAP.items():
            column_options[key] = _fetch_distinct_values(connection, column)

        return FilterMetadata(
            date_min=_parse_date(min_max[0]) if min_max else None,
            date_max=_parse_date(min_max[1]) if min_max else None,
            state_options=column_options["state"],
            model_options=column_options["model"],
            assigned_to_options=column_options["assigned_to"],
            engineer_options=column_options["engineer"],
            issue_category_options=column_options["issue_category"],
            instrument_status_options=column_options["instrument_status"],
            status_options=column_options["status"],
        )


def _fetch_distinct_values(connection, column: str) -> List[str]:
    cursor = connection.execute(
        f"""
        SELECT DISTINCT {column}
        FROM service_calls
        WHERE {column} IS NOT NULL AND TRIM({column}) <> ''
        ORDER BY {column}
        """
    )
    return [row[0] for row in cursor.fetchall()]


def fetch_filtered_calls(filters: Dict[str, object], limit: Optional[int] = None) -> pd.DataFrame:
    conditions: List[str] = []
    params: List[object] = []

    start_date = filters.get("start_date")
    end_date = filters.get("end_date")
    if start_date:
        conditions.append("date(call_entry_datetime) >= ?")
        params.append(str(start_date))
    if end_date:
        conditions.append("date(call_entry_datetime) <= ?")
        params.append(str(end_date))

    for key, column in FILTER_COLUMN_MAP.items():
        value = filters.get(key)
        if not value:
            continue
        _append_condition(column, value, conditions, params)

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    limit_clause = ""
    if limit is not None:
        if limit <= 0:
            raise ValueError("limit must be positive")
        limit_clause = " LIMIT ?"
        params.append(limit)

    sql = f"""
        SELECT *
        FROM service_calls
        {where_clause}
        ORDER BY call_entry_datetime DESC
        {limit_clause}
    """.strip()

    with connection_scope() as connection:
        connection.execute("PRAGMA synchronous = NORMAL;")
        connection.execute("PRAGMA temp_store = MEMORY;")
        df = pd.read_sql_query(sql, connection, params=params)

    return _postprocess_dataframe(df)


def _append_condition(column: str, value, conditions: List[str], params: List[object]) -> None:
    if isinstance(value, (list, tuple, set)):
        values = [v for v in value if v not in (None, "")]
        if not values:
            return
        placeholders = ", ".join(["?"] * len(values))
        conditions.append(f"{column} IN ({placeholders})")
        params.extend(values)
    else:
        conditions.append(f"{column} = ?")
        params.append(value)


def _postprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    for column in DATE_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce")
    numeric_columns = ["geo_loc_lat", "geo_loc_lon"]
    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def compute_kpis(df: pd.DataFrame) -> Dict[str, object]:
    total_calls = int(len(df))
    if total_calls == 0:
        return {
            "total_calls": 0,
            "closed_calls": 0,
            "pending_calls": 0,
            "avg_resolution_days": 0.0,
            "sla_compliance": 0.0,
            "repeated_issues": 0,
        }

    closed_mask = _status_matches(df, targets={"solved", "closed", "completed", "resolved"})
    closed_calls = int(closed_mask.sum())

    pending_mask = _status_matches(
        df,
        targets={"pending", "processing", "unsolved", "open", "in progress"},
    )
    pending_calls = int(pending_mask.sum())

    resolution_days = _resolution_days(df)
    avg_resolution = round(float(resolution_days.mean()), 2) if not resolution_days.empty else 0.0

    sla_hits = resolution_days[resolution_days <= 2]
    sla_compliance = (
        round(float(len(sla_hits) / len(resolution_days) * 100.0), 1) if len(resolution_days) else 0.0
    )

    repeated_issues = _repeated_issue_count(df)

    return {
        "total_calls": total_calls,
        "closed_calls": closed_calls,
        "pending_calls": pending_calls,
        "avg_resolution_days": avg_resolution,
        "sla_compliance": sla_compliance,
        "repeated_issues": repeated_issues,
    }


def _status_matches(df: pd.DataFrame, targets: Iterable[str]) -> pd.Series:
    targets_lower = {value.lower() for value in targets}
    default_series = pd.Series(index=df.index, dtype="object")
    status_series = df.get("status", default_series)
    call_status_series = df.get("call_status", default_series)
    status_match = status_series.fillna("").str.lower().isin(targets_lower)
    call_status_match = (
        call_status_series.fillna("").str.lower()
        .apply(lambda value: any(value.startswith(target) for target in targets_lower))
    )
    return status_match | call_status_match


def _resolution_days(df: pd.DataFrame) -> pd.Series:
    if "call_entry_datetime" not in df or "call_solved_datetime" not in df:
        return pd.Series(dtype=float)
    resolved = df.dropna(subset=["call_entry_datetime", "call_solved_datetime"]).copy()
    if resolved.empty:
        return pd.Series(dtype=float)
    delta = resolved["call_solved_datetime"] - resolved["call_entry_datetime"]
    return delta.dt.total_seconds().div(86400)


def _repeated_issue_count(df: pd.DataFrame) -> int:
    subset = df.dropna(subset=["customer_name", "customer_complaint", "call_entry_datetime"]).copy()
    if subset.empty:
        return 0
    subset.sort_values(by=["customer_name", "customer_complaint", "call_entry_datetime"], inplace=True)
    diff_days = (
        subset.groupby(["customer_name", "customer_complaint"])["call_entry_datetime"]
        .diff()
        .dt.total_seconds()
        .div(86400)
    )
    flags = diff_days.notna() & (diff_days <= 30)
    return int(flags.sum())


def prepare_calls_logged_per_day(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "call_entry_datetime" not in df:
        return pd.DataFrame(columns=["date", "count"])
    data = (
        df.dropna(subset=["call_entry_datetime"])
        .assign(date=lambda d: d["call_entry_datetime"].dt.date)
        .groupby("date")
        .size()
        .reset_index(name="count")
        .sort_values("date")
    )
    return data


def prepare_calls_closed_per_day(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "call_solved_datetime" not in df:
        return pd.DataFrame(columns=["date", "count"])
    data = (
        df.dropna(subset=["call_solved_datetime"])
        .assign(date=lambda d: d["call_solved_datetime"].dt.date)
        .groupby("date")
        .size()
        .reset_index(name="count")
        .sort_values("date")
    )
    return data


def prepare_top_issues(df: pd.DataFrame, limit: int = 10) -> pd.DataFrame:
    column = "customer_complaint"
    if column not in df:
        return pd.DataFrame(columns=["issue", "count"])
    data = (
        df[column]
        .dropna()
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .value_counts()
        .head(limit)
        .reset_index()
    )
    data.columns = ["issue", "count"]
    return data


def prepare_distribution(df: pd.DataFrame, column: str, limit: Optional[int] = None) -> pd.DataFrame:
    if column not in df:
        return pd.DataFrame(columns=[column, "count"])
    series = df[column].dropna().astype(str).str.strip()
    series = series.replace("", pd.NA).dropna()
    data = series.value_counts().reset_index()
    data.columns = [column, "count"]
    if limit:
        data = data.head(limit)
    return data


def prepare_resolution_distribution(df: pd.DataFrame) -> pd.DataFrame:
    resolutions = _resolution_days(df)
    if resolutions.empty:
        return pd.DataFrame(columns=["resolution_days"])
    return pd.DataFrame({"resolution_days": resolutions})


def prepare_map_points(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["geo_loc_lat", "geo_loc_lon"])
    lat_lon = df.dropna(subset=["geo_loc_lat", "geo_loc_lon"]).copy()
    lat_lon = lat_lon[
        (lat_lon["geo_loc_lat"].between(-90, 90)) & (lat_lon["geo_loc_lon"].between(-180, 180))
    ]
    return lat_lon[["geo_loc_lat", "geo_loc_lon", "state", "model", "visited_engineer_name"]]

