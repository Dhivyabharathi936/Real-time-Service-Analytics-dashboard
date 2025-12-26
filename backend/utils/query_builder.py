from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Sequence, Tuple


FILTER_COLUMN_MAP = {
    "state": "state",
    "model": "model",
    "engineer": "visited_engineer_name",
    "status": "status",
    "instrument_status": "instrument_status",
}

DATE_COLUMN = "call_entry_datetime"


@dataclass
class QueryResult:
    sql: str
    params: Sequence[object]


def build_service_calls_query(
    filters: Dict[str, str | date | None],
    *,
    limit: int | None = None,
) -> QueryResult:
    """
    Build a parameterized SQL query for service_calls based on optional filters.
    """
    conditions: List[str] = []
    params: List[object] = []

    for key, column in FILTER_COLUMN_MAP.items():
        value = filters.get(key)
        if value is None:
            continue
        conditions.append(f"{column} = ?")
        params.append(value)

    start_date = filters.get("start_date")
    if start_date:
        conditions.append(f"date({DATE_COLUMN}) >= ?")
        params.append(_coerce_date(start_date))

    end_date = filters.get("end_date")
    if end_date:
        conditions.append(f"date({DATE_COLUMN}) <= ?")
        params.append(_coerce_date(end_date))

    where_clause = ""
    if conditions:
        where_clause = " WHERE " + " AND ".join(conditions)

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
        ORDER BY {DATE_COLUMN} DESC
        {limit_clause}
    """.strip()

    return QueryResult(sql=sql, params=params)


def _coerce_date(value: str | date) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return str(value)




