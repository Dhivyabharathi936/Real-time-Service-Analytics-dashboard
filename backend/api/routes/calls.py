from __future__ import annotations

from datetime import date
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from backend.database.db_connection import get_connection
from backend.utils.query_builder import build_service_calls_query

router = APIRouter(prefix="/calls", tags=["Service Calls"])

ALL_CALLS_LIMIT = 2000
FILTER_DEFAULT_LIMIT = 1000


def get_db():
    connection = get_connection()
    try:
        yield connection
    finally:
        connection.close()


class CallRecord(BaseModel):
    call_id: int
    customer_name: Optional[str] = None
    address: Optional[str] = None
    state: Optional[str] = None
    geo_loc_lat: Optional[float] = None
    geo_loc_lon: Optional[float] = None
    geo_loc_pincode: Optional[str] = None
    model: Optional[str] = None
    instrument_serial_no: Optional[str] = None
    warranty_expiry_date: Optional[str] = None
    zone: Optional[str] = None
    priority: Optional[str] = None
    visited_engineer_name: Optional[str] = None
    ticket_no: Optional[int] = None
    call_entry_datetime: Optional[str] = None
    start_call_datetime: Optional[str] = None
    call_solved_datetime: Optional[str] = None
    call_aging: Optional[str] = None
    response_time: Optional[str] = None
    recovery_time: Optional[str] = None
    customer_complaint: Optional[str] = None
    call_type: Optional[str] = None
    nature_of_complaint: Optional[str] = None
    complaint_description: Optional[str] = None
    call_status: Optional[str] = None
    status: Optional[str] = None
    visitor_remarks: Optional[str] = None
    forward_employee_name: Optional[str] = None
    instrument_status: Optional[str] = None


class DistributionItem(BaseModel):
    label: str
    value: int


class KPIStats(BaseModel):
    total_calls: int
    closed_calls: int
    pending_calls: int
    average_resolution_hours: Optional[float]
    state_distribution: List[DistributionItem]
    model_distribution: List[DistributionItem]
    engineer_workload: List[DistributionItem]


class LatestUpdate(BaseModel):
    source: str
    file_name: Optional[str] = None
    rows_processed: Optional[int] = None
    new_rows: Optional[int] = None
    updated_at: Optional[str] = None
    fallback: bool = False
    metadata: Optional[Dict[str, Optional[str]]] = None


@router.get("/all", response_model=List[CallRecord])
def get_all_calls(
    db=Depends(get_db),
) -> List[CallRecord]:
    cursor = db.execute(
        """
        SELECT *
        FROM service_calls
        ORDER BY call_entry_datetime DESC
        LIMIT ?
        """,
        (ALL_CALLS_LIMIT,),
    )
    rows = cursor.fetchall()
    return [CallRecord(**dict(row)) for row in rows]


@router.get("/by_id/{call_id}", response_model=CallRecord)
def get_call_by_id(call_id: int, db=Depends(get_db)) -> CallRecord:
    cursor = db.execute("SELECT * FROM service_calls WHERE call_id = ?", (call_id,))
    row = cursor.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Call ID {call_id} not found")
    return CallRecord(**dict(row))


@router.get("/filter", response_model=List[CallRecord])
def filter_calls(
    state: Optional[str] = Query(None, description="Filter by state"),
    model: Optional[str] = Query(None, description="Filter by model"),
    engineer: Optional[str] = Query(None, description="Filter by engineer name"),
    start_date: Optional[date] = Query(None, description="Call logged date >= YYYY-MM-DD"),
    end_date: Optional[date] = Query(None, description="Call logged date <= YYYY-MM-DD"),
    status: Optional[str] = Query(None, description="Filter by status"),
    instrument_status: Optional[str] = Query(None, description="Filter by instrument status"),
    limit: int = Query(
        FILTER_DEFAULT_LIMIT,
        ge=1,
        le=5000,
        description="Maximum rows to return (default 1000, max 5000)",
    ),
    db=Depends(get_db),
) -> List[CallRecord]:
    filters = {
        "state": state,
        "model": model,
        "engineer": engineer,
        "start_date": start_date,
        "end_date": end_date,
        "status": status,
        "instrument_status": instrument_status,
    }
    query = build_service_calls_query(filters, limit=limit)
    cursor = db.execute(query.sql, query.params)
    rows = cursor.fetchall()
    return [CallRecord(**dict(row)) for row in rows]


@router.get("/stats", response_model=KPIStats)
def get_stats(db=Depends(get_db)) -> KPIStats:
    totals = db.execute(
        """
        SELECT
            COUNT(*) AS total_calls,
            SUM(
                CASE
                    WHEN LOWER(COALESCE(status, '')) IN ('solved', 'closed', 'completed', 'resolved')
                         OR LOWER(COALESCE(call_status, '')) LIKE 'solved%'
                    THEN 1 ELSE 0
                END
            ) AS closed_calls,
            SUM(
                CASE
                    WHEN LOWER(COALESCE(status, '')) IN ('pending', 'processing', 'unsolved', 'open', 'in progress')
                    THEN 1 ELSE 0
                END
            ) AS pending_calls,
            AVG(
                (julianday(call_solved_datetime) - julianday(call_entry_datetime)) * 24.0
            ) AS avg_resolution
        FROM service_calls
        """
    ).fetchone()

    total_calls = totals["total_calls"] or 0
    closed_calls = totals["closed_calls"] or 0
    pending_calls = totals["pending_calls"] or 0
    avg_resolution = totals["avg_resolution"]
    if avg_resolution is not None:
        avg_resolution = round(avg_resolution, 2)

    state_distribution = _build_distribution(db, "state")
    model_distribution = _build_distribution(db, "model")
    engineer_workload = _build_distribution(db, "visited_engineer_name", limit=25)

    return KPIStats(
        total_calls=total_calls,
        closed_calls=closed_calls,
        pending_calls=pending_calls,
        average_resolution_hours=avg_resolution,
        state_distribution=state_distribution,
        model_distribution=model_distribution,
        engineer_workload=engineer_workload,
    )


@router.get("/latest_update", response_model=LatestUpdate)
def get_latest_update(db=Depends(get_db)) -> LatestUpdate:
    history = _latest_update_from_history(db)
    if history:
        return history

    fallback = _fallback_latest_update(db)
    if fallback:
        return fallback

    raise HTTPException(status_code=404, detail="No update information available")


def _build_distribution(db, column: str, limit: Optional[int] = None) -> List[DistributionItem]:
    limit_fragment = ""
    params: List[object] = []
    if limit is not None:
        limit_fragment = " LIMIT ?"
        params.append(limit)

    cursor = db.execute(
        f"""
        SELECT {column} AS label, COUNT(*) AS value
        FROM service_calls
        WHERE {column} IS NOT NULL AND TRIM({column}) <> ''
        GROUP BY {column}
        ORDER BY value DESC
        {limit_fragment}
        """,
        params,
    )
    return [DistributionItem(label=row["label"], value=row["value"]) for row in cursor.fetchall()]


def _latest_update_from_history(db) -> Optional[LatestUpdate]:
    table_exists = db.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name='update_history'
        """
    ).fetchone()
    if not table_exists:
        return None

    cursor = db.execute(
        """
        SELECT file_name,
               rows_processed,
               new_rows,
               updated_at
        FROM update_history
        ORDER BY updated_at DESC
        LIMIT 1
        """
    )
    row = cursor.fetchone()
    if not row:
        return None

    return LatestUpdate(
        source="update_history",
        file_name=row["file_name"],
        rows_processed=row["rows_processed"],
        new_rows=row["new_rows"],
        updated_at=row["updated_at"],
        fallback=False,
    )


def _fallback_latest_update(db) -> Optional[LatestUpdate]:
    cursor = db.execute(
        """
        SELECT
            COUNT(*) AS total_rows,
            MAX(call_entry_datetime) AS last_call_entry,
            MAX(call_solved_datetime) AS last_call_solved,
            MAX(start_call_datetime) AS last_start
        FROM service_calls
        """
    )
    row = cursor.fetchone()
    if not row or row["total_rows"] == 0:
        return None

    metadata = {
        "last_call_entry": row["last_call_entry"],
        "last_call_solved": row["last_call_solved"],
        "last_start_call": row["last_start"],
        "total_rows": str(row["total_rows"]),
    }

    return LatestUpdate(
        source="service_calls",
        file_name=None,
        rows_processed=row["total_rows"],
        new_rows=None,
        updated_at=row["last_call_entry"],
        fallback=True,
        metadata=metadata,
    )

