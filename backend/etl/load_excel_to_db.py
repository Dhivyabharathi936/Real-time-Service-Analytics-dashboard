from __future__ import annotations

import argparse
import logging
import sqlite3
from pathlib import Path
from typing import Iterable, List, Sequence

import pandas as pd

LOGGER = logging.getLogger("etl.load_excel")

DEFAULT_SOURCE = Path("data/Call logs - Sample.xlsx")
DEFAULT_DB_PATH = Path("backend/database/service_calls.db")
DEFAULT_SCHEMA_PATH = Path("backend/database/schema.sql")

COLUMN_RENAMES = {
    "Call ID": "call_id",
    "Customer Name": "customer_name",
    "Address": "address",
    "State": "state",
    "Geo Loc - Lat": "geo_loc_lat",
    "Geo Loc - Lan": "geo_loc_lon",
    "Geo Loc - Pincode": "geo_loc_pincode",
    "Model": "model",
    "Instrument Serial No": "instrument_serial_no",
    "Warranty Expiry Date": "warranty_expiry_date",
    "Zone": "zone",
    "Priority": "priority",
    "Visited Engineer Name": "visited_engineer_name",
    "Ticket No": "ticket_no",
    "Call Entry Date Time": "call_entry_datetime",
    "Start Call Date Time": "start_call_datetime",
    "Call Solved Date Time": "call_solved_datetime",
    "Call Aging": "call_aging",
    "Response Time": "response_time",
    "Recovery Time": "recovery_time",
    "Customer Complaint": "customer_complaint",
    "Call Type": "call_type",
    "Nature Of Complaint": "nature_of_complaint",
    "Complaint Description": "complaint_description",
    "Call Status": "call_status",
    "Status": "status",
    "Visitor Remarks": "visitor_remarks",
    "Forward Employee Name": "forward_employee_name",
    "Instrument Status": "instrument_status",
}

SERVICE_CALL_COLUMNS: List[str] = list(COLUMN_RENAMES.values())

DATETIME_COLUMNS = {
    "call_entry_datetime": "%Y-%m-%d %H:%M:%S",
    "start_call_datetime": "%Y-%m-%d %H:%M:%S",
    "call_solved_datetime": "%Y-%m-%d %H:%M:%S",
}

DATE_ONLY_COLUMNS = {
    "warranty_expiry_date": "%Y-%m-%d",
}

FLOAT_COLUMNS = ("geo_loc_lat", "geo_loc_lon")
INT_COLUMNS = ("call_id", "ticket_no")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load the master Excel service call log into the SQLite database."
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=DEFAULT_SOURCE,
        help=f"Path to the Excel workbook to load (default: {DEFAULT_SOURCE})",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to the SQLite database file (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=DEFAULT_SCHEMA_PATH,
        help="Path to the SQL schema file that defines service_calls table.",
    )
    return parser.parse_args()


def read_workbook(workbook_path: Path) -> pd.DataFrame:
    if not workbook_path.exists():
        raise FileNotFoundError(f"Excel workbook not found: {workbook_path}")
    LOGGER.info("Reading Excel workbook: %s", workbook_path)
    return pd.read_excel(workbook_path)


def clean_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    missing_columns = set(COLUMN_RENAMES.keys()) - set(raw_df.columns)
    if missing_columns:
        raise ValueError(f"Workbook missing required columns: {sorted(missing_columns)}")

    df = raw_df.rename(columns=COLUMN_RENAMES)
    df = df[SERVICE_CALL_COLUMNS].copy()

    for column in FLOAT_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    for column in INT_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")

    df = df.dropna(subset=["call_id"])

    for column, date_format in DATETIME_COLUMNS.items():
        df[column] = _format_datetime_series(df[column], date_format)

    for column, date_format in DATE_ONLY_COLUMNS.items():
        df[column] = _format_datetime_series(df[column], date_format)

    df["geo_loc_pincode"] = df["geo_loc_pincode"].apply(_format_pincode)

    # Normalize whitespace for text columns
    text_columns = df.select_dtypes(include=["object"]).columns
    for column in text_columns:
        df[column] = df[column].apply(lambda value: value.strip() if isinstance(value, str) else value)

    df = df.replace({pd.NA: None, pd.NaT: None})
    for column in df.columns:
        df[column] = df[column].apply(_normalize_blank_strings)

    df = df.drop_duplicates(subset=["call_id"], keep="last")

    return df


def _format_datetime_series(series: pd.Series, date_format: str) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    return parsed.apply(lambda value: value.strftime(date_format) if pd.notna(value) else None)


def _format_pincode(value) -> str | None:
    if pd.isna(value):
        return None
    try:
        int_value = int(float(value))
    except (TypeError, ValueError):
        sanitized = str(value).strip()
        return sanitized or None
    return str(int_value)


def _normalize_blank_strings(value):
    if isinstance(value, str) and not value.strip():
        return None
    return value


def ensure_schema(connection: sqlite3.Connection, schema_path: Path) -> None:
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    LOGGER.info("Applying schema from %s", schema_path)
    with schema_path.open("r", encoding="utf-8") as schema_file:
        connection.executescript(schema_file.read())


def insert_dataframe(
    connection: sqlite3.Connection, df: pd.DataFrame, *, conflict_mode: str = "upsert"
) -> int:
    if df.empty:
        return 0
    sql = _build_insert_statement(conflict_mode)
    rows = _dataframe_to_rows(df, SERVICE_CALL_COLUMNS)
    LOGGER.info("Inserting %d rows into service_calls (%s)", len(rows), conflict_mode)
    before = connection.total_changes
    with connection:
        connection.executemany(sql, rows)
    inserted = connection.total_changes - before
    return inserted


def _build_insert_statement(conflict_mode: str) -> str:
    columns = ", ".join(SERVICE_CALL_COLUMNS)
    placeholders = ", ".join(["?"] * len(SERVICE_CALL_COLUMNS))
    base = f"INSERT INTO service_calls ({columns}) VALUES ({placeholders})"
    if conflict_mode == "upsert":
        assignments = ", ".join(
            f"{column}=excluded.{column}" for column in SERVICE_CALL_COLUMNS if column != "call_id"
        )
        return f"{base} ON CONFLICT(call_id) DO UPDATE SET {assignments}"
    if conflict_mode == "ignore":
        return f"{base} ON CONFLICT(call_id) DO NOTHING"
    raise ValueError(f"Unsupported conflict_mode: {conflict_mode}")


def _dataframe_to_rows(df: pd.DataFrame, columns: Sequence[str]) -> Iterable[Sequence]:
    sanitized = df.where(pd.notnull(df), None)
    return [tuple(row[column] for column in columns) for _, row in sanitized.iterrows()]


def ensure_parent_directory(file_path: Path) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)


def run_loader(source: Path, db_path: Path, schema_path: Path) -> int:
    df = clean_dataframe(read_workbook(source))
    ensure_parent_directory(db_path)
    with sqlite3.connect(db_path) as connection:
        ensure_schema(connection, schema_path)
        inserted = insert_dataframe(connection, df, conflict_mode="upsert")
    LOGGER.info("Completed load: %d rows upserted into %s", inserted, db_path)
    return inserted


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()
    run_loader(args.source, args.database, args.schema)


if __name__ == "__main__":
    main()

