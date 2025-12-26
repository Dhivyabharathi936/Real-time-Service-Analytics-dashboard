from __future__ import annotations

import argparse
import logging
import sqlite3
from pathlib import Path
from typing import Iterable, List

import pandas as pd

import load_excel_to_db as loader

LOGGER = logging.getLogger("etl.update_daily")

DEFAULT_UPDATES_DIR = Path("data/new_updates")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Append new service call logs from incremental Excel files."
    )
    parser.add_argument(
        "--updates-dir",
        type=Path,
        default=DEFAULT_UPDATES_DIR,
        help="Directory containing new Excel files to ingest.",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=loader.DEFAULT_DB_PATH,
        help="SQLite database file that stores service calls.",
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=loader.DEFAULT_SCHEMA_PATH,
        help="Schema file to ensure the database structure exists.",
    )
    return parser.parse_args()


def discover_workbooks(updates_dir: Path) -> List[Path]:
    if not updates_dir.exists():
        LOGGER.warning("Updates directory does not exist: %s", updates_dir)
        return []
    workbooks = sorted(
        [path for path in updates_dir.iterdir() if path.suffix.lower() in {".xls", ".xlsx"}]
    )
    LOGGER.info("Found %d update workbooks in %s", len(workbooks), updates_dir)
    return workbooks


def load_incremental_updates(workbooks: Iterable[Path]) -> pd.DataFrame:
    frames = []
    for workbook in workbooks:
        LOGGER.info("Processing incremental workbook: %s", workbook)
        df = pd.read_excel(workbook)
        cleaned = loader.clean_dataframe(df)
        frames.append(cleaned)
    if not frames:
        return pd.DataFrame(columns=loader.SERVICE_CALL_COLUMNS)
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["call_id"], keep="last")
    return combined


def run_updates(
    updates_dir: Path,
    database_path: Path,
    schema_path: Path,
) -> int:
    workbooks = discover_workbooks(updates_dir)
    if not workbooks:
        LOGGER.info("No update workbooks detected.")
        return 0

    incremental_df = load_incremental_updates(workbooks)
    if incremental_df.empty:
        LOGGER.info("No rows to append after cleaning.")
        return 0

    loader.ensure_parent_directory(database_path)

    with sqlite3.connect(database_path) as connection:
        loader.ensure_schema(connection, schema_path)
        inserted = loader.insert_dataframe(connection, incremental_df, conflict_mode="ignore")

    LOGGER.info(
        "Appended %d new service call rows from %d workbook(s).",
        inserted,
        len(workbooks),
    )
    return inserted


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()
    run_updates(args.updates_dir, args.database, args.schema)


if __name__ == "__main__":
    main()




