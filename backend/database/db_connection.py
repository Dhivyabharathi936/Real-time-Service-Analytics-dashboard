from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

DB_PATH = Path(__file__).resolve().parent / "service_calls.db"


def _configure_connection(connection: sqlite3.Connection) -> sqlite3.Connection:
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL;")
    connection.execute("PRAGMA foreign_keys=ON;")
    return connection


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """
    Create a SQLite connection to the service calls database.
    The caller is responsible for closing the connection.
    """
    target = db_path or DB_PATH
    connection = sqlite3.connect(target)
    return _configure_connection(connection)


@contextmanager
def connection_scope(db_path: Path | None = None) -> Iterator[sqlite3.Connection]:
    """
    Context manager that yields a configured connection and ensures cleanup.
    """
    connection = get_connection(db_path)
    try:
        yield connection
    finally:
        connection.close()




