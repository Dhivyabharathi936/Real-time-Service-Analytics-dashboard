## ETL Setup

### Overview
- The ETL scripts convert the Excel call logs into a clean `service_calls` table stored in `backend/database/service_calls.db`.
- All null/blank values are normalized to `NULL`, date fields are rewritten to ISO-8601 strings, and duplicates are resolved via `Call ID`.
- The SQLite schema (indexes + table definition) lives in `backend/database/schema.sql` and is executed automatically by every ETL run.

### Prerequisites
- Python 3.10+ with `pip`
- Install dependencies once:
  - `pip install -r requirements.txt`
- Ensure the Excel workbooks are available under the `data/` directory (main snapshot and any future incremental drops).

### One-Time Database Creation / Full Reload
- Place the master Excel file at `data/Call logs - Sample.xlsx` (or provide the actual path via `--source`).
- Run:
  - `python backend/etl/load_excel_to_db.py`
- Optional flags:
  - `--source <path-to-excel>`
  - `--database backend/database/service_calls.db`
  - `--schema backend/database/schema.sql`
- What happens:
  - The script reads the workbook with pandas, renames every column to snake_case, trims whitespace, converts the four datetime fields plus the warranty date, casts numeric columns, and drops rows that lack a `Call ID`.
  - Blanks become `NULL`, pincodes are stored as text, and duplicates are deduplicated on `call_id` (latest row wins).
  - The SQLite file and directory are created automatically if they do not exist, the schema file is executed, and the cleaned data is upserted (existing rows are refreshed, new rows are added).

### Monthly / Daily Incremental Updates
- Drop any new Excel exports into `data/new_updates/` (a single folder can hold multiple files; they are processed in alphabetical order).
- Run:
  - `python backend/etl/update_daily_data.py`
- Behavior:
  - Every workbook is cleaned using the exact same rules as the initial loader.
  - Rows are deduplicated inside the batch and inserted with `ON CONFLICT(call_id) DO NOTHING`, guaranteeing that prior calls are never duplicated.
  - Logs report how many net-new rows landed as well as which files were scanned.

### Managing the Database
- The schema file defines the `service_calls` table plus indexes on `state`, `model`, `visited_engineer_name`, `call_entry_datetime`, and `call_solved_datetime`—ideal for downstream analytics filters.
- To inspect data quickly, use the SQLite CLI:
  - `sqlite3 backend/database/service_calls.db "SELECT COUNT(*) FROM service_calls;"`,
  - or point BI tooling at the same file.
- If the schema evolves, update `backend/database/schema.sql` first, then rerun `load_excel_to_db.py` to rebuild/reseed the database.

### Adding New Data Sources
- For additional regional workbooks, follow the same pattern: drop them in `data/new_updates/` and rerun `update_daily_data.py`.
- If a workbook has structurally different columns, update `COLUMN_RENAMES` plus transformation rules inside `backend/etl/load_excel_to_db.py` so that both scripts stay in sync.
- Keep backups of the raw Excel files—only cleaned data is persisted inside SQLite.




