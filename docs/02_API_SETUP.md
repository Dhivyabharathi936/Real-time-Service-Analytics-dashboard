## API Setup

### 1. Install Dependencies
- Ensure Python 3.10+ is available.
- From the project root run `pip install -r requirements.txt` to install FastAPI, Uvicorn, pandas, and related libraries.

### 2. Start the FastAPI Server
- Command: `uvicorn backend.api.main:app --reload`
- The service listens on `http://127.0.0.1:8000` by default.
- Hot reload watches the backend folder so changes to routes or utilities are applied immediately.

### 3. Available Endpoints
- `GET /` – health check (`{"message": "Service Calls API is running"}`).
- `GET /calls/all` – latest 2,000 calls ordered by logged date.
- `GET /calls/by_id/{call_id}` – fetch a single service call.
- `GET /calls/filter` – apply query parameters (`state`, `model`, `engineer`, `start_date`, `end_date`, `status`, `instrument_status`, `limit`).
- `GET /calls/stats` – returns KPI snapshot (totals, closure counts, average resolution hours, distributions).
- `GET /calls/latest_update` – exposes the newest update entry (from `update_history` table when available, otherwise derives metadata from `service_calls`).

### 4. Testing Tips
- Browser / curl:
  - `curl http://127.0.0.1:8000/calls/by_id/100023`
  - `curl "http://127.0.0.1:8000/calls/filter?state=Karnataka&start_date=2024-01-01&end_date=2024-03-31&status=Solved"`
- Swagger UI: open `http://127.0.0.1:8000/docs` for an interactive tester. All schemas are documented via Pydantic models.
- Sample JSON (stats):
```
{
  "total_calls": 3087,
  "closed_calls": 3084,
  "pending_calls": 3,
  "average_resolution_hours": 14.27,
  "state_distribution": [{"label": "Tamil Nadu", "value": 1500}, ...],
  "model_distribution": [{"label": "Model X", "value": 420}, ...],
  "engineer_workload": [{"label": "Alex Kumar", "value": 180}, ...]
}
```

### 5. Using FastAPI Data in Power BI
- Power BI Desktop → Get Data → Web.
- Use the API endpoint URL (e.g., `http://127.0.0.1:8000/calls/all`).
- For filtered datasets, include query parameters inside the URL before loading.
- Convert the resulting JSON list into a table using Power Query’s `To Table` and `Expand` steps.
- Schedule refreshes by pointing to the same FastAPI endpoint hosted on your server/VM.

### 6. Automating Monthly / Daily Loads
- Run the ETL (`backend/etl/load_excel_to_db.py` for full refreshes or `backend/etl/update_daily_data.py` for increments) to keep `service_calls.db` synchronized.
- Restart the FastAPI server (or let `--reload` pick up the new data) to expose the latest records.

### 7. Example URL Reference
- All calls: `http://127.0.0.1:8000/calls/all`
- Filter by engineer and status: `http://127.0.0.1:8000/calls/filter?engineer=Alex%20Kumar&status=Solved`
- KPI dashboard feed: `http://127.0.0.1:8000/calls/stats`
- Latest update audit: `http://127.0.0.1:8000/calls/latest_update`




