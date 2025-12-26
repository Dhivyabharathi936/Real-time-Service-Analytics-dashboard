## Power BI Integration

### 1. Connect to the SQLite Database
- Open Power BI Desktop → `Get Data` → `More…`.
- Select **Database → SQLite database** (install the [official connector](https://github.com/ericstromberg/SQLiteOdbcDriver) or use the built-in connector on the latest releases).
- Browse to `backend/database/service_calls.db`.
- Authentication = `Default` (no username/password). Confirm.

### 2. Load the `service_calls` Table
- In Navigator select the `service_calls` table (and optionally future helper tables such as `update_history`).
- Click **Load** to bring the table into Power BI or **Transform Data** to open Power Query for shaping.
- Rename the query to `Service Calls` for clarity.

### 3. Refresh Strategy
- **Manual refresh:** from Power BI Desktop use `Home → Refresh`. This will rerun the SQLite query and pull the latest records. Save the `.pbix` file afterwards.
- **Automated refresh via Gateway:**
  1. Install the **Power BI On-premises Data Gateway** on the same machine (or a secure server) that hosts `service_calls.db`.
  2. Sign into the gateway with your Power BI service account and register a new data source pointing to the SQLite file path.
  3. Publish the report to the Power BI Service, then map the dataset to the configured gateway data source.
  4. Configure a refresh schedule (e.g., nightly after the ETL completes). Power BI will invoke the gateway, which in turn queries SQLite and pushes refreshed data to the cloud dataset.

### 4. Impact of Daily ETL Updates
- The ETL scripts (`backend/etl/load_excel_to_db.py` for full rebuilds, `backend/etl/update_daily_data.py` for incremental loads) always write to the same SQLite database.
- Once the ETL finishes, both Streamlit and Power BI see the new rows instantly because they query the same file.
- Scheduled gateway refreshes should run **after** the ETL job to ensure the freshest snapshot.

### 5. Building Visuals
- Create visuals directly from the `Service Calls` table:
  - **KPIs:** use card visuals for total calls, closed calls, SLA compliance (calculate measure using DAX similar to the Streamlit formula).
  - **Trend charts:** plot `Call Entry Date Time` or `Call Solved Date Time` as the axis with counts as values.
  - **Distributions:** stacked/clustered bar charts for state, model, engineer, and instrument status.
  - **Map:** use `Geo Loc - Lat` and `Geo Loc - Lan` (rename to Latitude/Longitude) in a map visual to mirror the Streamlit map.
  - **Tables:** create interactive tables with slicers for state/model/instrument status.

### 6. Tips
- Add Power Query steps to rename snake_case columns into user-friendly labels if needed.
- Keep the SQLite file on a stable path (prefer a non-OneDrive location for gateway scenarios).
- When the schema evolves (new columns/indexes), refresh in Power BI Desktop to detect the metadata change, then re-publish.



