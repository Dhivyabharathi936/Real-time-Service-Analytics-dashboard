CREATE TABLE IF NOT EXISTS service_calls (
    call_id INTEGER PRIMARY KEY,
    customer_name TEXT,
    address TEXT,
    state TEXT,
    geo_loc_lat FLOAT,
    geo_loc_lon FLOAT,
    geo_loc_pincode TEXT,
    model TEXT,
    instrument_serial_no TEXT,
    warranty_expiry_date DATE,
    zone TEXT,
    priority TEXT,
    visited_engineer_name TEXT,
    ticket_no INTEGER,
    call_entry_datetime DATE,
    start_call_datetime DATE,
    call_solved_datetime DATE,
    call_aging TEXT,
    response_time TEXT,
    recovery_time TEXT,
    customer_complaint TEXT,
    call_type TEXT,
    nature_of_complaint TEXT,
    complaint_description TEXT,
    call_status TEXT,
    status TEXT,
    visitor_remarks TEXT,
    forward_employee_name TEXT,
    instrument_status TEXT
);

CREATE INDEX IF NOT EXISTS idx_service_calls_state
    ON service_calls (state);

CREATE INDEX IF NOT EXISTS idx_service_calls_model
    ON service_calls (model);

CREATE INDEX IF NOT EXISTS idx_service_calls_engineer
    ON service_calls (visited_engineer_name);

CREATE INDEX IF NOT EXISTS idx_service_calls_call_entry
    ON service_calls (call_entry_datetime);

CREATE INDEX IF NOT EXISTS idx_service_calls_call_solved
    ON service_calls (call_solved_datetime);




