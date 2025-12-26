from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.api.routes import calls

app = FastAPI(
    title="Service Calls API",
    description="REST API for querying service call performance data and KPIs.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(calls.router)


@app.get("/")
def healthcheck():
    return {"message": "Service Calls API is running"}




