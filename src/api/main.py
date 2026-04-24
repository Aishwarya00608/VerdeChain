"""
verdechain/api/main.py
───────────────────────
FastAPI application — REST endpoints + WebSocket for live dashboard.
"""

from fastapi import (
    FastAPI,
    UploadFile,
    File,
    HTTPException,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import tempfile
import os  
from pathlib import Path

from src.ingestion.parser import FreightDataParser
from src.optimization.engine import rank_substitutions
from src.forecasting.predictor import forecast_budget
from src.alerts.router import AlertRouter, classify, CarbonAlert, AlertLevel

from src.notifyer import send_dynamic_alert

app = FastAPI(
    title="VerdeChain API",
    description="Automated ESG Logistics Intelligence Platform",
    version="2.1.0",
)

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# In-memory state (replace with TimescaleDB in production)
_sessions: dict = {}
_ws_clients: List[WebSocket] = []
alert_router = AlertRouter(config={})  # Load from config in production


# ── WebSocket broadcast ───────────────────────────────────────────────────────


async def broadcast(msg: dict):
    dead = []
    for ws in _ws_clients:
        try:
            await ws.send_json(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        _ws_clients.remove(ws)


@app.websocket("/ws/live")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    _ws_clients.append(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        _ws_clients.remove(ws)


# ── File upload + analysis ────────────────────────────────────────────────────


@app.post("/api/ingest")
async def ingest_files(files: List[UploadFile] = File(...)):
    """
    Upload CSV, JSON, or PDF freight data files.
    Returns parsed records and CO2e summary.
    """
    parser = FreightDataParser()
    results = []

    for file in files:
        ext = Path(file.filename).suffix.lower()
        if ext not in {".csv", ".json", ".pdf"}:
            raise HTTPException(400, f"Unsupported format: {ext}")

        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name

        try:
            added = parser.ingest_file(tmp_path)
            results.append({"file": file.filename, "records_added": added})
        finally:
            os.unlink(tmp_path)

    summary = parser.summary()
    df = parser.to_dataframe()

    # Broadcast to dashboard
    await broadcast({"event": "ingestion_complete", "summary": summary})

    return {
        "files_processed": len(files),
        "summary": summary,
        "files": results,
        "records": df.to_dict(orient="records") if not df.empty else [],
    }


# ── Optimization ──────────────────────────────────────────────────────────────


@app.get("/api/optimize/{route_id}")
def optimize_route(
    route_id: str, mode: str = "road", fuel: str = "diesel", threshold_pct: float = 0.0
):
    """Rank modal shift substitutions for a route."""
    subs = rank_substitutions(route_id, mode, fuel, threshold_pct)
    return {"route_id": route_id, "substitutions": [vars(s) for s in subs]}


# ── Forecasting ───────────────────────────────────────────────────────────────


@app.post("/api/forecast/{route_id}")
def forecast_route(
    route_id: str, daily_emissions: List[float], monthly_budget: float = 100.0
):
    """Forecast 30-day carbon trajectory and detect breach risk."""
    result = forecast_budget(route_id, daily_emissions, monthly_budget)
    return vars(result)


# ── Alerts ────────────────────────────────────────────────────────────────────


@app.post("/api/alert/check")
async def check_and_alert(
    route_id: str,
    threshold_pct: float,
    route_name: str = "",
    co2e: float = 0,
    budget: float = 100,
    forecast_days: int = None,
):
    """Classify alert level and dispatch notifications if threshold crossed."""
    level = classify(threshold_pct)
    if level == AlertLevel.NORMAL:
        return {"route_id": route_id, "level": "NORMAL", "dispatched": False}

    alert = CarbonAlert(
        route_id=route_id,
        route_name=route_name or route_id,
        threshold_pct=threshold_pct,
        level=level,
        co2e_tonnes=co2e,
        budget_tonnes=budget,
        top_substitution=None,
        forecast_breach_days=forecast_days,
    )
    result = alert_router.dispatch(alert)
    await broadcast({"event": "alert", "data": alert.to_dict()})
    return result


# ── Health check ─────────────────────────────────────────────────────────────


@app.get("/health")
def health():
    return {"status": "nominal", "version": "2.1.0", "system": "VerdeChain"}


@app.post("/shipments/{shipment_id}/alert")
async def trigger_shipment_alert(shipment_id: str, issue: str, user_email: str):
    # This function is called when the system detects a problem

    # 1. Logic to log the error to TimescaleDB goes here...

    # 2. Trigger the actual email alert
    success = send_dynamic_alert(user_email, shipment_id, issue)

    if success:
        return {"message": f"Alert sent to {user_email}"}
    else:
        return {"error": "Failed to send email notification"}
