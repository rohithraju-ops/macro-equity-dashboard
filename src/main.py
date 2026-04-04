from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import duckdb
import os
import time

# ── APP SETUP ─────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Macro Equity Dashboard API",
    description="Serves macroeconomic indicators, S&P 500 data, and anomaly flags",
    version="1.0.0",
)

# CORS — allows the React frontend to call this API from the browser
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten this to your Vercel URL in production
    allow_methods=["GET"],
    allow_headers=["*"],
)

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "macro_equity.db")

def get_db():
    return duckdb.connect(DB_PATH)


# ── PYDANTIC MODELS ───────────────────────────────────────────────────────────
# These define the exact shape of every API response.
# FastAPI uses these to validate output and auto-generate docs.

class MacroPoint(BaseModel):
    date: str
    series_id: str
    value: float

class EquityPoint(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int

class AnomalyFlag(BaseModel):
    date: str
    series_id: str
    value: float
    zscore: float
    confluence_score: Optional[int]
    severity: Optional[str]

class ConfluencePoint(BaseModel):
    date: str
    confluence_score: int
    severity: str

class HealthResponse(BaseModel):
    status: str
    db_connected: bool
    response_time_ms: float


# ── ENDPOINTS ─────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {
        "message": "Macro Equity Dashboard API",
        "docs": "/docs",
        "endpoints": ["/health", "/macro/{series_id}", "/equity", "/anomalies", "/confluence"]
    }


@app.get("/health", response_model=HealthResponse)
def health_check():
    """
    Confirms the API is running and the database is reachable.
    Use this to verify your deployment is live.
    """
    start = time.time()
    try:
        conn = get_db()
        conn.execute("SELECT 1").fetchone()
        conn.close()
        connected = True
    except Exception:
        connected = False

    elapsed_ms = round((time.time() - start) * 1000, 2)

    return HealthResponse(
        status="ok" if connected else "degraded",
        db_connected=connected,
        response_time_ms=elapsed_ms,
    )


@app.get("/macro/{series_id}", response_model=list[MacroPoint])
def get_macro_series(
    series_id: str,
    start_date: Optional[str] = Query(None, description="Filter from this date (YYYY-MM-DD)"),
    end_date:   Optional[str] = Query(None, description="Filter to this date (YYYY-MM-DD)"),
    limit:      Optional[int] = Query(None, description="Max number of rows to return"),
):
    """
    Returns historical data for a single macro indicator.
    Valid series_id values: FEDFUNDS, CPIAUCSL, UNRATE, GDP, T10Y2Y
    """
    valid_series = {"FEDFUNDS", "CPIAUCSL", "UNRATE", "GDP", "T10Y2Y"}
    if series_id.upper() not in valid_series:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid series_id. Must be one of: {', '.join(valid_series)}"
        )

    query = """
        SELECT date::VARCHAR, series_id, value
        FROM macro_indicators
        WHERE series_id = ?
    """
    params = [series_id.upper()]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date"

    if limit:
        query += f" LIMIT {limit}"

    try:
        conn = get_db()
        rows = conn.execute(query, params).fetchall()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return [
        MacroPoint(date=r[0], series_id=r[1], value=r[2])
        for r in rows
    ]


@app.get("/equity", response_model=list[EquityPoint])
def get_equity(
    start_date: Optional[str] = Query(None, description="Filter from this date (YYYY-MM-DD)"),
    end_date:   Optional[str] = Query(None, description="Filter to this date (YYYY-MM-DD)"),
    limit:      Optional[int] = Query(None, description="Max number of rows to return"),
):
    """
    Returns S&P 500 daily OHLCV price history.
    """
    query = """
        SELECT date::VARCHAR, open, high, low, close, volume
        FROM sp500_prices
        WHERE 1=1
    """
    params = []

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date"

    if limit:
        query += f" LIMIT {limit}"

    try:
        conn = get_db()
        rows = conn.execute(query, params).fetchall()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return [
        EquityPoint(
            date=r[0], open=r[1], high=r[2],
            low=r[3], close=r[4], volume=r[5]
        )
        for r in rows
    ]


@app.get("/anomalies", response_model=list[AnomalyFlag])
def get_anomalies(
    series_id:  Optional[str] = Query(None, description="Filter by indicator"),
    severity:   Optional[str] = Query(None, description="Filter by severity: WATCH, CAUTION, HIGH"),
    start_date: Optional[str] = Query(None, description="Filter from this date"),
    end_date:   Optional[str] = Query(None, description="Filter to this date"),
):
    """
    Returns all anomaly flags stored by the anomaly detection module.
    """
    query = """
        SELECT date::VARCHAR, series_id, value, zscore, confluence_score, severity
        FROM anomaly_flags
        WHERE 1=1
    """
    params = []

    if series_id:
        query += " AND series_id = ?"
        params.append(series_id.upper())
    if severity:
        query += " AND severity = ?"
        params.append(severity.upper())
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date"

    try:
        conn = get_db()
        rows = conn.execute(query, params).fetchall()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return [
        AnomalyFlag(
            date=r[0], series_id=r[1], value=r[2],
            zscore=r[3], confluence_score=r[4], severity=r[5]
        )
        for r in rows
    ]


@app.get("/confluence", response_model=list[ConfluencePoint])
def get_confluence(
    severity:   Optional[str] = Query(None, description="Filter by severity: WATCH, CAUTION, HIGH"),
    start_date: Optional[str] = Query(None, description="Filter from this date"),
    end_date:   Optional[str] = Query(None, description="Filter to this date"),
):
    """
    Returns multi-indicator confluence scores by date.
    High confluence = multiple macro indicators flagging simultaneously.
    """
    query = """
        SELECT date::VARCHAR, confluence_score, severity
        FROM anomaly_flags
        WHERE confluence_score IS NOT NULL
    """
    params = []

    if severity:
        query += " AND severity = ?"
        params.append(severity.upper())
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += """
        GROUP BY date, confluence_score, severity
        ORDER BY date
    """

    try:
        conn = get_db()
        rows = conn.execute(query, params).fetchall()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return [
        ConfluencePoint(date=r[0], confluence_score=r[1], severity=r[2])
        for r in rows
    ]