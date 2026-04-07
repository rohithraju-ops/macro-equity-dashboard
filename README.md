# Macro-Equity Dashboard

A full-stack macroeconomic and equity analytics dashboard that pulls live data from the FRED API and Yahoo Finance, stores it in a DuckDB database, and serves it through a FastAPI backend to a React frontend deployed on Vercel.

**Live demo:** https://macro-equity-dashboard-jzfj.vercel.app

---

## What it does

- Fetches key macroeconomic indicators from the FRED API — Fed Funds Rate, CPI, unemployment, GDP, and the 10Y-2Y yield spread
- Fetches S&P 500 equity data via Yahoo Finance
- Stores and queries all data using DuckDB + Polars for fast in-process analytics
- Runs anomaly detection on indicator time series to flag unusual movements
- Serves data through a FastAPI REST backend
- Visualizes everything in a React frontend with live charts

---

## Stack

| Layer | Technology |
|---|---|
| Data ingestion | FRED API, Yahoo Finance (yfinance) |
| Storage & queries | DuckDB, Polars |
| Anomaly detection | Custom statistical flagging (anomaly.py) |
| Backend API | FastAPI, Uvicorn |
| Frontend | React, deployed on Vercel |
| Environment | Python 3.13, dotenv |

---

## Project structure

```
macro-equity-dashboard/
├── data/
│   └── macro_equity.db        # DuckDB database
├── src/
│   ├── fred_fetcher.py        # Pulls macro indicators from FRED API
│   ├── equity_fetcher.py      # Pulls S&P 500 data from Yahoo Finance
│   ├── db_writer.py           # Writes fetched data into DuckDB
│   ├── queries.py             # Query layer for the API
│   ├── anomaly.py             # Anomaly detection on time series
│   └── main.py                # FastAPI app entry point
├── frontend/
│   └── src/                   # React app
├── requirements.txt
└── Procfile
```

---

## Running locally

```bash
# Clone the repo
git clone https://github.com/rohithraju-ops/macro-equity-dashboard.git
cd macro-equity-dashboard

# Set up virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Add your FRED API key
echo "FRED_API_KEY=your_key_here" > .env

# Run the backend
uvicorn src.main:app --reload

# In a separate terminal, run the frontend
cd frontend
npm install
npm start
```

Get a free FRED API key at https://fred.stlouisfed.org/docs/api/api_key.html

---

## Key indicators tracked

- **Fed Funds Rate** — monetary policy benchmark
- **CPI (YoY)** — inflation measure
- **Unemployment Rate** — labor market health
- **GDP Growth** — economic output
- **10Y-2Y Yield Spread** — recession early-warning signal
- **S&P 500** — equity market benchmark

---

## Author

Rohith Raju — DS/ML learner building production-grade data projects  
GitHub: [rohithraju-ops](https://github.com/rohithraju-ops)
