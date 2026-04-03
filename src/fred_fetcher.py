import requests
import os
from dotenv import load_dotenv

load_dotenv()
FRED_API_KEY = os.getenv("FRED_API_KEY")
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

SERIES = {
    "FEDFUNDS": "Federal Funds Rate",
    "CPIAUCSL": "Consumer Price Index",
    "UNRATE":   "Unemployment Rate",
    "GDP":      "Gross Domestic Product",
    "T10Y2Y":   "10Y-2Y Treasury Yield Spread",
}

def fetch_series(series_id: str, start_date: str = "2000-01-01") -> list[dict]:
    """
    Fetch all observations for a single FRED series.
    Returns a list of dicts: [{"date": "2000-01-01", "value": 6.5}, ...]
    """
    params = {
        "series_id":        series_id,
        "api_key":          FRED_API_KEY,
        "file_type":        "json",
        "observation_start": start_date,
    }

    response = requests.get(FRED_BASE_URL, params=params)
    response.raise_for_status()  # crash loudly if the API call fails

    observations = response.json()["observations"]

    cleaned = []
    for obs in observations:
        if obs["value"] == ".":      # FRED uses "." for missing data
            continue
        cleaned.append({
            "date":      obs["date"],
            "series_id": series_id,
            "value":     float(obs["value"]),
        })

    return cleaned


def fetch_all_series(start_date: str = "2000-01-01") -> dict[str, list[dict]]:
    """
    Loop through every series in SERIES dict, fetch each one.
    Returns a dict keyed by series_id:
    {
        "FEDFUNDS": [{"date": ..., "series_id": ..., "value": ...}, ...],
        "CPIAUCSL": [...],
        ...
    }
    """
    all_data = {}

    for series_id, label in SERIES.items():
        print(f"Fetching {label} ({series_id})...")
        all_data[series_id] = fetch_series(series_id, start_date)
        print(f"  → {len(all_data[series_id])} observations")

    return all_data


if __name__ == "__main__":
    data = fetch_all_series()

    # Quick sanity check — print the first 3 rows of each series
    for series_id, rows in data.items():
        print(f"\n{series_id} — first 3 rows:")
        for row in rows[:3]:
            print(f"  {row}")