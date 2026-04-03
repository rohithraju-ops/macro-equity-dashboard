import yfinance as yf

def fetch_sp500(start_date: str = "2000-01-01") -> list[dict]:
    """
    Fetch daily S&P 500 OHLCV data from Yahoo Finance.
    Returns a list of dicts, one per trading day.
    """
    ticker = yf.Ticker("^GSPC")  # ^GSPC is the S&P 500 index symbol
    df = ticker.history(start=start_date)

    cleaned = []
    for timestamp, row in df.iterrows():
        cleaned.append({
            "date":   timestamp.strftime("%Y-%m-%d"),
            "open":   round(float(row["Open"]),   2),
            "high":   round(float(row["High"]),   2),
            "low":    round(float(row["Low"]),     2),
            "close":  round(float(row["Close"]),  2),
            "volume": int(row["Volume"]),
        })

    return cleaned


if __name__ == "__main__":
    print("Fetching S&P 500 data...")
    data = fetch_sp500()
    print(f"  → {len(data)} trading days\n")

    print("First 3 rows:")
    for row in data[:3]:
        print(f"  {row}")

    print("\nLast 3 rows:")
    for row in data[-3:]:
        print(f"  {row}")