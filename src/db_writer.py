import duckdb
import os
from fred_fetcher import fetch_all_series
from equity_fetcher import fetch_sp500

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "macro_equity.db")

def get_connection():
    """
    Open (or create) the DuckDB database file.
    DuckDB creates the file automatically if it doesn't exist yet.
    """
    return duckdb.connect(DB_PATH)


def create_tables(conn):
    """
    Define the schema — what tables exist and what columns they have.
    IF NOT EXISTS means re-running this file won't wipe your data.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS macro_indicators (
            date      DATE,
            series_id VARCHAR,
            value     DOUBLE,
            PRIMARY KEY (date, series_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS sp500_prices (
            date   DATE PRIMARY KEY,
            open   DOUBLE,
            high   DOUBLE,
            low    DOUBLE,
            close  DOUBLE,
            volume BIGINT
        )
    """)

    print("Tables ready.")


def load_macro_data(conn):
    """
    Fetch all FRED series and insert into macro_indicators.
    INSERT OR REPLACE means re-running won't create duplicate rows.
    """
    all_data = fetch_all_series()

    total = 0
    for series_id, rows in all_data.items():
        for row in rows:
            conn.execute("""
                INSERT OR REPLACE INTO macro_indicators (date, series_id, value)
                VALUES (?, ?, ?)
            """, [row["date"], row["series_id"], row["value"]])
        total += len(rows)
        print(f"  Loaded {len(rows)} rows for {series_id}")

    print(f"  Total macro rows: {total}")


def load_equity_data(conn):
    """
    Fetch S&P 500 data and insert into sp500_prices.
    """
    rows = fetch_sp500()

    for row in rows:
        conn.execute("""
            INSERT OR REPLACE INTO sp500_prices (date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [row["date"], row["open"], row["high"], row["low"], row["close"], row["volume"]])

    print(f"  Loaded {len(rows)} rows for S&P 500")


def verify(conn):
    """
    Quick sanity check — print row counts and a few sample rows from each table.
    """
    print("\n--- Verification ---")

    count = conn.execute("SELECT COUNT(*) FROM macro_indicators").fetchone()[0]
    print(f"macro_indicators: {count} rows")

    count = conn.execute("SELECT COUNT(*) FROM sp500_prices").fetchone()[0]
    print(f"sp500_prices: {count} rows")

    print("\nSample macro rows:")
    rows = conn.execute("""
        SELECT date, series_id, value
        FROM macro_indicators
        ORDER BY date
        LIMIT 5
    """).fetchall()
    for row in rows:
        print(f"  {row}")

    print("\nSample S&P 500 rows:")
    rows = conn.execute("""
        SELECT date, close, volume
        FROM sp500_prices
        ORDER BY date
        LIMIT 5
    """).fetchall()
    for row in rows:
        print(f"  {row}")


if __name__ == "__main__":
    conn = get_connection()
    create_tables(conn)

    print("\nLoading macro data...")
    load_macro_data(conn)

    print("\nLoading equity data...")
    load_equity_data(conn)

    verify(conn)
    conn.close()
    print("\nDone. Database saved to data/macro_equity.db")