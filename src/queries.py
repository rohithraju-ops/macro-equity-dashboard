import duckdb
import polars as pl
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "macro_equity.db")

def get_connection():
    return duckdb.connect(DB_PATH)


# ── 1. ROLLING AVERAGE ────────────────────────────────────────────────────────

def query_rolling_avg(conn, series_id: str, window: int = 12) -> pl.DataFrame:
    """
    Compute a rolling average for a macro indicator.
    Default window = 12 months (1 year smoothing).
    """
    result = conn.execute(f"""
        SELECT
            date,
            series_id,
            value,
            AVG(value) OVER (
                PARTITION BY series_id
                ORDER BY date
                ROWS BETWEEN {window - 1} PRECEDING AND CURRENT ROW
            ) AS rolling_avg
        FROM macro_indicators
        WHERE series_id = ?
        ORDER BY date
    """, [series_id]).fetchall()

    return pl.DataFrame({
        "date":        [r[0] for r in result],
        "series_id":   [r[1] for r in result],
        "value":       [r[2] for r in result],
        "rolling_avg": [r[3] for r in result],
    })


# ── 2. YIELD CURVE INVERSIONS ─────────────────────────────────────────────────

def query_yield_curve_inversions(conn) -> pl.DataFrame:
    """
    Find all dates where T10Y2Y went negative — a recession warning signal.
    """
    result = conn.execute("""
        SELECT date, value
        FROM macro_indicators
        WHERE series_id = 'T10Y2Y'
          AND value < 0
        ORDER BY date
    """).fetchall()

    return pl.DataFrame({
        "date":  [r[0] for r in result],
        "value": [r[1] for r in result],
    })


# ── 3. RATE OF CHANGE ─────────────────────────────────────────────────────────

def query_rate_of_change(conn, series_id: str) -> pl.DataFrame:
    """
    Month-over-month change for a macro indicator.
    Useful for spotting sudden moves in rates, inflation, unemployment.
    """
    result = conn.execute("""
        SELECT
            date,
            series_id,
            value,
            value - LAG(value) OVER (
                PARTITION BY series_id ORDER BY date
            ) AS mom_change
        FROM macro_indicators
        WHERE series_id = ?
        ORDER BY date
    """, [series_id]).fetchall()

    df = pl.DataFrame({
        "date":       [r[0] for r in result],
        "series_id":  [r[1] for r in result],
        "value":      [r[2] for r in result],
        "mom_change": [r[3] for r in result],
    })

    # Drop the first row — it has no previous value so mom_change is null
    return df.filter(pl.col("mom_change").is_not_null())


# ── 4. EQUITY + MACRO JOINED ──────────────────────────────────────────────────

def query_macro_vs_equity(conn, series_id: str) -> pl.DataFrame:
    """
    Join a macro indicator with S&P 500 monthly close price.
    Uses the last trading day of each month as the monthly equity price.
    """
    result = conn.execute("""
        WITH monthly_equity AS (
            SELECT
                DATE_TRUNC('month', date) AS month,
                LAST(close ORDER BY date)  AS monthly_close
            FROM sp500_prices
            GROUP BY DATE_TRUNC('month', date)
        )
        SELECT
            m.date,
            m.series_id,
            m.value        AS macro_value,
            e.monthly_close AS sp500_close
        FROM macro_indicators m
        JOIN monthly_equity e
          ON DATE_TRUNC('month', m.date) = e.month
        WHERE m.series_id = ?
        ORDER BY m.date
    """, [series_id]).fetchall()

    return pl.DataFrame({
        "date":        [r[0] for r in result],
        "series_id":   [r[1] for r in result],
        "macro_value": [r[2] for r in result],
        "sp500_close": [r[3] for r in result],
    })


# ── 5. POLARS MANIPULATION ────────────────────────────────────────────────────

def compute_correlation(df: pl.DataFrame) -> float:
    """
    Given the output of query_macro_vs_equity,
    compute the Pearson correlation between the macro indicator and S&P 500.
    """
    return df.select(
        pl.corr("macro_value", "sp500_close")
    ).item()


# ── MAIN: RUNNING ALL QUERIES ─────────────────────────────────────────────────────

if __name__ == "__main__":
    conn = get_connection()

    # 1. Rolling average — Fed Funds Rate
    print("=== 12-Month Rolling Avg — FEDFUNDS ===")
    df = query_rolling_avg(conn, "FEDFUNDS")
    print(df.tail(5))

    # 2. Yield curve inversions
    print("\n=== Yield Curve Inversions (T10Y2Y < 0) ===")
    df_inv = query_yield_curve_inversions(conn)
    print(f"Total inversion days: {len(df_inv)}")
    print(df_inv.head(5))

    # 3. Rate of change — CPI
    print("\n=== Month-over-Month Change — CPIAUCSL ===")
    df_roc = query_rate_of_change(conn, "CPIAUCSL")
    print(df_roc.tail(5))

    # 4. Macro vs equity join
    print("\n=== FEDFUNDS vs S&P 500 (last 5 rows) ===")
    df_join = query_macro_vs_equity(conn, "FEDFUNDS")
    print(df_join.tail(5))

    # 5. Correlation
    corr = compute_correlation(df_join)
    print(f"\nCorrelation between FEDFUNDS and S&P 500 close: {corr:.4f}")

    conn.close()