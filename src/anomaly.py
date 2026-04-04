import duckdb
import polars as pl
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "macro_equity.db")

def get_connection():
    return duckdb.connect(DB_PATH)


# ── 1. Z-SCORE FLAGGING ───────────────────────────────────────────────────────

def compute_zscore_flags(conn, threshold: float = 2.0) -> pl.DataFrame:
    """
    For each macro indicator, compute a rolling Z-score.
    Flag any reading that is more than `threshold` standard deviations
    from the historical mean up to that point.

    We use an EXPANDING window (all prior rows) so the model only uses
    information available at the time — no lookahead bias.
    """
    result = conn.execute("""
        SELECT
            date,
            series_id,
            value,
            AVG(value) OVER (
                PARTITION BY series_id
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS expanding_mean,
            STDDEV(value) OVER (
                PARTITION BY series_id
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS expanding_std
        FROM macro_indicators
        ORDER BY series_id, date
    """).fetchall()

    df = pl.DataFrame({
        "date":          [r[0] for r in result],
        "series_id":     [r[1] for r in result],
        "value":         [r[2] for r in result],
        "expanding_mean": [r[3] for r in result],
        "expanding_std":  [r[4] for r in result],
    })

    # Compute Z-score in Polars
    df = df.with_columns([
        ((pl.col("value") - pl.col("expanding_mean")) / pl.col("expanding_std"))
        .alias("zscore")
    ])

    # Flag anything beyond the threshold
    df = df.with_columns([
        (pl.col("zscore").abs() > threshold).alias("zscore_flag")
    ])

    # Drop early rows where std is 0 (not enough history yet)
    df = df.filter(pl.col("expanding_std") > 0)

    return df

# ── 1.5. EQUITY Z-SCORE FLAGGING ───────────────────────────────────────────────

def compute_equity_zscore_flags(conn, threshold: float = 2.0) -> pl.DataFrame:
    """
    Compute Z-score flags on S&P 500 daily returns and volume.
    Flags extreme price drops and volume spikes — equity-side anomaly detection.
    """
    result = conn.execute("""
        SELECT
            date,
            close,
            volume,
            (close - LAG(close) OVER (ORDER BY date))
                / LAG(close) OVER (ORDER BY date) AS daily_return
        FROM sp500_prices
        ORDER BY date
    """).fetchall()

    df = pl.DataFrame({
        "date":         [r[0] for r in result],
        "close":        [r[1] for r in result],
        "volume":       [r[2] for r in result],
        "daily_return": [r[3] for r in result],
    }).filter(pl.col("daily_return").is_not_null())

    # Compute expanding mean and std using cumulative sum manually
    df = df.with_columns([
        pl.col("daily_return").cum_sum().alias("cum_sum_return"),
        pl.arange(1, pl.len() + 1).alias("row_num"),
    ])

    df = df.with_columns([
        (pl.col("cum_sum_return") / pl.col("row_num")).alias("expanding_mean"),
    ])

    # Expanding std — compute via variance definition
    df = df.with_columns([
        (pl.col("daily_return") - pl.col("expanding_mean")).alias("deviation")
    ])

    df = df.with_columns([
        pl.col("deviation").pow(2).cum_sum().alias("cum_sq_deviation")
    ])

    df = df.with_columns([
        ((pl.col("cum_sq_deviation") / pl.col("row_num")).sqrt()).alias("expanding_std")
    ])

    # Same for volume
    df = df.with_columns([
        pl.col("volume").cast(pl.Float64).cum_sum().alias("vol_cum_sum"),
    ])

    df = df.with_columns([
        (pl.col("vol_cum_sum") / pl.col("row_num")).alias("vol_expanding_mean"),
    ])

    df = df.with_columns([
        (pl.col("volume").cast(pl.Float64) - pl.col("vol_expanding_mean")).alias("vol_deviation")
    ])

    df = df.with_columns([
        pl.col("vol_deviation").pow(2).cum_sum().alias("vol_cum_sq_dev")
    ])

    df = df.with_columns([
        ((pl.col("vol_cum_sq_dev") / pl.col("row_num")).sqrt()).alias("vol_expanding_std")
    ])

    # Z-scores
    df = df.with_columns([
        ((pl.col("daily_return") - pl.col("expanding_mean")) / pl.col("expanding_std"))
        .alias("return_zscore"),
        ((pl.col("volume").cast(pl.Float64) - pl.col("vol_expanding_mean")) / pl.col("vol_expanding_std"))
        .alias("volume_zscore"),
    ])

    # Flag extreme drops or volume spikes
    df = df.with_columns([
        ((pl.col("return_zscore") < -threshold) |
         (pl.col("volume_zscore") > threshold))
        .alias("equity_flag")
    ])

    return df.filter(pl.col("expanding_std") > 0)

# ── 2. YIELD CURVE EPISODE CLUSTERING ────────────────────────────────────────

def compute_inversion_episodes(conn) -> pl.DataFrame:
    """
    Group contiguous yield curve inversion days into episodes.
    Each episode gets a start date, end date, and duration in days.
    A gap of more than 5 trading days breaks an episode.
    """
    result = conn.execute("""
        SELECT date, value
        FROM macro_indicators
        WHERE series_id = 'T10Y2Y'
        ORDER BY date
    """).fetchall()

    df = pl.DataFrame({
        "date":  [r[0] for r in result],
        "value": [r[1] for r in result],
    })

    # Mark inversion days
    df = df.with_columns([
        (pl.col("value") < 0).alias("inverted")
    ])

    # Cluster into episodes — group consecutive inversion days
    episodes = []
    in_episode = False
    episode_start = None
    last_date = None

    for row in df.iter_rows(named=True):
        current_date = row["date"]

        if row["inverted"]:
            if not in_episode:
                # Start a new episode
                in_episode = True
                episode_start = current_date
            last_date = current_date
        else:
            if in_episode:
                # End the current episode
                duration = (last_date - episode_start).days
                episodes.append({
                    "episode_start": episode_start,
                    "episode_end":   last_date,
                    "duration_days": duration,
                })
                in_episode = False

    # Catch any episode still open at end of data
    if in_episode:
        duration = (last_date - episode_start).days
        episodes.append({
            "episode_start": episode_start,
            "episode_end":   last_date,
            "duration_days": duration,
        })

    return pl.DataFrame(episodes)


# ── 3. CONFLUENCE SCORING ─────────────────────────────────────────────────────

def compute_confluence_scores(zscore_df: pl.DataFrame) -> pl.DataFrame:
    """
    For each date, count how many indicators are simultaneously flagged.
    A higher confluence score = more indicators firing at once = stronger signal.

    Score meaning:
        1 = one indicator flagged       (watch)
        2 = two indicators flagged      (caution)
        3+ = three or more flagged      (high alert)
    """
    # Only keep flagged rows
    flagged = zscore_df.filter(pl.col("zscore_flag") == True)

    # Count flags per date across all indicators
    confluence = (
        flagged
        .group_by("date")
        .agg(pl.len().alias("confluence_score"))
        .sort("date")
    )

    # Label severity
    confluence = confluence.with_columns([
        pl.when(pl.col("confluence_score") >= 3).then(pl.lit("HIGH"))
          .when(pl.col("confluence_score") == 2).then(pl.lit("CAUTION"))
          .otherwise(pl.lit("WATCH"))
          .alias("severity")
    ])

    return confluence


# ── 4. STORE ANOMALY FLAGS INTO DUCKDB ────────────────────────────────────────

def store_anomaly_flags(conn, zscore_df: pl.DataFrame, confluence_df: pl.DataFrame):
    """
    Write anomaly results back into DuckDB as a new table.
    This makes them queryable alongside macro and equity data.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS anomaly_flags (
            date             DATE,
            series_id        VARCHAR,
            value            DOUBLE,
            zscore           DOUBLE,
            zscore_flag      BOOLEAN,
            confluence_score INTEGER,
            severity         VARCHAR,
            PRIMARY KEY (date, series_id)
        )
    """)

    # Join confluence score onto zscore flags
    flagged_only = zscore_df.filter(pl.col("zscore_flag") == True)

    merged = flagged_only.join(confluence_df, on="date", how="left")

    for row in merged.iter_rows(named=True):
        conn.execute("""
            INSERT OR REPLACE INTO anomaly_flags
                (date, series_id, value, zscore, zscore_flag, confluence_score, severity)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            row["date"],
            row["series_id"],
            row["value"],
            row["zscore"],
            row["zscore_flag"],
            row["confluence_score"],
            row["severity"],
        ])

    print(f"  Stored {len(merged)} anomaly flags into DuckDB")


# ── 5. BACK-TEST AGAINST KNOWN CRISIS EVENTS ─────────────────────────────────

def backtest_crisis_events(conn, confluence_df: pl.DataFrame, zscore_df: pl.DataFrame, equity_df: pl.DataFrame):    
    """
    Back-test against known crisis events.
    Checks both confluence (multi-indicator) and single-indicator signals.
    """
    crisis_windows = {
        "2008 Financial Crisis":    ("2006-01-01", "2009-06-01"),
        "COVID Crash (March 2020)": ("2020-01-01", "2020-06-01"),
        "2022 Rate Hike Cycle":     ("2021-12-01", "2022-12-01"),
    }

    # Key indicators to watch per crisis — based on domain knowledge
    crisis_indicators = {
        "2008 Financial Crisis":    ["UNRATE", "T10Y2Y"],
        "COVID Crash (March 2020)": ["UNRATE", "GDP"],
        "2022 Rate Hike Cycle":     ["FEDFUNDS", "CPIAUCSL"],
    }

    print("\n=== Back-test: Crisis Event Detection ===")

    results = []
    for crisis_name, (start, end) in crisis_windows.items():

        # Check confluence (score >= 2)
        hits_confluence = confluence_df.filter(
            (pl.col("date").cast(pl.Utf8) >= start) &
            (pl.col("date").cast(pl.Utf8) <= end)  &
            (pl.col("confluence_score") >= 2)
        )

        # Check single indicator flags for crisis-specific indicators
        key_indicators = crisis_indicators[crisis_name]
        hits_single = zscore_df.filter(
            (pl.col("date").cast(pl.Utf8) >= start) &
            (pl.col("date").cast(pl.Utf8) <= end)  &
            (pl.col("zscore_flag") == True)         &
            (pl.col("series_id").is_in(key_indicators))
        )

        # Check equity flags (price drops + volume spikes)
        hits_equity = equity_df.filter(
            (pl.col("date").cast(pl.Utf8) >= start) &
            (pl.col("date").cast(pl.Utf8) <= end)  &
            (pl.col("equity_flag") == True)
        )

        detected = len(hits_confluence) > 0 or len(hits_single) > 0 or len(hits_equity) > 0

        first_confluence = hits_confluence["date"].min() if len(hits_confluence) > 0 else None
        first_single     = hits_single["date"].min()     if len(hits_single) > 0     else None
        first_equity     = hits_equity["date"].min()     if len(hits_equity) > 0     else None

        all_firsts = [first_confluence, first_single, first_equity]
        candidates = [d for d in all_firsts if d is not None]
        first_flag = min(candidates) if candidates else None

        print(f"\n{crisis_name}:")
        print(f"  Detected              : {'✅ YES' if detected else '❌ NO'}")
        print(f"  First signal          : {first_flag}")
        print(f"  Multi-indicator days  : {len(hits_confluence)}")
        print(f"  Key indicator flags   : {len(hits_single)} ({', '.join(key_indicators)})")
        print(f"  Equity signal days    : {len(hits_equity)}")

        results.append({
            "crisis":          crisis_name,
            "detected":        detected,
            "first_flag":      str(first_flag) if first_flag else "None",
            "confluence_days": len(hits_confluence),
            "single_flags":    len(hits_single),
            "equity_flags":    len(hits_equity),
        })

    return pl.DataFrame(results)


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    conn = get_connection()

    print("=== Layer 1: Z-Score Flagging ===")
    zscore_df = compute_zscore_flags(conn, threshold=2.0)
    total_flags = zscore_df.filter(pl.col("zscore_flag") == True)
    print(f"Total flagged readings: {len(total_flags)}")
    print(f"Breakdown by indicator:")
    print(
        total_flags
        .group_by("series_id")
        .agg(pl.len().alias("flag_count"))
        .sort("flag_count", descending=True)
    )

    print("\n=== Layer 2: Yield Curve Inversion Episodes ===")
    episodes = compute_inversion_episodes(conn)
    print(f"Total inversion episodes: {len(episodes)}")
    print(episodes)

    print("\n=== Layer 3: Confluence Scoring ===")
    confluence_df = compute_confluence_scores(zscore_df)
    print(f"Total multi-indicator alert days: {len(confluence_df)}")
    print("\nHigh severity days:")
    print(confluence_df.filter(pl.col("severity") == "HIGH").head(10))

    print("\n=== Equity Z-Score Flagging ===")
    equity_df = compute_equity_zscore_flags(conn, threshold=2.0)
    equity_flags = equity_df.filter(pl.col("equity_flag") == True)
    print(f"Total equity anomaly days: {len(equity_flags)}")
    print("\nFirst 5 flagged equity days:")
    print(equity_flags.select(["date", "close", "daily_return", "return_zscore"]).head(5))

    print("\n=== Storing Anomaly Flags to DuckDB ===")
    store_anomaly_flags(conn, zscore_df, confluence_df)

    backtest_df = backtest_crisis_events(conn, confluence_df, zscore_df, equity_df)

    print("\n=== Summary ===")
    detected_count = backtest_df.filter(pl.col("detected") == True)
    print(f"Crisis events detected: {len(detected_count)} / 3")

    conn.close()