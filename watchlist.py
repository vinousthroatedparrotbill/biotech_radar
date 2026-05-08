"""관심종목 CRUD."""
from __future__ import annotations

from datetime import datetime

import pandas as pd

from db import connect, pd_read_sql


def add(ticker: str) -> None:
    ticker = ticker.strip().upper()
    if not ticker:
        return
    with connect() as conn:
        conn.execute(
            "INSERT INTO watchlist (ticker, added_at) VALUES (?, ?) "
            "ON CONFLICT (ticker) DO NOTHING",
            (ticker, datetime.now().isoformat(timespec="seconds")),
        )
    # 즉시 가격 스냅샷 fetch — 다음 collect 안 기다리고도 보이게
    try:
        from collectors.high_low import collect_tickers
        collect_tickers([ticker])
    except Exception:
        pass   # 실패해도 watchlist 등록 자체는 성공


def remove(ticker: str) -> None:
    with connect() as conn:
        conn.execute("DELETE FROM watchlist WHERE ticker = ?", (ticker.upper(),))


def is_watched(ticker: str) -> bool:
    with connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM watchlist WHERE ticker = ?", (ticker.upper(),)
        ).fetchone()
        return row is not None


def list_all() -> pd.DataFrame:
    """관심종목 + ticker_master + 최근 high_low_cache(있으면) join.
    Columns: ticker, name, industry, market_cap, close, change_pct, high_52w, added_at, perf_*."""
    return pd_read_sql(
        """
        WITH latest AS (
            SELECT ticker, MAX(computed_date) AS d
            FROM high_low_cache GROUP BY ticker
        )
        SELECT w.ticker, t.name, t.industry, t.market_cap,
               h.today_close AS close, h.high_52w, h.low_52w,
               h.perf_1d, h.perf_7d, h.perf_1m, h.perf_3m, h.perf_6m, h.perf_1y,
               w.added_at
        FROM watchlist w
        LEFT JOIN ticker_master t ON t.ticker = w.ticker
        LEFT JOIN latest l ON l.ticker = w.ticker
        LEFT JOIN high_low_cache h
               ON h.ticker = w.ticker AND h.computed_date = l.d
        ORDER BY w.added_at DESC
        """
    )
