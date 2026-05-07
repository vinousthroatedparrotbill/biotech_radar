"""멀티 기간 수익률 + 52주 신고가 계산 — yfinance batch download 활용."""
from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

PERIODS = ("1d", "7d", "1m", "3m", "6m", "1y")
PERIOD_TRADING_DAYS = {"1d": 1, "7d": 5, "1m": 21, "3m": 63, "6m": 126, "1y": 252}


def _pct(now: float | None, then: float | None) -> float | None:
    if now is None or then is None or then == 0:
        return None
    return (now / then - 1) * 100


def compute_snapshot(tickers: list[str], lookback_days: int = 380) -> pd.DataFrame:
    """For each ticker: 52w high/low + 1d/7d/1m/3m/6m/1y returns.
    yfinance batch download로 한 번에 — 200~300종목 처리 가능."""
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    df = yf.download(
        tickers, start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"),
        interval="1d", auto_adjust=True, progress=False, group_by="ticker", threads=True,
    )
    if df is None or df.empty:
        return pd.DataFrame()

    rows = []
    today = end.strftime("%Y-%m-%d")
    for tk in tickers:
        try:
            sub = df[tk] if isinstance(df.columns, pd.MultiIndex) else df
        except KeyError:
            continue
        if sub is None or len(sub) == 0:
            continue
        closes = sub["Close"].dropna()
        highs = sub["High"].dropna()
        lows = sub["Low"].dropna()
        if closes.empty:
            continue
        last_close = float(closes.iloc[-1])
        last_high = float(highs.iloc[-1]) if not highs.empty else None
        last_low = float(lows.iloc[-1]) if not lows.empty else None
        high_52w = float(highs.tail(252).max()) if not highs.empty else None
        low_52w = float(lows.tail(252).min()) if not lows.empty else None

        def back(n: int) -> float | None:
            if len(closes) <= n:
                return None
            return float(closes.iloc[-1 - n])

        rows.append({
            "ticker": tk, "date": today,
            "today_close": last_close, "today_high": last_high, "today_low": last_low,
            "high_52w": high_52w, "low_52w": low_52w,
            "perf_1d": _pct(last_close, back(1)),
            "perf_7d": _pct(last_close, back(5)),
            "perf_1m": _pct(last_close, back(21)),
            "perf_3m": _pct(last_close, back(63)),
            "perf_6m": _pct(last_close, back(126)),
            "perf_1y": _pct(last_close, back(252)),
        })
    return pd.DataFrame(rows)
