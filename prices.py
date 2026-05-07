"""yfinance OHLCV + 이평선 + 일/주/월봉 변환."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Literal

import pandas as pd
import yfinance as yf

Interval = Literal["1d", "1wk", "1mo"]
PeriodKey = Literal["1d", "5d", "1m", "3m", "6m", "1y", "5y", "max"]
PERIOD_LABELS: dict[PeriodKey, str] = {
    "1d": "1일", "5d": "5일", "1m": "1달", "3m": "3달",
    "6m": "6달", "1y": "1년", "5y": "5년", "max": "최대",
}
PERIOD_DAYS: dict[PeriodKey, int] = {
    "1d": 1, "5d": 7, "1m": 31, "3m": 95,
    "6m": 190, "1y": 380, "5y": 1860, "max": 365 * 30,
}


def fetch_ohlcv(ticker: str, period: PeriodKey, interval: Interval = "1d") -> pd.DataFrame:
    """OHLCV DataFrame indexed by date.
    interval: 1d/1wk/1mo. period: 1d~5y.
    1d period + 1d interval은 단일 점이므로 yfinance가 1m 분봉으로 자동 처리(1d period면 1m default).
    """
    end = datetime.now()
    if period == "1d":
        df = yf.download(ticker, period="1d", interval="5m",
                         auto_adjust=True, progress=False)
    else:
        days = PERIOD_DAYS[period]
        # 이평선 계산 위해 추가 buffer 120 거래일
        start = end - timedelta(days=days + 250)
        df = yf.download(ticker, start=start.strftime("%Y-%m-%d"),
                         end=end.strftime("%Y-%m-%d"),
                         interval=interval, auto_adjust=True, progress=False)
    if df is None or df.empty:
        return pd.DataFrame()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.rename(columns={"Open": "Open", "High": "High", "Low": "Low",
                            "Close": "Close", "Volume": "Volume"})
    return df


def add_moving_averages(df: pd.DataFrame, windows: tuple[int, ...] = (20, 60, 120)) -> pd.DataFrame:
    """Add MA columns (MA20, MA60, MA120). Computed before slicing."""
    out = df.copy()
    if "Close" not in out.columns or out.empty:
        return out
    for w in windows:
        out[f"MA{w}"] = out["Close"].rolling(window=w, min_periods=1).mean()
    return out


def trim_to_period(df: pd.DataFrame, period: PeriodKey) -> pd.DataFrame:
    """이평선 계산용 buffer 잘라서 표시 기간만 남김."""
    if df.empty or period == "1d":
        return df
    days = PERIOD_DAYS[period]
    cutoff = datetime.now() - timedelta(days=days)
    return df[df.index >= pd.Timestamp(cutoff)]


def fetch_chart(ticker: str, period: PeriodKey, interval: Interval) -> pd.DataFrame:
    """fetch + MA + trim — UI에서 직접 호출."""
    raw = fetch_ohlcv(ticker, period, interval)
    if raw.empty:
        return raw
    with_ma = add_moving_averages(raw)
    return trim_to_period(with_ma, period)
