"""로컬 브릿지 — 토스 OHLCV(일봉)를 Supabase ohlcv_cache에 저장하고, 클라우드는 읽기.

토스/KIS 등 한국 증권 API는 해외(클라우드) IP를 차단하므로, 한국 IP인 로컬 PC가
일봉을 받아 DB에 적재한다(daily_runner/수동). 클라우드 앱의 prices.fetch_ohlcv는
토스 직접 호출이 막히면 이 캐시를 읽어 차트를 그린다.

- backfill(tickers): 신규 종목은 풀백필, 기존은 최근분만 증분 upsert
- backfill_board(): 보드에 보이는 종목(high_low_cache ∪ watchlist ∪ MP)을 백필
- get_cached(ticker, period, interval): DB → OHLCV DataFrame (prices.fetch_ohlcv 호환)
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd

from db import connect

log = logging.getLogger(__name__)

_PERIOD_DAYS = {"1d": 1, "5d": 7, "1m": 31, "3m": 95, "6m": 190,
                "1y": 380, "5y": 1860, "max": 365 * 30}
_RESAMPLE = {"1wk": "W", "1mo": "ME"}


def _cached_count(ticker: str) -> int:
    with connect() as conn:
        r = conn.execute("SELECT count(*) AS n FROM ohlcv_cache WHERE ticker = ?",
                         (ticker.upper(),)).fetchone()
    return int(r["n"]) if r else 0


def upsert(ticker: str, df: pd.DataFrame) -> int:
    """df(index=날짜, Open/High/Low/Close/Volume) → ohlcv_cache upsert."""
    if df is None or df.empty:
        return 0
    tk = ticker.upper()
    now = datetime.now().isoformat(timespec="seconds")
    rows = []
    for ts, r in df.iterrows():
        d = pd.Timestamp(ts).strftime("%Y-%m-%d")
        rows.append((tk, d, float(r["Open"]), float(r["High"]), float(r["Low"]),
                     float(r["Close"]), float(r.get("Volume") or 0), now))
    with connect() as conn:
        conn.executemany(
            "INSERT INTO ohlcv_cache (ticker,d,o,h,l,c,v,updated_at) "
            "VALUES (?,?,?,?,?,?,?,?) "
            "ON CONFLICT (ticker,d) DO UPDATE SET o=excluded.o, h=excluded.h, "
            "l=excluded.l, c=excluded.c, v=excluded.v, updated_at=excluded.updated_at",
            rows,
        )
    return len(rows)


def backfill(tickers, full_bars: int = 420, incr_bars: int = 12) -> dict:
    """토스 일봉으로 ohlcv_cache 채우기. 신규(<250행)는 풀백필, 기존은 증분."""
    import toss_market as tm
    if not tm.available():
        return {"ok": False, "msg": "토스 키 미설정"}
    done, skipped, errors, rows = 0, 0, 0, 0
    for tk in tickers:
        tk = (tk or "").strip().upper()
        if not tk or "." in tk:
            skipped += 1
            continue
        try:
            bars = full_bars if _cached_count(tk) < 250 else incr_bars
            df = tm.daily(tk, bars)
            if df.empty:
                skipped += 1
                continue
            rows += upsert(tk, df.tail(bars))
            done += 1
        except Exception as e:
            errors += 1
            log.warning("backfill %s 실패: %s", tk, e)
    return {"ok": True, "done": done, "skipped": skipped, "errors": errors, "rows": rows}


def _board_tickers(limit: int = 600) -> list[str]:
    """차트가 열릴 수 있는 종목 = high_low_cache(최신) ∪ watchlist ∪ MP holdings."""
    tks: set[str] = set()
    with connect() as conn:
        try:
            d = conn.execute("SELECT max(computed_date) AS d FROM high_low_cache").fetchone()
            if d and d["d"]:
                for r in conn.execute(
                    "SELECT ticker FROM high_low_cache WHERE computed_date = ?", (d["d"],)
                ).fetchall():
                    tks.add(r["ticker"])
        except Exception:
            pass
        try:
            for r in conn.execute("SELECT ticker FROM watchlist").fetchall():
                tks.add(r["ticker"])
        except Exception:
            pass
        try:
            for r in conn.execute(
                "SELECT DISTINCT ticker FROM portfolio_transactions"
            ).fetchall():
                tks.add(r["ticker"])
        except Exception:
            pass
    out = [t for t in tks if t and "." not in t]
    return out[:limit]


def backfill_board(limit: int = 600) -> dict:
    """보드 종목 일괄 백필 (로컬 스케줄/수동)."""
    tickers = _board_tickers(limit)
    log.info("ohlcv 백필 대상 %d종목", len(tickers))
    return backfill(tickers)


def get_cached(ticker: str, period: str, interval: str = "1d") -> pd.DataFrame:
    """ohlcv_cache → OHLCV DataFrame (prices.fetch_ohlcv 호환). 인트라데이(period=1d)는
    일봉 캐시라 미지원(빈 DF). 주/월봉은 일봉 리샘플."""
    if period == "1d":
        return pd.DataFrame()      # 인트라데이는 캐시 없음
    tk = (ticker or "").strip().upper()
    with connect() as conn:
        rows = conn.execute(
            "SELECT d, o, h, l, c, v FROM ohlcv_cache WHERE ticker = ? ORDER BY d",
            (tk,),
        ).fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([{
        "dt": pd.Timestamp(r["d"]),
        "Open": r["o"], "High": r["h"], "Low": r["l"], "Close": r["c"],
        "Volume": r["v"],
    } for r in rows]).set_index("dt").sort_index()
    rule = _RESAMPLE.get(interval)
    if rule:
        df = df.resample(rule).agg({"Open": "first", "High": "max", "Low": "min",
                                    "Close": "last", "Volume": "sum"}).dropna(subset=["Close"])
    return df
