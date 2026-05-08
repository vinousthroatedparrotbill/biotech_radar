"""52주 신고가 + 수익률 일괄 캐시."""
from __future__ import annotations

from datetime import datetime

import pandas as pd

from db import connect, pd_read_sql
from perf import compute_snapshot
from universe import get_universe


def collect(industry_filter: str | None = "Biotechnology") -> int:
    """52w high + 멀티 기간 수익률 계산.
    범위 = (mcap ≥ $1.5B) ∪ (관심종목 등록된 ticker). 작은 종목은 watchlist로만 추적."""
    universe = get_universe(industry_filter=industry_filter)
    if universe.empty:
        raise RuntimeError("ticker_master 비어있음. 먼저 universe 갱신.")

    # watchlist 등록된 ticker 합치기 (mcap 조건 무시하고 강제 포함)
    watch_tickers: set[str] = set()
    try:
        with connect() as conn:
            watch_tickers = {
                r["ticker"] for r in
                conn.execute("SELECT ticker FROM watchlist").fetchall()
            }
    except Exception:
        pass

    mask = (universe["market_cap"] >= 1500.0) | (universe["ticker"].isin(watch_tickers))
    universe = universe[mask].reset_index(drop=True)
    tickers = universe["ticker"].tolist()
    mcap_map = dict(zip(universe["ticker"], universe["market_cap"]))

    # yfinance batch — 한 번에 처리 가능한 chunk 단위로 (큰 universe도 안정적으로)
    CHUNK = 100
    all_frames = []
    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i:i + CHUNK]
        df = compute_snapshot(chunk)
        if not df.empty:
            all_frames.append(df)
    if not all_frames:
        return 0
    snap = pd.concat(all_frames, ignore_index=True)

    snap["market_cap"] = snap["ticker"].map(mcap_map)
    today = datetime.now().strftime("%Y-%m-%d")

    rows = [
        (r["ticker"], today,
         r.get("high_52w"), r.get("low_52w"),
         r.get("today_high"), r.get("today_low"), r.get("today_close"),
         r.get("market_cap"),
         r.get("perf_1d"), r.get("perf_7d"), r.get("perf_1m"),
         r.get("perf_3m"), r.get("perf_6m"), r.get("perf_1y"))
        for _, r in snap.iterrows()
    ]
    with connect() as conn:
        conn.executemany(
            """
            INSERT INTO high_low_cache
              (ticker, computed_date, high_52w, low_52w, today_high, today_low,
               today_close, market_cap, perf_1d, perf_7d, perf_1m, perf_3m, perf_6m, perf_1y)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(ticker, computed_date) DO UPDATE SET
                high_52w=excluded.high_52w, low_52w=excluded.low_52w,
                today_high=excluded.today_high, today_low=excluded.today_low,
                today_close=excluded.today_close,
                market_cap=excluded.market_cap,
                perf_1d=excluded.perf_1d, perf_7d=excluded.perf_7d,
                perf_1m=excluded.perf_1m, perf_3m=excluded.perf_3m,
                perf_6m=excluded.perf_6m, perf_1y=excluded.perf_1y
            """,
            rows,
        )
        conn.commit()
    return len(rows)


def fetch_new_highs(direction: str = "high", limit: int = 500) -> pd.DataFrame:
    """오늘 52w 신고가/신저가 종목 — ticker_master와 join하여 회사명 포함."""
    with connect() as conn:
        latest = conn.execute(
            "SELECT MAX(computed_date) AS d FROM high_low_cache"
        ).fetchone()["d"]
        if not latest:
            return pd.DataFrame()
        if direction == "high":
            cond = "h.today_high >= h.high_52w * 0.999"
            order = "(h.today_close / NULLIF(h.high_52w,0)) DESC"
        else:
            cond = "h.today_low <= h.low_52w * 1.001"
            order = "(h.today_close / NULLIF(h.low_52w,0)) ASC"
    df = pd_read_sql(
        f"""
        SELECT h.ticker, t.name, t.industry, h.today_close AS close,
               h.high_52w, h.low_52w, h.market_cap,
               h.perf_1d, h.perf_7d, h.perf_1m, h.perf_3m, h.perf_6m, h.perf_1y
        FROM high_low_cache h
        LEFT JOIN ticker_master t ON t.ticker = h.ticker
        WHERE h.computed_date = %s
          AND h.market_cap >= 1500
          AND {cond}
        ORDER BY {order}
        LIMIT %s
        """,
        params=(latest, limit),
    )
    return df


def latest_run_date() -> str | None:
    with connect() as conn:
        r = conn.execute("SELECT MAX(computed_date) AS d FROM high_low_cache").fetchone()
        return r["d"] if r else None


def fetch_new_today_highs(limit: int = 200) -> pd.DataFrame:
    """오늘 새로 52주 신고가를 찍은 종목들 (전일 대비 high_52w 상승).
    조건:
      - 오늘 today_high >= high_52w * 0.999 (오늘 신고가)
      - 전일 row가 없거나 (첫 기록), 전일 high_52w 보다 오늘 high_52w가 높음
    """
    with connect() as conn:
        dates = conn.execute(
            "SELECT DISTINCT computed_date FROM high_low_cache ORDER BY computed_date DESC LIMIT 2"
        ).fetchall()
        if not dates:
            return pd.DataFrame()
        today = dates[0]["computed_date"]
        yesterday = dates[1]["computed_date"] if len(dates) > 1 else None

        if yesterday:
            sql = """
                SELECT t.ticker, m.name, m.industry, t.today_close AS close,
                       t.high_52w, t.low_52w, t.market_cap,
                       t.perf_1d, t.perf_7d, t.perf_1m, t.perf_3m, t.perf_6m, t.perf_1y,
                       y.high_52w AS prev_high_52w
                FROM high_low_cache t
                LEFT JOIN high_low_cache y
                  ON y.ticker = t.ticker AND y.computed_date = %s
                LEFT JOIN ticker_master m ON m.ticker = t.ticker
                WHERE t.computed_date = %s
                  AND t.market_cap >= 1500
                  AND t.today_high >= t.high_52w * 0.999
                  AND (y.high_52w IS NULL OR t.high_52w > y.high_52w)
                ORDER BY (t.today_close / NULLIF(t.high_52w,0)) DESC
                LIMIT %s
            """
            params = (yesterday, today, limit)
        else:
            # 어제 스냅샷 없음 — 오늘 신고가 전부를 신규로 표시
            sql = """
                SELECT t.ticker, m.name, m.industry, t.today_close AS close,
                       t.high_52w, t.low_52w, t.market_cap,
                       t.perf_1d, t.perf_7d, t.perf_1m, t.perf_3m, t.perf_6m, t.perf_1y,
                       NULL AS prev_high_52w
                FROM high_low_cache t
                LEFT JOIN ticker_master m ON m.ticker = t.ticker
                WHERE t.computed_date = %s
                  AND t.market_cap >= 1500
                  AND t.today_high >= t.high_52w * 0.999
                ORDER BY (t.today_close / NULLIF(t.high_52w,0)) DESC
                LIMIT %s
            """
            params = (today, limit)

    df = pd_read_sql(sql, params=params)
    return df
