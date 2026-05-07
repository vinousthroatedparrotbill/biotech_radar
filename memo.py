"""메모 CRUD — created_at/updated_at 자동."""
from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

from db import connect


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def add(ticker: str, body: str) -> int:
    if not body.strip():
        raise ValueError("empty memo")
    now = _now()
    with connect() as conn:
        cur = conn.execute(
            "INSERT INTO memos (ticker, body, created_at, updated_at) "
            "VALUES (?,?,?,?) RETURNING id",
            (ticker, body.strip(), now, now),
        )
        row = cur.fetchone()
        return int(row["id"])


def update(memo_id: int, body: str) -> None:
    if not body.strip():
        raise ValueError("empty memo")
    with connect() as conn:
        conn.execute(
            "UPDATE memos SET body=?, updated_at=? WHERE id=?",
            (body.strip(), _now(), memo_id),
        )
        conn.commit()


def delete(memo_id: int) -> None:
    with connect() as conn:
        conn.execute("DELETE FROM memos WHERE id=?", (memo_id,))
        conn.commit()


def list_for(ticker: str) -> list[dict]:
    """최신순."""
    with connect() as conn:
        rows = conn.execute(
            "SELECT id, body, created_at, updated_at FROM memos "
            "WHERE ticker=? ORDER BY created_at DESC",
            (ticker,),
        ).fetchall()
        return [dict(r) for r in rows]


def timeline(limit: int = 50) -> list[dict]:
    """전체 메모 타임라인 (최신순) + 작성 후 주가 변동.
    yfinance auto_adjust=True로 액면분할·배당 자동 보정 (유상증자는 시장가 기준 그대로)."""
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT m.id, m.ticker, m.body, m.created_at, m.updated_at, t.name
            FROM memos m
            LEFT JOIN ticker_master t ON t.ticker = m.ticker
            ORDER BY m.created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    memos = [dict(r) for r in rows]
    if not memos:
        return []

    # batch fetch — 메모에 등장한 unique tickers 한 번에
    tickers = sorted({m["ticker"] for m in memos})
    end = datetime.now()
    earliest = min(pd.to_datetime(m["created_at"]) for m in memos)
    start = (earliest - timedelta(days=10))   # buffer
    try:
        df = yf.download(
            tickers, start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"),
            interval="1d", auto_adjust=True, progress=False,
            group_by="ticker", threads=True,
        )
    except Exception:
        df = None

    def _closes_for(tk: str):
        if df is None or df.empty:
            return None
        try:
            sub = df[tk] if isinstance(df.columns, pd.MultiIndex) else df
            return sub["Close"].dropna()
        except Exception:
            return None

    out = []
    for m in memos:
        tk = m["ticker"]
        closes = _closes_for(tk)
        price_at_create = price_now = change_pct = None
        if closes is not None and len(closes):
            create_dt = pd.to_datetime(m["created_at"]).normalize()
            # tz-aware closes에 맞추기
            if closes.index.tz is not None:
                create_dt = create_dt.tz_localize(closes.index.tz)
            mask = closes.index <= create_dt
            if mask.any():
                price_at_create = float(closes[mask].iloc[-1])
            price_now = float(closes.iloc[-1])
            if price_at_create and price_now and price_at_create != 0:
                change_pct = (price_now / price_at_create - 1) * 100
        out.append({
            **m,
            "price_at_create": price_at_create,
            "price_now": price_now,
            "change_pct": change_pct,
        })
    return out
