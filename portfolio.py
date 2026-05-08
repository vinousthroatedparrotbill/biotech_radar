"""모델 포트폴리오 — 펀드 관리 + 수익률 계산."""
from __future__ import annotations

from datetime import datetime

import yfinance as yf

from db import connect

DEFAULT_INITIAL_SIZE = 100_000_000.0   # $100M


# ───────────────────────── Portfolio CRUD ─────────────────────────
def create(name: str, initial_size: float = DEFAULT_INITIAL_SIZE) -> int:
    name = name.strip()
    if not name:
        raise ValueError("name required")
    with connect() as conn:
        cur = conn.execute(
            "INSERT INTO portfolios (name, initial_size, created_at) "
            "VALUES (?,?,?) RETURNING id",
            (name, initial_size, datetime.now().isoformat(timespec="seconds")),
        )
        return int(cur.fetchone()["id"])


def delete(portfolio_id: int) -> None:
    with connect() as conn:
        conn.execute("DELETE FROM portfolios WHERE id = ?", (portfolio_id,))


def rename(portfolio_id: int, new_name: str) -> None:
    with connect() as conn:
        conn.execute(
            "UPDATE portfolios SET name = ? WHERE id = ?",
            (new_name.strip(), portfolio_id),
        )


def list_all() -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT id, name, initial_size, created_at FROM portfolios "
            "ORDER BY created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def get(portfolio_id: int) -> dict | None:
    with connect() as conn:
        r = conn.execute(
            "SELECT id, name, initial_size, created_at FROM portfolios WHERE id = ?",
            (portfolio_id,),
        ).fetchone()
        return dict(r) if r else None


# ───────────────────────── Holding CRUD ─────────────────────────
def _fetch_current_price(ticker: str) -> float | None:
    """현재가 fetch — high_low_cache 우선, 없으면 yfinance live."""
    with connect() as conn:
        r = conn.execute(
            "SELECT today_close FROM high_low_cache "
            "WHERE ticker = ? ORDER BY computed_date DESC LIMIT 1",
            (ticker,),
        ).fetchone()
        if r and r["today_close"]:
            return float(r["today_close"])
    try:
        info = yf.Ticker(ticker).fast_info
        for k in ("last_price", "lastPrice", "regularMarketPrice"):
            v = info.get(k) if hasattr(info, "get") else getattr(info, k, None)
            if v:
                return float(v)
    except Exception:
        pass
    return None


def add_holding(portfolio_id: int, ticker: str, weight_pct: float,
                note: str = "") -> int:
    """편입 — 편입가는 현재가를 자동 사용 (편입일 = 오늘)."""
    ticker = ticker.strip().upper()
    if not ticker:
        raise ValueError("ticker required")
    price = _fetch_current_price(ticker)
    if not price:
        raise RuntimeError(f"{ticker}: 현재가 못 가져옴")
    with connect() as conn:
        cur = conn.execute(
            "INSERT INTO portfolio_holdings "
            "(portfolio_id, ticker, weight_pct, entry_date, entry_price, note) "
            "VALUES (?,?,?,?,?,?) RETURNING id",
            (portfolio_id, ticker, weight_pct,
             datetime.now().date().isoformat(), price, note or None),
        )
        return int(cur.fetchone()["id"])


def update_weight(holding_id: int, weight_pct: float) -> None:
    with connect() as conn:
        conn.execute(
            "UPDATE portfolio_holdings SET weight_pct = ? WHERE id = ?",
            (weight_pct, holding_id),
        )


def remove_holding(holding_id: int) -> None:
    with connect() as conn:
        conn.execute("DELETE FROM portfolio_holdings WHERE id = ?", (holding_id,))


def list_holdings(portfolio_id: int) -> list[dict]:
    """편입 종목 + 회사명 join."""
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT h.id, h.ticker, h.weight_pct, h.entry_date, h.entry_price,
                   h.note, t.name, t.industry
            FROM portfolio_holdings h
            LEFT JOIN ticker_master t ON t.ticker = h.ticker
            WHERE h.portfolio_id = ?
            ORDER BY h.weight_pct DESC, h.id
            """,
            (portfolio_id,),
        ).fetchall()
        return [dict(r) for r in rows]


# ───────────────────────── 수익률 / Summary ─────────────────────────
def summary(portfolio_id: int) -> dict:
    """포트폴리오 전체 요약.
    Returns: {portfolio, holdings (with curr_price/return_pct/$amount/$current),
              total_weight, cash_pct, current_size, return_pct}."""
    p = get(portfolio_id)
    if not p:
        return {}
    hs = list_holdings(portfolio_id)
    initial = p["initial_size"]

    enriched = []
    invested_initial = 0.0
    invested_current = 0.0
    for h in hs:
        weight = h["weight_pct"] or 0.0
        entry = h["entry_price"] or 0.0
        curr = _fetch_current_price(h["ticker"]) or entry
        ret = ((curr / entry) - 1) * 100 if entry else 0.0
        amt_initial = initial * (weight / 100.0)
        amt_current = amt_initial * (1 + ret / 100.0)
        invested_initial += amt_initial
        invested_current += amt_current
        enriched.append({
            **h,
            "curr_price": curr,
            "return_pct": ret,
            "amt_initial": amt_initial,
            "amt_current": amt_current,
        })

    total_weight = sum(h["weight_pct"] or 0.0 for h in hs)
    cash_pct = max(0.0, 100.0 - total_weight)
    cash_amt = initial * (cash_pct / 100.0)
    current_size = invested_current + cash_amt
    total_return_pct = (current_size / initial - 1) * 100 if initial else 0.0

    return {
        "portfolio": p,
        "holdings": enriched,
        "total_weight": total_weight,
        "cash_pct": cash_pct,
        "cash_amt": cash_amt,
        "current_size": current_size,
        "return_pct": total_return_pct,
    }
