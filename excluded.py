"""비-biotech ticker 블랙리스트 (상승폭/52w 신고가 표시에서 숨김)."""
from __future__ import annotations

from datetime import datetime

from db import connect


def add(ticker: str, note: str = "") -> None:
    ticker = ticker.strip().upper()
    if not ticker:
        return
    with connect() as conn:
        conn.execute(
            "INSERT INTO excluded_tickers (ticker, added_at, note) VALUES (?,?,?) "
            "ON CONFLICT (ticker) DO UPDATE SET note = EXCLUDED.note",
            (ticker, datetime.now().isoformat(timespec="seconds"), note or None),
        )


def remove(ticker: str) -> None:
    with connect() as conn:
        conn.execute("DELETE FROM excluded_tickers WHERE ticker = ?", (ticker.upper(),))


def is_excluded(ticker: str) -> bool:
    with connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM excluded_tickers WHERE ticker = ?", (ticker.upper(),)
        ).fetchone()
        return row is not None


def list_all() -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT ticker, added_at, note FROM excluded_tickers ORDER BY added_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def seed_defaults() -> None:
    """초기 블랙리스트 (사용자가 자주 마주칠 명백한 비-biotech)."""
    defaults = [
        ("WRBY", "Warby Parker — 안경 retail"),
    ]
    with connect() as conn:
        for tk, note in defaults:
            conn.execute(
                "INSERT INTO excluded_tickers (ticker, added_at, note) VALUES (?,?,?) "
                "ON CONFLICT (ticker) DO NOTHING",
                (tk, datetime.now().isoformat(timespec="seconds"), note),
            )
