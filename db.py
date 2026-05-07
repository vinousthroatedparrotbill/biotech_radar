"""Postgres (Supabase) 어댑터 — sqlite3 호환 인터페이스로 wrap.
'?' 플레이스홀더 자동 변환, dict-row 결과, 컨텍스트 매니저 트랜잭션."""
from __future__ import annotations

import os
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env", override=True)
DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip()


SCHEMA = """
CREATE TABLE IF NOT EXISTS ticker_master (
    ticker      TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    sector      TEXT,
    industry    TEXT,
    country     TEXT,
    market_cap  DOUBLE PRECISION,
    price       DOUBLE PRECISION,
    pe_ratio    DOUBLE PRECISION,
    updated_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ticker_industry ON ticker_master(industry);
CREATE INDEX IF NOT EXISTS idx_ticker_mcap ON ticker_master(market_cap DESC);

CREATE TABLE IF NOT EXISTS high_low_cache (
    ticker          TEXT NOT NULL,
    computed_date   TEXT NOT NULL,
    high_52w        DOUBLE PRECISION,
    low_52w         DOUBLE PRECISION,
    today_high      DOUBLE PRECISION,
    today_low       DOUBLE PRECISION,
    today_close     DOUBLE PRECISION,
    market_cap      DOUBLE PRECISION,
    perf_1d         DOUBLE PRECISION,
    perf_7d         DOUBLE PRECISION,
    perf_1m         DOUBLE PRECISION,
    perf_3m         DOUBLE PRECISION,
    perf_6m         DOUBLE PRECISION,
    perf_1y         DOUBLE PRECISION,
    PRIMARY KEY (ticker, computed_date)
);
CREATE INDEX IF NOT EXISTS idx_hl_date ON high_low_cache(computed_date);

CREATE TABLE IF NOT EXISTS memos (
    id          BIGSERIAL PRIMARY KEY,
    ticker      TEXT NOT NULL,
    body        TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_memos_ticker ON memos(ticker, created_at DESC);
"""


class _Cursor:
    """psycopg2 cursor wrapper — sqlite3 스타일 인터페이스 + '?' → '%s' 자동 변환."""

    def __init__(self, raw):
        self._raw = raw

    def execute(self, sql, params=()):
        if isinstance(sql, str) and "?" in sql:
            sql = sql.replace("?", "%s")
        self._raw.execute(sql, params if params else None)
        return self

    def executemany(self, sql, seq):
        if isinstance(sql, str) and "?" in sql:
            sql = sql.replace("?", "%s")
        self._raw.executemany(sql, seq)
        return self

    def fetchone(self):
        return self._raw.fetchone()

    def fetchall(self):
        return self._raw.fetchall()

    def fetchmany(self, n=None):
        return self._raw.fetchmany(n) if n is not None else self._raw.fetchmany()

    @property
    def description(self):
        return self._raw.description

    @property
    def rowcount(self):
        return self._raw.rowcount

    def close(self):
        self._raw.close()

    def __iter__(self):
        return iter(self._raw)


class _Conn:
    """sqlite3.Connection 호환 wrapper — `with`로 commit/rollback, conn.execute() 지원."""

    def __init__(self, raw):
        self._raw = raw

    def cursor(self):
        return _Cursor(self._raw.cursor())

    def execute(self, sql, params=()):
        return self.cursor().execute(sql, params)

    def executemany(self, sql, seq):
        return self.cursor().executemany(sql, seq)

    def executescript(self, sql):
        cur = self._raw.cursor()
        try:
            cur.execute(sql)
        finally:
            cur.close()

    def commit(self):
        self._raw.commit()

    def rollback(self):
        self._raw.rollback()

    def close(self):
        self._raw.close()

    @property
    def raw(self):
        """직접 psycopg2 connection 노출 — 필요시 사용."""
        return self._raw

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is not None:
                self._raw.rollback()
            else:
                self._raw.commit()
        finally:
            self._raw.close()


def connect():
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL not set in .env. Supabase Project Settings → Database → "
            "Connection string (URI)을 복사해 .env에 'DATABASE_URL=postgresql://...' 추가."
        )
    raw = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    raw.autocommit = False
    return _Conn(raw)


def init_db() -> None:
    with connect() as conn:
        conn.executescript(SCHEMA)


def pd_read_sql(sql: str, params=None):
    """pandas 전용 — RealDictCursor 우회 (pandas는 tuple row 필요).
    '?' → '%s' 자동 변환. 매 호출마다 fresh raw connection 사용 (가벼움)."""
    import pandas as pd
    if isinstance(sql, str) and "?" in sql:
        sql = sql.replace("?", "%s")
    raw = psycopg2.connect(DATABASE_URL)   # 기본 tuple cursor
    try:
        return pd.read_sql_query(sql, raw, params=params)
    finally:
        raw.close()


if __name__ == "__main__":
    init_db()
    print("schema initialized on Supabase")
