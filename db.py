"""Postgres (Supabase) 어댑터 — sqlite3 호환 인터페이스로 wrap.
'?' 플레이스홀더 자동 변환, dict-row 결과, 컨텍스트 매니저 트랜잭션."""
from __future__ import annotations

import os
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env", override=True)


def _database_url() -> str:
    """매 호출마다 환경변수에서 읽음 — Streamlit Cloud secrets가 import 후 주입되는 케이스 대응."""
    return (os.environ.get("DATABASE_URL") or "").strip()


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

-- 외부 사이트 ticker → slug 매핑 (investing.com 등)
ALTER TABLE ticker_master ADD COLUMN IF NOT EXISTS investing_slug TEXT;

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

CREATE TABLE IF NOT EXISTS watchlist (
    ticker      TEXT PRIMARY KEY,
    added_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_watchlist_added ON watchlist(added_at DESC);

CREATE TABLE IF NOT EXISTS excluded_tickers (
    ticker      TEXT PRIMARY KEY,
    added_at    TEXT NOT NULL,
    note        TEXT
);

CREATE TABLE IF NOT EXISTS portfolios (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    initial_size    DOUBLE PRECISION NOT NULL DEFAULT 100000000,   -- $100M default
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS portfolio_holdings (
    id              BIGSERIAL PRIMARY KEY,
    portfolio_id    BIGINT NOT NULL REFERENCES portfolios(id) ON DELETE CASCADE,
    ticker          TEXT NOT NULL,
    weight_pct      DOUBLE PRECISION NOT NULL,
    entry_date      TEXT NOT NULL,
    entry_price     DOUBLE PRECISION NOT NULL,
    note            TEXT,
    UNIQUE (portfolio_id, ticker)
);
CREATE INDEX IF NOT EXISTS idx_holdings_portfolio ON portfolio_holdings(portfolio_id);

CREATE TABLE IF NOT EXISTS catalysts (
    id              BIGSERIAL PRIMARY KEY,
    ticker          TEXT,                       -- NULL이면 학회 같은 sector-wide 이벤트
    event_date      TEXT NOT NULL,              -- ISO YYYY-MM-DD
    event_end_date  TEXT,                       -- 학회처럼 기간 있는 이벤트
    event_type      TEXT NOT NULL,              -- pdufa / earnings / conference / clinical_completion / advisory_committee
    title           TEXT NOT NULL,
    description     TEXT,
    source          TEXT,                       -- biopharmcatalyst / yfinance / clinicaltrials / hardcoded
    therapy_area    TEXT,                       -- 학회 분류용 (oncology/hepatology 등)
    fetched_at      TEXT NOT NULL,
    UNIQUE (ticker, event_date, event_type, title)
);
CREATE INDEX IF NOT EXISTS idx_catalysts_date ON catalysts(event_date);
CREATE INDEX IF NOT EXISTS idx_catalysts_ticker ON catalysts(ticker, event_date);

-- 워치 기능 — 체크박스로 표시한 카탈리스트의 알림 트리거 추적
ALTER TABLE catalysts ADD COLUMN IF NOT EXISTS watched BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE catalysts ADD COLUMN IF NOT EXISTS notify_date TEXT;        -- 조기 알림 트리거 기준일 (보수적, 시작-of-period)
ALTER TABLE catalysts ADD COLUMN IF NOT EXISTS notified_1m BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE catalysts ADD COLUMN IF NOT EXISTS notified_1w BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX IF NOT EXISTS idx_catalysts_watched ON catalysts(watched, notify_date);

CREATE TABLE IF NOT EXISTS insider_trades (
    id              BIGSERIAL PRIMARY KEY,
    ticker          TEXT NOT NULL,
    filing_date     TEXT NOT NULL,
    trade_date      TEXT NOT NULL,
    insider_name    TEXT NOT NULL,
    title           TEXT,                       -- CEO/CFO/Director 등
    transaction     TEXT NOT NULL,              -- P-Purchase / S-Sale / S-Sale+OE / etc.
    shares          DOUBLE PRECISION,
    price           DOUBLE PRECISION,
    value_usd       DOUBLE PRECISION,
    shares_after    DOUBLE PRECISION,
    fetched_at      TEXT NOT NULL,
    UNIQUE (ticker, trade_date, insider_name, transaction, shares)
);
CREATE INDEX IF NOT EXISTS idx_insider_ticker ON insider_trades(ticker, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_insider_filing ON insider_trades(filing_date DESC);

CREATE TABLE IF NOT EXISTS ai_reports (
    ticker          TEXT PRIMARY KEY,
    body            TEXT NOT NULL,           -- markdown
    generated_at    TEXT NOT NULL,
    model           TEXT
);

CREATE TABLE IF NOT EXISTS price_triggers (
    id              BIGSERIAL PRIMARY KEY,
    ticker          TEXT NOT NULL,
    direction       TEXT NOT NULL,           -- 'above' | 'below'
    threshold       DOUBLE PRECISION NOT NULL,
    note            TEXT,
    created_at      TEXT NOT NULL,
    triggered_at    TEXT,                    -- 발동 시각 (NULL = 대기 중)
    triggered_price DOUBLE PRECISION,
    status          TEXT NOT NULL DEFAULT 'active'   -- active / fired / cancelled
);
CREATE INDEX IF NOT EXISTS idx_triggers_active
    ON price_triggers(status, ticker) WHERE status = 'active';
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


def _is_transient_db_error(e: Exception) -> bool:
    """일시적 네트워크/DNS/연결 오류 — retry 가능."""
    msg = str(e).lower()
    return any(s in msg for s in (
        "could not translate host name",
        "name or service not known",
        "temporary failure in name resolution",
        "connection refused",
        "connection reset",
        "server closed the connection unexpectedly",
        "timeout expired",
        "ssl syscall error",
        "could not connect to server",
    ))


def _retry_db(func, *args, max_attempts: int = 3, base_delay: float = 1.0, **kwargs):
    """일시적 에러일 때 exponential backoff retry."""
    import time
    last_exc: Exception | None = None
    for attempt in range(max_attempts):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            if not _is_transient_db_error(e) or attempt == max_attempts - 1:
                raise
            sleep_s = base_delay * (2 ** attempt)
            time.sleep(sleep_s)
    if last_exc:
        raise last_exc


def connect():
    url = _database_url()
    if not url:
        raise RuntimeError(
            "DATABASE_URL not set. 로컬: .env 파일, 클라우드: Streamlit Secrets에 "
            "'DATABASE_URL = \"postgresql://...\"' 추가."
        )
    def _do_connect():
        raw = psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
        raw.autocommit = False
        return _Conn(raw)
    return _retry_db(_do_connect)


def init_db() -> None:
    with connect() as conn:
        conn.executescript(SCHEMA)


def pd_read_sql(sql: str, params=None):
    """pandas 전용 — RealDictCursor 우회 (pandas는 tuple row 필요).
    '?' → '%s' 자동 변환. 매 호출마다 fresh raw connection 사용 (가벼움).
    트랜지언트 DNS/연결 오류엔 자동 retry."""
    import pandas as pd
    if isinstance(sql, str) and "?" in sql:
        sql = sql.replace("?", "%s")
    def _do_query():
        raw = psycopg2.connect(_database_url())
        try:
            return pd.read_sql_query(sql, raw, params=params)
        finally:
            raw.close()
    return _retry_db(_do_query)


if __name__ == "__main__":
    init_db()
    print("schema initialized on Supabase")
