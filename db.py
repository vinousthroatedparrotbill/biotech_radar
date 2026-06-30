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
ALTER TABLE catalysts ADD COLUMN IF NOT EXISTS acknowledged BOOLEAN NOT NULL DEFAULT FALSE;
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

-- 투자 메모 텔레그램 발송 로그 — 최근 N일 내 중복 발송 방지용
CREATE TABLE IF NOT EXISTS report_sends (
    id          BIGSERIAL PRIMARY KEY,
    ticker      TEXT NOT NULL,
    sent_at     TEXT NOT NULL              -- ISO YYYY-MM-DDTHH:MM:SS
);
CREATE INDEX IF NOT EXISTS idx_report_sends_ticker ON report_sends(ticker, sent_at DESC);

-- OHLCV 캐시 — 로컬(한국 IP)이 토스로 일봉을 채우고, 클라우드(해외 IP, 토스 차단)는
-- 여기서 차트를 읽는다. [[로컬 브릿지]]
CREATE TABLE IF NOT EXISTS ohlcv_cache (
    ticker      TEXT NOT NULL,
    d           TEXT NOT NULL,            -- YYYY-MM-DD
    o           DOUBLE PRECISION,
    h           DOUBLE PRECISION,
    l           DOUBLE PRECISION,
    c           DOUBLE PRECISION,
    v           DOUBLE PRECISION,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (ticker, d)
);
CREATE INDEX IF NOT EXISTS idx_ohlcv_ticker ON ohlcv_cache(ticker, d);

-- MP 거래내역 — 비중 조정을 현재가 체결로 기록(실현손익/현금 반영). 평균단가 회계.
CREATE TABLE IF NOT EXISTS portfolio_transactions (
    id            BIGSERIAL PRIMARY KEY,
    portfolio_id  BIGINT NOT NULL REFERENCES portfolios(id) ON DELETE CASCADE,
    ticker        TEXT NOT NULL,
    action        TEXT NOT NULL,            -- 'buy' | 'sell'
    shares        DOUBLE PRECISION NOT NULL,    -- 항상 양수
    price         DOUBLE PRECISION NOT NULL,    -- 체결가
    amount        DOUBLE PRECISION NOT NULL,    -- shares*price (현금흐름 크기)
    realized_pnl  DOUBLE PRECISION NOT NULL DEFAULT 0,   -- 매도 시 실현손익(평단 기준)
    trade_date    TEXT NOT NULL,            -- ISO YYYY-MM-DD
    note          TEXT
);
CREATE INDEX IF NOT EXISTS idx_ptx_portfolio
    ON portfolio_transactions(portfolio_id, ticker, trade_date, id);

-- AI 챗 공유 대화 로그 — 텔레그램 봇 ↔ 웹앱 챗이 같은 대화를 공유(단일 사용자)
CREATE TABLE IF NOT EXISTS chat_log (
    id          BIGSERIAL PRIMARY KEY,
    role        TEXT NOT NULL,            -- 'user' | 'assistant'
    content     TEXT NOT NULL,
    source      TEXT,                     -- 'telegram' | 'web' (출처 표시용)
    created_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_chat_log_id ON chat_log(id DESC);

-- 신고가/상승폭 'AI 상승이유' 캐시 — 파일이 아닌 DB에 두어 로컬·클라우드 공유 +
-- 재배포에도 보존. snapshot_date(보드 기준일)가 바뀌면 무효(재생성). 데일리런이 채움.
CREATE TABLE IF NOT EXISTS reason_cache (
    country        TEXT NOT NULL,           -- 'USA' | 'KOR'
    kind           TEXT NOT NULL,           -- 'high' | 'movers'
    snapshot_date  TEXT NOT NULL,           -- latest_run_date(country) — 바뀌면 무효
    markdown       TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    PRIMARY KEY (country, kind)
);

-- 자동매매(조건매매) 주문 — 조건 충족 시 '발동'. 실주문은 브로커 미연동이라 dry_run(알림만).
-- condition은 유연한 JSON 트리(price/return_pct/high_break/date/event/ir_readout/all/any).
CREATE TABLE IF NOT EXISTS conditional_orders (
    id               BIGSERIAL PRIMARY KEY,
    portfolio_id     BIGINT,                 -- 대상 MP(선택)
    ticker           TEXT NOT NULL,
    name             TEXT,
    side             TEXT NOT NULL,          -- 'buy' | 'sell'
    size_type        TEXT NOT NULL,          -- 'weight_pct' | 'amount' | 'shares'
    size_value       DOUBLE PRECISION NOT NULL,
    condition        TEXT NOT NULL,          -- JSON 조건 트리
    title            TEXT NOT NULL,          -- 자동 요약 제목
    status           TEXT NOT NULL DEFAULT 'armed',  -- armed|triggered|cancelled|error
    note             TEXT,
    created_at       TEXT NOT NULL,
    armed_at         TEXT,
    triggered_at     TEXT,
    triggered_detail TEXT,                   -- 발동 시 평가 스냅샷(JSON)
    last_eval        TEXT,                   -- 마지막 평가/진행도(JSON) — 카드 '남은 조건' 표시
    dry_run          BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS idx_cond_status ON conditional_orders(status);
-- 라이프사이클(진입→보유→청산) + 체결 기록. status: armed(진입대기)→holding(보유,매도대기)→done.
-- 체결가/시각은 현재 dry_run=페이퍼(브로커 연결 시 실주문으로 대체).
ALTER TABLE conditional_orders ADD COLUMN IF NOT EXISTS exit_condition TEXT;
ALTER TABLE conditional_orders ADD COLUMN IF NOT EXISTS buy_at    TEXT;
ALTER TABLE conditional_orders ADD COLUMN IF NOT EXISTS buy_price DOUBLE PRECISION;
ALTER TABLE conditional_orders ADD COLUMN IF NOT EXISTS sell_at   TEXT;
ALTER TABLE conditional_orders ADD COLUMN IF NOT EXISTS sell_price DOUBLE PRECISION;
ALTER TABLE conditional_orders ADD COLUMN IF NOT EXISTS exit_eval TEXT;   -- 매도 조건 진행도

-- 리드 자산 '피크 글로벌 연매출($M)' LLM 추정 캐시(안정적 → 길게 보존, mcap만 매일 갱신).
CREATE TABLE IF NOT EXISTS peak_sales_est (
    ticker        TEXT PRIMARY KEY,
    peak_sales_m  DOUBLE PRECISION,        -- $M
    basis         TEXT,                     -- 추정 근거 한 줄
    updated_at    TEXT NOT NULL
);

-- '연두색 음영' 스크린 플래그 — 8개월 내 2/3상 readout + mcap/peak_sales ≤ 4배. 데일리 갱신.
CREATE TABLE IF NOT EXISTS screen_flags (
    ticker          TEXT PRIMARY KEY,
    snapshot_date   TEXT,
    flagged         BOOLEAN NOT NULL DEFAULT FALSE,
    ratio           DOUBLE PRECISION,       -- mcap / peak_sales
    peak_sales_m    DOUBLE PRECISION,
    market_cap      DOUBLE PRECISION,
    catalyst_date   TEXT,
    catalyst_title  TEXT,
    note            TEXT,
    updated_at      TEXT NOT NULL
);
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
