"""52주 신고가 + 수익률 일괄 캐시."""
from __future__ import annotations

from datetime import datetime

import pandas as pd

from db import connect, pd_read_sql
from perf import compute_snapshot
from universe import get_universe

# Healthcare에서 비-치료 영역 제외 (retailers/distribution/시설/보험/IT)
# 의료기기/CDMO/진단은 통과
EXCLUDED_INDUSTRIES = (
    "Pharmaceutical Retailers",
    "Medical Distribution",
    "Medical Care Facilities",
    "Health Information Services",
    "Healthcare Plans",
)


def _industry_filter(alias: str = "t") -> str:
    quoted = ", ".join(f"'{s}'" for s in EXCLUDED_INDUSTRIES)
    return f"({alias}.industry IS NOT NULL AND {alias}.industry NOT IN ({quoted}))"


def _excluded_ticker_filter(alias: str = "h") -> str:
    """사용자 지정 ticker 블랙리스트 제외."""
    return f"{alias}.ticker NOT IN (SELECT ticker FROM excluded_tickers)"


BIOTECH_INDUSTRY_FILTER = _industry_filter("t")


def collect_tickers(tickers: list[str], mcap_map: dict[str, float] | None = None) -> int:
    """주어진 ticker 리스트에 대해 yfinance 스냅샷 + high_low_cache upsert.
    mcap_map 미지정 시 ticker_master에서 자동 조회.
    return: 처리된 row 수.
    """
    if not tickers:
        return 0
    if mcap_map is None:
        with connect() as conn:
            ph = ",".join(["?"] * len(tickers))
            mcap_map = {
                r["ticker"]: r["market_cap"]
                for r in conn.execute(
                    f"SELECT ticker, market_cap FROM ticker_master WHERE ticker IN ({ph})",
                    tuple(tickers),
                ).fetchall()
            }

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
    return len(rows)


def collect(industry_filter: str | None = "Biotechnology") -> int:
    """52w high + 멀티 기간 수익률 계산.
    범위 = (mcap ≥ $1.5B) ∪ (관심종목 등록된 ticker). 작은 종목은 watchlist로만 추적."""
    universe = get_universe(industry_filter=industry_filter)
    if universe.empty:
        raise RuntimeError("ticker_master 비어있음. 먼저 universe 갱신.")
    # KR(6자리)은 yfinance로 못 읽음 → 미국/해외 collect에서 제외 (KR은 collect_kr가 토스로 처리)
    if "country" in universe.columns:
        universe = universe[universe["country"].fillna("USA") != "KOR"].reset_index(drop=True)

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

    # 수집 범위 = mcap ≥ $500M (52w 보드는 ≥$1.5B로 표시 시 필터, 상승폭 페이지는 $500M+)
    mask = (universe["market_cap"] >= 500.0) | (universe["ticker"].isin(watch_tickers))
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


def collect_kr() -> int:
    """한국(KOR) 바이오 유니버스 52w 스냅샷 — 토스 기반(compute_snapshot_kr, yfinance 우회).
    KR 종목 전부 수집(유니버스 소규모); 보드 표시 시총 하한은 조회 함수에서 필터."""
    from perf import compute_snapshot_kr
    uni = pd_read_sql("SELECT ticker, market_cap FROM ticker_master WHERE country = 'KOR'")
    if uni.empty:
        raise RuntimeError("KOR 유니버스 비어있음 — 먼저 kr_universe.seed() 실행")
    tickers = uni["ticker"].tolist()
    mcap_map = dict(zip(uni["ticker"], uni["market_cap"]))
    snap = compute_snapshot_kr(tickers)
    if snap.empty:
        return 0
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


def _country_clause(country: str | None, alias: str) -> str:
    """country 지정 시 해당 국가만; None이면 KOR 제외(미국 보드 기본 — KR 혼입 방지)."""
    if country:
        return f"{alias}.country = '{country}'"
    return f"({alias}.country IS NULL OR {alias}.country <> 'KOR')"


def _recent_dates(country: str | None, n: int = 1) -> list[str]:
    """해당 국가 스코프의 최근 computed_date 목록 (최신순).

    전역 MAX(computed_date)를 쓰면 US/KR 스케줄이 어긋날 때(예: US는 주말 스킵,
    KR은 매일 실행) 보드가 비어버린다 → 반드시 국가별로 최신 날짜를 구한다.
    """
    with connect() as conn:
        rows = conn.execute(
            f"""SELECT DISTINCT h.computed_date AS d
                FROM high_low_cache h
                LEFT JOIN ticker_master t ON t.ticker = h.ticker
                WHERE {_country_clause(country, 't')}
                ORDER BY h.computed_date DESC
                LIMIT {int(n)}"""
        ).fetchall()
    return [r["d"] for r in rows]


def fetch_new_highs(direction: str = "high", limit: int = 500,
                    country: str | None = None, min_mcap: float = 1500.0) -> pd.DataFrame:
    """오늘 52w 신고가/신저가 종목 — ticker_master와 join하여 회사명 포함."""
    with connect() as conn:
        dates = _recent_dates(country, 1)
        latest = dates[0] if dates else None
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
          AND h.market_cap >= %s
          AND {BIOTECH_INDUSTRY_FILTER}
          AND {_country_clause(country, 't')}
          AND {_excluded_ticker_filter('h')}
          AND {cond}
        ORDER BY {order}
        LIMIT %s
        """,
        params=(latest, min_mcap, limit),
    )
    return df


def fetch_top_movers(limit: int = 100, min_mcap: float = 500.0,
                     min_perf: float = 5.0, country: str | None = None) -> pd.DataFrame:
    """오늘 일일 상승률 상위 종목 (1D% 내림차순).
    필터: industry 블랙리스트(retailer/시설/보험 등) + 사용자 ticker 블랙리스트."""
    dates = _recent_dates(country, 1)
    latest = dates[0] if dates else None
    if not latest:
        return pd.DataFrame()
    return pd_read_sql(
        f"""
        SELECT h.ticker, t.name, t.industry, h.today_close AS close,
               h.high_52w, h.low_52w, h.market_cap,
               h.perf_1d, h.perf_7d, h.perf_1m, h.perf_3m, h.perf_6m, h.perf_1y
        FROM high_low_cache h
        LEFT JOIN ticker_master t ON t.ticker = h.ticker
        WHERE h.computed_date = %s
          AND h.market_cap >= %s
          AND h.perf_1d IS NOT NULL
          AND h.perf_1d >= %s
          AND {BIOTECH_INDUSTRY_FILTER}
          AND {_country_clause(country, 't')}
          AND {_excluded_ticker_filter('h')}
        ORDER BY h.perf_1d DESC
        LIMIT %s
        """,
        params=(latest, min_mcap, min_perf, limit),
    )


def latest_run_date(country: str | None = None) -> str | None:
    """해당 국가 보드의 최신 스냅샷 날짜(기본=미국 스코프, KOR 제외).
    전역 MAX이 아니라 국가별 — US/KR 스케줄이 어긋나도 올바른 기준일 반환."""
    dates = _recent_dates(country, 1)
    return dates[0] if dates else None


def fetch_new_today_highs(limit: int = 200, country: str | None = None,
                          min_mcap: float = 1500.0) -> pd.DataFrame:
    """오늘 새로 52주 신고가를 찍은 종목들 (전일 대비 high_52w 상승).
    조건:
      - 오늘 today_high >= high_52w * 0.999 (오늘 신고가)
      - 전일 row가 없거나 (첫 기록), 전일 high_52w 보다 오늘 high_52w가 높음
    """
    dates = _recent_dates(country, 2)
    if not dates:
        return pd.DataFrame()
    today = dates[0]
    yesterday = dates[1] if len(dates) > 1 else None

    bio_filter = (_industry_filter("m") + " AND " + _country_clause(country, "m")
                  + " AND " + _excluded_ticker_filter("t"))
    if yesterday:
        sql = f"""
            SELECT t.ticker, m.name, m.industry, t.today_close AS close,
                   t.high_52w, t.low_52w, t.market_cap,
                   t.perf_1d, t.perf_7d, t.perf_1m, t.perf_3m, t.perf_6m, t.perf_1y,
                   y.high_52w AS prev_high_52w
            FROM high_low_cache t
            LEFT JOIN high_low_cache y
              ON y.ticker = t.ticker AND y.computed_date = %s
            LEFT JOIN ticker_master m ON m.ticker = t.ticker
            WHERE t.computed_date = %s
              AND t.market_cap >= %s
              AND {bio_filter}
              AND t.today_high >= t.high_52w * 0.999
              AND (y.high_52w IS NULL OR t.high_52w > y.high_52w)
            ORDER BY (t.today_close / NULLIF(t.high_52w,0)) DESC
            LIMIT %s
        """
        params = (yesterday, today, min_mcap, limit)
    else:
        sql = f"""
            SELECT t.ticker, m.name, m.industry, t.today_close AS close,
                   t.high_52w, t.low_52w, t.market_cap,
                   t.perf_1d, t.perf_7d, t.perf_1m, t.perf_3m, t.perf_6m, t.perf_1y,
                   NULL AS prev_high_52w
            FROM high_low_cache t
            LEFT JOIN ticker_master m ON m.ticker = t.ticker
            WHERE t.computed_date = %s
              AND t.market_cap >= %s
              AND {bio_filter}
              AND t.today_high >= t.high_52w * 0.999
            ORDER BY (t.today_close / NULLIF(t.high_52w,0)) DESC
            LIMIT %s
        """
        params = (today, min_mcap, limit)

    df = pd_read_sql(sql, params=params)
    return df
