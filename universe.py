"""Finviz Elite screener — Healthcare sector, USA, mcap ≥ $1.5B."""
from __future__ import annotations

import io
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from db import connect, pd_read_sql

_ENV_PATH = Path(__file__).parent / ".env"
FINVIZ_EXPORT = "https://elite.finviz.com/export.ashx"
MIN_MCAP_M = 0.0    # mcap floor 없음 — Healthcare 전 종목 (관심종목 검색용 보장).
                    # 52w 신고가 수집은 ≥$1.5B 또는 watchlist 등록 종목만 처리 (collectors/high_low.py).


def _token() -> str:
    load_dotenv(_ENV_PATH)
    tok = os.environ.get("FINVIZ_AUTH_TOKEN")
    if not tok:
        raise RuntimeError("FINVIZ_AUTH_TOKEN not set in .env")
    return tok


def fetch_csv(filters: str = "sec_healthcare", view: str = "111") -> pd.DataFrame:
    """Pull Finviz Elite screener result as DataFrame.
    Default: 글로벌 Healthcare 전체 (mcap floor 없음).
    52w 신고가 수집 단계에서 다시 ≥$1.5B + watchlist 합집합으로 필터링됨."""
    r = requests.get(
        FINVIZ_EXPORT,
        params={"v": view, "f": filters, "auth": _token()},
        timeout=30,
    )
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    return df


def load_universe(min_mcap_m: float = MIN_MCAP_M) -> int:
    """Refresh ticker_master with US Healthcare ≥ min_mcap_m. Returns row count."""
    df = fetch_csv()
    # Finviz columns: No., Ticker, Company, Sector, Industry, Country, Market Cap, P/E, Price, Change, Volume
    df = df.rename(columns={
        "Ticker": "ticker", "Company": "name", "Sector": "sector",
        "Industry": "industry", "Country": "country",
        "Market Cap": "market_cap", "P/E": "pe_ratio", "Price": "price",
    })
    df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")
    # 신규 IPO 등 Finviz가 시총을 아직 안 채운(NaN) 종목 → yfinance로 백필.
    # (이걸 안 하면 dropna로 통째 버려져 신규 상장주가 신고가를 찍어도 보드에 안 뜸.)
    # 보통 몇 종목뿐이라 비용 미미. 백필 실패분만 최종 dropna로 제외.
    missing = df.index[df["market_cap"].isna()]
    if len(missing):
        import yfinance as yf
        for idx in missing:
            tk = str(df.at[idx, "ticker"]).strip()
            try:
                mc = (yf.Ticker(tk).info or {}).get("marketCap")
                if mc:
                    df.at[idx, "market_cap"] = float(mc) / 1e6   # USD → $M (Finviz 단위)
            except Exception:
                pass
    df = df.dropna(subset=["market_cap"])
    df = df[df["market_cap"] >= min_mcap_m]

    now_iso = datetime.now().isoformat(timespec="seconds")
    rows = [
        (r["ticker"], r["name"], r["sector"], r["industry"], r["country"],
         float(r["market_cap"]),
         float(r["price"]) if pd.notna(r["price"]) else None,
         float(r["pe_ratio"]) if pd.notna(r["pe_ratio"]) else None,
         now_iso)
        for _, r in df.iterrows()
    ]

    with connect() as conn:
        # USA / NULL country 종목 wipe — 수동 추가한 비-USA (JPN/CHN) 종목은 보존
        conn.execute(
            "DELETE FROM ticker_master "
            "WHERE country = 'USA' OR country IS NULL "
            "OR country NOT IN ('JPN','CHN','KOR','HKG','TWN','GBR','DEU','FRA','CHE','JAP')"
        )
        # UPSERT — 행 중복 시 update (혹시 남은 row와 충돌해도 안전)
        for r in rows:
            conn.execute(
                """
                INSERT INTO ticker_master
                  (ticker, name, sector, industry, country, market_cap, price, pe_ratio, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?)
                ON CONFLICT (ticker) DO UPDATE SET
                  name = EXCLUDED.name,
                  sector = EXCLUDED.sector,
                  industry = EXCLUDED.industry,
                  country = EXCLUDED.country,
                  market_cap = EXCLUDED.market_cap,
                  price = EXCLUDED.price,
                  pe_ratio = EXCLUDED.pe_ratio,
                  updated_at = EXCLUDED.updated_at
                """,
                r,
            )
        conn.commit()
    return len(rows)


def get_universe(industry_filter: str | None = None) -> pd.DataFrame:
    """Read ticker_master. industry_filter: e.g. 'Biotechnology' to narrow."""
    df = pd_read_sql("SELECT * FROM ticker_master ORDER BY market_cap DESC")
    if industry_filter:
        df = df[df["industry"] == industry_filter]
    return df


if __name__ == "__main__":
    n = load_universe()
    print(f"Loaded {n} tickers (USA Healthcare ≥ $1.5B)")
