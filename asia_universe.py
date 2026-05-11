"""Curated Tier 1 + Tier 2 일본/중국(HK) 바이오텍 시드 → ticker_master.

수동 큐레이션 (Finviz는 US-only). yfinance로 회사 정보·시총·가격 fetch.
시총은 현지통화 → USD M로 환산 (yfinance FX).

실행: python asia_universe.py
universe.load_universe()는 country='USA' 행만 wipe하므로 이 데이터는 보존됨.
"""
from __future__ import annotations

import logging
from datetime import datetime

import yfinance as yf

from db import connect

log = logging.getLogger(__name__)


# ─────────────────────── 큐레이션 리스트 ───────────────────────
# (ticker, korean_short, country, industry_hint)
ASIA_BIOTECH = [
    # ── 일본 Tier 1 (글로벌 large-cap, 영문 IR 풍부) ──
    ("4502.T", "Takeda Pharmaceutical",       "JPN", "Drug Manufacturers - General"),
    ("4503.T", "Astellas Pharma",             "JPN", "Drug Manufacturers - General"),
    ("4568.T", "Daiichi Sankyo",              "JPN", "Drug Manufacturers - General"),
    ("4523.T", "Eisai",                       "JPN", "Drug Manufacturers - General"),
    ("4519.T", "Chugai Pharmaceutical",       "JPN", "Drug Manufacturers - General"),
    ("4506.T", "Sumitomo Pharma",             "JPN", "Drug Manufacturers - Specialty"),
    ("4507.T", "Shionogi",                    "JPN", "Drug Manufacturers - General"),
    ("4151.T", "Kyowa Kirin",                 "JPN", "Drug Manufacturers - General"),
    ("4528.T", "Ono Pharmaceutical",          "JPN", "Drug Manufacturers - General"),
    ("4578.T", "Otsuka Holdings",             "JPN", "Drug Manufacturers - General"),
    # ── 일본 Tier 2 (mid-cap biotech) ──
    ("4587.T", "Peptidream",                  "JPN", "Biotechnology"),
    ("4565.T", "Nxera Pharma (Sosei)",        "JPN", "Biotechnology"),
    ("4593.T", "Healios",                     "JPN", "Biotechnology"),

    # ── 중국 HK Tier 1 (영문 IR, ClinicalTrials.gov 등록) ──
    ("1801.HK", "Innovent Biologics",         "CHN", "Biotechnology"),
    ("1093.HK", "CSPC Pharmaceutical",        "CHN", "Drug Manufacturers - Specialty"),
    ("3692.HK", "Hansoh Pharmaceutical",      "CHN", "Drug Manufacturers - Specialty"),
    ("1177.HK", "Sino Biopharmaceutical",     "CHN", "Drug Manufacturers - Specialty"),
    ("2269.HK", "WuXi Biologics",             "CHN", "Biotechnology"),
    ("9926.HK", "Akeso",                      "CHN", "Biotechnology"),
    ("1530.HK", "3SBio",                      "CHN", "Drug Manufacturers - Specialty"),
    ("2096.HK", "Simcere Pharmaceutical",     "CHN", "Drug Manufacturers - Specialty"),
    # ── 중국 HK Tier 2 (mid-cap, 영문 자료 일부 있음) ──
    ("1877.HK", "Junshi Biosciences",         "CHN", "Biotechnology"),
    ("9995.HK", "RemeGen",                    "CHN", "Biotechnology"),
    ("1548.HK", "Genscript Biotech",          "CHN", "Biotechnology"),
    ("1672.HK", "Ascletis Pharma",            "CHN", "Biotechnology"),
]


def _fx_rate(currency: str) -> float:
    """USD/{currency} (1 USD가 몇 currency인지). yfinance 환율 fetch."""
    if currency.upper() == "USD":
        return 1.0
    try:
        pair = f"{currency.upper()}=X"   # JPY=X → USD/JPY
        hist = yf.Ticker(pair).history(period="5d")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception:
        pass
    # fallback (2026년 중반 추정치)
    return {"JPY": 155.0, "HKD": 7.83, "CNY": 7.20}.get(currency.upper(), 1.0)


def seed() -> int:
    """ASIA_BIOTECH 리스트 → ticker_master upsert. yfinance 정보 fetch."""
    now_iso = datetime.now().isoformat(timespec="seconds")
    # FX 캐시 (반복 호출 회피)
    fx_cache: dict[str, float] = {}

    rows = []
    for ticker, short_name, country, industry in ASIA_BIOTECH:
        try:
            tk = yf.Ticker(ticker)
            info = tk.info or {}
        except Exception as e:
            log.warning("%s yfinance 실패: %s — fallback 기본값 사용", ticker, e)
            info = {}

        # 정확한 정보 우선, 없으면 큐레이션 이름
        name = (info.get("longName") or info.get("shortName") or short_name)[:200]
        currency = (info.get("currency") or
                    {"JPN": "JPY", "CHN": "HKD"}.get(country, "USD")).upper()
        if currency not in fx_cache:
            fx_cache[currency] = _fx_rate(currency)
        fx = fx_cache[currency]

        mcap_local = info.get("marketCap") or 0
        mcap_usd_m = (mcap_local / fx / 1e6) if (mcap_local and fx) else None

        price_local = (info.get("currentPrice") or info.get("regularMarketPrice")
                       or info.get("previousClose"))
        # price는 그대로 현지 (yfinance 차트와 일치하게)
        pe = info.get("trailingPE")

        rows.append((ticker, name, "Healthcare", industry, country,
                     mcap_usd_m, price_local, pe, now_iso))
        log.info("%s %s | mcap=$%s M %s | %s %s",
                 ticker, name[:30],
                 f"{mcap_usd_m:,.0f}" if mcap_usd_m else "?",
                 currency, country, industry)

    # Upsert (PK는 ticker)
    with connect() as conn:
        for r in rows:
            conn.execute(
                """
                INSERT INTO ticker_master
                  (ticker, name, sector, industry, country,
                   market_cap, price, pe_ratio, updated_at)
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
    return len(rows)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    n = seed()
    print(f"\n✓ {n} 종목 ticker_master에 추가/업데이트됨")
