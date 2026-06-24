"""한국 제약·바이오 유니버스 → ticker_master(KOR).

소스 (둘 다 IP 무관하게 동작 — pykrx의 KRX 데이터 엔드포인트 차단 회피):
  - 섹터 멤버십: 네이버 금융 업종(WICS) — 제약/생물공학/생명과학도구 구성종목
  - 시총·종가·종목명: FinanceDataReader StockListing('KRX')  (Marcap=KRW, Stocks, Close)

저장: ticker_master.market_cap은 전 종목 $M 통일(미국 Finviz와 호환) — KRW를 FX로 환산.
ticker는 bare 6자리(토스 native) → 토스 시세/차트가 그대로 동작.
country='KOR' → universe.load_universe()의 DELETE 절이 보존(미장 갱신에도 살아남음).

실행: python kr_universe.py
"""
from __future__ import annotations

import logging
import re
from datetime import datetime

import requests

from db import connect

log = logging.getLogger(__name__)

# 네이버 금융 업종(WICS) 코드 — 한국 바이오텍 코어.
#   261 제약 / 286 생물공학 / 262 생명과학도구및서비스
# (건강관리장비·업체/서비스(281/316)는 미국 EXCLUDED와 같은 취지로 제외)
NAVER_UPJONG = {"261": "제약", "286": "생물공학", "262": "생명과학도구"}

_NAVER_DETAIL = "https://finance.naver.com/sise/sise_group_detail.naver?type=upjong&no={no}"
_HDR = {"User-Agent": "Mozilla/5.0"}
_FX_FALLBACK = 1380.0   # USD/KRW fallback (2026 중반 추정)
KR_MCAP_FLOOR_KRW = 500_000_000_000.0   # 5,000억원 (52주 신고가 보드 시총 하한)


def kr_min_mcap_usd_m(krw_floor: float = KR_MCAP_FLOOR_KRW) -> float:
    """KRW 시총 하한 → $M (ticker_master.market_cap과 동일 단위). 현재 FX로 환산."""
    return krw_floor / cached_fx() / 1e6


import time as _time
_fx_cache = {"v": None, "t": 0.0}


def cached_fx(ttl: float = 3600.0) -> float:
    """USD/KRW — 1시간 캐시(렌더링 시 yfinance 반복 호출 회피)."""
    now = _time.time()
    if _fx_cache["v"] and (now - _fx_cache["t"]) < ttl:
        return _fx_cache["v"]
    v = _fx_krw_per_usd()
    _fx_cache.update(v=v, t=now)
    return v


def is_kr_ticker(ticker) -> bool:
    """한국 종목(6자리 숫자) 여부."""
    t = str(ticker or "").strip()
    return t.isdigit() and len(t) == 6


def fmt_mcap(mcap_usd_m, ticker=None) -> str:
    """시총 표시 — KR(6자리)은 KRW(억/조), 그 외는 $B/$M.
    market_cap은 전 종목 $M로 저장돼 있으므로 KR은 현재 FX로 KRW 환산."""
    if mcap_usd_m is None:
        return "—"
    try:
        v = float(mcap_usd_m)
    except (TypeError, ValueError):
        return "—"
    if is_kr_ticker(ticker):
        krw = v * cached_fx() * 1e6
        if krw >= 1e12:
            return f"{krw/1e12:,.1f}조"
        return f"{krw/1e8:,.0f}억"
    b = v / 1000.0
    if b >= 1:
        return f"${b:,.1f}B" if b < 100 else f"${b:,.0f}B"
    return f"${v:,.0f}M"


def fmt_price(close, ticker=None) -> str:
    """가격 표시 — KR(6자리)은 원(정수), 그 외는 $."""
    if close is None:
        return "—"
    try:
        c = float(close)
    except (TypeError, ValueError):
        return "—"
    if is_kr_ticker(ticker):
        return f"{c:,.0f}원"
    return f"${c:,.2f}"


def _fx_krw_per_usd() -> float:
    try:
        import yfinance as yf
        hist = yf.Ticker("KRW=X").history(period="5d")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception:
        pass
    return _FX_FALLBACK


def _sector_members() -> dict[str, str]:
    """{ticker: 업종명} — 네이버 업종별 구성종목 합집합."""
    out: dict[str, str] = {}
    for no, label in NAVER_UPJONG.items():
        try:
            r = requests.get(_NAVER_DETAIL.format(no=no), headers=_HDR, timeout=20)
            r.encoding = "euc-kr"
            for code in re.findall(r"/item/main\.naver\?code=(\d{6})", r.text):
                out.setdefault(code, label)
        except Exception as e:
            log.warning("네이버 업종 %s(%s) 실패: %s", no, label, e)
    return out


def seed() -> int:
    """네이버 업종 멤버 × FDR 시총 → ticker_master upsert. return: 종목 수."""
    import FinanceDataReader as fdr

    members = _sector_members()
    if not members:
        raise RuntimeError("네이버 업종 구성종목 0개 — 네이버 금융 접근/파싱 확인 필요")
    log.info("섹터 멤버 %d종목 (제약+생물공학+생명과학도구)", len(members))

    krx = fdr.StockListing("KRX").set_index("Code")
    fx = _fx_krw_per_usd()
    log.info("FDR KRX %d행 / FX 1USD=%.1fKRW", len(krx), fx)

    now_iso = datetime.now().isoformat(timespec="seconds")
    rows = []
    missing = 0
    for code, label in members.items():
        if code not in krx.index:
            missing += 1
            continue
        r = krx.loc[code]
        try:
            mcap_krw = float(r.get("Marcap") or 0)
        except Exception:
            mcap_krw = 0.0
        mcap_usd_m = (mcap_krw / fx / 1e6) if mcap_krw else None
        try:
            price_krw = float(r.get("Close") or 0) or None
        except Exception:
            price_krw = None
        name = str(r.get("Name") or code)[:200]
        # industry='Biotechnology' 통일 → 기존 미국 바이오 필터/캐시 그대로 통과.
        rows.append((code, name, f"Healthcare ({label})", "Biotechnology",
                     "KOR", mcap_usd_m, price_krw, None, now_iso))

    if missing:
        log.info("FDR에 없는 종목 %d개 스킵 (상폐/신규)", missing)

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
        conn.commit()
    log.info("%d 종목 upsert (KOR)", len(rows))
    return len(rows)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    n = seed()
    print(f"\n[OK] {n} 한국 제약·바이오 종목 ticker_master에 추가/업데이트됨")
