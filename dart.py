"""DART(전자공시) OpenAPI — 한국 종목 공시 조회.

env: DART_API_KEY (opendart.fss.or.kr 발급, 40자).
- corp_code_map(): 종목코드(6자리) → DART corp_code(8자리) 매핑. corpCode.xml(ZIP) 다운로드,
  data/dart_corpcode.json에 캐시(7일). 무거운 호출이라 종목별로 부르지 말 것.
- recent_disclosures(ticker, days, types): 최근 공시 리스트 [{date, title, url, filer, type}].

공시유형(pblntf_ty): A 정기 / B 주요사항보고 / C 발행 / D 지분 / E 기타 / F 외부감사 / I 거래소.
바이오 카탈리스트로는 B(주요사항: 유증·CB·기술이전·임상 관련)·I(거래소)가 유용.
일일 호출 한도 ~20,000. status '020'=한도초과, '013'=데이터없음, '000'=정상.
"""
from __future__ import annotations

import io
import json
import logging
import os
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET

import requests
from dotenv import load_dotenv

log = logging.getLogger(__name__)

_ENV = Path(__file__).parent / ".env"
_CACHE = Path(__file__).parent / "data" / "dart_corpcode.json"
_BASE = "https://opendart.fss.or.kr/api"
_VIEWER = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
_corp_map: dict[str, str] | None = None     # 프로세스 내 캐시


def _key() -> str:
    load_dotenv(_ENV)
    k = (os.environ.get("DART_API_KEY") or "").strip()
    if not k:
        raise RuntimeError("DART_API_KEY 미설정 — .env에 opendart.fss.or.kr 발급 키 추가")
    return k


def available() -> bool:
    try:
        load_dotenv(_ENV)
        return bool((os.environ.get("DART_API_KEY") or "").strip())
    except Exception:
        return False


def _download_corp_map() -> dict[str, str]:
    """corpCode.xml(ZIP) → {stock_code(6): corp_code(8)} (상장 종목만)."""
    r = requests.get(f"{_BASE}/corpCode.xml", params={"crtfc_key": _key()}, timeout=60)
    r.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    xml = zf.read(zf.namelist()[0])
    root = ET.fromstring(xml)
    out: dict[str, str] = {}
    for el in root.iter("list"):
        sc = (el.findtext("stock_code") or "").strip()
        cc = (el.findtext("corp_code") or "").strip()
        if sc and len(sc) == 6 and cc:      # 상장 종목(6자리)만
            out[sc] = cc
    return out


def corp_code_map(max_age_days: int = 7) -> dict[str, str]:
    """캐시된 {종목코드: corp_code}. 7일 경과/없으면 재다운로드."""
    global _corp_map
    if _corp_map is not None:
        return _corp_map
    # 디스크 캐시
    try:
        if _CACHE.exists():
            age = time.time() - _CACHE.stat().st_mtime
            if age < max_age_days * 86400:
                _corp_map = json.loads(_CACHE.read_text(encoding="utf-8"))
                return _corp_map
    except Exception as e:
        log.warning("corp_code 캐시 읽기 실패: %s", e)
    # 재다운로드
    _corp_map = _download_corp_map()
    try:
        _CACHE.parent.mkdir(parents=True, exist_ok=True)
        _CACHE.write_text(json.dumps(_corp_map, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        log.warning("corp_code 캐시 쓰기 실패: %s", e)
    log.info("corp_code 매핑 %d종목 로드", len(_corp_map))
    return _corp_map


def corp_code(ticker: str) -> str | None:
    return corp_code_map().get(str(ticker).strip())


def recent_disclosures(ticker: str, days: int = 30,
                       types: str | None = None, limit: int = 30) -> list[dict]:
    """종목의 최근 공시 [{date, title, url, filer, type}] (최신순).
    types: pblntf_ty 단일문자(예 'B' 주요사항). None이면 전체.
    """
    cc = corp_code(ticker)
    if not cc:
        return []                            # 비상장/미국 등 — corp_code 없음
    end = datetime.now()
    bgn = end - timedelta(days=days)
    params = {
        "crtfc_key": _key(), "corp_code": cc,
        "bgn_de": bgn.strftime("%Y%m%d"), "end_de": end.strftime("%Y%m%d"),
        "page_no": 1, "page_count": min(limit, 100),
        "sort": "date", "sort_mth": "desc",
    }
    if types:
        params["pblntf_ty"] = types
    try:
        r = requests.get(f"{_BASE}/list.json", params=params, timeout=20)
        r.raise_for_status()
        j = r.json()
    except Exception as e:
        log.warning("DART list 실패 %s: %s", ticker, e)
        return []
    if j.get("status") not in ("000", "013"):
        log.warning("DART status %s: %s", j.get("status"), j.get("message"))
        return []
    out = []
    for it in (j.get("list") or [])[:limit]:
        rd = it.get("rcept_dt") or ""
        date_iso = f"{rd[:4]}-{rd[4:6]}-{rd[6:8]}" if len(rd) == 8 else rd
        out.append({
            "date": date_iso,
            "title": it.get("report_nm", ""),
            "url": _VIEWER.format(rcept_no=it.get("rcept_no", "")),
            "filer": it.get("flr_nm", ""),
            "type": it.get("rm", ""),
        })
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    if not available():
        print("DART_API_KEY 미설정 — .env에 추가 필요")
        sys.exit(1)
    m = corp_code_map()
    print(f"corp_code 매핑: {len(m)}종목")
    for tk in ("207940", "226950", "196170"):   # 삼바, 올릭스, 알테오젠
        print(f"\n[{tk}] corp_code={corp_code(tk)}")
        for d in recent_disclosures(tk, days=60, limit=5):
            print(f"  {d['date']} · {d['title']} ({d['filer']})")
