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


def company_info(ticker: str) -> dict:
    """DART 회사개황 — {hm_url(홈페이지), corp_name, corp_name_eng, ceo}. KR IR 매칭용."""
    cc = corp_code(ticker)
    if not cc:
        return {}
    try:
        r = requests.get(f"{_BASE}/company.json",
                         params={"crtfc_key": _key(), "corp_code": cc}, timeout=20)
        r.raise_for_status()
        j = r.json()
    except Exception as e:
        log.warning("DART company %s: %s", ticker, e)
        return {}
    if j.get("status") != "000":
        return {}
    hm = (j.get("hm_url") or "").strip()
    if hm and not hm.startswith("http"):
        hm = "https://" + hm
    return {"hm_url": hm, "corp_name": j.get("corp_name"),
            "corp_name_eng": j.get("corp_name_eng"), "ceo": j.get("ceo_nm")}


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
            "rcept_no": it.get("rcept_no", ""),     # 본문 fetch_document() 호출용
            "url": _VIEWER.format(rcept_no=it.get("rcept_no", "")),
            "filer": it.get("flr_nm", ""),
            "type": it.get("rm", ""),
        })
    return out


_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
       "Referer": "https://dart.fss.or.kr/"}


def _html_to_text(raw: bytes) -> str:
    """HTML/XML bytes → 본문 텍스트(EUC-KR/UTF-8 자동, script/style 제거)."""
    import re as _re
    import warnings

    from bs4 import BeautifulSoup
    try:
        from bs4 import XMLParsedAsHTMLWarning
        warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
    except Exception:
        pass
    html = None
    for enc in ("utf-8", "euc-kr", "cp949"):     # DART 원문은 보통 EUC-KR
        try:
            html = raw.decode(enc); break
        except Exception:
            continue
    if html is None:
        html = raw.decode("utf-8", "replace")
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    txt = soup.get_text("\n", strip=True).replace("\xa0", " ")
    return _re.sub(r"\n{3,}", "\n\n", txt).strip()


def _doc_via_api(rcept_no: str) -> tuple[str, str | None]:
    """공식 document.xml API(ZIP) → (text, err). 인덱싱된 공시에 최적(깔끔)."""
    import io
    import zipfile
    try:
        r = requests.get(f"{_BASE}/document.xml",
                         params={"crtfc_key": _key(), "rcept_no": rcept_no}, timeout=30)
        r.raise_for_status()
    except Exception as e:
        return "", f"요청 실패: {e}"
    head = r.content[:300]
    if b"<status>" in head or b"<result>" in head:     # 키오류/한도/014(미인덱싱) 등
        from bs4 import BeautifulSoup
        try:
            soup = BeautifulSoup(r.content, "html.parser")
            st = soup.find("status"); msg = soup.find("message")
            return "", f"DART {st.text if st else '?'}: {msg.text if msg else '오류'}"
        except Exception:
            return "", "DART 오류 응답"
    try:
        zf = zipfile.ZipFile(io.BytesIO(r.content))
    except Exception as e:
        return "", f"ZIP 파싱 실패: {e}"
    parts = [_html_to_text(zf.read(n)) for n in zf.namelist()]
    return "\n\n".join(p for p in parts if p).strip(), None


def _doc_via_viewer(rcept_no: str) -> tuple[str, str | None]:
    """DART 웹뷰어 스크랩 → (text, err). main.do의 viewDoc 파라미터를 파싱해 viewer.do 본문을
    직접 받는다. **당일/미인덱싱(014) 공시도 즉시 가능**(API 인덱싱과 무관)."""
    import re as _re
    try:
        m = requests.get(f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}",
                         headers=_UA, timeout=20)
        m.raise_for_status()
    except Exception as e:
        return "", f"main.do 실패: {e}"
    # viewDoc("rcpNo","dcmNo","eleId","offset","length","dtd"[,"tocNo"]) — 첫 구체값(원문)
    mt = _re.search(
        r'viewDoc\(\s*"(\d+)"\s*,\s*"(\d+)"\s*,\s*"([^"]*)"\s*,\s*"([^"]*)"\s*,'
        r'\s*"([^"]*)"\s*,\s*"([^"]*)"', m.text)
    if not mt:
        return "", "viewDoc 파라미터 못 찾음(뷰어 구조 변경?)"
    rno, dcm, ele, off, length, dtd = mt.groups()
    try:
        v = requests.get("https://dart.fss.or.kr/report/viewer.do",
                         params={"rcpNo": rno, "dcmNo": dcm, "eleId": ele,
                                 "offset": off, "length": length, "dtd": dtd},
                         headers=_UA, timeout=25)
        v.raise_for_status()
    except Exception as e:
        return "", f"viewer.do 실패: {e}"
    txt = _html_to_text(v.content)
    return (txt, None) if txt else ("", "뷰어 본문 비어있음")


def fetch_document(rcept_no: str, max_chars: int = 12000) -> dict:
    """DART 공시 '원문 본문 텍스트'. ① 공식 API(document.xml) 시도 → 실패(특히 당일
    공시 014 미인덱싱) 시 ② 웹뷰어 스크랩 폴백(당일 공시도 정정 전/후 표까지 추출 가능).
    return {ok, rcept_no, text, chars, truncated, source} 또는 {ok:False, error}."""
    rcept_no = (rcept_no or "").strip()
    if not rcept_no.isdigit():
        return {"ok": False, "error": f"rcept_no(접수번호 숫자) 필요 — 받은 값: {rcept_no!r}"}

    text, api_err = _doc_via_api(rcept_no)
    source = "api"
    if not text:                                  # API 실패(014 등) → 뷰어 폴백
        text, web_err = _doc_via_viewer(rcept_no)
        source = "viewer"
        if not text:
            return {"ok": False,
                    "error": f"API: {api_err} / 뷰어: {web_err}"}
    return {"ok": True, "rcept_no": rcept_no, "source": source, "chars": len(text),
            "truncated": len(text) > max_chars, "text": text[:max_chars]}


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
