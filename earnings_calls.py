"""분기 어닝콜 + 학회 발표 transcript에서 forward-looking 멘션 추출.

소스 (cascade)
1) **investing.com** (가장 풍부) — Q1-Q4 어닝콜 transcript + Leerink/JPM/Goldman/
   TD Cowen/Cantor/Wells Fargo/Citi 등 학회 발표 transcript 모두 포함.
   ticker → equity slug는 search API로 자동 매핑 후 캐시.
2) **Yahoo Finance** (Finviz quote 경유) — fallback. 분기 자동 요약만.

추출된 forward-looking 멘션은 catalysts 테이블에 event_type='earnings_call'로 저장
(소스 URL을 source 필드에 기록해 어닝콜/학회 구분).
"""
from __future__ import annotations

import datetime as dt
import logging
import re
from typing import Iterable

import db

log = logging.getLogger(__name__)

# Yahoo summary URL 패턴 (finance.yahoo.com/m/ 또는 /news/)
YH_SUMMARY_HOST_PAT = re.compile(
    r"finance\.yahoo\.com/(?:m/|news/)", re.IGNORECASE
)
# 진성 어닝콜 요약/하이라이트 (관리진 발언) — 프리뷰/추정 기사 제외
EARNINGS_INCLUDE = (
    "earnings call summary",
    "earnings call highlights",
    "earnings call:",
    "earnings call -",
)
EARNINGS_EXCLUDE = (
    "preview", "what to expect", "everything you need",
    "ahead of earnings", "report preview", "you need to know",
    "to watch", "analyst", "estimate", "consensus",
    "12 month price targets",
)

# Forward-looking 일자 패턴 (catalysts.py와 동일 + 보강)
DATE_PAT = re.compile(
    r"(?:Q[1-4]|1H|2H|early|mid|late|year[\s\-]?end|"
    r"first half|second half|first quarter|second quarter|"
    r"third quarter|fourth quarter|by\s+(?:the\s+)?end|"
    r"by\s+mid|in\s+the\s+coming|in\s+early|in\s+late|in\s+mid)\s*"
    r"(?:of\s+)?(?:20\d{2}|'?2[5-9])",
    re.IGNORECASE,
)
# Asset/readout 키워드 — 임상 데이터/규제 이벤트 단서
ASSET_KEYWORDS = re.compile(
    r"(?:phase\s*[1-3]|readout|topline|interim|initial\s+data|"
    r"primary\s+endpoint|filing|approval|launch|enrollment|"
    r"data\s+(?:read|release|update|disclosure|presentation)|"
    r"first\s+(?:patient|dose)|expansion\s+cohort|pivotal|"
    r"start\s+(?:phase|the\s+phase)|initiate|"
    r"NDA|BLA|sNDA|IND|sBLA|advisory\s+committee|adcom|PDUFA)",
    re.IGNORECASE,
)


# ─────────────────────── investing.com (학회 + 어닝콜 transcripts) ───────────────────────
def resolve_investing_slug(ticker: str) -> str | None:
    """ticker → investing.com equity slug. ticker_master.investing_slug에 캐시."""
    from curl_cffi import requests as crq
    tk = ticker.upper()
    # 1) DB 캐시
    try:
        with db.connect() as conn:
            row = conn.execute(
                "SELECT investing_slug FROM ticker_master WHERE ticker = ?", (tk,)
            ).fetchone()
            if row and row.get("investing_slug"):
                return row["investing_slug"]
    except Exception:
        pass
    # 2) /search/service/search API
    try:
        r = crq.post(
            "https://www.investing.com/search/service/search",
            impersonate="chrome", timeout=15,
            headers={"X-Requested-With": "XMLHttpRequest",
                     "Referer": "https://www.investing.com/"},
            data={"search_text": tk},
        )
        r.raise_for_status()
        data = r.json()
        results = data.get("All", [])
        for it in results:
            if (it.get("pair_type") == "equities"
                    and (it.get("symbol") or "").upper() == tk):
                link = it.get("link") or ""   # /equities/arrowhead-research-corp
                slug = link.rsplit("/", 1)[-1] if link else None
                if slug:
                    try:
                        with db.connect() as conn:
                            conn.execute(
                                "UPDATE ticker_master SET investing_slug = ? "
                                "WHERE ticker = ?", (slug, tk),
                            )
                    except Exception:
                        pass
                    return slug
    except Exception as e:
        log.debug("investing slug %s: %s", tk, e)
    return None


def fetch_investing_transcripts(ticker: str, max_pages: int = 3,
                                 max_results: int = 8) -> list[dict]:
    """investing.com 뉴스 페이지 스캔 → /news/transcripts/ 링크 수집.
    어닝콜 + 학회 발표(Leerink/JPM/TD Cowen/Goldman 등) transcripts."""
    from curl_cffi import requests as crq
    from bs4 import BeautifulSoup
    slug = resolve_investing_slug(ticker)
    if not slug:
        return []
    # URL → title (같은 URL이 image 링크 + 제목 링크 두 번 나오므로 dict로 dedupe,
    # 텍스트 있는 쪽으로 덮어쓰기)
    url_to_title: dict[str, str] = {}
    for page in range(1, max_pages + 1):
        url = f"https://www.investing.com/equities/{slug}-news/{page}"
        try:
            r = crq.get(url, impersonate="chrome", timeout=15)
            if r.status_code != 200:
                continue
        except Exception:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/news/transcripts/" not in href:
                continue
            full = href if href.startswith("http") else f"https://www.investing.com{href}"
            text = a.get_text(" ", strip=True)
            existing = url_to_title.get(full, "")
            if text and len(text) > len(existing):
                url_to_title[full] = text[:240]
            elif full not in url_to_title:
                url_to_title[full] = ""
    # 텍스트 없는 항목은 제외
    out = [{"url": u, "title": t} for u, t in url_to_title.items() if t]
    return out[:max_results]


# ─────────────────────── Yahoo (Finviz 경유) ───────────────────────
def fetch_yahoo_summaries_via_finviz(ticker: str, limit: int = 4) -> list[dict]:
    """Finviz quote 페이지의 news 테이블에서 Yahoo earnings summary 링크 추출.
    최근 N개 분기 (보통 1-4건)."""
    from curl_cffi import requests as crq
    from bs4 import BeautifulSoup
    url = f"https://finviz.com/quote.ashx?t={ticker.upper()}"
    try:
        r = crq.get(url, impersonate="chrome", timeout=15)
        r.raise_for_status()
    except Exception as e:
        log.warning("finviz quote %s 실패: %s", ticker, e)
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    out: list[dict] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = (a.get_text(" ", strip=True) or "").lower()
        if not YH_SUMMARY_HOST_PAT.search(href):
            continue
        if not any(kw in text for kw in EARNINGS_INCLUDE):
            continue
        if any(kw in text for kw in EARNINGS_EXCLUDE):
            continue
        if href in seen:
            continue
        seen.add(href)
        out.append({
            "url": href,
            "title": a.get_text(" ", strip=True)[:200],
        })
        if len(out) >= limit:
            break
    return out


def extract_forward_looking(text: str) -> list[dict]:
    """본문 텍스트에서 forward-looking 문장 추출.
    각 문장 = {date_hint, asset_keyword?, sentence}."""
    if not text:
        return []
    # 문장 분리 (기간/!/? 기준 + 줄바꿈)
    sentences = re.split(r"(?<=[.!?])\s+|\n+", text)
    out: list[dict] = []
    seen: set[str] = set()
    for sent in sentences:
        s = sent.strip()
        if len(s) < 30 or len(s) > 600:
            continue
        date_m = DATE_PAT.search(s)
        if not date_m:
            continue
        s_lower = s.lower()
        # 1) 페이지 보일러플레이트 (Yahoo 페이지 헤더 / 광고 / 네비)
        boilerplate = (
            "earnings call summary" in s_lower
            or "earnings call highlights" in s_lower
            or "oops, something went wrong" in s_lower
            or "skip to navigation" in s_lower
            or "moby intelligence" in s_lower
            or "min read" in s_lower
            or "story continues" in s_lower and len(s) < 80
            or "potential to be the next nvidia" in s_lower
            or "our analysts just identified" in s_lower
        )
        if boilerplate:
            continue
        # 2) 재무 회상 멘션 제외 (Q1 2026 매출 같은건 forward-looking 아님)
        is_financial_recap = (
            ("$" in s or "million" in s_lower or "billion" in s_lower)
            and not re.search(r"(expect|plan|will|anticipate|target|aim|"
                              r"by the end|in the (coming|second half|"
                              r"first half|fourth quarter|third quarter)|"
                              r"upcoming|next|forthcoming|slated|scheduled|"
                              r"initiate)", s_lower)
        )
        if is_financial_recap:
            continue
        # 3) 가까운 미래(2026 이전)는 이미 지난 분기일 수 있어 제외하기엔 risky.
        #    그래서 keep — UI에서 날짜 필터로 처리.
        asset_m = ASSET_KEYWORDS.search(s)
        key = re.sub(r"\s+", " ", s.lower())[:120]
        if key in seen:
            continue
        seen.add(key)
        out.append({
            "date_hint": date_m.group(0),
            "has_asset_keyword": bool(asset_m),
            "asset_match": asset_m.group(0) if asset_m else "",
            "sentence": s[:500],
        })
    # asset 키워드 있는 것 우선 정렬
    out.sort(key=lambda x: (not x["has_asset_keyword"], x["sentence"]))
    return out


def _date_hint_to_iso(hint: str) -> str:
    """일자 힌트 → ISO 날짜 (정렬용 proxy). 학회명 스킵 (이 모듈에선 quarter/half만)."""
    h = hint.lower().strip()
    # YY 약어 ('27, '28)
    h_full = re.sub(r"'(2\d)", r"20\1", h)
    m = re.search(r"q([1-4])\s*(20\d{2})", h_full)
    if m:
        q, y = int(m.group(1)), int(m.group(2))
        end_month = q * 3
        end_day = 31 if end_month in (3, 12) else 30
        return f"{y}-{end_month:02d}-{end_day:02d}"
    m = re.search(r"(1h|first\s*half)\s*(20\d{2})", h_full)
    if m:
        return f"{int(m.group(2))}-06-30"
    m = re.search(r"(2h|second\s*half)\s*(20\d{2})", h_full)
    if m:
        return f"{int(m.group(2))}-12-31"
    m = re.search(r"(early|mid|late|year[\s\-]?end)\s*(?:of\s+)?\s*(20\d{2})", h_full)
    if m:
        kw, y = m.group(1), int(m.group(2))
        if "early" in kw:
            return f"{y}-03-31"
        if "mid" in kw:
            return f"{y}-06-30"
        return f"{y}-12-31"
    m = re.search(r"(20\d{2})", h_full)
    if m:
        return f"{m.group(1)}-12-31"
    return "2099-12-31"


def _save_transcript_extractions(ticker: str, transcripts: list[dict]) -> int:
    """transcripts = [{url, title}, ...] 본문 fetch → forward-looking → DB 저장."""
    if not transcripts:
        return 0
    from bot_tools import fetch_url
    saved = 0
    now = dt.datetime.now().isoformat(timespec="seconds")
    with db.connect() as conn:
        for t in transcripts:
            res = fetch_url(t["url"], max_chars=30000)
            text = res.get("text", "")
            if not text:
                continue
            items = extract_forward_looking(text)
            for it in items:
                ev_date = _date_hint_to_iso(it["date_hint"])
                title = it["sentence"][:300]
                desc = f"date_hint: {it['date_hint']} · 출처: {t['title'][:80]}"
                try:
                    conn.execute(
                        "INSERT INTO catalysts (ticker, event_date, event_type, title, "
                        "description, source, fetched_at) VALUES (?,?,?,?,?,?,?) "
                        "ON CONFLICT (ticker, event_date, event_type, title) DO NOTHING",
                        (ticker.upper(), ev_date, "earnings_call",
                         title, desc, t["url"][:300], now),
                    )
                    saved += 1
                except Exception as e:
                    log.debug("upsert: %s", e)
    return saved


def fetch_for_ticker(ticker: str, max_quarters: int = 3,
                     max_investing: int = 8) -> int:
    """ticker 1종목 — investing.com (어닝콜 + 학회) + Yahoo summary 통합 수집."""
    saved = 0
    # 1) investing.com — 가장 풍부 (어닝콜 transcripts + Leerink/JPM/등 학회 발표)
    investing = fetch_investing_transcripts(ticker, max_pages=3,
                                             max_results=max_investing)
    if investing:
        saved += _save_transcript_extractions(ticker, investing)
    # 2) Yahoo (Finviz 경유) — fallback / 보강
    yahoo = fetch_yahoo_summaries_via_finviz(ticker, limit=max_quarters)
    if yahoo:
        saved += _save_transcript_extractions(ticker, yahoo)
    log.info("%s earnings_call: investing=%d yahoo=%d total saved=%d",
             ticker, len(investing), len(yahoo), saved)
    return saved


def fetch_for_tickers(tickers: Iterable[str], max_per_ticker: int = 3) -> int:
    total = 0
    for t in tickers:
        try:
            total += fetch_for_ticker(t, max_quarters=max_per_ticker)
        except Exception as e:
            log.exception("fetch %s 실패: %s", t, e)
    return total


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    tk = sys.argv[1] if len(sys.argv) > 1 else "RVMD"
    print(f"=== {tk} ===")
    summaries = fetch_yahoo_summaries_via_finviz(tk, limit=4)
    print(f"Yahoo summaries: {len(summaries)}")
    for s in summaries:
        print(f"  - {s['title'][:80]}")
    n = fetch_for_ticker(tk, max_quarters=3)
    print(f"\nsaved: {n}")
    # 미리보기
    df = db.pd_read_sql(
        "SELECT event_date, title, description FROM catalysts "
        "WHERE ticker=? AND event_type='earnings_call' ORDER BY event_date",
        params=(tk.upper(),),
    )
    for _, r in df.head(15).iterrows():
        print(f"  [{r['event_date']}] {r['title'][:200]}")
