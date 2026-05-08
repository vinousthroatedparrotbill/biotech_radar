"""뉴스 멘션 + 약물명 추출 + 기전 분류.
출처: Yahoo Finance(yfinance Ticker.news) + Finviz Elite(per-ticker CSV) + Google News RSS."""
from __future__ import annotations

import csv
import io
import os
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote_plus

import feedparser
import requests
import yfinance as yf
from curl_cffi import requests as crequests
from dotenv import load_dotenv

from drugs_db import DRUG_MOA, SUFFIX_RULES, CONTEXT_KEYWORDS, classify

load_dotenv(Path(__file__).parent / ".env", override=True)

GNEWS_URL = "https://news.google.com/rss/search"
FINVIZ_NEWS_URL = "https://elite.finviz.com/news_export.ashx"


def _finviz_token() -> str:
    return (os.environ.get("FINVIZ_AUTH_TOKEN") or "").strip()

# INN 접미사 — 약물명 후보 추출용
DRUG_SUFFIX = re.compile(
    r"\b([a-z][a-z\-]{4,30}"
    r"(?:mab|nib|tinib|degib|lisib|sertib|ciclib|rafenib|kib|"
    r"plimab|tuximab|izumab|lumab|olimab|sumab|"
    r"caftor|trogene|gepant|siran|rsen|ersen|"
    r"glutide|glipron|cabtagene|vedotin|deruxtecan|govitecan|"
    r"tesirine|emtansine|soravtansine|"
    r"sertib|rasib|metinib|brutinib))\b",
    re.IGNORECASE,
)
# 회사 코드형: ABBV-1234, LY3527723
DRUG_CODE = re.compile(r"\b([A-Z]{2,5}[\- ]?\d{3,6}[A-Z]?)\b")

# 브랜드명/Trade name pattern: 캐피탈로 시작, ASCII, 5+ chars (고유명사 형태)
BRAND_NAME = re.compile(r"\b([A-Z][a-z]{4,15}(?:[a-z]{0,5})?)\b")

STOPWORDS = {
    "company", "common", "stock", "shares", "price", "market", "trial",
    "study", "report", "data", "results", "phase", "drug", "therapy",
    "patient", "healthcare", "biotech", "biotechnology", "research",
    "pharmaceuticals", "pharma", "today", "yesterday", "announce",
    "announces", "announced", "reports", "first", "second", "third",
    "quarter", "annual", "global", "press", "release",
}
CODE_STOP_PREFIXES = {
    # 회계연도
    "CY", "FY", "Q1", "Q2", "Q3", "Q4",
    # 의학 학회 (American/European/World 등 — 약물 코드와 형태 충돌)
    "AAN",      # American Academy of Neurology
    "AACR",     # American Association for Cancer Research
    "ASCO", "ASCO-GI", "ASCO-GU",  # American Society of Clinical Oncology
    "ESMO",     # European Society for Medical Oncology
    "AHA",      # American Heart Association
    "JPM",      # JP Morgan Healthcare Conf
    "ASH",      # American Society of Hematology
    "EHA",      # European Hematology Association
    "ASCB",     # Cell Biology
    "ESC",      # European Society of Cardiology
    "AAD",      # American Academy of Dermatology
    "AAO",      # American Academy of Ophthalmology
    "ACR",      # American College of Rheumatology
    "ADA",      # American Diabetes Association
    "ASN",      # American Society of Nephrology
    "ATS",      # American Thoracic Society
    "DDW",      # Digestive Disease Week
    "ENDO",     # Endocrine Society
    "EULAR",    # European Rheumatology
    "ERS",      # European Respiratory Society
    "WCLC",     # World Conference on Lung Cancer
    "SABCS",    # San Antonio Breast Cancer Symposium
    "ASTRO",    # Radiation Oncology
    "SNMMI",    # Nuclear Medicine
    "ISTH",     # Thrombosis and Haemostasis
    "ISMRM",    # MR in Medicine
    "RSNA",     # Radiological Society
    "AAAAI",    # Allergy/Asthma
    "AAGP",     # Geriatric Psychiatry
    "AANP",     # Nurse Practitioners
    "ASGCT",    # Gene & Cell Therapy
    "BIO",      # BIO International
    "ESH",      # European Society of Haematology
    "EASL",     # European Liver
    "AASLD",    # American Liver
    # 규제
    "FDA", "EMA", "NDA", "BLA", "IND", "ANDA", "PDUFA", "CHMP",
    # 금융 / 표준
    "SEC", "GAAP", "EPS", "EBIT", "EBITDA", "USD", "EUR", "ADR",
    "ISO", "PCT", "WIPO",
}


# ───────────────────────── 뉴스 fetch ─────────────────────────
def fetch_yahoo_news(ticker: str) -> list[dict]:
    """Yahoo via yfinance — 최근 ~10건. 각 item에는 title + summary가 있음 (신규 형식: item['content'])."""
    try:
        items = yf.Ticker(ticker).news or []
    except Exception:
        return []
    out: list[dict] = []
    for it in items:
        c = it.get("content", it)   # 신규 schema는 content 안에
        title = c.get("title", "")
        summary = c.get("summary") or c.get("description", "")
        link = c.get("canonicalUrl", {}).get("url") if isinstance(c.get("canonicalUrl"), dict) else c.get("link", "")
        pub = c.get("pubDate") or c.get("displayTime") or ""
        published_dt = None
        if pub:
            try:
                published_dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
            except Exception:
                pass
        elif c.get("providerPublishTime"):
            published_dt = datetime.fromtimestamp(c["providerPublishTime"], tz=timezone.utc)
        out.append({
            "title": title, "summary": summary, "link": link,
            "source": "Yahoo Finance",
            "published": published_dt.isoformat() if published_dt else "",
            "_published_dt": published_dt,
        })
    return out


def fetch_google_news(query: str, days: int = 30, limit: int = 100) -> list[dict]:
    when = f"when:{days}d"
    q = f"{query} {when}"
    url = f"{GNEWS_URL}?q={quote_plus(q)}&hl=en-US&gl=US&ceid=US:en"
    feed = feedparser.parse(url)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    out: list[dict] = []
    for e in feed.entries[:limit]:
        published = None
        if hasattr(e, "published_parsed") and e.published_parsed:
            published = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
            if published < cutoff:
                continue
        title = getattr(e, "title", "")
        summary = re.sub("<[^>]+>", "", getattr(e, "summary", ""))
        out.append({
            "title": title, "summary": summary,
            "link": getattr(e, "link", ""),
            "source": "Google News",
            "published": published.isoformat() if published else "",
            "_published_dt": published,
        })
    return out


def fetch_finviz_news(ticker: str, days: int = 180) -> list[dict]:
    """Finviz Elite per-ticker news. Returns list of {title, summary, link, source, published, _published_dt}.
    토큰 없거나 실패 시 빈 리스트."""
    token = _finviz_token()
    if not token:
        return []
    try:
        r = requests.get(
            FINVIZ_NEWS_URL,
            params={"v": "3", "t": ticker, "auth": token},
            timeout=15,
        )
        r.raise_for_status()
    except Exception:
        return []

    out: list[dict] = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    reader = csv.reader(io.StringIO(r.text))
    header = next(reader, None)   # skip header row
    for row in reader:
        if len(row) < 5:
            continue
        title, source, date_s, url, category = row[0], row[1], row[2], row[3], row[4]
        try:
            # 'YYYY-MM-DD HH:MM:SS' (Eastern time, UTC로 근사)
            dt = datetime.strptime(date_s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if dt < cutoff:
            continue
        out.append({
            "title": title,
            "summary": "",   # Finviz CSV는 summary 미제공 — title만으로 추출
            "link": url,
            "source": f"Finviz/{source}",
            "published": dt.isoformat(),
            "_published_dt": dt,
        })
    return out


def fetch_combined(ticker: str, name: str, days: int) -> list[dict]:
    """Yahoo + Finviz + Google News 합치고 제목 유사 중복 제거."""
    yahoo = fetch_yahoo_news(ticker)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    yahoo = [n for n in yahoo if n.get("_published_dt") is None or n["_published_dt"] >= cutoff]
    finviz = fetch_finviz_news(ticker, days=days)
    google = fetch_google_news(name, days=days, limit=200)
    seen_titles: set[str] = set()
    out = []
    # 우선순위: Finviz(가장 신뢰) → Yahoo → Google
    for n in finviz + yahoo + google:
        key = re.sub(r"[^a-z0-9]", "", (n["title"] or "").lower())[:60]
        if not key or key in seen_titles:
            continue
        seen_titles.add(key)
        out.append(n)
    return out


# ───────────────────────── 약물명 추출 ─────────────────────────
def _extract_drugs(text: str) -> set[str]:
    found: set[str] = set()
    text_lc = (text or "")
    # INN suffix
    for m in DRUG_SUFFIX.finditer(text_lc):
        name = m.group(1).lower()
        if name in STOPWORDS or len(name) < 6:
            continue
        found.add(name)
    # Code form
    for m in DRUG_CODE.finditer(text_lc):
        code = m.group(1).upper().replace(" ", "-")
        prefix_m = re.match(r"^([A-Z]+)", code)
        if not prefix_m:
            continue
        prefix = prefix_m.group(1)
        if prefix in CODE_STOP_PREFIXES:
            continue
        # 4자리 숫자가 연도 범위면 거부 (XXXX-2026 같은 학회/연도 표기 차단)
        digits_m = re.search(r"(\d+)", code)
        if digits_m:
            digits = digits_m.group(1)
            if len(digits) == 4 and 1990 <= int(digits) <= 2100:
                continue
        found.add(code)
    # Brand names — drugs_db에 등록된 브랜드명만 픽업
    text_norm = " " + (text or "").lower() + " "
    for brand in DRUG_MOA.keys():
        if " " in brand or "-" in brand:
            continue   # 멀티워드는 substring으로 잡기
        if f" {brand} " in text_norm:
            found.add(brand)
    # 멀티워드 / 하이픈 약물명
    for k in DRUG_MOA.keys():
        if (" " in k or "-" in k) and k in text_norm:
            found.add(k)
    return found


# ───────────────────────── TOP 3 + 기전 ─────────────────────────
def top_pipelines(ticker: str, name: str, days: int) -> list[dict]:
    """Returns [{drug, mentions, moa, sample_link}] (top 3, by mention count).
    파이프라인 페이지에서 검증 — 회사 자체 파이프라인에 없는 약물(타사 임상 등) 거름."""
    items = fetch_combined(ticker, name, days)
    drug_counter: Counter[str] = Counter()
    drug_context: dict[str, str] = {}
    drug_links: dict[str, str] = {}
    for it in items:
        text = f"{it['title']}. {it['summary']}"
        drugs = _extract_drugs(text)
        for d in drugs:
            drug_counter[d] += 1
            if len(text) > len(drug_context.get(d, "")):
                drug_context[d] = text
                drug_links[d] = it.get("link", "")

    # 파이프라인 검증 — 회사 페이지에 있는 약물만 통과
    if drug_counter:
        valid = _validate_against_pipeline(ticker, set(drug_counter.keys()))
        drug_counter = Counter({d: c for d, c in drug_counter.items() if d in valid})

    out = []
    for drug, cnt in drug_counter.most_common(3):
        moa = classify(drug, context=drug_context.get(drug, ""))
        out.append({
            "drug": drug,
            "mentions": cnt,
            "moa": moa or "—",
            "sample_link": drug_links.get(drug, ""),
        })
    return out


def news_count(ticker: str, name: str, days: int) -> int:
    """단순 뉴스 건수 (기전 분석용 보조 표시)."""
    return len(fetch_combined(ticker, name, days))


# ───────────────────────── Pipeline 검증 ─────────────────────────
def _strip_html(html: str) -> str:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(" ", strip=True).lower()


def _fetch_pipeline_text(pipeline_url: str) -> str:
    """파이프라인 페이지 본문 텍스트(소문자).
    1) curl_cffi 정적 fetch
    2) 약물명 패턴 부족하면 Playwright 헤드리스 렌더 fallback (JS-only 사이트 대응)
    """
    if not pipeline_url:
        return ""
    # tier 1: 정적 fetch
    try:
        r = crequests.get(pipeline_url, impersonate="chrome", timeout=10)
        r.raise_for_status()
        text = _strip_html(r.text)
    except Exception:
        text = ""
    if _pipeline_is_parseable(text):
        return text
    # tier 2: Playwright fallback
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
            )
            ctx = browser.new_context(
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/131.0.0.0 Safari/537.36"),
                viewport={"width": 1366, "height": 900},
            )
            page = ctx.new_page()
            page.goto(pipeline_url, wait_until="domcontentloaded", timeout=20000)
            page.wait_for_timeout(3500)
            content = page.content()
            browser.close()
        rendered = _strip_html(content)
        if _pipeline_is_parseable(rendered):
            return rendered
    except Exception:
        pass
    return text   # 최후엔 정적 결과 (빈 문자열일 수 있음)


def _pipeline_is_parseable(pipeline_text: str) -> bool:
    """페이지에 약물명 패턴 2개 이상 있으면 정적 렌더 OK로 판정.
    그 외엔 JS-only/빈 페이지로 보고 필터 적용 안 함."""
    if not pipeline_text or len(pipeline_text) < 300:
        return False
    n = len(DRUG_SUFFIX.findall(pipeline_text)) + len(DRUG_CODE.findall(pipeline_text))
    return n >= 2


def _drug_in_pipeline(drug: str, pipeline_text: str) -> bool:
    """약물명이 pipeline 페이지에 등장하는지 — alias 포함 (drugs_db)."""
    drug_lc = drug.lower()
    if drug_lc in pipeline_text:
        return True
    # drugs_db에서 동일 약물의 다른 표기 (브랜드↔generic 양쪽) 매칭
    for k, _moa in DRUG_MOA.items():
        kl = k.lower()
        if kl == drug_lc or drug_lc in kl or kl in drug_lc:
            if kl in pipeline_text:
                return True
    return False


def _competitor_ticker_prefix(drug: str, own_ticker: str, all_tickers: set[str]) -> bool:
    """drug가 'XXX-NNNN' 또는 'XXXNNNN' 형태이고 XXX가 universe의 다른 ticker(2~5자)면 True.
    예: ERAS-0015 (RVMD 뉴스에서 등장) → ERAS는 Erasca ticker → 거름."""
    m = re.match(r"^([A-Z]+)[-]?\d", drug.upper())
    if not m:
        return False
    prefix = m.group(1)
    if prefix == own_ticker.upper():
        return False
    if 2 <= len(prefix) <= 5 and prefix in all_tickers:
        return True
    return False


def _validate_against_pipeline(ticker: str, drugs: set[str]) -> set[str]:
    """약물 검증 (필터 단계):
    1) 코드형 약물의 prefix가 universe 다른 ticker면 거름 (ERAS-0015 류)
    2) 회사 파이프라인 페이지에 있는 약물만 통과 (페이지 파싱 불가능하면 통과)
    """
    # 1) ticker prefix 필터 — 빠르고 확실한 케이스부터
    try:
        from db import connect
        with connect() as conn:
            all_tickers = {
                r["ticker"] for r in
                conn.execute("SELECT ticker FROM ticker_master").fetchall()
            }
    except Exception:
        all_tickers = set()
    drugs = {d for d in drugs if not _competitor_ticker_prefix(d, ticker, all_tickers)}

    # 2) 파이프라인 페이지 검증
    try:
        import ticker_urls
        pl_url = ticker_urls.get(ticker).get("pipeline_url", "")
    except Exception:
        return drugs
    if not pl_url:
        return drugs
    text = _fetch_pipeline_text(pl_url)
    if not _pipeline_is_parseable(text):
        return drugs   # JS 렌더 후에도 빈 페이지면 필터 보류
    return {d for d in drugs if _drug_in_pipeline(d, text)}


# ───────────────────────── Daily News (universe-wide) ─────────────────────────
NEWS_CATEGORIES: dict[str, list[str]] = {
    "M&A": [
        r"\b(?:to\s+)?acquir(?:e|es|ed|ing|ition)\b",
        r"\bmerger?\b",
        r"\bbuyout\b",
        r"\btakeover\b",
        r"\bagree(?:s|d)?\s+to\s+(?:buy|acquire|purchase)\b",
        r"\b(?:agree|definitive)\s+agreement\s+to\s+acquire\b",
        r"\bsnaps?\s+up\b",
        r"\b(?:to\s+)?buy\s+(?:biotech|pharma|drugmaker|maker)\b",
        r"\bbillion-?dollar\s+deal\b",
        r"\b\$\d+(?:\.\d+)?\s*(?:b|bn|billion)\s+(?:deal|acquisition)\b",
    ],
    "라이센싱": [
        r"\bin-?licens(?:e|es|ed|ing)\b",
        r"\bout-?licens(?:e|es|ed|ing)\b",
        r"\blicens(?:e|es|ed|ing)\s+(?:agreement|deal|pact)\b",
        r"\b(?:enters?\s+into|signs?)\s+(?:a\s+)?licens",
        r"\bexclusive\s+(?:license|rights|option)\b",
        r"\bworldwide\s+(?:license|rights)\b",
        r"\boption\s+(?:and\s+)?licens",
        r"\bgrant(?:s|ed)?\s+.+\s+licens",
        r"\b(?:obtains?|secures?|gains?)\s+(?:exclusive\s+)?rights\b",
        r"\bsublicens(?:e|es|ed|ing)\b",
    ],
    "라이센싱 종료": [
        r"\bterminat(?:e|es|ed|ion)\s+(?:.+)?(?:agreement|license|partnership|collaboration|deal)\b",
        r"\breturn(?:s|ed)?\s+(?:.+)?rights\b",
        r"\bend(?:s|ed)?\s+(?:.+)?(?:collaboration|agreement|partnership|deal)\b",
        r"\bdiscontinu(?:e|es|ed)\s+.+\s+(?:program|trial|development)\b",
        r"\bopt(?:s|ed)?\s+out\b",
    ],
    "파트너십": [
        r"\bcollabor(?:ate|ation|ative)\b",
        r"\bpartner(?:s|ship)?\b",
        r"\bstrategic\s+alliance\b",
        r"\bjoint\s+venture\b",
        r"\bco-?develop(?:ment|ing)?\b",
        r"\bco-?market(?:ing)?\b",
        r"\bco-?commercializ\w*\b",
        r"\bresearch\s+(?:and\s+)?(?:dev|development)\s+agreement\b",
        r"\bteam(?:s|ed)?\s+up\s+with\b",
        r"\bjoins?\s+forces\b",
    ],
    "임상 결과": [
        r"\bphase\s*(?:1|2|3|i{1,3}|iv)\b",
        r"\btopline\s+(?:data|results)\b",
        r"\bdata\s+readout\b",
        r"\bprimary\s+endpoint\b",
        r"\b(?:trial|study)\s+(?:results|data)\b",
        r"\binterim\s+(?:data|results|analysis)\b",
        r"\bpivotal\b",
        r"\bmet\s+(?:its\s+)?primary\b",
        r"\b(?:achieves?|achieved|achieving)\s+(?:.+)?(?:endpoint|response|efficacy)\b",
        r"\b(?:reports?|presents?|announces?)\s+(?:.+)?(?:data|results|outcomes)\b",
        r"\b(?:positive|negative)\s+(?:.+)?data\b",
        r"\b(?:fail|failed|fails)\s+(?:to\s+)?(?:meet|achieve)\b",
        r"\boverall\s+survival\b",
        r"\bprogression-?free\s+survival\b|\bpfs\b",
    ],
    "FDA / 규제": [
        r"\bfda\s+(?:approves?|approval|grants?|clears?|clearance|accepts?)\b",
        r"\b(?:bla|nda|sNDA|cBLA)\s+(?:filing|accept|submit|approval)",
        r"\bfast\s+track\b",
        r"\bbreakthrough\s+(?:therapy|designation)\b",
        r"\borphan\s+drug\s+designation\b",
        r"\bpriority\s+review\b",
        r"\bpdufa\b",
        r"\bcrl\b|\bcomplete\s+response\s+letter\b",
        r"\baccelerated\s+approval\b",
        r"\b(?:ema|chmp)\s+(?:approves?|recommendation|opinion)\b",
        r"\b(?:withdraws?|withdraw)\s+.+\s+(?:application|filing)\b",
    ],
}


def categorize(title: str) -> list[str]:
    """제목에서 매칭되는 카테고리 라벨 반환 (다중 가능)."""
    if not title:
        return []
    cats: list[str] = []
    for cat, patterns in NEWS_CATEGORIES.items():
        for p in patterns:
            if re.search(p, title, re.I):
                cats.append(cat)
                break
    return cats


# 바이오/제약 관련 키워드 — 비-바이오 false positive (LG에너지·자동차·IT 등) 거름
BIOTECH_KEYWORDS = re.compile(
    r"\b(?:"
    # 일반 의료/바이오 용어
    r"bio(?:tech|pharma|tech\w*)?|pharma(?:ceutical)?s?|drug(?:maker|s)?|"
    r"medicine|therap(?:y|eutic|ies)|clinical|trial|fda|ema|chmp|"
    r"phase\s*[123]|cancer|oncology|tumor|vaccine|immun(?:o|e)|"
    r"gene\s+therapy|cell\s+therapy|crispr|monoclonal|antibody|antibodies|"
    r"diagnost(?:ic|ics)?|patholog|radiolog|surgical|"
    r"healthcare|health\s+care|life\s+science|medical|"
    # INN suffix
    r"\w+(?:mab|nib|tinib|caftor|gene)\b|"
    # 빅파마 회사명 (자주 등장)
    r"roche|pfizer|merck|novartis|lilly|astrazeneca|sanofi|bayer|takeda|"
    r"novo\s+nordisk|abbvie|gilead|amgen|regeneron|biogen|vertex|moderna|"
    r"bristol[\s-]?myers|johnson\s*&\s*johnson|j&j|gsk|glaxosmithkline|"
    r"daiichi\s+sankyo|eisai|chugai|servier|boehringer|teva|"
    r"angelini|recordati|"
    # 임상·승인 관련
    r"primary\s+endpoint|topline|readout|approved\s+(?:for|in)"
    r")",
    re.I,
)


def _is_biotech_relevant(item: dict) -> bool:
    """비-바이오 잡음 거름.
    - Finviz per-ticker 결과: 항상 통과 (universe ticker 기반이라 이미 biotech)
    - 바이오 전문 RSS: 항상 통과 (도메인 자체가 biotech)
    - Google News 결과: 제목에 biotech 키워드 있어야 통과"""
    source = item.get("source", "") or ""
    if source.startswith("Finviz/"):
        return True
    if source in BIOTECH_RSS_FEEDS:
        return True
    title = item.get("title", "") or ""
    return bool(BIOTECH_KEYWORDS.search(title))


# Google News 카테고리 검색 — 비상장·해외 거래 보강
GNEWS_BIOTECH_QUERIES = [
    "biotech acquisition",
    "pharma acquires",
    "biotech licensing deal",
    "biotech partnership",
    "biotech phase 3 results",
    "FDA approval biotech",
    "drugmaker acquires",
]


def fetch_gnews_biotech_categorized(days: int = 7, per_query: int = 30) -> list[dict]:
    """카테고리 키워드 query로 Google News RSS → categorize 매칭만."""
    out: list[dict] = []
    for q in GNEWS_BIOTECH_QUERIES:
        items = fetch_google_news(q, days=days, limit=per_query)
        for it in items:
            cats = categorize(it.get("title", ""))
            if not cats:
                continue
            it = dict(it)
            it["categories"] = cats
            it["tickers"] = []
            out.append(it)
    return out


# 바이오 전문 매체 RSS — Finviz로는 못 잡는 비상장 거래·해외 업계 뉴스 보강
BIOTECH_RSS_FEEDS = {
    "FiercePharma":   "https://www.fiercepharma.com/rss/xml",
    "FierceBiotech":  "https://www.fiercebiotech.com/rss/xml",
    "Endpoints":      "https://endpts.com/feed/",
    "BioPharma Dive": "https://www.biopharmadive.com/feeds/news/",
    "STAT":           "https://www.statnews.com/feed/",
    "BioSpace":       "https://www.biospace.com/rss/news",
}


def fetch_biotech_rss(days: int = 7, max_per_feed: int = 100) -> list[dict]:
    """바이오 전문 매체 RSS feed 합쳐서 카테고리 매칭만 반환."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    out: list[dict] = []
    for source, url in BIOTECH_RSS_FEEDS.items():
        try:
            feed = feedparser.parse(url)
        except Exception:
            continue
        for e in feed.entries[:max_per_feed]:
            published = None
            if hasattr(e, "published_parsed") and e.published_parsed:
                published = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
                if published < cutoff:
                    continue
            title = getattr(e, "title", "")
            cats = categorize(title)
            if not cats:
                continue
            summary = re.sub("<[^>]+>", "", getattr(e, "summary", ""))
            out.append({
                "title": title,
                "summary": summary[:300],
                "link": getattr(e, "link", ""),
                "source": source,
                "published": published.isoformat() if published else "",
                "_published_dt": published,
                "tickers": [],   # RSS는 ticker 태그 없음
                "categories": cats,
            })
    return out


def _fetch_finviz_universe_news(days: int, min_mcap: float,
                                max_workers: int) -> list[dict]:
    """Finviz per-ticker news 병렬 fetch (universe ≥ min_mcap)."""
    if not _finviz_token():
        return []
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from db import connect
    try:
        with connect() as conn:
            rows = conn.execute(
                "SELECT ticker FROM ticker_master WHERE market_cap >= %s",
                (min_mcap,),
            ).fetchall()
            tickers = [r["ticker"] for r in rows]
    except Exception:
        return []
    if not tickers:
        return []

    out: list[dict] = []
    seen: set[str] = set()
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(fetch_finviz_news, t, days): t for t in tickers}
        for fut in as_completed(futs):
            tk = futs[fut]
            try:
                items = fut.result()
            except Exception:
                continue
            for it in items:
                title = (it.get("title") or "").strip()
                if not title:
                    continue
                cats = categorize(title)
                if not cats:
                    continue
                key = re.sub(r"[^a-z0-9]", "", title.lower())[:60]
                if not key or key in seen:
                    for o in out:
                        if re.sub(r"[^a-z0-9]", "", o["title"].lower())[:60] == key:
                            if tk not in o["tickers"]:
                                o["tickers"].append(tk)
                            break
                    continue
                seen.add(key)
                it = dict(it)
                it["categories"] = cats
                it["tickers"] = [tk]
                out.append(it)
    return out


def fetch_global_healthcare_news(days: int = 1, max_items: int = 300,
                                 min_mcap: float = 500.0,
                                 max_workers: int = 15) -> list[dict]:
    """3-tier 통합 — Finviz per-ticker + 바이오 전문 매체 RSS + Google News.
    카테고리 매칭된 것만 + 제목 dedupe.
    Returns [{title, link, source, published, _published_dt, tickers, categories}]."""
    finviz_items = _fetch_finviz_universe_news(days, min_mcap, max_workers)
    rss_items = fetch_biotech_rss(days=days)
    gnews_items = fetch_gnews_biotech_categorized(days=days)

    # 통합 dedupe + 비-바이오 잡음 필터
    seen: set[str] = set()
    combined: list[dict] = []
    for src_list in (finviz_items, rss_items, gnews_items):
        for it in src_list:
            title = (it.get("title") or "").strip()
            if not title:
                continue
            if not _is_biotech_relevant(it):
                continue
            key = re.sub(r"[^a-z0-9]", "", title.lower())[:60]
            if not key or key in seen:
                continue
            seen.add(key)
            combined.append(it)

    combined.sort(key=lambda x: x.get("_published_dt")
                  or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return combined[:max_items]


def fetch_recent_titles(ticker: str, n: int = 3, days: int = 7) -> list[dict]:
    """텔레그램용 최근 뉴스 — 헤드라인 + 출처 + 링크. 날짜 내림차순.
    Returns list of {title, source, link, published}."""
    items = fetch_finviz_news(ticker, days=days)
    if len(items) < n:
        items += fetch_yahoo_news(ticker)
    items.sort(key=lambda x: x.get("_published_dt") or datetime.min.replace(tzinfo=timezone.utc),
               reverse=True)
    seen_titles: set[str] = set()
    out: list[dict] = []
    for it in items:
        title = (it.get("title") or "").strip()
        if not title:
            continue
        key = re.sub(r"[^a-z0-9]", "", title.lower())[:60]
        if not key or key in seen_titles:
            continue
        seen_titles.add(key)
        out.append({
            "title": title,
            "source": it.get("source", ""),
            "link": it.get("link", ""),
            "published": it.get("published", ""),
        })
        if len(out) >= n:
            break
    return out
