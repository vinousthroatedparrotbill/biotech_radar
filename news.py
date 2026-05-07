"""뉴스 멘션 + 약물명 추출 + 기전 분류.
출처: Yahoo Finance(yfinance Ticker.news, ~10건/티커) + Google News RSS(보조 볼륨)."""
from __future__ import annotations

import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus

import feedparser
import yfinance as yf
from curl_cffi import requests as crequests

from drugs_db import DRUG_MOA, SUFFIX_RULES, CONTEXT_KEYWORDS, classify

GNEWS_URL = "https://news.google.com/rss/search"

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


def fetch_combined(ticker: str, name: str, days: int) -> list[dict]:
    """Yahoo + Google News 합치고 제목 유사 중복 제거."""
    yahoo = fetch_yahoo_news(ticker)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    yahoo = [n for n in yahoo if n.get("_published_dt") is None or n["_published_dt"] >= cutoff]
    google = fetch_google_news(name, days=days, limit=200)
    seen_titles: set[str] = set()
    out = []
    for n in yahoo + google:
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
    """Returns [{drug, mentions, moa, sample_link}] (top 3, by mention count)."""
    items = fetch_combined(ticker, name, days)
    drug_counter: Counter[str] = Counter()
    drug_context: dict[str, str] = {}
    drug_links: dict[str, str] = {}
    for it in items:
        text = f"{it['title']}. {it['summary']}"
        drugs = _extract_drugs(text)
        for d in drugs:
            drug_counter[d] += 1
            # 가장 길고 정보 많은 컨텍스트 보관
            if len(text) > len(drug_context.get(d, "")):
                drug_context[d] = text
                drug_links[d] = it.get("link", "")

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
