"""IR 자료(투자자 프레젠테이션 PDF)에서 회사 자체 공개 카탈리스트 추출.

Pipeline
1) ticker_urls.json에서 IR URL 가져옴
2) ir_pdfs.fetch_pdf_links로 IR 페이지 PDF 목록 → 최신 'investor presentation' 류 PDF 선택
3) PDF 텍스트 추출 (pypdf)
4) "Anticipated Catalysts" / "Upcoming Milestones" / "Key Events" 섹션 찾기
5) 불릿 + 일자 힌트(Q1 2026, 2H 2026, mid-2026, ASCO 2026 등) 파싱
6) catalysts 테이블에 event_type='company_event'로 저장
"""
from __future__ import annotations

import datetime as dt
import io
import logging
import re
from typing import Iterable

import pandas as pd

import db
import ticker_urls
from ir_pdfs import fetch_pdf_links, fetch_edgar_8k_pdfs

log = logging.getLogger(__name__)


# 최신 PDF 후보 키워드 (가중치 — 큰 숫자 우선)
DECK_KEYWORDS = [
    ("investor presentation", 100),
    ("corporate presentation", 95),
    ("company overview", 90),
    ("investor deck", 90),
    ("investor day", 85),
    ("ir presentation", 85),
    ("ir-vf", 80),
    ("ir final", 80),
    ("jpmorgan", 75),
    ("jpm", 70),
    ("for-web", 65),
    ("for web", 65),
    ("non-confidential", 60),
    ("conference", 55),
    ("ash", 50),    # American Society of Hematology presentation
    ("eha", 50),
    ("asco", 50),
    ("esmo", 50),
    ("aacr", 50),
    ("easl", 50),
    ("ada", 50),
    ("aan", 45),
    ("acr", 45),
    ("aha", 45),
    ("esc", 45),
    ("presentation", 40),
    ("fact sheet", 35),
    ("earnings", 30),
    ("8-k", 20),
]

# 카탈리스트 섹션 헤더 패턴
SECTION_HEADERS = [
    r"anticipated\s+catalysts?",
    r"anticipated\s+milestones?",
    r"upcoming\s+(?:catalysts?|milestones?|events?)",
    r"near[\s\-]?term\s+catalysts?",
    r"key\s+upcoming\s+(?:events?|catalysts?|milestones?)",
    r"expected\s+milestones?",
    r"pipeline\s+milestones?",
    r"key\s+\d{4}\s+milestones?",
    r"\d{4}\s+(?:anticipated|expected|key)?\s*(?:catalysts?|milestones?|events?)",
    r"path\s+forward",
    r"key\s+\d{4}[\s/]\d{2,4}\s+milestones?",
]

# 일자 힌트 패턴 (quarter, half, mid, year-end, conference, month)
DATE_PATTERNS = [
    r"(?:Q[1-4]|1H|2H|early|mid|late|year[\s\-]?end)\s*(?:of\s+)?\s*\d{4}",
    r"(?:January|February|March|April|May|June|July|August|September|October|"
    r"November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sept?|Oct|Nov|Dec)\s*\d{4}",
    r"\d{4}\s*[ \-]?\s*(?:Q[1-4]|1H|2H|YE)",
    r"(?:by|in|expected)\s+\d{4}",
    r"(?:ASCO|ASH|EASL|ADA|AACR|ESMO|SITC|AAN|SABCS|ACR|AHA|ESC|JPM)\s*\d{2,4}",
]


def _select_latest_deck(pdfs: list[dict]) -> dict | None:
    """최신 투자자 프레젠테이션 PDF 선택. 키워드 가중치 + date hint."""
    if not pdfs:
        return None
    candidates = []
    for p in pdfs:
        if p.get("_error"):
            continue
        url = (p.get("url") or "").lower()
        title = (p.get("title") or "").lower()
        combined = f"{title} {url}"
        score = 0
        for kw, w in DECK_KEYWORDS:
            if kw in combined:
                score = max(score, w)
        if score == 0:
            continue
        # date hint 점수 (최신일수록 높게)
        date_score = 0
        dh = p.get("date_hint", "") or ""
        m = re.search(r"(20\d{2})[-/]?(\d{1,2})?", dh)
        if m:
            yr = int(m.group(1))
            mo = int(m.group(2)) if m.group(2) else 6
            date_score = (yr - 2020) * 12 + mo
        candidates.append((score, date_score, p))
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return candidates[0][2]


def extract_pdf_text(url: str, max_pages: int = 60) -> str:
    """PDF URL → 텍스트. pypdf 사용."""
    from curl_cffi import requests as crq
    from pypdf import PdfReader
    try:
        r = crq.get(url, impersonate="chrome", timeout=30)
        r.raise_for_status()
    except Exception as e:
        log.warning("PDF fetch 실패 %s: %s", url, e)
        return ""
    try:
        reader = PdfReader(io.BytesIO(r.content))
        pages = []
        for i, page in enumerate(reader.pages):
            if i >= max_pages:
                break
            try:
                pages.append(page.extract_text() or "")
            except Exception:
                continue
        return "\n\n".join(pages)
    except Exception as e:
        log.warning("PDF parse 실패 %s: %s", url, e)
        return ""


def find_milestone_sections(text: str) -> list[str]:
    """카탈리스트 섹션 텍스트들 추출. 헤더 매칭 후 다음 헤더 직전까지."""
    if not text:
        return []
    pattern = "(" + "|".join(SECTION_HEADERS) + ")"
    sections = []
    # 라인별로 헤더 위치 찾기
    matches = list(re.finditer(pattern, text, re.IGNORECASE))
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else min(start + 4000, len(text))
        section = text[start:end].strip()
        if len(section) > 80:   # 의미 있는 길이
            sections.append(section[:4000])
    return sections


def parse_milestones(section_text: str) -> list[dict]:
    """카탈리스트 섹션에서 개별 항목 파싱.
    각 항목 = {date_hint, text}. 일자 패턴 있는 줄만 추출."""
    out: list[dict] = []
    if not section_text:
        return out
    date_re = re.compile("(" + "|".join(DATE_PATTERNS) + ")", re.IGNORECASE)
    # 줄 단위로 분리 (PDF 추출은 줄 깨질 수 있어 sentence 단위도 시도)
    lines = re.split(r"[\n•·●▪♦◆◇○•]+", section_text)
    seen_text: set[str] = set()
    for line in lines:
        line = line.strip(" -–—\t")
        if len(line) < 12 or len(line) > 400:
            continue
        m = date_re.search(line)
        if not m:
            continue
        # 한 줄에 하나의 milestone — 중복 제거
        key = re.sub(r"\s+", " ", line.lower())[:120]
        if key in seen_text:
            continue
        seen_text.add(key)
        out.append({
            "date_hint": m.group(1),
            "text": line[:300],
        })
    return out


def _date_hint_to_iso(hint: str) -> str | None:
    """'1H 2026' → '2026-06-30', 'Q3 2026' → '2026-09-30', 'mid-2026' → '2026-06-30'.
    학회명 → 학회 시작일 (CONFERENCES_2026 참조). 정확도 < but 정렬 가능."""
    if not hint:
        return None
    h = hint.lower().strip()
    # 학회명
    from catalysts import CONFERENCES_2026
    for c in CONFERENCES_2026:
        for token in c["name"].lower().split():
            if len(token) >= 3 and token in h:
                return c["start"]
    # Q1-Q4
    m = re.search(r"q([1-4])\s*(20\d{2})", h)
    if m:
        q, y = int(m.group(1)), int(m.group(2))
        end_month = q * 3
        end_day = 31 if end_month in (3, 12) else 30
        return f"{y}-{end_month:02d}-{end_day:02d}"
    # 1H/2H
    m = re.search(r"(1h|2h)\s*(20\d{2})", h)
    if m:
        half, y = m.group(1), int(m.group(2))
        return f"{y}-06-30" if half == "1h" else f"{y}-12-31"
    # early/mid/late/year-end YYYY
    m = re.search(r"(early|mid|late|year[\s\-]?end)\s*(?:of\s+)?\s*(20\d{2})", h)
    if m:
        kw, y = m.group(1), int(m.group(2))
        if "early" in kw:
            return f"{y}-03-31"
        if "mid" in kw:
            return f"{y}-06-30"
        if "late" in kw or "year" in kw:
            return f"{y}-12-31"
    # Month YYYY
    months = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
              "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12}
    m = re.search(r"(jan|feb|mar|apr|may|jun|jul|aug|sept?|oct|nov|dec)\w*\s*(20\d{2})", h)
    if m:
        mo = months[m.group(1)[:4] if m.group(1).startswith("sept") else m.group(1)[:3]]
        y = int(m.group(2))
        last_day = 31 if mo in (1, 3, 5, 7, 8, 10, 12) else (30 if mo != 2 else 28)
        return f"{y}-{mo:02d}-{last_day:02d}"
    # YYYY only
    m = re.search(r"(20\d{2})", h)
    if m:
        return f"{m.group(1)}-12-31"
    return None


def extract_for_ticker(ticker: str, save: bool = True) -> dict:
    """전체 파이프라인. 결과 dict 반환 + (옵션) catalysts 테이블 저장."""
    ticker = ticker.upper()
    urls = ticker_urls.get(ticker) or {}
    ir_url = urls.get("ir_url")
    pdfs: list[dict] = []
    # 1a) IR 페이지에서 PDF 시도
    if ir_url:
        pdfs = fetch_pdf_links(ir_url, limit=40, use_browser=True)
        if pdfs and pdfs[0].get("_error"):
            log.info("IR 페이지 fetch 실패 (%s) — EDGAR fallback", pdfs[0].get("_error", "")[:80])
            pdfs = []
    # 1b) EDGAR 8-K fallback (또는 IR URL 미등록)
    if not pdfs or all(p.get("_error") for p in pdfs):
        edgar_pdfs = fetch_edgar_8k_pdfs(ticker, limit=20)
        if edgar_pdfs and not edgar_pdfs[0].get("_error"):
            pdfs = edgar_pdfs
        elif not ir_url:
            return {"ticker": ticker, "error": "IR URL 미등록 + EDGAR도 실패"}
    if not pdfs or all(p.get("_error") for p in pdfs):
        err = pdfs[0].get("_error") if pdfs else "no PDFs"
        return {"ticker": ticker, "ir_url": ir_url, "error": err}
    # 2) 최신 deck 선택
    deck = _select_latest_deck(pdfs)
    if not deck:
        return {"ticker": ticker, "ir_url": ir_url,
                "error": "investor presentation 류 PDF 못 찾음",
                "available_pdfs": [p.get("title") for p in pdfs[:5]]}
    # 3) PDF 텍스트
    text = extract_pdf_text(deck["url"])
    if not text:
        return {"ticker": ticker, "deck_url": deck["url"],
                "error": "PDF 텍스트 추출 실패"}
    # 4) 섹션 + 항목 파싱
    sections = find_milestone_sections(text)
    items: list[dict] = []
    for sec in sections:
        items.extend(parse_milestones(sec))
    # 중복 제거
    seen: set[str] = set()
    uniq: list[dict] = []
    for it in items:
        k = re.sub(r"\s+", " ", it["text"].lower())[:100]
        if k in seen:
            continue
        seen.add(k)
        uniq.append(it)
    items = uniq

    result = {
        "ticker": ticker,
        "deck_url": deck["url"],
        "deck_title": deck.get("title", ""),
        "deck_date_hint": deck.get("date_hint", ""),
        "sections_found": len(sections),
        "milestones": items,
    }

    # 5) 저장
    if save and items:
        now = dt.datetime.now().isoformat(timespec="seconds")
        with db.connect() as conn:
            for it in items:
                ev_date = _date_hint_to_iso(it["date_hint"]) or "2099-12-31"
                try:
                    conn.execute(
                        "INSERT INTO catalysts (ticker, event_date, event_type, "
                        "title, description, source, fetched_at) "
                        "VALUES (?,?,?,?,?,?,?) "
                        "ON CONFLICT (ticker, event_date, event_type, title) DO NOTHING",
                        (ticker, ev_date, "company_event",
                         it["text"][:300], f"date_hint: {it['date_hint']}",
                         "ir_deck", now),
                    )
                except Exception as e:
                    log.debug("upsert: %s", e)
    return result


def refresh_for_tickers(tickers: Iterable[str]) -> dict:
    out = {}
    for t in tickers:
        try:
            r = extract_for_ticker(t, save=True)
            out[t] = {
                "milestones": len(r.get("milestones", [])),
                "deck": r.get("deck_title", ""),
                "error": r.get("error"),
            }
            log.info("%s: %s milestones", t, len(r.get("milestones", [])))
        except Exception as e:
            log.exception("%s 실패", t)
            out[t] = {"error": str(e)}
    return out


def get_company_events(ticker: str) -> pd.DataFrame:
    """저장된 회사 카탈리스트 조회."""
    return db.pd_read_sql(
        "SELECT * FROM catalysts WHERE ticker = ? AND event_type = 'company_event' "
        "ORDER BY event_date ASC",
        params=(ticker.upper(),),
    )


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    tk = sys.argv[1] if len(sys.argv) > 1 else "RVMD"
    r = extract_for_ticker(tk, save=True)
    print(f"\n=== {tk} ===")
    print("deck:", r.get("deck_title"), "—", r.get("deck_url", "")[:80])
    print("sections:", r.get("sections_found"))
    print(f"milestones: {len(r.get('milestones', []))}")
    for it in r.get("milestones", [])[:15]:
        print(f"  [{it['date_hint']}] {it['text'][:200]}")
    if r.get("error"):
        print("ERR:", r["error"])
