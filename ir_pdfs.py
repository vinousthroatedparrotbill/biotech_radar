"""IR 페이지에서 PDF 링크 수집 — curl_cffi (브라우저 TLS 지문) + SEC EDGAR fallback."""
from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from curl_cffi import requests as crequests

EDGAR_UA = {"User-Agent": "Biotech Radar contact@example.com"}   # SEC requires identifying UA


_ASSET_EXTS = (".pdf", ".pptx", ".ppt", ".key")


def _looks_like_asset(url: str) -> tuple[bool, str]:
    u = url.lower()
    for ext in _ASSET_EXTS:
        if u.endswith(ext) or f"{ext}?" in u:
            return True, ext.lstrip(".")
    return False, ""


def _looks_like_pdf(url: str) -> bool:
    return url.lower().endswith(".pdf") or ".pdf?" in url.lower()


def _date_hint(text: str) -> str | None:
    """텍스트에서 'Q4 2024', '2025-03-15' 같은 패턴 추출."""
    if not text:
        return None
    patterns = [
        r"\b(Q[1-4]\s*20\d{2})\b",
        r"\b(20\d{2}[-/.]\d{1,2}[-/.]\d{1,2})\b",
        r"\b((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s*\d{1,2}?,?\s*20\d{2})\b",
        r"\b(20\d{2})\b",
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return m.group(1)
    return None


# ───────────── Q4 IR 호스팅 사이트 API (Q4Inc.com) ─────────────
Q4_ENDPOINTS = [
    ("Event",          "/feed/Event.svc/GetEventList",          "GetEventListResult"),
    ("PressRelease",   "/feed/PressRelease.svc/GetPressReleaseList", "GetPressReleaseListResult"),
    ("FinancialReport","/feed/FinancialReport.svc/GetFinancialReportList", "GetFinancialReportListResult"),
    ("Presentation",   "/feed/Presentation.svc/GetPresentationList", "GetPresentationListResult"),
]
Q4_DTO = ('{"StartIndex":0,"ItemCount":50,"IncludePastEvents":true,'
          '"IncludeFutureEvents":true,"IncludeTags":true,"LanguageId":1,'
          '"TagList":[],"Year":-1}')


def _q4_origin(ir_url: str) -> str | None:
    """IR URL이 Q4 hosted인지 시도 — origin (scheme://host) 반환.
    실제 Q4 여부는 endpoint 호출 결과로 판단."""
    p = urlparse(ir_url)
    if not p.scheme or not p.netloc:
        return None
    return f"{p.scheme}://{p.netloc}"


def fetch_q4_attachments(ir_url: str, limit: int = 60) -> list[dict]:
    """Q4 사이트의 SOAP-style API 호출하여 IR 자료 attachments 수집.
    Q4가 아니면 빈 리스트 반환. Returns list of {url, title, date_hint, asset_type, kind}."""
    origin = _q4_origin(ir_url)
    if not origin:
        return []
    out: list[dict] = []
    for kind_name, path, result_key in Q4_ENDPOINTS:
        try:
            r = crequests.get(
                f"{origin}{path}",
                params={"serviceDto": Q4_DTO},
                impersonate="chrome", timeout=12,
            )
            if r.status_code != 200:
                continue
            data = r.json()
        except Exception:
            continue
        items = data.get(result_key) or []
        if not isinstance(items, list):
            continue
        for it in items:
            event_date = (it.get("EventDate") or it.get("PressReleaseDate")
                          or it.get("ReportDate") or it.get("PresentationDate") or "")
            event_title = (it.get("EventName") or it.get("Headline")
                           or it.get("ReportTitle") or it.get("Title") or "")
            attachments = it.get("Attachments") or []
            if not attachments and it.get("Url"):
                # PressRelease 본문 URL — PDF가 아닐 가능성 (HTML article)
                continue
            for att in attachments:
                url = att.get("Url") or ""
                if not url:
                    continue
                ext_raw = (att.get("Extension") or "").lower().strip(".")
                if ext_raw not in ("pdf", "pptx", "ppt"):
                    continue
                title = att.get("Title") or event_title or url.split("/")[-1]
                full_title = f"{event_title} — {title}" if event_title and title != event_title else title
                date_hint = event_date.split("T")[0] if "T" in (event_date or "") else event_date
                out.append({
                    "url": url,
                    "title": full_title[:200],
                    "date_hint": date_hint or _date_hint(full_title),
                    "asset_type": ext_raw,
                    "kind": kind_name,
                })
                if len(out) >= limit:
                    return out
    return out


def fetch_pdf_links(ir_url: str, limit: int = 50, use_browser: bool = True) -> list[dict]:
    """IR 페이지에서 PDF/PPT/PPTX 추출 (cascade).
    1) Q4 hosted API → 2) 정적 HTML 스크랩 → 3) Playwright 헤드리스 렌더
    Returns list of {url, title, date_hint, asset_type, kind?}."""
    # 1) Q4 API 시도 (Q4 hosted IR 사이트 — ACLX, BMRN 등)
    q4_results = fetch_q4_attachments(ir_url, limit=limit)
    if q4_results:
        return q4_results
    try:
        r = crequests.get(ir_url, impersonate="chrome", timeout=20)
        r.raise_for_status()
    except Exception as e:
        return [{"_error": f"{type(e).__name__}: {e}"}]

    soup = BeautifulSoup(r.text, "html.parser")
    base = ir_url
    seen: set[str] = set()
    out: list[dict] = []

    # 1) <a href> 태그
    for a in soup.find_all("a", href=True):
        href = a["href"]
        absolute = urljoin(base, href)
        is_asset, ext = _looks_like_asset(absolute)
        if not is_asset:
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        title = (a.get_text(strip=True)
                 or a.get("title")
                 or urlparse(absolute).path.split("/")[-1])
        parent_text = a.parent.get_text(" ", strip=True) if a.parent else ""
        date_hint = _date_hint(title) or _date_hint(parent_text)
        out.append({
            "url": absolute, "title": title[:200],
            "date_hint": date_hint, "asset_type": ext,
        })
        if len(out) >= limit:
            break

    # 2) 페이지 raw HTML에 직접 박힌 PDF/PPT URL (JS data·JSON·script 안)
    if len(out) < limit:
        for m in re.finditer(
            r'(https?://[^\s"\'<>]+?\.(?:pdf|pptx|ppt)(?:\?[^\s"\'<>]*)?)',
            r.text, re.IGNORECASE,
        ):
            url = m.group(1)
            if url in seen:
                continue
            seen.add(url)
            ext = url.lower().rsplit(".", 1)[-1].split("?")[0]
            title = urlparse(url).path.split("/")[-1]
            out.append({
                "url": url, "title": title[:200],
                "date_hint": _date_hint(title), "asset_type": ext,
            })
            if len(out) >= limit:
                break

    if out:
        return out

    # 3) Playwright 헤드리스 렌더 fallback (JS 사이트)
    if use_browser:
        try:
            from ir_browser import fetch_via_browser
        except ImportError:
            return [{"_error": "PDF/PPT 링크 없음 — Playwright 미설치"}]
        browser_results = fetch_via_browser(ir_url)
        err = next((l.get("_error") for l in browser_results if l.get("_error")), None)
        if err:
            return [{"_error": f"PDF/PPT 링크 없음 (Playwright도 빈 결과): {err}"}]
        return browser_results
    return [{"_error": "PDF/PPT 링크 없음"}]


# ───────────── Google Docs Viewer 임베드 ─────────────
def gdocs_viewer(asset_url: str) -> str:
    """PDF/PPT URL → Google Docs Viewer 임베드용 URL.
    Google이 대신 렌더 + iframe 가능. 거의 모든 공개 PDF 통함."""
    from urllib.parse import quote
    return f"https://docs.google.com/gview?url={quote(asset_url, safe='')}&embedded=true"


# ───────────── SEC EDGAR fallback ─────────────
def _edgar_cik(ticker: str) -> str | None:
    """yfinance로 CIK 조회 (SEC EDGAR ticker→CIK 매핑)."""
    try:
        r = crequests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers=EDGAR_UA, timeout=15, impersonate="chrome",
        )
        r.raise_for_status()
        data = r.json()
        for _, v in data.items():
            if v.get("ticker", "").upper() == ticker.upper():
                return str(v["cik_str"]).zfill(10)
    except Exception:
        return None
    return None


def fetch_edgar_8k_pdfs(ticker: str, limit: int = 15) -> list[dict]:
    """SEC EDGAR에서 최근 8-K 발표 자료(EX-99) PDF 수집.
    실적 발표·학회 발표는 보통 8-K Item 7.01/2.02 + EX-99로 제출됨."""
    cik = _edgar_cik(ticker)
    if not cik:
        return [{"_error": f"CIK lookup 실패: {ticker}"}]

    # 최근 8-K 목록
    try:
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        r = crequests.get(url, headers=EDGAR_UA, timeout=15, impersonate="chrome")
        r.raise_for_status()
        sub = r.json()
    except Exception as e:
        return [{"_error": f"EDGAR submissions fetch 실패: {e}"}]

    recent = sub.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    accessions = recent.get("accessionNumber", [])
    dates = recent.get("filingDate", [])
    primary_docs = recent.get("primaryDocument", [])
    primary_descs = recent.get("primaryDocDescription", [])

    out: list[dict] = []
    for form, accno, date, pdoc, pdesc in zip(forms, accessions, dates, primary_docs, primary_descs):
        if form != "8-K":
            continue
        acc_clean = accno.replace("-", "")
        # 8-K filing index
        idx_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_clean}/"
        try:
            ridx = crequests.get(idx_url, headers=EDGAR_UA, timeout=15, impersonate="chrome")
            ridx.raise_for_status()
        except Exception:
            continue
        soup = BeautifulSoup(ridx.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = urljoin(idx_url, href)
            if not _looks_like_pdf(full):
                continue
            title = a.get_text(strip=True) or full.split("/")[-1]
            out.append({
                "url": full,
                "title": title[:200],
                "date_hint": date,
                "filing": f"8-K · {pdesc or 'Current Report'}",
            })
            if len(out) >= limit:
                return out
    if not out:
        return [{"_error": "최근 8-K에 PDF EX-99 없음."}]
    return out
