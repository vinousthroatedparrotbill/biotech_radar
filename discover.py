"""IR / Pipeline URL 자동 탐색.
1) yfinance에서 회사 홈페이지 URL 조회
2) 홈페이지 HTML 가져와 anchor 텍스트가 IR/Pipeline 패턴에 맞는 링크 추출
3) 후보가 여러 개면 텍스트 적합도로 score 매겨 best 선택
"""
from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

import yfinance as yf
from bs4 import BeautifulSoup
from curl_cffi import requests as crequests

# IR ROOT 패턴 — 회사 홈페이지에서 IR 섹션 찾을 때 (1단계)
IR_ROOT_PATTERNS: list[tuple[re.Pattern, int]] = [
    (re.compile(r"\binvestor\s*relations?\b", re.I), 12),
    (re.compile(r"\binvestor\s*(?:information|center|hub|portal|overview)\b", re.I), 11),
    (re.compile(r"\bfor\s*investors?\b", re.I), 10),
    (re.compile(r"\binvestors?\s*(?:&|and)\s*media", re.I), 10),
    (re.compile(r"\binvestors?\b", re.I), 8),
    (re.compile(r"\bshareholders?\b", re.I), 7),
    (re.compile(r"\bfinancial\s*(?:results?|information|reports?|filings?)\b", re.I), 7),
    (re.compile(r"\bIR\b", re.I), 5),
    (re.compile(r"\bcorporate\b", re.I), 3),   # 약한 시그널
]

# IR PRESENTATIONS 패턴 — IR 섹션 안에서 발표자료 페이지 찾을 때 (2단계, 더 구체적)
IR_PRESENTATIONS_PATTERNS: list[tuple[re.Pattern, int]] = [
    (re.compile(r"events?\s*(?:&amp;|&|and)\s*presentations?", re.I), 25),
    (re.compile(r"investor\s*(?:presentations?|day|update)", re.I), 22),
    (re.compile(r"\bIR\s*presentations?\b", re.I), 22),
    (re.compile(r"\bcorporate\s*presentations?\b", re.I), 20),
    (re.compile(r"\bpresentations?\b", re.I), 16),
    (re.compile(r"\bquarterly\s*results?\b", re.I), 14),
    (re.compile(r"\bquarterly\s*(?:reports?|earnings?|financials?)\b", re.I), 13),
    (re.compile(r"\bearnings?\s*(?:call|webcast|releases?|reports?)\b", re.I), 12),
    (re.compile(r"\bfinancial\s*(?:results?|reports?)\b", re.I), 11),
    (re.compile(r"\bnews\s*(?:&amp;|&|and)\s*events?", re.I), 10),
    (re.compile(r"\bevents?\b", re.I), 9),
    (re.compile(r"\bwebcasts?\b", re.I), 9),
    (re.compile(r"\bconferences?\b", re.I), 8),
    (re.compile(r"\bnews\s*releases?\b", re.I), 7),
]

# 도메인이 비-IR이면 직접 시도할 IR 서브도메인
IR_SUBDOMAIN_PREFIXES = ["ir", "investors", "investor"]

# 메인 도메인 위에 직접 시도할 IR 경로
IR_PATH_CANDIDATES = [
    "/investor-relations", "/investor-relations/",
    "/investors", "/investors/",
    "/investor", "/investor/",
    "/our-investors",
    "/ir", "/ir/",
    "/about/investors", "/about-us/investors",
    "/company/investors",
]

# IR root 위에 직접 시도할 발표자료 경로
PRESENTATIONS_PATH_CANDIDATES = [
    "/events-and-presentations", "/events-and-presentations/",
    "/events-presentations",
    "/investor-presentations",
    "/news-events/events-presentations",
    "/news-and-events/events-and-presentations",
    "/financial-information/quarterly-results",
    "/quarterly-results",
    "/presentations", "/presentations/",
    "/events", "/events/",
    "/news-events", "/news-and-events",
]

# 호환성 유지 — 기존 호출 코드 깨지지 않게
IR_PATTERNS = IR_ROOT_PATTERNS
PIPELINE_PATTERNS: list[tuple[re.Pattern, int]] = [
    (re.compile(r"\bour\s*pipeline\b", re.I), 10),
    (re.compile(r"\bpipeline\b", re.I), 8),
    (re.compile(r"\bour\s*medicines?\b", re.I), 9),
    (re.compile(r"\bour\s*research\b", re.I), 7),
    (re.compile(r"\bour\s*science\b", re.I), 6),
    (re.compile(r"\bproduct\s*portfolio\b", re.I), 7),
    (re.compile(r"\bportfolio\b", re.I), 4),
    (re.compile(r"\btherapeutic\s*areas?\b", re.I), 5),
    (re.compile(r"\bclinical\s*(?:program|pipeline|trials?)\b", re.I), 6),
]


def _company_website(ticker: str) -> str | None:
    try:
        info = yf.Ticker(ticker).info
        web = info.get("website") or info.get("irWebsite")
        if not web:
            return None
        if not web.startswith("http"):
            web = "https://" + web
        return web
    except Exception:
        return None


def _fetch_html(url: str, timeout: int = 12) -> str | None:
    try:
        r = crequests.get(url, impersonate="chrome", timeout=timeout)
        r.raise_for_status()
        return r.text
    except Exception:
        return None


def _url_alive(url: str, timeout: int = 8) -> bool:
    """URL이 200 OK 응답하는지 빠르게 확인."""
    try:
        r = crequests.get(url, impersonate="chrome", timeout=timeout, allow_redirects=True)
        return r.status_code == 200 and len(r.text) > 1000   # 짧으면 에러 페이지일 가능성
    except Exception:
        return False


def _try_subdomains(website: str) -> str | None:
    """ir.X, investors.X, investor.X 직접 시도. 첫 번째 살아있는 URL 반환."""
    p = urlparse(website)
    if not p.hostname:
        return None
    # 도메인 추출 — 'www.example.com' → 'example.com'
    host = p.hostname.lstrip("www.") if p.hostname.startswith("www.") else p.hostname
    parts = host.split(".")
    if len(parts) < 2:
        return None
    # apex 도메인 (마지막 두 부분)
    apex = ".".join(parts[-2:])
    for prefix in IR_SUBDOMAIN_PREFIXES:
        candidate = f"{p.scheme}://{prefix}.{apex}/"
        if _url_alive(candidate):
            return candidate
    return None


def _try_paths(base_url: str, paths: list[str]) -> str | None:
    """base_url 위에 paths 후보들 직접 시도. 첫 번째 살아있는 URL 반환."""
    p = urlparse(base_url)
    origin = f"{p.scheme}://{p.hostname}"
    for path in paths:
        candidate = f"{origin}{path}"
        if _url_alive(candidate):
            return candidate
    return None


def _score_anchor(text: str, href: str, patterns: list[tuple[re.Pattern, int]]) -> int:
    """anchor 텍스트와 href 둘 다 검사 (href에도 키워드 들어가는 경우 흔함)."""
    combo = f"{text or ''} {href or ''}"
    score = 0
    for pat, w in patterns:
        if pat.search(combo):
            score = max(score, w)
    return score


def _extract_best_link(html: str, base_url: str, patterns: list[tuple[re.Pattern, int]]) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    base_host = urlparse(base_url).netloc
    candidates: list[tuple[int, str, str]] = []   # (score, abs_url, text)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(" ", strip=True)
        score = _score_anchor(text, href, patterns)
        if score == 0:
            continue
        abs_url = urljoin(base_url, href)
        # 외부 도메인은 제외 (단, 같은 회사의 *.investors.foo.com 같은 서브도메인은 OK)
        href_host = urlparse(abs_url).netloc
        if href_host and base_host:
            base_root = ".".join(base_host.split(".")[-2:])
            href_root = ".".join(href_host.split(".")[-2:])
            if base_root != href_root:
                # 단, q4inc.com / s2.q4cdn.com 같은 IR 호스팅은 허용
                if not any(s in href_host for s in ("q4inc", "q4cdn", "investis", "edgar")):
                    continue
        candidates.append((score, abs_url, text[:80]))

    if not candidates:
        return None
    candidates.sort(key=lambda x: -x[0])
    return candidates[0][1]


def discover_batch(tickers: list[str], max_workers: int = 10) -> dict[str, dict]:
    """병렬 탐색 — 100~300종목 일괄 처리."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(discover, t): t for t in tickers}
        for fut in as_completed(futures):
            t = futures[fut]
            try:
                results[t] = fut.result()
            except Exception as e:
                results[t] = {"_error": str(e)}
    return results


def discover(ticker: str) -> dict[str, str]:
    """Returns {'ir_url'?, 'pipeline_url'?, 'website'?, '_error'?}.

    탐색 순서 (cascade):
      Step 1: 회사 홈페이지 anchor → IR root
      Step 2: 못 찾으면 → ir.X / investors.X / investor.X 서브도메인 직접 프로빙
      Step 3: 못 찾으면 → 메인 도메인의 /investors /ir /investor-relations 등 경로 프로빙
      Step 4: IR root에서 anchor → Events & Presentations (etc.) 발표자료 페이지
      Step 5: 못 찾으면 → IR root 위에 /events-and-presentations 같은 경로 프로빙
      Step 6: 그래도 못 찾으면 IR root 자체를 ir_url로 반환
    """
    web = _company_website(ticker)
    if not web:
        return {"_error": f"yfinance에 {ticker} 홈페이지 없음"}
    html = _fetch_html(web)
    if not html:
        return {"website": web, "_error": "홈페이지 HTML 가져오기 실패"}

    out: dict[str, str] = {"website": web}

    # Pipeline은 회사 홈페이지에서 직접
    pl = _extract_best_link(html, web, PIPELINE_PATTERNS)
    if pl:
        out["pipeline_url"] = pl

    # === IR root 탐색 (3-tier cascade) ===
    # tier 1: anchor 텍스트 매칭
    ir_root = _extract_best_link(html, web, IR_ROOT_PATTERNS)
    # tier 2: 서브도메인 프로빙
    if not ir_root:
        ir_root = _try_subdomains(web)
    # tier 3: 경로 프로빙
    if not ir_root:
        ir_root = _try_paths(web, IR_PATH_CANDIDATES)

    # === IR presentations 탐색 (2-tier cascade) ===
    events_url = None
    if ir_root:
        ir_html = _fetch_html(ir_root)
        if ir_html:
            # tier 1: IR root 페이지 안의 anchor
            events_url = _extract_best_link(ir_html, ir_root, IR_PRESENTATIONS_PATTERNS)
        # tier 2: IR root 위에 /events-and-presentations 같은 경로 직접 시도
        if not events_url:
            events_url = _try_paths(ir_root, PRESENTATIONS_PATH_CANDIDATES)

    out["ir_url"] = events_url or ir_root or ""
    if not out["ir_url"]:
        out.pop("ir_url")

    if "ir_url" not in out and "pipeline_url" not in out:
        out["_error"] = "IR/Pipeline 링크 못 찾음"
    return out
