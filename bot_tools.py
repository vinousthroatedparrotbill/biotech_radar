"""Telegram bot이 호출하는 외부/내부 도구 (Claude tool calling용)."""
from __future__ import annotations

from pathlib import Path

import requests
from dotenv import load_dotenv

# 모듈 import 시점에 .env 로드 (standalone 호출 / 테스트도 동작)
load_dotenv(Path(__file__).parent / ".env", override=True)


# ───────────────────────── ClinicalTrials.gov ─────────────────────────
def search_clinicaltrials(query: str, max_results: int = 5) -> list[dict]:
    """ClinicalTrials.gov v2 API search.
    query: 약물명·질환·NCT 번호 등.
    Returns 간단 요약 리스트."""
    try:
        r = requests.get(
            "https://clinicaltrials.gov/api/v2/studies",
            params={
                "query.term": query,
                "pageSize": max_results,
                "fields": ("NCTId,BriefTitle,Phase,OverallStatus,Condition,"
                           "InterventionName,LeadSponsorName,PrimaryCompletionDate,"
                           "StudyType,EnrollmentCount"),
            },
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return [{"_error": f"ClinicalTrials.gov error: {e}"}]

    out = []
    for study in (data.get("studies") or [])[:max_results]:
        proto = study.get("protocolSection", {})
        ident = proto.get("identificationModule", {})
        status = proto.get("statusModule", {})
        design = proto.get("designModule", {})
        spons = proto.get("sponsorCollaboratorsModule", {})
        cond = proto.get("conditionsModule", {})
        interv = proto.get("armsInterventionsModule", {})
        out.append({
            "nct_id": ident.get("nctId"),
            "title": ident.get("briefTitle"),
            "phase": ", ".join(design.get("phases") or []),
            "status": status.get("overallStatus"),
            "primary_completion": status.get("primaryCompletionDateStruct", {}).get("date"),
            "conditions": ", ".join((cond.get("conditions") or [])[:3]),
            "interventions": ", ".join(
                i.get("name", "") for i in (interv.get("interventions") or [])[:3]
            ),
            "sponsor": spons.get("leadSponsor", {}).get("name"),
            "enrollment": (design.get("enrollmentInfo") or {}).get("count"),
        })
    return out


# ───────────────────────── PubMed E-utilities ─────────────────────────
PUBMED_SEARCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
PUBMED_SUMMARY = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"


def search_pubmed(query: str, max_results: int = 5) -> list[dict]:
    """PubMed 논문 검색 → 메타데이터 요약."""
    try:
        rs = requests.get(PUBMED_SEARCH, params={
            "db": "pubmed", "term": query, "retmax": max_results,
            "retmode": "json", "sort": "date",
        }, timeout=15)
        rs.raise_for_status()
        ids = rs.json().get("esearchresult", {}).get("idlist", [])
        if not ids:
            return []
        ru = requests.get(PUBMED_SUMMARY, params={
            "db": "pubmed", "id": ",".join(ids), "retmode": "json",
        }, timeout=15)
        ru.raise_for_status()
        result = ru.json().get("result", {})
    except Exception as e:
        return [{"_error": f"PubMed error: {e}"}]

    out = []
    for pmid in ids:
        item = result.get(pmid, {})
        out.append({
            "pmid": pmid,
            "title": item.get("title"),
            "authors": ", ".join([a.get("name", "") for a in (item.get("authors") or [])[:3]]),
            "journal": item.get("source"),
            "pubdate": item.get("pubdate"),
            "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
        })
    return out


# ───────────────────────── Supabase 조회 ─────────────────────────
def get_ticker_info(ticker: str) -> dict:
    """ticker_master + 최신 high_low_cache 조회."""
    from db import connect
    with connect() as conn:
        t = conn.execute(
            "SELECT ticker, name, sector, industry, country, market_cap, price "
            "FROM ticker_master WHERE ticker = ?", (ticker.upper(),)
        ).fetchone()
        if not t:
            return {"_error": f"{ticker} 우리 universe에 없음"}
        h = conn.execute(
            "SELECT today_close, high_52w, low_52w, perf_1d, perf_7d, perf_1m, "
            "perf_3m, perf_6m, perf_1y FROM high_low_cache "
            "WHERE ticker = ? ORDER BY computed_date DESC LIMIT 1",
            (ticker.upper(),),
        ).fetchone()
        out = dict(t)
        if h:
            out.update(dict(h))
        return out


def get_memos_for(ticker: str) -> list[dict]:
    """특정 ticker 메모 히스토리."""
    from memo import list_for
    return list_for(ticker.upper())


def get_drug_moa(name: str) -> str | None:
    """drugs_db에서 약물의 MOA 매핑."""
    from drugs_db import classify
    return classify(name)


def fetch_recent_news_for(ticker: str, n: int = 5) -> list[dict]:
    """특정 ticker 최근 뉴스 헤드라인."""
    from news import fetch_recent_titles
    return fetch_recent_titles(ticker.upper(), n=n, days=14)


def get_pipeline_info(ticker: str) -> dict:
    """회사 파이프라인 페이지 본문 텍스트 추출 — 약물 코드(RM-055 등)가 보통 여기 등재됨.
    JS 렌더 페이지는 Playwright fallback. ticker_urls.json의 pipeline_url 사용."""
    import ticker_urls
    urls = ticker_urls.get(ticker.strip().upper())
    pl_url = urls.get("pipeline_url", "")
    if not pl_url:
        # 자동 탐색 시도
        try:
            from discover import discover
            r = discover(ticker.upper())
            if r.get("pipeline_url"):
                ticker_urls.set_urls(ticker.upper(), pipeline_url=r["pipeline_url"])
                pl_url = r["pipeline_url"]
        except Exception:
            pass
    if not pl_url:
        return {"error": f"{ticker}의 pipeline URL 없음"}
    from news import _fetch_pipeline_text
    text = _fetch_pipeline_text(pl_url)
    if not text or len(text) < 200:
        return {"pipeline_url": pl_url, "error": "페이지 파싱 실패 (JS-only or empty)"}
    return {
        "pipeline_url": pl_url,
        "text": text[:6000],   # 토큰 절약 — 처음 6000자
        "length": len(text),
    }


def get_realtime_quote(tickers: str) -> list[dict]:
    """실시간 주가 (Finviz Elite view=171 + yfinance 프리/애프터마켓 보강).
    tickers: 단일 또는 콤마 구분 (예: 'VRTX' 또는 'VRTX,TGTX,RVMD').
    Returns: price, change %, gap (premarket open vs prev close), volume, RSI, MA,
             52w high/low %, plus pre/post market price when active."""
    import os, io
    import requests
    import pandas as pd

    tk_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    if not tk_list:
        return [{"error": "no ticker"}]

    # tier 1: Finviz Elite view=171
    out_map: dict[str, dict] = {}
    token = (os.environ.get("FINVIZ_AUTH_TOKEN") or "").strip()
    if token:
        try:
            r = requests.get(
                "https://elite.finviz.com/export.ashx",
                params={"v": "171", "t": ",".join(tk_list), "auth": token},
                timeout=12,
            )
            r.raise_for_status()
            df = pd.read_csv(io.StringIO(r.text))
            for _, row in df.iterrows():
                tk = str(row.get("Ticker", "")).upper()
                if not tk:
                    continue
                out_map[tk] = {
                    "ticker": tk,
                    "price": row.get("Price"),
                    "change_pct": row.get("Change"),
                    "change_from_open": row.get("Change from Open"),
                    "gap_pct": row.get("Gap"),
                    "volume": row.get("Volume"),
                    "ma_20d": row.get("20-Day Simple Moving Average"),
                    "ma_50d": row.get("50-Day Simple Moving Average"),
                    "ma_200d": row.get("200-Day Simple Moving Average"),
                    "vs_52w_high": row.get("52-Week High"),
                    "vs_52w_low": row.get("52-Week Low"),
                    "rsi_14": row.get("Relative Strength Index (14)"),
                    "atr": row.get("Average True Range"),
                    "beta": row.get("Beta"),
                    "source": "Finviz Elite",
                }
        except Exception:
            pass

    # tier 2: yfinance — 프리/애프터마켓 가격 명시
    import yfinance as yf
    for tk in tk_list:
        cur = out_map.get(tk, {"ticker": tk})
        try:
            info = yf.Ticker(tk).info
            for k_in, k_out in [
                ("preMarketPrice", "premarket_price"),
                ("preMarketChangePercent", "premarket_change_pct"),
                ("postMarketPrice", "postmarket_price"),
                ("postMarketChangePercent", "postmarket_change_pct"),
                ("marketState", "market_state"),
                ("regularMarketPrice", "rt_price"),
                ("longName", "name"),
            ]:
                v = info.get(k_in)
                if v is not None:
                    cur[k_out] = v
        except Exception:
            pass
        out_map[tk] = cur

    # 순서 유지
    return [out_map[tk] for tk in tk_list if tk in out_map]


def _excluded_tickers_set() -> set:
    try:
        from db import connect
        with connect() as conn:
            return {r["ticker"] for r in
                    conn.execute("SELECT ticker FROM excluded_tickers").fetchall()}
    except Exception:
        return set()


def _finviz_screener_movers(sort: str, min_mcap_m: float, limit: int,
                            direction: str, value_field: str,
                            min_change_pct: float = 0.0) -> list[dict]:
    """Finviz Healthcare 스크리너 → 정렬 → mcap·임계값 후처리.
    sort: 'gap' | 'change'. value_field: 'Gap' | 'Change'.
    min_change_pct: |값| >= 이 값인 것만 (예: 10 → ±10% 이상)."""
    import os, io
    import requests
    import pandas as pd

    token = (os.environ.get("FINVIZ_AUTH_TOKEN") or "").strip()
    if not token:
        return [{"error": "FINVIZ_AUTH_TOKEN 없음"}]

    order = ("-" if direction == "up" else "") + sort

    try:
        r = requests.get(
            "https://elite.finviz.com/export.ashx",
            params={"v": "171", "f": "sec_healthcare,cap_smallover",
                    "o": order, "auth": token},
            timeout=15,
        )
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))
        # mcap 보강
        rmcap = requests.get(
            "https://elite.finviz.com/export.ashx",
            params={"v": "111", "f": "sec_healthcare,cap_smallover", "auth": token},
            timeout=15,
        )
        mcap_df = pd.read_csv(io.StringIO(rmcap.text))
        mcap_map = dict(zip(mcap_df["Ticker"], mcap_df["Market Cap"]))
    except Exception as e:
        return [{"error": str(e)}]

    excluded = _excluded_tickers_set()
    out = []
    for _, row in df.iterrows():
        tk = str(row.get("Ticker", "")).upper()
        if not tk or tk in excluded:
            continue
        mcap = mcap_map.get(tk)
        if mcap is None or mcap < min_mcap_m:
            continue
        val_str = str(row.get(value_field) or "")
        try:
            val = float(val_str.replace("%", "").strip())
        except Exception:
            continue
        if direction == "up" and val <= 0:
            continue
        if direction == "down" and val >= 0:
            continue
        if min_change_pct > 0 and abs(val) < min_change_pct:
            continue
        out.append({
            "ticker": tk,
            "price": row.get("Price"),
            f"{sort}_pct": val,
            "change_pct": row.get("Change"),
            "volume": row.get("Volume"),
            "rsi": row.get("Relative Strength Index (14)"),
            "vs_52w_high": row.get("52-Week High"),
            "market_cap_m": mcap,
        })
        if len(out) >= limit:
            break
    return out


def _yfinance_post_movers(min_mcap_m: float, limit: int,
                          direction: str,
                          min_change_pct: float = 0.0) -> list[dict]:
    """애프터마켓 무버 — yfinance per-ticker (병렬). 시총 필터된 universe에서만."""
    from db import connect
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import yfinance as yf

    try:
        with connect() as conn:
            rows = conn.execute(
                "SELECT ticker FROM ticker_master WHERE market_cap >= %s "
                "AND ticker NOT IN (SELECT ticker FROM excluded_tickers)",
                (min_mcap_m,),
            ).fetchall()
        tickers = [r["ticker"] for r in rows]
    except Exception as e:
        return [{"error": str(e)}]
    if not tickers:
        return []
    # 너무 많으면 제한 (비용·시간)
    tickers = tickers[:300]

    def fetch_one(t):
        try:
            info = yf.Ticker(t).info
            pct = info.get("postMarketChangePercent")
            price = info.get("postMarketPrice")
            if pct is None or price is None:
                return None
            return {
                "ticker": t,
                "price": price,
                "post_change_pct": pct,
                "regular_close": info.get("regularMarketPrice"),
                "name": info.get("longName"),
            }
        except Exception:
            return None

    results = []
    with ThreadPoolExecutor(max_workers=15) as ex:
        futs = [ex.submit(fetch_one, t) for t in tickers]
        for fut in as_completed(futs):
            r = fut.result()
            if r:
                results.append(r)

    if direction == "up":
        results = [r for r in results if r["post_change_pct"] > 0]
    elif direction == "down":
        results = [r for r in results if r["post_change_pct"] < 0]
    if min_change_pct > 0:
        results = [r for r in results if abs(r["post_change_pct"]) >= min_change_pct]
    results.sort(key=lambda r: r["post_change_pct"], reverse=(direction == "up"))
    return results[:limit]


def get_market_movers(session: str = "regular", min_mcap_m: float = 1000.0,
                      limit: int = 50, direction: str = "up",
                      min_change_pct: float = 0.0) -> list[dict]:
    """장중 무버 통합 — pre / regular / post 세션 지원.
    session: 'pre' (프리마켓, ET 4-9:30am / KST 18-23:30) |
             'regular' (정규장, ET 9:30am-4pm / KST 23:30-새벽5) |
             'post' (애프터마켓, ET 4-8pm / KST 새벽5-9)
    direction: 'up' | 'down'.
    min_mcap_m: 최소 시총 $M (기본 $1B).
    min_change_pct: 최소 변동률 % (예: 10 → ±10% 이상만). 0이면 필터 없음.
    limit: 최대 결과 수.
    """
    s = (session or "regular").lower()
    if s in ("pre", "premarket", "프리"):
        return _finviz_screener_movers("gap", min_mcap_m, limit, direction, "Gap",
                                       min_change_pct)
    if s in ("regular", "rth", "정규"):
        return _finviz_screener_movers("change", min_mcap_m, limit, direction, "Change",
                                       min_change_pct)
    if s in ("post", "aftermarket", "애프터", "after-hours"):
        return _yfinance_post_movers(min_mcap_m, limit, direction, min_change_pct)
    return [{"error": f"unknown session: {session}"}]


# 호환성
def get_premarket_movers(min_mcap_m: float = 1000.0, limit: int = 20,
                         direction: str = "up") -> list[dict]:
    """[Deprecated → get_market_movers] 프리마켓 무버."""
    return get_market_movers(session="pre", min_mcap_m=min_mcap_m,
                             limit=limit, direction=direction)
    import os, io
    import requests
    import pandas as pd

    token = (os.environ.get("FINVIZ_AUTH_TOKEN") or "").strip()
    if not token:
        return [{"error": "FINVIZ_AUTH_TOKEN 없음"}]

    # 정렬: -gap = gap 큰 순서, gap = gap 작은 순서 (음수 큰 것)
    if direction == "down":
        order = "gap"
    else:
        order = "-gap"

    try:
        r = requests.get(
            "https://elite.finviz.com/export.ashx",
            params={
                "v": "171",
                "f": "sec_healthcare,cap_smallover",   # $300M+ Healthcare
                "o": order,
                "auth": token,
            },
            timeout=15,
        )
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))
    except Exception as e:
        return [{"error": str(e)}]

    if df.empty:
        return []

    # 시총은 view=171에 없음 — 별도 view=111로 보강
    try:
        rmcap = requests.get(
            "https://elite.finviz.com/export.ashx",
            params={
                "v": "111",
                "f": "sec_healthcare,cap_smallover",
                "auth": token,
            },
            timeout=15,
        )
        mcap_df = pd.read_csv(io.StringIO(rmcap.text))
        mcap_map = dict(zip(mcap_df["Ticker"], mcap_df["Market Cap"]))
    except Exception:
        mcap_map = {}

    out = []
    # excluded ticker 블랙리스트
    try:
        from db import connect
        with connect() as conn:
            ex_rows = conn.execute("SELECT ticker FROM excluded_tickers").fetchall()
            excluded = {r["ticker"] for r in ex_rows}
    except Exception:
        excluded = set()

    for _, row in df.iterrows():
        tk = str(row.get("Ticker", "")).upper()
        if not tk or tk in excluded:
            continue
        mcap = mcap_map.get(tk)
        if mcap is None or mcap < min_mcap_m:
            continue
        gap_str = str(row.get("Gap") or "")
        try:
            gap_pct = float(gap_str.replace("%", "").strip())
        except Exception:
            continue
        if direction == "up" and gap_pct <= 0:
            continue
        if direction == "down" and gap_pct >= 0:
            continue
        out.append({
            "ticker": tk,
            "price": row.get("Price"),
            "gap_pct": gap_pct,
            "change_pct": row.get("Change"),
            "volume": row.get("Volume"),
            "rsi": row.get("Relative Strength Index (14)"),
            "vs_52w_high": row.get("52-Week High"),
            "market_cap_m": mcap,
        })
        if len(out) >= limit:
            break

    return out


def _clean_html_text(html: str) -> tuple[str, str]:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "nav", "footer",
                     "header", "aside", "iframe"]):
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    title = (soup.title.string if soup.title and soup.title.string else "").strip()
    return text, title


def fetch_url(url: str, max_chars: int = 8000) -> dict:
    """임의 URL 본문 텍스트 fetch — 뉴스 article, 학회 abstract, 회사 발표문 등.
    검색 도구로 URL을 찾았으면 이 도구로 본문 직접 읽기.
    Google News redirect / JS-rendered 페이지는 Playwright 자동 fallback."""
    from curl_cffi import requests as crq

    text, title, final_url = "", "", url
    err: str | None = None
    fetch_target = url

    # 1) Google News redirect URL이면 먼저 publisher URL로 디코딩
    if "news.google.com" in url:
        try:
            from googlenewsdecoder import gnewsdecoder
            dec = gnewsdecoder(url, interval=1)
            if dec.get("status") and dec.get("decoded_url"):
                fetch_target = dec["decoded_url"]
                final_url = fetch_target
        except Exception as e:
            err = f"decode: {type(e).__name__}: {e}"

    # 2) 정적 fetch (curl_cffi)
    try:
        r = crq.get(fetch_target, impersonate="chrome", timeout=15)
        r.raise_for_status()
        text, title = _clean_html_text(r.text)
        try:
            final_url = str(r.url)
        except Exception:
            pass
    except Exception as e:
        if not err:
            err = f"{type(e).__name__}: {e}"

    # 3) 본문 부족하면 Playwright fallback (JS-rendered 페이지)
    if len(text) < 400:
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
                page.goto(fetch_target, wait_until="domcontentloaded", timeout=25000)
                page.wait_for_timeout(3500)
                final_url = page.url
                rendered = page.content()
                browser.close()
            r_text, r_title = _clean_html_text(rendered)
            if len(r_text) > len(text):
                text, title = r_text, r_title
        except Exception as e:
            if not text:
                err = f"playwright: {type(e).__name__}: {e}"

    out: dict = {
        "url": url,
        "title": title[:200],
        "text": text[:max_chars],
        "length": len(text),
    }
    if final_url and final_url != url:
        out["final_url"] = final_url
    if err and not text:
        out["error"] = err
    return out


def search_news_by_query(query: str, days: int = 30, max_results: int = 15) -> list[dict]:
    """자유 query로 뉴스 검색 (Google News RSS).
    약물 코드(RM-055), 임상명(NCT...), 이슈 키워드 등 특정 토픽 추적용."""
    from news import fetch_google_news
    items = fetch_google_news(query, days=days, limit=max_results)
    return [
        {
            "title": it.get("title", "")[:200],
            "link": it.get("link", ""),
            "source": it.get("source", ""),
            "published": it.get("published", ""),
        }
        for it in items[:max_results]
    ]


# ───────────────────────── WRITE 도구 (대시보드 조작) ─────────────────────────
def watchlist_add(ticker: str) -> dict:
    """관심종목에 추가."""
    import watchlist as wl
    tk = ticker.strip().upper()
    if not tk:
        return {"error": "ticker required"}
    wl.add(tk)
    return {"ok": True, "ticker": tk, "msg": f"{tk} 관심종목에 추가됨"}


def watchlist_remove(ticker: str) -> dict:
    """관심종목에서 제거."""
    import watchlist as wl
    tk = ticker.strip().upper()
    wl.remove(tk)
    return {"ok": True, "ticker": tk, "msg": f"{tk} 관심종목에서 제거됨"}


def memo_add(ticker: str, body: str) -> dict:
    """특정 ticker에 메모 추가 (생성일·수정일 자동)."""
    from memo import add as add_fn
    if not body or not body.strip():
        return {"error": "memo body required"}
    tk = ticker.strip().upper()
    memo_id = add_fn(tk, body.strip())
    return {"ok": True, "memo_id": memo_id, "ticker": tk,
            "msg": f"{tk} 메모 추가됨"}


def memo_delete(memo_id: int) -> dict:
    """메모 삭제 (by id). 보통 get_memos_for(ticker)로 id 먼저 조회 후 호출."""
    from memo import delete as del_fn
    try:
        del_fn(int(memo_id))
        return {"ok": True, "memo_id": memo_id, "msg": f"메모 #{memo_id} 삭제됨"}
    except Exception as e:
        return {"error": str(e)}


def memo_update(memo_id: int, body: str) -> dict:
    """메모 내용 수정 (by id). updated_at 자동 갱신."""
    from memo import update as upd_fn
    if not body or not body.strip():
        return {"error": "memo body required"}
    try:
        upd_fn(int(memo_id), body.strip())
        return {"ok": True, "memo_id": memo_id, "msg": f"메모 #{memo_id} 수정됨"}
    except Exception as e:
        return {"error": str(e)}


def _find_portfolio(name: str) -> dict | None:
    """이름(부분일치 OK)으로 포트폴리오 찾기."""
    from portfolio import list_all
    name_lc = (name or "").lower().strip()
    if not name_lc:
        return None
    ports = list_all()
    # 1) 정확 매치 우선
    for p in ports:
        if p["name"].lower() == name_lc:
            return p
    # 2) 부분 매치
    for p in ports:
        if name_lc in p["name"].lower():
            return p
    return None


def portfolio_set_holding(portfolio_name: str, ticker: str,
                          weight_pct: float) -> dict:
    """포트폴리오에 종목 추가 또는 비중 조정.
    - 이미 보유 → weight 업데이트 (편입가 유지)
    - 미보유 → 새로 편입 (편입가 = 현재가)
    - weight=0 → 제거"""
    from portfolio import (
        list_holdings, add_holding as add_h, update_weight as upd, remove_holding,
    )
    p = _find_portfolio(portfolio_name)
    if not p:
        return {"error": f"포트폴리오 '{portfolio_name}' 못 찾음"}
    tk = ticker.strip().upper()
    holdings = list_holdings(p["id"])
    existing = next((h for h in holdings if h["ticker"] == tk), None)

    if existing and weight_pct == 0:
        remove_holding(existing["id"])
        return {"ok": True, "action": "removed", "portfolio": p["name"], "ticker": tk}
    if existing:
        upd(existing["id"], weight_pct)
        return {"ok": True, "action": "updated",
                "portfolio": p["name"], "ticker": tk,
                "old_weight": existing["weight_pct"], "new_weight": weight_pct}
    # 새 편입
    add_h(p["id"], tk, weight_pct)
    return {"ok": True, "action": "added",
            "portfolio": p["name"], "ticker": tk, "weight": weight_pct}


def portfolio_remove_holding(portfolio_name: str, ticker: str) -> dict:
    """포트폴리오에서 종목 제거."""
    return portfolio_set_holding(portfolio_name, ticker, 0)


def portfolio_create(name: str, initial_size_m: float = 100.0) -> dict:
    """새 모델 포트폴리오 생성. initial_size_m = $M 단위 (기본 100)."""
    from portfolio import create
    pid = create(name.strip(), initial_size=initial_size_m * 1_000_000)
    return {"ok": True, "portfolio_id": pid, "name": name,
            "initial_size_m": initial_size_m}


def portfolio_list() -> list[dict]:
    """모든 포트폴리오 + summary."""
    from portfolio import list_all, summary
    out = []
    for p in list_all():
        s = summary(p["id"])
        out.append({
            "id": p["id"],
            "name": p["name"],
            "initial_size_m": p["initial_size"] / 1e6,
            "current_size_m": s.get("current_size", 0) / 1e6,
            "return_pct": s.get("return_pct", 0),
            "n_holdings": len(s.get("holdings", [])),
            "total_weight_pct": s.get("total_weight", 0),
        })
    return out


def excluded_add(ticker: str, note: str = "") -> dict:
    """ticker 블랙리스트(상승폭/52w high 표시 제외)에 추가."""
    import excluded as ex
    tk = ticker.strip().upper()
    ex.add(tk, note=note)
    return {"ok": True, "ticker": tk, "msg": f"{tk} 비-biotech 블랙리스트 추가됨"}


# ───────────────────────── 카탈리스트 / 인사이더 / IR 마일스톤 ─────────────────────────
def get_catalysts(ticker: str | None = None, days: int = 90,
                  event_types: list[str] | None = None) -> list[dict]:
    """오늘부터 N일 이내 카탈리스트 (PDUFA, 학회, 어닝, 임상 완료, 회사 자체 공개 이벤트).
    ticker=None이면 전체 (학회 포함). event_types=['pdufa','conference','earnings',
    'clinical_completion','company_event']로 필터.
    """
    import catalysts as cat
    df = cat.get_catalysts(ticker=ticker, days=days, event_types=event_types)
    if df.empty:
        return []
    return df.to_dict("records")[:50]


def get_upcoming_pdufa(days: int = 90) -> list[dict]:
    """다가오는 PDUFA 일정 (FDA 결정일)."""
    import catalysts as cat
    df = cat.get_upcoming_pdufa(days=days)
    return df.to_dict("records")[:50] if not df.empty else []


def get_upcoming_conferences(days: int = 90, area: str = "") -> list[dict]:
    """다가오는 학회 일정 (oncology / hepatology / diabetes / cardiology / 등 area로 필터).
    area 비우면 전체. 'oncology', 'liver', 'diabetes' 등 부분 일치."""
    import catalysts as cat
    df = cat.get_upcoming_conferences(days=days, area=area or None)
    return df.to_dict("records")[:50] if not df.empty else []


def refresh_catalysts() -> dict:
    """카탈리스트 캐시 강제 갱신 (PDUFA + 어닝 + 임상 + 학회).
    watchlist 종목만 어닝/임상 fetch (속도)."""
    import catalysts as cat
    return cat.refresh_all(watchlist_only=True)


def get_insider_trades(ticker: str, days: int = 180) -> dict:
    """SEC Form 4 인사이더 매매 (CEO/CFO/Director/등). OpenInsider 기반.
    매매 내역 + 합산 (buy_value, sell_value, net_value) 반환.
    매수(P) > 매도(S) → 시그널 강함."""
    import insiders as ins
    summary = ins.summary_for_ticker(ticker, days=days)
    df = ins.get_insider_trades(ticker, days=days)
    rows = df.head(20).to_dict("records") if not df.empty else []
    return {**summary, "trades_recent": rows}


def get_ir_milestones(ticker: str, refresh: bool = False) -> dict:
    """회사가 IR 자료(투자자 프레젠테이션 PDF)에서 자체 공개한 upcoming milestones.
    PDF에서 'Anticipated Catalysts' 류 섹션 추출 (Q1 2026, 1H 2027, ASCO 2026 등).
    refresh=True면 IR PDF 새로 다운받아 재추출, False면 DB 캐시.
    PDF 추출 실패하면 search_company_milestones로 fallback 권장."""
    import ir_milestones as irm
    if refresh:
        result = irm.extract_for_ticker(ticker, save=True)
        return {
            "ticker": ticker.upper(),
            "deck_title": result.get("deck_title"),
            "deck_url": result.get("deck_url"),
            "milestones": result.get("milestones", []),
            "error": result.get("error"),
        }
    df = irm.get_company_events(ticker)
    if df.empty:
        # lazy: 캐시 없으면 1번만 시도
        result = irm.extract_for_ticker(ticker, save=True)
        df = irm.get_company_events(ticker)
        if df.empty:
            return {"ticker": ticker.upper(),
                    "milestones": [],
                    "note": result.get("error", "추출 실패 — IR PDF 접근 불가 가능성. "
                                     "search_company_milestones 시도 권장")}
    return {
        "ticker": ticker.upper(),
        "milestones": df.to_dict("records")[:30],
    }


def get_earnings_call_milestones(ticker: str, refresh: bool = False) -> dict:
    """최근 분기 어닝콜 요약(Yahoo Finance)에서 회사가 공개한 forward-looking 멘션
    (Q3 2026 readout, 2H 27 phase 1 initial data 등) 추출.
    Finviz quote 페이지의 Yahoo summary 링크 자동 탐색 → fetch_url로 본문 → regex 파싱.
    refresh=True면 재 fetch."""
    import db as _db
    import earnings_calls as ec
    if refresh:
        n = ec.fetch_for_ticker(ticker, max_quarters=3)
    df = _db.pd_read_sql(
        "SELECT event_date, title, description FROM catalysts "
        "WHERE ticker=? AND event_type='earnings_call' ORDER BY event_date ASC",
        params=(ticker.upper(),),
    )
    if df.empty and not refresh:
        n = ec.fetch_for_ticker(ticker, max_quarters=3)
        df = _db.pd_read_sql(
            "SELECT event_date, title, description FROM catalysts "
            "WHERE ticker=? AND event_type='earnings_call' ORDER BY event_date ASC",
            params=(ticker.upper(),),
        )
    return {
        "ticker": ticker.upper(),
        "milestones": df.to_dict("records") if not df.empty else [],
    }


def search_company_milestones(ticker: str, year: int = 2026) -> list[dict]:
    """회사명+ticker로 'upcoming milestones / anticipated catalysts' 관련 뉴스/IR 자료 검색.
    get_ir_milestones가 실패할 때 대체 경로. 결과 URL을 fetch_url로 본문 읽기."""
    import db
    try:
        with db.connect() as conn:
            row = conn.execute(
                "SELECT name FROM ticker_master WHERE ticker = ?", (ticker.upper(),)
            ).fetchone()
            company = (row.get("name") if row else "") or ticker.upper()
    except Exception:
        company = ticker.upper()
    queries = [
        f"{company} {year} anticipated milestones",
        f"{company} upcoming catalysts",
        f"{company} investor presentation {year}",
        f"{ticker} {year} upcoming readouts",
    ]
    out: list[dict] = []
    seen_links: set[str] = set()
    for q in queries:
        try:
            items = search_news_by_query(q, days=180, max_results=5)
            for it in items:
                if it.get("link") in seen_links:
                    continue
                seen_links.add(it.get("link"))
                it["query"] = q
                out.append(it)
        except Exception:
            continue
    return out[:15]


# ───────────────────────── Tool 스키마 (Claude API용) ─────────────────────────
TOOL_DEFS = [
    {
        "name": "search_clinicaltrials",
        "description": "Search ClinicalTrials.gov for clinical trials matching a drug name, "
                       "disease, or NCT number. Use for clinical phase, sponsor, status, "
                       "primary completion dates, intervention details.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Drug name, disease, or NCT id"},
                "max_results": {"type": "integer", "default": 5},
            },
            "required": ["query"],
        },
    },
    {
        "name": "search_pubmed",
        "description": "Search PubMed for recent papers about a drug, mechanism, or trial. "
                       "Returns titles, authors, journals, dates.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "max_results": {"type": "integer", "default": 5},
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_ticker_info",
        "description": "Look up a stock ticker in user's biotech universe (Healthcare). "
                       "Returns name, industry, market cap, current price, and 52w high/low.",
        "input_schema": {
            "type": "object",
            "properties": {"ticker": {"type": "string"}},
            "required": ["ticker"],
        },
    },
    {
        "name": "get_memos_for",
        "description": "Fetch user's personal memos saved for a specific ticker (their notes/thesis).",
        "input_schema": {
            "type": "object",
            "properties": {"ticker": {"type": "string"}},
            "required": ["ticker"],
        },
    },
    {
        "name": "get_drug_moa",
        "description": "Look up a drug's mechanism of action from curated database.",
        "input_schema": {
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
        },
    },
    {
        "name": "fetch_recent_news_for",
        "description": "Get recent news headlines for a specific ticker (last 14 days).",
        "input_schema": {
            "type": "object",
            "properties": {"ticker": {"type": "string"}, "n": {"type": "integer", "default": 5}},
            "required": ["ticker"],
        },
    },
    {
        "name": "get_pipeline_info",
        "description": "Fetch the company's official pipeline page text. Use this when user asks "
                       "about specific drug codes (e.g., 'RM-055', 'BMN-249', 'CTX310') — these "
                       "preclinical/early-stage codes are usually listed on the company's pipeline "
                       "page but not in ClinicalTrials.gov yet.",
        "input_schema": {
            "type": "object",
            "properties": {"ticker": {"type": "string"}},
            "required": ["ticker"],
        },
    },
    {
        "name": "get_market_movers",
        "description": "Top movers among healthcare stocks across pre/regular/post sessions.\n"
                       "Use whenever user asks about live price movements like:\n"
                       "  - '지금 미장에서 10% 이상 오르는 바이오텍' → session=auto, min_change_pct=10\n"
                       "  - '프리마켓 상승률 TOP' → session=pre\n"
                       "  - '오늘 정규장 상승 1B 이상' → session=regular\n"
                       "  - '애프터마켓 가장 많이 떨어진' → session=post, direction=down\n"
                       "\nSession auto-detection — Claude는 사용자 질문 시점(KST)을 기준으로:\n"
                       "  KST 18:00~23:30 = pre (미국 프리마켓)\n"
                       "  KST 23:30~05:00 = regular (정규장)\n"
                       "  KST 05:00~09:00 = post (애프터마켓)\n"
                       "  그 외 시간 = 가장 가까운 직전 세션 사용\n"
                       "\nPre-filtered: Healthcare sector + min mcap + non-biotech blacklist.",
        "input_schema": {
            "type": "object",
            "properties": {
                "session": {"type": "string", "enum": ["pre", "regular", "post"],
                            "description": "현재 시점에 맞는 세션 추론해서 전달"},
                "min_mcap_m": {"type": "number", "default": 1000,
                               "description": "최소 시총 $M (1B = 1000)"},
                "min_change_pct": {"type": "number", "default": 0,
                                   "description": "절대값 임계 (10 = ±10% 이상만)"},
                "direction": {"type": "string", "enum": ["up", "down"], "default": "up"},
                "limit": {"type": "integer", "default": 50},
            },
            "required": ["session"],
        },
    },
    {
        "name": "get_realtime_quote",
        "description": "Real-time stock quote with pre-market/after-hours data. Use whenever user "
                       "asks 'X 주가', '지금 가격', '프리마켓 어때', or wants current quote info. "
                       "Returns: price, change%, gap (premarket open vs prev close), volume, "
                       "20/50/200-day MA distance, RSI, 52w high/low distance, "
                       "plus explicit premarket/postmarket price when those sessions are active. "
                       "Accepts single or comma-separated tickers.",
        "input_schema": {
            "type": "object",
            "properties": {
                "tickers": {"type": "string",
                            "description": "Single ticker or comma-separated (e.g., 'VRTX' or 'VRTX,TGTX')"},
            },
            "required": ["tickers"],
        },
    },
    {
        "name": "fetch_url",
        "description": "Fetch the cleaned text content of any URL. Use after search_news_by_query "
                       "or search_clinicaltrials returns relevant URLs — read the actual article "
                       "to find specific data (PSA50, ORR, OS, safety details, etc.) that's not "
                       "in the headline. Aggressively use this for clinical data questions: "
                       "search → identify relevant article → fetch_url to read full text.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string"},
                "max_chars": {"type": "integer", "default": 8000},
            },
            "required": ["url"],
        },
    },
    {
        "name": "search_news_by_query",
        "description": "Free-form news search via Google News RSS. Use for drug codes, NCT numbers, "
                       "or specific topics that may not be tied to a single ticker. Returns recent "
                       "headlines + links.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "days": {"type": "integer", "default": 30},
                "max_results": {"type": "integer", "default": 15},
            },
            "required": ["query"],
        },
    },
    # ── WRITE 도구 (대시보드 조작) ──
    {
        "name": "watchlist_add",
        "description": "Add a ticker to user's watchlist (관심종목). "
                       "Use when user asks to add/track/save/즐겨찾기 추가.",
        "input_schema": {
            "type": "object",
            "properties": {"ticker": {"type": "string"}},
            "required": ["ticker"],
        },
    },
    {
        "name": "watchlist_remove",
        "description": "Remove a ticker from watchlist (관심종목 해제).",
        "input_schema": {
            "type": "object",
            "properties": {"ticker": {"type": "string"}},
            "required": ["ticker"],
        },
    },
    {
        "name": "memo_add",
        "description": "Add a memo/note to a specific ticker. Body is the user's thought "
                       "to save (생각·의견·thesis). Auto-timestamps.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "body": {"type": "string", "description": "memo content"},
            },
            "required": ["ticker", "body"],
        },
    },
    {
        "name": "memo_delete",
        "description": "Delete a memo by its id. To find the id, call get_memos_for(ticker) first "
                       "and identify which memo to delete based on user's request "
                       "(e.g., '가장 최근', '특정 내용 포함').",
        "input_schema": {
            "type": "object",
            "properties": {"memo_id": {"type": "integer"}},
            "required": ["memo_id"],
        },
    },
    {
        "name": "memo_update",
        "description": "Edit existing memo content by id (preserves created_at, updates updated_at).",
        "input_schema": {
            "type": "object",
            "properties": {
                "memo_id": {"type": "integer"},
                "body": {"type": "string"},
            },
            "required": ["memo_id", "body"],
        },
    },
    {
        "name": "portfolio_set_holding",
        "description": "Add a ticker to a Model Portfolio OR update its weight. "
                       "If ticker already in portfolio, updates the weight. "
                       "If not, adds new holding with current price as entry. "
                       "Setting weight_pct=0 removes the holding. "
                       "portfolio_name supports partial match (e.g., 'mp1', 'Bio Fund').",
        "input_schema": {
            "type": "object",
            "properties": {
                "portfolio_name": {"type": "string"},
                "ticker": {"type": "string"},
                "weight_pct": {"type": "number", "description": "% of fund (0~100)"},
            },
            "required": ["portfolio_name", "ticker", "weight_pct"],
        },
    },
    {
        "name": "portfolio_remove_holding",
        "description": "Remove a ticker from a Model Portfolio entirely.",
        "input_schema": {
            "type": "object",
            "properties": {
                "portfolio_name": {"type": "string"},
                "ticker": {"type": "string"},
            },
            "required": ["portfolio_name", "ticker"],
        },
    },
    {
        "name": "portfolio_create",
        "description": "Create a new Model Portfolio with given name and initial fund size in $M.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "initial_size_m": {"type": "number", "default": 100,
                                    "description": "in millions USD"},
            },
            "required": ["name"],
        },
    },
    {
        "name": "portfolio_list",
        "description": "List all Model Portfolios with summary (size, return%, holdings count).",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "excluded_add",
        "description": "Add a ticker to the non-biotech blacklist (hides from 52w/top movers boards). "
                       "Use when user says X is not biotech / not relevant / 제외해줘.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "note": {"type": "string", "default": ""},
            },
            "required": ["ticker"],
        },
    },
    {
        "name": "get_catalysts",
        "description": "Upcoming catalysts within next N days. Includes PDUFA dates, "
                       "medical conferences (ASCO/ASH/EASL/ADA/etc), earnings, clinical "
                       "data readouts (interim + final), company-disclosed milestones from "
                       "IR decks, and forward-looking statements from earnings calls. "
                       "ticker omitted = all (sector-wide). event_types filter: "
                       "pdufa / conference / earnings / clinical_readout / "
                       "clinical_milestone / regulatory / company_event.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "days": {"type": "integer", "default": 90},
                "event_types": {"type": "array", "items": {"type": "string"}},
            },
        },
    },
    {
        "name": "get_upcoming_pdufa",
        "description": "Upcoming FDA PDUFA decisions within next N days.",
        "input_schema": {
            "type": "object",
            "properties": {"days": {"type": "integer", "default": 90}},
        },
    },
    {
        "name": "get_upcoming_conferences",
        "description": "Upcoming medical/investor conferences within N days. "
                       "Optional area filter: 'oncology', 'hepatology', 'diabetes', "
                       "'cardiology', 'neurology', 'investor', etc.",
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "default": 90},
                "area": {"type": "string", "default": ""},
            },
        },
    },
    {
        "name": "refresh_catalysts",
        "description": "Force refresh catalysts cache (re-scrape biopharmcatalyst PDUFA, "
                       "fetch earnings/clinical for watchlist). Use sparingly.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_insider_trades",
        "description": "SEC Form 4 insider trading (CEO/CFO/Director purchases & sales). "
                       "Returns summary (buy_value, sell_value, net_value) + recent 20 trades. "
                       "Insiders buying (P-Purchase) is bullish signal; heavy selling bearish.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "days": {"type": "integer", "default": 180},
            },
            "required": ["ticker"],
        },
    },
    {
        "name": "get_ir_milestones",
        "description": "Extract company-disclosed upcoming milestones from latest investor "
                       "presentation PDF. Finds 'Anticipated Catalysts' / 'Upcoming Milestones' "
                       "section and parses dated bullet points (Q1 2026, 1H 2027, ASCO 2026 등). "
                       "If extraction fails, use search_company_milestones as fallback.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "refresh": {"type": "boolean", "default": False},
            },
            "required": ["ticker"],
        },
    },
    {
        "name": "get_earnings_call_milestones",
        "description": "Extract forward-looking milestone disclosures from recent transcripts "
                       "(investing.com — quarterly earnings calls + conference presentations "
                       "like Leerink, JPM, TD Cowen, Goldman Healthcare, Cantor, Wells Fargo). "
                       "Picks up phrases like '2H 26 phase 1 initial data', 'expect Q3 2026 "
                       "readout', 'initiate trial in late 2026'. Most reliable source for "
                       "company-disclosed catalysts including specific drug+date combos.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "refresh": {"type": "boolean", "default": False},
            },
            "required": ["ticker"],
        },
    },
    {
        "name": "search_company_milestones",
        "description": "Search news/PR for company-disclosed upcoming milestones when "
                       "get_ir_milestones fails. Returns URLs of relevant articles "
                       "(earnings press releases, investor day announcements). "
                       "Always follow up with fetch_url to read article body for actual data.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "year": {"type": "integer", "default": 2026},
            },
            "required": ["ticker"],
        },
    },
]


def run_tool(name: str, args: dict):
    """Tool name으로 디스패치."""
    funcs = {
        # 조회
        "search_clinicaltrials": search_clinicaltrials,
        "search_pubmed": search_pubmed,
        "get_ticker_info": get_ticker_info,
        "get_memos_for": get_memos_for,
        "get_drug_moa": get_drug_moa,
        "fetch_recent_news_for": fetch_recent_news_for,
        "get_pipeline_info": get_pipeline_info,
        "search_news_by_query": search_news_by_query,
        "fetch_url": fetch_url,
        "get_realtime_quote": get_realtime_quote,
        "get_premarket_movers": get_premarket_movers,
        "get_market_movers": get_market_movers,
        # write
        "watchlist_add": watchlist_add,
        "watchlist_remove": watchlist_remove,
        "memo_add": memo_add,
        "memo_delete": memo_delete,
        "memo_update": memo_update,
        "portfolio_set_holding": portfolio_set_holding,
        "portfolio_remove_holding": portfolio_remove_holding,
        "portfolio_create": portfolio_create,
        "portfolio_list": portfolio_list,
        "excluded_add": excluded_add,
        # 카탈리스트 / 인사이더
        "get_catalysts": get_catalysts,
        "get_upcoming_pdufa": get_upcoming_pdufa,
        "get_upcoming_conferences": get_upcoming_conferences,
        "refresh_catalysts": refresh_catalysts,
        "get_insider_trades": get_insider_trades,
        "get_ir_milestones": get_ir_milestones,
        "get_earnings_call_milestones": get_earnings_call_milestones,
        "search_company_milestones": search_company_milestones,
    }
    f = funcs.get(name)
    if not f:
        return {"error": f"unknown tool {name}"}
    try:
        return f(**args)
    except Exception as e:
        return {"error": str(e)}
