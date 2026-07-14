"""Telegram bot이 호출하는 외부/내부 도구 (Claude tool calling용)."""
from __future__ import annotations

from pathlib import Path

import requests
from dotenv import load_dotenv

import yf_session  # noqa: F401 — yfinance 레이트리밋 패치 (import 부수효과)

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


# ───────────────────────── 밸류에이션 ─────────────────────────
# ───────────── Europe PMC (논문 + 프리프린트 + 학회 초록 통합) ─────────────
EUROPEPMC_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"


def _europepmc_search(query: str, max_results: int = 6) -> list[dict]:
    import time as _t
    params = {
        "query": query,
        "format": "json",
        "pageSize": max(1, min(int(max_results), 25)),
        "resultType": "lite",
        "sort": "P_PDATE_D desc",
    }
    last_err = None
    for attempt in range(3):           # 일시적 5xx/타임아웃 재시도
        try:
            r = requests.get(EUROPEPMC_URL, params=params,
                             headers={"User-Agent": "biotech_radar/1.0"}, timeout=20)
            if r.status_code >= 500:
                last_err = f"{r.status_code} {r.reason}"
                _t.sleep(1.5 * (attempt + 1))
                continue
            r.raise_for_status()
            res = ((r.json() or {}).get("resultList") or {}).get("result") or []
            break
        except Exception as e:
            last_err = str(e)
            _t.sleep(1.0 * (attempt + 1))
    else:
        return [{"_error": f"Europe PMC error: {last_err}"}]
    out = []
    for x in res[:max_results]:
        src = x.get("source") or ""
        pmid, doi = x.get("pmid"), x.get("doi")
        if pmid:
            url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
        elif doi:
            url = f"https://doi.org/{doi}"
        else:
            url = f"https://europepmc.org/article/{src}/{x.get('id')}"
        out.append({
            "title": x.get("title"),
            "authors": (x.get("authorString") or "")[:200],
            "journal": x.get("journalTitle") or x.get("bookOrReportDetails") or src,
            "year": x.get("pubYear"),
            "source": src,                  # MED=PubMed, PMC, PPR=preprint
            "is_preprint": src == "PPR",
            "doi": doi,
            "cited_by": x.get("citedByCount"),
            "url": url,
        })
    return out or [{"_note": "결과 없음 — query를 단순화하거나 동의어로 재시도"}]


def search_europepmc(query: str, max_results: int = 6) -> list[dict]:
    """Europe PMC 통합 문헌 검색 — 논문 + 프리프린트 + 학회 초록까지 한 번에.
    상장 여부 무관, 기전/모달리티/적응증/타깃 어떤 주제든. 본문 수치는 fetch_url로."""
    return _europepmc_search(query, max_results)


def search_preprints(query: str, max_results: int = 6) -> list[dict]:
    """bioRxiv·medRxiv 등 프리프린트 검색 (Europe PMC SRC:PPR). peer-review 전 최신 연구."""
    return _europepmc_search(f'({query}) AND (SRC:"PPR")', max_results)


def search_conference_abstracts(query: str, society: str = "",
                                max_results: int = 6) -> list[dict]:
    """ASCO/AACR/ESMO/ASH 등 학회 발표·초록 검색. 학회 초록은 JCO(ASCO)·
    Cancer Research(AACR) 보충판으로 Europe PMC에 색인됨."""
    soc = (society or "").strip()
    terms = f"({query})"
    if soc:
        terms += f' AND ("{soc}" OR {soc})'
    q = f'{terms} AND (PUB_TYPE:"Meeting Abstract" OR "abstract" OR "meeting" OR "congress")'
    res = _europepmc_search(q, max_results)
    if res and (res[0].get("_error") or res[0].get("_note")):
        res = _europepmc_search(terms, max_results)   # 너무 좁으면 fallback
    return res


def get_valuation_metrics(ticker: str) -> dict:
    """yfinance 기반 밸류에이션 지표.
    Returns: market_cap, enterprise_value, P/E (trailing+forward), EV/Revenue,
             EV/EBITDA, P/S, P/B, cash, debt, revenue (TTM), ebit, ebitda, net_income.
    EV/EBIT은 EBITDA로 근사 (yfinance가 EBIT 별도 제공 안 함).
    bio-pharma는 P/E·EV/EBIT 무의미한 경우 많음 (적자) — null로 명시."""
    import yfinance as yf
    tk = ticker.upper()
    try:
        info = yf.Ticker(tk).info or {}
    except Exception as e:
        return {"ticker": tk, "error": f"yfinance: {e}"}

    def _b(v):
        """USD raw → $B (소수 2자리)."""
        return round(v / 1e9, 3) if v else None

    def _f(v, ndigits=2):
        return round(v, ndigits) if (v is not None and v == v) else None

    out = {
        "ticker": tk,
        "name": info.get("longName") or info.get("shortName"),
        "currency": info.get("currency"),
        # 시총 / EV
        "market_cap_b_usd": _b(info.get("marketCap")),
        "enterprise_value_b_usd": _b(info.get("enterpriseValue")),
        # 손익
        "revenue_ttm_b_usd": _b(info.get("totalRevenue")),
        "ebitda_b_usd": _b(info.get("ebitda")),
        "net_income_b_usd": _b(info.get("netIncomeToCommon")),
        "gross_margin_pct": _f((info.get("grossMargins") or 0) * 100, 1)
                            if info.get("grossMargins") else None,
        "operating_margin_pct": _f((info.get("operatingMargins") or 0) * 100, 1)
                                 if info.get("operatingMargins") else None,
        # 멀티플
        "pe_trailing": _f(info.get("trailingPE")),
        "pe_forward": _f(info.get("forwardPE")),
        "ev_revenue": _f(info.get("enterpriseToRevenue")),
        "ev_ebitda": _f(info.get("enterpriseToEbitda")),
        "ps_trailing": _f(info.get("priceToSalesTrailing12Months")),
        "pb": _f(info.get("priceToBook")),
        # 현금/부채 (cash runway 추정용)
        "cash_b_usd": _b(info.get("totalCash")),
        "debt_b_usd": _b(info.get("totalDebt")),
        # 주식
        "shares_outstanding_m": _f(
            (info.get("sharesOutstanding") or 0) / 1e6, 1
        ) if info.get("sharesOutstanding") else None,
        "free_cash_flow_b_usd": _b(info.get("freeCashflow")),
    }
    # 적자 시 P/E·EV/EBITDA를 None으로 명시 (학습 데이터로 만들지 말라는 신호)
    if out["net_income_b_usd"] and out["net_income_b_usd"] < 0:
        out["note_pe"] = "적자 — P/E 무의미"
    if out["ebitda_b_usd"] and out["ebitda_b_usd"] < 0:
        out["note_ev_ebitda"] = "EBITDA 적자 — EV/EBITDA 무의미"
    return out


# ───────────────────────── Supabase 조회 ─────────────────────────
def get_ticker_info(ticker: str) -> dict:
    """ticker_master + 최신 high_low_cache 조회. market_cap은 $M (백만달러) 단위.
    더 정확한 실시간 시총이 필요하면 get_realtime_quote의 market_cap_b_usd 사용."""
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
        # 시총 가독성 — market_cap이 $M 단위이므로 B 단위도 추가
        if out.get("market_cap"):
            out["market_cap_unit"] = "$M (백만달러)"
            out["market_cap_b_usd"] = round(out["market_cap"] / 1000.0, 3)
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
                ("marketCap", "market_cap_usd"),     # 실시간 시총 (USD raw)
                ("sharesOutstanding", "shares_outstanding"),
                ("currency", "currency"),
            ]:
                v = info.get(k_in)
                if v is not None:
                    cur[k_out] = v
            # USD M 환산값도 함께 (가독성)
            if "market_cap_usd" in cur and cur["market_cap_usd"]:
                cur["market_cap_m_usd"] = round(cur["market_cap_usd"] / 1e6, 1)
                cur["market_cap_b_usd"] = round(cur["market_cap_usd"] / 1e9, 3)
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
    """포트폴리오 비중을 **현재 NAV 대비 목표 %**로 조정 — 현재가로 체결(매수/매도)하여
    실현손익 확정 + 현금 반영(거래기반·평균단가). 미보유면 신규 편입, weight=0이면 전량 매도.
    예: 'X를 3%로 축소', 'Y 8%로 확대', 'Z 편입 5%'."""
    from portfolio import set_target_weight, list_all
    p = _find_portfolio(portfolio_name) if portfolio_name else None
    if not p:                                   # 이름 미지정 또는 단일 MP → 기본 사용
        ports = list_all()
        if ports and (not portfolio_name or len(ports) == 1):
            p = ports[0]
    if not p:
        return {"error": f"포트폴리오 '{portfolio_name}' 못 찾음 (이름을 확인하세요)"}
    try:
        r = set_target_weight(p["id"], ticker.strip().upper(), float(weight_pct))
        r["portfolio"] = p["name"]
        return r
    except Exception as e:
        return {"error": str(e)}


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


def portfolio_detail(name_or_id=None) -> dict:
    """단일 포트폴리오 상세 — 편입 종목별 weight/return + 각 종목의 사용자 메모.
    name_or_id 없으면 첫 번째 포트폴리오. 메모 = memo.list_for(ticker) (Claude 메모 아님,
    사용자가 보드에 적은 코멘트).
    "MP·비중·수익률·코멘트" 류 질문 한 번에 처리 — generate_investment_report 호출 금지."""
    from portfolio import list_all, summary, get as pf_get
    from memo import list_for as memo_list_for
    ports = list_all()
    if not ports:
        return {"error": "no portfolios"}
    if name_or_id is None:
        p = ports[0]
    elif isinstance(name_or_id, int) or str(name_or_id).isdigit():
        p = pf_get(int(name_or_id))
        if not p:
            return {"error": f"portfolio id {name_or_id} not found"}
    else:
        # name match (case-insensitive 부분 일치)
        matches = [x for x in ports
                   if str(name_or_id).lower() in (x.get("name") or "").lower()]
        if not matches:
            return {"error": f"portfolio '{name_or_id}' not found",
                    "available": [x["name"] for x in ports]}
        p = matches[0]
    s = summary(p["id"])
    if not s:
        return {"error": "summary 실패"}
    holdings_out = []
    for h in s.get("holdings", []):
        tk = h["ticker"]
        memos = memo_list_for(tk)[:3]   # 최근 3개
        holdings_out.append({
            "ticker": tk,
            "weight_pct": h.get("weight_pct"),
            "entry_price": h.get("entry_price"),
            "current_price": h.get("curr_price"),
            "return_pct": h.get("return_pct"),
            "amt_initial_m": (h.get("amt_initial") or 0) / 1e6,
            "amt_current_m": (h.get("amt_current") or 0) / 1e6,
            "note": h.get("note"),
            "memos": [{"body": m["body"][:300], "created_at": m["created_at"]}
                      for m in memos],
        })
    return {
        "portfolio": {
            "id": p["id"], "name": p["name"],
            "initial_size_m": p["initial_size"] / 1e6,
            "current_size_m": s["current_size"] / 1e6,
            "return_pct": round(s.get("return_pct", 0), 2),
            "cash_pct": round(s.get("cash_pct", 0), 1),
            "total_weight_pct": round(s.get("total_weight", 0), 1),
        },
        "holdings": holdings_out,
    }


# ───────────────────────── 가격 트리거 ─────────────────────────
def create_price_trigger(ticker: str, direction: str, threshold: float,
                         note: str = "") -> dict:
    """가격 트리거 등록. direction='above' (이상 돌파) 또는 'below' (이하 하락).
    PHVS 32달러 돌파 → direction='above', threshold=32.0.
    30분마다 + PC 부팅·로그온 시 체크되어 발동 시 텔레그램 알림.
    한 번 발동되면 status='fired'로 마킹 — 재발송 없음."""
    import price_triggers as pt
    tid = pt.create(ticker, direction, threshold, note=note)
    return {"ok": True, "trigger_id": tid, "ticker": ticker.upper(),
            "direction": direction, "threshold": threshold,
            "msg": f"✓ #{tid}: {ticker.upper()} {direction} ${threshold:.2f} 등록"}


def list_price_triggers(status: str = "active") -> list[dict]:
    """가격 트리거 목록. status='active' (기본) / 'fired' / 'cancelled'."""
    import price_triggers as pt
    return pt.list_all(status=status)


def cancel_price_trigger(trigger_id: int) -> dict:
    """가격 트리거 취소."""
    import price_triggers as pt
    pt.cancel(trigger_id)
    return {"ok": True, "trigger_id": trigger_id, "msg": f"✓ #{trigger_id} 취소"}


def check_price_triggers_now() -> dict:
    """즉시 트리거 체크 + 발동 시 알림 발송 (테스트용)."""
    from telegram_report import send_trigger_alerts
    n = send_trigger_alerts()
    return {"ok": True, "fired": n}


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


def discover_catalysts_via_ai(ticker: str) -> dict:
    """ticker의 향후 12개월 카탈리스트 능동 조사 — 정적 소스(ClinicalTrials/하드코딩 학회/
    yfinance 어닝)가 놓치는 Investor Day, accelerated CVOT readout, KOL event,
    회사 가이던스 변경 등을 Claude tool calling으로 발굴 후 catalysts 테이블에 저장.
    1-3분 소요. 사용자가 'X 카탈리스트 다시 찾아', 'X 누락된 일정 발굴' 등 요청 시."""
    import catalysts as cat
    return cat.discover_catalysts_via_ai(ticker)


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


def get_new_today_highs(limit: int = 100) -> list[dict]:
    """오늘 신규 52주 신고가 종목 리스트. ticker, name, close, perf_1d, market_cap 포함.
    리포트 일괄 생성·요약 등에 활용."""
    from collectors.high_low import fetch_new_today_highs
    df = fetch_new_today_highs(limit=limit)
    if df.empty:
        return []
    return df.to_dict("records")


def generate_investment_report(ticker: str) -> dict:
    """ticker에 대해 institutional-quality 투자 메모 생성 (Claude API).
    포함: 투자 포인트(thesis), 최근 주가 동향+상승 이유, 카탈리스트 워치, 인사이더 시그널,
    리스크, bottom line. ~15-25줄 markdown.
    데이터 소스: ticker_master / high_low_cache / catalysts / insider_trades / 펀더멘탈 뉴스 /
    pipeline 멘션. 매일 7am 자동 발송 + 수동 ('ARWR 리포트' 같은 질의)에 사용."""
    import investment_report as ir
    result = ir.generate_and_save(ticker)
    return {"ticker": ticker.upper(), "report": result["body"]}


def send_thesis_pdf(ticker: str, refresh: bool = False) -> dict:
    """투자 메모를 PDF로 만들어 텔레그램에 첨부 발송.
    refresh=False (기본): 캐시된 메모(ai_reports) 있으면 즉시 PDF 생성·발송.
                          캐시 없으면 generate_and_save 호출 후 PDF.
    refresh=True: 새로 deep research 후 PDF.
    사용자가 'X thesis PDF로', 'X 메모 PDF', 'X 리포트 PDF' 같은 요청 시.
    """
    import os
    import investment_report as ir
    from pdf_gen import render_pdf_to_file
    from telegram_report import send_document

    tk = ticker.upper()
    # 캐시 확인
    cached = ir.get_cached_report(tk) if not refresh else None
    if cached and cached.get("body"):
        report_md = cached["body"]
        source = f"cached @ {cached.get('generated_at', '')[:16]}"
    else:
        result = ir.generate_and_save(tk)
        report_md = result["body"]
        source = "freshly generated"

    if not report_md or len(report_md) < 100:
        return {"ticker": tk, "error": "리포트 본문 비어있음"}

    # TL;DR 추출 (캡션용)
    tldr, body = ir.split_tldr_and_body(report_md)
    # markdown → HTML 변환 (텔레그램 캡션용 — telegram_report._markdown_to_html과 동일)
    import re as _re
    def _md2html(s):
        s = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        s = _re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", s)
        s = _re.sub(r"(?<![\*\n])\*([^*\n]+)\*(?!\*)", r"<i>\1</i>", s)
        return s
    tldr_html = _md2html(tldr)
    caption = f"📊 <b>{tk} 투자 메모</b> <i>({source})</i>\n\n{tldr_html}"
    if len(caption) > 1000:
        caption = caption[:990] + "\n…(요약 잘림, 전체는 PDF)"

    # PDF 생성 + 발송
    pdf_path = render_pdf_to_file(body, ticker=tk)
    try:
        send_document(pdf_path, caption=caption)
    finally:
        try:
            os.unlink(pdf_path)
        except Exception:
            pass
    return {"ticker": tk, "sent": True, "source": source,
            "pdf_size_kb": None}


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


def render_candle_png(ticker: str, period: str = "2y"):
    """캔들 차트 PNG 렌더 → (파일경로, 마지막종가). 실패 시 (None, None). 호출자가 파일 삭제.
    OHLCV는 토스(로컬)/DB캐시(클라우드). 3y/5y는 주봉, 그 이하는 일봉. 이평선+거래량."""
    import os
    import tempfile
    from prices import fetch_chart
    tk = (ticker or "").strip().upper()
    if not tk:
        return None, None
    interval = "1wk" if period in ("3y", "5y") else "1d"
    try:
        df = fetch_chart(tk, period, interval)
    except Exception:
        return None, None
    if df is None or df.empty:
        return None, None
    df = df[["Open", "High", "Low", "Close", "Volume"]].dropna()
    if len(df) < 3:
        return None, None
    try:
        import matplotlib
        matplotlib.use("Agg")
        import mplfinance as mpf
        mc = mpf.make_marketcolors(up="#26a69a", down="#ef5350", inherit=True)
        # 한글 폰트(일봉/주봉·한글 종목명) 렌더 — Malgun Gothic(Windows). 영문 포함이라 미장도 안전.
        style = mpf.make_mpf_style(
            base_mpf_style="yahoo", marketcolors=mc,
            rc={"font.family": "Malgun Gothic", "axes.unicode_minus": False},
        )
        mav = (10, 30) if interval == "1wk" else (20, 60)
        path = os.path.join(tempfile.gettempdir(), f"chart_{tk}_{period}.png")
        mpf.plot(df, type="candle", style=style, mav=mav, volume=True, figsize=(11, 6),
                 title=f"{tk}  ·  {period} {'주봉' if interval == '1wk' else '일봉'}",
                 savefig=dict(fname=path, dpi=120, bbox_inches="tight"))
        return path, float(df["Close"].iloc[-1])
    except Exception:
        return None, None


def send_chart(ticker: str, period: str = "2y") -> dict:
    """종목 캔들스틱 차트(기본 2년 일봉)를 텔레그램으로 발송. period: 6m/1y/2y/3y/5y."""
    import os
    tk = (ticker or "").strip().upper()
    if not tk:
        return {"error": "ticker required"}
    path, last = render_candle_png(tk, period)
    if not path:
        return {"error": f"{tk} 차트 데이터 없음 (토스 미지원/캐시 없음)"}
    is_w = period in ("3y", "5y")
    try:
        from telegram_report import send_photo
        send_photo(path, caption=f"📊 <b>{tk}</b> · {period} "
                                 f"{'주봉' if is_w else '일봉'} 캔들 · 종가 ${last:,.2f}")
    except Exception as e:
        return {"error": f"발송 실패: {e}"}
    finally:
        try:
            os.unlink(path)
        except Exception:
            pass
    return {"ok": True, "ticker": tk, "period": period,
            "last_close": last, "msg": f"{tk} {period} 캔들차트 발송"}


def send_card(ticker: str) -> dict:
    """단일 종목 **카드 1메시지** — 캔들차트 + 시총/현재가/수익률 + 주가동인 뉴스 2개를
    하나로 묶어 텔레그램 발송. 'X 카드', 'X 보여줘'에 사용(차트만이면 send_chart)."""
    try:
        from telegram_report import send_card as _sc
        return _sc(ticker)
    except Exception as e:
        return {"error": str(e)}


def send_text_telegram(text: str) -> dict:
    """임의 텍스트(요약/정리 등)를 텔레그램으로 발송. '텔레그램으로 보내줘' 요청 시.
    text는 markdown 가능 — HTML로 변환해 발송(실패 시 평문). 길면 자동 분할."""
    try:
        from telegram_report import send, _markdown_to_html
        send(_markdown_to_html(text))
        return {"ok": True, "msg": "텔레그램 전송 완료"}
    except Exception as e:
        return {"error": str(e)}


def export_pdf(title: str, markdown: str) -> dict:
    """markdown 본문을 PDF로 만들어 텔레그램으로 발송. '원페이저/PDF로 뽑아줘' 요청 시.
    모델이 직접 요약·정리 본문을 작성해 markdown에 담아 전달. title은 문서 제목."""
    import os
    if not (markdown or "").strip():
        return {"error": "markdown 본문이 비어 있음 — 요약 내용을 작성해 전달하세요"}
    try:
        from pdf_gen import render_pdf_to_file
        from telegram_report import send_document
        safe = (title or "문서").strip()
        path = render_pdf_to_file(markdown, ticker=(safe[:18] or "doc"), title=safe)
        try:
            send_document(path, caption=f"📄 <b>{safe}</b>")
        finally:
            try:
                os.unlink(path)
            except Exception:
                pass
        return {"ok": True, "msg": f"'{safe}' PDF 텔레그램 전송 완료"}
    except Exception as e:
        return {"error": f"PDF 생성/발송 실패: {e}"}


def count_52w_highs(date: str = "") -> dict:
    """특정 날짜(또는 최신)의 52주 신고가 바이오텍(시총≥$1.5B) **종목 수**.
    date: 'YYYY-MM-DD' (없으면 최신 수집일). '신고가 종목 수', '대비 얼마나 늘었나' 류
    날짜 비교 질문에 두 날짜로 각각 호출해 차이를 계산. (리스트가 아니라 카운트)."""
    from db import connect
    from collectors.high_low import BIOTECH_INDUSTRY_FILTER, _excluded_ticker_filter
    with connect() as c:
        if not date:
            row = c.execute("SELECT max(computed_date) AS d FROM high_low_cache").fetchone()
            date = row["d"] if row else None
        if not date:
            return {"error": "수집 데이터 없음"}
        if not c.execute("SELECT 1 FROM high_low_cache WHERE computed_date=? LIMIT 1",
                         (date,)).fetchone():
            rng = c.execute("SELECT min(computed_date) AS mn, max(computed_date) AS mx "
                            "FROM high_low_cache").fetchone()
            return {"error": f"{date} 데이터 없음 (수집 범위 {rng['mn']}~{rng['mx']})"}
        n = c.execute(
            f"""SELECT count(*) AS n FROM high_low_cache h
                LEFT JOIN ticker_master t ON t.ticker = h.ticker
                WHERE h.computed_date = ? AND h.market_cap >= 1000
                  AND {BIOTECH_INDUSTRY_FILTER} AND {_excluded_ticker_filter('h')}
                  AND h.today_high >= h.high_52w * 0.999""",
            (date,),
        ).fetchone()["n"]
    return {"date": date, "count_52w_high": int(n), "universe": "biotech, 시총≥$1B"}


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
        "name": "get_valuation_metrics",
        "description": "Valuation multiples + financials for a ticker via yfinance. Returns "
                       "market cap, enterprise value, P/E (trailing+forward), EV/Revenue, "
                       "EV/EBITDA, P/S, P/B, revenue (TTM), EBITDA, net income, cash, debt, "
                       "shares outstanding, FCF — all in $B. Loss-making biotechs are "
                       "flagged with note_pe / note_ev_ebitda ('적자 — 무의미'). Always call "
                       "this before stating any valuation multiple — never guess from training.",
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
        "description": "Adjust a ticker's weight in a Model Portfolio to a TARGET % of "
                       "current NAV. Executes a buy or sell at the current price — realized "
                       "P&L is booked and cash updated (transaction-based, average-cost). "
                       "Use for trim ('cut X to 3%'), add ('raise Y to 8%'), new entry, or "
                       "weight_pct=0 to liquidate the whole position. portfolio_name supports "
                       "partial match (e.g., 'mp1').",
        "input_schema": {
            "type": "object",
            "properties": {
                "portfolio_name": {"type": "string"},
                "ticker": {"type": "string"},
                "weight_pct": {"type": "number",
                               "description": "target weight as % of current NAV (0~100)"},
            },
            "required": ["ticker", "weight_pct"],
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
        "name": "portfolio_detail",
        "description": "Single portfolio detail in ONE call — per-holding ticker, weight, "
                       "entry/current price, return %, and the user's saved memos (보드 "
                       "코멘트). Use this whenever user asks about portfolio holdings + "
                       "comments/memos/returns together. DO NOT call generate_investment_report "
                       "for each holding — 보드 메모는 사용자가 직접 적은 노트라 그 자체로 답.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name_or_id": {
                    "type": ["string", "integer"],
                    "description": "Portfolio id or name (부분 매칭). Empty면 첫 번째.",
                },
            },
        },
    },
    {
        "name": "create_price_trigger",
        "description": "Register a price trigger. When ticker's live price crosses threshold "
                       "in the specified direction, the bot sends a Telegram alert with "
                       "volume z-score, recent news, and past memos. Fires only once. "
                       "Trigger when user says like 'PHVS 32달러 돌파 알림', "
                       "'X 50불 아래로 떨어지면 알려줘', 'alert me when X above 100'.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "direction": {"type": "string", "enum": ["above", "below"]},
                "threshold": {"type": "number"},
                "note": {"type": "string"},
            },
            "required": ["ticker", "direction", "threshold"],
        },
    },
    {
        "name": "list_price_triggers",
        "description": "List price triggers. status='active' (default) / 'fired' / 'cancelled'.",
        "input_schema": {
            "type": "object",
            "properties": {"status": {"type": "string", "default": "active"}},
        },
    },
    {
        "name": "cancel_price_trigger",
        "description": "Cancel an active price trigger by id.",
        "input_schema": {
            "type": "object",
            "properties": {"trigger_id": {"type": "integer"}},
            "required": ["trigger_id"],
        },
    },
    {
        "name": "check_price_triggers_now",
        "description": "Immediately check all active triggers and send alerts. "
                       "Use only if user explicitly wants to force-check now.",
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
        "name": "get_dart_disclosures",
        "description": "한국 종목(KR, 6자리 코드) 공시 — **네이버 실시간 종목공시 + DART 병합** "
                       "(당일 막 올라온 유증/주요사항도 포함; DART API 인덱싱 지연 보완). "
                       "유상증자/CB·BW, "
                       "기술이전·단일판매공급계약(수주), 식약처 품목허가, 임상 관련 주요사항보고, "
                       "잠정실적, 임원·주요주주 소유보고 등 한국 카탈리스트·재무의 1차 출처. "
                       "types: 'B'(주요사항보고)·'I'(거래소)·'A'(정기)·'D'(지분) 단일문자, 생략 시 전체. "
                       "미국·비상장은 빈 결과(corp_code 없음).",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string", "description": "6자리 한국 종목코드 (예: 196170)"},
                "days": {"type": "integer", "default": 30},
                "types": {"type": "string"},
            },
            "required": ["ticker"],
        },
    },
    {
        "name": "get_dart_document",
        "description": "한국 공시의 **원문 본문 전체 텍스트**를 DART 공식 API(document.xml)로 직접 읽음. "
                       "공시 뷰어(dart.fss.or.kr) 본문은 iframe이라 스크랩 불가 — 정정공시/유증결정/"
                       "주요사항보고서 등 **본문 내용(정정 사유·변경 전후·금액·일정)을 읽어야 할 땐 반드시 "
                       "이 도구**를 써라. 먼저 get_dart_disclosures로 해당 공시의 rcept_no를 얻어 넘긴다. "
                       "당일 막 올라온 공시는 DART 인덱싱 지연으로 014(원본없음)가 날 수 있음(잠시 후 가능).",
        "input_schema": {
            "type": "object",
            "properties": {
                "rcept_no": {"type": "string",
                             "description": "공시 접수번호 14자리 (get_dart_disclosures의 rcept_no)"},
            },
            "required": ["rcept_no"],
        },
    },
    {
        "name": "get_kr_news",
        "description": "한국 종목/이슈 뉴스 — 네이버 금융 종목별 뉴스(6자리 코드 기반, 가장 풍부·정확) "
                       "+ 한국 바이오 전문매체(히트뉴스·팜뉴스·청년의사·더바이오). "
                       "**한국 종목/이슈 뉴스는 영문 소스(Finviz/Yahoo) 대신 반드시 이걸 사용.** "
                       "한국 상장 종목이면 ticker(6자리), 자유 키워드면 query.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string", "description": "6자리 한국 종목코드(선택)"},
                "query": {"type": "string", "description": "한국어 키워드(선택)"},
                "limit": {"type": "integer", "default": 15},
            },
        },
    },
    {
        "name": "read_naver_blog",
        "description": "네이버 블로그 글 읽기 — RSS로 글 목록 + 전체 본문(모바일) 회수. "
                       "사용자가 블로그 URL/ID를 주거나 '이 블로그 읽어줘/분석해줘' 할 때 사용 "
                       "(애널리스트 바이오 블로그 등). blog=URL 또는 ID, query=제목 키워드 필터(선택), "
                       "limit=본문 가져올 글 수(기본 4, 최대 8).",
        "input_schema": {
            "type": "object",
            "properties": {
                "blog": {"type": "string", "description": "네이버 블로그 URL 또는 ID (예: mljys10)"},
                "query": {"type": "string", "description": "제목 필터 키워드(선택)"},
                "limit": {"type": "integer", "default": 4},
            },
            "required": ["blog"],
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
        "name": "discover_catalysts_via_ai",
        "description": "Actively research ticker's upcoming 12-month catalysts using Claude "
                       "tool calling — picks up Investor Days, accelerated CVOT readouts, "
                       "KOL events, recent guidance changes that static sources miss. "
                       "Saves discovered items to catalysts table. 1-3 min. Use when user "
                       "asks to find missing catalysts or when default get_catalysts looks sparse.",
        "input_schema": {
            "type": "object",
            "properties": {"ticker": {"type": "string"}},
            "required": ["ticker"],
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
        "name": "get_new_today_highs",
        "description": "Today's newly-broken 52w high biotech tickers. Returns "
                       "ticker/name/close/perf_1d/market_cap. Combine with "
                       "generate_investment_report to deliver list + per-ticker memos.",
        "input_schema": {
            "type": "object",
            "properties": {"limit": {"type": "integer", "default": 100}},
        },
    },
    {
        "name": "generate_investment_report",
        "description": "Generate an institutional-quality (Goldman/MS-style) investment memo "
                       "for a ticker. Includes: thesis, recent price action + drivers, "
                       "catalyst watch list, insider signal, risk points, bottom line. "
                       "Uses all available data — ticker_master, high_low_cache, catalysts, "
                       "insider_trades, fundamental news, pipeline mentions. "
                       "Trigger when user says 'X 리포트', 'X 투자 메모', 'X analyze', etc. "
                       "If user wants the memo as a PDF, use send_thesis_pdf instead.",
        "input_schema": {
            "type": "object",
            "properties": {"ticker": {"type": "string"}},
            "required": ["ticker"],
        },
    },
    {
        "name": "send_thesis_pdf",
        "description": "Build the investment memo as a PDF and send it to the user's Telegram "
                       "as a document attachment with TL;DR caption. Use whenever the user "
                       "asks for 'X PDF', 'X thesis PDF로 만들어줘', 'X 메모 파일로', "
                       "'send X report as PDF', etc. Uses cached report if available; pass "
                       "refresh=true to force fresh deep research first.",
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
    {
        "name": "search_europepmc",
        "description": "Search Europe PMC biomedical literature — peer-reviewed papers, "
                       "preprints, AND conference abstracts in one query. Broader than PubMed "
                       "(adds European journals + preprints). Use for ANY topic: drug "
                       "mechanism/MOA, modality, indication, target biology — works for "
                       "NON-LISTED / private companies too (it is literature, not tickers). "
                       "Follow up with fetch_url for specific numbers (%, n, p-value).",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string",
                          "description": "drug/target/mechanism/modality/indication keywords"},
                "max_results": {"type": "integer", "default": 6},
            },
            "required": ["query"],
        },
    },
    {
        "name": "search_preprints",
        "description": "Search bioRxiv/medRxiv preprints (via Europe PMC). Latest, NOT-yet "
                       "peer-reviewed research — track emerging targets/mechanisms. Always "
                       "state in your answer that preprints are unreviewed.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "max_results": {"type": "integer", "default": 6},
            },
            "required": ["query"],
        },
    },
    {
        "name": "search_conference_abstracts",
        "description": "Search medical CONGRESS abstracts — ASCO, AACR, ESMO, ASH, EASL, ADA, "
                       "etc. (indexed via JCO / Cancer Research supplements in Europe PMC). "
                       "Use for 'ASCO 2026 X data', 'AACR abstract on Y'. Pass the society "
                       "abbreviation for precision. If sparse, also try search_news_by_query "
                       "+ fetch_url on the abstract URL.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "drug/target/indication"},
                "society": {"type": "string",
                            "description": "society abbreviation, e.g. ASCO, AACR, ESMO, ASH"},
                "max_results": {"type": "integer", "default": 6},
            },
            "required": ["query"],
        },
    },
    {
        "name": "send_chart",
        "description": "Render a CANDLESTICK chart (default 2-year DAILY, with MAs + volume) "
                       "and SEND it as an image to Telegram. Use when the user asks to see a "
                       "chart ('X 차트 보여줘', 'show me X chart'). OHLCV via Toss/DB cache. "
                       "After calling, reply with one short line (chart sent).",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "period": {"type": "string",
                           "description": "6m/1y/2y/3y/5y (default 2y). 3y이상 주봉, 이하 일봉 캔들"},
            },
            "required": ["ticker"],
        },
    },
    {
        "name": "send_card",
        "description": "Send a single COMBINED card for a ticker as ONE Telegram message: "
                       "2y daily candlestick chart + market cap/price/1D·1M·1Y returns + "
                       "2 curated price-moving news links — all in one message. Use when the "
                       "user asks for a stock 'card' or 'show me X' (overview). For chart-only, "
                       "use send_chart instead. Reply with one short line after.",
        "input_schema": {
            "type": "object",
            "properties": {"ticker": {"type": "string"}},
            "required": ["ticker"],
        },
    },
    {
        "name": "export_pdf",
        "description": "Compile a one-pager PDF from markdown YOU write and SEND it to Telegram. "
                       "Use when the user asks to '원페이저/PDF로 뽑아줘', 'summarize our DFTX "
                       "discussion into a PDF', etc. FIRST write the full summary/synthesis "
                       "yourself (markdown: headings, bullets, tables ok), THEN call this with "
                       "that markdown. Distinct from generate_investment_report (that builds a "
                       "fresh stock memo); export_pdf packages content you authored.",
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "문서 제목 (예: 'DFTX 논의 요약')"},
                "markdown": {"type": "string",
                             "description": "PDF 본문 — 모델이 작성한 요약/정리 markdown"},
            },
            "required": ["title", "markdown"],
        },
    },
    {
        "name": "send_text_telegram",
        "description": "Send arbitrary text (a summary/answer YOU wrote) to Telegram as a "
                       "message. Use when the user asks '이거 텔레그램으로 보내줘', 'send that "
                       "to telegram'. For a PDF document instead, use export_pdf.",
        "input_schema": {
            "type": "object",
            "properties": {"text": {"type": "string"}},
            "required": ["text"],
        },
    },
    {
        "name": "count_52w_highs",
        "description": "Return the NUMBER (count) of biotech 52-week-high stocks (mcap≥$1.5B) "
                       "on a given date. Use for '신고가 종목 *수*', 'how MANY at 52w high', "
                       "'X일 대비 얼마나 늘었나' — call once per date and compare. This is a "
                       "COUNT, not a list (use get_new_today_highs for the list). Data range "
                       "is limited to when collection started (~2026-05-07).",
        "input_schema": {
            "type": "object",
            "properties": {
                "date": {"type": "string",
                         "description": "YYYY-MM-DD (생략 시 최신 수집일)"},
            },
            "required": [],
        },
    },
]


def get_dart_disclosures(ticker: str, days: int = 30, types: str = None):
    """한국 종목 공시 — **네이버 실시간 종목공시 + DART 전자공시 병합**.
    당일 막 올라온 유상증자·주요사항·거래소 공시도 포함(DART OpenAPI는 인덱싱 지연이 있어
    네이버가 먼저 잡음). 미국·비상장은 빈 결과."""
    import re as _re

    def _rcpno(u):     # DART/네이버 링크에서 접수번호(14자리) 추출 → get_dart_document용
        m = _re.search(r"(?:rcpNo|rcept_no)=(\d{14})", u or "")
        return m.group(1) if m else ""

    out = []
    # 1) 네이버 종목 공시 — 실시간(당일 공시 포함)
    try:
        import kr_news
        for d in kr_news.naver_disclosures(ticker, limit=20):
            out.append({"date": (d.get("published") or "").replace(".", "-"),
                        "title": d["title"], "url": d["link"],
                        "rcept_no": _rcpno(d.get("link")), "source": "네이버공시"})
    except Exception:
        pass
    # 2) DART 전자공시 — 상세/뷰어 링크(지연 가능)
    try:
        import dart
        if dart.available():
            for d in dart.recent_disclosures(ticker, days=days, types=types, limit=30):
                out.append({"date": d["date"], "title": d["title"], "url": d["url"],
                            "rcept_no": d.get("rcept_no", ""),
                            "filer": d.get("filer", ""), "source": "DART"})
        elif not out:
            return {"error": "DART_API_KEY 미설정 — 네이버 공시도 미수집"}
    except Exception as e:
        if not out:
            return {"error": f"{type(e).__name__}: {e}"}
    if not out:
        return {"ticker": ticker, "disclosures": [], "note": "공시 없음 또는 비상장/미국"}
    seen, ded = set(), []
    for d in sorted(out, key=lambda x: x.get("date", ""), reverse=True):
        k = "".join((d.get("title") or "").split())[:40]
        if k and k not in seen:
            seen.add(k)
            ded.append(d)
    return {"ticker": ticker, "disclosures": ded}


def get_dart_document(rcept_no: str):
    """DART 공시 '원문 본문 텍스트' — 공식 document.xml API로 직접 받음.
    (뷰어는 본문이 iframe이라 스크랩 불가 → 반드시 이걸로 본문을 읽는다.)
    rcept_no는 get_dart_disclosures 결과의 rcept_no 필드. 당일(막 올라온) 공시는
    DART 인덱싱 지연으로 014(원본없음)가 날 수 있음 → 잠시 후 재시도/메타데이터로 대체."""
    try:
        import dart
        if not dart.available():
            return {"error": "DART_API_KEY 미설정"}
        return dart.fetch_document(str(rcept_no or "").strip())
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


def get_kr_news(ticker: str = "", query: str = "", limit: int = 15):
    """한국 종목/이슈 뉴스 — 네이버 금융 종목뉴스(6자리 코드 기반, 가장 풍부·정확) +
    한국 바이오 전문매체(히트뉴스·팜뉴스·청년의사·더바이오). 한국 건은 영문 소스 대신 이걸 사용."""
    try:
        import kr_news
        out, t = [], str(ticker or "").strip()
        if t.isdigit() and len(t) == 6:
            out += kr_news.naver_finance_news(t, limit=limit)
        if query and query.strip():
            out += kr_news.for_query(query, limit=10, days=30)
        elif t.isdigit() and len(t) == 6:
            try:
                from db import connect
                with connect() as c:
                    row = c.execute(
                        "SELECT name FROM ticker_master WHERE ticker=?", (t,)).fetchone()
                if row and row["name"]:
                    out += kr_news.for_query(row["name"], limit=8, days=30)
            except Exception:
                pass
        seen, ded = set(), []
        for it in out:
            k = (it.get("title") or "")[:50]
            if k and k not in seen:
                seen.add(k)
                ded.append({"title": it.get("title"), "source": it.get("source"),
                            "date": it.get("published"), "link": it.get("link")})
        return {"news": ded[:limit]} if ded else {"note": "한국 뉴스 없음"}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


def read_naver_blog(blog: str, query: str = "", limit: int = 4):
    """네이버 블로그 글 읽기 — RSS 목록 + 전체 본문(모바일). 바이오 애널리스트 블로그 등.
    blog: 블로그 URL 또는 ID. query: 제목 필터(선택). limit: 본문 가져올 글 수."""
    try:
        import kr_news
        posts = kr_news.naver_blog_posts(blog, limit=30)
        if not posts:
            return {"note": "블로그 글 없음 또는 RSS 접근 실패"}
        if query and query.strip():
            toks = [t for t in query.lower().split() if len(t) >= 2]
            filt = [p for p in posts if any(t in p["title"].lower() for t in toks)]
            posts = filt or posts
        out = []
        for p in posts[:max(1, min(limit, 8))]:
            body = kr_news.naver_blog_body(blog, p["log_no"]) if p.get("log_no") else ""
            out.append({"title": p["title"], "link": p["link"],
                        "body": body or p.get("preview", "")})
        return {"posts": out, "total_listed": len(posts)}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


def run_tool(name: str, args: dict):
    """Tool name으로 디스패치."""
    funcs = {
        # 조회
        "search_clinicaltrials": search_clinicaltrials,
        "search_pubmed": search_pubmed,
        "search_europepmc": search_europepmc,
        "search_preprints": search_preprints,
        "search_conference_abstracts": search_conference_abstracts,
        "get_ticker_info": get_ticker_info,
        "get_valuation_metrics": get_valuation_metrics,
        "get_memos_for": get_memos_for,
        "get_drug_moa": get_drug_moa,
        "fetch_recent_news_for": fetch_recent_news_for,
        "get_pipeline_info": get_pipeline_info,
        "search_news_by_query": search_news_by_query,
        "fetch_url": fetch_url,
        "get_realtime_quote": get_realtime_quote,
        "get_premarket_movers": get_premarket_movers,
        "get_market_movers": get_market_movers,
        "get_dart_disclosures": get_dart_disclosures,
        "get_dart_document": get_dart_document,
        "get_kr_news": get_kr_news,
        "read_naver_blog": read_naver_blog,
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
        "portfolio_detail": portfolio_detail,
        "excluded_add": excluded_add,
        "create_price_trigger": create_price_trigger,
        "list_price_triggers": list_price_triggers,
        "cancel_price_trigger": cancel_price_trigger,
        "check_price_triggers_now": check_price_triggers_now,
        # 카탈리스트 / 인사이더
        "get_catalysts": get_catalysts,
        "get_upcoming_pdufa": get_upcoming_pdufa,
        "get_upcoming_conferences": get_upcoming_conferences,
        "refresh_catalysts": refresh_catalysts,
        "discover_catalysts_via_ai": discover_catalysts_via_ai,
        "get_insider_trades": get_insider_trades,
        "get_ir_milestones": get_ir_milestones,
        "get_earnings_call_milestones": get_earnings_call_milestones,
        "generate_investment_report": generate_investment_report,
        "send_thesis_pdf": send_thesis_pdf,
        "send_chart": send_chart,
        "send_card": send_card,
        "export_pdf": export_pdf,
        "send_text_telegram": send_text_telegram,
        "get_new_today_highs": get_new_today_highs,
        "count_52w_highs": count_52w_highs,
        "search_company_milestones": search_company_milestones,
    }
    f = funcs.get(name)
    if not f:
        return {"error": f"unknown tool {name}"}
    try:
        return f(**args)
    except Exception as e:
        return {"error": str(e)}
