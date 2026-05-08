"""카탈리스트 캘린더 — PDUFA, 학회, 어닝, 임상 완료일 통합.

소스
- BioPharma Catalyst (FDA calendar 페이지 스크래핑) — PDUFA, AdCom
- 하드코딩 학회 일정 (2026) — ASCO, ASH, EASL, ADA 등 30+ 학회
- yfinance — 다음 어닝 일정
- ClinicalTrials.gov — primary completion date = 임상 데이터 공개 시점 proxy (interim 포함)
- Finviz quote → Yahoo Finance 어닝콜 요약 → forward-looking 멘션 추출 (earnings_calls.py)

캐시: catalysts 테이블 (UNIQUE 제약으로 중복 방지)
"""
from __future__ import annotations

import datetime as dt
import logging
import re
from typing import Iterable

import pandas as pd

import db

log = logging.getLogger(__name__)


# ─────────────────────── 2026 의학 학회 일정 ───────────────────────
# 주요 학회 — 매년 거의 같은 시기. 2026 확정 일정으로 입력.
# (대략 일정도 포함 — 정확 일자 미확정시 월 중순으로 placeholder)
CONFERENCES_2026 = [
    # ─── Oncology ───
    {"name": "ASCO GI Cancers Symposium", "start": "2026-01-22", "end": "2026-01-24",
     "area": "oncology", "city": "San Francisco"},
    {"name": "ASCO GU Cancers Symposium", "start": "2026-02-12", "end": "2026-02-14",
     "area": "oncology", "city": "San Francisco"},
    {"name": "AACR Annual Meeting", "start": "2026-04-17", "end": "2026-04-22",
     "area": "oncology", "city": "Chicago"},
    {"name": "ASCO Annual Meeting", "start": "2026-05-29", "end": "2026-06-02",
     "area": "oncology", "city": "Chicago"},
    {"name": "WCLC (World Conference on Lung Cancer)", "start": "2026-09-05",
     "end": "2026-09-08", "area": "oncology", "city": "Vienna"},
    {"name": "ESMO Congress", "start": "2026-10-16", "end": "2026-10-20",
     "area": "oncology", "city": "Madrid"},
    {"name": "SITC Annual Meeting", "start": "2026-11-04", "end": "2026-11-08",
     "area": "immuno-oncology", "city": "Houston"},
    {"name": "SABCS (San Antonio Breast Cancer)", "start": "2026-12-08",
     "end": "2026-12-11", "area": "oncology-breast", "city": "San Antonio"},
    {"name": "ASH Annual Meeting", "start": "2026-12-05", "end": "2026-12-08",
     "area": "hematology", "city": "Orlando"},
    {"name": "ESMO Asia", "start": "2026-12-04", "end": "2026-12-06",
     "area": "oncology", "city": "Singapore"},

    # ─── Hepatology / GI ───
    {"name": "EASL Congress", "start": "2026-06-10", "end": "2026-06-13",
     "area": "hepatology", "city": "Paris"},
    {"name": "AASLD The Liver Meeting", "start": "2026-11-06", "end": "2026-11-10",
     "area": "hepatology", "city": "Boston"},
    {"name": "DDW (Digestive Disease Week)", "start": "2026-05-02",
     "end": "2026-05-05", "area": "gi", "city": "Chicago"},
    {"name": "UEG Week", "start": "2026-10-10", "end": "2026-10-14",
     "area": "gi", "city": "Berlin"},

    # ─── Diabetes / Endocrine / Obesity ───
    {"name": "ADA Scientific Sessions", "start": "2026-06-19", "end": "2026-06-22",
     "area": "diabetes-obesity", "city": "Chicago"},
    {"name": "EASD Annual Meeting", "start": "2026-09-14", "end": "2026-09-18",
     "area": "diabetes", "city": "Vienna"},
    {"name": "ENDO (Endocrine Society)", "start": "2026-06-13", "end": "2026-06-16",
     "area": "endocrine", "city": "Boston"},
    {"name": "ObesityWeek", "start": "2026-11-01", "end": "2026-11-05",
     "area": "obesity", "city": "Atlanta"},

    # ─── Cardiology ───
    {"name": "ACC Scientific Sessions", "start": "2026-03-28", "end": "2026-03-30",
     "area": "cardiology", "city": "Atlanta"},
    {"name": "ESC Congress", "start": "2026-08-29", "end": "2026-09-01",
     "area": "cardiology", "city": "Madrid"},
    {"name": "AHA Scientific Sessions", "start": "2026-11-07", "end": "2026-11-10",
     "area": "cardiology", "city": "New Orleans"},
    {"name": "TCT (Transcatheter Cardio Therapeutics)", "start": "2026-10-23",
     "end": "2026-10-26", "area": "cardio-interventional", "city": "Washington DC"},

    # ─── Neurology / Psychiatry ───
    {"name": "AAN Annual Meeting", "start": "2026-04-25", "end": "2026-05-01",
     "area": "neurology", "city": "San Diego"},
    {"name": "AD/PD Conference", "start": "2026-04-07", "end": "2026-04-11",
     "area": "neurodegen", "city": "Vienna"},
    {"name": "CTAD (Clinical Trials on Alzheimer's)", "start": "2026-12-01",
     "end": "2026-12-04", "area": "neurodegen", "city": "Madrid"},
    {"name": "AES (American Epilepsy Society)", "start": "2026-12-04",
     "end": "2026-12-08", "area": "neurology-epilepsy", "city": "Boston"},
    {"name": "MDS Congress (Movement Disorders)", "start": "2026-09-12",
     "end": "2026-09-16", "area": "neurology-movement", "city": "Vienna"},
    {"name": "APA Annual Meeting", "start": "2026-05-16", "end": "2026-05-20",
     "area": "psychiatry", "city": "Boston"},
    {"name": "ECNP Congress", "start": "2026-10-10", "end": "2026-10-13",
     "area": "psychiatry", "city": "Amsterdam"},

    # ─── Rheumatology / Immunology ───
    {"name": "EULAR Congress", "start": "2026-06-10", "end": "2026-06-13",
     "area": "rheumatology", "city": "Barcelona"},
    {"name": "ACR Convergence", "start": "2026-10-30", "end": "2026-11-04",
     "area": "rheumatology", "city": "Chicago"},
    {"name": "AAAAI Annual Meeting", "start": "2026-02-27", "end": "2026-03-02",
     "area": "allergy-immunology", "city": "San Antonio"},

    # ─── Pulmonology ───
    {"name": "ATS International Conference", "start": "2026-05-15",
     "end": "2026-05-20", "area": "pulmonology", "city": "Boston"},
    {"name": "ERS Congress", "start": "2026-09-12", "end": "2026-09-16",
     "area": "pulmonology", "city": "Amsterdam"},

    # ─── Nephrology ───
    {"name": "ASN Kidney Week", "start": "2026-11-04", "end": "2026-11-08",
     "area": "nephrology", "city": "San Diego"},
    {"name": "ERA Congress (European Renal)", "start": "2026-05-21",
     "end": "2026-05-24", "area": "nephrology", "city": "Vienna"},

    # ─── Dermatology ───
    {"name": "AAD Annual Meeting", "start": "2026-03-20", "end": "2026-03-24",
     "area": "dermatology", "city": "Orlando"},
    {"name": "EADV Congress", "start": "2026-09-30", "end": "2026-10-03",
     "area": "dermatology", "city": "Vienna"},

    # ─── Ophthalmology ───
    {"name": "ARVO Annual Meeting", "start": "2026-05-03", "end": "2026-05-07",
     "area": "ophthalmology", "city": "Salt Lake City"},
    {"name": "AAO Annual Meeting", "start": "2026-10-17", "end": "2026-10-20",
     "area": "ophthalmology", "city": "Chicago"},

    # ─── Gene/Cell Therapy & Rare Disease ───
    {"name": "ASGCT Annual Meeting", "start": "2026-05-12", "end": "2026-05-16",
     "area": "gene-cell-therapy", "city": "New Orleans"},
    {"name": "WORLDSymposium", "start": "2026-02-09", "end": "2026-02-13",
     "area": "rare-lsd", "city": "San Diego"},

    # ─── Infectious Disease / Vaccine ───
    {"name": "IDWeek", "start": "2026-10-14", "end": "2026-10-18",
     "area": "infectious-disease", "city": "Atlanta"},
    {"name": "ECCMID (European Microbiology)", "start": "2026-04-18",
     "end": "2026-04-21", "area": "infectious-disease", "city": "Vienna"},
    {"name": "CROI", "start": "2026-02-22", "end": "2026-02-26",
     "area": "hiv-retrovirus", "city": "Boston"},

    # ─── Investor Conferences (지수 임팩트 큰 것들) ───
    {"name": "JPMorgan Healthcare Conference", "start": "2026-01-12",
     "end": "2026-01-15", "area": "investor", "city": "San Francisco"},
    {"name": "BIO International Convention", "start": "2026-06-08",
     "end": "2026-06-11", "area": "investor", "city": "Boston"},
    {"name": "Wells Fargo Healthcare Conference", "start": "2026-09-09",
     "end": "2026-09-11", "area": "investor", "city": "Boston"},

    # ─── Bone / Other ───
    {"name": "ASBMR Annual Meeting", "start": "2026-09-11", "end": "2026-09-14",
     "area": "bone", "city": "Phoenix"},
]


# ─────────────────────── BioPharma Catalyst (FDA) ───────────────────────
def fetch_pdufa_calendar() -> list[dict]:
    """biopharmcatalyst.com FDA 캘린더 스크래핑.
    PDUFA + AdCom + Major Catalyst 일정 반환 — 무료 페이지에 공개됨."""
    from curl_cffi import requests as crq
    from bs4 import BeautifulSoup
    url = "https://www.biopharmcatalyst.com/calendars/fda-calendar"
    try:
        r = crq.get(url, impersonate="chrome", timeout=20)
        r.raise_for_status()
    except Exception as e:
        log.warning("biopharmcatalyst fetch 실패: %s", e)
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    out: list[dict] = []
    # 페이지 구조: <table> 또는 catalysts list. 두 패턴 시도
    rows = soup.select("table tr")
    for tr in rows:
        cells = tr.find_all("td")
        if len(cells) < 4:
            continue
        # 보통 [Date, Ticker/Drug, Stage, Catalyst Type] 형태
        text_cells = [c.get_text(" ", strip=True) for c in cells]
        # date는 ISO 또는 "Mar 15, 2026" 형태
        date_str = _normalize_date(text_cells[0])
        if not date_str:
            continue
        # 티커는 a 링크 안에 있는 경우 많음
        ticker = ""
        ticker_a = tr.find("a", href=re.compile(r"/(?:companies|stocks)/", re.I))
        if ticker_a:
            ticker = ticker_a.get_text(strip=True).upper()
        # 두 번째 시도 — text에 (TICKER) 패턴
        if not ticker:
            for t in text_cells[1:3]:
                m = re.search(r"\(([A-Z]{2,5})\)", t)
                if m:
                    ticker = m.group(1)
                    break
        title = " | ".join(text_cells[1:])[:300]
        out.append({
            "ticker": ticker or None,
            "event_date": date_str,
            "event_type": "pdufa",
            "title": title,
            "description": title,
            "source": "biopharmcatalyst",
        })
    log.info("biopharmcatalyst: %d 건", len(out))
    return out


def _normalize_date(s: str) -> str:
    s = s.strip()
    if not s:
        return ""
    # ISO
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    # "Mar 15, 2026" / "March 15, 2026"
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return dt.datetime.strptime(s[:25].strip(), fmt).date().isoformat()
        except ValueError:
            continue
    # "Q2 2026" 같은 분기는 분기 마지막 날로
    qm = re.match(r"Q([1-4])\s*(\d{4})", s)
    if qm:
        q, y = int(qm.group(1)), int(qm.group(2))
        end_month = q * 3
        end_day = 31 if end_month in (3, 12) else 30
        return f"{y}-{end_month:02d}-{end_day:02d}"
    return ""


# ─────────────────────── 어닝 일정 (yfinance) ───────────────────────
def fetch_earnings_dates(tickers: Iterable[str]) -> list[dict]:
    import yfinance as yf
    out: list[dict] = []
    for t in tickers:
        try:
            tk = yf.Ticker(t)
            cal = None
            try:
                cal = tk.calendar
            except Exception:
                pass
            edate: dt.date | None = None
            if isinstance(cal, dict):
                ed = cal.get("Earnings Date")
                if isinstance(ed, list) and ed:
                    edate = ed[0] if isinstance(ed[0], dt.date) else None
                elif isinstance(ed, dt.date):
                    edate = ed
            elif cal is not None and hasattr(cal, "loc"):
                # DataFrame 형태
                try:
                    edate = cal.loc["Earnings Date"][0]
                except Exception:
                    pass
            if edate and isinstance(edate, dt.date):
                out.append({
                    "ticker": t.upper(),
                    "event_date": edate.isoformat(),
                    "event_type": "earnings",
                    "title": f"{t.upper()} Earnings",
                    "source": "yfinance",
                })
        except Exception as e:
            log.debug("earnings fetch %s: %s", t, e)
    return out


# ─────────────────────── ClinicalTrials.gov primary completion ───────────────────────
def fetch_clinical_completions(tickers: Iterable[str], days_ahead: int = 365) -> list[dict]:
    """watchlist 종목의 임상 primary completion date — 향후 N일 이내."""
    import requests
    today = dt.date.today()
    horizon = today + dt.timedelta(days=days_ahead)
    out: list[dict] = []
    for t in tickers:
        # 회사명 mapping은 ticker_master에서. 없으면 ticker로 검색
        name = _company_name(t) or t
        try:
            r = requests.get(
                "https://clinicaltrials.gov/api/v2/studies",
                params={
                    "query.spons": name,
                    "filter.overallStatus": "RECRUITING,ACTIVE_NOT_RECRUITING",
                    "fields": ("NCTId,BriefTitle,Phase,PrimaryCompletionDate,"
                               "OverallStatus,LeadSponsorName"),
                    "pageSize": 30,
                },
                timeout=15,
            )
            r.raise_for_status()
            studies = r.json().get("studies", [])
        except Exception as e:
            log.debug("clinicaltrials %s: %s", t, e)
            continue
        for s in studies:
            proto = s.get("protocolSection", {})
            comp = (proto.get("statusModule", {})
                    .get("primaryCompletionDateStruct", {}).get("date") or "")
            if not comp:
                continue
            comp_iso = _normalize_date(comp)
            if not comp_iso:
                continue
            try:
                cdate = dt.date.fromisoformat(comp_iso)
            except ValueError:
                continue
            if cdate < today or cdate > horizon:
                continue
            phase_list = proto.get("designModule", {}).get("phases") or []
            phase = "/".join(phase_list) if phase_list else ""
            title = (proto.get("identificationModule", {}).get("briefTitle") or "")[:200]
            nct = proto.get("identificationModule", {}).get("nctId", "")
            out.append({
                "ticker": t.upper(),
                "event_date": comp_iso,
                "event_type": "clinical_readout",
                "title": f"[{phase}] {title}" if phase else title,
                "description": f"NCT: {nct}",
                "source": "clinicaltrials",
            })
    return out


def _company_name(ticker: str) -> str:
    try:
        with db.connect() as conn:
            row = conn.execute(
                "SELECT name FROM ticker_master WHERE ticker = ?", (ticker.upper(),)
            ).fetchone()
            return (row.get("name") if row else "") or ""
    except Exception:
        return ""


# ─────────────────────── 통합 refresh ───────────────────────
def _scope_tickers(scope: str) -> list[str]:
    """scope에 맞는 ticker 리스트 반환.
    scope='watchlist' | 'biotech_1b' | 'all_tracked'."""
    with db.connect() as conn:
        if scope == "watchlist":
            rows = conn.execute("SELECT ticker FROM watchlist").fetchall()
            return [r["ticker"] for r in rows]
        if scope == "biotech_1b":
            # ≥$1B Healthcare. 1500=$1.5B; user 요청대로 1000=$1B 기준.
            rows = conn.execute(
                "SELECT ticker FROM ticker_master "
                "WHERE sector='Healthcare' AND market_cap >= 1000 "
                "AND country='USA' "
                "AND ticker NOT IN (SELECT ticker FROM excluded_tickers)"
            ).fetchall()
            return [r["ticker"] for r in rows]
        # all_tracked
        rows = conn.execute("SELECT ticker FROM ticker_master").fetchall()
        return [r["ticker"] for r in rows]


def refresh_all(scope: str = "watchlist",
                include_earnings_calls: bool = True,
                watchlist_only: bool | None = None) -> dict:
    """모든 소스에서 카탈리스트 fetch → DB upsert.
    scope='watchlist' (속도, ~10종목) / 'biotech_1b' (전수, $1B≥, 200-300종목, 30분+) /
    'all_tracked' (전체).
    watchlist_only는 호환용 (deprecated) — True면 scope='watchlist'."""
    if watchlist_only is True:
        scope = "watchlist"
    elif watchlist_only is False and scope == "watchlist":
        scope = "biotech_1b"

    now = dt.datetime.now().isoformat(timespec="seconds")
    counts = {"pdufa": 0, "conference": 0, "earnings": 0,
              "clinical_readout": 0, "earnings_call": 0}

    # 1) 학회 (하드코딩)
    conf_rows = []
    for c in CONFERENCES_2026:
        conf_rows.append({
            "ticker": "",   # NULL은 Postgres UNIQUE 제약에서 distinct 취급되어 중복 생김 → ''
            "event_date": c["start"],
            "event_end_date": c["end"],
            "event_type": "conference",
            "title": c["name"],
            "description": c.get("city", ""),
            "source": "hardcoded",
            "therapy_area": c["area"],
        })

    # 2) PDUFA
    pdufa_rows = fetch_pdufa_calendar()

    # 3) 어닝 + 4) 임상 — scope에 맞는 ticker
    earn_rows: list[dict] = []
    clin_rows: list[dict] = []
    tickers = _scope_tickers(scope)
    log.info("scope=%s, %d tickers", scope, len(tickers))
    if tickers:
        earn_rows = fetch_earnings_dates(tickers)
        clin_rows = fetch_clinical_completions(tickers)

    all_rows = conf_rows + pdufa_rows + earn_rows + clin_rows
    counts["conference"] = len(conf_rows)
    counts["pdufa"] = len(pdufa_rows)
    counts["earnings"] = len(earn_rows)
    counts["clinical_readout"] = len(clin_rows)

    # 5) Yahoo earnings call summary 수집
    if include_earnings_calls and tickers:
        try:
            from earnings_calls import fetch_for_tickers
            ec_count = fetch_for_tickers(tickers, max_per_ticker=3)
            counts["earnings_call"] = ec_count
        except Exception as e:
            log.warning("earnings_calls fetch 실패: %s", e)

    # upsert (UNIQUE 제약으로 중복 무시)
    with db.connect() as conn:
        for r in all_rows:
            try:
                conn.execute(
                    "INSERT INTO catalysts (ticker, event_date, event_end_date, "
                    "event_type, title, description, source, therapy_area, fetched_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT (ticker, event_date, event_type, title) DO NOTHING",
                    (r.get("ticker"), r.get("event_date"), r.get("event_end_date"),
                     r.get("event_type"), r.get("title"), r.get("description"),
                     r.get("source"), r.get("therapy_area"), now),
                )
            except Exception as e:
                log.debug("upsert 실패: %s — %s", r.get("title"), e)
    log.info("catalysts refreshed: %s", counts)
    return counts


# ─────────────────────── 조회 ───────────────────────
def get_catalysts(ticker: str | None = None, days: int = 90,
                  event_types: list[str] | None = None) -> pd.DataFrame:
    """오늘부터 N일 이내 카탈리스트.
    ticker=None이면 전체 (학회 포함). 특정 ticker면 해당 종목 + 학회(area 필터링은 별도).
    event_types로 ['pdufa','conference','earnings','clinical_completion'] 필터.
    """
    today = dt.date.today().isoformat()
    horizon = (dt.date.today() + dt.timedelta(days=days)).isoformat()
    where = ["event_date >= ?", "event_date <= ?"]
    params: list = [today, horizon]
    if ticker:
        # ticker별 이벤트 + (선택) 학회 (NULL ticker)는 제외 — UI에서 따로
        where.append("ticker = ?")
        params.append(ticker.upper())
    if event_types:
        ph = ",".join("?" * len(event_types))
        where.append(f"event_type IN ({ph})")
        params.extend(event_types)
    sql = (f"SELECT * FROM catalysts WHERE {' AND '.join(where)} "
           f"ORDER BY event_date ASC")
    return db.pd_read_sql(sql, params=tuple(params))


def get_upcoming_pdufa(days: int = 90) -> pd.DataFrame:
    return get_catalysts(days=days, event_types=["pdufa"])


def get_upcoming_conferences(days: int = 90, area: str | None = None) -> pd.DataFrame:
    df = get_catalysts(days=days, event_types=["conference"])
    if area and not df.empty:
        df = df[df["therapy_area"].str.contains(area, case=False, na=False)]
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(refresh_all(watchlist_only=True))
