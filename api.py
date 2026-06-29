"""FastAPI 백엔드 — 기존 Python 로직(수집·AI 챗·차트·뉴스·카탈리스트·포트폴리오)을
그대로 재사용해 REST로 노출. Streamlit app.py / telegram_bot.py 는 건드리지 않는다.

실행:  ./.venv/Scripts/python.exe -m uvicorn api:app --reload --port 8000
"""
from __future__ import annotations

import math
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="Biotech Radar API")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


# ── 인증 게이트 ── APP_PASSWORD가 설정돼 있으면 HTTP Basic Auth로 전체 보호.
# (공개 배포 시 /api/chat·/api/reason 무단 호출 = Anthropic 과금/데이터 노출 방지)
# 미설정(로컬 dev)이면 무인증. 브라우저가 401→로그인 프롬프트→자격증명 캐시를 자동 처리.
@app.middleware("http")
async def _basic_auth(request, call_next):
    import base64
    import os as _os
    import secrets as _secrets
    from starlette.responses import Response

    pw = (_os.environ.get("APP_PASSWORD") or "").strip()
    # CORS preflight + 헬스체크(/healthz)는 무인증 통과 (Render 헬스체크가 401 받으면
    # 서비스가 영원히 'almost live'에 멈춤)
    if pw and request.method != "OPTIONS" and request.url.path != "/healthz":
        hdr = request.headers.get("authorization", "")
        ok = False
        if hdr.startswith("Basic "):
            try:
                supplied = base64.b64decode(hdr[6:]).decode("utf-8").partition(":")[2]
                ok = _secrets.compare_digest(supplied, pw)
            except Exception:
                ok = False
        if not ok:
            return Response(status_code=401,
                            headers={"WWW-Authenticate": 'Basic realm="Biotech Radar"'})
    return await call_next(request)


# 헬스체크 — 무인증, DB 미접근(빠르고 항상 200). Render healthCheckPath 용.
@app.get("/healthz")
def healthz() -> dict:
    return {"ok": True}


# ───────────────────────── helpers ─────────────────────────
def _min_mcap(country: str) -> float:
    if country == "KOR":
        try:
            import kr_universe as ku
            return ku.kr_min_mcap_usd_m()
        except Exception:
            return 324.0
    return 1500.0


def _clean(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            return None
    return v


def _records(df) -> list[dict]:
    if df is None or getattr(df, "empty", True):
        return []
    return [{str(k): _clean(v) for k, v in r.items()} for _, r in df.iterrows()]


def _is_kr(t: str) -> bool:
    t = str(t or "")
    return t.isdigit() and len(t) == 6


# ── 범용 TTL 캐시 (Streamlit st.cache_data 대체) ──
import time as _time
_TTL_CACHE: dict = {}
_DISCOVER_TRIED: set = set()   # discover 1회 시도 가드 (못 찾는 종목 매번 재탐색 방지)


def _cached(key, ttl: float, fn):
    hit = _TTL_CACHE.get(key)
    if hit and (_time.time() - hit[0]) < ttl:
        return hit[1]
    val = fn()
    _TTL_CACHE[key] = (_time.time(), val)
    return val


# ───────────────────────── board ─────────────────────────
@app.get("/api/health")
def health() -> dict:
    return {"ok": True}


@app.get("/api/board")
def board(country: str = "USA", view: str = "high", limit: int = 300) -> dict:
    from collectors.high_low import (
        fetch_new_highs, fetch_new_today_highs, fetch_top_movers,
    )
    mm = _min_mcap(country)
    try:
        if view == "movers":
            df = fetch_top_movers(limit=limit, min_mcap=mm, min_perf=5.0, country=country)
        elif view == "new":
            df = fetch_new_today_highs(limit=limit, country=country, min_mcap=mm)
        else:
            df = fetch_new_highs("high", limit=limit, country=country, min_mcap=mm)
        return {"rows": _records(df), "country": country, "view": view}
    except Exception as e:
        return {"rows": [], "error": f"{type(e).__name__}: {e}"}


# ───────────────────────── chart ─────────────────────────
@app.get("/api/chart")
def chart(ticker: str, period: str = "6m", interval: str = "1d") -> dict:
    """OHLC + 이동평균. period: 1m/3m/6m/1y/5y, interval: 1d/1wk/1mo."""
    import prices as pr
    try:
        df = pr.fetch_chart(ticker, period, interval)
        if df is None or df.empty:
            return {"dates": [], "error": "no data"}
        df = df.reset_index()
        date_col = df.columns[0]
        dates = [str(d)[:10] for d in df[date_col]]
        def col(name):
            return [None if (isinstance(x, float) and math.isnan(x)) else (round(float(x), 4) if x is not None else None)
                    for x in df[name]] if name in df.columns else None
        return {
            "dates": dates,
            "open": col("Open"), "high": col("High"), "low": col("Low"), "close": col("Close"),
            "ma20": col("MA20"), "ma60": col("MA60"), "ma120": col("MA120"),
        }
    except Exception as e:
        return {"dates": [], "error": f"{type(e).__name__}: {e}"}


# ───────────────────────── stock detail (news/catalysts/ir) ─────────────────────────
def _stock_payload(ticker: str, name: str) -> dict:
    """무거운 조회(뉴스·파이프라인·카탈리스트·어닝콜·IR공개) — 10분 캐시."""
    out: dict = {}
    kr = _is_kr(ticker)
    try:
        if kr:
            import kr_news
            out["news"] = kr_news.naver_finance_news(ticker, 12)
        else:
            import news as _n
            out["news_count"] = _n.news_count(ticker, name, 180)
            out["pipelines"] = _n.top_pipelines(ticker, name, 180)
    except Exception as e:
        out["news_error"] = f"{type(e).__name__}: {e}"
    try:
        import catalysts as cat
        out["catalysts"] = _records(cat.get_catalysts(ticker=ticker, days=365))
    except Exception as e:
        out["catalysts_error"] = f"{type(e).__name__}: {e}"
    try:
        import db as _db
        out["earnings_call"] = _records(_db.pd_read_sql(
            "SELECT * FROM catalysts WHERE ticker=? AND event_type='earnings_call' "
            "ORDER BY event_date ASC", params=(ticker.upper(),)))
    except Exception:
        out["earnings_call"] = []
    try:
        import ir_milestones as irm
        out["company_events"] = _records(irm.get_company_events(ticker))
    except Exception:
        out["company_events"] = []
    return out


def _stock_urls(ticker: str) -> dict:
    """IR/Pipeline URL — 비어있으면 자동 탐색(프로세스당 1회만 시도). 결과는 JSON 영속."""
    import ticker_urls as tu
    try:
        urls = tu.get(ticker)
    except Exception:
        return {}
    if not (urls.get("ir_url") and urls.get("pipeline_url")) and ticker not in _DISCOVER_TRIED:
        _DISCOVER_TRIED.add(ticker)
        try:
            from discover import discover as _disc
            res = _disc(ticker) or {}
            new_ir = urls.get("ir_url") or res.get("ir_url", "")
            new_pl = urls.get("pipeline_url") or res.get("pipeline_url", "")
            if new_ir or new_pl:
                tu.set_urls(ticker, ir_url=new_ir, pipeline_url=new_pl)
                urls = tu.get(ticker)
        except Exception:
            pass
    return urls


@app.get("/api/stock")
def stock(ticker: str, name: str = "") -> dict:
    out = dict(_cached(("stock", ticker, name), 600, lambda: _stock_payload(ticker, name)))
    out["ticker"], out["name"] = ticker, name
    out["urls"] = _stock_urls(ticker)
    try:
        import watchlist as wl
        import excluded as ex
        out["watched"] = wl.is_watched(ticker)     # 토글 반영 위해 항상 최신
        out["excluded"] = ex.is_excluded(ticker)
    except Exception:
        pass
    return out


# ───────────────────────── 이유 분석 ─────────────────────────
class ReasonIn(BaseModel):
    kind: str = "high"          # high | movers
    country: str = "USA"        # USA | KOR
    rows: list[dict]            # [{ticker,name,close,perf_1d,market_cap}]
    generate: bool = True       # False면 캐시만 조회(peek), 생성 안 함


@app.post("/api/reason")
def reason(body: ReasonIn) -> dict:
    """신고가/급등 'AI 상승이유' 마크다운. DB 캐시(reason_cache 테이블) 기반 —
    로컬·클라우드 공유 + 재배포 보존. 유효성은 보드 스냅샷 날짜(latest_run_date) 기준이라
    미장 주말처럼 스냅샷이 안 바뀌는 동안은 그대로 유지. 데일리런이 미리 채워둠."""
    import reason_cache as rc
    from collectors.high_low import latest_run_date
    _country = body.country if body.country in ("USA", "KOR") else None
    snap = latest_run_date(_country) or "_"
    hit = rc.get(body.country, body.kind, snap)
    if hit:                                       # 스냅샷 동일 캐시 → 그대로
        return {"markdown": hit, "cached": True}
    if not body.generate:                         # peek: 캐시만(생성 안 함)
        return {"markdown": None, "cached": False}
    try:
        out = rc.refresh(body.country, body.kind, body.rows, snap)
        if not out:
            return {"markdown": "", "error": "분석 생성 실패(빈 결과 — API 키/크레딧 확인)"}
        return {"markdown": out, "cached": False}
    except Exception as e:
        return {"markdown": "", "error": f"{type(e).__name__}: {e}"}


# ───────────────────────── chat ─────────────────────────
class ChatIn(BaseModel):
    message: str
    history: list[dict] | None = None


@app.post("/api/chat")
def chat(body: ChatIn) -> dict:
    """텔레그램 봇과 동일 엔진(run_agent) + chat_store 공유 (텔레↔웹 대화 공유)."""
    import bot_agent
    import chat_store
    try:
        hist = chat_store.recent(40)            # 공유 히스토리 (텔레 포함), 현재 메시지 추가 전
    except Exception:
        hist = body.history or []
    try:
        chat_store.append("user", body.message, "web")
    except Exception:
        pass
    try:
        text, _ = bot_agent.run_agent(body.message, hist)
    except Exception as e:
        text = f"⚠️ 오류: {type(e).__name__}: {e}"
    try:
        chat_store.append("assistant", text, "web")
    except Exception:
        pass
    return {"reply": text}


@app.get("/api/chat/history")
def chat_history(limit: int = 60) -> dict:
    """텔레↔웹 공유 대화 (source 포함)."""
    import chat_store
    try:
        return {"messages": chat_store.recent_display(limit)}
    except Exception as e:
        return {"messages": [], "error": f"{type(e).__name__}: {e}"}


@app.post("/api/chat/clear")
def chat_clear() -> dict:
    import chat_store
    try:
        chat_store.clear()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


class TgIn(BaseModel):
    text: str


@app.post("/api/chat/telegram")
def chat_to_telegram(body: TgIn) -> dict:
    """챗 답변을 텔레그램으로 전송."""
    import telegram_report as tr
    try:
        tr.send(tr._markdown_to_html(body.text))
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


# ───────────────────────── portfolios ─────────────────────────
@app.get("/api/portfolios")
def portfolios() -> dict:
    import portfolio as pf
    out = []
    for p in pf.list_all():
        try:
            s = pf.summary(p["id"])
            out.append({"id": p["id"], "name": p["name"], "nav": s.get("current_size"),
                        "return_pct": s.get("return_pct"), "holdings": len(s.get("holdings", []))})
        except Exception:
            out.append({"id": p["id"], "name": p["name"]})
    return {"portfolios": out}


def _stooq_series(sym: str):
    """stooq 일별 종가 — 미국 알파 티커용(빠름, 미국 IP)."""
    import io
    import pandas as pd
    import requests
    try:
        r = requests.get(f"https://stooq.com/q/d/l/?s={sym}&i=d", timeout=15,
                         headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200 or not r.text or r.text.lstrip().startswith("<"):
            return None
        d = pd.read_csv(io.StringIO(r.text))
        if "Date" not in d.columns or "Close" not in d.columns or d.empty:
            return None
        s = pd.Series(d["Close"].values, index=pd.to_datetime(d["Date"]).dt.normalize())
        return s.dropna().sort_index()
    except Exception:
        return None


def _pf_close(tk: str, period: str):
    """포트폴리오용 종가 — 미국=stooq(빠름), 그외=fetch_ohlcv 폴백."""
    import pandas as pd
    import prices as pr
    t = (tk or "").strip()
    if t and not (t.isdigit() and len(t) == 6):
        s = _stooq_series(f"{t.lower()}.us")
        if s is not None and not s.empty:
            return s[~s.index.duplicated(keep="last")]
    try:
        df = pr.fetch_ohlcv(t, period, "1d")
    except Exception:
        return None
    if df is None or df.empty or "Close" not in df:
        return None
    s = df["Close"].copy()
    s.index = pd.to_datetime(s.index).normalize()
    return s[~s.index.duplicated(keep="last")].sort_index()


def _yf_period(p: str) -> str:
    return {"3m": "3mo", "6m": "6mo", "1y": "1y", "5y": "5y"}.get(p, "1y")


def _batch_us_closes(tickers: list[str], period: str) -> dict:
    """미국 알파 티커 종가 — yf.download 한 번에 배치(빠름)."""
    import pandas as pd
    import yfinance as yf
    tickers = [t for t in tickers if t]
    if not tickers:
        return {}
    try:
        df = yf.download(tickers, period=_yf_period(period), interval="1d",
                         auto_adjust=True, progress=False, group_by="ticker", threads=True)
    except Exception:
        return {}
    if df is None or df.empty:
        return {}
    multi = isinstance(df.columns, pd.MultiIndex)
    out = {}
    for tk in tickers:
        try:
            sub = df[tk]["Close"] if multi else df["Close"]
            s = sub.dropna()
            s.index = pd.to_datetime(s.index).normalize()
            if not s.empty:
                out[tk] = s[~s.index.duplicated(keep="last")].sort_index()
        except Exception:
            pass
    return out


_PERF_CACHE: dict = {}   # (id, benches) -> (ts, result). 거래 시 무효화.


def _pf_perf(portfolio_id: int, benches: list[str]) -> dict | None:
    """누적수익률(%) 시계열 — 거래내역+종가로 NAV 복원. 배치 yf + 30분 캐시."""
    import time
    import pandas as pd
    import portfolio as pf
    key = (portfolio_id, tuple(benches))
    hit = _PERF_CACHE.get(key)
    if hit and (time.time() - hit[0]) < 1800:
        return hit[1]
    txs = pf._transactions(portfolio_id)
    p = pf.get(portfolio_id)
    if not txs or not p:
        return None
    initial = float(p.get("initial_size") or 0) or 1.0
    start_ts = pd.Timestamp(min(t["trade_date"] for t in txs)[:10])
    days = max(1, (pd.Timestamp.now().normalize() - start_ts).days)
    period = "3m" if days <= 95 else "6m" if days <= 190 else "1y" if days <= 380 else "5y"
    held = sorted({t["ticker"] for t in txs})
    want = list(dict.fromkeys(held + list(benches)))
    us = [t for t in want if not (t.isdigit() and len(t) == 6)]
    kr = [t for t in want if t.isdigit() and len(t) == 6]
    fetched = _batch_us_closes(us, period)
    miss = [t for t in want if t not in fetched]   # 배치 누락분 + KR — 병렬 폴백
    if miss:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(8, len(miss))) as ex:
            for t, s in zip(miss, ex.map(lambda x: _pf_close(x, period), miss)):
                if s is not None and not s.empty:
                    fetched[t] = s
    closes = {tk: fetched[tk] for tk in held if tk in fetched}
    if not closes:
        return None
    idx = None
    for s in closes.values():
        idx = s.index if idx is None else idx.union(s.index)
    idx = idx[idx >= start_ts]
    if len(idx) == 0:
        return None
    txs_s = sorted(txs, key=lambda t: (t["trade_date"], t.get("id", 0)))
    navs = []
    for d in idx:
        sh, cash = {}, initial
        for t in txs_s:
            if pd.Timestamp(t["trade_date"][:10]) > d:
                break
            q, amt = float(t["shares"]), float(t["amount"])
            if t["action"] == "buy":
                sh[t["ticker"]] = sh.get(t["ticker"], 0.0) + q; cash -= amt
            else:
                sh[t["ticker"]] = sh.get(t["ticker"], 0.0) - q; cash += amt
        mv = 0.0
        for tk, q in sh.items():
            if abs(q) < 1e-9:
                continue
            cs = closes.get(tk)
            if cs is None:
                continue
            prior = cs[cs.index <= d]
            if not prior.empty:
                mv += q * float(prior.iloc[-1])
        navs.append(cash + mv)
    series = {"포트폴리오": [round((n / initial - 1) * 100, 2) for n in navs]}
    for b in benches:
        bs = fetched.get(b)
        if bs is None or bs.empty:
            continue
        bs2 = bs.reindex(idx, method="ffill").dropna()
        if bs2.empty:
            continue
        base = float(bs2.iloc[0])
        series[b] = [round((float(bs.reindex([d], method="ffill").iloc[0]) / base - 1) * 100, 2)
                     if d >= bs2.index[0] else None for d in idx]
    result = {"dates": [str(d)[:10] for d in idx], "series": series}
    _PERF_CACHE[key] = (time.time(), result)
    return result


def _invalidate_perf(portfolio_id: int) -> None:
    for k in [k for k in _PERF_CACHE if k[0] == portfolio_id]:
        _PERF_CACHE.pop(k, None)


@app.get("/api/portfolio")
def portfolio_detail(id: int, bench: str = "XBI") -> dict:
    """포트폴리오 요약 + 수익률 시계열(다중 벤치마크 콤마구분)."""
    import portfolio as pf
    try:
        s = pf.summary(id)
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}
    res: dict = {"summary": {k: _clean(v) for k, v in s.items() if k not in ("holdings", "portfolio")},
                 "holdings": [{kk: _clean(vv) for kk, vv in h.items()} for h in s.get("holdings", [])],
                 "name": s.get("portfolio", {}).get("name"),
                 "portfolio": {k: _clean(v) for k, v in (s.get("portfolio") or {}).items()}}
    benches = [b.strip().upper() for b in bench.split(",") if b.strip()]
    try:
        perf = _pf_perf(id, benches)
        if perf:
            res["perf"] = perf
    except Exception as e:
        res["perf_error"] = f"{type(e).__name__}: {e}"
    return res


@app.get("/api/portfolio/{id}/quote")
def portfolio_quote(id: int) -> dict:
    """경량 실시간 시세 — 현재가 기반 요약+보유종목만(수익률 시계열 제외). 폴링용."""
    import portfolio as pf
    try:
        s = pf.summary(id)
        return {"summary": {k: _clean(v) for k, v in s.items() if k not in ("holdings", "portfolio")},
                "holdings": [{kk: _clean(vv) for kk, vv in h.items()} for h in s.get("holdings", [])]}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


@app.get("/api/portfolio/{id}/transactions")
def portfolio_txs(id: int, limit: int = 50) -> dict:
    import portfolio as pf
    try:
        return {"txs": [{k: _clean(v) for k, v in t.items()} for t in pf.transactions_log(id, limit=limit)]}
    except Exception as e:
        return {"txs": [], "error": f"{type(e).__name__}: {e}"}


class HoldingIn(BaseModel):
    ticker: str
    weight: float = 5.0


@app.post("/api/portfolio/{id}/holding")
def portfolio_add_holding(id: int, body: HoldingIn) -> dict:
    import portfolio as pf
    try:
        r = pf.add_holding(id, body.ticker, body.weight)
        _invalidate_perf(id)
        return {"ok": True, "result": {k: _clean(v) for k, v in r.items()}}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


@app.post("/api/portfolio/{id}/weight")
def portfolio_set_weight(id: int, body: HoldingIn) -> dict:
    import portfolio as pf
    try:
        r = pf.set_target_weight(id, body.ticker, body.weight)
        _invalidate_perf(id)
        return {"ok": True, "result": {k: _clean(v) for k, v in r.items()}}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


@app.delete("/api/portfolio/{id}/holding/{ticker}")
def portfolio_sell_all(id: int, ticker: str) -> dict:
    import portfolio as pf
    try:
        r = pf.sell_all(id, ticker)
        _invalidate_perf(id)
        return {"ok": True, "result": {k: _clean(v) for k, v in r.items()}}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


class NewPfIn(BaseModel):
    name: str
    size_m: float = 100.0


@app.post("/api/portfolios")
def portfolio_create(body: NewPfIn) -> dict:
    import portfolio as pf
    try:
        return {"ok": True, "id": pf.create(body.name, initial_size=body.size_m * 1_000_000)}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


@app.delete("/api/portfolios/{id}")
def portfolio_delete(id: int) -> dict:
    import portfolio as pf
    try:
        pf.delete(id)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


# ───────────────────────── daily news / catalysts calendar ─────────────────────────
@app.get("/api/daily_news")
def daily_news(country: str = "USA", days: int = 1) -> dict:
    try:
        if country == "KOR":
            import kr_news
            return {"items": kr_news.latest(60, days)}
        from news import fetch_global_healthcare_news
        return {"items": fetch_global_healthcare_news(days=days, max_items=120)}
    except Exception as e:
        return {"items": [], "error": f"{type(e).__name__}: {e}"}


@app.get("/api/catalysts")
def catalysts_calendar(days: int = 90, types: str = "", scope: str = "all") -> dict:
    """카탈리스트 캘린더. types: 콤마구분 event_type 필터. scope: all|watchlist|biotech_1b."""
    import catalysts as cat
    type_list = [t for t in types.split(",") if t.strip()] or None
    try:
        df = cat.get_catalysts(days=days, event_types=type_list)
    except Exception as e:
        return {"rows": [], "error": f"{type(e).__name__}: {e}"}
    try:
        if not df.empty and scope in ("watchlist", "biotech_1b"):
            sectorwide = df["ticker"].isna() | (df["ticker"] == "")
            if scope == "watchlist":
                import watchlist as wl
                wl_df = wl.list_all()
                keep = set(wl_df["ticker"].tolist()) if not wl_df.empty else set()
            else:
                from db import connect
                with connect() as c:
                    rows = c.execute(
                        "SELECT ticker FROM ticker_master "
                        "WHERE sector='Healthcare' AND market_cap >= 1000 AND country='USA'"
                    ).fetchall()
                keep = {r["ticker"] for r in rows}
            df = df[df["ticker"].isin(keep) | sectorwide]
    except Exception:
        pass
    return {"rows": _records(df)}


class CatToggleIn(BaseModel):
    id: int
    value: bool = True


@app.post("/api/catalysts/watch")
def catalysts_watch(body: CatToggleIn) -> dict:
    import catalysts as cat
    try:
        cat.set_watched(body.id, body.value)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


@app.post("/api/catalysts/ack")
def catalysts_ack(body: CatToggleIn) -> dict:
    import catalysts as cat
    try:
        cat.set_acknowledged(body.id, body.value)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


class CatRefreshIn(BaseModel):
    scope: str = "watchlist"   # watchlist | biotech_1b | all_tracked


@app.post("/api/catalysts/refresh")
def catalysts_refresh(body: CatRefreshIn) -> dict:
    import catalysts as cat
    try:
        return {"counts": cat.refresh_all(scope=body.scope)}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


class TickerIn(BaseModel):
    ticker: str


@app.post("/api/catalysts/ir_extract")
def catalysts_ir_extract(body: TickerIn) -> dict:
    """IR 자료(투자설명회 PDF)에서 회사 자체공개 마일스톤 추출 (느림)."""
    import ir_milestones as irm
    try:
        return irm.extract_for_ticker(body.ticker, save=True)
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


@app.post("/api/catalysts/ai_discover")
def catalysts_ai_discover(body: TickerIn) -> dict:
    """Claude(opus) 도구호출로 12개월 카탈리스트 능동 발굴 (1-3분)."""
    import catalysts as cat
    try:
        return cat.discover_catalysts_via_ai(body.ticker)
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ───────────────────────── memos ─────────────────────────
class MemoIn(BaseModel):
    body: str


@app.get("/api/memos/timeline")
def memos_timeline(limit: int = 50) -> dict:
    import memo
    try:
        return {"memos": [{k: _clean(v) for k, v in m.items()} for m in memo.timeline(limit)]}
    except Exception as e:
        return {"memos": [], "error": f"{type(e).__name__}: {e}"}


@app.get("/api/memos/by_ticker/{ticker}")
def memos_for(ticker: str) -> dict:
    import memo
    try:
        return {"memos": memo.list_for(ticker)}
    except Exception as e:
        return {"memos": [], "error": f"{type(e).__name__}: {e}"}


@app.post("/api/memos/by_ticker/{ticker}")
def memo_add(ticker: str, body: MemoIn) -> dict:
    import memo
    try:
        return {"id": memo.add(ticker, body.body)}
    except ValueError as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


@app.patch("/api/memo/{memo_id}")
def memo_update(memo_id: int, body: MemoIn) -> dict:
    import memo
    try:
        memo.update(memo_id, body.body)
        return {"ok": True}
    except ValueError as e:
        return {"ok": False, "error": str(e)}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


@app.delete("/api/memo/{memo_id}")
def memo_delete(memo_id: int) -> dict:
    import memo
    try:
        memo.delete(memo_id)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


# ───────────────────────── valuation 템플릿 ─────────────────────────
def _build_valuation_template(ticker: str, v: dict) -> str:
    def _fmt(val, unit="", precision=2):
        if val is None:
            return "—"
        if isinstance(val, (int, float)):
            return f"{val:,.{precision}f}{unit}"
        return f"{val}{unit}"
    pe_t = _fmt(v.get("pe_trailing"), "x")
    pe_f = _fmt(v.get("pe_forward"), "x")
    if v.get("note_pe"):
        pe_t = f"N/M ({v['note_pe']})"
    ev_ebitda = _fmt(v.get("ev_ebitda"), "x")
    if v.get("note_ev_ebitda"):
        ev_ebitda = f"N/M ({v['note_ev_ebitda']})"
    ev_rev = _fmt(v.get("ev_revenue"), "x")
    ps = _fmt(v.get("ps_trailing"), "x")
    pb = _fmt(v.get("pb"), "x")
    cash = v.get("cash_b_usd") or 0
    debt = v.get("debt_b_usd") or 0
    net_cash = cash - debt
    return (
        f"## 💰 밸류에이션 ({ticker})\n\n"
        f"- 시총 ${_fmt(v.get('market_cap_b_usd'))}B · EV ${_fmt(v.get('enterprise_value_b_usd'))}B\n"
        f"- **P/E** trailing {pe_t}, forward {pe_f}\n"
        f"- **EV/EBITDA** {ev_ebitda} · EV/Revenue {ev_rev} · P/S {ps} · P/B {pb}\n"
        f"- 매출 (TTM) ${_fmt(v.get('revenue_ttm_b_usd'))}B · "
        f"EBITDA ${_fmt(v.get('ebitda_b_usd'))}B · 순이익 ${_fmt(v.get('net_income_b_usd'))}B\n"
        f"- 현금 ${_fmt(v.get('cash_b_usd'))}B - 부채 ${_fmt(v.get('debt_b_usd'))}B = 순현금 ${net_cash:,.2f}B\n"
        f"- 영업이익률 {_fmt(v.get('operating_margin_pct'), '%', 1)} · "
        f"매출총이익률 {_fmt(v.get('gross_margin_pct'), '%', 1)}\n\n"
        f"### Peak sales × OPM × P/EBIT 시나리오 (사용자 입력)\n"
        f"- 자산1: peak $___B × ___% OPM × ___x P/EBIT = $___B 시총\n"
        f"- 자산2: peak $___B × ___% × ___x = $___B 시총\n"
        f"- **합산 implied 시총**: $___B → 현 시총 ${_fmt(v.get('market_cap_b_usd'))}B 대비 ___%\n\n"
        f"### 코멘트\n- "
    )


def _valuation_payload(ticker: str) -> dict:
    from bot_tools import get_valuation_metrics
    v = get_valuation_metrics(ticker)
    return {"template": _build_valuation_template(ticker, v),
            "metrics": {k: _clean(x) for k, x in v.items()}}


@app.get("/api/valuation/{ticker}")
def valuation(ticker: str) -> dict:
    """yfinance 밸류에이션 지표 → 메모용 마크다운 템플릿 (30분 캐시)."""
    try:
        return _cached(("val", ticker), 1800, lambda: _valuation_payload(ticker))
    except Exception as e:
        return {"template": "", "error": f"{type(e).__name__}: {e}"}


# ───────────────────────── AI 리포트 ─────────────────────────
@app.get("/api/report/{ticker}")
def report_get(ticker: str) -> dict:
    """캐시된 AI 리포트 조회 (없으면 cached=False)."""
    import investment_report as ir
    try:
        rep = ir.get_cached_report(ticker)
        if not rep:
            return {"cached": False}
        return {"cached": True, **rep}
    except Exception as e:
        return {"cached": False, "error": f"{type(e).__name__}: {e}"}


@app.post("/api/report/{ticker}")
def report_generate(ticker: str) -> dict:
    """AI 리포트 생성+저장 (Claude opus, 1-3분)."""
    import investment_report as ir
    try:
        return {"ok": True, **ir.generate_and_save(ticker)}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


# ───────────────────────── 최근 펀더멘탈 기사 ─────────────────────────
import re as _re

_FUNDAMENTAL_PAT = _re.compile(
    r"(phase\s*[123][a-z]?|topline|interim|primary endpoint|"
    r"readout|data\s+(?:read|release|disclosure|update)|"
    r"\bfda\b|pdufa|adcom|advisory committee|approval|approve[ds]?|"
    r"breakthrough designation|orphan designation|priority review|"
    r"crl|complete response letter|snda|sbla|nda\b|bla\b|ind\b|"
    r"acquir(?:e|ed|es|ition)|merger|partner|partnership|collaboration|"
    r"licens(?:e|ed|es|ing)|deal\b|first patient|first dose|enrollment|"
    r"survival|response rate|orr\b|pfs\b|os\b|"
    r"clinical trial|study results?|safety|efficacy)", _re.IGNORECASE)
_NOISE_PAT = _re.compile(
    r"(price target|analyst|upgrad|downgrad|consensus|estimate|"
    r"insider (?:bought|sold)|filed form 4|options activity|unusual options|"
    r"short interest|(?:eps|revenue) (?:beat|miss|estimate)|"
    r"top \d+ stocks?|stocks? to (?:buy|watch)|trending|moving|gainers?|losers?|"
    r"premarket|after hours|benzinga|seeking alpha author|simply wall st)", _re.IGNORECASE)


def _ticker_name(ticker: str) -> str:
    try:
        from db import connect
        with connect() as c:
            row = c.execute("SELECT name FROM ticker_master WHERE ticker=?",
                            (ticker.upper(),)).fetchone()
        return (row["name"] if row else "") or ""
    except Exception:
        return ""


@app.get("/api/articles/{ticker}")
def articles(ticker: str, name: str = "", days: int = 60) -> dict:
    """최근 펀더멘탈 기사 (10분 캐시)."""
    return _cached(("articles", ticker, name, days), 600, lambda: _articles_payload(ticker, name, days))


def _articles_payload(ticker: str, name: str = "", days: int = 60) -> dict:
    nm = name or _ticker_name(ticker)
    t = str(ticker).strip()
    try:
        if t.isdigit() and len(t) == 6:
            import kr_news
            out = []
            for it in kr_news.naver_finance_news(t, limit=20):
                out.append({"title": it["title"], "link": it["link"],
                            "source": it["source"], "published": it.get("published", "")})
            for it in kr_news.for_query(nm, limit=10, days=30):
                out.append({"title": it["title"], "link": it["link"], "source": it["source"],
                            "published": it["published"].strftime("%Y-%m-%d") if it.get("published") else ""})
            seen, ded = set(), []
            for it in out:
                k = (it.get("title") or "")[:50]
                if k and k not in seen:
                    seen.add(k); ded.append(it)
            return {"articles": ded[:12]}
        from news import fetch_finviz_news, fetch_yahoo_news, fetch_google_news
        items = list(fetch_finviz_news(ticker, days=days)) + list(fetch_yahoo_news(ticker))
        clean = _re.sub(r"\b(Inc\.?|Corp\.?|Corporation|Limited|Ltd\.?|Co\.?|Company|"
                        r"Pharma|Pharmaceuticals?|Group|Holdings?|K\.K\.)$", "",
                        nm or "", flags=_re.IGNORECASE).strip(" ,")
        if len(clean) >= 4:
            try:
                items += list(fetch_google_news(clean, days=days, limit=30))
            except Exception:
                pass
        items.sort(key=lambda it: it.get("_published_dt") or 0, reverse=True)
        out, seen = [], set()
        for it in items:
            tt = (it.get("title") or "").strip()
            if len(tt) < 15 or _NOISE_PAT.search(tt) or not _FUNDAMENTAL_PAT.search(tt):
                continue
            norm = " ".join(sorted(set(tt.lower().split())))[:80]
            if norm in seen:
                continue
            seen.add(norm)
            out.append({"title": tt, "link": it.get("link") or it.get("url") or "",
                        "source": it.get("source", ""), "published": it.get("published", "")})
        return {"articles": out[:12]}
    except Exception as e:
        return {"articles": [], "error": f"{type(e).__name__}: {e}"}


# ───────────────────────── watchlist ─────────────────────────
@app.get("/api/watchlist")
def watchlist_list() -> dict:
    import watchlist as wl
    try:
        return {"rows": _records(wl.list_all())}
    except Exception as e:
        return {"rows": [], "error": f"{type(e).__name__}: {e}"}


@app.post("/api/watchlist/{ticker}")
def watchlist_add(ticker: str) -> dict:
    import watchlist as wl
    try:
        wl.add(ticker)
        return {"ok": True, "watched": True}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


@app.delete("/api/watchlist/{ticker}")
def watchlist_remove(ticker: str) -> dict:
    import watchlist as wl
    try:
        wl.remove(ticker)
        return {"ok": True, "watched": False}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


# ───────────────────────── excluded ─────────────────────────
class ExcludeIn(BaseModel):
    note: str = "user excluded"


@app.get("/api/excluded")
def excluded_list() -> dict:
    import excluded as ex
    try:
        return {"rows": ex.list_all()}
    except Exception as e:
        return {"rows": [], "error": f"{type(e).__name__}: {e}"}


@app.post("/api/excluded/{ticker}")
def excluded_add(ticker: str, body: ExcludeIn | None = None) -> dict:
    import excluded as ex
    try:
        ex.add(ticker, (body.note if body else "") or "user excluded")
        return {"ok": True, "excluded": True}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


@app.delete("/api/excluded/{ticker}")
def excluded_remove(ticker: str) -> dict:
    import excluded as ex
    try:
        ex.remove(ticker)
        return {"ok": True, "excluded": False}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


# ───────────────────────── universe 검색 (종목 추가) ─────────────────────────
@app.get("/api/universe")
def universe_search(q: str = "", limit: int = 30) -> dict:
    import universe as uni
    try:
        df = uni.get_universe()
        if q.strip():
            ql = q.strip().lower()
            df = df[df["ticker"].str.lower().str.contains(ql, na=False)
                    | df["name"].str.lower().str.contains(ql, na=False)]
        cols = [c for c in ["ticker", "name", "industry", "market_cap"] if c in df.columns]
        return {"rows": _records(df.head(limit)[cols])}
    except Exception as e:
        return {"rows": [], "error": f"{type(e).__name__}: {e}"}


# ───────────────────────── 내부자 거래 (SEC Form 4) ─────────────────────────
@app.get("/api/insiders/{ticker}")
def insiders_get(ticker: str, days: int = 180) -> dict:
    import insiders as ins
    try:
        summary = ins.summary_for_ticker(ticker, days=days)
        trades = _records(ins.get_insider_trades(ticker, days=days)) if summary.get("trades") else []
        return {"summary": {k: _clean(v) for k, v in summary.items()}, "trades": trades}
    except Exception as e:
        return {"summary": {}, "trades": [], "error": f"{type(e).__name__}: {e}"}


@app.post("/api/insiders/{ticker}/refresh")
def insiders_refresh(ticker: str) -> dict:
    import insiders as ins
    try:
        n = ins.refresh_for_tickers([ticker])
        return {"ok": True, "upserted": n}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


# ───────────────────────── IR/Pipeline URL 편집 + 자동탐색 ─────────────────────────
@app.get("/api/urls/{ticker}")
def urls_get(ticker: str) -> dict:
    import ticker_urls as tu
    try:
        return {"urls": tu.get(ticker)}
    except Exception as e:
        return {"urls": {}, "error": f"{type(e).__name__}: {e}"}


class UrlsIn(BaseModel):
    ir_url: str | None = None
    pipeline_url: str | None = None


@app.put("/api/urls/{ticker}")
def urls_set(ticker: str, body: UrlsIn) -> dict:
    import ticker_urls as tu
    try:
        tu.set_urls(ticker, ir_url=body.ir_url, pipeline_url=body.pipeline_url)
        return {"ok": True, "urls": tu.get(ticker)}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


@app.post("/api/urls/{ticker}/discover")
def urls_discover(ticker: str) -> dict:
    """홈페이지에서 IR·파이프라인 URL 자동 탐색."""
    import discover as dsc
    try:
        return {"ok": True, "found": dsc.discover(ticker)}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def _ir_pdfs_payload(ticker: str, limit: int) -> dict:
    import ticker_urls as tu
    import ir_pdfs as ip
    ir_url = tu.get(ticker).get("ir_url")
    if not ir_url:
        return {"items": [], "error": "no ir_url"}
    return {"items": ip.fetch_pdf_links(ir_url, limit=limit), "ir_url": ir_url}


@app.get("/api/ir_pdfs/{ticker}")
def ir_pdfs_get(ticker: str, limit: int = 40) -> dict:
    """IR 페이지에서 발표자료(PDF/PPT) 링크 추출 (30분 캐시)."""
    try:
        return _cached(("irpdfs", ticker, limit), 1800, lambda: _ir_pdfs_payload(ticker, limit))
    except Exception as e:
        return {"items": [], "error": f"{type(e).__name__}: {e}"}


# ───────────────────────── 워치 카탈리스트 배너 + 통계 ─────────────────────────
@app.get("/api/watched_banner")
def watched_banner(days_ahead: int = 35) -> dict:
    """워치 중 임박 카탈리스트 (이번주/1개월). notify_date 기준."""
    import datetime as _dt
    import catalysts as cat
    try:
        df = cat.get_watched(days_ahead=days_ahead)
        if df is None or df.empty:
            return {"week": [], "month": []}
        today = _dt.date.today()
        week, month = [], []
        for r in df.to_dict("records"):
            nd = str(r.get("notify_date") or "")[:10]
            try:
                days_left = (_dt.date.fromisoformat(nd) - today).days
            except Exception:
                continue
            desc = r.get("description") or ""
            m = _re.search(r"date_hint:\s*([^·]+?)(?:\s*·|$)", desc if isinstance(desc, str) else "")
            hint = m.group(1).strip() if m else (str(r.get("event_date") or "")[:10])
            item = {"id": r.get("id"), "ticker": r.get("ticker"), "title": (r.get("title") or "")[:120],
                    "date_hint": hint, "days_left": days_left}
            if days_left <= 7:
                week.append(item)
            elif days_left <= 35:
                month.append(item)
        week.sort(key=lambda x: x["days_left"]); month.sort(key=lambda x: x["days_left"])
        return {"week": week[:5], "month": month[:5]}
    except Exception as e:
        return {"week": [], "month": [], "error": f"{type(e).__name__}: {e}"}


@app.get("/api/stats")
def stats() -> dict:
    from db import connect
    out = {"memos": 0, "watchlist": 0, "universe": 0}
    try:
        with connect() as c:
            out["memos"] = c.execute("SELECT COUNT(*) AS n FROM memos").fetchone()["n"]
            out["watchlist"] = c.execute("SELECT COUNT(*) AS n FROM watchlist").fetchone()["n"]
            out["universe"] = c.execute("SELECT COUNT(*) AS n FROM ticker_master").fetchone()["n"]
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    return out


# ───────────────────────── 운영 (데이터 갱신 · 텔레그램) ─────────────────────────
class OpsIn(BaseModel):
    country: str = "USA"


@app.post("/api/ops/refresh_universe")
def ops_refresh_universe(body: OpsIn) -> dict:
    try:
        if body.country == "KOR":
            import kr_universe
            from collectors.high_low import collect_kr
            n_seed = kr_universe.seed()
            n_col = collect_kr()
            return {"ok": True, "seeded": n_seed, "collected": n_col}
        import universe as uni
        return {"ok": True, "loaded": uni.load_universe()}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


@app.post("/api/ops/refresh_highs")
def ops_refresh_highs(body: OpsIn) -> dict:
    try:
        from collectors.high_low import collect, collect_kr
        n = collect_kr() if body.country == "KOR" else collect(industry_filter=None)
        return {"ok": True, "processed": n}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


@app.post("/api/ops/telegram")
def ops_telegram(body: OpsIn) -> dict:
    import telegram_report as tr
    try:
        res = tr.daily_run_kr() if body.country == "KOR" else tr.daily_run()
        return {"ok": True, "result": {k: _clean(v) for k, v in (res or {}).items()} if isinstance(res, dict) else res}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


# ───────────────────────── 자동매매(조건매매) ─────────────────────────
class AutoChatIn(BaseModel):
    messages: list[dict]                 # [{role:'user'|'assistant', content:str}]


class AutoOrderIn(BaseModel):
    order: dict                          # build_condition 결과의 order(또는 수동 구성)


@app.post("/api/auto/chat")
def auto_chat(body: AutoChatIn) -> dict:
    """챗 조건 빌더 — need_info(질문) 또는 complete(완성된 order) 반환."""
    import auto_trade as at
    return at.build_condition(body.messages)


@app.get("/api/auto/orders")
def auto_orders() -> dict:
    import auto_trade as at
    return {"orders": [{k: _clean(v) for k, v in o.items()} for o in at.list_orders()]}


@app.post("/api/auto/orders")
def auto_create(body: AutoOrderIn) -> dict:
    import auto_trade as at
    try:
        return {"ok": True, "id": at.create(body.order)}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


@app.get("/api/auto/orders/{oid}")
def auto_get(oid: int) -> dict:
    import auto_trade as at
    o = at.get(oid)
    return {"order": {k: _clean(v) for k, v in o.items()}} if o else {"order": None}


@app.post("/api/auto/orders/{oid}/cancel")
def auto_cancel(oid: int) -> dict:
    import auto_trade as at
    try:
        at.cancel(oid)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


@app.post("/api/auto/evaluate")
def auto_evaluate() -> dict:
    """armed 조건 전부 평가(충족 시 dry-run 발동). 수동/크론 호출."""
    import auto_trade as at
    return at.evaluate_all()


@app.on_event("startup")
async def _auto_eval_loop():
    """24/7 자동매매 평가 루프 — AUTO_EVAL_ENABLED=1일 때만(=Render 상시 서비스).
    로컬 uvicorn은 이 env 없이 꺼둠(로컬은 triggers_runner 30분 cron이 담당) → 중복 방지.
    별도 cron 서비스/추가 빌드 없이 상시 웹 안에서 도는 게 가장 저렴·단순."""
    import asyncio
    import os as _os
    if (_os.environ.get("AUTO_EVAL_ENABLED") or "").strip() not in ("1", "true", "True"):
        return
    mins = max(1, int(_os.environ.get("AUTO_EVAL_MINUTES") or 15))

    async def _loop():
        import auto_trade as at
        await asyncio.sleep(20)            # 기동 안정화 대기
        while True:
            try:
                res = await asyncio.to_thread(at.evaluate_all)
                print(f"[auto_eval] {res}", flush=True)
            except Exception as e:
                print(f"[auto_eval] error: {e}", flush=True)
            await asyncio.sleep(mins * 60)

    asyncio.create_task(_loop())


# ───────────────────────── 정적 프론트(React 빌드) 서빙 ─────────────────────────
# 프로덕션(단일 서비스)에서는 FastAPI가 빌드된 React(web/dist)를 같은 오리진에서 서빙한다.
# api.js가 상대경로 /api/* 를 쓰므로 CORS·프록시 불필요. 라우트 정의가 모두 끝난 뒤
# 마지막에 catch-all로 등록 → /api/* 가 항상 우선 매칭됨. (dev에선 dist 없으면 비활성)
import os as _os
_DIST = _os.path.join(_os.path.dirname(__file__), "web", "dist")
if _os.path.isdir(_DIST):
    from fastapi.responses import FileResponse
    from starlette.responses import JSONResponse

    @app.get("/{full_path:path}")
    def _spa(full_path: str):
        # 위에서 매칭 안 된 /api/* 는 404 (index.html로 흘리지 않음)
        if full_path.startswith("api/") or full_path == "api":
            return JSONResponse({"detail": "Not Found"}, status_code=404)
        candidate = _os.path.normpath(_os.path.join(_DIST, full_path))
        # 경로 탈출 방지 + 실제 파일이면 그대로, 아니면 SPA index.html
        if candidate.startswith(_DIST) and full_path and _os.path.isfile(candidate):
            return FileResponse(candidate)
        return FileResponse(_os.path.join(_DIST, "index.html"))
