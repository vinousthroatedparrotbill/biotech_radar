"""토스증권 Open API 시세 연동 — 차트(OHLCV) + 현재가.

Yahoo(yfinance)는 클라우드 데이터센터 IP가 차단되지만, 토스는 OAuth2 토큰 인증이라
클라우드에서도 동작한다. 키가 설정돼 있으면 prices/portfolio가 토스를 우선 사용한다.

env(.env / Streamlit secrets): TOSS_API_KEY(client id), TOSS_API_SECRET.
엔드포인트:
  POST /oauth2/token            (client_credentials)
  GET  /api/v1/prices?symbols=  (현재가, 콤마구분 최대 200)
  GET  /api/v1/candles?symbol=&interval=1m|1d&count=<=200&before=ISO  (nextBefore 페이지네이션)
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

BASE = "https://openapi.tossinvest.com"
_TOKEN = {"value": None, "exp": 0.0}     # 프로세스 내 토큰 캐시


def _creds() -> tuple[str, str]:
    return ((os.environ.get("TOSS_API_KEY") or "").strip(),
            (os.environ.get("TOSS_API_SECRET") or "").strip())


def available() -> bool:
    """토스 키가 설정돼 있나."""
    cid, sec = _creds()
    return bool(cid and sec)


def diagnose() -> dict:
    """연결 상태 진단 — 키 존재 + 실제 토큰 발급까지 테스트. 값은 노출 안 함.
    {ok, stage, msg}. UI에서 클라우드 원인 파악용."""
    cid, sec = _creds()
    if not (cid and sec):
        miss = [n for n, v in [("TOSS_API_KEY", cid), ("TOSS_API_SECRET", sec)] if not v]
        return {"ok": False, "stage": "creds",
                "msg": f"키 미설정: {', '.join(miss)} (Secrets 확인)"}
    try:
        global _TOKEN
        _TOKEN = {"value": None, "exp": 0.0}      # 캐시 무시하고 실제 발급 테스트
        t = _token()
        return {"ok": True, "stage": "token", "msg": f"토큰 발급 OK (len={len(t)})"}
    except Exception as e:
        detail = ""
        resp = getattr(e, "response", None)
        if resp is not None:
            detail = f" [HTTP {resp.status_code}] {str(resp.text)[:120]}"
        return {"ok": False, "stage": "token",
                "msg": f"토큰 발급 실패: {type(e).__name__}{detail}"}


def _token() -> str:
    if _TOKEN["value"] and time.time() < _TOKEN["exp"] - 60:
        return _TOKEN["value"]
    cid, sec = _creds()
    if not (cid and sec):
        raise RuntimeError("TOSS_API_KEY/SECRET 미설정")
    r = requests.post(
        f"{BASE}/oauth2/token",
        data={"grant_type": "client_credentials", "client_id": cid, "client_secret": sec},
        headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=20,
    )
    r.raise_for_status()
    j = r.json()
    _TOKEN["value"] = j["access_token"]
    _TOKEN["exp"] = time.time() + float(j.get("expires_in", 3600))
    return _TOKEN["value"]


def _get(path: str, params: dict) -> dict:
    r = requests.get(f"{BASE}{path}", params=params,
                     headers={"Authorization": f"Bearer {_token()}"}, timeout=20)
    r.raise_for_status()
    return r.json()


def _is_supported(ticker: str) -> bool:
    """토스가 다루는 심볼인가 — 미국(영문) / 국내(6자리 숫자). .T/.HK 등 그외는 미지원."""
    t = (ticker or "").strip().upper()
    if not t or "." in t:
        return False
    return t.isalpha() or (t.isdigit() and len(t) == 6)


# ───────────────────────── 현재가 ─────────────────────────
def quote(symbols) -> dict:
    """{symbol: last_price(float)}. symbols: str 또는 list."""
    if isinstance(symbols, str):
        symbols = [symbols]
    syms = [s.strip().upper() for s in symbols if _is_supported(s)]
    if not syms:
        return {}
    out = {}
    for i in range(0, len(syms), 200):
        chunk = syms[i:i + 200]
        j = _get("/api/v1/prices", {"symbols": ",".join(chunk)})
        for p in (j.get("result") or []):
            try:
                out[p["symbol"].upper()] = float(p["lastPrice"])
            except Exception:
                pass
    return out


def price(ticker: str) -> float | None:
    return quote(ticker).get((ticker or "").strip().upper())


# ───────────────────────── 캔들(OHLCV) ─────────────────────────
def _candles_raw(symbol: str, interval: str, count: int, before: str | None) -> dict:
    params = {"symbol": symbol, "interval": interval, "count": min(max(count, 1), 200)}
    if before:
        params["before"] = before
    return _get("/api/v1/candles", params)


def _history(symbol: str, interval: str, bars_needed: int, max_pages: int = 12) -> pd.DataFrame:
    """interval 캔들을 bars_needed개 이상 모을 때까지 nextBefore로 페이지네이션."""
    rows, before, pages = [], None, 0
    while pages < max_pages:
        res = (_candles_raw(symbol, interval, 200, before).get("result") or {})
        items = res.get("candles") or []
        if not items:
            break
        rows.extend(items)
        before = res.get("nextBefore")
        pages += 1
        if not before or len(rows) >= bars_needed:
            break
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([{
        "dt": pd.to_datetime(c["timestamp"]),
        "Open": float(c["openPrice"]), "High": float(c["highPrice"]),
        "Low": float(c["lowPrice"]), "Close": float(c["closePrice"]),
        "Volume": float(c.get("volume") or 0),
    } for c in rows])
    df = df.dropna(subset=["Close"]).drop_duplicates("dt").set_index("dt").sort_index()
    if df.index.tz is not None:           # tz 제거 (yfinance 호환)
        df.index = df.index.tz_localize(None)
    return df


def daily(symbol: str, bars: int = 420) -> pd.DataFrame:
    """일봉 OHLCV `bars`개 (브릿지 백필용). Open/High/Low/Close/Volume DataFrame."""
    sym = (symbol or "").strip().upper()
    if not _is_supported(sym):
        return pd.DataFrame()
    return _history(sym, "1d", bars, max_pages=max(3, bars // 200 + 2))


_RESAMPLE = {"1wk": "W", "1mo": "ME"}


def get_ohlcv(ticker: str, period: str, interval: str = "1d") -> pd.DataFrame:
    """prices.fetch_ohlcv 호환 — Open/High/Low/Close/Volume DataFrame.
    토스 미지원 심볼이면 빈 DF(상위에서 yfinance fallback)."""
    sym = (ticker or "").strip().upper()
    if not _is_supported(sym):
        return pd.DataFrame()
    # 기간 → 필요 거래일 수 (이평선 buffer 포함, prices.py와 동일 취지)
    period_days = {"1d": 1, "5d": 7, "1m": 31, "3m": 95, "6m": 190,
                   "1y": 380, "5y": 1860, "max": 365 * 30}.get(period, 380)
    if period == "1d":                    # 인트라데이: 1m → 5m 리샘플
        df = _history(sym, "1m", 200, max_pages=3)
        if df.empty:
            return df
        last_day = df.index.max().normalize()
        df = df[df.index >= last_day]
        return df.resample("5min").agg({"Open": "first", "High": "max", "Low": "min",
                                        "Close": "last", "Volume": "sum"}).dropna(subset=["Close"])
    bars = int((period_days + 250) * 0.75) + 10     # 거래일≈영업일*0.75
    df = _history(sym, "1d", bars, max_pages=12)
    if df.empty:
        return df
    rule = _RESAMPLE.get(interval)
    if rule:
        df = df.resample(rule).agg({"Open": "first", "High": "max", "Low": "min",
                                    "Close": "last", "Volume": "sum"}).dropna(subset=["Close"])
    return df
