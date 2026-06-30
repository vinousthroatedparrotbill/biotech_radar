"""'연두색 음영' 스크린 — 신고가/상승폭 종목 중
  ① 8개월 내 2/3상 readout 예정  ②  mcap / peak_sales(base case) ≤ 4배
인 종목을 플래그(승률 높았던 패턴). 데일리런이 갱신, 보드가 표시.

peak_sales는 리드 자산의 **base case 글로벌 피크 연매출($M)** LLM 추정 — 안정적이라 길게
캐시(peak_sales_est), mcap/비율만 매일 재계산.
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import date, datetime, timedelta

from db import connect

log = logging.getLogger(__name__)
CLAUDE_MODEL = "claude-opus-4-8"
RATIO_MAX = 5.0
WINDOW_MONTHS = 8
PEAK_TTL_DAYS = 45          # 피크매출 추정 캐시 유효기간

# 2/3상 매칭(임상 1·4상 제외). ClinicalTrials는 PHASE2/PHASE3, 자유텍스트는 'phase 2/3','2상' 등
# psycopg2가 리터럴 %를 파라미터로 오인 → %% 이스케이프
_PHASE_SQL = ("(title ILIKE '%%phase2%%' OR title ILIKE '%%phase 2%%' OR title ILIKE '%%phase ii%%' "
              "OR title ILIKE '%%phase3%%' OR title ILIKE '%%phase 3%%' OR title ILIKE '%%phase iii%%' "
              "OR title ILIKE '%%2/3%%' OR title ILIKE '%%2상%%' OR title ILIKE '%%3상%%')")


def upcoming_p23(ticker: str, months: int = WINDOW_MONTHS) -> dict | None:
    """8개월 내 가장 가까운 2/3상 clinical_readout {date, title}. 없으면 None."""
    today = date.today()
    end = (today + timedelta(days=int(months * 30.4))).isoformat()
    with connect() as c:
        r = c.execute(
            f"""SELECT event_date, title FROM catalysts
                WHERE ticker = ? AND event_type = 'clinical_readout'
                  AND event_date >= ? AND event_date <= ? AND {_PHASE_SQL}
                ORDER BY event_date ASC LIMIT 1""",
            (ticker, today.isoformat(), end)).fetchone()
    return {"date": r["event_date"], "title": r["title"]} if r else None


def _peak_cached(ticker: str) -> dict | None:
    with connect() as c:
        r = c.execute("SELECT peak_sales_m, basis, updated_at FROM peak_sales_est WHERE ticker=?",
                      (ticker,)).fetchone()
    if not r or r["peak_sales_m"] is None:
        return None
    try:
        age = (datetime.now() - datetime.fromisoformat(r["updated_at"])).days
    except Exception:
        age = 0
    if age > PEAK_TTL_DAYS:
        return None
    return {"peak_sales_m": r["peak_sales_m"], "basis": r["basis"]}


def peak_sales(ticker: str, name: str, asset_hint: str = "") -> dict | None:
    """리드 자산 base case 글로벌 피크 연매출($M) — 캐시 우선, 없으면 LLM(web_search) 추정."""
    hit = _peak_cached(ticker)
    if hit:
        return hit
    key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not key:
        return None
    import anthropic
    cl = anthropic.Anthropic(api_key=key)
    prompt = (
        f"{name} ({ticker})의 리드 자산(특히 곧 2/3상 readout 예정인 자산"
        + (f"; 관련 이벤트: {asset_hint}" if asset_hint else "") + ")의 "
        "**base case(현실적·리스크조정 컨센서스 기준, bull 아님) 글로벌 피크 연매출**을 $M 단위로 "
        "추정해. 애널리스트 컨센서스/유사약물 벤치마크를 web_search로 참고. "
        "마지막 줄에 JSON만: {\"peak_sales_m\": <숫자 $M>, \"basis\": \"근거 한 줄\"}")
    tools = [{"type": "web_search_20250305", "name": "web_search"}]
    msgs = [{"role": "user", "content": prompt}]
    txt = ""
    try:
        for _ in range(5):
            r = cl.messages.create(model=CLAUDE_MODEL, max_tokens=1500, tools=tools, messages=msgs)
            if r.stop_reason == "pause_turn":
                msgs.append({"role": "assistant", "content": r.content}); continue
            txt = "".join(b.text for b in r.content if b.type == "text")
            break
    except Exception as e:
        log.warning("peak_sales LLM 실패 %s: %s", ticker, e)
        return None
    m = re.search(r"\{[^{}]*peak_sales_m[^{}]*\}", txt, re.S)
    if not m:
        return None
    try:
        j = json.loads(m.group(0))
        val = float(j.get("peak_sales_m"))
    except Exception:
        return None
    if val <= 0:
        return None
    with connect() as c:
        c.execute(
            """INSERT INTO peak_sales_est (ticker, peak_sales_m, basis, updated_at)
               VALUES (?,?,?,?) ON CONFLICT (ticker) DO UPDATE SET
                 peak_sales_m=excluded.peak_sales_m, basis=excluded.basis,
                 updated_at=excluded.updated_at""",
            (ticker, val, str(j.get("basis", ""))[:300],
             datetime.now().isoformat(timespec="seconds")))
        c.commit()
    return {"peak_sales_m": val, "basis": j.get("basis", "")}


def _upsert_flag(ticker, snap, flagged, ratio, ps, mcap, cat, note):
    with connect() as c:
        c.execute(
            """INSERT INTO screen_flags
               (ticker, snapshot_date, flagged, ratio, peak_sales_m, market_cap,
                catalyst_date, catalyst_title, note, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT (ticker) DO UPDATE SET
                 snapshot_date=excluded.snapshot_date, flagged=excluded.flagged,
                 ratio=excluded.ratio, peak_sales_m=excluded.peak_sales_m,
                 market_cap=excluded.market_cap, catalyst_date=excluded.catalyst_date,
                 catalyst_title=excluded.catalyst_title, note=excluded.note,
                 updated_at=excluded.updated_at""",
            (ticker, snap, bool(flagged), ratio, ps, mcap,
             (cat or {}).get("date"), (cat or {}).get("title"), note,
             datetime.now().isoformat(timespec="seconds")))
        c.commit()


def refresh_board_flags(country: str | None = None) -> dict:
    """보드(신고가 ∪ 상승폭) 종목에 대해 플래그 갱신. 8개월 내 2/3상 있는 종목만 peak_sales
    추정(캐시) → mcap/peak_sales ≤ 4면 flagged. 데일리런에서 호출."""
    from collectors.high_low import fetch_new_highs, fetch_top_movers
    floor = 1500.0
    if country == "KOR":
        try:
            import kr_universe as ku
            floor = ku.kr_min_mcap_usd_m()
        except Exception:
            floor = 324.0
    cand: dict[str, dict] = {}
    try:
        for df in (fetch_new_highs("high", limit=100, country=country, min_mcap=floor),
                   fetch_top_movers(limit=100, min_mcap=floor, min_perf=5.0, country=country)):
            for _, r in df.iterrows():
                tk = str(r.get("ticker") or "").strip()
                if tk and tk not in cand:
                    cand[tk] = {"name": r.get("name") or tk, "mcap": r.get("market_cap")}
    except Exception as e:
        return {"error": str(e)}
    snap = date.today().isoformat()
    flagged_n = checked = 0
    for tk, d in cand.items():
        try:
            cat = upcoming_p23(tk)
            if not cat:
                _upsert_flag(tk, snap, False, None, None, d["mcap"], None, "2/3상 없음")
                continue
            checked += 1
            mcap = d.get("mcap")
            ps = peak_sales(tk, d["name"], cat.get("title", "")) if mcap else None
            if not ps or not mcap:
                _upsert_flag(tk, snap, False, None, (ps or {}).get("peak_sales_m"),
                             mcap, cat, "peak_sales 추정 실패")
                continue
            ratio = mcap / ps["peak_sales_m"]
            flagged = ratio <= RATIO_MAX
            note = (f"2/3상 {cat['date']} · mcap/peak_sales {ratio:.1f}x "
                    f"(peak ${ps['peak_sales_m']:,.0f}M)")
            _upsert_flag(tk, snap, flagged, ratio, ps["peak_sales_m"], mcap, cat, note)
            if flagged:
                flagged_n += 1
        except Exception as e:
            log.warning("flag 계산 실패 %s: %s", tk, e)
    return {"candidates": len(cand), "with_p23": checked, "flagged": flagged_n}


def flags_map(tickers: list[str]) -> dict[str, dict]:
    """보드 표시용 — {ticker: {flagged, note, ratio, catalyst_date}}."""
    if not tickers:
        return {}
    out = {}
    with connect() as c:
        ph = ",".join(["?"] * len(tickers))
        for r in c.execute(
                f"SELECT ticker, flagged, note, ratio, catalyst_date, catalyst_title "
                f"FROM screen_flags WHERE ticker IN ({ph})", tuple(tickers)).fetchall():
            out[r["ticker"]] = dict(r)
    return out
