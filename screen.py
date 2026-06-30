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
CATALYST_CHECK_TTL_DAYS = 14   # 2/3상 '미발견' 종목 재web_search 주기(토큰 절약)
CATALYST_FILL_MAX_CALLS = 60   # 1회 보강에서 최대 web_search 호출 수(폭주 방지)

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
    # 견고 파싱 — JSON 코드블록/$·콤마/일본어 등 다양한 출력에서 숫자만 직접 추출
    val = None
    try:                                              # 우선 정식 JSON 시도
        m = re.search(r"\{[^{}]*peak_sales_m[^{}]*\}", txt, re.S)
        if m:
            val = float(json.loads(m.group(0)).get("peak_sales_m"))
    except Exception:
        val = None
    if val is None:                                   # 폴백: peak_sales_m 뒤 숫자 직접
        m = re.search(r'peak_sales_m["\s:=]*\$?\s*([\d,]+(?:\.\d+)?)', txt, re.I)
        if m:
            try:
                val = float(m.group(1).replace(",", ""))
            except Exception:
                val = None
    if not val or val <= 0:
        return None
    bm = re.search(r'basis["\s:=]*["\']?([^"\'\n}]+)', txt, re.I)
    basis = (bm.group(1).strip()[:300] if bm else "")
    with connect() as c:
        c.execute(
            """INSERT INTO peak_sales_est (ticker, peak_sales_m, basis, updated_at)
               VALUES (?,?,?,?) ON CONFLICT (ticker) DO UPDATE SET
                 peak_sales_m=excluded.peak_sales_m, basis=excluded.basis,
                 updated_at=excluded.updated_at""",
            (ticker, val, basis, datetime.now().isoformat(timespec="seconds")))
        c.commit()
    return {"peak_sales_m": val, "basis": basis}


# ─────────────── 2/3상 readout 날짜 자동 보강 (catalyst_fill) ───────────────
# 보드 종목 중 8개월 내 2/3상 clinical_readout이 catalysts에 없는 종목에 대해
# Claude(web_search)로 다음 확정 Phase 2/3 topline/data readout 날짜를 찾아 채운다.
# 절대 지어내지 않음: 모델이 구체 날짜+근거를 반환할 때만 insert, 아니면 미발견 기록만.
_DATE_RE = re.compile(r"\b(20\d{2})-(\d{2})-(\d{2})\b")


def _catalyst_checked_recently(ticker: str, ttl_days: int = CATALYST_CHECK_TTL_DAYS) -> bool:
    """TTL 내 이미 체크한 종목이면 True(web_search 스킵)."""
    with connect() as c:
        r = c.execute("SELECT checked_at FROM catalyst_check WHERE ticker=?",
                      (ticker,)).fetchone()
    if not r or not r["checked_at"]:
        return False
    try:
        age = (datetime.now() - datetime.fromisoformat(r["checked_at"])).days
    except Exception:
        return False
    return age < ttl_days


def _record_catalyst_check(ticker: str, found: bool, note: str = "") -> None:
    with connect() as c:
        c.execute(
            """INSERT INTO catalyst_check (ticker, checked_at, found, note)
               VALUES (?,?,?,?) ON CONFLICT (ticker) DO UPDATE SET
                 checked_at=excluded.checked_at, found=excluded.found, note=excluded.note""",
            (ticker, datetime.now().isoformat(timespec="seconds"), bool(found),
             (note or "")[:300]))
        c.commit()


def _find_next_p23(ticker: str, name: str) -> dict | None:
    """Claude(web_search)로 종목의 **다음 확정 Phase 2/3 topline/data readout 날짜**(리드
    프로그램 1건)를 조사. 구체 날짜+근거가 있으면 dict, 없으면 None(절대 지어내지 않음).
    반환 dict: {date(YYYY-MM-DD), phase, program, granularity, source, confidence}."""
    key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not key:
        return None
    import anthropic
    cl = anthropic.Anthropic(api_key=key)
    prompt = (
        f"{name} ({ticker})의 다음 **확정되었거나 회사가 공식 가이던스로 제시한 Phase 2 또는 "
        "Phase 3 임상의 topline/primary data readout 예정 시점**을 찾아라. 가장 핵심(lead)·"
        "material한 프로그램 1건만.\n"
        "- 반드시 web_search로 회사 IR/보도자료/ClinicalTrials.gov/신뢰할 만한 출처에서 확인.\n"
        "- Phase 1, Phase 4, 단순 enrollment/completion, 추정·루머는 제외. 회사가 제시한 "
        "data readout/topline 가이던스여야 함.\n"
        "- **확실한 근거가 없으면 절대 지어내지 말고 expected_date를 null로 둬라.**\n"
        "- 분기/반기/월만 공개됐으면 대표일(분기말 Q1→03-31·Q2→06-30·Q3→09-30·Q4→12-31, "
        "반기말 1H→06-30·2H→12-31, 월말)로 변환하고 granularity에 원문 표기(예: 'Q4 2026').\n"
        '마지막 줄에 JSON만 출력: {"expected_date": "YYYY-MM-DD" 또는 null, '
        '"phase": "Phase 2"|"Phase 3"|"Phase 2/3", "program": "자산명/적응증", '
        '"granularity": "exact|month|quarter|half|year", "source": "출처 한 줄(URL/발행처)", '
        '"confidence": "high|medium|low"}')
    tools = [{"type": "web_search_20250305", "name": "web_search"}]
    msgs = [{"role": "user", "content": prompt}]
    txt = ""
    try:
        for _ in range(6):
            r = cl.messages.create(model=CLAUDE_MODEL, max_tokens=1500, tools=tools, messages=msgs)
            if r.stop_reason == "pause_turn":
                msgs.append({"role": "assistant", "content": r.content}); continue
            txt = "".join(b.text for b in r.content if b.type == "text")
            break
    except Exception as e:
        log.warning("catalyst_fill LLM 실패 %s: %s", ticker, e)
        return None
    # 견고 파싱 — 마지막 JSON 객체 추출
    obj = None
    for m in re.finditer(r"\{[^{}]*expected_date[^{}]*\}", txt, re.S):
        try:
            obj = json.loads(m.group(0))
        except Exception:
            obj = None
    if not isinstance(obj, dict):
        return None
    ed = obj.get("expected_date")
    if not ed or str(ed).strip().lower() in ("null", "none", ""):
        return None
    dm = _DATE_RE.search(str(ed))
    if not dm:
        return None
    try:
        iso = date(int(dm.group(1)), int(dm.group(2)), int(dm.group(3))).isoformat()
    except Exception:
        return None
    if iso < date.today().isoformat():        # 이미 지난 readout은 무의미 → 미발견 취급
        return None
    conf = str(obj.get("confidence") or "").strip().lower()
    if conf == "low":                          # 불확실하면 보수적으로 미삽입
        return None
    phase_raw = str(obj.get("phase") or "").strip()
    # 제목이 screen._PHASE_SQL에 매칭되도록 'Phase 2'/'Phase 3' 토큰 보장
    pl = phase_raw.lower()
    if "2/3" in phase_raw or ("2" in pl and "3" in pl):
        phase = "Phase 2/3"
    elif "3" in pl:
        phase = "Phase 3"
    elif "2" in pl:
        phase = "Phase 2"
    else:
        phase = "Phase 2/3"                     # 명시 없으면 둘 다 매칭되는 표기
    return {
        "date": iso,
        "phase": phase,
        "program": (str(obj.get("program") or "").strip())[:120],
        "granularity": (str(obj.get("granularity") or "exact").strip())[:16],
        "source": (str(obj.get("source") or "").strip())[:280],
        "confidence": conf or "medium",
    }


def _insert_p23_catalyst(ticker: str, res: dict) -> bool:
    """찾은 readout을 catalysts에 clinical_readout으로 insert. 중복이면 skip. 삽입 여부 반환."""
    iso = res["date"]
    phase = res["phase"]
    prog = res.get("program") or ""
    gran = (res.get("granularity") or "exact").lower()
    title = f"{phase} {prog} topline/data readout".strip()
    if gran != "exact":                        # 분기/월 등 정밀도 표기를 제목에 남김
        title += " (가이던스 시점)"
    title = title[:300]
    desc = (f"date_hint: {res.get('source','')[:80]} · 출처: AI 2/3상 보강"
            f"({gran}, conf={res.get('confidence','')})")
    now = datetime.now().isoformat(timespec="seconds")
    with connect() as c:
        # 같은 ticker/날짜에 이미 2/3상 readout이 있으면 중복 방지
        dup = c.execute(
            f"""SELECT 1 FROM catalysts WHERE ticker=? AND event_type='clinical_readout'
                AND event_date=? AND {_PHASE_SQL} LIMIT 1""",
            (ticker, iso)).fetchone()
        if dup:
            return False
        c.execute(
            "INSERT INTO catalysts (ticker, event_date, event_type, title, "
            "description, source, fetched_at) VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT (ticker, event_date, event_type, title) DO NOTHING",
            (ticker, iso, "clinical_readout", title, desc, "ai_p23_fill", now))
        c.commit()
    return True


def fill_p23_for_board(country: str | None = None,
                       max_calls: int = CATALYST_FILL_MAX_CALLS) -> dict:
    """보드(신고가 ∪ 상승폭) 종목 중 8개월 내 2/3상 readout이 없는 종목에 대해
    Claude web_search로 다음 Phase 2/3 readout 날짜를 찾아 catalysts에 보강.
    - 이미 upcoming_p23 있으면 스킵(토큰 0).
    - TTL 내 체크한 종목 스킵. max_calls로 호출 상한.
    데일리런에서 refresh_board_flags 직전에 호출. 반환: 통계 dict."""
    from collectors.high_low import fetch_new_highs, fetch_top_movers
    floor = 1500.0
    if country == "KOR":
        try:
            import kr_universe as ku
            floor = ku.kr_min_mcap_usd_m()
        except Exception:
            floor = 324.0
    cand: dict[str, str] = {}
    try:
        for df in (fetch_new_highs("high", limit=100, country=country, min_mcap=floor),
                   fetch_top_movers(limit=100, min_mcap=floor, min_perf=5.0, country=country)):
            for _, r in df.iterrows():
                tk = str(r.get("ticker") or "").strip()
                if tk and tk not in cand:
                    cand[tk] = r.get("name") or tk
    except Exception as e:
        return {"error": str(e)}
    added = none_found = skipped_ttl = skipped_have = calls = 0
    for tk, nm in cand.items():
        try:
            if upcoming_p23(tk):                # 이미 미래 2/3상 있음 → 스킵(토큰 0)
                skipped_have += 1
                continue
            if _catalyst_checked_recently(tk):  # TTL 내 체크함 → 스킵
                skipped_ttl += 1
                continue
            if calls >= max_calls:
                break
            calls += 1
            res = _find_next_p23(tk, nm)
            if res and _insert_p23_catalyst(tk, res):
                _record_catalyst_check(tk, True,
                                       f"{res['phase']} {res['date']} ({res.get('confidence')})")
                added += 1
            elif res:                            # 찾았으나 중복(이미 존재) — found 기록
                _record_catalyst_check(tk, True, f"dup {res['phase']} {res['date']}")
            else:
                _record_catalyst_check(tk, False, "2/3상 미발견")
                none_found += 1
        except Exception as e:
            log.warning("catalyst_fill 실패 %s: %s", tk, e)
    return {"candidates": len(cand), "calls": calls, "added": added,
            "none_found": none_found, "skipped_have_p23": skipped_have,
            "skipped_ttl": skipped_ttl}


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
