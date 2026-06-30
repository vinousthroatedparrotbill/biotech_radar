"""유사 종목(peer) 아이디어 — 대상 종목의 투자포인트/적응증/기전/에셋을 공유하는 peer를
간단한 판단과 함께 제시. 모달의 'Peer 아이디어' 섹션용.

설계: 대상의 맥락(산업 + 상승이유 + 최근 뉴스)을 모아, 유니버스(ticker_master) 후보 풀과
함께 Claude에 주고 구조화 출력으로 peer 목록을 받는다. peer는 ticker_master로 검증 +
현재가/시총 부착 + 투자 가능(유니버스) 여부 플래그.
"""
from __future__ import annotations

import json
import logging
import os

from db import connect

log = logging.getLogger(__name__)
CLAUDE_MODEL = "claude-opus-4-8"


def _is_kr(t: str) -> bool:
    t = (t or "").strip()
    return t.isdigit() and len(t) == 6


def _universe_pool(country: str | None, limit: int = 500) -> list[dict]:
    """peer 후보 풀 — 같은 시장 우선. (ticker, name, industry)."""
    with connect() as c:
        if country == "KOR":
            rows = c.execute(
                "SELECT ticker, name, industry FROM ticker_master "
                "WHERE country='KOR' ORDER BY market_cap DESC NULLS LAST LIMIT ?", (limit,)
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT ticker, name, industry FROM ticker_master "
                "WHERE country IS NULL OR country <> 'KOR' "
                "ORDER BY market_cap DESC NULLS LAST LIMIT ?", (limit,)
            ).fetchall()
    return [dict(r) for r in rows]


def _target_context(ticker: str) -> dict:
    """대상 종목 맥락 — 이름/산업 + 상승이유(있으면) + 최근 뉴스 제목."""
    tk = (ticker or "").strip()
    ctx: dict = {"ticker": tk, "name": tk, "industry": None, "thesis": "", "news": []}
    try:
        with connect() as c:
            r = c.execute("SELECT name, industry, country FROM ticker_master WHERE ticker=?",
                          (tk,)).fetchone()
        if r:
            ctx["name"] = r.get("name") or tk
            ctx["industry"] = r.get("industry")
            ctx["country"] = r.get("country")
    except Exception:
        pass
    # 상승이유 캐시에서 이 종목 블록 추출(있으면 thesis로)
    try:
        country = "KOR" if _is_kr(tk) else "USA"
        with connect() as c:
            for kind in ("high", "movers"):
                row = c.execute("SELECT markdown FROM reason_cache WHERE country=? AND kind=?",
                                (country, kind)).fetchone()
                md = (row["markdown"] if row else "") or ""
                if tk in md:
                    # 해당 티커 헤더부터 다음 헤더 전까지
                    import re
                    m = re.search(rf"\*\*[^*]*{re.escape(tk)}[^*]*\*\*", md)
                    if m:
                        seg = md[m.start():]
                        nxt = re.search(r"\n\*\*", seg[2:])
                        ctx["thesis"] = (seg[:nxt.start() + 2] if nxt else seg)[:600]
                        break
    except Exception:
        pass
    # 최근 뉴스 제목
    try:
        import bot_tools as bt
        if _is_kr(tk):
            ctx["news"] = [n.get("title", "") for n in
                           (bt.get_kr_news(ticker=tk, limit=6).get("news", []) or [])][:6]
        else:
            ctx["news"] = [(n.get("title", "") if isinstance(n, dict) else str(n))
                           for n in (bt.fetch_recent_news_for(tk, 6) or [])][:6]
    except Exception:
        pass
    return ctx


_TOOL = {
    "name": "suggest_peers",
    "description": "대상 종목과 투자포인트/적응증/기전/에셋을 공유하는 peer 종목 제시.",
    "input_schema": {
        "type": "object",
        "properties": {
            "target_thesis": {"type": "string", "description": "대상의 핵심 투자포인트 1~2줄 요약"},
            "peers": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "ticker": {"type": "string", "description": "미국 심볼 또는 한국 6자리(모르면 빈칸)"},
                        "name": {"type": "string"},
                        "basis": {"type": "string",
                                  "description": "공유 근거: thesis|indication|mechanism|asset 중"},
                        "note": {"type": "string", "description": "간단한 판단 한 줄(왜 peer인지 + 차이/주의)"},
                    },
                    "required": ["name", "basis", "note"],
                },
            },
        },
        "required": ["target_thesis", "peers"],
    },
}

_SYS = (
    "너는 fund manager의 biotech 애널리스트다. 주어진 대상 종목의 투자포인트(상승 동인)·적응증·"
    "기전·핵심 에셋을 파악하고, 그것을 **공유하는 peer 종목**을 제시하라. 예: 사이키델릭/MDD 테마 "
    "종목이면 COMPASS(CMPS)·GH Research(GHRS) 등. 각 peer에 공유 근거(basis)와 **간단한 판단**"
    "(왜 peer인지 + 핵심 차이/주의)을 한 줄로. 제공된 유니버스 목록의 ticker를 우선 사용(투자 가능). "
    "유니버스에 없어도 명백한 표준치료/대표 peer면 포함하되 ticker를 정확히. 5~8개. 한국어 note. "
    "buy/sell 추천 단정 금지, 아이디어·관찰 위주. 반드시 suggest_peers 도구로만 답하라."
)


def suggest(ticker: str) -> dict:
    """{target:{ticker,name}, target_thesis, peers:[{ticker,name,basis,note,price,market_cap,
    in_universe}]}. 실패 시 {error}."""
    key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not key:
        return {"error": "ANTHROPIC_API_KEY 미설정"}
    tk = (ticker or "").strip()
    ctx = _target_context(tk)
    country = "KOR" if _is_kr(tk) else "USA"
    pool = _universe_pool(country)
    pool_str = "\n".join(f"{p['ticker']} {p['name']} [{p.get('industry') or ''}]" for p in pool[:400])
    user = (
        f"[대상] {ctx['name']} ({tk}) · 산업: {ctx.get('industry') or '-'}\n"
        f"[상승이유/투자포인트]\n{ctx.get('thesis') or '(캐시 없음 — 네 지식+뉴스로 파악)'}\n"
        f"[최근 뉴스 제목]\n" + "\n".join(f"- {t}" for t in ctx.get('news', [])) + "\n\n"
        f"[투자 가능 유니버스 후보(ticker name [industry])]\n{pool_str}\n\n"
        "위 대상과 투자포인트/적응증/기전/에셋을 공유하는 peer를 제시해."
    )
    import anthropic
    try:
        cl = anthropic.Anthropic(api_key=key)
        r = cl.messages.create(
            model=CLAUDE_MODEL, max_tokens=2000, system=_SYS,
            tools=[_TOOL], tool_choice={"type": "tool", "name": "suggest_peers"},
            messages=[{"role": "user", "content": user}])
        tu = next((b for b in r.content if b.type == "tool_use"), None)
        if not tu:
            return {"error": "도구 응답 없음"}
        out = tu.input
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}

    # peer 검증 + 가격/시총 부착
    import portfolio as pf
    peers = []
    seen = set()
    for p in (out.get("peers") or []):
        cand = (p.get("ticker") or "").strip() or (p.get("name") or "").strip()
        rt = pf._resolve_ticker(cand)            # 이름→코드/심볼 정규화
        if not rt or rt.upper() == tk.upper() or rt.upper() in seen:
            continue
        seen.add(rt.upper())
        row = None
        try:
            with connect() as c:
                row = c.execute("SELECT name, market_cap, country FROM ticker_master WHERE ticker=?",
                                (rt,)).fetchone()
        except Exception:
            pass
        price = None
        try:
            price = pf._fetch_current_price(rt)
        except Exception:
            pass
        peers.append({
            "ticker": rt, "name": (row["name"] if row else p.get("name") or rt),
            "basis": p.get("basis", ""), "note": p.get("note", ""),
            "price": price, "market_cap": (row["market_cap"] if row else None),
            "in_universe": bool(row),
        })
    return {"target": {"ticker": tk, "name": ctx["name"]},
            "target_thesis": out.get("target_thesis", ""), "peers": peers}
