"""신규 신고가 종목용 자동 투자 리포트 — 애널리스트 메모 스타일.

흐름
1) ticker별 데이터 수집:
   - ticker_master / high_low_cache (회사 정보 + 가격 + 멀티 기간 수익률)
   - catalysts (next 365d, type별 분류 — pdufa/clinical_readout/regulatory/etc)
   - insiders (최근 180d 매매 요약)
   - 펀더멘탈 뉴스 (60일, app.py와 동일 필터)
   - top_pipelines (180일 멘션 TOP 3)
2) Claude API로 sell-side 애널리스트 스타일 메모 1page 생성 (한국어, 5-8줄)
3) HTML/Telegram 포맷으로 반환
"""
from __future__ import annotations

import logging
import os
import re
from typing import Any

import anthropic

import db

log = logging.getLogger(__name__)

CLAUDE_MODEL = "claude-opus-4-7"

# app.py와 동일한 펀더멘탈 필터
_FUND_PAT = re.compile(
    r"(phase\s*[123][a-z]?|topline|interim|primary endpoint|"
    r"readout|data\s+(?:read|release|disclosure|update)|"
    r"\bfda\b|pdufa|adcom|advisory committee|approval|approve[ds]?|"
    r"breakthrough designation|orphan designation|priority review|"
    r"crl|complete response letter|"
    r"snda|sbla|nda\b|bla\b|ind\b|"
    r"acquir(?:e|ed|es|ition)|merger|partner|partnership|collaboration|"
    r"licens(?:e|ed|es|ing)|deal\b|"
    r"first patient|first dose|enrollment|"
    r"survival|response rate|orr\b|pfs\b|os\b|"
    r"clinical trial|study results?|safety|efficacy)",
    re.IGNORECASE,
)
_NOISE_PAT = re.compile(
    r"(price target|analyst|upgrad|downgrad|consensus|estimate|"
    r"insider (?:bought|sold)|filed form 4|"
    r"options activity|unusual options|short interest|"
    r"(?:eps|revenue) (?:beat|miss|estimate)|"
    r"top \d+ stocks?|stocks? to (?:buy|watch)|trending|moving|gainers?|losers?|"
    r"premarket|after hours|benzinga)",
    re.IGNORECASE,
)


def _fundamental_news(ticker: str, days: int = 60, limit: int = 5) -> list[dict]:
    from news import fetch_finviz_news, fetch_yahoo_news
    items = list(fetch_finviz_news(ticker, days=days)) + list(fetch_yahoo_news(ticker))
    items.sort(key=lambda it: it.get("_published_dt") or 0, reverse=True)
    out = []
    seen = set()
    for it in items:
        t = (it.get("title") or "").strip()
        if not t or len(t) < 15:
            continue
        if _NOISE_PAT.search(t):
            continue
        if not _FUND_PAT.search(t):
            continue
        norm = " ".join(sorted(set(t.lower().split())))[:80]
        if norm in seen:
            continue
        seen.add(norm)
        out.append(it)
        if len(out) >= limit:
            break
    return out


def _gather_context(ticker: str) -> dict[str, Any]:
    import catalysts as cat
    import insiders as ins
    from news import top_pipelines

    tk = ticker.upper()
    with db.connect() as conn:
        info = conn.execute(
            "SELECT name, market_cap, industry FROM ticker_master WHERE ticker = ?",
            (tk,),
        ).fetchone()
        snap = conn.execute(
            "SELECT today_close, perf_1d, perf_1m, perf_3m, perf_1y, high_52w "
            "FROM high_low_cache WHERE ticker = ? ORDER BY computed_date DESC LIMIT 1",
            (tk,),
        ).fetchone()

    name = (info.get("name") if info else "") or tk
    catalysts_df = cat.get_catalysts(ticker=tk, days=365)
    insider_summary = ins.summary_for_ticker(tk, days=180)
    news = _fundamental_news(tk, days=60, limit=5)
    try:
        pipelines = top_pipelines(tk, name, days=180) or []
    except Exception:
        pipelines = []

    return {
        "ticker": tk,
        "name": name,
        "info": info,
        "snap": snap,
        "catalysts": catalysts_df,
        "insider": insider_summary,
        "news": news,
        "pipelines": pipelines,
    }


def _serialize_context(ctx: dict) -> str:
    parts: list[str] = []
    info = ctx.get("info")
    if info:
        mcap_b = (info.get("market_cap") or 0) / 1000
        parts.append(
            f"회사: {info.get('name')} ({ctx['ticker']}), 시총 ${mcap_b:.1f}B, "
            f"업종 {info.get('industry') or '—'}"
        )
    snap = ctx.get("snap")
    if snap:
        parts.append(
            f"현재가: ${snap.get('today_close', 0):.2f} · "
            f"1D {snap.get('perf_1d') or 0:+.1f}% · "
            f"1M {snap.get('perf_1m') or 0:+.1f}% · "
            f"3M {snap.get('perf_3m') or 0:+.1f}% · "
            f"1Y {snap.get('perf_1y') or 0:+.1f}%"
        )
    cat_df = ctx.get("catalysts")
    if cat_df is not None and not cat_df.empty:
        parts.append("\n다가오는 카탈리스트 (next 365d, 가까운 순):")
        for _, r in cat_df.head(20).iterrows():
            ev_type = r.get("event_type", "")
            desc = r.get("description")
            if not isinstance(desc, str):
                desc = ""
            date_hint = ""
            m = re.search(r"date_hint:\s*([^·]+)", desc)
            if m:
                date_hint = f" ({m.group(1).strip()})"
            parts.append(
                f"  - [{r['event_date']}{date_hint}] [{ev_type}] "
                f"{(r.get('title') or '')[:240]}"
            )
    ins_s = ctx.get("insider") or {}
    if ins_s.get("trades", 0) > 0:
        parts.append(
            f"\n인사이더 매매 (180일): {ins_s['trades']}건 · "
            f"매수 ${ins_s.get('buy_value', 0)/1e6:.1f}M · "
            f"매도 ${abs(ins_s.get('sell_value', 0))/1e6:.1f}M · "
            f"net ${ins_s.get('net_value', 0)/1e6:+.1f}M"
        )
    news = ctx.get("news") or []
    if news:
        parts.append("\n최근 fundamental 뉴스 (60일):")
        for n in news:
            pub = (n.get("published") or "")[:10]
            parts.append(f"  - [{pub}] {n.get('title', '')[:200]}")
    pl = ctx.get("pipelines") or []
    if pl:
        parts.append("\n파이프라인 멘션 (180일 TOP):")
        for p in pl[:3]:
            d = p.get("drug") or ""
            m = p.get("mentions") or 0
            parts.append(f"  - {d} ({m}건)")
    return "\n".join(parts)


SYSTEM_PROMPT = """당신은 Goldman Sachs / Morgan Stanley급 sell-side biotech 애널리스트.
펀드매니저에게 보낼 institutional-quality 투자 메모 작성.

[형식]
- 한국어 작성. 약물명·기전·회사명은 영문 유지 (예: ARO-MAPT, PSMA, TCE).
- 분량: 약 15-25줄 (충분한 substance).
- markdown bold(**)·이탤릭(*) 사용. 텔레그램 HTML로 자동 변환됨.
- 회사명/티커 헤더로 시작.

[구조 — 모든 섹션 포함]

**1) 투자 포인트 (Thesis)**
3-4줄. 이 종목의 투자 매력 핵심. 어떤 자산이 movin needle인가, 시장 사이즈, MOA edge,
경쟁 포지셔닝. 추상적 형용사 금지 — 구체적 자산·기전·target market 언급.

**2) 최근 주가 동향 + 상승 이유**
2-3줄. 1D/1M/3M/1Y 수익률 인용 + 왜 올랐는지 (recent news, catalyst, deal 중 무엇이
드라이버였는지). 데이터에 명시적 단서 있으면 인용, 없으면 "최근 60일 fundamental
뉴스 부족 — 모멘텀 sustained 아님" 식으로 기록.

**3) 카탈리스트 워치 포인트**
가까운 순으로 핵심 3-5개 (날짜 + 자산명 + 드라이버):
- [날짜] [자산/이벤트]: 무엇이 나오나, 왜 중요한가 (시장 사이즈, 경쟁 비교, base case 시나리오)
- 일자 fuzzy면 그대로 인용 ("late 2026", "Q3 26")

**4) 인사이더 매매 시그널**
2줄. 180일 net buy/sell 금액. 해석: 매수 우세면 confidence, 매도 우세면 caution.
0건이면 "최근 6M 인사이더 거래 없음" — 중립.

**5) 리스크 포인트**
3개 — 각각 한 줄:
- 임상 리스크 (구체 자산 + 실패 시 영향)
- 경쟁/시장 리스크 (경쟁사·기전 대안)
- 재무/규제 리스크 (cash runway, FDA 우려, IP 등)

**6) Bottom Line**
1줄 — 핵심 한 문장 요약 (이번 분기/연도 추적 포인트).

[원칙]
- 데이터에 없는 내용 절대 만들지 말기 (M&A 가능성, 경쟁사 비교 등 hallucination 금지).
- 카탈리스트 일자/제목은 제공된 그대로 인용.
- buy/sell 추천 표현 금지 (compliance). 사실/시그널/관찰 형태로 서술.
- 너무 conservative 금지 — 펀드매니저 의사결정에 가치 있는 insight 제공."""


def generate(ticker: str) -> str:
    """ticker → 투자 메모 텍스트 (텔레그램 markdown). API 키 없으면 fallback 요약."""
    ctx = _gather_context(ticker)
    api_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    context_str = _serialize_context(ctx)

    if not api_key:
        # API 없으면 raw 컨텍스트 그대로 (포맷팅만)
        return f"*{ctx['name']} ({ticker})*\n\n{context_str}"

    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=2500,
            system=SYSTEM_PROMPT,
            messages=[{
                "role": "user",
                "content": (f"다음 데이터를 기반으로 institutional-quality 투자 메모 작성:\n\n"
                            f"{context_str}\n\n"
                            f"메모는 위에 명시된 6개 섹션 모두 포함."),
            }],
        )
        text = ""
        for b in resp.content:
            if b.type == "text":
                text += b.text
        return text or "(리포트 생성 실패)"
    except Exception as e:
        log.exception("Claude 리포트 실패: %s", e)
        return f"*{ctx['name']} ({ticker})*\n\n{context_str}\n\n_(Claude 리포트 생성 실패: {e})_"


def generate_for_tickers(tickers: list[str], max_n: int = 5) -> list[dict]:
    """여러 ticker 일괄 생성. 시총 큰 순으로 max_n개만 (속도/비용)."""
    if not tickers:
        return []
    # mcap 큰 순으로 정렬
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT ticker, market_cap FROM ticker_master "
            "WHERE ticker = ANY(%s) ORDER BY market_cap DESC NULLS LAST",
            ([t.upper() for t in tickers],),
        ).fetchall()
    sorted_tickers = [r["ticker"] for r in rows][:max_n]
    out = []
    for tk in sorted_tickers:
        try:
            text = generate(tk)
            out.append({"ticker": tk, "report": text})
        except Exception as e:
            log.exception("%s 리포트 실패", tk)
            out.append({"ticker": tk, "report": f"(실패: {e})"})
    return out


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    tk = sys.argv[1] if len(sys.argv) > 1 else "ARWR"
    print(f"=== {tk} 투자 메모 ===\n")
    print(generate(tk))
