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


SYSTEM_PROMPT = """당신은 Goldman Sachs / Morgan Stanley / Cowen / Leerink급 senior biotech 애널리스트.
펀드매니저에게 보낼 institutional-quality 심층 리포트 작성.

[작업 흐름 — 반드시 도구 사용]
제공 데이터(우리 DB)는 시작점일 뿐. 다음 도구로 적극 조사 후 작성:
1) get_pipeline_info(ticker) — 회사 파이프라인 페이지 본문 — 각 자산의 단계·적응증
2) search_clinicaltrials(자산명/적응증) — 임상 단계, 사이즈, 디자인
3) search_pubmed(자산명) — peer-reviewed 데이터 (LDL %, ORR, PFS 등 구체 수치)
4) search_news_by_query(자산명 또는 "competitor + 적응증") — 최근 데이터 readout
5) fetch_url — 검색에서 나온 PR/투자 자료 본문을 직접 읽어 LDL/Lp(a)/HDL 같은 구체 %, 시장 사이즈,
   경쟁 약물 데이터 확보. 헤드라인만 보고 추측 금지.
6) 경쟁 약물 조사 — "메인 자산이 X이면 X target / X 적응증의 경쟁 약물 Y, Z 모두 검색해서
   head-to-head 차별점 표/논의" 작성.

[형식]
- 한국어. 약물명·기전·회사명·임상명은 영문 유지 (ARO-MAPT, PSMA, CETP, PREVAIL 등).
- 분량 제한 없음 — substance 우선. Markdown.
- 표 사용 권장 (head-to-head 비교).

[필수 섹션 — 빠짐없이]

# {회사명} ({TICKER}) — 투자 메모

## 1) 투자 포인트 (Thesis) — 4-6줄
이 종목 매력 핵심. 메인 자산·기전·시장 사이즈·차별 포인트 구체적으로.

## 2) 메인 파이프라인 — 자산별 정리
각 핵심 자산:
- **자산명** (코드명) — 적응증, MOA, 단계, 핵심 데이터(% 수치)
- 시장 사이즈, peak sales 컨센서스 (있다면)
- 가까운 카탈리스트 (자산별 readout 일자)
파이프라인 4-7개 자산 다룰 것.

## 3) 경쟁 파이프라인 — head-to-head 차별 분석 ★ 가장 중요 ★
메인 자산 각각에 대해 같은 target / 같은 적응증의 경쟁 약물 조사·비교:
- 표 권장: 자산 | 회사 | MOA | Phase | 핵심 데이터 | readout 일정
- **차별 포인트 토론**: efficacy, safety, dosing, ROA, 환자 segment 분할,
  manufacturing/COGS, 가격, first-mover timing 등 다각도.
- 우리 자산이 어디서 우위/열위인지 솔직하게.
- 시장 구도 시나리오 (양분 / 대체 / 보완).

## 4) 최근 주가 동향 + 상승 이유
1D/1M/3M/1Y 수익률 + 왜 올랐는지. 최근 60일 fundamental 뉴스에서 드라이버 식별.

## 5) 카탈리스트 워치 — 자산별 가까운 순
- [일자] [자산]: 데이터/이벤트 + base case 시나리오 (성공/실패 시 주가 영향)
- fuzzy 일자는 그대로 ("Q3 2026", "1H 2027").

## 6) 인사이더 매매 시그널
180일 net buy/sell + 해석.

## 7) 리스크 포인트
3-5개 — 임상/경쟁/재무/규제/IP 각각 구체.

## 8) 바텀라인
종합 결론 — 핵심 변곡점, 추적 포인트, 시나리오.

[원칙]
- **펀더멘탈 데이터 풍부하게**: %, p-value, n, market size, peak sales 같은 구체 숫자
  도구로 확보 후 인용. "효능 좋음" 같은 추상 형용사 금지.
- 경쟁 분석 절대 빠뜨리지 말기. 모든 메인 자산에 경쟁 약물 1-3개 비교.
- 데이터 없으면 "공개 데이터 없음" 명시 — 추측·hallucination 금지.
- buy/sell 추천 표현 금지. 사실/시그널/관찰/시나리오 형태로 서술.
- "변명 불가", "압도적", "olympics" 같은 솔직한 톤 OK — sell-side analyst 톤."""


def generate(ticker: str, max_tool_calls: int = 15) -> str:
    """ticker → 도구 사용한 심층 투자 메모. Claude가 fetch_url/search_pubmed/
    search_clinicaltrials/get_pipeline_info 등 자유롭게 호출해 경쟁 약물·구체 데이터 조사."""
    import json as _json
    from bot_tools import TOOL_DEFS, run_tool

    ctx = _gather_context(ticker)
    api_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    context_str = _serialize_context(ctx)

    if not api_key:
        return f"*{ctx['name']} ({ticker})*\n\n{context_str}"

    user_msg = (
        f"종목 {ticker} ({ctx['name']})에 대한 institutional-quality 투자 메모 작성.\n\n"
        f"[우리 DB 컨텍스트 — 시작점, 추가 도구 조사 필수]\n{context_str}\n\n"
        "[작업 지침]\n"
        "1. get_pipeline_info(ticker)로 메인 자산 4-7개 파악\n"
        "2. 각 메인 자산에 대해 search_pubmed / search_clinicaltrials / "
        "search_news_by_query로 구체 데이터(%, n, p-value 가능시) 확보\n"
        "3. 각 메인 자산의 경쟁 약물(같은 target 또는 같은 적응증) 1-3개 조사 — "
        "search_news_by_query('경쟁자산명 phase 결과') + fetch_url로 PR 본문 읽기\n"
        "4. head-to-head 비교 표 + 차별 토론 포함\n"
        "5. 시스템 프롬프트의 8개 섹션 모두 작성\n"
        "도구 사용 끝나면 최종 메모만 텍스트로 출력."
    )
    messages: list[dict] = [{"role": "user", "content": user_msg}]

    try:
        client = anthropic.Anthropic(api_key=api_key)
        final_text = ""
        for step in range(max_tool_calls):
            resp = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=8000,
                system=SYSTEM_PROMPT,
                tools=TOOL_DEFS,
                messages=messages,
            )
            if resp.stop_reason == "tool_use":
                tool_uses = [b for b in resp.content if b.type == "tool_use"]
                messages.append({"role": "assistant", "content": resp.content})
                tool_results = []
                for tu in tool_uses:
                    log.info("[%s] tool: %s args=%s", ticker, tu.name,
                             str(tu.input)[:120])
                    try:
                        result = run_tool(tu.name, tu.input)
                    except Exception as e:
                        result = {"error": str(e)}
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tu.id,
                        "content": _json.dumps(result, ensure_ascii=False,
                                               default=str)[:10000],
                    })
                messages.append({"role": "user", "content": tool_results})
                continue
            for b in resp.content:
                if b.type == "text":
                    final_text += b.text
            break
        log.info("%s 메모 생성 완료 — tool_call %d step", ticker, step)
        return final_text or "(메모 생성 실패 — 응답 없음)"
    except Exception as e:
        log.exception("Claude 리포트 실패: %s", e)
        return f"*{ctx['name']} ({ticker})*\n\n{context_str}\n\n_(Claude 실패: {e})_"


def get_cached_report(ticker: str) -> dict | None:
    """ai_reports 테이블에서 캐시된 리포트 조회. 없으면 None."""
    with db.connect() as conn:
        row = conn.execute(
            "SELECT body, generated_at, model FROM ai_reports WHERE ticker = ?",
            (ticker.upper(),),
        ).fetchone()
    return dict(row) if row else None


def save_report(ticker: str, body: str) -> None:
    import datetime as dt
    now = dt.datetime.now().isoformat(timespec="seconds")
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO ai_reports (ticker, body, generated_at, model) "
            "VALUES (?,?,?,?) "
            "ON CONFLICT (ticker) DO UPDATE SET "
            "body = EXCLUDED.body, generated_at = EXCLUDED.generated_at, "
            "model = EXCLUDED.model",
            (ticker.upper(), body, now, CLAUDE_MODEL),
        )


def generate_and_save(ticker: str) -> dict:
    """generate() 호출 + DB 캐시 저장."""
    text = generate(ticker)
    save_report(ticker, text)
    return {"ticker": ticker.upper(), "body": text,
            "generated_at": __import__("datetime").datetime.now().isoformat(timespec="seconds")}


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
