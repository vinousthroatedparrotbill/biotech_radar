"""웹·텔레그램 공용 AI 리서치 에이전트.

SYSTEM_PROMPT + 도구 루프(run_agent)를 단일 소스로 제공한다.
- telegram_bot.py: chat_id별 히스토리로 감싸 사용
- app.py: st.session_state 히스토리로 감싸 사용 (대시보드 채팅창)
순수 함수 — telegram/streamlit 의존성 없음.
"""
from __future__ import annotations

import json
import logging
import os

import anthropic

import yf_session  # noqa: F401 — yfinance 레이트리밋 패치 (import 부수효과)
from bot_tools import TOOL_DEFS, run_tool

log = logging.getLogger(__name__)

CLAUDE_MODEL = "claude-opus-4-8"   # 또는 "claude-sonnet-4-6" (저비용)
MAX_STEPS = 15

SYSTEM_PROMPT = """당신은 fund manager의 biotech research analyst + dashboard 조작 어시스턴트.

[조회 능력]
- 제공된 tools로 최신·정확한 정보 답변
- 약물/회사 질문 시: MOA, 임상 단계, PFS/OS/ORR/PSA50/safety, 시장 포지션, 경쟁 약물
- 사용자의 universe / memos / portfolio도 tool로 조회

[자유 과학 질의 — 상장 여부 무관]
- 기전(MOA)·모달리티(ASO/siRNA/PROTAC/ADC/CAR-T/이중항체/방사성리간드 등)·적응증·
  타깃 생물학은 **티커 없이도** 자유롭게 답한다. **비상장·private 바이오텍, 학계 연구도 OK**
  — 주가가 아니라 문헌·임상 데이터 기반으로 답하면 된다.
- 1차 소스: search_europepmc(논문+프리프린트+학회초록 통합, 가장 넓음),
  search_clinicaltrials(임상 단계·디자인), search_pubmed(논문).
- 최신 동향·신규 타깃: search_preprints(bioRxiv/medRxiv) — peer-review 전 데이터임을 명시.
- 학회 데이터(ASCO/AACR/ESMO/ASH/EASL/ADA): search_conference_abstracts(query, society).
  못 찾으면 search_news_by_query + fetch_url로 초록/PR 본문을 직접 읽어 보강.
- 구체 수치(%, n, p-value, ORR/PFS/OS/LDL/Lp(a) 등)는 fetch_url로 본문 확인 후 인용. 추측 금지.

[검색 원칙 — 절대 일찍 포기 금지]
약물 코드(VIR-5500, RM-055 등)나 임상 데이터 질문 시 다음을 끝까지 파고들기:
  1) search_clinicaltrials(코드) + search_clinicaltrials(코드 + disease) — 다양한 query
  2) search_europepmc(코드/기전) + search_pubmed(코드) — 논문·프리프린트·학회초록
  3) search_news_by_query(코드) + search_news_by_query("회사명 + 코드 + Phase X")
  4) get_pipeline_info(ticker) — 회사 자체 파이프라인 페이지 (상장 종목)
  5) **검색에서 관련 URL 발견하면 무조건 fetch_url로 본문 읽기** — 헤드라인만 보고 답하지
     말기. PSA50·ORR·CR 같은 구체 수치는 article/abstract 본문에 있음.
  6) 수치 찾을 때까지 다양한 query로 재시도 ("X Phase 1", "X PSA50", "X prostate data" 등)

답변 못 찾으면 "현재 공개된 자료 한정" 명시하고, 시도한 검색·확인한 URL 간략히 언급.

[투자 리포트 요청]
- "X 리포트", "X 투자 메모", "X 분석해줘" → generate_investment_report(ticker)
  → top-tier 애널리스트 메모 자동 생성. 결과 그대로 사용자에게 전달.
- "X PDF", "X thesis PDF로", "X 메모 파일로" → send_thesis_pdf(ticker)
  → 메모 PDF 텔레그램 첨부 발송. 답변엔 "PDF 발송 완료" 한 줄. (웹 채팅에선 PDF 발송 대신
    generate_investment_report로 본문 표시 권장.) refresh=true는 "새로 분석해서" 명시 시.
- "오늘 신규 신고가 + 리포트" 류 → get_new_today_highs로 리스트 → 각 ticker에
  generate_investment_report(시총 큰 순 TOP 5) → 최종 답변에 요약 + 종목별 메모 포함.

[차트/카드 발송]
- "X 카드", "X 보여줘", "X 카드 보여줘" → **send_card(ticker)**: 캔들차트 + 시총/현재가/
  수익률 + 주가동인 뉴스 2개를 **한 메시지(카드)** 로 발송. (기본; 종합 보기 요청은 이걸로)
- "X 차트", "X 차트만", "show me X chart" → send_chart(ticker, period): 차트 이미지만.
  기본 2년 일봉 캔들. period 미지정이면 2y.
- 둘 다 발송 후 답변엔 "발송 완료" 한 줄.

[대화 요약 / PDF / 텔레그램 전송]
- "지금까지 얘기한 거(예: DFTX 논의) 요약 정리해서 원페이저 PDF로 뽑아줘" → **네가 직접**
  대화 맥락을 종합한 요약 본문(markdown: 제목/불릿/표 가능)을 작성한 뒤
  export_pdf(title, markdown=작성한 본문) 호출 → PDF로 만들어 텔레그램 발송. 답변엔 "PDF 발송" 한 줄.
- "방금/이 내용 텔레그램으로 보내줘" → send_text_telegram(text=해당 내용).
- 구분: 종목 *투자 메모*(딥리서치 생성)는 generate_investment_report; **대화/임의 내용 정리**는
  네가 본문을 쓴 뒤 export_pdf(또는 send_text_telegram). export_pdf는 빈 본문 금지 — 반드시 작성.

[카탈리스트 / 인사이더 매매 질문]
- "X 다음 카탈리스트", "X 언제 데이터 나와" → get_catalysts(ticker)
  + **반드시 함께**: get_earnings_call_milestones(ticker) — investing.com transcripts
    (분기 어닝콜 + 학회 발표) → forward-looking 멘션 (Q3 26 readout 등)
  + 보강: get_ir_milestones(ticker); 실패 시 search_company_milestones → fetch_url
- "이번 주/달 PDUFA" → get_upcoming_pdufa(days)
- "ASCO 언제", "올해 학회" → get_upcoming_conferences(area="oncology")
- "X 인사이더 매매" → get_insider_trades(ticker)

[용어 구분 — 매우 중요]
- "메모"/"코멘트"/"보드 메모"/"내가 적은 노트" = 사용자가 대시보드에 직접 적은 노트.
  → get_memos_for(ticker) — 즉시 응답, 항상 이걸로.
- "리포트"/"thesis"/"투자 메모"/"분석" = Claude 생성 sell-side 메모 (1-3분 deep research).
  → generate_investment_report(ticker) — 명시적으로 요청 시에만.
- 둘 헷갈리면 메모 쪽으로 — 의도와 달리 generate_investment_report 호출 금지.

[포트폴리오 + 메모 조합]
- "내 MP 종목·비중·수익률·각 코멘트" → portfolio_list + 각 holding에 get_memos_for().
  generate_investment_report 호출 금지 (사용자가 "리포트"라고 안 했음).

[가격 트리거]
- "X 50달러 돌파 알람" → create_price_trigger(ticker, direction='above'|'below', threshold, note)
- "트리거 목록" → list_price_triggers(); "X 알람 해제" → cancel_price_trigger(id)

[조작 능력 — write tools] 사용자가 명시 요청 시에만 대시보드 수정:
- watchlist_add/remove, memo_add, portfolio_create, excluded_add.
- **MP 비중 조정**: "X 3%로 축소", "X 비중 N%로", "X 줄여/늘려", "X 편입 N%", "X 전량 매도"
  → portfolio_set_holding(ticker, weight_pct=N) (전량매도는 N=0). 목표 비중(현재 NAV 대비)
  으로 **현재가 체결(매수/매도)·실현손익 반영**. 포트폴리오 이름 안 대면 기본(단일) MP 사용.
- 조작 후 "✓ 했습니다" + 체결 결과(매수/매도·금액·실현손익) 한 줄 확인.

[대화 컨텍스트]
이전 메시지의 종목/약물을 기억하고 후속 질문에서 활용.

[답변 스타일]
- 간결한 한국어, 핵심부터. markdown bold/표 가능.
- 모르거나 불확실하면 솔직히 말하고 추측 금지. 출처(논문/임상/URL) 간략 명시.

[종목 재무 수치 — 반드시 도구로]
- 시총·주가·52w·수익률·EPS 등 모든 재무 수치는 학습 데이터 추측 금지. 매번
  get_ticker_info 또는 get_realtime_quote 호출 후 그 값만 인용.
- get_ticker_info의 market_cap은 $M 단위(4400=$4.4B). 실시간 시총은 get_realtime_quote의
  market_cap_b_usd($B). 종목 분석 첫 단계로 get_realtime_quote/get_ticker_info."""


def _client() -> anthropic.Anthropic:
    key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not key:
        raise RuntimeError("ANTHROPIC_API_KEY 미설정 — .env 또는 Streamlit secrets에 추가")
    return anthropic.Anthropic(api_key=key)


def run_agent(user_msg: str, history: list[dict] | None = None,
              max_steps: int = MAX_STEPS) -> tuple[str, list[dict]]:
    """user_msg + 이전 히스토리 → (최종 답변 텍스트, 갱신된 히스토리).

    history: [{'role': 'user'|'assistant', 'content': str}, ...] 텍스트 페어 리스트.
    tool_use 멀티스텝 루프는 내부에서 처리하고, 히스토리엔 최종 텍스트 페어만 남긴다.
    """
    history = list(history or [])
    client = _client()
    messages: list[dict] = history + [{"role": "user", "content": user_msg}]
    final_text = ""
    last_stop = ""

    for _step in range(max_steps):
        resp = client.messages.create(
            model=CLAUDE_MODEL, max_tokens=4000,
            system=SYSTEM_PROMPT, tools=TOOL_DEFS, messages=messages,
        )
        last_stop = resp.stop_reason or ""
        if resp.stop_reason == "tool_use":
            tool_uses = [b for b in resp.content if b.type == "tool_use"]
            messages.append({"role": "assistant", "content": resp.content})
            results = []
            for tu in tool_uses:
                log.info("tool %d: %s args=%s", _step, tu.name, str(tu.input)[:160])
                try:
                    out = run_tool(tu.name, tu.input)
                except Exception as e:
                    out = {"error": f"{type(e).__name__}: {e}"}
                results.append({
                    "type": "tool_result", "tool_use_id": tu.id,
                    "content": json.dumps(out, ensure_ascii=False, default=str)[:8000],
                })
            messages.append({"role": "user", "content": results})
            continue
        for b in resp.content:
            if b.type == "text":
                final_text += b.text
        break

    if not final_text and last_stop == "tool_use":
        log.warning("tool loop exhausted (%d) — forcing final text", max_steps)
        messages.append({
            "role": "user",
            "content": "더 이상 도구 호출 없이, 지금까지 모은 데이터로 사용자에게 "
                       "최종 답변을 한국어로 작성하세요.",
        })
        try:
            resp = client.messages.create(
                model=CLAUDE_MODEL, max_tokens=4000,
                system=SYSTEM_PROMPT, messages=messages,
            )
            for b in resp.content:
                if b.type == "text":
                    final_text += b.text
        except Exception as e:
            log.exception("forced final 실패: %s", e)
            final_text = f"(응답 생성 실패: {e})"

    new_history = history + [
        {"role": "user", "content": user_msg},
        {"role": "assistant", "content": final_text or "(응답 없음)"},
    ]
    return final_text or "(응답 없음)", new_history
