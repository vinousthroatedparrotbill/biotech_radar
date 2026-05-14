"""Telegram bot listener — 인터랙티브 AI research analyst.
명령어 매칭 + Claude API 자유 텍스트 분석 + tool calling.

실행: python telegram_bot.py
종료: Ctrl+C
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import anthropic
from dotenv import load_dotenv
from telegram import Update
from telegram.constants import ChatAction, ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ContextTypes, filters,
)

from bot_tools import TOOL_DEFS, run_tool

load_dotenv(Path(__file__).parent / ".env", override=True)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

CLAUDE_MODEL = "claude-opus-4-7"   # 또는 "claude-sonnet-4-6" (저비용)
SYSTEM_PROMPT = """당신은 fund manager의 biotech research analyst + dashboard 조작 어시스턴트.

[조회 능력]
- 제공된 tools로 최신·정확한 정보 답변
- 약물/회사 질문 시: MOA, 임상 단계, PFS/OS/ORR/PSA50/safety, 시장 포지션, 경쟁 약물
- 사용자의 universe / memos / portfolio도 tool로 조회

[검색 원칙 — 절대 일찍 포기 금지]
약물 코드(VIR-5500, RM-055 등)나 임상 데이터 질문 시 다음 순서로 끝까지 파고들기:
  1) search_clinicaltrials(코드) + search_clinicaltrials(코드 + disease) — 다양한 query
  2) search_news_by_query(코드) + search_news_by_query("회사명 + 코드 + Phase X")
  3) search_pubmed(코드) — 학술 논문
  4) get_pipeline_info(ticker) — 회사 자체 파이프라인 페이지
  5) **검색에서 관련 URL 발견하면 무조건 fetch_url로 본문 읽기** — 헤드라인만 보고 답하지 말기. PSA50·ORR·CR 같은 구체 수치는 article 본문에 있음.
  6) 수치 찾을 때까지 다양한 query로 search_news_by_query 재시도 ("VIR-5500 Phase 1", "VIR-5500 PSA50", "VIR-5500 prostate cancer data" 등)

답변 못 찾으면 "현재 공개된 자료 한정" 명시하고, 시도한 검색·확인한 URL 간략히 언급.

[투자 리포트 요청]
- "X 리포트", "X 투자 메모", "X 분석해줘" → generate_investment_report(ticker)
  → top-tier 애널리스트 메모 (thesis / 최근 주가 동향 / 카탈리스트 워치 / 인사이더 /
    리스크 / bottom line) ~15-25줄 자동 생성. 결과 그대로 사용자에게 전달.
- "X PDF", "X thesis PDF로", "X 메모 파일로", "X report as PDF" → send_thesis_pdf(ticker)
  → 메모 PDF 텔레그램에 첨부 발송. 도구가 직접 sendDocument 함 — 답변에는 "PDF 발송 완료"
    한 줄 정도만. refresh=true 옵션은 사용자가 "새로 분석해서" 명시 시.
- "오늘 신규 신고가 + 리포트", "신고가 종목들 분석해서 보내줘" 류 → 1) get_new_today_highs로
  리스트 확보 → 2) 각 ticker에 generate_investment_report 호출(시총 큰 순 TOP 5) →
  최종 답변에 리스트 요약 + 종목별 메모 차례로 포함.

[카탈리스트 / 인사이더 매매 질문]
- "X 다음 카탈리스트", "X 다가오는 일정", "X 언제 데이터 나와" → get_catalysts(ticker)
  + **반드시 함께**: get_earnings_call_milestones(ticker) — investing.com transcripts
    (분기 어닝콜 + Leerink/JPM/TD Cowen/Goldman 등 학회 발표) 자동 수집 →
    forward-looking 멘션 (Q3 26 readout, 2H 26 initial data 등)
  + 보강: get_ir_milestones(ticker) — IR 자료 PDF에서 추출 (접근 가능시)
  + 추출 실패 시 search_company_milestones → fetch_url로 PR 본문 직접 읽기
- "이번 주/달 PDUFA", "다가오는 PDUFA" → get_upcoming_pdufa(days)
- "ASCO 언제", "올해 학회" → get_upcoming_conferences(area="oncology")
- "X 인사이더 매매", "CEO 매매", "내부자 사고 있어?" → get_insider_trades(ticker)
  → 매수(P-Purchase)와 매도(S-Sale) 합 비교, net_value 양수면 강한 시그널

[조작 능력 — write tools]
사용자가 명시 요청하면 대시보드를 직접 수정:
- "X 관심종목에 추가" → watchlist_add
- "X 관심종목 해제" → watchlist_remove
- "X에 [메모내용] 적어줘" → memo_add
- "MP1에 X 비중 N%로" → portfolio_set_holding
- "MP1에서 X 빼줘" → portfolio_remove_holding
- "X는 비-biotech이니까 제외" → excluded_add
- 새 포트폴리오 → portfolio_create
조작 후 "✓ N를 ~~ 했습니다" 짧게 확인.

[대화 컨텍스트]
이전 메시지의 종목/약물을 기억하고 후속 질문에서 활용.
예: "VIR 알려줘" → 답변 → "지금 주가는?" → VIR의 주가로 이해.

[답변 스타일]
- 간결한 한국어, 핵심부터
- 텔레그램 답변이라 markdown bold 가능, 표는 monospace
- 모르거나 불확실하면 솔직히 말하고 추측 금지

[종목 재무 수치 — 반드시 도구로]
- 시총·주가·52w 고가/저가·1D/1M/1Y 수익률·EPS 등 **모든 재무 수치는 절대 학습 데이터로
  추측하지 말 것**. 매번 get_ticker_info 또는 get_realtime_quote 호출 후 그 값만 인용.
- 시총 단위 주의: get_ticker_info의 market_cap은 **$M (백만달러)** 단위. 4400 = $4.4B.
  실시간 정확한 시총은 get_realtime_quote의 market_cap_b_usd (이미 $B 변환됨).
- 종목 thesis 작성·종목 분석 시 반드시 첫 단계: get_realtime_quote 또는 get_ticker_info."""


def _allowed_user_ids() -> set[str]:
    """인가된 user_id 집합.
    .env의 TELEGRAM_AUTHORIZED_USERS(콤마 구분) 우선, 없으면 TELEGRAM_CHAT_ID 기본 인가.
    user_id 기반이라 DM/그룹/채널 어디서든 인가된 사람만 답함."""
    raw = (os.environ.get("TELEGRAM_AUTHORIZED_USERS") or "").strip()
    if raw:
        return {x.strip() for x in raw.split(",") if x.strip()}
    chat_id = (os.environ.get("TELEGRAM_CHAT_ID") or "").strip()
    return {chat_id} if chat_id else set()


def _is_authorized(update: Update) -> bool:
    """메시지 보낸 user 기준 인가. 그룹에서도 본인 메시지면 통과."""
    user = update.effective_user
    if not user:
        return False
    return str(user.id) in _allowed_user_ids()


# 호환 (기존 코드에서 호출되면 같은 의미로 동작)
def _allowed_chat_id() -> str:
    return (os.environ.get("TELEGRAM_CHAT_ID") or "").strip()


# ───────────────────────── 명령어 핸들러 (DB 조회) ─────────────────────────
async def cmd_start(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_authorized(update):
        return   # 비인가 user는 silent ignore (DM이든 그룹이든)
    await update.message.reply_text(
        "🧬 Biotech Research Analyst Bot\n\n"
        "**키워드 명령어**:\n"
        "• `신고가` — 오늘 52w 신고가 리스트\n"
        "• `관심종목` — watchlist + 메모\n"
        "• `상승폭` — 오늘 1D 상승률 TOP\n"
        "• `포트폴리오` — Model Portfolio 현황\n"
        "• `오늘뉴스` — 데일리 바이오 뉴스\n\n"
        "**자유 질문**: 약물명·회사·임상·기전 등 자연어로 묻기.\n"
        "예: `ublituximab 알려줘` / `RVMD 파이프라인 분석`\n\n"
        "**대화 컨텍스트**: 직전 6턴까지 기억. /reset 으로 초기화.",
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_reset(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_authorized(update):
        return
    chat_id = update.message.chat.id
    _reset_history(chat_id)
    await update.message.reply_text("✓ 대화 히스토리 초기화됨.")


async def _send_high(update: Update) -> None:
    from collectors.high_low import fetch_new_today_highs, fetch_new_highs
    df = fetch_new_today_highs(limit=20)
    label = "🆕 오늘 신규 신고가"
    if df.empty:
        df = fetch_new_highs("high", limit=20)
        label = "📈 전체 52w 신고가"
    if df.empty:
        await update.message.reply_text("신고가 데이터 없음. 대시보드에서 갱신 필요.")
        return
    lines = [f"<b>{label}</b> ({len(df)}종목)\n"]
    lines.append("<pre>")
    lines.append(f"{'Ticker':6} {'Name':22} {'Price':>9} {'1D':>7}")
    for _, r in df.head(20).iterrows():
        n = (r.get("name") or "")[:22]
        p = f"${r['close']:,.2f}" if r.get("close") else "—"
        d = f"{r['perf_1d']:+.1f}%" if r.get("perf_1d") is not None else "—"
        lines.append(f"{r['ticker']:6} {n:22} {p:>9} {d:>7}")
    lines.append("</pre>")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def _send_watchlist(update: Update) -> None:
    import watchlist as wl
    df = wl.list_all()
    if df.empty:
        await update.message.reply_text("관심종목 비어있음.")
        return
    lines = [f"<b>⭐ 관심종목</b> ({len(df)}종목)\n"]
    from memo import list_for
    for _, r in df.iterrows():
        tk = r["ticker"]
        nm = (r.get("name") or "")[:30]
        cp = r.get("close")
        ret = r.get("perf_1d")
        price_str = f"${cp:,.2f}" if cp else "—"
        ret_str = f" ({ret:+.1f}%)" if ret is not None else ""
        lines.append(f"<b>{tk}</b> {nm}\n  {price_str}{ret_str}")
        memos = list_for(tk)
        for m in memos[:2]:
            body = m["body"]
            if len(body) > 80:
                body = body[:80] + "…"
            lines.append(f"  📝 {m['created_at'][:10]}: {body}")
        lines.append("")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def _send_top_movers(update: Update) -> None:
    from collectors.high_low import fetch_top_movers
    df = fetch_top_movers(limit=15, min_mcap=500, min_perf=5)
    if df.empty:
        await update.message.reply_text("조건에 맞는 상승폭 종목 없음.")
        return
    lines = [f"<b>🚀 상승폭 최대</b> (시총≥$500M, +5% 이상, {len(df)}종목)\n"]
    lines.append("<pre>")
    lines.append(f"{'Ticker':6} {'Name':22} {'1D':>7} {'Mcap':>8}")
    for _, r in df.iterrows():
        n = (r.get("name") or "")[:22]
        d = f"{r['perf_1d']:+.1f}%"
        mc = r.get("market_cap") or 0
        mc_s = f"${mc/1000:.1f}b" if mc >= 1000 else f"${mc:.0f}m"
        lines.append(f"{r['ticker']:6} {n:22} {d:>7} {mc_s:>8}")
    lines.append("</pre>")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def _send_portfolios(update: Update) -> None:
    import portfolio as pf
    ports = pf.list_all()
    if not ports:
        await update.message.reply_text("아직 포트폴리오 없음.")
        return
    lines = [f"<b>💼 Model Portfolio</b> ({len(ports)}개)\n"]
    for p in ports:
        s = pf.summary(p["id"])
        ret = s.get("return_pct", 0)
        sign = "🟢" if ret >= 0 else "🔴"
        lines.append(
            f"<b>{p['name']}</b>\n"
            f"  ${s['current_size']/1e6:,.2f}M  {sign} {ret:+.2f}%  "
            f"({len(s['holdings'])}종목, 편입 {s['total_weight']:.0f}%)"
        )
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def _send_daily_news(update: Update) -> None:
    from news import fetch_global_healthcare_news
    items = fetch_global_healthcare_news(days=1, max_items=20)
    if not items:
        await update.message.reply_text("오늘 매칭된 데일리 뉴스 없음.")
        return
    lines = [f"<b>📰 데일리 바이오 뉴스</b> ({len(items)}건)\n"]
    for it in items[:15]:
        cats = "/".join(it["categories"])
        tks = ",".join(it["tickers"][:2]) if it["tickers"] else ""
        tks_s = f" · {tks}" if tks else ""
        lines.append(f"<b>[{cats}]</b>{tks_s}\n  <a href=\"{it['link']}\">{it['title'][:120]}</a>")
    await update.message.reply_text(
        "\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=True,
    )


# ───────────────────────── 키워드 라우팅 ─────────────────────────
async def _handle_keyword(update: Update, msg_lower: str) -> bool:
    """키워드 매칭되면 처리하고 True. 안 되면 False (Claude로 넘김).
    단순 단일-인텐트 메시지만 fast-path. '리포트', '분석', '메모', '왜', '어떻게',
    '비교', '추가', '제외' 같은 추가 의도어가 함께 있으면 Claude로 넘김 (도구 호출)."""
    intent_words = ("리포트", "분석", "메모", "투자 메모", "report", "analyze",
                    "왜", "이유", "어떻게", "비교", "compare", "vs",
                    "추가", "제외", "삭제", "변경", "넣어", "빼", "바꿔",
                    "써줘", "적어줘", "찾아", "알려줘", "조사")
    if any(w in msg_lower for w in intent_words):
        return False   # Claude가 도구 조합으로 처리

    if any(k in msg_lower for k in ("신고가", "52w high", "52주 신고가")):
        await _send_high(update); return True
    if any(k in msg_lower for k in ("관심종목", "watchlist", "워치리스트")):
        await _send_watchlist(update); return True
    if any(k in msg_lower for k in ("상승폭", "top mover", "오늘 상승")):
        await _send_top_movers(update); return True
    if any(k in msg_lower for k in ("포트폴리오", "portfolio", "model portfolio", "mp")):
        await _send_portfolios(update); return True
    if any(k in msg_lower for k in ("오늘뉴스", "데일리뉴스", "daily news", "biotech news")):
        await _send_daily_news(update); return True
    return False


# ───────────────────────── Claude API + tool calling ─────────────────────────
def _claude_client():
    api_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY가 .env에 없음")
    return anthropic.Anthropic(api_key=api_key)


# 대화 히스토리 (chat_id별, 메모리 인메모리). 봇 재시작 시 초기화됨.
# 형식: 최종 user/assistant 텍스트만 (tool_use 중간 단계는 제외 — 토큰 절약)
_chat_history: dict[int, list[dict]] = {}
MAX_HISTORY_TURNS = 6   # user-assistant 쌍 6개 = 메시지 12개까지 유지


async def _ask_claude(user_msg: str, chat_id: int) -> str:
    """Claude에게 tool 사용 권한 + 대화 히스토리 주고 답변 받기."""
    client = _claude_client()
    history = _chat_history.get(chat_id, [])
    messages: list[dict] = list(history) + [{"role": "user", "content": user_msg}]

    final_text = ""
    MAX_STEPS = 15
    last_stop_reason = ""
    for _step in range(MAX_STEPS):
        resp = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=4000,
            system=SYSTEM_PROMPT,
            tools=TOOL_DEFS,
            messages=messages,
        )
        last_stop_reason = resp.stop_reason or ""
        if resp.stop_reason == "tool_use":
            tool_uses = [b for b in resp.content if b.type == "tool_use"]
            messages.append({"role": "assistant", "content": resp.content})
            tool_results = []
            for tu in tool_uses:
                log.info("tool call %d: %s args=%s", _step, tu.name,
                         str(tu.input)[:200])
                try:
                    result = run_tool(tu.name, tu.input)
                except Exception as e:
                    result = {"error": f"{type(e).__name__}: {e}"}
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tu.id,
                    "content": json.dumps(result, ensure_ascii=False, default=str)[:8000],
                })
            messages.append({"role": "user", "content": tool_results})
            continue
        # 도구 호출 안 함 — 텍스트 추출
        for b in resp.content:
            if b.type == "text":
                final_text += b.text
        break

    # 도구 호출 한도 초과로 텍스트 없이 빠져나옴 → 마지막에 한 번 더 강제 응답 요청
    if not final_text and last_stop_reason == "tool_use":
        log.warning("tool_use loop exhausted (%d steps) — forcing final text", MAX_STEPS)
        messages.append({
            "role": "user",
            "content": "더 이상 도구 호출 없이, 지금까지 모은 데이터로 사용자에게 최종 답변을 한국어로 작성하세요.",
        })
        try:
            resp = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=4000,
                system=SYSTEM_PROMPT,
                messages=messages,   # tools 빼서 강제 텍스트만 응답
            )
            for b in resp.content:
                if b.type == "text":
                    final_text += b.text
        except Exception as e:
            log.exception("forced final attempt 실패: %s", e)
            final_text = f"(응답 생성 실패: {e})"

    # 히스토리 업데이트 — 최종 텍스트 페어만 보관 (tool_use 중간 단계 제외)
    new_history = (history + [
        {"role": "user", "content": user_msg},
        {"role": "assistant", "content": final_text or "(응답 없음)"},
    ])[-MAX_HISTORY_TURNS * 2:]
    _chat_history[chat_id] = new_history

    return final_text or "(응답 없음)"


def _reset_history(chat_id: int) -> None:
    _chat_history.pop(chat_id, None)


# ───────────────────────── 메시지 핸들러 ─────────────────────────
async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_authorized(update):
        # 그룹에선 silent (스팸 방지), DM에서만 안내
        if update.message and update.message.chat.type == "private":
            await update.message.reply_text("Unauthorized")
        return
    text = (update.message.text or "").strip()
    if not text:
        return

    # 그룹/슈퍼그룹: 봇이 명시적으로 호출됐을 때만 응답
    chat_type = update.message.chat.type
    if chat_type in ("group", "supergroup"):
        bot_username = (ctx.bot.username or "").lower()
        text_lower = text.lower()
        addressed = False
        if bot_username and f"@{bot_username}" in text_lower:
            # @mention 잘라내고 나머지를 명령으로
            import re
            text = re.sub(rf"@{re.escape(bot_username)}", "", text,
                          flags=re.IGNORECASE).strip()
            addressed = True
        elif (update.message.reply_to_message
              and update.message.reply_to_message.from_user
              and update.message.reply_to_message.from_user.id == ctx.bot.id):
            # 봇 메시지에 reply
            addressed = True
        if not addressed:
            return   # 일반 잡담은 무시
        if not text:
            text = "안녕"   # 멘션만 했을 때 기본 응답

    log.info("incoming (%s): %s", chat_type, text[:100])
    await update.message.chat.send_action(ChatAction.TYPING)

    # 1) 키워드 명령
    if await _handle_keyword(update, text.lower()):
        return

    # 2) 자유 텍스트 → Claude (chat_id별 대화 히스토리 유지)
    chat_id = update.message.chat.id
    try:
        reply = await _ask_claude(text, chat_id)
    except Exception as e:
        log.exception("Claude error")
        await update.message.reply_text(f"실패: {e}")
        return
    # 텔레그램 4096자 제한
    if len(reply) > 4000:
        reply = reply[:3990] + "\n…(잘림)"
    await update.message.reply_text(reply, parse_mode=ParseMode.MARKDOWN,
                                    disable_web_page_preview=True)


# ───────────────────────── main ─────────────────────────
def main() -> None:
    token = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN이 .env에 없음")
    if not _allowed_user_ids():
        raise RuntimeError("TELEGRAM_CHAT_ID 또는 TELEGRAM_AUTHORIZED_USERS .env에 없음")
    if not (os.environ.get("ANTHROPIC_API_KEY") or "").strip():
        log.warning("ANTHROPIC_API_KEY 없음 — 키워드 명령만 동작, 자유 텍스트는 실패")

    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("reset", cmd_reset))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    log.info("Bot starting (long-polling)...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
