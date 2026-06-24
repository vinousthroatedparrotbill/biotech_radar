"""Telegram bot listener — 인터랙티브 AI research analyst.
명령어 매칭 + Claude API 자유 텍스트 분석 + tool calling.

실행: python telegram_bot.py
종료: Ctrl+C
"""
from __future__ import annotations

import json
import logging
import os
import re
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

from bot_agent import CLAUDE_MODEL, SYSTEM_PROMPT, run_agent


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
    # "mp"는 너무 짧아 substring 매칭하면 "compass", "cmps", "company" 등에 오매칭됨
    # → 독립 단어(word boundary)로만 인식. 나머지는 substring으로 충분.
    if (any(k in msg_lower for k in ("포트폴리오", "portfolio", "model portfolio"))
            or re.search(r"\bmp\b", msg_lower)):
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


# 직전 업로드 파일 — 다음 텍스트 질문 1턴에 자동 재첨부(멀티턴 "그 pdf 분석해줘" 대응)
_pending_file: dict[int, list] = {}


async def _ask_claude(user_msg: str, chat_id: int, attachments=None) -> str:
    """공용 run_agent에 위임 — chat_id별 히스토리만 telegram 측에서 관리.
    SYSTEM_PROMPT·도구·멀티스텝 루프는 bot_agent 단일 소스(웹 채팅과 100% 동일)."""
    history = _chat_history.get(chat_id, [])
    text, new_history = run_agent(user_msg, history, attachments=attachments)
    _chat_history[chat_id] = new_history[-MAX_HISTORY_TURNS * 2:]
    return text


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
    att = _pending_file.pop(chat_id, None)   # 직전 업로드 파일을 이 질문에 1회 재첨부
    try:
        reply = await _ask_claude(text, chat_id, attachments=att)
    except Exception as e:
        log.exception("Claude error")
        await update.message.reply_text(f"실패: {e}")
        return
    # 텔레그램 4096자 제한
    if len(reply) > 4000:
        reply = reply[:3990] + "\n…(잘림)"
    await _send_reply(update, reply)


async def handle_file(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """문서(PDF/텍스트)·사진 업로드 → run_agent 첨부로 분석 (웹챗과 동일 메커니즘)."""
    if not _is_authorized(update):
        if update.message and update.message.chat.type == "private":
            await update.message.reply_text("Unauthorized")
        return
    msg = update.message
    # 그룹에선 캡션에 봇 멘션이 있을 때만 (스팸 방지). DM은 항상 처리.
    if msg.chat.type in ("group", "supergroup"):
        bu = (ctx.bot.username or "").lower()
        if not (bu and f"@{bu}" in (msg.caption or "").lower()):
            return
    import base64
    kind = name = media_type = None
    try:
        if msg.document:
            doc = msg.document
            data = bytes(await (await doc.get_file()).download_as_bytearray())
            name = doc.file_name or "file"
            ext = name.lower().rsplit(".", 1)[-1] if "." in name else ""
            mime = (doc.mime_type or "").lower()
            if ext == "pdf" or "pdf" in mime:
                kind = "pdf"
            elif ext in ("png", "jpg", "jpeg") or mime.startswith("image"):
                kind = "image"
                media_type = "image/png" if ext == "png" else "image/jpeg"
            else:
                kind = "text"
        elif msg.photo:
            data = bytes(await (await msg.photo[-1].get_file()).download_as_bytearray())
            kind, media_type, name = "image", "image/jpeg", "photo.jpg"
        else:
            return
    except Exception as e:
        await msg.reply_text(f"파일 다운로드 실패: {e}")
        return

    if kind in ("pdf", "image"):
        att = [{"kind": kind, "name": name, "media_type": media_type,
                "data": base64.b64encode(data).decode()}]
    else:
        att = [{"kind": "text", "name": name,
                "text": data.decode("utf-8", errors="replace")[:20000]}]
    log.info("file received: name=%s kind=%s bytes=%d caption=%r",
             name, kind, len(data) if data else 0, (msg.caption or "")[:60])
    caption = (msg.caption or "").strip() or "첨부한 파일을 읽고 분석해줘."
    await msg.chat.send_action(ChatAction.TYPING)
    chat_id = msg.chat.id
    try:
        history = _chat_history.get(chat_id, [])
        text, new_history = run_agent(caption, history, attachments=att)
        _chat_history[chat_id] = new_history[-MAX_HISTORY_TURNS * 2:]
        _pending_file[chat_id] = att   # 다음 텍스트 후속질문에 재첨부 (그 pdf 분석해줘)
    except Exception as e:
        log.exception("file analysis error")
        await msg.reply_text(f"분석 실패: {e}")
        return
    await _send_reply(update, text)


async def _send_reply(update: Update, text: str) -> None:
    """최종 답변 전송 — 레거시 Markdown 파싱 실패(비대칭 *,_,[ 등) 시
    일반 텍스트로 자동 fallback. 둘 다 실패해도 silent로 안 끝나게 로깅."""
    try:
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN,
                                        disable_web_page_preview=True)
        log.info("reply sent (markdown, %d chars)", len(text))
        return
    except Exception as e:
        log.warning("markdown 전송 실패 (%s) — 일반 텍스트로 재시도", e)
    try:
        await update.message.reply_text(text, disable_web_page_preview=True)
        log.info("reply sent (plain, %d chars)", len(text))
    except Exception as e:
        log.exception("일반 텍스트 전송도 실패: %s", e)


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
    app.add_handler(MessageHandler(filters.Document.ALL | filters.PHOTO, handle_file))

    async def _on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        log.exception("unhandled handler error", exc_info=context.error)
    app.add_error_handler(_on_error)

    log.info("Bot starting (long-polling)...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
