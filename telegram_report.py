"""텔레그램 일일 요약 — 신규 52w 신고가 + 전체 신고가 + 메모 변동."""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

import yf_session  # noqa: F401 — yfinance 레이트리밋 패치 (import 부수효과)

_ENV_PATH = Path(__file__).parent / ".env"
TG_API = "https://api.telegram.org/bot{token}/sendMessage"
MAX_MSG_LEN = 4000   # Telegram 4096 limit, leave headroom
CLAUDE_MODEL_HIGHS = "claude-opus-4-8"   # 신고가 '투자 포인트 & 상승 동인' 요약


def _load_env() -> tuple[str, str]:
    # override=True — 사용자가 .env를 수정하면 즉시 반영 (Streamlit 재시작 없이도)
    load_dotenv(_ENV_PATH, override=True)
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN 또는 TELEGRAM_CHAT_ID가 .env에 없음. "
            "@BotFather로 봇 생성 후 토큰, @userinfobot으로 chat_id 확인."
        )
    return token, chat_id


def send(text: str, parse_mode: str = "HTML") -> dict:
    """단일 메시지 발송. text가 길면 chunk로 분할.
    HTML parse 실패 시 plain text로 fallback (chunk 단위)."""
    token, chat_id = _load_env()
    chunks = _split(text, MAX_MSG_LEN)
    last = {}
    for chunk in chunks:
        r = requests.post(
            TG_API.format(token=token),
            json={"chat_id": chat_id, "text": chunk,
                  "parse_mode": parse_mode, "disable_web_page_preview": True},
            timeout=15,
        )
        if r.status_code == 400 and parse_mode:
            # HTML 파싱 실패 — plain text로 재시도 (chunk 분할로 <pre> 깨졌을 가능성)
            import re as _re
            stripped = _re.sub(r"<[^>]+>", "", chunk)   # HTML 태그 제거
            stripped = (stripped.replace("&amp;", "&")
                         .replace("&lt;", "<").replace("&gt;", ">"))
            r = requests.post(
                TG_API.format(token=token),
                json={"chat_id": chat_id, "text": stripped[:MAX_MSG_LEN],
                      "disable_web_page_preview": True},
                timeout=15,
            )
        if not r.ok:
            # 실패 본문 surface해 디버깅 가능하게
            raise requests.exceptions.HTTPError(
                f"{r.status_code} from Telegram: {r.text[:300]}",
                response=r,
            )
        last = r.json()
    return last


def _split(text: str, n: int) -> list[str]:
    if len(text) <= n:
        return [text]
    out = []
    cur = ""
    for line in text.split("\n"):
        if len(cur) + len(line) + 1 > n:
            out.append(cur)
            cur = line
        else:
            cur = (cur + "\n" + line) if cur else line
    if cur:
        out.append(cur)
    return out


def _esc(s) -> str:
    if s is None:
        return ""
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def _fmt_pct(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    sign = "🟢" if v > 0 else ("🔴" if v < 0 else "⚪")
    return f"{sign} {v:+.1f}%"


def _row_line(r) -> str:
    """단일 종목 한 줄 (메모 등 비-표 컨텍스트용)."""
    name = _esc((r.get("name") or "")[:30])
    ticker = _esc(r.get("ticker", ""))
    close = r.get("close") or r.get("today_close")
    perf_1d = r.get("perf_1d")
    price_str = f"${close:,.2f}" if pd.notna(close) else "—"
    return f"• <b>{ticker}</b> {name} — {price_str} {_fmt_pct(perf_1d)}"


def _esc_pre(s: str) -> str:
    """<pre> 블록 안 텍스트용 — HTML 특수문자 이스케이프."""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _fmt_mcap_b(mcap_m, ticker=None) -> str:
    """시총 표기 — KR(6자리)은 KRW(억/조), 그 외 $B. (kr_universe.fmt_mcap 위임)"""
    if mcap_m is None or pd.isna(mcap_m):
        return "—"
    try:
        import kr_universe as _ku
        return _ku.fmt_mcap(mcap_m, ticker)
    except Exception:
        b = mcap_m / 1000.0
        return f"${b:,.0f}B" if b >= 100 else f"${b:,.1f}B"


def _table_render(df, max_rows: int = 25) -> str:
    """DataFrame → 텔레그램 <pre> 모노스페이스 표.
    컬럼: Ticker(6) Company(22) Price(10) 1D%(7) Mcap(8)."""
    if df is None or df.empty:
        return "  <i>(없음)</i>"
    lines = []
    header = f"{'Ticker':<6} {'Company':<22} {'Price':>10} {'1D%':>7} {'Mcap':>8}"
    lines.append(header)
    lines.append("-" * len(header))
    for _, r in df.head(max_rows).iterrows():
        ticker = (str(r.get("ticker") or "")[:6])
        name = (str(r.get("name") or "")[:22])
        close = r.get("close") if "close" in r else r.get("today_close")
        perf_1d = r.get("perf_1d")
        mcap = r.get("market_cap")
        try:
            import kr_universe as _ku
            price = _ku.fmt_price(close, ticker) if pd.notna(close) else "—"
        except Exception:
            price = f"${close:,.2f}" if pd.notna(close) else "—"
        pct = f"{perf_1d:+.1f}%" if pd.notna(perf_1d) else "—"
        mcap_s = _fmt_mcap_b(mcap, ticker)
        lines.append(f"{ticker:<6} {name:<22} {price:>10} {pct:>7} {mcap_s:>8}")
    body = "\n".join(lines)
    out = f"<pre>{_esc_pre(body)}</pre>"
    if len(df) > max_rows:
        out += f"\n  <i>... 외 {len(df) - max_rows}종목</i>"
    return out


def compose_report() -> str:
    """오늘의 요약 텍스트 (HTML format)."""
    from collectors.high_low import (
        fetch_new_today_highs, fetch_new_highs, latest_run_date,
    )
    from memo import timeline as memo_timeline

    today_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    last = latest_run_date() or "—"

    parts = [f"🧬 <b>Biotech Radar — {today_str}</b>",
             f"<i>52w 갱신 기준일: {last}</i>", ""]

    # ── 신규 신고가 ──
    new_today = fetch_new_today_highs(limit=100)
    parts.append(f"🆕 <b>오늘 신규 52w 신고가</b> ({len(new_today)}종목)")
    parts.append(_table_render(new_today, max_rows=25))
    parts.append("")

    # ── 신규 신고가 종목별 최근 뉴스 (헤드라인 3개씩) ──
    if not new_today.empty:
        from news import fetch_recent_titles
        parts.append("📰 <b>신규 신고가 종목 최근 뉴스</b> (헤드라인 3개)")
        for _, row in new_today.iterrows():
            ticker = str(row["ticker"])
            name = str(row.get("name") or "")
            titles = fetch_recent_titles(ticker, n=3, days=7)
            if not titles:
                continue
            parts.append(f"\n<b>{_esc(ticker)}</b> · {_esc(name[:40])}")
            for t in titles:
                title = t["title"][:160]
                src = t.get("source", "").replace("Finviz/", "")
                link = t.get("link", "")
                if link:
                    parts.append(f"  • <a href=\"{_esc(link)}\">{_esc(title)}</a> "
                                 f"<i>({_esc(src)})</i>")
                else:
                    parts.append(f"  • {_esc(title)} <i>({_esc(src)})</i>")
        parts.append("")

    # ── 전체 신고가 ──
    all_highs = fetch_new_highs("high", limit=200)
    parts.append(f"📈 <b>전체 52w 신고가</b> ({len(all_highs)}종목)")
    parts.append(_table_render(all_highs, max_rows=40))
    parts.append("")

    # ── 오늘 상승폭 최대 (시총 ≥$500M, 1D ≥+5%) ──
    from collectors.high_low import fetch_top_movers
    movers = fetch_top_movers(limit=20, min_mcap=500, min_perf=5)
    parts.append(f"🚀 <b>오늘 상승폭 최대</b> (시총≥$500M, +5%↑, {len(movers)}종목)")
    if movers.empty:
        parts.append("  <i>해당 종목 없음</i>")
    else:
        parts.append(_table_render(movers, max_rows=25))
    parts.append("")

    # ── 메모 + 이후 변동 ──
    memos = memo_timeline(limit=30)
    parts.append(f"📝 <b>내 메모와 이후 주가 변동</b> ({len(memos)}건)")
    if not memos:
        parts.append("  <i>(아직 메모 없음)</i>")
    else:
        for m in memos[:20]:
            ts = m["created_at"][:10]
            ticker = _esc(m["ticker"])
            name = _esc((m.get("name") or "")[:25])
            body = _esc(m["body"])
            if len(body) > 80:
                body = body[:80] + "…"
            cp = m.get("change_pct")
            pa = m.get("price_at_create")
            pn = m.get("price_now")
            change_line = "  <i>주가 데이터 없음</i>"
            if cp is not None and pa and pn:
                arrow = "🟢" if cp > 0 else ("🔴" if cp < 0 else "⚪")
                change_line = f"  {arrow} <b>{cp:+.1f}%</b>  ${pa:,.2f} → ${pn:,.2f}"
            parts.append(f"• <b>{ticker}</b> {name} <i>({ts})</i>")
            parts.append(f"  «{body}»")
            parts.append(change_line)
            parts.append("")

    return "\n".join(parts)


def _markdown_to_html(md: str) -> str:
    """Claude markdown 출력 → 텔레그램 HTML."""
    import re as _re
    # 이스케이프 (HTML 특수 문자)
    s = md.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    # **bold** → <b>
    s = _re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", s)
    # *italic* → <i> (단, list bullet 충돌 회피 — 줄 시작 * 제외)
    s = _re.sub(r"(?<![\*\n])\*([^*\n]+)\*(?!\*)", r"<i>\1</i>", s)
    return s


def _html_tags_balanced(s: str) -> bool:
    """간단 검사 — 모든 <tag>가 </tag>로 닫혀있는지."""
    import re as _re
    opens = _re.findall(r"<(b|i|u|s|code|pre|a)(?:\s[^>]*)?>", s)
    closes = _re.findall(r"</(b|i|u|s|code|pre|a)>", s)
    from collections import Counter
    return Counter(opens) == Counter(closes)


def _safe_caption(caption: str, limit: int = 1024) -> tuple[str, str | None]:
    """캡션 1024자 제한 + HTML 균형 검증.
    제한 넘거나 태그 불균형이면 plain text(parse_mode=None) 반환."""
    import re as _re
    if len(caption) <= limit and _html_tags_balanced(caption):
        return caption, "HTML"
    # 안전: HTML 태그 제거 + 엔티티 복원 → plain text
    stripped = _re.sub(r"<[^>]+>", "", caption)
    stripped = (stripped.replace("&amp;", "&")
                .replace("&lt;", "<").replace("&gt;", ">"))
    if len(stripped) > limit:
        stripped = stripped[:limit - 10] + "\n…(전체는 PDF)"
    return stripped, None


def send_document(path: str, caption: str = "",
                  parse_mode: str = "HTML") -> dict:
    """파일 첨부 발송 — sendDocument API. 캡션 HTML 깨지면 plain text fallback."""
    token, chat_id = _load_env()
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    safe_cap, mode = _safe_caption(caption, limit=1024)
    if parse_mode is None:
        mode = None
    with open(path, "rb") as fp:
        files = {"document": fp}
        data = {"chat_id": chat_id, "caption": safe_cap,
                "disable_notification": False}
        if mode:
            data["parse_mode"] = mode
        r = requests.post(url, data=data, files=files, timeout=60)
    if r.status_code == 400 and mode:
        # HTML 파싱 실패 — 태그 다 제거 후 재시도
        import re as _re
        plain = _re.sub(r"<[^>]+>", "", caption)[:1024]
        with open(path, "rb") as fp:
            files = {"document": fp}
            data = {"chat_id": chat_id, "caption": plain,
                    "disable_notification": False}
            r = requests.post(url, data=data, files=files, timeout=60)
    if not r.ok:
        raise requests.exceptions.HTTPError(
            f"{r.status_code} sendDocument: {r.text[:300]}", response=r,
        )
    return r.json()


def send_photo(path: str, caption: str = "", parse_mode: str = "HTML") -> dict:
    """이미지 첨부 발송 — sendPhoto API. 캡션 HTML 파싱 400이면 평문으로 재시도,
    그래도 실패하면 예외(호출자가 인지하도록 — 조용한 실패 방지)."""
    token, chat_id = _load_env()
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    safe_cap, mode = _safe_caption(caption, limit=1024)
    if parse_mode is None:
        mode = None
    with open(path, "rb") as fp:
        data = {"chat_id": chat_id, "caption": safe_cap}
        if mode:
            data["parse_mode"] = mode
        r = requests.post(url, data={**data}, files={"photo": fp}, timeout=60)
    if r.status_code == 400 and mode:
        import re as _re
        plain = _re.sub(r"<[^>]+>", "", caption)[:1024]
        with open(path, "rb") as fp:
            r = requests.post(url, data={"chat_id": chat_id, "caption": plain},
                              files={"photo": fp}, timeout=60)
    if not r.ok:
        raise requests.HTTPError(
            f"{r.status_code} sendPhoto: {r.text[:300]}", response=r)
    return r.json()


def send_investment_reports(tickers: list[str], max_n: int = 5,
                            skip_days: int = 7) -> int:
    """신규 신고가 종목 시총 TOP max_n — TL;DR 5-10줄을 텔레그램 메시지로,
    full report는 PDF 첨부로 발송. 발송 성공 종목 수 반환.

    skip_days > 0 이면 최근 `skip_days`일 내 이미 메모를 보낸 종목은
    생성·발송 모두 스킵 (비용·중복 방지). 0이면 스킵 안 함."""
    import os
    import investment_report as ir
    from pdf_gen import render_pdf_to_file
    if not tickers:
        return 0
    # 최근 skip_days일 내 발송 종목 제외 — generate() 호출 전에 걸러 비용 절감
    if skip_days > 0:
        try:
            recent = ir.recently_sent_tickers(days=skip_days)
        except Exception as e:
            print(f"[REPORT_DEDUP_FAIL] {type(e).__name__}: {e}", flush=True)
            recent = set()
        if recent:
            before = len(tickers)
            tickers = [t for t in tickers if t.upper() not in recent]
            skipped = before - len(tickers)
            if skipped:
                print(f"[REPORT_DEDUP] 최근 {skip_days}일 내 발송 {skipped}종목 스킵",
                      flush=True)
        if not tickers:
            print("[REPORT_DEDUP] 발송 대상 없음 (전부 최근 발송됨)", flush=True)
            return 0
    reports = ir.generate_for_tickers(tickers, max_n=max_n)
    sent = 0
    for r in reports:
        tk = r["ticker"]
        full_md = r["report"]
        try:
            tldr, body = ir.split_tldr_and_body(full_md)
            # TL;DR markdown → HTML (텔레그램 캡션용)
            tldr_html = _markdown_to_html(tldr)
            caption = (f"📊 <b>{tk} 투자 메모</b>\n\n{tldr_html}\n\n"
                       f"<i>📎 전체 리포트는 첨부 PDF 참조</i>")
            if len(caption) > 1000:
                caption = caption[:990] + "\n…(요약 잘림, 전체는 PDF)"
            # PDF 생성
            pdf_path = render_pdf_to_file(body, ticker=tk)
            try:
                send_document(pdf_path, caption=caption)
                sent += 1
                # 발송 성공 → 중복 방지 로그 기록
                try:
                    ir.mark_sent(tk)
                except Exception as e:
                    print(f"[REPORT_MARK_FAIL] {tk}: {type(e).__name__}: {e}",
                          flush=True)
            finally:
                try:
                    os.unlink(pdf_path)
                except Exception:
                    pass
        except Exception as e:
            import traceback as _tb
            print(f"[REPORT_FAIL] {tk}: {type(e).__name__}: {e}", flush=True)
            print(_tb.format_exc(), flush=True)
            # 실패 시 plain text 폴백
            try:
                send(f"⚠️ <b>{tk} 메모 PDF 발송 실패</b>: {type(e).__name__}: {e}\n\n"
                     f"---\n\n{_markdown_to_html(full_md)[:3000]}")
            except Exception as e2:
                print(f"[REPORT_FAIL_FALLBACK] {tk}: {e2}", flush=True)
    return sent


def send_portfolio_snapshots() -> int:
    """모든 MP의 수익률 + 편입 종목별 수익률 발송. 발송 청크 수 반환."""
    import portfolio as pf
    ports = pf.list_all()
    if not ports:
        return 0
    parts = ["💼 <b>Model Portfolio 일일 스냅샷</b>", ""]
    for p in ports:
        s = pf.summary(p["id"])
        if not s:
            continue
        ret = s.get("return_pct", 0) or 0
        sign = "🟢" if ret >= 0 else "🔴"
        current_m = s["current_size"] / 1e6
        initial_m = s["portfolio"]["initial_size"] / 1e6
        parts.append(
            f"━━━ <b>{_esc(p['name'])}</b> ━━━\n"
            f"  {sign} <b>{ret:+.2f}%</b>  "
            f"${current_m:,.2f}M / ${initial_m:,.0f}M 기준\n"
            f"  편입 {s['total_weight']:.0f}% · 현금 {s['cash_pct']:.0f}% "
            f"(${s['cash_amt']/1e6:,.2f}M)"
        )
        holdings = sorted(
            s["holdings"], key=lambda h: (h.get("return_pct") or 0), reverse=True,
        )
        if holdings:
            parts.append("")
            parts.append("<pre>")
            parts.append(f"{'Ticker':<7}{'편입%':>7}{'진입가':>10}{'현재가':>10}{'수익률':>9}")
            for h in holdings:
                ret = h.get("return_pct") or 0
                arrow = "🟢" if ret > 0 else ("🔴" if ret < 0 else "⚪")
                tk = h["ticker"][:7]
                wt = h.get("weight_pct") or 0
                ep = h.get("entry_price") or 0
                cp = h.get("curr_price") or 0
                parts.append(
                    f"{tk:<7}{wt:>6.1f}%{ep:>10.2f}{cp:>10.2f}"
                    f"{arrow} {ret:>+6.1f}%"
                )
            parts.append("</pre>")
        parts.append("")
    msg = "\n".join(parts)
    chunks = _split(msg, 3900)
    for chunk in chunks:
        send(chunk)
    return len(chunks)


def _fmt_catalyst_line(row: dict) -> str:
    """단일 카탈리스트 한 줄 (이모지 + 날짜 + 티커 + 제목)."""
    tt = row.get("event_type", "")
    emoji = {"pdufa": "💊", "earnings": "📊", "clinical_readout": "🧪",
             "clinical_milestone": "🚀", "regulatory": "📜",
             "conference": "🎤", "company_event": "📑",
             "earnings_call": "🎙️"}.get(tt, "📅")
    tk = (row.get("ticker") or "").upper().strip()
    tk_str = f"<b>{_esc(tk)}</b> · " if tk else ""
    # date_hint 우선, 없으면 event_date
    import re as _re
    desc = row.get("description") or ""
    dh = _re.search(r"date_hint:\s*([^·]+)", desc)
    date_label = dh.group(1).strip() if dh else (row.get("event_date") or "")
    title = (row.get("title") or "")[:200]
    return f"  {emoji} <i>{_esc(date_label)}</i>  {tk_str}{_esc(title)}"


def _format_trigger_alert(trig: dict, ctx: dict) -> str:
    """가격 트리거 발동 알림 HTML."""
    direction_label = "📈 위로 돌파" if trig["direction"] == "above" else "📉 아래로 돌파"
    parts = [
        f"🚨 <b>{trig['ticker']} 트리거 발동</b>",
        f"{direction_label} ${trig['threshold']:.2f} — "
        f"현재 <b>${trig['triggered_price']:.2f}</b>",
        "",
    ]
    if trig.get("note"):
        parts.append(f"📝 메모: <i>{_esc(trig['note'])}</i>")
        parts.append("")
    if ctx.get("volume_ratio"):
        parts.append(
            f"📊 거래량: {ctx['volume_today']/1e6:.1f}M "
            f"(30d avg {ctx['volume_30d_avg']/1e6:.1f}M, "
            f"{ctx['volume_ratio']:.1f}x · z={ctx.get('volume_zscore', 0):.1f})"
        )
    if ctx.get("recent_news"):
        parts.append("")
        parts.append("📰 <b>최근 14일 뉴스</b>")
        for n in ctx["recent_news"][:3]:
            title = (n.get("title") or "")[:140]
            link = n.get("link", "")
            src = (n.get("source") or "").replace("Finviz/", "")
            if link:
                parts.append(f'  • <a href="{_esc(link)}">{_esc(title)}</a> '
                             f"<i>({_esc(src)})</i>")
            else:
                parts.append(f"  • {_esc(title)} <i>({_esc(src)})</i>")
    if ctx.get("memos"):
        parts.append("")
        parts.append("📝 <b>이전 메모</b>")
        for m in ctx["memos"][:2]:
            body = (m.get("body") or "").strip()
            if len(body) > 200:
                body = body[:200] + "…"
            parts.append(f"  • <i>{_esc(body)}</i>")
    return "\n".join(parts)


def send_trigger_alerts() -> int:
    """active 가격 트리거 체크 → 발동 시 알림 발송. 발송 건수 반환."""
    import price_triggers as pt
    fired = pt.check_all_triggers()
    sent = 0
    for trig in fired:
        try:
            ctx = pt.enrich_for_alert(trig["ticker"])
            msg = _format_trigger_alert(trig, ctx)
            send(msg)
            sent += 1
        except Exception as e:
            try:
                send(f"⚠️ {trig['ticker']} 트리거 알림 발송 실패: {e}")
            except Exception:
                pass
    return sent


def send_monthly_catalyst_summary() -> int:
    """매달 1일 — 그달 카탈리스트 정리 발송."""
    import catalysts as cat
    today = datetime.now()
    df = cat.get_month_catalysts(today.year, today.month)
    if df.empty:
        return 0
    parts = [
        f"📅 <b>{today.year}년 {today.month}월 카탈리스트</b> ({len(df)}건)",
        "",
    ]
    # 타입별 그룹
    type_order = ["pdufa", "regulatory", "clinical_readout", "clinical_milestone",
                  "earnings", "conference", "company_event", "earnings_call"]
    type_labels = {"pdufa": "💊 PDUFA",
                   "regulatory": "📜 FDA 규제",
                   "clinical_readout": "🧪 임상 데이터 공개",
                   "clinical_milestone": "🚀 임상 마일스톤",
                   "earnings": "📊 어닝",
                   "conference": "🎤 학회·컨퍼런스",
                   "company_event": "📑 회사 공개",
                   "earnings_call": "🎙️ 어닝콜 멘션"}
    for ev_type in type_order:
        sub = df[df["event_type"] == ev_type]
        if sub.empty:
            continue
        parts.append(f"\n<b>{type_labels.get(ev_type, ev_type)}</b> ({len(sub)})")
        # 가까운 순, 최대 20개
        for _, r in sub.head(20).iterrows():
            parts.append(_fmt_catalyst_line(r.to_dict()))
    msg = "\n".join(parts)
    # 분할 발송
    chunks = _split(msg, 3900)
    for chunk in chunks:
        send(chunk)
    return len(chunks)


def send_watched_alerts() -> dict:
    """워치 카탈리스트의 1m/1w 임박 알림 발송."""
    import catalysts as cat
    due = cat.get_due_alerts()
    sent_m = 0
    sent_w = 0
    # 1개월 전
    for item in due["month_alerts"]:
        msg = (
            f"⏰ <b>1개월 전 알림</b>\n\n"
            f"{_fmt_catalyst_line(item)}\n\n"
            f"<i>notify_date: {item.get('notify_date')}</i>"
        )
        try:
            send(msg)
            cat.mark_notified(item["id"], "1m")
            sent_m += 1
        except Exception:
            pass
    # 1주 전
    for item in due["week_alerts"]:
        msg = (
            f"⏰ <b>1주 전 알림</b>\n\n"
            f"{_fmt_catalyst_line(item)}\n\n"
            f"<i>notify_date: {item.get('notify_date')}</i>"
        )
        try:
            send(msg)
            cat.mark_notified(item["id"], "1w")
            sent_w += 1
        except Exception:
            pass
    return {"month": sent_m, "week": sent_w}


def _esc_html(s) -> str:
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def _decode_gnews(url: str) -> str:
    """구글뉴스 RSS 인코딩 URL → 실제 기사 URL. 실패 시 원본."""
    if not url or "news.google.com" not in url:
        return url
    try:
        from googlenewsdecoder import gnewsdecoder
        r = gnewsdecoder(url)
        if isinstance(r, dict) and r.get("status") and r.get("decoded_url"):
            return r["decoded_url"]
    except Exception:
        pass
    return url


def _curated_news(ticker: str, name: str, n: int = 2) -> list[dict]:
    """주가를 움직였을 만한 fundamental/catalyst 뉴스 n개 (제목+유효 링크).
    investment_report의 fundamental 필터 재사용(임상/FDA/M&A/데이터 등, 노이즈 제외).
    구글뉴스 RSS 링크는 실제 기사 URL로 디코딩."""
    try:
        from investment_report import _fundamental_news
        items = _fundamental_news(ticker, days=21, limit=n)
    except Exception:
        items = []
    out = []
    for it in items[:n]:
        title = (it.get("title") or "").strip()
        url = _decode_gnews(it.get("link") or it.get("url") or "")
        if title:
            out.append({"title": title, "url": url})
    return out


def _send_memo_pdf(ticker: str) -> bool:
    """단일 종목 투자 메모 생성 + PDF 첨부 발송 + 발송 로그(mark_sent). 성공 여부."""
    import os
    import investment_report as ir
    from pdf_gen import render_pdf_to_file
    tk = (ticker or "").strip().upper()
    try:
        full_md = ir.generate(tk)
        tldr, body = ir.split_tldr_and_body(full_md)
        caption = (f"📊 <b>{tk} 투자 메모</b>\n\n{_markdown_to_html(tldr)}\n\n"
                   f"<i>📎 전체 리포트는 첨부 PDF</i>")
        if len(caption) > 1000:
            caption = caption[:990] + "\n…(요약 잘림, 전체는 PDF)"
        pdf_path = render_pdf_to_file(body, ticker=tk)
        try:
            send_document(pdf_path, caption=caption)
            try:
                ir.mark_sent(tk)
            except Exception:
                pass
        finally:
            try:
                os.unlink(pdf_path)
            except Exception:
                pass
        return True
    except Exception as e:
        print(f"[MEMO_FAIL] {tk}: {type(e).__name__}: {e}", flush=True)
        return False


def send_ticker_cards(df, memo_tickers=None, max_n: int = 15):
    """종목별 메시지 — 캔들차트 + 기본정보(시총/현재가/수익률) + 주가변동 뉴스 2개.
    시총 상위 max_n개. memo_tickers에 든 종목은 **카드 직후 투자 메모 PDF도 함께** 발송.
    (카드 발송수, 메모 발송수) 반환."""
    import os
    import bot_tools as bt
    if df is None or df.empty:
        return 0, 0
    memo_set = {str(t).upper() for t in (memo_tickers or [])}
    df = df.sort_values("market_cap", ascending=False, na_position="last").head(max_n)
    sent = memos = 0
    for _, r in df.iterrows():
        tk = str(r.get("ticker") or "").upper()
        if not tk:
            continue
        name = str(r.get("name") or tk)
        import kr_universe as _ku
        close = r.get("close") if "close" in r else r.get("today_close")
        cap = f"<b>{_esc_html(name)} ({tk})</b>\n💰 {_ku.fmt_mcap(r.get('market_cap'), tk)}"
        if pd.notna(close):
            cap += f" · {_ku.fmt_price(close, tk)}"
        for col, lab in [("perf_1d", "1D"), ("perf_1m", "1M"), ("perf_1y", "1Y")]:
            v = r.get(col)
            if pd.notna(v):
                cap += f" · {lab} {v:+.1f}%"
        for nz in _curated_news(tk, name, 2):
            t = _esc_html(nz["title"][:90])
            u = (nz["url"] or "").replace("&", "&amp;").replace('"', "%22")
            cap += (f"\n📰 <a href=\"{u}\">{t}</a>" if u else f"\n📰 {t}")
        cap = cap[:1024]
        path, _last = bt.render_candle_png(tk, "2y")
        try:
            if path:
                send_photo(path, caption=cap)
            else:
                send(cap)
            sent += 1
        except Exception as e:
            print(f"[CARD_FAIL] {tk}: {type(e).__name__}: {e}", flush=True)
        finally:
            if path:
                try:
                    os.unlink(path)
                except Exception:
                    pass
        # 시총 상위 6(메모 대상)은 카드 바로 뒤에 투자 메모 PDF도 함께
        if tk in memo_set and _send_memo_pdf(tk):
            memos += 1
    return sent, memos


def _highs_analysis(df, max_n: int = 20, context_label: str = "오늘 52주 신고가를 찍은") -> str:
    """신고가 종목들의 '투자 포인트 & 상승 동인' AI 요약(마크다운). 종목당 헤더 + 불릿 3개
    + 마지막 '요약 테마'. 각 종목 최근 fundamental 뉴스를 컨텍스트로 Opus 1콜. 실패 시 ''."""
    import os
    import anthropic
    if df is None or df.empty:
        return ""
    key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not key:
        return ""
    df = df.sort_values("market_cap", ascending=False, na_position="last").head(max_n)
    lines = []
    for _, r in df.iterrows():
        tk = str(r.get("ticker") or "").upper()
        if not tk:
            continue
        name = str(r.get("name") or tk)
        price = r.get("close") if "close" in r else r.get("today_close")
        p1d = r.get("perf_1d")
        news = _curated_news(tk, name, 4)
        nt = " || ".join(n["title"][:120] for n in news) or "(최근 fundamental 뉴스 없음)"
        pstr = f"${price:,.2f}" if pd.notna(price) else "—"
        dstr = f"{p1d:+.1f}%" if pd.notna(p1d) else "—"
        lines.append(f"- {name} ({tk}) {pstr} ({dstr}) · 뉴스: {nt}")
    ctx = "\n".join(lines)
    prompt = (
        f"다음은 {context_label} 바이오텍 종목들과 각 종목의 최근 fundamental 뉴스 제목이다.\n\n"
        f"{ctx}\n\n"
        "각 종목에 대해 아래 형식으로 '투자 포인트 & 상승 동인'을 작성하라:\n"
        "**종목명 · TICKER $가격 (+1D%)**\n"
        "- (불릿1) 상승 동인 — M&A/임상데이터/자금조달/규제 등 *왜 올랐나*\n"
        "- (불릿2) 핵심 자산·기전(영문 유지: 약물명/타깃/임상명)\n"
        "- (불릿3) 짧은 평가 — 이벤트 종료/모멘텀/재평가 구간 등\n\n"
        "[원칙]\n"
        "- 제공 뉴스 + 네 지식 기반. 뉴스에 동인이 없으면 일반적 섹터/펀더멘털 맥락으로.\n"
        "- buy/sell 추천 표현 금지. 사실·시그널·관찰 위주.\n"
        "- 한국어. 약물명·기전·임상명은 영문 유지.\n"
        "- 헤더는 **굵게**(마크다운 ** **), 불릿은 '- '. 마크다운 ## 제목/표 쓰지 말 것(텔레그램).\n"
        "- 맨 끝에 '---' 한 줄 후 '**요약 테마:**' 한 단락(랠리를 이끈 테마 분류)."
    )
    try:
        client = anthropic.Anthropic(api_key=key)
        resp = client.messages.create(
            model=CLAUDE_MODEL_HIGHS, max_tokens=8000,
            messages=[{"role": "user", "content": prompt}],
        )
        return "".join(b.text for b in resp.content if b.type == "text")
    except Exception as e:
        print(f"[HIGHS_ANALYSIS_FAIL] {type(e).__name__}: {e}", flush=True)
        return ""


def send_card(ticker: str) -> dict:
    """단일 종목 카드 1메시지 — 2년 일봉 캔들차트 + 시총/현재가/1D·1M·1Y +
    주가 동인 뉴스 2개. 봇이 'X 카드/보여줘' 요청 시 사용."""
    import os
    import bot_tools as bt
    from db import connect
    tk = (ticker or "").strip().upper()
    if not tk:
        return {"error": "ticker required"}
    name, mcap, close = tk, None, None
    p1d = p1m = p1y = None
    try:
        with connect() as c:
            row = c.execute(
                "SELECT name, market_cap FROM ticker_master WHERE ticker = ?", (tk,)
            ).fetchone()
            if row:
                name = row.get("name") or tk
                mcap = row.get("market_cap")
            hl = c.execute(
                "SELECT today_close, market_cap, perf_1d, perf_1m, perf_1y "
                "FROM high_low_cache WHERE ticker = ? ORDER BY computed_date DESC LIMIT 1",
                (tk,),
            ).fetchone()
            if hl:
                close = hl.get("today_close")
                mcap = hl.get("market_cap") or mcap
                p1d, p1m, p1y = hl.get("perf_1d"), hl.get("perf_1m"), hl.get("perf_1y")
    except Exception:
        pass
    try:                                    # 현재가는 토스 live로 보강
        import toss_market as tm
        if tm.available():
            pr = tm.price(tk)
            if pr:
                close = pr
    except Exception:
        pass
    cap = f"<b>{_esc_html(name)} ({tk})</b>\n💰 ${(mcap or 0)/1000.0:,.1f}B"
    if close:
        cap += f" · ${close:,.2f}"
    for v, lab in [(p1d, "1D"), (p1m, "1M"), (p1y, "1Y")]:
        if v is not None:
            cap += f" · {lab} {v:+.1f}%"
    for nz in _curated_news(tk, name, 2):
        t = _esc_html(nz["title"][:90])
        u = (nz["url"] or "").replace("&", "&amp;").replace('"', "%22")
        cap += (f"\n📰 <a href=\"{u}\">{t}</a>" if u else f"\n📰 {t}")
    cap = cap[:1024]
    path, _last = bt.render_candle_png(tk, "2y")
    try:
        if path:
            send_photo(path, caption=cap)
        else:
            send(cap)
    except Exception as e:
        return {"error": f"카드 발송 실패: {e}"}
    finally:
        if path:
            try:
                os.unlink(path)
            except Exception:
                pass
    # 카드 직후 투자 메모 PDF도 함께 (생성 1-3분 소요)
    memo_ok = _send_memo_pdf(tk)
    return {"ok": True, "ticker": tk, "memo_pdf": memo_ok,
            "msg": f"{tk} 카드" + (" + 메모 PDF 발송 완료" if memo_ok
                                  else " 발송(메모 생성 실패)")}


def daily_run() -> dict:
    """수집 + 발송 — 스케줄러에서 호출. 구성: ①신고가/상승폭 목록 → ②종목별 카드(차트+뉴스)
    → ③투자메모 PDF(상위6, 7일 제외) → ④MP 현황."""
    from universe import load_universe
    from collectors.high_low import collect as hl_collect
    from collectors.high_low import fetch_new_today_highs

    # 1) Universe 갱신
    n_uni = load_universe()
    # 2) 52w 신고가 갱신
    n_hl = hl_collect(industry_filter=None)
    # 3) ① 52주 신고가 목록 + ② 최대 상승폭 목록 (각 1 메시지)
    from collectors.high_low import fetch_new_highs, fetch_top_movers, latest_run_date
    highs = fetch_new_highs("high", limit=40)
    movers = fetch_top_movers(limit=40, min_mcap=1500.0, min_perf=5.0)
    header = (
        f"🧬 <b>Biotech Radar — {datetime.now().strftime('%Y-%m-%d')}</b>\n"
        f"<i>auto-run: universe={n_uni}, snapshot={n_hl} · 기준일 {latest_run_date() or '—'}</i>"
    )
    # ① 52주 신고가 — 단순 목록 대신 '투자 포인트 & 상승 동인' AI 분석. 실패 시 표로 fallback.
    analysis = _highs_analysis(highs, max_n=20)
    if analysis:
        main_result = send(
            f"{header}\n\n📊 <b>52주 신고가 — 투자 포인트 &amp; 상승 동인</b> "
            f"({len(highs)}종목)\n\n" + _markdown_to_html(analysis)
        )
    else:
        main_result = send(
            f"{header}\n\n📈 <b>52주 신고가</b> ({len(highs)}종목)\n"
            f"{_table_render(highs, max_rows=30)}"
        )
    send(f"🚀 <b>최대 상승폭 (1D)</b> ({len(movers)}종목)\n{_table_render(movers, max_rows=30)}")

    # 4) 종목별 카드 (시총 상위 15) — 차트+정보+뉴스. 시총 상위 6(최근 7일 미발송)은
    #    카드 바로 뒤에 투자 메모 PDF도 함께 발송. 대상 없으면 메모 생략.
    import investment_report as _ir
    card_df = pd.concat([highs, movers], ignore_index=True).drop_duplicates("ticker")
    card_df = card_df.sort_values("market_cap", ascending=False, na_position="last")
    try:
        recent = _ir.recently_sent_tickers(7)
    except Exception:
        recent = set()
    memo_tickers = [t for t in card_df["ticker"].tolist()
                    if t and str(t).upper() not in recent][:6]
    try:
        cards, memos = send_ticker_cards(card_df, memo_tickers=memo_tickers, max_n=15)
        main_result["ticker_cards"] = cards
        main_result["investment_reports"] = memos
    except Exception as e:
        main_result["cards_error"] = str(e)

    # 5) MP 현황
    try:
        main_result["mp_chunks"] = send_portfolio_snapshots()
    except Exception as e:
        main_result["mp_error"] = str(e)

    # 5) 매달 1일 — 월간 카탈리스트 요약
    if datetime.now().day == 1:
        try:
            n_monthly = send_monthly_catalyst_summary()
            main_result["monthly_catalyst_chunks"] = n_monthly
        except Exception as e:
            main_result["monthly_catalyst_error"] = str(e)

    # 6) 워치 카탈리스트 1m/1w 임박 알림
    try:
        alert_counts = send_watched_alerts()
        main_result["watched_alerts"] = alert_counts
    except Exception as e:
        main_result["watched_alerts_error"] = str(e)

    # 7) 가격 트리거 체크 (가벼움 — 별도 cron에서도 30분마다 호출됨)
    try:
        main_result["triggers_fired"] = send_trigger_alerts()
    except Exception as e:
        main_result["triggers_error"] = str(e)

    return main_result


def daily_run_kr() -> dict:
    """한국 바이오 15:30(KST) 푸시 — 미장 daily_run과 동일 구조
    (신고가 '투자 포인트 & 상승 동인' AI 분석 → 최대 상승폭 → 종목별 카드 + 투자메모).
    유니버스=FDR+네이버 / 스냅샷=토스 / 시총 하한 5천억(KOR 스코프)."""
    import kr_universe as ku
    from collectors.high_low import (collect_kr, fetch_new_highs, fetch_top_movers,
                                     latest_run_date)

    floor = ku.kr_min_mcap_usd_m()
    # 1) KR 유니버스 갱신 (실패해도 기존 유니버스로 진행)
    try:
        n_uni = ku.seed()
    except Exception as e:
        n_uni = -1
        print(f"daily_run_kr: universe 갱신 실패(무시) — {e}")
    # 2) KR 52w 스냅샷 (토스 기반)
    n_hl = collect_kr()
    # 3) 신고가 + 상승폭 (KR 스코프)
    highs = fetch_new_highs("high", limit=40, country="KOR", min_mcap=floor)
    movers = fetch_top_movers(limit=40, min_mcap=floor, min_perf=5.0, country="KOR")
    header = (
        f"🇰🇷 <b>K-Bio Radar — {datetime.now().strftime('%Y-%m-%d')} 15:30</b>\n"
        f"<i>auto-run: universe={n_uni}, snapshot={n_hl} · 기준일 "
        f"{latest_run_date('KOR') or '—'} · 시총≥5천억</i>"
    )
    # 신고가 있으면 신고가 종목, 없으면 상승폭 종목을 동일하게 'AI 상승 동인' 분석
    # (= 미장의 52주 신고가 카드처럼, 신고가 없는 날엔 상승폭 종목에 '왜 올랐나'를 적용)
    focus = highs if not highs.empty else movers
    focus_label = ("52주 신고가" if not highs.empty
                   else "오늘 최대 상승폭 (52주 신고가 없음)")
    analysis = _highs_analysis(focus, max_n=20)
    if analysis:
        main_result = send(
            f"{header}\n\n📊 <b>{focus_label} — 투자 포인트 &amp; 상승 동인</b> "
            f"({len(focus)}종목)\n\n" + _markdown_to_html(analysis)
        )
    else:
        main_result = send(
            f"{header}\n\n📈 <b>52주 신고가</b> ({len(highs)}종목)\n"
            f"{_table_render(highs, max_rows=30)}"
        )
    # 두 목록(신고가 + 상승폭)은 항상 함께 발송
    send(f"🚀 <b>최대 상승폭 (1D)</b> ({len(movers)}종목)\n"
         f"{_table_render(movers, max_rows=30)}")

    # 종목별 카드(차트 + 오른 이유 뉴스) + 투자메모 — focus(신고가 or 상승폭) 시총 상위 기준.
    # 투자메모: 시총 상위 5개 중 최근 7일 내 미발송 종목만.
    import investment_report as _ir
    card_df = focus.drop_duplicates("ticker").sort_values(
        "market_cap", ascending=False, na_position="last")
    try:
        recent = _ir.recently_sent_tickers(7)
    except Exception:
        recent = set()
    memo_tickers = [t for t in card_df["ticker"].tolist()
                    if t and str(t).upper() not in recent][:5]
    try:
        cards, memos = send_ticker_cards(card_df, memo_tickers=memo_tickers, max_n=12)
        main_result["ticker_cards"] = cards
        main_result["investment_reports"] = memos
    except Exception as e:
        main_result["cards_error"] = str(e)
    return main_result


if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    import sys
    try:                                  # cp949 콘솔에서 이모지(🇰🇷 등) print 크래시 방지
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # test: 데이터 수집 없이 현재 캐시로 발송
        print(send(compose_report()))
    elif len(sys.argv) > 1 and sys.argv[1] == "kr":
        # 한국 15:30 푸시: KR 수집 + 발송
        print(daily_run_kr())
    else:
        # 스케줄러 모드: 수집 + 발송
        print(daily_run())
