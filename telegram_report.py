"""텔레그램 일일 요약 — 신규 52w 신고가 + 전체 신고가 + 메모 변동."""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

_ENV_PATH = Path(__file__).parent / ".env"
TG_API = "https://api.telegram.org/bot{token}/sendMessage"
MAX_MSG_LEN = 4000   # Telegram 4096 limit, leave headroom


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


def _fmt_mcap_b(mcap_m) -> str:
    """million USD → 'X.XB' 표기."""
    if mcap_m is None or pd.isna(mcap_m):
        return "—"
    b = mcap_m / 1000.0
    if b >= 100:
        return f"${b:,.0f}B"
    return f"${b:,.1f}B"


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
        price = f"${close:,.2f}" if pd.notna(close) else "—"
        pct = f"{perf_1d:+.1f}%" if pd.notna(perf_1d) else "—"
        mcap_s = _fmt_mcap_b(mcap)
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


def send_investment_reports(tickers: list[str], max_n: int = 5) -> int:
    """신규 신고가 종목 시총 TOP max_n — TL;DR 5-10줄을 텔레그램 메시지로,
    full report는 PDF 첨부로 발송. 발송 성공 종목 수 반환."""
    import os
    import investment_report as ir
    from pdf_gen import render_pdf_to_file
    if not tickers:
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


def daily_run() -> dict:
    """수집 + 요약 + 발송 — 스케줄러에서 호출."""
    from universe import load_universe
    from collectors.high_low import collect as hl_collect
    from collectors.high_low import fetch_new_today_highs

    # 1) Universe 갱신
    n_uni = load_universe()
    # 2) 52w 신고가 갱신
    n_hl = hl_collect(industry_filter=None)
    # 3) 요약 + 발송
    text = compose_report()
    text = f"<i>auto-run: universe={n_uni}, snapshot={n_hl}</i>\n\n" + text
    main_result = send(text)

    # 4) Model Portfolio 일일 스냅샷 (전체 MP 수익률 + 편입 종목별)
    try:
        mp_chunks = send_portfolio_snapshots()
        main_result["mp_chunks"] = mp_chunks
    except Exception as e:
        main_result["mp_error"] = str(e)

    # 5) 신규 신고가 종목 자동 투자 메모 (시총 큰 순 TOP 5)
    new_today = fetch_new_today_highs(limit=100)
    if not new_today.empty:
        tickers = new_today["ticker"].tolist()
        sent_n = send_investment_reports(tickers, max_n=5)
        main_result["investment_reports"] = sent_n

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

    return main_result


if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # test: 데이터 수집 없이 현재 캐시로 발송
        print(send(compose_report()))
    else:
        # 스케줄러 모드: 수집 + 발송
        print(daily_run())
