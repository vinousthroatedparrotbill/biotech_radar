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
    """단일 메시지 발송. text가 길면 chunk로 분할."""
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
        r.raise_for_status()
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

    # ── 전체 신고가 ──
    all_highs = fetch_new_highs("high", limit=200)
    parts.append(f"📈 <b>전체 52w 신고가</b> ({len(all_highs)}종목)")
    parts.append(_table_render(all_highs, max_rows=40))
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


def daily_run() -> dict:
    """수집 + 요약 + 발송 — 스케줄러에서 호출."""
    from universe import load_universe
    from collectors.high_low import collect as hl_collect

    # 1) Universe 갱신
    n_uni = load_universe()
    # 2) 52w 신고가 갱신
    n_hl = hl_collect()
    # 3) 요약 + 발송
    text = compose_report()
    text = f"<i>auto-run: universe={n_uni}, snapshot={n_hl}</i>\n\n" + text
    return send(text)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # test: 데이터 수집 없이 현재 캐시로 발송
        print(send(compose_report()))
    else:
        # 스케줄러 모드: 수집 + 발송
        print(daily_run())
