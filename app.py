"""Biotech Radar — 미국 Healthcare 52주 신고가 + 종목 상세."""
from __future__ import annotations

import os
import streamlit as st

# Streamlit Cloud secrets → os.environ (다른 모듈들이 환경변수에서 읽음)
# 로컬 dev에선 secrets.toml 없으니 그냥 skip — .env 파일이 사용됨
try:
    for _k, _v in dict(st.secrets).items():
        if isinstance(_v, str) and _k not in os.environ:
            os.environ[_k] = _v
except Exception:
    pass

from datetime import datetime

import pandas as pd

import db
from db import init_db
from universe import load_universe, get_universe
from collectors.high_low import (
    collect as hl_collect, fetch_new_highs, latest_run_date,
    fetch_new_today_highs, fetch_top_movers,
)

st.set_page_config(page_title="Biotech Radar", layout="wide", page_icon="🧬")


# ───────────────────────── 로그인 게이트 ─────────────────────────
def _check_auth() -> bool:
    """APP_PASSWORD가 .env/secrets에 있으면 첫 진입 시 비밀번호 요구.
    없으면 로그인 X (개발 모드)."""
    if st.session_state.get("authed"):
        return True
    expected = (os.environ.get("APP_PASSWORD") or "").strip()
    if not expected:
        return True   # 비밀번호 미설정 = 인증 비활성화

    # 로그인 화면
    st.markdown(
        """
        <div style="max-width: 400px; margin: 8rem auto; text-align: center;">
          <h1 style="color: #0a3d3a;">🧬 Biotech Radar</h1>
          <p style="color: #5b6f6e;">접근하려면 비밀번호 입력</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    cc = st.columns([1, 2, 1])
    with cc[1]:
        with st.form("login_form"):
            pw = st.text_input("Password", type="password", label_visibility="collapsed",
                               placeholder="비밀번호")
            if st.form_submit_button("Login", type="primary", use_container_width=True):
                if pw == expected:
                    st.session_state["authed"] = True
                    st.rerun()
                else:
                    st.error("틀린 비밀번호")
    return False


if not _check_auth():
    st.stop()

init_db()

# ───────────────────────── 디자인 (CSS 인젝션) ─────────────────────────
st.markdown("""
<style>
  /* 전체 톤 — 차분한 다크그린 + 투명한 카드 */
  .stApp {
    background: linear-gradient(180deg, #f8fafb 0%, #eef2f4 100%);
  }
  /* 메인 타이틀 영역 — hero */
  h1 {
    color: #0a3d3a !important;
    font-weight: 700 !important;
    letter-spacing: -0.5px;
  }
  /* hero (다크그린 배너) 안의 h1은 무조건 흰색 */
  .hero-banner h1,
  .hero-banner h1 * {
    color: #ffffff !important;
  }
  h3 {
    color: #134e4a !important;
    font-weight: 600 !important;
  }
  /* 사이드바 */
  section[data-testid="stSidebar"] {
    background: #0d3b3a;
    color: #e0f2f1;
  }
  section[data-testid="stSidebar"] .stMarkdown,
  section[data-testid="stSidebar"] h1,
  section[data-testid="stSidebar"] [data-testid="stCaptionContainer"] {
    color: #e0f2f1 !important;
  }
  /* 사이드바 버튼도 메인처럼 — 투명 배경, 글자만. 호버 때만 옅은 강조 */
  section[data-testid="stSidebar"] [data-testid^="stBaseButton-"] {
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
    color: #e0f2f1 !important;
    font-weight: 500 !important;
    text-align: left !important;
    justify-content: flex-start !important;
    padding: 0.4rem 0.6rem !important;
    transition: background 0.12s;
  }
  section[data-testid="stSidebar"] [data-testid^="stBaseButton-"]:hover {
    background: rgba(255, 255, 255, 0.08) !important;
    color: #ffffff !important;
  }
  section[data-testid="stSidebar"] [data-testid^="stBaseButton-"]:focus {
    outline: 1px solid rgba(255, 255, 255, 0.25) !important;
    box-shadow: none !important;
  }
  /* 메인 영역 + 다이얼로그 모든 버튼 — 투명, 글자만 보이게.
     호버 때만 옅은 배경색 + 색 강조. (사이드바 버튼은 위 규칙으로 별도 처리) */
  div[data-testid="stMainBlockContainer"] [data-testid^="stBaseButton-"],
  div[data-testid="stDialog"] [data-testid^="stBaseButton-"] {
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
    color: #0a3d3a !important;
    font-weight: 500 !important;
    text-align: left !important;
    justify-content: flex-start !important;
    padding: 0.35rem 0.6rem !important;
    transition: background 0.12s, color 0.12s;
  }
  div[data-testid="stMainBlockContainer"] [data-testid^="stBaseButton-"]:hover,
  div[data-testid="stDialog"] [data-testid^="stBaseButton-"]:hover {
    background: rgba(20, 78, 74, 0.07) !important;
    color: #0a3d3a !important;
  }
  div[data-testid="stMainBlockContainer"] [data-testid^="stBaseButton-"]:focus,
  div[data-testid="stDialog"] [data-testid^="stBaseButton-"]:focus {
    box-shadow: none !important;
    outline: 1px solid rgba(20, 78, 74, 0.25) !important;
  }
  /* primary 버튼 — 탭 active 상태용. 다크그린 배경 + 흰 글자 */
  div[data-testid="stMainBlockContainer"] [data-testid="stBaseButton-primary"],
  div[data-testid="stDialog"] [data-testid="stBaseButton-primary"] {
    background: #134e4a !important;
    color: #ffffff !important;
    font-weight: 700 !important;
    border-radius: 8px !important;
  }
  div[data-testid="stMainBlockContainer"] [data-testid="stBaseButton-primary"]:hover,
  div[data-testid="stDialog"] [data-testid="stBaseButton-primary"]:hover {
    background: #0a3d3a !important;
    color: #ffffff !important;
  }
  /* 표 헤더 정렬 버튼 — 굵게만 표시 (배경 X) */
  div[data-testid="stMainBlockContainer"] [data-testid="stHorizontalBlock"]:first-of-type [data-testid^="stBaseButton-"] {
    font-weight: 700 !important;
    color: #134e4a !important;
  }
  /* 모달 다이얼로그 */
  div[data-testid="stDialog"] {
    border-radius: 12px;
  }
  /* expander 헤더 */
  details summary {
    font-weight: 600 !important;
    color: #134e4a !important;
  }
  /* 메트릭 / caption */
  [data-testid="stCaptionContainer"] {
    color: #5b6f6e;
  }
  /* divider 스타일 */
  hr {
    border-color: #cbd9d6;
  }
</style>
""", unsafe_allow_html=True)

# ───────────────────────── sidebar ─────────────────────────
st.sidebar.title("Biotech Radar")
st.sidebar.caption("글로벌 Healthcare ≥ $1.5B")

def _close_modal():
    """비-행 클릭(사이드바·정렬 헤더·라디오 변경) 시 모달 자동 재팝업 방지."""
    st.session_state["detail_open"] = False


def _go_main_tab(tab: str):
    _close_modal()
    st.session_state["page"] = "main"
    st.session_state["main_tab"] = tab
    st.session_state["_force_tab"] = tab   # 탭 라디오 위젯 상태 동기화 신호
    st.rerun()


if st.sidebar.button("🏠 메인"):
    _close_modal()
    st.session_state["page"] = "main"
    st.rerun()

if st.sidebar.button("📈 52주 신고가"):
    _go_main_tab("high")

if st.sidebar.button("🚀 상승폭 최대"):
    _go_main_tab("top_movers")

if st.sidebar.button("📰 데일리 뉴스"):
    _go_main_tab("daily_news")

if st.sidebar.button("📝 메모 타임라인"):
    _go_main_tab("memos")

if st.sidebar.button("⭐ 관심종목"):
    _close_modal()
    st.session_state["page"] = "watchlist"
    st.rerun()

if st.sidebar.button("💼 Model Portfolio"):
    _go_main_tab("portfolios")

if st.sidebar.button("📅 카탈리스트"):
    _go_main_tab("catalysts")

st.sidebar.divider()
universe_count = len(get_universe())
st.sidebar.caption(f"현재 universe: {universe_count}종목")
last = latest_run_date()
st.sidebar.caption(f"마지막 신고가 갱신: {last or '—'}")


def _stats_counts():
    """메모/관심종목 카운트."""
    from db import connect
    with connect() as conn:
        m = conn.execute("SELECT COUNT(*) AS n FROM memos").fetchone()["n"]
        w = conn.execute("SELECT COUNT(*) AS n FROM watchlist").fetchone()["n"]
    return m, w


try:
    _memo_n, _watch_n = _stats_counts()
    st.sidebar.caption(f"메모: {_memo_n}건  ·  관심종목: {_watch_n}종목")
except Exception:
    pass

# ── 사이드바 좌하단: Universe 갱신 + 텔레그램 + 모바일 모드 ──
st.sidebar.markdown("<div style='height:1.5rem;'></div>", unsafe_allow_html=True)
if st.sidebar.button("🔄 Universe 갱신", key="uni_refresh",
                     help="Finviz에서 Healthcare 전 종목 다시 로드"):
    _close_modal()
    with st.spinner("Finviz 호출 중..."):
        try:
            n = load_universe()
            st.sidebar.success(f"{n}종목 로드")
        except Exception as e:
            st.sidebar.error(f"실패: {e}")

if st.sidebar.button("📨 텔레그램 발송", key="tg_send",
                     help="현재 데이터로 즉시 텔레그램 요약 발송"):
    _close_modal()
    with st.spinner("텔레그램 발송 중..."):
        try:
            from telegram_report import send, compose_report
            send(compose_report())
            st.sidebar.success("발송됨")
        except Exception as e:
            st.sidebar.error(f"실패: {e}")

_is_mobile = st.session_state.get("mobile_mode", False)
_mobile_label = "🖥 데스크톱 보기" if _is_mobile else "📱 모바일 보기"
if st.sidebar.button(_mobile_label, key="toggle_mobile",
                     help="컬럼 축소 + 큰 글자 + 작은 패딩"):
    st.session_state["mobile_mode"] = not _is_mobile
    st.rerun()


# ───────────────────────── 메모 타임라인 페이지 ─────────────────────────
def _section_memos():
    """탭 컨텐츠 — 메모 타임라인."""
    from memo import timeline as memo_timeline

    @st.cache_data(ttl=600)
    def _cached_timeline(limit: int):
        return memo_timeline(limit=limit)

    cols = st.columns([1, 1, 6])
    with cols[0]:
        limit = st.selectbox("표시 개수", [20, 50, 100, 300], index=1)
    with cols[1]:
        if st.button("🔄 가격 재계산"):
            _cached_timeline.clear()
            st.rerun()

    try:
        timeline = _cached_timeline(limit)
    except Exception as e:
        st.error(f"타임라인 로드 실패: {e}")
        return
    if not timeline:
        st.info("아직 메모가 없습니다. 종목 모달에서 메모를 추가해 보세요.")
        return

    st.caption(f"{len(timeline)}건")

    for m in timeline:
        with st.container(border=True):
            top = st.columns([2, 5, 3])
            with top[0]:
                ts = m["created_at"].replace("T", " ")[:16]
                st.markdown(
                    f"<div style='color:#888; font-size:0.85em;'>{ts}</div>"
                    f"<div style='font-weight:700; font-size:1.05em; color:#0a3d3a;'>"
                    f"{m['ticker']}</div>"
                    f"<div style='color:#5b6f6e; font-size:0.85em;'>{m.get('name') or ''}</div>",
                    unsafe_allow_html=True,
                )
                btn_cols = st.columns(2)
                if btn_cols[0].button("종목", key=f"open_{m['id']}",
                                       use_container_width=True):
                    st.session_state["detail_ticker"] = m["ticker"]
                    st.session_state["detail_name"] = m.get("name") or m["ticker"]
                    st.session_state["detail_open"] = True
                    st.rerun()
                # 삭제 — 두 번 클릭 보호 (한 번 누르면 confirm 모드)
                confirm_key = f"del_confirm_{m['id']}"
                if st.session_state.get(confirm_key):
                    if btn_cols[1].button("✓ 확정", key=f"del_yes_{m['id']}",
                                           use_container_width=True, type="primary"):
                        from memo import delete as memo_delete
                        memo_delete(m["id"])
                        st.session_state.pop(confirm_key, None)
                        _cached_timeline.clear()
                        st.rerun()
                else:
                    if btn_cols[1].button("🗑", key=f"del_{m['id']}",
                                           use_container_width=True,
                                           help="메모 삭제 (한번 더 누르면 확정)"):
                        st.session_state[confirm_key] = True
                        st.rerun()

            with top[1]:
                st.markdown(
                    f"<div style='font-size:1em; line-height:1.55; padding-top:0.2rem;'>"
                    f"{m['body']}</div>",
                    unsafe_allow_html=True,
                )
                if m.get("updated_at") and m.get("updated_at") != m.get("created_at"):
                    st.caption(f"수정: {m['updated_at'].replace('T',' ')[:16]}")

            with top[2]:
                cp = m.get("change_pct")
                pa = m.get("price_at_create")
                pn = m.get("price_now")
                if cp is None or pa is None or pn is None:
                    st.caption("주가 데이터 없음")
                else:
                    color = "#26a69a" if cp > 0 else ("#ef5350" if cp < 0 else "#666")
                    arrow = "▲" if cp > 0 else ("▼" if cp < 0 else "—")
                    st.markdown(
                        f"<div style='text-align:right;'>"
                        f"<div style='color:{color}; font-weight:700; font-size:1.4em;'>"
                        f"{arrow} {cp:+.1f}%</div>"
                        f"<div style='color:#888; font-size:0.85em; margin-top:0.2rem;'>"
                        f"${pa:,.2f} → <b>${pn:,.2f}</b></div>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )


# ───────────────────────── 종목 상세 모달 ─────────────────────────
@st.dialog("📊 종목 상세", width="large")
def _detail_dialog(ticker: str, name: str):
    render_stock_detail(ticker, name)


@st.cache_data(ttl=300, show_spinner=False)
def _cached_fetch_chart(ticker: str, period: str, interval: str):
    """5분 캐시 — 모달 재오픈 시 즉시 응답."""
    from prices import fetch_chart
    return fetch_chart(ticker, period, interval)


@st.cache_data(ttl=600, show_spinner=False)
def _cached_top_pipelines(ticker: str, name: str, days: int):
    from news import top_pipelines, news_count
    return news_count(ticker, name, days), top_pipelines(ticker, name, days)


@st.cache_data(ttl=600, show_spinner=False)
def _cached_pdf_links(ir_url: str):
    from ir_pdfs import fetch_pdf_links
    return fetch_pdf_links(ir_url, limit=40)


def _ensure_urls_discovered(ticker: str) -> None:
    """모달 열 때 IR/Pipeline URL 비어있으면 자동 탐색.
    한쪽만 비어있어도 시도 (없는 쪽만 채움). 세션당 1회만."""
    import ticker_urls
    urls = ticker_urls.get(ticker)
    have_ir = bool(urls.get("ir_url"))
    have_pl = bool(urls.get("pipeline_url"))
    if have_ir and have_pl:
        return   # 둘 다 있으면 skip
    flag = f"_auto_disc_{ticker}"
    if st.session_state.get(flag):
        return
    st.session_state[flag] = True
    try:
        from discover import discover as auto_discover
        with st.spinner("IR/Pipeline URL 자동 탐색..."):
            result = auto_discover(ticker)
        # 비어있는 쪽만 새로 채움
        new_ir = urls.get("ir_url") or result.get("ir_url", "")
        new_pl = urls.get("pipeline_url") or result.get("pipeline_url", "")
        if (new_ir != urls.get("ir_url", "")) or (new_pl != urls.get("pipeline_url", "")):
            ticker_urls.set_urls(ticker, ir_url=new_ir, pipeline_url=new_pl)
            if new_ir and not have_ir:
                st.session_state[f"ir_in_{ticker}"] = new_ir
            if new_pl and not have_pl:
                st.session_state[f"pl_in_{ticker}"] = new_pl
    except Exception:
        pass


def render_stock_detail(ticker: str, name: str):
    import plotly.graph_objects as go
    import watchlist as wl
    import excluded as excl
    from prices import PERIOD_LABELS

    _ensure_urls_discovered(ticker)

    # 헤더 + 관심/제외 토글
    top = st.columns([6, 1.5, 1.5])
    with top[0]:
        st.markdown(f"### {name} ({ticker})")
    with top[1]:
        if wl.is_watched(ticker):
            if st.button("★ 관심 해제", key=f"wl_off_{ticker}", use_container_width=True):
                wl.remove(ticker)
                st.rerun()
        else:
            if st.button("☆ 관심종목", key=f"wl_on_{ticker}", use_container_width=True):
                wl.add(ticker)
                st.rerun()
    with top[2]:
        if excl.is_excluded(ticker):
            if st.button("✓ 제외 해제", key=f"ex_off_{ticker}", use_container_width=True):
                excl.remove(ticker)
                st.rerun()
        else:
            if st.button("🚫 제외", key=f"ex_on_{ticker}", use_container_width=True,
                         help="신고가/상승폭 리스트에서 영구 숨김 (비-biotech 종목용)"):
                excl.add(ticker, note="user excluded")
                st.rerun()

    cc = st.columns([2, 2])
    with cc[0]:
        period = st.radio(
            "기간", options=list(PERIOD_LABELS.keys()), index=4, horizontal=True,
            format_func=lambda k: PERIOD_LABELS[k], key=f"prd_{ticker}",
        )
    with cc[1]:
        interval_label = st.radio(
            "봉", options=["일봉", "주봉", "월봉"], horizontal=True, key=f"int_{ticker}",
        )
    interval_map = {"일봉": "1d", "주봉": "1wk", "월봉": "1mo"}
    interval = interval_map[interval_label]

    with st.spinner("OHLCV 다운로드..."):
        try:
            hist = _cached_fetch_chart(ticker, period, interval)
        except Exception as e:
            st.error(f"차트 로드 실패: {e}")
            hist = None

    if hist is None or hist.empty:
        st.info("차트 데이터 없음")
    else:
        fig = go.Figure()
        if period == "1d":
            fig.add_trace(go.Scatter(
                x=hist.index, y=hist["Close"], mode="lines",
                line=dict(color="#1976d2", width=1.6), name="Price",
            ))
        else:
            fig.add_trace(go.Candlestick(
                x=hist.index, open=hist["Open"], high=hist["High"],
                low=hist["Low"], close=hist["Close"], name=ticker,
                increasing_line_color="#26a69a", decreasing_line_color="#ef5350",
            ))
            for w, color in [(20, "#ff9800"), (60, "#9c27b0"), (120, "#607d8b")]:
                col = f"MA{w}"
                if col in hist.columns:
                    fig.add_trace(go.Scatter(
                        x=hist.index, y=hist[col], mode="lines", name=f"MA{w}",
                        line=dict(color=color, width=1.2),
                    ))
        fig.update_layout(
            height=440, margin=dict(l=0, r=0, t=10, b=0),
            xaxis_rangeslider_visible=False,
            legend=dict(orientation="h", y=1.05, x=0),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── 3-1, 3-2, 3-3 토글 섹션들 ──
    st.divider()
    _render_url_settings(ticker)
    _render_ir_section(ticker, name)
    _render_pipeline_section(ticker)
    _render_news_section(ticker, name)
    _render_catalyst_section(ticker)
    _render_insider_section(ticker)

    # ── 메모 (토글들 밑) ──
    st.divider()
    _render_memo_section(ticker)


def _render_catalyst_section(ticker: str):
    """종목별 다가오는 카탈리스트 + IR 자료에서 추출된 회사 공개 마일스톤."""
    import catalysts as cat
    import ir_milestones as irm
    df = cat.get_catalysts(ticker=ticker, days=365)
    co_df = irm.get_company_events(ticker)
    # 어닝콜 카탈리스트 (별도 표시)
    ec_df = db.pd_read_sql(
        "SELECT * FROM catalysts WHERE ticker=? AND event_type='earnings_call' "
        "ORDER BY event_date ASC",
        params=(ticker.upper(),),
    )
    badge_parts = []
    if not df.empty:
        badge_parts.append(f"{len(df)}건")
    if not ec_df.empty:
        badge_parts.append(f"어닝콜 {len(ec_df)}")
    if not co_df.empty:
        badge_parts.append(f"IR {len(co_df)}")
    badge = f"({', '.join(badge_parts)})" if badge_parts else ""
    with st.expander(f"📅 다가오는 카탈리스트 {badge}", expanded=False):
        if df.empty and co_df.empty and ec_df.empty:
            st.caption("저장된 카탈리스트 없음.")
        if not df.empty:
            for _, r in df.iterrows():
                tt = r.get("event_type", "")
                emoji = {"pdufa": "💊", "earnings": "📊",
                         "clinical_readout": "🧪", "conference": "🎤",
                         "company_event": "📑",
                         "earnings_call": "🎙️"}.get(tt, "📅")
                st.markdown(
                    f"- {emoji} **{r['event_date']}** · {r['title'][:160]}  "
                    f"<span style='opacity:0.55; font-size:0.85em'>· {tt}</span>",
                    unsafe_allow_html=True,
                )
        if not ec_df.empty:
            st.markdown("---")
            st.caption("🎙️ 최근 어닝콜에서 회사가 공개한 forward-looking 멘션")
            for _, r in ec_df.iterrows():
                d = r.get("description")
                d = d if isinstance(d, str) else ""
                date_hint = d.split("·")[0].replace("date_hint:", "").strip() or r.get("event_date", "")
                st.markdown(
                    f"- **[{date_hint}]** {r['title']}",
                )
        if not co_df.empty:
            st.markdown("---")
            st.caption("📑 IR 자료에서 회사가 자체 공개한 milestones")
            for _, r in co_df.iterrows():
                d = r.get("description")
                d = d if isinstance(d, str) else ""
                date_hint = d.replace("date_hint:", "").strip() or r.get("event_date", "")
                st.markdown(
                    f"- **[{date_hint}]** {r['title']}",
                )
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 카탈리스트 갱신", key=f"cat_rf_{ticker}",
                         use_container_width=True):
                with st.spinner("..."):
                    cat.fetch_earnings_dates([ticker])
                    cat.fetch_clinical_completions([ticker])
                    cat.refresh_all(watchlist_only=False)
                st.rerun()
        with col2:
            if st.button("🔍 IR PDF 마일스톤 추출", key=f"irm_rf_{ticker}",
                         use_container_width=True):
                with st.spinner("IR PDF 분석 (10-30초)..."):
                    result = irm.extract_for_ticker(ticker, save=True)
                if result.get("error"):
                    st.warning(f"⚠️ {result['error']}")
                else:
                    st.success(
                        f"✓ {len(result.get('milestones', []))}개 추출 — "
                        f"{result.get('deck_title', '')[:60]}"
                    )
                    st.rerun()


def _render_insider_section(ticker: str):
    """SEC Form 4 인사이더 매매 (OpenInsider)."""
    import insiders as ins
    summary = ins.summary_for_ticker(ticker, days=180)
    if summary.get("trades", 0) == 0:
        label = f"👥 인사이더 매매 (180일)"
    else:
        net = summary["net_value"]
        sign = "📈" if net > 0 else "📉" if net < 0 else "—"
        label = (f"👥 인사이더 매매 (180일) {sign} "
                 f"{summary['trades']}건 · 매수 ${summary['buy_value']/1e6:.1f}M · "
                 f"매도 ${abs(summary['sell_value'])/1e6:.1f}M")
    with st.expander(label, expanded=False):
        if summary["trades"] == 0:
            st.info("최근 180일 인사이더 매매 없음.")
        else:
            df = ins.get_insider_trades(ticker, days=180)
            if not df.empty:
                view = df[["trade_date", "insider_name", "title",
                           "transaction", "shares", "price", "value_usd"]].copy()
                view.columns = ["일자", "인사이더", "직책", "거래", "수량", "가격", "금액(USD)"]
                view["수량"] = view["수량"].apply(
                    lambda x: f"{int(x):,}" if pd.notna(x) else "—"
                )
                view["가격"] = view["가격"].apply(
                    lambda x: f"${x:.2f}" if pd.notna(x) else "—"
                )
                view["금액(USD)"] = view["금액(USD)"].apply(
                    lambda x: f"${x/1e3:+,.0f}K" if pd.notna(x) else "—"
                )
                st.dataframe(view, use_container_width=True, hide_index=True, height=300)
        if st.button("🔄 갱신", key=f"ins_rf_{ticker}", use_container_width=True):
            with st.spinner("OpenInsider fetching..."):
                ins.refresh_for_tickers([ticker])
            st.rerun()


def _render_url_settings(ticker: str):
    import ticker_urls
    from discover import discover as auto_discover
    urls = ticker_urls.get(ticker)

    # 세션 상태에 초기값 (저장된 값 또는 빈 문자열) — 자동 탐색 결과로 덮어쓸 수 있게
    ir_key = f"ir_in_{ticker}"
    pl_key = f"pl_in_{ticker}"
    if ir_key not in st.session_state:
        st.session_state[ir_key] = urls.get("ir_url", "")
    if pl_key not in st.session_state:
        st.session_state[pl_key] = urls.get("pipeline_url", "")

    with st.expander(f"⚙️ {ticker} URL 설정", expanded=not urls):
        cols = st.columns([1, 1])
        with cols[0]:
            if st.button("🔍 자동 탐색", key=f"discover_{ticker}",
                         help="yfinance 회사 홈페이지 + anchor 텍스트 패턴 매칭"):
                with st.spinner("홈페이지 분석..."):
                    result = auto_discover(ticker)
                if "_error" in result and "ir_url" not in result and "pipeline_url" not in result:
                    st.warning(result["_error"])
                else:
                    if "ir_url" in result:
                        st.session_state[ir_key] = result["ir_url"]
                    if "pipeline_url" in result:
                        st.session_state[pl_key] = result["pipeline_url"]
                    st.success("탐색 완료 — 확인 후 저장")
                    st.rerun()
        with cols[1]:
            if st.button("💾 저장", key=f"url_save_{ticker}", type="primary"):
                ticker_urls.set_urls(
                    ticker,
                    ir_url=st.session_state[ir_key],
                    pipeline_url=st.session_state[pl_key],
                )
                st.success("저장됨")
                st.rerun()

        st.text_input("IR 페이지 URL", key=ir_key,
                      placeholder="https://investors.example.com/...")
        st.text_input("Pipeline 페이지 URL", key=pl_key,
                      placeholder="https://www.example.com/pipeline")


def _render_ir_section(ticker: str, name: str):
    import ticker_urls
    from ir_pdfs import fetch_pdf_links

    with st.expander("📑 IR / Events & Presentations", expanded=False):
        urls = ticker_urls.get(ticker)
        ir_url = urls.get("ir_url")
        if not ir_url:
            st.info("위 URL 설정에 IR Events & Presentations URL을 입력하거나 🔍 자동 탐색을 누르세요.")
            return

        st.markdown(f"**IR 페이지**: [{ir_url}]({ir_url})")

        with st.spinner("IR 자료 URL 수집..."):
            assets = _cached_pdf_links(ir_url)
        err = next((l.get("_error") for l in assets if l.get("_error")), None)

        if err or not assets:
            st.caption(f"⚠️ 자동 추출 실패 — JS 렌더/봇 차단 사이트 가능성. 위 IR 페이지 링크로 새 창에서 확인.")
            return

        st.caption(f"📊 추출된 자료 {len(assets)}건")
        for a in assets:
            kind = a.get("kind", "")
            kind_tag = f" [{kind}]" if kind else ""
            date = a.get("date_hint") or "—"
            ext = a.get("asset_type", "?").upper()
            title = a.get("title") or a["url"].split("/")[-1]
            st.markdown(
                f"- `[{ext}]` {date}{kind_tag} · [{title[:120]}]({a['url']})"
            )


def _render_pipeline_section(ticker: str):
    import ticker_urls

    with st.expander("🧪 Pipeline 페이지", expanded=False):
        urls = ticker_urls.get(ticker)
        pl_url = urls.get("pipeline_url")
        if not pl_url:
            st.info("위 URL 설정에 Pipeline URL을 입력하거나 🔍 자동 탐색을 누르세요.")
            return
        st.markdown(f"**Pipeline 페이지**: [{pl_url}]({pl_url})")
        st.caption("아래는 임베드 시도 — 회색 빈 박스면 사이트가 iframe 차단함 (위 링크 사용)")
        st.markdown(
            f'<iframe src="{pl_url}" width="100%" height="650" '
            f'style="border:1px solid #cbd9d6; border-radius:8px;"></iframe>',
            unsafe_allow_html=True,
        )


def _render_news_section(ticker: str, name: str):
    with st.expander("📰 뉴스 멘션 — 최근 6개월 가장 많이 언급된 파이프라인 TOP 3", expanded=False):
        st.caption(f"검색: '{name}' · Yahoo + Finviz + Google News (6개월)")
        with st.spinner("뉴스 분석..."):
            try:
                nc, top = _cached_top_pipelines(ticker, name, 180)
            except Exception as e:
                st.error(str(e))
                return
        st.caption(f"총 {nc}건의 뉴스 분석")
        if not top:
            st.info("언급된 약물명/파이프라인 없음")
            return
        cols = st.columns(3)
        for i, t in enumerate(top):
            with cols[i]:
                moa = t["moa"] if t["moa"] != "—" else "기전 미상"
                st.markdown(
                    f"<div style='border:1px solid #cbd9d6; border-radius:8px; "
                    f"padding:0.8rem; background:#f8fafb;'>"
                    f"<div style='font-size:1.15em; font-weight:700; color:#0a3d3a;'>"
                    f"{t['drug']}</div>"
                    f"<div style='color:#5b6f6e; font-size:0.85em; margin-top:0.2rem;'>"
                    f"<b>{t['mentions']}회</b> 언급</div>"
                    f"<div style='margin-top:0.5rem; padding:0.25rem 0.5rem; "
                    f"background:#134e4a; color:#fff; border-radius:4px; "
                    f"display:inline-block; font-size:0.8em;'>"
                    f"{moa}</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )


def _render_memo_section(ticker: str):
    from memo import add as memo_add, update as memo_update, delete as memo_delete, list_for

    st.markdown("##### 📝 메모")

    # 새 메모
    with st.form(f"new_memo_{ticker}", clear_on_submit=True):
        new_body = st.text_area("새 메모", key=f"new_body_{ticker}",
                                placeholder="여기에 생각 적기…", height=80)
        if st.form_submit_button("추가"):
            if new_body.strip():
                memo_add(ticker, new_body)
                st.rerun()

    # 기존 메모들
    memos = list_for(ticker)
    if not memos:
        st.caption("(아직 메모 없음)")
        return
    for m in memos:
        with st.container(border=True):
            ts = m["updated_at"]
            edited = m["updated_at"] != m["created_at"]
            label = f"{ts}" + (" (수정됨)" if edited else "")
            st.caption(label)

            edit_key = f"edit_{m['id']}"
            in_edit = st.session_state.get(edit_key, False)

            if in_edit:
                txt = st.text_area("내용", value=m["body"], key=f"body_{m['id']}", height=80)
                cc = st.columns([1, 1, 6])
                if cc[0].button("저장", key=f"save_{m['id']}", type="primary"):
                    memo_update(m["id"], txt)
                    st.session_state[edit_key] = False
                    st.rerun()
                if cc[1].button("취소", key=f"cancel_{m['id']}"):
                    st.session_state[edit_key] = False
                    st.rerun()
            else:
                st.markdown(m["body"])
                cc = st.columns([1, 1, 6])
                if cc[0].button("수정", key=f"editbtn_{m['id']}"):
                    st.session_state[edit_key] = True
                    st.rerun()
                if cc[1].button("삭제", key=f"del_{m['id']}"):
                    memo_delete(m["id"])
                    st.rerun()


# ───────────────────────── main: 신고가 테이블 페이지 ─────────────────────────
SORT_KEYS = {
    "회사명": "name", "현재가": "close", "시총($M)": "market_cap",
    "1D": "perf_1d", "7D": "perf_7d", "1M": "perf_1m",
    "3M": "perf_3m", "6M": "perf_6m", "1Y": "perf_1y",
    "52w최고": "high_52w",
}


def _render_table(df: pd.DataFrame):
    mobile = st.session_state.get("mobile_mode", False)
    if mobile:
        # 모바일: 5컬럼 (Ticker / 회사명 / 현재가 / 1D / 시총)
        schema: list[tuple[str, int]] = [
            ("Ticker", 2), ("회사명", 5),
            ("현재가", 3), ("1D", 2), ("시총($M)", 2),
        ]
    else:
        schema: list[tuple[str, int]] = [
            ("Ticker", 1), ("회사명", 5),
            ("현재가", 2), ("시총($M)", 2),
            ("1D", 1), ("7D", 1), ("1M", 1), ("3M", 1), ("6M", 1), ("1Y", 1),
            ("52w최고", 2),
        ]
    weights = [w for _, w in schema]

    sort_col = st.session_state.get("table_sort_col")
    sort_dir = st.session_state.get("table_sort_dir", "desc")

    if sort_col and sort_col in df.columns:
        df = df.sort_values(sort_col, ascending=(sort_dir == "asc"), na_position="last").reset_index(drop=True)

    hdr = st.columns(weights, vertical_alignment="center")
    for i, (label, _) in enumerate(schema):
        sort_key = SORT_KEYS.get(label)
        if sort_key is None:
            hdr[i].markdown(f"**{label}**")
            continue
        arrow = ""
        if sort_col == sort_key:
            arrow = " ▼" if sort_dir == "desc" else " ▲"
        if hdr[i].button(f"{label}{arrow}", key=f"sort_{sort_key}",
                         use_container_width=True):
            _close_modal()
            if st.session_state.get("table_sort_col") == sort_key:
                st.session_state["table_sort_dir"] = "asc" if sort_dir == "desc" else "desc"
            else:
                st.session_state["table_sort_col"] = sort_key
                st.session_state["table_sort_dir"] = "desc"
            st.rerun()

    st.divider()

    def color_pct(v):
        if pd.isna(v) or v is None:
            return "—"
        c = "#26a69a" if v > 0 else ("#ef5350" if v < 0 else "#666")
        return f"<span style='color:{c}; font-weight:500;'>{v:+.1f}%</span>"

    with st.container(height=620):
        for idx, row in df.iterrows():
            cells = st.columns(weights, vertical_alignment="center")
            cells[0].caption(row["ticker"])
            if cells[1].button(
                str(row["name"] or row["ticker"]),
                key=f"btn_{row['ticker']}_{idx}",
                use_container_width=True,
            ):
                prev = st.session_state.get("detail_ticker")
                if prev and prev != row["ticker"]:
                    for k in list(st.session_state.keys()):
                        if k.startswith(("prd_", "int_", "ir_in_", "pl_in_", "asset_pick_")) and prev in k:
                            del st.session_state[k]
                st.session_state["detail_ticker"] = row["ticker"]
                st.session_state["detail_name"] = row["name"]
                st.session_state["detail_open"] = True
                st.rerun()
            if mobile:
                cells[2].write(f"${row['close']:,.2f}" if pd.notna(row["close"]) else "—")
                cells[3].markdown(color_pct(row["perf_1d"]), unsafe_allow_html=True)
                mcap = row["market_cap"]
                cells[4].write(f"{mcap/1000:,.1f}b" if pd.notna(mcap) and mcap >= 1000
                               else (f"{mcap:,.0f}m" if pd.notna(mcap) else "—"))
            else:
                cells[2].write(f"${row['close']:,.2f}" if pd.notna(row["close"]) else "—")
                mcap = row["market_cap"]
                cells[3].write(f"{mcap:,.0f}" if pd.notna(mcap) else "—")
                for j, k in enumerate(["perf_1d", "perf_7d", "perf_1m", "perf_3m", "perf_6m", "perf_1y"]):
                    cells[4 + j].markdown(color_pct(row[k]), unsafe_allow_html=True)
                cells[10].write(f"${row['high_52w']:,.2f}" if pd.notna(row["high_52w"]) else "—")


def _section_high():
    """탭 컨텐츠 — 52주 신고가."""
    if last is None:
        st.warning("아직 신고가 데이터 없음. 우측 '🔄 신고가 갱신' 먼저 실행.")
        return

    cc = st.columns([6, 2, 2])
    with cc[0]:
        view = st.radio(
            "구분", options=["new", "all"], horizontal=True,
            format_func=lambda v: "🆕 오늘 신규" if v == "new" else "📈 전체",
            on_change=_close_modal, key="hl_view",
        )
    with cc[1]:
        st.write("")
        if st.button("🔄 신고가 갱신", key="hl_refresh_btn",
                     help="universe 전체 yfinance OHLCV 일괄 다운로드 (~수분)"):
            _close_modal()
            with st.spinner("yfinance OHLCV 일괄 다운로드..."):
                try:
                    n = hl_collect(industry_filter=None)
                    st.success(f"{n}종목 처리")
                except Exception as e:
                    st.error(f"실패: {e}")
    with cc[2]:
        st.write("")
        if st.button("🔍 URL 자동 탐색", key="discover_all_btn",
                     help="universe 전체 종목의 IR/Pipeline URL 일괄 탐색 (~1~2분)"):
            _close_modal()
            import ticker_urls as _tu
            from discover import discover_batch
            with st.spinner("홈페이지 분석 중..."):
                try:
                    tks = get_universe()["ticker"].tolist()
                    results = discover_batch(tks, max_workers=10)
                    saved = 0
                    for tk, r in results.items():
                        if r.get("ir_url") or r.get("pipeline_url"):
                            _tu.set_urls(tk, ir_url=r.get("ir_url"),
                                         pipeline_url=r.get("pipeline_url"))
                            saved += 1
                    st.success(f"{saved}/{len(tks)}종목 URL 저장")
                except Exception as e:
                    st.error(f"실패: {e}")

    if view == "new":
        df = fetch_new_today_highs(limit=300)
        empty_msg = "오늘 신규로 52주 신고가를 찍은 종목 없음."
    else:
        df = fetch_new_highs("high", limit=500)
        empty_msg = "오늘 52주 신고가 종목 없음."

    if df.empty:
        st.info(empty_msg)
        return

    st.caption(f"{len(df)}종목 · 기준일 {last} · 📊 회사명을 클릭하면 모달로 차트+MA가 떠요.")
    _render_table(df)


def render_main_page():
    """메인 대시보드 — 4개 섹션 탭으로 전환."""
    st.markdown(
        f"""
        <div class="hero-banner" style="
          background: linear-gradient(135deg, #134e4a 0%, #0a3d3a 100%);
          color: #fff; padding: 1.5rem 2rem; border-radius: 12px;
          margin-bottom: 1.2rem;
          box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        ">
          <h1 style="margin:0;">🧬 Biotech Radar</h1>
          <div style="opacity:0.85; font-size:0.95rem; margin-top:0.3rem;">
            전체 조망 · {datetime.now():%Y-%m-%d %H:%M}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 탭 — 컴팩트 라디오 (사이드바 버튼이 외부에서 변경 가능)
    tab_options = ["high", "top_movers", "daily_news", "memos", "portfolios", "catalysts"]
    tab_labels = {
        "high": "📈 52주 신고가",
        "top_movers": "🚀 상승폭 최대",
        "daily_news": "📰 데일리 뉴스",
        "memos": "📝 메모 타임라인",
        "portfolios": "💼 MP 현황",
        "catalysts": "📅 카탈리스트",
    }
    # 사이드바에서 _force_tab을 set했으면 위젯 상태 강제 동기화
    if "_force_tab" in st.session_state:
        st.session_state["main_tab_radio"] = st.session_state.pop("_force_tab")
    elif "main_tab_radio" not in st.session_state:
        st.session_state["main_tab_radio"] = st.session_state.get("main_tab", "high")

    chosen = st.radio(
        "탭", options=tab_options,
        format_func=lambda k: tab_labels[k], horizontal=True,
        key="main_tab_radio", label_visibility="collapsed",
        on_change=_close_modal,
    )
    st.session_state["main_tab"] = chosen
    st.divider()

    if chosen == "high":
        _section_high()
    elif chosen == "top_movers":
        _section_top_movers()
    elif chosen == "daily_news":
        _section_daily_news()
    elif chosen == "memos":
        _section_memos()
    elif chosen == "portfolios":
        _section_portfolios()
    elif chosen == "catalysts":
        _section_catalysts()


# ───────────────────────── 카탈리스트 캘린더 ─────────────────────────
def _section_catalysts():
    import catalysts as cat
    import ir_milestones as irm
    st.subheader("📅 카탈리스트 캘린더")

    col_a, col_b, col_c, col_d = st.columns([1.2, 1, 1.2, 1])
    with col_a:
        days = st.selectbox(
            "기간",
            options=[14, 30, 60, 90, 180, 365],
            index=3,
            format_func=lambda d: f"{d}일",
            key="cat_days",
        )
    with col_b:
        type_options = {
            "전체": None,
            "PDUFA": ["pdufa"],
            "학회": ["conference"],
            "어닝": ["earnings"],
            "임상 데이터 공개": ["clinical_readout"],
            "회사 공개": ["company_event", "earnings_call"],
        }
        type_label = st.selectbox("타입", list(type_options.keys()), key="cat_type")
        types = type_options[type_label]
    with col_c:
        scope = st.selectbox(
            "범위",
            ["전체 (≥$1B 바이오텍)", "관심종목만"],
            key="cat_scope",
        )
    with col_d:
        st.markdown("<div style='height:1.7rem;'></div>", unsafe_allow_html=True)
        refresh_scope = "watchlist" if scope == "관심종목만" else "biotech_1b"
        btn_label = "🔄 갱신 (관심종목)" if refresh_scope == "watchlist" else "🔄 갱신 (전체, 30분+)"
        if st.button(btn_label, use_container_width=True, key="cat_refresh"):
            with st.spinner(
                f"카탈리스트 fetching ({refresh_scope})... "
                f"{'관심종목 ~1분' if refresh_scope == 'watchlist' else '$1B≥ ~30-60분'}"
            ):
                counts = cat.refresh_all(scope=refresh_scope)
            st.success(
                f"✓ PDUFA {counts['pdufa']} · 학회 {counts['conference']} · "
                f"어닝 {counts['earnings']} · 임상 데이터 공개 {counts['clinical_readout']} · "
                f"어닝콜 {counts.get('earnings_call', 0)}"
            )

    df = cat.get_catalysts(days=days, event_types=types)
    def _is_sectorwide(s):
        # 학회 같은 sector-wide 이벤트는 ticker=NULL/''
        return s.isna() | (s == "")
    if scope == "관심종목만" and not df.empty:
        import watchlist as wl
        wl_set = set(wl.list_all()["ticker"].tolist())
        df = df[df["ticker"].isin(wl_set) | _is_sectorwide(df["ticker"])]
    elif scope.startswith("전체") and not df.empty:
        # ≥$1B 바이오텍 + 학회
        from db import connect as _conn
        with _conn() as c:
            biotech_rows = c.execute(
                "SELECT ticker FROM ticker_master "
                "WHERE sector='Healthcare' AND market_cap >= 1000 "
                "AND country='USA'"
            ).fetchall()
        biotech_set = {r["ticker"] for r in biotech_rows}
        df = df[df["ticker"].isin(biotech_set) | _is_sectorwide(df["ticker"])]

    if df.empty:
        st.info("해당 조건의 카탈리스트 없음. 🔄 갱신 눌러 캐시 채우기.")
    else:
        st.caption(f"총 {len(df)}건 · 가까운 순")
        # 일자 표시 — description에 date_hint 있으면 우선 사용 (late 2026 같은 fuzzy 표기)
        import re as _re
        def _date_label(row):
            desc = row.get("description")
            if not isinstance(desc, str):
                desc = ""
            m = _re.search(r"date_hint:\s*([^·]+?)(?:\s*·|$)", desc)
            if m:
                return m.group(1).strip()
            return row.get("event_date") or ""
        df = df.copy()
        df["_disp_date"] = df.apply(_date_label, axis=1)
        view = df[["_disp_date", "event_date", "ticker", "event_type", "title",
                   "therapy_area", "source"]].copy()
        view.columns = ["일자", "정렬일", "티커", "타입", "제목", "분야", "소스"]
        view["티커"] = view["티커"].fillna("—")
        view["분야"] = view["분야"].fillna("—")
        st.dataframe(
            view.drop(columns=["정렬일"]),
            use_container_width=True, hide_index=True, height=520,
        )

    # IR 마일스톤 추출 — watchlist 종목별
    st.divider()
    st.subheader("📑 IR 자료 카탈리스트 추출 (회사 자체 공개)")
    st.caption("watchlist 종목의 최근 투자자 프레젠테이션 PDF에서 'Anticipated Catalysts' 섹션 자동 추출")
    import watchlist as wl
    wl_df = wl.list_all()
    if wl_df.empty:
        st.info("관심종목 비어있음.")
    else:
        col1, col2 = st.columns([3, 1])
        with col1:
            picked = st.selectbox(
                "종목 선택", wl_df["ticker"].tolist(), key="irm_ticker",
            )
        with col2:
            st.markdown("<div style='height:1.7rem;'></div>", unsafe_allow_html=True)
            if st.button("🔍 IR PDF 재추출", use_container_width=True, key="irm_refresh"):
                with st.spinner(f"{picked} IR PDF 분석 중..."):
                    result = irm.extract_for_ticker(picked, save=True)
                if result.get("error"):
                    st.warning(f"⚠️ {result['error']}")
                else:
                    st.success(
                        f"✓ {result.get('deck_title', '')[:80]} — "
                        f"{len(result.get('milestones', []))}개 마일스톤"
                    )
                    if result.get("deck_url"):
                        st.caption(f"📄 {result['deck_url']}")
        ev_df = irm.get_company_events(picked)
        if ev_df.empty:
            st.info(f"{picked}의 추출된 마일스톤 없음. 🔍 IR PDF 재추출 시도.")
        else:
            for _, r in ev_df.iterrows():
                d = r.get("description")
                d = d if isinstance(d, str) else ""
                date_hint = d.replace("date_hint:", "").strip() or r.get("event_date", "")
                st.markdown(
                    f"- **[{date_hint}]** {r['title']}  "
                    f"<span style='opacity:0.6; font-size:0.85em'>· {r['event_date']}</span>",
                    unsafe_allow_html=True,
                )


# ───────────────────────── Model Portfolio ─────────────────────────
@st.dialog("💼 포트폴리오 상세", width="large")
def _portfolio_dialog(portfolio_id: int):
    import portfolio as pf
    s = pf.summary(portfolio_id)
    if not s:
        st.error("포트폴리오 없음")
        return
    p = s["portfolio"]

    # 헤더 + 삭제
    head = st.columns([6, 2])
    with head[0]:
        st.markdown(f"### {p['name']}")
        st.caption(f"생성: {p['created_at'][:10]} · 초기 사이즈: ${p['initial_size']/1e6:,.0f}M")
    with head[1]:
        if st.button("🗑 삭제", key=f"del_pf_{portfolio_id}", use_container_width=True):
            pf.delete(portfolio_id)
            st.session_state.pop("pf_open", None)
            st.session_state.pop("pf_open_id", None)
            st.rerun()

    # 요약 metric
    m = st.columns(4)
    color = "#26a69a" if s["return_pct"] >= 0 else "#ef5350"
    m[0].markdown(f"<div style='font-size:0.8em; color:#666;'>현재 사이즈</div>"
                  f"<div style='font-size:1.4em; font-weight:700;'>${s['current_size']/1e6:,.2f}M</div>",
                  unsafe_allow_html=True)
    m[1].markdown(f"<div style='font-size:0.8em; color:#666;'>수익률</div>"
                  f"<div style='font-size:1.4em; font-weight:700; color:{color};'>{s['return_pct']:+.2f}%</div>",
                  unsafe_allow_html=True)
    m[2].markdown(f"<div style='font-size:0.8em; color:#666;'>편입 비중</div>"
                  f"<div style='font-size:1.4em; font-weight:700;'>{s['total_weight']:.1f}%</div>",
                  unsafe_allow_html=True)
    m[3].markdown(f"<div style='font-size:0.8em; color:#666;'>현금</div>"
                  f"<div style='font-size:1.4em; font-weight:700;'>${s['cash_amt']/1e6:,.1f}M</div>",
                  unsafe_allow_html=True)

    st.divider()

    # 종목 추가 (form)
    with st.expander("＋ 종목 추가", expanded=not s["holdings"]):
        with st.form(f"add_holding_{portfolio_id}", clear_on_submit=True):
            cc = st.columns([2, 1, 1])
            with cc[0]:
                ticker_in = st.text_input("티커", placeholder="VRTX")
            with cc[1]:
                weight_in = st.number_input("비중 %", min_value=0.0, max_value=100.0,
                                            value=5.0, step=0.5)
            with cc[2]:
                st.write("")
                submitted = st.form_submit_button("추가", type="primary",
                                                  use_container_width=True)
            if submitted and ticker_in.strip():
                try:
                    pf.add_holding(portfolio_id, ticker_in, weight_in)
                    st.success(f"{ticker_in.upper()} 추가됨")
                    st.rerun()
                except Exception as e:
                    st.error(f"실패: {e}")

    # 종목 리스트
    if not s["holdings"]:
        st.info("아직 편입 종목 없음.")
        return

    st.markdown("##### 편입 종목")
    schema = [("티커", 1), ("회사명", 4), ("비중%", 1), ("편입일", 2),
              ("편입가", 2), ("현재가", 2), ("수익률", 2), ("현재가치", 2), ("", 1)]
    weights = [w for _, w in schema]
    hdr = st.columns(weights, vertical_alignment="center")
    for i, (label, _) in enumerate(schema):
        hdr[i].markdown(f"**{label}**")

    for h in s["holdings"]:
        cells = st.columns(weights, vertical_alignment="center")
        cells[0].caption(h["ticker"])
        cells[1].caption((h.get("name") or h["ticker"])[:30])
        cells[2].write(f"{h['weight_pct']:.1f}%")
        cells[3].caption(h["entry_date"])
        cells[4].write(f"${h['entry_price']:,.2f}")
        cells[5].write(f"${h['curr_price']:,.2f}")
        ret = h["return_pct"]
        clr = "#26a69a" if ret >= 0 else "#ef5350"
        cells[6].markdown(
            f"<span style='color:{clr}; font-weight:600;'>{ret:+.2f}%</span>",
            unsafe_allow_html=True,
        )
        cells[7].write(f"${h['amt_current']/1e6:,.2f}M")
        if cells[8].button("✗", key=f"rm_{h['id']}", help="이 종목 제거"):
            pf.remove_holding(h["id"])
            st.rerun()


@st.dialog("새 포트폴리오 만들기", width="small")
def _new_portfolio_dialog():
    import portfolio as pf
    with st.form("new_pf_form", clear_on_submit=True):
        name = st.text_input("이름", placeholder="Bio Fund #1")
        size_m = st.number_input("초기 사이즈 ($M)", min_value=1.0, value=100.0, step=10.0)
        if st.form_submit_button("만들기", type="primary"):
            if not name.strip():
                st.error("이름 필수")
                return
            try:
                pf.create(name, initial_size=size_m * 1_000_000)
                st.session_state.pop("_new_pf_open", None)
                st.rerun()
            except Exception as e:
                st.error(f"실패: {e}")


def _section_portfolios():
    """탭 컨텐츠 — Model Portfolio 카드 그리드."""
    import portfolio as pf

    if st.button("＋ 새 포트폴리오", key="open_new_pf"):
        st.session_state["_new_pf_open"] = True
        st.rerun()
    if st.session_state.get("_new_pf_open"):
        _new_portfolio_dialog()

    open_id = st.session_state.get("pf_open_id")
    if st.session_state.get("pf_open") and open_id:
        _portfolio_dialog(open_id)

    portfolios = pf.list_all()
    if not portfolios:
        st.info("아직 포트폴리오 없음. 위 ＋ 버튼으로 만드세요.")
        return

    st.caption(f"{len(portfolios)}개 포트폴리오 · 카드 클릭 → 상세")
    cols_per_row = 3
    for i in range(0, len(portfolios), cols_per_row):
        row_pfs = portfolios[i:i + cols_per_row]
        cols = st.columns(cols_per_row)
        for col, p in zip(cols, row_pfs):
            with col:
                s = pf.summary(p["id"])
                ret = s.get("return_pct", 0.0)
                clr = "#26a69a" if ret >= 0 else "#ef5350"
                with st.container(border=True):
                    st.markdown(f"**{p['name']}**")
                    st.markdown(
                        f"<div style='font-size:1.3em; font-weight:700;'>"
                        f"${s['current_size']/1e6:,.2f}M</div>"
                        f"<div style='color:{clr}; font-weight:600;'>"
                        f"{ret:+.2f}%</div>"
                        f"<div style='color:#888; font-size:0.85em; margin-top:0.3rem;'>"
                        f"{len(s['holdings'])}종목 · 편입 {s['total_weight']:.0f}%</div>",
                        unsafe_allow_html=True,
                    )
                    if st.button("열기", key=f"open_pf_{p['id']}",
                                 use_container_width=True):
                        st.session_state["pf_open"] = True
                        st.session_state["pf_open_id"] = p["id"]
                        st.rerun()


@st.cache_data(ttl=1800, show_spinner=False)
def _cached_daily_news(days: int):
    from news import fetch_global_healthcare_news
    return fetch_global_healthcare_news(days=days, max_items=200)


def _section_daily_news():
    """탭 컨텐츠 — 데일리 바이오 뉴스 (M&A/라이센싱/임상/FDA)."""
    cc = st.columns([1, 2, 1, 4])
    with cc[0]:
        days = st.selectbox("기간", [1, 3, 7], index=0,
                            format_func=lambda v: f"최근 {v}일")
    with cc[1]:
        cat_filter = st.multiselect(
            "카테고리",
            ["M&A", "라이센싱", "라이센싱 종료", "파트너십", "임상 결과", "FDA / 규제"],
            default=[],
            placeholder="전체 (선택 시 필터)",
        )
    with cc[2]:
        st.write("")
        if st.button("🔄 새로 가져오기", help="캐시 무시하고 재수집"):
            _cached_daily_news.clear()
            st.rerun()

    with st.spinner("Finviz + 바이오 매체 RSS + Google News 통합 (~1분)..."):
        items = _cached_daily_news(days)

    if cat_filter:
        items = [n for n in items if any(c in cat_filter for c in n["categories"])]

    st.caption(f"{len(items)}건 · 출처: Finviz, FiercePharma, FierceBiotech, Endpoints, "
               f"BioPharma Dive, STAT, BioSpace, Google News (30분 캐시)")

    if not items:
        st.info("조건에 맞는 뉴스 없음.")
        return

    cat_color = {
        "M&A": "#d32f2f",
        "라이센싱": "#1976d2",
        "라이센싱 종료": "#666",
        "파트너십": "#7e57c2",
        "임상 결과": "#26a69a",
        "FDA / 규제": "#f9a825",
    }
    default_color = "#888"
    for n in items:
        with st.container(border=True):
            cat_spans = []
            for c in n["categories"]:
                color = cat_color.get(c, default_color)
                cat_spans.append(
                    f"<span style='background:{color}; color:#fff; "
                    f"padding:0.1rem 0.5rem; border-radius:4px; font-size:0.75em; "
                    f"margin-right:0.2rem;'>{c}</span>"
                )
            cats_html = " ".join(cat_spans)
            tks = ", ".join(n["tickers"][:3]) if n["tickers"] else ""
            tks_html = (f" · <span style='color:#1976d2; font-weight:600;'>{tks}</span>"
                        if tks else "")
            st.markdown(
                f"{cats_html} "
                f"<span style='color:#888; font-size:0.85em;'>{n['published'][:10]} · "
                f"{n['source']}</span>{tks_html}",
                unsafe_allow_html=True,
            )
            st.markdown(f"**[{n['title']}]({n['link']})**")
            if n.get("summary"):
                st.caption(n["summary"][:200])


def _section_top_movers():
    """탭 컨텐츠 — 상승폭 최대."""
    cc2 = st.columns([1, 1, 1, 5])
    with cc2[0]:
        min_mcap = st.selectbox(
            "최소 시총", [500, 1000, 2000, 5000, 10000], index=0,
            format_func=lambda v: f"{v//1000}b" if v >= 1000 else f"{v}m",
        )
    with cc2[1]:
        min_perf = st.selectbox(
            "최소 상승률", [5.0, 10.0, 20.0], index=0,
            format_func=lambda v: f"+{v:.0f}%",
        )
    with cc2[2]:
        limit = st.selectbox("개수", [50, 100, 200], index=1)

    df = fetch_top_movers(limit=limit, min_mcap=min_mcap, min_perf=min_perf)
    if df.empty:
        st.info("조건에 맞는 종목 없음. '🔄 신고가 갱신' (52주 신고가 탭) 먼저 실행하세요.")
        return

    st.caption(f"{len(df)}종목 · 기준일 {latest_run_date() or '—'} · 회사명 클릭 → 상세")
    _render_table(df)


# ───────────────────────── 관심종목 페이지 ─────────────────────────
@st.dialog("📊 종목 추가", width="large")
def _add_stock_dialog():
    """검색 → 종목 클릭 → detail 모달로 전환."""
    import watchlist as wl

    st.markdown("**티커 또는 회사명으로 검색** (universe: ~312 종목)")
    q = st.text_input("검색", value="", key="add_search", placeholder="VRTX, Vertex, ...")

    universe = get_universe()
    if q.strip():
        ql = q.strip().lower()
        mask = (universe["ticker"].str.lower().str.contains(ql, na=False)
                | universe["name"].str.lower().str.contains(ql, na=False))
        results = universe[mask].head(30)
    else:
        results = universe.head(30)   # 빈 검색 — 시총 상위 30

    st.caption(f"{len(results)}건 표시")

    for _, row in results.iterrows():
        cols = st.columns([2, 5, 3, 1])
        cols[0].caption(row["ticker"])
        if cols[1].button(str(row["name"] or row["ticker"]),
                          key=f"add_pick_{row['ticker']}", use_container_width=True):
            # detail 모달로 전환
            st.session_state["detail_ticker"] = row["ticker"]
            st.session_state["detail_name"] = row["name"]
            st.session_state["detail_open"] = True
            st.session_state["_add_dialog_open"] = False
            st.rerun()
        cols[2].caption(row["industry"] or "—")
        if wl.is_watched(row["ticker"]):
            cols[3].caption("★")


def render_watchlist_page():
    import watchlist as wl

    cc = st.columns([1, 8])
    with cc[0]:
        if st.button("← 메인", key="back_main_wl"):
            st.session_state["page"] = "main"
            st.rerun()

    st.markdown(
        """
        <div class="hero-banner" style="
          background: linear-gradient(135deg, #134e4a 0%, #0a3d3a 100%);
          color: #fff; padding: 1.2rem 1.8rem; border-radius: 12px;
          margin-bottom: 1.2rem;
        ">
          <h1 style="margin:0; font-size:1.6rem;">⭐ 관심종목</h1>
          <div style="opacity:0.85; font-size:0.9rem; margin-top:0.25rem;">
            신고가 여부 무관
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 종목 추가 버튼
    if st.button("＋ 종목 추가", key="open_add_dialog"):
        st.session_state["_add_dialog_open"] = True
        st.rerun()

    if st.session_state.get("_add_dialog_open"):
        _add_stock_dialog()

    # 관심종목 리스트
    df = wl.list_all()
    if df.empty:
        st.info("아직 관심종목 없음. 위 ＋ 버튼으로 추가하세요.")
        return

    st.caption(f"{len(df)}종목 · 종목명 클릭 → 상세")
    _render_table(df)


# ───────────────────────── 라우팅 ─────────────────────────
# 모달은 어느 페이지에서든 detail_open이면 띄움
if st.session_state.get("detail_open"):
    detail_ticker = st.session_state.get("detail_ticker")
    if detail_ticker:
        _detail_dialog(detail_ticker, st.session_state.get("detail_name") or detail_ticker)

page = st.session_state.get("page", "main")
if page == "watchlist":
    render_watchlist_page()
else:
    render_main_page()
