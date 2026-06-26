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

import yf_session  # noqa: F401 — yfinance 레이트리밋 패치 (import 부수효과)
import db
from db import init_db
from universe import load_universe, get_universe
from collectors.high_low import (
    collect as hl_collect, fetch_new_highs, latest_run_date,
    fetch_new_today_highs, fetch_top_movers,
)

st.set_page_config(page_title="Biotech Radar", layout="wide", page_icon="🧬",
                   initial_sidebar_state="collapsed")


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
  @import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.9/dist/web/static/pretendard.min.css');
  @import url('https://fonts.googleapis.com/css2?family=Newsreader:opsz,wght@6..72,400;6..72,500;6..72,600&display=swap');
  /* 전체 톤 — 차분한 다크그린, 절제된 헤지펀드 감성 */
  html, body, .stApp, button, input, textarea, select,
  [data-testid="stMarkdownContainer"] {
    font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, 'Malgun Gothic', 'Segoe UI', sans-serif;
  }
  .stApp {
    background: #ffffff;
    -webkit-font-smoothing: antialiased;
    text-rendering: optimizeLegibility;
    color: #14302e;
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
    border-color: #dbe6e3 !important;
    opacity: 0.7;
  }

  /* ───── 타이포 ───── */
  h2 { color:#0f433f !important; font-weight:700 !important; letter-spacing:-0.3px; }
  .hero-wordmark { font-family:'Newsreader', Georgia, serif !important; }

  /* ───── 메인 탭: 라디오 동그라미 → 밑줄형 사이트 내비 ───── */
  .st-key-main_tab_radio div[role="radiogroup"]{
    display:flex; gap:0.1rem; flex-wrap:wrap; align-items:flex-end;
    border-bottom:1px solid #dde6e3; margin:0.1rem 0 0.35rem;
  }
  .st-key-main_tab_radio div[role="radiogroup"] > label{
    margin:0 !important; padding:0.6rem 1.05rem !important; min-height:0 !important;
    background:transparent !important; border:0 !important;
    border-bottom:2px solid transparent !important; border-radius:0 !important;
    cursor:pointer; transition:color .15s, border-color .15s;
  }
  .st-key-main_tab_radio div[role="radiogroup"] > label > div:first-child{ display:none !important; }
  .st-key-main_tab_radio div[role="radiogroup"] label *{
    font-size:0.95rem !important; font-weight:600 !important; color:#6b7d7b !important;
  }
  .st-key-main_tab_radio div[role="radiogroup"] label:hover *{ color:#134e4a !important; }
  .st-key-main_tab_radio div[role="radiogroup"] label:has(input:checked){ border-bottom-color:#134e4a !important; }
  .st-key-main_tab_radio div[role="radiogroup"] label:has(input:checked) *{
    color:#0a3d3a !important; font-weight:700 !important;
  }

  /* ───── 시장 토글(해외/한국): 세그먼트 컨트롤 ───── */
  .st-key-country div[role="radiogroup"]{
    display:inline-flex; gap:0; padding:3px; border:1px solid #d6e0de;
    background:#eaf1ef; border-radius:11px;
  }
  .st-key-country div[role="radiogroup"] > label{
    margin:0 !important; padding:0.36rem 0.95rem !important; min-height:0 !important;
    background:transparent !important; border:0 !important; border-radius:8px !important;
    transition:background .15s; cursor:pointer;
  }
  .st-key-country div[role="radiogroup"] > label > div:first-child{ display:none !important; }
  .st-key-country div[role="radiogroup"] label *{
    font-size:0.9rem !important; font-weight:600 !important; color:#5b6f6e !important;
  }
  .st-key-country div[role="radiogroup"] label:has(input:checked){
    background:#134e4a !important; box-shadow:0 1px 4px rgba(10,61,58,0.28);
  }
  .st-key-country div[role="radiogroup"] label:has(input:checked) *{ color:#ffffff !important; }

  /* ───── 카드/컨테이너 ───── */
  div[data-testid="stVerticalBlockBorderWrapper"]{
    border-radius:14px !important; border-color:#e4ebe9 !important; background:#ffffff;
    box-shadow:0 1px 2px rgba(16,48,46,0.04), 0 8px 20px -12px rgba(16,48,46,0.12);
  }
  details{ border-radius:12px !important; border-color:#e4ebe9 !important; }

  /* ───── 데이터프레임/표 ───── */
  [data-testid="stDataFrame"]{ border-radius:12px; overflow:hidden; border:1px solid #e4ebe9; }
  [data-testid="stDataFrame"] [role="columnheader"]{
    background:#f1f5f4 !important; color:#0f433f !important; font-weight:700 !important;
  }

  /* ───── 입력/셀렉트 ───── */
  div[data-baseweb="select"] > div, .stTextInput input, .stNumberInput input{
    border-radius:10px !important; border-color:#d6e0de !important;
  }

  /* ───── 메트릭 ───── */
  [data-testid="stMetric"]{
    background:#ffffff; border:1px solid #e7edeb; border-radius:14px;
    padding:1rem 1.1rem; box-shadow:0 6px 16px -12px rgba(16,48,46,0.2);
  }
  [data-testid="stMetricValue"]{ color:#0a3d3a !important; font-weight:700 !important; }

  /* ───── 사이드바 폴리시 ───── */
  section[data-testid="stSidebar"]{
    background:linear-gradient(185deg,#0d3b3a 0%,#0a302e 100%) !important;
    border-right:1px solid rgba(255,255,255,0.05);
  }
  section[data-testid="stSidebar"] h1{
    font-family:'Newsreader', Georgia, serif !important; font-weight:600 !important;
    letter-spacing:-0.3px; font-size:1.5rem !important;
  }

  /* ───── 콘텐츠 프레임 / 여백 (헤지펀드 홈 느낌) ───── */
  div[data-testid="stMainBlockContainer"]{
    max-width: 100% !important; padding-top: 0; padding-bottom: 4rem;
    padding-left: 3rem; padding-right: 3rem;
  }

  /* ───── 섹션 헤더 — 자간 + 하단 헤어라인 ───── */
  div[data-testid="stMainBlockContainer"] h3{
    font-weight:700 !important; letter-spacing:0.01em; font-size:1.18rem !important;
    padding-bottom:0.5rem; margin-bottom:0.5rem; border-bottom:1px solid #e2eae8;
  }

  /* ───── 액션 버튼 — 절제된 텍스트 링크 톤 ───── */
  div[data-testid="stMainBlockContainer"] [data-testid^="stBaseButton-"]:not([data-testid="stBaseButton-primary"]){
    letter-spacing:0.02em !important; font-size:0.9rem !important;
  }

  /* '오늘 신규 / 전체' 토글 → 세그먼트 컨트롤 */
  .st-key-hl_view div[role="radiogroup"]{
    display:inline-flex; gap:0; padding:3px; border:1px solid #d6e0de;
    background:#eaf1ef; border-radius:11px;
  }
  .st-key-hl_view div[role="radiogroup"] > label{
    margin:0 !important; padding:0.36rem 0.95rem !important; min-height:0 !important;
    background:transparent !important; border:0 !important; border-radius:8px !important;
    transition:background .15s; cursor:pointer;
  }
  .st-key-hl_view div[role="radiogroup"] > label > div:first-child{ display:none !important; }
  .st-key-hl_view div[role="radiogroup"] label *{
    font-size:0.9rem !important; font-weight:600 !important; color:#5b6f6e !important;
  }
  .st-key-hl_view div[role="radiogroup"] label:has(input:checked){
    background:#134e4a !important; box-shadow:0 1px 4px rgba(10,61,58,0.28);
  }
  .st-key-hl_view div[role="radiogroup"] label:has(input:checked) *{ color:#ffffff !important; }

  /* ───── 사이드바 내비 — 펀드 사이트 메뉴 톤 ───── */
  section[data-testid="stSidebar"] [data-testid^="stBaseButton-"]{
    letter-spacing:0.04em !important; font-size:0.92rem !important;
    border-left:2px solid transparent !important;
  }
  section[data-testid="stSidebar"] [data-testid^="stBaseButton-"]:hover{
    border-left:2px solid #3fae9b !important;
  }

  /* ───── 상단 네비게이션 바 (홈페이지형, sticky) ───── */
  .st-key-topbar{
    position: sticky; top: 0; z-index: 999;
    width: 100vw; margin-left: calc(50% - 50vw); margin-right: calc(50% - 50vw);
    margin-top: 0; margin-bottom: 1.7rem; padding: 0.55rem 3rem;
    background: #ffffff;
    border-bottom: 1px solid #e6ece9;
    box-shadow: 0 4px 18px -16px rgba(16,48,46,0.4);
  }
  /* 컬럼이 좁은 폭에서 2×2로 접히지 않게 한 줄 고정 + 수직 중앙 정렬 */
  .st-key-topbar [data-testid="stHorizontalBlock"]{ flex-wrap: nowrap !important; align-items: center !important; }
  .st-key-topbar [data-testid="stMarkdownContainer"]{ margin: 0 !important; }
  .topbar-brand{ display:flex; flex-direction:column; line-height:1.08; }
  .topbar-brand .hero-wordmark{
    font-size:1.7rem; font-weight:600; color:#0a3d3a; letter-spacing:-0.5px; white-space:nowrap;
  }
  .topbar-brand .topbar-tag{
    font-size:0.7rem; letter-spacing:0.06em; color:#5b6f6e; opacity:0.9; margin-top:2px;
    white-space:nowrap;
  }
  /* 바 안의 탭 = 사이트 메뉴 (하단 라인 제거, 한 줄, 왼쪽 정렬) */
  .st-key-topbar .st-key-main_tab_radio div[role="radiogroup"]{
    border-bottom:0 !important; justify-content:flex-start; margin:0 !important;
    flex-wrap:nowrap !important; overflow-x:auto;
  }
  .st-key-topbar .st-key-main_tab_radio div[role="radiogroup"] > label{
    padding:0.4rem 0.75rem !important; white-space:nowrap;
  }
  .st-key-topbar .st-key-country{ display:flex; justify-content:flex-end; margin-right:-1.4rem; }
  .st-key-topbar .st-key-country div[role="radiogroup"]{ flex-wrap:nowrap !important; }

  /* ───── Streamlit 기본 상단 헤더/툴바 숨김 (share·별표·수정·rerun 바 제거) ───── */
  header[data-testid="stHeader"]{ display:none !important; }
  [data-testid="stToolbar"]{ display:none !important; }
  [data-testid="stToolbarActions"]{ display:none !important; }
  [data-testid="stDecoration"]{ display:none !important; }
  [data-testid="stStatusWidget"]{ display:none !important; }
  #MainMenu{ display:none !important; }
  /* ───── 사이드바 완전 제거 ───── */
  section[data-testid="stSidebar"]{ display:none !important; }
  [data-testid="stSidebarCollapsedControl"]{ display:none !important; }
  [data-testid="collapsedControl"]{ display:none !important; }
</style>
""", unsafe_allow_html=True)

# ───────────────────────── 공통 함수 ─────────────────────────
def _close_modal():
    """비-행 클릭(사이드바·정렬 헤더·라디오 변경) 시 모달 자동 재팝업 방지."""
    st.session_state["detail_open"] = False


def _go_main_tab(tab: str):
    _close_modal()
    st.session_state["page"] = "main"
    st.session_state["main_tab"] = tab
    st.session_state["_force_tab"] = tab   # 탭 라디오 위젯 상태 동기화 신호
    st.rerun()


# 시장(국가)은 메인 상단 탭(🌏 해외 / 🇰🇷 한국)에서 선택 — 사이드바 버튼은 그 값을 읽음
COUNTRY = st.session_state.get("country", "USA")


def _board_scope():
    """현재 선택 국가 + 52주 보드 시총 하한($M). 한국=5천억, 미국=$1.5B."""
    c = st.session_state.get("country", "USA")
    if c == "KOR":
        import kr_universe as _ku
        try:
            return c, _ku.kr_min_mcap_usd_m()
        except Exception:
            return c, 324.0
    return c, 1500.0


# ── 운영용 상태 값 (우하단 ⚙ 위젯 + 본문에서 사용) ──
universe_count = len(get_universe())
last = latest_run_date()


def _stats_counts():
    """메모/관심종목 카운트."""
    from db import connect
    with connect() as conn:
        m = conn.execute("SELECT COUNT(*) AS n FROM memos").fetchone()["n"]
        w = conn.execute("SELECT COUNT(*) AS n FROM watchlist").fetchone()["n"]
    return m, w


@st.fragment
def _floating_ops_widget():
    """우하단 ⚙ 운영 위젯 — Universe 갱신·텔레그램 발송·모바일 보기 + 상태(사이드바 대체).
    데이터/보기 변경 액션은 전체 rerun."""
    country = st.session_state.get("country", "USA")
    open_ = st.session_state.get("ops_widget_open", False)
    st.markdown(
        """
        <style>
        .st-key-opsbtn { position: fixed; bottom: 6.7rem; right: 1.7rem;
            z-index: 2147483000; width: auto !important; }
        div[data-testid="stMainBlockContainer"] .st-key-opsbtn button {
            border-radius: 16px !important; min-height: 54px !important; height: 54px !important;
            padding: 0 1.7rem !important; font-size: 1.2rem !important; font-weight: 800 !important;
            letter-spacing: .06em !important; line-height: 1 !important;
            justify-content: center !important; text-align: center !important;
            background: #13635a !important; color: #ffffff !important; border: none !important;
            box-shadow: 0 10px 26px -6px rgba(10,61,58,0.5) !important; }
        div[data-testid="stMainBlockContainer"] .st-key-opsbtn button:hover {
            background: #0f5a52 !important; color: #ffffff !important; }
        .st-key-opspanel { position: fixed; bottom: 6.7rem; right: 1.1rem;
            z-index: 2147483000; width: 300px; max-width: 90vw;
            background: #ffffff; border: 1px solid #d3dbd9; border-radius: 14px;
            box-shadow: 0 10px 34px rgba(0,0,0,0.30); padding: 0.7rem 0.9rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    if not open_:
        with st.container(key="opsbtn"):
            if st.button("⚙ 운영", key="ops_launch", help="Universe·신고가 갱신·URL 탐색·텔레그램·보기·상태"):
                st.session_state["ops_widget_open"] = True
                st.rerun(scope="fragment")
        return

    with st.container(key="opspanel"):
        hdr = st.columns([5, 1])
        with hdr[0]:
            st.markdown("**⚙ 운영**")
        with hdr[1]:
            if st.button("➖", key="ops_min", use_container_width=True, help="최소화"):
                st.session_state["ops_widget_open"] = False
                st.rerun(scope="fragment")

        try:
            _uc = len(get_universe())
        except Exception:
            _uc = "—"
        try:
            _mn, _wn = _stats_counts()
            _statline = f"메모 {_mn} · 관심 {_wn}"
        except Exception:
            _statline = ""
        st.caption(f"universe {_uc}종목 · 갱신 {latest_run_date() or '—'}")
        if _statline:
            st.caption(_statline)

        _uni_help = ("네이버 업종+FDR로 한국 제약·바이오 다시 로드 + 52주 스냅샷"
                     if country == "KOR" else "Finviz에서 Healthcare 전 종목 다시 로드")
        if st.button("🔄 Universe 갱신", key="uni_refresh", use_container_width=True, help=_uni_help):
            _close_modal()
            with st.spinner("갱신 중..."):
                try:
                    if country == "KOR":
                        import kr_universe as _ku
                        from collectors.high_low import collect_kr
                        n = _ku.seed(); m = collect_kr()
                        st.toast(f"한국 {n}종목 로드 · 스냅샷 {m}")
                    else:
                        n = load_universe()
                        st.toast(f"{n}종목 로드")
                except Exception as e:
                    st.toast(f"⚠️ {type(e).__name__}: {e}")
            st.rerun()

        if st.button("🔄 신고가 갱신", key="hl_refresh_btn", use_container_width=True,
                     help="universe 전체 yfinance OHLCV 일괄 다운로드 (~수분)"):
            _close_modal()
            with st.spinner("yfinance OHLCV 일괄 다운로드..."):
                try:
                    n = hl_collect(industry_filter=None)
                    st.toast(f"{n}종목 처리")
                except Exception as e:
                    st.toast(f"⚠️ {type(e).__name__}: {e}")
            st.rerun()

        if st.button("🔍 URL 자동 탐색", key="discover_all_btn", use_container_width=True,
                     help="universe 전체 IR/Pipeline URL 일괄 탐색 (~1~2분)"):
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
                            _tu.set_urls(tk, ir_url=r.get("ir_url"), pipeline_url=r.get("pipeline_url"))
                            saved += 1
                    st.toast(f"{saved}/{len(tks)}종목 URL 저장")
                except Exception as e:
                    st.toast(f"⚠️ {type(e).__name__}: {e}")

        _tg_help = ("한국 15:30 푸시 즉시 발송" if country == "KOR"
                    else "현재 데이터로 즉시 텔레그램 요약 발송")
        if st.button("📨 텔레그램 발송", key="tg_send", use_container_width=True, help=_tg_help):
            _close_modal()
            with st.spinner("텔레그램 발송 중..."):
                try:
                    if country == "KOR":
                        from telegram_report import daily_run_kr
                        daily_run_kr()
                    else:
                        from telegram_report import send, compose_report
                        send(compose_report())
                    st.toast("발송됨")
                except Exception as e:
                    st.toast(f"⚠️ {type(e).__name__}: {e}")

        _is_mobile = st.session_state.get("mobile_mode", False)
        if st.button("🖥 데스크톱 보기" if _is_mobile else "📱 모바일 보기",
                     key="toggle_mobile", use_container_width=True,
                     help="컬럼 축소 + 큰 글자 + 작은 패딩"):
            st.session_state["mobile_mode"] = not _is_mobile
            st.rerun()


# ───────────────────────── 메모 타임라인 페이지 ─────────────────────────
@st.fragment
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


@st.cache_data(ttl=45, show_spinner=False)
def _cached_pf_summary(portfolio_id: int):
    """MP 요약 캐시 — 매 rerun마다 보유종목별 시세 재호출 방지(rerun 가속).
    거래(편입/조정/매도) 후엔 _cached_pf_summary.clear()로 즉시 무효화."""
    import portfolio as pf
    return pf.summary(portfolio_id)


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
    try:
        from discover import discover as auto_discover
        with st.spinner("IR/Pipeline URL 자동 탐색..."):
            result = auto_discover(ticker)
    except Exception:
        return   # 일시적 실패(네트워크 등) → 플래그 미설정 → 다음에 열 때 재시도
    st.session_state[flag] = True   # discover 정상 실행됨(빈 결과여도 재시도 안 함)
    # 비어있는 쪽만 새로 채움
    new_ir = urls.get("ir_url") or result.get("ir_url", "")
    new_pl = urls.get("pipeline_url") or result.get("pipeline_url", "")
    if new_ir or new_pl:
        ticker_urls.set_urls(ticker, ir_url=new_ir, pipeline_url=new_pl)
        if new_ir and not have_ir:
            st.session_state[f"ir_in_{ticker}"] = new_ir
        if new_pl and not have_pl:
            st.session_state[f"pl_in_{ticker}"] = new_pl


@st.fragment
def _watch_excl_fragment(ticker: str):
    """관심/제외 토글 — fragment로 격리해 등록/해제 시 모달 전체 rerun 방지(이 버튼만 갱신)."""
    import watchlist as wl
    import excluded as excl
    c = st.columns(2)
    with c[0]:
        if wl.is_watched(ticker):
            if st.button("★ 관심 해제", key=f"wl_off_{ticker}", use_container_width=True):
                wl.remove(ticker)
                st.rerun(scope="fragment")
        else:
            if st.button("☆ 관심종목", key=f"wl_on_{ticker}", use_container_width=True):
                wl.add(ticker)
                st.rerun(scope="fragment")
    with c[1]:
        if excl.is_excluded(ticker):
            if st.button("✓ 제외 해제", key=f"ex_off_{ticker}", use_container_width=True):
                excl.remove(ticker)
                st.rerun(scope="fragment")
        else:
            if st.button("🚫 제외", key=f"ex_on_{ticker}", use_container_width=True,
                         help="신고가/상승폭 리스트에서 영구 숨김 (비-biotech 종목용)"):
                excl.add(ticker, note="user excluded")
                st.rerun(scope="fragment")


def render_stock_detail(ticker: str, name: str):
    import plotly.graph_objects as go
    import watchlist as wl
    import excluded as excl
    from prices import PERIOD_LABELS

    _ensure_urls_discovered(ticker)

    # 헤더 + 관심/제외 토글 (토글은 fragment — 등록/해제 시 모달 전체 rerun 방지)
    top = st.columns([6, 3])
    with top[0]:
        st.markdown(f"### {name} ({ticker})")
    with top[1]:
        _watch_excl_fragment(ticker)

    _chart_fragment(ticker)

    # ── 3-1, 3-2, 3-3 토글 섹션들 ──
    st.divider()
    _render_url_settings(ticker)
    _render_ir_section(ticker, name)
    _render_pipeline_section(ticker)
    _render_news_section(ticker, name)
    _render_recent_articles_section(ticker, name)
    _render_catalyst_section(ticker)
    _render_insider_section(ticker)
    _render_ai_report_section(ticker, name)

    # ── 메모 (토글들 밑) ──
    st.divider()
    _render_memo_section(ticker)


@st.fragment
def _chart_fragment(ticker: str):
    """차트 섹션 — 기간/봉 변경 시 모달 전체(뉴스·파이프라인·밸류 등)가 아니라 차트만 rerun."""
    import plotly.graph_objects as go
    from prices import PERIOD_LABELS
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
        _cached_fetch_chart.clear()   # 빈 결과 캐시 방지 — 새로고침 시 재시도
        try:
            import toss_market as tm
            diag = tm.diagnose()
        except Exception as e:
            diag = {"ok": False, "msg": f"toss_market 오류: {e}"}
        if "." in (ticker or ""):
            st.info(f"차트 데이터 없음 — `{ticker}`는 토스 미지원 심볼(미국/국내 외).")
        elif not diag.get("ok"):
            st.warning(
                f"차트 데이터 없음 — **토스 시세 연결 문제**: {diag.get('msg')}\n\n"
                "· 키 미설정이면 Streamlit **Secrets**에 `TOSS_API_KEY`/`TOSS_API_SECRET` "
                "(따옴표 O, 주석 # 제거) 확인 후 reboot.\n"
                "· 토큰은 되는데 막히면 **한국 API의 해외(클라우드) IP 차단** 가능성."
            )
        else:
            st.info("차트 데이터 없음 (일시적 — 새로고침 재시도).")
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


_FUNDAMENTAL_PAT = __import__("re").compile(
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
    __import__("re").IGNORECASE,
)
_NOISE_PAT = __import__("re").compile(
    r"(price target|analyst|upgrad|downgrad|consensus|estimate|"
    r"insider (?:bought|sold)|filed form 4|"
    r"options activity|unusual options|short interest|"
    r"(?:eps|revenue) (?:beat|miss|estimate)|"
    r"top \d+ stocks?|stocks? to (?:buy|watch)|trending|moving|gainers?|losers?|"
    r"premarket|after hours|"
    r"benzinga|seeking alpha author|simply wall st)",
    __import__("re").IGNORECASE,
)


@st.cache_data(ttl=600, show_spinner=False)
def _cached_recent_articles(ticker: str, name: str, days: int):
    """Finviz quote + Yahoo per-ticker + Google News (회사명) — fundamental 필터링.
    KR(6자리)은 kr_news 한국 전문매체 + 한국어 Google News로 대체(영문 필터 미적용)."""
    _t = str(ticker).strip()
    if _t.isdigit() and len(_t) == 6:
        out = []
        try:
            import kr_news
            # 1차: 네이버 금융 종목뉴스 (종목코드 기반 — 가장 풍부·정확)
            for it in kr_news.naver_finance_news(_t, limit=20):
                out.append({"title": it["title"], "link": it["link"],
                            "source": it["source"], "published": it.get("published", "")})
            # 보강: 한국 바이오 전문매체 (제목-회사명 매칭)
            for it in kr_news.for_query(name, limit=10, days=30):
                out.append({"title": it["title"], "link": it["link"],
                            "source": it["source"],
                            "published": it["published"].strftime("%Y-%m-%d")
                                         if it.get("published") else ""})
        except Exception:
            pass
        seen, ded = set(), []
        for it in out:
            k = (it.get("title") or "")[:50]
            if k and k not in seen:
                seen.add(k)
                ded.append(it)
        return ded
    from news import fetch_finviz_news, fetch_yahoo_news, fetch_google_news
    items = list(fetch_finviz_news(ticker, days=days))
    items += list(fetch_yahoo_news(ticker))
    # 회사명 영문 검색 — Asia 또는 회사명에 큰 의미 있는 종목 모두 적용
    # 회사명 짧으면 "Inc"/"Limited"/"Co"/"Ltd" 같은 흔한 단어 제거
    import re as _re
    clean_name = _re.sub(
        r"\b(Inc\.?|Corp\.?|Corporation|Limited|Ltd\.?|Co\.?|Company|"
        r"Pharma|Pharmaceuticals?|Group|Holdings?|K\.K\.)$",
        "", name or "", flags=_re.IGNORECASE,
    ).strip(" ,")
    if len(clean_name) >= 4:
        try:
            items += list(fetch_google_news(clean_name, days=days, limit=30))
        except Exception:
            pass
    # 최근순 정렬
    items.sort(key=lambda it: it.get("_published_dt") or 0, reverse=True)
    # fundamental 필터 + dedup
    out = []
    seen_titles = set()
    for it in items:
        t = (it.get("title") or "").strip()
        if not t or len(t) < 15:
            continue
        if _NOISE_PAT.search(t):
            continue
        if not _FUNDAMENTAL_PAT.search(t):
            continue
        # 제목 정규화 — 유사 제목 중복 제거
        norm = " ".join(sorted(set(t.lower().split())))[:80]
        if norm in seen_titles:
            continue
        seen_titles.add(norm)
        out.append(it)
    return out


def _render_recent_articles_section(ticker: str, name: str):
    """FiercePharma / Yahoo / Finviz 등에서 fundamental 기사 TOP 3."""
    with st.expander("📰 최근 주요 기사 — 펀더멘탈 (TOP 3)", expanded=False):
        articles = _cached_recent_articles(ticker, name, 60)
        if not articles:
            st.caption("최근 60일 fundamental 기사 없음.")
            return
        for it in articles[:3]:
            title = it.get("title", "")
            link = it.get("link", "")
            src = (it.get("source") or "").replace("Finviz/", "")
            pub = (it.get("published") or "")[:10]
            st.markdown(
                f"**[{title}]({link})**  \n"
                f"<span style='opacity:0.6; font-size:0.85em'>{src} · {pub}</span>",
                unsafe_allow_html=True,
            )
            st.markdown("")


def _render_ai_report_section(ticker: str, name: str):
    """AI 투자 메모 — Claude 도구 사용한 deep research 리포트.
    캐시 보존(ai_reports 테이블), '생성/재생성' 버튼으로 갱신."""
    import investment_report as ir
    cached = ir.get_cached_report(ticker)
    if cached:
        gen_at = (cached.get("generated_at") or "")[:16].replace("T", " ")
        # 경과 시간
        try:
            from datetime import datetime as _dt
            elapsed = _dt.now() - _dt.fromisoformat(cached["generated_at"])
            h = int(elapsed.total_seconds() // 3600)
            if h < 24:
                age = f"{h}시간 전" if h > 0 else "방금"
            else:
                age = f"{h//24}일 전"
        except Exception:
            age = ""
        label = f"🎯 Thesis  ·  {gen_at} ({age})"
    else:
        label = "🎯 Thesis (미생성)"

    with st.expander(label, expanded=bool(cached)):
        if cached:
            st.markdown(cached["body"])
            st.caption(f"_생성 모델: {cached.get('model') or 'claude-opus-4-8'}_")
        else:
            st.info(
                "아직 생성된 리포트가 없습니다. 아래 버튼을 누르면 Claude가 도구를 호출해 "
                "파이프라인·경쟁 약물·임상 데이터·인사이더를 조사하고 institutional-quality "
                "메모를 작성합니다 (1-3분 소요)."
            )

        btn_label = "🔄 리포트 재생성 (1-3분)" if cached else "🎯 리포트 생성 (1-3분)"
        if st.button(btn_label, key=f"ir_gen_{ticker}", use_container_width=True):
            with st.spinner(f"{ticker} deep research 중... 도구 호출·경쟁 분석·메모 작성"):
                try:
                    result = ir.generate_and_save(ticker)
                    st.success(f"✓ 리포트 생성 완료 ({len(result['body']):,}자)")
                    st.rerun()
                except Exception as e:
                    st.error(f"실패: {e}")


def _render_catalyst_section(ticker: str):
    """종목별 다가오는 카탈리스트 + IR 자료에서 추출된 회사 공개 마일스톤.
    KR(6자리)은 DART 전자공시(유증·기술이전·식약처 허가·실적·임상 주요사항)를 카탈리스트 1차 소스로."""
    _t = str(ticker).strip()
    if _t.isdigit() and len(_t) == 6:
        with st.expander("📅 카탈리스트 / 공시 (DART)", expanded=True):
            try:
                from bot_tools import get_dart_disclosures
                r = get_dart_disclosures(_t, days=120)
                ds = r.get("disclosures", []) if isinstance(r, dict) else []
                if isinstance(r, dict) and r.get("error"):
                    st.caption(f"DART: {r['error']}")
                elif not ds:
                    st.caption("최근 120일 공시 없음.")
                else:
                    st.caption("DART 전자공시 — 유증·기술이전·식약처 허가·실적·임상 주요사항 "
                               "(한국 카탈리스트 1차 소스)")
                    for d in ds[:15]:
                        st.markdown(
                            f"- {d['date']} · **[{d['title']}]({d['url']})** "
                            f"<span style='opacity:0.6;font-size:0.85em'>{d.get('filer','')}</span>",
                            unsafe_allow_html=True,
                        )
            except Exception as e:
                st.caption(f"DART 조회 실패: {e}")
        return
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
                         "clinical_readout": "🧪", "clinical_milestone": "🚀",
                         "regulatory": "📜", "conference": "🎤",
                         "company_event": "📑",
                         "earnings_call": "🎙️"}.get(tt, "📅")
                # 워치 토글 체크박스 (per row)
                cid = int(r["id"])
                is_watched = bool(r.get("watched"))
                cc = st.columns([0.5, 11])
                with cc[0]:
                    new_state = st.checkbox(
                        "", value=is_watched, key=f"cat_watch_{cid}",
                        label_visibility="collapsed",
                        help="워치 — 1m·1w 전 텔레그램 알림",
                    )
                    if new_state != is_watched:
                        cat.set_watched(cid, new_state)
                        st.rerun()
                with cc[1]:
                    st.markdown(
                        f"{emoji} **{r['event_date']}** · {r['title'][:160]}  "
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
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("🔄 자동 갱신", key=f"cat_rf_{ticker}",
                         use_container_width=True,
                         help="ClinicalTrials/yfinance/투자자료 등 정적 소스 다시 fetch"):
                with st.spinner("..."):
                    cat.fetch_earnings_dates([ticker])
                    cat.fetch_clinical_completions([ticker])
                    from earnings_calls import fetch_for_ticker as _ec_fetch
                    _ec_fetch(ticker, max_quarters=3)
                st.rerun()
        with col2:
            if st.button("🔍 IR PDF 추출", key=f"irm_rf_{ticker}",
                         use_container_width=True,
                         help="회사 투자자 프레젠테이션 PDF에서 'Anticipated Catalysts' 추출"):
                with st.spinner("IR PDF 분석 (10-30초)..."):
                    result = irm.extract_for_ticker(ticker, save=True)
                if result.get("error"):
                    st.warning(f"⚠️ {result['error']}")
                else:
                    st.success(
                        f"✓ {len(result.get('milestones', []))}개 — "
                        f"{result.get('deck_title', '')[:50]}"
                    )
                    st.rerun()
        with col3:
            if st.button("🤖 AI 발굴 (1-3분)", key=f"cat_ai_{ticker}",
                         use_container_width=True,
                         help="Claude가 능동 조사로 Investor Day, accelerated readout, "
                              "KOL event 등 정적 소스가 놓치는 카탈리스트 발굴"):
                with st.spinner(f"{ticker} 카탈리스트 능동 조사 중 (web 검색·PR fetch)..."):
                    result = cat.discover_catalysts_via_ai(ticker)
                if result.get("error"):
                    st.warning(f"⚠️ {result['error']}")
                else:
                    st.success(
                        f"✓ AI 발굴: {result['found']}개 발견, {result['saved']}개 신규 저장"
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
    _t = str(ticker).strip()
    if _t.isdigit() and len(_t) == 6:
        with st.expander("📰 한국 바이오 매체 뉴스 (최근)", expanded=False):
            arts = _cached_recent_articles(ticker, name, 30)
            if not arts:
                st.caption("최근 한국 매체 기사 없음.")
                return
            st.caption("히트뉴스·팜뉴스·청년의사·더바이오 + 한국어 Google News")
            for it in arts[:8]:
                st.markdown(
                    f"- **[{it['title']}]({it['link']})**  "
                    f"<span style='opacity:0.6;font-size:0.85em'>· {it['source']} "
                    f"{it.get('published','')}</span>",
                    unsafe_allow_html=True,
                )
        return
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


def _build_valuation_template(ticker: str, v: dict) -> str:
    """yfinance 밸류에이션 dict → 메모용 마크다운 템플릿."""
    def _fmt(val, unit="", precision=2):
        if val is None:
            return "—"
        if isinstance(val, (int, float)):
            return f"{val:,.{precision}f}{unit}"
        return f"{val}{unit}"

    pe_t = _fmt(v.get("pe_trailing"), "x")
    pe_f = _fmt(v.get("pe_forward"), "x")
    if v.get("note_pe"):
        pe_t = f"N/M ({v['note_pe']})"
    ev_ebitda = _fmt(v.get("ev_ebitda"), "x")
    if v.get("note_ev_ebitda"):
        ev_ebitda = f"N/M ({v['note_ev_ebitda']})"
    ev_rev = _fmt(v.get("ev_revenue"), "x")
    ps = _fmt(v.get("ps_trailing"), "x")
    pb = _fmt(v.get("pb"), "x")

    cash = v.get("cash_b_usd") or 0
    debt = v.get("debt_b_usd") or 0
    net_cash = cash - debt

    return (
        f"## 💰 밸류에이션 ({ticker})\n\n"
        f"- 시총 ${_fmt(v.get('market_cap_b_usd'))}B · "
        f"EV ${_fmt(v.get('enterprise_value_b_usd'))}B\n"
        f"- **P/E** trailing {pe_t}, forward {pe_f}\n"
        f"- **EV/EBITDA** {ev_ebitda} · EV/Revenue {ev_rev} · P/S {ps} · P/B {pb}\n"
        f"- 매출 (TTM) ${_fmt(v.get('revenue_ttm_b_usd'))}B · "
        f"EBITDA ${_fmt(v.get('ebitda_b_usd'))}B · "
        f"순이익 ${_fmt(v.get('net_income_b_usd'))}B\n"
        f"- 현금 ${_fmt(v.get('cash_b_usd'))}B - 부채 ${_fmt(v.get('debt_b_usd'))}B = "
        f"순현금 ${net_cash:,.2f}B\n"
        f"- 영업이익률 {_fmt(v.get('operating_margin_pct'), '%', 1)} · "
        f"매출총이익률 {_fmt(v.get('gross_margin_pct'), '%', 1)}\n\n"
        f"### Peak sales × OPM × P/EBIT 시나리오 (사용자 입력)\n"
        f"- 자산1: peak $___B × ___% OPM × ___x P/EBIT = $___B 시총\n"
        f"- 자산2: peak $___B × ___% × ___x = $___B 시총\n"
        f"- **합산 implied 시총**: $___B → 현 시총 ${_fmt(v.get('market_cap_b_usd'))}B 대비 ___%\n\n"
        f"### 코멘트\n"
        f"- "
    )


def _render_memo_section(ticker: str):
    from memo import add as memo_add, update as memo_update, delete as memo_delete, list_for

    st.markdown("##### 📝 메모")

    # 💰 밸류에이션 자동 템플릿 — 누르면 현재 yfinance 지표로 마크다운 채워줌
    if st.button("💰 밸류에이션 템플릿 채우기", key=f"val_tpl_{ticker}",
                 help="yfinance에서 시총·EV·P/E·EV/EBITDA·매출 fetch 후 메모 텍스트박스에 채움. "
                      "Peak sales 부분은 사용자가 직접 입력."):
        with st.spinner("밸류에이션 fetch..."):
            try:
                from bot_tools import get_valuation_metrics
                v = get_valuation_metrics(ticker)
                template = _build_valuation_template(ticker, v)
                # session_state로 textarea 초기값 주입
                st.session_state[f"new_body_{ticker}"] = template
                st.rerun()
            except Exception as e:
                st.error(f"실패: {e}")

    _memo_fragment(ticker)


@st.fragment
def _memo_fragment(ticker: str):
    """모달 메모 영역 — fragment로 격리해 메모 추가/수정/삭제 시 대시보드 전체 rerun 방지
    (모달 안에서 이 영역만 갱신)."""
    from memo import (add as memo_add, update as memo_update,
                      delete as memo_delete, list_for)
    # 새 메모
    with st.form(f"new_memo_{ticker}", clear_on_submit=True):
        new_body = st.text_area("새 메모", key=f"new_body_{ticker}",
                                placeholder="여기에 생각 적기…", height=160)
        if st.form_submit_button("추가"):
            if new_body.strip():
                memo_add(ticker, new_body)
                st.rerun(scope="fragment")

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
                    st.rerun(scope="fragment")
                if cc[1].button("취소", key=f"cancel_{m['id']}"):
                    st.session_state[edit_key] = False
                    st.rerun(scope="fragment")
            else:
                st.markdown(m["body"])
                cc = st.columns([1, 1, 6])
                if cc[0].button("수정", key=f"editbtn_{m['id']}"):
                    st.session_state[edit_key] = True
                    st.rerun(scope="fragment")
                if cc[1].button("삭제", key=f"del_{m['id']}"):
                    memo_delete(m["id"])
                    st.rerun(scope="fragment")


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
                help=st.session_state.get("ticker_reason_map", {}).get(row["ticker"]),
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
            import kr_universe as _ku
            _tk = row["ticker"]
            _kr = _ku.is_kr_ticker(_tk)
            mcap = row["market_cap"]
            _price_s = (_ku.fmt_price(row["close"], _tk) if _kr
                        else f"${row['close']:,.2f}") if pd.notna(row["close"]) else "—"
            if mobile:
                cells[2].write(_price_s)
                cells[3].markdown(color_pct(row["perf_1d"]), unsafe_allow_html=True)
                if _kr:
                    cells[4].write(_ku.fmt_mcap(mcap, _tk))
                else:
                    cells[4].write(f"{mcap/1000:,.1f}b" if pd.notna(mcap) and mcap >= 1000
                                   else (f"{mcap:,.0f}m" if pd.notna(mcap) else "—"))
            else:
                cells[2].write(_price_s)
                cells[3].write(_ku.fmt_mcap(mcap, _tk) if _kr
                               else (f"{mcap:,.0f}" if pd.notna(mcap) else "—"))
                for j, k in enumerate(["perf_1d", "perf_7d", "perf_1m", "perf_3m", "perf_6m", "perf_1y"]):
                    cells[4 + j].markdown(color_pct(row[k]), unsafe_allow_html=True)
                cells[10].write((_ku.fmt_price(row['high_52w'], _tk) if _kr
                                 else f"${row['high_52w']:,.2f}") if pd.notna(row["high_52w"]) else "—")


@st.cache_data(ttl=3600, show_spinner=False)
def _cached_reason_analysis(sig: str, rows: tuple, kind: str) -> str:
    """신고가/급등 '이유 추정' 분석 — 텔레그램 _highs_analysis 재사용. sig로 1시간 캐시."""
    import pandas as _pd
    import telegram_report as _tr
    df = _pd.DataFrame(list(rows), columns=["ticker", "name", "close", "perf_1d", "market_cap"])
    label = "오늘 크게 상승(급등)한" if kind == "movers" else "오늘 52주 신고가를 찍은"
    try:
        return _tr._highs_analysis(df, max_n=20, context_label=label)
    except TypeError:
        return _tr._highs_analysis(df, max_n=20)


def _render_reason_section(df, kind: str):
    """티커 리스트 아래 — AI '이유 추정' 카드(텔레 봇과 동일 로직). 버튼 생성 + 캐시로 비용 관리."""
    import re as _re
    if df is None or df.empty:
        return
    sub = df.sort_values("market_cap", ascending=False, na_position="last").head(20)
    rows = tuple(
        (str(r.get("ticker") or ""), str(r.get("name") or ""),
         float(r["close"]) if pd.notna(r.get("close")) else None,
         float(r["perf_1d"]) if pd.notna(r.get("perf_1d")) else None,
         float(r["market_cap"]) if pd.notna(r.get("market_cap")) else None)
        for _, r in sub.iterrows()
    )
    sig = f"{kind}:{latest_run_date()}:" + ",".join(sorted(t[0] for t in rows))
    st.divider()
    st.subheader("🧠 신고가 이유 분석" if kind == "high" else "🧠 급등 이유 분석")
    st.caption("AI 추정 — 상승 동인·핵심 자산/기전·짧은 평가 (텔레그램 데일리와 동일 로직, 투자 추천 아님).")
    gen_key = f"reason_gen_{kind}"
    if st.session_state.get(gen_key) != sig:
        if st.button("🧠 이유 분석 생성 (AI · ~30초)", key=f"reason_btn_{kind}"):
            st.session_state[gen_key] = sig
            st.rerun()
        return
    with st.spinner("AI 이유 분석 생성 중…"):
        md = _cached_reason_analysis(sig, rows, kind)
    if not md:
        st.info("분석 생성 실패 (API 키/크레딧 확인).")
        if st.button("🔄 다시 시도", key=f"reason_retry_{kind}"):
            _cached_reason_analysis.clear()
            st.session_state.pop(gen_key, None)
            st.rerun()
        return
    blocks = [b.strip() for b in _re.split(r"\n(?=\*\*)", md.strip()) if b.strip()]
    # 종목별 블록 → 티커 매핑 (테이블 티커 hover 툴팁용)
    known = [t[0] for t in rows if t[0]]
    rmap = {}
    for blk in blocks:
        head = blk.split("\n", 1)[0]
        for tk in known:
            if _re.search(rf"(?<![A-Za-z0-9]){_re.escape(tk)}(?![A-Za-z0-9])", head, _re.I):
                rmap[tk] = blk
                break
    built_key = f"reason_map_built_{kind}"
    if rmap and st.session_state.get(built_key) != sig:
        shared = dict(st.session_state.get("ticker_reason_map", {}))
        shared.update(rmap)
        st.session_state["ticker_reason_map"] = shared
        st.session_state[built_key] = sig
        st.rerun()   # 테이블 hover 툴팁 즉시 반영
    cols = st.columns(2)
    for i, blk in enumerate(blocks):
        with cols[i % 2]:
            with st.container(border=True):
                st.markdown(blk)


@st.fragment
def _section_high():
    """탭 컨텐츠 — 52주 신고가."""
    if last is None:
        st.warning("아직 신고가 데이터 없음. 우측 '🔄 신고가 갱신' 먼저 실행.")
        return

    view = st.radio(
        "구분", options=["new", "all"], horizontal=True,
        format_func=lambda v: "🆕 오늘 신규" if v == "new" else "📈 전체",
        on_change=_close_modal, key="hl_view",
    )

    _c, _floor = _board_scope()
    if view == "new":
        df = fetch_new_today_highs(limit=300, country=_c, min_mcap=_floor)
        empty_msg = "오늘 신규로 52주 신고가를 찍은 종목 없음."
    else:
        df = fetch_new_highs("high", limit=500, country=_c, min_mcap=_floor)
        empty_msg = "오늘 52주 신고가 종목 없음."

    if df.empty:
        st.info(empty_msg)
        return

    st.caption(f"{len(df)}종목 · 기준일 {last} · 📊 회사명을 클릭하면 모달로 차트+MA가 떠요.")
    _render_table(df)
    _render_reason_section(df, "high")


def _render_watched_catalyst_banner():
    """다가오는 워치 카탈리스트가 있으면 상단 배너로 표시 (1주 / 1개월 임박)."""
    import catalysts as cat
    import datetime as _dt
    df = cat.get_watched(days_ahead=35)
    if df.empty:
        return
    today = _dt.date.today()
    week_items = []
    month_items = []
    for _, r in df.iterrows():
        nd = r.get("notify_date")
        if not nd:
            continue
        try:
            d = _dt.date.fromisoformat(nd)
        except ValueError:
            continue
        days_left = (d - today).days
        if days_left < 0:
            continue
        if days_left <= 7:
            week_items.append((days_left, r))
        elif days_left <= 35:
            month_items.append((days_left, r))
    if not week_items and not month_items:
        return
    parts = []
    if week_items:
        parts.append("🚨 <b>이번주 워치 카탈리스트</b>")
        for days_left, r in sorted(week_items, key=lambda x: x[0])[:5]:
            import re as _re
            desc = r.get("description") or ""
            m = _re.search(r"date_hint:\s*([^·]+)", desc)
            dh = m.group(1).strip() if m else (r.get("event_date") or "")
            tk = (r.get("ticker") or "").upper().strip()
            tk_s = f"<b>{tk}</b> · " if tk else ""
            parts.append(
                f"&nbsp;&nbsp;⏰ D-{days_left} · {dh} · {tk_s}"
                f"{(r.get('title') or '')[:120]}"
            )
    if month_items:
        if week_items:
            parts.append("")
        parts.append("📅 <b>1개월 이내 워치</b>")
        for days_left, r in sorted(month_items, key=lambda x: x[0])[:5]:
            import re as _re
            desc = r.get("description") or ""
            m = _re.search(r"date_hint:\s*([^·]+)", desc)
            dh = m.group(1).strip() if m else (r.get("event_date") or "")
            tk = (r.get("ticker") or "").upper().strip()
            tk_s = f"<b>{tk}</b> · " if tk else ""
            parts.append(
                f"&nbsp;&nbsp;📍 D-{days_left} · {dh} · {tk_s}"
                f"{(r.get('title') or '')[:120]}"
            )
    st.markdown(
        f"<div style='background:#fff3cd; border:1px solid #ffd966; "
        f"border-left:4px solid #f9a825; padding:0.8rem 1.2rem; "
        f"border-radius:8px; margin-bottom:1rem; line-height:1.5; "
        f"font-size:0.92em;'>{'<br/>'.join(parts)}</div>",
        unsafe_allow_html=True,
    )


def render_main_page():
    """메인 대시보드 — 4개 섹션 탭으로 전환."""
    # 탭/시장 상태 준비 (위젯 생성 전에 기본값·강제동기화 처리)
    tab_options = ["high", "top_movers", "daily_news", "memos", "portfolios", "catalysts", "watchlist"]
    tab_labels = {
        "high": "52주 신고가",
        "top_movers": "상승폭",
        "daily_news": "데일리 뉴스",
        "memos": "투자 메모",
        "portfolios": "포트폴리오",
        "catalysts": "카탈리스트",
        "watchlist": "관심종목",
    }
    if "country" not in st.session_state:
        st.session_state["country"] = "USA"
    if "_force_tab" in st.session_state:
        st.session_state["main_tab_radio"] = st.session_state.pop("_force_tab")
    elif "main_tab_radio" not in st.session_state:
        st.session_state["main_tab_radio"] = st.session_state.get("main_tab", "high")
    if st.session_state.get("main_tab_radio") not in tab_options:
        st.session_state["main_tab_radio"] = "high"   # 구버전 'chat' 등 잔존값 방어

    # ── 상단 네비게이션 바 (로고 · 메뉴 · 시장 토글) — 홈페이지형 ──
    with st.container(key="topbar"):
        bar = st.columns([3.0, 6.4, 1.6], vertical_alignment="center")
        with bar[0]:
            st.markdown(
                "<div class='topbar-brand'>"
                "<span class='hero-wordmark'>BioTech&nbsp;Radar</span>"
                "<span class='topbar-tag'>Biotech Trading · Analysis Intelligence</span>"
                "</div>",
                unsafe_allow_html=True,
            )
        with bar[1]:
            chosen = st.radio(
                "탭", options=tab_options, format_func=lambda k: tab_labels[k],
                horizontal=True, key="main_tab_radio", label_visibility="collapsed",
                on_change=_close_modal,
            )
        with bar[2]:
            st.radio(
                "시장", ["USA", "KOR"],
                format_func=lambda k: {"USA": "해외", "KOR": "한국"}[k],
                horizontal=True, key="country", label_visibility="collapsed",
                on_change=_close_modal,
            )
    st.session_state["main_tab"] = chosen

    _render_watched_catalyst_banner()

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
    elif chosen == "watchlist":
        render_watchlist_page()

    # 우하단 플로팅 위젯 — AI 챗(💬) + 운영(⚙). 탭이 아니라 항상 떠 있음.
    _floating_chat_widget()
    _floating_ops_widget()


# ───────────────────────── 카탈리스트 캘린더 ─────────────────────────
@st.fragment
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
            "임상 마일스톤": ["clinical_milestone"],
            "FDA 규제": ["regulatory"],
            "회사 공개": ["company_event"],
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
                f"어닝 {counts['earnings']} · 임상 데이터 {counts['clinical_readout']} · "
                f"transcript 멘션 {counts.get('earnings_call', 0)} (자동분류됨)"
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
        st.caption(f"총 {len(df)}건 · 가까운 순 · 👁️ 컬럼 체크하면 워치 (1m·1w 전 텔레그램 알림)")
        # 일자 표시 — description에 date_hint 있으면 우선 사용
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
        view = df[["id", "watched", "_disp_date", "event_date", "ticker",
                   "event_type", "title", "therapy_area", "source"]].copy()
        view.columns = ["id", "👁️", "일자", "정렬일", "티커",
                        "타입", "제목", "분야", "소스"]
        view["티커"] = view["티커"].fillna("—")
        view["분야"] = view["분야"].fillna("—")
        view["👁️"] = view["👁️"].fillna(False).astype(bool)

        edited = st.data_editor(
            view.drop(columns=["정렬일"]),
            use_container_width=True, hide_index=True, height=520,
            column_config={
                "id": None,   # 숨김
                "👁️": st.column_config.CheckboxColumn(
                    "👁️", help="워치 (1개월·1주 전 알림)", width="small",
                ),
            },
            disabled=["일자", "티커", "타입", "제목", "분야", "소스"],
            key="cat_editor",
        )
        # 변경된 watched 토글 detection
        if edited is not None:
            for _, before, after in zip(view.index, view.to_dict("records"),
                                         edited.to_dict("records")):
                if before["👁️"] != after["👁️"]:
                    cat.set_watched(int(before["id"]), bool(after["👁️"]))
                    st.toast(
                        f"{'✅ 워치 추가' if after['👁️'] else '❌ 워치 해제'}"
                        f": {after['제목'][:40]}",
                        icon="👁️",
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
    s = _cached_pf_summary(portfolio_id)
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

    # 요약 metric (거래기반: NAV / 총수익률 / 현금 / 실현손익)
    m = st.columns(4)
    color = "#26a69a" if s["return_pct"] >= 0 else "#ef5350"
    rcolor = "#26a69a" if s.get("realized_pnl", 0) >= 0 else "#ef5350"
    m[0].markdown(f"<div style='font-size:0.8em; color:#666;'>현재 NAV</div>"
                  f"<div style='font-size:1.4em; font-weight:700;'>${s['current_size']/1e6:,.2f}M</div>",
                  unsafe_allow_html=True)
    m[1].markdown(f"<div style='font-size:0.8em; color:#666;'>총수익률</div>"
                  f"<div style='font-size:1.4em; font-weight:700; color:{color};'>{s['return_pct']:+.2f}%</div>",
                  unsafe_allow_html=True)
    m[2].markdown(f"<div style='font-size:0.8em; color:#666;'>현금</div>"
                  f"<div style='font-size:1.4em; font-weight:700;'>${s['cash_amt']/1e6:,.1f}M</div>",
                  unsafe_allow_html=True)
    m[3].markdown(f"<div style='font-size:0.8em; color:#666;'>실현손익</div>"
                  f"<div style='font-size:1.4em; font-weight:700; color:{rcolor};'>${s.get('realized_pnl',0)/1e6:+,.2f}M</div>",
                  unsafe_allow_html=True)
    st.caption(
        f"미실현 ${s.get('unrealized_pnl',0)/1e6:+,.2f}M · 투자원가 ${s.get('invested',0)/1e6:,.1f}M · "
        f"편입 {s['total_weight']:.1f}% · 현금 {s['cash_pct']:.1f}%"
    )

    st.divider()

    # 종목 추가 (form) — 목표 비중까지 현재가로 매수
    with st.expander("＋ 종목 편입 (현재가 매수)", expanded=not s["holdings"]):
        with st.form(f"add_holding_{portfolio_id}", clear_on_submit=True):
            cc = st.columns([2, 1, 1])
            with cc[0]:
                ticker_in = st.text_input("티커", placeholder="VRTX")
            with cc[1]:
                weight_in = st.number_input("목표 비중 %", min_value=0.0, max_value=100.0,
                                            value=5.0, step=0.5)
            with cc[2]:
                st.write("")
                submitted = st.form_submit_button("편입", type="primary",
                                                  use_container_width=True)
            if submitted and ticker_in.strip():
                try:
                    r = pf.add_holding(portfolio_id, ticker_in, weight_in)
                    st.success(f"{ticker_in.upper()} 편입 — {r.get('action')} "
                               f"${r.get('amount_usd',0)/1e6:,.2f}M @ ${r.get('price',0):,.2f}")
                    _cached_pf_summary.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"실패: {e}")

    # 종목 리스트 + 비중 조정
    if not s["holdings"]:
        st.info("아직 편입 종목 없음.")
        return

    st.markdown("##### 편입 종목  ·  목표 비중% 입력 후 **조정** → 현재가 체결(실현손익·현금 반영)")
    schema = [("티커", 1.1), ("회사명", 2.4), ("비중%", 1.0), ("평단", 1.4),
              ("현재가", 1.4), ("수익률", 1.3), ("평가액", 1.5), ("실현", 1.3),
              ("목표%", 1.3), ("", 1.0), ("", 0.6)]
    weights = [w for _, w in schema]
    hdr = st.columns(weights, vertical_alignment="center")
    for i, (label, _) in enumerate(schema):
        hdr[i].markdown(f"**{label}**")

    for h in s["holdings"]:
        tk = h["ticker"]
        cells = st.columns(weights, vertical_alignment="center")
        cells[0].caption(tk)
        cells[1].caption((h.get("name") or tk)[:24])
        cells[2].write(f"{h['weight_pct']:.1f}%")
        cells[3].write(f"${h['avg_cost']:,.2f}")
        cells[4].write(f"${h['curr_price']:,.2f}")
        ret = h["return_pct"]
        clr = "#26a69a" if ret >= 0 else "#ef5350"
        cells[5].markdown(f"<span style='color:{clr}; font-weight:600;'>{ret:+.1f}%</span>",
                          unsafe_allow_html=True)
        cells[6].write(f"${h['amt_current']/1e6:,.2f}M")
        rp = h.get("realized_pnl", 0)
        if abs(rp) >= 1:
            rpc = "#26a69a" if rp >= 0 else "#ef5350"
            cells[7].markdown(f"<span style='color:{rpc}; font-size:0.85em;'>${rp/1e6:+,.2f}M</span>",
                              unsafe_allow_html=True)
        else:
            cells[7].caption("—")
        tgt = cells[8].number_input("목표%", min_value=0.0, max_value=100.0,
                                    value=round(float(h["weight_pct"]), 1), step=0.5,
                                    key=f"tgt_{portfolio_id}_{tk}",
                                    label_visibility="collapsed")
        if cells[9].button("조정", key=f"adj_{portfolio_id}_{tk}",
                           use_container_width=True):
            try:
                r = pf.set_target_weight(portfolio_id, tk, float(tgt))
                act = {"sold": "매도", "bought": "매수", "noop": "변동없음"}.get(r.get("action"), r.get("action"))
                st.toast(f"{tk} {act} ${r.get('amount_usd',0)/1e6:,.2f}M @ ${r.get('price',0):,.2f}"
                         + (f" · 실현 ${r.get('realized_pnl',0)/1e6:+,.2f}M" if r.get('realized_pnl') else ""))
                _cached_pf_summary.clear()
                st.rerun()
            except Exception as e:
                st.toast(f"⚠️ {tk}: {e}")
        if cells[10].button("✗", key=f"rm_{portfolio_id}_{tk}", help="전량 매도"):
            try:
                pf.sell_all(portfolio_id, tk)
                _cached_pf_summary.clear()
                st.rerun()
            except Exception as e:
                st.toast(f"⚠️ {e}")

    # 거래내역 로그
    txs = pf.transactions_log(portfolio_id, limit=40)
    if txs:
        with st.expander(f"📒 거래내역 ({len(txs)})"):
            for t in txs:
                act = "🟢 매수" if t["action"] == "buy" else "🔴 매도"
                rp = (f" · 실현 ${t['realized_pnl']/1e6:+,.2f}M"
                      if t["action"] == "sell" and t.get("realized_pnl") else "")
                st.caption(
                    f"{t['trade_date']}  {act}  **{t['ticker']}**  "
                    f"{t['shares']:,.0f}주 @ ${t['price']:,.2f}  "
                    f"(${t['amount']/1e6:,.2f}M){rp}"
                )


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
                s = _cached_pf_summary(p["id"])
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


@st.cache_data(ttl=900, show_spinner=False)
def _cached_kr_news(days: int, query: str):
    import kr_news
    if query.strip():
        return kr_news.for_query(query, limit=60, days=max(days, 14))
    return kr_news.latest(limit=60, days=days)


def _section_daily_news_kr():
    """한국 바이오 전문매체 데일리 뉴스 — 히트뉴스·팜뉴스·청년의사·더바이오 RSS."""
    cc = st.columns([1, 2, 5])
    with cc[0]:
        days = st.selectbox("기간", [1, 3, 7], index=1, key="kr_news_days",
                            format_func=lambda d: f"{d}일")
    with cc[1]:
        q = st.text_input("검색(종목·키워드)", key="kr_news_q",
                          placeholder="예: 알테오젠 / ADC / 비만")
    with st.spinner("한국 바이오 매체 RSS 통합..."):
        items = _cached_kr_news(days, q or "")
    if not items:
        st.info("해당 기간 기사 없음.")
        return
    st.caption(f"📰 {len(items)}건 · 히트뉴스·팜뉴스·청년의사·더바이오")
    for it in items:
        d = it["published"].strftime("%m-%d %H:%M") if it.get("published") else ""
        st.markdown(
            f"- **[{it['title']}]({it['link']})**  "
            f"<span style='color:#888;font-size:0.85em;'>· {it['source']} {d}</span>",
            unsafe_allow_html=True,
        )


def _chat_to_markdown(msgs: list[dict]) -> str:
    """챗 히스토리 → PDF용 마크다운."""
    from datetime import datetime as _dt
    out = [f"# Biotech Radar — AI 챗 대화\n",
           f"_{_dt.now().strftime('%Y-%m-%d %H:%M')}_\n"]
    for m in msgs:
        if m.get("role") == "user":
            out.append(f"\n---\n\n## ❓ {m.get('content','')}\n")
        else:
            out.append(f"\n{m.get('content','')}\n")
    return "\n".join(out)


def _render_chat_core(box_height: int = 330):
    """플로팅 챗 위젯 본체 — 텔레그램↔웹 공유 AI 애널리스트 (bot_agent 공용).
    위젯 fragment 안에서 호출되어 질문/응답 시 보드가 아니라 위젯만 rerun."""
    import bot_agent
    # 입력창을 흰 배경 + 테두리로 구분
    st.markdown(
        """
        <style>
        [data-testid="stChatInput"] { background:#ffffff !important;
            border:1px solid #cfd3da; border-radius:10px; }
        [data-testid="stChatInput"] textarea { background:#ffffff !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    import chat_store
    msgs0 = chat_store.recent_display(60)   # 텔레그램↔웹 공유 대화
    msgs = msgs0

    # 대화 로그 — 고정 높이 스크롤 박스(일반 챗봇 형태). 입력창은 박스 아래 고정.
    box = st.container(height=box_height, border=True)
    with box:
        if not msgs:
            st.caption("아직 대화가 없습니다. 아래에 질문을 입력하세요.")
        for i, m in enumerate(msgs):
            with st.chat_message(m["role"]):
                if m.get("source") == "telegram":
                    st.caption("📱 텔레그램")
                st.markdown(m["content"])
                if m["role"] == "assistant" and not str(m["content"]).startswith("⚠️"):
                    if st.button("📤 텔레그램", key=f"tg_msg_{i}",
                                 help="이 답변을 텔레그램으로 전송"):
                        try:
                            from telegram_report import send, _markdown_to_html
                            send(_markdown_to_html(m["content"]))
                            st.toast("📤 텔레그램 전송 완료")
                        except Exception as e:
                            st.toast(f"⚠️ {type(e).__name__}: {e}")

    upfiles = st.file_uploader(
        "📎 파일 첨부 (PDF·이미지·txt·csv·md) — 업로드 후 아래에 질문 입력",
        type=["pdf", "png", "jpg", "jpeg", "txt", "md", "csv"],
        accept_multiple_files=True, key="chat_files",
    )
    prompt = st.chat_input(
        "질문 입력  (예: KRAS G12D degrader 기전 / 첨부 IR PDF 요약·분석 / RVMD 분석)"
    )
    if prompt:
        import base64 as _b64
        attachments = []
        for f in (upfiles or []):
            try:
                data = f.getvalue()
                ext = (f.name.rsplit(".", 1)[-1] if "." in f.name else "").lower()
                if ext == "pdf":
                    attachments.append({"kind": "pdf", "name": f.name,
                                        "data": _b64.b64encode(data).decode()})
                elif ext in ("png", "jpg", "jpeg"):
                    attachments.append({"kind": "image", "name": f.name,
                                        "media_type": "image/png" if ext == "png" else "image/jpeg",
                                        "data": _b64.b64encode(data).decode()})
                else:
                    attachments.append({"kind": "text", "name": f.name,
                                        "text": data.decode("utf-8", errors="replace")[:20000]})
            except Exception:
                pass
        _disp = prompt + (f"  \n_📎 {len(attachments)}개 파일 첨부됨_" if attachments else "")
        history = chat_store.recent(40)   # 공유 대화 직전까지
        with box:
            with st.chat_message("user"):
                st.markdown(_disp)
            with st.chat_message("assistant"):
                with st.spinner("조사 중… (도구 호출/파일 분석, 최대 1-2분)"):
                    try:
                        text, _ = bot_agent.run_agent(prompt, history,
                                                      attachments=attachments)
                    except Exception as e:
                        text = f"⚠️ 오류: {type(e).__name__}: {e}"
                st.markdown(text)
        chat_store.append("user", _disp, "web")
        chat_store.append("assistant", text, "web")
        st.rerun(scope="fragment")


@st.fragment
def _floating_chat_widget():
    """우하단 플로팅 AI 챗 — 어느 탭/모달에서든 떠 있는 런처 버튼 ↔ 펼친 패널(최소화 가능).
    @st.fragment 라 버튼·질문·응답이 보드를 리런하지 않고 위젯만 갱신한다."""
    import chat_store
    open_ = st.session_state.get("chat_widget_open", False)
    st.markdown(
        """
        <style>
        .st-key-chatbtn { position: fixed; bottom: 1.6rem; right: 1.7rem;
            z-index: 2147483000; width: auto !important; }
        /* 전역 메인-버튼(투명) 규칙을 이기도록 특이도 ↑ — 솔리드 초록 박스 */
        div[data-testid="stMainBlockContainer"] .st-key-chatbtn button {
            border-radius: 16px !important; min-height: 64px !important; height: 64px !important;
            padding: 0 2.0rem !important; font-size: 1.45rem !important; font-weight: 800 !important;
            letter-spacing: .08em !important; line-height: 1 !important;
            justify-content: center !important; text-align: center !important;
            background: #0a3d3a !important; color: #ffffff !important; border: none !important;
            box-shadow: 0 10px 26px -6px rgba(10,61,58,0.55) !important; }
        div[data-testid="stMainBlockContainer"] .st-key-chatbtn button:hover {
            background: #0f5a52 !important; color: #ffffff !important; }
        .st-key-chatpanel { position: fixed; bottom: 1.1rem; right: 1.1rem;
            z-index: 2147483000; width: 470px; height: 600px;
            min-width: 330px; min-height: 340px; max-width: 96vw; max-height: 90vh;
            resize: both; overflow: auto; direction: rtl;   /* 좌하단 손잡이로 크기 조절 */
            background: #f3f5f8; border: 1px solid #cfd3da; border-radius: 14px;
            box-shadow: 0 10px 34px rgba(0,0,0,0.32); padding: 0.6rem 0.8rem 0.3rem; }
        .st-key-chatpanel > * { direction: ltr; }
        .st-key-chatpanel [data-testid="stVerticalBlock"] { gap: 0.45rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    if not open_:
        with st.container(key="chatbtn"):
            if st.button("💬 CHAT", key="chat_launch", help="AI 챗 열기 (텔레그램 봇과 공유 대화)"):
                st.session_state["chat_widget_open"] = True
                st.rerun(scope="fragment")
        return

    with st.container(key="chatpanel"):
        hdr = st.columns([4.2, 1, 1, 1])
        with hdr[0]:
            st.markdown(
                "**💬 AI 챗** <span style='opacity:.55;font-size:.78rem;'>· 텔레 공유</span>",
                unsafe_allow_html=True)
        with hdr[1]:
            if st.button("📄", key="chat_pdf_tg", use_container_width=True,
                         help="전체 대화를 PDF로 만들어 텔레그램 발송"):
                try:
                    import os as _os
                    from pdf_gen import render_pdf_to_file
                    from telegram_report import send_document
                    with st.spinner("PDF 생성·전송 중…"):
                        path = render_pdf_to_file(_chat_to_markdown(chat_store.recent_display(60)),
                                                  ticker="chat", title="AI 챗 대화")
                        try:
                            send_document(path, caption="💬 <b>AI 챗 대화</b>")
                        finally:
                            try:
                                _os.unlink(path)
                            except Exception:
                                pass
                    st.toast("📄 대화 PDF 텔레그램 전송 완료")
                except Exception as e:
                    st.toast(f"⚠️ {type(e).__name__}: {e}")
        with hdr[2]:
            if st.button("🗑️", key="chat_reset", use_container_width=True,
                         help="대화 초기화 (텔레그램과 공유 — 양쪽 초기화)"):
                chat_store.clear()
                st.rerun(scope="fragment")
        with hdr[3]:
            if st.button("➖", key="chat_min", use_container_width=True, help="최소화"):
                st.session_state["chat_widget_open"] = False
                st.rerun(scope="fragment")
        _render_chat_core(box_height=320)


@st.fragment
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

    _c, _floor = _board_scope()
    if _c == "KOR":
        min_mcap = _floor   # 한국은 5천억 하한 적용
    df = fetch_top_movers(limit=limit, min_mcap=min_mcap, min_perf=min_perf, country=_c)
    if df.empty:
        st.info("조건에 맞는 종목 없음. '🔄 신고가 갱신' (52주 신고가 탭) 먼저 실행하세요.")
        return

    st.caption(f"{len(df)}종목 · 기준일 {latest_run_date() or '—'} · 회사명 클릭 → 상세")
    _render_table(df)
    _render_reason_section(df, "movers")


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

    st.subheader("⭐ 관심종목")
    st.caption("신고가 여부 무관")

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

render_main_page()
