"""Biotech Radar — 미국 Healthcare 52주 신고가 + 종목 상세."""
from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st

from db import init_db
from universe import load_universe, get_universe
from collectors.high_low import (
    collect as hl_collect, fetch_new_highs, latest_run_date,
    fetch_new_today_highs,
)

st.set_page_config(page_title="Biotech Radar", layout="wide", page_icon="🧬")
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
  /* primary 버튼도 동일 처리 (저장 같은 강조 액션도 차분하게) */
  div[data-testid="stMainBlockContainer"] [data-testid="stBaseButton-primary"],
  div[data-testid="stDialog"] [data-testid="stBaseButton-primary"] {
    color: #0d3b3a !important;
    font-weight: 600 !important;
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


if st.sidebar.button("🔄 Universe 갱신 (Finviz)"):
    _close_modal()
    with st.spinner("Finviz Elite 스크리너 호출 중..."):
        try:
            n = load_universe()
            st.sidebar.success(f"{n}종목 로드")
        except Exception as e:
            st.sidebar.error(f"실패: {e}")

if st.sidebar.button("📈 52주 신고가 갱신"):
    _close_modal()
    with st.spinner("yfinance OHLCV 일괄 다운로드 (~수분)..."):
        try:
            n = hl_collect(industry_filter=None)
            st.sidebar.success(f"{n}종목 처리")
        except Exception as e:
            st.sidebar.error(f"실패: {e}")

if st.sidebar.button("📝 메모 타임라인"):
    _close_modal()
    st.session_state["page"] = "memos"
    st.rerun()

if st.sidebar.button("📨 텔레그램 테스트 발송"):
    _close_modal()
    with st.spinner("텔레그램 발송 중..."):
        try:
            from telegram_report import send, compose_report
            send(compose_report())
            st.sidebar.success("발송됨")
        except Exception as e:
            st.sidebar.error(f"실패: {e}")

if st.sidebar.button("🔍 전체 URL 자동 탐색", help="universe 모든 종목의 IR/Pipeline URL 일괄 탐색 (~1~2분)"):
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
            st.sidebar.success(f"{saved}/{len(tks)}종목 URL 저장")
        except Exception as e:
            st.sidebar.error(f"실패: {e}")

st.sidebar.divider()
universe_count = len(get_universe())
st.sidebar.caption(f"현재 universe: {universe_count}종목")
last = latest_run_date()
st.sidebar.caption(f"마지막 신고가 갱신: {last or '—'}")


# ───────────────────────── 메모 타임라인 페이지 ─────────────────────────
def render_memo_timeline_page():
    from memo import timeline as memo_timeline

    @st.cache_data(ttl=600)
    def _cached_timeline(limit: int):
        return memo_timeline(limit=limit)

    cc = st.columns([1, 8])
    with cc[0]:
        if st.button("← 메인", key="back_main"):
            st.session_state["page"] = "main"
            st.rerun()

    st.markdown(
        """
        <div class="hero-banner" style="
          background: linear-gradient(135deg, #134e4a 0%, #0a3d3a 100%);
          color: #fff; padding: 1.2rem 1.8rem; border-radius: 12px;
          margin-bottom: 1.2rem;
        ">
          <h1 style="margin:0; font-size:1.6rem;">📝 메모 타임라인</h1>
          <div style="opacity:0.85; font-size:0.9rem; margin-top:0.25rem;">
            최신순 · 액면분할/배당 자동 보정 · 유상증자는 시장가 기준
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

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
                if st.button("종목 보기", key=f"open_{m['id']}",
                             use_container_width=True):
                    st.session_state["detail_ticker"] = m["ticker"]
                    st.session_state["detail_name"] = m.get("name") or m["ticker"]
                    st.session_state["detail_open"] = True
                    st.session_state["page"] = "main"
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


def render_stock_detail(ticker: str, name: str):
    import plotly.graph_objects as go
    from prices import fetch_chart, PERIOD_LABELS

    st.markdown(f"### {name} ({ticker})")

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
            hist = fetch_chart(ticker, period, interval)
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

    # ── 메모 (토글들 밑) ──
    st.divider()
    _render_memo_section(ticker)


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

        # 자료 URL 리스트
        with st.spinner("IR 자료 URL 수집..."):
            assets = fetch_pdf_links(ir_url, limit=40)
        err = next((l.get("_error") for l in assets if l.get("_error")), None)

        if not (err or not assets):
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
        elif err:
            st.caption(f"⚠️ 자동 추출 실패 — JS 렌더/봇 차단 사이트 가능성. 위 링크로 새 창에서 확인.")

        # iframe 시도 — 임베드 허용하는 사이트(Mirum 등)는 이대로 보임
        st.divider()
        st.caption("아래는 임베드 시도 — 회색 빈 박스면 사이트가 iframe 차단함 (위 링크 사용)")
        st.markdown(
            f'<iframe src="{ir_url}" width="100%" height="650" '
            f'style="border:1px solid #cbd9d6; border-radius:8px;"></iframe>',
            unsafe_allow_html=True,
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
    from news import top_pipelines, news_count

    with st.expander("📰 뉴스 멘션 — 최근 6개월 가장 많이 언급된 파이프라인 TOP 3", expanded=False):
        st.caption(f"검색: '{name}' · Yahoo Finance + Google News (6개월)")
        with st.spinner("뉴스 분석..."):
            try:
                nc = news_count(ticker, name, days=180)
                top = top_pipelines(ticker, name, days=180)
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
                st.session_state["detail_ticker"] = row["ticker"]
                st.session_state["detail_name"] = row["name"]
                st.session_state["detail_open"] = True
                st.rerun()
            cells[2].write(f"${row['close']:,.2f}" if pd.notna(row["close"]) else "—")
            mcap = row["market_cap"]
            cells[3].write(f"{mcap:,.0f}" if pd.notna(mcap) else "—")
            for j, k in enumerate(["perf_1d", "perf_7d", "perf_1m", "perf_3m", "perf_6m", "perf_1y"]):
                cells[4 + j].markdown(color_pct(row[k]), unsafe_allow_html=True)
            cells[10].write(f"${row['high_52w']:,.2f}" if pd.notna(row["high_52w"]) else "—")


def render_main_page():
    st.markdown(
        f"""
        <div class="hero-banner" style="
          background: linear-gradient(135deg, #134e4a 0%, #0a3d3a 100%);
          color: #fff; padding: 1.5rem 2rem; border-radius: 12px;
          margin-bottom: 1.5rem;
          box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        ">
          <h1 style="margin:0;">🧬 Biotech Radar</h1>
          <div style="opacity:0.85; font-size:0.95rem; margin-top:0.3rem;">
            글로벌 Healthcare · 시총 ≥ $1.5B · 52주 신고가/신저가 · {datetime.now():%Y-%m-%d %H:%M}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if last is None:
        st.warning("아직 신고가 데이터 없음. 사이드바에서 1) Universe 갱신 → 2) 52주 신고가 갱신 순으로 실행하세요.")
        return

    view = st.radio(
        "구분", options=["new", "all"], horizontal=True,
        format_func=lambda v: "🆕 오늘 신규 52w 신고가" if v == "new" else "📈 전체 52w 신고가",
        on_change=_close_modal, key="hl_view",
    )

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


# ───────────────────────── 라우팅 ─────────────────────────
# 모달은 어느 페이지에서든 detail_open이면 띄움
if st.session_state.get("detail_open"):
    detail_ticker = st.session_state.get("detail_ticker")
    if detail_ticker:
        _detail_dialog(detail_ticker, st.session_state.get("detail_name") or detail_ticker)

page = st.session_state.get("page", "main")
if page == "memos":
    render_memo_timeline_page()
else:
    render_main_page()
