"""신고가/상승폭 'AI 상승이유' 캐시 — DB(reason_cache 테이블) 기반.

기존엔 로컬 파일(data/reason_cache.json)이라 ①로컬에서 생성한 분석이 클라우드(Render)로
공유 안 되고 ②Render는 파일시스템이 휘발성이라 재배포 때마다 사라졌다. → DB로 옮겨
로컬·클라우드가 같은 캐시를 보고, 데일리런이 스냅샷 만들 때 같이 채워 둔다.

유효성 = `snapshot_date`(보드 기준일 latest_run_date(country)). 보드가 새 스냅샷으로
넘어가면 자동 무효 → 재생성. 같은 스냅샷 동안은 새로고침·재배포에도 유지.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime

import pandas as pd

from db import connect

log = logging.getLogger(__name__)

MAX = 100      # 분석 대상 상한(과다 방지)
CHUNK = 22     # 청크당 종목 수 — 토큰 한도로 잘리지 않게 분할
_COLS = ["ticker", "name", "close", "perf_1d", "market_cap"]


def _covered_tickers(md: str) -> set[str]:
    """생성된 markdown에서 커버된 티커 집합 (App.jsx parseReasonByTicker와 동일 규칙)."""
    out: set[str] = set()
    for line in (md or "").split("\n"):
        h = re.match(r"^\s*\*\*(.+?)\*\*", line)
        if not h:
            continue
        seg = h.group(1)
        m = re.search(r"·\s*([A-Z]{1,6}|\d{6})\b", seg) or re.search(r"\b(\d{6}|[A-Z]{2,6})\b", seg)
        if m:
            out.add(m.group(1))
    return out


def _analyze_chunk(chunk, label: str, retries: int = 2) -> str:
    """한 청크 분석 — 빈 결과/예외면 재시도(모든 예외 포착). 최종 실패 시 ''.
    상위 generate()가 누락분을 더 작은 청크로 보충하므로 여기선 조용히 실패해도 됨."""
    import telegram_report as tr
    for attempt in range(retries + 1):
        try:
            try:
                md = tr._highs_analysis(chunk, max_n=len(chunk), context_label=label)
            except TypeError:
                md = tr._highs_analysis(chunk, max_n=len(chunk))
            if md and md.strip():
                return re.split(r"\n-{3,}\s*\n?\s*\*\*\s*요약", md)[0].strip()
            log.warning("reason 청크 빈 결과 (%d/%d, n=%d)", attempt + 1, retries + 1, len(chunk))
        except Exception as e:
            log.warning("reason 청크 실패 (%d/%d, n=%d): %s", attempt + 1, retries + 1, len(chunk), e)
    return ""


def _gen_over(df, label: str, size: int) -> list[str]:
    """df를 size 청크로 순회 분석 → 성공한 markdown 조각 리스트."""
    parts = []
    for i in range(0, len(df), size):
        md = _analyze_chunk(df.iloc[i:i + size], label)
        if md:
            parts.append(md)
    return parts


def get(country: str, kind: str, snap: str | None) -> str | None:
    """snapshot_date == snap 인 캐시 markdown 반환 (없거나 스냅샷 불일치면 None)."""
    if not snap:
        return None
    with connect() as conn:
        r = conn.execute(
            "SELECT markdown FROM reason_cache "
            "WHERE country=? AND kind=? AND snapshot_date=?",
            (country, kind, snap),
        ).fetchone()
    return r["markdown"] if r and r["markdown"] else None


def put(country: str, kind: str, snap: str, markdown: str) -> None:
    with connect() as conn:
        conn.execute(
            """INSERT INTO reason_cache (country, kind, snapshot_date, markdown, updated_at)
               VALUES (?,?,?,?,?)
               ON CONFLICT (country, kind) DO UPDATE SET
                 snapshot_date = excluded.snapshot_date,
                 markdown      = excluded.markdown,
                 updated_at    = excluded.updated_at""",
            (country, kind, snap, markdown,
             datetime.now().isoformat(timespec="seconds")),
        )
        conn.commit()


def generate(kind: str, rows: list[dict]) -> str:
    """행 목록(ticker,name,close,perf_1d,market_cap) → AI 상승이유 마크다운(청크 분할).
    DB 미접근 — 순수 생성. 빈 입력이면 ''.

    재발 방지: 청크가 실패해도 그 종목들을 통째로 버리지 않는다. 1차(22)로 돌린 뒤
    커버 안 된 티커만 더 작은 청크(7→1)로 보충 재시도 → 포이즌 티커를 격리하고
    상위 종목 누락(예: 'RVMD 전까지 박스 안 뜸')을 방지."""
    rows = (rows or [])[:MAX]
    if not rows:
        return ""
    label = "오늘 크게 상승(급등)한" if kind == "movers" else "오늘 52주 신고가를 찍은"
    full = pd.DataFrame([{c: r.get(c) for c in _COLS} for r in rows])
    want = [str(t).strip() for t in full["ticker"].tolist() if str(t).strip()]
    parts = _gen_over(full, label, CHUNK)
    tk_col = full["ticker"].astype(str).str.strip()
    for size in (max(6, CHUNK // 3), 1):        # 누락 보충: 점점 작은 청크로
        missing = [t for t in want if t not in _covered_tickers("\n\n".join(parts))]
        if not missing:
            break
        if size == 1 and len(missing) > 12:     # 대량 누락=시스템 장애 → 개별 재시도 낭비 방지
            log.warning("reason 대량 누락 %d종목 → 개별 재시도 생략", len(missing))
            break
        log.warning("reason 누락 %d종목 보충(size=%d): %s", len(missing), size, missing)
        parts += _gen_over(full[tk_col.isin(missing)], label, size)
    return "\n\n".join(parts)


def refresh(country: str, kind: str, rows: list[dict], snap: str) -> str:
    """생성 후 DB 저장. 단, 새 결과 커버리지가 기존보다 나쁘면 기존을 유지(반쪽 결과로
    덮어쓰기 방지) — API 일시장애로 재생성이 반쪽 나도 기존 좋은 캐시를 지킨다."""
    md = generate(kind, rows)
    if not (md and snap):
        return md
    old = get(country, kind, snap)
    if old and len(_covered_tickers(md)) < len(_covered_tickers(old)):
        log.warning("reason 재생성 커버리지 저하(new=%d < old=%d) → 기존 캐시 유지",
                    len(_covered_tickers(md)), len(_covered_tickers(old)))
        return old
    put(country, kind, snap, md)
    return md
