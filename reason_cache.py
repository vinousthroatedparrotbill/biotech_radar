"""신고가/상승폭 'AI 상승이유' 캐시 — DB(reason_cache 테이블) 기반.

기존엔 로컬 파일(data/reason_cache.json)이라 ①로컬에서 생성한 분석이 클라우드(Render)로
공유 안 되고 ②Render는 파일시스템이 휘발성이라 재배포 때마다 사라졌다. → DB로 옮겨
로컬·클라우드가 같은 캐시를 보고, 데일리런이 스냅샷 만들 때 같이 채워 둔다.

유효성 = `snapshot_date`(보드 기준일 latest_run_date(country)). 보드가 새 스냅샷으로
넘어가면 자동 무효 → 재생성. 같은 스냅샷 동안은 새로고침·재배포에도 유지.
"""
from __future__ import annotations

import re
from datetime import datetime

import pandas as pd

from db import connect

MAX = 100      # 분석 대상 상한(과다 방지)
CHUNK = 22     # 청크당 종목 수 — 토큰 한도로 잘리지 않게 분할
_COLS = ["ticker", "name", "close", "perf_1d", "market_cap"]


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
    DB 미접근 — 순수 생성. 빈 입력이면 ''."""
    import telegram_report as tr
    rows = (rows or [])[:MAX]
    if not rows:
        return ""
    label = "오늘 크게 상승(급등)한" if kind == "movers" else "오늘 52주 신고가를 찍은"
    full = pd.DataFrame([{c: r.get(c) for c in _COLS} for r in rows])
    parts: list[str] = []
    for i in range(0, len(full), CHUNK):
        chunk = full.iloc[i:i + CHUNK]
        try:
            md = tr._highs_analysis(chunk, max_n=len(chunk), context_label=label)
        except TypeError:
            md = tr._highs_analysis(chunk, max_n=len(chunk))
        if not md:
            continue
        md = re.split(r"\n-{3,}\s*\n?\s*\*\*\s*요약", md)[0].strip()  # 청크별 '요약' 제거
        parts.append(md)
    return "\n\n".join(parts)


def refresh(country: str, kind: str, rows: list[dict], snap: str) -> str:
    """생성 후 DB 저장. 빈 결과면 저장하지 않음. 생성된 markdown 반환."""
    md = generate(kind, rows)
    if md and snap:
        put(country, kind, snap, md)
    return md
