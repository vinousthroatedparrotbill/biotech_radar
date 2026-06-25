"""AI 챗 공유 대화 로그 — 텔레그램 봇 ↔ 웹앱 챗이 하나의 대화를 공유(단일 사용자).

DB의 chat_log 테이블에 append, recent로 읽어 run_agent 히스토리로 사용.
DB 실패 시에도 봇이 죽지 않도록 모든 함수가 예외를 삼키고 안전한 기본값 반환.
"""
from __future__ import annotations

import logging
from datetime import datetime

from db import connect, pd_read_sql

log = logging.getLogger(__name__)


def append(role: str, content: str, source: str = "") -> None:
    """대화 한 줄 추가 (role: 'user'|'assistant')."""
    if not content:
        return
    try:
        with connect() as c:
            c.execute(
                "INSERT INTO chat_log (role, content, source, created_at) VALUES (?,?,?,?)",
                (role, content, source,
                 datetime.now().isoformat(timespec="seconds")),
            )
            c.commit()
    except Exception as e:
        log.warning("chat_store.append 실패: %s", e)


def recent(limit: int = 40) -> list[dict]:
    """최근 대화 limit개 → [{'role','content'}] (오래된→최신 순, run_agent 히스토리용)."""
    try:
        df = pd_read_sql(
            "SELECT role, content FROM chat_log ORDER BY id DESC LIMIT ?",
            params=(limit,),
        )
        rows = [{"role": r["role"], "content": r["content"]}
                for _, r in df.iterrows()]
        return list(reversed(rows))
    except Exception as e:
        log.warning("chat_store.recent 실패: %s", e)
        return []


def recent_display(limit: int = 60) -> list[dict]:
    """표시용 — source 포함 [{'role','content','source'}]."""
    try:
        df = pd_read_sql(
            "SELECT role, content, source FROM chat_log ORDER BY id DESC LIMIT ?",
            params=(limit,),
        )
        rows = [{"role": r["role"], "content": r["content"],
                 "source": r.get("source") or ""} for _, r in df.iterrows()]
        return list(reversed(rows))
    except Exception as e:
        log.warning("chat_store.recent_display 실패: %s", e)
        return []


def clear() -> None:
    try:
        with connect() as c:
            c.execute("DELETE FROM chat_log")
            c.commit()
    except Exception as e:
        log.warning("chat_store.clear 실패: %s", e)
