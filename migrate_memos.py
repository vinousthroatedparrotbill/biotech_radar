"""기존 로컬 SQLite (data/app.db)의 메모를 Supabase Postgres로 일회성 이전.
실행: python migrate_memos.py
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

# Windows 콘솔 한글/특수문자 출력 호환
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

SQLITE_PATH = Path(__file__).parent / "data" / "app.db"


def main() -> None:
    if not SQLITE_PATH.exists():
        print(f"ℹ로컬 SQLite 파일 없음: {SQLITE_PATH}. 이전할 메모 없음.")
        return

    src = sqlite3.connect(SQLITE_PATH)
    src.row_factory = sqlite3.Row
    try:
        rows = src.execute(
            "SELECT ticker, body, created_at, updated_at "
            "FROM memos ORDER BY id"
        ).fetchall()
    except sqlite3.OperationalError as e:
        print(f"ℹSQLite memos 테이블 없음: {e}")
        return
    finally:
        src.close()

    if not rows:
        print("ℹSQLite에 메모 없음.")
        return

    from db import connect
    with connect() as conn:
        # 중복 방지 — 같은 (ticker, created_at, body) 이미 있으면 skip
        existing = {
            (r["ticker"], r["created_at"], r["body"])
            for r in conn.execute(
                "SELECT ticker, body, created_at FROM memos"
            ).fetchall()
        }
        new_rows = [
            (r["ticker"], r["body"], r["created_at"], r["updated_at"])
            for r in rows
            if (r["ticker"], r["created_at"], r["body"]) not in existing
        ]
        if not new_rows:
            print(f"ℹ모든 메모({len(rows)}건)가 이미 Postgres에 있음. skip.")
            return
        conn.executemany(
            "INSERT INTO memos (ticker, body, created_at, updated_at) "
            "VALUES (?,?,?,?)",
            new_rows,
        )
    print(f"✓ {len(new_rows)}건의 메모를 Supabase로 이전 완료 "
          f"(전체 {len(rows)}건 중 중복 {len(rows) - len(new_rows)}건 skip).")


if __name__ == "__main__":
    main()
