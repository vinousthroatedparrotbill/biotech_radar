"""Daily run guarded entry point — 같은 날 중복 실행 방지.

호출 시점:
1. 매일 7am 스케줄러 (PC 켜져 있을 때)
2. PC 부팅 후 첫 logon (catch-up)
3. 수동 (Start-ScheduledTask 또는 직접 실행)

로직:
- data/.last_daily_run 파일에 마지막 성공 날짜 (ISO YYYY-MM-DD) 저장
- 오늘 이미 실행했으면 skip
- 아니면 daily_run() 실행 후 마커 업데이트
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).parent
MARKER = ROOT / "data" / ".last_daily_run"


def _setup_logging() -> None:
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main() -> int:
    _setup_logging()
    today = date.today().isoformat()
    if MARKER.exists():
        last = MARKER.read_text(encoding="utf-8").strip()
        if last == today:
            print(f"daily_runner: already ran today ({today}). skip.")
            return 0

    print(f"daily_runner: running daily_run for {today}...")
    sys.path.insert(0, str(ROOT))
    from telegram_report import daily_run
    try:
        result = daily_run()
        print(f"daily_runner: complete — {result}")
        MARKER.parent.mkdir(parents=True, exist_ok=True)
        MARKER.write_text(today, encoding="utf-8")
        return 0
    except Exception as e:
        print(f"daily_runner: FAILED — {e}", file=sys.stderr)
        # 마커 업데이트 안 함 → 다음 트리거에서 재시도
        return 1


if __name__ == "__main__":
    sys.exit(main())
