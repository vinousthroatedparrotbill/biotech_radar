"""한국 15:30(KST) 푸시 — 같은 날 중복 실행 방지 가드 (daily_runner.py의 KR 버전).

호출 시점:
1. 매일 15:35 스케줄러 (KRX 종가 15:30 직후)
2. catch-up 트리거 (16:30/18:00) + 로그온
3. 수동: Start-ScheduledTask -TaskName BiotechRadarKR / 또는 직접 실행

로직: data/.last_kr_run에 마지막 성공 날짜 저장 → 오늘 이미 실행했으면 skip.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).parent
MARKER = ROOT / "data" / ".last_kr_run"
LOCK = ROOT / "data" / ".kr_run.lock"
sys.path.insert(0, str(ROOT))
import run_lock  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


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
            print(f"kr_daily_runner: already ran today ({today}). skip.")
            return 0

    # 중복 실행 방지 락 — 긴 런 도중 다음 트리거(16:30/18:00)가 떠도 중복 실행 안 함
    if not run_lock.acquire(LOCK):
        print("kr_daily_runner: 다른 daily_run_kr이 진행 중 — skip.")
        return 0

    print(f"kr_daily_runner: running daily_run_kr for {today}...")
    from telegram_report import daily_run_kr
    try:
        result = daily_run_kr()
        print(f"kr_daily_runner: complete — {result}")
        # 클라우드 차트용 KR OHLCV 백필 (best-effort)
        try:
            import ohlcv_bridge as _ob
            print(f"kr_daily_runner: ohlcv 백필 — {_ob.backfill_board()}")
        except Exception as _e:
            print(f"kr_daily_runner: ohlcv 백필 실패(무시) — {_e}", file=sys.stderr)
        MARKER.parent.mkdir(parents=True, exist_ok=True)
        MARKER.write_text(today, encoding="utf-8")
        return 0
    except Exception as e:
        print(f"kr_daily_runner: FAILED — {e}", file=sys.stderr)
        return 1
    finally:
        run_lock.release(LOCK)


if __name__ == "__main__":
    sys.exit(main())
