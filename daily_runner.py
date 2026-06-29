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
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).parent
MARKER = ROOT / "data" / ".last_daily_run"
LOCK = ROOT / "data" / ".daily_run.lock"
sys.path.insert(0, str(ROOT))
import run_lock  # noqa: E402

# Windows cp949 콘솔에서 불릿(•) 등 유니코드 print 시 UnicodeEncodeError → exit 1 방지.
# (작업은 성공해도 마지막 print에서 죽으면 마커 미기록 → 재실행/중복 발송 위험)
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

    # 미국장은 주말 휴장 → 일/월(KST) 07시 발송은 직전 금요일 종가의 반복(= 토요일에
    # 보낸 것과 동일)이라 중복. 단 "토요일에 이미 돌았을 때만" 스킵한다 — 토요일에 PC가
    # 꺼져 발송을 놓쳤으면 일/월에라도 돌아 금요일 종가를 잡아야 하므로(catch-up).
    # 스냅샷도 안 쓰므로(스킵 시) 웹 보드/신규/상승이유는 금요일 자료를 그대로 유지.
    # 강제 발송: python telegram_report.py (가드 없는 직접 경로) 또는 --force
    today_d = date.today()
    wd = today_d.weekday()  # Mon=0 ... Sat=5, Sun=6
    if wd in (0, 6) and "--force" not in sys.argv:
        saturday = (today_d - timedelta(days=1 if wd == 6 else 2)).isoformat()
        last = MARKER.read_text(encoding="utf-8").strip() if MARKER.exists() else ""
        if last >= saturday:    # 이번 주말(토요일~)에 이미 발송함 → 금요일 종가 중복이라 스킵
            label = {0: "월요일", 6: "일요일"}[wd]
            print(f"daily_runner: {label}(KST) — 이번 주말 금요일 종가 이미 발송됨"
                  f"(last={last} ≥ 토 {saturday}). 중복이라 skip. (강제: --force)")
            return 0
        print(f"daily_runner: 주말이지만 토요일({saturday}) 발송 누락(last={last or '없음'}) "
              f"→ 금요일 종가 catch-up 실행.")

    today = today_d.isoformat()
    if MARKER.exists():
        last = MARKER.read_text(encoding="utf-8").strip()
        if last == today:
            print(f"daily_runner: already ran today ({today}). skip.")
            return 0

    # 중복 실행 방지 락 — 다른 런이 진행 중이면(긴 런 도중 다음 트리거가 떠도) skip
    if not run_lock.acquire(LOCK):
        print("daily_runner: 다른 daily_run이 진행 중 — skip.")
        return 0

    print(f"daily_runner: running daily_run for {today}...")
    from telegram_report import daily_run
    try:
        result = daily_run()
        print(f"daily_runner: complete — {result}")
        # 로컬(한국 IP) OHLCV 백필 → 클라우드 차트용 ohlcv_cache 갱신 (best-effort)
        try:
            import ohlcv_bridge as _ob
            print(f"daily_runner: ohlcv 백필 — {_ob.backfill_board()}")
        except Exception as _e:
            print(f"daily_runner: ohlcv 백필 실패(무시) — {_e}", file=sys.stderr)
        MARKER.parent.mkdir(parents=True, exist_ok=True)
        MARKER.write_text(today, encoding="utf-8")
        return 0
    except Exception as e:
        print(f"daily_runner: FAILED — {e}", file=sys.stderr)
        # 마커 업데이트 안 함 → 다음 트리거에서 재시도
        return 1
    finally:
        run_lock.release(LOCK)


if __name__ == "__main__":
    sys.exit(main())
