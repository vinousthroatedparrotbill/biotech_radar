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


def _kr_traded_today():
    """오늘 KRX가 실제 거래했나. True=개장 / False=휴장(주말·공휴일) / None=판정불가(→진행).
    독립 판정: 주말이면 휴장, 그 외엔 KOSPI 지수(KS11)의 마지막 거래일이 오늘인지로 확인.
    (휴장일엔 토스/스냅샷이 직전 종가를 '오늘'로 찍어 이틀 전 데이터가 올라오는 것 방지.)"""
    from datetime import timedelta
    d = date.today()
    if d.weekday() >= 5:          # 토(5)/일(6)
        return False
    try:
        import FinanceDataReader as fdr
        import pandas as pd
        df = fdr.DataReader("KS11", (d - timedelta(days=10)).isoformat())
        if df is None or df.empty:
            return None
        return pd.Timestamp(df.index[-1]).date() == d
    except Exception:
        return None


def main() -> int:
    _setup_logging()
    today = date.today().isoformat()
    if MARKER.exists():
        last = MARKER.read_text(encoding="utf-8").strip()
        if last == today:
            print(f"kr_daily_runner: already ran today ({today}). skip.")
            return 0

    # 한국장 휴장(주말/공휴일)이면 stale(직전 종가) 데이터 발송 방지 — 실제 거래일 아니면 skip.
    # 마커는 안 남긴다(FDR 지연에 의한 오판이면 catch-up 트리거가 재확인해 정상 발송).
    if _kr_traded_today() is False:
        print(f"kr_daily_runner: 오늘({today}) KRX 휴장 — 발송 skip (stale 데이터 방지).")
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
