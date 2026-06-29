"""Price trigger 체크 — 30분 cron + 부팅/로그온 트리거.

특징:
- daily_runner와 별개로 가볍게 (yfinance fetch만)
- PC 켜진 직후 즉시 미수신 트리거 catch-up
- 중복 발송 방지: status='active' → 'fired' 마킹으로 한 번만 발송
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).parent

try:  # Windows cp949 콘솔 유니코드 print 안전 (exit 1 방지)
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )
    sys.path.insert(0, str(ROOT))
    rc = 0
    try:
        from telegram_report import send_trigger_alerts
        n = send_trigger_alerts()
        print(f"triggers_runner: {n} price triggers fired")
    except Exception as e:
        print(f"triggers_runner price triggers FAILED: {e}", file=sys.stderr)
        rc = 1
    # 자동매매 조건 평가(충족 시 dry-run 발동) — 같은 30분 사이클에서
    try:
        import auto_trade
        print(f"triggers_runner: auto_trade {auto_trade.evaluate_all()}")
    except Exception as e:
        print(f"triggers_runner auto_trade FAILED: {e}", file=sys.stderr)
        rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
