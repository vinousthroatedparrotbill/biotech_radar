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


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )
    sys.path.insert(0, str(ROOT))
    try:
        from telegram_report import send_trigger_alerts
        n = send_trigger_alerts()
        print(f"triggers_runner: {n} fired")
        return 0
    except Exception as e:
        print(f"triggers_runner FAILED: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
