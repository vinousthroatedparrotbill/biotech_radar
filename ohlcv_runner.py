"""로컬(한국 IP) OHLCV 백필 러너 — 토스 일봉을 Supabase ohlcv_cache에 적재.
클라우드(해외 IP, 토스 차단)가 차트를 DB에서 읽도록. 스케줄/수동 실행.

실행: python ohlcv_runner.py
"""
from __future__ import annotations

import logging
import sys

try:  # Windows cp949 콘솔 유니코드 print 안전
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    import ohlcv_bridge as ob
    r = ob.backfill_board()
    print(f"ohlcv_runner: {r}")
    return 0 if r.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
