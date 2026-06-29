"""데일리런 중복 실행 방지 락.

마커(.last_*_run)는 런이 '끝'에서만 기록돼서, 긴 런(투자메모·백필 포함 ~30분+) 도중
다음 스케줄 트리거(예: 16:30)가 '오늘 미실행'으로 보고 중복 실행하던 문제를 막는다.

락 = data/.<name>.lock 파일에 'PID 시작시각(epoch)'. 진행 중(살아있는 PID + 1시간 이내)이면
acquire가 False. 죽은 프로세스거나 STALE_SEC 초과면 stale로 보고 덮어쓴다(크래시 복구).
"""
from __future__ import annotations

import os
import time
from pathlib import Path

STALE_SEC = 60 * 60   # 1시간 넘으면 죽은 락으로 간주(런이 이보다 오래 걸리진 않음)


def _pid_alive(pid: int) -> bool:
    """Windows에서 해당 PID 프로세스가 살아있는지(보수적: 확인 불가 시 True)."""
    if pid <= 0:
        return False
    try:
        import ctypes
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        h = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if h:
            ctypes.windll.kernel32.CloseHandle(h)
            return True
        return False
    except Exception:
        return True   # 확인 불가 → 중복 방지 우선(살아있다고 간주)


def acquire(lock_path: Path) -> bool:
    """획득 성공 시 True, 다른 런이 진행 중이면 False."""
    now = time.time()
    if lock_path.exists():
        pid, started = -1, 0.0
        try:
            parts = lock_path.read_text(encoding="utf-8").split()
            pid, started = int(parts[0]), float(parts[1])
        except Exception:
            pass
        if (now - started) < STALE_SEC and _pid_alive(pid):
            return False   # 진행 중
        # stale(시간 초과 or 죽은 프로세스) → 덮어씀
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(f"{os.getpid()} {now}", encoding="utf-8")
    return True


def release(lock_path: Path) -> None:
    try:
        lock_path.unlink()
    except Exception:
        pass
