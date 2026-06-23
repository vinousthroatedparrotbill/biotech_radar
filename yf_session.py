"""yfinance 공용 세션 + rate-limit 재시도 패치.

Yahoo Finance 레이트리밋(Too Many Requests / 429) 완화용.
- curl_cffi 브라우저 임퍼소네이션 세션을 yf.download / yf.Ticker 에 자동 주입
- yfinance 내부 요청 retries 상향 (.info / .history 등 lazy 호출까지 커버)
- bulk download 경로엔 추가 백오프 재시도

이 모듈을 import 하면 부수효과로 패치가 적용된다. yfinance 모듈 객체는
프로세스 내 싱글톤이므로 어느 진입점에서 1회 import 하면 `import yfinance as yf`
로 같은 객체를 참조하는 모든 호출부(prices/memo/perf/bot_tools/...)에 반영된다.
"""
from __future__ import annotations

import logging
import time

import yfinance as yf

log = logging.getLogger(__name__)

_SESSION = None          # curl_cffi 세션 싱글톤 (False = 생성 실패 sentinel)
_PATCHED = False
_RETRIES = 4
_BASE_DELAY = 2.0


def session():
    """curl_cffi chrome 임퍼소네이션 세션 (싱글톤). 실패 시 None."""
    global _SESSION
    if _SESSION is None:
        try:
            from curl_cffi import requests as _cffi
            _SESSION = _cffi.Session(impersonate="chrome")
        except Exception as e:
            log.warning("curl_cffi 세션 생성 실패 — 세션 없이 진행: %s", e)
            _SESSION = False
    return _SESSION or None


def _is_rate_limit(e: Exception) -> bool:
    name = type(e).__name__.lower()
    s = str(e).lower()
    return ("ratelimit" in name or "rate limit" in s
            or "too many requests" in s or "429" in s)


def _with_retry(fn, *args, **kwargs):
    last = None
    for attempt in range(_RETRIES):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last = e
            if not _is_rate_limit(e) or attempt == _RETRIES - 1:
                raise
            delay = _BASE_DELAY * (2 ** attempt)
            log.warning("yfinance rate-limited — %.0fs 후 재시도 (%d/%d)",
                        delay, attempt + 1, _RETRIES)
            time.sleep(delay)
    if last:
        raise last


def patch() -> None:
    """yf.download / yf.Ticker 에 세션 주입 + 재시도 래핑. idempotent."""
    global _PATCHED
    if _PATCHED:
        return
    _orig_download = yf.download
    _orig_ticker = yf.Ticker

    def download(*args, **kwargs):
        if "session" not in kwargs and session() is not None:
            kwargs["session"] = session()
        kwargs.setdefault("progress", False)
        return _with_retry(_orig_download, *args, **kwargs)

    def Ticker(*args, **kwargs):
        if "session" not in kwargs and session() is not None:
            kwargs["session"] = session()
        return _orig_ticker(*args, **kwargs)

    yf.download = download
    yf.Ticker = Ticker

    # 내부 요청 재시도 상향 — .info / .history 등 lazy 네트워크 호출까지 커버
    try:
        yf.config.network.retries = 3        # yfinance 1.3+ 신 config 경로
    except Exception:
        try:
            yf.set_config(retries=3)         # 구버전 fallback
        except Exception as e:
            log.debug("yfinance retries 설정 실패: %s", e)

    _PATCHED = True
    log.info("yfinance 패치 적용 — curl_cffi 세션 주입 + retries")


patch()
