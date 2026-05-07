"""헤드리스 Chromium으로 JS 렌더링 IR 페이지에서 PDF/PPT 링크 추출.
Q4 API + 정적 스크랩이 모두 실패할 때 fallback."""
from __future__ import annotations

import re
from urllib.parse import urlparse

from ir_pdfs import _ASSET_EXTS, _date_hint


def _is_asset(url: str) -> tuple[bool, str]:
    u = url.lower()
    for ext in _ASSET_EXTS:
        if u.endswith(ext) or f"{ext}?" in u:
            return True, ext.lstrip(".")
    return False, ""


def fetch_via_browser(url: str, wait_ms: int = 6000, timeout_ms: int = 30000) -> list[dict]:
    """Chromium으로 페이지 로드 → JS 렌더 후 모든 a[href] PDF/PPT 추출.
    HTTP/2 비활성화 + 자동화 탐지 우회 플래그 + 스크롤로 lazy-load 트리거.
    Returns list of {url, title, date_hint, asset_type} or [{_error}]."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return [{"_error": "playwright 미설치 (pip install playwright + playwright install chromium)"}]

    out: list[dict] = []
    seen: set[str] = set()

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ],
            )
            ctx = browser.new_context(
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/131.0.0.0 Safari/537.36"),
                viewport={"width": 1366, "height": 900},
                java_script_enabled=True,
                ignore_https_errors=True,
            )
            page = ctx.new_page()

            # 네트워크 응답에서 PDF/PPT URL 캡처 (asset이 background fetch 되는 경우)
            def on_response(resp):
                u = resp.url
                ok, ext = _is_asset(u)
                if ok and u not in seen:
                    seen.add(u)
                    title = urlparse(u).path.split("/")[-1]
                    out.append({
                        "url": u, "title": title[:200],
                        "date_hint": _date_hint(title),
                        "asset_type": ext, "kind": "Browser-Net",
                    })
            page.on("response", on_response)

            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(wait_ms)

            # lazy-load 트리거 — 페이지 끝까지 스크롤
            try:
                page.evaluate("""async () => {
                    const sleep = ms => new Promise(r => setTimeout(r, ms));
                    let prev = 0;
                    for (let i = 0; i < 8; i++) {
                        window.scrollTo(0, document.body.scrollHeight);
                        await sleep(400);
                        if (document.body.scrollHeight === prev) break;
                        prev = document.body.scrollHeight;
                    }
                    window.scrollTo(0, 0);
                }""")
                page.wait_for_timeout(2000)
            except Exception:
                pass

            # 모든 anchor + 텍스트 추출
            links = page.eval_on_selector_all(
                "a[href]",
                """els => els.map(el => ({
                    href: el.href,
                    text: (el.innerText || '').trim() || el.title || ''
                }))""",
            )
            browser.close()
    except Exception as e:
        if not out:
            return [{"_error": f"{type(e).__name__}: {str(e)[:200]}"}]
        # 일부라도 캡처됐으면 반환
        return out

    for link in links:
        href = link.get("href") or ""
        is_asset, ext = _is_asset(href)
        if not is_asset:
            continue
        if href in seen:
            continue
        seen.add(href)
        title = link.get("text") or urlparse(href).path.split("/")[-1]
        out.append({
            "url": href,
            "title": title[:200],
            "date_hint": _date_hint(title),
            "asset_type": ext,
            "kind": "Browser",
        })
    if not out:
        return [{"_error": "헤드리스 렌더 후에도 PDF/PPT 링크 없음 — 클릭/모달 필요한 사이트 가능성"}]
    return out
