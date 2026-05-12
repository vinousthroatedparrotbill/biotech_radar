"""마크다운 → PDF 생성 — Playwright 헤드리스 렌더 사용 (이미 설치된 의존성 재활용).

흐름
1) markdown → HTML (markdown lib 또는 자체 변환)
2) 스타일링된 HTML 템플릿에 임베드
3) Playwright Chromium 렌더 → PDF 바이트 반환
"""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path

log = logging.getLogger(__name__)


_HTML_TEMPLATE = """<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<title>{title}</title>
<style>
  @page {{ size: A4; margin: 18mm 16mm 18mm 16mm; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Noto Sans KR',
                 'Malgun Gothic', sans-serif;
    color: #1a1a1a;
    line-height: 1.55;
    font-size: 10.5pt;
  }}
  h1 {{ font-size: 18pt; color: #0a3d3a; border-bottom: 2px solid #0a3d3a;
        padding-bottom: 6px; margin-bottom: 18px; }}
  h2 {{ font-size: 13.5pt; color: #134e4a; margin-top: 22px;
        margin-bottom: 8px; }}
  h3 {{ font-size: 11.5pt; color: #1a237e; margin-top: 16px; margin-bottom: 6px; }}
  p, li {{ font-size: 10.5pt; }}
  ul, ol {{ padding-left: 18px; margin: 6px 0; }}
  li {{ margin: 3px 0; }}
  strong, b {{ color: #0a3d3a; }}
  table {{ border-collapse: collapse; margin: 10px 0; font-size: 9.5pt;
           width: 100%; }}
  th, td {{ border: 1px solid #ccc; padding: 5px 8px; text-align: left;
            vertical-align: top; }}
  th {{ background: #f1f5f4; font-weight: 600; }}
  code {{ font-family: 'Menlo', 'Consolas', monospace; background: #f5f5f5;
          padding: 1px 4px; border-radius: 3px; font-size: 9.5pt; }}
  pre {{ background: #f8f8f8; border-left: 3px solid #134e4a;
         padding: 8px 10px; overflow-x: auto; font-size: 9pt;
         border-radius: 4px; }}
  hr {{ border: none; border-top: 1px solid #ddd; margin: 16px 0; }}
  .footer {{ margin-top: 40px; font-size: 9pt; color: #999;
             border-top: 1px solid #eee; padding-top: 8px; }}
</style>
</head>
<body>
{body}
<div class="footer">{footer}</div>
</body>
</html>
"""


def markdown_to_html(md: str) -> str:
    """markdown → HTML. python-markdown lib 우선, 없으면 간단 fallback."""
    try:
        import markdown as _md
        return _md.markdown(
            md,
            extensions=["tables", "fenced_code", "nl2br"],
        )
    except ImportError:
        return _simple_md_to_html(md)


def _simple_md_to_html(md: str) -> str:
    """fallback — 의존성 없이 기본 markdown 변환."""
    import re
    lines = md.split("\n")
    html_parts: list[str] = []
    in_list = False
    in_table = False
    in_pre = False
    for line in lines:
        if line.startswith("```"):
            if in_pre:
                html_parts.append("</pre>")
                in_pre = False
            else:
                html_parts.append("<pre>")
                in_pre = True
            continue
        if in_pre:
            html_parts.append(line.replace("<", "&lt;").replace(">", "&gt;"))
            continue
        # headers
        m = re.match(r"^(#{1,4})\s+(.+)$", line)
        if m:
            if in_list:
                html_parts.append("</ul>"); in_list = False
            level = len(m.group(1))
            html_parts.append(f"<h{level}>{_inline(m.group(2))}</h{level}>")
            continue
        # bullets
        m = re.match(r"^[\-\*]\s+(.+)$", line)
        if m:
            if not in_list:
                html_parts.append("<ul>"); in_list = True
            html_parts.append(f"<li>{_inline(m.group(1))}</li>")
            continue
        if in_list and not line.strip():
            html_parts.append("</ul>"); in_list = False
        # horizontal rule
        if re.match(r"^---+$", line.strip()):
            html_parts.append("<hr/>")
            continue
        # paragraph
        if line.strip():
            html_parts.append(f"<p>{_inline(line)}</p>")
        else:
            html_parts.append("")
    if in_list:
        html_parts.append("</ul>")
    if in_pre:
        html_parts.append("</pre>")
    return "\n".join(html_parts)


def _inline(text: str) -> str:
    import re
    t = text
    # bold
    t = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", t)
    # italic
    t = re.sub(r"(?<![\*\w])\*([^*\n]+)\*(?!\*)", r"<i>\1</i>", t)
    # inline code
    t = re.sub(r"`([^`]+)`", r"<code>\1</code>", t)
    return t


def _render_pdf_blocking(markdown_text: str, title: str, footer: str) -> bytes:
    """동기 Playwright 호출 — 반드시 worker thread에서 실행 (asyncio 루프 충돌 회피)."""
    from playwright.sync_api import sync_playwright
    html_body = markdown_to_html(markdown_text)
    html = _HTML_TEMPLATE.format(
        title=title.replace("<", "&lt;"),
        body=html_body,
        footer=footer.replace("<", "&lt;"),
    )
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = browser.new_page()
        page.set_content(html, wait_until="domcontentloaded")
        pdf_bytes = page.pdf(
            format="A4",
            margin={"top": "18mm", "right": "16mm",
                    "bottom": "18mm", "left": "16mm"},
            print_background=True,
        )
        browser.close()
    return pdf_bytes


def render_pdf(markdown_text: str, title: str = "Investment Memo",
               footer: str = "") -> bytes:
    """markdown → PDF 바이트. asyncio 이벤트 루프 안에서 호출돼도 안전하게
    별도 worker thread에서 sync Playwright 실행."""
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(_render_pdf_blocking, markdown_text, title, footer)
        return future.result()


def render_pdf_to_file(markdown_text: str, ticker: str,
                       title: str | None = None) -> str:
    """편의 함수: 임시 PDF 파일로 저장 후 경로 반환."""
    pdf = render_pdf(markdown_text, title=title or f"{ticker} Investment Memo",
                     footer=f"{ticker} · biotech_radar")
    tmp = tempfile.NamedTemporaryFile(
        prefix=f"memo_{ticker}_", suffix=".pdf", delete=False,
    )
    tmp.write(pdf)
    tmp.close()
    return tmp.name


if __name__ == "__main__":
    import sys
    sample = (sys.stdin.read() if not sys.stdin.isatty() else
              "# Sample\n\n## Heading\n\n- **bold** point\n- *italic* point")
    pdf = render_pdf(sample, title="Sample")
    out = Path("/tmp/test.pdf") if Path("/tmp").exists() else Path("test.pdf")
    out.write_bytes(pdf)
    print(f"wrote {out} ({len(pdf):,} bytes)")
