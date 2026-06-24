"""한국 바이오·제약 전문 매체 뉴스 — RSS 통합 + 종목명 매칭.

소스(동일 CMS, /rss/allArticle.xml): 히트뉴스·팜뉴스·청년의사·더바이오(thebionews).
표준 라이브러리만 사용(requests + xml.etree). news.fetch_google_news와 상보 — 한국 전문 매체
헤드라인을 종목/자유주제로 끌어온다.

함수:
- latest(limit, days): 전체 매체 최신 기사 통합(최신순)
- for_query(query, limit, days): 제목/요약에 query(종목명·키워드) 포함 기사
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree as ET

import requests

log = logging.getLogger(__name__)

RSS_FEEDS = {
    "히트뉴스": "https://www.hitnews.co.kr/rss/allArticle.xml",
    "팜뉴스": "https://www.pharmnews.com/rss/allArticle.xml",
    "청년의사": "https://www.docdocdoc.co.kr/rss/allArticle.xml",
    "더바이오": "https://www.thebionews.net/rss/allArticle.xml",
}
_HDR = {"User-Agent": "Mozilla/5.0"}


_KST = timezone(timedelta(hours=9))


def _parse_date(s: str | None) -> datetime | None:
    if not s:
        return None
    s = s.strip()
    # 한국 매체 CMS 형식: "YYYY-MM-DD HH:MM:SS" (KST, naive)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=_KST)
        except ValueError:
            pass
    try:                                     # RFC822 fallback (혹시 모를 표준 피드)
        dt = parsedate_to_datetime(s)
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    except Exception:
        return None


def _fetch_feed(name: str, url: str) -> list[dict]:
    try:
        r = requests.get(url, headers=_HDR, timeout=15)
        r.raise_for_status()
        root = ET.fromstring(r.content)
    except Exception as e:
        log.warning("RSS 실패 %s: %s", name, e)
        return []
    out = []
    for item in root.iter("item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        desc = (item.findtext("description") or "").strip()
        pub = _parse_date(item.findtext("pubDate"))
        if not title or not link:
            continue
        out.append({"source": name, "title": title, "link": link,
                    "summary": desc[:300], "published": pub})
    return out


def _all_items(days: int) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    items: list[dict] = []
    for name, url in RSS_FEEDS.items():
        for it in _fetch_feed(name, url):
            if it["published"] is None or it["published"] >= cutoff:
                items.append(it)
    items.sort(key=lambda x: x["published"] or datetime.min.replace(tzinfo=timezone.utc),
               reverse=True)
    return items


def latest(limit: int = 30, days: int = 7) -> list[dict]:
    """전체 한국 바이오 매체 최신 기사 (최신순)."""
    return _all_items(days)[:limit]


def for_query(query: str, limit: int = 10, days: int = 30) -> list[dict]:
    """제목·요약에 query(종목명/키워드) 포함 기사. 공백 분리 토큰 중 하나라도 매칭."""
    q = (query or "").strip()
    if not q:
        return []
    tokens = [t for t in q.replace(",", " ").split() if len(t) >= 2]
    key = max(tokens, key=len) if tokens else q   # 식별력 높은 토큰(회사명 전체)
    out = []
    for it in _all_items(days):
        # 제목 매칭만 — 여러 종목 묶인 roundup 기사 오탐 방지(요약 매칭 제거)
        if q in it["title"] or key in it["title"]:
            out.append(it)
        if len(out) >= limit:
            break
    return out


import html as _html


def _clean(s: str) -> str:
    return _html.unescape(re.sub(r"<[^>]+>", "", s or "")).strip()


def naver_finance_news(code: str, limit: int = 15) -> list[dict]:
    """네이버 금융 종목별 뉴스 — **종목코드 기반**이라 이름 오탐 없음(가장 풍부·정확).
    [{source, title, link, summary, published}]."""
    code = str(code).strip()
    if not (code.isdigit() and len(code) == 6):
        return []
    url = (f"https://finance.naver.com/item/news_news.naver?code={code}"
           f"&page=1&sm=title_entity_id.basic")
    try:
        r = requests.get(url, headers={**_HDR, "Referer": "https://finance.naver.com/"},
                         timeout=15)
        r.encoding = "euc-kr"
        html = r.text
    except Exception as e:
        log.warning("네이버 종목뉴스 실패 %s: %s", code, e)
        return []
    out = []
    for row in re.split(r"<tr", html)[1:]:        # 행 단위 — 컬럼 순서/구조 변화에 견고
        mt = re.search(r'class="title">\s*<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>', row, re.S)
        if not mt:
            continue
        title = _clean(mt.group(2))
        if not title:
            continue
        href = mt.group(1)
        link = ("https://finance.naver.com" + href) if href.startswith("/") else href
        info = re.search(r'class="info"[^>]*>(.*?)</', row, re.S)
        date = re.search(r'class="date"[^>]*>(.*?)</', row, re.S)
        out.append({"source": _clean(info.group(1)) if info else "네이버",
                    "title": title, "link": link, "summary": "",
                    "published": _clean(date.group(1)) if date else ""})
        if len(out) >= limit:
            break
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    for it in latest(8):
        d = it["published"].strftime("%m-%d") if it["published"] else "??"
        print(f"[{it['source']} {d}] {it['title']}")
