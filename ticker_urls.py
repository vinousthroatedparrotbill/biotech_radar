"""Ticker → IR/Pipeline URL 매핑. JSON 파일 기반 (수동 편집 가능)."""
from __future__ import annotations

import json
from pathlib import Path

URLS_PATH = Path(__file__).parent / "data" / "ticker_urls.json"


def _load() -> dict[str, dict[str, str]]:
    if not URLS_PATH.exists():
        return {}
    try:
        return json.loads(URLS_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _save(d: dict[str, dict[str, str]]) -> None:
    URLS_PATH.parent.mkdir(parents=True, exist_ok=True)
    URLS_PATH.write_text(
        json.dumps(d, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def get(ticker: str) -> dict[str, str]:
    return _load().get(ticker, {})


def set_urls(ticker: str, ir_url: str | None = None, pipeline_url: str | None = None) -> None:
    d = _load()
    entry = d.get(ticker, {})
    if ir_url is not None:
        if ir_url.strip():
            entry["ir_url"] = ir_url.strip()
        else:
            entry.pop("ir_url", None)
    if pipeline_url is not None:
        if pipeline_url.strip():
            entry["pipeline_url"] = pipeline_url.strip()
        else:
            entry.pop("pipeline_url", None)
    if entry:
        d[ticker] = entry
    else:
        d.pop(ticker, None)
    _save(d)


def all_entries() -> dict[str, dict[str, str]]:
    return _load()
