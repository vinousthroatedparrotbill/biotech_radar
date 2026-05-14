"""가격 트리거 — 종목 가격이 임계값 돌파 시 텔레그램 알림.

사용 흐름
1. 사용자: "PHVS 32달러 돌파 알람" → 봇이 create_trigger() 호출
2. 30분마다 (또는 daily_run 시) check_all_triggers() 실행
3. yfinance 현재가 fetch → threshold 비교
4. 돌파 시 텔레그램 발송 + status='fired' 마킹 → 재발송 안 됨

표현
- direction='above' & price >= threshold → fire
- direction='below' & price <= threshold → fire
"""
from __future__ import annotations

import datetime as dt
import json
import logging

import db

log = logging.getLogger(__name__)


# ─────────────────────── CRUD ───────────────────────
def create(ticker: str, direction: str, threshold: float,
           note: str = "") -> int:
    """트리거 생성. direction='above' or 'below'."""
    direction = direction.lower().strip()
    if direction not in ("above", "below"):
        raise ValueError(f"direction must be 'above' or 'below', got {direction}")
    now = dt.datetime.now().isoformat(timespec="seconds")
    with db.connect() as conn:
        cur = conn.execute(
            "INSERT INTO price_triggers (ticker, direction, threshold, note, "
            "created_at, status) VALUES (?,?,?,?,?,'active') RETURNING id",
            (ticker.upper(), direction, float(threshold), note, now),
        )
        return int(cur.fetchone()["id"])


def cancel(trigger_id: int) -> None:
    with db.connect() as conn:
        conn.execute(
            "UPDATE price_triggers SET status='cancelled' WHERE id = ? "
            "AND status='active'",
            (trigger_id,),
        )


def list_all(status: str | None = "active") -> list[dict]:
    """status=None이면 전체, 'active' / 'fired' / 'cancelled'."""
    with db.connect() as conn:
        if status is None:
            rows = conn.execute(
                "SELECT * FROM price_triggers ORDER BY created_at DESC"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM price_triggers WHERE status = ? "
                "ORDER BY created_at DESC",
                (status,),
            ).fetchall()
    return [dict(r) for r in rows]


def list_for_ticker(ticker: str, status: str = "active") -> list[dict]:
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT * FROM price_triggers WHERE ticker = ? AND status = ? "
            "ORDER BY created_at DESC",
            (ticker.upper(), status),
        ).fetchall()
    return [dict(r) for r in rows]


def _mark_fired(trigger_id: int, price: float) -> None:
    now = dt.datetime.now().isoformat(timespec="seconds")
    with db.connect() as conn:
        conn.execute(
            "UPDATE price_triggers SET status='fired', "
            "triggered_at=?, triggered_price=? WHERE id = ?",
            (now, float(price), trigger_id),
        )


# ─────────────────────── 체크 ───────────────────────
def _fetch_current_prices(tickers: list[str]) -> dict[str, float]:
    """yfinance fast_info / info regularMarketPrice — 실시간성 보장."""
    import yfinance as yf
    out: dict[str, float] = {}
    for tk in set(tickers):
        try:
            t = yf.Ticker(tk)
            # 1) fast_info (low latency)
            try:
                p = float(t.fast_info["last_price"])
                if p > 0:
                    out[tk] = p
                    continue
            except Exception:
                pass
            # 2) info
            info = t.info or {}
            for k in ("regularMarketPrice", "currentPrice", "previousClose"):
                v = info.get(k)
                if v:
                    out[tk] = float(v)
                    break
        except Exception as e:
            log.debug("price fetch %s: %s", tk, e)
    return out


def _evaluate(trigger: dict, current_price: float) -> bool:
    """돌파 여부."""
    if trigger["direction"] == "above":
        return current_price >= trigger["threshold"]
    return current_price <= trigger["threshold"]


def check_all_triggers() -> list[dict]:
    """모든 active 트리거 체크. 발동된 것만 dict list로 반환 (caller가 발송)."""
    active = list_all(status="active")
    if not active:
        return []
    tickers = list({t["ticker"] for t in active})
    prices = _fetch_current_prices(tickers)
    fired: list[dict] = []
    for trig in active:
        tk = trig["ticker"]
        cur = prices.get(tk)
        if cur is None:
            log.debug("no price for %s — skip", tk)
            continue
        if _evaluate(trig, cur):
            _mark_fired(trig["id"], cur)
            trig["triggered_price"] = cur
            fired.append(trig)
    log.info("checked %d active triggers — %d fired", len(active), len(fired))
    return fired


# ─────────────────────── 보강 데이터 (알림 메시지용) ───────────────────────
def enrich_for_alert(ticker: str) -> dict:
    """발동 시 함께 보낼 컨텍스트 — 거래량 z-score, 최근 뉴스 3개, memo."""
    import yfinance as yf
    out: dict = {"ticker": ticker}
    # 거래량
    try:
        hist = yf.Ticker(ticker).history(period="35d", interval="1d")
        if not hist.empty:
            today_vol = float(hist["Volume"].iloc[-1])
            avg_30d = float(hist["Volume"].iloc[-31:-1].mean())
            std_30d = float(hist["Volume"].iloc[-31:-1].std() or 1.0)
            z = (today_vol - avg_30d) / std_30d if std_30d else 0.0
            out["volume_today"] = today_vol
            out["volume_30d_avg"] = avg_30d
            out["volume_zscore"] = z
            out["volume_ratio"] = today_vol / avg_30d if avg_30d else None
    except Exception as e:
        log.debug("volume %s: %s", ticker, e)
    # 최근 뉴스
    try:
        from news import fetch_finviz_news
        news = fetch_finviz_news(ticker, days=14)[:3]
        out["recent_news"] = [
            {"title": n.get("title", "")[:200], "link": n.get("link", ""),
             "source": n.get("source", ""), "published": n.get("published", "")}
            for n in news
        ]
    except Exception as e:
        log.debug("news %s: %s", ticker, e)
        out["recent_news"] = []
    # 메모
    try:
        from memo import list_for
        memos = list_for(ticker)[:2]
        out["memos"] = [{"body": m["body"], "created_at": m["created_at"]}
                        for m in memos]
    except Exception:
        out["memos"] = []
    return out


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cmd = sys.argv[1] if len(sys.argv) > 1 else "check"
    if cmd == "list":
        for t in list_all():
            print(f"  #{t['id']}  {t['ticker']:6} {t['direction']:5} "
                  f"${t['threshold']:.2f}  [{t['status']}]  {t.get('note','')[:60]}")
    elif cmd == "check":
        fired = check_all_triggers()
        print(f"fired: {len(fired)}")
        for t in fired:
            print(f"  #{t['id']} {t['ticker']} {t['direction']} ${t['threshold']:.2f}"
                  f" → ${t['triggered_price']:.2f}")
    elif cmd == "test":
        tid = create("PHVS", "above", 32.0, note="테스트 트리거")
        print(f"created trigger #{tid}")
