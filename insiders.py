"""인사이더 매매 (SEC Form 4) — OpenInsider 스크래핑.

OpenInsider URL 패턴
- 종목별: http://openinsider.com/screener?s=TICKER
- 결과: HTML 테이블 (Filing Date, Trade Date, Ticker, Company, Insider, Title,
  Trade Type, Price, Qty, Owned, ΔOwn, Value)

캐시: insider_trades 테이블 (UNIQUE: ticker+trade_date+insider+transaction+shares)
"""
from __future__ import annotations

import datetime as dt
import logging
import re

import pandas as pd

import db

log = logging.getLogger(__name__)


def fetch_insider_trades_for_ticker(ticker: str, days: int = 180) -> list[dict]:
    """OpenInsider에서 ticker의 최근 N일 Form 4 매매 가져오기."""
    from curl_cffi import requests as crq
    from bs4 import BeautifulSoup
    url = f"http://openinsider.com/screener?s={ticker.upper()}&fd={days}"
    try:
        r = crq.get(url, impersonate="chrome", timeout=15)
        r.raise_for_status()
    except Exception as e:
        log.warning("openinsider %s 실패: %s", ticker, e)
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", class_="tinytable")
    if not table:
        return []
    out: list[dict] = []
    tbody = table.find("tbody")
    for tr in (tbody.find_all("tr") if tbody else []):
        cells = tr.find_all("td")
        if len(cells) < 12:
            continue
        cols = [c.get_text(" ", strip=True) for c in cells]
        # OpenInsider screener 칼럼:
        # 0:X 1:FilingDate 2:TradeDate 3:Ticker 4:Insider 5:Title
        # 6:TradeType 7:Price 8:Qty 9:Owned 10:ΔOwn 11:Value
        try:
            filing = _iso_date(cols[1])
            trade = _iso_date(cols[2])
            ticker_v = cols[3].upper()
            insider = cols[4]
            title = cols[5]
            trade_type = cols[6]
            price = _num(cols[7])
            qty = _num(cols[8])
            owned = _num(cols[9])
            value = _num(cols[11])
        except Exception:
            continue
        if not filing or not trade:
            continue
        out.append({
            "ticker": ticker_v or ticker.upper(),
            "filing_date": filing,
            "trade_date": trade,
            "insider_name": insider[:200],
            "title": title[:100],
            "transaction": trade_type[:60],
            "price": price,
            "shares": qty,
            "value_usd": value,
            "shares_after": owned,
        })
    return out


def _iso_date(s: str) -> str:
    """OpenInsider date format: '2026-04-15 16:32:01' or '2026-04-15'."""
    if not s:
        return ""
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s.strip())
    return m.group(1) if m else ""


def _num(s: str) -> float | None:
    if not s:
        return None
    cleaned = s.replace("$", "").replace(",", "").replace("+", "").strip()
    if cleaned in ("", "-"):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def refresh_for_tickers(tickers: list[str], days: int = 180) -> int:
    """여러 ticker fetch + DB upsert."""
    now = dt.datetime.now().isoformat(timespec="seconds")
    total = 0
    with db.connect() as conn:
        for t in tickers:
            rows = fetch_insider_trades_for_ticker(t, days=days)
            for r in rows:
                try:
                    conn.execute(
                        "INSERT INTO insider_trades (ticker, filing_date, trade_date, "
                        "insider_name, title, transaction, shares, price, value_usd, "
                        "shares_after, fetched_at) VALUES (?,?,?,?,?,?,?,?,?,?,?) "
                        "ON CONFLICT (ticker, trade_date, insider_name, transaction, "
                        "shares) DO NOTHING",
                        (r["ticker"], r["filing_date"], r["trade_date"],
                         r["insider_name"], r["title"], r["transaction"],
                         r["shares"], r["price"], r["value_usd"], r["shares_after"], now),
                    )
                    total += 1
                except Exception as e:
                    log.debug("insider upsert 실패: %s", e)
    log.info("insider trades refreshed: %d rows across %d tickers", total, len(tickers))
    return total


def refresh_watchlist(days: int = 180) -> int:
    with db.connect() as conn:
        wl = conn.execute("SELECT ticker FROM watchlist").fetchall()
    tickers = [r["ticker"] for r in wl]
    if not tickers:
        return 0
    return refresh_for_tickers(tickers, days=days)


def get_insider_trades(ticker: str, days: int = 180) -> pd.DataFrame:
    """DB에서 조회. 캐시 부족하면 즉시 fetch."""
    cutoff = (dt.date.today() - dt.timedelta(days=days)).isoformat()
    df = db.pd_read_sql(
        "SELECT * FROM insider_trades WHERE ticker = ? AND trade_date >= ? "
        "ORDER BY trade_date DESC",
        params=(ticker.upper(), cutoff),
    )
    if df.empty:
        # lazy refresh
        refresh_for_tickers([ticker], days=days)
        df = db.pd_read_sql(
            "SELECT * FROM insider_trades WHERE ticker = ? AND trade_date >= ? "
            "ORDER BY trade_date DESC",
            params=(ticker.upper(), cutoff),
        )
    return df


def summary_for_ticker(ticker: str, days: int = 180) -> dict:
    """매매 요약 — buy/sell 건수, 합계 금액."""
    df = get_insider_trades(ticker, days=days)
    if df.empty:
        return {"ticker": ticker, "trades": 0, "net_value": 0,
                "buys": 0, "sells": 0}
    is_buy = df["transaction"].str.startswith("P", na=False)
    is_sell = df["transaction"].str.startswith("S", na=False)
    return {
        "ticker": ticker.upper(),
        "trades": int(len(df)),
        "buys": int(is_buy.sum()),
        "sells": int(is_sell.sum()),
        "buy_value": float(df.loc[is_buy, "value_usd"].sum() or 0),
        "sell_value": float(df.loc[is_sell, "value_usd"].sum() or 0),
        "net_value": float(
            (df.loc[is_buy, "value_usd"].sum() or 0)
            - (df.loc[is_sell, "value_usd"].sum() or 0)
        ),
        "last_trade_date": df.iloc[0]["trade_date"] if not df.empty else None,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # 빠른 테스트
    rows = fetch_insider_trades_for_ticker("RVMD", days=180)
    print(f"RVMD: {len(rows)} trades")
    for r in rows[:3]:
        print(r)
