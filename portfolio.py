"""모델 포트폴리오 — 펀드 관리 + 수익률 계산."""
from __future__ import annotations

from datetime import datetime

import yfinance as yf

from db import connect

DEFAULT_INITIAL_SIZE = 100_000_000.0   # $100M


# ───────────────────────── Portfolio CRUD ─────────────────────────
def create(name: str, initial_size: float = DEFAULT_INITIAL_SIZE) -> int:
    name = name.strip()
    if not name:
        raise ValueError("name required")
    with connect() as conn:
        cur = conn.execute(
            "INSERT INTO portfolios (name, initial_size, created_at) "
            "VALUES (?,?,?) RETURNING id",
            (name, initial_size, datetime.now().isoformat(timespec="seconds")),
        )
        return int(cur.fetchone()["id"])


def delete(portfolio_id: int) -> None:
    with connect() as conn:
        conn.execute("DELETE FROM portfolios WHERE id = ?", (portfolio_id,))


def rename(portfolio_id: int, new_name: str) -> None:
    with connect() as conn:
        conn.execute(
            "UPDATE portfolios SET name = ? WHERE id = ?",
            (new_name.strip(), portfolio_id),
        )


def list_all() -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT id, name, initial_size, created_at FROM portfolios "
            "ORDER BY created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def get(portfolio_id: int) -> dict | None:
    with connect() as conn:
        r = conn.execute(
            "SELECT id, name, initial_size, created_at FROM portfolios WHERE id = ?",
            (portfolio_id,),
        ).fetchone()
        return dict(r) if r else None


# ───────────────────────── Holding CRUD ─────────────────────────
def _stooq_last(ticker: str) -> float | None:
    """stooq 일별 CSV 마지막 종가 — 미국 티커(.us), 키 불필요·클라우드(미국 IP)에서 동작.
    yfinance가 차단돼 종목마다 타임아웃하는 것을 대체."""
    t = (ticker or "").strip().lower()
    if not t:
        return None
    sym = t if "." in t else f"{t}.us"
    try:
        import requests, io, csv
        r = requests.get(f"https://stooq.com/q/d/l/?s={sym}&i=d", timeout=8,
                         headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200 or not r.text or r.text.lstrip().startswith("<"):
            return None
        rows = list(csv.DictReader(io.StringIO(r.text)))
        if rows and rows[-1].get("Close"):
            return float(rows[-1]["Close"])
    except Exception:
        return None
    return None


def _fetch_current_price(ticker: str) -> float | None:
    """현재가 fetch — 토스 live → high_low_cache → stooq(미국) → yfinance(최후)."""
    try:
        import toss_market as tm
        if tm.available():
            p = tm.price(ticker)
            if p:
                return float(p)
    except Exception:
        pass
    with connect() as conn:
        r = conn.execute(
            "SELECT today_close FROM high_low_cache "
            "WHERE ticker = ? ORDER BY computed_date DESC LIMIT 1",
            (ticker,),
        ).fetchone()
        if r and r["today_close"]:
            return float(r["today_close"])
    _t = (ticker or "").strip()
    if _t and not (_t.isdigit() and len(_t) == 6):   # 미국 알파 티커 → stooq(클라우드 OK)
        sp = _stooq_last(_t)
        if sp:
            return sp
    try:
        info = yf.Ticker(ticker).fast_info
        for k in ("last_price", "lastPrice", "regularMarketPrice"):
            v = info.get(k) if hasattr(info, "get") else getattr(info, k, None)
            if v:
                return float(v)
    except Exception:
        pass
    return None


def _transactions(portfolio_id: int) -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT id, ticker, action, shares, price, amount, realized_pnl, "
            "trade_date, note FROM portfolio_transactions WHERE portfolio_id = ? "
            "ORDER BY trade_date, id",
            (portfolio_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def migrate_holdings_to_tx(portfolio_id: int) -> int:
    """레거시 portfolio_holdings → 편입가 기준 매수 거래로 시드 (거래내역 없을 때 1회).
    초기 사이즈·비중·편입가·현금을 그대로 보존."""
    with connect() as conn:
        n_tx = conn.execute(
            "SELECT count(*) AS n FROM portfolio_transactions WHERE portfolio_id = ?",
            (portfolio_id,),
        ).fetchone()["n"]
        if n_tx:
            return 0
        prow = conn.execute("SELECT initial_size FROM portfolios WHERE id = ?",
                            (portfolio_id,)).fetchone()
        initial = float(prow["initial_size"]) if prow else DEFAULT_INITIAL_SIZE
        hs = conn.execute(
            "SELECT ticker, weight_pct, entry_date, entry_price, note "
            "FROM portfolio_holdings WHERE portfolio_id = ? ORDER BY entry_date, id",
            (portfolio_id,),
        ).fetchall()
        seeded = 0
        for h in hs:
            entry = float(h["entry_price"] or 0)
            w = float(h["weight_pct"] or 0)
            if entry <= 0 or w <= 0:
                continue
            amount = initial * (w / 100.0)
            conn.execute(
                "INSERT INTO portfolio_transactions "
                "(portfolio_id, ticker, action, shares, price, amount, realized_pnl, "
                " trade_date, note) VALUES (?,?,?,?,?,?,?,?,?)",
                (portfolio_id, h["ticker"], "buy", amount / entry, entry, amount, 0.0,
                 h["entry_date"] or datetime.now().date().isoformat(), h["note"] or None),
            )
            seeded += 1
        conn.commit()
    return seeded


def _positions(portfolio_id: int) -> dict:
    """거래내역 → 종목별 포지션 (평균단가 회계).
    {ticker: {shares, avg_cost, realized_pnl, cost_basis, first_date}}"""
    migrate_holdings_to_tx(portfolio_id)
    pos: dict[str, dict] = {}
    for t in _transactions(portfolio_id):
        tk = t["ticker"]
        d = pos.setdefault(tk, {"shares": 0.0, "avg_cost": 0.0,
                                "realized_pnl": 0.0, "first_date": t["trade_date"]})
        sh, pr = float(t["shares"]), float(t["price"])
        if t["action"] == "buy":
            new_sh = d["shares"] + sh
            d["avg_cost"] = (d["avg_cost"] * d["shares"] + pr * sh) / new_sh if new_sh else 0.0
            d["shares"] = new_sh
        else:  # sell — 평단 유지, 실현손익 누적
            d["realized_pnl"] += sh * (pr - d["avg_cost"])
            d["shares"] = max(0.0, d["shares"] - sh)
    for d in pos.values():
        d["cost_basis"] = d["shares"] * d["avg_cost"]
    return pos


def _cash(portfolio_id: int, initial: float) -> float:
    """현금 = 초기 - 매수금액 + 매도금액 (현금 이자 0)."""
    cash = initial
    for t in _transactions(portfolio_id):
        cash += float(t["amount"]) if t["action"] == "sell" else -float(t["amount"])
    return cash


def _names_for(tickers: list[str]) -> dict:
    if not tickers:
        return {}
    with connect() as conn:
        rows = conn.execute(
            "SELECT ticker, name, industry FROM ticker_master WHERE ticker = ANY(%s)",
            (list(tickers),),
        ).fetchall()
    return {r["ticker"]: dict(r) for r in rows}


def _prices_for(tickers) -> dict:
    """여러 종목 현재가 일괄 — 토스 /prices 배치(1콜, 최대 200) 우선, 미수신분만 개별 fallback.
    (보유종목 1개씩 순차 호출하던 병목 제거 — MP 요약/모달 가속)."""
    tickers = [t for t in tickers if t]
    out: dict = {}
    if not tickers:
        return out
    try:
        import toss_market as tm
        if tm.available():
            out.update({k: v for k, v in tm.quote(list(tickers)).items() if v})
    except Exception:
        pass
    miss = [tk for tk in tickers if not out.get(tk)]   # 토스 미수신분 — 캐시/stooq/yf 병렬
    if miss:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(8, len(miss))) as ex:
            for tk, p in zip(miss, ex.map(_fetch_current_price, miss)):
                if p:
                    out[tk] = p
    return out


def _record_tx(portfolio_id, ticker, action, shares, price, realized_pnl, note):
    with connect() as conn:
        conn.execute(
            "INSERT INTO portfolio_transactions "
            "(portfolio_id, ticker, action, shares, price, amount, realized_pnl, "
            " trade_date, note) VALUES (?,?,?,?,?,?,?,?,?)",
            (portfolio_id, ticker, action, shares, price, shares * price,
             realized_pnl, datetime.now().date().isoformat(), note or None),
        )
        conn.commit()


def nav(portfolio_id: int) -> float:
    p = get(portfolio_id)
    if not p:
        return 0.0
    initial = float(p["initial_size"])
    pos = _positions(portfolio_id)
    active = [tk for tk, d in pos.items() if d["shares"] > 1e-9]
    pmap = _prices_for(active)
    mv = sum(pos[tk]["shares"] * (pmap.get(tk) or pos[tk]["avg_cost"]) for tk in active)
    return _cash(portfolio_id, initial) + mv


def set_target_weight(portfolio_id: int, ticker: str, target_weight_pct: float,
                      note: str = "") -> dict:
    """종목을 현재 NAV 대비 target_weight_pct%가 되도록 현재가로 체결(매수/매도).
    실현손익 확정 + 현금 반영. target=0 → 전량 매도. 시장가 체결은 NAV 중립."""
    ticker = ticker.strip().upper()
    if not ticker:
        raise ValueError("ticker required")
    if target_weight_pct < 0:
        raise ValueError("비중은 0 이상이어야 함")
    p = get(portfolio_id)
    if not p:
        raise ValueError("포트폴리오 없음")
    initial = float(p["initial_size"])
    pos = _positions(portfolio_id)
    cur = pos.get(ticker) or {"shares": 0.0, "avg_cost": 0.0, "realized_pnl": 0.0}
    price = _fetch_current_price(ticker)
    if not price:
        raise RuntimeError(f"{ticker}: 현재가 못 가져옴")
    cash = _cash(portfolio_id, initial)
    active = [tk for tk, d in pos.items() if d["shares"] > 1e-9]
    pmap = _prices_for(active)
    nav_now = cash + sum(pos[tk]["shares"] * (pmap.get(tk) or pos[tk]["avg_cost"])
                         for tk in active)
    cur_mv = cur["shares"] * price
    delta_mv = nav_now * (target_weight_pct / 100.0) - cur_mv
    if abs(delta_mv) < max(1.0, nav_now * 1e-6):
        return {"ok": True, "action": "noop", "ticker": ticker,
                "weight_pct": (cur_mv / nav_now * 100) if nav_now else 0.0}
    if delta_mv > 0:                       # 매수
        if delta_mv > cash + 1.0:
            raise RuntimeError(
                f"현금 부족: 필요 ${delta_mv/1e6:.2f}M · 가용 ${cash/1e6:.2f}M. "
                "다른 종목을 먼저 축소하세요.")
        shares = delta_mv / price
        _record_tx(portfolio_id, ticker, "buy", shares, price, 0.0, note)
        return {"ok": True, "action": "bought", "ticker": ticker, "price": price,
                "shares": shares, "amount_usd": delta_mv, "realized_pnl": 0.0,
                "target_weight_pct": target_weight_pct}
    sell_shares = min(cur["shares"], (-delta_mv) / price)   # 매도
    realized = sell_shares * (price - cur["avg_cost"])
    _record_tx(portfolio_id, ticker, "sell", sell_shares, price, realized, note)
    return {"ok": True, "action": "sold", "ticker": ticker, "price": price,
            "shares": sell_shares, "amount_usd": sell_shares * price,
            "realized_pnl": realized, "target_weight_pct": target_weight_pct}


def sell_all(portfolio_id: int, ticker: str, note: str = "") -> dict:
    return set_target_weight(portfolio_id, ticker, 0.0, note=note or "전량 매도")


def add_holding(portfolio_id: int, ticker: str, weight_pct: float,
                note: str = "") -> dict:
    """신규 편입 또는 목표 비중까지 조정 (현재 NAV 대비 weight_pct%)."""
    return set_target_weight(portfolio_id, ticker, weight_pct, note=note)


def transactions_log(portfolio_id: int, limit: int = 50) -> list[dict]:
    """최신 거래내역 (UI 표시용)."""
    txs = _transactions(portfolio_id)
    txs.sort(key=lambda t: (t["trade_date"], t["id"]), reverse=True)
    return txs[:limit]


def list_holdings(portfolio_id: int) -> list[dict]:
    """현재 보유 포지션 (거래내역 파생) + 회사명."""
    pos = _positions(portfolio_id)
    active = {tk: d for tk, d in pos.items() if d["shares"] > 1e-9}
    names = _names_for(list(active.keys()))
    out = []
    for tk, d in active.items():
        meta = names.get(tk) or {}
        out.append({
            "id": tk, "ticker": tk,
            "shares": d["shares"], "avg_cost": d["avg_cost"],
            "entry_price": d["avg_cost"], "entry_date": d["first_date"],
            "realized_pnl": d["realized_pnl"], "cost_basis": d["cost_basis"],
            "note": None, "name": meta.get("name"), "industry": meta.get("industry"),
        })
    out.sort(key=lambda h: h["cost_basis"], reverse=True)
    return out


# ───────────────────────── 수익률 / Summary ─────────────────────────
def summary(portfolio_id: int) -> dict:
    """포트폴리오 전체 요약 — 거래기반 평균단가 회계.
    holdings 각 항목: curr_price/weight_pct(현재 NAV 대비)/return_pct(미실현%)/
    amt_current(시가)/realized_pnl/unrealized_pnl. 상위: current_size(NAV)/return_pct
    /cash_amt/realized_pnl/unrealized_pnl/invested(원가)."""
    p = get(portfolio_id)
    if not p:
        return {}
    initial = float(p["initial_size"])
    pos = _positions(portfolio_id)
    cash = _cash(portfolio_id, initial)
    realized_total = sum(d["realized_pnl"] for d in pos.values())

    active = [tk for tk, d in pos.items() if d["shares"] > 1e-9]
    pmap = _prices_for(active)                 # 1콜 배치
    prices, mv_total = {}, 0.0
    for tk in active:
        d = pos[tk]
        prices[tk] = pmap.get(tk) or d["avg_cost"]
        mv_total += d["shares"] * prices[tk]
    nav_val = cash + mv_total
    names = _names_for(list(prices.keys()))

    enriched, unreal_total = [], 0.0
    for tk, d in pos.items():
        if d["shares"] <= 1e-9:
            continue
        curr = prices[tk]
        mv = d["shares"] * curr
        unreal = d["shares"] * (curr - d["avg_cost"])
        unreal_total += unreal
        meta = names.get(tk) or {}
        enriched.append({
            "id": tk, "ticker": tk,
            "name": meta.get("name"), "industry": meta.get("industry"),
            "shares": d["shares"], "avg_cost": d["avg_cost"],
            "entry_price": d["avg_cost"], "entry_date": d["first_date"],
            "curr_price": curr,
            "weight_pct": (mv / nav_val * 100) if nav_val else 0.0,
            "mv": mv, "amt_current": mv, "cost_basis": d["cost_basis"],
            "realized_pnl": d["realized_pnl"], "unrealized_pnl": unreal,
            "return_pct": ((curr / d["avg_cost"]) - 1) * 100 if d["avg_cost"] else 0.0,
            "note": None,
        })
    enriched.sort(key=lambda h: h["mv"], reverse=True)
    total_weight = sum(h["weight_pct"] for h in enriched)
    total_return = (nav_val / initial - 1) * 100 if initial else 0.0
    return {
        "portfolio": p,
        "holdings": enriched,
        "total_weight": total_weight,
        "cash_pct": (cash / nav_val * 100) if nav_val else 100.0,
        "cash_amt": cash,
        "current_size": nav_val,
        "return_pct": total_return,
        "realized_pnl": realized_total,
        "unrealized_pnl": unreal_total,
        "invested": sum(h["cost_basis"] for h in enriched),
    }
