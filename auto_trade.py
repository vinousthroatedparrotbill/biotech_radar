"""자동매매(조건매매) 엔진.

조건(condition)은 유연한 JSON 트리:
  - {"kind":"price","op":">="|"<=","value":12.5}              # 현재가 도달(지정가)
  - {"kind":"return_pct","op":..,"value":20,"ref":"today"|"entry"}  # 수익률%
  - {"kind":"high_break"}                                      # 52주 신고가 돌파
  - {"kind":"date","date":"YYYY-MM-DD","window":"before"|"on"|"after","offset_days":0}
  - {"kind":"ir_readout","date":"YYYY-MM-DD","metric":"MADRS","op":">=","value":8,"unit":"점","hint":"..."}
  - {"kind":"all"|"any","of":[<node>, ...]}                    # 복합(AND/OR)

조건 충족 시 '발동(triggered)' — **실주문은 브로커 미연동이라 dry_run(알림만)**. 안전 게이트.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime

from db import connect

log = logging.getLogger(__name__)

CLAUDE_MODEL = "claude-opus-4-8"
_IR_MAX_ATTEMPTS = 8     # ir_readout 자료 미확보 시 재시도 상한(LLM 비용 캡)


# ───────────────────────── CRUD ─────────────────────────
def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def create(order: dict) -> int:
    """완성된 조건 주문 저장. order = {ticker,name,side,size_type,size_value,condition,title,
    portfolio_id?,note?}. status='armed'."""
    cond = order["condition"]
    cond_s = cond if isinstance(cond, str) else json.dumps(cond, ensure_ascii=False)
    now = _now()
    with connect() as c:
        row = c.execute(
            """INSERT INTO conditional_orders
               (portfolio_id, ticker, name, side, size_type, size_value, condition,
                title, status, note, created_at, armed_at, dry_run)
               VALUES (?,?,?,?,?,?,?,?,'armed',?,?,?,TRUE) RETURNING id""",
            (order.get("portfolio_id"), order["ticker"].strip(), order.get("name"),
             order["side"].lower().strip(), order["size_type"].strip(),
             float(order["size_value"]), cond_s, order["title"].strip(),
             order.get("note"), now, now),
        ).fetchone()
        c.commit()
        return row["id"]


def _row_to_dict(r: dict) -> dict:
    d = dict(r)
    for f in ("condition", "triggered_detail", "last_eval"):
        if d.get(f):
            try:
                d[f] = json.loads(d[f])
            except Exception:
                pass
    return d


def list_orders(status: str | None = None) -> list[dict]:
    with connect() as c:
        if status:
            rows = c.execute(
                "SELECT * FROM conditional_orders WHERE status=? ORDER BY id DESC",
                (status,)).fetchall()
        else:
            rows = c.execute(
                "SELECT * FROM conditional_orders ORDER BY "
                "CASE status WHEN 'triggered' THEN 0 WHEN 'armed' THEN 1 ELSE 2 END, id DESC"
            ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get(order_id: int) -> dict | None:
    with connect() as c:
        r = c.execute("SELECT * FROM conditional_orders WHERE id=?", (order_id,)).fetchone()
    return _row_to_dict(r) if r else None


def cancel(order_id: int) -> None:
    with connect() as c:
        c.execute("UPDATE conditional_orders SET status='cancelled' "
                  "WHERE id=? AND status IN ('armed','triggered')", (order_id,))
        c.commit()


def _save_eval(order_id: int, last_eval: dict) -> None:
    with connect() as c:
        c.execute("UPDATE conditional_orders SET last_eval=? WHERE id=?",
                  (json.dumps(last_eval, ensure_ascii=False), order_id))
        c.commit()


def _mark_triggered(order_id: int, detail: dict) -> bool:
    """armed→triggered 원자적 전이. 실제 전이된 경우만 True(로컬+클라우드 동시 평가 시
    한쪽만 알림 발송 → 중복 발동 방지)."""
    with connect() as c:
        row = c.execute(
            "UPDATE conditional_orders SET status='triggered', triggered_at=?, "
            "triggered_detail=? WHERE id=? AND status='armed' RETURNING id",
            (_now(), json.dumps(detail, ensure_ascii=False), order_id)).fetchone()
        c.commit()
        return row is not None


# ───────────────────────── 평가 컨텍스트 ─────────────────────────
def _ctx(ticker: str, portfolio_id=None) -> dict:
    """종목 현재 상태 — 현재가/52주고가/오늘고가/1일수익률/편입평단."""
    import portfolio as pf
    tk = (ticker or "").strip()
    ctx: dict = {"ticker": tk, "name": tk, "price": None}
    try:
        ctx["price"] = pf._fetch_current_price(tk)
    except Exception:
        pass
    try:
        with connect() as c:
            row = c.execute(
                "SELECT high_52w, today_high, today_close, perf_1d FROM high_low_cache "
                "WHERE ticker=? ORDER BY computed_date DESC LIMIT 1", (tk,)).fetchone()
            nm = c.execute("SELECT name FROM ticker_master WHERE ticker=?", (tk,)).fetchone()
        if row:
            ctx.update({k: row[k] for k in ("high_52w", "today_high", "today_close", "perf_1d")})
        if nm and nm.get("name"):
            ctx["name"] = nm["name"]
        if ctx.get("price") is None and ctx.get("today_close"):
            ctx["price"] = ctx["today_close"]
    except Exception as e:
        log.warning("ctx 조회 실패 %s: %s", tk, e)
    if portfolio_id:
        try:
            pos = pf._positions(portfolio_id).get(tk)
            if pos and pos.get("avg_cost"):
                ctx["entry"] = pos["avg_cost"]
        except Exception:
            pass
    return ctx


def _cmp(a: float, op: str, b: float) -> bool:
    return a >= b if op == ">=" else a <= b if op == "<=" else (
        a > b if op == ">" else a < b if op == "<" else abs(a - b) < 1e-9)


def _opsym(op: str) -> str:
    return {">=": "≥", "<=": "≤", ">": ">", "<": "<", "==": "="}.get(op, op)


# ───────────────────────── 조건 평가 ─────────────────────────
def evaluate(node: dict, ctx: dict, order: dict | None = None) -> dict:
    """{met, summary, attempts?}. order는 ir_readout 재시도 카운트 보존용."""
    kind = (node or {}).get("kind")

    if kind in ("all", "any"):
        subs = [evaluate(n, ctx, order) for n in node.get("of", [])]
        met = all(s["met"] for s in subs) if kind == "all" else any(s["met"] for s in subs)
        join = " 그리고 " if kind == "all" else " 또는 "
        return {"met": met, "summary": join.join(s["summary"] for s in subs)}

    if kind == "price":
        p, v, op = ctx.get("price"), node.get("value"), node.get("op", ">=")
        if p is None:
            return {"met": False, "summary": "현재가 미확보"}
        met = _cmp(p, op, v)
        gap = (abs(v - p) / p * 100) if p else 0
        tail = "도달 ✅" if met else f"남은 {gap:.1f}%"
        return {"met": met, "summary": f"현재가 {p:,.2f} {_opsym(op)} {v:,.2f} ({tail})"}

    if kind == "return_pct":
        ref, v, op = node.get("ref", "today"), node.get("value"), node.get("op", ">=")
        if ref == "entry" and ctx.get("entry") and ctx.get("price"):
            cur = (ctx["price"] / ctx["entry"] - 1) * 100
            label = "편입대비"
        else:
            cur = ctx.get("perf_1d")
            label = "당일"
        if cur is None:
            return {"met": False, "summary": "수익률 미확보"}
        met = _cmp(cur, op, v)
        return {"met": met, "summary": f"{label}수익률 {cur:+.1f}% {_opsym(op)} {v:+.1f}% "
                f"({'충족 ✅' if met else f'남은 {abs(v-cur):.1f}%p'})"}

    if kind == "high_break":
        p, hi = ctx.get("price"), ctx.get("high_52w")
        if p is None or not hi:
            return {"met": False, "summary": "52주 고가/현재가 미확보"}
        met = p >= hi * 0.999
        gap = (hi - p) / p * 100 if p else 0
        return {"met": met, "summary": f"52주 신고가 돌파 — 현재 {p:,.2f} / 52wH {hi:,.2f} "
                f"({'돌파 ✅' if met else f'남은 {gap:.1f}%'})"}

    if kind == "date":
        d = node.get("date", "")[:10]
        win, off = node.get("window", "on"), int(node.get("offset_days", 0) or 0)
        today = date.today().isoformat()
        from datetime import timedelta
        try:
            base = date.fromisoformat(d)
        except Exception:
            return {"met": False, "summary": f"날짜 파싱 실패 {d!r}"}
        if win == "before":
            target = (base - timedelta(days=off)).isoformat()
            met = today <= target
            return {"met": met, "summary": f"{target}까지(이벤트 {d} 전) — 오늘 {today} "
                    f"({'유효 ✅' if met else '기간 지남'})"}
        if win == "after":
            target = (base + timedelta(days=off)).isoformat()
            met = today >= target
            return {"met": met, "summary": f"{target} 이후(이벤트 {d} 후) ({'도달 ✅' if met else f'대기({today})'})"}
        met = today == d
        return {"met": met, "summary": f"이벤트 당일 {d} ({'당일 ✅' if met else f'대기({today})'})"}

    if kind == "ir_readout":
        return _eval_ir(node, ctx, order)

    return {"met": False, "summary": f"알 수 없는 조건 kind={kind!r}"}


def _eval_ir(node: dict, ctx: dict, order: dict | None) -> dict:
    """임상/IR 발표 판독형 — 이벤트일 도달 후 IR/뉴스/공시 본문에서 수치 추출 후 비교(LLM).
    자료 미확보면 재시도(상한 _IR_MAX_ATTEMPTS), 한 번 추출되면 캐시."""
    d = node.get("date", "")[:10]
    metric, op, v = node.get("metric", ""), node.get("op", ">="), node.get("value")
    today = date.today().isoformat()
    if d and today < d:
        return {"met": False, "summary": f"{metric} — 발표일({d}) 대기"}
    # 이전에 추출 성공했으면 재사용
    prev = (order or {}).get("last_eval") or {}
    cached = (prev.get("ir") or {})
    attempts = int(cached.get("attempts", 0))
    val = cached.get("value")
    if val is None and attempts < _IR_MAX_ATTEMPTS:
        ext = _extract_ir_metric(ctx["ticker"], node)
        attempts += 1
        val = ext.get("value")
        cached = {"attempts": attempts, "value": val, "evidence": ext.get("evidence"),
                  "found": ext.get("found")}
    if val is None:
        return {"met": False, "summary": f"{metric} 발표 판독 대기(자료 미확보, 시도 {attempts})",
                "ir": cached}
    met = _cmp(val, op, v)
    return {"met": met, "summary": f"{metric} {val} {_opsym(op)} {v} "
            f"({'충족 ✅' if met else '미충족'})", "ir": cached}


def _extract_ir_metric(ticker: str, node: dict) -> dict:
    """최근 뉴스/IR/DART 본문 → 해당 metric 수치 추출(LLM). {value, found, evidence}."""
    key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not key:
        return {"value": None, "found": False, "evidence": "ANTHROPIC_API_KEY 미설정"}
    # 자료 수집: 한국=DART+네이버, 공통=뉴스
    chunks: list[str] = []
    tk = ticker.strip()
    try:
        import bot_tools as bt
        if tk.isdigit() and len(tk) == 6:
            dz = bt.get_dart_disclosures(tk, days=10).get("disclosures", [])
            for d in dz[:3]:
                if d.get("rcept_no"):
                    doc = bt.get_dart_document(d["rcept_no"])
                    if doc.get("ok"):
                        chunks.append(f"[공시] {d.get('title','')}\n{doc['text'][:4000]}")
            for n in (bt.get_kr_news(ticker=tk, limit=8).get("news", []) or [])[:8]:
                chunks.append(f"[뉴스] {n.get('title','')}")
        else:
            for n in (bt.fetch_recent_news_for(tk, 8) or [])[:8]:
                t = n.get("title", "") if isinstance(n, dict) else str(n)
                chunks.append(f"[뉴스] {t}")
    except Exception as e:
        log.warning("ir 자료수집 실패 %s: %s", tk, e)
    if not chunks:
        return {"value": None, "found": False, "evidence": "자료 미수집"}
    import anthropic
    metric, hint = node.get("metric", ""), node.get("hint", "")
    prompt = (
        f"다음은 {tk}의 최근 공시/뉴스다. 발표된 '{metric}' 수치를 찾아라"
        + (f" (힌트: {hint})" if hint else "") + ".\n\n" + "\n\n".join(chunks)[:12000]
        + "\n\nJSON만 출력: {\"found\": true/false, \"value\": <숫자 또는 null>, "
        "\"evidence\": \"근거 한 줄\"}")
    try:
        cl = anthropic.Anthropic(api_key=key)
        r = cl.messages.create(model=CLAUDE_MODEL, max_tokens=600,
                               messages=[{"role": "user", "content": prompt}])
        txt = "".join(b.text for b in r.content if b.type == "text")
        import re
        m = re.search(r"\{.*\}", txt, re.S)
        j = json.loads(m.group(0)) if m else {}
        return {"value": j.get("value"), "found": bool(j.get("found")),
                "evidence": j.get("evidence")}
    except Exception as e:
        return {"value": None, "found": False, "evidence": f"추출 실패: {e}"}


# ───────────────────────── 실행(dry-run) + 모니터링 ─────────────────────────
def _place_order_dry_run(order: dict, eval_res: dict) -> None:
    """발동 시 처리 — **실주문 미발송**. 텔레그램 알림 + status='triggered'.
    브로커 연동 전까지 이 함수가 실제 주문을 내지 않는다(안전 게이트)."""
    side_kr = "매수" if order["side"] == "buy" else "매도"
    unit = {"weight_pct": "비중%", "amount": "금액", "shares": "주"}.get(order["size_type"], "")
    msg = (f"🔔 <b>자동매매 조건 발동</b> (dry-run · 실주문 미발송)\n"
           f"<b>{order.get('name') or order['ticker']} ({order['ticker']})</b>\n"
           f"• 주문: {side_kr} {order['size_value']:g} {unit}\n"
           f"• 조건: {order['title']}\n"
           f"• 평가: {eval_res.get('summary','')}\n"
           f"⚠️ 증권사 미연동 — 실제 주문은 나가지 않았습니다.")
    try:
        from telegram_report import send
        send(msg)
    except Exception as e:
        log.warning("발동 알림 실패: %s", e)


def evaluate_all() -> dict:
    """armed 주문 전부 평가 → 충족 시 발동(dry-run). triggers_runner/30분 cron에서 호출."""
    armed = list_orders("armed")
    fired = 0
    for o in armed:
        try:
            ctx = _ctx(o["ticker"], o.get("portfolio_id"))
            res = evaluate(o["condition"], ctx, o)
            _save_eval(o["id"], {"at": _now(), "summary": res["summary"],
                                 "price": ctx.get("price"),
                                 **({"ir": res["ir"]} if res.get("ir") else {})})
            if res["met"]:
                detail = {"summary": res["summary"], "price": ctx.get("price"), "at": _now()}
                if _mark_triggered(o["id"], detail):    # 원자적 전이 성공한 쪽만 알림
                    _place_order_dry_run(o, res)
                    fired += 1
        except Exception as e:
            log.warning("주문 평가 실패 id=%s: %s", o.get("id"), e)
    return {"evaluated": len(armed), "fired": fired}


# ───────────────────────── 챗 조건 빌더 ─────────────────────────
_BUILDER_TOOL = {
    "name": "propose_condition",
    "description": "사용자의 자동매매 의도를 안전성·정확도 확인 후 구조화한다. 모호하거나 "
                   "안전 확인이 필요하면 status='need_info'로 질문 하나만, 완전하면 status='complete'.",
    "input_schema": {
        "type": "object",
        "properties": {
            "status": {"type": "string", "enum": ["need_info", "complete"]},
            "question": {"type": "string", "description": "need_info일 때 사용자에게 물을 질문(하나)"},
            "order": {
                "type": "object",
                "description": "complete일 때만",
                "properties": {
                    "ticker": {"type": "string", "description": "미국 심볼 또는 한국 6자리 코드"},
                    "name": {"type": "string"},
                    "side": {"type": "string", "enum": ["buy", "sell"]},
                    "size_type": {"type": "string", "enum": ["weight_pct", "amount", "shares"]},
                    "size_value": {"type": "number"},
                    "condition": {"type": "object", "description": "조건 트리(kind: price/return_pct/"
                                  "high_break/date/ir_readout/all/any)"},
                    "title": {"type": "string", "description": "조건 요약 제목(카드 제목)"},
                    "safety_checks": {"type": "array", "items": {"type": "string"},
                                      "description": "확인한 안전 체크 목록"},
                },
                "required": ["ticker", "side", "size_type", "size_value", "condition", "title"],
            },
        },
        "required": ["status"],
    },
}

_BUILDER_SYS = (
    "너는 신중한 '자동매매 조건 빌더'다. 사용자가 자연어로 매매 조건을 말하면, 안전성과 정확도를 "
    "위해 반드시 확인이 필요한 것만 한 번에 하나씩 되묻고, 충분히 명확해지면 구조화된 주문을 완성한다.\n"
    "[확인 사항] ①종목 확정(한국 종목명은 6자리 코드로 변환, 모호하면 질문) ②매수/매도 방향 "
    "③수량 단위(비중%/금액/주식수)와 값 ④조건이 모호하거나 비현실적이지 않은지 ⑤방향과 조건의 "
    "논리가 맞는지(예: '떨어지면 매수'면 price <=).\n"
    "[조건 트리] price{op,value}, return_pct{op,value,ref:today|entry}, high_break, "
    "date{date,window:before|on|after,offset_days}, ir_readout{date,metric,op,value,hint}, "
    "all/any{of:[...]}. 복합은 all/any로 묶어라.\n"
    "[원칙] 실제 증권사 미연동(dry-run)이라 발동돼도 실주문은 안 나간다는 점을 알고 설계하되, "
    "조건 자체는 정확해야 한다. 답은 반드시 propose_condition 도구로만. 한국어로 질문/요약."
)


def build_condition(messages: list[dict]) -> dict:
    """대화(messages: [{role, content}]) → {status, question} 또는 {status, order}.
    프론트 챗 빌더가 호출."""
    key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not key:
        return {"status": "error", "error": "ANTHROPIC_API_KEY 미설정"}
    import anthropic
    cl = anthropic.Anthropic(api_key=key)
    msgs = [{"role": m["role"], "content": m["content"]} for m in (messages or [])
            if m.get("content")]
    if not msgs:
        return {"status": "error", "error": "메시지 없음"}
    try:
        r = cl.messages.create(
            model=CLAUDE_MODEL, max_tokens=2000, system=_BUILDER_SYS,
            tools=[_BUILDER_TOOL], tool_choice={"type": "tool", "name": "propose_condition"},
            messages=msgs)
        tu = next((b for b in r.content if b.type == "tool_use"), None)
        if not tu:
            return {"status": "error", "error": "도구 응답 없음"}
        out = tu.input
        # 한국 종목명 → 코드 보정
        if out.get("status") == "complete" and out.get("order"):
            try:
                import portfolio as pf
                o = out["order"]
                o["ticker"] = pf._resolve_ticker(o.get("ticker", ""))
            except Exception:
                pass
        return out
    except Exception as e:
        return {"status": "error", "error": f"{type(e).__name__}: {e}"}
