import React, { useEffect, useRef, useState, useCallback, useMemo } from 'react'
import * as api from './api.js'
import { PriceChart, PerfChart } from './Chart.jsx'

/* 그린 플래그 — 8개월 내 2/3상 + mcap/peak_sales ≤ 5배(승률 높았던 패턴) 표식 */
function GreenFlag({ title }) {
  return (
    <span className="gflag" title={title}>
      <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true">
        <line x1="2.5" y1="1.2" x2="2.5" y2="11" stroke="#0a3d3a" strokeWidth="1.4" strokeLinecap="round" />
        <path d="M2.6 1.6 L10.2 3.7 L2.6 5.8 Z" fill="#3fae9b" />
      </svg>
    </span>
  )
}

/* 상승 이유 마크다운 → 티커별 분석 텍스트 맵. 헤더 예: **회사명 · TICKER $123 (+4%)** */
function parseReasonByTicker(md) {
  if (!md) return {}
  const map = {}
  let curTk = null, buf = []
  const flush = () => { if (curTk && buf.length) map[curTk] = buf.join('\n').trim() }
  for (const line of md.split('\n')) {
    const h = line.match(/^\s*\*\*(.+?)\*\*/)
    if (h) {
      flush(); buf = []
      const seg = h[1]
      const m = seg.match(/·\s*([A-Z]{1,6}|\d{6})\b/) || seg.match(/\b(\d{6}|[A-Z]{2,6})\b/)
      curTk = m ? m[1] : null
    }
    buf.push(line)
  }
  flush()
  return map
}

const isKR = (t) => /^\d{6}$/.test(String(t || ''))
const fmtPrice = (v, t) => v == null ? '—' : (isKR(t) ? `${Math.round(v).toLocaleString()}원` : `$${v.toFixed(2)}`)
const fmtMcap = (v, t) => {
  if (v == null) return '—'
  if (isKR(t)) { const krw = v * 1e6 * 1300; return krw >= 1e12 ? `${(krw / 1e12).toFixed(1)}조` : `${Math.round(krw / 1e8).toLocaleString()}억` }
  return v >= 1000 ? `$${(v / 1000).toFixed(1)}B` : `$${Math.round(v)}M`
}
const fmtUSD = (v) => v == null ? '—' : (Math.abs(v) >= 1e6 ? `$${(v / 1e6).toFixed(2)}M` : `$${Math.round(v).toLocaleString()}`)
const Pct = ({ v }) => v == null ? <span className="muted">—</span>
  : <span className={v > 0 ? 'pos' : v < 0 ? 'neg' : ''}>{v > 0 ? '+' : ''}{v.toFixed(1)}%</span>
// 미국(9:30–16:00 ET) 또는 한국(9:00–15:30 KST) 장중이면 true
function marketOpen() {
  const inWin = (tz, oh, om, ch, cm) => {
    const s = new Date(new Date().toLocaleString('en-US', { timeZone: tz }))
    if (s.getDay() === 0 || s.getDay() === 6) return false
    const m = s.getHours() * 60 + s.getMinutes()
    return m >= oh * 60 + om && m <= ch * 60 + cm
  }
  try { return inWin('America/New_York', 9, 30, 16, 0) || inWin('Asia/Seoul', 9, 0, 15, 30) } catch { return false }
}

const dateHint = (r) => {
  const d = r.description
  if (typeof d === 'string') { const m = d.match(/date_hint:\s*([^·]+?)(?:\s*·|$)/); if (m) return m[1].trim() }
  return (r.event_date || r.date || '').slice(0, 10)
}

const Dna = () => (
  <svg width="26" height="36" viewBox="0 0 30 42">
    <defs><linearGradient id="g" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stopColor="#3fae9b" /><stop offset="1" stopColor="#0a3d3a" /></linearGradient></defs>
    <g fill="none" stroke="url(#g)" strokeWidth="2.4" strokeLinecap="round">
      <path d="M6 3 Q24 11 6 21 Q24 31 6 39" /><path d="M24 3 Q6 11 24 21 Q6 31 24 39" /></g>
    <g stroke="#0a3d3a" strokeWidth="1.5" strokeLinecap="round" opacity="0.45">
      <line x1="10" y1="6" x2="20" y2="6" /><line x1="21" y1="13.5" x2="9" y2="13.5" />
      <line x1="10" y1="21" x2="20" y2="21" /><line x1="21" y1="28.5" x2="9" y2="28.5" />
      <line x1="10" y1="36" x2="20" y2="36" /></g>
  </svg>
)

const NAV = [
  { k: 'high', label: '52주 신고가', kind: 'board' },
  { k: 'movers', label: '상승폭', kind: 'board' },
  { k: 'news', label: '데일리 뉴스', kind: 'news' },
  { k: 'memos', label: '투자 메모', kind: 'memos' },
  { k: 'catalysts', label: '카탈리스트', kind: 'cat' },
  { k: 'portfolio', label: '포트폴리오', kind: 'pf' },
  { k: 'auto', label: '자동 매매', kind: 'auto' },
  { k: 'watchlist', label: '관심종목', kind: 'wl' },
]

export default function App() {
  const [country, setCountry] = useState('USA')
  const [page, setPage] = useState('high')
  const [modals, setModals] = useState([])   // 동시에 여러 종목 패널(병렬 비교, 상단 도킹)
  const [dockW, setDockW] = useState({})      // 패널별 너비(px) — 티커별
  const [tickerMap, setTickerMap] = useState({})
  const meta = NAV.find(n => n.k === page)

  useEffect(() => { api.getTickers().then(d => setTickerMap(d.map || {})).catch(() => { }) }, [])

  // 패널 열기(중복 티커는 무시), 닫기, 전체 닫기
  const openModal = useCallback((r) => {
    if (!r || !r.ticker) return
    setModals(ms => ms.some(x => x.ticker === r.ticker) ? ms : [...ms, r])
  }, [])
  const closeModal = useCallback((tk) => setModals(ms => ms.filter(x => x.ticker !== tk)), [])
  const closeAll = useCallback(() => setModals([]), [])

  return (
    <>
      <header className="topbar">
        <div className="brand">
          <Dna />
          <div>
            <div className="name wordmark">BioTech&nbsp;Radar</div>
            <div className="tag">Biotech Trading · Analysis Intelligence</div>
          </div>
        </div>
        <nav className="nav">
          {NAV.map(n => (
            <button key={n.k} className={page === n.k ? 'active' : ''} onClick={() => setPage(n.k)}>{n.label}</button>
          ))}
        </nav>
        <div className="seg">
          {[['USA', '해외'], ['KOR', '한국']].map(([k, l]) =>
            <button key={k} className={country === k ? 'active' : ''} onClick={() => setCountry(k)}>{l}</button>)}
        </div>
      </header>

      <div className="wrap">
        {/* 보드 외 페이지: 플로팅 도킹 패널(폴백). 보드 페이지에선 가운데 리스트 칸 위 오버레이로 Board가 직접 렌더 */}
        {meta.kind !== 'board' && modals.length > 0 && (
          <div className="dock-strip">
            <div className="dock-bar">
              <span className="muted small">{modals.length}개 패널 · 오른쪽 경계 드래그로 너비 조절</span>
              {modals.length > 1 && <button className="btn ghost sm" onClick={closeAll}>모두 닫기 ✕</button>}
            </div>
            <div className="dock-row">
              {modals.map(m => (
                <DockPanel key={m.ticker} row={m} width={dockW[m.ticker] || 'var(--list-width, 480px)'}
                  onResizeDelta={dx => setDockW(s => ({ ...s, [m.ticker]: clampW((s[m.ticker] || curListW()) + dx, 320, 900) }))}
                  onClose={() => closeModal(m.ticker)} onPick={openModal} tickerMap={tickerMap} />
              ))}
            </div>
          </div>
        )}
        <WatchedBanner onPick={openModal} />
        {meta.kind === 'board' && <Board country={country} view={page} onPick={openModal} tickerMap={tickerMap}
          modals={modals} onCloseModal={closeModal} onCloseAll={closeAll} />}
        {meta.kind === 'news' && <DailyNews country={country} />}
        {meta.kind === 'memos' && <Memos onPick={openModal} />}
        {meta.kind === 'cat' && <Catalysts onPick={openModal} />}
        {meta.kind === 'pf' && <Portfolios onPick={openModal} />}
        {meta.kind === 'auto' && <AutoTrade onPick={openModal} />}
        {meta.kind === 'wl' && <Watchlist onPick={openModal} />}
      </div>

      <Chat />
      <OpsWidget country={country} />
    </>
  )
}

/* 가로 리사이즈 핸들 — onDrag(deltaX) */
function Splitter({ onDrag }) {
  const last = useRef(0)
  const down = (e) => {
    e.preventDefault()
    last.current = e.clientX
    const move = (ev) => { const dx = ev.clientX - last.current; last.current = ev.clientX; onDrag(dx) }
    const up = () => { document.body.style.cursor = ''; window.removeEventListener('mousemove', move); window.removeEventListener('mouseup', up) }
    document.body.style.cursor = 'col-resize'
    window.addEventListener('mousemove', move); window.addEventListener('mouseup', up)
  }
  return <div className="splitter" onMouseDown={down} title="드래그하여 너비 조절" />
}

const clampW = (w, min, max) => Math.max(min, Math.min(max, w))
// 측정된 리스트 너비(--list-width)를 px 숫자로 — 패널 첫 드래그 시작값(미측정 시 480 폴백)
const curListW = () => parseFloat(getComputedStyle(document.documentElement).getPropertyValue('--list-width')) || 480
const PERF_COLS = [['perf_1d', '1D'], ['perf_7d', '1W'], ['perf_1m', '1M'], ['perf_3m', '3M'], ['perf_6m', '6M'], ['perf_1y', '1Y']]

/* ───────────── 보드 3분할 (좌: 상승이유 · 중: 티커 리스트 · 우: 인라인 상세) — 가로 리사이즈 ───────────── */
function Board({ country, view, onPick, tickerMap, modals = [], onCloseModal, onCloseAll }) {
  const isHigh = view === 'high'
  const [sub, setSub] = useState('new')   // 신고가 전용: new(오늘 신규) | all(전체)
  const [rows, setRows] = useState([])
  const [loading, setLoading] = useState(false)
  const [reason, setReason] = useState(null)
  const [reasonBusy, setReasonBusy] = useState(false)
  const [picked, setPicked] = useState(null)
  const [leftW, setLeftW] = useState(250)    // 상승이유 너비(px)
  const [rightW, setRightW] = useState(580)  // 상세 너비(px)
  const listRef = useRef(null)               // 가운데 '리스트' 컬럼 — 도킹 패널 정렬 기준

  // 가운데 리스트 컬럼의 실제 위치/너비를 측정 → CSS 변수로 노출(도킹 패널 정렬·기본 너비)
  useEffect(() => {
    const el = listRef.current
    if (!el) return
    const measure = () => {
      const r = el.getBoundingClientRect()
      const root = document.documentElement.style
      root.setProperty('--list-left', r.left + 'px')
      root.setProperty('--list-width', r.width + 'px')
    }
    measure()
    const ro = new ResizeObserver(measure)
    ro.observe(el)
    window.addEventListener('resize', measure)
    return () => { ro.disconnect(); window.removeEventListener('resize', measure) }
  }, [leftW, rightW])

  const apiView = isHigh ? (sub === 'new' ? 'new' : 'high') : 'movers'
  const reasonKind = view === 'movers' ? 'movers' : 'high'
  useEffect(() => {
    setLoading(true); setReason(null); setPicked(null)
    api.getBoard(country, apiView)
      .then(d => setRows(d.rows || [])).catch(() => setRows([])).finally(() => setLoading(false))
  }, [country, apiView])

  // 보드 로드 시: ①진행 중 분석 있으면 다시 붙기(탭 전환에도 유지) ②없으면 오늘자 캐시 자동 표시
  useEffect(() => {
    if (!rows.length) return
    const sub100 = rows.slice(0, 100)
    const pending = api.pendingReason(reasonKind, country)
    if (pending) {
      setReasonBusy(true)
      pending.then(md => setReason(md)).catch(e => setReason('⚠️ ' + e)).finally(() => setReasonBusy(false))
      return
    }
    api.postReason(reasonKind, sub100, false, country)
      .then(d => { if (d.markdown) setReason(d.markdown) }).catch(() => { })
  }, [rows, reasonKind, country])

  const loadReason = () => {
    if (reasonBusy || !rows.length) return
    setReasonBusy(true)
    // 버튼(생성/재생성)은 항상 강제 재생성(force) — 캐시가 있어도 새로 분석
    api.runReason(reasonKind, rows.slice(0, 100), country, true)
      .then(md => setReason(md)).catch(e => setReason('⚠️ ' + e)).finally(() => setReasonBusy(false))
  }

  // 티커별 이유 맵 + 호버 박스
  const reasonMap = useMemo(() => parseReasonByTicker(reason), [reason])
  const [pop, setPop] = useState(null)   // { html, x, y }
  const showPop = (r, e) => { const t = reasonMap[r.ticker]; if (t) setPop({ html: api.mdToHtml(t), x: e.clientX, y: e.clientY }) }

  return (
    <div className="tri">
      {/* 좌: 상승 이유 */}
      <div className="tri-reason" style={{ width: leftW }}>
        <div className="reason-head">
          <h3 className="wordmark" style={{ margin: 0 }}>상승 이유</h3>
          <button className="btn ghost sm" onClick={loadReason} disabled={reasonBusy || !rows.length}>
            {reasonBusy ? '분석…' : reason ? '재생성' : '생성'}</button>
        </div>
        {reasonBusy && !reason && <p className="muted small">분석 중…<br />(종목 수에 따라 1–3분)</p>}
        {reason ? <div className="md reason-md" dangerouslySetInnerHTML={{ __html: api.mdToHtml(reason) }} />
          : !reasonBusy && <p className="muted small">‘생성’ → 신고가/급등 이유를 분석합니다.</p>}
      </div>

      <Splitter onDrag={dx => setLeftW(w => clampW(w + dx, 150, 480))} />

      {/* 중: 티커 리스트 (넓히면 perf 컬럼 전체 노출) */}
      <div className="tri-list" ref={listRef}>
        <div className="board-head">
          <h2 className="wordmark sec" style={{ margin: 0 }}>{isHigh ? '52주 신고가' : '상승폭'}</h2>
          {isHigh && (
            <div className="seg sm">
              {[['new', '오늘 신규'], ['all', '전체']].map(([k, l]) =>
                <button key={k} className={sub === k ? 'active' : ''} onClick={() => setSub(k)}>{l}</button>)}
            </div>
          )}
        </div>
        <p className="muted small" style={{ margin: '0.2rem 0 0.4rem' }}>
          {loading ? '불러오는 중…' : `${rows.length}종목 · 클릭 → 우측 상세 · 경계 드래그로 너비 조절`}
        </p>
        <div className="list-scroll">
          <table className="board-tbl">
            <thead><tr>
              <th className="l">회사명</th><th>현재가</th>
              {PERF_COLS.map(([k, l]) => <th key={k}>{l}</th>)}<th>시총</th>
            </tr></thead>
            <tbody>
              {rows.map((r, i) => {
                const hasReason = !!reasonMap[r.ticker]
                return (
                  <tr key={r.ticker + i}
                    className={(picked?.ticker === r.ticker ? 'on' : '') + (hasReason ? ' has-reason' : '')}
                    onClick={() => setPicked(r)}
                    onMouseEnter={e => showPop(r, e)}
                    onMouseMove={e => hasReason && setPop(p => p ? { ...p, x: e.clientX, y: e.clientY } : p)}
                    onMouseLeave={() => setPop(null)}>
                    <td className="l"><span className="pick-name">{r.name || r.ticker}</span>{r.green && <GreenFlag title={r.green_note || '8개월 내 2/3상 · mcap/peak_sales ≤ 5배 (승률 높았던 패턴)'} />}<span className="pick-tk muted">{r.ticker}</span></td>
                    <td>{fmtPrice(r.close, r.ticker)}</td>
                    {PERF_COLS.map(([k]) => <td key={k}><Pct v={r[k]} /></td>)}
                    <td>{fmtMcap(r.market_cap, r.ticker)}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>

        {/* 종목 모달 — 가운데 신고가 리스트 칸 위에 같은 크기로 오버레이. 좌/우 삼분할 splitter가
            그대로 리사이즈 → 상승이유(좌)·원래 상세(우)가 유동적으로 함께 밀림 */}
        {modals.length > 0 && (
          <div className="mid-overlay">
            {/* 모달 좌·우 모서리 리사이즈 핸들 — 드래그하면 상승이유(좌)·원래 상세(우)가 함께 유동적으로 움직임 */}
            <div className="ov-edge ov-edge-l" title="왼쪽 경계 — 상승이유와 함께 너비 조절">
              <Splitter onDrag={dx => setLeftW(w => clampW(w + dx, 150, 480))} />
            </div>
            <div className="ov-edge ov-edge-r" title="오른쪽 경계 — 상세와 함께 너비 조절">
              <Splitter onDrag={dx => setRightW(w => clampW(w - dx, 340, 1000))} />
            </div>
            <div className="dock-bar">
              <span className="muted small">{modals.length}개 패널 · 좌·우 모서리를 드래그하면 상승이유·상세와 함께 너비 조절</span>
              {modals.length > 1 && <button className="btn ghost sm" onClick={onCloseAll}>모두 닫기 ✕</button>}
            </div>
            <div className="dock-row">
              {modals.map(m => (
                <DockPanel key={m.ticker} row={m} onClose={() => onCloseModal(m.ticker)} onPick={onPick} tickerMap={tickerMap} />
              ))}
            </div>
          </div>
        )}
      </div>

      <Splitter onDrag={dx => setRightW(w => clampW(w - dx, 340, 1000))} />

      {/* 우: 인라인 상세 (클릭 시) — 중간을 넓히면 거꾸로 줄어듦 */}
      <div className="tri-detail" style={{ width: rightW }}>
        {picked
          ? <div className="detail-inline" key={picked.ticker}><StockDetail row={picked} onClose={() => setPicked(null)} onPick={onPick} tickerMap={tickerMap} /></div>
          : <div className="detail-empty muted">← 종목을 클릭하면 여기에 상세가 표시됩니다.</div>}
      </div>

      {/* 티커 호버 → 상승 이유 박스 */}
      {pop && <div className="reason-pop md"
        style={{ left: Math.min(pop.x + 18, window.innerWidth - 400), top: Math.min(pop.y + 14, window.innerHeight - 300) }}
        dangerouslySetInnerHTML={{ __html: pop.html }} />}
    </div>
  )
}

/* ───────────── 데일리 뉴스 ───────────── */
function DailyNews({ country }) {
  const [items, setItems] = useState([])
  const [loading, setLoading] = useState(false)
  useEffect(() => {
    setLoading(true)
    api.getDailyNews(country, 1).then(d => setItems(d.items || [])).catch(() => setItems([])).finally(() => setLoading(false))
  }, [country])
  return (
    <>
      <h2 className="wordmark sec">데일리 헬스케어 뉴스</h2>
      {loading ? <p className="muted">불러오는 중…</p> : items.length === 0 ? <p className="muted">뉴스 없음</p> :
        items.map((n, i) => (
          <a key={i} className="news-item" href={n.url || n.link || '#'} target="_blank" rel="noreferrer">
            <div className="news-title">{n.title || n.headline}</div>
            <div className="news-meta">{n.source || n.publisher || ''} {n.published || n.date || n.time || ''}</div>
            {n.summary && <div className="news-sum">{n.summary}</div>}
          </a>
        ))}
    </>
  )
}

/* ───────────── 투자 메모 타임라인 ───────────── */
function Memos({ onPick }) {
  const [memos, setMemos] = useState([])
  const [loading, setLoading] = useState(false)
  useEffect(() => {
    setLoading(true)
    api.getTimeline(100).then(d => setMemos(d.memos || [])).catch(() => setMemos([])).finally(() => setLoading(false))
  }, [])
  return (
    <>
      <h2 className="wordmark sec">투자 메모</h2>
      {loading ? <p className="muted">불러오는 중…</p> : memos.length === 0 ? <p className="muted">메모 없음 · 종목 상세에서 작성</p> :
        <>
          <p className="muted" style={{ margin: '0 0 0.6rem' }}>{memos.length}건 · 작성 후 주가 변동</p>
          <table>
            <thead><tr><th className="l">작성일</th><th className="l">종목</th><th className="l">메모</th><th>작성가</th><th>현재가</th><th>변동</th></tr></thead>
            <tbody>
              {memos.map(m => (
                <tr key={m.id}>
                  <td className="l muted">{(m.created_at || '').slice(0, 10)}</td>
                  <td className="l"><button className="tk" onClick={() => onPick({ ticker: m.ticker, name: m.name })}>{m.name || m.ticker}</button></td>
                  <td className="l memo-cell">{m.body}</td>
                  <td>{fmtPrice(m.price_at_create, m.ticker)}</td>
                  <td>{fmtPrice(m.price_now, m.ticker)}</td>
                  <td><Pct v={m.change_pct} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </>}
    </>
  )
}

/* ───────────── 카탈리스트 캘린더 (필터 + 액션) ───────────── */
const CAT_TYPES = [
  ['', '전체'], ['pdufa', 'PDUFA'], ['conference', '학회'], ['earnings', '어닝'],
  ['clinical_readout', '임상 데이터'], ['clinical_milestone', '임상 마일스톤'],
  ['regulatory', 'FDA 규제'], ['company_event', '회사 공개'],
]
function Catalysts({ onPick }) {
  const [days, setDays] = useState(90)
  const [type, setType] = useState('')
  const [scope, setScope] = useState('all')
  const [rows, setRows] = useState([])
  const [loading, setLoading] = useState(false)
  const [busy, setBusy] = useState(false)
  const load = useCallback(() => {
    setLoading(true)
    api.getCatalysts(days, type, scope).then(d => setRows(d.rows || [])).catch(() => setRows([])).finally(() => setLoading(false))
  }, [days, type, scope])
  useEffect(() => { load() }, [load])

  const refresh = async () => {
    setBusy(true)
    try { await api.catRefresh(scope === 'watchlist' ? 'watchlist' : 'biotech_1b') } catch { }
    setBusy(false); load()
  }
  const toggle = (r, field) => {
    const v = !r[field]
    setRows(rs => rs.map(x => x.id === r.id ? { ...x, [field]: v } : x))
      ; (field === 'watched' ? api.catWatch : api.catAck)(r.id, v).catch(() => { })
  }

  return (
    <>
      <h2 className="wordmark sec">카탈리스트 캘린더</h2>
      <div className="filters">
        <select value={days} onChange={e => setDays(+e.target.value)}>
          {[14, 30, 60, 90, 180, 365].map(d => <option key={d} value={d}>{d}일</option>)}
        </select>
        <select value={type} onChange={e => setType(e.target.value)}>
          {CAT_TYPES.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
        </select>
        <select value={scope} onChange={e => setScope(e.target.value)}>
          <option value="all">전체</option>
          <option value="biotech_1b">≥$1B 바이오텍</option>
          <option value="watchlist">관심종목만</option>
        </select>
        <button className="btn ghost sm" onClick={refresh} disabled={busy}>
          {busy ? '갱신 중…' : `갱신 (${scope === 'watchlist' ? '관심' : '전체'})`}</button>
      </div>
      {loading ? <p className="muted">불러오는 중…</p> : rows.length === 0 ? <p className="muted">예정 이벤트 없음 · 갱신 눌러 채우기</p> :
        <>
          <p className="muted" style={{ margin: '0.4rem 0' }}>{rows.length}건 · 가까운 순</p>
          <table>
            <thead><tr><th>워치</th><th className="l">일자</th><th className="l">Ticker</th><th className="l">이벤트</th><th className="l">유형</th><th>✔</th></tr></thead>
            <tbody>
              {rows.map(r => (
                <tr key={r.id}>
                  <td><input type="checkbox" checked={!!r.watched} onChange={() => toggle(r, 'watched')} /></td>
                  <td className="l muted">{dateHint(r)}</td>
                  <td className="l">{r.ticker ? <button className="tk" onClick={() => onPick({ ticker: r.ticker, name: r.name })}>{r.ticker}</button> : '—'}</td>
                  <td className="l">{r.title}</td>
                  <td className="l muted">{r.event_type}</td>
                  <td><input type="checkbox" checked={!!r.acknowledged} onChange={() => toggle(r, 'acknowledged')} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </>}
    </>
  )
}

/* ───────────── 자동 매매(조건매매) ───────────── */
// 조건 트리 → 사람이 읽는 한국어 요약
function condToText(node) {
  if (!node) return '—'
  const op = (o) => ({ '>=': '≥', '<=': '≤', '>': '>', '<': '<', '==': '=' }[o] || o)
  const isCmp = (o) => ['>=', '<=', '>', '<', '=='].includes(o)
  // 사용자가 입력한 메모/힌트를 항상 뒤에 덧붙임
  const withExtra = (s, n) => {
    const extra = [n.note, n.hint].filter(Boolean).join(' · ')
    return extra ? `${s} — ${extra}` : s
  }
  const k = node.kind
  if (k === 'price') return withExtra(`현재가 ${op(node.op)} ${Number(node.value).toLocaleString()}`, node)
  if (k === 'return_pct') return withExtra(`${node.ref === 'entry' ? '편입대비' : '당일'} 수익률 ${op(node.op)} ${node.value}%`, node)
  if (k === 'high_break') return withExtra('52주 신고가 돌파', node)
  if (k === 'twap') return withExtra(`${node.tranches}회 ${node.horizon} TWAP 분할`, node)
  if (k === 'date') {
    let w
    if (node.window === 'before') w = `${node.offset_days || 0}일 전까지`
    else if (node.window === 'after') w = node.poll_until ? `이후~${node.poll_until}` : '이후'
    else w = '당일'
    return withExtra(`${node.date} ${w}`, node)
  }
  if (k === 'ir_readout') {
    let s = isCmp(node.op)
      ? `[발표 판독] ${node.metric || '발표'} ${op(node.op)} ${node.value}${node.unit || ''}`
      : `[발표 판독] ${node.metric || '발표'}`
    const win = []
    if (node.date) win.push(node.date)
    if (node.window) win.push(node.window === 'after' ? '이후' : node.window)
    if (node.poll_until) win.push(`~${node.poll_until}`)
    if (win.length) s += ` (${win.join(' ')})`
    // 판독은 힌트(다중 hold 기준)를 항상 표시
    if (node.hint) s += ` — ${node.hint}`
    if (node.note) s += ` — ${node.note}`
    return s
  }
  if (k === 'all' || k === 'any') {
    const label = k === 'all' ? '모두 충족:' : '하나라도:'
    const lines = (node.of || []).map(child =>
      condToText(child).split('\n').map((ln, i) => (i === 0 ? `• ${ln}` : `  ${ln}`)).join('\n'))
    return [label, ...lines].join('\n')
  }
  return JSON.stringify(node)
}

function AutoTrade() {
  const [msgs, setMsgs] = useState([])
  const [input, setInput] = useState('')
  const [busy, setBusy] = useState(false)
  const [proposal, setProposal] = useState(null)
  const [orders, setOrders] = useState([])
  const [sel, setSel] = useState(null)
  const [evalBusy, setEvalBusy] = useState(false)
  const [chart, setChart] = useState(null)
  const [chartIv, setChartIv] = useState('1d')   // 봉: 1d/1wk/10m/30m/60m
  // 봉 종류별 표시 기간 (인트라데이는 짧게)
  const IV_PERIOD = { '1d': '6m', '1wk': '1y', '10m': '5d', '30m': '5d', '60m': '1m' }
  const IV_LABEL = { '1d': '일봉', '1wk': '주봉', '10m': '10분', '30m': '30분', '60m': '1시간' }

  const loadOrders = useCallback(
    () => api.autoOrders().then(d => setOrders(d.orders || [])).catch(() => { }), [])
  useEffect(() => { loadOrders() }, [loadOrders])

  // 선택된 카드의 종목 차트 — 봉 종류(일/주/10·30·60분) + 매수/매도 마커 + 진입/청산 가격선
  useEffect(() => {
    if (!sel?.ticker) { setChart(null); return }
    setChart(null)
    api.getChart(sel.ticker, IV_PERIOD[chartIv] || '6m', chartIv)
      .then(d => setChart(d)).catch(() => setChart(null))
  }, [sel?.ticker, chartIv])

  const sideKr = s => s === 'buy' ? '매수' : '매도'
  const unitKr = t => ({ weight_pct: '% 비중', amount: ' (금액)', shares: '주' }[t] || '')
  const statusKr = s => ({ armed: '대기', holding: '보유', done: '완료', cancelled: '취소', error: '오류' }[s] || s)

  // 조건 트리에서 첫 가격(kind:'price') 노드를 재귀 탐색 (all/any.of 포함)
  const firstPriceNode = (cond) => {
    if (!cond || typeof cond !== 'object') return null
    if (cond.kind === 'price') return cond
    if (Array.isArray(cond.of)) {
      for (const n of cond.of) { const f = firstPriceNode(n); if (f) return f }
    }
    return null
  }

  const send = async () => {
    const text = input.trim()
    if (!text || busy) return
    const next = [...msgs, { role: 'user', content: text }]
    setMsgs(next); setInput(''); setBusy(true); setProposal(null)
    try {
      const r = await api.autoChat(next)
      if (r.status === 'need_info') setMsgs([...next, { role: 'assistant', content: r.question || '?' }])
      else if (r.status === 'complete') {
        setProposal(r.order)
        const sc = r.order.safety_checks?.length ? '\n확인: ' + r.order.safety_checks.join(' · ') : ''
        setMsgs([...next, { role: 'assistant', content: '✅ 조건 완성: ' + (r.order.title || '') + sc + '\n아래 「매매 조건 완성」을 누르면 카드가 생성됩니다.' }])
      } else setMsgs([...next, { role: 'assistant', content: '⚠️ ' + (r.error || '오류') }])
    } catch (e) { setMsgs([...next, { role: 'assistant', content: '⚠️ ' + e }]) }
    finally { setBusy(false) }
  }

  const confirmOrder = async () => {
    if (!proposal || busy) return
    setBusy(true)
    const d = await api.autoCreate(proposal).catch(() => ({}))
    setBusy(false)
    if (d.ok) { setProposal(null); setMsgs([]); loadOrders() }
    else alert('생성 실패: ' + (d.error || ''))
  }

  const openCard = async (id) => {
    const d = await api.autoGet(id).catch(() => ({}))
    if (d.order) setSel(d.order)
  }
  const runEval = async () => {
    setEvalBusy(true); await api.autoEvaluate().catch(() => { }); setEvalBusy(false)
    loadOrders(); if (sel) openCard(sel.id)
  }
  const cancel = async (id) => {
    if (!confirm('이 조건을 취소할까요?')) return
    await api.autoCancel(id).catch(() => { }); setSel(null); loadOrders()
  }

  return (
    <div className="auto-wrap">
      <div className="auto-head">
        <h2 className="wordmark" style={{ margin: 0 }}>자동 매매</h2>
        <span className="muted small">조건 충족 시 발동 · dry-run</span>
        <button className="btn ghost sm" onClick={runEval} disabled={evalBusy}>{evalBusy ? '평가…' : '지금 평가'}</button>
      </div>

      <div className="auto-chat">
        <div className="auto-msgs">
          {msgs.length === 0 && <p className="muted small">예: "올릭스가 20만원 넘으면 예수금의 5% 비중으로 매수" / "에이비엘바이오 3상 발표 전날까지 보유, 신고가 돌파 시 3% 매수"</p>}
          {msgs.map((m, i) => <div key={i} className={'auto-msg ' + m.role}><pre>{m.content}</pre></div>)}
          {busy && <p className="muted small">분석 중…</p>}
        </div>
        {proposal && <button className="btn primary" onClick={confirmOrder} disabled={busy}>＋ 매매 조건 완성 (카드 생성)</button>}
        <div className="auto-input">
          <input value={input} onChange={e => setInput(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && send()} placeholder="조건을 자연어로 입력…" disabled={busy} />
          <button className="btn" onClick={send} disabled={busy || !input.trim()}>보내기</button>
        </div>
      </div>

      <div className="auto-cards">
        {orders.length === 0 && <p className="muted small">아직 등록된 조건이 없습니다.</p>}
        {orders.map(o => (
          <div key={o.id} className={'auto-card st-' + o.status + (sel?.id === o.id ? ' on' : '')} onClick={() => openCard(o.id)}>
            <div className="auto-card-top">
              <span className={'badge side-' + o.side}>{sideKr(o.side)}</span>
              <span className={'badge st-' + o.status}>{statusKr(o.status)}</span>
            </div>
            <div className="auto-card-title">{o.title}</div>
            <div className="muted small">{o.name || o.ticker} ({o.ticker}) · {o.size_value}{unitKr(o.size_type)}</div>
            {o.last_eval?.summary && <div className="auto-card-prog">{o.last_eval.summary}</div>}
            {o.buy_price && <div className="muted small">매수 @ {o.buy_price.toLocaleString()}{o.sell_price ? ` · 매도 @ ${o.sell_price.toLocaleString()}` : ''}</div>}
          </div>
        ))}
      </div>

      {sel && (
        <div className="auto-detail">
          <div className="auto-detail-head">
            <h3 style={{ margin: 0 }}>{sel.title}</h3>
            <button className="x-sm" onClick={() => setSel(null)}>닫기 ✕</button>
          </div>
          <div className="auto-grid2">
            <div><b>종목</b> {sel.name || sel.ticker} ({sel.ticker})</div>
            <div><b>방향</b> {sideKr(sel.side)}</div>
            <div><b>수량</b> {sel.size_value}{unitKr(sel.size_type)}</div>
            <div><b>상태</b> {statusKr(sel.status)} {sel.dry_run && <span className="badge">dry-run</span>}</div>
          </div>
          {(() => {
            const entryPx = firstPriceNode(sel.condition)
            const exitPx = firstPriceNode(sel.exit_condition)
            const priceLines = []
            if (entryPx) priceLines.push({ price: entryPx.value, color: '#3fb950', title: '매수 계획 ' + entryPx.op + entryPx.value })
            if (exitPx) priceLines.push({ price: exitPx.value, color: '#e3b341', title: '매도 계획 ' + exitPx.op + exitPx.value })
            const markers = []
            if (sel.buy_at && sel.buy_price) markers.push({ time: String(sel.buy_at).slice(0, 10), position: 'belowBar', color: '#3fb950', shape: 'arrowUp', text: '매수(실)' })
            if (sel.sell_at && sel.sell_price) markers.push({ time: String(sel.sell_at).slice(0, 10), position: 'aboveBar', color: '#f85149', shape: 'arrowDown', text: '매도(실)' })
            return (
              <div className="auto-sec"><b>차트</b>
                <div className="ranges">
                  {['1d', '1wk', '10m', '30m', '60m'].map(iv =>
                    <button key={iv} className={chartIv === iv ? 'active' : ''}
                      onClick={() => setChartIv(iv)}>{IV_LABEL[iv]}</button>)}
                </div>
                <div className="auto-chart">
                  {chart && !chart.error && chart.dates?.length
                    ? <PriceChart data={chart} period={IV_PERIOD[chartIv] || '6m'} height={300} markers={markers} priceLines={priceLines} />
                    : <p className="muted small">{chart === null ? '차트 불러오는 중…' : '차트 데이터 없음'}</p>}
                </div>
              </div>
            )
          })()}
          <div className="auto-sec"><b>조건 요약</b> <span className="muted small">(내가 입력한 조건)</span>
            <div className="auto-prog" style={{ whiteSpace: 'pre-line' }}><b>진입</b> {sideKr(sel.side)} · {condToText(sel.condition)}</div>
            <div className="auto-prog" style={{ whiteSpace: 'pre-line' }}><b>청산</b> {sel.exit_condition ? condToText(sel.exit_condition) : '없음(진입만)'}</div>
          </div>
          <div className="auto-sec"><b>조건 진행</b>
            <div className="auto-prog">{sel.last_eval?.summary || '아직 평가 전'}</div>
            {sel.last_eval?.at && <div className="muted small">마지막 평가 {String(sel.last_eval.at).slice(0, 16).replace('T', ' ')}</div>}
          </div>
          <div className="auto-sec"><b>계획 / 실제(체결)</b>
            <div className="auto-prog"><b>진입 계획</b> {sel.last_eval?.summary || '—'}</div>
            <div className="auto-prog"><b>매수 체결</b> {sel.buy_at ? `${String(sel.buy_at).slice(0, 16).replace('T', ' ')} @ ${sel.buy_price?.toLocaleString()}` : '대기'}</div>
            <div className="auto-prog"><b>청산 계획</b> {sel.exit_condition ? (sel.exit_eval?.summary || '(보유 시 평가)') : '없음(진입만)'}</div>
            <div className="auto-prog"><b>매도 체결</b> {sel.sell_at ? `${String(sel.sell_at).slice(0, 16).replace('T', ' ')} @ ${sel.sell_price?.toLocaleString()}` : '대기'}</div>
            <div className="muted small" style={{ marginTop: '0.3rem' }}>dry-run</div>
          </div>
          <div className="auto-sec"><b>매매 실행 내역</b>
            {sel.status === 'triggered'
              ? <div className="auto-fired">발동됨 · {String(sel.triggered_at || '').slice(0, 16).replace('T', ' ')}<br />{sel.triggered_detail?.summary}<br /><span className="muted small">dry-run</span></div>
              : <div className="muted small">발동 전 — 조건 충족 시 여기에 발동 내역이 기록됩니다.</div>}
          </div>
          <details className="auto-sec"><summary className="muted small">조건 원본(JSON)</summary><pre className="auto-json">{JSON.stringify(sel.condition, null, 2)}</pre></details>
          {sel.status !== 'cancelled' && <button className="btn ghost sm" onClick={() => cancel(sel.id)}>조건 취소</button>}
        </div>
      )}
    </div>
  )
}

/* ───────────── 모델 포트폴리오 ───────────── */
const BENCHES = [['XBI', 'XBI (미국 바이오)'], ['IBB', 'IBB'], ['ARKG', 'ARKG'], ['SPY', 'SPY'],
['463050', 'TIMEFOLIO K바이오'], ['305720', 'KODEX 바이오']]
function Portfolios({ onPick }) {
  const [list, setList] = useState([])
  const [sel, setSel] = useState(null)
  const [detail, setDetail] = useState(null)
  const [loading, setLoading] = useState(false)
  const [bench, setBench] = useState(['IBB'])
  const benchInit = useRef(null)   // 포트폴리오별 기본 벤치마크를 1회만 적용(수동 선택 보존)
  const [txs, setTxs] = useState([])
  const [showTx, setShowTx] = useState(false)
  const [newTk, setNewTk] = useState('')
  const [newW, setNewW] = useState(5)
  const [busy, setBusy] = useState(false)
  const [tgt, setTgt] = useState({})
  const [liveTs, setLiveTs] = useState(null)   // 마지막 실시간 갱신 시각
  const [live, setLive] = useState(true)       // 장중 자동 갱신 on/off

  const loadList = useCallback(() => api.getPortfolios().then(d => {
    setList(d.portfolios || []); if (sel == null && d.portfolios?.[0]) setSel(d.portfolios[0].id)
  }), [sel])
  useEffect(() => { loadList() }, [])

  // 포트폴리오 전환 시 기본 벤치마크: 한국 바이오텍 → TIMEFOLIO K바이오(463050), 그 외(미국) → IBB.
  // 같은 포트폴리오에선 1회만 적용해 사용자의 수동 선택을 덮어쓰지 않음.
  useEffect(() => {
    if (sel == null || benchInit.current === sel) return
    const p = list.find(x => x.id === sel)
    if (!p) return
    benchInit.current = sel
    const isKR = /바이오|한국|k-?bio|kbio/i.test(p.name || '')
    setBench([isKR ? '463050' : 'IBB'])
  }, [sel, list])

  const loadId = useRef(0)   // 요청 경합 가드: 최신 요청 결과만 반영(벤치마크 전환 시 stale 응답 무시)
  const load = useCallback(() => {
    if (sel == null) return
    const myId = ++loadId.current
    setLoading(true); setDetail(null)
    api.getPortfolio(sel, bench.join(',')).then(d => {
      if (myId !== loadId.current) return
      setDetail(d)
      const t = {}; (d.holdings || []).forEach(h => t[h.ticker] = Math.round((h.weight_pct || 0) * 10) / 10); setTgt(t)
      setLiveTs(new Date())
    }).catch(() => { if (myId === loadId.current) setDetail(null) })
      .finally(() => { if (myId === loadId.current) setLoading(false) })
    api.getPortfolioTxs(sel).then(d => { if (myId === loadId.current) setTxs(d.txs || []) }).catch(() => { if (myId === loadId.current) setTxs([]) })
  }, [sel, bench])
  useEffect(() => { load() }, [load])

  // 장중 실시간 수익률 — 경량 quote만 폴링(30초). 수익률 차트/거래내역은 재요청 안 함.
  useEffect(() => {
    if (sel == null || !live) return
    let stop = false, inFlight = false
    const tick = () => {
      if (stop || inFlight || !marketOpen()) return
      inFlight = true
      api.getPortfolioQuote(sel).then(d => {
        if (stop || !d.summary) return
        setDetail(prev => prev ? { ...prev, summary: d.summary, holdings: d.holdings || prev.holdings } : prev)
        setLiveTs(new Date())
      }).catch(() => { }).finally(() => { inFlight = false })
    }
    const iv = setInterval(tick, 30000)
    return () => { stop = true; clearInterval(iv) }
  }, [sel, live])

  const addHolding = async () => {
    if (!newTk.trim() || busy) return
    setBusy(true); await api.pfAddHolding(sel, newTk, +newW).catch(() => { }); setNewTk(''); setBusy(false); load()
  }
  const adjust = async (tk) => { setBusy(true); await api.pfSetWeight(sel, tk, +tgt[tk]).catch(() => { }); setBusy(false); load() }
  const sellAll = async (tk) => { setBusy(true); await api.pfSellAll(sel, tk).catch(() => { }); setBusy(false); load() }
  const createPf = async () => {
    const name = prompt('새 포트폴리오 이름'); if (!name) return
    const d = await api.pfCreate(name, 100); if (d.ok) { setSel(d.id); loadList() }
  }
  const deletePf = async () => {
    if (!confirm('이 포트폴리오를 삭제할까요?')) return
    await api.pfDelete(sel); setSel(null); setDetail(null); loadList()
  }
  const [extra, setExtra] = useState('')
  const addCompare = () => {
    const tks = extra.split(',').map(x => x.trim().toUpperCase()).filter(Boolean)
    if (tks.length) setBench(b => [...new Set([...b, ...tks])])
    setExtra('')
  }
  const customBench = bench.filter(b => !BENCHES.some(([k]) => k === b))

  const s = detail?.summary
  return (
    <>
      <div className="sec-head">
        <div className="pf-tabs">
          {list.map(p => <button key={p.id} className={p.id === sel ? 'active' : ''} onClick={() => setSel(p.id)}>{p.name}</button>)}
        </div>
        <div style={{ display: 'flex', gap: '0.4rem' }}>
          <button className="btn ghost sm" onClick={createPf}>＋ 새 포트폴리오</button>
          {sel != null && <button className="x-sm" onClick={deletePf}>삭제</button>}
        </div>
      </div>
      {loading && <p className="muted">불러오는 중…</p>}
      {detail && s && (
        <>
          <div className="pf-live">
            <span className={'live-dot' + (live && marketOpen() ? ' on' : '')} />
            <span className="muted small">
              {marketOpen() ? '장중 실시간' : '장 마감'}
              {liveTs ? ` · ${liveTs.toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit', second: '2-digit' })} 갱신` : ''}
            </span>
            <button className="x-sm" onClick={() => setLive(v => !v)}>{live ? '자동갱신 끄기' : '자동갱신 켜기'}</button>
            <button className="x-sm" onClick={load}>새로고침</button>
          </div>
          <div className="pf-stats">
            <div><b>NAV</b>{fmtUSD(s.current_size)}</div>
            <div><b>총수익률</b><Pct v={s.return_pct} /></div>
            <div><b>실현손익</b>{fmtUSD(s.realized_pnl)}</div>
            <div><b>현금</b>{fmtUSD(s.cash_amt)} ({(s.cash_pct || 0).toFixed(0)}%)</div>
          </div>

          <div className="pf-bench">
            <span className="muted small">벤치마크:</span>
            {BENCHES.map(([k, l]) => (
              <label key={k} className="chk"><input type="checkbox" checked={bench.includes(k)}
                onChange={() => setBench(b => b.includes(k) ? b.filter(x => x !== k) : [...b, k])} />{l}</label>
            ))}
            {customBench.map(tk => (
              <span key={tk} className="chip">{tk}<button onClick={() => setBench(b => b.filter(x => x !== tk))}>×</button></span>
            ))}
            <input className="cmp-in" placeholder="비교 종목 (예: AMGN, GILD)" value={extra}
              onChange={e => setExtra(e.target.value)} onKeyDown={e => { if (e.key === 'Enter') addCompare() }} />
            <button className="btn ghost sm" onClick={addCompare}>＋ 비교</button>
          </div>
          {detail.perf ? <PerfChart perf={detail.perf} /> : <p className="muted">{detail.perf_error ? '수익률 데이터 부족' : '차트 계산 중…'}</p>}

          <div className="sec-head" style={{ marginTop: '1.2rem' }}>
            <h3 className="wordmark" style={{ margin: 0 }}>편입 종목 · 목표% 입력 후 조정 → 현재가 체결</h3>
          </div>
          <div className="pf-add">
            <input placeholder="티커 (예: VRTX)" value={newTk} onChange={e => setNewTk(e.target.value)} />
            <input type="number" step="0.5" value={newW} onChange={e => setNewW(e.target.value)} style={{ width: '5rem' }} />
            <span className="muted small">% 비중</span>
            <button className="btn sm" onClick={addHolding} disabled={busy || !newTk.trim()}>편입</button>
          </div>
          <table style={{ marginTop: '0.6rem' }}>
            <thead><tr><th className="l">종목</th><th>비중</th><th>평단</th><th>현재가</th><th>수익률</th><th>평가액</th><th>실현</th><th>목표%</th><th></th></tr></thead>
            <tbody>
              {(detail.holdings || []).map((h, i) => (
                <tr key={i}>
                  <td className="l"><button className="tk" onClick={() => onPick({ ticker: h.ticker, name: h.name, close: h.curr_price })}>{h.name || h.ticker}</button></td>
                  <td>{(h.weight_pct || 0).toFixed(1)}%</td>
                  <td>{fmtPrice(h.avg_cost, h.ticker)}</td>
                  <td>{fmtPrice(h.curr_price, h.ticker)}</td>
                  <td><Pct v={h.return_pct} /></td>
                  <td>{fmtUSD(h.amt_current)}</td>
                  <td className="small">{Math.abs(h.realized_pnl || 0) >= 1 ? fmtUSD(h.realized_pnl) : '—'}</td>
                  <td><input type="number" step="0.5" className="wt" value={tgt[h.ticker] ?? ''} onChange={e => setTgt(t => ({ ...t, [h.ticker]: e.target.value }))} /></td>
                  <td style={{ whiteSpace: 'nowrap' }}>
                    <button className="x-sm" onClick={() => adjust(h.ticker)} disabled={busy}>조정</button>
                    <button className="x-sm" onClick={() => sellAll(h.ticker)} disabled={busy} title="전량 매도">✗</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {(!detail.holdings || !detail.holdings.length) && <p className="muted">아직 편입 종목 없음.</p>}

          {txs.length > 0 && (
            <div className="pf-tx">
              <button className="x-sm" onClick={() => setShowTx(v => !v)}>거래내역 ({txs.length}) {showTx ? '▾' : '▸'}</button>
              {showTx && <ul className="plist">{txs.map((t, i) => (
                <li key={i} className="small">{(t.trade_date || '').slice(0, 10)} {t.action === 'buy' ? '🟢 매수' : '🔴 매도'} <b>{t.ticker}</b> {(t.shares || 0).toLocaleString()}주 @ {fmtPrice(t.price, t.ticker)} ({fmtUSD(t.amount)})
                  {t.action === 'sell' && t.realized_pnl ? ` · 실현 ${fmtUSD(t.realized_pnl)}` : ''}</li>
              ))}</ul>}
            </div>
          )}
        </>
      )}
    </>
  )
}

/* ───────────── 관심종목 ───────────── */
function Watchlist({ onPick }) {
  const [rows, setRows] = useState([])
  const [loading, setLoading] = useState(false)
  const [addOpen, setAddOpen] = useState(false)
  const load = useCallback(() => {
    setLoading(true)
    api.getWatchlist().then(d => setRows(d.rows || [])).catch(() => setRows([])).finally(() => setLoading(false))
  }, [])
  useEffect(() => { load() }, [load])

  const remove = async (ticker) => { await api.watchRemove(ticker); load() }

  return (
    <>
      <div className="sec-head">
        <h2 className="wordmark sec">관심종목</h2>
        <button className="btn ghost sm" onClick={() => setAddOpen(true)}>＋ 종목 추가</button>
      </div>
      {loading ? <p className="muted">불러오는 중…</p> : rows.length === 0 ? <p className="muted">관심종목 없음</p> :
        <table>
          <thead><tr><th className="l">Ticker</th><th className="l">회사명</th><th className="l">산업</th>
            <th>현재가</th><th>1D</th><th>1M</th><th>1Y</th><th>시총</th><th></th></tr></thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={r.ticker + i}>
                <td className="l muted">{r.ticker}</td>
                <td className="l"><button className="tk" onClick={() => onPick(r)}>{r.name || r.ticker}</button></td>
                <td className="l muted">{r.industry || '—'}</td>
                <td>{fmtPrice(r.close, r.ticker)}</td>
                <td><Pct v={r.perf_1d} /></td>
                <td><Pct v={r.perf_1m} /></td>
                <td><Pct v={r.perf_1y} /></td>
                <td>{fmtMcap(r.market_cap, r.ticker)}</td>
                <td><button className="x-sm" title="해제" onClick={() => remove(r.ticker)}>★</button></td>
              </tr>
            ))}
          </tbody>
        </table>}
      {addOpen && <AddStockDialog onClose={() => { setAddOpen(false); load() }} onPick={onPick} />}
    </>
  )
}

function AddStockDialog({ onClose, onPick }) {
  const [q, setQ] = useState('')
  const [rows, setRows] = useState([])
  useEffect(() => {
    const t = setTimeout(() => api.searchUniverse(q, 30).then(d => setRows(d.rows || [])).catch(() => setRows([])), 250)
    return () => clearTimeout(t)
  }, [q])
  return (
    <>
      <div className="backdrop" onClick={onClose} />
      <div className="modal sm">
        <button className="x" onClick={onClose}>×</button>
        <h2>종목 추가</h2>
        <p className="muted">티커 또는 회사명 검색 (universe)</p>
        <input className="search" autoFocus value={q} onChange={e => setQ(e.target.value)} placeholder="VRTX, Vertex, …" />
        <p className="muted small">{rows.length}건</p>
        <table>
          <thead><tr><th className="l">Ticker</th><th className="l">회사명</th><th className="l">산업</th></tr></thead>
          <tbody>
            {rows.map(r => (
              <tr key={r.ticker}>
                <td className="l muted">{r.ticker}</td>
                <td className="l"><button className="tk" onClick={() => { onPick(r); onClose() }}>{r.name || r.ticker}</button></td>
                <td className="l muted">{r.industry || '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  )
}

/* ───────────── 워치 카탈리스트 배너 ───────────── */
function WatchedBanner({ onPick }) {
  const [data, setData] = useState({ week: [], month: [] })
  const load = () => api.getWatchedBanner().then(setData).catch(() => { })
  useEffect(() => { load() }, [])
  const ack = (id) => { api.catAck(id, true).catch(() => { }); setData(d => ({ week: d.week.filter(x => x.id !== id), month: d.month.filter(x => x.id !== id) })) }
  if (!data.week.length && !data.month.length) return null
  const Row = ({ it }) => (
    <div className="ban-row">
      <span className="ban-d">D-{it.days_left}</span>
      <span className="muted">{it.date_hint}</span>
      <button className="tk" onClick={() => onPick({ ticker: it.ticker, name: it.ticker })}>{it.ticker}</button>
      <span className="ban-t">{it.title}</span>
      <button className="x-sm" title="확인" onClick={() => ack(it.id)}>확인</button>
    </div>
  )
  return (
    <div className="banner">
      {data.week.length > 0 && <div className="ban-grp">
        <b>이번주 워치 카탈리스트</b>{data.week.map(it => <Row key={it.id} it={it} />)}</div>}
      {data.month.length > 0 && <div className="ban-grp">
        <b>1개월 이내 워치</b>{data.month.map(it => <Row key={it.id} it={it} />)}</div>}
    </div>
  )
}

/* ───────────── 공통: 지연로딩 접이식 섹션 ───────────── */
function Lazy({ title, children, defaultOpen = false }) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <details className="acc" open={defaultOpen} onToggle={e => setOpen(e.currentTarget.open)}>
      <summary>{title}</summary>
      {open && <div className="acc-body">{children}</div>}
    </details>
  )
}

/* ───────────── 종목 상세 (모달/인라인 공용 본문) ───────────── */
function StockDetail({ row, onClose, onPick, tickerMap }) {
  const [period, setPeriod] = useState('1y')
  const [interval, setIntervalV] = useState('1d')
  const [chart, setChart] = useState(null)
  const [stock, setStock] = useState(null)
  const [watched, setWatched] = useState(false)
  const [excluded, setExcluded] = useState(false)
  useEffect(() => { setChart(null); api.getChart(row.ticker, period, interval).then(setChart).catch(() => setChart({ error: 'x' })) }, [row.ticker, period, interval])
  useEffect(() => {
    setStock(null)
    api.getStock(row.ticker, row.name).then(d => { setStock(d); setWatched(!!d.watched); setExcluded(!!d.excluded) }).catch(() => setStock({}))
  }, [row.ticker])

  const toggleWatch = async () => { const v = !watched; setWatched(v); (v ? api.watchAdd : api.watchRemove)(row.ticker).catch(() => { }) }
  const toggleExcl = async () => { const v = !excluded; setExcluded(v); (v ? api.exclAdd : api.exclRemove)(row.ticker).catch(() => { }) }

  const RANGES = [['1m', '1M'], ['3m', '3M'], ['6m', '6M'], ['1y', '1Y'], ['5y', '5Y']]
  const IVALS = [['1d', '일봉'], ['1wk', '주봉'], ['1mo', '월봉']]
  return (
    <>
        <button className="x" onClick={onClose}>×</button>
        <div className="modal-head">
          <div><h2>{row.name || row.ticker}</h2><div className="muted">{row.ticker}</div></div>
          <div className="toggles">
            <button className={watched ? 'btn sm' : 'btn ghost sm'} onClick={toggleWatch}>{watched ? '★ 관심 해제' : '☆ 관심종목'}</button>
            <button className={excluded ? 'btn sm danger' : 'btn ghost sm'} onClick={toggleExcl}>{excluded ? '✓ 제외 해제' : '🚫 제외'}</button>
          </div>
        </div>
        <div className="kv">
          <div><b>현재가</b>{fmtPrice(row.close, row.ticker)}</div>
          <div><b>시총</b>{fmtMcap(row.market_cap, row.ticker)}</div>
          <div><b>1D</b><Pct v={row.perf_1d} /></div>
          <div><b>7D</b><Pct v={row.perf_7d} /></div>
          <div><b>1M</b><Pct v={row.perf_1m} /></div>
          <div><b>1Y</b><Pct v={row.perf_1y} /></div>
        </div>

        <div className="ranges">
          {RANGES.map(([k, l]) => <button key={k} className={k === period ? 'active' : ''} onClick={() => setPeriod(k)}>{l}</button>)}
          <span className="ival-sep" />
          {IVALS.map(([k, l]) => <button key={k} className={k === interval ? 'active' : ''} onClick={() => setIntervalV(k)}>{l}</button>)}
        </div>
        {chart ? (chart.error || !chart.dates?.length ? <p className="muted">차트 데이터 없음</p> : <PriceChart data={chart} period={period} />)
          : <div className="chartbox skel" />}

        <Lazy title="Peer 아이디어"><PeerIdeas ticker={row.ticker} name={row.name} onPick={onPick} tickerMap={tickerMap} /></Lazy>

        <Lazy title="IR · 파이프라인 URL 설정"><UrlSettings ticker={row.ticker} init={stock?.urls} /></Lazy>
        <Lazy title="IR 발표자료(PDF) 추출"><IrPdfs ticker={row.ticker} /></Lazy>
        <Lazy title="파이프라인 페이지"><PipelinePage url={stock?.urls?.pipeline_url} /></Lazy>

        <Lazy title={`최근 언급 많이 되는 파이프라인${stock?.news_count ? ` (뉴스 ${stock.news_count}건)` : ''}`}>
          {stock && Array.isArray(stock.pipelines) && stock.pipelines.length > 0
            ? <ul className="plist">{stock.pipelines.map((p, i) =>
              <li key={i}>{typeof p === 'string' ? p : (p.drug || p.title || p.name || JSON.stringify(p))}{p.moa ? ` — ${p.moa}` : ''}</li>)}</ul>
            : <p className="muted">최근 언급된 파이프라인 없음</p>}
        </Lazy>

        <Lazy title="최근 기사"><Articles ticker={row.ticker} name={row.name} /></Lazy>

        <Lazy title="카탈리스트">
          {stock && Array.isArray(stock.catalysts) && stock.catalysts.length > 0 &&
            <ul className="plist">{stock.catalysts.map((c, i) =>
              <li key={i}><b>{dateHint(c)}</b> — {c.title || c.event || c.description}</li>)}</ul>}
          {stock && Array.isArray(stock.earnings_call) && stock.earnings_call.length > 0 &&
            <><div className="muted small" style={{ marginTop: '0.5rem' }}>어닝콜 forward-looking 멘션</div>
              <ul className="plist">{stock.earnings_call.map((c, i) => <li key={i}><b>{(c.event_date || '').slice(0, 10)}</b> — {c.title}</li>)}</ul></>}
          {stock && Array.isArray(stock.company_events) && stock.company_events.length > 0 &&
            <><div className="muted small" style={{ marginTop: '0.5rem' }}>IR 자체공개 마일스톤</div>
              <ul className="plist">{stock.company_events.map((c, i) => <li key={i}><b>{dateHint(c)}</b> — {c.title}</li>)}</ul></>}
          {stock && !(stock.catalysts?.length || stock.earnings_call?.length || stock.company_events?.length) &&
            <p className="muted">예정 카탈리스트 없음</p>}
          <CatalystDiscover ticker={row.ticker} />
        </Lazy>

        <Lazy title="내부자 거래 (SEC Form 4)"><Insiders ticker={row.ticker} /></Lazy>
        <Lazy title="투자 리포트"><AiReport ticker={row.ticker} onPick={onPick} tickerMap={tickerMap} /></Lazy>
        <Lazy title="투자 메모" defaultOpen><MemoSection ticker={row.ticker} /></Lazy>
    </>
  )
}

/* 도킹 패널 — 페이지 위 플로팅 오버레이로 가로로 나란히 배치, 오른쪽 경계 Splitter로 너비 조절. 보드 리스트는 뒤에 그대로 보임. */
function DockPanel({ row, width, onResizeDelta, onClose, onPick, tickerMap }) {
  return (
    <div className="dock-panel" style={width ? { width } : undefined}>
      <div className="dock-body">
        <StockDetail row={row} onClose={onClose} onPick={onPick} tickerMap={tickerMap} />
      </div>
      {onResizeDelta && <Splitter onDrag={onResizeDelta} />}
    </div>
  )
}

/* 본문 마크다운 + 알려진 티커 클릭 → 병렬 모달. .tklink 클릭은 상위(행) onClick 전파 차단. */
function Linkified({ md, map, onPick, className = 'md', stop = false }) {
  const html = useMemo(() => api.linkify(api.mdToHtml(md || ''), map || {}), [md, map])
  const onClick = (e) => {
    const el = e.target.closest && e.target.closest('.tklink')
    if (!el) return
    e.preventDefault(); if (stop) e.stopPropagation()
    const tk = el.getAttribute('data-tk')
    if (tk && onPick) onPick({ ticker: tk, name: (map && map[tk]) || tk })
  }
  return <div className={className} onClick={onClick} dangerouslySetInnerHTML={{ __html: html }} />
}

/* ───────────── 모달 하위: Peer 아이디어 (유사 투자 아이디어 — 클릭 시 병렬 모달) ───────────── */
const PEER_BASIS = { thesis: '투자포인트', indication: '적응증', mechanism: '기전', asset: '에셋' }
function PeerIdeas({ ticker, name, onPick, tickerMap }) {
  const [d, setD] = useState(undefined)
  useEffect(() => { setD(undefined); api.getPeers(ticker).then(setD).catch(() => setD({ error: 'x' })) }, [ticker])
  if (d === undefined) return <p className="muted">유사 아이디어 분석 중… (최초 1회 1–2분, 이후 캐시)</p>
  if (d.error) return <p className="muted">불러오기 실패</p>
  const peers = d.peers || []
  return (
    <div className="peers">
      {d.target_thesis && <Linkified className="peer-thesis md" md={d.target_thesis} map={tickerMap} onPick={onPick} />}
      {peers.length === 0 ? <p className="muted">추천 아이디어 없음</p> : peers.map((p, i) => (
        <div key={p.ticker || i} className="peer-row" onClick={() => onPick && onPick({ ticker: p.ticker, name: p.name })}>
          <div className="peer-top">
            <span className="peer-name">{p.name || p.ticker} <span className="muted">({p.ticker})</span></span>
            <span className="peer-px">{fmtPrice(p.price, p.ticker)}</span>
            {p.basis && <span className="peer-basis">{PEER_BASIS[p.basis] || p.basis}</span>}
            {p.in_universe === false && <span className="peer-ref">참고</span>}
          </div>
          {p.note && <Linkified className="peer-note" md={p.note} map={tickerMap} onPick={onPick} stop />}
        </div>
      ))}
    </div>
  )
}

/* ───────────── 모달 하위: 메모 섹션 ───────────── */
function MemoSection({ ticker }) {
  const [memos, setMemos] = useState([])
  const [text, setText] = useState('')
  const [busy, setBusy] = useState(false)
  const [editId, setEditId] = useState(null)
  const [editText, setEditText] = useState('')
  const load = useCallback(() => api.getMemos(ticker).then(d => setMemos(d.memos || [])).catch(() => { }), [ticker])
  useEffect(() => { load() }, [load])

  const add = async () => { if (!text.trim() || busy) return; setBusy(true); await api.addMemo(ticker, text).catch(() => { }); setText(''); await load(); setBusy(false) }
  const fillVal = async () => {
    setBusy(true)
    try { const d = await api.getValuation(ticker); if (d.template) setText(t => (t ? t + '\n\n' : '') + d.template) } catch { }
    setBusy(false)
  }
  const save = async (id) => { if (!editText.trim()) return; await api.updateMemo(id, editText).catch(() => { }); setEditId(null); await load() }
  const del = async (id) => { await api.deleteMemo(id).catch(() => { }); await load() }

  return (
    <div className="memos">
      <div className="memo-new">
        <textarea value={text} onChange={e => setText(e.target.value)} placeholder="메모 입력…" rows={3} />
        <div className="memo-btns">
          <button className="btn ghost sm" onClick={fillVal} disabled={busy}>밸류에이션 템플릿</button>
          <button className="btn sm" onClick={add} disabled={busy || !text.trim()}>{busy ? '…' : '추가'}</button>
        </div>
      </div>
      {memos.map(m => (
        <div key={m.id} className="memo-item">
          {editId === m.id ? (
            <>
              <textarea value={editText} onChange={e => setEditText(e.target.value)} rows={3} />
              <div className="memo-btns">
                <button className="btn sm" onClick={() => save(m.id)}>저장</button>
                <button className="btn ghost sm" onClick={() => setEditId(null)}>취소</button>
              </div>
            </>
          ) : (
            <>
              <div className="memo-meta muted">{(m.created_at || '').slice(0, 16).replace('T', ' ')}{m.updated_at !== m.created_at ? ' (수정됨)' : ''}</div>
              <div className="md" dangerouslySetInnerHTML={{ __html: api.mdToHtml(m.body) }} />
              <div className="memo-btns">
                <button className="x-sm" onClick={() => { setEditId(m.id); setEditText(m.body) }}>수정</button>
                <button className="x-sm" onClick={() => del(m.id)}>삭제</button>
              </div>
            </>
          )}
        </div>
      ))}
    </div>
  )
}

/* AI 리포트 본문을 (경쟁 파이프라인 섹션) / (나머지)로 분리.
   "## …경쟁 파이프라인" 헤딩부터 다음 최상위 "## " 헤딩 직전까지를 comp로 떼어낸다.
   헤딩이 없으면 전체를 before로 두어 리포트 내 linkify를 끈다. */
function splitCompetitive(md) {
  if (!md) return { before: '', comp: '', after: '' }
  const lines = md.split('\n')
  let start = -1
  for (let i = 0; i < lines.length; i++) {
    if (/^##\s+.*경쟁\s*파이프라인/.test(lines[i])) { start = i; break }
  }
  if (start === -1) return { before: md, comp: '', after: '' }
  let end = lines.length
  for (let i = start + 1; i < lines.length; i++) {
    if (/^##\s+/.test(lines[i])) { end = i; break }
  }
  return {
    before: lines.slice(0, start).join('\n'),
    comp: lines.slice(start, end).join('\n'),
    after: lines.slice(end).join('\n'),
  }
}

/* ───────────── 모달 하위: AI 리포트 ───────────── */
function AiReport({ ticker, onPick, tickerMap }) {
  const [rep, setRep] = useState(undefined)
  const [busy, setBusy] = useState(false)
  useEffect(() => { api.getReport(ticker).then(d => setRep(d.cached ? d : null)).catch(() => setRep(null)) }, [ticker])
  const gen = async () => { setBusy(true); try { const d = await api.genReport(ticker); if (d.ok) setRep(d); else alert(d.error || '생성 실패') } finally { setBusy(false) } }
  if (rep === undefined) return <p className="muted">…</p>
  if (!rep) return <button className="btn" onClick={gen} disabled={busy}>{busy ? '생성 중… (1–3분)' : '리포트 생성'}</button>
  // 리포트 본문 중 '경쟁 파이프라인' 섹션만 linkify(경쟁사명 클릭) → 나머지는 오탐 방지로 plain 렌더
  const { before, comp, after } = splitCompetitive(rep.body)
  return (
    <>
      <div className="muted small">{(rep.generated_at || '').slice(0, 16).replace('T', ' ')} · {rep.model || ''}</div>
      {before && <div className="md" dangerouslySetInnerHTML={{ __html: api.mdToHtml(before) }} />}
      {comp && <Linkified md={comp} map={tickerMap} onPick={onPick} />}
      {after && <div className="md" dangerouslySetInnerHTML={{ __html: api.mdToHtml(after) }} />}
      <button className="btn ghost sm" onClick={gen} disabled={busy}>{busy ? '재생성 중… (1–3분)' : '재생성'}</button>
    </>
  )
}

/* ───────────── 모달 하위: 카탈리스트 발굴 ───────────── */
function CatalystDiscover({ ticker }) {
  const [busy, setBusy] = useState('')
  const [msg, setMsg] = useState('')
  const irExtract = async () => {
    setBusy('ir'); setMsg('IR 자료 추출 중… (느릴 수 있음)')
    try { const d = await api.catIrExtract(ticker); setMsg(d.error ? ('⚠️ ' + d.error) : `✓ ${d.deck_title || 'IR 자료'} — 마일스톤 ${(d.milestones || []).length}건 추출·저장`) }
    catch (e) { setMsg('⚠️ ' + e) } finally { setBusy('') }
  }
  const aiDiscover = async () => {
    setBusy('ai'); setMsg('12개월 카탈리스트 발굴 중… (1–3분)')
    try { const d = await api.catAiDiscover(ticker); setMsg(d.error ? ('⚠️ ' + d.error) : `✓ 발견 ${d.found ?? 0}건 · 저장 ${d.saved ?? 0}건`) }
    catch (e) { setMsg('⚠️ ' + e) } finally { setBusy('') }
  }
  return (
    <>
      <div className="memo-btns">
        <button className="btn ghost sm" onClick={irExtract} disabled={!!busy}>IR 자료 추출</button>
        <button className="btn ghost sm" onClick={aiDiscover} disabled={!!busy}>발굴 (1–3분)</button>
      </div>
      {msg && <p className="muted small" style={{ marginTop: '0.5rem' }}>{msg}</p>}
    </>
  )
}

/* ───────────── 모달 하위: 내부자 거래 ───────────── */
function Insiders({ ticker }) {
  const [d, setD] = useState(undefined)
  const [busy, setBusy] = useState(false)
  const load = useCallback(() => api.getInsiders(ticker).then(setD).catch(() => setD({ summary: {}, trades: [] })), [ticker])
  useEffect(() => { load() }, [load])
  if (d === undefined) return <p className="muted">…</p>
  const s = d.summary || {}
  const refresh = async () => { setBusy(true); await api.refreshInsiders(ticker).catch(() => { }); await load(); setBusy(false) }
  return (
    <>
      <div className="kv small">
        <div><b>거래</b>{s.trades ?? 0}건</div>
        <div><b>매수</b>{s.buys ?? 0} ({fmtUSD(s.buy_value)})</div>
        <div><b>매도</b>{s.sells ?? 0} ({fmtUSD(s.sell_value)})</div>
        <div><b>순매수</b>{fmtUSD(s.net_value)}</div>
      </div>
      {(d.trades || []).length > 0 &&
        <table className="small"><thead><tr><th className="l">일자</th><th className="l">내부자</th><th className="l">거래</th><th>수량</th><th>금액</th></tr></thead>
          <tbody>{d.trades.slice(0, 20).map((t, i) => (
            <tr key={i}><td className="l muted">{(t.trade_date || '').slice(0, 10)}</td>
              <td className="l">{t.insider_name}<span className="muted"> {t.title}</span></td>
              <td className="l">{t.transaction}</td><td>{(t.shares || 0).toLocaleString()}</td><td>{fmtUSD(t.value_usd)}</td></tr>
          ))}</tbody></table>}
      <button className="btn ghost sm" onClick={refresh} disabled={busy}>{busy ? '갱신 중…' : '갱신'}</button>
    </>
  )
}

/* ───────────── 모달 하위: 최근 기사 ───────────── */
function Articles({ ticker, name }) {
  const [items, setItems] = useState(undefined)
  useEffect(() => { api.getArticles(ticker, name).then(d => setItems(d.articles || [])).catch(() => setItems([])) }, [ticker])
  if (items === undefined) return <p className="muted">…</p>
  if (!items.length) return <p className="muted">관련 기사 없음</p>
  return items.slice(0, 8).map((n, i) => (
    <a key={i} className="news-item" href={n.link || '#'} target="_blank" rel="noreferrer">
      <div className="news-title">{n.title}</div>
      <div className="news-meta">{n.source || ''} {n.published || ''}</div>
    </a>
  ))
}

/* ───────────── 모달 하위: 파이프라인 페이지 임베드 ───────────── */
function PipelinePage({ url }) {
  if (!url) return <p className="muted">파이프라인 URL 없음 — ‘IR · 파이프라인 URL 설정’에서 자동 탐색/등록하세요.</p>
  return (
    <>
      <a href={url} target="_blank" rel="noreferrer" className="btn ghost sm" style={{ marginBottom: '0.5rem', display: 'inline-block' }}>새 창에서 열기 ↗</a>
      <iframe className="pipe-frame" src={url} title="pipeline" loading="lazy" />
      <p className="muted small">사이트가 임베드를 차단하면 위 ‘새 창에서 열기’로 확인하세요.</p>
    </>
  )
}

/* ───────────── 모달 하위: IR URL 설정 ───────────── */
function UrlSettings({ ticker, init }) {
  const [ir, setIr] = useState('')
  const [pl, setPl] = useState('')
  const [msg, setMsg] = useState('')
  const [busy, setBusy] = useState(false)
  useEffect(() => { setIr(init?.ir_url || ''); setPl(init?.pipeline_url || '') }, [init])
  const save = async () => { setBusy(true); await api.setUrls(ticker, ir, pl).catch(() => { }); setMsg('저장됨'); setBusy(false) }
  const disc = async () => {
    setBusy(true); setMsg('탐색 중…')
    try { const d = await api.discoverUrls(ticker); if (d.found) { if (d.found.ir_url) setIr(d.found.ir_url); if (d.found.pipeline_url) setPl(d.found.pipeline_url); setMsg('탐색 완료') } else setMsg('실패') }
    catch { setMsg('실패') } finally { setBusy(false) }
  }
  return (
    <div className="urlset">
      <label>IR 페이지 URL<input value={ir} onChange={e => setIr(e.target.value)} placeholder="https://investors…" /></label>
      <label>Pipeline URL<input value={pl} onChange={e => setPl(e.target.value)} placeholder="https://…/pipeline" /></label>
      <div className="memo-btns">
        <button className="btn ghost sm" onClick={disc} disabled={busy}>🔍 자동 탐색</button>
        <button className="btn sm" onClick={save} disabled={busy}>💾 저장</button>
        {msg && <span className="muted small">{msg}</span>}
      </div>
    </div>
  )
}

/* ───────────── 모달 하위: IR PDF 추출 ───────────── */
function IrPdfs({ ticker }) {
  const [items, setItems] = useState(undefined)
  useEffect(() => { api.getIrPdfs(ticker).then(d => setItems(d.items || [])).catch(() => setItems([])) }, [ticker])
  if (items === undefined) return <p className="muted">추출 중… (느릴 수 있음)</p>
  if (!items.length) return <p className="muted">추출된 자료 없음 (IR URL 등록 필요)</p>
  return (
    <ul className="plist">
      {items.map((p, i) => (
        <li key={i}><a href={p.url} target="_blank" rel="noreferrer">{p.date_hint && p.date_hint !== '—' ? `[${p.date_hint}] ` : ''}{p.title || p.url}</a>
          {p.kind ? <span className="muted"> · {p.kind}</span> : ''}</li>
      ))}
    </ul>
  )
}

/* ───────────── 플로팅 운영 위젯 ───────────── */
function OpsWidget({ country }) {
  const [open, setOpen] = useState(false)
  const [stats, setStats] = useState(null)
  const [busy, setBusy] = useState('')
  const [msg, setMsg] = useState('')
  useEffect(() => { if (open) api.getStats().then(setStats).catch(() => { }) }, [open])
  const run = (label, fn) => async () => {
    setBusy(label); setMsg('')
    try { const d = await fn(country); setMsg(d.ok === false ? ('⚠️ ' + (d.error || '실패')) : '✓ ' + JSON.stringify(d).slice(0, 120)) }
    catch (e) { setMsg('⚠️ ' + e) } finally { setBusy('') }
  }
  if (!open) return <button className="ops-launch" onClick={() => setOpen(true)}>운영</button>
  return (
    <div className="ops">
      <div className="ops-head">
        <span className="t">운영 · {country}</span>
        <button onClick={() => setOpen(false)}>✕</button>
      </div>
      {stats && <div className="ops-stats">universe {stats.universe} · 메모 {stats.memos} · 관심 {stats.watchlist}</div>}
      <div className="ops-btns">
        <button className="btn ghost sm" onClick={run('uni', api.opsRefreshUniverse)} disabled={!!busy}>{busy === 'uni' ? '갱신 중…' : 'Universe 갱신'}</button>
        <button className="btn ghost sm" onClick={run('high', api.opsRefreshHighs)} disabled={!!busy}>{busy === 'high' ? '갱신 중…' : '신고가 갱신'}</button>
        <button className="btn ghost sm" onClick={run('tg', api.opsTelegram)} disabled={!!busy}>{busy === 'tg' ? '발송 중…' : '텔레그램 발송'}</button>
      </div>
      {msg && <div className="ops-msg muted">{msg}</div>}
    </div>
  )
}

/* ───────────── 플로팅 챗 ───────────── */
function Chat() {
  const [open, setOpen] = useState(false)
  const [msgs, setMsgs] = useState([])
  const [text, setText] = useState('')
  const [busy, setBusy] = useState(false)
  const [atts, setAtts] = useState([])          // 첨부파일 [{kind,name,...}]
  const [geo, setGeo] = useState({ x: window.innerWidth - 470, y: 90, w: 430, h: 540 })
  const drag = useRef(null)
  const msgsRef = useRef(null)
  const fileRef = useRef(null)

  useEffect(() => { window.__askChat = (q) => { setOpen(true); setText(q) }; return () => { delete window.__askChat } }, [])
  useEffect(() => { if (msgsRef.current) msgsRef.current.scrollTop = msgsRef.current.scrollHeight }, [msgs, busy])
  // 열 때 텔레↔웹 공유 대화 로드
  useEffect(() => {
    if (!open) return
    api.getChatHistory().then(d => setMsgs((d.messages || []).map(m => ({ role: m.role, content: m.content, source: m.source }))))
      .catch(() => { })
  }, [open])
  const reset = async () => { await api.clearChat().catch(() => { }); setMsgs([]) }
  const toTelegram = async (txt) => { const d = await api.sendToTelegram(txt).catch(() => null); alert(d?.ok ? '텔레그램 전송됨' : '전송 실패') }

  const onMove = useCallback((e) => {
    const d = drag.current; if (!d) return
    if (d.mode === 'move') setGeo(g => ({ ...g, x: d.x0 + (e.clientX - d.mx), y: Math.max(0, d.y0 + (e.clientY - d.my)) }))
    else setGeo(g => ({ ...g, w: Math.max(320, d.w0 + (e.clientX - d.mx)), h: Math.max(300, d.h0 + (e.clientY - d.my)) }))
  }, [])
  const onUp = useCallback(() => { drag.current = null; window.removeEventListener('mousemove', onMove); window.removeEventListener('mouseup', onUp) }, [onMove])
  const start = (mode) => (e) => {
    e.preventDefault()
    drag.current = { mode, mx: e.clientX, my: e.clientY, x0: geo.x, y0: geo.y, w0: geo.w, h0: geo.h }
    window.addEventListener('mousemove', onMove); window.addEventListener('mouseup', onUp)
  }

  const pickFiles = async (fileList) => {
    const files = Array.from(fileList || [])
    if (!files.length) return
    const MAX = 25 * 1024 * 1024   // 25MB/파일 가드
    for (const f of files) {
      if (f.size > MAX) { alert(`${f.name}: 25MB 초과 — 첨부 불가`); continue }
      try { const a = await api.fileToAttachment(f); setAtts(p => [...p, a]) }
      catch (e) { alert(String(e.message || e)) }
    }
    if (fileRef.current) fileRef.current.value = ''   // 같은 파일 재선택 허용
  }
  const removeAtt = (i) => setAtts(p => p.filter((_, k) => k !== i))

  const send = async () => {
    const q = text.trim(); if ((!q && atts.length === 0) || busy) return
    const hist = msgs.map(m => ({ role: m.role, content: m.content }))
    const sending = atts
    const label = q || `📎 ${sending.map(a => a.name).join(', ')}`
    setMsgs(m => [...m, { role: 'user', content: label }]); setText(''); setAtts([]); setBusy(true)
    try { const d = await api.postChat(q, hist, sending.length ? sending : undefined); setMsgs(m => [...m, { role: 'assistant', content: d.reply || '(응답 없음)' }]) }
    catch (e) { setMsgs(m => [...m, { role: 'assistant', content: '⚠️ ' + e }]) }
    finally { setBusy(false) }
  }

  if (!open) return <button className="chat-launch" onClick={() => setOpen(true)}>CHAT</button>
  return (
    <div className="chat" style={{ left: geo.x, top: geo.y, width: geo.w, height: geo.h }}>
      <div className="head" onMouseDown={start('move')}>
        <span className="t">챗 · 텔레그램 공유</span>
        <button onMouseDown={e => e.stopPropagation()} onClick={reset}>초기화</button>
        <button onMouseDown={e => e.stopPropagation()} onClick={() => setOpen(false)}>➖</button>
      </div>
      <div className="msgs" ref={msgsRef}
        onDragOver={e => { e.preventDefault() }}
        onDrop={e => { e.preventDefault(); pickFiles(e.dataTransfer?.files) }}>
        {msgs.length === 0 && <div className="spin">질문을 입력하세요. (예: RVMD 분석 / KRAS G12D degrader 기전)</div>}
        {msgs.map((m, i) => m.role === 'assistant'
          ? <div key={i} className="msg assistant md-wrap">
              <div className="md" dangerouslySetInnerHTML={{ __html: api.mdToHtml(m.content) }} />
              <button className="tg-btn" onMouseDown={e => e.stopPropagation()} onClick={() => toTelegram(m.content)} title="텔레그램으로 전송">텔레 전송</button>
            </div>
          : <div key={i} className={'msg user' + (m.source === 'telegram' ? ' tg' : '')}>{m.source === 'telegram' ? '[텔레] ' : ''}{m.content}</div>)}
        {busy && <div className="spin">조사 중… (도구 호출, 최대 1–2분)</div>}
      </div>
      {atts.length > 0 && (
        <div className="att-chips">
          {atts.map((a, i) => (
            <span key={i} className="att-chip" title={a.name}>
              {a.kind === 'image' ? '🖼' : a.kind === 'pdf' ? '📄' : '📎'} {a.name}
              <button onClick={() => removeAtt(i)} title="제거">✕</button>
            </span>
          ))}
        </div>
      )}
      <div className="input">
        <input ref={fileRef} type="file" multiple accept="image/*,application/pdf,.txt,.md,.csv,.tsv,.json,.log,.xml,.yml,.yaml,.py,.js,.ts,.html"
          style={{ display: 'none' }} onChange={e => pickFiles(e.target.files)} />
        <button className="attach" onClick={() => fileRef.current?.click()} disabled={busy} title="파일 첨부 (PDF·이미지·텍스트)">📎</button>
        <textarea value={text} placeholder="질문 입력…  (Enter 전송, Shift+Enter 줄바꿈)"
          onChange={e => setText(e.target.value)}
          onPaste={e => { const fs = Array.from(e.clipboardData?.files || []); if (fs.length) { e.preventDefault(); pickFiles(fs) } }}
          onKeyDown={e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send() } }} />
        <button className="send" onClick={send} disabled={busy}>전송</button>
      </div>
      <div className="grip" onMouseDown={start('resize')} title="드래그하여 크기 조절" />
    </div>
  )
}
