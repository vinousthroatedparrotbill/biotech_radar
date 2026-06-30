// 백엔드(FastAPI) 호출 헬퍼 — vite proxy가 /api → :8000 으로 전달
const j = (r) => r.json()
const enc = encodeURIComponent
const post = (url, body) =>
  fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }).then(j)

const del = (url) => fetch(url, { method: 'DELETE' }).then(j)
const put = (url, body) =>
  fetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }).then(j)
const patch = (url, body) =>
  fetch(url, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }).then(j)

export const getBoard = (country, view) => fetch(`/api/board?country=${country}&view=${view}`).then(j)
export const getChart = (ticker, period, interval = '1d') => fetch(`/api/chart?ticker=${enc(ticker)}&period=${period}&interval=${interval}`).then(j)
export const getStock = (ticker, name) => fetch(`/api/stock?ticker=${enc(ticker)}&name=${enc(name || '')}`).then(j)
export const getPeers = (ticker) => fetch(`/api/peers?ticker=${enc(ticker)}`).then(j)
export const getTickers = () => fetch('/api/tickers').then(j)
export const postReason = (kind, rows, generate = true, country = 'USA', force = false) => post('/api/reason', { kind, rows, generate, country, force })

// 진행 중 이유분석 promise를 모듈 레벨에 보존 — 탭 전환(컴포넌트 언마운트)에도 유지
const _reasonRuns = {}
const _reasonKey = (kind, country) => country + '|' + kind
export function runReason(kind, rows, country = 'USA', force = false) {
  const key = _reasonKey(kind, country)
  if (force) delete _reasonRuns[key]            // 강제 재생성: 기존(완료된) promise 버리고 새로
  if (!_reasonRuns[key]) {
    _reasonRuns[key] = postReason(kind, rows, true, country, force)
      .then(d => { delete _reasonRuns[key]; return d.markdown || '(분석 없음)' })  // 완료 후 정리 → 다음 재생성 가능
      .catch(e => { delete _reasonRuns[key]; throw e })
  }
  return _reasonRuns[key]
}
export const pendingReason = (kind, country = 'USA') => _reasonRuns[_reasonKey(kind, country)] || null
export const getPortfolios = () => fetch('/api/portfolios').then(j)
export const getPortfolio = (id, bench = 'XBI') => fetch(`/api/portfolio?id=${id}&bench=${enc(bench)}`).then(j)
export const getPortfolioTxs = (id) => fetch(`/api/portfolio/${id}/transactions`).then(j)
export const getPortfolioQuote = (id) => fetch(`/api/portfolio/${id}/quote`).then(j)
export const pfAddHolding = (id, ticker, weight) => post(`/api/portfolio/${id}/holding`, { ticker, weight })
export const pfSetWeight = (id, ticker, weight) => post(`/api/portfolio/${id}/weight`, { ticker, weight })
export const pfSellAll = (id, ticker) => del(`/api/portfolio/${id}/holding/${enc(ticker)}`)
export const pfCreate = (name, size_m = 100) => post('/api/portfolios', { name, size_m })
export const pfDelete = (id) => del(`/api/portfolios/${id}`)

// 자동매매(조건매매)
export const autoChat = (messages) => post('/api/auto/chat', { messages })
export const autoOrders = () => fetch('/api/auto/orders').then(j)
export const autoCreate = (order) => post('/api/auto/orders', { order })
export const autoGet = (id) => fetch(`/api/auto/orders/${id}`).then(j)
export const autoCancel = (id) => post(`/api/auto/orders/${id}/cancel`, {})
export const autoEvaluate = () => post('/api/auto/evaluate', {})
export const getDailyNews = (country, days = 1) => fetch(`/api/daily_news?country=${country}&days=${days}`).then(j)
export const postChat = (message, history) => post('/api/chat', { message, history })
export const getChatHistory = () => fetch('/api/chat/history').then(j)
export const clearChat = () => post('/api/chat/clear', {})
export const sendToTelegram = (text) => post('/api/chat/telegram', { text })

// 카탈리스트
export const getCatalysts = (days = 90, types = '', scope = 'all') =>
  fetch(`/api/catalysts?days=${days}&types=${enc(types)}&scope=${scope}`).then(j)
export const catWatch = (id, value) => post('/api/catalysts/watch', { id, value })
export const catAck = (id, value) => post('/api/catalysts/ack', { id, value })
export const catRefresh = (scope) => post('/api/catalysts/refresh', { scope })
export const catIrExtract = (ticker) => post('/api/catalysts/ir_extract', { ticker })
export const catAiDiscover = (ticker) => post('/api/catalysts/ai_discover', { ticker })

// 메모
export const getTimeline = (limit = 50) => fetch(`/api/memos/timeline?limit=${limit}`).then(j)
export const getMemos = (ticker) => fetch(`/api/memos/by_ticker/${enc(ticker)}`).then(j)
export const addMemo = (ticker, body) => post(`/api/memos/by_ticker/${enc(ticker)}`, { body })
export const updateMemo = (id, body) => patch(`/api/memo/${id}`, { body })
export const deleteMemo = (id) => del(`/api/memo/${id}`)
export const getValuation = (ticker) => fetch(`/api/valuation/${enc(ticker)}`).then(j)

// AI 리포트 / 기사
export const getReport = (ticker) => fetch(`/api/report/${enc(ticker)}`).then(j)
export const genReport = (ticker) => post(`/api/report/${enc(ticker)}`, {})
export const getArticles = (ticker, name) => fetch(`/api/articles/${enc(ticker)}?name=${enc(name || '')}`).then(j)

// 관심종목 / 제외
export const getWatchlist = () => fetch('/api/watchlist').then(j)
export const watchAdd = (ticker) => post(`/api/watchlist/${enc(ticker)}`, {})
export const watchRemove = (ticker) => del(`/api/watchlist/${enc(ticker)}`)
export const getExcluded = () => fetch('/api/excluded').then(j)
export const exclAdd = (ticker) => post(`/api/excluded/${enc(ticker)}`, { note: 'user excluded' })
export const exclRemove = (ticker) => del(`/api/excluded/${enc(ticker)}`)
export const searchUniverse = (q, limit = 30) => fetch(`/api/universe?q=${enc(q)}&limit=${limit}`).then(j)

// 내부자 / URL / IR
export const getInsiders = (ticker) => fetch(`/api/insiders/${enc(ticker)}`).then(j)
export const refreshInsiders = (ticker) => post(`/api/insiders/${enc(ticker)}/refresh`, {})
export const getUrls = (ticker) => fetch(`/api/urls/${enc(ticker)}`).then(j)
export const setUrls = (ticker, ir_url, pipeline_url) => put(`/api/urls/${enc(ticker)}`, { ir_url, pipeline_url })
export const discoverUrls = (ticker) => post(`/api/urls/${enc(ticker)}/discover`, {})
export const getIrPdfs = (ticker) => fetch(`/api/ir_pdfs/${enc(ticker)}`).then(j)

// 배너 / 통계 / 운영
export const getWatchedBanner = () => fetch('/api/watched_banner').then(j)
export const getStats = () => fetch('/api/stats').then(j)
export const opsRefreshUniverse = (country) => post('/api/ops/refresh_universe', { country })
export const opsRefreshHighs = (country) => post('/api/ops/refresh_highs', { country })
export const opsTelegram = (country) => post('/api/ops/telegram', { country })

// 아주 가벼운 마크다운 → HTML (이유분석 카드/챗에 사용)
export function mdToHtml(md) {
  if (!md) return ''
  const esc = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  const inline = (s) =>
    esc(s)
      .replace(/\*\*(.+?)\*\*/g, '<b>$1</b>')
      .replace(/\[(.+?)\]\((https?:[^)]+)\)/g, '<a href="$2" target="_blank" rel="noreferrer">$1</a>')
  const out = []
  let inList = false
  for (const raw of md.split('\n')) {
    const line = raw.trimEnd()
    const close = () => { if (inList) { out.push('</ul>'); inList = false } }
    if (/^#{1,6}\s/.test(line)) { close(); const lv = line.match(/^#+/)[0].length; out.push(`<h${Math.min(lv + 2, 6)}>${inline(line.replace(/^#+\s/, ''))}</h${Math.min(lv + 2, 6)}>`) }
    else if (/^\s*[-*]\s+/.test(line)) { if (!inList) { out.push('<ul>'); inList = true } out.push(`<li>${inline(line.replace(/^\s*[-*]\s+/, ''))}</li>`) }
    else if (line === '') { close(); out.push('') }
    else { close(); out.push(`<p>${inline(line)}</p>`) }
  }
  if (inList) out.push('</ul>')
  return out.join('\n')
}

// 본문 텍스트(HTML) 안의 알려진 티커를 클릭 가능한 <span class="tklink">로 래핑.
// 텍스트 노드만 변환(태그/속성·<a> 내부는 건너뜀), map에 정확히 존재하는 토큰만, 흔한 영단어는 제외.
const _TK_STOP = new Set([
  'A', 'I', 'IT', 'IS', 'ON', 'OR', 'SO', 'BE', 'AT', 'IN', 'AS', 'AI', 'AN', 'OF', 'TO', 'BY', 'NO', 'UP',
  'US', 'EU', 'AND', 'THE', 'FOR', 'ARE', 'CAN', 'MAY', 'NEW', 'ALL', 'CEO', 'CFO', 'FDA', 'IND', 'NDA',
  'BLA', 'IPO', 'ETF', 'USA', 'API', 'II', 'III', 'IV', 'Q1', 'Q2', 'Q3', 'Q4', 'DATA', 'PHASE', 'NYSE', 'OTC',
])
export function linkify(html, map) {
  if (!html || !map) return html || ''
  const re = /(<[^>]+>)/g
  const TK = /\b([A-Z]{1,6}|\d{6})\b/g
  let inA = false
  return html.split(re).map(seg => {
    if (!seg) return seg
    if (seg[0] === '<') {
      const t = seg.slice(0, 3).toLowerCase()
      if (t === '<a ' || t === '<a>') inA = true
      else if (t === '</a') inA = false
      return seg
    }
    if (inA) return seg
    return seg.replace(TK, (m) => {
      if (_TK_STOP.has(m)) return m
      if (!Object.prototype.hasOwnProperty.call(map, m)) return m
      return `<span class="tklink" data-tk="${m}">${m}</span>`
    })
  }).join('')
}
