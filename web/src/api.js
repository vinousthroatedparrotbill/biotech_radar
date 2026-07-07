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
export const postChat = (message, history, attachments) => post('/api/chat', { message, history, attachments })

// File → 첨부 객체 {kind, data|text, media_type, name}. bot_agent._build_user_content 규격.
export function fileToAttachment(file) {
  return new Promise((resolve, reject) => {
    const name = file.name || 'file'
    const mt = file.type || ''
    const isImg = mt.startsWith('image/')
    const isPdf = mt === 'application/pdf' || /\.pdf$/i.test(name)
    const isText = mt.startsWith('text/') || /\.(txt|md|csv|tsv|json|log|xml|ya?ml|py|js|ts|html?)$/i.test(name)
    const r = new FileReader()
    r.onerror = () => reject(new Error('파일 읽기 실패: ' + name))
    if (isText) {
      r.onload = () => resolve({ kind: 'text', name, text: String(r.result || '') })
      r.readAsText(file)
    } else if (isImg || isPdf) {
      r.onload = () => {
        const s = String(r.result || '')
        const data = s.slice(s.indexOf(',') + 1)   // "data:...;base64," 접두 제거
        resolve(isPdf
          ? { kind: 'pdf', name, data }
          : { kind: 'image', name, data, media_type: mt || 'image/png' })
      }
      r.readAsDataURL(file)
    } else {
      reject(new Error('지원하지 않는 형식: ' + name + ' (PDF·이미지·텍스트만)'))
    }
  })
}
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

// 본문 텍스트(HTML) 안의 알려진 티커/회사명을 클릭 가능한 <span class="tklink">로 래핑.
// 텍스트 노드만 변환(태그/속성·<a> 내부는 건너뜀). 티커는 map에 정확히 존재하는 ALL-CAPS 토큰만,
// 흔한 영단어/바이오 약어는 제외. 회사명(map의 value)은 전체명 + 안전한 약칭으로 매칭(대소문자 무시, 최장일치).
const _TK_STOP = new Set([
  'A', 'I', 'IT', 'IS', 'ON', 'OR', 'SO', 'BE', 'AT', 'IN', 'AS', 'AI', 'AN', 'OF', 'TO', 'BY', 'NO', 'UP',
  'US', 'EU', 'UK', 'AND', 'THE', 'FOR', 'ARE', 'CAN', 'MAY', 'NEW', 'ALL', 'CEO', 'CFO', 'FDA', 'IND', 'NDA',
  'BLA', 'IPO', 'ETF', 'USA', 'API', 'II', 'III', 'IV', 'Q1', 'Q2', 'Q3', 'Q4', 'DATA', 'PHASE', 'NYSE', 'OTC',
  // 흔한 바이오/금융 약어 — 티커와 충돌하지만 본문에선 일반명사처럼 쓰임 → 티커로 링크하지 않음
  'RNA', 'DNA', 'MRNA', 'SIRNA', 'ADC', 'TCE', 'CAR', 'IL', 'CNS', 'NASH', 'MASH', 'NSCLC',
  'ORR', 'PFS', 'OS', 'PASI', 'IGA', 'EPS', 'SOTP', 'DCF', 'P1', 'P2', 'P3',
])

// 회사명 약칭을 만들 때 떼어내는 접미사(소문자, 끝의 마침표 제거)
const _ALIAS_SUFFIX = new Set([
  'pharmaceuticals', 'pharmaceutical', 'pharma', 'therapeutics', 'therapeutic', 'biosciences', 'bioscience',
  'sciences', 'science', 'biopharmaceuticals', 'biopharma', 'biotechnology', 'biotechnologies', 'biotech',
  'holdings', 'holding', 'corporation', 'incorporated', 'technologies', 'technology', 'laboratories',
  'group', 'company', 'limited', 'bio', 'inc', 'corp', 'ltd', 'plc', 'co', 'llc', 'nv', 'sa', 'ag', 'ab',
])
// 약칭으로 쓰기엔 너무 일반적인 단어 → 전체명만 링크
const _GENERIC_ALIAS = new Set([
  'BIO', 'LIFE', 'HEALTH', 'HEALTHCARE', 'GLOBAL', 'AMERICAN', 'UNITED', 'NATIONAL', 'MEDICAL', 'PHARMA',
  'MEDICINE', 'MEDICINES', 'GENETICS', 'GENOMICS', 'ONCOLOGY', 'IMMUNO', 'CELL', 'GENE', 'DRUG', 'CLINICAL',
  'NOVA', 'NOVO', 'META', 'BETA', 'ALPHA', 'VITAL', 'PRIME', 'CORE', 'EDGE', 'NEXT', 'OPEN', 'TRUE', 'REAL',
  'BLUE', 'NORTH', 'SOUTH', 'FIRST', 'GRAND', 'GREAT', 'THERAPY', 'GROUP', 'HOLDINGS', 'BIOPHARMA', 'BIOTECH',
])

// 큐레이션된 대형 제약사 별칭(소문자) → 티커. ticker_master에 풀네임이 없거나 약칭 생성이
// 실패하는 흔한 빅파마를 보강. 모두 실제 상장 티커이며 이름 인덱스에 최우선으로 병합한다.
const _CURATED_ALIASES = {
  'pfizer': 'PFE', 'merck': 'MRK', 'merck & co': 'MRK', 'abbvie': 'ABBV', 'sanofi': 'SNY',
  'novartis': 'NVS', 'astrazeneca': 'AZN', 'bristol myers': 'BMY', 'bristol-myers squibb': 'BMY',
  'bristol myers squibb': 'BMY', 'bms': 'BMY', 'eli lilly': 'LLY', 'lilly': 'LLY', 'amgen': 'AMGN',
  'gilead': 'GILD', 'regeneron': 'REGN', 'biogen': 'BIIB', 'vertex': 'VRTX', 'moderna': 'MRNA',
  'gsk': 'GSK', 'glaxosmithkline': 'GSK', 'johnson & johnson': 'JNJ', 'j&j': 'JNJ', 'roche': 'RHHBY',
  'genentech': 'RHHBY', 'novo nordisk': 'NVO', 'takeda': 'TAK', 'wave life sciences': 'WVE',
  // 경쟁 파이프라인에서 이름으로 자주 언급되는 (보드 유니버스 밖) 바이오텍 — 모두 실제 상장 티커
  'merus': 'MRUS', 'genmab': 'GMAB', 'argenx': 'ARGX', 'zymeworks': 'ZYME', 'moonlake': 'MLTX',
  'apogee': 'APGE', 'insmed': 'INSM', 'arcus': 'RCUS', 'immunome': 'IMNM', 'nuvation': 'NUVB',
}

// 회사명 → 약칭 (끝의 접미사 단어들을 반복 제거)
function _alias(name) {
  let words = name.replace(/,/g, ' ').trim().split(/\s+/)
  while (words.length > 1) {
    const last = words[words.length - 1].toLowerCase().replace(/[.,]+$/, '')
    if (_ALIAS_SUFFIX.has(last)) words.pop()
    else break
  }
  return words.join(' ').trim()
}

// map(티커→회사명)에서 이름 인덱스를 1회 빌드(WeakMap 캐시).
// 전체명 + 고유한 약칭만 등록하고, 두 종목이 같은 변형을 만들면 충돌로 제외.
const _nameIdxCache = new WeakMap()
function _buildNameIndex(map) {
  let idx = _nameIdxCache.get(map)
  if (idx) return idx
  const cand = new Map()   // 소문자 변형 → 티커 (null = 충돌하여 제외)
  const add = (variant, tk) => {
    const k = variant.toLowerCase()
    if (!cand.has(k)) cand.set(k, tk)
    else if (cand.get(k) !== tk) cand.set(k, null)
  }
  for (const tk in map) {
    if (!Object.prototype.hasOwnProperty.call(map, tk)) continue
    const name = (map[tk] || '').trim()
    if (name.length >= 4 && /[A-Za-z]/.test(name) && name.toUpperCase() !== tk) add(name, tk)
    const al = _alias(name)
    if (al.length >= 4 && al.toLowerCase() !== name.toLowerCase() && /[A-Za-z]/.test(al)
      && al.toUpperCase() !== tk && !_GENERIC_ALIAS.has(al.toUpperCase())) add(al, tk)
  }
  const nameMap = new Map()
  for (const [k, tk] of cand) if (tk) nameMap.set(k, tk)
  // 큐레이션 별칭은 최우선 — 기존 변형/충돌을 덮어쓴다. '&'는 본문이 mdToHtml로
  // '&amp;'로 이스케이프되므로 이스케이프 변형도 함께 등록한다.
  for (const k in _CURATED_ALIASES) {
    const tk = _CURATED_ALIASES[k]
    nameMap.set(k, tk)
    if (k.includes('&')) nameMap.set(k.replace(/&/g, '&amp;'), tk)
  }
  const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  // 최장일치 우선: 긴 이름을 alternation 앞쪽에 배치
  const nameAlt = [...nameMap.keys()].sort((a, b) => b.length - a.length)
    .map(k => `(?<![A-Za-z0-9])${esc(k)}(?![A-Za-z0-9])`)
  // 이름 기준 전용 — 단독 대문자 티커 토큰(DNA/RNA 등 단어성 오탐) 제외, KR 6자리 코드만 추가 허용
  const tokenSrc = (nameAlt.length ? nameAlt.join('|') + '|' : '') + '\\b\\d{6}\\b'
  idx = { nameMap, re: new RegExp(tokenSrc, 'gi') }   // i 플래그 → 회사명은 대소문자 무시
  _nameIdxCache.set(map, idx)
  return idx
}

export function linkify(html, map) {
  if (!html || !map) return html || ''
  const { nameMap, re } = _buildNameIndex(map)
  const tag = /(<[^>]+>)/g
  let inA = false
  return html.split(tag).map(seg => {
    if (!seg) return seg
    if (seg[0] === '<') {
      const t = seg.slice(0, 3).toLowerCase()
      if (t === '<a ' || t === '<a>') inA = true
      else if (t === '</a') inA = false
      return seg
    }
    if (inA) return seg
    // 한 번의 패스로 회사명/티커 모두 래핑 → 중복 래핑·교차 매칭 없음
    return seg.replace(re, (m) => {
      const byName = nameMap.get(m.toLowerCase())
      if (byName) return `<span class="tklink" data-tk="${byName}">${m}</span>`
      // 단독 토큰은 KR 6자리 코드만(명확). US 대문자 토큰(DNA/RNA 등)은 단어 오탐이라 링크 안 함 — 회사명으로만.
      if (/^\d{6}$/.test(m) && Object.prototype.hasOwnProperty.call(map, m))
        return `<span class="tklink" data-tk="${m}">${m}</span>`
      return m
    })
  }).join('')
}
