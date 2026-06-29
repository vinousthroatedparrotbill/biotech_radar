import React, { useEffect, useRef, useState } from 'react'
import { createChart, CandlestickSeries, LineSeries, AreaSeries } from 'lightweight-charts'

const BASE = {
  layout: { background: { color: '#fff' }, textColor: '#5b6f6e', fontFamily: 'Pretendard, sans-serif' },
  grid: { vertLines: { color: '#f0f4f3' }, horzLines: { color: '#f0f4f3' } },
  rightPriceScale: { borderColor: '#e4ebe9' },
  timeScale: { borderColor: '#e4ebe9', timeVisible: false },
  crosshair: { mode: 0 },
  autoSize: false,
}
const MA = { ma20: '#ff9800', ma60: '#9c27b0', ma120: '#607d8b' }

/* 종목 캔들 차트 + 이동평균 (TradingView lightweight-charts — 경량) */
export function PriceChart({ data, period = '1y', height = 360 }) {
  const box = useRef(null)
  useEffect(() => {
    const el = box.current
    if (!el || !data || !data.dates?.length) return
    const chart = createChart(el, { ...BASE, width: el.clientWidth, height })
    const lineOnly = period === '1d' || !data.open
    if (lineOnly) {
      const ln = chart.addSeries(LineSeries, { color: '#1976d2', lineWidth: 2 })
      ln.setData(data.dates.map((t, i) => ({ time: t, value: data.close?.[i] })).filter(p => p.value != null))
    } else {
      const candle = chart.addSeries(CandlestickSeries, {
        upColor: '#26a69a', downColor: '#ef5350', borderVisible: false,
        wickUpColor: '#26a69a', wickDownColor: '#ef5350',
      })
      candle.setData(data.dates.map((t, i) => ({
        time: t, open: data.open?.[i], high: data.high?.[i], low: data.low?.[i], close: data.close?.[i],
      })).filter(p => p.open != null && p.close != null))
      for (const [k, color] of Object.entries(MA)) {
        if (!data[k]) continue
        const line = chart.addSeries(LineSeries, { color, lineWidth: 1.4, priceLineVisible: false, lastValueVisible: false })
        line.setData(data.dates.map((t, i) => ({ time: t, value: data[k][i] })).filter(p => p.value != null))
      }
    }
    chart.timeScale().fitContent()
    const ro = new ResizeObserver(() => chart.applyOptions({ width: el.clientWidth }))
    ro.observe(el)
    return () => { ro.disconnect(); chart.remove() }
  }, [data, period, height])
  return (
    <div>
      <div ref={box} className="chartbox" />
      {(period !== '1d' && data?.open) && (
        <div className="legend">
          <span style={{ color: MA.ma20 }}>— MA20</span>
          <span style={{ color: MA.ma60 }}>— MA60</span>
          <span style={{ color: MA.ma120 }}>— MA120</span>
        </div>
      )}
    </div>
  )
}

/* 포트폴리오 누적수익률(%) — 다중 시리즈 + 구간 재정규화 */
const RANGES = { '1W': 7, '1M': 31, '3M': 95, '6M': 190, '1Y': 380, '최대': 1e9 }
const COLORS = ['#0a3d3a', '#9aa7a5', '#c9b072', '#7e9cc4', '#b48ead', '#88b04b']

export function PerfChart({ perf, height = 300 }) {
  const box = useRef(null)
  const [range, setRange] = useState('1M')
  const [last, setLast] = useState('')
  useEffect(() => {
    const el = box.current
    if (!el || !perf || !perf.dates?.length) return
    const n = perf.dates.length
    const cutMs = Date.now() - RANGES[range] * 86400000
    let from = 0
    for (let i = 0; i < n; i++) { if (new Date(perf.dates[i]).getTime() >= cutMs) { from = i; break }; from = i }
    if (RANGES[range] >= 1e9) from = 0
    const dates = perf.dates.slice(from)
    const chart = createChart(el, { ...BASE, width: el.clientWidth, height })
    const lastTxt = []
    Object.entries(perf.series).forEach(([name, vals], idx) => {
      const sl = vals.slice(from)
      const base = sl.find(v => v != null)
      const series = chart.addSeries(idx === 0 ? AreaSeries : LineSeries,
        idx === 0
          ? { lineColor: COLORS[0], topColor: 'rgba(10,61,58,0.18)', bottomColor: 'rgba(10,61,58,0.01)', lineWidth: 2, title: name }
          : { color: COLORS[idx % COLORS.length], lineWidth: 1.6, title: name })
      const pts = dates.map((t, i) => ({
        time: t, value: (sl[i] == null || base == null) ? null : Math.round((sl[i] - base) * 100) / 100,
      })).filter(p => p.value != null)
      series.setData(pts)
      if (pts.length) lastTxt.push(`${name} ${pts[pts.length - 1].value > 0 ? '+' : ''}${pts[pts.length - 1].value.toFixed(1)}%`)
    })
    setLast(`[${range}] ` + lastTxt.join(' · '))
    chart.timeScale().fitContent()
    const ro = new ResizeObserver(() => chart.applyOptions({ width: el.clientWidth }))
    ro.observe(el)
    return () => { ro.disconnect(); chart.remove() }
  }, [perf, range, height])
  return (
    <div>
      <div className="ranges">
        {Object.keys(RANGES).map(r => (
          <button key={r} className={r === range ? 'active' : ''} onClick={() => setRange(r)}>{r}</button>
        ))}
      </div>
      <div ref={box} className="chartbox" />
      {last && <div className="muted small" style={{ marginTop: '0.3rem' }}>{last}</div>}
    </div>
  )
}
