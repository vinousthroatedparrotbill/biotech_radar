"""Sell-side급 재무 모델 xlsx 생성기.

설계 참고: JPM CJ제일제당 모델 + 삼성전자 실적추정 (sell-side standard).

구조 (공통):
  Cover / Summary / IS / BS / CF / Valuation_Band / SOTP / Notes
산업별 변동:
  Drivers / Segment_Buildup

산업 템플릿:
  - auto_parts: 자동차 생산대수 × 부품 ASP × 점유율
  - semis: B/G × ASP × utilization
  - saas: ARR × NRR × billings
  - cpg: category volume × ASP × raw material spread
  - biotech: peptides_count × price × penetration × ramp
  - general: 기본 driver (매출 성장률 + 마진 가정)

데이터 소스:
  - yfinance (미국 + 한국 .KS/.KQ — 제한적)
  - DART (한국 — 별도 API 키 필요, 미구현)
  - 사용자 driver 직접 입력 (xlsx 편집)
"""
from __future__ import annotations

import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path

import openpyxl
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

log = logging.getLogger(__name__)


# ─────────────────────── 스타일 ───────────────────────
HEADER_FONT = Font(name="Malgun Gothic", size=11, bold=True, color="FFFFFF")
HEADER_FILL = PatternFill("solid", fgColor="0A3D3A")
SUBHEADER_FONT = Font(name="Malgun Gothic", size=10, bold=True, color="1A237E")
SUBHEADER_FILL = PatternFill("solid", fgColor="E8EAF6")
LABEL_FONT = Font(name="Malgun Gothic", size=10, bold=True)
DATA_FONT = Font(name="Malgun Gothic", size=10)
INPUT_FONT = Font(name="Malgun Gothic", size=10, color="1976D2", bold=True)
INPUT_FILL = PatternFill("solid", fgColor="FFF9C4")   # 노란 — 사용자 입력 셀
FORMULA_FONT = Font(name="Malgun Gothic", size=10, color="2E7D32")
THIN = Side(border_style="thin", color="BBBBBB")
BORDER = Border(top=THIN, bottom=THIN, left=THIN, right=THIN)
CENTER = Alignment(horizontal="center", vertical="center")
RIGHT = Alignment(horizontal="right", vertical="center")
LEFT = Alignment(horizontal="left", vertical="center")
WRAP = Alignment(horizontal="left", vertical="top", wrap_text=True)


# ─────────────────────── 산업 템플릿 ───────────────────────
@dataclass
class IndustryTemplate:
    code: str
    label: str
    segments: list[str]
    drivers: list[tuple[str, str, str]]   # (segment, driver_name, unit)
    valuation_multiples: list[str]
    notes: str


INDUSTRY_TEMPLATES: dict[str, IndustryTemplate] = {
    "auto_parts": IndustryTemplate(
        code="auto_parts",
        label="자동차 부품",
        segments=["자동차부품", "방산", "기타"],
        drivers=[
            ("자동차부품", "글로벌 자동차 생산대수 (M units)", "M units"),
            ("자동차부품", "당사 점유율 (%)", "%"),
            ("자동차부품", "차량당 부품 수 (units)", "units"),
            ("자동차부품", "부품당 ASP (KRW)", "₩"),
            ("자동차부품", "GP 마진 (%)", "%"),
            ("자동차부품", "OP 마진 (%)", "%"),
            ("방산", "수주 잔고 (₩bn)", "₩bn"),
            ("방산", "연간 인식 비율 (%)", "%"),
            ("방산", "GP 마진 (%)", "%"),
            ("방산", "OP 마진 (%)", "%"),
            ("기타", "매출 (₩bn)", "₩bn"),
            ("기타", "OP 마진 (%)", "%"),
        ],
        valuation_multiples=["P/E", "P/B", "EV/EBITDA", "EV/Sales"],
        notes=(
            "자동차부품: 현대차/기아 글로벌 생산 사이클 + EV 전환 영향 모니터링.\n"
            "방산: 수주 잔고 기반 매출 가시성 확보. K2 소총·권총 수출 사이클.\n"
            "라이트 capex 사업이라 FCF 양호 — 순현금 포지션 확인."
        ),
    ),
    "semis": IndustryTemplate(
        code="semis",
        label="반도체",
        segments=["DRAM", "NAND", "Foundry", "기타"],
        drivers=[
            ("DRAM", "B/G QoQ (%)", "%"),
            ("DRAM", "ASP QoQ (%)", "%"),
            ("NAND", "B/G QoQ (%)", "%"),
            ("NAND", "ASP QoQ (%)", "%"),
            ("Foundry", "Utilization (%)", "%"),
            ("Foundry", "Wafer ASP ($)", "$"),
        ],
        valuation_multiples=["P/E", "P/B", "EV/EBITDA"],
        notes="Memory cycle 위치 + HBM mix + capex 동향 추적.",
    ),
    "saas": IndustryTemplate(
        code="saas",
        label="SaaS",
        segments=["Subscription", "Services"],
        drivers=[
            ("Subscription", "Net New ARR ($M)", "$M"),
            ("Subscription", "NRR (%)", "%"),
            ("Subscription", "Gross Margin (%)", "%"),
            ("Services", "Revenue ($M)", "$M"),
        ],
        valuation_multiples=["EV/Revenue", "EV/ARR", "P/FCF"],
        notes="Rule of 40, magic number, S&M efficiency 모니터링.",
    ),
    "biotech": IndustryTemplate(
        code="biotech",
        label="Biotech (commercial)",
        segments=["Product1", "Product2", "Pipeline"],
        drivers=[
            ("Product1", "Patients (K)", "K"),
            ("Product1", "Price per year ($K)", "$K"),
            ("Product1", "Penetration (%)", "%"),
            ("Product1", "GP 마진 (%)", "%"),
            ("Pipeline", "R&D ($M)", "$M"),
            ("Pipeline", "Probability of success (%)", "%"),
        ],
        valuation_multiples=["EV/Revenue", "EV/Peak Sales", "P/E (post-profitability)"],
        notes="Cash runway = (cash) / (quarterly burn). 임상 readout 카탈리스트 별도 추적.",
    ),
    "cpg": IndustryTemplate(
        code="cpg",
        label="Consumer / Food",
        segments=["Food", "Bio", "Logistics", "기타"],
        drivers=[
            ("Food", "Volume growth (%)", "%"),
            ("Food", "ASP growth (%)", "%"),
            ("Food", "GP 마진 (%)", "%"),
            ("Bio", "Lysine price ($/ton)", "$/ton"),
            ("Bio", "Methionine price ($/ton)", "$/ton"),
            ("Bio", "Volume (kton)", "kton"),
        ],
        valuation_multiples=["P/E", "EV/EBITDA", "SOTP"],
        notes="Raw material spread (옥수수/대두 vs 라이신/메티오닌) 모니터링.",
    ),
    "general": IndustryTemplate(
        code="general",
        label="General",
        segments=["Core"],
        drivers=[
            ("Core", "Revenue growth (%)", "%"),
            ("Core", "GP 마진 (%)", "%"),
            ("Core", "OP 마진 (%)", "%"),
        ],
        valuation_multiples=["P/E", "EV/EBITDA"],
        notes="기본 driver only — 산업 특화 필요시 다른 템플릿 사용.",
    ),
}


# ─────────────────────── 헬퍼 ───────────────────────
def _set_widths(ws, widths: dict[int, int]):
    for col, w in widths.items():
        ws.column_dimensions[get_column_letter(col)].width = w


def _hdr(ws, row: int, col: int, text: str, span: int = 1):
    """헤더 셀 (다크 그린 배경 + 흰 글씨)."""
    cell = ws.cell(row=row, column=col, value=text)
    cell.font = HEADER_FONT
    cell.fill = HEADER_FILL
    cell.alignment = CENTER
    cell.border = BORDER
    if span > 1:
        ws.merge_cells(start_row=row, start_column=col,
                       end_row=row, end_column=col + span - 1)


def _subhdr(ws, row: int, col: int, text: str, span: int = 1):
    cell = ws.cell(row=row, column=col, value=text)
    cell.font = SUBHEADER_FONT
    cell.fill = SUBHEADER_FILL
    cell.alignment = LEFT
    cell.border = BORDER
    if span > 1:
        ws.merge_cells(start_row=row, start_column=col,
                       end_row=row, end_column=col + span - 1)


def _label(ws, row: int, col: int, text: str, indent: int = 0):
    cell = ws.cell(row=row, column=col, value=("  " * indent) + text)
    cell.font = LABEL_FONT
    cell.alignment = LEFT
    cell.border = BORDER


def _data(ws, row: int, col: int, value, fmt: str = "#,##0.0"):
    cell = ws.cell(row=row, column=col, value=value)
    cell.font = DATA_FONT
    cell.alignment = RIGHT
    cell.border = BORDER
    if fmt:
        cell.number_format = fmt


def _input_cell(ws, row: int, col: int, value=None, fmt: str = "#,##0.00"):
    """사용자 편집용 (노란 셀)."""
    cell = ws.cell(row=row, column=col, value=value)
    cell.font = INPUT_FONT
    cell.fill = INPUT_FILL
    cell.alignment = RIGHT
    cell.border = BORDER
    if fmt:
        cell.number_format = fmt


def _formula_cell(ws, row: int, col: int, formula: str, fmt: str = "#,##0.0"):
    cell = ws.cell(row=row, column=col, value=formula)
    cell.font = FORMULA_FONT
    cell.alignment = RIGHT
    cell.border = BORDER
    if fmt:
        cell.number_format = fmt


def _periods(hist_years: list[int], proj_years: list[int]) -> list[str]:
    """주기 라벨: 2021 / 2022 / 2023 / 2024A / 2025E / 2026E ..."""
    out = []
    for y in hist_years:
        out.append(f"{y}A")
    for y in proj_years:
        out.append(f"{y}E")
    return out


# ─────────────────────── 시트 빌더 ───────────────────────
def _build_cover(wb, ticker: str, name: str, industry_label: str, analyst: str = ""):
    ws = wb.create_sheet("Cover", 0)
    _set_widths(ws, {1: 4, 2: 38, 3: 30})
    ws.row_dimensions[3].height = 30
    ws.merge_cells("B3:C3")
    h = ws.cell(row=3, column=2, value=f"{name} ({ticker})")
    h.font = Font(name="Malgun Gothic", size=18, bold=True, color="0A3D3A")
    h.alignment = LEFT

    rows = [
        ("Industry template", industry_label),
        ("Ticker", ticker),
        ("Company", name),
        ("Analyst", analyst or "—"),
        ("Model date", "=TEXT(TODAY(),\"yyyy-mm-dd\")"),
        ("", ""),
        ("Sheets", "Cover / Summary / IS / BS / CF / Drivers / "
                   "Segment_Buildup / Valuation_Band / SOTP / Notes"),
        ("Color code",
         "🟡 노란 셀 = 사용자 input · 🟢 녹색 = 자동 계산 · 일반 = historical actual"),
        ("Convention",
         "단위는 시트 좌상단 명시. 한국 ₩bn / 미국 $M 기본. 추정치 = 'E' 접미."),
        ("Reference",
         "JPM CJ제일제당 모델 + 삼성전자 실적추정 sell-side 표준 참고"),
    ]
    for i, (k, v) in enumerate(rows, start=5):
        _label(ws, i, 2, k)
        c = ws.cell(row=i, column=3, value=v)
        c.font = DATA_FONT
        c.alignment = LEFT
        c.border = BORDER


def _build_drivers(wb, template: IndustryTemplate, hist_years: list[int],
                    proj_years: list[int]):
    ws = wb.create_sheet("Drivers")
    periods = _periods(hist_years, proj_years)
    n_periods = len(periods)
    _set_widths(ws, {1: 4, 2: 22, 3: 32, 4: 10})
    for c in range(5, 5 + n_periods):
        ws.column_dimensions[get_column_letter(c)].width = 11

    _hdr(ws, 1, 2, f"📊 Drivers — {template.label}", span=3 + n_periods)
    _subhdr(ws, 2, 2, "Segment")
    _subhdr(ws, 2, 3, "Driver")
    _subhdr(ws, 2, 4, "Unit")
    for i, p in enumerate(periods):
        _subhdr(ws, 2, 5 + i, p)

    row = 3
    for segment, driver, unit in template.drivers:
        _label(ws, row, 2, segment)
        _label(ws, row, 3, driver)
        c = ws.cell(row=row, column=4, value=unit)
        c.font = DATA_FONT
        c.alignment = CENTER
        c.border = BORDER
        # 모든 기간은 사용자 input (노란 셀)
        for i in range(n_periods):
            _input_cell(ws, row, 5 + i, value=None)
        row += 1

    # 가이드
    row += 1
    _subhdr(ws, row, 2, "💡 가이드", span=3 + n_periods)
    row += 1
    note_lines = template.notes.split("\n")
    for line in note_lines:
        c = ws.cell(row=row, column=2, value=line)
        c.font = DATA_FONT
        c.alignment = WRAP
        ws.merge_cells(start_row=row, start_column=2,
                       end_row=row, end_column=4 + n_periods)
        row += 1


def _build_segment_buildup(wb, template: IndustryTemplate, hist_years: list[int],
                            proj_years: list[int]):
    ws = wb.create_sheet("Segment_Buildup")
    periods = _periods(hist_years, proj_years)
    n_periods = len(periods)
    _set_widths(ws, {1: 4, 2: 22, 3: 18})
    for c in range(4, 4 + n_periods):
        ws.column_dimensions[get_column_letter(c)].width = 12

    _hdr(ws, 1, 2, f"🧩 Segment Buildup — {template.label}", span=2 + n_periods)
    _subhdr(ws, 2, 2, "Segment")
    _subhdr(ws, 2, 3, "Line item")
    for i, p in enumerate(periods):
        _subhdr(ws, 2, 4 + i, p)

    row = 3
    for segment in template.segments:
        _label(ws, row, 2, segment)
        for line in ("Revenue", "GP", "OP", "OPM (%)"):
            _label(ws, row, 3, line, indent=1)
            for i in range(n_periods):
                _input_cell(ws, row, 4 + i, value=None,
                            fmt="0.0%" if "OPM" in line else "#,##0.0")
            row += 1
        # 1줄 공백
        row += 1

    # 합계 (Total) — formula
    _subhdr(ws, row, 2, "Total")
    for line in ("Revenue", "GP", "OP", "GP margin (%)", "OP margin (%)"):
        _label(ws, row, 3, line, indent=1)
        for i in range(n_periods):
            col_letter = get_column_letter(4 + i)
            # 각 segment의 같은 line 합산 — 단순화: 사용자가 SUM() 직접 추가 권장
            _input_cell(ws, row, 4 + i, value=None,
                        fmt="0.0%" if "%" in line else "#,##0.0")
        row += 1


def _build_3statements(wb, hist_years: list[int], proj_years: list[int],
                       unit: str = "₩bn"):
    """IS / BS / CF — 표준 계정 (산업 무관)."""
    periods = _periods(hist_years, proj_years)
    n_periods = len(periods)

    # === IS ===
    ws = wb.create_sheet("IS")
    _set_widths(ws, {1: 4, 2: 32})
    for c in range(3, 3 + n_periods):
        ws.column_dimensions[get_column_letter(c)].width = 12
    _hdr(ws, 1, 2, f"💰 Income Statement ({unit})", span=1 + n_periods)
    for i, p in enumerate(periods):
        _subhdr(ws, 2, 3 + i, p)

    is_lines = [
        ("Revenue", "label"),
        ("(-) COGS", "input"),
        ("Gross Profit", "formula", "=Revenue - COGS"),
        ("GP Margin (%)", "formula", "=GP / Revenue"),
        ("", ""),
        ("(-) SG&A", "input"),
        ("(-) R&D", "input"),
        ("Operating Profit", "formula", "=GP - SG&A - R&D"),
        ("OP Margin (%)", "formula"),
        ("", ""),
        ("(+) 금융수익", "input"),
        ("(-) 금융비용", "input"),
        ("(+) 지분법손익", "input"),
        ("(+/-) 기타 영업외", "input"),
        ("Pre-tax Profit (PBT)", "formula"),
        ("(-) Income tax", "input"),
        ("Net Income", "formula"),
        ("(-) Minority interest", "input"),
        ("Net Income (지배)", "formula"),
        ("", ""),
        ("EPS (KRW or $)", "formula"),
        ("Diluted EPS", "formula"),
        ("DPS", "input"),
        ("Payout ratio (%)", "formula"),
        ("", ""),
        ("EBITDA", "formula", "=OP + D&A"),
        ("EBITDA Margin (%)", "formula"),
    ]
    row = 3
    for line in is_lines:
        if line[0] == "":
            row += 1
            continue
        kind = line[1]
        _label(ws, row, 2, line[0])
        for i in range(n_periods):
            if kind == "label":
                _input_cell(ws, row, 3 + i)
            elif kind == "input":
                _input_cell(ws, row, 3 + i)
            elif kind == "formula":
                # placeholder — 사용자가 직접 formula 입력
                _formula_cell(ws, row, 3 + i, "")
        row += 1

    # === BS ===
    ws = wb.create_sheet("BS")
    _set_widths(ws, {1: 4, 2: 32})
    for c in range(3, 3 + n_periods):
        ws.column_dimensions[get_column_letter(c)].width = 12
    _hdr(ws, 1, 2, f"📋 Balance Sheet ({unit})", span=1 + n_periods)
    for i, p in enumerate(periods):
        _subhdr(ws, 2, 3 + i, p)

    bs_groups = [
        ("[자산]", "subheader"),
        ("현금성 자산", "input"),
        ("매출채권", "input"),
        ("재고자산", "input"),
        ("기타 유동자산", "input"),
        ("유동자산 합계", "formula"),
        ("유형자산 (PP&E)", "input"),
        ("무형자산", "input"),
        ("투자자산", "input"),
        ("기타 비유동자산", "input"),
        ("비유동자산 합계", "formula"),
        ("자산 총계", "formula"),
        ("", ""),
        ("[부채]", "subheader"),
        ("단기차입금", "input"),
        ("매입채무", "input"),
        ("기타 유동부채", "input"),
        ("유동부채 합계", "formula"),
        ("장기차입금", "input"),
        ("사채", "input"),
        ("기타 비유동부채", "input"),
        ("비유동부채 합계", "formula"),
        ("부채 총계", "formula"),
        ("", ""),
        ("[자본]", "subheader"),
        ("자본금", "input"),
        ("자본잉여금", "input"),
        ("이익잉여금", "input"),
        ("기타 자본구성요소", "input"),
        ("자본 총계", "formula"),
        ("", ""),
        ("Net Debt (Cash)", "formula"),
        ("ROE (%)", "formula"),
        ("Debt/Equity (%)", "formula"),
    ]
    row = 3
    for line in bs_groups:
        if line[0] == "":
            row += 1
            continue
        kind = line[1]
        if kind == "subheader":
            _subhdr(ws, row, 2, line[0], span=1 + n_periods)
            row += 1
            continue
        _label(ws, row, 2, line[0])
        for i in range(n_periods):
            if kind == "input":
                _input_cell(ws, row, 3 + i)
            else:
                _formula_cell(ws, row, 3 + i, "")
        row += 1

    # === CF ===
    ws = wb.create_sheet("CF")
    _set_widths(ws, {1: 4, 2: 32})
    for c in range(3, 3 + n_periods):
        ws.column_dimensions[get_column_letter(c)].width = 12
    _hdr(ws, 1, 2, f"💵 Cash Flow Statement ({unit})", span=1 + n_periods)
    for i, p in enumerate(periods):
        _subhdr(ws, 2, 3 + i, p)

    cf_groups = [
        ("[영업활동]", "subheader"),
        ("Net Income", "formula"),
        ("(+) D&A", "input"),
        ("(+) 운전자본 변동", "input"),
        ("(+/-) 기타", "input"),
        ("영업활동 CF", "formula"),
        ("", ""),
        ("[투자활동]", "subheader"),
        ("(-) Capex", "input"),
        ("(-) 무형자산 취득", "input"),
        ("(+/-) 투자자산 매매", "input"),
        ("투자활동 CF", "formula"),
        ("", ""),
        ("[재무활동]", "subheader"),
        ("(+/-) 차입금 증감", "input"),
        ("(-) 배당금", "input"),
        ("(+/-) 자기주식", "input"),
        ("재무활동 CF", "formula"),
        ("", ""),
        ("Net Cash Change", "formula"),
        ("Cash, beginning", "input"),
        ("Cash, ending", "formula"),
        ("", ""),
        ("Free Cash Flow (영업 - Capex)", "formula"),
        ("FCF margin (%)", "formula"),
    ]
    row = 3
    for line in cf_groups:
        if line[0] == "":
            row += 1
            continue
        kind = line[1]
        if kind == "subheader":
            _subhdr(ws, row, 2, line[0], span=1 + n_periods)
            row += 1
            continue
        _label(ws, row, 2, line[0])
        for i in range(n_periods):
            if kind == "input":
                _input_cell(ws, row, 3 + i)
            else:
                _formula_cell(ws, row, 3 + i, "")
        row += 1


def _build_valuation_band(wb, template: IndustryTemplate, hist_years: list[int],
                            proj_years: list[int]):
    ws = wb.create_sheet("Valuation_Band")
    periods = _periods(hist_years, proj_years)
    n_periods = len(periods)
    _set_widths(ws, {1: 4, 2: 24})
    for c in range(3, 3 + n_periods):
        ws.column_dimensions[get_column_letter(c)].width = 11

    _hdr(ws, 1, 2, "📈 Valuation Band — Historical multiples + projections",
         span=1 + n_periods)
    for i, p in enumerate(periods):
        _subhdr(ws, 2, 3 + i, p)

    row = 3
    for m in template.valuation_multiples:
        _label(ws, row, 2, m)
        for i in range(n_periods):
            _input_cell(ws, row, 3 + i, fmt="#,##0.00")
        row += 1

    row += 1
    _subhdr(ws, row, 2, "Statistical bands", span=1 + n_periods)
    row += 1
    for stat in ("Avg (5y)", "Median (5y)", "1 σ ±", "Min", "Max"):
        _label(ws, row, 2, stat)
        for i in range(n_periods):
            _input_cell(ws, row, 3 + i, fmt="#,##0.00")
        row += 1

    row += 1
    _subhdr(ws, row, 2, "Implied target price", span=1 + n_periods)
    row += 1
    for line in ("Target multiple", "EPS / EBITDA", "Implied price",
                 "Upside (%)"):
        _label(ws, row, 2, line)
        for i in range(n_periods):
            _input_cell(ws, row, 3 + i,
                        fmt="0.0%" if "Upside" in line else "#,##0.00")
        row += 1


def _build_sotp(wb, template: IndustryTemplate):
    ws = wb.create_sheet("SOTP")
    _set_widths(ws, {1: 4, 2: 22, 3: 12, 4: 14, 5: 14, 6: 14, 7: 12, 8: 14})
    _hdr(ws, 1, 2, "💎 Sum-of-the-parts Valuation", span=7)
    headers = ["Segment", "Metric", "Value", "Multiple", "EV",
               "Stake (%)", "Attributable EV"]
    for i, h in enumerate(headers):
        _subhdr(ws, 2, 2 + i, h)

    row = 3
    for seg in template.segments:
        _label(ws, row, 2, seg)
        for i in range(2, 8):
            _input_cell(ws, row, 1 + i)
        row += 1

    row += 1
    _subhdr(ws, row, 2, "Operating EV (합계)", span=2)
    for i in range(2, 8):
        _input_cell(ws, row, 1 + i)
    row += 1

    for line in ("(+) Net cash", "(+) Investments at market",
                 "(-) Minority interest", "Equity value",
                 "Total shares (m)", "Target price", "Current price",
                 "Upside (%)"):
        _label(ws, row, 2, line)
        for i in range(2, 8):
            _input_cell(ws, row, 1 + i,
                        fmt="0.0%" if "Upside" in line else "#,##0.0")
        row += 1


def _build_summary(wb, template: IndustryTemplate, hist_years: list[int],
                    proj_years: list[int]):
    ws = wb.create_sheet("Summary", 1)
    periods = _periods(hist_years, proj_years)
    n_periods = len(periods)
    _set_widths(ws, {1: 4, 2: 26})
    for c in range(3, 3 + n_periods):
        ws.column_dimensions[get_column_letter(c)].width = 11

    _hdr(ws, 1, 2, "📊 Summary — Key financials & KPI", span=1 + n_periods)
    for i, p in enumerate(periods):
        _subhdr(ws, 2, 3 + i, p)

    rows = [
        ("[손익]", "subheader"),
        "Revenue (₩bn)",
        "  YoY (%)",
        "GP",
        "OP",
        "  OPM (%)",
        "EBITDA",
        "  EBITDA Margin (%)",
        "Net Income",
        "EPS (KRW)",
        ("[현금흐름·재무건전성]", "subheader"),
        "OCF",
        "Capex",
        "FCF",
        "Net Debt (Cash)",
        ("[밸류에이션·KPI]", "subheader"),
        "Market cap (₩bn)",
        "EV",
        "P/E",
        "EV/EBITDA",
        "P/B",
        "ROE (%)",
        "Div yield (%)",
    ]
    row = 3
    for r in rows:
        if isinstance(r, tuple):
            _subhdr(ws, row, 2, r[0], span=1 + n_periods)
            row += 1
            continue
        _label(ws, row, 2, r)
        for i in range(n_periods):
            _input_cell(ws, row, 3 + i,
                        fmt="0.0%" if "%" in r else "#,##0.0")
        row += 1


def _build_notes(wb, template: IndustryTemplate):
    ws = wb.create_sheet("Notes")
    _set_widths(ws, {1: 4, 2: 90})
    _hdr(ws, 1, 2, "📝 Notes & Assumptions")
    row = 3
    notes_blocks = [
        ("Industry template:", template.label + " (" + template.code + ")"),
        ("Segments:", " · ".join(template.segments)),
        ("Key drivers:", "; ".join(f"{s}-{d}" for s, d, _ in template.drivers[:10])),
        ("Notes:", template.notes),
        ("", ""),
        ("Color code:",
         "🟡 노란 셀 = 사용자 input · 🟢 녹색 = 자동 계산 / formula · "
         "일반 = historical actual"),
        ("Workflow:",
         "1) Drivers 시트 — 분기/연간 driver 입력\n"
         "2) Segment_Buildup — 사업부별 매출/OP 계산\n"
         "3) IS/BS/CF — segment 합산 → consol 재무제표\n"
         "4) Valuation_Band — historical multiple 입력 + 평균/σ 자동 계산\n"
         "5) SOTP — 사업부별 EV 가산 → 목표주가\n"
         "6) Summary — KPI 한 페이지 정리"),
        ("References:",
         "JPM CJ제일제당 모델 (Youna Kim, 1Q23) — Earnings Review·Band·SOTP\n"
         "삼성전자 실적추정 — driver→quarterly chain · DS/MX/Display 세그먼트"),
    ]
    for title, body in notes_blocks:
        if title:
            c = ws.cell(row=row, column=2, value=title)
            c.font = SUBHEADER_FONT
            row += 1
        if body:
            c = ws.cell(row=row, column=2, value=body)
            c.font = DATA_FONT
            c.alignment = WRAP
            ws.row_dimensions[row].height = max(20, body.count("\n") * 18 + 20)
            row += 1
        row += 1


# ─────────────────────── 통합 ───────────────────────
def generate_model_xlsx(
    ticker: str,
    name: str,
    industry: str = "general",
    hist_years: list[int] | None = None,
    proj_years: list[int] | None = None,
    unit: str = "₩bn",
    analyst: str = "",
    out_path: str | None = None,
) -> str:
    """sell-side급 빈 모델 xlsx 생성. 파일 경로 반환.

    Args:
        ticker: 종목 코드 (예: '064960.KS')
        name: 회사명 (예: 'SNT Motiv')
        industry: 'auto_parts' / 'semis' / 'saas' / 'biotech' / 'cpg' / 'general'
        hist_years: historical 연도 list (기본: 최근 5년)
        proj_years: projection 연도 list (기본: 향후 3년)
        unit: '₩bn' 또는 '$M'
        analyst: 분석가 이름
        out_path: 저장 경로. None이면 temp file.
    """
    import datetime as _dt
    if hist_years is None:
        cy = _dt.date.today().year
        hist_years = [cy - 5, cy - 4, cy - 3, cy - 2, cy - 1]
    if proj_years is None:
        cy = _dt.date.today().year
        proj_years = [cy, cy + 1, cy + 2]

    template = INDUSTRY_TEMPLATES.get(industry, INDUSTRY_TEMPLATES["general"])

    wb = openpyxl.Workbook()
    # default sheet 제거
    wb.remove(wb.active)

    _build_cover(wb, ticker, name, template.label, analyst=analyst)
    _build_summary(wb, template, hist_years, proj_years)
    _build_3statements(wb, hist_years, proj_years, unit=unit)
    _build_drivers(wb, template, hist_years, proj_years)
    _build_segment_buildup(wb, template, hist_years, proj_years)
    _build_valuation_band(wb, template, hist_years, proj_years)
    _build_sotp(wb, template)
    _build_notes(wb, template)

    if out_path is None:
        out_path = tempfile.NamedTemporaryFile(
            prefix=f"model_{ticker.replace('.', '_')}_",
            suffix=".xlsx", delete=False,
        ).name
    wb.save(out_path)
    log.info("model xlsx saved: %s (%d sheets)", out_path, len(wb.sheetnames))
    return out_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # SNT Motiv (064960.KS) — auto_parts 템플릿으로 빌드
    path = generate_model_xlsx(
        ticker="064960.KS",
        name="SNT Motiv",
        industry="auto_parts",
        unit="₩bn",
        analyst="기계/자동차",
    )
    print(f"\n✓ xlsx 생성 완료: {path}")
    print(f"  사이즈: {Path(path).stat().st_size:,} bytes")
