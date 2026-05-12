"""신규 신고가 종목 투자 메모 재발송 — universe/snapshot 스킵, 캐시된 high_low_cache 사용."""
import logging
logging.basicConfig(level=logging.INFO)

from collectors.high_low import fetch_new_today_highs
from telegram_report import send_investment_reports

df = fetch_new_today_highs(limit=100)
print(f"오늘 신규 신고가: {len(df)}건")
if df.empty:
    print("종목 없음 — 종료")
else:
    print(df[["ticker", "name", "market_cap"]].head(10).to_string())
    print()
    tickers = df["ticker"].tolist()
    sent = send_investment_reports(tickers, max_n=5)
    print(f"\n발송 청크 수: {sent}")
