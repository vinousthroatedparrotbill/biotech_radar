# Biotech Radar Telegram 봇 — 상주 서비스 래퍼
# 스케줄 작업(BiotechRadarBot)이 로그온/부팅 시 이 스크립트를 숨김 실행한다.
# 봇이 종료(크래시/네트워크 등)되면 10초 후 자동 재시작. 로그는 data\bot_YYYYMMDD.log.
$proj   = "C:\Users\srkwn\biotech_radar"
$py     = "$proj\.venv\Scripts\python.exe"
$script = "$proj\telegram_bot.py"

$env:PYTHONUNBUFFERED = "1"
Set-Location $proj

while ($true) {
    $log = "$proj\data\bot_$(Get-Date -Format 'yyyyMMdd').log"
    # cmd 리다이렉션으로 네이티브 stdout+stderr를 안정적으로 로그에 적재
    cmd /c "`"$py`" `"$script`" >> `"$log`" 2>&1"
    # 여기 도달 = 봇 프로세스 종료됨 → 잠시 후 재기동
    Start-Sleep -Seconds 10
}
