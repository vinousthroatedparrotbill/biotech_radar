# Telegram bot listener 실행
# 사용: .\run_bot.ps1
# 종료: 터미널에서 Ctrl+C

$projectDir = "C:\Users\srkwn\biotech_radar"
$pythonExe = "$projectDir\.venv\Scripts\python.exe"
$script = "$projectDir\telegram_bot.py"

if (-not (Test-Path $pythonExe)) {
    Write-Error "Python 실행 파일 없음: $pythonExe"
    exit 1
}

$env:PYTHONUNBUFFERED = "1"   # 로그 즉시 출력
Set-Location $projectDir

Write-Host "Telegram bot 시작 (Ctrl+C로 종료)..." -ForegroundColor Green
& $pythonExe $script
