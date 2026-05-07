# Biotech Radar — Windows 작업 스케줄러 등록
# 매일 오전 7시에 telegram_report.py를 실행 (universe 갱신 + 52w 수집 + 텔레그램 발송)
#
# 사용법:
#   PowerShell을 관리자 권한으로 열고 이 폴더에서:
#     .\setup_schedule.ps1
#
# 등록 후 확인: schtasks /Query /TN BiotechRadarDaily
# 삭제:        schtasks /Delete /TN BiotechRadarDaily /F

$projectDir = "C:\Users\srkwn\biotech_radar"
$pythonExe = "$projectDir\.venv\Scripts\python.exe"
$script    = "$projectDir\telegram_report.py"
$taskName  = "BiotechRadarDaily"
$logFile   = "$projectDir\data\daily.log"

if (-not (Test-Path $pythonExe)) {
    Write-Error "Python 실행 파일을 찾을 수 없음: $pythonExe"
    exit 1
}
if (-not (Test-Path $script)) {
    Write-Error "스크립트를 찾을 수 없음: $script"
    exit 1
}

# 작업 정의
$action = New-ScheduledTaskAction `
    -Execute $pythonExe `
    -Argument "`"$script`"" `
    -WorkingDirectory $projectDir

$trigger = New-ScheduledTaskTrigger -Daily -At 7:00am

$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -DontStopIfGoingOnBatteries `
    -AllowStartIfOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30)

$principal = New-ScheduledTaskPrincipal `
    -UserId "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType Interactive `
    -RunLevel Limited

# 기존 작업 있으면 갱신
if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    Write-Host "기존 작업 삭제됨, 재등록..." -ForegroundColor Yellow
}

Register-ScheduledTask `
    -TaskName $taskName `
    -Description "Biotech Radar daily 7am summary to Telegram" `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal | Out-Null

Write-Host ""
Write-Host "✓ 작업 등록 완료" -ForegroundColor Green
Write-Host "  이름:    $taskName"
Write-Host "  실행:    $pythonExe `"$script`""
Write-Host "  트리거:  매일 07:00"
Write-Host ""
Write-Host "확인:    schtasks /Query /TN $taskName /V /FO LIST"
Write-Host "수동실행: Start-ScheduledTask -TaskName $taskName"
Write-Host "삭제:    Unregister-ScheduledTask -TaskName $taskName -Confirm:`$false"
