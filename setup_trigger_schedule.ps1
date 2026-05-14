# Biotech Radar — Price Trigger 체크 스케줄러
# 가격 트리거 발동 알림을 30분마다 + 부팅·로그온 직후 즉시 체크.
# daily_run보다 훨씬 가볍고 (yfinance fetch만) PC 켜진 직후 catch-up 가능.
#
# 사용법: 관리자 PowerShell에서
#   cd C:\Users\srkwn\biotech_radar
#   .\setup_trigger_schedule.ps1

$projectDir = "C:\Users\srkwn\biotech_radar"
$pythonExe  = "$projectDir\.venv\Scripts\python.exe"
$script     = "$projectDir\triggers_runner.py"
$taskName   = "BiotechRadarTriggers"

if (-not (Test-Path $pythonExe)) {
    Write-Error "Python 실행 파일을 찾을 수 없음: $pythonExe"
    exit 1
}
if (-not (Test-Path $script)) {
    Write-Error "스크립트를 찾을 수 없음: $script"
    exit 1
}

$action = New-ScheduledTaskAction `
    -Execute $pythonExe `
    -Argument "`"$script`"" `
    -WorkingDirectory $projectDir

# 트리거 정의
# 1) 30분마다 (전일 23:30 KST = 미국 09:30 ET 장 open, 다음 날 06:30 KST = 미국 16:30 ET 장 close 이후도 약간 더)
$cronTrigger = New-ScheduledTaskTrigger `
    -Once -At (Get-Date "00:00") `
    -RepetitionInterval (New-TimeSpan -Minutes 30) `
    -RepetitionDuration (New-TimeSpan -Days 365)
# 2) 부팅 직후 catch-up
$bootTrig = New-ScheduledTaskTrigger -AtStartup
$bootTrig.Delay = "PT2M"
# 3) 로그온 직후 catch-up
$logonTrig = New-ScheduledTaskTrigger -AtLogOn
$logonTrig.Delay = "PT1M"

$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -WakeToRun `
    -DontStopIfGoingOnBatteries `
    -AllowStartIfOnBatteries `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 5)

$principal = New-ScheduledTaskPrincipal `
    -UserId "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType Interactive `
    -RunLevel Limited

if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    Write-Host "기존 작업 삭제됨, 재등록..." -ForegroundColor Yellow
}

Register-ScheduledTask `
    -TaskName $taskName `
    -Description "Biotech Radar price trigger checks (30-min + boot/logon catch-up)" `
    -Action $action `
    -Trigger @($cronTrigger, $bootTrig, $logonTrig) `
    -Settings $settings `
    -Principal $principal | Out-Null

Write-Host ""
Write-Host "✓ 작업 등록 완료" -ForegroundColor Green
Write-Host "  이름:    $taskName"
Write-Host "  실행:    $pythonExe `"$script`""
Write-Host "  · 30분마다 반복 (PC 켜져있을 때)"
Write-Host "  · 부팅 시 (2분 지연)"
Write-Host "  · 로그온 시 (1분 지연)"
Write-Host "  활성 트리거가 있고 가격이 임계값 돌파하면 텔레그램 알림."
Write-Host "  status='fired' 마킹으로 중복 발송 차단."
Write-Host ""
Write-Host "확인:     schtasks /Query /TN $taskName /V /FO LIST"
Write-Host "수동실행: Start-ScheduledTask -TaskName $taskName"
Write-Host "삭제:     Unregister-ScheduledTask -TaskName $taskName -Confirm:`$false"
