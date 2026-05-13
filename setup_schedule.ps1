# Biotech Radar — Windows 작업 스케줄러 등록
# 매일 오전 7시에 telegram_report.py를 실행 (universe 갱신 + 52w 수집 + 텔레그램 발송)
# PC가 7시에 꺼져있던 경우 다음 부팅·로그온 시 자동 catch-up 실행.
#
# 사용법:
#   PowerShell을 관리자 권한으로 열고 이 폴더에서:
#     .\setup_schedule.ps1
#
# 등록 후 확인: schtasks /Query /TN BiotechRadarDaily
# 삭제:        schtasks /Delete /TN BiotechRadarDaily /F

$projectDir = "C:\Users\srkwn\biotech_radar"
$pythonExe = "$projectDir\.venv\Scripts\python.exe"
$script    = "$projectDir\daily_runner.py"
$taskName  = "BiotechRadarDaily"

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

# 다중 트리거 — daily_runner.py가 마커로 중복 막아주므로 안전
#  · 매일 07:00 (primary)
#  · 매일 09:00 / 11:00 / 14:00 (catch-up — 7am 시점 PC 꺼져있었거나 sleep이면)
#  · 부팅 (AtStartup) — PC 켜질 때
#  · 로그온 (AtLogOn) — 로그인할 때
# 첫 성공 후엔 마커가 다른 트리거를 skip 시킴.
$trigger7  = New-ScheduledTaskTrigger -Daily -At 7:00am
$trigger9  = New-ScheduledTaskTrigger -Daily -At 9:00am
$trigger11 = New-ScheduledTaskTrigger -Daily -At 11:00am
$trigger14 = New-ScheduledTaskTrigger -Daily -At 2:00pm
$bootTrig  = New-ScheduledTaskTrigger -AtStartup
$logonTrig = New-ScheduledTaskTrigger -AtLogOn
# 부팅/로그온 트리거는 시스템 안정화 위해 2분 지연
$bootTrig.Delay = "PT2M"
$logonTrig.Delay = "PT2M"

# StartWhenAvailable — 미스 시 catchup
# WakeToRun — sleep 상태 PC 깨워서 실행 (있으면)
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -WakeToRun `
    -DontStopIfGoingOnBatteries `
    -AllowStartIfOnBatteries `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 45)

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
    -Description "Biotech Radar daily summary (7am + catch-ups at 9/11/2pm + boot/logon)" `
    -Action $action `
    -Trigger @($trigger7, $trigger9, $trigger11, $trigger14, $bootTrig, $logonTrig) `
    -Settings $settings `
    -Principal $principal | Out-Null

Write-Host ""
Write-Host "✓ 작업 등록 완료 (트리거 6개)" -ForegroundColor Green
Write-Host "  이름:     $taskName"
Write-Host "  실행:     $pythonExe `"$script`""
Write-Host "  · 매일 07:00 (primary)"
Write-Host "  · 매일 09:00 / 11:00 / 14:00 (catch-up)"
Write-Host "  · 부팅 시 (2분 지연)"
Write-Host "  · 로그온 시 (2분 지연)"
Write-Host "  중복방지: daily_runner.py가 data/.last_daily_run 마커로 같은 날 중복 실행 차단"
Write-Host ""
Write-Host "확인:     schtasks /Query /TN $taskName /V /FO LIST"
Write-Host "수동실행: Start-ScheduledTask -TaskName $taskName"
Write-Host "삭제:     Unregister-ScheduledTask -TaskName $taskName -Confirm:`$false"
