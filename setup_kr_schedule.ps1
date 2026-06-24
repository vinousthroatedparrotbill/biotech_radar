# Biotech Radar (한국) — Windows 작업 스케줄러 등록
# KRX 종가(15:30) 직후 매일 15:35에 kr_daily_runner.py 실행
#   (KR universe 갱신 + 토스 52w 수집 + 텔레그램 한국 푸시)
# 16:30/18:00 catch-up + 로그온 트리거. 마커(data/.last_kr_run)로 같은 날 중복 차단.
#
# 사용법: PowerShell 관리자 권한으로 이 폴더에서  .\setup_kr_schedule.ps1
# 확인: schtasks /Query /TN BiotechRadarKR /V /FO LIST
# 삭제: Unregister-ScheduledTask -TaskName BiotechRadarKR -Confirm:$false

$projectDir = "C:\Users\srkwn\biotech_radar"
$pythonExe = "$projectDir\.venv\Scripts\python.exe"
$script    = "$projectDir\kr_daily_runner.py"
$taskName  = "BiotechRadarKR"

if (-not (Test-Path $pythonExe)) { Write-Error "Python 없음: $pythonExe"; exit 1 }
if (-not (Test-Path $script))    { Write-Error "스크립트 없음: $script"; exit 1 }

$action = New-ScheduledTaskAction -Execute $pythonExe -Argument "`"$script`"" -WorkingDirectory $projectDir

# 15:35 primary (종가 직후) + 16:30/18:00 catch-up + 로그온(2분 지연)
$t1 = New-ScheduledTaskTrigger -Daily -At 3:35pm
$t2 = New-ScheduledTaskTrigger -Daily -At 4:30pm
$t3 = New-ScheduledTaskTrigger -Daily -At 6:00pm
$logonTrig = New-ScheduledTaskTrigger -AtLogOn
$logonTrig.Delay = "PT2M"

$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable -WakeToRun -DontStopIfGoingOnBatteries -AllowStartIfOnBatteries `
    -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 45)

$principal = New-ScheduledTaskPrincipal `
    -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited

if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    Write-Host "기존 작업 삭제됨, 재등록..." -ForegroundColor Yellow
}

Register-ScheduledTask `
    -TaskName $taskName `
    -Description "Biotech Radar 한국 15:30 푸시 (15:35 + catch-up 16:30/18:00 + 로그온)" `
    -Action $action -Trigger @($t1, $t2, $t3, $logonTrig) `
    -Settings $settings -Principal $principal | Out-Null

Write-Host ""
Write-Host "[OK] 작업 등록 완료 — $taskName" -ForegroundColor Green
Write-Host "  · 매일 15:35 (KRX 종가 직후 primary)"
Write-Host "  · 16:30 / 18:00 (catch-up) · 로그온(2분 지연)"
Write-Host "  중복방지: kr_daily_runner.py가 data/.last_kr_run 마커로 차단"
Write-Host "수동실행: Start-ScheduledTask -TaskName $taskName"
