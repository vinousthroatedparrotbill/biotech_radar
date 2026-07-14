# Biotech Radar Telegram 봇 자동시작 등록 (관리자 권한 필요)
# 로그온/부팅 시 bot_service.ps1을 숨김 상주 실행 → 봇 항상 켜짐 + 크래시 자동 재시작.
# 데일리런(BiotechRadarDaily)과 동일한 "PC 켜지면 자동 기동" 방식.
#
# 사용: 관리자 PowerShell에서  .\setup_bot_autostart.ps1
# 삭제: Unregister-ScheduledTask -TaskName BiotechRadarBot -Confirm:$false

$proj = "C:\Users\srkwn\biotech_radar"
$svc  = "$proj\bot_service.ps1"
$name = "BiotechRadarBot"

if (-not (Test-Path $svc)) { Write-Error "bot_service.ps1 없음: $svc"; exit 1 }

$action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$svc`"" `
    -WorkingDirectory $proj

# 로그온 시 기동(1분 지연 — 네트워크 안정화). 부팅 트리거도 추가(대화형이라 실질 로그온에 발동).
$logon = New-ScheduledTaskTrigger -AtLogOn
$boot  = New-ScheduledTaskTrigger -AtStartup
$logon.Delay = "PT1M"
$boot.Delay  = "PT1M"

# 상주 프로세스 → 실행시간 무제한, 중복 인스턴스 금지(409 방지), 크래시 시 작업 자체 재시작 보강
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -DontStopIfGoingOnBatteries `
    -AllowStartIfOnBatteries `
    -MultipleInstances IgnoreNew `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1)
$settings.ExecutionTimeLimit = "PT0S"   # 무제한(상주)

$principal = New-ScheduledTaskPrincipal `
    -UserId "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType Interactive `
    -RunLevel Limited

if (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $name -Confirm:$false
    Write-Host "기존 $name 삭제 후 재등록..." -ForegroundColor Yellow
}

Register-ScheduledTask `
    -TaskName $name `
    -Description "Biotech Radar Telegram bot — 로그온/부팅 시 자동 기동 + 크래시 자동 재시작(상주)" `
    -Action $action -Trigger @($logon, $boot) -Settings $settings -Principal $principal | Out-Null
Write-Host "✓ $name 등록 완료 (로그온/부팅 자동 기동, 크래시 재시작)" -ForegroundColor Green

# 기존에 떠 있는 봇(수동/세션) 정리 → 중복 폴러(409) 방지 후, 지금 즉시 상주 봇 기동
Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.CommandLine -match 'telegram_bot' } |
    ForEach-Object { try { Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop; Write-Host "기존 봇 종료: PID $($_.ProcessId)" } catch {} }
Start-Sleep -Seconds 2
Start-ScheduledTask -TaskName $name
Write-Host "✓ 봇 상주 기동 시작됨 (data\bot_YYYYMMDD.log 확인)" -ForegroundColor Green
