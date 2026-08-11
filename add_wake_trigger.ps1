# BiotechRadarDaily에 '절전 복귀(wake-from-sleep)' 이벤트 트리거 추가.
# 노트북이 절전 중 07/09/11/14시 트리거를 놓쳐도, 깨어난 직후 1분 뒤 daily_runner를
# 실행해 그날 리포트를 catch-up 발송한다(마커 가드로 이미 보냈으면 즉시 skip → 중복 없음).
# 관리자 권한 필요. 결과는 data/add_wake_trigger.result.txt 에 기록.
$ErrorActionPreference = 'Stop'
$result = 'C:\Users\srkwn\biotech_radar\data\add_wake_trigger.result.txt'
try {
  $svc = New-Object -ComObject Schedule.Service; $svc.Connect()
  $folder = $svc.GetFolder('\')
  $out = @()
  foreach ($name in 'BiotechRadarDaily') {
    $def = $folder.GetTask($name).Definition
    if ($def.Triggers | Where-Object { $_.Type -eq 0 }) {
      $out += "$name : event trigger already present — skipped"
    } else {
      $t = $def.Triggers.Create(0)   # 0 = TASK_TRIGGER_EVENT
      $t.Subscription = "<QueryList><Query Id='0' Path='System'><Select Path='System'>*[System[Provider[@Name='Microsoft-Windows-Power-Troubleshooter'] and (EventID=1)]]</Select></Query></QueryList>"
      $t.Delay = 'PT1M'
      $t.Id = 'WakeFromSleep'
      $t.Enabled = $true
      $folder.RegisterTaskDefinition($name, $def, 6, $null, $null, 3) | Out-Null
      $out += "$name : ADDED wake-from-sleep trigger (delay 1m)"
    }
    $trigs = ($folder.GetTask($name).Definition.Triggers | ForEach-Object { "type=$($_.Type)$(if($_.Id){'('+$_.Id+')'})" }) -join ', '
    $out += "  triggers now: $trigs"
  }
  ($out -join "`n") | Out-File -FilePath $result -Encoding utf8
} catch {
  "ERROR: $($_.Exception.Message)" | Out-File -FilePath $result -Encoding utf8
}
