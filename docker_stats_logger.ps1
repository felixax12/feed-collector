Param(
  [string]$ContainerName = "feed_clickhouse",
  [int]$IntervalSec = 10,
  [int]$DurationMin = 30,
  [string]$OutFile = "logs\docker_stats.log"
)

$dir = Split-Path -Parent $OutFile
if ($dir -and !(Test-Path $dir)) {
  New-Item -ItemType Directory -Force -Path $dir | Out-Null
}

$iterations = [int]([math]::Floor($DurationMin * 60 / $IntervalSec))
for ($i = 0; $i -lt $iterations; $i++) {
  $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  Add-Content -Path $OutFile -Value $ts
  $stats = docker stats --no-stream $ContainerName 2>&1
  Add-Content -Path $OutFile -Value $stats
  Add-Content -Path $OutFile -Value ""
  Start-Sleep -Seconds $IntervalSec
}
