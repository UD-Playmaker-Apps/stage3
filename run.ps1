param(
  [switch]$CleanVolumes,
  [int[]]$Docs = @(1342, 11),
  [int]$Limit = 5
)

$ErrorActionPreference = "Stop"

function Step([string]$msg) { Write-Host ""; Write-Host ("==> " + $msg) -ForegroundColor Cyan }
function Ok([string]$msg)   { Write-Host ("OK: " + $msg) -ForegroundColor Green }
function Warn([string]$msg) { Write-Host ("WARN: " + $msg) -ForegroundColor Yellow }
function Fail([string]$msg) { Write-Host ("FAIL: " + $msg) -ForegroundColor Red; throw $msg }

function DockerCompose([string[]]$ComposeArgs) {
  & docker compose @ComposeArgs
  if ($LASTEXITCODE -ne 0) {
    Fail ("docker compose failed: docker compose " + ($ComposeArgs -join " "))
  }
}

function TryJsonGet([string]$url) {
  try { return Invoke-RestMethod -Method GET -Uri $url -TimeoutSec 3 }
  catch { return $null }
}

function JsonGet([string]$url) {
  return Invoke-RestMethod -Method GET -Uri $url -TimeoutSec 10
}

function JsonPost([string]$url) {
  return Invoke-RestMethod -Method POST -Uri $url -TimeoutSec 20
}

function WaitHttpOk([string]$url, [int]$timeoutSec = 120) {
  $start = Get-Date
  while (((Get-Date) - $start).TotalSeconds -lt $timeoutSec) {
    try {
      $r = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 2
      if ($r.StatusCode -eq 200) { return $true }
    } catch {}
    Start-Sleep -Seconds 1
  }
  return $false
}

function WaitIngestCompleted([int]$docId, [int]$timeoutSec = 240) {
  $url = "http://localhost:7001/ingest/status/$docId"
  $start = Get-Date
  while (((Get-Date) - $start).TotalSeconds -lt $timeoutSec) {
    $st = TryJsonGet $url
    if ($st -ne $null) {
      if ($st.status -eq "COMPLETED") { return $true }
      if ($st.status -eq "FAILED") { Fail ("Ingest FAILED for doc " + $docId) }
    }
    Start-Sleep -Seconds 2
  }
  Fail ("Timeout waiting COMPLETED for doc " + $docId)
}

function WaitIndexedDocs([int]$expected, [int]$timeoutSec = 240) {
  $url = "http://localhost:7003/index/status"
  $start = Get-Date
  while (((Get-Date) - $start).TotalSeconds -lt $timeoutSec) {
    $st = TryJsonGet $url
    if ($st -ne $null) {
      if ([int]$st.indexedDocs -ge $expected) { return $st }
    }
    Start-Sleep -Seconds 2
  }
  Fail ("Timeout waiting indexedDocs >= " + $expected)
}

function TailLogs() {
  Warn "Tail logs (200 lines) for diagnostics..."
  $services = @("activemq","hazelcast","ingestion1","ingestion2","indexing1","search1","nginx")
  foreach ($svc in $services) {
    Write-Host ""
    Write-Host ("--- logs: " + $svc + " ---") -ForegroundColor DarkGray
    try { DockerCompose @("logs","--tail=200",$svc) | Out-Host } catch {}
  }
}

try {
  Step "Check docker compose"
  & docker compose version | Out-Host
  if ($LASTEXITCODE -ne 0) { Fail "Cannot run docker compose. Check Docker Desktop." }

  Step "Down stack"
  if ($CleanVolumes) {
    Warn "CleanVolumes enabled: removing volumes"
    DockerCompose @("down","-v")
  } else {
    DockerCompose @("down")
  }

  Step "Up stack (build + detach)"
  DockerCompose @("up","-d","--build")

  Step "Compose ps"
  DockerCompose @("ps") | Out-Host

  Step "Wait /health endpoints"
  $ok1 = WaitHttpOk "http://localhost:7001/health" 180
  $ok2 = WaitHttpOk "http://localhost:7003/health" 180
  $ok3 = WaitHttpOk "http://localhost:7004/health" 180

  if (-not ($ok1 -and $ok2 -and $ok3)) {
    TailLogs
    Fail "Some /health did not respond (7001/7003/7004)."
  }
  Ok "Health OK on 7001/7003/7004"

  Step ("Ingest docs: " + ($Docs -join ", "))
  foreach ($d in $Docs) {
    $null = JsonPost ("http://localhost:7001/ingest/" + $d)
    Ok ("Ingest requested: " + $d)
  }

  Step "Wait ingestion COMPLETED"
  foreach ($d in $Docs) {
    $null = WaitIngestCompleted $d 300
    Ok ("Ingest COMPLETED: " + $d)
  }

  Step "Wait indexing indexedDocs"
  $idx = WaitIndexedDocs $Docs.Count 300
  Ok ("Index OK: indexedDocs=" + $idx.indexedDocs + " terms=" + $idx.terms + " cluster=" + $idx.clusterName)

  Step "Index tests"
  $darcyDocs = JsonGet "http://localhost:7003/index/search?term=darcy"
  $theDocs   = JsonGet "http://localhost:7003/index/search?term=the"

  Write-Host ("darcy => " + ($darcyDocs | ConvertTo-Json -Compress))
  Write-Host ("the   => " + ($theDocs   | ConvertTo-Json -Compress))

  Step "Search score tests"
  $s1 = JsonGet ("http://localhost:7004/search?q=darcy&limit=" + $Limit)
  $s2 = JsonGet ("http://localhost:7004/search?q=darcy%20darcy&limit=" + $Limit)
  $s3 = JsonGet ("http://localhost:7004/search?q=the&limit=" + $Limit)

  Write-Host ("search darcy       => " + ($s1 | ConvertTo-Json -Depth 10 -Compress))
  Write-Host ("search darcy darcy => " + ($s2 | ConvertTo-Json -Depth 10 -Compress))
  Write-Host ("search the         => " + ($s3 | ConvertTo-Json -Depth 10 -Compress))

  if ([int]$s1.totalHits -lt 1) { TailLogs; Fail "Search darcy returned 0 hits" }
  if ([int]$s2.totalHits -lt 1) { TailLogs; Fail "Search darcy darcy returned 0 hits" }

  $score1 = [double]$s1.hits[0].score
  $score2 = [double]$s2.hits[0].score

  if ($score2 -le $score1) { TailLogs; Fail ("Expected score(darcy darcy) > score(darcy). Got " + $score2 + " <= " + $score1) }

  Ok ("Score OK: " + $score1 + " < " + $score2)
  Ok "DONE"

} catch {
  Write-Host ""
  Write-Host ("ERROR: " + $_.Exception.Message) -ForegroundColor Red
  TailLogs
  exit 1
}
