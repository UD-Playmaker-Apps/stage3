Write-Host "=== FAULT TOLERANCE BENCHMARK ==="

$Url = "http://localhost:7004/search?q=the"
$DurationBefore = 10   # seconds
$DurationAfter  = 10   # seconds
$ResultFile = "benchmarks/results/fault.csv"

"phase,duration_s,successful_requests,avg_latency_ms" | Out-File $ResultFile -Encoding utf8

# Start the system
Write-Host "Starting system..."
docker compose up -d
Start-Sleep -Seconds 40

# Step before failure
Write-Host "Phase BEFORE failure..."

$latencies = @()
$success = 0
$endTime = (Get-Date).AddSeconds($DurationBefore)

while ((Get-Date) -lt $endTime) {
    try {
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        Invoke-WebRequest $Url -UseBasicParsing -TimeoutSec 5 | Out-Null
        $sw.Stop()

        $latencies += $sw.ElapsedMilliseconds
        $success++
    }
    catch {
        Start-Sleep -Milliseconds 200
    }
}

if ($success -gt 0) {
    $avgLatency = [math]::Round(($latencies | Measure-Object -Average).Average, 2)
} else {
    $avgLatency = -1
}

"before_failure,$DurationBefore,$success,$avgLatency" | Add-Content $ResultFile

# Simulate failure
Write-Host "Stopping search1 (simulated failure)..."
docker stop search1
Start-Sleep -Seconds 5

# Step after failure
Write-Host "Phase AFTER failure..."

$latencies = @()
$success = 0
$endTime = (Get-Date).AddSeconds($DurationAfter)

while ((Get-Date) -lt $endTime) {
    try {
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        Invoke-WebRequest $Url -UseBasicParsing -TimeoutSec 5 | Out-Null
        $sw.Stop()

        $latencies += $sw.ElapsedMilliseconds
        $success++
    }
    catch {
        Start-Sleep -Milliseconds 300
    }
}

if ($success -gt 0) {
    $avgLatency2 = [math]::Round(($latencies | Measure-Object -Average).Average, 2)
} else {
    $avgLatency2 = -1
}

"after_failure,$DurationAfter,$success,$avgLatency2" | Add-Content $ResultFile

# Step recovery
Write-Host "Restarting search1..."
docker start search1
Start-Sleep -Seconds 30

docker compose down

Write-Host "Fault tolerance benchmark finished."
Write-Host "Results saved in benchmarks/results/fault.csv"
