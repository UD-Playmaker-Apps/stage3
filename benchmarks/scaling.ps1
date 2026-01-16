Write-Host "=== SCALING BENCHMARK (LOAD-BASED, TIME-LIMITED) ==="

$Url = "http://localhost:7004/search?q=the"
$Durations = @(10, 20, 30)   # seconds
$ResultFile = "benchmarks/results/scaling.csv"

"duration_s,successful_requests,avg_latency_ms" | Out-File $ResultFile -Encoding utf8

# Start the full system only once
Write-Host "Starting full system..."
docker compose up -d
Start-Sleep -Seconds 40

foreach ($duration in $Durations) {

    Write-Host "Running load test for $duration seconds..."

    $successful = 0
    $latencies = @()

    $endTime = (Get-Date).AddSeconds($duration)

    while ((Get-Date) -lt $endTime) {
        try {
            $swReq = [System.Diagnostics.Stopwatch]::StartNew()
            Invoke-WebRequest $Url -UseBasicParsing -TimeoutSec 5 | Out-Null
            $swReq.Stop()

            $latencies += $swReq.ElapsedMilliseconds
            $successful++
        }
        catch {
            Start-Sleep -Milliseconds 200
        }
    }

    if ($successful -gt 0) {
        $avgLatency = [math]::Round(($latencies | Measure-Object -Average).Average, 2)
    }
    else {
        $avgLatency = -1
    }

    "$duration,$successful,$avgLatency" | Add-Content $ResultFile
}

docker compose down

Write-Host "Scaling benchmark finished."
Write-Host "Results saved in benchmarks/results/scaling.csv"
