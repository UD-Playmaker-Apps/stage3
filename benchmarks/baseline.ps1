Write-Host "=== BASELINE BENCHMARK ==="

# Configuration
$Url = "http://localhost:8080/search?q=the"
$Requests = 10
$ResultFile = "benchmarks/results/baseline.csv"

# CSV Header
"test,requests,total_time_ms,avg_latency_ms" | Out-File $ResultFile -Encoding utf8

# Start the system
docker compose up -d
Start-Sleep -Seconds 20

# Measurement
$sw = [System.Diagnostics.Stopwatch]::StartNew()

for ($i = 0; $i -lt $Requests; $i++) {
    Invoke-WebRequest $Url -UseBasicParsing | Out-Null
}

$sw.Stop()

$totalMs = $sw.ElapsedMilliseconds
$avgMs = [math]::Round($totalMs / $Requests, 2)

# Save results
"baseline,$Requests,$totalMs,$avgMs" | Add-Content $ResultFile

# Stop system
docker compose down

Write-Host "Baseline finished"
Write-Host "Results saved in benchmarks/results/baseline.csv"