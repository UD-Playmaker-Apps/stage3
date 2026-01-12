Write-Host "=== BASELINE BENCHMARK ==="

# Configuración
$Url = "http://localhost:8080/search?q=the"
$Requests = 10
$ResultFile = "benchmarks/results/baseline.csv"

# Cabecera CSV
"test,requests,total_time_ms,avg_latency_ms" | Out-File $ResultFile -Encoding utf8

# Arrancar sistema
docker compose up -d
Start-Sleep -Seconds 20

# Medición
$sw = [System.Diagnostics.Stopwatch]::StartNew()

for ($i = 0; $i -lt $Requests; $i++) {
    Invoke-WebRequest $Url -UseBasicParsing | Out-Null
}

$sw.Stop()

$totalMs = $sw.ElapsedMilliseconds
$avgMs = [math]::Round($totalMs / $Requests, 2)

# Guardar resultados
"baseline,$Requests,$totalMs,$avgMs" | Add-Content $ResultFile

# Parar sistema
docker compose down

Write-Host "Baseline terminado"
Write-Host "Resultados en benchmarks/results/baseline.csv"
