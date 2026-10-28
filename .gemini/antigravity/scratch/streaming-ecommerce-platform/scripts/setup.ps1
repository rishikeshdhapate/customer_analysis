# ==============================================================================
# Streaming E-Commerce Platform - Windows Bootstrap Script (PowerShell)
# ==============================================================================

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  🛒 Streaming E-Commerce Data Platform Setup (Windows)    " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Environment file check
if (!(Test-Path ".env")) {
    Write-Host "[1/4] Copying .env.example to .env..." -ForegroundColor Yellow
    Copy-Item ".env.example" ".env"
} else {
    Write-Host "[1/4] Existing .env file found." -ForegroundColor Green
}

# 2. Check Docker availability
$dockerCmd = Get-Command docker -ErrorAction SilentlyContinue
if ($null -eq $dockerCmd) {
    Write-Host "[WARNING] Docker is not found in PATH." -ForegroundColor Red
    Write-Host "  You can still run the complete pipeline simulator locally using:" -ForegroundColor Yellow
    Write-Host "  python scripts/run_local_demo.py" -ForegroundColor Cyan
    exit 0
}

# 3. Spin up Docker Stack
Write-Host "[2/4] Starting all services via Docker Compose..." -ForegroundColor Green
docker compose up -d --build

Write-Host "[3/4] Waiting for services to achieve healthy state..." -ForegroundColor Green
Start-Sleep -Seconds 10

# 4. Show service endpoints
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  Platform Services are running! Access UIs below:        " -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  * Streamlit Dashboard:    http://localhost:8501" -ForegroundColor Yellow
Write-Host "  * Kafka Web UI:           http://localhost:8080" -ForegroundColor Yellow
Write-Host "  * MinIO Console (Lake):   http://localhost:9001 (minioadmin / minioadmin)" -ForegroundColor Yellow
Write-Host "  * Apache Airflow:         http://localhost:8085 (admin / admin)" -ForegroundColor Yellow
Write-Host "  * Apache Spark Master:    http://localhost:8081" -ForegroundColor Yellow
Write-Host "  * PostgreSQL Warehouse:   localhost:5432 (ecommerce_dw / postgres / postgres)" -ForegroundColor Yellow
Write-Host "==========================================================" -ForegroundColor Cyan
