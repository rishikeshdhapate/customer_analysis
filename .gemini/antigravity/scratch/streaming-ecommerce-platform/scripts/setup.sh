#!/usr/bin/env bash
# ==============================================================================
# Streaming E-Commerce Platform - Unix / Linux / macOS Bootstrap Script
# ==============================================================================

set -e

echo "=========================================================="
echo "  🛒 Streaming E-Commerce Data Platform Setup             "
echo "=========================================================="

if [ ! -f ".env" ]; then
    echo "[1/4] Copying .env.example to .env..."
    cp .env.example .env
else
    echo "[1/4] Existing .env file found."
fi

if ! command -v docker &> /dev/null; then
    echo "[WARNING] Docker not found in PATH."
    echo "  You can run the complete pipeline simulator locally using:"
    echo "  python scripts/run_local_demo.py"
    exit 0
fi

echo "[2/4] Starting Docker services..."
docker compose up -d --build

echo "[3/4] Waiting for services to initialize..."
sleep 10

echo "=========================================================="
echo "  Platform Services are running! Access UIs below:        "
echo "=========================================================="
echo "  * Streamlit Dashboard:    http://localhost:8501"
echo "  * Kafka Web UI:           http://localhost:8080"
echo "  * MinIO Console (Lake):   http://localhost:9001 (minioadmin / minioadmin)"
echo "  * Apache Airflow:         http://localhost:8085 (admin / admin)"
echo "  * Apache Spark Master:    http://localhost:8081"
echo "  * PostgreSQL Warehouse:   localhost:5432 (ecommerce_dw / postgres / postgres)"
echo "=========================================================="
