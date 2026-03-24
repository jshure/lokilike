#!/usr/bin/env bash
set -euo pipefail

echo "=== sigyn quickstart ==="
echo ""

# Check prerequisites
command -v go >/dev/null 2>&1 || { echo "ERROR: go is required but not installed."; exit 1; }
command -v docker >/dev/null 2>&1 || { echo "ERROR: docker is required but not installed."; exit 1; }

echo "[1/5] Building..."
go build ./...

echo "[2/5] Starting MinIO and OpenSearch..."
docker compose up -d
echo "    Waiting for MinIO..."
until curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; do sleep 1; done
echo "    MinIO ready."

echo "[3/5] Starting ingester on :8082..."
go run ./cmd/ingester config.local.json &
INGESTER_PID=$!
sleep 2

echo "[4/5] Starting exporter on :8081..."
go run ./cmd/exporter config.local.json &
EXPORTER_PID=$!
sleep 2

echo "[5/5] Sending sample logs..."
curl -s -X POST http://localhost:8082/logs \
  -H "Content-Type: application/json" \
  -d '{
    "entries": [
      {"timestamp": "'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'", "service": "web", "level": "info", "message": "GET /index.html 200 12ms", "labels": {"app": "nginx", "env": "prod", "cluster": "us-west-2"}},
      {"timestamp": "'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'", "service": "web", "level": "warn", "message": "slow upstream response 1.2s", "labels": {"app": "nginx", "env": "prod", "cluster": "us-west-2"}},
      {"timestamp": "'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'", "service": "api", "level": "error", "message": "database connection timeout", "labels": {"app": "api-server", "env": "prod"}},
      {"timestamp": "'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'", "service": "api", "level": "info", "message": "POST /api/v1/users 201 45ms", "labels": {"app": "api-server", "env": "prod"}},
      {"timestamp": "'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'", "service": "worker", "level": "debug", "message": "processing batch 42", "labels": {"app": "batch-worker", "env": "staging"}}
    ]
  }' | jq . 2>/dev/null || true

echo ""
echo "=== sigyn is running ==="
echo ""
echo "  Web UI:        http://localhost:8081/ui"
echo "  Ingester:      http://localhost:8082 (POST /logs, WS /tail)"
echo "  Exporter:      http://localhost:8081 (GET /query, POST /export)"
echo "  MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
echo "  Metrics:       http://localhost:9090/metrics"
echo ""
echo "  Try: curl 'http://localhost:8081/query?start=2020-01-01T00:00:00Z&end=2030-01-01T00:00:00Z&limit=10'"
echo "  Try: wscat -c 'ws://localhost:8082/tail'"
echo ""
echo "Press Ctrl+C to stop."

trap "kill $INGESTER_PID $EXPORTER_PID 2>/dev/null; docker compose down; echo 'Stopped.'" EXIT
wait
