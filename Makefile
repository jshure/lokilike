.PHONY: build test test-integration dev-up dev-down run-ingester run-exporter

build:
	go build ./...

test:
	go test ./... -count=1

test-integration: dev-up
	@echo "Waiting for MinIO to be ready..."
	@until curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; do sleep 1; done
	go test -tags=integration ./internal/integration/ -v -count=1

dev-up:
	docker compose up -d

dev-down:
	docker compose down -v

run-ingester:
	go run ./cmd/ingester config.local.json

run-exporter:
	go run ./cmd/exporter config.local.json
