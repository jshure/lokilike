#!/usr/bin/env bash
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=== sigyn test suite ==="
echo ""

# --- Step 1: Build ---
echo -e "${YELLOW}[1/4] Building...${NC}"
go build ./...
echo -e "${GREEN}  Build OK${NC}"

# --- Step 2: Vet ---
echo -e "${YELLOW}[2/4] Vet...${NC}"
go vet ./...
echo -e "${GREEN}  Vet OK${NC}"

# --- Step 3: Unit tests ---
echo -e "${YELLOW}[3/4] Unit tests...${NC}"
UNIT_OUTPUT=$(go test ./... -count=1 -v 2>&1)
PASS_COUNT=$(echo "$UNIT_OUTPUT" | grep -c "^--- PASS" || true)
FAIL_COUNT=$(echo "$UNIT_OUTPUT" | grep -c "^--- FAIL" || true)

if [ "$FAIL_COUNT" -gt 0 ]; then
  echo "$UNIT_OUTPUT"
  echo ""
  echo -e "${RED}  FAILED: $FAIL_COUNT test(s) failed${NC}"
  exit 1
fi
echo -e "${GREEN}  $PASS_COUNT tests passed${NC}"

# --- Step 4: Integration tests (optional) ---
echo -e "${YELLOW}[4/4] Integration tests...${NC}"
if curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; then
  INT_OUTPUT=$(go test -tags=integration ./internal/integration/ -v -count=1 2>&1)
  INT_PASS=$(echo "$INT_OUTPUT" | grep -c "^--- PASS" || true)
  INT_FAIL=$(echo "$INT_OUTPUT" | grep -c "^--- FAIL" || true)

  if [ "$INT_FAIL" -gt 0 ]; then
    echo "$INT_OUTPUT"
    echo -e "${RED}  FAILED: $INT_FAIL integration test(s) failed${NC}"
    exit 1
  fi
  echo -e "${GREEN}  $INT_PASS integration tests passed${NC}"
else
  echo -e "${YELLOW}  Skipped (MinIO not running — run 'make dev-up' first)${NC}"
fi

# --- Summary ---
echo ""
echo "==========================="
echo -e "${GREEN}All tests passed.${NC}"
echo ""

# Package coverage summary.
echo "Coverage by package:"
go test ./... -count=1 -cover 2>&1 | grep -E "^ok" | while read -r line; do
  pkg=$(echo "$line" | awk '{print $2}')
  cov=$(echo "$line" | grep -o 'coverage: [0-9.]*%' || echo "no test files")
  short=$(echo "$pkg" | sed 's|github.com/joel-shure/sigyn/||')
  printf "  %-40s %s\n" "$short" "$cov"
done || true
