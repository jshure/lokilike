#!/usr/bin/env bash
set -euo pipefail

# Generate ~5000 test log entries and POST them to the sigyn ingester.
# Usage: ./scripts/gen-test-data.sh [ingester_url]
#   default ingester_url: http://localhost:3100

INGESTER="${1:-http://localhost:3100}"
BATCH=500
TOTAL=5000
SENT=0

services=("api-gateway" "auth-service" "user-service" "payment-service" "order-service"
          "notification-service" "inventory-service" "search-service" "analytics-worker" "cdn-edge")

levels=("debug" "info" "info" "info" "info" "warn" "warn" "error" "error" "fatal")

envs=("prod" "staging" "dev")
regions=("us-east-1" "us-west-2" "eu-west-1" "ap-southeast-1")
clusters=("primary" "secondary" "canary")
versions=("v1.2.0" "v1.2.1" "v1.3.0-rc1" "v1.1.9" "v2.0.0")

paths=("/api/v1/users" "/api/v1/orders" "/api/v1/payments" "/api/v1/search" "/api/v1/inventory"
       "/api/v1/auth/login" "/api/v1/auth/refresh" "/api/v1/notifications" "/health" "/ready"
       "/api/v2/users/bulk" "/api/v1/orders/{id}/cancel" "/api/v1/payments/webhook" "/metrics")

status_codes=(200 200 200 200 201 204 301 400 401 403 404 500 502 503)

pick() {
  local -n arr=$1
  echo "${arr[$((RANDOM % ${#arr[@]}))]}"
}

rand_range() {
  echo $(( $1 + RANDOM % ($2 - $1 + 1) ))
}

gen_message() {
  local level="$1" svc="$2"
  local path=$(pick paths)
  local code=$(pick status_codes)
  local ms=$(rand_range 1 2500)

  case "$level" in
    debug)
      local msgs=(
        "loading config from /etc/${svc}/config.yaml"
        "cache lookup key=user:$(rand_range 1000 9999) hit=true"
        "connection pool stats: active=$(rand_range 1 50) idle=$(rand_range 0 20)"
        "retry attempt $(rand_range 1 3) for request $(rand_range 10000 99999)"
        "parsed request body bytes=$(rand_range 50 5000)"
        "dns resolution for upstream.internal took $(rand_range 1 15)ms"
        "feature flag dark-mode-v2 evaluated: enabled=true"
        "grpc stream opened client=$(rand_range 1 255).$(rand_range 0 255).$(rand_range 0 255).$(rand_range 0 255)"
      ) ;;
    info)
      local msgs=(
        "${path} ${code} ${ms}ms"
        "request completed method=GET path=${path} status=${code} duration=${ms}ms"
        "user $(rand_range 1000 99999) authenticated via oauth2"
        "order ord-$(rand_range 100000 999999) created total=\$$(rand_range 10 500).$(rand_range 10 99)"
        "payment processed txn=$(rand_range 100000 999999) amount=\$$(rand_range 5 1000).$(rand_range 10 99)"
        "email sent to user $(rand_range 1000 99999) template=welcome"
        "batch job completed processed=$(rand_range 100 5000) failed=0 duration=$(rand_range 1 30)s"
        "inventory sync finished items=$(rand_range 500 10000) updated=$(rand_range 0 200)"
        "search index refreshed segments=$(rand_range 5 50) docs=$(rand_range 10000 500000)"
        "healthcheck passed latency=$(rand_range 1 10)ms"
        "websocket connection established client_id=ws-$(rand_range 10000 99999)"
        "rate limiter reset bucket=${svc} tokens=$(rand_range 50 1000)"
      ) ;;
    warn)
      local msgs=(
        "slow query duration=${ms}ms threshold=500ms query=\"SELECT * FROM orders WHERE...\""
        "connection pool near capacity active=$(rand_range 40 49)/50"
        "retry succeeded after $(rand_range 2 5) attempts for ${path}"
        "deprecated api version called path=${path} client=mobile-app/$(pick versions)"
        "disk usage at $(rand_range 75 89)% on /var/data"
        "certificate expires in $(rand_range 5 30) days cn=${svc}.internal"
        "response time degraded p99=$(rand_range 800 3000)ms baseline=200ms"
        "upstream ${svc}-db responded slowly latency=$(rand_range 500 2000)ms"
        "memory usage elevated heap=$(rand_range 70 90)% gc_pause=$(rand_range 10 50)ms"
        "request body exceeded soft limit size=$(rand_range 1 10)MB"
      ) ;;
    error)
      local msgs=(
        "request failed method=POST path=${path} status=500 err=\"connection refused\""
        "database query failed err=\"deadlock detected\" table=orders retry=true"
        "upstream timeout after $(rand_range 5 30)s host=payments.internal:443"
        "tls handshake failed remote=$(rand_range 1 255).$(rand_range 0 255).$(rand_range 0 255).$(rand_range 0 255):$(rand_range 30000 65000) err=\"certificate verify failed\""
        "kafka produce failed topic=events partition=$(rand_range 0 11) err=\"broker not available\""
        "redis command failed cmd=GET key=session:$(rand_range 10000 99999) err=\"READONLY\""
        "s3 upload failed bucket=logs key=chunk-$(rand_range 1000 9999).gz err=\"SlowDown\""
        "panic recovered handler=${path} err=\"index out of range [$(rand_range 0 10)] with length $(rand_range 0 5)\""
        "circuit breaker opened service=${svc}-upstream failures=$(rand_range 5 20) threshold=5"
        "unhandled exception NullPointerException at ${svc}/handler.go:$(rand_range 50 300)"
      ) ;;
    fatal)
      local msgs=(
        "failed to bind port :$(rand_range 3000 9000) err=\"address already in use\""
        "database migration failed version=$(rand_range 20 50) err=\"column already exists\""
        "unable to load required secret vault_path=secret/${svc}/api-key"
        "out of memory allocator failed size=$(rand_range 100 500)MB"
        "data corruption detected table=events checksum mismatch block=$(rand_range 1 100)"
      ) ;;
  esac
  echo "${msgs[$((RANDOM % ${#msgs[@]}))]}"
}

echo "Generating ${TOTAL} log entries -> ${INGESTER}/logs"

# Spread timestamps over the last 24 hours
now=$(date +%s)
day_ago=$(( now - 86400 ))

while (( SENT < TOTAL )); do
  remaining=$(( TOTAL - SENT ))
  batch=$(( remaining < BATCH ? remaining : BATCH ))
  entries="["

  for (( i=0; i<batch; i++ )); do
    svc=$(pick services)
    level=$(pick levels)
    env=$(pick envs)
    region=$(pick regions)
    cluster=$(pick clusters)
    version=$(pick versions)
    msg=$(gen_message "$level" "$svc")
    ts=$(( day_ago + RANDOM % 86400 ))
    ts_fmt=$(date -u -r "$ts" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d "@$ts" +%Y-%m-%dT%H:%M:%SZ)

    # Vary labels per entry - not every entry has every label
    labels="{\"app\":\"${svc}\",\"env\":\"${env}\",\"region\":\"${region}\""
    if (( RANDOM % 2 == 0 )); then labels+=",\"cluster\":\"${cluster}\""; fi
    if (( RANDOM % 3 == 0 )); then labels+=",\"version\":\"${version}\""; fi
    if [[ "$level" == "error" || "$level" == "fatal" ]] && (( RANDOM % 2 == 0 )); then
      labels+=",\"oncall\":\"team-$(pick services)\""
    fi
    labels+="}"

    entry="{\"timestamp\":\"${ts_fmt}\",\"service\":\"${svc}\",\"level\":\"${level}\",\"message\":$(printf '%s' "$msg" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))'),\"labels\":${labels}}"

    if (( i > 0 )); then entries+=","; fi
    entries+="$entry"
  done

  entries+="]"

  response=$(curl -s -w "\n%{http_code}" -X POST "${INGESTER}/logs" \
    -H "Content-Type: application/json" \
    -d "{\"entries\":${entries}}")

  http_code=$(echo "$response" | tail -1)
  body=$(echo "$response" | head -1)

  if [[ "$http_code" == "202" ]]; then
    SENT=$(( SENT + batch ))
    echo "  sent ${SENT}/${TOTAL} (batch=${batch}, status=${http_code})"
  else
    echo "  ERROR: status=${http_code} body=${body}"
    exit 1
  fi
done

echo "Done. ${TOTAL} entries sent to ${INGESTER}/logs"
