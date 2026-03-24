#!/usr/bin/env bash
set -euo pipefail

# Generate ~5000 test log entries and POST them to the sigyn ingester.
# Usage: ./scripts/gen-test-data.sh [ingester_url]
#   default ingester_url: http://localhost:8082

INGESTER="${1:-http://localhost:8082}"
BATCH=2500
TOTAL=5000
SENT=0

echo "Generating ${TOTAL} log entries -> ${INGESTER}/logs"

while (( SENT < TOTAL )); do
  remaining=$(( TOTAL - SENT ))
  batch=$(( remaining < BATCH ? remaining : BATCH ))

  payload=$(python3 -c "
import json, random, time

services = ['api-gateway','auth-service','user-service','payment-service','order-service',
            'notification-service','inventory-service','search-service','analytics-worker','cdn-edge']
levels = ['debug','info','info','info','info','warn','warn','error','error','fatal']
envs = ['prod','staging','dev']
regions = ['us-east-1','us-west-2','eu-west-1','ap-southeast-1']
clusters = ['primary','secondary','canary']
versions = ['v1.2.0','v1.2.1','v1.3.0-rc1','v1.1.9','v2.0.0']
api_paths = ['/api/v1/users','/api/v1/orders','/api/v1/payments','/api/v1/search','/api/v1/inventory',
         '/api/v1/auth/login','/api/v1/auth/refresh','/api/v1/notifications','/health','/ready',
         '/api/v2/users/bulk','/api/v1/orders/{id}/cancel','/api/v1/payments/webhook','/metrics']
codes = [200,200,200,200,201,204,301,400,401,403,404,500,502,503]

R = random.randint
C = random.choice
now = int(time.time())
day_ago = now - 86400

def msg(level, svc):
    p, code, ms = C(api_paths), C(codes), R(1,2500)
    uid = R(1000,99999)
    if level=='debug':
        return C([
            f'loading config from /etc/{svc}/config.yaml',
            f'cache lookup key=user:{uid} hit=true',
            f'connection pool stats: active={R(1,50)} idle={R(0,20)}',
            f'retry attempt {R(1,3)} for request {R(10000,99999)}',
            f'parsed request body bytes={R(50,5000)}',
            f'dns resolution for upstream.internal took {R(1,15)}ms',
            f'feature flag dark-mode-v2 evaluated: enabled=true',
            f'grpc stream opened client={R(1,255)}.{R(0,255)}.{R(0,255)}.{R(0,255)}',
        ])
    elif level=='info':
        return C([
            f'{p} {code} {ms}ms',
            f'request completed method=GET path={p} status={code} duration={ms}ms',
            f'user {uid} authenticated via oauth2',
            f'order ord-{R(100000,999999)} created total=\${R(10,500)}.{R(10,99)}',
            f'payment processed txn={R(100000,999999)} amount=\${R(5,1000)}.{R(10,99)}',
            f'email sent to user {uid} template=welcome',
            f'batch job completed processed={R(100,5000)} failed=0 duration={R(1,30)}s',
            f'inventory sync finished items={R(500,10000)} updated={R(0,200)}',
            f'search index refreshed segments={R(5,50)} docs={R(10000,500000)}',
            f'healthcheck passed latency={R(1,10)}ms',
            f'websocket connection established client_id=ws-{R(10000,99999)}',
            f'rate limiter reset bucket={svc} tokens={R(50,1000)}',
        ])
    elif level=='warn':
        return C([
            f'slow query duration={ms}ms threshold=500ms query=\"SELECT * FROM orders WHERE...\"',
            f'connection pool near capacity active={R(40,49)}/50',
            f'retry succeeded after {R(2,5)} attempts for {p}',
            f'deprecated api version called path={p} client=mobile-app/{C(versions)}',
            f'disk usage at {R(75,89)}% on /var/data',
            f'certificate expires in {R(5,30)} days cn={svc}.internal',
            f'response time degraded p99={R(800,3000)}ms baseline=200ms',
            f'upstream {svc}-db responded slowly latency={R(500,2000)}ms',
            f'memory usage elevated heap={R(70,90)}% gc_pause={R(10,50)}ms',
            f'request body exceeded soft limit size={R(1,10)}MB',
        ])
    elif level=='error':
        return C([
            f'request failed method=POST path={p} status=500 err=\"connection refused\"',
            f'database query failed err=\"deadlock detected\" table=orders retry=true',
            f'upstream timeout after {R(5,30)}s host=payments.internal:443',
            f'tls handshake failed remote={R(1,255)}.{R(0,255)}.{R(0,255)}.{R(0,255)}:{R(30000,65000)} err=\"certificate verify failed\"',
            f'kafka produce failed topic=events partition={R(0,11)} err=\"broker not available\"',
            f'redis command failed cmd=GET key=session:{R(10000,99999)} err=\"READONLY\"',
            f's3 upload failed bucket=logs key=chunk-{R(1000,9999)}.gz err=\"SlowDown\"',
            f'panic recovered handler={p} err=\"index out of range [{R(0,10)}] with length {R(0,5)}\"',
            f'circuit breaker opened service={svc}-upstream failures={R(5,20)} threshold=5',
            f'unhandled exception NullPointerException at {svc}/handler.go:{R(50,300)}',
        ])
    else:
        return C([
            f'failed to bind port :{R(3000,9000)} err=\"address already in use\"',
            f'database migration failed version={R(20,50)} err=\"column already exists\"',
            f'unable to load required secret vault_path=secret/{svc}/api-key',
            f'out of memory allocator failed size={R(100,500)}MB',
            f'data corruption detected table=events checksum mismatch block={R(1,100)}',
        ])

from datetime import datetime, timezone
entries = []
for _ in range($batch):
    svc = C(services)
    level = C(levels)
    ts = day_ago + R(0, 86399)
    labels = {'app': svc, 'env': C(envs), 'region': C(regions)}
    if R(0,1) == 0: labels['cluster'] = C(clusters)
    if R(0,2) == 0: labels['version'] = C(versions)
    if level in ('error','fatal') and R(0,1) == 0: labels['oncall'] = f'team-{C(services)}'
    entries.append({
        'timestamp': datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'service': svc,
        'level': level,
        'message': msg(level, svc),
        'labels': labels,
    })
print(json.dumps({'entries': entries}, separators=(',',':')))
")

  response=$(curl -s -w "\n%{http_code}" -X POST "${INGESTER}/logs" \
    -H "Content-Type: application/json" \
    -d "$payload")

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
