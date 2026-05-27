#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${AEGIS_API_BASE_URL:-http://127.0.0.1:3100}"
DASHBOARD_URL="${AEGIS_DASHBOARD_URL:-http://127.0.0.1:3101}"
ACCESS_TOKEN="${AEGIS_ACCESS_TOKEN:-}"
RESEARCH_PLAN_ID="${AEGIS_RESEARCH_PLAN_ID:-}"
WITH_RESEARCH_RUN=0

usage() {
  cat <<'USAGE'
Usage: scripts/verify-research-loop.sh [--with-research-run]

Default mode is safe:
  - checks API health
  - checks dashboard HTTP status if reachable
  - calls read-only research endpoints when possible
  - never submits orders
  - never mutates execution tables

Optional --with-research-run:
  - requires AEGIS_ACCESS_TOKEN and AEGIS_RESEARCH_PLAN_ID
  - previews the existing research experiment plan
  - runs it with exact confirmation
  - verifies execution table counts are unchanged

Environment:
  AEGIS_API_BASE_URL      default http://127.0.0.1:3100
  AEGIS_DASHBOARD_URL    default http://127.0.0.1:3101
  AEGIS_ACCESS_TOKEN     bearer token for authenticated endpoints
  AEGIS_RESEARCH_PLAN_ID existing plan ID for --with-research-run
  DATABASE_URL           optional; otherwise docker exec against aegis-quant-postgres is attempted
USAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --with-research-run)
      WITH_RESEARCH_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

curl_status() {
  local method="$1"
  local path="$2"
  local data="${3:-}"
  local url="$API_BASE_URL$path"

  if [ -n "$data" ]; then
    if [ -n "$ACCESS_TOKEN" ]; then
      curl -sS -o /tmp/aegis-verify-response.json -w "%{http_code}" \
        -X "$method" -H "Authorization: Bearer $ACCESS_TOKEN" \
        -H "Content-Type: application/json" --data "$data" "$url"
    else
      curl -sS -o /tmp/aegis-verify-response.json -w "%{http_code}" \
        -X "$method" -H "Content-Type: application/json" --data "$data" "$url"
    fi
  else
    if [ -n "$ACCESS_TOKEN" ]; then
      curl -sS -o /tmp/aegis-verify-response.json -w "%{http_code}" \
        -X "$method" -H "Authorization: Bearer $ACCESS_TOKEN" "$url"
    else
      curl -sS -o /tmp/aegis-verify-response.json -w "%{http_code}" \
        -X "$method" "$url"
    fi
  fi
}

require_status() {
  local label="$1"
  local expected="$2"
  local actual="$3"
  if [ "$actual" != "$expected" ]; then
    echo "FAIL $label expected HTTP $expected, got $actual" >&2
    if [ -s /tmp/aegis-verify-response.json ]; then
      sed -n '1,20p' /tmp/aegis-verify-response.json >&2
    fi
    exit 1
  fi
  echo "OK   $label HTTP $actual"
}

check_read_endpoint() {
  local path="$1"
  local status
  status="$(curl_status GET "$path")"
  case "$status" in
    200)
      echo "OK   GET $path HTTP 200"
      ;;
    401|403)
      echo "SKIP GET $path HTTP $status (set AEGIS_ACCESS_TOKEN for authenticated checks)"
      ;;
    404)
      echo "SKIP GET $path HTTP 404 (endpoint not present in this build)"
      ;;
    *)
      echo "FAIL GET $path HTTP $status" >&2
      if [ -s /tmp/aegis-verify-response.json ]; then
        sed -n '1,20p' /tmp/aegis-verify-response.json >&2
      fi
      exit 1
      ;;
  esac
}

db_count_query() {
  local sql="$1"
  if [ -n "${DATABASE_URL:-}" ] && command -v psql >/dev/null 2>&1; then
    psql "$DATABASE_URL" -At -c "$sql"
    return
  fi

  if docker ps --format '{{.Names}}' | grep -qx 'aegis-quant-postgres'; then
    docker exec aegis-quant-postgres psql \
      -U "${POSTGRES_USER:-aegis}" \
      -d "${POSTGRES_DB:-aegis_quant}" \
      -At -c "$sql"
    return
  fi

  echo "database unavailable for execution table count checks" >&2
  return 1
}

execution_counts() {
  db_count_query "SELECT 'orders=' || COUNT(*) FROM orders
UNION ALL SELECT 'paper_positions=' || COUNT(*) FROM paper_positions
UNION ALL SELECT 'paper_fills=' || COUNT(*) FROM paper_fills
UNION ALL SELECT 'exchange_testnet_orders=' || COUNT(*) FROM exchange_testnet_orders
UNION ALL SELECT 'exchange_testnet_order_lifecycle_events=' || COUNT(*) FROM exchange_testnet_order_lifecycle_events
UNION ALL SELECT 'testnet_shadow_promotions=' || COUNT(*) FROM testnet_shadow_promotions
ORDER BY 1;"
}

echo "Aegis research-loop smoke"
echo "API: $API_BASE_URL"
echo "Dashboard: $DASHBOARD_URL"

health_status="$(curl_status GET /system/health)"
require_status "GET /system/health" 200 "$health_status"

dashboard_status="$(curl -sS -o /tmp/aegis-dashboard-response.txt -w "%{http_code}" -I "$DASHBOARD_URL" || true)"
case "$dashboard_status" in
  200|301|302|307|308)
    echo "OK   dashboard HTTP $dashboard_status"
    ;;
  000)
    echo "SKIP dashboard not reachable at $DASHBOARD_URL"
    ;;
  *)
    echo "WARN dashboard HTTP $dashboard_status at $DASHBOARD_URL"
    ;;
esac

check_read_endpoint "/research/data/builds?limit=1"
check_read_endpoint "/research/campaigns?limit=1"
check_read_endpoint "/research/hypotheses?limit=1"
check_read_endpoint "/research/experiment-plans?limit=1"
check_read_endpoint "/research/candidates?limit=1"

if [ "$WITH_RESEARCH_RUN" -eq 0 ]; then
  echo "OK   default smoke completed without research mutations"
  exit 0
fi

if [ -z "$ACCESS_TOKEN" ]; then
  echo "FAIL --with-research-run requires AEGIS_ACCESS_TOKEN" >&2
  exit 1
fi
if [ -z "$RESEARCH_PLAN_ID" ]; then
  echo "FAIL --with-research-run requires AEGIS_RESEARCH_PLAN_ID" >&2
  exit 1
fi

before_counts="$(execution_counts)"
echo "Execution counts before:"
echo "$before_counts"

preview_status="$(curl_status POST "/research/experiment-plans/$RESEARCH_PLAN_ID/run-preview")"
require_status "POST /research/experiment-plans/:id/run-preview" 200 "$preview_status"
if ! grep -q '"mode":"PREVIEW"' /tmp/aegis-verify-response.json; then
  echo "FAIL preview response did not report PREVIEW mode" >&2
  sed -n '1,20p' /tmp/aegis-verify-response.json >&2
  exit 1
fi
if grep -q '"artifact_ids":\[[^]]' /tmp/aegis-verify-response.json; then
  echo "FAIL preview reported persisted artifact IDs" >&2
  sed -n '1,20p' /tmp/aegis-verify-response.json >&2
  exit 1
fi

confirmation="RUN RESEARCH PLAN $RESEARCH_PLAN_ID"
run_body="$(printf '{"mode":"RUN","confirmation":"%s"}' "$confirmation")"
run_status="$(curl_status POST "/research/experiment-plans/$RESEARCH_PLAN_ID/run" "$run_body")"
require_status "POST /research/experiment-plans/:id/run" 200 "$run_status"
if ! grep -q '"mode":"RUN"' /tmp/aegis-verify-response.json; then
  echo "FAIL run response did not report RUN mode" >&2
  sed -n '1,20p' /tmp/aegis-verify-response.json >&2
  exit 1
fi

after_counts="$(execution_counts)"
echo "Execution counts after:"
echo "$after_counts"

if [ "$before_counts" != "$after_counts" ]; then
  echo "FAIL execution table counts changed during research run" >&2
  exit 1
fi

echo "OK   research run created only research artifacts; execution counts unchanged"
