#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${AEGIS_API_BASE_URL:-http://127.0.0.1:3100}"
DASHBOARD_URL="${AEGIS_DASHBOARD_URL:-http://127.0.0.1:3101}"
ACCESS_TOKEN="${AEGIS_ACCESS_TOKEN:-}"
READONLY_DATABASE_URL="${AEGIS_READONLY_DATABASE_URL:-}"
TAIL_LINES="${AEGIS_VALIDATE_LOG_TAIL_LINES:-80}"
JOB_LIMIT="${AEGIS_VALIDATE_JOB_LIMIT:-50}"
RUN_LIMIT="${AEGIS_VALIDATE_RUN_LIMIT:-20}"
RUN_JOB_SAMPLE_LIMIT="${AEGIS_VALIDATE_RUN_JOB_SAMPLE_LIMIT:-10}"

OK_COUNT=0
WARN_COUNT=0
FAIL_COUNT=0

usage() {
  cat <<'USAGE'
Usage: scripts/validate-vps-readonly.sh

Read-only VPS validation for scheduled research state.

Allowed operations used by this script:
  - docker ps
  - docker logs --tail
  - curl GET health/read-only endpoints
  - psql SELECT against ai_read views through AEGIS_READONLY_DATABASE_URL

Environment:
  AEGIS_API_BASE_URL              default http://127.0.0.1:3100
  AEGIS_DASHBOARD_URL            default http://127.0.0.1:3101
  AEGIS_ACCESS_TOKEN             optional bearer token for authenticated GET endpoints
  AEGIS_READONLY_DATABASE_URL    optional read-only Postgres URL, expected to use aegis_readonly
  AEGIS_VALIDATE_LOG_TAIL_LINES  default 80
  AEGIS_VALIDATE_JOB_LIMIT       default 50
  AEGIS_VALIDATE_RUN_LIMIT       default 20
  AEGIS_VALIDATE_RUN_JOB_SAMPLE_LIMIT default 10
USAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
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

section() {
  printf '\n== %s ==\n' "$1"
}

ok() {
  OK_COUNT=$((OK_COUNT + 1))
  printf 'OK   %s\n' "$1"
}

warn() {
  WARN_COUNT=$((WARN_COUNT + 1))
  printf 'WARN %s\n' "$1"
}

fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  printf 'FAIL %s\n' "$1"
}

need_command() {
  if command -v "$1" >/dev/null 2>&1; then
    return 0
  fi
  warn "$1 not installed; skipping checks that require it"
  return 1
}

curl_get() {
  local url="$1"
  if [ -n "$ACCESS_TOKEN" ]; then
    curl -fsS -H "Authorization: Bearer $ACCESS_TOKEN" "$url"
  else
    curl -fsS "$url"
  fi
}

curl_status() {
  local url="$1"
  if [ -n "$ACCESS_TOKEN" ]; then
    curl -sS -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $ACCESS_TOKEN" "$url" 2>/dev/null || true
  else
    curl -sS -o /dev/null -w "%{http_code}" "$url" 2>/dev/null || true
  fi
}

dashboard_status() {
  curl -sS -o /dev/null -w "%{http_code}" -I "$DASHBOARD_URL" 2>/dev/null || true
}

json_count() {
  local body="$1"
  local filter="$2"
  if command -v jq >/dev/null 2>&1; then
    jq -r "$filter" 2>/dev/null <<<"$body" || printf 'unknown'
  else
    printf 'unknown'
  fi
}

redact_log_line() {
  sed -E \
    -e 's/(Authorization: Bearer )[A-Za-z0-9._~+\/=-]+/\1[REDACTED]/g' \
    -e 's/(access[_-]?token[=:])[A-Za-z0-9._~+\/=-]+/\1[REDACTED]/Ig' \
    -e 's/(refresh[_-]?token[=:])[A-Za-z0-9._~+\/=-]+/\1[REDACTED]/Ig' \
    -e 's/([A-Za-z0-9._%+-]+)@([A-Za-z0-9.-]+\.[A-Za-z]{2,})/[REDACTED_EMAIL]/g'
}

docker_running() {
  local name="$1"
  docker ps --format '{{.Names}}' | grep -qx "$name"
}

docker_error_count() {
  local name="$1"
  docker logs --tail "$TAIL_LINES" "$name" 2>&1 \
    | redact_log_line \
    | grep -Eic '(^|[^A-Za-z])(ERROR|FATAL|panic|panicked|failed)([^A-Za-z]|$)' || true
}

psql_readonly() {
  local sql="$1"
  if [ -z "$READONLY_DATABASE_URL" ]; then
    return 2
  fi
  psql "$READONLY_DATABASE_URL" -X -v ON_ERROR_STOP=1 -At -c "$sql"
}

db_view_exists() {
  local view_name="$1"
  local result
  result="$(psql_readonly "SELECT to_regclass('$view_name') IS NOT NULL;" 2>/dev/null || true)"
  [ "$result" = "t" ]
}

print_db_view() {
  local label="$1"
  local view_name="$2"
  local sql="$3"

  if [ -z "$READONLY_DATABASE_URL" ]; then
    warn "$label skipped; set AEGIS_READONLY_DATABASE_URL for ai_read view checks"
    return
  fi

  if ! need_command psql >/dev/null; then
    warn "$label skipped; psql not available"
    return
  fi

  local current_user
  current_user="$(psql_readonly "SELECT current_user;" 2>/dev/null || true)"
  if [ -z "$current_user" ]; then
    warn "$label skipped; read-only database connection failed"
    return
  fi
  if [ "$current_user" != "aegis_readonly" ]; then
    warn "$label using database role '$current_user'; expected aegis_readonly"
  fi

  if ! db_view_exists "$view_name"; then
    warn "$label skipped; $view_name view is not available"
    return
  fi

  local output
  output="$(psql_readonly "$sql" 2>/dev/null || true)"
  if [ -z "$output" ]; then
    ok "$label returned no rows"
    return
  fi
  ok "$label"
  printf '%s\n' "$output"
}

print_execution_safety_counts() {
  local label="ai_read.execution_safety_counts"
  local view_name="ai_read.execution_safety_counts"
  local sql="SELECT * FROM ai_read.execution_safety_counts ORDER BY 1 LIMIT 50;"

  if [ -z "$READONLY_DATABASE_URL" ]; then
    warn "$label skipped; set AEGIS_READONLY_DATABASE_URL for ai_read view checks"
    return
  fi

  if ! need_command psql >/dev/null; then
    warn "$label skipped; psql not available"
    return
  fi

  local current_user
  current_user="$(psql_readonly "SELECT current_user;" 2>/dev/null || true)"
  if [ -z "$current_user" ]; then
    warn "$label skipped; read-only database connection failed"
    return
  fi
  if [ "$current_user" != "aegis_readonly" ]; then
    warn "$label using database role '$current_user'; expected aegis_readonly"
  fi

  if ! db_view_exists "$view_name"; then
    warn "$label skipped; $view_name view is not available"
    return
  fi

  local output
  output="$(psql_readonly "$sql" 2>/dev/null || true)"
  if [ -z "$output" ]; then
    ok "$label returned no rows"
    return
  fi

  local non_zero_count
  non_zero_count="$(
    awk -F '|' '
      {
        for (i = 1; i <= NF; i++) {
          if ($i ~ /^[0-9]+$/ && $i + 0 > 0) {
            found += 1
            break
          }
        }
      }
      END { print found + 0 }
    ' <<<"$output"
  )"

  if [ "$non_zero_count" = "0" ]; then
    ok "$label all reported counts are zero"
  else
    warn "$label has $non_zero_count non-zero row(s)"
  fi
  printf '%s\n' "$output"
}

echo "Aegis VPS read-only validation"
echo "API: $API_BASE_URL"
echo "Dashboard: $DASHBOARD_URL"
echo "Database view checks: $([ -n "$READONLY_DATABASE_URL" ] && printf 'enabled' || printf 'skipped')"

section "API Health"
health_status="$(curl_status "$API_BASE_URL/system/health")"
case "$health_status" in
  200)
    ok "GET /system/health HTTP 200"
    ;;
  000)
    fail "GET /system/health unreachable at $API_BASE_URL"
    ;;
  *)
    fail "GET /system/health HTTP $health_status"
    ;;
esac

section "Dashboard"
dash_status="$(dashboard_status)"
case "$dash_status" in
  200|301|302|307|308)
    ok "dashboard HTTP $dash_status"
    ;;
  000)
    warn "dashboard unreachable at $DASHBOARD_URL"
    ;;
  *)
    warn "dashboard HTTP $dash_status at $DASHBOARD_URL"
    ;;
esac

section "Containers"
if need_command docker; then
  containers=(
    aegis-quant-postgres
    aegis-quant-api
    aegis-quant-dashboard
    aegis-quant-market-ingest
    aegis-quant-candle-aggregator
    aegis-quant-scheduled-research-runner
  )
  for container in "${containers[@]}"; do
    if docker_running "$container"; then
      errors="$(docker_error_count "$container")"
      if [ "$errors" = "0" ]; then
        ok "$container running; no error-like log lines in last $TAIL_LINES lines"
      else
        warn "$container running; $errors error-like log line(s) in last $TAIL_LINES lines"
      fi
    else
      warn "$container is not running"
    fi
  done
else
  warn "container checks skipped"
fi

section "Market Feed"
feed_status="$(curl_status "$API_BASE_URL/market/feed-status")"
case "$feed_status" in
  200)
    feed_body="$(curl_get "$API_BASE_URL/market/feed-status" || true)"
    if command -v jq >/dev/null 2>&1; then
      feed_count="$(json_count "$feed_body" '.feeds | length')"
      stale_count="$(json_count "$feed_body" '[.feeds[]? | select((.freshness_status // .status // "") | test("stale|degraded|error"; "i"))] | length')"
      ok "GET /market/feed-status HTTP 200; feeds=$feed_count stale_or_degraded=$stale_count"
      jq -r '.feeds[]? | "  " + .exchange + " " + .symbol + " status=" + (.status // "unknown") + " freshness=" + (.freshness_status // "unknown") + " last_event_at=" + (.last_event_at // "null")' 2>/dev/null <<<"$feed_body" || true
    else
      ok "GET /market/feed-status HTTP 200"
      warn "jq not installed; feed summary not parsed"
    fi
    ;;
  401|403)
    warn "GET /market/feed-status HTTP $feed_status; set AEGIS_ACCESS_TOKEN if required"
    ;;
  404)
    warn "GET /market/feed-status HTTP 404; endpoint not present in this build"
    ;;
  000)
    fail "GET /market/feed-status unreachable"
    ;;
  *)
    fail "GET /market/feed-status HTTP $feed_status"
    ;;
esac

section "Candle Coverage"
print_db_view \
  "ai_read.candle_coverage" \
  "ai_read.candle_coverage" \
  "SELECT * FROM ai_read.candle_coverage ORDER BY 1 LIMIT 50;"

section "Aggregation Status"
aggregation_status="$(curl_status "$API_BASE_URL/market/candles/aggregation-status")"
case "$aggregation_status" in
  200)
    aggregation_body="$(curl_get "$API_BASE_URL/market/candles/aggregation-status" || true)"
    if command -v jq >/dev/null 2>&1; then
      row_count="$(json_count "$aggregation_body" '.rows | length')"
      stale_count="$(json_count "$aggregation_body" '[.rows[]? | select((.status // "") | test("stale|lagging|missing|error"; "i"))] | length')"
      ok "GET /market/candles/aggregation-status HTTP 200; rows=$row_count stale_or_missing=$stale_count"
      jq -r '.rows[]? | "  " + .symbol + " " + .source_interval + "->" + .target_interval + " status=" + (.status // "unknown") + " lag_seconds=" + ((.lag_seconds // "null") | tostring)' 2>/dev/null <<<"$aggregation_body" || true
    else
      ok "GET /market/candles/aggregation-status HTTP 200"
      warn "jq not installed; aggregation summary not parsed"
    fi
    ;;
  401|403)
    warn "GET /market/candles/aggregation-status HTTP $aggregation_status; set AEGIS_ACCESS_TOKEN if required"
    ;;
  404)
    warn "GET /market/candles/aggregation-status HTTP 404; endpoint not present in this build"
    ;;
  000)
    fail "GET /market/candles/aggregation-status unreachable"
    ;;
  *)
    fail "GET /market/candles/aggregation-status HTTP $aggregation_status"
    ;;
esac

section "Scheduled Jobs"
jobs_status="$(curl_status "$API_BASE_URL/research/scheduled-jobs?limit=$JOB_LIMIT")"
case "$jobs_status" in
  200)
    jobs_body="$(curl_get "$API_BASE_URL/research/scheduled-jobs?limit=$JOB_LIMIT" || true)"
    if command -v jq >/dev/null 2>&1; then
      job_count="$(json_count "$jobs_body" '.jobs | length')"
      enabled_count="$(json_count "$jobs_body" '[.jobs[]? | select(.enabled == true)] | length')"
      auto_paused_count="$(json_count "$jobs_body" '[.jobs[]? | select((.status // "") == "AUTO_PAUSED")] | length')"
      backing_off_count="$(json_count "$jobs_body" '[.jobs[]? | select((.status // "") == "BACKING_OFF" or (.backoff_until // null) != null)] | length')"
      ok "GET /research/scheduled-jobs HTTP 200; jobs=$job_count enabled=$enabled_count auto_paused=$auto_paused_count backing_off=$backing_off_count"
      jq -r '.jobs[]? | "  " + .id + " " + .name + " kind=" + .kind + " status=" + .status + " enabled=" + (.enabled | tostring) + " next_run_at=" + (.next_run_at // "null") + " failures=" + ((.consecutive_failure_count // 0) | tostring)' 2>/dev/null <<<"$jobs_body" || true

      if [ "$auto_paused_count" != "0" ] || [ "$backing_off_count" != "0" ]; then
        warn "scheduled research has auto-paused or backing-off jobs"
        jq -r '.jobs[]? | select((.status // "") == "AUTO_PAUSED" or (.status // "") == "BACKING_OFF" or (.backoff_until // null) != null) | "  attention " + .name + " status=" + .status + " backoff_until=" + (.backoff_until // "null") + " reason=" + (.auto_paused_reason // .last_failure_reason // "null")' 2>/dev/null <<<"$jobs_body" || true
      fi

      sampled_job_ids="$(jq -r ".jobs[]?.id" 2>/dev/null <<<"$jobs_body" | head -n "$RUN_JOB_SAMPLE_LIMIT")"
      if [ -n "$sampled_job_ids" ]; then
        while IFS= read -r job_id; do
          [ -n "$job_id" ] || continue
          job_name="$(jq -r --arg id "$job_id" '.jobs[]? | select(.id == $id) | .name' 2>/dev/null <<<"$jobs_body")"
          runs_status="$(curl_status "$API_BASE_URL/research/scheduled-jobs/$job_id/runs?limit=$RUN_LIMIT")"
          case "$runs_status" in
            200)
              runs_body="$(curl_get "$API_BASE_URL/research/scheduled-jobs/$job_id/runs?limit=$RUN_LIMIT" || true)"
              run_count="$(json_count "$runs_body" '.runs | length')"
              failed_runs="$(json_count "$runs_body" '[.runs[]? | select((.status // "") | test("FAILED"; "i"))] | length')"
              latest_run="$(jq -r '.runs[0]? | "latest_started_at=" + (.started_at // "null") + " status=" + (.status // "unknown") + " completed_at=" + (.completed_at // "null") + " artifact=" + (.created_artifact_type // "none")' 2>/dev/null <<<"$runs_body")"
              ok "runs for $job_name; runs=$run_count failed=$failed_runs ${latest_run:-latest_started_at=null}"
              ;;
            401|403)
              warn "GET /research/scheduled-jobs/:id/runs HTTP $runs_status; set AEGIS_ACCESS_TOKEN if required"
              ;;
            *)
              warn "GET /research/scheduled-jobs/:id/runs HTTP $runs_status for $job_name"
              ;;
          esac
        done <<<"$sampled_job_ids"
      else
        warn "no scheduled jobs returned; recent job run endpoint not checked"
      fi
    else
      ok "GET /research/scheduled-jobs HTTP 200"
      warn "jq not installed; scheduled job summary not parsed"
    fi
    ;;
  401|403)
    warn "GET /research/scheduled-jobs HTTP $jobs_status; set AEGIS_ACCESS_TOKEN if required"
    ;;
  404)
    warn "GET /research/scheduled-jobs HTTP 404; endpoint not present in this build"
    ;;
  000)
    fail "GET /research/scheduled-jobs unreachable"
    ;;
  *)
    fail "GET /research/scheduled-jobs HTTP $jobs_status"
    ;;
esac

section "Execution Safety"
print_execution_safety_counts

section "Operator Report"
reports_status="$(curl_status "$API_BASE_URL/reports/operator?limit=1")"
case "$reports_status" in
  200)
    reports_body="$(curl_get "$API_BASE_URL/reports/operator?limit=1" || true)"
    if command -v jq >/dev/null 2>&1; then
      report_count="$(json_count "$reports_body" '.reports | length')"
      if [ "$report_count" = "0" ]; then
        warn "GET /reports/operator HTTP 200; no persisted operator reports found"
      else
        ok "GET /reports/operator HTTP 200; latest persisted report found"
        jq -r '.reports[0] | "  report_id=" + .report_id + " status=" + .status + " format=" + .format + " created_at=" + .created_at + " window_end=" + .window_end' 2>/dev/null <<<"$reports_body" || true
      fi
    else
      ok "GET /reports/operator HTTP 200"
      warn "jq not installed; operator report summary not parsed"
    fi
    ;;
  401|403)
    warn "GET /reports/operator HTTP $reports_status; set AEGIS_ACCESS_TOKEN if required"
    ;;
  404)
    warn "GET /reports/operator HTTP 404; endpoint not present in this build"
    ;;
  000)
    fail "GET /reports/operator unreachable"
    ;;
  *)
    fail "GET /reports/operator HTTP $reports_status"
    ;;
esac

section "Summary"
printf 'OK=%s WARN=%s FAIL=%s\n' "$OK_COUNT" "$WARN_COUNT" "$FAIL_COUNT"
if [ "$FAIL_COUNT" -gt 0 ]; then
  exit 1
fi
