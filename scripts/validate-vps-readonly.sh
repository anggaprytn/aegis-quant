#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${AEGIS_API_BASE_URL:-http://127.0.0.1:3100}"
DASHBOARD_URL="${AEGIS_DASHBOARD_URL:-http://127.0.0.1:3101}"
ACCESS_TOKEN="${AEGIS_ACCESS_TOKEN:-}"
TOKEN_SOURCE="none"
READONLY_DATABASE_URL="${AEGIS_READONLY_DATABASE_URL:-}"
TAIL_LINES="${AEGIS_VALIDATE_LOG_TAIL_LINES:-80}"
JOB_LIMIT="${AEGIS_VALIDATE_JOB_LIMIT:-50}"
RUN_LIMIT="${AEGIS_VALIDATE_RUN_LIMIT:-20}"
RUN_JOB_SAMPLE_LIMIT="${AEGIS_VALIDATE_RUN_JOB_SAMPLE_LIMIT:-10}"

STRICT=0
JSON_OUTPUT=0
SKIP_DB=0
SKIP_API=0

OK_COUNT=0
WARN_COUNT=0
FAIL_COUNT=0
DB_MODE="none"
JSON_EVENTS=()

usage() {
  cat <<'USAGE'
Usage: scripts/validate-vps-readonly.sh [--strict] [--json] [--skip-db] [--skip-api]

Read-only VPS validation for Aegis API and Docker-based ai_read database views.

Allowed operations used by this script:
  - docker ps
  - docker logs --tail
  - curl GET health/read-only endpoints
  - psql SELECT statements against ai_read views only
  - docker exec -i aegis-quant-postgres psql -U aegis_readonly -d aegis_quant -c "<SELECT * FROM ai_read...>"

Flags:
  --strict   Treat missing ai_read views and skipped DB checks as failures.
  --json     Also print a compact JSON event summary at the end.
  --skip-db  Skip all database checks.
  --skip-api Skip API and dashboard checks.

Environment:
  AEGIS_API_BASE_URL              default http://127.0.0.1:3100
  AEGIS_DASHBOARD_URL            default http://127.0.0.1:3101
  AEGIS_ACCESS_TOKEN             optional bearer token for authenticated GET endpoints
  token fallback uses ~/.config/aegis/token.json when AEGIS_ACCESS_TOKEN is unset
  AEGIS_READONLY_DATABASE_URL    optional Postgres URL; must use aegis_readonly
  AEGIS_VALIDATE_LOG_TAIL_LINES  default 80
  AEGIS_VALIDATE_JOB_LIMIT       default 50
  AEGIS_VALIDATE_RUN_LIMIT       default 20
  AEGIS_VALIDATE_RUN_JOB_SAMPLE_LIMIT default 10
USAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --strict)
      STRICT=1
      ;;
    --json)
      JSON_OUTPUT=1
      ;;
    --skip-db)
      SKIP_DB=1
      ;;
    --skip-api)
      SKIP_API=1
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
  shift
done

json_escape() {
  if command -v jq >/dev/null 2>&1; then
    jq -Rn --arg value "$1" '$value'
  else
    printf '"%s"' "$(printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g')"
  fi
}

record_event() {
  local level="$1"
  local message="$2"
  JSON_EVENTS+=("{\"level\":$(json_escape "$level"),\"message\":$(json_escape "$message")}")
}

section() {
  printf '\n== %s ==\n' "$1"
}

ok() {
  OK_COUNT=$((OK_COUNT + 1))
  printf 'OK   %s\n' "$1"
  record_event "OK" "$1"
}

warn() {
  WARN_COUNT=$((WARN_COUNT + 1))
  printf 'WARN %s\n' "$1"
  record_event "WARN" "$1"
}

info() {
  printf 'INFO  %s\n' "$1"
  record_event "INFO" "$1"
}

fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  printf 'FAIL %s\n' "$1"
  record_event "FAIL" "$1"
}

strict_fail() {
  local message="$1"
  if [ "$STRICT" -eq 1 ]; then
    fail "$message"
  fi
}

need_command() {
  if command -v "$1" >/dev/null 2>&1; then
    return 0
  fi
  warn "$1 not installed; skipping checks that require it"
  return 1
}

load_access_token() {
  if [ -n "$ACCESS_TOKEN" ]; then
    TOKEN_SOURCE="environment"
    return 0
  fi

  local token_file="${HOME}/.config/aegis/token.json"
  if [ ! -f "$token_file" ]; then
    TOKEN_SOURCE="missing"
    return 1
  fi

  if command -v jq >/dev/null 2>&1; then
    ACCESS_TOKEN="$(jq -r '.access_token // empty' "$token_file" 2>/dev/null || true)"
  else
    ACCESS_TOKEN="$(sed -n 's/.*\"access_token\"[[:space:]]*:[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p' "$token_file" 2>/dev/null | head -n 1 || true)"
  fi

  if [ -n "$ACCESS_TOKEN" ] && [ "$ACCESS_TOKEN" != "null" ]; then
    TOKEN_SOURCE="file:$token_file"
    return 0
  fi

  TOKEN_SOURCE="invalid"
  ACCESS_TOKEN=""
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
  curl -sS -o /dev/null -w "%{http_code}" "$DASHBOARD_URL" 2>/dev/null || true
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
  local logs
  local line
  local count=0

  logs="$(docker logs --tail "$TAIL_LINES" "$name" 2>&1 | redact_log_line)"
  while IFS= read -r line; do
    [ -n "$line" ] || continue

    if grep -Eq 'disabled; idling|failed_runs=0|error_count=0|failed=0|no 500s|database system is ready to accept connections|startup process|shutting down|shutdown complete' <<<"$line"; then
      continue
    fi

    if grep -Eiq 'level=ERROR|\\bpanic\\b|\\bpanicked\\b' <<<"$line"; then
      count=$((count + 1))
      continue
    fi

    if grep -Eiq '\\blevel=FATAL\\b|\\bFATAL\\b' <<<"$line"; then
      if ! grep -Eiq 'clean shutdown|shutdown|shut.?down|terminated|terminating|closed|close' <<<"$line"; then
        count=$((count + 1))
      fi
      continue
    fi

    if grep -Eiq 'status[=: ]+500|failed_runs=[1-9][0-9]*|auto_paused|backing_off|connection refused|relation does not exist|permission denied|\"failed_runs\": *[1-9][0-9]*|\\blevel=ERROR\\b|\\b\"level\"[[:space:]]*:[[:space:]]*\"ERROR\"' <<<"$line"; then
      count=$((count + 1))
      continue
    fi
  done <<<"$logs"

  printf '%s\n' "$count"
}

readonly_url_looks_safe() {
  case "$READONLY_DATABASE_URL" in
    *aegis_readonly*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

detect_db_mode() {
  if [ "$SKIP_DB" -eq 1 ]; then
    DB_MODE="skipped"
    return
  fi

  if [ -n "$READONLY_DATABASE_URL" ]; then
    if readonly_url_looks_safe; then
      DB_MODE="url"
    else
      warn "AEGIS_READONLY_DATABASE_URL is set but does not appear to use aegis_readonly; DB checks skipped"
      strict_fail "strict mode requires AEGIS_READONLY_DATABASE_URL to use aegis_readonly"
      DB_MODE="unsafe-url"
    fi
    return
  fi

  if need_command docker; then
    if docker_running "aegis-quant-postgres"; then
      DB_MODE="docker"
      return
    fi
  fi

  DB_MODE="none"
  warn "DB checks skipped; set AEGIS_READONLY_DATABASE_URL or run aegis-quant-postgres for docker exec mode"
  strict_fail "strict mode requires DB validation"
}

psql_ai_read() {
  local sql="$1"
  case "$DB_MODE" in
    url)
      psql "$READONLY_DATABASE_URL" -X -v ON_ERROR_STOP=1 -At -c "$sql"
      ;;
    docker)
      docker exec -i aegis-quant-postgres psql -U aegis_readonly -d aegis_quant -X -v ON_ERROR_STOP=1 -At -c "$sql"
      ;;
    *)
      return 2
      ;;
  esac
}

is_missing_view_error() {
  local output="$1"
  grep -Eiq 'relation "ai_read\.[^"]+" does not exist|schema "ai_read" does not exist|permission denied for schema ai_read|permission denied for view' <<<"$output"
}

is_readonly_role_error() {
  local output="$1"
  grep -Eiq 'role "aegis_readonly" does not exist|password authentication failed for user "aegis_readonly"|permission denied' <<<"$output"
}

print_db_view() {
  local label="$1"
  local sql="$2"
  local output
  local status

  if [ "$DB_MODE" = "none" ] || [ "$DB_MODE" = "skipped" ] || [ "$DB_MODE" = "unsafe-url" ]; then
    return
  fi

  set +e
  output="$(psql_ai_read "$sql" 2>&1)"
  status=$?
  set -e
  output="$(redact_log_line <<<"$output")"

  if [ "$status" -ne 0 ]; then
    if is_missing_view_error "$output"; then
      warn "$label missing or inaccessible; install/grant the ai_read read-only view for VPS validation"
      strict_fail "strict mode requires $label"
    elif is_readonly_role_error "$output"; then
      warn "$label skipped; set up the aegis_readonly role and grant access to ai_read views"
      strict_fail "strict mode requires aegis_readonly access to $label"
    else
      warn "$label query failed in $DB_MODE mode"
      strict_fail "strict mode requires successful $label query"
      printf '%s\n' "$output"
    fi
    return
  fi

  ok "$label"
  printf '%s\n' "$output"
}

print_execution_safety_counts() {
  local label="ai_read.execution_safety_counts"
  local output
  local status

  if [ "$DB_MODE" = "none" ] || [ "$DB_MODE" = "skipped" ] || [ "$DB_MODE" = "unsafe-url" ]; then
    return
  fi

  set +e
  output="$(psql_ai_read "SELECT * FROM ai_read.execution_safety_counts;" 2>&1)"
  status=$?
  set -e
  output="$(redact_log_line <<<"$output")"

  if [ "$status" -ne 0 ]; then
    if is_missing_view_error "$output"; then
      warn "$label missing or inaccessible; install/grant the ai_read read-only view for VPS validation"
      strict_fail "strict mode requires $label"
    elif is_readonly_role_error "$output"; then
      warn "$label skipped; set up the aegis_readonly role and grant access to ai_read views"
      strict_fail "strict mode requires aegis_readonly access to $label"
    else
      warn "$label query failed in $DB_MODE mode"
      strict_fail "strict mode requires successful $label query"
      printf '%s\n' "$output"
    fi
    return
  fi

  local non_zero_count
  non_zero_count="$(
    awk -F "|" '
      /^[[:space:]]*[A-Za-z_][A-Za-z0-9_]*[[:space:]]*\|/ {
        for (i = 2; i <= NF; i++) {
          if ($i ~ /^[[:space:]]*[0-9]+[[:space:]]*$/ && $i + 0 > 0) {
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

run_api_checks() {
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
  if [ -z "$ACCESS_TOKEN" ]; then
    warn "GET /research/scheduled-jobs skipped; set AEGIS_ACCESS_TOKEN for authenticated read-only endpoint"
    return
  fi

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

        if [ "$job_count" = "0" ] || [ -z "$job_count" ]; then
          fail "scheduled jobs endpoint returned no jobs; expected at least one safe monitoring job"
        fi
        if [ "$enabled_count" = "0" ] || [ -z "$enabled_count" ]; then
          fail "scheduled jobs endpoint returned 0 enabled jobs; expected safe jobs to be enabled"
        fi
        if [ "$auto_paused_count" != "0" ]; then
          fail "scheduled jobs endpoint shows auto-paused=$auto_paused_count (expected 0)"
        fi
        if [ "$backing_off_count" != "0" ]; then
          fail "scheduled jobs endpoint shows backing_off=$backing_off_count (expected 0)"
        fi

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
                    ok "GET recent scheduled job runs for $job_name; runs=$run_count failed=$failed_runs ${latest_run:-latest_started_at=null}"
                    if [ "$failed_runs" != "0" ]; then
                      fail "scheduled job $job_name recent runs include failed=$failed_runs"
                    fi
                    if [ "$run_count" = "0" ]; then
                      warn "scheduled job $job_name has no recent runs in the sample"
                    fi
                    ;;
                  401|403)
                    warn "GET /research/scheduled-jobs/:id/runs HTTP $runs_status; token lacks access"
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
      warn "GET /research/scheduled-jobs HTTP $jobs_status; token lacks access"
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
}

run_container_checks() {
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
          ok "$container running; no meaningful warning patterns in last $TAIL_LINES lines"
        else
          warn "$container running; $errors meaningful warning pattern(s) in last $TAIL_LINES lines"
        fi
      else
        warn "$container is not running"
      fi
    done
  else
    warn "container checks skipped"
  fi
}

run_db_checks() {
  detect_db_mode

  section "Database"
  case "$DB_MODE" in
    url)
      ok "DB validation mode: AEGIS_READONLY_DATABASE_URL"
      ;;
    docker)
      ok "DB validation mode: docker exec aegis-quant-postgres as aegis_readonly"
      ;;
    skipped)
      warn "DB validation skipped by --skip-db"
      return
      ;;
    unsafe-url|none)
      return
      ;;
  esac

  section "Candle Coverage"
  print_db_view "ai_read.candle_coverage" "SELECT * FROM ai_read.candle_coverage;"

  section "Execution Safety"
  print_execution_safety_counts

  section "Shadow Decision Summary"
  print_db_view "ai_read.shadow_decision_summary" "SELECT * FROM ai_read.shadow_decision_summary;"

  section "Research Candidate Status"
  print_db_view "ai_read.research_candidate_status" "SELECT * FROM ai_read.research_candidate_status;"

  section "Walk Forward Status"
  print_db_view "ai_read.walk_forward_status" "SELECT * FROM ai_read.walk_forward_status;"
}

print_json_summary() {
  local events_json
  events_json="$(IFS=,; printf '%s' "${JSON_EVENTS[*]}")"
  printf '\n== JSON Summary ==\n'
  printf '{"ok":%s,"warn":%s,"fail":%s,"strict":%s,"skip_db":%s,"skip_api":%s,"db_mode":%s,"events":[%s]}\n' \
    "$OK_COUNT" \
    "$WARN_COUNT" \
    "$FAIL_COUNT" \
    "$STRICT" \
    "$SKIP_DB" \
    "$SKIP_API" \
    "$(json_escape "$DB_MODE")" \
    "$events_json"
}

echo "Aegis VPS read-only validation"
echo "API: $API_BASE_URL"
echo "Dashboard: $DASHBOARD_URL"
echo "Strict: $STRICT"
echo "API checks: $([ "$SKIP_API" -eq 1 ] && printf 'skipped' || printf 'enabled')"
echo "Database checks: $([ "$SKIP_DB" -eq 1 ] && printf 'skipped' || printf 'enabled')"
load_access_token
if [ -n "$ACCESS_TOKEN" ]; then
  case "$TOKEN_SOURCE" in
    environment)
      echo "Auth token: provided via AEGIS_ACCESS_TOKEN"
      ;;
    file:*)
      echo "Auth token: loaded from ~/.config/aegis/token.json"
      ;;
    *)
      echo "Auth token: available"
      ;;
  esac
else
  echo "Auth token: unavailable; authenticated endpoints may be skipped"
  if [ "$TOKEN_SOURCE" = "missing" ]; then
    warn "AEGIS_ACCESS_TOKEN unset and ~/.config/aegis/token.json not found; authenticated API checks will be skipped"
  elif [ "$TOKEN_SOURCE" = "invalid" ]; then
    warn "failed to load token from ~/.config/aegis/token.json; authenticated API checks may be skipped"
  else
    warn "no access token available; authenticated API checks may be skipped"
  fi
fi

if [ "$SKIP_API" -eq 1 ]; then
  warn "API checks skipped by --skip-api"
else
  run_api_checks
fi

run_container_checks
run_db_checks

section "Summary"
printf 'OK=%s WARN=%s FAIL=%s\n' "$OK_COUNT" "$WARN_COUNT" "$FAIL_COUNT"

if [ "$JSON_OUTPUT" -eq 1 ]; then
  print_json_summary
fi

if [ "$FAIL_COUNT" -gt 0 ]; then
  exit 1
fi
