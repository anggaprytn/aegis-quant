# Runbook

## Local Start

```bash
cp .env.example .env
docker compose -f infra/docker-compose.yml --env-file .env up -d postgres
docker compose -f infra/docker-compose.yml --env-file .env --profile migrate run --rm migrate
docker compose -f infra/docker-compose.yml --env-file .env up -d api
docker compose -f infra/docker-compose.yml --env-file .env --profile dashboard up -d dashboard
docker compose -f infra/docker-compose.yml --env-file .env --profile aggregation up -d candle-aggregator
```

Local default URLs:

- API through Compose: `http://127.0.0.1:3100`
- Dashboard through Compose: `http://127.0.0.1:3101`
- Local dashboard dev server: `http://127.0.0.1:3001`
- Prometheus profile: `http://127.0.0.1:9090`

Bootstrap owner after the API is healthy:

```bash
curl -X POST http://127.0.0.1:3100/auth/bootstrap-owner
cargo run -p cli -- auth login --email "$AEGIS_BOOTSTRAP_OWNER_EMAIL" --password "$AEGIS_BOOTSTRAP_OWNER_PASSWORD"
```

## VPS Sync Flow

If `/usr/local/bin/syncaegis` is used, keep the order explicit:

```bash
git pull --ff-only
docker compose -f infra/docker-compose.yml --env-file .env build api dashboard market-ingest testnet-shadow-runner candle-aggregator
docker compose -f infra/docker-compose.yml --env-file .env up -d postgres
docker compose -f infra/docker-compose.yml --env-file .env --profile migrate run --rm migrate
docker compose -f infra/docker-compose.yml --env-file .env up -d api
docker compose -f infra/docker-compose.yml --env-file .env --profile dashboard up -d dashboard
docker compose -f infra/docker-compose.yml --env-file .env --profile ingest up -d market-ingest
docker compose -f infra/docker-compose.yml --env-file .env --profile shadow up -d testnet-shadow-runner
docker compose -f infra/docker-compose.yml --env-file .env --profile aggregation up -d candle-aggregator
```

If VPS operations use the host CLI at `/usr/local/bin/aegis`, install it from the same checked-out source after the deploy:

```bash
./scripts/install-vps-cli.sh
```

The VPS host does not need a Rust toolchain. The installer uses host Cargo when available:

```bash
cargo build --release -p cli
```

If `cargo` is unavailable, it falls back to Docker, builds the repo `Dockerfile`, extracts `/usr/local/bin/aegis` from the built image, installs it to `/usr/local/bin/aegis`, then verifies:

```bash
aegis --help
aegis research --help
aegis research scheduled-jobs --help
```

If `/usr/local/bin/syncaegis` is managed outside this repo, keep the script itself out of git and add this step after `git pull` and the service builds:

```bash
cd /app/aegis-quant
/app/aegis-quant/scripts/install-vps-cli.sh
```

To refresh only the scheduled research runner image/container after pulling runner changes, use the targeted helper:

```bash
./scripts/refresh-vps-scheduled-runner.sh
```

It only runs:

```bash
docker compose -f infra/docker-compose.yml --env-file .env --profile research-scheduler build scheduled-research-runner
docker compose -f infra/docker-compose.yml --env-file .env --profile research-scheduler up -d --force-recreate scheduled-research-runner
```

It does not run `docker compose down`, touch Postgres volumes, recreate API/dashboard/market-ingest, edit `.env`, or enable the scheduler.

Do not reset VPS volumes as part of routine sync. Take a database backup before migrations or destructive maintenance.

## Migrations

The `migrate` service applies sorted SQL files from `crates/db/migrations` with `psql`:

```bash
docker compose -f infra/docker-compose.yml --env-file .env --profile migrate run --rm migrate
```

Run it after Postgres is healthy and before API/workers. Local environments are disposable; VPS environments should be backed up first.

## Backups

Recommended before VPS migrations:

```bash
docker exec aegis-quant-postgres pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB" > "backup-$(date +%Y%m%d-%H%M%S).sql"
```

Store backups outside the deployment directory and restrict file permissions.

## Provider Fallbacks

Public market data can use Binance fallback hosts:

```txt
BINANCE_REST_BASE_URL=https://api.binance.com
BINANCE_REST_FALLBACK_BASE_URLS=https://data-api.binance.vision,https://api1.binance.com,https://api2.binance.com,https://api3.binance.com,https://api4.binance.com
BINANCE_WS_BASE_URL=wss://stream.binance.com:443
```

If public REST is blocked by the current network, prefer `https://data-api.binance.vision` or a VPN. Do not switch to production private trading endpoints.

## Health Checks

```bash
curl -fsS http://127.0.0.1:3100/system/health
curl -fsS http://127.0.0.1:3100/metrics >/dev/null
curl -I http://127.0.0.1:3101
./scripts/verify-research-loop.sh
```

Use `AEGIS_ACCESS_TOKEN` with the smoke script when authenticated research read endpoints should be checked.

## VPS Read-Only Scheduled Research Validation

Use the VPS read-only validator after deployment or during scheduler triage when you need evidence without mutating production state. SSH to the VPS and run it from the deployed repo:

```bash
ssh tencent
cd /app/aegis-quant
./scripts/validate-vps-readonly.sh
```

The validator is designed for Docker-based VPS deployments. If `AEGIS_READONLY_DATABASE_URL` is set, it uses local `psql` with that URL and expects the URL to use the `aegis_readonly` role. If the URL is not set and the `aegis-quant-postgres` container is running, it runs:

```bash
docker exec -i aegis-quant-postgres psql -U aegis_readonly -d aegis_quant -c "<SELECT * FROM ai_read...>"
```

This means Postgres does not need to expose a host port for validation. If neither a read-only URL nor the Docker container is available, DB checks are reported as `WARN` unless `--strict` is passed.

Run with VPS CLI auth token auto-load (no token required on command line):

```bash
AEGIS_API_BASE_URL=http://127.0.0.1:3100 \
AEGIS_DASHBOARD_URL=http://127.0.0.1:3101 \
./scripts/validate-vps-readonly.sh
```

`validate-vps-readonly.sh` is read-only and does not print secrets.
It uses `AEGIS_ACCESS_TOKEN` when set, otherwise loads `~/.config/aegis/token.json` as fallback.

## Fix stale validator token

If authenticated checks return `401`, refresh the CLI token cache and re-run the validator:

```bash
unset AEGIS_ACCESS_TOKEN
aegislogin

export AEGIS_API_BASE_URL=http://127.0.0.1:3100
export AEGIS_ACCESS_TOKEN="$(jq -r '.access_token' ~/.config/aegis/token.json)"

bash scripts/validate-vps-readonly.sh
```

- `AEGIS_ACCESS_TOKEN` in the environment has priority in the validator.
- If it is stale, unset it before running the validator.
- The validator never prints token values.

Useful modes:

```bash
./scripts/validate-vps-readonly.sh --skip-db
./scripts/validate-vps-readonly.sh --skip-api
./scripts/validate-vps-readonly.sh --strict
./scripts/validate-vps-readonly.sh --json
```

The script only uses `docker ps`, `docker logs --tail`, `curl` GET requests, `psql` SELECT statements against `ai_read` views, and Docker exec into `aegis-quant-postgres` as `aegis_readonly`. It does not run sync, restart containers, apply migrations, create jobs, run jobs, call POST endpoints, or touch execution paths. If a token is not available, authenticated scheduled research API checks are reported as `WARN` and skipped.

DB validation queries are limited to these read-only views:

```sql
SELECT * FROM ai_read.candle_coverage;
SELECT * FROM ai_read.execution_safety_counts;
SELECT * FROM ai_read.shadow_decision_summary;
SELECT * FROM ai_read.research_candidate_status;
SELECT * FROM ai_read.walk_forward_status;
```

Expected healthy shape:

```txt
Aegis VPS read-only validation
...
== API Health ==
OK   GET /system/health HTTP 200

== Dashboard ==
OK   dashboard HTTP 200

== Containers ==
OK   aegis-quant-api running; no meaningful warning patterns in last 80 lines
OK   aegis-quant-scheduled-research-runner running; no meaningful warning patterns in last 80 lines

== Market Feed ==
OK   GET /market/feed-status HTTP 200; feeds=3 stale_or_degraded=0

== Aggregation Status ==
OK   GET /market/candles/aggregation-status HTTP 200; rows=9 stale_or_missing=0

== Scheduled Jobs ==
OK   GET /research/scheduled-jobs HTTP 200; jobs=14 enabled=14 auto_paused=0 backing_off=0

== Database ==
OK   DB validation mode: docker exec aegis-quant-postgres as aegis_readonly

== Execution Safety ==
OK   ai_read.execution_safety_counts all reported counts are zero
orders|0
paper_positions|0
paper_fills|0
exchange_testnet_orders|0

== Summary ==
OK=... WARN=0 FAIL=0
```

`OK` means a read-only check completed successfully. `WARN` means the check was unavailable, skipped, stale, missing, or otherwise worth operator attention without making the validator fail by default. `FAIL` means a required endpoint was unreachable or returned an unexpected hard error. With `--strict`, missing `ai_read` views and skipped DB validation become failures.

If an `ai_read` view is missing, the validator prints a warning like:

```txt
WARN ai_read.walk_forward_status missing or inaccessible; install/grant the ai_read read-only view for VPS validation
```

Do not inspect the underlying tables with write-capable credentials as part of validation. Apply or repair the `ai_read` schema through the normal reviewed migration/deployment path, then rerun the validator. On the VPS, keep validation read-only: do not run migrations, `syncaegis`, Docker restarts, scheduled jobs, POST requests, or direct SQL against non-`ai_read` tables during this check.

If scheduler jobs are `AUTO_PAUSED` or `BACKING_OFF`, do not immediately reset failures. First inspect the listed job name, last failure reason, and scheduler logs. Fix the underlying input problem, usually provider reachability, missing candles, stale aggregation, or report-generation dependencies. Only after the cause is understood should an operator use the normal authenticated reset or resume flow.

If aggregation is stale or missing, confirm the market ingest and candle aggregator containers are running and that 1m candle freshness is healthy. Prefer read-only inspection first: feed status, aggregation status, and `ai_read.candle_coverage` if available. Do not run repair, backfill, aggregation POSTs, migrations, or restarts as part of validation; handle those as a separate operator action with an explicit backup/maintenance plan when needed.

If `ai_read.execution_safety_counts` reports non-zero counts on a VPS that should be research-only, stop treating the environment as clean. Do not clear rows or run destructive SQL. Capture the counts, review audit logs and operator history, identify which execution surface produced the rows, and keep scheduled research paused until the source is understood. Non-zero paper or testnet counts may be expected only if the VPS is intentionally running those isolated modes.

## Scheduled Research Runner

The scheduled research runner is disabled by default. When `SCHEDULED_RESEARCH_RUNNER_ENABLED=false`, the runner stays alive in idle mode, logs `scheduled research runner disabled; idling`, and does not connect to the database or process jobs. The idle sleep interval is controlled by `SCHEDULED_RESEARCH_DISABLED_SLEEP_SECONDS` and defaults to `300`. In this state the Docker container should show `Up`, not `Restarting`.

Keep the runner disabled during deploys, run migrations, make sure the host `aegis` CLI is current if using it on the VPS, then bootstrap low-risk monitoring jobs first:

```bash
docker compose -f infra/docker-compose.yml --env-file .env --profile migrate run --rm migrate
./scripts/install-vps-cli.sh
./scripts/refresh-vps-scheduled-runner.sh
cargo run -p cli -- research scheduled-jobs bootstrap-safe --dry-run
cargo run -p cli -- research scheduled-jobs bootstrap-safe
cargo run -p cli -- research scheduled-jobs list
```

Only run `scheduled-jobs bootstrap-safe` after the host CLI supports `aegis research scheduled-jobs --help` and the disabled scheduler container is idling instead of restart-looping.

The safe bootstrap creates only:

- `provider-health-binance` every 15 minutes
- `aggregation-status` every 5 minutes
- `market-data-quality-<SYMBOL>-<INTERVAL>` every 30 minutes for configured symbols and `1m,5m,15m,1h`
- `operator-report-daily` every 24 hours

Jobs are created disabled unless `--enable` is passed. The normal VPS path is:

```bash
cargo run -p cli -- research scheduled-jobs bootstrap-safe --dry-run
cargo run -p cli -- research scheduled-jobs bootstrap-safe
cargo run -p cli -- research scheduled-jobs bootstrap-safe --enable
# set SCHEDULED_RESEARCH_RUNNER_ENABLED=true in .env first, then restart the scheduler
docker compose -f infra/docker-compose.yml --env-file .env --profile research-scheduler up -d scheduled-research-runner
```

Use explicit symbols or intervals when needed:

```bash
cargo run -p cli -- research scheduled-jobs bootstrap-safe --symbols BTCUSDT,ETHUSDT --intervals 1m,5m,15m,1h --dry-run
```

Running the bootstrap repeatedly is idempotent by job name and does not create campaign, batch, regime discovery, or robustness matrix jobs. `--replace-existing` updates definitions for existing bootstrap jobs; without it, `--enable` only changes enabled state for existing jobs.

Monitor auto-paused or failing jobs:

```bash
cargo run -p cli -- research scheduled-jobs list
cargo run -p cli -- research scheduled-jobs runs <job-id> --limit 20
cargo run -p cli -- research scheduled-jobs reset-failures <job-id>
```

## Common Failures

Missing migrations:

- Symptom: API returns 500s or logs `relation does not exist`.
- Fix: run the migration service, then restart API/workers.

Binance public REST blocked:

- Symptom: provider health reports unreachable Binance REST.
- Fix: set `BINANCE_REST_BASE_URL=https://data-api.binance.vision`, keep fallback URLs populated, or validate through a network/VPN that can reach public Binance data.

CORS/auth mismatch:

- Symptom: dashboard login succeeds in API logs but browser remains unauthenticated or refresh fails.
- Fix: align `AEGIS_CORS_ALLOWED_ORIGINS` with the browser origin and set `NEXT_PUBLIC_API_BASE_URL` to the API URL reachable from the browser.

Dashboard hydration extension warning:

- Symptom: browser console reports hydration mismatch with no API error and UI still works.
- Fix: retest in a clean profile or disable browser extensions that inject DOM nodes. Treat it as a browser-extension warning unless the production build also renders broken UI.

## Safety

No live trading is implemented. Testnet submit paths require owner auth and typed confirmation. Research smoke checks are read-only by default; `scripts/verify-research-loop.sh --with-research-run` requires an existing plan ID and verifies execution table counts before/after.
