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

## Scheduled Research Runner

The scheduled research runner is disabled by default. Keep it disabled during deploys, run migrations, then bootstrap low-risk monitoring jobs first:

```bash
docker compose -f infra/docker-compose.yml --env-file .env --profile migrate run --rm migrate
cargo run -p cli -- research scheduled-jobs bootstrap-safe --dry-run
cargo run -p cli -- research scheduled-jobs bootstrap-safe
cargo run -p cli -- research scheduled-jobs list
```

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
# set SCHEDULED_RESEARCH_RUNNER_ENABLED=true in .env first
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
