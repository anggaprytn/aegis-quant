# Operations Runbook

This runbook covers the supported local and host-style operating procedures for
Aegis Quant. It assumes a single deployment owner, PostgreSQL, Docker Compose,
and the safety boundary documented in the [security model](SECURITY.md).

This is experimental infrastructure. It has no live-trading path. Treat every
non-disposable database as production data even when the application is being
used only for research.

## Local startup

Create a local environment file and start services in dependency order:

~~~bash
cp .env.example .env

docker compose -f infra/docker-compose.yml --env-file .env up -d postgres
docker compose -f infra/docker-compose.yml --env-file .env --profile migrate run --rm migrate
docker compose -f infra/docker-compose.yml --env-file .env up -d api
~~~

The default host URLs are:

- API: http://127.0.0.1:3100
- Dashboard development server: http://127.0.0.1:3001
- Containerized dashboard profile: http://127.0.0.1:3101
- Prometheus profile: http://127.0.0.1:9090

Check the API and bootstrap the first owner after the API is healthy:

~~~bash
curl --fail --silent http://127.0.0.1:3100/system/health
curl --fail --silent -X POST http://127.0.0.1:3100/auth/bootstrap-owner
cargo run -p cli -- auth login --email "$AEGIS_BOOTSTRAP_OWNER_EMAIL" --password "$AEGIS_BOOTSTRAP_OWNER_PASSWORD"
~~~

Bootstrap is intended to be a one-time operation. A later call should report
that an owner already exists.

Start optional profiles only when they are needed:

~~~bash
docker compose -f infra/docker-compose.yml --env-file .env --profile dashboard up -d dashboard
docker compose -f infra/docker-compose.yml --env-file .env --profile ingest up -d market-ingest
docker compose -f infra/docker-compose.yml --env-file .env --profile aggregation up -d candle-aggregator
docker compose -f infra/docker-compose.yml --env-file .env --profile shadow up -d testnet-shadow-runner
docker compose -f infra/docker-compose.yml --env-file .env --profile research-scheduler up -d scheduled-research-runner
docker compose -f infra/docker-compose.yml --env-file .env --profile prometheus up -d prometheus
~~~

The scheduled research runner is disabled by default. Shadow observation is a
no-submit path; enabling a worker does not authorize exchange execution.

Stop local services with:

~~~bash
docker compose -f infra/docker-compose.yml --env-file .env down
~~~

## Migration procedure

Run migrations only after PostgreSQL reports healthy and before starting
database-backed application services:

~~~bash
docker compose -f infra/docker-compose.yml --env-file .env --profile migrate run --rm migrate
~~~

The migration runner maintains a ledger, skips already-applied matching
migrations, and stops on checksum mismatches or failed pending migrations.
Inspect the ledger explicitly when diagnosing a deployment:

~~~bash
docker compose -f infra/docker-compose.yml --env-file .env --profile migrate run --rm migrate aegis db migrations status
~~~

For a non-disposable database:

1. Record the current application and migration versions.
2. Take and verify a database backup.
3. Review the migration diff.
4. Run the migration container.
5. Recheck migration status and API health.
6. Inspect application logs before enabling optional workers.

Do not bypass a checksum mismatch by editing the migration ledger manually.
Resolve the migration history through a reviewed change.

## Host-style deployment order

The repository supports Docker Compose on a host or VPS, but does not provide a
managed deployment service. Keep host names, filesystem paths, synchronization
scripts, and credentials outside the public repository.

For a reviewed revision, use this order:

~~~bash
git fetch --prune
git status --short --branch
git pull --ff-only

docker compose -f infra/docker-compose.yml --env-file .env build api
docker compose -f infra/docker-compose.yml --env-file .env up -d postgres
docker compose -f infra/docker-compose.yml --env-file .env --profile migrate run --rm migrate
docker compose -f infra/docker-compose.yml --env-file .env up -d api
~~~

Build and start the dashboard or workers separately after the API is healthy:

~~~bash
docker compose -f infra/docker-compose.yml --env-file .env --profile dashboard up -d dashboard
docker compose -f infra/docker-compose.yml --env-file .env --profile ingest up -d market-ingest
docker compose -f infra/docker-compose.yml --env-file .env --profile aggregation up -d candle-aggregator
~~~

Do not reset volumes as part of a routine update. Do not run a broad
docker-compose-down operation when refreshing one worker. If a host-specific
helper is used, keep that helper outside the repository and make its ordering
equivalent to the sequence above.

## Backups

For the Compose PostgreSQL service, a simple logical backup is:

~~~bash
docker compose -f infra/docker-compose.yml --env-file .env exec -T postgres pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" > "backup-$(date +%Y%m%d-%H%M%S).sql"
~~~

Store backups outside the deployment directory, restrict their permissions, and
test restoration separately. Do not publish database dumps, research exports
containing private identifiers, or token-bearing logs.

## Read-only validation

The repository includes scripts/validate-vps-readonly.sh. The filename reflects
its original host-style use; the checks are also useful on a local Compose
deployment. It is intended to inspect state without mutating it:

~~~bash
export AEGIS_API_BASE_URL=http://127.0.0.1:3100
export AEGIS_DASHBOARD_URL=http://127.0.0.1:3101
./scripts/validate-vps-readonly.sh --skip-db
~~~

When a read-only database role and the ai_read views are configured:

~~~bash
export AEGIS_READONLY_DATABASE_URL=postgres://readonly-user:password@db-host/aegis_quant
./scripts/validate-vps-readonly.sh --strict
~~~

The validator uses health and read endpoints, container status/log tails, and
read-only queries against ai_read views. It does not run migrations, call
mutation endpoints, create jobs, run research, repair orders, or restart
containers. Without a token, authenticated checks are reported as warnings
unless an explicit auto-login flow is requested.

Use the validator to collect evidence, not to make the environment look clean.
Non-zero execution-safety counts require investigation and audit review; do not
delete rows or reset state to clear the result.

## Scheduled research runner

The scheduled research runner remains idle when
SCHEDULED_RESEARCH_RUNNER_ENABLED=false. Keep it disabled during migrations and
deploys. Bootstrap safe monitoring jobs in preview mode first:

~~~bash
cargo run -p cli -- research scheduled-jobs bootstrap-safe --dry-run
cargo run -p cli -- research scheduled-jobs bootstrap-safe
cargo run -p cli -- research scheduled-jobs list
~~~

The safe bootstrap is intended for provider health, aggregation status, market
data quality, and operator reporting. It does not create candidates, paper
orders, testnet orders, or live orders.

Candidate-specific observation jobs require a reviewed candidate identifier,
current shadow-runner coverage, and SHADOW_OBSERVATION_ONLY=true:

~~~bash
cargo run -p cli -- research scheduled-jobs create --name candidate-shadow-observe --kind CANDIDATE_SHADOW_OBSERVE_ONCE --interval-seconds 300 --request-json '{"candidate_id":"<candidate-id>"}'
~~~

Replace the placeholder with an identifier from the running system. This job
records observation evidence only and is intentionally not part of the safe
bootstrap.

Enable the scheduler only after reviewing the jobs and confirming the
deployment's data and safety posture:

~~~bash
docker compose -f infra/docker-compose.yml --env-file .env --profile research-scheduler up -d scheduled-research-runner
~~~

## Paper and testnet safety

Paper execution and Spot Testnet execution are separate persistence domains.
Before any operator action:

- Check system health and the persistent kill-switch state.
- Confirm the intended symbol, strategy, timeframe, and risk configuration.
- Use preview endpoints or CLI commands before applying a mutation.
- Keep testnet credentials backend-only and testnet-specific.
- Use the exact typed confirmation required by the command.
- Review the resulting event, audit, order-lifecycle, or reconciliation record.

Research, replay, analytics, readiness, and shadow observation do not grant
execution authority. No production exchange private endpoint should be
configured. See the [operator checklist](OPERATOR_CHECKLIST.md) and [security
model](SECURITY.md) for the detailed boundary.

## Common failures

### API reports a missing relation or migration error

Check PostgreSQL health and migration status, run the migration service, then
restart the API. Do not edit the migration ledger to skip a checksum mismatch.

### Dashboard login or refresh fails

Confirm that the browser origin is present in
AEGIS_CORS_ALLOWED_ORIGINS and that NEXT_PUBLIC_API_BASE_URL is reachable from
the browser. If cookies are served over HTTPS, use AEGIS_COOKIE_SECURE=true.

### Public Binance data is unavailable

Inspect provider health and logs. Public REST fallback hosts can be configured
with BINANCE_REST_FALLBACK_BASE_URLS. This does not enable private exchange
actions and should not be used to point authenticated requests at production
trading endpoints.

### Scheduled jobs are paused or backing off

Inspect the job's last failure, provider health, candle coverage, aggregation
status, and runner logs. Fix the underlying input problem before resetting
failures. A reset is not a substitute for understanding the cause.

### Read-only validation reports non-zero execution counts

Stop treating the environment as research-only. Preserve the evidence, review
events and audit records, identify the responsible workflow, and pause
scheduled research until the state is understood.

## Shutdown and emergency stop

For a suspected unsafe state, stop optional workers first and use the persistent
kill-switch control through the authenticated API or CLI. Then preserve logs and
database evidence for review. Do not rely on a process-local flag or a container
restart as the safety mechanism.

The project does not document a live-trading emergency procedure because live
trading is not implemented. Any future execution-surface change requires a
separate security and operational review.
