# Development Guide

Aegis Quant is a Rust workspace with a separately built Next.js dashboard. The normal development loop is local, deterministic, and database-aware.

## Prerequisites

- Rust and Cargo
- Node.js 20 or newer and npm
- Docker Compose for a disposable PostgreSQL instance
- Bash, curl, and standard Unix tools for the helper scripts

The repository Dockerfile currently builds with Rust 1.88. There is no checked-in Rust toolchain pin, so local contributors should use a current toolchain that remains compatible with the workspace.

## Initial setup

~~~bash
cp .env.example .env
npm --prefix apps/dashboard ci
~~~

Review .env before starting services. The example database password is intentionally local-only; keep the API JWT secret and bootstrap password private.

Start PostgreSQL and apply the migration ledger:

~~~bash
docker compose -f infra/docker-compose.yml --env-file .env up -d postgres
docker compose -f infra/docker-compose.yml --env-file .env --profile migrate run --rm migrate
~~~

Run the API with Compose or directly through Cargo:

~~~bash
docker compose -f infra/docker-compose.yml --env-file .env up -d api

# Direct process alternative; source .env first and ensure DATABASE_URL is host-reachable.
cargo run -p api --bin api
~~~

## Verification commands

The repository's primary verification target is:

~~~bash
make verify
~~~

It runs:

- cargo fmt --all --check;
- cargo check;
- cargo test;
- compile-only checks for the database-backed integration tests;
- dashboard typecheck and production build;
- shell syntax validation for the demo script;
- git diff --check.

If you only need an individual check:

~~~bash
cargo fmt --all
cargo check
cargo test
npm --prefix apps/dashboard run typecheck
npm --prefix apps/dashboard run build
~~~

Unit and library tests run by default. PostgreSQL-backed tests are marked ignored because they reset a database and need an explicit test URL.

## Integration tests

Use a disposable database whose name contains test. The test support code refuses a non-test database unless ALLOW_NON_TEST_DB=1 is explicitly set.

Host-run integration tests need a host-reachable PostgreSQL URL. The default
Compose file deliberately does not publish PostgreSQL on the host, so either
use an existing local PostgreSQL installation or start a disposable test
container with a temporary host port:

~~~bash
docker run --detach --name aegis-quant-test-postgres --env POSTGRES_DB=aegis_quant_test --env POSTGRES_USER=aegis --env POSTGRES_PASSWORD=aegis-local-only --publish 127.0.0.1:5433:5432 postgres:16

export TEST_DATABASE_URL=postgres://aegis:aegis-local-only@127.0.0.1:5433/aegis_quant_test
~~~

Wait for PostgreSQL to become healthy, then source any remaining environment
variables and run:

~~~bash
set -a
source ./.env
set +a
make integration-test
~~~

The harness applies migrations, truncates its known tables, and ensures persistent system state before each test setup. Do not point it at a database containing valuable data.

The two integration suites can also be compiled without a database:

~~~bash
cargo test -p api --test pipeline_persistence --no-run
cargo test -p db --test integration_db --no-run
~~~

## Workspace map

| Package | Responsibility |
| --- | --- |
| api | Axum API, auth middleware, handlers, and worker-facing orchestration |
| cli | HTTP API client and operator command tree |
| aegis-core | Shared domain types, validation, events, and request/response models |
| db | SQLx/PostgreSQL access, migrations, persistence mapping, and test support |
| events | Event taxonomy and persistence publisher |
| market-ingest | Public market-data clients, candle construction, derivatives/microstructure research collectors |
| strategy-engine | Deterministic strategy configs, evaluation, diagnostics, and opportunity analysis |
| risk-engine | Risk configuration validation and rule evaluation |
| replay-engine | Backtest, experiments, walk-forward, and robustness analysis |
| execution-engine | Small execution-state interface boundary |
| accounting | Paper-accounting domain helpers |
| exchange | Isolated Binance Spot Testnet adapter and lifecycle mapping |
| telemetry | Prometheus metrics registry and instrumentation |
| llm-analyst | Dormant advisory boundary; not wired into execution |

## Database changes

Add schema changes as the next numbered SQL file under crates/db/migrations. The migration runner stores a SHA-256 checksum in schema_migrations, skips matching applied migrations, and stops on checksum mismatch. Do not edit an already-applied migration in place.

For local migrations:

~~~bash
cargo run -p cli -- db migrations status
cargo run -p cli -- db migrations migrate
~~~

Use the baseline command only for a reviewed existing deployment and follow the [Runbook](RUNBOOK.md). Do not use a production database for local experimentation.

## Design constraints

Changes should preserve the following:

- market event -> signal -> risk decision -> order intent -> execution state;
- decimal types for money, prices, balances, notional, and PnL;
- persistent kill-switch state;
- explicit role and typed-confirmation checks for dangerous actions;
- append-only audit and event records where state changes matter;
- isolation between research/backtest, paper, shadow, and testnet tables;
- no LLM execution authority and no live-trading path.

## Documentation and pull requests

Update the relevant guide when a command, route, worker, environment variable, migration, or safety boundary changes. Use the [contribution guide](CONTRIBUTING.md) and pull-request template. Never include .env, token files, exchange credentials, private URLs, or unredacted operational logs in a commit.
