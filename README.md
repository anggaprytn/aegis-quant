![Aegis Quant Cover](https://testing-1355450658.cos.ap-jakarta.myqcloud.com/aegis-quant.webp)

# Aegis Quant

Deterministic infrastructure for market-data research, risk-gated paper
execution, and isolated exchange-testnet operations.

> [!WARNING]
> Aegis Quant is experimental v0.1 software. It is not financial advice, an
> investment product, or a live-trading system. Live trading is not implemented.
> Do not connect real-money credentials or treat research, backtest, paper,
> shadow, or testnet results as evidence of future returns.

Aegis Quant is a Rust and PostgreSQL control plane for making trading
experiments and execution workflows inspectable. It ingests public market data,
stores deterministic candles, evaluates explicit strategy and risk rules, and
persists the events and state transitions needed to understand what happened.
Operators can use the HTTP API, the aegis CLI, or the Next.js dashboard over the
same backend state.

## Why it exists

Many trading experiments become difficult to trust before the strategy itself is
evaluated. Data may be incomplete, prices may be stale, signals may be
duplicated, accounting may drift, exchange state may require reconciliation, and
operator actions may be impossible to reconstruct.

Aegis Quant treats those concerns as infrastructure problems. The project is
for developers, researchers, and operators who want to exercise correctness,
auditability, and safety controls before considering capital deployment.

## Current scope

| Area        | Implemented scope                                                                                                                                                      |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Market data | Binance public WebSocket trade ingestion and public REST candle backfill                                                                                               |
| Storage     | PostgreSQL migrations, events, candles, signals, risk decisions, orders, research artifacts, and audit records                                                         |
| Research    | Data quality, aggregation, replay/backtest, experiments, walk-forward validation, robustness analysis, attribution, hypotheses, plans, candidates, and shadow evidence |
| Execution   | Risk-gated simulated paper flow and an isolated, owner-confirmed Binance Spot Testnet flow                                                                             |
| Interfaces  | Axum API, aegis CLI, Next.js dashboard, JSON output, and Prometheus-compatible metrics                                                                                 |
| AI boundary | llm-analyst is a dormant advisory boundary; no LLM has execution authority                                                                                             |
| Maturity    | Experimental, single-tenant, and local/host-oriented; not a managed production service                                                                                 |

## Safety model

All trade-like behavior is intended to remain traceable through:

```text
market event -> signal -> risk decision -> order intent -> execution state
```

The boundaries are explicit:

- Research and replay write research or backtest state only.
- Paper accounting uses separate simulated order, fill, position, and PnL state.
- Shadow observation records would-submit evidence and never submits orders.
- Testnet actions use isolated persistence and require authorization plus typed
  confirmation.
- A persistent PostgreSQL kill switch is checked before guarded actions.
- No production exchange private endpoint or live-trading path is implemented.

## Architecture

```text
              aegis CLI / Next.js dashboard
                           |
                           v
                    Axum operational API
                           |
       +-------------------+-------------------+
       |                   |                   |
       v                   v                   v
 public market data   research / replay   paper / testnet
   and workers        and strategy work   guarded workflows
       |                   |                   |
       +-------------------+-------------------+
                           |
                           v
                   PostgreSQL + migrations
                           |
                           v
              events, audit, metrics, reports
```

The repository is a Rust workspace with separate boundaries for core types,
database access, market ingest, strategy evaluation, risk, replay, accounting,
exchange state, execution state, telemetry, and the operator API.

## Quick start

### Requirements

- Rust and Cargo. The repository Docker build currently uses Rust 1.88.
- Node.js 20 or newer and npm for the dashboard.
- Docker with Compose v2.
- PostgreSQL if running the API or integration tests without Docker.

### Start PostgreSQL and the API

From the repository root:

```bash
cp .env.example .env

# Review .env and replace the example JWT and owner password.
set -a
source ./.env
set +a

docker compose -f infra/docker-compose.yml --env-file .env up -d postgres
docker compose -f infra/docker-compose.yml --env-file .env --profile migrate run --rm migrate
docker compose -f infra/docker-compose.yml --env-file .env up -d api

curl --fail --silent "$AEGIS_API_BASE_URL/system/health"
curl --fail --silent -X POST "$AEGIS_API_BASE_URL/auth/bootstrap-owner"
cargo run -p cli -- auth login --email "$AEGIS_BOOTSTRAP_OWNER_EMAIL" --password "$AEGIS_BOOTSTRAP_OWNER_PASSWORD"
```

The API is published on http://127.0.0.1:3100 by Compose and listens on port
3000 inside the container. Owner bootstrap is intended to run once.

### Start the dashboard

For frontend development:

```bash
npm --prefix apps/dashboard ci
npm --prefix apps/dashboard run dev -- --hostname 127.0.0.1 --port 3001
```

Open http://127.0.0.1:3001. The dashboard uses
NEXT_PUBLIC_API_BASE_URL from the environment. The containerized dashboard
profile is available on http://127.0.0.1:3101:

```bash
docker compose -f infra/docker-compose.yml --env-file .env --profile dashboard up -d dashboard
```

### Optional workers

Start only the workers required for the workflow you are testing:

```bash
docker compose -f infra/docker-compose.yml --env-file .env --profile ingest up -d market-ingest
docker compose -f infra/docker-compose.yml --env-file .env --profile aggregation up -d candle-aggregator
docker compose -f infra/docker-compose.yml --env-file .env --profile shadow up -d testnet-shadow-runner
docker compose -f infra/docker-compose.yml --env-file .env --profile research-scheduler up -d scheduled-research-runner
docker compose -f infra/docker-compose.yml --env-file .env --profile prometheus up -d prometheus
```

The scheduled research runner is disabled by default. Shadow mode is
observation-only and does not submit exchange orders.

## First workflow

After authentication, inspect the service, hydrate public candles, and run a
deterministic replay:

```bash
cargo run -p cli -- status
cargo run -p cli -- market provider-health --provider binance
cargo run -p cli -- market backfill --symbol BTCUSDT --timeframe 1m --start 2026-05-01T00:00:00Z --end 2026-05-02T00:00:00Z
cargo run -p cli -- market aggregate-candles --symbol BTCUSDT --source 1m --target 5m --start 2026-05-01T00:00:00Z --end 2026-05-02T00:00:00Z
cargo run -p cli -- backtest run --strategy momentum_v1 --symbol BTCUSDT --timeframe 1m --start 2026-05-01T00:00:00Z --end 2026-05-02T00:00:00Z --initial-capital 1000000 --fee-bps 10 --slippage-bps 5 --holding-candles 3
cargo run -p cli -- reports operator daily --format markdown
```

Use aegis --help and the [usage guide](docs/USAGE.md) for the complete command
tree and for mutation-specific safety requirements. Most supported commands
also provide JSON output.

## Operating modes

| Mode                | Purpose                                                                     | Exchange submission               |
| ------------------- | --------------------------------------------------------------------------- | --------------------------------- |
| Research and replay | Prepare data, evaluate strategies, and collect evidence                     | Never                             |
| Paper               | Exercise the signal, risk, order, and accounting path with simulated fills  | Never                             |
| Shadow              | Record whether a candidate would submit under current data and risk context | Never                             |
| Testnet             | Exercise isolated Binance Spot Testnet state and reconciliation             | Explicit, authorized actions only |

Research results and candidate qualification are decision support. They do not
automatically promote a candidate or create execution state.

## CLI and API

The operator CLI is the primary local fallback:

```bash
cargo run -p cli -- --help
cargo run -p cli -- market --help
cargo run -p cli -- research --help
cargo run -p cli -- exchange testnet --help
```

The API exposes route groups for system health, authentication, market data,
strategy and risk, paper accounting, backtests, research, isolated testnet
operations, analytics, reports, events, orders, readiness, and metrics. The
repository does not currently include a generated OpenAPI document; use the
handlers, CLI help, and [usage guide](docs/USAGE.md) as the source of truth.

## Configuration

Copy the .env.example file to .env and keep the latter untracked. Read the
[configuration reference](docs/CONFIGURATION.md) before changing
authentication, CORS, metrics, database URLs, worker behavior, or testnet
settings.

Important boundaries:

- Authentication is enabled by default and requires AEGIS_JWT_SECRET.
- AEGIS_AUTH_DISABLED=true is for isolated local development only.
- Public market-data variables do not authorize exchange actions.
- Testnet keys are optional, backend-only, and must remain pointed at Binance
  Spot Testnet.
- Never put exchange secrets in frontend or NEXT_PUBLIC variables.

## Development

Install dashboard dependencies and run the repository verification target:

```bash
npm --prefix apps/dashboard ci
make verify
```

The verification target runs Rust formatting, checks, tests, compile-only
database integration tests, dashboard typechecking/build, shell syntax checks,
and whitespace validation.

Database integration tests are ignored by default because they require a
disposable PostgreSQL database:

```bash
make integration-test
```

See [DEVELOPMENT.md](docs/DEVELOPMENT.md) for the test database setup and
[CONTRIBUTING.md](docs/CONTRIBUTING.md) for change expectations.

## Deployment and operations

Docker Compose is the supported packaging shape for local and host-style
deployments. Start PostgreSQL, apply migrations, then start the API and
optional workers in that order. Back up non-disposable databases before
migrations or maintenance.

The [operations runbook](docs/RUNBOOK.md) covers startup, migration behavior,
read-only validation, backups, worker scheduling, shutdown, and common
failures. This repository does not provide a hosted service, a secrets manager,
or a production live-trading deployment.

## Repository layout

```text
crates/
  api/               Axum API and worker binaries
  cli/               aegis operator CLI
  core/              shared domain types and validation
  db/                PostgreSQL access, migrations, and test support
  events/            event taxonomy and publisher boundary
  exchange/          Binance Spot Testnet adapter and state mapping
  execution-engine/  execution-state interface
  market-ingest/     public market-data clients and collectors
  replay-engine/     deterministic replay, backtest, and research analysis
  risk-engine/       risk rules and decision evaluation
  strategy-engine/   deterministic strategies and diagnostics
  telemetry/         Prometheus-compatible metrics
  llm-analyst/       dormant advisory boundary
apps/dashboard/      Next.js operator cockpit
infra/               Docker Compose and Prometheus configuration
scripts/              demo, integration, deployment, and validation helpers
docs/                 project, architecture, usage, security, and operations
```

## Documentation

- [Documentation index](docs/README.md)
- [Product requirements](docs/PRD.md)
- [Usage guide](docs/USAGE.md)
- [Configuration reference](docs/CONFIGURATION.md)
- [Development guide](docs/DEVELOPMENT.md)
- [Architecture overview](docs/ARCHITECTURE_OVERVIEW.md)
- [Detailed architecture](docs/ARCHITECTURE.md)
- [Research workflows](docs/RESEARCH.md)
- [Research milestone](docs/RESEARCH_MILESTONE.md)
- [Roadmap](docs/ROADMAP.md)
- [Operations runbook](docs/RUNBOOK.md)
- [Operator checklist](docs/OPERATOR_CHECKLIST.md)
- [Security model](docs/SECURITY.md)
- [Security policy](docs/SECURITY_POLICY.md)
- [Contribution guide](docs/CONTRIBUTING.md)
- [Code of Conduct](docs/CODE_OF_CONDUCT.md)
- [Release notes](docs/RELEASE_NOTES.md)
- [Sample evidence bundle](docs/examples/research-candidate-evidence-bundle.json)

## Roadmap and maturity

The current phase is research-control-plane hardening and evidence collection.
Near-term work focuses on migration and recovery drills, integration coverage,
data reliability, operator diagnostics, deployment hygiene, and manual
promotion gates.

Live trading, production exchange private endpoints, leverage, multi-exchange
routing, automatic promotion, and production secrets management are explicitly
deferred. See the [roadmap](docs/ROADMAP.md) for the current priorities.

## Contributing

Contributions are welcome around correctness, tests, documentation,
observability, and safe operator workflows. Read the
[contribution guide](docs/CONTRIBUTING.md) and [Code of Conduct](docs/CODE_OF_CONDUCT.md)
before opening a pull request.

## Security

Do not commit credentials, tokens, private URLs, unredacted logs, or production
data. Report suspected vulnerabilities privately using the process in the
[security policy](docs/SECURITY_POLICY.md). The
[security model](docs/SECURITY.md) documents the implemented boundaries.

## License

Aegis Quant is released under the [MIT License](LICENSE).

This software is provided for research and infrastructure experimentation. It
does not provide financial advice, investment recommendations, or guarantees of
profitability.
