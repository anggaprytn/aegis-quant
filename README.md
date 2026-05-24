# Aegis Quant

Aegis Quant is Rust-first deterministic execution infrastructure for market-data ingest, replay, risk-gated paper execution, isolated testnet workflows, and operator auditability. It is not an AI trading bot, it does not enable live trading in v0.1, and no production exchange order path is supported.

## v0.1 scope

v0.1 is a local, demo-ready hardening release for:

- public market-data ingest and historical backfill
- validated strategy configuration and deterministic dry-run evaluation
- persisted risk decisions and paper-only order lifecycle
- paper positions, PnL, equity snapshots, and manual close
- deterministic replay/backtest over stored candles
- read-only readiness checks, analytics, and operator reports
- isolated Binance Spot Testnet preview, shadow mode, reconciliation, and owner-confirmed submit path
- dashboard cockpit, CLI fallback, Prometheus metrics, and Docker Compose for local services

v0.1 does not support:

- live trading
- production exchange private endpoints
- automatic testnet submission
- LLM-assisted execution
- NATS, Kafka, or distributed orchestration

## Safety boundaries

- Execution flow remains `market event -> signal -> risk decision -> order intent -> execution state`.
- Strategy logic cannot submit orders directly.
- Kill switch state is persistent.
- Readiness, analytics, and reports are read-only decision support.
- Shadow mode is no-submit by design.
- Testnet submission is owner-confirmed only.
- Public Binance market-data endpoints may be used for ingest/backfill; authenticated exchange actions remain testnet-only.

## Repository layout

```txt
crates/           Rust services and libraries
apps/dashboard/   Next.js operator cockpit
infra/            Docker Compose and Prometheus config
docs/             Architecture, security, and operator docs
scripts/          Local helper scripts, including the v0.1 demo flow
```

## Documentation

- [Release notes](./RELEASE_NOTES.md)
- [Documentation index](./docs/README.md)
- [Architecture overview](./docs/ARCHITECTURE_OVERVIEW.md)
- [Operator checklist](./docs/OPERATOR_CHECKLIST.md)
- [Security checklist](./docs/SECURITY_CHECKLIST.md)

## Local prerequisites

- Rust toolchain with `cargo`
- Node.js 20+ and npm for the dashboard
- Docker and Docker Compose for local Postgres and optional services
- PostgreSQL reachability from the configured `DATABASE_URL`

## Quick start

1. Copy `.env.example` to `.env`.
2. Review `.env` and set a real local `AEGIS_JWT_SECRET`.
3. Start core services:
   `docker compose -f infra/docker-compose.yml up -d postgres api`
4. Bootstrap the owner:
   `curl -X POST http://127.0.0.1:3000/auth/bootstrap-owner`
5. Log in:
   `cargo run -p cli -- auth login --email "$AEGIS_BOOTSTRAP_OWNER_EMAIL" --password "$AEGIS_BOOTSTRAP_OWNER_PASSWORD"`
6. Optional dashboard:
   `docker compose -f infra/docker-compose.yml --profile dashboard up -d dashboard`
7. Optional Prometheus:
   `docker compose -f infra/docker-compose.yml --profile prometheus up -d prometheus`

## Compose profiles

Core API + DB:
`docker compose -f infra/docker-compose.yml up -d postgres api`

Dashboard:
`docker compose -f infra/docker-compose.yml --profile dashboard up -d dashboard`

Prometheus:
`docker compose -f infra/docker-compose.yml --profile prometheus up -d prometheus`

Optional workers:

- `market-ingest` is not wired into Compose today; run it directly with `cargo run -p market-ingest`.
- The testnet private-stream worker is not wired into Compose today; run the existing Rust binary directly when needed.
- The testnet shadow runner is not wired into Compose today; run it directly with `cargo run -p api --bin testnet-shadow-runner`.

## Verification

Core verification:

```bash
cargo fmt --all
cargo check
cargo test
cargo test -p api --test pipeline_persistence --no-run
cargo test -p db --test integration_db --no-run
npm --prefix apps/dashboard install
npm --prefix apps/dashboard run typecheck
npm --prefix apps/dashboard run build
```

Convenience target:

```bash
make verify
```

## Demo flow

Use the defensive demo script:

```bash
./scripts/demo-v0.1.sh
```

Optional flags:

- `./scripts/demo-v0.1.sh --with-checks`
- `./scripts/demo-v0.1.sh --with-compose`
- `./scripts/demo-v0.1.sh --with-checks --with-compose`

The script does not submit testnet orders and does not require Binance credentials for the base flow.

## Common commands

Owner bootstrap:
`curl -X POST http://127.0.0.1:3000/auth/bootstrap-owner`

Health:
`curl http://127.0.0.1:3000/system/health`

Feed status:
`curl http://127.0.0.1:3000/market/feed-status`

Backfill example:
`cargo run -p cli -- market backfill --symbol BTCUSDT --timeframe 1m --start 2026-05-01T00:00:00Z --end 2026-05-02T00:00:00Z`

Backtest example:
`cargo run -p cli -- backtest run --strategy momentum_v1 --symbol BTCUSDT --timeframe 1m --start 2026-05-01T00:00:00Z --end 2026-05-02T00:00:00Z --initial-capital 1000000 --fee-bps 10 --slippage-bps 5 --holding-candles 3`

Readiness example:
`cargo run -p cli -- readiness check --target PAPER_PIPELINE --symbol BTCUSDT --strategy momentum_v1 --timeframe 1m`

Operator report example:
`cargo run -p cli -- reports operator daily --start 2026-05-24T00:00:00Z --end 2026-05-24T23:59:59Z --symbol BTCUSDT --strategy momentum_v1 --format markdown`

Optional shadow example:
`cargo run -p cli -- exchange testnet shadow-run --strategy momentum_v1 --symbol BTCUSDT --timeframe 1m`

## Notes

- The tracked `apps/dashboard/tsconfig.tsbuildinfo` file was a generated artifact and is now ignored. Fresh builds will recreate it locally without polluting git status.
- `crates/llm-analyst` remains present in the workspace as an unused boundary only. No LLM integration is enabled in v0.1.
- Public market-data ingest/backfill still use Binance public endpoints today. Authenticated exchange functionality remains isolated to Binance Spot Testnet only.
