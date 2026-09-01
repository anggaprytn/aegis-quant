# Aegis Quant v0.1 Release Notes

## What Aegis Quant is

Aegis Quant v0.1 is deterministic execution infrastructure for local research, paper execution, replay, and guarded exchange-testnet workflows. It is designed around explicit state transitions, persisted audit trails, replayability, reconciliation visibility, and operator control.

This release is not an AI trading bot. It does not grant execution authority to any LLM component, and no live trading path is enabled.

## What v0.1 supports

- Rust workspace with shared core types, Postgres migrations, and operational API
- Persistent kill switch and role-gated auth bootstrap/login flow
- Binance public market-data ingest and REST historical backfill for stored candles
- Deterministic strategy config validation, versioning, audit logging, and dry-run evaluation
- Risk-gated paper execution pipeline with persisted order lifecycle
- Paper positions, PnL, equity snapshots, and manual close with typed confirmation
- Deterministic replay/backtest over stored candles
- Read-only readiness checks, analytics, and operator daily reports
- Isolated Binance Spot Testnet adapter, reconciliation, private-stream skeleton, and owner-confirmed submit path
- Testnet shadow mode and shadow runner that persist would-submit state without submitting by default
- Dashboard cockpit, CLI fallback, Docker Compose for local API/Postgres/dashboard/Prometheus flows, and Prometheus metrics

## What v0.1 does not support

- Live trading
- Production exchange order submission
- Production Binance private endpoints
- Automatic testnet submission by default
- Strategy direct-to-exchange execution
- LLM-controlled execution
- Multi-exchange routing
- NATS, Kafka, or distributed orchestration

## Safety boundaries

- Trade-like flow remains `market event -> signal -> risk decision -> order intent -> execution state`
- Kill switch state is persisted and must be honored before paper or testnet actions
- Paper accounting is isolated from testnet execution tables
- Shadow mode is no-submit and read-mostly by design
- Testnet submit requires owner authorization and explicit typed confirmation
- Readiness, analytics, and operator reports are inspection-only
- Public Binance market-data endpoints may be used for ingest/backfill; authenticated exchange actions remain testnet-only

## How to run locally

1. Copy `.env.example` to `.env`.
2. Start PostgreSQL and apply migrations:
   `docker compose -f infra/docker-compose.yml --env-file .env up -d postgres`
   `docker compose -f infra/docker-compose.yml --env-file .env --profile migrate run --rm migrate`
3. Start the API and bootstrap the owner:
   `docker compose -f infra/docker-compose.yml --env-file .env up -d api`
   `curl -X POST http://127.0.0.1:3100/auth/bootstrap-owner`
4. Log in from the CLI:
   `cargo run -p cli -- auth login --email "$AEGIS_BOOTSTRAP_OWNER_EMAIL" --password "$AEGIS_BOOTSTRAP_OWNER_PASSWORD"`
5. Optional dashboard:
   `docker compose -f infra/docker-compose.yml --env-file .env --profile dashboard up -d dashboard`
6. Optional Prometheus:
   `docker compose -f infra/docker-compose.yml --env-file .env --profile prometheus up -d prometheus`

## Verification commands

```bash
cargo fmt --all
cargo check
cargo test
cargo test -p api --test pipeline_persistence --no-run
cargo test -p db --test integration_db --no-run
npm --prefix apps/dashboard install
npm --prefix apps/dashboard run typecheck
npm --prefix apps/dashboard run build
make verify
```

## Known limitations

- Local single-tenant auth bootstrap and operator workflow only
- Docker Compose currently wires API, Postgres, dashboard, and Prometheus; optional ingest and testnet workers are still launched as Rust binaries
- Public market ingest/backfill depends on Binance public market-data availability
- Testnet private-stream and reconciliation flows are present but remain bounded to testnet-only operations
- `crates/llm-analyst` exists in the workspace as an unused boundary; no LLM execution path is enabled or documented for v0.1

## Next milestones

- Tighten operational runbooks and failure-recovery drills around readiness, reconciliation, and kill-switch handling
- Expand compose and service packaging for optional ingest and shadow/testnet worker processes without changing safety boundaries
- Increase test coverage around operator workflows and release packaging
- Prepare a stricter v0.2 release candidate focused on hardening and operability, not live trading
