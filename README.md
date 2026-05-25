# Aegis Quant

Deterministic execution infrastructure for risk-gated autonomous market systems.

## v0.1 disclaimer

- Not an AI trading bot
- Not financial advice
- No live trading support in v0.1
- Testnet-only exchange execution

## Why it exists

Aegis Quant is a Rust-first execution stack for operators who care about deterministic state machines, explicit risk gates, replay and backtest, paper accounting, shadow mode, isolated testnet lifecycle and reconciliation, operational dashboard and CLI surfaces, and readiness-driven reporting.

## Quick architecture

```txt
Public Market Data
        |
        v
     Candles  <----- historical backfill / deterministic candle builder
        |
        v
     Strategy
        |
        v
       Risk
        |
        v
  +-----+-------------------+------------------------+-------------------+
  |                         |                        |                   |
  v                         v                        v                   v
Paper Pipeline         Shadow Runner         Testnet Promotion      Analytics /
(simulated only)       (does not submit)     (isolated testnet)     Reports /
  |                         |                        |                Readiness
  +-------------------------+------------------------+-------------------+

Live Trading: not implemented
```

## What works in v0.1

- [x] Auth-gated operational API
- [x] Binance public market ingest
- [x] Historical candle backfill
- [x] Deterministic candle builder
- [x] Strategy config validation
- [x] Risk config validation
- [x] Paper pipeline
- [x] Paper PnL/accounting
- [x] Replay/backtest
- [x] Shadow runner
- [x] Testnet order lifecycle
- [x] Testnet reconciliation
- [x] Private stream skeleton
- [x] Promotion gate
- [x] Readiness gate
- [x] Operator report
- [x] CLI/dashboard/Prometheus

## What does not exist

- No live trading
- No production Binance trading endpoint
- No real-money execution
- No LLM decision-maker
- No auto-submit from strategy
- No HFT
- No leverage, futures, or options
- No financial promise

## 30-second demo path

```bash
cp .env.example .env
make verify
./scripts/demo-v0.1.sh
```

## Safety boundaries

- Execution flow remains `market event -> signal -> risk decision -> order intent -> execution state`.
- Strategy logic cannot submit orders directly.
- Kill switch state is persistent.
- Paper is simulated only.
- Shadow mode persists would-submit state and does not submit.
- Testnet execution is isolated from paper and uses testnet-only authenticated exchange actions.
- Live trading is not implemented.
- Readiness, analytics, and reports are read-only decision support.
- Strategy experiments are research-only parameter sweeps on stored candles; they must not mutate live, paper, shadow, promotion, or testnet execution state.
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
3. Apply database migrations:
   `docker compose -f infra/docker-compose.yml --env-file .env up -d postgres`
   `docker compose -f infra/docker-compose.yml --env-file .env --profile migrate run --rm migrate`
4. Start core services:
   `docker compose -f infra/docker-compose.yml --env-file .env up -d api`
5. Bootstrap the owner:
   `curl -X POST http://127.0.0.1:3000/auth/bootstrap-owner`
6. Log in:
   `cargo run -p cli -- auth login --email "$AEGIS_BOOTSTRAP_OWNER_EMAIL" --password "$AEGIS_BOOTSTRAP_OWNER_PASSWORD"`
7. Optional dashboard:
   `docker compose -f infra/docker-compose.yml --env-file .env --profile dashboard up -d dashboard`
   This Compose profile builds and runs the dashboard as a production Next.js container for VPS/deployed usage.
8. Optional market ingest:
   `docker compose -f infra/docker-compose.yml --env-file .env --profile ingest up -d market-ingest`
9. Optional shadow runner:
   `docker compose -f infra/docker-compose.yml --env-file .env --profile shadow up -d testnet-shadow-runner`
10. Optional Prometheus:
   `docker compose -f infra/docker-compose.yml --env-file .env --profile prometheus up -d prometheus`

Local dashboard auth note:
`AEGIS_CORS_ALLOWED_ORIGINS` defaults to `http://localhost:3001,http://127.0.0.1:3001` so the dashboard can call the API at `http://localhost:3000` and receive the refresh-token cookie on `/auth/login` and `/auth/refresh`. For production, add explicit origins such as `https://aegis.anggaprytn.com` via env instead of using `*`.

## Compose profiles

Core API + DB:
`docker compose -f infra/docker-compose.yml --env-file .env up -d postgres api`

Migrations:
`docker compose -f infra/docker-compose.yml --env-file .env --profile migrate run --rm migrate`

Migration note:
The `migrate` service currently applies raw SQL files from `crates/db/migrations` in sorted order with `psql`. This is acceptable for current VPS bootstrap, but rerunning raw SQL may fail if any migration is non-idempotent. Treat this path as intended for fresh/bootstrap VPS databases until a proper `sqlx migrate` image is added.

Dashboard:
`docker compose -f infra/docker-compose.yml --env-file .env --profile dashboard up -d dashboard`

For local frontend development, run the dashboard manually with `npm --prefix apps/dashboard install` and `npm --prefix apps/dashboard run dev -- --hostname 0.0.0.0 --port 3001`.

If you run the dashboard outside Compose on `http://localhost:3001`, keep `AEGIS_CORS_ALLOWED_ORIGINS` aligned with the browser origin. Example:
`AEGIS_CORS_ALLOWED_ORIGINS=http://localhost:3001,http://127.0.0.1:3001,https://aegis.anggaprytn.com`

Market ingest:
`docker compose -f infra/docker-compose.yml --env-file .env --profile ingest up -d market-ingest`

Shadow runner:
`docker compose -f infra/docker-compose.yml --env-file .env --profile shadow up -d testnet-shadow-runner`

Prometheus:
`docker compose -f infra/docker-compose.yml --env-file .env --profile prometheus up -d prometheus`

## Deploy

For VPS or any fresh environment, run migrations before starting API or DB-backed workers:

```bash
docker compose -f infra/docker-compose.yml --env-file .env up -d postgres
docker compose -f infra/docker-compose.yml --env-file .env --profile migrate run --rm migrate
docker compose -f infra/docker-compose.yml --env-file .env up -d api
docker compose -f infra/docker-compose.yml --env-file .env --profile dashboard up -d dashboard
docker compose -f infra/docker-compose.yml --env-file .env --profile ingest up -d market-ingest
docker compose -f infra/docker-compose.yml --env-file .env --profile shadow up -d testnet-shadow-runner
```

If your VPS bootstrap script is `/usr/local/bin/syncaegis`, it should run the exact migration command below after Postgres is healthy and before starting `api`, `dashboard`, or any workers:

`docker compose -f infra/docker-compose.yml --env-file .env --profile migrate run --rm migrate`

Optional workers:

- `market-ingest` uses public Binance market data only.
- `testnet-shadow-runner` never submits orders.
- No live trading is implemented.
- No production Binance endpoints are used.

Worker logs:
`docker logs -f aegis-quant-market-ingest`

`docker logs -f aegis-quant-shadow-runner`

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

Aggregate higher timeframe candles from stored 1m candles:
`cargo run -p cli -- market aggregate-candles --symbol BTCUSDT --source 1m --target 5m --start 2026-05-23T00:00:00Z --end 2026-05-24T00:00:00Z`

Inspect persisted candle coverage by interval:
`cargo run -p cli -- market candle-coverage --symbol BTCUSDT`

Backtest example:
`cargo run -p cli -- backtest run --strategy momentum_v1 --symbol BTCUSDT --timeframe 1m --start 2026-05-01T00:00:00Z --end 2026-05-02T00:00:00Z --initial-capital 1000000 --fee-bps 10 --slippage-bps 5 --holding-candles 3`

Higher timeframe backtest example:
`cargo run -p cli -- backtest run --strategy momentum_v1 --symbol BTCUSDT --timeframe 5m --start 2026-05-23T00:00:00Z --end 2026-05-24T00:00:00Z --initial-capital 1000000 --fee-bps 10 --slippage-bps 5 --holding-candles 3`

Strategy experiment sweep:
`cargo run -p cli -- experiments strategy run --strategy momentum_v1 --symbol BTCUSDT --timeframe 1m --start 2026-05-01T00:00:00Z --end 2026-05-02T00:00:00Z --initial-capital 1000000 --fee-bps 10 --slippage-bps 5 --lookbacks 3,5,10,20 --holding-candles 3,5,10 --max-signal-age-ms 180000 --max-runs 12`

List persisted strategy experiments:
`cargo run -p cli -- experiments strategy list`

Inspect a persisted strategy experiment:
`cargo run -p cli -- experiments strategy get <experiment_id>`

List ranked candidate runs for an experiment:
`cargo run -p cli -- experiments strategy runs <experiment_id>`

Readiness example:
`cargo run -p cli -- readiness check --target PAPER_PIPELINE --symbol BTCUSDT --strategy momentum_v1 --timeframe 1m`

Operator report example:
`cargo run -p cli -- reports operator daily --start 2026-05-24T00:00:00Z --end 2026-05-24T23:59:59Z --symbol BTCUSDT --strategy momentum_v1 --format markdown`

Optional shadow example:
`cargo run -p cli -- exchange testnet shadow-run --strategy momentum_v1 --symbol BTCUSDT --timeframe 1m`

## Strategy experiment interpretation

- `very_high_trade_count`: turnover is high enough that fee drag and slippage assumptions may dominate the edge.
- `negative_after_fees`: the candidate ended negative once transaction costs were included.
- `high_drawdown`: the candidate took materially deep peak-to-trough losses during the replay window.
- `too_few_trades`: the sample is too small to trust the ranking.

Strategy experiments are for research only. They reuse the deterministic replay engine, persist into isolated `strategy_experiments` and `strategy_experiment_runs` tables, and do not update persisted strategy config or execution state.

## Notes

- The tracked `apps/dashboard/tsconfig.tsbuildinfo` file was a generated artifact and is now ignored. Fresh builds will recreate it locally without polluting git status.
- `crates/llm-analyst` remains present in the workspace as an unused boundary only. No LLM integration is enabled in v0.1.
- Public market-data ingest/backfill still use Binance public endpoints today. Authenticated exchange functionality remains isolated to Binance Spot Testnet only.
