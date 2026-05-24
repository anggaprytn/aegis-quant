# Aegis Quant

Aegis Quant is a Rust-first autonomous quant execution infrastructure focused on deterministic signal flow, risk-gated paper execution, event logging, and operational auditability.

## Scope of this scaffold

This repository foundation includes:

- Rust workspace with bounded service and engine crates
- Operator CLI fallback through the existing HTTP API
- Shared core domain types using `rust_decimal`
- Minimal Axum API for health and system status
- Binance public WebSocket market ingest with deterministic 1m candle building
- Binance public REST historical candle backfill into closed stored candles
- Deterministic candle-only strategy evaluation for `momentum_v1` and `volatility_breakout_v1`
- Deterministic paper trading pipeline from closed candles to risk-gated paper order lifecycle
- Paper account, position, fill, journal, equity snapshot, and manual close accounting for operational paper trading
- Deterministic replay/backtest engine from stored candles and persisted strategy configs
- Strategy config validation, versioning, audit logging, and dry-run evaluation before pipeline/backtest use
- Read-only strategy performance analytics across backtest, paper, and shadow data
- Read-only testnet promotion funnel analytics from shadow would-submit through isolated testnet lifecycle outcome
- Read-only operator daily reports across health, market, risk, paper, shadow, promotion, and testnet execution state
- Minimal Next.js operational dashboard shell for paper-only inspection and control
- Binance Spot Testnet adapter skeleton with protected inspection and owner-gated testnet submit/cancel
- Event model and publisher trait skeleton
- Postgres migration baseline
- Local development Docker Compose setup
- Architecture, roadmap, and security documentation

## Non-goals in this scaffold

- Live trading
- Live exchange order execution
- Automatic strategy scheduling

## Quick start

1. Copy `.env.example` to `.env`.
2. Start Postgres with `docker compose -f infra/docker-compose.yml up -d postgres`.
3. Load the local environment with `set -a; source .env; set +a`.
4. Run `cargo fmt` and `cargo check`.
5. Start the API with `cargo run -p api`.
6. Bootstrap the local owner once:
   `curl -X POST http://127.0.0.1:3000/auth/bootstrap-owner`
7. Login from CLI:
   `cargo run -p cli -- auth login --email "$AEGIS_BOOTSTRAP_OWNER_EMAIL" --password "$AEGIS_BOOTSTRAP_OWNER_PASSWORD"`
8. Start market ingest with `cargo run -p market-ingest`.
9. Start the dashboard:
   `cd apps/dashboard`
   `npm install`
   `npm run dev`
10. Open `http://127.0.0.1:3001` and sign in.
11. Use the local operator CLI fallback when needed:
   `cargo run -p cli -- status`
12. Verify JSON endpoints:
   `curl http://127.0.0.1:3000/system/health`
   `curl http://127.0.0.1:3000/system/status`
   `curl http://127.0.0.1:3000/system/db-health`
   `curl http://127.0.0.1:3000/metrics`
   `curl 'http://127.0.0.1:3000/market/symbols'`
   `curl 'http://127.0.0.1:3000/market/ticks/latest?symbol=BTCUSDT'`
   `curl 'http://127.0.0.1:3000/market/candles?symbol=BTCUSDT&interval=1m&limit=100'`
   `curl -X POST http://127.0.0.1:3000/market/backfill/candles -H 'content-type: application/json' -d '{"exchange":"binance","symbol":"BTCUSDT","interval":"1m","start_time":"2026-05-01T00:00:00Z","end_time":"2026-05-02T00:00:00Z","limit_per_request":1000}'`
   `curl 'http://127.0.0.1:3000/market/backfill/runs?limit=20'`
   `curl 'http://127.0.0.1:3000/market/feed-status'`
   `curl 'http://127.0.0.1:3000/strategy/list'`
   `curl 'http://127.0.0.1:3000/strategy/momentum_v1/config'`
   `curl -X POST http://127.0.0.1:3000/strategy/momentum_v1/config/validate -H 'content-type: application/json' -d '{"strategy_id":"momentum_v1","enabled":true,"mode":"paper","symbols":["BTCUSDT"],"timeframe":"1m","suggested_notional":"100000","max_signal_age_ms":5000,"cooldown_seconds":900,"lookback_candles":3}'`
   `curl -X POST http://127.0.0.1:3000/strategy/momentum_v1/dry-run -H 'content-type: application/json' -d '{"symbol":"BTCUSDT","timeframe":"1m"}'`
   `curl 'http://127.0.0.1:3000/signals/recent?symbol=BTCUSDT&limit=50'`
   `curl 'http://127.0.0.1:3000/risk/decisions?symbol=BTCUSDT&limit=50'`
   `curl 'http://127.0.0.1:3000/risk/decisions/<risk_decision_id>'`
   `curl 'http://127.0.0.1:3000/orders'`
   `curl 'http://127.0.0.1:3000/orders/<order_id>'`
   `curl 'http://127.0.0.1:3000/paper/account'`
   `curl 'http://127.0.0.1:3000/paper/positions?status=OPEN&limit=50'`
   `curl -X POST http://127.0.0.1:3000/paper/positions/<position_id>/close -H 'content-type: application/json' -d '{"confirmation_text":"CLOSE BTCUSDT","reason":"manual_operator_exit","close_mode":"MARKET_SIMULATED"}'`
   `curl 'http://127.0.0.1:3000/paper/pnl/daily'`
   `curl 'http://127.0.0.1:3000/paper/equity?limit=50'`
   `curl 'http://127.0.0.1:3000/paper/trade-journal?limit=50'`
   `curl -X POST http://127.0.0.1:3000/paper/account/mark-to-market`
   `curl -X POST http://127.0.0.1:3000/reports/operator/daily -H 'content-type: application/json' -d '{"start_time":"2026-05-24T00:00:00Z","end_time":"2026-05-24T23:59:59Z","symbol":"BTCUSDT","strategy_id":"momentum_v1","format":"MARKDOWN","persist":false}'`
   `curl 'http://127.0.0.1:3000/events/recent?limit=100&event_type=risk.rejected&source=aegis-quant-api'`
   `curl 'http://127.0.0.1:3000/backtest/runs?limit=10'`
Required environment variables:

- `DATABASE_URL`
- `AEGIS_JWT_SECRET` when `AEGIS_AUTH_DISABLED=false`

Optional environment variables:

- `APP_NAME`
- `APP_ENV`
- `API_BIND_ADDR`
- `TEST_DATABASE_URL`
- `DATABASE_MAX_CONNECTIONS`
- `AEGIS_AUTH_DISABLED`
- `AEGIS_ACCESS_TOKEN`
- `AEGIS_ACCESS_TOKEN_TTL_SECONDS`
- `AEGIS_REFRESH_TOKEN_TTL_SECONDS`
- `AEGIS_COOKIE_SECURE`
- `AEGIS_PROTECT_METRICS`
- `AEGIS_BOOTSTRAP_OWNER_EMAIL`
- `AEGIS_BOOTSTRAP_OWNER_PASSWORD`
- `MARKET_EXCHANGE`
- `MARKET_SYMBOLS`
- `MARKET_STALE_THRESHOLD_SECONDS`
- `BINANCE_WS_BASE_URL`
- `BINANCE_TESTNET_REST_BASE_URL`
- `BINANCE_TESTNET_WS_BASE_URL`
- `BINANCE_TESTNET_API_KEY`
- `BINANCE_TESTNET_API_SECRET`
- `BINANCE_TESTNET_RECV_WINDOW_MS`
- `BINANCE_TESTNET_PRIVATE_STREAM_STALE_THRESHOLD_SECONDS`
- `BINANCE_TESTNET_PRIVATE_STREAM_KEEPALIVE_SECONDS`
- `BINANCE_TESTNET_PRIVATE_STREAM_RECONNECT_DELAY_SECONDS`
- `TESTNET_SHADOW_PROMOTION_TTL_SECONDS`
- `BINANCE_REST_BASE_URL`
- `STRATEGY_DEFAULT_SYMBOLS`
- `STRATEGY_DEFAULT_TIMEFRAME`
- `STRATEGY_DEFAULT_NOTIONAL`
- `MOMENTUM_LOOKBACK_CANDLES`
- `BREAKOUT_LOOKBACK_CANDLES`
- `RUST_LOG`
- `NEXT_PUBLIC_API_BASE_URL`
- `AEGIS_API_BASE_URL`

## CLI fallback

`crates/cli` provides a local/operator fallback when the dashboard is unavailable. The `aegis` binary talks only to the existing HTTP API and never mutates the database directly.

Base URL resolution:

- `AEGIS_API_BASE_URL`
- fallback: `http://127.0.0.1:3000`

Examples:

```bash
cargo run -p cli -- auth login --email owner@example.com --password 'replace-with-a-12-char-min-password'
cargo run -p cli -- auth refresh
cargo run -p cli -- auth me
cargo run -p cli -- status
cargo run -p cli -- kill --reason "manual operator halt"
cargo run -p cli -- resume --confirm "RESUME TRADING"
cargo run -p cli -- pipeline run --strategy momentum_v1 --symbol BTCUSDT --timeframe 1m
cargo run -p cli -- strategy list
cargo run -p cli -- strategy disable momentum_v1
cargo run -p cli -- strategy config get momentum_v1
cargo run -p cli -- strategy config validate momentum_v1 --symbol BTCUSDT --timeframe 1m --suggested-notional 100000 --lookback-candles 3 --max-signal-age-ms 5000 --cooldown-seconds 900
cargo run -p cli -- strategy dry-run momentum_v1 --symbol BTCUSDT --timeframe 1m
cargo run -p cli -- orders list --limit 20
cargo run -p cli -- orders get 00000000-0000-0000-0000-000000000000
cargo run -p cli -- paper account
cargo run -p cli -- paper positions --status OPEN --limit 50
cargo run -p cli -- paper close 00000000-0000-0000-0000-000000000000 --confirm "CLOSE BTCUSDT" --reason manual_operator_exit
cargo run -p cli -- paper pnl
cargo run -p cli -- paper equity --limit 50
cargo run -p cli -- paper journal --limit 50
cargo run -p cli -- paper mark
cargo run -p cli -- events list --limit 50 --event-type risk.rejected
cargo run -p cli -- risk decisions --limit 50 --symbol BTCUSDT
cargo run -p cli -- backtest run \
  --strategy momentum_v1 \
  --symbol BTCUSDT \
  --timeframe 1m \
  --start 2026-05-01T00:00:00Z \
  --end 2026-05-02T00:00:00Z \
  --initial-capital 1000000 \
  --fee-bps 10 \
  --slippage-bps 5 \
  --holding-candles 3
cargo run -p cli -- backtest list
cargo run -p cli -- analytics strategy performance \
  --strategy momentum_v1 \
  --symbol BTCUSDT \
  --timeframe 1m \
  --mode COMBINED
cargo run -p cli -- analytics strategy rankings --mode SHADOW --limit 20
cargo run -p cli -- analytics strategy decision-breakdown momentum_v1 \
  --symbol BTCUSDT \
  --timeframe 1m
cargo run -p cli -- analytics testnet promotion-funnel \
  --strategy momentum_v1 \
  --symbol BTCUSDT \
  --timeframe 1m
cargo run -p cli -- analytics testnet promotion-outcomes \
  --symbol BTCUSDT
cargo run -p cli -- analytics testnet promotion-rows --limit 50
cargo run -p cli -- reports operator daily \
  --start 2026-05-24T00:00:00Z \
  --end 2026-05-24T23:59:59Z \
  --symbol BTCUSDT \
  --strategy momentum_v1 \
  --format markdown
cargo run -p cli -- reports operator list --limit 20
cargo run -p cli -- reports operator get <report_id>
cargo run -p cli -- market backfill \
  --symbol BTCUSDT \
  --timeframe 1m \
  --start 2026-05-01T00:00:00Z \
  --end 2026-05-02T00:00:00Z
cargo run -p cli -- market backfills
cargo run -p cli -- metrics
cargo run -p cli -- metrics --grep paper
cargo run -p cli -- auth logout
```

Notes:

- Add `--json` before the command for raw API-shaped output.
- CLI auth state is loaded from `AEGIS_ACCESS_TOKEN` first, then `~/.config/aegis/token.json`.
- The token file stores the access token, refresh token, access expiry, and a small user summary so `aegis auth refresh` and one-shot `401` retries can work across CLI runs.
- When a stored refresh token exists, the CLI will automatically refresh once on `401` and retry the original request once.
- If refresh fails, the CLI returns a clear login-required error instead of printing tokens.
- If `AEGIS_ACCESS_TOKEN` is set, it overrides the token file for that run and the CLI will not rewrite `~/.config/aegis/token.json` unless `aegis auth login` is executed.
- `resume` refuses locally unless `--confirm "RESUME TRADING"` matches exactly.
- `orders list --limit` trims results client-side because `/orders` is currently unfiltered.
- The CLI does not print tokens by default and does not implement live trading, production exchange private APIs, API key reads, or any TUI layer.
- Testnet exchange commands talk only to the Aegis HTTP API; the CLI never reads Binance secrets directly.
- Strategy analytics is read-only: it compares persisted backtest, paper, and shadow behavior and never submits or mutates orders, positions, PnL, reconciliation rows, or exchange state.
- Promotion funnel analytics is read-only: it joins `testnet_shadow_runs`, `testnet_shadow_promotions`, `exchange_testnet_orders`, and isolated lifecycle history strictly for inspection.
- Operator reports are read-only: they aggregate persisted health, market, risk, paper, shadow, promotion, and isolated testnet tables into deterministic findings and recommendations. The only optional write is a persisted `operator_reports` export row.

## Binance Spot Testnet adapter

This repository now includes a testnet-only exchange adapter skeleton for future controlled execution.

Guardrails:

- Only Binance Spot Testnet is supported
- `ExchangeEnvironment::Live` is hard-rejected everywhere
- Production Binance endpoints are not configured or used
- Testnet submission is isolated from paper accounting and paper orders
- Testnet pipeline preview is operator-visible only and never submits an exchange order or persists `exchange_testnet_orders`
- Testnet shadow mode is operator-triggered only, persists isolated `testnet_shadow_runs`, and records would-submit intents without submitting or creating `exchange_testnet_orders`
- Testnet shadow promotion preview is operator-visible only, persists isolated `testnet_shadow_promotions`, requires a persisted `WOULD_SUBMIT` shadow run plus fresh local pricing, and never auto-submits or creates lifecycle rows
- Testnet shadow promotion submit is owner-only, requires exact typed confirmation `PROMOTE TESTNET <SYMBOL>`, submits only the persisted would-submit payload from the selected promotion, and mutates only isolated testnet execution state
- Testnet shadow runner is a persistent no-submit scheduler over the same shadow path, persists only runner config/state plus `testnet_shadow_runs`, and never creates exchange order or lifecycle rows
- Testnet pipeline submit is owner-only, requires an existing approved `risk_decision_id`, and requires exact typed confirmation `SUBMIT TESTNET <SYMBOL>`
- Submit/cancel require `OWNER`, typed confirmation `TESTNET ORDER`, an inactive kill switch, and a preapproved `risk_decision_id`
- Reconciliation is testnet-only, persists runs plus mismatches, and never mutates paper/backtest/live tables
- Unknown exchange state or missing exchange orders are surfaced as explicit mismatches and alerts, not treated as success
- Testnet execution now has an isolated internal lifecycle in `exchange_testnet_order_lifecycle_events` and `exchange_testnet_orders.execution_state`
- Exchange ACK is recorded as `EXCHANGE_ACKED` only; fills must come from private stream or reconciliation evidence
- Testnet repair controls are explicit operator actions only; there is no automatic repair and no production Binance endpoint usage

Required env when using the adapter:

- `BINANCE_TESTNET_REST_BASE_URL=https://testnet.binance.vision`
- `BINANCE_TESTNET_WS_BASE_URL=wss://stream.testnet.binance.vision/ws`
- `BINANCE_TESTNET_API_KEY`
- `BINANCE_TESTNET_API_SECRET`

Operator examples:

```bash
cargo run -p cli -- exchange testnet status
cargo run -p cli -- exchange testnet symbols
cargo run -p cli -- exchange testnet balances
cargo run -p cli -- exchange testnet pipeline-preview \
  --risk-decision-id 00000000-0000-0000-0000-000000000000
cargo run -p cli -- exchange testnet pipeline-submit \
  --risk-decision-id 00000000-0000-0000-0000-000000000000 \
  --confirm "SUBMIT TESTNET BTCUSDT"
cargo run -p cli -- exchange testnet shadow-run \
  --strategy momentum_v1 \
  --symbol BTCUSDT \
  --timeframe 1m
cargo run -p cli -- exchange testnet shadow-runs --limit 50
cargo run -p cli -- exchange testnet shadow-get <run_id>
cargo run -p cli -- exchange testnet shadow-promotion-preview <shadow_run_id>
cargo run -p cli -- exchange testnet shadow-promotions --limit 50
cargo run -p cli -- exchange testnet shadow-promotion-get <promotion_id>
cargo run -p cli -- exchange testnet shadow-promotion-submit <promotion_id> \
  --confirm "PROMOTE TESTNET BTCUSDT"
cargo run -p cli -- exchange testnet shadow-runner status
cargo run -p cli -- exchange testnet shadow-runner config
cargo run -p cli -- exchange testnet shadow-runner config-update \
  --enabled true \
  --interval-seconds 60 \
  --strategies momentum_v1,volatility_breakout_v1 \
  --symbols BTCUSDT,ETHUSDT \
  --timeframe 1m \
  --max-runs-per-tick 4 \
  --stale-feed-policy SKIP
cargo run -p cli -- exchange testnet shadow-runner run-once
cargo run -p cli -- exchange testnet shadow-runner pause
cargo run -p cli -- exchange testnet shadow-runner resume
cargo run -p cli -- exchange testnet shadow-runner start
cargo run -p cli -- exchange testnet shadow-runner stop
cargo run -p cli -- exchange testnet order-submit \
  --symbol BTCUSDT \
  --side BUY \
  --type MARKET \
  --quote-notional 10 \
  --risk-decision-id 00000000-0000-0000-0000-000000000000 \
  --confirm "TESTNET ORDER"
cargo run -p cli -- exchange testnet order-get aegis-testnet-<correlationid>
cargo run -p cli -- exchange testnet order-lifecycle aegis-testnet-<correlationid>
cargo run -p cli -- exchange testnet order-cancel aegis-testnet-<correlationid> --confirm "TESTNET ORDER"
cargo run -p cli -- exchange testnet order-repair aegis-testnet-<correlationid> \
  --action MANUAL_RECHECK \
  --confirm "REPAIR TESTNET aegis-testnet-<correlationid>" \
  --reason "operator_requested_recheck"
cargo run -p cli -- exchange testnet order-repair aegis-testnet-<correlationid> \
  --action SAFE_CANCEL_REQUEST \
  --confirm "CANCEL TESTNET aegis-testnet-<correlationid>"
cargo run -p cli -- exchange testnet order-repairs aegis-testnet-<correlationid>
cargo run -p cli -- exchange testnet reconcile --limit 50
cargo run -p cli -- exchange testnet reconciliation-runs
cargo run -p cli -- exchange testnet reconciliation-get <run_id>
cargo run -p cli -- exchange testnet reconciliation-mismatches <run_id>
cargo run -p cli -- exchange testnet private-stream status
cargo run -p cli -- exchange testnet private-stream events --limit 50
cargo run -p cli -- exchange testnet private-stream listen-key
cargo run -p cli -- exchange testnet private-stream keepalive --listen-key <testnet-listen-key>
cargo run -p cli -- exchange testnet private-stream close --listen-key <testnet-listen-key>
rtk cargo run -p exchange --bin testnet-private-stream
```

API examples:

```bash
curl -X POST http://127.0.0.1:3000/exchange/testnet/pipeline/preview \
  -H 'content-type: application/json' \
  -H "authorization: Bearer $AEGIS_ACCESS_TOKEN" \
  -d '{"risk_decision_id":"00000000-0000-0000-0000-000000000000"}'

curl -X POST http://127.0.0.1:3000/exchange/testnet/pipeline/submit \
  -H 'content-type: application/json' \
  -H "authorization: Bearer $AEGIS_ACCESS_TOKEN" \
  -d '{"risk_decision_id":"00000000-0000-0000-0000-000000000000","confirmation_text":"SUBMIT TESTNET BTCUSDT"}'

curl -X POST http://127.0.0.1:3000/exchange/testnet/shadow/run \
  -H 'content-type: application/json' \
  -H "authorization: Bearer $AEGIS_ACCESS_TOKEN" \
  -d '{"strategy_id":"momentum_v1","symbol":"BTCUSDT","timeframe":"1m"}'
```

Private stream notes:

- The worker is testnet-only and uses only `wss://stream.testnet.binance.vision/ws`.
- Raw private events are persisted in `exchange_private_stream_events`; stream lifecycle state is persisted in `exchange_private_stream_state`.
- Normalized `executionReport` events append deterministic lifecycle transitions and update only isolated `exchange_testnet_orders` when `client_order_id` matches.
- `GET /exchange/testnet/orders/:client_order_id/lifecycle` returns the ordered transition history for operator inspection.
- `POST /exchange/testnet/orders/:client_order_id/repair` requires typed per-order confirmation and records repair history in `exchange_testnet_repair_actions`.
- Listen keys are hashed in Postgres; the API, CLI, logs, and dashboard use masked values only.
- The dashboard Settings page now exposes a private-stream status card, recent private events, and operator lifecycle controls.
- The dashboard Settings page now also exposes a testnet shadow-run form, recent shadow runs, and run-detail payload inspection.
- The dashboard Settings page also exposes shadow promotion preview/list/detail flows with owner-only typed confirmation before any testnet submit.
- The dashboard Settings page also exposes persistent testnet shadow-runner status, config, and start/pause/resume/run-once controls with role gating.

To run the scheduler daemon directly:

```bash
cargo run -p api --bin testnet-shadow-runner
```
- Preview and submit both require an existing approved `risk_decision_id`, block on an active kill switch, and require fresh local market pricing from the stored tick/candle path.

## Auth MVP

- Public endpoints: `GET /system/health`, `POST /auth/login`, `POST /auth/bootstrap-owner`, `POST /auth/refresh`
- All other `GET` endpoints require an authenticated `VIEWER` or above
- Mutating paper/backfill/backtest endpoints require `OPERATOR` or `OWNER`
- `POST /risk/resume`, `POST /risk/config/update`, and `POST /strategy/:id/config/update` require `OWNER`
- `POST /exchange/testnet/pipeline/preview` requires `OPERATOR` or `OWNER`; `POST /exchange/testnet/pipeline/submit` requires `OWNER`
- `POST /exchange/testnet/shadow/run` requires `OPERATOR` or `OWNER`; `GET /exchange/testnet/shadow/runs` and `GET /exchange/testnet/shadow/runs/:id` require authenticated inspection access
- Dashboard access requires login unless `AEGIS_AUTH_DISABLED=true`
- `AEGIS_AUTH_DISABLED=true` injects a synthetic local `OWNER` actor for development only and logs a startup warning

## Telemetry

Aegis exposes Prometheus-compatible telemetry at `GET /metrics`.

Examples:

```bash
curl http://127.0.0.1:3000/metrics
cargo run -p cli -- metrics
cargo run -p cli -- metrics --grep risk
docker compose -f infra/docker-compose.yml --profile prometheus up -d prometheus
```

Current metric coverage includes:

- API request counters and latency histograms
- System and database health gauges
- Market tick, candle close, feed freshness, and backfill counters
- Strategy evaluation and signal counters
- Risk decision and rejection counters
- Kill switch, paper pipeline, paper order/fill/close, paper position, and paper PnL gauges
- Backtest run, duration, and trade counters

Notes:

- `/metrics` stays public by default for local development unless `AEGIS_PROTECT_METRICS=true`.
- Scrape-time gauges read current operational state from Postgres and do not mutate state.
- Metrics never expose API keys or private exchange credentials because those are not part of this MVP.

## Historical candle backfill

Historical closed candles can be loaded from Binance public REST without API keys. Backfill is idempotent: it upserts by `(exchange, symbol, interval, open_time)` and records each run in `candle_backfill_runs`.

API example:

```bash
curl -X POST http://127.0.0.1:3000/market/backfill/candles \
  -H 'content-type: application/json' \
  -d '{
    "exchange":"binance",
    "symbol":"BTCUSDT",
    "interval":"1m",
    "start_time":"2026-05-01T00:00:00Z",
    "end_time":"2026-05-02T00:00:00Z",
    "limit_per_request":1000
  }'
```

CLI example:

```bash
cargo run -p cli -- market backfill \
  --symbol BTCUSDT \
  --timeframe 1m \
  --start 2026-05-01T00:00:00Z \
  --end 2026-05-02T00:00:00Z
```

After backfill, replay/backtest uses the same stored closed candles:

```bash
cargo run -p cli -- backtest run \
  --strategy momentum_v1 \
  --symbol BTCUSDT \
  --timeframe 1m \
  --start 2026-05-01T00:00:00Z \
  --end 2026-05-02T00:00:00Z \
  --initial-capital 1000000 \
  --fee-bps 10 \
  --slippage-bps 5
```

## Paper accounting

Operational paper trading now persists a separate accounting surface:

- `paper_accounts`
- `paper_positions`
- `paper_fills`
- `paper_equity_snapshots`
- `paper_trade_journal`

The flow is deterministic and paper-only:

```txt
paper order filled
-> paper fill
-> paper position open/update
-> realized/unrealized PnL
-> account equity update
-> equity snapshot
-> trade journal
```

Examples:

```bash
curl http://127.0.0.1:3000/paper/account
curl http://127.0.0.1:3000/paper/positions?status=ALL&limit=50
curl -X POST http://127.0.0.1:3000/paper/account/mark-to-market
curl -X POST http://127.0.0.1:3000/paper/positions/<position_id>/close \
  -H 'content-type: application/json' \
  -d '{"confirmation_text":"CLOSE BTCUSDT","reason":"manual_operator_exit","close_mode":"MARKET_SIMULATED"}'

cargo run -p cli -- paper account
cargo run -p cli -- paper positions --status OPEN --limit 50
cargo run -p cli -- paper pnl
cargo run -p cli -- paper mark
```

Paper close rules:

- Manual close is simulated only and never calls a private exchange API.
- Operators must type `CLOSE <SYMBOL>` exactly, for example `CLOSE BTCUSDT`.
- Close uses the latest stored public market tick as the mark price and rejects missing/stale price data by default.
- A successful close writes a closing fill, closes the position, updates realized PnL/equity, snapshots equity, writes the trade journal, and emits audit/system events transactionally.

## Dashboard shell

`apps/dashboard` is a minimal operational cockpit, not a marketing site. It exposes paper-only controls and inspection views for:

- Command Center
- Market Data
- Strategies
- Risk
- Orders
- Backtests
- Logs / Events

Safety constraints:

- No live trading controls
- No exchange private API usage
- No API key entry
- CLI fallback shares the same paper-only safety boundary through the HTTP API
- Resume requires typed confirmation exactly equal to `RESUME TRADING`
- Kill switch activation and resume both flow through the existing backend endpoints

Operational inspection additions:

- Risk screen reads persisted risk decisions through `/risk/decisions` and `/risk/decisions/:id`
- Orders expose true `signal_id` linkage through the persisted `risk_decision_id`, not dashboard-side correlation guesses
- Events screen reads server-filtered `/events/recent` rows by `event_type`, `source`, and `correlation_id`
- Command Center surfaces the latest persisted risk rejection without adding any live-trading controls

Local frontend env:

```bash
cd apps/dashboard
printf 'NEXT_PUBLIC_API_BASE_URL=http://127.0.0.1:3000\n' > .env.local
```

Optional Docker Compose profile:

```bash
docker compose -f infra/docker-compose.yml --profile dashboard up dashboard
```

## Running DB integration tests

DB-backed persistence tests are ignored by default and only run when you explicitly opt in with a Postgres test database.

Safety rules:

- `TEST_DATABASE_URL` is preferred over `DATABASE_URL`
- the target database name must contain `test`
- if you intentionally need a different name, set `ALLOW_NON_TEST_DB=1`
- the harness runs migrations, truncates known tables, and seeds baseline `system_state`

Example:

```bash
TEST_DATABASE_URL=postgres://aegis:aegis@127.0.0.1:5432/aegis_quant_test \
  cargo test -p db --test integration_db -- --ignored
```

Run the end-to-end pipeline persistence tests:

```bash
TEST_DATABASE_URL=postgres://aegis:aegis@127.0.0.1:5432/aegis_quant_test \
  cargo test -p api --test pipeline_persistence -- --ignored
```

Or run both through the helper script:

```bash
TEST_DATABASE_URL=postgres://aegis:aegis@127.0.0.1:5432/aegis_quant_test \
  ./scripts/test-integration.sh
```

## Market ingest local flow

`market-ingest` connects only to Binance public trade streams for configured symbols and can also backfill historical candles from Binance public REST. WebSocket trades are persisted to `market_ticks`, fed through a deterministic 1m candle builder, upserted into `candles`, and reflected in `market_feed_status`. REST backfill upserts only closed candles, records run metadata in `candle_backfill_runs`, and emits `market.backfill.started`, `market.backfill.page_fetched`, `market.backfill.completed`, and `market.backfill.failed`.

## Strategy evaluation example

Signals are generated from stored closed candles only. This step does not place orders and does not bypass later risk evaluation.

```bash
curl -X POST http://127.0.0.1:3000/strategy/momentum_v1/evaluate \
  -H 'content-type: application/json' \
  -d '{"symbol":"BTCUSDT"}'
```

```json
{
  "strategy_id": "momentum_v1",
  "symbol": "BTCUSDT",
  "generated": true,
  "signal_id": "00000000-0000-0000-0000-000000000000",
  "side": "BUY",
  "confidence": "0.65",
  "reason": "three_consecutive_higher_closes",
  "source_candle_open_time": "2026-01-01T00:03:00Z",
  "correlation_id": "00000000-0000-0000-0000-000000000000"
}
```

## Paper pipeline example

`/paper/pipeline/run` executes the deterministic paper-only path:

```txt
closed candles -> strategy evaluation -> signal -> risk decision -> paper order intent -> paper order lifecycle
```

It never uses live trading, private exchange APIs, API keys, or any bypass around risk.

```bash
curl -X POST http://127.0.0.1:3000/paper/pipeline/run \
  -H 'content-type: application/json' \
  -d '{"strategy_id":"momentum_v1","symbol":"BTCUSDT","timeframe":"1m"}'
```

Example no-signal response:

```json
{
  "pipeline_decision": "NO_SIGNAL",
  "strategy_id": "momentum_v1",
  "symbol": "BTCUSDT",
  "timeframe": "1m",
  "signal_generated": false,
  "signal_reused": false,
  "signal_id": null,
  "risk_decision_id": null,
  "paper_order_id": null,
  "execution_state": null,
  "reasons": ["conditions_not_met"],
  "correlation_id": "00000000-0000-0000-0000-000000000000",
  "trace": {
    "strategy_evaluation": "completed",
    "signal": "skipped",
    "risk_evaluation": "skipped",
    "paper_order": "skipped",
    "order_intent_source": null
  }
}
```

## Backtest example

`/backtest/run` replays stored closed candles only. It does not connect to Binance, does not call private exchange APIs, and does not mutate production `signals`, `risk_decisions`, or `orders`.

```bash
curl -X POST http://127.0.0.1:3000/backtest/run \
  -H 'content-type: application/json' \
  -d '{
    "strategy_id":"momentum_v1",
    "symbol":"BTCUSDT",
    "timeframe":"1m",
    "start_time":"2026-05-01T00:00:00Z",
    "end_time":"2026-05-02T00:00:00Z",
    "initial_capital":"1000000",
    "fee_bps":"10",
    "slippage_bps":"5",
    "holding_candles":3
  }'
```

```json
{
  "run_id": "00000000-0000-0000-0000-000000000000",
  "status": "COMPLETED",
  "strategy_id": "momentum_v1",
  "symbol": "BTCUSDT",
  "trade_count": 12,
  "pnl": "15200",
  "pnl_pct": "1.52",
  "max_drawdown_pct": "0.84",
  "win_rate": "58.33",
  "fee_paid": "12000",
  "slippage_cost": "6000",
  "correlation_id": "00000000-0000-0000-0000-000000000000"
}
```

Inspect persisted results:

```bash
curl 'http://127.0.0.1:3000/backtest/runs?limit=10'
curl 'http://127.0.0.1:3000/backtest/runs/<run_id>'
curl 'http://127.0.0.1:3000/backtest/runs/<run_id>/trades'
curl 'http://127.0.0.1:3000/backtest/runs/<run_id>/equity'
```

## Workspace layout

```txt
crates/
  api/
  core/
  db/
  events/
  exchange/
  execution-engine/
  llm-analyst/
  market-ingest/
  replay-engine/
  risk-engine/
  strategy-engine/
docs/
infra/
```
