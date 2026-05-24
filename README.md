# Aegis Quant

Aegis Quant is a Rust-first autonomous quant execution infrastructure focused on deterministic signal flow, risk-gated paper execution, event logging, and operational auditability.

## Scope of this scaffold

This repository foundation includes:

- Rust workspace with bounded service and engine crates
- Shared core domain types using `rust_decimal`
- Minimal Axum API for health and system status
- Binance public WebSocket market ingest with deterministic 1m candle building
- Deterministic candle-only strategy evaluation for `momentum_v1` and `volatility_breakout_v1`
- Deterministic paper trading pipeline from closed candles to risk-gated paper order lifecycle
- Deterministic replay/backtest engine from stored candles and persisted strategy configs
- Minimal Next.js operational dashboard shell for paper-only inspection and control
- Event model and publisher trait skeleton
- Postgres migration baseline
- Local development Docker Compose setup
- Architecture, roadmap, and security documentation

## Non-goals in this scaffold

- Live trading
- Real exchange order execution
- Exchange secrets
- Automatic strategy scheduling

## Quick start

1. Copy `.env.example` to `.env`.
2. Start Postgres with `docker compose -f infra/docker-compose.yml up -d postgres`.
3. Load the local environment with `set -a; source .env; set +a`.
4. Run `cargo fmt` and `cargo check`.
5. Start the API with `cargo run -p api`.
6. Start market ingest with `cargo run -p market-ingest`.
7. Verify JSON endpoints:
   `curl http://127.0.0.1:3000/system/health`
   `curl http://127.0.0.1:3000/system/status`
   `curl http://127.0.0.1:3000/system/db-health`
   `curl 'http://127.0.0.1:3000/market/symbols'`
   `curl 'http://127.0.0.1:3000/market/ticks/latest?symbol=BTCUSDT'`
   `curl 'http://127.0.0.1:3000/market/candles?symbol=BTCUSDT&interval=1m&limit=100'`
   `curl 'http://127.0.0.1:3000/market/feed-status'`
   `curl 'http://127.0.0.1:3000/strategy/list'`
   `curl 'http://127.0.0.1:3000/signals/recent?symbol=BTCUSDT&limit=50'`
   `curl 'http://127.0.0.1:3000/risk/decisions?symbol=BTCUSDT&limit=50'`
   `curl 'http://127.0.0.1:3000/risk/decisions/<risk_decision_id>'`
   `curl 'http://127.0.0.1:3000/orders'`
   `curl 'http://127.0.0.1:3000/orders/<order_id>'`
   `curl 'http://127.0.0.1:3000/events/recent?limit=100&event_type=risk.rejected&source=aegis-quant-api'`
   `curl 'http://127.0.0.1:3000/backtest/runs?limit=10'`
8. Start the dashboard:
   `cd apps/dashboard`
   `npm install`
   `npm run dev`
9. Open `http://127.0.0.1:3001`.

Required environment variables:

- `DATABASE_URL`

Optional environment variables:

- `APP_NAME`
- `APP_ENV`
- `API_BIND_ADDR`
- `TEST_DATABASE_URL`
- `DATABASE_MAX_CONNECTIONS`
- `MARKET_EXCHANGE`
- `MARKET_SYMBOLS`
- `MARKET_STALE_THRESHOLD_SECONDS`
- `BINANCE_WS_BASE_URL`
- `STRATEGY_DEFAULT_SYMBOLS`
- `STRATEGY_DEFAULT_TIMEFRAME`
- `STRATEGY_DEFAULT_NOTIONAL`
- `MOMENTUM_LOOKBACK_CANDLES`
- `BREAKOUT_LOOKBACK_CANDLES`
- `RUST_LOG`
- `NEXT_PUBLIC_API_BASE_URL`

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

`market-ingest` connects only to Binance public trade streams for configured symbols. Each trade is persisted to `market_ticks`, fed through a deterministic 1m candle builder, upserted into `candles`, and reflected in `market_feed_status`. The service emits `market.feed.connected`, `market.feed.disconnected`, `market.feed.stale`, `market.trade.received`, and `market.candle.closed` into `system_events`.

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
