# Architecture

## Intent

Aegis Quant is deterministic execution infrastructure, not an AI trading bot. The control flow is explicit:

```txt
market event -> signal -> risk decision -> order intent -> execution state
```

LLM components are advisory only and do not have execution authority.

## Initial components

- `crates/core`: shared domain types and event envelope
- `crates/api`: operational health/status API
- `crates/events`: event taxonomy and publisher contract
- `crates/db`: database configuration and migrations
- `crates/market-ingest`: Binance public market data ingestion and deterministic candle boundary
- `crates/replay-engine`: deterministic historical candle replay and backtest simulation boundary
- `crates/strategy-engine`: deterministic signal generation boundary
- `crates/risk-engine`: risk gating boundary
- `crates/execution-engine`: paper execution lifecycle boundary
- `crates/exchange`: exchange adapter boundary, disabled for live execution in MVP
- `crates/llm-analyst`: advisory-only market commentary boundary
- `apps/dashboard`: Next.js operational cockpit for paper-only inspection and operator actions

## Data boundaries

- Money, price, balances, and PnL use `rust_decimal`
- Correlation IDs are required on events
- Auditable state changes should land in `system_events` or `audit_logs`
- Kill switch persistence is required and lives in the database

## Market ingest flow

Phase 1 market data follows this path:

```txt
Binance public trade stream
-> parse trade payload into MarketTrade
-> persist tick into market_ticks
-> update market_feed_status
-> feed deterministic 1m CandleBuilder
-> upsert active/closed candles into candles
-> emit system_events for feed transitions, trades, and candle close
```

Historical candle hydration follows this parallel path:

```txt
Binance public REST klines
-> deterministic page planning over start_time/end_time
-> parse only final closed klines
-> idempotent candle upsert into candles
-> persist run metadata in candle_backfill_runs
-> emit market.backfill.* system events
```

Notes:

- Supported symbols are env-configured and uppercase in persistence/API responses.
- Candle building is deterministic for identical trade ordering.
- Out-of-order trades are rejected explicitly rather than rewriting historical candles.
- Replay/backtest reads the same `candles` table populated by both live WebSocket accumulation and historical REST backfill.
- The ingest boundary is public market data only. No API keys, private streams, or exchange execution are introduced here.

## Strategy evaluation flow

Current deterministic paper flow follows this path:

```txt
closed candles from Postgres
-> deterministic strategy evaluation
-> persisted signal or deduped existing signal
-> persisted risk_decision
-> order intent with deterministic idempotency key
-> paper order lifecycle
```

Notes:

- Strategy evaluation reads stored candles only and ignores open candles.
- `momentum_v1` and `volatility_breakout_v1` are deterministic library strategies with explicit config.
- Duplicate signals for the same strategy, symbol, timeframe, side, reason, and closed candle are deduped in Postgres.
- Every signal passed into the pipeline reaches an explicit `APPROVED` or `REJECTED` risk decision in `risk_decisions`.
- Risk rejection is machine-readable and emits `risk.approved` or `risk.rejected` system events.
- Strategy logic cannot submit orders directly. Paper orders are created only through the persisted approved `risk_decision_id`.
- Order idempotency is deterministic from `strategy_id + signal_id + risk_decision_id + symbol + side + source_candle_open_time`.
- Duplicate pipeline runs reuse the existing paper order instead of creating a second active order for the same idempotency key.
- If the strategy is disabled, the market feed is stale/degraded, the kill switch is active, or the signal is stale, the pipeline stops safely without creating a paper order.

## Replay and backtest flow

Replay/backtest follows this isolated path:

```txt
stored closed candles
-> deterministic strategy evaluation
-> simulated entry/exit decisions
-> simulated trades
-> equity curve
-> persisted backtest metrics
```

Notes:

- Replay reads only stored closed candles from Postgres for the requested symbol, timeframe, and time range.
- Strategy evaluation sees only candles available up to the replay point; no lookahead into future candles is allowed.
- Entries execute at the next candle open with fixed deterministic slippage and fee assumptions.
- Exits use deterministic TP/SL threshold checks or a fixed holding-candle fallback.
- Replay emits `replay.backtest.started`, `replay.backtest.completed`, and `replay.backtest.failed` into `system_events`.
- Replay persists only into `backtest_runs`, `backtest_trades`, and `backtest_equity_curve`.
- Replay must not mutate production `signals`, `risk_decisions`, or `orders`.

## Deployment shape

For MVP local development:

- One Axum API process
- One market-ingest process
- One Next.js dashboard process
- One PostgreSQL instance
- Docker Compose orchestration

No Kubernetes, no microservice decomposition, and no paid infrastructure assumptions are introduced in this foundation.

## Frontend cockpit overview

The dashboard is intentionally dense and operational:

- Sidebar sections: Command Center, Market Data, Strategies, Risk, Orders, Backtests, Logs / Events, Settings placeholder
- Sticky header: mode, kill switch state, feed state, data age, daily PnL placeholder, API health
- Paper-only controls: kill switch activation, typed resume confirmation, strategy evaluation, paper pipeline run, and backtest run
- Read-only cockpit inspection: persisted risk decisions, enriched paper order detail, and filtered recent system events

Frontend constraints:

- No live trading controls
- No exchange private API or secret handling
- No chart-heavy UX in MVP
- Defensive rendering around backend errors and optional data shapes

## Cockpit observability flow

The operational cockpit should expose persisted truth from the backend rather than reconstructing links in the browser:

```txt
signals
-> risk_decisions
-> orders
-> system_events
-> dashboard read APIs
```

Read API boundaries:

- `/risk/decisions` and `/risk/decisions/:id` expose persisted risk approvals and rejections, including notional, score, reasons, correlation, and linked signal metadata when available
- `/orders` and `/orders/:id` expose paper order inspection data enriched from the linked `risk_decision_id`, including truthful `signal_id` and `strategy_id`
- `/events/recent` exposes newest-first system events with optional server-side filters for `event_type`, `source`, and `correlation_id`

Operational intent:

- The dashboard should show what the database recorded, not best-effort guesses from correlation IDs alone
- Risk rejection review is read-only and does not mutate risk state
- Event inspection remains append-only and filterable for operator triage

## CLI fallback

`crates/cli` adds an operator-local fallback path for the same paper-only operational surface area when the Next.js dashboard is unavailable.

Design constraints:

- The binary name is `aegis`
- It uses the existing HTTP API only and does not connect to Postgres directly
- It reads `AEGIS_API_BASE_URL` with fallback to `http://127.0.0.1:3000`
- It does not add live trading, exchange private API usage, API key handling, auth bypasses, or a TUI layer
- Dangerous actions remain bounded by the same backend rules as the dashboard

Supported control and inspection flow:

- `aegis status` aggregates `/system/health`, `/system/status`, `/risk/status`, and `/market/feed-status`
- `aegis kill` and `aegis resume --confirm "RESUME TRADING"` call the existing risk endpoints and preserve typed confirmation
- `aegis pipeline run`, `strategy list|enable|disable`, `orders list|get`, `events list`, `risk decisions`, and `backtest run|list|get` map directly onto the existing read and paper-only control APIs

Operational intent:

- Dashboard and CLI are parallel operator surfaces over the same API truth
- The CLI is a fallback for local inspection and safe paper-only intervention, not a separate execution path
- Output stays compact by default, with optional `--json` when operators need exact payloads
