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
- `crates/strategy-engine`: deterministic signal generation boundary
- `crates/risk-engine`: risk gating boundary
- `crates/execution-engine`: paper execution lifecycle boundary
- `crates/exchange`: exchange adapter boundary, disabled for live execution in MVP
- `crates/llm-analyst`: advisory-only market commentary boundary

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

Notes:

- Supported symbols are env-configured and uppercase in persistence/API responses.
- Candle building is deterministic for identical trade ordering.
- Out-of-order trades are rejected explicitly rather than rewriting historical candles.
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

## Deployment shape

For MVP local development:

- One Axum API process
- One market-ingest process
- One PostgreSQL instance
- Docker Compose orchestration

No Kubernetes, no microservice decomposition, and no paid infrastructure assumptions are introduced in this foundation.
