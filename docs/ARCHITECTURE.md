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

## Deployment shape

For MVP local development:

- One Axum API process
- One market-ingest process
- One PostgreSQL instance
- Docker Compose orchestration

No Kubernetes, no microservice decomposition, and no paid infrastructure assumptions are introduced in this foundation.
