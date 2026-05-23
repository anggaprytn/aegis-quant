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
- `crates/market-ingest`: market data ingestion boundary
- `crates/strategy-engine`: deterministic signal generation boundary
- `crates/risk-engine`: risk gating boundary
- `crates/execution-engine`: paper execution lifecycle boundary
- `crates/exchange`: exchange adapter boundary, disabled for live execution in MVP
- `crates/llm-analyst`: advisory-only market commentary boundary

## Data boundaries

- Money, price, balances, and PnL use `rust_decimal`
- Correlation IDs are required on events
- Auditable state changes should land in `system_events` or `audit_logs`
- Kill switch persistence is required later and should live in the database

## Deployment shape

For MVP local development:

- One Axum API process
- One PostgreSQL instance
- Docker Compose orchestration

No Kubernetes, no microservice decomposition, and no paid infrastructure assumptions are introduced in this foundation.
