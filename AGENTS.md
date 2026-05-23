# Aegis Quant Agent Instructions

You are working on Aegis Quant, a Rust-first autonomous quant execution infrastructure.

Read `docs/PRD.md` before making architectural or implementation decisions.

## Core Principle

This is not an AI trading bot.

This is deterministic execution infrastructure focused on:
- market data ingestion
- event logging
- risk-gated execution
- paper trading
- order state lifecycle
- reconciliation
- auditability
- operational observability

LLM components are advisory only and must never have execution authority.

## Hard Rules

- Do not implement live trading first.
- Do not add real exchange secret handling beyond safe `.env.example`.
- Do not use `f64` for money, balances, notional, prices, or PnL. Use `rust_decimal`.
- Do not let strategy logic submit orders directly.
- All trade-like actions must flow through:
  market event -> signal -> risk decision -> order intent -> execution state.
- All dangerous actions must be auditable.
- Kill switch state must be persistent, not memory-only.
- Backend correctness matters more than frontend beauty.
- Prefer boring, explicit code over clever abstractions.
- Do not introduce Kubernetes, microservice overkill, or paid data APIs for MVP.
- Do not add unofficial exchange SDKs unless explicitly justified.

## MVP Priority

Build in this order:
1. Rust workspace
2. shared core types
3. Postgres migrations
4. event log
5. health API
6. market ingest skeleton
7. candle storage
8. strategy signal skeleton
9. paper trading skeleton
10. risk engine skeleton
11. dashboard shell
12. kill switch

## Stack

Backend:
- Rust
- Tokio
- Axum
- SQLx
- PostgreSQL
- tracing
- rust_decimal
- uuid
- chrono
- thiserror / anyhow

Frontend:
- Next.js
- TypeScript
- Tailwind
- shadcn/ui
- TanStack Query
- lightweight-charts later

Infra:
- Docker Compose
- Postgres
- Caddy or Nginx later
- Prometheus optional later

## Repo Expectations

Use a monorepo:

```txt
crates/
apps/dashboard/
infra/
docs/
