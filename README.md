# Aegis Quant

Aegis Quant is a Rust-first autonomous quant execution infrastructure focused on deterministic signal flow, risk-gated paper execution, event logging, and operational auditability.

## Scope of this scaffold

This repository foundation includes:

- Rust workspace with bounded service and engine crates
- Shared core domain types using `rust_decimal`
- Minimal Axum API for health and system status
- Event model and publisher trait skeleton
- Postgres migration baseline
- Local development Docker Compose setup
- Architecture, roadmap, and security documentation

## Non-goals in this scaffold

- Live trading
- Real exchange order execution
- Exchange secrets
- Frontend/dashboard implementation
- Advanced strategy logic

## Quick start

1. Copy `.env.example` to `.env` if needed for local overrides.
2. Run `cargo check`.
3. Start local infrastructure with `docker compose -f infra/docker-compose.yml up --build`.

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
  risk-engine/
  strategy-engine/
docs/
infra/
```
