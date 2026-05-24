# Security

## Current posture

This repository scaffold intentionally avoids live trading and real exchange credentials.

## Rules

- Do not commit real API keys or secrets
- Use `.env.example` only for safe placeholders
- Keep LLM components advisory only
- Require auditable state transitions for dangerous actions
- Keep risk controls and kill switch persistent when implemented
- Replay/backtest must stay isolated from live and paper execution tables

## TODO boundaries

- Authentication and authorization are not implemented yet
- Secret management is not implemented yet
- Database role hardening is not implemented yet
- Request signing and exchange credential handling are intentionally deferred until after paper trading is stable
- Replay/backtest reads stored candles only and must not mutate production `signals`, `risk_decisions`, or `orders`
