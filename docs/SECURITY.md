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
- Historical backfill uses Binance public REST market data only and does not use API keys or private exchange endpoints
- Paper accounting is simulated only and never submits exchange orders
- `/metrics` exposes operational state only, not secrets, but it should still be restricted at the network boundary in production

## TODO boundaries

- Authentication and authorization are not implemented yet
- Secret management is not implemented yet
- Database role hardening is not implemented yet
- Request signing and exchange credential handling are intentionally deferred until after paper trading is stable
- Replay/backtest reads stored candles only and must not mutate production `signals`, `risk_decisions`, or `orders`
- Paper accounting reads only stored paper orders and stored public market data; it does not call exchange private endpoints or handle API keys
- `/metrics` is intentionally unauthenticated for local/internal MVP use; production deployment should place it behind private networking, reverse-proxy policy, or equivalent access controls
