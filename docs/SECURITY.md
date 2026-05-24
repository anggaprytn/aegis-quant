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
- Passwords are hashed with Argon2id; plaintext passwords are never stored or returned
- Access tokens are short-lived JWTs and refresh tokens are stored only as hashes in Postgres
- Dashboard login stores only the access token client-side; refresh tokens are sent as HTTP-only cookies
- Auth-disabled mode is local-development only and injects a synthetic OWNER actor with a startup warning
- Paper position close is simulated only, requires typed confirmation `CLOSE <SYMBOL>`, and rejects missing/stale public mark prices by default
- Strategy config changes are audited, versioned, and emitted as system events; live mode remains blocked even in config validation/update paths
- `/metrics` exposes operational state only, not secrets, but it should still be restricted at the network boundary in production

## TODO boundaries

- Secret management is not implemented yet
- Database role hardening is not implemented yet
- Operator auth is intentionally local/single-tenant: OWNER, OPERATOR, VIEWER
- Bootstrap owner creation is one-time only and requires `AEGIS_BOOTSTRAP_OWNER_EMAIL` plus `AEGIS_BOOTSTRAP_OWNER_PASSWORD`
- Request signing and exchange credential handling are intentionally deferred until after paper trading is stable
- Replay/backtest reads stored candles only and must not mutate production `signals`, `risk_decisions`, or `orders`
- Paper accounting reads only stored paper orders and stored public market data; it does not call exchange private endpoints or handle API keys
- `/metrics` is public by default for local/internal MVP use unless `AEGIS_PROTECT_METRICS=true`; production deployment should place it behind private networking, reverse-proxy policy, or equivalent access controls
