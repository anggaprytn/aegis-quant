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
- Binance Spot Testnet credentials, when configured, are backend-only and never exposed to the dashboard or CLI
- Passwords are hashed with Argon2id; plaintext passwords are never stored or returned
- Access tokens are short-lived JWTs and refresh tokens are stored only as hashes in Postgres
- Dashboard login stores only the access token client-side; refresh tokens stay in HTTP-only cookies and the dashboard refreshes access on `401`
- CLI login receives a refresh token only in explicit CLI JSON auth flows, persists it locally for operator use, and never prints it by default
- CLI refresh rotates the persisted refresh token, updates the local session file when file-backed auth is active, and does not overwrite the token file when the run is using `AEGIS_ACCESS_TOKEN`
- Auth-disabled mode is local-development only and injects a synthetic OWNER actor with a startup warning
- Paper position close is simulated only, requires typed confirmation `CLOSE <SYMBOL>`, and rejects missing/stale public mark prices by default
- Strategy config changes are audited, versioned, and emitted as system events; live mode remains blocked even in config validation/update paths
- `/metrics` exposes operational state only, not secrets, but it should still be restricted at the network boundary in production
- Testnet submit/cancel require typed confirmation `TESTNET ORDER`, owner authorization, persisted audit logs, and system events
- Testnet repair actions require per-order typed confirmation: `REPAIR TESTNET <CLIENT_ORDER_ID>` or `CANCEL TESTNET <CLIENT_ORDER_ID>` for safe cancel
- Testnet execution is isolated in `exchange_testnet_orders` and must not mutate paper accounting or live execution tables
- Testnet lifecycle history is isolated in `exchange_testnet_order_lifecycle_events` and must not mutate paper accounting, backtest, or live execution tables
- Testnet repair history is isolated in `exchange_testnet_repair_actions` and must not mutate paper accounting, backtest, or live execution tables
- Testnet reconciliation is isolated in `exchange_reconciliation_runs` and `exchange_reconciliation_mismatches`, is operator-triggered, and never reads secrets from the CLI or dashboard
- Testnet private stream events are isolated in `exchange_private_stream_events`, update only isolated `exchange_testnet_orders`, and never mutate paper/backtest/live tables
- Invalid private-stream or reconciliation transitions must remain visible as reconciliation-required lifecycle events; they must not be silently coerced into success
- Spot Testnet listen keys must not be persisted in plaintext; only masked values leave the backend and only hashed values are stored in Postgres
- Private stream runtime, API, CLI, metrics, and dashboard are testnet-only and must not use production Binance websocket endpoints
- `SAFE_CANCEL_REQUEST` is testnet-only, uses Binance Spot Testnet REST cancel only, and must never call production Binance endpoints
- Reconciliation mismatches and failure events must not include API secrets or high-cardinality metric labels
- Binance private REST support is testnet-only, signs requests with HMAC SHA256, and does not log API secrets
- Production Binance env vars and withdrawal endpoints are intentionally absent

## TODO boundaries

- Secret management is not implemented yet
- Database role hardening is not implemented yet
- Operator auth is intentionally local/single-tenant: OWNER, OPERATOR, VIEWER
- Bootstrap owner creation is one-time only and requires `AEGIS_BOOTSTRAP_OWNER_EMAIL` plus `AEGIS_BOOTSTRAP_OWNER_PASSWORD`
- DB-backed auth tests cover bootstrap, login/session persistence, refresh rotation, logout revocation, unauthenticated rejection, and role-based forbids against Postgres
- Production/private exchange execution remains deferred; only Binance Spot Testnet skeleton endpoints are present
- Replay/backtest reads stored candles only and must not mutate production `signals`, `risk_decisions`, or `orders`
- Paper accounting reads only stored paper orders and stored public market data; it does not call exchange private endpoints or handle API keys
- `/metrics` is public by default for local/internal MVP use unless `AEGIS_PROTECT_METRICS=true`; production deployment should place it behind private networking, reverse-proxy policy, or equivalent access controls
