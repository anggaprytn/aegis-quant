# Security

## Current posture

This repository scaffold intentionally avoids live trading and real exchange credentials. v0.1 also keeps authenticated exchange actions on Binance Spot Testnet only and does not enable any LLM execution path.

## Rules

- Do not commit real API keys or secrets
- Use `.env.example` only for safe placeholders
- No LLM execution path is enabled in v0.1
- Keep any future LLM components advisory only
- Require auditable state transitions for dangerous actions
- Keep risk controls and kill switch persistent when implemented
- Replay/backtest must stay isolated from live and paper execution tables
- Strategy analytics is read-only, VIEWER-readable inspection only and must never trigger execution, reconciliation, repair, or any paper/testnet mutation
- Operator daily reports are read-only, VIEWER-readable inspection/export only; they may expose operational state but must never trigger execution, reconciliation, repair, promotion submit, paper mutation, or live/testnet order submission
- Execution readiness is read-only, VIEWER-readable inspection only; it may compute and optionally persist bounded readiness snapshots, but it must never trigger execution, promotion submit, reconciliation, repair, paper mutation, or live/testnet order submission
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
- Testnet direct submit/cancel require typed confirmation `TESTNET ORDER`, owner authorization, persisted audit logs, and system events
- Testnet pipeline preview requires operator-or-owner auth, an approved persisted `risk_decision_id`, an inactive kill switch, and fresh local stored pricing; it must not submit exchange orders or persist isolated order lifecycle state
- Testnet shadow mode requires operator-or-owner auth, an inactive kill switch, enabled strategy config, persisted risk evaluation, and fresh local stored pricing; it persists only `testnet_shadow_runs` and never submits exchange orders
- Testnet shadow promotion preview requires operator-or-owner auth, a persisted `WOULD_SUBMIT` shadow run, an approved persisted `risk_decision_id`, an inactive kill switch, an enabled strategy config, and fresh local stored pricing; it persists only `testnet_shadow_promotions` and never auto-submits or creates isolated lifecycle state
- Testnet shadow promotion submit requires owner authorization, exact typed confirmation `PROMOTE TESTNET <SYMBOL>`, a non-expired `PREVIEWED` promotion, an inactive kill switch, and a still-approved persisted `risk_decision_id`; it submits only the promotion's persisted would-submit payload
- Testnet shadow runner status/config are inspectable by VIEWER, manual `RUN_ONCE`/`PAUSE`/`RESUME` are operator-or-owner only, and `START`/`STOP`/config update remain owner-gated
- Testnet pipeline submit requires owner authorization, exact typed confirmation `SUBMIT TESTNET <SYMBOL>`, an approved persisted `risk_decision_id`, an inactive kill switch, and persists only isolated testnet execution state
- Testnet repair actions require per-order typed confirmation: `REPAIR TESTNET <CLIENT_ORDER_ID>` or `CANCEL TESTNET <CLIENT_ORDER_ID>` for safe cancel
- Testnet shadow mode must not create `exchange_testnet_orders`, must not append lifecycle events, and must not mutate paper/backtest/live execution tables
- Testnet shadow promotions must not auto-submit, must not touch production Binance endpoints, and must not mutate paper orders, paper positions, paper PnL, backtest tables, or live execution tables
- Testnet shadow runner must never submit automatically, must never touch production Binance endpoints, and may persist only `testnet_shadow_runs`, `testnet_shadow_runner_config`, and `testnet_shadow_runner_state`
- Testnet execution is isolated in `exchange_testnet_orders` and must not mutate paper accounting or live execution tables
- Testnet lifecycle history is isolated in `exchange_testnet_order_lifecycle_events` and must not mutate paper accounting, backtest, or live execution tables
- Testnet promotion funnel analytics is read-only: it may read `testnet_shadow_runs`, `testnet_shadow_promotions`, `exchange_testnet_orders`, and `exchange_testnet_order_lifecycle_events`, but it must never trigger preview, submit, repair, reconciliation, paper, backtest, or live execution paths
- Testnet repair history is isolated in `exchange_testnet_repair_actions` and must not mutate paper accounting, backtest, or live execution tables
- Testnet reconciliation is isolated in `exchange_reconciliation_runs` and `exchange_reconciliation_mismatches`, is operator-triggered, and never reads secrets from the CLI or dashboard
- Testnet private stream events are isolated in `exchange_private_stream_events`, update only isolated `exchange_testnet_orders`, and never mutate paper/backtest/live tables
- Invalid private-stream or reconciliation transitions must remain visible as reconciliation-required lifecycle events; they must not be silently coerced into success
- Spot Testnet listen keys must not be persisted in plaintext; only masked values leave the backend and only hashed values are stored in Postgres
- Private stream runtime, API, CLI, metrics, and dashboard are testnet-only and must not use production Binance websocket endpoints
- `SAFE_CANCEL_REQUEST` is testnet-only, uses Binance Spot Testnet REST cancel only, and must never call production Binance endpoints
- Reconciliation mismatches and failure events must not include API secrets or high-cardinality metric labels
- Binance private REST support is testnet-only, signs requests with HMAC SHA256, and does not log API secrets
- Production Binance private-trading env vars and withdrawal endpoints are intentionally absent
- No live execution path is enabled in this repository; Binance production private trading endpoints remain intentionally unsupported

## TODO boundaries

- Secret management is not implemented yet
- Database role hardening is not implemented yet
- Operator auth is intentionally local/single-tenant: OWNER, OPERATOR, VIEWER
- Bootstrap owner creation is one-time only and requires `AEGIS_BOOTSTRAP_OWNER_EMAIL` plus `AEGIS_BOOTSTRAP_OWNER_PASSWORD`
- DB-backed auth tests cover bootstrap, login/session persistence, refresh rotation, logout revocation, unauthenticated rejection, and role-based forbids against Postgres
- Production/private exchange execution remains deferred; only Binance Spot Testnet skeleton endpoints are present
- Replay/backtest reads stored candles only and must not mutate production `signals`, `risk_decisions`, or `orders`
- Strategy experiments are research-only and may persist only `strategy_experiments` plus `strategy_experiment_runs`; they must not mutate `strategy_configs`, `signals`, `risk_decisions`, `orders`, `paper_*`, `testnet_shadow_*`, or `exchange_testnet_*`
- Strategy analytics reads persisted backtest, paper, signal/risk, and shadow rows only; it must not mutate paper orders, paper positions, paper PnL, backtest tables, isolated testnet execution tables, or lifecycle history
- Operator reports may optionally persist into `operator_reports`, but generation must never mutate `orders`, `paper_*`, `backtest_*`, `testnet_shadow_*`, `exchange_testnet_*`, or reconciliation tables
- Paper accounting reads only stored paper orders and stored public market data; it does not call exchange private endpoints or handle API keys
- `/metrics` is public by default for local/internal MVP use unless `AEGIS_PROTECT_METRICS=true`; production deployment should place it behind private networking, reverse-proxy policy, or equivalent access controls
