# Security

## Current posture

This repository intentionally avoids live trading and real exchange credentials. The current milestone is a research control plane with safe VPS monitoring and local research/shadow evidence collection. Authenticated exchange actions remain isolated to explicit testnet paths; research and shadow observation do not submit orders. No LLM execution path is enabled.

## Safety boundary summary

- No live trading is implemented.
- Binance production/private trading endpoints are intentionally unsupported.
- Public market-data endpoints may be used for ingest, backfill, repair, and research.
- Research actions do not mutate execution tables.
- Research shadow rows are evidence records, not exchange, paper, or live execution rows.
- Candidate shadow observation requires `SHADOW_OBSERVATION_ONLY=true`; missing or false guard state fails closed.
- Candidate-specific scheduled shadow observation is observation-only and not part of bootstrap-safe.
- Shadow and testnet paths are isolated from paper and live execution state.
- Local development may use disposable YOLO validation: reset Docker volumes, recreate Postgres, bootstrap a new owner, reseed data, run migrations from scratch, and execute research/backtest/shadow smoke checks.
- VPS or production-ish validation must be conservative: run migrations intentionally, back up before destructive changes, avoid local-volume reset assumptions, and prefer read-only checks unless a deployment task explicitly requires mutation.

Execution tables for safety checks:

- `orders`
- `paper_positions`
- `paper_fills`
- `exchange_testnet_orders`
- `exchange_testnet_order_lifecycle_events`
- `testnet_shadow_promotions`

Research-only smoke runs may create rows in research tables such as plan-run history, experiments, batches, campaigns, robustness matrix, walk-forward, candidates, reviews, observations, and reports. They must leave the execution tables above unchanged unless a task explicitly asks to test a paper/testnet execution path.

If a VPS has read-only database roles or views such as `aegis_readonly` / `ai_read`, use them for validation queries, count checks, and evidence collection. Do not use write-capable credentials for exploratory inspection when read-only access is available.

The current research-only VPS safety expectation is:

```txt
orders=0
paper_positions=0
paper_fills=0
exchange_testnet_orders=0
exchange_testnet_order_lifecycle_events=0
testnet_shadow_promotions=0
```

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
- Research experiment plan preview persists only plan-run audit/history and does not create experiments, batches, campaigns, matrices, walk-forward runs, candidates, or execution rows
- Research experiment plan run creates only the explicit research artifact for the selected plan type and must not mutate execution tables
- Candidate proposal, candidate creation gate, candidate lifecycle decisions, candidate observation, and shadow config promotion are research controls only and must not submit orders
- Unique-candle candidate shadow observation may create at most one independent observation per new closed candle and must not create paper, testnet, or live execution rows
- `NO_SIGNAL` is valid shadow evidence but does not qualify a candidate; `WOULD_SUBMIT` is qualifying evidence; stale-feed skips are infrastructure skips
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
- Research candidate shadow observation uses the no-submit shadow path for evidence only and does not authorize testnet shadow promotion submit
- Testnet shadow promotion preview requires operator-or-owner auth, a persisted `WOULD_SUBMIT` shadow run, an approved persisted `risk_decision_id`, an inactive kill switch, an enabled strategy config, and fresh local stored pricing; it persists only `testnet_shadow_promotions` and never auto-submits or creates isolated lifecycle state
- Testnet shadow promotion submit requires owner authorization, exact typed confirmation `PROMOTE TESTNET <SYMBOL>`, a non-expired `PREVIEWED` promotion, an inactive kill switch, and a still-approved persisted `risk_decision_id`; it submits only the promotion's persisted would-submit payload
- Testnet shadow runner status/config are inspectable by VIEWER, manual `RUN_ONCE`/`PAUSE`/`RESUME` are operator-or-owner only, and `START`/`STOP`/config update remain owner-gated
- Testnet pipeline submit requires owner authorization, exact typed confirmation `SUBMIT TESTNET <SYMBOL>`, an approved persisted `risk_decision_id`, an inactive kill switch, and persists only isolated testnet execution state
- Testnet repair actions require per-order typed confirmation: `REPAIR TESTNET <CLIENT_ORDER_ID>` or `CANCEL TESTNET <CLIENT_ORDER_ID>` for safe cancel
- Testnet shadow mode must not create `exchange_testnet_orders`, must not append lifecycle events, and must not mutate paper/backtest/live execution tables
- Testnet shadow promotions must not auto-submit, must not touch production Binance endpoints, and must not mutate paper orders, paper positions, paper PnL, backtest tables, or live execution tables
- Testnet shadow runner must never submit automatically, must never touch production Binance endpoints, and may persist only `testnet_shadow_runs`, `testnet_shadow_runner_config`, and `testnet_shadow_runner_state`
- Scheduled `CANDIDATE_SHADOW_OBSERVE_ONCE` jobs must never create `orders`, `paper_positions`, `paper_fills`, `exchange_testnet_orders`, `exchange_testnet_order_lifecycle_events`, `testnet_shadow_promotions`, or live execution rows
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
- VPS read-only validation must use `aegis_readonly` and `ai_read` views when available. The validator must not print secrets and must not run sync, migrations, POST research actions, scheduler mutations, paper actions, testnet submits, or live execution actions.
- Prefer `aegislogin` and the CLI token file for VPS auth. If `AEGIS_ACCESS_TOKEN` is stale, unset it before validation so the CLI token file or `--auto-login` flow can refresh safely.

## TODO boundaries

- Secret management is not implemented yet
- Database role hardening is not implemented yet
- Operator auth is intentionally local/single-tenant: OWNER, OPERATOR, VIEWER
- Bootstrap owner creation is one-time only and requires `AEGIS_BOOTSTRAP_OWNER_EMAIL` plus `AEGIS_BOOTSTRAP_OWNER_PASSWORD`
- DB-backed auth tests cover bootstrap, login/session persistence, refresh rotation, logout revocation, unauthenticated rejection, and role-based forbids against Postgres
- Production/private exchange execution remains deferred; only Binance Spot Testnet skeleton endpoints are present
- Replay/backtest reads stored candles only and must not mutate production `signals`, `risk_decisions`, or `orders`
- Strategy experiments are research-only and may persist only `strategy_experiments` plus `strategy_experiment_runs`; they must not mutate `strategy_configs`, `signals`, `risk_decisions`, `orders`, `paper_*`, `testnet_shadow_*`, or `exchange_testnet_*`
- Baseline research strategies are candle-only, long-only, deterministic signal generators for replay, diagnostics, and candidate evidence. They provide no financial promise and do not receive execution authority.
- Strategy analytics reads persisted backtest, paper, signal/risk, and shadow rows only; it must not mutate paper orders, paper positions, paper PnL, backtest tables, isolated testnet execution tables, or lifecycle history
- Operator reports may optionally persist into `operator_reports`, but generation must never mutate `orders`, `paper_*`, `backtest_*`, `testnet_shadow_*`, `exchange_testnet_*`, or reconciliation tables
- Paper accounting reads only stored paper orders and stored public market data; it does not call exchange private endpoints or handle API keys
- `/metrics` is public by default for local/internal MVP use unless `AEGIS_PROTECT_METRICS=true`; production deployment should place it behind private networking, reverse-proxy policy, or equivalent access controls
