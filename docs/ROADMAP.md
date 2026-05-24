# Roadmap

## MVP foundation

1. Rust workspace and compile-safe crate boundaries
2. Shared core types
3. Initial Postgres schema
4. Event model and publisher abstraction
5. Health and status API
6. Binance public market ingest and deterministic 1m candles
7. Persistent market feed status and market data read APIs
8. Deterministic candle-only strategy signal generation on stored candles
9. Deterministic paper pipeline: closed candles -> signal -> risk decision -> paper order lifecycle
10. Persistent kill switch and paper-only order lifecycle APIs
11. Deterministic replay/backtest MVP on stored candles and persisted strategy configs
12. Minimal operational dashboard shell for paper-only inspection and control

## Current status

- Completed: persistent kill switch, deterministic risk evaluation, Binance public ingest, deterministic candle building, strategy signal generation, paper-only order lifecycle, replay/backtest MVP, DB-backed integration harness, and minimal dashboard shell
- Completed: Binance public REST historical candle backfill with persisted run tracking, CLI/API entrypoints, and dashboard inspection
- Completed: cockpit inspection APIs for persisted risk decisions, enriched order inspection, and filtered recent events
- Completed: local/operator CLI fallback over the existing HTTP API for status, kill switch control, paper pipeline runs, strategies, orders, events, risk decisions, and backtests
- Completed: paper account, position, fill, journal, mark-to-market, manual close flow, and equity snapshot accounting for operational paper trading
- Completed: Prometheus-compatible telemetry, `/metrics`, CLI metrics fetch, and dashboard telemetry inspection
- Completed: strategy config validation, version history, audit logging, dry-run evaluation, and operator-facing config controls in API/CLI/dashboard
- Completed: local operator auth MVP with OWNER/OPERATOR/VIEWER roles, dashboard login, CLI bearer login flow, DB-backed sessions, refresh rotation, and protected mutating endpoints
- Completed: DB-backed auth persistence/authorization coverage plus CLI refresh persistence, explicit `auth refresh`, and one-shot automatic refresh retry on `401`
- Completed: Binance Spot Testnet adapter skeleton with isolated persistence, owner-gated submit/cancel, CLI support, dashboard inspection, and testnet-only guardrails
- Completed: testnet order reconciliation with persisted runs/mismatches, operator-triggered API/CLI/dashboard controls, and testnet-only status normalization
- Completed: Binance Spot Testnet private user-data stream skeleton with listen-key lifecycle, normalized execution-report persistence, isolated testnet order status updates, stale/disconnect tracking, and API/CLI/dashboard inspection
- Current focus: tighten paper reconciliation and broader operator controls around paper/testnet boundaries before any live execution surface area

## Next implementation steps

1. Add paper trading reconciliation on top of the close/exit lifecycle
2. Add richer risk rules using data freshness, open position state, and paper account drawdown
3. Add monitoring polish and alerting guidance on top of the telemetry surface
4. Add strategy scheduling and bounded automation around the existing pipeline
5. Extend replay/backtest with richer sizing, short/flat state, and research workflows

## Explicitly deferred

- Live trading
- Production exchange order execution
- Production private exchange streams and API keys
- Multi-exchange support
- Complex dashboard UI or heavy charting
- Complex terminal UI
- Production secrets management
