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
- Completed: deterministic higher-timeframe candle aggregation (5m/15m/1h) from stored 1m candles with shared persistence, coverage inspection, and replay/diagnostics/experiment support
- Completed: research data coverage planning and dataset build orchestration with explicit missing-range detection, persisted build history, 1m-only source backfill, higher-timeframe re-aggregation, API/CLI/dashboard surfaces, and no execution-side mutation
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
- Completed: isolated testnet execution lifecycle bridge with deterministic transitions across submit ACK, private stream, reconciliation, and cancel flows
- Completed: isolated testnet operator repair controls with typed confirmation, owner/operator role gates, repair history persistence, and API/CLI/dashboard surfaces
- Completed: gated testnet pipeline preview/submit boundary with approved-risk gating, owner-confirmed submit, isolated persistence, and no paper/backtest mutation
- Completed: testnet shadow mode runner with persistent scheduler config/state, daemon loop, manual run-once controls, API/CLI/dashboard inspection, isolated telemetry, and no exchange submission
- Completed: shadow-to-testnet promotion gate with persisted `WOULD_SUBMIT` promotion previews, owner-confirmed `PROMOTE TESTNET <SYMBOL>` submits, isolated audit trail from shadow run to testnet order, and no paper/backtest/live mutation
- Completed: read-only strategy performance analytics across backtest, paper, and shadow data with API, CLI, dashboard, and bounded telemetry support
- Completed: read-only promotion funnel analytics across shadow would-submit, promotion preview/submit, isolated testnet order lifecycle outcomes, API/CLI/dashboard surfaces, and bounded low-cardinality telemetry
- Completed: read-only operator daily reports across health, feed freshness, strategy/risk behavior, paper PnL, shadow outcomes, promotion funnel, and isolated testnet execution with optional persisted exports
- Completed: deterministic execution readiness gate with API/CLI/dashboard inspection, optional readiness snapshots, and bounded telemetry across paper/testnet shadow promotion boundaries
- Current focus: use research-ready dataset preparation to raise experiment and walk-forward reliability while continuing deterministic multi-timeframe comparison, global ranking across parameter sweeps, skipped-window visibility, readiness heuristics, and promotion-readiness workflows before any live execution surface area

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
