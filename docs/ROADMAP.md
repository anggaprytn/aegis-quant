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

## Current phase: Research Control Plane v0.1

- Aegis is currently a research control plane, not a live trading bot.
- VPS safe scheduled monitoring is operational.
- The research pipeline supports data ingestion, public Binance backfill, 1m -> 5m/15m/1h aggregation, candle quality and repair, provider fallback diagnostics, research campaigns, regime work, robustness matrix, walk-forward validation, attribution/opportunity analysis, candidate gating, stale recovery, and shadow observation-only evidence collection.
- The first real candidate family is `failed_breakdown_reclaim_v1`.
- Current candidate: `70867792-93df-494c-9a8b-d961c73107e4`, `ETHUSDT 1h`, `PROMOTED_TO_SHADOW_CONFIG`, `3/30` independent observations, `0/3` `WOULD_SUBMIT`, `NOT_QUALIFIED`, dossier `BLOCKED`.
- Paper/testnet promotion is gated by evidence thresholds, dossier review, and human review.
- Live trading is not on the near-term roadmap.

## Completed foundation

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
- Completed: conservative candle-only research baselines, including trend-filter momentum and volume-confirmed breakout, exposed through diagnostics, backtests, experiments, CLI/API, and dashboard selection without changing execution authority
- Completed: scheduled safe monitoring jobs for provider health, aggregation status, market-data quality, and daily operator report
- Completed: candidate-specific scheduled shadow observation job kind, excluded from bootstrap-safe and manual per candidate
- Current focus: accumulate independent shadow evidence for the ETH-specific candidate while keeping all research, qualification, and dossier work non-executing

## Next implementation steps

1. Accumulate at least 30 independent unique-candle shadow observations for the ETH candidate.
2. Accumulate at least 3 `WOULD_SUBMIT` observations.
3. Keep skipped/error rates low enough for qualification thresholds.
4. Re-run qualification evaluation and dossier review after evidence thresholds are met.
5. Record manual `MARK_READY_FOR_TESTNET_REVIEW` only after evidence and review justify it.
6. Keep paper/testnet disabled for this candidate until the dossier is unblocked by human review.

## Explicitly deferred

- Live trading
- Near-term live trading roadmap
- Production exchange order execution
- Production private exchange streams and API keys
- Automatic paper/testnet promotion from research
- Multi-exchange support
- Complex dashboard UI or heavy charting
- Complex terminal UI
- Production secrets management
