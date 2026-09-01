# Roadmap

The roadmap is intentionally biased toward correctness and operator visibility.
Live trading is not a near-term milestone.

## MVP foundation

The initial product sequence is:

1. Rust workspace and shared core types.
2. PostgreSQL migrations and persisted event model.
3. Health/status API and operational observability.
4. Public market-data ingestion, deterministic candles, and storage.
5. Strategy signal skeleton and paper-trading skeleton.
6. Persistent risk gates and kill switch.
7. Replay/backtest and research analysis.
8. Dashboard shell and operator CLI.
9. Isolated Spot Testnet adapter, reconciliation, and shadow boundaries.

## Current phase: research control plane v0.1

The repository currently supports:

- public Binance ingest and historical backfill;
- deterministic 1-minute candle construction and 5-minute, 15-minute, and
  1-hour aggregation;
- coverage, quality, freshness, provider diagnostics, repair, and dataset
  preparation;
- deterministic strategies, replay/backtest, experiments, walk-forward
  validation, robustness analysis, and attribution;
- research campaigns, regime work, hypotheses, experiment plans, candidate
  gates, qualification, dossiers, and evidence exports;
- no-submit shadow observation with unique-candle semantics;
- persistent risk decisions, paper accounting, readiness checks, operator
  reports, and kill-switch enforcement;
- isolated Binance Spot Testnet order lifecycle, private-stream skeleton,
  reconciliation, repair, and typed-confirmation controls; and
- API, CLI, dashboard, Docker Compose profiles, and Prometheus-compatible
  metrics.

This phase is about accumulating trustworthy evidence and exercising safety
controls. It does not establish profitability, production readiness, or
permission to trade.

## Completed foundation

- Compile-safe Rust workspace with shared domain types.
- Postgres schema, migration runner, event taxonomy, and audit boundaries.
- Public Binance market-data client, ingest service, backfill, and candle
  aggregation.
- Strategy configuration validation, version history, deterministic signal
  evaluation, replay, and paper lifecycle.
- Risk evaluation, persistent kill switch, readiness checks, and operator
  controls.
- Paper positions, fills, journal, mark-to-market, PnL, equity snapshots, and
  typed-confirmation close flow.
- Testnet-only exchange adapter with isolated persistence, state mapping,
  reconciliation, private-stream skeleton, cancellation, repair, and promotion
  boundaries.
- Research dataset preparation, experiments, walk-forward, robustness,
  attribution, candidate lifecycle, qualification, and shadow evidence.
- Dashboard cockpit, CLI fallback, HTTP API, telemetry, reports, and Docker
  Compose packaging.
- Local verification targets, database integration-test harness, safe demo
  script, read-only validator, and operator/security documentation.

## Near-term implementation priorities

1. Add more integration coverage for migration, readiness, reconciliation, and
   kill-switch recovery.
2. Exercise the runbook against disposable Compose deployments.
3. Improve deployment and worker packaging while preserving isolated persistence
   and explicit authorization.
4. Improve research data reliability, evidence export hygiene, and operator
   diagnostics.
5. Keep all candidate promotion and testnet actions manual, typed, and
   auditable.

## Explicitly deferred

- Live trading.
- Production exchange order execution or private endpoints.
- Automatic promotion from research to paper or testnet.
- Multi-exchange routing.
- Leverage, margin, and derivatives execution.
- Production secrets management.
- Kubernetes, distributed orchestration, and microservice decomposition.
- Heavy charting, complex terminal UI, and unrelated frontend expansion.
