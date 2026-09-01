# Research Milestone

This is a repository-level capability snapshot, not a live deployment report.
Runtime counts, candidate identifiers, and evidence metrics belong to the
database instance that produced them and should be reviewed through the API,
CLI, or an exported evidence bundle.

## Current phase

Aegis Quant is in an experimental research-control-plane phase. The core
objective is to make market-data quality, deterministic analysis, risk
decisions, paper accounting, isolated testnet state, reconciliation, and
operator actions inspectable before any consideration of real capital.

No live trading path is enabled. Research and shadow observation do not submit
paper, testnet, or live orders.

## Implemented capabilities

- Rust workspace with shared domain types and explicit crate boundaries.
- PostgreSQL migrations, event logging, audit records, and health/status APIs.
- Public Binance market-data ingestion and historical candle backfill.
- Deterministic 1-minute candle construction and 5-minute, 15-minute, and
  1-hour aggregation.
- Candle coverage, freshness, quality, provider diagnostics, and repair flows.
- Deterministic strategy evaluation, replay, backtest, experiments, and
  walk-forward validation.
- Robustness matrices, attribution, hypotheses, experiment plans, and
  research-only analytics.
- Candidate proposals, creation gates, lifecycle decisions, qualification, and
  testnet-review dossier support.
- No-submit shadow observation with unique-candle evidence semantics.
- Persistent kill switch, risk decisions, paper accounting, readiness checks,
  operator reports, and Prometheus-compatible metrics.
- Isolated Binance Spot Testnet adapter, order lifecycle, reconciliation,
  private-stream skeleton, and typed-confirmation operator controls.
- Axum API, aegis CLI, Next.js dashboard, and Docker Compose packaging.

## Evidence posture

Research output is evidence for operator review, not an assertion of alpha or
profitability. The implementation intentionally expects candidates to be
rejected, degraded, or kept in observation when:

- validation is concentrated in one symbol or market regime;
- out-of-sample or holdout coverage is insufficient;
- cross-symbol generalization is weak;
- shadow observations are too few;
- data quality is degraded; or
- qualification and dossier checks are incomplete.

The default candidate-observation thresholds are implementation defaults, not a
promise that a candidate is ready: at least 30 independent shadow observations,
at least 3 WOULD_SUBMIT observations, a risk-rejection rate no higher than 40%,
and an error-or-skipped rate no higher than 20%. Operators must inspect the
full evidence and warnings before any further review.

## Next engineering focus

1. Exercise migration, readiness, reconciliation, and kill-switch recovery
   procedures on disposable environments.
2. Expand integration and operator-workflow coverage.
3. Improve deployment packaging and read-only observability without weakening
   execution isolation.
4. Continue research evidence collection only through explicit, no-submit
   workflows.
5. Keep paper/testnet promotion manual and review-gated.

## Explicitly deferred

- Live trading and production exchange order submission.
- Production exchange private endpoints.
- Automatic paper or testnet promotion from research.
- Multi-exchange routing.
- Production secret-management integration.
- Distributed orchestration and unnecessary service decomposition.
- Large dashboard/charting or terminal-UI expansion.

See the [roadmap](ROADMAP.md), [architecture](ARCHITECTURE.md), and
[research workflow guide](RESEARCH.md) for implementation detail.
