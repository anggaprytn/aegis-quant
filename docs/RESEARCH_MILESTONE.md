# Aegis Research Milestone Report

## Current Status

Aegis is operating as a research control plane.

- Research control plane capabilities are operational.
- VPS safe scheduled monitoring is live.
- The first ETH-specific candidate is promoted to shadow config.
- No paper, testnet, or live execution is active from research or shadow observation.

## What Aegis Can Do Now

- Market data ingestion.
- Public Binance backfill.
- 1m candle storage and 5m / 15m / 1h aggregation.
- Candle quality checks and repair.
- Provider diagnostics and fallback handling.
- Research batches and campaigns.
- Regime calibration, regime discovery, and regime datasets.
- Robustness matrix and walk-forward validation.
- Failure attribution, opportunity analysis, replay/opportunity consistency, exit attribution, and signal-feature attribution.
- Hypothesis generation and experiment planning.
- Explicit research plan preview/run.
- Candidate creation gate and proposal flow.
- Stale research run recovery.
- Shadow observation-only mode.
- Unique-candle shadow observation.
- Scheduled safe monitoring.
- Candidate-specific scheduled shadow observation job kind.
- Manual scheduled cross-asset public market-data refresh for research observation freshness.

## First Real Candidate Family

`failed_breakdown_reclaim_v1` is the first real candidate family found so far.

- Deterministic, candle-only, long-only.
- Strongest current evidence is `ETHUSDT 1h`.
- It is stronger than previous families because the ETH run passed the candidate gate, walk-forward validation, and robustness matrix with profitable windows.
- BTC did not generalize cleanly and remains failed, mixed, or overfit-risk evidence.

Previous tested families remain not actionable: `trend_filter_momentum_v1`, `trend_filter_momentum_v2`, `range_reversion_v1`, `volatility_breakout_v2`, `volatility_compression_breakout_v1`, and `trend_pullback_continuation_v1`.

## Current Candidate Lifecycle Status

- `candidate_id`: `70867792-93df-494c-9a8b-d961c73107e4`
- `strategy`: `failed_breakdown_reclaim_v1`
- `symbol`: `ETHUSDT`
- `timeframe`: `1h`
- `status`: `PROMOTED_TO_SHADOW_CONFIG`
- `source_experiment_run_id`: `cdd3fbef-9e39-49e3-8e16-e23f19611cf0`
- `source_walk_forward_run_id`: `1279f5b3-9ffb-4534-babe-2e07f94a8180`
- `source_robustness_matrix_run_id`: `cebc28cd-36c3-4877-a6f0-172e4dcc2d80`
- `config_fingerprint`: `399c3e554330ffb1bfbeafe1f1b090e32ba51e985eb383d242527137833750da`
- `independent_shadow_observation_count`: `3 / 30`
- `would_submit_count`: `0 / 3`
- `qualification`: `NOT_QUALIFIED`
- `dossier`: `BLOCKED`
- `recommendation`: `KEEP_OBSERVING`

## Why No Paper/Testnet Yet

- Independent shadow evidence is insufficient.
- `WOULD_SUBMIT` evidence is `0 / 3`.
- No ready-review action has been recorded.
- Evidence is ETH-only and not BTC-generalized.
- 2025+ final holdout evidence is limited or unavailable locally.
- Qualification remains `NOT_QUALIFIED`.
- Testnet review dossier remains `BLOCKED`.
- Research and shadow observation are explicitly non-executing.

## Next Evidence Thresholds

- Reach 30 independent unique-candle shadow observations.
- Reach at least 3 `WOULD_SUBMIT` observations.
- Keep skipped/error rate low.
- Re-run qualification evaluation.
- Review the testnet dossier.
- Record manual `MARK_READY_FOR_TESTNET_REVIEW` only after evidence supports it.
- Do not automatically promote.

## Current Verdict

The platform is mature enough for research control-plane operation and safe monitoring.

Alpha evidence exists only as an ETH-specific research candidate. The candidate is not ready for testnet. Continue observing; do not trade.
