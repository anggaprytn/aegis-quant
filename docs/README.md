# Documentation Index

- [PRD](./PRD.md): product framing and long-range intent
- [Architecture](./ARCHITECTURE.md): detailed component and data-flow notes
- [Architecture Overview](./ARCHITECTURE_OVERVIEW.md): v0.1 release-safe ASCII system diagram
- [Research Workflow](./RESEARCH.md): research lifecycle, candidate gates, shadow observation semantics, and current candidate state
- [Research Milestone Report](./RESEARCH_MILESTONE.md): compact current-state report for the research control plane milestone
- [Security](./SECURITY.md): detailed security and isolation rules
- [Security Checklist](./SECURITY_CHECKLIST.md): operator-facing hardening checklist for local and demo environments
- [Operator Checklist](./OPERATOR_CHECKLIST.md): preflight and emergency-stop runbook
- [Runbook](./RUNBOOK.md): local start, VPS sync, migrations, health checks, and common failures
- [Roadmap](./ROADMAP.md): milestone history and next work

Current research milestone:
- Aegis is operating as a research control plane with safe VPS monitoring and local research/shadow evidence collection.
- VPS safe scheduled monitoring is live with `provider-health-binance`, `aggregation-status`, BTCUSDT/ETHUSDT market-data quality jobs across `1m,5m,15m,1h`, and `operator-report-daily`.
- The first real candidate family is `failed_breakdown_reclaim_v1`; the current candidate is ETH-specific, `ETHUSDT 1h`, and not yet qualified for testnet review.
- Research and shadow observation do not submit paper, testnet, or live orders.

Research note:
- Research experiment plan preview persists plan-run history for auditability but does not create downstream artifacts. Confirmed run creates the explicit research artifact only and still does not mutate execution tables.
- Walk-forward validation is available through the strategy experiments surface as research-only out-of-sample testing. It reports robustness status and recommendation metadata to help detect overfit candidates, persists into isolated walk-forward tables, and must not mutate paper, shadow, testnet, or live execution state.
- Most tested families are currently not actionable: `trend_filter_momentum_v1`, `trend_filter_momentum_v2`, `range_reversion_v1`, `volatility_breakout_v2`, `volatility_compression_breakout_v1`, and `trend_pullback_continuation_v1`.
- `failed_breakdown_reclaim_v1` is the first real candidate family. Current evidence is strongest on ETH and did not generalize cleanly to BTC.

Research workflow:
1. Build the research dataset so 1m coverage gaps are backfilled and 5m/15m/1h candles are re-aggregated.
2. Run the multi-timeframe strategy experiment on the prepared dataset.
3. Run walk-forward validation on the same prepared window.
4. Run robustness matrix and supporting attribution/opportunity checks.
5. Use the candidate creation gate and proposal flow to separate proposed evidence from an accepted candidate.
6. Review candidate details, lifecycle events, and read-only observation output.
7. Decide `ACCEPT_FOR_SHADOW`, `REJECT`, `ARCHIVE`, or `REOPEN` with an auditable reason.
8. Preview and explicitly confirm shadow-runner config promotion only after the accepted candidate remains fresh and eligible.
9. Collect unique-candle shadow observations.
10. Review qualification and testnet dossier before any owner action.

Research candidate lifecycle boundaries:
- Candidate creation, observation, decisions, and archival do not execute trades.
- Candidate lifecycle operations do not auto-submit anything.
- Candidate observation does not mutate paper or testnet execution state.
- Candidate lifecycle operations do not mutate signals, risk decisions, paper state, testnet state, or live execution state.
- `ACCEPT_FOR_SHADOW` requires a fresh persisted observation. The default freshness window is 15 minutes.
- `ACCEPTED_FOR_SHADOW` means human/research approval for shadow observation only.
- `PROMOTED_TO_SHADOW_CONFIG` means the shadow-runner config covers the candidate strategy, symbol, and timeframe.
- Shadow promotion moves an accepted candidate to `PROMOTED_TO_SHADOW_CONFIG` when config coverage is applied or already present. It only updates shadow-runner config/state-adjacent audit records and candidate lifecycle state. It does not submit testnet orders or mutate paper/testnet/live execution tables.
- Candidate-linked shadow performance is read-only and research-only. New shadow-run links attach only to `PROMOTED_TO_SHADOW_CONFIG` candidates.
- Candidate qualification checks are stateless/read-only decision support for testnet promotion consideration. They do not auto-promote, submit orders, or mutate paper/testnet/live execution tables. Default thresholds are `min_shadow_runs=30`, `min_would_submit_count=3`, `max_risk_rejection_rate_pct=40`, and `max_error_or_skipped_rate_pct=20`.
- Persisted qualification evaluations are research snapshots only. They power qualification history and the candidate watchlist so operators can track improving, degrading, newly qualified, lost qualification, or stale candidate health over time with no execution side effects.
- Unique-candle shadow observation creates only one independent observation per new closed candle. Same-candle duplicates are operational checks, not independent evidence.
- `NO_SIGNAL` is a valid observation but not qualifying evidence. `WOULD_SUBMIT` is qualifying evidence. Stale-feed skips are infrastructure skips.
- Candidate-specific `CANDIDATE_SHADOW_OBSERVE_ONCE` jobs are manual per candidate and not part of `scheduled-jobs bootstrap-safe`.
- No live trading path is enabled.
