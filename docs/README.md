# Documentation Index

- [PRD](./PRD.md): product framing and long-range intent
- [Architecture](./ARCHITECTURE.md): detailed component and data-flow notes
- [Architecture Overview](./ARCHITECTURE_OVERVIEW.md): v0.1 release-safe ASCII system diagram
- [Security](./SECURITY.md): detailed security and isolation rules
- [Security Checklist](./SECURITY_CHECKLIST.md): operator-facing hardening checklist for local and demo environments
- [Operator Checklist](./OPERATOR_CHECKLIST.md): preflight and emergency-stop runbook
- [Roadmap](./ROADMAP.md): milestone history and next work

Research note:
- Walk-forward validation is available through the strategy experiments surface as research-only out-of-sample testing. It reports robustness status and recommendation metadata to help detect overfit candidates, persists into isolated walk-forward tables, and must not mutate paper, shadow, testnet, or live execution state.
- `trend_filter_momentum_v1` and `volatility_breakout_v2` are conservative candle-only research baselines for experiments, diagnostics, and candidate evidence. They are long-only comparators with no financial promise and no execution authority.

Research workflow:
1. Build the research dataset so 1m coverage gaps are backfilled and 5m/15m/1h candles are re-aggregated.
2. Run the multi-timeframe strategy experiment on the prepared dataset.
3. Run walk-forward validation on the same prepared window.
4. Create a research candidate from the strongest experiment run or manual review package.
5. Review candidate details, lifecycle events, and read-only observation output.
6. Explicitly mark the candidate as observing when shadow review begins.
7. Re-run observation whenever runner configuration or readiness context changes.
8. Decide `ACCEPT_FOR_SHADOW`, `REJECT`, `ARCHIVE`, or `REOPEN` with an auditable reason.
9. Preview and explicitly confirm shadow-runner config promotion only after the accepted candidate remains fresh and eligible.
10. Review candidate qualification before any testnet promotion consideration.

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
- No live trading path is enabled.
