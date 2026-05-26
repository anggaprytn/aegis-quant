# Documentation Index

- [PRD](./PRD.md): product framing and long-range intent
- [Architecture](./ARCHITECTURE.md): detailed component and data-flow notes
- [Architecture Overview](./ARCHITECTURE_OVERVIEW.md): v0.1 release-safe ASCII system diagram
- [Security](./SECURITY.md): detailed security and isolation rules
- [Security Checklist](./SECURITY_CHECKLIST.md): operator-facing hardening checklist for local and demo environments
- [Operator Checklist](./OPERATOR_CHECKLIST.md): preflight and emergency-stop runbook
- [Roadmap](./ROADMAP.md): milestone history and next work

Research note:
- Walk-forward validation is available through the strategy experiments surface as research-only out-of-sample testing. It persists into isolated walk-forward tables and must not mutate paper, shadow, testnet, or live execution state.

Research workflow:
1. Build the research dataset so 1m coverage gaps are backfilled and 5m/15m/1h candles are re-aggregated.
2. Run the multi-timeframe strategy experiment on the prepared dataset.
3. Run walk-forward validation on the same prepared window.
4. Create a research candidate from the strongest experiment run or manual review package.
5. Review candidate details, lifecycle events, and read-only observation output.
6. Explicitly mark the candidate as observing when shadow review begins.
7. Decide `ACCEPT_FOR_SHADOW`, `REJECT`, `ARCHIVE`, or `REOPEN` with an auditable reason.

Research candidate lifecycle boundaries:
- Candidate creation, observation, decisions, and archival do not execute trades.
- Candidate lifecycle operations do not auto-submit anything.
- Candidate observation does not mutate paper or testnet execution state.
- Candidate lifecycle operations do not mutate signals, risk decisions, paper state, testnet state, or live execution state.
- No live trading path is enabled.
