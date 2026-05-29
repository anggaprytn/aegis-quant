# Research Workflows

## Current Research Loop

The research platform is an evidence factory around stored public market data. It does not own order submission.

```txt
public market ingest
-> candle storage
-> data quality / repair
-> higher-timeframe aggregation
-> research dataset build
-> campaigns
-> failure attribution
-> hypothesis generation
-> operator review
-> experiment plan
-> plan preview / run
-> explicit artifact:
   strategy experiment | research batch | research campaign | robustness matrix | walk-forward
-> research candidates
-> shadow tracking
-> qualification
-> testnet review dossier
-> operator report
```

Boundaries:

- Market data ingestion and repair use public data only.
- Research dataset builds, campaigns, hypotheses, plans, matrices, walk-forward runs, candidates, qualifications, dossiers, and reports are research or inspection surfaces.
- Strategy/risk/execution behavior is not changed by research workflows.
- Research workflows must not create paper orders, paper fills, paper positions, isolated testnet orders, testnet lifecycle events, testnet shadow promotions, or live execution state.
- Candidate promotion to shadow configuration is a review/config coverage step; it does not submit orders.
- No live trading path is enabled.

## Shadow Runner Promotion Config Policy

The MVP shadow runner stores one singleton `timeframe` for all configured strategy/symbol pairs. Because that cannot safely represent mixed coverage such as `BTCUSDT 1m` and `ETHUSDT 1h` at the same time, research candidate shadow promotion uses candidate-only replacement when the accepted candidate differs from the current runner config.

Preview must show the current config, proposed config, structured diff, blockers, warnings, status, and recommendation. Timeframe mismatch is a proposed config change, not a hard blocker, when the candidate is already `ACCEPTED_FOR_SHADOW` and the operator allows missing runner alignment. Apply keeps the existing enabled flag, does not start the runner, does not create shadow runs, and does not create paper, testnet, or live execution rows.

## Experiment Plan Runner Semantics

`POST /research/experiment-plans/:id/run-preview` is not a dry run in the sense of being invisible. It persists a plan-run history record so the operator has an audit trail of previews and blockers.

Preview creates:

- a persisted `research_experiment_plan_runs` history row
- the response payload showing the artifact type that would be created
- warnings/blockers/recommendation metadata

Preview does not create:

- `strategy_experiments` or `strategy_experiment_runs`
- `research_batches` or campaign-linked batch artifacts
- `research_campaigns`
- `strategy_robustness_matrix_runs` or cells
- `strategy_walk_forward_runs`
- research candidates
- paper, shadow, testnet, or live execution rows

`POST /research/experiment-plans/:id/run` requires exact confirmation `RUN RESEARCH PLAN <plan_id>`. A successful run creates exactly the explicit research artifact for the plan type and records completion history. It still does not touch execution tables.

## Regime Calibration Precedence

Regime discovery accepts both an inline `classifier_config` and a saved `calibration_id`.

The deterministic precedence rule is:

1. If `classifier_config` is present, discovery uses it.
2. If `classifier_config` is absent and `calibration_id` is present, discovery loads the saved calibration's `recommended_config`.
3. If neither is present, discovery uses the default classifier config.

This keeps ad hoc threshold experiments explicit while making persisted calibration reusable by ID.
