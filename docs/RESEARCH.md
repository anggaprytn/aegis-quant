# Research Workflows

## Current Research Milestone

Aegis is currently a research control plane and evidence factory around stored public market data. It does not own order submission and is not a live trading bot.

Current state:

- Market data ingestion, public Binance backfill, 1m candle storage, 5m/15m/1h aggregation, candle quality checks, repair, provider diagnostics, and fallback handling are available.
- Research batches, campaigns, regime calibration/discovery/datasets, robustness matrix, walk-forward validation, failure attribution, opportunity analysis, replay/opportunity consistency, exit attribution, signal-feature attribution, hypothesis generation, experiment planning, and explicit plan preview/run are available.
- Candidate creation gate, proposal flow, stale research run recovery, shadow observation-only mode, unique-candle shadow observation, and scheduled safe monitoring jobs are available.
- VPS safe scheduled monitoring is live. The latest read-only validator result is `OK=28`, `WARN=0`, `FAIL=0`.
- No paper, testnet, or live order path should be used from research or shadow observation.

## Current Research Lifecycle

```txt
public market ingest
-> candle storage
-> data quality / repair
-> higher-timeframe aggregation
-> research dataset build
-> experiment / batch / campaign
-> walk-forward validation
-> robustness matrix
-> failure attribution
-> opportunity / replay consistency / exit / signal-feature attribution
-> hypothesis generation
-> operator review
-> experiment plan
-> plan preview / run
-> explicit artifact:
   strategy experiment | research batch | research campaign | robustness matrix | walk-forward
-> candidate proposal
-> candidate creation gate
-> research candidate
-> observation
-> accepted shadow config coverage
-> unique-candle shadow observation
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

## Current Alpha Status

Most tested families are not actionable:

- `trend_filter_momentum_v1`: fee drag and overtrade.
- `trend_filter_momentum_v2`: sample-specific and not robust.
- `range_reversion_v1`: weak with too few trades.
- `volatility_breakout_v2`: weak.
- `volatility_compression_breakout_v1`: interesting but fragile and false-breakout prone.
- `trend_pullback_continuation_v1`: too restrictive by default; loosened versions remain weak or overfit.

The first real candidate family is `failed_breakdown_reclaim_v1`:

- Deterministic, candle-only, long-only.
- Best evidence is ETH-specific, not BTC-generalized.
- BTC failed or remained mixed with overfit risk.
- ETH evidence is the strongest found so far, but it is not production-ready.

Current ETH candidate:

- `candidate_id`: `70867792-93df-494c-9a8b-d961c73107e4`
- `strategy`: `failed_breakdown_reclaim_v1`
- `symbol`: `ETHUSDT`
- `timeframe`: `1h`
- `status`: `PROMOTED_TO_SHADOW_CONFIG`
- `source_experiment_run_id`: `cdd3fbef-9e39-49e3-8e16-e23f19611cf0`
- `source_walk_forward_run_id`: `1279f5b3-9ffb-4534-babe-2e07f94a8180`
- `source_robustness_matrix_run_id`: `cebc28cd-36c3-4877-a6f0-172e4dcc2d80`
- `config_fingerprint`: `399c3e554330ffb1bfbeafe1f1b090e32ba51e985eb383d242527137833750da`

Candidate evidence:

- ETH-only.
- Data quality `GOOD`.
- Experiment PnL about `+77.27%`.
- Walk-forward `ROBUST`.
- `7/7` profitable walk-forward windows.
- Worst walk-forward window about `+0.4045%`.
- Robustness matrix `ROBUST`.
- Candidate gate `ACTIONABLE`.
- Accepted for shadow manually with warning acknowledgement.
- Promoted to shadow config manually through exact-confirmation flow.
- Runner config is aligned locally for `failed_breakdown_reclaim_v1 ETHUSDT 1h`.
- No paper, testnet, or live execution.

Current threshold state:

- `independent_shadow_observation_count`: `3 / 30`
- `would_submit_count`: `0 / 3`
- `qualification`: `NOT_QUALIFIED`
- `dossier`: `BLOCKED`
- `recommendation`: `KEEP_OBSERVING`

Why there is no paper/testnet/live step yet:

- Candidate evidence is ETH-only and not BTC-generalized.
- 2025+ final holdout is limited or unavailable in local evidence.
- Shadow evidence is still insufficient.
- `WOULD_SUBMIT` evidence is still `0`.
- Qualification remains `NOT_QUALIFIED`.
- Testnet review dossier remains `BLOCKED`.
- No `MARK_READY_FOR_TESTNET_REVIEW` action is recorded.

## Candidate Proposal and Creation Gate

Candidate proposals are research review packages. They describe evidence that may justify a candidate, but they are not candidates and do not alter execution surfaces.

Candidate creation requires the candidate gate to classify the evidence as actionable enough for review. Creation appends auditable research lifecycle records only. It does not create signals, risk decisions, paper orders, paper fills, paper positions, testnet shadow promotions, isolated testnet orders, lifecycle events, or live execution state.

The gate is intentionally conservative. It can allow a candidate into observation while preserving warnings such as ETH-only evidence, limited holdout, or generalization failure. Those warnings must remain visible through acceptance, shadow-config promotion, qualification, and dossier review.

## Stale Run Recovery

Stale research run recovery exists for incomplete or abandoned research artifacts. Preview/apply flows must be explicit and auditable.

Recovery may close or mark stale research runs according to the recovery action. It must not mutate execution tables or imply that any candidate is ready for paper, testnet, or live execution.

## Shadow Evidence Semantics

Shadow observation is observation-only:

- `SHADOW_OBSERVATION_ONLY=false` fails closed for candidate shadow observation.
- `SHADOW_OBSERVATION_ONLY=true` allows observation-only shadow rows.
- Cross-asset shadow observation uses the same server-side guard. CLI request flags are operator
  intent acknowledgements only; they do not enable observation-only mode on the server.
- `POST /research/candidates/:id/shadow-observe-once` creates at most one independent observation for the newest closed candle.
- CLI equivalent: `aegis research candidates shadow-observe-once <candidate_id>`.
- Duplicate same-candle observations are duplicate operational checks, not independent evidence.
- `NO_SIGNAL` is a valid observation, but it is not qualifying evidence.
- `WOULD_SUBMIT` is qualifying evidence.
- Skipped stale feed is an infrastructure skip, not alpha evidence.
- Shadow observation does not create paper, testnet, or live orders.

## Shadow Runner Promotion Config Policy

The MVP shadow runner stores one singleton `timeframe` for all configured strategy/symbol pairs. Because that cannot safely represent mixed coverage such as `BTCUSDT 1m` and `ETHUSDT 1h` at the same time, research candidate shadow promotion uses candidate-only replacement when the accepted candidate differs from the current runner config.

Preview must show the current config, proposed config, structured diff, blockers, warnings, status, and recommendation. Timeframe mismatch is a proposed config change, not a hard blocker, when the candidate is already `ACCEPTED_FOR_SHADOW` and the operator allows missing runner alignment. Apply keeps the existing enabled flag, does not start the runner, does not create shadow runs, and does not create paper, testnet, or live execution rows.

## Scheduled Candidate Shadow Observation

`CANDIDATE_SHADOW_OBSERVE_ONCE` is a manually created scheduled research job for one promoted candidate. It requires `SHADOW_OBSERVATION_ONLY=true` and current shadow-runner config coverage for the candidate. Each run checks the newest closed candle first. If there is no newer closed candle than the candidate's latest linked evaluated candle, it records `SKIPPED_NO_NEW_CANDLE` in the scheduled-job result and does not create a `testnet_shadow_runs` row. If a newer closed candle exists, it uses the existing no-submit shadow path to create one safe shadow run and candidate link.

This job kind is intentionally excluded from `scheduled-jobs bootstrap-safe` because it is candidate-specific.

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
