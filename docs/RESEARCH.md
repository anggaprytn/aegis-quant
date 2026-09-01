# Research Workflows

This document describes the research and evidence workflows implemented by
Aegis Quant. Research artifacts are persisted for reproducibility and audit, but
they do not grant execution authority. Any identifiers or metrics in exported
evidence bundles are instance data and must not be read as the current status of
another deployment.

## Scope and current posture

Aegis Quant is a research control plane around stored public market data. The
research surface can prepare data, run deterministic analysis, compare
strategies, record evidence, and support an operator decision. It does not
submit paper, testnet, or live orders.

Implemented research capabilities include:

- public Binance candle backfill and deterministic higher-timeframe aggregation;
- candle coverage, freshness, quality, provider diagnostics, and repair;
- research dataset build orchestration with explicit missing-range reporting;
- replay and backtest over stored closed candles;
- strategy experiments, campaigns, batches, regime datasets, calibration and
  discovery;
- walk-forward validation, robustness matrices, failure attribution, opportunity
  analysis, replay/opportunity consistency, exit attribution, and
  signal-feature attribution;
- hypothesis generation and explicit experiment-plan preview/run;
- candidate proposals, creation gates, lifecycle decisions, qualification, and
  dossier decision support;
- observation-only shadow runs and unique-candle evidence collection; and
- scheduled safe monitoring jobs and operator reports.

The release is experimental and single-tenant. Research results are not a
profitability claim, and no workflow automatically promotes a candidate to
execution.

## Research lifecycle

~~~text
public market ingest
-> candle storage
-> data quality / repair
-> higher-timeframe aggregation
-> research dataset build
-> experiment / batch / campaign
-> walk-forward validation
-> robustness matrix
-> attribution and consistency checks
-> hypothesis / experiment plan
-> operator review
-> candidate proposal
-> candidate creation gate
-> research candidate
-> observation-only shadow evidence
-> qualification and dossier review
-> explicit human decision
~~~

The lifecycle is deliberately separate from the execution pipeline:

~~~text
market event -> signal -> risk decision -> order intent -> execution state
~~~

Research may inspect or evaluate the same strategy and risk definitions, but it
must not create or mutate:

- paper orders, fills, positions, equity, or journal state;
- exchange testnet orders, lifecycle events, reconciliation, or repair state;
- testnet shadow-promotion submission state; or
- any live execution state.

## Data preparation

Start by checking coverage for each requested symbol, interval, and time window.
The dataset build path computes the expected UTC-aligned candle grid, identifies
missing ranges, backfills missing 1-minute candles from public Binance REST,
derives supported higher intervals from stored 1-minute candles, validates the
result, and persists build history.

Data preparation is idempotent at the candle and build-step boundaries. A
current incomplete candle is not treated as a missing closed candle. Research
readiness should be checked before running an experiment or walk-forward job.

Useful surfaces:

- API: GET and POST routes under /research/data
- CLI: aegis research data
- Dashboard: research data coverage and build controls

See the [usage guide](USAGE.md) for command forms and the
[architecture](ARCHITECTURE.md) for persistence details.

## Experiments and validation

Research experiments evaluate explicit strategy configurations over prepared
stored candles. Backtests and replay use decimal arithmetic for prices, money,
fees, slippage, and PnL. Walk-forward validation separates training and
out-of-sample windows so a strategy can be evaluated across multiple periods.
Robustness and attribution outputs are evidence for review, not approval.

The strategy engine currently recognizes these deterministic strategy IDs:

- momentum_v1
- volatility_breakout_v1
- trend_filter_momentum_v1
- trend_filter_momentum_v2
- volatility_breakout_v2
- volatility_compression_breakout_v1
- range_reversion_v1
- trend_pullback_continuation_v1
- failed_breakdown_reclaim_v1

The set is an implementation detail and may change as strategies are added or
removed. A strategy ID is not a recommendation.

## Candidate proposals and gates

A candidate proposal is a review package assembled from evidence. It is not an
execution configuration. Candidate creation uses a conservative gate and
persists lifecycle events so an operator can distinguish proposed, accepted,
rejected, archived, and reopened work.

The gate may preserve warnings such as:

- evidence covering one symbol or market regime only;
- limited or unavailable final holdout data;
- weak cross-symbol generalization;
- too few observations; or
- degraded data quality.

An accepted candidate means approved for the next research observation step. It
does not mean approved for paper, testnet, or live execution.

## Shadow evidence semantics

Shadow observation is a no-submit research path:

- SHADOW_OBSERVATION_ONLY=true is required by the server-side guard;
- false or missing guard state fails closed for candidate shadow observation;
- the CLI flag is an operator acknowledgement, not a server-side override;
- one run evaluates at most one newest closed candle for a candidate;
- duplicate same-candle runs are operational checks, not independent evidence;
- NO_SIGNAL is a valid observation but does not qualify a candidate;
- WOULD_SUBMIT is qualifying evidence for the observation threshold; and
- stale-feed skips are infrastructure skips, not alpha evidence.

Shadow observation writes isolated evidence rows. It does not create paper
positions, paper fills, exchange testnet orders, lifecycle events, or live
orders.

The shadow runner stores a singleton timeframe for configured
strategy/symbol pairs. Candidate-specific promotion therefore previews the
current configuration, proposed configuration, structured diff, blockers,
warnings, and recommendation before applying a reviewed configuration change.
Applying that change does not start the runner or submit orders.

## Scheduled research

The scheduled runner is disabled by default. The safe bootstrap contains
monitoring-oriented jobs such as provider health, aggregation status, market-data
quality, and operator reporting. It does not create candidates or execution
state.

Candidate-specific CANDIDATE_SHADOW_OBSERVE_ONCE jobs are created manually for
an already reviewed candidate and are intentionally excluded from the safe
bootstrap. CROSS_ASSET_MARKET_DATA_REFRESH is also manual because it writes
research data-build artifacts even though it uses public data only.

Scheduled research should be enabled only after:

1. the database migration ledger is healthy;
2. market-data coverage and provider health are understood;
3. SHADOW_OBSERVATION_ONLY is explicitly set for shadow observation;
4. the candidate and runner configuration have been reviewed; and
5. read-only validation confirms the expected execution-safety counts.

## Experiment plan runner

Experiment-plan preview is auditable rather than invisible. A preview persists
plan-run history and returns the artifact type, warnings, blockers, and
recommendation. It does not create the downstream strategy experiment, batch,
campaign, robustness, walk-forward, or candidate artifact.

Confirmed plan execution requires exact confirmation in the form
RUN RESEARCH PLAN <plan-id>. A successful run creates only the explicit research
artifact for the selected plan type and records completion history. It still
does not mutate paper, shadow, testnet, or live execution tables.

## Regime calibration precedence

Regime discovery accepts an inline classifier configuration and/or a saved
calibration identifier. The deterministic precedence rule is:

1. use classifier_config when it is present;
2. otherwise load recommended_config from calibration_id when it is present;
3. otherwise use the default classifier configuration.

This makes ad hoc threshold experiments explicit while allowing a reviewed
calibration to be reused by identifier.

## Evidence exports

The repository includes
[a sample evidence bundle](examples/research-candidate-evidence-bundle.json).
It illustrates the persisted data shape and contains historical development
data. It is not a live deployment credential, API response, or recommendation.
Do not add real tokens, private URLs, account identifiers, or unreleased
production data to exported examples.
