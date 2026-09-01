# Usage Guide

This guide describes the supported operator paths after the local stack is running. For installation and service startup, begin with the [root README](../README.md).

## Operating modes

| Mode | Purpose | External order submission |
| --- | --- | --- |
| Research and replay | Build datasets, evaluate deterministic strategies, compare configurations, and collect evidence | Never |
| Paper | Exercise the persisted signal -> risk -> order -> accounting path with simulated fills | Never |
| Shadow | Record whether a candidate would have submitted under current data and risk context | Never |
| Testnet | Exercise the isolated Binance Spot Testnet adapter and lifecycle/reconciliation code | Only through explicit, authorized testnet actions |

Research plan runs, candidate lifecycle decisions, readiness checks, reports, analytics, and shadow observation are decision-support or evidence paths. They do not implicitly promote or execute anything.

## CLI

The operator binary is named aegis:

~~~bash
cargo run -p cli -- --help
~~~

After building, the same command can be run as ./target/debug/aegis or an installed release binary. Use --json on supported commands when a stable machine-readable response is needed.

| Area | Useful commands |
| --- | --- |
| Service state | aegis status, aegis metrics |
| Authentication | aegis auth login, aegis auth refresh, aegis auth me, aegis auth logout |
| Database | aegis db migrations status, aegis db migrations migrate, aegis db migrations baseline |
| Market data | aegis market provider-health, backfill, aggregate-candles, candle-coverage, candle-quality, repair-plan, repair-run |
| Strategies | aegis strategy list, enable, disable, config, dry-run, diagnostics |
| Risk and controls | aegis risk config, risk decisions, kill, resume, readiness check |
| Paper | aegis pipeline run, aegis paper account, positions, mark, close, pnl, equity, journal |
| Replay | aegis backtest run, list, get |
| Research | aegis research data, campaigns, batches, hypotheses, experiment-plans, candidates, scheduled-jobs |
| Testnet | aegis exchange testnet status, shadow-run, pipeline-preview, pipeline-submit, reconcile |
| Reports | aegis reports operator daily, aegis reports operator list |

Run the command-specific help before using a mutating command:

~~~bash
cargo run -p cli -- market --help
cargo run -p cli -- research --help
cargo run -p cli -- exchange testnet --help
~~~

## Market data workflow

Market ingestion and backfill use public Binance endpoints. They do not require Binance credentials.

~~~bash
# Check provider reachability
cargo run -p cli -- market provider-health --provider binance

# Backfill closed 1m candles
cargo run -p cli -- market backfill \
  --symbol BTCUSDT --timeframe 1m \
  --start 2026-05-01T00:00:00Z --end 2026-05-02T00:00:00Z

# Derive higher timeframes from stored 1m candles
cargo run -p cli -- market aggregate-candles \
  --symbol BTCUSDT --source 1m --target 5m \
  --start 2026-05-01T00:00:00Z --end 2026-05-02T00:00:00Z

# Inspect coverage and quality
cargo run -p cli -- market candle-coverage --symbol BTCUSDT
cargo run -p cli -- market candle-quality --symbol BTCUSDT --interval 1m --start 2026-05-01T00:00:00Z --end 2026-05-02T00:00:00Z
~~~

The backfill path stores run metadata and returns structured provider diagnostics when a request fails. Aggregation uses UTC-aligned buckets and only closed 1m source candles.

## Replay, experiments, and validation

Replay reads stored closed candles and persists results in isolated backtest or research tables:

~~~bash
cargo run -p cli -- backtest run \
  --strategy momentum_v1 --symbol BTCUSDT --timeframe 1m \
  --start 2026-05-01T00:00:00Z --end 2026-05-02T00:00:00Z \
  --initial-capital 1000000 --fee-bps 10 --slippage-bps 5 \
  --holding-candles 3

cargo run -p cli -- experiments strategy run \
  --strategy momentum_v1 --symbol BTCUSDT --timeframe 1m \
  --start 2026-05-01T00:00:00Z --end 2026-05-02T00:00:00Z \
  --initial-capital 1000000 --fee-bps 10 --slippage-bps 5 \
  --lookbacks 3,5,10 --holding-candles 3,5 --max-runs 6

cargo run -p cli -- experiments strategy walk-forward \
  --strategy momentum_v1 --symbol BTCUSDT --timeframe 15m \
  --start 2026-05-01T00:00:00Z --end 2026-05-24T00:00:00Z \
  --train-hours 72 --test-hours 24 --step-hours 24 \
  --initial-capital 1000000 --fee-bps 10 --slippage-bps 5 \
  --lookback-candles 5 --holding-candles 3
~~~

Experiments and walk-forward runs are research evidence. They do not update active strategy configuration or create paper, shadow, testnet, or live execution rows.

## Research lifecycle

The research workflow is intentionally explicit:

1. Check data coverage and build missing datasets.
2. Run an experiment, campaign, robustness matrix, or walk-forward evaluation.
3. Inspect diagnostics, attribution, and evidence quality.
4. Create or import a research candidate.
5. Review the candidate and collect observation-only shadow evidence.
6. Evaluate qualification and inspect the testnet review dossier.
7. Require a separate human decision before any isolated testnet action.

Useful inspection commands:

~~~bash
cargo run -p cli -- research data coverage
cargo run -p cli -- research campaigns list
cargo run -p cli -- research experiment-plans list
cargo run -p cli -- research candidates list --limit 20
cargo run -p cli -- research candidates qualification <candidate-id>
cargo run -p cli -- research candidates shadow-performance <candidate-id>
cargo run -p cli -- research candidates testnet-review-dossier <candidate-id>
~~~

Candidate observation is distinct from testnet promotion:

~~~bash
cargo run -p cli -- research candidates shadow-observe-once <candidate-id>
~~~

The observation path is guarded by SHADOW_OBSERVATION_ONLY=true, evaluates at most one independent observation for a new closed candle, and does not submit an exchange order. Duplicate same-candle checks are operational duplicates, not independent evidence.

For the full lifecycle, thresholds, and import/export semantics, see [Research Workflows](RESEARCH.md).

## Paper execution

Paper execution uses the same broad state sequence as the intended guarded execution model, but fills are simulated and persisted separately from testnet state:

~~~bash
cargo run -p cli -- readiness check \
  --target PAPER_PIPELINE --symbol BTCUSDT \
  --strategy momentum_v1 --timeframe 1m

cargo run -p cli -- pipeline run \
  --strategy momentum_v1 --symbol BTCUSDT --timeframe 1m

cargo run -p cli -- paper account
cargo run -p cli -- paper positions
cargo run -p cli -- paper mark
~~~

A simulated full-position close requires the exact confirmation CLOSE <SYMBOL>:

~~~bash
cargo run -p cli -- paper close <position-id> --confirm "CLOSE BTCUSDT"
~~~

The pipeline can stop on a disabled strategy, stale data, active kill switch, stale signal, or a rejected risk decision. Paper actions do not call exchange private endpoints.

## Testnet boundary

The repository contains an isolated Binance Spot Testnet adapter, not a production exchange adapter. Inspect its state and read the command-specific help first:

~~~bash
cargo run -p cli -- exchange testnet status
cargo run -p cli -- exchange testnet pipeline-preview --help
cargo run -p cli -- exchange testnet pipeline-submit --help
cargo run -p cli -- exchange testnet order-submit --help
cargo run -p cli -- exchange testnet reconcile --help
~~~

Testnet actions require backend-only testnet credentials, role checks, an inactive kill switch, an approved persisted risk decision where required, and exact typed confirmations. Direct submit and pipeline submit are testnet-only and must never be pointed at production endpoints. Shadow runs and research candidate observation never submit.

## API

The API is served by the api binary. Compose publishes it on host port 3100; a directly launched process defaults to port 3000.

Public or inspection-oriented examples:

~~~bash
curl --fail --silent http://127.0.0.1:3100/system/health
curl --fail --silent http://127.0.0.1:3100/system/status
curl --fail --silent http://127.0.0.1:3100/market/feed-status
curl --fail --silent http://127.0.0.1:3100/metrics
~~~

The API route groups are:

- system and auth: /system/*, /auth/*;
- events: /events/*;
- market data: /market/*;
- strategy and risk: /strategy/*, /risk/*, /readiness/*;
- paper and replay: /paper/*, /backtest/*, /experiments/*;
- research: /research/*;
- isolated testnet: /exchange/testnet/*;
- analytics and reports: /analytics/*, /reports/*;
- order inspection: /orders/*.

Protected routes use a Bearer access token unless auth is intentionally disabled for local development. The dashboard uses the same auth API and refresh-cookie flow; it does not receive testnet secrets.

## Dashboard

Run the dashboard in development with:

~~~bash
npm --prefix apps/dashboard ci
npm --prefix apps/dashboard run dev -- --hostname 127.0.0.1 --port 3001
~~~

The cockpit includes market data, strategies, risk, paper orders and accounting, research and experiments, analytics, reports, events, readiness, and isolated testnet inspection. It is an operator surface over the API, not a separate execution engine.
