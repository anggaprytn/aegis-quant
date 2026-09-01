# Product Requirements

## Document status

This document describes the implementation-aligned product scope for Aegis
Quant v0.1. It is a product boundary, not a promise of production readiness or
trading performance.

## Problem

Market experiments are difficult to trust when data coverage, signal
deduplication, risk decisions, order state, exchange state, and operator
actions cannot be reconstructed. A strategy can appear successful while the
surrounding system is using incomplete candles, stale prices, duplicated
events, inconsistent accounting, or unreviewed execution state.

Aegis Quant treats those concerns as an execution-infrastructure problem. The
system is intended to make each meaningful transition persisted, inspectable,
replayable, and bounded by explicit authorization.

## Product goal

Provide a deterministic, auditable control plane for:

1. ingesting and validating public market data;
2. running reproducible strategy research and replay;
3. exercising risk-gated paper execution;
4. inspecting an isolated Binance Spot Testnet path when an operator
   explicitly enables it; and
5. understanding health, state transitions, reconciliation, and recovery.

The product should help a technical operator answer what happened, why it
happened, what state was persisted, and which control authorized it.

## Intended users

- Developers building deterministic market-data, strategy, risk, and execution
  infrastructure.
- Operators testing paper or isolated testnet workflows with explicit controls.
- Researchers comparing evidence without granting research code execution
  authority.

The project is not intended to be a consumer trading application, a hosted
investment service, or a financial-advice product.

## Core invariant

All trade-like behavior must remain traceable through:

~~~text
market event -> signal -> risk decision -> order intent -> execution state
~~~

Strategy and research code must not submit orders directly. Any future advisory
or LLM component must remain outside execution authority.

## In scope for v0.1

### Market data and storage

- Binance public WebSocket trade ingestion.
- Public REST candle backfill.
- Deterministic closed-candle construction and higher-timeframe aggregation.
- Coverage, freshness, quality, provider diagnostics, and repair workflows.
- PostgreSQL persistence and migration checksums.

### Research and replay

- Deterministic strategy evaluation over stored candles.
- Replay/backtest with decimal money and price arithmetic, fees, slippage,
  trades, and equity results.
- Experiments, walk-forward validation, robustness analysis, attribution,
  hypotheses, experiment plans, candidates, qualification, and evidence
  exports.
- Observation-only shadow evidence that never submits an exchange order.

### Guarded execution and operations

- Persisted risk decisions and risk configuration audit history.
- Paper order lifecycle, fills, positions, journal, mark-to-market, PnL, and
  equity snapshots.
- Persistent kill switch and role-gated operator controls.
- Isolated Binance Spot Testnet order lifecycle, private-stream skeleton,
  reconciliation, repair, and typed confirmations.
- API, CLI, dashboard, reports, readiness checks, events, and metrics.

## Explicit non-goals

- Live trading or production exchange order submission.
- Production exchange private endpoints.
- Automatic promotion from research to paper or testnet.
- LLM-controlled execution.
- Multi-exchange routing, leverage, margin, or derivatives execution.
- A hosted service, managed secrets platform, or guaranteed availability.
- Claims of profitability, alpha, or suitability for real-money use.

## Product requirements

The implementation should preserve these requirements:

- Money, balances, notional, prices, fees, and PnL use decimal types rather than
  binary floating point.
- Kill-switch state is persistent and checked before guarded actions.
- Research, replay, paper, shadow, and testnet state remain isolated.
- Dangerous actions require the applicable role, an inactive kill switch,
  explicit typed confirmation, and an auditable result.
- State transitions are idempotent where retries are expected.
- Stale data, missing coverage, provider failure, and reconciliation mismatch
  are visible rather than silently repaired.
- The CLI and dashboard use the API as the operational boundary; they are not
  separate database or exchange implementations.
- Public documentation distinguishes implemented behavior, instance evidence,
  assumptions, and future work.

## Success criteria for this phase

An operator should be able to:

1. start the local Compose stack and apply migrations;
2. check health and authenticate an owner;
3. hydrate public candles and inspect their coverage and quality;
4. run a deterministic backtest or research workflow;
5. execute the paper path with persisted risk and accounting state;
6. inspect readiness, events, reports, and metrics;
7. keep shadow observation no-submit; and
8. deliberately inspect or perform an isolated testnet action only after
   command-specific authorization and confirmation.

These criteria describe workflow completeness, not production readiness.

## Current maturity and future work

The repository is an experimental, single-tenant v0.1 implementation. The next
work is operational hardening, integration coverage, deployment hygiene,
evidence reliability, and recovery drills. Live execution remains explicitly
deferred.

See the [roadmap](ROADMAP.md), [architecture](ARCHITECTURE.md), and
[development guide](DEVELOPMENT.md) for implementation detail.
