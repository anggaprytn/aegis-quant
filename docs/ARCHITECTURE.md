# Architecture

## Intent

Aegis Quant is deterministic execution infrastructure, not an AI trading bot. The control flow is explicit:

```txt
market event -> signal -> risk decision -> order intent -> execution state
```

LLM components are advisory only and do not have execution authority.

## Initial components

- `crates/core`: shared domain types and event envelope
- `crates/api`: operational health/status API
- `crates/events`: event taxonomy and publisher contract
- `crates/db`: database configuration and migrations
- `crates/market-ingest`: Binance public market data ingestion and deterministic candle boundary
- `crates/replay-engine`: deterministic historical candle replay and backtest simulation boundary
- `crates/strategy-engine`: deterministic signal generation boundary
- `crates/risk-engine`: risk gating boundary
- `crates/execution-engine`: paper execution lifecycle boundary
- `crates/exchange`: exchange adapter boundary, disabled for live execution in MVP
- `crates/llm-analyst`: advisory-only market commentary boundary
- `apps/dashboard`: Next.js operational cockpit for paper-only inspection and operator actions

## Data boundaries

- Money, price, balances, and PnL use `rust_decimal`
- Correlation IDs are required on events
- Auditable state changes should land in `system_events` or `audit_logs`
- Kill switch persistence is required and lives in the database
- Exchange execution state is isolated from internal paper execution state

## Market ingest flow

Phase 1 market data follows this path:

```txt
Binance public trade stream
-> parse trade payload into MarketTrade
-> persist tick into market_ticks
-> update market_feed_status
-> feed deterministic 1m CandleBuilder
-> upsert active/closed candles into candles
-> emit system_events for feed transitions, trades, and candle close
```

Historical candle hydration follows this parallel path:

```txt
Binance public REST klines
-> deterministic page planning over start_time/end_time
-> parse only final closed klines
-> idempotent candle upsert into candles
-> persist run metadata in candle_backfill_runs
-> emit market.backfill.* system events
```

Notes:

- Supported symbols are env-configured and uppercase in persistence/API responses.
- Candle building is deterministic for identical trade ordering.
- Out-of-order trades are rejected explicitly rather than rewriting historical candles.
- Replay/backtest reads the same `candles` table populated by both live WebSocket accumulation and historical REST backfill.
- The ingest boundary is public market data only. No API keys, private streams, or exchange execution are introduced here.

## Strategy evaluation flow

Current deterministic paper flow follows this path:

```txt
closed candles from Postgres
-> deterministic strategy evaluation
-> persisted signal or deduped existing signal
-> persisted risk_decision
-> order intent with deterministic idempotency key
-> paper order lifecycle
-> paper fill
-> paper position update
-> paper PnL and equity snapshot
```

Notes:

- Strategy evaluation reads stored candles only and ignores open candles.
- `momentum_v1` and `volatility_breakout_v1` are deterministic library strategies with explicit config.
- Strategy configs are validated before update, versioned in Postgres, and audited with the old config, new config, validation issues, actor placeholder, and correlation ID.
- Dry-run evaluation loads recent closed candles and executes the strategy without mutating `signals`, `risk_decisions`, `orders`, paper accounting tables, or backtest tables.
- Duplicate signals for the same strategy, symbol, timeframe, side, reason, and closed candle are deduped in Postgres.
- Every signal passed into the pipeline reaches an explicit `APPROVED` or `REJECTED` risk decision in `risk_decisions`.
- Risk rejection is machine-readable and emits `risk.approved` or `risk.rejected` system events.
- Strategy logic cannot submit orders directly. Paper orders are created only through the persisted approved `risk_decision_id`.
- Order idempotency is deterministic from `strategy_id + signal_id + risk_decision_id + symbol + side + source_candle_open_time`.
- Duplicate pipeline runs reuse the existing paper order instead of creating a second active order for the same idempotency key.
- If the strategy is disabled, the market feed is stale/degraded, the kill switch is active, or the signal is stale, the pipeline stops safely without creating a paper order.

## Paper accounting flow

Operational paper accounting is isolated from replay/backtest state:

```txt
paper order filled
-> paper_fills
-> paper_positions
-> paper_trade_journal
-> paper_accounts
-> paper_equity_snapshots
```

Notes:

- Only paper orders in `PAPER_FILLED` state create accounting artifacts.
- Spot long-only is the current MVP assumption: buy opens/increases a paper long position.
- Manual simulated close is full-position only in MVP and requires typed confirmation `CLOSE <SYMBOL>`.
- Manual close reads the latest stored public market tick and rejects missing/stale price data by default.
- Manual close persists a synthetic approved paper close decision and a simulated sell order record so fills, journal entries, and events remain auditable without introducing live execution.
- Missing price does not fabricate PnL; the position/account is marked with explicit missing or stale price state.
- Replay/backtest tables remain separate and must not be mutated by paper accounting.

Current paper open/close accounting path:

```txt
paper order filled
-> paper fill
-> paper long position open/update

manual simulated close request
-> typed confirmation check
-> latest public mark price freshness check
-> simulated close fill
-> position status CLOSED
-> realized PnL / account equity update
-> equity snapshot
-> trade journal
-> paper.position.close_requested / paper.fill.created / paper.position.closed / paper.equity.updated
```

## Replay and backtest flow

Replay/backtest follows this isolated path:

```txt
stored closed candles
-> deterministic strategy evaluation
-> simulated entry/exit decisions
-> simulated trades
-> equity curve
-> persisted backtest metrics
```

Notes:

- Replay reads only stored closed candles from Postgres for the requested symbol, timeframe, and time range.
- Replay/backtest uses the persisted validated strategy config by default, with an optional validated per-run override isolated to the backtest request payload.
- Strategy evaluation sees only candles available up to the replay point; no lookahead into future candles is allowed.
- Entries execute at the next candle open with fixed deterministic slippage and fee assumptions.
- Exits use deterministic TP/SL threshold checks or a fixed holding-candle fallback.
- Replay emits `replay.backtest.started`, `replay.backtest.completed`, and `replay.backtest.failed` into `system_events`.
- Replay persists only into `backtest_runs`, `backtest_trades`, and `backtest_equity_curve`.
- Replay must not mutate production `signals`, `risk_decisions`, or `orders`.

## Strategy analytics read model

Operator analytics is a read-only aggregation layer over the existing isolated persistence:

```txt
backtest_runs
+ paper_positions / paper_equity_snapshots / orders
+ testnet_shadow_runs
+ signals / risk_decisions
-> read-only SQL aggregation helpers
-> /analytics/strategy/*
-> dashboard cards/tables + CLI inspection
```

Notes:

- Analytics never writes derived rows back into Postgres.
- Backtest metrics stay sourced from `backtest_runs`.
- Paper metrics stay sourced from paper accounting tables and paper orders only.
- Shadow metrics stay sourced from `testnet_shadow_runs` only.
- Promotion funnel metrics stay sourced from `testnet_shadow_runs`, `testnet_shadow_promotions`, `exchange_testnet_orders`, and `exchange_testnet_order_lifecycle_events` only.
- Combined summaries are assembled in memory from bounded per-mode reads; they are not a new execution source of truth.
- The analytics API is inspection-only and must never trigger exchange submission, paper pipeline execution, backtests, repair actions, or reconciliation.

## Deployment shape

For MVP local development:

- One Axum API process
- One market-ingest process
- One Next.js dashboard process
- One PostgreSQL instance
- Docker Compose orchestration

No Kubernetes, no microservice decomposition, and no paid infrastructure assumptions are introduced in this foundation.

## Exchange adapter boundary

The exchange crate now models a testnet-only private execution boundary:

```txt
approved risk_decision_id
-> preview with fresh local tick/candle
-> operator review
-> owner-confirmed testnet order request
-> exchange adapter trait
-> Binance Spot Testnet REST boundary
-> isolated exchange_testnet_orders persistence
-> audit_logs + system_events
```

Notes:

- `ExchangeEnvironment::Live` is hard-rejected in core validation and adapter config checks.
- Binance Spot Testnet uses only `https://testnet.binance.vision`.
- `POST /exchange/testnet/pipeline/preview` is an operator-visible dry run only: it requires an existing approved `risk_decision_id`, blocks on the persistent kill switch, requires fresh local price context from stored tick/candle data, and must not create `exchange_testnet_orders` or lifecycle rows.
- `POST /exchange/testnet/pipeline/submit` is owner-only: it revalidates the preview boundary, requires exact typed confirmation `SUBMIT TESTNET <SYMBOL>`, persists only isolated testnet-order state, and must never touch paper or backtest tables.
- `POST /exchange/testnet/shadow/run` is operator-visible shadow execution only: it runs strategy -> signal -> risk -> local-price resolution -> would-submit intent, persists only `testnet_shadow_runs`, and must never submit to Binance or create isolated lifecycle rows.
- `POST /exchange/testnet/shadow/promotions/preview` is the manual bridge from a persisted `WOULD_SUBMIT` shadow run into a gated testnet promotion record: it requires an existing approved `risk_decision_id`, an inactive kill switch, an enabled strategy config, fresh local pricing, persists only `testnet_shadow_promotions`, and must not create `exchange_testnet_orders` or lifecycle rows.
- `POST /exchange/testnet/shadow/promotions/:id/submit` is owner-only: it requires exact typed confirmation `PROMOTE TESTNET <SYMBOL>`, revalidates kill switch plus risk approval, submits exactly the persisted promotion payload, persists only isolated testnet execution state, and must never recompute strategy or mutate paper/backtest/live tables.
- `GET/POST /exchange/testnet/shadow-runner/*` manages a persistent no-submit scheduler: config/state live in singleton Postgres tables, `RUN_ONCE` reuses the same shadow path, scheduled ticks never submit, and only `testnet_shadow_runs` plus runner config/state are mutated.
- Private testnet orders do not mutate `orders`, `paper_positions`, `paper_fills`, or paper PnL tables.
- The adapter now also manages Spot Testnet listen-key lifecycle and testnet-only user-data stream URL construction.
- Reconciliation runs against isolated `exchange_testnet_orders`, persists `exchange_reconciliation_runs` plus `exchange_reconciliation_mismatches`, and updates local testnet status only through safe exchange-to-local mappings.
- Manual repair actions persist separately in `exchange_testnet_repair_actions`, remain testnet-only, and must be operator-triggered one command at a time.
- Unknown exchange states or missing exchange orders emit explicit mismatch events and remain operator-visible; they do not automatically toggle the global kill switch.
- Private user-data events persist into `exchange_private_stream_events`, stream connectivity persists into `exchange_private_stream_state`, and normalized `executionReport` handling updates only the isolated `exchange_testnet_orders` table.
- Private stream confirmation stops at isolated testnet order status. It must not auto-bridge into paper orders, paper positions, paper PnL, replay tables, or any live execution path.

Testnet execution lifecycle flow:

```txt
local submit intent
-> ORDER_SUBMIT_REQUESTED
-> exchange REST ACK
-> EXCHANGE_ACKED
-> private executionReport / REST reconciliation
-> NEW / PARTIALLY_FILLED / FILLED / CANCELLED / REJECTED / EXPIRED
-> invalid or unknown exchange evidence
-> RECONCILIATION_REQUIRED or UNKNOWN_EXCHANGE_STATE
```

Rules:

- `EXCHANGE_ACKED` is not a fill.
- `FILLED`, `CANCELLED`, `REJECTED`, and `EXPIRED` are terminal.
- Private stream and REST reconciliation share the same transition validator.
- Preview audit/system events are allowed, but preview must remain non-submitting and non-persistent with respect to exchange order lifecycle state.
- Every accepted transition appends an event into `exchange_testnet_order_lifecycle_events`.
- Repair controls may only touch isolated `exchange_testnet_orders`, `exchange_testnet_order_lifecycle_events`, and `exchange_testnet_repair_actions`.
- `MANUAL_RECHECK` uses the shared REST reconciliation validator; explicit mark actions use a dedicated repair validator and never reactivate a terminal `FILLED` order.
- `SAFE_CANCEL_REQUEST` is Binance Spot Testnet only and must never call live Binance endpoints.

Testnet shadow flow:

```txt
closed candles
-> strategy evaluation
-> optional deduped signal persistence
-> mandatory persisted risk decision
-> fresh local tick/candle price resolution
-> would-submit testnet intent
-> persisted testnet_shadow_runs row
```

Testnet shadow promotion flow:

```txt
persisted WOULD_SUBMIT testnet_shadow_runs row
-> operator preview request
-> current kill switch + risk approval + strategy enabled + fresh local price checks
-> persisted testnet_shadow_promotions row
-> owner typed confirmation
-> isolated exchange_testnet_orders row
-> ORDER_SUBMIT_REQUESTED
-> EXCHANGE_ACKED on adapter success
```

Promotion funnel read model:

```txt
testnet_shadow_runs (WOULD_SUBMIT only)
-> optional testnet_shadow_promotions row
-> optional isolated exchange_testnet_orders row
-> optional exchange_testnet_order_lifecycle_events history
-> read-only SQL join + bounded API/CLI/dashboard views
```

Notes:

- The funnel is observational only; it does not create promotion rows, submit testnet orders, append lifecycle events, or update reconciliation state.
- Missing linked order rows are tolerated and surfaced as incomplete analytics rows rather than treated as executable repair actions.

Testnet shadow runner flow:

```txt
persisted testnet_shadow_runner_config + state
-> testnet-shadow-runner daemon loop or manual RUN_ONCE control
-> bounded strategy x symbol batch
-> shared POST /exchange/testnet/shadow/run execution path
-> persisted testnet_shadow_runs rows
-> updated testnet_shadow_runner_state
-> system_events + metrics
```

Rules:

- Scheduled ticks no-op when config is disabled or persisted status is `STOPPED` or `PAUSED`.
- Manual `RUN_ONCE` is allowed even when the scheduler is disabled or stopped; it still remains strictly no-submit.
- Kill switch handling is per shadow run: the runner persists `SKIPPED_KILL_SWITCH` decisions rather than silently dropping configured pairs.
- Per-pair failures are recorded into runner state and system events without creating exchange testnet orders or lifecycle rows.

## Frontend cockpit overview

The dashboard is intentionally dense and operational:

- Sidebar sections: Command Center, Market Data, Strategies, Risk, Orders, Backtests, Logs / Events, Settings placeholder
- Settings now includes a minimal Testnet Exchange surface for status, symbols, balances, recent isolated testnet orders, and owner-gated submit/cancel controls
- Settings now includes typed-confirmation repair controls and isolated repair history for stuck testnet orders
- Settings now also includes manual testnet reconciliation, recent reconciliation runs, mismatch counts, and mismatch detail inspection
- Settings now includes private-stream status, recent private events, and operator listen-key lifecycle controls with a clear testnet-only warning
- Sticky header: mode, kill switch state, feed state, data age, daily PnL placeholder, API health
- Paper-only controls: kill switch activation, typed resume confirmation, strategy evaluation, paper pipeline run, and backtest run
- Read-only cockpit inspection: persisted risk decisions, enriched paper order detail, and filtered recent system events

Frontend constraints:

- No live trading controls
- No exchange private API or secret handling
- No chart-heavy UX in MVP
- Defensive rendering around backend errors and optional data shapes

## Auth flow

The operator auth MVP is intentionally local and single-tenant in shape:

```txt
bootstrap owner from env
-> Argon2id password hash in users
-> login validates password
-> create DB-backed session with hashed refresh token
-> issue short-lived JWT access token
-> dashboard/CLI send Bearer token
-> API middleware resolves AuthenticatedActor
-> mutating handlers write actor_id into audit/event payloads where practical
```

Notes:

- Roles are `OWNER`, `OPERATOR`, and `VIEWER`.
- `OWNER` controls risk config updates, strategy config updates, and kill-switch resume.
- Dashboard and CLI both use the same `/auth/*` API surface.
- `AEGIS_AUTH_DISABLED=true` bypasses login for local development by injecting a synthetic OWNER actor.

## Cockpit observability flow

The operational cockpit should expose persisted truth from the backend rather than reconstructing links in the browser:

```txt
signals
-> risk_decisions
-> orders
-> system_events
-> dashboard read APIs
```

Read API boundaries:

- `/risk/decisions` and `/risk/decisions/:id` expose persisted risk approvals and rejections, including notional, score, reasons, correlation, and linked signal metadata when available
- `/orders` and `/orders/:id` expose paper order inspection data enriched from the linked `risk_decision_id`, including truthful `signal_id` and `strategy_id`
- `/events/recent` exposes newest-first system events with optional server-side filters for `event_type`, `source`, and `correlation_id`

Operational intent:

- The dashboard should show what the database recorded, not best-effort guesses from correlation IDs alone
- Risk rejection review is read-only and does not mutate risk state
- Event inspection remains append-only and filterable for operator triage

## Telemetry flow

Operational metrics are exposed from the API at `GET /metrics` in Prometheus text format.

The telemetry design has two layers:

```txt
event-time instrumentation
-> counters / histograms updated when deterministic actions complete

scrape-time snapshot
-> lightweight current-state queries
-> gauges refreshed just before metrics exposition
```

Event-time coverage:

- API request count and duration
- persisted market ticks
- closed candles produced by the deterministic candle builder
- strategy evaluations and generated signals
- persisted risk decisions and rejections
- completed paper pipeline outcomes
- created/reused paper orders
- completed backfill runs and candle totals
- completed backtest runs, durations, and trade counts

Scrape-time gauge coverage:

- kill switch active state
- DB health state
- market feed status and last-event age
- open paper positions by symbol
- paper equity, realized PnL, and unrealized PnL
- paper position closes by symbol/result and paper fills by symbol/side

Constraints:

- Route labels use bounded template paths like `/orders/:id`, not raw IDs.
- Metrics do not include idempotency keys, correlation IDs, UUIDs, or raw user input as labels.
- Scrape-time queries target current-state tables only and avoid historical scans.
- `/metrics` is currently unauthenticated and should be isolated at the network boundary in production.

## CLI fallback

`crates/cli` adds an operator-local fallback path for the same paper-only operational surface area when the Next.js dashboard is unavailable.

Design constraints:

- The binary name is `aegis`
- It uses the existing HTTP API only and does not connect to Postgres directly
- It reads `AEGIS_API_BASE_URL` with fallback to `http://127.0.0.1:3000`
- It does not add live trading, exchange private API usage, API key handling, auth bypasses, or a TUI layer
- Dangerous actions remain bounded by the same backend rules as the dashboard

Supported control and inspection flow:

- `aegis status` aggregates `/system/health`, `/system/status`, `/risk/status`, and `/market/feed-status`
- `aegis kill` and `aegis resume --confirm "RESUME TRADING"` call the existing risk endpoints and preserve typed confirmation
- `aegis pipeline run`, `strategy list|enable|disable`, `orders list|get`, `events list`, `risk decisions`, and `backtest run|list|get` map directly onto the existing read and paper-only control APIs
- `aegis paper account|positions|close|pnl|equity|journal|mark` maps directly onto the paper accounting HTTP APIs

Operational intent:

- Dashboard and CLI are parallel operator surfaces over the same API truth
- The CLI is a fallback for local inspection and safe paper-only intervention, not a separate execution path
- Output stays compact by default, with optional `--json` when operators need exact payloads
