# PRD: Aegis Quant Execution Infrastructure

# PRD v9.5

### Rust-first, secure, operational FE, AI-assisted execution system

Base dari PRD sebelumnya, tapi ini versi lebih serius, lebih production-grade, dan lebih realistis buat solo builder dengan modal awal kecil.


---

# 1. Product Summary

## Product Name

**Aegis Quant Execution Infrastructure**

Nama alternatif:

* **Aegis Trade Core**
* **Vanta Quant**
* **Sentinel Execution Engine**
* **QuantOps Core**

Rekomendasi: **Aegis Quant**.

Kenapa? Karena produk ini bukan "AI trading bot". Ini sistem penjaga eksekusi, risiko, state, dan operasi trading.


---

# 2. One-liner

**Aegis Quant is a Rust-based autonomous trading infrastructure that ingests real-time market data, evaluates deterministic trading strategies, uses LLMs for market regime reasoning, and executes trades under strict risk, security, and operational constraints.**

Versi CV/portfolio:

> Built a Rust-based autonomous execution infrastructure for crypto and equities, designed around deterministic order handling, risk-gated execution, market data ingestion, state reconciliation, and operational observability under volatile market conditions.


---

# 3. Brutal Product Thesis

Target kamu bukan bikin "Jane Street retail".

Target realistis:

> Build execution-grade infra that behaves like a disciplined micro hedge fund operator, not a degenerate signal bot.

Alpha itu susah, rapuh, dan cepat mati.

Yang bisa kamu kontrol:

* data integrity
* order correctness
* risk limits
* duplicate order prevention
* state reconciliation
* latency visibility
* slippage tracking
* auditability
* replayability
* security

Kalau sistem ini belum bisa paper trade dengan konsisten selama 30 hari, jangan live trade.


---

# 4. Product Goal

## Primary Goal

Membangun sistem trading autonomous yang:

* ingest data market real-time
* simpan data historis
* generate signal deterministic
* validasi semua trade lewat risk engine
* execute order via exchange API
* monitor posisi, PnL, latency, dan risk
* support kill switch
* support paper trading dan live trading
* menggunakan LLM hanya sebagai reasoning/meta layer, bukan decision maker utama


---

# 5. Non-Goals

Untuk v9.5, sistem **tidak** mengejar:

* HFT
* market making serius
* options trading
* high leverage
* GPU training
* deep reinforcement learning
* GPT auto-buy/sell tanpa guardrail
* multi-exchange arbitrage live
* public SaaS multi-user
* Kubernetes
* complex frontend
* prediction fantasy

Kalau kamu lompat ke situ terlalu cepat, project ini bakal jadi toy yang kelihatan canggih tapi operasionalnya busuk.


---

# 6. Target Market

## Phase 1

Internal use only.

User:

* kamu sebagai operator
* kamu sebagai quant researcher
* kamu sebagai backend engineer
* kamu sebagai risk manager

## Phase 2

Potential externalization:

* solo quant trader
* prop trading learner
* crypto systematic trader
* execution infra showcase
* open-source credibility artifact

## Phase 3

Possible product direction:

* retail execution middleware
* systematic trading infra toolkit
* risk engine as API
* autonomous trading observability platform


---

# 7. Supported Markets

## Initial Market

**Crypto spot only.**

Start with:

* BTC/USDT
* ETH/USDT
* SOL/USDT

Avoid early:

* futures
* leverage
* illiquid altcoins
* meme coins
* options
* equities

## Why crypto first

* free WebSocket data
* 24/7 market
* easier exchange API access
* smaller capital requirement
* faster feedback loop
* simpler account setup


---

# 8. Capital Constraint

## Initial Capital

Rp2 juta.

This is not hedge fund capital.

This is **infrastructure validation capital**.

Use it to test:

* order correctness
* exchange latency
* slippage
* fee impact
* recovery behavior
* kill switch
* reconciliation
* logging accuracy

Not to get rich.

## Trading Constraints

For Rp2 juta:

```txt
Max capital deployed per trade: 5% to 10%
Max daily loss: 2%
Max weekly loss: 5%
Max open positions: 1 to 2
Max leverage: 1x only
Min observation period before live trading: 30 days paper trading
```

Hard truth: with Rp2 juta, fee and slippage can eat you alive. So your first edge is not profit. It is operational correctness.


---

# 9. Product Principles

## 9.1 Deterministic First

Every trade must be explainable by:

```txt
market event -> signal -> risk decision -> order intent -> exchange order -> fill -> position update
```

No magical black box.

## 9.2 Risk Before Alpha

The risk engine can veto everything.

Strategy engine never directly sends orders.

## 9.3 LLM Never Has Final Authority

LLM can:

* summarize
* classify regime
* detect anomaly
* suggest strategy mode
* explain drawdown
* produce risk commentary

LLM cannot:

* bypass risk engine
* place raw orders
* change max loss silently
* change API keys
* disable kill switch
* increase leverage
* modify live strategy config without approval

## 9.4 Every State Change Is Audited

All critical actions must produce event logs:

* login
* config change
* strategy enable/disable
* kill switch
* order submit
* order cancel
* risk reject
* exchange reconnect
* reconciliation mismatch

## 9.5 Operational FE, Not Pretty FE

Frontend exists to operate the system.

It is not a fintech landing page.


---

# 10. High-Level Architecture

```txt
                        ┌────────────────────────┐
                        │ Operational Frontend   │
                        │ Next.js Dashboard      │
                        └───────────┬────────────┘
                                    │
                            HTTPS / WSS
                                    │
                        ┌───────────▼────────────┐
                        │ API Gateway             │
                        │ Rust + Axum             │
                        └───────────┬────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        │                           │                           │
┌───────▼────────┐          ┌───────▼────────┐          ┌───────▼────────┐
│ Market Ingest  │          │ Strategy Core  │          │ Risk Engine    │
│ Rust Service   │          │ Rust Service   │          │ Rust Service   │
└───────┬────────┘          └───────┬────────┘          └───────┬────────┘
        │                           │                           │
        │                           │                           │
┌───────▼───────────────────────────▼───────────────────────────▼────────┐
│ Event Bus / Internal Event Layer                                       │
│ NATS optional in later phase, Postgres event table for MVP             │
└───────┬───────────────────────────┬───────────────────────────┬────────┘
        │                           │                           │
┌───────▼────────┐          ┌───────▼────────┐          ┌───────▼────────┐
│ Data Store     │          │ Execution Core │          │ LLM Analyst    │
│ PostgreSQL     │          │ Rust Service   │          │ Controlled AI  │
└────────────────┘          └───────┬────────┘          └────────────────┘
                                    │
                          ┌─────────▼─────────┐
                          │ Exchange Adapter  │
                          │ Binance / Bybit   │
                          └───────────────────┘
```


---

# 11. Recommended Stack

## Backend

| Layer | Recommendation |
|-------|----------------|
| Language | Rust           |
| API Framework | Axum           |
| Async Runtime | Tokio          |
| DB    | PostgreSQL     |
| DB Access | SQLx           |
| Eventing MVP | PostgreSQL event log |
| Eventing Later | NATS           |
| Cache | Redis optional |
| Auth  | JWT access token + refresh token |
| Secrets | age/sops or encrypted env |
| Logging | tracing        |
| Metrics | Prometheus     |
| Dashboard Metrics | Grafana        |
| Deployment | Docker Compose |
| Reverse Proxy | Caddy or Nginx |
| TLS   | Caddy auto TLS or Cloudflare tunnel |

## Frontend

| Layer | Recommendation |
|-------|----------------|
| Framework | Next.js        |
| UI    | Tailwind + shadcn/ui |
| Charts | lightweight-charts |
| Data fetching | TanStack Query |
| Realtime | WebSocket      |
| Auth  | HTTP-only cookie |
| Hosting | Same VPS or Vercel if separated |

## Why Rust

Rust cocok karena sistem ini butuh:

* strict typing
* concurrency
* low runtime overhead
* deterministic service behavior
* safer memory model
* good async ecosystem
* strong CLI/service deployment story

Node bagus buat dashboard. Python bagus buat research. Tapi execution core jangan pakai Python kalau targetnya serius.


---

# 12. Service Breakdown

## 12.1 API Gateway

### Responsibility

* expose API to frontend
* authenticate users
* authorize operational actions
* expose live system state
* stream logs and metrics
* proxy commands into internal services

### Tech

```txt
Rust
Axum
Tokio
Tower middleware
JWT
SQLx
tracing
```

### Critical Endpoints

```txt
POST /auth/login
POST /auth/logout
POST /auth/refresh

GET /system/health
GET /system/status
GET /system/config

GET /market/symbols
GET /market/candles
GET /market/ticks/latest

GET /strategy/list
GET /strategy/:id/status
POST /strategy/:id/enable
POST /strategy/:id/disable
POST /strategy/:id/config

GET /risk/status
GET /risk/events
POST /risk/kill-switch
POST /risk/resume

GET /orders
GET /orders/:id
POST /orders/cancel

GET /positions
GET /pnl/daily
GET /pnl/history

GET /logs
GET /audit
```


---

## 12.2 Market Ingest Service

### Responsibility

* connect to exchange WebSocket
* ingest trades, candles, orderbook snapshots
* normalize exchange payloads
* persist market data
* publish market events
* handle reconnect
* detect stale feed

### Supported Data

```txt
trade ticks
OHLCV candles
best bid/ask
orderbook snapshot
orderbook delta later
funding rate later
exchange status later
```

### Initial Sources

* Binance Spot WebSocket
* Bybit public WebSocket

### Failure Handling

If data feed stale:

```txt
if last_market_event_age > threshold:
    pause strategy
    reject new trades
    emit risk.data_stale
```

### Events

```txt
market.trade.received
market.candle.closed
market.orderbook.snapshot
market.feed.connected
market.feed.disconnected
market.feed.stale
```


---

## 12.3 Strategy Engine

### Responsibility

* consume market data
* compute indicators
* generate signals
* assign confidence
* emit order intent
* never directly execute order

### Initial Strategies

## Strategy A: Volatility Breakout

```txt
IF price breaks above recent range
AND volume confirms
AND volatility expansion detected
THEN emit long intent
```

## Strategy B: Mean Reversion

```txt
IF price deviates from moving average
AND RSI extreme
AND spread acceptable
THEN emit revert intent
```

## Strategy C: Momentum Continuation

```txt
IF trend strength high
AND pullback shallow
AND volatility regime acceptable
THEN emit continuation intent
```

### Signal Output Schema

```json
{
  "signal_id": "uuid",
  "strategy_id": "vol_breakout_v1",
  "symbol": "BTCUSDT",
  "side": "BUY",
  "confidence": 0.71,
  "timeframe": "5m",
  "reason": "breakout_above_20_bar_high_with_volume_expansion",
  "suggested_notional": 100000,
  "stop_loss_pct": 0.8,
  "take_profit_pct": 1.5,
  "created_at": "timestamp"
}
```


---

## 12.4 Risk Engine

This is the most important component.

### Responsibility

* validate every order intent
* enforce position limits
* enforce daily loss limits
* prevent duplicate orders
* prevent stale signals
* prevent oversized trades
* prevent trading during degraded state
* enforce kill switch
* enforce cooldown
* reject invalid execution state

### Risk Rules

```txt
Max open positions: 2
Max position notional: configurable
Max daily loss: 2%
Max weekly loss: 5%
Max order age: 3 seconds to 30 seconds depending mode
Max slippage tolerance: 0.2% to 0.5%
Max consecutive losses: 3
Cooldown after loss: 15 minutes
No trade if data stale
No trade if exchange reconciliation mismatch
No trade if kill switch active
No trade if API latency exceeds threshold
No trade if spread too wide
No trade if symbol disabled
```

### Risk Decision Output

```json
{
  "risk_decision_id": "uuid",
  "signal_id": "uuid",
  "decision": "APPROVED",
  "approved_notional": 75000,
  "risk_score": 0.32,
  "reasons": [
    "within_daily_loss_limit",
    "no_open_position_conflict",
    "spread_acceptable"
  ],
  "created_at": "timestamp"
}
```

### Rejection Example

```json
{
  "decision": "REJECTED",
  "reasons": [
    "daily_loss_limit_reached",
    "kill_switch_active"
  ]
}
```


---

## 12.5 Execution Core

### Responsibility

* convert risk-approved intent into exchange order
* sign requests
* submit order
* track ack
* track fill
* handle cancel
* reconcile order state
* prevent duplicates
* store all execution states

### Execution State Machine

```txt
INTENT_CREATED
RISK_APPROVED
ORDER_PREPARED
ORDER_SUBMITTED
EXCHANGE_ACKED
PARTIALLY_FILLED
FILLED
CANCEL_REQUESTED
CANCELLED
REJECTED
EXPIRED
RECONCILIATION_REQUIRED
```

### Duplicate Order Protection

Every order must have:

```txt
client_order_id
idempotency_key
signal_id
risk_decision_id
strategy_id
symbol
side
notional
timestamp
```

Rule:

```txt
Never submit two active orders with the same idempotency key.
Never submit new order if previous state is unknown.
Never submit live order from stale signal.
```

### Exchange ACK Rule

Exchange ACK is not fill.

The system must wait for order status or private stream confirmation before treating position as updated.


---

## 12.6 Exchange Adapter

### Responsibility

* abstract Binance/Bybit differences
* normalize order types
* normalize balances
* normalize fills
* normalize errors
* enforce exchange-specific rate limits
* implement retry policy

### Interface

```rust
#[async_trait]
pub trait ExchangeAdapter {
    async fn get_balances(&self) -> Result<Vec<Balance>>;
    async fn get_positions(&self) -> Result<Vec<Position>>;
    async fn submit_order(&self, order: OrderRequest) -> Result<OrderAck>;
    async fn cancel_order(&self, order_id: OrderId) -> Result<CancelAck>;
    async fn get_order_status(&self, order_id: OrderId) -> Result<OrderStatus>;
    async fn stream_market_data(&self, symbols: Vec<Symbol>) -> Result<MarketStream>;
    async fn stream_private_events(&self) -> Result<PrivateEventStream>;
}
```


---

## 12.7 LLM Analyst

### Responsibility

LLM acts as:

* market regime classifier
* news summarizer
* anomaly explainer
* strategy mode recommender
* risk commentary generator
* post-trade analyst

LLM does **not** execute trades.

### Allowed LLM Outputs

```txt
REGIME_RISK_ON
REGIME_RISK_OFF
REGIME_CHOPPY
REGIME_HIGH_VOLATILITY
REGIME_LOW_LIQUIDITY
ANOMALY_DETECTED
NO_ACTION
```

### LLM Input

```txt
recent candles
volatility metrics
drawdown state
strategy performance
market news summary
spread/liquidity stats
```

### LLM Output Example

```json
{
  "regime": "REGIME_HIGH_VOLATILITY",
  "confidence": 0.77,
  "recommendation": "reduce_position_size",
  "risk_multiplier": 0.5,
  "explanation": "volatility expansion with unstable directionality"
}
```

### Guardrail

LLM output can only reduce risk or recommend mode change.

LLM cannot increase risk automatically in v9.5.

That is the sane move.


---

# 13. Operational Frontend

## FE Philosophy

Frontend is a cockpit.

Not a chart app. Not a landing page. Not a toy dashboard.

It must answer:

```txt
Is the system safe?
Is it trading?
Why did it trade?
What is the current risk?
Can I stop it instantly?
What broke?
```


---

## 13.1 Main Screens

### Screen 1: Command Center

Purpose: one-glance system state.

Must show:

* system status
* live/paper mode
* kill switch status
* active strategies
* current positions
* daily PnL
* daily drawdown
* API latency
* exchange connection status
* data freshness
* last order
* last risk rejection

Layout:

```txt
┌────────────────────────────────────────────┐
│ SYSTEM: LIVE / PAPER / PAUSED / KILLED     │
├────────────────────────────────────────────┤
│ PnL Today | Drawdown | Open Risk | Latency │
├────────────────────────────────────────────┤
│ Positions                                │
├────────────────────────────────────────────┤
│ Active Strategies                         │
├────────────────────────────────────────────┤
│ Latest Events / Alerts                    │
└────────────────────────────────────────────┘
```


---

### Screen 2: Positions

Must show:

* symbol
* side
* quantity
* entry
* mark price
* unrealized PnL
* realized PnL
* stop loss
* take profit
* strategy source
* age
* close button

Manual close must require confirmation.

For live mode:

```txt
Type CLOSE BTCUSDT to confirm.
```


---

### Screen 3: Orders

Must show:

* order id
* client order id
* exchange order id
* symbol
* side
* type
* status
* submitted price
* average fill price
* filled quantity
* created at
* updated at
* source strategy
* risk decision id

Actions:

* cancel order
* inspect order lifecycle
* inspect exchange payload


---

### Screen 4: Strategy Control

Must show:

* strategy list
* enabled/disabled
* paper/live permission
* current config
* recent signals
* hit rate
* average return
* drawdown
* rejection rate
* last signal reason

Actions:

* enable strategy
* disable strategy
* edit config
* dry-run config
* view backtest result

Critical:

Live strategy config change should be audited.


---

### Screen 5: Risk Console

Most important screen.

Must show:

* kill switch
* daily loss limit
* weekly loss limit
* max position
* max open positions
* current exposure
* stale data status
* exchange mismatch status
* consecutive losses
* risk rejections

Actions:

* activate kill switch
* resume trading
* reduce risk mode
* disable all strategies
* force reconciliation

Do not allow casual resume.

Resume should require:

```txt
Type RESUME TRADING
```


---

### Screen 6: Market Data

Must show:

* latest ticks
* candles
* spread
* volume
* feed status
* last event age
* dropped events
* reconnect count

Charts are optional.

Data freshness is mandatory.


---

### Screen 7: Logs & Audit

Must show:

* event stream
* risk events
* order lifecycle
* auth events
* config changes
* errors
* warnings

Filter by:

* symbol
* strategy
* severity
* event type
* date range


---

### Screen 8: LLM Analyst

Must show:

* current regime
* LLM confidence
* latest reasoning
* risk multiplier recommendation
* anomaly notes
* news summary
* whether LLM output affected strategy mode

Important:

Always display:

```txt
LLM has no execution authority.
```


---

# 14. Security Requirements

## 14.1 Authentication

Required:

* login with email/password
* password hashing with Argon2id
* HTTP-only secure cookies
* access token short TTL
* refresh token rotation
* logout invalidates refresh token
* optional TOTP for live trading mode

No localStorage token.

That is amateur hour.


---

## 14.2 Authorization

Roles:

```txt
OWNER
OPERATOR
VIEWER
```

For MVP, just OWNER is fine.

But still model RBAC from day one.

Permission examples:

```txt
VIEWER:
- view dashboard
- view logs

OPERATOR:
- pause strategy
- cancel order
- trigger reconciliation

OWNER:
- enable live trading
- edit risk limits
- manage API keys
- resume from kill switch
```


---

## 14.3 API Key Security

Exchange API keys must be:

* never exposed to frontend
* never logged
* encrypted at rest
* scoped to trading only
* withdrawal disabled
* IP-restricted if exchange supports it
* rotated periodically
* separated between paper/testnet/live

Use environment variable for MVP.

Better later:

```txt
encrypted secrets table
age/sops
cloud KMS
HashiCorp Vault if overkill is acceptable
```

For your stage, use:

```txt
.env encrypted locally
server env injection
no key in repo
no key in logs
```


---

## 14.4 Live Trading Lock

Live mode must be hard to accidentally enable.

Required:

```txt
LIVE_TRADING_ENABLED=true
EXCHANGE_ENV=live
OWNER_TOTP_REQUIRED=true
MAX_LIVE_NOTIONAL configured
```

Frontend should show a persistent red/live state.

Backend must reject live orders unless live mode is explicitly enabled server-side.


---

## 14.5 Kill Switch

Kill switch must:

* stop new signals
* reject all new order intents
* optionally cancel open orders
* optionally close positions manually
* persist state in database
* survive service restart

Kill switch state must not live only in memory.


---

## 14.6 Network Security

MVP:

* VPS firewall
* expose only 80/443
* SSH key only
* disable password SSH
* fail2ban optional
* Caddy/Nginx reverse proxy
* TLS mandatory
* admin dashboard behind auth
* no public trading webhook

Better:

* Cloudflare Access
* allowlist your IP
* Tailscale private access
* dashboard not publicly exposed

Best early setup:

```txt
VPS + Docker Compose + Tailscale
Frontend accessible only over private network
```


---

## 14.7 Audit Logging

Every sensitive action logs:

```txt
actor_id
action
resource
old_value
new_value
ip_address
user_agent
timestamp
request_id
```

Sensitive actions:

* login
* failed login
* strategy enable
* live mode enable
* risk config update
* kill switch activate
* kill switch resume
* API key update
* manual order cancel
* manual position close


---

## 14.8 Rate Limiting

Apply rate limit to:

* login
* command endpoints
* strategy config update
* order cancel
* kill switch resume

Use Tower middleware or reverse proxy.


---

## 14.9 Supply Chain Security

Minimum:

```txt
cargo audit
cargo deny
npm audit
Docker image scan
pin dependency versions
no random exchange SDK unless reviewed
```

Prefer writing your own exchange adapter over trusting random unofficial SDKs for execution.


---

# 15. Data Model

## 15.1 Core Tables

```sql
users
sessions
api_keys
symbols
market_ticks
candles
signals
risk_decisions
orders
fills
positions
strategy_configs
strategy_state
risk_events
system_events
audit_logs
llm_regime_reports
exchange_reconciliation_runs
```


---

## 15.2 orders

```sql
CREATE TABLE orders (
    id UUID PRIMARY KEY,
    client_order_id TEXT NOT NULL UNIQUE,
    exchange_order_id TEXT,
    signal_id UUID,
    risk_decision_id UUID,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    order_type TEXT NOT NULL,
    status TEXT NOT NULL,
    requested_qty NUMERIC,
    requested_notional NUMERIC,
    limit_price NUMERIC,
    avg_fill_price NUMERIC,
    filled_qty NUMERIC DEFAULT 0,
    idempotency_key TEXT NOT NULL UNIQUE,
    exchange TEXT NOT NULL,
    mode TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
```


---

## 15.3 risk_decisions

```sql
CREATE TABLE risk_decisions (
    id UUID PRIMARY KEY,
    signal_id UUID NOT NULL,
    decision TEXT NOT NULL,
    approved_notional NUMERIC,
    risk_score NUMERIC,
    reasons JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
```


---

## 15.4 audit_logs

```sql
CREATE TABLE audit_logs (
    id UUID PRIMARY KEY,
    actor_id UUID,
    action TEXT NOT NULL,
    resource TEXT NOT NULL,
    old_value JSONB,
    new_value JSONB,
    ip_address TEXT,
    user_agent TEXT,
    request_id TEXT,
    created_at TIMESTAMPTZ NOT NULL
);
```


---

# 16. Event Model

## Event Naming

```txt
market.trade.received
market.candle.closed
signal.generated
risk.approved
risk.rejected
order.intent.created
order.submitted
order.acked
order.partially_filled
order.filled
order.cancelled
position.opened
position.closed
system.kill_switch.enabled
system.kill_switch.disabled
exchange.feed.connected
exchange.feed.disconnected
exchange.reconciliation.started
exchange.reconciliation.mismatch
exchange.reconciliation.completed
llm.regime.updated
```

## Event Envelope

```json
{
  "event_id": "uuid",
  "event_type": "risk.rejected",
  "source": "risk-engine",
  "correlation_id": "uuid",
  "payload": {},
  "created_at": "timestamp"
}
```

Correlation ID is mandatory.

Without it, debugging order lifecycle becomes hell.


---

# 17. Rust Project Structure

```txt
aegis-quant/
  Cargo.toml
  crates/
    core/
      src/
        types.rs
        errors.rs
        money.rs
        time.rs
    api/
      src/
        main.rs
        routes/
        middleware/
        auth/
    market-ingest/
      src/
        main.rs
        binance.rs
        bybit.rs
        normalizer.rs
    strategy-engine/
      src/
        main.rs
        strategies/
        indicators/
    risk-engine/
      src/
        main.rs
        rules/
        evaluator.rs
    execution-engine/
      src/
        main.rs
        order_manager.rs
        state_machine.rs
    exchange/
      src/
        mod.rs
        binance.rs
        bybit.rs
        signing.rs
    llm-analyst/
      src/
        main.rs
        regime.rs
        prompt_guard.rs
    db/
      src/
        queries/
        migrations/
    events/
      src/
        envelope.rs
        publisher.rs
        subscriber.rs
  apps/
    dashboard/
      package.json
      app/
      components/
      lib/
  infra/
    docker-compose.yml
    Dockerfile.api
    Dockerfile.service
    Caddyfile
    prometheus.yml
```


---

# 18. Rust Crates

## Core

```toml
tokio = "1"
axum = "0.8"
tower = "0.5"
tower-http = "0.6"
serde = "1"
serde_json = "1"
uuid = "1"
chrono = "0.4"
thiserror = "2"
anyhow = "1"
tracing = "0.1"
tracing-subscriber = "0.3"
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres", "uuid", "chrono", "json", "bigdecimal"] }
reqwest = { version = "0.12", features = ["json", "rustls-tls"] }
tokio-tungstenite = "0.26"
rust_decimal = "1"
secrecy = "0.10"
argon2 = "0.5"
jsonwebtoken = "9"
```

## Notes

Use `rust_decimal`, not `f64`, for money-sensitive logic.

Use `f64` only for indicators where tiny floating error is acceptable.


---

# 19. Deployment

## MVP Deployment

```txt
Single VPS
Docker Compose
PostgreSQL container
Rust services containers
Next.js dashboard container
Caddy reverse proxy
Prometheus optional
Grafana optional
```

## Docker Compose Services

```yaml
services:
  api:
    build:
      context: ..
      dockerfile: infra/Dockerfile.api
    env_file:
      - .env
    depends_on:
      - postgres

  market-ingest:
    build:
      context: ..
      dockerfile: infra/Dockerfile.service
    command: ["market-ingest"]
    env_file:
      - .env
    depends_on:
      - postgres

  strategy-engine:
    build:
      context: ..
      dockerfile: infra/Dockerfile.service
    command: ["strategy-engine"]
    env_file:
      - .env
    depends_on:
      - postgres

  risk-engine:
    build:
      context: ..
      dockerfile: infra/Dockerfile.service
    command: ["risk-engine"]
    env_file:
      - .env
    depends_on:
      - postgres

  execution-engine:
    build:
      context: ..
      dockerfile: infra/Dockerfile.service
    command: ["execution-engine"]
    env_file:
      - .env
    depends_on:
      - postgres

  dashboard:
    build:
      context: ../apps/dashboard
    env_file:
      - .env

  postgres:
    image: postgres:16
    volumes:
      - postgres_data:/var/lib/postgresql/data
    environment:
      POSTGRES_DB: aegis
      POSTGRES_USER: aegis
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}

  caddy:
    image: caddy:2
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile
    ports:
      - "80:80"
      - "443:443"

volumes:
  postgres_data:
```


---

# 20. Runtime Modes

## Mode 1: Research

```txt
No orders
Only ingest data
Compute indicators
Store signals
```

## Mode 2: Paper

```txt
Fake execution
Simulated fills
Real market data
No exchange order
```

## Mode 3: Shadow

```txt
Real exchange account read-only
No trade
Compare intended orders vs real market
```

## Mode 4: Live Tiny

```txt
Real orders
Small notional
Strict risk limits
Manual supervision
```

## Mode 5: Live Autonomous

Not allowed until:

```txt
30+ days paper trading
7+ days shadow mode
zero reconciliation bugs
zero duplicate order bugs
kill switch tested
daily loss rule tested
exchange reconnect tested
```


---

# 21. MVP Scope

## MVP v1

Build only:

* Binance public WebSocket ingest
* BTC/USDT and ETH/USDT candles
* Postgres storage
* simple strategy engine
* paper trading engine
* risk engine
* operational dashboard
* kill switch
* audit logs
* Docker Compose deployment

No real money yet.

## MVP Success Criteria

```txt
System runs 7 days without crash
Market data reconnect works
Candles stored correctly
Signals generated deterministically
Paper orders generated
Risk engine rejects invalid trades
Dashboard shows live state
Kill switch blocks orders
Audit logs capture sensitive actions
```


---

# 22. Phase Roadmap

## Phase 0: Foundation

Duration target: 1 to 2 weeks.

Deliverables:

* Rust workspace
* Postgres migrations
* Docker Compose
* API health endpoint
* dashboard shell
* auth MVP
* structured logging


---

## Phase 1: Market Data

Deliverables:

* Binance WebSocket ingest
* candle builder
* market data storage
* reconnect handling
* stale feed detector
* market data dashboard

Acceptance:

```txt
BTCUSDT data streams continuously
Candles generated correctly
Feed stale event emitted if stream dies
```


---

## Phase 2: Strategy + Paper Trading

Deliverables:

* indicator engine
* momentum strategy
* volatility breakout strategy
* signal table
* paper order simulator
* paper position tracking

Acceptance:

```txt
Signals can be replayed
Paper PnL is computed
Every order has signal source
```


---

## Phase 3: Risk Engine

Deliverables:

* max position rule
* max daily loss rule
* max open position rule
* stale signal rule
* duplicate order rule
* kill switch
* cooldown rule

Acceptance:

```txt
Risk engine rejects unsafe trades
Risk reasons are visible in dashboard
Kill switch survives restart
```


---

## Phase 4: Execution Engine Testnet

Deliverables:

* exchange adapter
* Binance testnet or Bybit testnet
* order submit
* order cancel
* order status reconciliation
* private event stream

Acceptance:

```txt
No duplicate orders
Order state transitions are correct
Exchange mismatch triggers pause
```


---

## Phase 5: Live Tiny Capital

Deliverables:

* live key config
* live mode lock
* TOTP confirmation
* max live notional
* live order audit
* emergency stop

Acceptance:

```txt
Rp2 juta capital protected by hard risk limits
No order can bypass risk engine
Manual kill switch works immediately
```


---

## Phase 6: LLM Analyst

Deliverables:

* regime classifier
* anomaly summaries
* drawdown explanation
* strategy mode recommendations
* no direct execution authority

Acceptance:

```txt
LLM output can reduce risk
LLM output cannot place order
LLM output cannot increase exposure automatically
```


---

# 23. Frontend MVP Layout

## Sidebar

```txt
Command Center
Positions
Orders
Strategies
Risk
Market Data
LLM Analyst
Logs
Settings
```

## Header

Always visible:

```txt
MODE: PAPER / LIVE
KILL SWITCH: ON / OFF
EXCHANGE: CONNECTED / DEGRADED
DATA AGE: 231ms
DAILY PNL: -0.8%
```

## Critical UX Rule

Dangerous actions require typed confirmation.

Examples:

```txt
ENABLE LIVE
DISABLE KILL SWITCH
CLOSE BTCUSDT
ROTATE API KEY
```


---

# 24. Settings

## Strategy Config

```json
{
  "strategy_id": "vol_breakout_v1",
  "enabled": true,
  "mode": "paper",
  "symbols": ["BTCUSDT", "ETHUSDT"],
  "timeframe": "5m",
  "max_signal_age_ms": 5000,
  "cooldown_seconds": 900
}
```

## Risk Config

```json
{
  "max_open_positions": 2,
  "max_daily_loss_pct": 2.0,
  "max_weekly_loss_pct": 5.0,
  "max_position_notional_idr": 150000,
  "max_slippage_pct": 0.3,
  "max_consecutive_losses": 3,
  "kill_switch": false
}
```


---

# 25. Observability

## Required Metrics

```txt
market_events_per_second
market_feed_latency_ms
exchange_api_latency_ms
orders_submitted_total
orders_rejected_total
risk_rejections_total
strategy_signals_total
pnl_daily
drawdown_daily_pct
open_positions
reconnect_count
stale_feed_count
llm_requests_total
llm_latency_ms
```

## Required Logs

Use structured logs.

Example:

```json
{
  "level": "INFO",
  "event": "risk.rejected",
  "signal_id": "uuid",
  "reason": "daily_loss_limit_reached",
  "timestamp": "timestamp"
}
```

No unstructured "println debugging" in production.


---

# 26. Backtesting and Replay

## MVP Backtest

Simple historical replay.

Input:

```txt
candles from database
strategy config
risk config
fee model
slippage model
```

Output:

```txt
PnL
max drawdown
win rate
avg win
avg loss
Sharpe-ish metric
trade count
fee impact
```

## Replay Rule

Same data + same config must produce same result.

If not deterministic, the system is broken.


---

# 27. Security Acceptance Criteria

System is not v9.5 unless:

```txt
No API key appears in logs
No frontend receives exchange secret
Kill switch persists in DB
Live trading disabled by default
Risk engine cannot be bypassed
All commands audited
All order requests have idempotency key
All live actions require auth
Dangerous actions require typed confirmation
Postgres is not publicly exposed
Dashboard is behind auth
```


---

# 28. Failure Modes

## Failure: Exchange WebSocket Disconnect

Action:

```txt
attempt reconnect
mark feed degraded
pause strategy if stale threshold exceeded
emit risk.data_stale
```

## Failure: Exchange REST Timeout

Action:

```txt
do not retry blindly for order placement
check order status using client_order_id
pause if state unknown
```

## Failure: Duplicate Signal

Action:

```txt
dedupe by signal hash + strategy_id + candle timestamp
```

## Failure: Unknown Order State

Action:

```txt
pause execution
run reconciliation
block new orders
alert dashboard
```

## Failure: LLM Bad Recommendation

Action:

```txt
treat LLM as advisory
risk engine ignores unsafe recommendation
```

## Failure: DB Down

Action:

```txt
stop trading
do not operate from memory only
```

## Failure: Frontend Down

Action:

```txt
backend continues only if safe
kill switch must also be callable via secure CLI
```


---

# 29. CLI Fallback

Build a CLI.

Frontend can fail. CLI saves you.

```txt
aegis status
aegis kill
aegis resume
aegis positions
aegis orders
aegis reconcile
aegis strategy disable vol_breakout_v1
```

This is very "real infra".


---

# 30. Recommended API/Data Source Plan

## Phase 1

Use:

* Binance public market WebSocket
* Bybit public market WebSocket optional
* no paid market data
* no equity data

## Phase 2

Add:

* exchange private WebSocket
* account balance
* order updates
* fills

## Phase 3

Add cheap external context:

* RSS news
* economic calendar
* Fear & Greed index
* funding rates
* open interest
* liquidation data if available

Do not buy expensive data before the infra proves it can survive.


---

# 31. Strategy Philosophy

## Safe

Rule-based only.

```txt
momentum
mean reversion
volatility breakout
trend filter
```

## Edgy

LLM modifies risk mode.

```txt
risk_multiplier = 0.5
strategy_mode = defensive
trade_frequency = reduced
```

## Speculative

LLM as portfolio committee.

It reads:

* news
* volatility
* recent drawdown
* liquidity
* strategy performance

Then outputs:

```txt
disable mean reversion today
allow breakout only
reduce size by 50%
```

Still no direct orders.


---

# 32. The "Jane Street-like" Part

Not:

* secret alpha
* HFT speed
* genius math cosplay

Yes:

* correctness
* risk discipline
* state machines
* audit trails
* low-latency awareness
* deterministic behavior
* post-trade analysis
* failure containment
* replayability

That's the part worth copying.


---

# 33. Version 9.5 Definition

This PRD is "9.5" because it includes:

```txt
Rust-first backend
Operational FE
Risk-first design
Security requirements
Execution state machine
Duplicate order protection
Kill switch
Audit logs
Replay/backtest requirement
LLM guardrails
Docker deployment
VPS feasibility
Live trading lock
```

It is not 10.0 because it does not yet include:

```txt
formal verification
full smart order routing
multi-exchange arbitrage
portfolio optimizer
colocated infra
market making engine
full VaR engine
Kubernetes production cluster
institutional compliance layer
```


---

# 34. Final Recommendation

Build in this order:

```txt
1. Rust workspace + Docker Compose
2. Postgres schema + event log
3. Binance market ingest
4. Candle builder
5. Dashboard command center
6. Strategy engine
7. Paper trading
8. Risk engine
9. Kill switch
10. Execution engine testnet
11. Reconciliation
12. Live tiny capital
13. LLM analyst
```

Do **not** start from LLM.

Do **not** start from UI.

Do **not** start from real trading.

Start from event log + market ingest + deterministic replay.

That is the real foundation.


---

# 35. Best Final Positioning

Use this for resume, GitHub, or portfolio:

> Designed and built a Rust-based autonomous quant execution infrastructure for crypto markets, focused on deterministic order lifecycle management, real-time market data ingestion, risk-gated execution, duplicate order prevention, exchange state reconciliation, and operational observability. The system uses AI only as a constrained market regime analyst while preserving deterministic risk and execution controls.

This sounds top-tier because it avoids cringe "AI trading bot" framing.


---

# Sources

Previous uploaded PRD baseline: Axum official docs describe it as an HTTP routing and request-handling library focused on ergonomics and modularity. ([Docs.rs](https://docs.rs/axum/latest/axum/?utm_source=chatgpt.com "axum - Rust")) Tokio official docs describe it as Rust's async runtime for reliable asynchronous applications with I/O, networking, scheduling, and timers. ([tokio.rs](https://tokio.rs/?utm_source=chatgpt.com "Tokio - An asynchronous Rust runtime")) SQLx docs describe it as an async Rust SQL toolkit with optional compile-time checked queries. ([Docs.rs](https://docs.rs/sqlx/latest/sqlx/?utm_source=chatgpt.com "sqlx - Rust")) Binance Spot WebSocket docs note 24-hour connection validity, ping/pong behavior, and timestamp behavior, so reconnect/stale-feed handling should be designed explicitly. ([Binance Developers](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams?utm_source=chatgpt.com "WebSocket Streams | Binance Open Platform")) Bybit WebSocket trade docs state that order ACK means accepted, while order status should be confirmed through the order stream, supporting the PRD's "ACK is not fill" rule. ([bybit-exchange.github.io](https://bybit-exchange.github.io/docs/v5/websocket/trade/guideline?utm_source=chatgpt.com "Websocket Trade Guideline | Bybit API Documentation"))