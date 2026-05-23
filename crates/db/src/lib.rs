use aegis_core::{
    Candle, CandleInterval, DataFreshnessStatus, EventEnvelope, ExecutionState, FeedStatus,
    MarketDataSource, MarketTick, OrderIntent, OrderStatus, PaperOrder, RiskCheckContext,
    RiskEvaluationDecision, RiskEvaluationResult, Side, Symbol,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, Postgres, Row, Transaction};
use thiserror::Error;
use uuid::Uuid;

pub const MIGRATIONS_DIR: &str = "crates/db/migrations";
const GLOBAL_SYSTEM_STATE_KEY: &str = "global";
pub use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct DbConfig {
    pub database_url: String,
    pub max_connections: u32,
}

impl DbConfig {
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            max_connections: 5,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemEventRecord {
    pub event_id: Uuid,
    pub event_type: String,
    pub source: String,
    pub correlation_id: Uuid,
    pub payload: Value,
    pub occurred_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStateRecord {
    pub state_key: String,
    pub kill_switch_enabled: bool,
    pub kill_switch_reason: Option<String>,
    pub updated_by_actor: String,
    pub updated_by_actor_id: Option<Uuid>,
    pub last_correlation_id: Uuid,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskDecisionRecord {
    pub risk_decision_id: Uuid,
    pub correlation_id: Uuid,
    pub signal_id: Option<Uuid>,
    pub decision: String,
    pub rationale: String,
    pub decided_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRecord {
    pub order_id: Uuid,
    pub correlation_id: Uuid,
    pub risk_decision_id: Uuid,
    pub idempotency_key: String,
    pub symbol: String,
    pub side: String,
    pub quantity: sqlx::types::Decimal,
    pub limit_price: Option<sqlx::types::Decimal>,
    pub market_mode: String,
    pub status: String,
    pub execution_state: String,
    pub status_reason: Option<String>,
    pub filled_price: Option<sqlx::types::Decimal>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub filled_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub rejected_at: Option<DateTime<Utc>>,
    pub expired_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketTickRecord {
    pub id: Uuid,
    pub exchange: String,
    pub symbol: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub trade_time: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub raw_payload: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandleRecord {
    pub id: Uuid,
    pub exchange: String,
    pub symbol: String,
    pub interval: String,
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub quote_volume: Option<Decimal>,
    pub trade_count: i32,
    pub is_closed: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketFeedStatusRecord {
    pub exchange: String,
    pub symbol: String,
    pub status: String,
    pub freshness_status: DataFreshnessStatus,
    pub last_event_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub reconnect_count: i32,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum CreateOrderError {
    #[error("risk decision was not found")]
    RiskDecisionNotFound,
    #[error("risk decision is not approved")]
    RiskDecisionNotApproved,
    #[error("duplicate idempotency key")]
    DuplicateIdempotencyKey,
    #[error("order intent is invalid: {0}")]
    InvalidIntent(String),
    #[error(transparent)]
    Unexpected(#[from] anyhow::Error),
}

#[derive(Debug, Clone)]
pub struct OrderCreateOutcome {
    pub order: OrderRecord,
    pub transitions: Vec<ExecutionState>,
}

#[derive(Debug, Clone)]
pub struct StateActor {
    pub actor: String,
    pub actor_id: Option<Uuid>,
}

impl StateActor {
    pub fn system(actor: impl Into<String>) -> Self {
        Self {
            actor: actor.into(),
            actor_id: None,
        }
    }
}

pub async fn connect_pool(config: &DbConfig) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect(&config.database_url)
        .await?;

    Ok(pool)
}

pub async fn check_health(pool: &PgPool) -> Result<()> {
    sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(pool)
        .await?;

    Ok(())
}

pub async fn ensure_system_state(pool: &PgPool) -> Result<SystemStateRecord> {
    let bootstrap_correlation_id = Uuid::new_v4();
    let row = sqlx::query(
        r#"
        INSERT INTO system_state (
            state_key,
            kill_switch_enabled,
            kill_switch_reason,
            updated_by_actor,
            updated_by_actor_id,
            last_correlation_id,
            updated_at
        )
        VALUES ($1, FALSE, NULL, $2, NULL, $3, NOW())
        ON CONFLICT (state_key) DO UPDATE
        SET updated_at = system_state.updated_at
        RETURNING
            state_key,
            kill_switch_enabled,
            kill_switch_reason,
            updated_by_actor,
            updated_by_actor_id,
            last_correlation_id,
            updated_at
        "#,
    )
    .bind(GLOBAL_SYSTEM_STATE_KEY)
    .bind("system.bootstrap")
    .bind(bootstrap_correlation_id)
    .fetch_one(pool)
    .await?;

    Ok(map_system_state(&row))
}

pub async fn get_system_state(pool: &PgPool) -> Result<SystemStateRecord> {
    let row = sqlx::query(
        r#"
        SELECT
            state_key,
            kill_switch_enabled,
            kill_switch_reason,
            updated_by_actor,
            updated_by_actor_id,
            last_correlation_id,
            updated_at
        FROM system_state
        WHERE state_key = $1
        "#,
    )
    .bind(GLOBAL_SYSTEM_STATE_KEY)
    .fetch_optional(pool)
    .await?;

    match row {
        Some(row) => Ok(map_system_state(&row)),
        None => ensure_system_state(pool).await,
    }
}

pub async fn set_kill_switch_state(
    pool: &PgPool,
    actor: &StateActor,
    correlation_id: Uuid,
    source: &str,
    enabled: bool,
    reason: Option<String>,
) -> Result<SystemStateRecord> {
    let mut tx = pool.begin().await?;
    let action = if enabled {
        "risk.kill_switch.activate"
    } else {
        "risk.kill_switch.resume"
    };
    let event_type = if enabled {
        "system.kill_switch.enabled"
    } else {
        "system.kill_switch.disabled"
    };

    let state_row = sqlx::query(
        r#"
        INSERT INTO system_state (
            state_key,
            kill_switch_enabled,
            kill_switch_reason,
            updated_by_actor,
            updated_by_actor_id,
            last_correlation_id,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        ON CONFLICT (state_key) DO UPDATE
        SET
            kill_switch_enabled = EXCLUDED.kill_switch_enabled,
            kill_switch_reason = EXCLUDED.kill_switch_reason,
            updated_by_actor = EXCLUDED.updated_by_actor,
            updated_by_actor_id = EXCLUDED.updated_by_actor_id,
            last_correlation_id = EXCLUDED.last_correlation_id,
            updated_at = NOW()
        RETURNING
            state_key,
            kill_switch_enabled,
            kill_switch_reason,
            updated_by_actor,
            updated_by_actor_id,
            last_correlation_id,
            updated_at
        "#,
    )
    .bind(GLOBAL_SYSTEM_STATE_KEY)
    .bind(enabled)
    .bind(reason.as_deref())
    .bind(&actor.actor)
    .bind(actor.actor_id)
    .bind(correlation_id)
    .fetch_one(&mut *tx)
    .await?;

    let updated_state = map_system_state(&state_row);
    let metadata = json!({
        "actor_id": actor.actor_id,
        "kill_switch_enabled": updated_state.kill_switch_enabled,
        "kill_switch_reason": updated_state.kill_switch_reason,
        "state_key": updated_state.state_key,
    });

    sqlx::query(
        r#"
        INSERT INTO audit_logs (id, correlation_id, actor, action, target, metadata)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(correlation_id)
    .bind(&actor.actor)
    .bind(action)
    .bind("system_state.kill_switch")
    .bind(&metadata)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO system_events (id, correlation_id, event_type, source, payload, occurred_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(correlation_id)
    .bind(event_type)
    .bind(source)
    .bind(&metadata)
    .bind(Utc::now())
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(updated_state)
}

pub async fn insert_system_event(
    pool: &PgPool,
    event: &EventEnvelope,
) -> Result<SystemEventRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO system_events (id, correlation_id, event_type, source, payload, occurred_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING
            id,
            correlation_id,
            event_type,
            source,
            payload,
            occurred_at,
            created_at
        "#,
    )
    .bind(event.event_id)
    .bind(event.correlation_id)
    .bind(&event.event_type)
    .bind(&event.source)
    .bind(&event.payload)
    .bind(event.occurred_at)
    .fetch_one(pool)
    .await?;

    Ok(map_system_event(&row))
}

pub async fn load_risk_state_snapshot(pool: &PgPool) -> Result<risk_engine::RiskStateSnapshot> {
    let system_state = get_system_state(pool).await?;
    let latest_market_data_at = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
        r#"
        SELECT MAX(last_event_at)
        FROM market_feed_status
        "#,
    )
    .fetch_one(pool)
    .await
    .ok()
    .flatten();

    Ok(risk_engine::RiskStateSnapshot {
        kill_switch_enabled: system_state.kill_switch_enabled,
        kill_switch_reason: system_state.kill_switch_reason,
        open_positions_count: None,
        daily_loss: None,
        latest_market_data_at,
    })
}

pub async fn insert_risk_evaluation(
    pool: &PgPool,
    source: &str,
    context: &RiskCheckContext,
    evaluation: &RiskEvaluationResult,
) -> Result<RiskDecisionRecord> {
    let mut tx = pool.begin().await?;
    let rationale = serde_json::to_string(&json!({
        "approved_notional": evaluation.approved_notional,
        "risk_score": evaluation.risk_score,
        "reasons": evaluation.reasons,
        "rule_results": evaluation.rule_results,
        "strategy_id": context.strategy_id,
        "symbol": context.symbol.as_str(),
        "side": context.side,
        "suggested_notional": context.suggested_notional,
    }))?;

    let row = sqlx::query(
        r#"
        INSERT INTO risk_decisions (id, correlation_id, signal_id, decision, rationale, decided_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING
            id,
            correlation_id,
            signal_id,
            decision,
            rationale,
            decided_at
        "#,
    )
    .bind(evaluation.risk_decision_id)
    .bind(evaluation.correlation_id)
    .bind(context.signal_id)
    .bind(match evaluation.decision {
        RiskEvaluationDecision::Approved => "APPROVED",
        RiskEvaluationDecision::Rejected => "REJECTED",
    })
    .bind(&rationale)
    .bind(Utc::now())
    .fetch_one(&mut *tx)
    .await?;

    let event_type = match evaluation.decision {
        RiskEvaluationDecision::Approved => "risk.approved",
        RiskEvaluationDecision::Rejected => "risk.rejected",
    };

    let payload = json!({
        "risk_decision_id": evaluation.risk_decision_id,
        "signal_id": context.signal_id,
        "decision": event_type.strip_prefix("risk.").unwrap_or(event_type).to_ascii_uppercase(),
        "approved_notional": evaluation.approved_notional,
        "risk_score": evaluation.risk_score,
        "reasons": evaluation.reasons,
        "correlation_id": evaluation.correlation_id,
    });

    sqlx::query(
        r#"
        INSERT INTO system_events (id, correlation_id, event_type, source, payload, occurred_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(evaluation.correlation_id)
    .bind(event_type)
    .bind(source)
    .bind(&payload)
    .bind(Utc::now())
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(map_risk_decision(&row))
}

pub async fn create_paper_order(
    pool: &PgPool,
    source: &str,
    actor: &StateActor,
    intent: OrderIntent,
) -> std::result::Result<OrderCreateOutcome, CreateOrderError> {
    intent
        .validate()
        .map_err(|err| CreateOrderError::InvalidIntent(err.to_string()))?;

    let mut order = PaperOrder::new(intent.clone())
        .map_err(|err| CreateOrderError::InvalidIntent(err.to_string()))?;
    let mut tx = pool.begin().await.map_err(anyhow::Error::from)?;

    let risk_row = sqlx::query(
        r#"
        SELECT id, decision
        FROM risk_decisions
        WHERE id = $1
        "#,
    )
    .bind(intent.risk_decision_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(anyhow::Error::from)?;

    let Some(risk_row) = risk_row else {
        return Err(CreateOrderError::RiskDecisionNotFound);
    };

    let decision: String = risk_row.get("decision");
    if decision != "APPROVED" {
        return Err(CreateOrderError::RiskDecisionNotApproved);
    }

    let insert_result = sqlx::query(
        r#"
        INSERT INTO orders (
            id,
            correlation_id,
            risk_decision_id,
            idempotency_key,
            symbol,
            side,
            quantity,
            limit_price,
            market_mode,
            status,
            execution_state,
            status_reason,
            filled_price,
            submitted_at,
            filled_at,
            cancelled_at,
            rejected_at,
            expired_at,
            expires_at,
            created_at,
            updated_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, 'PAPER', $9, $10, NULL, NULL, NULL, NULL, NULL, NULL, NULL, $11, $12, $12
        )
        "#,
    )
    .bind(intent.order_id)
    .bind(intent.correlation_id)
    .bind(intent.risk_decision_id)
    .bind(&intent.idempotency_key)
    .bind(intent.symbol.as_str())
    .bind(match intent.side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    })
    .bind(intent.quantity)
    .bind(intent.limit_price)
    .bind(order_status_as_str(order.status))
    .bind(execution_state_as_str(order.execution_state))
    .bind(intent.expires_at)
    .bind(intent.created_at)
    .execute(&mut *tx)
    .await;

    if let Err(err) = insert_result {
        if is_unique_violation(&err) {
            return Err(CreateOrderError::DuplicateIdempotencyKey);
        }
        return Err(CreateOrderError::Unexpected(anyhow::Error::from(err)));
    }

    let mut transitions = vec![ExecutionState::IntentCreated];
    insert_order_event(&mut tx, source, &order, ExecutionState::IntentCreated).await?;

    let risk_approved_at = Utc::now();
    order
        .transition_to(ExecutionState::RiskApproved, risk_approved_at, None)
        .map_err(|err| CreateOrderError::InvalidIntent(err.to_string()))?;
    update_order_state(&mut tx, &order).await?;
    insert_order_event(&mut tx, source, &order, ExecutionState::RiskApproved).await?;
    transitions.push(ExecutionState::RiskApproved);

    let prepared_at = Utc::now();
    order
        .transition_to(ExecutionState::OrderPrepared, prepared_at, None)
        .map_err(|err| CreateOrderError::InvalidIntent(err.to_string()))?;
    update_order_state(&mut tx, &order).await?;
    insert_order_event(&mut tx, source, &order, ExecutionState::OrderPrepared).await?;
    transitions.push(ExecutionState::OrderPrepared);

    if let Some(expires_at) = order.intent.expires_at {
        if expires_at <= Utc::now() {
            order
                .transition_to(
                    ExecutionState::Expired,
                    Utc::now(),
                    Some("order intent expired before paper submission".to_string()),
                )
                .map_err(|err| CreateOrderError::InvalidIntent(err.to_string()))?;
            update_order_state(&mut tx, &order).await?;
            insert_order_event(&mut tx, source, &order, ExecutionState::Expired).await?;
            insert_order_audit_log(&mut tx, actor, &order, "paper_order.create").await?;
            tx.commit().await.map_err(anyhow::Error::from)?;

            return Ok(OrderCreateOutcome {
                order: get_order_by_id(pool, order.intent.order_id)
                    .await
                    .map_err(CreateOrderError::Unexpected)?
                    .expect("order must exist after commit"),
                transitions: {
                    transitions.push(ExecutionState::Expired);
                    transitions
                },
            });
        }
    }

    let submitted_at = Utc::now();
    order
        .transition_to(ExecutionState::PaperSubmitted, submitted_at, None)
        .map_err(|err| CreateOrderError::InvalidIntent(err.to_string()))?;
    update_order_state(&mut tx, &order).await?;
    insert_order_event(&mut tx, source, &order, ExecutionState::PaperSubmitted).await?;
    transitions.push(ExecutionState::PaperSubmitted);

    let filled_at = Utc::now();
    order.filled_price = order.intent.limit_price;
    order
        .transition_to(ExecutionState::PaperFilled, filled_at, None)
        .map_err(|err| CreateOrderError::InvalidIntent(err.to_string()))?;
    update_order_state(&mut tx, &order).await?;
    insert_order_event(&mut tx, source, &order, ExecutionState::PaperFilled).await?;
    transitions.push(ExecutionState::PaperFilled);

    insert_order_audit_log(&mut tx, actor, &order, "paper_order.create").await?;
    tx.commit().await.map_err(anyhow::Error::from)?;

    let persisted = get_order_by_id(pool, order.intent.order_id)
        .await
        .map_err(CreateOrderError::Unexpected)?
        .expect("order must exist after commit");

    Ok(OrderCreateOutcome {
        order: persisted,
        transitions,
    })
}

pub async fn list_orders(pool: &PgPool) -> Result<Vec<OrderRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            correlation_id,
            risk_decision_id,
            idempotency_key,
            symbol,
            side,
            quantity,
            limit_price,
            market_mode,
            status,
            execution_state,
            status_reason,
            filled_price,
            submitted_at,
            filled_at,
            cancelled_at,
            rejected_at,
            expired_at,
            expires_at,
            created_at,
            updated_at
        FROM orders
        ORDER BY created_at DESC
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_order).collect())
}

pub async fn get_order_by_id(pool: &PgPool, order_id: Uuid) -> Result<Option<OrderRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            correlation_id,
            risk_decision_id,
            idempotency_key,
            symbol,
            side,
            quantity,
            limit_price,
            market_mode,
            status,
            execution_state,
            status_reason,
            filled_price,
            submitted_at,
            filled_at,
            cancelled_at,
            rejected_at,
            expired_at,
            expires_at,
            created_at,
            updated_at
        FROM orders
        WHERE id = $1
        "#,
    )
    .bind(order_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_order))
}

pub async fn insert_market_tick(pool: &PgPool, tick: &MarketTick) -> Result<MarketTickRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO market_ticks (
            id,
            exchange,
            symbol,
            price,
            quantity,
            trade_time,
            received_at,
            raw_payload
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING
            id,
            exchange,
            symbol,
            price,
            quantity,
            trade_time,
            received_at,
            raw_payload
        "#,
    )
    .bind(tick.id)
    .bind(tick.exchange.as_str())
    .bind(tick.symbol.as_str())
    .bind(tick.price)
    .bind(tick.quantity)
    .bind(tick.trade_time)
    .bind(tick.received_at)
    .bind(&tick.raw_payload)
    .fetch_one(pool)
    .await?;

    Ok(map_market_tick(&row))
}

pub async fn get_latest_market_tick(
    pool: &PgPool,
    exchange: MarketDataSource,
    symbol: &Symbol,
) -> Result<Option<MarketTickRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            exchange,
            symbol,
            price,
            quantity,
            trade_time,
            received_at,
            raw_payload
        FROM market_ticks
        WHERE exchange = $1 AND symbol = $2
        ORDER BY trade_time DESC, received_at DESC
        LIMIT 1
        "#,
    )
    .bind(exchange.as_str())
    .bind(symbol.as_str())
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_market_tick))
}

pub async fn upsert_candle(pool: &PgPool, candle: &Candle) -> Result<CandleRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO candles (
            id,
            exchange,
            symbol,
            interval,
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            trade_count,
            is_closed,
            created_at,
            updated_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
        )
        ON CONFLICT (exchange, symbol, interval, open_time) DO UPDATE
        SET
            close_time = EXCLUDED.close_time,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            quote_volume = EXCLUDED.quote_volume,
            trade_count = EXCLUDED.trade_count,
            is_closed = EXCLUDED.is_closed,
            updated_at = EXCLUDED.updated_at
        RETURNING
            id,
            exchange,
            symbol,
            interval,
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            trade_count,
            is_closed,
            created_at,
            updated_at
        "#,
    )
    .bind(candle.id)
    .bind(candle.exchange.as_str())
    .bind(candle.symbol.as_str())
    .bind(candle.interval.as_str())
    .bind(candle.open_time)
    .bind(candle.close_time)
    .bind(candle.open)
    .bind(candle.high)
    .bind(candle.low)
    .bind(candle.close)
    .bind(candle.volume)
    .bind(candle.quote_volume)
    .bind(candle.trade_count)
    .bind(candle.is_closed)
    .bind(candle.created_at)
    .bind(candle.updated_at)
    .fetch_one(pool)
    .await?;

    Ok(map_candle(&row))
}

pub async fn list_candles(
    pool: &PgPool,
    exchange: MarketDataSource,
    symbol: &Symbol,
    interval: CandleInterval,
    limit: i64,
) -> Result<Vec<CandleRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            exchange,
            symbol,
            interval,
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            trade_count,
            is_closed,
            created_at,
            updated_at
        FROM candles
        WHERE exchange = $1 AND symbol = $2 AND interval = $3
        ORDER BY open_time DESC
        LIMIT $4
        "#,
    )
    .bind(exchange.as_str())
    .bind(symbol.as_str())
    .bind(interval.as_str())
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_candle).collect())
}

pub async fn upsert_market_feed_status(
    pool: &PgPool,
    exchange: MarketDataSource,
    symbol: &Symbol,
    status: FeedStatus,
    freshness_status: DataFreshnessStatus,
    last_event_at: Option<DateTime<Utc>>,
    last_error: Option<&str>,
    reconnect_count: i32,
) -> Result<MarketFeedStatusRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO market_feed_status (
            exchange,
            symbol,
            status,
            freshness_status,
            last_event_at,
            last_error,
            reconnect_count,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
        ON CONFLICT (exchange, symbol) DO UPDATE
        SET
            status = EXCLUDED.status,
            freshness_status = EXCLUDED.freshness_status,
            last_event_at = EXCLUDED.last_event_at,
            last_error = EXCLUDED.last_error,
            reconnect_count = EXCLUDED.reconnect_count,
            updated_at = NOW()
        RETURNING
            exchange,
            symbol,
            status,
            freshness_status,
            last_event_at,
            last_error,
            reconnect_count,
            updated_at
        "#,
    )
    .bind(exchange.as_str())
    .bind(symbol.as_str())
    .bind(status.as_str())
    .bind(match freshness_status {
        DataFreshnessStatus::Fresh => "fresh",
        DataFreshnessStatus::Stale => "stale",
        DataFreshnessStatus::Unknown => "unknown",
    })
    .bind(last_event_at)
    .bind(last_error)
    .bind(reconnect_count)
    .fetch_one(pool)
    .await?;

    Ok(map_market_feed_status(&row))
}

pub async fn list_market_feed_statuses(pool: &PgPool) -> Result<Vec<MarketFeedStatusRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            exchange,
            symbol,
            status,
            freshness_status,
            last_event_at,
            last_error,
            reconnect_count,
            updated_at
        FROM market_feed_status
        ORDER BY exchange, symbol
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_market_feed_status).collect())
}

pub async fn process_market_trade(
    pool: &PgPool,
    source: &str,
    tick: &MarketTick,
    active_candle: &Candle,
    closed_candle: Option<&Candle>,
    reconnect_count: i32,
) -> Result<()> {
    let mut tx = pool.begin().await?;

    insert_market_tick_tx(&mut tx, tick).await?;
    upsert_candle_tx(&mut tx, active_candle).await?;

    if let Some(closed_candle) = closed_candle {
        upsert_candle_tx(&mut tx, closed_candle).await?;
    }

    upsert_market_feed_status_tx(
        &mut tx,
        tick.exchange,
        &tick.symbol,
        FeedStatus::Connected,
        DataFreshnessStatus::Fresh,
        Some(tick.trade_time),
        None,
        reconnect_count,
    )
    .await?;

    let trade_payload = json!({
        "exchange": tick.exchange.as_str(),
        "symbol": tick.symbol.as_str(),
        "price": tick.price,
        "quantity": tick.quantity,
        "trade_time": tick.trade_time,
        "received_at": tick.received_at,
    });
    insert_system_event_tx(
        &mut tx,
        &EventEnvelope::new(
            "market.trade.received",
            Uuid::new_v4(),
            source,
            trade_payload,
        ),
    )
    .await?;

    if let Some(closed_candle) = closed_candle {
        let candle_payload = json!({
            "exchange": closed_candle.exchange.as_str(),
            "symbol": closed_candle.symbol.as_str(),
            "interval": closed_candle.interval.as_str(),
            "open_time": closed_candle.open_time,
            "close_time": closed_candle.close_time,
            "open": closed_candle.open,
            "high": closed_candle.high,
            "low": closed_candle.low,
            "close": closed_candle.close,
            "volume": closed_candle.volume,
            "quote_volume": closed_candle.quote_volume,
            "trade_count": closed_candle.trade_count,
        });
        insert_system_event_tx(
            &mut tx,
            &EventEnvelope::new(
                "market.candle.closed",
                Uuid::new_v4(),
                source,
                candle_payload,
            ),
        )
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

pub async fn list_recent_system_events(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<SystemEventRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            correlation_id,
            event_type,
            source,
            payload,
            occurred_at,
            created_at
        FROM system_events
        ORDER BY created_at DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_system_event).collect())
}

pub async fn get_system_event(pool: &PgPool, event_id: Uuid) -> Result<Option<SystemEventRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            correlation_id,
            event_type,
            source,
            payload,
            occurred_at,
            created_at
        FROM system_events
        WHERE id = $1
        "#,
    )
    .bind(event_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_system_event))
}

fn map_system_event(row: &sqlx::postgres::PgRow) -> SystemEventRecord {
    SystemEventRecord {
        event_id: row.get("id"),
        correlation_id: row.get("correlation_id"),
        event_type: row.get("event_type"),
        source: row.get("source"),
        payload: row.get("payload"),
        occurred_at: row.get("occurred_at"),
        created_at: row.get("created_at"),
    }
}

fn map_market_tick(row: &sqlx::postgres::PgRow) -> MarketTickRecord {
    MarketTickRecord {
        id: row.get("id"),
        exchange: row.get("exchange"),
        symbol: row.get("symbol"),
        price: row.get("price"),
        quantity: row.get("quantity"),
        trade_time: row.get("trade_time"),
        received_at: row.get("received_at"),
        raw_payload: row.get("raw_payload"),
    }
}

fn map_candle(row: &sqlx::postgres::PgRow) -> CandleRecord {
    CandleRecord {
        id: row.get("id"),
        exchange: row.get("exchange"),
        symbol: row.get("symbol"),
        interval: row.get("interval"),
        open_time: row.get("open_time"),
        close_time: row.get("close_time"),
        open: row.get("open"),
        high: row.get("high"),
        low: row.get("low"),
        close: row.get("close"),
        volume: row.get("volume"),
        quote_volume: row.get("quote_volume"),
        trade_count: row.get("trade_count"),
        is_closed: row.get("is_closed"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn map_market_feed_status(row: &sqlx::postgres::PgRow) -> MarketFeedStatusRecord {
    MarketFeedStatusRecord {
        exchange: row.get("exchange"),
        symbol: row.get("symbol"),
        status: row.get("status"),
        freshness_status: freshness_status_from_str(row.get("freshness_status")),
        last_event_at: row.get("last_event_at"),
        last_error: row.get("last_error"),
        reconnect_count: row.get("reconnect_count"),
        updated_at: row.get("updated_at"),
    }
}

fn map_risk_decision(row: &sqlx::postgres::PgRow) -> RiskDecisionRecord {
    RiskDecisionRecord {
        risk_decision_id: row.get("id"),
        correlation_id: row.get("correlation_id"),
        signal_id: row.get("signal_id"),
        decision: row.get("decision"),
        rationale: row.get("rationale"),
        decided_at: row.get("decided_at"),
    }
}

fn map_system_state(row: &sqlx::postgres::PgRow) -> SystemStateRecord {
    SystemStateRecord {
        state_key: row.get("state_key"),
        kill_switch_enabled: row.get("kill_switch_enabled"),
        kill_switch_reason: row.get("kill_switch_reason"),
        updated_by_actor: row.get("updated_by_actor"),
        updated_by_actor_id: row.get("updated_by_actor_id"),
        last_correlation_id: row.get("last_correlation_id"),
        updated_at: row.get("updated_at"),
    }
}

fn map_order(row: &sqlx::postgres::PgRow) -> OrderRecord {
    OrderRecord {
        order_id: row.get("id"),
        correlation_id: row.get("correlation_id"),
        risk_decision_id: row.get("risk_decision_id"),
        idempotency_key: row.get("idempotency_key"),
        symbol: row.get("symbol"),
        side: row.get("side"),
        quantity: row.get("quantity"),
        limit_price: row.get("limit_price"),
        market_mode: row.get("market_mode"),
        status: row.get("status"),
        execution_state: row.get("execution_state"),
        status_reason: row.get("status_reason"),
        filled_price: row.get("filled_price"),
        submitted_at: row.get("submitted_at"),
        filled_at: row.get("filled_at"),
        cancelled_at: row.get("cancelled_at"),
        rejected_at: row.get("rejected_at"),
        expired_at: row.get("expired_at"),
        expires_at: row.get("expires_at"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

async fn insert_system_event_tx(
    tx: &mut Transaction<'_, Postgres>,
    event: &EventEnvelope,
) -> Result<SystemEventRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO system_events (id, correlation_id, event_type, source, payload, occurred_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING
            id,
            correlation_id,
            event_type,
            source,
            payload,
            occurred_at,
            created_at
        "#,
    )
    .bind(event.event_id)
    .bind(event.correlation_id)
    .bind(&event.event_type)
    .bind(&event.source)
    .bind(&event.payload)
    .bind(event.occurred_at)
    .fetch_one(&mut **tx)
    .await?;

    Ok(map_system_event(&row))
}

async fn insert_market_tick_tx(
    tx: &mut Transaction<'_, Postgres>,
    tick: &MarketTick,
) -> Result<MarketTickRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO market_ticks (
            id,
            exchange,
            symbol,
            price,
            quantity,
            trade_time,
            received_at,
            raw_payload
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING
            id,
            exchange,
            symbol,
            price,
            quantity,
            trade_time,
            received_at,
            raw_payload
        "#,
    )
    .bind(tick.id)
    .bind(tick.exchange.as_str())
    .bind(tick.symbol.as_str())
    .bind(tick.price)
    .bind(tick.quantity)
    .bind(tick.trade_time)
    .bind(tick.received_at)
    .bind(&tick.raw_payload)
    .fetch_one(&mut **tx)
    .await?;

    Ok(map_market_tick(&row))
}

async fn upsert_candle_tx(
    tx: &mut Transaction<'_, Postgres>,
    candle: &Candle,
) -> Result<CandleRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO candles (
            id,
            exchange,
            symbol,
            interval,
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            trade_count,
            is_closed,
            created_at,
            updated_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
        )
        ON CONFLICT (exchange, symbol, interval, open_time) DO UPDATE
        SET
            close_time = EXCLUDED.close_time,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            quote_volume = EXCLUDED.quote_volume,
            trade_count = EXCLUDED.trade_count,
            is_closed = EXCLUDED.is_closed,
            updated_at = EXCLUDED.updated_at
        RETURNING
            id,
            exchange,
            symbol,
            interval,
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            trade_count,
            is_closed,
            created_at,
            updated_at
        "#,
    )
    .bind(candle.id)
    .bind(candle.exchange.as_str())
    .bind(candle.symbol.as_str())
    .bind(candle.interval.as_str())
    .bind(candle.open_time)
    .bind(candle.close_time)
    .bind(candle.open)
    .bind(candle.high)
    .bind(candle.low)
    .bind(candle.close)
    .bind(candle.volume)
    .bind(candle.quote_volume)
    .bind(candle.trade_count)
    .bind(candle.is_closed)
    .bind(candle.created_at)
    .bind(candle.updated_at)
    .fetch_one(&mut **tx)
    .await?;

    Ok(map_candle(&row))
}

async fn upsert_market_feed_status_tx(
    tx: &mut Transaction<'_, Postgres>,
    exchange: MarketDataSource,
    symbol: &Symbol,
    status: FeedStatus,
    freshness_status: DataFreshnessStatus,
    last_event_at: Option<DateTime<Utc>>,
    last_error: Option<&str>,
    reconnect_count: i32,
) -> Result<MarketFeedStatusRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO market_feed_status (
            exchange,
            symbol,
            status,
            freshness_status,
            last_event_at,
            last_error,
            reconnect_count,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
        ON CONFLICT (exchange, symbol) DO UPDATE
        SET
            status = EXCLUDED.status,
            freshness_status = EXCLUDED.freshness_status,
            last_event_at = EXCLUDED.last_event_at,
            last_error = EXCLUDED.last_error,
            reconnect_count = EXCLUDED.reconnect_count,
            updated_at = NOW()
        RETURNING
            exchange,
            symbol,
            status,
            freshness_status,
            last_event_at,
            last_error,
            reconnect_count,
            updated_at
        "#,
    )
    .bind(exchange.as_str())
    .bind(symbol.as_str())
    .bind(status.as_str())
    .bind(match freshness_status {
        DataFreshnessStatus::Fresh => "fresh",
        DataFreshnessStatus::Stale => "stale",
        DataFreshnessStatus::Unknown => "unknown",
    })
    .bind(last_event_at)
    .bind(last_error)
    .bind(reconnect_count)
    .fetch_one(&mut **tx)
    .await?;

    Ok(map_market_feed_status(&row))
}

fn freshness_status_from_str(value: String) -> DataFreshnessStatus {
    match value.as_str() {
        "fresh" => DataFreshnessStatus::Fresh,
        "stale" => DataFreshnessStatus::Stale,
        _ => DataFreshnessStatus::Unknown,
    }
}

fn order_status_as_str(status: OrderStatus) -> &'static str {
    match status {
        OrderStatus::Open => "OPEN",
        OrderStatus::Rejected => "REJECTED",
        OrderStatus::Filled => "FILLED",
        OrderStatus::Cancelled => "CANCELLED",
        OrderStatus::Expired => "EXPIRED",
    }
}

fn execution_state_as_str(state: ExecutionState) -> &'static str {
    match state {
        ExecutionState::IntentCreated => "INTENT_CREATED",
        ExecutionState::RiskApproved => "RISK_APPROVED",
        ExecutionState::OrderPrepared => "ORDER_PREPARED",
        ExecutionState::PaperSubmitted => "PAPER_SUBMITTED",
        ExecutionState::PaperFilled => "PAPER_FILLED",
        ExecutionState::PaperCancelled => "PAPER_CANCELLED",
        ExecutionState::Rejected => "REJECTED",
        ExecutionState::Expired => "EXPIRED",
    }
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Database(db_error) => db_error.code().as_deref() == Some("23505"),
        _ => false,
    }
}

async fn update_order_state(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    order: &PaperOrder,
) -> std::result::Result<(), CreateOrderError> {
    sqlx::query(
        r#"
        UPDATE orders
        SET
            status = $2,
            execution_state = $3,
            status_reason = $4,
            filled_price = $5,
            submitted_at = $6,
            filled_at = $7,
            cancelled_at = $8,
            rejected_at = $9,
            expired_at = $10,
            updated_at = $11
        WHERE id = $1
        "#,
    )
    .bind(order.intent.order_id)
    .bind(order_status_as_str(order.status))
    .bind(execution_state_as_str(order.execution_state))
    .bind(order.status_reason.as_deref())
    .bind(order.filled_price)
    .bind(order.submitted_at)
    .bind(order.filled_at)
    .bind(order.cancelled_at)
    .bind(order.rejected_at)
    .bind(order.expired_at)
    .bind(order.updated_at)
    .execute(&mut **tx)
    .await
    .map_err(anyhow::Error::from)
    .map_err(CreateOrderError::Unexpected)?;

    Ok(())
}

async fn insert_order_event(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    source: &str,
    order: &PaperOrder,
    transition: ExecutionState,
) -> std::result::Result<(), CreateOrderError> {
    let payload = json!({
        "order_id": order.intent.order_id,
        "correlation_id": order.intent.correlation_id,
        "risk_decision_id": order.intent.risk_decision_id,
        "idempotency_key": order.intent.idempotency_key,
        "symbol": order.intent.symbol.as_str(),
        "side": order.intent.side,
        "quantity": order.intent.quantity,
        "limit_price": order.intent.limit_price,
        "filled_price": order.filled_price,
        "status": order_status_as_str(order.status),
        "execution_state": execution_state_as_str(order.execution_state),
        "transition": transition.as_event_name(),
        "status_reason": order.status_reason,
    });

    sqlx::query(
        r#"
        INSERT INTO system_events (id, correlation_id, event_type, source, payload, occurred_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(order.intent.correlation_id)
    .bind(format!(
        "order.{}",
        transition.as_event_name().to_ascii_lowercase()
    ))
    .bind(source)
    .bind(payload)
    .bind(order.updated_at)
    .execute(&mut **tx)
    .await
    .map_err(anyhow::Error::from)
    .map_err(CreateOrderError::Unexpected)?;

    Ok(())
}

async fn insert_order_audit_log(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    actor: &StateActor,
    order: &PaperOrder,
    action: &str,
) -> std::result::Result<(), CreateOrderError> {
    let metadata = json!({
        "order_id": order.intent.order_id,
        "risk_decision_id": order.intent.risk_decision_id,
        "idempotency_key": order.intent.idempotency_key,
        "execution_state": execution_state_as_str(order.execution_state),
        "status": order_status_as_str(order.status),
        "status_reason": order.status_reason,
    });

    sqlx::query(
        r#"
        INSERT INTO audit_logs (id, correlation_id, actor, action, target, metadata)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(order.intent.correlation_id)
    .bind(&actor.actor)
    .bind(action)
    .bind(format!("orders/{}", order.intent.order_id))
    .bind(metadata)
    .execute(&mut **tx)
    .await
    .map_err(anyhow::Error::from)
    .map_err(CreateOrderError::Unexpected)?;

    Ok(())
}
