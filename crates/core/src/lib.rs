use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

pub type Quantity = Decimal;
pub type Price = Decimal;
pub type Volume = Decimal;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Symbol(String);

impl Symbol {
    pub fn new(value: impl Into<String>) -> Result<Self, CoreError> {
        let value = value.into().trim().to_ascii_uppercase();
        if value.is_empty() {
            return Err(CoreError::EmptySymbol);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for Symbol {
    type Error = CoreError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataSource {
    Binance,
}

impl MarketDataSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Binance => "binance",
        }
    }
}

impl std::str::FromStr for MarketDataSource {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "binance" => Ok(Self::Binance),
            other => Err(CoreError::UnsupportedMarketDataSource(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FeedStatus {
    Connecting,
    Connected,
    Disconnected,
    Stale,
    Error,
}

impl FeedStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connecting => "connecting",
            Self::Connected => "connected",
            Self::Disconnected => "disconnected",
            Self::Stale => "stale",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DataFreshnessStatus {
    Fresh,
    Stale,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum CandleInterval {
    OneMinute,
}

impl CandleInterval {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OneMinute => "1m",
        }
    }

    pub fn duration(self) -> chrono::Duration {
        match self {
            Self::OneMinute => chrono::Duration::minutes(1),
        }
    }
}

impl std::str::FromStr for CandleInterval {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "1m" => Ok(Self::OneMinute),
            other => Err(CoreError::UnsupportedCandleInterval(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketTrade {
    pub trade_id: String,
    pub exchange: MarketDataSource,
    pub symbol: Symbol,
    pub price: Price,
    pub quantity: Quantity,
    pub trade_time: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub is_buyer_maker: Option<bool>,
    pub raw_payload: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketTick {
    pub id: Uuid,
    pub exchange: MarketDataSource,
    pub symbol: Symbol,
    pub price: Price,
    pub quantity: Quantity,
    pub trade_time: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub raw_payload: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Candle {
    pub id: Uuid,
    pub exchange: MarketDataSource,
    pub symbol: Symbol,
    pub interval: CandleInterval,
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: Volume,
    pub quote_volume: Option<Volume>,
    pub trade_count: i32,
    pub is_closed: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketFeedStatus {
    pub exchange: MarketDataSource,
    pub symbol: Symbol,
    pub status: FeedStatus,
    pub freshness_status: DataFreshnessStatus,
    pub last_event_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub reconnect_count: i32,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MarketMode {
    Paper,
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Money {
    pub amount: Decimal,
    pub currency: String,
}

impl Money {
    pub fn new(amount: Decimal, currency: impl Into<String>) -> Self {
        Self {
            amount,
            currency: currency.into().to_ascii_uppercase(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    pub signal_id: Uuid,
    pub correlation_id: Uuid,
    pub symbol: Symbol,
    pub side: Side,
    pub strength: Decimal,
    pub strategy_name: String,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskCheckContext {
    pub signal_id: Uuid,
    pub correlation_id: Uuid,
    pub strategy_id: String,
    pub symbol: Symbol,
    pub side: Side,
    pub suggested_notional: Decimal,
    pub signal_created_at: DateTime<Utc>,
    pub evaluated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskRuleDecision {
    Pass,
    Reject,
    Warn,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RiskRejectionReason {
    KillSwitchActive,
    MaxOpenPositionsExceeded,
    MaxDailyLossExceeded,
    SignalTooOld,
    DuplicateOrderDetected,
    DataStale,
    PositionNotionalExceeded,
    UnsupportedState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RiskRuleResult {
    pub rule_name: String,
    pub decision: RiskRuleDecision,
    pub reason: Option<RiskRejectionReason>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    pub max_open_positions: u32,
    pub max_daily_loss: Decimal,
    pub max_signal_age_secs: i64,
    pub max_position_notional: Decimal,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_open_positions: 2,
            max_daily_loss: Decimal::new(20_000, 0),
            max_signal_age_secs: 30,
            max_position_notional: Decimal::new(150_000, 0),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskEvaluationDecision {
    Approved,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskEvaluationResult {
    pub risk_decision_id: Uuid,
    pub decision: RiskEvaluationDecision,
    pub approved_notional: Option<Decimal>,
    pub risk_score: Decimal,
    pub reasons: Vec<RiskRejectionReason>,
    pub rule_results: Vec<RiskRuleResult>,
    pub correlation_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskDecision {
    Approved {
        decision_id: Uuid,
        rationale: String,
    },
    Rejected {
        decision_id: Uuid,
        rationale: String,
    },
    ManualReview {
        decision_id: Uuid,
        rationale: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Open,
    Rejected,
    Filled,
    Cancelled,
    Expired,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionState {
    IntentCreated,
    RiskApproved,
    OrderPrepared,
    PaperSubmitted,
    PaperFilled,
    PaperCancelled,
    Rejected,
    Expired,
}

impl ExecutionState {
    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::IntentCreated, Self::RiskApproved)
                | (Self::IntentCreated, Self::Rejected)
                | (Self::IntentCreated, Self::Expired)
                | (Self::RiskApproved, Self::OrderPrepared)
                | (Self::RiskApproved, Self::Rejected)
                | (Self::RiskApproved, Self::Expired)
                | (Self::OrderPrepared, Self::PaperSubmitted)
                | (Self::OrderPrepared, Self::PaperCancelled)
                | (Self::OrderPrepared, Self::Rejected)
                | (Self::OrderPrepared, Self::Expired)
                | (Self::PaperSubmitted, Self::PaperFilled)
                | (Self::PaperSubmitted, Self::PaperCancelled)
                | (Self::PaperSubmitted, Self::Expired)
        )
    }

    pub fn transition(self, next: Self) -> Result<Self, CoreError> {
        if self.can_transition_to(next) {
            Ok(next)
        } else {
            Err(CoreError::InvalidExecutionTransition {
                from: self,
                to: next,
            })
        }
    }

    pub fn as_event_name(self) -> &'static str {
        match self {
            Self::IntentCreated => "INTENT_CREATED",
            Self::RiskApproved => "RISK_APPROVED",
            Self::OrderPrepared => "ORDER_PREPARED",
            Self::PaperSubmitted => "PAPER_SUBMITTED",
            Self::PaperFilled => "PAPER_FILLED",
            Self::PaperCancelled => "PAPER_CANCELLED",
            Self::Rejected => "REJECTED",
            Self::Expired => "EXPIRED",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderIntent {
    pub order_id: Uuid,
    pub correlation_id: Uuid,
    pub risk_decision_id: Uuid,
    pub idempotency_key: String,
    pub symbol: Symbol,
    pub side: Side,
    pub quantity: Quantity,
    pub limit_price: Option<Price>,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

impl OrderIntent {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.idempotency_key.trim().is_empty() {
            return Err(CoreError::EmptyIdempotencyKey);
        }
        if self.quantity <= Decimal::ZERO {
            return Err(CoreError::InvalidOrderQuantity);
        }
        if let Some(limit_price) = self.limit_price {
            if limit_price <= Decimal::ZERO {
                return Err(CoreError::InvalidLimitPrice);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperOrder {
    pub intent: OrderIntent,
    pub status: OrderStatus,
    pub execution_state: ExecutionState,
    pub filled_price: Option<Price>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub filled_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub rejected_at: Option<DateTime<Utc>>,
    pub expired_at: Option<DateTime<Utc>>,
    pub status_reason: Option<String>,
    pub updated_at: DateTime<Utc>,
}

impl PaperOrder {
    pub fn new(intent: OrderIntent) -> Result<Self, CoreError> {
        intent.validate()?;

        Ok(Self {
            intent,
            status: OrderStatus::Open,
            execution_state: ExecutionState::IntentCreated,
            filled_price: None,
            submitted_at: None,
            filled_at: None,
            cancelled_at: None,
            rejected_at: None,
            expired_at: None,
            status_reason: None,
            updated_at: Utc::now(),
        })
    }

    pub fn transition_to(
        &mut self,
        next: ExecutionState,
        occurred_at: DateTime<Utc>,
        status_reason: Option<String>,
    ) -> Result<(), CoreError> {
        self.execution_state = self.execution_state.transition(next)?;
        self.updated_at = occurred_at;

        match next {
            ExecutionState::IntentCreated
            | ExecutionState::RiskApproved
            | ExecutionState::OrderPrepared => {
                self.status = OrderStatus::Open;
            }
            ExecutionState::PaperSubmitted => {
                self.status = OrderStatus::Open;
                self.submitted_at = Some(occurred_at);
            }
            ExecutionState::PaperFilled => {
                self.status = OrderStatus::Filled;
                self.filled_at = Some(occurred_at);
            }
            ExecutionState::PaperCancelled => {
                self.status = OrderStatus::Cancelled;
                self.cancelled_at = Some(occurred_at);
            }
            ExecutionState::Rejected => {
                self.status = OrderStatus::Rejected;
                self.rejected_at = Some(occurred_at);
            }
            ExecutionState::Expired => {
                self.status = OrderStatus::Expired;
                self.expired_at = Some(occurred_at);
            }
        }

        self.status_reason = status_reason;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: Uuid,
    pub correlation_id: Uuid,
    pub event_type: String,
    pub occurred_at: DateTime<Utc>,
    pub source: String,
    pub payload: Value,
}

impl EventEnvelope {
    pub fn new(
        event_type: impl Into<String>,
        correlation_id: Uuid,
        source: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            correlation_id,
            event_type: event_type.into(),
            occurred_at: Utc::now(),
            source: source.into(),
            payload,
        }
    }
}

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("symbol cannot be empty")]
    EmptySymbol,
    #[error("unsupported market data source: {0}")]
    UnsupportedMarketDataSource(String),
    #[error("unsupported candle interval: {0}")]
    UnsupportedCandleInterval(String),
    #[error("idempotency_key cannot be empty")]
    EmptyIdempotencyKey,
    #[error("quantity must be greater than zero")]
    InvalidOrderQuantity,
    #[error("limit_price must be greater than zero")]
    InvalidLimitPrice,
    #[error("market trade price must be greater than zero")]
    InvalidMarketTradePrice,
    #[error("market trade quantity must be greater than zero")]
    InvalidMarketTradeQuantity,
    #[error("invalid execution transition from {from:?} to {to:?}")]
    InvalidExecutionTransition {
        from: ExecutionState,
        to: ExecutionState,
    },
}

#[cfg(test)]
mod tests {
    use super::{ExecutionState, OrderIntent, PaperOrder, Side, Symbol};
    use chrono::Utc;
    use rust_decimal::Decimal;
    use uuid::Uuid;

    fn sample_intent() -> OrderIntent {
        OrderIntent {
            order_id: Uuid::new_v4(),
            correlation_id: Uuid::new_v4(),
            risk_decision_id: Uuid::new_v4(),
            idempotency_key: "paper-order-1".to_string(),
            symbol: Symbol::new("btcusdt").expect("valid symbol"),
            side: Side::Buy,
            quantity: Decimal::new(1, 0),
            limit_price: Some(Decimal::new(100_000, 0)),
            created_at: Utc::now(),
            expires_at: None,
        }
    }

    #[test]
    fn valid_execution_transitions_are_allowed() {
        let mut order = PaperOrder::new(sample_intent()).expect("order should be valid");
        let at = Utc::now();

        order
            .transition_to(ExecutionState::RiskApproved, at, None)
            .expect("intent -> approved");
        order
            .transition_to(ExecutionState::OrderPrepared, at, None)
            .expect("approved -> prepared");
        order
            .transition_to(ExecutionState::PaperSubmitted, at, None)
            .expect("prepared -> submitted");
        order
            .transition_to(ExecutionState::PaperFilled, at, None)
            .expect("submitted -> filled");
    }

    #[test]
    fn invalid_execution_transitions_are_rejected() {
        let mut order = PaperOrder::new(sample_intent()).expect("order should be valid");

        let err = order
            .transition_to(ExecutionState::PaperFilled, Utc::now(), None)
            .expect_err("intent cannot jump to filled");

        assert!(matches!(
            err,
            super::CoreError::InvalidExecutionTransition {
                from: ExecutionState::IntentCreated,
                to: ExecutionState::PaperFilled,
            }
        ));
    }
}
