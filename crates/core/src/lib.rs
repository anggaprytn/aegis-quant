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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum StrategyId {
    MomentumV1,
    VolatilityBreakoutV1,
}

impl StrategyId {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MomentumV1 => "momentum_v1",
            Self::VolatilityBreakoutV1 => "volatility_breakout_v1",
        }
    }
}

impl std::str::FromStr for StrategyId {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "momentum_v1" => Ok(Self::MomentumV1),
            "volatility_breakout_v1" => Ok(Self::VolatilityBreakoutV1),
            other => Err(CoreError::UnsupportedStrategyId(other.to_string())),
        }
    }
}

impl std::fmt::Display for StrategyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StrategyStatus {
    Enabled,
    Disabled,
}

impl StrategyStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
        }
    }
}

impl std::str::FromStr for StrategyStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "enabled" => Ok(Self::Enabled),
            "disabled" => Ok(Self::Disabled),
            other => Err(CoreError::UnsupportedStrategyStatus(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StrategyMode {
    SignalOnly,
}

impl StrategyMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SignalOnly => "signal_only",
        }
    }
}

impl std::str::FromStr for StrategyMode {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "signal_only" => Ok(Self::SignalOnly),
            other => Err(CoreError::UnsupportedStrategyMode(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SignalSide {
    Buy,
    Sell,
}

impl SignalSide {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

impl std::str::FromStr for SignalSide {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "BUY" => Ok(Self::Buy),
            "SELL" => Ok(Self::Sell),
            other => Err(CoreError::UnsupportedSignalSide(other.to_string())),
        }
    }
}

impl From<SignalSide> for Side {
    fn from(value: SignalSide) -> Self {
        match value {
            SignalSide::Buy => Side::Buy,
            SignalSide::Sell => Side::Sell,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SignalReason {
    ThreeConsecutiveHigherCloses,
    MomentumHigherCloses,
    BreakoutAboveRecentHigh,
    ConditionsNotMet,
    InsufficientHistory,
    StrategyDisabled,
}

impl SignalReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ThreeConsecutiveHigherCloses => "three_consecutive_higher_closes",
            Self::MomentumHigherCloses => "momentum_higher_closes",
            Self::BreakoutAboveRecentHigh => "breakout_above_recent_high",
            Self::ConditionsNotMet => "conditions_not_met",
            Self::InsufficientHistory => "insufficient_history",
            Self::StrategyDisabled => "strategy_disabled",
        }
    }
}

impl std::str::FromStr for SignalReason {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "three_consecutive_higher_closes" => Ok(Self::ThreeConsecutiveHigherCloses),
            "momentum_higher_closes" => Ok(Self::MomentumHigherCloses),
            "breakout_above_recent_high" => Ok(Self::BreakoutAboveRecentHigh),
            "conditions_not_met" => Ok(Self::ConditionsNotMet),
            "insufficient_history" => Ok(Self::InsufficientHistory),
            "strategy_disabled" => Ok(Self::StrategyDisabled),
            other => Err(CoreError::UnsupportedSignalReason(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignalConfidence {
    pub value: Decimal,
}

impl SignalConfidence {
    pub fn new(value: Decimal) -> Result<Self, CoreError> {
        if value < Decimal::ZERO || value > Decimal::ONE {
            return Err(CoreError::InvalidSignalConfidence(value.to_string()));
        }

        Ok(Self { value })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyConfig {
    pub strategy_id: StrategyId,
    pub status: StrategyStatus,
    pub mode: StrategyMode,
    pub symbols: Vec<Symbol>,
    pub timeframe: CandleInterval,
    pub suggested_notional: Decimal,
    pub momentum_lookback_candles: u32,
    pub breakout_lookback_candles: u32,
    pub stop_loss_pct: Option<Decimal>,
    pub take_profit_pct: Option<Decimal>,
}

impl StrategyConfig {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.suggested_notional <= Decimal::ZERO {
            return Err(CoreError::InvalidStrategyNotional);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategySignal {
    pub signal_id: Uuid,
    pub strategy_id: StrategyId,
    pub symbol: Symbol,
    pub side: SignalSide,
    pub confidence: SignalConfidence,
    pub timeframe: CandleInterval,
    pub reason: SignalReason,
    pub suggested_notional: Decimal,
    pub stop_loss_pct: Option<Decimal>,
    pub take_profit_pct: Option<Decimal>,
    pub source_candle_open_time: DateTime<Utc>,
    pub correlation_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyEvaluationContext {
    pub correlation_id: Uuid,
    pub strategy_id: StrategyId,
    pub symbol: Symbol,
    pub config: StrategyConfig,
    pub candles: Vec<Candle>,
    pub evaluated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyEvaluationResult {
    pub strategy_id: StrategyId,
    pub symbol: Symbol,
    pub timeframe: CandleInterval,
    pub generated: bool,
    pub reason: SignalReason,
    pub signal: Option<StrategySignal>,
    pub correlation_id: Uuid,
    pub evaluated_at: DateTime<Utc>,
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

pub type Signal = StrategySignal;

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
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PipelineDecision {
    NoSignal,
    RiskRejected,
    PaperOrderCreated,
    PaperOrderReused,
    StrategyDisabled,
    SafetyStopped,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PipelineStepStatus {
    NotStarted,
    Completed,
    Skipped,
    Rejected,
    Reused,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PipelineRejectionReason {
    ConditionsNotMet,
    InsufficientHistory,
    StrategyDisabled,
    KillSwitchActive,
    SignalTooOld,
    DataStale,
    MarketFeedUnavailable,
    MarketFeedDegraded,
    UnsupportedTimeframe,
    UnsupportedState,
}

impl PipelineRejectionReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ConditionsNotMet => "conditions_not_met",
            Self::InsufficientHistory => "insufficient_history",
            Self::StrategyDisabled => "strategy_disabled",
            Self::KillSwitchActive => "kill_switch_active",
            Self::SignalTooOld => "signal_too_old",
            Self::DataStale => "data_stale",
            Self::MarketFeedUnavailable => "market_feed_unavailable",
            Self::MarketFeedDegraded => "market_feed_degraded",
            Self::UnsupportedTimeframe => "unsupported_timeframe",
            Self::UnsupportedState => "unsupported_state",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrderIntentSource {
    StrategySignal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StrategyRiskExecutionTrace {
    pub strategy_evaluation: PipelineStepStatus,
    pub signal: PipelineStepStatus,
    pub risk_evaluation: PipelineStepStatus,
    pub paper_order: PipelineStepStatus,
    pub order_intent_source: Option<OrderIntentSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaperTradingPipelineRequest {
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperTradingPipelineResult {
    pub pipeline_decision: PipelineDecision,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub signal_generated: bool,
    pub signal_reused: bool,
    pub signal_id: Option<Uuid>,
    pub risk_decision_id: Option<Uuid>,
    pub paper_order_id: Option<Uuid>,
    pub execution_state: Option<String>,
    pub reasons: Vec<String>,
    pub correlation_id: Uuid,
    pub trace: StrategyRiskExecutionTrace,
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
    #[error("unsupported strategy id: {0}")]
    UnsupportedStrategyId(String),
    #[error("unsupported strategy status: {0}")]
    UnsupportedStrategyStatus(String),
    #[error("unsupported strategy mode: {0}")]
    UnsupportedStrategyMode(String),
    #[error("unsupported signal side: {0}")]
    UnsupportedSignalSide(String),
    #[error("unsupported signal reason: {0}")]
    UnsupportedSignalReason(String),
    #[error("idempotency_key cannot be empty")]
    EmptyIdempotencyKey,
    #[error("quantity must be greater than zero")]
    InvalidOrderQuantity,
    #[error("limit_price must be greater than zero")]
    InvalidLimitPrice,
    #[error("strategy suggested notional must be greater than zero")]
    InvalidStrategyNotional,
    #[error("signal confidence must be between 0 and 1: {0}")]
    InvalidSignalConfidence(String),
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
