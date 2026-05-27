use aegis_core::{
    calculate_average_duration_seconds, calculate_strategy_rejection_rate,
    calculate_strategy_win_rate, calculate_testnet_promotion_rate,
    combine_strategy_performance_summaries, BacktestConfig, BacktestEquityPoint, BacktestResult,
    BacktestTrade, Candle, CandleBackfillProgress, CandleBackfillRequest, CandleBackfillResult,
    CandleBackfillStatus, CandleInterval, DataFreshnessStatus, EventEnvelope,
    ExchangeReconciliationStatus, ExecutionReadinessBlockingReason, ExecutionReadinessCheck,
    ExecutionReadinessRecommendation, ExecutionReadinessSnapshot, ExecutionReadinessStatus,
    ExecutionReadinessTarget, ExecutionState, FeedStatus, MarketCandleCoverageSummary,
    MarketCandleIntervalCoverage, MarketDataSource, MarketTick, OrderIntent, OrderStatus,
    PaperAccount, PaperAccountStatus, PaperClosePositionResult, PaperCloseStatus,
    PaperEquitySnapshot, PaperFill, PaperOrder, PaperPosition, PaperPositionCloseSummary,
    PaperPositionStatusFilter, PaperPriceStatus, PaperTradeJournalEntry, PositionSide,
    PositionStatus, ReplayRunStatus, RiskCheckContext, RiskConfig, RiskConfigAuditEntry,
    RiskConfigVersion, RiskEvaluationDecision, RiskEvaluationResult, Session, Side, SignalReason,
    StrategyComparisonSummary, StrategyConfig, StrategyConfigAuditEntry, StrategyConfigVersion,
    StrategyDecisionBreakdown, StrategyExperimentCandidate, StrategyExperimentComparison,
    StrategyExperimentResult, StrategyExperimentRun, StrategyId, StrategyPerformanceMode,
    StrategyPerformanceRequest, StrategyPerformanceSummary, StrategyPnlBreakdown,
    StrategyRiskBreakdown, StrategySignal, StrategyStatus, StrategyWalkForwardResult,
    StrategyWalkForwardRobustnessSummary, StrategyWalkForwardWindow,
    StrategyWalkForwardWindowResult, Symbol, TestnetExecutionState,
    TestnetPromotionDropoffBreakdown, TestnetPromotionFunnelRequest, TestnetPromotionFunnelRow,
    TestnetPromotionFunnelStage, TestnetPromotionFunnelSummary, TestnetPromotionLifecycleBreakdown,
    TestnetPromotionOutcomeBreakdown, TestnetPromotionQualitySignal, TestnetShadowDecision,
    TestnetShadowIntent, TestnetShadowPromotionPreview, TestnetShadowPromotionRejectionReason,
    TestnetShadowPromotionStatus, TestnetShadowRejectionReason, TestnetShadowRunResult,
    TestnetShadowRunnerConfig, TestnetShadowRunnerStaleFeedPolicy, TestnetShadowRunnerState,
    TestnetShadowRunnerStatus, TestnetShadowStatus, User, UserRole, UserStatus,
};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, Postgres, QueryBuilder, Row, Transaction};
use std::collections::BTreeMap;
use thiserror::Error;
use uuid::Uuid;

pub const MIGRATIONS_DIR: &str = "crates/db/migrations";
const GLOBAL_SYSTEM_STATE_KEY: &str = "global";
const GLOBAL_RISK_CONFIG_KEY: &str = "global";
pub const TESTNET_SHADOW_RUNNER_CONFIG_ID: Uuid =
    Uuid::from_u128(0x0180_0000_0000_0000_0000_0000_0000_0001);
pub const TESTNET_SHADOW_RUNNER_STATE_ID: Uuid =
    Uuid::from_u128(0x0180_0000_0000_0000_0000_0000_0000_0002);
pub use sqlx::PgPool;
mod research;
pub use research::*;
pub mod test_support;

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
pub struct UserRecord {
    pub id: Uuid,
    pub email: String,
    pub password_hash: String,
    pub role: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_login_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecord {
    pub id: Uuid,
    pub user_id: Uuid,
    pub refresh_token_hash: String,
    pub expires_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub user_agent: Option<String>,
    pub ip_address: Option<String>,
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
    pub approved_notional: Option<Decimal>,
    pub risk_score: Option<Decimal>,
    pub reasons: Vec<String>,
    pub strategy_id: Option<String>,
    pub symbol: Option<String>,
    pub rationale: String,
    pub created_at: DateTime<Utc>,
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
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub signal_id: Option<Uuid>,
    pub strategy_id: Option<String>,
    pub requested_notional: Option<sqlx::types::Decimal>,
    pub filled_qty: sqlx::types::Decimal,
    pub avg_fill_price: Option<sqlx::types::Decimal>,
    pub mode: String,
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
pub struct ExchangeTestnetOrderRecord {
    pub id: Uuid,
    pub exchange: String,
    pub environment: String,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub time_in_force: Option<String>,
    pub requested_qty: Option<Decimal>,
    pub requested_notional: Option<Decimal>,
    pub limit_price: Option<Decimal>,
    pub status: String,
    pub execution_state: String,
    pub ack_payload: Option<Value>,
    pub latest_status_payload: Option<Value>,
    pub risk_decision_id: Option<Uuid>,
    pub created_by: Option<Uuid>,
    pub last_transition_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeTestnetOrderLifecycleEventRecord {
    pub id: Uuid,
    pub order_id: Option<Uuid>,
    pub client_order_id: String,
    pub previous_state: Option<String>,
    pub next_state: String,
    pub transition_source: String,
    pub reason: Option<String>,
    pub payload: Option<Value>,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeTestnetRepairActionRecord {
    pub id: Uuid,
    pub client_order_id: String,
    pub action: String,
    pub status: String,
    pub previous_state: Option<String>,
    pub next_state: Option<String>,
    pub reason: Option<String>,
    pub payload: Option<Value>,
    pub actor_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangePrivateStreamEventRecord {
    pub id: Uuid,
    pub exchange: String,
    pub environment: String,
    pub event_type: String,
    pub symbol: Option<String>,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub execution_type: Option<String>,
    pub order_status: Option<String>,
    pub payload: Value,
    pub event_time: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangePrivateStreamStateRecord {
    pub exchange: String,
    pub environment: String,
    pub status: String,
    pub listen_key_hash: Option<String>,
    pub connected_at: Option<DateTime<Utc>>,
    pub last_event_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub reconnect_count: i32,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeReconciliationRunRecord {
    pub id: Uuid,
    pub exchange: String,
    pub environment: String,
    pub status: String,
    pub checked_orders: i32,
    pub matched_orders: i32,
    pub mismatched_orders: i32,
    pub unknown_orders: i32,
    pub failed_reason: Option<String>,
    pub correlation_id: Uuid,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeReconciliationMismatchRecord {
    pub id: Uuid,
    pub run_id: Uuid,
    pub client_order_id: String,
    pub local_status: Option<String>,
    pub exchange_status: Option<String>,
    pub mismatch_kind: String,
    pub action: String,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReadinessSnapshotRecord {
    pub id: Uuid,
    pub target: String,
    pub status: String,
    pub score: i32,
    pub blocking_reasons: Value,
    pub warnings: Value,
    pub checks: Value,
    pub recommendations: Value,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestnetShadowRunRecord {
    pub id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub decision: String,
    pub signal_id: Option<Uuid>,
    pub risk_decision_id: Option<Uuid>,
    pub would_submit_payload: Option<Value>,
    pub price_source: Option<String>,
    pub resolved_price: Option<Decimal>,
    pub reasons: Vec<String>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestnetShadowPromotionRecord {
    pub id: Uuid,
    pub shadow_run_id: Uuid,
    pub status: String,
    pub strategy_id: Option<String>,
    pub symbol: Option<String>,
    pub timeframe: Option<String>,
    pub signal_id: Option<Uuid>,
    pub risk_decision_id: Option<Uuid>,
    pub would_submit_payload: Value,
    pub resolved_price: Option<Decimal>,
    pub price_source: Option<String>,
    pub rejection_reasons: Vec<String>,
    pub testnet_order_id: Option<Uuid>,
    pub client_order_id: Option<String>,
    pub expires_at: DateTime<Utc>,
    pub created_by: Option<Uuid>,
    pub submitted_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestnetShadowRunnerConfigRecord {
    pub id: Uuid,
    pub enabled: bool,
    pub interval_seconds: i32,
    pub strategies: Value,
    pub symbols: Value,
    pub timeframe: String,
    pub max_runs_per_tick: i32,
    pub stale_feed_policy: String,
    pub notes: Option<String>,
    pub updated_by: Option<Uuid>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestnetShadowRunnerStateRecord {
    pub id: Uuid,
    pub status: String,
    pub last_tick_at: Option<DateTime<Utc>>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub total_ticks: i64,
    pub total_runs: i64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperAccountRecord {
    pub id: Uuid,
    pub name: String,
    pub base_currency: String,
    pub initial_equity: Decimal,
    pub current_equity: Decimal,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperPositionRecord {
    pub id: Uuid,
    pub account_id: Uuid,
    pub symbol: String,
    pub side: String,
    pub quantity: Decimal,
    pub entry_price: Decimal,
    pub mark_price: Option<Decimal>,
    pub price_status: String,
    pub notional: Decimal,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub status: String,
    pub opened_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    pub strategy_id: Option<String>,
    pub signal_id: Option<Uuid>,
    pub risk_decision_id: Option<Uuid>,
    pub order_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperFillRecord {
    pub id: Uuid,
    pub account_id: Uuid,
    pub order_id: Uuid,
    pub position_id: Option<Uuid>,
    pub symbol: String,
    pub side: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub notional: Decimal,
    pub fee: Decimal,
    pub slippage_cost: Decimal,
    pub filled_at: DateTime<Utc>,
    pub strategy_id: Option<String>,
    pub signal_id: Option<Uuid>,
    pub risk_decision_id: Option<Uuid>,
    pub correlation_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperEquitySnapshotRecord {
    pub id: Uuid,
    pub account_id: Uuid,
    pub equity: Decimal,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub drawdown_pct: Decimal,
    pub snapshot_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperTradeJournalRecord {
    pub id: Uuid,
    pub account_id: Uuid,
    pub position_id: Option<Uuid>,
    pub order_id: Option<Uuid>,
    pub event_type: String,
    pub symbol: Option<String>,
    pub pnl: Option<Decimal>,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Uuid,
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
pub struct PaperCloseArtifacts {
    pub risk_decision_id: Uuid,
    pub order_id: Uuid,
    pub fill_id: Uuid,
    pub journal_entry_id: Uuid,
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
pub struct CandleBackfillRunRecord {
    pub id: Uuid,
    pub exchange: String,
    pub symbol: String,
    pub interval: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub status: String,
    pub requested_candles_estimate: i32,
    pub fetched_candles: i32,
    pub inserted_candles: i32,
    pub updated_candles: i32,
    pub skipped_candles: i32,
    pub failed_reason: Option<String>,
    pub correlation_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub config: Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CandleUpsertBatchResult {
    pub inserted_candles: i32,
    pub updated_candles: i32,
    pub skipped_candles: i32,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfigRecord {
    pub strategy_id: String,
    pub enabled: bool,
    pub mode: String,
    pub symbols: String,
    pub timeframe: String,
    pub suggested_notional: Decimal,
    pub max_signal_age_ms: i64,
    pub cooldown_seconds: i32,
    pub lookback_candles: i32,
    pub trend_lookback_candles: Option<i32>,
    pub momentum_lookback_candles: Option<i32>,
    pub breakout_lookback_candles: Option<i32>,
    pub confidence_floor: Option<Decimal>,
    pub stop_loss_pct: Option<Decimal>,
    pub take_profit_pct: Option<Decimal>,
    pub holding_candles: Option<i32>,
    pub notes: Option<String>,
    pub current_version: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfigVersionRecord {
    pub id: Uuid,
    pub strategy_id: String,
    pub version: i32,
    pub config: Value,
    pub actor_id: Option<Uuid>,
    pub correlation_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfigAuditRecord {
    pub id: Uuid,
    pub strategy_id: String,
    pub version: Option<i32>,
    pub old_config: Option<Value>,
    pub new_config: Option<Value>,
    pub validation_issues: Value,
    pub actor_id: Option<Uuid>,
    pub correlation_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfigRecord {
    pub config_key: String,
    pub config_id: Uuid,
    pub max_open_positions: i32,
    pub max_daily_loss_pct: Decimal,
    pub max_weekly_loss_pct: Decimal,
    pub max_position_notional: Decimal,
    pub max_slippage_pct: Decimal,
    pub max_consecutive_losses: i32,
    pub cooldown_seconds: i32,
    pub max_signal_age_ms: i64,
    pub stale_feed_threshold_seconds: i32,
    pub current_version: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfigVersionRecord {
    pub id: Uuid,
    pub config_key: String,
    pub config_id: Uuid,
    pub version: i32,
    pub config: Value,
    pub actor_id: Option<Uuid>,
    pub correlation_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfigAuditRecord {
    pub id: Uuid,
    pub config_key: String,
    pub config_id: Uuid,
    pub version: Option<i32>,
    pub old_config: Option<Value>,
    pub new_config: Option<Value>,
    pub validation_issues: Value,
    pub actor_id: Option<Uuid>,
    pub correlation_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyStateRecord {
    pub strategy_id: String,
    pub last_evaluated_at: Option<DateTime<Utc>>,
    pub last_evaluation_reason: Option<String>,
    pub last_signal_id: Option<Uuid>,
    pub last_signal_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalRecord {
    pub id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub side: String,
    pub confidence: Decimal,
    pub timeframe: String,
    pub reason: String,
    pub suggested_notional: Decimal,
    pub stop_loss_pct: Option<Decimal>,
    pub take_profit_pct: Option<Decimal>,
    pub source_candle_open_time: DateTime<Utc>,
    pub correlation_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyStatusRecord {
    pub config: StrategyConfigRecord,
    pub state: Option<StrategyStateRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestRunRecord {
    pub id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub initial_capital: Decimal,
    pub final_equity: Decimal,
    pub pnl: Decimal,
    pub pnl_pct: Decimal,
    pub max_drawdown_pct: Decimal,
    pub win_rate: Decimal,
    pub trade_count: i32,
    pub winning_trades: i32,
    pub losing_trades: i32,
    pub avg_win: Decimal,
    pub avg_loss: Decimal,
    pub fee_paid: Decimal,
    pub slippage_cost: Decimal,
    pub status: String,
    pub config: Value,
    pub correlation_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestTradeRecord {
    pub id: Uuid,
    pub run_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub side: String,
    pub entry_time: DateTime<Utc>,
    pub entry_price: Decimal,
    pub exit_time: Option<DateTime<Utc>>,
    pub exit_price: Option<Decimal>,
    pub quantity: Decimal,
    pub notional: Decimal,
    pub fee_paid: Decimal,
    pub slippage_cost: Decimal,
    pub realized_pnl: Decimal,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestEquityPointRecord {
    pub id: Uuid,
    pub run_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub equity: Decimal,
    pub drawdown_pct: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyExperimentRecord {
    pub id: Uuid,
    pub experiment_group_id: Option<Uuid>,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub initial_capital: Decimal,
    pub fee_bps: Decimal,
    pub slippage_bps: Decimal,
    pub max_signal_age_ms: Option<i64>,
    pub max_runs: Option<i32>,
    pub status: String,
    pub comparison: Value,
    pub candle_count: Option<i32>,
    pub warnings: Value,
    pub skipped_reason: Option<String>,
    pub correlation_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyExperimentRunRecord {
    pub id: Uuid,
    pub experiment_id: Uuid,
    pub rank: i32,
    pub candidate_config: Value,
    pub final_equity: Decimal,
    pub pnl: Decimal,
    pub pnl_pct: Decimal,
    pub max_drawdown_pct: Decimal,
    pub win_rate: Decimal,
    pub trade_count: i32,
    pub fee_paid: Decimal,
    pub slippage_cost: Decimal,
    pub fee_slippage_drag_pct: Decimal,
    pub score: Decimal,
    pub status: String,
    pub warnings: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyWalkForwardRunRecord {
    pub id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub request: Value,
    pub status: String,
    pub total_windows: i32,
    pub completed_windows: i32,
    pub skipped_windows: i32,
    pub profitable_test_windows: i32,
    pub losing_test_windows: i32,
    pub avg_test_pnl_pct: Decimal,
    pub median_test_pnl_pct: Decimal,
    pub worst_test_pnl_pct: Decimal,
    pub best_test_pnl_pct: Decimal,
    pub avg_max_drawdown_pct: Decimal,
    pub robustness_score: Decimal,
    pub robustness_summary: Value,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyWalkForwardWindowRecord {
    pub id: Uuid,
    pub walk_forward_id: Uuid,
    pub window_index: i32,
    pub train_start: DateTime<Utc>,
    pub train_end: DateTime<Utc>,
    pub test_start: DateTime<Utc>,
    pub test_end: DateTime<Utc>,
    pub status: String,
    pub skip_reason: Option<String>,
    pub trade_count: i32,
    pub pnl: Decimal,
    pub pnl_pct: Decimal,
    pub max_drawdown_pct: Decimal,
    pub win_rate: Decimal,
    pub fee_paid: Decimal,
    pub slippage_cost: Decimal,
    pub result: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct InsertSignalOutcome {
    pub signal: SignalRecord,
    pub inserted: bool,
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

pub async fn count_users(pool: &PgPool) -> Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM users")
        .fetch_one(pool)
        .await?)
}

pub async fn get_user_by_email(pool: &PgPool, email: &str) -> Result<Option<UserRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            email,
            password_hash,
            role,
            status,
            created_at,
            updated_at,
            last_login_at
        FROM users
        WHERE lower(email) = lower($1)
        "#,
    )
    .bind(email)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| map_user(&row)))
}

pub async fn get_user_by_id(pool: &PgPool, user_id: Uuid) -> Result<Option<UserRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            email,
            password_hash,
            role,
            status,
            created_at,
            updated_at,
            last_login_at
        FROM users
        WHERE id = $1
        "#,
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| map_user(&row)))
}

pub async fn insert_user(
    pool: &PgPool,
    id: Uuid,
    email: &str,
    password_hash: &str,
    role: UserRole,
    status: UserStatus,
) -> Result<UserRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO users (
            id,
            email,
            password_hash,
            role,
            status,
            created_at,
            updated_at
        )
        VALUES ($1, lower($2), $3, $4, $5, NOW(), NOW())
        RETURNING
            id,
            email,
            password_hash,
            role,
            status,
            created_at,
            updated_at,
            last_login_at
        "#,
    )
    .bind(id)
    .bind(email)
    .bind(password_hash)
    .bind(role.as_str())
    .bind(status.as_str())
    .fetch_one(pool)
    .await?;

    Ok(map_user(&row))
}

pub async fn update_user_last_login(
    pool: &PgPool,
    user_id: Uuid,
    logged_in_at: DateTime<Utc>,
) -> Result<UserRecord> {
    let row = sqlx::query(
        r#"
        UPDATE users
        SET
            last_login_at = $2,
            updated_at = NOW()
        WHERE id = $1
        RETURNING
            id,
            email,
            password_hash,
            role,
            status,
            created_at,
            updated_at,
            last_login_at
        "#,
    )
    .bind(user_id)
    .bind(logged_in_at)
    .fetch_one(pool)
    .await?;

    Ok(map_user(&row))
}

pub async fn insert_session(
    pool: &PgPool,
    id: Uuid,
    user_id: Uuid,
    refresh_token_hash: &str,
    expires_at: DateTime<Utc>,
    user_agent: Option<&str>,
    ip_address: Option<&str>,
) -> Result<SessionRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO sessions (
            id,
            user_id,
            refresh_token_hash,
            expires_at,
            user_agent,
            ip_address,
            created_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
        RETURNING
            id,
            user_id,
            refresh_token_hash,
            expires_at,
            revoked_at,
            created_at,
            updated_at,
            user_agent,
            ip_address
        "#,
    )
    .bind(id)
    .bind(user_id)
    .bind(refresh_token_hash)
    .bind(expires_at)
    .bind(user_agent)
    .bind(ip_address)
    .fetch_one(pool)
    .await?;

    Ok(map_session(&row))
}

pub async fn get_session_by_id(pool: &PgPool, session_id: Uuid) -> Result<Option<SessionRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            user_id,
            refresh_token_hash,
            expires_at,
            revoked_at,
            created_at,
            updated_at,
            user_agent,
            ip_address
        FROM sessions
        WHERE id = $1
        "#,
    )
    .bind(session_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| map_session(&row)))
}

pub async fn get_session_by_id_and_hash(
    pool: &PgPool,
    session_id: Uuid,
    refresh_token_hash: &str,
) -> Result<Option<SessionRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            user_id,
            refresh_token_hash,
            expires_at,
            revoked_at,
            created_at,
            updated_at,
            user_agent,
            ip_address
        FROM sessions
        WHERE id = $1
          AND refresh_token_hash = $2
        "#,
    )
    .bind(session_id)
    .bind(refresh_token_hash)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| map_session(&row)))
}

pub async fn rotate_session_refresh_token(
    pool: &PgPool,
    session_id: Uuid,
    current_refresh_token_hash: &str,
    next_refresh_token_hash: &str,
    expires_at: DateTime<Utc>,
    user_agent: Option<&str>,
    ip_address: Option<&str>,
) -> Result<Option<SessionRecord>> {
    let row = sqlx::query(
        r#"
        UPDATE sessions
        SET
            refresh_token_hash = $3,
            expires_at = $4,
            updated_at = NOW(),
            user_agent = COALESCE($5, user_agent),
            ip_address = COALESCE($6, ip_address)
        WHERE id = $1
          AND refresh_token_hash = $2
          AND revoked_at IS NULL
        RETURNING
            id,
            user_id,
            refresh_token_hash,
            expires_at,
            revoked_at,
            created_at,
            updated_at,
            user_agent,
            ip_address
        "#,
    )
    .bind(session_id)
    .bind(current_refresh_token_hash)
    .bind(next_refresh_token_hash)
    .bind(expires_at)
    .bind(user_agent)
    .bind(ip_address)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| map_session(&row)))
}

pub async fn revoke_session(
    pool: &PgPool,
    session_id: Uuid,
    revoked_at: DateTime<Utc>,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE sessions
        SET
            revoked_at = COALESCE(revoked_at, $2),
            updated_at = NOW()
        WHERE id = $1
        "#,
    )
    .bind(session_id)
    .bind(revoked_at)
    .execute(pool)
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

pub async fn insert_audit_log(
    pool: &PgPool,
    correlation_id: Uuid,
    actor: &StateActor,
    action: &str,
    target: &str,
    metadata: &Value,
) -> Result<()> {
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
    .bind(target)
    .bind(metadata)
    .execute(pool)
    .await?;

    Ok(())
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
    let open_positions_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM paper_positions
        WHERE status = 'OPEN'
        "#,
    )
    .fetch_one(pool)
    .await
    .ok()
    .and_then(|count| u32::try_from(count).ok());
    let current_equity = sqlx::query_scalar::<_, Option<Decimal>>(
        r#"
        SELECT current_equity
        FROM paper_accounts
        ORDER BY updated_at DESC
        LIMIT 1
        "#,
    )
    .fetch_one(pool)
    .await
    .ok()
    .flatten();
    let daily_realized_loss = sqlx::query_scalar::<_, Option<Decimal>>(
        r#"
        SELECT ABS(COALESCE(SUM(pnl), 0))
        FROM paper_trade_journal
        WHERE pnl < 0
          AND created_at >= date_trunc('day', NOW())
        "#,
    )
    .fetch_one(pool)
    .await
    .ok()
    .flatten();
    let weekly_realized_loss = sqlx::query_scalar::<_, Option<Decimal>>(
        r#"
        SELECT ABS(COALESCE(SUM(pnl), 0))
        FROM paper_trade_journal
        WHERE pnl < 0
          AND created_at >= date_trunc('week', NOW())
        "#,
    )
    .fetch_one(pool)
    .await
    .ok()
    .flatten();
    let recent_pnls = sqlx::query_scalar::<_, Decimal>(
        r#"
        SELECT pnl
        FROM paper_trade_journal
        WHERE pnl IS NOT NULL
        ORDER BY created_at DESC
        LIMIT 20
        "#,
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    let consecutive_losses = recent_pnls
        .iter()
        .take_while(|pnl| **pnl < Decimal::ZERO)
        .count();
    let last_trade_at = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
        r#"
        SELECT MAX(COALESCE(filled_at, submitted_at, created_at))
        FROM orders
        WHERE execution_state IN ('PAPER_CREATED', 'PAPER_FILLED')
        "#,
    )
    .fetch_one(pool)
    .await
    .ok()
    .flatten();

    let daily_loss_pct = match (daily_realized_loss, current_equity) {
        (Some(loss), Some(equity)) if equity > Decimal::ZERO => {
            Some((loss / equity) * Decimal::new(100, 0))
        }
        _ => None,
    };
    let weekly_loss_pct = match (weekly_realized_loss, current_equity) {
        (Some(loss), Some(equity)) if equity > Decimal::ZERO => {
            Some((loss / equity) * Decimal::new(100, 0))
        }
        _ => None,
    };

    Ok(risk_engine::RiskStateSnapshot {
        kill_switch_enabled: system_state.kill_switch_enabled,
        kill_switch_reason: system_state.kill_switch_reason,
        open_positions_count,
        daily_loss_pct,
        weekly_loss_pct,
        consecutive_losses: u32::try_from(consecutive_losses).ok(),
        latest_market_data_at,
        last_trade_at,
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

pub async fn insert_risk_decision(
    pool: &PgPool,
    source: &str,
    context: &RiskCheckContext,
    evaluation: &RiskEvaluationResult,
) -> Result<RiskDecisionRecord> {
    insert_risk_evaluation(pool, source, context, evaluation).await
}

pub async fn get_risk_decision(
    pool: &PgPool,
    risk_decision_id: Uuid,
) -> Result<Option<RiskDecisionRecord>> {
    get_risk_decision_by_id(pool, risk_decision_id).await
}

pub async fn get_risk_decision_by_id(
    pool: &PgPool,
    risk_decision_id: Uuid,
) -> Result<Option<RiskDecisionRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            rd.id,
            rd.correlation_id,
            rd.signal_id,
            rd.decision,
            rd.rationale,
            rd.decided_at,
            COALESCE(s.strategy_id, rd.rationale::jsonb ->> 'strategy_id') AS strategy_id,
            COALESCE(s.symbol, rd.rationale::jsonb ->> 'symbol') AS symbol
        FROM risk_decisions rd
        LEFT JOIN signals s ON s.id = rd.signal_id
        WHERE rd.id = $1
        "#,
    )
    .bind(risk_decision_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_risk_decision))
}

pub async fn list_recent_risk_decisions_filtered(
    pool: &PgPool,
    symbol: Option<&str>,
    limit: i64,
) -> Result<Vec<RiskDecisionRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            rd.id,
            rd.correlation_id,
            rd.signal_id,
            rd.decision,
            rd.rationale,
            rd.decided_at,
            COALESCE(s.strategy_id, rd.rationale::jsonb ->> 'strategy_id') AS strategy_id,
            COALESCE(s.symbol, rd.rationale::jsonb ->> 'symbol') AS symbol
        FROM risk_decisions rd
        LEFT JOIN signals s ON s.id = rd.signal_id
        WHERE ($1::text IS NULL OR COALESCE(s.symbol, rd.rationale::jsonb ->> 'symbol') = $1)
        ORDER BY rd.decided_at DESC
        LIMIT $2
        "#,
    )
    .bind(symbol)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_risk_decision).collect())
}

pub async fn list_recent_risk_decisions(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<RiskDecisionRecord>> {
    list_recent_risk_decisions_filtered(pool, None, limit).await
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
            o.id,
            o.correlation_id,
            o.risk_decision_id,
            o.idempotency_key,
            o.symbol,
            o.side,
            o.quantity,
            o.limit_price,
            o.market_mode,
            o.status,
            o.execution_state,
            o.status_reason,
            o.filled_price,
            o.submitted_at,
            o.filled_at,
            o.cancelled_at,
            o.rejected_at,
            o.expired_at,
            o.expires_at,
            o.created_at,
            o.updated_at,
            rd.signal_id,
            COALESCE(s.strategy_id, rd.rationale::jsonb ->> 'strategy_id') AS strategy_id,
            rd.rationale AS risk_rationale
        FROM orders o
        LEFT JOIN risk_decisions rd ON rd.id = o.risk_decision_id
        LEFT JOIN signals s ON s.id = rd.signal_id
        ORDER BY o.created_at DESC
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
            o.id,
            o.correlation_id,
            o.risk_decision_id,
            o.idempotency_key,
            o.symbol,
            o.side,
            o.quantity,
            o.limit_price,
            o.market_mode,
            o.status,
            o.execution_state,
            o.status_reason,
            o.filled_price,
            o.submitted_at,
            o.filled_at,
            o.cancelled_at,
            o.rejected_at,
            o.expired_at,
            o.expires_at,
            o.created_at,
            o.updated_at,
            rd.signal_id,
            COALESCE(s.strategy_id, rd.rationale::jsonb ->> 'strategy_id') AS strategy_id,
            rd.rationale AS risk_rationale
        FROM orders o
        LEFT JOIN risk_decisions rd ON rd.id = o.risk_decision_id
        LEFT JOIN signals s ON s.id = rd.signal_id
        WHERE o.id = $1
        "#,
    )
    .bind(order_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_order))
}

pub async fn get_order_by_idempotency_key(
    pool: &PgPool,
    idempotency_key: &str,
) -> Result<Option<OrderRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            o.id,
            o.correlation_id,
            o.risk_decision_id,
            o.idempotency_key,
            o.symbol,
            o.side,
            o.quantity,
            o.limit_price,
            o.market_mode,
            o.status,
            o.execution_state,
            o.status_reason,
            o.filled_price,
            o.submitted_at,
            o.filled_at,
            o.cancelled_at,
            o.rejected_at,
            o.expired_at,
            o.expires_at,
            o.created_at,
            o.updated_at,
            rd.signal_id,
            COALESCE(s.strategy_id, rd.rationale::jsonb ->> 'strategy_id') AS strategy_id,
            rd.rationale AS risk_rationale
        FROM orders o
        LEFT JOIN risk_decisions rd ON rd.id = o.risk_decision_id
        LEFT JOIN signals s ON s.id = rd.signal_id
        WHERE o.idempotency_key = $1
        "#,
    )
    .bind(idempotency_key)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_order))
}

pub async fn insert_exchange_testnet_order(
    pool: &PgPool,
    record: &ExchangeTestnetOrderRecord,
) -> Result<ExchangeTestnetOrderRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO exchange_testnet_orders (
            id,
            exchange,
            environment,
            client_order_id,
            exchange_order_id,
            symbol,
            side,
            order_type,
            time_in_force,
            requested_qty,
            requested_notional,
            limit_price,
            status,
            execution_state,
            ack_payload,
            latest_status_payload,
            risk_decision_id,
            created_by,
            last_transition_at,
            created_at,
            updated_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
        )
        RETURNING
            id,
            exchange,
            environment,
            client_order_id,
            exchange_order_id,
            symbol,
            side,
            order_type,
            time_in_force,
            requested_qty,
            requested_notional,
            limit_price,
            status,
            execution_state,
            ack_payload,
            latest_status_payload,
            risk_decision_id,
            created_by,
            last_transition_at,
            created_at,
            updated_at
        "#,
    )
    .bind(record.id)
    .bind(&record.exchange)
    .bind(&record.environment)
    .bind(&record.client_order_id)
    .bind(&record.exchange_order_id)
    .bind(&record.symbol)
    .bind(&record.side)
    .bind(&record.order_type)
    .bind(&record.time_in_force)
    .bind(record.requested_qty)
    .bind(record.requested_notional)
    .bind(record.limit_price)
    .bind(&record.status)
    .bind(&record.execution_state)
    .bind(&record.ack_payload)
    .bind(&record.latest_status_payload)
    .bind(record.risk_decision_id)
    .bind(record.created_by)
    .bind(record.last_transition_at)
    .bind(record.created_at)
    .bind(record.updated_at)
    .fetch_one(pool)
    .await?;

    Ok(map_exchange_testnet_order(&row))
}

pub async fn update_exchange_testnet_order_ack(
    pool: &PgPool,
    client_order_id: &str,
    exchange_order_id: Option<&str>,
    status: &str,
    execution_state: &str,
    ack_payload: &Value,
    last_transition_at: Option<DateTime<Utc>>,
) -> Result<Option<ExchangeTestnetOrderRecord>> {
    let row = sqlx::query(
        r#"
        UPDATE exchange_testnet_orders
        SET
            exchange_order_id = COALESCE($2, exchange_order_id),
            status = $3,
            execution_state = $4,
            ack_payload = $5,
            latest_status_payload = COALESCE(latest_status_payload, $5),
            last_transition_at = COALESCE($6, last_transition_at, NOW()),
            updated_at = NOW()
        WHERE client_order_id = $1
        RETURNING
            id,
            exchange,
            environment,
            client_order_id,
            exchange_order_id,
            symbol,
            side,
            order_type,
            time_in_force,
            requested_qty,
            requested_notional,
            limit_price,
            status,
            execution_state,
            ack_payload,
            latest_status_payload,
            risk_decision_id,
            created_by,
            last_transition_at,
            created_at,
            updated_at
        "#,
    )
    .bind(client_order_id)
    .bind(exchange_order_id)
    .bind(status)
    .bind(execution_state)
    .bind(ack_payload)
    .bind(last_transition_at)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_exchange_testnet_order))
}

pub async fn update_exchange_testnet_order_status(
    pool: &PgPool,
    client_order_id: &str,
    exchange_order_id: Option<&str>,
    status: &str,
    execution_state: &str,
    latest_status_payload: &Value,
    last_transition_at: Option<DateTime<Utc>>,
) -> Result<Option<ExchangeTestnetOrderRecord>> {
    let row = sqlx::query(
        r#"
        UPDATE exchange_testnet_orders
        SET
            exchange_order_id = COALESCE($2, exchange_order_id),
            status = $3,
            execution_state = $4,
            latest_status_payload = $5,
            last_transition_at = COALESCE($6, last_transition_at, NOW()),
            updated_at = NOW()
        WHERE client_order_id = $1
        RETURNING
            id,
            exchange,
            environment,
            client_order_id,
            exchange_order_id,
            symbol,
            side,
            order_type,
            time_in_force,
            requested_qty,
            requested_notional,
            limit_price,
            status,
            execution_state,
            ack_payload,
            latest_status_payload,
            risk_decision_id,
            created_by,
            last_transition_at,
            created_at,
            updated_at
        "#,
    )
    .bind(client_order_id)
    .bind(exchange_order_id)
    .bind(status)
    .bind(execution_state)
    .bind(latest_status_payload)
    .bind(last_transition_at)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_exchange_testnet_order))
}

pub async fn get_exchange_testnet_order_by_client_order_id(
    pool: &PgPool,
    client_order_id: &str,
) -> Result<Option<ExchangeTestnetOrderRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            exchange,
            environment,
            client_order_id,
            exchange_order_id,
            symbol,
            side,
            order_type,
            time_in_force,
            requested_qty,
            requested_notional,
            limit_price,
            status,
            execution_state,
            ack_payload,
            latest_status_payload,
            risk_decision_id,
            created_by,
            last_transition_at,
            created_at,
            updated_at
        FROM exchange_testnet_orders
        WHERE client_order_id = $1
        "#,
    )
    .bind(client_order_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_exchange_testnet_order))
}

pub async fn list_exchange_testnet_orders(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<ExchangeTestnetOrderRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            exchange,
            environment,
            client_order_id,
            exchange_order_id,
            symbol,
            side,
            order_type,
            time_in_force,
            requested_qty,
            requested_notional,
            limit_price,
            status,
            execution_state,
            ack_payload,
            latest_status_payload,
            risk_decision_id,
            created_by,
            last_transition_at,
            created_at,
            updated_at
        FROM exchange_testnet_orders
        ORDER BY created_at DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_exchange_testnet_order).collect())
}

pub async fn insert_testnet_shadow_run(
    pool: &PgPool,
    run: &TestnetShadowRunRecord,
) -> Result<TestnetShadowRunRecord> {
    let reasons = serde_json::to_value(&run.reasons)?;
    let row = sqlx::query(
        r#"
        INSERT INTO testnet_shadow_runs (
            id,
            strategy_id,
            symbol,
            timeframe,
            decision,
            signal_id,
            risk_decision_id,
            would_submit_payload,
            price_source,
            resolved_price,
            reasons,
            status,
            created_at,
            correlation_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        RETURNING
            id,
            strategy_id,
            symbol,
            timeframe,
            decision,
            signal_id,
            risk_decision_id,
            would_submit_payload,
            price_source,
            resolved_price,
            reasons,
            status,
            created_at,
            correlation_id
        "#,
    )
    .bind(run.id)
    .bind(&run.strategy_id)
    .bind(&run.symbol)
    .bind(&run.timeframe)
    .bind(&run.decision)
    .bind(run.signal_id)
    .bind(run.risk_decision_id)
    .bind(&run.would_submit_payload)
    .bind(&run.price_source)
    .bind(run.resolved_price)
    .bind(reasons)
    .bind(&run.status)
    .bind(run.created_at)
    .bind(run.correlation_id)
    .fetch_one(pool)
    .await?;

    Ok(map_testnet_shadow_run(&row))
}

pub async fn list_testnet_shadow_runs(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<TestnetShadowRunRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            strategy_id,
            symbol,
            timeframe,
            decision,
            signal_id,
            risk_decision_id,
            would_submit_payload,
            price_source,
            resolved_price,
            reasons,
            status,
            created_at,
            correlation_id
        FROM testnet_shadow_runs
        ORDER BY created_at DESC, id DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_testnet_shadow_run).collect())
}

pub async fn get_testnet_shadow_run_by_id(
    pool: &PgPool,
    run_id: Uuid,
) -> Result<Option<TestnetShadowRunRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            strategy_id,
            symbol,
            timeframe,
            decision,
            signal_id,
            risk_decision_id,
            would_submit_payload,
            price_source,
            resolved_price,
            reasons,
            status,
            created_at,
            correlation_id
        FROM testnet_shadow_runs
        WHERE id = $1
        "#,
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_testnet_shadow_run))
}

pub async fn insert_testnet_shadow_promotion(
    pool: &PgPool,
    promotion: &TestnetShadowPromotionRecord,
) -> Result<TestnetShadowPromotionRecord> {
    let rejection_reasons = serde_json::to_value(&promotion.rejection_reasons)?;
    let row = sqlx::query(
        r#"
        INSERT INTO testnet_shadow_promotions (
            id,
            shadow_run_id,
            status,
            strategy_id,
            symbol,
            timeframe,
            signal_id,
            risk_decision_id,
            would_submit_payload,
            resolved_price,
            price_source,
            rejection_reasons,
            testnet_order_id,
            client_order_id,
            expires_at,
            created_by,
            submitted_by,
            created_at,
            submitted_at,
            correlation_id
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
        )
        RETURNING
            id,
            shadow_run_id,
            status,
            strategy_id,
            symbol,
            timeframe,
            signal_id,
            risk_decision_id,
            would_submit_payload,
            resolved_price,
            price_source,
            rejection_reasons,
            testnet_order_id,
            client_order_id,
            expires_at,
            created_by,
            submitted_by,
            created_at,
            submitted_at,
            correlation_id
        "#,
    )
    .bind(promotion.id)
    .bind(promotion.shadow_run_id)
    .bind(&promotion.status)
    .bind(&promotion.strategy_id)
    .bind(&promotion.symbol)
    .bind(&promotion.timeframe)
    .bind(promotion.signal_id)
    .bind(promotion.risk_decision_id)
    .bind(&promotion.would_submit_payload)
    .bind(promotion.resolved_price)
    .bind(&promotion.price_source)
    .bind(rejection_reasons)
    .bind(promotion.testnet_order_id)
    .bind(&promotion.client_order_id)
    .bind(promotion.expires_at)
    .bind(promotion.created_by)
    .bind(promotion.submitted_by)
    .bind(promotion.created_at)
    .bind(promotion.submitted_at)
    .bind(promotion.correlation_id)
    .fetch_one(pool)
    .await?;

    Ok(map_testnet_shadow_promotion(&row))
}

pub async fn list_testnet_shadow_promotions(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<TestnetShadowPromotionRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            shadow_run_id,
            status,
            strategy_id,
            symbol,
            timeframe,
            signal_id,
            risk_decision_id,
            would_submit_payload,
            resolved_price,
            price_source,
            rejection_reasons,
            testnet_order_id,
            client_order_id,
            expires_at,
            created_by,
            submitted_by,
            created_at,
            submitted_at,
            correlation_id
        FROM testnet_shadow_promotions
        ORDER BY created_at DESC, id DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_testnet_shadow_promotion).collect())
}

pub async fn get_testnet_shadow_promotion_by_id(
    pool: &PgPool,
    promotion_id: Uuid,
) -> Result<Option<TestnetShadowPromotionRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            shadow_run_id,
            status,
            strategy_id,
            symbol,
            timeframe,
            signal_id,
            risk_decision_id,
            would_submit_payload,
            resolved_price,
            price_source,
            rejection_reasons,
            testnet_order_id,
            client_order_id,
            expires_at,
            created_by,
            submitted_by,
            created_at,
            submitted_at,
            correlation_id
        FROM testnet_shadow_promotions
        WHERE id = $1
        "#,
    )
    .bind(promotion_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_testnet_shadow_promotion))
}

pub async fn get_active_testnet_shadow_promotion_for_shadow_run(
    pool: &PgPool,
    shadow_run_id: Uuid,
) -> Result<Option<TestnetShadowPromotionRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            shadow_run_id,
            status,
            strategy_id,
            symbol,
            timeframe,
            signal_id,
            risk_decision_id,
            would_submit_payload,
            resolved_price,
            price_source,
            rejection_reasons,
            testnet_order_id,
            client_order_id,
            expires_at,
            created_by,
            submitted_by,
            created_at,
            submitted_at,
            correlation_id
        FROM testnet_shadow_promotions
        WHERE shadow_run_id = $1
          AND status IN ('PREVIEWED', 'SUBMITTED')
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        "#,
    )
    .bind(shadow_run_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_testnet_shadow_promotion))
}

pub async fn update_testnet_shadow_promotion_submission(
    pool: &PgPool,
    promotion_id: Uuid,
    status: &str,
    rejection_reasons: &[String],
    testnet_order_id: Option<Uuid>,
    client_order_id: Option<&str>,
    submitted_by: Option<Uuid>,
    submitted_at: Option<DateTime<Utc>>,
) -> Result<Option<TestnetShadowPromotionRecord>> {
    let reasons = serde_json::to_value(rejection_reasons)?;
    let row = sqlx::query(
        r#"
        UPDATE testnet_shadow_promotions
        SET
            status = $2,
            rejection_reasons = $3,
            testnet_order_id = COALESCE($4, testnet_order_id),
            client_order_id = COALESCE($5, client_order_id),
            submitted_by = COALESCE($6, submitted_by),
            submitted_at = COALESCE($7, submitted_at)
        WHERE id = $1
        RETURNING
            id,
            shadow_run_id,
            status,
            strategy_id,
            symbol,
            timeframe,
            signal_id,
            risk_decision_id,
            would_submit_payload,
            resolved_price,
            price_source,
            rejection_reasons,
            testnet_order_id,
            client_order_id,
            expires_at,
            created_by,
            submitted_by,
            created_at,
            submitted_at,
            correlation_id
        "#,
    )
    .bind(promotion_id)
    .bind(status)
    .bind(reasons)
    .bind(testnet_order_id)
    .bind(client_order_id)
    .bind(submitted_by)
    .bind(submitted_at)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_testnet_shadow_promotion))
}

pub fn default_testnet_shadow_runner_config(now: DateTime<Utc>) -> TestnetShadowRunnerConfig {
    TestnetShadowRunnerConfig {
        id: TESTNET_SHADOW_RUNNER_CONFIG_ID,
        enabled: false,
        interval_seconds: 60,
        strategies: vec!["momentum_v1".to_string()],
        symbols: vec!["BTCUSDT".to_string()],
        timeframe: "1m".to_string(),
        max_runs_per_tick: 1,
        stale_feed_policy: TestnetShadowRunnerStaleFeedPolicy::Skip,
        notes: None,
        updated_by: None,
        updated_at: now,
    }
}

pub fn default_testnet_shadow_runner_state(now: DateTime<Utc>) -> TestnetShadowRunnerState {
    TestnetShadowRunnerState {
        id: TESTNET_SHADOW_RUNNER_STATE_ID,
        status: TestnetShadowRunnerStatus::Stopped,
        last_tick_at: None,
        last_success_at: None,
        last_error: None,
        total_ticks: 0,
        total_runs: 0,
        updated_at: now,
    }
}

pub async fn upsert_testnet_shadow_runner_config(
    pool: &PgPool,
    config: &TestnetShadowRunnerConfig,
) -> Result<TestnetShadowRunnerConfigRecord> {
    let strategies = serde_json::to_value(&config.strategies)?;
    let symbols = serde_json::to_value(&config.symbols)?;
    let row = sqlx::query(
        r#"
        INSERT INTO testnet_shadow_runner_config (
            id,
            enabled,
            interval_seconds,
            strategies,
            symbols,
            timeframe,
            max_runs_per_tick,
            stale_feed_policy,
            notes,
            updated_by,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (id) DO UPDATE
        SET
            enabled = EXCLUDED.enabled,
            interval_seconds = EXCLUDED.interval_seconds,
            strategies = EXCLUDED.strategies,
            symbols = EXCLUDED.symbols,
            timeframe = EXCLUDED.timeframe,
            max_runs_per_tick = EXCLUDED.max_runs_per_tick,
            stale_feed_policy = EXCLUDED.stale_feed_policy,
            notes = EXCLUDED.notes,
            updated_by = EXCLUDED.updated_by,
            updated_at = EXCLUDED.updated_at
        RETURNING
            id,
            enabled,
            interval_seconds,
            strategies,
            symbols,
            timeframe,
            max_runs_per_tick,
            stale_feed_policy,
            notes,
            updated_by,
            updated_at
        "#,
    )
    .bind(config.id)
    .bind(config.enabled)
    .bind(config.interval_seconds)
    .bind(strategies)
    .bind(symbols)
    .bind(&config.timeframe)
    .bind(config.max_runs_per_tick)
    .bind(config.stale_feed_policy.as_str())
    .bind(&config.notes)
    .bind(config.updated_by)
    .bind(config.updated_at)
    .fetch_one(pool)
    .await?;

    Ok(map_testnet_shadow_runner_config(&row))
}

pub async fn get_testnet_shadow_runner_config(
    pool: &PgPool,
) -> Result<Option<TestnetShadowRunnerConfigRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            enabled,
            interval_seconds,
            strategies,
            symbols,
            timeframe,
            max_runs_per_tick,
            stale_feed_policy,
            notes,
            updated_by,
            updated_at
        FROM testnet_shadow_runner_config
        WHERE id = $1
        "#,
    )
    .bind(TESTNET_SHADOW_RUNNER_CONFIG_ID)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_testnet_shadow_runner_config))
}

pub async fn ensure_testnet_shadow_runner_config(
    pool: &PgPool,
) -> Result<TestnetShadowRunnerConfigRecord> {
    if let Some(record) = get_testnet_shadow_runner_config(pool).await? {
        return Ok(record);
    }

    let config = default_testnet_shadow_runner_config(Utc::now());
    upsert_testnet_shadow_runner_config(pool, &config).await
}

pub async fn upsert_testnet_shadow_runner_state(
    pool: &PgPool,
    state: &TestnetShadowRunnerState,
) -> Result<TestnetShadowRunnerStateRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO testnet_shadow_runner_state (
            id,
            status,
            last_tick_at,
            last_success_at,
            last_error,
            total_ticks,
            total_runs,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (id) DO UPDATE
        SET
            status = EXCLUDED.status,
            last_tick_at = EXCLUDED.last_tick_at,
            last_success_at = EXCLUDED.last_success_at,
            last_error = EXCLUDED.last_error,
            total_ticks = EXCLUDED.total_ticks,
            total_runs = EXCLUDED.total_runs,
            updated_at = EXCLUDED.updated_at
        RETURNING
            id,
            status,
            last_tick_at,
            last_success_at,
            last_error,
            total_ticks,
            total_runs,
            updated_at
        "#,
    )
    .bind(state.id)
    .bind(state.status.as_str())
    .bind(state.last_tick_at)
    .bind(state.last_success_at)
    .bind(&state.last_error)
    .bind(state.total_ticks)
    .bind(state.total_runs)
    .bind(state.updated_at)
    .fetch_one(pool)
    .await?;

    Ok(map_testnet_shadow_runner_state(&row))
}

pub async fn get_testnet_shadow_runner_state(
    pool: &PgPool,
) -> Result<Option<TestnetShadowRunnerStateRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            status,
            last_tick_at,
            last_success_at,
            last_error,
            total_ticks,
            total_runs,
            updated_at
        FROM testnet_shadow_runner_state
        WHERE id = $1
        "#,
    )
    .bind(TESTNET_SHADOW_RUNNER_STATE_ID)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_testnet_shadow_runner_state))
}

pub async fn ensure_testnet_shadow_runner_state(
    pool: &PgPool,
) -> Result<TestnetShadowRunnerStateRecord> {
    if let Some(record) = get_testnet_shadow_runner_state(pool).await? {
        return Ok(record);
    }

    let state = default_testnet_shadow_runner_state(Utc::now());
    upsert_testnet_shadow_runner_state(pool, &state).await
}

pub async fn list_exchange_testnet_orders_for_reconciliation(
    pool: &PgPool,
    limit: i64,
    status_filter: &[String],
) -> Result<Vec<ExchangeTestnetOrderRecord>> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
            id,
            exchange,
            environment,
            client_order_id,
            exchange_order_id,
            symbol,
            side,
            order_type,
            time_in_force,
            requested_qty,
            requested_notional,
            limit_price,
            status,
            execution_state,
            ack_payload,
            latest_status_payload,
            risk_decision_id,
            created_by,
            last_transition_at,
            created_at,
            updated_at
        FROM exchange_testnet_orders
        "#,
    );

    if !status_filter.is_empty() {
        builder.push(" WHERE status IN (");
        let mut separated = builder.separated(", ");
        for status in status_filter {
            separated.push_bind(status);
        }
        separated.push_unseparated(")");
    }

    builder.push(" ORDER BY updated_at DESC LIMIT ");
    builder.push_bind(limit);

    let rows = builder.build().fetch_all(pool).await?;
    Ok(rows.iter().map(map_exchange_testnet_order).collect())
}

pub async fn insert_exchange_testnet_order_lifecycle_event(
    pool: &PgPool,
    record: &ExchangeTestnetOrderLifecycleEventRecord,
) -> Result<ExchangeTestnetOrderLifecycleEventRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO exchange_testnet_order_lifecycle_events (
            id,
            order_id,
            client_order_id,
            previous_state,
            next_state,
            transition_source,
            reason,
            payload,
            created_by,
            created_at,
            correlation_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        RETURNING
            id,
            order_id,
            client_order_id,
            previous_state,
            next_state,
            transition_source,
            reason,
            payload,
            created_by,
            created_at,
            correlation_id
        "#,
    )
    .bind(record.id)
    .bind(record.order_id)
    .bind(&record.client_order_id)
    .bind(&record.previous_state)
    .bind(&record.next_state)
    .bind(&record.transition_source)
    .bind(&record.reason)
    .bind(&record.payload)
    .bind(record.created_by)
    .bind(record.created_at)
    .bind(record.correlation_id)
    .fetch_one(pool)
    .await?;

    Ok(map_exchange_testnet_order_lifecycle_event(&row))
}

pub async fn list_exchange_testnet_order_lifecycle_events(
    pool: &PgPool,
    client_order_id: &str,
) -> Result<Vec<ExchangeTestnetOrderLifecycleEventRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            order_id,
            client_order_id,
            previous_state,
            next_state,
            transition_source,
            reason,
            payload,
            created_by,
            created_at,
            correlation_id
        FROM exchange_testnet_order_lifecycle_events
        WHERE client_order_id = $1
        ORDER BY created_at ASC, id ASC
        "#,
    )
    .bind(client_order_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(map_exchange_testnet_order_lifecycle_event)
        .collect())
}

pub async fn insert_exchange_testnet_repair_action(
    pool: &PgPool,
    record: &ExchangeTestnetRepairActionRecord,
) -> Result<ExchangeTestnetRepairActionRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO exchange_testnet_repair_actions (
            id,
            client_order_id,
            action,
            status,
            previous_state,
            next_state,
            reason,
            payload,
            actor_id,
            created_at,
            correlation_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        RETURNING
            id,
            client_order_id,
            action,
            status,
            previous_state,
            next_state,
            reason,
            payload,
            actor_id,
            created_at,
            correlation_id
        "#,
    )
    .bind(record.id)
    .bind(&record.client_order_id)
    .bind(&record.action)
    .bind(&record.status)
    .bind(&record.previous_state)
    .bind(&record.next_state)
    .bind(&record.reason)
    .bind(&record.payload)
    .bind(record.actor_id)
    .bind(record.created_at)
    .bind(record.correlation_id)
    .fetch_one(pool)
    .await?;

    Ok(map_exchange_testnet_repair_action(&row))
}

pub async fn list_exchange_testnet_repair_actions(
    pool: &PgPool,
    client_order_id: &str,
) -> Result<Vec<ExchangeTestnetRepairActionRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            client_order_id,
            action,
            status,
            previous_state,
            next_state,
            reason,
            payload,
            actor_id,
            created_at,
            correlation_id
        FROM exchange_testnet_repair_actions
        WHERE client_order_id = $1
        ORDER BY created_at DESC, id DESC
        "#,
    )
    .bind(client_order_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(map_exchange_testnet_repair_action)
        .collect())
}

pub async fn count_recent_exchange_testnet_repair_failures(
    pool: &PgPool,
    since: DateTime<Utc>,
) -> Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM exchange_testnet_repair_actions
        WHERE created_at >= $1
          AND lower(status) = 'failed'
        "#,
    )
    .bind(since)
    .fetch_one(pool)
    .await?)
}

pub async fn append_exchange_testnet_lifecycle_event_and_update_order(
    pool: &PgPool,
    event: &ExchangeTestnetOrderLifecycleEventRecord,
    exchange_order_id: Option<&str>,
    status: Option<&str>,
    execution_state: TestnetExecutionState,
    latest_status_payload: Option<&Value>,
    ack_payload: Option<&Value>,
) -> Result<Option<ExchangeTestnetOrderRecord>> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        r#"
        INSERT INTO exchange_testnet_order_lifecycle_events (
            id,
            order_id,
            client_order_id,
            previous_state,
            next_state,
            transition_source,
            reason,
            payload,
            created_by,
            created_at,
            correlation_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        "#,
    )
    .bind(event.id)
    .bind(event.order_id)
    .bind(&event.client_order_id)
    .bind(&event.previous_state)
    .bind(&event.next_state)
    .bind(&event.transition_source)
    .bind(&event.reason)
    .bind(&event.payload)
    .bind(event.created_by)
    .bind(event.created_at)
    .bind(event.correlation_id)
    .execute(&mut *tx)
    .await?;

    let row = sqlx::query(
        r#"
        UPDATE exchange_testnet_orders
        SET
            exchange_order_id = COALESCE($2, exchange_order_id),
            status = COALESCE($3, status),
            execution_state = $4,
            latest_status_payload = COALESCE($5, latest_status_payload),
            ack_payload = COALESCE($6, ack_payload),
            last_transition_at = $7,
            updated_at = NOW()
        WHERE client_order_id = $1
        RETURNING
            id,
            exchange,
            environment,
            client_order_id,
            exchange_order_id,
            symbol,
            side,
            order_type,
            time_in_force,
            requested_qty,
            requested_notional,
            limit_price,
            status,
            execution_state,
            ack_payload,
            latest_status_payload,
            risk_decision_id,
            created_by,
            last_transition_at,
            created_at,
            updated_at
        "#,
    )
    .bind(&event.client_order_id)
    .bind(exchange_order_id)
    .bind(status)
    .bind(execution_state.as_str())
    .bind(latest_status_payload)
    .bind(ack_payload)
    .bind(event.created_at)
    .fetch_optional(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(row.as_ref().map(map_exchange_testnet_order))
}

pub async fn insert_exchange_private_stream_event(
    pool: &PgPool,
    record: &ExchangePrivateStreamEventRecord,
) -> Result<ExchangePrivateStreamEventRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO exchange_private_stream_events (
            id,
            exchange,
            environment,
            event_type,
            symbol,
            client_order_id,
            exchange_order_id,
            execution_type,
            order_status,
            payload,
            event_time,
            received_at,
            correlation_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        RETURNING
            id,
            exchange,
            environment,
            event_type,
            symbol,
            client_order_id,
            exchange_order_id,
            execution_type,
            order_status,
            payload,
            event_time,
            received_at,
            correlation_id
        "#,
    )
    .bind(record.id)
    .bind(&record.exchange)
    .bind(&record.environment)
    .bind(&record.event_type)
    .bind(&record.symbol)
    .bind(&record.client_order_id)
    .bind(&record.exchange_order_id)
    .bind(&record.execution_type)
    .bind(&record.order_status)
    .bind(&record.payload)
    .bind(record.event_time)
    .bind(record.received_at)
    .bind(record.correlation_id)
    .fetch_one(pool)
    .await?;

    Ok(map_exchange_private_stream_event(&row))
}

pub async fn list_exchange_private_stream_events(
    pool: &PgPool,
    environment: &str,
    limit: i64,
    client_order_id: Option<&str>,
    event_type: Option<&str>,
) -> Result<Vec<ExchangePrivateStreamEventRecord>> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
            id,
            exchange,
            environment,
            event_type,
            symbol,
            client_order_id,
            exchange_order_id,
            execution_type,
            order_status,
            payload,
            event_time,
            received_at,
            correlation_id
        FROM exchange_private_stream_events
        WHERE environment = 
        "#,
    );
    builder.push_bind(environment);

    if let Some(client_order_id) = client_order_id.filter(|value| !value.trim().is_empty()) {
        builder.push(" AND client_order_id = ");
        builder.push_bind(client_order_id);
    }
    if let Some(event_type) = event_type.filter(|value| !value.trim().is_empty()) {
        builder.push(" AND event_type = ");
        builder.push_bind(event_type);
    }

    builder.push(" ORDER BY received_at DESC LIMIT ");
    builder.push_bind(limit);

    let rows = builder.build().fetch_all(pool).await?;
    Ok(rows.iter().map(map_exchange_private_stream_event).collect())
}

pub async fn get_exchange_private_stream_state(
    pool: &PgPool,
    exchange: &str,
    environment: &str,
) -> Result<Option<ExchangePrivateStreamStateRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            exchange,
            environment,
            status,
            listen_key_hash,
            connected_at,
            last_event_at,
            last_error,
            reconnect_count,
            updated_at
        FROM exchange_private_stream_state
        WHERE exchange = $1 AND environment = $2
        "#,
    )
    .bind(exchange)
    .bind(environment)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_exchange_private_stream_state))
}

pub async fn upsert_exchange_private_stream_state(
    pool: &PgPool,
    record: &ExchangePrivateStreamStateRecord,
) -> Result<ExchangePrivateStreamStateRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO exchange_private_stream_state (
            exchange,
            environment,
            status,
            listen_key_hash,
            connected_at,
            last_event_at,
            last_error,
            reconnect_count,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (exchange, environment)
        DO UPDATE SET
            status = EXCLUDED.status,
            listen_key_hash = EXCLUDED.listen_key_hash,
            connected_at = EXCLUDED.connected_at,
            last_event_at = EXCLUDED.last_event_at,
            last_error = EXCLUDED.last_error,
            reconnect_count = EXCLUDED.reconnect_count,
            updated_at = EXCLUDED.updated_at
        RETURNING
            exchange,
            environment,
            status,
            listen_key_hash,
            connected_at,
            last_event_at,
            last_error,
            reconnect_count,
            updated_at
        "#,
    )
    .bind(&record.exchange)
    .bind(&record.environment)
    .bind(&record.status)
    .bind(&record.listen_key_hash)
    .bind(record.connected_at)
    .bind(record.last_event_at)
    .bind(&record.last_error)
    .bind(record.reconnect_count)
    .bind(record.updated_at)
    .fetch_one(pool)
    .await?;

    Ok(map_exchange_private_stream_state(&row))
}

pub async fn insert_exchange_reconciliation_run(
    pool: &PgPool,
    run: &ExchangeReconciliationRunRecord,
) -> Result<ExchangeReconciliationRunRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO exchange_reconciliation_runs (
            id,
            exchange,
            environment,
            status,
            checked_orders,
            matched_orders,
            mismatched_orders,
            unknown_orders,
            failed_reason,
            correlation_id,
            started_at,
            completed_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        RETURNING
            id,
            exchange,
            environment,
            status,
            checked_orders,
            matched_orders,
            mismatched_orders,
            unknown_orders,
            failed_reason,
            correlation_id,
            started_at,
            completed_at
        "#,
    )
    .bind(run.id)
    .bind(&run.exchange)
    .bind(&run.environment)
    .bind(&run.status)
    .bind(run.checked_orders)
    .bind(run.matched_orders)
    .bind(run.mismatched_orders)
    .bind(run.unknown_orders)
    .bind(&run.failed_reason)
    .bind(run.correlation_id)
    .bind(run.started_at)
    .bind(run.completed_at)
    .fetch_one(pool)
    .await?;

    Ok(map_exchange_reconciliation_run(&row))
}

pub async fn complete_exchange_reconciliation_run(
    pool: &PgPool,
    run_id: Uuid,
    summary: &aegis_core::ExchangeReconciliationSummary,
) -> Result<Option<ExchangeReconciliationRunRecord>> {
    let row = sqlx::query(
        r#"
        UPDATE exchange_reconciliation_runs
        SET
            status = $2,
            checked_orders = $3,
            matched_orders = $4,
            mismatched_orders = $5,
            unknown_orders = $6,
            failed_reason = NULL,
            completed_at = NOW()
        WHERE id = $1
        RETURNING
            id,
            exchange,
            environment,
            status,
            checked_orders,
            matched_orders,
            mismatched_orders,
            unknown_orders,
            failed_reason,
            correlation_id,
            started_at,
            completed_at
        "#,
    )
    .bind(run_id)
    .bind(ExchangeReconciliationStatus::Completed.as_str())
    .bind(summary.checked_orders)
    .bind(summary.matched_orders)
    .bind(summary.mismatched_orders)
    .bind(summary.unknown_orders)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_exchange_reconciliation_run))
}

pub async fn fail_exchange_reconciliation_run(
    pool: &PgPool,
    run_id: Uuid,
    summary: &aegis_core::ExchangeReconciliationSummary,
    failed_reason: &str,
) -> Result<Option<ExchangeReconciliationRunRecord>> {
    let row = sqlx::query(
        r#"
        UPDATE exchange_reconciliation_runs
        SET
            status = $2,
            checked_orders = $3,
            matched_orders = $4,
            mismatched_orders = $5,
            unknown_orders = $6,
            failed_reason = $7,
            completed_at = NOW()
        WHERE id = $1
        RETURNING
            id,
            exchange,
            environment,
            status,
            checked_orders,
            matched_orders,
            mismatched_orders,
            unknown_orders,
            failed_reason,
            correlation_id,
            started_at,
            completed_at
        "#,
    )
    .bind(run_id)
    .bind(ExchangeReconciliationStatus::Failed.as_str())
    .bind(summary.checked_orders)
    .bind(summary.matched_orders)
    .bind(summary.mismatched_orders)
    .bind(summary.unknown_orders)
    .bind(failed_reason)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_exchange_reconciliation_run))
}

pub async fn get_exchange_reconciliation_run(
    pool: &PgPool,
    run_id: Uuid,
) -> Result<Option<ExchangeReconciliationRunRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            exchange,
            environment,
            status,
            checked_orders,
            matched_orders,
            mismatched_orders,
            unknown_orders,
            failed_reason,
            correlation_id,
            started_at,
            completed_at
        FROM exchange_reconciliation_runs
        WHERE id = $1
        "#,
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_exchange_reconciliation_run))
}

pub async fn list_exchange_reconciliation_runs(
    pool: &PgPool,
    environment: &str,
    limit: i64,
) -> Result<Vec<ExchangeReconciliationRunRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            exchange,
            environment,
            status,
            checked_orders,
            matched_orders,
            mismatched_orders,
            unknown_orders,
            failed_reason,
            correlation_id,
            started_at,
            completed_at
        FROM exchange_reconciliation_runs
        WHERE environment = $1
        ORDER BY started_at DESC
        LIMIT $2
        "#,
    )
    .bind(environment)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_exchange_reconciliation_run).collect())
}

pub async fn insert_exchange_reconciliation_mismatch(
    pool: &PgPool,
    mismatch: &ExchangeReconciliationMismatchRecord,
) -> Result<ExchangeReconciliationMismatchRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO exchange_reconciliation_mismatches (
            id,
            run_id,
            client_order_id,
            local_status,
            exchange_status,
            mismatch_kind,
            action,
            payload,
            created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING
            id,
            run_id,
            client_order_id,
            local_status,
            exchange_status,
            mismatch_kind,
            action,
            payload,
            created_at
        "#,
    )
    .bind(mismatch.id)
    .bind(mismatch.run_id)
    .bind(&mismatch.client_order_id)
    .bind(&mismatch.local_status)
    .bind(&mismatch.exchange_status)
    .bind(&mismatch.mismatch_kind)
    .bind(&mismatch.action)
    .bind(&mismatch.payload)
    .bind(mismatch.created_at)
    .fetch_one(pool)
    .await?;

    Ok(map_exchange_reconciliation_mismatch(&row))
}

pub async fn list_exchange_reconciliation_mismatches(
    pool: &PgPool,
    run_id: Uuid,
) -> Result<Vec<ExchangeReconciliationMismatchRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            run_id,
            client_order_id,
            local_status,
            exchange_status,
            mismatch_kind,
            action,
            payload,
            created_at
        FROM exchange_reconciliation_mismatches
        WHERE run_id = $1
        ORDER BY created_at ASC
        "#,
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(map_exchange_reconciliation_mismatch)
        .collect())
}

pub async fn insert_execution_readiness_snapshot(
    pool: &PgPool,
    snapshot: &ExecutionReadinessSnapshot,
) -> Result<ExecutionReadinessSnapshotRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO execution_readiness_snapshots (
            id,
            target,
            status,
            score,
            blocking_reasons,
            warnings,
            checks,
            recommendations,
            created_by,
            created_at,
            correlation_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        RETURNING
            id,
            target,
            status,
            score,
            blocking_reasons,
            warnings,
            checks,
            recommendations,
            created_by,
            created_at,
            correlation_id
        "#,
    )
    .bind(snapshot.id)
    .bind(snapshot.target.as_str())
    .bind(snapshot.status.as_str())
    .bind(snapshot.score)
    .bind(serde_json::to_value(&snapshot.blocking_reasons)?)
    .bind(serde_json::to_value(&snapshot.warnings)?)
    .bind(serde_json::to_value(&snapshot.checks)?)
    .bind(serde_json::to_value(&snapshot.recommendations)?)
    .bind(snapshot.created_by)
    .bind(snapshot.created_at)
    .bind(snapshot.correlation_id)
    .fetch_one(pool)
    .await?;

    Ok(map_execution_readiness_snapshot(&row))
}

pub async fn get_execution_readiness_snapshot(
    pool: &PgPool,
    id: Uuid,
) -> Result<Option<ExecutionReadinessSnapshotRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            target,
            status,
            score,
            blocking_reasons,
            warnings,
            checks,
            recommendations,
            created_by,
            created_at,
            correlation_id
        FROM execution_readiness_snapshots
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_execution_readiness_snapshot))
}

pub async fn list_execution_readiness_snapshots(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<ExecutionReadinessSnapshotRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            target,
            status,
            score,
            blocking_reasons,
            warnings,
            checks,
            recommendations,
            created_by,
            created_at,
            correlation_id
        FROM execution_readiness_snapshots
        ORDER BY created_at DESC, id DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_execution_readiness_snapshot).collect())
}

pub async fn get_default_paper_account(pool: &PgPool) -> Result<Option<PaperAccountRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            name,
            base_currency,
            initial_equity,
            current_equity,
            realized_pnl,
            unrealized_pnl,
            status,
            created_at,
            updated_at
        FROM paper_accounts
        ORDER BY created_at ASC
        LIMIT 1
        "#,
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_paper_account))
}

pub async fn insert_paper_account(
    pool: &PgPool,
    account: &PaperAccount,
) -> Result<PaperAccountRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO paper_accounts (
            id,
            name,
            base_currency,
            initial_equity,
            current_equity,
            realized_pnl,
            unrealized_pnl,
            status,
            created_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (id) DO UPDATE
        SET
            name = EXCLUDED.name,
            base_currency = EXCLUDED.base_currency,
            initial_equity = EXCLUDED.initial_equity,
            current_equity = EXCLUDED.current_equity,
            realized_pnl = EXCLUDED.realized_pnl,
            unrealized_pnl = EXCLUDED.unrealized_pnl,
            status = EXCLUDED.status,
            updated_at = EXCLUDED.updated_at
        RETURNING
            id,
            name,
            base_currency,
            initial_equity,
            current_equity,
            realized_pnl,
            unrealized_pnl,
            status,
            created_at,
            updated_at
        "#,
    )
    .bind(account.id)
    .bind(&account.name)
    .bind(&account.base_currency)
    .bind(account.initial_equity)
    .bind(account.current_equity)
    .bind(account.realized_pnl)
    .bind(account.unrealized_pnl)
    .bind(account.status.as_str())
    .bind(account.created_at)
    .bind(account.updated_at)
    .fetch_one(pool)
    .await?;

    Ok(map_paper_account(&row))
}

pub async fn get_open_paper_position(
    pool: &PgPool,
    account_id: Uuid,
    symbol: &str,
    side: PositionSide,
) -> Result<Option<PaperPositionRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            account_id,
            symbol,
            side,
            quantity,
            entry_price,
            mark_price,
            price_status,
            notional,
            realized_pnl,
            unrealized_pnl,
            status,
            opened_at,
            closed_at,
            strategy_id,
            signal_id,
            risk_decision_id,
            order_id,
            created_at,
            updated_at
        FROM paper_positions
        WHERE account_id = $1
          AND symbol = $2
          AND side = $3
          AND status = 'open'
        LIMIT 1
        "#,
    )
    .bind(account_id)
    .bind(symbol)
    .bind(side.as_str())
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_paper_position))
}

pub async fn upsert_paper_position(
    pool: &PgPool,
    position: &PaperPosition,
) -> Result<PaperPositionRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO paper_positions (
            id,
            account_id,
            symbol,
            side,
            quantity,
            entry_price,
            mark_price,
            price_status,
            notional,
            realized_pnl,
            unrealized_pnl,
            status,
            opened_at,
            closed_at,
            strategy_id,
            signal_id,
            risk_decision_id,
            order_id,
            updated_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
        )
        ON CONFLICT (id) DO UPDATE
        SET
            quantity = EXCLUDED.quantity,
            entry_price = EXCLUDED.entry_price,
            mark_price = EXCLUDED.mark_price,
            price_status = EXCLUDED.price_status,
            notional = EXCLUDED.notional,
            realized_pnl = EXCLUDED.realized_pnl,
            unrealized_pnl = EXCLUDED.unrealized_pnl,
            status = EXCLUDED.status,
            closed_at = EXCLUDED.closed_at,
            strategy_id = EXCLUDED.strategy_id,
            signal_id = EXCLUDED.signal_id,
            risk_decision_id = EXCLUDED.risk_decision_id,
            order_id = EXCLUDED.order_id,
            updated_at = EXCLUDED.updated_at
        RETURNING
            id,
            account_id,
            symbol,
            side,
            quantity,
            entry_price,
            mark_price,
            price_status,
            notional,
            realized_pnl,
            unrealized_pnl,
            status,
            opened_at,
            closed_at,
            strategy_id,
            signal_id,
            risk_decision_id,
            order_id,
            created_at,
            updated_at
        "#,
    )
    .bind(position.id)
    .bind(position.account_id)
    .bind(&position.symbol)
    .bind(position.side.as_str())
    .bind(position.quantity)
    .bind(position.entry_price)
    .bind(position.mark_price)
    .bind(position.price_status.as_str())
    .bind(position.notional)
    .bind(position.realized_pnl)
    .bind(position.unrealized_pnl)
    .bind(position.status.as_str())
    .bind(position.opened_at)
    .bind(position.closed_at)
    .bind(&position.strategy_id)
    .bind(position.signal_id)
    .bind(position.risk_decision_id)
    .bind(position.order_id)
    .bind(position.updated_at)
    .fetch_one(pool)
    .await?;

    Ok(map_paper_position(&row))
}

pub async fn insert_paper_fill(pool: &PgPool, fill: &PaperFill) -> Result<PaperFillRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO paper_fills (
            id,
            account_id,
            order_id,
            position_id,
            symbol,
            side,
            price,
            quantity,
            notional,
            fee,
            slippage_cost,
            filled_at,
            strategy_id,
            signal_id,
            risk_decision_id,
            correlation_id
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
        )
        ON CONFLICT (order_id) DO UPDATE
        SET
            position_id = EXCLUDED.position_id
        RETURNING
            id,
            account_id,
            order_id,
            position_id,
            symbol,
            side,
            price,
            quantity,
            notional,
            fee,
            slippage_cost,
            filled_at,
            strategy_id,
            signal_id,
            risk_decision_id,
            correlation_id,
            created_at
        "#,
    )
    .bind(fill.id)
    .bind(fill.account_id)
    .bind(fill.order_id)
    .bind(fill.position_id)
    .bind(&fill.symbol)
    .bind(fill.side.as_str())
    .bind(fill.price)
    .bind(fill.quantity)
    .bind(fill.notional)
    .bind(fill.fee)
    .bind(fill.slippage_cost)
    .bind(fill.filled_at)
    .bind(&fill.strategy_id)
    .bind(fill.signal_id)
    .bind(fill.risk_decision_id)
    .bind(fill.correlation_id)
    .fetch_one(pool)
    .await?;

    Ok(map_paper_fill(&row))
}

pub async fn insert_paper_equity_snapshot(
    pool: &PgPool,
    snapshot: &PaperEquitySnapshot,
) -> Result<PaperEquitySnapshotRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO paper_equity_snapshots (
            id,
            account_id,
            equity,
            realized_pnl,
            unrealized_pnl,
            drawdown_pct,
            snapshot_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING
            id,
            account_id,
            equity,
            realized_pnl,
            unrealized_pnl,
            drawdown_pct,
            snapshot_at
        "#,
    )
    .bind(snapshot.id)
    .bind(snapshot.account_id)
    .bind(snapshot.equity)
    .bind(snapshot.realized_pnl)
    .bind(snapshot.unrealized_pnl)
    .bind(snapshot.drawdown_pct)
    .bind(snapshot.snapshot_at)
    .fetch_one(pool)
    .await?;

    Ok(map_paper_equity_snapshot(&row))
}

pub async fn insert_paper_trade_journal_entry(
    pool: &PgPool,
    entry: &PaperTradeJournalEntry,
) -> Result<PaperTradeJournalRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO paper_trade_journal (
            id,
            account_id,
            position_id,
            order_id,
            event_type,
            symbol,
            pnl,
            payload,
            created_at,
            correlation_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING
            id,
            account_id,
            position_id,
            order_id,
            event_type,
            symbol,
            pnl,
            payload,
            created_at,
            correlation_id
        "#,
    )
    .bind(entry.id)
    .bind(entry.account_id)
    .bind(entry.position_id)
    .bind(entry.order_id)
    .bind(&entry.event_type)
    .bind(&entry.symbol)
    .bind(entry.pnl)
    .bind(&entry.payload)
    .bind(entry.created_at)
    .bind(entry.correlation_id)
    .fetch_one(pool)
    .await?;

    Ok(map_paper_trade_journal(&row))
}

pub async fn list_paper_positions(
    pool: &PgPool,
    account_id: Uuid,
    status_filter: PaperPositionStatusFilter,
    limit: i64,
) -> Result<Vec<PaperPositionRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            account_id,
            symbol,
            side,
            quantity,
            entry_price,
            mark_price,
            price_status,
            notional,
            realized_pnl,
            unrealized_pnl,
            status,
            opened_at,
            closed_at,
            strategy_id,
            signal_id,
            risk_decision_id,
            order_id,
            created_at,
            updated_at
        FROM paper_positions
        WHERE account_id = $1
          AND ($2 = 'all' OR status = $2)
        ORDER BY opened_at DESC
        LIMIT $3
        "#,
    )
    .bind(account_id)
    .bind(status_filter.as_str())
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_paper_position).collect())
}

pub async fn get_paper_position_by_id(
    pool: &PgPool,
    account_id: Uuid,
    position_id: Uuid,
) -> Result<Option<PaperPositionRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            account_id,
            symbol,
            side,
            quantity,
            entry_price,
            mark_price,
            price_status,
            notional,
            realized_pnl,
            unrealized_pnl,
            status,
            opened_at,
            closed_at,
            strategy_id,
            signal_id,
            risk_decision_id,
            order_id,
            created_at,
            updated_at
        FROM paper_positions
        WHERE account_id = $1 AND id = $2
        "#,
    )
    .bind(account_id)
    .bind(position_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_paper_position))
}

pub async fn get_paper_position(
    pool: &PgPool,
    account_id: Uuid,
    position_id: Uuid,
) -> Result<Option<PaperPositionRecord>> {
    get_paper_position_by_id(pool, account_id, position_id).await
}

pub async fn list_open_paper_positions(
    pool: &PgPool,
    account_id: Uuid,
) -> Result<Vec<PaperPositionRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            account_id,
            symbol,
            side,
            quantity,
            entry_price,
            mark_price,
            price_status,
            notional,
            realized_pnl,
            unrealized_pnl,
            status,
            opened_at,
            closed_at,
            strategy_id,
            signal_id,
            risk_decision_id,
            order_id,
            created_at,
            updated_at
        FROM paper_positions
        WHERE account_id = $1 AND status = 'open'
        ORDER BY opened_at DESC
        "#,
    )
    .bind(account_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_paper_position).collect())
}

pub async fn list_paper_equity_snapshots(
    pool: &PgPool,
    account_id: Uuid,
    limit: i64,
) -> Result<Vec<PaperEquitySnapshotRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            account_id,
            equity,
            realized_pnl,
            unrealized_pnl,
            drawdown_pct,
            snapshot_at
        FROM paper_equity_snapshots
        WHERE account_id = $1
        ORDER BY snapshot_at DESC
        LIMIT $2
        "#,
    )
    .bind(account_id)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_paper_equity_snapshot).collect())
}

pub async fn list_paper_trade_journal(
    pool: &PgPool,
    account_id: Uuid,
    limit: i64,
) -> Result<Vec<PaperTradeJournalRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            account_id,
            position_id,
            order_id,
            event_type,
            symbol,
            pnl,
            payload,
            created_at,
            correlation_id
        FROM paper_trade_journal
        WHERE account_id = $1
        ORDER BY created_at DESC
        LIMIT $2
        "#,
    )
    .bind(account_id)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_paper_trade_journal).collect())
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

pub async fn get_latest_mark_price(
    pool: &PgPool,
    symbol: &str,
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
        WHERE symbol = $1
        ORDER BY trade_time DESC, received_at DESC
        LIMIT 1
        "#,
    )
    .bind(symbol)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_market_tick))
}

pub async fn get_paper_close_summary(
    pool: &PgPool,
    account_id: Uuid,
    position_id: Uuid,
) -> Result<Option<PaperPositionCloseSummary>> {
    let row = sqlx::query(
        r#"
        SELECT
            p.id AS position_id,
            p.account_id,
            p.symbol,
            f.quantity,
            p.entry_price,
            f.price AS exit_price,
            p.realized_pnl,
            f.fee,
            f.slippage_cost,
            p.closed_at,
            j.correlation_id,
            j.id AS journal_entry_id,
            f.id AS close_fill_id
        FROM paper_positions p
        JOIN paper_fills f
          ON f.position_id = p.id
        JOIN paper_trade_journal j
          ON j.position_id = p.id
         AND j.event_type = 'paper.position.closed'
        WHERE p.account_id = $1
          AND p.id = $2
          AND p.status = 'closed'
        ORDER BY f.filled_at DESC, j.created_at DESC
        LIMIT 1
        "#,
    )
    .bind(account_id)
    .bind(position_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| PaperPositionCloseSummary {
        status: PaperCloseStatus::AlreadyClosed,
        position_id: row.get("position_id"),
        account_id: row.get("account_id"),
        symbol: row.get("symbol"),
        quantity: row.get("quantity"),
        entry_price: row.get("entry_price"),
        exit_price: row.get("exit_price"),
        realized_pnl: row.get("realized_pnl"),
        fee: row.get("fee"),
        slippage_cost: row.get("slippage_cost"),
        closed_at: row.get("closed_at"),
        correlation_id: row.get("correlation_id"),
        journal_entry_id: row.get("journal_entry_id"),
        close_fill_id: row.get("close_fill_id"),
    }))
}

pub async fn close_paper_position_transactional(
    pool: &PgPool,
    source: &str,
    actor: &StateActor,
    account: &PaperAccount,
    position: &PaperPosition,
    close_result: &PaperClosePositionResult,
    updated_account: &PaperAccount,
    updated_position: &PaperPosition,
    fill: &PaperFill,
    snapshot: &PaperEquitySnapshot,
    journal_entries: &[PaperTradeJournalEntry],
) -> Result<PaperPositionCloseSummary> {
    let mut tx = pool.begin().await?;
    let rationale = serde_json::to_string(&json!({
        "strategy_id": position.strategy_id,
        "symbol": position.symbol,
        "side": "SELL",
        "reason": "paper_position_close",
        "approved_notional": fill.notional,
        "close_position_id": position.id,
    }))?;

    sqlx::query(
        r#"
        INSERT INTO risk_decisions (id, correlation_id, signal_id, decision, rationale, decided_at)
        VALUES ($1, $2, $3, 'APPROVED', $4, $5)
        "#,
    )
    .bind(close_result.close_fill_id)
    .bind(close_result.correlation_id)
    .bind(position.signal_id)
    .bind(&rationale)
    .bind(close_result.closed_at)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
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
            $1, $2, $3, $4, $5, 'SELL', $6, $7, 'PAPER', 'FILLED', 'PAPER_FILLED', NULL, $7, $8, $8, NULL, NULL, NULL, NULL, $8, $8
        )
        "#,
    )
    .bind(fill.order_id)
    .bind(close_result.correlation_id)
    .bind(close_result.close_fill_id)
    .bind(format!("paper-close:{}", position.id))
    .bind(&position.symbol)
    .bind(position.quantity)
    .bind(fill.price)
    .bind(close_result.closed_at)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE paper_positions
        SET
            quantity = $2,
            mark_price = $3,
            price_status = $4,
            notional = $5,
            realized_pnl = $6,
            unrealized_pnl = $7,
            status = $8,
            closed_at = $9,
            order_id = $10,
            updated_at = $11
        WHERE id = $1
        "#,
    )
    .bind(updated_position.id)
    .bind(updated_position.quantity)
    .bind(updated_position.mark_price)
    .bind(updated_position.price_status.as_str())
    .bind(updated_position.notional)
    .bind(updated_position.realized_pnl)
    .bind(updated_position.unrealized_pnl)
    .bind(updated_position.status.as_str())
    .bind(updated_position.closed_at)
    .bind(fill.order_id)
    .bind(updated_position.updated_at)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE paper_accounts
        SET
            current_equity = $2,
            realized_pnl = $3,
            unrealized_pnl = $4,
            updated_at = $5
        WHERE id = $1
        "#,
    )
    .bind(updated_account.id)
    .bind(updated_account.current_equity)
    .bind(updated_account.realized_pnl)
    .bind(updated_account.unrealized_pnl)
    .bind(updated_account.updated_at)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO paper_fills (
            id, account_id, order_id, position_id, symbol, side, price, quantity, notional,
            fee, slippage_cost, filled_at, strategy_id, signal_id, risk_decision_id, correlation_id
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9,
            $10, $11, $12, $13, $14, $15, $16
        )
        "#,
    )
    .bind(fill.id)
    .bind(fill.account_id)
    .bind(fill.order_id)
    .bind(fill.position_id)
    .bind(&fill.symbol)
    .bind(fill.side.as_str())
    .bind(fill.price)
    .bind(fill.quantity)
    .bind(fill.notional)
    .bind(fill.fee)
    .bind(fill.slippage_cost)
    .bind(fill.filled_at)
    .bind(&fill.strategy_id)
    .bind(fill.signal_id)
    .bind(close_result.close_fill_id)
    .bind(fill.correlation_id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO paper_equity_snapshots (
            id, account_id, equity, realized_pnl, unrealized_pnl, drawdown_pct, snapshot_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(snapshot.id)
    .bind(snapshot.account_id)
    .bind(snapshot.equity)
    .bind(snapshot.realized_pnl)
    .bind(snapshot.unrealized_pnl)
    .bind(snapshot.drawdown_pct)
    .bind(snapshot.snapshot_at)
    .execute(&mut *tx)
    .await?;

    for entry in journal_entries {
        sqlx::query(
            r#"
            INSERT INTO paper_trade_journal (
                id, account_id, position_id, order_id, event_type, symbol, pnl, payload, created_at, correlation_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(entry.id)
        .bind(entry.account_id)
        .bind(entry.position_id)
        .bind(entry.order_id)
        .bind(&entry.event_type)
        .bind(&entry.symbol)
        .bind(entry.pnl)
        .bind(&entry.payload)
        .bind(entry.created_at)
        .bind(entry.correlation_id)
        .execute(&mut *tx)
        .await?;
    }

    let close_requested_payload = json!({
        "position_id": position.id,
        "account_id": account.id,
        "symbol": position.symbol,
        "quantity": position.quantity,
    });
    let fill_payload = json!({
        "fill_id": fill.id,
        "position_id": position.id,
        "symbol": position.symbol,
        "price": fill.price,
        "quantity": fill.quantity,
    });
    let closed_payload = serde_json::to_value(&close_result.summary)?;
    let equity_payload = json!({
        "account_id": updated_account.id,
        "equity": updated_account.current_equity,
        "realized_pnl": updated_account.realized_pnl,
        "unrealized_pnl": updated_account.unrealized_pnl,
    });
    for (event_type, payload) in [
        ("paper.position.close_requested", close_requested_payload),
        ("paper.fill.created", fill_payload),
        ("paper.position.closed", closed_payload),
        ("paper.equity.updated", equity_payload),
    ] {
        sqlx::query(
            r#"
            INSERT INTO system_events (id, correlation_id, event_type, source, payload, occurred_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(close_result.correlation_id)
        .bind(event_type)
        .bind(source)
        .bind(payload)
        .bind(close_result.closed_at)
        .execute(&mut *tx)
        .await?;
    }

    let metadata = json!({
        "account_id": account.id,
        "position_id": position.id,
        "symbol": position.symbol,
        "close_fill_id": fill.id,
        "order_id": fill.order_id,
        "correlation_id": close_result.correlation_id,
    });
    sqlx::query(
        r#"
        INSERT INTO audit_logs (id, correlation_id, actor, action, target, metadata)
        VALUES ($1, $2, $3, 'paper.position.close', $4, $5)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(close_result.correlation_id)
    .bind(&actor.actor)
    .bind(format!("paper_position:{}", position.id))
    .bind(metadata)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(PaperPositionCloseSummary {
        status: PaperCloseStatus::Closed,
        ..close_result.summary.clone()
    })
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

pub async fn upsert_candles_batch(
    pool: &PgPool,
    candles: &[Candle],
) -> Result<CandleUpsertBatchResult> {
    let deduped = dedupe_candles_for_upsert(candles);
    if deduped.is_empty() {
        return Ok(CandleUpsertBatchResult::default());
    }

    let first = deduped
        .first()
        .expect("deduped candle batch must contain at least one item");
    let open_times = deduped
        .iter()
        .map(|candle| candle.open_time)
        .collect::<Vec<_>>();

    let existing_rows = sqlx::query(
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
        WHERE exchange = $1
          AND symbol = $2
          AND interval = $3
          AND open_time = ANY($4)
        "#,
    )
    .bind(first.exchange.as_str())
    .bind(first.symbol.as_str())
    .bind(first.interval.as_str())
    .bind(&open_times)
    .fetch_all(pool)
    .await?;

    let existing = existing_rows
        .iter()
        .map(map_candle)
        .map(|record| (record.open_time, record))
        .collect::<BTreeMap<_, _>>();

    let mut outcome = CandleUpsertBatchResult::default();
    for candle in &deduped {
        match existing.get(&candle.open_time) {
            None => {
                upsert_candle(pool, candle).await?;
                outcome.inserted_candles += 1;
            }
            Some(record) if candle_matches_record(candle, record) => {
                outcome.skipped_candles += 1;
            }
            Some(_) => {
                upsert_candle(pool, candle).await?;
                outcome.updated_candles += 1;
            }
        }
    }

    Ok(outcome)
}

pub fn dedupe_candles_for_upsert(candles: &[Candle]) -> Vec<Candle> {
    let mut deduped = BTreeMap::new();
    for candle in candles {
        deduped.insert(candle.open_time, candle.clone());
    }
    deduped.into_values().collect()
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

pub async fn get_recent_closed_candles(
    pool: &PgPool,
    symbol: &Symbol,
    interval: CandleInterval,
    limit: i64,
) -> Result<Vec<Candle>> {
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
        WHERE symbol = $1 AND interval = $2 AND is_closed = TRUE
        ORDER BY open_time DESC
        LIMIT $3
        "#,
    )
    .bind(symbol.as_str())
    .bind(interval.as_str())
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let mut candles = rows.iter().map(map_candle_domain).collect::<Vec<_>>();
    candles.sort_by_key(|candle| candle.open_time);
    Ok(candles)
}

pub async fn get_closed_candles_range(
    pool: &PgPool,
    symbol: &Symbol,
    interval: CandleInterval,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> Result<Vec<Candle>> {
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
        WHERE symbol = $1
          AND interval = $2
          AND is_closed = TRUE
          AND open_time >= $3
          AND close_time <= $4
        ORDER BY open_time ASC
        "#,
    )
    .bind(symbol.as_str())
    .bind(interval.as_str())
    .bind(start_time)
    .bind(end_time)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_candle_domain).collect())
}

pub async fn count_candles_range(
    pool: &PgPool,
    exchange: MarketDataSource,
    symbol: &Symbol,
    interval: CandleInterval,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> Result<i64> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM candles
        WHERE exchange = $1
          AND symbol = $2
          AND interval = $3
          AND is_closed = TRUE
          AND open_time >= $4
          AND close_time <= $5
        "#,
    )
    .bind(exchange.as_str())
    .bind(symbol.as_str())
    .bind(interval.as_str())
    .bind(start_time)
    .bind(end_time)
    .fetch_one(pool)
    .await?;

    Ok(count)
}

pub async fn get_closed_1m_candles_range(
    pool: &PgPool,
    exchange: MarketDataSource,
    symbol: &Symbol,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> Result<Vec<Candle>> {
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
        WHERE exchange = $1
          AND symbol = $2
          AND interval = $3
          AND is_closed = TRUE
          AND open_time >= $4
          AND close_time <= $5
        ORDER BY open_time ASC
        "#,
    )
    .bind(exchange.as_str())
    .bind(symbol.as_str())
    .bind(CandleInterval::OneMinute.as_str())
    .bind(start_time)
    .bind(end_time)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_candle_domain).collect())
}

pub async fn upsert_aggregated_candles(
    pool: &PgPool,
    candles: &[Candle],
) -> Result<CandleUpsertBatchResult> {
    upsert_candles_batch(pool, candles).await
}

pub async fn count_candles_by_interval(
    pool: &PgPool,
    exchange: MarketDataSource,
    symbol: &Symbol,
    interval: CandleInterval,
) -> Result<i64> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM candles
        WHERE exchange = $1
          AND symbol = $2
          AND interval = $3
          AND is_closed = TRUE
        "#,
    )
    .bind(exchange.as_str())
    .bind(symbol.as_str())
    .bind(interval.as_str())
    .fetch_one(pool)
    .await?;

    Ok(count)
}

pub async fn get_aggregated_candle_coverage(
    pool: &PgPool,
    exchange: MarketDataSource,
    symbol: &Symbol,
) -> Result<MarketCandleCoverageSummary> {
    let intervals = [
        CandleInterval::OneMinute,
        CandleInterval::FiveMinutes,
        CandleInterval::FifteenMinutes,
        CandleInterval::OneHour,
    ];
    let mut coverage = Vec::with_capacity(intervals.len());

    for interval in intervals {
        coverage.push(MarketCandleIntervalCoverage {
            interval: interval.as_str().to_string(),
            candle_count: count_candles_by_interval(pool, exchange, symbol, interval).await?,
        });
    }

    Ok(MarketCandleCoverageSummary {
        exchange,
        symbol: symbol.as_str().to_string(),
        intervals: coverage,
    })
}

pub async fn insert_candle_backfill_run(
    pool: &PgPool,
    run_id: Uuid,
    request: &CandleBackfillRequest,
    correlation_id: Uuid,
    created_at: DateTime<Utc>,
    config: Value,
) -> Result<CandleBackfillRunRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO candle_backfill_runs (
            id,
            exchange,
            symbol,
            interval,
            start_time,
            end_time,
            status,
            requested_candles_estimate,
            fetched_candles,
            inserted_candles,
            updated_candles,
            skipped_candles,
            failed_reason,
            correlation_id,
            created_at,
            completed_at,
            config
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, 0, 0, 0, 0, NULL, $9, $10, NULL, $11
        )
        RETURNING
            id,
            exchange,
            symbol,
            interval,
            start_time,
            end_time,
            status,
            requested_candles_estimate,
            fetched_candles,
            inserted_candles,
            updated_candles,
            skipped_candles,
            failed_reason,
            correlation_id,
            created_at,
            completed_at,
            config
        "#,
    )
    .bind(run_id)
    .bind(request.exchange.as_str())
    .bind(&request.symbol)
    .bind(&request.interval)
    .bind(request.start_time)
    .bind(request.end_time)
    .bind(CandleBackfillStatus::Running.as_str())
    .bind(request.requested_candles_estimate()?)
    .bind(correlation_id)
    .bind(created_at)
    .bind(config)
    .fetch_one(pool)
    .await?;

    Ok(map_candle_backfill_run(&row))
}

pub async fn update_candle_backfill_progress(
    pool: &PgPool,
    run_id: Uuid,
    progress: &CandleBackfillProgress,
) -> Result<CandleBackfillRunRecord> {
    let row = sqlx::query(
        r#"
        UPDATE candle_backfill_runs
        SET
            fetched_candles = fetched_candles + $2,
            inserted_candles = inserted_candles + $3,
            updated_candles = updated_candles + $4,
            skipped_candles = skipped_candles + $5
        WHERE id = $1
        RETURNING
            id,
            exchange,
            symbol,
            interval,
            start_time,
            end_time,
            status,
            requested_candles_estimate,
            fetched_candles,
            inserted_candles,
            updated_candles,
            skipped_candles,
            failed_reason,
            correlation_id,
            created_at,
            completed_at,
            config
        "#,
    )
    .bind(run_id)
    .bind(progress.fetched_candles)
    .bind(progress.inserted_candles)
    .bind(progress.updated_candles)
    .bind(progress.skipped_candles)
    .fetch_one(pool)
    .await?;

    Ok(map_candle_backfill_run(&row))
}

pub async fn complete_candle_backfill_run(
    pool: &PgPool,
    run_id: Uuid,
    completed_at: DateTime<Utc>,
) -> Result<CandleBackfillRunRecord> {
    let row = sqlx::query(
        r#"
        UPDATE candle_backfill_runs
        SET
            status = $2,
            completed_at = $3
        WHERE id = $1
        RETURNING
            id,
            exchange,
            symbol,
            interval,
            start_time,
            end_time,
            status,
            requested_candles_estimate,
            fetched_candles,
            inserted_candles,
            updated_candles,
            skipped_candles,
            failed_reason,
            correlation_id,
            created_at,
            completed_at,
            config
        "#,
    )
    .bind(run_id)
    .bind(CandleBackfillStatus::Completed.as_str())
    .bind(completed_at)
    .fetch_one(pool)
    .await?;

    Ok(map_candle_backfill_run(&row))
}

pub async fn fail_candle_backfill_run(
    pool: &PgPool,
    run_id: Uuid,
    failed_reason: &str,
    completed_at: DateTime<Utc>,
) -> Result<CandleBackfillRunRecord> {
    let row = sqlx::query(
        r#"
        UPDATE candle_backfill_runs
        SET
            status = $2,
            failed_reason = $3,
            completed_at = $4
        WHERE id = $1
        RETURNING
            id,
            exchange,
            symbol,
            interval,
            start_time,
            end_time,
            status,
            requested_candles_estimate,
            fetched_candles,
            inserted_candles,
            updated_candles,
            skipped_candles,
            failed_reason,
            correlation_id,
            created_at,
            completed_at,
            config
        "#,
    )
    .bind(run_id)
    .bind(CandleBackfillStatus::Failed.as_str())
    .bind(failed_reason)
    .bind(completed_at)
    .fetch_one(pool)
    .await?;

    Ok(map_candle_backfill_run(&row))
}

pub async fn list_candle_backfill_runs(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<CandleBackfillRunRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            exchange,
            symbol,
            interval,
            start_time,
            end_time,
            status,
            requested_candles_estimate,
            fetched_candles,
            inserted_candles,
            updated_candles,
            skipped_candles,
            failed_reason,
            correlation_id,
            created_at,
            completed_at,
            config
        FROM candle_backfill_runs
        ORDER BY created_at DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_candle_backfill_run).collect())
}

pub async fn get_candle_backfill_run(
    pool: &PgPool,
    run_id: Uuid,
) -> Result<Option<CandleBackfillRunRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            exchange,
            symbol,
            interval,
            start_time,
            end_time,
            status,
            requested_candles_estimate,
            fetched_candles,
            inserted_candles,
            updated_candles,
            skipped_candles,
            failed_reason,
            correlation_id,
            created_at,
            completed_at,
            config
        FROM candle_backfill_runs
        WHERE id = $1
        "#,
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_candle_backfill_run))
}

pub async fn insert_backtest_run(
    pool: &PgPool,
    run_id: Uuid,
    request: &aegis_core::BacktestRequest,
    config: &BacktestConfig,
    created_at: DateTime<Utc>,
    status: ReplayRunStatus,
    correlation_id: Option<Uuid>,
) -> Result<BacktestRunRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO backtest_runs (
            id,
            strategy_id,
            symbol,
            timeframe,
            start_time,
            end_time,
            initial_capital,
            final_equity,
            pnl,
            pnl_pct,
            max_drawdown_pct,
            win_rate,
            trade_count,
            winning_trades,
            losing_trades,
            avg_win,
            avg_loss,
            fee_paid,
            slippage_cost,
            status,
            config,
            correlation_id,
            created_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            $8, $9, $10, $11
        )
        RETURNING
            id,
            strategy_id,
            symbol,
            timeframe,
            start_time,
            end_time,
            initial_capital,
            final_equity,
            pnl,
            pnl_pct,
            max_drawdown_pct,
            win_rate,
            trade_count,
            winning_trades,
            losing_trades,
            avg_win,
            avg_loss,
            fee_paid,
            slippage_cost,
            status,
            config,
            correlation_id,
            created_at
        "#,
    )
    .bind(run_id)
    .bind(&request.strategy_id)
    .bind(&request.symbol)
    .bind(&request.timeframe)
    .bind(request.start_time)
    .bind(request.end_time)
    .bind(request.initial_capital)
    .bind(status.as_str())
    .bind(serde_json::to_value(config)?)
    .bind(correlation_id)
    .bind(created_at)
    .fetch_one(pool)
    .await?;

    Ok(map_backtest_run(&row))
}

pub async fn update_backtest_run_completed(
    pool: &PgPool,
    result: &BacktestResult,
    config: &BacktestConfig,
) -> Result<BacktestRunRecord> {
    let row = sqlx::query(
        r#"
        UPDATE backtest_runs
        SET
            final_equity = $2,
            pnl = $3,
            pnl_pct = $4,
            max_drawdown_pct = $5,
            win_rate = $6,
            trade_count = $7,
            winning_trades = $8,
            losing_trades = $9,
            avg_win = $10,
            avg_loss = $11,
            fee_paid = $12,
            slippage_cost = $13,
            status = $14,
            config = $15,
            correlation_id = $16
        WHERE id = $1
        RETURNING
            id,
            strategy_id,
            symbol,
            timeframe,
            start_time,
            end_time,
            initial_capital,
            final_equity,
            pnl,
            pnl_pct,
            max_drawdown_pct,
            win_rate,
            trade_count,
            winning_trades,
            losing_trades,
            avg_win,
            avg_loss,
            fee_paid,
            slippage_cost,
            status,
            config,
            correlation_id,
            created_at
        "#,
    )
    .bind(result.run_id)
    .bind(result.final_equity)
    .bind(result.pnl)
    .bind(result.pnl_pct)
    .bind(result.max_drawdown_pct)
    .bind(result.win_rate)
    .bind(result.trade_count)
    .bind(result.winning_trades)
    .bind(result.losing_trades)
    .bind(result.avg_win)
    .bind(result.avg_loss)
    .bind(result.fee_paid)
    .bind(result.slippage_cost)
    .bind(result.status.as_str())
    .bind(serde_json::to_value(config)?)
    .bind(result.correlation_id)
    .fetch_one(pool)
    .await?;

    Ok(map_backtest_run(&row))
}

pub async fn insert_backtest_trade(
    pool: &PgPool,
    trade: &BacktestTrade,
) -> Result<BacktestTradeRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO backtest_trades (
            id,
            run_id,
            strategy_id,
            symbol,
            side,
            entry_time,
            entry_price,
            exit_time,
            exit_price,
            quantity,
            notional,
            fee_paid,
            slippage_cost,
            realized_pnl,
            reason,
            created_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9,
            $10, $11, $12, $13, $14, $15, $16
        )
        RETURNING
            id,
            run_id,
            strategy_id,
            symbol,
            side,
            entry_time,
            entry_price,
            exit_time,
            exit_price,
            quantity,
            notional,
            fee_paid,
            slippage_cost,
            realized_pnl,
            reason,
            created_at
        "#,
    )
    .bind(trade.id)
    .bind(trade.run_id)
    .bind(&trade.strategy_id)
    .bind(&trade.symbol)
    .bind(format!("{:?}", trade.side).to_ascii_uppercase())
    .bind(trade.entry_time)
    .bind(trade.entry_price)
    .bind(trade.exit_time)
    .bind(trade.exit_price)
    .bind(trade.quantity)
    .bind(trade.notional)
    .bind(trade.fee_paid)
    .bind(trade.slippage_cost)
    .bind(trade.realized_pnl)
    .bind(&trade.reason)
    .bind(trade.created_at)
    .fetch_one(pool)
    .await?;

    Ok(map_backtest_trade(&row))
}

pub async fn insert_backtest_equity_points(
    pool: &PgPool,
    points: &[BacktestEquityPoint],
) -> Result<Vec<BacktestEquityPointRecord>> {
    let mut records = Vec::with_capacity(points.len());
    for point in points {
        let row = sqlx::query(
            r#"
            INSERT INTO backtest_equity_curve (
                id,
                run_id,
                timestamp,
                equity,
                drawdown_pct
            )
            VALUES ($1, $2, $3, $4, $5)
            RETURNING
                id,
                run_id,
                timestamp,
                equity,
                drawdown_pct
            "#,
        )
        .bind(point.id)
        .bind(point.run_id)
        .bind(point.timestamp)
        .bind(point.equity)
        .bind(point.drawdown_pct)
        .fetch_one(pool)
        .await?;
        records.push(map_backtest_equity_point(&row));
    }
    Ok(records)
}

pub async fn get_backtest_run(pool: &PgPool, run_id: Uuid) -> Result<Option<BacktestRunRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            experiment_group_id,
            strategy_id,
            symbol,
            timeframe,
            start_time,
            end_time,
            initial_capital,
            final_equity,
            pnl,
            pnl_pct,
            max_drawdown_pct,
            win_rate,
            trade_count,
            winning_trades,
            losing_trades,
            avg_win,
            avg_loss,
            fee_paid,
            slippage_cost,
            status,
            config,
            correlation_id,
            created_at
        FROM backtest_runs
        WHERE id = $1
        "#,
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_backtest_run))
}

pub async fn list_backtest_runs(pool: &PgPool, limit: i64) -> Result<Vec<BacktestRunRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            experiment_group_id,
            strategy_id,
            symbol,
            timeframe,
            start_time,
            end_time,
            initial_capital,
            final_equity,
            pnl,
            pnl_pct,
            max_drawdown_pct,
            win_rate,
            trade_count,
            winning_trades,
            losing_trades,
            avg_win,
            avg_loss,
            fee_paid,
            slippage_cost,
            status,
            config,
            correlation_id,
            created_at
        FROM backtest_runs
        ORDER BY created_at DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_backtest_run).collect())
}

pub async fn count_backtest_runs_in_window(
    pool: &PgPool,
    strategy_id: Option<&str>,
    symbol: Option<&str>,
    timeframe: Option<&str>,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM backtest_runs
        WHERE created_at >= $1
          AND created_at <= $2
          AND ($3::TEXT IS NULL OR strategy_id = $3)
          AND ($4::TEXT IS NULL OR symbol = $4)
          AND ($5::TEXT IS NULL OR timeframe = $5)
        "#,
    )
    .bind(start_time)
    .bind(end_time)
    .bind(strategy_id)
    .bind(symbol)
    .bind(timeframe)
    .fetch_one(pool)
    .await?)
}

pub async fn insert_strategy_experiment(
    pool: &PgPool,
    result: &StrategyExperimentResult,
) -> Result<StrategyExperimentRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO strategy_experiments (
            id,
            experiment_group_id,
            strategy_id,
            symbol,
            timeframe,
            start_time,
            end_time,
            initial_capital,
            fee_bps,
            slippage_bps,
            max_signal_age_ms,
            max_runs,
            status,
            comparison,
            candle_count,
            warnings,
            skipped_reason,
            correlation_id,
            created_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
        )
        RETURNING
            id,
            experiment_group_id,
            strategy_id,
            symbol,
            timeframe,
            start_time,
            end_time,
            initial_capital,
            fee_bps,
            slippage_bps,
            max_signal_age_ms,
            max_runs,
            status,
            comparison,
            candle_count,
            warnings,
            skipped_reason,
            correlation_id,
            created_at
        "#,
    )
    .bind(result.experiment_id)
    .bind(result.experiment_group_id)
    .bind(&result.strategy_id)
    .bind(&result.symbol)
    .bind(&result.timeframe)
    .bind(result.start_time)
    .bind(result.end_time)
    .bind(result.initial_capital)
    .bind(result.fee_bps)
    .bind(result.slippage_bps)
    .bind(result.max_signal_age_ms)
    .bind(result.max_runs.map(|value| value as i32))
    .bind(result.status.as_str())
    .bind(serde_json::to_value(&result.comparison)?)
    .bind(result.candle_count)
    .bind(serde_json::to_value(&result.warnings)?)
    .bind(&result.skipped_reason)
    .bind(result.correlation_id)
    .bind(result.created_at)
    .fetch_one(pool)
    .await?;

    Ok(map_strategy_experiment(&row))
}

pub async fn insert_strategy_experiment_runs(
    pool: &PgPool,
    runs: &[StrategyExperimentRun],
) -> Result<Vec<StrategyExperimentRunRecord>> {
    let mut records = Vec::with_capacity(runs.len());
    for run in runs {
        let row = sqlx::query(
            r#"
            INSERT INTO strategy_experiment_runs (
                id,
                experiment_id,
                rank,
                candidate_config,
                final_equity,
                pnl,
                pnl_pct,
                max_drawdown_pct,
                win_rate,
                trade_count,
                fee_paid,
                slippage_cost,
                fee_slippage_drag_pct,
                score,
                status,
                warnings,
                created_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
            )
            RETURNING
                id,
                experiment_id,
                rank,
                candidate_config,
                final_equity,
                pnl,
                pnl_pct,
                max_drawdown_pct,
                win_rate,
                trade_count,
                fee_paid,
                slippage_cost,
                fee_slippage_drag_pct,
                score,
                status,
                warnings,
                created_at
            "#,
        )
        .bind(run.id)
        .bind(run.experiment_id)
        .bind(run.rank)
        .bind(serde_json::to_value(&run.candidate)?)
        .bind(run.final_equity)
        .bind(run.pnl)
        .bind(run.pnl_pct)
        .bind(run.max_drawdown_pct)
        .bind(run.win_rate)
        .bind(run.trade_count)
        .bind(run.fee_paid)
        .bind(run.slippage_cost)
        .bind(run.fee_slippage_drag_pct)
        .bind(run.score)
        .bind(run.status.as_str())
        .bind(serde_json::to_value(&run.warnings)?)
        .bind(run.created_at)
        .fetch_one(pool)
        .await?;
        records.push(map_strategy_experiment_run(&row));
    }
    Ok(records)
}

pub async fn insert_strategy_walk_forward_run(
    pool: &PgPool,
    request: &aegis_core::StrategyWalkForwardRequest,
    result: &StrategyWalkForwardResult,
) -> Result<StrategyWalkForwardRunRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO strategy_walk_forward_runs (
            id,
            strategy_id,
            symbol,
            timeframe,
            request,
            status,
            total_windows,
            completed_windows,
            skipped_windows,
            profitable_test_windows,
            losing_test_windows,
            avg_test_pnl_pct,
            median_test_pnl_pct,
            worst_test_pnl_pct,
            best_test_pnl_pct,
            avg_max_drawdown_pct,
            robustness_score,
            robustness_summary,
            created_at,
            correlation_id
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
        )
        RETURNING
            id,
            strategy_id,
            symbol,
            timeframe,
            request,
            status,
            total_windows,
            completed_windows,
            skipped_windows,
            profitable_test_windows,
            losing_test_windows,
            avg_test_pnl_pct,
            median_test_pnl_pct,
            worst_test_pnl_pct,
            best_test_pnl_pct,
            avg_max_drawdown_pct,
            robustness_score,
            robustness_summary,
            created_at,
            correlation_id
        "#,
    )
    .bind(result.walk_forward_id)
    .bind(&result.strategy_id)
    .bind(&result.symbol)
    .bind(&result.timeframe)
    .bind(serde_json::to_value(request)?)
    .bind(result.status.as_str())
    .bind(result.total_windows)
    .bind(result.completed_windows)
    .bind(result.skipped_windows)
    .bind(result.profitable_test_windows)
    .bind(result.losing_test_windows)
    .bind(result.avg_test_pnl_pct)
    .bind(result.median_test_pnl_pct)
    .bind(result.worst_test_pnl_pct)
    .bind(result.best_test_pnl_pct)
    .bind(result.avg_max_drawdown_pct)
    .bind(result.robustness_score)
    .bind(serde_json::to_value(&result.robustness_summary)?)
    .bind(result.created_at)
    .bind(result.correlation_id)
    .fetch_one(pool)
    .await?;

    Ok(map_strategy_walk_forward_run(&row))
}

pub async fn insert_strategy_walk_forward_windows(
    pool: &PgPool,
    windows: &[StrategyWalkForwardWindowResult],
) -> Result<Vec<StrategyWalkForwardWindowRecord>> {
    let mut records = Vec::with_capacity(windows.len());
    for window in windows {
        let row = sqlx::query(
            r#"
            INSERT INTO strategy_walk_forward_windows (
                id,
                walk_forward_id,
                window_index,
                train_start,
                train_end,
                test_start,
                test_end,
                status,
                skip_reason,
                trade_count,
                pnl,
                pnl_pct,
                max_drawdown_pct,
                win_rate,
                fee_paid,
                slippage_cost,
                result,
                created_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9,
                $10, $11, $12, $13, $14, $15, $16, $17, $18
            )
            RETURNING
                id,
                walk_forward_id,
                window_index,
                train_start,
                train_end,
                test_start,
                test_end,
                status,
                skip_reason,
                trade_count,
                pnl,
                pnl_pct,
                max_drawdown_pct,
                win_rate,
                fee_paid,
                slippage_cost,
                result,
                created_at
            "#,
        )
        .bind(window.id)
        .bind(window.walk_forward_id)
        .bind(window.window.window_index)
        .bind(window.window.train_start)
        .bind(window.window.train_end)
        .bind(window.window.test_start)
        .bind(window.window.test_end)
        .bind(window.status.as_str())
        .bind(&window.skip_reason)
        .bind(window.trade_count)
        .bind(window.pnl)
        .bind(window.pnl_pct)
        .bind(window.max_drawdown_pct)
        .bind(window.win_rate)
        .bind(window.fee_paid)
        .bind(window.slippage_cost)
        .bind(&window.result)
        .bind(window.created_at)
        .fetch_one(pool)
        .await?;
        records.push(map_strategy_walk_forward_window(&row));
    }

    Ok(records)
}

pub async fn get_strategy_experiment(
    pool: &PgPool,
    experiment_id: Uuid,
) -> Result<Option<StrategyExperimentRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            experiment_group_id,
            strategy_id,
            symbol,
            timeframe,
            start_time,
            end_time,
            initial_capital,
            fee_bps,
            slippage_bps,
            max_signal_age_ms,
            max_runs,
            status,
            comparison,
            candle_count,
            warnings,
            skipped_reason,
            correlation_id,
            created_at
        FROM strategy_experiments
        WHERE id = $1
        "#,
    )
    .bind(experiment_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_strategy_experiment))
}

pub async fn list_strategy_experiments(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<StrategyExperimentRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            experiment_group_id,
            strategy_id,
            symbol,
            timeframe,
            start_time,
            end_time,
            initial_capital,
            fee_bps,
            slippage_bps,
            max_signal_age_ms,
            max_runs,
            status,
            comparison,
            candle_count,
            warnings,
            skipped_reason,
            correlation_id,
            created_at
        FROM strategy_experiments
        ORDER BY created_at DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_strategy_experiment).collect())
}

pub async fn list_strategy_experiments_by_group(
    pool: &PgPool,
    experiment_group_id: Uuid,
) -> Result<Vec<StrategyExperimentRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            experiment_group_id,
            strategy_id,
            symbol,
            timeframe,
            start_time,
            end_time,
            initial_capital,
            fee_bps,
            slippage_bps,
            max_signal_age_ms,
            max_runs,
            status,
            comparison,
            candle_count,
            warnings,
            skipped_reason,
            correlation_id,
            created_at
        FROM strategy_experiments
        WHERE experiment_group_id = $1
        ORDER BY created_at ASC, timeframe ASC
        "#,
    )
    .bind(experiment_group_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_strategy_experiment).collect())
}

pub async fn list_strategy_experiment_runs(
    pool: &PgPool,
    experiment_id: Uuid,
) -> Result<Vec<StrategyExperimentRunRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            experiment_id,
            rank,
            candidate_config,
            final_equity,
            pnl,
            pnl_pct,
            max_drawdown_pct,
            win_rate,
            trade_count,
            fee_paid,
            slippage_cost,
            fee_slippage_drag_pct,
            score,
            status,
            warnings,
            created_at
        FROM strategy_experiment_runs
        WHERE experiment_id = $1
        ORDER BY rank ASC, created_at ASC
        "#,
    )
    .bind(experiment_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_strategy_experiment_run).collect())
}

pub async fn get_strategy_experiment_run(
    pool: &PgPool,
    run_id: Uuid,
) -> Result<Option<StrategyExperimentRunRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            experiment_id,
            rank,
            candidate_config,
            final_equity,
            pnl,
            pnl_pct,
            max_drawdown_pct,
            win_rate,
            trade_count,
            fee_paid,
            slippage_cost,
            fee_slippage_drag_pct,
            score,
            status,
            warnings,
            created_at
        FROM strategy_experiment_runs
        WHERE id = $1
        "#,
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_strategy_experiment_run))
}

pub async fn get_strategy_walk_forward_run(
    pool: &PgPool,
    walk_forward_id: Uuid,
) -> Result<Option<StrategyWalkForwardRunRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            strategy_id,
            symbol,
            timeframe,
            request,
            status,
            total_windows,
            completed_windows,
            skipped_windows,
            profitable_test_windows,
            losing_test_windows,
            avg_test_pnl_pct,
            median_test_pnl_pct,
            worst_test_pnl_pct,
            best_test_pnl_pct,
            avg_max_drawdown_pct,
            robustness_score,
            robustness_summary,
            created_at,
            correlation_id
        FROM strategy_walk_forward_runs
        WHERE id = $1
        "#,
    )
    .bind(walk_forward_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_strategy_walk_forward_run))
}

pub async fn list_strategy_walk_forward_runs(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<StrategyWalkForwardRunRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            strategy_id,
            symbol,
            timeframe,
            request,
            status,
            total_windows,
            completed_windows,
            skipped_windows,
            profitable_test_windows,
            losing_test_windows,
            avg_test_pnl_pct,
            median_test_pnl_pct,
            worst_test_pnl_pct,
            best_test_pnl_pct,
            avg_max_drawdown_pct,
            robustness_score,
            robustness_summary,
            created_at,
            correlation_id
        FROM strategy_walk_forward_runs
        ORDER BY created_at DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_strategy_walk_forward_run).collect())
}

pub async fn list_strategy_walk_forward_windows(
    pool: &PgPool,
    walk_forward_id: Uuid,
) -> Result<Vec<StrategyWalkForwardWindowRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            walk_forward_id,
            window_index,
            train_start,
            train_end,
            test_start,
            test_end,
            status,
            skip_reason,
            trade_count,
            pnl,
            pnl_pct,
            max_drawdown_pct,
            win_rate,
            fee_paid,
            slippage_cost,
            result,
            created_at
        FROM strategy_walk_forward_windows
        WHERE walk_forward_id = $1
        ORDER BY window_index ASC, created_at ASC
        "#,
    )
    .bind(walk_forward_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_strategy_walk_forward_window).collect())
}

pub async fn get_backtest_trades(pool: &PgPool, run_id: Uuid) -> Result<Vec<BacktestTradeRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            run_id,
            strategy_id,
            symbol,
            side,
            entry_time,
            entry_price,
            exit_time,
            exit_price,
            quantity,
            notional,
            fee_paid,
            slippage_cost,
            realized_pnl,
            reason,
            created_at
        FROM backtest_trades
        WHERE run_id = $1
        ORDER BY entry_time ASC, created_at ASC
        "#,
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_backtest_trade).collect())
}

pub async fn get_backtest_equity_curve(
    pool: &PgPool,
    run_id: Uuid,
) -> Result<Vec<BacktestEquityPointRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            run_id,
            timestamp,
            equity,
            drawdown_pct
        FROM backtest_equity_curve
        WHERE run_id = $1
        ORDER BY timestamp ASC
        "#,
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_backtest_equity_point).collect())
}

const ANALYTICS_DEFAULT_LIMIT: i64 = 20;
const ANALYTICS_MAX_LIMIT: i64 = 100;
const ANALYTICS_DEFAULT_WINDOW_DAYS: i64 = 7;
const TESTNET_PROMOTION_FUNNEL_DEFAULT_LIMIT: i64 = 100;
const TESTNET_PROMOTION_FUNNEL_MAX_LIMIT: i64 = 1000;

#[derive(Debug, Clone)]
struct TestnetPromotionFunnelMaterializedRow {
    shadow_run_id: Uuid,
    promotion_id: Option<Uuid>,
    strategy_id: String,
    symbol: String,
    timeframe: String,
    promotion_status: Option<String>,
    promotion_rejection_reasons: Vec<String>,
    testnet_order_id: Option<Uuid>,
    client_order_id: Option<String>,
    effective_execution_state: Option<TestnetExecutionState>,
    linked_order_missing: bool,
    shadow_created_at: DateTime<Utc>,
    promotion_created_at: Option<DateTime<Utc>>,
    submitted_at: Option<DateTime<Utc>>,
    acked_at: Option<DateTime<Utc>>,
    last_lifecycle_at: Option<DateTime<Utc>>,
}

fn bounded_analytics_limit(limit: Option<i64>) -> i64 {
    match limit {
        Some(value) if value > 0 => value.min(ANALYTICS_MAX_LIMIT),
        _ => ANALYTICS_DEFAULT_LIMIT,
    }
}

fn analytics_window(
    request: &StrategyPerformanceRequest,
) -> (DateTime<Utc>, DateTime<Utc>, DateTime<Utc>) {
    let computed_at = Utc::now();
    let end_time = request.end_time.unwrap_or(computed_at);
    let start_time = request
        .start_time
        .unwrap_or_else(|| end_time - Duration::days(ANALYTICS_DEFAULT_WINDOW_DAYS));
    if start_time <= end_time {
        (start_time, end_time, computed_at)
    } else {
        (end_time, start_time, computed_at)
    }
}

fn bounded_testnet_promotion_funnel_limit(limit: Option<i64>) -> i64 {
    match limit {
        Some(value) if value > 0 => value.min(TESTNET_PROMOTION_FUNNEL_MAX_LIMIT),
        _ => TESTNET_PROMOTION_FUNNEL_DEFAULT_LIMIT,
    }
}

fn empty_testnet_promotion_funnel_summary(
    request: &TestnetPromotionFunnelRequest,
    computed_at: DateTime<Utc>,
) -> TestnetPromotionFunnelSummary {
    TestnetPromotionFunnelSummary {
        strategy_id: request.strategy_id.clone(),
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        window_start: request.start_time,
        window_end: request.end_time,
        shadow_would_submit_count: 0,
        promotion_previewed_count: 0,
        promotion_submitted_count: 0,
        promotion_rejected_count: 0,
        promotion_expired_count: 0,
        promotion_duplicate_rejected_count: 0,
        testnet_orders_created_count: 0,
        acked_count: 0,
        filled_count: 0,
        partially_filled_count: 0,
        cancelled_count: 0,
        rejected_count: 0,
        expired_count: 0,
        reconciliation_required_count: 0,
        unknown_exchange_state_count: 0,
        failed_count: 0,
        preview_rate_pct: Decimal::ZERO,
        submit_rate_pct: Decimal::ZERO,
        ack_rate_pct: Decimal::ZERO,
        fill_rate_pct: Decimal::ZERO,
        reconciliation_required_rate_pct: Decimal::ZERO,
        avg_time_shadow_to_preview_seconds: None,
        avg_time_preview_to_submit_seconds: None,
        stages: Vec::new(),
        outcome_breakdown: Vec::new(),
        dropoff_breakdown: Vec::new(),
        lifecycle_breakdown: Vec::new(),
        quality_signals: Vec::new(),
        computed_at,
    }
}

fn duration_seconds_decimal(start: DateTime<Utc>, end: DateTime<Utc>) -> Option<Decimal> {
    let millis = end.signed_duration_since(start).num_milliseconds();
    if millis < 0 {
        None
    } else {
        Some(Decimal::from(millis) / Decimal::from(1000))
    }
}

fn row_counts_as_submitted(row: &TestnetPromotionFunnelMaterializedRow) -> bool {
    row.submitted_at.is_some() || row.client_order_id.is_some() || row.testnet_order_id.is_some()
}

fn row_counts_as_duplicate_rejected(row: &TestnetPromotionFunnelMaterializedRow) -> bool {
    matches!(row.promotion_status.as_deref(), Some("ALREADY_PROMOTED"))
        || row
            .promotion_rejection_reasons
            .iter()
            .any(|reason| matches!(reason.as_str(), "duplicate_submit" | "already_promoted"))
}

fn build_testnet_promotion_funnel_stage_breakdown(
    summary: &TestnetPromotionFunnelSummary,
) -> Vec<TestnetPromotionFunnelStage> {
    vec![
        TestnetPromotionFunnelStage {
            stage: "shadow_would_submit".to_string(),
            count: summary.shadow_would_submit_count,
            rate_pct: if summary.shadow_would_submit_count > 0 {
                Decimal::from(100)
            } else {
                Decimal::ZERO
            },
        },
        TestnetPromotionFunnelStage {
            stage: "promotion_previewed".to_string(),
            count: summary.promotion_previewed_count,
            rate_pct: calculate_testnet_promotion_rate(
                summary.promotion_previewed_count,
                summary.shadow_would_submit_count,
            ),
        },
        TestnetPromotionFunnelStage {
            stage: "promotion_submitted".to_string(),
            count: summary.promotion_submitted_count,
            rate_pct: calculate_testnet_promotion_rate(
                summary.promotion_submitted_count,
                summary.promotion_previewed_count,
            ),
        },
        TestnetPromotionFunnelStage {
            stage: "acked".to_string(),
            count: summary.acked_count,
            rate_pct: calculate_testnet_promotion_rate(
                summary.acked_count,
                summary.testnet_orders_created_count,
            ),
        },
        TestnetPromotionFunnelStage {
            stage: "filled".to_string(),
            count: summary.filled_count,
            rate_pct: calculate_testnet_promotion_rate(summary.filled_count, summary.acked_count),
        },
        TestnetPromotionFunnelStage {
            stage: "reconciliation_required".to_string(),
            count: summary.reconciliation_required_count,
            rate_pct: calculate_testnet_promotion_rate(
                summary.reconciliation_required_count,
                summary.testnet_orders_created_count,
            ),
        },
    ]
}

fn build_testnet_promotion_dropoff_breakdown(
    summary: &TestnetPromotionFunnelSummary,
) -> Vec<TestnetPromotionDropoffBreakdown> {
    let shadow_to_preview =
        (summary.shadow_would_submit_count - summary.promotion_previewed_count).max(0);
    let preview_to_submit =
        (summary.promotion_previewed_count - summary.promotion_submitted_count).max(0);
    let submit_to_ack = (summary.promotion_submitted_count - summary.acked_count).max(0);
    let ack_to_fill = (summary.acked_count - summary.filled_count).max(0);

    vec![
        TestnetPromotionDropoffBreakdown {
            stage: "shadow_to_preview".to_string(),
            dropped_count: shadow_to_preview,
            dropoff_rate_pct: calculate_testnet_promotion_rate(
                shadow_to_preview,
                summary.shadow_would_submit_count,
            ),
        },
        TestnetPromotionDropoffBreakdown {
            stage: "preview_to_submit".to_string(),
            dropped_count: preview_to_submit,
            dropoff_rate_pct: calculate_testnet_promotion_rate(
                preview_to_submit,
                summary.promotion_previewed_count,
            ),
        },
        TestnetPromotionDropoffBreakdown {
            stage: "submit_to_ack".to_string(),
            dropped_count: submit_to_ack,
            dropoff_rate_pct: calculate_testnet_promotion_rate(
                submit_to_ack,
                summary.promotion_submitted_count,
            ),
        },
        TestnetPromotionDropoffBreakdown {
            stage: "ack_to_fill".to_string(),
            dropped_count: ack_to_fill,
            dropoff_rate_pct: calculate_testnet_promotion_rate(ack_to_fill, summary.acked_count),
        },
    ]
}

fn build_testnet_promotion_outcome_breakdown(
    summary: &TestnetPromotionFunnelSummary,
) -> Vec<TestnetPromotionOutcomeBreakdown> {
    let denominator = summary.shadow_would_submit_count;
    vec![
        TestnetPromotionOutcomeBreakdown {
            outcome: "promotion_rejected".to_string(),
            count: summary.promotion_rejected_count,
            rate_pct: calculate_testnet_promotion_rate(
                summary.promotion_rejected_count,
                denominator,
            ),
        },
        TestnetPromotionOutcomeBreakdown {
            outcome: "promotion_expired".to_string(),
            count: summary.promotion_expired_count,
            rate_pct: calculate_testnet_promotion_rate(
                summary.promotion_expired_count,
                denominator,
            ),
        },
        TestnetPromotionOutcomeBreakdown {
            outcome: "filled".to_string(),
            count: summary.filled_count,
            rate_pct: calculate_testnet_promotion_rate(summary.filled_count, denominator),
        },
        TestnetPromotionOutcomeBreakdown {
            outcome: "partially_filled".to_string(),
            count: summary.partially_filled_count,
            rate_pct: calculate_testnet_promotion_rate(summary.partially_filled_count, denominator),
        },
        TestnetPromotionOutcomeBreakdown {
            outcome: "cancelled".to_string(),
            count: summary.cancelled_count,
            rate_pct: calculate_testnet_promotion_rate(summary.cancelled_count, denominator),
        },
        TestnetPromotionOutcomeBreakdown {
            outcome: "rejected".to_string(),
            count: summary.rejected_count,
            rate_pct: calculate_testnet_promotion_rate(summary.rejected_count, denominator),
        },
        TestnetPromotionOutcomeBreakdown {
            outcome: "expired".to_string(),
            count: summary.expired_count,
            rate_pct: calculate_testnet_promotion_rate(summary.expired_count, denominator),
        },
        TestnetPromotionOutcomeBreakdown {
            outcome: "reconciliation_required".to_string(),
            count: summary.reconciliation_required_count,
            rate_pct: calculate_testnet_promotion_rate(
                summary.reconciliation_required_count,
                denominator,
            ),
        },
        TestnetPromotionOutcomeBreakdown {
            outcome: "unknown_exchange_state".to_string(),
            count: summary.unknown_exchange_state_count,
            rate_pct: calculate_testnet_promotion_rate(
                summary.unknown_exchange_state_count,
                denominator,
            ),
        },
        TestnetPromotionOutcomeBreakdown {
            outcome: "failed".to_string(),
            count: summary.failed_count,
            rate_pct: calculate_testnet_promotion_rate(summary.failed_count, denominator),
        },
    ]
}

fn build_testnet_promotion_lifecycle_breakdown_from_summary(
    summary: &TestnetPromotionFunnelSummary,
) -> Vec<TestnetPromotionLifecycleBreakdown> {
    let denominator = summary.testnet_orders_created_count;
    vec![
        TestnetPromotionLifecycleBreakdown {
            execution_state: TestnetExecutionState::ExchangeAcked.as_str().to_string(),
            count: summary.acked_count,
            rate_pct: calculate_testnet_promotion_rate(summary.acked_count, denominator),
        },
        TestnetPromotionLifecycleBreakdown {
            execution_state: TestnetExecutionState::PartiallyFilled.as_str().to_string(),
            count: summary.partially_filled_count,
            rate_pct: calculate_testnet_promotion_rate(summary.partially_filled_count, denominator),
        },
        TestnetPromotionLifecycleBreakdown {
            execution_state: TestnetExecutionState::Filled.as_str().to_string(),
            count: summary.filled_count,
            rate_pct: calculate_testnet_promotion_rate(summary.filled_count, denominator),
        },
        TestnetPromotionLifecycleBreakdown {
            execution_state: TestnetExecutionState::Cancelled.as_str().to_string(),
            count: summary.cancelled_count,
            rate_pct: calculate_testnet_promotion_rate(summary.cancelled_count, denominator),
        },
        TestnetPromotionLifecycleBreakdown {
            execution_state: TestnetExecutionState::Rejected.as_str().to_string(),
            count: summary.rejected_count,
            rate_pct: calculate_testnet_promotion_rate(summary.rejected_count, denominator),
        },
        TestnetPromotionLifecycleBreakdown {
            execution_state: TestnetExecutionState::Expired.as_str().to_string(),
            count: summary.expired_count,
            rate_pct: calculate_testnet_promotion_rate(summary.expired_count, denominator),
        },
        TestnetPromotionLifecycleBreakdown {
            execution_state: TestnetExecutionState::ReconciliationRequired
                .as_str()
                .to_string(),
            count: summary.reconciliation_required_count,
            rate_pct: calculate_testnet_promotion_rate(
                summary.reconciliation_required_count,
                denominator,
            ),
        },
        TestnetPromotionLifecycleBreakdown {
            execution_state: TestnetExecutionState::UnknownExchangeState
                .as_str()
                .to_string(),
            count: summary.unknown_exchange_state_count,
            rate_pct: calculate_testnet_promotion_rate(
                summary.unknown_exchange_state_count,
                denominator,
            ),
        },
        TestnetPromotionLifecycleBreakdown {
            execution_state: TestnetExecutionState::Failed.as_str().to_string(),
            count: summary.failed_count,
            rate_pct: calculate_testnet_promotion_rate(summary.failed_count, denominator),
        },
    ]
}

fn build_testnet_promotion_quality_signals(
    summary: &TestnetPromotionFunnelSummary,
) -> Vec<TestnetPromotionQualitySignal> {
    vec![
        TestnetPromotionQualitySignal {
            signal: "preview_rate".to_string(),
            value_pct: summary.preview_rate_pct,
            numerator: summary.promotion_previewed_count,
            denominator: summary.shadow_would_submit_count,
        },
        TestnetPromotionQualitySignal {
            signal: "submit_rate".to_string(),
            value_pct: summary.submit_rate_pct,
            numerator: summary.promotion_submitted_count,
            denominator: summary.promotion_previewed_count,
        },
        TestnetPromotionQualitySignal {
            signal: "ack_rate".to_string(),
            value_pct: summary.ack_rate_pct,
            numerator: summary.acked_count,
            denominator: summary.testnet_orders_created_count,
        },
        TestnetPromotionQualitySignal {
            signal: "fill_rate".to_string(),
            value_pct: summary.fill_rate_pct,
            numerator: summary.filled_count,
            denominator: summary.acked_count,
        },
        TestnetPromotionQualitySignal {
            signal: "reconciliation_required_rate".to_string(),
            value_pct: summary.reconciliation_required_rate_pct,
            numerator: summary.reconciliation_required_count,
            denominator: summary.testnet_orders_created_count,
        },
    ]
}

fn summarize_testnet_promotion_funnel_rows(
    request: &TestnetPromotionFunnelRequest,
    rows: &[TestnetPromotionFunnelMaterializedRow],
) -> TestnetPromotionFunnelSummary {
    let computed_at = Utc::now();
    let mut summary = empty_testnet_promotion_funnel_summary(request, computed_at);
    let mut total_shadow_to_preview_seconds = Decimal::ZERO;
    let mut shadow_to_preview_samples = 0;
    let mut total_preview_to_submit_seconds = Decimal::ZERO;
    let mut preview_to_submit_samples = 0;

    for row in rows {
        summary.shadow_would_submit_count += 1;

        if row.promotion_id.is_some() {
            summary.promotion_previewed_count += 1;
        }

        if row_counts_as_submitted(row) {
            summary.promotion_submitted_count += 1;
        }

        match row.promotion_status.as_deref() {
            Some("REJECTED") => summary.promotion_rejected_count += 1,
            Some("EXPIRED") => summary.promotion_expired_count += 1,
            _ => {}
        }

        if row_counts_as_duplicate_rejected(row) {
            summary.promotion_duplicate_rejected_count += 1;
        }

        if row.client_order_id.is_some() || row.testnet_order_id.is_some() {
            summary.testnet_orders_created_count += 1;
        }

        if row.acked_at.is_some() {
            summary.acked_count += 1;
        }

        if let Some(state) = row.effective_execution_state {
            match state {
                TestnetExecutionState::Filled => summary.filled_count += 1,
                TestnetExecutionState::PartiallyFilled => summary.partially_filled_count += 1,
                TestnetExecutionState::Cancelled => summary.cancelled_count += 1,
                TestnetExecutionState::Rejected => summary.rejected_count += 1,
                TestnetExecutionState::Expired => summary.expired_count += 1,
                TestnetExecutionState::ReconciliationRequired => {
                    summary.reconciliation_required_count += 1
                }
                TestnetExecutionState::UnknownExchangeState => {
                    summary.unknown_exchange_state_count += 1
                }
                TestnetExecutionState::Failed => summary.failed_count += 1,
                _ => {}
            }
        }

        if let Some(promotion_created_at) = row.promotion_created_at {
            if let Some(seconds) =
                duration_seconds_decimal(row.shadow_created_at, promotion_created_at)
            {
                total_shadow_to_preview_seconds += seconds;
                shadow_to_preview_samples += 1;
            }
        }

        if let (Some(promotion_created_at), Some(submitted_at)) =
            (row.promotion_created_at, row.submitted_at)
        {
            if let Some(seconds) = duration_seconds_decimal(promotion_created_at, submitted_at) {
                total_preview_to_submit_seconds += seconds;
                preview_to_submit_samples += 1;
            }
        }
    }

    summary.preview_rate_pct = calculate_testnet_promotion_rate(
        summary.promotion_previewed_count,
        summary.shadow_would_submit_count,
    );
    summary.submit_rate_pct = calculate_testnet_promotion_rate(
        summary.promotion_submitted_count,
        summary.promotion_previewed_count,
    );
    summary.ack_rate_pct =
        calculate_testnet_promotion_rate(summary.acked_count, summary.testnet_orders_created_count);
    summary.fill_rate_pct =
        calculate_testnet_promotion_rate(summary.filled_count, summary.acked_count);
    summary.reconciliation_required_rate_pct = calculate_testnet_promotion_rate(
        summary.reconciliation_required_count,
        summary.testnet_orders_created_count,
    );
    summary.avg_time_shadow_to_preview_seconds = calculate_average_duration_seconds(
        total_shadow_to_preview_seconds,
        shadow_to_preview_samples,
    );
    summary.avg_time_preview_to_submit_seconds = calculate_average_duration_seconds(
        total_preview_to_submit_seconds,
        preview_to_submit_samples,
    );
    summary.stages = build_testnet_promotion_funnel_stage_breakdown(&summary);
    summary.outcome_breakdown = build_testnet_promotion_outcome_breakdown(&summary);
    summary.dropoff_breakdown = build_testnet_promotion_dropoff_breakdown(&summary);
    summary.lifecycle_breakdown =
        build_testnet_promotion_lifecycle_breakdown_from_summary(&summary);
    summary.quality_signals = build_testnet_promotion_quality_signals(&summary);
    summary
}

async fn query_testnet_promotion_funnel_materialized_rows(
    pool: &PgPool,
    request: &TestnetPromotionFunnelRequest,
    limit: Option<i64>,
) -> Result<Vec<TestnetPromotionFunnelMaterializedRow>> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
            sr.id AS shadow_run_id,
            sr.strategy_id,
            sr.symbol,
            sr.timeframe,
            sr.created_at AS shadow_created_at,
            sp.id AS promotion_id,
            sp.status AS promotion_status,
            COALESCE(sp.rejection_reasons, '[]'::jsonb) AS promotion_rejection_reasons,
            sp.testnet_order_id,
            COALESCE(sp.client_order_id, eo.client_order_id) AS client_order_id,
            sp.created_at AS promotion_created_at,
            sp.submitted_at,
            COALESCE(eo.execution_state, latest_lifecycle.next_state) AS effective_execution_state,
            (sp.client_order_id IS NOT NULL AND eo.id IS NULL) AS linked_order_missing,
            ack_lifecycle.acked_at,
            latest_lifecycle.created_at AS last_lifecycle_at
        FROM testnet_shadow_runs sr
        LEFT JOIN testnet_shadow_promotions sp
            ON sp.shadow_run_id = sr.id
        LEFT JOIN exchange_testnet_orders eo
            ON eo.id = sp.testnet_order_id
        LEFT JOIN LATERAL (
            SELECT
                le.next_state,
                le.created_at
            FROM exchange_testnet_order_lifecycle_events le
            WHERE (
                eo.id IS NOT NULL
                AND le.order_id = eo.id
            ) OR (
                sp.client_order_id IS NOT NULL
                AND le.client_order_id = sp.client_order_id
            )
            ORDER BY le.created_at DESC, le.id DESC
            LIMIT 1
        ) latest_lifecycle ON TRUE
        LEFT JOIN LATERAL (
            SELECT MIN(le.created_at) AS acked_at
            FROM exchange_testnet_order_lifecycle_events le
            WHERE (
                eo.id IS NOT NULL
                AND le.order_id = eo.id
            ) OR (
                sp.client_order_id IS NOT NULL
                AND le.client_order_id = sp.client_order_id
            )
              AND le.next_state = 'EXCHANGE_ACKED'
        ) ack_lifecycle ON TRUE
        WHERE sr.decision = 'WOULD_SUBMIT'
        "#,
    );

    if let Some(strategy_id) = request.strategy_id.as_deref() {
        builder.push(" AND sr.strategy_id = ");
        builder.push_bind(strategy_id);
    }
    if let Some(symbol) = request.symbol.as_deref() {
        builder.push(" AND sr.symbol = ");
        builder.push_bind(symbol);
    }
    if let Some(timeframe) = request.timeframe.as_deref() {
        builder.push(" AND sr.timeframe = ");
        builder.push_bind(timeframe);
    }
    if let Some(start_time) = request.start_time {
        builder.push(" AND sr.created_at >= ");
        builder.push_bind(start_time);
    }
    if let Some(end_time) = request.end_time {
        builder.push(" AND sr.created_at <= ");
        builder.push_bind(end_time);
    }

    builder.push(" ORDER BY sr.created_at DESC, sr.id DESC");
    if let Some(limit) = limit {
        builder.push(" LIMIT ");
        builder.push_bind(limit);
    }

    let rows = builder.build().fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .map(|row| TestnetPromotionFunnelMaterializedRow {
            shadow_run_id: row.get("shadow_run_id"),
            promotion_id: row.get("promotion_id"),
            strategy_id: row.get("strategy_id"),
            symbol: row.get("symbol"),
            timeframe: row.get("timeframe"),
            promotion_status: row.get("promotion_status"),
            promotion_rejection_reasons: row
                .get::<Value, _>("promotion_rejection_reasons")
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|value| value.as_str().map(ToOwned::to_owned))
                .collect(),
            testnet_order_id: row.get("testnet_order_id"),
            client_order_id: row.get("client_order_id"),
            effective_execution_state: row
                .get::<Option<String>, _>("effective_execution_state")
                .and_then(|value| value.parse::<TestnetExecutionState>().ok()),
            linked_order_missing: row.get("linked_order_missing"),
            shadow_created_at: row.get("shadow_created_at"),
            promotion_created_at: row.get("promotion_created_at"),
            submitted_at: row.get("submitted_at"),
            acked_at: row.get("acked_at"),
            last_lifecycle_at: row.get("last_lifecycle_at"),
        })
        .collect())
}

pub async fn get_testnet_promotion_funnel_summary(
    pool: &PgPool,
    request: &TestnetPromotionFunnelRequest,
) -> Result<TestnetPromotionFunnelSummary> {
    let rows = query_testnet_promotion_funnel_materialized_rows(pool, request, None).await?;
    Ok(summarize_testnet_promotion_funnel_rows(request, &rows))
}

pub async fn get_testnet_promotion_outcome_breakdown(
    pool: &PgPool,
    request: &TestnetPromotionFunnelRequest,
) -> Result<Vec<TestnetPromotionOutcomeBreakdown>> {
    let summary = get_testnet_promotion_funnel_summary(pool, request).await?;
    Ok(summary.outcome_breakdown)
}

pub async fn get_testnet_promotion_lifecycle_breakdown(
    pool: &PgPool,
    request: &TestnetPromotionFunnelRequest,
) -> Result<Vec<TestnetPromotionLifecycleBreakdown>> {
    let summary = get_testnet_promotion_funnel_summary(pool, request).await?;
    Ok(summary.lifecycle_breakdown)
}

pub async fn list_testnet_promotion_funnel_rows(
    pool: &PgPool,
    request: &TestnetPromotionFunnelRequest,
) -> Result<Vec<TestnetPromotionFunnelRow>> {
    let rows = query_testnet_promotion_funnel_materialized_rows(
        pool,
        request,
        Some(bounded_testnet_promotion_funnel_limit(request.limit)),
    )
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| TestnetPromotionFunnelRow {
            shadow_run_id: row.shadow_run_id,
            promotion_id: row.promotion_id,
            strategy_id: row.strategy_id,
            symbol: row.symbol,
            timeframe: row.timeframe,
            promotion_status: row.promotion_status,
            promotion_rejection_reasons: row.promotion_rejection_reasons,
            testnet_order_id: row.testnet_order_id,
            client_order_id: row.client_order_id,
            execution_state: row.effective_execution_state,
            linked_order_missing: row.linked_order_missing,
            shadow_created_at: row.shadow_created_at,
            promotion_created_at: row.promotion_created_at,
            submitted_at: row.submitted_at,
            acked_at: row.acked_at,
            last_lifecycle_at: row.last_lifecycle_at,
        })
        .collect())
}

fn empty_strategy_performance_summary(
    request: &StrategyPerformanceRequest,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    computed_at: DateTime<Utc>,
) -> StrategyPerformanceSummary {
    StrategyPerformanceSummary {
        strategy_id: request.strategy_id.clone(),
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        mode: request.mode,
        window_start,
        window_end,
        total_runs: 0,
        total_signals: 0,
        approved_risk_decisions: 0,
        rejected_risk_decisions: 0,
        risk_rejection_rate: Decimal::ZERO,
        shadow_would_submit_count: 0,
        shadow_no_signal_count: 0,
        shadow_risk_rejected_count: 0,
        paper_orders_count: 0,
        paper_positions_opened: 0,
        paper_positions_closed: 0,
        realized_pnl: Decimal::ZERO,
        unrealized_pnl: Decimal::ZERO,
        win_rate: None,
        avg_win: None,
        avg_loss: None,
        max_drawdown_pct: None,
        backtest_runs_count: 0,
        best_backtest_pnl_pct: None,
        worst_backtest_pnl_pct: None,
        avg_backtest_pnl_pct: None,
        created_at: computed_at,
        computed_at,
    }
}

fn empty_strategy_decision_breakdown(
    request: &StrategyPerformanceRequest,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    computed_at: DateTime<Utc>,
) -> StrategyDecisionBreakdown {
    StrategyDecisionBreakdown {
        strategy_id: request.strategy_id.clone().unwrap_or_default(),
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        window_start,
        window_end,
        total_runs: 0,
        would_submit_count: 0,
        no_signal_count: 0,
        risk_rejected_count: 0,
        skipped_count: 0,
        error_count: 0,
        computed_at,
    }
}

fn empty_strategy_pnl_breakdown(
    request: &StrategyPerformanceRequest,
    mode: StrategyPerformanceMode,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    computed_at: DateTime<Utc>,
) -> StrategyPnlBreakdown {
    StrategyPnlBreakdown {
        strategy_id: request.strategy_id.clone(),
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        mode,
        window_start,
        window_end,
        positions_opened: 0,
        positions_closed: 0,
        realized_pnl: Decimal::ZERO,
        unrealized_pnl: Decimal::ZERO,
        win_rate: None,
        avg_win: None,
        avg_loss: None,
        max_drawdown_pct: None,
        computed_at,
    }
}

fn build_strategy_risk_breakdown(
    request: &StrategyPerformanceRequest,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    computed_at: DateTime<Utc>,
    approved_decisions: i64,
    rejected_decisions: i64,
) -> StrategyRiskBreakdown {
    StrategyRiskBreakdown {
        strategy_id: request.strategy_id.clone(),
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        window_start,
        window_end,
        approved_decisions,
        rejected_decisions,
        rejection_rate: calculate_strategy_rejection_rate(
            rejected_decisions,
            approved_decisions + rejected_decisions,
        ),
        computed_at,
    }
}

async fn fetch_signals_and_risk_breakdown(
    pool: &PgPool,
    request: &StrategyPerformanceRequest,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    computed_at: DateTime<Utc>,
) -> Result<(i64, StrategyRiskBreakdown)> {
    let signal_row = sqlx::query(
        r#"
        SELECT COUNT(*)::BIGINT AS count
        FROM signals
        WHERE created_at >= $1
          AND created_at <= $2
          AND ($3::TEXT IS NULL OR strategy_id = $3)
          AND ($4::TEXT IS NULL OR symbol = $4)
          AND ($5::TEXT IS NULL OR timeframe = $5)
        "#,
    )
    .bind(window_start)
    .bind(window_end)
    .bind(request.strategy_id.as_deref())
    .bind(request.symbol.as_deref())
    .bind(request.timeframe.as_deref())
    .fetch_one(pool)
    .await?;
    let total_signals = signal_row.get::<i64, _>("count");

    let risk_row = sqlx::query(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE rd.decision = 'APPROVED')::BIGINT AS approved_count,
            COUNT(*) FILTER (WHERE rd.decision = 'REJECTED')::BIGINT AS rejected_count
        FROM risk_decisions rd
        LEFT JOIN signals s ON s.id = rd.signal_id
        WHERE rd.decided_at >= $1
          AND rd.decided_at <= $2
          AND (
                $3::TEXT IS NULL
                OR COALESCE(s.strategy_id, rd.rationale::jsonb ->> 'strategy_id') = $3
            )
          AND (
                $4::TEXT IS NULL
                OR COALESCE(s.symbol, rd.rationale::jsonb ->> 'symbol') = $4
            )
          AND ($5::TEXT IS NULL OR s.timeframe = $5)
        "#,
    )
    .bind(window_start)
    .bind(window_end)
    .bind(request.strategy_id.as_deref())
    .bind(request.symbol.as_deref())
    .bind(request.timeframe.as_deref())
    .fetch_one(pool)
    .await?;

    let approved_decisions = risk_row.get::<i64, _>("approved_count");
    let rejected_decisions = risk_row.get::<i64, _>("rejected_count");
    Ok((
        total_signals,
        build_strategy_risk_breakdown(
            request,
            window_start,
            window_end,
            computed_at,
            approved_decisions,
            rejected_decisions,
        ),
    ))
}

pub async fn get_strategy_shadow_decision_breakdown(
    pool: &PgPool,
    request: &StrategyPerformanceRequest,
) -> Result<StrategyDecisionBreakdown> {
    let (window_start, window_end, computed_at) = analytics_window(request);
    let Some(strategy_id) = request.strategy_id.clone() else {
        return Ok(empty_strategy_decision_breakdown(
            request,
            window_start,
            window_end,
            computed_at,
        ));
    };

    let row = sqlx::query(
        r#"
        SELECT
            COUNT(*)::BIGINT AS total_runs,
            COUNT(*) FILTER (WHERE decision = 'WOULD_SUBMIT')::BIGINT AS would_submit_count,
            COUNT(*) FILTER (WHERE decision = 'NO_SIGNAL')::BIGINT AS no_signal_count,
            COUNT(*) FILTER (WHERE decision = 'RISK_REJECTED')::BIGINT AS risk_rejected_count,
            COUNT(*) FILTER (
                WHERE decision LIKE 'SKIPPED_%'
            )::BIGINT AS skipped_count,
            COUNT(*) FILTER (WHERE decision = 'ERROR')::BIGINT AS error_count
        FROM testnet_shadow_runs
        WHERE created_at >= $1
          AND created_at <= $2
          AND strategy_id = $3
          AND ($4::TEXT IS NULL OR symbol = $4)
          AND ($5::TEXT IS NULL OR timeframe = $5)
        "#,
    )
    .bind(window_start)
    .bind(window_end)
    .bind(strategy_id.clone())
    .bind(request.symbol.as_deref())
    .bind(request.timeframe.as_deref())
    .fetch_one(pool)
    .await?;

    Ok(StrategyDecisionBreakdown {
        strategy_id,
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        window_start,
        window_end,
        total_runs: row.get("total_runs"),
        would_submit_count: row.get("would_submit_count"),
        no_signal_count: row.get("no_signal_count"),
        risk_rejected_count: row.get("risk_rejected_count"),
        skipped_count: row.get("skipped_count"),
        error_count: row.get("error_count"),
        computed_at,
    })
}

pub async fn get_strategy_paper_pnl_breakdown(
    pool: &PgPool,
    request: &StrategyPerformanceRequest,
) -> Result<StrategyPnlBreakdown> {
    let (window_start, window_end, computed_at) = analytics_window(request);
    let Some(account) = get_default_paper_account(pool).await? else {
        return Ok(empty_strategy_pnl_breakdown(
            request,
            StrategyPerformanceMode::Paper,
            window_start,
            window_end,
            computed_at,
        ));
    };

    let row = sqlx::query(
        r#"
        SELECT
            COUNT(*)::BIGINT AS positions_opened,
            COUNT(*) FILTER (WHERE pp.status = 'closed')::BIGINT AS positions_closed,
            COALESCE(SUM(pp.realized_pnl), 0) AS realized_pnl,
            COALESCE(SUM(pp.unrealized_pnl), 0) AS unrealized_pnl,
            COUNT(*) FILTER (
                WHERE pp.status = 'closed' AND pp.realized_pnl > 0
            )::BIGINT AS wins,
            AVG(pp.realized_pnl) FILTER (
                WHERE pp.status = 'closed' AND pp.realized_pnl > 0
            ) AS avg_win,
            AVG(pp.realized_pnl) FILTER (
                WHERE pp.status = 'closed' AND pp.realized_pnl < 0
            ) AS avg_loss
        FROM paper_positions pp
        LEFT JOIN signals s ON s.id = pp.signal_id
        WHERE pp.account_id = $1
          AND pp.opened_at >= $2
          AND pp.opened_at <= $3
          AND ($4::TEXT IS NULL OR pp.strategy_id = $4)
          AND ($5::TEXT IS NULL OR pp.symbol = $5)
          AND ($6::TEXT IS NULL OR s.timeframe = $6)
        "#,
    )
    .bind(account.id)
    .bind(window_start)
    .bind(window_end)
    .bind(request.strategy_id.as_deref())
    .bind(request.symbol.as_deref())
    .bind(request.timeframe.as_deref())
    .fetch_one(pool)
    .await?;

    let positions_closed = row.get::<i64, _>("positions_closed");
    let max_drawdown_pct =
        if request.strategy_id.is_none() && request.symbol.is_none() && request.timeframe.is_none()
        {
            let drawdown_row = sqlx::query(
                r#"
            SELECT MAX(drawdown_pct) AS max_drawdown_pct
            FROM paper_equity_snapshots
            WHERE account_id = $1
              AND snapshot_at >= $2
              AND snapshot_at <= $3
            "#,
            )
            .bind(account.id)
            .bind(window_start)
            .bind(window_end)
            .fetch_one(pool)
            .await?;
            drawdown_row.get::<Option<Decimal>, _>("max_drawdown_pct")
        } else {
            None
        };

    Ok(StrategyPnlBreakdown {
        strategy_id: request.strategy_id.clone(),
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        mode: StrategyPerformanceMode::Paper,
        window_start,
        window_end,
        positions_opened: row.get("positions_opened"),
        positions_closed,
        realized_pnl: row.get("realized_pnl"),
        unrealized_pnl: row.get("unrealized_pnl"),
        win_rate: calculate_strategy_win_rate(row.get("wins"), positions_closed),
        avg_win: row.get("avg_win"),
        avg_loss: row.get("avg_loss"),
        max_drawdown_pct,
        computed_at,
    })
}

pub async fn get_strategy_backtest_breakdown(
    pool: &PgPool,
    request: &StrategyPerformanceRequest,
) -> Result<StrategyPnlBreakdown> {
    let (window_start, window_end, computed_at) = analytics_window(request);
    let row = sqlx::query(
        r#"
        SELECT
            COUNT(*)::BIGINT AS run_count,
            COALESCE(SUM(pnl), 0) AS realized_pnl,
            COUNT(*) FILTER (WHERE pnl > 0)::BIGINT AS wins,
            AVG(avg_win) FILTER (WHERE status = 'COMPLETED') AS avg_win,
            AVG(avg_loss) FILTER (WHERE status = 'COMPLETED') AS avg_loss,
            AVG(max_drawdown_pct) FILTER (WHERE status = 'COMPLETED') AS max_drawdown_pct
        FROM backtest_runs
        WHERE created_at >= $1
          AND created_at <= $2
          AND ($3::TEXT IS NULL OR strategy_id = $3)
          AND ($4::TEXT IS NULL OR symbol = $4)
          AND ($5::TEXT IS NULL OR timeframe = $5)
        "#,
    )
    .bind(window_start)
    .bind(window_end)
    .bind(request.strategy_id.as_deref())
    .bind(request.symbol.as_deref())
    .bind(request.timeframe.as_deref())
    .fetch_one(pool)
    .await?;

    let run_count = row.get::<i64, _>("run_count");
    Ok(StrategyPnlBreakdown {
        strategy_id: request.strategy_id.clone(),
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        mode: StrategyPerformanceMode::Backtest,
        window_start,
        window_end,
        positions_opened: run_count,
        positions_closed: run_count,
        realized_pnl: row.get("realized_pnl"),
        unrealized_pnl: Decimal::ZERO,
        win_rate: calculate_strategy_win_rate(row.get("wins"), run_count),
        avg_win: row.get("avg_win"),
        avg_loss: row.get("avg_loss"),
        max_drawdown_pct: row.get("max_drawdown_pct"),
        computed_at,
    })
}

async fn get_shadow_mode_summary(
    pool: &PgPool,
    request: &StrategyPerformanceRequest,
) -> Result<StrategyPerformanceSummary> {
    let (window_start, window_end, computed_at) = analytics_window(request);
    let mut summary =
        empty_strategy_performance_summary(request, window_start, window_end, computed_at);
    let row = sqlx::query(
        r#"
        SELECT
            COUNT(*)::BIGINT AS total_runs,
            COUNT(*) FILTER (WHERE decision = 'WOULD_SUBMIT')::BIGINT AS would_submit_count,
            COUNT(*) FILTER (WHERE decision = 'NO_SIGNAL')::BIGINT AS no_signal_count,
            COUNT(*) FILTER (WHERE decision = 'RISK_REJECTED')::BIGINT AS risk_rejected_count,
            COUNT(*) FILTER (WHERE risk_decision_id IS NOT NULL AND decision != 'RISK_REJECTED')::BIGINT AS approved_risk_decisions,
            COUNT(*) FILTER (WHERE decision = 'RISK_REJECTED')::BIGINT AS rejected_risk_decisions,
            COUNT(*) FILTER (WHERE signal_id IS NOT NULL)::BIGINT AS total_signals
        FROM testnet_shadow_runs
        WHERE created_at >= $1
          AND created_at <= $2
          AND ($3::TEXT IS NULL OR strategy_id = $3)
          AND ($4::TEXT IS NULL OR symbol = $4)
          AND ($5::TEXT IS NULL OR timeframe = $5)
        "#,
    )
    .bind(window_start)
    .bind(window_end)
    .bind(request.strategy_id.as_deref())
    .bind(request.symbol.as_deref())
    .bind(request.timeframe.as_deref())
    .fetch_one(pool)
    .await?;

    summary.total_runs = row.get("total_runs");
    summary.total_signals = row.get("total_signals");
    summary.approved_risk_decisions = row.get("approved_risk_decisions");
    summary.rejected_risk_decisions = row.get("rejected_risk_decisions");
    summary.risk_rejection_rate = calculate_strategy_rejection_rate(
        summary.rejected_risk_decisions,
        summary.approved_risk_decisions + summary.rejected_risk_decisions,
    );
    summary.shadow_would_submit_count = row.get("would_submit_count");
    summary.shadow_no_signal_count = row.get("no_signal_count");
    summary.shadow_risk_rejected_count = row.get("risk_rejected_count");
    Ok(summary)
}

async fn get_paper_mode_summary(
    pool: &PgPool,
    request: &StrategyPerformanceRequest,
) -> Result<StrategyPerformanceSummary> {
    let (window_start, window_end, computed_at) = analytics_window(request);
    let mut summary =
        empty_strategy_performance_summary(request, window_start, window_end, computed_at);
    let (total_signals, risk_breakdown) =
        fetch_signals_and_risk_breakdown(pool, request, window_start, window_end, computed_at)
            .await?;
    summary.total_signals = total_signals;
    summary.approved_risk_decisions = risk_breakdown.approved_decisions;
    summary.rejected_risk_decisions = risk_breakdown.rejected_decisions;
    summary.risk_rejection_rate = risk_breakdown.rejection_rate;

    let order_row = sqlx::query(
        r#"
        SELECT COUNT(*)::BIGINT AS paper_orders_count
        FROM orders o
        LEFT JOIN risk_decisions rd ON rd.id = o.risk_decision_id
        LEFT JOIN signals s ON s.id = rd.signal_id
        WHERE o.market_mode = 'paper'
          AND o.created_at >= $1
          AND o.created_at <= $2
          AND (
                $3::TEXT IS NULL
                OR COALESCE(s.strategy_id, rd.rationale::jsonb ->> 'strategy_id') = $3
            )
          AND ($4::TEXT IS NULL OR o.symbol = $4)
          AND ($5::TEXT IS NULL OR s.timeframe = $5)
        "#,
    )
    .bind(window_start)
    .bind(window_end)
    .bind(request.strategy_id.as_deref())
    .bind(request.symbol.as_deref())
    .bind(request.timeframe.as_deref())
    .fetch_one(pool)
    .await?;
    summary.paper_orders_count = order_row.get("paper_orders_count");

    let pnl = get_strategy_paper_pnl_breakdown(pool, request).await?;
    summary.total_runs = pnl.positions_opened;
    summary.paper_positions_opened = pnl.positions_opened;
    summary.paper_positions_closed = pnl.positions_closed;
    summary.realized_pnl = pnl.realized_pnl;
    summary.unrealized_pnl = pnl.unrealized_pnl;
    summary.win_rate = pnl.win_rate;
    summary.avg_win = pnl.avg_win;
    summary.avg_loss = pnl.avg_loss;
    summary.max_drawdown_pct = pnl.max_drawdown_pct;
    Ok(summary)
}

async fn get_backtest_mode_summary(
    pool: &PgPool,
    request: &StrategyPerformanceRequest,
) -> Result<StrategyPerformanceSummary> {
    let (window_start, window_end, computed_at) = analytics_window(request);
    let mut summary =
        empty_strategy_performance_summary(request, window_start, window_end, computed_at);
    let row = sqlx::query(
        r#"
        SELECT
            COUNT(*)::BIGINT AS run_count,
            COALESCE(SUM(pnl), 0) AS realized_pnl,
            MAX(pnl_pct) FILTER (WHERE status = 'COMPLETED') AS best_backtest_pnl_pct,
            MIN(pnl_pct) FILTER (WHERE status = 'COMPLETED') AS worst_backtest_pnl_pct,
            AVG(pnl_pct) FILTER (WHERE status = 'COMPLETED') AS avg_backtest_pnl_pct,
            AVG(win_rate) FILTER (WHERE status = 'COMPLETED') AS win_rate,
            AVG(avg_win) FILTER (WHERE status = 'COMPLETED') AS avg_win,
            AVG(avg_loss) FILTER (WHERE status = 'COMPLETED') AS avg_loss,
            MAX(max_drawdown_pct) FILTER (WHERE status = 'COMPLETED') AS max_drawdown_pct,
            MIN(created_at) AS created_at
        FROM backtest_runs
        WHERE created_at >= $1
          AND created_at <= $2
          AND ($3::TEXT IS NULL OR strategy_id = $3)
          AND ($4::TEXT IS NULL OR symbol = $4)
          AND ($5::TEXT IS NULL OR timeframe = $5)
        "#,
    )
    .bind(window_start)
    .bind(window_end)
    .bind(request.strategy_id.as_deref())
    .bind(request.symbol.as_deref())
    .bind(request.timeframe.as_deref())
    .fetch_one(pool)
    .await?;

    summary.total_runs = row.get("run_count");
    summary.backtest_runs_count = row.get("run_count");
    summary.realized_pnl = row.get("realized_pnl");
    summary.win_rate = row.get("win_rate");
    summary.avg_win = row.get("avg_win");
    summary.avg_loss = row.get("avg_loss");
    summary.max_drawdown_pct = row.get("max_drawdown_pct");
    summary.best_backtest_pnl_pct = row.get("best_backtest_pnl_pct");
    summary.worst_backtest_pnl_pct = row.get("worst_backtest_pnl_pct");
    summary.avg_backtest_pnl_pct = row.get("avg_backtest_pnl_pct");
    summary.created_at = row
        .get::<Option<DateTime<Utc>>, _>("created_at")
        .unwrap_or(computed_at);
    Ok(summary)
}

pub async fn get_strategy_performance_summary(
    pool: &PgPool,
    request: &StrategyPerformanceRequest,
) -> Result<StrategyPerformanceSummary> {
    match request.mode {
        StrategyPerformanceMode::Backtest => get_backtest_mode_summary(pool, request).await,
        StrategyPerformanceMode::Paper => get_paper_mode_summary(pool, request).await,
        StrategyPerformanceMode::Shadow => get_shadow_mode_summary(pool, request).await,
        StrategyPerformanceMode::Combined => {
            let backtest_request = StrategyPerformanceRequest {
                mode: StrategyPerformanceMode::Backtest,
                ..request.clone()
            };
            let paper_request = StrategyPerformanceRequest {
                mode: StrategyPerformanceMode::Paper,
                ..request.clone()
            };
            let shadow_request = StrategyPerformanceRequest {
                mode: StrategyPerformanceMode::Shadow,
                ..request.clone()
            };
            let combined = combine_strategy_performance_summaries(vec![
                get_backtest_mode_summary(pool, &backtest_request).await?,
                get_paper_mode_summary(pool, &paper_request).await?,
                get_shadow_mode_summary(pool, &shadow_request).await?,
            ]);
            Ok(combined.unwrap_or_else(|| {
                let (window_start, window_end, computed_at) = analytics_window(request);
                empty_strategy_performance_summary(request, window_start, window_end, computed_at)
            }))
        }
    }
}

pub async fn list_strategy_performance_rankings(
    pool: &PgPool,
    request: &StrategyPerformanceRequest,
) -> Result<Vec<StrategyComparisonSummary>> {
    let (window_start, window_end, computed_at) = analytics_window(request);
    let limit = bounded_analytics_limit(request.limit);
    let strategy_rows = match request.mode {
        StrategyPerformanceMode::Backtest => {
            sqlx::query(
                r#"
                SELECT strategy_id
                FROM backtest_runs
                WHERE created_at >= $1
                  AND created_at <= $2
                  AND ($3::TEXT IS NULL OR symbol = $3)
                  AND ($4::TEXT IS NULL OR timeframe = $4)
                GROUP BY strategy_id
                ORDER BY AVG(pnl_pct) DESC NULLS LAST, strategy_id ASC
                LIMIT $5
                "#,
            )
            .bind(window_start)
            .bind(window_end)
            .bind(request.symbol.as_deref())
            .bind(request.timeframe.as_deref())
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
        StrategyPerformanceMode::Paper => {
            let Some(account) = get_default_paper_account(pool).await? else {
                return Ok(Vec::new());
            };
            sqlx::query(
                r#"
                SELECT pp.strategy_id
                FROM paper_positions pp
                LEFT JOIN signals s ON s.id = pp.signal_id
                WHERE pp.account_id = $1
                  AND pp.strategy_id IS NOT NULL
                  AND pp.opened_at >= $2
                  AND pp.opened_at <= $3
                  AND ($4::TEXT IS NULL OR pp.symbol = $4)
                  AND ($5::TEXT IS NULL OR s.timeframe = $5)
                GROUP BY pp.strategy_id
                ORDER BY SUM(pp.realized_pnl) DESC NULLS LAST, pp.strategy_id ASC
                LIMIT $6
                "#,
            )
            .bind(account.id)
            .bind(window_start)
            .bind(window_end)
            .bind(request.symbol.as_deref())
            .bind(request.timeframe.as_deref())
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
        StrategyPerformanceMode::Shadow => {
            sqlx::query(
                r#"
                SELECT strategy_id
                FROM testnet_shadow_runs
                WHERE created_at >= $1
                  AND created_at <= $2
                  AND ($3::TEXT IS NULL OR symbol = $3)
                  AND ($4::TEXT IS NULL OR timeframe = $4)
                GROUP BY strategy_id
                ORDER BY COUNT(*) FILTER (WHERE decision = 'WOULD_SUBMIT') DESC, strategy_id ASC
                LIMIT $5
                "#,
            )
            .bind(window_start)
            .bind(window_end)
            .bind(request.symbol.as_deref())
            .bind(request.timeframe.as_deref())
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
        StrategyPerformanceMode::Combined => {
            sqlx::query(
                r#"
                SELECT strategy_id
                FROM (
                    SELECT strategy_id
                    FROM backtest_runs
                    WHERE created_at >= $1
                      AND created_at <= $2
                      AND ($3::TEXT IS NULL OR symbol = $3)
                      AND ($4::TEXT IS NULL OR timeframe = $4)
                    UNION
                    SELECT strategy_id
                    FROM paper_positions
                    WHERE strategy_id IS NOT NULL
                      AND opened_at >= $1
                      AND opened_at <= $2
                      AND ($3::TEXT IS NULL OR symbol = $3)
                    UNION
                    SELECT strategy_id
                    FROM testnet_shadow_runs
                    WHERE created_at >= $1
                      AND created_at <= $2
                      AND ($3::TEXT IS NULL OR symbol = $3)
                      AND ($4::TEXT IS NULL OR timeframe = $4)
                ) strategies
                ORDER BY strategy_id ASC
                LIMIT $5
                "#,
            )
            .bind(window_start)
            .bind(window_end)
            .bind(request.symbol.as_deref())
            .bind(request.timeframe.as_deref())
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
    };

    let mut rankings = Vec::new();
    for row in strategy_rows {
        let strategy_id = row.get::<String, _>("strategy_id");
        let summary = get_strategy_performance_summary(
            pool,
            &StrategyPerformanceRequest {
                strategy_id: Some(strategy_id.clone()),
                ..request.clone()
            },
        )
        .await?;
        rankings.push(StrategyComparisonSummary {
            strategy_id,
            symbol: request.symbol.clone(),
            timeframe: request.timeframe.clone(),
            mode: request.mode,
            realized_pnl: summary.realized_pnl,
            unrealized_pnl: summary.unrealized_pnl,
            risk_rejection_rate: summary.risk_rejection_rate,
            win_rate: summary.win_rate,
            best_backtest_pnl_pct: summary.best_backtest_pnl_pct,
            worst_backtest_pnl_pct: summary.worst_backtest_pnl_pct,
            avg_backtest_pnl_pct: summary.avg_backtest_pnl_pct,
            shadow_would_submit_count: summary.shadow_would_submit_count,
            shadow_no_signal_count: summary.shadow_no_signal_count,
            shadow_risk_rejected_count: summary.shadow_risk_rejected_count,
            approved_risk_decisions: summary.approved_risk_decisions,
            rejected_risk_decisions: summary.rejected_risk_decisions,
            paper_orders_count: summary.paper_orders_count,
            total_signals: summary.total_signals,
            total_runs: summary.total_runs,
            computed_at,
        });
    }

    rankings.sort_by(|left, right| match request.mode {
        StrategyPerformanceMode::Backtest => right
            .avg_backtest_pnl_pct
            .unwrap_or(Decimal::ZERO)
            .cmp(&left.avg_backtest_pnl_pct.unwrap_or(Decimal::ZERO))
            .then_with(|| left.strategy_id.cmp(&right.strategy_id)),
        StrategyPerformanceMode::Paper => right
            .realized_pnl
            .cmp(&left.realized_pnl)
            .then_with(|| left.strategy_id.cmp(&right.strategy_id)),
        StrategyPerformanceMode::Shadow => right
            .shadow_would_submit_count
            .cmp(&left.shadow_would_submit_count)
            .then_with(|| {
                right
                    .rejected_risk_decisions
                    .cmp(&left.rejected_risk_decisions)
            })
            .then_with(|| left.strategy_id.cmp(&right.strategy_id)),
        StrategyPerformanceMode::Combined => right
            .realized_pnl
            .cmp(&left.realized_pnl)
            .then_with(|| {
                right
                    .shadow_would_submit_count
                    .cmp(&left.shadow_would_submit_count)
            })
            .then_with(|| left.strategy_id.cmp(&right.strategy_id)),
    });
    rankings.truncate(limit as usize);
    Ok(rankings)
}

pub async fn upsert_strategy_config(
    pool: &PgPool,
    config: &StrategyConfig,
) -> Result<StrategyConfigRecord> {
    let mut tx = pool.begin().await?;
    let current_version = get_strategy_config_tx(&mut tx, config.strategy_id)
        .await?
        .map(|record| record.current_version)
        .unwrap_or(1);
    let record = upsert_strategy_config_tx(&mut tx, config, current_version).await?;
    tx.commit().await?;
    Ok(record)
}

async fn upsert_strategy_config_tx(
    tx: &mut Transaction<'_, Postgres>,
    config: &StrategyConfig,
    current_version: i32,
) -> Result<StrategyConfigRecord> {
    let status = if config.enabled {
        StrategyStatus::Enabled
    } else {
        StrategyStatus::Disabled
    };
    let symbols = config
        .symbols
        .iter()
        .map(|symbol| symbol.as_str().to_string())
        .collect::<Vec<_>>()
        .join(",");

    let row = sqlx::query(
        r#"
        INSERT INTO strategy_configs (
            strategy_id,
            status,
            enabled,
            mode,
            symbols,
            timeframe,
            suggested_notional,
            momentum_lookback_candles,
            breakout_lookback_candles,
            max_signal_age_ms,
            cooldown_seconds,
            lookback_candles,
            trend_lookback_candles,
            strategy_momentum_lookback_candles,
            strategy_breakout_lookback_candles,
            confidence_floor,
            stop_loss_pct,
            take_profit_pct,
            holding_candles,
            notes,
            current_version,
            created_at,
            updated_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, NOW(), NOW()
        )
        ON CONFLICT (strategy_id) DO UPDATE
        SET
            status = EXCLUDED.status,
            enabled = EXCLUDED.enabled,
            mode = EXCLUDED.mode,
            symbols = EXCLUDED.symbols,
            timeframe = EXCLUDED.timeframe,
            suggested_notional = EXCLUDED.suggested_notional,
            momentum_lookback_candles = EXCLUDED.momentum_lookback_candles,
            breakout_lookback_candles = EXCLUDED.breakout_lookback_candles,
            max_signal_age_ms = EXCLUDED.max_signal_age_ms,
            cooldown_seconds = EXCLUDED.cooldown_seconds,
            lookback_candles = EXCLUDED.lookback_candles,
            trend_lookback_candles = EXCLUDED.trend_lookback_candles,
            strategy_momentum_lookback_candles = EXCLUDED.strategy_momentum_lookback_candles,
            strategy_breakout_lookback_candles = EXCLUDED.strategy_breakout_lookback_candles,
            confidence_floor = EXCLUDED.confidence_floor,
            stop_loss_pct = EXCLUDED.stop_loss_pct,
            take_profit_pct = EXCLUDED.take_profit_pct,
            holding_candles = EXCLUDED.holding_candles,
            notes = EXCLUDED.notes,
            current_version = EXCLUDED.current_version,
            updated_at = NOW()
        RETURNING
            strategy_id,
            enabled,
            mode,
            symbols,
            timeframe,
            suggested_notional,
            max_signal_age_ms,
            cooldown_seconds,
            lookback_candles,
            trend_lookback_candles,
            strategy_momentum_lookback_candles,
            strategy_breakout_lookback_candles,
            confidence_floor,
            stop_loss_pct,
            take_profit_pct,
            holding_candles,
            notes,
            current_version,
            created_at,
            updated_at
        "#,
    )
    .bind(config.strategy_id.as_str())
    .bind(status.as_str())
    .bind(config.enabled)
    .bind(config.mode.as_str())
    .bind(symbols)
    .bind(config.timeframe.as_str())
    .bind(config.suggested_notional)
    .bind(config.momentum_lookback_candles.unwrap_or(config.lookback_candles) as i32)
    .bind(config.breakout_lookback_candles.unwrap_or(config.lookback_candles) as i32)
    .bind(config.max_signal_age_ms)
    .bind(config.cooldown_seconds as i32)
    .bind(config.lookback_candles as i32)
    .bind(config.trend_lookback_candles.map(|value| value as i32))
    .bind(config.momentum_lookback_candles.map(|value| value as i32))
    .bind(config.breakout_lookback_candles.map(|value| value as i32))
    .bind(config.confidence_floor)
    .bind(config.stop_loss_pct)
    .bind(config.take_profit_pct)
    .bind(config.holding_candles.map(|value| value as i32))
    .bind(config.notes.clone())
    .bind(current_version)
    .fetch_one(&mut **tx)
    .await?;

    Ok(map_strategy_config(&row))
}

async fn get_strategy_config_tx(
    tx: &mut Transaction<'_, Postgres>,
    strategy_id: StrategyId,
) -> Result<Option<StrategyConfigRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            strategy_id,
            enabled,
            mode,
            symbols,
            timeframe,
            suggested_notional,
            max_signal_age_ms,
            cooldown_seconds,
            lookback_candles,
            trend_lookback_candles,
            strategy_momentum_lookback_candles,
            strategy_breakout_lookback_candles,
            confidence_floor,
            stop_loss_pct,
            take_profit_pct,
            holding_candles,
            notes,
            current_version,
            created_at,
            updated_at
        FROM strategy_configs
        WHERE strategy_id = $1
        "#,
    )
    .bind(strategy_id.as_str())
    .fetch_optional(&mut **tx)
    .await?;

    Ok(row.as_ref().map(map_strategy_config))
}

pub async fn get_strategy_config(
    pool: &PgPool,
    strategy_id: StrategyId,
) -> Result<Option<StrategyConfigRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            strategy_id,
            enabled,
            mode,
            symbols,
            timeframe,
            suggested_notional,
            max_signal_age_ms,
            cooldown_seconds,
            lookback_candles,
            trend_lookback_candles,
            strategy_momentum_lookback_candles,
            strategy_breakout_lookback_candles,
            confidence_floor,
            stop_loss_pct,
            take_profit_pct,
            holding_candles,
            notes,
            current_version,
            created_at,
            updated_at
        FROM strategy_configs
        WHERE strategy_id = $1
        "#,
    )
    .bind(strategy_id.as_str())
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_strategy_config))
}

pub async fn update_strategy_state(
    pool: &PgPool,
    strategy_id: StrategyId,
    last_evaluated_at: DateTime<Utc>,
    last_evaluation_reason: SignalReason,
    last_signal_id: Option<Uuid>,
    last_signal_at: Option<DateTime<Utc>>,
) -> Result<StrategyStateRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO strategy_state (
            strategy_id,
            last_evaluated_at,
            last_evaluation_reason,
            last_signal_id,
            last_signal_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (strategy_id) DO UPDATE
        SET
            last_evaluated_at = EXCLUDED.last_evaluated_at,
            last_evaluation_reason = EXCLUDED.last_evaluation_reason,
            last_signal_id = EXCLUDED.last_signal_id,
            last_signal_at = EXCLUDED.last_signal_at,
            updated_at = NOW()
        RETURNING
            strategy_id,
            last_evaluated_at,
            last_evaluation_reason,
            last_signal_id,
            last_signal_at,
            updated_at
        "#,
    )
    .bind(strategy_id.as_str())
    .bind(last_evaluated_at)
    .bind(last_evaluation_reason.as_str())
    .bind(last_signal_id)
    .bind(last_signal_at)
    .fetch_one(pool)
    .await?;

    Ok(map_strategy_state(&row))
}

pub async fn insert_signal_deduped(
    pool: &PgPool,
    signal: &StrategySignal,
) -> Result<InsertSignalOutcome> {
    let inserted_row = sqlx::query(
        r#"
        INSERT INTO signals (
            id,
            correlation_id,
            symbol,
            side,
            confidence,
            strategy_id,
            timeframe,
            reason,
            suggested_notional,
            stop_loss_pct,
            take_profit_pct,
            source_candle_open_time,
            generated_at,
            created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW())
        ON CONFLICT (
            strategy_id,
            symbol,
            timeframe,
            source_candle_open_time,
            side,
            reason
        ) DO NOTHING
        RETURNING
            id,
            strategy_id,
            symbol,
            side,
            confidence,
            timeframe,
            reason,
            suggested_notional,
            stop_loss_pct,
            take_profit_pct,
            source_candle_open_time,
            correlation_id,
            created_at
        "#,
    )
    .bind(signal.signal_id)
    .bind(signal.correlation_id)
    .bind(signal.symbol.as_str())
    .bind(signal.side.as_str())
    .bind(signal.confidence.value)
    .bind(signal.strategy_id.as_str())
    .bind(signal.timeframe.as_str())
    .bind(signal.reason.as_str())
    .bind(signal.suggested_notional)
    .bind(signal.stop_loss_pct)
    .bind(signal.take_profit_pct)
    .bind(signal.source_candle_open_time)
    .bind(signal.created_at)
    .fetch_optional(pool)
    .await?;

    if let Some(row) = inserted_row {
        return Ok(InsertSignalOutcome {
            signal: map_signal(&row),
            inserted: true,
        });
    }

    let existing_row = sqlx::query(
        r#"
        SELECT
            id,
            strategy_id,
            symbol,
            side,
            confidence,
            timeframe,
            reason,
            suggested_notional,
            stop_loss_pct,
            take_profit_pct,
            source_candle_open_time,
            correlation_id,
            created_at
        FROM signals
        WHERE strategy_id = $1
          AND symbol = $2
          AND timeframe = $3
          AND source_candle_open_time = $4
          AND side = $5
          AND reason = $6
        "#,
    )
    .bind(signal.strategy_id.as_str())
    .bind(signal.symbol.as_str())
    .bind(signal.timeframe.as_str())
    .bind(signal.source_candle_open_time)
    .bind(signal.side.as_str())
    .bind(signal.reason.as_str())
    .fetch_one(pool)
    .await?;

    Ok(InsertSignalOutcome {
        signal: map_signal(&existing_row),
        inserted: false,
    })
}

pub async fn get_signal_by_id(pool: &PgPool, signal_id: Uuid) -> Result<Option<SignalRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            strategy_id,
            symbol,
            side,
            confidence,
            timeframe,
            reason,
            suggested_notional,
            stop_loss_pct,
            take_profit_pct,
            source_candle_open_time,
            correlation_id,
            created_at
        FROM signals
        WHERE id = $1
        "#,
    )
    .bind(signal_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_signal))
}

pub async fn find_signal_by_identity(
    pool: &PgPool,
    strategy_id: &str,
    symbol: &str,
    timeframe: &str,
    side: &str,
    reason: &str,
    source_candle_open_time: DateTime<Utc>,
) -> Result<Option<SignalRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            strategy_id,
            symbol,
            side,
            confidence,
            timeframe,
            reason,
            suggested_notional,
            stop_loss_pct,
            take_profit_pct,
            source_candle_open_time,
            correlation_id,
            created_at
        FROM signals
        WHERE strategy_id = $1
          AND symbol = $2
          AND timeframe = $3
          AND source_candle_open_time = $4
          AND side = $5
          AND reason = $6
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        "#,
    )
    .bind(strategy_id)
    .bind(symbol)
    .bind(timeframe)
    .bind(source_candle_open_time)
    .bind(side)
    .bind(reason)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_signal))
}

pub async fn list_recent_signals(
    pool: &PgPool,
    symbol: Option<&Symbol>,
    limit: i64,
) -> Result<Vec<SignalRecord>> {
    let rows = if let Some(symbol) = symbol {
        sqlx::query(
            r#"
            SELECT
                id,
                strategy_id,
                symbol,
                side,
                confidence,
                timeframe,
                reason,
                suggested_notional,
                stop_loss_pct,
                take_profit_pct,
                source_candle_open_time,
                correlation_id,
                created_at
            FROM signals
            WHERE symbol = $1
            ORDER BY created_at DESC
            LIMIT $2
            "#,
        )
        .bind(symbol.as_str())
        .bind(limit)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            r#"
            SELECT
                id,
                strategy_id,
                symbol,
                side,
                confidence,
                timeframe,
                reason,
                suggested_notional,
                stop_loss_pct,
                take_profit_pct,
                source_candle_open_time,
                correlation_id,
                created_at
            FROM signals
            ORDER BY created_at DESC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(pool)
        .await?
    };

    Ok(rows.iter().map(map_signal).collect())
}

pub async fn get_strategy_status(
    pool: &PgPool,
    strategy_id: StrategyId,
) -> Result<Option<StrategyStatusRecord>> {
    let config = match get_strategy_config(pool, strategy_id).await? {
        Some(config) => config,
        None => return Ok(None),
    };

    let state = sqlx::query(
        r#"
        SELECT
            strategy_id,
            last_evaluated_at,
            last_evaluation_reason,
            last_signal_id,
            last_signal_at,
            updated_at
        FROM strategy_state
        WHERE strategy_id = $1
        "#,
    )
    .bind(strategy_id.as_str())
    .fetch_optional(pool)
    .await?;

    Ok(Some(StrategyStatusRecord {
        config,
        state: state.as_ref().map(map_strategy_state),
    }))
}

pub async fn list_strategy_status(pool: &PgPool) -> Result<Vec<StrategyStatusRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            c.strategy_id AS config_strategy_id,
            c.enabled,
            c.mode,
            c.symbols,
            c.timeframe,
            c.suggested_notional,
            c.max_signal_age_ms,
            c.cooldown_seconds,
            c.lookback_candles,
            c.trend_lookback_candles,
            c.strategy_momentum_lookback_candles,
            c.strategy_breakout_lookback_candles,
            c.confidence_floor,
            c.stop_loss_pct,
            c.take_profit_pct,
            c.holding_candles,
            c.notes,
            c.current_version,
            c.created_at AS config_created_at,
            c.updated_at AS config_updated_at,
            s.strategy_id AS state_strategy_id,
            s.last_evaluated_at,
            s.last_evaluation_reason,
            s.last_signal_id,
            s.last_signal_at,
            s.updated_at AS state_updated_at
        FROM strategy_configs c
        LEFT JOIN strategy_state s
            ON s.strategy_id = c.strategy_id
        ORDER BY c.strategy_id
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|row| StrategyStatusRecord {
            config: StrategyConfigRecord {
                strategy_id: row.get("config_strategy_id"),
                enabled: row.get("enabled"),
                mode: row.get("mode"),
                symbols: row.get("symbols"),
                timeframe: row.get("timeframe"),
                suggested_notional: row.get("suggested_notional"),
                max_signal_age_ms: row.get("max_signal_age_ms"),
                cooldown_seconds: row.get("cooldown_seconds"),
                lookback_candles: row.get("lookback_candles"),
                trend_lookback_candles: row.get("trend_lookback_candles"),
                momentum_lookback_candles: row.get("strategy_momentum_lookback_candles"),
                breakout_lookback_candles: row.get("strategy_breakout_lookback_candles"),
                confidence_floor: row.get("confidence_floor"),
                stop_loss_pct: row.get("stop_loss_pct"),
                take_profit_pct: row.get("take_profit_pct"),
                holding_candles: row.get("holding_candles"),
                notes: row.get("notes"),
                current_version: row.get("current_version"),
                created_at: row.get("config_created_at"),
                updated_at: row.get("config_updated_at"),
            },
            state: row
                .get::<Option<String>, _>("state_strategy_id")
                .map(|strategy_id| StrategyStateRecord {
                    strategy_id,
                    last_evaluated_at: row.get("last_evaluated_at"),
                    last_evaluation_reason: row.get("last_evaluation_reason"),
                    last_signal_id: row.get("last_signal_id"),
                    last_signal_at: row.get("last_signal_at"),
                    updated_at: row.get("state_updated_at"),
                }),
        })
        .collect())
}

pub fn strategy_config_from_record(record: &StrategyConfigRecord) -> Result<StrategyConfig> {
    let symbols = record
        .symbols
        .split(',')
        .filter(|value| !value.trim().is_empty())
        .map(Symbol::new)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let config = StrategyConfig {
        strategy_id: record.strategy_id.parse()?,
        enabled: record.enabled,
        mode: record.mode.parse()?,
        symbols,
        timeframe: record.timeframe.parse()?,
        suggested_notional: record.suggested_notional,
        max_signal_age_ms: record.max_signal_age_ms,
        cooldown_seconds: record.cooldown_seconds as u32,
        lookback_candles: record.lookback_candles as u32,
        trend_lookback_candles: record.trend_lookback_candles.map(|value| value as u32),
        momentum_lookback_candles: record.momentum_lookback_candles.map(|value| value as u32),
        breakout_lookback_candles: record.breakout_lookback_candles.map(|value| value as u32),
        confidence_floor: record.confidence_floor,
        stop_loss_pct: record.stop_loss_pct,
        take_profit_pct: record.take_profit_pct,
        holding_candles: record.holding_candles.map(|value| value as u32),
        notes: record.notes.clone(),
    };
    config.validate()?;
    Ok(config)
}

pub async fn persist_strategy_config_version(
    pool: &PgPool,
    config: &StrategyConfig,
    actor_id: Option<Uuid>,
    correlation_id: Uuid,
) -> Result<StrategyConfigRecord> {
    let mut tx = pool.begin().await?;
    let existing = get_strategy_config_tx(&mut tx, config.strategy_id).await?;
    let persisted_version = sqlx::query_scalar::<_, Option<i32>>(
        r#"
        SELECT MAX(version)
        FROM strategy_config_versions
        WHERE strategy_id = $1
        "#,
    )
    .bind(config.strategy_id.as_str())
    .fetch_one(&mut *tx)
    .await?
    .unwrap_or(0);
    let next_version = existing
        .as_ref()
        .map(|record| record.current_version)
        .unwrap_or(0)
        .max(persisted_version)
        + 1;
    let record = upsert_strategy_config_tx(&mut tx, config, next_version).await?;
    let config_json = strategy_config_to_value(config)?;

    sqlx::query(
        r#"
        INSERT INTO strategy_config_versions (
            id,
            strategy_id,
            version,
            config,
            actor_id,
            correlation_id,
            created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(config.strategy_id.as_str())
    .bind(next_version)
    .bind(config_json.clone())
    .bind(actor_id)
    .bind(correlation_id)
    .execute(&mut *tx)
    .await?;

    insert_strategy_config_audit_tx(
        &mut tx,
        &StrategyConfigAuditEntry {
            audit_id: Uuid::new_v4(),
            strategy_id: config.strategy_id.to_string(),
            version: Some(next_version),
            old_config: existing
                .as_ref()
                .map(strategy_config_from_record)
                .transpose()?,
            new_config: Some(config.clone()),
            validation_issues: Vec::new(),
            actor_id,
            correlation_id,
            created_at: Utc::now(),
        },
    )
    .await?;

    tx.commit().await?;
    Ok(record)
}

pub async fn insert_strategy_config_audit(
    pool: &PgPool,
    entry: &StrategyConfigAuditEntry,
) -> Result<StrategyConfigAuditRecord> {
    let mut tx = pool.begin().await?;
    let record = insert_strategy_config_audit_tx(&mut tx, entry).await?;
    tx.commit().await?;
    Ok(record)
}

pub async fn list_strategy_config_versions(
    pool: &PgPool,
    strategy_id: StrategyId,
) -> Result<Vec<StrategyConfigVersionRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            strategy_id,
            version,
            config,
            actor_id,
            correlation_id,
            created_at
        FROM strategy_config_versions
        WHERE strategy_id = $1
        ORDER BY version DESC, created_at DESC
        "#,
    )
    .bind(strategy_id.as_str())
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_strategy_config_version).collect())
}

pub async fn list_strategy_config_audit(
    pool: &PgPool,
    strategy_id: StrategyId,
) -> Result<Vec<StrategyConfigAuditRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            strategy_id,
            version,
            old_config,
            new_config,
            validation_issues,
            actor_id,
            correlation_id,
            created_at
        FROM strategy_config_audit
        WHERE strategy_id = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(strategy_id.as_str())
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_strategy_config_audit).collect())
}

pub fn strategy_config_version_from_record(
    record: &StrategyConfigVersionRecord,
) -> Result<StrategyConfigVersion> {
    Ok(StrategyConfigVersion {
        strategy_id: record.strategy_id.clone(),
        version: record.version,
        config: serde_json::from_value(record.config.clone())?,
        actor_id: record.actor_id,
        correlation_id: record.correlation_id,
        created_at: record.created_at,
    })
}

pub fn strategy_config_audit_from_record(
    record: &StrategyConfigAuditRecord,
) -> Result<StrategyConfigAuditEntry> {
    Ok(StrategyConfigAuditEntry {
        audit_id: record.id,
        strategy_id: record.strategy_id.clone(),
        version: record.version,
        old_config: record
            .old_config
            .clone()
            .map(serde_json::from_value)
            .transpose()?,
        new_config: record
            .new_config
            .clone()
            .map(serde_json::from_value)
            .transpose()?,
        validation_issues: serde_json::from_value(record.validation_issues.clone())?,
        actor_id: record.actor_id,
        correlation_id: record.correlation_id,
        created_at: record.created_at,
    })
}

async fn insert_strategy_config_audit_tx(
    tx: &mut Transaction<'_, Postgres>,
    entry: &StrategyConfigAuditEntry,
) -> Result<StrategyConfigAuditRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO strategy_config_audit (
            id,
            strategy_id,
            version,
            old_config,
            new_config,
            validation_issues,
            actor_id,
            correlation_id,
            created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING
            id,
            strategy_id,
            version,
            old_config,
            new_config,
            validation_issues,
            actor_id,
            correlation_id,
            created_at
        "#,
    )
    .bind(entry.audit_id)
    .bind(&entry.strategy_id)
    .bind(entry.version)
    .bind(
        entry
            .old_config
            .as_ref()
            .map(strategy_config_to_value)
            .transpose()?,
    )
    .bind(
        entry
            .new_config
            .as_ref()
            .map(strategy_config_to_value)
            .transpose()?,
    )
    .bind(serde_json::to_value(&entry.validation_issues)?)
    .bind(entry.actor_id)
    .bind(entry.correlation_id)
    .bind(entry.created_at)
    .fetch_one(&mut **tx)
    .await?;

    Ok(map_strategy_config_audit(&row))
}

fn strategy_config_to_value(config: &StrategyConfig) -> Result<Value> {
    Ok(serde_json::to_value(config)?)
}

pub async fn upsert_risk_config(pool: &PgPool, config: &RiskConfig) -> Result<RiskConfigRecord> {
    let mut tx = pool.begin().await?;
    let existing = get_risk_config_tx(&mut tx).await?;
    let current_version = existing
        .as_ref()
        .map(|record| record.current_version)
        .unwrap_or(1);
    let config_id = existing
        .as_ref()
        .map(|record| record.config_id)
        .unwrap_or_else(Uuid::new_v4);
    let record = upsert_risk_config_tx(&mut tx, config_id, config, current_version).await?;
    tx.commit().await?;
    Ok(record)
}

async fn upsert_risk_config_tx(
    tx: &mut Transaction<'_, Postgres>,
    config_id: Uuid,
    config: &RiskConfig,
    current_version: i32,
) -> Result<RiskConfigRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO risk_configs (
            config_key,
            config_id,
            max_open_positions,
            max_daily_loss_pct,
            max_weekly_loss_pct,
            max_position_notional,
            max_slippage_pct,
            max_consecutive_losses,
            cooldown_seconds,
            max_signal_age_ms,
            stale_feed_threshold_seconds,
            current_version,
            created_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW(), NOW())
        ON CONFLICT (config_key) DO UPDATE
        SET
            config_id = EXCLUDED.config_id,
            max_open_positions = EXCLUDED.max_open_positions,
            max_daily_loss_pct = EXCLUDED.max_daily_loss_pct,
            max_weekly_loss_pct = EXCLUDED.max_weekly_loss_pct,
            max_position_notional = EXCLUDED.max_position_notional,
            max_slippage_pct = EXCLUDED.max_slippage_pct,
            max_consecutive_losses = EXCLUDED.max_consecutive_losses,
            cooldown_seconds = EXCLUDED.cooldown_seconds,
            max_signal_age_ms = EXCLUDED.max_signal_age_ms,
            stale_feed_threshold_seconds = EXCLUDED.stale_feed_threshold_seconds,
            current_version = EXCLUDED.current_version,
            updated_at = NOW()
        RETURNING
            config_key,
            config_id,
            max_open_positions,
            max_daily_loss_pct,
            max_weekly_loss_pct,
            max_position_notional,
            max_slippage_pct,
            max_consecutive_losses,
            cooldown_seconds,
            max_signal_age_ms,
            stale_feed_threshold_seconds,
            current_version,
            created_at,
            updated_at
        "#,
    )
    .bind(GLOBAL_RISK_CONFIG_KEY)
    .bind(config_id)
    .bind(config.max_open_positions as i32)
    .bind(config.max_daily_loss_pct)
    .bind(config.max_weekly_loss_pct)
    .bind(config.max_position_notional)
    .bind(config.max_slippage_pct)
    .bind(config.max_consecutive_losses as i32)
    .bind(config.cooldown_seconds as i32)
    .bind(config.max_signal_age_ms)
    .bind(config.stale_feed_threshold_seconds as i32)
    .bind(current_version)
    .fetch_one(&mut **tx)
    .await?;

    Ok(map_risk_config(&row))
}

async fn get_risk_config_tx(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Option<RiskConfigRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            config_key,
            config_id,
            max_open_positions,
            max_daily_loss_pct,
            max_weekly_loss_pct,
            max_position_notional,
            max_slippage_pct,
            max_consecutive_losses,
            cooldown_seconds,
            max_signal_age_ms,
            stale_feed_threshold_seconds,
            current_version,
            created_at,
            updated_at
        FROM risk_configs
        WHERE config_key = $1
        "#,
    )
    .bind(GLOBAL_RISK_CONFIG_KEY)
    .fetch_optional(&mut **tx)
    .await?;

    Ok(row.as_ref().map(map_risk_config))
}

pub async fn get_risk_config(pool: &PgPool) -> Result<Option<RiskConfigRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            config_key,
            config_id,
            max_open_positions,
            max_daily_loss_pct,
            max_weekly_loss_pct,
            max_position_notional,
            max_slippage_pct,
            max_consecutive_losses,
            cooldown_seconds,
            max_signal_age_ms,
            stale_feed_threshold_seconds,
            current_version,
            created_at,
            updated_at
        FROM risk_configs
        WHERE config_key = $1
        "#,
    )
    .bind(GLOBAL_RISK_CONFIG_KEY)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_risk_config))
}

pub fn risk_config_from_record(record: &RiskConfigRecord) -> Result<RiskConfig> {
    let config = RiskConfig {
        max_open_positions: record.max_open_positions as u32,
        max_daily_loss_pct: record.max_daily_loss_pct,
        max_weekly_loss_pct: record.max_weekly_loss_pct,
        max_position_notional: record.max_position_notional,
        max_slippage_pct: record.max_slippage_pct,
        max_consecutive_losses: record.max_consecutive_losses as u32,
        cooldown_seconds: record.cooldown_seconds as u32,
        max_signal_age_ms: record.max_signal_age_ms,
        stale_feed_threshold_seconds: record.stale_feed_threshold_seconds as u32,
    };
    config.validate()?;
    Ok(config)
}

pub async fn persist_risk_config_version(
    pool: &PgPool,
    config: &RiskConfig,
    actor_id: Option<Uuid>,
    correlation_id: Uuid,
) -> Result<RiskConfigRecord> {
    let mut tx = pool.begin().await?;
    let existing = get_risk_config_tx(&mut tx).await?;
    let next_version = existing
        .as_ref()
        .map(|record| record.current_version + 1)
        .unwrap_or(1);
    let config_id = existing
        .as_ref()
        .map(|record| record.config_id)
        .unwrap_or_else(Uuid::new_v4);
    let record = upsert_risk_config_tx(&mut tx, config_id, config, next_version).await?;
    let config_json = risk_config_to_value(config)?;

    sqlx::query(
        r#"
        INSERT INTO risk_config_versions (
            id,
            config_key,
            config_id,
            version,
            config,
            actor_id,
            correlation_id,
            created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(GLOBAL_RISK_CONFIG_KEY)
    .bind(config_id)
    .bind(next_version)
    .bind(config_json)
    .bind(actor_id)
    .bind(correlation_id)
    .execute(&mut *tx)
    .await?;

    insert_risk_config_audit_tx(
        &mut tx,
        &RiskConfigAuditEntry {
            audit_id: Uuid::new_v4(),
            config_id,
            version: Some(next_version),
            old_config: existing.as_ref().map(risk_config_from_record).transpose()?,
            new_config: Some(config.clone()),
            validation_issues: Vec::new(),
            actor_id,
            correlation_id,
            created_at: Utc::now(),
        },
    )
    .await?;

    tx.commit().await?;
    Ok(record)
}

pub async fn insert_risk_config_audit(
    pool: &PgPool,
    entry: &RiskConfigAuditEntry,
) -> Result<RiskConfigAuditRecord> {
    let mut tx = pool.begin().await?;
    let record = insert_risk_config_audit_tx(&mut tx, entry).await?;
    tx.commit().await?;
    Ok(record)
}

pub async fn list_risk_config_versions(pool: &PgPool) -> Result<Vec<RiskConfigVersionRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            config_key,
            config_id,
            version,
            config,
            actor_id,
            correlation_id,
            created_at
        FROM risk_config_versions
        WHERE config_key = $1
        ORDER BY version DESC, created_at DESC
        "#,
    )
    .bind(GLOBAL_RISK_CONFIG_KEY)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_risk_config_version).collect())
}

pub async fn list_risk_config_audit(pool: &PgPool) -> Result<Vec<RiskConfigAuditRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            config_key,
            config_id,
            version,
            old_config,
            new_config,
            validation_issues,
            actor_id,
            correlation_id,
            created_at
        FROM risk_config_audit
        WHERE config_key = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(GLOBAL_RISK_CONFIG_KEY)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_risk_config_audit).collect())
}

pub fn risk_config_version_from_record(
    record: &RiskConfigVersionRecord,
) -> Result<RiskConfigVersion> {
    Ok(RiskConfigVersion {
        config_id: record.config_id,
        version: record.version,
        config: serde_json::from_value(record.config.clone())?,
        actor_id: record.actor_id,
        correlation_id: record.correlation_id,
        created_at: record.created_at,
    })
}

pub fn risk_config_audit_from_record(
    record: &RiskConfigAuditRecord,
) -> Result<RiskConfigAuditEntry> {
    Ok(RiskConfigAuditEntry {
        audit_id: record.id,
        config_id: record.config_id,
        version: record.version,
        old_config: record
            .old_config
            .clone()
            .map(serde_json::from_value)
            .transpose()?,
        new_config: record
            .new_config
            .clone()
            .map(serde_json::from_value)
            .transpose()?,
        validation_issues: serde_json::from_value(record.validation_issues.clone())?,
        actor_id: record.actor_id,
        correlation_id: record.correlation_id,
        created_at: record.created_at,
    })
}

async fn insert_risk_config_audit_tx(
    tx: &mut Transaction<'_, Postgres>,
    entry: &RiskConfigAuditEntry,
) -> Result<RiskConfigAuditRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO risk_config_audit (
            id,
            config_key,
            config_id,
            version,
            old_config,
            new_config,
            validation_issues,
            actor_id,
            correlation_id,
            created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING
            id,
            config_key,
            config_id,
            version,
            old_config,
            new_config,
            validation_issues,
            actor_id,
            correlation_id,
            created_at
        "#,
    )
    .bind(entry.audit_id)
    .bind(GLOBAL_RISK_CONFIG_KEY)
    .bind(entry.config_id)
    .bind(entry.version)
    .bind(
        entry
            .old_config
            .as_ref()
            .map(risk_config_to_value)
            .transpose()?,
    )
    .bind(
        entry
            .new_config
            .as_ref()
            .map(risk_config_to_value)
            .transpose()?,
    )
    .bind(serde_json::to_value(&entry.validation_issues)?)
    .bind(entry.actor_id)
    .bind(entry.correlation_id)
    .bind(entry.created_at)
    .fetch_one(&mut **tx)
    .await?;

    Ok(map_risk_config_audit(&row))
}

fn risk_config_to_value(config: &RiskConfig) -> Result<Value> {
    Ok(serde_json::to_value(config)?)
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
    list_recent_system_events_filtered(pool, limit, None, None, None).await
}

pub async fn list_recent_system_events_filtered(
    pool: &PgPool,
    limit: i64,
    event_type: Option<&str>,
    source: Option<&str>,
    correlation_id: Option<Uuid>,
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
        WHERE ($1::text IS NULL OR event_type = $1)
          AND ($2::text IS NULL OR source = $2)
          AND ($3::uuid IS NULL OR correlation_id = $3)
        ORDER BY created_at DESC
        LIMIT $4
        "#,
    )
    .bind(event_type)
    .bind(source)
    .bind(correlation_id)
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

fn map_candle_backfill_run(row: &sqlx::postgres::PgRow) -> CandleBackfillRunRecord {
    CandleBackfillRunRecord {
        id: row.get("id"),
        exchange: row.get("exchange"),
        symbol: row.get("symbol"),
        interval: row.get("interval"),
        start_time: row.get("start_time"),
        end_time: row.get("end_time"),
        status: row.get("status"),
        requested_candles_estimate: row.get("requested_candles_estimate"),
        fetched_candles: row.get("fetched_candles"),
        inserted_candles: row.get("inserted_candles"),
        updated_candles: row.get("updated_candles"),
        skipped_candles: row.get("skipped_candles"),
        failed_reason: row.get("failed_reason"),
        correlation_id: row.get("correlation_id"),
        created_at: row.get("created_at"),
        completed_at: row.get("completed_at"),
        config: row.get("config"),
    }
}

fn map_candle_domain(row: &sqlx::postgres::PgRow) -> Candle {
    Candle {
        id: row.get("id"),
        exchange: row
            .get::<String, _>("exchange")
            .parse()
            .expect("database exchange must be supported"),
        symbol: Symbol::new(row.get::<String, _>("symbol")).expect("database symbol must be valid"),
        interval: row
            .get::<String, _>("interval")
            .parse()
            .expect("database interval must be supported"),
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

fn candle_matches_record(candle: &Candle, record: &CandleRecord) -> bool {
    candle.exchange.as_str() == record.exchange
        && candle.symbol.as_str() == record.symbol
        && candle.interval.as_str() == record.interval
        && candle.open_time == record.open_time
        && candle.close_time == record.close_time
        && candle.open == record.open
        && candle.high == record.high
        && candle.low == record.low
        && candle.close == record.close
        && candle.volume == record.volume
        && candle.quote_volume == record.quote_volume
        && candle.trade_count == record.trade_count
        && candle.is_closed == record.is_closed
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

fn map_strategy_config(row: &sqlx::postgres::PgRow) -> StrategyConfigRecord {
    StrategyConfigRecord {
        strategy_id: row.get("strategy_id"),
        enabled: row.get("enabled"),
        mode: row.get("mode"),
        symbols: row.get("symbols"),
        timeframe: row.get("timeframe"),
        suggested_notional: row.get("suggested_notional"),
        max_signal_age_ms: row.get("max_signal_age_ms"),
        cooldown_seconds: row.get("cooldown_seconds"),
        lookback_candles: row.get("lookback_candles"),
        trend_lookback_candles: row.get("trend_lookback_candles"),
        momentum_lookback_candles: row.get("strategy_momentum_lookback_candles"),
        breakout_lookback_candles: row.get("strategy_breakout_lookback_candles"),
        confidence_floor: row.get("confidence_floor"),
        stop_loss_pct: row.get("stop_loss_pct"),
        take_profit_pct: row.get("take_profit_pct"),
        holding_candles: row.get("holding_candles"),
        notes: row.get("notes"),
        current_version: row.get("current_version"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn map_strategy_config_version(row: &sqlx::postgres::PgRow) -> StrategyConfigVersionRecord {
    StrategyConfigVersionRecord {
        id: row.get("id"),
        strategy_id: row.get("strategy_id"),
        version: row.get("version"),
        config: row.get("config"),
        actor_id: row.get("actor_id"),
        correlation_id: row.get("correlation_id"),
        created_at: row.get("created_at"),
    }
}

fn map_strategy_config_audit(row: &sqlx::postgres::PgRow) -> StrategyConfigAuditRecord {
    StrategyConfigAuditRecord {
        id: row.get("id"),
        strategy_id: row.get("strategy_id"),
        version: row.get("version"),
        old_config: row.get("old_config"),
        new_config: row.get("new_config"),
        validation_issues: row.get("validation_issues"),
        actor_id: row.get("actor_id"),
        correlation_id: row.get("correlation_id"),
        created_at: row.get("created_at"),
    }
}

fn map_risk_config(row: &sqlx::postgres::PgRow) -> RiskConfigRecord {
    RiskConfigRecord {
        config_key: row.get("config_key"),
        config_id: row.get("config_id"),
        max_open_positions: row.get("max_open_positions"),
        max_daily_loss_pct: row.get("max_daily_loss_pct"),
        max_weekly_loss_pct: row.get("max_weekly_loss_pct"),
        max_position_notional: row.get("max_position_notional"),
        max_slippage_pct: row.get("max_slippage_pct"),
        max_consecutive_losses: row.get("max_consecutive_losses"),
        cooldown_seconds: row.get("cooldown_seconds"),
        max_signal_age_ms: row.get("max_signal_age_ms"),
        stale_feed_threshold_seconds: row.get("stale_feed_threshold_seconds"),
        current_version: row.get("current_version"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn map_risk_config_version(row: &sqlx::postgres::PgRow) -> RiskConfigVersionRecord {
    RiskConfigVersionRecord {
        id: row.get("id"),
        config_key: row.get("config_key"),
        config_id: row.get("config_id"),
        version: row.get("version"),
        config: row.get("config"),
        actor_id: row.get("actor_id"),
        correlation_id: row.get("correlation_id"),
        created_at: row.get("created_at"),
    }
}

fn map_risk_config_audit(row: &sqlx::postgres::PgRow) -> RiskConfigAuditRecord {
    RiskConfigAuditRecord {
        id: row.get("id"),
        config_key: row.get("config_key"),
        config_id: row.get("config_id"),
        version: row.get("version"),
        old_config: row.get("old_config"),
        new_config: row.get("new_config"),
        validation_issues: row.get("validation_issues"),
        actor_id: row.get("actor_id"),
        correlation_id: row.get("correlation_id"),
        created_at: row.get("created_at"),
    }
}

fn map_strategy_state(row: &sqlx::postgres::PgRow) -> StrategyStateRecord {
    StrategyStateRecord {
        strategy_id: row.get("strategy_id"),
        last_evaluated_at: row.get("last_evaluated_at"),
        last_evaluation_reason: row.get("last_evaluation_reason"),
        last_signal_id: row.get("last_signal_id"),
        last_signal_at: row.get("last_signal_at"),
        updated_at: row.get("updated_at"),
    }
}

fn map_backtest_run(row: &sqlx::postgres::PgRow) -> BacktestRunRecord {
    BacktestRunRecord {
        id: row.get("id"),
        strategy_id: row.get("strategy_id"),
        symbol: row.get("symbol"),
        timeframe: row.get("timeframe"),
        start_time: row.get("start_time"),
        end_time: row.get("end_time"),
        initial_capital: row.get("initial_capital"),
        final_equity: row.get("final_equity"),
        pnl: row.get("pnl"),
        pnl_pct: row.get("pnl_pct"),
        max_drawdown_pct: row.get("max_drawdown_pct"),
        win_rate: row.get("win_rate"),
        trade_count: row.get("trade_count"),
        winning_trades: row.get("winning_trades"),
        losing_trades: row.get("losing_trades"),
        avg_win: row.get("avg_win"),
        avg_loss: row.get("avg_loss"),
        fee_paid: row.get("fee_paid"),
        slippage_cost: row.get("slippage_cost"),
        status: row.get("status"),
        config: row.get("config"),
        correlation_id: row.get("correlation_id"),
        created_at: row.get("created_at"),
    }
}

fn map_backtest_trade(row: &sqlx::postgres::PgRow) -> BacktestTradeRecord {
    BacktestTradeRecord {
        id: row.get("id"),
        run_id: row.get("run_id"),
        strategy_id: row.get("strategy_id"),
        symbol: row.get("symbol"),
        side: row.get("side"),
        entry_time: row.get("entry_time"),
        entry_price: row.get("entry_price"),
        exit_time: row.get("exit_time"),
        exit_price: row.get("exit_price"),
        quantity: row.get("quantity"),
        notional: row.get("notional"),
        fee_paid: row.get("fee_paid"),
        slippage_cost: row.get("slippage_cost"),
        realized_pnl: row.get("realized_pnl"),
        reason: row.get("reason"),
        created_at: row.get("created_at"),
    }
}

fn map_backtest_equity_point(row: &sqlx::postgres::PgRow) -> BacktestEquityPointRecord {
    BacktestEquityPointRecord {
        id: row.get("id"),
        run_id: row.get("run_id"),
        timestamp: row.get("timestamp"),
        equity: row.get("equity"),
        drawdown_pct: row.get("drawdown_pct"),
    }
}

fn map_strategy_experiment(row: &sqlx::postgres::PgRow) -> StrategyExperimentRecord {
    StrategyExperimentRecord {
        id: row.get("id"),
        experiment_group_id: row.get("experiment_group_id"),
        strategy_id: row.get("strategy_id"),
        symbol: row.get("symbol"),
        timeframe: row.get("timeframe"),
        start_time: row.get("start_time"),
        end_time: row.get("end_time"),
        initial_capital: row.get("initial_capital"),
        fee_bps: row.get("fee_bps"),
        slippage_bps: row.get("slippage_bps"),
        max_signal_age_ms: row.get("max_signal_age_ms"),
        max_runs: row.get("max_runs"),
        status: row.get("status"),
        comparison: row.get("comparison"),
        candle_count: row.get("candle_count"),
        warnings: row.get("warnings"),
        skipped_reason: row.get("skipped_reason"),
        correlation_id: row.get("correlation_id"),
        created_at: row.get("created_at"),
    }
}

fn map_strategy_experiment_run(row: &sqlx::postgres::PgRow) -> StrategyExperimentRunRecord {
    StrategyExperimentRunRecord {
        id: row.get("id"),
        experiment_id: row.get("experiment_id"),
        rank: row.get("rank"),
        candidate_config: row.get("candidate_config"),
        final_equity: row.get("final_equity"),
        pnl: row.get("pnl"),
        pnl_pct: row.get("pnl_pct"),
        max_drawdown_pct: row.get("max_drawdown_pct"),
        win_rate: row.get("win_rate"),
        trade_count: row.get("trade_count"),
        fee_paid: row.get("fee_paid"),
        slippage_cost: row.get("slippage_cost"),
        fee_slippage_drag_pct: row.get("fee_slippage_drag_pct"),
        score: row.get("score"),
        status: row.get("status"),
        warnings: row.get("warnings"),
        created_at: row.get("created_at"),
    }
}

fn map_strategy_walk_forward_run(row: &sqlx::postgres::PgRow) -> StrategyWalkForwardRunRecord {
    StrategyWalkForwardRunRecord {
        id: row.get("id"),
        strategy_id: row.get("strategy_id"),
        symbol: row.get("symbol"),
        timeframe: row.get("timeframe"),
        request: row.get("request"),
        status: row.get("status"),
        total_windows: row.get("total_windows"),
        completed_windows: row.get("completed_windows"),
        skipped_windows: row.get("skipped_windows"),
        profitable_test_windows: row.get("profitable_test_windows"),
        losing_test_windows: row.get("losing_test_windows"),
        avg_test_pnl_pct: row.get("avg_test_pnl_pct"),
        median_test_pnl_pct: row.get("median_test_pnl_pct"),
        worst_test_pnl_pct: row.get("worst_test_pnl_pct"),
        best_test_pnl_pct: row.get("best_test_pnl_pct"),
        avg_max_drawdown_pct: row.get("avg_max_drawdown_pct"),
        robustness_score: row.get("robustness_score"),
        robustness_summary: row.get("robustness_summary"),
        created_at: row.get("created_at"),
        correlation_id: row.get("correlation_id"),
    }
}

fn map_strategy_walk_forward_window(
    row: &sqlx::postgres::PgRow,
) -> StrategyWalkForwardWindowRecord {
    StrategyWalkForwardWindowRecord {
        id: row.get("id"),
        walk_forward_id: row.get("walk_forward_id"),
        window_index: row.get("window_index"),
        train_start: row.get("train_start"),
        train_end: row.get("train_end"),
        test_start: row.get("test_start"),
        test_end: row.get("test_end"),
        status: row.get("status"),
        skip_reason: row.get("skip_reason"),
        trade_count: row.get("trade_count"),
        pnl: row.get("pnl"),
        pnl_pct: row.get("pnl_pct"),
        max_drawdown_pct: row.get("max_drawdown_pct"),
        win_rate: row.get("win_rate"),
        fee_paid: row.get("fee_paid"),
        slippage_cost: row.get("slippage_cost"),
        result: row.get("result"),
        created_at: row.get("created_at"),
    }
}

fn map_signal(row: &sqlx::postgres::PgRow) -> SignalRecord {
    SignalRecord {
        id: row.get("id"),
        strategy_id: row.get("strategy_id"),
        symbol: row.get("symbol"),
        side: row.get("side"),
        confidence: row.get("confidence"),
        timeframe: row.get("timeframe"),
        reason: row.get("reason"),
        suggested_notional: row.get("suggested_notional"),
        stop_loss_pct: row.get("stop_loss_pct"),
        take_profit_pct: row.get("take_profit_pct"),
        source_candle_open_time: row.get("source_candle_open_time"),
        correlation_id: row.get("correlation_id"),
        created_at: row.get("created_at"),
    }
}

fn map_testnet_shadow_runner_config(
    row: &sqlx::postgres::PgRow,
) -> TestnetShadowRunnerConfigRecord {
    TestnetShadowRunnerConfigRecord {
        id: row.get("id"),
        enabled: row.get("enabled"),
        interval_seconds: row.get("interval_seconds"),
        strategies: row.get("strategies"),
        symbols: row.get("symbols"),
        timeframe: row.get("timeframe"),
        max_runs_per_tick: row.get("max_runs_per_tick"),
        stale_feed_policy: row.get("stale_feed_policy"),
        notes: row.get("notes"),
        updated_by: row.get("updated_by"),
        updated_at: row.get("updated_at"),
    }
}

fn map_testnet_shadow_runner_state(row: &sqlx::postgres::PgRow) -> TestnetShadowRunnerStateRecord {
    TestnetShadowRunnerStateRecord {
        id: row.get("id"),
        status: row.get("status"),
        last_tick_at: row.get("last_tick_at"),
        last_success_at: row.get("last_success_at"),
        last_error: row.get("last_error"),
        total_ticks: row.get("total_ticks"),
        total_runs: row.get("total_runs"),
        updated_at: row.get("updated_at"),
    }
}

pub fn testnet_shadow_run_result_from_record(
    record: &TestnetShadowRunRecord,
) -> Result<TestnetShadowRunResult> {
    let would_submit_order = record
        .would_submit_payload
        .as_ref()
        .map(|value| serde_json::from_value::<TestnetShadowIntent>(value.clone()))
        .transpose()?;

    Ok(TestnetShadowRunResult {
        run_id: record.id,
        strategy_id: record.strategy_id.clone(),
        symbol: record.symbol.clone(),
        timeframe: record.timeframe.clone(),
        decision: record.decision.parse::<TestnetShadowDecision>()?,
        signal_id: record.signal_id,
        risk_decision_id: record.risk_decision_id,
        would_submit_order,
        reasons: record
            .reasons
            .iter()
            .map(|value| value.parse::<TestnetShadowRejectionReason>())
            .collect::<Result<Vec<_>, _>>()?,
        price_source: record.price_source.clone(),
        resolved_price: record.resolved_price,
        status: record.status.parse::<TestnetShadowStatus>()?,
        created_at: record.created_at,
        correlation_id: record.correlation_id.unwrap_or(record.id),
    })
}

pub fn testnet_shadow_promotion_from_record(
    record: &TestnetShadowPromotionRecord,
) -> Result<TestnetShadowPromotionPreview> {
    Ok(TestnetShadowPromotionPreview {
        promotion_id: record.id,
        shadow_run_id: record.shadow_run_id,
        strategy_id: record.strategy_id.clone().unwrap_or_default(),
        symbol: record.symbol.clone().unwrap_or_default(),
        timeframe: record.timeframe.clone().unwrap_or_default(),
        signal_id: record.signal_id,
        risk_decision_id: record
            .risk_decision_id
            .ok_or_else(|| anyhow::anyhow!("promotion is missing risk_decision_id"))?,
        would_submit_payload: serde_json::from_value(record.would_submit_payload.clone())?,
        resolved_price: record.resolved_price,
        price_source: record.price_source.clone(),
        expires_at: record.expires_at,
        reasons: record
            .rejection_reasons
            .iter()
            .map(|value| value.parse::<TestnetShadowPromotionRejectionReason>())
            .collect::<Result<Vec<_>, _>>()?,
        status: record.status.parse::<TestnetShadowPromotionStatus>()?,
        correlation_id: record.correlation_id.unwrap_or(record.id),
        created_at: record.created_at,
        submitted_at: record.submitted_at,
        testnet_order_id: record.testnet_order_id,
        client_order_id: record.client_order_id.clone(),
    })
}

pub fn testnet_shadow_runner_config_from_record(
    record: &TestnetShadowRunnerConfigRecord,
) -> Result<TestnetShadowRunnerConfig> {
    Ok(TestnetShadowRunnerConfig {
        id: record.id,
        enabled: record.enabled,
        interval_seconds: record.interval_seconds,
        strategies: serde_json::from_value(record.strategies.clone())?,
        symbols: serde_json::from_value(record.symbols.clone())?,
        timeframe: record.timeframe.clone(),
        max_runs_per_tick: record.max_runs_per_tick,
        stale_feed_policy: record
            .stale_feed_policy
            .parse::<TestnetShadowRunnerStaleFeedPolicy>()?,
        notes: record.notes.clone(),
        updated_by: record.updated_by,
        updated_at: record.updated_at,
    })
}

pub fn testnet_shadow_runner_state_from_record(
    record: &TestnetShadowRunnerStateRecord,
) -> Result<TestnetShadowRunnerState> {
    Ok(TestnetShadowRunnerState {
        id: record.id,
        status: record.status.parse::<TestnetShadowRunnerStatus>()?,
        last_tick_at: record.last_tick_at,
        last_success_at: record.last_success_at,
        last_error: record.last_error.clone(),
        total_ticks: record.total_ticks,
        total_runs: record.total_runs,
        updated_at: record.updated_at,
    })
}

pub fn backtest_result_from_record(record: &BacktestRunRecord) -> Result<BacktestResult> {
    Ok(BacktestResult {
        run_id: record.id,
        strategy_id: record.strategy_id.clone(),
        symbol: record.symbol.clone(),
        timeframe: record.timeframe.clone(),
        start_time: record.start_time,
        end_time: record.end_time,
        initial_capital: record.initial_capital,
        final_equity: record.final_equity,
        pnl: record.pnl,
        pnl_pct: record.pnl_pct,
        max_drawdown_pct: record.max_drawdown_pct,
        win_rate: record.win_rate,
        trade_count: record.trade_count,
        winning_trades: record.winning_trades,
        losing_trades: record.losing_trades,
        avg_win: record.avg_win,
        avg_loss: record.avg_loss,
        fee_paid: record.fee_paid,
        slippage_cost: record.slippage_cost,
        status: record.status.parse()?,
        created_at: record.created_at,
        correlation_id: record.correlation_id,
    })
}

pub fn strategy_experiment_run_from_record(
    record: &StrategyExperimentRunRecord,
) -> Result<StrategyExperimentRun> {
    Ok(StrategyExperimentRun {
        id: record.id,
        experiment_id: record.experiment_id,
        rank: record.rank,
        candidate: serde_json::from_value::<StrategyExperimentCandidate>(
            record.candidate_config.clone(),
        )?,
        final_equity: record.final_equity,
        pnl: record.pnl,
        pnl_pct: record.pnl_pct,
        max_drawdown_pct: record.max_drawdown_pct,
        win_rate: record.win_rate,
        trade_count: record.trade_count,
        fee_paid: record.fee_paid,
        slippage_cost: record.slippage_cost,
        fee_slippage_drag_pct: record.fee_slippage_drag_pct,
        score: record.score,
        status: record.status.parse()?,
        warnings: serde_json::from_value(record.warnings.clone())?,
        created_at: record.created_at,
    })
}

pub fn strategy_experiment_result_from_records(
    record: &StrategyExperimentRecord,
    run_records: &[StrategyExperimentRunRecord],
) -> Result<StrategyExperimentResult> {
    let runs = run_records
        .iter()
        .map(strategy_experiment_run_from_record)
        .collect::<Result<Vec<_>>>()?;
    let comparison =
        serde_json::from_value::<StrategyExperimentComparison>(record.comparison.clone())?;
    let best_run = comparison
        .best_run_id
        .and_then(|id| runs.iter().find(|run| run.id == id).cloned());
    let worst_run = comparison
        .worst_run_id
        .and_then(|id| runs.iter().find(|run| run.id == id).cloned());

    Ok(StrategyExperimentResult {
        experiment_id: record.id,
        experiment_group_id: record.experiment_group_id,
        strategy_id: record.strategy_id.clone(),
        symbol: record.symbol.clone(),
        timeframe: record.timeframe.clone(),
        start_time: record.start_time,
        end_time: record.end_time,
        initial_capital: record.initial_capital,
        fee_bps: record.fee_bps,
        slippage_bps: record.slippage_bps,
        max_signal_age_ms: record.max_signal_age_ms,
        max_runs: record.max_runs.map(|value| value as u32),
        status: record.status.parse()?,
        run_count: runs.len() as i32,
        comparison,
        best_run,
        worst_run,
        candle_count: record.candle_count,
        warnings: serde_json::from_value(record.warnings.clone())?,
        skipped_reason: record.skipped_reason.clone(),
        created_at: record.created_at,
        correlation_id: record.correlation_id,
    })
}

pub fn strategy_walk_forward_window_from_record(
    record: &StrategyWalkForwardWindowRecord,
) -> Result<StrategyWalkForwardWindowResult> {
    Ok(StrategyWalkForwardWindowResult {
        id: record.id,
        walk_forward_id: record.walk_forward_id,
        window: StrategyWalkForwardWindow {
            window_index: record.window_index,
            train_start: record.train_start,
            train_end: record.train_end,
            test_start: record.test_start,
            test_end: record.test_end,
        },
        status: record.status.parse()?,
        skip_reason: record.skip_reason.clone(),
        trade_count: record.trade_count,
        pnl: record.pnl,
        pnl_pct: record.pnl_pct,
        max_drawdown_pct: record.max_drawdown_pct,
        win_rate: record.win_rate,
        fee_paid: record.fee_paid,
        slippage_cost: record.slippage_cost,
        result: record.result.clone(),
        created_at: record.created_at,
    })
}

pub fn strategy_walk_forward_result_from_records(
    record: &StrategyWalkForwardRunRecord,
    _window_records: &[StrategyWalkForwardWindowRecord],
) -> Result<StrategyWalkForwardResult> {
    Ok(StrategyWalkForwardResult {
        walk_forward_id: record.id,
        strategy_id: record.strategy_id.clone(),
        symbol: record.symbol.clone(),
        timeframe: record.timeframe.clone(),
        total_windows: record.total_windows,
        completed_windows: record.completed_windows,
        skipped_windows: record.skipped_windows,
        profitable_test_windows: record.profitable_test_windows,
        losing_test_windows: record.losing_test_windows,
        avg_test_pnl_pct: record.avg_test_pnl_pct,
        median_test_pnl_pct: record.median_test_pnl_pct,
        worst_test_pnl_pct: record.worst_test_pnl_pct,
        best_test_pnl_pct: record.best_test_pnl_pct,
        avg_max_drawdown_pct: record.avg_max_drawdown_pct,
        robustness_score: record.robustness_score,
        status: record.status.parse()?,
        robustness_summary: serde_json::from_value::<StrategyWalkForwardRobustnessSummary>(
            record.robustness_summary.clone(),
        )?,
        created_at: record.created_at,
        correlation_id: record.correlation_id,
    })
}

pub fn candle_backfill_result_from_record(
    record: &CandleBackfillRunRecord,
) -> Result<CandleBackfillResult> {
    Ok(CandleBackfillResult {
        run_id: record.id,
        exchange: record.exchange.parse()?,
        symbol: record.symbol.clone(),
        interval: record.interval.clone(),
        start_time: record.start_time,
        end_time: record.end_time,
        status: record.status.parse()?,
        requested_candles_estimate: record.requested_candles_estimate,
        fetched_candles: record.fetched_candles,
        inserted_candles: record.inserted_candles,
        updated_candles: record.updated_candles,
        skipped_candles: record.skipped_candles,
        failed_reason: record.failed_reason.clone(),
        correlation_id: record.correlation_id,
        created_at: record.created_at,
        completed_at: record.completed_at,
    })
}

pub fn backtest_config_from_value(value: &Value) -> Result<BacktestConfig> {
    Ok(serde_json::from_value(value.clone())?)
}

pub fn user_from_record(record: &UserRecord) -> Result<User> {
    Ok(User {
        id: record.id,
        email: record.email.clone(),
        role: record.role.parse()?,
        status: record.status.parse()?,
        created_at: record.created_at,
        updated_at: record.updated_at,
        last_login_at: record.last_login_at,
    })
}

pub fn session_from_record(record: &SessionRecord) -> Session {
    Session {
        id: record.id,
        user_id: record.user_id,
        expires_at: record.expires_at,
        revoked_at: record.revoked_at,
        created_at: record.created_at,
        updated_at: record.updated_at,
        user_agent: record.user_agent.clone(),
        ip_address: record.ip_address.clone(),
    }
}

fn map_risk_decision(row: &sqlx::postgres::PgRow) -> RiskDecisionRecord {
    let rationale = row.get::<String, _>("rationale");
    let rationale_json = serde_json::from_str::<Value>(&rationale).ok();

    RiskDecisionRecord {
        risk_decision_id: row.get("id"),
        correlation_id: row.get("correlation_id"),
        signal_id: row.get("signal_id"),
        decision: row.get("decision"),
        approved_notional: rationale_json
            .as_ref()
            .and_then(|value| decimal_from_json_field(value, "approved_notional")),
        risk_score: rationale_json
            .as_ref()
            .and_then(|value| decimal_from_json_field(value, "risk_score")),
        reasons: rationale_json
            .as_ref()
            .map(|value| string_array_from_json_field(value, "reasons"))
            .unwrap_or_default(),
        strategy_id: row.try_get("strategy_id").ok(),
        symbol: row.try_get("symbol").ok(),
        rationale,
        created_at: row.get("decided_at"),
    }
}

fn map_testnet_shadow_run(row: &sqlx::postgres::PgRow) -> TestnetShadowRunRecord {
    let reasons_json = row.get::<Value, _>("reasons");
    let reasons = reasons_json
        .as_array()
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(ToString::to_string))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    TestnetShadowRunRecord {
        id: row.get("id"),
        strategy_id: row.get("strategy_id"),
        symbol: row.get("symbol"),
        timeframe: row.get("timeframe"),
        decision: row.get("decision"),
        signal_id: row.get("signal_id"),
        risk_decision_id: row.get("risk_decision_id"),
        would_submit_payload: row.get("would_submit_payload"),
        price_source: row.get("price_source"),
        resolved_price: row.get("resolved_price"),
        reasons,
        status: row.get("status"),
        created_at: row.get("created_at"),
        correlation_id: row.get("correlation_id"),
    }
}

fn map_testnet_shadow_promotion(row: &sqlx::postgres::PgRow) -> TestnetShadowPromotionRecord {
    let reasons_json = row.get::<Value, _>("rejection_reasons");
    let rejection_reasons = reasons_json
        .as_array()
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(ToString::to_string))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    TestnetShadowPromotionRecord {
        id: row.get("id"),
        shadow_run_id: row.get("shadow_run_id"),
        status: row.get("status"),
        strategy_id: row.get("strategy_id"),
        symbol: row.get("symbol"),
        timeframe: row.get("timeframe"),
        signal_id: row.get("signal_id"),
        risk_decision_id: row.get("risk_decision_id"),
        would_submit_payload: row.get("would_submit_payload"),
        resolved_price: row.get("resolved_price"),
        price_source: row.get("price_source"),
        rejection_reasons,
        testnet_order_id: row.get("testnet_order_id"),
        client_order_id: row.get("client_order_id"),
        expires_at: row.get("expires_at"),
        created_by: row.get("created_by"),
        submitted_by: row.get("submitted_by"),
        created_at: row.get("created_at"),
        submitted_at: row.get("submitted_at"),
        correlation_id: row.get("correlation_id"),
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

fn map_user(row: &sqlx::postgres::PgRow) -> UserRecord {
    UserRecord {
        id: row.get("id"),
        email: row.get("email"),
        password_hash: row.get("password_hash"),
        role: row.get("role"),
        status: row.get("status"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        last_login_at: row.get("last_login_at"),
    }
}

fn map_session(row: &sqlx::postgres::PgRow) -> SessionRecord {
    SessionRecord {
        id: row.get("id"),
        user_id: row.get("user_id"),
        refresh_token_hash: row.get("refresh_token_hash"),
        expires_at: row.get("expires_at"),
        revoked_at: row.get("revoked_at"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        user_agent: row.get("user_agent"),
        ip_address: row.get("ip_address"),
    }
}

fn map_order(row: &sqlx::postgres::PgRow) -> OrderRecord {
    let quantity = row.get("quantity");
    let filled_price = row.get("filled_price");
    let market_mode = row.get::<String, _>("market_mode");
    let idempotency_key = row.get::<String, _>("idempotency_key");
    let risk_rationale = row.try_get::<String, _>("risk_rationale").ok();
    let rationale_json = risk_rationale
        .as_deref()
        .and_then(|value| serde_json::from_str::<Value>(value).ok());

    OrderRecord {
        order_id: row.get("id"),
        correlation_id: row.get("correlation_id"),
        risk_decision_id: row.get("risk_decision_id"),
        idempotency_key: idempotency_key.clone(),
        symbol: row.get("symbol"),
        side: row.get("side"),
        quantity,
        limit_price: row.get("limit_price"),
        market_mode: market_mode.clone(),
        status: row.get("status"),
        execution_state: row.get("execution_state"),
        status_reason: row.get("status_reason"),
        filled_price,
        client_order_id: idempotency_key,
        exchange_order_id: None,
        signal_id: row.try_get("signal_id").ok(),
        strategy_id: row.try_get("strategy_id").ok(),
        requested_notional: rationale_json
            .as_ref()
            .and_then(|value| decimal_from_json_field(value, "suggested_notional")),
        filled_qty: if row.get::<String, _>("status") == "FILLED" {
            quantity
        } else {
            Decimal::ZERO
        },
        avg_fill_price: filled_price,
        mode: market_mode,
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

fn map_paper_account(row: &sqlx::postgres::PgRow) -> PaperAccountRecord {
    PaperAccountRecord {
        id: row.get("id"),
        name: row.get("name"),
        base_currency: row.get("base_currency"),
        initial_equity: row.get("initial_equity"),
        current_equity: row.get("current_equity"),
        realized_pnl: row.get("realized_pnl"),
        unrealized_pnl: row.get("unrealized_pnl"),
        status: row.get("status"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn map_paper_position(row: &sqlx::postgres::PgRow) -> PaperPositionRecord {
    PaperPositionRecord {
        id: row.get("id"),
        account_id: row.get("account_id"),
        symbol: row.get("symbol"),
        side: row.get("side"),
        quantity: row.get("quantity"),
        entry_price: row.get("entry_price"),
        mark_price: row.get("mark_price"),
        price_status: row.get("price_status"),
        notional: row.get("notional"),
        realized_pnl: row.get("realized_pnl"),
        unrealized_pnl: row.get("unrealized_pnl"),
        status: row.get("status"),
        opened_at: row.get("opened_at"),
        closed_at: row.get("closed_at"),
        strategy_id: row.get("strategy_id"),
        signal_id: row.get("signal_id"),
        risk_decision_id: row.get("risk_decision_id"),
        order_id: row.get("order_id"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn map_paper_fill(row: &sqlx::postgres::PgRow) -> PaperFillRecord {
    PaperFillRecord {
        id: row.get("id"),
        account_id: row.get("account_id"),
        order_id: row.get("order_id"),
        position_id: row.get("position_id"),
        symbol: row.get("symbol"),
        side: row.get("side"),
        price: row.get("price"),
        quantity: row.get("quantity"),
        notional: row.get("notional"),
        fee: row.get("fee"),
        slippage_cost: row.get("slippage_cost"),
        filled_at: row.get("filled_at"),
        strategy_id: row.get("strategy_id"),
        signal_id: row.get("signal_id"),
        risk_decision_id: row.get("risk_decision_id"),
        correlation_id: row.get("correlation_id"),
        created_at: row.get("created_at"),
    }
}

fn map_paper_equity_snapshot(row: &sqlx::postgres::PgRow) -> PaperEquitySnapshotRecord {
    PaperEquitySnapshotRecord {
        id: row.get("id"),
        account_id: row.get("account_id"),
        equity: row.get("equity"),
        realized_pnl: row.get("realized_pnl"),
        unrealized_pnl: row.get("unrealized_pnl"),
        drawdown_pct: row.get("drawdown_pct"),
        snapshot_at: row.get("snapshot_at"),
    }
}

fn map_paper_trade_journal(row: &sqlx::postgres::PgRow) -> PaperTradeJournalRecord {
    PaperTradeJournalRecord {
        id: row.get("id"),
        account_id: row.get("account_id"),
        position_id: row.get("position_id"),
        order_id: row.get("order_id"),
        event_type: row.get("event_type"),
        symbol: row.get("symbol"),
        pnl: row.get("pnl"),
        payload: row.get("payload"),
        created_at: row.get("created_at"),
        correlation_id: row.get("correlation_id"),
    }
}

fn map_exchange_testnet_order(row: &sqlx::postgres::PgRow) -> ExchangeTestnetOrderRecord {
    ExchangeTestnetOrderRecord {
        id: row.get("id"),
        exchange: row.get("exchange"),
        environment: row.get("environment"),
        client_order_id: row.get("client_order_id"),
        exchange_order_id: row.get("exchange_order_id"),
        symbol: row.get("symbol"),
        side: row.get("side"),
        order_type: row.get("order_type"),
        time_in_force: row.get("time_in_force"),
        requested_qty: row.get("requested_qty"),
        requested_notional: row.get("requested_notional"),
        limit_price: row.get("limit_price"),
        status: row.get("status"),
        execution_state: row.get("execution_state"),
        ack_payload: row.get("ack_payload"),
        latest_status_payload: row.get("latest_status_payload"),
        risk_decision_id: row.get("risk_decision_id"),
        created_by: row.get("created_by"),
        last_transition_at: row.get("last_transition_at"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn map_exchange_testnet_order_lifecycle_event(
    row: &sqlx::postgres::PgRow,
) -> ExchangeTestnetOrderLifecycleEventRecord {
    ExchangeTestnetOrderLifecycleEventRecord {
        id: row.get("id"),
        order_id: row.get("order_id"),
        client_order_id: row.get("client_order_id"),
        previous_state: row.get("previous_state"),
        next_state: row.get("next_state"),
        transition_source: row.get("transition_source"),
        reason: row.get("reason"),
        payload: row.get("payload"),
        created_by: row.get("created_by"),
        created_at: row.get("created_at"),
        correlation_id: row.get("correlation_id"),
    }
}

fn map_exchange_testnet_repair_action(
    row: &sqlx::postgres::PgRow,
) -> ExchangeTestnetRepairActionRecord {
    ExchangeTestnetRepairActionRecord {
        id: row.get("id"),
        client_order_id: row.get("client_order_id"),
        action: row.get("action"),
        status: row.get("status"),
        previous_state: row.get("previous_state"),
        next_state: row.get("next_state"),
        reason: row.get("reason"),
        payload: row.get("payload"),
        actor_id: row.get("actor_id"),
        created_at: row.get("created_at"),
        correlation_id: row.get("correlation_id"),
    }
}

fn map_exchange_private_stream_event(
    row: &sqlx::postgres::PgRow,
) -> ExchangePrivateStreamEventRecord {
    ExchangePrivateStreamEventRecord {
        id: row.get("id"),
        exchange: row.get("exchange"),
        environment: row.get("environment"),
        event_type: row.get("event_type"),
        symbol: row.get("symbol"),
        client_order_id: row.get("client_order_id"),
        exchange_order_id: row.get("exchange_order_id"),
        execution_type: row.get("execution_type"),
        order_status: row.get("order_status"),
        payload: row.get("payload"),
        event_time: row.get("event_time"),
        received_at: row.get("received_at"),
        correlation_id: row.get("correlation_id"),
    }
}

fn map_exchange_private_stream_state(
    row: &sqlx::postgres::PgRow,
) -> ExchangePrivateStreamStateRecord {
    ExchangePrivateStreamStateRecord {
        exchange: row.get("exchange"),
        environment: row.get("environment"),
        status: row.get("status"),
        listen_key_hash: row.get("listen_key_hash"),
        connected_at: row.get("connected_at"),
        last_event_at: row.get("last_event_at"),
        last_error: row.get("last_error"),
        reconnect_count: row.get("reconnect_count"),
        updated_at: row.get("updated_at"),
    }
}

fn map_exchange_reconciliation_run(row: &sqlx::postgres::PgRow) -> ExchangeReconciliationRunRecord {
    ExchangeReconciliationRunRecord {
        id: row.get("id"),
        exchange: row.get("exchange"),
        environment: row.get("environment"),
        status: row.get("status"),
        checked_orders: row.get("checked_orders"),
        matched_orders: row.get("matched_orders"),
        mismatched_orders: row.get("mismatched_orders"),
        unknown_orders: row.get("unknown_orders"),
        failed_reason: row.get("failed_reason"),
        correlation_id: row.get("correlation_id"),
        started_at: row.get("started_at"),
        completed_at: row.get("completed_at"),
    }
}

fn map_exchange_reconciliation_mismatch(
    row: &sqlx::postgres::PgRow,
) -> ExchangeReconciliationMismatchRecord {
    ExchangeReconciliationMismatchRecord {
        id: row.get("id"),
        run_id: row.get("run_id"),
        client_order_id: row.get("client_order_id"),
        local_status: row.get("local_status"),
        exchange_status: row.get("exchange_status"),
        mismatch_kind: row.get("mismatch_kind"),
        action: row.get("action"),
        payload: row.get("payload"),
        created_at: row.get("created_at"),
    }
}

fn map_execution_readiness_snapshot(
    row: &sqlx::postgres::PgRow,
) -> ExecutionReadinessSnapshotRecord {
    ExecutionReadinessSnapshotRecord {
        id: row.get("id"),
        target: row.get("target"),
        status: row.get("status"),
        score: row.get("score"),
        blocking_reasons: row.get("blocking_reasons"),
        warnings: row.get("warnings"),
        checks: row.get("checks"),
        recommendations: row.get("recommendations"),
        created_by: row.get("created_by"),
        created_at: row.get("created_at"),
        correlation_id: row.get("correlation_id"),
    }
}

pub fn execution_readiness_snapshot_from_record(
    record: &ExecutionReadinessSnapshotRecord,
) -> Result<ExecutionReadinessSnapshot> {
    Ok(ExecutionReadinessSnapshot {
        id: record.id,
        target: record.target.parse::<ExecutionReadinessTarget>()?,
        status: match record.status.as_str() {
            "READY" => ExecutionReadinessStatus::Ready,
            "NOT_READY" => ExecutionReadinessStatus::NotReady,
            "DEGRADED" => ExecutionReadinessStatus::Degraded,
            "UNKNOWN" => ExecutionReadinessStatus::Unknown,
            other => {
                return Err(anyhow::anyhow!("unsupported readiness status: {other}").into());
            }
        },
        score: record.score,
        blocking_reasons: serde_json::from_value::<Vec<ExecutionReadinessBlockingReason>>(
            record.blocking_reasons.clone(),
        )?,
        warnings: serde_json::from_value::<Vec<ExecutionReadinessCheck>>(record.warnings.clone())?,
        checks: serde_json::from_value::<Vec<ExecutionReadinessCheck>>(record.checks.clone())?,
        recommendations: serde_json::from_value::<Vec<ExecutionReadinessRecommendation>>(
            record.recommendations.clone(),
        )?,
        created_by: record.created_by,
        created_at: record.created_at,
        correlation_id: record.correlation_id,
    })
}

pub fn paper_account_from_record(record: &PaperAccountRecord) -> Result<PaperAccount> {
    Ok(PaperAccount {
        id: record.id,
        name: record.name.clone(),
        base_currency: record.base_currency.clone(),
        initial_equity: record.initial_equity,
        current_equity: record.current_equity,
        realized_pnl: record.realized_pnl,
        unrealized_pnl: record.unrealized_pnl,
        status: record.status.parse::<PaperAccountStatus>()?,
        created_at: record.created_at,
        updated_at: record.updated_at,
    })
}

pub fn paper_position_from_record(record: &PaperPositionRecord) -> Result<PaperPosition> {
    Ok(PaperPosition {
        id: record.id,
        account_id: record.account_id,
        symbol: record.symbol.clone(),
        side: record.side.parse::<PositionSide>()?,
        quantity: record.quantity,
        entry_price: record.entry_price,
        mark_price: record.mark_price,
        price_status: record.price_status.parse::<PaperPriceStatus>()?,
        notional: record.notional,
        realized_pnl: record.realized_pnl,
        unrealized_pnl: record.unrealized_pnl,
        status: record.status.parse::<PositionStatus>()?,
        opened_at: record.opened_at,
        closed_at: record.closed_at,
        strategy_id: record.strategy_id.clone(),
        signal_id: record.signal_id,
        risk_decision_id: record.risk_decision_id,
        order_id: record.order_id,
        updated_at: record.updated_at,
    })
}

pub fn paper_fill_from_record(record: &PaperFillRecord) -> Result<PaperFill> {
    Ok(PaperFill {
        id: record.id,
        account_id: record.account_id,
        order_id: record.order_id,
        position_id: record.position_id,
        symbol: record.symbol.clone(),
        side: record.side.parse::<PositionSide>()?,
        price: record.price,
        quantity: record.quantity,
        notional: record.notional,
        fee: record.fee,
        slippage_cost: record.slippage_cost,
        filled_at: record.filled_at,
        strategy_id: record.strategy_id.clone(),
        signal_id: record.signal_id,
        risk_decision_id: record.risk_decision_id,
        correlation_id: record.correlation_id,
    })
}

pub fn paper_equity_snapshot_from_record(
    record: &PaperEquitySnapshotRecord,
) -> PaperEquitySnapshot {
    PaperEquitySnapshot {
        id: record.id,
        account_id: record.account_id,
        equity: record.equity,
        realized_pnl: record.realized_pnl,
        unrealized_pnl: record.unrealized_pnl,
        drawdown_pct: record.drawdown_pct,
        snapshot_at: record.snapshot_at,
    }
}

pub fn paper_trade_journal_from_record(record: &PaperTradeJournalRecord) -> PaperTradeJournalEntry {
    PaperTradeJournalEntry {
        id: record.id,
        account_id: record.account_id,
        position_id: record.position_id,
        order_id: record.order_id,
        event_type: record.event_type.clone(),
        symbol: record.symbol.clone(),
        pnl: record.pnl,
        payload: record.payload.clone(),
        created_at: record.created_at,
        correlation_id: record.correlation_id,
    }
}

fn decimal_from_json_field(value: &Value, field: &str) -> Option<Decimal> {
    match value.get(field) {
        Some(Value::String(raw)) => raw.parse::<Decimal>().ok(),
        Some(Value::Number(raw)) => raw.to_string().parse::<Decimal>().ok(),
        _ => None,
    }
}

fn string_array_from_json_field(value: &Value, field: &str) -> Vec<String> {
    value
        .get(field)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(ToOwned::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{
        dedupe_candles_for_upsert, summarize_testnet_promotion_funnel_rows,
        TestnetPromotionFunnelMaterializedRow,
    };
    use aegis_core::{
        Candle, CandleInterval, MarketDataSource, Symbol, TestnetExecutionState,
        TestnetPromotionFunnelRequest,
    };
    use chrono::{TimeZone, Utc};
    use rust_decimal::Decimal;
    use uuid::Uuid;

    fn candle(open_minute: i64, close: i64) -> Candle {
        let open_time = Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap()
            + chrono::Duration::minutes(open_minute);
        Candle {
            id: Uuid::new_v4(),
            exchange: MarketDataSource::Binance,
            symbol: Symbol::new("BTCUSDT").unwrap(),
            interval: CandleInterval::OneMinute,
            open_time,
            close_time: open_time + chrono::Duration::minutes(1)
                - chrono::Duration::milliseconds(1),
            open: Decimal::new(close - 1, 0),
            high: Decimal::new(close + 1, 0),
            low: Decimal::new(close - 2, 0),
            close: Decimal::new(close, 0),
            volume: Decimal::new(10, 0),
            quote_volume: Some(Decimal::new(1_000, 0)),
            trade_count: 1,
            is_closed: true,
            created_at: open_time,
            updated_at: open_time,
        }
    }

    #[test]
    fn dedupe_candles_keeps_latest_entry_per_open_time() {
        let original = candle(0, 100);
        let replacement = candle(0, 105);
        let next = candle(1, 110);

        let deduped = dedupe_candles_for_upsert(&[original, replacement.clone(), next.clone()]);

        assert_eq!(deduped.len(), 2);
        assert_eq!(deduped[0].close, replacement.close);
        assert_eq!(deduped[1].close, next.close);
    }

    #[test]
    fn promotion_funnel_summary_handles_missing_linked_order() {
        let shadow_created_at = Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap();
        let promotion_created_at = shadow_created_at + chrono::Duration::seconds(8);
        let submitted_at = promotion_created_at + chrono::Duration::seconds(4);
        let summary = summarize_testnet_promotion_funnel_rows(
            &TestnetPromotionFunnelRequest {
                strategy_id: Some("momentum_v1".to_string()),
                symbol: Some("BTCUSDT".to_string()),
                timeframe: Some("1m".to_string()),
                start_time: None,
                end_time: None,
                limit: None,
            },
            &[TestnetPromotionFunnelMaterializedRow {
                shadow_run_id: Uuid::new_v4(),
                promotion_id: Some(Uuid::new_v4()),
                strategy_id: "momentum_v1".to_string(),
                symbol: "BTCUSDT".to_string(),
                timeframe: "1m".to_string(),
                promotion_status: Some("SUBMITTED".to_string()),
                promotion_rejection_reasons: Vec::new(),
                testnet_order_id: None,
                client_order_id: Some("client-1".to_string()),
                effective_execution_state: None,
                linked_order_missing: true,
                shadow_created_at,
                promotion_created_at: Some(promotion_created_at),
                submitted_at: Some(submitted_at),
                acked_at: None,
                last_lifecycle_at: None,
            }],
        );

        assert_eq!(summary.shadow_would_submit_count, 1);
        assert_eq!(summary.promotion_previewed_count, 1);
        assert_eq!(summary.promotion_submitted_count, 1);
        assert_eq!(summary.testnet_orders_created_count, 1);
        assert_eq!(summary.acked_count, 0);
        assert_eq!(summary.fill_rate_pct, Decimal::ZERO);
        assert_eq!(
            summary.avg_time_shadow_to_preview_seconds,
            Some(Decimal::from(8)),
        );
        assert_eq!(
            summary.avg_time_preview_to_submit_seconds,
            Some(Decimal::from(4)),
        );
    }

    #[test]
    fn promotion_funnel_summary_counts_lifecycle_outcomes() {
        let created_at = Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap();
        let summary = summarize_testnet_promotion_funnel_rows(
            &TestnetPromotionFunnelRequest {
                strategy_id: None,
                symbol: None,
                timeframe: None,
                start_time: None,
                end_time: None,
                limit: None,
            },
            &[TestnetPromotionFunnelMaterializedRow {
                shadow_run_id: Uuid::new_v4(),
                promotion_id: Some(Uuid::new_v4()),
                strategy_id: "momentum_v1".to_string(),
                symbol: "BTCUSDT".to_string(),
                timeframe: "1m".to_string(),
                promotion_status: Some("SUBMITTED".to_string()),
                promotion_rejection_reasons: Vec::new(),
                testnet_order_id: Some(Uuid::new_v4()),
                client_order_id: Some("client-2".to_string()),
                effective_execution_state: Some(TestnetExecutionState::Filled),
                linked_order_missing: false,
                shadow_created_at: created_at,
                promotion_created_at: Some(created_at + chrono::Duration::seconds(2)),
                submitted_at: Some(created_at + chrono::Duration::seconds(3)),
                acked_at: Some(created_at + chrono::Duration::seconds(4)),
                last_lifecycle_at: Some(created_at + chrono::Duration::seconds(5)),
            }],
        );

        assert_eq!(summary.acked_count, 1);
        assert_eq!(summary.filled_count, 1);
        assert_eq!(
            summary.lifecycle_breakdown[2].execution_state,
            TestnetExecutionState::Filled.as_str()
        );
        assert_eq!(
            summary.fill_rate_pct,
            Decimal::from_str_exact("100.00").expect("valid decimal"),
        );
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
        "actor_id": actor.actor_id,
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
