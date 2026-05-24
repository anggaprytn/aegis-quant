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

impl Default for MarketDataSource {
    fn default() -> Self {
        Self::Binance
    }
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

pub const MIN_PASSWORD_LENGTH: usize = 12;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum UserRole {
    #[serde(rename = "OWNER")]
    Owner,
    #[serde(rename = "OPERATOR")]
    Operator,
    #[serde(rename = "VIEWER")]
    Viewer,
}

impl UserRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "OWNER",
            Self::Operator => "OPERATOR",
            Self::Viewer => "VIEWER",
        }
    }

    pub fn permissions(self) -> &'static [Permission] {
        match self {
            Self::Owner => &[
                Permission::ReadInspection,
                Permission::RunPaperPipeline,
                Permission::RunBacktest,
                Permission::RunBackfill,
                Permission::MarkPaperAccount,
                Permission::ClosePaperPosition,
                Permission::ToggleStrategy,
                Permission::UpdateStrategyConfig,
                Permission::UpdateRiskConfig,
                Permission::ResumeKillSwitch,
                Permission::ManageApiKeysPlaceholder,
            ],
            Self::Operator => &[
                Permission::ReadInspection,
                Permission::RunPaperPipeline,
                Permission::RunBacktest,
                Permission::RunBackfill,
                Permission::MarkPaperAccount,
                Permission::ClosePaperPosition,
                Permission::ToggleStrategy,
            ],
            Self::Viewer => &[Permission::ReadInspection],
        }
    }

    pub fn has_permission(self, permission: Permission) -> bool {
        self.permissions().contains(&permission)
    }
}

impl std::str::FromStr for UserRole {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "OWNER" => Ok(Self::Owner),
            "OPERATOR" => Ok(Self::Operator),
            "VIEWER" => Ok(Self::Viewer),
            other => Err(CoreError::UnsupportedUserRole(other.to_string())),
        }
    }
}

impl std::fmt::Display for UserRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum UserStatus {
    #[serde(rename = "ACTIVE")]
    Active,
    #[serde(rename = "DISABLED")]
    Disabled,
}

impl UserStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Disabled => "DISABLED",
        }
    }
}

impl std::str::FromStr for UserStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "ACTIVE" => Ok(Self::Active),
            "DISABLED" => Ok(Self::Disabled),
            other => Err(CoreError::UnsupportedUserStatus(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    ReadInspection,
    RunPaperPipeline,
    RunBacktest,
    RunBackfill,
    MarkPaperAccount,
    ClosePaperPosition,
    ToggleStrategy,
    UpdateStrategyConfig,
    UpdateRiskConfig,
    ResumeKillSwitch,
    ManageApiKeysPlaceholder,
}

impl Permission {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadInspection => "read_inspection",
            Self::RunPaperPipeline => "run_paper_pipeline",
            Self::RunBacktest => "run_backtest",
            Self::RunBackfill => "run_backfill",
            Self::MarkPaperAccount => "mark_paper_account",
            Self::ClosePaperPosition => "close_paper_position",
            Self::ToggleStrategy => "toggle_strategy",
            Self::UpdateStrategyConfig => "update_strategy_config",
            Self::UpdateRiskConfig => "update_risk_config",
            Self::ResumeKillSwitch => "resume_kill_switch",
            Self::ManageApiKeysPlaceholder => "manage_api_keys_placeholder",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct User {
    pub id: Uuid,
    pub email: String,
    pub role: UserRole,
    pub status: UserStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_login_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Session {
    pub id: Uuid,
    pub user_id: Uuid,
    pub expires_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub user_agent: Option<String>,
    pub ip_address: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthenticatedActor {
    pub user_id: Uuid,
    pub email: String,
    pub role: UserRole,
    pub session_id: Option<Uuid>,
}

impl AuthenticatedActor {
    pub fn has_permission(&self, permission: Permission) -> bool {
        self.role.has_permission(permission)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthLoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthLoginResponse {
    pub user: User,
    pub access_token: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthUserResponse {
    pub user: User,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthLogoutResponse {
    pub logged_out: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthRefreshResponse {
    pub user: User,
    pub access_token: String,
    pub expires_at: DateTime<Utc>,
}

pub fn validate_password_length(password: &str) -> Result<(), CoreError> {
    if password.chars().count() < MIN_PASSWORD_LENGTH {
        return Err(CoreError::PasswordTooShort {
            min_length: MIN_PASSWORD_LENGTH,
        });
    }

    Ok(())
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

    pub fn candles_between(
        self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<i32, CoreError> {
        if end_time <= start_time {
            return Err(CoreError::InvalidCandleBackfillTimeRange);
        }

        let step_ms = self.duration().num_milliseconds();
        let window_ms = end_time
            .signed_duration_since(start_time)
            .num_milliseconds();
        let candles = window_ms / step_ms;
        i32::try_from(candles).map_err(|_| CoreError::InvalidCandleBackfillEstimate)
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
    Paper,
    Research,
    Shadow,
    Live,
}

impl StrategyMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Paper => "paper",
            Self::Research => "research",
            Self::Shadow => "shadow",
            Self::Live => "live",
        }
    }
}

impl std::str::FromStr for StrategyMode {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "paper" => Ok(Self::Paper),
            "research" => Ok(Self::Research),
            "shadow" => Ok(Self::Shadow),
            "live" => Ok(Self::Live),
            "signal_only" => Ok(Self::Paper),
            other => Err(CoreError::UnsupportedStrategyMode(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyConfigValidationSeverity {
    Error,
    Warn,
}

impl StrategyConfigValidationSeverity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "ERROR",
            Self::Warn => "WARN",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StrategyConfigValidationIssue {
    pub severity: StrategyConfigValidationSeverity,
    pub code: String,
    pub field: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyConfigValidationResult {
    pub strategy_id: String,
    pub valid: bool,
    pub issues: Vec<StrategyConfigValidationIssue>,
    pub normalized_config: Option<StrategyConfig>,
    pub validated_at: DateTime<Utc>,
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
    pub enabled: bool,
    pub mode: StrategyMode,
    pub symbols: Vec<Symbol>,
    pub timeframe: CandleInterval,
    pub suggested_notional: Decimal,
    pub max_signal_age_ms: i64,
    pub cooldown_seconds: u32,
    pub lookback_candles: u32,
    pub confidence_floor: Option<Decimal>,
    pub stop_loss_pct: Option<Decimal>,
    pub take_profit_pct: Option<Decimal>,
    pub holding_candles: Option<u32>,
    pub notes: Option<String>,
}

impl StrategyConfig {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.suggested_notional <= Decimal::ZERO {
            return Err(CoreError::InvalidStrategyNotional);
        }

        if self.max_signal_age_ms <= 0 {
            return Err(CoreError::InvalidStrategyMaxSignalAgeMs(
                self.max_signal_age_ms,
            ));
        }

        if self.symbols.is_empty() {
            return Err(CoreError::EmptyStrategySymbols);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyConfigUpdateRequest {
    pub strategy_id: String,
    pub enabled: bool,
    pub mode: StrategyMode,
    pub symbols: Vec<String>,
    pub timeframe: String,
    pub suggested_notional: Decimal,
    pub max_signal_age_ms: i64,
    pub cooldown_seconds: u32,
    pub lookback_candles: u32,
    pub confidence_floor: Option<Decimal>,
    pub stop_loss_pct: Option<Decimal>,
    pub take_profit_pct: Option<Decimal>,
    pub holding_candles: Option<u32>,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyDryRunRequest {
    pub symbol: Option<String>,
    pub timeframe: Option<String>,
    pub config_override: Option<StrategyConfigUpdateRequest>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyDryRunResult {
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub config_valid: bool,
    pub validation_issues: Vec<StrategyConfigValidationIssue>,
    pub would_generate_signal: bool,
    pub reason: String,
    pub source_candle_open_time: Option<DateTime<Utc>>,
    pub confidence: Option<Decimal>,
    pub correlation_id: Uuid,
    pub evaluated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyConfigVersion {
    pub strategy_id: String,
    pub version: i32,
    pub config: StrategyConfig,
    pub actor_id: Option<Uuid>,
    pub correlation_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyConfigAuditEntry {
    pub audit_id: Uuid,
    pub strategy_id: String,
    pub version: Option<i32>,
    pub old_config: Option<StrategyConfig>,
    pub new_config: Option<StrategyConfig>,
    pub validation_issues: Vec<StrategyConfigValidationIssue>,
    pub actor_id: Option<Uuid>,
    pub correlation_id: Uuid,
    pub created_at: DateTime<Utc>,
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
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReplayRunStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl ReplayRunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
        }
    }
}

impl std::str::FromStr for ReplayRunStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "PENDING" => Ok(Self::Pending),
            "RUNNING" => Ok(Self::Running),
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            other => Err(CoreError::UnsupportedReplayRunStatus(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplayMode {
    Backtest,
}

impl ReplayMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Backtest => "backtest",
        }
    }
}

impl std::str::FromStr for ReplayMode {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "backtest" => Ok(Self::Backtest),
            other => Err(CoreError::UnsupportedReplayMode(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FeeModel {
    Bps,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SlippageModel {
    Bps,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BacktestConfig {
    pub replay_mode: ReplayMode,
    pub holding_candles: u32,
    pub fee_model: FeeModel,
    pub slippage_model: SlippageModel,
    pub fee_bps: Decimal,
    pub slippage_bps: Decimal,
    pub risk_config_id: Option<Uuid>,
    pub risk_config: Option<Value>,
}

impl BacktestConfig {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.holding_candles == 0 {
            return Err(CoreError::InvalidHoldingCandles);
        }
        if self.fee_bps < Decimal::ZERO {
            return Err(CoreError::InvalidBacktestBps("fee_bps".to_string()));
        }
        if self.slippage_bps < Decimal::ZERO {
            return Err(CoreError::InvalidBacktestBps("slippage_bps".to_string()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BacktestRequest {
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub initial_capital: Decimal,
    pub risk_config_id: Option<Uuid>,
    pub risk_config: Option<Value>,
    pub fee_bps: Decimal,
    pub slippage_bps: Decimal,
    pub correlation_id: Option<Uuid>,
    pub holding_candles: Option<u32>,
    pub strategy_config_override: Option<StrategyConfigUpdateRequest>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CandleBackfillStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl CandleBackfillStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
        }
    }
}

impl std::str::FromStr for CandleBackfillStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "PENDING" => Ok(Self::Pending),
            "RUNNING" => Ok(Self::Running),
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            other => Err(CoreError::UnsupportedCandleBackfillStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CandleBackfillSource {
    BinanceRestPublic,
}

impl CandleBackfillSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BinanceRestPublic => "binance_rest_public",
        }
    }
}

impl std::str::FromStr for CandleBackfillSource {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "binance_rest_public" => Ok(Self::BinanceRestPublic),
            other => Err(CoreError::UnsupportedCandleBackfillSource(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CandleBackfillRequest {
    #[serde(default)]
    pub exchange: MarketDataSource,
    pub symbol: String,
    pub interval: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub limit_per_request: Option<u16>,
    pub correlation_id: Option<Uuid>,
}

impl CandleBackfillRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.symbol.trim().is_empty() {
            return Err(CoreError::EmptyCandleBackfillSymbol);
        }
        if self.interval.trim().is_empty() {
            return Err(CoreError::EmptyCandleBackfillInterval);
        }
        let interval: CandleInterval = self.interval.parse()?;
        let limit = self.limit_per_request.unwrap_or(1000);
        if limit == 0 {
            return Err(CoreError::InvalidCandleBackfillLimit);
        }
        if limit > 1000 {
            return Err(CoreError::CandleBackfillLimitTooHigh(limit));
        }
        interval.candles_between(self.start_time, self.end_time)?;
        Ok(())
    }

    pub fn normalized_symbol(&self) -> Result<Symbol, CoreError> {
        Symbol::new(self.symbol.clone())
    }

    pub fn parsed_interval(&self) -> Result<CandleInterval, CoreError> {
        self.interval.parse()
    }

    pub fn effective_limit_per_request(&self) -> u16 {
        self.limit_per_request.unwrap_or(1000)
    }

    pub fn requested_candles_estimate(&self) -> Result<i32, CoreError> {
        self.parsed_interval()?
            .candles_between(self.start_time, self.end_time)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CandleBackfillProgress {
    pub run_id: Uuid,
    pub page: i32,
    pub request_start_time: DateTime<Utc>,
    pub request_end_time: DateTime<Utc>,
    pub fetched_candles: i32,
    pub inserted_candles: i32,
    pub updated_candles: i32,
    pub skipped_candles: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BackfilledCandleSummary {
    pub exchange: MarketDataSource,
    pub symbol: String,
    pub interval: String,
    pub first_open_time: Option<DateTime<Utc>>,
    pub last_open_time: Option<DateTime<Utc>>,
    pub candle_count: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CandleBackfillResult {
    pub run_id: Uuid,
    pub exchange: MarketDataSource,
    pub symbol: String,
    pub interval: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub status: CandleBackfillStatus,
    pub requested_candles_estimate: i32,
    pub fetched_candles: i32,
    pub inserted_candles: i32,
    pub updated_candles: i32,
    pub skipped_candles: i32,
    pub failed_reason: Option<String>,
    pub correlation_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl BacktestRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.strategy_id.trim().is_empty() {
            return Err(CoreError::EmptyBacktestStrategyId);
        }
        if self.symbol.trim().is_empty() {
            return Err(CoreError::EmptyBacktestSymbol);
        }
        if self.timeframe.trim().is_empty() {
            return Err(CoreError::EmptyBacktestTimeframe);
        }
        if self.end_time <= self.start_time {
            return Err(CoreError::InvalidBacktestTimeRange);
        }
        if self.initial_capital <= Decimal::ZERO {
            return Err(CoreError::InvalidBacktestInitialCapital);
        }

        self.config().validate()?;
        Ok(())
    }

    pub fn config(&self) -> BacktestConfig {
        BacktestConfig {
            replay_mode: ReplayMode::Backtest,
            holding_candles: self.holding_candles.unwrap_or(3),
            fee_model: FeeModel::Bps,
            slippage_model: SlippageModel::Bps,
            fee_bps: self.fee_bps,
            slippage_bps: self.slippage_bps,
            risk_config_id: self.risk_config_id,
            risk_config: self.risk_config.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BacktestTrade {
    pub id: Uuid,
    pub run_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub side: Side,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BacktestPosition {
    pub side: Side,
    pub entry_time: DateTime<Utc>,
    pub entry_price: Decimal,
    pub quantity: Decimal,
    pub notional: Decimal,
    pub fee_paid: Decimal,
    pub slippage_cost: Decimal,
    pub remaining_holding_candles: u32,
    pub stop_loss_price: Option<Decimal>,
    pub take_profit_price: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BacktestEquityPoint {
    pub id: Uuid,
    pub run_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub equity: Decimal,
    pub drawdown_pct: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BacktestMetricSummary {
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BacktestResult {
    pub run_id: Uuid,
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
    pub status: ReplayRunStatus,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
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
    MaxWeeklyLossExceeded,
    MaxConsecutiveLossesExceeded,
    SignalTooOld,
    DuplicateOrderDetected,
    DataStale,
    PositionNotionalExceeded,
    CooldownActive,
    UnsupportedState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RiskRuleResult {
    pub rule_name: String,
    pub decision: RiskRuleDecision,
    pub reason: Option<RiskRejectionReason>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RiskConfig {
    pub max_open_positions: u32,
    pub max_daily_loss_pct: Decimal,
    pub max_weekly_loss_pct: Decimal,
    pub max_position_notional: Decimal,
    pub max_slippage_pct: Decimal,
    pub max_consecutive_losses: u32,
    pub cooldown_seconds: u32,
    pub max_signal_age_ms: i64,
    pub stale_feed_threshold_seconds: u32,
}

impl RiskConfig {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.max_open_positions == 0 {
            return Err(CoreError::InvalidRiskConfig(
                "max_open_positions must be greater than zero".to_string(),
            ));
        }
        if self.max_daily_loss_pct <= Decimal::ZERO {
            return Err(CoreError::InvalidRiskConfig(
                "max_daily_loss_pct must be greater than zero".to_string(),
            ));
        }
        if self.max_weekly_loss_pct <= Decimal::ZERO {
            return Err(CoreError::InvalidRiskConfig(
                "max_weekly_loss_pct must be greater than zero".to_string(),
            ));
        }
        if self.max_position_notional <= Decimal::ZERO {
            return Err(CoreError::InvalidRiskConfig(
                "max_position_notional must be greater than zero".to_string(),
            ));
        }
        if self.max_slippage_pct < Decimal::ZERO {
            return Err(CoreError::InvalidRiskConfig(
                "max_slippage_pct cannot be negative".to_string(),
            ));
        }
        if self.max_consecutive_losses == 0 {
            return Err(CoreError::InvalidRiskConfig(
                "max_consecutive_losses must be greater than zero".to_string(),
            ));
        }
        if self.max_signal_age_ms <= 0 {
            return Err(CoreError::InvalidRiskConfig(
                "max_signal_age_ms must be greater than zero".to_string(),
            ));
        }
        if self.stale_feed_threshold_seconds == 0 {
            return Err(CoreError::InvalidRiskConfig(
                "stale_feed_threshold_seconds must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RiskConfigValidationIssue {
    pub severity: StrategyConfigValidationSeverity,
    pub code: String,
    pub field: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RiskConfigValidationResult {
    pub valid: bool,
    pub issues: Vec<RiskConfigValidationIssue>,
    pub normalized_config: Option<RiskConfig>,
    pub validated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RiskConfigVersion {
    pub config_id: Uuid,
    pub version: i32,
    pub config: RiskConfig,
    pub actor_id: Option<Uuid>,
    pub correlation_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RiskConfigAuditEntry {
    pub audit_id: Uuid,
    pub config_id: Uuid,
    pub version: Option<i32>,
    pub old_config: Option<RiskConfig>,
    pub new_config: Option<RiskConfig>,
    pub validation_issues: Vec<RiskConfigValidationIssue>,
    pub actor_id: Option<Uuid>,
    pub correlation_id: Uuid,
    pub created_at: DateTime<Utc>,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_open_positions: 2,
            max_daily_loss_pct: Decimal::new(2, 0),
            max_weekly_loss_pct: Decimal::new(5, 0),
            max_position_notional: Decimal::new(150_000, 0),
            max_slippage_pct: Decimal::new(1, 0),
            max_consecutive_losses: 3,
            cooldown_seconds: 900,
            max_signal_age_ms: 5_000,
            stale_feed_threshold_seconds: 10,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PaperAccountStatus {
    Active,
    Disabled,
}

impl PaperAccountStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Disabled => "disabled",
        }
    }
}

impl std::str::FromStr for PaperAccountStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "active" => Ok(Self::Active),
            "disabled" => Ok(Self::Disabled),
            other => Err(CoreError::UnsupportedPaperAccountStatus(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PositionSide {
    Long,
}

impl PositionSide {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Long => "long",
        }
    }
}

impl std::str::FromStr for PositionSide {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "long" => Ok(Self::Long),
            other => Err(CoreError::UnsupportedPositionSide(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PositionStatus {
    Open,
    Closed,
}

impl PositionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Closed => "closed",
        }
    }
}

impl std::str::FromStr for PositionStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "open" => Ok(Self::Open),
            "closed" => Ok(Self::Closed),
            other => Err(CoreError::UnsupportedPositionStatus(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PnlCalculationMode {
    WeightedAverage,
}

impl PnlCalculationMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WeightedAverage => "weighted_average",
        }
    }
}

impl std::str::FromStr for PnlCalculationMode {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "weighted_average" => Ok(Self::WeightedAverage),
            other => Err(CoreError::UnsupportedPnlCalculationMode(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PaperPriceStatus {
    Live,
    Stale,
    Missing,
}

impl PaperPriceStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Stale => "stale",
            Self::Missing => "missing",
        }
    }
}

impl std::str::FromStr for PaperPriceStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "live" => Ok(Self::Live),
            "stale" => Ok(Self::Stale),
            "missing" => Ok(Self::Missing),
            other => Err(CoreError::UnsupportedPaperPriceStatus(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PaperCloseMode {
    MarketSimulated,
}

impl Default for PaperCloseMode {
    fn default() -> Self {
        Self::MarketSimulated
    }
}

impl PaperCloseMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MarketSimulated => "market_simulated",
        }
    }
}

impl std::str::FromStr for PaperCloseMode {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "market_simulated" => Ok(Self::MarketSimulated),
            other => Err(CoreError::UnsupportedPaperCloseMode(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PaperCloseReason {
    ManualOperatorExit,
    RiskOperatorExit,
    EmergencyExit,
}

impl PaperCloseReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ManualOperatorExit => "manual_operator_exit",
            Self::RiskOperatorExit => "risk_operator_exit",
            Self::EmergencyExit => "emergency_exit",
        }
    }
}

impl std::str::FromStr for PaperCloseReason {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "manual_operator_exit" => Ok(Self::ManualOperatorExit),
            "risk_operator_exit" => Ok(Self::RiskOperatorExit),
            "emergency_exit" => Ok(Self::EmergencyExit),
            other => Err(CoreError::UnsupportedPaperCloseReason(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PaperCloseValidationIssue {
    PositionNotFound,
    AccountNotFound,
    PositionNotOpen,
    WrongConfirmationText,
    MissingMarketPrice,
    StaleMarketPrice,
    AlreadyClosed,
    UnsupportedCloseMode,
}

impl PaperCloseValidationIssue {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PositionNotFound => "position_not_found",
            Self::AccountNotFound => "account_not_found",
            Self::PositionNotOpen => "position_not_open",
            Self::WrongConfirmationText => "wrong_confirmation_text",
            Self::MissingMarketPrice => "missing_market_price",
            Self::StaleMarketPrice => "stale_market_price",
            Self::AlreadyClosed => "already_closed",
            Self::UnsupportedCloseMode => "unsupported_close_mode",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PaperCloseStatus {
    Closed,
    AlreadyClosed,
}

impl PaperCloseStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::AlreadyClosed => "already_closed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PaperClosePositionRequest {
    pub position_id: Uuid,
    pub confirmation_text: String,
    pub reason: Option<PaperCloseReason>,
    #[serde(default)]
    pub close_mode: PaperCloseMode,
    pub correlation_id: Option<Uuid>,
    #[serde(default)]
    pub allow_stale_price: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PaperPositionCloseSummary {
    pub status: PaperCloseStatus,
    pub position_id: Uuid,
    pub account_id: Uuid,
    pub symbol: String,
    pub quantity: Decimal,
    pub entry_price: Decimal,
    pub exit_price: Decimal,
    pub realized_pnl: Decimal,
    pub fee: Decimal,
    pub slippage_cost: Decimal,
    pub closed_at: DateTime<Utc>,
    pub correlation_id: Uuid,
    pub journal_entry_id: Uuid,
    pub close_fill_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PaperClosePositionResult {
    pub position_id: Uuid,
    pub account_id: Uuid,
    pub symbol: String,
    pub quantity: Decimal,
    pub entry_price: Decimal,
    pub exit_price: Decimal,
    pub realized_pnl: Decimal,
    pub fee: Decimal,
    pub slippage_cost: Decimal,
    pub closed_at: DateTime<Utc>,
    pub correlation_id: Uuid,
    pub journal_entry_id: Uuid,
    pub close_fill_id: Uuid,
    pub summary: PaperPositionCloseSummary,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PaperPositionStatusFilter {
    Open,
    Closed,
    All,
}

impl PaperPositionStatusFilter {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Closed => "closed",
            Self::All => "all",
        }
    }
}

impl std::str::FromStr for PaperPositionStatusFilter {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "open" => Ok(Self::Open),
            "closed" => Ok(Self::Closed),
            "all" => Ok(Self::All),
            other => Err(CoreError::UnsupportedPaperPositionStatusFilter(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PaperAccount {
    pub id: Uuid,
    pub name: String,
    pub base_currency: String,
    pub initial_equity: Decimal,
    pub current_equity: Decimal,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub status: PaperAccountStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PaperPosition {
    pub id: Uuid,
    pub account_id: Uuid,
    pub symbol: String,
    pub side: PositionSide,
    pub quantity: Decimal,
    pub entry_price: Decimal,
    pub mark_price: Option<Decimal>,
    pub price_status: PaperPriceStatus,
    pub notional: Decimal,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub status: PositionStatus,
    pub opened_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    pub strategy_id: Option<String>,
    pub signal_id: Option<Uuid>,
    pub risk_decision_id: Option<Uuid>,
    pub order_id: Option<Uuid>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PaperFill {
    pub id: Uuid,
    pub account_id: Uuid,
    pub order_id: Uuid,
    pub position_id: Option<Uuid>,
    pub symbol: String,
    pub side: PositionSide,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PaperEquitySnapshot {
    pub id: Uuid,
    pub account_id: Uuid,
    pub equity: Decimal,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub drawdown_pct: Decimal,
    pub snapshot_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PaperPnlSummary {
    pub account_id: Uuid,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub equity: Decimal,
    pub daily_pnl: Decimal,
    pub drawdown_pct: Decimal,
    pub price_status: PaperPriceStatus,
    pub calculated_at: DateTime<Utc>,
    pub peak_equity: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PaperTradeJournalEntry {
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
    #[error("unsupported replay run status: {0}")]
    UnsupportedReplayRunStatus(String),
    #[error("unsupported replay mode: {0}")]
    UnsupportedReplayMode(String),
    #[error("unsupported paper account status: {0}")]
    UnsupportedPaperAccountStatus(String),
    #[error("unsupported position side: {0}")]
    UnsupportedPositionSide(String),
    #[error("unsupported position status: {0}")]
    UnsupportedPositionStatus(String),
    #[error("unsupported pnl calculation mode: {0}")]
    UnsupportedPnlCalculationMode(String),
    #[error("unsupported paper price status: {0}")]
    UnsupportedPaperPriceStatus(String),
    #[error("unsupported paper close mode: {0}")]
    UnsupportedPaperCloseMode(String),
    #[error("unsupported paper close reason: {0}")]
    UnsupportedPaperCloseReason(String),
    #[error("unsupported paper position status filter: {0}")]
    UnsupportedPaperPositionStatusFilter(String),
    #[error("unsupported candle backfill status: {0}")]
    UnsupportedCandleBackfillStatus(String),
    #[error("unsupported candle backfill source: {0}")]
    UnsupportedCandleBackfillSource(String),
    #[error("unsupported user role: {0}")]
    UnsupportedUserRole(String),
    #[error("unsupported user status: {0}")]
    UnsupportedUserStatus(String),
    #[error("idempotency_key cannot be empty")]
    EmptyIdempotencyKey,
    #[error("backtest strategy_id cannot be empty")]
    EmptyBacktestStrategyId,
    #[error("backtest symbol cannot be empty")]
    EmptyBacktestSymbol,
    #[error("backtest timeframe cannot be empty")]
    EmptyBacktestTimeframe,
    #[error("candle backfill symbol cannot be empty")]
    EmptyCandleBackfillSymbol,
    #[error("candle backfill interval cannot be empty")]
    EmptyCandleBackfillInterval,
    #[error("backtest end_time must be after start_time")]
    InvalidBacktestTimeRange,
    #[error("candle backfill end_time must be after start_time")]
    InvalidCandleBackfillTimeRange,
    #[error("backtest initial_capital must be greater than zero")]
    InvalidBacktestInitialCapital,
    #[error("candle backfill request limit must be greater than zero")]
    InvalidCandleBackfillLimit,
    #[error("candle backfill request limit exceeds Binance maximum: {0}")]
    CandleBackfillLimitTooHigh(u16),
    #[error("candle backfill estimate exceeds supported bounds")]
    InvalidCandleBackfillEstimate,
    #[error("holding_candles must be greater than zero")]
    InvalidHoldingCandles,
    #[error("invalid backtest bps field: {0}")]
    InvalidBacktestBps(String),
    #[error("password must be at least {min_length} characters")]
    PasswordTooShort { min_length: usize },
    #[error("quantity must be greater than zero")]
    InvalidOrderQuantity,
    #[error("limit_price must be greater than zero")]
    InvalidLimitPrice,
    #[error("strategy suggested notional must be greater than zero")]
    InvalidStrategyNotional,
    #[error("strategy max_signal_age_ms must be greater than zero: {0}")]
    InvalidStrategyMaxSignalAgeMs(i64),
    #[error("invalid risk config: {0}")]
    InvalidRiskConfig(String),
    #[error("strategy symbols cannot be empty")]
    EmptyStrategySymbols,
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
    use super::{
        validate_password_length, ExecutionState, OrderIntent, PaperOrder, Permission, Side,
        Symbol, UserRole,
    };
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

    #[test]
    fn viewer_permissions_are_read_only() {
        assert!(UserRole::Viewer.has_permission(Permission::ReadInspection));
        assert!(!UserRole::Viewer.has_permission(Permission::RunPaperPipeline));
        assert!(!UserRole::Viewer.has_permission(Permission::UpdateRiskConfig));
    }

    #[test]
    fn owner_permissions_include_owner_actions() {
        assert!(UserRole::Owner.has_permission(Permission::UpdateStrategyConfig));
        assert!(UserRole::Owner.has_permission(Permission::UpdateRiskConfig));
        assert!(UserRole::Owner.has_permission(Permission::ResumeKillSwitch));
    }

    #[test]
    fn password_min_length_is_enforced() {
        assert!(validate_password_length("123456789012").is_ok());
        assert!(validate_password_length("too-short").is_err());
    }
}
