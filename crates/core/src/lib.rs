use chrono::{DateTime, Duration, TimeZone, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

mod research;

pub use research::*;

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
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
    FiveMinutes,
    FifteenMinutes,
    OneHour,
}

impl CandleInterval {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OneMinute => "1m",
            Self::FiveMinutes => "5m",
            Self::FifteenMinutes => "15m",
            Self::OneHour => "1h",
        }
    }

    pub fn duration(self) -> Duration {
        match self {
            Self::OneMinute => Duration::minutes(1),
            Self::FiveMinutes => Duration::minutes(5),
            Self::FifteenMinutes => Duration::minutes(15),
            Self::OneHour => Duration::hours(1),
        }
    }

    pub fn source_candle_count(self) -> i64 {
        match self {
            Self::OneMinute => 1,
            Self::FiveMinutes => 5,
            Self::FifteenMinutes => 15,
            Self::OneHour => 60,
        }
    }

    pub fn bucket_start(self, timestamp: DateTime<Utc>) -> DateTime<Utc> {
        let step_seconds = self.duration().num_seconds();
        let aligned_seconds =
            timestamp.timestamp() - timestamp.timestamp().rem_euclid(step_seconds);
        Utc.timestamp_opt(aligned_seconds, 0)
            .single()
            .expect("aligned candle bucket timestamp must be valid")
    }

    pub fn bucket_close_time(self, bucket_start: DateTime<Utc>) -> DateTime<Utc> {
        bucket_start + self.duration() - Duration::milliseconds(1)
    }

    pub fn is_aggregated_from_one_minute(self) -> bool {
        !matches!(self, Self::OneMinute)
    }

    pub fn recommended_max_signal_age_ms(self) -> (i64, i64) {
        match self {
            Self::OneMinute => (120_000, 300_000),
            Self::FiveMinutes => (600_000, 900_000),
            Self::FifteenMinutes => (1_800_000, 2_700_000),
            Self::OneHour => (7_200_000, 10_800_000),
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
            "5m" => Ok(Self::FiveMinutes),
            "15m" => Ok(Self::FifteenMinutes),
            "1h" => Ok(Self::OneHour),
            other => Err(CoreError::UnsupportedCandleInterval(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CandleAggregationRequest {
    #[serde(default)]
    pub exchange: MarketDataSource,
    pub symbol: String,
    pub source_interval: String,
    pub target_interval: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

impl CandleAggregationRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.symbol.trim().is_empty() {
            return Err(CoreError::EmptyCandleBackfillSymbol);
        }
        let source_interval = self.source_interval.parse::<CandleInterval>()?;
        let target_interval = self.target_interval.parse::<CandleInterval>()?;
        if source_interval != CandleInterval::OneMinute
            || !target_interval.is_aggregated_from_one_minute()
        {
            return Err(CoreError::UnsupportedCandleInterval(
                self.target_interval.clone(),
            ));
        }
        target_interval.candles_between(self.start_time, self.end_time)?;
        Ok(())
    }

    pub fn normalized_symbol(&self) -> Result<Symbol, CoreError> {
        Symbol::new(self.symbol.clone())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CandleAggregationResult {
    pub exchange: MarketDataSource,
    pub symbol: String,
    pub source_interval: String,
    pub target_interval: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub source_candles: i32,
    pub aggregated_candles: i32,
    pub inserted: i32,
    pub updated: i32,
    pub skipped_incomplete: i32,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketCandleIntervalCoverage {
    pub interval: String,
    pub candle_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketCandleCoverageSummary {
    pub exchange: MarketDataSource,
    pub symbol: String,
    pub intervals: Vec<MarketCandleIntervalCoverage>,
}

fn default_market_data_quality_exchange() -> MarketDataSource {
    MarketDataSource::Binance
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketDataQualityRequest {
    #[serde(default = "default_market_data_quality_exchange")]
    pub exchange: MarketDataSource,
    pub symbol: String,
    pub interval: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub expected_interval_seconds: Option<i64>,
    pub max_allowed_gap_count: Option<i64>,
    pub max_allowed_gap_pct: Option<Decimal>,
}

impl MarketDataQualityRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.symbol.trim().is_empty() {
            return Err(CoreError::EmptyCandleBackfillSymbol);
        }
        self.parsed_interval()?;
        if self.end_time <= self.start_time {
            return Err(CoreError::InvalidCandleBackfillTimeRange);
        }
        if let Some(seconds) = self.expected_interval_seconds {
            if seconds <= 0 {
                return Err(CoreError::InvalidCandleBackfillTimeRange);
            }
        }
        Ok(())
    }

    pub fn normalized_symbol(&self) -> Result<Symbol, CoreError> {
        Symbol::new(self.symbol.clone())
    }

    pub fn parsed_interval(&self) -> Result<CandleInterval, CoreError> {
        self.interval.parse::<CandleInterval>()
    }

    pub fn expected_interval_seconds(&self) -> Result<i64, CoreError> {
        Ok(self.expected_interval_seconds.unwrap_or_else(|| {
            self.parsed_interval()
                .map_or(0, |interval| interval.duration().num_seconds())
        }))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketDataQualityStatus {
    Good,
    Degraded,
    Bad,
    InsufficientData,
    Unknown,
}

impl MarketDataQualityStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Good => "GOOD",
            Self::Degraded => "DEGRADED",
            Self::Bad => "BAD",
            Self::InsufficientData => "INSUFFICIENT_DATA",
            Self::Unknown => "UNKNOWN",
        }
    }
}

impl std::str::FromStr for MarketDataQualityStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "GOOD" => Ok(Self::Good),
            "DEGRADED" => Ok(Self::Degraded),
            "BAD" => Ok(Self::Bad),
            "INSUFFICIENT_DATA" => Ok(Self::InsufficientData),
            "UNKNOWN" => Ok(Self::Unknown),
            other => Err(CoreError::UnsupportedMarketDataQualityStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketDataGap {
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub missing_candle_count: i64,
    pub gap_seconds: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketDataCoverageSummary {
    pub expected_candle_count: i64,
    pub actual_candle_count: i64,
    pub closed_candle_count: i64,
    pub open_candle_count: i64,
    pub missing_candle_count: i64,
    pub coverage_pct: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketDataQualityFinding {
    pub severity: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketDataQualityRecommendation {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketDataQualityReport {
    pub exchange: MarketDataSource,
    pub symbol: String,
    pub interval: String,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub expected_candle_count: i64,
    pub actual_candle_count: i64,
    pub closed_candle_count: i64,
    pub open_candle_count: i64,
    pub missing_candle_count: i64,
    pub coverage_pct: Decimal,
    pub gap_count: i64,
    pub largest_gap_seconds: i64,
    pub gaps: Vec<MarketDataGap>,
    pub first_candle_time: Option<DateTime<Utc>>,
    pub last_candle_time: Option<DateTime<Utc>>,
    pub status: MarketDataQualityStatus,
    pub findings: Vec<MarketDataQualityFinding>,
    pub recommendations: Vec<MarketDataQualityRecommendation>,
}

fn default_market_data_repair_max_ranges() -> i32 {
    100
}

fn default_market_data_repair_reaggregate() -> bool {
    true
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketDataRepairMode {
    PlanOnly,
    Repair,
}

impl MarketDataRepairMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PlanOnly => "PLAN_ONLY",
            Self::Repair => "REPAIR",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketDataRepairStatus {
    NoRepairNeeded,
    RepairPlanned,
    RepairCompleted,
    PartialRepair,
    RepairFailed,
    InsufficientData,
    UnsupportedInterval,
}

impl MarketDataRepairStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoRepairNeeded => "NO_REPAIR_NEEDED",
            Self::RepairPlanned => "REPAIR_PLANNED",
            Self::RepairCompleted => "REPAIR_COMPLETED",
            Self::PartialRepair => "PARTIAL_REPAIR",
            Self::RepairFailed => "REPAIR_FAILED",
            Self::InsufficientData => "INSUFFICIENT_DATA",
            Self::UnsupportedInterval => "UNSUPPORTED_INTERVAL",
        }
    }
}

impl std::str::FromStr for MarketDataRepairStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "NO_REPAIR_NEEDED" => Ok(Self::NoRepairNeeded),
            "REPAIR_PLANNED" => Ok(Self::RepairPlanned),
            "REPAIR_COMPLETED" => Ok(Self::RepairCompleted),
            "PARTIAL_REPAIR" => Ok(Self::PartialRepair),
            "REPAIR_FAILED" => Ok(Self::RepairFailed),
            "INSUFFICIENT_DATA" => Ok(Self::InsufficientData),
            "UNSUPPORTED_INTERVAL" => Ok(Self::UnsupportedInterval),
            other => Err(CoreError::UnsupportedMarketDataRepairStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketDataRepairPlanRequest {
    #[serde(default = "default_market_data_quality_exchange")]
    pub exchange: MarketDataSource,
    pub symbol: String,
    pub interval: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    #[serde(default = "default_market_data_repair_mode")]
    pub repair_mode: MarketDataRepairMode,
    #[serde(default = "default_market_data_repair_max_ranges")]
    pub max_ranges: i32,
    #[serde(default = "default_market_data_repair_reaggregate")]
    pub reaggregate_derived_intervals: bool,
    pub correlation_id: Option<Uuid>,
}

fn default_market_data_repair_mode() -> MarketDataRepairMode {
    MarketDataRepairMode::PlanOnly
}

impl MarketDataRepairPlanRequest {
    pub fn validate_without_interval_support(&self) -> Result<(), CoreError> {
        if self.symbol.trim().is_empty() {
            return Err(CoreError::EmptyCandleBackfillSymbol);
        }
        if self.end_time <= self.start_time {
            return Err(CoreError::InvalidCandleBackfillTimeRange);
        }
        if self.max_ranges <= 0 {
            return Err(CoreError::InvalidMarketDataRepairMaxRanges);
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<(), CoreError> {
        self.validate_without_interval_support()?;
        self.parsed_interval()?;
        Ok(())
    }

    pub fn normalized_symbol(&self) -> Result<Symbol, CoreError> {
        Symbol::new(self.symbol.clone())
    }

    pub fn parsed_interval(&self) -> Result<CandleInterval, CoreError> {
        self.interval.parse::<CandleInterval>()
    }

    pub fn quality_request(&self) -> MarketDataQualityRequest {
        MarketDataQualityRequest {
            exchange: self.exchange,
            symbol: self.symbol.clone(),
            interval: self.interval.clone(),
            start_time: self.start_time,
            end_time: self.end_time,
            expected_interval_seconds: None,
            max_allowed_gap_count: None,
            max_allowed_gap_pct: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketDataRepairRunRequest {
    pub plan: MarketDataRepairPlanRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketDataRepairRange {
    pub source_interval: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub missing_candle_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketDataRepairFinding {
    pub severity: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketDataRepairRecommendation {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketDataRepairPlan {
    pub exchange: MarketDataSource,
    pub symbol: String,
    pub interval: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub status: MarketDataRepairStatus,
    pub initial_quality_status: MarketDataQualityStatus,
    pub gap_count: i64,
    pub repair_ranges: Vec<MarketDataRepairRange>,
    pub estimated_source_interval: Option<String>,
    pub requires_source_interval: bool,
    pub reaggregate_derived_intervals: bool,
    pub findings: Vec<MarketDataRepairFinding>,
    pub recommendations: Vec<MarketDataRepairRecommendation>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketDataRepairRunResult {
    pub run_id: Uuid,
    pub plan: MarketDataRepairPlan,
    pub status: MarketDataRepairStatus,
    pub before_quality_status: MarketDataQualityStatus,
    pub after_quality_status: MarketDataQualityStatus,
    pub gap_count_before: i64,
    pub gap_count_after: i64,
    pub attempted_ranges: Vec<MarketDataRepairRange>,
    pub inserted_candles: i32,
    pub updated_candles: i32,
    pub skipped_candles: i32,
    pub failed_ranges: i32,
    pub provider_attempts: Vec<MarketProviderAttempt>,
    pub selected_provider: Option<String>,
    pub aggregation_result: Option<CandleAggregationResult>,
    pub recommendations: Vec<MarketDataRepairRecommendation>,
    pub correlation_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

pub fn plan_market_data_repair(
    request: &MarketDataRepairPlanRequest,
    quality_report: &MarketDataQualityReport,
) -> Result<MarketDataRepairPlan, CoreError> {
    request.validate_without_interval_support()?;
    let symbol = request.normalized_symbol()?;
    let interval = match request.parsed_interval() {
        Ok(interval) => interval,
        Err(_) => {
            return Ok(MarketDataRepairPlan {
                exchange: request.exchange,
                symbol: symbol.as_str().to_string(),
                interval: request.interval.clone(),
                start_time: request.start_time,
                end_time: request.end_time,
                status: MarketDataRepairStatus::UnsupportedInterval,
                initial_quality_status: quality_report.status,
                gap_count: quality_report.gap_count,
                repair_ranges: Vec::new(),
                estimated_source_interval: None,
                requires_source_interval: false,
                reaggregate_derived_intervals: false,
                findings: vec![MarketDataRepairFinding {
                    severity: "MEDIUM".to_string(),
                    code: "unsupported_interval".to_string(),
                    message: format!("Interval {} is not supported for repair.", request.interval),
                }],
                recommendations: vec![MarketDataRepairRecommendation {
                    code: "use_supported_interval".to_string(),
                    message: "Use one of 1m, 5m, 15m, or 1h.".to_string(),
                }],
                correlation_id: request.correlation_id,
            });
        }
    };

    let requires_source_interval = interval.is_aggregated_from_one_minute();
    let estimated_source_interval = requires_source_interval
        .then(|| CandleInterval::OneMinute.as_str().to_string())
        .or_else(|| Some(interval.as_str().to_string()));

    if quality_report.status == MarketDataQualityStatus::InsufficientData {
        return Ok(MarketDataRepairPlan {
            exchange: request.exchange,
            symbol: symbol.as_str().to_string(),
            interval: interval.as_str().to_string(),
            start_time: request.start_time,
            end_time: request.end_time,
            status: MarketDataRepairStatus::InsufficientData,
            initial_quality_status: quality_report.status,
            gap_count: quality_report.gap_count,
            repair_ranges: vec![MarketDataRepairRange {
                source_interval: estimated_source_interval
                    .clone()
                    .unwrap_or_else(|| "1m".to_string()),
                start_time: request.start_time,
                end_time: request.end_time,
                missing_candle_count: CandleInterval::OneMinute
                    .candles_between(request.start_time, request.end_time)
                    .unwrap_or(0) as i64,
            }],
            estimated_source_interval,
            requires_source_interval,
            reaggregate_derived_intervals: requires_source_interval
                && request.reaggregate_derived_intervals,
            findings: vec![MarketDataRepairFinding {
                severity: "HIGH".to_string(),
                code: "insufficient_market_data".to_string(),
                message: "No candles exist for the requested repair window.".to_string(),
            }],
            recommendations: vec![MarketDataRepairRecommendation {
                code: "backfill_full_window".to_string(),
                message: "Backfill the requested window before using it for research.".to_string(),
            }],
            correlation_id: request.correlation_id,
        });
    }

    let mut repair_ranges = merge_repair_gaps(
        &quality_report.gaps,
        if requires_source_interval {
            CandleInterval::OneMinute
        } else {
            interval
        },
        request.max_ranges as usize,
    );

    let mut findings = Vec::new();
    let mut recommendations = Vec::new();
    let status = if quality_report.gap_count == 0 || repair_ranges.is_empty() {
        findings.push(MarketDataRepairFinding {
            severity: "INFO".to_string(),
            code: "no_repair_needed".to_string(),
            message: "No candle gaps were detected in the requested window.".to_string(),
        });
        MarketDataRepairStatus::NoRepairNeeded
    } else {
        if i64::try_from(repair_ranges.len()).unwrap_or(i64::MAX) < quality_report.gap_count {
            findings.push(MarketDataRepairFinding {
                severity: "MEDIUM".to_string(),
                code: "repair_ranges_truncated".to_string(),
                message: format!(
                    "Repair ranges were limited to max_ranges={}.",
                    request.max_ranges
                ),
            });
            recommendations.push(MarketDataRepairRecommendation {
                code: "increase_max_ranges".to_string(),
                message: "Increase max_ranges or repair a narrower window.".to_string(),
            });
        }
        if requires_source_interval {
            recommendations.push(MarketDataRepairRecommendation {
                code: "repair_source_then_reaggregate".to_string(),
                message:
                    "Repair missing 1m source candles, then re-aggregate the requested interval."
                        .to_string(),
            });
            for range in &mut repair_ranges {
                range.source_interval = CandleInterval::OneMinute.as_str().to_string();
            }
        } else {
            recommendations.push(MarketDataRepairRecommendation {
                code: "backfill_missing_ranges".to_string(),
                message: "Backfill only the missing public market-data ranges.".to_string(),
            });
        }
        MarketDataRepairStatus::RepairPlanned
    };

    Ok(MarketDataRepairPlan {
        exchange: request.exchange,
        symbol: symbol.as_str().to_string(),
        interval: interval.as_str().to_string(),
        start_time: request.start_time,
        end_time: request.end_time,
        status,
        initial_quality_status: quality_report.status,
        gap_count: quality_report.gap_count,
        repair_ranges,
        estimated_source_interval,
        requires_source_interval,
        reaggregate_derived_intervals: requires_source_interval
            && request.reaggregate_derived_intervals,
        findings,
        recommendations,
        correlation_id: request.correlation_id,
    })
}

fn merge_repair_gaps(
    gaps: &[MarketDataGap],
    source_interval: CandleInterval,
    max_ranges: usize,
) -> Vec<MarketDataRepairRange> {
    let mut ranges = Vec::<MarketDataRepairRange>::new();
    let source_interval_seconds = source_interval.duration().num_seconds();
    for gap in gaps {
        let missing = count_expected_candles_for_window(
            source_interval_seconds,
            gap.start_time,
            gap.end_time,
        )
        .max(gap.missing_candle_count);
        if let Some(last) = ranges.last_mut() {
            if last.end_time >= gap.start_time {
                last.end_time = last.end_time.max(gap.end_time);
                last.missing_candle_count += missing;
                continue;
            }
        }
        if ranges.len() >= max_ranges {
            break;
        }
        ranges.push(MarketDataRepairRange {
            source_interval: source_interval.as_str().to_string(),
            start_time: gap.start_time,
            end_time: gap.end_time,
            missing_candle_count: missing,
        });
    }
    ranges
}

pub fn count_expected_candles_for_window(
    interval_seconds: i64,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> i64 {
    if interval_seconds <= 0 || end_time <= start_time {
        return 0;
    }
    end_time
        .signed_duration_since(start_time)
        .num_seconds()
        .div_euclid(interval_seconds)
        .max(0)
}

pub fn summarize_candle_continuity(
    request: &MarketDataQualityRequest,
    candles: &[Candle],
    max_gaps_returned: usize,
) -> Result<MarketDataQualityReport, CoreError> {
    request.validate()?;
    let symbol = request.normalized_symbol()?;
    let interval = request.parsed_interval()?;
    let interval_seconds = request.expected_interval_seconds()?;
    let expected_candle_count =
        count_expected_candles_for_window(interval_seconds, request.start_time, request.end_time);

    let mut sorted = candles.to_vec();
    sorted.sort_by_key(|candle| candle.open_time);

    let actual_candle_count = i64::try_from(sorted.len()).unwrap_or(i64::MAX);
    let closed_candle_count =
        i64::try_from(sorted.iter().filter(|candle| candle.is_closed).count()).unwrap_or(i64::MAX);
    let open_candle_count = actual_candle_count.saturating_sub(closed_candle_count);
    let coverage_pct = if expected_candle_count > 0 {
        Decimal::from(closed_candle_count.min(expected_candle_count)) * Decimal::new(100, 0)
            / Decimal::from(expected_candle_count)
    } else {
        Decimal::ZERO
    }
    .round_dp(4);

    let mut findings = Vec::new();
    let mut recommendations = Vec::new();
    let mut gaps = Vec::new();
    let mut duplicate_count = 0_i64;
    let mut total_missing_from_gaps = 0_i64;
    let mut gap_count = 0_i64;
    let mut largest_gap_seconds = 0_i64;
    let closed = sorted
        .iter()
        .filter(|candle| candle.is_closed)
        .collect::<Vec<_>>();

    for pair in closed.windows(2) {
        let previous = pair[0];
        let next = pair[1];
        let expected_next = previous.open_time + Duration::seconds(interval_seconds);
        if next.open_time == previous.open_time {
            duplicate_count += 1;
        } else if next.open_time > expected_next {
            let gap_seconds = next
                .open_time
                .signed_duration_since(expected_next)
                .num_seconds()
                .max(0);
            let missing = gap_seconds.div_euclid(interval_seconds).max(1);
            gap_count += 1;
            total_missing_from_gaps += missing;
            largest_gap_seconds = largest_gap_seconds.max(gap_seconds);
            if gaps.len() < max_gaps_returned {
                gaps.push(MarketDataGap {
                    start_time: expected_next,
                    end_time: next.open_time,
                    missing_candle_count: missing,
                    gap_seconds,
                });
            }
        }
    }

    let missing_candle_count = expected_candle_count
        .saturating_sub(closed_candle_count)
        .max(total_missing_from_gaps);
    let gap_pct = if expected_candle_count > 0 {
        Decimal::from(total_missing_from_gaps) * Decimal::new(100, 0)
            / Decimal::from(expected_candle_count)
    } else {
        Decimal::ZERO
    };
    let max_allowed_gap_count = request.max_allowed_gap_count.unwrap_or(0).max(0);
    let max_allowed_gap_pct = request.max_allowed_gap_pct.unwrap_or(Decimal::ZERO);

    if actual_candle_count == 0 {
        findings.push(MarketDataQualityFinding {
            severity: "HIGH".to_string(),
            code: "no_candles".to_string(),
            message: "No candles exist for the requested market data window.".to_string(),
        });
        recommendations.push(MarketDataQualityRecommendation {
            code: "backfill_market_data".to_string(),
            message:
                "Backfill the requested symbol and interval before using this window for research."
                    .to_string(),
        });
    }

    if gap_count > 0 {
        findings.push(MarketDataQualityFinding {
            severity: if gap_pct > Decimal::new(5, 0) {
                "HIGH"
            } else {
                "MEDIUM"
            }
            .to_string(),
            code: "candle_gaps_detected".to_string(),
            message: format!(
                "{gap_count} expected candle slots are missing from the closed-candle sequence."
            ),
        });
        recommendations.push(MarketDataQualityRecommendation {
            code: "repair_candle_gaps".to_string(),
            message: "Backfill or exclude gap windows before trusting experiments, walk-forward validation, attribution, qualification, or dossiers.".to_string(),
        });
    }

    if duplicate_count > 0 {
        findings.push(MarketDataQualityFinding {
            severity: "MEDIUM".to_string(),
            code: "duplicate_open_times_detected".to_string(),
            message: format!("{duplicate_count} duplicate closed candle open times were detected."),
        });
    }

    if open_candle_count > 0 && request.end_time < Utc::now() {
        findings.push(MarketDataQualityFinding {
            severity: "MEDIUM".to_string(),
            code: "open_candles_in_historical_window".to_string(),
            message: format!(
                "{open_candle_count} open candles were found inside a completed historical window."
            ),
        });
    }

    if expected_candle_count > 0 && coverage_pct < Decimal::new(95, 0) {
        findings.push(MarketDataQualityFinding {
            severity: "HIGH".to_string(),
            code: "coverage_below_95_pct".to_string(),
            message: format!("Closed-candle coverage is {coverage_pct}% for the requested window."),
        });
    } else if expected_candle_count > 0 && coverage_pct < Decimal::new(99, 0) {
        findings.push(MarketDataQualityFinding {
            severity: "MEDIUM".to_string(),
            code: "coverage_below_99_pct".to_string(),
            message: format!("Closed-candle coverage is {coverage_pct}% for the requested window."),
        });
    }

    let status = if actual_candle_count == 0 {
        MarketDataQualityStatus::InsufficientData
    } else if expected_candle_count == 0 {
        MarketDataQualityStatus::Unknown
    } else if coverage_pct < Decimal::new(95, 0) || gap_pct > Decimal::new(5, 0) {
        MarketDataQualityStatus::Bad
    } else if coverage_pct < Decimal::new(99, 0)
        || gap_count > max_allowed_gap_count
        || gap_pct > max_allowed_gap_pct
        || duplicate_count > 0
        || open_candle_count > 0
    {
        MarketDataQualityStatus::Degraded
    } else {
        MarketDataQualityStatus::Good
    };

    if status == MarketDataQualityStatus::Good {
        findings.push(MarketDataQualityFinding {
            severity: "INFO".to_string(),
            code: "market_data_quality_good".to_string(),
            message: "Closed candles are continuous for the requested window.".to_string(),
        });
    }

    Ok(MarketDataQualityReport {
        exchange: request.exchange,
        symbol: symbol.as_str().to_string(),
        interval: interval.as_str().to_string(),
        window_start: request.start_time,
        window_end: request.end_time,
        expected_candle_count,
        actual_candle_count,
        closed_candle_count,
        open_candle_count,
        missing_candle_count,
        coverage_pct,
        gap_count,
        largest_gap_seconds,
        gaps,
        first_candle_time: sorted.first().map(|candle| candle.open_time),
        last_candle_time: sorted.last().map(|candle| candle.open_time),
        status,
        findings,
        recommendations,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CandleAggregationOutcome {
    pub candles: Vec<Candle>,
    pub skipped_incomplete_buckets: i32,
}

pub fn aggregate_closed_1m_candles(
    candles: &[Candle],
    target_interval: CandleInterval,
) -> CandleAggregationOutcome {
    if candles.is_empty() {
        return CandleAggregationOutcome {
            candles: Vec::new(),
            skipped_incomplete_buckets: 0,
        };
    }

    let expected_per_bucket = target_interval.source_candle_count();
    let mut buckets = std::collections::BTreeMap::<DateTime<Utc>, Vec<Candle>>::new();

    for candle in candles
        .iter()
        .filter(|candle| candle.is_closed && candle.interval == CandleInterval::OneMinute)
    {
        buckets
            .entry(target_interval.bucket_start(candle.open_time))
            .or_default()
            .push(candle.clone());
    }

    let mut aggregated = Vec::new();
    let mut skipped_incomplete_buckets = 0;

    for (bucket_start, mut bucket) in buckets {
        bucket.sort_by_key(|candle| candle.open_time);
        let is_complete = i64::try_from(bucket.len()).ok() == Some(expected_per_bucket)
            && bucket.iter().enumerate().all(|(index, candle)| {
                candle.open_time
                    == bucket_start + Duration::minutes(i64::try_from(index).unwrap_or_default())
            });
        if !is_complete {
            skipped_incomplete_buckets += 1;
            continue;
        }

        let first = bucket
            .first()
            .expect("complete aggregation bucket must contain at least one candle");
        let last = bucket
            .last()
            .expect("complete aggregation bucket must contain at least one candle");
        let high = bucket
            .iter()
            .map(|candle| candle.high)
            .max()
            .expect("complete aggregation bucket must contain a high");
        let low = bucket
            .iter()
            .map(|candle| candle.low)
            .min()
            .expect("complete aggregation bucket must contain a low");
        let volume = bucket
            .iter()
            .fold(Decimal::ZERO, |sum, candle| sum + candle.volume);
        let quote_volume = bucket.iter().try_fold(Decimal::ZERO, |sum, candle| {
            candle.quote_volume.map(|value| sum + value)
        });
        let trade_count = bucket.iter().map(|candle| candle.trade_count).sum();
        let id_seed = format!(
            "{}:{}:{}:{}",
            first.exchange.as_str(),
            first.symbol.as_str(),
            target_interval.as_str(),
            bucket_start.to_rfc3339()
        );

        aggregated.push(Candle {
            id: Uuid::new_v5(&Uuid::NAMESPACE_OID, id_seed.as_bytes()),
            exchange: first.exchange,
            symbol: first.symbol.clone(),
            interval: target_interval,
            open_time: bucket_start,
            close_time: target_interval.bucket_close_time(bucket_start),
            open: first.open,
            high,
            low,
            close: last.close,
            volume,
            quote_volume,
            trade_count,
            is_closed: true,
            created_at: last.updated_at,
            updated_at: last.updated_at,
        });
    }

    CandleAggregationOutcome {
        candles: aggregated,
        skipped_incomplete_buckets,
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
    TrendFilterMomentumV1,
    VolatilityBreakoutV2,
    RangeReversionV1,
}

impl StrategyId {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MomentumV1 => "momentum_v1",
            Self::VolatilityBreakoutV1 => "volatility_breakout_v1",
            Self::TrendFilterMomentumV1 => "trend_filter_momentum_v1",
            Self::VolatilityBreakoutV2 => "volatility_breakout_v2",
            Self::RangeReversionV1 => "range_reversion_v1",
        }
    }
}

impl std::str::FromStr for StrategyId {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "momentum_v1" => Ok(Self::MomentumV1),
            "volatility_breakout_v1" => Ok(Self::VolatilityBreakoutV1),
            "trend_filter_momentum_v1" => Ok(Self::TrendFilterMomentumV1),
            "volatility_breakout_v2" => Ok(Self::VolatilityBreakoutV2),
            "range_reversion_v1" => Ok(Self::RangeReversionV1),
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
    TrendFilterMomentum,
    BreakoutAboveRecentHigh,
    VolumeConfirmedBreakout,
    RangeReversion,
    ConditionsNotMet,
    InsufficientHistory,
    StrategyDisabled,
}

impl SignalReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ThreeConsecutiveHigherCloses => "three_consecutive_higher_closes",
            Self::MomentumHigherCloses => "momentum_higher_closes",
            Self::TrendFilterMomentum => "trend_filter_momentum",
            Self::BreakoutAboveRecentHigh => "breakout_above_recent_high",
            Self::VolumeConfirmedBreakout => "volume_confirmed_breakout",
            Self::RangeReversion => "range_reversion",
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
            "trend_filter_momentum" => Ok(Self::TrendFilterMomentum),
            "breakout_above_recent_high" => Ok(Self::BreakoutAboveRecentHigh),
            "volume_confirmed_breakout" => Ok(Self::VolumeConfirmedBreakout),
            "range_reversion" => Ok(Self::RangeReversion),
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
    pub trend_lookback_candles: Option<u32>,
    pub momentum_lookback_candles: Option<u32>,
    pub breakout_lookback_candles: Option<u32>,
    pub lower_band_pct: Option<Decimal>,
    pub upper_band_pct: Option<Decimal>,
    pub min_range_width_pct: Option<Decimal>,
    pub max_range_width_pct: Option<Decimal>,
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
    pub trend_lookback_candles: Option<u32>,
    pub momentum_lookback_candles: Option<u32>,
    pub breakout_lookback_candles: Option<u32>,
    pub lower_band_pct: Option<Decimal>,
    pub upper_band_pct: Option<Decimal>,
    pub min_range_width_pct: Option<Decimal>,
    pub max_range_width_pct: Option<Decimal>,
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
pub struct StrategyDiagnosticsRequest {
    pub symbol: Option<String>,
    pub timeframe: Option<String>,
    pub limit: Option<i64>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyDiagnosticSeverity {
    Info,
    Warn,
    Error,
}

impl StrategyDiagnosticSeverity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyDiagnosticCheck {
    pub name: String,
    pub passed: bool,
    pub severity: StrategyDiagnosticSeverity,
    pub message: String,
    pub actual: Option<String>,
    pub expected: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyNoSignalReason {
    MomentumNotStrictlyHigherCloses,
    TrendCloseNotAboveSma,
    TrendMomentumNotPositive,
    TrendTooExtended,
    VolatilityBelowMinimum,
    BreakoutNotAboveRecentHigh,
    BreakoutVolumeBelowAverage,
    BreakoutTooExtended,
    InsufficientData,
    RangeTooNarrow,
    RangeTooWide,
    NotNearLowerBand,
    NoReversalConfirmation,
    ConfidenceBelowFloor,
    InsufficientCandles,
    StrategyDisabled,
    InvalidConfig,
    StaleData,
}

impl StrategyNoSignalReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MomentumNotStrictlyHigherCloses => "MOMENTUM_NOT_STRICTLY_HIGHER_CLOSES",
            Self::TrendCloseNotAboveSma => "TREND_CLOSE_NOT_ABOVE_SMA",
            Self::TrendMomentumNotPositive => "TREND_MOMENTUM_NOT_POSITIVE",
            Self::TrendTooExtended => "TREND_TOO_EXTENDED",
            Self::VolatilityBelowMinimum => "VOLATILITY_BELOW_MINIMUM",
            Self::BreakoutNotAboveRecentHigh => "BREAKOUT_NOT_ABOVE_RECENT_HIGH",
            Self::BreakoutVolumeBelowAverage => "BREAKOUT_VOLUME_BELOW_AVERAGE",
            Self::BreakoutTooExtended => "BREAKOUT_TOO_EXTENDED",
            Self::InsufficientData => "INSUFFICIENT_DATA",
            Self::RangeTooNarrow => "RANGE_TOO_NARROW",
            Self::RangeTooWide => "RANGE_TOO_WIDE",
            Self::NotNearLowerBand => "NOT_NEAR_LOWER_BAND",
            Self::NoReversalConfirmation => "NO_REVERSAL_CONFIRMATION",
            Self::ConfidenceBelowFloor => "CONFIDENCE_BELOW_FLOOR",
            Self::InsufficientCandles => "INSUFFICIENT_CANDLES",
            Self::StrategyDisabled => "STRATEGY_DISABLED",
            Self::InvalidConfig => "INVALID_CONFIG",
            Self::StaleData => "STALE_DATA",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyDataHealth {
    pub required_lookback_candles: u32,
    pub required_closed_candles: i64,
    pub available_closed_candles: i64,
    pub latest_closed_candle_time: Option<DateTime<Utc>>,
    pub latest_closed_candle_age_ms: Option<i64>,
    pub stale: bool,
    pub latest_closes: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyDiagnosticsDecision {
    WouldSignal,
    NoSignal,
    InsufficientData,
    StrategyDisabled,
    InvalidConfig,
    StaleData,
}

impl StrategyDiagnosticsDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WouldSignal => "WOULD_SIGNAL",
            Self::NoSignal => "NO_SIGNAL",
            Self::InsufficientData => "INSUFFICIENT_DATA",
            Self::StrategyDisabled => "STRATEGY_DISABLED",
            Self::InvalidConfig => "INVALID_CONFIG",
            Self::StaleData => "STALE_DATA",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyDiagnosticsResult {
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub strategy_enabled: bool,
    pub config_valid: bool,
    pub validation_issues: Vec<StrategyConfigValidationIssue>,
    pub data_health: StrategyDataHealth,
    pub condition_checks: Vec<StrategyDiagnosticCheck>,
    pub final_decision: StrategyDiagnosticsDecision,
    pub no_signal_reason: Option<StrategyNoSignalReason>,
    pub summary: String,
    pub source_candle_open_time: Option<DateTime<Utc>>,
    pub confidence: Option<Decimal>,
    pub correlation_id: Uuid,
    pub evaluated_at: DateTime<Utc>,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketProviderErrorKind {
    DnsError,
    ConnectError,
    Timeout,
    Http4xx,
    Http5xx,
    RateLimited,
    ParseError,
    EmptyResponse,
    Unknown,
}

impl MarketProviderErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DnsError => "DNS_ERROR",
            Self::ConnectError => "CONNECT_ERROR",
            Self::Timeout => "TIMEOUT",
            Self::Http4xx => "HTTP_4XX",
            Self::Http5xx => "HTTP_5XX",
            Self::RateLimited => "RATE_LIMITED",
            Self::ParseError => "PARSE_ERROR",
            Self::EmptyResponse => "EMPTY_RESPONSE",
            Self::Unknown => "UNKNOWN",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketProviderDiagnostic {
    pub provider: String,
    pub base_url: String,
    pub endpoint: String,
    pub symbol: Option<String>,
    pub interval: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub http_status: Option<u16>,
    pub error_kind: MarketProviderErrorKind,
    pub retryable: bool,
    pub message: String,
    pub recommendation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketProviderAttempt {
    pub provider: String,
    pub base_url: String,
    pub endpoint: String,
    pub success: bool,
    pub latency_ms: Option<u128>,
    pub http_status: Option<u16>,
    pub error_kind: Option<MarketProviderErrorKind>,
    pub recommendation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketProviderHealth {
    pub provider: String,
    pub status: String,
    pub base_url: String,
    pub endpoint: String,
    pub latency_ms: Option<u128>,
    pub http_status: Option<u16>,
    pub error_kind: Option<MarketProviderErrorKind>,
    pub recommendation: Option<String>,
    pub fallback_available: bool,
    pub fallback_base_urls: Vec<String>,
    pub attempts: Vec<MarketProviderAttempt>,
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
    pub provider_attempts: Vec<MarketProviderAttempt>,
    pub selected_provider: Option<String>,
    pub failure_diagnostic: Option<MarketProviderDiagnostic>,
    pub recommendation: Option<String>,
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
        self.timeframe.parse::<CandleInterval>()?;

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
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyExperimentStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl StrategyExperimentStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
        }
    }
}

impl std::str::FromStr for StrategyExperimentStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "PENDING" => Ok(Self::Pending),
            "RUNNING" => Ok(Self::Running),
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            other => Err(CoreError::UnsupportedStrategyExperimentStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StrategyExperimentMetric {
    NetPnlPct,
    MaxDrawdownPct,
    TradeCount,
    WinRate,
    FeeSlippageDragPct,
    RiskAdjustedScore,
}

impl StrategyExperimentMetric {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NetPnlPct => "net_pnl_pct",
            Self::MaxDrawdownPct => "max_drawdown_pct",
            Self::TradeCount => "trade_count",
            Self::WinRate => "win_rate",
            Self::FeeSlippageDragPct => "fee_slippage_drag_pct",
            Self::RiskAdjustedScore => "risk_adjusted_score",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyExperimentCandidate {
    pub lookback_candles: u32,
    pub trend_lookback_candles: Option<u32>,
    pub momentum_lookback_candles: Option<u32>,
    pub breakout_lookback_candles: Option<u32>,
    pub lower_band_pct: Option<Decimal>,
    pub min_range_width_pct: Option<Decimal>,
    pub max_range_width_pct: Option<Decimal>,
    pub holding_candles: Option<u32>,
    pub stop_loss_pct: Option<Decimal>,
    pub take_profit_pct: Option<Decimal>,
    pub max_signal_age_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyExperimentRequest {
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub initial_capital: Decimal,
    pub fee_bps: Decimal,
    pub slippage_bps: Decimal,
    pub lookback_candidates: Vec<u32>,
    pub trend_lookback_candidates: Option<Vec<u32>>,
    pub momentum_lookback_candidates: Option<Vec<u32>>,
    pub breakout_lookback_candidates: Option<Vec<u32>>,
    pub lower_band_pct_candidates: Option<Vec<Decimal>>,
    pub min_range_width_pct_candidates: Option<Vec<Decimal>>,
    pub max_range_width_pct_candidates: Option<Vec<Decimal>>,
    pub holding_candles_candidates: Option<Vec<u32>>,
    pub stop_loss_pct_candidates: Option<Vec<Decimal>>,
    pub take_profit_pct_candidates: Option<Vec<Decimal>>,
    pub max_signal_age_ms: Option<i64>,
    pub max_runs: Option<u32>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyWalkForwardStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Skipped,
}

impl StrategyWalkForwardStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Skipped => "SKIPPED",
        }
    }
}

impl std::str::FromStr for StrategyWalkForwardStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "PENDING" => Ok(Self::Pending),
            "RUNNING" => Ok(Self::Running),
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            "SKIPPED" => Ok(Self::Skipped),
            other => Err(CoreError::UnsupportedStrategyWalkForwardStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyWalkForwardCandidate {
    #[serde(default)]
    pub lookback_candles: u32,
    #[serde(default)]
    pub trend_lookback_candles: Option<u32>,
    #[serde(default)]
    pub momentum_lookback_candles: Option<u32>,
    #[serde(default)]
    pub breakout_lookback_candles: Option<u32>,
    #[serde(default)]
    pub lower_band_pct: Option<Decimal>,
    #[serde(default)]
    pub min_range_width_pct: Option<Decimal>,
    #[serde(default)]
    pub max_range_width_pct: Option<Decimal>,
    #[serde(default)]
    pub holding_candles: Option<u32>,
    #[serde(default)]
    pub stop_loss_pct: Option<Decimal>,
    #[serde(default)]
    pub take_profit_pct: Option<Decimal>,
    #[serde(default)]
    pub max_signal_age_ms: Option<i64>,
}

fn default_strategy_walk_forward_candidate() -> StrategyWalkForwardCandidate {
    StrategyWalkForwardCandidate {
        lookback_candles: 0,
        trend_lookback_candles: None,
        momentum_lookback_candles: None,
        breakout_lookback_candles: None,
        lower_band_pct: None,
        min_range_width_pct: None,
        max_range_width_pct: None,
        holding_candles: None,
        stop_loss_pct: None,
        take_profit_pct: None,
        max_signal_age_ms: None,
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyWalkForwardRobustnessStatus {
    Robust,
    Weak,
    OverfitRisk,
    InsufficientData,
    Failed,
}

impl StrategyWalkForwardRobustnessStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Robust => "ROBUST",
            Self::Weak => "WEAK",
            Self::OverfitRisk => "OVERFIT_RISK",
            Self::InsufficientData => "INSUFFICIENT_DATA",
            Self::Failed => "FAILED",
        }
    }
}

impl std::str::FromStr for StrategyWalkForwardRobustnessStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "ROBUST" => Ok(Self::Robust),
            "WEAK" => Ok(Self::Weak),
            "OVERFIT_RISK" => Ok(Self::OverfitRisk),
            "INSUFFICIENT_DATA" => Ok(Self::InsufficientData),
            "FAILED" => Ok(Self::Failed),
            other => Err(CoreError::UnsupportedStrategyWalkForwardStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyWalkForwardRecommendation {
    pub action: String,
    pub reason: String,
}

impl Default for StrategyWalkForwardRecommendation {
    fn default() -> Self {
        Self {
            action: "REVIEW".to_string(),
            reason: "Review walk-forward robustness before candidate acceptance.".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyWalkForwardRequest {
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    #[serde(default)]
    pub config: Option<Value>,
    #[serde(default)]
    pub experiment_run_id: Option<Uuid>,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    #[serde(default, alias = "train_window_hours")]
    pub window_train_size_hours: i64,
    #[serde(alias = "test_window_hours")]
    pub window_test_size_hours: i64,
    #[serde(alias = "step_hours")]
    pub step_size_hours: i64,
    pub initial_capital: Decimal,
    pub fee_bps: Decimal,
    pub slippage_bps: Decimal,
    #[serde(default = "default_strategy_walk_forward_candidate")]
    pub candidate_config: StrategyWalkForwardCandidate,
    #[serde(default, alias = "min_windows")]
    pub min_required_test_windows: Option<u32>,
    pub correlation_id: Option<Uuid>,
}

impl StrategyWalkForwardRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.strategy_id.trim().is_empty() {
            return Err(CoreError::EmptyStrategyWalkForwardStrategyId);
        }
        if self.symbol.trim().is_empty() {
            return Err(CoreError::EmptyStrategyWalkForwardSymbol);
        }
        if self.timeframe.trim().is_empty() {
            return Err(CoreError::EmptyStrategyWalkForwardTimeframe);
        }
        if self.end_time <= self.start_time {
            return Err(CoreError::InvalidStrategyWalkForwardTimeRange);
        }
        if self.window_train_size_hours < 0 {
            return Err(CoreError::InvalidStrategyWalkForwardWindowSize(
                "window_train_size_hours".to_string(),
            ));
        }
        if self.window_test_size_hours <= 0 {
            return Err(CoreError::InvalidStrategyWalkForwardWindowSize(
                "window_test_size_hours".to_string(),
            ));
        }
        if self.step_size_hours <= 0 {
            return Err(CoreError::InvalidStrategyWalkForwardStepSize);
        }
        if self.initial_capital <= Decimal::ZERO {
            return Err(CoreError::InvalidStrategyWalkForwardInitialCapital);
        }
        if self.fee_bps < Decimal::ZERO {
            return Err(CoreError::InvalidBacktestBps("fee_bps".to_string()));
        }
        if self.slippage_bps < Decimal::ZERO {
            return Err(CoreError::InvalidBacktestBps("slippage_bps".to_string()));
        }
        if self.candidate_config.lookback_candles == 0
            && self.config.is_none()
            && self.experiment_run_id.is_none()
        {
            return Err(CoreError::EmptyStrategyWalkForwardCandidateLookback);
        }
        if let Some(holding_candles) = self.candidate_config.holding_candles {
            if holding_candles == 0 {
                return Err(CoreError::InvalidHoldingCandles);
            }
        }
        if let Some(max_signal_age_ms) = self.candidate_config.max_signal_age_ms {
            if max_signal_age_ms <= 0 {
                return Err(CoreError::InvalidStrategyMaxSignalAgeMs(max_signal_age_ms));
            }
        }
        if let Some(min_required_test_windows) = self.min_required_test_windows {
            if min_required_test_windows == 0 {
                return Err(CoreError::InvalidStrategyWalkForwardMinRequiredWindows);
            }
        }

        self.timeframe.parse::<CandleInterval>()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyWalkForwardWindow {
    pub window_index: i32,
    pub train_start: DateTime<Utc>,
    pub train_end: DateTime<Utc>,
    pub test_start: DateTime<Utc>,
    pub test_end: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyWalkForwardRobustnessSummary {
    pub profitable_window_pct: Decimal,
    pub total_trade_count: i32,
    pub avg_trades_per_completed_window: Decimal,
    pub avg_fee_slippage_drag_pct: Decimal,
    pub skipped_window_pct: Decimal,
    pub dominant_winner_share_pct: Decimal,
    #[serde(default)]
    pub recommendation: StrategyWalkForwardRecommendation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyWalkForwardWindowResult {
    pub id: Uuid,
    pub walk_forward_id: Uuid,
    pub window: StrategyWalkForwardWindow,
    pub status: StrategyWalkForwardStatus,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyWalkForwardResult {
    pub walk_forward_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub total_windows: i32,
    pub completed_windows: i32,
    pub failed_windows: i32,
    pub skipped_windows: i32,
    pub profitable_test_windows: i32,
    pub profitable_windows: i32,
    pub losing_test_windows: i32,
    pub losing_windows: i32,
    pub avg_test_pnl_pct: Decimal,
    pub avg_pnl_pct: Decimal,
    pub median_test_pnl_pct: Decimal,
    pub median_pnl_pct: Decimal,
    pub worst_test_pnl_pct: Decimal,
    pub worst_pnl_pct: Decimal,
    pub best_test_pnl_pct: Decimal,
    pub best_pnl_pct: Decimal,
    pub avg_max_drawdown_pct: Decimal,
    pub max_drawdown_pct: Decimal,
    pub avg_trade_count: Decimal,
    pub robustness_score: Decimal,
    pub consistency_score: Decimal,
    pub status: StrategyWalkForwardStatus,
    pub robustness_status: StrategyWalkForwardRobustnessStatus,
    pub robustness_summary: StrategyWalkForwardRobustnessSummary,
    pub recommendation: StrategyWalkForwardRecommendation,
    #[serde(default)]
    pub warnings: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

impl StrategyExperimentRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.strategy_id.trim().is_empty() {
            return Err(CoreError::EmptyStrategyExperimentStrategyId);
        }
        if self.symbol.trim().is_empty() {
            return Err(CoreError::EmptyStrategyExperimentSymbol);
        }
        if self.timeframe.trim().is_empty() {
            return Err(CoreError::EmptyStrategyExperimentTimeframe);
        }
        if self.end_time <= self.start_time {
            return Err(CoreError::InvalidStrategyExperimentTimeRange);
        }
        if self.initial_capital <= Decimal::ZERO {
            return Err(CoreError::InvalidStrategyExperimentInitialCapital);
        }
        self.timeframe.parse::<CandleInterval>()?;
        if self.fee_bps < Decimal::ZERO {
            return Err(CoreError::InvalidBacktestBps("fee_bps".to_string()));
        }
        if self.slippage_bps < Decimal::ZERO {
            return Err(CoreError::InvalidBacktestBps("slippage_bps".to_string()));
        }
        if self.lookback_candidates.is_empty() {
            return Err(CoreError::EmptyStrategyExperimentCandidates);
        }
        for lookback in self
            .trend_lookback_candidates
            .as_ref()
            .into_iter()
            .flatten()
            .chain(
                self.momentum_lookback_candidates
                    .as_ref()
                    .into_iter()
                    .flatten(),
            )
            .chain(
                self.breakout_lookback_candidates
                    .as_ref()
                    .into_iter()
                    .flatten(),
            )
            .copied()
        {
            if lookback == 0 {
                return Err(CoreError::EmptyStrategyExperimentCandidates);
            }
        }
        if let Some(max_runs) = self.max_runs {
            if max_runs == 0 {
                return Err(CoreError::InvalidStrategyExperimentMaxRuns);
            }
        }

        for holding in self
            .holding_candles_candidates
            .as_ref()
            .into_iter()
            .flatten()
            .copied()
        {
            if holding == 0 {
                return Err(CoreError::InvalidHoldingCandles);
            }
        }

        if let Some(max_signal_age_ms) = self.max_signal_age_ms {
            if max_signal_age_ms <= 0 {
                return Err(CoreError::InvalidStrategyMaxSignalAgeMs(max_signal_age_ms));
            }
        }

        Ok(())
    }

    pub fn candidates(&self) -> Vec<StrategyExperimentCandidate> {
        let holdings = self
            .holding_candles_candidates
            .clone()
            .filter(|values| !values.is_empty())
            .unwrap_or_else(|| vec![0]);
        let stop_losses = self
            .stop_loss_pct_candidates
            .clone()
            .filter(|values| !values.is_empty())
            .unwrap_or_else(|| vec![Decimal::ZERO]);
        let take_profits = self
            .take_profit_pct_candidates
            .clone()
            .filter(|values| !values.is_empty())
            .unwrap_or_else(|| vec![Decimal::ZERO]);

        let mut candidates = Vec::new();
        let trend_lookbacks = self
            .trend_lookback_candidates
            .clone()
            .filter(|values| !values.is_empty())
            .unwrap_or_else(|| self.lookback_candidates.clone());
        let momentum_lookbacks = self
            .momentum_lookback_candidates
            .clone()
            .filter(|values| !values.is_empty())
            .unwrap_or_else(|| vec![0]);
        let breakout_lookbacks = self
            .breakout_lookback_candidates
            .clone()
            .filter(|values| !values.is_empty())
            .unwrap_or_else(|| self.lookback_candidates.clone());
        let lower_bands = self
            .lower_band_pct_candidates
            .clone()
            .filter(|values| !values.is_empty())
            .unwrap_or_else(|| vec![Decimal::new(20, 0)]);
        let min_range_widths = self
            .min_range_width_pct_candidates
            .clone()
            .filter(|values| !values.is_empty())
            .unwrap_or_else(|| vec![Decimal::ZERO]);
        let max_range_widths = self
            .max_range_width_pct_candidates
            .clone()
            .filter(|values| !values.is_empty())
            .unwrap_or_else(|| vec![Decimal::ZERO]);

        for lookback_candles in &self.lookback_candidates {
            for trend_lookback_candles in &trend_lookbacks {
                for momentum_lookback_candles in &momentum_lookbacks {
                    for breakout_lookback_candles in &breakout_lookbacks {
                        for lower_band_pct in &lower_bands {
                            for min_range_width_pct in &min_range_widths {
                                for max_range_width_pct in &max_range_widths {
                                    for holding_candles in &holdings {
                                        for stop_loss_pct in &stop_losses {
                                            for take_profit_pct in &take_profits {
                                                candidates.push(StrategyExperimentCandidate {
                                                    lookback_candles: *lookback_candles,
                                                    trend_lookback_candles: Some(
                                                        *trend_lookback_candles,
                                                    ),
                                                    momentum_lookback_candles:
                                                        if *momentum_lookback_candles == 0 {
                                                            None
                                                        } else {
                                                            Some(*momentum_lookback_candles)
                                                        },
                                                    breakout_lookback_candles: Some(
                                                        *breakout_lookback_candles,
                                                    ),
                                                    lower_band_pct: Some(*lower_band_pct).filter(
                                                        |_| {
                                                            self.strategy_id == "range_reversion_v1"
                                                        },
                                                    ),
                                                    min_range_width_pct: Some(*min_range_width_pct)
                                                        .filter(|value| {
                                                            self.strategy_id == "range_reversion_v1"
                                                                && *value != Decimal::ZERO
                                                        }),
                                                    max_range_width_pct: Some(*max_range_width_pct)
                                                        .filter(|value| {
                                                            self.strategy_id == "range_reversion_v1"
                                                                && *value != Decimal::ZERO
                                                        }),
                                                    holding_candles: if *holding_candles == 0 {
                                                        None
                                                    } else {
                                                        Some(*holding_candles)
                                                    },
                                                    stop_loss_pct: if *stop_loss_pct
                                                        == Decimal::ZERO
                                                    {
                                                        None
                                                    } else {
                                                        Some(*stop_loss_pct)
                                                    },
                                                    take_profit_pct: if *take_profit_pct
                                                        == Decimal::ZERO
                                                    {
                                                        None
                                                    } else {
                                                        Some(*take_profit_pct)
                                                    },
                                                    max_signal_age_ms: self.max_signal_age_ms,
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if let Some(max_runs) = self.max_runs {
            candidates.truncate(max_runs as usize);
        }

        candidates
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyMultiTimeframeExperimentRequest {
    pub strategy_id: String,
    pub symbol: String,
    pub timeframes: Vec<String>,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub initial_capital: Decimal,
    pub fee_bps: Decimal,
    pub slippage_bps: Decimal,
    pub lookback_candidates: Vec<u32>,
    pub trend_lookback_candidates: Option<Vec<u32>>,
    pub momentum_lookback_candidates: Option<Vec<u32>>,
    pub breakout_lookback_candidates: Option<Vec<u32>>,
    pub lower_band_pct_candidates: Option<Vec<Decimal>>,
    pub min_range_width_pct_candidates: Option<Vec<Decimal>>,
    pub max_range_width_pct_candidates: Option<Vec<Decimal>>,
    pub holding_candles_candidates: Option<Vec<u32>>,
    pub stop_loss_pct_candidates: Option<Vec<Decimal>>,
    pub take_profit_pct_candidates: Option<Vec<Decimal>>,
    pub max_signal_age_ms: Option<i64>,
    pub max_runs: Option<u32>,
    pub correlation_id: Option<Uuid>,
}

impl StrategyMultiTimeframeExperimentRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.timeframes.is_empty() {
            return Err(CoreError::EmptyStrategyExperimentTimeframes);
        }

        for timeframe in &self.timeframes {
            if timeframe.trim().is_empty() {
                return Err(CoreError::EmptyStrategyExperimentTimeframe);
            }
            timeframe.parse::<CandleInterval>()?;
        }

        self.single_timeframe_request(self.timeframes[0].clone())
            .validate()
    }

    pub fn single_timeframe_request(&self, timeframe: String) -> StrategyExperimentRequest {
        StrategyExperimentRequest {
            strategy_id: self.strategy_id.clone(),
            symbol: self.symbol.clone(),
            timeframe,
            start_time: self.start_time,
            end_time: self.end_time,
            initial_capital: self.initial_capital,
            fee_bps: self.fee_bps,
            slippage_bps: self.slippage_bps,
            lookback_candidates: self.lookback_candidates.clone(),
            trend_lookback_candidates: self.trend_lookback_candidates.clone(),
            momentum_lookback_candidates: self.momentum_lookback_candidates.clone(),
            breakout_lookback_candidates: self.breakout_lookback_candidates.clone(),
            lower_band_pct_candidates: self.lower_band_pct_candidates.clone(),
            min_range_width_pct_candidates: self.min_range_width_pct_candidates.clone(),
            max_range_width_pct_candidates: self.max_range_width_pct_candidates.clone(),
            holding_candles_candidates: self.holding_candles_candidates.clone(),
            stop_loss_pct_candidates: self.stop_loss_pct_candidates.clone(),
            take_profit_pct_candidates: self.take_profit_pct_candidates.clone(),
            max_signal_age_ms: self.max_signal_age_ms,
            max_runs: self.max_runs,
            correlation_id: self.correlation_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyExperimentRun {
    pub id: Uuid,
    pub experiment_id: Uuid,
    pub rank: i32,
    pub candidate: StrategyExperimentCandidate,
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
    pub status: StrategyExperimentStatus,
    pub warnings: Vec<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyExperimentComparison {
    pub ranking_metric: StrategyExperimentMetric,
    pub best_run_id: Option<Uuid>,
    pub worst_run_id: Option<Uuid>,
    pub ranked_run_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyExperimentResult {
    pub experiment_id: Uuid,
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
    pub max_runs: Option<u32>,
    pub status: StrategyExperimentStatus,
    pub run_count: i32,
    pub comparison: StrategyExperimentComparison,
    pub best_run: Option<StrategyExperimentRun>,
    pub worst_run: Option<StrategyExperimentRun>,
    pub candle_count: Option<i32>,
    pub warnings: Vec<String>,
    pub skipped_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyTimeframeCandidate {
    pub timeframe: String,
    pub candle_count: i32,
    pub required_candles: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyTimeframeComparison {
    pub candidate: StrategyTimeframeCandidate,
    pub experiment_id: Option<Uuid>,
    pub status: StrategyExperimentStatus,
    pub run_count: i32,
    pub best_run: Option<StrategyExperimentRun>,
    pub skipped_reason: Option<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyExperimentGlobalRankingEntry {
    pub timeframe: String,
    pub experiment_id: Uuid,
    pub candle_count: i32,
    pub required_candles: i32,
    pub insufficient_data_penalty: Decimal,
    pub overtrading_penalty: Decimal,
    pub run: StrategyExperimentRun,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyExperimentGlobalRanking {
    pub ranking_metric: StrategyExperimentMetric,
    pub best_run_id: Option<Uuid>,
    pub ranked_runs: Vec<StrategyExperimentGlobalRankingEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyMultiTimeframeExperimentResult {
    pub experiment_group_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub requested_timeframes: Vec<String>,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub initial_capital: Decimal,
    pub fee_bps: Decimal,
    pub slippage_bps: Decimal,
    pub max_signal_age_ms: Option<i64>,
    pub max_runs: Option<u32>,
    pub status: StrategyExperimentStatus,
    pub timeframe_comparisons: Vec<StrategyTimeframeComparison>,
    pub global_ranking: StrategyExperimentGlobalRanking,
    pub warnings: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MarketMode {
    Paper,
    Disabled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeEnvironment {
    Testnet,
    Live,
}

impl ExchangeEnvironment {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Testnet => "testnet",
            Self::Live => "live",
        }
    }
}

impl std::str::FromStr for ExchangeEnvironment {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "testnet" => Ok(Self::Testnet),
            "live" => Ok(Self::Live),
            other => Err(CoreError::UnsupportedExchangeEnvironment(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeName {
    Binance,
}

impl ExchangeName {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Binance => "binance",
        }
    }
}

impl std::str::FromStr for ExchangeName {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "binance" => Ok(Self::Binance),
            other => Err(CoreError::UnsupportedExchangeName(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyPerformanceMode {
    Backtest,
    Paper,
    Shadow,
    Combined,
}

impl StrategyPerformanceMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Backtest => "BACKTEST",
            Self::Paper => "PAPER",
            Self::Shadow => "SHADOW",
            Self::Combined => "COMBINED",
        }
    }
}

impl std::str::FromStr for StrategyPerformanceMode {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "BACKTEST" => Ok(Self::Backtest),
            "PAPER" => Ok(Self::Paper),
            "SHADOW" => Ok(Self::Shadow),
            "COMBINED" => Ok(Self::Combined),
            other => Err(CoreError::UnsupportedStrategyPerformanceMode(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StrategyPerformanceWindow {
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StrategyPerformanceRequest {
    pub strategy_id: Option<String>,
    pub symbol: Option<String>,
    pub timeframe: Option<String>,
    pub mode: StrategyPerformanceMode,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestnetPromotionFunnelRequest {
    pub strategy_id: Option<String>,
    pub symbol: Option<String>,
    pub timeframe: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperatorReportFormat {
    #[default]
    Json,
    Markdown,
}

impl OperatorReportFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Json => "JSON",
            Self::Markdown => "MARKDOWN",
        }
    }
}

impl std::str::FromStr for OperatorReportFormat {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "JSON" => Ok(Self::Json),
            "MARKDOWN" => Ok(Self::Markdown),
            other => Err(CoreError::UnsupportedOperatorReportFormat(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperatorReportStatus {
    Ok,
    Warning,
    Critical,
}

impl OperatorReportStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::Warning => "WARNING",
            Self::Critical => "CRITICAL",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperatorReportSeverity {
    Info,
    Low,
    Medium,
    High,
    Critical,
}

impl OperatorReportSeverity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Low => "LOW",
            Self::Medium => "MEDIUM",
            Self::High => "HIGH",
            Self::Critical => "CRITICAL",
        }
    }

    pub fn sort_weight(self) -> u8 {
        match self {
            Self::Critical => 5,
            Self::High => 4,
            Self::Medium => 3,
            Self::Low => 2,
            Self::Info => 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorReportRequest {
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub symbol: Option<String>,
    pub interval: Option<String>,
    pub strategy_id: Option<String>,
    #[serde(default)]
    pub format: OperatorReportFormat,
    #[serde(default)]
    pub persist: bool,
    pub correlation_id: Option<Uuid>,
}

impl Default for OperatorReportRequest {
    fn default() -> Self {
        Self {
            start_time: None,
            end_time: None,
            symbol: None,
            interval: None,
            strategy_id: None,
            format: OperatorReportFormat::Json,
            persist: false,
            correlation_id: None,
        }
    }
}

impl OperatorReportRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if let (Some(start_time), Some(end_time)) = (self.start_time, self.end_time) {
            if end_time < start_time {
                return Err(CoreError::InvalidOperatorReportTimeRange);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorReportHighlight {
    pub label: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorReportFinding {
    pub code: String,
    pub severity: OperatorReportSeverity,
    pub title: String,
    pub detail: String,
    pub section: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorReportRecommendation {
    pub code: String,
    pub priority: OperatorReportSeverity,
    pub detail: String,
    pub related_finding_codes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorReportSection {
    pub key: String,
    pub title: String,
    pub status: OperatorReportStatus,
    pub summary: String,
    pub highlights: Vec<OperatorReportHighlight>,
    pub snapshot: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperatorReportSummary {
    pub total_findings: usize,
    pub critical_findings: usize,
    pub high_findings: usize,
    pub medium_findings: usize,
    pub low_findings: usize,
    pub info_findings: usize,
    pub highest_severity: Option<OperatorReportSeverity>,
    pub kill_switch_active: bool,
    pub stale_feed_count: i64,
    pub risk_rejection_rate_pct: Decimal,
    pub paper_daily_pnl: Decimal,
    pub shadow_would_submit_count: i64,
    pub reconciliation_required_count: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionReadinessTarget {
    PaperPipeline,
    TestnetShadow,
    TestnetPromotion,
    TestnetSubmit,
}

impl ExecutionReadinessTarget {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PaperPipeline => "PAPER_PIPELINE",
            Self::TestnetShadow => "TESTNET_SHADOW",
            Self::TestnetPromotion => "TESTNET_PROMOTION",
            Self::TestnetSubmit => "TESTNET_SUBMIT",
        }
    }
}

impl std::str::FromStr for ExecutionReadinessTarget {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "PAPER_PIPELINE" => Ok(Self::PaperPipeline),
            "TESTNET_SHADOW" => Ok(Self::TestnetShadow),
            "TESTNET_PROMOTION" => Ok(Self::TestnetPromotion),
            "TESTNET_SUBMIT" => Ok(Self::TestnetSubmit),
            other => Err(CoreError::UnsupportedExecutionReadinessTarget(
                other.to_string(),
            )),
        }
    }
}

impl std::fmt::Display for ExecutionReadinessTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionReadinessStatus {
    Ready,
    NotReady,
    Degraded,
    Unknown,
}

impl ExecutionReadinessStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "READY",
            Self::NotReady => "NOT_READY",
            Self::Degraded => "DEGRADED",
            Self::Unknown => "UNKNOWN",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionReadinessCheckSeverity {
    Low,
    Medium,
    High,
    Critical,
}

impl ExecutionReadinessCheckSeverity {
    pub fn score_penalty(self) -> i32 {
        match self {
            Self::Low => 3,
            Self::Medium => 8,
            Self::High => 15,
            Self::Critical => 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionReadinessBlockingReason {
    DbUnhealthy,
    KillSwitchActive,
    MissingValidatedRiskConfig,
    StaleMarketFeed,
    AuthDisabled,
    MissingRecentMarketPrice,
    StrategyDisabled,
    StrategyConfigInvalid,
    MissingRecentClosedCandles,
    RiskConfigInvalid,
    PaperAccountMissing,
    PaperAccountUnhealthy,
    ShadowRunnerError,
    ZeroShadowWouldSubmitCount,
    PromotionFunnelHighRejectionRate,
    StaleLocalPrice,
    HighRiskRejectionRate,
    TestnetAdapterNotConfigured,
    PrivateStreamStale,
    PrivateStreamDisconnected,
    UnresolvedReconciliationMismatches,
    ReconciliationRequiredOrdersPresent,
    UnknownExchangeStateOrdersPresent,
    RecentRepairFailures,
    PromotionExpired,
    PromotionNotPreviewed,
    MissingApprovedRiskDecision,
    NonOwnerActor,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionReadinessRecommendation {
    RestoreDatabaseHealth,
    ResumeFromKillSwitch,
    ValidateRiskConfig,
    RefreshMarketFeed,
    EnableAuth,
    SeedRecentMarketPrice,
    EnableStrategy,
    FixStrategyConfig,
    BackfillClosedCandles,
    CreateOrRepairPaperAccount,
    ReviewPaperPnl,
    RestartShadowRunner,
    IncreaseShadowCoverage,
    ReducePromotionRejections,
    ReduceRiskRejections,
    ConfigureTestnetAdapter,
    ReconnectPrivateStream,
    ReconcileTestnetOrders,
    ResolveRepairFailures,
    PreviewOrRenewPromotion,
    ApproveRiskDecision,
    UseOwnerActor,
    RunRecentBacktest,
    VerifyRunnerState,
}

impl ExecutionReadinessRecommendation {
    pub fn message(self) -> &'static str {
        match self {
            Self::RestoreDatabaseHealth => {
                "Restore database health before trusting readiness output."
            }
            Self::ResumeFromKillSwitch => "Clear only after investigating active blockers.",
            Self::ValidateRiskConfig => {
                "Validate and persist the risk config before relying on execution gating."
            }
            Self::RefreshMarketFeed => {
                "Restore fresh market-feed updates before relying on target-specific checks."
            }
            Self::EnableAuth => "Enable auth before using readiness for operator escalation.",
            Self::SeedRecentMarketPrice => {
                "Restore a recent local market price before relying on execution checks."
            }
            Self::EnableStrategy => {
                "Enable strategy only after validating config and reviewing recent analytics."
            }
            Self::FixStrategyConfig => {
                "Fix strategy validation errors before re-checking readiness."
            }
            Self::BackfillClosedCandles => {
                "Backfill recent closed candles before relying on execution readiness."
            }
            Self::CreateOrRepairPaperAccount => {
                "Create or repair the default paper account before paper execution checks."
            }
            Self::ReviewPaperPnl => "Review recent paper PnL before operator escalation.",
            Self::RestartShadowRunner => {
                "Restart the shadow runner and confirm it is healthy before proceeding."
            }
            Self::IncreaseShadowCoverage => "Continue shadow mode before promotion.",
            Self::ReducePromotionRejections => {
                "Review recent promotion failures before widening promotion scope."
            }
            Self::ReduceRiskRejections => {
                "Review elevated risk rejections before escalating execution."
            }
            Self::ConfigureTestnetAdapter => {
                "Configure the testnet adapter before any submit-readiness checks."
            }
            Self::ReconnectPrivateStream => {
                "Restart private stream worker or verify listen-key lifecycle."
            }
            Self::ReconcileTestnetOrders => {
                "Run reconciliation and repair before further testnet submits."
            }
            Self::ResolveRepairFailures => {
                "Resolve recent repair failures before relying on submit readiness."
            }
            Self::PreviewOrRenewPromotion => {
                "Preview or renew the promotion before submit-readiness checks."
            }
            Self::ApproveRiskDecision => {
                "Use a recent approved risk decision before checking submit readiness."
            }
            Self::UseOwnerActor => "Use an owner actor for submit-readiness verification.",
            Self::RunRecentBacktest => {
                "Run a recent backtest to improve confidence, but it is not a hard blocker."
            }
            Self::VerifyRunnerState => {
                "Resume the shadow runner only after confirming the pause reason is understood."
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionReadinessCheck {
    pub code: String,
    pub name: String,
    pub passed: bool,
    pub blocking: bool,
    pub severity: ExecutionReadinessCheckSeverity,
    pub summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionReadinessRequest {
    pub target: ExecutionReadinessTarget,
    pub symbol: Option<String>,
    pub strategy_id: Option<String>,
    pub timeframe: Option<String>,
    pub promotion_id: Option<Uuid>,
    pub risk_decision_id: Option<Uuid>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub persist: bool,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionReadinessResult {
    pub readiness_id: Uuid,
    pub target: ExecutionReadinessTarget,
    pub status: ExecutionReadinessStatus,
    pub score: i32,
    pub blocking_reasons: Vec<ExecutionReadinessBlockingReason>,
    pub warnings: Vec<ExecutionReadinessCheck>,
    pub checks: Vec<ExecutionReadinessCheck>,
    pub recommendations: Vec<ExecutionReadinessRecommendation>,
    pub computed_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionReadinessSnapshot {
    pub id: Uuid,
    pub target: ExecutionReadinessTarget,
    pub status: ExecutionReadinessStatus,
    pub score: i32,
    pub blocking_reasons: Vec<ExecutionReadinessBlockingReason>,
    pub warnings: Vec<ExecutionReadinessCheck>,
    pub checks: Vec<ExecutionReadinessCheck>,
    pub recommendations: Vec<ExecutionReadinessRecommendation>,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

pub fn score_execution_readiness(checks: &[ExecutionReadinessCheck]) -> i32 {
    let mut score = 100_i32;

    for check in checks
        .iter()
        .filter(|check| !check.passed && !check.blocking)
    {
        score -= check.severity.score_penalty();
    }

    score.clamp(0, 100)
}

pub fn execution_readiness_status_from_checks(
    checks: &[ExecutionReadinessCheck],
    score: i32,
) -> ExecutionReadinessStatus {
    if checks.iter().any(|check| {
        !check.passed
            && check.blocking
            && check.severity == ExecutionReadinessCheckSeverity::Critical
    }) {
        return ExecutionReadinessStatus::Unknown;
    }

    if checks.iter().any(|check| !check.passed && check.blocking) || score < 60 {
        return ExecutionReadinessStatus::NotReady;
    }

    if score >= 85 {
        return ExecutionReadinessStatus::Ready;
    }

    ExecutionReadinessStatus::Degraded
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorReportSystemSnapshot {
    pub api_healthy: bool,
    pub db_healthy: bool,
    pub kill_switch_active: bool,
    pub auth_enabled: bool,
    pub metrics_available: bool,
    pub uptime_seconds: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorReportMarketFeedSnapshot {
    pub symbol: String,
    pub status: String,
    pub freshness_status: String,
    pub last_event_age_seconds: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperatorReportMarketSnapshot {
    pub feeds: Vec<OperatorReportMarketFeedSnapshot>,
    pub stale_feed_count: i64,
    pub degraded_feed_count: i64,
    pub backfill_completed_count: i64,
    pub backfill_failed_count: i64,
    pub candle_count_in_window: i64,
    pub data_quality: Option<MarketDataQualityReport>,
    pub repair_completed_count: i64,
    pub repair_failed_count: i64,
    pub repair_partial_count: i64,
    pub repair_degraded_after_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorReportTopPairCount {
    pub strategy_id: String,
    pub symbol: String,
    pub count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperatorReportStrategySnapshot {
    pub total_strategy_evaluations: i64,
    pub total_signals: i64,
    pub risk_rejection_rate_pct: Decimal,
    pub strategy_analytics_summary: Option<StrategyPerformanceSummary>,
    pub top_rejected_pairs: Vec<OperatorReportTopPairCount>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorReportReasonCount {
    pub reason: String,
    pub count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorReportRiskSnapshot {
    pub approved_decisions: i64,
    pub rejected_decisions: i64,
    pub top_rejection_reasons: Vec<OperatorReportReasonCount>,
    pub kill_switch_change_count: i64,
    pub risk_config_version: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperatorReportPaperSnapshot {
    pub paper_equity: Decimal,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub daily_pnl: Decimal,
    pub open_position_count: i64,
    pub closed_position_count: i64,
    pub manual_close_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorReportShadowSnapshot {
    pub shadow_run_count: i64,
    pub would_submit_count: i64,
    pub no_signal_count: i64,
    pub risk_rejected_count: i64,
    pub skipped_count: i64,
    pub runner_status: String,
    pub runner_last_tick_age_seconds: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperatorReportPromotionSnapshot {
    pub shadow_would_submit_count: i64,
    pub previewed_count: i64,
    pub submitted_count: i64,
    pub acked_count: i64,
    pub filled_count: i64,
    pub reconciliation_required_count: i64,
    pub preview_rate_pct: Decimal,
    pub submit_rate_pct: Decimal,
    pub ack_rate_pct: Decimal,
    pub fill_rate_pct: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorReportTestnetSnapshot {
    pub testnet_orders_created: i64,
    pub active_order_count: i64,
    pub terminal_order_count: i64,
    pub unknown_order_count: i64,
    pub reconciliation_run_count: i64,
    pub mismatch_count: i64,
    pub repair_action_count: i64,
    pub private_stream_status: String,
    pub private_stream_last_event_age_seconds: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorReportResearchQualificationTopCandidate {
    pub candidate_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub status: ResearchCandidateQualificationStatus,
    pub score: i32,
    pub readiness_status: Option<ExecutionReadinessStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperatorReportResearchQualificationSnapshot {
    pub total_candidates: i64,
    pub accepted_for_shadow_count: i64,
    pub qualified_count: i64,
    pub needs_more_data_count: i64,
    pub not_qualified_count: i64,
    pub degraded_count: i64,
    pub unknown_count: i64,
    pub stale_observation_count: i64,
    pub runner_mismatch_count: i64,
    pub readiness_degraded_count: i64,
    pub readiness_not_ready_count: i64,
    pub degraded_or_not_ready_readiness_count: i64,
    pub below_default_threshold_override_count: i64,
    pub latest_evaluated_candidates_count: i64,
    pub newly_qualified_count: i64,
    pub lost_qualification_count: i64,
    pub needs_attention_count: i64,
    pub stale_evaluation_count: i64,
    pub reviews_in_window: i64,
    pub ready_for_testnet_review_count: i64,
    pub ready_for_testnet_review_dossier_count: i64,
    pub blocked_testnet_review_dossier_count: i64,
    pub marked_ready_but_dossier_not_ready_count: i64,
    pub dossier_needs_more_shadow_data_count: i64,
    pub rejected_from_watchlist_count: i64,
    pub archived_from_watchlist_count: i64,
    pub candidates_needing_review_count: i64,
    pub walk_forward_overfit_risk_count: i64,
    pub walk_forward_missing_count: i64,
    pub walk_forward_robust_count: i64,
    pub top_candidate: Option<OperatorReportResearchQualificationTopCandidate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperatorReport {
    pub report_id: Uuid,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub generated_at: DateTime<Utc>,
    pub status: OperatorReportStatus,
    pub summary: OperatorReportSummary,
    pub findings: Vec<OperatorReportFinding>,
    pub recommendations: Vec<OperatorReportRecommendation>,
    pub sections: Vec<OperatorReportSection>,
    pub format: OperatorReportFormat,
    pub persisted: bool,
    pub correlation_id: Uuid,
    pub markdown: Option<String>,
}

impl OperatorReport {
    pub fn with_markdown(mut self) -> Self {
        self.markdown = Some(self.render_markdown());
        self
    }

    pub fn render_markdown(&self) -> String {
        let mut lines = vec![
            "# Operator Daily Report".to_string(),
            String::new(),
            format!("- Report ID: `{}`", self.report_id),
            format!("- Status: `{}`", self.status.as_str()),
            format!(
                "- Window: `{}` to `{}`",
                self.window_start.to_rfc3339(),
                self.window_end.to_rfc3339()
            ),
            format!("- Generated At: `{}`", self.generated_at.to_rfc3339()),
            String::new(),
            "## Summary".to_string(),
            String::new(),
            format!("- Total findings: {}", self.summary.total_findings),
            format!(
                "- Highest severity: {}",
                self.summary
                    .highest_severity
                    .map(|value| value.as_str().to_string())
                    .unwrap_or_else(|| "NONE".to_string())
            ),
            format!(
                "- Kill switch active: {}",
                if self.summary.kill_switch_active {
                    "yes"
                } else {
                    "no"
                }
            ),
            format!("- Stale feeds: {}", self.summary.stale_feed_count),
            format!(
                "- Risk rejection rate: {}%",
                self.summary.risk_rejection_rate_pct.round_dp(2)
            ),
            format!(
                "- Paper daily PnL: {}",
                self.summary.paper_daily_pnl.round_dp(2)
            ),
            format!(
                "- Shadow WOULD_SUBMIT: {}",
                self.summary.shadow_would_submit_count
            ),
            format!(
                "- Reconciliation required: {}",
                self.summary.reconciliation_required_count
            ),
            String::new(),
            "## Findings".to_string(),
            String::new(),
        ];

        if self.findings.is_empty() {
            lines.push("- INFO: No findings.".to_string());
        } else {
            for finding in &self.findings {
                lines.push(format!(
                    "- {}: {}. {}",
                    finding.severity.as_str(),
                    finding.title,
                    finding.detail
                ));
            }
        }

        lines.push(String::new());
        lines.push("## Recommendations".to_string());
        lines.push(String::new());
        if self.recommendations.is_empty() {
            lines.push("- No operator actions recommended.".to_string());
        } else {
            for recommendation in &self.recommendations {
                lines.push(format!(
                    "- {}: {}",
                    recommendation.priority.as_str(),
                    recommendation.detail
                ));
            }
        }

        for section in &self.sections {
            lines.push(String::new());
            lines.push(format!("## {}", section.title));
            lines.push(String::new());
            lines.push(format!("- Status: `{}`", section.status.as_str()));
            lines.push(format!("- Summary: {}", section.summary));
            for highlight in &section.highlights {
                lines.push(format!("- {}: {}", highlight.label, highlight.value));
            }
        }

        lines.join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestnetPromotionFunnelStage {
    pub stage: String,
    pub count: i64,
    pub rate_pct: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestnetPromotionOutcomeBreakdown {
    pub outcome: String,
    pub count: i64,
    pub rate_pct: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestnetPromotionDropoffBreakdown {
    pub stage: String,
    pub dropped_count: i64,
    pub dropoff_rate_pct: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestnetPromotionLifecycleBreakdown {
    pub execution_state: String,
    pub count: i64,
    pub rate_pct: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestnetPromotionQualitySignal {
    pub signal: String,
    pub value_pct: Decimal,
    pub numerator: i64,
    pub denominator: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestnetPromotionFunnelRow {
    pub shadow_run_id: Uuid,
    pub promotion_id: Option<Uuid>,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub promotion_status: Option<String>,
    pub promotion_rejection_reasons: Vec<String>,
    pub testnet_order_id: Option<Uuid>,
    pub client_order_id: Option<String>,
    pub execution_state: Option<TestnetExecutionState>,
    pub linked_order_missing: bool,
    pub shadow_created_at: DateTime<Utc>,
    pub promotion_created_at: Option<DateTime<Utc>>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub acked_at: Option<DateTime<Utc>>,
    pub last_lifecycle_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestnetPromotionFunnelSummary {
    pub strategy_id: Option<String>,
    pub symbol: Option<String>,
    pub timeframe: Option<String>,
    pub window_start: Option<DateTime<Utc>>,
    pub window_end: Option<DateTime<Utc>>,
    pub shadow_would_submit_count: i64,
    pub promotion_previewed_count: i64,
    pub promotion_submitted_count: i64,
    pub promotion_rejected_count: i64,
    pub promotion_expired_count: i64,
    pub promotion_duplicate_rejected_count: i64,
    pub testnet_orders_created_count: i64,
    pub acked_count: i64,
    pub filled_count: i64,
    pub partially_filled_count: i64,
    pub cancelled_count: i64,
    pub rejected_count: i64,
    pub expired_count: i64,
    pub reconciliation_required_count: i64,
    pub unknown_exchange_state_count: i64,
    pub failed_count: i64,
    pub preview_rate_pct: Decimal,
    pub submit_rate_pct: Decimal,
    pub ack_rate_pct: Decimal,
    pub fill_rate_pct: Decimal,
    pub reconciliation_required_rate_pct: Decimal,
    pub avg_time_shadow_to_preview_seconds: Option<Decimal>,
    pub avg_time_preview_to_submit_seconds: Option<Decimal>,
    pub stages: Vec<TestnetPromotionFunnelStage>,
    pub outcome_breakdown: Vec<TestnetPromotionOutcomeBreakdown>,
    pub dropoff_breakdown: Vec<TestnetPromotionDropoffBreakdown>,
    pub lifecycle_breakdown: Vec<TestnetPromotionLifecycleBreakdown>,
    pub quality_signals: Vec<TestnetPromotionQualitySignal>,
    pub computed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyPerformanceMetric {
    pub name: String,
    pub value: Decimal,
    pub mode: StrategyPerformanceMode,
    pub computed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyComparisonSummary {
    pub strategy_id: String,
    pub symbol: Option<String>,
    pub timeframe: Option<String>,
    pub mode: StrategyPerformanceMode,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub risk_rejection_rate: Decimal,
    pub win_rate: Option<Decimal>,
    pub best_backtest_pnl_pct: Option<Decimal>,
    pub worst_backtest_pnl_pct: Option<Decimal>,
    pub avg_backtest_pnl_pct: Option<Decimal>,
    pub shadow_would_submit_count: i64,
    pub shadow_no_signal_count: i64,
    pub shadow_risk_rejected_count: i64,
    pub approved_risk_decisions: i64,
    pub rejected_risk_decisions: i64,
    pub paper_orders_count: i64,
    pub total_signals: i64,
    pub total_runs: i64,
    pub computed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyDecisionBreakdown {
    pub strategy_id: String,
    pub symbol: Option<String>,
    pub timeframe: Option<String>,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub total_runs: i64,
    pub would_submit_count: i64,
    pub no_signal_count: i64,
    pub risk_rejected_count: i64,
    pub skipped_count: i64,
    pub error_count: i64,
    pub computed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyRiskBreakdown {
    pub strategy_id: Option<String>,
    pub symbol: Option<String>,
    pub timeframe: Option<String>,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub approved_decisions: i64,
    pub rejected_decisions: i64,
    pub rejection_rate: Decimal,
    pub computed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyPnlBreakdown {
    pub strategy_id: Option<String>,
    pub symbol: Option<String>,
    pub timeframe: Option<String>,
    pub mode: StrategyPerformanceMode,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub positions_opened: i64,
    pub positions_closed: i64,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub win_rate: Option<Decimal>,
    pub avg_win: Option<Decimal>,
    pub avg_loss: Option<Decimal>,
    pub max_drawdown_pct: Option<Decimal>,
    pub computed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyPerformanceSummary {
    pub strategy_id: Option<String>,
    pub symbol: Option<String>,
    pub timeframe: Option<String>,
    pub mode: StrategyPerformanceMode,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub total_runs: i64,
    pub total_signals: i64,
    pub approved_risk_decisions: i64,
    pub rejected_risk_decisions: i64,
    pub risk_rejection_rate: Decimal,
    pub shadow_would_submit_count: i64,
    pub shadow_no_signal_count: i64,
    pub shadow_risk_rejected_count: i64,
    pub paper_orders_count: i64,
    pub paper_positions_opened: i64,
    pub paper_positions_closed: i64,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub win_rate: Option<Decimal>,
    pub avg_win: Option<Decimal>,
    pub avg_loss: Option<Decimal>,
    pub max_drawdown_pct: Option<Decimal>,
    pub backtest_runs_count: i64,
    pub best_backtest_pnl_pct: Option<Decimal>,
    pub worst_backtest_pnl_pct: Option<Decimal>,
    pub avg_backtest_pnl_pct: Option<Decimal>,
    pub created_at: DateTime<Utc>,
    pub computed_at: DateTime<Utc>,
}

pub fn calculate_strategy_rejection_rate(rejected: i64, total: i64) -> Decimal {
    if total <= 0 {
        Decimal::ZERO
    } else {
        Decimal::from(rejected) / Decimal::from(total)
    }
}

pub fn calculate_strategy_win_rate(wins: i64, total: i64) -> Option<Decimal> {
    if total <= 0 {
        None
    } else {
        Some(Decimal::from(wins) / Decimal::from(total))
    }
}

pub fn calculate_strategy_average_pnl(total: Decimal, count: i64) -> Option<Decimal> {
    if count <= 0 {
        None
    } else {
        Some(total / Decimal::from(count))
    }
}

pub fn calculate_testnet_promotion_rate(numerator: i64, denominator: i64) -> Decimal {
    if denominator <= 0 {
        Decimal::ZERO
    } else {
        ((Decimal::from(numerator) * Decimal::from(100)) / Decimal::from(denominator)).round_dp(2)
    }
}

pub fn calculate_average_duration_seconds(
    total_seconds: Decimal,
    sample_count: i64,
) -> Option<Decimal> {
    if sample_count <= 0 {
        None
    } else {
        Some((total_seconds / Decimal::from(sample_count)).round_dp(2))
    }
}

pub fn combine_strategy_performance_summaries(
    mut summaries: Vec<StrategyPerformanceSummary>,
) -> Option<StrategyPerformanceSummary> {
    let first = summaries.pop()?;
    let mut combined = first.clone();
    combined.mode = StrategyPerformanceMode::Combined;
    let mut total_winning_outcomes = match (first.win_rate, first.paper_positions_closed) {
        (Some(rate), count) if count > 0 => rate * Decimal::from(count),
        _ => Decimal::ZERO,
    };
    let mut total_avg_win_sum = match (first.avg_win, first.paper_positions_closed) {
        (Some(avg), count) if count > 0 => avg * Decimal::from(count),
        _ => Decimal::ZERO,
    };
    let mut total_avg_loss_sum = match (first.avg_loss, first.paper_positions_closed) {
        (Some(avg), count) if count > 0 => avg * Decimal::from(count),
        _ => Decimal::ZERO,
    };
    let mut avg_win_count = if first.avg_win.is_some() {
        first.paper_positions_closed
    } else {
        0
    };
    let mut avg_loss_count = if first.avg_loss.is_some() {
        first.paper_positions_closed
    } else {
        0
    };
    let mut avg_backtest_sum = match (first.avg_backtest_pnl_pct, first.backtest_runs_count) {
        (Some(avg), count) if count > 0 => avg * Decimal::from(count),
        _ => Decimal::ZERO,
    };

    for summary in summaries {
        combined.window_start = combined.window_start.min(summary.window_start);
        combined.window_end = combined.window_end.max(summary.window_end);
        combined.total_runs += summary.total_runs;
        combined.total_signals += summary.total_signals;
        combined.approved_risk_decisions += summary.approved_risk_decisions;
        combined.rejected_risk_decisions += summary.rejected_risk_decisions;
        combined.shadow_would_submit_count += summary.shadow_would_submit_count;
        combined.shadow_no_signal_count += summary.shadow_no_signal_count;
        combined.shadow_risk_rejected_count += summary.shadow_risk_rejected_count;
        combined.paper_orders_count += summary.paper_orders_count;
        combined.paper_positions_opened += summary.paper_positions_opened;
        combined.paper_positions_closed += summary.paper_positions_closed;
        combined.realized_pnl += summary.realized_pnl;
        combined.unrealized_pnl += summary.unrealized_pnl;
        combined.backtest_runs_count += summary.backtest_runs_count;
        combined.created_at = combined.created_at.min(summary.created_at);
        combined.computed_at = combined.computed_at.max(summary.computed_at);
        if let Some(rate) = summary.win_rate {
            total_winning_outcomes += rate * Decimal::from(summary.paper_positions_closed);
        }
        if let Some(avg) = summary.avg_win {
            total_avg_win_sum += avg * Decimal::from(summary.paper_positions_closed);
            avg_win_count += summary.paper_positions_closed;
        }
        if let Some(avg) = summary.avg_loss {
            total_avg_loss_sum += avg * Decimal::from(summary.paper_positions_closed);
            avg_loss_count += summary.paper_positions_closed;
        }
        if let Some(avg) = summary.avg_backtest_pnl_pct {
            avg_backtest_sum += avg * Decimal::from(summary.backtest_runs_count);
        }
        combined.max_drawdown_pct = match (combined.max_drawdown_pct, summary.max_drawdown_pct) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        };
        combined.best_backtest_pnl_pct = match (
            combined.best_backtest_pnl_pct,
            summary.best_backtest_pnl_pct,
        ) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        };
        combined.worst_backtest_pnl_pct = match (
            combined.worst_backtest_pnl_pct,
            summary.worst_backtest_pnl_pct,
        ) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        };
    }

    let total_risk_decisions = combined.approved_risk_decisions + combined.rejected_risk_decisions;
    combined.risk_rejection_rate =
        calculate_strategy_rejection_rate(combined.rejected_risk_decisions, total_risk_decisions);
    combined.win_rate = if combined.paper_positions_closed > 0 {
        Some(total_winning_outcomes / Decimal::from(combined.paper_positions_closed))
    } else {
        None
    };
    combined.avg_win = calculate_strategy_average_pnl(total_avg_win_sum, avg_win_count);
    combined.avg_loss = calculate_strategy_average_pnl(total_avg_loss_sum, avg_loss_count);
    combined.avg_backtest_pnl_pct =
        calculate_strategy_average_pnl(avg_backtest_sum, combined.backtest_runs_count);

    Some(combined)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExchangeOrderSide {
    Buy,
    Sell,
}

impl ExchangeOrderSide {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExchangeOrderType {
    Market,
    Limit,
}

impl ExchangeOrderType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Market => "MARKET",
            Self::Limit => "LIMIT",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExchangeOrderTimeInForce {
    Gtc,
    Ioc,
    Fok,
}

impl ExchangeOrderTimeInForce {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gtc => "GTC",
            Self::Ioc => "IOC",
            Self::Fok => "FOK",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeRequestMode {
    Signed,
    Public,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExchangeOrderState {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    PendingCancel,
    Rejected,
    Expired,
}

impl ExchangeOrderState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::New => "NEW",
            Self::PartiallyFilled => "PARTIALLY_FILLED",
            Self::Filled => "FILLED",
            Self::Canceled => "CANCELED",
            Self::PendingCancel => "PENDING_CANCEL",
            Self::Rejected => "REJECTED",
            Self::Expired => "EXPIRED",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TestnetExecutionState {
    IntentCreated,
    RiskApproved,
    OrderPrepared,
    OrderSubmitRequested,
    ExchangeAcked,
    New,
    PartiallyFilled,
    Filled,
    CancelRequested,
    Cancelled,
    Rejected,
    Expired,
    ReconciliationRequired,
    UnknownExchangeState,
    Failed,
}

impl TestnetExecutionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::IntentCreated => "INTENT_CREATED",
            Self::RiskApproved => "RISK_APPROVED",
            Self::OrderPrepared => "ORDER_PREPARED",
            Self::OrderSubmitRequested => "ORDER_SUBMIT_REQUESTED",
            Self::ExchangeAcked => "EXCHANGE_ACKED",
            Self::New => "NEW",
            Self::PartiallyFilled => "PARTIALLY_FILLED",
            Self::Filled => "FILLED",
            Self::CancelRequested => "CANCEL_REQUESTED",
            Self::Cancelled => "CANCELLED",
            Self::Rejected => "REJECTED",
            Self::Expired => "EXPIRED",
            Self::ReconciliationRequired => "RECONCILIATION_REQUIRED",
            Self::UnknownExchangeState => "UNKNOWN_EXCHANGE_STATE",
            Self::Failed => "FAILED",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Cancelled | Self::Rejected | Self::Expired | Self::Failed
        )
    }
}

impl std::str::FromStr for TestnetExecutionState {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "INTENT_CREATED" => Ok(Self::IntentCreated),
            "RISK_APPROVED" => Ok(Self::RiskApproved),
            "ORDER_PREPARED" => Ok(Self::OrderPrepared),
            "ORDER_SUBMIT_REQUESTED" => Ok(Self::OrderSubmitRequested),
            "EXCHANGE_ACKED" => Ok(Self::ExchangeAcked),
            "NEW" => Ok(Self::New),
            "PARTIALLY_FILLED" => Ok(Self::PartiallyFilled),
            "FILLED" => Ok(Self::Filled),
            "CANCEL_REQUESTED" => Ok(Self::CancelRequested),
            "CANCELLED" => Ok(Self::Cancelled),
            "REJECTED" => Ok(Self::Rejected),
            "EXPIRED" => Ok(Self::Expired),
            "RECONCILIATION_REQUIRED" => Ok(Self::ReconciliationRequired),
            "UNKNOWN_EXCHANGE_STATE" => Ok(Self::UnknownExchangeState),
            "FAILED" => Ok(Self::Failed),
            other => Err(CoreError::UnsupportedTestnetExecutionState(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TestnetExecutionTransitionSource {
    ApiSubmit,
    ExchangeAck,
    PrivateStream,
    RestReconciliation,
    ApiCancel,
    ExchangeCancelAck,
    OperatorMarkReconciliationRequired,
}

impl TestnetExecutionTransitionSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ApiSubmit => "API_SUBMIT",
            Self::ExchangeAck => "EXCHANGE_ACK",
            Self::PrivateStream => "PRIVATE_STREAM",
            Self::RestReconciliation => "REST_RECONCILIATION",
            Self::ApiCancel => "API_CANCEL",
            Self::ExchangeCancelAck => "EXCHANGE_CANCEL_ACK",
            Self::OperatorMarkReconciliationRequired => "OPERATOR_MARK_RECONCILIATION_REQUIRED",
        }
    }
}

impl std::str::FromStr for TestnetExecutionTransitionSource {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "API_SUBMIT" => Ok(Self::ApiSubmit),
            "EXCHANGE_ACK" => Ok(Self::ExchangeAck),
            "PRIVATE_STREAM" => Ok(Self::PrivateStream),
            "REST_RECONCILIATION" => Ok(Self::RestReconciliation),
            "API_CANCEL" => Ok(Self::ApiCancel),
            "EXCHANGE_CANCEL_ACK" => Ok(Self::ExchangeCancelAck),
            "OPERATOR_MARK_RECONCILIATION_REQUIRED" => Ok(Self::OperatorMarkReconciliationRequired),
            other => Err(CoreError::UnsupportedTestnetExecutionTransitionSource(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestnetOrderLifecycleSnapshot {
    pub order_id: Option<Uuid>,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub current_state: TestnetExecutionState,
    pub last_transition_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestnetExecutionLifecycleEvent {
    pub id: Uuid,
    pub order_id: Option<Uuid>,
    pub client_order_id: String,
    pub previous_state: Option<TestnetExecutionState>,
    pub next_state: TestnetExecutionState,
    pub transition_source: TestnetExecutionTransitionSource,
    pub reason: Option<String>,
    pub payload: Option<Value>,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestnetExecutionTransition {
    pub previous_state: Option<TestnetExecutionState>,
    pub next_state: TestnetExecutionState,
    pub source: TestnetExecutionTransitionSource,
    pub reason: Option<String>,
    pub payload: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestnetExecutionTransitionResult {
    pub previous_state: Option<TestnetExecutionState>,
    pub next_state: TestnetExecutionState,
    pub source: TestnetExecutionTransitionSource,
    pub accepted: bool,
    pub terminal: bool,
    pub requires_reconciliation: bool,
}

#[derive(Debug, Clone, Error, Serialize, Deserialize, PartialEq, Eq)]
pub enum TestnetExecutionStateError {
    #[error(
        "invalid transition from {previous_state:?} to {next_state:?} via {transition_source:?}"
    )]
    InvalidTransition {
        previous_state: Option<TestnetExecutionState>,
        next_state: TestnetExecutionState,
        transition_source: TestnetExecutionTransitionSource,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TestnetRepairAction {
    MarkReconciliationRequired,
    ManualRecheck,
    MarkAcked,
    MarkCancelled,
    MarkRejected,
    MarkFailed,
    SafeCancelRequest,
}

impl TestnetRepairAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MarkReconciliationRequired => "MARK_RECONCILIATION_REQUIRED",
            Self::ManualRecheck => "MANUAL_RECHECK",
            Self::MarkAcked => "MARK_ACKED",
            Self::MarkCancelled => "MARK_CANCELLED",
            Self::MarkRejected => "MARK_REJECTED",
            Self::MarkFailed => "MARK_FAILED",
            Self::SafeCancelRequest => "SAFE_CANCEL_REQUEST",
        }
    }

    pub fn required_confirmation_text(self, client_order_id: &str) -> String {
        match self {
            Self::SafeCancelRequest => format!("CANCEL TESTNET {client_order_id}"),
            _ => format!("REPAIR TESTNET {client_order_id}"),
        }
    }

    pub fn requires_owner(self) -> bool {
        matches!(
            self,
            Self::MarkAcked
                | Self::MarkCancelled
                | Self::MarkRejected
                | Self::MarkFailed
                | Self::SafeCancelRequest
        )
    }

    pub fn allows_operator(self) -> bool {
        matches!(self, Self::MarkReconciliationRequired | Self::ManualRecheck)
    }

    pub fn is_dangerous(self) -> bool {
        !self.allows_operator()
    }
}

impl std::str::FromStr for TestnetRepairAction {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "MARK_RECONCILIATION_REQUIRED" => Ok(Self::MarkReconciliationRequired),
            "MANUAL_RECHECK" => Ok(Self::ManualRecheck),
            "MARK_ACKED" => Ok(Self::MarkAcked),
            "MARK_CANCELLED" => Ok(Self::MarkCancelled),
            "MARK_REJECTED" => Ok(Self::MarkRejected),
            "MARK_FAILED" => Ok(Self::MarkFailed),
            "SAFE_CANCEL_REQUEST" => Ok(Self::SafeCancelRequest),
            other => Err(CoreError::UnsupportedTestnetRepairAction(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TestnetRepairActionStatus {
    Applied,
    Rejected,
    Failed,
}

impl TestnetRepairActionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "APPLIED",
            Self::Rejected => "REJECTED",
            Self::Failed => "FAILED",
        }
    }
}

impl std::str::FromStr for TestnetRepairActionStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "APPLIED" => Ok(Self::Applied),
            "REJECTED" => Ok(Self::Rejected),
            "FAILED" => Ok(Self::Failed),
            other => Err(CoreError::UnsupportedTestnetRepairActionStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestnetRepairValidationIssue {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestnetRepairRequest {
    pub action: TestnetRepairAction,
    pub confirmation_text: String,
    pub reason: Option<String>,
    pub force: bool,
    pub correlation_id: Option<Uuid>,
}

impl TestnetRepairRequest {
    pub fn validate_confirmation(&self, client_order_id: &str) -> Result<(), CoreError> {
        let expected = self.action.required_confirmation_text(client_order_id);
        if self.confirmation_text == expected {
            return Ok(());
        }
        Err(CoreError::InvalidTestnetRepairConfirmation {
            expected,
            actual: self.confirmation_text.clone(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestnetRepairResult {
    pub client_order_id: String,
    pub action: TestnetRepairAction,
    pub status: TestnetRepairActionStatus,
    pub previous_state: Option<TestnetExecutionState>,
    pub next_state: Option<TestnetExecutionState>,
    pub correlation_id: Uuid,
    pub issues: Vec<TestnetRepairValidationIssue>,
}

pub fn expected_testnet_pipeline_confirmation(symbol: &str) -> String {
    format!("SUBMIT TESTNET {}", symbol.trim().to_ascii_uppercase())
}

pub fn is_valid_testnet_pipeline_confirmation(symbol: &str, confirmation_text: &str) -> bool {
    confirmation_text == expected_testnet_pipeline_confirmation(symbol)
}

pub fn expected_testnet_shadow_promotion_confirmation(symbol: &str) -> String {
    format!("PROMOTE TESTNET {}", symbol.trim().to_ascii_uppercase())
}

pub fn is_valid_testnet_shadow_promotion_confirmation(
    symbol: &str,
    confirmation_text: &str,
) -> bool {
    confirmation_text == expected_testnet_shadow_promotion_confirmation(symbol)
}

pub fn validate_testnet_repair_transition(
    action: TestnetRepairAction,
    previous: TestnetExecutionState,
    next: Option<TestnetExecutionState>,
    force: bool,
) -> Result<(), CoreError> {
    if previous == TestnetExecutionState::Filled {
        match next {
            Some(TestnetExecutionState::Filled) | None => {}
            _ => {
                return Err(CoreError::InvalidTestnetRepairTransition {
                    action,
                    previous_state: previous,
                    next_state: next,
                });
            }
        }
    }

    match action {
        TestnetRepairAction::MarkReconciliationRequired => {
            if previous.is_terminal() && previous != TestnetExecutionState::Failed {
                return Err(CoreError::InvalidTestnetRepairTransition {
                    action,
                    previous_state: previous,
                    next_state: next,
                });
            }
            if next != Some(TestnetExecutionState::ReconciliationRequired) {
                return Err(CoreError::InvalidTestnetRepairTransition {
                    action,
                    previous_state: previous,
                    next_state: next,
                });
            }
        }
        TestnetRepairAction::ManualRecheck => {
            if next.is_none() {
                return Err(CoreError::InvalidTestnetRepairTransition {
                    action,
                    previous_state: previous,
                    next_state: next,
                });
            }
        }
        TestnetRepairAction::MarkAcked => {
            let allowed = matches!(
                previous,
                TestnetExecutionState::OrderSubmitRequested
                    | TestnetExecutionState::New
                    | TestnetExecutionState::ReconciliationRequired
                    | TestnetExecutionState::UnknownExchangeState
                    | TestnetExecutionState::Failed
            );
            if !allowed || next != Some(TestnetExecutionState::ExchangeAcked) {
                return Err(CoreError::InvalidTestnetRepairTransition {
                    action,
                    previous_state: previous,
                    next_state: next,
                });
            }
        }
        TestnetRepairAction::MarkCancelled => {
            if next != Some(TestnetExecutionState::Cancelled) {
                return Err(CoreError::InvalidTestnetRepairTransition {
                    action,
                    previous_state: previous,
                    next_state: next,
                });
            }
            if previous == TestnetExecutionState::Filled && !force {
                return Err(CoreError::InvalidTestnetRepairTransition {
                    action,
                    previous_state: previous,
                    next_state: next,
                });
            }
        }
        TestnetRepairAction::MarkRejected => {
            if next != Some(TestnetExecutionState::Rejected) {
                return Err(CoreError::InvalidTestnetRepairTransition {
                    action,
                    previous_state: previous,
                    next_state: next,
                });
            }
        }
        TestnetRepairAction::MarkFailed => {
            if next != Some(TestnetExecutionState::Failed) {
                return Err(CoreError::InvalidTestnetRepairTransition {
                    action,
                    previous_state: previous,
                    next_state: next,
                });
            }
        }
        TestnetRepairAction::SafeCancelRequest => {
            if next != Some(TestnetExecutionState::CancelRequested) {
                return Err(CoreError::InvalidTestnetRepairTransition {
                    action,
                    previous_state: previous,
                    next_state: next,
                });
            }
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExchangePrivateStreamStatus {
    Connecting,
    Connected,
    Stale,
    Disconnected,
    Error,
}

impl ExchangePrivateStreamStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connecting => "CONNECTING",
            Self::Connected => "CONNECTED",
            Self::Stale => "STALE",
            Self::Disconnected => "DISCONNECTED",
            Self::Error => "ERROR",
        }
    }
}

impl std::str::FromStr for ExchangePrivateStreamStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "CONNECTING" => Ok(Self::Connecting),
            "CONNECTED" => Ok(Self::Connected),
            "STALE" => Ok(Self::Stale),
            "DISCONNECTED" => Ok(Self::Disconnected),
            "ERROR" => Ok(Self::Error),
            other => Err(CoreError::UnsupportedExchangePrivateStreamStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExchangePrivateStreamSource {
    Websocket,
    ListenKeyLifecycle,
    Runtime,
}

impl ExchangePrivateStreamSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Websocket => "websocket",
            Self::ListenKeyLifecycle => "listen_key_lifecycle",
            Self::Runtime => "runtime",
        }
    }
}

impl std::str::FromStr for ExchangePrivateStreamSource {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "websocket" => Ok(Self::Websocket),
            "listen_key_lifecycle" => Ok(Self::ListenKeyLifecycle),
            "runtime" => Ok(Self::Runtime),
            other => Err(CoreError::UnsupportedExchangePrivateStreamSource(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExchangeExecutionReportType {
    New,
    Canceled,
    Replaced,
    Rejected,
    Trade,
    Expired,
    TradePrevention,
    Unknown,
}

impl ExchangeExecutionReportType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::New => "NEW",
            Self::Canceled => "CANCELED",
            Self::Replaced => "REPLACED",
            Self::Rejected => "REJECTED",
            Self::Trade => "TRADE",
            Self::Expired => "EXPIRED",
            Self::TradePrevention => "TRADE_PREVENTION",
            Self::Unknown => "UNKNOWN",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExchangeExecutionStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    PendingCancel,
    Rejected,
    Expired,
    ExpiredInMatch,
    Unknown,
}

impl ExchangeExecutionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::New => "NEW",
            Self::PartiallyFilled => "PARTIALLY_FILLED",
            Self::Filled => "FILLED",
            Self::Canceled => "CANCELED",
            Self::PendingCancel => "PENDING_CANCEL",
            Self::Rejected => "REJECTED",
            Self::Expired => "EXPIRED",
            Self::ExpiredInMatch => "EXPIRED_IN_MATCH",
            Self::Unknown => "UNKNOWN",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangeFillEvent {
    pub last_executed_qty: Decimal,
    pub last_executed_price: Decimal,
    pub commission_amount: Option<Decimal>,
    pub commission_asset: Option<String>,
    pub transaction_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangeExecutionReport {
    pub exchange: ExchangeName,
    pub environment: ExchangeEnvironment,
    pub symbol: String,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub side: ExchangeOrderSide,
    pub order_type: ExchangeOrderType,
    pub time_in_force: Option<ExchangeOrderTimeInForce>,
    pub order_status: ExchangeExecutionStatus,
    pub execution_type: ExchangeExecutionReportType,
    pub last_executed_qty: Decimal,
    pub cumulative_filled_qty: Decimal,
    pub last_executed_price: Decimal,
    pub commission_amount: Option<Decimal>,
    pub commission_asset: Option<String>,
    pub event_time: DateTime<Utc>,
    pub transaction_time: Option<DateTime<Utc>>,
    pub raw_payload: Value,
}

impl ExchangeExecutionReport {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.environment == ExchangeEnvironment::Live {
            return Err(CoreError::LiveExchangeEnvironmentRejected);
        }
        if self.client_order_id.trim().is_empty() {
            return Err(CoreError::EmptyClientOrderId);
        }
        Ok(())
    }

    pub fn fill_event(&self) -> Option<ExchangeFillEvent> {
        if self.execution_type != ExchangeExecutionReportType::Trade
            || self.last_executed_qty <= Decimal::ZERO
        {
            return None;
        }

        Some(ExchangeFillEvent {
            last_executed_qty: self.last_executed_qty,
            last_executed_price: self.last_executed_price,
            commission_amount: self.commission_amount,
            commission_asset: self.commission_asset.clone(),
            transaction_time: self.transaction_time,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExchangeListenKeyStatus {
    Missing,
    Active,
    Expired,
    Closing,
    Closed,
}

impl ExchangeListenKeyStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "MISSING",
            Self::Active => "ACTIVE",
            Self::Expired => "EXPIRED",
            Self::Closing => "CLOSING",
            Self::Closed => "CLOSED",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExchangePrivateStreamState {
    pub exchange: ExchangeName,
    pub environment: ExchangeEnvironment,
    pub status: ExchangePrivateStreamStatus,
    pub listen_key_hash: Option<String>,
    pub connected_at: Option<DateTime<Utc>>,
    pub last_event_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub reconnect_count: i32,
    pub updated_at: DateTime<Utc>,
}

impl ExchangePrivateStreamState {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.environment == ExchangeEnvironment::Live {
            return Err(CoreError::LiveExchangeEnvironmentRejected);
        }
        if self.reconnect_count < 0 {
            return Err(CoreError::InvalidExchangeReconnectCount(
                self.reconnect_count,
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangePrivateStreamEvent {
    pub exchange: ExchangeName,
    pub environment: ExchangeEnvironment,
    pub source: ExchangePrivateStreamSource,
    pub event_type: String,
    pub symbol: Option<String>,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub execution_type: Option<ExchangeExecutionReportType>,
    pub order_status: Option<ExchangeExecutionStatus>,
    pub event_time: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub raw_payload: Value,
}

impl ExchangePrivateStreamEvent {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.environment == ExchangeEnvironment::Live {
            return Err(CoreError::LiveExchangeEnvironmentRejected);
        }
        if self.event_type.trim().is_empty() {
            return Err(CoreError::InvalidExchangePrivateStreamEventType);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangeOrderRequest {
    pub exchange: ExchangeName,
    pub environment: ExchangeEnvironment,
    pub symbol: Symbol,
    pub side: ExchangeOrderSide,
    pub order_type: ExchangeOrderType,
    pub time_in_force: Option<ExchangeOrderTimeInForce>,
    pub quantity: Option<Decimal>,
    pub quote_notional: Option<Decimal>,
    pub limit_price: Option<Decimal>,
    pub client_order_id: String,
    pub recv_window_ms: Option<u64>,
    pub risk_decision_id: Option<Uuid>,
}

impl ExchangeOrderRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.client_order_id.trim().is_empty() {
            return Err(CoreError::EmptyClientOrderId);
        }
        if self.environment == ExchangeEnvironment::Live {
            return Err(CoreError::LiveExchangeEnvironmentRejected);
        }
        match (self.quantity, self.quote_notional) {
            (Some(quantity), None) => {
                if quantity <= Decimal::ZERO {
                    return Err(CoreError::InvalidExchangeQuantity);
                }
            }
            (None, Some(notional)) => {
                if notional <= Decimal::ZERO {
                    return Err(CoreError::InvalidExchangeNotional);
                }
            }
            (Some(quantity), Some(notional)) => {
                if quantity <= Decimal::ZERO {
                    return Err(CoreError::InvalidExchangeQuantity);
                }
                if notional <= Decimal::ZERO {
                    return Err(CoreError::InvalidExchangeNotional);
                }
            }
            (None, None) => return Err(CoreError::MissingExchangeQuantityOrNotional),
        }
        if self.order_type == ExchangeOrderType::Limit {
            let Some(limit_price) = self.limit_price else {
                return Err(CoreError::MissingExchangeLimitPrice);
            };
            if limit_price <= Decimal::ZERO {
                return Err(CoreError::InvalidExchangeLimitPrice);
            }
            if self.time_in_force.is_none() {
                return Err(CoreError::MissingExchangeTimeInForce);
            }
        }
        if let Some(limit_price) = self.limit_price {
            if limit_price <= Decimal::ZERO {
                return Err(CoreError::InvalidExchangeLimitPrice);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TestnetShadowDecision {
    NoSignal,
    RiskRejected,
    WouldSubmit,
    SkippedDisabledStrategy,
    SkippedKillSwitch,
    SkippedStalePrice,
    SkippedStaleFeed,
    Error,
}

impl TestnetShadowDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoSignal => "NO_SIGNAL",
            Self::RiskRejected => "RISK_REJECTED",
            Self::WouldSubmit => "WOULD_SUBMIT",
            Self::SkippedDisabledStrategy => "SKIPPED_DISABLED_STRATEGY",
            Self::SkippedKillSwitch => "SKIPPED_KILL_SWITCH",
            Self::SkippedStalePrice => "SKIPPED_STALE_PRICE",
            Self::SkippedStaleFeed => "SKIPPED_STALE_FEED",
            Self::Error => "ERROR",
        }
    }
}

impl std::str::FromStr for TestnetShadowDecision {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "NO_SIGNAL" => Ok(Self::NoSignal),
            "RISK_REJECTED" => Ok(Self::RiskRejected),
            "WOULD_SUBMIT" => Ok(Self::WouldSubmit),
            "SKIPPED_DISABLED_STRATEGY" => Ok(Self::SkippedDisabledStrategy),
            "SKIPPED_KILL_SWITCH" => Ok(Self::SkippedKillSwitch),
            "SKIPPED_STALE_PRICE" => Ok(Self::SkippedStalePrice),
            "SKIPPED_STALE_FEED" => Ok(Self::SkippedStaleFeed),
            "ERROR" => Ok(Self::Error),
            other => Err(CoreError::UnsupportedShadowDecision(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TestnetShadowStatus {
    Completed,
    Rejected,
    Error,
}

impl TestnetShadowStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "COMPLETED",
            Self::Rejected => "REJECTED",
            Self::Error => "ERROR",
        }
    }
}

impl std::str::FromStr for TestnetShadowStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "COMPLETED" => Ok(Self::Completed),
            "REJECTED" => Ok(Self::Rejected),
            "ERROR" => Ok(Self::Error),
            other => Err(CoreError::UnsupportedShadowStatus(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TestnetShadowRejectionReason {
    NoSignal,
    ConditionsNotMet,
    InsufficientHistory,
    StrategyDisabled,
    KillSwitchActive,
    StalePrice,
    StaleFeed,
    MarketFeedUnavailable,
    MarketFeedDegraded,
    UnsupportedTimeframe,
    InvalidPrice,
    RiskRejected,
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
    Error,
}

impl TestnetShadowRejectionReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoSignal => "no_signal",
            Self::ConditionsNotMet => "conditions_not_met",
            Self::InsufficientHistory => "insufficient_history",
            Self::StrategyDisabled => "strategy_disabled",
            Self::KillSwitchActive => "kill_switch_active",
            Self::StalePrice => "stale_price",
            Self::StaleFeed => "stale_feed",
            Self::MarketFeedUnavailable => "market_feed_unavailable",
            Self::MarketFeedDegraded => "market_feed_degraded",
            Self::UnsupportedTimeframe => "unsupported_timeframe",
            Self::InvalidPrice => "invalid_price",
            Self::RiskRejected => "risk_rejected",
            Self::MaxOpenPositionsExceeded => "max_open_positions_exceeded",
            Self::MaxDailyLossExceeded => "max_daily_loss_exceeded",
            Self::MaxWeeklyLossExceeded => "max_weekly_loss_exceeded",
            Self::MaxConsecutiveLossesExceeded => "max_consecutive_losses_exceeded",
            Self::SignalTooOld => "signal_too_old",
            Self::DuplicateOrderDetected => "duplicate_order_detected",
            Self::DataStale => "data_stale",
            Self::PositionNotionalExceeded => "position_notional_exceeded",
            Self::CooldownActive => "cooldown_active",
            Self::UnsupportedState => "unsupported_state",
            Self::Error => "error",
        }
    }
}

impl std::str::FromStr for TestnetShadowRejectionReason {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "no_signal" => Ok(Self::NoSignal),
            "conditions_not_met" => Ok(Self::ConditionsNotMet),
            "insufficient_history" => Ok(Self::InsufficientHistory),
            "strategy_disabled" => Ok(Self::StrategyDisabled),
            "kill_switch_active" => Ok(Self::KillSwitchActive),
            "stale_price" => Ok(Self::StalePrice),
            "stale_feed" => Ok(Self::StaleFeed),
            "market_feed_unavailable" => Ok(Self::MarketFeedUnavailable),
            "market_feed_degraded" => Ok(Self::MarketFeedDegraded),
            "unsupported_timeframe" => Ok(Self::UnsupportedTimeframe),
            "invalid_price" => Ok(Self::InvalidPrice),
            "risk_rejected" => Ok(Self::RiskRejected),
            "max_open_positions_exceeded" => Ok(Self::MaxOpenPositionsExceeded),
            "max_daily_loss_exceeded" => Ok(Self::MaxDailyLossExceeded),
            "max_weekly_loss_exceeded" => Ok(Self::MaxWeeklyLossExceeded),
            "max_consecutive_losses_exceeded" => Ok(Self::MaxConsecutiveLossesExceeded),
            "signal_too_old" => Ok(Self::SignalTooOld),
            "duplicate_order_detected" => Ok(Self::DuplicateOrderDetected),
            "data_stale" => Ok(Self::DataStale),
            "position_notional_exceeded" => Ok(Self::PositionNotionalExceeded),
            "cooldown_active" => Ok(Self::CooldownActive),
            "unsupported_state" => Ok(Self::UnsupportedState),
            "error" => Ok(Self::Error),
            other => Err(CoreError::UnsupportedShadowRejectionReason(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestnetShadowIntent {
    pub exchange: ExchangeName,
    pub environment: ExchangeEnvironment,
    pub symbol: Symbol,
    pub side: ExchangeOrderSide,
    pub order_type: ExchangeOrderType,
    pub time_in_force: Option<ExchangeOrderTimeInForce>,
    pub quantity: Option<Decimal>,
    pub quote_notional: Option<Decimal>,
    pub limit_price: Option<Decimal>,
    pub risk_decision_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestnetShadowModeConfig {
    pub stale_price_threshold_seconds: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestnetShadowRunRequest {
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestnetShadowRunResult {
    pub run_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub decision: TestnetShadowDecision,
    pub signal_id: Option<Uuid>,
    pub risk_decision_id: Option<Uuid>,
    pub would_submit_order: Option<TestnetShadowIntent>,
    pub reasons: Vec<TestnetShadowRejectionReason>,
    pub price_source: Option<String>,
    pub resolved_price: Option<Decimal>,
    pub status: TestnetShadowStatus,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TestnetShadowPromotionStatus {
    Previewed,
    Submitted,
    Rejected,
    Expired,
    AlreadyPromoted,
}

impl TestnetShadowPromotionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Previewed => "PREVIEWED",
            Self::Submitted => "SUBMITTED",
            Self::Rejected => "REJECTED",
            Self::Expired => "EXPIRED",
            Self::AlreadyPromoted => "ALREADY_PROMOTED",
        }
    }
}

impl std::str::FromStr for TestnetShadowPromotionStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "PREVIEWED" => Ok(Self::Previewed),
            "SUBMITTED" => Ok(Self::Submitted),
            "REJECTED" => Ok(Self::Rejected),
            "EXPIRED" => Ok(Self::Expired),
            "ALREADY_PROMOTED" => Ok(Self::AlreadyPromoted),
            other => Err(CoreError::UnsupportedShadowPromotionStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TestnetShadowPromotionRejectionReason {
    ShadowRunNotFound,
    ShadowDecisionNotWouldSubmit,
    MissingRiskDecision,
    RiskDecisionNotApproved,
    KillSwitchActive,
    StrategyDisabled,
    StalePrice,
    InvalidEnvironment,
    MissingWouldSubmitPayload,
    PromotionExpired,
    DuplicateSubmit,
    InvalidConfirmation,
    OwnerRequired,
    AlreadyPromoted,
    SubmitFailed,
    Error,
}

impl TestnetShadowPromotionRejectionReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ShadowRunNotFound => "shadow_run_not_found",
            Self::ShadowDecisionNotWouldSubmit => "shadow_decision_not_would_submit",
            Self::MissingRiskDecision => "missing_risk_decision",
            Self::RiskDecisionNotApproved => "risk_decision_not_approved",
            Self::KillSwitchActive => "kill_switch_active",
            Self::StrategyDisabled => "strategy_disabled",
            Self::StalePrice => "stale_price",
            Self::InvalidEnvironment => "invalid_environment",
            Self::MissingWouldSubmitPayload => "missing_would_submit_payload",
            Self::PromotionExpired => "promotion_expired",
            Self::DuplicateSubmit => "duplicate_submit",
            Self::InvalidConfirmation => "invalid_confirmation",
            Self::OwnerRequired => "owner_required",
            Self::AlreadyPromoted => "already_promoted",
            Self::SubmitFailed => "submit_failed",
            Self::Error => "error",
        }
    }
}

impl std::str::FromStr for TestnetShadowPromotionRejectionReason {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "shadow_run_not_found" => Ok(Self::ShadowRunNotFound),
            "shadow_decision_not_would_submit" => Ok(Self::ShadowDecisionNotWouldSubmit),
            "missing_risk_decision" => Ok(Self::MissingRiskDecision),
            "risk_decision_not_approved" => Ok(Self::RiskDecisionNotApproved),
            "kill_switch_active" => Ok(Self::KillSwitchActive),
            "strategy_disabled" => Ok(Self::StrategyDisabled),
            "stale_price" => Ok(Self::StalePrice),
            "invalid_environment" => Ok(Self::InvalidEnvironment),
            "missing_would_submit_payload" => Ok(Self::MissingWouldSubmitPayload),
            "promotion_expired" => Ok(Self::PromotionExpired),
            "duplicate_submit" => Ok(Self::DuplicateSubmit),
            "invalid_confirmation" => Ok(Self::InvalidConfirmation),
            "owner_required" => Ok(Self::OwnerRequired),
            "already_promoted" => Ok(Self::AlreadyPromoted),
            "submit_failed" => Ok(Self::SubmitFailed),
            "error" => Ok(Self::Error),
            other => Err(CoreError::UnsupportedShadowPromotionRejectionReason(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestnetShadowPromotionRequest {
    pub shadow_run_id: Uuid,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestnetShadowPromotionSubmitRequest {
    pub confirmation_text: String,
    pub correlation_id: Option<Uuid>,
}

impl TestnetShadowPromotionSubmitRequest {
    pub fn validate_confirmation(&self, symbol: &str) -> Result<(), CoreError> {
        let expected = expected_testnet_shadow_promotion_confirmation(symbol);
        if self.confirmation_text == expected {
            return Ok(());
        }
        Err(CoreError::InvalidTestnetShadowPromotionConfirmation {
            expected,
            actual: self.confirmation_text.clone(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestnetShadowPromotionPreview {
    pub promotion_id: Uuid,
    pub shadow_run_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub signal_id: Option<Uuid>,
    pub risk_decision_id: Uuid,
    pub would_submit_payload: TestnetShadowIntent,
    pub resolved_price: Option<Decimal>,
    pub price_source: Option<String>,
    pub expires_at: DateTime<Utc>,
    pub reasons: Vec<TestnetShadowPromotionRejectionReason>,
    pub status: TestnetShadowPromotionStatus,
    pub correlation_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub testnet_order_id: Option<Uuid>,
    pub client_order_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestnetShadowPromotionResult {
    pub promotion_id: Uuid,
    pub shadow_run_id: Uuid,
    pub testnet_order_id: Uuid,
    pub client_order_id: String,
    pub execution_state: TestnetExecutionState,
    pub correlation_id: Uuid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TestnetShadowRunnerStatus {
    Stopped,
    Running,
    Paused,
    Error,
}

impl TestnetShadowRunnerStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stopped => "STOPPED",
            Self::Running => "RUNNING",
            Self::Paused => "PAUSED",
            Self::Error => "ERROR",
        }
    }
}

impl std::str::FromStr for TestnetShadowRunnerStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "STOPPED" => Ok(Self::Stopped),
            "RUNNING" => Ok(Self::Running),
            "PAUSED" => Ok(Self::Paused),
            "ERROR" => Ok(Self::Error),
            other => Err(CoreError::UnsupportedShadowRunnerStatus(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TestnetShadowRunnerStaleFeedPolicy {
    Skip,
}

impl TestnetShadowRunnerStaleFeedPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Skip => "SKIP",
        }
    }
}

impl std::str::FromStr for TestnetShadowRunnerStaleFeedPolicy {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "SKIP" => Ok(Self::Skip),
            other => Err(CoreError::UnsupportedShadowRunnerStaleFeedPolicy(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestnetShadowRunnerConfigInput {
    pub enabled: bool,
    pub interval_seconds: i32,
    pub strategies: Vec<String>,
    pub symbols: Vec<String>,
    pub timeframe: String,
    pub max_runs_per_tick: i32,
    pub stale_feed_policy: TestnetShadowRunnerStaleFeedPolicy,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestnetShadowRunnerConfig {
    pub id: Uuid,
    pub enabled: bool,
    pub interval_seconds: i32,
    pub strategies: Vec<String>,
    pub symbols: Vec<String>,
    pub timeframe: String,
    pub max_runs_per_tick: i32,
    pub stale_feed_policy: TestnetShadowRunnerStaleFeedPolicy,
    pub notes: Option<String>,
    pub updated_by: Option<Uuid>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestnetShadowRunnerState {
    pub id: Uuid,
    pub status: TestnetShadowRunnerStatus,
    pub last_tick_at: Option<DateTime<Utc>>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub total_ticks: i64,
    pub total_runs: i64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TestnetShadowRunnerControlAction {
    Start,
    Stop,
    Pause,
    Resume,
    RunOnce,
}

impl TestnetShadowRunnerControlAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Start => "START",
            Self::Stop => "STOP",
            Self::Pause => "PAUSE",
            Self::Resume => "RESUME",
            Self::RunOnce => "RUN_ONCE",
        }
    }
}

impl std::str::FromStr for TestnetShadowRunnerControlAction {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "START" => Ok(Self::Start),
            "STOP" => Ok(Self::Stop),
            "PAUSE" => Ok(Self::Pause),
            "RESUME" => Ok(Self::Resume),
            "RUN_ONCE" => Ok(Self::RunOnce),
            other => Err(CoreError::UnsupportedShadowRunnerControlAction(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestnetShadowRunnerControlRequest {
    pub action: TestnetShadowRunnerControlAction,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TestnetShadowRunnerTickStatus {
    NoOp,
    Completed,
    PartialFailure,
    Failed,
}

impl TestnetShadowRunnerTickStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoOp => "NO_OP",
            Self::Completed => "COMPLETED",
            Self::PartialFailure => "PARTIAL_FAILURE",
            Self::Failed => "FAILED",
        }
    }
}

impl std::str::FromStr for TestnetShadowRunnerTickStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "NO_OP" => Ok(Self::NoOp),
            "COMPLETED" => Ok(Self::Completed),
            "PARTIAL_FAILURE" => Ok(Self::PartialFailure),
            "FAILED" => Ok(Self::Failed),
            other => Err(CoreError::UnsupportedShadowRunnerTickStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestnetShadowRunnerTickResult {
    pub status: TestnetShadowRunnerTickStatus,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub scheduled: bool,
    pub attempted_runs: i32,
    pub completed_runs: i32,
    pub failed_runs: i32,
    pub correlation_id: Uuid,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExchangeTestnetPipelinePreviewRequest {
    pub risk_decision_id: Uuid,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExchangeTestnetPipelineSubmitRequest {
    pub risk_decision_id: Uuid,
    pub confirmation_text: String,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangeTestnetPipelinePreview {
    pub strategy_id: Option<String>,
    pub signal_id: Option<Uuid>,
    pub risk_decision_id: Uuid,
    pub symbol: String,
    pub side: ExchangeOrderSide,
    pub order_type: ExchangeOrderType,
    pub quantity: Decimal,
    pub quote_notional: Decimal,
    pub reference_price: Decimal,
    pub reference_price_received_at: DateTime<Utc>,
    pub confirmation_text: String,
    pub execution_state_preview: TestnetExecutionState,
    pub correlation_id: Uuid,
    pub previewed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangeOrderAck {
    pub exchange: ExchangeName,
    pub environment: ExchangeEnvironment,
    pub symbol: String,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub status: ExchangeOrderState,
    pub transact_time: DateTime<Utc>,
    pub executed_qty: Decimal,
    pub cumulative_quote_qty: Decimal,
    pub is_working: Option<bool>,
    pub raw_payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangeOrderStatus {
    pub exchange: ExchangeName,
    pub environment: ExchangeEnvironment,
    pub symbol: String,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub status: ExchangeOrderState,
    pub side: ExchangeOrderSide,
    pub order_type: ExchangeOrderType,
    pub time_in_force: Option<ExchangeOrderTimeInForce>,
    pub original_qty: Option<Decimal>,
    pub executed_qty: Decimal,
    pub cumulative_quote_qty: Decimal,
    pub limit_price: Option<Decimal>,
    pub updated_at: DateTime<Utc>,
    pub raw_payload: Value,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExchangeReconciliationStatus {
    Running,
    Completed,
    Failed,
}

impl ExchangeReconciliationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "RUNNING",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
        }
    }
}

impl std::str::FromStr for ExchangeReconciliationStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "RUNNING" => Ok(Self::Running),
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            other => Err(CoreError::UnsupportedExchangeReconciliationStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExchangeReconciliationMismatchKind {
    StatusMismatch,
    ExchangeOrderMissing,
    LocalOrderMissing,
    AckWithoutStatus,
    CancelNotConfirmed,
    UnknownExchangeState,
}

impl ExchangeReconciliationMismatchKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StatusMismatch => "STATUS_MISMATCH",
            Self::ExchangeOrderMissing => "EXCHANGE_ORDER_MISSING",
            Self::LocalOrderMissing => "LOCAL_ORDER_MISSING",
            Self::AckWithoutStatus => "ACK_WITHOUT_STATUS",
            Self::CancelNotConfirmed => "CANCEL_NOT_CONFIRMED",
            Self::UnknownExchangeState => "UNKNOWN_EXCHANGE_STATE",
        }
    }
}

impl std::str::FromStr for ExchangeReconciliationMismatchKind {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "STATUS_MISMATCH" => Ok(Self::StatusMismatch),
            "EXCHANGE_ORDER_MISSING" => Ok(Self::ExchangeOrderMissing),
            "LOCAL_ORDER_MISSING" => Ok(Self::LocalOrderMissing),
            "ACK_WITHOUT_STATUS" => Ok(Self::AckWithoutStatus),
            "CANCEL_NOT_CONFIRMED" => Ok(Self::CancelNotConfirmed),
            "UNKNOWN_EXCHANGE_STATE" => Ok(Self::UnknownExchangeState),
            other => Err(CoreError::UnsupportedExchangeReconciliationMismatchKind(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExchangeReconciliationAction {
    None,
    UpdateLocalStatus,
    Alert,
}

impl ExchangeReconciliationAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::UpdateLocalStatus => "UPDATE_LOCAL_STATUS",
            Self::Alert => "ALERT",
        }
    }
}

impl std::str::FromStr for ExchangeReconciliationAction {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "NONE" => Ok(Self::None),
            "UPDATE_LOCAL_STATUS" => Ok(Self::UpdateLocalStatus),
            "ALERT" => Ok(Self::Alert),
            other => Err(CoreError::UnsupportedExchangeReconciliationAction(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExchangeReconciliationSummary {
    pub checked_orders: i32,
    pub matched_orders: i32,
    pub mismatched_orders: i32,
    pub unknown_orders: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExchangeReconciliationRun {
    pub id: Uuid,
    pub exchange: ExchangeName,
    pub environment: ExchangeEnvironment,
    pub status: ExchangeReconciliationStatus,
    pub checked_orders: i32,
    pub matched_orders: i32,
    pub mismatched_orders: i32,
    pub unknown_orders: i32,
    pub failed_reason: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub correlation_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangeReconciliationMismatch {
    pub id: Uuid,
    pub run_id: Uuid,
    pub client_order_id: String,
    pub local_status: Option<String>,
    pub exchange_status: Option<String>,
    pub mismatch_kind: ExchangeReconciliationMismatchKind,
    pub action: ExchangeReconciliationAction,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExchangeReconciliationResult {
    pub run_id: Uuid,
    pub status: ExchangeReconciliationStatus,
    pub checked_orders: i32,
    pub matched_orders: i32,
    pub mismatched_orders: i32,
    pub unknown_orders: i32,
    pub correlation_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExchangeReconciliationRequest {
    pub exchange: ExchangeName,
    pub environment: ExchangeEnvironment,
    pub limit: i64,
    pub status_filter: Vec<String>,
    pub correlation_id: Option<Uuid>,
}

impl ExchangeReconciliationRequest {
    pub const MAX_LIMIT: i64 = 200;

    pub fn validate(&self) -> Result<(), CoreError> {
        if self.environment == ExchangeEnvironment::Live {
            return Err(CoreError::LiveExchangeEnvironmentRejected);
        }
        if self.limit <= 0 {
            return Err(CoreError::InvalidExchangeReconciliationLimit(self.limit));
        }
        if self.limit > Self::MAX_LIMIT {
            return Err(CoreError::ExchangeReconciliationLimitTooHigh(self.limit));
        }
        if self
            .status_filter
            .iter()
            .any(|status| status.trim().is_empty())
        {
            return Err(CoreError::InvalidExchangeReconciliationStatusFilter);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangeCancelRequest {
    pub exchange: ExchangeName,
    pub environment: ExchangeEnvironment,
    pub symbol: Symbol,
    pub client_order_id: String,
    pub recv_window_ms: Option<u64>,
}

impl ExchangeCancelRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.client_order_id.trim().is_empty() {
            return Err(CoreError::EmptyClientOrderId);
        }
        if self.environment == ExchangeEnvironment::Live {
            return Err(CoreError::LiveExchangeEnvironmentRejected);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangeCancelAck {
    pub exchange: ExchangeName,
    pub environment: ExchangeEnvironment,
    pub symbol: String,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub status: ExchangeOrderState,
    pub cancelled_at: DateTime<Utc>,
    pub raw_payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangeBalance {
    pub exchange: ExchangeName,
    pub environment: ExchangeEnvironment,
    pub asset: String,
    pub free: Decimal,
    pub locked: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangeSymbolInfo {
    pub exchange: ExchangeName,
    pub environment: ExchangeEnvironment,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub status: String,
    pub min_price: Option<Decimal>,
    pub tick_size: Option<Decimal>,
    pub min_qty: Option<Decimal>,
    pub step_size: Option<Decimal>,
    pub min_notional: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExchangeRateLimitState {
    pub request_weight: Option<u32>,
    pub orders_1m: Option<u32>,
    pub raw_requests_5m: Option<u32>,
    pub retry_after_ms: Option<u64>,
}

#[derive(Debug, Clone, Error, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExchangeError {
    #[error("exchange adapter configuration is missing: {0}")]
    Configuration(String),
    #[error("live exchange environment is disabled")]
    LiveEnvironmentDisabled,
    #[error("exchange request validation failed: {0}")]
    Validation(String),
    #[error("exchange authentication failed")]
    Authentication,
    #[error("exchange request was rate limited")]
    RateLimited,
    #[error("exchange returned an API error: {0}")]
    Api(String),
    #[error("exchange transport error: {0}")]
    Transport(String),
    #[error("exchange response could not be decoded: {0}")]
    Serialization(String),
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
    #[error("unsupported strategy performance mode: {0}")]
    UnsupportedStrategyPerformanceMode(String),
    #[error("unsupported execution readiness target: {0}")]
    UnsupportedExecutionReadinessTarget(String),
    #[error("unsupported operator report format: {0}")]
    UnsupportedOperatorReportFormat(String),
    #[error("unsupported signal side: {0}")]
    UnsupportedSignalSide(String),
    #[error("unsupported signal reason: {0}")]
    UnsupportedSignalReason(String),
    #[error("unsupported testnet shadow decision: {0}")]
    UnsupportedShadowDecision(String),
    #[error("unsupported testnet shadow status: {0}")]
    UnsupportedShadowStatus(String),
    #[error("unsupported testnet shadow rejection reason: {0}")]
    UnsupportedShadowRejectionReason(String),
    #[error("unsupported testnet shadow promotion status: {0}")]
    UnsupportedShadowPromotionStatus(String),
    #[error("unsupported testnet shadow promotion rejection reason: {0}")]
    UnsupportedShadowPromotionRejectionReason(String),
    #[error("unsupported testnet shadow runner status: {0}")]
    UnsupportedShadowRunnerStatus(String),
    #[error("unsupported testnet shadow runner stale feed policy: {0}")]
    UnsupportedShadowRunnerStaleFeedPolicy(String),
    #[error("unsupported testnet shadow runner control action: {0}")]
    UnsupportedShadowRunnerControlAction(String),
    #[error("unsupported testnet shadow runner tick status: {0}")]
    UnsupportedShadowRunnerTickStatus(String),
    #[error("unsupported replay run status: {0}")]
    UnsupportedReplayRunStatus(String),
    #[error("unsupported strategy experiment status: {0}")]
    UnsupportedStrategyExperimentStatus(String),
    #[error("unsupported strategy walk-forward status: {0}")]
    UnsupportedStrategyWalkForwardStatus(String),
    #[error("unsupported strategy research candidate source: {0}")]
    UnsupportedStrategyResearchCandidateSource(String),
    #[error("unsupported strategy research candidate status: {0}")]
    UnsupportedStrategyResearchCandidateStatus(String),
    #[error("unsupported strategy research candidate rejection reason: {0}")]
    UnsupportedStrategyResearchCandidateRejectionReason(String),
    #[error("unsupported strategy candidate observation status: {0}")]
    UnsupportedStrategyCandidateObservationStatus(String),
    #[error("unsupported strategy candidate observation decision: {0}")]
    UnsupportedStrategyCandidateObservationDecision(String),
    #[error("unsupported research candidate status: {0}")]
    UnsupportedResearchCandidateStatus(String),
    #[error("unsupported research candidate decision: {0}")]
    UnsupportedResearchCandidateDecision(String),
    #[error("unsupported research candidate review action: {0}")]
    UnsupportedResearchCandidateReviewAction(String),
    #[error("unsupported research candidate review status: {0}")]
    UnsupportedResearchCandidateReviewStatus(String),
    #[error("unsupported research candidate shadow performance status: {0}")]
    UnsupportedResearchCandidateShadowPerformanceStatus(String),
    #[error("unsupported research candidate shadow performance recommendation: {0}")]
    UnsupportedResearchCandidateShadowPerformanceRecommendation(String),
    #[error("unsupported research batch status: {0}")]
    UnsupportedResearchBatchStatus(String),
    #[error("unsupported research batch step status: {0}")]
    UnsupportedResearchBatchStepStatus(String),
    #[error("unsupported research campaign status: {0}")]
    UnsupportedResearchCampaignStatus(String),
    #[error("research campaign requires at least one strategy")]
    EmptyResearchCampaignStrategies,
    #[error("research campaign requires at least one symbol")]
    EmptyResearchCampaignSymbols,
    #[error("research campaign requires at least one timeframe")]
    EmptyResearchCampaignTimeframes,
    #[error("research campaign requires at least one window")]
    EmptyResearchCampaignWindows,
    #[error("invalid research campaign time range")]
    InvalidResearchCampaignTimeRange,
    #[error("invalid research campaign window or step hours")]
    InvalidResearchCampaignWindowStep,
    #[error("invalid research candidate transition from {0} using decision {1}")]
    InvalidResearchCandidateTransition(String, String),
    #[error("invalid research candidate review action {1} for status {0}")]
    InvalidResearchCandidateReviewAction(String, String),
    #[error("research candidate review reason is required for action {0}")]
    MissingResearchCandidateReviewReason(String),
    #[error("research candidate review action {0} requires latest qualification status QUALIFIED")]
    ResearchCandidateReviewRequiresQualified(String),
    #[error("research candidate review action {0} requires lost qualification, needs attention, or not qualified context")]
    ResearchCandidateReviewRequiresInvestigationContext(String),
    #[error("unsupported replay mode: {0}")]
    UnsupportedReplayMode(String),
    #[error("unsupported exchange environment: {0}")]
    UnsupportedExchangeEnvironment(String),
    #[error("unsupported exchange name: {0}")]
    UnsupportedExchangeName(String),
    #[error("unsupported exchange reconciliation status: {0}")]
    UnsupportedExchangeReconciliationStatus(String),
    #[error("unsupported exchange private stream status: {0}")]
    UnsupportedExchangePrivateStreamStatus(String),
    #[error("unsupported exchange private stream source: {0}")]
    UnsupportedExchangePrivateStreamSource(String),
    #[error("unsupported testnet execution state: {0}")]
    UnsupportedTestnetExecutionState(String),
    #[error("unsupported testnet repair action: {0}")]
    UnsupportedTestnetRepairAction(String),
    #[error("unsupported testnet repair action status: {0}")]
    UnsupportedTestnetRepairActionStatus(String),
    #[error("unsupported testnet execution transition source: {0}")]
    UnsupportedTestnetExecutionTransitionSource(String),
    #[error("unsupported exchange reconciliation mismatch kind: {0}")]
    UnsupportedExchangeReconciliationMismatchKind(String),
    #[error("unsupported exchange reconciliation action: {0}")]
    UnsupportedExchangeReconciliationAction(String),
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
    #[error("unsupported market data quality status: {0}")]
    UnsupportedMarketDataQualityStatus(String),
    #[error("unsupported market data repair status: {0}")]
    UnsupportedMarketDataRepairStatus(String),
    #[error("unsupported user role: {0}")]
    UnsupportedUserRole(String),
    #[error("unsupported user status: {0}")]
    UnsupportedUserStatus(String),
    #[error("idempotency_key cannot be empty")]
    EmptyIdempotencyKey,
    #[error("client_order_id cannot be empty")]
    EmptyClientOrderId,
    #[error("backtest strategy_id cannot be empty")]
    EmptyBacktestStrategyId,
    #[error("backtest symbol cannot be empty")]
    EmptyBacktestSymbol,
    #[error("backtest timeframe cannot be empty")]
    EmptyBacktestTimeframe,
    #[error("strategy experiment strategy_id cannot be empty")]
    EmptyStrategyExperimentStrategyId,
    #[error("strategy experiment symbol cannot be empty")]
    EmptyStrategyExperimentSymbol,
    #[error("strategy experiment timeframe cannot be empty")]
    EmptyStrategyExperimentTimeframe,
    #[error("strategy experiment requires at least one timeframe")]
    EmptyStrategyExperimentTimeframes,
    #[error("strategy experiment requires at least one candidate")]
    EmptyStrategyExperimentCandidates,
    #[error("strategy walk-forward strategy_id cannot be empty")]
    EmptyStrategyWalkForwardStrategyId,
    #[error("strategy walk-forward symbol cannot be empty")]
    EmptyStrategyWalkForwardSymbol,
    #[error("strategy walk-forward timeframe cannot be empty")]
    EmptyStrategyWalkForwardTimeframe,
    #[error("strategy walk-forward candidate lookback_candles must be greater than zero")]
    EmptyStrategyWalkForwardCandidateLookback,
    #[error("candle backfill symbol cannot be empty")]
    EmptyCandleBackfillSymbol,
    #[error("candle backfill interval cannot be empty")]
    EmptyCandleBackfillInterval,
    #[error("research data interval cannot be empty")]
    EmptyResearchDataInterval,
    #[error("research data requires at least one interval")]
    EmptyResearchDataIntervals,
    #[error("backtest end_time must be after start_time")]
    InvalidBacktestTimeRange,
    #[error("candle backfill end_time must be after start_time")]
    InvalidCandleBackfillTimeRange,
    #[error("research data end_time must be after start_time")]
    InvalidResearchDataTimeRange,
    #[error("research coverage percentage must be greater than zero and at most 100")]
    InvalidResearchCoveragePct,
    #[error("operator report end_time must be after start_time")]
    InvalidOperatorReportTimeRange,
    #[error("strategy experiment end_time must be after start_time")]
    InvalidStrategyExperimentTimeRange,
    #[error("strategy walk-forward end_time must be after start_time")]
    InvalidStrategyWalkForwardTimeRange,
    #[error("backtest initial_capital must be greater than zero")]
    InvalidBacktestInitialCapital,
    #[error("strategy experiment initial_capital must be greater than zero")]
    InvalidStrategyExperimentInitialCapital,
    #[error("strategy walk-forward initial_capital must be greater than zero")]
    InvalidStrategyWalkForwardInitialCapital,
    #[error("strategy experiment max_runs must be greater than zero")]
    InvalidStrategyExperimentMaxRuns,
    #[error("strategy walk-forward min_required_test_windows must be greater than zero")]
    InvalidStrategyWalkForwardMinRequiredWindows,
    #[error("strategy walk-forward {0} must be greater than zero")]
    InvalidStrategyWalkForwardWindowSize(String),
    #[error("strategy walk-forward step_size_hours must be greater than zero")]
    InvalidStrategyWalkForwardStepSize,
    #[error("candle backfill request limit must be greater than zero")]
    InvalidCandleBackfillLimit,
    #[error("market data repair max_ranges must be greater than zero")]
    InvalidMarketDataRepairMaxRanges,
    #[error("candle backfill request limit exceeds Binance maximum: {0}")]
    CandleBackfillLimitTooHigh(u16),
    #[error("exchange reconciliation request limit must be greater than zero: {0}")]
    InvalidExchangeReconciliationLimit(i64),
    #[error("exchange reconciliation request limit exceeds maximum: {0}")]
    ExchangeReconciliationLimitTooHigh(i64),
    #[error("exchange reconciliation status_filter cannot contain empty values")]
    InvalidExchangeReconciliationStatusFilter,
    #[error("exchange private stream reconnect_count must be zero or greater: {0}")]
    InvalidExchangeReconnectCount(i32),
    #[error("exchange private stream event_type cannot be empty")]
    InvalidExchangePrivateStreamEventType,
    #[error("invalid testnet execution transition from {previous_state:?} to {next_state:?} via {transition_source:?}")]
    InvalidTestnetExecutionTransition {
        previous_state: Option<TestnetExecutionState>,
        next_state: TestnetExecutionState,
        transition_source: TestnetExecutionTransitionSource,
    },
    #[error("invalid testnet repair confirmation: expected {expected:?}, got {actual:?}")]
    InvalidTestnetRepairConfirmation { expected: String, actual: String },
    #[error(
        "invalid testnet shadow promotion confirmation: expected {expected:?}, got {actual:?}"
    )]
    InvalidTestnetShadowPromotionConfirmation { expected: String, actual: String },
    #[error("invalid testnet repair transition for {action:?} from {previous_state:?} to {next_state:?}")]
    InvalidTestnetRepairTransition {
        action: TestnetRepairAction,
        previous_state: TestnetExecutionState,
        next_state: Option<TestnetExecutionState>,
    },
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
    #[error("exchange quantity must be greater than zero")]
    InvalidExchangeQuantity,
    #[error("exchange quote notional must be greater than zero")]
    InvalidExchangeNotional,
    #[error("exchange limit_price must be greater than zero")]
    InvalidExchangeLimitPrice,
    #[error("exchange order requires quantity or quote_notional")]
    MissingExchangeQuantityOrNotional,
    #[error("exchange limit order requires limit_price")]
    MissingExchangeLimitPrice,
    #[error("exchange limit order requires time_in_force")]
    MissingExchangeTimeInForce,
    #[error("live exchange environment is rejected")]
    LiveExchangeEnvironmentRejected,
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
        aggregate_closed_1m_candles, calculate_average_duration_seconds,
        calculate_strategy_average_pnl, calculate_strategy_rejection_rate,
        calculate_strategy_win_rate, calculate_testnet_promotion_rate,
        combine_strategy_performance_summaries, execution_readiness_status_from_checks,
        plan_market_data_repair, score_execution_readiness, summarize_candle_continuity,
        validate_password_length, validate_testnet_repair_transition, Candle, CandleInterval,
        ExchangeEnvironment, ExchangeExecutionReport, ExchangeExecutionReportType,
        ExchangeExecutionStatus, ExchangeName, ExchangeOrderRequest, ExchangeOrderSide,
        ExchangeOrderState, ExchangeOrderType, ExchangePrivateStreamEvent,
        ExchangePrivateStreamSource, ExchangePrivateStreamState, ExchangePrivateStreamStatus,
        ExecutionReadinessCheck, ExecutionReadinessCheckSeverity, ExecutionReadinessRecommendation,
        ExecutionReadinessStatus, ExecutionState, MarketDataQualityRequest,
        MarketDataQualityStatus, MarketDataRepairMode, MarketDataRepairPlanRequest,
        MarketDataRepairStatus, MarketDataSource, OperatorReport, OperatorReportFinding,
        OperatorReportFormat, OperatorReportRecommendation, OperatorReportRequest,
        OperatorReportSection, OperatorReportSeverity, OperatorReportStatus, OperatorReportSummary,
        OrderIntent, PaperOrder, Permission, Side, StrategyPerformanceMode,
        StrategyPerformanceSummary, Symbol, TestnetExecutionState, TestnetRepairAction,
        TestnetRepairRequest, UserRole,
    };
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use rust_decimal::Decimal;
    use serde_json::json;
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

    #[test]
    fn exchange_order_rejects_live_environment() {
        let request = ExchangeOrderRequest {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Live,
            symbol: Symbol::new("btcusdt").expect("valid symbol"),
            side: ExchangeOrderSide::Buy,
            order_type: ExchangeOrderType::Market,
            time_in_force: None,
            quantity: Some(Decimal::ONE),
            quote_notional: None,
            limit_price: None,
            client_order_id: "test-client-order".to_string(),
            recv_window_ms: None,
            risk_decision_id: None,
        };

        assert!(matches!(
            request.validate(),
            Err(super::CoreError::LiveExchangeEnvironmentRejected)
        ));
    }

    #[test]
    fn exchange_order_validation_requires_price_for_limit_order() {
        let request = ExchangeOrderRequest {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            symbol: Symbol::new("btcusdt").expect("valid symbol"),
            side: ExchangeOrderSide::Buy,
            order_type: ExchangeOrderType::Limit,
            time_in_force: None,
            quantity: Some(Decimal::ONE),
            quote_notional: None,
            limit_price: None,
            client_order_id: "test-client-order".to_string(),
            recv_window_ms: None,
            risk_decision_id: None,
        };

        assert!(matches!(
            request.validate(),
            Err(super::CoreError::MissingExchangeLimitPrice)
        ));
    }

    #[test]
    fn exchange_ack_does_not_imply_fill() {
        let ack = super::ExchangeOrderAck {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            symbol: "BTCUSDT".to_string(),
            client_order_id: "test-client-order".to_string(),
            exchange_order_id: Some("12345".to_string()),
            status: ExchangeOrderState::New,
            transact_time: Utc::now(),
            executed_qty: Decimal::ZERO,
            cumulative_quote_qty: Decimal::ZERO,
            is_working: Some(true),
            raw_payload: json!({ "status": "NEW" }),
        };

        assert_eq!(ack.status, ExchangeOrderState::New);
        assert_eq!(ack.executed_qty, Decimal::ZERO);
    }

    #[test]
    fn private_stream_state_rejects_live_environment() {
        let err = ExchangePrivateStreamState {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Live,
            status: ExchangePrivateStreamStatus::Disconnected,
            listen_key_hash: None,
            connected_at: None,
            last_event_at: None,
            last_error: None,
            reconnect_count: 0,
            updated_at: Utc::now(),
        }
        .validate()
        .expect_err("live should be rejected");

        assert!(matches!(
            err,
            super::CoreError::LiveExchangeEnvironmentRejected
        ));
    }

    #[test]
    fn trade_execution_report_emits_fill_event() {
        let report = ExchangeExecutionReport {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            symbol: "BTCUSDT".to_string(),
            client_order_id: "client-1".to_string(),
            exchange_order_id: Some("42".to_string()),
            side: ExchangeOrderSide::Buy,
            order_type: ExchangeOrderType::Market,
            time_in_force: None,
            order_status: ExchangeExecutionStatus::Filled,
            execution_type: ExchangeExecutionReportType::Trade,
            last_executed_qty: Decimal::new(1, 0),
            cumulative_filled_qty: Decimal::new(1, 0),
            last_executed_price: Decimal::new(100_000, 0),
            commission_amount: Some(Decimal::new(25, 4)),
            commission_asset: Some("BNB".to_string()),
            event_time: Utc::now(),
            transaction_time: Some(Utc::now()),
            raw_payload: json!({"e":"executionReport"}),
        };

        let fill = report.fill_event().expect("trade should create fill event");
        assert_eq!(fill.last_executed_qty, Decimal::new(1, 0));
        assert_eq!(fill.commission_asset.as_deref(), Some("BNB"));
    }

    #[test]
    fn new_execution_report_is_not_a_fill() {
        let report = ExchangeExecutionReport {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            symbol: "BTCUSDT".to_string(),
            client_order_id: "client-1".to_string(),
            exchange_order_id: Some("42".to_string()),
            side: ExchangeOrderSide::Buy,
            order_type: ExchangeOrderType::Limit,
            time_in_force: None,
            order_status: ExchangeExecutionStatus::New,
            execution_type: ExchangeExecutionReportType::New,
            last_executed_qty: Decimal::ZERO,
            cumulative_filled_qty: Decimal::ZERO,
            last_executed_price: Decimal::ZERO,
            commission_amount: None,
            commission_asset: None,
            event_time: Utc::now(),
            transaction_time: None,
            raw_payload: json!({"e":"executionReport"}),
        };

        assert!(report.fill_event().is_none());
    }

    #[test]
    fn private_stream_event_rejects_empty_type() {
        let err = ExchangePrivateStreamEvent {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            source: ExchangePrivateStreamSource::Websocket,
            event_type: " ".to_string(),
            symbol: None,
            client_order_id: None,
            exchange_order_id: None,
            execution_type: None,
            order_status: None,
            event_time: Utc::now(),
            received_at: Utc::now(),
            raw_payload: json!({}),
        }
        .validate()
        .expect_err("empty event type should be rejected");

        assert!(matches!(
            err,
            super::CoreError::InvalidExchangePrivateStreamEventType
        ));
    }

    #[test]
    fn testnet_repair_confirmation_must_match_exact_phrase() {
        let request = TestnetRepairRequest {
            action: TestnetRepairAction::ManualRecheck,
            confirmation_text: "REPAIR TESTNET client-1".to_string(),
            reason: None,
            force: false,
            correlation_id: None,
        };
        assert!(request.validate_confirmation("client-1").is_ok());

        let invalid = TestnetRepairRequest {
            confirmation_text: "repair testnet client-1".to_string(),
            ..request
        };
        assert!(invalid.validate_confirmation("client-1").is_err());
    }

    #[test]
    fn safe_cancel_requires_cancel_confirmation() {
        let request = TestnetRepairRequest {
            action: TestnetRepairAction::SafeCancelRequest,
            confirmation_text: "REPAIR TESTNET client-1".to_string(),
            reason: None,
            force: false,
            correlation_id: None,
        };
        assert!(request.validate_confirmation("client-1").is_err());
    }

    #[test]
    fn repair_cannot_move_filled_back_to_active() {
        assert!(validate_testnet_repair_transition(
            TestnetRepairAction::MarkAcked,
            TestnetExecutionState::Filled,
            Some(TestnetExecutionState::ExchangeAcked),
            false,
        )
        .is_err());
    }

    #[test]
    fn mark_reconciliation_required_is_allowed_for_failed_state() {
        assert!(validate_testnet_repair_transition(
            TestnetRepairAction::MarkReconciliationRequired,
            TestnetExecutionState::Failed,
            Some(TestnetExecutionState::ReconciliationRequired),
            false,
        )
        .is_ok());
    }

    #[test]
    fn mark_acked_is_limited_to_allowed_states() {
        assert!(validate_testnet_repair_transition(
            TestnetRepairAction::MarkAcked,
            TestnetExecutionState::ReconciliationRequired,
            Some(TestnetExecutionState::ExchangeAcked),
            false,
        )
        .is_ok());
        assert!(validate_testnet_repair_transition(
            TestnetRepairAction::MarkAcked,
            TestnetExecutionState::CancelRequested,
            Some(TestnetExecutionState::ExchangeAcked),
            false,
        )
        .is_err());
    }

    #[test]
    fn strategy_rejection_rate_returns_zero_for_empty_totals() {
        assert_eq!(calculate_strategy_rejection_rate(0, 0), Decimal::ZERO);
    }

    #[test]
    fn strategy_win_rate_returns_none_for_empty_totals() {
        assert_eq!(calculate_strategy_win_rate(0, 0), None);
    }

    #[test]
    fn strategy_average_pnl_returns_none_for_empty_totals() {
        assert_eq!(calculate_strategy_average_pnl(Decimal::ZERO, 0), None);
    }

    #[test]
    fn promotion_rate_returns_zero_for_empty_denominator() {
        assert_eq!(calculate_testnet_promotion_rate(5, 0), Decimal::ZERO);
    }

    #[test]
    fn promotion_preview_rate_is_percent_rounded() {
        assert_eq!(
            calculate_testnet_promotion_rate(12, 42),
            Decimal::from_str_exact("28.57").expect("valid decimal"),
        );
    }

    #[test]
    fn promotion_submit_rate_is_percent_rounded() {
        assert_eq!(
            calculate_testnet_promotion_rate(5, 12),
            Decimal::from_str_exact("41.67").expect("valid decimal"),
        );
    }

    #[test]
    fn promotion_fill_rate_is_percent_rounded() {
        assert_eq!(
            calculate_testnet_promotion_rate(3, 5),
            Decimal::from_str_exact("60.00").expect("valid decimal"),
        );
    }

    #[test]
    fn average_duration_returns_none_for_empty_samples() {
        assert_eq!(calculate_average_duration_seconds(Decimal::ZERO, 0), None);
    }

    #[test]
    fn average_duration_calculates_decimal_seconds() {
        assert_eq!(
            calculate_average_duration_seconds(Decimal::from(11), 2),
            Some(Decimal::from_str_exact("5.50").expect("valid decimal")),
        );
    }

    #[test]
    fn combined_strategy_summary_handles_missing_modes() {
        let now = Utc::now();
        let paper = StrategyPerformanceSummary {
            strategy_id: Some("momentum_v1".to_string()),
            symbol: Some("BTCUSDT".to_string()),
            timeframe: Some("1m".to_string()),
            mode: StrategyPerformanceMode::Paper,
            window_start: now,
            window_end: now,
            total_runs: 0,
            total_signals: 3,
            approved_risk_decisions: 2,
            rejected_risk_decisions: 1,
            risk_rejection_rate: Decimal::ZERO,
            shadow_would_submit_count: 0,
            shadow_no_signal_count: 0,
            shadow_risk_rejected_count: 0,
            paper_orders_count: 2,
            paper_positions_opened: 2,
            paper_positions_closed: 1,
            realized_pnl: Decimal::from(12),
            unrealized_pnl: Decimal::from(4),
            win_rate: Some(Decimal::ONE),
            avg_win: Some(Decimal::from(12)),
            avg_loss: None,
            max_drawdown_pct: Some(Decimal::from_str_exact("0.12").unwrap()),
            backtest_runs_count: 0,
            best_backtest_pnl_pct: None,
            worst_backtest_pnl_pct: None,
            avg_backtest_pnl_pct: None,
            created_at: now,
            computed_at: now,
        };
        let shadow = StrategyPerformanceSummary {
            strategy_id: Some("momentum_v1".to_string()),
            symbol: Some("BTCUSDT".to_string()),
            timeframe: Some("1m".to_string()),
            mode: StrategyPerformanceMode::Shadow,
            window_start: now,
            window_end: now,
            total_runs: 4,
            total_signals: 0,
            approved_risk_decisions: 0,
            rejected_risk_decisions: 0,
            risk_rejection_rate: Decimal::ZERO,
            shadow_would_submit_count: 2,
            shadow_no_signal_count: 1,
            shadow_risk_rejected_count: 1,
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
            created_at: now,
            computed_at: now,
        };

        let combined = combine_strategy_performance_summaries(vec![paper, shadow]).unwrap();

        assert_eq!(combined.mode, StrategyPerformanceMode::Combined);
        assert_eq!(combined.total_runs, 4);
        assert_eq!(combined.total_signals, 3);
        assert_eq!(combined.shadow_would_submit_count, 2);
        assert_eq!(combined.paper_orders_count, 2);
        assert_eq!(combined.realized_pnl, Decimal::from(12));
        assert_eq!(combined.unrealized_pnl, Decimal::from(4));
        assert_eq!(combined.win_rate, Some(Decimal::ONE));
        assert_eq!(combined.avg_backtest_pnl_pct, None);
    }

    #[test]
    fn operator_report_request_rejects_end_before_start() {
        let request = OperatorReportRequest {
            start_time: Some(Utc::now()),
            end_time: Some(Utc::now() - chrono::Duration::hours(1)),
            ..OperatorReportRequest::default()
        };

        assert!(request.validate().is_err());
    }

    #[test]
    fn operator_report_markdown_contains_key_sections() {
        let now = Utc::now();
        let report = OperatorReport {
            report_id: Uuid::new_v4(),
            window_start: now,
            window_end: now,
            generated_at: now,
            status: OperatorReportStatus::Warning,
            summary: OperatorReportSummary {
                total_findings: 1,
                critical_findings: 0,
                high_findings: 1,
                medium_findings: 0,
                low_findings: 0,
                info_findings: 0,
                highest_severity: Some(OperatorReportSeverity::High),
                kill_switch_active: false,
                stale_feed_count: 1,
                risk_rejection_rate_pct: Decimal::from(55),
                paper_daily_pnl: Decimal::from(-12),
                shadow_would_submit_count: 0,
                reconciliation_required_count: 1,
            },
            findings: vec![OperatorReportFinding {
                code: "private_stream_stale".to_string(),
                severity: OperatorReportSeverity::High,
                title: "Private stream stale".to_string(),
                detail: "No recent private-stream activity was observed.".to_string(),
                section: "testnet_execution".to_string(),
            }],
            recommendations: vec![OperatorReportRecommendation {
                code: "keep_shadow_mode".to_string(),
                priority: OperatorReportSeverity::High,
                detail: "Keep system in shadow mode until private stream is stable.".to_string(),
                related_finding_codes: vec!["private_stream_stale".to_string()],
            }],
            sections: vec![OperatorReportSection {
                key: "system_health".to_string(),
                title: "System Health".to_string(),
                status: OperatorReportStatus::Ok,
                summary: "Core services are reachable.".to_string(),
                highlights: vec![],
                snapshot: json!({ "api_healthy": true }),
            }],
            format: OperatorReportFormat::Markdown,
            persisted: false,
            correlation_id: Uuid::new_v4(),
            markdown: None,
        };

        let markdown = report.render_markdown();
        assert!(markdown.contains("# Operator Daily Report"));
        assert!(markdown.contains("## Summary"));
        assert!(markdown.contains("## Findings"));
        assert!(markdown.contains("## Recommendations"));
        assert!(markdown.contains("## System Health"));
    }

    #[test]
    fn readiness_score_clamps_to_zero() {
        let checks = vec![
            ExecutionReadinessCheck {
                code: "warn_a".to_string(),
                name: "Warn A".to_string(),
                passed: false,
                blocking: false,
                severity: ExecutionReadinessCheckSeverity::High,
                summary: "warn".to_string(),
                details: None,
            };
            8
        ];

        assert_eq!(score_execution_readiness(&checks), 0);
    }

    #[test]
    fn readiness_ready_threshold_applies_without_blockers() {
        let checks = vec![ExecutionReadinessCheck {
            code: "feed_near_threshold".to_string(),
            name: "Feed near threshold".to_string(),
            passed: false,
            blocking: false,
            severity: ExecutionReadinessCheckSeverity::Low,
            summary: "warn".to_string(),
            details: None,
        }];

        let score = score_execution_readiness(&checks);
        assert_eq!(score, 97);
        assert_eq!(
            execution_readiness_status_from_checks(&checks, score),
            ExecutionReadinessStatus::Ready
        );
    }

    #[test]
    fn readiness_degraded_threshold_applies_without_blockers() {
        let checks = vec![
            ExecutionReadinessCheck {
                code: "warn_a".to_string(),
                name: "Warn A".to_string(),
                passed: false,
                blocking: false,
                severity: ExecutionReadinessCheckSeverity::High,
                summary: "warn".to_string(),
                details: None,
            },
            ExecutionReadinessCheck {
                code: "warn_b".to_string(),
                name: "Warn B".to_string(),
                passed: false,
                blocking: false,
                severity: ExecutionReadinessCheckSeverity::Medium,
                summary: "warn".to_string(),
                details: None,
            },
        ];

        let score = score_execution_readiness(&checks);
        assert_eq!(score, 77);
        assert_eq!(
            execution_readiness_status_from_checks(&checks, score),
            ExecutionReadinessStatus::Degraded
        );
    }

    #[test]
    fn readiness_blocker_forces_not_ready() {
        let checks = vec![ExecutionReadinessCheck {
            code: "kill_switch_active".to_string(),
            name: "Kill switch active".to_string(),
            passed: false,
            blocking: true,
            severity: ExecutionReadinessCheckSeverity::High,
            summary: "blocked".to_string(),
            details: None,
        }];

        let score = score_execution_readiness(&checks).min(40);
        assert_eq!(
            execution_readiness_status_from_checks(&checks, score),
            ExecutionReadinessStatus::NotReady
        );
    }

    #[test]
    fn readiness_recommendation_messages_match_operator_guidance() {
        assert_eq!(
            ExecutionReadinessRecommendation::ResumeFromKillSwitch.message(),
            "Clear only after investigating active blockers."
        );
        assert_eq!(
            ExecutionReadinessRecommendation::ReconnectPrivateStream.message(),
            "Restart private stream worker or verify listen-key lifecycle."
        );
        assert_eq!(
            ExecutionReadinessRecommendation::ReconcileTestnetOrders.message(),
            "Run reconciliation and repair before further testnet submits."
        );
        assert_eq!(
            ExecutionReadinessRecommendation::EnableStrategy.message(),
            "Enable strategy only after validating config and reviewing recent analytics."
        );
        assert_eq!(
            ExecutionReadinessRecommendation::IncreaseShadowCoverage.message(),
            "Continue shadow mode before promotion."
        );
    }

    fn sample_1m_candle(open_minute: i64, open: i64, high: i64, low: i64, close: i64) -> Candle {
        let open_time =
            Utc.with_ymd_and_hms(2026, 5, 23, 0, 0, 0).unwrap() + Duration::minutes(open_minute);
        Candle {
            id: Uuid::new_v4(),
            exchange: MarketDataSource::Binance,
            symbol: Symbol::new("BTCUSDT").unwrap(),
            interval: CandleInterval::OneMinute,
            open_time,
            close_time: CandleInterval::OneMinute.bucket_close_time(open_time),
            open: Decimal::from(open),
            high: Decimal::from(high),
            low: Decimal::from(low),
            close: Decimal::from(close),
            volume: Decimal::from(10 + open_minute),
            quote_volume: Some(Decimal::from(100 + open_minute)),
            trade_count: 1 + i32::try_from(open_minute).unwrap_or_default(),
            is_closed: true,
            created_at: open_time,
            updated_at: open_time + Duration::seconds(59),
        }
    }

    #[test]
    fn candle_interval_supports_higher_timeframes() {
        assert_eq!(
            "5m".parse::<CandleInterval>().unwrap(),
            CandleInterval::FiveMinutes
        );
        assert_eq!(
            "15m".parse::<CandleInterval>().unwrap(),
            CandleInterval::FifteenMinutes
        );
        assert_eq!(
            "1h".parse::<CandleInterval>().unwrap(),
            CandleInterval::OneHour
        );
    }

    #[test]
    fn aggregates_five_1m_candles_into_one_5m_candle() {
        let candles = (0..5)
            .map(|minute| {
                sample_1m_candle(
                    minute,
                    100 + minute,
                    110 + minute,
                    90 + minute,
                    105 + minute,
                )
            })
            .collect::<Vec<_>>();

        let outcome = aggregate_closed_1m_candles(&candles, CandleInterval::FiveMinutes);

        assert_eq!(outcome.skipped_incomplete_buckets, 0);
        assert_eq!(outcome.candles.len(), 1);
        let candle = &outcome.candles[0];
        assert_eq!(candle.open, Decimal::from(100));
        assert_eq!(candle.high, Decimal::from(114));
        assert_eq!(candle.low, Decimal::from(90));
        assert_eq!(candle.close, Decimal::from(109));
        assert_eq!(
            candle.volume,
            candles
                .iter()
                .fold(Decimal::ZERO, |sum, item| sum + item.volume)
        );
        assert_eq!(
            candle.quote_volume,
            Some(candles.iter().fold(Decimal::ZERO, |sum, item| {
                sum + item.quote_volume.unwrap()
            }))
        );
        assert_eq!(
            candle.trade_count,
            candles.iter().map(|item| item.trade_count).sum::<i32>()
        );
    }

    #[test]
    fn incomplete_bucket_is_skipped() {
        let candles = (0..4)
            .map(|minute| sample_1m_candle(minute, 100, 101, 99, 100))
            .collect::<Vec<_>>();

        let outcome = aggregate_closed_1m_candles(&candles, CandleInterval::FiveMinutes);

        assert_eq!(outcome.candles.len(), 0);
        assert_eq!(outcome.skipped_incomplete_buckets, 1);
    }

    #[test]
    fn bucket_alignment_uses_target_boundaries() {
        let candles = (5..10)
            .map(|minute| sample_1m_candle(minute, 100, 101, 99, 100))
            .collect::<Vec<_>>();

        let outcome = aggregate_closed_1m_candles(&candles, CandleInterval::FiveMinutes);
        let candle = &outcome.candles[0];

        assert_eq!(
            candle.open_time,
            Utc.with_ymd_and_hms(2026, 5, 23, 0, 5, 0).unwrap()
        );
        assert_eq!(
            candle.close_time,
            Utc.with_ymd_and_hms(2026, 5, 23, 0, 9, 59).unwrap() + Duration::milliseconds(999)
        );
    }

    #[test]
    fn fifteen_minute_aggregation_works() {
        let candles = (0..15)
            .map(|minute| sample_1m_candle(minute, 100, 101 + minute, 99 - minute, 100 + minute))
            .collect::<Vec<_>>();

        let outcome = aggregate_closed_1m_candles(&candles, CandleInterval::FifteenMinutes);

        assert_eq!(outcome.candles.len(), 1);
        assert_eq!(outcome.candles[0].high, Decimal::from(115));
        assert_eq!(outcome.candles[0].low, Decimal::from(85));
        assert_eq!(outcome.candles[0].close, Decimal::from(114));
    }

    #[test]
    fn one_hour_aggregation_works() {
        let candles = (0..60)
            .map(|minute| sample_1m_candle(minute, 100, 100 + minute, 80, 100 + minute))
            .collect::<Vec<_>>();

        let outcome = aggregate_closed_1m_candles(&candles, CandleInterval::OneHour);

        assert_eq!(outcome.candles.len(), 1);
        assert_eq!(
            outcome.candles[0].open_time,
            Utc.with_ymd_and_hms(2026, 5, 23, 0, 0, 0).unwrap()
        );
        assert_eq!(
            outcome.candles[0].close_time,
            Utc.with_ymd_and_hms(2026, 5, 23, 0, 59, 59).unwrap() + Duration::milliseconds(999)
        );
    }

    #[test]
    fn aggregation_is_deterministic_for_same_input() {
        let candles = (0..5)
            .map(|minute| sample_1m_candle(minute, 100 + minute, 101 + minute, 99, 100 + minute))
            .collect::<Vec<_>>();

        let first = aggregate_closed_1m_candles(&candles, CandleInterval::FiveMinutes);
        let second = aggregate_closed_1m_candles(&candles, CandleInterval::FiveMinutes);

        assert_eq!(first, second);
    }

    fn quality_request(start: DateTime<Utc>, end: DateTime<Utc>) -> MarketDataQualityRequest {
        MarketDataQualityRequest {
            exchange: MarketDataSource::Binance,
            symbol: "BTCUSDT".to_string(),
            interval: "1m".to_string(),
            start_time: start,
            end_time: end,
            expected_interval_seconds: None,
            max_allowed_gap_count: None,
            max_allowed_gap_pct: None,
        }
    }

    fn quality_candle(open_time: DateTime<Utc>, is_closed: bool) -> Candle {
        Candle {
            id: Uuid::new_v4(),
            exchange: MarketDataSource::Binance,
            symbol: Symbol::new("BTCUSDT").unwrap(),
            interval: CandleInterval::OneMinute,
            open_time,
            close_time: open_time + Duration::minutes(1) - Duration::milliseconds(1),
            open: Decimal::new(100, 0),
            high: Decimal::new(101, 0),
            low: Decimal::new(99, 0),
            close: Decimal::new(100, 0),
            volume: Decimal::new(1, 0),
            quote_volume: Some(Decimal::new(100, 0)),
            trade_count: 1,
            is_closed,
            created_at: open_time,
            updated_at: open_time,
        }
    }

    #[test]
    fn market_data_quality_perfect_continuity_is_good() {
        let start = Utc.with_ymd_and_hms(2024, 5, 1, 0, 0, 0).unwrap();
        let end = start + Duration::minutes(3);
        let candles = (0..3)
            .map(|minute| quality_candle(start + Duration::minutes(minute), true))
            .collect::<Vec<_>>();

        let report =
            super::summarize_candle_continuity(&quality_request(start, end), &candles, 100)
                .unwrap();

        assert_eq!(report.status, MarketDataQualityStatus::Good);
        assert_eq!(report.expected_candle_count, 3);
        assert_eq!(report.coverage_pct, Decimal::new(100, 0));
        assert_eq!(report.gap_count, 0);
    }

    #[test]
    fn market_data_quality_one_missing_candle_detects_gap() {
        let start = Utc.with_ymd_and_hms(2024, 5, 1, 0, 0, 0).unwrap();
        let end = start + Duration::minutes(3);
        let candles = vec![
            quality_candle(start, true),
            quality_candle(start + Duration::minutes(2), true),
        ];

        let report =
            super::summarize_candle_continuity(&quality_request(start, end), &candles, 100)
                .unwrap();

        assert_eq!(report.status, MarketDataQualityStatus::Bad);
        assert_eq!(report.gap_count, 1);
        assert_eq!(report.missing_candle_count, 1);
        assert_eq!(report.gaps[0].missing_candle_count, 1);
    }

    #[test]
    fn market_data_quality_duplicates_detected() {
        let start = Utc.with_ymd_and_hms(2024, 5, 1, 0, 0, 0).unwrap();
        let end = start + Duration::minutes(2);
        let candles = vec![
            quality_candle(start, true),
            quality_candle(start, true),
            quality_candle(start + Duration::minutes(1), true),
        ];

        let report =
            super::summarize_candle_continuity(&quality_request(start, end), &candles, 100)
                .unwrap();

        assert_eq!(report.status, MarketDataQualityStatus::Degraded);
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.code == "duplicate_open_times_detected"));
    }

    #[test]
    fn market_data_quality_no_candles_is_insufficient_data() {
        let start = Utc.with_ymd_and_hms(2024, 5, 1, 0, 0, 0).unwrap();
        let end = start + Duration::minutes(2);

        let report =
            super::summarize_candle_continuity(&quality_request(start, end), &[], 100).unwrap();

        assert_eq!(report.status, MarketDataQualityStatus::InsufficientData);
        assert_eq!(report.coverage_pct, Decimal::ZERO);
    }

    #[test]
    fn market_data_quality_calculates_coverage() {
        let start = Utc.with_ymd_and_hms(2024, 5, 1, 0, 0, 0).unwrap();
        let end = start + Duration::minutes(4);
        let candles = vec![
            quality_candle(start, true),
            quality_candle(start + Duration::minutes(1), true),
            quality_candle(start + Duration::minutes(2), true),
        ];

        let report =
            super::summarize_candle_continuity(&quality_request(start, end), &candles, 100)
                .unwrap();

        assert_eq!(report.coverage_pct, Decimal::new(75, 0));
        assert_eq!(report.missing_candle_count, 1);
    }

    #[test]
    fn market_data_quality_calculates_largest_gap() {
        let start = Utc.with_ymd_and_hms(2024, 5, 1, 0, 0, 0).unwrap();
        let end = start + Duration::minutes(8);
        let candles = vec![
            quality_candle(start, true),
            quality_candle(start + Duration::minutes(2), true),
            quality_candle(start + Duration::minutes(7), true),
        ];

        let report =
            super::summarize_candle_continuity(&quality_request(start, end), &candles, 100)
                .unwrap();

        assert_eq!(report.gap_count, 2);
        assert_eq!(report.largest_gap_seconds, 240);
    }

    #[test]
    fn market_data_quality_warns_on_open_candle_in_historical_window() {
        let start = Utc.with_ymd_and_hms(2024, 5, 1, 0, 0, 0).unwrap();
        let end = start + Duration::minutes(2);
        let candles = vec![
            quality_candle(start, true),
            quality_candle(start + Duration::minutes(1), false),
        ];

        let report =
            super::summarize_candle_continuity(&quality_request(start, end), &candles, 100)
                .unwrap();

        assert_eq!(report.status, MarketDataQualityStatus::Bad);
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.code == "open_candles_in_historical_window"));
    }

    fn repair_request(
        interval: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> MarketDataRepairPlanRequest {
        MarketDataRepairPlanRequest {
            exchange: MarketDataSource::Binance,
            symbol: "BTCUSDT".to_string(),
            interval: interval.to_string(),
            start_time: start,
            end_time: end,
            repair_mode: MarketDataRepairMode::PlanOnly,
            max_ranges: 100,
            reaggregate_derived_intervals: true,
            correlation_id: None,
        }
    }

    #[test]
    fn repair_plan_one_missing_candle_creates_one_range() {
        let start = Utc.with_ymd_and_hms(2024, 5, 1, 0, 0, 0).unwrap();
        let end = start + Duration::minutes(3);
        let report = summarize_candle_continuity(
            &quality_request(start, end),
            &[
                quality_candle(start, true),
                quality_candle(start + Duration::minutes(2), true),
            ],
            100,
        )
        .unwrap();

        let plan = plan_market_data_repair(&repair_request("1m", start, end), &report).unwrap();

        assert_eq!(plan.status, MarketDataRepairStatus::RepairPlanned);
        assert_eq!(plan.repair_ranges.len(), 1);
        assert_eq!(
            plan.repair_ranges[0].start_time,
            start + Duration::minutes(1)
        );
        assert_eq!(plan.repair_ranges[0].end_time, start + Duration::minutes(2));
    }

    #[test]
    fn repair_plan_adjacent_gaps_merge_into_one_range() {
        let start = Utc.with_ymd_and_hms(2024, 5, 1, 0, 0, 0).unwrap();
        let end = start + Duration::minutes(4);
        let report = summarize_candle_continuity(
            &quality_request(start, end),
            &[
                quality_candle(start, true),
                quality_candle(start + Duration::minutes(3), true),
            ],
            100,
        )
        .unwrap();

        let plan = plan_market_data_repair(&repair_request("1m", start, end), &report).unwrap();

        assert_eq!(plan.repair_ranges.len(), 1);
        assert_eq!(plan.repair_ranges[0].missing_candle_count, 2);
        assert_eq!(plan.repair_ranges[0].end_time, start + Duration::minutes(3));
    }

    #[test]
    fn repair_plan_separated_gaps_create_multiple_ranges() {
        let start = Utc.with_ymd_and_hms(2024, 5, 1, 0, 0, 0).unwrap();
        let end = start + Duration::minutes(6);
        let report = summarize_candle_continuity(
            &quality_request(start, end),
            &[
                quality_candle(start, true),
                quality_candle(start + Duration::minutes(2), true),
                quality_candle(start + Duration::minutes(5), true),
            ],
            100,
        )
        .unwrap();

        let plan = plan_market_data_repair(&repair_request("1m", start, end), &report).unwrap();

        assert_eq!(plan.repair_ranges.len(), 2);
        assert_eq!(
            plan.repair_ranges[0].start_time,
            start + Duration::minutes(1)
        );
        assert_eq!(
            plan.repair_ranges[1].start_time,
            start + Duration::minutes(3)
        );
    }

    #[test]
    fn repair_plan_no_gaps_returns_no_repair_needed() {
        let start = Utc.with_ymd_and_hms(2024, 5, 1, 0, 0, 0).unwrap();
        let end = start + Duration::minutes(3);
        let candles = (0..3)
            .map(|minute| quality_candle(start + Duration::minutes(minute), true))
            .collect::<Vec<_>>();
        let report =
            summarize_candle_continuity(&quality_request(start, end), &candles, 100).unwrap();

        let plan = plan_market_data_repair(&repair_request("1m", start, end), &report).unwrap();

        assert_eq!(plan.status, MarketDataRepairStatus::NoRepairNeeded);
        assert!(plan.repair_ranges.is_empty());
    }

    #[test]
    fn repair_plan_derived_15m_uses_source_1m_and_reaggregation() {
        let start = Utc.with_ymd_and_hms(2024, 5, 1, 0, 0, 0).unwrap();
        let end = start + Duration::minutes(45);
        let request = MarketDataQualityRequest {
            interval: "15m".to_string(),
            ..quality_request(start, end)
        };
        let report = summarize_candle_continuity(
            &request,
            &[
                quality_candle(start, true),
                quality_candle(start + Duration::minutes(30), true),
            ],
            100,
        )
        .unwrap();

        let plan = plan_market_data_repair(&repair_request("15m", start, end), &report).unwrap();

        assert_eq!(plan.status, MarketDataRepairStatus::RepairPlanned);
        assert!(plan.requires_source_interval);
        assert!(plan.reaggregate_derived_intervals);
        assert_eq!(plan.repair_ranges[0].source_interval, "1m");
    }

    #[test]
    fn repair_plan_max_ranges_is_enforced() {
        let start = Utc.with_ymd_and_hms(2024, 5, 1, 0, 0, 0).unwrap();
        let end = start + Duration::minutes(8);
        let report = summarize_candle_continuity(
            &quality_request(start, end),
            &[
                quality_candle(start, true),
                quality_candle(start + Duration::minutes(2), true),
                quality_candle(start + Duration::minutes(4), true),
                quality_candle(start + Duration::minutes(6), true),
            ],
            100,
        )
        .unwrap();
        let mut request = repair_request("1m", start, end);
        request.max_ranges = 2;

        let plan = plan_market_data_repair(&request, &report).unwrap();

        assert_eq!(plan.repair_ranges.len(), 2);
        assert!(plan
            .findings
            .iter()
            .any(|finding| finding.code == "repair_ranges_truncated"));
    }

    #[test]
    fn repair_plan_unsupported_interval_is_structured() {
        let start = Utc.with_ymd_and_hms(2024, 5, 1, 0, 0, 0).unwrap();
        let end = start + Duration::minutes(8);
        let report = summarize_candle_continuity(&quality_request(start, end), &[], 100).unwrap();

        let plan = plan_market_data_repair(&repair_request("2m", start, end), &report).unwrap();

        assert_eq!(plan.status, MarketDataRepairStatus::UnsupportedInterval);
        assert!(plan.repair_ranges.is_empty());
    }
}
