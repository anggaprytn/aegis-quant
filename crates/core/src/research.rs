use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Datelike, Duration, TimeZone, Utc};
use rust_decimal::{
    prelude::{FromPrimitive, ToPrimitive},
    Decimal,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    calculate_strategy_rejection_rate, summarize_candle_continuity, Candle, CandleInterval,
    CoreError, ExecutionReadinessStatus, InvalidStrategyConfigExample, MarketDataQualityReport,
    MarketDataQualityRequest, MarketDataQualityStatus, MarketDataSource, MarketProviderHealth,
    StrategyExitAttributionResult, StrategyOpportunityAnalysisResult, StrategyOpportunityStatus,
    StrategySignalFeatureAttributionResult, StrategyWalkForwardRobustnessStatus, Symbol,
    TestnetShadowRunnerConfig,
};

const REGIME_MIN_CANDLES: usize = 5;
const MANY_TRADES_PER_WINDOW_THRESHOLD: i32 = 20;
const FEW_TRADES_THRESHOLD: i32 = 2;

fn default_required_coverage_pct() -> Decimal {
    Decimal::new(95, 0)
}

fn default_shadow_pnl_holding_windows() -> Vec<u32> {
    vec![1, 3, 5, 10]
}

fn default_shadow_pnl_fee_bps() -> Decimal {
    Decimal::new(10, 0)
}

fn default_shadow_pnl_slippage_bps() -> Decimal {
    Decimal::new(5, 0)
}

fn default_shadow_pnl_extreme_threshold_pct() -> Decimal {
    Decimal::new(5, 0)
}

fn default_research_batch_base_interval() -> String {
    "1m".to_string()
}

fn default_research_batch_walk_forward_top_n() -> u32 {
    3
}

fn default_research_batch_repair_degraded_data() -> bool {
    true
}

fn default_research_batch_create_candidates() -> bool {
    true
}

fn default_research_batch_max_candidates() -> u32 {
    3
}

fn default_research_campaign_repair_degraded_data() -> bool {
    true
}

fn default_research_campaign_walk_forward_top_n() -> u32 {
    3
}

fn default_research_campaign_max_candidates_per_batch() -> u32 {
    3
}

fn default_research_campaign_lookback_candidates() -> Vec<u32> {
    vec![10, 20, 50]
}

fn default_strategy_robustness_min_trades_per_cell() -> i32 {
    5
}

fn default_strategy_robustness_min_profitable_window_ratio() -> Decimal {
    Decimal::new(5, 1)
}

fn default_research_regime_dataset_require_good_data_quality() -> bool {
    true
}

fn default_research_regime_discovery_max_windows_per_regime() -> u32 {
    20
}

fn default_research_regime_calibration_target_min_windows_per_regime() -> u32 {
    5
}

fn default_research_regime_priority_order() -> Vec<ResearchRegimeLabel> {
    vec![
        ResearchRegimeLabel::HighVolatility,
        ResearchRegimeLabel::TrendUp,
        ResearchRegimeLabel::TrendDown,
        ResearchRegimeLabel::Range,
        ResearchRegimeLabel::LowVolatility,
    ]
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ScheduledResearchJobStatus {
    Disabled,
    Enabled,
    Paused,
    Running,
    BackingOff,
    Error,
    AutoPaused,
}

impl ScheduledResearchJobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "DISABLED",
            Self::Enabled => "ENABLED",
            Self::Paused => "PAUSED",
            Self::Running => "RUNNING",
            Self::BackingOff => "BACKING_OFF",
            Self::Error => "ERROR",
            Self::AutoPaused => "AUTO_PAUSED",
        }
    }
}

impl std::str::FromStr for ScheduledResearchJobStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "DISABLED" => Ok(Self::Disabled),
            "ENABLED" => Ok(Self::Enabled),
            "PAUSED" => Ok(Self::Paused),
            "RUNNING" => Ok(Self::Running),
            "BACKING_OFF" => Ok(Self::BackingOff),
            "ERROR" => Ok(Self::Error),
            "AUTO_PAUSED" => Ok(Self::AutoPaused),
            other => Err(CoreError::UnsupportedScheduledResearchJobStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ScheduledResearchJobKind {
    ProviderHealth,
    MarketDataQuality,
    AggregationStatus,
    CandidateShadowObserveOnce,
    CrossAssetCandidateShadowObserveOnce,
    ResearchBatch,
    ResearchCampaign,
    RegimeDiscovery,
    RobustnessMatrix,
    OperatorReport,
}

impl ScheduledResearchJobKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProviderHealth => "PROVIDER_HEALTH",
            Self::MarketDataQuality => "MARKET_DATA_QUALITY",
            Self::AggregationStatus => "AGGREGATION_STATUS",
            Self::CandidateShadowObserveOnce => "CANDIDATE_SHADOW_OBSERVE_ONCE",
            Self::CrossAssetCandidateShadowObserveOnce => {
                "CROSS_ASSET_CANDIDATE_SHADOW_OBSERVE_ONCE"
            }
            Self::ResearchBatch => "RESEARCH_BATCH",
            Self::ResearchCampaign => "RESEARCH_CAMPAIGN",
            Self::RegimeDiscovery => "REGIME_DISCOVERY",
            Self::RobustnessMatrix => "ROBUSTNESS_MATRIX",
            Self::OperatorReport => "OPERATOR_REPORT",
        }
    }

    pub fn is_safe_research_kind(self) -> bool {
        matches!(
            self,
            Self::ProviderHealth
                | Self::MarketDataQuality
                | Self::AggregationStatus
                | Self::CandidateShadowObserveOnce
                | Self::CrossAssetCandidateShadowObserveOnce
                | Self::ResearchBatch
                | Self::ResearchCampaign
                | Self::RegimeDiscovery
                | Self::RobustnessMatrix
                | Self::OperatorReport
        )
    }
}

impl std::str::FromStr for ScheduledResearchJobKind {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "PROVIDER_HEALTH" => Ok(Self::ProviderHealth),
            "MARKET_DATA_QUALITY" => Ok(Self::MarketDataQuality),
            "AGGREGATION_STATUS" => Ok(Self::AggregationStatus),
            "CANDIDATE_SHADOW_OBSERVE_ONCE" => Ok(Self::CandidateShadowObserveOnce),
            "CROSS_ASSET_CANDIDATE_SHADOW_OBSERVE_ONCE" => {
                Ok(Self::CrossAssetCandidateShadowObserveOnce)
            }
            "RESEARCH_BATCH" => Ok(Self::ResearchBatch),
            "RESEARCH_CAMPAIGN" => Ok(Self::ResearchCampaign),
            "REGIME_DISCOVERY" => Ok(Self::RegimeDiscovery),
            "ROBUSTNESS_MATRIX" => Ok(Self::RobustnessMatrix),
            "OPERATOR_REPORT" => Ok(Self::OperatorReport),
            other => Err(CoreError::UnsupportedScheduledResearchJobKind(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ScheduledResearchJobRunStatus {
    Completed,
    Failed,
    Skipped,
    SkippedOverlap,
    SkippedBackoff,
    PartialSuccess,
}

impl ScheduledResearchJobRunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Skipped => "SKIPPED",
            Self::SkippedOverlap => "SKIPPED_OVERLAP",
            Self::SkippedBackoff => "SKIPPED_BACKOFF",
            Self::PartialSuccess => "PARTIAL_SUCCESS",
        }
    }
}

impl std::str::FromStr for ScheduledResearchJobRunStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            "SKIPPED" => Ok(Self::Skipped),
            "SKIPPED_OVERLAP" => Ok(Self::SkippedOverlap),
            "SKIPPED_BACKOFF" => Ok(Self::SkippedBackoff),
            "PARTIAL_SUCCESS" => Ok(Self::PartialSuccess),
            other => Err(CoreError::UnsupportedScheduledResearchJobRunStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScheduledResearchJob {
    pub id: Uuid,
    pub name: String,
    pub kind: ScheduledResearchJobKind,
    pub enabled: bool,
    pub interval_seconds: i64,
    pub request: Value,
    pub max_runs_per_tick: i32,
    pub last_run_at: Option<DateTime<Utc>>,
    pub last_failure_at: Option<DateTime<Utc>>,
    pub last_failure_reason: Option<String>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub next_run_at: Option<DateTime<Utc>>,
    pub backoff_until: Option<DateTime<Utc>>,
    pub consecutive_failure_count: i32,
    pub auto_paused_reason: Option<String>,
    pub status: ScheduledResearchJobStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScheduledResearchJobRun {
    pub id: Uuid,
    pub job_id: Uuid,
    pub status: ScheduledResearchJobRunStatus,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub result: Value,
    pub error: Option<String>,
    pub created_artifact_type: Option<String>,
    pub created_artifact_id: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScheduledResearchJobRequest {
    pub name: String,
    pub kind: ScheduledResearchJobKind,
    #[serde(default)]
    pub enabled: bool,
    pub interval_seconds: i64,
    #[serde(default)]
    pub request: Value,
    #[serde(default = "default_scheduled_research_max_runs_per_tick")]
    pub max_runs_per_tick: i32,
    pub next_run_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduledResearchBootstrapSafeRequest {
    #[serde(default)]
    pub enable: bool,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub intervals: Vec<String>,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub replace_existing: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScheduledResearchBootstrapSafePlanItem {
    pub name: String,
    pub kind: ScheduledResearchJobKind,
    pub interval_seconds: i64,
    pub enabled: bool,
    pub request: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScheduledResearchBootstrapSafeJobResult {
    pub name: String,
    pub kind: ScheduledResearchJobKind,
    pub action: String,
    pub job: Option<ScheduledResearchJob>,
    pub planned: ScheduledResearchBootstrapSafePlanItem,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScheduledResearchBootstrapSafeResult {
    pub dry_run: bool,
    pub replace_existing: bool,
    pub requested_enabled: bool,
    pub created: i64,
    pub existing: i64,
    pub updated: i64,
    pub skipped: i64,
    pub jobs: Vec<ScheduledResearchBootstrapSafeJobResult>,
}

fn default_scheduled_research_max_runs_per_tick() -> i32 {
    1
}

impl ScheduledResearchJobRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.name.trim().is_empty() {
            return Err(CoreError::EmptyScheduledResearchJobName);
        }
        if !self.kind.is_safe_research_kind() {
            return Err(CoreError::UnsafeScheduledResearchJobKind(
                self.kind.as_str().to_string(),
            ));
        }
        if self.interval_seconds <= 0 {
            return Err(CoreError::InvalidScheduledResearchJobInterval);
        }
        if self.max_runs_per_tick <= 0 {
            return Err(CoreError::InvalidScheduledResearchJobMaxRuns);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduledResearchJobControlRequest {
    pub reason: Option<String>,
    pub correlation_id: Option<Uuid>,
}

pub fn scheduled_research_next_run_at(
    completed_at: DateTime<Utc>,
    interval_seconds: i64,
) -> Result<DateTime<Utc>, CoreError> {
    if interval_seconds <= 0 {
        return Err(CoreError::InvalidScheduledResearchJobInterval);
    }
    Ok(completed_at + Duration::seconds(interval_seconds))
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchBatchStatus {
    Started,
    Partial,
    Completed,
    Failed,
}

impl ResearchBatchStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Started => "STARTED",
            Self::Partial => "PARTIAL",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
        }
    }
}

impl std::str::FromStr for ResearchBatchStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "STARTED" => Ok(Self::Started),
            "PARTIAL" => Ok(Self::Partial),
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            other => Err(CoreError::UnsupportedResearchBatchStatus(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchBatchStepStatus {
    Pending,
    Running,
    Completed,
    Skipped,
    Failed,
}

impl ResearchBatchStepStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Completed => "COMPLETED",
            Self::Skipped => "SKIPPED",
            Self::Failed => "FAILED",
        }
    }
}

impl std::str::FromStr for ResearchBatchStepStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "PENDING" => Ok(Self::Pending),
            "RUNNING" => Ok(Self::Running),
            "COMPLETED" => Ok(Self::Completed),
            "SKIPPED" => Ok(Self::Skipped),
            "FAILED" => Ok(Self::Failed),
            other => Err(CoreError::UnsupportedResearchBatchStepStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchBatchRequest {
    pub strategy_id: String,
    pub symbol: String,
    #[serde(default = "default_research_batch_base_interval")]
    pub base_interval: String,
    pub target_intervals: Vec<String>,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub initial_capital: Decimal,
    pub fee_bps: Decimal,
    pub slippage_bps: Decimal,
    pub experiment_timeframes: Vec<String>,
    pub lookback_candidates: Vec<u32>,
    pub trend_lookback_candidates: Option<Vec<u32>>,
    pub momentum_lookback_candidates: Option<Vec<u32>>,
    pub compression_lookback_candidates: Option<Vec<u32>>,
    pub breakout_lookback_candidates: Option<Vec<u32>>,
    pub pullback_lookback_candidates: Option<Vec<u32>>,
    pub pullback_sma_lookback_candidates: Option<Vec<u32>>,
    pub compression_percentile_threshold_candidates: Option<Vec<Decimal>>,
    pub min_breakout_pct_candidates: Option<Vec<Decimal>>,
    pub max_breakout_extension_pct_candidates: Option<Vec<Decimal>>,
    pub min_volume_expansion_ratio_candidates: Option<Vec<Decimal>>,
    pub lower_band_pct_candidates: Option<Vec<Decimal>>,
    pub upper_band_pct_candidates: Option<Vec<Decimal>>,
    pub min_range_width_pct_candidates: Option<Vec<Decimal>>,
    pub max_range_width_pct_candidates: Option<Vec<Decimal>>,
    pub min_close_above_sma_pct_candidates: Option<Vec<Decimal>>,
    pub max_close_above_sma_pct_candidates: Option<Vec<Decimal>>,
    pub min_momentum_return_pct_candidates: Option<Vec<Decimal>>,
    pub min_trend_return_pct_candidates: Option<Vec<Decimal>>,
    pub min_trend_slope_pct_candidates: Option<Vec<Decimal>>,
    pub min_pullback_depth_pct_candidates: Option<Vec<Decimal>>,
    pub max_pullback_depth_pct_candidates: Option<Vec<Decimal>>,
    pub min_reclaim_pct_candidates: Option<Vec<Decimal>>,
    pub min_breakdown_pct_candidates: Option<Vec<Decimal>>,
    pub min_reclaim_close_pct_candidates: Option<Vec<Decimal>>,
    pub min_lower_wick_pct_candidates: Option<Vec<Decimal>>,
    pub min_volume_ratio_candidates: Option<Vec<Decimal>>,
    pub max_choppiness_candidates: Option<Vec<Decimal>>,
    pub holding_candles_candidates: Option<Vec<u32>>,
    #[serde(default = "default_research_batch_walk_forward_top_n")]
    pub walk_forward_top_n: u32,
    #[serde(default = "default_research_batch_repair_degraded_data")]
    pub repair_degraded_data: bool,
    #[serde(default = "default_research_batch_create_candidates")]
    pub create_candidates: bool,
    #[serde(default = "default_batch_candidate_creation_mode")]
    pub candidate_creation_mode: ResearchCandidateCreationMode,
    #[serde(default = "default_research_batch_max_candidates")]
    pub max_candidates: u32,
    pub correlation_id: Option<Uuid>,
}

impl ResearchBatchRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.strategy_id.trim().is_empty() {
            return Err(CoreError::EmptyStrategyExperimentStrategyId);
        }
        if self.symbol.trim().is_empty() {
            return Err(CoreError::EmptyStrategyExperimentSymbol);
        }
        self.base_interval.parse::<CandleInterval>()?;
        for interval in self
            .target_intervals
            .iter()
            .chain(self.experiment_timeframes.iter())
        {
            if interval.trim().is_empty() {
                return Err(CoreError::EmptyStrategyExperimentTimeframe);
            }
            interval.parse::<CandleInterval>()?;
        }
        if self.end_time <= self.start_time {
            return Err(CoreError::InvalidStrategyExperimentTimeRange);
        }
        if self.initial_capital <= Decimal::ZERO {
            return Err(CoreError::InvalidStrategyExperimentInitialCapital);
        }
        if self.fee_bps < Decimal::ZERO {
            return Err(CoreError::InvalidBacktestBps("fee_bps".to_string()));
        }
        if self.slippage_bps < Decimal::ZERO {
            return Err(CoreError::InvalidBacktestBps("slippage_bps".to_string()));
        }
        if self.experiment_timeframes.is_empty() {
            return Err(CoreError::EmptyStrategyExperimentTimeframes);
        }
        if self.lookback_candidates.is_empty() {
            return Err(CoreError::EmptyStrategyExperimentCandidates);
        }
        if self.walk_forward_top_n == 0 || self.max_candidates == 0 {
            return Err(CoreError::InvalidStrategyExperimentMaxRuns);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchBatchStep {
    pub id: Uuid,
    pub batch_id: Uuid,
    pub step_name: String,
    pub status: ResearchBatchStepStatus,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub result: Option<Value>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchBatchCandidateSummary {
    pub experiment_id: Uuid,
    pub experiment_run_id: Uuid,
    pub walk_forward_run_id: Option<Uuid>,
    pub candidate_id: Option<Uuid>,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub score: Decimal,
    pub pnl_pct: Decimal,
    pub max_drawdown_pct: Decimal,
    pub trade_count: i32,
    pub win_rate: Decimal,
    pub robustness_status: Option<StrategyWalkForwardRobustnessStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchBatchRecommendation {
    pub severity: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchBatchResult {
    pub batch_id: Uuid,
    pub status: ResearchBatchStatus,
    pub steps: Vec<ResearchBatchStep>,
    pub provider_health_summary: Option<MarketProviderHealth>,
    pub backfill_summary: Option<Value>,
    pub quality_before: Option<MarketDataQualityReport>,
    pub repair_summary: Option<Value>,
    pub quality_after: Option<MarketDataQualityReport>,
    pub aggregation_summary: Option<Value>,
    pub experiment_ids: Vec<Uuid>,
    #[serde(default)]
    pub total_candidate_configs: i32,
    #[serde(default)]
    pub skipped_invalid_config_count: i32,
    #[serde(default)]
    pub executed_config_count: i32,
    #[serde(default)]
    pub invalid_config_examples: Vec<InvalidStrategyConfigExample>,
    pub walk_forward_run_ids: Vec<Uuid>,
    pub created_candidate_ids: Vec<Uuid>,
    #[serde(default)]
    pub candidates_blocked_by_gate: i32,
    #[serde(default)]
    pub proposals_created: i32,
    #[serde(default)]
    pub gate_decisions: Vec<ResearchCandidateCreationDecision>,
    pub top_candidates: Vec<ResearchBatchCandidateSummary>,
    pub recommendations: Vec<ResearchBatchRecommendation>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchBatchTriageStatus {
    Actionable,
    Weak,
    OverfitOnly,
    NoCandidates,
    DataQualityBlocked,
    Failed,
    Unknown,
}

impl ResearchBatchTriageStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Actionable => "ACTIONABLE",
            Self::Weak => "WEAK",
            Self::OverfitOnly => "OVERFIT_ONLY",
            Self::NoCandidates => "NO_CANDIDATES",
            Self::DataQualityBlocked => "DATA_QUALITY_BLOCKED",
            Self::Failed => "FAILED",
            Self::Unknown => "UNKNOWN",
        }
    }
}

impl std::str::FromStr for ResearchBatchTriageStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "ACTIONABLE" => Ok(Self::Actionable),
            "WEAK" => Ok(Self::Weak),
            "OVERFIT_ONLY" => Ok(Self::OverfitOnly),
            "NO_CANDIDATES" => Ok(Self::NoCandidates),
            "DATA_QUALITY_BLOCKED" => Ok(Self::DataQualityBlocked),
            "FAILED" => Ok(Self::Failed),
            "UNKNOWN" => Ok(Self::Unknown),
            other => Err(CoreError::UnsupportedResearchBatchStatus(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchBatchTriageFinding {
    pub severity: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchBatchTriageRecommendation {
    pub priority: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchBatchCandidateTriage {
    pub candidate_id: Option<Uuid>,
    pub experiment_run_id: Uuid,
    pub walk_forward_run_id: Option<Uuid>,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub experiment_score: Decimal,
    pub experiment_pnl_pct: Decimal,
    pub walk_forward_status: Option<String>,
    pub walk_forward_recommendation: Option<String>,
    pub qualification_status: Option<String>,
    pub dossier_status: Option<String>,
    pub triage_status: ResearchBatchTriageStatus,
    pub rank: i32,
    pub reasons: Vec<String>,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchBatchTriage {
    pub batch_id: Uuid,
    pub status: ResearchBatchTriageStatus,
    pub candidate_count: i32,
    pub actionable_count: i32,
    pub weak_count: i32,
    pub overfit_count: i32,
    pub candidates: Vec<ResearchBatchCandidateTriage>,
    pub findings: Vec<ResearchBatchTriageFinding>,
    pub recommendations: Vec<ResearchBatchTriageRecommendation>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateCreationMode {
    CreateAll,
    CreateActionableOnly,
    CreatePromisingOnly,
    ProposalOnly,
    Disabled,
}

impl ResearchCandidateCreationMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CreateAll => "CREATE_ALL",
            Self::CreateActionableOnly => "CREATE_ACTIONABLE_ONLY",
            Self::CreatePromisingOnly => "CREATE_PROMISING_ONLY",
            Self::ProposalOnly => "PROPOSAL_ONLY",
            Self::Disabled => "DISABLED",
        }
    }

    pub fn should_create_proposal_for_blocked(self) -> bool {
        matches!(
            self,
            Self::CreateActionableOnly | Self::CreatePromisingOnly | Self::ProposalOnly
        )
    }
}

impl std::str::FromStr for ResearchCandidateCreationMode {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim().replace('-', "_").to_ascii_uppercase();
        match normalized.as_str() {
            "CREATE_ALL" => Ok(Self::CreateAll),
            "CREATE_ACTIONABLE_ONLY" | "ACTIONABLE_ONLY" => Ok(Self::CreateActionableOnly),
            "CREATE_PROMISING_ONLY" | "PROMISING_ONLY" => Ok(Self::CreatePromisingOnly),
            "PROPOSAL_ONLY" => Ok(Self::ProposalOnly),
            "DISABLED" => Ok(Self::Disabled),
            other => Err(CoreError::UnsupportedResearchCandidateCreationMode(
                other.to_string(),
            )),
        }
    }
}

pub fn default_research_candidate_creation_min_trades() -> i32 {
    3
}

pub fn default_research_candidate_creation_min_score() -> Decimal {
    Decimal::ZERO
}

pub fn default_campaign_candidate_creation_mode() -> ResearchCandidateCreationMode {
    ResearchCandidateCreationMode::CreateActionableOnly
}

pub fn default_batch_candidate_creation_mode() -> ResearchCandidateCreationMode {
    ResearchCandidateCreationMode::CreateAll
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateCreationPolicy {
    pub mode: ResearchCandidateCreationMode,
    #[serde(default = "default_research_candidate_creation_min_trades")]
    pub min_trade_count: i32,
    #[serde(default = "default_research_candidate_creation_min_score")]
    pub min_score: Decimal,
}

impl ResearchCandidateCreationPolicy {
    pub fn for_mode(mode: ResearchCandidateCreationMode) -> Self {
        Self {
            mode,
            min_trade_count: default_research_candidate_creation_min_trades(),
            min_score: default_research_candidate_creation_min_score(),
        }
    }
}

impl Default for ResearchCandidateCreationPolicy {
    fn default() -> Self {
        Self::for_mode(default_campaign_candidate_creation_mode())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateCreationDecision {
    pub should_create_candidate: bool,
    #[serde(default)]
    pub should_create_proposal: bool,
    pub reason: String,
    pub blockers: Vec<String>,
    pub warnings: Vec<String>,
    pub source_batch_id: Option<Uuid>,
    #[serde(default)]
    pub source_experiment_run_id: Option<Uuid>,
    #[serde(default)]
    pub source_walk_forward_run_id: Option<Uuid>,
    #[serde(default)]
    pub source_robustness_matrix_run_id: Option<Uuid>,
    pub experiment_run_id: Uuid,
    pub walk_forward_status: Option<String>,
    pub batch_triage_status: ResearchBatchTriageStatus,
    pub robustness_status: Option<String>,
    #[serde(default)]
    pub data_quality_status: Option<String>,
    #[serde(default)]
    pub normalized_strategy_config: Option<Value>,
    #[serde(default)]
    pub config_fingerprint: Option<String>,
    #[serde(default)]
    pub evidence_status_summary: Option<Value>,
    #[serde(default)]
    pub gate_evidence_mismatch: bool,
    pub pnl_pct: Decimal,
    pub score: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateCreationGateResult {
    pub policy: ResearchCandidateCreationPolicy,
    pub decisions: Vec<ResearchCandidateCreationDecision>,
    pub candidates_created: i32,
    pub candidates_blocked_by_gate: i32,
    pub proposals_created: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateCreationInput {
    pub source_batch_id: Option<Uuid>,
    pub experiment_run_id: Uuid,
    pub source_walk_forward_run_id: Option<Uuid>,
    pub source_robustness_matrix_run_id: Option<Uuid>,
    pub walk_forward_status: Option<StrategyWalkForwardRobustnessStatus>,
    pub batch_triage_status: ResearchBatchTriageStatus,
    pub robustness_status: Option<StrategyRobustnessMatrixStatus>,
    pub data_quality_status: Option<MarketDataQualityStatus>,
    pub normalized_strategy_config: Option<Value>,
    pub config_fingerprint: Option<String>,
    pub evidence_status_summary: Option<Value>,
    pub gate_evidence_mismatch: bool,
    pub trade_count: i32,
    pub pnl_pct: Decimal,
    pub score: Decimal,
}

pub fn evaluate_research_candidate_creation(
    policy: &ResearchCandidateCreationPolicy,
    input: ResearchCandidateCreationInput,
) -> ResearchCandidateCreationDecision {
    let mut blockers = Vec::new();
    let mut warnings = Vec::new();

    match policy.mode {
        ResearchCandidateCreationMode::CreateAll => {
            warnings.push("create_all_override_bypassed_candidate_creation_gate".to_string());
            return ResearchCandidateCreationDecision {
                should_create_candidate: true,
                should_create_proposal: false,
                reason: "CREATE_ALL override allowed candidate creation.".to_string(),
                blockers,
                warnings,
                source_batch_id: input.source_batch_id,
                source_experiment_run_id: Some(input.experiment_run_id),
                source_walk_forward_run_id: input.source_walk_forward_run_id,
                source_robustness_matrix_run_id: input.source_robustness_matrix_run_id,
                experiment_run_id: input.experiment_run_id,
                walk_forward_status: input
                    .walk_forward_status
                    .map(|status| status.as_str().to_string()),
                batch_triage_status: input.batch_triage_status,
                robustness_status: input
                    .robustness_status
                    .map(|status| status.as_str().to_string()),
                data_quality_status: input
                    .data_quality_status
                    .map(|status| status.as_str().to_string()),
                normalized_strategy_config: input.normalized_strategy_config,
                config_fingerprint: input.config_fingerprint,
                evidence_status_summary: input.evidence_status_summary,
                gate_evidence_mismatch: input.gate_evidence_mismatch,
                pnl_pct: input.pnl_pct,
                score: input.score,
            };
        }
        ResearchCandidateCreationMode::Disabled => {
            blockers.push("candidate_creation_disabled".to_string());
        }
        ResearchCandidateCreationMode::ProposalOnly => {
            blockers.push("candidate_creation_proposal_only".to_string());
        }
        ResearchCandidateCreationMode::CreateActionableOnly
        | ResearchCandidateCreationMode::CreatePromisingOnly => {}
    }

    match input.batch_triage_status {
        ResearchBatchTriageStatus::Actionable => {}
        ResearchBatchTriageStatus::OverfitOnly => {
            blockers.push("batch_triage_overfit_only".to_string())
        }
        ResearchBatchTriageStatus::Weak => blockers.push("batch_triage_weak".to_string()),
        ResearchBatchTriageStatus::DataQualityBlocked => {
            blockers.push("batch_triage_data_quality_blocked".to_string())
        }
        ResearchBatchTriageStatus::NoCandidates => {
            blockers.push("batch_triage_no_candidates".to_string())
        }
        ResearchBatchTriageStatus::Failed => blockers.push("batch_triage_failed".to_string()),
        ResearchBatchTriageStatus::Unknown => blockers.push("batch_triage_unknown".to_string()),
    }

    match input.walk_forward_status {
        Some(StrategyWalkForwardRobustnessStatus::Robust) => {}
        Some(StrategyWalkForwardRobustnessStatus::Weak)
            if policy.mode == ResearchCandidateCreationMode::CreatePromisingOnly =>
        {
            warnings.push("walk_forward_weak_promising_only".to_string());
        }
        Some(StrategyWalkForwardRobustnessStatus::Weak) => {
            blockers.push("walk_forward_weak".to_string())
        }
        Some(StrategyWalkForwardRobustnessStatus::OverfitRisk) => {
            blockers.push("walk_forward_overfit_risk".to_string())
        }
        Some(StrategyWalkForwardRobustnessStatus::InsufficientData) => {
            blockers.push("walk_forward_insufficient_data".to_string())
        }
        Some(StrategyWalkForwardRobustnessStatus::Failed) => {
            blockers.push("walk_forward_failed".to_string())
        }
        None => blockers.push("walk_forward_missing".to_string()),
    }

    if input.robustness_status == Some(StrategyRobustnessMatrixStatus::Negative) {
        blockers.push("robustness_matrix_negative".to_string());
    }
    if input.gate_evidence_mismatch {
        warnings.push("gate_evidence_mismatch".to_string());
    }

    match input.data_quality_status {
        Some(MarketDataQualityStatus::Good) => {}
        Some(MarketDataQualityStatus::Degraded) => {
            blockers.push("data_quality_degraded".to_string())
        }
        Some(MarketDataQualityStatus::Bad) => blockers.push("data_quality_bad".to_string()),
        Some(MarketDataQualityStatus::InsufficientData) => {
            blockers.push("data_quality_insufficient_data".to_string())
        }
        Some(MarketDataQualityStatus::Unknown) | None => {
            blockers.push("data_quality_not_good".to_string())
        }
    }

    if input.trade_count < policy.min_trade_count {
        blockers.push(format!(
            "trade_count_below_threshold:{}<{}",
            input.trade_count, policy.min_trade_count
        ));
    }
    if input.score < policy.min_score {
        blockers.push(format!(
            "score_below_threshold:{}<{}",
            input.score, policy.min_score
        ));
    }
    if input.pnl_pct < Decimal::ZERO
        && input.walk_forward_status != Some(StrategyWalkForwardRobustnessStatus::Robust)
    {
        blockers.push("negative_pnl_without_strong_walk_forward".to_string());
    }

    blockers.sort();
    blockers.dedup();
    warnings.sort();
    warnings.dedup();

    let should_create_candidate = blockers.is_empty();
    let should_create_proposal =
        !should_create_candidate && policy.mode.should_create_proposal_for_blocked();
    let reason = if should_create_candidate {
        "Candidate creation allowed by gate.".to_string()
    } else {
        format!(
            "Candidate creation blocked by gate: {}.",
            blockers.join(", ")
        )
    };

    ResearchCandidateCreationDecision {
        should_create_candidate,
        should_create_proposal,
        reason,
        blockers,
        warnings,
        source_batch_id: input.source_batch_id,
        source_experiment_run_id: Some(input.experiment_run_id),
        source_walk_forward_run_id: input.source_walk_forward_run_id,
        source_robustness_matrix_run_id: input.source_robustness_matrix_run_id,
        experiment_run_id: input.experiment_run_id,
        walk_forward_status: input
            .walk_forward_status
            .map(|status| status.as_str().to_string()),
        batch_triage_status: input.batch_triage_status,
        robustness_status: input
            .robustness_status
            .map(|status| status.as_str().to_string()),
        data_quality_status: input
            .data_quality_status
            .map(|status| status.as_str().to_string()),
        normalized_strategy_config: input.normalized_strategy_config,
        config_fingerprint: input.config_fingerprint,
        evidence_status_summary: input.evidence_status_summary,
        gate_evidence_mismatch: input.gate_evidence_mismatch,
        pnl_pct: input.pnl_pct,
        score: input.score,
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCampaignStatus {
    Started,
    Completed,
    PartialSuccess,
    Failed,
    Cancelled,
}

impl ResearchCampaignStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Started => "STARTED",
            Self::Completed => "COMPLETED",
            Self::PartialSuccess => "PARTIAL_SUCCESS",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
        }
    }
}

impl std::str::FromStr for ResearchCampaignStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "STARTED" => Ok(Self::Started),
            "COMPLETED" => Ok(Self::Completed),
            "PARTIAL_SUCCESS" => Ok(Self::PartialSuccess),
            "FAILED" => Ok(Self::Failed),
            "CANCELLED" => Ok(Self::Cancelled),
            other => Err(CoreError::UnsupportedResearchCampaignStatus(
                other.to_string(),
            )),
        }
    }
}

pub const RESEARCH_STALE_RUN_RECOVERY_CONFIRMATION: &str = "RECOVER STALE RESEARCH RUNS";
pub const DEFAULT_RESEARCH_STALE_RUN_OLDER_THAN_MINUTES: i64 = 60;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchStaleRunRecoveryTargetType {
    ResearchCampaign,
    ResearchCampaignBatch,
    ResearchBatch,
    ResearchBatchStep,
    StrategyExperiment,
    StrategyWalkForwardRun,
    StrategyRobustnessMatrixRun,
    ResearchExperimentPlanRun,
}

impl ResearchStaleRunRecoveryTargetType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ResearchCampaign => "RESEARCH_CAMPAIGN",
            Self::ResearchCampaignBatch => "RESEARCH_CAMPAIGN_BATCH",
            Self::ResearchBatch => "RESEARCH_BATCH",
            Self::ResearchBatchStep => "RESEARCH_BATCH_STEP",
            Self::StrategyExperiment => "STRATEGY_EXPERIMENT",
            Self::StrategyWalkForwardRun => "STRATEGY_WALK_FORWARD_RUN",
            Self::StrategyRobustnessMatrixRun => "STRATEGY_ROBUSTNESS_MATRIX_RUN",
            Self::ResearchExperimentPlanRun => "RESEARCH_EXPERIMENT_PLAN_RUN",
        }
    }
}

impl std::str::FromStr for ResearchStaleRunRecoveryTargetType {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "RESEARCH_CAMPAIGN" | "RESEARCH_CAMPAIGNS" => Ok(Self::ResearchCampaign),
            "RESEARCH_CAMPAIGN_BATCH" | "RESEARCH_CAMPAIGN_BATCHES" => {
                Ok(Self::ResearchCampaignBatch)
            }
            "RESEARCH_BATCH" | "RESEARCH_BATCHES" => Ok(Self::ResearchBatch),
            "RESEARCH_BATCH_STEP" | "RESEARCH_BATCH_STEPS" => Ok(Self::ResearchBatchStep),
            "STRATEGY_EXPERIMENT" | "STRATEGY_EXPERIMENTS" => Ok(Self::StrategyExperiment),
            "STRATEGY_WALK_FORWARD_RUN" | "STRATEGY_WALK_FORWARD_RUNS" => {
                Ok(Self::StrategyWalkForwardRun)
            }
            "STRATEGY_ROBUSTNESS_MATRIX_RUN" | "STRATEGY_ROBUSTNESS_MATRIX_RUNS" => {
                Ok(Self::StrategyRobustnessMatrixRun)
            }
            "RESEARCH_EXPERIMENT_PLAN_RUN" | "RESEARCH_EXPERIMENT_PLAN_RUNS" => {
                Ok(Self::ResearchExperimentPlanRun)
            }
            other => Err(CoreError::UnsupportedResearchStaleRunRecoveryTargetType(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchStaleRunRecoveryStatus {
    Proposed,
    Recovered,
    Skipped,
}

impl ResearchStaleRunRecoveryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Proposed => "PROPOSED",
            Self::Recovered => "RECOVERED",
            Self::Skipped => "SKIPPED",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchStaleRunRecoveryRequest {
    #[serde(default = "default_research_stale_run_older_than_minutes")]
    pub older_than_minutes: i64,
    #[serde(default = "default_research_stale_run_dry_run")]
    pub dry_run: bool,
    #[serde(default)]
    pub target_types: Option<Vec<ResearchStaleRunRecoveryTargetType>>,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub correlation_id: Option<Uuid>,
    #[serde(default)]
    pub confirmation: Option<String>,
}

impl Default for ResearchStaleRunRecoveryRequest {
    fn default() -> Self {
        Self {
            older_than_minutes: DEFAULT_RESEARCH_STALE_RUN_OLDER_THAN_MINUTES,
            dry_run: true,
            target_types: None,
            limit: None,
            correlation_id: None,
            confirmation: None,
        }
    }
}

impl ResearchStaleRunRecoveryRequest {
    pub fn normalized_older_than_minutes(&self) -> i64 {
        self.older_than_minutes.max(1)
    }

    pub fn normalized_limit(&self) -> Option<usize> {
        self.limit
            .and_then(|value| usize::try_from(value).ok())
            .filter(|value| *value > 0)
    }

    pub fn confirmation_matches(&self) -> bool {
        self.confirmation.as_deref() == Some(RESEARCH_STALE_RUN_RECOVERY_CONFIRMATION)
    }
}

fn default_research_stale_run_older_than_minutes() -> i64 {
    DEFAULT_RESEARCH_STALE_RUN_OLDER_THAN_MINUTES
}

fn default_research_stale_run_dry_run() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchStaleRunRecoveryTarget {
    pub target_type: ResearchStaleRunRecoveryTargetType,
    pub target_id: Uuid,
    pub current_status: String,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
    pub age_minutes: i64,
    pub proposed_status: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchStaleRunRecoveryAction {
    pub target_type: ResearchStaleRunRecoveryTargetType,
    pub target_id: Uuid,
    pub action: String,
    pub from_status: String,
    pub to_status: String,
    pub reason: String,
    pub status: ResearchStaleRunRecoveryStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchStaleRunRecoveryResult {
    pub scanned_count: i64,
    pub stale_count: i64,
    pub recovered_count: i64,
    pub skipped_count: i64,
    pub targets: Vec<ResearchStaleRunRecoveryTarget>,
    #[serde(default)]
    pub actions: Vec<ResearchStaleRunRecoveryAction>,
    pub warnings: Vec<String>,
}

pub fn is_research_stale_candidate_status(status: &str) -> bool {
    matches!(
        status.trim().to_ascii_uppercase().as_str(),
        "STARTED" | "RUNNING" | "IN_PROGRESS" | "PENDING"
    )
}

pub fn stale_research_run_reason(age_minutes: i64) -> String {
    format!("Marked stale after no completion update for {age_minutes} minutes.")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCampaignWindow {
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    #[serde(default)]
    pub regime_label: Option<ResearchRegimeLabel>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCampaignRequest {
    pub strategies: Vec<String>,
    pub symbols: Vec<String>,
    pub experiment_timeframes: Vec<String>,
    #[serde(default)]
    pub windows: Vec<ResearchCampaignWindow>,
    pub campaign_start: Option<DateTime<Utc>>,
    pub campaign_end: Option<DateTime<Utc>>,
    pub window_hours: Option<i64>,
    pub step_hours: Option<i64>,
    pub initial_capital: Decimal,
    pub fee_bps: Decimal,
    pub slippage_bps: Decimal,
    pub max_batches: Option<u32>,
    #[serde(default)]
    pub regime_dataset_id: Option<Uuid>,
    #[serde(default)]
    pub target_regimes: Option<Vec<ResearchRegimeLabel>>,
    #[serde(default)]
    pub max_windows_per_regime: Option<u32>,
    #[serde(default = "default_research_campaign_max_candidates_per_batch")]
    pub max_candidates_per_batch: u32,
    #[serde(default = "default_research_batch_create_candidates")]
    pub create_candidates: bool,
    #[serde(default = "default_campaign_candidate_creation_mode")]
    pub candidate_creation_mode: ResearchCandidateCreationMode,
    #[serde(default = "default_research_campaign_repair_degraded_data")]
    pub repair_degraded_data: bool,
    #[serde(default = "default_research_campaign_walk_forward_top_n")]
    pub walk_forward_top_n: u32,
    #[serde(default = "default_research_batch_base_interval")]
    pub base_interval: String,
    #[serde(default = "default_research_campaign_lookback_candidates")]
    pub lookback_candidates: Vec<u32>,
    pub trend_lookback_candidates: Option<Vec<u32>>,
    pub momentum_lookback_candidates: Option<Vec<u32>>,
    pub compression_lookback_candidates: Option<Vec<u32>>,
    pub breakout_lookback_candidates: Option<Vec<u32>>,
    pub pullback_lookback_candidates: Option<Vec<u32>>,
    pub pullback_sma_lookback_candidates: Option<Vec<u32>>,
    pub compression_percentile_threshold_candidates: Option<Vec<Decimal>>,
    pub min_breakout_pct_candidates: Option<Vec<Decimal>>,
    pub max_breakout_extension_pct_candidates: Option<Vec<Decimal>>,
    pub min_volume_expansion_ratio_candidates: Option<Vec<Decimal>>,
    pub lower_band_pct_candidates: Option<Vec<Decimal>>,
    pub upper_band_pct_candidates: Option<Vec<Decimal>>,
    pub min_range_width_pct_candidates: Option<Vec<Decimal>>,
    pub max_range_width_pct_candidates: Option<Vec<Decimal>>,
    pub min_close_above_sma_pct_candidates: Option<Vec<Decimal>>,
    pub max_close_above_sma_pct_candidates: Option<Vec<Decimal>>,
    pub min_momentum_return_pct_candidates: Option<Vec<Decimal>>,
    pub min_trend_return_pct_candidates: Option<Vec<Decimal>>,
    pub min_trend_slope_pct_candidates: Option<Vec<Decimal>>,
    pub min_pullback_depth_pct_candidates: Option<Vec<Decimal>>,
    pub max_pullback_depth_pct_candidates: Option<Vec<Decimal>>,
    pub min_reclaim_pct_candidates: Option<Vec<Decimal>>,
    pub min_breakdown_pct_candidates: Option<Vec<Decimal>>,
    pub min_reclaim_close_pct_candidates: Option<Vec<Decimal>>,
    pub min_lower_wick_pct_candidates: Option<Vec<Decimal>>,
    pub min_volume_ratio_candidates: Option<Vec<Decimal>>,
    pub max_choppiness_candidates: Option<Vec<Decimal>>,
    pub holding_candles_candidates: Option<Vec<u32>>,
    pub correlation_id: Option<Uuid>,
}

impl ResearchCampaignRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.strategies.is_empty() || self.strategies.iter().any(|value| value.trim().is_empty())
        {
            return Err(CoreError::EmptyResearchCampaignStrategies);
        }
        if self.symbols.is_empty() || self.symbols.iter().any(|value| value.trim().is_empty()) {
            return Err(CoreError::EmptyResearchCampaignSymbols);
        }
        if self.experiment_timeframes.is_empty()
            || self
                .experiment_timeframes
                .iter()
                .any(|value| value.trim().is_empty())
        {
            return Err(CoreError::EmptyResearchCampaignTimeframes);
        }
        self.base_interval.parse::<CandleInterval>()?;
        for timeframe in &self.experiment_timeframes {
            timeframe.parse::<CandleInterval>()?;
        }
        if self.initial_capital <= Decimal::ZERO {
            return Err(CoreError::InvalidStrategyExperimentInitialCapital);
        }
        if self.fee_bps < Decimal::ZERO {
            return Err(CoreError::InvalidBacktestBps("fee_bps".to_string()));
        }
        if self.slippage_bps < Decimal::ZERO {
            return Err(CoreError::InvalidBacktestBps("slippage_bps".to_string()));
        }
        if self.lookback_candidates.is_empty() {
            return Err(CoreError::EmptyStrategyExperimentCandidates);
        }
        if self.walk_forward_top_n == 0 || self.max_candidates_per_batch == 0 {
            return Err(CoreError::InvalidStrategyExperimentMaxRuns);
        }
        if self.regime_dataset_id.is_none() {
            let windows = campaign_windows(self)?;
            if windows.is_empty() {
                return Err(CoreError::EmptyResearchCampaignWindows);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCampaignBatchPlan {
    pub plan_index: i32,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    #[serde(default)]
    pub regime_label: Option<ResearchRegimeLabel>,
}

impl ResearchCampaignBatchPlan {
    pub fn to_batch_request(&self, campaign: &ResearchCampaignRequest) -> ResearchBatchRequest {
        ResearchBatchRequest {
            strategy_id: self.strategy_id.clone(),
            symbol: self.symbol.clone(),
            base_interval: campaign.base_interval.clone(),
            target_intervals: vec![self.timeframe.clone()],
            start_time: self.start_time,
            end_time: self.end_time,
            initial_capital: campaign.initial_capital,
            fee_bps: campaign.fee_bps,
            slippage_bps: campaign.slippage_bps,
            experiment_timeframes: vec![self.timeframe.clone()],
            lookback_candidates: campaign.lookback_candidates.clone(),
            trend_lookback_candidates: campaign.trend_lookback_candidates.clone(),
            momentum_lookback_candidates: campaign.momentum_lookback_candidates.clone(),
            compression_lookback_candidates: campaign.compression_lookback_candidates.clone(),
            breakout_lookback_candidates: campaign.breakout_lookback_candidates.clone(),
            pullback_lookback_candidates: campaign.pullback_lookback_candidates.clone(),
            pullback_sma_lookback_candidates: campaign.pullback_sma_lookback_candidates.clone(),
            compression_percentile_threshold_candidates: campaign
                .compression_percentile_threshold_candidates
                .clone(),
            min_breakout_pct_candidates: campaign.min_breakout_pct_candidates.clone(),
            max_breakout_extension_pct_candidates: campaign
                .max_breakout_extension_pct_candidates
                .clone(),
            min_volume_expansion_ratio_candidates: campaign
                .min_volume_expansion_ratio_candidates
                .clone(),
            lower_band_pct_candidates: campaign.lower_band_pct_candidates.clone(),
            upper_band_pct_candidates: campaign.upper_band_pct_candidates.clone(),
            min_range_width_pct_candidates: campaign.min_range_width_pct_candidates.clone(),
            max_range_width_pct_candidates: campaign.max_range_width_pct_candidates.clone(),
            min_close_above_sma_pct_candidates: campaign.min_close_above_sma_pct_candidates.clone(),
            max_close_above_sma_pct_candidates: campaign.max_close_above_sma_pct_candidates.clone(),
            min_momentum_return_pct_candidates: campaign.min_momentum_return_pct_candidates.clone(),
            min_trend_return_pct_candidates: campaign.min_trend_return_pct_candidates.clone(),
            min_trend_slope_pct_candidates: campaign.min_trend_slope_pct_candidates.clone(),
            min_pullback_depth_pct_candidates: campaign.min_pullback_depth_pct_candidates.clone(),
            max_pullback_depth_pct_candidates: campaign.max_pullback_depth_pct_candidates.clone(),
            min_reclaim_pct_candidates: campaign.min_reclaim_pct_candidates.clone(),
            min_breakdown_pct_candidates: campaign.min_breakdown_pct_candidates.clone(),
            min_reclaim_close_pct_candidates: campaign.min_reclaim_close_pct_candidates.clone(),
            min_lower_wick_pct_candidates: campaign.min_lower_wick_pct_candidates.clone(),
            min_volume_ratio_candidates: campaign.min_volume_ratio_candidates.clone(),
            max_choppiness_candidates: campaign.max_choppiness_candidates.clone(),
            holding_candles_candidates: campaign.holding_candles_candidates.clone(),
            walk_forward_top_n: campaign.walk_forward_top_n,
            repair_degraded_data: campaign.repair_degraded_data,
            create_candidates: campaign.create_candidates,
            candidate_creation_mode: campaign.candidate_creation_mode,
            max_candidates: campaign.max_candidates_per_batch,
            correlation_id: campaign.correlation_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCampaignBatchResult {
    pub plan: ResearchCampaignBatchPlan,
    pub research_batch_id: Option<Uuid>,
    pub batch_status: Option<ResearchBatchStatus>,
    pub triage_status: ResearchBatchTriageStatus,
    pub candidates_created: i32,
    #[serde(default)]
    pub total_candidate_configs: i32,
    #[serde(default)]
    pub skipped_invalid_config_count: i32,
    #[serde(default)]
    pub executed_config_count: i32,
    #[serde(default)]
    pub invalid_config_examples: Vec<InvalidStrategyConfigExample>,
    #[serde(default)]
    pub candidates_blocked_by_gate: i32,
    #[serde(default)]
    pub proposals_created: i32,
    #[serde(default)]
    pub gate_decisions: Vec<ResearchCandidateCreationDecision>,
    pub top_candidates: Vec<ResearchBatchCandidateSummary>,
    pub error: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchCampaignFinding {
    pub severity: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchCampaignRecommendation {
    pub priority: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCampaignSummary {
    pub total_batches_planned: i32,
    pub total_batches_completed: i32,
    pub total_batches_failed: i32,
    pub actionable_batches: i32,
    pub overfit_only_batches: i32,
    pub weak_batches: i32,
    pub data_quality_blocked_batches: i32,
    pub no_candidate_batches: i32,
    pub candidates_created: i32,
    #[serde(default)]
    pub total_candidate_configs: i32,
    #[serde(default)]
    pub skipped_invalid_config_count: i32,
    #[serde(default)]
    pub executed_config_count: i32,
    #[serde(default)]
    pub invalid_config_examples: Vec<InvalidStrategyConfigExample>,
    #[serde(default)]
    pub candidates_blocked_by_gate: i32,
    #[serde(default)]
    pub proposals_created: i32,
    pub top_candidates: Vec<ResearchBatchCandidateSummary>,
    pub best_strategy_symbol_timeframe: Option<String>,
    #[serde(default)]
    pub per_regime_performance: Vec<ResearchCampaignRegimePerformance>,
    pub findings: Vec<ResearchCampaignFinding>,
    pub recommendations: Vec<ResearchCampaignRecommendation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCampaignRegimePerformance {
    pub regime_label: ResearchRegimeLabel,
    pub planned_batches: i32,
    pub completed_batches: i32,
    pub failed_batches: i32,
    pub actionable_batches: i32,
    pub weak_batches: i32,
    pub candidates_created: i32,
    #[serde(default)]
    pub total_candidate_configs: i32,
    #[serde(default)]
    pub skipped_invalid_config_count: i32,
    #[serde(default)]
    pub executed_config_count: i32,
    #[serde(default)]
    pub candidates_blocked_by_gate: i32,
    #[serde(default)]
    pub proposals_created: i32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchRegimeStrategyStatus {
    Robust,
    Promising,
    Weak,
    Negative,
    Overfit,
    InsufficientData,
    DataQualityBlocked,
}

impl ResearchRegimeStrategyStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Robust => "ROBUST",
            Self::Promising => "PROMISING",
            Self::Weak => "WEAK",
            Self::Negative => "NEGATIVE",
            Self::Overfit => "OVERFIT",
            Self::InsufficientData => "INSUFFICIENT_DATA",
            Self::DataQualityBlocked => "DATA_QUALITY_BLOCKED",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeStrategyRanking {
    pub rank: i32,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub status: ResearchRegimeStrategyStatus,
    pub candidate_count: i32,
    pub batch_count: i32,
    pub avg_pnl_pct: Decimal,
    pub median_pnl_pct: Decimal,
    pub best_pnl_pct: Decimal,
    pub worst_pnl_pct: Decimal,
    pub profitable_candidate_ratio: Decimal,
    pub overfit_count: i32,
    pub weak_count: i32,
    pub actionable_count: i32,
    pub avg_walk_forward_score: Option<Decimal>,
    pub avg_trade_count: Decimal,
    pub avg_fee_drag_pct: Option<Decimal>,
    pub data_quality_warning_count: i32,
    pub robustness_score: i32,
    pub ranking_score: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeStrategyCell {
    pub regime_label: ResearchRegimeLabel,
    pub rankings: Vec<ResearchRegimeStrategyRanking>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchRegimeStrategyFinding {
    pub severity: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchRegimeStrategyRecommendation {
    pub priority: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeStrategySelection {
    pub regime_label: ResearchRegimeLabel,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub status: ResearchRegimeStrategyStatus,
    #[serde(default)]
    pub is_promising: bool,
    #[serde(default)]
    pub is_least_bad: bool,
    #[serde(default)]
    pub score: i32,
    #[serde(default)]
    pub reason: String,
    pub robustness_score: i32,
    pub median_pnl_pct: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeSymbolTimeframeSelection {
    pub regime_label: ResearchRegimeLabel,
    pub symbol: String,
    pub timeframe: String,
    pub strategy_id: String,
    pub status: ResearchRegimeStrategyStatus,
    pub robustness_score: i32,
    pub median_pnl_pct: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeStrategyLeaderboard {
    pub campaign_id: Uuid,
    pub generated_at: DateTime<Utc>,
    pub per_regime: Vec<ResearchRegimeStrategyCell>,
    pub overall_rankings: Vec<ResearchRegimeStrategyRanking>,
    #[serde(default)]
    pub overall_best: Option<ResearchRegimeStrategyRanking>,
    #[serde(default)]
    pub overall_promising: Option<ResearchRegimeStrategyRanking>,
    #[serde(default)]
    pub overall_least_bad: Option<ResearchRegimeStrategyRanking>,
    pub best_strategy_by_regime: Vec<ResearchRegimeStrategySelection>,
    pub worst_strategy_by_regime: Vec<ResearchRegimeStrategySelection>,
    pub best_symbol_timeframe_by_regime: Vec<ResearchRegimeSymbolTimeframeSelection>,
    pub findings: Vec<ResearchRegimeStrategyFinding>,
    pub recommendations: Vec<ResearchRegimeStrategyRecommendation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCampaignResult {
    pub campaign_id: Uuid,
    pub status: ResearchCampaignStatus,
    pub request: ResearchCampaignRequest,
    pub batches: Vec<ResearchCampaignBatchResult>,
    pub summary: ResearchCampaignSummary,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchRegimeLabel {
    TrendUp,
    TrendDown,
    Range,
    HighVolatility,
    LowVolatility,
    Mixed,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchRegimeDatasetStatus {
    Completed,
    Partial,
    Failed,
}

impl ResearchRegimeDatasetStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "COMPLETED",
            Self::Partial => "PARTIAL",
            Self::Failed => "FAILED",
        }
    }
}

impl std::str::FromStr for ResearchRegimeDatasetStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "COMPLETED" => Ok(Self::Completed),
            "PARTIAL" => Ok(Self::Partial),
            "FAILED" => Ok(Self::Failed),
            other => Err(CoreError::UnsupportedResearchRegimeDatasetStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchRegimeDiscoveryStatus {
    Completed,
    Partial,
    InsufficientData,
    Failed,
}

impl ResearchRegimeDiscoveryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "COMPLETED",
            Self::Partial => "PARTIAL",
            Self::InsufficientData => "INSUFFICIENT_DATA",
            Self::Failed => "FAILED",
        }
    }
}

impl std::str::FromStr for ResearchRegimeDiscoveryStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "COMPLETED" => Ok(Self::Completed),
            "PARTIAL" => Ok(Self::Partial),
            "INSUFFICIENT_DATA" => Ok(Self::InsufficientData),
            "FAILED" => Ok(Self::Failed),
            other => Err(CoreError::UnsupportedResearchRegimeDiscoveryStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchRegimeCalibrationStatus {
    Completed,
    Partial,
    InsufficientData,
    Failed,
}

impl ResearchRegimeCalibrationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "COMPLETED",
            Self::Partial => "PARTIAL",
            Self::InsufficientData => "INSUFFICIENT_DATA",
            Self::Failed => "FAILED",
        }
    }
}

impl std::str::FromStr for ResearchRegimeCalibrationStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "COMPLETED" => Ok(Self::Completed),
            "PARTIAL" => Ok(Self::Partial),
            "INSUFFICIENT_DATA" => Ok(Self::InsufficientData),
            "FAILED" => Ok(Self::Failed),
            other => Err(CoreError::UnsupportedResearchRegimeCalibrationStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeClassifierConfig {
    pub trend_return_threshold_pct: Decimal,
    pub trend_slope_threshold: Decimal,
    pub range_return_max_pct: Decimal,
    pub range_choppiness_min: Decimal,
    pub high_volatility_threshold_pct: Decimal,
    pub low_volatility_threshold_pct: Decimal,
    pub min_confidence: Decimal,
    #[serde(default = "default_research_regime_priority_order")]
    pub priority_order: Vec<ResearchRegimeLabel>,
}

impl Default for ResearchRegimeClassifierConfig {
    fn default() -> Self {
        Self {
            trend_return_threshold_pct: Decimal::new(3, 0),
            trend_slope_threshold: Decimal::ZERO,
            range_return_max_pct: Decimal::ONE,
            range_choppiness_min: Decimal::new(65, 0),
            high_volatility_threshold_pct: Decimal::new(8, 0),
            low_volatility_threshold_pct: Decimal::new(15, 1),
            min_confidence: Decimal::ZERO,
            priority_order: default_research_regime_priority_order(),
        }
    }
}

impl ResearchRegimeClassifierConfig {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.trend_return_threshold_pct < Decimal::ZERO
            || self.range_return_max_pct < Decimal::ZERO
            || self.range_choppiness_min < Decimal::ZERO
            || self.high_volatility_threshold_pct < Decimal::ZERO
            || self.low_volatility_threshold_pct < Decimal::ZERO
            || self.min_confidence < Decimal::ZERO
        {
            return Err(CoreError::InvalidResearchRegimeClassifierConfig(
                "thresholds must be non-negative".to_string(),
            ));
        }
        if self.priority_order.is_empty() {
            return Err(CoreError::InvalidResearchRegimeClassifierConfig(
                "priority_order cannot be empty".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeDatasetRequest {
    pub symbol: String,
    pub timeframe: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub window_hours: i64,
    pub step_hours: i64,
    pub min_candles_per_window: i32,
    #[serde(default)]
    pub target_regimes: Option<Vec<ResearchRegimeLabel>>,
    #[serde(default)]
    pub max_windows_per_regime: Option<u32>,
    #[serde(default = "default_research_regime_dataset_require_good_data_quality")]
    pub require_good_data_quality: bool,
    #[serde(default)]
    pub classifier_config: Option<ResearchRegimeClassifierConfig>,
}

impl ResearchRegimeDatasetRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.symbol.trim().is_empty() {
            return Err(CoreError::EmptySymbol);
        }
        self.timeframe.parse::<CandleInterval>()?;
        if self.end_time <= self.start_time {
            return Err(CoreError::InvalidResearchRegimeDatasetTimeRange);
        }
        if self.window_hours <= 0 || self.step_hours <= 0 {
            return Err(CoreError::InvalidResearchRegimeDatasetWindowStep);
        }
        if self.min_candles_per_window <= 0 {
            return Err(CoreError::InvalidResearchRegimeDatasetMinCandles);
        }
        if let Some(config) = &self.classifier_config {
            config.validate()?;
        }
        Ok(())
    }

    pub fn target_regime_set(&self) -> Vec<ResearchRegimeLabel> {
        self.target_regimes.clone().unwrap_or_else(|| {
            vec![
                ResearchRegimeLabel::TrendUp,
                ResearchRegimeLabel::TrendDown,
                ResearchRegimeLabel::Range,
                ResearchRegimeLabel::HighVolatility,
                ResearchRegimeLabel::LowVolatility,
            ]
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeDiscoveryRequest {
    pub symbol: String,
    pub timeframe: String,
    pub scan_start: DateTime<Utc>,
    pub scan_end: DateTime<Utc>,
    pub window_hours: i64,
    pub step_hours: i64,
    #[serde(default)]
    pub target_regimes: Option<Vec<ResearchRegimeLabel>>,
    #[serde(default = "default_research_regime_discovery_max_windows_per_regime")]
    pub max_windows_per_regime: u32,
    #[serde(default)]
    pub min_confidence: Option<Decimal>,
    #[serde(default = "default_true")]
    pub require_existing_candles: bool,
    #[serde(default)]
    pub auto_backfill_missing: bool,
    #[serde(default)]
    pub classifier_config: Option<ResearchRegimeClassifierConfig>,
    #[serde(default)]
    pub calibration_id: Option<Uuid>,
}

impl ResearchRegimeDiscoveryRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.symbol.trim().is_empty() {
            return Err(CoreError::EmptySymbol);
        }
        self.timeframe.parse::<CandleInterval>()?;
        if self.scan_end <= self.scan_start {
            return Err(CoreError::InvalidResearchRegimeDatasetTimeRange);
        }
        if self.window_hours <= 0 || self.step_hours <= 0 {
            return Err(CoreError::InvalidResearchRegimeDatasetWindowStep);
        }
        if let Some(config) = &self.classifier_config {
            config.validate()?;
        }
        Ok(())
    }

    pub fn target_regime_set(&self) -> Vec<ResearchRegimeLabel> {
        self.target_regimes.clone().unwrap_or_else(|| {
            vec![
                ResearchRegimeLabel::TrendUp,
                ResearchRegimeLabel::TrendDown,
                ResearchRegimeLabel::Range,
                ResearchRegimeLabel::HighVolatility,
                ResearchRegimeLabel::LowVolatility,
            ]
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeWindowMetric {
    pub name: String,
    pub value: Decimal,
    pub threshold: Option<Decimal>,
    pub passed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeClassificationCondition {
    pub label: ResearchRegimeLabel,
    pub metric: String,
    pub operator: String,
    pub value: Decimal,
    pub threshold: Decimal,
    pub passed: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeClassificationExplanation {
    pub return_pct: Decimal,
    pub realized_volatility: Decimal,
    pub avg_range_pct: Decimal,
    pub trend_slope: Decimal,
    pub choppiness_proxy: Decimal,
    pub thresholds_used: ResearchRegimeClassifierConfig,
    pub conditions: Vec<ResearchRegimeClassificationCondition>,
    pub final_label: ResearchRegimeLabel,
    pub confidence: Decimal,
    pub alternate_labels_considered: Vec<ResearchRegimeLabel>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeWindow {
    pub id: Uuid,
    pub symbol: String,
    pub timeframe: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub regime_label: ResearchRegimeLabel,
    pub return_pct: Decimal,
    pub realized_volatility: Decimal,
    pub avg_range_pct: Decimal,
    pub trend_slope: Decimal,
    pub choppiness_proxy: Decimal,
    pub data_quality_status: MarketDataQualityStatus,
    pub candle_count: i32,
    pub score: Decimal,
    pub confidence: Decimal,
    pub metrics: Vec<ResearchRegimeWindowMetric>,
    pub explanation: ResearchRegimeClassificationExplanation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchRegimeDatasetRecommendation {
    pub priority: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeDatasetSummary {
    pub total_candidate_windows: i32,
    pub selected_windows: i32,
    pub data_quality_blocked_windows: i32,
    pub insufficient_candle_windows: i32,
    pub regime_counts: BTreeMap<ResearchRegimeLabel, i32>,
    pub missing_regimes: Vec<ResearchRegimeLabel>,
    pub recommendations: Vec<ResearchRegimeDatasetRecommendation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeDatasetResult {
    pub dataset_id: Uuid,
    pub status: ResearchRegimeDatasetStatus,
    pub request: ResearchRegimeDatasetRequest,
    pub summary: ResearchRegimeDatasetSummary,
    pub windows: Vec<ResearchRegimeWindow>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeDiscoveryCandidateWindow {
    pub id: Uuid,
    pub regime_label: ResearchRegimeLabel,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub confidence: Decimal,
    pub return_pct: Decimal,
    pub realized_volatility: Decimal,
    pub avg_range_pct: Decimal,
    pub trend_slope: Decimal,
    pub choppiness_proxy: Decimal,
    pub data_quality_status: MarketDataQualityStatus,
    pub candle_count: i32,
    pub explanation: ResearchRegimeClassificationExplanation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchRegimeDiscoveryRecommendation {
    pub priority: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeDiscoverySummary {
    pub total_windows_scanned: i32,
    pub selected_window_count: i32,
    pub counts_by_regime: BTreeMap<ResearchRegimeLabel, i32>,
    pub missing_regimes: Vec<ResearchRegimeLabel>,
    pub data_quality_blocked_count: i32,
    pub insufficient_data_count: i32,
    pub recommendations: Vec<ResearchRegimeDiscoveryRecommendation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeDiscoveryResult {
    pub discovery_id: Uuid,
    pub status: ResearchRegimeDiscoveryStatus,
    pub symbol: String,
    pub timeframe: String,
    pub scan_start: DateTime<Utc>,
    pub scan_end: DateTime<Utc>,
    pub total_windows_scanned: i32,
    pub selected_windows: Vec<ResearchRegimeDiscoveryCandidateWindow>,
    pub counts_by_regime: BTreeMap<ResearchRegimeLabel, i32>,
    pub missing_regimes: Vec<ResearchRegimeLabel>,
    pub data_quality_blocked_count: i32,
    pub recommendations: Vec<ResearchRegimeDiscoveryRecommendation>,
    pub request: ResearchRegimeDiscoveryRequest,
    pub summary: ResearchRegimeDiscoverySummary,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeDatasetFromDiscoveryRequest {
    pub discovery_id: Uuid,
    #[serde(default)]
    pub target_regimes: Option<Vec<ResearchRegimeLabel>>,
    #[serde(default)]
    pub max_windows_per_regime: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeThresholdCandidate {
    pub candidate_id: String,
    pub classifier_config: ResearchRegimeClassifierConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchRegimeCalibrationRecommendation {
    pub priority: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeCalibrationRequest {
    pub symbol: String,
    pub timeframe: String,
    pub scan_start: DateTime<Utc>,
    pub scan_end: DateTime<Utc>,
    pub window_hours: i64,
    pub step_hours: i64,
    #[serde(default)]
    pub threshold_candidates: Option<Vec<ResearchRegimeThresholdCandidate>>,
    #[serde(default = "default_research_regime_calibration_target_min_windows_per_regime")]
    pub target_min_windows_per_regime: u32,
}

impl ResearchRegimeCalibrationRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.symbol.trim().is_empty() {
            return Err(CoreError::EmptySymbol);
        }
        self.timeframe.parse::<CandleInterval>()?;
        if self.scan_end <= self.scan_start {
            return Err(CoreError::InvalidResearchRegimeDatasetTimeRange);
        }
        if self.window_hours <= 0 || self.step_hours <= 0 {
            return Err(CoreError::InvalidResearchRegimeDatasetWindowStep);
        }
        if let Some(candidates) = &self.threshold_candidates {
            if candidates.is_empty() {
                return Err(CoreError::InvalidResearchRegimeClassifierConfig(
                    "threshold_candidates cannot be empty when provided".to_string(),
                ));
            }
            for candidate in candidates {
                candidate.classifier_config.validate()?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeCalibrationCandidateResult {
    pub candidate_id: String,
    pub classifier_config: ResearchRegimeClassifierConfig,
    pub counts_by_regime: BTreeMap<ResearchRegimeLabel, i32>,
    pub missing_regimes: Vec<ResearchRegimeLabel>,
    pub total_windows_scanned: i32,
    pub data_quality_good_windows: i32,
    pub avg_confidence: Decimal,
    pub diversity_score: Decimal,
    pub balance_score: Decimal,
    pub dominant_regime_share: Decimal,
    pub total_score: Decimal,
    pub warnings: Vec<String>,
    pub explanation_samples: Vec<ResearchRegimeClassificationExplanation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeCalibrationResult {
    pub calibration_id: Uuid,
    pub status: ResearchRegimeCalibrationStatus,
    pub request: ResearchRegimeCalibrationRequest,
    pub candidates: Vec<ResearchRegimeCalibrationCandidateResult>,
    pub recommended_config: Option<ResearchRegimeClassifierConfig>,
    pub recommended_candidate_id: Option<String>,
    pub missing_regimes: Vec<ResearchRegimeLabel>,
    pub recommendations: Vec<ResearchRegimeCalibrationRecommendation>,
    pub created_at: DateTime<Utc>,
}

impl std::str::FromStr for ResearchRegimeLabel {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "TREND_UP" => Ok(Self::TrendUp),
            "TREND_DOWN" => Ok(Self::TrendDown),
            "RANGE" => Ok(Self::Range),
            "HIGH_VOLATILITY" => Ok(Self::HighVolatility),
            "LOW_VOLATILITY" => Ok(Self::LowVolatility),
            "MIXED" => Ok(Self::Mixed),
            "UNKNOWN" => Ok(Self::Unknown),
            other => Err(CoreError::UnsupportedResearchRegimeLabel(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyRobustnessMatrixStatus {
    Robust,
    PromisingButWeak,
    Mixed,
    OverfitRisk,
    Negative,
    InsufficientData,
    Failed,
}

impl StrategyRobustnessMatrixStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Robust => "ROBUST",
            Self::PromisingButWeak => "PROMISING_BUT_WEAK",
            Self::Mixed => "MIXED",
            Self::OverfitRisk => "OVERFIT_RISK",
            Self::Negative => "NEGATIVE",
            Self::InsufficientData => "INSUFFICIENT_DATA",
            Self::Failed => "FAILED",
        }
    }
}

impl std::str::FromStr for StrategyRobustnessMatrixStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "ROBUST" => Ok(Self::Robust),
            "PROMISING_BUT_WEAK" => Ok(Self::PromisingButWeak),
            "MIXED" => Ok(Self::Mixed),
            "OVERFIT_RISK" => Ok(Self::OverfitRisk),
            "NEGATIVE" => Ok(Self::Negative),
            "INSUFFICIENT_DATA" => Ok(Self::InsufficientData),
            "FAILED" => Ok(Self::Failed),
            other => Err(CoreError::UnsupportedStrategyRobustnessMatrixStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StrategyRobustnessMatrixFinding {
    pub severity: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StrategyRobustnessMatrixRecommendation {
    pub priority: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyRobustnessMatrixWindow {
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyRobustnessMatrixRequest {
    pub strategy_ids: Vec<String>,
    pub symbols: Vec<String>,
    pub timeframes: Vec<String>,
    #[serde(default)]
    pub windows: Vec<StrategyRobustnessMatrixWindow>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub window_hours: Option<i64>,
    pub step_hours: Option<i64>,
    #[serde(default)]
    pub config_json_by_strategy: Option<BTreeMap<String, Value>>,
    pub experiment_run_id: Option<Uuid>,
    pub initial_capital: Decimal,
    pub fee_bps: Decimal,
    pub slippage_bps: Decimal,
    pub holding_candles: Option<u32>,
    #[serde(default = "default_strategy_robustness_min_trades_per_cell")]
    pub min_trades_per_cell: i32,
    #[serde(default = "default_strategy_robustness_min_profitable_window_ratio")]
    pub min_profitable_window_ratio: Decimal,
}

impl StrategyRobustnessMatrixRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self
            .strategy_ids
            .iter()
            .any(|value| value.trim().is_empty())
            || self.strategy_ids.is_empty()
        {
            return Err(CoreError::EmptyStrategyRobustnessMatrixStrategies);
        }
        if self.symbols.iter().any(|value| value.trim().is_empty()) || self.symbols.is_empty() {
            return Err(CoreError::EmptyStrategyRobustnessMatrixSymbols);
        }
        if self.timeframes.iter().any(|value| value.trim().is_empty()) || self.timeframes.is_empty()
        {
            return Err(CoreError::EmptyStrategyRobustnessMatrixTimeframes);
        }
        for timeframe in &self.timeframes {
            timeframe.parse::<CandleInterval>()?;
        }
        if self.initial_capital <= Decimal::ZERO {
            return Err(CoreError::InvalidStrategyRobustnessMatrixInitialCapital);
        }
        if self.fee_bps < Decimal::ZERO {
            return Err(CoreError::InvalidBacktestBps("fee_bps".to_string()));
        }
        if self.slippage_bps < Decimal::ZERO {
            return Err(CoreError::InvalidBacktestBps("slippage_bps".to_string()));
        }
        if self.min_trades_per_cell < 0 {
            return Err(CoreError::InvalidStrategyRobustnessMatrixThreshold(
                "min_trades_per_cell".to_string(),
            ));
        }
        if self.min_profitable_window_ratio < Decimal::ZERO
            || self.min_profitable_window_ratio > Decimal::ONE
        {
            return Err(CoreError::InvalidStrategyRobustnessMatrixThreshold(
                "min_profitable_window_ratio".to_string(),
            ));
        }
        if self.windows.is_empty() {
            let Some(start_time) = self.start_time else {
                return Err(CoreError::EmptyStrategyRobustnessMatrixWindows);
            };
            let Some(end_time) = self.end_time else {
                return Err(CoreError::EmptyStrategyRobustnessMatrixWindows);
            };
            if end_time <= start_time {
                return Err(CoreError::InvalidStrategyRobustnessMatrixTimeRange);
            }
            if self.window_hours.unwrap_or(0) <= 0 || self.step_hours.unwrap_or(0) <= 0 {
                return Err(CoreError::InvalidStrategyRobustnessMatrixWindowStep);
            }
        } else {
            for window in &self.windows {
                if window.end_time <= window.start_time {
                    return Err(CoreError::InvalidStrategyRobustnessMatrixTimeRange);
                }
            }
        }
        if self.holding_candles.is_some_and(|value| value == 0) {
            return Err(CoreError::InvalidHoldingCandles);
        }
        Ok(())
    }

    pub fn resolved_windows(&self) -> Result<Vec<StrategyRobustnessMatrixWindow>, CoreError> {
        self.validate()?;
        if !self.windows.is_empty() {
            return Ok(self.windows.clone());
        }
        let mut windows = Vec::new();
        let mut cursor = self.start_time.expect("validated start_time");
        let end_time = self.end_time.expect("validated end_time");
        let window_size = Duration::hours(self.window_hours.expect("validated window_hours"));
        let step_size = Duration::hours(self.step_hours.expect("validated step_hours"));
        while cursor + window_size <= end_time {
            windows.push(StrategyRobustnessMatrixWindow {
                start_time: cursor,
                end_time: cursor + window_size,
            });
            cursor += step_size;
        }
        if windows.is_empty() {
            return Err(CoreError::EmptyStrategyRobustnessMatrixWindows);
        }
        Ok(windows)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyRobustnessMatrixCell {
    pub id: Uuid,
    pub matrix_run_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub regime_label: ResearchRegimeLabel,
    pub data_quality_status: MarketDataQualityStatus,
    pub status: StrategyRobustnessMatrixStatus,
    pub pnl_pct: Decimal,
    pub trade_count: i32,
    pub raw_signal_count: i32,
    pub executed_trade_count: i32,
    pub cooldown_suppressed_count: i32,
    pub win_rate: Decimal,
    pub max_drawdown_pct: Decimal,
    pub fee_drag: Decimal,
    pub findings: Vec<StrategyRobustnessMatrixFinding>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyRobustnessMatrixStrategySummary {
    pub strategy_id: String,
    pub status: StrategyRobustnessMatrixStatus,
    pub profitable_window_ratio: Decimal,
    pub avg_pnl_pct: Decimal,
    pub median_pnl_pct: Decimal,
    pub worst_window_pnl_pct: Decimal,
    pub best_window_pnl_pct: Decimal,
    pub avg_trade_count: Decimal,
    pub regime_consistency: Decimal,
    pub data_quality_penalty: Decimal,
    pub robustness_score: Decimal,
    pub completed_cells: i32,
    pub insufficient_data_cells: i32,
    pub failed_cells: i32,
    pub best_symbol: Option<String>,
    pub worst_symbol: Option<String>,
    pub best_regime: Option<ResearchRegimeLabel>,
    pub worst_regime: Option<ResearchRegimeLabel>,
    pub findings: Vec<StrategyRobustnessMatrixFinding>,
    pub recommendations: Vec<StrategyRobustnessMatrixRecommendation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyRobustnessMatrixResult {
    pub run_id: Uuid,
    pub status: StrategyRobustnessMatrixStatus,
    pub request: StrategyRobustnessMatrixRequest,
    pub strategy_rankings: Vec<StrategyRobustnessMatrixStrategySummary>,
    pub findings: Vec<StrategyRobustnessMatrixFinding>,
    pub recommendations: Vec<StrategyRobustnessMatrixRecommendation>,
    pub cell_count: i32,
    pub created_at: DateTime<Utc>,
}

pub fn build_strategy_robustness_matrix_result(
    run_id: Uuid,
    request: StrategyRobustnessMatrixRequest,
    cells: Vec<StrategyRobustnessMatrixCell>,
    created_at: DateTime<Utc>,
) -> StrategyRobustnessMatrixResult {
    let mut by_strategy = BTreeMap::<String, Vec<StrategyRobustnessMatrixCell>>::new();
    for cell in cells.iter().cloned() {
        by_strategy
            .entry(cell.strategy_id.clone())
            .or_default()
            .push(cell);
    }

    let mut strategy_rankings = by_strategy
        .into_iter()
        .map(|(strategy_id, strategy_cells)| {
            summarize_strategy_robustness_matrix_strategy(&strategy_id, &request, &strategy_cells)
        })
        .collect::<Vec<_>>();
    strategy_rankings.sort_by(|left, right| {
        right
            .robustness_score
            .cmp(&left.robustness_score)
            .then_with(|| left.strategy_id.cmp(&right.strategy_id))
    });

    let robust_count = strategy_rankings
        .iter()
        .filter(|summary| summary.status == StrategyRobustnessMatrixStatus::Robust)
        .count();
    let status = if strategy_rankings.is_empty() {
        StrategyRobustnessMatrixStatus::InsufficientData
    } else if robust_count > 0 {
        StrategyRobustnessMatrixStatus::Robust
    } else if strategy_rankings
        .iter()
        .any(|summary| summary.status == StrategyRobustnessMatrixStatus::PromisingButWeak)
    {
        StrategyRobustnessMatrixStatus::PromisingButWeak
    } else if strategy_rankings
        .iter()
        .any(|summary| summary.status == StrategyRobustnessMatrixStatus::Mixed)
    {
        StrategyRobustnessMatrixStatus::Mixed
    } else if strategy_rankings
        .iter()
        .all(|summary| summary.status == StrategyRobustnessMatrixStatus::Failed)
    {
        StrategyRobustnessMatrixStatus::Failed
    } else {
        StrategyRobustnessMatrixStatus::Negative
    };

    let mut findings = Vec::new();
    let mut recommendations = Vec::new();
    if robust_count == 0 {
        findings.push(StrategyRobustnessMatrixFinding {
            severity: "MEDIUM".to_string(),
            code: "no_robust_strategy_found".to_string(),
            message: "No strategy met robustness thresholds across the matrix.".to_string(),
        });
        recommendations.push(StrategyRobustnessMatrixRecommendation {
            priority: "MEDIUM".to_string(),
            code: "continue_research".to_string(),
            message: "Treat the matrix as research evidence only and avoid candidate promotion."
                .to_string(),
        });
    } else {
        findings.push(StrategyRobustnessMatrixFinding {
            severity: "LOW".to_string(),
            code: "promising_strategy_found".to_string(),
            message: "Strategy robustness matrix found at least one robust or promising strategy."
                .to_string(),
        });
    }

    if strategy_rankings
        .iter()
        .any(|summary| summary.data_quality_penalty >= Decimal::new(25, 0))
    {
        findings.push(StrategyRobustnessMatrixFinding {
            severity: "HIGH".to_string(),
            code: "bad_data_quality_blocks_robustness".to_string(),
            message: "One or more strategies were materially penalized by bad market data quality."
                .to_string(),
        });
    }

    StrategyRobustnessMatrixResult {
        run_id,
        status,
        request,
        strategy_rankings,
        findings,
        recommendations,
        cell_count: i32::try_from(cells.len()).unwrap_or(i32::MAX),
        created_at,
    }
}

pub fn summarize_strategy_robustness_matrix_strategy(
    strategy_id: &str,
    request: &StrategyRobustnessMatrixRequest,
    cells: &[StrategyRobustnessMatrixCell],
) -> StrategyRobustnessMatrixStrategySummary {
    let completed = cells
        .iter()
        .filter(|cell| {
            !matches!(
                cell.status,
                StrategyRobustnessMatrixStatus::Failed
                    | StrategyRobustnessMatrixStatus::InsufficientData
            )
        })
        .collect::<Vec<_>>();
    let completed_cells = i32::try_from(completed.len()).unwrap_or(i32::MAX);
    let insufficient_data_cells = cells
        .iter()
        .filter(|cell| cell.status == StrategyRobustnessMatrixStatus::InsufficientData)
        .count() as i32;
    let failed_cells = cells
        .iter()
        .filter(|cell| cell.status == StrategyRobustnessMatrixStatus::Failed)
        .count() as i32;
    let profitable_windows = completed
        .iter()
        .filter(|cell| cell.pnl_pct > Decimal::ZERO)
        .count() as i32;
    let profitable_window_ratio = if completed_cells == 0 {
        Decimal::ZERO
    } else {
        Decimal::from(profitable_windows) / Decimal::from(completed_cells)
    };
    let avg_pnl_pct = decimal_avg(completed.iter().map(|cell| cell.pnl_pct));
    let mut sorted_pnl = completed
        .iter()
        .map(|cell| cell.pnl_pct)
        .collect::<Vec<_>>();
    sorted_pnl.sort();
    let median_pnl_pct = median_decimal(&sorted_pnl);
    let worst_window_pnl_pct = completed
        .iter()
        .map(|cell| cell.pnl_pct)
        .min()
        .unwrap_or(Decimal::ZERO);
    let best_window_pnl_pct = completed
        .iter()
        .map(|cell| cell.pnl_pct)
        .max()
        .unwrap_or(Decimal::ZERO);
    let avg_trade_count = decimal_avg(completed.iter().map(|cell| Decimal::from(cell.trade_count)));
    let regime_consistency = calculate_strategy_robustness_regime_consistency(&completed);
    let data_quality_penalty = calculate_strategy_robustness_data_quality_penalty(cells);
    let concentration_pct = calculate_strategy_robustness_winner_concentration_pct(&completed);
    let robustness_score = calculate_strategy_robustness_score(
        profitable_window_ratio,
        avg_pnl_pct,
        median_pnl_pct,
        worst_window_pnl_pct,
        avg_trade_count,
        regime_consistency,
        data_quality_penalty,
        concentration_pct,
    );

    let status = classify_strategy_robustness_matrix_status(
        cells.len() as i32,
        completed_cells,
        insufficient_data_cells,
        failed_cells,
        profitable_window_ratio,
        median_pnl_pct,
        avg_pnl_pct,
        worst_window_pnl_pct,
        avg_trade_count,
        concentration_pct,
        data_quality_penalty,
        robustness_score,
        request.min_trades_per_cell,
        request.min_profitable_window_ratio,
    );

    let best_symbol = best_group_by_avg_pnl(&completed, |cell| cell.symbol.clone(), true);
    let worst_symbol = best_group_by_avg_pnl(&completed, |cell| cell.symbol.clone(), false);
    let best_regime = best_group_by_avg_pnl(&completed, |cell| cell.regime_label, true);
    let worst_regime = best_group_by_avg_pnl(&completed, |cell| cell.regime_label, false);
    let (findings, recommendations) = strategy_robustness_findings_recommendations(
        status,
        concentration_pct,
        data_quality_penalty,
        median_pnl_pct,
        avg_trade_count,
        request.min_trades_per_cell,
    );

    StrategyRobustnessMatrixStrategySummary {
        strategy_id: strategy_id.to_string(),
        status,
        profitable_window_ratio,
        avg_pnl_pct,
        median_pnl_pct,
        worst_window_pnl_pct,
        best_window_pnl_pct,
        avg_trade_count,
        regime_consistency,
        data_quality_penalty,
        robustness_score,
        completed_cells,
        insufficient_data_cells,
        failed_cells,
        best_symbol,
        worst_symbol,
        best_regime,
        worst_regime,
        findings,
        recommendations,
    }
}

pub fn calculate_strategy_robustness_regime_consistency(
    cells: &[&StrategyRobustnessMatrixCell],
) -> Decimal {
    let mut by_regime = BTreeMap::<ResearchRegimeLabel, Vec<&StrategyRobustnessMatrixCell>>::new();
    for cell in cells {
        if cell.regime_label != ResearchRegimeLabel::Unknown {
            by_regime.entry(cell.regime_label).or_default().push(*cell);
        }
    }
    if by_regime.is_empty() {
        return Decimal::ZERO;
    }
    let positive_regimes = by_regime
        .values()
        .filter(|items| decimal_avg(items.iter().map(|cell| cell.pnl_pct)) > Decimal::ZERO)
        .count() as i32;
    (Decimal::from(positive_regimes) / Decimal::from(by_regime.len() as i32)) * Decimal::new(100, 0)
}

pub fn calculate_strategy_robustness_data_quality_penalty(
    cells: &[StrategyRobustnessMatrixCell],
) -> Decimal {
    if cells.is_empty() {
        return Decimal::new(50, 0);
    }
    let total = cells.iter().fold(Decimal::ZERO, |sum, cell| {
        sum + match cell.data_quality_status {
            MarketDataQualityStatus::Good => Decimal::ZERO,
            MarketDataQualityStatus::Degraded => Decimal::new(10, 0),
            MarketDataQualityStatus::Bad => Decimal::new(30, 0),
            MarketDataQualityStatus::InsufficientData => Decimal::new(40, 0),
            MarketDataQualityStatus::Unknown => Decimal::new(15, 0),
        }
    });
    total / Decimal::from(cells.len() as i32)
}

pub fn calculate_strategy_robustness_winner_concentration_pct(
    cells: &[&StrategyRobustnessMatrixCell],
) -> Decimal {
    let total_positive = cells
        .iter()
        .filter(|cell| cell.pnl_pct > Decimal::ZERO)
        .fold(Decimal::ZERO, |sum, cell| sum + cell.pnl_pct);
    if total_positive <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let best = cells
        .iter()
        .map(|cell| cell.pnl_pct.max(Decimal::ZERO))
        .max()
        .unwrap_or(Decimal::ZERO);
    (best / total_positive) * Decimal::new(100, 0)
}

pub fn calculate_strategy_robustness_score(
    profitable_window_ratio: Decimal,
    avg_pnl_pct: Decimal,
    median_pnl_pct: Decimal,
    worst_window_pnl_pct: Decimal,
    avg_trade_count: Decimal,
    regime_consistency: Decimal,
    data_quality_penalty: Decimal,
    concentration_pct: Decimal,
) -> Decimal {
    let profitability_component =
        (profitable_window_ratio * Decimal::new(40, 0)).clamp(Decimal::ZERO, Decimal::new(40, 0));
    let median_component =
        (median_pnl_pct * Decimal::new(12, 0)).clamp(Decimal::new(-20, 0), Decimal::new(20, 0));
    let avg_component =
        (avg_pnl_pct * Decimal::new(8, 0)).clamp(Decimal::new(-15, 0), Decimal::new(15, 0));
    let worst_penalty = (worst_window_pnl_pct.min(Decimal::ZERO).abs() * Decimal::new(5, 0))
        .min(Decimal::new(20, 0));
    let trade_component =
        (avg_trade_count.min(Decimal::new(20, 0)) / Decimal::new(20, 0)) * Decimal::new(10, 0);
    let regime_component = (regime_consistency / Decimal::new(100, 0)) * Decimal::new(15, 0);
    let concentration_penalty = if concentration_pct > Decimal::new(55, 0) {
        ((concentration_pct - Decimal::new(55, 0)) / Decimal::new(45, 0)) * Decimal::new(25, 0)
    } else {
        Decimal::ZERO
    };
    let score = Decimal::new(20, 0)
        + profitability_component
        + median_component
        + avg_component
        + trade_component
        + regime_component
        - worst_penalty
        - data_quality_penalty
        - concentration_penalty;
    score.clamp(Decimal::ZERO, Decimal::new(100, 0)).round_dp(4)
}

#[allow(clippy::too_many_arguments)]
pub fn classify_strategy_robustness_matrix_status(
    total_cells: i32,
    completed_cells: i32,
    insufficient_data_cells: i32,
    failed_cells: i32,
    profitable_window_ratio: Decimal,
    median_pnl_pct: Decimal,
    avg_pnl_pct: Decimal,
    worst_window_pnl_pct: Decimal,
    avg_trade_count: Decimal,
    concentration_pct: Decimal,
    data_quality_penalty: Decimal,
    robustness_score: Decimal,
    min_trades_per_cell: i32,
    min_profitable_window_ratio: Decimal,
) -> StrategyRobustnessMatrixStatus {
    if total_cells == 0 || completed_cells == 0 {
        return StrategyRobustnessMatrixStatus::InsufficientData;
    }
    if failed_cells == total_cells {
        return StrategyRobustnessMatrixStatus::Failed;
    }
    if insufficient_data_cells > 0 && completed_cells < total_cells / 2 {
        return StrategyRobustnessMatrixStatus::InsufficientData;
    }
    if data_quality_penalty >= Decimal::new(30, 0) {
        return StrategyRobustnessMatrixStatus::InsufficientData;
    }
    if median_pnl_pct < Decimal::ZERO {
        return StrategyRobustnessMatrixStatus::Negative;
    }
    if concentration_pct >= Decimal::new(60, 0) && completed_cells >= 3 {
        return StrategyRobustnessMatrixStatus::OverfitRisk;
    }
    if avg_pnl_pct <= Decimal::ZERO || profitable_window_ratio < min_profitable_window_ratio {
        return StrategyRobustnessMatrixStatus::Mixed;
    }
    if avg_trade_count < Decimal::from(min_trades_per_cell) {
        return StrategyRobustnessMatrixStatus::PromisingButWeak;
    }
    if robustness_score >= Decimal::new(70, 0)
        && median_pnl_pct > Decimal::ZERO
        && worst_window_pnl_pct > -Decimal::new(2, 0)
        && completed_cells >= 4
    {
        return StrategyRobustnessMatrixStatus::Robust;
    }
    StrategyRobustnessMatrixStatus::PromisingButWeak
}

fn best_group_by_avg_pnl<T, F>(
    cells: &[&StrategyRobustnessMatrixCell],
    f: F,
    descending: bool,
) -> Option<T>
where
    T: Ord + Clone,
    F: Fn(&StrategyRobustnessMatrixCell) -> T,
{
    let mut groups = BTreeMap::<T, Vec<Decimal>>::new();
    for cell in cells {
        groups.entry(f(cell)).or_default().push(cell.pnl_pct);
    }
    let mut ranked = groups
        .into_iter()
        .map(|(key, values)| (key, decimal_avg(values)))
        .collect::<Vec<_>>();
    if descending {
        ranked.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    } else {
        ranked.sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
    }
    ranked.first().map(|(key, _)| key.clone())
}

fn strategy_robustness_findings_recommendations(
    status: StrategyRobustnessMatrixStatus,
    concentration_pct: Decimal,
    data_quality_penalty: Decimal,
    median_pnl_pct: Decimal,
    avg_trade_count: Decimal,
    min_trades_per_cell: i32,
) -> (
    Vec<StrategyRobustnessMatrixFinding>,
    Vec<StrategyRobustnessMatrixRecommendation>,
) {
    let mut findings = Vec::new();
    let mut recommendations = Vec::new();
    match status {
        StrategyRobustnessMatrixStatus::Robust => findings.push(StrategyRobustnessMatrixFinding {
            severity: "LOW".to_string(),
            code: "strategy_robust_across_matrix".to_string(),
            message: "Strategy remained positive across enough symbols, windows, and regimes."
                .to_string(),
        }),
        StrategyRobustnessMatrixStatus::Negative => {
            findings.push(StrategyRobustnessMatrixFinding {
                severity: "MEDIUM".to_string(),
                code: "negative_median_performance".to_string(),
                message: "Negative median performance prevents robustness.".to_string(),
            })
        }
        StrategyRobustnessMatrixStatus::OverfitRisk => {
            findings.push(StrategyRobustnessMatrixFinding {
                severity: "MEDIUM".to_string(),
                code: "performance_concentrated".to_string(),
                message: "Positive performance is concentrated in a small subset of cells."
                    .to_string(),
            })
        }
        StrategyRobustnessMatrixStatus::InsufficientData => {
            findings.push(StrategyRobustnessMatrixFinding {
                severity: "MEDIUM".to_string(),
                code: "insufficient_matrix_data".to_string(),
                message: "Too few usable cells or trades were available.".to_string(),
            })
        }
        _ => {}
    }
    if concentration_pct >= Decimal::new(60, 0) {
        findings.push(StrategyRobustnessMatrixFinding {
            severity: "MEDIUM".to_string(),
            code: "single_window_driver".to_string(),
            message: "Strategy performance is concentrated in one window or cell.".to_string(),
        });
    }
    if data_quality_penalty >= Decimal::new(25, 0) {
        findings.push(StrategyRobustnessMatrixFinding {
            severity: "HIGH".to_string(),
            code: "data_quality_blocks_matrix".to_string(),
            message: "Bad or degraded data quality materially blocks interpretation.".to_string(),
        });
        recommendations.push(StrategyRobustnessMatrixRecommendation {
            priority: "HIGH".to_string(),
            code: "repair_market_data".to_string(),
            message: "Repair or exclude degraded windows before trusting the matrix.".to_string(),
        });
    }
    if median_pnl_pct >= Decimal::ZERO && avg_trade_count < Decimal::from(min_trades_per_cell) {
        recommendations.push(StrategyRobustnessMatrixRecommendation {
            priority: "MEDIUM".to_string(),
            code: "collect_more_trades".to_string(),
            message: "Expand symbols, windows, or holding periods to collect enough trades."
                .to_string(),
        });
    }
    recommendations.push(StrategyRobustnessMatrixRecommendation {
        priority: "LOW".to_string(),
        code: "do_not_auto_promote".to_string(),
        message: "Use the matrix as decision support only; do not auto-promote candidates."
            .to_string(),
    });
    (findings, recommendations)
}

impl ResearchRegimeLabel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TrendUp => "TREND_UP",
            Self::TrendDown => "TREND_DOWN",
            Self::Range => "RANGE",
            Self::HighVolatility => "HIGH_VOLATILITY",
            Self::LowVolatility => "LOW_VOLATILITY",
            Self::Mixed => "MIXED",
            Self::Unknown => "UNKNOWN",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateFailureReason {
    OverfitRisk,
    FeeDrag,
    NoBreakout,
    BreakoutTooExtended,
    TooManyTrades,
    TooFewTrades,
    LowWinRate,
    HighDrawdown,
    WeakEdge,
    DataQualityDegraded,
    RegimeMismatch,
    InsufficientData,
}

impl ResearchCandidateFailureReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OverfitRisk => "OVERFIT_RISK",
            Self::FeeDrag => "FEE_DRAG",
            Self::NoBreakout => "NO_BREAKOUT",
            Self::BreakoutTooExtended => "BREAKOUT_TOO_EXTENDED",
            Self::TooManyTrades => "TOO_MANY_TRADES",
            Self::TooFewTrades => "TOO_FEW_TRADES",
            Self::LowWinRate => "LOW_WIN_RATE",
            Self::HighDrawdown => "HIGH_DRAWDOWN",
            Self::WeakEdge => "WEAK_EDGE",
            Self::DataQualityDegraded => "DATA_QUALITY_DEGRADED",
            Self::RegimeMismatch => "REGIME_MISMATCH",
            Self::InsufficientData => "INSUFFICIENT_DATA",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchRegimeMetric {
    pub symbol: String,
    pub timeframe: String,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub candle_count: i32,
    pub return_pct: Decimal,
    pub realized_volatility: Decimal,
    pub average_candle_range_pct: Decimal,
    pub close_vs_sma_pct: Decimal,
    pub directional_movement_pct: Decimal,
    pub trend_slope: Decimal,
    pub choppiness_pct: Decimal,
    pub label: ResearchRegimeLabel,
    pub confidence: Decimal,
    pub explanation: ResearchRegimeClassificationExplanation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCampaignRegimeSummary {
    pub label: ResearchRegimeLabel,
    pub window_count: i32,
    pub candidate_count: i32,
    pub avg_return_pct: Decimal,
    pub avg_realized_volatility: Decimal,
    pub avg_candle_range_pct: Decimal,
    pub failure_reasons: Vec<ResearchCandidateFailureReason>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchCampaignFailureFinding {
    pub severity: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchCampaignFailureRecommendation {
    pub priority: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateFailureAttributionRow {
    pub candidate_id: Option<Uuid>,
    pub experiment_run_id: Option<Uuid>,
    pub walk_forward_run_id: Option<Uuid>,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub regime_label: ResearchRegimeLabel,
    pub failure_reasons: Vec<ResearchCandidateFailureReason>,
    pub pnl_pct: Option<Decimal>,
    pub gross_pnl_pct: Option<Decimal>,
    pub fee_drag_pct: Option<Decimal>,
    pub trade_count: Option<i32>,
    pub win_rate: Option<Decimal>,
    pub max_drawdown_pct: Option<Decimal>,
    pub walk_forward_status: Option<String>,
    pub data_quality_status: Option<MarketDataQualityStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchStrategyTimeframeFailureBreakdown {
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub candidate_count: i32,
    pub dominant_regime: ResearchRegimeLabel,
    pub top_failure_reasons: Vec<ResearchCandidateFailureReason>,
    pub avg_pnl_pct: Option<Decimal>,
    pub avg_trade_count: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCampaignFailureAttribution {
    pub campaign_id: Uuid,
    pub overall_failure_reasons: Vec<ResearchCandidateFailureReason>,
    pub regime_summary: Vec<ResearchCampaignRegimeSummary>,
    pub candidate_failure_table: Vec<ResearchCandidateFailureAttributionRow>,
    pub strategy_timeframe_breakdown: Vec<ResearchStrategyTimeframeFailureBreakdown>,
    pub findings: Vec<ResearchCampaignFailureFinding>,
    pub recommendations: Vec<ResearchCampaignFailureRecommendation>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchHypothesisSource {
    CampaignFailureAttribution,
    RegimeLeaderboard,
    OpportunityAnalysis,
    SignalFeatureAttribution,
    ExitAttribution,
    DataQuality,
}

impl ResearchHypothesisSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CampaignFailureAttribution => "CAMPAIGN_FAILURE_ATTRIBUTION",
            Self::RegimeLeaderboard => "REGIME_LEADERBOARD",
            Self::OpportunityAnalysis => "OPPORTUNITY_ANALYSIS",
            Self::SignalFeatureAttribution => "SIGNAL_FEATURE_ATTRIBUTION",
            Self::ExitAttribution => "EXIT_ATTRIBUTION",
            Self::DataQuality => "DATA_QUALITY",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchHypothesisStatus {
    Proposed,
    AcceptedForExperiment,
    Rejected,
    Archived,
}

impl ResearchHypothesisStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Proposed => "PROPOSED",
            Self::AcceptedForExperiment => "ACCEPTED_FOR_EXPERIMENT",
            Self::Rejected => "REJECTED",
            Self::Archived => "ARCHIVED",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchHypothesisPriority {
    High,
    Medium,
    Low,
}

impl ResearchHypothesisPriority {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::High => "HIGH",
            Self::Medium => "MEDIUM",
            Self::Low => "LOW",
        }
    }

    fn rank(self) -> i32 {
        match self {
            Self::High => 0,
            Self::Medium => 1,
            Self::Low => 2,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchHypothesisRecommendation {
    pub code: String,
    pub actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchHypothesisEvidence {
    pub summary: String,
    pub details: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchHypothesis {
    pub id: Option<Uuid>,
    pub source_type: ResearchHypothesisSource,
    pub status: ResearchHypothesisStatus,
    pub strategy_id: Option<String>,
    pub symbol: Option<String>,
    pub timeframe: Option<String>,
    pub regime: Option<ResearchRegimeLabel>,
    pub failure_reasons: Vec<ResearchCandidateFailureReason>,
    pub evidence: ResearchHypothesisEvidence,
    pub recommendation: ResearchHypothesisRecommendation,
    pub proposed_action: String,
    pub proposed_experiment_config: Value,
    pub priority: ResearchHypothesisPriority,
    pub expected_effect: String,
    pub risk: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchExperimentPlanStatus {
    Draft,
    Ready,
    Invalid,
    Runnable,
    Archived,
}

impl ResearchExperimentPlanStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "DRAFT",
            Self::Ready => "READY",
            Self::Invalid => "INVALID",
            Self::Runnable => "RUNNABLE",
            Self::Archived => "ARCHIVED",
        }
    }
}

impl std::str::FromStr for ResearchExperimentPlanStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "DRAFT" => Ok(Self::Draft),
            "READY" => Ok(Self::Ready),
            "INVALID" => Ok(Self::Invalid),
            "RUNNABLE" => Ok(Self::Runnable),
            "ARCHIVED" => Ok(Self::Archived),
            other => Err(CoreError::UnsupportedResearchExperimentPlanStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchExperimentPlanSource {
    AcceptedHypothesis,
    OperatorDraft,
}

impl ResearchExperimentPlanSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AcceptedHypothesis => "ACCEPTED_HYPOTHESIS",
            Self::OperatorDraft => "OPERATOR_DRAFT",
        }
    }
}

impl std::str::FromStr for ResearchExperimentPlanSource {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "ACCEPTED_HYPOTHESIS" => Ok(Self::AcceptedHypothesis),
            "OPERATOR_DRAFT" => Ok(Self::OperatorDraft),
            other => Err(CoreError::UnsupportedResearchExperimentPlanSource(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchExperimentPlanType {
    StrategyExperiment,
    ResearchBatch,
    ResearchCampaign,
    RobustnessMatrix,
    WalkForward,
}

impl ResearchExperimentPlanType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StrategyExperiment => "STRATEGY_EXPERIMENT",
            Self::ResearchBatch => "RESEARCH_BATCH",
            Self::ResearchCampaign => "RESEARCH_CAMPAIGN",
            Self::RobustnessMatrix => "ROBUSTNESS_MATRIX",
            Self::WalkForward => "WALK_FORWARD",
        }
    }
}

impl std::str::FromStr for ResearchExperimentPlanType {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "STRATEGY_EXPERIMENT" => Ok(Self::StrategyExperiment),
            "RESEARCH_BATCH" => Ok(Self::ResearchBatch),
            "RESEARCH_CAMPAIGN" => Ok(Self::ResearchCampaign),
            "ROBUSTNESS_MATRIX" => Ok(Self::RobustnessMatrix),
            "WALK_FORWARD" => Ok(Self::WalkForward),
            other => Err(CoreError::UnsupportedResearchExperimentPlanType(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchExperimentPlanStep {
    pub step_index: i32,
    pub code: String,
    pub description: String,
    pub research_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchExperimentPlanValidation {
    pub status: ResearchExperimentPlanStatus,
    pub issues: Vec<String>,
    pub validated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchExperimentPlanRecommendation {
    pub code: String,
    pub action: String,
    pub rationale: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchExperimentPlan {
    pub id: Option<Uuid>,
    pub hypothesis_id: Uuid,
    pub source: ResearchExperimentPlanSource,
    pub source_campaign_id: Option<Uuid>,
    pub strategy_id: String,
    pub symbol: Option<String>,
    pub timeframe: Option<String>,
    pub proposed_request: Value,
    pub plan_type: ResearchExperimentPlanType,
    pub status: ResearchExperimentPlanStatus,
    pub validation_status: ResearchExperimentPlanStatus,
    pub validation_issues: Vec<String>,
    pub steps: Vec<ResearchExperimentPlanStep>,
    pub recommendation: ResearchExperimentPlanRecommendation,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchExperimentPlanRunMode {
    Preview,
    Run,
}

impl ResearchExperimentPlanRunMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Preview => "PREVIEW",
            Self::Run => "RUN",
        }
    }
}

impl std::str::FromStr for ResearchExperimentPlanRunMode {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "PREVIEW" => Ok(Self::Preview),
            "RUN" => Ok(Self::Run),
            other => Err(CoreError::UnsupportedResearchExperimentPlanRunMode(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchExperimentPlanRunStatus {
    Ready,
    Running,
    Completed,
    Failed,
    Blocked,
    InvalidPlan,
}

impl ResearchExperimentPlanRunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "READY",
            Self::Running => "RUNNING",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Blocked => "BLOCKED",
            Self::InvalidPlan => "INVALID_PLAN",
        }
    }
}

impl std::str::FromStr for ResearchExperimentPlanRunStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "READY" => Ok(Self::Ready),
            "RUNNING" => Ok(Self::Running),
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            "BLOCKED" => Ok(Self::Blocked),
            "INVALID_PLAN" => Ok(Self::InvalidPlan),
            other => Err(CoreError::UnsupportedResearchExperimentPlanRunStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ResearchExperimentPlanRunArtifact {
    pub strategy_experiment_id: Option<Uuid>,
    pub research_batch_id: Option<Uuid>,
    pub research_campaign_id: Option<Uuid>,
    pub robustness_matrix_run_id: Option<Uuid>,
    pub walk_forward_run_id: Option<Uuid>,
}

impl ResearchExperimentPlanRunArtifact {
    pub fn artifact_type(&self) -> Option<&'static str> {
        if self.strategy_experiment_id.is_some() {
            Some("strategy_experiment_id")
        } else if self.research_batch_id.is_some() {
            Some("research_batch_id")
        } else if self.research_campaign_id.is_some() {
            Some("research_campaign_id")
        } else if self.robustness_matrix_run_id.is_some() {
            Some("robustness_matrix_run_id")
        } else if self.walk_forward_run_id.is_some() {
            Some("walk_forward_run_id")
        } else {
            None
        }
    }

    pub fn artifact_id(&self) -> Option<Uuid> {
        self.strategy_experiment_id
            .or(self.research_batch_id)
            .or(self.research_campaign_id)
            .or(self.robustness_matrix_run_id)
            .or(self.walk_forward_run_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchExperimentPlanRunRequest {
    pub mode: ResearchExperimentPlanRunMode,
    pub confirmation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchExperimentPlanRunResult {
    pub plan_id: Uuid,
    pub hypothesis_id: Uuid,
    pub plan_type: ResearchExperimentPlanType,
    pub status: ResearchExperimentPlanRunStatus,
    pub mode: ResearchExperimentPlanRunMode,
    pub validation_status: ResearchExperimentPlanStatus,
    pub created_artifacts: Vec<ResearchExperimentPlanRunArtifact>,
    pub artifact_ids: Vec<Uuid>,
    pub warnings: Vec<String>,
    pub blockers: Vec<String>,
    pub recommendation: String,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResearchHypothesisIncludedSource {
    FailureAttribution,
    RegimeLeaderboard,
    OpportunityAnalysis,
    SignalFeatureAttribution,
    ExitAttribution,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchHypothesisGenerationRequest {
    pub campaign_id: Option<Uuid>,
    pub batch_id: Option<Uuid>,
    pub candidate_id: Option<Uuid>,
    #[serde(default)]
    pub include_sources: Vec<ResearchHypothesisIncludedSource>,
    #[serde(default = "default_research_hypothesis_persist")]
    pub persist: bool,
}

fn default_research_hypothesis_persist() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchHypothesisGenerationResult {
    pub hypotheses: Vec<ResearchHypothesis>,
    pub generated_count: i32,
    pub persisted_count: i32,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct ResearchHypothesisGenerationEvidence {
    pub failure_attribution: Option<ResearchCampaignFailureAttribution>,
    pub regime_leaderboard: Option<ResearchRegimeStrategyLeaderboard>,
    pub opportunity_analysis: Option<StrategyOpportunityAnalysisResult>,
    pub signal_feature_attribution: Option<StrategySignalFeatureAttributionResult>,
    pub exit_attribution: Option<StrategyExitAttributionResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateFailureInput {
    pub candidate_id: Option<Uuid>,
    pub experiment_run_id: Option<Uuid>,
    pub walk_forward_run_id: Option<Uuid>,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub regime_metric: ResearchRegimeMetric,
    pub pnl_pct: Option<Decimal>,
    pub gross_pnl_pct: Option<Decimal>,
    pub fee_drag_pct: Option<Decimal>,
    pub trade_count: Option<i32>,
    pub win_rate: Option<Decimal>,
    pub max_drawdown_pct: Option<Decimal>,
    pub walk_forward_status: Option<String>,
    pub walk_forward_profitable_windows: Option<i32>,
    pub walk_forward_losing_windows: Option<i32>,
    pub data_quality_status: Option<MarketDataQualityStatus>,
}

pub fn classify_research_regime(
    symbol: impl Into<String>,
    timeframe: impl Into<String>,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    candles: &[Candle],
) -> ResearchRegimeMetric {
    classify_research_regime_with_config(
        symbol,
        timeframe,
        window_start,
        window_end,
        candles,
        &ResearchRegimeClassifierConfig::default(),
    )
}

fn push_regime_condition(
    conditions: &mut Vec<ResearchRegimeClassificationCondition>,
    label: ResearchRegimeLabel,
    metric: &str,
    operator: &str,
    value: Decimal,
    threshold: Decimal,
    passed: bool,
) {
    conditions.push(ResearchRegimeClassificationCondition {
        label,
        metric: metric.to_string(),
        operator: operator.to_string(),
        value,
        threshold,
        passed,
        reason: format!(
            "{} {} {} {}",
            metric,
            operator,
            threshold,
            if passed { "passed" } else { "failed" }
        ),
    });
}

pub fn classify_research_regime_with_config(
    symbol: impl Into<String>,
    timeframe: impl Into<String>,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    candles: &[Candle],
    config: &ResearchRegimeClassifierConfig,
) -> ResearchRegimeMetric {
    let symbol = symbol.into();
    let timeframe = timeframe.into();
    let config = config.clone();
    if candles.len() < REGIME_MIN_CANDLES {
        let explanation = ResearchRegimeClassificationExplanation {
            return_pct: Decimal::ZERO,
            realized_volatility: Decimal::ZERO,
            avg_range_pct: Decimal::ZERO,
            trend_slope: Decimal::ZERO,
            choppiness_proxy: Decimal::ZERO,
            thresholds_used: config.clone(),
            conditions: Vec::new(),
            final_label: ResearchRegimeLabel::Unknown,
            confidence: Decimal::ZERO,
            alternate_labels_considered: Vec::new(),
        };
        return ResearchRegimeMetric {
            symbol,
            timeframe,
            window_start,
            window_end,
            candle_count: i32::try_from(candles.len()).unwrap_or(i32::MAX),
            return_pct: Decimal::ZERO,
            realized_volatility: Decimal::ZERO,
            average_candle_range_pct: Decimal::ZERO,
            close_vs_sma_pct: Decimal::ZERO,
            directional_movement_pct: Decimal::ZERO,
            trend_slope: Decimal::ZERO,
            choppiness_pct: Decimal::ZERO,
            label: ResearchRegimeLabel::Unknown,
            confidence: Decimal::ZERO,
            explanation,
        };
    }

    let first_close = candles
        .first()
        .map(|candle| candle.close)
        .unwrap_or_default();
    let last_close = candles
        .last()
        .map(|candle| candle.close)
        .unwrap_or_default();
    let return_pct = pct_change(first_close, last_close);
    let sma = decimal_avg(candles.iter().map(|candle| candle.close));
    let close_vs_sma_pct = pct_change(sma, last_close);
    let average_candle_range_pct = decimal_avg(candles.iter().map(|candle| {
        let mid = (candle.high + candle.low) / Decimal::new(2, 0);
        pct_ratio(candle.high - candle.low, mid)
    }));

    let mut absolute_returns = Vec::new();
    let mut directional_moves = Decimal::ZERO;
    for pair in candles.windows(2) {
        let previous = pair[0].close;
        let current = pair[1].close;
        let change = current - previous;
        absolute_returns.push(pct_ratio(change.abs(), previous));
        if change > Decimal::ZERO {
            directional_moves += Decimal::ONE;
        } else if change < Decimal::ZERO {
            directional_moves -= Decimal::ONE;
        }
    }
    let realized_volatility = decimal_avg(absolute_returns.into_iter());
    let trend_slope = (return_pct
        / Decimal::from(i64::try_from(candles.len().saturating_sub(1)).unwrap_or(i64::MAX)))
    .round_dp(8);
    let directional_movement_pct = pct_ratio(
        directional_moves.abs(),
        Decimal::from(i64::try_from(candles.len().saturating_sub(1)).unwrap_or(i64::MAX)),
    );
    let path_pct = candles
        .windows(2)
        .map(|pair| (pct_change(pair[0].close, pair[1].close)).abs())
        .fold(Decimal::ZERO, |sum, value| sum + value);
    let choppiness_pct = if path_pct > Decimal::ZERO {
        Decimal::new(100, 0) - pct_ratio(return_pct.abs(), path_pct).min(Decimal::new(100, 0))
    } else {
        Decimal::ZERO
    };

    let mut conditions = Vec::new();
    push_regime_condition(
        &mut conditions,
        ResearchRegimeLabel::HighVolatility,
        "realized_volatility",
        ">=",
        realized_volatility,
        config.high_volatility_threshold_pct,
        realized_volatility >= config.high_volatility_threshold_pct,
    );
    push_regime_condition(
        &mut conditions,
        ResearchRegimeLabel::HighVolatility,
        "avg_range_pct",
        ">=",
        average_candle_range_pct,
        config.high_volatility_threshold_pct,
        average_candle_range_pct >= config.high_volatility_threshold_pct,
    );
    push_regime_condition(
        &mut conditions,
        ResearchRegimeLabel::TrendUp,
        "return_pct",
        ">=",
        return_pct,
        config.trend_return_threshold_pct,
        return_pct >= config.trend_return_threshold_pct,
    );
    push_regime_condition(
        &mut conditions,
        ResearchRegimeLabel::TrendUp,
        "trend_slope",
        ">=",
        trend_slope,
        config.trend_slope_threshold,
        trend_slope >= config.trend_slope_threshold,
    );
    push_regime_condition(
        &mut conditions,
        ResearchRegimeLabel::TrendDown,
        "return_pct",
        "<=",
        return_pct,
        -config.trend_return_threshold_pct,
        return_pct <= -config.trend_return_threshold_pct,
    );
    push_regime_condition(
        &mut conditions,
        ResearchRegimeLabel::TrendDown,
        "trend_slope",
        "<=",
        trend_slope,
        -config.trend_slope_threshold,
        trend_slope <= -config.trend_slope_threshold,
    );
    push_regime_condition(
        &mut conditions,
        ResearchRegimeLabel::Range,
        "abs_return_pct",
        "<=",
        return_pct.abs(),
        config.range_return_max_pct,
        return_pct.abs() <= config.range_return_max_pct,
    );
    push_regime_condition(
        &mut conditions,
        ResearchRegimeLabel::Range,
        "choppiness_proxy",
        ">=",
        choppiness_pct,
        config.range_choppiness_min,
        choppiness_pct >= config.range_choppiness_min,
    );
    push_regime_condition(
        &mut conditions,
        ResearchRegimeLabel::LowVolatility,
        "realized_volatility",
        "<=",
        realized_volatility,
        config.low_volatility_threshold_pct,
        realized_volatility <= config.low_volatility_threshold_pct,
    );
    push_regime_condition(
        &mut conditions,
        ResearchRegimeLabel::LowVolatility,
        "avg_range_pct",
        "<=",
        average_candle_range_pct,
        config.low_volatility_threshold_pct,
        average_candle_range_pct <= config.low_volatility_threshold_pct,
    );

    let mut passed_labels = Vec::new();
    if realized_volatility >= config.high_volatility_threshold_pct
        || average_candle_range_pct >= config.high_volatility_threshold_pct
    {
        passed_labels.push(ResearchRegimeLabel::HighVolatility);
    }
    if return_pct >= config.trend_return_threshold_pct
        && trend_slope >= config.trend_slope_threshold
    {
        passed_labels.push(ResearchRegimeLabel::TrendUp);
    }
    if return_pct <= -config.trend_return_threshold_pct
        && trend_slope <= -config.trend_slope_threshold
    {
        passed_labels.push(ResearchRegimeLabel::TrendDown);
    }
    if return_pct.abs() <= config.range_return_max_pct
        || choppiness_pct >= config.range_choppiness_min
    {
        passed_labels.push(ResearchRegimeLabel::Range);
    }
    if realized_volatility <= config.low_volatility_threshold_pct
        && average_candle_range_pct <= config.low_volatility_threshold_pct
    {
        passed_labels.push(ResearchRegimeLabel::LowVolatility);
    }
    let mut label = config
        .priority_order
        .iter()
        .copied()
        .find(|priority| passed_labels.contains(priority))
        .unwrap_or(ResearchRegimeLabel::Mixed);

    let mut metric = ResearchRegimeMetric {
        symbol,
        timeframe,
        window_start,
        window_end,
        candle_count: i32::try_from(candles.len()).unwrap_or(i32::MAX),
        return_pct,
        realized_volatility,
        average_candle_range_pct,
        close_vs_sma_pct,
        directional_movement_pct,
        trend_slope,
        choppiness_pct,
        label,
        confidence: Decimal::ZERO,
        explanation: ResearchRegimeClassificationExplanation {
            return_pct,
            realized_volatility,
            avg_range_pct: average_candle_range_pct,
            trend_slope,
            choppiness_proxy: choppiness_pct,
            thresholds_used: config.clone(),
            conditions: conditions.clone(),
            final_label: label,
            confidence: Decimal::ZERO,
            alternate_labels_considered: passed_labels
                .iter()
                .copied()
                .filter(|candidate| *candidate != label)
                .collect(),
        },
    };
    let confidence = regime_confidence(&metric).round_dp(4);
    if confidence < config.min_confidence && label != ResearchRegimeLabel::Unknown {
        label = ResearchRegimeLabel::Mixed;
    }
    metric.label = label;
    metric.confidence = if label == ResearchRegimeLabel::Mixed {
        confidence.min(Decimal::new(50, 0))
    } else {
        confidence
    };
    metric.explanation.final_label = label;
    metric.explanation.confidence = metric.confidence;
    metric.explanation.alternate_labels_considered = passed_labels
        .into_iter()
        .filter(|candidate| *candidate != label)
        .collect();
    metric
}

pub fn infer_research_candidate_failure_reasons(
    input: &ResearchCandidateFailureInput,
) -> Vec<ResearchCandidateFailureReason> {
    let mut reasons = BTreeSet::new();
    if input.regime_metric.label == ResearchRegimeLabel::Unknown
        || input
            .data_quality_status
            .is_some_and(|status| status == MarketDataQualityStatus::InsufficientData)
    {
        reasons.insert(ResearchCandidateFailureReason::InsufficientData);
    }
    if input.data_quality_status.is_some_and(|status| {
        matches!(
            status,
            MarketDataQualityStatus::Degraded
                | MarketDataQualityStatus::Bad
                | MarketDataQualityStatus::InsufficientData
        )
    }) {
        reasons.insert(ResearchCandidateFailureReason::DataQualityDegraded);
    }
    if input
        .walk_forward_status
        .as_deref()
        .is_some_and(|status| status == "OVERFIT_RISK")
        || input
            .walk_forward_losing_windows
            .zip(input.walk_forward_profitable_windows)
            .is_some_and(|(losing, profitable)| profitable <= 1 && losing >= 2)
    {
        reasons.insert(ResearchCandidateFailureReason::OverfitRisk);
    }
    if input
        .trade_count
        .is_some_and(|count| count <= FEW_TRADES_THRESHOLD)
    {
        reasons.insert(ResearchCandidateFailureReason::TooFewTrades);
    }
    if input
        .trade_count
        .is_some_and(|count| count >= MANY_TRADES_PER_WINDOW_THRESHOLD)
        && input.pnl_pct.is_some_and(|pnl| pnl < Decimal::ZERO)
    {
        reasons.insert(ResearchCandidateFailureReason::TooManyTrades);
    }
    if input
        .fee_drag_pct
        .is_some_and(|fee_drag| fee_drag >= Decimal::new(20, 1))
        || input
            .gross_pnl_pct
            .zip(input.pnl_pct)
            .is_some_and(|(gross, net)| gross > Decimal::ZERO && net < Decimal::ZERO)
    {
        reasons.insert(ResearchCandidateFailureReason::FeeDrag);
    }
    if input.strategy_id == "volatility_compression_breakout_v1" {
        if matches!(
            input.regime_metric.label,
            ResearchRegimeLabel::LowVolatility | ResearchRegimeLabel::Range
        ) && input
            .trade_count
            .is_none_or(|count| count <= FEW_TRADES_THRESHOLD)
        {
            reasons.insert(ResearchCandidateFailureReason::NoBreakout);
            reasons.insert(ResearchCandidateFailureReason::TooFewTrades);
        }
        if input.regime_metric.label == ResearchRegimeLabel::HighVolatility
            && input.pnl_pct.is_some_and(|pnl| pnl <= Decimal::ZERO)
        {
            reasons.insert(ResearchCandidateFailureReason::BreakoutTooExtended);
        }
    }
    if input
        .win_rate
        .is_some_and(|win_rate| win_rate < Decimal::new(40, 0))
    {
        reasons.insert(ResearchCandidateFailureReason::LowWinRate);
    }
    if input
        .max_drawdown_pct
        .is_some_and(|drawdown| drawdown >= Decimal::new(10, 0))
    {
        reasons.insert(ResearchCandidateFailureReason::HighDrawdown);
    }
    if input.pnl_pct.is_some_and(|pnl| pnl <= Decimal::ZERO)
        && !reasons.contains(&ResearchCandidateFailureReason::OverfitRisk)
    {
        reasons.insert(ResearchCandidateFailureReason::WeakEdge);
    }
    if strategy_expects_trend(&input.strategy_id)
        && matches!(
            input.regime_metric.label,
            ResearchRegimeLabel::Range | ResearchRegimeLabel::LowVolatility
        )
    {
        reasons.insert(ResearchCandidateFailureReason::RegimeMismatch);
    }
    if strategy_expects_range(&input.strategy_id)
        && matches!(
            input.regime_metric.label,
            ResearchRegimeLabel::TrendUp | ResearchRegimeLabel::TrendDown
        )
    {
        reasons.insert(ResearchCandidateFailureReason::RegimeMismatch);
    }
    if reasons.is_empty() {
        reasons.insert(ResearchCandidateFailureReason::WeakEdge);
    }
    reasons.into_iter().collect()
}

pub fn build_research_campaign_failure_attribution(
    campaign_id: Uuid,
    inputs: Vec<ResearchCandidateFailureInput>,
    generated_at: DateTime<Utc>,
) -> ResearchCampaignFailureAttribution {
    let candidate_failure_table = inputs
        .iter()
        .map(|input| ResearchCandidateFailureAttributionRow {
            candidate_id: input.candidate_id,
            experiment_run_id: input.experiment_run_id,
            walk_forward_run_id: input.walk_forward_run_id,
            strategy_id: input.strategy_id.clone(),
            symbol: input.symbol.clone(),
            timeframe: input.timeframe.clone(),
            window_start: input.window_start,
            window_end: input.window_end,
            regime_label: input.regime_metric.label,
            failure_reasons: infer_research_candidate_failure_reasons(input),
            pnl_pct: input.pnl_pct,
            gross_pnl_pct: input.gross_pnl_pct,
            fee_drag_pct: input.fee_drag_pct,
            trade_count: input.trade_count,
            win_rate: input.win_rate,
            max_drawdown_pct: input.max_drawdown_pct,
            walk_forward_status: input.walk_forward_status.clone(),
            data_quality_status: input.data_quality_status,
        })
        .collect::<Vec<_>>();
    let overall_failure_reasons = ranked_failure_reasons(
        candidate_failure_table
            .iter()
            .flat_map(|row| row.failure_reasons.iter().copied()),
    );
    let regime_summary = build_regime_summary(&inputs, &candidate_failure_table);
    let strategy_timeframe_breakdown =
        build_strategy_timeframe_breakdown(&inputs, &candidate_failure_table);
    let findings = build_failure_findings(&candidate_failure_table);
    let recommendations = build_failure_recommendations(&candidate_failure_table);

    ResearchCampaignFailureAttribution {
        campaign_id,
        overall_failure_reasons,
        regime_summary,
        candidate_failure_table,
        strategy_timeframe_breakdown,
        findings,
        recommendations,
        generated_at,
    }
}

fn build_regime_summary(
    inputs: &[ResearchCandidateFailureInput],
    rows: &[ResearchCandidateFailureAttributionRow],
) -> Vec<ResearchCampaignRegimeSummary> {
    let mut by_label: BTreeMap<ResearchRegimeLabel, Vec<&ResearchCandidateFailureInput>> =
        BTreeMap::new();
    for input in inputs {
        by_label
            .entry(input.regime_metric.label)
            .or_default()
            .push(input);
    }
    by_label
        .into_iter()
        .map(|(label, inputs)| {
            let candidate_count =
                rows.iter().filter(|row| row.regime_label == label).count() as i32;
            ResearchCampaignRegimeSummary {
                label,
                window_count: i32::try_from(inputs.len()).unwrap_or(i32::MAX),
                candidate_count,
                avg_return_pct: decimal_avg(
                    inputs.iter().map(|input| input.regime_metric.return_pct),
                ),
                avg_realized_volatility: decimal_avg(
                    inputs
                        .iter()
                        .map(|input| input.regime_metric.realized_volatility),
                ),
                avg_candle_range_pct: decimal_avg(
                    inputs
                        .iter()
                        .map(|input| input.regime_metric.average_candle_range_pct),
                ),
                failure_reasons: ranked_failure_reasons(
                    rows.iter()
                        .filter(|row| row.regime_label == label)
                        .flat_map(|row| row.failure_reasons.iter().copied()),
                ),
            }
        })
        .collect()
}

fn build_strategy_timeframe_breakdown(
    inputs: &[ResearchCandidateFailureInput],
    rows: &[ResearchCandidateFailureAttributionRow],
) -> Vec<ResearchStrategyTimeframeFailureBreakdown> {
    let mut keys = BTreeSet::new();
    for row in rows {
        keys.insert((
            row.strategy_id.clone(),
            row.symbol.clone(),
            row.timeframe.clone(),
        ));
    }
    keys.into_iter()
        .map(|(strategy_id, symbol, timeframe)| {
            let matching_rows = rows
                .iter()
                .filter(|row| {
                    row.strategy_id == strategy_id
                        && row.symbol == symbol
                        && row.timeframe == timeframe
                })
                .collect::<Vec<_>>();
            let matching_inputs = inputs
                .iter()
                .filter(|input| {
                    input.strategy_id == strategy_id
                        && input.symbol == symbol
                        && input.timeframe == timeframe
                })
                .collect::<Vec<_>>();
            let dominant_regime = ranked_regime_labels(
                matching_inputs
                    .iter()
                    .map(|input| input.regime_metric.label),
            )
            .first()
            .copied()
            .unwrap_or(ResearchRegimeLabel::Unknown);
            ResearchStrategyTimeframeFailureBreakdown {
                strategy_id,
                symbol,
                timeframe,
                candidate_count: i32::try_from(matching_rows.len()).unwrap_or(i32::MAX),
                dominant_regime,
                top_failure_reasons: ranked_failure_reasons(
                    matching_rows
                        .iter()
                        .flat_map(|row| row.failure_reasons.iter().copied()),
                ),
                avg_pnl_pct: optional_decimal_avg(
                    matching_rows.iter().filter_map(|row| row.pnl_pct),
                ),
                avg_trade_count: optional_decimal_avg(
                    matching_rows
                        .iter()
                        .filter_map(|row| row.trade_count.map(Decimal::from)),
                ),
            }
        })
        .collect()
}

fn build_failure_findings(
    rows: &[ResearchCandidateFailureAttributionRow],
) -> Vec<ResearchCampaignFailureFinding> {
    let overfit = count_reason(rows, ResearchCandidateFailureReason::OverfitRisk);
    let fee_drag = count_reason(rows, ResearchCandidateFailureReason::FeeDrag);
    let regime_mismatch = count_reason(rows, ResearchCandidateFailureReason::RegimeMismatch);
    let actionable = rows
        .iter()
        .filter(|row| row.pnl_pct.is_some_and(|pnl| pnl > Decimal::ZERO))
        .filter(|row| {
            !row.failure_reasons
                .contains(&ResearchCandidateFailureReason::OverfitRisk)
        })
        .count();
    let mut findings = Vec::new();
    if overfit > 0 {
        findings.push(failure_finding(
            "MEDIUM",
            "campaign_candidates_mostly_overfit",
            "Campaign candidates mostly failed due to overfit risk.",
        ));
    }
    if fee_drag > 0 {
        findings.push(failure_finding(
            "MEDIUM",
            "campaign_fee_drag_sensitive",
            "Campaign candidates show fee-drag sensitivity.",
        ));
    }
    if regime_mismatch > 0 {
        findings.push(failure_finding(
            "MEDIUM",
            "strategy_regime_mismatch",
            "Strategy appears mismatched to market regime.",
        ));
    }
    if actionable == 0 {
        findings.push(failure_finding(
            "LOW",
            "no_actionable_candidate_found",
            "No actionable candidate found.",
        ));
    }
    findings
}

fn build_failure_recommendations(
    rows: &[ResearchCandidateFailureAttributionRow],
) -> Vec<ResearchCampaignFailureRecommendation> {
    let mut recommendations = Vec::new();
    let reason_order = ranked_failure_reasons(
        rows.iter()
            .flat_map(|row| row.failure_reasons.iter().copied()),
    );
    for reason in reason_order {
        match reason {
            ResearchCandidateFailureReason::OverfitRisk => recommendations.push(
                failure_recommendation(
                    "HIGH",
                    "widen_walk_forward_validation",
                    "Reject overfit candidates and test broader windows before adding new strategy families.",
                ),
            ),
            ResearchCandidateFailureReason::FeeDrag
            | ResearchCandidateFailureReason::BreakoutTooExtended
            | ResearchCandidateFailureReason::TooManyTrades => recommendations.push(
                failure_recommendation(
                    "HIGH",
                    "reduce_turnover_before_expansion",
                    "Prioritize lower-turnover parameters and fee sensitivity checks.",
                ),
            ),
            ResearchCandidateFailureReason::RegimeMismatch => recommendations.push(
                failure_recommendation(
                    "MEDIUM",
                    "separate_trend_and_range_campaigns",
                    "Segment future campaigns by deterministic regime before comparing strategy families.",
                ),
            ),
            ResearchCandidateFailureReason::DataQualityDegraded
            | ResearchCandidateFailureReason::InsufficientData => recommendations.push(
                failure_recommendation(
                    "MEDIUM",
                    "repair_data_before_research",
                    "Repair or backfill degraded candle windows before trusting candidate evidence.",
                ),
            ),
            ResearchCandidateFailureReason::TooFewTrades => recommendations.push(
                failure_recommendation(
                    "LOW",
                    "run_opportunity_analysis",
                    "Run opportunity analysis for strategies with too few trades before changing strategy logic or extending campaign windows.",
                ),
            ),
            ResearchCandidateFailureReason::LowWinRate
            | ResearchCandidateFailureReason::HighDrawdown
            | ResearchCandidateFailureReason::NoBreakout
            | ResearchCandidateFailureReason::WeakEdge => recommendations.push(
                failure_recommendation(
                    "LOW",
                    "run_signal_feature_attribution",
                    "Run signal feature attribution for weak-edge strategies before changing exits or promoting candidates.",
                ),
            ),
        }
    }
    recommendations.dedup_by(|left, right| left.code == right.code);
    recommendations
}

fn ranked_failure_reasons(
    reasons: impl IntoIterator<Item = ResearchCandidateFailureReason>,
) -> Vec<ResearchCandidateFailureReason> {
    let mut counts: BTreeMap<ResearchCandidateFailureReason, i32> = BTreeMap::new();
    for reason in reasons {
        *counts.entry(reason).or_default() += 1;
    }
    let mut ranked = counts.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|(left_reason, left_count), (right_reason, right_count)| {
        right_count.cmp(left_count).then_with(|| {
            failure_reason_rank(*left_reason).cmp(&failure_reason_rank(*right_reason))
        })
    });
    ranked.into_iter().map(|(reason, _)| reason).collect()
}

fn ranked_regime_labels(
    labels: impl IntoIterator<Item = ResearchRegimeLabel>,
) -> Vec<ResearchRegimeLabel> {
    let mut counts: BTreeMap<ResearchRegimeLabel, i32> = BTreeMap::new();
    for label in labels {
        *counts.entry(label).or_default() += 1;
    }
    let mut ranked = counts.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|(left_label, left_count), (right_label, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_label.cmp(right_label))
    });
    ranked.into_iter().map(|(label, _)| label).collect()
}

fn failure_reason_rank(reason: ResearchCandidateFailureReason) -> i32 {
    match reason {
        ResearchCandidateFailureReason::OverfitRisk => 0,
        ResearchCandidateFailureReason::FeeDrag => 1,
        ResearchCandidateFailureReason::BreakoutTooExtended => 2,
        ResearchCandidateFailureReason::NoBreakout => 3,
        ResearchCandidateFailureReason::RegimeMismatch => 4,
        ResearchCandidateFailureReason::TooManyTrades => 5,
        ResearchCandidateFailureReason::TooFewTrades => 6,
        ResearchCandidateFailureReason::LowWinRate => 7,
        ResearchCandidateFailureReason::HighDrawdown => 8,
        ResearchCandidateFailureReason::WeakEdge => 9,
        ResearchCandidateFailureReason::DataQualityDegraded => 10,
        ResearchCandidateFailureReason::InsufficientData => 11,
    }
}

fn count_reason(
    rows: &[ResearchCandidateFailureAttributionRow],
    reason: ResearchCandidateFailureReason,
) -> usize {
    rows.iter()
        .filter(|row| row.failure_reasons.contains(&reason))
        .count()
}

fn decimal_avg(values: impl IntoIterator<Item = Decimal>) -> Decimal {
    let mut sum = Decimal::ZERO;
    let mut count = 0i64;
    for value in values {
        sum += value;
        count += 1;
    }
    if count == 0 {
        Decimal::ZERO
    } else {
        sum / Decimal::from(count)
    }
}

fn optional_decimal_avg(values: impl IntoIterator<Item = Decimal>) -> Option<Decimal> {
    let mut sum = Decimal::ZERO;
    let mut count = 0i64;
    for value in values {
        sum += value;
        count += 1;
    }
    (count > 0).then(|| sum / Decimal::from(count))
}

fn pct_change(base: Decimal, value: Decimal) -> Decimal {
    pct_ratio(value - base, base)
}

fn pct_ratio(numerator: Decimal, denominator: Decimal) -> Decimal {
    if denominator == Decimal::ZERO {
        Decimal::ZERO
    } else {
        (numerator / denominator) * Decimal::new(100, 0)
    }
}

fn strategy_expects_trend(strategy_id: &str) -> bool {
    let strategy_id = strategy_id.to_ascii_lowercase();
    strategy_id.contains("trend")
        || strategy_id.contains("momentum")
        || strategy_id.contains("breakout")
}

fn strategy_expects_range(strategy_id: &str) -> bool {
    strategy_id.to_ascii_lowercase().contains("range")
}

fn failure_finding(
    severity: impl Into<String>,
    code: impl Into<String>,
    message: impl Into<String>,
) -> ResearchCampaignFailureFinding {
    ResearchCampaignFailureFinding {
        severity: severity.into(),
        code: code.into(),
        message: message.into(),
    }
}

fn failure_recommendation(
    priority: impl Into<String>,
    code: impl Into<String>,
    message: impl Into<String>,
) -> ResearchCampaignFailureRecommendation {
    ResearchCampaignFailureRecommendation {
        priority: priority.into(),
        code: code.into(),
        message: message.into(),
    }
}

pub fn generate_research_hypotheses(
    evidence: ResearchHypothesisGenerationEvidence,
    generated_at: DateTime<Utc>,
) -> ResearchHypothesisGenerationResult {
    let mut hypotheses = Vec::new();
    if let Some(attribution) = evidence.failure_attribution.as_ref() {
        hypotheses.extend(hypotheses_from_failure_attribution(
            attribution,
            generated_at,
        ));
    }
    if let Some(leaderboard) = evidence.regime_leaderboard.as_ref() {
        hypotheses.extend(hypotheses_from_regime_leaderboard(
            leaderboard,
            generated_at,
        ));
    }
    if let Some(opportunity) = evidence.opportunity_analysis.as_ref() {
        hypotheses.extend(hypotheses_from_opportunity_analysis(
            opportunity,
            generated_at,
        ));
    }
    if let Some(signal_features) = evidence.signal_feature_attribution.as_ref() {
        hypotheses.extend(hypotheses_from_signal_feature_attribution(
            signal_features,
            generated_at,
        ));
    }
    if let Some(exit_attribution) = evidence.exit_attribution.as_ref() {
        hypotheses.extend(hypotheses_from_exit_attribution(
            exit_attribution,
            generated_at,
        ));
    }
    hypotheses = dedupe_and_sort_hypotheses(hypotheses);
    ResearchHypothesisGenerationResult {
        generated_count: i32::try_from(hypotheses.len()).unwrap_or(i32::MAX),
        persisted_count: 0,
        hypotheses,
        generated_at,
    }
}

fn hypotheses_from_failure_attribution(
    attribution: &ResearchCampaignFailureAttribution,
    created_at: DateTime<Utc>,
) -> Vec<ResearchHypothesis> {
    let mut hypotheses = Vec::new();
    for breakdown in &attribution.strategy_timeframe_breakdown {
        let matching_rows = attribution
            .candidate_failure_table
            .iter()
            .filter(|row| {
                row.strategy_id == breakdown.strategy_id
                    && row.symbol == breakdown.symbol
                    && row.timeframe == breakdown.timeframe
            })
            .collect::<Vec<_>>();
        let top_reason = breakdown.top_failure_reasons.first().copied();
        for reason in &breakdown.top_failure_reasons {
            let priority = match reason {
                ResearchCandidateFailureReason::FeeDrag => {
                    if top_reason == Some(ResearchCandidateFailureReason::FeeDrag) {
                        ResearchHypothesisPriority::High
                    } else {
                        ResearchHypothesisPriority::Medium
                    }
                }
                ResearchCandidateFailureReason::TooManyTrades
                | ResearchCandidateFailureReason::RegimeMismatch
                | ResearchCandidateFailureReason::OverfitRisk
                | ResearchCandidateFailureReason::DataQualityDegraded => {
                    ResearchHypothesisPriority::High
                }
                ResearchCandidateFailureReason::TooFewTrades => ResearchHypothesisPriority::Medium,
                _ => ResearchHypothesisPriority::Low,
            };
            let (code, actions, proposed_action, expected_effect, risk, config) = match reason {
                ResearchCandidateFailureReason::FeeDrag
                | ResearchCandidateFailureReason::TooManyTrades => (
                    "reduce_fee_drag_and_turnover",
                    vec![
                        "increase timeframe",
                        "increase cooldown",
                        "tighten entry filters",
                        "test lower trade frequency",
                    ],
                    "Test a lower-turnover variant before adding new strategy families.",
                    "Lower fee drag and reduce weak churn.",
                    "May miss short-lived opportunities and reduce sample count.",
                    json!({
                        "experiment": "lower_trade_frequency",
                        "timeframe_multiplier": 2,
                        "cooldown_multiplier": 2,
                        "entry_filter": "tighten",
                        "max_trade_frequency": "below_current_median"
                    }),
                ),
                ResearchCandidateFailureReason::TooFewTrades => (
                    "loosen_entry_opportunity",
                    vec![
                        "loosen entry thresholds",
                        "expand lower/upper bands",
                        "test more permissive config",
                    ],
                    "Test a more permissive configuration to prove opportunity exists.",
                    "Increase sample size before judging edge.",
                    "May add noisy trades and worsen fee drag.",
                    json!({
                        "experiment": "more_permissive_entries",
                        "entry_threshold": "loosen",
                        "range_bands": "expand",
                        "min_trade_count": "above_too_few_threshold"
                    }),
                ),
                ResearchCandidateFailureReason::RegimeMismatch => (
                    "split_or_disable_mismatched_regime",
                    vec![
                        "disable strategy for mismatched regime",
                        "test regime-specific strategy",
                        "split campaigns by regime",
                    ],
                    "Run regime-segmented research and disable this strategy in mismatched regimes.",
                    "Reduce strategy/regime mismatch and make comparisons cleaner.",
                    "Could discard rare transitional regimes too aggressively.",
                    json!({
                        "experiment": "regime_specific_campaign",
                        "disabled_regime": breakdown.dominant_regime.as_str(),
                        "split_by_regime": true
                    }),
                ),
                ResearchCandidateFailureReason::OverfitRisk => (
                    "broaden_walk_forward_validation",
                    vec![
                        "require broader walk-forward",
                        "add more windows/symbols",
                        "reject candidate unless robustness improves",
                    ],
                    "Broaden walk-forward validation before accepting any candidate.",
                    "Separate robust behavior from window-specific overfit.",
                    "More validation may leave no candidate eligible.",
                    json!({
                        "experiment": "broader_walk_forward",
                        "min_windows": "increase",
                        "symbols": "expand",
                        "acceptance_gate": "robustness_improves"
                    }),
                ),
                ResearchCandidateFailureReason::DataQualityDegraded
                | ResearchCandidateFailureReason::InsufficientData => (
                    "repair_data_before_research",
                    vec![
                        "repair data before research",
                        "do not accept candidates from degraded windows",
                    ],
                    "Repair or backfill data, then rerun research for affected windows.",
                    "Avoid accepting candidates based on degraded evidence.",
                    "Research is blocked until data coverage is fixed.",
                    json!({
                        "experiment": "data_repair_rerun",
                        "require_good_data_quality": true,
                        "accept_degraded_windows": false
                    }),
                ),
                _ => (
                    "investigate_weak_edge",
                    vec!["run feature attribution", "compare against opportunity analysis"],
                    "Run attribution before changing strategy logic.",
                    "Identify whether weak edge is entry quality or opportunity scarcity.",
                    "May confirm that the strategy should be rejected.",
                    json!({ "experiment": "diagnose_weak_edge" }),
                ),
            };
            hypotheses.push(research_hypothesis(
                ResearchHypothesisSource::CampaignFailureAttribution,
                Some(breakdown.strategy_id.clone()),
                Some(breakdown.symbol.clone()),
                Some(breakdown.timeframe.clone()),
                Some(breakdown.dominant_regime),
                vec![*reason],
                format!(
                    "{} {} {} failed with {} across {} candidates.",
                    breakdown.strategy_id,
                    breakdown.symbol,
                    breakdown.timeframe,
                    reason.as_str(),
                    breakdown.candidate_count
                ),
                json!({
                    "campaign_id": attribution.campaign_id,
                    "candidate_count": breakdown.candidate_count,
                    "avg_pnl_pct": breakdown.avg_pnl_pct,
                    "avg_trade_count": breakdown.avg_trade_count,
                    "matching_rows": matching_rows.len()
                }),
                code,
                actions,
                proposed_action,
                config,
                priority,
                expected_effect,
                risk,
                created_at,
            ));
        }
    }
    hypotheses
}

fn hypotheses_from_regime_leaderboard(
    leaderboard: &ResearchRegimeStrategyLeaderboard,
    created_at: DateTime<Utc>,
) -> Vec<ResearchHypothesis> {
    let mut hypotheses = Vec::new();
    for cell in &leaderboard.per_regime {
        let promising = cell
            .rankings
            .iter()
            .any(regime_strategy_ranking_is_promising);
        if !promising {
            let least_bad = cell
                .rankings
                .iter()
                .find(|ranking| regime_strategy_ranking_is_least_bad(ranking));
            hypotheses.push(research_hypothesis(
                ResearchHypothesisSource::RegimeLeaderboard,
                least_bad.map(|ranking| ranking.strategy_id.clone()),
                least_bad.map(|ranking| ranking.symbol.clone()),
                least_bad.map(|ranking| ranking.timeframe.clone()),
                Some(cell.regime_label),
                vec![ResearchCandidateFailureReason::WeakEdge],
                format!("No promising strategy found for {}.", cell.regime_label.as_str()),
                json!({
                    "campaign_id": leaderboard.campaign_id,
                    "least_bad": least_bad,
                    "ranking_count": cell.rankings.len()
                }),
                "define_next_regime_hypothesis",
                vec![
                    "do not promote least-bad strategy",
                    "create explicit regime-specific experiment",
                    "compare against broader validation",
                ],
                "Treat least-bad as diagnostic evidence and design a new regime-specific experiment.",
                json!({
                    "experiment": "regime_specific_hypothesis",
                    "target_regime": cell.regime_label.as_str(),
                    "promote_least_bad": false
                }),
                ResearchHypothesisPriority::Medium,
                "Convert negative regime evidence into a targeted next test.",
                "The next experiment may still find no robust strategy.",
                created_at,
            ));
        }
        if cell
            .rankings
            .iter()
            .any(|ranking| ranking.status == ResearchRegimeStrategyStatus::Overfit)
        {
            hypotheses.push(research_hypothesis(
                ResearchHypothesisSource::RegimeLeaderboard,
                None,
                None,
                None,
                Some(cell.regime_label),
                vec![ResearchCandidateFailureReason::OverfitRisk],
                format!("{} is overfit-heavy.", cell.regime_label.as_str()),
                json!({
                    "campaign_id": leaderboard.campaign_id,
                    "overfit_rankings": cell.rankings.iter().filter(|ranking| ranking.status == ResearchRegimeStrategyStatus::Overfit).count()
                }),
                "tighten_regime_walk_forward",
                vec![
                    "require broader walk-forward",
                    "add more windows/symbols",
                    "reject candidate unless robustness improves",
                ],
                "Require broader validation for this regime before candidate acceptance.",
                json!({
                    "experiment": "regime_walk_forward_expansion",
                    "target_regime": cell.regime_label.as_str(),
                    "min_windows": "increase"
                }),
                ResearchHypothesisPriority::High,
                "Reduce overfit-heavy regime selection.",
                "May block all candidates in this regime.",
                created_at,
            ));
        }
    }
    hypotheses
}

fn hypotheses_from_opportunity_analysis(
    result: &StrategyOpportunityAnalysisResult,
    created_at: DateTime<Utc>,
) -> Vec<ResearchHypothesis> {
    if result.data_quality_status != StrategyOpportunityStatus::HealthyOpportunity {
        return vec![research_hypothesis(
            ResearchHypothesisSource::DataQuality,
            Some(result.strategy_id.clone()),
            Some(result.symbol.clone()),
            Some(result.timeframe.clone()),
            None,
            vec![ResearchCandidateFailureReason::DataQualityDegraded],
            "Opportunity analysis was blocked or degraded by data quality.".to_string(),
            json!({ "data_quality_status": result.data_quality_status }),
            "repair_data_before_research",
            vec![
                "repair data before research",
                "do not accept candidates from degraded windows",
            ],
            "Repair data before using this opportunity analysis.",
            json!({ "require_good_data_quality": true }),
            ResearchHypothesisPriority::High,
            "Prevent degraded windows from driving research acceptance.",
            "Research is delayed until data repair is complete.",
            created_at,
        )];
    }
    Vec::new()
}

fn hypotheses_from_signal_feature_attribution(
    result: &StrategySignalFeatureAttributionResult,
    created_at: DateTime<Utc>,
) -> Vec<ResearchHypothesis> {
    let threshold = 5_i64;
    result
        .best_buckets
        .iter()
        .filter(|bucket| bucket.sample_count >= threshold)
        .map(|bucket| {
            research_hypothesis(
                ResearchHypothesisSource::SignalFeatureAttribution,
                Some(result.strategy_id.clone()),
                Some(result.symbol.clone()),
                Some(result.timeframe.clone()),
                None,
                vec![ResearchCandidateFailureReason::WeakEdge],
                format!(
                    "Feature bucket {} looks promising with {} samples.",
                    bucket.bucket_label, bucket.sample_count
                ),
                json!({
                    "feature": bucket.feature_name,
                    "bucket": bucket.bucket_label,
                    "sample_count": bucket.sample_count,
                    "worst_buckets": result.worst_buckets
                }),
                "use_promising_feature_bucket",
                vec![
                    "create strategy/config variant using promising bucket boundaries",
                    "avoid worst bucket boundaries",
                ],
                "Create a config variant bounded by promising feature buckets.",
                json!({
                    "experiment": "feature_bucket_variant",
                    "feature": bucket.feature_name,
                    "include_bucket": bucket.bucket_label,
                    "avoid_worst_buckets": true
                }),
                ResearchHypothesisPriority::High,
                "Improve entry selectivity using observed feature buckets.",
                "Bucket edge may be sample-specific and needs walk-forward validation.",
                created_at,
            )
        })
        .collect()
}

fn hypotheses_from_exit_attribution(
    result: &StrategyExitAttributionResult,
    created_at: DateTime<Utc>,
) -> Vec<ResearchHypothesis> {
    if result.per_holding_window.is_empty()
        || !result
            .per_holding_window
            .iter()
            .all(|window| window.avg_net_pnl_pct < Decimal::ZERO)
    {
        return Vec::new();
    }
    vec![research_hypothesis(
        ResearchHypothesisSource::ExitAttribution,
        Some(result.strategy_id.clone()),
        Some(result.symbol.clone()),
        Some(result.timeframe.clone()),
        None,
        vec![ResearchCandidateFailureReason::WeakEdge],
        "Exit attribution is negative across all tested holding windows.".to_string(),
        json!({
            "holding_windows": result.per_holding_window,
            "recommendation": result.recommendation
        }),
        "reject_before_exit_tweaks",
        vec![
            "reject strategy/config",
            "test alternative entry logic before exit tweaks",
        ],
        "Reject this config until entry logic improves.",
        json!({
            "experiment": "alternative_entry_logic",
            "exit_tweaks_first": false
        }),
        ResearchHypothesisPriority::High,
        "Avoid optimizing exits for consistently negative entries.",
        "A different exit may still help, but evidence says entry quality is the first problem.",
        created_at,
    )]
}

fn research_hypothesis(
    source_type: ResearchHypothesisSource,
    strategy_id: Option<String>,
    symbol: Option<String>,
    timeframe: Option<String>,
    regime: Option<ResearchRegimeLabel>,
    failure_reasons: Vec<ResearchCandidateFailureReason>,
    evidence_summary: String,
    evidence_details: Value,
    recommendation_code: impl Into<String>,
    actions: Vec<&str>,
    proposed_action: impl Into<String>,
    proposed_experiment_config: Value,
    priority: ResearchHypothesisPriority,
    expected_effect: impl Into<String>,
    risk: impl Into<String>,
    created_at: DateTime<Utc>,
) -> ResearchHypothesis {
    ResearchHypothesis {
        id: None,
        source_type,
        status: ResearchHypothesisStatus::Proposed,
        strategy_id,
        symbol,
        timeframe,
        regime,
        failure_reasons,
        evidence: ResearchHypothesisEvidence {
            summary: evidence_summary,
            details: evidence_details,
        },
        recommendation: ResearchHypothesisRecommendation {
            code: recommendation_code.into(),
            actions: actions.into_iter().map(str::to_string).collect(),
        },
        proposed_action: proposed_action.into(),
        proposed_experiment_config,
        priority,
        expected_effect: expected_effect.into(),
        risk: risk.into(),
        created_at,
    }
}

fn dedupe_and_sort_hypotheses(hypotheses: Vec<ResearchHypothesis>) -> Vec<ResearchHypothesis> {
    let mut by_key = BTreeMap::<String, ResearchHypothesis>::new();
    for hypothesis in hypotheses {
        let key = format!(
            "{}|{}|{}|{}|{}|{}",
            hypothesis.source_type.as_str(),
            hypothesis.strategy_id.as_deref().unwrap_or(""),
            hypothesis.symbol.as_deref().unwrap_or(""),
            hypothesis.timeframe.as_deref().unwrap_or(""),
            hypothesis.regime.map(|value| value.as_str()).unwrap_or(""),
            hypothesis.recommendation.code
        );
        by_key
            .entry(key)
            .and_modify(|existing| {
                if hypothesis.priority.rank() < existing.priority.rank() {
                    *existing = hypothesis.clone();
                }
            })
            .or_insert(hypothesis);
    }
    let mut values = by_key.into_values().collect::<Vec<_>>();
    values.sort_by(|left, right| {
        left.priority
            .rank()
            .cmp(&right.priority.rank())
            .then_with(|| left.source_type.cmp(&right.source_type))
            .then_with(|| left.strategy_id.cmp(&right.strategy_id))
            .then_with(|| left.symbol.cmp(&right.symbol))
            .then_with(|| left.timeframe.cmp(&right.timeframe))
            .then_with(|| left.regime.cmp(&right.regime))
            .then_with(|| left.recommendation.code.cmp(&right.recommendation.code))
    });
    values
}

pub fn plan_research_experiment_from_hypothesis(
    hypothesis: &ResearchHypothesis,
    planned_at: DateTime<Utc>,
    correlation_id: Option<Uuid>,
) -> Result<ResearchExperimentPlan, CoreError> {
    if hypothesis.status != ResearchHypothesisStatus::AcceptedForExperiment {
        return Err(CoreError::ResearchExperimentPlanRequiresAcceptedHypothesis);
    }
    let Some(hypothesis_id) = hypothesis.id else {
        return Err(CoreError::ResearchExperimentPlanRequiresPersistedHypothesis);
    };
    let strategy_id = hypothesis
        .strategy_id
        .clone()
        .unwrap_or_else(|| "operator_selected_strategy".to_string());
    let source_campaign_id = hypothesis
        .evidence
        .details
        .get("campaign_id")
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok());
    let (plan_type, request, steps, recommendation) =
        research_experiment_plan_template(hypothesis, &strategy_id, source_campaign_id);
    let mut plan = ResearchExperimentPlan {
        id: None,
        hypothesis_id,
        source: ResearchExperimentPlanSource::AcceptedHypothesis,
        source_campaign_id,
        strategy_id,
        symbol: hypothesis.symbol.clone(),
        timeframe: hypothesis.timeframe.clone(),
        proposed_request: request,
        plan_type,
        status: ResearchExperimentPlanStatus::Draft,
        validation_status: ResearchExperimentPlanStatus::Draft,
        validation_issues: Vec::new(),
        steps,
        recommendation,
        created_at: planned_at,
        updated_at: planned_at,
        correlation_id,
    };
    let validation = validate_research_experiment_plan(&plan, planned_at);
    plan.validation_status = validation.status;
    plan.validation_issues = validation.issues;
    plan.status = match plan.validation_status {
        ResearchExperimentPlanStatus::Ready => ResearchExperimentPlanStatus::Ready,
        ResearchExperimentPlanStatus::Runnable => ResearchExperimentPlanStatus::Runnable,
        ResearchExperimentPlanStatus::Invalid => ResearchExperimentPlanStatus::Invalid,
        _ => ResearchExperimentPlanStatus::Draft,
    };
    Ok(plan)
}

pub fn validate_research_experiment_plan(
    plan: &ResearchExperimentPlan,
    validated_at: DateTime<Utc>,
) -> ResearchExperimentPlanValidation {
    let mut issues = Vec::new();
    if plan.strategy_id.trim().is_empty() || plan.strategy_id == "operator_selected_strategy" {
        issues.push("missing strategy_id".to_string());
    }
    if plan
        .symbol
        .as_deref()
        .is_none_or(|value| value.trim().is_empty())
    {
        issues.push("missing symbol".to_string());
    }
    if plan
        .timeframe
        .as_deref()
        .is_none_or(|value| value.trim().is_empty())
    {
        issues.push("missing timeframe".to_string());
    } else if let Some(timeframe) = plan.timeframe.as_deref() {
        if timeframe.parse::<CandleInterval>().is_err() {
            issues.push("invalid timeframe".to_string());
        }
    }
    let has_window = plan.source_campaign_id.is_some()
        || plan
            .proposed_request
            .get("source_campaign_id")
            .is_some_and(|value| !value.is_null())
        || plan
            .proposed_request
            .get("regime_dataset_id")
            .is_some_and(|value| !value.is_null())
        || plan
            .proposed_request
            .get("windows")
            .and_then(Value::as_array)
            .is_some_and(|windows| !windows.is_empty())
        || (plan.proposed_request.get("start_time").is_some()
            && plan.proposed_request.get("end_time").is_some());
    if !has_window {
        issues.push("missing research window".to_string());
    }
    if plan.proposed_request.get("research_only") != Some(&Value::Bool(true)) {
        issues.push("proposed_request must be explicitly research_only".to_string());
    }
    if plan
        .proposed_request
        .get("auto_run")
        .is_some_and(|value| value == &Value::Bool(true))
    {
        issues.push("auto_run is not allowed for experiment plans".to_string());
    }
    let status = if issues.is_empty() {
        ResearchExperimentPlanStatus::Ready
    } else {
        ResearchExperimentPlanStatus::Invalid
    };
    ResearchExperimentPlanValidation {
        status,
        issues,
        validated_at: Some(validated_at),
    }
}

fn research_experiment_plan_template(
    hypothesis: &ResearchHypothesis,
    strategy_id: &str,
    source_campaign_id: Option<Uuid>,
) -> (
    ResearchExperimentPlanType,
    Value,
    Vec<ResearchExperimentPlanStep>,
    ResearchExperimentPlanRecommendation,
) {
    let reason_set = hypothesis
        .failure_reasons
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let base = json!({
        "research_only": true,
        "auto_run": false,
        "hypothesis_id": hypothesis.id,
        "source_campaign_id": source_campaign_id,
        "strategy_id": strategy_id,
        "symbol": hypothesis.symbol,
        "timeframe": hypothesis.timeframe,
        "regime": hypothesis.regime.map(|value| value.as_str()),
        "evidence_summary": hypothesis.evidence.summary,
        "hypothesis_recommendation": hypothesis.recommendation,
        "operator_review_required": true
    });
    if reason_set.contains(&ResearchCandidateFailureReason::OverfitRisk) {
        return (
            ResearchExperimentPlanType::RobustnessMatrix,
            merge_json(
                base,
                json!({
                    "plan": "broader_robustness_matrix",
                    "strategy_ids": [strategy_id],
                    "symbols": hypothesis.symbol.iter().collect::<Vec<_>>(),
                    "timeframes": hypothesis.timeframe.iter().collect::<Vec<_>>(),
                    "validation": {
                        "expand_windows": true,
                        "expand_symbols": true,
                        "require_walk_forward_review": true
                    }
                }),
            ),
            plan_steps(&[
                (
                    "define_matrix",
                    "Review broader windows and symbols before running.",
                ),
                (
                    "run_research_only_matrix",
                    "Run robustness matrix only after operator approval.",
                ),
                (
                    "review_overfit",
                    "Reject candidates unless robustness materially improves.",
                ),
            ]),
            plan_recommendation(
                "broaden_validation",
                "Review robustness matrix plan",
                hypothesis,
            ),
        );
    }
    if reason_set.contains(&ResearchCandidateFailureReason::RegimeMismatch) {
        return (
            ResearchExperimentPlanType::ResearchCampaign,
            merge_json(
                base,
                json!({
                    "plan": "regime_filtered_campaign",
                    "strategies": [strategy_id],
                    "symbols": hypothesis.symbol.iter().collect::<Vec<_>>(),
                    "experiment_timeframes": hypothesis.timeframe.iter().collect::<Vec<_>>(),
                    "target_regimes": hypothesis.regime.map(|value| vec![value.as_str()]),
                    "metadata": {
                        "disable_mismatched_regimes": true,
                        "mismatched_regime": hypothesis.regime.map(|value| value.as_str())
                    }
                }),
            ),
            plan_steps(&[
                (
                    "confirm_regime",
                    "Confirm target regime dataset and excluded regimes.",
                ),
                (
                    "run_campaign",
                    "Run a research-only regime-filtered campaign explicitly.",
                ),
                (
                    "review_results",
                    "Compare against unrestricted campaign evidence.",
                ),
            ]),
            plan_recommendation(
                "regime_filter_campaign",
                "Review regime-filtered campaign",
                hypothesis,
            ),
        );
    }
    if reason_set.contains(&ResearchCandidateFailureReason::FeeDrag)
        || reason_set.contains(&ResearchCandidateFailureReason::TooManyTrades)
    {
        return (
            ResearchExperimentPlanType::ResearchCampaign,
            merge_json(
                base,
                json!({
                    "plan": "current_vs_stricter_configs",
                    "adjustments": {
                        "increase_timeframe": true,
                        "cooldown_multiplier": 2,
                        "entry_filter": "tighter",
                        "max_trade_frequency": "below_current_median"
                    },
                    "comparison": ["current_config", "stricter_config"]
                }),
            ),
            plan_steps(&[
                (
                    "baseline",
                    "Keep current config as the baseline comparator.",
                ),
                (
                    "stricter_variant",
                    "Increase timeframe/cooldown and tighten entries.",
                ),
                (
                    "compare",
                    "Compare net PnL, fee drag, trade count, and drawdown.",
                ),
            ]),
            plan_recommendation(
                "reduce_turnover",
                "Review stricter turnover experiment",
                hypothesis,
            ),
        );
    }
    if reason_set.contains(&ResearchCandidateFailureReason::TooFewTrades) {
        return (
            ResearchExperimentPlanType::ResearchBatch,
            merge_json(
                base,
                json!({
                    "plan": "looser_entry_opportunity_test",
                    "precheck": "run opportunity analysis first if sample remains too small",
                    "adjustments": {
                        "loosen_thresholds": true,
                        "expand_entry_band": true,
                        "min_trade_count_gate": "above_too_few_threshold"
                    }
                }),
            ),
            plan_steps(&[
                (
                    "opportunity_check",
                    "Confirm the window has enough candle opportunity.",
                ),
                (
                    "looser_variant",
                    "Loosen thresholds and expand entry bands.",
                ),
                (
                    "sample_gate",
                    "Reject the variant if trade count remains too low.",
                ),
            ]),
            plan_recommendation(
                "increase_sample_size",
                "Review looser-entry experiment",
                hypothesis,
            ),
        );
    }
    if hypothesis.recommendation.code == "use_promising_feature_bucket" {
        return (
            ResearchExperimentPlanType::StrategyExperiment,
            merge_json(
                base,
                json!({
                    "plan": "feature_bucket_config_boundary",
                    "feature_bucket": hypothesis.evidence.details,
                    "bounds": {
                        "include_promising_bucket": true,
                        "avoid_worst_buckets": true
                    }
                }),
            ),
            plan_steps(&[
                (
                    "derive_bounds",
                    "Translate the feature bucket into config boundaries.",
                ),
                (
                    "run_experiment",
                    "Run a strategy experiment explicitly after review.",
                ),
                (
                    "walk_forward",
                    "Require walk-forward validation before candidate creation.",
                ),
            ]),
            plan_recommendation(
                "test_feature_bucket",
                "Review feature-bucket experiment",
                hypothesis,
            ),
        );
    }
    if hypothesis.recommendation.code == "reject_before_exit_tweaks" {
        return (
            ResearchExperimentPlanType::StrategyExperiment,
            merge_json(
                base,
                json!({
                    "plan": "alternative_entry_logic",
                    "exit_only_optimization_allowed": false,
                    "decision": "reject_exit_only_tweak"
                }),
            ),
            plan_steps(&[
                (
                    "reject_exit_only",
                    "Do not optimize exits before entry quality improves.",
                ),
                (
                    "define_entry_variant",
                    "Plan alternative entry logic if research continues.",
                ),
                ("review", "Require operator review before any run."),
            ]),
            plan_recommendation(
                "avoid_exit_only_optimization",
                "Review alternative-entry plan",
                hypothesis,
            ),
        );
    }
    (
        ResearchExperimentPlanType::StrategyExperiment,
        merge_json(
            base,
            json!({
                "plan": "diagnostic_strategy_experiment",
                "diagnostics": ["feature_attribution", "opportunity_analysis"]
            }),
        ),
        plan_steps(&[
            ("diagnose", "Run attribution or opportunity analysis first."),
            (
                "define_variant",
                "Define a bounded strategy variant from the diagnostic evidence.",
            ),
            ("review", "Review before explicitly running research."),
        ]),
        plan_recommendation(
            "diagnose_before_variant",
            "Review diagnostic experiment",
            hypothesis,
        ),
    )
}

fn merge_json(mut base: Value, extra: Value) -> Value {
    if let (Some(base), Some(extra)) = (base.as_object_mut(), extra.as_object()) {
        for (key, value) in extra {
            base.insert(key.clone(), value.clone());
        }
    }
    base
}

fn plan_steps(values: &[(&str, &str)]) -> Vec<ResearchExperimentPlanStep> {
    values
        .iter()
        .enumerate()
        .map(|(index, (code, description))| ResearchExperimentPlanStep {
            step_index: i32::try_from(index + 1).unwrap_or(i32::MAX),
            code: (*code).to_string(),
            description: (*description).to_string(),
            research_only: true,
        })
        .collect()
}

fn plan_recommendation(
    code: &str,
    action: &str,
    hypothesis: &ResearchHypothesis,
) -> ResearchExperimentPlanRecommendation {
    ResearchExperimentPlanRecommendation {
        code: code.to_string(),
        action: action.to_string(),
        rationale: format!(
            "{} Expected effect: {}",
            hypothesis.proposed_action, hypothesis.expected_effect
        ),
    }
}

pub fn campaign_windows(
    request: &ResearchCampaignRequest,
) -> Result<Vec<ResearchCampaignWindow>, CoreError> {
    if !request.windows.is_empty() {
        for window in &request.windows {
            if window.end_time <= window.start_time {
                return Err(CoreError::InvalidResearchCampaignTimeRange);
            }
        }
        return Ok(request.windows.clone());
    }

    let (Some(campaign_start), Some(campaign_end), Some(window_hours), Some(step_hours)) = (
        request.campaign_start,
        request.campaign_end,
        request.window_hours,
        request.step_hours,
    ) else {
        return Err(CoreError::EmptyResearchCampaignWindows);
    };
    if campaign_end <= campaign_start {
        return Err(CoreError::InvalidResearchCampaignTimeRange);
    }
    if window_hours <= 0 || step_hours <= 0 {
        return Err(CoreError::InvalidResearchCampaignWindowStep);
    }

    let mut windows = Vec::new();
    let mut cursor = campaign_start;
    while cursor < campaign_end {
        let end_time = (cursor + Duration::hours(window_hours)).min(campaign_end);
        if end_time <= cursor {
            return Err(CoreError::InvalidResearchCampaignWindowStep);
        }
        windows.push(ResearchCampaignWindow {
            start_time: cursor,
            end_time,
            regime_label: None,
        });
        cursor += Duration::hours(step_hours);
    }
    Ok(windows)
}

pub fn expand_research_campaign(
    request: &ResearchCampaignRequest,
) -> Result<Vec<ResearchCampaignBatchPlan>, CoreError> {
    request.validate()?;
    let windows = campaign_windows(request)?;
    let mut plans = Vec::new();
    for strategy_id in &request.strategies {
        for symbol in &request.symbols {
            for timeframe in &request.experiment_timeframes {
                for window in &windows {
                    plans.push(ResearchCampaignBatchPlan {
                        plan_index: i32::try_from(plans.len() + 1).unwrap_or(i32::MAX),
                        strategy_id: strategy_id.clone(),
                        symbol: symbol.clone(),
                        timeframe: timeframe.clone(),
                        start_time: window.start_time,
                        end_time: window.end_time,
                        regime_label: window.regime_label,
                    });
                    if request
                        .max_batches
                        .is_some_and(|max| plans.len() >= max as usize)
                    {
                        return Ok(plans);
                    }
                }
            }
        }
    }
    Ok(plans)
}

pub fn build_research_regime_dataset(
    dataset_id: Uuid,
    request: ResearchRegimeDatasetRequest,
    candles: &[Candle],
    created_at: DateTime<Utc>,
) -> Result<ResearchRegimeDatasetResult, CoreError> {
    request.validate()?;
    let interval = request.timeframe.parse::<CandleInterval>()?;
    let symbol = Symbol::new(request.symbol.clone())?;
    let classifier_config = request.classifier_config.clone().unwrap_or_default();
    let target_regimes = request.target_regime_set();
    let target_set = target_regimes.iter().copied().collect::<BTreeSet<_>>();
    let mut candidate_windows = Vec::new();
    let mut total_candidate_windows = 0_i32;
    let mut data_quality_blocked_windows = 0_i32;
    let mut insufficient_candle_windows = 0_i32;
    let mut cursor = request.start_time;
    let window_size = Duration::hours(request.window_hours);
    let step_size = Duration::hours(request.step_hours);

    while cursor + window_size <= request.end_time {
        let window_start = cursor;
        let window_end = cursor + window_size;
        total_candidate_windows += 1;
        let window_candles = candles
            .iter()
            .filter(|candle| {
                candle.symbol == symbol
                    && candle.interval == interval
                    && candle.is_closed
                    && candle.open_time >= window_start
                    && candle.close_time <= window_end
            })
            .cloned()
            .collect::<Vec<_>>();
        let quality = summarize_candle_continuity(
            &MarketDataQualityRequest {
                exchange: MarketDataSource::Binance,
                symbol: request.symbol.clone(),
                interval: request.timeframe.clone(),
                start_time: window_start,
                end_time: window_end,
                expected_interval_seconds: Some(interval.duration().num_seconds()),
                max_allowed_gap_count: Some(0),
                max_allowed_gap_pct: Some(Decimal::ZERO),
            },
            &window_candles,
            0,
        )?;
        if window_candles.len() < request.min_candles_per_window as usize {
            insufficient_candle_windows += 1;
            cursor += step_size;
            continue;
        }
        if request.require_good_data_quality && quality.status != MarketDataQualityStatus::Good {
            data_quality_blocked_windows += 1;
            cursor += step_size;
            continue;
        }

        let metric = classify_research_regime_with_config(
            request.symbol.clone(),
            request.timeframe.clone(),
            window_start,
            window_end,
            &window_candles,
            &classifier_config,
        );
        if !target_set.contains(&metric.label) {
            cursor += step_size;
            continue;
        }
        candidate_windows.push(regime_window_from_metric(metric, quality.status));
        cursor += step_size;
    }

    let mut by_regime = BTreeMap::<ResearchRegimeLabel, Vec<ResearchRegimeWindow>>::new();
    for window in candidate_windows {
        by_regime
            .entry(window.regime_label)
            .or_default()
            .push(window);
    }

    let mut selected = Vec::new();
    for regime in &target_regimes {
        if let Some(mut windows) = by_regime.remove(regime) {
            windows.sort_by(|left, right| {
                right
                    .confidence
                    .cmp(&left.confidence)
                    .then_with(|| left.start_time.cmp(&right.start_time))
            });
            if let Some(max_windows) = request.max_windows_per_regime {
                windows.truncate(max_windows as usize);
            }
            selected.extend(windows);
        }
    }
    selected.sort_by(|left, right| {
        left.regime_label
            .cmp(&right.regime_label)
            .then_with(|| right.confidence.cmp(&left.confidence))
            .then_with(|| left.start_time.cmp(&right.start_time))
    });

    let mut regime_counts = BTreeMap::new();
    for window in &selected {
        *regime_counts.entry(window.regime_label).or_insert(0) += 1;
    }
    let missing_regimes = target_regimes
        .iter()
        .copied()
        .filter(|regime| regime_counts.get(regime).copied().unwrap_or(0) == 0)
        .collect::<Vec<_>>();
    let recommendations = regime_dataset_recommendations(
        &missing_regimes,
        data_quality_blocked_windows,
        insufficient_candle_windows,
    );
    let selected_windows = i32::try_from(selected.len()).unwrap_or(i32::MAX);
    let status = if selected_windows == 0 {
        ResearchRegimeDatasetStatus::Failed
    } else if missing_regimes.is_empty() {
        ResearchRegimeDatasetStatus::Completed
    } else {
        ResearchRegimeDatasetStatus::Partial
    };

    Ok(ResearchRegimeDatasetResult {
        dataset_id,
        status,
        request,
        summary: ResearchRegimeDatasetSummary {
            total_candidate_windows,
            selected_windows,
            data_quality_blocked_windows,
            insufficient_candle_windows,
            regime_counts,
            missing_regimes,
            recommendations,
        },
        windows: selected,
        created_at,
    })
}

pub fn run_research_regime_discovery(
    discovery_id: Uuid,
    request: ResearchRegimeDiscoveryRequest,
    candles: &[Candle],
    created_at: DateTime<Utc>,
) -> Result<ResearchRegimeDiscoveryResult, CoreError> {
    request.validate()?;
    let interval = request.timeframe.parse::<CandleInterval>()?;
    let symbol = Symbol::new(request.symbol.clone())?;
    let classifier_config = request.classifier_config.clone().unwrap_or_default();
    let target_regimes = request.target_regime_set();
    let target_set = target_regimes.iter().copied().collect::<BTreeSet<_>>();
    let mut candidates = Vec::new();
    let mut total_windows_scanned = 0_i32;
    let mut data_quality_blocked_count = 0_i32;
    let mut insufficient_data_count = 0_i32;
    let mut cursor = request.scan_start;
    let window_size = Duration::hours(request.window_hours);
    let step_size = Duration::hours(request.step_hours);

    while cursor + window_size <= request.scan_end {
        let window_start = cursor;
        let window_end = cursor + window_size;
        total_windows_scanned += 1;
        let window_candles = candles
            .iter()
            .filter(|candle| {
                candle.symbol == symbol
                    && candle.interval == interval
                    && candle.is_closed
                    && candle.open_time >= window_start
                    && candle.close_time <= window_end
            })
            .cloned()
            .collect::<Vec<_>>();
        let quality = summarize_candle_continuity(
            &MarketDataQualityRequest {
                exchange: MarketDataSource::Binance,
                symbol: request.symbol.clone(),
                interval: request.timeframe.clone(),
                start_time: window_start,
                end_time: window_end,
                expected_interval_seconds: Some(interval.duration().num_seconds()),
                max_allowed_gap_count: Some(0),
                max_allowed_gap_pct: Some(Decimal::ZERO),
            },
            &window_candles,
            0,
        )?;
        if window_candles.len() < REGIME_MIN_CANDLES {
            insufficient_data_count += 1;
            cursor += step_size;
            continue;
        }
        if request.require_existing_candles && quality.status != MarketDataQualityStatus::Good {
            data_quality_blocked_count += 1;
            cursor += step_size;
            continue;
        }

        let metric = classify_research_regime_with_config(
            request.symbol.clone(),
            request.timeframe.clone(),
            window_start,
            window_end,
            &window_candles,
            &classifier_config,
        );
        if !target_set.contains(&metric.label) {
            cursor += step_size;
            continue;
        }
        let window = regime_window_from_metric(metric, quality.status);
        if request
            .min_confidence
            .is_some_and(|min_confidence| window.confidence < min_confidence)
        {
            cursor += step_size;
            continue;
        }
        candidates.push(discovery_candidate_from_window(window));
        cursor += step_size;
    }

    let mut by_regime =
        BTreeMap::<ResearchRegimeLabel, Vec<ResearchRegimeDiscoveryCandidateWindow>>::new();
    for window in candidates {
        by_regime
            .entry(window.regime_label)
            .or_default()
            .push(window);
    }

    let mut selected_windows = Vec::new();
    for regime in &target_regimes {
        if let Some(mut windows) = by_regime.remove(regime) {
            windows.sort_by(|left, right| {
                right
                    .confidence
                    .cmp(&left.confidence)
                    .then_with(|| left.start_time.cmp(&right.start_time))
                    .then_with(|| left.id.cmp(&right.id))
            });
            let mut selected_for_regime = Vec::new();
            for window in windows {
                if selected_for_regime.iter().any(
                    |selected: &ResearchRegimeDiscoveryCandidateWindow| {
                        windows_overlap(
                            selected.start_time,
                            selected.end_time,
                            window.start_time,
                            window.end_time,
                        )
                    },
                ) {
                    continue;
                }
                selected_for_regime.push(window);
                if selected_for_regime.len() >= request.max_windows_per_regime as usize {
                    break;
                }
            }
            selected_windows.extend(selected_for_regime);
        }
    }
    selected_windows.sort_by(|left, right| {
        left.regime_label
            .cmp(&right.regime_label)
            .then_with(|| right.confidence.cmp(&left.confidence))
            .then_with(|| left.start_time.cmp(&right.start_time))
            .then_with(|| left.id.cmp(&right.id))
    });

    let mut counts_by_regime = BTreeMap::new();
    for window in &selected_windows {
        *counts_by_regime.entry(window.regime_label).or_insert(0) += 1;
    }
    let missing_regimes = target_regimes
        .iter()
        .copied()
        .filter(|regime| counts_by_regime.get(regime).copied().unwrap_or(0) == 0)
        .collect::<Vec<_>>();
    let recommendations = regime_discovery_recommendations(
        &missing_regimes,
        data_quality_blocked_count,
        insufficient_data_count,
        request.auto_backfill_missing,
        !selected_windows.is_empty() && missing_regimes.is_empty(),
    );
    let selected_window_count = i32::try_from(selected_windows.len()).unwrap_or(i32::MAX);
    let status = if total_windows_scanned == 0 || selected_window_count == 0 {
        ResearchRegimeDiscoveryStatus::InsufficientData
    } else if missing_regimes.is_empty() {
        ResearchRegimeDiscoveryStatus::Completed
    } else {
        ResearchRegimeDiscoveryStatus::Partial
    };
    let summary = ResearchRegimeDiscoverySummary {
        total_windows_scanned,
        selected_window_count,
        counts_by_regime: counts_by_regime.clone(),
        missing_regimes: missing_regimes.clone(),
        data_quality_blocked_count,
        insufficient_data_count,
        recommendations: recommendations.clone(),
    };

    Ok(ResearchRegimeDiscoveryResult {
        discovery_id,
        status,
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        scan_start: request.scan_start,
        scan_end: request.scan_end,
        total_windows_scanned,
        selected_windows,
        counts_by_regime,
        missing_regimes,
        data_quality_blocked_count,
        recommendations,
        request,
        summary,
        created_at,
    })
}

pub fn build_research_regime_dataset_from_discovery(
    dataset_id: Uuid,
    discovery: &ResearchRegimeDiscoveryResult,
    request: ResearchRegimeDatasetFromDiscoveryRequest,
    created_at: DateTime<Utc>,
) -> Result<ResearchRegimeDatasetResult, CoreError> {
    let mut windows = discovery.selected_windows.clone();
    if let Some(target_regimes) = request.target_regimes.as_ref() {
        windows.retain(|window| target_regimes.contains(&window.regime_label));
    }
    if let Some(max_windows_per_regime) = request.max_windows_per_regime {
        let mut counts = BTreeMap::<ResearchRegimeLabel, u32>::new();
        windows.retain(|window| {
            let count = counts.entry(window.regime_label).or_insert(0);
            if *count >= max_windows_per_regime {
                false
            } else {
                *count += 1;
                true
            }
        });
    }
    let target_regimes = request
        .target_regimes
        .clone()
        .unwrap_or_else(|| discovery.request.target_regime_set());
    let selected = windows
        .into_iter()
        .map(|window| ResearchRegimeWindow {
            id: Uuid::new_v4(),
            symbol: discovery.symbol.clone(),
            timeframe: discovery.timeframe.clone(),
            start_time: window.start_time,
            end_time: window.end_time,
            regime_label: window.regime_label,
            return_pct: window.return_pct,
            realized_volatility: window.realized_volatility,
            avg_range_pct: window.avg_range_pct,
            trend_slope: window.trend_slope,
            choppiness_proxy: window.choppiness_proxy,
            data_quality_status: window.data_quality_status,
            candle_count: window.candle_count,
            score: window.confidence,
            confidence: window.confidence,
            metrics: Vec::new(),
            explanation: window.explanation,
        })
        .collect::<Vec<_>>();
    let mut regime_counts = BTreeMap::new();
    for window in &selected {
        *regime_counts.entry(window.regime_label).or_insert(0) += 1;
    }
    let missing_regimes = target_regimes
        .iter()
        .copied()
        .filter(|regime| regime_counts.get(regime).copied().unwrap_or(0) == 0)
        .collect::<Vec<_>>();
    let selected_windows = i32::try_from(selected.len()).unwrap_or(i32::MAX);
    let status = if selected_windows == 0 {
        ResearchRegimeDatasetStatus::Failed
    } else if missing_regimes.is_empty() {
        ResearchRegimeDatasetStatus::Completed
    } else {
        ResearchRegimeDatasetStatus::Partial
    };

    Ok(ResearchRegimeDatasetResult {
        dataset_id,
        status,
        request: ResearchRegimeDatasetRequest {
            symbol: discovery.symbol.clone(),
            timeframe: discovery.timeframe.clone(),
            start_time: discovery.scan_start,
            end_time: discovery.scan_end,
            window_hours: discovery.request.window_hours,
            step_hours: discovery.request.step_hours,
            min_candles_per_window: REGIME_MIN_CANDLES as i32,
            target_regimes: Some(target_regimes),
            max_windows_per_regime: request.max_windows_per_regime,
            require_good_data_quality: discovery.request.require_existing_candles,
            classifier_config: discovery.request.classifier_config.clone(),
        },
        summary: ResearchRegimeDatasetSummary {
            total_candidate_windows: discovery.total_windows_scanned,
            selected_windows,
            data_quality_blocked_windows: discovery.data_quality_blocked_count,
            insufficient_candle_windows: discovery.summary.insufficient_data_count,
            regime_counts,
            missing_regimes: missing_regimes.clone(),
            recommendations: regime_dataset_recommendations(
                &missing_regimes,
                discovery.data_quality_blocked_count,
                discovery.summary.insufficient_data_count,
            ),
        },
        windows: selected,
        created_at,
    })
}

pub fn run_research_regime_calibration(
    calibration_id: Uuid,
    request: ResearchRegimeCalibrationRequest,
    candles: &[Candle],
    created_at: DateTime<Utc>,
) -> Result<ResearchRegimeCalibrationResult, CoreError> {
    request.validate()?;
    let interval = request.timeframe.parse::<CandleInterval>()?;
    let symbol = Symbol::new(request.symbol.clone())?;
    let threshold_candidates = request
        .threshold_candidates
        .clone()
        .unwrap_or_else(default_research_regime_threshold_candidates);
    let target_regimes = default_research_regime_priority_order();
    let mut candidate_results = Vec::new();

    for candidate in threshold_candidates {
        let mut counts_by_regime = BTreeMap::<ResearchRegimeLabel, i32>::new();
        let mut confidence_sum = Decimal::ZERO;
        let mut classified_count = 0_i32;
        let mut total_windows_scanned = 0_i32;
        let mut data_quality_good_windows = 0_i32;
        let mut samples = Vec::new();
        let mut cursor = request.scan_start;
        let window_size = Duration::hours(request.window_hours);
        let step_size = Duration::hours(request.step_hours);

        while cursor + window_size <= request.scan_end {
            let window_start = cursor;
            let window_end = cursor + window_size;
            total_windows_scanned += 1;
            let window_candles = candles
                .iter()
                .filter(|candle| {
                    candle.symbol == symbol
                        && candle.interval == interval
                        && candle.is_closed
                        && candle.open_time >= window_start
                        && candle.close_time <= window_end
                })
                .cloned()
                .collect::<Vec<_>>();
            let quality = summarize_candle_continuity(
                &MarketDataQualityRequest {
                    exchange: MarketDataSource::Binance,
                    symbol: request.symbol.clone(),
                    interval: request.timeframe.clone(),
                    start_time: window_start,
                    end_time: window_end,
                    expected_interval_seconds: Some(interval.duration().num_seconds()),
                    max_allowed_gap_count: Some(0),
                    max_allowed_gap_pct: Some(Decimal::ZERO),
                },
                &window_candles,
                0,
            )?;
            if quality.status == MarketDataQualityStatus::Good {
                data_quality_good_windows += 1;
            }
            if window_candles.len() >= REGIME_MIN_CANDLES {
                let metric = classify_research_regime_with_config(
                    request.symbol.clone(),
                    request.timeframe.clone(),
                    window_start,
                    window_end,
                    &window_candles,
                    &candidate.classifier_config,
                );
                if target_regimes.contains(&metric.label) {
                    *counts_by_regime.entry(metric.label).or_insert(0) += 1;
                    confidence_sum += metric.confidence;
                    classified_count += 1;
                    if samples.len() < 10
                        && !samples.iter().any(
                            |sample: &ResearchRegimeClassificationExplanation| {
                                sample.final_label == metric.label
                            },
                        )
                    {
                        samples.push(metric.explanation.clone());
                    }
                }
            }
            cursor += step_size;
        }

        let missing_regimes = target_regimes
            .iter()
            .copied()
            .filter(|regime| counts_by_regime.get(regime).copied().unwrap_or(0) == 0)
            .collect::<Vec<_>>();
        let represented = target_regimes
            .iter()
            .filter(|regime| counts_by_regime.get(regime).copied().unwrap_or(0) > 0)
            .count();
        let met_target = target_regimes
            .iter()
            .filter(|regime| {
                counts_by_regime.get(regime).copied().unwrap_or(0)
                    >= request.target_min_windows_per_regime as i32
            })
            .count();
        let max_count = counts_by_regime.values().copied().max().unwrap_or(0);
        let total_selected: i32 = counts_by_regime.values().copied().sum();
        let dominant_regime_share = if total_selected > 0 {
            pct_ratio(Decimal::from(max_count), Decimal::from(total_selected)).round_dp(4)
        } else {
            Decimal::ZERO
        };
        let avg_confidence = if classified_count > 0 {
            (confidence_sum / Decimal::from(classified_count)).round_dp(4)
        } else {
            Decimal::ZERO
        };
        let diversity_score = pct_ratio(
            Decimal::from(met_target as i64),
            Decimal::from(target_regimes.len() as i64),
        )
        .round_dp(4);
        let balance_score = (Decimal::new(100, 0) - dominant_regime_share)
            .max(Decimal::ZERO)
            .round_dp(4);
        let data_quality_score = if total_windows_scanned > 0 {
            pct_ratio(
                Decimal::from(data_quality_good_windows),
                Decimal::from(total_windows_scanned),
            )
        } else {
            Decimal::ZERO
        };
        let total_score = (diversity_score * Decimal::new(45, 2)
            + balance_score * Decimal::new(25, 2)
            + avg_confidence * Decimal::new(20, 2)
            + data_quality_score * Decimal::new(10, 2))
        .round_dp(4);
        let mut warnings = Vec::new();
        if represented <= 1 {
            warnings.push("range_only_or_single_regime".to_string());
        }
        if dominant_regime_share >= Decimal::new(80, 0) {
            warnings.push("dominant_regime_share_above_80pct".to_string());
        }
        if !missing_regimes.is_empty() {
            warnings.push(format!(
                "missing_regimes={}",
                missing_regimes
                    .iter()
                    .map(|regime| regime.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }

        candidate_results.push(ResearchRegimeCalibrationCandidateResult {
            candidate_id: candidate.candidate_id,
            classifier_config: candidate.classifier_config,
            counts_by_regime,
            missing_regimes,
            total_windows_scanned,
            data_quality_good_windows,
            avg_confidence,
            diversity_score,
            balance_score,
            dominant_regime_share,
            total_score,
            warnings,
            explanation_samples: samples,
        });
    }

    candidate_results.sort_by(|left, right| {
        right
            .total_score
            .cmp(&left.total_score)
            .then_with(|| right.diversity_score.cmp(&left.diversity_score))
            .then_with(|| left.dominant_regime_share.cmp(&right.dominant_regime_share))
            .then_with(|| left.candidate_id.cmp(&right.candidate_id))
    });
    let recommended = candidate_results.first();
    let recommended_config = recommended.map(|candidate| candidate.classifier_config.clone());
    let recommended_candidate_id = recommended.map(|candidate| candidate.candidate_id.clone());
    let missing_regimes = recommended
        .map(|candidate| candidate.missing_regimes.clone())
        .unwrap_or_else(|| target_regimes.clone());
    let status = if candidate_results.is_empty() {
        ResearchRegimeCalibrationStatus::Failed
    } else if recommended.is_some_and(|candidate| candidate.total_windows_scanned == 0) {
        ResearchRegimeCalibrationStatus::InsufficientData
    } else if missing_regimes.is_empty() {
        ResearchRegimeCalibrationStatus::Completed
    } else {
        ResearchRegimeCalibrationStatus::Partial
    };

    Ok(ResearchRegimeCalibrationResult {
        calibration_id,
        status,
        request,
        candidates: candidate_results,
        recommended_config,
        recommended_candidate_id,
        missing_regimes: missing_regimes.clone(),
        recommendations: regime_calibration_recommendations(&missing_regimes),
        created_at,
    })
}

fn default_research_regime_threshold_candidates() -> Vec<ResearchRegimeThresholdCandidate> {
    let mut candidates = Vec::new();
    let base = ResearchRegimeClassifierConfig::default();
    candidates.push(ResearchRegimeThresholdCandidate {
        candidate_id: "default".to_string(),
        classifier_config: base.clone(),
    });
    for (id, trend, range, chop, high, low) in [
        ("crypto_vol_balanced", "1.0", "0.8", "70", "0.45", "0.18"),
        ("crypto_all_sensitive", "1.2", "0.6", "75", "0.35", "0.15"),
        ("balanced_1", "1.5", "1.0", "55", "2.5", "0.4"),
        ("balanced_2", "2.0", "1.5", "60", "3.5", "0.6"),
        ("trend_sensitive", "1.0", "0.8", "65", "4.0", "0.5"),
        ("vol_sensitive", "2.0", "1.0", "60", "2.0", "0.5"),
        ("range_sensitive", "2.5", "2.0", "50", "4.0", "0.8"),
    ] {
        candidates.push(ResearchRegimeThresholdCandidate {
            candidate_id: id.to_string(),
            classifier_config: ResearchRegimeClassifierConfig {
                trend_return_threshold_pct: trend
                    .parse()
                    .unwrap_or(base.trend_return_threshold_pct),
                trend_slope_threshold: Decimal::ZERO,
                range_return_max_pct: range.parse().unwrap_or(base.range_return_max_pct),
                range_choppiness_min: chop.parse().unwrap_or(base.range_choppiness_min),
                high_volatility_threshold_pct: high
                    .parse()
                    .unwrap_or(base.high_volatility_threshold_pct),
                low_volatility_threshold_pct: low
                    .parse()
                    .unwrap_or(base.low_volatility_threshold_pct),
                min_confidence: Decimal::ZERO,
                priority_order: if id == "crypto_all_sensitive" {
                    vec![
                        ResearchRegimeLabel::HighVolatility,
                        ResearchRegimeLabel::LowVolatility,
                        ResearchRegimeLabel::TrendUp,
                        ResearchRegimeLabel::TrendDown,
                        ResearchRegimeLabel::Range,
                    ]
                } else if id == "crypto_vol_balanced" {
                    vec![
                        ResearchRegimeLabel::HighVolatility,
                        ResearchRegimeLabel::TrendUp,
                        ResearchRegimeLabel::TrendDown,
                        ResearchRegimeLabel::LowVolatility,
                        ResearchRegimeLabel::Range,
                    ]
                } else {
                    base.priority_order.clone()
                },
            },
        });
    }
    candidates
}

fn regime_calibration_recommendations(
    missing_regimes: &[ResearchRegimeLabel],
) -> Vec<ResearchRegimeCalibrationRecommendation> {
    let mut recommendations = Vec::new();
    if !missing_regimes.is_empty() {
        recommendations.push(ResearchRegimeCalibrationRecommendation {
            priority: "MEDIUM".to_string(),
            code: "broaden_history_or_symbols".to_string(),
            message:
                "Recommended thresholds still miss regimes; scan more history or add symbols before trusting balanced campaigns."
                    .to_string(),
        });
    }
    recommendations.push(ResearchRegimeCalibrationRecommendation {
        priority: "LOW".to_string(),
        code: "research_only".to_string(),
        message:
            "Calibration is research-only and must not submit orders, create paper state, or auto-promote candidates."
                .to_string(),
    });
    recommendations
}

fn regime_window_from_metric(
    metric: ResearchRegimeMetric,
    data_quality_status: MarketDataQualityStatus,
) -> ResearchRegimeWindow {
    let trend_slope = metric.trend_slope;
    let confidence = metric.confidence;
    ResearchRegimeWindow {
        id: Uuid::new_v4(),
        symbol: metric.symbol.clone(),
        timeframe: metric.timeframe.clone(),
        start_time: metric.window_start,
        end_time: metric.window_end,
        regime_label: metric.label,
        return_pct: metric.return_pct,
        realized_volatility: metric.realized_volatility,
        avg_range_pct: metric.average_candle_range_pct,
        trend_slope,
        choppiness_proxy: metric.choppiness_pct,
        data_quality_status,
        candle_count: metric.candle_count,
        score: confidence,
        confidence,
        metrics: vec![
            ResearchRegimeWindowMetric {
                name: "return_pct".to_string(),
                value: metric.return_pct,
                threshold: Some(
                    metric
                        .explanation
                        .thresholds_used
                        .trend_return_threshold_pct,
                ),
                passed: metric.return_pct.abs()
                    >= metric
                        .explanation
                        .thresholds_used
                        .trend_return_threshold_pct,
            },
            ResearchRegimeWindowMetric {
                name: "realized_volatility".to_string(),
                value: metric.realized_volatility,
                threshold: Some(
                    metric
                        .explanation
                        .thresholds_used
                        .high_volatility_threshold_pct,
                ),
                passed: metric.realized_volatility
                    >= metric
                        .explanation
                        .thresholds_used
                        .high_volatility_threshold_pct,
            },
            ResearchRegimeWindowMetric {
                name: "trend_slope".to_string(),
                value: metric.trend_slope,
                threshold: Some(metric.explanation.thresholds_used.trend_slope_threshold),
                passed: metric.trend_slope.abs()
                    >= metric.explanation.thresholds_used.trend_slope_threshold,
            },
            ResearchRegimeWindowMetric {
                name: "choppiness_proxy".to_string(),
                value: metric.choppiness_pct,
                threshold: Some(metric.explanation.thresholds_used.range_choppiness_min),
                passed: metric.choppiness_pct
                    >= metric.explanation.thresholds_used.range_choppiness_min,
            },
        ],
        explanation: metric.explanation,
    }
}

fn discovery_candidate_from_window(
    window: ResearchRegimeWindow,
) -> ResearchRegimeDiscoveryCandidateWindow {
    ResearchRegimeDiscoveryCandidateWindow {
        id: window.id,
        regime_label: window.regime_label,
        start_time: window.start_time,
        end_time: window.end_time,
        confidence: window.confidence,
        return_pct: window.return_pct,
        realized_volatility: window.realized_volatility,
        avg_range_pct: window.avg_range_pct,
        trend_slope: window.trend_slope,
        choppiness_proxy: window.choppiness_proxy,
        data_quality_status: window.data_quality_status,
        candle_count: window.candle_count,
        explanation: window.explanation,
    }
}

fn windows_overlap(
    left_start: DateTime<Utc>,
    left_end: DateTime<Utc>,
    right_start: DateTime<Utc>,
    right_end: DateTime<Utc>,
) -> bool {
    left_start < right_end && right_start < left_end
}

fn regime_confidence(metric: &ResearchRegimeMetric) -> Decimal {
    let hundred = Decimal::new(100, 0);
    match metric.label {
        ResearchRegimeLabel::HighVolatility => (metric.realized_volatility * Decimal::new(10, 0))
            .max(metric.average_candle_range_pct * Decimal::new(10, 0))
            .min(hundred),
        ResearchRegimeLabel::TrendUp | ResearchRegimeLabel::TrendDown => (metric.return_pct.abs()
            * Decimal::new(12, 0)
            + metric.directional_movement_pct / Decimal::new(2, 0))
        .min(hundred),
        ResearchRegimeLabel::Range => (metric.choppiness_pct
            + (Decimal::new(5, 0) - metric.return_pct.abs()).max(Decimal::ZERO)
                * Decimal::new(5, 0))
        .min(hundred),
        ResearchRegimeLabel::LowVolatility => {
            (hundred - (metric.realized_volatility * Decimal::new(20, 0))).max(Decimal::ZERO)
        }
        ResearchRegimeLabel::Mixed => Decimal::new(50, 0),
        ResearchRegimeLabel::Unknown => Decimal::ZERO,
    }
}

fn regime_discovery_recommendations(
    missing_regimes: &[ResearchRegimeLabel],
    data_quality_blocked_count: i32,
    insufficient_data_count: i32,
    auto_backfill_missing: bool,
    balanced: bool,
) -> Vec<ResearchRegimeDiscoveryRecommendation> {
    let mut recommendations = Vec::new();
    if missing_regimes.contains(&ResearchRegimeLabel::TrendUp)
        || missing_regimes.contains(&ResearchRegimeLabel::TrendDown)
    {
        recommendations.push(ResearchRegimeDiscoveryRecommendation {
            priority: "MEDIUM".to_string(),
            code: "missing_trend_regimes".to_string(),
            message: "Missing trend regimes; scan a wider range, reduce confidence threshold, or add symbols."
                .to_string(),
        });
    }
    if missing_regimes.contains(&ResearchRegimeLabel::HighVolatility) {
        recommendations.push(ResearchRegimeDiscoveryRecommendation {
            priority: "MEDIUM".to_string(),
            code: "missing_high_volatility_regimes".to_string(),
            message: "Missing high-volatility regimes; include known stress windows before judging strategy families."
                .to_string(),
        });
    }
    if data_quality_blocked_count > 0 || insufficient_data_count > 0 {
        recommendations.push(ResearchRegimeDiscoveryRecommendation {
            priority: "LOW".to_string(),
            code: "repair_or_backfill_market_data".to_string(),
            message: "Some windows lacked complete existing candles; repair or public-backfill market data before rerunning discovery."
                .to_string(),
        });
    }
    if auto_backfill_missing {
        recommendations.push(ResearchRegimeDiscoveryRecommendation {
            priority: "LOW".to_string(),
            code: "auto_backfill_requested".to_string(),
            message: "Auto-backfill was requested; API handlers must use only public market-data backfill paths."
                .to_string(),
        });
    }
    if balanced {
        recommendations.push(ResearchRegimeDiscoveryRecommendation {
            priority: "LOW".to_string(),
            code: "balanced_candidates_found".to_string(),
            message:
                "Regime discovery found balanced dataset candidates for research-only campaigns."
                    .to_string(),
        });
    }
    recommendations.push(ResearchRegimeDiscoveryRecommendation {
        priority: "LOW".to_string(),
        code: "research_only".to_string(),
        message: "Discovery is research-only and must not submit orders, mutate execution state, or auto-promote candidates."
            .to_string(),
    });
    recommendations
}

fn regime_dataset_recommendations(
    missing_regimes: &[ResearchRegimeLabel],
    data_quality_blocked_windows: i32,
    insufficient_candle_windows: i32,
) -> Vec<ResearchRegimeDatasetRecommendation> {
    let mut recommendations = Vec::new();
    if missing_regimes.contains(&ResearchRegimeLabel::TrendUp)
        || missing_regimes.contains(&ResearchRegimeLabel::TrendDown)
    {
        recommendations.push(ResearchRegimeDatasetRecommendation {
            priority: "MEDIUM".to_string(),
            code: "expand_trend_history".to_string(),
            message:
                "Research dataset lacks trend regimes; extend the historical window or add symbols."
                    .to_string(),
        });
    }
    if missing_regimes.contains(&ResearchRegimeLabel::HighVolatility) {
        recommendations.push(ResearchRegimeDatasetRecommendation {
            priority: "MEDIUM".to_string(),
            code: "expand_high_volatility_history".to_string(),
            message:
                "Research dataset lacks high-volatility regimes; include known stress windows."
                    .to_string(),
        });
    }
    if data_quality_blocked_windows > 0 || insufficient_candle_windows > 0 {
        recommendations.push(ResearchRegimeDatasetRecommendation {
            priority: "LOW".to_string(),
            code: "repair_market_data".to_string(),
            message: "Repair or backfill candle gaps before judging missing regimes.".to_string(),
        });
    }
    recommendations.push(ResearchRegimeDatasetRecommendation {
        priority: "LOW".to_string(),
        code: "research_only".to_string(),
        message: "Regime datasets are research-only and must not auto-promote candidates or submit orders."
            .to_string(),
    });
    recommendations
}

pub fn summarize_research_campaign(
    planned_count: usize,
    batches: &[ResearchCampaignBatchResult],
) -> ResearchCampaignSummary {
    let total_batches_completed = batches
        .iter()
        .filter(|batch| {
            batch.error.is_none() && batch.batch_status != Some(ResearchBatchStatus::Failed)
        })
        .count() as i32;
    let total_batches_failed = batches
        .iter()
        .filter(|batch| {
            batch.error.is_some()
                || batch.batch_status == Some(ResearchBatchStatus::Failed)
                || batch.triage_status == ResearchBatchTriageStatus::Failed
        })
        .count() as i32;
    let actionable_batches = batches
        .iter()
        .filter(|batch| batch.triage_status == ResearchBatchTriageStatus::Actionable)
        .count() as i32;
    let overfit_only_batches = batches
        .iter()
        .filter(|batch| batch.triage_status == ResearchBatchTriageStatus::OverfitOnly)
        .count() as i32;
    let weak_batches = batches
        .iter()
        .filter(|batch| batch.triage_status == ResearchBatchTriageStatus::Weak)
        .count() as i32;
    let data_quality_blocked_batches = batches
        .iter()
        .filter(|batch| batch.triage_status == ResearchBatchTriageStatus::DataQualityBlocked)
        .count() as i32;
    let no_candidate_batches = batches
        .iter()
        .filter(|batch| batch.triage_status == ResearchBatchTriageStatus::NoCandidates)
        .count() as i32;
    let candidates_created = batches.iter().map(|batch| batch.candidates_created).sum();
    let total_candidate_configs = batches
        .iter()
        .map(|batch| batch.total_candidate_configs)
        .sum();
    let skipped_invalid_config_count = batches
        .iter()
        .map(|batch| batch.skipped_invalid_config_count)
        .sum();
    let executed_config_count = batches
        .iter()
        .map(|batch| batch.executed_config_count)
        .sum();
    let invalid_config_examples = batches
        .iter()
        .flat_map(|batch| batch.invalid_config_examples.clone())
        .take(10)
        .collect::<Vec<_>>();
    let candidates_blocked_by_gate = batches
        .iter()
        .map(|batch| batch.candidates_blocked_by_gate)
        .sum();
    let proposals_created = batches.iter().map(|batch| batch.proposals_created).sum();
    let top_candidates = ranked_research_batch_candidates(
        batches
            .iter()
            .flat_map(|batch| batch.top_candidates.clone())
            .collect(),
    )
    .into_iter()
    .take(10)
    .collect::<Vec<_>>();
    let best_strategy_symbol_timeframe = top_candidates.first().map(|candidate| {
        format!(
            "{}:{}:{}",
            candidate.strategy_id, candidate.symbol, candidate.timeframe
        )
    });
    let per_regime_performance = summarize_research_campaign_regimes(batches);

    let mut findings = Vec::new();
    let mut recommendations = Vec::new();
    if candidates_created > 0 {
        findings.push(campaign_finding(
            "LOW",
            "research_campaign_candidates_for_review",
            "Campaign produced candidates for review.",
        ));
        recommendations.push(campaign_recommendation(
            "LOW",
            "manual_candidate_review",
            "Review candidate evidence manually; campaign runner does not promote candidates.",
        ));
    }
    if actionable_batches == 0 && planned_count > 0 {
        findings.push(campaign_finding(
            "LOW",
            "research_campaign_no_actionable_output",
            "No actionable campaign output.",
        ));
        recommendations.push(campaign_recommendation(
            "LOW",
            "expand_or_refine_campaign",
            "Expand windows or refine deterministic strategy parameters before review.",
        ));
    }
    if candidates_blocked_by_gate > 0 {
        findings.push(campaign_finding(
            "LOW",
            "candidate_creation_gate_prevented_weak_candidates",
            "Candidate creation gate prevented weak candidates from being registered.",
        ));
        recommendations.push(campaign_recommendation(
            "LOW",
            "review_gate_reasons",
            "Review gate decisions and proposals before explicitly registering any candidate.",
        ));
    }
    if skipped_invalid_config_count > 0 {
        findings.push(campaign_finding(
            "LOW",
            "invalid_strategy_configs_skipped",
            "Invalid strategy configurations were skipped before replay.",
        ));
        recommendations.push(campaign_recommendation(
            "LOW",
            "review_invalid_strategy_grid",
            "Review invalid_config_examples and tighten the strategy grid before rerunning.",
        ));
    }
    if proposals_created >= 100 {
        findings.push(campaign_finding(
            "MEDIUM",
            "campaign_many_blocked_candidate_proposals",
            "Campaign produced many blocked candidate proposals.",
        ));
        recommendations.push(campaign_recommendation(
            "MEDIUM",
            "tighten_research_sweep",
            "Tighten sweep ranges or evidence thresholds before rerunning broad campaigns.",
        ));
    }
    if overfit_only_batches > 0 && actionable_batches == 0 {
        findings.push(campaign_finding(
            "MEDIUM",
            "research_campaign_only_overfit_candidates",
            "Campaign produced only overfit candidates.",
        ));
        recommendations.push(campaign_recommendation(
            "MEDIUM",
            "reject_overfit_campaign_output",
            "Do not promote overfit-only output without stronger out-of-sample evidence.",
        ));
    }
    if total_batches_failed > 0 {
        findings.push(campaign_finding(
            "MEDIUM",
            "research_campaign_failed_batches",
            "Campaign had failed batches.",
        ));
        recommendations.push(campaign_recommendation(
            "MEDIUM",
            "inspect_failed_campaign_batches",
            "Inspect failed batch rows and rerun after fixing data or configuration issues.",
        ));
    }

    ResearchCampaignSummary {
        total_batches_planned: i32::try_from(planned_count).unwrap_or(i32::MAX),
        total_batches_completed,
        total_batches_failed,
        actionable_batches,
        overfit_only_batches,
        weak_batches,
        data_quality_blocked_batches,
        no_candidate_batches,
        candidates_created,
        total_candidate_configs,
        skipped_invalid_config_count,
        executed_config_count,
        invalid_config_examples,
        candidates_blocked_by_gate,
        proposals_created,
        top_candidates,
        best_strategy_symbol_timeframe,
        per_regime_performance,
        findings,
        recommendations,
    }
}

fn summarize_research_campaign_regimes(
    batches: &[ResearchCampaignBatchResult],
) -> Vec<ResearchCampaignRegimePerformance> {
    let mut by_regime = BTreeMap::<ResearchRegimeLabel, ResearchCampaignRegimePerformance>::new();
    for batch in batches {
        let Some(regime_label) = batch.plan.regime_label else {
            continue;
        };
        let entry =
            by_regime
                .entry(regime_label)
                .or_insert_with(|| ResearchCampaignRegimePerformance {
                    regime_label,
                    planned_batches: 0,
                    completed_batches: 0,
                    failed_batches: 0,
                    actionable_batches: 0,
                    weak_batches: 0,
                    candidates_created: 0,
                    total_candidate_configs: 0,
                    skipped_invalid_config_count: 0,
                    executed_config_count: 0,
                    candidates_blocked_by_gate: 0,
                    proposals_created: 0,
                });
        entry.planned_batches += 1;
        if batch.error.is_none() && batch.batch_status != Some(ResearchBatchStatus::Failed) {
            entry.completed_batches += 1;
        }
        if batch.error.is_some()
            || batch.batch_status == Some(ResearchBatchStatus::Failed)
            || batch.triage_status == ResearchBatchTriageStatus::Failed
        {
            entry.failed_batches += 1;
        }
        if batch.triage_status == ResearchBatchTriageStatus::Actionable {
            entry.actionable_batches += 1;
        }
        if batch.triage_status == ResearchBatchTriageStatus::Weak {
            entry.weak_batches += 1;
        }
        entry.candidates_created += batch.candidates_created;
        entry.total_candidate_configs += batch.total_candidate_configs;
        entry.skipped_invalid_config_count += batch.skipped_invalid_config_count;
        entry.executed_config_count += batch.executed_config_count;
        entry.candidates_blocked_by_gate += batch.candidates_blocked_by_gate;
        entry.proposals_created += batch.proposals_created;
    }
    by_regime.into_values().collect()
}

#[derive(Debug, Clone)]
struct RegimeStrategyGroup {
    strategy_id: String,
    symbol: String,
    timeframe: String,
    batch_indices: BTreeSet<i32>,
    pnl_values: Vec<Decimal>,
    trade_counts: Vec<i32>,
    walk_forward_scores: Vec<Decimal>,
    overfit_count: i32,
    weak_count: i32,
    actionable_count: i32,
    data_quality_warning_count: i32,
}

impl RegimeStrategyGroup {
    fn new(strategy_id: String, symbol: String, timeframe: String) -> Self {
        Self {
            strategy_id,
            symbol,
            timeframe,
            batch_indices: BTreeSet::new(),
            pnl_values: Vec::new(),
            trade_counts: Vec::new(),
            walk_forward_scores: Vec::new(),
            overfit_count: 0,
            weak_count: 0,
            actionable_count: 0,
            data_quality_warning_count: 0,
        }
    }

    fn push_batch(&mut self, batch: &ResearchCampaignBatchResult) {
        self.batch_indices.insert(batch.plan.plan_index);
        match batch.triage_status {
            ResearchBatchTriageStatus::Actionable => self.actionable_count += 1,
            ResearchBatchTriageStatus::DataQualityBlocked | ResearchBatchTriageStatus::Failed => {
                self.data_quality_warning_count += 1
            }
            ResearchBatchTriageStatus::Weak
            | ResearchBatchTriageStatus::OverfitOnly
            | ResearchBatchTriageStatus::NoCandidates
            | ResearchBatchTriageStatus::Unknown => {}
        }
    }

    fn push_candidate(&mut self, candidate: &ResearchBatchCandidateSummary) {
        self.pnl_values.push(candidate.pnl_pct);
        self.trade_counts.push(candidate.trade_count);
        if let Some(status) = candidate.robustness_status {
            self.walk_forward_scores
                .push(walk_forward_status_score(status));
            match status {
                StrategyWalkForwardRobustnessStatus::OverfitRisk => self.overfit_count += 1,
                StrategyWalkForwardRobustnessStatus::Weak
                | StrategyWalkForwardRobustnessStatus::InsufficientData
                | StrategyWalkForwardRobustnessStatus::Failed => self.weak_count += 1,
                StrategyWalkForwardRobustnessStatus::Robust => {}
            }
        }
    }
}

pub fn build_research_regime_strategy_leaderboard(
    campaign: &ResearchCampaignResult,
    generated_at: DateTime<Utc>,
) -> ResearchRegimeStrategyLeaderboard {
    let per_regime_groups = build_regime_strategy_groups(&campaign.batches, true);
    let overall_groups = build_regime_strategy_groups(&campaign.batches, false);
    let mut per_regime = Vec::new();
    for (regime_label, groups) in per_regime_groups {
        let rankings = rank_regime_strategy_groups(groups);
        per_regime.push(ResearchRegimeStrategyCell {
            regime_label,
            rankings,
        });
    }
    let overall_rankings =
        rank_regime_strategy_groups(overall_groups.into_values().flatten().collect());
    let overall_best = overall_rankings.first().cloned();
    let overall_promising = overall_rankings
        .iter()
        .find(|ranking| regime_strategy_ranking_is_promising(ranking))
        .cloned();
    let overall_least_bad = overall_promising
        .is_none()
        .then(|| {
            overall_rankings
                .iter()
                .find(|ranking| regime_strategy_ranking_is_least_bad(ranking))
                .cloned()
        })
        .flatten();
    let best_strategy_by_regime = per_regime
        .iter()
        .filter_map(|cell| {
            cell.rankings
                .first()
                .map(|ranking| regime_strategy_selection(cell.regime_label, ranking))
        })
        .collect::<Vec<_>>();
    let worst_strategy_by_regime = per_regime
        .iter()
        .filter_map(|cell| {
            cell.rankings
                .last()
                .map(|ranking| regime_strategy_selection(cell.regime_label, ranking))
        })
        .collect::<Vec<_>>();
    let best_symbol_timeframe_by_regime = per_regime
        .iter()
        .filter_map(|cell| {
            cell.rankings
                .first()
                .map(|ranking| ResearchRegimeSymbolTimeframeSelection {
                    regime_label: cell.regime_label,
                    symbol: ranking.symbol.clone(),
                    timeframe: ranking.timeframe.clone(),
                    strategy_id: ranking.strategy_id.clone(),
                    status: ranking.status,
                    robustness_score: ranking.robustness_score,
                    median_pnl_pct: ranking.median_pnl_pct,
                })
        })
        .collect::<Vec<_>>();
    let (findings, recommendations) = build_regime_strategy_leaderboard_guidance(&per_regime);

    ResearchRegimeStrategyLeaderboard {
        campaign_id: campaign.campaign_id,
        generated_at,
        per_regime,
        overall_rankings,
        overall_best,
        overall_promising,
        overall_least_bad,
        best_strategy_by_regime,
        worst_strategy_by_regime,
        best_symbol_timeframe_by_regime,
        findings,
        recommendations,
    }
}

fn build_regime_strategy_groups(
    batches: &[ResearchCampaignBatchResult],
    split_by_regime: bool,
) -> BTreeMap<ResearchRegimeLabel, Vec<RegimeStrategyGroup>> {
    let mut groups =
        BTreeMap::<(ResearchRegimeLabel, String, String, String), RegimeStrategyGroup>::new();
    for batch in batches {
        let regime_label = if split_by_regime {
            batch
                .plan
                .regime_label
                .unwrap_or(ResearchRegimeLabel::Unknown)
        } else {
            ResearchRegimeLabel::Mixed
        };
        let key = (
            regime_label,
            batch.plan.strategy_id.clone(),
            batch.plan.symbol.clone(),
            batch.plan.timeframe.clone(),
        );
        let entry = groups.entry(key).or_insert_with(|| {
            RegimeStrategyGroup::new(
                batch.plan.strategy_id.clone(),
                batch.plan.symbol.clone(),
                batch.plan.timeframe.clone(),
            )
        });
        entry.push_batch(batch);
        for candidate in &batch.top_candidates {
            entry.push_candidate(candidate);
        }
    }

    let mut by_regime = BTreeMap::<ResearchRegimeLabel, Vec<RegimeStrategyGroup>>::new();
    for ((regime_label, _, _, _), group) in groups {
        by_regime.entry(regime_label).or_default().push(group);
    }
    by_regime
}

fn rank_regime_strategy_groups(
    groups: Vec<RegimeStrategyGroup>,
) -> Vec<ResearchRegimeStrategyRanking> {
    let mut rankings = groups
        .into_iter()
        .map(regime_strategy_ranking_from_group)
        .collect::<Vec<_>>();
    rankings.sort_by(|left, right| {
        right
            .ranking_score
            .cmp(&left.ranking_score)
            .then_with(|| right.median_pnl_pct.cmp(&left.median_pnl_pct))
            .then_with(|| right.robustness_score.cmp(&left.robustness_score))
            .then_with(|| right.actionable_count.cmp(&left.actionable_count))
            .then_with(|| left.overfit_count.cmp(&right.overfit_count))
            .then_with(|| left.strategy_id.cmp(&right.strategy_id))
            .then_with(|| left.symbol.cmp(&right.symbol))
            .then_with(|| left.timeframe.cmp(&right.timeframe))
    });
    for (index, ranking) in rankings.iter_mut().enumerate() {
        ranking.rank = i32::try_from(index + 1).unwrap_or(i32::MAX);
    }
    rankings
}

fn regime_strategy_ranking_from_group(group: RegimeStrategyGroup) -> ResearchRegimeStrategyRanking {
    let candidate_count = i32::try_from(group.pnl_values.len()).unwrap_or(i32::MAX);
    let batch_count = i32::try_from(group.batch_indices.len()).unwrap_or(i32::MAX);
    let avg_pnl_pct = avg_decimal(&group.pnl_values).unwrap_or(Decimal::ZERO);
    let median_pnl_pct = median_decimal_option(group.pnl_values.clone()).unwrap_or(Decimal::ZERO);
    let best_pnl_pct = group
        .pnl_values
        .iter()
        .copied()
        .max()
        .unwrap_or(Decimal::ZERO);
    let worst_pnl_pct = group
        .pnl_values
        .iter()
        .copied()
        .min()
        .unwrap_or(Decimal::ZERO);
    let profitable_candidate_ratio = if candidate_count > 0 {
        Decimal::from(
            i32::try_from(
                group
                    .pnl_values
                    .iter()
                    .filter(|value| **value > Decimal::ZERO)
                    .count(),
            )
            .unwrap_or(i32::MAX),
        ) / Decimal::from(candidate_count)
    } else {
        Decimal::ZERO
    };
    let avg_walk_forward_score = avg_decimal(&group.walk_forward_scores);
    let avg_trade_count = if group.trade_counts.is_empty() {
        Decimal::ZERO
    } else {
        Decimal::from(group.trade_counts.iter().sum::<i32>())
            / Decimal::from(i32::try_from(group.trade_counts.len()).unwrap_or(1))
    };
    let status = regime_strategy_status(
        candidate_count,
        batch_count,
        median_pnl_pct,
        group.overfit_count,
        group.weak_count,
        group.actionable_count,
        group.data_quality_warning_count,
        avg_walk_forward_score,
    );
    let robustness_score = regime_strategy_robustness_score(
        median_pnl_pct,
        profitable_candidate_ratio,
        candidate_count,
        batch_count,
        group.overfit_count,
        group.weak_count,
        group.data_quality_warning_count,
        avg_walk_forward_score,
    );
    let ranking_score = Decimal::from(robustness_score) + (median_pnl_pct / Decimal::new(10, 0));

    ResearchRegimeStrategyRanking {
        rank: 0,
        strategy_id: group.strategy_id,
        symbol: group.symbol,
        timeframe: group.timeframe,
        status,
        candidate_count,
        batch_count,
        avg_pnl_pct,
        median_pnl_pct,
        best_pnl_pct,
        worst_pnl_pct,
        profitable_candidate_ratio,
        overfit_count: group.overfit_count,
        weak_count: group.weak_count,
        actionable_count: group.actionable_count,
        avg_walk_forward_score,
        avg_trade_count,
        avg_fee_drag_pct: None,
        data_quality_warning_count: group.data_quality_warning_count,
        robustness_score,
        ranking_score,
    }
}

fn regime_strategy_status(
    candidate_count: i32,
    batch_count: i32,
    median_pnl_pct: Decimal,
    overfit_count: i32,
    weak_count: i32,
    actionable_count: i32,
    data_quality_warning_count: i32,
    avg_walk_forward_score: Option<Decimal>,
) -> ResearchRegimeStrategyStatus {
    if data_quality_warning_count > 0 && data_quality_warning_count >= batch_count.max(1) {
        return ResearchRegimeStrategyStatus::DataQualityBlocked;
    }
    if candidate_count < 2 || batch_count < 2 {
        return ResearchRegimeStrategyStatus::InsufficientData;
    }
    if median_pnl_pct < Decimal::ZERO {
        return ResearchRegimeStrategyStatus::Negative;
    }
    if overfit_count > actionable_count && overfit_count >= weak_count {
        return ResearchRegimeStrategyStatus::Overfit;
    }
    if actionable_count > 0
        && weak_count == 0
        && overfit_count == 0
        && median_pnl_pct > Decimal::ZERO
        && avg_walk_forward_score.is_some_and(|score| score >= Decimal::new(80, 0))
    {
        return ResearchRegimeStrategyStatus::Robust;
    }
    if actionable_count > 0 && median_pnl_pct > Decimal::ZERO {
        return ResearchRegimeStrategyStatus::Promising;
    }
    ResearchRegimeStrategyStatus::Weak
}

fn regime_strategy_robustness_score(
    median_pnl_pct: Decimal,
    profitable_candidate_ratio: Decimal,
    candidate_count: i32,
    batch_count: i32,
    overfit_count: i32,
    weak_count: i32,
    data_quality_warning_count: i32,
    avg_walk_forward_score: Option<Decimal>,
) -> i32 {
    let mut score = Decimal::new(50, 0);
    score += median_pnl_pct * Decimal::new(5, 0);
    score += profitable_candidate_ratio * Decimal::new(20, 0);
    if let Some(walk_forward_score) = avg_walk_forward_score {
        score += (walk_forward_score - Decimal::new(50, 0)) / Decimal::new(5, 0);
    }
    if candidate_count < 2 {
        score -= Decimal::new(30, 0);
    } else if candidate_count < 5 {
        score -= Decimal::new(10, 0);
    }
    if batch_count < 2 {
        score -= Decimal::new(30, 0);
    } else if batch_count < 5 {
        score -= Decimal::new(10, 0);
    }
    score -= Decimal::from(overfit_count) * Decimal::new(15, 0);
    score -= Decimal::from(weak_count) * Decimal::new(5, 0);
    score -= Decimal::from(data_quality_warning_count) * Decimal::new(20, 0);
    if median_pnl_pct < Decimal::ZERO {
        score -= Decimal::new(25, 0);
    }
    if candidate_count < 2 || batch_count < 2 {
        score = score.min(Decimal::new(20, 0));
    }
    let clamped = score.clamp(Decimal::ZERO, Decimal::new(100, 0));
    clamped.round().to_i32().unwrap_or(0)
}

fn walk_forward_status_score(status: StrategyWalkForwardRobustnessStatus) -> Decimal {
    Decimal::from(match status {
        StrategyWalkForwardRobustnessStatus::Robust => 100,
        StrategyWalkForwardRobustnessStatus::Weak => 40,
        StrategyWalkForwardRobustnessStatus::OverfitRisk => 20,
        StrategyWalkForwardRobustnessStatus::InsufficientData => 10,
        StrategyWalkForwardRobustnessStatus::Failed => 0,
    })
}

fn avg_decimal(values: &[Decimal]) -> Option<Decimal> {
    if values.is_empty() {
        return None;
    }
    Some(
        values.iter().copied().sum::<Decimal>()
            / Decimal::from(i32::try_from(values.len()).unwrap_or(1)),
    )
}

fn median_decimal_option(mut values: Vec<Decimal>) -> Option<Decimal> {
    if values.is_empty() {
        return None;
    }
    values.sort();
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        Some((values[mid - 1] + values[mid]) / Decimal::new(2, 0))
    } else {
        Some(values[mid])
    }
}

fn regime_strategy_selection(
    regime_label: ResearchRegimeLabel,
    ranking: &ResearchRegimeStrategyRanking,
) -> ResearchRegimeStrategySelection {
    let is_promising = regime_strategy_ranking_is_promising(ranking);
    let is_least_bad = !is_promising && regime_strategy_ranking_is_least_bad(ranking);
    ResearchRegimeStrategySelection {
        regime_label,
        strategy_id: ranking.strategy_id.clone(),
        symbol: ranking.symbol.clone(),
        timeframe: ranking.timeframe.clone(),
        status: ranking.status,
        is_promising,
        is_least_bad,
        score: ranking.robustness_score,
        reason: regime_strategy_selection_reason(ranking, is_promising, is_least_bad),
        robustness_score: ranking.robustness_score,
        median_pnl_pct: ranking.median_pnl_pct,
    }
}

fn regime_strategy_ranking_is_promising(ranking: &ResearchRegimeStrategyRanking) -> bool {
    matches!(
        ranking.status,
        ResearchRegimeStrategyStatus::Robust | ResearchRegimeStrategyStatus::Promising
    ) && ranking.robustness_score > 0
}

fn regime_strategy_ranking_is_least_bad(ranking: &ResearchRegimeStrategyRanking) -> bool {
    matches!(
        ranking.status,
        ResearchRegimeStrategyStatus::Weak
            | ResearchRegimeStrategyStatus::Negative
            | ResearchRegimeStrategyStatus::Overfit
            | ResearchRegimeStrategyStatus::InsufficientData
    )
}

fn regime_strategy_selection_reason(
    ranking: &ResearchRegimeStrategyRanking,
    is_promising: bool,
    is_least_bad: bool,
) -> String {
    if is_promising {
        return format!(
            "{} status with robustness_score={} and median_pnl_pct={}.",
            ranking.status.as_str(),
            ranking.robustness_score,
            ranking.median_pnl_pct
        );
    }
    if is_least_bad {
        return format!(
            "Least-bad {} result; not promising because status={} and robustness_score={}.",
            ranking.status.as_str().to_ascii_lowercase(),
            ranking.status.as_str(),
            ranking.robustness_score
        );
    }
    format!(
        "Not promising because status={} and robustness_score={}.",
        ranking.status.as_str(),
        ranking.robustness_score
    )
}

fn build_regime_strategy_leaderboard_guidance(
    per_regime: &[ResearchRegimeStrategyCell],
) -> (
    Vec<ResearchRegimeStrategyFinding>,
    Vec<ResearchRegimeStrategyRecommendation>,
) {
    let mut findings = Vec::new();
    let mut recommendations = Vec::new();
    for cell in per_regime {
        let promising = cell
            .rankings
            .iter()
            .filter(|ranking| regime_strategy_ranking_is_promising(ranking))
            .count();
        if let Some(top) = cell
            .rankings
            .first()
            .filter(|ranking| regime_strategy_ranking_is_promising(ranking))
        {
            findings.push(regime_strategy_finding(
                "LOW",
                "regime_strategy_promising",
                format!(
                    "{} appears promising in {} with median_pnl_pct={} and robustness_score={}.",
                    top.strategy_id,
                    cell.regime_label.as_str(),
                    top.median_pnl_pct,
                    top.robustness_score
                ),
            ));
        }
        if promising == 0 {
            if let Some(top) = cell
                .rankings
                .first()
                .filter(|ranking| regime_strategy_ranking_is_least_bad(ranking))
            {
                findings.push(regime_strategy_finding(
                    "LOW",
                    "regime_least_bad_strategy_identified",
                    format!(
                        "{} is the least-bad {} strategy in {}; it is not promising.",
                        top.strategy_id,
                        top.status.as_str().to_ascii_lowercase(),
                        cell.regime_label.as_str()
                    ),
                ));
            }
        }
        if promising == 0 {
            findings.push(regime_strategy_finding(
                "MEDIUM",
                "regime_no_promising_strategy",
                format!(
                    "No promising strategy found for {}.",
                    cell.regime_label.as_str()
                ),
            ));
            recommendations.push(regime_strategy_recommendation(
                "MEDIUM",
                "expand_regime_research",
                format!(
                    "Expand deterministic research coverage before using {} candidates for shadow review.",
                    cell.regime_label.as_str()
                ),
            ));
        }
        let overfit_heavy = cell
            .rankings
            .iter()
            .filter(|ranking| ranking.status == ResearchRegimeStrategyStatus::Overfit)
            .count();
        if overfit_heavy > 0 {
            findings.push(regime_strategy_finding(
                "MEDIUM",
                "regime_overfit_heavy",
                format!(
                    "{} has {} overfit-heavy strategy cells.",
                    cell.regime_label.as_str(),
                    overfit_heavy
                ),
            ));
            recommendations.push(regime_strategy_recommendation(
                "MEDIUM",
                "tighten_walk_forward_validation",
                format!(
                    "Prioritize walk-forward and out-of-sample validation for {}.",
                    cell.regime_label.as_str()
                ),
            ));
        }
        if cell
            .rankings
            .iter()
            .any(|ranking| ranking.status == ResearchRegimeStrategyStatus::DataQualityBlocked)
        {
            findings.push(regime_strategy_finding(
                "HIGH",
                "regime_leaderboard_data_quality_blocked",
                format!(
                    "Data quality blocks regime leaderboard interpretation for {}.",
                    cell.regime_label.as_str()
                ),
            ));
            recommendations.push(regime_strategy_recommendation(
                "HIGH",
                "repair_regime_market_data",
                format!(
                    "Repair market data and rerun research before ranking {}.",
                    cell.regime_label.as_str()
                ),
            ));
        }
    }
    recommendations.push(regime_strategy_recommendation(
        "LOW",
        "research_only_no_auto_promotion",
        "Use regime leaderboard as research evidence only; do not auto-promote or submit orders.",
    ));
    (findings, recommendations)
}

fn regime_strategy_finding(
    severity: impl Into<String>,
    code: impl Into<String>,
    message: impl Into<String>,
) -> ResearchRegimeStrategyFinding {
    ResearchRegimeStrategyFinding {
        severity: severity.into(),
        code: code.into(),
        message: message.into(),
    }
}

fn regime_strategy_recommendation(
    priority: impl Into<String>,
    code: impl Into<String>,
    message: impl Into<String>,
) -> ResearchRegimeStrategyRecommendation {
    ResearchRegimeStrategyRecommendation {
        priority: priority.into(),
        code: code.into(),
        message: message.into(),
    }
}

pub fn status_from_campaign_summary(summary: &ResearchCampaignSummary) -> ResearchCampaignStatus {
    if summary.total_batches_planned == 0
        || summary.total_batches_failed == summary.total_batches_planned
    {
        ResearchCampaignStatus::Failed
    } else if summary.total_batches_failed > 0
        || summary.total_batches_completed < summary.total_batches_planned
    {
        ResearchCampaignStatus::PartialSuccess
    } else {
        ResearchCampaignStatus::Completed
    }
}

fn campaign_finding(
    severity: impl Into<String>,
    code: impl Into<String>,
    message: impl Into<String>,
) -> ResearchCampaignFinding {
    ResearchCampaignFinding {
        severity: severity.into(),
        code: code.into(),
        message: message.into(),
    }
}

fn campaign_recommendation(
    priority: impl Into<String>,
    code: impl Into<String>,
    message: impl Into<String>,
) -> ResearchCampaignRecommendation {
    ResearchCampaignRecommendation {
        priority: priority.into(),
        code: code.into(),
        message: message.into(),
    }
}

pub fn build_research_batch_triage(
    batch: &ResearchBatchResult,
    mut candidates: Vec<ResearchBatchCandidateTriage>,
    generated_at: DateTime<Utc>,
) -> ResearchBatchTriage {
    for candidate in &mut candidates {
        let overfit = candidate.walk_forward_status.as_deref() == Some("OVERFIT_RISK")
            || candidate.walk_forward_recommendation.as_deref() == Some("DO_NOT_ACCEPT");
        let actionable = !overfit
            && candidate.walk_forward_status.as_deref() == Some("ROBUST")
            && candidate.experiment_score > Decimal::ZERO
            && candidate.experiment_pnl_pct > Decimal::ZERO;

        candidate.triage_status = if overfit {
            candidate
                .reasons
                .push("walk_forward_overfit_or_do_not_accept".to_string());
            candidate
                .recommendations
                .push("Do not accept automatically; review overfit evidence.".to_string());
            ResearchBatchTriageStatus::OverfitOnly
        } else if actionable {
            candidate
                .reasons
                .push("robust_walk_forward_and_positive_experiment_score".to_string());
            candidate.recommendations.push(
                "Review candidate evidence manually before any lifecycle decision.".to_string(),
            );
            ResearchBatchTriageStatus::Actionable
        } else {
            candidate
                .reasons
                .push("insufficient_or_weak_candidate_evidence".to_string());
            candidate.recommendations.push(
                "Gather stronger walk-forward, qualification, or shadow evidence.".to_string(),
            );
            ResearchBatchTriageStatus::Weak
        };
    }

    candidates.sort_by(|left, right| {
        candidate_triage_sort_bucket(left.triage_status)
            .cmp(&candidate_triage_sort_bucket(right.triage_status))
            .then_with(|| right.experiment_score.cmp(&left.experiment_score))
            .then_with(|| right.experiment_pnl_pct.cmp(&left.experiment_pnl_pct))
            .then_with(|| left.strategy_id.cmp(&right.strategy_id))
            .then_with(|| left.symbol.cmp(&right.symbol))
            .then_with(|| left.timeframe.cmp(&right.timeframe))
            .then_with(|| left.experiment_run_id.cmp(&right.experiment_run_id))
            .then_with(|| left.candidate_id.cmp(&right.candidate_id))
    });
    for (index, candidate) in candidates.iter_mut().enumerate() {
        candidate.rank = i32::try_from(index + 1).unwrap_or(i32::MAX);
    }

    let actionable_count = candidates
        .iter()
        .filter(|candidate| candidate.triage_status == ResearchBatchTriageStatus::Actionable)
        .count() as i32;
    let overfit_count = candidates
        .iter()
        .filter(|candidate| candidate.triage_status == ResearchBatchTriageStatus::OverfitOnly)
        .count() as i32;
    let weak_count = candidates
        .iter()
        .filter(|candidate| candidate.triage_status == ResearchBatchTriageStatus::Weak)
        .count() as i32;

    let mut findings = Vec::new();
    let mut recommendations = Vec::new();
    let data_quality_blocked = matches!(
        batch.quality_after.as_ref().map(|quality| quality.status),
        Some(MarketDataQualityStatus::Bad | MarketDataQualityStatus::InsufficientData)
    );
    if data_quality_blocked {
        findings.push(triage_finding(
            "MEDIUM",
            "research_batch_data_quality_blocked",
            "Research batch blocked by degraded market data.",
        ));
        recommendations.push(triage_recommendation(
            "MEDIUM",
            "repair_or_rebuild_market_data",
            "Repair or rebuild market data before reviewing candidates.",
        ));
    } else if matches!(
        batch.quality_after.as_ref().map(|quality| quality.status),
        Some(MarketDataQualityStatus::Degraded)
    ) {
        findings.push(triage_finding(
            "MEDIUM",
            "research_batch_degraded_market_data",
            "Research batch completed with degraded market data.",
        ));
        recommendations.push(triage_recommendation(
            "MEDIUM",
            "review_market_data_quality",
            "Review data quality warnings before trusting candidate evidence.",
        ));
    }

    let status = if batch.status == ResearchBatchStatus::Failed {
        findings.push(triage_finding(
            "MEDIUM",
            "research_batch_failed",
            "Research batch failed.",
        ));
        recommendations.push(triage_recommendation(
            "MEDIUM",
            "rerun_failed_batch",
            "Inspect failed steps and rerun the batch after fixing the cause.",
        ));
        ResearchBatchTriageStatus::Failed
    } else if data_quality_blocked {
        ResearchBatchTriageStatus::DataQualityBlocked
    } else if candidates.is_empty() {
        findings.push(triage_finding(
            "LOW",
            "research_batch_no_actionable_candidates",
            "No actionable candidates found.",
        ));
        recommendations.push(triage_recommendation(
            "LOW",
            "expand_research_sweep",
            "Expand the research sweep or improve data coverage.",
        ));
        ResearchBatchTriageStatus::NoCandidates
    } else if overfit_count == candidates.len() as i32 {
        findings.push(triage_finding(
            "MEDIUM",
            "research_batch_only_overfit_candidates",
            "Research batch produced only overfit candidates.",
        ));
        recommendations.push(triage_recommendation(
            "MEDIUM",
            "reject_overfit_candidates",
            "Do not accept these candidates without stronger out-of-sample evidence.",
        ));
        ResearchBatchTriageStatus::OverfitOnly
    } else if actionable_count > 0 {
        findings.push(triage_finding(
            "LOW",
            "research_batch_candidates_for_review",
            "Research batch produced candidates for review.",
        ));
        recommendations.push(triage_recommendation(
            "LOW",
            "manual_candidate_review",
            "Review top-ranked candidates manually; triage does not promote candidates.",
        ));
        ResearchBatchTriageStatus::Actionable
    } else {
        findings.push(triage_finding(
            "LOW",
            "research_batch_no_actionable_candidates",
            "No actionable candidates found.",
        ));
        recommendations.push(triage_recommendation(
            "LOW",
            "gather_more_evidence",
            "Gather stronger experiment or walk-forward evidence before review.",
        ));
        ResearchBatchTriageStatus::Weak
    };

    ResearchBatchTriage {
        batch_id: batch.batch_id,
        status,
        candidate_count: candidates.len() as i32,
        actionable_count,
        weak_count,
        overfit_count,
        candidates,
        findings,
        recommendations,
        generated_at,
    }
}

fn candidate_triage_sort_bucket(status: ResearchBatchTriageStatus) -> i32 {
    match status {
        ResearchBatchTriageStatus::Actionable => 0,
        ResearchBatchTriageStatus::Weak => 1,
        ResearchBatchTriageStatus::OverfitOnly => 2,
        ResearchBatchTriageStatus::DataQualityBlocked => 3,
        ResearchBatchTriageStatus::NoCandidates => 4,
        ResearchBatchTriageStatus::Failed => 5,
        ResearchBatchTriageStatus::Unknown => 6,
    }
}

fn triage_finding(
    severity: impl Into<String>,
    code: impl Into<String>,
    message: impl Into<String>,
) -> ResearchBatchTriageFinding {
    ResearchBatchTriageFinding {
        severity: severity.into(),
        code: code.into(),
        message: message.into(),
    }
}

fn triage_recommendation(
    priority: impl Into<String>,
    code: impl Into<String>,
    message: impl Into<String>,
) -> ResearchBatchTriageRecommendation {
    ResearchBatchTriageRecommendation {
        priority: priority.into(),
        code: code.into(),
        message: message.into(),
    }
}

fn ranked_research_batch_candidates(
    mut candidates: Vec<ResearchBatchCandidateSummary>,
) -> Vec<ResearchBatchCandidateSummary> {
    candidates.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| right.pnl_pct.cmp(&left.pnl_pct))
            .then_with(|| left.max_drawdown_pct.cmp(&right.max_drawdown_pct))
            .then_with(|| right.win_rate.cmp(&left.win_rate))
            .then_with(|| right.trade_count.cmp(&left.trade_count))
            .then_with(|| left.strategy_id.cmp(&right.strategy_id))
            .then_with(|| left.symbol.cmp(&right.symbol))
            .then_with(|| left.timeframe.cmp(&right.timeframe))
            .then_with(|| left.experiment_run_id.cmp(&right.experiment_run_id))
    });
    candidates
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchDataReadinessStatus {
    Ready,
    Degraded,
    Insufficient,
}

impl ResearchDataReadinessStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "READY",
            Self::Degraded => "DEGRADED",
            Self::Insufficient => "INSUFFICIENT",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchDataGap {
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub missing_candles: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CandleCoverageSummary {
    pub interval: String,
    pub expected_candles: i64,
    pub actual_candles: i64,
    pub coverage_pct: Decimal,
    pub first_candle_at: Option<DateTime<Utc>>,
    pub last_candle_at: Option<DateTime<Utc>>,
    pub missing_ranges: Vec<ResearchDataGap>,
    pub status: ResearchDataReadinessStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchDataCoverageRequest {
    #[serde(default)]
    pub exchange: MarketDataSource,
    pub symbol: String,
    pub intervals: Vec<String>,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    #[serde(default = "default_required_coverage_pct")]
    pub required_coverage_pct: Decimal,
    pub correlation_id: Option<Uuid>,
}

impl ResearchDataCoverageRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.symbol.trim().is_empty() {
            return Err(CoreError::EmptyCandleBackfillSymbol);
        }
        if self.intervals.is_empty() {
            return Err(CoreError::EmptyResearchDataIntervals);
        }
        for interval in &self.intervals {
            if interval.trim().is_empty() {
                return Err(CoreError::EmptyResearchDataInterval);
            }
            interval.parse::<CandleInterval>()?;
        }
        if self.end_time <= self.start_time {
            return Err(CoreError::InvalidResearchDataTimeRange);
        }
        if self.required_coverage_pct <= Decimal::ZERO
            || self.required_coverage_pct > Decimal::new(100, 0)
        {
            return Err(CoreError::InvalidResearchCoveragePct);
        }
        Ok(())
    }

    pub fn normalized_symbol(&self) -> Result<Symbol, CoreError> {
        Symbol::new(self.symbol.clone())
    }

    pub fn parsed_intervals(&self) -> Result<Vec<CandleInterval>, CoreError> {
        let mut intervals = Vec::with_capacity(self.intervals.len());
        for raw in &self.intervals {
            let interval = raw.parse::<CandleInterval>()?;
            if !intervals.contains(&interval) {
                intervals.push(interval);
            }
        }
        Ok(intervals)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchDataCoverageResult {
    pub exchange: MarketDataSource,
    pub symbol: String,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub required_coverage_pct: Decimal,
    pub status: ResearchDataReadinessStatus,
    pub per_interval: Vec<CandleCoverageSummary>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrossAssetResearchStatus {
    Completed,
    Failed,
    InsufficientData,
}

impl CrossAssetResearchStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::InsufficientData => "INSUFFICIENT_DATA",
        }
    }
}

impl std::str::FromStr for CrossAssetResearchStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            "INSUFFICIENT_DATA" => Ok(Self::InsufficientData),
            other => Err(CoreError::UnsupportedCrossAssetResearchStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrossAssetStrategyKind {
    RelativeStrengthContinuationV1Research,
}

impl CrossAssetStrategyKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RelativeStrengthContinuationV1Research => {
                "RELATIVE_STRENGTH_CONTINUATION_V1_RESEARCH"
            }
        }
    }
}

impl std::str::FromStr for CrossAssetStrategyKind {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "RELATIVE_STRENGTH_CONTINUATION_V1_RESEARCH" => {
                Ok(Self::RelativeStrengthContinuationV1Research)
            }
            other => Err(CoreError::UnsupportedCrossAssetStrategyKind(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum CrossAssetRankMetric {
    RawReturn,
    VolAdjustedReturn,
}

impl CrossAssetRankMetric {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RawReturn => "raw_return",
            Self::VolAdjustedReturn => "vol_adjusted_return",
        }
    }
}

impl std::str::FromStr for CrossAssetRankMetric {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "raw_return" => Ok(Self::RawReturn),
            "vol_adjusted_return" | "vol_adjusted" | "norm24" => Ok(Self::VolAdjustedReturn),
            other => Err(CoreError::UnsupportedCrossAssetRankMetric(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum CrossAssetSizingMode {
    EqualNotional,
    VolatilityNormalized,
}

impl CrossAssetSizingMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EqualNotional => "equal_notional",
            Self::VolatilityNormalized => "volatility_normalized",
        }
    }
}

impl std::str::FromStr for CrossAssetSizingMode {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "equal_notional" | "equal" => Ok(Self::EqualNotional),
            "volatility_normalized" | "vol_norm" => Ok(Self::VolatilityNormalized),
            other => Err(CoreError::UnsupportedCrossAssetSizingMode(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CrossAssetMarketFilter {
    None,
    #[serde(rename = "basket_72h_return_gt", alias = "basket72h_return_gt")]
    Basket72hReturnGt {
        threshold_pct: Decimal,
    },
    #[serde(rename = "basket_24h_return_gt", alias = "basket24h_return_gt")]
    Basket24hReturnGt {
        threshold_pct: Decimal,
    },
    #[serde(
        rename = "at_least_n_symbols_positive_24h",
        alias = "at_least_n_symbols_positive24h"
    )]
    AtLeastNSymbolsPositive24h {
        min_symbols: u32,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CrossAssetVolFilter {
    None,
    AssetNotExtremeVsBasket { max_ratio: Decimal },
    BasketVolBelowPercentile { percentile: Decimal },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CrossAssetOverextensionFilter {
    None,
    #[serde(rename = "max_return_24h_pct", alias = "max_return24h_pct")]
    MaxReturn24hPct {
        max_pct: Decimal,
    },
    #[serde(
        rename = "min_distance_from_72h_high_pct",
        alias = "min_distance_from72h_high_pct"
    )]
    MinDistanceFrom72hHighPct {
        min_pct: Decimal,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CrossAssetExitRule {
    FixedHold,
    StopPct { stop_pct: Decimal },
    TakeProfitPct { take_profit_pct: Decimal },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrossAssetPortfolioStatus {
    Promising,
    Weak,
    OverfitRisk,
    Negative,
    TooSparse,
}

impl CrossAssetPortfolioStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Promising => "PROMISING",
            Self::Weak => "WEAK",
            Self::OverfitRisk => "OVERFIT_RISK",
            Self::Negative => "NEGATIVE",
            Self::TooSparse => "TOO_SPARSE",
        }
    }
}

impl std::str::FromStr for CrossAssetPortfolioStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "PROMISING" => Ok(Self::Promising),
            "WEAK" => Ok(Self::Weak),
            "OVERFIT_RISK" => Ok(Self::OverfitRisk),
            "NEGATIVE" => Ok(Self::Negative),
            "TOO_SPARSE" => Ok(Self::TooSparse),
            other => Err(CoreError::UnsupportedCrossAssetResearchStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrossAssetResearchRecommendation {
    KeepResearching,
    Reject,
    ConsiderImplementationResearchOnly,
}

impl CrossAssetResearchRecommendation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::KeepResearching => "KEEP_RESEARCHING",
            Self::Reject => "REJECT",
            Self::ConsiderImplementationResearchOnly => "CONSIDER_IMPLEMENTATION_RESEARCH_ONLY",
        }
    }
}

impl std::str::FromStr for CrossAssetResearchRecommendation {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "KEEP_RESEARCHING" => Ok(Self::KeepResearching),
            "REJECT" => Ok(Self::Reject),
            "CONSIDER_IMPLEMENTATION_RESEARCH_ONLY" => Ok(Self::ConsiderImplementationResearchOnly),
            other => Err(CoreError::UnsupportedCrossAssetResearchRecommendation(
                other.to_string(),
            )),
        }
    }
}

fn default_cross_asset_strategy_kind() -> CrossAssetStrategyKind {
    CrossAssetStrategyKind::RelativeStrengthContinuationV1Research
}

fn default_cross_asset_timeframe() -> String {
    "1h".to_string()
}

fn default_cross_asset_one_active_position() -> bool {
    true
}

fn default_cross_asset_min_weight() -> Decimal {
    Decimal::new(25, 2)
}

fn default_cross_asset_max_weight() -> Decimal {
    Decimal::ONE
}

fn default_cross_asset_market_filter() -> CrossAssetMarketFilter {
    CrossAssetMarketFilter::None
}

fn default_cross_asset_vol_filter() -> CrossAssetVolFilter {
    CrossAssetVolFilter::None
}

fn default_cross_asset_overextension_filter() -> CrossAssetOverextensionFilter {
    CrossAssetOverextensionFilter::None
}

fn default_cross_asset_exit_rule() -> CrossAssetExitRule {
    CrossAssetExitRule::FixedHold
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetResearchRequest {
    #[serde(default = "default_cross_asset_strategy_kind")]
    pub strategy_kind: CrossAssetStrategyKind,
    pub symbols: Vec<String>,
    #[serde(default = "default_cross_asset_timeframe")]
    pub timeframe: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub ranking_lookback_hours: u32,
    pub rank_metric: CrossAssetRankMetric,
    pub min_top_return_pct: Decimal,
    pub min_rank_spread_pct: Decimal,
    pub holding_hours: u32,
    pub fee_bps: Decimal,
    pub slippage_bps: Decimal,
    #[serde(default = "default_cross_asset_one_active_position")]
    pub one_active_position: bool,
    pub sizing_mode: CrossAssetSizingMode,
    #[serde(default = "default_cross_asset_min_weight")]
    pub min_weight: Decimal,
    #[serde(default = "default_cross_asset_max_weight")]
    pub max_weight: Decimal,
    #[serde(default = "default_cross_asset_market_filter")]
    pub market_filter: CrossAssetMarketFilter,
    #[serde(default = "default_cross_asset_vol_filter")]
    pub vol_filter: CrossAssetVolFilter,
    #[serde(default = "default_cross_asset_overextension_filter")]
    pub overextension_filter: CrossAssetOverextensionFilter,
    #[serde(default = "default_cross_asset_exit_rule")]
    pub exit_rule: CrossAssetExitRule,
    pub correlation_id: Option<Uuid>,
}

impl CrossAssetResearchRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.symbols.len() < 2 {
            return Err(CoreError::InvalidCrossAssetResearchRequest(
                "at least two symbols are required".to_string(),
            ));
        }
        if self.symbols.iter().any(|symbol| symbol.trim().is_empty()) {
            return Err(CoreError::InvalidCrossAssetResearchRequest(
                "symbols cannot contain empty values".to_string(),
            ));
        }
        self.parsed_timeframe()?;
        if self.timeframe != "1h" && self.timeframe != "4h" {
            return Err(CoreError::InvalidCrossAssetResearchRequest(
                "only 1h and 4h timeframes are supported for cross-asset research MVP".to_string(),
            ));
        }
        if self.end_time <= self.start_time {
            return Err(CoreError::InvalidCrossAssetResearchRequest(
                "end_time must be after start_time".to_string(),
            ));
        }
        if self.ranking_lookback_hours == 0 || self.holding_hours == 0 {
            return Err(CoreError::InvalidCrossAssetResearchRequest(
                "ranking_lookback_hours and holding_hours must be greater than zero".to_string(),
            ));
        }
        if self.min_weight <= Decimal::ZERO || self.max_weight <= Decimal::ZERO {
            return Err(CoreError::InvalidCrossAssetResearchRequest(
                "min_weight and max_weight must be greater than zero".to_string(),
            ));
        }
        if self.min_weight > self.max_weight {
            return Err(CoreError::InvalidCrossAssetResearchRequest(
                "min_weight cannot exceed max_weight".to_string(),
            ));
        }
        if self.fee_bps < Decimal::ZERO || self.slippage_bps < Decimal::ZERO {
            return Err(CoreError::InvalidCrossAssetResearchRequest(
                "fee_bps and slippage_bps cannot be negative".to_string(),
            ));
        }
        Ok(())
    }

    pub fn parsed_symbols(&self) -> Result<Vec<Symbol>, CoreError> {
        self.symbols.iter().cloned().map(Symbol::new).collect()
    }

    pub fn parsed_timeframe(&self) -> Result<CandleInterval, CoreError> {
        self.timeframe.parse::<CandleInterval>()
    }
}

pub const RELATIVE_STRENGTH_CONTINUATION_V1_ID: &str = "relative_strength_continuation_v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossAssetStrategyPackageIdentity {
    pub strategy_id: String,
    pub scope: String,
    pub long_only: bool,
    pub one_active_position: bool,
    pub portfolio_cross_sectional: bool,
    pub live_executable: bool,
    pub paper_executable: bool,
    pub testnet_executable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetStrategyPackage {
    pub identity: CrossAssetStrategyPackageIdentity,
    pub default_config: CrossAssetResearchRequest,
    pub allowed_next_actions: Vec<String>,
    pub forbidden_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetRelativeStrengthV1Dossier {
    pub strategy_identity: CrossAssetStrategyPackageIdentity,
    pub default_config: CrossAssetResearchRequest,
    pub latest_supporting_run_id: Option<Uuid>,
    pub latest_matrix_id: Option<Uuid>,
    pub evidence_2023_2024: Option<CrossAssetResearchResult>,
    pub evidence_2025_plus: Option<CrossAssetResearchResult>,
    pub evidence_combined: Option<CrossAssetResearchResult>,
    pub robustness_status: Option<CrossAssetRobustnessStatus>,
    pub robustness_recommendation: Option<CrossAssetRobustnessRecommendation>,
    pub candidate_gate_preview_status: Option<CrossAssetCandidateGatePreviewStatus>,
    pub candidate_gate_preview_recommended_action: Option<String>,
    pub candidate_gate_blockers: Vec<String>,
    pub candidate_gate_warnings: Vec<String>,
    pub candidate_creation_policy_status: Option<CrossAssetCandidateCreationPolicyStatus>,
    pub candidate_creation_policy_summaries: Vec<CrossAssetCandidateCreationPolicyDossierSummary>,
    pub candidate_creation_policy_recommended_next_action: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub research_candidate_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub research_candidate_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_scope: Option<String>,
    #[serde(default)]
    pub execution_authority: String,
    #[serde(default)]
    pub paper_ready: bool,
    #[serde(default)]
    pub shadow_ready: bool,
    #[serde(default)]
    pub testnet_ready: bool,
    #[serde(default)]
    pub live_ready: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_action_after_candidate_creation: Option<String>,
    pub blockers: Vec<String>,
    pub allowed_next_actions: Vec<String>,
    pub forbidden_actions: Vec<String>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossAssetCandidateGatePreviewRequest {
    pub strategy_package_id: String,
    pub run_id: Option<Uuid>,
    pub matrix_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrossAssetCandidateGatePreviewStatus {
    NotReady,
    NearReady,
    ReadyForResearchCandidateReview,
    Blocked,
}

impl CrossAssetCandidateGatePreviewStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotReady => "NOT_READY",
            Self::NearReady => "NEAR_READY",
            Self::ReadyForResearchCandidateReview => "READY_FOR_RESEARCH_CANDIDATE_REVIEW",
            Self::Blocked => "BLOCKED",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrossAssetCandidateGateCheckStatus {
    Pass,
    Warn,
    Block,
}

impl CrossAssetCandidateGateCheckStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Warn => "WARN",
            Self::Block => "BLOCK",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossAssetCandidateGateCheck {
    pub code: String,
    pub status: CrossAssetCandidateGateCheckStatus,
    pub message: String,
    pub observed_value: Option<String>,
    pub threshold: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossAssetCandidateGateBlocker {
    pub code: String,
    pub message: String,
    pub observed_value: Option<String>,
    pub threshold: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossAssetCandidateGateWarning {
    pub code: String,
    pub message: String,
    pub observed_value: Option<String>,
    pub threshold: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetCandidateGatePreviewResult {
    pub package_id: String,
    pub run_id: Option<Uuid>,
    pub matrix_id: Option<Uuid>,
    pub status: CrossAssetCandidateGatePreviewStatus,
    pub checks: Vec<CrossAssetCandidateGateCheck>,
    pub blockers: Vec<CrossAssetCandidateGateBlocker>,
    pub warnings: Vec<CrossAssetCandidateGateWarning>,
    pub recommended_action: String,
    pub forbidden_actions: Vec<String>,
    pub preview_only: bool,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CrossAssetCandidateCreationPolicyStrictness {
    Conservative,
    Balanced,
    Experimental,
}

impl CrossAssetCandidateCreationPolicyStrictness {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Conservative => "conservative",
            Self::Balanced => "balanced",
            Self::Experimental => "experimental",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "conservative" => Some(Self::Conservative),
            "balanced" => Some(Self::Balanced),
            "experimental" => Some(Self::Experimental),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossAssetCandidateCreationPolicyPreviewRequest {
    pub package_id: String,
    pub run_id: Option<Uuid>,
    pub matrix_id: Option<Uuid>,
    pub strictness: CrossAssetCandidateCreationPolicyStrictness,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrossAssetCandidateCreationPolicyStatus {
    PolicyBlocked,
    PolicyNeedsReview,
    PolicyReadyForManualCreate,
    PolicyNotApplicable,
}

impl CrossAssetCandidateCreationPolicyStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PolicyBlocked => "POLICY_BLOCKED",
            Self::PolicyNeedsReview => "POLICY_NEEDS_REVIEW",
            Self::PolicyReadyForManualCreate => "POLICY_READY_FOR_MANUAL_CREATE",
            Self::PolicyNotApplicable => "POLICY_NOT_APPLICABLE",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossAssetCandidateCreationRequirement {
    pub code: String,
    pub status: CrossAssetCandidateGateCheckStatus,
    pub message: String,
    pub observed_value: Option<String>,
    pub threshold: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossAssetCandidateCreationRisk {
    pub code: String,
    pub severity: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetCandidateCreationPolicyPreviewResult {
    pub package_id: String,
    pub run_id: Option<Uuid>,
    pub matrix_id: Option<Uuid>,
    pub strictness: CrossAssetCandidateCreationPolicyStrictness,
    pub status: CrossAssetCandidateCreationPolicyStatus,
    pub hard_requirements: Vec<CrossAssetCandidateCreationRequirement>,
    pub review_requirements: Vec<CrossAssetCandidateCreationRequirement>,
    pub risks: Vec<CrossAssetCandidateCreationRisk>,
    pub warnings: Vec<String>,
    pub recommended_action: String,
    pub forbidden_actions: Vec<String>,
    pub preview_only: bool,
    pub no_candidate_created: bool,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossAssetCandidateCreationPolicyDossierSummary {
    pub strictness: CrossAssetCandidateCreationPolicyStrictness,
    pub status: CrossAssetCandidateCreationPolicyStatus,
    pub hard_blocker_count: i32,
    pub review_warning_count: i32,
    pub recommended_action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetResearchCandidateManualCreatePreview {
    pub package_id: String,
    pub run_id: Option<Uuid>,
    pub matrix_id: Option<Uuid>,
    pub strictness: CrossAssetCandidateCreationPolicyStrictness,
    pub policy_status: CrossAssetCandidateCreationPolicyStatus,
    pub blockers: Vec<String>,
    pub warnings: Vec<String>,
    pub candidate_would_be_created: bool,
    pub proposed_candidate_status: String,
    pub candidate_scope: String,
    pub execution_authority: String,
    pub exact_confirmation_required: String,
    pub forbidden_actions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub existing_candidate_id: Option<Uuid>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossAssetResearchCandidateManualCreateRequest {
    pub strictness: CrossAssetCandidateCreationPolicyStrictness,
    #[serde(alias = "confirm")]
    pub confirmation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetResearchCandidateManualCreateResult {
    pub package_id: String,
    pub run_id: Option<Uuid>,
    pub matrix_id: Option<Uuid>,
    pub strictness: CrossAssetCandidateCreationPolicyStrictness,
    pub policy_status: CrossAssetCandidateCreationPolicyStatus,
    pub candidate_id: Uuid,
    pub candidate_status: String,
    pub candidate_scope: String,
    pub implementation_research_only: bool,
    pub execution_authority: String,
    pub created: bool,
    pub idempotent: bool,
    pub forbidden_actions: Vec<String>,
    pub warnings_acknowledged: Vec<String>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrossAssetCandidateReviewStatus {
    CandidateReviewReady,
    NeedsHumanReview,
    Blocked,
    NotApplicable,
}

impl CrossAssetCandidateReviewStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CandidateReviewReady => "CANDIDATE_REVIEW_READY",
            Self::NeedsHumanReview => "NEEDS_HUMAN_REVIEW",
            Self::Blocked => "BLOCKED",
            Self::NotApplicable => "NOT_APPLICABLE",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossAssetCandidateReviewBlocker {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossAssetCandidateReviewWarning {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetCandidateFixedRunMetrics {
    pub run_id: Uuid,
    pub status: CrossAssetResearchStatus,
    pub portfolio_status: CrossAssetPortfolioStatus,
    pub recommendation: CrossAssetResearchRecommendation,
    pub total_trades: i32,
    pub compounded_pnl_pct: Decimal,
    pub max_drawdown_pct: Decimal,
    pub max_symbol_concentration_pct: Decimal,
    pub worst_window_pnl_pct: Decimal,
    pub symbol_distribution: BTreeMap<String, i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetCandidateMatrixMetrics {
    pub matrix_id: Uuid,
    pub status: CrossAssetRobustnessStatus,
    pub recommendation: CrossAssetRobustnessRecommendation,
    pub cell_count: i32,
    pub evaluated_config_count: i32,
    pub full_config_count: i32,
    pub skipped_config_count: i32,
    pub top_config_index: Option<i32>,
    pub top_total_trades: Option<i32>,
    pub top_compounded_pnl_pct: Option<Decimal>,
    pub top_median_trade_pnl_pct: Option<Decimal>,
    pub top_max_drawdown_pct: Option<Decimal>,
    pub top_worst_window_pnl_pct: Option<Decimal>,
    pub top_btc_trade_count: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetCandidateReadiness {
    pub candidate_review_ready: bool,
    pub shadow_ready: bool,
    pub paper_ready: bool,
    pub testnet_ready: bool,
    pub live_ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetCandidateQualification {
    pub candidate_id: Uuid,
    pub package_id: String,
    pub status: CrossAssetCandidateReviewStatus,
    pub readiness: CrossAssetCandidateReadiness,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_performance: Option<CrossAssetShadowObservationPerformanceSummary>,
    pub blockers: Vec<CrossAssetCandidateReviewBlocker>,
    pub warnings: Vec<CrossAssetCandidateReviewWarning>,
    pub allowed_next_actions: Vec<String>,
    pub forbidden_actions: Vec<String>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetResearchCandidateDossier {
    pub candidate_id: Uuid,
    pub package_id: String,
    pub candidate_status: ResearchCandidateStatus,
    pub scope: String,
    pub execution_authority: String,
    pub source_run_id: Option<Uuid>,
    pub source_matrix_id: Option<Uuid>,
    pub fixed_run_metrics: Option<CrossAssetCandidateFixedRunMetrics>,
    pub matrix_metrics: Option<CrossAssetCandidateMatrixMetrics>,
    pub policy_strictness_used: CrossAssetCandidateCreationPolicyStrictness,
    pub policy_warnings_acknowledged: Vec<String>,
    pub lifecycle_status: CrossAssetCandidateReviewStatus,
    pub readiness: CrossAssetCandidateReadiness,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_performance: Option<CrossAssetShadowObservationPerformanceSummary>,
    pub blockers: Vec<CrossAssetCandidateReviewBlocker>,
    pub warnings: Vec<CrossAssetCandidateReviewWarning>,
    pub allowed_next_actions: Vec<String>,
    pub forbidden_actions: Vec<String>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrossAssetAcceptShadowPreviewStatus {
    Blocked,
    NotApplicable,
}

impl CrossAssetAcceptShadowPreviewStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Blocked => "BLOCKED",
            Self::NotApplicable => "NOT_APPLICABLE",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetAcceptShadowPreview {
    pub candidate_id: Uuid,
    pub package_id: String,
    pub status: CrossAssetAcceptShadowPreviewStatus,
    pub reasons: Vec<CrossAssetCandidateReviewBlocker>,
    pub no_mutation: bool,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrossAssetShadowObservationDecision {
    WouldSelect,
    NoSignal,
    SkippedNoNewCandle,
    SkippedStaleCandles,
    BlockedNotPromotedOrNotAccepted,
    Error,
}

impl CrossAssetShadowObservationDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WouldSelect => "WOULD_SELECT",
            Self::NoSignal => "NO_SIGNAL",
            Self::SkippedNoNewCandle => "SKIPPED_NO_NEW_CANDLE",
            Self::SkippedStaleCandles => "SKIPPED_STALE_CANDLES",
            Self::BlockedNotPromotedOrNotAccepted => "BLOCKED_NOT_PROMOTED_OR_NOT_ACCEPTED",
            Self::Error => "ERROR",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrossAssetShadowObservationStatus {
    PreviewOnly,
    ObservationRecorded,
    Skipped,
    Blocked,
    Error,
}

impl CrossAssetShadowObservationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PreviewOnly => "PREVIEW_ONLY",
            Self::ObservationRecorded => "OBSERVATION_RECORDED",
            Self::Skipped => "SKIPPED",
            Self::Blocked => "BLOCKED",
            Self::Error => "ERROR",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossAssetShadowObservationPreviewRequest {
    pub candidate_id: Uuid,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossAssetShadowObservationRunRequest {
    #[serde(default)]
    pub research_observation_only: bool,
    pub confirmation_text: Option<String>,
    #[serde(default)]
    pub allow_duplicate_same_candle: bool,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetShadowObservationRankingRow {
    pub symbol: String,
    pub rank: i32,
    pub score: Decimal,
    pub return_pct: Decimal,
    pub return_24h_pct: Option<Decimal>,
    pub realized_vol_24h_pct: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetShadowObservationPreviewResult {
    pub candidate_id: Uuid,
    pub package_id: String,
    pub candidate_status: ResearchCandidateStatus,
    pub latest_available_aligned_candle_time: Option<DateTime<Utc>>,
    pub latest_evaluated_candle_time: Option<DateTime<Utc>>,
    pub would_create_observation: bool,
    pub decision: CrossAssetShadowObservationDecision,
    pub status: CrossAssetShadowObservationStatus,
    pub selected_symbol: Option<String>,
    pub ranking_snapshot: Vec<CrossAssetShadowObservationRankingRow>,
    pub rank_spread_pct: Option<Decimal>,
    pub market_filter_passed: bool,
    pub vol_filter_passed: bool,
    pub reason: String,
    pub warnings: Vec<String>,
    pub no_mutation: bool,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetShadowObservationRunResult {
    pub candidate_id: Uuid,
    pub package_id: String,
    pub candidate_status: ResearchCandidateStatus,
    pub observation_id: Option<Uuid>,
    pub observation_created: bool,
    pub duplicate_same_candle: bool,
    pub latest_available_aligned_candle_time: Option<DateTime<Utc>>,
    pub latest_evaluated_candle_time: Option<DateTime<Utc>>,
    pub evaluated_candle_time: Option<DateTime<Utc>>,
    pub decision: CrossAssetShadowObservationDecision,
    pub status: CrossAssetShadowObservationStatus,
    pub selected_symbol: Option<String>,
    pub ranking_snapshot: Vec<CrossAssetShadowObservationRankingRow>,
    pub rank_spread_pct: Option<Decimal>,
    pub market_filter_passed: bool,
    pub vol_filter_passed: bool,
    pub reason: String,
    pub warnings: Vec<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetShadowObservationPerformanceSummary {
    pub total_shadow_observations: i64,
    pub independent_shadow_observations: i64,
    pub unique_evaluated_candle_count: i64,
    pub would_select_count: i64,
    pub no_signal_count: i64,
    pub skipped_count: i64,
    pub latest_evaluated_candle_time: Option<DateTime<Utc>>,
    pub latest_selected_symbol: Option<String>,
    pub selected_symbol_distribution: BTreeMap<String, i64>,
    pub shadow_status: Option<String>,
}

pub fn relative_strength_continuation_v1_identity() -> CrossAssetStrategyPackageIdentity {
    CrossAssetStrategyPackageIdentity {
        strategy_id: RELATIVE_STRENGTH_CONTINUATION_V1_ID.to_string(),
        scope: "cross_asset_research_only".to_string(),
        long_only: true,
        one_active_position: true,
        portfolio_cross_sectional: true,
        live_executable: false,
        paper_executable: false,
        testnet_executable: false,
    }
}

pub fn relative_strength_continuation_v1_forbidden_actions() -> Vec<String> {
    vec![
        "no_accept_shadow".to_string(),
        "no_promote_shadow".to_string(),
        "no_paper".to_string(),
        "no_testnet".to_string(),
        "no_live".to_string(),
    ]
}

fn cross_asset_candidate_blocker(code: &str, message: &str) -> CrossAssetCandidateReviewBlocker {
    CrossAssetCandidateReviewBlocker {
        code: code.to_string(),
        message: message.to_string(),
    }
}

fn cross_asset_candidate_warning(code: &str, message: &str) -> CrossAssetCandidateReviewWarning {
    CrossAssetCandidateReviewWarning {
        code: code.to_string(),
        message: message.to_string(),
    }
}

pub fn cross_asset_fixed_run_metrics(
    run: &CrossAssetResearchResult,
) -> CrossAssetCandidateFixedRunMetrics {
    CrossAssetCandidateFixedRunMetrics {
        run_id: run.run_id,
        status: run.status,
        portfolio_status: run.portfolio_status,
        recommendation: run.recommendation,
        total_trades: run.total_trades,
        compounded_pnl_pct: run.compounded_pnl_pct,
        max_drawdown_pct: run.max_drawdown_pct,
        max_symbol_concentration_pct: run.max_symbol_concentration_pct,
        worst_window_pnl_pct: run.worst_window_pnl_pct,
        symbol_distribution: run.symbol_distribution.clone(),
    }
}

pub fn cross_asset_matrix_metrics(
    matrix: &CrossAssetRobustnessMatrixResult,
) -> CrossAssetCandidateMatrixMetrics {
    let top = matrix.rankings.first();
    CrossAssetCandidateMatrixMetrics {
        matrix_id: matrix.run_id,
        status: matrix.status,
        recommendation: matrix.recommendation,
        cell_count: matrix.cell_count,
        evaluated_config_count: matrix.evaluated_config_count,
        full_config_count: matrix.full_config_count,
        skipped_config_count: matrix.skipped_config_count,
        top_config_index: top.map(|value| value.config_index),
        top_total_trades: top.map(|value| value.total_trades),
        top_compounded_pnl_pct: top.map(|value| value.combined_pnl_pct),
        top_median_trade_pnl_pct: top.map(|value| value.median_cell_pnl_pct),
        top_max_drawdown_pct: top.map(|value| value.max_drawdown_pct),
        top_worst_window_pnl_pct: top.map(|value| value.worst_window_pnl_pct),
        top_btc_trade_count: top.map(|value| value.btc_trade_count),
    }
}

pub fn evaluate_cross_asset_candidate_qualification(
    candidate_id: Uuid,
    package_id: String,
    candidate_status: ResearchCandidateStatus,
    execution_authority: &str,
    policy_strictness_used: CrossAssetCandidateCreationPolicyStrictness,
    policy_warnings_acknowledged: &[String],
    shadow_performance: Option<CrossAssetShadowObservationPerformanceSummary>,
    generated_at: DateTime<Utc>,
) -> CrossAssetCandidateQualification {
    let mut blockers = vec![
        cross_asset_candidate_blocker(
            "direct_execution_unsupported",
            "Direct execution is unsupported for cross-asset implementation research.",
        ),
        cross_asset_candidate_blocker(
            "no_cross_asset_shadow_runner_support",
            "No cross-asset shadow runner support exists for portfolio strategy observation.",
        ),
        cross_asset_candidate_blocker(
            "not_accepted_for_shadow",
            "Candidate has not been accepted for shadow observation.",
        ),
        cross_asset_candidate_blocker(
            "not_marked_ready_for_testnet",
            "Candidate is not marked ready for testnet review.",
        ),
    ];

    let independent_observations = shadow_performance
        .as_ref()
        .map(|summary| summary.independent_shadow_observations)
        .unwrap_or(0);
    if independent_observations < 30 {
        blockers.push(cross_asset_candidate_blocker(
            "not_enough_shadow_observations",
            "At least 30 independent cross-asset shadow observations are required before any further review.",
        ));
    }

    if execution_authority != "NONE" {
        blockers.push(cross_asset_candidate_blocker(
            "execution_authority_not_none",
            "Cross-asset research candidates must have execution authority NONE.",
        ));
    }

    let mut warnings = vec![
        cross_asset_candidate_warning(
            "twenty_25_plus_median_trade_weak",
            "2025+ median trade evidence is weak and must stay visible during review.",
        ),
        cross_asset_candidate_warning(
            "trade_count_barely_above_threshold",
            "2025+ trade count is barely above the minimum evidence threshold.",
        ),
        cross_asset_candidate_warning(
            "btc_participation_below_5_pct",
            "BTC participation is below 5%, limiting cross-asset generalization confidence.",
        ),
        cross_asset_candidate_warning(
            "implementation_research_only",
            "Package remains implementation-research only.",
        ),
    ];

    if policy_strictness_used == CrossAssetCandidateCreationPolicyStrictness::Experimental {
        warnings.push(cross_asset_candidate_warning(
            "experimental_policy_strictness_used",
            "Candidate was created under experimental policy strictness.",
        ));
    }
    for warning in policy_warnings_acknowledged {
        if !warnings.iter().any(|value| value.code == *warning) {
            warnings.push(cross_asset_candidate_warning(
                warning,
                "Policy warning was acknowledged during manual candidate creation.",
            ));
        }
    }

    let candidate_review_ready = matches!(
        candidate_status,
        ResearchCandidateStatus::Discovered
            | ResearchCandidateStatus::Observing
            | ResearchCandidateStatus::AcceptedForShadow
            | ResearchCandidateStatus::PromotedToShadowConfig
    ) && execution_authority == "NONE";
    let readiness = CrossAssetCandidateReadiness {
        candidate_review_ready,
        shadow_ready: false,
        paper_ready: false,
        testnet_ready: false,
        live_ready: false,
    };
    let status = if package_id != RELATIVE_STRENGTH_CONTINUATION_V1_ID {
        CrossAssetCandidateReviewStatus::NotApplicable
    } else if candidate_review_ready {
        CrossAssetCandidateReviewStatus::NeedsHumanReview
    } else {
        CrossAssetCandidateReviewStatus::Blocked
    };
    let mut forbidden_actions = relative_strength_continuation_v1_forbidden_actions();
    forbidden_actions.extend([
        "no_runner_config_change".to_string(),
        "no_scheduled_jobs".to_string(),
        "no_shadow_runs".to_string(),
        "no_ready_for_testnet".to_string(),
    ]);
    forbidden_actions.sort();
    forbidden_actions.dedup();

    CrossAssetCandidateQualification {
        candidate_id,
        package_id,
        status,
        readiness,
        shadow_performance,
        blockers,
        warnings,
        allowed_next_actions: relative_strength_continuation_v1_allowed_next_actions(),
        forbidden_actions,
        generated_at,
    }
}

#[allow(clippy::too_many_arguments)]
pub fn build_cross_asset_research_candidate_dossier(
    candidate_id: Uuid,
    package_id: String,
    candidate_status: ResearchCandidateStatus,
    scope: String,
    execution_authority: String,
    source_run_id: Option<Uuid>,
    source_matrix_id: Option<Uuid>,
    fixed_run_metrics: Option<CrossAssetCandidateFixedRunMetrics>,
    matrix_metrics: Option<CrossAssetCandidateMatrixMetrics>,
    policy_strictness_used: CrossAssetCandidateCreationPolicyStrictness,
    policy_warnings_acknowledged: Vec<String>,
    shadow_performance: Option<CrossAssetShadowObservationPerformanceSummary>,
    generated_at: DateTime<Utc>,
) -> CrossAssetResearchCandidateDossier {
    let qualification = evaluate_cross_asset_candidate_qualification(
        candidate_id,
        package_id.clone(),
        candidate_status,
        &execution_authority,
        policy_strictness_used,
        &policy_warnings_acknowledged,
        shadow_performance,
        generated_at,
    );
    CrossAssetResearchCandidateDossier {
        candidate_id,
        package_id,
        candidate_status,
        scope,
        execution_authority,
        source_run_id,
        source_matrix_id,
        fixed_run_metrics,
        matrix_metrics,
        policy_strictness_used,
        policy_warnings_acknowledged,
        lifecycle_status: qualification.status,
        readiness: qualification.readiness,
        shadow_performance: qualification.shadow_performance,
        blockers: qualification.blockers,
        warnings: qualification.warnings,
        allowed_next_actions: qualification.allowed_next_actions,
        forbidden_actions: qualification.forbidden_actions,
        generated_at,
    }
}

pub fn preview_cross_asset_accept_shadow(
    candidate_id: Uuid,
    package_id: String,
    generated_at: DateTime<Utc>,
) -> CrossAssetAcceptShadowPreview {
    CrossAssetAcceptShadowPreview {
        candidate_id,
        package_id,
        status: CrossAssetAcceptShadowPreviewStatus::NotApplicable,
        reasons: vec![
            cross_asset_candidate_blocker(
                "no_cross_asset_shadow_runner_support",
                "No cross-asset shadow runner support exists.",
            ),
            cross_asset_candidate_blocker(
                "direct_execution_unsupported",
                "Direct execution is unsupported for this research package.",
            ),
            cross_asset_candidate_blocker(
                "no_observation_path",
                "No observation path exists for cross-asset portfolio strategy candidates.",
            ),
        ],
        no_mutation: true,
        generated_at,
    }
}

pub fn relative_strength_continuation_v1_allowed_next_actions() -> Vec<String> {
    vec![
        "KEEP_IMPLEMENTATION_RESEARCH".to_string(),
        "RUN_MORE_ROBUSTNESS".to_string(),
    ]
}

pub fn preview_cross_asset_candidate_gate(
    request: CrossAssetCandidateGatePreviewRequest,
    supporting_run: Option<&CrossAssetResearchResult>,
    matrix: Option<&CrossAssetRobustnessMatrixResult>,
    matrix_cells: &[CrossAssetRobustnessMatrixCell],
    existing_candidate_count: i64,
    data_quality_good: bool,
    execution_authority_none: bool,
) -> CrossAssetCandidateGatePreviewResult {
    let mut checks = Vec::new();
    let mut blockers = Vec::new();
    let mut warnings = Vec::new();

    let add_blocker = |checks: &mut Vec<CrossAssetCandidateGateCheck>,
                       blockers: &mut Vec<CrossAssetCandidateGateBlocker>,
                       code: &str,
                       message: &str,
                       observed_value: Option<String>,
                       threshold: Option<&str>| {
        checks.push(CrossAssetCandidateGateCheck {
            code: code.to_string(),
            status: CrossAssetCandidateGateCheckStatus::Block,
            message: message.to_string(),
            observed_value: observed_value.clone(),
            threshold: threshold.map(ToString::to_string),
        });
        blockers.push(CrossAssetCandidateGateBlocker {
            code: code.to_string(),
            message: message.to_string(),
            observed_value,
            threshold: threshold.map(ToString::to_string),
        });
    };
    let add_warning = |checks: &mut Vec<CrossAssetCandidateGateCheck>,
                       warnings: &mut Vec<CrossAssetCandidateGateWarning>,
                       code: &str,
                       message: &str,
                       observed_value: Option<String>,
                       threshold: Option<&str>| {
        checks.push(CrossAssetCandidateGateCheck {
            code: code.to_string(),
            status: CrossAssetCandidateGateCheckStatus::Warn,
            message: message.to_string(),
            observed_value: observed_value.clone(),
            threshold: threshold.map(ToString::to_string),
        });
        warnings.push(CrossAssetCandidateGateWarning {
            code: code.to_string(),
            message: message.to_string(),
            observed_value,
            threshold: threshold.map(ToString::to_string),
        });
    };
    let add_pass = |checks: &mut Vec<CrossAssetCandidateGateCheck>,
                    code: &str,
                    message: &str,
                    observed_value: Option<String>,
                    threshold: Option<&str>| {
        checks.push(CrossAssetCandidateGateCheck {
            code: code.to_string(),
            status: CrossAssetCandidateGateCheckStatus::Pass,
            message: message.to_string(),
            observed_value,
            threshold: threshold.map(ToString::to_string),
        });
    };

    if request.strategy_package_id != RELATIVE_STRENGTH_CONTINUATION_V1_ID {
        add_blocker(
            &mut checks,
            &mut blockers,
            "unsupported_strategy_package",
            "Candidate gate preview currently supports only relative_strength_continuation_v1.",
            Some(request.strategy_package_id.clone()),
            Some(RELATIVE_STRENGTH_CONTINUATION_V1_ID),
        );
    } else {
        add_pass(
            &mut checks,
            "supported_strategy_package",
            "Strategy package is supported by the cross-asset candidate gate preview.",
            Some(request.strategy_package_id.clone()),
            None,
        );
    }

    match supporting_run {
        Some(run) => add_pass(
            &mut checks,
            "supporting_run_present",
            "A persisted supporting run is available.",
            Some(run.run_id.to_string()),
            None,
        ),
        None => add_blocker(
            &mut checks,
            &mut blockers,
            "no_supporting_run",
            "A supporting fixed research run is required before candidate review.",
            None,
            None,
        ),
    }

    match matrix {
        Some(matrix) => add_pass(
            &mut checks,
            "robustness_matrix_present",
            "A persisted robustness matrix is available.",
            Some(matrix.run_id.to_string()),
            None,
        ),
        None => add_blocker(
            &mut checks,
            &mut blockers,
            "no_robustness_matrix",
            "A robustness matrix is required before candidate review.",
            None,
            None,
        ),
    }

    if let Some(matrix) = matrix {
        if matches!(
            matrix.status,
            CrossAssetRobustnessStatus::Robust | CrossAssetRobustnessStatus::Promising
        ) {
            add_pass(
                &mut checks,
                "matrix_status_acceptable",
                "Matrix status is acceptable for research candidate review.",
                Some(matrix.status.as_str().to_string()),
                Some("ROBUST or PROMISING"),
            );
        } else {
            add_blocker(
                &mut checks,
                &mut blockers,
                "matrix_status_not_acceptable",
                "Matrix status is not strong enough for research candidate review.",
                Some(matrix.status.as_str().to_string()),
                Some("ROBUST or PROMISING"),
            );
        }
    }

    if execution_authority_none {
        add_pass(
            &mut checks,
            "execution_authority_none",
            "Preview has no paper, testnet, or live execution authority.",
            Some("NONE".to_string()),
            Some("NONE"),
        );
    } else {
        add_blocker(
            &mut checks,
            &mut blockers,
            "execution_authority_not_none",
            "Candidate gate preview must not run with execution authority.",
            Some("NOT_NONE".to_string()),
            Some("NONE"),
        );
    }

    if data_quality_good {
        add_pass(
            &mut checks,
            "data_quality_good",
            "Persisted cross-asset evidence was produced from GOOD aligned data coverage.",
            Some("GOOD".to_string()),
            Some("GOOD"),
        );
    } else {
        add_blocker(
            &mut checks,
            &mut blockers,
            "data_quality_not_good",
            "GOOD aligned cross-asset data quality is required before candidate review.",
            Some("NOT_GOOD".to_string()),
            Some("GOOD"),
        );
    }

    let top_ranking = matrix.and_then(|matrix| matrix.rankings.first());
    let evidence_run = supporting_run;
    let max_drawdown_pct = top_ranking
        .map(|ranking| ranking.max_drawdown_pct)
        .or_else(|| evidence_run.map(|run| run.max_drawdown_pct));
    let worst_window_pnl_pct = top_ranking
        .map(|ranking| ranking.worst_window_pnl_pct)
        .or_else(|| evidence_run.map(|run| run.worst_window_pnl_pct));
    let max_symbol_concentration_pct = top_ranking
        .map(|ranking| ranking.max_symbol_concentration_pct)
        .or_else(|| evidence_run.map(|run| run.max_symbol_concentration_pct));
    let total_trades = top_ranking
        .map(|ranking| ranking.total_trades)
        .or_else(|| evidence_run.map(|run| run.total_trades));
    let btc_trade_count = top_ranking
        .map(|ranking| ranking.btc_trade_count)
        .or_else(|| {
            evidence_run.map(|run| run.symbol_distribution.get("BTCUSDT").copied().unwrap_or(0))
        });

    if let Some(value) = max_drawdown_pct {
        if value < Decimal::new(-10, 0) {
            add_blocker(
                &mut checks,
                &mut blockers,
                "max_drawdown_above_10_pct",
                "Maximum drawdown exceeds the research candidate review limit.",
                Some(value.to_string()),
                Some(">= -10"),
            );
        } else {
            add_pass(
                &mut checks,
                "max_drawdown_within_10_pct",
                "Maximum drawdown is within the research candidate review limit.",
                Some(value.to_string()),
                Some(">= -10"),
            );
        }
    }

    if let Some(value) = worst_window_pnl_pct {
        if value < Decimal::new(-5, 0) {
            add_blocker(
                &mut checks,
                &mut blockers,
                "worst_window_below_minus_5_pct",
                "Worst window is below the research candidate review limit.",
                Some(value.to_string()),
                Some(">= -5"),
            );
        } else {
            add_pass(
                &mut checks,
                "worst_window_within_minus_5_pct",
                "Worst window is within the research candidate review limit.",
                Some(value.to_string()),
                Some(">= -5"),
            );
        }
    }

    if let Some(value) = max_symbol_concentration_pct {
        if value > Decimal::new(50, 0) {
            add_blocker(
                &mut checks,
                &mut blockers,
                "symbol_concentration_above_50_pct",
                "Maximum symbol concentration exceeds the research candidate review limit.",
                Some(value.to_string()),
                Some("<= 50"),
            );
        } else {
            add_pass(
                &mut checks,
                "symbol_concentration_within_50_pct",
                "Maximum symbol concentration is within the research candidate review limit.",
                Some(value.to_string()),
                Some("<= 50"),
            );
        }
    }

    let late_cell = matrix_cells
        .iter()
        .filter(|cell| {
            top_ranking.is_none_or(|ranking| cell.config_index == ranking.config_index)
                && (cell.window_label.contains("2025") || cell.window_start.year() >= 2025)
        })
        .min_by_key(|cell| cell.window_start);
    if let Some(cell) = late_cell {
        if cell.compounded_pnl_pct <= Decimal::ZERO {
            add_blocker(
                &mut checks,
                &mut blockers,
                "twenty_25_plus_negative",
                "2025+ evidence is not profitable at portfolio level.",
                Some(cell.compounded_pnl_pct.to_string()),
                Some("> 0"),
            );
        } else {
            add_pass(
                &mut checks,
                "twenty_25_plus_positive",
                "2025+ evidence is profitable at portfolio level.",
                Some(cell.compounded_pnl_pct.to_string()),
                Some("> 0"),
            );
        }
        if cell.median_trade_pnl_pct <= Decimal::ZERO && cell.compounded_pnl_pct > Decimal::ZERO {
            add_warning(
                &mut checks,
                &mut warnings,
                "twenty_25_plus_median_trade_weak",
                "2025+ median trade PnL is non-positive even though portfolio metrics pass.",
                Some(cell.median_trade_pnl_pct.to_string()),
                Some("> 0"),
            );
        }
        if cell.total_trades <= 60 && cell.total_trades >= 50 {
            add_warning(
                &mut checks,
                &mut warnings,
                "trade_count_barely_above_threshold",
                "2025+ trade count is only barely above the minimum evidence threshold.",
                Some(cell.total_trades.to_string()),
                Some("> 50"),
            );
        }
    } else if matrix.is_some() {
        add_warning(
            &mut checks,
            &mut warnings,
            "twenty_25_plus_cell_not_loaded",
            "2025+ cell details were not loaded; preview relies on aggregate matrix/run metrics.",
            None,
            None,
        );
    }

    let top_config_cells = matrix_cells
        .iter()
        .filter(|cell| top_ranking.is_none_or(|ranking| cell.config_index == ranking.config_index))
        .collect::<Vec<_>>();
    if one_window_dominates_preview_cells(&top_config_cells) {
        add_blocker(
            &mut checks,
            &mut blockers,
            "one_window_dominance_severe",
            "One window contributes more than 70% of positive window PnL.",
            Some("> 70%".to_string()),
            Some("<= 70%"),
        );
    } else if matrix.is_some() && !matrix_cells.is_empty() {
        add_pass(
            &mut checks,
            "one_window_dominance_not_severe",
            "Positive window PnL is not dominated by one window.",
            Some("<= 70%".to_string()),
            Some("<= 70%"),
        );
    }

    if existing_candidate_count > 0 {
        add_blocker(
            &mut checks,
            &mut blockers,
            "candidate_already_exists",
            "A candidate already exists for this strategy package.",
            Some(existing_candidate_count.to_string()),
            Some("0"),
        );
    } else {
        add_pass(
            &mut checks,
            "no_existing_candidate",
            "No existing candidate rows were found for this strategy package.",
            Some("0".to_string()),
            Some("0"),
        );
    }

    if let (Some(btc), Some(total)) = (btc_trade_count, total_trades) {
        if total > 0 {
            let btc_pct = Decimal::from(btc) * Decimal::new(100, 0) / Decimal::from(total);
            if btc_pct < Decimal::new(5, 0) {
                add_warning(
                    &mut checks,
                    &mut warnings,
                    "btc_participation_below_5_pct",
                    "BTC participation is low; this is a structural warning, not a hard blocker for cross-asset relative strength.",
                    Some(btc_pct.round_dp(4).to_string()),
                    Some(">= 5"),
                );
            } else {
                add_pass(
                    &mut checks,
                    "btc_participation_at_least_5_pct",
                    "BTC participation meets the soft participation threshold.",
                    Some(btc_pct.round_dp(4).to_string()),
                    Some(">= 5"),
                );
            }
        }
    }

    add_warning(
        &mut checks,
        &mut warnings,
        "implementation_research_only",
        "Package remains implementation-research only.",
        Some("true".to_string()),
        None,
    );
    add_warning(
        &mut checks,
        &mut warnings,
        "not_paper_testnet_live_ready",
        "Package is not paper, testnet, or live ready.",
        Some("paper=false,testnet=false,live=false".to_string()),
        None,
    );
    add_warning(
        &mut checks,
        &mut warnings,
        "no_live_shadow_observations",
        "No live shadow observation evidence exists for this package.",
        Some("0".to_string()),
        None,
    );
    add_warning(
        &mut checks,
        &mut warnings,
        "direct_execution_not_supported",
        "Direct execution is not supported for research packages.",
        Some("false".to_string()),
        None,
    );

    let status = if !blockers.is_empty() {
        CrossAssetCandidateGatePreviewStatus::Blocked
    } else if warnings.is_empty() {
        CrossAssetCandidateGatePreviewStatus::ReadyForResearchCandidateReview
    } else {
        CrossAssetCandidateGatePreviewStatus::NearReady
    };
    let recommended_action = match status {
        CrossAssetCandidateGatePreviewStatus::ReadyForResearchCandidateReview => {
            "DESIGN_CANDIDATE_CREATION_POLICY"
        }
        CrossAssetCandidateGatePreviewStatus::NearReady => "DESIGN_CANDIDATE_CREATION_POLICY",
        CrossAssetCandidateGatePreviewStatus::NotReady
        | CrossAssetCandidateGatePreviewStatus::Blocked => "RUN_ADDITIONAL_OUT_OF_SAMPLE",
    }
    .to_string();

    let mut forbidden_actions = relative_strength_continuation_v1_forbidden_actions();
    forbidden_actions.push("no_auto_candidate_creation".to_string());

    CrossAssetCandidateGatePreviewResult {
        package_id: request.strategy_package_id,
        run_id: supporting_run.map(|run| run.run_id).or(request.run_id),
        matrix_id: matrix.map(|matrix| matrix.run_id).or(request.matrix_id),
        status,
        checks,
        blockers,
        warnings,
        recommended_action,
        forbidden_actions,
        preview_only: true,
        generated_at: Utc::now(),
    }
}

fn one_window_dominates_preview_cells(cells: &[&CrossAssetRobustnessMatrixCell]) -> bool {
    let positives = cells
        .iter()
        .filter(|cell| cell.window_label != "combined")
        .map(|cell| cell.compounded_pnl_pct.max(Decimal::ZERO))
        .collect::<Vec<_>>();
    let total_positive = positives.iter().copied().sum::<Decimal>();
    if total_positive <= Decimal::ZERO {
        return false;
    }
    positives
        .into_iter()
        .max()
        .is_some_and(|value| value / total_positive > Decimal::new(70, 2))
}

#[allow(clippy::too_many_arguments)]
pub fn preview_cross_asset_candidate_creation_policy(
    request: CrossAssetCandidateCreationPolicyPreviewRequest,
    gate_preview: &CrossAssetCandidateGatePreviewResult,
    supporting_run: Option<&CrossAssetResearchResult>,
    matrix: Option<&CrossAssetRobustnessMatrixResult>,
    matrix_cells: &[CrossAssetRobustnessMatrixCell],
    existing_active_candidate_count: i64,
    route_enabled: bool,
    execution_authority_none: bool,
) -> CrossAssetCandidateCreationPolicyPreviewResult {
    let mut hard_requirements = Vec::new();
    let mut review_requirements = Vec::new();
    let mut risks = Vec::new();
    let mut warnings = Vec::new();

    let add_requirement = |items: &mut Vec<CrossAssetCandidateCreationRequirement>,
                           code: &str,
                           status: CrossAssetCandidateGateCheckStatus,
                           message: &str,
                           observed_value: Option<String>,
                           threshold: Option<&str>| {
        items.push(CrossAssetCandidateCreationRequirement {
            code: code.to_string(),
            status,
            message: message.to_string(),
            observed_value,
            threshold: threshold.map(ToString::to_string),
        });
    };
    let add_risk = |risks: &mut Vec<CrossAssetCandidateCreationRisk>,
                    code: &str,
                    severity: &str,
                    message: &str| {
        risks.push(CrossAssetCandidateCreationRisk {
            code: code.to_string(),
            severity: severity.to_string(),
            message: message.to_string(),
        });
    };

    if request.package_id != RELATIVE_STRENGTH_CONTINUATION_V1_ID {
        add_requirement(
            &mut hard_requirements,
            "supported_strategy_package",
            CrossAssetCandidateGateCheckStatus::Block,
            "Candidate creation policy preview currently supports only relative_strength_continuation_v1.",
            Some(request.package_id.clone()),
            Some(RELATIVE_STRENGTH_CONTINUATION_V1_ID),
        );
    } else {
        add_requirement(
            &mut hard_requirements,
            "supported_strategy_package",
            CrossAssetCandidateGateCheckStatus::Pass,
            "Strategy package is supported by the candidate creation policy preview.",
            Some(request.package_id.clone()),
            Some(RELATIVE_STRENGTH_CONTINUATION_V1_ID),
        );
    }

    if gate_preview.blockers.is_empty() {
        add_requirement(
            &mut hard_requirements,
            "package_gate_preview_has_no_blockers",
            CrossAssetCandidateGateCheckStatus::Pass,
            "Package gate preview has no blockers.",
            Some("0".to_string()),
            Some("0"),
        );
    } else {
        add_requirement(
            &mut hard_requirements,
            "package_gate_preview_has_no_blockers",
            CrossAssetCandidateGateCheckStatus::Block,
            "Package gate preview still has blockers.",
            Some(gate_preview.blockers.len().to_string()),
            Some("0"),
        );
    }

    match matrix {
        Some(matrix)
            if matches!(
                matrix.status,
                CrossAssetRobustnessStatus::Robust | CrossAssetRobustnessStatus::Promising
            ) =>
        {
            add_requirement(
                &mut hard_requirements,
                "matrix_status_robust_or_promising",
                CrossAssetCandidateGateCheckStatus::Pass,
                "Matrix status is acceptable for candidate creation policy review.",
                Some(matrix.status.as_str().to_string()),
                Some("ROBUST or PROMISING"),
            );
        }
        Some(matrix) => add_requirement(
            &mut hard_requirements,
            "matrix_status_robust_or_promising",
            CrossAssetCandidateGateCheckStatus::Block,
            "Matrix status is not strong enough for candidate creation policy review.",
            Some(matrix.status.as_str().to_string()),
            Some("ROBUST or PROMISING"),
        ),
        None => add_requirement(
            &mut hard_requirements,
            "matrix_present",
            CrossAssetCandidateGateCheckStatus::Block,
            "A robustness matrix is required before candidate creation policy review.",
            None,
            None,
        ),
    }

    if execution_authority_none {
        add_requirement(
            &mut hard_requirements,
            "execution_authority_none",
            CrossAssetCandidateGateCheckStatus::Pass,
            "Policy preview has no paper, testnet, or live execution authority.",
            Some("NONE".to_string()),
            Some("NONE"),
        );
    } else {
        add_requirement(
            &mut hard_requirements,
            "execution_authority_none",
            CrossAssetCandidateGateCheckStatus::Block,
            "Candidate creation policy preview must not run with execution authority.",
            Some("NOT_NONE".to_string()),
            Some("NONE"),
        );
    }

    let identity = relative_strength_continuation_v1_identity();
    let research_only = identity.scope == "cross_asset_research_only"
        && !identity.paper_executable
        && !identity.testnet_executable
        && !identity.live_executable;
    add_requirement(
        &mut hard_requirements,
        "package_is_research_only",
        if research_only {
            CrossAssetCandidateGateCheckStatus::Pass
        } else {
            CrossAssetCandidateGateCheckStatus::Block
        },
        "Package must remain research-only before candidate creation can be manually considered.",
        Some(format!(
            "scope={},paper={},testnet={},live={}",
            identity.scope,
            identity.paper_executable,
            identity.testnet_executable,
            identity.live_executable
        )),
        Some("research-only and no execution routes"),
    );

    add_requirement(
        &mut hard_requirements,
        "no_existing_active_candidate_for_same_package_config",
        if existing_active_candidate_count == 0 {
            CrossAssetCandidateGateCheckStatus::Pass
        } else {
            CrossAssetCandidateGateCheckStatus::Block
        },
        "No active candidate may already exist for the same package/config.",
        Some(existing_active_candidate_count.to_string()),
        Some("0"),
    );

    add_requirement(
        &mut hard_requirements,
        "no_paper_testnet_live_route_enabled",
        if route_enabled {
            CrossAssetCandidateGateCheckStatus::Block
        } else {
            CrossAssetCandidateGateCheckStatus::Pass
        },
        "No paper, testnet, or live route may be enabled for this research package.",
        Some(route_enabled.to_string()),
        Some("false"),
    );

    let top_ranking = matrix.and_then(|matrix| matrix.rankings.first());
    let max_drawdown_pct = top_ranking
        .map(|ranking| ranking.max_drawdown_pct)
        .or_else(|| supporting_run.map(|run| run.max_drawdown_pct));
    let worst_window_pnl_pct = top_ranking
        .map(|ranking| ranking.worst_window_pnl_pct)
        .or_else(|| supporting_run.map(|run| run.worst_window_pnl_pct));
    let max_symbol_concentration_pct = top_ranking
        .map(|ranking| ranking.max_symbol_concentration_pct)
        .or_else(|| supporting_run.map(|run| run.max_symbol_concentration_pct));
    let total_trades = top_ranking
        .map(|ranking| ranking.total_trades)
        .or_else(|| supporting_run.map(|run| run.total_trades));
    let btc_trade_count = top_ranking
        .map(|ranking| ranking.btc_trade_count)
        .or_else(|| {
            supporting_run.map(|run| run.symbol_distribution.get("BTCUSDT").copied().unwrap_or(0))
        });

    match max_drawdown_pct {
        Some(value) if value >= Decimal::new(-10, 0) => add_requirement(
            &mut hard_requirements,
            "max_drawdown_lte_10_pct",
            CrossAssetCandidateGateCheckStatus::Pass,
            "Maximum drawdown is within the candidate creation policy threshold.",
            Some(value.to_string()),
            Some(">= -10"),
        ),
        Some(value) => add_requirement(
            &mut hard_requirements,
            "max_drawdown_lte_10_pct",
            CrossAssetCandidateGateCheckStatus::Block,
            "Maximum drawdown exceeds the candidate creation policy threshold.",
            Some(value.to_string()),
            Some(">= -10"),
        ),
        None => add_requirement(
            &mut hard_requirements,
            "max_drawdown_lte_10_pct",
            CrossAssetCandidateGateCheckStatus::Block,
            "Maximum drawdown evidence is missing.",
            None,
            Some(">= -10"),
        ),
    }

    match worst_window_pnl_pct {
        Some(value) if value >= Decimal::new(-5, 0) => add_requirement(
            &mut hard_requirements,
            "worst_window_gte_minus_5_pct",
            CrossAssetCandidateGateCheckStatus::Pass,
            "Worst window is within the candidate creation policy threshold.",
            Some(value.to_string()),
            Some(">= -5"),
        ),
        Some(value) => add_requirement(
            &mut hard_requirements,
            "worst_window_gte_minus_5_pct",
            CrossAssetCandidateGateCheckStatus::Block,
            "Worst window breaches the candidate creation policy threshold.",
            Some(value.to_string()),
            Some(">= -5"),
        ),
        None => add_requirement(
            &mut hard_requirements,
            "worst_window_gte_minus_5_pct",
            CrossAssetCandidateGateCheckStatus::Block,
            "Worst-window evidence is missing.",
            None,
            Some(">= -5"),
        ),
    }

    match max_symbol_concentration_pct {
        Some(value) if value <= Decimal::new(50, 0) => add_requirement(
            &mut hard_requirements,
            "max_symbol_concentration_lte_50_pct",
            CrossAssetCandidateGateCheckStatus::Pass,
            "Maximum symbol concentration is within the candidate creation policy threshold.",
            Some(value.to_string()),
            Some("<= 50"),
        ),
        Some(value) => add_requirement(
            &mut hard_requirements,
            "max_symbol_concentration_lte_50_pct",
            CrossAssetCandidateGateCheckStatus::Block,
            "Maximum symbol concentration exceeds the candidate creation policy threshold.",
            Some(value.to_string()),
            Some("<= 50"),
        ),
        None => add_requirement(
            &mut hard_requirements,
            "max_symbol_concentration_lte_50_pct",
            CrossAssetCandidateGateCheckStatus::Block,
            "Symbol concentration evidence is missing.",
            None,
            Some("<= 50"),
        ),
    }

    let top_config_cells = matrix_cells
        .iter()
        .filter(|cell| top_ranking.is_none_or(|ranking| cell.config_index == ranking.config_index))
        .collect::<Vec<_>>();
    let early_cell = top_config_cells
        .iter()
        .copied()
        .find(|cell| cell.window_label.contains("2023") || cell.window_start.year() < 2025);
    let late_cell = top_config_cells
        .iter()
        .copied()
        .find(|cell| cell.window_label.contains("2025") || cell.window_start.year() >= 2025);

    match early_cell {
        Some(cell) if cell.compounded_pnl_pct > Decimal::ZERO => add_requirement(
            &mut hard_requirements,
            "positive_2023_2024",
            CrossAssetCandidateGateCheckStatus::Pass,
            "2023-2024 evidence is positive.",
            Some(cell.compounded_pnl_pct.to_string()),
            Some("> 0"),
        ),
        Some(cell) => add_requirement(
            &mut hard_requirements,
            "positive_2023_2024",
            CrossAssetCandidateGateCheckStatus::Block,
            "2023-2024 evidence is not positive.",
            Some(cell.compounded_pnl_pct.to_string()),
            Some("> 0"),
        ),
        None => add_requirement(
            &mut hard_requirements,
            "positive_2023_2024",
            CrossAssetCandidateGateCheckStatus::Block,
            "2023-2024 evidence cell is missing.",
            None,
            Some("> 0"),
        ),
    }

    match late_cell {
        Some(cell) if cell.compounded_pnl_pct > Decimal::ZERO => add_requirement(
            &mut hard_requirements,
            "positive_2025_plus",
            CrossAssetCandidateGateCheckStatus::Pass,
            "2025+ evidence is positive.",
            Some(cell.compounded_pnl_pct.to_string()),
            Some("> 0"),
        ),
        Some(cell) => add_requirement(
            &mut hard_requirements,
            "positive_2025_plus",
            CrossAssetCandidateGateCheckStatus::Block,
            "2025+ evidence is not positive.",
            Some(cell.compounded_pnl_pct.to_string()),
            Some("> 0"),
        ),
        None => add_requirement(
            &mut hard_requirements,
            "positive_2025_plus",
            CrossAssetCandidateGateCheckStatus::Block,
            "2025+ evidence cell is missing.",
            None,
            Some("> 0"),
        ),
    }

    if one_window_dominates_preview_cells(&top_config_cells) {
        add_requirement(
            &mut hard_requirements,
            "no_severe_one_window_dominance",
            CrossAssetCandidateGateCheckStatus::Block,
            "One window contributes more than 70% of positive window PnL.",
            Some("> 70%".to_string()),
            Some("<= 70%"),
        );
    } else if !top_config_cells.is_empty() {
        add_requirement(
            &mut hard_requirements,
            "no_severe_one_window_dominance",
            CrossAssetCandidateGateCheckStatus::Pass,
            "Positive window PnL is not severely dominated by one window.",
            Some("<= 70%".to_string()),
            Some("<= 70%"),
        );
    } else {
        add_requirement(
            &mut hard_requirements,
            "no_severe_one_window_dominance",
            CrossAssetCandidateGateCheckStatus::Block,
            "Window-level evidence is missing.",
            None,
            Some("<= 70%"),
        );
    }

    if let Some(cell) = late_cell {
        let strict_status =
            |condition: bool, strictness: CrossAssetCandidateCreationPolicyStrictness| {
                if condition {
                    CrossAssetCandidateGateCheckStatus::Pass
                } else if strictness == CrossAssetCandidateCreationPolicyStrictness::Conservative {
                    CrossAssetCandidateGateCheckStatus::Block
                } else {
                    CrossAssetCandidateGateCheckStatus::Warn
                }
            };
        add_requirement(
            &mut review_requirements,
            "twenty_25_plus_median_trade_positive",
            strict_status(
                cell.median_trade_pnl_pct > Decimal::ZERO,
                request.strictness,
            ),
            "2025+ median trade should be positive before manually creating a research candidate.",
            Some(cell.median_trade_pnl_pct.to_string()),
            Some("> 0"),
        );
        add_requirement(
            &mut review_requirements,
            "twenty_25_plus_trade_count_comfortably_above_minimum",
            strict_status(cell.total_trades > 60, request.strictness),
            "2025+ trade count should be comfortably above the minimum, not barely over it.",
            Some(cell.total_trades.to_string()),
            Some("> 60"),
        );
    } else {
        add_requirement(
            &mut review_requirements,
            "twenty_25_plus_review_cell_present",
            if request.strictness == CrossAssetCandidateCreationPolicyStrictness::Conservative {
                CrossAssetCandidateGateCheckStatus::Block
            } else {
                CrossAssetCandidateGateCheckStatus::Warn
            },
            "2025+ review cell is missing.",
            None,
            None,
        );
    }

    if let (Some(btc), Some(total)) = (btc_trade_count, total_trades) {
        let btc_pct = if total > 0 {
            Decimal::from(btc) * Decimal::new(100, 0) / Decimal::from(total)
        } else {
            Decimal::ZERO
        };
        let btc_ok = btc_pct >= Decimal::new(5, 0);
        let btc_status = if btc_ok {
            CrossAssetCandidateGateCheckStatus::Pass
        } else if request.strictness == CrossAssetCandidateCreationPolicyStrictness::Conservative {
            CrossAssetCandidateGateCheckStatus::Block
        } else {
            CrossAssetCandidateGateCheckStatus::Warn
        };
        add_requirement(
            &mut review_requirements,
            "btc_participation_at_least_5_pct",
            btc_status,
            "BTC participation should be at least 5% for cross-asset generalization.",
            Some(btc_pct.round_dp(4).to_string()),
            Some(">= 5"),
        );
    }

    add_requirement(
        &mut review_requirements,
        "live_shadow_observations_exist",
        CrossAssetCandidateGateCheckStatus::Warn,
        "Live shadow observations are optional for research candidate creation but must remain visible.",
        Some("0".to_string()),
        Some("> 0 optional"),
    );
    add_requirement(
        &mut review_requirements,
        "direct_execution_unsupported",
        CrossAssetCandidateGateCheckStatus::Warn,
        "Direct execution is unsupported for this research package.",
        Some("false".to_string()),
        Some("false"),
    );

    for requirement in hard_requirements
        .iter()
        .chain(review_requirements.iter())
        .filter(|requirement| requirement.status != CrossAssetCandidateGateCheckStatus::Pass)
    {
        warnings.push(requirement.code.clone());
    }
    for warning in &gate_preview.warnings {
        if !warnings.contains(&warning.code) {
            warnings.push(warning.code.clone());
        }
    }
    warnings.sort();
    warnings.dedup();

    for requirement in hard_requirements.iter().chain(review_requirements.iter()) {
        match requirement.status {
            CrossAssetCandidateGateCheckStatus::Block => {
                add_risk(&mut risks, &requirement.code, "HIGH", &requirement.message)
            }
            CrossAssetCandidateGateCheckStatus::Warn => add_risk(
                &mut risks,
                &requirement.code,
                "MEDIUM",
                &requirement.message,
            ),
            CrossAssetCandidateGateCheckStatus::Pass => {}
        }
    }

    let hard_blocked = hard_requirements
        .iter()
        .any(|requirement| requirement.status == CrossAssetCandidateGateCheckStatus::Block);
    let review_blocked = review_requirements
        .iter()
        .any(|requirement| requirement.status == CrossAssetCandidateGateCheckStatus::Block);
    let review_warned = review_requirements
        .iter()
        .any(|requirement| requirement.status == CrossAssetCandidateGateCheckStatus::Warn);
    let status = if request.package_id != RELATIVE_STRENGTH_CONTINUATION_V1_ID {
        CrossAssetCandidateCreationPolicyStatus::PolicyNotApplicable
    } else if hard_blocked || review_blocked {
        CrossAssetCandidateCreationPolicyStatus::PolicyBlocked
    } else if request.strictness == CrossAssetCandidateCreationPolicyStrictness::Balanced
        && review_warned
    {
        CrossAssetCandidateCreationPolicyStatus::PolicyNeedsReview
    } else if request.strictness == CrossAssetCandidateCreationPolicyStrictness::Experimental
        && !hard_blocked
    {
        CrossAssetCandidateCreationPolicyStatus::PolicyReadyForManualCreate
    } else if review_warned {
        CrossAssetCandidateCreationPolicyStatus::PolicyNeedsReview
    } else {
        CrossAssetCandidateCreationPolicyStatus::PolicyReadyForManualCreate
    };

    let recommended_action = match status {
        CrossAssetCandidateCreationPolicyStatus::PolicyBlocked => {
            if hard_blocked {
                "RUN_MORE_OUT_OF_SAMPLE"
            } else {
                "DO_NOT_CREATE_CANDIDATE"
            }
        }
        CrossAssetCandidateCreationPolicyStatus::PolicyNeedsReview => {
            "HUMAN_REVIEW_CANDIDATE_POLICY"
        }
        CrossAssetCandidateCreationPolicyStatus::PolicyReadyForManualCreate => {
            "HUMAN_REVIEW_CANDIDATE_POLICY"
        }
        CrossAssetCandidateCreationPolicyStatus::PolicyNotApplicable => "DO_NOT_CREATE_CANDIDATE",
    }
    .to_string();

    let mut forbidden_actions = relative_strength_continuation_v1_forbidden_actions();
    forbidden_actions.extend([
        "no_auto_candidate_creation".to_string(),
        "no_candidate_acceptance".to_string(),
        "no_shadow_promotion".to_string(),
        "no_runner_config_change".to_string(),
        "no_scheduled_job_enablement".to_string(),
        "no_ready_for_testnet_mark".to_string(),
    ]);
    forbidden_actions.sort();
    forbidden_actions.dedup();

    CrossAssetCandidateCreationPolicyPreviewResult {
        package_id: request.package_id,
        run_id: supporting_run.map(|run| run.run_id).or(request.run_id),
        matrix_id: matrix.map(|matrix| matrix.run_id).or(request.matrix_id),
        strictness: request.strictness,
        status,
        hard_requirements,
        review_requirements,
        risks,
        warnings,
        recommended_action,
        forbidden_actions,
        preview_only: true,
        no_candidate_created: true,
        generated_at: Utc::now(),
    }
}

pub fn relative_strength_continuation_v1_default_request(
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    correlation_id: Option<Uuid>,
) -> CrossAssetResearchRequest {
    CrossAssetResearchRequest {
        strategy_kind: CrossAssetStrategyKind::RelativeStrengthContinuationV1Research,
        symbols: default_cross_asset_robustness_symbols(),
        timeframe: "4h".to_string(),
        start_time,
        end_time,
        ranking_lookback_hours: 12,
        rank_metric: CrossAssetRankMetric::VolAdjustedReturn,
        min_top_return_pct: Decimal::ZERO,
        min_rank_spread_pct: Decimal::new(3, 0),
        holding_hours: 12,
        fee_bps: default_cross_asset_fee_bps(),
        slippage_bps: default_cross_asset_slippage_bps(),
        one_active_position: true,
        sizing_mode: CrossAssetSizingMode::VolatilityNormalized,
        min_weight: default_cross_asset_min_weight(),
        max_weight: default_cross_asset_max_weight(),
        market_filter: CrossAssetMarketFilter::Basket24hReturnGt {
            threshold_pct: Decimal::new(-2, 0),
        },
        vol_filter: CrossAssetVolFilter::AssetNotExtremeVsBasket {
            max_ratio: Decimal::new(15, 1),
        },
        overextension_filter: CrossAssetOverextensionFilter::None,
        exit_rule: CrossAssetExitRule::FixedHold,
        correlation_id,
    }
}

pub fn relative_strength_continuation_v1_default_matrix_windows(
    end_time: DateTime<Utc>,
) -> Vec<CrossAssetRobustnessMatrixWindow> {
    let start_time = Utc.with_ymd_and_hms(2023, 3, 25, 0, 0, 0).unwrap();
    let split_time = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    vec![
        CrossAssetRobustnessMatrixWindow {
            label: "2023_2024".to_string(),
            start_time,
            end_time: split_time,
        },
        CrossAssetRobustnessMatrixWindow {
            label: "2025_plus".to_string(),
            start_time: split_time,
            end_time,
        },
        CrossAssetRobustnessMatrixWindow {
            label: "combined".to_string(),
            start_time,
            end_time,
        },
    ]
}

pub fn relative_strength_continuation_v1_default_robustness_matrix_request(
    windows: Vec<CrossAssetRobustnessMatrixWindow>,
    correlation_id: Option<Uuid>,
) -> CrossAssetRobustnessMatrixRequest {
    CrossAssetRobustnessMatrixRequest {
        strategy_kind: CrossAssetStrategyKind::RelativeStrengthContinuationV1Research,
        symbols: default_cross_asset_robustness_symbols(),
        timeframe: "4h".to_string(),
        windows,
        ranking_lookback_hours: vec![12, 24],
        rank_metrics: vec![
            CrossAssetRankMetric::VolAdjustedReturn,
            CrossAssetRankMetric::RawReturn,
        ],
        min_top_return_pct: Decimal::ZERO,
        min_rank_spread_pct: vec![Decimal::new(2, 0), Decimal::new(3, 0), Decimal::new(5, 0)],
        holding_hours: vec![12, 24],
        fee_bps: default_cross_asset_fee_bps(),
        slippage_bps: default_cross_asset_slippage_bps(),
        one_active_position: true,
        sizing_modes: vec![CrossAssetSizingMode::VolatilityNormalized],
        min_weight: default_cross_asset_min_weight(),
        max_weight: default_cross_asset_max_weight(),
        market_filters: vec![
            CrossAssetMarketFilter::Basket24hReturnGt {
                threshold_pct: Decimal::new(-2, 0),
            },
            CrossAssetMarketFilter::Basket72hReturnGt {
                threshold_pct: Decimal::new(-5, 0),
            },
            CrossAssetMarketFilter::None,
        ],
        vol_filters: vec![
            CrossAssetVolFilter::AssetNotExtremeVsBasket {
                max_ratio: Decimal::new(15, 1),
            },
            CrossAssetVolFilter::None,
        ],
        overextension_filters: vec![CrossAssetOverextensionFilter::None],
        exit_rules: vec![
            CrossAssetExitRule::FixedHold,
            CrossAssetExitRule::StopPct {
                stop_pct: Decimal::new(3, 0),
            },
            CrossAssetExitRule::StopPct {
                stop_pct: Decimal::new(5, 0),
            },
        ],
        max_configs: 432,
        correlation_id,
    }
}

pub fn relative_strength_continuation_v1_package(
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> CrossAssetStrategyPackage {
    CrossAssetStrategyPackage {
        identity: relative_strength_continuation_v1_identity(),
        default_config: relative_strength_continuation_v1_default_request(
            start_time, end_time, None,
        ),
        allowed_next_actions: relative_strength_continuation_v1_allowed_next_actions(),
        forbidden_actions: relative_strength_continuation_v1_forbidden_actions(),
    }
}

pub fn is_relative_strength_continuation_v1_default_request(
    request: &CrossAssetResearchRequest,
) -> bool {
    request.strategy_kind == CrossAssetStrategyKind::RelativeStrengthContinuationV1Research
        && request.symbols == default_cross_asset_robustness_symbols()
        && request.timeframe == "4h"
        && request.ranking_lookback_hours == 12
        && request.rank_metric == CrossAssetRankMetric::VolAdjustedReturn
        && request.min_top_return_pct == Decimal::ZERO
        && request.min_rank_spread_pct == Decimal::new(3, 0)
        && request.holding_hours == 12
        && request.fee_bps == default_cross_asset_fee_bps()
        && request.slippage_bps == default_cross_asset_slippage_bps()
        && request.one_active_position
        && request.sizing_mode == CrossAssetSizingMode::VolatilityNormalized
        && request.min_weight == default_cross_asset_min_weight()
        && request.max_weight == default_cross_asset_max_weight()
        && request.market_filter
            == CrossAssetMarketFilter::Basket24hReturnGt {
                threshold_pct: Decimal::new(-2, 0),
            }
        && request.vol_filter
            == CrossAssetVolFilter::AssetNotExtremeVsBasket {
                max_ratio: Decimal::new(15, 1),
            }
        && request.overextension_filter == CrossAssetOverextensionFilter::None
        && request.exit_rule == CrossAssetExitRule::FixedHold
}

pub fn is_relative_strength_continuation_v1_default_matrix_ranking(
    ranking: &CrossAssetRobustnessRanking,
) -> bool {
    ranking.config.ranking_lookback_hours == 12
        && ranking.config.rank_metric == CrossAssetRankMetric::VolAdjustedReturn
        && ranking.config.min_rank_spread_pct == Decimal::new(3, 0)
        && ranking.config.holding_hours == 12
        && ranking.config.sizing_mode == CrossAssetSizingMode::VolatilityNormalized
        && ranking.config.market_filter
            == CrossAssetMarketFilter::Basket24hReturnGt {
                threshold_pct: Decimal::new(-2, 0),
            }
        && ranking.config.vol_filter
            == CrossAssetVolFilter::AssetNotExtremeVsBasket {
                max_ratio: Decimal::new(15, 1),
            }
        && ranking.config.overextension_filter == CrossAssetOverextensionFilter::None
        && ranking.config.exit_rule == CrossAssetExitRule::FixedHold
}

pub fn evaluate_cross_asset_shadow_observation_preview(
    candidate_id: Uuid,
    package_id: String,
    candidate_status: ResearchCandidateStatus,
    candles_by_symbol: BTreeMap<String, Vec<Candle>>,
    latest_evaluated_candle_time: Option<DateTime<Utc>>,
    generated_at: DateTime<Utc>,
) -> CrossAssetShadowObservationPreviewResult {
    let mut request =
        relative_strength_continuation_v1_default_request(generated_at, generated_at, None);
    request.correlation_id = None;
    let symbols = request
        .symbols
        .iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .collect::<Vec<_>>();
    let interval = match request.parsed_timeframe() {
        Ok(value) => value,
        Err(err) => {
            return CrossAssetShadowObservationPreviewResult {
                candidate_id,
                package_id,
                candidate_status,
                latest_available_aligned_candle_time: None,
                latest_evaluated_candle_time,
                would_create_observation: false,
                decision: CrossAssetShadowObservationDecision::Error,
                status: CrossAssetShadowObservationStatus::Error,
                selected_symbol: None,
                ranking_snapshot: Vec::new(),
                rank_spread_pct: None,
                market_filter_passed: false,
                vol_filter_passed: false,
                reason: err.to_string(),
                warnings: vec!["invalid_cross_asset_shadow_observation_config".to_string()],
                no_mutation: true,
                generated_at,
            };
        }
    };
    let required_lookback = match (
        cross_asset_hours_to_candles(request.ranking_lookback_hours, interval),
        cross_asset_hours_to_candles(6, interval),
        cross_asset_hours_to_candles(24, interval),
        cross_asset_hours_to_candles(72, interval),
    ) {
        (Ok(ranking), Ok(six), Ok(twenty_four), Ok(seventy_two)) => {
            ranking.max(six).max(twenty_four).max(seventy_two)
        }
        _ => 0,
    };

    let mut aligned_times: Option<BTreeSet<DateTime<Utc>>> = None;
    let mut normalized = BTreeMap::new();
    let mut warnings = Vec::new();
    for symbol in &symbols {
        let mut candles = candles_by_symbol
            .get(symbol)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|candle| candle.interval == interval && candle.is_closed)
            .collect::<Vec<_>>();
        candles.sort_by_key(|candle| candle.open_time);
        if candles.is_empty() {
            warnings.push(format!("missing_closed_candles_{symbol}"));
        }
        let times = candles
            .iter()
            .map(|candle| candle.open_time)
            .collect::<BTreeSet<_>>();
        aligned_times = Some(match aligned_times {
            Some(existing) => existing.intersection(&times).copied().collect(),
            None => times,
        });
        normalized.insert(symbol.clone(), candles);
    }

    let aligned_times = aligned_times.unwrap_or_default();
    let latest_aligned = aligned_times.iter().next_back().copied();
    let Some(latest_aligned_time) = latest_aligned else {
        return CrossAssetShadowObservationPreviewResult {
            candidate_id,
            package_id,
            candidate_status,
            latest_available_aligned_candle_time: None,
            latest_evaluated_candle_time,
            would_create_observation: false,
            decision: CrossAssetShadowObservationDecision::SkippedStaleCandles,
            status: CrossAssetShadowObservationStatus::Skipped,
            selected_symbol: None,
            ranking_snapshot: Vec::new(),
            rank_spread_pct: None,
            market_filter_passed: false,
            vol_filter_passed: false,
            reason: "No aligned closed 4h candle exists across the cross-asset basket.".to_string(),
            warnings,
            no_mutation: true,
            generated_at,
        };
    };

    if latest_evaluated_candle_time.is_some_and(|time| time >= latest_aligned_time) {
        return CrossAssetShadowObservationPreviewResult {
            candidate_id,
            package_id,
            candidate_status,
            latest_available_aligned_candle_time: Some(latest_aligned_time),
            latest_evaluated_candle_time,
            would_create_observation: false,
            decision: CrossAssetShadowObservationDecision::SkippedNoNewCandle,
            status: CrossAssetShadowObservationStatus::Skipped,
            selected_symbol: None,
            ranking_snapshot: Vec::new(),
            rank_spread_pct: None,
            market_filter_passed: false,
            vol_filter_passed: false,
            reason: "Latest aligned candle has already been evaluated for this candidate."
                .to_string(),
            warnings,
            no_mutation: true,
            generated_at,
        };
    }

    let needed = required_lookback + 1;
    let times = aligned_times
        .iter()
        .copied()
        .filter(|time| *time <= latest_aligned_time)
        .collect::<Vec<_>>();
    if needed == 0 || times.len() < needed {
        warnings.push("insufficient_cross_asset_lookback".to_string());
        return CrossAssetShadowObservationPreviewResult {
            candidate_id,
            package_id,
            candidate_status,
            latest_available_aligned_candle_time: Some(latest_aligned_time),
            latest_evaluated_candle_time,
            would_create_observation: false,
            decision: CrossAssetShadowObservationDecision::SkippedStaleCandles,
            status: CrossAssetShadowObservationStatus::Skipped,
            selected_symbol: None,
            ranking_snapshot: Vec::new(),
            rank_spread_pct: None,
            market_filter_passed: false,
            vol_filter_passed: false,
            reason: "Not enough aligned 4h candles for ranking, 24h basket return, and volatility features.".to_string(),
            warnings,
            no_mutation: true,
            generated_at,
        };
    }
    let times = times[times.len() - needed..].to_vec();
    if times.windows(2).any(|pair| {
        pair[1].signed_duration_since(pair[0]).num_seconds() != interval.duration().num_seconds()
    }) {
        warnings.push("latest_aligned_window_contains_gap".to_string());
        return CrossAssetShadowObservationPreviewResult {
            candidate_id,
            package_id,
            candidate_status,
            latest_available_aligned_candle_time: Some(latest_aligned_time),
            latest_evaluated_candle_time,
            would_create_observation: false,
            decision: CrossAssetShadowObservationDecision::SkippedStaleCandles,
            status: CrossAssetShadowObservationStatus::Skipped,
            selected_symbol: None,
            ranking_snapshot: Vec::new(),
            rank_spread_pct: None,
            market_filter_passed: false,
            vol_filter_passed: false,
            reason: "Latest aligned 4h candle window contains a gap.".to_string(),
            warnings,
            no_mutation: true,
            generated_at,
        };
    }

    let mut candles = BTreeMap::new();
    for symbol in &symbols {
        let by_time = normalized
            .remove(symbol)
            .unwrap_or_default()
            .into_iter()
            .map(|candle| (candle.open_time, candle))
            .collect::<BTreeMap<_, _>>();
        candles.insert(
            symbol.clone(),
            times
                .iter()
                .filter_map(|time| by_time.get(time).cloned())
                .collect::<Vec<_>>(),
        );
    }

    let feature_lookbacks = CrossAssetFeatureLookbacks {
        ranking: cross_asset_hours_to_candles(request.ranking_lookback_hours, interval)
            .unwrap_or(1),
        six_hours: cross_asset_hours_to_candles(6, interval).unwrap_or(1),
        twenty_four_hours: cross_asset_hours_to_candles(24, interval).unwrap_or(1),
        seventy_two_hours: cross_asset_hours_to_candles(72, interval).unwrap_or(1),
    };
    let features = compute_cross_asset_features(&symbols, &candles, feature_lookbacks);
    let index = times.len() - 1;
    let market_filter_passed =
        cross_asset_market_filter_passes(&request, &symbols, &features, index)
            && basket_vol_filter_passes(
                &request,
                &symbols,
                &features,
                index,
                match &request.vol_filter {
                    CrossAssetVolFilter::BasketVolBelowPercentile { percentile } => {
                        basket_vol_percentile(&symbols, &features, *percentile)
                    }
                    _ => None,
                },
            );
    let ranking = cross_asset_ranking(&request, &symbols, &features, index);
    let ranking_snapshot = ranking
        .iter()
        .enumerate()
        .map(|(idx, row)| {
            let feature = features.get(&row.symbol).and_then(|rows| rows.get(index));
            CrossAssetShadowObservationRankingRow {
                symbol: row.symbol.clone(),
                rank: i32::try_from(idx + 1).unwrap_or(i32::MAX),
                score: row.score,
                return_pct: row.return_pct,
                return_24h_pct: feature.and_then(|value| value.return_24h).map(percent),
                realized_vol_24h_pct: feature.and_then(|value| value.vol_24h).map(percent),
            }
        })
        .collect::<Vec<_>>();
    let rank_spread_pct = ranking
        .first()
        .zip(ranking.get(1))
        .map(|(top, second)| top.return_pct - second.return_pct);
    let selected_symbol = ranking.first().map(|row| row.symbol.clone());
    let vol_filter_passed = selected_symbol.as_deref().is_some_and(|symbol| {
        cross_asset_vol_filter_passes(&request, &symbols, &features, index, symbol)
            && cross_asset_overextension_filter_passes(&request, &features, index, symbol)
    });
    let signal_passed = market_filter_passed
        && vol_filter_passed
        && ranking.len() == symbols.len()
        && ranking
            .first()
            .is_some_and(|top| top.return_pct > request.min_top_return_pct)
        && rank_spread_pct.is_some_and(|spread| spread >= request.min_rank_spread_pct);
    let decision = if signal_passed {
        CrossAssetShadowObservationDecision::WouldSelect
    } else {
        CrossAssetShadowObservationDecision::NoSignal
    };
    let reason = if signal_passed {
        "Hypothetical portfolio selection only; no order path is invoked.".to_string()
    } else if !market_filter_passed {
        "Market filter did not pass for the latest aligned candle.".to_string()
    } else if !vol_filter_passed {
        "Volatility or overextension filter did not pass for the top-ranked symbol.".to_string()
    } else {
        "Ranking thresholds did not produce a portfolio selection.".to_string()
    };

    CrossAssetShadowObservationPreviewResult {
        candidate_id,
        package_id,
        candidate_status,
        latest_available_aligned_candle_time: Some(latest_aligned_time),
        latest_evaluated_candle_time,
        would_create_observation: false,
        decision,
        status: CrossAssetShadowObservationStatus::PreviewOnly,
        selected_symbol: signal_passed.then_some(selected_symbol).flatten(),
        ranking_snapshot,
        rank_spread_pct,
        market_filter_passed,
        vol_filter_passed,
        reason,
        warnings,
        no_mutation: true,
        generated_at,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetPortfolioRankingSnapshot {
    pub signal_time: DateTime<Utc>,
    pub selected_symbol: String,
    pub top_score: Decimal,
    pub second_score: Decimal,
    pub rank_spread_pct: Decimal,
    pub top_return_pct: Decimal,
    pub return_6h_pct: Option<Decimal>,
    pub return_24h_pct: Option<Decimal>,
    pub return_72h_pct: Option<Decimal>,
    pub realized_vol_24h_pct: Option<Decimal>,
    pub realized_vol_72h_pct: Option<Decimal>,
    pub distance_from_72h_high_pct: Option<Decimal>,
    pub basket_return_24h_pct: Option<Decimal>,
    pub basket_return_72h_pct: Option<Decimal>,
    pub basket_vol_72h_pct: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetPortfolioTrade {
    pub id: Uuid,
    pub run_id: Uuid,
    pub trade_index: i32,
    pub symbol: String,
    pub signal_time: DateTime<Utc>,
    pub entry_time: DateTime<Utc>,
    pub exit_time: DateTime<Utc>,
    pub entry_price: Decimal,
    pub exit_price: Decimal,
    pub weight: Decimal,
    pub gross_pnl_pct: Decimal,
    pub net_pnl_pct: Decimal,
    pub fee_slippage_drag_pct: Decimal,
    pub exit_reason: String,
    pub ranking_snapshot: CrossAssetPortfolioRankingSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetPortfolioWindow {
    pub id: Uuid,
    pub run_id: Uuid,
    pub window_index: i32,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub trade_count: i32,
    pub net_pnl_pct: Decimal,
    pub compounded_pnl_pct: Decimal,
    pub avg_trade_pnl_pct: Decimal,
    pub median_trade_pnl_pct: Decimal,
    pub win_rate: Decimal,
    pub max_drawdown_pct: Decimal,
    pub worst_trade_pct: Decimal,
    pub best_trade_pct: Decimal,
    pub symbol_distribution: BTreeMap<String, i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetResearchResult {
    pub run_id: Uuid,
    pub strategy_kind: CrossAssetStrategyKind,
    pub request: CrossAssetResearchRequest,
    pub status: CrossAssetResearchStatus,
    pub portfolio_status: CrossAssetPortfolioStatus,
    pub recommendation: CrossAssetResearchRecommendation,
    pub total_trades: i32,
    pub net_pnl_pct: Decimal,
    pub compounded_pnl_pct: Decimal,
    pub avg_trade_pnl_pct: Decimal,
    pub median_trade_pnl_pct: Decimal,
    pub win_rate: Decimal,
    pub max_drawdown_pct: Decimal,
    pub worst_trade_pct: Decimal,
    pub best_trade_pct: Decimal,
    pub fee_slippage_drag_pct: Decimal,
    pub symbol_distribution: BTreeMap<String, i32>,
    pub max_symbol_concentration_pct: Decimal,
    pub window_count: i32,
    pub profitable_windows: i32,
    pub worst_window_pnl_pct: Decimal,
    pub median_window_pnl_pct: Decimal,
    pub warnings: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub correlation_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetResearchRun {
    pub result: CrossAssetResearchResult,
    pub trades: Vec<CrossAssetPortfolioTrade>,
    pub windows: Vec<CrossAssetPortfolioWindow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetRobustnessMatrixWindow {
    pub label: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetRobustnessConfig {
    pub ranking_lookback_hours: u32,
    pub rank_metric: CrossAssetRankMetric,
    pub min_rank_spread_pct: Decimal,
    pub holding_hours: u32,
    pub sizing_mode: CrossAssetSizingMode,
    pub market_filter: CrossAssetMarketFilter,
    pub vol_filter: CrossAssetVolFilter,
    pub overextension_filter: CrossAssetOverextensionFilter,
    pub exit_rule: CrossAssetExitRule,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrossAssetRobustnessStatus {
    Robust,
    Promising,
    Weak,
    OverfitRisk,
    Negative,
    TooSparse,
}

impl CrossAssetRobustnessStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Robust => "ROBUST",
            Self::Promising => "PROMISING",
            Self::Weak => "WEAK",
            Self::OverfitRisk => "OVERFIT_RISK",
            Self::Negative => "NEGATIVE",
            Self::TooSparse => "TOO_SPARSE",
        }
    }
}

impl std::str::FromStr for CrossAssetRobustnessStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "ROBUST" => Ok(Self::Robust),
            "PROMISING" => Ok(Self::Promising),
            "WEAK" => Ok(Self::Weak),
            "OVERFIT_RISK" => Ok(Self::OverfitRisk),
            "NEGATIVE" => Ok(Self::Negative),
            "TOO_SPARSE" => Ok(Self::TooSparse),
            other => Err(CoreError::UnsupportedCrossAssetRobustnessStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CrossAssetRobustnessRecommendation {
    ImplementResearchOnly,
    KeepResearchOnly,
    RejectFamily,
}

impl CrossAssetRobustnessRecommendation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ImplementResearchOnly => "IMPLEMENT_RESEARCH_ONLY",
            Self::KeepResearchOnly => "KEEP_RESEARCH_ONLY",
            Self::RejectFamily => "REJECT_FAMILY",
        }
    }
}

impl std::str::FromStr for CrossAssetRobustnessRecommendation {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "IMPLEMENT_RESEARCH_ONLY" => Ok(Self::ImplementResearchOnly),
            "KEEP_RESEARCH_ONLY" => Ok(Self::KeepResearchOnly),
            "REJECT_FAMILY" => Ok(Self::RejectFamily),
            other => Err(CoreError::UnsupportedCrossAssetRobustnessRecommendation(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossAssetRobustnessFinding {
    pub severity: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossAssetRobustnessRecommendationItem {
    pub priority: String,
    pub code: String,
    pub message: String,
}

fn default_cross_asset_robustness_symbols() -> Vec<String> {
    vec![
        "BTCUSDT".to_string(),
        "ETHUSDT".to_string(),
        "SOLUSDT".to_string(),
        "BNBUSDT".to_string(),
    ]
}

fn default_cross_asset_robustness_ranking_lookbacks() -> Vec<u32> {
    vec![6, 12, 24]
}

fn default_cross_asset_robustness_rank_metrics() -> Vec<CrossAssetRankMetric> {
    vec![
        CrossAssetRankMetric::RawReturn,
        CrossAssetRankMetric::VolAdjustedReturn,
    ]
}

fn default_cross_asset_robustness_spreads() -> Vec<Decimal> {
    vec![Decimal::new(2, 0), Decimal::new(3, 0), Decimal::new(5, 0)]
}

fn default_cross_asset_robustness_holding_hours() -> Vec<u32> {
    vec![6, 12, 24]
}

fn default_cross_asset_fee_bps() -> Decimal {
    Decimal::new(10, 0)
}

fn default_cross_asset_slippage_bps() -> Decimal {
    Decimal::new(5, 0)
}

fn default_cross_asset_robustness_sizing_modes() -> Vec<CrossAssetSizingMode> {
    vec![
        CrossAssetSizingMode::EqualNotional,
        CrossAssetSizingMode::VolatilityNormalized,
    ]
}

fn default_cross_asset_robustness_market_filters() -> Vec<CrossAssetMarketFilter> {
    vec![
        CrossAssetMarketFilter::None,
        CrossAssetMarketFilter::Basket72hReturnGt {
            threshold_pct: Decimal::new(-5, 0),
        },
        CrossAssetMarketFilter::AtLeastNSymbolsPositive24h { min_symbols: 2 },
    ]
}

fn default_cross_asset_robustness_vol_filters() -> Vec<CrossAssetVolFilter> {
    vec![
        CrossAssetVolFilter::None,
        CrossAssetVolFilter::AssetNotExtremeVsBasket {
            max_ratio: Decimal::new(15, 1),
        },
    ]
}

fn default_cross_asset_robustness_overextension_filters() -> Vec<CrossAssetOverextensionFilter> {
    vec![
        CrossAssetOverextensionFilter::None,
        CrossAssetOverextensionFilter::MaxReturn24hPct {
            max_pct: Decimal::new(8, 0),
        },
        CrossAssetOverextensionFilter::MaxReturn24hPct {
            max_pct: Decimal::new(10, 0),
        },
    ]
}

fn default_cross_asset_robustness_exit_rules() -> Vec<CrossAssetExitRule> {
    vec![
        CrossAssetExitRule::FixedHold,
        CrossAssetExitRule::StopPct {
            stop_pct: Decimal::new(3, 0),
        },
        CrossAssetExitRule::StopPct {
            stop_pct: Decimal::new(5, 0),
        },
        CrossAssetExitRule::TakeProfitPct {
            take_profit_pct: Decimal::new(5, 0),
        },
    ]
}

fn default_cross_asset_robustness_max_configs() -> usize {
    256
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetRobustnessMatrixRequest {
    #[serde(default = "default_cross_asset_strategy_kind")]
    pub strategy_kind: CrossAssetStrategyKind,
    #[serde(default = "default_cross_asset_robustness_symbols")]
    pub symbols: Vec<String>,
    #[serde(default = "default_cross_asset_timeframe")]
    pub timeframe: String,
    #[serde(default)]
    pub windows: Vec<CrossAssetRobustnessMatrixWindow>,
    #[serde(default = "default_cross_asset_robustness_ranking_lookbacks")]
    pub ranking_lookback_hours: Vec<u32>,
    #[serde(default = "default_cross_asset_robustness_rank_metrics")]
    pub rank_metrics: Vec<CrossAssetRankMetric>,
    #[serde(default)]
    pub min_top_return_pct: Decimal,
    #[serde(default = "default_cross_asset_robustness_spreads")]
    pub min_rank_spread_pct: Vec<Decimal>,
    #[serde(default = "default_cross_asset_robustness_holding_hours")]
    pub holding_hours: Vec<u32>,
    #[serde(default = "default_cross_asset_fee_bps")]
    pub fee_bps: Decimal,
    #[serde(default = "default_cross_asset_slippage_bps")]
    pub slippage_bps: Decimal,
    #[serde(default = "default_cross_asset_one_active_position")]
    pub one_active_position: bool,
    #[serde(default = "default_cross_asset_robustness_sizing_modes")]
    pub sizing_modes: Vec<CrossAssetSizingMode>,
    #[serde(default = "default_cross_asset_min_weight")]
    pub min_weight: Decimal,
    #[serde(default = "default_cross_asset_max_weight")]
    pub max_weight: Decimal,
    #[serde(default = "default_cross_asset_robustness_market_filters")]
    pub market_filters: Vec<CrossAssetMarketFilter>,
    #[serde(default = "default_cross_asset_robustness_vol_filters")]
    pub vol_filters: Vec<CrossAssetVolFilter>,
    #[serde(default = "default_cross_asset_robustness_overextension_filters")]
    pub overextension_filters: Vec<CrossAssetOverextensionFilter>,
    #[serde(default = "default_cross_asset_robustness_exit_rules")]
    pub exit_rules: Vec<CrossAssetExitRule>,
    #[serde(default = "default_cross_asset_robustness_max_configs")]
    pub max_configs: usize,
    pub correlation_id: Option<Uuid>,
}

impl CrossAssetRobustnessMatrixRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.symbols.len() < 2 {
            return Err(CoreError::InvalidCrossAssetRobustnessMatrixRequest(
                "at least two symbols are required".to_string(),
            ));
        }
        if self.symbols.iter().any(|value| value.trim().is_empty()) {
            return Err(CoreError::InvalidCrossAssetRobustnessMatrixRequest(
                "symbols cannot contain empty values".to_string(),
            ));
        }
        if self.timeframe != "1h" && self.timeframe != "4h" {
            return Err(CoreError::InvalidCrossAssetRobustnessMatrixRequest(
                "only 1h and 4h timeframes are supported".to_string(),
            ));
        }
        self.timeframe.parse::<CandleInterval>()?;
        if self.windows.is_empty() {
            return Err(CoreError::InvalidCrossAssetRobustnessMatrixRequest(
                "at least one window is required".to_string(),
            ));
        }
        for window in &self.windows {
            if window.label.trim().is_empty() || window.end_time <= window.start_time {
                return Err(CoreError::InvalidCrossAssetRobustnessMatrixRequest(
                    "matrix windows require a label and valid time range".to_string(),
                ));
            }
        }
        if self.ranking_lookback_hours.is_empty()
            || self.rank_metrics.is_empty()
            || self.min_rank_spread_pct.is_empty()
            || self.holding_hours.is_empty()
            || self.sizing_modes.is_empty()
            || self.market_filters.is_empty()
            || self.vol_filters.is_empty()
            || self.overextension_filters.is_empty()
            || self.exit_rules.is_empty()
        {
            return Err(CoreError::InvalidCrossAssetRobustnessMatrixRequest(
                "all matrix parameter grids must contain at least one value".to_string(),
            ));
        }
        if self.ranking_lookback_hours.iter().any(|value| *value == 0)
            || self.holding_hours.iter().any(|value| *value == 0)
        {
            return Err(CoreError::InvalidCrossAssetRobustnessMatrixRequest(
                "ranking lookbacks and holding hours must be greater than zero".to_string(),
            ));
        }
        if self
            .min_rank_spread_pct
            .iter()
            .any(|value| *value < Decimal::ZERO)
            || self.fee_bps < Decimal::ZERO
            || self.slippage_bps < Decimal::ZERO
        {
            return Err(CoreError::InvalidCrossAssetRobustnessMatrixRequest(
                "spread, fee, and slippage values cannot be negative".to_string(),
            ));
        }
        if self.min_weight <= Decimal::ZERO
            || self.max_weight <= Decimal::ZERO
            || self.min_weight > self.max_weight
        {
            return Err(CoreError::InvalidCrossAssetRobustnessMatrixRequest(
                "min_weight and max_weight must be positive and ordered".to_string(),
            ));
        }
        if self.max_configs == 0 {
            return Err(CoreError::InvalidCrossAssetRobustnessMatrixRequest(
                "max_configs must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }

    pub fn full_config_count(&self) -> usize {
        self.ranking_lookback_hours.len()
            * self.rank_metrics.len()
            * self.min_rank_spread_pct.len()
            * self.holding_hours.len()
            * self.sizing_modes.len()
            * self.market_filters.len()
            * self.vol_filters.len()
            * self.overextension_filters.len()
            * self.exit_rules.len()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetRobustnessMatrixCell {
    pub id: Uuid,
    pub matrix_run_id: Uuid,
    pub config_index: i32,
    pub config: CrossAssetRobustnessConfig,
    pub window_label: String,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub status: CrossAssetRobustnessStatus,
    pub total_trades: i32,
    pub compounded_pnl_pct: Decimal,
    pub avg_trade_pnl_pct: Decimal,
    pub median_trade_pnl_pct: Decimal,
    pub win_rate: Decimal,
    pub max_drawdown_pct: Decimal,
    pub worst_trade_pct: Decimal,
    pub worst_window_pnl_pct: Decimal,
    pub fee_slippage_drag_pct: Decimal,
    pub symbol_distribution: BTreeMap<String, i32>,
    pub max_symbol_concentration_pct: Decimal,
    pub quarter_distribution: BTreeMap<String, i32>,
    pub max_quarter_concentration_pct: Decimal,
    pub findings: Vec<CrossAssetRobustnessFinding>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetRobustnessRanking {
    pub rank: i32,
    pub config_index: i32,
    pub config: CrossAssetRobustnessConfig,
    pub status: CrossAssetRobustnessStatus,
    pub robustness_score: Decimal,
    pub total_trades: i32,
    pub combined_pnl_pct: Decimal,
    pub median_cell_pnl_pct: Decimal,
    pub worst_window_pnl_pct: Decimal,
    pub max_drawdown_pct: Decimal,
    pub max_symbol_concentration_pct: Decimal,
    pub btc_trade_count: i32,
    pub skipped_window_count: i32,
    pub warnings: Vec<String>,
    pub findings: Vec<CrossAssetRobustnessFinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetRobustnessMatrixResult {
    pub run_id: Uuid,
    pub request: CrossAssetRobustnessMatrixRequest,
    pub status: CrossAssetRobustnessStatus,
    pub recommendation: CrossAssetRobustnessRecommendation,
    pub rankings: Vec<CrossAssetRobustnessRanking>,
    pub findings: Vec<CrossAssetRobustnessFinding>,
    pub recommendations: Vec<CrossAssetRobustnessRecommendationItem>,
    pub cell_count: i32,
    pub evaluated_config_count: i32,
    pub full_config_count: i32,
    pub skipped_config_count: i32,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub correlation_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CrossAssetRobustnessMatrixRun {
    pub result: CrossAssetRobustnessMatrixResult,
    pub cells: Vec<CrossAssetRobustnessMatrixCell>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchDatasetBuildStatus {
    Started,
    Completed,
    Failed,
}

impl ResearchDatasetBuildStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Started => "STARTED",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchDatasetBuildStepStatus {
    Started,
    Completed,
    Failed,
}

impl ResearchDatasetBuildStepStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Started => "STARTED",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchDatasetBuildStep {
    pub step: String,
    pub status: ResearchDatasetBuildStepStatus,
    pub details: Option<Value>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchDatasetBuildRequest {
    #[serde(default)]
    pub exchange: MarketDataSource,
    pub symbol: String,
    pub intervals: Vec<String>,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    #[serde(default = "default_required_coverage_pct")]
    pub required_coverage_pct: Decimal,
    pub correlation_id: Option<Uuid>,
}

impl ResearchDatasetBuildRequest {
    pub fn validate(&self) -> Result<(), CoreError> {
        ResearchDataCoverageRequest {
            exchange: self.exchange,
            symbol: self.symbol.clone(),
            intervals: self.intervals.clone(),
            start_time: self.start_time,
            end_time: self.end_time,
            required_coverage_pct: self.required_coverage_pct,
            correlation_id: self.correlation_id,
        }
        .validate()
    }

    pub fn normalized_symbol(&self) -> Result<Symbol, CoreError> {
        Symbol::new(self.symbol.clone())
    }

    pub fn parsed_intervals(&self) -> Result<Vec<CandleInterval>, CoreError> {
        ResearchDataCoverageRequest {
            exchange: self.exchange,
            symbol: self.symbol.clone(),
            intervals: self.intervals.clone(),
            start_time: self.start_time,
            end_time: self.end_time,
            required_coverage_pct: self.required_coverage_pct,
            correlation_id: self.correlation_id,
        }
        .parsed_intervals()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchDatasetBuildResult {
    pub build_id: Uuid,
    pub exchange: MarketDataSource,
    pub symbol: String,
    pub requested_intervals: Vec<String>,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub status: ResearchDatasetBuildStatus,
    pub coverage_before: ResearchDataCoverageResult,
    pub coverage_after: ResearchDataCoverageResult,
    pub steps: Vec<ResearchDatasetBuildStep>,
    pub failed_reason: Option<String>,
    pub correlation_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateStatus {
    Discovered,
    Observing,
    AcceptedForShadow,
    PromotedToShadowConfig,
    Rejected,
    Archived,
}

impl ResearchCandidateStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Discovered => "DISCOVERED",
            Self::Observing => "OBSERVING",
            Self::AcceptedForShadow => "ACCEPTED_FOR_SHADOW",
            Self::PromotedToShadowConfig => "PROMOTED_TO_SHADOW_CONFIG",
            Self::Rejected => "REJECTED",
            Self::Archived => "ARCHIVED",
        }
    }
}

impl std::str::FromStr for ResearchCandidateStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "DISCOVERED" => Ok(Self::Discovered),
            "OBSERVING" => Ok(Self::Observing),
            "ACCEPTED_FOR_SHADOW" => Ok(Self::AcceptedForShadow),
            "PROMOTED_TO_SHADOW_CONFIG" => Ok(Self::PromotedToShadowConfig),
            "REJECTED" => Ok(Self::Rejected),
            "ARCHIVED" => Ok(Self::Archived),
            other => Err(CoreError::UnsupportedResearchCandidateStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateDecision {
    AcceptForShadow,
    PromoteToShadowConfig,
    Reject,
    Archive,
    Reopen,
}

impl ResearchCandidateDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AcceptForShadow => "ACCEPT_FOR_SHADOW",
            Self::PromoteToShadowConfig => "PROMOTE_TO_SHADOW_CONFIG",
            Self::Reject => "REJECT",
            Self::Archive => "ARCHIVE",
            Self::Reopen => "REOPEN",
        }
    }
}

impl std::str::FromStr for ResearchCandidateDecision {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "ACCEPT_FOR_SHADOW" => Ok(Self::AcceptForShadow),
            "PROMOTE_TO_SHADOW_CONFIG" => Ok(Self::PromoteToShadowConfig),
            "REJECT" => Ok(Self::Reject),
            "ARCHIVE" => Ok(Self::Archive),
            "REOPEN" => Ok(Self::Reopen),
            other => Err(CoreError::UnsupportedResearchCandidateDecision(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidate {
    pub id: Uuid,
    pub experiment_id: Option<Uuid>,
    pub experiment_run_id: Option<Uuid>,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub config: Value,
    pub score: Option<Decimal>,
    pub pnl_pct: Option<Decimal>,
    pub max_drawdown_pct: Option<Decimal>,
    pub trade_count: Option<i32>,
    pub win_rate: Option<Decimal>,
    pub fee_drag: Option<Decimal>,
    pub status: ResearchCandidateStatus,
    pub rejection_reason: Option<String>,
    pub notes: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateProposal {
    pub id: Uuid,
    pub source_batch_id: Option<Uuid>,
    #[serde(default)]
    pub source_experiment_run_id: Option<Uuid>,
    #[serde(default)]
    pub source_walk_forward_run_id: Option<Uuid>,
    #[serde(default)]
    pub source_robustness_matrix_run_id: Option<Uuid>,
    pub experiment_run_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub config: Value,
    pub score: Decimal,
    pub pnl_pct: Decimal,
    pub triage_status: ResearchBatchTriageStatus,
    pub walk_forward_status: Option<String>,
    #[serde(default)]
    pub source_robustness_status: Option<String>,
    #[serde(default)]
    pub normalized_strategy_config: Option<Value>,
    #[serde(default)]
    pub config_fingerprint: Option<String>,
    #[serde(default)]
    pub evidence_status_summary: Option<Value>,
    #[serde(default)]
    pub gate_evidence_mismatch: bool,
    pub gate_decision: ResearchCandidateCreationDecision,
    pub reason: String,
    pub promoted_candidate_id: Option<Uuid>,
    pub promoted_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateLifecycleEvent {
    pub id: Uuid,
    pub candidate_id: Uuid,
    pub previous_status: Option<ResearchCandidateStatus>,
    pub next_status: ResearchCandidateStatus,
    pub decision: ResearchCandidateDecision,
    pub reason: Option<String>,
    pub notes: Option<String>,
    pub actor_id: Option<Uuid>,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateEvidenceBundleCandidate {
    pub candidate_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub status: ResearchCandidateStatus,
    pub config: Value,
    pub config_fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ResearchCandidateEvidenceProvenance {
    pub source_experiment_run_id: Option<Uuid>,
    pub source_walk_forward_run_id: Option<Uuid>,
    pub source_robustness_matrix_run_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_robustness_matrix_cell_id: Option<Uuid>,
    pub source_proposal_id: Option<Uuid>,
    pub source_batch_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_campaign_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub campaign_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_creation_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate_decision: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_status_summary: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateExecutionBoundary {
    pub no_paper_testnet_live: bool,
    pub import_does_not_authorize_execution: bool,
    pub paper_testnet_live_boundary_crossed: bool,
    #[serde(default)]
    pub import_does_not_create_shadow_runs: bool,
    #[serde(default)]
    pub import_does_not_update_runner_config: bool,
    #[serde(default)]
    pub import_does_not_create_scheduled_jobs: bool,
    #[serde(default)]
    pub import_does_not_mark_ready_for_testnet: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateEvidenceBundleIntegrity {
    pub bundle_fingerprint: String,
    pub algorithm: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateValidationProtocol {
    pub data_window_start: DateTime<Utc>,
    pub data_window_end: DateTime<Utc>,
    pub symbol: String,
    pub timeframe: String,
    pub fee_bps: Decimal,
    pub slippage_bps: Decimal,
    pub holding_candles: Option<u32>,
    pub replay_mode: String,
    pub strategy_id: String,
    pub normalized_strategy_config: Value,
    pub config_fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateWalkForwardProtocolWindow {
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub pnl_pct: Decimal,
    pub trade_count: i32,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateWalkForwardProtocol {
    pub train_window_hours: Option<i64>,
    pub test_window_hours: Option<i64>,
    pub step_hours: Option<i64>,
    pub min_windows: Option<i32>,
    pub explicit_windows: Vec<ResearchCandidateWalkForwardProtocolWindow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateDataQualitySnapshot {
    pub candle_count: Option<i64>,
    pub expected_candle_count: Option<i64>,
    pub coverage_pct: Option<Decimal>,
    pub gap_count: Option<i64>,
    pub first_candle: Option<DateTime<Utc>>,
    pub last_candle: Option<DateTime<Utc>>,
    pub quality_status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateEvidenceArtifacts {
    pub experiment_run_id: Option<Uuid>,
    pub walk_forward_run_id: Option<Uuid>,
    pub robustness_matrix_run_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub robustness_matrix_cell_id: Option<Uuid>,
    pub candidate_gate_source_id: Option<Uuid>,
    pub proposal_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_batch_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_campaign_id: Option<Uuid>,
    #[serde(default)]
    pub has_experiment: bool,
    #[serde(default)]
    pub has_walk_forward: bool,
    #[serde(default)]
    pub has_robustness_matrix: bool,
    #[serde(default)]
    pub has_data_quality_snapshot: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateEngineFingerprint {
    pub app_git_hash: Option<String>,
    pub replay_engine_version: Option<String>,
    pub replay_engine_hash: Option<String>,
    pub strategy_engine_version: Option<String>,
    pub strategy_engine_hash: Option<String>,
    pub migration_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateEvidenceBundle {
    pub schema_version: String,
    pub exported_at: DateTime<Utc>,
    pub source_environment: Option<String>,
    pub candidate: ResearchCandidateEvidenceBundleCandidate,
    pub evidence_provenance: ResearchCandidateEvidenceProvenance,
    pub evidence_summary: Value,
    pub warnings: Vec<String>,
    pub execution_boundary: ResearchCandidateExecutionBoundary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation_protocol: Option<ResearchCandidateValidationProtocol>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub walk_forward_protocol: Option<ResearchCandidateWalkForwardProtocol>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_quality_snapshot: Option<ResearchCandidateDataQualitySnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_artifacts: Option<ResearchCandidateEvidenceArtifacts>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_fingerprint: Option<ResearchCandidateEngineFingerprint>,
    pub integrity: ResearchCandidateEvidenceBundleIntegrity,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateImportBundlePreviewRequest {
    pub bundle: ResearchCandidateEvidenceBundle,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateImportBundleRequest {
    pub bundle: ResearchCandidateEvidenceBundle,
    pub confirm: String,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateImportProposedActions {
    pub create_candidate: bool,
    pub create_proposal: bool,
    pub attach_evidence_summary: bool,
    pub preserve_imported_status: bool,
    pub imported_candidate_status: ResearchCandidateStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateImportBundlePreview {
    pub bundle_fingerprint: String,
    pub config_fingerprint: String,
    pub source_candidate_id: Uuid,
    pub candidate_id: Option<Uuid>,
    pub candidate_exists: bool,
    pub config_exists: bool,
    pub import_exists: bool,
    pub proposed_actions: ResearchCandidateImportProposedActions,
    pub missing_source_ids: Vec<String>,
    pub warnings: Vec<String>,
    pub blockers: Vec<String>,
    pub confirmation_required: String,
    pub recommendation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateImportBundleResult {
    pub preview: ResearchCandidateImportBundlePreview,
    pub candidate: ResearchCandidate,
    pub import_audit_id: Uuid,
    pub applied: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateImportReconciliationRequest {
    pub reconciliation_status: String,
    pub local_validation_window_start: Option<DateTime<Utc>>,
    pub local_validation_window_end: Option<DateTime<Utc>>,
    pub local_walk_forward_status: Option<String>,
    pub local_worst_window_pnl: Option<Decimal>,
    pub local_recommendation: Option<String>,
    pub reconciliation_summary_json: Option<Value>,
    pub recommended_next_action: String,
    pub confirm: String,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateImportReconciliationResult {
    pub import_audit_id: Uuid,
    pub candidate_id: Uuid,
    pub candidate_status: ResearchCandidateStatus,
    pub reconciliation_status: String,
    pub recommended_next_action: String,
    pub reconciliation_checked_at: DateTime<Utc>,
}

pub fn expected_research_candidate_import_confirmation(config_fingerprint: &str) -> String {
    format!("IMPORT RESEARCH CANDIDATE {}", config_fingerprint.trim())
}

pub fn is_valid_research_candidate_import_confirmation(
    config_fingerprint: &str,
    confirmation_text: &str,
) -> bool {
    confirmation_text == expected_research_candidate_import_confirmation(config_fingerprint)
}

pub fn expected_research_candidate_import_reconciliation_confirmation(import_id: Uuid) -> String {
    format!("RECORD RECONCILIATION {import_id}")
}

pub fn is_valid_research_candidate_import_reconciliation_confirmation(
    import_id: Uuid,
    confirmation_text: &str,
) -> bool {
    confirmation_text == expected_research_candidate_import_reconciliation_confirmation(import_id)
}

pub fn canonical_json_value(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(canonical_json_value).collect()),
        Value::Object(map) => {
            let mut sorted = serde_json::Map::new();
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                if let Some(value) = map.get(key) {
                    sorted.insert(key.clone(), canonical_json_value(value));
                }
            }
            Value::Object(sorted)
        }
        other => other.clone(),
    }
}

pub fn hash_canonical_json_value(value: &Value) -> Result<String, CoreError> {
    let normalized = canonical_json_value(value);
    let bytes = serde_json::to_vec(&normalized)
        .map_err(|err| CoreError::InvalidResearchCandidateImportBundle(err.to_string()))?;
    let digest = Sha256::digest(bytes);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub fn research_candidate_bundle_fingerprint_payload(
    bundle: &ResearchCandidateEvidenceBundle,
) -> Value {
    json!({
        "schema_version": bundle.schema_version,
        "source_environment": bundle.source_environment,
        "candidate": bundle.candidate,
        "evidence_provenance": bundle.evidence_provenance,
        "evidence_summary": bundle.evidence_summary,
        "warnings": bundle.warnings,
        "execution_boundary": bundle.execution_boundary,
        "validation_protocol": bundle.validation_protocol,
        "walk_forward_protocol": bundle.walk_forward_protocol,
        "data_quality_snapshot": bundle.data_quality_snapshot,
        "evidence_artifacts": bundle.evidence_artifacts,
        "engine_fingerprint": bundle.engine_fingerprint,
    })
}

pub fn research_candidate_bundle_fingerprint(
    bundle: &ResearchCandidateEvidenceBundle,
) -> Result<String, CoreError> {
    hash_canonical_json_value(&research_candidate_bundle_fingerprint_payload(bundle))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateDecisionRequest {
    pub decision: ResearchCandidateDecision,
    pub reason: Option<String>,
    pub notes: Option<String>,
    #[serde(default)]
    pub acknowledge_runner_mismatch: bool,
    #[serde(default)]
    pub acknowledge_overfit_risk: bool,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateWalkForwardEvidence {
    pub walk_forward_run_id: Uuid,
    pub robustness_status: StrategyWalkForwardRobustnessStatus,
    pub status: String,
    pub recommendation_action: Option<String>,
    pub recommendation_reason: Option<String>,
    pub total_windows: i32,
    pub completed_windows: i32,
    pub profitable_windows: i32,
    pub losing_windows: i32,
    pub avg_pnl_pct: Decimal,
    pub worst_pnl_pct: Decimal,
    pub best_pnl_pct: Decimal,
    pub robustness_score: Decimal,
    pub consistency_score: Decimal,
    pub created_at: DateTime<Utc>,
    pub linked_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchCandidateAcceptForShadowPreviewRequest {
    pub candidate_id: Uuid,
    #[serde(default = "default_true")]
    pub require_fresh_observation: bool,
    #[serde(default)]
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateAcceptForShadowPreviewStatus {
    ReadyForHumanAcceptance,
    Blocked,
    NeedsMoreData,
    WarningReviewRequired,
}

impl ResearchCandidateAcceptForShadowPreviewStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadyForHumanAcceptance => "READY_FOR_HUMAN_ACCEPTANCE",
            Self::Blocked => "BLOCKED",
            Self::NeedsMoreData => "NEEDS_MORE_DATA",
            Self::WarningReviewRequired => "WARNING_REVIEW_REQUIRED",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchCandidateAcceptForShadowCheck {
    pub code: String,
    pub name: String,
    pub passed: bool,
    pub blocking: bool,
    pub summary: String,
    #[serde(default)]
    pub details: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateAcceptForShadowEvidenceSummary {
    pub candidate_score: Option<Decimal>,
    pub candidate_pnl_pct: Option<Decimal>,
    pub candidate_max_drawdown_pct: Option<Decimal>,
    pub candidate_trade_count: Option<i32>,
    pub candidate_win_rate: Option<Decimal>,
    pub candidate_fee_drag: Option<Decimal>,
    pub source_experiment_id: Option<Uuid>,
    pub source_experiment_run_id: Option<Uuid>,
    pub source_walk_forward_run_id: Option<Uuid>,
    pub source_robustness_matrix_run_id: Option<Uuid>,
    pub config_fingerprint: Option<String>,
    pub data_quality_status: Option<MarketDataQualityStatus>,
    pub evaluated_start_time: Option<DateTime<Utc>>,
    pub evaluated_end_time: Option<DateTime<Utc>>,
    pub walk_forward_status: Option<StrategyWalkForwardRobustnessStatus>,
    pub walk_forward_total_windows: Option<i32>,
    pub walk_forward_profitable_windows: Option<i32>,
    pub walk_forward_losing_windows: Option<i32>,
    pub walk_forward_worst_pnl_pct: Option<Decimal>,
    pub robustness_matrix_status: Option<StrategyRobustnessMatrixStatus>,
    pub shadow_run_count: i64,
}

#[derive(Debug, Clone)]
pub struct ResearchCandidateAcceptForShadowPreviewInput {
    pub candidate: ResearchCandidate,
    pub evidence_summary: ResearchCandidateAcceptForShadowEvidenceSummary,
    pub source_experiment_exists: bool,
    pub source_walk_forward_exists: bool,
    pub source_robustness_matrix_exists: bool,
    pub fresh_observation: bool,
    pub require_fresh_observation: bool,
    pub runner_alignment: StrategyCandidateRunnerAlignment,
    pub required_runner_config_change: Vec<String>,
    pub holdout_2025_available: bool,
    pub btc_generalization_failed: bool,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateAcceptForShadowPreviewResult {
    pub candidate_id: Uuid,
    pub current_status: ResearchCandidateStatus,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub evidence_summary: ResearchCandidateAcceptForShadowEvidenceSummary,
    pub checks: Vec<ResearchCandidateAcceptForShadowCheck>,
    pub blockers: Vec<String>,
    pub warnings: Vec<String>,
    pub recommended_action: String,
    pub runner_alignment: StrategyCandidateRunnerAlignment,
    pub required_runner_config_change: Vec<String>,
    pub status: ResearchCandidateAcceptForShadowPreviewStatus,
    pub no_mutation: bool,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchCandidateAcceptForShadowApplyRequest {
    pub confirm: String,
    #[serde(default)]
    pub acknowledge_warnings: bool,
    #[serde(default)]
    pub acknowledged_warning_codes: Vec<String>,
    pub operator_note: Option<String>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateAcceptForShadowApplyResult {
    pub candidate_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub previous_status: ResearchCandidateStatus,
    pub new_status: ResearchCandidateStatus,
    pub warnings_acknowledged: bool,
    pub acknowledged_warning_codes: Vec<String>,
    pub warnings: Vec<String>,
    pub lifecycle_event_id: Uuid,
    pub runner_config_unchanged: bool,
    pub shadow_runs_created: i64,
    pub execution_tables_mutated: bool,
    pub recommended_next_action: String,
    pub applied_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResearchCandidateAcceptForShadowApplyRejection {
    InvalidConfirmation,
    CandidateNotEligible,
    PreviewBlocked,
    PreviewNotReady,
    WarningAcknowledgementRequired,
}

impl ResearchCandidateAcceptForShadowApplyRejection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidConfirmation => "invalid_confirmation",
            Self::CandidateNotEligible => "candidate_not_eligible",
            Self::PreviewBlocked => "preview_blocked",
            Self::PreviewNotReady => "preview_not_ready",
            Self::WarningAcknowledgementRequired => "warning_acknowledgement_required",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateReviewAction {
    MarkReviewed,
    MarkNeedsMoreObservation,
    MarkReadyForTestnetReview,
    MarkInvestigated,
    RejectFromWatchlist,
    ArchiveFromWatchlist,
}

impl ResearchCandidateReviewAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MarkReviewed => "MARK_REVIEWED",
            Self::MarkNeedsMoreObservation => "MARK_NEEDS_MORE_OBSERVATION",
            Self::MarkReadyForTestnetReview => "MARK_READY_FOR_TESTNET_REVIEW",
            Self::MarkInvestigated => "MARK_INVESTIGATED",
            Self::RejectFromWatchlist => "REJECT_FROM_WATCHLIST",
            Self::ArchiveFromWatchlist => "ARCHIVE_FROM_WATCHLIST",
        }
    }
}

impl std::str::FromStr for ResearchCandidateReviewAction {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "MARK_REVIEWED" => Ok(Self::MarkReviewed),
            "MARK_NEEDS_MORE_OBSERVATION" => Ok(Self::MarkNeedsMoreObservation),
            "MARK_READY_FOR_TESTNET_REVIEW" => Ok(Self::MarkReadyForTestnetReview),
            "MARK_INVESTIGATED" => Ok(Self::MarkInvestigated),
            "REJECT_FROM_WATCHLIST" => Ok(Self::RejectFromWatchlist),
            "ARCHIVE_FROM_WATCHLIST" => Ok(Self::ArchiveFromWatchlist),
            other => Err(CoreError::UnsupportedResearchCandidateReviewAction(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateReviewStatus {
    Recorded,
    CandidateStatusUpdated,
}

impl ResearchCandidateReviewStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Recorded => "RECORDED",
            Self::CandidateStatusUpdated => "CANDIDATE_STATUS_UPDATED",
        }
    }
}

impl std::str::FromStr for ResearchCandidateReviewStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "RECORDED" => Ok(Self::Recorded),
            "CANDIDATE_STATUS_UPDATED" => Ok(Self::CandidateStatusUpdated),
            other => Err(CoreError::UnsupportedResearchCandidateReviewStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateReview {
    pub id: Uuid,
    pub candidate_id: Uuid,
    pub action: ResearchCandidateReviewAction,
    pub status: ResearchCandidateReviewStatus,
    pub previous_candidate_status: ResearchCandidateStatus,
    pub next_candidate_status: Option<ResearchCandidateStatus>,
    pub reason: Option<String>,
    pub notes: Option<String>,
    pub actor_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
    pub qualification_evaluation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateReviewRequest {
    pub action: ResearchCandidateReviewAction,
    pub reason: Option<String>,
    pub notes: Option<String>,
    pub qualification_evaluation_id: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateReviewResult {
    pub review: ResearchCandidateReview,
    pub candidate_status_before: ResearchCandidateStatus,
    pub candidate_status_after: ResearchCandidateStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResearchCandidateReviewContext {
    pub latest_qualification_status: Option<ResearchCandidateQualificationStatus>,
    pub latest_watchlist_status: Option<ResearchCandidateWatchlistStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResearchCandidateReviewOutcome {
    pub status: ResearchCandidateReviewStatus,
    pub next_candidate_status: Option<ResearchCandidateStatus>,
}

pub fn research_candidate_review_outcome(
    current_status: ResearchCandidateStatus,
    action: ResearchCandidateReviewAction,
    context: ResearchCandidateReviewContext,
    reason: Option<&str>,
) -> Result<ResearchCandidateReviewOutcome, CoreError> {
    match action {
        ResearchCandidateReviewAction::MarkReviewed => Ok(ResearchCandidateReviewOutcome {
            status: ResearchCandidateReviewStatus::Recorded,
            next_candidate_status: None,
        }),
        ResearchCandidateReviewAction::MarkNeedsMoreObservation => match current_status {
            ResearchCandidateStatus::Discovered => Ok(ResearchCandidateReviewOutcome {
                status: ResearchCandidateReviewStatus::CandidateStatusUpdated,
                next_candidate_status: Some(ResearchCandidateStatus::Observing),
            }),
            ResearchCandidateStatus::Observing | ResearchCandidateStatus::AcceptedForShadow => {
                Ok(ResearchCandidateReviewOutcome {
                    status: ResearchCandidateReviewStatus::Recorded,
                    next_candidate_status: None,
                })
            }
            _ => Err(CoreError::InvalidResearchCandidateReviewAction(
                current_status.as_str().to_string(),
                action.as_str().to_string(),
            )),
        },
        ResearchCandidateReviewAction::MarkReadyForTestnetReview => {
            if context.latest_qualification_status
                != Some(ResearchCandidateQualificationStatus::Qualified)
            {
                return Err(CoreError::ResearchCandidateReviewRequiresQualified(
                    action.as_str().to_string(),
                ));
            }
            Ok(ResearchCandidateReviewOutcome {
                status: ResearchCandidateReviewStatus::Recorded,
                next_candidate_status: None,
            })
        }
        ResearchCandidateReviewAction::MarkInvestigated => {
            let investigation_allowed = matches!(
                context.latest_watchlist_status,
                Some(ResearchCandidateWatchlistStatus::LostQualification)
                    | Some(ResearchCandidateWatchlistStatus::NeedsAttention)
            ) || context.latest_qualification_status
                == Some(ResearchCandidateQualificationStatus::NotQualified);
            if !investigation_allowed {
                return Err(
                    CoreError::ResearchCandidateReviewRequiresInvestigationContext(
                        action.as_str().to_string(),
                    ),
                );
            }
            Ok(ResearchCandidateReviewOutcome {
                status: ResearchCandidateReviewStatus::Recorded,
                next_candidate_status: None,
            })
        }
        ResearchCandidateReviewAction::RejectFromWatchlist => {
            if reason
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
            {
                return Err(CoreError::MissingResearchCandidateReviewReason(
                    action.as_str().to_string(),
                ));
            }
            Ok(ResearchCandidateReviewOutcome {
                status: ResearchCandidateReviewStatus::CandidateStatusUpdated,
                next_candidate_status: Some(ResearchCandidateStatus::Rejected),
            })
        }
        ResearchCandidateReviewAction::ArchiveFromWatchlist => {
            if reason
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
            {
                return Err(CoreError::MissingResearchCandidateReviewReason(
                    action.as_str().to_string(),
                ));
            }
            Ok(ResearchCandidateReviewOutcome {
                status: ResearchCandidateReviewStatus::CandidateStatusUpdated,
                next_candidate_status: Some(ResearchCandidateStatus::Archived),
            })
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidatePromotionReadiness {
    pub candidate_id: Uuid,
    pub target: String,
    pub latest_observation_id: Option<Uuid>,
    pub latest_observation_decision: Option<StrategyCandidateObservationDecision>,
    pub last_observed_at: Option<DateTime<Utc>>,
    pub observation_expires_at: Option<DateTime<Utc>>,
    pub observation_age_seconds: Option<i64>,
    pub observation_max_age_seconds: Option<i64>,
    pub observation_snapshot_hash: Option<String>,
    pub latest_recommendation: Option<String>,
    pub readiness_status: Option<ExecutionReadinessStatus>,
    pub readiness_score: Option<i32>,
    #[serde(default)]
    pub runner_alignment: StrategyCandidateRunnerAlignment,
    #[serde(default)]
    pub blockers: Vec<String>,
    pub is_ready: bool,
    pub evaluated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateArchiveRequest {
    pub reason: Option<String>,
    pub notes: Option<String>,
    pub correlation_id: Option<Uuid>,
}

pub fn research_candidate_next_status(
    current_status: ResearchCandidateStatus,
    decision: ResearchCandidateDecision,
) -> Result<ResearchCandidateStatus, CoreError> {
    match decision {
        ResearchCandidateDecision::AcceptForShadow => match current_status {
            ResearchCandidateStatus::Discovered | ResearchCandidateStatus::Observing => {
                Ok(ResearchCandidateStatus::AcceptedForShadow)
            }
            _ => Err(CoreError::InvalidResearchCandidateTransition(
                current_status.as_str().to_string(),
                decision.as_str().to_string(),
            )),
        },
        ResearchCandidateDecision::PromoteToShadowConfig => match current_status {
            ResearchCandidateStatus::AcceptedForShadow => {
                Ok(ResearchCandidateStatus::PromotedToShadowConfig)
            }
            ResearchCandidateStatus::PromotedToShadowConfig => Ok(current_status),
            _ => Err(CoreError::InvalidResearchCandidateTransition(
                current_status.as_str().to_string(),
                decision.as_str().to_string(),
            )),
        },
        ResearchCandidateDecision::Reject => match current_status {
            ResearchCandidateStatus::Discovered
            | ResearchCandidateStatus::Observing
            | ResearchCandidateStatus::AcceptedForShadow
            | ResearchCandidateStatus::PromotedToShadowConfig => {
                Ok(ResearchCandidateStatus::Rejected)
            }
            _ => Err(CoreError::InvalidResearchCandidateTransition(
                current_status.as_str().to_string(),
                decision.as_str().to_string(),
            )),
        },
        ResearchCandidateDecision::Archive => match current_status {
            ResearchCandidateStatus::Archived => {
                Err(CoreError::InvalidResearchCandidateTransition(
                    current_status.as_str().to_string(),
                    decision.as_str().to_string(),
                ))
            }
            _ => Ok(ResearchCandidateStatus::Archived),
        },
        ResearchCandidateDecision::Reopen => match current_status {
            ResearchCandidateStatus::Rejected | ResearchCandidateStatus::Archived => {
                Ok(ResearchCandidateStatus::Discovered)
            }
            _ => Err(CoreError::InvalidResearchCandidateTransition(
                current_status.as_str().to_string(),
                decision.as_str().to_string(),
            )),
        },
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateShadowPromotionMode {
    PreviewOnly,
    Apply,
}

impl ResearchCandidateShadowPromotionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PreviewOnly => "PREVIEW_ONLY",
            Self::Apply => "APPLY",
        }
    }
}

impl std::str::FromStr for ResearchCandidateShadowPromotionMode {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "PREVIEW_ONLY" => Ok(Self::PreviewOnly),
            "APPLY" => Ok(Self::Apply),
            other => Err(CoreError::UnsupportedResearchCandidateDecision(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateShadowPromotionStatus {
    Ready,
    WarningReviewRequired,
    Blocked,
    NoChanges,
    Applied,
}

impl ResearchCandidateShadowPromotionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "READY",
            Self::WarningReviewRequired => "WARNING_REVIEW_REQUIRED",
            Self::Blocked => "BLOCKED",
            Self::NoChanges => "NO_CHANGES",
            Self::Applied => "APPLIED",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchCandidateShadowPromotionRequest {
    pub mode: ResearchCandidateShadowPromotionMode,
    #[serde(default)]
    pub allow_missing_runner_alignment: bool,
    pub confirmation_text: Option<String>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchCandidateShadowPromotionTimeframeChange {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchCandidateShadowPromotionEnabledChange {
    pub from: bool,
    pub to: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ResearchCandidateShadowPromotionDiff {
    pub added_strategies: Vec<String>,
    pub removed_strategies: Vec<String>,
    pub added_symbols: Vec<String>,
    pub removed_symbols: Vec<String>,
    pub timeframe_change: Option<ResearchCandidateShadowPromotionTimeframeChange>,
    pub enabled_change: Option<ResearchCandidateShadowPromotionEnabledChange>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchCandidateShadowPromotionPreview {
    pub candidate_id: Uuid,
    pub candidate_status: ResearchCandidateStatus,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub current_runner_config: TestnetShadowRunnerConfig,
    pub proposed_runner_config: TestnetShadowRunnerConfig,
    pub diff: ResearchCandidateShadowPromotionDiff,
    pub changes: Vec<String>,
    pub status: ResearchCandidateShadowPromotionStatus,
    pub reasons: Vec<String>,
    pub blockers: Vec<String>,
    pub warnings: Vec<String>,
    pub recommendation: String,
    pub confirmation_required: bool,
    pub correlation_id: Uuid,
    pub mode: ResearchCandidateShadowPromotionMode,
    pub allow_missing_runner_alignment: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchCandidateShadowPromotionResult {
    pub candidate_id: Uuid,
    pub candidate_status: ResearchCandidateStatus,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub current_runner_config: TestnetShadowRunnerConfig,
    pub proposed_runner_config: TestnetShadowRunnerConfig,
    pub diff: ResearchCandidateShadowPromotionDiff,
    pub changes: Vec<String>,
    pub status: ResearchCandidateShadowPromotionStatus,
    pub reasons: Vec<String>,
    pub blockers: Vec<String>,
    pub warnings: Vec<String>,
    pub recommendation: String,
    pub confirmation_required: bool,
    pub correlation_id: Uuid,
    pub mode: ResearchCandidateShadowPromotionMode,
    pub allow_missing_runner_alignment: bool,
    pub applied: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyResearchCandidateSource {
    ExperimentRun,
    MultiTimeframeExperiment,
    WalkForward,
    Manual,
}

impl StrategyResearchCandidateSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ExperimentRun => "EXPERIMENT_RUN",
            Self::MultiTimeframeExperiment => "MULTI_TIMEFRAME_EXPERIMENT",
            Self::WalkForward => "WALK_FORWARD",
            Self::Manual => "MANUAL",
        }
    }
}

impl std::str::FromStr for StrategyResearchCandidateSource {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "EXPERIMENT_RUN" => Ok(Self::ExperimentRun),
            "MULTI_TIMEFRAME_EXPERIMENT" => Ok(Self::MultiTimeframeExperiment),
            "WALK_FORWARD" => Ok(Self::WalkForward),
            "MANUAL" => Ok(Self::Manual),
            other => Err(CoreError::UnsupportedStrategyResearchCandidateSource(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyResearchCandidateStatus {
    Draft,
    Registered,
    Rejected,
    PromotedToShadowConfig,
    Archived,
}

impl StrategyResearchCandidateStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "DRAFT",
            Self::Registered => "REGISTERED",
            Self::Rejected => "REJECTED",
            Self::PromotedToShadowConfig => "PROMOTED_TO_SHADOW_CONFIG",
            Self::Archived => "ARCHIVED",
        }
    }
}

impl std::str::FromStr for StrategyResearchCandidateStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "DRAFT" => Ok(Self::Draft),
            "REGISTERED" => Ok(Self::Registered),
            "REJECTED" => Ok(Self::Rejected),
            "PROMOTED_TO_SHADOW_CONFIG" => Ok(Self::PromotedToShadowConfig),
            "ARCHIVED" => Ok(Self::Archived),
            other => Err(CoreError::UnsupportedStrategyResearchCandidateStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StrategyResearchCandidateRejectionReason {
    InvalidConfig,
    InvalidSource,
    MissingEvidence,
    CandidateNotRegistered,
    AlreadyPromoted,
    WrongConfirmationText,
}

impl StrategyResearchCandidateRejectionReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidConfig => "invalid_config",
            Self::InvalidSource => "invalid_source",
            Self::MissingEvidence => "missing_evidence",
            Self::CandidateNotRegistered => "candidate_not_registered",
            Self::AlreadyPromoted => "already_promoted",
            Self::WrongConfirmationText => "wrong_confirmation_text",
        }
    }
}

impl std::str::FromStr for StrategyResearchCandidateRejectionReason {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "invalid_config" => Ok(Self::InvalidConfig),
            "invalid_source" => Ok(Self::InvalidSource),
            "missing_evidence" => Ok(Self::MissingEvidence),
            "candidate_not_registered" => Ok(Self::CandidateNotRegistered),
            "already_promoted" => Ok(Self::AlreadyPromoted),
            "wrong_confirmation_text" => Ok(Self::WrongConfirmationText),
            other => Err(
                CoreError::UnsupportedStrategyResearchCandidateRejectionReason(other.to_string()),
            ),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyResearchCandidateEvidence {
    pub experiment_id: Option<Uuid>,
    pub experiment_run_id: Option<Uuid>,
    pub walk_forward_id: Option<Uuid>,
    pub pnl_pct: Option<Decimal>,
    pub max_drawdown_pct: Option<Decimal>,
    pub win_rate: Option<Decimal>,
    pub trade_count: Option<i32>,
    pub fee_paid: Option<Decimal>,
    pub slippage_cost: Option<Decimal>,
    pub robustness_score: Option<Decimal>,
    pub profitable_windows: Option<i32>,
    pub losing_windows: Option<i32>,
    pub skipped_windows: Option<i32>,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyResearchCandidateScore {
    pub score: Decimal,
    pub warnings: Vec<String>,
    pub rejection_hints: Vec<StrategyResearchCandidateRejectionReason>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyResearchCandidate {
    pub id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub config: Value,
    pub source_type: StrategyResearchCandidateSource,
    pub source_id: Option<Uuid>,
    pub evidence: StrategyResearchCandidateEvidence,
    pub score: StrategyResearchCandidateScore,
    pub status: StrategyResearchCandidateStatus,
    pub created_at: DateTime<Utc>,
    pub promoted_at: Option<DateTime<Utc>>,
    pub promoted_by: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyResearchCandidatePromotionRequest {
    pub confirmation_text: String,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyResearchCandidatePromotionResult {
    pub candidate_id: Uuid,
    pub strategy_id: String,
    pub previous_config: Option<Value>,
    pub promoted_config: Value,
    pub status: StrategyResearchCandidateStatus,
    pub promoted_at: DateTime<Utc>,
    pub promoted_by: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
}

fn default_min_observation_hours() -> i64 {
    24
}

fn default_last_observed_at() -> DateTime<Utc> {
    Utc.timestamp_opt(0, 0)
        .single()
        .expect("unix epoch should be valid")
}

fn default_min_shadow_runs() -> i64 {
    30
}

fn default_min_would_submit_count() -> i64 {
    1
}

fn default_qualification_min_would_submit_count() -> i64 {
    3
}

fn default_require_readiness_ready() -> bool {
    true
}

fn default_qualification_max_risk_rejection_rate_pct() -> Decimal {
    Decimal::new(40, 0)
}

fn default_qualification_max_error_or_skipped_rate_pct() -> Decimal {
    Decimal::new(20, 0)
}

fn default_max_runner_mismatch_count() -> i64 {
    0
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyCandidateObservationStatus {
    Observing,
    ReadyForReview,
    Failed,
    InsufficientData,
    Archived,
}

impl StrategyCandidateObservationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Observing => "OBSERVING",
            Self::ReadyForReview => "READY_FOR_REVIEW",
            Self::Failed => "FAILED",
            Self::InsufficientData => "INSUFFICIENT_DATA",
            Self::Archived => "ARCHIVED",
        }
    }
}

impl std::str::FromStr for StrategyCandidateObservationStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "OBSERVING" => Ok(Self::Observing),
            "READY_FOR_REVIEW" => Ok(Self::ReadyForReview),
            "FAILED" => Ok(Self::Failed),
            "INSUFFICIENT_DATA" => Ok(Self::InsufficientData),
            "ARCHIVED" => Ok(Self::Archived),
            other => Err(CoreError::UnsupportedStrategyCandidateObservationStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyCandidateObservationDecision {
    Pass,
    Fail,
    ContinueObserving,
    InsufficientData,
}

impl StrategyCandidateObservationDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Fail => "FAIL",
            Self::ContinueObserving => "CONTINUE_OBSERVING",
            Self::InsufficientData => "INSUFFICIENT_DATA",
        }
    }
}

impl std::str::FromStr for StrategyCandidateObservationDecision {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "PASS" => Ok(Self::Pass),
            "FAIL" => Ok(Self::Fail),
            "CONTINUE_OBSERVING" => Ok(Self::ContinueObserving),
            "INSUFFICIENT_DATA" => Ok(Self::InsufficientData),
            other => Err(CoreError::UnsupportedStrategyCandidateObservationDecision(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StrategyCandidateObservationFinding {
    pub code: String,
    pub message: String,
    pub blocking: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct StrategyCandidateRunnerAlignment {
    #[serde(default)]
    pub strategy_config_matches_runner: bool,
    #[serde(default)]
    pub runner_enabled: bool,
    #[serde(default)]
    pub runner_status: String,
    #[serde(default)]
    pub runner_timeframe: String,
    #[serde(default)]
    pub runner_symbols: Vec<String>,
    #[serde(default)]
    pub runner_strategies: Vec<String>,
    #[serde(default)]
    pub mismatch_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyCandidateObservationRequirement {
    pub candidate_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    #[serde(default = "default_min_observation_hours")]
    pub min_observation_hours: i64,
    #[serde(default = "default_min_shadow_runs")]
    pub min_shadow_runs: i64,
    pub max_risk_rejection_rate: Option<Decimal>,
    #[serde(default = "default_min_would_submit_count")]
    pub min_would_submit_count: i64,
    pub max_no_signal_rate: Option<Decimal>,
    #[serde(default = "default_require_readiness_ready")]
    pub require_readiness_ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyCandidateObservationRequest {
    pub candidate_id: Uuid,
    pub start_time: Option<DateTime<Utc>>,
    #[serde(default = "default_min_observation_hours")]
    pub min_observation_hours: i64,
    #[serde(default = "default_min_shadow_runs")]
    pub min_shadow_runs: i64,
    pub max_risk_rejection_rate: Option<Decimal>,
    #[serde(default = "default_min_would_submit_count")]
    pub min_would_submit_count: i64,
    pub max_no_signal_rate: Option<Decimal>,
    #[serde(default = "default_require_readiness_ready")]
    pub require_readiness_ready: bool,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyCandidateObservationSummary {
    pub candidate_id: Uuid,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub shadow_runs: i64,
    pub would_submit_count: i64,
    pub no_signal_count: i64,
    pub risk_rejected_count: i64,
    pub skipped_count: i64,
    pub risk_rejection_rate: Decimal,
    pub no_signal_rate: Decimal,
    pub latest_readiness_status: Option<ExecutionReadinessStatus>,
    pub latest_readiness_score: Option<i32>,
    #[serde(default)]
    pub runner_alignment: StrategyCandidateRunnerAlignment,
    pub decision: StrategyCandidateObservationDecision,
    pub findings: Vec<StrategyCandidateObservationFinding>,
    #[serde(default)]
    pub recommendations: Vec<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyCandidateObservationResult {
    pub observation_id: Uuid,
    pub candidate_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub status: StrategyCandidateObservationStatus,
    pub requirements: StrategyCandidateObservationRequirement,
    #[serde(default)]
    pub runner_alignment: StrategyCandidateRunnerAlignment,
    pub summary: StrategyCandidateObservationSummary,
    pub decision: StrategyCandidateObservationDecision,
    pub started_at: DateTime<Utc>,
    pub evaluated_at: DateTime<Utc>,
    #[serde(default = "default_last_observed_at")]
    pub last_observed_at: DateTime<Utc>,
    #[serde(default)]
    pub observation_expires_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub observation_max_age_seconds: Option<i64>,
    #[serde(default)]
    pub observation_snapshot_hash: Option<String>,
    #[serde(default)]
    pub runner_config_snapshot: Option<Value>,
    #[serde(default)]
    pub readiness_snapshot: Option<Value>,
    pub created_by: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateObservationFreshnessStatus {
    Fresh,
    Stale,
    Unknown,
}

impl ResearchCandidateObservationFreshnessStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "FRESH",
            Self::Stale => "STALE",
            Self::Unknown => "UNKNOWN",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateObservationHistoryItem {
    pub observation: StrategyCandidateObservationResult,
    pub freshness_status: ResearchCandidateObservationFreshnessStatus,
    pub observation_age_seconds: Option<i64>,
    pub runner_config_drifted: bool,
    pub accept_for_shadow_eligible: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateObservationSummaryView {
    pub candidate_id: Uuid,
    pub total_observations: i64,
    pub latest_observation_status: Option<StrategyCandidateObservationStatus>,
    pub latest_formal_observation_at: Option<DateTime<Utc>>,
    pub formal_observation_stale: bool,
    pub latest_runner_alignment: Option<StrategyCandidateRunnerAlignment>,
    pub latest_readiness_status: Option<ExecutionReadinessStatus>,
    pub latest_recommendations: Vec<String>,
    pub stale_count: i64,
    pub alignment_mismatch_count: i64,
    pub runner_config_drift_count: i64,
    pub last_observed_at: Option<DateTime<Utc>>,
    pub latest_linked_shadow_run_id: Option<Uuid>,
    pub latest_linked_shadow_decision: Option<String>,
    pub latest_linked_shadow_status: Option<String>,
    pub latest_linked_shadow_run_at: Option<DateTime<Utc>>,
    pub latest_valid_shadow_run_id: Option<Uuid>,
    pub latest_valid_shadow_decision: Option<String>,
    pub latest_valid_shadow_status: Option<String>,
    pub latest_valid_shadow_run_at: Option<DateTime<Utc>>,
    pub latest_skipped_shadow_run_at: Option<DateTime<Utc>>,
    pub linked_shadow_completed_count: i64,
    pub linked_shadow_no_signal_count: i64,
    pub linked_shadow_would_submit_count: i64,
    pub linked_shadow_risk_rejected_count: i64,
    pub linked_shadow_skipped_count: i64,
    pub shadow_observation_status: Option<ResearchCandidateShadowPerformanceStatus>,
    pub current_accept_for_shadow_eligible: bool,
    pub current_accept_for_shadow_blockers: Vec<String>,
    pub computed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateShadowPerformanceStatus {
    NotPromotedToShadowConfig,
    InsufficientData,
    Healthy,
    UnderObservation,
    NeedsReview,
}

impl ResearchCandidateShadowPerformanceStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotPromotedToShadowConfig => "NOT_PROMOTED_TO_SHADOW_CONFIG",
            Self::InsufficientData => "INSUFFICIENT_DATA",
            Self::Healthy => "HEALTHY",
            Self::UnderObservation => "UNDER_OBSERVATION",
            Self::NeedsReview => "NEEDS_REVIEW",
        }
    }
}

impl std::str::FromStr for ResearchCandidateShadowPerformanceStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "NOT_PROMOTED_TO_SHADOW_CONFIG" => Ok(Self::NotPromotedToShadowConfig),
            "INSUFFICIENT_DATA" => Ok(Self::InsufficientData),
            "HEALTHY" => Ok(Self::Healthy),
            "UNDER_OBSERVATION" => Ok(Self::UnderObservation),
            "NEEDS_REVIEW" => Ok(Self::NeedsReview),
            other => Err(
                CoreError::UnsupportedResearchCandidateShadowPerformanceStatus(other.to_string()),
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateShadowPerformanceRecommendation {
    PromoteToShadowConfig,
    InsufficientData,
    KeepObserving,
    NeedsReview,
    CandidateNotCoveredByRunner,
    RejectCandidate,
}

impl ResearchCandidateShadowPerformanceRecommendation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PromoteToShadowConfig => "PROMOTE_TO_SHADOW_CONFIG",
            Self::InsufficientData => "INSUFFICIENT_DATA",
            Self::KeepObserving => "KEEP_OBSERVING",
            Self::NeedsReview => "NEEDS_REVIEW",
            Self::CandidateNotCoveredByRunner => "CANDIDATE_NOT_COVERED_BY_RUNNER",
            Self::RejectCandidate => "REJECT_CANDIDATE",
        }
    }
}

impl std::str::FromStr for ResearchCandidateShadowPerformanceRecommendation {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "PROMOTE_TO_SHADOW_CONFIG" => Ok(Self::PromoteToShadowConfig),
            "INSUFFICIENT_DATA" => Ok(Self::InsufficientData),
            "KEEP_OBSERVING" => Ok(Self::KeepObserving),
            "NEEDS_REVIEW" => Ok(Self::NeedsReview),
            "CANDIDATE_NOT_COVERED_BY_RUNNER" => Ok(Self::CandidateNotCoveredByRunner),
            "REJECT_CANDIDATE" => Ok(Self::RejectCandidate),
            other => Err(
                CoreError::UnsupportedResearchCandidateShadowPerformanceRecommendation(
                    other.to_string(),
                ),
            ),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateShadowOutcomeBreakdown {
    pub total_shadow_runs: i64,
    pub completed_shadow_runs: i64,
    pub independent_shadow_observation_count: i64,
    pub unique_evaluated_candle_count: i64,
    pub duplicate_same_candle_runs_count: i64,
    pub would_submit_count: i64,
    pub no_signal_count: i64,
    pub risk_rejected_count: i64,
    pub skipped_count: i64,
    pub error_count: i64,
    pub would_submit_rate_pct: Decimal,
    pub risk_rejection_rate_pct: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateShadowPerformance {
    pub candidate_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub total_shadow_runs: i64,
    pub would_submit_count: i64,
    pub no_signal_count: i64,
    pub risk_rejected_count: i64,
    pub skipped_count: i64,
    pub error_count: i64,
    pub would_submit_rate_pct: Decimal,
    pub risk_rejection_rate_pct: Decimal,
    pub last_shadow_run_at: Option<DateTime<Utc>>,
    pub completed_shadow_runs: i64,
    pub independent_shadow_observation_count: i64,
    pub unique_evaluated_candle_count: i64,
    pub duplicate_same_candle_runs_count: i64,
    pub latest_evaluated_candle_open_time: Option<DateTime<Utc>>,
    pub latest_valid_shadow_run_at: Option<DateTime<Utc>>,
    pub latest_skipped_shadow_run_at: Option<DateTime<Utc>>,
    pub runner_alignment_current: bool,
    pub recommendation: ResearchCandidateShadowPerformanceRecommendation,
    pub status: ResearchCandidateShadowPerformanceStatus,
    pub outcome_breakdown: ResearchCandidateShadowOutcomeBreakdown,
    pub computed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateShadowRunLink {
    pub candidate_id: Uuid,
    pub shadow_run_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub decision: String,
    pub status: String,
    pub signal_id: Option<Uuid>,
    pub risk_decision_id: Option<Uuid>,
    pub linked_at: DateTime<Utc>,
    pub shadow_created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchShadowPnlStatus {
    Attributed,
    InsufficientForwardData,
    GapDetected,
    ExtremePnl,
}

impl ResearchShadowPnlStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Attributed => "ATTRIBUTED",
            Self::InsufficientForwardData => "INSUFFICIENT_FORWARD_DATA",
            Self::GapDetected => "GAP_DETECTED",
            Self::ExtremePnl => "EXTREME_PNL",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchShadowPnlRecommendation {
    Promising,
    Weak,
    Negative,
    InsufficientData,
}

impl ResearchShadowPnlRecommendation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Promising => "PROMISING",
            Self::Weak => "WEAK",
            Self::Negative => "NEGATIVE",
            Self::InsufficientData => "INSUFFICIENT_DATA",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchShadowPnlAttributionRequest {
    pub candidate_id: Uuid,
    #[serde(default = "default_shadow_pnl_holding_windows")]
    pub holding_windows: Vec<u32>,
    #[serde(default = "default_shadow_pnl_fee_bps")]
    pub fee_bps: Decimal,
    #[serde(default = "default_shadow_pnl_slippage_bps")]
    pub slippage_bps: Decimal,
    #[serde(default = "default_shadow_pnl_extreme_threshold_pct")]
    pub extreme_pnl_threshold_pct: Decimal,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub limit: Option<i64>,
}

impl ResearchShadowPnlAttributionRequest {
    pub fn normalized_holding_windows(&self) -> Vec<u32> {
        let mut windows = self
            .holding_windows
            .iter()
            .copied()
            .filter(|value| *value > 0)
            .collect::<Vec<_>>();
        if windows.is_empty() {
            windows = default_shadow_pnl_holding_windows();
        }
        windows.sort_unstable();
        windows.dedup();
        windows
    }
}

impl Default for ResearchShadowPnlAttributionRequest {
    fn default() -> Self {
        Self {
            candidate_id: Uuid::nil(),
            holding_windows: default_shadow_pnl_holding_windows(),
            fee_bps: default_shadow_pnl_fee_bps(),
            slippage_bps: default_shadow_pnl_slippage_bps(),
            extreme_pnl_threshold_pct: default_shadow_pnl_extreme_threshold_pct(),
            start_time: None,
            end_time: None,
            limit: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchShadowPnlTradeHoldingWindowResult {
    pub holding_window: u32,
    pub status: ResearchShadowPnlStatus,
    pub attribution_status: ResearchShadowPnlStatus,
    pub exit_candle_open_time: Option<DateTime<Utc>>,
    pub exit_candle_close_time: Option<DateTime<Utc>>,
    pub exit_price: Option<Decimal>,
    pub gross_pnl_pct: Option<Decimal>,
    pub fee_bps: Decimal,
    pub slippage_bps: Decimal,
    pub net_pnl_pct: Option<Decimal>,
    pub fee_drag_pct: Decimal,
    pub candle_gap_seconds: Option<i64>,
    pub warning: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchShadowPnlAttributionTrade {
    pub candidate_id: Uuid,
    pub shadow_run_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub shadow_created_at: DateTime<Utc>,
    pub signal_time: Option<DateTime<Utc>>,
    pub status: ResearchShadowPnlStatus,
    pub attribution_status: ResearchShadowPnlStatus,
    pub entry_candle_open_time: Option<DateTime<Utc>>,
    pub entry_candle_close_time: Option<DateTime<Utc>>,
    pub entry_price: Option<Decimal>,
    pub holding_windows: Vec<ResearchShadowPnlTradeHoldingWindowResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchShadowPnlHoldingWindowResult {
    pub holding_window: u32,
    pub trade_count: i64,
    pub win_rate: Decimal,
    pub avg_net_pnl_pct: Decimal,
    pub median_net_pnl_pct: Decimal,
    pub best_net_pnl_pct: Decimal,
    pub worst_net_pnl_pct: Decimal,
    pub total_net_pnl_pct: Decimal,
    pub fee_drag_pct: Decimal,
    pub recommendation: ResearchShadowPnlRecommendation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchShadowPnlSummary {
    pub total_attributed_runs: i64,
    pub extreme_pnl_count: i64,
    pub gap_detected_count: i64,
    pub insufficient_forward_data_count: i64,
    pub negative_all_windows: bool,
    pub warnings: Vec<String>,
    pub per_holding_window: Vec<ResearchShadowPnlHoldingWindowResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchShadowPnlAttributionResult {
    pub candidate_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub holding_windows: Vec<u32>,
    pub fee_bps: Decimal,
    pub slippage_bps: Decimal,
    pub extreme_pnl_threshold_pct: Decimal,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub summary: ResearchShadowPnlSummary,
    pub trades: Vec<ResearchShadowPnlAttributionTrade>,
    pub latest_shadow_pnl_status: ResearchShadowPnlRecommendation,
    pub best_holding_window: Option<u32>,
    pub best_avg_net_pnl_pct: Option<Decimal>,
    pub computed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResearchShadowPnlRunInput {
    pub shadow_run_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub shadow_created_at: DateTime<Utc>,
    pub signal_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateQualificationStatus {
    Qualified,
    NotQualified,
    NeedsMoreData,
    Degraded,
    Unknown,
}

impl ResearchCandidateQualificationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Qualified => "QUALIFIED",
            Self::NotQualified => "NOT_QUALIFIED",
            Self::NeedsMoreData => "NEEDS_MORE_DATA",
            Self::Degraded => "DEGRADED",
            Self::Unknown => "UNKNOWN",
        }
    }
}

impl std::str::FromStr for ResearchCandidateQualificationStatus {
    type Err = crate::CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "QUALIFIED" => Ok(Self::Qualified),
            "NOT_QUALIFIED" => Ok(Self::NotQualified),
            "NEEDS_MORE_DATA" => Ok(Self::NeedsMoreData),
            "DEGRADED" => Ok(Self::Degraded),
            "UNKNOWN" => Ok(Self::Unknown),
            other => Err(crate::CoreError::UnsupportedResearchCandidateStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateQualificationTrend {
    Improving,
    Stable,
    Degrading,
    NewlyQualified,
    LostQualification,
    NeedsAttention,
    InsufficientHistory,
}

impl ResearchCandidateQualificationTrend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Improving => "IMPROVING",
            Self::Stable => "STABLE",
            Self::Degrading => "DEGRADING",
            Self::NewlyQualified => "NEWLY_QUALIFIED",
            Self::LostQualification => "LOST_QUALIFICATION",
            Self::NeedsAttention => "NEEDS_ATTENTION",
            Self::InsufficientHistory => "INSUFFICIENT_HISTORY",
        }
    }
}

impl std::str::FromStr for ResearchCandidateQualificationTrend {
    type Err = crate::CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "IMPROVING" => Ok(Self::Improving),
            "STABLE" => Ok(Self::Stable),
            "DEGRADING" => Ok(Self::Degrading),
            "NEWLY_QUALIFIED" => Ok(Self::NewlyQualified),
            "LOST_QUALIFICATION" => Ok(Self::LostQualification),
            "NEEDS_ATTENTION" => Ok(Self::NeedsAttention),
            "INSUFFICIENT_HISTORY" => Ok(Self::InsufficientHistory),
            other => Err(crate::CoreError::UnsupportedResearchCandidateStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateWatchlistStatus {
    Improving,
    Stable,
    Degrading,
    NewlyQualified,
    LostQualification,
    NeedsAttention,
    InsufficientHistory,
}

impl ResearchCandidateWatchlistStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Improving => "IMPROVING",
            Self::Stable => "STABLE",
            Self::Degrading => "DEGRADING",
            Self::NewlyQualified => "NEWLY_QUALIFIED",
            Self::LostQualification => "LOST_QUALIFICATION",
            Self::NeedsAttention => "NEEDS_ATTENTION",
            Self::InsufficientHistory => "INSUFFICIENT_HISTORY",
        }
    }
}

impl std::str::FromStr for ResearchCandidateWatchlistStatus {
    type Err = crate::CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "IMPROVING" => Ok(Self::Improving),
            "STABLE" => Ok(Self::Stable),
            "DEGRADING" => Ok(Self::Degrading),
            "NEWLY_QUALIFIED" => Ok(Self::NewlyQualified),
            "LOST_QUALIFICATION" => Ok(Self::LostQualification),
            "NEEDS_ATTENTION" => Ok(Self::NeedsAttention),
            "INSUFFICIENT_HISTORY" => Ok(Self::InsufficientHistory),
            other => Err(crate::CoreError::UnsupportedResearchCandidateStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateQualificationSeverity {
    Low,
    Medium,
    High,
    Critical,
}

impl ResearchCandidateQualificationSeverity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "LOW",
            Self::Medium => "MEDIUM",
            Self::High => "HIGH",
            Self::Critical => "CRITICAL",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateQualificationRecommendation {
    RefreshCandidateObservation,
    FixRunnerAlignment,
    ExpandShadowRunnerCoverage,
    GatherMoreShadowRuns,
    GenerateMoreWouldSubmitEvidence,
    ReviewRiskRejections,
    ReduceShadowErrorsOrSkips,
    RestoreTestnetShadowReadiness,
    ReAcceptCandidateForShadow,
    ReadyForTestnetPromotionConsideration,
}

impl ResearchCandidateQualificationRecommendation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RefreshCandidateObservation => "REFRESH_CANDIDATE_OBSERVATION",
            Self::FixRunnerAlignment => "FIX_RUNNER_ALIGNMENT",
            Self::ExpandShadowRunnerCoverage => "EXPAND_SHADOW_RUNNER_COVERAGE",
            Self::GatherMoreShadowRuns => "GATHER_MORE_SHADOW_RUNS",
            Self::GenerateMoreWouldSubmitEvidence => "GENERATE_MORE_WOULD_SUBMIT_EVIDENCE",
            Self::ReviewRiskRejections => "REVIEW_RISK_REJECTIONS",
            Self::ReduceShadowErrorsOrSkips => "REDUCE_SHADOW_ERRORS_OR_SKIPS",
            Self::RestoreTestnetShadowReadiness => "RESTORE_TESTNET_SHADOW_READINESS",
            Self::ReAcceptCandidateForShadow => "RE_ACCEPT_CANDIDATE_FOR_SHADOW",
            Self::ReadyForTestnetPromotionConsideration => {
                "READY_FOR_TESTNET_PROMOTION_CONSIDERATION"
            }
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::RefreshCandidateObservation => {
                "Run a fresh candidate observation before using shadow evidence for promotion review."
            }
            Self::FixRunnerAlignment => {
                "Align the shadow runner strategy, symbol, and timeframe with the candidate."
            }
            Self::ExpandShadowRunnerCoverage => {
                "Ensure the active shadow runner currently covers this candidate."
            }
            Self::GatherMoreShadowRuns => {
                "Accumulate more linked shadow runs before considering testnet promotion."
            }
            Self::GenerateMoreWouldSubmitEvidence => {
                "Collect more WOULD_SUBMIT outcomes to show the candidate produces actionable signals."
            }
            Self::ReviewRiskRejections => {
                "Review why shadow decisions are being rejected by risk before promotion."
            }
            Self::ReduceShadowErrorsOrSkips => {
                "Reduce skipped or error shadow runs before promotion review."
            }
            Self::RestoreTestnetShadowReadiness => {
                "Resolve TESTNET_SHADOW readiness issues before considering testnet promotion."
            }
            Self::ReAcceptCandidateForShadow => {
                "Move the candidate back to ACCEPTED_FOR_SHADOW before promotion consideration."
            }
            Self::ReadyForTestnetPromotionConsideration => {
                "Candidate has enough shadow evidence to be considered for testnet promotion review."
            }
        }
    }
}

impl std::str::FromStr for ResearchCandidateQualificationRecommendation {
    type Err = crate::CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "REFRESH_CANDIDATE_OBSERVATION" => Ok(Self::RefreshCandidateObservation),
            "FIX_RUNNER_ALIGNMENT" => Ok(Self::FixRunnerAlignment),
            "EXPAND_SHADOW_RUNNER_COVERAGE" => Ok(Self::ExpandShadowRunnerCoverage),
            "GATHER_MORE_SHADOW_RUNS" => Ok(Self::GatherMoreShadowRuns),
            "GENERATE_MORE_WOULD_SUBMIT_EVIDENCE" => Ok(Self::GenerateMoreWouldSubmitEvidence),
            "REVIEW_RISK_REJECTIONS" => Ok(Self::ReviewRiskRejections),
            "REDUCE_SHADOW_ERRORS_OR_SKIPS" => Ok(Self::ReduceShadowErrorsOrSkips),
            "RESTORE_TESTNET_SHADOW_READINESS" => Ok(Self::RestoreTestnetShadowReadiness),
            "RE_ACCEPT_CANDIDATE_FOR_SHADOW" => Ok(Self::ReAcceptCandidateForShadow),
            "READY_FOR_TESTNET_PROMOTION_CONSIDERATION" => {
                Ok(Self::ReadyForTestnetPromotionConsideration)
            }
            other => Err(crate::CoreError::UnsupportedResearchCandidateStatus(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateQualificationThresholds {
    #[serde(default = "default_min_shadow_runs")]
    pub min_shadow_runs: i64,
    #[serde(default = "default_qualification_min_would_submit_count")]
    pub min_would_submit_count: i64,
    #[serde(default = "default_qualification_max_risk_rejection_rate_pct")]
    pub max_risk_rejection_rate_pct: Decimal,
    #[serde(default = "default_qualification_max_error_or_skipped_rate_pct")]
    pub max_error_or_skipped_rate_pct: Decimal,
    #[serde(default = "default_max_runner_mismatch_count")]
    pub max_runner_mismatch_count: i64,
    #[serde(default = "default_true")]
    pub require_fresh_observation: bool,
    #[serde(default = "default_true")]
    pub require_runner_alignment: bool,
    #[serde(default = "default_true")]
    pub require_readiness_not_not_ready: bool,
}

impl Default for ResearchCandidateQualificationThresholds {
    fn default() -> Self {
        Self {
            min_shadow_runs: default_min_shadow_runs(),
            min_would_submit_count: default_qualification_min_would_submit_count(),
            max_risk_rejection_rate_pct: default_qualification_max_risk_rejection_rate_pct(),
            max_error_or_skipped_rate_pct: default_qualification_max_error_or_skipped_rate_pct(),
            max_runner_mismatch_count: default_max_runner_mismatch_count(),
            require_fresh_observation: true,
            require_runner_alignment: true,
            require_readiness_not_not_ready: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateQualificationCheck {
    pub code: String,
    pub name: String,
    pub passed: bool,
    pub blocking: bool,
    pub severity: ResearchCandidateQualificationSeverity,
    pub summary: String,
    #[serde(default)]
    pub details: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateQualificationRequest {
    pub candidate_id: Uuid,
    pub candidate_status: Option<ResearchCandidateStatus>,
    pub fresh_observation: bool,
    pub formal_observation_status: Option<StrategyCandidateObservationStatus>,
    pub formal_observation_stale: bool,
    pub runner_alignment_valid: bool,
    pub shadow_runner_covers_candidate: bool,
    #[serde(default)]
    pub runner_mismatch_count: i64,
    pub latest_readiness_status: Option<ExecutionReadinessStatus>,
    pub walk_forward_evidence: Option<ResearchCandidateWalkForwardEvidence>,
    pub shadow_performance: Option<ResearchCandidateShadowPerformance>,
    pub shadow_pnl_attribution: Option<ResearchShadowPnlAttributionResult>,
    #[serde(default)]
    pub thresholds: ResearchCandidateQualificationThresholds,
    pub computed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateQualificationResult {
    pub candidate_id: Uuid,
    pub status: ResearchCandidateQualificationStatus,
    pub score: i32,
    pub fresh_observation: bool,
    pub runner_alignment_valid: bool,
    pub latest_readiness_status: Option<ExecutionReadinessStatus>,
    pub linked_shadow_completed_count: i64,
    pub linked_shadow_no_signal_count: i64,
    pub linked_shadow_would_submit_count: i64,
    pub linked_shadow_risk_rejected_count: i64,
    pub linked_shadow_skipped_count: i64,
    pub latest_valid_shadow_run_at: Option<DateTime<Utc>>,
    pub latest_skipped_shadow_run_at: Option<DateTime<Utc>>,
    pub formal_observation_status: Option<StrategyCandidateObservationStatus>,
    pub formal_observation_stale: bool,
    pub evidence_interpretation: String,
    pub walk_forward_status: Option<StrategyWalkForwardRobustnessStatus>,
    pub walk_forward_run_id: Option<Uuid>,
    pub walk_forward_score: Option<Decimal>,
    pub walk_forward_consistency_score: Option<Decimal>,
    pub walk_forward_recommendation: Option<String>,
    #[serde(default)]
    pub walk_forward_blockers: Vec<String>,
    #[serde(default)]
    pub walk_forward_warnings: Vec<String>,
    pub readiness_penalty_points: i32,
    pub threshold_override_below_default: bool,
    pub threshold_override_penalty_points: i32,
    pub score_explanation: Vec<String>,
    pub checks: Vec<ResearchCandidateQualificationCheck>,
    pub blockers: Vec<String>,
    pub warnings: Vec<String>,
    pub recommendations: Vec<ResearchCandidateQualificationRecommendation>,
    pub thresholds: ResearchCandidateQualificationThresholds,
    pub shadow_performance: Option<ResearchCandidateShadowPerformance>,
    pub latest_shadow_pnl_status: Option<ResearchShadowPnlRecommendation>,
    pub best_holding_window: Option<u32>,
    pub best_avg_net_pnl_pct: Option<Decimal>,
    #[serde(default)]
    pub negative_all_windows: bool,
    pub computed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateQualificationEvaluation {
    pub id: Uuid,
    pub candidate_id: Uuid,
    pub status: ResearchCandidateQualificationStatus,
    pub score: i32,
    pub latest_readiness_status: Option<ExecutionReadinessStatus>,
    pub total_shadow_runs: i64,
    pub would_submit_count: i64,
    pub risk_rejection_rate_pct: Option<Decimal>,
    pub walk_forward_status: Option<StrategyWalkForwardRobustnessStatus>,
    pub walk_forward_run_id: Option<Uuid>,
    pub walk_forward_score: Option<Decimal>,
    pub walk_forward_consistency_score: Option<Decimal>,
    pub walk_forward_recommendation: Option<String>,
    #[serde(default)]
    pub walk_forward_blockers: Vec<String>,
    #[serde(default)]
    pub walk_forward_warnings: Vec<String>,
    pub warnings: Vec<String>,
    pub blockers: Vec<String>,
    pub recommendations: Vec<ResearchCandidateQualificationRecommendation>,
    pub thresholds: ResearchCandidateQualificationThresholds,
    pub evaluated_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchCandidateQualificationChange {
    pub status_changed: bool,
    pub material_score_change: bool,
    pub newly_qualified: bool,
    pub lost_qualification: bool,
    pub previous_status: Option<ResearchCandidateQualificationStatus>,
    pub current_status: ResearchCandidateQualificationStatus,
    pub previous_score: Option<i32>,
    pub current_score: i32,
    pub score_delta: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateQualificationHistory {
    pub candidate_id: Uuid,
    pub evaluations: Vec<ResearchCandidateQualificationEvaluation>,
    pub latest_change: Option<ResearchCandidateQualificationChange>,
    pub latest_trend: ResearchCandidateQualificationTrend,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateTestnetReviewStatus {
    ReadyForReview,
    NotReady,
    NeedsMoreShadowData,
    NeedsOperatorReview,
    Blocked,
}

impl ResearchCandidateTestnetReviewStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadyForReview => "READY_FOR_REVIEW",
            Self::NotReady => "NOT_READY",
            Self::NeedsMoreShadowData => "NEEDS_MORE_SHADOW_DATA",
            Self::NeedsOperatorReview => "NEEDS_OPERATOR_REVIEW",
            Self::Blocked => "BLOCKED",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateTestnetReviewSection {
    Identity,
    Qualification,
    ShadowPerformance,
    Observation,
    RunnerAlignment,
    Readiness,
    Provenance,
    WalkForward,
    OperatorReview,
    Controls,
}

impl ResearchCandidateTestnetReviewSection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Identity => "IDENTITY",
            Self::Qualification => "QUALIFICATION",
            Self::ShadowPerformance => "SHADOW_PERFORMANCE",
            Self::Observation => "OBSERVATION",
            Self::RunnerAlignment => "RUNNER_ALIGNMENT",
            Self::Readiness => "READINESS",
            Self::Provenance => "PROVENANCE",
            Self::WalkForward => "WALK_FORWARD",
            Self::OperatorReview => "OPERATOR_REVIEW",
            Self::Controls => "CONTROLS",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateTestnetReviewRecommendation {
    RefreshObservation,
    GatherMoreShadowData,
    ReevaluateQualification,
    FixRunnerAlignment,
    ClearReadinessBlockers,
    RecordReadyForTestnetReview,
    ReviewPrivateStreamFreshness,
    VerifyExperimentProvenance,
    ManualOperatorReview,
}

impl ResearchCandidateTestnetReviewRecommendation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RefreshObservation => "REFRESH_OBSERVATION",
            Self::GatherMoreShadowData => "GATHER_MORE_SHADOW_DATA",
            Self::ReevaluateQualification => "RE_EVALUATE_QUALIFICATION",
            Self::FixRunnerAlignment => "FIX_RUNNER_ALIGNMENT",
            Self::ClearReadinessBlockers => "CLEAR_READINESS_BLOCKERS",
            Self::RecordReadyForTestnetReview => "RECORD_READY_FOR_TESTNET_REVIEW",
            Self::ReviewPrivateStreamFreshness => "REVIEW_PRIVATE_STREAM_FRESHNESS",
            Self::VerifyExperimentProvenance => "VERIFY_EXPERIMENT_PROVENANCE",
            Self::ManualOperatorReview => "MANUAL_OPERATOR_REVIEW",
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::RefreshObservation => {
                "Run a fresh candidate observation before considering a controlled testnet review."
            }
            Self::GatherMoreShadowData => {
                "Collect more linked shadow evidence before testnet review."
            }
            Self::ReevaluateQualification => {
                "Record a fresh qualification evaluation for the candidate."
            }
            Self::FixRunnerAlignment => {
                "Align the active shadow runner with the candidate strategy, symbol, and timeframe."
            }
            Self::ClearReadinessBlockers => {
                "Resolve TESTNET_SHADOW readiness blockers before testnet review."
            }
            Self::RecordReadyForTestnetReview => {
                "Record MARK_READY_FOR_TESTNET_REVIEW before using this dossier for operator review."
            }
            Self::ReviewPrivateStreamFreshness => {
                "Check the private stream freshness before using this dossier for submit decisions."
            }
            Self::VerifyExperimentProvenance => {
                "Verify experiment provenance before promoting beyond research review."
            }
            Self::ManualOperatorReview => {
                "Operator review is required before any controlled testnet promotion preview."
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateTestnetReviewFinding {
    pub section: ResearchCandidateTestnetReviewSection,
    pub code: String,
    pub summary: String,
    #[serde(default)]
    pub detail: Option<String>,
    pub blocking: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateTestnetReviewChecklist {
    pub code: String,
    pub name: String,
    pub passed: bool,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateTestnetReviewEvidence {
    pub candidate_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub candidate_status: Option<ResearchCandidateStatus>,
    pub latest_review_action: Option<ResearchCandidateReview>,
    pub latest_qualification_evaluation: Option<ResearchCandidateQualificationEvaluation>,
    pub qualification_trend: ResearchCandidateQualificationTrend,
    pub shadow_performance_summary: Option<ResearchCandidateShadowPerformance>,
    pub latest_observation: Option<StrategyCandidateObservationResult>,
    pub observation_summary: Option<ResearchCandidateObservationSummaryView>,
    pub observation_freshness: ResearchCandidateObservationFreshnessStatus,
    pub observation_age_seconds: Option<i64>,
    pub observation_expires_at: Option<DateTime<Utc>>,
    pub runner_alignment: Option<StrategyCandidateRunnerAlignment>,
    pub readiness_snapshot: Option<ResearchCandidatePromotionReadiness>,
    pub source_label: String,
    pub provenance_available: bool,
    #[serde(default)]
    pub provenance_notes: Vec<String>,
    pub candidate_score: Option<Decimal>,
    pub candidate_pnl_pct: Option<Decimal>,
    pub candidate_max_drawdown_pct: Option<Decimal>,
    pub candidate_trade_count: Option<i32>,
    pub candidate_win_rate: Option<Decimal>,
    pub candidate_fee_drag: Option<Decimal>,
    pub experiment_id: Option<Uuid>,
    pub experiment_run_id: Option<Uuid>,
    pub walk_forward_evidence: Option<ResearchCandidateWalkForwardEvidence>,
    pub shadow_pnl_attribution: Option<ResearchShadowPnlAttributionResult>,
    pub exit_attribution: Option<StrategyExitAttributionResult>,
    #[serde(default)]
    pub operator_report_findings: Vec<ResearchCandidateTestnetReviewFinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateTestnetReviewDossier {
    pub candidate_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub candidate_status: Option<ResearchCandidateStatus>,
    pub status: ResearchCandidateTestnetReviewStatus,
    pub evidence: ResearchCandidateTestnetReviewEvidence,
    #[serde(default)]
    pub checklist: Vec<ResearchCandidateTestnetReviewChecklist>,
    #[serde(default)]
    pub findings: Vec<ResearchCandidateTestnetReviewFinding>,
    #[serde(default)]
    pub blockers: Vec<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub recommendations: Vec<ResearchCandidateTestnetReviewRecommendation>,
    pub generated_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}

#[derive(Debug, Clone)]
pub struct ResearchCandidateTestnetReviewRequest {
    pub candidate: Option<ResearchCandidate>,
    pub latest_review_action: Option<ResearchCandidateReview>,
    pub ready_review_action_recorded: bool,
    pub latest_qualification_evaluation: Option<ResearchCandidateQualificationEvaluation>,
    pub qualification_trend: ResearchCandidateQualificationTrend,
    pub qualification_evaluation_recent: bool,
    pub shadow_performance_summary: Option<ResearchCandidateShadowPerformance>,
    pub latest_observation: Option<StrategyCandidateObservationResult>,
    pub observation_summary: Option<ResearchCandidateObservationSummaryView>,
    pub observation_freshness: ResearchCandidateObservationFreshnessStatus,
    pub observation_age_seconds: Option<i64>,
    pub runner_alignment: Option<StrategyCandidateRunnerAlignment>,
    pub readiness_snapshot: Option<ResearchCandidatePromotionReadiness>,
    pub walk_forward_evidence: Option<ResearchCandidateWalkForwardEvidence>,
    pub shadow_pnl_attribution: Option<ResearchShadowPnlAttributionResult>,
    pub exit_attribution: Option<StrategyExitAttributionResult>,
    pub private_stream_stale_warning: bool,
    pub require_ready_review_action: bool,
    pub no_execution_table_mutation: bool,
    pub generated_at: DateTime<Utc>,
    pub correlation_id: Uuid,
    pub operator_report_findings: Vec<ResearchCandidateTestnetReviewFinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateWatchlistEntry {
    pub candidate_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub candidate_status: ResearchCandidateStatus,
    pub latest_evaluation: Option<ResearchCandidateQualificationEvaluation>,
    pub walk_forward_evidence: Option<ResearchCandidateWalkForwardEvidence>,
    pub latest_change: Option<ResearchCandidateQualificationChange>,
    pub trend: ResearchCandidateQualificationTrend,
    pub watchlist_status: ResearchCandidateWatchlistStatus,
}

fn qualification_status_counts_as_qualified(status: ResearchCandidateQualificationStatus) -> bool {
    status == ResearchCandidateQualificationStatus::Qualified
}

pub fn research_candidate_qualification_change(
    current: &ResearchCandidateQualificationEvaluation,
    previous: Option<&ResearchCandidateQualificationEvaluation>,
) -> Option<ResearchCandidateQualificationChange> {
    previous.map(|previous| {
        let score_delta = current.score - previous.score;
        let newly_qualified = !qualification_status_counts_as_qualified(previous.status)
            && qualification_status_counts_as_qualified(current.status);
        let lost_qualification = qualification_status_counts_as_qualified(previous.status)
            && !qualification_status_counts_as_qualified(current.status);

        ResearchCandidateQualificationChange {
            status_changed: previous.status != current.status,
            material_score_change: score_delta.abs() >= 10,
            newly_qualified,
            lost_qualification,
            previous_status: Some(previous.status),
            current_status: current.status,
            previous_score: Some(previous.score),
            current_score: current.score,
            score_delta,
        }
    })
}

pub fn research_candidate_qualification_trend(
    current: &ResearchCandidateQualificationEvaluation,
    previous: Option<&ResearchCandidateQualificationEvaluation>,
) -> ResearchCandidateQualificationTrend {
    let Some(change) = research_candidate_qualification_change(current, previous) else {
        return ResearchCandidateQualificationTrend::InsufficientHistory;
    };

    if change.newly_qualified {
        ResearchCandidateQualificationTrend::NewlyQualified
    } else if change.lost_qualification {
        ResearchCandidateQualificationTrend::LostQualification
    } else if matches!(
        current.status,
        ResearchCandidateQualificationStatus::Degraded
            | ResearchCandidateQualificationStatus::NotQualified
    ) {
        ResearchCandidateQualificationTrend::NeedsAttention
    } else if change.score_delta >= 10 {
        ResearchCandidateQualificationTrend::Improving
    } else if change.score_delta <= -10 {
        ResearchCandidateQualificationTrend::Degrading
    } else {
        ResearchCandidateQualificationTrend::Stable
    }
}

pub fn research_candidate_watchlist_status(
    latest: Option<&ResearchCandidateQualificationEvaluation>,
    trend: ResearchCandidateQualificationTrend,
    now: DateTime<Utc>,
    stale_after: Duration,
) -> ResearchCandidateWatchlistStatus {
    let Some(latest) = latest else {
        return ResearchCandidateWatchlistStatus::InsufficientHistory;
    };

    if now - latest.evaluated_at > stale_after {
        return ResearchCandidateWatchlistStatus::NeedsAttention;
    }

    match trend {
        ResearchCandidateQualificationTrend::Improving => {
            ResearchCandidateWatchlistStatus::Improving
        }
        ResearchCandidateQualificationTrend::Stable => ResearchCandidateWatchlistStatus::Stable,
        ResearchCandidateQualificationTrend::Degrading => {
            ResearchCandidateWatchlistStatus::Degrading
        }
        ResearchCandidateQualificationTrend::NewlyQualified => {
            ResearchCandidateWatchlistStatus::NewlyQualified
        }
        ResearchCandidateQualificationTrend::LostQualification => {
            ResearchCandidateWatchlistStatus::LostQualification
        }
        ResearchCandidateQualificationTrend::NeedsAttention => {
            ResearchCandidateWatchlistStatus::NeedsAttention
        }
        ResearchCandidateQualificationTrend::InsufficientHistory => {
            ResearchCandidateWatchlistStatus::InsufficientHistory
        }
    }
}

pub fn is_research_candidate_evaluation_stale(
    evaluation: &ResearchCandidateQualificationEvaluation,
    now: DateTime<Utc>,
    stale_after: Duration,
) -> bool {
    now - evaluation.evaluated_at > stale_after
}

fn testnet_review_checklist_item(
    code: &str,
    name: &str,
    passed: bool,
    summary: impl Into<String>,
) -> ResearchCandidateTestnetReviewChecklist {
    ResearchCandidateTestnetReviewChecklist {
        code: code.to_string(),
        name: name.to_string(),
        passed,
        summary: summary.into(),
    }
}

fn testnet_review_finding(
    section: ResearchCandidateTestnetReviewSection,
    code: &str,
    summary: impl Into<String>,
    detail: Option<String>,
    blocking: bool,
) -> ResearchCandidateTestnetReviewFinding {
    ResearchCandidateTestnetReviewFinding {
        section,
        code: code.to_string(),
        summary: summary.into(),
        detail,
        blocking,
    }
}

fn near_observation_expiry(age_seconds: Option<i64>, max_age_seconds: Option<i64>) -> bool {
    match (age_seconds, max_age_seconds) {
        (Some(age), Some(max_age)) if max_age > 0 => age.saturating_mul(5) >= max_age * 4,
        _ => false,
    }
}

fn checklist_summary(passed: bool, success: &'static str, failure: &'static str) -> String {
    if passed {
        success.to_string()
    } else {
        failure.to_string()
    }
}

fn accept_shadow_check(
    code: &str,
    name: &str,
    passed: bool,
    blocking: bool,
    summary: impl Into<String>,
    details: Option<Value>,
) -> ResearchCandidateAcceptForShadowCheck {
    ResearchCandidateAcceptForShadowCheck {
        code: code.to_string(),
        name: name.to_string(),
        passed,
        blocking,
        summary: summary.into(),
        details,
    }
}

pub fn evaluate_research_candidate_accept_for_shadow_preview(
    input: ResearchCandidateAcceptForShadowPreviewInput,
) -> ResearchCandidateAcceptForShadowPreviewResult {
    let candidate = input.candidate;
    let mut checks = Vec::new();
    let mut blockers = Vec::new();
    let mut warnings = Vec::new();

    let status_eligible = matches!(
        candidate.status,
        ResearchCandidateStatus::Discovered | ResearchCandidateStatus::Observing
    );
    checks.push(accept_shadow_check(
        "candidate_pre_acceptance_status",
        "Candidate is in a pre-acceptance state",
        status_eligible,
        true,
        if status_eligible {
            "Candidate is eligible for accept-for-shadow preview."
        } else {
            "Candidate is already accepted, promoted, rejected, archived, or otherwise not eligible for pre-acceptance."
        },
        Some(json!({ "current_status": candidate.status.as_str() })),
    ));
    if !status_eligible {
        blockers.push(format!(
            "Candidate status {} is not eligible for accept-for-shadow preview.",
            candidate.status.as_str()
        ));
    }

    checks.push(accept_shadow_check(
        "source_experiment_exists",
        "Source experiment exists",
        input.source_experiment_exists,
        true,
        if input.source_experiment_exists {
            "Source experiment provenance is present."
        } else {
            "Source experiment provenance is missing."
        },
        Some(json!({
            "experiment_id": input.evidence_summary.source_experiment_id,
            "experiment_run_id": input.evidence_summary.source_experiment_run_id
        })),
    ));
    if !input.source_experiment_exists {
        blockers.push("Source experiment provenance is missing.".to_string());
    }

    let walk_forward_robust = input.evidence_summary.walk_forward_status
        == Some(StrategyWalkForwardRobustnessStatus::Robust);
    checks.push(accept_shadow_check(
        "source_walk_forward_robust",
        "Source walk-forward exists and is ROBUST",
        input.source_walk_forward_exists && walk_forward_robust,
        true,
        if input.source_walk_forward_exists && walk_forward_robust {
            "Source walk-forward evidence is ROBUST."
        } else if input.source_walk_forward_exists {
            "Source walk-forward evidence is not ROBUST."
        } else {
            "Source walk-forward evidence is missing."
        },
        Some(json!({
            "walk_forward_run_id": input.evidence_summary.source_walk_forward_run_id,
            "walk_forward_status": input.evidence_summary.walk_forward_status
        })),
    ));
    if !input.source_walk_forward_exists {
        blockers.push("Source walk-forward evidence is missing.".to_string());
    } else if !walk_forward_robust {
        blockers.push("Source walk-forward evidence is not ROBUST.".to_string());
    }

    let robustness_matrix_robust = input.evidence_summary.robustness_matrix_status
        == Some(StrategyRobustnessMatrixStatus::Robust);
    checks.push(accept_shadow_check(
        "source_robustness_matrix_robust",
        "Source robustness matrix exists and is ROBUST",
        input.source_robustness_matrix_exists && robustness_matrix_robust,
        true,
        if input.source_robustness_matrix_exists && robustness_matrix_robust {
            "Source robustness matrix evidence is ROBUST."
        } else if input.source_robustness_matrix_exists {
            "Source robustness matrix evidence is not ROBUST."
        } else {
            "Source robustness matrix evidence is missing."
        },
        Some(json!({
            "robustness_matrix_run_id": input.evidence_summary.source_robustness_matrix_run_id,
            "robustness_matrix_status": input.evidence_summary.robustness_matrix_status
        })),
    ));
    if !input.source_robustness_matrix_exists {
        blockers.push("Source robustness matrix evidence is missing.".to_string());
    } else if !robustness_matrix_robust {
        blockers.push("Source robustness matrix evidence is not ROBUST.".to_string());
    }

    let data_quality_good =
        input.evidence_summary.data_quality_status == Some(MarketDataQualityStatus::Good);
    checks.push(accept_shadow_check(
        "data_quality_good",
        "Data quality is GOOD",
        data_quality_good,
        true,
        if data_quality_good {
            "Data quality is GOOD for the evaluated window."
        } else {
            "Data quality is not GOOD for the evaluated window."
        },
        Some(json!({ "data_quality_status": input.evidence_summary.data_quality_status })),
    ));
    if !data_quality_good {
        blockers.push("Data quality is not GOOD for the evaluated window.".to_string());
    }

    checks.push(accept_shadow_check(
        "fresh_observation_exists",
        "Fresh observation exists",
        !input.require_fresh_observation || input.fresh_observation,
        input.require_fresh_observation,
        if input.fresh_observation {
            "A fresh persisted observation exists."
        } else if input.require_fresh_observation {
            "A fresh persisted observation is required but missing."
        } else {
            "Fresh observation is not required by this preview request."
        },
        None,
    ));
    if input.require_fresh_observation && !input.fresh_observation {
        warnings.push("Fresh persisted observation is missing.".to_string());
    }

    checks.push(accept_shadow_check(
        "candidate_not_already_terminal_or_accepted",
        "Candidate is not already accepted/promoted/rejected/archived",
        status_eligible,
        true,
        if status_eligible {
            "Candidate has not already moved beyond pre-acceptance review."
        } else {
            "Candidate has already moved beyond pre-acceptance review."
        },
        Some(json!({ "current_status": candidate.status.as_str() })),
    ));

    let runner_aligned = input.runner_alignment.strategy_config_matches_runner;
    checks.push(accept_shadow_check(
        "runner_alignment",
        "Runner alignment status",
        runner_aligned,
        false,
        if runner_aligned {
            "Current shadow runner config already covers the candidate."
        } else {
            "Current shadow runner config does not cover the candidate."
        },
        Some(json!({
            "mismatch_reasons": input.runner_alignment.mismatch_reasons.clone(),
            "required_runner_config_change": input.required_runner_config_change.clone()
        })),
    ));
    if !runner_aligned {
        warnings.push("Current shadow runner config does not cover the candidate.".to_string());
    }

    checks.push(accept_shadow_check(
        "symbol_specific_generalization",
        "Evidence is symbol-specific",
        true,
        false,
        "Preview treats this as symbol-specific evidence only.",
        Some(json!({
            "symbol": candidate.symbol.clone(),
            "btc_generalization_failed": input.btc_generalization_failed
        })),
    ));
    if input.btc_generalization_failed {
        warnings.push(
            "BTC/generalization evidence is failed or unavailable; candidate is ETH-specific only."
                .to_string(),
        );
    }

    checks.push(accept_shadow_check(
        "holdout_2025_available",
        "2025+ holdout data is available",
        input.holdout_2025_available,
        false,
        if input.holdout_2025_available {
            "2025+ holdout candles are available for later validation."
        } else {
            "2025+ holdout evidence is unavailable."
        },
        None,
    ));
    if !input.holdout_2025_available {
        warnings.push("2025+ holdout evidence is unavailable.".to_string());
    }

    checks.push(accept_shadow_check(
        "no_shadow_runs_yet",
        "No shadow runs yet",
        input.evidence_summary.shadow_run_count > 0,
        false,
        if input.evidence_summary.shadow_run_count > 0 {
            "Linked shadow runs exist."
        } else {
            "No linked shadow runs exist yet."
        },
        Some(json!({ "shadow_run_count": input.evidence_summary.shadow_run_count })),
    ));
    if input.evidence_summary.shadow_run_count == 0 {
        warnings.push("No linked shadow runs exist yet.".to_string());
    }

    let status = if !blockers.is_empty() {
        ResearchCandidateAcceptForShadowPreviewStatus::Blocked
    } else if input.require_fresh_observation && !input.fresh_observation {
        ResearchCandidateAcceptForShadowPreviewStatus::NeedsMoreData
    } else if !warnings.is_empty() {
        ResearchCandidateAcceptForShadowPreviewStatus::WarningReviewRequired
    } else {
        ResearchCandidateAcceptForShadowPreviewStatus::ReadyForHumanAcceptance
    };

    let recommended_action = match status {
        ResearchCandidateAcceptForShadowPreviewStatus::ReadyForHumanAcceptance => {
            "HUMAN_ACCEPT_FOR_SHADOW_REVIEW"
        }
        ResearchCandidateAcceptForShadowPreviewStatus::NeedsMoreData => "RUN_CANDIDATE_OBSERVATION",
        ResearchCandidateAcceptForShadowPreviewStatus::WarningReviewRequired => {
            "HUMAN_REVIEW_WARNINGS_BEFORE_ACCEPTANCE"
        }
        ResearchCandidateAcceptForShadowPreviewStatus::Blocked
            if candidate.status == ResearchCandidateStatus::AcceptedForShadow =>
        {
            "PREVIEW_SHADOW_RUNNER_PROMOTION"
        }
        ResearchCandidateAcceptForShadowPreviewStatus::Blocked => "KEEP_DISCOVERED",
    }
    .to_string();

    ResearchCandidateAcceptForShadowPreviewResult {
        candidate_id: candidate.id,
        current_status: candidate.status,
        strategy_id: candidate.strategy_id.clone(),
        symbol: candidate.symbol.clone(),
        timeframe: candidate.timeframe.clone(),
        evidence_summary: input.evidence_summary,
        checks,
        blockers,
        warnings,
        recommended_action,
        runner_alignment: input.runner_alignment,
        required_runner_config_change: input.required_runner_config_change,
        status,
        no_mutation: true,
        generated_at: input.generated_at,
    }
}

pub fn validate_research_candidate_accept_for_shadow_apply(
    candidate_id: Uuid,
    preview: &ResearchCandidateAcceptForShadowPreviewResult,
    confirm: &str,
    acknowledge_warnings: bool,
) -> Result<(), ResearchCandidateAcceptForShadowApplyRejection> {
    if !is_valid_research_candidate_accept_shadow_confirmation(candidate_id, confirm) {
        return Err(ResearchCandidateAcceptForShadowApplyRejection::InvalidConfirmation);
    }
    if !matches!(
        preview.current_status,
        ResearchCandidateStatus::Discovered | ResearchCandidateStatus::Observing
    ) {
        return Err(ResearchCandidateAcceptForShadowApplyRejection::CandidateNotEligible);
    }
    if !preview.blockers.is_empty() {
        return Err(ResearchCandidateAcceptForShadowApplyRejection::PreviewBlocked);
    }
    if !matches!(
        preview.status,
        ResearchCandidateAcceptForShadowPreviewStatus::ReadyForHumanAcceptance
            | ResearchCandidateAcceptForShadowPreviewStatus::WarningReviewRequired
    ) {
        return Err(ResearchCandidateAcceptForShadowApplyRejection::PreviewNotReady);
    }
    if !preview.warnings.is_empty() && !acknowledge_warnings {
        return Err(ResearchCandidateAcceptForShadowApplyRejection::WarningAcknowledgementRequired);
    }
    Ok(())
}

fn provenance_label(candidate: Option<&ResearchCandidate>) -> String {
    match candidate {
        Some(value) if value.experiment_run_id.is_some() => "EXPERIMENT_RUN".to_string(),
        Some(_) => "MANUAL".to_string(),
        None => "UNKNOWN".to_string(),
    }
}

pub fn evaluate_research_candidate_testnet_review_dossier(
    request: &ResearchCandidateTestnetReviewRequest,
) -> ResearchCandidateTestnetReviewDossier {
    let candidate = request.candidate.as_ref();
    let candidate_id = candidate
        .map(|value| value.id)
        .or_else(|| {
            request
                .latest_observation
                .as_ref()
                .map(|value| value.candidate_id)
                .or_else(|| {
                    request
                        .latest_qualification_evaluation
                        .as_ref()
                        .map(|value| value.candidate_id)
                })
        })
        .unwrap_or(request.correlation_id);
    let strategy_id = candidate
        .map(|value| value.strategy_id.clone())
        .or_else(|| {
            request
                .latest_observation
                .as_ref()
                .map(|value| value.strategy_id.clone())
        })
        .or_else(|| {
            request
                .shadow_performance_summary
                .as_ref()
                .map(|value| value.strategy_id.clone())
        })
        .unwrap_or_else(|| "unknown".to_string());
    let symbol = candidate
        .map(|value| value.symbol.clone())
        .or_else(|| {
            request
                .latest_observation
                .as_ref()
                .map(|value| value.symbol.clone())
        })
        .or_else(|| {
            request
                .shadow_performance_summary
                .as_ref()
                .map(|value| value.symbol.clone())
        })
        .unwrap_or_else(|| "unknown".to_string());
    let timeframe = candidate
        .map(|value| value.timeframe.clone())
        .or_else(|| {
            request
                .latest_observation
                .as_ref()
                .map(|value| value.timeframe.clone())
        })
        .or_else(|| {
            request
                .shadow_performance_summary
                .as_ref()
                .map(|value| value.timeframe.clone())
        })
        .unwrap_or_else(|| "unknown".to_string());

    let latest_qualification = request.latest_qualification_evaluation.as_ref();
    let latest_review = request.latest_review_action.as_ref();
    let shadow_performance = request.shadow_performance_summary.as_ref();
    let latest_observation = request.latest_observation.as_ref();
    let readiness = request.readiness_snapshot.as_ref();
    let runner_alignment = request.runner_alignment.as_ref();
    let default_thresholds = ResearchCandidateQualificationThresholds::default();
    let total_shadow_runs = shadow_performance
        .map(|value| value.total_shadow_runs)
        .or_else(|| latest_qualification.map(|value| value.total_shadow_runs))
        .unwrap_or(0);
    let independent_shadow_observation_count = shadow_performance
        .map(|value| value.independent_shadow_observation_count)
        .or_else(|| latest_qualification.map(|value| value.total_shadow_runs))
        .unwrap_or(0);
    let duplicate_same_candle_runs_count = shadow_performance
        .map(|value| value.duplicate_same_candle_runs_count)
        .unwrap_or(0);
    let valid_shadow_runs = shadow_performance
        .map(|value| value.completed_shadow_runs + value.risk_rejected_count)
        .unwrap_or(0);
    let would_submit_count = shadow_performance
        .map(|value| value.would_submit_count)
        .or_else(|| latest_qualification.map(|value| value.would_submit_count))
        .unwrap_or(0);
    let skipped_shadow_runs = shadow_performance
        .map(|value| value.skipped_count)
        .unwrap_or(0);
    let valid_shadow_evidence_available = valid_shadow_runs > 0
        || shadow_performance
            .and_then(|value| value.latest_valid_shadow_run_at)
            .is_some();
    let current_status = candidate.map(|value| value.status);
    let accepted_for_shadow = matches!(
        current_status,
        Some(
            ResearchCandidateStatus::AcceptedForShadow
                | ResearchCandidateStatus::PromotedToShadowConfig
        )
    );
    let promoted_to_shadow_config =
        current_status == Some(ResearchCandidateStatus::PromotedToShadowConfig);
    let fresh_observation =
        request.observation_freshness == ResearchCandidateObservationFreshnessStatus::Fresh;
    let runner_matches = runner_alignment
        .map(|value| value.strategy_config_matches_runner)
        .unwrap_or(false);
    let readiness_status = readiness.and_then(|value| value.readiness_status);
    let latest_qualification_status = latest_qualification.map(|value| value.status);
    let walk_forward = request.walk_forward_evidence.as_ref();
    let threshold_override_below_default = latest_qualification
        .map(|value| {
            value.thresholds.min_shadow_runs < default_thresholds.min_shadow_runs
                || value.thresholds.min_would_submit_count
                    < default_thresholds.min_would_submit_count
        })
        .unwrap_or(false);

    let mut findings = Vec::new();
    let mut recommendations = BTreeSet::new();

    if candidate.is_none() {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::Identity,
            "candidate_missing",
            "Research candidate was not found.",
            None,
            true,
        ));
    }

    if matches!(
        current_status,
        Some(ResearchCandidateStatus::Rejected | ResearchCandidateStatus::Archived)
    ) {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::Identity,
            "candidate_inactive",
            format!(
                "Candidate status {} is blocked for testnet review.",
                current_status
                    .expect("status should exist for inactive candidate")
                    .as_str()
            ),
            None,
            true,
        ));
    }

    if matches!(
        current_status,
        Some(ResearchCandidateStatus::Discovered | ResearchCandidateStatus::Observing)
    ) {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::Identity,
            "candidate_not_accepted_for_shadow",
            "Candidate is not yet accepted for shadow review promotion staging.",
            None,
            true,
        ));
        recommendations.insert(ResearchCandidateTestnetReviewRecommendation::ManualOperatorReview);
    }

    if accepted_for_shadow && !promoted_to_shadow_config {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::RunnerAlignment,
            "candidate_not_promoted_to_shadow_config",
            "Candidate is accepted but not promoted to shadow runner config.",
            None,
            true,
        ));
        recommendations.insert(ResearchCandidateTestnetReviewRecommendation::FixRunnerAlignment);
    }

    if !fresh_observation && valid_shadow_evidence_available {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::Observation,
            "formal_observation_stale",
            "Formal observation is stale or missing; valid linked shadow evidence is shown separately.",
            latest_observation
                .and_then(|value| value.observation_expires_at)
                .map(|value| format!("formal observation expires at {}", value.to_rfc3339())),
            false,
        ));
        recommendations.insert(ResearchCandidateTestnetReviewRecommendation::RefreshObservation);
    } else if !fresh_observation {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::Observation,
            "fresh_observation_missing",
            "Fresh formal observation is missing or stale and no valid linked shadow evidence exists.",
            latest_observation
                .and_then(|value| value.observation_expires_at)
                .map(|value| format!("observation expires at {}", value.to_rfc3339())),
            true,
        ));
        recommendations.insert(ResearchCandidateTestnetReviewRecommendation::RefreshObservation);
    } else if near_observation_expiry(
        request.observation_age_seconds,
        latest_observation.and_then(|value| value.observation_max_age_seconds),
    ) {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::Observation,
            "observation_near_expiry",
            "Fresh observation is close to expiry.",
            None,
            false,
        ));
        recommendations.insert(ResearchCandidateTestnetReviewRecommendation::RefreshObservation);
    }

    if latest_qualification_status == Some(ResearchCandidateQualificationStatus::NotQualified) {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::Qualification,
            "qualification_not_qualified",
            "Latest qualification evaluation is NOT_QUALIFIED.",
            None,
            true,
        ));
        recommendations
            .insert(ResearchCandidateTestnetReviewRecommendation::ReevaluateQualification);
    } else if latest_qualification_status == Some(ResearchCandidateQualificationStatus::Degraded) {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::Qualification,
            "qualification_degraded",
            "Latest qualification evaluation is DEGRADED.",
            None,
            false,
        ));
        recommendations.insert(ResearchCandidateTestnetReviewRecommendation::ManualOperatorReview);
    } else if latest_qualification.is_none() || !request.qualification_evaluation_recent {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::Qualification,
            "qualification_not_recent",
            "No recent qualification evaluation is available.",
            latest_qualification
                .map(|value| format!("latest evaluation at {}", value.evaluated_at.to_rfc3339())),
            false,
        ));
        recommendations
            .insert(ResearchCandidateTestnetReviewRecommendation::ReevaluateQualification);
    }

    if threshold_override_below_default {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::Qualification,
            "threshold_override_below_default",
            "Qualification used a threshold override below defaults.",
            None,
            false,
        ));
        recommendations.insert(ResearchCandidateTestnetReviewRecommendation::ManualOperatorReview);
    }

    match walk_forward.map(|value| value.robustness_status) {
        Some(StrategyWalkForwardRobustnessStatus::Robust) => {}
        Some(StrategyWalkForwardRobustnessStatus::OverfitRisk) => {
            findings.push(testnet_review_finding(
                ResearchCandidateTestnetReviewSection::WalkForward,
                "walk_forward_overfit_risk",
                "Walk-forward robustness is OVERFIT_RISK.",
                walk_forward.map(|value| {
                    format!(
                        "run={} recommendation={}",
                        value.walk_forward_run_id,
                        value.recommendation_reason.clone().unwrap_or_else(|| {
                            "Do not accept candidate until walk-forward robustness improves."
                                .to_string()
                        })
                    )
                }),
                true,
            ));
            recommendations
                .insert(ResearchCandidateTestnetReviewRecommendation::ManualOperatorReview);
        }
        Some(StrategyWalkForwardRobustnessStatus::Failed) => {
            findings.push(testnet_review_finding(
                ResearchCandidateTestnetReviewSection::WalkForward,
                "walk_forward_failed",
                "Walk-forward validation failed.",
                walk_forward.map(|value| format!("run={}", value.walk_forward_run_id)),
                true,
            ));
            recommendations
                .insert(ResearchCandidateTestnetReviewRecommendation::ManualOperatorReview);
        }
        Some(StrategyWalkForwardRobustnessStatus::InsufficientData) => {
            findings.push(testnet_review_finding(
                ResearchCandidateTestnetReviewSection::WalkForward,
                "walk_forward_insufficient_data",
                "Walk-forward validation has insufficient data.",
                walk_forward.map(|value| format!("run={}", value.walk_forward_run_id)),
                true,
            ));
            recommendations
                .insert(ResearchCandidateTestnetReviewRecommendation::ManualOperatorReview);
        }
        Some(StrategyWalkForwardRobustnessStatus::Weak) => {
            findings.push(testnet_review_finding(
                ResearchCandidateTestnetReviewSection::WalkForward,
                "walk_forward_weak",
                "Walk-forward robustness is WEAK.",
                walk_forward.map(|value| format!("run={}", value.walk_forward_run_id)),
                false,
            ));
            recommendations
                .insert(ResearchCandidateTestnetReviewRecommendation::ManualOperatorReview);
        }
        None => {
            findings.push(testnet_review_finding(
                ResearchCandidateTestnetReviewSection::WalkForward,
                "walk_forward_missing",
                "Walk-forward evidence is missing.",
                None,
                true,
            ));
            recommendations
                .insert(ResearchCandidateTestnetReviewRecommendation::ManualOperatorReview);
        }
    }

    if !runner_matches {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::RunnerAlignment,
            "runner_mismatch",
            "Current runner alignment does not match the candidate.",
            runner_alignment
                .map(|value| value.mismatch_reasons.join(" | "))
                .filter(|value| !value.is_empty()),
            true,
        ));
        recommendations.insert(ResearchCandidateTestnetReviewRecommendation::FixRunnerAlignment);
    }

    if readiness_status == Some(ExecutionReadinessStatus::NotReady)
        || readiness.map(|value| !value.is_ready).unwrap_or(false)
    {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::Readiness,
            "readiness_not_ready",
            "Latest readiness snapshot is NOT_READY for testnet review purposes.",
            readiness
                .map(|value| value.blockers.join(" | "))
                .filter(|value| !value.is_empty()),
            true,
        ));
        recommendations
            .insert(ResearchCandidateTestnetReviewRecommendation::ClearReadinessBlockers);
    } else if matches!(
        readiness_status,
        Some(ExecutionReadinessStatus::Degraded | ExecutionReadinessStatus::Unknown) | None
    ) {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::Readiness,
            "readiness_requires_review",
            "Latest readiness snapshot is degraded, unknown, or missing.",
            readiness_status.map(|value| value.as_str().to_string()),
            false,
        ));
        recommendations.insert(ResearchCandidateTestnetReviewRecommendation::ManualOperatorReview);
    }

    if total_shadow_runs <= 0 {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::ShadowPerformance,
            "shadow_runs_missing",
            "No linked shadow runs are available for review.",
            None,
            true,
        ));
        recommendations.insert(ResearchCandidateTestnetReviewRecommendation::GatherMoreShadowData);
    } else if independent_shadow_observation_count < default_thresholds.min_shadow_runs {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::ShadowPerformance,
            "shadow_runs_below_default",
            format!(
                "Independent linked shadow observations {} are below the default minimum {}.",
                independent_shadow_observation_count, default_thresholds.min_shadow_runs
            ),
            Some(format!(
                "total_linked_shadow_runs={} duplicate_same_candle_runs={}",
                total_shadow_runs, duplicate_same_candle_runs_count
            )),
            true,
        ));
        recommendations.insert(ResearchCandidateTestnetReviewRecommendation::GatherMoreShadowData);
    }

    if would_submit_count < default_thresholds.min_would_submit_count {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::ShadowPerformance,
            "would_submit_evidence_insufficient",
            format!(
                "WOULD_SUBMIT evidence {} is below the default minimum {}.",
                would_submit_count, default_thresholds.min_would_submit_count
            ),
            shadow_performance.map(|value| {
                format!(
                    "NO_SIGNAL={} RISK_REJECTED={} skipped={} latest_valid_shadow_run_at={}",
                    value.no_signal_count,
                    value.risk_rejected_count,
                    value.skipped_count,
                    value
                        .latest_valid_shadow_run_at
                        .map(|time| time.to_rfc3339())
                        .unwrap_or_else(|| "none".to_string())
                )
            }),
            true,
        ));
        recommendations.insert(ResearchCandidateTestnetReviewRecommendation::GatherMoreShadowData);
    }

    if skipped_shadow_runs > 0 {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::ShadowPerformance,
            "shadow_skips_present",
            format!(
                "Skipped shadow runs are present and counted separately (skipped={}).",
                skipped_shadow_runs
            ),
            shadow_performance
                .and_then(|value| value.latest_skipped_shadow_run_at)
                .map(|time| format!("latest skipped shadow run at {}", time.to_rfc3339())),
            false,
        ));
        recommendations.insert(ResearchCandidateTestnetReviewRecommendation::GatherMoreShadowData);
    }

    if request.require_ready_review_action && !request.ready_review_action_recorded {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::OperatorReview,
            "ready_review_action_missing",
            "MARK_READY_FOR_TESTNET_REVIEW has not been recorded.",
            latest_review.map(|value| format!("latest review action is {}", value.action.as_str())),
            true,
        ));
        recommendations
            .insert(ResearchCandidateTestnetReviewRecommendation::RecordReadyForTestnetReview);
    }

    if request.private_stream_stale_warning {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::Controls,
            "private_stream_stale",
            "Exchange private stream freshness is stale for review context.",
            None,
            false,
        ));
        recommendations
            .insert(ResearchCandidateTestnetReviewRecommendation::ReviewPrivateStreamFreshness);
    }

    let provenance_available = candidate
        .map(|value| value.experiment_id.is_some() || value.experiment_run_id.is_some())
        .unwrap_or(false);
    if !provenance_available {
        findings.push(testnet_review_finding(
            ResearchCandidateTestnetReviewSection::Provenance,
            "experiment_provenance_missing",
            "Experiment provenance is missing; manual verification is required.",
            None,
            false,
        ));
        recommendations
            .insert(ResearchCandidateTestnetReviewRecommendation::VerifyExperimentProvenance);
    }

    findings.extend(request.operator_report_findings.iter().cloned());

    let blockers = findings
        .iter()
        .filter(|item| item.blocking)
        .map(|item| item.summary.clone())
        .collect::<Vec<_>>();
    let warnings = findings
        .iter()
        .filter(|item| !item.blocking)
        .map(|item| item.summary.clone())
        .collect::<Vec<_>>();

    let status = if !blockers.is_empty() {
        ResearchCandidateTestnetReviewStatus::Blocked
    } else if total_shadow_runs <= 0 || total_shadow_runs < default_thresholds.min_shadow_runs {
        ResearchCandidateTestnetReviewStatus::NeedsMoreShadowData
    } else if latest_qualification_status == Some(ResearchCandidateQualificationStatus::Degraded)
        || !request.qualification_evaluation_recent
        || request.private_stream_stale_warning
        || matches!(readiness_status, Some(ExecutionReadinessStatus::Degraded))
    {
        ResearchCandidateTestnetReviewStatus::NeedsOperatorReview
    } else if !promoted_to_shadow_config
        || latest_qualification.is_none()
        || matches!(
            latest_qualification_status,
            Some(ResearchCandidateQualificationStatus::NeedsMoreData)
                | Some(ResearchCandidateQualificationStatus::Unknown)
                | None
        )
        || matches!(
            readiness_status,
            Some(ExecutionReadinessStatus::Unknown) | None
        )
    {
        ResearchCandidateTestnetReviewStatus::NotReady
    } else {
        ResearchCandidateTestnetReviewStatus::ReadyForReview
    };

    recommendations.insert(ResearchCandidateTestnetReviewRecommendation::ManualOperatorReview);

    let checklist = vec![
        testnet_review_checklist_item(
            "experiment_exists",
            "Experiment exists",
            provenance_available,
            checklist_summary(
                provenance_available,
                "Experiment provenance is present.",
                "Experiment provenance is missing.",
            ),
        ),
        testnet_review_checklist_item(
            "candidate_observed",
            "Candidate observed",
            latest_observation.is_some(),
            checklist_summary(
                latest_observation.is_some(),
                "Candidate has at least one persisted observation.",
                "Candidate has no persisted observation.",
            ),
        ),
        testnet_review_checklist_item(
            "candidate_accepted_for_shadow",
            "Candidate accepted for shadow",
            accepted_for_shadow,
            checklist_summary(
                accepted_for_shadow,
                "Candidate is accepted for shadow.",
                "Candidate is not accepted for shadow.",
            ),
        ),
        testnet_review_checklist_item(
            "candidate_promoted_to_shadow_runner_config",
            "Candidate promoted to shadow runner config",
            promoted_to_shadow_config,
            checklist_summary(
                promoted_to_shadow_config,
                "Candidate is in PROMOTED_TO_SHADOW_CONFIG.",
                "Candidate is not in PROMOTED_TO_SHADOW_CONFIG.",
            ),
        ),
        testnet_review_checklist_item(
            "shadow_runs_linked",
            "Shadow runs linked",
            total_shadow_runs > 0,
            checklist_summary(
                total_shadow_runs > 0,
                "Linked shadow runs exist.",
                "No linked shadow runs exist.",
            ),
        ),
        testnet_review_checklist_item(
            "qualification_evaluated",
            "Qualification evaluated",
            latest_qualification.is_some(),
            checklist_summary(
                latest_qualification.is_some(),
                "Qualification evaluation exists.",
                "Qualification evaluation is missing.",
            ),
        ),
        testnet_review_checklist_item(
            "walk_forward_validation_completed",
            "Walk-forward validation completed",
            matches!(
                walk_forward.map(|value| value.robustness_status),
                Some(
                    StrategyWalkForwardRobustnessStatus::Robust
                        | StrategyWalkForwardRobustnessStatus::Weak
                        | StrategyWalkForwardRobustnessStatus::OverfitRisk
                )
            ),
            checklist_summary(
                walk_forward.is_some(),
                "Walk-forward evidence is linked.",
                "Walk-forward evidence is missing.",
            ),
        ),
        testnet_review_checklist_item(
            "operator_reviewed",
            "Operator reviewed",
            latest_review.is_some(),
            checklist_summary(
                latest_review.is_some(),
                "At least one review action has been recorded.",
                "No review action has been recorded.",
            ),
        ),
        testnet_review_checklist_item(
            "no_execution_table_mutation",
            "No execution table mutation",
            request.no_execution_table_mutation,
            checklist_summary(
                request.no_execution_table_mutation,
                "Dossier evaluation is read-only.",
                "Execution table mutation is not allowed.",
            ),
        ),
        testnet_review_checklist_item(
            "latest_readiness_not_not_ready",
            "Latest readiness not NOT_READY",
            readiness_status != Some(ExecutionReadinessStatus::NotReady),
            checklist_summary(
                readiness_status != Some(ExecutionReadinessStatus::NotReady),
                "Latest readiness is not NOT_READY.",
                "Latest readiness is NOT_READY.",
            ),
        ),
    ];

    ResearchCandidateTestnetReviewDossier {
        candidate_id,
        strategy_id: strategy_id.clone(),
        symbol: symbol.clone(),
        timeframe: timeframe.clone(),
        candidate_status: current_status,
        status,
        evidence: ResearchCandidateTestnetReviewEvidence {
            candidate_id,
            strategy_id,
            symbol,
            timeframe,
            candidate_status: current_status,
            latest_review_action: request.latest_review_action.clone(),
            latest_qualification_evaluation: request.latest_qualification_evaluation.clone(),
            qualification_trend: request.qualification_trend,
            shadow_performance_summary: request.shadow_performance_summary.clone(),
            latest_observation: request.latest_observation.clone(),
            observation_summary: request.observation_summary.clone(),
            observation_freshness: request.observation_freshness,
            observation_age_seconds: request.observation_age_seconds,
            observation_expires_at: latest_observation
                .and_then(|value| value.observation_expires_at),
            runner_alignment: request.runner_alignment.clone(),
            readiness_snapshot: request.readiness_snapshot.clone(),
            source_label: provenance_label(candidate),
            provenance_available,
            provenance_notes: if provenance_available {
                Vec::new()
            } else {
                vec!["candidate has no linked experiment provenance".to_string()]
            },
            candidate_score: candidate.and_then(|value| value.score),
            candidate_pnl_pct: candidate.and_then(|value| value.pnl_pct),
            candidate_max_drawdown_pct: candidate.and_then(|value| value.max_drawdown_pct),
            candidate_trade_count: candidate.and_then(|value| value.trade_count),
            candidate_win_rate: candidate.and_then(|value| value.win_rate),
            candidate_fee_drag: candidate.and_then(|value| value.fee_drag),
            experiment_id: candidate.and_then(|value| value.experiment_id),
            experiment_run_id: candidate.and_then(|value| value.experiment_run_id),
            walk_forward_evidence: request.walk_forward_evidence.clone(),
            shadow_pnl_attribution: request.shadow_pnl_attribution.clone(),
            exit_attribution: request.exit_attribution.clone(),
            operator_report_findings: request.operator_report_findings.clone(),
        },
        checklist,
        findings,
        blockers,
        warnings,
        recommendations: recommendations.into_iter().collect(),
        generated_at: request.generated_at,
        correlation_id: request.correlation_id,
    }
}

pub fn research_candidate_qualification_evaluation_from_result(
    id: Uuid,
    qualification: &ResearchCandidateQualificationResult,
    correlation_id: Option<Uuid>,
) -> ResearchCandidateQualificationEvaluation {
    let total_shadow_runs = qualification
        .shadow_performance
        .as_ref()
        .map(|value| value.total_shadow_runs)
        .unwrap_or(0);
    let would_submit_count = qualification
        .shadow_performance
        .as_ref()
        .map(|value| value.would_submit_count)
        .unwrap_or(0);
    let risk_rejection_rate_pct = qualification
        .shadow_performance
        .as_ref()
        .map(|value| value.risk_rejection_rate_pct);

    ResearchCandidateQualificationEvaluation {
        id,
        candidate_id: qualification.candidate_id,
        status: qualification.status,
        score: qualification.score,
        latest_readiness_status: qualification.latest_readiness_status,
        total_shadow_runs,
        would_submit_count,
        risk_rejection_rate_pct,
        walk_forward_status: qualification.walk_forward_status,
        walk_forward_run_id: qualification.walk_forward_run_id,
        walk_forward_score: qualification.walk_forward_score,
        walk_forward_consistency_score: qualification.walk_forward_consistency_score,
        walk_forward_recommendation: qualification.walk_forward_recommendation.clone(),
        walk_forward_blockers: qualification.walk_forward_blockers.clone(),
        walk_forward_warnings: qualification.walk_forward_warnings.clone(),
        warnings: qualification.warnings.clone(),
        blockers: qualification.blockers.clone(),
        recommendations: qualification.recommendations.clone(),
        thresholds: qualification.thresholds.clone(),
        evaluated_at: qualification.computed_at,
        correlation_id,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResearchCandidateDecisionRejection {
    pub reason_code: String,
    pub recommendation: String,
    pub last_observed_at: Option<DateTime<Utc>>,
    pub observation_expires_at: Option<DateTime<Utc>>,
    pub observation_age_seconds: Option<i64>,
    pub observation_max_age_seconds: Option<i64>,
}

impl StrategyCandidateObservationRequest {
    pub fn to_requirement(
        &self,
        strategy_id: impl Into<String>,
        symbol: impl Into<String>,
        timeframe: impl Into<String>,
    ) -> StrategyCandidateObservationRequirement {
        StrategyCandidateObservationRequirement {
            candidate_id: self.candidate_id,
            strategy_id: strategy_id.into(),
            symbol: symbol.into(),
            timeframe: timeframe.into(),
            min_observation_hours: self.min_observation_hours,
            min_shadow_runs: self.min_shadow_runs,
            max_risk_rejection_rate: self.max_risk_rejection_rate,
            min_would_submit_count: self.min_would_submit_count,
            max_no_signal_rate: self.max_no_signal_rate,
            require_readiness_ready: self.require_readiness_ready,
        }
    }
}

pub fn calculate_observation_rate(count: i64, total: i64) -> Decimal {
    if count <= 0 || total <= 0 {
        Decimal::ZERO
    } else {
        Decimal::from(count) / Decimal::from(total)
    }
}

pub fn calculate_percentage_rate(count: i64, total: i64) -> Decimal {
    (calculate_observation_rate(count, total) * Decimal::from(100)).round_dp(2)
}

fn qualification_check(
    code: &str,
    name: &str,
    passed: bool,
    blocking: bool,
    severity: ResearchCandidateQualificationSeverity,
    summary: impl Into<String>,
    details: Option<Value>,
) -> ResearchCandidateQualificationCheck {
    ResearchCandidateQualificationCheck {
        code: code.to_string(),
        name: name.to_string(),
        passed,
        blocking,
        severity,
        summary: summary.into(),
        details,
    }
}

fn clamp_score(score: i32) -> i32 {
    score.clamp(0, 100)
}

pub fn evaluate_research_candidate_qualification(
    request: &ResearchCandidateQualificationRequest,
) -> ResearchCandidateQualificationResult {
    let thresholds = request.thresholds.clone();
    let default_thresholds = ResearchCandidateQualificationThresholds::default();
    let mut checks = Vec::new();
    let mut recommendations = BTreeSet::new();
    let mut score_explanation = Vec::new();
    let mut score = 100;
    let mut readiness_penalty_points = 0;
    let mut threshold_override_penalty_points = 0;
    let mut degraded_status_trigger = false;
    let mut needs_more_data_trigger = false;
    let mut walk_forward_blockers = Vec::new();
    let mut walk_forward_warnings = Vec::new();

    let threshold_override_below_default = thresholds.min_shadow_runs
        < default_thresholds.min_shadow_runs
        || thresholds.min_would_submit_count < default_thresholds.min_would_submit_count;
    if threshold_override_below_default {
        let warning =
            "Qualification threshold override is below default; treat result as exploratory.";
        checks.push(qualification_check(
            "threshold_override_below_default",
            "Qualification thresholds are not below defaults",
            false,
            false,
            ResearchCandidateQualificationSeverity::Low,
            warning,
            Some(serde_json::json!({
                "min_shadow_runs": thresholds.min_shadow_runs,
                "default_min_shadow_runs": default_thresholds.min_shadow_runs,
                "min_would_submit_count": thresholds.min_would_submit_count,
                "default_min_would_submit_count": default_thresholds.min_would_submit_count,
            })),
        ));
        threshold_override_penalty_points = 5;
        score -= threshold_override_penalty_points;
        score_explanation.push(format!(
            "{warning} (-{} points)",
            threshold_override_penalty_points
        ));
    }

    let candidate_exists = request.candidate_status.is_some();
    checks.push(qualification_check(
        "candidate_exists",
        "Candidate exists",
        candidate_exists,
        true,
        ResearchCandidateQualificationSeverity::Critical,
        if candidate_exists {
            "Candidate exists."
        } else {
            "Candidate was not found."
        },
        None,
    ));
    if !candidate_exists {
        recommendations
            .insert(ResearchCandidateQualificationRecommendation::ReAcceptCandidateForShadow);
    }

    let status_is_accepted = matches!(
        request.candidate_status,
        Some(
            ResearchCandidateStatus::AcceptedForShadow
                | ResearchCandidateStatus::PromotedToShadowConfig
        )
    );
    checks.push(qualification_check(
        "candidate_status_accepted_for_shadow",
        "Candidate status is ACCEPTED_FOR_SHADOW",
        status_is_accepted,
        true,
        ResearchCandidateQualificationSeverity::High,
        if status_is_accepted {
            "Candidate is accepted for shadow."
        } else {
            "Candidate is not accepted for shadow."
        },
        request
            .candidate_status
            .map(|status| serde_json::json!({ "candidate_status": status.as_str() })),
    ));
    if !status_is_accepted {
        score -= 35;
        score_explanation.push(
            "Candidate is not in ACCEPTED_FOR_SHADOW, so qualification lost 35 points.".to_string(),
        );
        recommendations
            .insert(ResearchCandidateQualificationRecommendation::ReAcceptCandidateForShadow);
    }

    let status_is_promoted =
        request.candidate_status == Some(ResearchCandidateStatus::PromotedToShadowConfig);
    let shadow_performance = request.shadow_performance.clone();
    let total_shadow_runs = shadow_performance
        .as_ref()
        .map(|value| value.total_shadow_runs)
        .unwrap_or(0);
    let independent_shadow_observation_count = shadow_performance
        .as_ref()
        .map(|value| value.independent_shadow_observation_count)
        .unwrap_or(0);
    let duplicate_same_candle_runs_count = shadow_performance
        .as_ref()
        .map(|value| value.duplicate_same_candle_runs_count)
        .unwrap_or(0);
    let linked_shadow_completed_count = shadow_performance
        .as_ref()
        .map(|value| value.completed_shadow_runs)
        .unwrap_or(0);
    let would_submit_count = shadow_performance
        .as_ref()
        .map(|value| value.would_submit_count)
        .unwrap_or(0);
    let no_signal_count = shadow_performance
        .as_ref()
        .map(|value| value.no_signal_count)
        .unwrap_or(0);
    let risk_rejected_count = shadow_performance
        .as_ref()
        .map(|value| value.risk_rejected_count)
        .unwrap_or(0);
    let linked_shadow_skipped_count = shadow_performance
        .as_ref()
        .map(|value| value.skipped_count)
        .unwrap_or(0);
    let latest_valid_shadow_run_at = shadow_performance
        .as_ref()
        .and_then(|value| value.latest_valid_shadow_run_at);
    let latest_skipped_shadow_run_at = shadow_performance
        .as_ref()
        .and_then(|value| value.latest_skipped_shadow_run_at);
    let valid_shadow_evidence_available = latest_valid_shadow_run_at.is_some()
        || linked_shadow_completed_count > 0
        || would_submit_count > 0
        || no_signal_count > 0
        || risk_rejected_count > 0;
    checks.push(qualification_check(
        "candidate_status_promoted_to_shadow_config",
        "Candidate status is PROMOTED_TO_SHADOW_CONFIG",
        status_is_promoted,
        false,
        ResearchCandidateQualificationSeverity::High,
        if status_is_promoted {
            "Candidate is promoted to shadow runner config."
        } else if request.candidate_status == Some(ResearchCandidateStatus::AcceptedForShadow) {
            "Candidate is accepted but not promoted to shadow runner config."
        } else {
            "Candidate is not promoted to shadow runner config."
        },
        request
            .candidate_status
            .map(|status| serde_json::json!({ "candidate_status": status.as_str() })),
    ));
    if !status_is_promoted {
        score -= 20;
        score_explanation.push(
            "Candidate is accepted but not promoted to shadow runner config. (-20 points)"
                .to_string(),
        );
        recommendations
            .insert(ResearchCandidateQualificationRecommendation::ExpandShadowRunnerCoverage);
    }

    let fresh_observation_requirement_satisfied = !thresholds.require_fresh_observation
        || request.fresh_observation
        || valid_shadow_evidence_available;
    let fresh_observation_check_passed =
        !thresholds.require_fresh_observation || request.fresh_observation;
    let fresh_observation_blocking =
        thresholds.require_fresh_observation && !valid_shadow_evidence_available;
    let fresh_observation_summary = if fresh_observation_check_passed {
        if request.fresh_observation {
            "Fresh formal observation is available.".to_string()
        } else {
            "Fresh formal observation is not required by the active thresholds.".to_string()
        }
    } else if fresh_observation_requirement_satisfied {
        "Formal observation is stale or missing, but valid linked shadow evidence exists."
            .to_string()
    } else {
        "Fresh formal observation is required and no valid linked shadow evidence exists."
            .to_string()
    };
    checks.push(qualification_check(
        "fresh_observation_required",
        "Candidate has fresh formal observation or valid linked shadow evidence",
        fresh_observation_check_passed,
        fresh_observation_blocking,
        ResearchCandidateQualificationSeverity::High,
        fresh_observation_summary,
        Some(serde_json::json!({
            "formal_observation_status": request
                .formal_observation_status
                .map(|status| status.as_str()),
            "formal_observation_stale": request.formal_observation_stale,
            "valid_linked_shadow_evidence_available": valid_shadow_evidence_available,
            "latest_valid_shadow_run_at": latest_valid_shadow_run_at,
        })),
    ));
    if thresholds.require_fresh_observation && !request.fresh_observation {
        score -= 25;
        if valid_shadow_evidence_available {
            score_explanation.push(
                "Formal observation is stale or missing, but valid linked shadow evidence exists; qualification lost 25 points as a warning."
                    .to_string(),
            );
        } else {
            score_explanation.push(
                "Fresh formal observation is missing or stale and no valid linked shadow evidence exists, so qualification lost 25 points."
                    .to_string(),
            );
        }
        recommendations
            .insert(ResearchCandidateQualificationRecommendation::RefreshCandidateObservation);
    }

    let runner_alignment_passed =
        !thresholds.require_runner_alignment || request.runner_alignment_valid;
    checks.push(qualification_check(
        "runner_alignment_valid",
        "Runner alignment is valid",
        runner_alignment_passed,
        thresholds.require_runner_alignment,
        ResearchCandidateQualificationSeverity::High,
        if runner_alignment_passed {
            "Runner alignment is valid."
        } else {
            "Runner alignment is invalid for this candidate."
        },
        Some(serde_json::json!({
            "runner_alignment_valid": request.runner_alignment_valid,
        })),
    ));
    if !runner_alignment_passed {
        score -= 25;
        score_explanation.push(
            "Runner alignment is invalid for this candidate, so qualification lost 25 points."
                .to_string(),
        );
        recommendations.insert(ResearchCandidateQualificationRecommendation::FixRunnerAlignment);
    }

    let runner_coverage_passed = request.shadow_runner_covers_candidate;
    checks.push(qualification_check(
        "shadow_runner_covers_candidate",
        "Shadow runner covers candidate strategy, symbol, and timeframe",
        runner_coverage_passed,
        true,
        ResearchCandidateQualificationSeverity::High,
        if runner_coverage_passed {
            "Current shadow runner covers the candidate."
        } else {
            "Current shadow runner does not cover the candidate."
        },
        None,
    ));
    if !runner_coverage_passed {
        score -= 20;
        score_explanation.push(
            "The active shadow runner does not cover this candidate, so qualification lost 20 points."
                .to_string(),
        );
        recommendations
            .insert(ResearchCandidateQualificationRecommendation::ExpandShadowRunnerCoverage);
    }

    let mismatch_count_passed =
        request.runner_mismatch_count <= thresholds.max_runner_mismatch_count;
    checks.push(qualification_check(
        "runner_mismatch_count",
        "Runner mismatch count is within threshold",
        mismatch_count_passed,
        thresholds.max_runner_mismatch_count == 0,
        ResearchCandidateQualificationSeverity::Medium,
        if mismatch_count_passed {
            "Runner mismatch count is within threshold.".to_string()
        } else {
            format!(
                "Runner mismatch count {} exceeded max {}.",
                request.runner_mismatch_count, thresholds.max_runner_mismatch_count
            )
        },
        Some(serde_json::json!({
            "runner_mismatch_count": request.runner_mismatch_count,
            "max_runner_mismatch_count": thresholds.max_runner_mismatch_count,
        })),
    ));
    if !mismatch_count_passed {
        score -= 10;
        score_explanation.push(format!(
            "Runner mismatch count exceeded the configured limit, so qualification lost 10 points."
        ));
        degraded_status_trigger = true;
        recommendations.insert(ResearchCandidateQualificationRecommendation::FixRunnerAlignment);
    }

    let walk_forward_evidence = request.walk_forward_evidence.clone();
    match walk_forward_evidence
        .as_ref()
        .map(|value| value.robustness_status)
    {
        Some(StrategyWalkForwardRobustnessStatus::Robust) => {
            checks.push(qualification_check(
                "walk_forward_robustness",
                "Walk-forward robustness is acceptable",
                true,
                true,
                ResearchCandidateQualificationSeverity::Low,
                "Walk-forward robustness is ROBUST.",
                walk_forward_evidence.as_ref().map(|value| {
                    serde_json::json!({
                        "walk_forward_run_id": value.walk_forward_run_id,
                        "robustness_score": value.robustness_score,
                        "consistency_score": value.consistency_score,
                    })
                }),
            ));
        }
        Some(StrategyWalkForwardRobustnessStatus::OverfitRisk) => {
            let message = "Walk-forward robustness is OVERFIT_RISK.";
            checks.push(qualification_check(
                "walk_forward_robustness",
                "Walk-forward robustness is acceptable",
                false,
                true,
                ResearchCandidateQualificationSeverity::High,
                message,
                walk_forward_evidence.as_ref().map(|value| {
                    serde_json::json!({
                        "walk_forward_run_id": value.walk_forward_run_id,
                        "robustness_score": value.robustness_score,
                        "consistency_score": value.consistency_score,
                        "recommendation": value.recommendation_reason,
                    })
                }),
            ));
            score -= 40;
            score = score.min(40);
            walk_forward_blockers.push(message.to_string());
            score_explanation.push(
                "Walk-forward OVERFIT_RISK blocks testnet promotion consideration. (-40 points, score capped at 40)"
                    .to_string(),
            );
        }
        Some(StrategyWalkForwardRobustnessStatus::Failed) => {
            let message = "Walk-forward validation failed.";
            checks.push(qualification_check(
                "walk_forward_robustness",
                "Walk-forward robustness is acceptable",
                false,
                true,
                ResearchCandidateQualificationSeverity::High,
                message,
                walk_forward_evidence.as_ref().map(|value| {
                    serde_json::json!({
                        "walk_forward_run_id": value.walk_forward_run_id,
                        "robustness_score": value.robustness_score,
                    })
                }),
            ));
            score -= 40;
            score = score.min(40);
            walk_forward_blockers.push(message.to_string());
            score_explanation.push("Failed walk-forward validation blocks qualification. (-40 points, score capped at 40)".to_string());
        }
        Some(StrategyWalkForwardRobustnessStatus::InsufficientData) => {
            let message = "Walk-forward validation has insufficient data.";
            checks.push(qualification_check(
                "walk_forward_robustness",
                "Walk-forward robustness is acceptable",
                false,
                false,
                ResearchCandidateQualificationSeverity::Medium,
                message,
                walk_forward_evidence.as_ref().map(|value| {
                    serde_json::json!({
                        "walk_forward_run_id": value.walk_forward_run_id,
                        "robustness_score": value.robustness_score,
                    })
                }),
            ));
            score -= 20;
            needs_more_data_trigger = true;
            walk_forward_warnings.push(message.to_string());
            score_explanation.push("Walk-forward evidence is insufficient; collect more data before testnet review. (-20 points)".to_string());
        }
        Some(StrategyWalkForwardRobustnessStatus::Weak) => {
            let message = "Walk-forward robustness is WEAK.";
            checks.push(qualification_check(
                "walk_forward_robustness",
                "Walk-forward robustness is acceptable",
                false,
                false,
                ResearchCandidateQualificationSeverity::Medium,
                message,
                walk_forward_evidence.as_ref().map(|value| {
                    serde_json::json!({
                        "walk_forward_run_id": value.walk_forward_run_id,
                        "robustness_score": value.robustness_score,
                        "consistency_score": value.consistency_score,
                    })
                }),
            ));
            score -= 15;
            degraded_status_trigger = true;
            walk_forward_warnings.push(message.to_string());
            score_explanation.push(
                "Weak walk-forward robustness degrades qualification. (-15 points)".to_string(),
            );
        }
        None => {
            let message = "Candidate missing walk-forward validation.";
            checks.push(qualification_check(
                "walk_forward_robustness",
                "Walk-forward robustness is acceptable",
                false,
                false,
                ResearchCandidateQualificationSeverity::Medium,
                message,
                None,
            ));
            score -= 20;
            needs_more_data_trigger = true;
            walk_forward_warnings.push("NO_WALK_FORWARD_EVIDENCE".to_string());
            score_explanation.push(
                "Missing walk-forward evidence requires more data before testnet review. (-20 points)"
                    .to_string(),
            );
        }
    }

    let risk_rejection_rate_pct = shadow_performance
        .as_ref()
        .map(|value| value.risk_rejection_rate_pct)
        .unwrap_or(Decimal::ZERO);
    let skipped_or_error_count = shadow_performance
        .as_ref()
        .map(|value| value.skipped_count + value.error_count)
        .unwrap_or(0);
    let skipped_or_error_rate_pct =
        calculate_percentage_rate(skipped_or_error_count, total_shadow_runs);

    let enough_shadow_runs = independent_shadow_observation_count >= thresholds.min_shadow_runs;
    checks.push(qualification_check(
        "enough_linked_shadow_runs",
        "Enough independent linked shadow observations exist",
        enough_shadow_runs,
        false,
        ResearchCandidateQualificationSeverity::Medium,
        if enough_shadow_runs {
            "Independent linked shadow observation count meets threshold.".to_string()
        } else {
            format!(
                "Independent linked shadow observations {} are below min {}.",
                independent_shadow_observation_count, thresholds.min_shadow_runs
            )
        },
        Some(serde_json::json!({
            "total_shadow_runs": total_shadow_runs,
            "completed_shadow_runs": linked_shadow_completed_count,
            "independent_shadow_observation_count": independent_shadow_observation_count,
            "unique_evaluated_candle_count": shadow_performance
                .as_ref()
                .map(|value| value.unique_evaluated_candle_count)
                .unwrap_or(0),
            "duplicate_same_candle_runs_count": duplicate_same_candle_runs_count,
            "latest_evaluated_candle_open_time": shadow_performance
                .as_ref()
                .and_then(|value| value.latest_evaluated_candle_open_time),
            "min_shadow_runs": thresholds.min_shadow_runs,
        })),
    ));
    if !enough_shadow_runs {
        score -= 20;
        score_explanation.push(format!(
            "Linked shadow runs are below the configured threshold, so qualification lost 20 points."
        ));
        recommendations.insert(ResearchCandidateQualificationRecommendation::GatherMoreShadowRuns);
    }
    if duplicate_same_candle_runs_count > 0 {
        let warning = format!(
            "{} duplicate same-candle shadow runs were treated as operational checks, not independent strategy evidence.",
            duplicate_same_candle_runs_count
        );
        checks.push(qualification_check(
            "duplicate_same_candle_shadow_runs",
            "Duplicate same-candle shadow runs are not counted as independent evidence",
            false,
            false,
            ResearchCandidateQualificationSeverity::Low,
            &warning,
            Some(serde_json::json!({
                "duplicate_same_candle_runs_count": duplicate_same_candle_runs_count,
                "independent_shadow_observation_count": independent_shadow_observation_count,
            })),
        ));
        score -= 5;
        score_explanation.push(format!("{warning} (-5 points)"));
    }

    let enough_would_submit = would_submit_count >= thresholds.min_would_submit_count;
    checks.push(qualification_check(
        "would_submit_count_enough",
        "WOULD_SUBMIT count is nonzero and meets threshold",
        enough_would_submit,
        enough_shadow_runs,
        ResearchCandidateQualificationSeverity::Medium,
        if enough_would_submit {
            "WOULD_SUBMIT evidence meets threshold.".to_string()
        } else {
            format!(
                "WOULD_SUBMIT count {} is below min {}.",
                would_submit_count, thresholds.min_would_submit_count
            )
        },
        Some(serde_json::json!({
            "would_submit_count": would_submit_count,
            "min_would_submit_count": thresholds.min_would_submit_count,
        })),
    ));
    if !enough_would_submit {
        score -= 20;
        score_explanation.push(
            "WOULD_SUBMIT evidence is below the configured threshold, so qualification lost 20 points."
                .to_string(),
        );
        degraded_status_trigger = true;
        recommendations
            .insert(ResearchCandidateQualificationRecommendation::GenerateMoreWouldSubmitEvidence);
    }

    let risk_rejection_passed = risk_rejection_rate_pct <= thresholds.max_risk_rejection_rate_pct;
    checks.push(qualification_check(
        "risk_rejection_rate_acceptable",
        "Risk rejection rate is within threshold",
        risk_rejection_passed,
        false,
        ResearchCandidateQualificationSeverity::High,
        if risk_rejection_passed {
            "Risk rejection rate is within threshold.".to_string()
        } else {
            format!(
                "Risk rejection rate {}% exceeded max {}%.",
                risk_rejection_rate_pct, thresholds.max_risk_rejection_rate_pct
            )
        },
        Some(serde_json::json!({
            "risk_rejection_rate_pct": risk_rejection_rate_pct,
            "max_risk_rejection_rate_pct": thresholds.max_risk_rejection_rate_pct,
        })),
    ));
    if !risk_rejection_passed {
        score -= 15;
        score_explanation.push(
            "Risk rejection rate exceeded the configured threshold, so qualification lost 15 points."
                .to_string(),
        );
        degraded_status_trigger = true;
        recommendations.insert(ResearchCandidateQualificationRecommendation::ReviewRiskRejections);
    }

    let skipped_error_passed =
        skipped_or_error_rate_pct <= thresholds.max_error_or_skipped_rate_pct;
    checks.push(qualification_check(
        "error_or_skipped_rate_acceptable",
        "Skipped or error rate is within threshold",
        skipped_error_passed,
        false,
        ResearchCandidateQualificationSeverity::Medium,
        if skipped_error_passed {
            "Skipped or error rate is within threshold.".to_string()
        } else {
            format!(
                "Skipped or error rate {}% exceeded max {}%.",
                skipped_or_error_rate_pct, thresholds.max_error_or_skipped_rate_pct
            )
        },
        Some(serde_json::json!({
            "skipped_or_error_rate_pct": skipped_or_error_rate_pct,
            "max_error_or_skipped_rate_pct": thresholds.max_error_or_skipped_rate_pct,
        })),
    ));
    if !skipped_error_passed {
        score -= 15;
        score_explanation.push(
            "Skipped or error rate exceeded the configured threshold, so qualification lost 15 points."
                .to_string(),
        );
        degraded_status_trigger = true;
        recommendations
            .insert(ResearchCandidateQualificationRecommendation::ReduceShadowErrorsOrSkips);
    }
    let readiness_status = request.latest_readiness_status;
    match readiness_status {
        Some(ExecutionReadinessStatus::Ready) => {
            checks.push(qualification_check(
                "readiness_status",
                "TESTNET_SHADOW readiness is READY",
                true,
                false,
                ResearchCandidateQualificationSeverity::Low,
                "Latest TESTNET_SHADOW readiness is READY.",
                Some(serde_json::json!({
                    "latest_readiness_status": ExecutionReadinessStatus::Ready.as_str(),
                })),
            ));
        }
        Some(ExecutionReadinessStatus::Degraded) => {
            let warning = "Latest TESTNET_SHADOW readiness is DEGRADED.";
            checks.push(qualification_check(
                "readiness_status",
                "TESTNET_SHADOW readiness is READY",
                false,
                false,
                ResearchCandidateQualificationSeverity::Medium,
                warning,
                Some(serde_json::json!({
                    "latest_readiness_status": ExecutionReadinessStatus::Degraded.as_str(),
                })),
            ));
            readiness_penalty_points = 12;
            score -= readiness_penalty_points;
            degraded_status_trigger = true;
            score_explanation.push(format!("{warning} (-{} points)", readiness_penalty_points));
            score_explanation.push(
                "Resolve degraded readiness conditions before considering testnet promotion."
                    .to_string(),
            );
            recommendations.insert(
                ResearchCandidateQualificationRecommendation::RestoreTestnetShadowReadiness,
            );
        }
        Some(ExecutionReadinessStatus::NotReady) => {
            let warning = "Latest TESTNET_SHADOW readiness is NOT_READY.";
            checks.push(qualification_check(
                "readiness_status",
                "TESTNET_SHADOW readiness is READY",
                false,
                true,
                ResearchCandidateQualificationSeverity::High,
                warning,
                Some(serde_json::json!({
                    "latest_readiness_status": ExecutionReadinessStatus::NotReady.as_str(),
                })),
            ));
            readiness_penalty_points = 30;
            score -= readiness_penalty_points;
            score = score.min(40);
            score_explanation.push(format!(
                "{warning} (-{} points, score capped at 40)",
                readiness_penalty_points
            ));
            score_explanation.push(
                "Do not consider testnet promotion until readiness blockers are cleared."
                    .to_string(),
            );
            recommendations.insert(
                ResearchCandidateQualificationRecommendation::RestoreTestnetShadowReadiness,
            );
        }
        Some(ExecutionReadinessStatus::Unknown) | None => {
            let warning = "Latest TESTNET_SHADOW readiness is UNKNOWN.";
            checks.push(qualification_check(
                "readiness_status",
                "TESTNET_SHADOW readiness is READY",
                false,
                false,
                ResearchCandidateQualificationSeverity::Medium,
                warning,
                Some(serde_json::json!({
                    "latest_readiness_status": readiness_status.map(|status| status.as_str()).unwrap_or("UNKNOWN"),
                })),
            ));
            readiness_penalty_points = 10;
            score -= readiness_penalty_points;
            degraded_status_trigger = true;
            score_explanation.push(format!("{warning} (-{} points)", readiness_penalty_points));
            score_explanation.push(
                "Collect a fresh readiness result before considering testnet promotion."
                    .to_string(),
            );
            recommendations.insert(
                ResearchCandidateQualificationRecommendation::RestoreTestnetShadowReadiness,
            );
        }
    }

    let blockers = checks
        .iter()
        .filter(|check| !check.passed && check.blocking)
        .map(|check| check.summary.clone())
        .collect::<Vec<_>>();
    let warnings = checks
        .iter()
        .filter(|check| !check.passed && !check.blocking)
        .map(|check| check.summary.clone())
        .collect::<Vec<_>>();

    let status = if !candidate_exists {
        ResearchCandidateQualificationStatus::Unknown
    } else if !blockers.is_empty() {
        ResearchCandidateQualificationStatus::NotQualified
    } else if needs_more_data_trigger {
        ResearchCandidateQualificationStatus::NeedsMoreData
    } else if !status_is_promoted {
        ResearchCandidateQualificationStatus::NeedsMoreData
    } else if !enough_shadow_runs {
        ResearchCandidateQualificationStatus::NeedsMoreData
    } else if degraded_status_trigger {
        ResearchCandidateQualificationStatus::Degraded
    } else {
        ResearchCandidateQualificationStatus::Qualified
    };

    if score_explanation.is_empty() {
        score_explanation.push(
            "All qualification checks passed with READY readiness and default thresholds; score remained 100."
                .to_string(),
        );
    }

    if status == ResearchCandidateQualificationStatus::Qualified
        && !threshold_override_below_default
        && readiness_status == Some(ExecutionReadinessStatus::Ready)
    {
        recommendations.insert(
            ResearchCandidateQualificationRecommendation::ReadyForTestnetPromotionConsideration,
        );
    }

    let evidence_interpretation = if valid_shadow_evidence_available && would_submit_count <= 0 {
        format!(
            "Valid linked shadow observation exists (NO_SIGNAL={}, RISK_REJECTED={}, WOULD_SUBMIT=0); pipeline is observing the candidate, but actionable WOULD_SUBMIT evidence is insufficient for testnet readiness.",
            no_signal_count, risk_rejected_count
        )
    } else if valid_shadow_evidence_available {
        format!(
            "Valid linked shadow observation exists (WOULD_SUBMIT={}, NO_SIGNAL={}, RISK_REJECTED={}); stale or skipped runs are tracked separately.",
            would_submit_count, no_signal_count, risk_rejected_count
        )
    } else if linked_shadow_skipped_count > 0 {
        format!(
            "Only skipped shadow runs are linked (skipped={}); this is infrastructure/readiness evidence, not valid signal observation evidence.",
            linked_shadow_skipped_count
        )
    } else {
        "No valid linked shadow observation evidence is available.".to_string()
    };

    ResearchCandidateQualificationResult {
        candidate_id: request.candidate_id,
        status,
        score: clamp_score(score),
        fresh_observation: request.fresh_observation,
        runner_alignment_valid: request.runner_alignment_valid,
        latest_readiness_status: readiness_status,
        linked_shadow_completed_count,
        linked_shadow_no_signal_count: no_signal_count,
        linked_shadow_would_submit_count: would_submit_count,
        linked_shadow_risk_rejected_count: risk_rejected_count,
        linked_shadow_skipped_count,
        latest_valid_shadow_run_at,
        latest_skipped_shadow_run_at,
        formal_observation_status: request.formal_observation_status,
        formal_observation_stale: request.formal_observation_stale,
        evidence_interpretation,
        walk_forward_status: walk_forward_evidence
            .as_ref()
            .map(|value| value.robustness_status),
        walk_forward_run_id: walk_forward_evidence
            .as_ref()
            .map(|value| value.walk_forward_run_id),
        walk_forward_score: walk_forward_evidence
            .as_ref()
            .map(|value| value.robustness_score),
        walk_forward_consistency_score: walk_forward_evidence
            .as_ref()
            .map(|value| value.consistency_score),
        walk_forward_recommendation: walk_forward_evidence.as_ref().and_then(|value| {
            value
                .recommendation_reason
                .clone()
                .or_else(|| value.recommendation_action.clone())
        }),
        walk_forward_blockers,
        walk_forward_warnings,
        readiness_penalty_points,
        threshold_override_below_default,
        threshold_override_penalty_points,
        score_explanation,
        checks,
        blockers,
        warnings,
        recommendations: recommendations.into_iter().collect(),
        thresholds,
        shadow_performance,
        latest_shadow_pnl_status: request
            .shadow_pnl_attribution
            .as_ref()
            .map(|value| value.latest_shadow_pnl_status),
        best_holding_window: request
            .shadow_pnl_attribution
            .as_ref()
            .and_then(|value| value.best_holding_window),
        best_avg_net_pnl_pct: request
            .shadow_pnl_attribution
            .as_ref()
            .and_then(|value| value.best_avg_net_pnl_pct),
        negative_all_windows: request
            .shadow_pnl_attribution
            .as_ref()
            .map(|value| value.summary.negative_all_windows)
            .unwrap_or(false),
        computed_at: request.computed_at,
    }
}

pub fn evaluate_research_candidate_shadow_performance(
    candidate_id: Uuid,
    candidate_status: ResearchCandidateStatus,
    strategy_id: impl Into<String>,
    symbol: impl Into<String>,
    timeframe: impl Into<String>,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    total_shadow_runs: i64,
    would_submit_count: i64,
    no_signal_count: i64,
    risk_rejected_count: i64,
    skipped_count: i64,
    error_count: i64,
    last_shadow_run_at: Option<DateTime<Utc>>,
    completed_shadow_runs: i64,
    independent_shadow_observation_count: i64,
    unique_evaluated_candle_count: i64,
    duplicate_same_candle_runs_count: i64,
    latest_evaluated_candle_open_time: Option<DateTime<Utc>>,
    latest_valid_shadow_run_at: Option<DateTime<Utc>>,
    latest_skipped_shadow_run_at: Option<DateTime<Utc>>,
    runner_alignment_current: bool,
    computed_at: DateTime<Utc>,
) -> ResearchCandidateShadowPerformance {
    let would_submit_rate_pct = calculate_percentage_rate(would_submit_count, total_shadow_runs);
    let risk_rejection_rate_pct = calculate_percentage_rate(risk_rejected_count, total_shadow_runs);
    let skipped_or_error_count = skipped_count + error_count;
    let skipped_or_error_rate_pct =
        calculate_percentage_rate(skipped_or_error_count, total_shadow_runs);
    let independent_shadow_observation_count = independent_shadow_observation_count.max(0);
    let unique_evaluated_candle_count = unique_evaluated_candle_count.max(0);
    let duplicate_same_candle_runs_count = duplicate_same_candle_runs_count.max(0);

    let (status, recommendation) =
        if candidate_status != ResearchCandidateStatus::PromotedToShadowConfig {
            (
                ResearchCandidateShadowPerformanceStatus::NotPromotedToShadowConfig,
                ResearchCandidateShadowPerformanceRecommendation::PromoteToShadowConfig,
            )
        } else if !runner_alignment_current {
            (
                ResearchCandidateShadowPerformanceStatus::NeedsReview,
                ResearchCandidateShadowPerformanceRecommendation::CandidateNotCoveredByRunner,
            )
        } else if total_shadow_runs <= 0 {
            (
                ResearchCandidateShadowPerformanceStatus::InsufficientData,
                ResearchCandidateShadowPerformanceRecommendation::InsufficientData,
            )
        } else if total_shadow_runs < default_min_shadow_runs() {
            (
                ResearchCandidateShadowPerformanceStatus::UnderObservation,
                ResearchCandidateShadowPerformanceRecommendation::KeepObserving,
            )
        } else if would_submit_count == 0
            || risk_rejection_rate_pct >= Decimal::new(75, 0)
            || skipped_or_error_rate_pct >= Decimal::new(50, 0)
        {
            (
                ResearchCandidateShadowPerformanceStatus::NeedsReview,
                if would_submit_count == 0 {
                    ResearchCandidateShadowPerformanceRecommendation::RejectCandidate
                } else {
                    ResearchCandidateShadowPerformanceRecommendation::NeedsReview
                },
            )
        } else {
            (
                ResearchCandidateShadowPerformanceStatus::Healthy,
                ResearchCandidateShadowPerformanceRecommendation::KeepObserving,
            )
        };

    let strategy_id = strategy_id.into();
    let symbol = symbol.into();
    let timeframe = timeframe.into();
    let outcome_breakdown = ResearchCandidateShadowOutcomeBreakdown {
        total_shadow_runs,
        completed_shadow_runs,
        independent_shadow_observation_count,
        unique_evaluated_candle_count,
        duplicate_same_candle_runs_count,
        would_submit_count,
        no_signal_count,
        risk_rejected_count,
        skipped_count,
        error_count,
        would_submit_rate_pct,
        risk_rejection_rate_pct,
    };

    ResearchCandidateShadowPerformance {
        candidate_id,
        strategy_id,
        symbol,
        timeframe,
        window_start,
        window_end,
        total_shadow_runs,
        would_submit_count,
        no_signal_count,
        risk_rejected_count,
        skipped_count,
        error_count,
        would_submit_rate_pct,
        risk_rejection_rate_pct,
        last_shadow_run_at,
        completed_shadow_runs,
        independent_shadow_observation_count,
        unique_evaluated_candle_count,
        duplicate_same_candle_runs_count,
        latest_evaluated_candle_open_time,
        latest_valid_shadow_run_at,
        latest_skipped_shadow_run_at,
        runner_alignment_current,
        recommendation,
        status,
        outcome_breakdown,
        computed_at,
    }
}

pub fn calculate_research_shadow_pnl_attribution(
    candidate: &ResearchCandidate,
    request: &ResearchShadowPnlAttributionRequest,
    runs: &[ResearchShadowPnlRunInput],
    candles: &[Candle],
    computed_at: DateTime<Utc>,
) -> ResearchShadowPnlAttributionResult {
    let holding_windows = request.normalized_holding_windows();
    let fee_drag_pct = (request.fee_bps + request.slippage_bps) / Decimal::new(100, 0);
    let extreme_pnl_threshold_pct = if request.extreme_pnl_threshold_pct > Decimal::ZERO {
        request.extreme_pnl_threshold_pct
    } else {
        default_shadow_pnl_extreme_threshold_pct()
    };
    let interval = candidate
        .timeframe
        .parse::<CandleInterval>()
        .unwrap_or(CandleInterval::OneMinute);
    let expected_interval_seconds = interval.duration().num_seconds();
    let gap_tolerance_seconds = expected_interval_seconds + 1;

    let mut sorted_runs = runs.to_vec();
    sorted_runs.sort_by_key(|run| run.shadow_created_at);
    let mut sorted_candles = candles.to_vec();
    sorted_candles.sort_by_key(|candle| candle.open_time);

    let trades = sorted_runs
        .iter()
        .map(|run| {
            let attribution_time = run.signal_time.unwrap_or(run.shadow_created_at);
            let entry_index = sorted_candles
                .iter()
                .position(|candle| candle.is_closed && candle.open_time > attribution_time);

            let Some(entry_index) = entry_index else {
                return ResearchShadowPnlAttributionTrade {
                    candidate_id: candidate.id,
                    shadow_run_id: run.shadow_run_id,
                    strategy_id: run.strategy_id.clone(),
                    symbol: run.symbol.clone(),
                    timeframe: run.timeframe.clone(),
                    shadow_created_at: run.shadow_created_at,
                    signal_time: run.signal_time,
                    status: ResearchShadowPnlStatus::InsufficientForwardData,
                    attribution_status: ResearchShadowPnlStatus::InsufficientForwardData,
                    entry_candle_open_time: None,
                    entry_candle_close_time: None,
                    entry_price: None,
                    holding_windows: holding_windows
                        .iter()
                        .copied()
                        .map(|holding_window| ResearchShadowPnlTradeHoldingWindowResult {
                            holding_window,
                            status: ResearchShadowPnlStatus::InsufficientForwardData,
                            attribution_status: ResearchShadowPnlStatus::InsufficientForwardData,
                            exit_candle_open_time: None,
                            exit_candle_close_time: None,
                            exit_price: None,
                            gross_pnl_pct: None,
                            fee_bps: request.fee_bps,
                            slippage_bps: request.slippage_bps,
                            net_pnl_pct: None,
                            fee_drag_pct,
                            candle_gap_seconds: None,
                            warning: None,
                        })
                        .collect(),
                };
            };

            let entry = &sorted_candles[entry_index];
            let entry_price = entry.open;
            let window_results = holding_windows
                .iter()
                .copied()
                .map(|holding_window| {
                    let exit_index = entry_index + holding_window as usize;
                    let Some(exit) = sorted_candles.get(exit_index) else {
                        return ResearchShadowPnlTradeHoldingWindowResult {
                            holding_window,
                            status: ResearchShadowPnlStatus::InsufficientForwardData,
                            attribution_status: ResearchShadowPnlStatus::InsufficientForwardData,
                            exit_candle_open_time: None,
                            exit_candle_close_time: None,
                            exit_price: None,
                            gross_pnl_pct: None,
                            fee_bps: request.fee_bps,
                            slippage_bps: request.slippage_bps,
                            net_pnl_pct: None,
                            fee_drag_pct,
                            candle_gap_seconds: None,
                            warning: None,
                        };
                    };
                    let max_gap_seconds = sorted_candles[entry_index..=exit_index]
                        .windows(2)
                        .map(|pair| pair[1].open_time.signed_duration_since(pair[0].open_time))
                        .map(|duration| duration.num_seconds())
                        .max()
                        .unwrap_or(expected_interval_seconds);
                    let gross_pnl_pct = if entry_price > Decimal::ZERO {
                        ((exit.close - entry_price) / entry_price) * Decimal::new(100, 0)
                    } else {
                        Decimal::ZERO
                    };
                    let net_pnl_pct = gross_pnl_pct - fee_drag_pct;
                    let has_gap = max_gap_seconds > gap_tolerance_seconds;
                    let is_extreme = net_pnl_pct.abs() > extreme_pnl_threshold_pct;
                    let status = if has_gap {
                        ResearchShadowPnlStatus::GapDetected
                    } else if is_extreme {
                        ResearchShadowPnlStatus::ExtremePnl
                    } else {
                        ResearchShadowPnlStatus::Attributed
                    };
                    let warning = if is_extreme {
                        Some("Attribution PnL is unusually large; inspect candle continuity and timestamps.".to_string())
                    } else {
                        None
                    };
                    ResearchShadowPnlTradeHoldingWindowResult {
                        holding_window,
                        status,
                        attribution_status: status,
                        exit_candle_open_time: Some(exit.open_time),
                        exit_candle_close_time: Some(exit.close_time),
                        exit_price: Some(exit.close),
                        gross_pnl_pct: Some(gross_pnl_pct),
                        fee_bps: request.fee_bps,
                        slippage_bps: request.slippage_bps,
                        net_pnl_pct: Some(net_pnl_pct),
                        fee_drag_pct,
                        candle_gap_seconds: Some(max_gap_seconds),
                        warning,
                    }
                })
                .collect::<Vec<_>>();
            let status = if window_results
                .iter()
                .any(|window| window.status == ResearchShadowPnlStatus::GapDetected)
            {
                ResearchShadowPnlStatus::GapDetected
            } else if window_results
                .iter()
                .any(|window| window.status == ResearchShadowPnlStatus::ExtremePnl)
            {
                ResearchShadowPnlStatus::ExtremePnl
            } else if window_results
                .iter()
                .any(|window| window.net_pnl_pct.is_some())
            {
                ResearchShadowPnlStatus::Attributed
            } else {
                ResearchShadowPnlStatus::InsufficientForwardData
            };

            ResearchShadowPnlAttributionTrade {
                candidate_id: candidate.id,
                shadow_run_id: run.shadow_run_id,
                strategy_id: run.strategy_id.clone(),
                symbol: run.symbol.clone(),
                timeframe: run.timeframe.clone(),
                shadow_created_at: run.shadow_created_at,
                signal_time: run.signal_time,
                status,
                attribution_status: status,
                entry_candle_open_time: Some(entry.open_time),
                entry_candle_close_time: Some(entry.close_time),
                entry_price: Some(entry_price),
                holding_windows: window_results,
            }
        })
        .collect::<Vec<_>>();

    let per_holding_window = holding_windows
        .iter()
        .copied()
        .map(|holding_window| {
            let mut values = trades
                .iter()
                .flat_map(|trade| trade.holding_windows.iter())
                .filter(|window| window.holding_window == holding_window)
                .filter(|window| window.status != ResearchShadowPnlStatus::GapDetected)
                .filter_map(|window| window.net_pnl_pct)
                .collect::<Vec<_>>();
            values.sort();
            let trade_count = values.len() as i64;
            let total_net_pnl_pct = values.iter().copied().sum::<Decimal>();
            let win_count = values
                .iter()
                .filter(|value| **value > Decimal::ZERO)
                .count() as i64;
            let avg_net_pnl_pct = if trade_count > 0 {
                total_net_pnl_pct / Decimal::from(trade_count)
            } else {
                Decimal::ZERO
            };
            let median_net_pnl_pct = median_decimal(&values);
            let best_net_pnl_pct = values.last().copied().unwrap_or(Decimal::ZERO);
            let worst_net_pnl_pct = values.first().copied().unwrap_or(Decimal::ZERO);
            let win_rate = calculate_percentage_rate(win_count, trade_count);
            let total_fee_drag_pct = fee_drag_pct * Decimal::from(trade_count);
            let recommendation = if trade_count < 3 {
                ResearchShadowPnlRecommendation::InsufficientData
            } else if avg_net_pnl_pct < Decimal::ZERO && total_net_pnl_pct < Decimal::ZERO {
                ResearchShadowPnlRecommendation::Negative
            } else if avg_net_pnl_pct > Decimal::ZERO && win_rate >= Decimal::new(50, 0) {
                ResearchShadowPnlRecommendation::Promising
            } else {
                ResearchShadowPnlRecommendation::Weak
            };

            ResearchShadowPnlHoldingWindowResult {
                holding_window,
                trade_count,
                win_rate,
                avg_net_pnl_pct,
                median_net_pnl_pct,
                best_net_pnl_pct,
                worst_net_pnl_pct,
                total_net_pnl_pct,
                fee_drag_pct: total_fee_drag_pct,
                recommendation,
            }
        })
        .collect::<Vec<_>>();

    let total_attributed_runs = trades
        .iter()
        .filter(|trade| {
            trade.holding_windows.iter().any(|window| {
                window.net_pnl_pct.is_some()
                    && window.status != ResearchShadowPnlStatus::GapDetected
            })
        })
        .count() as i64;
    let insufficient_forward_data_count = trades
        .iter()
        .flat_map(|trade| trade.holding_windows.iter())
        .filter(|window| window.status == ResearchShadowPnlStatus::InsufficientForwardData)
        .count() as i64;
    let gap_detected_count = trades
        .iter()
        .flat_map(|trade| trade.holding_windows.iter())
        .filter(|window| window.status == ResearchShadowPnlStatus::GapDetected)
        .count() as i64;
    let extreme_pnl_count = trades
        .iter()
        .flat_map(|trade| trade.holding_windows.iter())
        .filter_map(|window| window.net_pnl_pct)
        .filter(|net_pnl_pct| net_pnl_pct.abs() > extreme_pnl_threshold_pct)
        .count() as i64;
    let mut warnings = Vec::new();
    if extreme_pnl_count > 0 {
        warnings.push(
            "Attribution PnL is unusually large; inspect candle continuity and timestamps."
                .to_string(),
        );
    }
    if gap_detected_count > 0 {
        warnings.push("Attribution crossed one or more candle data gaps.".to_string());
    }
    let negative_all_windows = !per_holding_window.is_empty()
        && per_holding_window.iter().all(|window| {
            window.recommendation == ResearchShadowPnlRecommendation::Negative
                || window.recommendation == ResearchShadowPnlRecommendation::InsufficientData
        })
        && per_holding_window
            .iter()
            .any(|window| window.recommendation == ResearchShadowPnlRecommendation::Negative);
    let best_window = per_holding_window
        .iter()
        .filter(|window| window.trade_count > 0)
        .max_by(|left, right| left.avg_net_pnl_pct.cmp(&right.avg_net_pnl_pct));
    let best_holding_window = best_window.map(|window| window.holding_window);
    let best_avg_net_pnl_pct = best_window.map(|window| window.avg_net_pnl_pct);
    let latest_shadow_pnl_status = if per_holding_window
        .iter()
        .any(|window| window.recommendation == ResearchShadowPnlRecommendation::Promising)
    {
        ResearchShadowPnlRecommendation::Promising
    } else if negative_all_windows {
        ResearchShadowPnlRecommendation::Negative
    } else if total_attributed_runs == 0 {
        ResearchShadowPnlRecommendation::InsufficientData
    } else {
        ResearchShadowPnlRecommendation::Weak
    };

    ResearchShadowPnlAttributionResult {
        candidate_id: candidate.id,
        strategy_id: candidate.strategy_id.clone(),
        symbol: candidate.symbol.clone(),
        timeframe: candidate.timeframe.clone(),
        holding_windows,
        fee_bps: request.fee_bps,
        slippage_bps: request.slippage_bps,
        extreme_pnl_threshold_pct,
        start_time: request.start_time,
        end_time: request.end_time,
        summary: ResearchShadowPnlSummary {
            total_attributed_runs,
            extreme_pnl_count,
            gap_detected_count,
            insufficient_forward_data_count,
            negative_all_windows,
            warnings,
            per_holding_window,
        },
        trades,
        latest_shadow_pnl_status,
        best_holding_window,
        best_avg_net_pnl_pct,
        computed_at,
    }
}

fn median_decimal(values: &[Decimal]) -> Decimal {
    if values.is_empty() {
        return Decimal::ZERO;
    }
    let mid = values.len() / 2;
    if values.len() % 2 == 1 {
        values[mid]
    } else {
        (values[mid - 1] + values[mid]) / Decimal::new(2, 0)
    }
}

pub fn evaluate_strategy_candidate_observation(
    requirements: &StrategyCandidateObservationRequirement,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    shadow_runs: i64,
    would_submit_count: i64,
    no_signal_count: i64,
    risk_rejected_count: i64,
    skipped_count: i64,
    latest_readiness_status: Option<ExecutionReadinessStatus>,
    latest_readiness_score: Option<i32>,
    runner_alignment: StrategyCandidateRunnerAlignment,
    created_at: DateTime<Utc>,
) -> StrategyCandidateObservationSummary {
    let risk_rejection_rate = calculate_strategy_rejection_rate(risk_rejected_count, shadow_runs);
    let no_signal_rate = calculate_observation_rate(no_signal_count, shadow_runs);
    let mut findings = Vec::new();
    let mut recommendations = Vec::new();
    let observed_hours = window_end.signed_duration_since(window_start).num_hours();
    let mut decision = StrategyCandidateObservationDecision::Pass;

    if !runner_alignment.strategy_config_matches_runner {
        decision = StrategyCandidateObservationDecision::InsufficientData;
        findings.push(StrategyCandidateObservationFinding {
            code: "shadow_runner_config_mismatch".to_string(),
            message: "Shadow runner is not configured for candidate timeframe/symbol/strategy."
                .to_string(),
            blocking: true,
        });
        recommendations.push(format!(
            "Update shadow runner config to include {} {} {}.",
            requirements.strategy_id, requirements.symbol, requirements.timeframe
        ));
    }

    if observed_hours < requirements.min_observation_hours {
        if decision == StrategyCandidateObservationDecision::Pass {
            decision = StrategyCandidateObservationDecision::ContinueObserving;
        }
        findings.push(StrategyCandidateObservationFinding {
            code: "not_enough_time_observed".to_string(),
            message: format!(
                "Observed {observed_hours}h but requires at least {}h.",
                requirements.min_observation_hours
            ),
            blocking: true,
        });
    }

    if shadow_runs < requirements.min_shadow_runs {
        decision = StrategyCandidateObservationDecision::InsufficientData;
        findings.push(StrategyCandidateObservationFinding {
            code: "not_enough_shadow_runs".to_string(),
            message: format!(
                "Observed {shadow_runs} shadow runs but requires at least {}.",
                requirements.min_shadow_runs
            ),
            blocking: true,
        });
    }

    if decision == StrategyCandidateObservationDecision::Pass {
        if requirements.require_readiness_ready
            && latest_readiness_status != Some(ExecutionReadinessStatus::Ready)
        {
            decision = StrategyCandidateObservationDecision::Fail;
            findings.push(StrategyCandidateObservationFinding {
                code: "readiness_not_ready".to_string(),
                message: format!(
                    "Latest TESTNET_SHADOW readiness was {}.",
                    latest_readiness_status
                        .map(|value| value.as_str().to_string())
                        .unwrap_or_else(|| "UNKNOWN".to_string())
                ),
                blocking: true,
            });
        }

        if would_submit_count < requirements.min_would_submit_count {
            decision = StrategyCandidateObservationDecision::Fail;
            findings.push(StrategyCandidateObservationFinding {
                code: "zero_or_low_would_submit".to_string(),
                message: format!(
                    "Observed {would_submit_count} WOULD_SUBMIT runs but requires at least {}.",
                    requirements.min_would_submit_count
                ),
                blocking: true,
            });
        }

        if let Some(max_risk_rejection_rate) = requirements.max_risk_rejection_rate {
            if risk_rejection_rate > max_risk_rejection_rate {
                decision = StrategyCandidateObservationDecision::Fail;
                findings.push(StrategyCandidateObservationFinding {
                    code: "high_risk_rejection_rate".to_string(),
                    message: format!(
                        "Risk rejection rate {} exceeded max {}.",
                        risk_rejection_rate.round_dp(4),
                        max_risk_rejection_rate.round_dp(4)
                    ),
                    blocking: true,
                });
            }
        }

        if let Some(max_no_signal_rate) = requirements.max_no_signal_rate {
            if no_signal_rate > max_no_signal_rate {
                decision = StrategyCandidateObservationDecision::Fail;
                findings.push(StrategyCandidateObservationFinding {
                    code: "high_no_signal_rate".to_string(),
                    message: format!(
                        "No-signal rate {} exceeded max {}.",
                        no_signal_rate.round_dp(4),
                        max_no_signal_rate.round_dp(4)
                    ),
                    blocking: true,
                });
            }
        }
    }

    if findings.is_empty() {
        findings.push(StrategyCandidateObservationFinding {
            code: "requirements_met".to_string(),
            message: "Observation requirements were met.".to_string(),
            blocking: false,
        });
    }

    StrategyCandidateObservationSummary {
        candidate_id: requirements.candidate_id,
        window_start,
        window_end,
        shadow_runs,
        would_submit_count,
        no_signal_count,
        risk_rejected_count,
        skipped_count,
        risk_rejection_rate: risk_rejection_rate.round_dp(4),
        no_signal_rate: no_signal_rate.round_dp(4),
        latest_readiness_status,
        latest_readiness_score,
        runner_alignment,
        decision,
        findings,
        recommendations,
        created_at,
    }
}

pub fn expected_strategy_research_promotion_confirmation(strategy_id: &str) -> String {
    format!(
        "PROMOTE STRATEGY {}",
        strategy_id.trim().to_ascii_uppercase()
    )
}

pub fn expected_research_candidate_shadow_promotion_confirmation(candidate_id: Uuid) -> String {
    format!("PROMOTE CANDIDATE {candidate_id} TO SHADOW")
}

pub fn expected_research_candidate_accept_shadow_confirmation(candidate_id: Uuid) -> String {
    format!("ACCEPT CANDIDATE {candidate_id} FOR SHADOW")
}

pub fn is_valid_research_candidate_accept_shadow_confirmation(
    candidate_id: Uuid,
    confirmation_text: &str,
) -> bool {
    confirmation_text == expected_research_candidate_accept_shadow_confirmation(candidate_id)
}

pub fn is_valid_research_candidate_shadow_promotion_confirmation(
    candidate_id: Uuid,
    confirmation_text: &str,
) -> bool {
    confirmation_text == expected_research_candidate_shadow_promotion_confirmation(candidate_id)
}

pub fn is_valid_strategy_research_promotion_confirmation(
    strategy_id: &str,
    confirmation_text: &str,
) -> bool {
    confirmation_text == expected_strategy_research_promotion_confirmation(strategy_id)
}

pub fn score_strategy_research_candidate(
    evidence: &StrategyResearchCandidateEvidence,
) -> StrategyResearchCandidateScore {
    let mut score = Decimal::new(50, 0);
    let mut warnings = Vec::new();
    let mut rejection_hints = Vec::new();

    if let Some(robustness) = evidence.robustness_score {
        score += robustness * Decimal::new(35, 0);
        if robustness < Decimal::new(40, 2) {
            warnings.push("low_walk_forward_robustness".to_string());
        }
    } else {
        warnings.push("missing_walk_forward_robustness".to_string());
    }

    if let Some(pnl_pct) = evidence.pnl_pct {
        score += pnl_pct * Decimal::new(2, 0);
        if pnl_pct <= Decimal::ZERO {
            warnings.push("non_positive_net_pnl_pct".to_string());
        }
    } else {
        warnings.push("missing_net_pnl_pct".to_string());
    }

    if let Some(drawdown) = evidence.max_drawdown_pct {
        let penalty = drawdown * Decimal::new(3, 0);
        score -= penalty;
        if drawdown >= Decimal::new(15, 0) {
            warnings.push("high_drawdown_penalty".to_string());
            rejection_hints.push(StrategyResearchCandidateRejectionReason::MissingEvidence);
        }
    }

    let fee_paid = evidence.fee_paid.unwrap_or(Decimal::ZERO);
    let slippage_cost = evidence.slippage_cost.unwrap_or(Decimal::ZERO);
    let drag_penalty = (fee_paid + slippage_cost) / Decimal::new(50, 0);
    if drag_penalty > Decimal::ZERO {
        score -= drag_penalty;
    }
    if fee_paid > Decimal::ZERO || slippage_cost > Decimal::ZERO {
        warnings.push("fee_slippage_drag_penalty".to_string());
    }

    match evidence.trade_count.unwrap_or_default() {
        0 => {
            score -= Decimal::new(10, 0);
            warnings.push("zero_trade_count".to_string());
        }
        1..=2 => {
            score -= Decimal::new(8, 0);
            warnings.push("thin_trade_count".to_string());
        }
        3..=5 => {
            score -= Decimal::new(3, 0);
            warnings.push("low_trade_count".to_string());
        }
        6..=200 => {}
        _ => {
            score -= Decimal::new(5, 0);
            warnings.push("trade_count_outlier".to_string());
        }
    }

    let skipped_windows = evidence.skipped_windows.unwrap_or_default();
    if skipped_windows > 0 {
        score -= Decimal::from(skipped_windows) * Decimal::new(4, 0);
        warnings.push("skipped_windows_penalty".to_string());
    }

    let completed_windows = evidence.profitable_windows.unwrap_or_default()
        + evidence.losing_windows.unwrap_or_default();
    if completed_windows == 1 {
        score -= Decimal::new(12, 0);
        warnings.push("single_window_overfitting_warning".to_string());
    }

    if score < Decimal::ZERO {
        score = Decimal::ZERO;
    }
    if score > Decimal::new(100, 0) {
        score = Decimal::new(100, 0);
    }

    warnings.sort();
    warnings.dedup();
    rejection_hints.sort_by_key(|value| value.as_str());
    rejection_hints.dedup();

    StrategyResearchCandidateScore {
        score: score.round_dp(2),
        warnings,
        rejection_hints,
    }
}

pub fn expected_candle_open_times(
    interval: CandleInterval,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Vec<DateTime<Utc>> {
    if end_time <= start_time {
        return Vec::new();
    }

    let effective_end = end_time.min(now);
    if effective_end <= start_time {
        return Vec::new();
    }

    let mut cursor = interval.bucket_start(start_time);
    if cursor < start_time {
        cursor += interval.duration();
    }

    let mut expected = Vec::new();
    while cursor + interval.duration() <= effective_end {
        expected.push(cursor);
        cursor += interval.duration();
    }

    expected
}

pub fn detect_research_data_gaps(
    interval: CandleInterval,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    now: DateTime<Utc>,
    actual_open_times: &[DateTime<Utc>],
) -> Vec<ResearchDataGap> {
    let expected = expected_candle_open_times(interval, start_time, end_time, now);
    if expected.is_empty() {
        return Vec::new();
    }

    let actual = actual_open_times.iter().copied().collect::<BTreeSet<_>>();
    let mut gaps = Vec::new();
    let mut gap_start = None;
    let mut gap_count = 0_i64;
    let mut previous_missing = None;

    for open_time in expected {
        if actual.contains(&open_time) {
            if let Some(start) = gap_start.take() {
                let last_missing = previous_missing.expect("gap should track previous missing");
                gaps.push(ResearchDataGap {
                    start_time: start,
                    end_time: last_missing + interval.duration(),
                    missing_candles: gap_count,
                });
                gap_count = 0;
                previous_missing = None;
            }
            continue;
        }

        if gap_start.is_none() {
            gap_start = Some(open_time);
        }
        gap_count += 1;
        previous_missing = Some(open_time);
    }

    if let Some(start) = gap_start {
        let last_missing = previous_missing.expect("trailing gap should have last missing");
        gaps.push(ResearchDataGap {
            start_time: start,
            end_time: last_missing + interval.duration(),
            missing_candles: gap_count,
        });
    }

    gaps
}

#[derive(Debug, Clone)]
struct CrossAssetFeatureRow {
    return_6h: Option<Decimal>,
    return_24h: Option<Decimal>,
    return_72h: Option<Decimal>,
    lookback_return: Option<Decimal>,
    vol_24h: Option<Decimal>,
    vol_72h: Option<Decimal>,
    lookback_vol: Option<Decimal>,
    distance_72h_high_pct: Option<Decimal>,
}

#[derive(Debug, Clone, Copy)]
struct CrossAssetFeatureLookbacks {
    ranking: usize,
    six_hours: usize,
    twenty_four_hours: usize,
    seventy_two_hours: usize,
}

#[derive(Debug, Clone)]
struct CrossAssetScore {
    symbol: String,
    score: Decimal,
    return_pct: Decimal,
}

#[derive(Debug, Clone)]
struct CrossAssetExit {
    index: usize,
    reason: String,
}

#[derive(Debug, Clone)]
struct CrossAssetMetricSummary {
    net_pnl_pct: Decimal,
    compounded_pnl_pct: Decimal,
    avg_trade_pnl_pct: Decimal,
    median_trade_pnl_pct: Decimal,
    win_rate: Decimal,
    max_drawdown_pct: Decimal,
    worst_trade_pct: Decimal,
    best_trade_pct: Decimal,
}

pub fn run_cross_asset_relative_strength_research(
    request: CrossAssetResearchRequest,
    candles_by_symbol: BTreeMap<String, Vec<Candle>>,
) -> Result<CrossAssetResearchRun, CoreError> {
    request.validate()?;
    let run_id = Uuid::new_v4();
    let correlation_id = request.correlation_id.unwrap_or_else(Uuid::new_v4);
    let created_at = Utc::now();
    let symbols = request
        .symbols
        .iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .collect::<Vec<_>>();
    let interval = request.parsed_timeframe()?;
    let interval_seconds = interval.duration().num_seconds();
    let ranking_lookback = cross_asset_hours_to_candles(request.ranking_lookback_hours, interval)?;
    let holding = cross_asset_hours_to_candles(request.holding_hours, interval)?;
    let feature_lookbacks = CrossAssetFeatureLookbacks {
        ranking: ranking_lookback,
        six_hours: cross_asset_hours_to_candles(6, interval)?,
        twenty_four_hours: cross_asset_hours_to_candles(24, interval)?,
        seventy_two_hours: cross_asset_hours_to_candles(72, interval)?,
    };
    let required_lookback = feature_lookbacks
        .ranking
        .max(feature_lookbacks.seventy_two_hours)
        .max(feature_lookbacks.twenty_four_hours)
        .max(feature_lookbacks.six_hours);

    let mut aligned_times: Option<BTreeSet<DateTime<Utc>>> = None;
    let mut normalized_candles = BTreeMap::new();
    for symbol in &symbols {
        let mut candles = candles_by_symbol.get(symbol).cloned().ok_or_else(|| {
            CoreError::InvalidCrossAssetResearchRequest(format!("missing candles for {symbol}"))
        })?;
        candles.sort_by_key(|candle| candle.open_time);
        candles.retain(|candle| {
            candle.interval == interval
                && candle.is_closed
                && candle.open_time >= request.start_time
                && candle.close_time <= request.end_time
        });
        let times = candles
            .iter()
            .map(|candle| candle.open_time)
            .collect::<BTreeSet<_>>();
        aligned_times = Some(match aligned_times {
            Some(existing) => existing.intersection(&times).copied().collect(),
            None => times,
        });
        normalized_candles.insert(symbol.clone(), candles);
    }

    let aligned_times = aligned_times.unwrap_or_default();
    let expected = i64::from(interval.candles_between(request.start_time, request.end_time)?);
    if aligned_times.len() as i64 != expected {
        return Err(CoreError::InvalidCrossAssetResearchRequest(format!(
            "insufficient aligned candle coverage: expected {expected}, actual {}",
            aligned_times.len()
        )));
    }
    let times = aligned_times.iter().copied().collect::<Vec<_>>();
    for pair in times.windows(2) {
        if pair[1].signed_duration_since(pair[0]).num_seconds() != interval_seconds {
            return Err(CoreError::InvalidCrossAssetResearchRequest(
                "aligned candles contain a gap".to_string(),
            ));
        }
    }
    if times.len() <= required_lookback + holding + 1 {
        return Err(CoreError::InvalidCrossAssetResearchRequest(
            "not enough candles for requested lookback and holding period".to_string(),
        ));
    }

    let mut candles = BTreeMap::new();
    for symbol in &symbols {
        let by_time = normalized_candles
            .remove(symbol)
            .unwrap_or_default()
            .into_iter()
            .map(|candle| (candle.open_time, candle))
            .collect::<BTreeMap<_, _>>();
        candles.insert(
            symbol.clone(),
            times
                .iter()
                .filter_map(|time| by_time.get(time).cloned())
                .collect::<Vec<_>>(),
        );
    }

    let features = compute_cross_asset_features(&symbols, &candles, feature_lookbacks);
    let basket_vol_threshold = match &request.vol_filter {
        CrossAssetVolFilter::BasketVolBelowPercentile { percentile } => {
            basket_vol_percentile(&symbols, &features, *percentile)
        }
        _ => None,
    };
    let median_vol_24h = median_asset_vol_24h(&symbols, &features).unwrap_or(Decimal::ZERO);
    let drag = (request.fee_bps + request.slippage_bps) * Decimal::new(2, 0) / Decimal::new(100, 0);
    let mut trades = Vec::new();
    let mut next_available_index = 0usize;

    for index in required_lookback..times.len().saturating_sub(holding + 1) {
        if request.one_active_position && index < next_available_index {
            continue;
        }
        if !cross_asset_market_filter_passes(&request, &symbols, &features, index) {
            continue;
        }
        if !basket_vol_filter_passes(&request, &symbols, &features, index, basket_vol_threshold) {
            continue;
        }
        let ranking = cross_asset_ranking(&request, &symbols, &features, index);
        if ranking.len() < symbols.len() {
            continue;
        }
        let top = &ranking[0];
        let second = &ranking[1];
        if top.return_pct <= request.min_top_return_pct {
            continue;
        }
        let spread = top.return_pct - second.return_pct;
        if spread < request.min_rank_spread_pct {
            continue;
        }
        if !cross_asset_vol_filter_passes(&request, &symbols, &features, index, &top.symbol) {
            continue;
        }
        if !cross_asset_overextension_filter_passes(&request, &features, index, &top.symbol) {
            continue;
        }

        let entry_index = index + 1;
        let Some(exit) =
            cross_asset_exit_index(&request, holding, &candles, &top.symbol, entry_index)
        else {
            continue;
        };
        if exit.index <= entry_index || exit.index >= times.len() {
            continue;
        }
        let symbol_candles = candles
            .get(&top.symbol)
            .expect("selected symbol must have candles");
        let entry_price = symbol_candles[entry_index].open;
        let exit_price = symbol_candles[exit.index].open;
        if entry_price <= Decimal::ZERO {
            continue;
        }
        let weight = cross_asset_weight(&request, &features, &top.symbol, index, median_vol_24h);
        let gross_pct = percent(exit_price / entry_price - Decimal::ONE);
        let net_pct = (gross_pct - drag) * weight;
        let feature = features
            .get(&top.symbol)
            .and_then(|rows| rows.get(index))
            .expect("feature must exist");
        let snapshot = CrossAssetPortfolioRankingSnapshot {
            signal_time: times[index],
            selected_symbol: top.symbol.clone(),
            top_score: top.score,
            second_score: second.score,
            rank_spread_pct: spread,
            top_return_pct: top.return_pct,
            return_6h_pct: feature.return_6h.map(percent),
            return_24h_pct: feature.return_24h.map(percent),
            return_72h_pct: feature.return_72h.map(percent),
            realized_vol_24h_pct: feature.vol_24h.map(percent),
            realized_vol_72h_pct: feature.vol_72h.map(percent),
            distance_from_72h_high_pct: feature.distance_72h_high_pct,
            basket_return_24h_pct: basket_return_pct(&symbols, &features, index, 24),
            basket_return_72h_pct: basket_return_pct(&symbols, &features, index, 72),
            basket_vol_72h_pct: basket_vol_pct(&symbols, &features, index),
        };
        trades.push(CrossAssetPortfolioTrade {
            id: Uuid::new_v4(),
            run_id,
            trade_index: trades.len() as i32,
            symbol: top.symbol.clone(),
            signal_time: times[index],
            entry_time: times[entry_index],
            exit_time: times[exit.index],
            entry_price,
            exit_price,
            weight,
            gross_pnl_pct: gross_pct * weight,
            net_pnl_pct: net_pct,
            fee_slippage_drag_pct: drag * weight,
            exit_reason: exit.reason,
            ranking_snapshot: snapshot,
        });
        if request.one_active_position {
            next_available_index = exit.index + 1;
        }
    }

    let windows = cross_asset_windows(run_id, request.start_time, request.end_time, &trades);
    let summary = cross_asset_metric_summary(&trades);
    let window_pnls = windows
        .iter()
        .map(|window| window.compounded_pnl_pct)
        .collect::<Vec<_>>();
    let symbol_distribution = symbol_distribution(&trades);
    let max_symbol_concentration_pct =
        max_symbol_concentration_pct(&symbol_distribution, trades.len());
    let profitable_windows = windows
        .iter()
        .filter(|window| window.compounded_pnl_pct > Decimal::ZERO)
        .count() as i32;
    let worst_window_pnl_pct = window_pnls.iter().copied().min().unwrap_or(Decimal::ZERO);
    let median_window_pnl_pct = median_decimal_vec(window_pnls);
    let mut warnings = Vec::new();
    if max_symbol_concentration_pct > Decimal::new(60, 0) {
        warnings.push("max_symbol_concentration_gt_60_pct".to_string());
    }
    if worst_window_pnl_pct < Decimal::new(-15, 0) {
        warnings.push("worst_window_below_minus_15_pct".to_string());
    }
    if has_weak_2025_plus(&trades) {
        warnings.push("2025_plus_weak_relative_to_2023_2024".to_string());
    }
    let portfolio_status = classify_cross_asset_portfolio(
        trades.len() as i32,
        &summary,
        max_symbol_concentration_pct,
        worst_window_pnl_pct,
    );
    let recommendation = match portfolio_status {
        CrossAssetPortfolioStatus::Promising => {
            CrossAssetResearchRecommendation::ConsiderImplementationResearchOnly
        }
        CrossAssetPortfolioStatus::Weak | CrossAssetPortfolioStatus::OverfitRisk => {
            CrossAssetResearchRecommendation::KeepResearching
        }
        CrossAssetPortfolioStatus::Negative | CrossAssetPortfolioStatus::TooSparse => {
            CrossAssetResearchRecommendation::Reject
        }
    };
    let result = CrossAssetResearchResult {
        run_id,
        strategy_kind: request.strategy_kind,
        request,
        status: CrossAssetResearchStatus::Completed,
        portfolio_status,
        recommendation,
        total_trades: trades.len() as i32,
        net_pnl_pct: summary.net_pnl_pct,
        compounded_pnl_pct: summary.compounded_pnl_pct,
        avg_trade_pnl_pct: summary.avg_trade_pnl_pct,
        median_trade_pnl_pct: summary.median_trade_pnl_pct,
        win_rate: summary.win_rate,
        max_drawdown_pct: summary.max_drawdown_pct,
        worst_trade_pct: summary.worst_trade_pct,
        best_trade_pct: summary.best_trade_pct,
        fee_slippage_drag_pct: trades.iter().fold(Decimal::ZERO, |sum, trade| {
            sum + trade.fee_slippage_drag_pct
        }),
        symbol_distribution,
        max_symbol_concentration_pct,
        window_count: windows.len() as i32,
        profitable_windows,
        worst_window_pnl_pct,
        median_window_pnl_pct,
        warnings,
        created_at,
        completed_at: Some(Utc::now()),
        correlation_id,
    };

    Ok(CrossAssetResearchRun {
        result,
        trades,
        windows,
    })
}

fn compute_cross_asset_features(
    symbols: &[String],
    candles: &BTreeMap<String, Vec<Candle>>,
    lookbacks: CrossAssetFeatureLookbacks,
) -> BTreeMap<String, Vec<CrossAssetFeatureRow>> {
    let mut out = BTreeMap::new();
    for symbol in symbols {
        let rows = candles.get(symbol).expect("symbol candles must exist");
        let hourly_returns = hourly_returns(rows);
        let mut features = Vec::with_capacity(rows.len());
        for index in 0..rows.len() {
            let distance_72h_high_pct = if index + 1 >= lookbacks.seventy_two_hours {
                let start = index + 1 - lookbacks.seventy_two_hours;
                let max_high = rows[start..=index]
                    .iter()
                    .map(|candle| candle.high)
                    .max()
                    .unwrap_or(Decimal::ZERO);
                (max_high > Decimal::ZERO)
                    .then(|| percent((max_high - rows[index].close) / max_high))
            } else {
                None
            };
            features.push(CrossAssetFeatureRow {
                return_6h: return_over(rows, index, lookbacks.six_hours),
                return_24h: return_over(rows, index, lookbacks.twenty_four_hours),
                return_72h: return_over(rows, index, lookbacks.seventy_two_hours),
                lookback_return: return_over(rows, index, lookbacks.ranking),
                vol_24h: realized_vol(&hourly_returns, index, lookbacks.twenty_four_hours),
                vol_72h: realized_vol(&hourly_returns, index, lookbacks.seventy_two_hours),
                lookback_vol: realized_vol(&hourly_returns, index, lookbacks.ranking),
                distance_72h_high_pct,
            });
        }
        out.insert(symbol.clone(), features);
    }
    out
}

fn hourly_returns(candles: &[Candle]) -> Vec<Option<Decimal>> {
    let mut returns = vec![None; candles.len()];
    for index in 1..candles.len() {
        let previous = candles[index - 1].close;
        if previous > Decimal::ZERO {
            returns[index] = Some(candles[index].close / previous - Decimal::ONE);
        }
    }
    returns
}

fn cross_asset_hours_to_candles(hours: u32, interval: CandleInterval) -> Result<usize, CoreError> {
    let interval_hours = interval.duration().num_seconds() / 3_600;
    if interval_hours <= 0 {
        return Err(CoreError::InvalidCrossAssetResearchRequest(
            "cross-asset research requires an hourly-or-slower timeframe".to_string(),
        ));
    }
    let hours = i64::from(hours);
    let candles = (hours + interval_hours - 1) / interval_hours;
    usize::try_from(candles.max(1)).map_err(|_| {
        CoreError::InvalidCrossAssetResearchRequest(
            "lookback or holding period is too large".to_string(),
        )
    })
}

fn return_over(candles: &[Candle], index: usize, lookback: usize) -> Option<Decimal> {
    if index < lookback {
        return None;
    }
    let previous = candles[index - lookback].close;
    (previous > Decimal::ZERO).then(|| candles[index].close / previous - Decimal::ONE)
}

fn realized_vol(returns: &[Option<Decimal>], index: usize, lookback: usize) -> Option<Decimal> {
    if index < lookback {
        return None;
    }
    let values = returns[index + 1 - lookback..=index]
        .iter()
        .copied()
        .collect::<Option<Vec<_>>>()?;
    let mean = values.iter().copied().sum::<Decimal>() / Decimal::from(lookback as i64);
    let variance = values
        .iter()
        .map(|value| {
            let diff = *value - mean;
            diff * diff
        })
        .sum::<Decimal>()
        / Decimal::from(lookback as i64);
    Decimal::from_f64(variance.to_f64().unwrap_or(0.0).sqrt())
}

fn cross_asset_ranking(
    request: &CrossAssetResearchRequest,
    symbols: &[String],
    features: &BTreeMap<String, Vec<CrossAssetFeatureRow>>,
    index: usize,
) -> Vec<CrossAssetScore> {
    let mut scores = Vec::new();
    for symbol in symbols {
        let Some(feature) = features.get(symbol).and_then(|rows| rows.get(index)) else {
            continue;
        };
        let Some(return_value) = feature.lookback_return else {
            continue;
        };
        let score = match request.rank_metric {
            CrossAssetRankMetric::RawReturn => return_value,
            CrossAssetRankMetric::VolAdjustedReturn => {
                let Some(vol) = feature.lookback_vol else {
                    continue;
                };
                if vol <= Decimal::ZERO {
                    continue;
                }
                return_value / vol
            }
        };
        scores.push(CrossAssetScore {
            symbol: symbol.clone(),
            score,
            return_pct: percent(return_value),
        });
    }
    scores.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.symbol.cmp(&right.symbol))
    });
    scores
}

fn cross_asset_market_filter_passes(
    request: &CrossAssetResearchRequest,
    symbols: &[String],
    features: &BTreeMap<String, Vec<CrossAssetFeatureRow>>,
    index: usize,
) -> bool {
    match &request.market_filter {
        CrossAssetMarketFilter::None => true,
        CrossAssetMarketFilter::Basket72hReturnGt { threshold_pct } => {
            basket_return_pct(symbols, features, index, 72)
                .is_some_and(|value| value > *threshold_pct)
        }
        CrossAssetMarketFilter::Basket24hReturnGt { threshold_pct } => {
            basket_return_pct(symbols, features, index, 24)
                .is_some_and(|value| value > *threshold_pct)
        }
        CrossAssetMarketFilter::AtLeastNSymbolsPositive24h { min_symbols } => {
            let count = symbols
                .iter()
                .filter(|symbol| {
                    features
                        .get(*symbol)
                        .and_then(|rows| rows.get(index))
                        .and_then(|feature| feature.return_24h)
                        .is_some_and(|value| value > Decimal::ZERO)
                })
                .count() as u32;
            count >= *min_symbols
        }
    }
}

fn cross_asset_vol_filter_passes(
    request: &CrossAssetResearchRequest,
    symbols: &[String],
    features: &BTreeMap<String, Vec<CrossAssetFeatureRow>>,
    index: usize,
    selected_symbol: &str,
) -> bool {
    match &request.vol_filter {
        CrossAssetVolFilter::None | CrossAssetVolFilter::BasketVolBelowPercentile { .. } => true,
        CrossAssetVolFilter::AssetNotExtremeVsBasket { max_ratio } => {
            let Some(asset_vol) = features
                .get(selected_symbol)
                .and_then(|rows| rows.get(index))
                .and_then(|feature| feature.vol_24h)
            else {
                return false;
            };
            let vols = symbols
                .iter()
                .filter_map(|symbol| {
                    features
                        .get(symbol)
                        .and_then(|rows| rows.get(index))
                        .and_then(|feature| feature.vol_24h)
                })
                .collect::<Vec<_>>();
            if vols.len() != symbols.len() {
                return false;
            }
            let basket = vols.iter().copied().sum::<Decimal>() / Decimal::from(vols.len() as i64);
            basket > Decimal::ZERO && asset_vol <= basket * *max_ratio
        }
    }
}

fn basket_vol_filter_passes(
    request: &CrossAssetResearchRequest,
    symbols: &[String],
    features: &BTreeMap<String, Vec<CrossAssetFeatureRow>>,
    index: usize,
    threshold: Option<Decimal>,
) -> bool {
    match &request.vol_filter {
        CrossAssetVolFilter::BasketVolBelowPercentile { .. } => threshold
            .zip(basket_vol_decimal(symbols, features, index))
            .is_some_and(|(threshold, value)| value <= threshold),
        _ => true,
    }
}

fn cross_asset_overextension_filter_passes(
    request: &CrossAssetResearchRequest,
    features: &BTreeMap<String, Vec<CrossAssetFeatureRow>>,
    index: usize,
    selected_symbol: &str,
) -> bool {
    let Some(feature) = features
        .get(selected_symbol)
        .and_then(|rows| rows.get(index))
    else {
        return false;
    };
    match &request.overextension_filter {
        CrossAssetOverextensionFilter::None => true,
        CrossAssetOverextensionFilter::MaxReturn24hPct { max_pct } => feature
            .return_24h
            .map(percent)
            .is_some_and(|value| value <= *max_pct),
        CrossAssetOverextensionFilter::MinDistanceFrom72hHighPct { min_pct } => feature
            .distance_72h_high_pct
            .is_some_and(|value| value >= *min_pct),
    }
}

fn cross_asset_weight(
    request: &CrossAssetResearchRequest,
    features: &BTreeMap<String, Vec<CrossAssetFeatureRow>>,
    symbol: &str,
    index: usize,
    median_vol_24h: Decimal,
) -> Decimal {
    match request.sizing_mode {
        CrossAssetSizingMode::EqualNotional => request.max_weight,
        CrossAssetSizingMode::VolatilityNormalized => {
            let Some(vol) = features
                .get(symbol)
                .and_then(|rows| rows.get(index))
                .and_then(|feature| feature.vol_24h)
            else {
                return request.min_weight;
            };
            if vol <= Decimal::ZERO || median_vol_24h <= Decimal::ZERO {
                return request.min_weight;
            }
            (median_vol_24h / vol).clamp(request.min_weight, request.max_weight)
        }
    }
}

fn cross_asset_exit_index(
    request: &CrossAssetResearchRequest,
    holding_candles: usize,
    candles: &BTreeMap<String, Vec<Candle>>,
    symbol: &str,
    entry_index: usize,
) -> Option<CrossAssetExit> {
    let rows = candles.get(symbol)?;
    let deadline = (entry_index + holding_candles).min(rows.len().saturating_sub(1));
    let entry = rows.get(entry_index)?.open;
    match &request.exit_rule {
        CrossAssetExitRule::FixedHold => Some(CrossAssetExit {
            index: deadline,
            reason: "fixed_hold".to_string(),
        }),
        CrossAssetExitRule::StopPct { stop_pct } => {
            let stop = -*stop_pct;
            for index in entry_index..deadline {
                let pnl_pct = percent(rows[index].close / entry - Decimal::ONE);
                if pnl_pct <= stop {
                    return Some(CrossAssetExit {
                        index: (index + 1).min(rows.len().saturating_sub(1)),
                        reason: "stop_pct".to_string(),
                    });
                }
            }
            Some(CrossAssetExit {
                index: deadline,
                reason: "fixed_hold".to_string(),
            })
        }
        CrossAssetExitRule::TakeProfitPct { take_profit_pct } => {
            for index in entry_index..deadline {
                let pnl_pct = percent(rows[index].close / entry - Decimal::ONE);
                if pnl_pct >= *take_profit_pct {
                    return Some(CrossAssetExit {
                        index: (index + 1).min(rows.len().saturating_sub(1)),
                        reason: "take_profit_pct".to_string(),
                    });
                }
            }
            Some(CrossAssetExit {
                index: deadline,
                reason: "fixed_hold".to_string(),
            })
        }
    }
}

fn cross_asset_windows(
    run_id: Uuid,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    trades: &[CrossAssetPortfolioTrade],
) -> Vec<CrossAssetPortfolioWindow> {
    let mut windows = Vec::new();
    let mut cursor = start_time;
    let mut index = 0;
    while cursor < end_time {
        let window_end = (cursor + Duration::hours(24 * 91)).min(end_time);
        let window_trades = trades
            .iter()
            .filter(|trade| trade.entry_time >= cursor && trade.entry_time < window_end)
            .cloned()
            .collect::<Vec<_>>();
        let summary = cross_asset_metric_summary(&window_trades);
        windows.push(CrossAssetPortfolioWindow {
            id: Uuid::new_v4(),
            run_id,
            window_index: index,
            window_start: cursor,
            window_end,
            trade_count: window_trades.len() as i32,
            net_pnl_pct: summary.net_pnl_pct,
            compounded_pnl_pct: summary.compounded_pnl_pct,
            avg_trade_pnl_pct: summary.avg_trade_pnl_pct,
            median_trade_pnl_pct: summary.median_trade_pnl_pct,
            win_rate: summary.win_rate,
            max_drawdown_pct: summary.max_drawdown_pct,
            worst_trade_pct: summary.worst_trade_pct,
            best_trade_pct: summary.best_trade_pct,
            symbol_distribution: symbol_distribution(&window_trades),
        });
        cursor = window_end;
        index += 1;
    }
    windows
}

fn cross_asset_metric_summary(trades: &[CrossAssetPortfolioTrade]) -> CrossAssetMetricSummary {
    if trades.is_empty() {
        return CrossAssetMetricSummary {
            net_pnl_pct: Decimal::ZERO,
            compounded_pnl_pct: Decimal::ZERO,
            avg_trade_pnl_pct: Decimal::ZERO,
            median_trade_pnl_pct: Decimal::ZERO,
            win_rate: Decimal::ZERO,
            max_drawdown_pct: Decimal::ZERO,
            worst_trade_pct: Decimal::ZERO,
            best_trade_pct: Decimal::ZERO,
        };
    }
    let pnl_values = trades
        .iter()
        .map(|trade| trade.net_pnl_pct)
        .collect::<Vec<_>>();
    let net_pnl_pct = pnl_values.iter().copied().sum::<Decimal>();
    let avg_trade_pnl_pct = net_pnl_pct / Decimal::from(trades.len() as i64);
    let median_trade_pnl_pct = median_decimal_vec(pnl_values.clone());
    let wins = pnl_values
        .iter()
        .filter(|value| **value > Decimal::ZERO)
        .count();
    let win_rate =
        Decimal::from(wins as i64) * Decimal::new(100, 0) / Decimal::from(trades.len() as i64);
    let mut equity = Decimal::ONE;
    let mut peak = Decimal::ONE;
    let mut max_drawdown_pct = Decimal::ZERO;
    for pnl_pct in &pnl_values {
        equity *= Decimal::ONE + (*pnl_pct / Decimal::new(100, 0));
        peak = peak.max(equity);
        if peak > Decimal::ZERO {
            max_drawdown_pct = max_drawdown_pct.min(percent(equity / peak - Decimal::ONE));
        }
    }
    CrossAssetMetricSummary {
        net_pnl_pct,
        compounded_pnl_pct: percent(equity - Decimal::ONE),
        avg_trade_pnl_pct,
        median_trade_pnl_pct,
        win_rate,
        max_drawdown_pct,
        worst_trade_pct: pnl_values.iter().copied().min().unwrap_or(Decimal::ZERO),
        best_trade_pct: pnl_values.iter().copied().max().unwrap_or(Decimal::ZERO),
    }
}

fn classify_cross_asset_portfolio(
    trade_count: i32,
    summary: &CrossAssetMetricSummary,
    max_symbol_concentration_pct: Decimal,
    worst_window_pnl_pct: Decimal,
) -> CrossAssetPortfolioStatus {
    if trade_count < 50 {
        CrossAssetPortfolioStatus::TooSparse
    } else if summary.compounded_pnl_pct <= Decimal::ZERO {
        CrossAssetPortfolioStatus::Negative
    } else if max_symbol_concentration_pct > Decimal::new(60, 0)
        || summary.median_trade_pnl_pct <= Decimal::ZERO
    {
        CrossAssetPortfolioStatus::OverfitRisk
    } else if summary.max_drawdown_pct < Decimal::new(-25, 0)
        || worst_window_pnl_pct < Decimal::new(-15, 0)
    {
        CrossAssetPortfolioStatus::Weak
    } else {
        CrossAssetPortfolioStatus::Promising
    }
}

fn basket_return_pct(
    symbols: &[String],
    features: &BTreeMap<String, Vec<CrossAssetFeatureRow>>,
    index: usize,
    lookback: usize,
) -> Option<Decimal> {
    let returns = symbols
        .iter()
        .map(|symbol| {
            let feature = features.get(symbol).and_then(|rows| rows.get(index))?;
            match lookback {
                24 => feature.return_24h,
                72 => feature.return_72h,
                _ => feature.lookback_return,
            }
        })
        .collect::<Option<Vec<_>>>()?;
    Some(percent(
        returns.iter().copied().sum::<Decimal>() / Decimal::from(returns.len() as i64),
    ))
}

fn basket_vol_decimal(
    symbols: &[String],
    features: &BTreeMap<String, Vec<CrossAssetFeatureRow>>,
    index: usize,
) -> Option<Decimal> {
    let vols = symbols
        .iter()
        .map(|symbol| {
            features
                .get(symbol)
                .and_then(|rows| rows.get(index))
                .and_then(|feature| feature.vol_72h)
        })
        .collect::<Option<Vec<_>>>()?;
    Some(vols.iter().copied().sum::<Decimal>() / Decimal::from(vols.len() as i64))
}

fn basket_vol_pct(
    symbols: &[String],
    features: &BTreeMap<String, Vec<CrossAssetFeatureRow>>,
    index: usize,
) -> Option<Decimal> {
    basket_vol_decimal(symbols, features, index).map(percent)
}

fn basket_vol_percentile(
    symbols: &[String],
    features: &BTreeMap<String, Vec<CrossAssetFeatureRow>>,
    percentile_value: Decimal,
) -> Option<Decimal> {
    let len = features.values().next().map(|rows| rows.len()).unwrap_or(0);
    percentile_decimal(
        (0..len)
            .filter_map(|index| basket_vol_decimal(symbols, features, index))
            .collect(),
        percentile_value,
    )
}

fn median_asset_vol_24h(
    symbols: &[String],
    features: &BTreeMap<String, Vec<CrossAssetFeatureRow>>,
) -> Option<Decimal> {
    let mut values = Vec::new();
    for symbol in symbols {
        if let Some(rows) = features.get(symbol) {
            values.extend(rows.iter().filter_map(|row| row.vol_24h));
        }
    }
    percentile_decimal(values, Decimal::new(50, 0))
}

fn symbol_distribution(trades: &[CrossAssetPortfolioTrade]) -> BTreeMap<String, i32> {
    let mut distribution = BTreeMap::new();
    for trade in trades {
        *distribution.entry(trade.symbol.clone()).or_insert(0) += 1;
    }
    distribution
}

fn max_symbol_concentration_pct(
    distribution: &BTreeMap<String, i32>,
    total_trades: usize,
) -> Decimal {
    if total_trades == 0 {
        return Decimal::ZERO;
    }
    Decimal::from(*distribution.values().max().unwrap_or(&0) as i64) * Decimal::new(100, 0)
        / Decimal::from(total_trades as i64)
}

fn has_weak_2025_plus(trades: &[CrossAssetPortfolioTrade]) -> bool {
    let split = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let early = trades
        .iter()
        .filter(|trade| trade.entry_time < split)
        .cloned()
        .collect::<Vec<_>>();
    let late = trades
        .iter()
        .filter(|trade| trade.entry_time >= split)
        .cloned()
        .collect::<Vec<_>>();
    let early_summary = cross_asset_metric_summary(&early);
    let late_summary = cross_asset_metric_summary(&late);
    early_summary.compounded_pnl_pct > Decimal::new(20, 0)
        && late_summary.compounded_pnl_pct < Decimal::new(5, 0)
}

pub fn build_cross_asset_robustness_configs(
    request: &CrossAssetRobustnessMatrixRequest,
) -> Result<(Vec<CrossAssetRobustnessConfig>, usize), CoreError> {
    request.validate()?;
    let full_count = request.full_config_count();
    let mut configs = Vec::with_capacity(full_count);
    for ranking_lookback_hours in &request.ranking_lookback_hours {
        for rank_metric in &request.rank_metrics {
            for min_rank_spread_pct in &request.min_rank_spread_pct {
                for holding_hours in &request.holding_hours {
                    for sizing_mode in &request.sizing_modes {
                        for market_filter in &request.market_filters {
                            for vol_filter in &request.vol_filters {
                                for overextension_filter in &request.overextension_filters {
                                    for exit_rule in &request.exit_rules {
                                        configs.push(CrossAssetRobustnessConfig {
                                            ranking_lookback_hours: *ranking_lookback_hours,
                                            rank_metric: *rank_metric,
                                            min_rank_spread_pct: *min_rank_spread_pct,
                                            holding_hours: *holding_hours,
                                            sizing_mode: *sizing_mode,
                                            market_filter: market_filter.clone(),
                                            vol_filter: vol_filter.clone(),
                                            overextension_filter: overextension_filter.clone(),
                                            exit_rule: exit_rule.clone(),
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
    configs.sort_by(|left, right| {
        cross_asset_robustness_config_distance(left)
            .cmp(&cross_asset_robustness_config_distance(right))
            .then_with(|| {
                serde_json::to_string(left)
                    .unwrap_or_default()
                    .cmp(&serde_json::to_string(right).unwrap_or_default())
            })
    });
    let skipped = full_count.saturating_sub(request.max_configs);
    configs.truncate(request.max_configs);
    Ok((configs, skipped))
}

fn cross_asset_robustness_config_distance(config: &CrossAssetRobustnessConfig) -> i32 {
    let mut distance = 0i32;
    distance += config.ranking_lookback_hours.abs_diff(12) as i32;
    distance += config.holding_hours.abs_diff(12) as i32;
    distance += (config.min_rank_spread_pct - Decimal::new(3, 0))
        .abs()
        .to_i32()
        .unwrap_or(50);
    distance += match config.rank_metric {
        CrossAssetRankMetric::RawReturn => 0,
        CrossAssetRankMetric::VolAdjustedReturn => 2,
    };
    distance += match config.sizing_mode {
        CrossAssetSizingMode::EqualNotional => 0,
        CrossAssetSizingMode::VolatilityNormalized => 1,
    };
    distance += match &config.market_filter {
        CrossAssetMarketFilter::None => 0,
        CrossAssetMarketFilter::Basket72hReturnGt { .. }
        | CrossAssetMarketFilter::AtLeastNSymbolsPositive24h { .. } => 1,
        CrossAssetMarketFilter::Basket24hReturnGt { .. } => 3,
    };
    distance += match &config.vol_filter {
        CrossAssetVolFilter::None => 0,
        CrossAssetVolFilter::AssetNotExtremeVsBasket { .. } => 1,
        CrossAssetVolFilter::BasketVolBelowPercentile { .. } => 3,
    };
    distance += match &config.overextension_filter {
        CrossAssetOverextensionFilter::None => 0,
        CrossAssetOverextensionFilter::MaxReturn24hPct { max_pct } => {
            if *max_pct == Decimal::new(10, 0) {
                1
            } else {
                2
            }
        }
        CrossAssetOverextensionFilter::MinDistanceFrom72hHighPct { .. } => 3,
    };
    distance += match &config.exit_rule {
        CrossAssetExitRule::FixedHold => 0,
        CrossAssetExitRule::StopPct { stop_pct } if *stop_pct == Decimal::new(5, 0) => 1,
        CrossAssetExitRule::StopPct { .. } | CrossAssetExitRule::TakeProfitPct { .. } => 2,
    };
    distance
}

pub fn run_cross_asset_robustness_matrix(
    request: CrossAssetRobustnessMatrixRequest,
    candles_by_symbol: BTreeMap<String, Vec<Candle>>,
) -> Result<CrossAssetRobustnessMatrixRun, CoreError> {
    request.validate()?;
    let run_id = Uuid::new_v4();
    let correlation_id = request.correlation_id.unwrap_or_else(Uuid::new_v4);
    let created_at = Utc::now();
    let (configs, skipped_config_count) = build_cross_asset_robustness_configs(&request)?;
    let mut cells = Vec::with_capacity(configs.len() * request.windows.len());

    for (config_index, config) in configs.iter().enumerate() {
        for window in &request.windows {
            let cell_request =
                cross_asset_request_for_robustness(&request, config, window, correlation_id);
            let cell = match run_cross_asset_relative_strength_research(
                cell_request,
                candles_by_symbol.clone(),
            ) {
                Ok(run) => cross_asset_robustness_cell_from_run(
                    run_id,
                    config_index as i32,
                    config.clone(),
                    window,
                    &run,
                    created_at,
                ),
                Err(err) => empty_cross_asset_robustness_cell(
                    run_id,
                    config_index as i32,
                    config.clone(),
                    window,
                    CrossAssetRobustnessStatus::TooSparse,
                    vec![CrossAssetRobustnessFinding {
                        severity: "WARN".to_string(),
                        code: "cell_failed".to_string(),
                        message: err.to_string(),
                    }],
                    Some(err.to_string()),
                    created_at,
                ),
            };
            cells.push(cell);
        }
    }

    let rankings = rank_cross_asset_robustness_configs(&cells);
    let status = rankings
        .first()
        .map(|ranking| ranking.status)
        .unwrap_or(CrossAssetRobustnessStatus::TooSparse);
    let recommendation = cross_asset_robustness_recommendation(status);
    let findings = cross_asset_robustness_matrix_findings(&rankings, skipped_config_count);
    let recommendations =
        cross_asset_robustness_matrix_recommendations(status, &rankings, skipped_config_count);
    let result = CrossAssetRobustnessMatrixResult {
        run_id,
        request,
        status,
        recommendation,
        rankings,
        findings,
        recommendations,
        cell_count: cells.len() as i32,
        evaluated_config_count: configs.len() as i32,
        full_config_count: (configs.len() + skipped_config_count) as i32,
        skipped_config_count: skipped_config_count as i32,
        created_at,
        completed_at: Some(Utc::now()),
        correlation_id,
    };

    Ok(CrossAssetRobustnessMatrixRun { result, cells })
}

fn cross_asset_request_for_robustness(
    matrix: &CrossAssetRobustnessMatrixRequest,
    config: &CrossAssetRobustnessConfig,
    window: &CrossAssetRobustnessMatrixWindow,
    correlation_id: Uuid,
) -> CrossAssetResearchRequest {
    CrossAssetResearchRequest {
        strategy_kind: matrix.strategy_kind,
        symbols: matrix.symbols.clone(),
        timeframe: matrix.timeframe.clone(),
        start_time: window.start_time,
        end_time: window.end_time,
        ranking_lookback_hours: config.ranking_lookback_hours,
        rank_metric: config.rank_metric,
        min_top_return_pct: matrix.min_top_return_pct,
        min_rank_spread_pct: config.min_rank_spread_pct,
        holding_hours: config.holding_hours,
        fee_bps: matrix.fee_bps,
        slippage_bps: matrix.slippage_bps,
        one_active_position: matrix.one_active_position,
        sizing_mode: config.sizing_mode,
        min_weight: matrix.min_weight,
        max_weight: matrix.max_weight,
        market_filter: config.market_filter.clone(),
        vol_filter: config.vol_filter.clone(),
        overextension_filter: config.overextension_filter.clone(),
        exit_rule: config.exit_rule.clone(),
        correlation_id: Some(correlation_id),
    }
}

fn cross_asset_robustness_cell_from_run(
    matrix_run_id: Uuid,
    config_index: i32,
    config: CrossAssetRobustnessConfig,
    window: &CrossAssetRobustnessMatrixWindow,
    run: &CrossAssetResearchRun,
    created_at: DateTime<Utc>,
) -> CrossAssetRobustnessMatrixCell {
    let quarter_distribution = quarter_distribution(&run.trades);
    let max_quarter_concentration_pct =
        max_quarter_concentration_pct(&quarter_distribution, run.trades.len());
    let findings = cross_asset_robustness_cell_findings(
        &run.result,
        &quarter_distribution,
        max_quarter_concentration_pct,
    );
    let status = classify_cross_asset_robustness_cell(
        run.result.total_trades,
        run.result.compounded_pnl_pct,
        run.result.median_trade_pnl_pct,
        run.result.max_drawdown_pct,
        run.result.worst_window_pnl_pct,
        run.result.max_symbol_concentration_pct,
        max_quarter_concentration_pct,
    );
    CrossAssetRobustnessMatrixCell {
        id: Uuid::new_v4(),
        matrix_run_id,
        config_index,
        config,
        window_label: window.label.clone(),
        window_start: window.start_time,
        window_end: window.end_time,
        status,
        total_trades: run.result.total_trades,
        compounded_pnl_pct: run.result.compounded_pnl_pct,
        avg_trade_pnl_pct: run.result.avg_trade_pnl_pct,
        median_trade_pnl_pct: run.result.median_trade_pnl_pct,
        win_rate: run.result.win_rate,
        max_drawdown_pct: run.result.max_drawdown_pct,
        worst_trade_pct: run.result.worst_trade_pct,
        worst_window_pnl_pct: run.result.worst_window_pnl_pct,
        fee_slippage_drag_pct: run.result.fee_slippage_drag_pct,
        symbol_distribution: run.result.symbol_distribution.clone(),
        max_symbol_concentration_pct: run.result.max_symbol_concentration_pct,
        quarter_distribution,
        max_quarter_concentration_pct,
        findings,
        error: None,
        created_at,
    }
}

fn empty_cross_asset_robustness_cell(
    matrix_run_id: Uuid,
    config_index: i32,
    config: CrossAssetRobustnessConfig,
    window: &CrossAssetRobustnessMatrixWindow,
    status: CrossAssetRobustnessStatus,
    findings: Vec<CrossAssetRobustnessFinding>,
    error: Option<String>,
    created_at: DateTime<Utc>,
) -> CrossAssetRobustnessMatrixCell {
    CrossAssetRobustnessMatrixCell {
        id: Uuid::new_v4(),
        matrix_run_id,
        config_index,
        config,
        window_label: window.label.clone(),
        window_start: window.start_time,
        window_end: window.end_time,
        status,
        total_trades: 0,
        compounded_pnl_pct: Decimal::ZERO,
        avg_trade_pnl_pct: Decimal::ZERO,
        median_trade_pnl_pct: Decimal::ZERO,
        win_rate: Decimal::ZERO,
        max_drawdown_pct: Decimal::ZERO,
        worst_trade_pct: Decimal::ZERO,
        worst_window_pnl_pct: Decimal::ZERO,
        fee_slippage_drag_pct: Decimal::ZERO,
        symbol_distribution: BTreeMap::new(),
        max_symbol_concentration_pct: Decimal::ZERO,
        quarter_distribution: BTreeMap::new(),
        max_quarter_concentration_pct: Decimal::ZERO,
        findings,
        error,
        created_at,
    }
}

fn classify_cross_asset_robustness_cell(
    total_trades: i32,
    compounded_pnl_pct: Decimal,
    median_trade_pnl_pct: Decimal,
    max_drawdown_pct: Decimal,
    worst_window_pnl_pct: Decimal,
    max_symbol_concentration_pct: Decimal,
    max_quarter_concentration_pct: Decimal,
) -> CrossAssetRobustnessStatus {
    if total_trades < 50 {
        CrossAssetRobustnessStatus::TooSparse
    } else if compounded_pnl_pct <= Decimal::ZERO {
        CrossAssetRobustnessStatus::Negative
    } else if max_symbol_concentration_pct > Decimal::new(50, 0)
        || max_quarter_concentration_pct > Decimal::new(50, 0)
        || median_trade_pnl_pct <= Decimal::ZERO
    {
        CrossAssetRobustnessStatus::OverfitRisk
    } else if max_drawdown_pct < Decimal::new(-10, 0) || worst_window_pnl_pct < Decimal::new(-5, 0)
    {
        CrossAssetRobustnessStatus::Weak
    } else if total_trades >= 70 {
        CrossAssetRobustnessStatus::Robust
    } else {
        CrossAssetRobustnessStatus::Promising
    }
}

fn cross_asset_robustness_cell_findings(
    result: &CrossAssetResearchResult,
    quarter_distribution: &BTreeMap<String, i32>,
    max_quarter_concentration_pct: Decimal,
) -> Vec<CrossAssetRobustnessFinding> {
    let mut findings = Vec::new();
    if result.total_trades < 50 {
        findings.push(CrossAssetRobustnessFinding {
            severity: "WARN".to_string(),
            code: "too_sparse".to_string(),
            message: "Cell has fewer than 50 trades.".to_string(),
        });
    }
    if result.max_drawdown_pct < Decimal::new(-10, 0) {
        findings.push(CrossAssetRobustnessFinding {
            severity: "WARN".to_string(),
            code: "drawdown_gt_10_pct".to_string(),
            message: "Max drawdown exceeds 10%.".to_string(),
        });
    }
    if result.worst_window_pnl_pct < Decimal::new(-5, 0) {
        findings.push(CrossAssetRobustnessFinding {
            severity: "WARN".to_string(),
            code: "worst_window_below_minus_5_pct".to_string(),
            message: "Worst nested window is below -5%.".to_string(),
        });
    }
    if result.max_symbol_concentration_pct > Decimal::new(50, 0) {
        findings.push(CrossAssetRobustnessFinding {
            severity: "WARN".to_string(),
            code: "symbol_concentration_gt_50_pct".to_string(),
            message: "One symbol contributes more than 50% of trades.".to_string(),
        });
    }
    if result
        .symbol_distribution
        .get("BTCUSDT")
        .copied()
        .unwrap_or(0)
        == 0
    {
        findings.push(CrossAssetRobustnessFinding {
            severity: "INFO".to_string(),
            code: "btc_zero_participation".to_string(),
            message: "BTCUSDT produced zero selected trades in this cell.".to_string(),
        });
    }
    if max_quarter_concentration_pct > Decimal::new(50, 0) && quarter_distribution.len() > 1 {
        findings.push(CrossAssetRobustnessFinding {
            severity: "WARN".to_string(),
            code: "quarter_concentration_gt_50_pct".to_string(),
            message: "One quarter contributes more than 50% of trades.".to_string(),
        });
    }
    findings
}

pub fn rank_cross_asset_robustness_configs(
    cells: &[CrossAssetRobustnessMatrixCell],
) -> Vec<CrossAssetRobustnessRanking> {
    let mut by_config = BTreeMap::<i32, Vec<CrossAssetRobustnessMatrixCell>>::new();
    for cell in cells.iter().cloned() {
        by_config.entry(cell.config_index).or_default().push(cell);
    }
    let mut rankings = Vec::with_capacity(by_config.len());
    for (config_index, cells) in by_config {
        let config = cells
            .first()
            .expect("grouped config cells must be non-empty")
            .config
            .clone();
        let mut warnings = Vec::new();
        let mut findings = Vec::new();
        let total_trades = cells.iter().map(|cell| cell.total_trades).sum::<i32>();
        let combined = cells
            .iter()
            .find(|cell| cell.window_label == "combined")
            .or_else(|| cells.last())
            .expect("grouped config cells must be non-empty");
        let combined_pnl_pct = combined.compounded_pnl_pct;
        let median_cell_pnl_pct = median_decimal_vec(
            cells
                .iter()
                .map(|cell| cell.compounded_pnl_pct)
                .collect::<Vec<_>>(),
        );
        let worst_window_pnl_pct = cells
            .iter()
            .map(|cell| cell.worst_window_pnl_pct.min(cell.compounded_pnl_pct))
            .min()
            .unwrap_or(Decimal::ZERO);
        let max_drawdown_pct = cells
            .iter()
            .map(|cell| cell.max_drawdown_pct)
            .min()
            .unwrap_or(Decimal::ZERO);
        let max_symbol_concentration_pct = cells
            .iter()
            .map(|cell| cell.max_symbol_concentration_pct)
            .max()
            .unwrap_or(Decimal::ZERO);
        let btc_trade_count = cells
            .iter()
            .map(|cell| {
                cell.symbol_distribution
                    .get("BTCUSDT")
                    .copied()
                    .unwrap_or(0)
            })
            .sum::<i32>();
        let skipped_window_count = cells
            .iter()
            .filter(|cell| cell.status == CrossAssetRobustnessStatus::TooSparse)
            .count() as i32;
        let mut score = Decimal::new(50, 0);

        for cell in &cells {
            let is_late = cell.window_label.contains("2025") || cell.window_start.year() >= 2025;
            if cell.compounded_pnl_pct > Decimal::ZERO {
                score += if is_late {
                    Decimal::new(20, 0)
                } else {
                    Decimal::new(15, 0)
                };
            } else if is_late {
                score -= Decimal::new(35, 0);
                warnings.push("2025_plus_negative".to_string());
            } else {
                score -= Decimal::new(15, 0);
            }
            if is_late && cell.status == CrossAssetRobustnessStatus::TooSparse {
                score -= Decimal::new(30, 0);
                warnings.push("2025_plus_too_sparse".to_string());
            }
            if cell.max_drawdown_pct < Decimal::new(-10, 0) {
                score -= Decimal::new(20, 0);
                warnings.push("drawdown_gt_10_pct".to_string());
            } else {
                score += Decimal::new(8, 0);
            }
            if cell.worst_window_pnl_pct < Decimal::new(-5, 0)
                || cell.compounded_pnl_pct < Decimal::new(-5, 0)
            {
                score -= Decimal::new(15, 0);
                warnings.push("worst_window_below_minus_5_pct".to_string());
            }
            if cell.max_symbol_concentration_pct > Decimal::new(50, 0) {
                score -= Decimal::new(15, 0);
                warnings.push("symbol_concentration_gt_50_pct".to_string());
            } else {
                score += Decimal::new(8, 0);
            }
            if cell.total_trades < 30 {
                score -= Decimal::new(15, 0);
            } else if cell.total_trades < 50 {
                score -= Decimal::new(8, 0);
            } else {
                score += Decimal::new(10, 0);
            }
            score -= (cell.fee_slippage_drag_pct / Decimal::new(2, 0)).min(Decimal::new(10, 0));
            findings.extend(cell.findings.clone());
        }
        if btc_trade_count == 0 {
            score -= Decimal::new(5, 0);
            warnings.push("btc_zero_participation".to_string());
        }
        if median_cell_pnl_pct > Decimal::ZERO {
            score += Decimal::new(8, 0);
        }
        if one_window_dominates_pnl(&cells) {
            score -= Decimal::new(15, 0);
            warnings.push("one_window_dominates_pnl".to_string());
        }

        warnings.sort();
        warnings.dedup();
        findings.sort_by(|left, right| left.code.cmp(&right.code));
        findings.dedup_by(|left, right| left.code == right.code && left.message == right.message);
        let status = classify_cross_asset_robustness_ranking(
            &cells,
            score,
            combined_pnl_pct,
            median_cell_pnl_pct,
            worst_window_pnl_pct,
            max_drawdown_pct,
            max_symbol_concentration_pct,
        );
        rankings.push(CrossAssetRobustnessRanking {
            rank: 0,
            config_index,
            config,
            status,
            robustness_score: score,
            total_trades,
            combined_pnl_pct,
            median_cell_pnl_pct,
            worst_window_pnl_pct,
            max_drawdown_pct,
            max_symbol_concentration_pct,
            btc_trade_count,
            skipped_window_count,
            warnings,
            findings,
        });
    }
    rankings.sort_by(|left, right| {
        right
            .robustness_score
            .cmp(&left.robustness_score)
            .then_with(|| left.config_index.cmp(&right.config_index))
    });
    for (index, ranking) in rankings.iter_mut().enumerate() {
        ranking.rank = index as i32 + 1;
    }
    rankings
}

fn classify_cross_asset_robustness_ranking(
    cells: &[CrossAssetRobustnessMatrixCell],
    score: Decimal,
    combined_pnl_pct: Decimal,
    median_cell_pnl_pct: Decimal,
    worst_window_pnl_pct: Decimal,
    max_drawdown_pct: Decimal,
    max_symbol_concentration_pct: Decimal,
) -> CrossAssetRobustnessStatus {
    let late = cells
        .iter()
        .find(|cell| cell.window_label.contains("2025") || cell.window_start.year() >= 2025);
    if late.is_some_and(|cell| cell.status == CrossAssetRobustnessStatus::TooSparse) {
        return CrossAssetRobustnessStatus::TooSparse;
    }
    if combined_pnl_pct <= Decimal::ZERO
        || late.is_some_and(|cell| cell.compounded_pnl_pct <= Decimal::ZERO)
    {
        return CrossAssetRobustnessStatus::Negative;
    }
    if max_symbol_concentration_pct > Decimal::new(50, 0)
        || median_cell_pnl_pct <= Decimal::ZERO
        || one_window_dominates_pnl(cells)
    {
        return CrossAssetRobustnessStatus::OverfitRisk;
    }
    if max_drawdown_pct < Decimal::new(-10, 0) || worst_window_pnl_pct < Decimal::new(-5, 0) {
        return CrossAssetRobustnessStatus::Weak;
    }
    if score >= Decimal::new(115, 0) {
        CrossAssetRobustnessStatus::Robust
    } else {
        CrossAssetRobustnessStatus::Promising
    }
}

fn cross_asset_robustness_recommendation(
    status: CrossAssetRobustnessStatus,
) -> CrossAssetRobustnessRecommendation {
    match status {
        CrossAssetRobustnessStatus::Robust => {
            CrossAssetRobustnessRecommendation::ImplementResearchOnly
        }
        CrossAssetRobustnessStatus::Promising
        | CrossAssetRobustnessStatus::Weak
        | CrossAssetRobustnessStatus::OverfitRisk
        | CrossAssetRobustnessStatus::TooSparse => {
            CrossAssetRobustnessRecommendation::KeepResearchOnly
        }
        CrossAssetRobustnessStatus::Negative => CrossAssetRobustnessRecommendation::RejectFamily,
    }
}

fn cross_asset_robustness_matrix_findings(
    rankings: &[CrossAssetRobustnessRanking],
    skipped_config_count: usize,
) -> Vec<CrossAssetRobustnessFinding> {
    let mut findings = Vec::new();
    if skipped_config_count > 0 {
        findings.push(CrossAssetRobustnessFinding {
            severity: "INFO".to_string(),
            code: "grid_capped".to_string(),
            message: format!("{skipped_config_count} configs skipped by deterministic cap."),
        });
    }
    if rankings
        .first()
        .is_some_and(|ranking| ranking.status == CrossAssetRobustnessStatus::TooSparse)
    {
        findings.push(CrossAssetRobustnessFinding {
            severity: "WARN".to_string(),
            code: "top_config_too_sparse".to_string(),
            message: "Top-ranked config is still too sparse.".to_string(),
        });
    }
    if rankings
        .first()
        .is_some_and(|ranking| ranking.btc_trade_count == 0)
    {
        findings.push(CrossAssetRobustnessFinding {
            severity: "INFO".to_string(),
            code: "top_config_btc_zero_participation".to_string(),
            message: "Top-ranked config has zero BTCUSDT participation.".to_string(),
        });
    }
    findings
}

fn cross_asset_robustness_matrix_recommendations(
    status: CrossAssetRobustnessStatus,
    rankings: &[CrossAssetRobustnessRanking],
    skipped_config_count: usize,
) -> Vec<CrossAssetRobustnessRecommendationItem> {
    let mut recommendations = Vec::new();
    match status {
        CrossAssetRobustnessStatus::Robust => recommendations.push(
            CrossAssetRobustnessRecommendationItem {
                priority: "HIGH".to_string(),
                code: "implementation_research_only_review".to_string(),
                message: "Review deterministic implementation research only; do not create candidates automatically.".to_string(),
            },
        ),
        CrossAssetRobustnessStatus::Promising => recommendations.push(
            CrossAssetRobustnessRecommendationItem {
                priority: "MEDIUM".to_string(),
                code: "continue_parameter_stability_research".to_string(),
                message: "Evidence is promising but should remain research-only until stability blockers clear.".to_string(),
            },
        ),
        CrossAssetRobustnessStatus::Weak
        | CrossAssetRobustnessStatus::OverfitRisk
        | CrossAssetRobustnessStatus::TooSparse => recommendations.push(
            CrossAssetRobustnessRecommendationItem {
                priority: "HIGH".to_string(),
                code: "keep_research_only".to_string(),
                message: "Do not implement or promote; continue matrix and walk-forward breadth research.".to_string(),
            },
        ),
        CrossAssetRobustnessStatus::Negative => recommendations.push(
            CrossAssetRobustnessRecommendationItem {
                priority: "HIGH".to_string(),
                code: "reject_family".to_string(),
                message: "Reject this parameter family unless a materially new hypothesis is introduced.".to_string(),
            },
        ),
    }
    if skipped_config_count > 0 {
        recommendations.push(CrossAssetRobustnessRecommendationItem {
            priority: "LOW".to_string(),
            code: "expand_cap_if_needed".to_string(),
            message:
                "Increase max_configs only if the capped deterministic prefix is inconclusive."
                    .to_string(),
        });
    }
    if rankings.first().is_some_and(|ranking| {
        ranking
            .warnings
            .iter()
            .any(|warning| warning == "btc_zero_participation")
    }) {
        recommendations.push(CrossAssetRobustnessRecommendationItem {
            priority: "MEDIUM".to_string(),
            code: "investigate_btc_non_participation".to_string(),
            message: "BTC zero participation is a warning for cross-asset generalization."
                .to_string(),
        });
    }
    recommendations
}

fn one_window_dominates_pnl(cells: &[CrossAssetRobustnessMatrixCell]) -> bool {
    let positives = cells
        .iter()
        .filter(|cell| cell.window_label != "combined")
        .map(|cell| cell.compounded_pnl_pct.max(Decimal::ZERO))
        .collect::<Vec<_>>();
    let total_positive = positives.iter().copied().sum::<Decimal>();
    if total_positive <= Decimal::ZERO {
        return false;
    }
    positives
        .into_iter()
        .max()
        .is_some_and(|value| value / total_positive > Decimal::new(70, 2))
}

fn quarter_distribution(trades: &[CrossAssetPortfolioTrade]) -> BTreeMap<String, i32> {
    let mut distribution = BTreeMap::new();
    for trade in trades {
        let quarter = ((trade.entry_time.month0() / 3) + 1) as i32;
        let key = format!("{}-Q{quarter}", trade.entry_time.year());
        *distribution.entry(key).or_insert(0) += 1;
    }
    distribution
}

fn max_quarter_concentration_pct(
    distribution: &BTreeMap<String, i32>,
    total_trades: usize,
) -> Decimal {
    max_symbol_concentration_pct(distribution, total_trades)
}

fn percentile_decimal(mut values: Vec<Decimal>, percentile_value: Decimal) -> Option<Decimal> {
    if values.is_empty() {
        return None;
    }
    values.sort();
    let p = percentile_value.clamp(Decimal::ZERO, Decimal::new(100, 0));
    let rank = ((values.len() - 1) as f64) * (p.to_f64().unwrap_or(0.0) / 100.0);
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        return values.get(lower).copied();
    }
    let lower_weight = Decimal::from_f64(upper as f64 - rank)?;
    let upper_weight = Decimal::from_f64(rank - lower as f64)?;
    Some(values[lower] * lower_weight + values[upper] * upper_weight)
}

fn median_decimal_vec(values: Vec<Decimal>) -> Decimal {
    percentile_decimal(values, Decimal::new(50, 0)).unwrap_or(Decimal::ZERO)
}

fn percent(value: Decimal) -> Decimal {
    value * Decimal::new(100, 0)
}

#[cfg(test)]
mod cross_asset_research_tests {
    use super::*;

    fn ts(hour: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap() + Duration::hours(hour)
    }

    fn candle(symbol: &str, hour: i64, open: i64, high: i64, low: i64, close: i64) -> Candle {
        Candle {
            id: Uuid::new_v4(),
            exchange: MarketDataSource::Binance,
            symbol: Symbol::new(symbol).unwrap(),
            interval: CandleInterval::OneHour,
            open_time: ts(hour),
            close_time: ts(hour + 1),
            open: Decimal::from(open),
            high: Decimal::from(high),
            low: Decimal::from(low),
            close: Decimal::from(close),
            volume: Decimal::from(1000),
            quote_volume: Some(Decimal::from(1000)),
            trade_count: 100,
            is_closed: true,
            created_at: ts(hour + 1),
            updated_at: ts(hour + 1),
        }
    }

    fn candles(symbol: &str, slope: i64, volatility: i64) -> Vec<Candle> {
        (0..120)
            .map(|hour| {
                let base = 1000 + hour * slope;
                let wobble = if hour % 2 == 0 {
                    volatility
                } else {
                    -volatility
                };
                candle(
                    symbol,
                    hour,
                    base + wobble,
                    base + wobble + 5,
                    base + wobble - 5,
                    base + wobble + slope,
                )
            })
            .collect()
    }

    fn request() -> CrossAssetResearchRequest {
        CrossAssetResearchRequest {
            strategy_kind: CrossAssetStrategyKind::RelativeStrengthContinuationV1Research,
            symbols: vec!["AAAUSDT".to_string(), "BBBUSDT".to_string()],
            timeframe: "1h".to_string(),
            start_time: ts(0),
            end_time: ts(120),
            ranking_lookback_hours: 6,
            rank_metric: CrossAssetRankMetric::RawReturn,
            min_top_return_pct: Decimal::ZERO,
            min_rank_spread_pct: Decimal::ZERO,
            holding_hours: 12,
            fee_bps: Decimal::ZERO,
            slippage_bps: Decimal::ZERO,
            one_active_position: true,
            sizing_mode: CrossAssetSizingMode::EqualNotional,
            min_weight: Decimal::new(25, 2),
            max_weight: Decimal::ONE,
            market_filter: CrossAssetMarketFilter::None,
            vol_filter: CrossAssetVolFilter::None,
            overextension_filter: CrossAssetOverextensionFilter::None,
            exit_rule: CrossAssetExitRule::FixedHold,
            correlation_id: None,
        }
    }

    #[test]
    fn deterministic_ranking_tie_break_uses_symbol() {
        let mut by_symbol = BTreeMap::new();
        by_symbol.insert("AAAUSDT".to_string(), candles("AAAUSDT", 1, 0));
        by_symbol.insert("BBBUSDT".to_string(), candles("BBBUSDT", 1, 0));
        let run = run_cross_asset_relative_strength_research(request(), by_symbol).unwrap();
        assert_eq!(run.trades.first().unwrap().symbol, "AAAUSDT");
    }

    #[test]
    fn one_active_position_prevents_overlap() {
        let mut by_symbol = BTreeMap::new();
        by_symbol.insert("AAAUSDT".to_string(), candles("AAAUSDT", 3, 0));
        by_symbol.insert("BBBUSDT".to_string(), candles("BBBUSDT", 1, 0));
        let run = run_cross_asset_relative_strength_research(request(), by_symbol).unwrap();
        for pair in run.trades.windows(2) {
            assert!(pair[1].entry_time >= pair[0].exit_time + Duration::hours(1));
        }
    }

    #[test]
    fn volatility_normalized_sizing_is_bounded() {
        let mut by_symbol = BTreeMap::new();
        by_symbol.insert("AAAUSDT".to_string(), candles("AAAUSDT", 5, 30));
        by_symbol.insert("BBBUSDT".to_string(), candles("BBBUSDT", 1, 1));
        let mut req = request();
        req.sizing_mode = CrossAssetSizingMode::VolatilityNormalized;
        let run = run_cross_asset_relative_strength_research(req, by_symbol).unwrap();
        assert!(run
            .trades
            .iter()
            .all(|trade| trade.weight >= Decimal::new(25, 2) && trade.weight <= Decimal::ONE));
    }

    #[test]
    fn stop_and_take_profit_exits_trigger() {
        let mut stop_candles = candles("AAAUSDT", 2, 0);
        stop_candles[80].close = stop_candles[73].open * Decimal::new(96, 2);
        let mut by_symbol = BTreeMap::new();
        by_symbol.insert("AAAUSDT".to_string(), stop_candles);
        by_symbol.insert("BBBUSDT".to_string(), candles("BBBUSDT", 1, 0));
        let mut req = request();
        req.exit_rule = CrossAssetExitRule::StopPct {
            stop_pct: Decimal::new(3, 0),
        };
        let stop_run = run_cross_asset_relative_strength_research(req, by_symbol).unwrap();
        assert!(stop_run
            .trades
            .iter()
            .any(|trade| trade.exit_reason == "stop_pct"));

        let mut by_symbol = BTreeMap::new();
        by_symbol.insert("AAAUSDT".to_string(), candles("AAAUSDT", 5, 0));
        by_symbol.insert("BBBUSDT".to_string(), candles("BBBUSDT", 1, 0));
        let mut req = request();
        req.exit_rule = CrossAssetExitRule::TakeProfitPct {
            take_profit_pct: Decimal::new(3, 0),
        };
        let tp_run = run_cross_asset_relative_strength_research(req, by_symbol).unwrap();
        assert!(tp_run
            .trades
            .iter()
            .any(|trade| trade.exit_reason == "take_profit_pct"));
    }

    #[test]
    fn symbol_concentration_and_window_metrics_are_calculated() {
        let mut by_symbol = BTreeMap::new();
        by_symbol.insert("AAAUSDT".to_string(), candles("AAAUSDT", 3, 0));
        by_symbol.insert("BBBUSDT".to_string(), candles("BBBUSDT", 1, 0));
        let run = run_cross_asset_relative_strength_research(request(), by_symbol).unwrap();
        assert!(run.result.max_symbol_concentration_pct > Decimal::ZERO);
        assert!(run.result.window_count > 0);
        assert_eq!(run.result.total_trades, run.trades.len() as i32);
    }

    fn robustness_config() -> CrossAssetRobustnessConfig {
        CrossAssetRobustnessConfig {
            ranking_lookback_hours: 12,
            rank_metric: CrossAssetRankMetric::RawReturn,
            min_rank_spread_pct: Decimal::new(3, 0),
            holding_hours: 12,
            sizing_mode: CrossAssetSizingMode::EqualNotional,
            market_filter: CrossAssetMarketFilter::None,
            vol_filter: CrossAssetVolFilter::None,
            overextension_filter: CrossAssetOverextensionFilter::None,
            exit_rule: CrossAssetExitRule::FixedHold,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn robustness_cell(
        config_index: i32,
        label: &str,
        trades: i32,
        pnl: i64,
        dd: i64,
        concentration: i64,
        btc_trades: i32,
    ) -> CrossAssetRobustnessMatrixCell {
        let mut symbol_distribution = BTreeMap::new();
        symbol_distribution.insert("BTCUSDT".to_string(), btc_trades);
        symbol_distribution.insert("ETHUSDT".to_string(), trades.saturating_sub(btc_trades));
        CrossAssetRobustnessMatrixCell {
            id: Uuid::new_v4(),
            matrix_run_id: Uuid::new_v4(),
            config_index,
            config: robustness_config(),
            window_label: label.to_string(),
            window_start: if label.contains("2025") {
                Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap()
            } else {
                Utc.with_ymd_and_hms(2023, 3, 25, 0, 0, 0).unwrap()
            },
            window_end: Utc.with_ymd_and_hms(2026, 5, 29, 0, 0, 0).unwrap(),
            status: classify_cross_asset_robustness_cell(
                trades,
                Decimal::new(pnl, 0),
                Decimal::new(1, 1),
                Decimal::new(dd, 0),
                Decimal::new(pnl.min(0), 0),
                Decimal::new(concentration, 0),
                Decimal::new(30, 0),
            ),
            total_trades: trades,
            compounded_pnl_pct: Decimal::new(pnl, 0),
            avg_trade_pnl_pct: Decimal::new(1, 1),
            median_trade_pnl_pct: Decimal::new(1, 1),
            win_rate: Decimal::new(55, 0),
            max_drawdown_pct: Decimal::new(dd, 0),
            worst_trade_pct: Decimal::new(-2, 0),
            worst_window_pnl_pct: Decimal::new(pnl.min(0), 0),
            fee_slippage_drag_pct: Decimal::ZERO,
            symbol_distribution,
            max_symbol_concentration_pct: Decimal::new(concentration, 0),
            quarter_distribution: BTreeMap::from([("2024-Q1".to_string(), trades)]),
            max_quarter_concentration_pct: Decimal::new(30, 0),
            findings: Vec::new(),
            error: None,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn robust_config_beats_headline_return_but_concentrated_config() {
        let cells = vec![
            robustness_cell(0, "2023_2024", 60, 4, -4, 45, 10),
            robustness_cell(0, "2025_plus", 55, 3, -4, 45, 8),
            robustness_cell(0, "combined", 115, 7, -5, 45, 18),
            robustness_cell(1, "2023_2024", 80, 20, -4, 80, 0),
            robustness_cell(1, "2025_plus", 55, 1, -4, 80, 0),
            robustness_cell(1, "combined", 135, 25, -5, 80, 0),
        ];
        let rankings = rank_cross_asset_robustness_configs(&cells);
        assert_eq!(rankings[0].config_index, 0);
        assert_eq!(rankings[1].status, CrossAssetRobustnessStatus::OverfitRisk);
    }

    #[test]
    fn robustness_ranking_penalizes_2025_plus_sparse_cells() {
        let cells = vec![
            robustness_cell(0, "2023_2024", 60, 4, -4, 45, 6),
            robustness_cell(0, "2025_plus", 25, 2, -4, 45, 2),
            robustness_cell(0, "combined", 85, 6, -5, 45, 8),
        ];
        let rankings = rank_cross_asset_robustness_configs(&cells);
        assert_eq!(rankings[0].status, CrossAssetRobustnessStatus::TooSparse);
        assert!(rankings[0]
            .warnings
            .iter()
            .any(|warning| warning == "2025_plus_too_sparse"));
    }

    #[test]
    fn robustness_ranking_penalizes_symbol_concentration() {
        let concentrated = vec![
            robustness_cell(0, "2023_2024", 60, 6, -4, 80, 0),
            robustness_cell(0, "2025_plus", 60, 3, -4, 80, 0),
            robustness_cell(0, "combined", 120, 10, -5, 80, 0),
        ];
        let balanced = vec![
            robustness_cell(1, "2023_2024", 60, 4, -4, 45, 10),
            robustness_cell(1, "2025_plus", 60, 3, -4, 45, 10),
            robustness_cell(1, "combined", 120, 7, -5, 45, 20),
        ];
        let cells = concentrated.into_iter().chain(balanced).collect::<Vec<_>>();
        let rankings = rank_cross_asset_robustness_configs(&cells);
        assert_eq!(rankings[0].config_index, 1);
        assert_eq!(rankings[1].status, CrossAssetRobustnessStatus::OverfitRisk);
    }

    #[test]
    fn robustness_ranking_penalizes_drawdown() {
        let cells = vec![
            robustness_cell(0, "2023_2024", 60, 4, -12, 45, 8),
            robustness_cell(0, "2025_plus", 60, 3, -4, 45, 8),
            robustness_cell(0, "combined", 120, 7, -12, 45, 16),
        ];
        let rankings = rank_cross_asset_robustness_configs(&cells);
        assert_eq!(rankings[0].status, CrossAssetRobustnessStatus::Weak);
        assert!(rankings[0]
            .warnings
            .iter()
            .any(|warning| warning == "drawdown_gt_10_pct"));
    }

    #[test]
    fn robustness_ranking_order_is_deterministic_for_ties() {
        let cells = vec![
            robustness_cell(2, "2023_2024", 60, 4, -4, 45, 8),
            robustness_cell(1, "2023_2024", 60, 4, -4, 45, 8),
        ];
        let rankings = rank_cross_asset_robustness_configs(&cells);
        assert_eq!(rankings[0].config_index, 1);
        assert_eq!(rankings[1].config_index, 2);
    }

    #[test]
    fn robustness_grid_cap_skipped_count_is_deterministic() {
        let mut request = CrossAssetRobustnessMatrixRequest {
            strategy_kind: CrossAssetStrategyKind::RelativeStrengthContinuationV1Research,
            symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
            timeframe: "1h".to_string(),
            windows: vec![CrossAssetRobustnessMatrixWindow {
                label: "combined".to_string(),
                start_time: ts(0),
                end_time: ts(120),
            }],
            ranking_lookback_hours: vec![6, 12],
            rank_metrics: vec![
                CrossAssetRankMetric::RawReturn,
                CrossAssetRankMetric::VolAdjustedReturn,
            ],
            min_top_return_pct: Decimal::ZERO,
            min_rank_spread_pct: vec![Decimal::new(2, 0), Decimal::new(3, 0)],
            holding_hours: vec![6, 12],
            fee_bps: Decimal::ZERO,
            slippage_bps: Decimal::ZERO,
            one_active_position: true,
            sizing_modes: vec![CrossAssetSizingMode::EqualNotional],
            min_weight: Decimal::new(25, 2),
            max_weight: Decimal::ONE,
            market_filters: vec![CrossAssetMarketFilter::None],
            vol_filters: vec![CrossAssetVolFilter::None],
            overextension_filters: vec![CrossAssetOverextensionFilter::None],
            exit_rules: vec![CrossAssetExitRule::FixedHold],
            max_configs: 3,
            correlation_id: None,
        };
        let full_count = request.full_config_count();
        let (configs, skipped) = build_cross_asset_robustness_configs(&request).unwrap();
        assert_eq!(configs.len(), 3);
        assert_eq!(skipped, full_count - 3);
        request.max_configs = 3;
        let (again, again_skipped) = build_cross_asset_robustness_configs(&request).unwrap();
        assert_eq!(again, configs);
        assert_eq!(again_skipped, skipped);
    }

    #[test]
    fn relative_strength_v1_package_defaults_are_research_only() {
        let package = relative_strength_continuation_v1_package(ts(0), ts(120));
        assert_eq!(
            package.identity.strategy_id,
            RELATIVE_STRENGTH_CONTINUATION_V1_ID
        );
        assert!(!package.identity.paper_executable);
        assert!(!package.identity.testnet_executable);
        assert!(!package.identity.live_executable);
        assert_eq!(package.default_config.timeframe, "4h");
        assert_eq!(
            package.default_config.market_filter,
            CrossAssetMarketFilter::Basket24hReturnGt {
                threshold_pct: Decimal::new(-2, 0)
            }
        );
        assert!(is_relative_strength_continuation_v1_default_request(
            &package.default_config
        ));
        assert!(package
            .forbidden_actions
            .iter()
            .any(|action| action == "no_testnet"));
    }

    #[test]
    fn relative_strength_v1_matrix_preset_contains_filtered_config() {
        let latest_end = Utc.with_ymd_and_hms(2026, 5, 29, 20, 0, 0).unwrap();
        let request = relative_strength_continuation_v1_default_robustness_matrix_request(
            relative_strength_continuation_v1_default_matrix_windows(latest_end),
            None,
        );
        assert_eq!(request.timeframe, "4h");
        assert_eq!(request.max_configs, 432);
        assert!(request.market_filters.iter().any(|filter| {
            *filter
                == CrossAssetMarketFilter::Basket24hReturnGt {
                    threshold_pct: Decimal::new(-2, 0),
                }
        }));
        let (configs, skipped) = build_cross_asset_robustness_configs(&request).unwrap();
        assert_eq!(skipped, 0);
        assert!(configs.iter().any(|config| {
            let ranking = CrossAssetRobustnessRanking {
                rank: 1,
                config_index: 0,
                config: config.clone(),
                status: CrossAssetRobustnessStatus::Robust,
                robustness_score: Decimal::ZERO,
                total_trades: 0,
                combined_pnl_pct: Decimal::ZERO,
                median_cell_pnl_pct: Decimal::ZERO,
                worst_window_pnl_pct: Decimal::ZERO,
                max_drawdown_pct: Decimal::ZERO,
                max_symbol_concentration_pct: Decimal::ZERO,
                btc_trade_count: 0,
                skipped_window_count: 0,
                warnings: Vec::new(),
                findings: Vec::new(),
            };
            is_relative_strength_continuation_v1_default_matrix_ranking(&ranking)
        }));
    }

    fn candidate_gate_run() -> CrossAssetResearchResult {
        let mut symbols = BTreeMap::new();
        symbols.insert("BTCUSDT".to_string(), 6);
        symbols.insert("ETHUSDT".to_string(), 29);
        symbols.insert("BNBUSDT".to_string(), 41);
        symbols.insert("SOLUSDT".to_string(), 50);
        CrossAssetResearchResult {
            run_id: Uuid::new_v4(),
            strategy_kind: CrossAssetStrategyKind::RelativeStrengthContinuationV1Research,
            request: request(),
            status: CrossAssetResearchStatus::Completed,
            portfolio_status: CrossAssetPortfolioStatus::Promising,
            recommendation: CrossAssetResearchRecommendation::ConsiderImplementationResearchOnly,
            total_trades: 126,
            net_pnl_pct: Decimal::new(248391952775, 10),
            compounded_pnl_pct: Decimal::new(248391952775, 10),
            avg_trade_pnl_pct: Decimal::new(1, 1),
            median_trade_pnl_pct: Decimal::new(1, 2),
            win_rate: Decimal::new(55, 0),
            max_drawdown_pct: Decimal::new(-92735574175, 10),
            worst_trade_pct: Decimal::new(-3, 0),
            best_trade_pct: Decimal::new(5, 0),
            fee_slippage_drag_pct: Decimal::new(15, 1),
            symbol_distribution: symbols,
            max_symbol_concentration_pct: Decimal::new(3968, 2),
            window_count: 2,
            profitable_windows: 2,
            worst_window_pnl_pct: Decimal::new(-33163181185, 10),
            median_window_pnl_pct: Decimal::new(10, 0),
            warnings: Vec::new(),
            created_at: Utc::now(),
            completed_at: Some(Utc::now()),
            correlation_id: Uuid::new_v4(),
        }
    }

    fn candidate_gate_matrix(
        cells: &[CrossAssetRobustnessMatrixCell],
    ) -> CrossAssetRobustnessMatrixResult {
        let rankings = rank_cross_asset_robustness_configs(cells);
        CrossAssetRobustnessMatrixResult {
            run_id: Uuid::new_v4(),
            request: relative_strength_continuation_v1_default_robustness_matrix_request(
                relative_strength_continuation_v1_default_matrix_windows(
                    Utc.with_ymd_and_hms(2026, 5, 29, 20, 0, 0).unwrap(),
                ),
                None,
            ),
            status: CrossAssetRobustnessStatus::Robust,
            recommendation: CrossAssetRobustnessRecommendation::ImplementResearchOnly,
            rankings,
            findings: Vec::new(),
            recommendations: Vec::new(),
            cell_count: cells.len() as i32,
            evaluated_config_count: 1,
            full_config_count: 1,
            skipped_config_count: 0,
            created_at: Utc::now(),
            completed_at: Some(Utc::now()),
            correlation_id: Uuid::new_v4(),
        }
    }

    fn candidate_gate_request(
        run: &CrossAssetResearchResult,
        matrix: &CrossAssetRobustnessMatrixResult,
    ) -> CrossAssetCandidateGatePreviewRequest {
        CrossAssetCandidateGatePreviewRequest {
            strategy_package_id: RELATIVE_STRENGTH_CONTINUATION_V1_ID.to_string(),
            run_id: Some(run.run_id),
            matrix_id: Some(matrix.run_id),
        }
    }

    fn preview_warning_codes(preview: &CrossAssetCandidateGatePreviewResult) -> Vec<String> {
        preview
            .warnings
            .iter()
            .map(|warning| warning.code.clone())
            .collect()
    }

    fn preview_blocker_codes(preview: &CrossAssetCandidateGatePreviewResult) -> Vec<String> {
        preview
            .blockers
            .iter()
            .map(|blocker| blocker.code.clone())
            .collect()
    }

    fn policy_request(
        run: &CrossAssetResearchResult,
        matrix: &CrossAssetRobustnessMatrixResult,
        strictness: CrossAssetCandidateCreationPolicyStrictness,
    ) -> CrossAssetCandidateCreationPolicyPreviewRequest {
        CrossAssetCandidateCreationPolicyPreviewRequest {
            package_id: RELATIVE_STRENGTH_CONTINUATION_V1_ID.to_string(),
            run_id: Some(run.run_id),
            matrix_id: Some(matrix.run_id),
            strictness,
        }
    }

    fn policy_preview(
        run: &CrossAssetResearchResult,
        matrix: &CrossAssetRobustnessMatrixResult,
        cells: &[CrossAssetRobustnessMatrixCell],
        strictness: CrossAssetCandidateCreationPolicyStrictness,
        existing_active_candidate_count: i64,
    ) -> CrossAssetCandidateCreationPolicyPreviewResult {
        let gate = preview_cross_asset_candidate_gate(
            candidate_gate_request(run, matrix),
            Some(run),
            Some(matrix),
            cells,
            existing_active_candidate_count,
            true,
            true,
        );
        preview_cross_asset_candidate_creation_policy(
            policy_request(run, matrix, strictness),
            &gate,
            Some(run),
            Some(matrix),
            cells,
            existing_active_candidate_count,
            false,
            true,
        )
    }

    fn policy_requirement_codes(
        preview: &CrossAssetCandidateCreationPolicyPreviewResult,
        status: CrossAssetCandidateGateCheckStatus,
    ) -> Vec<String> {
        preview
            .hard_requirements
            .iter()
            .chain(preview.review_requirements.iter())
            .filter(|requirement| requirement.status == status)
            .map(|requirement| requirement.code.clone())
            .collect()
    }

    #[test]
    fn candidate_gate_preview_treats_weak_2025_median_as_warning() {
        let run = candidate_gate_run();
        let mut cells = vec![
            robustness_cell(0, "2023_2024", 75, 12, -6, 40, 6),
            robustness_cell(0, "2025_plus", 51, 10, -4, 45, 0),
            robustness_cell(0, "combined", 126, 25, -9, 45, 6),
        ];
        cells[1].median_trade_pnl_pct = Decimal::ZERO;
        let matrix = candidate_gate_matrix(&cells);
        let preview = preview_cross_asset_candidate_gate(
            candidate_gate_request(&run, &matrix),
            Some(&run),
            Some(&matrix),
            &cells,
            0,
            true,
            true,
        );

        assert_eq!(
            preview.status,
            CrossAssetCandidateGatePreviewStatus::NearReady
        );
        assert!(preview.blockers.is_empty());
        assert!(preview_warning_codes(&preview)
            .contains(&"twenty_25_plus_median_trade_weak".to_string()));
    }

    #[test]
    fn cross_asset_candidate_dossier_keeps_execution_readiness_false() {
        let candidate_id = Uuid::new_v4();
        let dossier = build_cross_asset_research_candidate_dossier(
            candidate_id,
            RELATIVE_STRENGTH_CONTINUATION_V1_ID.to_string(),
            ResearchCandidateStatus::Discovered,
            "cross_asset_research".to_string(),
            "NONE".to_string(),
            Some(Uuid::new_v4()),
            Some(Uuid::new_v4()),
            None,
            None,
            CrossAssetCandidateCreationPolicyStrictness::Experimental,
            vec!["twenty_25_plus_median_trade_weak".to_string()],
            None,
            Utc::now(),
        );

        assert_eq!(dossier.candidate_id, candidate_id);
        assert!(dossier.readiness.candidate_review_ready);
        assert!(!dossier.readiness.shadow_ready);
        assert!(!dossier.readiness.paper_ready);
        assert!(!dossier.readiness.testnet_ready);
        assert!(!dossier.readiness.live_ready);
        assert_eq!(dossier.execution_authority, "NONE");
    }

    #[test]
    fn cross_asset_candidate_qualification_blocks_shadow_runner_support() {
        let qualification = evaluate_cross_asset_candidate_qualification(
            Uuid::new_v4(),
            RELATIVE_STRENGTH_CONTINUATION_V1_ID.to_string(),
            ResearchCandidateStatus::Discovered,
            "NONE",
            CrossAssetCandidateCreationPolicyStrictness::Experimental,
            &[],
            None,
            Utc::now(),
        );

        assert_eq!(
            qualification.status,
            CrossAssetCandidateReviewStatus::NeedsHumanReview
        );
        assert!(qualification
            .blockers
            .iter()
            .any(|blocker| blocker.code == "no_cross_asset_shadow_runner_support"));
        assert!(qualification
            .warnings
            .iter()
            .any(|warning| warning.code == "experimental_policy_strictness_used"));
        assert!(!qualification.readiness.shadow_ready);
    }

    fn shadow_4h_candles(symbol: &str, slope: i64) -> Vec<Candle> {
        (0..32)
            .map(|index| {
                let open_time = ts(index * 4);
                let base = 1000 + index * slope;
                Candle {
                    id: Uuid::new_v4(),
                    exchange: MarketDataSource::Binance,
                    symbol: Symbol::new(symbol).unwrap(),
                    interval: CandleInterval::FourHours,
                    open_time,
                    close_time: open_time + Duration::hours(4) - Duration::milliseconds(1),
                    open: Decimal::from(base),
                    high: Decimal::from(base + slope.abs() + 10),
                    low: Decimal::from(base - 10),
                    close: Decimal::from(base + slope),
                    volume: Decimal::from(1000),
                    quote_volume: Some(Decimal::from(1000)),
                    trade_count: 100,
                    is_closed: true,
                    created_at: open_time + Duration::hours(4),
                    updated_at: open_time + Duration::hours(4),
                }
            })
            .collect()
    }

    fn shadow_basket() -> BTreeMap<String, Vec<Candle>> {
        BTreeMap::from([
            ("BTCUSDT".to_string(), shadow_4h_candles("BTCUSDT", 1)),
            ("ETHUSDT".to_string(), shadow_4h_candles("ETHUSDT", 2)),
            ("SOLUSDT".to_string(), shadow_4h_candles("SOLUSDT", 8)),
            ("BNBUSDT".to_string(), shadow_4h_candles("BNBUSDT", 3)),
        ])
    }

    #[test]
    fn cross_asset_shadow_preview_is_read_only_and_deterministic() {
        let candidate_id = Uuid::new_v4();
        let first = evaluate_cross_asset_shadow_observation_preview(
            candidate_id,
            RELATIVE_STRENGTH_CONTINUATION_V1_ID.to_string(),
            ResearchCandidateStatus::Discovered,
            shadow_basket(),
            None,
            ts(200),
        );
        let second = evaluate_cross_asset_shadow_observation_preview(
            candidate_id,
            RELATIVE_STRENGTH_CONTINUATION_V1_ID.to_string(),
            ResearchCandidateStatus::Discovered,
            shadow_basket(),
            None,
            ts(200),
        );

        assert!(first.no_mutation);
        assert!(!first.would_create_observation);
        assert_eq!(first.decision, second.decision);
        assert_eq!(first.ranking_snapshot, second.ranking_snapshot);
        assert_eq!(first.selected_symbol, second.selected_symbol);
    }

    #[test]
    fn cross_asset_shadow_preview_skips_duplicate_latest_candle() {
        let candidate_id = Uuid::new_v4();
        let latest = shadow_basket()
            .values()
            .next()
            .and_then(|candles| candles.last())
            .map(|candle| candle.open_time)
            .unwrap();
        let preview = evaluate_cross_asset_shadow_observation_preview(
            candidate_id,
            RELATIVE_STRENGTH_CONTINUATION_V1_ID.to_string(),
            ResearchCandidateStatus::Discovered,
            shadow_basket(),
            Some(latest),
            ts(200),
        );

        assert_eq!(
            preview.decision,
            CrossAssetShadowObservationDecision::SkippedNoNewCandle
        );
        assert!(preview.no_mutation);
        assert!(preview.ranking_snapshot.is_empty());
    }

    #[test]
    fn cross_asset_accept_shadow_preview_blocks_without_mutation() {
        let preview = preview_cross_asset_accept_shadow(
            Uuid::new_v4(),
            RELATIVE_STRENGTH_CONTINUATION_V1_ID.to_string(),
            Utc::now(),
        );

        assert_eq!(
            preview.status,
            CrossAssetAcceptShadowPreviewStatus::NotApplicable
        );
        assert!(preview.no_mutation);
        assert!(preview
            .reasons
            .iter()
            .any(|reason| reason.code == "direct_execution_unsupported"));
        assert!(preview
            .reasons
            .iter()
            .any(|reason| reason.code == "no_observation_path"));
    }

    #[test]
    fn candidate_gate_preview_blocks_drawdown_above_10_pct() {
        let run = candidate_gate_run();
        let cells = vec![
            robustness_cell(0, "2023_2024", 75, 12, -11, 40, 6),
            robustness_cell(0, "2025_plus", 60, 10, -4, 45, 2),
            robustness_cell(0, "combined", 135, 25, -11, 45, 8),
        ];
        let matrix = candidate_gate_matrix(&cells);
        let preview = preview_cross_asset_candidate_gate(
            candidate_gate_request(&run, &matrix),
            Some(&run),
            Some(&matrix),
            &cells,
            0,
            true,
            true,
        );

        assert_eq!(
            preview.status,
            CrossAssetCandidateGatePreviewStatus::Blocked
        );
        assert!(preview_blocker_codes(&preview).contains(&"max_drawdown_above_10_pct".to_string()));
    }

    #[test]
    fn candidate_gate_preview_blocks_worst_window_below_minus_5_pct() {
        let run = candidate_gate_run();
        let cells = vec![
            robustness_cell(0, "2023_2024", 75, -6, -4, 40, 6),
            robustness_cell(0, "2025_plus", 60, 10, -4, 45, 2),
            robustness_cell(0, "combined", 135, 20, -6, 45, 8),
        ];
        let matrix = candidate_gate_matrix(&cells);
        let preview = preview_cross_asset_candidate_gate(
            candidate_gate_request(&run, &matrix),
            Some(&run),
            Some(&matrix),
            &cells,
            0,
            true,
            true,
        );

        assert_eq!(
            preview.status,
            CrossAssetCandidateGatePreviewStatus::Blocked
        );
        assert!(
            preview_blocker_codes(&preview).contains(&"worst_window_below_minus_5_pct".to_string())
        );
    }

    #[test]
    fn candidate_gate_preview_blocks_symbol_concentration_above_50_pct() {
        let run = candidate_gate_run();
        let cells = vec![
            robustness_cell(0, "2023_2024", 75, 12, -4, 55, 6),
            robustness_cell(0, "2025_plus", 60, 10, -4, 55, 2),
            robustness_cell(0, "combined", 135, 25, -6, 55, 8),
        ];
        let matrix = candidate_gate_matrix(&cells);
        let preview = preview_cross_asset_candidate_gate(
            candidate_gate_request(&run, &matrix),
            Some(&run),
            Some(&matrix),
            &cells,
            0,
            true,
            true,
        );

        assert_eq!(
            preview.status,
            CrossAssetCandidateGatePreviewStatus::Blocked
        );
        assert!(preview_blocker_codes(&preview)
            .contains(&"symbol_concentration_above_50_pct".to_string()));
    }

    #[test]
    fn candidate_gate_preview_warns_on_low_btc_participation() {
        let run = candidate_gate_run();
        let cells = vec![
            robustness_cell(0, "2023_2024", 75, 12, -4, 40, 3),
            robustness_cell(0, "2025_plus", 60, 10, -4, 45, 1),
            robustness_cell(0, "combined", 135, 25, -6, 45, 4),
        ];
        let matrix = candidate_gate_matrix(&cells);
        let preview = preview_cross_asset_candidate_gate(
            candidate_gate_request(&run, &matrix),
            Some(&run),
            Some(&matrix),
            &cells,
            0,
            true,
            true,
        );

        assert!(preview.blockers.is_empty());
        assert!(
            preview_warning_codes(&preview).contains(&"btc_participation_below_5_pct".to_string())
        );
    }

    #[test]
    fn candidate_gate_preview_blocks_missing_matrix() {
        let run = candidate_gate_run();
        let preview = preview_cross_asset_candidate_gate(
            CrossAssetCandidateGatePreviewRequest {
                strategy_package_id: RELATIVE_STRENGTH_CONTINUATION_V1_ID.to_string(),
                run_id: Some(run.run_id),
                matrix_id: None,
            },
            Some(&run),
            None,
            &[],
            0,
            true,
            true,
        );

        assert_eq!(
            preview.status,
            CrossAssetCandidateGatePreviewStatus::Blocked
        );
        assert!(preview_blocker_codes(&preview).contains(&"no_robustness_matrix".to_string()));
    }

    #[test]
    fn candidate_gate_preview_is_non_mutating_preview_only() {
        let run = candidate_gate_run();
        let cells = vec![
            robustness_cell(0, "2023_2024", 75, 12, -4, 40, 6),
            robustness_cell(0, "2025_plus", 60, 10, -4, 45, 2),
            robustness_cell(0, "combined", 135, 25, -6, 45, 8),
        ];
        let matrix = candidate_gate_matrix(&cells);
        let preview = preview_cross_asset_candidate_gate(
            candidate_gate_request(&run, &matrix),
            Some(&run),
            Some(&matrix),
            &cells,
            0,
            true,
            true,
        );

        assert!(preview.preview_only);
        assert!(preview
            .forbidden_actions
            .contains(&"no_auto_candidate_creation".to_string()));
        assert!(preview
            .checks
            .iter()
            .any(|check| check.code == "no_existing_candidate"
                && check.status == CrossAssetCandidateGateCheckStatus::Pass));
    }

    #[test]
    fn candidate_creation_policy_conservative_blocks_current_warning_shape() {
        let run = candidate_gate_run();
        let mut cells = vec![
            robustness_cell(0, "2023_2024", 75, 12, -4, 40, 3),
            robustness_cell(0, "2025_plus", 51, 10, -4, 45, 1),
            robustness_cell(0, "combined", 126, 25, -6, 45, 4),
        ];
        cells[1].median_trade_pnl_pct = Decimal::ZERO;
        let matrix = candidate_gate_matrix(&cells);
        let preview = policy_preview(
            &run,
            &matrix,
            &cells,
            CrossAssetCandidateCreationPolicyStrictness::Conservative,
            0,
        );

        assert_eq!(
            preview.status,
            CrossAssetCandidateCreationPolicyStatus::PolicyBlocked
        );
        let blocks = policy_requirement_codes(&preview, CrossAssetCandidateGateCheckStatus::Block);
        assert!(blocks.contains(&"twenty_25_plus_median_trade_positive".to_string()));
        assert!(blocks.contains(&"btc_participation_at_least_5_pct".to_string()));
        assert!(
            blocks.contains(&"twenty_25_plus_trade_count_comfortably_above_minimum".to_string())
        );
        assert!(preview.no_candidate_created);
    }

    #[test]
    fn candidate_creation_policy_balanced_needs_review_for_current_warning_shape() {
        let run = candidate_gate_run();
        let mut cells = vec![
            robustness_cell(0, "2023_2024", 75, 12, -4, 40, 3),
            robustness_cell(0, "2025_plus", 51, 10, -4, 45, 1),
            robustness_cell(0, "combined", 126, 25, -6, 45, 4),
        ];
        cells[1].median_trade_pnl_pct = Decimal::ZERO;
        let matrix = candidate_gate_matrix(&cells);
        let preview = policy_preview(
            &run,
            &matrix,
            &cells,
            CrossAssetCandidateCreationPolicyStrictness::Balanced,
            0,
        );

        assert_eq!(
            preview.status,
            CrossAssetCandidateCreationPolicyStatus::PolicyNeedsReview
        );
        assert!(
            policy_requirement_codes(&preview, CrossAssetCandidateGateCheckStatus::Block)
                .is_empty()
        );
        assert!(
            policy_requirement_codes(&preview, CrossAssetCandidateGateCheckStatus::Warn)
                .contains(&"btc_participation_at_least_5_pct".to_string())
        );
    }

    #[test]
    fn candidate_creation_policy_experimental_can_be_ready_with_review_warnings() {
        let run = candidate_gate_run();
        let mut cells = vec![
            robustness_cell(0, "2023_2024", 75, 12, -4, 40, 3),
            robustness_cell(0, "2025_plus", 51, 10, -4, 45, 1),
            robustness_cell(0, "combined", 126, 25, -6, 45, 4),
        ];
        cells[1].median_trade_pnl_pct = Decimal::ZERO;
        let matrix = candidate_gate_matrix(&cells);
        let preview = policy_preview(
            &run,
            &matrix,
            &cells,
            CrossAssetCandidateCreationPolicyStrictness::Experimental,
            0,
        );

        assert_eq!(
            preview.status,
            CrossAssetCandidateCreationPolicyStatus::PolicyReadyForManualCreate
        );
        assert!(preview.preview_only);
        assert!(preview
            .forbidden_actions
            .contains(&"no_auto_candidate_creation".to_string()));
    }

    #[test]
    fn candidate_creation_policy_hard_drawdown_failure_blocks_all_modes() {
        let run = candidate_gate_run();
        let cells = vec![
            robustness_cell(0, "2023_2024", 75, 12, -11, 40, 6),
            robustness_cell(0, "2025_plus", 65, 10, -4, 45, 4),
            robustness_cell(0, "combined", 140, 25, -11, 45, 10),
        ];
        let matrix = candidate_gate_matrix(&cells);

        for strictness in [
            CrossAssetCandidateCreationPolicyStrictness::Conservative,
            CrossAssetCandidateCreationPolicyStrictness::Balanced,
            CrossAssetCandidateCreationPolicyStrictness::Experimental,
        ] {
            let preview = policy_preview(&run, &matrix, &cells, strictness, 0);
            assert_eq!(
                preview.status,
                CrossAssetCandidateCreationPolicyStatus::PolicyBlocked
            );
            assert!(
                policy_requirement_codes(&preview, CrossAssetCandidateGateCheckStatus::Block)
                    .contains(&"package_gate_preview_has_no_blockers".to_string())
            );
        }
    }

    #[test]
    fn candidate_creation_policy_hard_worst_window_failure_blocks_all_modes() {
        let run = candidate_gate_run();
        let cells = vec![
            robustness_cell(0, "2023_2024", 75, -6, -4, 40, 6),
            robustness_cell(0, "2025_plus", 65, 10, -4, 45, 4),
            robustness_cell(0, "combined", 140, 20, -6, 45, 10),
        ];
        let matrix = candidate_gate_matrix(&cells);

        for strictness in [
            CrossAssetCandidateCreationPolicyStrictness::Conservative,
            CrossAssetCandidateCreationPolicyStrictness::Balanced,
            CrossAssetCandidateCreationPolicyStrictness::Experimental,
        ] {
            let preview = policy_preview(&run, &matrix, &cells, strictness, 0);
            assert_eq!(
                preview.status,
                CrossAssetCandidateCreationPolicyStatus::PolicyBlocked
            );
            assert!(
                policy_requirement_codes(&preview, CrossAssetCandidateGateCheckStatus::Block)
                    .contains(&"worst_window_gte_minus_5_pct".to_string())
            );
        }
    }

    #[test]
    fn candidate_creation_policy_existing_active_candidate_blocks() {
        let run = candidate_gate_run();
        let cells = vec![
            robustness_cell(0, "2023_2024", 75, 12, -4, 40, 6),
            robustness_cell(0, "2025_plus", 65, 10, -4, 45, 4),
            robustness_cell(0, "combined", 140, 25, -6, 45, 10),
        ];
        let matrix = candidate_gate_matrix(&cells);
        let preview = policy_preview(
            &run,
            &matrix,
            &cells,
            CrossAssetCandidateCreationPolicyStrictness::Experimental,
            1,
        );

        assert_eq!(
            preview.status,
            CrossAssetCandidateCreationPolicyStatus::PolicyBlocked
        );
        assert!(
            policy_requirement_codes(&preview, CrossAssetCandidateGateCheckStatus::Block)
                .contains(&"no_existing_active_candidate_for_same_package_config".to_string())
        );
    }

    #[test]
    fn candidate_creation_policy_no_matrix_blocks() {
        let run = candidate_gate_run();
        let gate = preview_cross_asset_candidate_gate(
            CrossAssetCandidateGatePreviewRequest {
                strategy_package_id: RELATIVE_STRENGTH_CONTINUATION_V1_ID.to_string(),
                run_id: Some(run.run_id),
                matrix_id: None,
            },
            Some(&run),
            None,
            &[],
            0,
            true,
            true,
        );
        let preview = preview_cross_asset_candidate_creation_policy(
            CrossAssetCandidateCreationPolicyPreviewRequest {
                package_id: RELATIVE_STRENGTH_CONTINUATION_V1_ID.to_string(),
                run_id: Some(run.run_id),
                matrix_id: None,
                strictness: CrossAssetCandidateCreationPolicyStrictness::Experimental,
            },
            &gate,
            Some(&run),
            None,
            &[],
            0,
            false,
            true,
        );

        assert_eq!(
            preview.status,
            CrossAssetCandidateCreationPolicyStatus::PolicyBlocked
        );
        assert!(
            policy_requirement_codes(&preview, CrossAssetCandidateGateCheckStatus::Block)
                .contains(&"matrix_present".to_string())
        );
        assert!(preview.no_candidate_created);
    }
}

#[cfg(test)]
mod research_candidate_tests {
    use super::*;

    fn base_evidence() -> StrategyResearchCandidateEvidence {
        StrategyResearchCandidateEvidence {
            experiment_id: None,
            experiment_run_id: None,
            walk_forward_id: Some(Uuid::new_v4()),
            pnl_pct: Some(Decimal::new(12, 0)),
            max_drawdown_pct: Some(Decimal::new(4, 0)),
            win_rate: Some(Decimal::new(55, 2)),
            trade_count: Some(12),
            fee_paid: Some(Decimal::new(1, 0)),
            slippage_cost: Some(Decimal::new(1, 0)),
            robustness_score: Some(Decimal::new(80, 2)),
            profitable_windows: Some(4),
            losing_windows: Some(1),
            skipped_windows: Some(0),
            notes: None,
        }
    }

    #[test]
    fn scoring_penalizes_high_drawdown() {
        let low = score_strategy_research_candidate(&base_evidence()).score;
        let mut degraded = base_evidence();
        degraded.max_drawdown_pct = Some(Decimal::new(20, 0));
        let high = score_strategy_research_candidate(&degraded);

        assert!(high.score < low);
        assert!(high.warnings.contains(&"high_drawdown_penalty".to_string()));
    }

    #[test]
    fn scoring_penalizes_fee_drag() {
        let low_drag = score_strategy_research_candidate(&base_evidence()).score;
        let mut high_drag = base_evidence();
        high_drag.fee_paid = Some(Decimal::new(25, 0));
        high_drag.slippage_cost = Some(Decimal::new(20, 0));
        let result = score_strategy_research_candidate(&high_drag);

        assert!(result.score < low_drag);
        assert!(result
            .warnings
            .contains(&"fee_slippage_drag_penalty".to_string()));
    }

    #[test]
    fn scoring_penalizes_skipped_windows() {
        let baseline = score_strategy_research_candidate(&base_evidence()).score;
        let mut skipped = base_evidence();
        skipped.skipped_windows = Some(3);
        let result = score_strategy_research_candidate(&skipped);

        assert!(result.score < baseline);
        assert!(result
            .warnings
            .contains(&"skipped_windows_penalty".to_string()));
    }

    #[test]
    fn one_window_evidence_warns() {
        let mut single_window = base_evidence();
        single_window.profitable_windows = Some(1);
        single_window.losing_windows = Some(0);

        let result = score_strategy_research_candidate(&single_window);

        assert!(result
            .warnings
            .contains(&"single_window_overfitting_warning".to_string()));
    }

    #[test]
    fn promotion_confirmation_requires_exact_match() {
        assert!(is_valid_strategy_research_promotion_confirmation(
            "momentum_v1",
            "PROMOTE STRATEGY MOMENTUM_V1"
        ));
        assert!(!is_valid_strategy_research_promotion_confirmation(
            "momentum_v1",
            "promote strategy momentum_v1"
        ));
    }
}

pub fn summarize_candle_coverage(
    interval: CandleInterval,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    now: DateTime<Utc>,
    required_coverage_pct: Decimal,
    actual_open_times: &[DateTime<Utc>],
) -> CandleCoverageSummary {
    let expected = expected_candle_open_times(interval, start_time, end_time, now);
    let expected_set = expected.iter().copied().collect::<BTreeSet<_>>();
    let actual_set = actual_open_times.iter().copied().collect::<BTreeSet<_>>();
    let actual = actual_set
        .iter()
        .filter(|open_time| expected_set.contains(open_time))
        .copied()
        .collect::<Vec<_>>();
    let expected_candles = i64::try_from(expected.len()).unwrap_or(i64::MAX);
    let actual_candles = i64::try_from(actual.len()).unwrap_or(i64::MAX);
    let coverage_pct = if expected_candles == 0 {
        Decimal::ZERO
    } else {
        (Decimal::from(actual_candles) * Decimal::new(100, 0) / Decimal::from(expected_candles))
            .round_dp(2)
    };
    let status = if expected_candles == 0 || actual_candles == 0 {
        ResearchDataReadinessStatus::Insufficient
    } else if coverage_pct >= required_coverage_pct {
        ResearchDataReadinessStatus::Ready
    } else {
        ResearchDataReadinessStatus::Degraded
    };

    CandleCoverageSummary {
        interval: interval.as_str().to_string(),
        expected_candles,
        actual_candles,
        coverage_pct,
        first_candle_at: actual.first().copied(),
        last_candle_at: actual.last().copied(),
        missing_ranges: detect_research_data_gaps(
            interval,
            start_time,
            end_time,
            now,
            actual_open_times,
        ),
        status,
    }
}

pub fn summarize_candle_coverage_from_candles(
    interval: CandleInterval,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    now: DateTime<Utc>,
    required_coverage_pct: Decimal,
    candles: &[Candle],
) -> CandleCoverageSummary {
    let actual_open_times = candles
        .iter()
        .filter(|candle| candle.is_closed && candle.interval == interval)
        .map(|candle| candle.open_time)
        .collect::<Vec<_>>();
    summarize_candle_coverage(
        interval,
        start_time,
        end_time,
        now,
        required_coverage_pct,
        &actual_open_times,
    )
}

pub fn derive_research_data_readiness_status(
    summaries: &[CandleCoverageSummary],
) -> ResearchDataReadinessStatus {
    if summaries.is_empty() {
        return ResearchDataReadinessStatus::Insufficient;
    }
    if summaries
        .iter()
        .all(|summary| summary.status == ResearchDataReadinessStatus::Ready)
    {
        return ResearchDataReadinessStatus::Ready;
    }
    if summaries
        .iter()
        .any(|summary| summary.status == ResearchDataReadinessStatus::Insufficient)
    {
        return ResearchDataReadinessStatus::Insufficient;
    }
    ResearchDataReadinessStatus::Degraded
}

pub fn build_research_data_coverage_result(
    request: &ResearchDataCoverageRequest,
    summaries: Vec<CandleCoverageSummary>,
) -> ResearchDataCoverageResult {
    ResearchDataCoverageResult {
        exchange: request.exchange,
        symbol: request.symbol.trim().to_ascii_uppercase(),
        window_start: request.start_time,
        window_end: request.end_time,
        required_coverage_pct: request.required_coverage_pct,
        status: derive_research_data_readiness_status(&summaries),
        per_interval: summaries,
        correlation_id: request.correlation_id,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Timelike};

    use super::*;
    use crate::{aggregate_closed_1m_candles, Candle, MarketDataSource};

    fn ts(hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 24, hour, minute, second)
            .single()
            .unwrap()
    }

    fn shadow_pnl_candidate() -> ResearchCandidate {
        ResearchCandidate {
            id: Uuid::new_v4(),
            experiment_id: None,
            experiment_run_id: None,
            strategy_id: "baseline".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
            config: serde_json::json!({}),
            score: None,
            pnl_pct: None,
            max_drawdown_pct: None,
            trade_count: None,
            win_rate: None,
            fee_drag: None,
            status: ResearchCandidateStatus::PromotedToShadowConfig,
            rejection_reason: None,
            notes: None,
            created_at: ts(0, 0, 0),
            updated_at: ts(0, 0, 0),
            correlation_id: None,
        }
    }

    fn shadow_pnl_candle(minute: u32, open: i64, close: i64) -> Candle {
        Candle {
            id: Uuid::new_v4(),
            exchange: MarketDataSource::Binance,
            symbol: Symbol::new("BTCUSDT").unwrap(),
            interval: CandleInterval::OneMinute,
            open_time: ts(0, minute, 0),
            close_time: ts(0, minute, 59),
            open: Decimal::new(open, 0),
            high: Decimal::new(open.max(close), 0),
            low: Decimal::new(open.min(close), 0),
            close: Decimal::new(close, 0),
            volume: Decimal::ONE,
            quote_volume: None,
            trade_count: 1,
            is_closed: true,
            created_at: ts(0, minute, 59),
            updated_at: ts(0, minute, 59),
        }
    }

    fn shadow_pnl_result(
        prices: &[(u32, i64, i64)],
        windows: Vec<u32>,
        run_times: Vec<DateTime<Utc>>,
    ) -> ResearchShadowPnlAttributionResult {
        let candidate = shadow_pnl_candidate();
        let request = ResearchShadowPnlAttributionRequest {
            candidate_id: candidate.id,
            holding_windows: windows,
            fee_bps: Decimal::new(10, 0),
            slippage_bps: Decimal::new(5, 0),
            extreme_pnl_threshold_pct: Decimal::new(5, 0),
            start_time: None,
            end_time: None,
            limit: None,
        };
        let runs = run_times
            .into_iter()
            .map(|shadow_created_at| ResearchShadowPnlRunInput {
                shadow_run_id: Uuid::new_v4(),
                strategy_id: candidate.strategy_id.clone(),
                symbol: candidate.symbol.clone(),
                timeframe: candidate.timeframe.clone(),
                shadow_created_at,
                signal_time: None,
            })
            .collect::<Vec<_>>();
        let candles = prices
            .iter()
            .map(|(minute, open, close)| shadow_pnl_candle(*minute, *open, *close))
            .collect::<Vec<_>>();
        calculate_research_shadow_pnl_attribution(
            &candidate,
            &request,
            &runs,
            &candles,
            ts(1, 0, 0),
        )
    }

    #[test]
    fn shadow_pnl_entry_uses_next_candle_open_and_exit_uses_nth_close() {
        let result = shadow_pnl_result(
            &[(0, 100, 101), (1, 110, 111), (2, 120, 132)],
            vec![1],
            vec![ts(0, 0, 30)],
        );

        let trade = &result.trades[0];
        assert_eq!(trade.entry_candle_open_time, Some(ts(0, 1, 0)));
        assert_eq!(trade.entry_price, Some(Decimal::new(110, 0)));
        let window = &trade.holding_windows[0];
        assert_eq!(window.exit_candle_close_time, Some(ts(0, 2, 59)));
        assert_eq!(window.exit_price, Some(Decimal::new(132, 0)));
    }

    #[test]
    fn shadow_pnl_net_pnl_subtracts_fee_and_slippage() {
        let result =
            shadow_pnl_result(&[(1, 100, 100), (2, 100, 110)], vec![1], vec![ts(0, 0, 30)]);

        let window = &result.trades[0].holding_windows[0];
        assert_eq!(window.gross_pnl_pct, Some(Decimal::new(10, 0)));
        assert_eq!(window.net_pnl_pct, Some(Decimal::new(985, 2)));
        assert_eq!(window.fee_drag_pct, Decimal::new(15, 2));
    }

    #[test]
    fn shadow_pnl_formula_handles_positive_negative_and_flat() {
        let positive =
            shadow_pnl_result(&[(1, 100, 100), (2, 100, 101)], vec![1], vec![ts(0, 0, 30)]);
        assert_eq!(
            positive.trades[0].holding_windows[0].gross_pnl_pct,
            Some(Decimal::new(1, 0))
        );
        assert_eq!(
            positive.trades[0].holding_windows[0].net_pnl_pct,
            Some(Decimal::new(85, 2))
        );

        let negative =
            shadow_pnl_result(&[(1, 100, 100), (2, 100, 99)], vec![1], vec![ts(0, 0, 30)]);
        assert_eq!(
            negative.trades[0].holding_windows[0].gross_pnl_pct,
            Some(Decimal::new(-1, 0))
        );
        assert_eq!(
            negative.trades[0].holding_windows[0].net_pnl_pct,
            Some(Decimal::new(-115, 2))
        );

        let flat = shadow_pnl_result(&[(1, 100, 100), (2, 100, 100)], vec![1], vec![ts(0, 0, 30)]);
        assert_eq!(
            flat.trades[0].holding_windows[0].gross_pnl_pct,
            Some(Decimal::ZERO)
        );
        assert_eq!(
            flat.trades[0].holding_windows[0].net_pnl_pct,
            Some(Decimal::new(-15, 2))
        );
    }

    #[test]
    fn shadow_pnl_open_candles_are_ignored_for_entry() {
        let candidate = shadow_pnl_candidate();
        let request = ResearchShadowPnlAttributionRequest {
            candidate_id: candidate.id,
            holding_windows: vec![1],
            fee_bps: Decimal::ZERO,
            slippage_bps: Decimal::ZERO,
            extreme_pnl_threshold_pct: Decimal::new(100, 0),
            start_time: None,
            end_time: None,
            limit: None,
        };
        let runs = vec![ResearchShadowPnlRunInput {
            shadow_run_id: Uuid::new_v4(),
            strategy_id: candidate.strategy_id.clone(),
            symbol: candidate.symbol.clone(),
            timeframe: candidate.timeframe.clone(),
            shadow_created_at: ts(0, 0, 30),
            signal_time: None,
        }];
        let mut open_candle = shadow_pnl_candle(1, 100, 100);
        open_candle.is_closed = false;
        let candles = vec![
            open_candle,
            shadow_pnl_candle(2, 110, 110),
            shadow_pnl_candle(3, 110, 111),
        ];
        let result = calculate_research_shadow_pnl_attribution(
            &candidate,
            &request,
            &runs,
            &candles,
            ts(1, 0, 0),
        );

        assert_eq!(result.trades[0].entry_candle_open_time, Some(ts(0, 2, 0)));
        assert_eq!(result.trades[0].entry_price, Some(Decimal::new(110, 0)));
    }

    #[test]
    fn shadow_pnl_insufficient_forward_data_is_counted() {
        let result = shadow_pnl_result(&[(1, 100, 100)], vec![1], vec![ts(0, 0, 30)]);

        assert_eq!(
            result.trades[0].status,
            ResearchShadowPnlStatus::InsufficientForwardData
        );
        assert_eq!(result.summary.total_attributed_runs, 0);
        assert_eq!(result.summary.insufficient_forward_data_count, 1);
    }

    #[test]
    fn shadow_pnl_gap_detection_marks_window() {
        let result =
            shadow_pnl_result(&[(1, 100, 100), (4, 100, 101)], vec![1], vec![ts(0, 0, 30)]);

        let window = &result.trades[0].holding_windows[0];
        assert_eq!(window.status, ResearchShadowPnlStatus::GapDetected);
        assert_eq!(window.candle_gap_seconds, Some(180));
        assert_eq!(result.summary.gap_detected_count, 1);
    }

    #[test]
    fn shadow_pnl_extreme_pnl_is_flagged_not_discarded() {
        let result =
            shadow_pnl_result(&[(1, 100, 100), (2, 100, 106)], vec![1], vec![ts(0, 0, 30)]);

        let window = &result.trades[0].holding_windows[0];
        assert_eq!(window.status, ResearchShadowPnlStatus::ExtremePnl);
        assert_eq!(window.gross_pnl_pct, Some(Decimal::new(6, 0)));
        assert_eq!(window.net_pnl_pct, Some(Decimal::new(585, 2)));
        assert_eq!(result.summary.extreme_pnl_count, 1);
        assert!(window.warning.is_some());
    }

    #[test]
    fn shadow_pnl_summary_calculates_median_avg_and_win_rate() {
        let result = shadow_pnl_result(
            &[
                (1, 100, 100),
                (2, 100, 110),
                (3, 100, 90),
                (4, 100, 120),
                (5, 100, 100),
                (6, 100, 130),
            ],
            vec![1],
            vec![ts(0, 0, 30), ts(0, 1, 30), ts(0, 2, 30), ts(0, 3, 30)],
        );

        let summary = &result.summary.per_holding_window[0];
        assert_eq!(summary.trade_count, 4);
        assert_eq!(summary.win_rate, Decimal::new(50, 0));
        assert_eq!(summary.avg_net_pnl_pct.round_dp(2), Decimal::new(485, 2));
        assert_eq!(summary.median_net_pnl_pct, Decimal::new(485, 2));
        assert_eq!(summary.best_net_pnl_pct, Decimal::new(1985, 2));
        assert_eq!(summary.worst_net_pnl_pct, Decimal::new(-1015, 2));
    }

    #[test]
    fn shadow_pnl_negative_all_windows_and_promising_recommendations() {
        let negative = shadow_pnl_result(
            &[
                (1, 100, 100),
                (2, 100, 90),
                (3, 100, 85),
                (4, 100, 80),
                (5, 100, 75),
            ],
            vec![1],
            vec![ts(0, 0, 30), ts(0, 1, 30), ts(0, 2, 30)],
        );
        assert!(negative.summary.negative_all_windows);
        assert_eq!(
            negative.latest_shadow_pnl_status,
            ResearchShadowPnlRecommendation::Negative
        );

        let promising = shadow_pnl_result(
            &[
                (1, 100, 100),
                (2, 100, 110),
                (3, 100, 112),
                (4, 100, 114),
                (5, 100, 116),
            ],
            vec![1],
            vec![ts(0, 0, 30), ts(0, 1, 30), ts(0, 2, 30)],
        );
        assert_eq!(
            promising.summary.per_holding_window[0].recommendation,
            ResearchShadowPnlRecommendation::Promising
        );
    }

    #[test]
    fn expected_candle_count_supports_supported_intervals() {
        let now = ts(2, 0, 0);
        assert_eq!(
            expected_candle_open_times(CandleInterval::OneMinute, ts(0, 0, 0), ts(1, 0, 0), now)
                .len(),
            60
        );
        assert_eq!(
            expected_candle_open_times(CandleInterval::FiveMinutes, ts(0, 0, 0), ts(1, 0, 0), now)
                .len(),
            12
        );
        assert_eq!(
            expected_candle_open_times(
                CandleInterval::FifteenMinutes,
                ts(0, 0, 0),
                ts(1, 0, 0),
                now
            )
            .len(),
            4
        );
        assert_eq!(
            expected_candle_open_times(CandleInterval::OneHour, ts(0, 0, 0), ts(2, 0, 0), now)
                .len(),
            2
        );
    }

    #[test]
    fn full_coverage_returns_ready() {
        let now = ts(1, 0, 0);
        let actual =
            expected_candle_open_times(CandleInterval::OneMinute, ts(0, 0, 0), ts(0, 10, 0), now);
        let summary = summarize_candle_coverage(
            CandleInterval::OneMinute,
            ts(0, 0, 0),
            ts(0, 10, 0),
            now,
            Decimal::new(95, 0),
            &actual,
        );

        assert_eq!(summary.status, ResearchDataReadinessStatus::Ready);
        assert_eq!(summary.missing_ranges.len(), 0);
        assert_eq!(summary.expected_candles, 10);
        assert_eq!(summary.actual_candles, 10);
    }

    #[test]
    fn missing_middle_range_detected() {
        let now = ts(1, 0, 0);
        let mut actual =
            expected_candle_open_times(CandleInterval::OneMinute, ts(0, 0, 0), ts(0, 10, 0), now);
        actual.retain(|open_time| !matches!(open_time.minute(), 3 | 4 | 5));

        let gaps = detect_research_data_gaps(
            CandleInterval::OneMinute,
            ts(0, 0, 0),
            ts(0, 10, 0),
            now,
            &actual,
        );

        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].start_time, ts(0, 3, 0));
        assert_eq!(gaps[0].end_time, ts(0, 6, 0));
        assert_eq!(gaps[0].missing_candles, 3);
    }

    #[test]
    fn missing_leading_and_trailing_ranges_detected() {
        let now = ts(1, 0, 0);
        let mut actual =
            expected_candle_open_times(CandleInterval::FiveMinutes, ts(0, 0, 0), ts(1, 0, 0), now);
        actual.retain(|open_time| *open_time != ts(0, 0, 0) && *open_time != ts(0, 55, 0));

        let gaps = detect_research_data_gaps(
            CandleInterval::FiveMinutes,
            ts(0, 0, 0),
            ts(1, 0, 0),
            now,
            &actual,
        );

        assert_eq!(gaps.len(), 2);
        assert_eq!(gaps[0].start_time, ts(0, 0, 0));
        assert_eq!(gaps[0].end_time, ts(0, 5, 0));
        assert_eq!(gaps[1].start_time, ts(0, 55, 0));
        assert_eq!(gaps[1].end_time, ts(1, 0, 0));
    }

    #[test]
    fn compact_gap_ranges_generated() {
        let now = ts(1, 0, 0);
        let actual = vec![
            ts(0, 0, 0),
            ts(0, 1, 0),
            ts(0, 4, 0),
            ts(0, 7, 0),
            ts(0, 8, 0),
        ];
        let gaps = detect_research_data_gaps(
            CandleInterval::OneMinute,
            ts(0, 0, 0),
            ts(0, 10, 0),
            now,
            &actual,
        );

        assert_eq!(gaps.len(), 3);
        assert_eq!(gaps[0].missing_candles, 2);
        assert_eq!(gaps[1].missing_candles, 2);
        assert_eq!(gaps[2].missing_candles, 1);
    }

    #[test]
    fn incomplete_current_bucket_is_ignored() {
        let now = ts(0, 10, 30);
        let actual =
            expected_candle_open_times(CandleInterval::OneMinute, ts(0, 0, 0), ts(0, 11, 0), now);
        let summary = summarize_candle_coverage(
            CandleInterval::OneMinute,
            ts(0, 0, 0),
            ts(0, 11, 0),
            now,
            Decimal::new(95, 0),
            &actual,
        );

        assert_eq!(summary.expected_candles, 10);
        assert_eq!(summary.actual_candles, 10);
        assert_eq!(summary.status, ResearchDataReadinessStatus::Ready);
    }

    #[test]
    fn aggregation_step_is_idempotent() {
        let now = ts(0, 10, 0);
        let candles =
            expected_candle_open_times(CandleInterval::OneMinute, ts(0, 0, 0), ts(0, 10, 0), now)
                .into_iter()
                .map(|open_time| Candle {
                    id: Uuid::new_v4(),
                    exchange: MarketDataSource::Binance,
                    symbol: Symbol::new("BTCUSDT").unwrap(),
                    interval: CandleInterval::OneMinute,
                    open_time,
                    close_time: CandleInterval::OneMinute.bucket_close_time(open_time),
                    open: Decimal::ONE,
                    high: Decimal::ONE,
                    low: Decimal::ONE,
                    close: Decimal::ONE,
                    volume: Decimal::ONE,
                    quote_volume: Some(Decimal::ONE),
                    trade_count: 1,
                    is_closed: true,
                    created_at: now,
                    updated_at: now,
                })
                .collect::<Vec<_>>();

        let first = aggregate_closed_1m_candles(&candles, CandleInterval::FiveMinutes);
        let second = aggregate_closed_1m_candles(&candles, CandleInterval::FiveMinutes);

        assert_eq!(first, second);
    }

    fn base_observation_requirement() -> StrategyCandidateObservationRequirement {
        StrategyCandidateObservationRequirement {
            candidate_id: Uuid::from_u128(0xabc),
            strategy_id: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "15m".to_string(),
            min_observation_hours: 24,
            min_shadow_runs: 30,
            max_risk_rejection_rate: Some(Decimal::new(2, 1)),
            min_would_submit_count: 1,
            max_no_signal_rate: Some(Decimal::new(6, 1)),
            require_readiness_ready: true,
        }
    }

    fn aligned_runner_alignment() -> StrategyCandidateRunnerAlignment {
        StrategyCandidateRunnerAlignment {
            strategy_config_matches_runner: true,
            runner_enabled: true,
            runner_status: "RUNNING".to_string(),
            runner_timeframe: "15m".to_string(),
            runner_symbols: vec!["BTCUSDT".to_string()],
            runner_strategies: vec!["momentum_v1".to_string()],
            mismatch_reasons: Vec::new(),
        }
    }

    #[test]
    fn insufficient_hours_continue_observing() {
        let requirement = base_observation_requirement();
        let summary = evaluate_strategy_candidate_observation(
            &requirement,
            ts(0, 0, 0),
            ts(23, 0, 0),
            30,
            2,
            5,
            3,
            1,
            Some(ExecutionReadinessStatus::Ready),
            Some(92),
            aligned_runner_alignment(),
            ts(23, 0, 0),
        );

        assert_eq!(
            summary.decision,
            StrategyCandidateObservationDecision::ContinueObserving
        );
        assert!(summary
            .findings
            .iter()
            .any(|finding| finding.code == "not_enough_time_observed"));
    }

    #[test]
    fn insufficient_shadow_runs_yield_insufficient_data() {
        let requirement = base_observation_requirement();
        let summary = evaluate_strategy_candidate_observation(
            &requirement,
            ts(0, 0, 0),
            ts(0, 0, 0) + chrono::Duration::hours(24),
            12,
            2,
            2,
            1,
            0,
            Some(ExecutionReadinessStatus::Ready),
            Some(90),
            aligned_runner_alignment(),
            ts(0, 0, 0) + chrono::Duration::hours(24),
        );

        assert_eq!(
            summary.decision,
            StrategyCandidateObservationDecision::InsufficientData
        );
        assert!(summary
            .findings
            .iter()
            .any(|finding| finding.code == "not_enough_shadow_runs"));
    }

    #[test]
    fn zero_would_submit_fails_after_requirements_met() {
        let requirement = base_observation_requirement();
        let summary = evaluate_strategy_candidate_observation(
            &requirement,
            ts(0, 0, 0),
            ts(0, 0, 0) + chrono::Duration::hours(24),
            30,
            0,
            5,
            2,
            0,
            Some(ExecutionReadinessStatus::Ready),
            Some(90),
            aligned_runner_alignment(),
            ts(0, 0, 0) + chrono::Duration::hours(24),
        );

        assert_eq!(summary.decision, StrategyCandidateObservationDecision::Fail);
        assert!(summary
            .findings
            .iter()
            .any(|finding| finding.code == "zero_or_low_would_submit"));
    }

    #[test]
    fn readiness_not_ready_fails() {
        let requirement = base_observation_requirement();
        let summary = evaluate_strategy_candidate_observation(
            &requirement,
            ts(0, 0, 0),
            ts(0, 0, 0) + chrono::Duration::hours(24),
            30,
            2,
            5,
            2,
            0,
            Some(ExecutionReadinessStatus::Degraded),
            Some(70),
            aligned_runner_alignment(),
            ts(0, 0, 0) + chrono::Duration::hours(24),
        );

        assert_eq!(summary.decision, StrategyCandidateObservationDecision::Fail);
        assert!(summary
            .findings
            .iter()
            .any(|finding| finding.code == "readiness_not_ready"));
    }

    #[test]
    fn pass_when_requirements_met() {
        let requirement = base_observation_requirement();
        let summary = evaluate_strategy_candidate_observation(
            &requirement,
            ts(0, 0, 0),
            ts(0, 0, 0) + chrono::Duration::hours(24),
            30,
            4,
            6,
            3,
            1,
            Some(ExecutionReadinessStatus::Ready),
            Some(95),
            aligned_runner_alignment(),
            ts(0, 0, 0) + chrono::Duration::hours(24),
        );

        assert_eq!(summary.decision, StrategyCandidateObservationDecision::Pass);
        assert_eq!(summary.findings[0].code, "requirements_met");
    }

    #[test]
    fn no_signal_rate_calculation_is_deterministic() {
        assert_eq!(
            calculate_observation_rate(6, 30).round_dp(4),
            Decimal::new(2, 1).round_dp(4)
        );
    }

    #[test]
    fn risk_rejection_rate_calculation_is_deterministic() {
        let requirement = base_observation_requirement();
        let summary = evaluate_strategy_candidate_observation(
            &requirement,
            ts(0, 0, 0),
            ts(0, 0, 0) + chrono::Duration::hours(24),
            30,
            3,
            2,
            6,
            0,
            Some(ExecutionReadinessStatus::Ready),
            Some(95),
            aligned_runner_alignment(),
            ts(0, 0, 0) + chrono::Duration::hours(24),
        );

        assert_eq!(summary.risk_rejection_rate, Decimal::new(2, 1).round_dp(4));
    }

    #[test]
    fn shadow_performance_status_is_insufficient_without_runs() {
        let performance = evaluate_research_candidate_shadow_performance(
            Uuid::nil(),
            ResearchCandidateStatus::PromotedToShadowConfig,
            "momentum_v1",
            "BTCUSDT",
            "15m",
            ts(0, 0, 0),
            ts(0, 1, 0),
            0,
            0,
            0,
            0,
            0,
            0,
            None,
            0,
            0,
            0,
            0,
            None,
            None,
            None,
            true,
            ts(0, 1, 0),
        );

        assert_eq!(
            performance.status,
            ResearchCandidateShadowPerformanceStatus::InsufficientData
        );
        assert_eq!(
            performance.recommendation,
            ResearchCandidateShadowPerformanceRecommendation::InsufficientData
        );
    }

    #[test]
    fn shadow_performance_recommends_promotion_before_linked_runs() {
        let performance = evaluate_research_candidate_shadow_performance(
            Uuid::nil(),
            ResearchCandidateStatus::AcceptedForShadow,
            "momentum_v1",
            "BTCUSDT",
            "15m",
            ts(0, 0, 0),
            ts(0, 1, 0),
            0,
            0,
            0,
            0,
            0,
            0,
            None,
            0,
            0,
            0,
            0,
            None,
            None,
            None,
            true,
            ts(0, 1, 0),
        );

        assert_eq!(
            performance.status,
            ResearchCandidateShadowPerformanceStatus::NotPromotedToShadowConfig
        );
        assert_eq!(
            performance.recommendation,
            ResearchCandidateShadowPerformanceRecommendation::PromoteToShadowConfig
        );
    }

    #[test]
    fn would_submit_rate_pct_calculation_is_deterministic() {
        assert_eq!(calculate_percentage_rate(3, 12), Decimal::new(25, 0));
    }

    #[test]
    fn shadow_risk_rejection_rate_pct_calculation_is_deterministic() {
        let performance = evaluate_research_candidate_shadow_performance(
            Uuid::nil(),
            ResearchCandidateStatus::PromotedToShadowConfig,
            "momentum_v1",
            "BTCUSDT",
            "15m",
            ts(0, 0, 0),
            ts(0, 1, 0),
            20,
            4,
            2,
            5,
            1,
            0,
            Some(ts(0, 1, 0)),
            19,
            19,
            19,
            0,
            Some(ts(0, 1, 0)),
            Some(ts(0, 1, 0)),
            Some(ts(0, 0, 30)),
            true,
            ts(0, 1, 0),
        );

        assert_eq!(performance.risk_rejection_rate_pct, Decimal::new(25, 0));
    }

    #[test]
    fn shadow_performance_recommends_reject_after_enough_zero_would_submit_runs() {
        let performance = evaluate_research_candidate_shadow_performance(
            Uuid::nil(),
            ResearchCandidateStatus::PromotedToShadowConfig,
            "momentum_v1",
            "BTCUSDT",
            "15m",
            ts(0, 0, 0),
            ts(1, 0, 0),
            30,
            0,
            20,
            6,
            4,
            0,
            Some(ts(1, 0, 0)),
            20,
            20,
            20,
            0,
            Some(ts(1, 0, 0)),
            Some(ts(1, 0, 0)),
            Some(ts(0, 30, 0)),
            true,
            ts(1, 0, 0),
        );

        assert_eq!(
            performance.status,
            ResearchCandidateShadowPerformanceStatus::NeedsReview
        );
        assert_eq!(
            performance.recommendation,
            ResearchCandidateShadowPerformanceRecommendation::RejectCandidate
        );
    }

    fn qualification_request(
        performance: Option<ResearchCandidateShadowPerformance>,
    ) -> ResearchCandidateQualificationRequest {
        ResearchCandidateQualificationRequest {
            candidate_id: Uuid::nil(),
            candidate_status: Some(ResearchCandidateStatus::PromotedToShadowConfig),
            fresh_observation: true,
            formal_observation_status: Some(StrategyCandidateObservationStatus::ReadyForReview),
            formal_observation_stale: false,
            runner_alignment_valid: true,
            shadow_runner_covers_candidate: true,
            runner_mismatch_count: 0,
            latest_readiness_status: Some(ExecutionReadinessStatus::Ready),
            walk_forward_evidence: Some(sample_walk_forward_evidence(
                StrategyWalkForwardRobustnessStatus::Robust,
            )),
            shadow_performance: performance,
            shadow_pnl_attribution: None,
            thresholds: ResearchCandidateQualificationThresholds::default(),
            computed_at: ts(1, 0, 0),
        }
    }

    fn sample_walk_forward_evidence(
        status: StrategyWalkForwardRobustnessStatus,
    ) -> ResearchCandidateWalkForwardEvidence {
        ResearchCandidateWalkForwardEvidence {
            walk_forward_run_id: Uuid::from_u128(0x700),
            robustness_status: status,
            status: "COMPLETED".to_string(),
            recommendation_action: Some(if status == StrategyWalkForwardRobustnessStatus::Robust {
                "ACCEPT_FOR_REVIEW".to_string()
            } else {
                "DO_NOT_ACCEPT".to_string()
            }),
            recommendation_reason: Some(if status == StrategyWalkForwardRobustnessStatus::Robust {
                "Walk-forward robustness is acceptable.".to_string()
            } else {
                "Do not accept candidate until walk-forward robustness improves.".to_string()
            }),
            total_windows: 12,
            completed_windows: 12,
            profitable_windows: 9,
            losing_windows: 3,
            avg_pnl_pct: Decimal::new(12, 2),
            worst_pnl_pct: Decimal::new(-5, 2),
            best_pnl_pct: Decimal::new(40, 2),
            robustness_score: Decimal::new(75, 0),
            consistency_score: Decimal::new(75, 0),
            created_at: ts(1, 0, 0),
            linked_at: ts(1, 0, 0),
        }
    }

    fn sample_research_candidate(status: ResearchCandidateStatus) -> ResearchCandidate {
        ResearchCandidate {
            id: Uuid::from_u128(0x100),
            experiment_id: Some(Uuid::from_u128(0x101)),
            experiment_run_id: Some(Uuid::from_u128(0x102)),
            strategy_id: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "15m".to_string(),
            config: serde_json::json!({ "lookback": 20 }),
            score: Some(Decimal::new(87, 0)),
            pnl_pct: Some(Decimal::new(1245, 2)),
            max_drawdown_pct: Some(Decimal::new(315, 2)),
            trade_count: Some(32),
            win_rate: Some(Decimal::new(55, 0)),
            fee_drag: Some(Decimal::new(25, 2)),
            status,
            rejection_reason: None,
            notes: None,
            created_at: ts(0, 0, 0),
            updated_at: ts(1, 0, 0),
            correlation_id: Some(Uuid::from_u128(0x103)),
        }
    }

    fn sample_accept_shadow_preview_input() -> ResearchCandidateAcceptForShadowPreviewInput {
        ResearchCandidateAcceptForShadowPreviewInput {
            candidate: sample_research_candidate(ResearchCandidateStatus::Discovered),
            evidence_summary: ResearchCandidateAcceptForShadowEvidenceSummary {
                candidate_score: Some(Decimal::new(87, 0)),
                candidate_pnl_pct: Some(Decimal::new(1245, 2)),
                candidate_max_drawdown_pct: Some(Decimal::new(315, 2)),
                candidate_trade_count: Some(32),
                candidate_win_rate: Some(Decimal::new(55, 0)),
                candidate_fee_drag: Some(Decimal::new(25, 2)),
                source_experiment_id: Some(Uuid::from_u128(0x101)),
                source_experiment_run_id: Some(Uuid::from_u128(0x102)),
                source_walk_forward_run_id: Some(Uuid::from_u128(0x301)),
                source_robustness_matrix_run_id: Some(Uuid::from_u128(0x401)),
                config_fingerprint: Some("fingerprint".to_string()),
                data_quality_status: Some(MarketDataQualityStatus::Good),
                evaluated_start_time: Some(ts(0, 0, 0)),
                evaluated_end_time: Some(ts(1, 0, 0)),
                walk_forward_status: Some(StrategyWalkForwardRobustnessStatus::Robust),
                walk_forward_total_windows: Some(7),
                walk_forward_profitable_windows: Some(7),
                walk_forward_losing_windows: Some(0),
                walk_forward_worst_pnl_pct: Some(Decimal::new(40, 2)),
                robustness_matrix_status: Some(StrategyRobustnessMatrixStatus::Robust),
                shadow_run_count: 1,
            },
            source_experiment_exists: true,
            source_walk_forward_exists: true,
            source_robustness_matrix_exists: true,
            fresh_observation: true,
            require_fresh_observation: true,
            runner_alignment: aligned_runner_alignment(),
            required_runner_config_change: Vec::new(),
            btc_generalization_failed: false,
            holdout_2025_available: true,
            generated_at: ts(1, 0, 0),
        }
    }

    fn sample_observation(
        readiness_status: ExecutionReadinessStatus,
    ) -> StrategyCandidateObservationResult {
        StrategyCandidateObservationResult {
            observation_id: Uuid::from_u128(0x200),
            candidate_id: Uuid::from_u128(0x100),
            strategy_id: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "15m".to_string(),
            status: StrategyCandidateObservationStatus::ReadyForReview,
            requirements: base_observation_requirement(),
            runner_alignment: aligned_runner_alignment(),
            summary: StrategyCandidateObservationSummary {
                candidate_id: Uuid::from_u128(0x100),
                window_start: ts(0, 0, 0),
                window_end: ts(1, 0, 0),
                shadow_runs: 35,
                would_submit_count: 8,
                no_signal_count: 4,
                risk_rejected_count: 3,
                skipped_count: 1,
                risk_rejection_rate: Decimal::new(857, 2),
                no_signal_rate: Decimal::new(1143, 2),
                latest_readiness_status: Some(readiness_status),
                latest_readiness_score: Some(91),
                runner_alignment: aligned_runner_alignment(),
                decision: StrategyCandidateObservationDecision::Pass,
                findings: vec![StrategyCandidateObservationFinding {
                    code: "requirements_met".to_string(),
                    message: "Requirements met.".to_string(),
                    blocking: false,
                }],
                recommendations: vec!["Ready for review.".to_string()],
                created_at: ts(1, 0, 0),
            },
            decision: StrategyCandidateObservationDecision::Pass,
            started_at: ts(0, 0, 0),
            evaluated_at: ts(1, 0, 0),
            last_observed_at: ts(1, 0, 0),
            observation_expires_at: Some(ts(1, 15, 0)),
            observation_max_age_seconds: Some(900),
            observation_snapshot_hash: Some("hash".to_string()),
            runner_config_snapshot: None,
            readiness_snapshot: None,
            created_by: None,
            correlation_id: Some(Uuid::from_u128(0x201)),
        }
    }

    fn sample_testnet_review_request() -> ResearchCandidateTestnetReviewRequest {
        let candidate = sample_research_candidate(ResearchCandidateStatus::PromotedToShadowConfig);
        let observation = sample_observation(ExecutionReadinessStatus::Ready);
        let performance = qualification_performance(35, 8, 3, 0, 0);
        let qualification = ResearchCandidateQualificationEvaluation {
            id: Uuid::from_u128(0x300),
            candidate_id: candidate.id,
            status: ResearchCandidateQualificationStatus::Qualified,
            score: 91,
            latest_readiness_status: Some(ExecutionReadinessStatus::Ready),
            total_shadow_runs: performance.total_shadow_runs,
            would_submit_count: performance.would_submit_count,
            risk_rejection_rate_pct: Some(performance.risk_rejection_rate_pct),
            walk_forward_status: Some(StrategyWalkForwardRobustnessStatus::Robust),
            walk_forward_run_id: Some(Uuid::from_u128(0x700)),
            walk_forward_score: Some(Decimal::new(75, 0)),
            walk_forward_consistency_score: Some(Decimal::new(75, 0)),
            walk_forward_recommendation: Some("Walk-forward robustness is acceptable.".to_string()),
            walk_forward_blockers: Vec::new(),
            walk_forward_warnings: Vec::new(),
            warnings: Vec::new(),
            blockers: Vec::new(),
            recommendations: vec![
                ResearchCandidateQualificationRecommendation::ReadyForTestnetPromotionConsideration,
            ],
            thresholds: ResearchCandidateQualificationThresholds::default(),
            evaluated_at: ts(1, 0, 0),
            correlation_id: Some(Uuid::from_u128(0x301)),
        };
        let readiness = ResearchCandidatePromotionReadiness {
            candidate_id: candidate.id,
            target: "TESTNET_SHADOW".to_string(),
            latest_observation_id: Some(observation.observation_id),
            latest_observation_decision: Some(observation.decision),
            last_observed_at: Some(observation.last_observed_at),
            observation_expires_at: observation.observation_expires_at,
            observation_age_seconds: Some(0),
            observation_max_age_seconds: observation.observation_max_age_seconds,
            observation_snapshot_hash: observation.observation_snapshot_hash.clone(),
            latest_recommendation: Some("Ready for review.".to_string()),
            readiness_status: Some(ExecutionReadinessStatus::Ready),
            readiness_score: Some(91),
            runner_alignment: aligned_runner_alignment(),
            blockers: Vec::new(),
            is_ready: true,
            evaluated_at: ts(1, 0, 0),
        };

        ResearchCandidateTestnetReviewRequest {
            candidate: Some(candidate),
            latest_review_action: Some(ResearchCandidateReview {
                id: Uuid::from_u128(0x400),
                candidate_id: Uuid::from_u128(0x100),
                action: ResearchCandidateReviewAction::MarkReadyForTestnetReview,
                status: ResearchCandidateReviewStatus::Recorded,
                previous_candidate_status: ResearchCandidateStatus::PromotedToShadowConfig,
                next_candidate_status: None,
                reason: Some("ready".to_string()),
                notes: None,
                actor_id: Some(Uuid::from_u128(0x401)),
                created_at: ts(1, 0, 0),
                correlation_id: Some(Uuid::from_u128(0x402)),
                qualification_evaluation_id: Some(Uuid::from_u128(0x300)),
            }),
            ready_review_action_recorded: true,
            latest_qualification_evaluation: Some(qualification),
            qualification_trend: ResearchCandidateQualificationTrend::Stable,
            qualification_evaluation_recent: true,
            shadow_performance_summary: Some(performance.clone()),
            latest_observation: Some(observation.clone()),
            observation_summary: Some(ResearchCandidateObservationSummaryView {
                candidate_id: Uuid::from_u128(0x100),
                total_observations: 1,
                latest_observation_status: Some(observation.status),
                latest_formal_observation_at: Some(observation.evaluated_at),
                formal_observation_stale: false,
                latest_runner_alignment: Some(aligned_runner_alignment()),
                latest_readiness_status: Some(ExecutionReadinessStatus::Ready),
                latest_recommendations: vec!["Ready for review.".to_string()],
                stale_count: 0,
                alignment_mismatch_count: 0,
                runner_config_drift_count: 0,
                last_observed_at: Some(observation.last_observed_at),
                latest_linked_shadow_run_id: None,
                latest_linked_shadow_decision: None,
                latest_linked_shadow_status: None,
                latest_linked_shadow_run_at: None,
                latest_valid_shadow_run_id: None,
                latest_valid_shadow_decision: None,
                latest_valid_shadow_status: None,
                latest_valid_shadow_run_at: Some(ts(1, 0, 0)),
                latest_skipped_shadow_run_at: None,
                linked_shadow_completed_count: performance.completed_shadow_runs,
                linked_shadow_no_signal_count: performance.no_signal_count,
                linked_shadow_would_submit_count: performance.would_submit_count,
                linked_shadow_risk_rejected_count: performance.risk_rejected_count,
                linked_shadow_skipped_count: performance.skipped_count,
                shadow_observation_status: Some(performance.status),
                current_accept_for_shadow_eligible: true,
                current_accept_for_shadow_blockers: Vec::new(),
                computed_at: ts(1, 0, 0),
            }),
            observation_freshness: ResearchCandidateObservationFreshnessStatus::Fresh,
            observation_age_seconds: Some(0),
            runner_alignment: Some(aligned_runner_alignment()),
            readiness_snapshot: Some(readiness),
            walk_forward_evidence: Some(sample_walk_forward_evidence(
                StrategyWalkForwardRobustnessStatus::Robust,
            )),
            shadow_pnl_attribution: None,
            exit_attribution: None,
            private_stream_stale_warning: false,
            require_ready_review_action: true,
            no_execution_table_mutation: true,
            generated_at: ts(1, 0, 0),
            correlation_id: Uuid::from_u128(0x500),
            operator_report_findings: Vec::new(),
        }
    }

    fn qualification_performance(
        total_shadow_runs: i64,
        would_submit_count: i64,
        risk_rejected_count: i64,
        skipped_count: i64,
        error_count: i64,
    ) -> ResearchCandidateShadowPerformance {
        evaluate_research_candidate_shadow_performance(
            Uuid::nil(),
            ResearchCandidateStatus::PromotedToShadowConfig,
            "momentum_v1",
            "BTCUSDT",
            "15m",
            ts(0, 0, 0),
            ts(1, 0, 0),
            total_shadow_runs,
            would_submit_count,
            0,
            risk_rejected_count,
            skipped_count,
            error_count,
            Some(ts(1, 0, 0)),
            total_shadow_runs - skipped_count - error_count,
            total_shadow_runs - skipped_count - error_count,
            total_shadow_runs - skipped_count - error_count,
            0,
            Some(ts(1, 0, 0)),
            Some(ts(1, 0, 0)),
            if skipped_count > 0 {
                Some(ts(0, 30, 0))
            } else {
                None
            },
            true,
            ts(1, 0, 0),
        )
    }

    #[test]
    fn qualification_needs_more_data_when_total_shadow_runs_below_threshold() {
        let result = evaluate_research_candidate_qualification(&qualification_request(Some(
            qualification_performance(12, 4, 1, 0, 0),
        )));

        assert_eq!(
            result.status,
            ResearchCandidateQualificationStatus::NeedsMoreData
        );
        assert!(result
            .recommendations
            .contains(&ResearchCandidateQualificationRecommendation::GatherMoreShadowRuns));
    }

    #[test]
    fn qualification_counts_duplicate_same_candle_runs_as_non_independent() {
        let performance = evaluate_research_candidate_shadow_performance(
            Uuid::nil(),
            ResearchCandidateStatus::PromotedToShadowConfig,
            "momentum_v1",
            "BTCUSDT",
            "1h",
            ts(0, 0, 0),
            ts(1, 0, 0),
            5,
            3,
            2,
            0,
            0,
            0,
            Some(ts(1, 0, 0)),
            5,
            1,
            1,
            4,
            Some(ts(0, 0, 0)),
            Some(ts(1, 0, 0)),
            None,
            true,
            ts(1, 0, 0),
        );
        let mut request = qualification_request(Some(performance));
        request.thresholds.min_shadow_runs = 5;
        request.thresholds.min_would_submit_count = 1;

        let result = evaluate_research_candidate_qualification(&request);

        assert!(result
            .checks
            .iter()
            .any(|check| check.code == "duplicate_same_candle_shadow_runs"));
        assert!(result
            .checks
            .iter()
            .any(|check| check.code == "enough_linked_shadow_runs" && !check.passed));
    }

    #[test]
    fn qualification_treats_completed_no_signal_as_valid_but_not_qualifying_evidence() {
        let performance = evaluate_research_candidate_shadow_performance(
            Uuid::nil(),
            ResearchCandidateStatus::PromotedToShadowConfig,
            "momentum_v1",
            "BTCUSDT",
            "15m",
            ts(0, 0, 0),
            ts(1, 0, 0),
            2,
            0,
            1,
            0,
            1,
            0,
            Some(ts(1, 0, 0)),
            1,
            1,
            1,
            0,
            Some(ts(1, 0, 0)),
            Some(ts(1, 0, 0)),
            Some(ts(0, 30, 0)),
            true,
            ts(1, 0, 0),
        );
        let mut request = qualification_request(Some(performance));
        request.fresh_observation = false;
        request.formal_observation_stale = true;

        let result = evaluate_research_candidate_qualification(&request);

        assert_eq!(
            result.status,
            ResearchCandidateQualificationStatus::NeedsMoreData
        );
        assert_eq!(result.linked_shadow_completed_count, 1);
        assert_eq!(result.linked_shadow_no_signal_count, 1);
        assert_eq!(result.linked_shadow_would_submit_count, 0);
        assert_eq!(result.linked_shadow_skipped_count, 1);
        assert_eq!(result.latest_valid_shadow_run_at, Some(ts(1, 0, 0)));
        assert_eq!(result.latest_skipped_shadow_run_at, Some(ts(0, 30, 0)));
        assert!(result
            .warnings
            .iter()
            .any(|warning| warning.contains("valid linked shadow evidence")));
        assert!(result
            .evidence_interpretation
            .contains("Valid linked shadow observation exists"));
        assert!(result.recommendations.contains(
            &ResearchCandidateQualificationRecommendation::GenerateMoreWouldSubmitEvidence
        ));
    }

    #[test]
    fn qualification_not_qualified_when_candidate_is_not_accepted_for_shadow() {
        let mut request = qualification_request(Some(qualification_performance(40, 6, 4, 0, 0)));
        request.candidate_status = Some(ResearchCandidateStatus::Observing);

        let result = evaluate_research_candidate_qualification(&request);

        assert_eq!(
            result.status,
            ResearchCandidateQualificationStatus::NotQualified
        );
    }

    #[test]
    fn qualification_needs_more_data_when_candidate_is_accepted_but_not_promoted() {
        let mut request = qualification_request(Some(qualification_performance(40, 6, 4, 0, 0)));
        request.candidate_status = Some(ResearchCandidateStatus::AcceptedForShadow);

        let result = evaluate_research_candidate_qualification(&request);

        assert_eq!(
            result.status,
            ResearchCandidateQualificationStatus::NeedsMoreData
        );
        assert!(result.warnings.iter().any(|warning| {
            warning == "Candidate is accepted but not promoted to shadow runner config."
        }));
    }

    #[test]
    fn qualification_warns_without_fresh_observation_when_valid_shadow_evidence_exists() {
        let mut request = qualification_request(Some(qualification_performance(40, 6, 4, 0, 0)));
        request.fresh_observation = false;
        request.formal_observation_stale = true;

        let result = evaluate_research_candidate_qualification(&request);

        assert_eq!(
            result.status,
            ResearchCandidateQualificationStatus::Qualified
        );
        assert!(result.formal_observation_stale);
        assert!(result
            .warnings
            .iter()
            .any(|warning| warning.contains("valid linked shadow evidence")));
    }

    #[test]
    fn qualification_blocks_stale_formal_observation_without_valid_shadow_evidence() {
        let mut request = qualification_request(None);
        request.fresh_observation = false;
        request.formal_observation_stale = true;

        let result = evaluate_research_candidate_qualification(&request);

        assert_eq!(
            result.status,
            ResearchCandidateQualificationStatus::NotQualified
        );
        assert!(result
            .blockers
            .iter()
            .any(|blocker| blocker.contains("no valid linked shadow evidence")));
    }

    #[test]
    fn qualification_not_qualified_when_runner_alignment_mismatches() {
        let mut request = qualification_request(Some(qualification_performance(40, 6, 4, 0, 0)));
        request.runner_alignment_valid = false;
        request.runner_mismatch_count = 1;

        let result = evaluate_research_candidate_qualification(&request);

        assert_eq!(
            result.status,
            ResearchCandidateQualificationStatus::NotQualified
        );
    }

    #[test]
    fn qualification_degraded_when_risk_rejection_rate_is_high() {
        let result = evaluate_research_candidate_qualification(&qualification_request(Some(
            qualification_performance(40, 8, 20, 0, 0),
        )));

        assert_eq!(
            result.status,
            ResearchCandidateQualificationStatus::Degraded
        );
        assert!(result
            .warnings
            .iter()
            .any(|warning| warning.contains("Risk rejection rate")));
    }

    #[test]
    fn qualification_is_qualified_when_thresholds_pass() {
        let result = evaluate_research_candidate_qualification(&qualification_request(Some(
            qualification_performance(40, 8, 8, 0, 0),
        )));

        assert_eq!(
            result.status,
            ResearchCandidateQualificationStatus::Qualified
        );
        assert_eq!(result.score, 100);
        assert_eq!(
            result.recommendations,
            vec![
                ResearchCandidateQualificationRecommendation::ReadyForTestnetPromotionConsideration
            ]
        );
    }

    #[test]
    fn qualification_not_qualified_when_walk_forward_overfit_risk() {
        let mut request = qualification_request(Some(qualification_performance(40, 8, 0, 0, 0)));
        request.walk_forward_evidence = Some(sample_walk_forward_evidence(
            StrategyWalkForwardRobustnessStatus::OverfitRisk,
        ));

        let result = evaluate_research_candidate_qualification(&request);

        assert_eq!(
            result.status,
            ResearchCandidateQualificationStatus::NotQualified
        );
        assert_eq!(
            result.walk_forward_status,
            Some(StrategyWalkForwardRobustnessStatus::OverfitRisk)
        );
        assert!(result
            .walk_forward_blockers
            .iter()
            .any(|value| value.contains("OVERFIT_RISK")));
    }

    #[test]
    fn qualification_needs_more_data_without_walk_forward_evidence() {
        let mut request = qualification_request(Some(qualification_performance(40, 8, 0, 0, 0)));
        request.walk_forward_evidence = None;

        let result = evaluate_research_candidate_qualification(&request);

        assert_eq!(
            result.status,
            ResearchCandidateQualificationStatus::NeedsMoreData
        );
        assert!(result
            .walk_forward_warnings
            .contains(&"NO_WALK_FORWARD_EVIDENCE".to_string()));
    }

    #[test]
    fn qualification_degraded_readiness_cannot_score_hundred() {
        let mut request = qualification_request(Some(qualification_performance(40, 8, 8, 0, 0)));
        request.latest_readiness_status = Some(ExecutionReadinessStatus::Degraded);

        let result = evaluate_research_candidate_qualification(&request);

        assert_eq!(
            result.status,
            ResearchCandidateQualificationStatus::Degraded
        );
        assert!(result.score < 100);
        assert_eq!(result.readiness_penalty_points, 12);
        assert!(result
            .warnings
            .iter()
            .any(|warning| warning.contains("DEGRADED")));
        assert!(result.score_explanation.iter().any(|item| {
            item.contains(
                "Resolve degraded readiness conditions before considering testnet promotion.",
            )
        }));
    }

    #[test]
    fn qualification_not_ready_blocks_promotion_and_caps_score() {
        let mut request = qualification_request(Some(qualification_performance(40, 8, 8, 0, 0)));
        request.latest_readiness_status = Some(ExecutionReadinessStatus::NotReady);

        let result = evaluate_research_candidate_qualification(&request);

        assert_eq!(
            result.status,
            ResearchCandidateQualificationStatus::NotQualified
        );
        assert!(result.score <= 40);
        assert_eq!(result.readiness_penalty_points, 30);
        assert!(result
            .blockers
            .iter()
            .any(|item| item.contains("NOT_READY")));
        assert!(result.score_explanation.iter().any(|item| {
            item.contains("Do not consider testnet promotion until readiness blockers are cleared.")
        }));
    }

    #[test]
    fn qualification_unknown_readiness_does_not_score_hundred() {
        let mut request = qualification_request(Some(qualification_performance(40, 8, 8, 0, 0)));
        request.latest_readiness_status = Some(ExecutionReadinessStatus::Unknown);

        let result = evaluate_research_candidate_qualification(&request);

        assert_eq!(
            result.status,
            ResearchCandidateQualificationStatus::Degraded
        );
        assert!(result.score < 100);
        assert_eq!(result.readiness_penalty_points, 10);
        assert!(result
            .warnings
            .iter()
            .any(|warning| warning.contains("UNKNOWN")));
    }

    #[test]
    fn qualification_override_below_default_adds_warning() {
        let mut request = qualification_request(Some(qualification_performance(5, 3, 0, 0, 0)));
        request.thresholds.min_shadow_runs = 5;

        let result = evaluate_research_candidate_qualification(&request);

        assert!(result.threshold_override_below_default);
        assert_eq!(result.threshold_override_penalty_points, 5);
        assert!(result.score < 100);
        assert!(result
            .warnings
            .iter()
            .any(|warning| warning.contains("below default")));
        assert!(result.score_explanation.iter().any(|item| {
            item.contains(
                "Qualification threshold override is below default; treat result as exploratory.",
            )
        }));
    }

    #[test]
    fn qualification_score_clamps_to_zero_and_hundred() {
        let mut request = qualification_request(Some(qualification_performance(0, 0, 0, 50, 50)));
        request.candidate_status = Some(ResearchCandidateStatus::Rejected);
        request.fresh_observation = false;
        request.runner_alignment_valid = false;
        request.shadow_runner_covers_candidate = false;
        request.runner_mismatch_count = 10;
        request.latest_readiness_status = Some(ExecutionReadinessStatus::NotReady);

        let low = evaluate_research_candidate_qualification(&request);
        let high = evaluate_research_candidate_qualification(&qualification_request(Some(
            qualification_performance(40, 8, 0, 0, 0),
        )));

        assert_eq!(low.score, 0);
        assert_eq!(high.score, 100);
    }

    #[test]
    fn qualification_recommendations_are_deterministic() {
        let mut request = qualification_request(Some(qualification_performance(10, 1, 5, 3, 3)));
        request.fresh_observation = false;

        let result = evaluate_research_candidate_qualification(&request);

        assert_eq!(
            result.recommendations,
            vec![
                ResearchCandidateQualificationRecommendation::RefreshCandidateObservation,
                ResearchCandidateQualificationRecommendation::GatherMoreShadowRuns,
                ResearchCandidateQualificationRecommendation::GenerateMoreWouldSubmitEvidence,
                ResearchCandidateQualificationRecommendation::ReviewRiskRejections,
                ResearchCandidateQualificationRecommendation::ReduceShadowErrorsOrSkips,
            ]
        );
    }

    fn sample_qualification_evaluation(
        status: ResearchCandidateQualificationStatus,
        score: i32,
        evaluated_at: DateTime<Utc>,
    ) -> ResearchCandidateQualificationEvaluation {
        ResearchCandidateQualificationEvaluation {
            id: Uuid::new_v4(),
            candidate_id: Uuid::new_v4(),
            status,
            score,
            latest_readiness_status: Some(ExecutionReadinessStatus::Ready),
            total_shadow_runs: 30,
            would_submit_count: 5,
            risk_rejection_rate_pct: Some(Decimal::new(10, 0)),
            walk_forward_status: Some(StrategyWalkForwardRobustnessStatus::Robust),
            walk_forward_run_id: Some(Uuid::from_u128(0x700)),
            walk_forward_score: Some(Decimal::new(75, 0)),
            walk_forward_consistency_score: Some(Decimal::new(75, 0)),
            walk_forward_recommendation: Some("Walk-forward robustness is acceptable.".to_string()),
            walk_forward_blockers: Vec::new(),
            walk_forward_warnings: Vec::new(),
            warnings: Vec::new(),
            blockers: Vec::new(),
            recommendations: Vec::new(),
            thresholds: ResearchCandidateQualificationThresholds::default(),
            evaluated_at,
            correlation_id: None,
        }
    }

    #[test]
    fn qualification_change_detects_newly_qualified() {
        let previous = sample_qualification_evaluation(
            ResearchCandidateQualificationStatus::NeedsMoreData,
            68,
            Utc::now() - Duration::hours(2),
        );
        let current = sample_qualification_evaluation(
            ResearchCandidateQualificationStatus::Qualified,
            82,
            Utc::now(),
        );

        let change = research_candidate_qualification_change(&current, Some(&previous))
            .expect("change should exist");

        assert!(change.newly_qualified);
        assert!(!change.lost_qualification);
    }

    #[test]
    fn qualification_change_detects_lost_qualification() {
        let previous = sample_qualification_evaluation(
            ResearchCandidateQualificationStatus::Qualified,
            88,
            Utc::now() - Duration::hours(2),
        );
        let current = sample_qualification_evaluation(
            ResearchCandidateQualificationStatus::Degraded,
            70,
            Utc::now(),
        );

        let change = research_candidate_qualification_change(&current, Some(&previous))
            .expect("change should exist");

        assert!(change.lost_qualification);
        assert!(!change.newly_qualified);
    }

    #[test]
    fn qualification_trend_detects_improving_and_degrading() {
        let previous = sample_qualification_evaluation(
            ResearchCandidateQualificationStatus::NeedsMoreData,
            60,
            Utc::now() - Duration::hours(2),
        );
        let improving = sample_qualification_evaluation(
            ResearchCandidateQualificationStatus::NeedsMoreData,
            74,
            Utc::now(),
        );
        let degrading = sample_qualification_evaluation(
            ResearchCandidateQualificationStatus::NeedsMoreData,
            48,
            Utc::now(),
        );

        assert_eq!(
            research_candidate_qualification_trend(&improving, Some(&previous)),
            ResearchCandidateQualificationTrend::Improving
        );
        assert_eq!(
            research_candidate_qualification_trend(&degrading, Some(&previous)),
            ResearchCandidateQualificationTrend::Degrading
        );
    }

    #[test]
    fn qualification_evaluation_stale_detection_is_deterministic() {
        let now = Utc::now();
        let stale = sample_qualification_evaluation(
            ResearchCandidateQualificationStatus::Qualified,
            90,
            now - Duration::hours(25),
        );
        let fresh = sample_qualification_evaluation(
            ResearchCandidateQualificationStatus::Qualified,
            90,
            now - Duration::hours(1),
        );

        assert!(is_research_candidate_evaluation_stale(
            &stale,
            now,
            Duration::hours(24)
        ));
        assert!(!is_research_candidate_evaluation_stale(
            &fresh,
            now,
            Duration::hours(24)
        ));
    }

    #[test]
    fn watchlist_status_uses_latest_evaluation_and_staleness() {
        let now = Utc::now();
        let latest = sample_qualification_evaluation(
            ResearchCandidateQualificationStatus::Qualified,
            90,
            now - Duration::hours(1),
        );
        let stale = sample_qualification_evaluation(
            ResearchCandidateQualificationStatus::Qualified,
            90,
            now - Duration::hours(30),
        );

        assert_eq!(
            research_candidate_watchlist_status(
                Some(&latest),
                ResearchCandidateQualificationTrend::NewlyQualified,
                now,
                Duration::hours(24),
            ),
            ResearchCandidateWatchlistStatus::NewlyQualified
        );
        assert_eq!(
            research_candidate_watchlist_status(
                Some(&stale),
                ResearchCandidateQualificationTrend::Stable,
                now,
                Duration::hours(24),
            ),
            ResearchCandidateWatchlistStatus::NeedsAttention
        );
    }

    #[test]
    fn runner_timeframe_mismatch_detected() {
        let requirement = base_observation_requirement();
        let summary = evaluate_strategy_candidate_observation(
            &requirement,
            ts(0, 0, 0),
            ts(0, 0, 0) + chrono::Duration::hours(24),
            30,
            2,
            5,
            2,
            0,
            Some(ExecutionReadinessStatus::Ready),
            Some(90),
            StrategyCandidateRunnerAlignment {
                strategy_config_matches_runner: false,
                runner_enabled: true,
                runner_status: "RUNNING".to_string(),
                runner_timeframe: "1m".to_string(),
                runner_symbols: vec!["BTCUSDT".to_string()],
                runner_strategies: vec!["momentum_v1".to_string()],
                mismatch_reasons: vec![
                    "runner timeframe 1m does not include candidate timeframe 15m".to_string(),
                ],
            },
            ts(0, 0, 0) + chrono::Duration::hours(24),
        );

        assert_eq!(
            summary.decision,
            StrategyCandidateObservationDecision::InsufficientData
        );
        assert!(summary
            .findings
            .iter()
            .any(|finding| finding.code == "shadow_runner_config_mismatch"));
        assert_eq!(
            summary.recommendations,
            vec!["Update shadow runner config to include momentum_v1 BTCUSDT 15m.".to_string()]
        );
    }

    #[test]
    fn runner_symbol_mismatch_detected() {
        let requirement = base_observation_requirement();
        let summary = evaluate_strategy_candidate_observation(
            &requirement,
            ts(0, 0, 0),
            ts(0, 0, 0) + chrono::Duration::hours(24),
            30,
            2,
            5,
            2,
            0,
            Some(ExecutionReadinessStatus::Ready),
            Some(90),
            StrategyCandidateRunnerAlignment {
                strategy_config_matches_runner: false,
                runner_enabled: true,
                runner_status: "RUNNING".to_string(),
                runner_timeframe: "15m".to_string(),
                runner_symbols: vec!["ETHUSDT".to_string()],
                runner_strategies: vec!["momentum_v1".to_string()],
                mismatch_reasons: vec![
                    "runner symbols [ETHUSDT] do not include candidate symbol BTCUSDT".to_string(),
                ],
            },
            ts(0, 0, 0) + chrono::Duration::hours(24),
        );

        assert_eq!(
            summary.decision,
            StrategyCandidateObservationDecision::InsufficientData
        );
        assert_eq!(summary.runner_alignment.runner_symbols, vec!["ETHUSDT"]);
    }

    #[test]
    fn runner_strategy_mismatch_detected() {
        let requirement = base_observation_requirement();
        let summary = evaluate_strategy_candidate_observation(
            &requirement,
            ts(0, 0, 0),
            ts(0, 0, 0) + chrono::Duration::hours(24),
            30,
            2,
            5,
            2,
            0,
            Some(ExecutionReadinessStatus::Ready),
            Some(90),
            StrategyCandidateRunnerAlignment {
                strategy_config_matches_runner: false,
                runner_enabled: true,
                runner_status: "RUNNING".to_string(),
                runner_timeframe: "15m".to_string(),
                runner_symbols: vec!["BTCUSDT".to_string()],
                runner_strategies: vec!["mean_reversion_v1".to_string()],
                mismatch_reasons: vec![
                    "runner strategies [mean_reversion_v1] do not include candidate strategy momentum_v1"
                        .to_string(),
                ],
            },
            ts(0, 0, 0) + chrono::Duration::hours(24),
        );

        assert_eq!(
            summary.decision,
            StrategyCandidateObservationDecision::InsufficientData
        );
        assert_eq!(
            summary.runner_alignment.runner_strategies,
            vec!["mean_reversion_v1"]
        );
    }

    #[test]
    fn aligned_runner_passes_preflight() {
        let requirement = base_observation_requirement();
        let summary = evaluate_strategy_candidate_observation(
            &requirement,
            ts(0, 0, 0),
            ts(0, 0, 0) + chrono::Duration::hours(24),
            30,
            4,
            6,
            3,
            1,
            Some(ExecutionReadinessStatus::Ready),
            Some(95),
            aligned_runner_alignment(),
            ts(0, 0, 0) + chrono::Duration::hours(24),
        );

        assert!(summary.runner_alignment.strategy_config_matches_runner);
        assert!(summary.recommendations.is_empty());
        assert!(!summary
            .findings
            .iter()
            .any(|finding| finding.code == "shadow_runner_config_mismatch"));
    }

    #[test]
    fn accept_shadow_preview_missing_observation_needs_more_data() {
        let mut input = sample_accept_shadow_preview_input();
        input.fresh_observation = false;
        input.evidence_summary.shadow_run_count = 0;

        let preview = evaluate_research_candidate_accept_for_shadow_preview(input);

        assert_eq!(
            preview.status,
            ResearchCandidateAcceptForShadowPreviewStatus::NeedsMoreData
        );
        assert_eq!(preview.recommended_action, "RUN_CANDIDATE_OBSERVATION");
        assert!(preview
            .warnings
            .contains(&"Fresh persisted observation is missing.".to_string()));
        assert!(preview.no_mutation);
    }

    #[test]
    fn accept_shadow_preview_runner_mismatch_requires_warning_review() {
        let mut input = sample_accept_shadow_preview_input();
        input.runner_alignment = StrategyCandidateRunnerAlignment {
            strategy_config_matches_runner: false,
            runner_enabled: false,
            runner_status: "STOPPED".to_string(),
            runner_timeframe: "1m".to_string(),
            runner_symbols: vec!["BTCUSDT".to_string()],
            runner_strategies: vec!["momentum_v1".to_string()],
            mismatch_reasons: vec![
                "runner timeframe 1m does not include candidate timeframe 15m".to_string(),
            ],
        };
        input.required_runner_config_change = vec!["set timeframe from 1m to 15m".to_string()];

        let preview = evaluate_research_candidate_accept_for_shadow_preview(input);

        assert_eq!(
            preview.status,
            ResearchCandidateAcceptForShadowPreviewStatus::WarningReviewRequired
        );
        assert!(preview
            .warnings
            .contains(&"Current shadow runner config does not cover the candidate.".to_string()));
        assert_eq!(
            preview.required_runner_config_change,
            vec!["set timeframe from 1m to 15m"]
        );
    }

    #[test]
    fn accept_shadow_preview_non_robust_walk_forward_blocks() {
        let mut input = sample_accept_shadow_preview_input();
        input.evidence_summary.walk_forward_status =
            Some(StrategyWalkForwardRobustnessStatus::Weak);

        let preview = evaluate_research_candidate_accept_for_shadow_preview(input);

        assert_eq!(
            preview.status,
            ResearchCandidateAcceptForShadowPreviewStatus::Blocked
        );
        assert!(preview
            .blockers
            .contains(&"Source walk-forward evidence is not ROBUST.".to_string()));
    }

    #[test]
    fn accept_shadow_preview_rejected_and_archived_candidates_block() {
        for status in [
            ResearchCandidateStatus::Rejected,
            ResearchCandidateStatus::Archived,
        ] {
            let mut input = sample_accept_shadow_preview_input();
            input.candidate.status = status;

            let preview = evaluate_research_candidate_accept_for_shadow_preview(input);

            assert_eq!(
                preview.status,
                ResearchCandidateAcceptForShadowPreviewStatus::Blocked
            );
            assert_eq!(preview.current_status, status);
            assert!(preview
                .blockers
                .iter()
                .any(|blocker| blocker.contains("not eligible")));
        }
    }

    #[test]
    fn accept_shadow_preview_accepted_candidate_points_to_shadow_promotion_preview() {
        let mut input = sample_accept_shadow_preview_input();
        input.candidate.status = ResearchCandidateStatus::AcceptedForShadow;

        let preview = evaluate_research_candidate_accept_for_shadow_preview(input);

        assert_eq!(
            preview.status,
            ResearchCandidateAcceptForShadowPreviewStatus::Blocked
        );
        assert_eq!(
            preview.recommended_action,
            "PREVIEW_SHADOW_RUNNER_PROMOTION"
        );
    }

    #[test]
    fn accept_shadow_preview_does_not_mutate_candidate_status_or_emit_events() {
        let input = sample_accept_shadow_preview_input();
        let candidate_status = input.candidate.status;

        let preview = evaluate_research_candidate_accept_for_shadow_preview(input);

        assert_eq!(preview.current_status, candidate_status);
        assert_eq!(
            preview.status,
            ResearchCandidateAcceptForShadowPreviewStatus::ReadyForHumanAcceptance
        );
        assert!(preview.no_mutation);
    }

    #[test]
    fn accept_shadow_apply_requires_exact_confirmation() {
        let input = sample_accept_shadow_preview_input();
        let candidate_id = input.candidate.id;
        let preview = evaluate_research_candidate_accept_for_shadow_preview(input);

        let rejection = validate_research_candidate_accept_for_shadow_apply(
            candidate_id,
            &preview,
            "ACCEPT CANDIDATE wrong FOR SHADOW",
            true,
        )
        .expect_err("bad confirmation should reject");

        assert_eq!(
            rejection,
            ResearchCandidateAcceptForShadowApplyRejection::InvalidConfirmation
        );
    }

    #[test]
    fn accept_shadow_apply_requires_warning_acknowledgement() {
        let mut input = sample_accept_shadow_preview_input();
        let candidate_id = input.candidate.id;
        input.runner_alignment = StrategyCandidateRunnerAlignment {
            strategy_config_matches_runner: false,
            runner_enabled: false,
            runner_status: "STOPPED".to_string(),
            runner_timeframe: "1m".to_string(),
            runner_symbols: vec!["BTCUSDT".to_string()],
            runner_strategies: vec!["momentum_v1".to_string()],
            mismatch_reasons: vec![
                "runner timeframe 1m does not include candidate timeframe 15m".to_string(),
            ],
        };
        let preview = evaluate_research_candidate_accept_for_shadow_preview(input);

        let rejection = validate_research_candidate_accept_for_shadow_apply(
            candidate_id,
            &preview,
            &expected_research_candidate_accept_shadow_confirmation(candidate_id),
            false,
        )
        .expect_err("unacknowledged warnings should reject");

        assert_eq!(
            rejection,
            ResearchCandidateAcceptForShadowApplyRejection::WarningAcknowledgementRequired
        );
        validate_research_candidate_accept_for_shadow_apply(
            candidate_id,
            &preview,
            &expected_research_candidate_accept_shadow_confirmation(candidate_id),
            true,
        )
        .expect("acknowledged warnings should pass");
    }

    #[test]
    fn accept_shadow_apply_rejects_blockers_and_already_accepted_candidates() {
        let mut blocked_input = sample_accept_shadow_preview_input();
        let blocked_candidate_id = blocked_input.candidate.id;
        blocked_input.source_walk_forward_exists = false;
        let blocked_preview = evaluate_research_candidate_accept_for_shadow_preview(blocked_input);

        let blocked = validate_research_candidate_accept_for_shadow_apply(
            blocked_candidate_id,
            &blocked_preview,
            &expected_research_candidate_accept_shadow_confirmation(blocked_candidate_id),
            true,
        )
        .expect_err("blockers should reject");
        assert_eq!(
            blocked,
            ResearchCandidateAcceptForShadowApplyRejection::PreviewBlocked
        );

        let mut accepted_input = sample_accept_shadow_preview_input();
        accepted_input.candidate.status = ResearchCandidateStatus::AcceptedForShadow;
        let accepted_candidate_id = accepted_input.candidate.id;
        let accepted_preview =
            evaluate_research_candidate_accept_for_shadow_preview(accepted_input);

        let accepted = validate_research_candidate_accept_for_shadow_apply(
            accepted_candidate_id,
            &accepted_preview,
            &expected_research_candidate_accept_shadow_confirmation(accepted_candidate_id),
            true,
        )
        .expect_err("already accepted should reject");
        assert_eq!(
            accepted,
            ResearchCandidateAcceptForShadowApplyRejection::CandidateNotEligible
        );
    }

    #[test]
    fn accept_for_shadow_transition_is_valid_from_observing() {
        let next = research_candidate_next_status(
            ResearchCandidateStatus::Observing,
            ResearchCandidateDecision::AcceptForShadow,
        )
        .expect("transition should be valid");

        assert_eq!(next, ResearchCandidateStatus::AcceptedForShadow);
    }

    #[test]
    fn promote_to_shadow_config_decision_moves_accepted_candidate_to_promoted() {
        let next = research_candidate_next_status(
            ResearchCandidateStatus::AcceptedForShadow,
            ResearchCandidateDecision::PromoteToShadowConfig,
        )
        .expect("promotion transition should be valid");

        assert_eq!(next, ResearchCandidateStatus::PromotedToShadowConfig);
    }

    #[test]
    fn reject_transition_requires_supported_source_status() {
        let err = research_candidate_next_status(
            ResearchCandidateStatus::Archived,
            ResearchCandidateDecision::Reject,
        )
        .expect_err("archived candidate should not reject again");

        assert!(matches!(
            err,
            CoreError::InvalidResearchCandidateTransition(_, _)
        ));
    }

    #[test]
    fn archive_transition_is_valid_from_any_non_archived_status() {
        for status in [
            ResearchCandidateStatus::Discovered,
            ResearchCandidateStatus::Observing,
            ResearchCandidateStatus::AcceptedForShadow,
            ResearchCandidateStatus::PromotedToShadowConfig,
            ResearchCandidateStatus::Rejected,
        ] {
            let next = research_candidate_next_status(status, ResearchCandidateDecision::Archive)
                .expect("archive should be valid");
            assert_eq!(next, ResearchCandidateStatus::Archived);
        }
    }

    #[test]
    fn reopen_only_allows_rejected_or_archived() {
        assert_eq!(
            research_candidate_next_status(
                ResearchCandidateStatus::Rejected,
                ResearchCandidateDecision::Reopen
            )
            .expect("rejected candidate should reopen"),
            ResearchCandidateStatus::Discovered
        );
        assert_eq!(
            research_candidate_next_status(
                ResearchCandidateStatus::Archived,
                ResearchCandidateDecision::Reopen
            )
            .expect("archived candidate should reopen"),
            ResearchCandidateStatus::Discovered
        );
        assert!(research_candidate_next_status(
            ResearchCandidateStatus::Observing,
            ResearchCandidateDecision::Reopen
        )
        .is_err());
    }

    #[test]
    fn review_reject_and_archive_require_reason() {
        let reject = research_candidate_review_outcome(
            ResearchCandidateStatus::Observing,
            ResearchCandidateReviewAction::RejectFromWatchlist,
            ResearchCandidateReviewContext {
                latest_qualification_status: None,
                latest_watchlist_status: None,
            },
            None,
        );
        let archive = research_candidate_review_outcome(
            ResearchCandidateStatus::Observing,
            ResearchCandidateReviewAction::ArchiveFromWatchlist,
            ResearchCandidateReviewContext {
                latest_qualification_status: None,
                latest_watchlist_status: None,
            },
            Some(" "),
        );

        assert!(matches!(
            reject,
            Err(CoreError::MissingResearchCandidateReviewReason(_))
        ));
        assert!(matches!(
            archive,
            Err(CoreError::MissingResearchCandidateReviewReason(_))
        ));
    }

    #[test]
    fn review_ready_for_testnet_requires_latest_qualified() {
        let result = research_candidate_review_outcome(
            ResearchCandidateStatus::AcceptedForShadow,
            ResearchCandidateReviewAction::MarkReadyForTestnetReview,
            ResearchCandidateReviewContext {
                latest_qualification_status: Some(ResearchCandidateQualificationStatus::Degraded),
                latest_watchlist_status: Some(ResearchCandidateWatchlistStatus::NeedsAttention),
            },
            Some("not yet"),
        );

        assert!(matches!(
            result,
            Err(CoreError::ResearchCandidateReviewRequiresQualified(_))
        ));
    }

    #[test]
    fn review_mark_reviewed_does_not_change_candidate_status() {
        let result = research_candidate_review_outcome(
            ResearchCandidateStatus::AcceptedForShadow,
            ResearchCandidateReviewAction::MarkReviewed,
            ResearchCandidateReviewContext {
                latest_qualification_status: Some(ResearchCandidateQualificationStatus::Qualified),
                latest_watchlist_status: Some(ResearchCandidateWatchlistStatus::Stable),
            },
            Some("reviewed"),
        )
        .expect("review should be recorded");

        assert_eq!(result.status, ResearchCandidateReviewStatus::Recorded);
        assert_eq!(result.next_candidate_status, None);
    }

    #[test]
    fn review_reject_moves_candidate_to_rejected() {
        let result = research_candidate_review_outcome(
            ResearchCandidateStatus::AcceptedForShadow,
            ResearchCandidateReviewAction::RejectFromWatchlist,
            ResearchCandidateReviewContext {
                latest_qualification_status: Some(
                    ResearchCandidateQualificationStatus::NotQualified,
                ),
                latest_watchlist_status: Some(ResearchCandidateWatchlistStatus::NeedsAttention),
            },
            Some("insufficient evidence"),
        )
        .expect("reject should be valid");

        assert_eq!(
            result.next_candidate_status,
            Some(ResearchCandidateStatus::Rejected)
        );
        assert_eq!(
            result.status,
            ResearchCandidateReviewStatus::CandidateStatusUpdated
        );
    }

    #[test]
    fn review_archive_moves_candidate_to_archived() {
        let result = research_candidate_review_outcome(
            ResearchCandidateStatus::Observing,
            ResearchCandidateReviewAction::ArchiveFromWatchlist,
            ResearchCandidateReviewContext {
                latest_qualification_status: Some(ResearchCandidateQualificationStatus::Unknown),
                latest_watchlist_status: Some(ResearchCandidateWatchlistStatus::Stable),
            },
            Some("completed review window"),
        )
        .expect("archive should be valid");

        assert_eq!(
            result.next_candidate_status,
            Some(ResearchCandidateStatus::Archived)
        );
        assert_eq!(
            result.status,
            ResearchCandidateReviewStatus::CandidateStatusUpdated
        );
    }

    fn sample_candidate_creation_input(
        triage_status: ResearchBatchTriageStatus,
        walk_forward_status: Option<StrategyWalkForwardRobustnessStatus>,
    ) -> ResearchCandidateCreationInput {
        ResearchCandidateCreationInput {
            source_batch_id: Some(Uuid::new_v4()),
            experiment_run_id: Uuid::new_v4(),
            source_walk_forward_run_id: None,
            source_robustness_matrix_run_id: None,
            walk_forward_status,
            batch_triage_status: triage_status,
            robustness_status: None,
            data_quality_status: Some(MarketDataQualityStatus::Good),
            normalized_strategy_config: None,
            config_fingerprint: None,
            evidence_status_summary: None,
            gate_evidence_mismatch: false,
            trade_count: 8,
            pnl_pct: Decimal::new(25, 1),
            score: Decimal::new(50, 0),
        }
    }

    #[test]
    fn candidate_creation_gate_blocks_overfit_only_by_default() {
        let policy = ResearchCandidateCreationPolicy::for_mode(
            ResearchCandidateCreationMode::CreateActionableOnly,
        );
        let decision = evaluate_research_candidate_creation(
            &policy,
            sample_candidate_creation_input(
                ResearchBatchTriageStatus::OverfitOnly,
                Some(StrategyWalkForwardRobustnessStatus::OverfitRisk),
            ),
        );

        assert!(!decision.should_create_candidate);
        assert!(decision.should_create_proposal);
        assert_eq!(
            decision.blockers,
            vec!["batch_triage_overfit_only", "walk_forward_overfit_risk"]
        );
    }

    #[test]
    fn candidate_creation_gate_blocks_weak_by_default() {
        let policy = ResearchCandidateCreationPolicy::for_mode(
            ResearchCandidateCreationMode::CreateActionableOnly,
        );
        let decision = evaluate_research_candidate_creation(
            &policy,
            sample_candidate_creation_input(
                ResearchBatchTriageStatus::Weak,
                Some(StrategyWalkForwardRobustnessStatus::Weak),
            ),
        );

        assert!(!decision.should_create_candidate);
        assert!(decision.blockers.contains(&"batch_triage_weak".to_string()));
        assert!(decision.blockers.contains(&"walk_forward_weak".to_string()));
    }

    #[test]
    fn candidate_creation_gate_allows_actionable() {
        let policy = ResearchCandidateCreationPolicy::for_mode(
            ResearchCandidateCreationMode::CreateActionableOnly,
        );
        let decision = evaluate_research_candidate_creation(
            &policy,
            sample_candidate_creation_input(
                ResearchBatchTriageStatus::Actionable,
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
        );

        assert!(decision.should_create_candidate);
        assert!(!decision.should_create_proposal);
        assert!(decision.blockers.is_empty());
    }

    #[test]
    fn candidate_creation_gate_create_all_override_creates_candidates() {
        let policy =
            ResearchCandidateCreationPolicy::for_mode(ResearchCandidateCreationMode::CreateAll);
        let mut input = sample_candidate_creation_input(
            ResearchBatchTriageStatus::OverfitOnly,
            Some(StrategyWalkForwardRobustnessStatus::OverfitRisk),
        );
        input.data_quality_status = Some(MarketDataQualityStatus::Bad);
        input.trade_count = 0;
        input.pnl_pct = Decimal::new(-1, 0);
        let decision = evaluate_research_candidate_creation(&policy, input);

        assert!(decision.should_create_candidate);
        assert!(decision
            .warnings
            .contains(&"create_all_override_bypassed_candidate_creation_gate".to_string()));
    }

    #[test]
    fn candidate_creation_gate_disabled_creates_none() {
        let policy =
            ResearchCandidateCreationPolicy::for_mode(ResearchCandidateCreationMode::Disabled);
        let decision = evaluate_research_candidate_creation(
            &policy,
            sample_candidate_creation_input(
                ResearchBatchTriageStatus::Actionable,
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
        );

        assert!(!decision.should_create_candidate);
        assert!(!decision.should_create_proposal);
        assert_eq!(decision.blockers, vec!["candidate_creation_disabled"]);
    }

    #[test]
    fn candidate_creation_gate_proposal_only_creates_proposals_not_candidates() {
        let policy =
            ResearchCandidateCreationPolicy::for_mode(ResearchCandidateCreationMode::ProposalOnly);
        let decision = evaluate_research_candidate_creation(
            &policy,
            sample_candidate_creation_input(
                ResearchBatchTriageStatus::Actionable,
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
        );

        assert!(!decision.should_create_candidate);
        assert!(decision.should_create_proposal);
        assert_eq!(decision.blockers, vec!["candidate_creation_proposal_only"]);
    }

    #[test]
    fn candidate_creation_gate_reason_order_is_deterministic() {
        let policy = ResearchCandidateCreationPolicy::for_mode(
            ResearchCandidateCreationMode::CreateActionableOnly,
        );
        let mut input = sample_candidate_creation_input(
            ResearchBatchTriageStatus::Weak,
            Some(StrategyWalkForwardRobustnessStatus::OverfitRisk),
        );
        input.data_quality_status = Some(MarketDataQualityStatus::Degraded);
        input.trade_count = 0;
        input.pnl_pct = Decimal::new(-1, 0);
        let first = evaluate_research_candidate_creation(&policy, input.clone());
        let second = evaluate_research_candidate_creation(&policy, input);

        assert_eq!(first.blockers, second.blockers);
        assert_eq!(first.reason, second.reason);
    }

    #[test]
    fn testnet_review_dossier_rejected_candidate_is_blocked() {
        let mut request = sample_testnet_review_request();
        request.candidate = Some(sample_research_candidate(ResearchCandidateStatus::Rejected));

        let dossier = evaluate_research_candidate_testnet_review_dossier(&request);

        assert_eq!(
            dossier.status,
            ResearchCandidateTestnetReviewStatus::Blocked
        );
        assert!(dossier
            .blockers
            .iter()
            .any(|item| item.contains("blocked for testnet review")));
    }

    #[test]
    fn testnet_review_dossier_without_observation_uses_shadow_evidence_but_requires_review() {
        let mut request = sample_testnet_review_request();
        request.latest_observation = None;
        request.observation_summary = None;
        request.observation_freshness = ResearchCandidateObservationFreshnessStatus::Unknown;
        request.observation_age_seconds = None;
        request.readiness_snapshot = None;

        let dossier = evaluate_research_candidate_testnet_review_dossier(&request);

        assert_eq!(
            dossier.status,
            ResearchCandidateTestnetReviewStatus::NotReady
        );
        assert!(dossier
            .warnings
            .iter()
            .any(|item| item.contains("valid linked shadow evidence")));
    }

    #[test]
    fn testnet_review_dossier_without_shadow_runs_is_blocked() {
        let mut request = sample_testnet_review_request();
        request.shadow_performance_summary = Some(qualification_performance(0, 0, 0, 0, 0));
        if let Some(evaluation) = request.latest_qualification_evaluation.as_mut() {
            evaluation.total_shadow_runs = 0;
            evaluation.would_submit_count = 0;
        }

        let dossier = evaluate_research_candidate_testnet_review_dossier(&request);

        assert_eq!(
            dossier.status,
            ResearchCandidateTestnetReviewStatus::Blocked
        );
        assert!(dossier
            .blockers
            .iter()
            .any(|item| item.contains("No linked shadow runs")));
    }

    #[test]
    fn testnet_review_dossier_blocks_no_signal_only_shadow_evidence_as_insufficient() {
        let mut request = sample_testnet_review_request();
        request.shadow_performance_summary = Some(evaluate_research_candidate_shadow_performance(
            Uuid::nil(),
            ResearchCandidateStatus::PromotedToShadowConfig,
            "momentum_v1",
            "BTCUSDT",
            "15m",
            ts(0, 0, 0),
            ts(1, 0, 0),
            2,
            0,
            1,
            0,
            1,
            0,
            Some(ts(1, 0, 0)),
            1,
            1,
            1,
            0,
            Some(ts(1, 0, 0)),
            Some(ts(1, 0, 0)),
            Some(ts(0, 30, 0)),
            true,
            ts(1, 0, 0),
        ));
        if let Some(evaluation) = request.latest_qualification_evaluation.as_mut() {
            evaluation.status = ResearchCandidateQualificationStatus::NotQualified;
            evaluation.total_shadow_runs = 2;
            evaluation.would_submit_count = 0;
        }
        request.observation_freshness = ResearchCandidateObservationFreshnessStatus::Stale;

        let dossier = evaluate_research_candidate_testnet_review_dossier(&request);

        assert_eq!(
            dossier.status,
            ResearchCandidateTestnetReviewStatus::Blocked
        );
        assert!(dossier
            .warnings
            .iter()
            .any(|item| item.contains("Formal observation")));
        assert!(dossier
            .warnings
            .iter()
            .any(|item| item.contains("Skipped shadow runs")));
        assert!(dossier
            .blockers
            .iter()
            .any(|item| item.contains("WOULD_SUBMIT evidence 0")));
        assert!(dossier
            .blockers
            .iter()
            .any(|item| item.contains("Independent linked shadow observations 1")));
        assert_eq!(
            dossier
                .evidence
                .shadow_performance_summary
                .as_ref()
                .map(|summary| summary.no_signal_count),
            Some(1)
        );
    }

    #[test]
    fn testnet_review_dossier_blocks_accepted_candidate_not_promoted() {
        let mut request = sample_testnet_review_request();
        request.candidate = Some(sample_research_candidate(
            ResearchCandidateStatus::AcceptedForShadow,
        ));

        let dossier = evaluate_research_candidate_testnet_review_dossier(&request);

        assert_eq!(
            dossier.status,
            ResearchCandidateTestnetReviewStatus::Blocked
        );
        assert!(dossier
            .blockers
            .iter()
            .any(|item| item == "Candidate is accepted but not promoted to shadow runner config."));
        let accepted = dossier
            .checklist
            .iter()
            .find(|item| item.code == "candidate_accepted_for_shadow")
            .expect("accepted checklist item");
        let promoted = dossier
            .checklist
            .iter()
            .find(|item| item.code == "candidate_promoted_to_shadow_runner_config")
            .expect("promoted checklist item");
        assert!(accepted.passed);
        assert!(!promoted.passed);
    }

    #[test]
    fn testnet_review_dossier_not_qualified_is_blocked() {
        let mut request = sample_testnet_review_request();
        if let Some(evaluation) = request.latest_qualification_evaluation.as_mut() {
            evaluation.status = ResearchCandidateQualificationStatus::NotQualified;
        }

        let dossier = evaluate_research_candidate_testnet_review_dossier(&request);

        assert_eq!(
            dossier.status,
            ResearchCandidateTestnetReviewStatus::Blocked
        );
        assert!(dossier
            .blockers
            .iter()
            .any(|item| item.contains("NOT_QUALIFIED")));
    }

    #[test]
    fn testnet_review_dossier_degraded_qualification_needs_operator_review() {
        let mut request = sample_testnet_review_request();
        if let Some(evaluation) = request.latest_qualification_evaluation.as_mut() {
            evaluation.status = ResearchCandidateQualificationStatus::Degraded;
        }

        let dossier = evaluate_research_candidate_testnet_review_dossier(&request);

        assert_eq!(
            dossier.status,
            ResearchCandidateTestnetReviewStatus::NeedsOperatorReview
        );
        assert!(dossier
            .warnings
            .iter()
            .any(|item| item.contains("DEGRADED")));
    }

    #[test]
    fn testnet_review_dossier_marked_ready_with_evidence_is_ready_for_review() {
        let dossier =
            evaluate_research_candidate_testnet_review_dossier(&sample_testnet_review_request());

        assert_eq!(
            dossier.status,
            ResearchCandidateTestnetReviewStatus::ReadyForReview
        );
        assert!(dossier.blockers.is_empty());
        assert_eq!(
            dossier.recommendations,
            vec![ResearchCandidateTestnetReviewRecommendation::ManualOperatorReview]
        );
    }

    #[test]
    fn testnet_review_dossier_checklist_order_is_deterministic() {
        let dossier =
            evaluate_research_candidate_testnet_review_dossier(&sample_testnet_review_request());

        assert_eq!(
            dossier
                .checklist
                .iter()
                .map(|item| item.code.as_str())
                .collect::<Vec<_>>(),
            vec![
                "experiment_exists",
                "candidate_observed",
                "candidate_accepted_for_shadow",
                "candidate_promoted_to_shadow_runner_config",
                "shadow_runs_linked",
                "qualification_evaluated",
                "walk_forward_validation_completed",
                "operator_reviewed",
                "no_execution_table_mutation",
                "latest_readiness_not_not_ready",
            ]
        );
    }

    #[test]
    fn research_candidate_shadow_promotion_confirmation_must_match_exact_candidate_id() {
        let candidate_id =
            Uuid::parse_str("1a5e9b4b-0a5a-4bb4-907d-49f2648b2b6f").expect("valid uuid");
        let expected = expected_research_candidate_shadow_promotion_confirmation(candidate_id);

        assert!(is_valid_research_candidate_shadow_promotion_confirmation(
            candidate_id,
            &expected
        ));
        assert!(!is_valid_research_candidate_shadow_promotion_confirmation(
            candidate_id,
            "PROMOTE CANDIDATE 1a5e9b4b-0a5a-4bb4-907d-49f2648b2b6f TO TESTNET"
        ));
    }

    #[test]
    fn research_candidate_accept_shadow_confirmation_must_match_exact_candidate_id() {
        let candidate_id =
            Uuid::parse_str("70867792-93df-494c-9a8b-d961c73107e4").expect("valid uuid");
        let expected = expected_research_candidate_accept_shadow_confirmation(candidate_id);

        assert_eq!(
            expected,
            "ACCEPT CANDIDATE 70867792-93df-494c-9a8b-d961c73107e4 FOR SHADOW"
        );
        assert!(is_valid_research_candidate_accept_shadow_confirmation(
            candidate_id,
            &expected
        ));
        assert!(!is_valid_research_candidate_accept_shadow_confirmation(
            candidate_id,
            "accept candidate 70867792-93df-494c-9a8b-d961c73107e4 for shadow"
        ));
    }

    fn sample_batch() -> ResearchBatchResult {
        ResearchBatchResult {
            batch_id: Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
            status: ResearchBatchStatus::Completed,
            steps: Vec::new(),
            provider_health_summary: None,
            backfill_summary: None,
            quality_before: None,
            repair_summary: None,
            quality_after: None,
            aggregation_summary: None,
            experiment_ids: Vec::new(),
            total_candidate_configs: 0,
            skipped_invalid_config_count: 0,
            executed_config_count: 0,
            invalid_config_examples: Vec::new(),
            walk_forward_run_ids: Vec::new(),
            created_candidate_ids: Vec::new(),
            candidates_blocked_by_gate: 0,
            proposals_created: 0,
            gate_decisions: Vec::new(),
            top_candidates: Vec::new(),
            recommendations: Vec::new(),
            created_at: ts(0, 0, 0),
            completed_at: Some(ts(1, 0, 0)),
        }
    }

    fn sample_candidate(
        index: u128,
        score: Decimal,
        pnl_pct: Decimal,
        walk_forward_status: Option<&str>,
        walk_forward_recommendation: Option<&str>,
    ) -> ResearchBatchCandidateTriage {
        ResearchBatchCandidateTriage {
            candidate_id: Some(Uuid::from_u128(index)),
            experiment_run_id: Uuid::from_u128(100 + index),
            walk_forward_run_id: Some(Uuid::from_u128(200 + index)),
            strategy_id: "trend_filter_momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "5m".to_string(),
            experiment_score: score,
            experiment_pnl_pct: pnl_pct,
            walk_forward_status: walk_forward_status.map(str::to_string),
            walk_forward_recommendation: walk_forward_recommendation.map(str::to_string),
            qualification_status: None,
            dossier_status: None,
            triage_status: ResearchBatchTriageStatus::Unknown,
            rank: 0,
            reasons: Vec::new(),
            recommendations: Vec::new(),
        }
    }

    fn sample_quality(status: MarketDataQualityStatus) -> MarketDataQualityReport {
        MarketDataQualityReport {
            exchange: MarketDataSource::Binance,
            symbol: "BTCUSDT".to_string(),
            interval: "1m".to_string(),
            window_start: ts(0, 0, 0),
            window_end: ts(1, 0, 0),
            expected_candle_count: 60,
            actual_candle_count: 60,
            closed_candle_count: 60,
            open_candle_count: 0,
            missing_candle_count: 0,
            coverage_pct: Decimal::new(100, 0),
            gap_count: 0,
            largest_gap_seconds: 0,
            gaps: Vec::new(),
            first_candle_time: Some(ts(0, 0, 0)),
            last_candle_time: Some(ts(0, 59, 0)),
            status,
            findings: Vec::new(),
            recommendations: Vec::new(),
        }
    }

    #[test]
    fn research_batch_triage_no_candidates() {
        let triage = build_research_batch_triage(&sample_batch(), Vec::new(), ts(2, 0, 0));

        assert_eq!(triage.status, ResearchBatchTriageStatus::NoCandidates);
        assert_eq!(triage.candidate_count, 0);
    }

    #[test]
    fn research_batch_triage_all_overfit() {
        let candidates = vec![
            sample_candidate(
                1,
                Decimal::new(10, 0),
                Decimal::new(2, 0),
                Some("OVERFIT_RISK"),
                Some("DO_NOT_ACCEPT"),
            ),
            sample_candidate(
                2,
                Decimal::new(8, 0),
                Decimal::new(1, 0),
                Some("OVERFIT_RISK"),
                None,
            ),
        ];
        let triage = build_research_batch_triage(&sample_batch(), candidates, ts(2, 0, 0));

        assert_eq!(triage.status, ResearchBatchTriageStatus::OverfitOnly);
        assert_eq!(triage.overfit_count, 2);
    }

    #[test]
    fn research_batch_triage_one_actionable() {
        let candidates = vec![
            sample_candidate(1, Decimal::new(1, 0), Decimal::ZERO, Some("WEAK"), None),
            sample_candidate(
                2,
                Decimal::new(10, 0),
                Decimal::new(2, 0),
                Some("ROBUST"),
                Some("REVIEW"),
            ),
        ];
        let triage = build_research_batch_triage(&sample_batch(), candidates, ts(2, 0, 0));

        assert_eq!(triage.status, ResearchBatchTriageStatus::Actionable);
        assert_eq!(triage.actionable_count, 1);
        assert_eq!(triage.candidates[0].candidate_id, Some(Uuid::from_u128(2)));
    }

    #[test]
    fn research_batch_triage_degraded_data_quality_warns_without_blocking() {
        let mut batch = sample_batch();
        batch.quality_after = Some(sample_quality(MarketDataQualityStatus::Degraded));
        let candidates = vec![sample_candidate(
            1,
            Decimal::new(10, 0),
            Decimal::new(2, 0),
            Some("ROBUST"),
            Some("REVIEW"),
        )];
        let triage = build_research_batch_triage(&batch, candidates, ts(2, 0, 0));

        assert_eq!(triage.status, ResearchBatchTriageStatus::Actionable);
        assert!(triage
            .findings
            .iter()
            .any(|finding| finding.code == "research_batch_degraded_market_data"));
    }

    #[test]
    fn research_batch_triage_bad_data_quality_blocks() {
        let mut batch = sample_batch();
        batch.quality_after = Some(sample_quality(MarketDataQualityStatus::Bad));
        let candidates = vec![sample_candidate(
            1,
            Decimal::new(10, 0),
            Decimal::new(2, 0),
            Some("ROBUST"),
            Some("REVIEW"),
        )];
        let triage = build_research_batch_triage(&batch, candidates, ts(2, 0, 0));

        assert_eq!(triage.status, ResearchBatchTriageStatus::DataQualityBlocked);
    }

    #[test]
    fn research_batch_triage_ranking_is_deterministic() {
        let candidates = vec![
            sample_candidate(
                3,
                Decimal::new(5, 0),
                Decimal::new(2, 0),
                Some("ROBUST"),
                None,
            ),
            sample_candidate(
                1,
                Decimal::new(5, 0),
                Decimal::new(2, 0),
                Some("ROBUST"),
                None,
            ),
            sample_candidate(
                2,
                Decimal::new(7, 0),
                Decimal::new(1, 0),
                Some("ROBUST"),
                None,
            ),
        ];
        let triage = build_research_batch_triage(&sample_batch(), candidates, ts(2, 0, 0));

        assert_eq!(
            triage
                .candidates
                .iter()
                .map(|candidate| (candidate.rank, candidate.candidate_id))
                .collect::<Vec<_>>(),
            vec![
                (1, Some(Uuid::from_u128(2))),
                (2, Some(Uuid::from_u128(1))),
                (3, Some(Uuid::from_u128(3))),
            ]
        );
    }

    fn sample_campaign_request() -> ResearchCampaignRequest {
        ResearchCampaignRequest {
            strategies: vec![
                "trend_filter_momentum_v1".to_string(),
                "volatility_breakout_v2".to_string(),
            ],
            symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
            experiment_timeframes: vec!["5m".to_string(), "15m".to_string()],
            windows: Vec::new(),
            campaign_start: Some(ts(0, 0, 0)),
            campaign_end: Some(ts(0, 0, 0) + Duration::hours(48)),
            window_hours: Some(24),
            step_hours: Some(24),
            initial_capital: Decimal::new(1000000, 0),
            fee_bps: Decimal::new(10, 0),
            slippage_bps: Decimal::new(5, 0),
            max_batches: None,
            regime_dataset_id: None,
            target_regimes: None,
            max_windows_per_regime: None,
            max_candidates_per_batch: 2,
            create_candidates: true,
            candidate_creation_mode: ResearchCandidateCreationMode::CreateActionableOnly,
            repair_degraded_data: true,
            walk_forward_top_n: 3,
            base_interval: "1m".to_string(),
            lookback_candidates: vec![10, 20],
            trend_lookback_candidates: None,
            momentum_lookback_candidates: Some(vec![2, 3]),
            compression_lookback_candidates: None,
            breakout_lookback_candidates: None,
            pullback_lookback_candidates: None,
            pullback_sma_lookback_candidates: None,
            compression_percentile_threshold_candidates: None,
            min_breakout_pct_candidates: None,
            max_breakout_extension_pct_candidates: None,
            min_volume_expansion_ratio_candidates: None,
            lower_band_pct_candidates: None,
            upper_band_pct_candidates: None,
            min_range_width_pct_candidates: None,
            max_range_width_pct_candidates: None,
            min_close_above_sma_pct_candidates: None,
            max_close_above_sma_pct_candidates: None,
            min_momentum_return_pct_candidates: None,
            min_trend_return_pct_candidates: None,
            min_trend_slope_pct_candidates: None,
            min_pullback_depth_pct_candidates: None,
            max_pullback_depth_pct_candidates: None,
            min_reclaim_pct_candidates: None,
            min_breakdown_pct_candidates: None,
            min_reclaim_close_pct_candidates: None,
            min_lower_wick_pct_candidates: None,
            min_volume_ratio_candidates: None,
            max_choppiness_candidates: None,
            holding_candles_candidates: None,
            correlation_id: None,
        }
    }

    fn campaign_batch(
        plan_index: i32,
        status: ResearchBatchTriageStatus,
        score: Decimal,
        pnl_pct: Decimal,
    ) -> ResearchCampaignBatchResult {
        ResearchCampaignBatchResult {
            plan: ResearchCampaignBatchPlan {
                plan_index,
                strategy_id: "trend_filter_momentum_v1".to_string(),
                symbol: "BTCUSDT".to_string(),
                timeframe: "5m".to_string(),
                start_time: ts(1, 0, 0),
                end_time: ts(2, 0, 0),
                regime_label: None,
            },
            research_batch_id: Some(Uuid::from_u128(plan_index as u128)),
            batch_status: Some(if status == ResearchBatchTriageStatus::Failed {
                ResearchBatchStatus::Failed
            } else {
                ResearchBatchStatus::Completed
            }),
            triage_status: status,
            candidates_created: 1,
            total_candidate_configs: 0,
            skipped_invalid_config_count: 0,
            executed_config_count: 0,
            invalid_config_examples: Vec::new(),
            candidates_blocked_by_gate: 0,
            proposals_created: 0,
            gate_decisions: Vec::new(),
            top_candidates: vec![ResearchBatchCandidateSummary {
                experiment_id: Uuid::from_u128(100 + plan_index as u128),
                experiment_run_id: Uuid::from_u128(200 + plan_index as u128),
                walk_forward_run_id: None,
                candidate_id: Some(Uuid::from_u128(300 + plan_index as u128)),
                strategy_id: "trend_filter_momentum_v1".to_string(),
                symbol: "BTCUSDT".to_string(),
                timeframe: "5m".to_string(),
                score,
                pnl_pct,
                max_drawdown_pct: Decimal::new(1, 0),
                trade_count: 3,
                win_rate: Decimal::new(50, 0),
                robustness_status: None,
            }],
            error: if status == ResearchBatchTriageStatus::Failed {
                Some("failed".to_string())
            } else {
                None
            },
            started_at: ts(1, 0, 0),
            completed_at: Some(ts(1, 1, 0)),
        }
    }

    fn leaderboard_batch(
        plan_index: i32,
        strategy_id: &str,
        symbol: &str,
        timeframe: &str,
        regime_label: ResearchRegimeLabel,
        triage_status: ResearchBatchTriageStatus,
        pnl_pct: Decimal,
        robustness_status: Option<StrategyWalkForwardRobustnessStatus>,
    ) -> ResearchCampaignBatchResult {
        let mut batch = campaign_batch(plan_index, triage_status, pnl_pct, pnl_pct);
        batch.plan.strategy_id = strategy_id.to_string();
        batch.plan.symbol = symbol.to_string();
        batch.plan.timeframe = timeframe.to_string();
        batch.plan.regime_label = Some(regime_label);
        batch.candidates_created = 1;
        batch.top_candidates[0].strategy_id = strategy_id.to_string();
        batch.top_candidates[0].symbol = symbol.to_string();
        batch.top_candidates[0].timeframe = timeframe.to_string();
        batch.top_candidates[0].pnl_pct = pnl_pct;
        batch.top_candidates[0].score = pnl_pct;
        batch.top_candidates[0].robustness_status = robustness_status;
        batch
    }

    fn leaderboard_campaign(batches: Vec<ResearchCampaignBatchResult>) -> ResearchCampaignResult {
        ResearchCampaignResult {
            campaign_id: Uuid::from_u128(42),
            status: ResearchCampaignStatus::Completed,
            request: sample_campaign_request(),
            summary: summarize_research_campaign(batches.len(), &batches),
            batches,
            created_at: ts(0, 0, 0),
            completed_at: Some(ts(3, 0, 0)),
        }
    }

    fn regime_candles(closes: &[i64]) -> Vec<Candle> {
        closes
            .windows(2)
            .enumerate()
            .map(|(index, pair)| {
                let open = pair[0];
                let close = pair[1];
                Candle {
                    id: Uuid::new_v4(),
                    exchange: MarketDataSource::Binance,
                    symbol: Symbol::new("BTCUSDT").unwrap(),
                    interval: CandleInterval::OneMinute,
                    open_time: ts(0, index as u32, 0),
                    close_time: ts(0, index as u32, 59),
                    open: Decimal::new(open, 0),
                    high: Decimal::new(open.max(close), 0),
                    low: Decimal::new(open.min(close), 0),
                    close: Decimal::new(close, 0),
                    volume: Decimal::ONE,
                    quote_volume: None,
                    trade_count: 1,
                    is_closed: true,
                    created_at: ts(0, index as u32, 59),
                    updated_at: ts(0, index as u32, 59),
                }
            })
            .collect()
    }

    fn decimal_regime_candles(closes: &[&str]) -> Vec<Candle> {
        closes
            .windows(2)
            .enumerate()
            .map(|(index, pair)| {
                let open = pair[0].parse::<Decimal>().unwrap();
                let close = pair[1].parse::<Decimal>().unwrap();
                Candle {
                    id: Uuid::new_v4(),
                    exchange: MarketDataSource::Binance,
                    symbol: Symbol::new("BTCUSDT").unwrap(),
                    interval: CandleInterval::OneMinute,
                    open_time: ts(0, index as u32, 0),
                    close_time: ts(0, index as u32, 59),
                    open,
                    high: open.max(close),
                    low: open.min(close),
                    close,
                    volume: Decimal::ONE,
                    quote_volume: None,
                    trade_count: 1,
                    is_closed: true,
                    created_at: ts(0, index as u32, 59),
                    updated_at: ts(0, index as u32, 59),
                }
            })
            .collect()
    }

    fn dataset_range_candles(hours: u32) -> Vec<Candle> {
        let mut candles = Vec::new();
        for hour in 0..hours {
            for minute in 0..6 {
                let open = if minute % 2 == 0 {
                    Decimal::new(100, 0)
                } else {
                    Decimal::new(101, 0)
                };
                let close = if minute % 2 == 0 {
                    Decimal::new(101, 0)
                } else {
                    Decimal::new(100, 0)
                };
                candles.push(Candle {
                    id: Uuid::new_v4(),
                    exchange: MarketDataSource::Binance,
                    symbol: Symbol::new("BTCUSDT").unwrap(),
                    interval: CandleInterval::OneMinute,
                    open_time: ts(hour, minute, 0),
                    close_time: ts(hour, minute, 59),
                    open,
                    high: open.max(close),
                    low: open.min(close),
                    close,
                    volume: Decimal::ONE,
                    quote_volume: None,
                    trade_count: 1,
                    is_closed: true,
                    created_at: ts(hour, minute, 59),
                    updated_at: ts(hour, minute, 59),
                });
            }
        }
        candles
    }

    fn regime_dataset_request(max_windows_per_regime: Option<u32>) -> ResearchRegimeDatasetRequest {
        ResearchRegimeDatasetRequest {
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
            start_time: ts(0, 0, 0),
            end_time: ts(3, 0, 0),
            window_hours: 1,
            step_hours: 1,
            min_candles_per_window: 5,
            target_regimes: Some(vec![ResearchRegimeLabel::Range]),
            max_windows_per_regime,
            require_good_data_quality: false,
            classifier_config: None,
        }
    }

    fn sample_failure_input() -> ResearchCandidateFailureInput {
        let candles = regime_candles(&[100, 101, 100, 101, 100, 101, 100]);
        ResearchCandidateFailureInput {
            candidate_id: Some(Uuid::from_u128(1)),
            experiment_run_id: Some(Uuid::from_u128(2)),
            walk_forward_run_id: Some(Uuid::from_u128(3)),
            strategy_id: "trend_filter_momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "5m".to_string(),
            window_start: ts(0, 0, 0),
            window_end: ts(1, 0, 0),
            regime_metric: classify_research_regime(
                "BTCUSDT",
                "5m",
                ts(0, 0, 0),
                ts(1, 0, 0),
                &candles,
            ),
            pnl_pct: Some(Decimal::new(-1, 0)),
            gross_pnl_pct: Some(Decimal::new(-1, 0)),
            fee_drag_pct: Some(Decimal::ZERO),
            trade_count: Some(5),
            win_rate: Some(Decimal::new(50, 0)),
            max_drawdown_pct: Some(Decimal::new(1, 0)),
            walk_forward_status: Some("WEAK".to_string()),
            walk_forward_profitable_windows: Some(1),
            walk_forward_losing_windows: Some(1),
            data_quality_status: Some(MarketDataQualityStatus::Good),
        }
    }

    #[test]
    fn research_campaign_plan_expansion_cross_joins_inputs_and_windows() {
        let request = sample_campaign_request();
        let plans = expand_research_campaign(&request).expect("campaign should expand");

        assert_eq!(plans.len(), 16);
        assert_eq!(plans[0].strategy_id, "trend_filter_momentum_v1");
        assert_eq!(plans[0].symbol, "BTCUSDT");
        assert_eq!(plans[0].timeframe, "5m");
        assert_eq!(plans[0].start_time, ts(0, 0, 0));
        assert_eq!(plans[1].start_time, ts(0, 0, 0) + Duration::hours(24));
    }

    #[test]
    fn research_campaign_can_include_range_reversion() {
        let mut request = sample_campaign_request();
        request.strategies = vec!["range_reversion_v1".to_string()];
        request.lower_band_pct_candidates = Some(vec![Decimal::new(10, 0), Decimal::new(20, 0)]);

        let plans = expand_research_campaign(&request).expect("campaign should expand");

        assert!(plans
            .iter()
            .all(|plan| plan.strategy_id == "range_reversion_v1"));
        assert_eq!(plans.len(), 8);
    }

    #[test]
    fn research_campaign_max_batches_is_enforced() {
        let mut request = sample_campaign_request();
        request.max_batches = Some(3);
        let plans = expand_research_campaign(&request).expect("campaign should expand");

        assert_eq!(plans.len(), 3);
        assert_eq!(plans[2].plan_index, 3);
    }

    #[test]
    fn research_campaign_failed_batch_leads_partial_success() {
        let batches = vec![
            campaign_batch(
                1,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(8, 0),
                Decimal::new(2, 0),
            ),
            campaign_batch(
                2,
                ResearchBatchTriageStatus::Failed,
                Decimal::ZERO,
                Decimal::ZERO,
            ),
        ];
        let summary = summarize_research_campaign(2, &batches);

        assert_eq!(
            status_from_campaign_summary(&summary),
            ResearchCampaignStatus::PartialSuccess
        );
        assert_eq!(summary.total_batches_failed, 1);
    }

    #[test]
    fn research_campaign_all_failed_leads_failed() {
        let batches = vec![
            campaign_batch(
                1,
                ResearchBatchTriageStatus::Failed,
                Decimal::ZERO,
                Decimal::ZERO,
            ),
            campaign_batch(
                2,
                ResearchBatchTriageStatus::Failed,
                Decimal::ZERO,
                Decimal::ZERO,
            ),
        ];
        let summary = summarize_research_campaign(2, &batches);

        assert_eq!(
            status_from_campaign_summary(&summary),
            ResearchCampaignStatus::Failed
        );
    }

    #[test]
    fn research_campaign_summary_counts_triage_buckets() {
        let batches = vec![
            campaign_batch(
                1,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(8, 0),
                Decimal::new(2, 0),
            ),
            campaign_batch(
                2,
                ResearchBatchTriageStatus::OverfitOnly,
                Decimal::new(7, 0),
                Decimal::new(3, 0),
            ),
            campaign_batch(
                3,
                ResearchBatchTriageStatus::Weak,
                Decimal::new(1, 0),
                Decimal::new(0, 0),
            ),
        ];
        let summary = summarize_research_campaign(3, &batches);

        assert_eq!(summary.actionable_batches, 1);
        assert_eq!(summary.overfit_only_batches, 1);
        assert_eq!(summary.weak_batches, 1);
        assert_eq!(summary.candidates_created, 3);
    }

    #[test]
    fn research_campaign_summary_includes_skipped_invalid_config_count() {
        let mut batch = campaign_batch(
            1,
            ResearchBatchTriageStatus::Weak,
            Decimal::new(1, 0),
            Decimal::ZERO,
        );
        batch.total_candidate_configs = 6;
        batch.executed_config_count = 3;
        batch.skipped_invalid_config_count = 3;

        let summary = summarize_research_campaign(1, &[batch]);

        assert_eq!(summary.total_candidate_configs, 6);
        assert_eq!(summary.executed_config_count, 3);
        assert_eq!(summary.skipped_invalid_config_count, 3);
        assert!(summary
            .findings
            .iter()
            .any(|finding| { finding.code == "invalid_strategy_configs_skipped" }));
    }

    #[test]
    fn research_campaign_top_candidate_ranking_is_deterministic() {
        let batches = vec![
            campaign_batch(
                2,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(5, 0),
                Decimal::new(3, 0),
            ),
            campaign_batch(
                1,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(5, 0),
                Decimal::new(3, 0),
            ),
            campaign_batch(
                3,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(8, 0),
                Decimal::new(1, 0),
            ),
        ];
        let summary = summarize_research_campaign(3, &batches);

        assert_eq!(
            summary
                .top_candidates
                .iter()
                .map(|candidate| candidate.experiment_run_id)
                .collect::<Vec<_>>(),
            vec![
                Uuid::from_u128(203),
                Uuid::from_u128(201),
                Uuid::from_u128(202)
            ]
        );
    }

    #[test]
    fn regime_leaderboard_median_ranks_above_single_best_outlier() {
        let campaign = leaderboard_campaign(vec![
            leaderboard_batch(
                1,
                "steady_strategy",
                "BTCUSDT",
                "5m",
                ResearchRegimeLabel::TrendUp,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(4, 0),
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
            leaderboard_batch(
                2,
                "steady_strategy",
                "BTCUSDT",
                "5m",
                ResearchRegimeLabel::TrendUp,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(5, 0),
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
            leaderboard_batch(
                3,
                "outlier_strategy",
                "BTCUSDT",
                "5m",
                ResearchRegimeLabel::TrendUp,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(100, 0),
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
        ]);

        let leaderboard = build_research_regime_strategy_leaderboard(&campaign, ts(4, 0, 0));
        let rankings = &leaderboard.per_regime[0].rankings;

        assert_eq!(rankings[0].strategy_id, "steady_strategy");
        assert_eq!(rankings[1].strategy_id, "outlier_strategy");
        assert_eq!(
            rankings[1].status,
            ResearchRegimeStrategyStatus::InsufficientData
        );
    }

    #[test]
    fn regime_leaderboard_overfit_penalty_lowers_rank() {
        let campaign = leaderboard_campaign(vec![
            leaderboard_batch(
                1,
                "robust_strategy",
                "BTCUSDT",
                "5m",
                ResearchRegimeLabel::Range,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(2, 0),
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
            leaderboard_batch(
                2,
                "robust_strategy",
                "BTCUSDT",
                "5m",
                ResearchRegimeLabel::Range,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(2, 0),
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
            leaderboard_batch(
                3,
                "overfit_strategy",
                "BTCUSDT",
                "5m",
                ResearchRegimeLabel::Range,
                ResearchBatchTriageStatus::OverfitOnly,
                Decimal::new(2, 0),
                Some(StrategyWalkForwardRobustnessStatus::OverfitRisk),
            ),
            leaderboard_batch(
                4,
                "overfit_strategy",
                "BTCUSDT",
                "5m",
                ResearchRegimeLabel::Range,
                ResearchBatchTriageStatus::OverfitOnly,
                Decimal::new(2, 0),
                Some(StrategyWalkForwardRobustnessStatus::OverfitRisk),
            ),
        ]);

        let leaderboard = build_research_regime_strategy_leaderboard(&campaign, ts(4, 0, 0));
        let rankings = &leaderboard.per_regime[0].rankings;

        assert_eq!(rankings[0].strategy_id, "robust_strategy");
        assert_eq!(rankings[1].strategy_id, "overfit_strategy");
        assert_eq!(rankings[1].status, ResearchRegimeStrategyStatus::Overfit);
    }

    #[test]
    fn regime_leaderboard_insufficient_samples_are_penalized() {
        let campaign = leaderboard_campaign(vec![
            leaderboard_batch(
                1,
                "sampled_strategy",
                "BTCUSDT",
                "15m",
                ResearchRegimeLabel::HighVolatility,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(1, 0),
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
            leaderboard_batch(
                2,
                "sampled_strategy",
                "BTCUSDT",
                "15m",
                ResearchRegimeLabel::HighVolatility,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(1, 0),
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
            leaderboard_batch(
                3,
                "thin_strategy",
                "BTCUSDT",
                "15m",
                ResearchRegimeLabel::HighVolatility,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(20, 0),
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
        ]);

        let leaderboard = build_research_regime_strategy_leaderboard(&campaign, ts(4, 0, 0));
        let thin = leaderboard.per_regime[0]
            .rankings
            .iter()
            .find(|ranking| ranking.strategy_id == "thin_strategy")
            .unwrap();

        assert_eq!(thin.status, ResearchRegimeStrategyStatus::InsufficientData);
        assert!(thin.rank > 1);
    }

    #[test]
    fn regime_leaderboard_negative_median_yields_negative() {
        let campaign = leaderboard_campaign(vec![
            leaderboard_batch(
                1,
                "losing_strategy",
                "ETHUSDT",
                "5m",
                ResearchRegimeLabel::TrendDown,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(-2, 0),
                Some(StrategyWalkForwardRobustnessStatus::Weak),
            ),
            leaderboard_batch(
                2,
                "losing_strategy",
                "ETHUSDT",
                "5m",
                ResearchRegimeLabel::TrendDown,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(-1, 0),
                Some(StrategyWalkForwardRobustnessStatus::Weak),
            ),
        ]);

        let leaderboard = build_research_regime_strategy_leaderboard(&campaign, ts(4, 0, 0));

        assert_eq!(
            leaderboard.per_regime[0].rankings[0].status,
            ResearchRegimeStrategyStatus::Negative
        );
    }

    #[test]
    fn regime_leaderboard_ranking_order_is_deterministic() {
        let campaign = leaderboard_campaign(vec![
            leaderboard_batch(
                2,
                "alpha_b",
                "BTCUSDT",
                "5m",
                ResearchRegimeLabel::LowVolatility,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(1, 0),
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
            leaderboard_batch(
                1,
                "alpha_a",
                "BTCUSDT",
                "5m",
                ResearchRegimeLabel::LowVolatility,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(1, 0),
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
            leaderboard_batch(
                4,
                "alpha_b",
                "BTCUSDT",
                "5m",
                ResearchRegimeLabel::LowVolatility,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(1, 0),
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
            leaderboard_batch(
                3,
                "alpha_a",
                "BTCUSDT",
                "5m",
                ResearchRegimeLabel::LowVolatility,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(1, 0),
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
        ]);

        let first = build_research_regime_strategy_leaderboard(&campaign, ts(4, 0, 0));
        let second = build_research_regime_strategy_leaderboard(&campaign, ts(5, 0, 0));

        assert_eq!(
            first.per_regime[0]
                .rankings
                .iter()
                .map(|ranking| ranking.strategy_id.clone())
                .collect::<Vec<_>>(),
            second.per_regime[0]
                .rankings
                .iter()
                .map(|ranking| ranking.strategy_id.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(first.per_regime[0].rankings[0].strategy_id, "alpha_a");
    }

    #[test]
    fn regime_leaderboard_selects_per_regime_best() {
        let campaign = leaderboard_campaign(vec![
            leaderboard_batch(
                1,
                "trend_strategy",
                "BTCUSDT",
                "5m",
                ResearchRegimeLabel::TrendUp,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(3, 0),
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
            leaderboard_batch(
                2,
                "trend_strategy",
                "BTCUSDT",
                "5m",
                ResearchRegimeLabel::TrendUp,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(3, 0),
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
            leaderboard_batch(
                3,
                "range_strategy",
                "ETHUSDT",
                "15m",
                ResearchRegimeLabel::Range,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(4, 0),
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
            leaderboard_batch(
                4,
                "range_strategy",
                "ETHUSDT",
                "15m",
                ResearchRegimeLabel::Range,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(4, 0),
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
        ]);

        let leaderboard = build_research_regime_strategy_leaderboard(&campaign, ts(4, 0, 0));

        assert_eq!(leaderboard.best_strategy_by_regime.len(), 2);
        assert!(leaderboard.best_strategy_by_regime.iter().any(|selection| {
            selection.regime_label == ResearchRegimeLabel::TrendUp
                && selection.strategy_id == "trend_strategy"
        }));
        assert!(leaderboard.best_strategy_by_regime.iter().any(|selection| {
            selection.regime_label == ResearchRegimeLabel::Range
                && selection.strategy_id == "range_strategy"
        }));
    }

    #[test]
    fn regime_leaderboard_overfit_zero_score_is_not_promising() {
        let campaign = leaderboard_campaign(vec![
            leaderboard_batch(
                1,
                "overfit_strategy",
                "BTCUSDT",
                "15m",
                ResearchRegimeLabel::Range,
                ResearchBatchTriageStatus::OverfitOnly,
                Decimal::ZERO,
                Some(StrategyWalkForwardRobustnessStatus::OverfitRisk),
            ),
            leaderboard_batch(
                2,
                "overfit_strategy",
                "BTCUSDT",
                "15m",
                ResearchRegimeLabel::Range,
                ResearchBatchTriageStatus::OverfitOnly,
                Decimal::ZERO,
                Some(StrategyWalkForwardRobustnessStatus::OverfitRisk),
            ),
        ]);

        let leaderboard = build_research_regime_strategy_leaderboard(&campaign, ts(4, 0, 0));
        let selection = &leaderboard.best_strategy_by_regime[0];

        assert_eq!(selection.status, ResearchRegimeStrategyStatus::Overfit);
        assert_eq!(selection.score, 0);
        assert_eq!(selection.robustness_score, 0);
        assert!(!selection.is_promising);
        assert!(selection.is_least_bad);
        assert!(leaderboard.overall_promising.is_none());
        assert_eq!(
            leaderboard
                .overall_least_bad
                .as_ref()
                .map(|ranking| ranking.strategy_id.as_str()),
            Some("overfit_strategy")
        );
        assert!(!leaderboard
            .findings
            .iter()
            .any(|finding| finding.code == "regime_strategy_promising"));
    }

    #[test]
    fn regime_leaderboard_reports_least_bad_separately_from_promising() {
        let campaign = leaderboard_campaign(vec![
            leaderboard_batch(
                1,
                "losing_strategy",
                "ETHUSDT",
                "5m",
                ResearchRegimeLabel::TrendDown,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(-2, 0),
                Some(StrategyWalkForwardRobustnessStatus::Weak),
            ),
            leaderboard_batch(
                2,
                "losing_strategy",
                "ETHUSDT",
                "5m",
                ResearchRegimeLabel::TrendDown,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(-1, 0),
                Some(StrategyWalkForwardRobustnessStatus::Weak),
            ),
        ]);

        let leaderboard = build_research_regime_strategy_leaderboard(&campaign, ts(4, 0, 0));

        assert!(leaderboard.overall_promising.is_none());
        assert_eq!(
            leaderboard
                .overall_least_bad
                .as_ref()
                .map(|ranking| ranking.strategy_id.as_str()),
            Some("losing_strategy")
        );
        assert!(leaderboard.best_strategy_by_regime[0].is_least_bad);
        assert!(leaderboard.findings.iter().any(|finding| {
            finding.code == "regime_least_bad_strategy_identified"
                && finding.message.contains("not promising")
        }));
    }

    #[test]
    fn regime_leaderboard_promising_finding_only_for_promising_or_robust_status() {
        let campaign = leaderboard_campaign(vec![
            leaderboard_batch(
                1,
                "robust_strategy",
                "BTCUSDT",
                "5m",
                ResearchRegimeLabel::TrendUp,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(3, 0),
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
            leaderboard_batch(
                2,
                "robust_strategy",
                "BTCUSDT",
                "5m",
                ResearchRegimeLabel::TrendUp,
                ResearchBatchTriageStatus::Actionable,
                Decimal::new(3, 0),
                Some(StrategyWalkForwardRobustnessStatus::Robust),
            ),
        ]);

        let leaderboard = build_research_regime_strategy_leaderboard(&campaign, ts(4, 0, 0));

        assert!(matches!(
            leaderboard.best_strategy_by_regime[0].status,
            ResearchRegimeStrategyStatus::Robust | ResearchRegimeStrategyStatus::Promising
        ));
        assert!(leaderboard.best_strategy_by_regime[0].is_promising);
        assert!(leaderboard.overall_promising.is_some());
        assert!(leaderboard
            .findings
            .iter()
            .any(|finding| finding.code == "regime_strategy_promising"));
    }

    #[test]
    fn regime_leaderboard_finding_order_is_deterministic_for_negative_results() {
        let campaign = leaderboard_campaign(vec![
            leaderboard_batch(
                1,
                "overfit_strategy",
                "BTCUSDT",
                "5m",
                ResearchRegimeLabel::Range,
                ResearchBatchTriageStatus::OverfitOnly,
                Decimal::ZERO,
                Some(StrategyWalkForwardRobustnessStatus::OverfitRisk),
            ),
            leaderboard_batch(
                2,
                "overfit_strategy",
                "BTCUSDT",
                "5m",
                ResearchRegimeLabel::Range,
                ResearchBatchTriageStatus::OverfitOnly,
                Decimal::ZERO,
                Some(StrategyWalkForwardRobustnessStatus::OverfitRisk),
            ),
        ]);

        let leaderboard = build_research_regime_strategy_leaderboard(&campaign, ts(4, 0, 0));

        assert_eq!(
            leaderboard
                .findings
                .iter()
                .map(|finding| finding.code.as_str())
                .collect::<Vec<_>>(),
            vec![
                "regime_least_bad_strategy_identified",
                "regime_no_promising_strategy",
                "regime_overfit_heavy"
            ]
        );
    }

    #[test]
    fn regime_classification_detects_trend_up() {
        let candles = regime_candles(&[100, 102, 104, 106, 108, 110, 112]);
        let metric = classify_research_regime("BTCUSDT", "5m", ts(0, 0, 0), ts(1, 0, 0), &candles);

        assert_eq!(metric.label, ResearchRegimeLabel::TrendUp);
        assert!(metric.return_pct > Decimal::new(5, 0));
    }

    #[test]
    fn regime_classification_detects_trend_down() {
        let candles = regime_candles(&[112, 110, 108, 106, 104, 102, 100]);
        let metric = classify_research_regime("BTCUSDT", "5m", ts(0, 0, 0), ts(1, 0, 0), &candles);

        assert_eq!(metric.label, ResearchRegimeLabel::TrendDown);
    }

    #[test]
    fn regime_classification_detects_range() {
        let candles = regime_candles(&[100, 101, 100, 101, 100, 101, 100]);
        let metric = classify_research_regime("BTCUSDT", "5m", ts(0, 0, 0), ts(1, 0, 0), &candles);

        assert_eq!(metric.label, ResearchRegimeLabel::Range);
    }

    #[test]
    fn regime_classification_detects_high_volatility() {
        let candles = regime_candles(&[100, 120, 90, 125, 85, 130, 80]);
        let metric = classify_research_regime("BTCUSDT", "5m", ts(0, 0, 0), ts(1, 0, 0), &candles);

        assert_eq!(metric.label, ResearchRegimeLabel::HighVolatility);
    }

    #[test]
    fn regime_classification_detects_low_volatility() {
        let candles = decimal_regime_candles(&["100", "100.4", "100.8", "101.2", "101.6", "102.0"]);
        let metric = classify_research_regime("BTCUSDT", "5m", ts(0, 0, 0), ts(1, 0, 0), &candles);

        assert_eq!(metric.label, ResearchRegimeLabel::LowVolatility);
    }

    #[test]
    fn regime_explanation_includes_thresholds_and_pass_fail_reasons() {
        let candles = regime_candles(&[100, 102, 104, 106, 108, 110, 112]);
        let metric = classify_research_regime("BTCUSDT", "5m", ts(0, 0, 0), ts(1, 0, 0), &candles);

        assert_eq!(metric.explanation.final_label, ResearchRegimeLabel::TrendUp);
        assert_eq!(
            metric
                .explanation
                .thresholds_used
                .trend_return_threshold_pct,
            Decimal::new(3, 0)
        );
        assert!(metric
            .explanation
            .conditions
            .iter()
            .any(|condition| condition.passed && !condition.reason.is_empty()));
        assert!(metric
            .explanation
            .conditions
            .iter()
            .any(|condition| !condition.passed && !condition.reason.is_empty()));
    }

    #[test]
    fn threshold_config_can_classify_known_trend_up_fixture() {
        let candles = regime_candles(&[100, 101, 102, 103, 104, 105]);
        let config = ResearchRegimeClassifierConfig {
            trend_return_threshold_pct: Decimal::ONE,
            high_volatility_threshold_pct: Decimal::new(10, 0),
            ..Default::default()
        };
        let metric = classify_research_regime_with_config(
            "BTCUSDT",
            "5m",
            ts(0, 0, 0),
            ts(1, 0, 0),
            &candles,
            &config,
        );

        assert_eq!(metric.label, ResearchRegimeLabel::TrendUp);
    }

    #[test]
    fn threshold_config_can_classify_known_trend_down_fixture() {
        let candles = regime_candles(&[105, 104, 103, 102, 101, 100]);
        let config = ResearchRegimeClassifierConfig {
            trend_return_threshold_pct: Decimal::ONE,
            high_volatility_threshold_pct: Decimal::new(10, 0),
            ..Default::default()
        };
        let metric = classify_research_regime_with_config(
            "BTCUSDT",
            "5m",
            ts(0, 0, 0),
            ts(1, 0, 0),
            &candles,
            &config,
        );

        assert_eq!(metric.label, ResearchRegimeLabel::TrendDown);
    }

    #[test]
    fn threshold_config_can_classify_known_high_vol_fixture() {
        let candles = regime_candles(&[100, 112, 98, 115, 95, 118]);
        let config = ResearchRegimeClassifierConfig {
            high_volatility_threshold_pct: Decimal::new(5, 0),
            ..Default::default()
        };
        let metric = classify_research_regime_with_config(
            "BTCUSDT",
            "5m",
            ts(0, 0, 0),
            ts(1, 0, 0),
            &candles,
            &config,
        );

        assert_eq!(metric.label, ResearchRegimeLabel::HighVolatility);
    }

    #[test]
    fn threshold_config_can_classify_known_low_vol_fixture() {
        let candles = decimal_regime_candles(&["100", "100.1", "100.2", "100.3", "100.4", "100.5"]);
        let config = ResearchRegimeClassifierConfig {
            range_return_max_pct: Decimal::new(1, 1),
            range_choppiness_min: Decimal::new(90, 0),
            low_volatility_threshold_pct: Decimal::new(5, 1),
            priority_order: vec![
                ResearchRegimeLabel::LowVolatility,
                ResearchRegimeLabel::Range,
                ResearchRegimeLabel::HighVolatility,
                ResearchRegimeLabel::TrendUp,
                ResearchRegimeLabel::TrendDown,
            ],
            ..Default::default()
        };
        let metric = classify_research_regime_with_config(
            "BTCUSDT",
            "5m",
            ts(0, 0, 0),
            ts(1, 0, 0),
            &candles,
            &config,
        );

        assert_eq!(metric.label, ResearchRegimeLabel::LowVolatility);
    }

    #[test]
    fn threshold_config_can_classify_known_range_fixture() {
        let candles = regime_candles(&[100, 101, 100, 101, 100, 101]);
        let metric = classify_research_regime_with_config(
            "BTCUSDT",
            "5m",
            ts(0, 0, 0),
            ts(1, 0, 0),
            &candles,
            &ResearchRegimeClassifierConfig::default(),
        );

        assert_eq!(metric.label, ResearchRegimeLabel::Range);
    }

    #[test]
    fn overlapping_regime_labels_resolve_by_priority_order() {
        let candles = decimal_regime_candles(&["100", "100.1", "100.0", "100.1", "100.0", "100.1"]);
        let config = ResearchRegimeClassifierConfig {
            priority_order: vec![
                ResearchRegimeLabel::LowVolatility,
                ResearchRegimeLabel::Range,
                ResearchRegimeLabel::HighVolatility,
            ],
            ..Default::default()
        };
        let metric = classify_research_regime_with_config(
            "BTCUSDT",
            "5m",
            ts(0, 0, 0),
            ts(1, 0, 0),
            &candles,
            &config,
        );

        assert_eq!(metric.label, ResearchRegimeLabel::LowVolatility);
        assert!(metric
            .explanation
            .alternate_labels_considered
            .contains(&ResearchRegimeLabel::Range));
    }

    #[test]
    fn regime_dataset_window_generation_is_deterministic() {
        let candles = dataset_range_candles(3);
        let request = regime_dataset_request(None);

        let first = build_research_regime_dataset(
            Uuid::from_u128(1),
            request.clone(),
            &candles,
            ts(3, 0, 0),
        )
        .expect("dataset should build");
        let second =
            build_research_regime_dataset(Uuid::from_u128(1), request, &candles, ts(3, 0, 0))
                .expect("dataset should build");

        assert_eq!(first.summary.selected_windows, 3);
        assert_eq!(
            first
                .windows
                .iter()
                .map(|window| (window.start_time, window.end_time, window.regime_label))
                .collect::<Vec<_>>(),
            second
                .windows
                .iter()
                .map(|window| (window.start_time, window.end_time, window.regime_label))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn regime_dataset_max_windows_per_regime_is_enforced() {
        let candles = dataset_range_candles(3);
        let request = regime_dataset_request(Some(2));

        let dataset =
            build_research_regime_dataset(Uuid::from_u128(1), request, &candles, ts(3, 0, 0))
                .expect("dataset should build");

        assert_eq!(
            dataset
                .summary
                .regime_counts
                .get(&ResearchRegimeLabel::Range)
                .copied(),
            Some(2)
        );
    }

    #[test]
    fn regime_dataset_reports_missing_regimes() {
        let candles = dataset_range_candles(1);
        let mut request = regime_dataset_request(None);
        request.target_regimes = Some(vec![
            ResearchRegimeLabel::Range,
            ResearchRegimeLabel::TrendUp,
        ]);

        let dataset =
            build_research_regime_dataset(Uuid::from_u128(1), request, &candles, ts(3, 0, 0))
                .expect("dataset should build");

        assert_eq!(dataset.status, ResearchRegimeDatasetStatus::Partial);
        assert!(dataset
            .summary
            .missing_regimes
            .contains(&ResearchRegimeLabel::TrendUp));
    }

    fn regime_discovery_request(max_windows_per_regime: u32) -> ResearchRegimeDiscoveryRequest {
        ResearchRegimeDiscoveryRequest {
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
            scan_start: ts(0, 0, 0),
            scan_end: ts(4, 0, 0),
            window_hours: 1,
            step_hours: 1,
            target_regimes: Some(vec![ResearchRegimeLabel::Range]),
            max_windows_per_regime,
            min_confidence: None,
            require_existing_candles: false,
            auto_backfill_missing: false,
            classifier_config: None,
            calibration_id: None,
        }
    }

    #[test]
    fn regime_discovery_selects_max_windows_per_regime() {
        let candles = dataset_range_candles(4);
        let request = regime_discovery_request(2);

        let discovery =
            run_research_regime_discovery(Uuid::from_u128(1), request, &candles, ts(4, 0, 0))
                .expect("discovery should run");

        assert_eq!(
            discovery
                .counts_by_regime
                .get(&ResearchRegimeLabel::Range)
                .copied(),
            Some(2)
        );
        assert_eq!(discovery.summary.selected_window_count, 2);
    }

    #[test]
    fn regime_discovery_reports_missing_regimes() {
        let candles = dataset_range_candles(1);
        let mut request = regime_discovery_request(10);
        request.scan_end = ts(1, 0, 0);
        request.target_regimes = Some(vec![
            ResearchRegimeLabel::Range,
            ResearchRegimeLabel::TrendUp,
        ]);

        let discovery =
            run_research_regime_discovery(Uuid::from_u128(1), request, &candles, ts(2, 0, 0))
                .expect("discovery should run");

        assert_eq!(discovery.status, ResearchRegimeDiscoveryStatus::Partial);
        assert!(discovery
            .missing_regimes
            .contains(&ResearchRegimeLabel::TrendUp));
    }

    #[test]
    fn regime_discovery_confidence_ordering_is_deterministic() {
        let candles = dataset_range_candles(3);
        let request = regime_discovery_request(3);

        let discovery =
            run_research_regime_discovery(Uuid::from_u128(1), request, &candles, ts(4, 0, 0))
                .expect("discovery should run");

        assert_eq!(
            discovery
                .selected_windows
                .iter()
                .map(|window| window.start_time)
                .collect::<Vec<_>>(),
            vec![ts(0, 0, 0), ts(1, 0, 0), ts(2, 0, 0)]
        );
    }

    #[test]
    fn regime_discovery_reduces_overlapping_windows() {
        let candles = dataset_range_candles(4);
        let mut request = regime_discovery_request(10);
        request.window_hours = 2;
        request.step_hours = 1;

        let discovery =
            run_research_regime_discovery(Uuid::from_u128(1), request, &candles, ts(4, 0, 0))
                .expect("discovery should run");

        assert_eq!(discovery.selected_windows.len(), 2);
        assert!(discovery.selected_windows[0].end_time <= discovery.selected_windows[1].start_time);
    }

    #[test]
    fn regime_discovery_no_candles_is_insufficient_data() {
        let request = regime_discovery_request(10);

        let discovery =
            run_research_regime_discovery(Uuid::from_u128(1), request, &[], ts(4, 0, 0))
                .expect("discovery should run");

        assert_eq!(
            discovery.status,
            ResearchRegimeDiscoveryStatus::InsufficientData
        );
        assert!(discovery.selected_windows.is_empty());
    }

    #[test]
    fn calibration_ranks_more_diverse_config_over_range_only_config() {
        let mut candles = Vec::new();
        for (hour, closes) in [
            (0_u32, vec![100, 101, 100, 101, 100, 101]),
            (1_u32, vec![100, 101, 102, 103, 104, 105]),
            (2_u32, vec![105, 104, 103, 102, 101, 100]),
        ] {
            for (minute, pair) in closes.windows(2).enumerate() {
                let open = Decimal::new(pair[0], 0);
                let close = Decimal::new(pair[1], 0);
                candles.push(Candle {
                    id: Uuid::new_v4(),
                    exchange: MarketDataSource::Binance,
                    symbol: Symbol::new("BTCUSDT").unwrap(),
                    interval: CandleInterval::OneMinute,
                    open_time: ts(hour, minute as u32, 0),
                    close_time: ts(hour, minute as u32, 59),
                    open,
                    high: open.max(close),
                    low: open.min(close),
                    close,
                    volume: Decimal::ONE,
                    quote_volume: None,
                    trade_count: 1,
                    is_closed: true,
                    created_at: ts(hour, minute as u32, 59),
                    updated_at: ts(hour, minute as u32, 59),
                });
            }
        }
        let range_only = ResearchRegimeThresholdCandidate {
            candidate_id: "range_only".to_string(),
            classifier_config: ResearchRegimeClassifierConfig {
                range_return_max_pct: Decimal::new(100, 0),
                range_choppiness_min: Decimal::ZERO,
                priority_order: vec![
                    ResearchRegimeLabel::Range,
                    ResearchRegimeLabel::TrendUp,
                    ResearchRegimeLabel::TrendDown,
                ],
                ..Default::default()
            },
        };
        let diverse = ResearchRegimeThresholdCandidate {
            candidate_id: "diverse".to_string(),
            classifier_config: ResearchRegimeClassifierConfig {
                trend_return_threshold_pct: Decimal::ONE,
                high_volatility_threshold_pct: Decimal::new(20, 0),
                range_return_max_pct: Decimal::ONE,
                priority_order: vec![
                    ResearchRegimeLabel::TrendUp,
                    ResearchRegimeLabel::TrendDown,
                    ResearchRegimeLabel::Range,
                ],
                ..Default::default()
            },
        };
        let result = run_research_regime_calibration(
            Uuid::from_u128(99),
            ResearchRegimeCalibrationRequest {
                symbol: "BTCUSDT".to_string(),
                timeframe: "1m".to_string(),
                scan_start: ts(0, 0, 0),
                scan_end: ts(3, 0, 0),
                window_hours: 1,
                step_hours: 1,
                threshold_candidates: Some(vec![range_only, diverse]),
                target_min_windows_per_regime: 1,
            },
            &candles,
            ts(3, 0, 0),
        )
        .expect("calibration should run");

        assert_eq!(result.recommended_candidate_id.as_deref(), Some("diverse"));
        assert!(result.candidates[0].diversity_score > result.candidates[1].diversity_score);
    }

    #[test]
    fn regime_dataset_from_discovery_preserves_windows() {
        let candles = dataset_range_candles(2);
        let discovery = run_research_regime_discovery(
            Uuid::from_u128(1),
            regime_discovery_request(2),
            &candles,
            ts(3, 0, 0),
        )
        .expect("discovery should run");

        let dataset = build_research_regime_dataset_from_discovery(
            Uuid::from_u128(2),
            &discovery,
            ResearchRegimeDatasetFromDiscoveryRequest {
                discovery_id: discovery.discovery_id,
                target_regimes: None,
                max_windows_per_regime: None,
            },
            ts(4, 0, 0),
        )
        .expect("dataset should build");

        assert_eq!(dataset.windows.len(), discovery.selected_windows.len());
        assert_eq!(
            dataset
                .windows
                .iter()
                .map(|window| (window.start_time, window.end_time, window.regime_label))
                .collect::<Vec<_>>(),
            discovery
                .selected_windows
                .iter()
                .map(|window| (window.start_time, window.end_time, window.regime_label))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn overfit_failure_reason_is_inferred() {
        let mut input = sample_failure_input();
        input.walk_forward_status = Some("OVERFIT_RISK".to_string());

        let reasons = infer_research_candidate_failure_reasons(&input);

        assert!(reasons.contains(&ResearchCandidateFailureReason::OverfitRisk));
    }

    #[test]
    fn fee_drag_failure_reason_is_inferred() {
        let mut input = sample_failure_input();
        input.pnl_pct = Some(Decimal::new(-1, 0));
        input.gross_pnl_pct = Some(Decimal::new(1, 0));

        let reasons = infer_research_candidate_failure_reasons(&input);

        assert!(reasons.contains(&ResearchCandidateFailureReason::FeeDrag));
    }

    #[test]
    fn too_many_trades_failure_reason_is_inferred() {
        let mut input = sample_failure_input();
        input.trade_count = Some(40);
        input.pnl_pct = Some(Decimal::new(-2, 0));

        let reasons = infer_research_candidate_failure_reasons(&input);

        assert!(reasons.contains(&ResearchCandidateFailureReason::TooManyTrades));
    }

    #[test]
    fn trend_strategy_in_range_infers_regime_mismatch() {
        let input = sample_failure_input();

        let reasons = infer_research_candidate_failure_reasons(&input);

        assert!(reasons.contains(&ResearchCandidateFailureReason::RegimeMismatch));
    }

    #[test]
    fn range_strategy_in_range_does_not_infer_regime_mismatch() {
        let mut input = sample_failure_input();
        input.strategy_id = "range_reversion_v1".to_string();

        let reasons = infer_research_candidate_failure_reasons(&input);

        assert!(!reasons.contains(&ResearchCandidateFailureReason::RegimeMismatch));
        assert!(reasons.contains(&ResearchCandidateFailureReason::WeakEdge));
    }

    #[test]
    fn range_strategy_in_trend_infers_regime_mismatch() {
        let candles = regime_candles(&[100, 102, 104, 106, 108, 110, 112]);
        let mut input = sample_failure_input();
        input.strategy_id = "range_reversion_v1".to_string();
        input.regime_metric =
            classify_research_regime("BTCUSDT", "5m", ts(0, 0, 0), ts(1, 0, 0), &candles);

        let reasons = infer_research_candidate_failure_reasons(&input);

        assert!(reasons.contains(&ResearchCandidateFailureReason::RegimeMismatch));
    }

    #[test]
    fn recommendation_ordering_is_deterministic() {
        let mut overfit = sample_failure_input();
        overfit.walk_forward_status = Some("OVERFIT_RISK".to_string());
        let mut fee_drag = sample_failure_input();
        fee_drag.candidate_id = Some(Uuid::from_u128(4));
        fee_drag.gross_pnl_pct = Some(Decimal::new(2, 0));
        fee_drag.pnl_pct = Some(Decimal::new(-1, 0));

        let first = build_research_campaign_failure_attribution(
            Uuid::from_u128(9),
            vec![fee_drag.clone(), overfit.clone()],
            ts(2, 0, 0),
        );
        let second = build_research_campaign_failure_attribution(
            Uuid::from_u128(9),
            vec![fee_drag, overfit],
            ts(2, 0, 0),
        );

        assert_eq!(
            first
                .recommendations
                .iter()
                .map(|recommendation| recommendation.code.as_str())
                .collect::<Vec<_>>(),
            second
                .recommendations
                .iter()
                .map(|recommendation| recommendation.code.as_str())
                .collect::<Vec<_>>()
        );
    }

    fn accepted_hypothesis_with_reason(
        reason: ResearchCandidateFailureReason,
        code: &str,
    ) -> ResearchHypothesis {
        ResearchHypothesis {
            id: Some(Uuid::from_u128(42)),
            source_type: ResearchHypothesisSource::CampaignFailureAttribution,
            status: ResearchHypothesisStatus::AcceptedForExperiment,
            strategy_id: Some("trend_filter_momentum_v2".to_string()),
            symbol: Some("BTCUSDT".to_string()),
            timeframe: Some("15m".to_string()),
            regime: Some(ResearchRegimeLabel::TrendUp),
            failure_reasons: vec![reason],
            evidence: ResearchHypothesisEvidence {
                summary: "sample accepted hypothesis".to_string(),
                details: json!({ "campaign_id": Uuid::from_u128(7) }),
            },
            recommendation: ResearchHypothesisRecommendation {
                code: code.to_string(),
                actions: vec!["review".to_string()],
            },
            proposed_action: "Review deterministic research plan.".to_string(),
            proposed_experiment_config: json!({ "source": "test" }),
            priority: ResearchHypothesisPriority::High,
            expected_effect: "Improve research evidence.".to_string(),
            risk: "May reject the variant.".to_string(),
            created_at: ts(0, 0, 0),
        }
    }

    #[test]
    fn fee_drag_hypothesis_creates_stricter_config_plan() {
        let hypothesis = accepted_hypothesis_with_reason(
            ResearchCandidateFailureReason::FeeDrag,
            "reduce_fee_drag_and_turnover",
        );

        let plan =
            plan_research_experiment_from_hypothesis(&hypothesis, ts(1, 0, 0), None).unwrap();

        assert_eq!(plan.plan_type, ResearchExperimentPlanType::ResearchCampaign);
        assert_eq!(plan.validation_status, ResearchExperimentPlanStatus::Ready);
        assert_eq!(
            plan.proposed_request["adjustments"]["entry_filter"],
            "tighter"
        );
        assert_eq!(
            plan.proposed_request["adjustments"]["cooldown_multiplier"],
            2
        );
    }

    #[test]
    fn too_few_trades_hypothesis_creates_looser_config_plan() {
        let hypothesis = accepted_hypothesis_with_reason(
            ResearchCandidateFailureReason::TooFewTrades,
            "loosen_entry_opportunity",
        );

        let plan =
            plan_research_experiment_from_hypothesis(&hypothesis, ts(1, 0, 0), None).unwrap();

        assert_eq!(plan.plan_type, ResearchExperimentPlanType::ResearchBatch);
        assert_eq!(
            plan.proposed_request["adjustments"]["loosen_thresholds"],
            true
        );
        assert_eq!(
            plan.proposed_request["adjustments"]["expand_entry_band"],
            true
        );
    }

    #[test]
    fn overfit_hypothesis_creates_robustness_plan() {
        let hypothesis = accepted_hypothesis_with_reason(
            ResearchCandidateFailureReason::OverfitRisk,
            "broaden_walk_forward_validation",
        );

        let plan =
            plan_research_experiment_from_hypothesis(&hypothesis, ts(1, 0, 0), None).unwrap();

        assert_eq!(plan.plan_type, ResearchExperimentPlanType::RobustnessMatrix);
        assert_eq!(plan.proposed_request["validation"]["expand_windows"], true);
    }

    #[test]
    fn regime_mismatch_hypothesis_creates_regime_filtered_campaign_plan() {
        let hypothesis = accepted_hypothesis_with_reason(
            ResearchCandidateFailureReason::RegimeMismatch,
            "split_or_disable_mismatched_regime",
        );

        let plan =
            plan_research_experiment_from_hypothesis(&hypothesis, ts(1, 0, 0), None).unwrap();

        assert_eq!(plan.plan_type, ResearchExperimentPlanType::ResearchCampaign);
        assert_eq!(
            plan.proposed_request["metadata"]["disable_mismatched_regimes"],
            true
        );
    }

    #[test]
    fn plan_validation_catches_missing_strategy_symbol_window() {
        let mut hypothesis = accepted_hypothesis_with_reason(
            ResearchCandidateFailureReason::WeakEdge,
            "diagnose_weak_edge",
        );
        hypothesis.strategy_id = None;
        hypothesis.symbol = None;
        hypothesis.evidence.details = json!({});

        let plan =
            plan_research_experiment_from_hypothesis(&hypothesis, ts(1, 0, 0), None).unwrap();

        assert_eq!(
            plan.validation_status,
            ResearchExperimentPlanStatus::Invalid
        );
        assert!(plan
            .validation_issues
            .iter()
            .any(|issue| issue == "missing strategy_id"));
        assert!(plan
            .validation_issues
            .iter()
            .any(|issue| issue == "missing symbol"));
        assert!(plan
            .validation_issues
            .iter()
            .any(|issue| issue == "missing research window"));
    }

    #[test]
    fn accepted_hypothesis_is_required_for_plan_generation() {
        let mut hypothesis = accepted_hypothesis_with_reason(
            ResearchCandidateFailureReason::FeeDrag,
            "reduce_fee_drag_and_turnover",
        );
        hypothesis.status = ResearchHypothesisStatus::Proposed;

        let err =
            plan_research_experiment_from_hypothesis(&hypothesis, ts(1, 0, 0), None).unwrap_err();

        assert!(matches!(
            err,
            CoreError::ResearchExperimentPlanRequiresAcceptedHypothesis
        ));
    }

    fn matrix_request() -> StrategyRobustnessMatrixRequest {
        StrategyRobustnessMatrixRequest {
            strategy_ids: vec!["s1".to_string()],
            symbols: vec!["BTCUSDT".to_string()],
            timeframes: vec!["5m".to_string()],
            windows: vec![StrategyRobustnessMatrixWindow {
                start_time: ts(0, 0, 0),
                end_time: ts(0, 1, 0),
            }],
            start_time: None,
            end_time: None,
            window_hours: None,
            step_hours: None,
            config_json_by_strategy: None,
            experiment_run_id: None,
            initial_capital: Decimal::new(10000, 0),
            fee_bps: Decimal::new(10, 0),
            slippage_bps: Decimal::new(5, 0),
            holding_candles: None,
            min_trades_per_cell: 5,
            min_profitable_window_ratio: Decimal::new(5, 1),
        }
    }

    fn matrix_cell(
        strategy_id: &str,
        pnl_pct: Decimal,
        trade_count: i32,
    ) -> StrategyRobustnessMatrixCell {
        StrategyRobustnessMatrixCell {
            id: Uuid::new_v4(),
            matrix_run_id: Uuid::from_u128(77),
            strategy_id: strategy_id.to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "5m".to_string(),
            window_start: ts(0, 0, 0),
            window_end: ts(0, 1, 0),
            regime_label: ResearchRegimeLabel::TrendUp,
            data_quality_status: MarketDataQualityStatus::Good,
            status: if pnl_pct >= Decimal::ZERO {
                StrategyRobustnessMatrixStatus::PromisingButWeak
            } else {
                StrategyRobustnessMatrixStatus::Negative
            },
            pnl_pct,
            trade_count,
            raw_signal_count: trade_count,
            executed_trade_count: trade_count,
            cooldown_suppressed_count: 0,
            win_rate: Decimal::new(50, 0),
            max_drawdown_pct: Decimal::new(1, 0),
            fee_drag: Decimal::new(1, 1),
            findings: Vec::new(),
            created_at: ts(0, 0, 0),
        }
    }

    #[test]
    fn matrix_one_positive_many_negative_is_overfit_or_negative() {
        let request = matrix_request();
        let cells = vec![
            matrix_cell("s1", Decimal::new(5, 0), 8),
            matrix_cell("s1", Decimal::new(-1, 0), 8),
            matrix_cell("s1", Decimal::new(-2, 0), 8),
            matrix_cell("s1", Decimal::new(-3, 0), 8),
        ];

        let summary = summarize_strategy_robustness_matrix_strategy("s1", &request, &cells);

        assert!(matches!(
            summary.status,
            StrategyRobustnessMatrixStatus::OverfitRisk | StrategyRobustnessMatrixStatus::Negative
        ));
    }

    #[test]
    fn matrix_positive_median_enough_trades_is_robust_or_promising() {
        let request = matrix_request();
        let cells = vec![
            matrix_cell("s1", Decimal::new(2, 0), 10),
            matrix_cell("s1", Decimal::new(1, 0), 12),
            matrix_cell("s1", Decimal::new(15, 1), 11),
            matrix_cell("s1", Decimal::new(5, 1), 9),
        ];

        let summary = summarize_strategy_robustness_matrix_strategy("s1", &request, &cells);

        assert!(matches!(
            summary.status,
            StrategyRobustnessMatrixStatus::Robust
                | StrategyRobustnessMatrixStatus::PromisingButWeak
        ));
    }

    #[test]
    fn matrix_too_few_trades_is_weak() {
        let request = matrix_request();
        let cells = vec![
            matrix_cell("s1", Decimal::new(1, 0), 1),
            matrix_cell("s1", Decimal::new(1, 0), 2),
            matrix_cell("s1", Decimal::new(1, 0), 1),
        ];

        let summary = summarize_strategy_robustness_matrix_strategy("s1", &request, &cells);

        assert_eq!(
            summary.status,
            StrategyRobustnessMatrixStatus::PromisingButWeak
        );
    }

    #[test]
    fn matrix_negative_median_is_negative() {
        let request = matrix_request();
        let cells = vec![
            matrix_cell("s1", Decimal::new(1, 0), 8),
            matrix_cell("s1", Decimal::new(-1, 0), 8),
            matrix_cell("s1", Decimal::new(-2, 0), 8),
        ];

        let summary = summarize_strategy_robustness_matrix_strategy("s1", &request, &cells);

        assert_eq!(summary.status, StrategyRobustnessMatrixStatus::Negative);
    }

    #[test]
    fn matrix_ranking_is_deterministic() {
        let request = matrix_request();
        let cells = vec![
            matrix_cell("b", Decimal::new(1, 0), 8),
            matrix_cell("a", Decimal::new(1, 0), 8),
        ];

        let result = build_strategy_robustness_matrix_result(
            Uuid::from_u128(8),
            request,
            cells,
            ts(0, 0, 0),
        );

        assert_eq!(result.strategy_rankings[0].strategy_id, "a");
        assert_eq!(result.strategy_rankings[1].strategy_id, "b");
    }

    #[test]
    fn matrix_regime_consistency_counts_positive_regimes() {
        let mut trend = matrix_cell("s1", Decimal::new(1, 0), 8);
        trend.regime_label = ResearchRegimeLabel::TrendUp;
        let mut range = matrix_cell("s1", Decimal::new(-1, 0), 8);
        range.regime_label = ResearchRegimeLabel::Range;
        let cells = vec![&trend, &range];

        let consistency = calculate_strategy_robustness_regime_consistency(&cells);

        assert_eq!(consistency, Decimal::new(50, 0));
    }

    fn hypothesis_attribution_with_reason(
        reason: ResearchCandidateFailureReason,
    ) -> ResearchCampaignFailureAttribution {
        ResearchCampaignFailureAttribution {
            campaign_id: Uuid::from_u128(42),
            overall_failure_reasons: vec![reason],
            regime_summary: Vec::new(),
            candidate_failure_table: vec![ResearchCandidateFailureAttributionRow {
                candidate_id: Some(Uuid::from_u128(1)),
                experiment_run_id: None,
                walk_forward_run_id: None,
                strategy_id: "range_reversion_v1".to_string(),
                symbol: "BTCUSDT".to_string(),
                timeframe: "15m".to_string(),
                window_start: ts(0, 0, 0),
                window_end: ts(1, 0, 0),
                regime_label: ResearchRegimeLabel::TrendUp,
                failure_reasons: vec![reason],
                pnl_pct: Some(Decimal::new(-1, 0)),
                gross_pnl_pct: Some(Decimal::new(1, 0)),
                fee_drag_pct: Some(Decimal::new(25, 1)),
                trade_count: Some(30),
                win_rate: Some(Decimal::new(30, 0)),
                max_drawdown_pct: Some(Decimal::new(5, 0)),
                walk_forward_status: Some("OVERFIT_RISK".to_string()),
                data_quality_status: None,
            }],
            strategy_timeframe_breakdown: vec![ResearchStrategyTimeframeFailureBreakdown {
                strategy_id: "range_reversion_v1".to_string(),
                symbol: "BTCUSDT".to_string(),
                timeframe: "15m".to_string(),
                candidate_count: 1,
                dominant_regime: ResearchRegimeLabel::TrendUp,
                top_failure_reasons: vec![reason],
                avg_pnl_pct: Some(Decimal::new(-1, 0)),
                avg_trade_count: Some(Decimal::new(30, 0)),
            }],
            findings: Vec::new(),
            recommendations: Vec::new(),
            generated_at: ts(2, 0, 0),
        }
    }

    fn generated_from_attribution(
        reason: ResearchCandidateFailureReason,
    ) -> Vec<ResearchHypothesis> {
        generate_research_hypotheses(
            ResearchHypothesisGenerationEvidence {
                failure_attribution: Some(hypothesis_attribution_with_reason(reason)),
                ..Default::default()
            },
            ts(3, 0, 0),
        )
        .hypotheses
    }

    #[test]
    fn hypothesis_fee_drag_rule_is_high_priority() {
        let hypotheses = generated_from_attribution(ResearchCandidateFailureReason::FeeDrag);
        assert_eq!(hypotheses[0].priority, ResearchHypothesisPriority::High);
        assert_eq!(
            hypotheses[0].recommendation.code,
            "reduce_fee_drag_and_turnover"
        );
    }

    #[test]
    fn hypothesis_too_few_trades_rule_is_medium_priority() {
        let hypotheses = generated_from_attribution(ResearchCandidateFailureReason::TooFewTrades);
        assert_eq!(hypotheses[0].priority, ResearchHypothesisPriority::Medium);
        assert_eq!(
            hypotheses[0].recommendation.code,
            "loosen_entry_opportunity"
        );
    }

    #[test]
    fn hypothesis_regime_mismatch_rule_is_high_priority() {
        let hypotheses = generated_from_attribution(ResearchCandidateFailureReason::RegimeMismatch);
        assert_eq!(hypotheses[0].priority, ResearchHypothesisPriority::High);
        assert_eq!(
            hypotheses[0].recommendation.code,
            "split_or_disable_mismatched_regime"
        );
    }

    #[test]
    fn hypothesis_overfit_risk_rule_is_high_priority() {
        let hypotheses = generated_from_attribution(ResearchCandidateFailureReason::OverfitRisk);
        assert_eq!(hypotheses[0].priority, ResearchHypothesisPriority::High);
        assert_eq!(
            hypotheses[0].recommendation.code,
            "broaden_walk_forward_validation"
        );
    }

    #[test]
    fn hypothesis_promising_bucket_rule_uses_bucket_boundaries() {
        let bucket = crate::StrategySignalFeatureBucket {
            feature_name: "close_vs_sma_pct".to_string(),
            bucket_label: "0..1".to_string(),
            sample_count: 5,
            win_rate: Decimal::new(60, 0),
            avg_net_pnl_pct: Decimal::new(1, 0),
            median_net_pnl_pct: Decimal::new(1, 0),
            best_net_pnl_pct: Decimal::new(2, 0),
            worst_net_pnl_pct: Decimal::new(-1, 0),
            total_net_pnl_pct: Decimal::new(5, 0),
            recommendation: crate::StrategySignalFeatureRecommendation::Promising,
        };
        let result = generate_research_hypotheses(
            ResearchHypothesisGenerationEvidence {
                signal_feature_attribution: Some(StrategySignalFeatureAttributionResult {
                    strategy_id: "s1".to_string(),
                    symbol: "BTCUSDT".to_string(),
                    timeframe: "15m".to_string(),
                    start_time: ts(0, 0, 0),
                    end_time: ts(1, 0, 0),
                    holding_window: 5,
                    total_raw_signals: 10,
                    executable_signals: 10,
                    attributed_signals: 5,
                    insufficient_forward_data_count: 0,
                    suppression_breakdown: Vec::new(),
                    feature_buckets: vec![bucket.clone()],
                    best_buckets: vec![bucket],
                    worst_buckets: Vec::new(),
                    recommendations: Vec::new(),
                    samples: Vec::new(),
                    status: crate::StrategySignalFeatureAttributionStatus::PromisingFeaturesFound,
                    computed_at: ts(2, 0, 0),
                }),
                ..Default::default()
            },
            ts(3, 0, 0),
        );
        assert_eq!(
            result.hypotheses[0].recommendation.code,
            "use_promising_feature_bucket"
        );
    }

    #[test]
    fn hypothesis_exit_attribution_negative_rule_rejects_config() {
        let result = generate_research_hypotheses(
            ResearchHypothesisGenerationEvidence {
                exit_attribution: Some(StrategyExitAttributionResult {
                    strategy_id: "s1".to_string(),
                    symbol: "BTCUSDT".to_string(),
                    timeframe: "15m".to_string(),
                    start_time: ts(0, 0, 0),
                    end_time: ts(1, 0, 0),
                    total_raw_signals: 1,
                    total_executable_signals: 1,
                    suppression_breakdown: Vec::new(),
                    per_holding_window: vec![crate::StrategyExitAttributionHoldingWindow {
                        holding_candles: 5,
                        trade_count: 1,
                        win_rate: Decimal::ZERO,
                        avg_net_pnl_pct: Decimal::new(-1, 0),
                        median_net_pnl_pct: Decimal::new(-1, 0),
                        total_net_pnl_pct: Decimal::new(-1, 0),
                        best_net_pnl_pct: Decimal::new(-1, 0),
                        worst_net_pnl_pct: Decimal::new(-1, 0),
                        max_drawdown_pct: None,
                        fee_drag_pct: Decimal::new(1, 0),
                        recommendation: crate::StrategyExitAttributionRecommendation::Negative,
                    }],
                    best_holding_window: None,
                    worst_holding_window: Some(5),
                    status: crate::StrategyExitAttributionStatus::Negative,
                    recommendation: crate::StrategyExitAttributionRecommendation::Negative,
                    trades: Vec::new(),
                    computed_at: ts(2, 0, 0),
                }),
                ..Default::default()
            },
            ts(3, 0, 0),
        );
        assert_eq!(
            result.hypotheses[0].recommendation.code,
            "reject_before_exit_tweaks"
        );
    }

    #[test]
    fn hypothesis_data_quality_rule_blocks_research() {
        let result = generate_research_hypotheses(
            ResearchHypothesisGenerationEvidence {
                opportunity_analysis: Some(StrategyOpportunityAnalysisResult {
                    strategy_id: "s1".to_string(),
                    symbol: "BTCUSDT".to_string(),
                    timeframe: "15m".to_string(),
                    start_time: ts(0, 0, 0),
                    end_time: ts(1, 0, 0),
                    total_closed_candles: 0,
                    evaluable_windows: 0,
                    would_signal_count: 0,
                    no_signal_count: 0,
                    signal_rate_pct: Decimal::ZERO,
                    top_blocking_conditions: Vec::new(),
                    condition_pass_rates: Vec::new(),
                    condition_failure_breakdown: Vec::new(),
                    example_pass_windows: Vec::new(),
                    example_fail_windows: Vec::new(),
                    distributions: json!({}),
                    recommendation: crate::StrategyOpportunityRecommendation {
                        status: StrategyOpportunityStatus::DataQualityDegraded,
                        messages: Vec::new(),
                    },
                    data_quality_status: StrategyOpportunityStatus::DataQualityDegraded,
                    analyzed_at: ts(2, 0, 0),
                }),
                ..Default::default()
            },
            ts(3, 0, 0),
        );
        assert_eq!(
            result.hypotheses[0].source_type,
            ResearchHypothesisSource::DataQuality
        );
    }

    #[test]
    fn hypothesis_ordering_and_dedup_are_deterministic() {
        let attribution =
            hypothesis_attribution_with_reason(ResearchCandidateFailureReason::FeeDrag);
        let first = generate_research_hypotheses(
            ResearchHypothesisGenerationEvidence {
                failure_attribution: Some(attribution.clone()),
                ..Default::default()
            },
            ts(3, 0, 0),
        );
        let second = generate_research_hypotheses(
            ResearchHypothesisGenerationEvidence {
                failure_attribution: Some(attribution),
                ..Default::default()
            },
            ts(3, 0, 0),
        );
        assert_eq!(first.hypotheses, second.hypotheses);
        assert_eq!(first.hypotheses.len(), 1);
    }

    #[test]
    fn research_experiment_plan_run_status_and_mode_parse_wire_values() {
        assert_eq!(
            "PREVIEW".parse::<ResearchExperimentPlanRunMode>().unwrap(),
            ResearchExperimentPlanRunMode::Preview
        );
        assert_eq!(
            "INVALID_PLAN"
                .parse::<ResearchExperimentPlanRunStatus>()
                .unwrap(),
            ResearchExperimentPlanRunStatus::InvalidPlan
        );
        assert!("unsupported"
            .parse::<ResearchExperimentPlanRunStatus>()
            .is_err());
    }

    #[test]
    fn research_experiment_plan_run_artifact_reports_single_created_id() {
        let run_id = Uuid::from_u128(42);
        let artifact = ResearchExperimentPlanRunArtifact {
            robustness_matrix_run_id: Some(run_id),
            ..Default::default()
        };
        assert_eq!(artifact.artifact_type(), Some("robustness_matrix_run_id"));
        assert_eq!(artifact.artifact_id(), Some(run_id));
    }

    #[test]
    fn scheduled_research_disabled_job_request_is_valid_but_disabled() {
        let request = ScheduledResearchJobRequest {
            name: "Aggregation status".to_string(),
            kind: ScheduledResearchJobKind::AggregationStatus,
            enabled: false,
            interval_seconds: 60,
            request: json!({}),
            max_runs_per_tick: 1,
            next_run_at: None,
        };
        assert!(request.validate().is_ok());
        assert!(!request.enabled);
    }

    #[test]
    fn scheduled_research_unsafe_job_kind_is_rejected_by_parser() {
        assert!("PAPER_ORDER".parse::<ScheduledResearchJobKind>().is_err());
        assert!("LIVE_ORDER".parse::<ScheduledResearchJobKind>().is_err());
        assert!("PROMOTE_CANDIDATE"
            .parse::<ScheduledResearchJobKind>()
            .is_err());
    }

    #[test]
    fn scheduled_research_next_run_at_uses_interval_seconds() {
        let completed_at = ts(1, 0, 0);
        assert_eq!(
            scheduled_research_next_run_at(completed_at, 300).unwrap(),
            ts(1, 5, 0)
        );
        assert!(scheduled_research_next_run_at(completed_at, 0).is_err());
    }

    #[test]
    fn stale_research_status_detection_is_limited_to_active_states() {
        assert!(is_research_stale_candidate_status("STARTED"));
        assert!(is_research_stale_candidate_status("RUNNING"));
        assert!(is_research_stale_candidate_status("IN_PROGRESS"));
        assert!(is_research_stale_candidate_status("PENDING"));
        assert!(!is_research_stale_candidate_status("COMPLETED"));
        assert!(!is_research_stale_candidate_status("FAILED"));
    }

    #[test]
    fn stale_research_recovery_request_defaults_are_safe() {
        let request = ResearchStaleRunRecoveryRequest::default();
        assert_eq!(
            request.older_than_minutes,
            DEFAULT_RESEARCH_STALE_RUN_OLDER_THAN_MINUTES
        );
        assert!(request.dry_run);
        assert_eq!(request.normalized_older_than_minutes(), 60);
        assert!(!request.confirmation_matches());
    }

    #[test]
    fn scheduled_research_max_runs_per_tick_must_be_positive() {
        let request = ScheduledResearchJobRequest {
            name: "Bad max".to_string(),
            kind: ScheduledResearchJobKind::AggregationStatus,
            enabled: true,
            interval_seconds: 60,
            request: json!({}),
            max_runs_per_tick: 0,
            next_run_at: None,
        };
        assert!(matches!(
            request.validate(),
            Err(CoreError::InvalidScheduledResearchJobMaxRuns)
        ));
    }

    #[test]
    fn research_candidate_bundle_fingerprint_is_deterministic_for_object_key_order() {
        let mut first = sample_research_candidate_bundle(json!({
            "b": 2,
            "a": {"z": 1, "y": 2}
        }));
        let mut second = sample_research_candidate_bundle(json!({
            "a": {"y": 2, "z": 1},
            "b": 2
        }));
        first.integrity.bundle_fingerprint = research_candidate_bundle_fingerprint(&first).unwrap();
        second.integrity.bundle_fingerprint =
            research_candidate_bundle_fingerprint(&second).unwrap();

        assert_eq!(
            first.integrity.bundle_fingerprint,
            second.integrity.bundle_fingerprint
        );
    }

    #[test]
    fn research_candidate_import_confirmation_requires_exact_config_fingerprint() {
        let fingerprint = "399c3e554330ffb1bfbeafe1f1b090e32ba51e985eb383d242527137833750da";
        assert!(is_valid_research_candidate_import_confirmation(
            fingerprint,
            &expected_research_candidate_import_confirmation(fingerprint)
        ));
        assert!(!is_valid_research_candidate_import_confirmation(
            fingerprint,
            "IMPORT RESEARCH CANDIDATE wrong"
        ));
    }

    #[test]
    fn research_candidate_import_reconciliation_confirmation_requires_exact_import_id() {
        let import_id = Uuid::parse_str("41a3ea74-0a43-4a1f-9665-ad03167a61ff").unwrap();
        assert!(
            is_valid_research_candidate_import_reconciliation_confirmation(
                import_id,
                &expected_research_candidate_import_reconciliation_confirmation(import_id)
            )
        );
        assert!(
            !is_valid_research_candidate_import_reconciliation_confirmation(
                import_id,
                "RECORD RECONCILIATION wrong"
            )
        );
    }

    #[test]
    fn research_candidate_bundle_v2_fingerprint_includes_validation_protocol() {
        let mut first = sample_research_candidate_bundle_v2(json!({"holding_candles": 20}));
        let mut second = first.clone();
        first.integrity.bundle_fingerprint = research_candidate_bundle_fingerprint(&first).unwrap();
        second.validation_protocol.as_mut().unwrap().fee_bps = Decimal::new(11, 1);
        second.integrity.bundle_fingerprint =
            research_candidate_bundle_fingerprint(&second).unwrap();

        assert_ne!(
            first.integrity.bundle_fingerprint,
            second.integrity.bundle_fingerprint
        );
    }

    #[test]
    fn research_candidate_bundle_fingerprint_ignores_export_time() {
        let mut first = sample_research_candidate_bundle_v2(json!({"holding_candles": 20}));
        let mut second = first.clone();
        first.exported_at = ts(1, 0, 0);
        second.exported_at = ts(1, 0, 1);
        first.integrity.bundle_fingerprint = research_candidate_bundle_fingerprint(&first).unwrap();
        second.integrity.bundle_fingerprint =
            research_candidate_bundle_fingerprint(&second).unwrap();

        assert_eq!(
            first.integrity.bundle_fingerprint,
            second.integrity.bundle_fingerprint
        );
    }

    #[test]
    fn research_candidate_bundle_v1_deserializes_without_v2_protocol() {
        let bundle_json = serde_json::to_value(sample_research_candidate_bundle(json!({
            "holding_candles": 20
        })))
        .unwrap();
        let bundle: ResearchCandidateEvidenceBundle = serde_json::from_value(bundle_json).unwrap();

        assert_eq!(
            bundle.schema_version,
            "research_candidate_evidence_bundle.v1"
        );
        assert!(bundle.validation_protocol.is_none());
        assert!(bundle.walk_forward_protocol.is_none());
    }

    fn sample_research_candidate_bundle_v2(config: Value) -> ResearchCandidateEvidenceBundle {
        let mut bundle = sample_research_candidate_bundle(config);
        bundle.schema_version = "research_candidate_evidence_bundle.v2".to_string();
        bundle.validation_protocol = Some(ResearchCandidateValidationProtocol {
            data_window_start: ts(1, 0, 0),
            data_window_end: ts(2, 0, 0),
            symbol: "ETHUSDT".to_string(),
            timeframe: "1h".to_string(),
            fee_bps: Decimal::new(10, 1),
            slippage_bps: Decimal::new(5, 1),
            holding_candles: Some(20),
            replay_mode: "strategy_experiment".to_string(),
            strategy_id: "failed_breakdown_reclaim_v1".to_string(),
            normalized_strategy_config: bundle.candidate.config.clone(),
            config_fingerprint: bundle.candidate.config_fingerprint.clone(),
        });
        bundle.walk_forward_protocol = Some(ResearchCandidateWalkForwardProtocol {
            train_window_hours: Some(24 * 90),
            test_window_hours: Some(24 * 30),
            step_hours: Some(24 * 30),
            min_windows: Some(3),
            explicit_windows: vec![ResearchCandidateWalkForwardProtocolWindow {
                window_start: ts(1, 0, 0),
                window_end: ts(1, 12, 0),
                pnl_pct: Decimal::new(4045420723, 10),
                trade_count: 4,
                status: "PROFITABLE".to_string(),
            }],
        });
        bundle.data_quality_snapshot = Some(ResearchCandidateDataQualitySnapshot {
            candle_count: Some(24),
            expected_candle_count: Some(24),
            coverage_pct: Some(Decimal::new(100, 0)),
            gap_count: Some(0),
            first_candle: Some(ts(1, 0, 0)),
            last_candle: Some(ts(2, 0, 0)),
            quality_status: Some("GOOD".to_string()),
        });
        bundle.evidence_artifacts = Some(ResearchCandidateEvidenceArtifacts {
            experiment_run_id: Some(Uuid::new_v4()),
            walk_forward_run_id: Some(Uuid::new_v4()),
            robustness_matrix_run_id: None,
            robustness_matrix_cell_id: None,
            candidate_gate_source_id: None,
            proposal_id: None,
            source_batch_id: None,
            source_campaign_id: None,
            has_experiment: true,
            has_walk_forward: true,
            has_robustness_matrix: false,
            has_data_quality_snapshot: true,
        });
        bundle.engine_fingerprint = Some(ResearchCandidateEngineFingerprint {
            app_git_hash: Some("abc123".to_string()),
            replay_engine_version: Some("test".to_string()),
            replay_engine_hash: None,
            strategy_engine_version: Some("test".to_string()),
            strategy_engine_hash: None,
            migration_version: Some("0064".to_string()),
        });
        bundle
    }

    fn sample_research_candidate_bundle(config: Value) -> ResearchCandidateEvidenceBundle {
        ResearchCandidateEvidenceBundle {
            schema_version: "research_candidate_evidence_bundle.v1".to_string(),
            exported_at: ts(1, 0, 0),
            source_environment: Some("test".to_string()),
            candidate: ResearchCandidateEvidenceBundleCandidate {
                candidate_id: Uuid::parse_str("70867792-93df-494c-9a8b-d961c73107e4").unwrap(),
                strategy_id: "failed_breakdown_reclaim_v1".to_string(),
                symbol: "ETHUSDT".to_string(),
                timeframe: "1h".to_string(),
                status: ResearchCandidateStatus::PromotedToShadowConfig,
                config,
                config_fingerprint:
                    "399c3e554330ffb1bfbeafe1f1b090e32ba51e985eb383d242527137833750da".to_string(),
            },
            evidence_provenance: ResearchCandidateEvidenceProvenance::default(),
            evidence_summary: json!({"qualification_status": "NOT_QUALIFIED"}),
            warnings: vec!["not paper/testnet-ready".to_string()],
            execution_boundary: ResearchCandidateExecutionBoundary {
                no_paper_testnet_live: true,
                import_does_not_authorize_execution: true,
                paper_testnet_live_boundary_crossed: false,
                import_does_not_create_shadow_runs: true,
                import_does_not_update_runner_config: true,
                import_does_not_create_scheduled_jobs: true,
                import_does_not_mark_ready_for_testnet: true,
            },
            validation_protocol: None,
            walk_forward_protocol: None,
            data_quality_snapshot: None,
            evidence_artifacts: None,
            engine_fingerprint: None,
            integrity: ResearchCandidateEvidenceBundleIntegrity {
                bundle_fingerprint: String::new(),
                algorithm: "sha256:canonical-json-without-integrity".to_string(),
            },
        }
    }
}
