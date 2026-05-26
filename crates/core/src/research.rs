use std::collections::BTreeSet;

use chrono::{DateTime, Duration, TimeZone, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    calculate_strategy_rejection_rate, Candle, CandleInterval, CoreError, ExecutionReadinessStatus,
    MarketDataSource, Symbol, TestnetShadowRunnerConfig,
};

fn default_required_coverage_pct() -> Decimal {
    Decimal::new(95, 0)
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
    Rejected,
    Archived,
}

impl ResearchCandidateStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Discovered => "DISCOVERED",
            Self::Observing => "OBSERVING",
            Self::AcceptedForShadow => "ACCEPTED_FOR_SHADOW",
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
pub struct ResearchCandidateDecisionRequest {
    pub decision: ResearchCandidateDecision,
    pub reason: Option<String>,
    pub notes: Option<String>,
    #[serde(default)]
    pub acknowledge_runner_mismatch: bool,
    pub correlation_id: Option<Uuid>,
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
        ResearchCandidateDecision::PromoteToShadowConfig => Ok(current_status),
        ResearchCandidateDecision::Reject => match current_status {
            ResearchCandidateStatus::Discovered
            | ResearchCandidateStatus::Observing
            | ResearchCandidateStatus::AcceptedForShadow => Ok(ResearchCandidateStatus::Rejected),
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
    Blocked,
    NoChanges,
    Applied,
}

impl ResearchCandidateShadowPromotionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "READY",
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
pub struct ResearchCandidateShadowPromotionPreview {
    pub candidate_id: Uuid,
    pub candidate_status: ResearchCandidateStatus,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub current_runner_config: TestnetShadowRunnerConfig,
    pub proposed_runner_config: TestnetShadowRunnerConfig,
    pub changes: Vec<String>,
    pub status: ResearchCandidateShadowPromotionStatus,
    pub reasons: Vec<String>,
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
    pub changes: Vec<String>,
    pub status: ResearchCandidateShadowPromotionStatus,
    pub reasons: Vec<String>,
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
    pub latest_runner_alignment: Option<StrategyCandidateRunnerAlignment>,
    pub latest_readiness_status: Option<ExecutionReadinessStatus>,
    pub latest_recommendations: Vec<String>,
    pub stale_count: i64,
    pub alignment_mismatch_count: i64,
    pub runner_config_drift_count: i64,
    pub last_observed_at: Option<DateTime<Utc>>,
    pub current_accept_for_shadow_eligible: bool,
    pub current_accept_for_shadow_blockers: Vec<String>,
    pub computed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCandidateShadowPerformanceStatus {
    InsufficientData,
    Healthy,
    UnderObservation,
    NeedsReview,
}

impl ResearchCandidateShadowPerformanceStatus {
    pub fn as_str(self) -> &'static str {
        match self {
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
    InsufficientData,
    KeepObserving,
    NeedsReview,
    CandidateNotCoveredByRunner,
    RejectCandidate,
}

impl ResearchCandidateShadowPerformanceRecommendation {
    pub fn as_str(self) -> &'static str {
        match self {
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
    pub runner_alignment_valid: bool,
    pub shadow_runner_covers_candidate: bool,
    #[serde(default)]
    pub runner_mismatch_count: i64,
    pub latest_readiness_status: Option<ExecutionReadinessStatus>,
    pub shadow_performance: Option<ResearchCandidateShadowPerformance>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResearchCandidateWatchlistEntry {
    pub candidate_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub candidate_status: ResearchCandidateStatus,
    pub latest_evaluation: Option<ResearchCandidateQualificationEvaluation>,
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

    let status_is_accepted =
        request.candidate_status == Some(ResearchCandidateStatus::AcceptedForShadow);
    checks.push(qualification_check(
        "candidate_status_accepted_for_shadow",
        "Candidate status is ACCEPTED_FOR_SHADOW",
        status_is_accepted,
        true,
        ResearchCandidateQualificationSeverity::High,
        if status_is_accepted {
            "Candidate is in ACCEPTED_FOR_SHADOW."
        } else {
            "Candidate is not in ACCEPTED_FOR_SHADOW."
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

    let fresh_observation_passed =
        !thresholds.require_fresh_observation || request.fresh_observation;
    checks.push(qualification_check(
        "fresh_observation_required",
        "Candidate has fresh observation",
        fresh_observation_passed,
        thresholds.require_fresh_observation,
        ResearchCandidateQualificationSeverity::High,
        if fresh_observation_passed {
            "Fresh observation is available."
        } else {
            "Fresh observation is required but missing or stale."
        },
        None,
    ));
    if !fresh_observation_passed {
        score -= 25;
        score_explanation.push(
            "Fresh observation is missing or stale, so qualification lost 25 points.".to_string(),
        );
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

    let shadow_performance = request.shadow_performance.clone();
    let total_shadow_runs = shadow_performance
        .as_ref()
        .map(|value| value.total_shadow_runs)
        .unwrap_or(0);
    let would_submit_count = shadow_performance
        .as_ref()
        .map(|value| value.would_submit_count)
        .unwrap_or(0);
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

    let enough_shadow_runs = total_shadow_runs >= thresholds.min_shadow_runs;
    checks.push(qualification_check(
        "enough_linked_shadow_runs",
        "Enough linked shadow runs exist",
        enough_shadow_runs,
        false,
        ResearchCandidateQualificationSeverity::Medium,
        if enough_shadow_runs {
            "Linked shadow run count meets threshold.".to_string()
        } else {
            format!(
                "Linked shadow runs {} are below min {}.",
                total_shadow_runs, thresholds.min_shadow_runs
            )
        },
        Some(serde_json::json!({
            "total_shadow_runs": total_shadow_runs,
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

    ResearchCandidateQualificationResult {
        candidate_id: request.candidate_id,
        status,
        score: clamp_score(score),
        fresh_observation: request.fresh_observation,
        runner_alignment_valid: request.runner_alignment_valid,
        latest_readiness_status: readiness_status,
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
        computed_at: request.computed_at,
    }
}

pub fn evaluate_research_candidate_shadow_performance(
    candidate_id: Uuid,
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
    runner_alignment_current: bool,
    computed_at: DateTime<Utc>,
) -> ResearchCandidateShadowPerformance {
    let would_submit_rate_pct = calculate_percentage_rate(would_submit_count, total_shadow_runs);
    let risk_rejection_rate_pct = calculate_percentage_rate(risk_rejected_count, total_shadow_runs);
    let skipped_or_error_count = skipped_count + error_count;
    let skipped_or_error_rate_pct =
        calculate_percentage_rate(skipped_or_error_count, total_shadow_runs);

    let (status, recommendation) = if !runner_alignment_current {
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
        runner_alignment_current,
        recommendation,
        status,
        outcome_breakdown,
        computed_at,
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
    fn would_submit_rate_pct_calculation_is_deterministic() {
        assert_eq!(calculate_percentage_rate(3, 12), Decimal::new(25, 0));
    }

    #[test]
    fn shadow_risk_rejection_rate_pct_calculation_is_deterministic() {
        let performance = evaluate_research_candidate_shadow_performance(
            Uuid::nil(),
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
            true,
            ts(0, 1, 0),
        );

        assert_eq!(performance.risk_rejection_rate_pct, Decimal::new(25, 0));
    }

    #[test]
    fn shadow_performance_recommends_reject_after_enough_zero_would_submit_runs() {
        let performance = evaluate_research_candidate_shadow_performance(
            Uuid::nil(),
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
            candidate_status: Some(ResearchCandidateStatus::AcceptedForShadow),
            fresh_observation: true,
            runner_alignment_valid: true,
            shadow_runner_covers_candidate: true,
            runner_mismatch_count: 0,
            latest_readiness_status: Some(ExecutionReadinessStatus::Ready),
            shadow_performance: performance,
            thresholds: ResearchCandidateQualificationThresholds::default(),
            computed_at: ts(1, 0, 0),
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
    fn qualification_not_qualified_without_fresh_observation() {
        let mut request = qualification_request(Some(qualification_performance(40, 6, 4, 0, 0)));
        request.fresh_observation = false;

        let result = evaluate_research_candidate_qualification(&request);

        assert_eq!(
            result.status,
            ResearchCandidateQualificationStatus::NotQualified
        );
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
    fn accept_for_shadow_transition_is_valid_from_observing() {
        let next = research_candidate_next_status(
            ResearchCandidateStatus::Observing,
            ResearchCandidateDecision::AcceptForShadow,
        )
        .expect("transition should be valid");

        assert_eq!(next, ResearchCandidateStatus::AcceptedForShadow);
    }

    #[test]
    fn promote_to_shadow_config_decision_keeps_status_unchanged() {
        let next = research_candidate_next_status(
            ResearchCandidateStatus::AcceptedForShadow,
            ResearchCandidateDecision::PromoteToShadowConfig,
        )
        .expect("promotion event should keep status");

        assert_eq!(next, ResearchCandidateStatus::AcceptedForShadow);
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
}
