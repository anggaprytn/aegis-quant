use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{Candle, CandleInterval, CoreError, MarketDataSource, Symbol};

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

pub fn expected_strategy_research_promotion_confirmation(strategy_id: &str) -> String {
    format!(
        "PROMOTE STRATEGY {}",
        strategy_id.trim().to_ascii_uppercase()
    )
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
}
