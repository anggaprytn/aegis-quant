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
