use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde_json::json;
use uuid::Uuid;

use aegis_core::{
    aggregate_closed_1m_candles, build_research_data_coverage_result, summarize_candle_coverage,
    CandleAggregationResult, CandleBackfillRequest, CandleBackfillResult, CandleInterval,
    ResearchDataCoverageRequest, ResearchDataCoverageResult, ResearchDatasetBuildRequest,
    ResearchDatasetBuildResult, ResearchDatasetBuildStatus, ResearchDatasetBuildStep,
    ResearchDatasetBuildStepStatus,
};
use db::{
    complete_research_dataset_build, get_closed_1m_candles_range, get_research_dataset_build,
    insert_research_dataset_build, list_closed_candle_open_times_in_range,
    list_research_dataset_build_steps, list_research_dataset_builds,
    replace_research_dataset_build_steps, research_dataset_build_result_from_records,
    upsert_aggregated_candles, PgPool,
};
use events::{EventPublisher, PostgresEventPublisher, SystemEventType};

use crate::HistoricalCandleBackfillService;

#[derive(Clone)]
pub struct ResearchDatasetService {
    pool: PgPool,
    source: String,
    binance_rest_base_url: String,
}

impl ResearchDatasetService {
    pub fn new(
        pool: PgPool,
        source: impl Into<String>,
        binance_rest_base_url: impl Into<String>,
    ) -> Self {
        Self {
            pool,
            source: source.into(),
            binance_rest_base_url: binance_rest_base_url.into(),
        }
    }

    pub async fn inspect_coverage(
        &self,
        request: ResearchDataCoverageRequest,
    ) -> Result<ResearchDataCoverageResult> {
        request.validate()?;
        let symbol = request.normalized_symbol()?;
        let intervals = request.parsed_intervals()?;
        let now = Utc::now();
        let mut per_interval = Vec::with_capacity(intervals.len());

        for interval in intervals {
            let actual_open_times = list_closed_candle_open_times_in_range(
                &self.pool,
                request.exchange,
                &symbol,
                interval,
                request.start_time,
                request.end_time,
            )
            .await?;
            per_interval.push(summarize_candle_coverage(
                interval,
                request.start_time,
                request.end_time,
                now,
                request.required_coverage_pct,
                &actual_open_times,
            ));
        }

        Ok(build_research_data_coverage_result(&request, per_interval))
    }

    pub async fn build_dataset(
        &self,
        request: ResearchDatasetBuildRequest,
    ) -> Result<ResearchDatasetBuildResult> {
        request.validate()?;
        let symbol = request.normalized_symbol()?;
        let correlation_id = request.correlation_id.unwrap_or_else(Uuid::new_v4);
        let build_id = Uuid::new_v4();
        let created_at = Utc::now();
        let publisher = PostgresEventPublisher::new(self.pool.clone());
        let one_minute_request = one_minute_coverage_request(&request);
        let requested_coverage_request = coverage_request_from_build(&request);
        let coverage_before = self
            .inspect_coverage(requested_coverage_request.clone())
            .await?;

        insert_research_dataset_build(
            &self.pool,
            build_id,
            &request,
            &coverage_before,
            correlation_id,
            created_at,
        )
        .await?;

        publisher
            .publish(SystemEventType::ResearchDatasetBuildStarted.into_event(
                correlation_id,
                self.source.clone(),
                json!({
                    "build_id": build_id,
                    "exchange": request.exchange.as_str(),
                    "symbol": symbol.as_str(),
                    "intervals": request.intervals,
                    "start_time": request.start_time,
                    "end_time": request.end_time,
                }),
            ))
            .await?;

        let execution: Result<ResearchDatasetBuildResult> = async {
            let coverage_1m_before = self.inspect_coverage(one_minute_request.clone()).await?;
            let mut steps = Vec::new();

            let backfill_started_at = Utc::now();
            let mut backfill_results = Vec::<CandleBackfillResult>::new();
            let one_minute_summary = coverage_1m_before
                .per_interval
                .first()
                .ok_or_else(|| anyhow!("1m coverage summary missing"))?;

            if !one_minute_summary.missing_ranges.is_empty() {
                let service = HistoricalCandleBackfillService::new(
                    self.pool.clone(),
                    self.source.clone(),
                    &self.binance_rest_base_url,
                )?;
                for gap in &one_minute_summary.missing_ranges {
                    backfill_results.push(
                        service
                            .run(CandleBackfillRequest {
                                exchange: request.exchange,
                                symbol: request.symbol.clone(),
                                interval: CandleInterval::OneMinute.as_str().to_string(),
                                start_time: gap.start_time,
                                end_time: gap.end_time,
                                limit_per_request: Some(1000),
                                correlation_id: Some(correlation_id),
                            })
                            .await?,
                    );
                }
            }
            let inserted_candles = backfill_results
                .iter()
                .map(|result| result.inserted_candles)
                .sum::<i32>();
            let updated_candles = backfill_results
                .iter()
                .map(|result| result.updated_candles)
                .sum::<i32>();
            let skipped_candles = backfill_results
                .iter()
                .map(|result| result.skipped_candles)
                .sum::<i32>();

            publisher
                .publish(SystemEventType::ResearchDatasetBackfillCompleted.into_event(
                    correlation_id,
                    self.source.clone(),
                    json!({
                        "build_id": build_id,
                        "symbol": symbol.as_str(),
                        "missing_ranges": one_minute_summary.missing_ranges.len(),
                        "backfill_runs": backfill_results.iter().map(|result| result.run_id).collect::<Vec<_>>(),
                        "inserted_candles": inserted_candles,
                        "updated_candles": updated_candles,
                        "skipped_candles": skipped_candles,
                    }),
                ))
                .await?;
            steps.push(completed_step(
                "check_and_backfill_1m",
                backfill_started_at,
                json!({
                    "missing_ranges": one_minute_summary.missing_ranges.len(),
                    "backfill_runs": backfill_results.len(),
                    "inserted_candles": inserted_candles,
                    "updated_candles": updated_candles,
                    "skipped_candles": skipped_candles,
                }),
            ));

            let recompute_started_at = Utc::now();
            let coverage_1m_after = self.inspect_coverage(one_minute_request.clone()).await?;
            let remaining_1m_gaps = coverage_1m_after
                .per_interval
                .first()
                .map(|summary| summary.missing_ranges.len())
                .unwrap_or_default();
            steps.push(completed_step(
                "recompute_1m_coverage",
                recompute_started_at,
                json!({
                    "status": coverage_1m_after.status.as_str(),
                    "remaining_missing_ranges": remaining_1m_gaps,
                }),
            ));

            let aggregate_started_at = Utc::now();
            let mut aggregation_results = Vec::<CandleAggregationResult>::new();
            for interval in request.parsed_intervals()? {
                if !interval.is_aggregated_from_one_minute() {
                    continue;
                }
                let source_candles = get_closed_1m_candles_range(
                    &self.pool,
                    request.exchange,
                    &symbol,
                    request.start_time,
                    request.end_time,
                )
                .await?;
                let aggregated = aggregate_closed_1m_candles(&source_candles, interval);
                let upsert = upsert_aggregated_candles(&self.pool, &aggregated.candles).await?;
                aggregation_results.push(CandleAggregationResult {
                    exchange: request.exchange,
                    symbol: request.symbol.trim().to_ascii_uppercase(),
                    source_interval: CandleInterval::OneMinute.as_str().to_string(),
                    target_interval: interval.as_str().to_string(),
                    start_time: request.start_time,
                    end_time: request.end_time,
                    source_candles: i32::try_from(source_candles.len()).unwrap_or(i32::MAX),
                    aggregated_candles: i32::try_from(aggregated.candles.len()).unwrap_or(i32::MAX),
                    inserted: upsert.inserted_candles,
                    updated: upsert.updated_candles,
                    skipped_incomplete: aggregated.skipped_incomplete_buckets,
                    correlation_id: Some(correlation_id),
                });
            }

            publisher
                .publish(SystemEventType::ResearchDatasetAggregateCompleted.into_event(
                    correlation_id,
                    self.source.clone(),
                    json!({
                        "build_id": build_id,
                        "symbol": symbol.as_str(),
                        "intervals": aggregation_results.iter().map(|result| result.target_interval.clone()).collect::<Vec<_>>(),
                        "aggregated_candles": aggregation_results.iter().map(|result| result.aggregated_candles).sum::<i32>(),
                        "inserted": aggregation_results.iter().map(|result| result.inserted).sum::<i32>(),
                        "updated": aggregation_results.iter().map(|result| result.updated).sum::<i32>(),
                    }),
                ))
                .await?;
            steps.push(completed_step(
                "aggregate_higher_timeframes",
                aggregate_started_at,
                json!({
                    "intervals": aggregation_results.iter().map(|result| result.target_interval.clone()).collect::<Vec<_>>(),
                    "results": aggregation_results,
                }),
            ));

            let final_coverage = self.inspect_coverage(requested_coverage_request.clone()).await?;
            let completed_at = Utc::now();
            complete_research_dataset_build(
                &self.pool,
                build_id,
                ResearchDatasetBuildStatus::Completed,
                &final_coverage,
                None,
                completed_at,
            )
            .await?;
            replace_research_dataset_build_steps(&self.pool, build_id, &steps).await?;

            publisher
                .publish(SystemEventType::ResearchDatasetBuildCompleted.into_event(
                    correlation_id,
                    self.source.clone(),
                    json!({
                        "build_id": build_id,
                        "symbol": symbol.as_str(),
                        "status": final_coverage.status.as_str(),
                        "interval_statuses": final_coverage
                            .per_interval
                            .iter()
                            .map(|summary| json!({
                                "interval": summary.interval,
                                "status": summary.status.as_str(),
                                "coverage_pct": summary.coverage_pct,
                            }))
                            .collect::<Vec<_>>(),
                    }),
                ))
                .await?;

            Ok(ResearchDatasetBuildResult {
                build_id,
                exchange: request.exchange,
                symbol: request.symbol.trim().to_ascii_uppercase(),
                requested_intervals: request.intervals.clone(),
                start_time: request.start_time,
                end_time: request.end_time,
                status: ResearchDatasetBuildStatus::Completed,
                coverage_before: coverage_before.clone(),
                coverage_after: final_coverage,
                steps,
                failed_reason: None,
                correlation_id,
                created_at,
                completed_at: Some(completed_at),
            })
        }
        .await;

        match execution {
            Ok(result) => Ok(result),
            Err(err) => {
                let failed_at = Utc::now();
                let failed_step = failed_step("build_dataset", failed_at, err.to_string());
                let _ = complete_research_dataset_build(
                    &self.pool,
                    build_id,
                    ResearchDatasetBuildStatus::Failed,
                    &coverage_before,
                    Some(&err.to_string()),
                    failed_at,
                )
                .await;
                let _ = replace_research_dataset_build_steps(&self.pool, build_id, &[failed_step])
                    .await;
                let _ = publisher
                    .publish(SystemEventType::ResearchDatasetBuildFailed.into_event(
                        correlation_id,
                        self.source.clone(),
                        json!({
                            "build_id": build_id,
                            "symbol": symbol.as_str(),
                            "error": err.to_string(),
                        }),
                    ))
                    .await;
                Err(err)
            }
        }
    }

    pub async fn list_builds(&self, limit: i64) -> Result<Vec<ResearchDatasetBuildResult>> {
        let records = list_research_dataset_builds(&self.pool, limit).await?;
        let mut builds = Vec::with_capacity(records.len());
        for record in records {
            let steps = list_research_dataset_build_steps(&self.pool, record.id).await?;
            builds.push(research_dataset_build_result_from_records(&record, &steps)?);
        }
        Ok(builds)
    }

    pub async fn get_build(&self, build_id: Uuid) -> Result<Option<ResearchDatasetBuildResult>> {
        let Some(record) = get_research_dataset_build(&self.pool, build_id).await? else {
            return Ok(None);
        };
        let steps = list_research_dataset_build_steps(&self.pool, build_id).await?;
        Ok(Some(research_dataset_build_result_from_records(
            &record, &steps,
        )?))
    }
}

fn coverage_request_from_build(
    request: &ResearchDatasetBuildRequest,
) -> ResearchDataCoverageRequest {
    ResearchDataCoverageRequest {
        exchange: request.exchange,
        symbol: request.symbol.clone(),
        intervals: request.intervals.clone(),
        start_time: request.start_time,
        end_time: request.end_time,
        required_coverage_pct: request.required_coverage_pct,
        correlation_id: request.correlation_id,
    }
}

fn one_minute_coverage_request(
    request: &ResearchDatasetBuildRequest,
) -> ResearchDataCoverageRequest {
    ResearchDataCoverageRequest {
        exchange: request.exchange,
        symbol: request.symbol.clone(),
        intervals: vec![CandleInterval::OneMinute.as_str().to_string()],
        start_time: request.start_time,
        end_time: request.end_time,
        required_coverage_pct: request.required_coverage_pct,
        correlation_id: request.correlation_id,
    }
}

fn completed_step(
    step: &str,
    started_at: DateTime<Utc>,
    details: serde_json::Value,
) -> ResearchDatasetBuildStep {
    ResearchDatasetBuildStep {
        step: step.to_string(),
        status: ResearchDatasetBuildStepStatus::Completed,
        details: Some(details),
        started_at,
        completed_at: Some(Utc::now()),
    }
}

fn failed_step(step: &str, started_at: DateTime<Utc>, error: String) -> ResearchDatasetBuildStep {
    ResearchDatasetBuildStep {
        step: step.to_string(),
        status: ResearchDatasetBuildStepStatus::Failed,
        details: Some(json!({ "error": error })),
        started_at,
        completed_at: Some(Utc::now()),
    }
}
