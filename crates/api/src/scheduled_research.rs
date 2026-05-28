use aegis_core::{
    candle_aggregation_status, scheduled_research_next_run_at, CandleInterval,
    MarketDataQualityRequest, MarketDataSource, ScheduledResearchBootstrapSafePlanItem,
    ScheduledResearchBootstrapSafeRequest, ScheduledResearchJob, ScheduledResearchJobKind,
    ScheduledResearchJobRequest, ScheduledResearchJobRun, ScheduledResearchJobRunStatus,
    ScheduledResearchJobStatus, Symbol,
};
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use db::{
    get_latest_candle_aggregation_run, get_latest_closed_candle_time, get_scheduled_research_job,
    insert_scheduled_research_job_run, list_due_scheduled_research_jobs, list_market_feed_statuses,
    mark_scheduled_research_job_after_run, scheduled_research_job_from_record,
    scheduled_research_job_run_from_record, summarize_candle_continuity_report,
    try_claim_scheduled_research_job,
};
use serde_json::{json, Value};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::AppState;

pub const DEFAULT_SCHEDULED_RESEARCH_RUNNER_INTERVAL_SECONDS: u64 = 60;
pub const DEFAULT_SCHEDULED_RESEARCH_DISABLED_SLEEP_SECONDS: u64 = 300;
pub const DEFAULT_SCHEDULED_RESEARCH_MAX_CONSECUTIVE_FAILURES: i32 = 5;
pub const DEFAULT_SCHEDULED_RESEARCH_BACKOFF_BASE_SECONDS: i64 = 300;
pub const DEFAULT_SCHEDULED_RESEARCH_BACKOFF_MAX_SECONDS: i64 = 3600;
pub const SAFE_PROVIDER_HEALTH_INTERVAL_SECONDS: i64 = 15 * 60;
pub const SAFE_AGGREGATION_STATUS_INTERVAL_SECONDS: i64 = 5 * 60;
pub const SAFE_MARKET_DATA_QUALITY_INTERVAL_SECONDS: i64 = 30 * 60;
pub const SAFE_OPERATOR_REPORT_INTERVAL_SECONDS: i64 = 24 * 60 * 60;
pub const SAFE_MARKET_DATA_QUALITY_LOOKBACK_HOURS: i64 = 24;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledResearchTickResult {
    pub attempted_jobs: usize,
    pub completed_runs: usize,
    pub failed_runs: usize,
    pub skipped_runs: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExecutionTableCounts {
    orders: i64,
    paper_positions: i64,
    paper_fills: i64,
    exchange_testnet_orders: i64,
    exchange_testnet_order_lifecycle_events: i64,
    testnet_shadow_promotions: i64,
}

#[derive(Debug, Clone)]
struct ScheduledJobExecution {
    status: ScheduledResearchJobRunStatus,
    result: Value,
    error: Option<String>,
    artifact_type: Option<String>,
    artifact_id: Option<Uuid>,
}

pub fn build_safe_bootstrap_scheduled_research_jobs(
    payload: &ScheduledResearchBootstrapSafeRequest,
    exchange: MarketDataSource,
    configured_symbols: &[Symbol],
) -> Result<Vec<ScheduledResearchJobRequest>> {
    let symbols = normalize_bootstrap_symbols(&payload.symbols, configured_symbols)?;
    let intervals = normalize_bootstrap_intervals(&payload.intervals)?;
    let interval_names = intervals
        .iter()
        .map(|interval| interval.as_str().to_string())
        .collect::<Vec<_>>();
    let aggregation_targets = intervals
        .iter()
        .copied()
        .filter(|interval| *interval != CandleInterval::OneMinute)
        .map(|interval| interval.as_str().to_string())
        .collect::<Vec<_>>();

    let mut jobs = vec![
        ScheduledResearchJobRequest {
            name: format!("provider-health-{}", exchange.as_str()),
            kind: ScheduledResearchJobKind::ProviderHealth,
            enabled: payload.enable,
            interval_seconds: SAFE_PROVIDER_HEALTH_INTERVAL_SECONDS,
            request: json!({ "exchange": exchange.as_str() }),
            max_runs_per_tick: 1,
            next_run_at: None,
        },
        ScheduledResearchJobRequest {
            name: "aggregation-status".to_string(),
            kind: ScheduledResearchJobKind::AggregationStatus,
            enabled: payload.enable,
            interval_seconds: SAFE_AGGREGATION_STATUS_INTERVAL_SECONDS,
            request: json!({
                "exchange": exchange.as_str(),
                "symbols": symbols.iter().map(|symbol| symbol.as_str()).collect::<Vec<_>>(),
                "target_intervals": aggregation_targets
            }),
            max_runs_per_tick: 1,
            next_run_at: None,
        },
    ];

    for symbol in &symbols {
        for interval in &intervals {
            jobs.push(ScheduledResearchJobRequest {
                name: format!(
                    "market-data-quality-{}-{}",
                    symbol.as_str(),
                    interval.as_str()
                ),
                kind: ScheduledResearchJobKind::MarketDataQuality,
                enabled: payload.enable,
                interval_seconds: SAFE_MARKET_DATA_QUALITY_INTERVAL_SECONDS,
                request: json!({
                    "exchange": exchange.as_str(),
                    "symbol": symbol.as_str(),
                    "interval": interval.as_str(),
                    "lookback_hours": SAFE_MARKET_DATA_QUALITY_LOOKBACK_HOURS,
                    "expected_interval_seconds": interval.duration().num_seconds(),
                    "max_allowed_gap_count": 0
                }),
                max_runs_per_tick: 1,
                next_run_at: None,
            });
        }
    }

    jobs.push(ScheduledResearchJobRequest {
        name: "operator-report-daily".to_string(),
        kind: ScheduledResearchJobKind::OperatorReport,
        enabled: payload.enable,
        interval_seconds: SAFE_OPERATOR_REPORT_INTERVAL_SECONDS,
        request: json!({
            "exchange": exchange.as_str(),
            "symbols": symbols.iter().map(|symbol| symbol.as_str()).collect::<Vec<_>>(),
            "intervals": interval_names,
            "window_hours": 24
        }),
        max_runs_per_tick: 1,
        next_run_at: None,
    });

    for job in &jobs {
        if !is_safe_monitoring_job_kind(job.kind) {
            anyhow::bail!(
                "unsafe scheduled research bootstrap job kind: {}",
                job.kind.as_str()
            );
        }
        job.validate()?;
    }

    Ok(jobs)
}

pub fn safe_bootstrap_plan_item(
    request: &ScheduledResearchJobRequest,
) -> ScheduledResearchBootstrapSafePlanItem {
    ScheduledResearchBootstrapSafePlanItem {
        name: request.name.clone(),
        kind: request.kind,
        interval_seconds: request.interval_seconds,
        enabled: request.enabled,
        request: request.request.clone(),
    }
}

fn is_safe_monitoring_job_kind(kind: ScheduledResearchJobKind) -> bool {
    matches!(
        kind,
        ScheduledResearchJobKind::ProviderHealth
            | ScheduledResearchJobKind::AggregationStatus
            | ScheduledResearchJobKind::MarketDataQuality
            | ScheduledResearchJobKind::OperatorReport
    )
}

fn normalize_bootstrap_symbols(requested: &[String], configured: &[Symbol]) -> Result<Vec<Symbol>> {
    let raw_symbols = if requested.is_empty() {
        configured
            .iter()
            .map(|symbol| symbol.as_str().to_string())
            .collect::<Vec<_>>()
    } else {
        requested.to_vec()
    };
    if raw_symbols.is_empty() {
        anyhow::bail!("safe scheduled research bootstrap requires at least one symbol");
    }
    let mut symbols = Vec::new();
    for raw in raw_symbols {
        let symbol = Symbol::new(raw)?;
        if !symbols.iter().any(|existing: &Symbol| existing == &symbol) {
            symbols.push(symbol);
        }
    }
    Ok(symbols)
}

fn normalize_bootstrap_intervals(requested: &[String]) -> Result<Vec<CandleInterval>> {
    let raw_intervals = if requested.is_empty() {
        vec![
            "1m".to_string(),
            "5m".to_string(),
            "15m".to_string(),
            "1h".to_string(),
        ]
    } else {
        requested.to_vec()
    };
    let mut intervals = Vec::new();
    for raw in raw_intervals {
        let interval = raw.parse::<CandleInterval>()?;
        if !intervals.contains(&interval) {
            intervals.push(interval);
        }
    }
    Ok(intervals)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScheduledJobCompletionUpdate {
    status: ScheduledResearchJobStatus,
    next_run_at: Option<chrono::DateTime<Utc>>,
    backoff_until: Option<chrono::DateTime<Utc>>,
    consecutive_failure_count: i32,
    last_failure_at: Option<chrono::DateTime<Utc>>,
    last_failure_reason: Option<String>,
    last_success_at: Option<chrono::DateTime<Utc>>,
    enabled: bool,
    auto_paused_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScheduledResearchFailurePolicy {
    max_consecutive_failures: i32,
    backoff_base_seconds: i64,
    backoff_max_seconds: i64,
}

impl Default for ScheduledResearchFailurePolicy {
    fn default() -> Self {
        Self {
            max_consecutive_failures: DEFAULT_SCHEDULED_RESEARCH_MAX_CONSECUTIVE_FAILURES,
            backoff_base_seconds: DEFAULT_SCHEDULED_RESEARCH_BACKOFF_BASE_SECONDS,
            backoff_max_seconds: DEFAULT_SCHEDULED_RESEARCH_BACKOFF_MAX_SECONDS,
        }
    }
}

impl ScheduledResearchFailurePolicy {
    fn from_env() -> Self {
        Self {
            max_consecutive_failures: env_i32(
                "SCHEDULED_RESEARCH_MAX_CONSECUTIVE_FAILURES",
                DEFAULT_SCHEDULED_RESEARCH_MAX_CONSECUTIVE_FAILURES,
            )
            .max(1),
            backoff_base_seconds: env_i64(
                "SCHEDULED_RESEARCH_BACKOFF_BASE_SECONDS",
                DEFAULT_SCHEDULED_RESEARCH_BACKOFF_BASE_SECONDS,
            )
            .max(1),
            backoff_max_seconds: env_i64(
                "SCHEDULED_RESEARCH_BACKOFF_MAX_SECONDS",
                DEFAULT_SCHEDULED_RESEARCH_BACKOFF_MAX_SECONDS,
            )
            .max(1),
        }
    }
}

fn job_completion_update(
    job: &ScheduledResearchJob,
    execution: &ScheduledJobExecution,
    completed_at: chrono::DateTime<Utc>,
    policy: &ScheduledResearchFailurePolicy,
) -> ScheduledJobCompletionUpdate {
    let normal_next_run_at =
        scheduled_research_next_run_at(completed_at, job.interval_seconds).ok();
    if matches!(execution.status, ScheduledResearchJobRunStatus::Failed) {
        let consecutive_failure_count = job.consecutive_failure_count.saturating_add(1);
        let last_failure_reason = execution
            .error
            .clone()
            .or_else(|| Some("scheduled research job failed".to_string()));

        if consecutive_failure_count >= policy.max_consecutive_failures {
            let reason = format!(
                "auto-paused after {consecutive_failure_count} consecutive scheduled research failures"
            );
            return ScheduledJobCompletionUpdate {
                status: ScheduledResearchJobStatus::AutoPaused,
                next_run_at: None,
                backoff_until: None,
                consecutive_failure_count,
                last_failure_at: Some(completed_at),
                last_failure_reason,
                last_success_at: job.last_success_at,
                enabled: false,
                auto_paused_reason: Some(reason),
            };
        }

        let backoff_until = scheduled_research_backoff_until(
            completed_at,
            consecutive_failure_count,
            policy.backoff_base_seconds,
            policy.backoff_max_seconds,
        );
        let status = if backoff_until.is_some() {
            ScheduledResearchJobStatus::BackingOff
        } else {
            ScheduledResearchJobStatus::Error
        };
        return ScheduledJobCompletionUpdate {
            status,
            next_run_at: backoff_until.or(normal_next_run_at),
            backoff_until,
            consecutive_failure_count,
            last_failure_at: Some(completed_at),
            last_failure_reason,
            last_success_at: job.last_success_at,
            enabled: job.enabled,
            auto_paused_reason: None,
        };
    }

    ScheduledJobCompletionUpdate {
        status: if job.enabled {
            ScheduledResearchJobStatus::Enabled
        } else {
            ScheduledResearchJobStatus::Disabled
        },
        next_run_at: normal_next_run_at,
        backoff_until: None,
        consecutive_failure_count: 0,
        last_failure_at: job.last_failure_at,
        last_failure_reason: None,
        last_success_at: Some(completed_at),
        enabled: job.enabled,
        auto_paused_reason: None,
    }
}

fn scheduled_research_backoff_until(
    completed_at: chrono::DateTime<Utc>,
    consecutive_failure_count: i32,
    base_seconds: i64,
    max_seconds: i64,
) -> Option<chrono::DateTime<Utc>> {
    if consecutive_failure_count < 2 {
        return None;
    }
    let multiplier = if consecutive_failure_count == 2 {
        1
    } else {
        3_i64.saturating_mul(2_i64.saturating_pow((consecutive_failure_count - 3) as u32))
    };
    let seconds = base_seconds.saturating_mul(multiplier).min(max_seconds);
    Some(completed_at + Duration::seconds(seconds))
}

pub async fn run_scheduled_research_tick(
    state: &AppState,
    max_jobs: i64,
) -> Result<ScheduledResearchTickResult> {
    let due_jobs = list_due_scheduled_research_jobs(&state.db_pool, Utc::now(), max_jobs).await?;
    let mut tick = ScheduledResearchTickResult {
        attempted_jobs: due_jobs.len(),
        completed_runs: 0,
        failed_runs: 0,
        skipped_runs: 0,
    };

    for record in due_jobs {
        let job = scheduled_research_job_from_record(&record)?;
        let per_job_limit = job.max_runs_per_tick.max(1);
        for _ in 0..per_job_limit {
            match run_scheduled_research_job(state, &job, false).await {
                Ok(run) => match run.status {
                    ScheduledResearchJobRunStatus::Completed => tick.completed_runs += 1,
                    ScheduledResearchJobRunStatus::Failed => tick.failed_runs += 1,
                    ScheduledResearchJobRunStatus::Skipped
                    | ScheduledResearchJobRunStatus::SkippedOverlap
                    | ScheduledResearchJobRunStatus::SkippedBackoff => tick.skipped_runs += 1,
                    ScheduledResearchJobRunStatus::PartialSuccess => tick.completed_runs += 1,
                },
                Err(err) => {
                    tick.failed_runs += 1;
                    error!(job_id = %job.id, kind = job.kind.as_str(), error = %err, "scheduled research job failed outside run recorder");
                }
            }
        }
    }

    Ok(tick)
}

pub async fn run_scheduled_research_job_once(
    state: &AppState,
    job: &ScheduledResearchJob,
) -> Result<ScheduledResearchJobRun> {
    run_scheduled_research_job(state, job, true).await
}

async fn run_scheduled_research_job(
    state: &AppState,
    job: &ScheduledResearchJob,
    manual: bool,
) -> Result<ScheduledResearchJobRun> {
    let started_at = Utc::now();
    let correlation_id = Uuid::new_v4();
    let policy = ScheduledResearchFailurePolicy::from_env();

    if !manual && (!job.enabled || matches!(job.status, ScheduledResearchJobStatus::Paused)) {
        let completed_at = Utc::now();
        let run = ScheduledResearchJobRun {
            id: Uuid::new_v4(),
            job_id: job.id,
            status: ScheduledResearchJobRunStatus::Skipped,
            started_at,
            completed_at: Some(completed_at),
            result: json!({"skipped": true, "reason": "job_disabled_or_paused"}),
            error: None,
            created_artifact_type: None,
            created_artifact_id: None,
            correlation_id: Some(correlation_id),
        };
        let record = insert_scheduled_research_job_run(&state.db_pool, &run).await?;
        return scheduled_research_job_run_from_record(&record);
    }

    if !manual {
        if let Some(backoff_until) = job.backoff_until {
            if backoff_until > started_at {
                return record_skip_without_run_history(
                    state,
                    job,
                    started_at,
                    correlation_id,
                    ScheduledResearchJobRunStatus::SkippedBackoff,
                    "job_backing_off",
                )
                .await;
            }
        }
    }

    let claimed_record =
        try_claim_scheduled_research_job(&state.db_pool, job.id, started_at, manual).await?;
    let claimed_job = match claimed_record {
        Some(record) => scheduled_research_job_from_record(&record)?,
        None => {
            let latest = get_scheduled_research_job(&state.db_pool, job.id).await?;
            let reason = latest
                .as_ref()
                .map(|record| record.status.as_str())
                .unwrap_or("not_found");
            let status = if reason == "RUNNING" {
                ScheduledResearchJobRunStatus::SkippedOverlap
            } else if reason == "BACKING_OFF" {
                ScheduledResearchJobRunStatus::SkippedBackoff
            } else {
                ScheduledResearchJobRunStatus::Skipped
            };
            return record_skip_without_run_history(
                state,
                job,
                started_at,
                correlation_id,
                status,
                reason,
            )
            .await;
        }
    };

    let before_counts = execution_table_counts(&state.db_pool).await?;
    let execution = execute_job_kind(state, &claimed_job, correlation_id).await;
    let after_counts = execution_table_counts(&state.db_pool).await?;
    let isolation_ok = before_counts == after_counts;

    let completed_at = Utc::now();
    let mut execution = match execution {
        Ok(value) => value,
        Err(err) => ScheduledJobExecution {
            status: ScheduledResearchJobRunStatus::Failed,
            result: json!({}),
            error: Some(err.to_string()),
            artifact_type: None,
            artifact_id: None,
        },
    };

    if !isolation_ok {
        execution.status = ScheduledResearchJobRunStatus::Failed;
        execution.error =
            Some("execution table counts changed during scheduled research job".to_string());
    }

    let result = json!({
        "job_kind": claimed_job.kind.as_str(),
        "manual": manual,
        "payload": execution.result,
        "execution_isolation": {
            "unchanged": isolation_ok,
            "before": before_counts_json(&before_counts),
            "after": before_counts_json(&after_counts)
        }
    });

    let run = ScheduledResearchJobRun {
        id: Uuid::new_v4(),
        job_id: claimed_job.id,
        status: execution.status,
        started_at,
        completed_at: Some(completed_at),
        result,
        error: execution.error.clone(),
        created_artifact_type: execution.artifact_type.clone(),
        created_artifact_id: execution.artifact_id,
        correlation_id: Some(correlation_id),
    };
    let record = insert_scheduled_research_job_run(&state.db_pool, &run).await?;
    let completion = job_completion_update(&claimed_job, &execution, completed_at, &policy);
    let _ = mark_scheduled_research_job_after_run(
        &state.db_pool,
        claimed_job.id,
        completed_at,
        completion.next_run_at,
        completion.status,
        completion.consecutive_failure_count,
        completion.last_failure_at,
        completion.last_failure_reason.as_deref(),
        completion.last_success_at,
        completion.backoff_until,
        completion.enabled,
        completion.auto_paused_reason.as_deref(),
    )
    .await;

    info!(
        job_id = %claimed_job.id,
        kind = claimed_job.kind.as_str(),
        status = run.status.as_str(),
        artifact_type = run.created_artifact_type.as_deref().unwrap_or(""),
        artifact_id = ?run.created_artifact_id,
        correlation_id = %correlation_id,
        "scheduled research job run recorded"
    );

    scheduled_research_job_run_from_record(&record)
}

async fn record_skip_without_run_history(
    state: &AppState,
    job: &ScheduledResearchJob,
    started_at: chrono::DateTime<Utc>,
    correlation_id: Uuid,
    status: ScheduledResearchJobRunStatus,
    reason: &str,
) -> Result<ScheduledResearchJobRun> {
    let completed_at = Utc::now();
    let run = ScheduledResearchJobRun {
        id: Uuid::new_v4(),
        job_id: job.id,
        status,
        started_at,
        completed_at: Some(completed_at),
        result: json!({"skipped": true, "reason": reason}),
        error: None,
        created_artifact_type: None,
        created_artifact_id: None,
        correlation_id: Some(correlation_id),
    };

    if matches!(status, ScheduledResearchJobRunStatus::SkippedBackoff) {
        return Ok(run);
    }

    let record = insert_scheduled_research_job_run(&state.db_pool, &run).await?;
    scheduled_research_job_run_from_record(&record)
}

async fn execute_job_kind(
    state: &AppState,
    job: &ScheduledResearchJob,
    _correlation_id: Uuid,
) -> Result<ScheduledJobExecution> {
    match job.kind {
        ScheduledResearchJobKind::ProviderHealth => {
            let feeds = list_market_feed_statuses(&state.db_pool).await?;
            Ok(ScheduledJobExecution {
                status: ScheduledResearchJobRunStatus::Completed,
                result: json!({ "feed_count": feeds.len(), "feeds": feeds }),
                error: None,
                artifact_type: None,
                artifact_id: None,
            })
        }
        ScheduledResearchJobKind::MarketDataQuality => {
            let request = market_data_quality_request_from_scheduled_job(job)
                .context("invalid MARKET_DATA_QUALITY request")?;
            request.validate()?;
            let report = summarize_candle_continuity_report(&state.db_pool, &request).await?;
            Ok(ScheduledJobExecution {
                status: ScheduledResearchJobRunStatus::Completed,
                result: serde_json::to_value(report)?,
                error: None,
                artifact_type: None,
                artifact_id: None,
            })
        }
        ScheduledResearchJobKind::AggregationStatus => {
            let exchange = job
                .request
                .get("exchange")
                .and_then(Value::as_str)
                .unwrap_or(state.market_config.exchange.as_str())
                .parse::<MarketDataSource>()?;
            let raw_symbols = job
                .request
                .get("symbols")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(Value::as_str)
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| {
                    state
                        .market_config
                        .symbols
                        .iter()
                        .map(|symbol| symbol.as_str().to_string())
                        .collect()
                });
            let raw_targets = job
                .request
                .get("target_intervals")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(Value::as_str)
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| vec!["5m".to_string(), "15m".to_string(), "1h".to_string()]);
            let mut rows = Vec::new();
            for raw_symbol in raw_symbols {
                let symbol = Symbol::new(raw_symbol)?;
                let latest_source = get_latest_closed_candle_time(
                    &state.db_pool,
                    exchange,
                    &symbol,
                    CandleInterval::OneMinute,
                )
                .await?;
                for raw_target in &raw_targets {
                    let target = raw_target.parse::<CandleInterval>()?;
                    let latest_target =
                        get_latest_closed_candle_time(&state.db_pool, exchange, &symbol, target)
                            .await?;
                    let latest_run = get_latest_candle_aggregation_run(
                        &state.db_pool,
                        &symbol,
                        CandleInterval::OneMinute,
                        target,
                    )
                    .await?;
                    let (status, lag_seconds, recommendation) =
                        candle_aggregation_status(latest_source, latest_target, target);
                    rows.push(json!({
                        "symbol": symbol.as_str(),
                        "source_interval": CandleInterval::OneMinute.as_str(),
                        "target_interval": target.as_str(),
                        "latest_source_closed_candle": latest_source,
                        "latest_target_closed_candle": latest_target,
                        "lag_seconds": lag_seconds,
                        "status": status.as_str(),
                        "inserted_last_tick": latest_run.as_ref().map(|run| run.inserted),
                        "updated_last_tick": latest_run.as_ref().map(|run| run.updated),
                        "recommendation": recommendation
                    }));
                }
            }
            Ok(ScheduledJobExecution {
                status: ScheduledResearchJobRunStatus::Completed,
                result: json!({ "rows": rows }),
                error: None,
                artifact_type: None,
                artifact_id: None,
            })
        }
        ScheduledResearchJobKind::OperatorReport => {
            let (total_runs, failed_runs) = db::count_recent_scheduled_research_runs(
                &state.db_pool,
                Utc::now() - Duration::hours(24),
            )
            .await?;
            Ok(ScheduledJobExecution {
                status: ScheduledResearchJobRunStatus::Completed,
                result: json!({
                    "scheduled_research": {
                        "recent_runs_24h": total_runs,
                        "failed_runs_24h": failed_runs
                    }
                }),
                error: None,
                artifact_type: Some("scheduled_research_operator_report".to_string()),
                artifact_id: None,
            })
        }
        ScheduledResearchJobKind::ResearchBatch
        | ScheduledResearchJobKind::ResearchCampaign
        | ScheduledResearchJobKind::RegimeDiscovery
        | ScheduledResearchJobKind::RobustnessMatrix => {
            warn!(job_id = %job.id, kind = job.kind.as_str(), "scheduled job kind requires API handler integration and was skipped safely");
            Ok(ScheduledJobExecution {
                status: ScheduledResearchJobRunStatus::Skipped,
                result: json!({
                    "skipped": true,
                    "reason": "job_kind_not_enabled_in_scheduler_worker",
                    "kind": job.kind.as_str()
                }),
                error: None,
                artifact_type: None,
                artifact_id: None,
            })
        }
    }
}

fn market_data_quality_request_from_scheduled_job(
    job: &ScheduledResearchJob,
) -> Result<MarketDataQualityRequest> {
    if job.request.get("lookback_hours").is_none() && job.request.get("lookback_minutes").is_none()
    {
        return Ok(serde_json::from_value(job.request.clone())?);
    }

    let exchange = job
        .request
        .get("exchange")
        .and_then(Value::as_str)
        .unwrap_or(MarketDataSource::Binance.as_str())
        .parse::<MarketDataSource>()?;
    let symbol = job
        .request
        .get("symbol")
        .and_then(Value::as_str)
        .context("MARKET_DATA_QUALITY dynamic request requires symbol")?
        .to_string();
    let interval = job
        .request
        .get("interval")
        .and_then(Value::as_str)
        .context("MARKET_DATA_QUALITY dynamic request requires interval")?
        .to_string();
    let lookback_minutes = job
        .request
        .get("lookback_minutes")
        .and_then(Value::as_i64)
        .or_else(|| {
            job.request
                .get("lookback_hours")
                .and_then(Value::as_i64)
                .map(|hours| hours.saturating_mul(60))
        })
        .context("MARKET_DATA_QUALITY dynamic request requires positive lookback")?;
    if lookback_minutes <= 0 {
        anyhow::bail!("MARKET_DATA_QUALITY dynamic request requires positive lookback");
    }
    let end_time = Utc::now();
    let start_time = end_time - Duration::minutes(lookback_minutes);
    Ok(MarketDataQualityRequest {
        exchange,
        symbol,
        interval,
        start_time,
        end_time,
        expected_interval_seconds: job
            .request
            .get("expected_interval_seconds")
            .and_then(Value::as_i64),
        max_allowed_gap_count: job
            .request
            .get("max_allowed_gap_count")
            .and_then(Value::as_i64),
        max_allowed_gap_pct: job
            .request
            .get("max_allowed_gap_pct")
            .cloned()
            .map(serde_json::from_value)
            .transpose()?,
    })
}

async fn execution_table_counts(pool: &db::PgPool) -> Result<ExecutionTableCounts> {
    let row = sqlx::query(
        r#"
        SELECT
            (SELECT COUNT(*)::BIGINT FROM orders) AS orders,
            (SELECT COUNT(*)::BIGINT FROM paper_positions) AS paper_positions,
            (SELECT COUNT(*)::BIGINT FROM paper_fills) AS paper_fills,
            (SELECT COUNT(*)::BIGINT FROM exchange_testnet_orders) AS exchange_testnet_orders,
            (SELECT COUNT(*)::BIGINT FROM exchange_testnet_order_lifecycle_events) AS exchange_testnet_order_lifecycle_events,
            (SELECT COUNT(*)::BIGINT FROM testnet_shadow_promotions) AS testnet_shadow_promotions
        "#,
    )
    .fetch_one(pool)
    .await?;

    use sqlx::Row;
    Ok(ExecutionTableCounts {
        orders: row.get("orders"),
        paper_positions: row.get("paper_positions"),
        paper_fills: row.get("paper_fills"),
        exchange_testnet_orders: row.get("exchange_testnet_orders"),
        exchange_testnet_order_lifecycle_events: row.get("exchange_testnet_order_lifecycle_events"),
        testnet_shadow_promotions: row.get("testnet_shadow_promotions"),
    })
}

fn before_counts_json(counts: &ExecutionTableCounts) -> Value {
    json!({
        "orders": counts.orders,
        "paper_positions": counts.paper_positions,
        "paper_fills": counts.paper_fills,
        "exchange_testnet_orders": counts.exchange_testnet_orders,
        "exchange_testnet_order_lifecycle_events": counts.exchange_testnet_order_lifecycle_events,
        "testnet_shadow_promotions": counts.testnet_shadow_promotions
    })
}

pub fn runner_interval_from_env() -> Result<u64> {
    parse_runner_interval(std::env::var("SCHEDULED_RESEARCH_RUNNER_INTERVAL_SECONDS").ok())
}

pub fn runner_disabled_sleep_seconds_from_env() -> Result<u64> {
    parse_runner_disabled_sleep_seconds(
        std::env::var("SCHEDULED_RESEARCH_DISABLED_SLEEP_SECONDS").ok(),
    )
}

fn parse_runner_interval(value: Option<String>) -> Result<u64> {
    Ok(value
        .unwrap_or_else(|| DEFAULT_SCHEDULED_RESEARCH_RUNNER_INTERVAL_SECONDS.to_string())
        .parse()
        .context("invalid SCHEDULED_RESEARCH_RUNNER_INTERVAL_SECONDS")?)
}

fn parse_runner_disabled_sleep_seconds(value: Option<String>) -> Result<u64> {
    Ok(value
        .unwrap_or_else(|| DEFAULT_SCHEDULED_RESEARCH_DISABLED_SLEEP_SECONDS.to_string())
        .parse()
        .context("invalid SCHEDULED_RESEARCH_DISABLED_SLEEP_SECONDS")?)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduledResearchRunnerMode {
    Enabled { interval_seconds: u64 },
    Disabled { sleep_seconds: u64 },
}

pub fn scheduled_research_runner_mode_from_env() -> Result<ScheduledResearchRunnerMode> {
    parse_scheduled_research_runner_mode(
        std::env::var("SCHEDULED_RESEARCH_RUNNER_ENABLED").ok(),
        std::env::var("SCHEDULED_RESEARCH_RUNNER_INTERVAL_SECONDS").ok(),
        std::env::var("SCHEDULED_RESEARCH_DISABLED_SLEEP_SECONDS").ok(),
    )
}

fn parse_scheduled_research_runner_mode(
    enabled: Option<String>,
    interval_seconds: Option<String>,
    disabled_sleep_seconds: Option<String>,
) -> Result<ScheduledResearchRunnerMode> {
    let enabled = enabled
        .map(|value| value.eq_ignore_ascii_case("true") || value == "1")
        .unwrap_or(false);
    if enabled {
        Ok(ScheduledResearchRunnerMode::Enabled {
            interval_seconds: parse_runner_interval(interval_seconds)?,
        })
    } else {
        Ok(ScheduledResearchRunnerMode::Disabled {
            sleep_seconds: parse_runner_disabled_sleep_seconds(disabled_sleep_seconds)?,
        })
    }
}

fn env_i32(name: &str, default: i32) -> i32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(default)
}

fn env_i64(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(default)
}

pub fn stale_scheduled_job_threshold() -> Duration {
    Duration::hours(24)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aegis_core::ScheduledResearchJobKind;

    fn job(consecutive_failure_count: i32) -> ScheduledResearchJob {
        ScheduledResearchJob {
            id: Uuid::new_v4(),
            name: "test".to_string(),
            kind: ScheduledResearchJobKind::ProviderHealth,
            enabled: true,
            interval_seconds: 60,
            request: json!({}),
            max_runs_per_tick: 1,
            last_run_at: None,
            last_failure_at: None,
            last_failure_reason: None,
            last_success_at: None,
            next_run_at: None,
            backoff_until: None,
            consecutive_failure_count,
            auto_paused_reason: None,
            status: ScheduledResearchJobStatus::Enabled,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn failed_execution() -> ScheduledJobExecution {
        ScheduledJobExecution {
            status: ScheduledResearchJobRunStatus::Failed,
            result: json!({}),
            error: Some("boom".to_string()),
            artifact_type: None,
            artifact_id: None,
        }
    }

    fn completed_execution() -> ScheduledJobExecution {
        ScheduledJobExecution {
            status: ScheduledResearchJobRunStatus::Completed,
            result: json!({}),
            error: None,
            artifact_type: None,
            artifact_id: None,
        }
    }

    #[test]
    fn failure_increments_consecutive_count() {
        let now = Utc::now();
        let update = job_completion_update(
            &job(0),
            &failed_execution(),
            now,
            &ScheduledResearchFailurePolicy::default(),
        );
        assert_eq!(update.consecutive_failure_count, 1);
        assert_eq!(update.status, ScheduledResearchJobStatus::Error);
        assert_eq!(update.backoff_until, None);
    }

    #[test]
    fn success_resets_failure_count() {
        let now = Utc::now();
        let update = job_completion_update(
            &job(3),
            &completed_execution(),
            now,
            &ScheduledResearchFailurePolicy::default(),
        );
        assert_eq!(update.consecutive_failure_count, 0);
        assert_eq!(update.status, ScheduledResearchJobStatus::Enabled);
        assert_eq!(update.last_success_at, Some(now));
        assert_eq!(update.backoff_until, None);
    }

    #[test]
    fn second_and_third_failure_compute_backoff() {
        let now = Utc::now();
        let policy = ScheduledResearchFailurePolicy::default();
        let second = job_completion_update(&job(1), &failed_execution(), now, &policy);
        let third = job_completion_update(&job(2), &failed_execution(), now, &policy);
        assert_eq!(second.status, ScheduledResearchJobStatus::BackingOff);
        assert_eq!(second.backoff_until, Some(now + Duration::minutes(5)));
        assert_eq!(third.backoff_until, Some(now + Duration::minutes(15)));
    }

    #[test]
    fn max_failures_auto_pauses_job() {
        let now = Utc::now();
        let update = job_completion_update(
            &job(4),
            &failed_execution(),
            now,
            &ScheduledResearchFailurePolicy::default(),
        );
        assert_eq!(update.consecutive_failure_count, 5);
        assert_eq!(update.status, ScheduledResearchJobStatus::AutoPaused);
        assert!(!update.enabled);
        assert!(update.auto_paused_reason.is_some());
    }

    #[test]
    fn backoff_is_capped() {
        let now = Utc::now();
        let backoff = scheduled_research_backoff_until(now, 6, 300, 900);
        assert_eq!(backoff, Some(now + Duration::seconds(900)));
    }

    #[test]
    fn safe_bootstrap_defaults_create_only_monitoring_jobs_disabled() {
        let request = ScheduledResearchBootstrapSafeRequest {
            enable: false,
            symbols: Vec::new(),
            intervals: Vec::new(),
            dry_run: false,
            replace_existing: false,
        };
        let jobs = build_safe_bootstrap_scheduled_research_jobs(
            &request,
            MarketDataSource::Binance,
            &[
                Symbol::new("btcusdt").unwrap(),
                Symbol::new("ethusdt").unwrap(),
            ],
        )
        .unwrap();
        assert_eq!(jobs.len(), 11);
        assert!(jobs.iter().all(|job| !job.enabled));
        assert!(jobs.iter().all(|job| matches!(
            job.kind,
            ScheduledResearchJobKind::ProviderHealth
                | ScheduledResearchJobKind::AggregationStatus
                | ScheduledResearchJobKind::MarketDataQuality
                | ScheduledResearchJobKind::OperatorReport
        )));
        assert!(!jobs.iter().any(|job| matches!(
            job.kind,
            ScheduledResearchJobKind::ResearchBatch
                | ScheduledResearchJobKind::ResearchCampaign
                | ScheduledResearchJobKind::RegimeDiscovery
                | ScheduledResearchJobKind::RobustnessMatrix
        )));
    }

    #[test]
    fn safe_bootstrap_enable_controls_enabled_state() {
        let request = ScheduledResearchBootstrapSafeRequest {
            enable: true,
            symbols: vec!["BTCUSDT".to_string()],
            intervals: vec!["15m".to_string()],
            dry_run: false,
            replace_existing: false,
        };
        let jobs = build_safe_bootstrap_scheduled_research_jobs(
            &request,
            MarketDataSource::Binance,
            &[Symbol::new("ETHUSDT").unwrap()],
        )
        .unwrap();
        assert_eq!(
            jobs.iter()
                .filter(|job| job.kind == ScheduledResearchJobKind::MarketDataQuality)
                .count(),
            1
        );
        assert!(jobs.iter().all(|job| job.enabled));
        assert!(jobs
            .iter()
            .any(|job| job.name == "market-data-quality-BTCUSDT-15m"));
    }

    #[test]
    fn dynamic_market_data_quality_request_uses_lookback_window() {
        let job = ScheduledResearchJob {
            id: Uuid::new_v4(),
            name: "market-data-quality-BTCUSDT-15m".to_string(),
            kind: ScheduledResearchJobKind::MarketDataQuality,
            enabled: true,
            interval_seconds: 60,
            request: json!({
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "interval": "15m",
                "lookback_minutes": 90,
                "expected_interval_seconds": 900,
                "max_allowed_gap_count": 0
            }),
            max_runs_per_tick: 1,
            last_run_at: None,
            last_failure_at: None,
            last_failure_reason: None,
            last_success_at: None,
            next_run_at: None,
            backoff_until: None,
            consecutive_failure_count: 0,
            auto_paused_reason: None,
            status: ScheduledResearchJobStatus::Enabled,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let request = market_data_quality_request_from_scheduled_job(&job).unwrap();
        assert_eq!(request.symbol, "BTCUSDT");
        assert_eq!(request.interval, "15m");
        assert_eq!(request.expected_interval_seconds, Some(900));
        assert_eq!(request.max_allowed_gap_count, Some(0));
        assert_eq!((request.end_time - request.start_time).num_minutes(), 90);
    }

    #[test]
    fn scheduled_runner_disabled_mode_uses_idle_sleep_default() {
        let mode = parse_scheduled_research_runner_mode(None, None, None).unwrap();
        assert_eq!(
            mode,
            ScheduledResearchRunnerMode::Disabled {
                sleep_seconds: DEFAULT_SCHEDULED_RESEARCH_DISABLED_SLEEP_SECONDS
            }
        );
    }

    #[test]
    fn scheduled_runner_disabled_mode_uses_configured_idle_sleep() {
        let mode = parse_scheduled_research_runner_mode(
            Some("false".to_string()),
            Some("5".to_string()),
            Some("7".to_string()),
        )
        .unwrap();
        assert_eq!(
            mode,
            ScheduledResearchRunnerMode::Disabled { sleep_seconds: 7 }
        );
    }

    #[test]
    fn scheduled_runner_enabled_mode_uses_tick_interval() {
        let mode = parse_scheduled_research_runner_mode(
            Some("true".to_string()),
            Some("11".to_string()),
            Some("7".to_string()),
        )
        .unwrap();
        assert_eq!(
            mode,
            ScheduledResearchRunnerMode::Enabled {
                interval_seconds: 11
            }
        );
    }
}
