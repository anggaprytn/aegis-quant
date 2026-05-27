use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use aegis_core::{
    calculate_research_shadow_pnl_attribution, evaluate_research_candidate_shadow_performance,
    Candle, CandleInterval, ExecutionReadinessStatus, MarketDataSource, ResearchBatchResult,
    ResearchBatchStatus, ResearchBatchStep, ResearchBatchStepStatus, ResearchCampaignBatchPlan,
    ResearchCampaignBatchResult, ResearchCampaignResult, ResearchCampaignStatus, ResearchCandidate,
    ResearchCandidateDecision, ResearchCandidateLifecycleEvent,
    ResearchCandidateQualificationEvaluation, ResearchCandidateQualificationRecommendation,
    ResearchCandidateQualificationStatus, ResearchCandidateQualificationThresholds,
    ResearchCandidateReview, ResearchCandidateReviewAction, ResearchCandidateReviewStatus,
    ResearchCandidateShadowPerformance, ResearchCandidateShadowRunLink, ResearchCandidateStatus,
    ResearchCandidateWalkForwardEvidence, ResearchDataCoverageResult, ResearchDatasetBuildRequest,
    ResearchDatasetBuildResult, ResearchDatasetBuildStatus, ResearchDatasetBuildStep,
    ResearchDatasetBuildStepStatus, ResearchShadowPnlAttributionRequest,
    ResearchShadowPnlAttributionResult, ResearchShadowPnlRunInput,
    StrategyCandidateObservationDecision, StrategyCandidateObservationRequirement,
    StrategyCandidateObservationResult, StrategyCandidateObservationStatus,
    StrategyCandidateObservationSummary, StrategyResearchCandidate,
    StrategyResearchCandidateEvidence, StrategyResearchCandidatePromotionResult,
    StrategyResearchCandidateScore, StrategyResearchCandidateSource,
    StrategyResearchCandidateStatus, StrategyWalkForwardRecommendation,
    StrategyWalkForwardRobustnessStatus, Symbol,
};

use crate::{PgPool, TestnetShadowRunRecord};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchDatasetBuildRecord {
    pub id: Uuid,
    pub exchange: String,
    pub symbol: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub requested_intervals: Value,
    pub status: String,
    pub coverage_before: Value,
    pub coverage_after: Value,
    pub failed_reason: Option<String>,
    pub correlation_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchDatasetBuildStepRecord {
    pub id: Uuid,
    pub build_id: Uuid,
    pub step_index: i32,
    pub step_name: String,
    pub status: String,
    pub details: Option<Value>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchBatchRecord {
    pub id: Uuid,
    pub request: Value,
    pub status: String,
    pub summary: Value,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchBatchStepRecord {
    pub id: Uuid,
    pub batch_id: Uuid,
    pub step_name: String,
    pub status: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub result: Value,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCampaignRecord {
    pub id: Uuid,
    pub request: Value,
    pub status: String,
    pub summary: Value,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub correlation_id: Option<Uuid>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCampaignBatchRecord {
    pub id: Uuid,
    pub campaign_id: Uuid,
    pub research_batch_id: Option<Uuid>,
    pub plan_index: i32,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub status: String,
    pub triage_status: String,
    pub candidates_created: i32,
    pub summary: Value,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyResearchCandidateRecord {
    pub id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub config: Value,
    pub source_type: String,
    pub source_id: Option<Uuid>,
    pub evidence: Value,
    pub score: Decimal,
    pub status: String,
    pub warnings: Value,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub promoted_at: Option<DateTime<Utc>>,
    pub promoted_by: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyResearchCandidatePromotionRecord {
    pub id: Uuid,
    pub candidate_id: Uuid,
    pub previous_config: Option<Value>,
    pub promoted_config: Value,
    pub status: String,
    pub actor_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyCandidateObservationRecord {
    pub id: Uuid,
    pub candidate_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub status: String,
    pub requirements: Value,
    pub summary: Value,
    pub decision: String,
    pub started_at: DateTime<Utc>,
    pub evaluated_at: DateTime<Utc>,
    pub last_observed_at: DateTime<Utc>,
    pub observation_expires_at: Option<DateTime<Utc>>,
    pub observation_max_age_seconds: Option<i64>,
    pub observation_snapshot_hash: Option<String>,
    pub runner_config_snapshot: Option<Value>,
    pub readiness_snapshot: Option<Value>,
    pub created_by: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyCandidateObservationCheckRecord {
    pub id: Uuid,
    pub observation_id: Uuid,
    pub finding_index: i32,
    pub code: String,
    pub message: String,
    pub blocking: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateShadowRunLinkRecord {
    pub candidate_id: Uuid,
    pub shadow_run_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateRecord {
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
    pub status: String,
    pub rejection_reason: Option<String>,
    pub notes: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateEventRecord {
    pub id: Uuid,
    pub candidate_id: Uuid,
    pub previous_status: Option<String>,
    pub next_status: String,
    pub decision: String,
    pub reason: Option<String>,
    pub notes: Option<String>,
    pub actor_id: Option<Uuid>,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateQualificationEvaluationRecord {
    pub id: Uuid,
    pub candidate_id: Uuid,
    pub status: String,
    pub score: i32,
    pub latest_readiness_status: Option<String>,
    pub total_shadow_runs: i64,
    pub would_submit_count: i64,
    pub risk_rejection_rate_pct: Option<Decimal>,
    pub walk_forward_status: Option<String>,
    pub walk_forward_run_id: Option<Uuid>,
    pub walk_forward_score: Option<Decimal>,
    pub walk_forward_consistency_score: Option<Decimal>,
    pub walk_forward_recommendation: Option<String>,
    pub walk_forward_blockers: Value,
    pub walk_forward_warnings: Value,
    pub warnings: Value,
    pub blockers: Value,
    pub recommendations: Value,
    pub thresholds: Value,
    pub evaluated_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateReviewRecord {
    pub id: Uuid,
    pub candidate_id: Uuid,
    pub action: String,
    pub status: String,
    pub previous_candidate_status: String,
    pub next_candidate_status: Option<String>,
    pub reason: Option<String>,
    pub notes: Option<String>,
    pub actor_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
    pub qualification_evaluation_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchCandidateWatchlistRow {
    pub candidate_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub candidate_status: String,
    pub evaluation_id: Option<Uuid>,
    pub evaluation_status: Option<String>,
    pub evaluation_score: Option<i32>,
    pub latest_readiness_status: Option<String>,
    pub total_shadow_runs: Option<i64>,
    pub would_submit_count: Option<i64>,
    pub risk_rejection_rate_pct: Option<Decimal>,
    pub warnings: Option<Value>,
    pub blockers: Option<Value>,
    pub recommendations: Option<Value>,
    pub thresholds: Option<Value>,
    pub evaluated_at: Option<DateTime<Utc>>,
    pub correlation_id: Option<Uuid>,
    pub walk_forward_run_id: Option<Uuid>,
    pub walk_forward_robustness_status: Option<String>,
    pub walk_forward_status: Option<String>,
    pub walk_forward_recommendation: Option<Value>,
    pub walk_forward_total_windows: Option<i32>,
    pub walk_forward_completed_windows: Option<i32>,
    pub walk_forward_profitable_windows: Option<i32>,
    pub walk_forward_losing_windows: Option<i32>,
    pub walk_forward_avg_pnl_pct: Option<Decimal>,
    pub walk_forward_worst_pnl_pct: Option<Decimal>,
    pub walk_forward_best_pnl_pct: Option<Decimal>,
    pub walk_forward_robustness_score: Option<Decimal>,
    pub walk_forward_consistency_score: Option<Decimal>,
    pub walk_forward_created_at: Option<DateTime<Utc>>,
    pub walk_forward_linked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
pub struct StrategyResearchCandidateListFilters {
    pub strategy_id: Option<String>,
    pub symbol: Option<String>,
    pub timeframe: Option<String>,
    pub status: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ResearchCandidateListFilters {
    pub strategy_id: Option<String>,
    pub symbol: Option<String>,
    pub timeframe: Option<String>,
    pub status: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResearchCandidateShadowRunsQuery {
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub limit: i64,
}

#[derive(Debug, Clone)]
pub struct ResearchCandidateShadowPerformanceWindow {
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShadowRunCandidateMatchOutcome {
    NotFound,
    Matched(Uuid),
    Ambiguous,
}

fn legacy_status_for_research_candidate(
    status: ResearchCandidateStatus,
) -> StrategyResearchCandidateStatus {
    match status {
        ResearchCandidateStatus::Discovered | ResearchCandidateStatus::Observing => {
            StrategyResearchCandidateStatus::Registered
        }
        ResearchCandidateStatus::AcceptedForShadow => StrategyResearchCandidateStatus::Registered,
        ResearchCandidateStatus::PromotedToShadowConfig => {
            StrategyResearchCandidateStatus::PromotedToShadowConfig
        }
        ResearchCandidateStatus::Rejected => StrategyResearchCandidateStatus::Rejected,
        ResearchCandidateStatus::Archived => StrategyResearchCandidateStatus::Archived,
    }
}

fn map_research_candidate_qualification_evaluation(
    row: sqlx::postgres::PgRow,
) -> ResearchCandidateQualificationEvaluationRecord {
    ResearchCandidateQualificationEvaluationRecord {
        id: row.get("id"),
        candidate_id: row.get("candidate_id"),
        status: row.get("status"),
        score: row.get("score"),
        latest_readiness_status: row.get("latest_readiness_status"),
        total_shadow_runs: row.get("total_shadow_runs"),
        would_submit_count: row.get("would_submit_count"),
        risk_rejection_rate_pct: row.get("risk_rejection_rate_pct"),
        walk_forward_status: row.get("walk_forward_status"),
        walk_forward_run_id: row.get("walk_forward_run_id"),
        walk_forward_score: row.get("walk_forward_score"),
        walk_forward_consistency_score: row.get("walk_forward_consistency_score"),
        walk_forward_recommendation: row.get("walk_forward_recommendation"),
        walk_forward_blockers: row.get("walk_forward_blockers"),
        walk_forward_warnings: row.get("walk_forward_warnings"),
        warnings: row.get("warnings"),
        blockers: row.get("blockers"),
        recommendations: row.get("recommendations"),
        thresholds: row.get("thresholds"),
        evaluated_at: row.get("evaluated_at"),
        correlation_id: row.get("correlation_id"),
    }
}

fn map_research_candidate_watchlist_row(
    row: sqlx::postgres::PgRow,
) -> ResearchCandidateWatchlistRow {
    ResearchCandidateWatchlistRow {
        candidate_id: row.get("candidate_id"),
        strategy_id: row.get("strategy_id"),
        symbol: row.get("symbol"),
        timeframe: row.get("timeframe"),
        candidate_status: row.get("candidate_status"),
        evaluation_id: row.get("evaluation_id"),
        evaluation_status: row.get("evaluation_status"),
        evaluation_score: row.get("evaluation_score"),
        latest_readiness_status: row.get("latest_readiness_status"),
        total_shadow_runs: row.get("total_shadow_runs"),
        would_submit_count: row.get("would_submit_count"),
        risk_rejection_rate_pct: row.get("risk_rejection_rate_pct"),
        warnings: row.get("warnings"),
        blockers: row.get("blockers"),
        recommendations: row.get("recommendations"),
        thresholds: row.get("thresholds"),
        evaluated_at: row.get("evaluated_at"),
        correlation_id: row.get("correlation_id"),
        walk_forward_run_id: row.get("walk_forward_run_id"),
        walk_forward_robustness_status: row.get("walk_forward_robustness_status"),
        walk_forward_status: row.get("walk_forward_status"),
        walk_forward_recommendation: row.get("walk_forward_recommendation"),
        walk_forward_total_windows: row.get("walk_forward_total_windows"),
        walk_forward_completed_windows: row.get("walk_forward_completed_windows"),
        walk_forward_profitable_windows: row.get("walk_forward_profitable_windows"),
        walk_forward_losing_windows: row.get("walk_forward_losing_windows"),
        walk_forward_avg_pnl_pct: row.get("walk_forward_avg_pnl_pct"),
        walk_forward_worst_pnl_pct: row.get("walk_forward_worst_pnl_pct"),
        walk_forward_best_pnl_pct: row.get("walk_forward_best_pnl_pct"),
        walk_forward_robustness_score: row.get("walk_forward_robustness_score"),
        walk_forward_consistency_score: row.get("walk_forward_consistency_score"),
        walk_forward_created_at: row.get("walk_forward_created_at"),
        walk_forward_linked_at: row.get("walk_forward_linked_at"),
    }
}

fn map_research_candidate_review(row: sqlx::postgres::PgRow) -> ResearchCandidateReviewRecord {
    ResearchCandidateReviewRecord {
        id: row.get("id"),
        candidate_id: row.get("candidate_id"),
        action: row.get("action"),
        status: row.get("status"),
        previous_candidate_status: row.get("previous_candidate_status"),
        next_candidate_status: row.get("next_candidate_status"),
        reason: row.get("reason"),
        notes: row.get("notes"),
        actor_id: row.get("actor_id"),
        created_at: row.get("created_at"),
        correlation_id: row.get("correlation_id"),
        qualification_evaluation_id: row.get("qualification_evaluation_id"),
    }
}

fn map_research_batch(row: sqlx::postgres::PgRow) -> ResearchBatchRecord {
    ResearchBatchRecord {
        id: row.get("id"),
        request: row.get("request"),
        status: row.get("status"),
        summary: row.get("summary"),
        created_at: row.get("created_at"),
        completed_at: row.get("completed_at"),
        correlation_id: row.get("correlation_id"),
    }
}

fn map_research_batch_step(row: sqlx::postgres::PgRow) -> ResearchBatchStepRecord {
    ResearchBatchStepRecord {
        id: row.get("id"),
        batch_id: row.get("batch_id"),
        step_name: row.get("step_name"),
        status: row.get("status"),
        started_at: row.get("started_at"),
        completed_at: row.get("completed_at"),
        result: row.get("result"),
        error: row.get("error"),
    }
}

fn map_research_campaign(row: sqlx::postgres::PgRow) -> ResearchCampaignRecord {
    ResearchCampaignRecord {
        id: row.get("id"),
        request: row.get("request"),
        status: row.get("status"),
        summary: row.get("summary"),
        created_at: row.get("created_at"),
        completed_at: row.get("completed_at"),
        correlation_id: row.get("correlation_id"),
        error: row.get("error"),
    }
}

fn map_research_campaign_batch(row: sqlx::postgres::PgRow) -> ResearchCampaignBatchRecord {
    ResearchCampaignBatchRecord {
        id: row.get("id"),
        campaign_id: row.get("campaign_id"),
        research_batch_id: row.get("research_batch_id"),
        plan_index: row.get("plan_index"),
        strategy_id: row.get("strategy_id"),
        symbol: row.get("symbol"),
        timeframe: row.get("timeframe"),
        window_start: row.get("window_start"),
        window_end: row.get("window_end"),
        status: row.get("status"),
        triage_status: row.get("triage_status"),
        candidates_created: row.get("candidates_created"),
        summary: row.get("summary"),
        error: row.get("error"),
        created_at: row.get("created_at"),
        completed_at: row.get("completed_at"),
    }
}

pub fn research_campaign_batch_result_from_record(
    record: &ResearchCampaignBatchRecord,
) -> Result<ResearchCampaignBatchResult> {
    let mut result: ResearchCampaignBatchResult = serde_json::from_value(record.summary.clone())?;
    result.research_batch_id = record.research_batch_id;
    result.batch_status = Some(record.status.parse()?);
    result.triage_status = record
        .triage_status
        .parse()
        .unwrap_or(aegis_core::ResearchBatchTriageStatus::Unknown);
    result.candidates_created = record.candidates_created;
    result.error = record.error.clone();
    result.started_at = record.created_at;
    result.completed_at = record.completed_at;
    Ok(result)
}

pub fn research_campaign_result_from_records(
    record: &ResearchCampaignRecord,
    batch_records: &[ResearchCampaignBatchRecord],
) -> Result<ResearchCampaignResult> {
    let mut result: ResearchCampaignResult = serde_json::from_value(record.summary.clone())?;
    result.campaign_id = record.id;
    result.status = record.status.parse()?;
    result.batches = batch_records
        .iter()
        .map(research_campaign_batch_result_from_record)
        .collect::<Result<Vec<_>>>()?;
    result.created_at = record.created_at;
    result.completed_at = record.completed_at;
    Ok(result)
}

pub fn research_batch_step_from_record(
    record: &ResearchBatchStepRecord,
) -> Result<ResearchBatchStep> {
    Ok(ResearchBatchStep {
        id: record.id,
        batch_id: record.batch_id,
        step_name: record.step_name.clone(),
        status: record.status.parse::<ResearchBatchStepStatus>()?,
        started_at: record.started_at,
        completed_at: record.completed_at,
        result: Some(record.result.clone()).filter(|value| !value.is_null()),
        error: record.error.clone(),
    })
}

pub fn research_batch_result_from_records(
    record: &ResearchBatchRecord,
    step_records: &[ResearchBatchStepRecord],
) -> Result<ResearchBatchResult> {
    let mut result: ResearchBatchResult = serde_json::from_value(record.summary.clone())?;
    result.batch_id = record.id;
    result.status = record.status.parse::<ResearchBatchStatus>()?;
    result.steps = step_records
        .iter()
        .map(research_batch_step_from_record)
        .collect::<Result<Vec<_>>>()?;
    result.created_at = record.created_at;
    result.completed_at = record.completed_at;
    Ok(result)
}

pub async fn insert_research_batch(
    pool: &PgPool,
    result: &ResearchBatchResult,
    request: &Value,
    summary: &Value,
    correlation_id: Option<Uuid>,
) -> Result<ResearchBatchRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO research_batches (
            id,
            request,
            status,
            summary,
            created_at,
            completed_at,
            correlation_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING
            id,
            request,
            status,
            summary,
            created_at,
            completed_at,
            correlation_id
        "#,
    )
    .bind(result.batch_id)
    .bind(request)
    .bind(result.status.as_str())
    .bind(summary)
    .bind(result.created_at)
    .bind(result.completed_at)
    .bind(correlation_id)
    .fetch_one(pool)
    .await?;

    Ok(map_research_batch(row))
}

pub async fn update_research_batch_summary(
    pool: &PgPool,
    batch_id: Uuid,
    status: ResearchBatchStatus,
    summary: &Value,
    completed_at: Option<DateTime<Utc>>,
) -> Result<Option<ResearchBatchRecord>> {
    let row = sqlx::query(
        r#"
        UPDATE research_batches
        SET status = $2,
            summary = $3,
            completed_at = $4
        WHERE id = $1
        RETURNING
            id,
            request,
            status,
            summary,
            created_at,
            completed_at,
            correlation_id
        "#,
    )
    .bind(batch_id)
    .bind(status.as_str())
    .bind(summary)
    .bind(completed_at)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(map_research_batch))
}

pub async fn get_research_batch(
    pool: &PgPool,
    batch_id: Uuid,
) -> Result<Option<ResearchBatchRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            request,
            status,
            summary,
            created_at,
            completed_at,
            correlation_id
        FROM research_batches
        WHERE id = $1
        "#,
    )
    .bind(batch_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(map_research_batch))
}

pub async fn list_research_batches(pool: &PgPool, limit: i64) -> Result<Vec<ResearchBatchRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            request,
            status,
            summary,
            created_at,
            completed_at,
            correlation_id
        FROM research_batches
        ORDER BY created_at DESC, id DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(map_research_batch).collect())
}

pub async fn insert_research_batch_step(
    pool: &PgPool,
    batch_id: Uuid,
    step_name: &str,
    status: ResearchBatchStepStatus,
    result: &Value,
    error: Option<&str>,
) -> Result<ResearchBatchStepRecord> {
    let completed_at: Option<DateTime<Utc>> = match status {
        ResearchBatchStepStatus::Running | ResearchBatchStepStatus::Pending => None,
        _ => Some(Utc::now()),
    };
    let row = sqlx::query(
        r#"
        INSERT INTO research_batch_steps (
            id,
            batch_id,
            step_name,
            status,
            started_at,
            completed_at,
            result,
            error
        )
        VALUES ($1, $2, $3, $4, NOW(), $5, $6, $7)
        RETURNING
            id,
            batch_id,
            step_name,
            status,
            started_at,
            completed_at,
            result,
            error
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(batch_id)
    .bind(step_name)
    .bind(status.as_str())
    .bind(completed_at)
    .bind(result)
    .bind(error)
    .fetch_one(pool)
    .await?;

    Ok(map_research_batch_step(row))
}

pub async fn complete_research_batch_step(
    pool: &PgPool,
    step_id: Uuid,
    status: ResearchBatchStepStatus,
    result: &Value,
    error: Option<&str>,
) -> Result<Option<ResearchBatchStepRecord>> {
    let row = sqlx::query(
        r#"
        UPDATE research_batch_steps
        SET status = $2,
            completed_at = NOW(),
            result = $3,
            error = $4
        WHERE id = $1
        RETURNING
            id,
            batch_id,
            step_name,
            status,
            started_at,
            completed_at,
            result,
            error
        "#,
    )
    .bind(step_id)
    .bind(status.as_str())
    .bind(result)
    .bind(error)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(map_research_batch_step))
}

pub async fn list_research_batch_steps(
    pool: &PgPool,
    batch_id: Uuid,
) -> Result<Vec<ResearchBatchStepRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            batch_id,
            step_name,
            status,
            started_at,
            completed_at,
            result,
            error
        FROM research_batch_steps
        WHERE batch_id = $1
        ORDER BY started_at ASC, id ASC
        "#,
    )
    .bind(batch_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(map_research_batch_step).collect())
}

pub async fn insert_research_campaign(
    pool: &PgPool,
    result: &ResearchCampaignResult,
    request: &Value,
    summary: &Value,
    correlation_id: Option<Uuid>,
    error: Option<&str>,
) -> Result<ResearchCampaignRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO research_campaigns (
            id,
            request,
            status,
            summary,
            created_at,
            completed_at,
            correlation_id,
            error
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING
            id,
            request,
            status,
            summary,
            created_at,
            completed_at,
            correlation_id,
            error
        "#,
    )
    .bind(result.campaign_id)
    .bind(request)
    .bind(result.status.as_str())
    .bind(summary)
    .bind(result.created_at)
    .bind(result.completed_at)
    .bind(correlation_id)
    .bind(error)
    .fetch_one(pool)
    .await?;

    Ok(map_research_campaign(row))
}

pub async fn update_research_campaign_summary(
    pool: &PgPool,
    campaign_id: Uuid,
    status: ResearchCampaignStatus,
    summary: &Value,
    completed_at: Option<DateTime<Utc>>,
    error: Option<&str>,
) -> Result<Option<ResearchCampaignRecord>> {
    let row = sqlx::query(
        r#"
        UPDATE research_campaigns
        SET status = $2,
            summary = $3,
            completed_at = $4,
            error = $5
        WHERE id = $1
        RETURNING
            id,
            request,
            status,
            summary,
            created_at,
            completed_at,
            correlation_id,
            error
        "#,
    )
    .bind(campaign_id)
    .bind(status.as_str())
    .bind(summary)
    .bind(completed_at)
    .bind(error)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(map_research_campaign))
}

pub async fn get_research_campaign(
    pool: &PgPool,
    campaign_id: Uuid,
) -> Result<Option<ResearchCampaignRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            request,
            status,
            summary,
            created_at,
            completed_at,
            correlation_id,
            error
        FROM research_campaigns
        WHERE id = $1
        "#,
    )
    .bind(campaign_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(map_research_campaign))
}

pub async fn list_research_campaigns(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<ResearchCampaignRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            request,
            status,
            summary,
            created_at,
            completed_at,
            correlation_id,
            error
        FROM research_campaigns
        ORDER BY created_at DESC, id DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(map_research_campaign).collect())
}

pub async fn insert_research_campaign_batch(
    pool: &PgPool,
    campaign_id: Uuid,
    plan: &ResearchCampaignBatchPlan,
    status: ResearchBatchStatus,
    triage_status: aegis_core::ResearchBatchTriageStatus,
    summary: &Value,
    error: Option<&str>,
) -> Result<ResearchCampaignBatchRecord> {
    let completed_at: Option<DateTime<Utc>> = if status == ResearchBatchStatus::Started {
        None
    } else {
        Some(Utc::now())
    };
    let row = sqlx::query(
        r#"
        INSERT INTO research_campaign_batches (
            id,
            campaign_id,
            research_batch_id,
            plan_index,
            strategy_id,
            symbol,
            timeframe,
            window_start,
            window_end,
            status,
            triage_status,
            candidates_created,
            summary,
            error,
            completed_at
        )
        VALUES ($1, $2, NULL, $3, $4, $5, $6, $7, $8, $9, $10, 0, $11, $12, $13)
        RETURNING
            id,
            campaign_id,
            research_batch_id,
            plan_index,
            strategy_id,
            symbol,
            timeframe,
            window_start,
            window_end,
            status,
            triage_status,
            candidates_created,
            summary,
            error,
            created_at,
            completed_at
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(campaign_id)
    .bind(plan.plan_index)
    .bind(&plan.strategy_id)
    .bind(&plan.symbol)
    .bind(&plan.timeframe)
    .bind(plan.start_time)
    .bind(plan.end_time)
    .bind(status.as_str())
    .bind(triage_status.as_str())
    .bind(summary)
    .bind(error)
    .bind(completed_at)
    .fetch_one(pool)
    .await?;

    Ok(map_research_campaign_batch(row))
}

pub async fn update_research_campaign_batch(
    pool: &PgPool,
    id: Uuid,
    research_batch_id: Option<Uuid>,
    status: ResearchBatchStatus,
    triage_status: aegis_core::ResearchBatchTriageStatus,
    candidates_created: i32,
    summary: &Value,
    error: Option<&str>,
) -> Result<Option<ResearchCampaignBatchRecord>> {
    let completed_at: Option<DateTime<Utc>> = if status == ResearchBatchStatus::Started {
        None
    } else {
        Some(Utc::now())
    };
    let row = sqlx::query(
        r#"
        UPDATE research_campaign_batches
        SET research_batch_id = $2,
            status = $3,
            triage_status = $4,
            candidates_created = $5,
            summary = $6,
            error = $7,
            completed_at = $8
        WHERE id = $1
        RETURNING
            id,
            campaign_id,
            research_batch_id,
            plan_index,
            strategy_id,
            symbol,
            timeframe,
            window_start,
            window_end,
            status,
            triage_status,
            candidates_created,
            summary,
            error,
            created_at,
            completed_at
        "#,
    )
    .bind(id)
    .bind(research_batch_id)
    .bind(status.as_str())
    .bind(triage_status.as_str())
    .bind(candidates_created)
    .bind(summary)
    .bind(error)
    .bind(completed_at)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(map_research_campaign_batch))
}

pub async fn list_research_campaign_batches(
    pool: &PgPool,
    campaign_id: Uuid,
) -> Result<Vec<ResearchCampaignBatchRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            campaign_id,
            research_batch_id,
            plan_index,
            strategy_id,
            symbol,
            timeframe,
            window_start,
            window_end,
            status,
            triage_status,
            candidates_created,
            summary,
            error,
            created_at,
            completed_at
        FROM research_campaign_batches
        WHERE campaign_id = $1
        ORDER BY plan_index ASC, id ASC
        "#,
    )
    .bind(campaign_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(map_research_campaign_batch).collect())
}

fn parse_qualification_status(value: &str) -> Result<ResearchCandidateQualificationStatus> {
    match value.trim().to_ascii_uppercase().as_str() {
        "QUALIFIED" => Ok(ResearchCandidateQualificationStatus::Qualified),
        "NOT_QUALIFIED" => Ok(ResearchCandidateQualificationStatus::NotQualified),
        "NEEDS_MORE_DATA" => Ok(ResearchCandidateQualificationStatus::NeedsMoreData),
        "DEGRADED" => Ok(ResearchCandidateQualificationStatus::Degraded),
        "UNKNOWN" => Ok(ResearchCandidateQualificationStatus::Unknown),
        other => anyhow::bail!("unsupported research candidate qualification status: {other}"),
    }
}

fn parse_qualification_recommendation(
    value: &str,
) -> Result<ResearchCandidateQualificationRecommendation> {
    match value.trim().to_ascii_uppercase().as_str() {
        "REFRESH_CANDIDATE_OBSERVATION" => {
            Ok(ResearchCandidateQualificationRecommendation::RefreshCandidateObservation)
        }
        "FIX_RUNNER_ALIGNMENT" => {
            Ok(ResearchCandidateQualificationRecommendation::FixRunnerAlignment)
        }
        "EXPAND_SHADOW_RUNNER_COVERAGE" => {
            Ok(ResearchCandidateQualificationRecommendation::ExpandShadowRunnerCoverage)
        }
        "GATHER_MORE_SHADOW_RUNS" => {
            Ok(ResearchCandidateQualificationRecommendation::GatherMoreShadowRuns)
        }
        "GENERATE_MORE_WOULD_SUBMIT_EVIDENCE" => {
            Ok(ResearchCandidateQualificationRecommendation::GenerateMoreWouldSubmitEvidence)
        }
        "REVIEW_RISK_REJECTIONS" => {
            Ok(ResearchCandidateQualificationRecommendation::ReviewRiskRejections)
        }
        "REDUCE_SHADOW_ERRORS_OR_SKIPS" => {
            Ok(ResearchCandidateQualificationRecommendation::ReduceShadowErrorsOrSkips)
        }
        "RESTORE_TESTNET_SHADOW_READINESS" => {
            Ok(ResearchCandidateQualificationRecommendation::RestoreTestnetShadowReadiness)
        }
        "RE_ACCEPT_CANDIDATE_FOR_SHADOW" => {
            Ok(ResearchCandidateQualificationRecommendation::ReAcceptCandidateForShadow)
        }
        "READY_FOR_TESTNET_PROMOTION_CONSIDERATION" => {
            Ok(ResearchCandidateQualificationRecommendation::ReadyForTestnetPromotionConsideration)
        }
        other => {
            anyhow::bail!("unsupported research candidate qualification recommendation: {other}")
        }
    }
}

fn parse_research_candidate_review_action(value: &str) -> Result<ResearchCandidateReviewAction> {
    value.parse().map_err(Into::into)
}

fn parse_research_candidate_review_status(value: &str) -> Result<ResearchCandidateReviewStatus> {
    value.parse().map_err(Into::into)
}

fn parse_execution_readiness_status(value: &str) -> Result<ExecutionReadinessStatus> {
    match value.trim().to_ascii_uppercase().as_str() {
        "READY" => Ok(ExecutionReadinessStatus::Ready),
        "NOT_READY" => Ok(ExecutionReadinessStatus::NotReady),
        "DEGRADED" => Ok(ExecutionReadinessStatus::Degraded),
        "UNKNOWN" => Ok(ExecutionReadinessStatus::Unknown),
        other => anyhow::bail!("unsupported readiness status: {other}"),
    }
}

fn walk_forward_evidence_from_parts(
    walk_forward_run_id: Option<Uuid>,
    robustness_status: Option<String>,
    status: Option<String>,
    recommendation: Option<Value>,
    total_windows: Option<i32>,
    completed_windows: Option<i32>,
    profitable_windows: Option<i32>,
    losing_windows: Option<i32>,
    avg_pnl_pct: Option<Decimal>,
    worst_pnl_pct: Option<Decimal>,
    best_pnl_pct: Option<Decimal>,
    robustness_score: Option<Decimal>,
    consistency_score: Option<Decimal>,
    created_at: Option<DateTime<Utc>>,
    linked_at: Option<DateTime<Utc>>,
) -> Result<Option<ResearchCandidateWalkForwardEvidence>> {
    let Some(walk_forward_run_id) = walk_forward_run_id else {
        return Ok(None);
    };
    let recommendation = recommendation
        .and_then(|value| serde_json::from_value::<StrategyWalkForwardRecommendation>(value).ok());

    Ok(Some(ResearchCandidateWalkForwardEvidence {
        walk_forward_run_id,
        robustness_status: robustness_status
            .unwrap_or_else(|| {
                StrategyWalkForwardRobustnessStatus::InsufficientData
                    .as_str()
                    .to_string()
            })
            .parse()?,
        status: status.unwrap_or_else(|| "UNKNOWN".to_string()),
        recommendation_action: recommendation.as_ref().map(|value| value.action.clone()),
        recommendation_reason: recommendation.as_ref().map(|value| value.reason.clone()),
        total_windows: total_windows.unwrap_or_default(),
        completed_windows: completed_windows.unwrap_or_default(),
        profitable_windows: profitable_windows.unwrap_or_default(),
        losing_windows: losing_windows.unwrap_or_default(),
        avg_pnl_pct: avg_pnl_pct.unwrap_or(Decimal::ZERO),
        worst_pnl_pct: worst_pnl_pct.unwrap_or(Decimal::ZERO),
        best_pnl_pct: best_pnl_pct.unwrap_or(Decimal::ZERO),
        robustness_score: robustness_score.unwrap_or(Decimal::ZERO),
        consistency_score: consistency_score.unwrap_or(Decimal::ZERO),
        created_at: created_at.unwrap_or_else(Utc::now),
        linked_at: linked_at.unwrap_or_else(Utc::now),
    }))
}

fn walk_forward_evidence_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ResearchCandidateWalkForwardEvidence> {
    walk_forward_evidence_from_parts(
        Some(row.get("walk_forward_run_id")),
        Some(row.get("walk_forward_robustness_status")),
        Some(row.get("walk_forward_status")),
        row.get("walk_forward_recommendation"),
        Some(row.get("walk_forward_total_windows")),
        Some(row.get("walk_forward_completed_windows")),
        Some(row.get("walk_forward_profitable_windows")),
        Some(row.get("walk_forward_losing_windows")),
        Some(row.get("walk_forward_avg_pnl_pct")),
        Some(row.get("walk_forward_worst_pnl_pct")),
        Some(row.get("walk_forward_best_pnl_pct")),
        Some(row.get("walk_forward_robustness_score")),
        Some(row.get("walk_forward_consistency_score")),
        Some(row.get("walk_forward_created_at")),
        Some(row.get("walk_forward_linked_at")),
    )?
    .ok_or_else(|| anyhow::anyhow!("walk-forward evidence row was empty"))
}

pub fn research_candidate_walk_forward_evidence_from_watchlist_row(
    row: &ResearchCandidateWatchlistRow,
) -> Result<Option<ResearchCandidateWalkForwardEvidence>> {
    walk_forward_evidence_from_parts(
        row.walk_forward_run_id,
        row.walk_forward_robustness_status.clone(),
        row.walk_forward_status.clone(),
        row.walk_forward_recommendation.clone(),
        row.walk_forward_total_windows,
        row.walk_forward_completed_windows,
        row.walk_forward_profitable_windows,
        row.walk_forward_losing_windows,
        row.walk_forward_avg_pnl_pct,
        row.walk_forward_worst_pnl_pct,
        row.walk_forward_best_pnl_pct,
        row.walk_forward_robustness_score,
        row.walk_forward_consistency_score,
        row.walk_forward_created_at,
        row.walk_forward_linked_at,
    )
}

pub fn research_candidate_qualification_evaluation_from_record(
    record: &ResearchCandidateQualificationEvaluationRecord,
) -> Result<ResearchCandidateQualificationEvaluation> {
    Ok(ResearchCandidateQualificationEvaluation {
        id: record.id,
        candidate_id: record.candidate_id,
        status: parse_qualification_status(&record.status)?,
        score: record.score,
        latest_readiness_status: record
            .latest_readiness_status
            .as_deref()
            .map(parse_execution_readiness_status)
            .transpose()?,
        total_shadow_runs: record.total_shadow_runs,
        would_submit_count: record.would_submit_count,
        risk_rejection_rate_pct: record.risk_rejection_rate_pct,
        walk_forward_status: record
            .walk_forward_status
            .as_deref()
            .map(str::parse)
            .transpose()?,
        walk_forward_run_id: record.walk_forward_run_id,
        walk_forward_score: record.walk_forward_score,
        walk_forward_consistency_score: record.walk_forward_consistency_score,
        walk_forward_recommendation: record.walk_forward_recommendation.clone(),
        walk_forward_blockers: serde_json::from_value(record.walk_forward_blockers.clone())?,
        walk_forward_warnings: serde_json::from_value(record.walk_forward_warnings.clone())?,
        warnings: serde_json::from_value(record.warnings.clone())?,
        blockers: serde_json::from_value(record.blockers.clone())?,
        recommendations: serde_json::from_value::<Vec<String>>(record.recommendations.clone())?
            .into_iter()
            .map(|value| parse_qualification_recommendation(&value))
            .collect::<Result<Vec<_>>>()?,
        thresholds: serde_json::from_value::<ResearchCandidateQualificationThresholds>(
            record.thresholds.clone(),
        )?,
        evaluated_at: record.evaluated_at,
        correlation_id: record.correlation_id,
    })
}

pub fn research_candidate_review_from_record(
    record: &ResearchCandidateReviewRecord,
) -> Result<ResearchCandidateReview> {
    Ok(ResearchCandidateReview {
        id: record.id,
        candidate_id: record.candidate_id,
        action: parse_research_candidate_review_action(&record.action)?,
        status: parse_research_candidate_review_status(&record.status)?,
        previous_candidate_status: record.previous_candidate_status.parse()?,
        next_candidate_status: record
            .next_candidate_status
            .as_deref()
            .map(str::parse)
            .transpose()?,
        reason: record.reason.clone(),
        notes: record.notes.clone(),
        actor_id: record.actor_id,
        created_at: record.created_at,
        correlation_id: record.correlation_id,
        qualification_evaluation_id: record.qualification_evaluation_id,
    })
}

pub async fn create_research_candidate(
    pool: &PgPool,
    candidate: &ResearchCandidate,
    actor_id: Option<Uuid>,
    event_decision: ResearchCandidateDecision,
    reason: Option<&str>,
    notes: Option<&str>,
    payload: &Value,
) -> Result<(ResearchCandidateRecord, ResearchCandidateEventRecord)> {
    let mut tx = pool.begin().await?;
    let legacy_source_type = if candidate.experiment_run_id.is_some() {
        StrategyResearchCandidateSource::ExperimentRun
    } else {
        StrategyResearchCandidateSource::Manual
    };
    let legacy_status = legacy_status_for_research_candidate(candidate.status);
    let legacy_evidence = StrategyResearchCandidateEvidence {
        experiment_id: candidate.experiment_id,
        experiment_run_id: candidate.experiment_run_id,
        walk_forward_id: None,
        pnl_pct: candidate.pnl_pct,
        max_drawdown_pct: candidate.max_drawdown_pct,
        win_rate: candidate.win_rate,
        trade_count: candidate.trade_count,
        fee_paid: None,
        slippage_cost: None,
        robustness_score: None,
        profitable_windows: None,
        losing_windows: None,
        skipped_windows: None,
        notes: candidate.notes.clone(),
    };
    sqlx::query(
        r#"
        INSERT INTO strategy_research_candidates (
            id,
            strategy_id,
            symbol,
            timeframe,
            config,
            source_type,
            source_id,
            evidence,
            score,
            status,
            warnings,
            created_by,
            created_at,
            updated_at,
            promoted_at,
            promoted_by,
            correlation_id
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, '[]'::JSONB, $11, $12, $13, NULL, NULL, $14
        )
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(candidate.id)
    .bind(&candidate.strategy_id)
    .bind(candidate.symbol.trim().to_ascii_uppercase())
    .bind(&candidate.timeframe)
    .bind(candidate.config.clone())
    .bind(legacy_source_type.as_str())
    .bind(candidate.experiment_run_id)
    .bind(serde_json::to_value(&legacy_evidence)?)
    .bind(candidate.score.unwrap_or(Decimal::ZERO))
    .bind(legacy_status.as_str())
    .bind(actor_id)
    .bind(candidate.created_at)
    .bind(candidate.updated_at)
    .bind(candidate.correlation_id)
    .execute(&mut *tx)
    .await?;

    let candidate_row = sqlx::query(
        r#"
        INSERT INTO research_candidates (
            id,
            experiment_id,
            experiment_run_id,
            strategy_id,
            symbol,
            timeframe,
            config,
            score,
            pnl_pct,
            max_drawdown_pct,
            trade_count,
            win_rate,
            fee_drag,
            status,
            rejection_reason,
            notes,
            created_at,
            updated_at,
            correlation_id
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
        )
        RETURNING
            id,
            experiment_id,
            experiment_run_id,
            strategy_id,
            symbol,
            timeframe,
            config,
            score,
            pnl_pct,
            max_drawdown_pct,
            trade_count,
            win_rate,
            fee_drag,
            status,
            rejection_reason,
            notes,
            created_at,
            updated_at,
            correlation_id
        "#,
    )
    .bind(candidate.id)
    .bind(candidate.experiment_id)
    .bind(candidate.experiment_run_id)
    .bind(&candidate.strategy_id)
    .bind(candidate.symbol.trim().to_ascii_uppercase())
    .bind(&candidate.timeframe)
    .bind(candidate.config.clone())
    .bind(candidate.score)
    .bind(candidate.pnl_pct)
    .bind(candidate.max_drawdown_pct)
    .bind(candidate.trade_count)
    .bind(candidate.win_rate)
    .bind(candidate.fee_drag)
    .bind(candidate.status.as_str())
    .bind(&candidate.rejection_reason)
    .bind(&candidate.notes)
    .bind(candidate.created_at)
    .bind(candidate.updated_at)
    .bind(candidate.correlation_id)
    .fetch_one(&mut *tx)
    .await?;

    let event_row = sqlx::query(
        r#"
        INSERT INTO research_candidate_events (
            id,
            candidate_id,
            previous_status,
            next_status,
            decision,
            reason,
            notes,
            actor_id,
            payload,
            created_at,
            correlation_id
        )
        VALUES ($1, $2, NULL, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING
            id,
            candidate_id,
            previous_status,
            next_status,
            decision,
            reason,
            notes,
            actor_id,
            payload,
            created_at,
            correlation_id
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(candidate.id)
    .bind(candidate.status.as_str())
    .bind(event_decision.as_str())
    .bind(reason)
    .bind(notes)
    .bind(actor_id)
    .bind(payload.clone())
    .bind(candidate.created_at)
    .bind(candidate.correlation_id)
    .fetch_one(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok((
        map_research_candidate(candidate_row),
        map_research_candidate_event(event_row),
    ))
}

pub async fn list_research_candidates(
    pool: &PgPool,
    filters: &ResearchCandidateListFilters,
    limit: i64,
) -> Result<Vec<ResearchCandidateRecord>> {
    let mut builder = sqlx::QueryBuilder::new(
        r#"
        SELECT
            id,
            experiment_id,
            experiment_run_id,
            strategy_id,
            symbol,
            timeframe,
            config,
            score,
            pnl_pct,
            max_drawdown_pct,
            trade_count,
            win_rate,
            fee_drag,
            status,
            rejection_reason,
            notes,
            created_at,
            updated_at,
            correlation_id
        FROM research_candidates
        WHERE 1 = 1
        "#,
    );

    if let Some(strategy_id) = &filters.strategy_id {
        builder.push(" AND strategy_id = ");
        builder.push_bind(strategy_id);
    }
    if let Some(symbol) = &filters.symbol {
        builder.push(" AND symbol = ");
        builder.push_bind(symbol.trim().to_ascii_uppercase());
    }
    if let Some(timeframe) = &filters.timeframe {
        builder.push(" AND timeframe = ");
        builder.push_bind(timeframe);
    }
    if let Some(status) = &filters.status {
        builder.push(" AND status = ");
        builder.push_bind(status);
    }

    builder.push(" ORDER BY created_at DESC LIMIT ");
    builder.push_bind(limit);

    let rows = builder.build().fetch_all(pool).await?;
    Ok(rows.into_iter().map(map_research_candidate).collect())
}

pub async fn get_research_candidate(
    pool: &PgPool,
    candidate_id: Uuid,
) -> Result<Option<ResearchCandidateRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            experiment_id,
            experiment_run_id,
            strategy_id,
            symbol,
            timeframe,
            config,
            score,
            pnl_pct,
            max_drawdown_pct,
            trade_count,
            win_rate,
            fee_drag,
            status,
            rejection_reason,
            notes,
            created_at,
            updated_at,
            correlation_id
        FROM research_candidates
        WHERE id = $1
        "#,
    )
    .bind(candidate_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(map_research_candidate))
}

pub async fn update_research_candidate_status(
    pool: &PgPool,
    candidate_id: Uuid,
    next_status: ResearchCandidateStatus,
    rejection_reason: Option<&str>,
    notes: Option<&str>,
    updated_at: DateTime<Utc>,
    correlation_id: Option<Uuid>,
) -> Result<Option<ResearchCandidateRecord>> {
    let mut tx = pool.begin().await?;
    let row = sqlx::query(
        r#"
        UPDATE research_candidates
        SET status = $2,
            rejection_reason = $3,
            notes = COALESCE($4, notes),
            updated_at = $5,
            correlation_id = COALESCE($6, correlation_id)
        WHERE id = $1
        RETURNING
            id,
            experiment_id,
            experiment_run_id,
            strategy_id,
            symbol,
            timeframe,
            config,
            score,
            pnl_pct,
            max_drawdown_pct,
            trade_count,
            win_rate,
            fee_drag,
            status,
            rejection_reason,
            notes,
            created_at,
            updated_at,
            correlation_id
        "#,
    )
    .bind(candidate_id)
    .bind(next_status.as_str())
    .bind(rejection_reason)
    .bind(notes)
    .bind(updated_at)
    .bind(correlation_id)
    .fetch_optional(&mut *tx)
    .await?;

    if row.is_some() {
        sqlx::query(
            r#"
            UPDATE strategy_research_candidates
            SET status = $2,
                updated_at = $3,
                correlation_id = COALESCE($4, correlation_id),
                promoted_at = CASE
                    WHEN $2 = 'PROMOTED_TO_SHADOW_CONFIG' THEN COALESCE(promoted_at, $3)
                    ELSE promoted_at
                END
            WHERE id = $1
            "#,
        )
        .bind(candidate_id)
        .bind(legacy_status_for_research_candidate(next_status).as_str())
        .bind(updated_at)
        .bind(correlation_id)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(row.map(map_research_candidate))
}

pub async fn append_research_candidate_event(
    pool: &PgPool,
    event: &ResearchCandidateLifecycleEvent,
) -> Result<ResearchCandidateEventRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO research_candidate_events (
            id,
            candidate_id,
            previous_status,
            next_status,
            decision,
            reason,
            notes,
            actor_id,
            payload,
            created_at,
            correlation_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        RETURNING
            id,
            candidate_id,
            previous_status,
            next_status,
            decision,
            reason,
            notes,
            actor_id,
            payload,
            created_at,
            correlation_id
        "#,
    )
    .bind(event.id)
    .bind(event.candidate_id)
    .bind(
        event
            .previous_status
            .map(|value| value.as_str().to_string()),
    )
    .bind(event.next_status.as_str())
    .bind(event.decision.as_str())
    .bind(&event.reason)
    .bind(&event.notes)
    .bind(event.actor_id)
    .bind(event.payload.clone())
    .bind(event.created_at)
    .bind(event.correlation_id)
    .fetch_one(pool)
    .await?;

    Ok(map_research_candidate_event(row))
}

pub async fn list_research_candidate_events(
    pool: &PgPool,
    candidate_id: Uuid,
) -> Result<Vec<ResearchCandidateEventRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            candidate_id,
            previous_status,
            next_status,
            decision,
            reason,
            notes,
            actor_id,
            payload,
            created_at,
            correlation_id
        FROM research_candidate_events
        WHERE candidate_id = $1
        ORDER BY created_at ASC, id ASC
        "#,
    )
    .bind(candidate_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(map_research_candidate_event).collect())
}

pub async fn insert_research_candidate_review(
    pool: &PgPool,
    review: &ResearchCandidateReview,
) -> Result<ResearchCandidateReviewRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO research_candidate_reviews (
            id,
            candidate_id,
            action,
            status,
            previous_candidate_status,
            next_candidate_status,
            reason,
            notes,
            actor_id,
            created_at,
            correlation_id,
            qualification_evaluation_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        RETURNING
            id,
            candidate_id,
            action,
            status,
            previous_candidate_status,
            next_candidate_status,
            reason,
            notes,
            actor_id,
            created_at,
            correlation_id,
            qualification_evaluation_id
        "#,
    )
    .bind(review.id)
    .bind(review.candidate_id)
    .bind(review.action.as_str())
    .bind(review.status.as_str())
    .bind(review.previous_candidate_status.as_str())
    .bind(
        review
            .next_candidate_status
            .map(|value| value.as_str().to_string()),
    )
    .bind(&review.reason)
    .bind(&review.notes)
    .bind(review.actor_id)
    .bind(review.created_at)
    .bind(review.correlation_id)
    .bind(review.qualification_evaluation_id)
    .fetch_one(pool)
    .await?;

    Ok(map_research_candidate_review(row))
}

pub async fn list_research_candidate_reviews(
    pool: &PgPool,
    candidate_id: Uuid,
) -> Result<Vec<ResearchCandidateReviewRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            candidate_id,
            action,
            status,
            previous_candidate_status,
            next_candidate_status,
            reason,
            notes,
            actor_id,
            created_at,
            correlation_id,
            qualification_evaluation_id
        FROM research_candidate_reviews
        WHERE candidate_id = $1
        ORDER BY created_at DESC, id DESC
        "#,
    )
    .bind(candidate_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(map_research_candidate_review)
        .collect())
}

pub async fn apply_research_candidate_review(
    pool: &PgPool,
    review: &ResearchCandidateReview,
    lifecycle_event: Option<&ResearchCandidateLifecycleEvent>,
) -> Result<(
    ResearchCandidateReviewRecord,
    Option<ResearchCandidateRecord>,
)> {
    let mut tx = pool.begin().await?;
    let updated_candidate = if let Some(next_status) = review.next_candidate_status {
        let row = sqlx::query(
            r#"
            UPDATE research_candidates
            SET status = $2,
                rejection_reason = $3,
                notes = COALESCE($4, notes),
                updated_at = $5,
                correlation_id = COALESCE($6, correlation_id)
            WHERE id = $1
            RETURNING
                id,
                experiment_id,
                experiment_run_id,
                strategy_id,
                symbol,
                timeframe,
                config,
                score,
                pnl_pct,
                max_drawdown_pct,
                trade_count,
                win_rate,
                fee_drag,
                status,
                rejection_reason,
                notes,
                created_at,
                updated_at,
                correlation_id
            "#,
        )
        .bind(review.candidate_id)
        .bind(next_status.as_str())
        .bind(if next_status == ResearchCandidateStatus::Rejected {
            review.reason.as_deref()
        } else {
            None
        })
        .bind(review.notes.as_deref())
        .bind(review.created_at)
        .bind(review.correlation_id)
        .fetch_optional(&mut *tx)
        .await?;

        if row.is_some() {
            sqlx::query(
                r#"
                UPDATE strategy_research_candidates
                SET status = $2,
                    updated_at = $3,
                    correlation_id = COALESCE($4, correlation_id),
                    promoted_at = CASE
                        WHEN $2 = 'PROMOTED_TO_SHADOW_CONFIG' THEN COALESCE(promoted_at, $3)
                        ELSE promoted_at
                    END
                WHERE id = $1
                "#,
            )
            .bind(review.candidate_id)
            .bind(legacy_status_for_research_candidate(next_status).as_str())
            .bind(review.created_at)
            .bind(review.correlation_id)
            .execute(&mut *tx)
            .await?;
        }

        row.map(map_research_candidate)
    } else {
        None
    };

    let review_row = sqlx::query(
        r#"
        INSERT INTO research_candidate_reviews (
            id,
            candidate_id,
            action,
            status,
            previous_candidate_status,
            next_candidate_status,
            reason,
            notes,
            actor_id,
            created_at,
            correlation_id,
            qualification_evaluation_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        RETURNING
            id,
            candidate_id,
            action,
            status,
            previous_candidate_status,
            next_candidate_status,
            reason,
            notes,
            actor_id,
            created_at,
            correlation_id,
            qualification_evaluation_id
        "#,
    )
    .bind(review.id)
    .bind(review.candidate_id)
    .bind(review.action.as_str())
    .bind(review.status.as_str())
    .bind(review.previous_candidate_status.as_str())
    .bind(
        review
            .next_candidate_status
            .map(|value| value.as_str().to_string()),
    )
    .bind(&review.reason)
    .bind(&review.notes)
    .bind(review.actor_id)
    .bind(review.created_at)
    .bind(review.correlation_id)
    .bind(review.qualification_evaluation_id)
    .fetch_one(&mut *tx)
    .await?;

    if let Some(event) = lifecycle_event {
        sqlx::query(
            r#"
            INSERT INTO research_candidate_events (
                id,
                candidate_id,
                previous_status,
                next_status,
                decision,
                reason,
                notes,
                actor_id,
                payload,
                created_at,
                correlation_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            "#,
        )
        .bind(event.id)
        .bind(event.candidate_id)
        .bind(
            event
                .previous_status
                .map(|value| value.as_str().to_string()),
        )
        .bind(event.next_status.as_str())
        .bind(event.decision.as_str())
        .bind(&event.reason)
        .bind(&event.notes)
        .bind(event.actor_id)
        .bind(event.payload.clone())
        .bind(event.created_at)
        .bind(event.correlation_id)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok((map_research_candidate_review(review_row), updated_candidate))
}

pub async fn get_latest_strategy_candidate_observation(
    pool: &PgPool,
    candidate_id: Uuid,
) -> Result<Option<StrategyCandidateObservationRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            candidate_id,
            strategy_id,
            symbol,
            timeframe,
            status,
            requirements,
            summary,
            decision,
            started_at,
            evaluated_at,
            last_observed_at,
            observation_expires_at,
            observation_max_age_seconds,
            observation_snapshot_hash,
            runner_config_snapshot,
            readiness_snapshot,
            created_by,
            correlation_id
        FROM strategy_candidate_observations
        WHERE candidate_id = $1
        ORDER BY evaluated_at DESC, id DESC
        LIMIT 1
        "#,
    )
    .bind(candidate_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(map_strategy_candidate_observation))
}

pub async fn insert_strategy_research_candidate(
    pool: &PgPool,
    candidate: &StrategyResearchCandidate,
    created_by: Option<Uuid>,
) -> Result<StrategyResearchCandidateRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO strategy_research_candidates (
            id,
            strategy_id,
            symbol,
            timeframe,
            config,
            source_type,
            source_id,
            evidence,
            score,
            status,
            warnings,
            created_by,
            created_at,
            updated_at,
            promoted_at,
            promoted_by,
            correlation_id
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $13, $14, $15, $16
        )
        RETURNING
            id,
            strategy_id,
            symbol,
            timeframe,
            config,
            source_type,
            source_id,
            evidence,
            score,
            status,
            warnings,
            created_by,
            created_at,
            updated_at,
            promoted_at,
            promoted_by,
            correlation_id
        "#,
    )
    .bind(candidate.id)
    .bind(&candidate.strategy_id)
    .bind(&candidate.symbol)
    .bind(&candidate.timeframe)
    .bind(candidate.config.clone())
    .bind(candidate.source_type.as_str())
    .bind(candidate.source_id)
    .bind(serde_json::to_value(&candidate.evidence)?)
    .bind(candidate.score.score)
    .bind(candidate.status.as_str())
    .bind(serde_json::to_value(&candidate.score.warnings)?)
    .bind(created_by)
    .bind(candidate.created_at)
    .bind(candidate.promoted_at)
    .bind(candidate.promoted_by)
    .bind(candidate.correlation_id)
    .fetch_one(pool)
    .await?;

    Ok(map_strategy_research_candidate(row))
}

pub async fn list_strategy_research_candidates(
    pool: &PgPool,
    filters: &StrategyResearchCandidateListFilters,
    limit: i64,
) -> Result<Vec<StrategyResearchCandidateRecord>> {
    let mut builder = sqlx::QueryBuilder::new(
        r#"
        SELECT
            id,
            strategy_id,
            symbol,
            timeframe,
            config,
            source_type,
            source_id,
            evidence,
            score,
            status,
            warnings,
            created_by,
            created_at,
            updated_at,
            promoted_at,
            promoted_by,
            correlation_id
        FROM strategy_research_candidates
        WHERE 1 = 1
        "#,
    );

    if let Some(strategy_id) = &filters.strategy_id {
        builder.push(" AND strategy_id = ");
        builder.push_bind(strategy_id);
    }
    if let Some(symbol) = &filters.symbol {
        builder.push(" AND symbol = ");
        builder.push_bind(symbol.trim().to_ascii_uppercase());
    }
    if let Some(timeframe) = &filters.timeframe {
        builder.push(" AND timeframe = ");
        builder.push_bind(timeframe);
    }
    if let Some(status) = &filters.status {
        builder.push(" AND status = ");
        builder.push_bind(status);
    }

    builder.push(" ORDER BY created_at DESC LIMIT ");
    builder.push_bind(limit);

    let rows = builder.build().fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .map(map_strategy_research_candidate)
        .collect())
}

pub async fn get_strategy_research_candidate(
    pool: &PgPool,
    candidate_id: Uuid,
) -> Result<Option<StrategyResearchCandidateRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            strategy_id,
            symbol,
            timeframe,
            config,
            source_type,
            source_id,
            evidence,
            score,
            status,
            warnings,
            created_by,
            created_at,
            updated_at,
            promoted_at,
            promoted_by,
            correlation_id
        FROM strategy_research_candidates
        WHERE id = $1
        "#,
    )
    .bind(candidate_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(map_strategy_research_candidate))
}

pub async fn get_active_strategy_research_candidate_promotion(
    pool: &PgPool,
    candidate_id: Uuid,
) -> Result<Option<StrategyResearchCandidatePromotionRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            candidate_id,
            previous_config,
            promoted_config,
            status,
            actor_id,
            created_at,
            correlation_id
        FROM strategy_research_candidate_promotions
        WHERE candidate_id = $1
          AND status = 'PROMOTED_TO_SHADOW_CONFIG'
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(candidate_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(map_strategy_research_candidate_promotion))
}

pub async fn insert_strategy_candidate_observation(
    pool: &PgPool,
    observation: &StrategyCandidateObservationResult,
) -> Result<StrategyCandidateObservationRecord> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        r#"
        INSERT INTO strategy_research_candidates (
            id,
            strategy_id,
            symbol,
            timeframe,
            config,
            source_type,
            source_id,
            evidence,
            score,
            status,
            warnings,
            created_by,
            created_at,
            updated_at,
            promoted_at,
            promoted_by,
            correlation_id
        )
        SELECT
            id,
            strategy_id,
            symbol,
            timeframe,
            config,
            CASE
                WHEN experiment_run_id IS NULL THEN 'MANUAL'
                ELSE 'EXPERIMENT_RUN'
            END,
            experiment_run_id,
            jsonb_build_object(
                'experiment_id', experiment_id,
                'experiment_run_id', experiment_run_id,
                'walk_forward_id', NULL,
                'pnl_pct', pnl_pct,
                'max_drawdown_pct', max_drawdown_pct,
                'win_rate', win_rate,
                'trade_count', trade_count,
                'fee_paid', NULL,
                'slippage_cost', NULL,
                'robustness_score', NULL,
                'profitable_windows', NULL,
                'losing_windows', NULL,
                'skipped_windows', NULL,
                'notes', notes
            ),
            COALESCE(score, 0),
            CASE
                WHEN status IN ('DISCOVERED', 'OBSERVING') THEN 'REGISTERED'
                WHEN status = 'PROMOTED_TO_SHADOW_CONFIG' THEN 'PROMOTED_TO_SHADOW_CONFIG'
                WHEN status = 'REJECTED' THEN 'REJECTED'
                WHEN status = 'ARCHIVED' THEN 'ARCHIVED'
                ELSE 'REGISTERED'
            END,
            '[]'::JSONB,
            NULL,
            created_at,
            updated_at,
            CASE
                WHEN status = 'PROMOTED_TO_SHADOW_CONFIG' THEN updated_at
                ELSE NULL
            END,
            NULL,
            correlation_id
        FROM research_candidates
        WHERE id = $1
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(observation.candidate_id)
    .execute(&mut *tx)
    .await?;

    let row = sqlx::query(
        r#"
        INSERT INTO strategy_candidate_observations (
            id,
            candidate_id,
            strategy_id,
            symbol,
            timeframe,
            status,
            requirements,
            summary,
            decision,
            started_at,
            evaluated_at,
            last_observed_at,
            observation_expires_at,
            observation_max_age_seconds,
            observation_snapshot_hash,
            runner_config_snapshot,
            readiness_snapshot,
            created_by,
            correlation_id
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19
        )
        RETURNING
            id,
            candidate_id,
            strategy_id,
            symbol,
            timeframe,
            status,
            requirements,
            summary,
            decision,
            started_at,
            evaluated_at,
            last_observed_at,
            observation_expires_at,
            observation_max_age_seconds,
            observation_snapshot_hash,
            runner_config_snapshot,
            readiness_snapshot,
            created_by,
            correlation_id
        "#,
    )
    .bind(observation.observation_id)
    .bind(observation.candidate_id)
    .bind(&observation.strategy_id)
    .bind(&observation.symbol)
    .bind(&observation.timeframe)
    .bind(observation.status.as_str())
    .bind(serde_json::to_value(&observation.requirements)?)
    .bind(serde_json::to_value(&observation.summary)?)
    .bind(observation.decision.as_str())
    .bind(observation.started_at)
    .bind(observation.evaluated_at)
    .bind(observation.last_observed_at)
    .bind(observation.observation_expires_at)
    .bind(observation.observation_max_age_seconds)
    .bind(&observation.observation_snapshot_hash)
    .bind(observation.runner_config_snapshot.clone())
    .bind(observation.readiness_snapshot.clone())
    .bind(observation.created_by)
    .bind(observation.correlation_id)
    .fetch_one(&mut *tx)
    .await?;

    for (index, finding) in observation.summary.findings.iter().enumerate() {
        sqlx::query(
            r#"
            INSERT INTO strategy_candidate_observation_checks (
                id,
                observation_id,
                finding_index,
                code,
                message,
                blocking,
                created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(observation.observation_id)
        .bind(index as i32)
        .bind(&finding.code)
        .bind(&finding.message)
        .bind(finding.blocking)
        .bind(observation.evaluated_at)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(map_strategy_candidate_observation(row))
}

pub async fn get_strategy_candidate_observation(
    pool: &PgPool,
    observation_id: Uuid,
) -> Result<Option<StrategyCandidateObservationRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            candidate_id,
            strategy_id,
            symbol,
            timeframe,
            status,
            requirements,
            summary,
            decision,
            started_at,
            evaluated_at,
            last_observed_at,
            observation_expires_at,
            observation_max_age_seconds,
            observation_snapshot_hash,
            runner_config_snapshot,
            readiness_snapshot,
            created_by,
            correlation_id
        FROM strategy_candidate_observations
        WHERE id = $1
        "#,
    )
    .bind(observation_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(map_strategy_candidate_observation))
}

pub async fn list_strategy_candidate_observations(
    pool: &PgPool,
    candidate_id: Uuid,
) -> Result<Vec<StrategyCandidateObservationRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            candidate_id,
            strategy_id,
            symbol,
            timeframe,
            status,
            requirements,
            summary,
            decision,
            started_at,
            evaluated_at,
            last_observed_at,
            observation_expires_at,
            observation_max_age_seconds,
            observation_snapshot_hash,
            runner_config_snapshot,
            readiness_snapshot,
            created_by,
            correlation_id
        FROM strategy_candidate_observations
        WHERE candidate_id = $1
        ORDER BY evaluated_at DESC, id DESC
        "#,
    )
    .bind(candidate_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(map_strategy_candidate_observation)
        .collect())
}

pub async fn list_strategy_candidate_observation_checks(
    pool: &PgPool,
    observation_id: Uuid,
) -> Result<Vec<StrategyCandidateObservationCheckRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            observation_id,
            finding_index,
            code,
            message,
            blocking,
            created_at
        FROM strategy_candidate_observation_checks
        WHERE observation_id = $1
        ORDER BY finding_index ASC, created_at ASC
        "#,
    )
    .bind(observation_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(map_strategy_candidate_observation_check)
        .collect())
}

pub async fn list_testnet_shadow_runs_in_window(
    pool: &PgPool,
    strategy_id: &str,
    symbol: &str,
    timeframe: &str,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
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
        WHERE strategy_id = $1
          AND symbol = $2
          AND timeframe = $3
          AND created_at >= $4
          AND created_at <= $5
        ORDER BY created_at ASC, id ASC
        "#,
    )
    .bind(strategy_id)
    .bind(symbol.trim().to_ascii_uppercase())
    .bind(timeframe)
    .bind(start_time)
    .bind(end_time)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| TestnetShadowRunRecord {
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
            reasons: serde_json::from_value(row.get("reasons")).unwrap_or_default(),
            status: row.get("status"),
            created_at: row.get("created_at"),
            correlation_id: row.get("correlation_id"),
        })
        .collect())
}

fn bounded_research_candidate_shadow_run_limit(limit: i64) -> i64 {
    limit.clamp(1, 500)
}

fn bounded_research_candidate_qualification_history_limit(limit: i64) -> i64 {
    limit.clamp(1, 500)
}

pub async fn resolve_promoted_research_candidate_for_shadow_run(
    pool: &PgPool,
    strategy_id: &str,
    symbol: &str,
    timeframe: &str,
) -> Result<ShadowRunCandidateMatchOutcome> {
    let rows = sqlx::query(
        r#"
        SELECT
            legacy.id,
            legacy.promoted_at
        FROM strategy_research_candidates legacy
        INNER JOIN research_candidates candidate
            ON candidate.id = legacy.id
           AND candidate.status = 'PROMOTED_TO_SHADOW_CONFIG'
        WHERE legacy.status = 'PROMOTED_TO_SHADOW_CONFIG'
          AND legacy.promoted_at IS NOT NULL
          AND legacy.strategy_id = $1
          AND legacy.symbol = $2
          AND legacy.timeframe = $3
        ORDER BY legacy.promoted_at DESC, legacy.created_at DESC, legacy.id DESC
        LIMIT 2
        "#,
    )
    .bind(strategy_id.trim().to_ascii_lowercase())
    .bind(symbol.trim().to_ascii_uppercase())
    .bind(timeframe.trim().to_ascii_lowercase())
    .fetch_all(pool)
    .await?;

    let Some(first) = rows.first() else {
        return Ok(ShadowRunCandidateMatchOutcome::NotFound);
    };
    let first_id = first.get::<Uuid, _>("id");
    let first_promoted_at = first.get::<DateTime<Utc>, _>("promoted_at");

    if rows.len() > 1 {
        let second_promoted_at = rows[1].get::<DateTime<Utc>, _>("promoted_at");
        if second_promoted_at == first_promoted_at {
            return Ok(ShadowRunCandidateMatchOutcome::Ambiguous);
        }
    }

    Ok(ShadowRunCandidateMatchOutcome::Matched(first_id))
}

pub async fn insert_research_candidate_shadow_run_link(
    pool: &PgPool,
    candidate_id: Uuid,
    shadow_run_id: Uuid,
    created_at: DateTime<Utc>,
) -> Result<Option<ResearchCandidateShadowRunLinkRecord>> {
    let row = sqlx::query(
        r#"
        INSERT INTO research_candidate_shadow_runs (
            candidate_id,
            shadow_run_id,
            created_at
        )
        SELECT $1, $2, $3
        WHERE EXISTS (
            SELECT 1
            FROM research_candidates
            WHERE id = $1
              AND status = 'PROMOTED_TO_SHADOW_CONFIG'
        )
        ON CONFLICT (shadow_run_id) DO NOTHING
        RETURNING candidate_id, shadow_run_id, created_at
        "#,
    )
    .bind(candidate_id)
    .bind(shadow_run_id)
    .bind(created_at)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| ResearchCandidateShadowRunLinkRecord {
        candidate_id: row.get("candidate_id"),
        shadow_run_id: row.get("shadow_run_id"),
        created_at: row.get("created_at"),
    }))
}

pub async fn list_research_candidate_shadow_runs(
    pool: &PgPool,
    candidate_id: Uuid,
    query: &ResearchCandidateShadowRunsQuery,
) -> Result<Vec<ResearchCandidateShadowRunLink>> {
    let rows = sqlx::query(
        r#"
        SELECT
            link.candidate_id,
            link.shadow_run_id,
            link.created_at AS linked_at,
            run.strategy_id,
            run.symbol,
            run.timeframe,
            run.decision,
            run.status,
            run.signal_id,
            run.risk_decision_id,
            run.created_at AS shadow_created_at,
            run.correlation_id
        FROM research_candidate_shadow_runs link
        INNER JOIN testnet_shadow_runs run
            ON run.id = link.shadow_run_id
        WHERE link.candidate_id = $1
          AND run.created_at >= $2
          AND run.created_at <= $3
        ORDER BY run.created_at DESC, run.id DESC
        LIMIT $4
        "#,
    )
    .bind(candidate_id)
    .bind(query.start_time)
    .bind(query.end_time)
    .bind(bounded_research_candidate_shadow_run_limit(query.limit))
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| ResearchCandidateShadowRunLink {
            candidate_id: row.get("candidate_id"),
            shadow_run_id: row.get("shadow_run_id"),
            strategy_id: row.get("strategy_id"),
            symbol: row.get("symbol"),
            timeframe: row.get("timeframe"),
            decision: row.get("decision"),
            status: row.get("status"),
            signal_id: row.get("signal_id"),
            risk_decision_id: row.get("risk_decision_id"),
            linked_at: row.get("linked_at"),
            shadow_created_at: row.get("shadow_created_at"),
            correlation_id: row.get("correlation_id"),
        })
        .collect())
}

pub async fn link_research_candidate_walk_forward_run(
    pool: &PgPool,
    candidate_id: Uuid,
    walk_forward_run_id: Uuid,
) -> Result<ResearchCandidateWalkForwardEvidence> {
    sqlx::query(
        r#"
        INSERT INTO research_candidate_walk_forward_runs (
            candidate_id,
            walk_forward_run_id
        )
        VALUES ($1, $2)
        ON CONFLICT (candidate_id, walk_forward_run_id) DO NOTHING
        "#,
    )
    .bind(candidate_id)
    .bind(walk_forward_run_id)
    .execute(pool)
    .await?;

    get_research_candidate_walk_forward_evidence_by_run(pool, candidate_id, walk_forward_run_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("linked walk-forward evidence was not found"))
}

pub async fn list_research_candidate_walk_forward_evidence(
    pool: &PgPool,
    candidate_id: Uuid,
) -> Result<Vec<ResearchCandidateWalkForwardEvidence>> {
    let rows = sqlx::query(
        r#"
        SELECT
            run.id AS walk_forward_run_id,
            run.robustness_status AS walk_forward_robustness_status,
            run.status AS walk_forward_status,
            run.recommendation AS walk_forward_recommendation,
            run.total_windows AS walk_forward_total_windows,
            run.completed_windows AS walk_forward_completed_windows,
            run.profitable_test_windows AS walk_forward_profitable_windows,
            run.losing_test_windows AS walk_forward_losing_windows,
            run.avg_test_pnl_pct AS walk_forward_avg_pnl_pct,
            run.worst_test_pnl_pct AS walk_forward_worst_pnl_pct,
            run.best_test_pnl_pct AS walk_forward_best_pnl_pct,
            run.robustness_score AS walk_forward_robustness_score,
            run.consistency_score AS walk_forward_consistency_score,
            run.created_at AS walk_forward_created_at,
            link.created_at AS walk_forward_linked_at
        FROM research_candidate_walk_forward_runs link
        INNER JOIN strategy_walk_forward_runs run
            ON run.id = link.walk_forward_run_id
        WHERE link.candidate_id = $1
        ORDER BY
            CASE WHEN run.status = 'COMPLETED' THEN 0 ELSE 1 END,
            run.created_at DESC,
            link.created_at DESC
        "#,
    )
    .bind(candidate_id)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(walk_forward_evidence_from_row)
        .collect()
}

pub async fn get_latest_research_candidate_walk_forward_evidence(
    pool: &PgPool,
    candidate_id: Uuid,
) -> Result<Option<ResearchCandidateWalkForwardEvidence>> {
    Ok(
        list_research_candidate_walk_forward_evidence(pool, candidate_id)
            .await?
            .into_iter()
            .next(),
    )
}

async fn get_research_candidate_walk_forward_evidence_by_run(
    pool: &PgPool,
    candidate_id: Uuid,
    walk_forward_run_id: Uuid,
) -> Result<Option<ResearchCandidateWalkForwardEvidence>> {
    let row = sqlx::query(
        r#"
        SELECT
            run.id AS walk_forward_run_id,
            run.robustness_status AS walk_forward_robustness_status,
            run.status AS walk_forward_status,
            run.recommendation AS walk_forward_recommendation,
            run.total_windows AS walk_forward_total_windows,
            run.completed_windows AS walk_forward_completed_windows,
            run.profitable_test_windows AS walk_forward_profitable_windows,
            run.losing_test_windows AS walk_forward_losing_windows,
            run.avg_test_pnl_pct AS walk_forward_avg_pnl_pct,
            run.worst_test_pnl_pct AS walk_forward_worst_pnl_pct,
            run.best_test_pnl_pct AS walk_forward_best_pnl_pct,
            run.robustness_score AS walk_forward_robustness_score,
            run.consistency_score AS walk_forward_consistency_score,
            run.created_at AS walk_forward_created_at,
            link.created_at AS walk_forward_linked_at
        FROM research_candidate_walk_forward_runs link
        INNER JOIN strategy_walk_forward_runs run
            ON run.id = link.walk_forward_run_id
        WHERE link.candidate_id = $1
          AND link.walk_forward_run_id = $2
        "#,
    )
    .bind(candidate_id)
    .bind(walk_forward_run_id)
    .fetch_optional(pool)
    .await?;

    row.map(walk_forward_evidence_from_row).transpose()
}

pub async fn insert_research_candidate_qualification_evaluation(
    pool: &PgPool,
    evaluation: &ResearchCandidateQualificationEvaluation,
) -> Result<ResearchCandidateQualificationEvaluationRecord> {
    let recommendations = evaluation
        .recommendations
        .iter()
        .map(|value| value.as_str().to_string())
        .collect::<Vec<_>>();
    let row = sqlx::query(
        r#"
        INSERT INTO research_candidate_qualification_evaluations (
            id,
            candidate_id,
            status,
            score,
            latest_readiness_status,
            total_shadow_runs,
            would_submit_count,
            risk_rejection_rate_pct,
            walk_forward_status,
            walk_forward_run_id,
            walk_forward_score,
            walk_forward_consistency_score,
            walk_forward_recommendation,
            walk_forward_blockers,
            walk_forward_warnings,
            warnings,
            blockers,
            recommendations,
            thresholds,
            evaluated_at,
            correlation_id
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
        )
        RETURNING
            id,
            candidate_id,
            status,
            score,
            latest_readiness_status,
            total_shadow_runs::BIGINT AS total_shadow_runs,
            would_submit_count::BIGINT AS would_submit_count,
            risk_rejection_rate_pct,
            walk_forward_status,
            walk_forward_run_id,
            walk_forward_score,
            walk_forward_consistency_score,
            walk_forward_recommendation,
            walk_forward_blockers,
            walk_forward_warnings,
            warnings,
            blockers,
            recommendations,
            thresholds,
            evaluated_at,
            correlation_id
        "#,
    )
    .bind(evaluation.id)
    .bind(evaluation.candidate_id)
    .bind(evaluation.status.as_str())
    .bind(evaluation.score)
    .bind(
        evaluation
            .latest_readiness_status
            .map(|value| value.as_str().to_string()),
    )
    .bind(evaluation.total_shadow_runs)
    .bind(evaluation.would_submit_count)
    .bind(evaluation.risk_rejection_rate_pct)
    .bind(evaluation.walk_forward_status.map(|value| value.as_str().to_string()))
    .bind(evaluation.walk_forward_run_id)
    .bind(evaluation.walk_forward_score)
    .bind(evaluation.walk_forward_consistency_score)
    .bind(&evaluation.walk_forward_recommendation)
    .bind(serde_json::to_value(&evaluation.walk_forward_blockers)?)
    .bind(serde_json::to_value(&evaluation.walk_forward_warnings)?)
    .bind(serde_json::to_value(&evaluation.warnings)?)
    .bind(serde_json::to_value(&evaluation.blockers)?)
    .bind(serde_json::to_value(&recommendations)?)
    .bind(serde_json::to_value(&evaluation.thresholds)?)
    .bind(evaluation.evaluated_at)
    .bind(evaluation.correlation_id)
    .fetch_one(pool)
    .await?;

    Ok(map_research_candidate_qualification_evaluation(row))
}

pub async fn get_latest_research_candidate_qualification_evaluation(
    pool: &PgPool,
    candidate_id: Uuid,
) -> Result<Option<ResearchCandidateQualificationEvaluationRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            candidate_id,
            status,
            score,
            latest_readiness_status,
            total_shadow_runs::BIGINT AS total_shadow_runs,
            would_submit_count::BIGINT AS would_submit_count,
            risk_rejection_rate_pct,
            walk_forward_status,
            walk_forward_run_id,
            walk_forward_score,
            walk_forward_consistency_score,
            walk_forward_recommendation,
            walk_forward_blockers,
            walk_forward_warnings,
            warnings,
            blockers,
            recommendations,
            thresholds,
            evaluated_at,
            correlation_id
        FROM research_candidate_qualification_evaluations
        WHERE candidate_id = $1
        ORDER BY evaluated_at DESC, id DESC
        LIMIT 1
        "#,
    )
    .bind(candidate_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(map_research_candidate_qualification_evaluation))
}

pub async fn get_research_candidate_qualification_evaluation_by_id(
    pool: &PgPool,
    evaluation_id: Uuid,
) -> Result<Option<ResearchCandidateQualificationEvaluationRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            candidate_id,
            status,
            score,
            latest_readiness_status,
            total_shadow_runs::BIGINT AS total_shadow_runs,
            would_submit_count::BIGINT AS would_submit_count,
            risk_rejection_rate_pct,
            walk_forward_status,
            walk_forward_run_id,
            walk_forward_score,
            walk_forward_consistency_score,
            walk_forward_recommendation,
            walk_forward_blockers,
            walk_forward_warnings,
            warnings,
            blockers,
            recommendations,
            thresholds,
            evaluated_at,
            correlation_id
        FROM research_candidate_qualification_evaluations
        WHERE id = $1
        "#,
    )
    .bind(evaluation_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(map_research_candidate_qualification_evaluation))
}

pub async fn list_research_candidate_qualification_evaluations(
    pool: &PgPool,
    candidate_id: Uuid,
    limit: i64,
) -> Result<Vec<ResearchCandidateQualificationEvaluationRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            candidate_id,
            status,
            score,
            latest_readiness_status,
            total_shadow_runs::BIGINT AS total_shadow_runs,
            would_submit_count::BIGINT AS would_submit_count,
            risk_rejection_rate_pct,
            walk_forward_status,
            walk_forward_run_id,
            walk_forward_score,
            walk_forward_consistency_score,
            walk_forward_recommendation,
            walk_forward_blockers,
            walk_forward_warnings,
            warnings,
            blockers,
            recommendations,
            thresholds,
            evaluated_at,
            correlation_id
        FROM research_candidate_qualification_evaluations
        WHERE candidate_id = $1
        ORDER BY evaluated_at DESC, id DESC
        LIMIT $2
        "#,
    )
    .bind(candidate_id)
    .bind(bounded_research_candidate_qualification_history_limit(
        limit,
    ))
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(map_research_candidate_qualification_evaluation)
        .collect())
}

pub async fn list_research_candidate_watchlist_rows(
    pool: &PgPool,
    filters: &ResearchCandidateListFilters,
    limit: i64,
) -> Result<Vec<ResearchCandidateWatchlistRow>> {
    let rows = sqlx::query(
        r#"
        WITH filtered_candidates AS (
            SELECT
                id,
                strategy_id,
                symbol,
                timeframe,
                status
            FROM research_candidates
            WHERE ($1::text IS NULL OR strategy_id = $1)
              AND ($2::text IS NULL OR symbol = $2)
              AND ($3::text IS NULL OR timeframe = $3)
              AND ($4::text IS NULL OR status = $4)
            ORDER BY updated_at DESC, id DESC
            LIMIT $5
        ),
        latest_evaluations AS (
            SELECT DISTINCT ON (candidate_id)
                id,
                candidate_id,
                status,
                score,
                latest_readiness_status,
                total_shadow_runs::BIGINT AS total_shadow_runs,
                would_submit_count::BIGINT AS would_submit_count,
                risk_rejection_rate_pct,
                walk_forward_status,
                walk_forward_run_id,
                walk_forward_score,
                walk_forward_consistency_score,
                walk_forward_recommendation,
                walk_forward_blockers,
                walk_forward_warnings,
                warnings,
                blockers,
                recommendations,
                thresholds,
                evaluated_at,
                correlation_id
            FROM research_candidate_qualification_evaluations
            ORDER BY candidate_id, evaluated_at DESC, id DESC
        ),
        latest_walk_forward AS (
            SELECT DISTINCT ON (link.candidate_id)
                link.candidate_id,
                run.id AS walk_forward_run_id,
                run.robustness_status AS walk_forward_robustness_status,
                run.status AS walk_forward_status,
                run.recommendation AS walk_forward_recommendation,
                run.total_windows AS walk_forward_total_windows,
                run.completed_windows AS walk_forward_completed_windows,
                run.profitable_test_windows AS walk_forward_profitable_windows,
                run.losing_test_windows AS walk_forward_losing_windows,
                run.avg_test_pnl_pct AS walk_forward_avg_pnl_pct,
                run.worst_test_pnl_pct AS walk_forward_worst_pnl_pct,
                run.best_test_pnl_pct AS walk_forward_best_pnl_pct,
                run.robustness_score AS walk_forward_robustness_score,
                run.consistency_score AS walk_forward_consistency_score,
                run.created_at AS walk_forward_created_at,
                link.created_at AS walk_forward_linked_at
            FROM research_candidate_walk_forward_runs link
            INNER JOIN strategy_walk_forward_runs run
                ON run.id = link.walk_forward_run_id
            ORDER BY
                link.candidate_id,
                CASE WHEN run.status = 'COMPLETED' THEN 0 ELSE 1 END,
                run.created_at DESC,
                link.created_at DESC
        )
        SELECT
            candidate.id AS candidate_id,
            candidate.strategy_id,
            candidate.symbol,
            candidate.timeframe,
            candidate.status AS candidate_status,
            eval.id AS evaluation_id,
            eval.status AS evaluation_status,
            eval.score AS evaluation_score,
            eval.latest_readiness_status,
            eval.total_shadow_runs::BIGINT AS total_shadow_runs,
            eval.would_submit_count::BIGINT AS would_submit_count,
            eval.risk_rejection_rate_pct,
            eval.warnings,
            eval.blockers,
            eval.recommendations,
            eval.thresholds,
            eval.evaluated_at,
            eval.correlation_id,
            wf.walk_forward_run_id,
            wf.walk_forward_robustness_status,
            wf.walk_forward_status,
            wf.walk_forward_recommendation,
            wf.walk_forward_total_windows,
            wf.walk_forward_completed_windows,
            wf.walk_forward_profitable_windows,
            wf.walk_forward_losing_windows,
            wf.walk_forward_avg_pnl_pct,
            wf.walk_forward_worst_pnl_pct,
            wf.walk_forward_best_pnl_pct,
            wf.walk_forward_robustness_score,
            wf.walk_forward_consistency_score,
            wf.walk_forward_created_at,
            wf.walk_forward_linked_at
        FROM filtered_candidates candidate
        LEFT JOIN latest_evaluations eval
            ON eval.candidate_id = candidate.id
        LEFT JOIN latest_walk_forward wf
            ON wf.candidate_id = candidate.id
        ORDER BY
            eval.evaluated_at DESC NULLS LAST,
            candidate.status ASC,
            candidate.strategy_id ASC,
            candidate.symbol ASC,
            candidate.timeframe ASC
        "#,
    )
    .bind(filters.strategy_id.as_deref())
    .bind(
        filters
            .symbol
            .as_ref()
            .map(|value| value.trim().to_ascii_uppercase()),
    )
    .bind(
        filters
            .timeframe
            .as_ref()
            .map(|value| value.trim().to_ascii_lowercase()),
    )
    .bind(filters.status.as_deref())
    .bind(limit.clamp(1, 500))
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(map_research_candidate_watchlist_row)
        .collect())
}

pub async fn get_research_candidate_shadow_performance(
    pool: &PgPool,
    candidate: &ResearchCandidate,
    window: &ResearchCandidateShadowPerformanceWindow,
    runner_alignment_current: bool,
    computed_at: DateTime<Utc>,
) -> Result<ResearchCandidateShadowPerformance> {
    let row = sqlx::query(
        r#"
        SELECT
            COUNT(*)::BIGINT AS total_shadow_runs,
            COUNT(*) FILTER (WHERE run.decision = 'WOULD_SUBMIT')::BIGINT AS would_submit_count,
            COUNT(*) FILTER (WHERE run.decision = 'NO_SIGNAL')::BIGINT AS no_signal_count,
            COUNT(*) FILTER (WHERE run.decision = 'RISK_REJECTED')::BIGINT AS risk_rejected_count,
            COUNT(*) FILTER (WHERE run.decision LIKE 'SKIPPED_%')::BIGINT AS skipped_count,
            COUNT(*) FILTER (WHERE run.decision = 'ERROR' OR run.status = 'ERROR')::BIGINT AS error_count,
            MAX(run.created_at) AS last_shadow_run_at
        FROM research_candidate_shadow_runs link
        INNER JOIN testnet_shadow_runs run
            ON run.id = link.shadow_run_id
        WHERE link.candidate_id = $1
          AND run.created_at >= $2
          AND run.created_at <= $3
        "#,
    )
    .bind(candidate.id)
    .bind(window.start_time)
    .bind(window.end_time)
    .fetch_one(pool)
    .await?;

    let total_shadow_runs: i64 = row.get("total_shadow_runs");
    let would_submit_count: i64 = row.get("would_submit_count");
    let no_signal_count: i64 = row.get("no_signal_count");
    let risk_rejected_count: i64 = row.get("risk_rejected_count");
    let skipped_count: i64 = row.get("skipped_count");
    let error_count: i64 = row.get("error_count");
    let last_shadow_run_at = row.get("last_shadow_run_at");

    Ok(evaluate_research_candidate_shadow_performance(
        candidate.id,
        candidate.status,
        candidate.strategy_id.clone(),
        candidate.symbol.clone(),
        candidate.timeframe.clone(),
        window.start_time,
        window.end_time,
        total_shadow_runs,
        would_submit_count,
        no_signal_count,
        risk_rejected_count,
        skipped_count,
        error_count,
        last_shadow_run_at,
        runner_alignment_current,
        computed_at,
    ))
}

pub async fn get_research_candidate_shadow_pnl_attribution(
    pool: &PgPool,
    candidate: &ResearchCandidate,
    request: &ResearchShadowPnlAttributionRequest,
    computed_at: DateTime<Utc>,
) -> Result<ResearchShadowPnlAttributionResult> {
    let start_time = request.start_time.unwrap_or(candidate.updated_at);
    let end_time = request.end_time.unwrap_or(computed_at);
    let limit = request.limit.unwrap_or(100).clamp(1, 1_000);
    let run_rows = sqlx::query(
        r#"
        SELECT
            run.id AS shadow_run_id,
            run.strategy_id,
            run.symbol,
            run.timeframe,
            run.created_at AS shadow_created_at,
            signal.created_at AS signal_time
        FROM research_candidate_shadow_runs link
        INNER JOIN testnet_shadow_runs run
            ON run.id = link.shadow_run_id
        LEFT JOIN signals signal
            ON signal.id = run.signal_id
        WHERE link.candidate_id = $1
          AND run.decision = 'WOULD_SUBMIT'
          AND run.created_at >= $2
          AND run.created_at <= $3
        ORDER BY run.created_at DESC, run.id DESC
        LIMIT $4
        "#,
    )
    .bind(candidate.id)
    .bind(start_time)
    .bind(end_time)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let runs = run_rows
        .iter()
        .map(|row| ResearchShadowPnlRunInput {
            shadow_run_id: row.get("shadow_run_id"),
            strategy_id: row.get("strategy_id"),
            symbol: row.get("symbol"),
            timeframe: row.get("timeframe"),
            shadow_created_at: row.get("shadow_created_at"),
            signal_time: row.get("signal_time"),
        })
        .collect::<Vec<_>>();

    let candles = if let Some(first_attribution_time) = runs
        .iter()
        .map(|run| run.signal_time.unwrap_or(run.shadow_created_at))
        .min()
    {
        let symbol = Symbol::new(candidate.symbol.clone())?;
        let interval = candidate.timeframe.parse::<CandleInterval>()?;
        let candle_rows = sqlx::query(
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
              AND open_time > $3
            ORDER BY open_time ASC
            "#,
        )
        .bind(symbol.as_str())
        .bind(interval.as_str())
        .bind(first_attribution_time)
        .fetch_all(pool)
        .await?;

        candle_rows
            .iter()
            .map(|row| -> Result<Candle> {
                Ok(Candle {
                    id: row.get("id"),
                    exchange: row
                        .get::<String, _>("exchange")
                        .parse::<MarketDataSource>()?,
                    symbol: Symbol::new(row.get::<String, _>("symbol"))?,
                    interval: row.get::<String, _>("interval").parse::<CandleInterval>()?,
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
                })
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        Vec::new()
    };

    Ok(calculate_research_shadow_pnl_attribution(
        candidate,
        request,
        &runs,
        &candles,
        computed_at,
    ))
}

pub async fn insert_strategy_research_candidate_promotion(
    pool: &PgPool,
    promotion_id: Uuid,
    candidate_id: Uuid,
    previous_config: Option<Value>,
    promoted_config: Value,
    status: &str,
    actor_id: Option<Uuid>,
    correlation_id: Option<Uuid>,
    promoted_at: DateTime<Utc>,
) -> Result<StrategyResearchCandidatePromotionRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO strategy_research_candidate_promotions (
            id,
            candidate_id,
            previous_config,
            promoted_config,
            status,
            actor_id,
            created_at,
            correlation_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING
            id,
            candidate_id,
            previous_config,
            promoted_config,
            status,
            actor_id,
            created_at,
            correlation_id
        "#,
    )
    .bind(promotion_id)
    .bind(candidate_id)
    .bind(previous_config)
    .bind(promoted_config)
    .bind(status)
    .bind(actor_id)
    .bind(promoted_at)
    .bind(correlation_id)
    .fetch_one(pool)
    .await?;

    Ok(map_strategy_research_candidate_promotion(row))
}

pub async fn mark_strategy_research_candidate_promoted(
    pool: &PgPool,
    candidate_id: Uuid,
    promoted_by: Option<Uuid>,
    promoted_at: DateTime<Utc>,
    correlation_id: Option<Uuid>,
) -> Result<StrategyResearchCandidateRecord> {
    let row = sqlx::query(
        r#"
        UPDATE strategy_research_candidates
        SET status = $2,
            promoted_at = $3,
            promoted_by = $4,
            correlation_id = COALESCE($5, correlation_id),
            updated_at = $3
        WHERE id = $1
        RETURNING
            id,
            strategy_id,
            symbol,
            timeframe,
            config,
            source_type,
            source_id,
            evidence,
            score,
            status,
            warnings,
            created_by,
            created_at,
            updated_at,
            promoted_at,
            promoted_by,
            correlation_id
        "#,
    )
    .bind(candidate_id)
    .bind(StrategyResearchCandidateStatus::PromotedToShadowConfig.as_str())
    .bind(promoted_at)
    .bind(promoted_by)
    .bind(correlation_id)
    .fetch_one(pool)
    .await?;

    Ok(map_strategy_research_candidate(row))
}

pub fn strategy_research_candidate_from_record(
    record: &StrategyResearchCandidateRecord,
) -> Result<StrategyResearchCandidate> {
    let evidence =
        serde_json::from_value::<StrategyResearchCandidateEvidence>(record.evidence.clone())?;
    let warnings = serde_json::from_value::<Vec<String>>(record.warnings.clone())?;

    Ok(StrategyResearchCandidate {
        id: record.id,
        strategy_id: record.strategy_id.clone(),
        symbol: record.symbol.clone(),
        timeframe: record.timeframe.clone(),
        config: record.config.clone(),
        source_type: record
            .source_type
            .parse::<StrategyResearchCandidateSource>()?,
        source_id: record.source_id,
        evidence,
        score: StrategyResearchCandidateScore {
            score: record.score,
            warnings,
            rejection_hints: Vec::new(),
        },
        status: record.status.parse::<StrategyResearchCandidateStatus>()?,
        created_at: record.created_at,
        promoted_at: record.promoted_at,
        promoted_by: record.promoted_by,
        correlation_id: record.correlation_id,
    })
}

pub fn research_candidate_from_record(
    record: &ResearchCandidateRecord,
) -> Result<ResearchCandidate> {
    Ok(ResearchCandidate {
        id: record.id,
        experiment_id: record.experiment_id,
        experiment_run_id: record.experiment_run_id,
        strategy_id: record.strategy_id.clone(),
        symbol: record.symbol.clone(),
        timeframe: record.timeframe.clone(),
        config: record.config.clone(),
        score: record.score,
        pnl_pct: record.pnl_pct,
        max_drawdown_pct: record.max_drawdown_pct,
        trade_count: record.trade_count,
        win_rate: record.win_rate,
        fee_drag: record.fee_drag,
        status: record.status.parse::<ResearchCandidateStatus>()?,
        rejection_reason: record.rejection_reason.clone(),
        notes: record.notes.clone(),
        created_at: record.created_at,
        updated_at: record.updated_at,
        correlation_id: record.correlation_id,
    })
}

pub fn research_candidate_event_from_record(
    record: &ResearchCandidateEventRecord,
) -> Result<ResearchCandidateLifecycleEvent> {
    Ok(ResearchCandidateLifecycleEvent {
        id: record.id,
        candidate_id: record.candidate_id,
        previous_status: record
            .previous_status
            .as_deref()
            .map(str::parse::<ResearchCandidateStatus>)
            .transpose()?,
        next_status: record.next_status.parse::<ResearchCandidateStatus>()?,
        decision: record.decision.parse::<ResearchCandidateDecision>()?,
        reason: record.reason.clone(),
        notes: record.notes.clone(),
        actor_id: record.actor_id,
        payload: record.payload.clone(),
        created_at: record.created_at,
        correlation_id: record.correlation_id,
    })
}

pub fn strategy_research_candidate_promotion_result_from_records(
    candidate: &StrategyResearchCandidateRecord,
    promotion: &StrategyResearchCandidatePromotionRecord,
) -> Result<StrategyResearchCandidatePromotionResult> {
    Ok(StrategyResearchCandidatePromotionResult {
        candidate_id: candidate.id,
        strategy_id: candidate.strategy_id.clone(),
        previous_config: promotion.previous_config.clone(),
        promoted_config: promotion.promoted_config.clone(),
        status: candidate
            .status
            .parse::<StrategyResearchCandidateStatus>()?,
        promoted_at: promotion.created_at,
        promoted_by: promotion.actor_id,
        correlation_id: promotion.correlation_id,
    })
}

pub fn strategy_candidate_observation_result_from_record(
    record: &StrategyCandidateObservationRecord,
) -> Result<StrategyCandidateObservationResult> {
    let requirements = serde_json::from_value::<StrategyCandidateObservationRequirement>(
        record.requirements.clone(),
    )?;
    let summary =
        serde_json::from_value::<StrategyCandidateObservationSummary>(record.summary.clone())?;

    Ok(StrategyCandidateObservationResult {
        observation_id: record.id,
        candidate_id: record.candidate_id,
        strategy_id: record.strategy_id.clone(),
        symbol: record.symbol.clone(),
        timeframe: record.timeframe.clone(),
        status: record
            .status
            .parse::<StrategyCandidateObservationStatus>()?,
        requirements,
        runner_alignment: summary.runner_alignment.clone(),
        summary,
        decision: record
            .decision
            .parse::<StrategyCandidateObservationDecision>()?,
        started_at: record.started_at,
        evaluated_at: record.evaluated_at,
        last_observed_at: record.last_observed_at,
        observation_expires_at: record.observation_expires_at,
        observation_max_age_seconds: record.observation_max_age_seconds,
        observation_snapshot_hash: record.observation_snapshot_hash.clone(),
        runner_config_snapshot: record.runner_config_snapshot.clone(),
        readiness_snapshot: record.readiness_snapshot.clone(),
        created_by: record.created_by,
        correlation_id: record.correlation_id,
    })
}

pub async fn list_closed_candle_open_times_in_range(
    pool: &PgPool,
    exchange: MarketDataSource,
    symbol: &Symbol,
    interval: CandleInterval,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> Result<Vec<DateTime<Utc>>> {
    let rows = sqlx::query(
        r#"
        SELECT open_time
        FROM candles
        WHERE exchange = $1
          AND symbol = $2
          AND interval = $3
          AND is_closed = TRUE
          AND open_time >= $4
          AND open_time < $5
        ORDER BY open_time ASC
        "#,
    )
    .bind(exchange.as_str())
    .bind(symbol.as_str())
    .bind(interval.as_str())
    .bind(start_time)
    .bind(end_time)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| row.get::<DateTime<Utc>, _>("open_time"))
        .collect())
}

pub async fn insert_research_dataset_build(
    pool: &PgPool,
    build_id: Uuid,
    request: &ResearchDatasetBuildRequest,
    coverage_before: &ResearchDataCoverageResult,
    correlation_id: Uuid,
    created_at: DateTime<Utc>,
) -> Result<ResearchDatasetBuildRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO research_dataset_builds (
            id,
            exchange,
            symbol,
            start_time,
            end_time,
            requested_intervals,
            status,
            coverage_before,
            coverage_after,
            failed_reason,
            correlation_id,
            created_at,
            completed_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, '{}'::JSONB, NULL, $9, $10, NULL)
        RETURNING
            id,
            exchange,
            symbol,
            start_time,
            end_time,
            requested_intervals,
            status,
            coverage_before,
            coverage_after,
            failed_reason,
            correlation_id,
            created_at,
            completed_at
        "#,
    )
    .bind(build_id)
    .bind(request.exchange.as_str())
    .bind(request.symbol.trim().to_ascii_uppercase())
    .bind(request.start_time)
    .bind(request.end_time)
    .bind(serde_json::to_value(&request.intervals)?)
    .bind(ResearchDatasetBuildStatus::Started.as_str())
    .bind(serde_json::to_value(coverage_before)?)
    .bind(correlation_id)
    .bind(created_at)
    .fetch_one(pool)
    .await?;

    Ok(map_research_dataset_build(row))
}

pub async fn complete_research_dataset_build(
    pool: &PgPool,
    build_id: Uuid,
    status: ResearchDatasetBuildStatus,
    coverage_after: &ResearchDataCoverageResult,
    failed_reason: Option<&str>,
    completed_at: DateTime<Utc>,
) -> Result<ResearchDatasetBuildRecord> {
    let row = sqlx::query(
        r#"
        UPDATE research_dataset_builds
        SET status = $2,
            coverage_after = $3,
            failed_reason = $4,
            completed_at = $5
        WHERE id = $1
        RETURNING
            id,
            exchange,
            symbol,
            start_time,
            end_time,
            requested_intervals,
            status,
            coverage_before,
            coverage_after,
            failed_reason,
            correlation_id,
            created_at,
            completed_at
        "#,
    )
    .bind(build_id)
    .bind(status.as_str())
    .bind(serde_json::to_value(coverage_after)?)
    .bind(failed_reason)
    .bind(completed_at)
    .fetch_one(pool)
    .await?;

    Ok(map_research_dataset_build(row))
}

pub async fn replace_research_dataset_build_steps(
    pool: &PgPool,
    build_id: Uuid,
    steps: &[ResearchDatasetBuildStep],
) -> Result<Vec<ResearchDatasetBuildStepRecord>> {
    sqlx::query(
        r#"
        DELETE FROM research_dataset_build_steps
        WHERE build_id = $1
        "#,
    )
    .bind(build_id)
    .execute(pool)
    .await?;

    let mut records = Vec::with_capacity(steps.len());
    for (index, step) in steps.iter().enumerate() {
        let row = sqlx::query(
            r#"
            INSERT INTO research_dataset_build_steps (
                id,
                build_id,
                step_index,
                step_name,
                status,
                details,
                started_at,
                completed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING
                id,
                build_id,
                step_index,
                step_name,
                status,
                details,
                started_at,
                completed_at
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(build_id)
        .bind(i32::try_from(index).unwrap_or(i32::MAX))
        .bind(&step.step)
        .bind(step.status.as_str())
        .bind(step.details.clone())
        .bind(step.started_at)
        .bind(step.completed_at)
        .fetch_one(pool)
        .await?;
        records.push(map_research_dataset_build_step(row));
    }

    Ok(records)
}

pub async fn list_research_dataset_builds(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<ResearchDatasetBuildRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            exchange,
            symbol,
            start_time,
            end_time,
            requested_intervals,
            status,
            coverage_before,
            coverage_after,
            failed_reason,
            correlation_id,
            created_at,
            completed_at
        FROM research_dataset_builds
        ORDER BY created_at DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(map_research_dataset_build).collect())
}

pub async fn get_research_dataset_build(
    pool: &PgPool,
    build_id: Uuid,
) -> Result<Option<ResearchDatasetBuildRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            exchange,
            symbol,
            start_time,
            end_time,
            requested_intervals,
            status,
            coverage_before,
            coverage_after,
            failed_reason,
            correlation_id,
            created_at,
            completed_at
        FROM research_dataset_builds
        WHERE id = $1
        "#,
    )
    .bind(build_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(map_research_dataset_build))
}

pub async fn list_research_dataset_build_steps(
    pool: &PgPool,
    build_id: Uuid,
) -> Result<Vec<ResearchDatasetBuildStepRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            build_id,
            step_index,
            step_name,
            status,
            details,
            started_at,
            completed_at
        FROM research_dataset_build_steps
        WHERE build_id = $1
        ORDER BY step_index ASC
        "#,
    )
    .bind(build_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(map_research_dataset_build_step)
        .collect())
}

pub fn research_dataset_build_result_from_records(
    record: &ResearchDatasetBuildRecord,
    step_records: &[ResearchDatasetBuildStepRecord],
) -> Result<ResearchDatasetBuildResult> {
    let requested_intervals = serde_json::from_value(record.requested_intervals.clone())
        .context("invalid research_dataset_builds.requested_intervals")?;
    let coverage_before = serde_json::from_value(record.coverage_before.clone())
        .context("invalid research_dataset_builds.coverage_before")?;
    let coverage_after = serde_json::from_value(record.coverage_after.clone())
        .context("invalid research_dataset_builds.coverage_after")?;
    let status = match record.status.as_str() {
        "STARTED" => ResearchDatasetBuildStatus::Started,
        "COMPLETED" => ResearchDatasetBuildStatus::Completed,
        "FAILED" => ResearchDatasetBuildStatus::Failed,
        other => anyhow::bail!("unsupported research dataset build status: {other}"),
    };

    let steps = step_records
        .iter()
        .map(research_dataset_build_step_from_record)
        .collect::<Result<Vec<_>>>()?;

    Ok(ResearchDatasetBuildResult {
        build_id: record.id,
        exchange: record.exchange.parse()?,
        symbol: record.symbol.clone(),
        requested_intervals,
        start_time: record.start_time,
        end_time: record.end_time,
        status,
        coverage_before,
        coverage_after,
        steps,
        failed_reason: record.failed_reason.clone(),
        correlation_id: record.correlation_id,
        created_at: record.created_at,
        completed_at: record.completed_at,
    })
}

fn research_dataset_build_step_from_record(
    record: &ResearchDatasetBuildStepRecord,
) -> Result<ResearchDatasetBuildStep> {
    let status = match record.status.as_str() {
        "STARTED" => ResearchDatasetBuildStepStatus::Started,
        "COMPLETED" => ResearchDatasetBuildStepStatus::Completed,
        "FAILED" => ResearchDatasetBuildStepStatus::Failed,
        other => anyhow::bail!("unsupported research dataset build step status: {other}"),
    };

    Ok(ResearchDatasetBuildStep {
        step: record.step_name.clone(),
        status,
        details: record.details.clone(),
        started_at: record.started_at,
        completed_at: record.completed_at,
    })
}

fn map_research_dataset_build(row: sqlx::postgres::PgRow) -> ResearchDatasetBuildRecord {
    ResearchDatasetBuildRecord {
        id: row.get("id"),
        exchange: row.get("exchange"),
        symbol: row.get("symbol"),
        start_time: row.get("start_time"),
        end_time: row.get("end_time"),
        requested_intervals: row.get("requested_intervals"),
        status: row.get("status"),
        coverage_before: row.get("coverage_before"),
        coverage_after: row.get("coverage_after"),
        failed_reason: row.get("failed_reason"),
        correlation_id: row.get("correlation_id"),
        created_at: row.get("created_at"),
        completed_at: row.get("completed_at"),
    }
}

fn map_research_dataset_build_step(row: sqlx::postgres::PgRow) -> ResearchDatasetBuildStepRecord {
    ResearchDatasetBuildStepRecord {
        id: row.get("id"),
        build_id: row.get("build_id"),
        step_index: row.get("step_index"),
        step_name: row.get("step_name"),
        status: row.get("status"),
        details: row.get("details"),
        started_at: row.get("started_at"),
        completed_at: row.get("completed_at"),
    }
}

fn map_strategy_research_candidate(row: sqlx::postgres::PgRow) -> StrategyResearchCandidateRecord {
    StrategyResearchCandidateRecord {
        id: row.get("id"),
        strategy_id: row.get("strategy_id"),
        symbol: row.get("symbol"),
        timeframe: row.get("timeframe"),
        config: row.get("config"),
        source_type: row.get("source_type"),
        source_id: row.get("source_id"),
        evidence: row.get("evidence"),
        score: row.get("score"),
        status: row.get("status"),
        warnings: row.get("warnings"),
        created_by: row.get("created_by"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        promoted_at: row.get("promoted_at"),
        promoted_by: row.get("promoted_by"),
        correlation_id: row.get("correlation_id"),
    }
}

fn map_research_candidate(row: sqlx::postgres::PgRow) -> ResearchCandidateRecord {
    ResearchCandidateRecord {
        id: row.get("id"),
        experiment_id: row.get("experiment_id"),
        experiment_run_id: row.get("experiment_run_id"),
        strategy_id: row.get("strategy_id"),
        symbol: row.get("symbol"),
        timeframe: row.get("timeframe"),
        config: row.get("config"),
        score: row.get("score"),
        pnl_pct: row.get("pnl_pct"),
        max_drawdown_pct: row.get("max_drawdown_pct"),
        trade_count: row.get("trade_count"),
        win_rate: row.get("win_rate"),
        fee_drag: row.get("fee_drag"),
        status: row.get("status"),
        rejection_reason: row.get("rejection_reason"),
        notes: row.get("notes"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        correlation_id: row.get("correlation_id"),
    }
}

fn map_research_candidate_event(row: sqlx::postgres::PgRow) -> ResearchCandidateEventRecord {
    ResearchCandidateEventRecord {
        id: row.get("id"),
        candidate_id: row.get("candidate_id"),
        previous_status: row.get("previous_status"),
        next_status: row.get("next_status"),
        decision: row.get("decision"),
        reason: row.get("reason"),
        notes: row.get("notes"),
        actor_id: row.get("actor_id"),
        payload: row.get("payload"),
        created_at: row.get("created_at"),
        correlation_id: row.get("correlation_id"),
    }
}

fn map_strategy_research_candidate_promotion(
    row: sqlx::postgres::PgRow,
) -> StrategyResearchCandidatePromotionRecord {
    StrategyResearchCandidatePromotionRecord {
        id: row.get("id"),
        candidate_id: row.get("candidate_id"),
        previous_config: row.get("previous_config"),
        promoted_config: row.get("promoted_config"),
        status: row.get("status"),
        actor_id: row.get("actor_id"),
        created_at: row.get("created_at"),
        correlation_id: row.get("correlation_id"),
    }
}

fn map_strategy_candidate_observation(
    row: sqlx::postgres::PgRow,
) -> StrategyCandidateObservationRecord {
    StrategyCandidateObservationRecord {
        id: row.get("id"),
        candidate_id: row.get("candidate_id"),
        strategy_id: row.get("strategy_id"),
        symbol: row.get("symbol"),
        timeframe: row.get("timeframe"),
        status: row.get("status"),
        requirements: row.get("requirements"),
        summary: row.get("summary"),
        decision: row.get("decision"),
        started_at: row.get("started_at"),
        evaluated_at: row.get("evaluated_at"),
        last_observed_at: row.get("last_observed_at"),
        observation_expires_at: row.get("observation_expires_at"),
        observation_max_age_seconds: row.get("observation_max_age_seconds"),
        observation_snapshot_hash: row.get("observation_snapshot_hash"),
        runner_config_snapshot: row.get("runner_config_snapshot"),
        readiness_snapshot: row.get("readiness_snapshot"),
        created_by: row.get("created_by"),
        correlation_id: row.get("correlation_id"),
    }
}

fn map_strategy_candidate_observation_check(
    row: sqlx::postgres::PgRow,
) -> StrategyCandidateObservationCheckRecord {
    StrategyCandidateObservationCheckRecord {
        id: row.get("id"),
        observation_id: row.get("observation_id"),
        finding_index: row.get("finding_index"),
        code: row.get("code"),
        message: row.get("message"),
        blocking: row.get("blocking"),
        created_at: row.get("created_at"),
    }
}
