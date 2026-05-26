use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use aegis_core::{
    CandleInterval, MarketDataSource, ResearchDataCoverageResult, ResearchDatasetBuildRequest,
    ResearchDatasetBuildResult, ResearchDatasetBuildStatus, ResearchDatasetBuildStep,
    ResearchDatasetBuildStepStatus, StrategyCandidateObservationDecision,
    StrategyCandidateObservationRequirement, StrategyCandidateObservationResult,
    StrategyCandidateObservationStatus, StrategyCandidateObservationSummary,
    StrategyResearchCandidate, StrategyResearchCandidateEvidence,
    StrategyResearchCandidatePromotionResult, StrategyResearchCandidateScore,
    StrategyResearchCandidateSource, StrategyResearchCandidateStatus, Symbol,
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

#[derive(Debug, Clone, Default)]
pub struct StrategyResearchCandidateListFilters {
    pub strategy_id: Option<String>,
    pub symbol: Option<String>,
    pub timeframe: Option<String>,
    pub status: Option<String>,
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
            created_by,
            correlation_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
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
