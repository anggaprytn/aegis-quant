use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use aegis_core::{
    CandleInterval, MarketDataSource, ResearchDataCoverageResult, ResearchDatasetBuildRequest,
    ResearchDatasetBuildResult, ResearchDatasetBuildStatus, ResearchDatasetBuildStep,
    ResearchDatasetBuildStepStatus, Symbol,
};

use crate::PgPool;

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
