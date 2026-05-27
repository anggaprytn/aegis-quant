use std::cmp::Reverse;

use aegis_core::{
    AuthenticatedActor, MarketDataQualityRequest, MarketDataQualityStatus, OperatorReport,
    OperatorReportFinding, OperatorReportFormat, OperatorReportHighlight,
    OperatorReportMarketFeedSnapshot, OperatorReportMarketSnapshot, OperatorReportPaperSnapshot,
    OperatorReportPromotionSnapshot, OperatorReportReasonCount, OperatorReportRecommendation,
    OperatorReportRequest, OperatorReportResearchQualificationSnapshot,
    OperatorReportResearchQualificationTopCandidate, OperatorReportRiskSnapshot,
    OperatorReportSection, OperatorReportSeverity, OperatorReportShadowSnapshot,
    OperatorReportStatus, OperatorReportStrategySnapshot, OperatorReportSummary,
    OperatorReportSystemSnapshot, OperatorReportTestnetSnapshot, OperatorReportTopPairCount,
    ResearchCandidateQualificationStatus, ResearchCandidateQualificationThresholds,
    ResearchCandidateStatus, ResearchCandidateTestnetReviewStatus,
    ResearchShadowPnlAttributionRequest, ResearchShadowPnlAttributionResult,
    ResearchShadowPnlRecommendation, StrategyPerformanceMode, StrategyPerformanceRequest,
    TestnetPromotionFunnelRequest,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{query, query_scalar, PgPool, Row};
use telemetry::telemetry;
use uuid::Uuid;

use crate::AppState;
use db::{
    get_research_candidate, get_research_candidate_shadow_pnl_attribution,
    list_research_candidate_qualification_evaluations, list_research_candidate_reviews,
    list_research_candidates, research_candidate_from_record,
    research_candidate_qualification_evaluation_from_record, research_candidate_review_from_record,
    summarize_candle_continuity_report, ResearchCandidateListFilters,
};

const DEFAULT_REPORT_WINDOW_HOURS: i64 = 24;
const REPORT_LIST_DEFAULT_LIMIT: i64 = 20;
const REPORT_LIST_MAX_LIMIT: i64 = 100;
const REPORT_RESEARCH_CANDIDATE_LIMIT: i64 = 250;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorReportListItem {
    pub report_id: Uuid,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub format: OperatorReportFormat,
    pub status: OperatorReportStatus,
    pub created_at: DateTime<Utc>,
    pub created_by: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
struct ReportWindow {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct StrategyBehaviorData {
    total_strategy_evaluations: i64,
    total_signals: i64,
    risk_rejection_rate_pct: Decimal,
    strategy_analytics_summary: Option<aegis_core::StrategyPerformanceSummary>,
    top_rejected_pairs: Vec<OperatorReportTopPairCount>,
    enabled_strategy_count: i64,
}

#[derive(Debug, Clone)]
struct BacktestActivity {
    run_count: i64,
}

#[derive(Debug, Clone)]
struct SystemAndMarketData {
    system: OperatorReportSystemSnapshot,
    market: OperatorReportMarketSnapshot,
    stale_threshold_seconds: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct CandidateShadowPnlReportSnapshot {
    candidate_id: Uuid,
    strategy_id: String,
    symbol: String,
    timeframe: String,
    total_attributed_runs: i64,
    insufficient_forward_data_count: i64,
    best_holding_window: Option<u32>,
    best_avg_net_pnl_pct: Option<Decimal>,
    recommendation: ResearchShadowPnlRecommendation,
    negative_all_windows: bool,
}

impl From<&ResearchShadowPnlAttributionResult> for CandidateShadowPnlReportSnapshot {
    fn from(value: &ResearchShadowPnlAttributionResult) -> Self {
        Self {
            candidate_id: value.candidate_id,
            strategy_id: value.strategy_id.clone(),
            symbol: value.symbol.clone(),
            timeframe: value.timeframe.clone(),
            total_attributed_runs: value.summary.total_attributed_runs,
            insufficient_forward_data_count: value.summary.insufficient_forward_data_count,
            best_holding_window: value.best_holding_window,
            best_avg_net_pnl_pct: value.best_avg_net_pnl_pct,
            recommendation: value.latest_shadow_pnl_status,
            negative_all_windows: value.summary.negative_all_windows,
        }
    }
}

#[derive(Debug, Clone)]
struct OperatorReportRecord {
    payload: serde_json::Value,
    markdown: Option<String>,
}

pub fn persist_allowed(actor: Option<&AuthenticatedActor>) -> bool {
    matches!(
        actor.map(|value| value.role),
        Some(aegis_core::UserRole::Owner | aegis_core::UserRole::Operator)
    )
}

pub fn bounded_report_list_limit(limit: Option<i64>) -> i64 {
    match limit {
        Some(value) if value > 0 => value.min(REPORT_LIST_MAX_LIMIT),
        _ => REPORT_LIST_DEFAULT_LIMIT,
    }
}

pub async fn generate_operator_report(
    state: &AppState,
    request: &OperatorReportRequest,
    actor: Option<&AuthenticatedActor>,
) -> Result<OperatorReport> {
    request.validate()?;
    let window = resolve_window(request);
    let correlation_id = request.correlation_id.unwrap_or_else(Uuid::new_v4);

    let system_market = load_system_and_market_data(state, request, &window).await?;
    let strategy = load_strategy_behavior(&state.db_pool, request, &window).await?;
    let risk = load_risk_snapshot(&state.db_pool, request, &window).await?;
    let paper = load_paper_snapshot(&state.db_pool, request, &window).await?;
    let shadow = load_shadow_snapshot(&state.db_pool, request, &window).await?;
    let promotion = load_promotion_snapshot(&state.db_pool, request, &window).await?;
    let testnet = load_testnet_snapshot(&state.db_pool, request, &window).await?;
    let research_qualification =
        load_research_candidate_qualification_snapshot(state, request, &window).await?;
    let shadow_pnl =
        load_candidate_shadow_pnl_snapshot(state, &research_qualification, request, &window)
            .await?;
    let backtest = load_backtest_activity(&state.db_pool, request, &window).await?;

    let mut findings = build_findings(
        &system_market,
        &strategy,
        &risk,
        &paper,
        &shadow,
        &promotion,
        &testnet,
        &research_qualification,
        shadow_pnl.as_ref(),
        &backtest,
    );
    findings.sort_by_key(|finding| Reverse(finding.severity.sort_weight()));

    let recommendations = build_recommendations(&findings);
    let status = status_from_findings(&findings);
    let summary = build_summary(
        &findings,
        system_market.system.kill_switch_active,
        system_market.market.stale_feed_count,
        strategy.risk_rejection_rate_pct,
        paper.daily_pnl,
        shadow.would_submit_count,
        promotion.reconciliation_required_count,
    );

    let mut report = OperatorReport {
        report_id: Uuid::new_v4(),
        window_start: window.start,
        window_end: window.end,
        generated_at: window.generated_at,
        status,
        summary,
        findings,
        recommendations,
        sections: build_sections(
            &system_market.system,
            &system_market.market,
            &strategy,
            &risk,
            &paper,
            &shadow,
            &promotion,
            &testnet,
            &research_qualification,
            shadow_pnl.as_ref(),
        )?,
        format: request.format,
        persisted: false,
        correlation_id,
        markdown: None,
    }
    .with_markdown();

    if request.persist {
        persist_report(&state.db_pool, &report, actor.map(|value| value.user_id)).await?;
        report.persisted = true;
    }

    telemetry().inc_operator_report_generated(request.format.as_str(), report.status.as_str());
    for finding in &report.findings {
        telemetry().inc_operator_report_finding(finding.severity.as_str());
    }

    Ok(report)
}

pub async fn list_operator_reports(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<OperatorReportListItem>> {
    let rows = query(
        r#"
        SELECT
            id,
            window_start,
            window_end,
            format,
            status,
            created_by,
            created_at,
            correlation_id
        FROM operator_reports
        ORDER BY created_at DESC, id DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(map_operator_report_list_item)
        .collect()
}

pub async fn get_operator_report(pool: &PgPool, report_id: Uuid) -> Result<Option<OperatorReport>> {
    let row = query(
        r#"
        SELECT
            id,
            window_start,
            window_end,
            format,
            status,
            payload,
            markdown,
            created_by,
            created_at,
            correlation_id
        FROM operator_reports
        WHERE id = $1
        "#,
    )
    .bind(report_id)
    .fetch_optional(pool)
    .await?;

    row.map(map_operator_report_record)
        .transpose()?
        .map(report_from_record)
        .transpose()
}

fn resolve_window(request: &OperatorReportRequest) -> ReportWindow {
    let generated_at = Utc::now();
    let end = request.end_time.unwrap_or(generated_at);
    let start = request
        .start_time
        .unwrap_or_else(|| end - Duration::hours(DEFAULT_REPORT_WINDOW_HOURS));
    ReportWindow {
        start,
        end,
        generated_at,
    }
}

async fn load_system_and_market_data(
    state: &AppState,
    request: &OperatorReportRequest,
    window: &ReportWindow,
) -> Result<SystemAndMarketData> {
    let db_healthy = db::check_health(&state.db_pool).await.is_ok();
    let system_state = db::get_system_state(&state.db_pool)
        .await
        .context("failed to load system state")?;
    let risk_config = db::get_risk_config(&state.db_pool)
        .await
        .context("failed to load risk config")?;
    let stale_threshold_seconds = risk_config
        .as_ref()
        .map(|value| value.stale_feed_threshold_seconds)
        .unwrap_or(10);
    let feeds = db::list_market_feed_statuses(&state.db_pool)
        .await
        .context("failed to load market feed statuses")?;

    let feed_snapshots: Vec<_> = feeds
        .into_iter()
        .filter(|feed| {
            request
                .symbol
                .as_deref()
                .map(|symbol| feed.symbol.eq_ignore_ascii_case(symbol))
                .unwrap_or(true)
        })
        .map(|feed| OperatorReportMarketFeedSnapshot {
            symbol: feed.symbol,
            status: feed.status,
            freshness_status: match feed.freshness_status {
                aegis_core::DataFreshnessStatus::Fresh => "fresh".to_string(),
                aegis_core::DataFreshnessStatus::Stale => "stale".to_string(),
                aegis_core::DataFreshnessStatus::Unknown => "unknown".to_string(),
            },
            last_event_age_seconds: feed.last_event_at.map(|value| {
                window
                    .generated_at
                    .signed_duration_since(value)
                    .num_seconds()
                    .max(0)
            }),
        })
        .collect();

    let stale_feed_count = feed_snapshots
        .iter()
        .filter(|feed| {
            feed.freshness_status.eq_ignore_ascii_case("stale")
                || feed
                    .last_event_age_seconds
                    .map(|age| age > i64::from(stale_threshold_seconds))
                    .unwrap_or(false)
        })
        .count() as i64;
    let degraded_feed_count = feed_snapshots
        .iter()
        .filter(|feed| !feed.status.eq_ignore_ascii_case("connected"))
        .count() as i64;

    let backfill_row = query(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE status = 'COMPLETED') AS completed_count,
            COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_count
        FROM candle_backfill_runs
        WHERE created_at >= $1
          AND created_at <= $2
          AND ($3::text IS NULL OR symbol = $3)
        "#,
    )
    .bind(window.start)
    .bind(window.end)
    .bind(request.symbol.as_deref())
    .fetch_one(&state.db_pool)
    .await?;

    let repair_row = query(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE status = 'REPAIR_COMPLETED') AS completed_count,
            COUNT(*) FILTER (WHERE status = 'REPAIR_FAILED') AS failed_count,
            COUNT(*) FILTER (WHERE status = 'PARTIAL_REPAIR') AS partial_count,
            COUNT(*) FILTER (
                WHERE after_quality_status IN ('BAD', 'DEGRADED', 'INSUFFICIENT_DATA')
            ) AS degraded_after_count
        FROM market_data_repair_runs
        WHERE created_at >= $1
          AND created_at <= $2
          AND ($3::text IS NULL OR symbol = $3)
          AND ($4::text IS NULL OR interval = $4)
        "#,
    )
    .bind(window.start)
    .bind(window.end)
    .bind(request.symbol.as_deref())
    .bind(request.interval.as_deref())
    .fetch_one(&state.db_pool)
    .await?;

    let candle_count_in_window: i64 = query_scalar(
        r#"
        SELECT COUNT(*)
        FROM candles
        WHERE close_time >= $1
          AND close_time <= $2
          AND ($3::text IS NULL OR symbol = $3)
        "#,
    )
    .bind(window.start)
    .bind(window.end)
    .bind(request.symbol.as_deref())
    .fetch_one(&state.db_pool)
    .await?;

    let data_quality = match (request.symbol.as_deref(), request.interval.as_deref()) {
        (Some(symbol), Some(interval)) => {
            let quality_request = MarketDataQualityRequest {
                exchange: state.market_config.exchange,
                symbol: symbol.to_string(),
                interval: interval.to_string(),
                start_time: window.start,
                end_time: window.end,
                expected_interval_seconds: None,
                max_allowed_gap_count: None,
                max_allowed_gap_pct: None,
            };
            summarize_candle_continuity_report(&state.db_pool, &quality_request)
                .await
                .ok()
        }
        _ => None,
    };

    Ok(SystemAndMarketData {
        system: OperatorReportSystemSnapshot {
            api_healthy: true,
            db_healthy,
            kill_switch_active: system_state.kill_switch_enabled,
            auth_enabled: !state.auth_config.disabled,
            metrics_available: telemetry().encode().is_ok(),
            uptime_seconds: window
                .generated_at
                .signed_duration_since(state.started_at)
                .num_seconds()
                .max(0),
        },
        market: OperatorReportMarketSnapshot {
            feeds: feed_snapshots,
            stale_feed_count,
            degraded_feed_count,
            backfill_completed_count: backfill_row.get::<i64, _>("completed_count"),
            backfill_failed_count: backfill_row.get::<i64, _>("failed_count"),
            candle_count_in_window,
            data_quality,
            repair_completed_count: repair_row.get::<i64, _>("completed_count"),
            repair_failed_count: repair_row.get::<i64, _>("failed_count"),
            repair_partial_count: repair_row.get::<i64, _>("partial_count"),
            repair_degraded_after_count: repair_row.get::<i64, _>("degraded_after_count"),
        },
        stale_threshold_seconds,
    })
}

async fn load_strategy_behavior(
    pool: &PgPool,
    request: &OperatorReportRequest,
    window: &ReportWindow,
) -> Result<StrategyBehaviorData> {
    let evaluations_row = query(
        r#"
        SELECT
            COUNT(*) AS total_strategy_evaluations,
            COUNT(*) FILTER (WHERE decision = 'WOULD_SUBMIT') AS would_submit_count,
            COUNT(*) FILTER (WHERE decision = 'NO_SIGNAL') AS no_signal_count
        FROM testnet_shadow_runs
        WHERE created_at >= $1
          AND created_at <= $2
          AND ($3::text IS NULL OR symbol = $3)
          AND ($4::text IS NULL OR strategy_id = $4)
        "#,
    )
    .bind(window.start)
    .bind(window.end)
    .bind(request.symbol.as_deref())
    .bind(request.strategy_id.as_deref())
    .fetch_one(pool)
    .await?;

    let total_signals: i64 = query_scalar(
        r#"
        SELECT COUNT(*)
        FROM signals
        WHERE created_at >= $1
          AND created_at <= $2
          AND ($3::text IS NULL OR symbol = $3)
          AND ($4::text IS NULL OR strategy_id = $4)
        "#,
    )
    .bind(window.start)
    .bind(window.end)
    .bind(request.symbol.as_deref())
    .bind(request.strategy_id.as_deref())
    .fetch_one(pool)
    .await?;

    let risk_counts = query(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE decision = 'APPROVED') AS approved_count,
            COUNT(*) FILTER (WHERE decision = 'REJECTED') AS rejected_count
        FROM risk_decisions
        WHERE decided_at >= $1
          AND decided_at <= $2
          AND ($3::text IS NULL OR COALESCE(rationale::jsonb ->> 'symbol', '') = $3)
          AND ($4::text IS NULL OR COALESCE(rationale::jsonb ->> 'strategy_id', '') = $4)
        "#,
    )
    .bind(window.start)
    .bind(window.end)
    .bind(request.symbol.as_deref())
    .bind(request.strategy_id.as_deref())
    .fetch_one(pool)
    .await?;

    let approved_count: i64 = risk_counts.get("approved_count");
    let rejected_count: i64 = risk_counts.get("rejected_count");
    let total_decisions = approved_count + rejected_count;
    let risk_rejection_rate_pct = if total_decisions == 0 {
        Decimal::ZERO
    } else {
        (Decimal::from(rejected_count) * Decimal::from(100) / Decimal::from(total_decisions))
            .round_dp(2)
    };

    let pair_rows = query(
        r#"
        SELECT
            COALESCE(rationale::jsonb ->> 'strategy_id', 'unknown') AS strategy_id,
            COALESCE(rationale::jsonb ->> 'symbol', 'unknown') AS symbol,
            COUNT(*) AS pair_count
        FROM risk_decisions
        WHERE decision = 'REJECTED'
          AND decided_at >= $1
          AND decided_at <= $2
          AND ($3::text IS NULL OR COALESCE(rationale::jsonb ->> 'symbol', '') = $3)
          AND ($4::text IS NULL OR COALESCE(rationale::jsonb ->> 'strategy_id', '') = $4)
        GROUP BY 1, 2
        ORDER BY pair_count DESC, strategy_id ASC, symbol ASC
        LIMIT 5
        "#,
    )
    .bind(window.start)
    .bind(window.end)
    .bind(request.symbol.as_deref())
    .bind(request.strategy_id.as_deref())
    .fetch_all(pool)
    .await?;

    let analytics_request = StrategyPerformanceRequest {
        strategy_id: request.strategy_id.clone(),
        symbol: request.symbol.clone(),
        timeframe: None,
        mode: StrategyPerformanceMode::Combined,
        start_time: Some(window.start),
        end_time: Some(window.end),
        limit: Some(20),
    };
    let strategy_analytics_summary = db::get_strategy_performance_summary(pool, &analytics_request)
        .await
        .ok();

    let enabled_strategy_count: i64 = query_scalar(
        r#"
        SELECT COUNT(*)
        FROM strategy_configs
        WHERE enabled = TRUE
          AND ($1::text IS NULL OR strategy_id = $1)
          AND (
                $2::text IS NULL
                OR symbols = $2
                OR symbols LIKE $3
                OR symbols LIKE $4
                OR symbols LIKE $5
            )
        "#,
    )
    .bind(request.strategy_id.as_deref())
    .bind(request.symbol.as_deref())
    .bind(request.symbol.as_ref().map(|value| format!("{value},%")))
    .bind(request.symbol.as_ref().map(|value| format!("%,{value},%")))
    .bind(request.symbol.as_ref().map(|value| format!("%,{value}")))
    .fetch_one(pool)
    .await?;

    Ok(StrategyBehaviorData {
        total_strategy_evaluations: evaluations_row.get("total_strategy_evaluations"),
        total_signals,
        risk_rejection_rate_pct,
        strategy_analytics_summary,
        top_rejected_pairs: pair_rows
            .into_iter()
            .map(|row| OperatorReportTopPairCount {
                strategy_id: row.get("strategy_id"),
                symbol: row.get("symbol"),
                count: row.get("pair_count"),
            })
            .collect(),
        enabled_strategy_count,
    })
}

async fn load_risk_snapshot(
    pool: &PgPool,
    request: &OperatorReportRequest,
    window: &ReportWindow,
) -> Result<OperatorReportRiskSnapshot> {
    let counts = query(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE decision = 'APPROVED') AS approved_count,
            COUNT(*) FILTER (WHERE decision = 'REJECTED') AS rejected_count
        FROM risk_decisions
        WHERE decided_at >= $1
          AND decided_at <= $2
          AND ($3::text IS NULL OR COALESCE(rationale::jsonb ->> 'symbol', '') = $3)
          AND ($4::text IS NULL OR COALESCE(rationale::jsonb ->> 'strategy_id', '') = $4)
        "#,
    )
    .bind(window.start)
    .bind(window.end)
    .bind(request.symbol.as_deref())
    .bind(request.strategy_id.as_deref())
    .fetch_one(pool)
    .await?;

    let reason_rows = query(
        r#"
        SELECT reason, COUNT(*) AS reason_count
        FROM (
            SELECT jsonb_array_elements_text(COALESCE(rationale::jsonb -> 'reasons', '[]'::jsonb)) AS reason
            FROM risk_decisions
            WHERE decision = 'REJECTED'
              AND decided_at >= $1
              AND decided_at <= $2
              AND ($3::text IS NULL OR COALESCE(rationale::jsonb ->> 'symbol', '') = $3)
              AND ($4::text IS NULL OR COALESCE(rationale::jsonb ->> 'strategy_id', '') = $4)
        ) reasons
        GROUP BY reason
        ORDER BY reason_count DESC, reason ASC
        LIMIT 5
        "#,
    )
    .bind(window.start)
    .bind(window.end)
    .bind(request.symbol.as_deref())
    .bind(request.strategy_id.as_deref())
    .fetch_all(pool)
    .await?;

    let kill_switch_change_count: i64 = query_scalar(
        r#"
        SELECT COUNT(*)
        FROM system_events
        WHERE event_type IN ('risk.kill_switch.activate', 'risk.kill_switch.resume')
          AND occurred_at >= $1
          AND occurred_at <= $2
        "#,
    )
    .bind(window.start)
    .bind(window.end)
    .fetch_one(pool)
    .await?;

    let risk_config_version = db::get_risk_config(pool)
        .await?
        .map(|config| config.current_version);

    Ok(OperatorReportRiskSnapshot {
        approved_decisions: counts.get("approved_count"),
        rejected_decisions: counts.get("rejected_count"),
        top_rejection_reasons: reason_rows
            .into_iter()
            .map(|row| OperatorReportReasonCount {
                reason: row.get("reason"),
                count: row.get("reason_count"),
            })
            .collect(),
        kill_switch_change_count,
        risk_config_version,
    })
}

async fn load_paper_snapshot(
    pool: &PgPool,
    request: &OperatorReportRequest,
    window: &ReportWindow,
) -> Result<OperatorReportPaperSnapshot> {
    let account = db::get_default_paper_account(pool).await?;
    let account_equity = account
        .as_ref()
        .map(|value| value.current_equity)
        .unwrap_or_default();
    let realized_pnl = account
        .as_ref()
        .map(|value| value.realized_pnl)
        .unwrap_or_default();
    let unrealized_pnl = account
        .as_ref()
        .map(|value| value.unrealized_pnl)
        .unwrap_or_default();

    let counts = query(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE status = 'open') AS open_count,
            COUNT(*) FILTER (
                WHERE status = 'closed'
                  AND closed_at >= $1
                  AND closed_at <= $2
            ) AS closed_count
        FROM paper_positions
        WHERE ($3::text IS NULL OR symbol = $3)
          AND ($4::text IS NULL OR strategy_id = $4)
        "#,
    )
    .bind(window.start)
    .bind(window.end)
    .bind(request.symbol.as_deref())
    .bind(request.strategy_id.as_deref())
    .fetch_one(pool)
    .await?;

    let manual_close_row = query(
        r#"
        SELECT
            COUNT(*) AS manual_close_count,
            COALESCE(SUM(pnl), 0) AS daily_pnl
        FROM paper_trade_journal
        WHERE event_type = 'paper.position.closed'
          AND created_at >= $1
          AND created_at <= $2
          AND COALESCE(payload ->> 'reason', '') = 'manual_operator_exit'
          AND ($3::text IS NULL OR symbol = $3)
        "#,
    )
    .bind(window.start)
    .bind(window.end)
    .bind(request.symbol.as_deref())
    .fetch_one(pool)
    .await?;

    Ok(OperatorReportPaperSnapshot {
        paper_equity: account_equity,
        realized_pnl,
        unrealized_pnl,
        daily_pnl: manual_close_row.get("daily_pnl"),
        open_position_count: counts.get("open_count"),
        closed_position_count: counts.get("closed_count"),
        manual_close_count: manual_close_row.get("manual_close_count"),
    })
}

async fn load_shadow_snapshot(
    pool: &PgPool,
    request: &OperatorReportRequest,
    window: &ReportWindow,
) -> Result<OperatorReportShadowSnapshot> {
    let row = query(
        r#"
        SELECT
            COUNT(*) AS shadow_run_count,
            COUNT(*) FILTER (WHERE decision = 'WOULD_SUBMIT') AS would_submit_count,
            COUNT(*) FILTER (WHERE decision = 'NO_SIGNAL') AS no_signal_count,
            COUNT(*) FILTER (WHERE decision = 'RISK_REJECTED') AS risk_rejected_count,
            COUNT(*) FILTER (
                WHERE decision IN (
                    'SKIPPED_DISABLED_STRATEGY',
                    'SKIPPED_KILL_SWITCH',
                    'SKIPPED_STALE_PRICE',
                    'SKIPPED_STALE_FEED',
                    'ERROR'
                )
            ) AS skipped_count
        FROM testnet_shadow_runs
        WHERE created_at >= $1
          AND created_at <= $2
          AND ($3::text IS NULL OR symbol = $3)
          AND ($4::text IS NULL OR strategy_id = $4)
        "#,
    )
    .bind(window.start)
    .bind(window.end)
    .bind(request.symbol.as_deref())
    .bind(request.strategy_id.as_deref())
    .fetch_one(pool)
    .await?;

    let runner_state = db::get_testnet_shadow_runner_state(pool).await?;
    let runner_status = runner_state
        .as_ref()
        .map(|value| value.status.clone())
        .unwrap_or_else(|| "UNKNOWN".to_string());
    let runner_last_tick_age_seconds =
        runner_state
            .and_then(|value| value.last_tick_at)
            .map(|value| {
                window
                    .generated_at
                    .signed_duration_since(value)
                    .num_seconds()
                    .max(0)
            });

    Ok(OperatorReportShadowSnapshot {
        shadow_run_count: row.get("shadow_run_count"),
        would_submit_count: row.get("would_submit_count"),
        no_signal_count: row.get("no_signal_count"),
        risk_rejected_count: row.get("risk_rejected_count"),
        skipped_count: row.get("skipped_count"),
        runner_status,
        runner_last_tick_age_seconds,
    })
}

async fn load_promotion_snapshot(
    pool: &PgPool,
    request: &OperatorReportRequest,
    window: &ReportWindow,
) -> Result<OperatorReportPromotionSnapshot> {
    let summary = db::get_testnet_promotion_funnel_summary(
        pool,
        &TestnetPromotionFunnelRequest {
            strategy_id: request.strategy_id.clone(),
            symbol: request.symbol.clone(),
            timeframe: None,
            start_time: Some(window.start),
            end_time: Some(window.end),
            limit: Some(100),
        },
    )
    .await?;

    Ok(OperatorReportPromotionSnapshot {
        shadow_would_submit_count: summary.shadow_would_submit_count,
        previewed_count: summary.promotion_previewed_count,
        submitted_count: summary.promotion_submitted_count,
        acked_count: summary.acked_count,
        filled_count: summary.filled_count,
        reconciliation_required_count: summary.reconciliation_required_count,
        preview_rate_pct: summary.preview_rate_pct,
        submit_rate_pct: summary.submit_rate_pct,
        ack_rate_pct: summary.ack_rate_pct,
        fill_rate_pct: summary.fill_rate_pct,
    })
}

async fn load_testnet_snapshot(
    pool: &PgPool,
    request: &OperatorReportRequest,
    window: &ReportWindow,
) -> Result<OperatorReportTestnetSnapshot> {
    let order_row = query(
        r#"
        SELECT
            COUNT(*) AS orders_created,
            COUNT(*) FILTER (
                WHERE execution_state IN (
                    'ORDER_SUBMIT_REQUESTED',
                    'EXCHANGE_ACKED',
                    'CANCEL_REQUESTED',
                    'PARTIALLY_FILLED'
                )
            ) AS active_order_count,
            COUNT(*) FILTER (
                WHERE execution_state IN ('FILLED', 'CANCELLED', 'REJECTED', 'EXPIRED', 'FAILED')
            ) AS terminal_order_count,
            COUNT(*) FILTER (
                WHERE execution_state IN ('UNKNOWN_EXCHANGE_STATE', 'RECONCILIATION_REQUIRED')
            ) AS unknown_order_count
        FROM exchange_testnet_orders
        WHERE created_at >= $1
          AND created_at <= $2
          AND environment = 'testnet'
          AND ($3::text IS NULL OR symbol = $3)
        "#,
    )
    .bind(window.start)
    .bind(window.end)
    .bind(request.symbol.as_deref())
    .fetch_one(pool)
    .await?;

    let reconciliation_run_count: i64 = query_scalar(
        r#"
        SELECT COUNT(*)
        FROM exchange_reconciliation_runs
        WHERE started_at >= $1
          AND started_at <= $2
          AND environment = 'testnet'
        "#,
    )
    .bind(window.start)
    .bind(window.end)
    .fetch_one(pool)
    .await?;

    let mismatch_count: i64 = query_scalar(
        r#"
        SELECT COUNT(*)
        FROM exchange_reconciliation_mismatches m
        JOIN exchange_reconciliation_runs r
          ON r.id = m.run_id
        WHERE r.environment = 'testnet'
          AND m.created_at >= $1
          AND m.created_at <= $2
        "#,
    )
    .bind(window.start)
    .bind(window.end)
    .fetch_one(pool)
    .await?;

    let repair_action_count: i64 = query_scalar(
        r#"
        SELECT COUNT(*)
        FROM exchange_testnet_repair_actions
        WHERE created_at >= $1
          AND created_at <= $2
        "#,
    )
    .bind(window.start)
    .bind(window.end)
    .fetch_one(pool)
    .await?;

    let private_stream_state =
        db::get_exchange_private_stream_state(pool, "binance", "testnet").await?;
    let private_stream_status = private_stream_state
        .as_ref()
        .map(|value| value.status.clone())
        .unwrap_or_else(|| "UNKNOWN".to_string());
    let private_stream_last_event_age_seconds = private_stream_state
        .and_then(|value| value.last_event_at)
        .map(|value| {
            window
                .generated_at
                .signed_duration_since(value)
                .num_seconds()
                .max(0)
        });

    Ok(OperatorReportTestnetSnapshot {
        testnet_orders_created: order_row.get("orders_created"),
        active_order_count: order_row.get("active_order_count"),
        terminal_order_count: order_row.get("terminal_order_count"),
        unknown_order_count: order_row.get("unknown_order_count"),
        reconciliation_run_count,
        mismatch_count,
        repair_action_count,
        private_stream_status,
        private_stream_last_event_age_seconds,
    })
}

async fn load_research_candidate_qualification_snapshot(
    state: &AppState,
    request: &OperatorReportRequest,
    window: &ReportWindow,
) -> Result<OperatorReportResearchQualificationSnapshot> {
    let filters = ResearchCandidateListFilters {
        strategy_id: request.strategy_id.clone(),
        symbol: request.symbol.clone(),
        timeframe: None,
        status: None,
    };
    let default_thresholds = ResearchCandidateQualificationThresholds::default();
    let records =
        list_research_candidates(&state.db_pool, &filters, REPORT_RESEARCH_CANDIDATE_LIMIT).await?;

    let mut summary = OperatorReportResearchQualificationSnapshot {
        total_candidates: records.len() as i64,
        accepted_for_shadow_count: 0,
        qualified_count: 0,
        needs_more_data_count: 0,
        not_qualified_count: 0,
        degraded_count: 0,
        unknown_count: 0,
        stale_observation_count: 0,
        runner_mismatch_count: 0,
        readiness_degraded_count: 0,
        readiness_not_ready_count: 0,
        degraded_or_not_ready_readiness_count: 0,
        below_default_threshold_override_count: 0,
        latest_evaluated_candidates_count: 0,
        newly_qualified_count: 0,
        lost_qualification_count: 0,
        needs_attention_count: 0,
        stale_evaluation_count: 0,
        reviews_in_window: 0,
        ready_for_testnet_review_count: 0,
        ready_for_testnet_review_dossier_count: 0,
        blocked_testnet_review_dossier_count: 0,
        marked_ready_but_dossier_not_ready_count: 0,
        dossier_needs_more_shadow_data_count: 0,
        rejected_from_watchlist_count: 0,
        archived_from_watchlist_count: 0,
        candidates_needing_review_count: 0,
        walk_forward_overfit_risk_count: 0,
        walk_forward_missing_count: 0,
        walk_forward_robust_count: 0,
        top_candidate: None,
    };

    for record in records {
        let candidate = crate::research_candidate_from_record(&record)?;
        if candidate.status == ResearchCandidateStatus::AcceptedForShadow {
            summary.accepted_for_shadow_count += 1;
        }

        let qualification = crate::build_research_candidate_qualification(
            state,
            &candidate,
            default_thresholds.clone(),
            window.generated_at,
        )
        .await?;

        match qualification.status {
            ResearchCandidateQualificationStatus::Qualified => summary.qualified_count += 1,
            ResearchCandidateQualificationStatus::NeedsMoreData => {
                summary.needs_more_data_count += 1
            }
            ResearchCandidateQualificationStatus::NotQualified => summary.not_qualified_count += 1,
            ResearchCandidateQualificationStatus::Degraded => summary.degraded_count += 1,
            ResearchCandidateQualificationStatus::Unknown => summary.unknown_count += 1,
        }

        if !qualification.fresh_observation {
            summary.stale_observation_count += 1;
        }
        if !qualification.runner_alignment_valid {
            summary.runner_mismatch_count += 1;
        }
        match qualification.latest_readiness_status {
            Some(aegis_core::ExecutionReadinessStatus::Degraded) => {
                summary.readiness_degraded_count += 1;
                summary.degraded_or_not_ready_readiness_count += 1;
            }
            Some(aegis_core::ExecutionReadinessStatus::NotReady) => {
                summary.readiness_not_ready_count += 1;
                summary.degraded_or_not_ready_readiness_count += 1;
            }
            _ => {}
        }
        if qualification.threshold_override_below_default {
            summary.below_default_threshold_override_count += 1;
        }
        match qualification.walk_forward_status {
            Some(aegis_core::StrategyWalkForwardRobustnessStatus::OverfitRisk) => {
                summary.walk_forward_overfit_risk_count += 1;
                summary.candidates_needing_review_count += 1;
            }
            Some(aegis_core::StrategyWalkForwardRobustnessStatus::Robust) => {
                summary.walk_forward_robust_count += 1;
            }
            None => {
                summary.walk_forward_missing_count += 1;
                summary.candidates_needing_review_count += 1;
            }
            _ => {}
        }

        let evaluation_history =
            list_research_candidate_qualification_evaluations(&state.db_pool, candidate.id, 2)
                .await?;
        let evaluations = evaluation_history
            .iter()
            .map(research_candidate_qualification_evaluation_from_record)
            .collect::<Result<Vec<_>>>()?;
        if let Some(latest_evaluation) = evaluations.first() {
            summary.latest_evaluated_candidates_count += 1;
            if aegis_core::is_research_candidate_evaluation_stale(
                latest_evaluation,
                window.generated_at,
                Duration::hours(24),
            ) {
                summary.stale_evaluation_count += 1;
            }
            let previous = evaluations.get(1);
            let trend =
                aegis_core::research_candidate_qualification_trend(latest_evaluation, previous);
            match trend {
                aegis_core::ResearchCandidateQualificationTrend::NewlyQualified => {
                    summary.newly_qualified_count += 1;
                    summary.candidates_needing_review_count += 1;
                }
                aegis_core::ResearchCandidateQualificationTrend::LostQualification => {
                    summary.lost_qualification_count += 1;
                    summary.needs_attention_count += 1;
                    summary.candidates_needing_review_count += 1;
                }
                aegis_core::ResearchCandidateQualificationTrend::NeedsAttention => {
                    summary.needs_attention_count += 1;
                    summary.candidates_needing_review_count += 1;
                }
                aegis_core::ResearchCandidateQualificationTrend::Degrading => {
                    summary.needs_attention_count += 1;
                }
                _ => {}
            }
        }

        let candidate_reviews = list_research_candidate_reviews(&state.db_pool, candidate.id)
            .await?
            .iter()
            .map(research_candidate_review_from_record)
            .collect::<Result<Vec<_>>>()?;
        let ready_review_marked = candidate_reviews.iter().any(|review| {
            review.action == aegis_core::ResearchCandidateReviewAction::MarkReadyForTestnetReview
        });

        let dossier = crate::build_research_candidate_testnet_review_dossier(
            state,
            &candidate,
            window.generated_at,
            Uuid::new_v4(),
        )
        .await?;
        match dossier.status {
            ResearchCandidateTestnetReviewStatus::ReadyForReview => {
                summary.ready_for_testnet_review_dossier_count += 1;
            }
            ResearchCandidateTestnetReviewStatus::NeedsMoreShadowData => {
                summary.dossier_needs_more_shadow_data_count += 1;
            }
            ResearchCandidateTestnetReviewStatus::Blocked => {
                summary.blocked_testnet_review_dossier_count += 1;
            }
            _ => {}
        }
        if ready_review_marked
            && dossier.status != ResearchCandidateTestnetReviewStatus::ReadyForReview
        {
            summary.marked_ready_but_dossier_not_ready_count += 1;
        }

        let replace_top = summary
            .top_candidate
            .as_ref()
            .map(|current| qualification.score > current.score)
            .unwrap_or(true);
        if replace_top {
            summary.top_candidate = Some(OperatorReportResearchQualificationTopCandidate {
                candidate_id: candidate.id,
                strategy_id: candidate.strategy_id.clone(),
                symbol: candidate.symbol.clone(),
                timeframe: candidate.timeframe.clone(),
                status: qualification.status,
                score: qualification.score,
                readiness_status: qualification.latest_readiness_status,
            });
        }
    }

    let review_counts = sqlx::query(
        r#"
        SELECT
            COUNT(*)::BIGINT AS reviews_in_window,
            COUNT(*) FILTER (WHERE review.action = 'MARK_READY_FOR_TESTNET_REVIEW')::BIGINT AS ready_for_testnet_review_count,
            COUNT(*) FILTER (WHERE review.action = 'REJECT_FROM_WATCHLIST')::BIGINT AS rejected_from_watchlist_count,
            COUNT(*) FILTER (WHERE review.action = 'ARCHIVE_FROM_WATCHLIST')::BIGINT AS archived_from_watchlist_count
        FROM research_candidate_reviews review
        INNER JOIN research_candidates candidate
            ON candidate.id = review.candidate_id
        WHERE review.created_at >= $1
          AND review.created_at <= $2
          AND ($3::text IS NULL OR candidate.symbol = $3)
          AND ($4::text IS NULL OR candidate.strategy_id = $4)
        "#,
    )
    .bind(window.start)
    .bind(window.end)
    .bind(request.symbol.as_deref())
    .bind(request.strategy_id.as_deref())
    .fetch_one(&state.db_pool)
    .await?;

    summary.reviews_in_window = review_counts.get("reviews_in_window");
    summary.ready_for_testnet_review_count = review_counts.get("ready_for_testnet_review_count");
    summary.rejected_from_watchlist_count = review_counts.get("rejected_from_watchlist_count");
    summary.archived_from_watchlist_count = review_counts.get("archived_from_watchlist_count");

    Ok(summary)
}

async fn load_backtest_activity(
    pool: &PgPool,
    request: &OperatorReportRequest,
    window: &ReportWindow,
) -> Result<BacktestActivity> {
    let run_count: i64 = query_scalar(
        r#"
        SELECT COUNT(*)
        FROM backtest_runs
        WHERE created_at >= $1
          AND created_at <= $2
          AND ($3::text IS NULL OR symbol = $3)
          AND ($4::text IS NULL OR strategy_id = $4)
        "#,
    )
    .bind(window.start)
    .bind(window.end)
    .bind(request.symbol.as_deref())
    .bind(request.strategy_id.as_deref())
    .fetch_one(pool)
    .await?;

    Ok(BacktestActivity { run_count })
}

async fn load_candidate_shadow_pnl_snapshot(
    state: &AppState,
    research_qualification: &OperatorReportResearchQualificationSnapshot,
    request: &OperatorReportRequest,
    window: &ReportWindow,
) -> Result<Option<CandidateShadowPnlReportSnapshot>> {
    let Some(top_candidate) = research_qualification.top_candidate.as_ref() else {
        return Ok(None);
    };
    let Some(record) = get_research_candidate(&state.db_pool, top_candidate.candidate_id).await?
    else {
        return Ok(None);
    };
    let candidate = research_candidate_from_record(&record)?;
    let attribution = get_research_candidate_shadow_pnl_attribution(
        &state.db_pool,
        &candidate,
        &ResearchShadowPnlAttributionRequest {
            candidate_id: candidate.id,
            holding_windows: vec![1, 3, 5, 10],
            fee_bps: Decimal::new(10, 0),
            slippage_bps: Decimal::new(5, 0),
            extreme_pnl_threshold_pct: Decimal::new(5, 0),
            start_time: Some(window.start),
            end_time: Some(window.end),
            limit: Some(100),
        },
        window.generated_at,
    )
    .await?;

    if let Some(strategy_id) = request.strategy_id.as_ref() {
        if attribution.strategy_id != *strategy_id {
            return Ok(None);
        }
    }
    if let Some(symbol) = request.symbol.as_ref() {
        if !attribution.symbol.eq_ignore_ascii_case(symbol) {
            return Ok(None);
        }
    }

    Ok(Some(CandidateShadowPnlReportSnapshot::from(&attribution)))
}

fn build_findings(
    system_market: &SystemAndMarketData,
    strategy: &StrategyBehaviorData,
    risk: &OperatorReportRiskSnapshot,
    paper: &OperatorReportPaperSnapshot,
    shadow: &OperatorReportShadowSnapshot,
    promotion: &OperatorReportPromotionSnapshot,
    testnet: &OperatorReportTestnetSnapshot,
    research_qualification: &OperatorReportResearchQualificationSnapshot,
    shadow_pnl: Option<&CandidateShadowPnlReportSnapshot>,
    backtest: &BacktestActivity,
) -> Vec<OperatorReportFinding> {
    let mut findings = Vec::new();

    if let Some(quality) = system_market.market.data_quality.as_ref() {
        match quality.status {
            MarketDataQualityStatus::Bad => findings.push(finding(
                "market_data_quality_bad",
                OperatorReportSeverity::High,
                "Market data quality BAD",
                &format!(
                    "{} {} coverage={}%, gaps={}, largest_gap_seconds={}",
                    quality.symbol,
                    quality.interval,
                    quality.coverage_pct,
                    quality.gap_count,
                    quality.largest_gap_seconds
                ),
                "market_data_quality",
            )),
            MarketDataQualityStatus::Degraded => findings.push(finding(
                "market_data_quality_degraded",
                OperatorReportSeverity::Medium,
                "Market data quality DEGRADED",
                &format!(
                    "{} {} coverage={}%, gaps={}, largest_gap_seconds={}",
                    quality.symbol,
                    quality.interval,
                    quality.coverage_pct,
                    quality.gap_count,
                    quality.largest_gap_seconds
                ),
                "market_data_quality",
            )),
            MarketDataQualityStatus::Good if quality.gap_count > 0 => findings.push(finding(
                "minor_candle_gaps_detected",
                OperatorReportSeverity::Low,
                "Minor candle gaps detected",
                &format!(
                    "{} {} has {} gaps in the selected window.",
                    quality.symbol, quality.interval, quality.gap_count
                ),
                "market_data_quality",
            )),
            _ => {}
        }
    }
    if system_market.market.repair_failed_count > 0 {
        findings.push(finding(
            "market_data_repair_failed",
            OperatorReportSeverity::High,
            "Market data repair failed",
            &format!(
                "{} market data repair runs failed in the selected window.",
                system_market.market.repair_failed_count
            ),
            "market_data_repair",
        ));
    }
    if system_market.market.repair_partial_count > 0
        || system_market.market.repair_degraded_after_count > 0
    {
        findings.push(finding(
            "market_data_remains_degraded_after_repair",
            OperatorReportSeverity::Medium,
            "Market data remains degraded after repair",
            &format!(
                "partial_repairs={}, degraded_after_repair={}",
                system_market.market.repair_partial_count,
                system_market.market.repair_degraded_after_count
            ),
            "market_data_repair",
        ));
    }
    if system_market.market.repair_completed_count > 0
        && system_market.market.repair_failed_count == 0
        && system_market.market.repair_partial_count == 0
    {
        findings.push(finding(
            "market_data_repair_completed",
            OperatorReportSeverity::Low,
            "Market data repair completed",
            &format!(
                "{} market data repair runs completed in the selected window.",
                system_market.market.repair_completed_count
            ),
            "market_data_repair",
        ));
    }

    if system_market.system.kill_switch_active {
        findings.push(finding(
            "kill_switch_active",
            OperatorReportSeverity::Critical,
            "Kill switch active",
            "Execution remains halted until an operator explicitly resumes the system.",
            "system_health",
        ));
    }

    if testnet
        .private_stream_last_event_age_seconds
        .map(|age| age > i64::from(system_market.stale_threshold_seconds))
        .unwrap_or(true)
        || !testnet
            .private_stream_status
            .eq_ignore_ascii_case("CONNECTED")
    {
        findings.push(finding(
            "private_stream_stale",
            OperatorReportSeverity::High,
            "Private stream stale",
            "Testnet private-stream state is stale or not connected, so lifecycle evidence may be incomplete.",
            "testnet_execution",
        ));
    }

    if promotion.reconciliation_required_count > 0 || testnet.mismatch_count > 0 {
        findings.push(finding(
            "reconciliation_required_present",
            OperatorReportSeverity::High,
            "Reconciliation required items present",
            "At least one promoted testnet order requires reconciliation review.",
            "testnet_execution",
        ));
    }

    if strategy.risk_rejection_rate_pct > Decimal::from(50) {
        findings.push(finding(
            "high_risk_rejection_rate",
            OperatorReportSeverity::Medium,
            "Risk rejection rate above 50%",
            "More than half of persisted risk decisions were rejected in the selected window.",
            "risk",
        ));
    }

    if strategy.enabled_strategy_count > 0
        && shadow.would_submit_count == 0
        && (strategy.total_strategy_evaluations > 0 || strategy.total_signals > 0)
    {
        findings.push(finding(
            "zero_shadow_would_submit",
            OperatorReportSeverity::Medium,
            "No shadow WOULD_SUBMIT outcomes",
            "Enabled strategies produced zero WOULD_SUBMIT shadow outcomes in the selected window.",
            "shadow_mode",
        ));
    }

    let unrealized_loss_threshold = paper.paper_equity.abs() * Decimal::new(2, 2);
    if paper.unrealized_pnl < Decimal::ZERO
        && paper.unrealized_pnl.abs() >= unrealized_loss_threshold
    {
        findings.push(finding(
            "paper_unrealized_loss_threshold",
            OperatorReportSeverity::Medium,
            "Paper unrealized loss threshold breached",
            "Current unrealized paper loss exceeds 2% of paper equity.",
            "paper_trading",
        ));
    }

    if backtest.run_count == 0 {
        findings.push(finding(
            "no_backtest_runs",
            OperatorReportSeverity::Low,
            "No backtest runs in window",
            "No persisted backtest runs were recorded for the selected window.",
            "strategy_behavior",
        ));
    }

    if system_market.market.stale_feed_count == 0 {
        let threshold = i64::from(system_market.stale_threshold_seconds);
        if system_market
            .market
            .feeds
            .iter()
            .any(|feed| feed.last_event_age_seconds.unwrap_or_default() >= ((threshold * 8) / 10))
        {
            findings.push(finding(
                "feed_age_approaching_stale",
                OperatorReportSeverity::Low,
                "Feed age approaching stale threshold",
                "At least one market feed is close to the configured stale threshold.",
                "market_health",
            ));
        }
    }

    if system_market.market.stale_feed_count > 0 {
        findings.push(finding(
            "stale_market_feeds",
            OperatorReportSeverity::Medium,
            "Stale market feeds detected",
            "One or more market feeds are stale for the selected symbols.",
            "market_health",
        ));
    }

    if research_qualification.qualified_count == 0 {
        findings.push(finding(
            "no_qualified_candidates",
            OperatorReportSeverity::Low,
            "No qualified candidates yet",
            "No research candidates currently qualify for testnet promotion consideration under default thresholds.",
            "research_candidate_qualification",
        ));
    }

    if research_qualification.needs_more_data_count > 0
        && research_qualification.accepted_for_shadow_count > 0
    {
        findings.push(finding(
            "accepted_candidates_need_more_shadow_data",
            OperatorReportSeverity::Medium,
            "Accepted candidates still need more shadow data",
            "One or more ACCEPTED_FOR_SHADOW candidates still need more linked shadow evidence before promotion review.",
            "research_candidate_qualification",
        ));
    }

    if research_qualification.below_default_threshold_override_count > 0 {
        findings.push(finding(
            "qualification_below_default_thresholds",
            OperatorReportSeverity::Medium,
            "Candidate qualification used below-default thresholds",
            "At least one candidate qualification result used below-default thresholds and should be treated as exploratory.",
            "research_candidate_qualification",
        ));
    }

    if research_qualification.readiness_degraded_count > 0 {
        findings.push(finding(
            "candidate_readiness_degraded",
            OperatorReportSeverity::Medium,
            "Candidate qualification is DEGRADED due to readiness",
            "At least one candidate carries DEGRADED TESTNET_SHADOW readiness and should not be treated as promotion-ready.",
            "research_candidate_qualification",
        ));
    }

    if research_qualification.readiness_not_ready_count > 0 {
        findings.push(finding(
            "candidate_readiness_not_ready_blocker",
            OperatorReportSeverity::High,
            "Candidate qualification reports NOT_READY readiness blocker",
            "At least one candidate is blocked by NOT_READY TESTNET_SHADOW readiness.",
            "research_candidate_qualification",
        ));
    }

    if research_qualification.stale_evaluation_count > 0 {
        findings.push(finding(
            "candidate_qualification_evaluations_stale",
            OperatorReportSeverity::Low,
            "Candidate qualification evaluations are stale",
            "One or more persisted qualification evaluations are older than 24 hours.",
            "research_candidate_qualification",
        ));
    }

    if research_qualification.lost_qualification_count > 0 {
        findings.push(finding(
            "candidate_lost_qualification",
            OperatorReportSeverity::Medium,
            "Candidate lost qualification",
            "At least one persisted qualification evaluation shows a candidate lost qualified status.",
            "research_candidate_qualification",
        ));
    }

    if research_qualification.walk_forward_overfit_risk_count > 0 {
        findings.push(finding(
            "candidate_walk_forward_overfit_risk",
            OperatorReportSeverity::High,
            "Candidate has OVERFIT_RISK walk-forward result",
            "One or more research candidates have OVERFIT_RISK walk-forward evidence and should not advance toward testnet review.",
            "research_candidate_qualification",
        ));
    }

    if research_qualification.walk_forward_missing_count > 0 {
        findings.push(finding(
            "candidate_walk_forward_missing",
            OperatorReportSeverity::Medium,
            "Candidate missing walk-forward validation",
            "One or more research candidates are missing linked walk-forward evidence.",
            "research_candidate_qualification",
        ));
    }

    if research_qualification.walk_forward_robust_count > 0 {
        findings.push(finding(
            "candidate_walk_forward_robust",
            OperatorReportSeverity::Low,
            "Candidate has ROBUST walk-forward evidence",
            "One or more research candidates have ROBUST linked walk-forward evidence.",
            "research_candidate_qualification",
        ));
    }

    if let Some(shadow_pnl) = shadow_pnl {
        if shadow_pnl.total_attributed_runs == 0 || shadow_pnl.insufficient_forward_data_count > 0 {
            findings.push(finding(
                "candidate_shadow_pnl_insufficient_data",
                OperatorReportSeverity::Low,
                "Shadow PnL attribution insufficient data",
                "The top candidate has missing forward candles for at least one linked WOULD_SUBMIT shadow run.",
                "candidate_shadow_pnl_attribution",
            ));
        }
        if shadow_pnl.negative_all_windows {
            findings.push(finding(
                "candidate_shadow_pnl_negative_all_windows",
                OperatorReportSeverity::Medium,
                "Candidate shadow PnL negative across all tested holding windows",
                "Stored-candle hypothetical attribution is negative across all tested holding windows for the top candidate.",
                "candidate_shadow_pnl_attribution",
            ));
        }
        if shadow_pnl.recommendation == ResearchShadowPnlRecommendation::Promising {
            findings.push(finding(
                "candidate_shadow_pnl_promising_window",
                OperatorReportSeverity::Low,
                "Candidate shadow PnL promising in at least one holding window",
                "Stored-candle hypothetical attribution is promising for at least one tested holding window.",
                "candidate_shadow_pnl_attribution",
            ));
        }
    }

    if research_qualification.newly_qualified_count > 0 {
        findings.push(finding(
            "candidate_newly_qualified_awaiting_review",
            OperatorReportSeverity::Low,
            "Newly qualified candidates awaiting review",
            "At least one candidate became qualified and should receive explicit operator review.",
            "research_candidate_qualification",
        ));
    }

    if research_qualification.ready_for_testnet_review_count > 0 {
        findings.push(finding(
            "candidate_ready_for_testnet_review",
            OperatorReportSeverity::Low,
            "Candidates marked ready for testnet review",
            "One or more review actions recorded readiness for testnet review without submitting orders.",
            "research_candidate_qualification",
        ));
    }

    if research_qualification.blocked_testnet_review_dossier_count > 0 {
        findings.push(finding(
            "candidate_marked_ready_but_dossier_blocked",
            OperatorReportSeverity::Medium,
            "Candidate marked ready for testnet review but dossier is blocked",
            "At least one candidate has review readiness recorded while the read-only dossier remains blocked.",
            "research_candidate_qualification",
        ));
    }

    if research_qualification.ready_for_testnet_review_dossier_count > 0 {
        findings.push(finding(
            "candidate_ready_for_testnet_review_dossier",
            OperatorReportSeverity::Low,
            "Candidate ready for testnet review dossier",
            "At least one research candidate has a ready-for-review dossier without submitting orders.",
            "research_candidate_qualification",
        ));
    }

    if research_qualification.dossier_needs_more_shadow_data_count > 0 {
        findings.push(finding(
            "candidate_testnet_review_dossier_needs_more_shadow_data",
            OperatorReportSeverity::Medium,
            "Candidate needs more shadow data before testnet review",
            "At least one research candidate dossier still needs more linked shadow evidence before controlled testnet review.",
            "research_candidate_qualification",
        ));
    }

    if research_qualification.needs_attention_count > 0 {
        findings.push(finding(
            "candidate_needs_attention",
            OperatorReportSeverity::Medium,
            "Candidate needs attention",
            "At least one candidate is degrading, stale, or otherwise requires qualification follow-up.",
            "research_candidate_qualification",
        ));
    }

    if findings.is_empty() {
        findings.push(finding(
            "low_activity_report",
            OperatorReportSeverity::Info,
            "No material operator issues detected",
            "The selected window completed without critical, high, medium, or low-severity report conditions.",
            "summary",
        ));
    }

    if risk.approved_decisions == 0
        && risk.rejected_decisions == 0
        && paper.manual_close_count == 0
        && shadow.shadow_run_count == 0
        && testnet.testnet_orders_created == 0
        && backtest.run_count == 0
    {
        findings.push(finding(
            "empty_window_activity",
            OperatorReportSeverity::Low,
            "Minimal persisted activity",
            "No paper closes, shadow runs, backtests, or testnet orders were persisted in the selected window.",
            "summary",
        ));
    }

    findings
}

fn build_recommendations(findings: &[OperatorReportFinding]) -> Vec<OperatorReportRecommendation> {
    let mut recommendations = Vec::new();

    if findings
        .iter()
        .any(|finding| finding.code == "reconciliation_required_present")
    {
        recommendations.push(recommendation(
            "review_reconciliation",
            OperatorReportSeverity::High,
            "Review reconciliation mismatches before any further testnet promotion.",
            &["reconciliation_required_present"],
        ));
    }

    if findings
        .iter()
        .any(|finding| finding.code == "private_stream_stale")
    {
        recommendations.push(recommendation(
            "stabilize_private_stream",
            OperatorReportSeverity::High,
            "Keep system in shadow mode until private stream is stable.",
            &["private_stream_stale"],
        ));
    }

    if findings.iter().any(|finding| {
        matches!(
            finding.code.as_str(),
            "stale_market_feeds" | "feed_age_approaching_stale"
        )
    }) {
        recommendations.push(recommendation(
            "restore_feed_freshness",
            OperatorReportSeverity::Medium,
            "Backfill missing candles before trusting backtest results or promotion decisions.",
            &["stale_market_feeds", "feed_age_approaching_stale"],
        ));
    }

    if findings
        .iter()
        .any(|finding| finding.code == "high_risk_rejection_rate")
    {
        recommendations.push(recommendation(
            "inspect_risk_boundaries",
            OperatorReportSeverity::Medium,
            "Review recent risk rejections and strategy inputs before expanding promotion scope.",
            &["high_risk_rejection_rate"],
        ));
    }

    if findings
        .iter()
        .any(|finding| finding.code == "kill_switch_active")
    {
        recommendations.push(recommendation(
            "validate_kill_switch",
            OperatorReportSeverity::Critical,
            "Confirm the kill-switch reason and keep execution paused until the blocking condition is resolved.",
            &["kill_switch_active"],
        ));
    }

    if findings
        .iter()
        .any(|finding| finding.code == "accepted_candidates_need_more_shadow_data")
    {
        recommendations.push(recommendation(
            "gather_more_shadow_evidence",
            OperatorReportSeverity::Medium,
            "Keep candidates in observation until linked shadow runs and WOULD_SUBMIT evidence meet default thresholds.",
            &["accepted_candidates_need_more_shadow_data"],
        ));
    }

    if findings
        .iter()
        .any(|finding| finding.code == "candidate_readiness_not_ready_blocker")
    {
        recommendations.push(recommendation(
            "clear_candidate_readiness_blockers",
            OperatorReportSeverity::High,
            "Clear TESTNET_SHADOW readiness blockers before considering any testnet promotion.",
            &["candidate_readiness_not_ready_blocker"],
        ));
    }

    if findings
        .iter()
        .any(|finding| finding.code == "candidate_readiness_degraded")
    {
        recommendations.push(recommendation(
            "resolve_candidate_readiness_degradation",
            OperatorReportSeverity::Medium,
            "Resolve degraded readiness conditions before considering candidate promotion.",
            &["candidate_readiness_degraded"],
        ));
    }

    if findings.iter().any(|finding| {
        finding.code == "market_data_quality_bad"
            || finding.code == "market_data_quality_degraded"
            || finding.code == "minor_candle_gaps_detected"
    }) {
        recommendations.push(recommendation(
            "repair_market_data_quality",
            OperatorReportSeverity::Medium,
            "Backfill or exclude gappy candle windows before trusting research and promotion evidence.",
            &[
                "market_data_quality_bad",
                "market_data_quality_degraded",
                "minor_candle_gaps_detected",
            ],
        ));
    }

    recommendations
}

fn build_sections(
    system: &OperatorReportSystemSnapshot,
    market: &OperatorReportMarketSnapshot,
    strategy: &StrategyBehaviorData,
    risk: &OperatorReportRiskSnapshot,
    paper: &OperatorReportPaperSnapshot,
    shadow: &OperatorReportShadowSnapshot,
    promotion: &OperatorReportPromotionSnapshot,
    testnet: &OperatorReportTestnetSnapshot,
    research_qualification: &OperatorReportResearchQualificationSnapshot,
    shadow_pnl: Option<&CandidateShadowPnlReportSnapshot>,
) -> Result<Vec<OperatorReportSection>> {
    let mut sections = vec![
        section(
            "system_health",
            "System Health",
            if system.kill_switch_active {
                OperatorReportStatus::Critical
            } else if !system.db_healthy || !system.api_healthy {
                OperatorReportStatus::Warning
            } else {
                OperatorReportStatus::Ok
            },
            if system.kill_switch_active {
                "Kill switch is active."
            } else {
                "Core service health is available."
            },
            vec![
                highlight("API Healthy", yes_no(system.api_healthy)),
                highlight("DB Healthy", yes_no(system.db_healthy)),
                highlight("Kill Switch", yes_no(system.kill_switch_active)),
                highlight("Auth Enabled", yes_no(system.auth_enabled)),
                highlight("Metrics Available", yes_no(system.metrics_available)),
                highlight("Uptime Seconds", system.uptime_seconds.to_string()),
            ],
            system,
        )?,
        section(
            "market_health",
            "Market Health",
            if market.stale_feed_count > 0 {
                OperatorReportStatus::Warning
            } else {
                OperatorReportStatus::Ok
            },
            "Feed freshness, backfill activity, and candle coverage for the selected window.",
            vec![
                highlight("Stale Feeds", market.stale_feed_count.to_string()),
                highlight("Degraded Feeds", market.degraded_feed_count.to_string()),
                highlight(
                    "Backfills Completed",
                    market.backfill_completed_count.to_string(),
                ),
                highlight("Backfills Failed", market.backfill_failed_count.to_string()),
                highlight(
                    "Candles In Window",
                    market.candle_count_in_window.to_string(),
                ),
            ],
            market,
        )?,
        section(
            "strategy_behavior",
            "Strategy Behavior",
            if strategy.risk_rejection_rate_pct > Decimal::from(50) {
                OperatorReportStatus::Warning
            } else {
                OperatorReportStatus::Ok
            },
            "Persisted strategy evaluation, signal, and rejection behavior.",
            vec![
                highlight(
                    "Strategy Evaluations",
                    strategy.total_strategy_evaluations.to_string(),
                ),
                highlight("Signals", strategy.total_signals.to_string()),
                highlight(
                    "Risk Rejection Rate",
                    format!("{}%", strategy.risk_rejection_rate_pct.round_dp(2)),
                ),
                highlight(
                    "Top Rejected Pairs",
                    if strategy.top_rejected_pairs.is_empty() {
                        "-".to_string()
                    } else {
                        strategy
                            .top_rejected_pairs
                            .iter()
                            .map(|pair| {
                                format!("{}:{} ({})", pair.strategy_id, pair.symbol, pair.count)
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    },
                ),
            ],
            &OperatorReportStrategySnapshot {
                total_strategy_evaluations: strategy.total_strategy_evaluations,
                total_signals: strategy.total_signals,
                risk_rejection_rate_pct: strategy.risk_rejection_rate_pct,
                strategy_analytics_summary: strategy.strategy_analytics_summary.clone(),
                top_rejected_pairs: strategy.top_rejected_pairs.clone(),
            },
        )?,
        section(
            "risk",
            "Risk",
            if risk.rejected_decisions > risk.approved_decisions {
                OperatorReportStatus::Warning
            } else {
                OperatorReportStatus::Ok
            },
            "Approved and rejected risk decisions plus top rejection reasons.",
            vec![
                highlight("Approved", risk.approved_decisions.to_string()),
                highlight("Rejected", risk.rejected_decisions.to_string()),
                highlight(
                    "Kill Switch Changes",
                    risk.kill_switch_change_count.to_string(),
                ),
                highlight(
                    "Risk Config Version",
                    risk.risk_config_version
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                ),
            ],
            risk,
        )?,
        section(
            "paper_trading",
            "Paper Trading",
            if paper.unrealized_pnl < Decimal::ZERO {
                OperatorReportStatus::Warning
            } else {
                OperatorReportStatus::Ok
            },
            "Current paper account state, open exposure, and close activity.",
            vec![
                highlight("Paper Equity", paper.paper_equity.round_dp(2).to_string()),
                highlight("Realized PnL", paper.realized_pnl.round_dp(2).to_string()),
                highlight(
                    "Unrealized PnL",
                    paper.unrealized_pnl.round_dp(2).to_string(),
                ),
                highlight("Daily PnL", paper.daily_pnl.round_dp(2).to_string()),
                highlight("Open Positions", paper.open_position_count.to_string()),
                highlight("Closed Positions", paper.closed_position_count.to_string()),
                highlight("Manual Closes", paper.manual_close_count.to_string()),
            ],
            paper,
        )?,
        section(
            "shadow_mode",
            "Shadow Mode",
            if shadow.runner_status.eq_ignore_ascii_case("ERROR") {
                OperatorReportStatus::Warning
            } else {
                OperatorReportStatus::Ok
            },
            "Shadow runner status and outcome distribution.",
            vec![
                highlight("Shadow Runs", shadow.shadow_run_count.to_string()),
                highlight("Would Submit", shadow.would_submit_count.to_string()),
                highlight("No Signal", shadow.no_signal_count.to_string()),
                highlight("Risk Rejected", shadow.risk_rejected_count.to_string()),
                highlight("Skipped", shadow.skipped_count.to_string()),
                highlight("Runner Status", shadow.runner_status.clone()),
                highlight(
                    "Runner Last Tick Age Seconds",
                    shadow
                        .runner_last_tick_age_seconds
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                ),
            ],
            shadow,
        )?,
        section(
            "research_candidate_qualification",
            "Research Candidate Qualification",
            if research_qualification.readiness_not_ready_count > 0 {
                OperatorReportStatus::Warning
            } else if research_qualification.degraded_count > 0
                || research_qualification.needs_more_data_count > 0
            {
                OperatorReportStatus::Warning
            } else {
                OperatorReportStatus::Ok
            },
            "Read-only qualification summary for research candidates under default thresholds.",
            vec![
                highlight(
                    "Total Candidates",
                    research_qualification.total_candidates.to_string(),
                ),
                highlight(
                    "Accepted For Shadow",
                    research_qualification.accepted_for_shadow_count.to_string(),
                ),
                highlight(
                    "Qualified",
                    research_qualification.qualified_count.to_string(),
                ),
                highlight(
                    "Needs More Data",
                    research_qualification.needs_more_data_count.to_string(),
                ),
                highlight(
                    "Not Qualified",
                    research_qualification.not_qualified_count.to_string(),
                ),
                highlight(
                    "Degraded",
                    research_qualification.degraded_count.to_string(),
                ),
                highlight("Unknown", research_qualification.unknown_count.to_string()),
                highlight(
                    "Stale Observation",
                    research_qualification.stale_observation_count.to_string(),
                ),
                highlight(
                    "Runner Mismatch",
                    research_qualification.runner_mismatch_count.to_string(),
                ),
                highlight(
                    "Degraded/Not Ready Readiness",
                    research_qualification
                        .degraded_or_not_ready_readiness_count
                        .to_string(),
                ),
                highlight(
                    "Latest Evaluated",
                    research_qualification
                        .latest_evaluated_candidates_count
                        .to_string(),
                ),
                highlight(
                    "Newly Qualified",
                    research_qualification.newly_qualified_count.to_string(),
                ),
                highlight(
                    "Lost Qualification",
                    research_qualification.lost_qualification_count.to_string(),
                ),
                highlight(
                    "Needs Attention",
                    research_qualification.needs_attention_count.to_string(),
                ),
                highlight(
                    "Stale Evaluations",
                    research_qualification.stale_evaluation_count.to_string(),
                ),
                highlight(
                    "Reviews In Window",
                    research_qualification.reviews_in_window.to_string(),
                ),
                highlight(
                    "Ready For Testnet Review",
                    research_qualification
                        .ready_for_testnet_review_count
                        .to_string(),
                ),
                highlight(
                    "Ready Review Dossiers",
                    research_qualification
                        .ready_for_testnet_review_dossier_count
                        .to_string(),
                ),
                highlight(
                    "Blocked Review Dossiers",
                    research_qualification
                        .blocked_testnet_review_dossier_count
                        .to_string(),
                ),
                highlight(
                    "Marked Ready But Dossier Not Ready",
                    research_qualification
                        .marked_ready_but_dossier_not_ready_count
                        .to_string(),
                ),
                highlight(
                    "Dossiers Need More Shadow Data",
                    research_qualification
                        .dossier_needs_more_shadow_data_count
                        .to_string(),
                ),
                highlight(
                    "Rejected From Watchlist",
                    research_qualification
                        .rejected_from_watchlist_count
                        .to_string(),
                ),
                highlight(
                    "Archived From Watchlist",
                    research_qualification
                        .archived_from_watchlist_count
                        .to_string(),
                ),
                highlight(
                    "Candidates Needing Review",
                    research_qualification
                        .candidates_needing_review_count
                        .to_string(),
                ),
                highlight(
                    "Top Candidate",
                    research_qualification
                        .top_candidate
                        .as_ref()
                        .map(|candidate| {
                            format!(
                                "{}:{} {} {} score={} readiness={}",
                                candidate.strategy_id,
                                candidate.symbol,
                                candidate.timeframe,
                                candidate.status.as_str(),
                                candidate.score,
                                candidate
                                    .readiness_status
                                    .map(|value| value.as_str())
                                    .unwrap_or("UNKNOWN")
                            )
                        })
                        .unwrap_or_else(|| "-".to_string()),
                ),
            ],
            research_qualification,
        )?,
    ];

    if let Some(quality) = market.data_quality.as_ref() {
        sections.push(section(
            "market_data_quality",
            "Market Data Quality",
            match quality.status {
                MarketDataQualityStatus::Good => OperatorReportStatus::Ok,
                MarketDataQualityStatus::Degraded => OperatorReportStatus::Warning,
                MarketDataQualityStatus::Bad | MarketDataQualityStatus::InsufficientData => {
                    OperatorReportStatus::Warning
                }
                MarketDataQualityStatus::Unknown => OperatorReportStatus::Warning,
            },
            "Candle coverage and continuity for the selected symbol, timeframe, and report window.",
            vec![
                highlight("Status", quality.status.as_str()),
                highlight("Symbol", quality.symbol.clone()),
                highlight("Interval", quality.interval.clone()),
                highlight("Coverage %", quality.coverage_pct.to_string()),
                highlight("Gap Count", quality.gap_count.to_string()),
                highlight(
                    "Largest Gap Seconds",
                    quality.largest_gap_seconds.to_string(),
                ),
            ],
            quality,
        )?);
    }

    if let Some(shadow_pnl) = shadow_pnl {
        sections.push(section(
            "candidate_shadow_pnl_attribution",
            "Candidate Shadow PnL Attribution",
            if shadow_pnl.negative_all_windows {
                OperatorReportStatus::Warning
            } else {
                OperatorReportStatus::Ok
            },
            "Research-only stored-candle hypothetical PnL attribution for the top candidate.",
            vec![
                highlight(
                    "Top Candidate",
                    format!(
                        "{}:{} {}",
                        shadow_pnl.strategy_id, shadow_pnl.symbol, shadow_pnl.timeframe
                    ),
                ),
                highlight(
                    "Attributed Runs",
                    shadow_pnl.total_attributed_runs.to_string(),
                ),
                highlight(
                    "Insufficient Forward Data",
                    shadow_pnl.insufficient_forward_data_count.to_string(),
                ),
                highlight(
                    "Best Holding Window",
                    shadow_pnl
                        .best_holding_window
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                ),
                highlight(
                    "Best Avg Net PnL %",
                    shadow_pnl
                        .best_avg_net_pnl_pct
                        .map(|value| value.round_dp(4).to_string())
                        .unwrap_or_else(|| "-".to_string()),
                ),
                highlight("Recommendation", shadow_pnl.recommendation.as_str()),
                highlight(
                    "Negative All Windows",
                    yes_no(shadow_pnl.negative_all_windows),
                ),
            ],
            shadow_pnl,
        )?);
    }

    sections.extend(vec![
        section(
            "promotion_funnel",
            "Promotion Funnel",
            if promotion.reconciliation_required_count > 0 {
                OperatorReportStatus::Warning
            } else {
                OperatorReportStatus::Ok
            },
            "Shadow-to-testnet promotion progression and rates.",
            vec![
                highlight(
                    "Shadow Would Submit",
                    promotion.shadow_would_submit_count.to_string(),
                ),
                highlight("Previewed", promotion.previewed_count.to_string()),
                highlight("Submitted", promotion.submitted_count.to_string()),
                highlight("Acked", promotion.acked_count.to_string()),
                highlight("Filled", promotion.filled_count.to_string()),
                highlight(
                    "Reconciliation Required",
                    promotion.reconciliation_required_count.to_string(),
                ),
                highlight("Preview Rate", format!("{}%", promotion.preview_rate_pct)),
                highlight("Submit Rate", format!("{}%", promotion.submit_rate_pct)),
                highlight("Ack Rate", format!("{}%", promotion.ack_rate_pct)),
                highlight("Fill Rate", format!("{}%", promotion.fill_rate_pct)),
            ],
            promotion,
        )?,
        section(
            "testnet_execution",
            "Testnet Execution",
            if testnet.mismatch_count > 0
                || !testnet
                    .private_stream_status
                    .eq_ignore_ascii_case("CONNECTED")
            {
                OperatorReportStatus::Warning
            } else {
                OperatorReportStatus::Ok
            },
            "Isolated testnet lifecycle state, reconciliation, repair, and private-stream health.",
            vec![
                highlight("Orders Created", testnet.testnet_orders_created.to_string()),
                highlight("Active Orders", testnet.active_order_count.to_string()),
                highlight("Terminal Orders", testnet.terminal_order_count.to_string()),
                highlight("Unknown Orders", testnet.unknown_order_count.to_string()),
                highlight(
                    "Reconciliation Runs",
                    testnet.reconciliation_run_count.to_string(),
                ),
                highlight("Mismatch Count", testnet.mismatch_count.to_string()),
                highlight("Repair Actions", testnet.repair_action_count.to_string()),
                highlight(
                    "Private Stream Status",
                    testnet.private_stream_status.clone(),
                ),
                highlight(
                    "Private Stream Last Event Age Seconds",
                    testnet
                        .private_stream_last_event_age_seconds
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                ),
            ],
            testnet,
        )?,
    ]);

    Ok(sections)
}

fn build_summary(
    findings: &[OperatorReportFinding],
    kill_switch_active: bool,
    stale_feed_count: i64,
    risk_rejection_rate_pct: Decimal,
    paper_daily_pnl: Decimal,
    shadow_would_submit_count: i64,
    reconciliation_required_count: i64,
) -> OperatorReportSummary {
    OperatorReportSummary {
        total_findings: findings.len(),
        critical_findings: findings
            .iter()
            .filter(|finding| finding.severity == OperatorReportSeverity::Critical)
            .count(),
        high_findings: findings
            .iter()
            .filter(|finding| finding.severity == OperatorReportSeverity::High)
            .count(),
        medium_findings: findings
            .iter()
            .filter(|finding| finding.severity == OperatorReportSeverity::Medium)
            .count(),
        low_findings: findings
            .iter()
            .filter(|finding| finding.severity == OperatorReportSeverity::Low)
            .count(),
        info_findings: findings
            .iter()
            .filter(|finding| finding.severity == OperatorReportSeverity::Info)
            .count(),
        highest_severity: findings
            .iter()
            .max_by_key(|finding| finding.severity.sort_weight())
            .map(|finding| finding.severity),
        kill_switch_active,
        stale_feed_count,
        risk_rejection_rate_pct,
        paper_daily_pnl,
        shadow_would_submit_count,
        reconciliation_required_count,
    }
}

fn status_from_findings(findings: &[OperatorReportFinding]) -> OperatorReportStatus {
    if findings
        .iter()
        .any(|finding| finding.severity == OperatorReportSeverity::Critical)
    {
        OperatorReportStatus::Critical
    } else if findings.iter().any(|finding| {
        matches!(
            finding.severity,
            OperatorReportSeverity::High | OperatorReportSeverity::Medium
        )
    }) {
        OperatorReportStatus::Warning
    } else {
        OperatorReportStatus::Ok
    }
}

async fn persist_report(
    pool: &PgPool,
    report: &OperatorReport,
    created_by: Option<Uuid>,
) -> Result<()> {
    query(
        r#"
        INSERT INTO operator_reports (
            id,
            window_start,
            window_end,
            format,
            status,
            payload,
            markdown,
            created_by,
            created_at,
            correlation_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(report.report_id)
    .bind(report.window_start)
    .bind(report.window_end)
    .bind(report.format.as_str())
    .bind(report.status.as_str())
    .bind(serde_json::to_value(report)?)
    .bind(report.markdown.as_deref())
    .bind(created_by)
    .bind(report.generated_at)
    .bind(report.correlation_id)
    .execute(pool)
    .await?;

    Ok(())
}

fn report_from_record(record: OperatorReportRecord) -> Result<OperatorReport> {
    let mut report: OperatorReport = serde_json::from_value(record.payload)?;
    report.persisted = true;
    if report.markdown.is_none() {
        report.markdown = record.markdown;
    }
    Ok(report)
}

fn map_operator_report_list_item(row: sqlx::postgres::PgRow) -> Result<OperatorReportListItem> {
    Ok(OperatorReportListItem {
        report_id: row.get("id"),
        window_start: row.get("window_start"),
        window_end: row.get("window_end"),
        format: row.get::<String, _>("format").parse()?,
        status: parse_operator_report_status(row.get::<String, _>("status").as_str())?,
        created_at: row.get("created_at"),
        created_by: row.get("created_by"),
        correlation_id: row.get("correlation_id"),
    })
}

fn map_operator_report_record(row: sqlx::postgres::PgRow) -> Result<OperatorReportRecord> {
    Ok(OperatorReportRecord {
        payload: row.get("payload"),
        markdown: row.get("markdown"),
    })
}

fn parse_operator_report_status(value: &str) -> Result<OperatorReportStatus> {
    match value.trim().to_ascii_uppercase().as_str() {
        "OK" => Ok(OperatorReportStatus::Ok),
        "WARNING" => Ok(OperatorReportStatus::Warning),
        "CRITICAL" => Ok(OperatorReportStatus::Critical),
        other => anyhow::bail!("unsupported operator report status: {other}"),
    }
}

fn finding(
    code: &str,
    severity: OperatorReportSeverity,
    title: &str,
    detail: &str,
    section: &str,
) -> OperatorReportFinding {
    OperatorReportFinding {
        code: code.to_string(),
        severity,
        title: title.to_string(),
        detail: detail.to_string(),
        section: section.to_string(),
    }
}

fn recommendation(
    code: &str,
    priority: OperatorReportSeverity,
    detail: &str,
    related_finding_codes: &[&str],
) -> OperatorReportRecommendation {
    OperatorReportRecommendation {
        code: code.to_string(),
        priority,
        detail: detail.to_string(),
        related_finding_codes: related_finding_codes
            .iter()
            .map(|value| value.to_string())
            .collect(),
    }
}

fn section<T: Serialize>(
    key: &str,
    title: &str,
    status: OperatorReportStatus,
    summary: &str,
    highlights: Vec<OperatorReportHighlight>,
    snapshot: &T,
) -> Result<OperatorReportSection> {
    Ok(OperatorReportSection {
        key: key.to_string(),
        title: title.to_string(),
        status,
        summary: summary.to_string(),
        highlights,
        snapshot: serde_json::to_value(snapshot)?,
    })
}

fn highlight(label: &str, value: impl Into<String>) -> OperatorReportHighlight {
    OperatorReportHighlight {
        label: label.to_string(),
        value: value.into(),
    }
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_findings, build_recommendations, status_from_findings, BacktestActivity,
        StrategyBehaviorData, SystemAndMarketData,
    };
    use aegis_core::{
        OperatorReportMarketSnapshot, OperatorReportPaperSnapshot, OperatorReportPromotionSnapshot,
        OperatorReportReasonCount, OperatorReportResearchQualificationSnapshot,
        OperatorReportRiskSnapshot, OperatorReportSeverity, OperatorReportShadowSnapshot,
        OperatorReportStatus, OperatorReportSystemSnapshot, OperatorReportTestnetSnapshot,
    };
    use rust_decimal::Decimal;

    fn base_system_market() -> SystemAndMarketData {
        SystemAndMarketData {
            system: OperatorReportSystemSnapshot {
                api_healthy: true,
                db_healthy: true,
                kill_switch_active: false,
                auth_enabled: true,
                metrics_available: true,
                uptime_seconds: 60,
            },
            market: OperatorReportMarketSnapshot {
                feeds: Vec::new(),
                stale_feed_count: 0,
                degraded_feed_count: 0,
                backfill_completed_count: 0,
                backfill_failed_count: 0,
                candle_count_in_window: 0,
                data_quality: None,
                repair_completed_count: 0,
                repair_failed_count: 0,
                repair_partial_count: 0,
                repair_degraded_after_count: 0,
            },
            stale_threshold_seconds: 10,
        }
    }

    fn base_strategy() -> StrategyBehaviorData {
        StrategyBehaviorData {
            total_strategy_evaluations: 0,
            total_signals: 0,
            risk_rejection_rate_pct: Decimal::ZERO,
            strategy_analytics_summary: None,
            top_rejected_pairs: Vec::new(),
            enabled_strategy_count: 1,
        }
    }

    fn base_risk() -> OperatorReportRiskSnapshot {
        OperatorReportRiskSnapshot {
            approved_decisions: 0,
            rejected_decisions: 0,
            top_rejection_reasons: vec![OperatorReportReasonCount {
                reason: "kill_switch_active".to_string(),
                count: 1,
            }],
            kill_switch_change_count: 0,
            risk_config_version: Some(1),
        }
    }

    fn base_paper() -> OperatorReportPaperSnapshot {
        OperatorReportPaperSnapshot {
            paper_equity: Decimal::from(1000),
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            daily_pnl: Decimal::ZERO,
            open_position_count: 0,
            closed_position_count: 0,
            manual_close_count: 0,
        }
    }

    fn base_shadow() -> OperatorReportShadowSnapshot {
        OperatorReportShadowSnapshot {
            shadow_run_count: 0,
            would_submit_count: 1,
            no_signal_count: 0,
            risk_rejected_count: 0,
            skipped_count: 0,
            runner_status: "RUNNING".to_string(),
            runner_last_tick_age_seconds: Some(1),
        }
    }

    fn base_promotion() -> OperatorReportPromotionSnapshot {
        OperatorReportPromotionSnapshot {
            shadow_would_submit_count: 1,
            previewed_count: 1,
            submitted_count: 1,
            acked_count: 1,
            filled_count: 0,
            reconciliation_required_count: 0,
            preview_rate_pct: Decimal::from(100),
            submit_rate_pct: Decimal::from(100),
            ack_rate_pct: Decimal::from(100),
            fill_rate_pct: Decimal::ZERO,
        }
    }

    fn base_testnet() -> OperatorReportTestnetSnapshot {
        OperatorReportTestnetSnapshot {
            testnet_orders_created: 0,
            active_order_count: 0,
            terminal_order_count: 0,
            unknown_order_count: 0,
            reconciliation_run_count: 0,
            mismatch_count: 0,
            repair_action_count: 0,
            private_stream_status: "CONNECTED".to_string(),
            private_stream_last_event_age_seconds: Some(1),
        }
    }

    fn base_research_qualification() -> OperatorReportResearchQualificationSnapshot {
        OperatorReportResearchQualificationSnapshot {
            total_candidates: 0,
            accepted_for_shadow_count: 0,
            qualified_count: 0,
            needs_more_data_count: 0,
            not_qualified_count: 0,
            degraded_count: 0,
            unknown_count: 0,
            stale_observation_count: 0,
            runner_mismatch_count: 0,
            readiness_degraded_count: 0,
            readiness_not_ready_count: 0,
            degraded_or_not_ready_readiness_count: 0,
            below_default_threshold_override_count: 0,
            latest_evaluated_candidates_count: 0,
            newly_qualified_count: 0,
            lost_qualification_count: 0,
            needs_attention_count: 0,
            stale_evaluation_count: 0,
            reviews_in_window: 0,
            ready_for_testnet_review_count: 0,
            ready_for_testnet_review_dossier_count: 0,
            blocked_testnet_review_dossier_count: 0,
            marked_ready_but_dossier_not_ready_count: 0,
            dossier_needs_more_shadow_data_count: 0,
            rejected_from_watchlist_count: 0,
            archived_from_watchlist_count: 0,
            candidates_needing_review_count: 0,
            walk_forward_overfit_risk_count: 0,
            walk_forward_missing_count: 0,
            walk_forward_robust_count: 0,
            top_candidate: None,
        }
    }

    #[test]
    fn finding_severity_is_critical_for_kill_switch() {
        let mut system_market = base_system_market();
        system_market.system.kill_switch_active = true;

        let findings = build_findings(
            &system_market,
            &base_strategy(),
            &base_risk(),
            &base_paper(),
            &base_shadow(),
            &base_promotion(),
            &base_testnet(),
            &base_research_qualification(),
            None,
            &BacktestActivity { run_count: 1 },
        );

        assert!(findings.iter().any(|finding| {
            finding.code == "kill_switch_active"
                && finding.severity == OperatorReportSeverity::Critical
        }));
        assert_eq!(
            status_from_findings(&findings),
            OperatorReportStatus::Critical
        );
    }

    #[test]
    fn finding_is_high_for_stale_private_stream() {
        let mut testnet = base_testnet();
        testnet.private_stream_status = "STALE".to_string();
        testnet.private_stream_last_event_age_seconds = Some(30);

        let findings = build_findings(
            &base_system_market(),
            &base_strategy(),
            &base_risk(),
            &base_paper(),
            &base_shadow(),
            &base_promotion(),
            &testnet,
            &base_research_qualification(),
            None,
            &BacktestActivity { run_count: 1 },
        );

        assert!(findings.iter().any(|finding| {
            finding.code == "private_stream_stale"
                && finding.severity == OperatorReportSeverity::High
        }));
    }

    #[test]
    fn finding_is_high_for_reconciliation_required_count() {
        let mut promotion = base_promotion();
        promotion.reconciliation_required_count = 1;

        let findings = build_findings(
            &base_system_market(),
            &base_strategy(),
            &base_risk(),
            &base_paper(),
            &base_shadow(),
            &promotion,
            &base_testnet(),
            &base_research_qualification(),
            None,
            &BacktestActivity { run_count: 1 },
        );

        assert!(findings.iter().any(|finding| {
            finding.code == "reconciliation_required_present"
                && finding.severity == OperatorReportSeverity::High
        }));
    }

    #[test]
    fn finding_is_medium_for_high_risk_rejection_rate() {
        let mut strategy = base_strategy();
        strategy.risk_rejection_rate_pct = Decimal::from(75);

        let findings = build_findings(
            &base_system_market(),
            &strategy,
            &base_risk(),
            &base_paper(),
            &base_shadow(),
            &base_promotion(),
            &base_testnet(),
            &base_research_qualification(),
            None,
            &BacktestActivity { run_count: 1 },
        );

        assert!(findings.iter().any(|finding| {
            finding.code == "high_risk_rejection_rate"
                && finding.severity == OperatorReportSeverity::Medium
        }));
    }

    #[test]
    fn empty_data_report_yields_low_or_info_findings() {
        let findings = build_findings(
            &base_system_market(),
            &base_strategy(),
            &base_risk(),
            &base_paper(),
            &OperatorReportShadowSnapshot {
                shadow_run_count: 0,
                would_submit_count: 0,
                no_signal_count: 0,
                risk_rejected_count: 0,
                skipped_count: 0,
                runner_status: "STOPPED".to_string(),
                runner_last_tick_age_seconds: None,
            },
            &OperatorReportPromotionSnapshot {
                shadow_would_submit_count: 0,
                previewed_count: 0,
                submitted_count: 0,
                acked_count: 0,
                filled_count: 0,
                reconciliation_required_count: 0,
                preview_rate_pct: Decimal::ZERO,
                submit_rate_pct: Decimal::ZERO,
                ack_rate_pct: Decimal::ZERO,
                fill_rate_pct: Decimal::ZERO,
            },
            &OperatorReportTestnetSnapshot {
                testnet_orders_created: 0,
                active_order_count: 0,
                terminal_order_count: 0,
                unknown_order_count: 0,
                reconciliation_run_count: 0,
                mismatch_count: 0,
                repair_action_count: 0,
                private_stream_status: "CONNECTED".to_string(),
                private_stream_last_event_age_seconds: Some(1),
            },
            &base_research_qualification(),
            None,
            &BacktestActivity { run_count: 0 },
        );

        assert!(!findings.is_empty());
        assert!(findings.iter().all(|finding| {
            matches!(
                finding.severity,
                OperatorReportSeverity::Info
                    | OperatorReportSeverity::Low
                    | OperatorReportSeverity::Medium
            )
        }));
        let recommendations = build_recommendations(&findings);
        assert!(recommendations
            .iter()
            .all(|value| value.priority != OperatorReportSeverity::High));
    }

    #[test]
    fn qualification_findings_are_emitted_when_candidates_need_more_shadow_data() {
        let mut research = base_research_qualification();
        research.total_candidates = 2;
        research.accepted_for_shadow_count = 2;
        research.needs_more_data_count = 2;

        let findings = build_findings(
            &base_system_market(),
            &base_strategy(),
            &base_risk(),
            &base_paper(),
            &base_shadow(),
            &base_promotion(),
            &base_testnet(),
            &research,
            None,
            &BacktestActivity { run_count: 1 },
        );

        assert!(findings.iter().any(|finding| {
            finding.code == "accepted_candidates_need_more_shadow_data"
                && finding.severity == OperatorReportSeverity::Medium
        }));
        assert!(findings.iter().any(|finding| {
            finding.code == "no_qualified_candidates"
                && finding.severity == OperatorReportSeverity::Low
        }));
    }

    #[test]
    fn qualification_findings_are_emitted_for_readiness_blockers() {
        let mut research = base_research_qualification();
        research.total_candidates = 1;
        research.not_qualified_count = 1;
        research.readiness_degraded_count = 1;
        research.readiness_not_ready_count = 1;
        research.degraded_or_not_ready_readiness_count = 1;

        let findings = build_findings(
            &base_system_market(),
            &base_strategy(),
            &base_risk(),
            &base_paper(),
            &base_shadow(),
            &base_promotion(),
            &base_testnet(),
            &research,
            None,
            &BacktestActivity { run_count: 1 },
        );

        assert!(findings.iter().any(|finding| {
            finding.code == "candidate_readiness_not_ready_blocker"
                && finding.severity == OperatorReportSeverity::High
        }));
        assert!(findings.iter().any(|finding| {
            finding.code == "candidate_readiness_degraded"
                && finding.severity == OperatorReportSeverity::Medium
        }));
    }
}
