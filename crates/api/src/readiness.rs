use std::collections::BTreeSet;

use aegis_core::{
    score_execution_readiness, AuthenticatedActor, CandleInterval, ExchangePrivateStreamStatus,
    ExecutionReadinessBlockingReason, ExecutionReadinessCheck, ExecutionReadinessCheckSeverity,
    ExecutionReadinessRecommendation, ExecutionReadinessRequest, ExecutionReadinessResult,
    ExecutionReadinessSnapshot, ExecutionReadinessStatus, ExecutionReadinessTarget,
    PaperAccountStatus, StrategyId, StrategyPerformanceMode, StrategyPerformanceRequest, Symbol,
    TestnetExecutionState, TestnetPromotionFunnelRequest, TestnetShadowRunnerStatus, UserRole,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use db::{
    check_health, count_backtest_runs_in_window, count_recent_exchange_testnet_repair_failures,
    execution_readiness_snapshot_from_record, get_default_paper_account,
    get_exchange_private_stream_state, get_execution_readiness_snapshot, get_latest_mark_price,
    get_recent_closed_candles, get_risk_config, get_risk_decision_by_id,
    get_strategy_performance_summary, get_strategy_status, get_system_state,
    get_testnet_promotion_funnel_summary, get_testnet_shadow_promotion_by_id,
    get_testnet_shadow_runner_state, insert_execution_readiness_snapshot,
    list_exchange_reconciliation_runs, list_exchange_testnet_orders,
    list_execution_readiness_snapshots, paper_account_from_record, risk_config_from_record,
    strategy_config_from_record,
};
use rust_decimal::Decimal;
use strategy_engine::required_candle_count;
use telemetry::telemetry;
use uuid::Uuid;

use crate::AppState;

const DEFAULT_WINDOW_HOURS: i64 = 24;
const SNAPSHOT_LIST_DEFAULT_LIMIT: i64 = 20;
const SNAPSHOT_LIST_MAX_LIMIT: i64 = 100;
const RECENT_REPAIR_FAILURE_WINDOW_MINUTES: i64 = 60;
const PRIVATE_STREAM_WARN_AGE_MULTIPLIER_NUM: i64 = 4;
const PRIVATE_STREAM_WARN_AGE_MULTIPLIER_DEN: i64 = 5;

#[derive(Debug, Clone)]
struct ReadinessContext {
    strategy_id: String,
    symbol: String,
    timeframe: CandleInterval,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    now: DateTime<Utc>,
}

pub fn persist_allowed(actor: Option<&AuthenticatedActor>) -> bool {
    matches!(
        actor.map(|value| value.role),
        Some(UserRole::Owner | UserRole::Operator)
    )
}

pub fn bounded_snapshot_list_limit(limit: Option<i64>) -> i64 {
    match limit {
        Some(value) if value > 0 => value.min(SNAPSHOT_LIST_MAX_LIMIT),
        _ => SNAPSHOT_LIST_DEFAULT_LIMIT,
    }
}

pub async fn compute_execution_readiness(
    state: &AppState,
    request: &ExecutionReadinessRequest,
    actor: Option<&AuthenticatedActor>,
) -> Result<ExecutionReadinessResult> {
    let context = resolve_context(state, request).await?;
    let correlation_id = request.correlation_id.unwrap_or_else(Uuid::new_v4);
    let mut checks = Vec::new();
    let mut blockers = Vec::new();
    let mut recommendations = BTreeSet::new();

    let db_healthy = check_health(&state.db_pool).await.is_ok();
    push_check(
        &mut checks,
        &mut blockers,
        &mut recommendations,
        "db_healthy",
        "Database health",
        db_healthy,
        true,
        ExecutionReadinessCheckSeverity::High,
        "Database health query succeeded.",
        "Database health query failed.",
        ExecutionReadinessBlockingReason::DbUnhealthy,
        ExecutionReadinessRecommendation::RestoreDatabaseHealth,
    );

    let system_state = get_system_state(&state.db_pool).await?;
    push_check(
        &mut checks,
        &mut blockers,
        &mut recommendations,
        "kill_switch_inactive",
        "Kill switch inactive",
        !system_state.kill_switch_enabled,
        true,
        ExecutionReadinessCheckSeverity::High,
        "Kill switch is inactive.",
        "Kill switch is active.",
        ExecutionReadinessBlockingReason::KillSwitchActive,
        ExecutionReadinessRecommendation::ResumeFromKillSwitch,
    );

    let risk_record = get_risk_config(&state.db_pool).await?;
    let (risk_config, risk_config_valid) = match risk_record.as_ref() {
        Some(record) => match risk_config_from_record(record) {
            Ok(config) => (Some(config), true),
            Err(_) => (None, false),
        },
        None => (None, false),
    };
    push_check(
        &mut checks,
        &mut blockers,
        &mut recommendations,
        "validated_risk_config",
        "Validated risk config",
        risk_record.is_some(),
        true,
        ExecutionReadinessCheckSeverity::High,
        "Validated risk config exists.",
        "Validated risk config is missing.",
        ExecutionReadinessBlockingReason::MissingValidatedRiskConfig,
        ExecutionReadinessRecommendation::ValidateRiskConfig,
    );
    if matches!(
        request.target,
        ExecutionReadinessTarget::PaperPipeline | ExecutionReadinessTarget::TestnetShadow
    ) {
        push_check(
            &mut checks,
            &mut blockers,
            &mut recommendations,
            "risk_config_valid",
            "Risk config valid",
            risk_config_valid,
            true,
            ExecutionReadinessCheckSeverity::High,
            "Risk config validation passed.",
            "Risk config validation failed.",
            ExecutionReadinessBlockingReason::RiskConfigInvalid,
            ExecutionReadinessRecommendation::ValidateRiskConfig,
        );
    }

    let feed_threshold_seconds = risk_config
        .as_ref()
        .map(|config| i64::from(config.stale_feed_threshold_seconds))
        .unwrap_or(10);
    let feed = db::list_market_feed_statuses(&state.db_pool)
        .await?
        .into_iter()
        .find(|feed| {
            feed.exchange == state.market_config.exchange.as_str() && feed.symbol == context.symbol
        });
    let feed_fresh = feed
        .as_ref()
        .map(|feed| feed.freshness_status == aegis_core::DataFreshnessStatus::Fresh)
        .unwrap_or(false);
    push_check(
        &mut checks,
        &mut blockers,
        &mut recommendations,
        "market_feed_fresh",
        "Market feed fresh",
        feed_fresh,
        true,
        ExecutionReadinessCheckSeverity::High,
        "Market feed is fresh.",
        "Market feed is stale or unavailable.",
        ExecutionReadinessBlockingReason::StaleMarketFeed,
        ExecutionReadinessRecommendation::RefreshMarketFeed,
    );
    if let Some(feed) = &feed {
        if let Some(last_event_at) = feed.last_event_at {
            let age = context
                .now
                .signed_duration_since(last_event_at)
                .num_seconds();
            if age >= (feed_threshold_seconds * 4 / 5) && age < feed_threshold_seconds {
                push_warning(
                    &mut checks,
                    "feed_age_near_threshold",
                    "Feed age near threshold",
                    ExecutionReadinessCheckSeverity::Low,
                    format!(
                        "Feed age is {age}s and approaching the {feed_threshold_seconds}s threshold."
                    ),
                );
            }
        }
    }

    let auth_allowed = !state.auth_config.disabled
        || matches!(request.target, ExecutionReadinessTarget::PaperPipeline)
            && state.config.environment.eq_ignore_ascii_case("development");
    push_check(
        &mut checks,
        &mut blockers,
        &mut recommendations,
        "auth_enabled",
        "Auth enabled",
        auth_allowed,
        !matches!(request.target, ExecutionReadinessTarget::PaperPipeline),
        ExecutionReadinessCheckSeverity::High,
        "Auth is enabled for readiness target.",
        "Auth is disabled for readiness target.",
        ExecutionReadinessBlockingReason::AuthDisabled,
        ExecutionReadinessRecommendation::EnableAuth,
    );
    if state.auth_config.disabled && request.target == ExecutionReadinessTarget::PaperPipeline {
        push_warning(
            &mut checks,
            "auth_disabled_local_dev",
            "Auth disabled in local development",
            ExecutionReadinessCheckSeverity::Low,
            "Auth-disabled local development mode is active.".to_string(),
        );
    }

    let latest_price = get_latest_mark_price(&state.db_pool, &context.symbol).await?;
    let price_recent = latest_price
        .as_ref()
        .map(|tick| {
            context
                .now
                .signed_duration_since(tick.received_at)
                .num_seconds()
                <= feed_threshold_seconds
        })
        .unwrap_or(false);
    push_check(
        &mut checks,
        &mut blockers,
        &mut recommendations,
        "recent_market_price",
        "Recent market price",
        price_recent,
        true,
        ExecutionReadinessCheckSeverity::High,
        "Recent market price is available.",
        "Recent market price is missing or stale.",
        ExecutionReadinessBlockingReason::MissingRecentMarketPrice,
        ExecutionReadinessRecommendation::SeedRecentMarketPrice,
    );

    let strategy_id = context
        .strategy_id
        .parse::<StrategyId>()
        .context("invalid readiness strategy id")?;
    let strategy_status = get_strategy_status(&state.db_pool, strategy_id).await?;
    let strategy_config = strategy_status
        .as_ref()
        .map(|status| strategy_config_from_record(&status.config))
        .transpose()?;

    if matches!(
        request.target,
        ExecutionReadinessTarget::PaperPipeline
            | ExecutionReadinessTarget::TestnetShadow
            | ExecutionReadinessTarget::TestnetPromotion
    ) {
        let strategy_enabled = strategy_config
            .as_ref()
            .map(|config| config.enabled)
            .unwrap_or(false);
        push_check(
            &mut checks,
            &mut blockers,
            &mut recommendations,
            "strategy_enabled",
            "Strategy enabled",
            strategy_enabled,
            true,
            ExecutionReadinessCheckSeverity::High,
            "Strategy is enabled.",
            "Strategy is disabled or unavailable.",
            ExecutionReadinessBlockingReason::StrategyDisabled,
            ExecutionReadinessRecommendation::EnableStrategy,
        );
        let strategy_valid = strategy_config
            .as_ref()
            .map(|config| config.validate().is_ok())
            .unwrap_or(false);
        if request.target == ExecutionReadinessTarget::PaperPipeline {
            push_check(
                &mut checks,
                &mut blockers,
                &mut recommendations,
                "strategy_config_valid",
                "Strategy config valid",
                strategy_valid,
                true,
                ExecutionReadinessCheckSeverity::High,
                "Strategy config validation passed.",
                "Strategy config validation failed.",
                ExecutionReadinessBlockingReason::StrategyConfigInvalid,
                ExecutionReadinessRecommendation::FixStrategyConfig,
            );
        }
    }

    if matches!(
        request.target,
        ExecutionReadinessTarget::PaperPipeline | ExecutionReadinessTarget::TestnetShadow
    ) {
        let required = strategy_config
            .as_ref()
            .map(required_candle_count)
            .unwrap_or(1)
            .max(1) as usize;
        let candles = get_recent_closed_candles(
            &state.db_pool,
            &Symbol::new(&context.symbol)?,
            context.timeframe,
            required as i64,
        )
        .await?;
        push_check(
            &mut checks,
            &mut blockers,
            &mut recommendations,
            "recent_closed_candles",
            "Recent closed candles",
            candles.len() >= required,
            true,
            ExecutionReadinessCheckSeverity::High,
            "Recent closed candles are available.",
            "Recent closed candles are missing.",
            ExecutionReadinessBlockingReason::MissingRecentClosedCandles,
            ExecutionReadinessRecommendation::BackfillClosedCandles,
        );
    }

    match request.target {
        ExecutionReadinessTarget::PaperPipeline => {
            let paper_account = get_default_paper_account(&state.db_pool).await?;
            push_check(
                &mut checks,
                &mut blockers,
                &mut recommendations,
                "paper_account_present",
                "Paper account present",
                paper_account.is_some(),
                true,
                ExecutionReadinessCheckSeverity::High,
                "Default paper account exists.",
                "Default paper account is missing.",
                ExecutionReadinessBlockingReason::PaperAccountMissing,
                ExecutionReadinessRecommendation::CreateOrRepairPaperAccount,
            );
            let paper_healthy = paper_account
                .as_ref()
                .map(paper_account_from_record)
                .transpose()?
                .map(|account| account.status == PaperAccountStatus::Active)
                .unwrap_or(false);
            push_check(
                &mut checks,
                &mut blockers,
                &mut recommendations,
                "paper_account_healthy",
                "Paper account healthy",
                paper_healthy,
                true,
                ExecutionReadinessCheckSeverity::High,
                "Paper account is active.",
                "Paper account is unhealthy.",
                ExecutionReadinessBlockingReason::PaperAccountUnhealthy,
                ExecutionReadinessRecommendation::CreateOrRepairPaperAccount,
            );
            if let Some(account) = paper_account
                .as_ref()
                .map(paper_account_from_record)
                .transpose()?
            {
                if account.realized_pnl + account.unrealized_pnl < Decimal::ZERO {
                    push_warning(
                        &mut checks,
                        "paper_pnl_negative",
                        "Paper PnL negative",
                        ExecutionReadinessCheckSeverity::Medium,
                        "Paper account realized plus unrealized PnL is negative.".to_string(),
                    );
                    recommendations.insert(ExecutionReadinessRecommendation::ReviewPaperPnl);
                }
            }
        }
        ExecutionReadinessTarget::TestnetShadow => {
            let runner_state = get_testnet_shadow_runner_state(&state.db_pool)
                .await?
                .map(|record| record.status.parse::<TestnetShadowRunnerStatus>())
                .transpose()?;
            push_check(
                &mut checks,
                &mut blockers,
                &mut recommendations,
                "shadow_runner_not_error",
                "Shadow runner not error",
                !matches!(runner_state, Some(TestnetShadowRunnerStatus::Error)),
                true,
                ExecutionReadinessCheckSeverity::High,
                "Shadow runner is not in error state.",
                "Shadow runner is in error state.",
                ExecutionReadinessBlockingReason::ShadowRunnerError,
                ExecutionReadinessRecommendation::RestartShadowRunner,
            );
            if matches!(
                runner_state,
                Some(TestnetShadowRunnerStatus::Paused | TestnetShadowRunnerStatus::Stopped)
            ) {
                push_warning(
                    &mut checks,
                    "shadow_runner_paused",
                    "Shadow runner paused or stopped",
                    ExecutionReadinessCheckSeverity::Medium,
                    "Shadow runner is paused or stopped.".to_string(),
                );
                recommendations.insert(ExecutionReadinessRecommendation::VerifyRunnerState);
            }
        }
        ExecutionReadinessTarget::TestnetPromotion => {
            let summary = get_testnet_promotion_funnel_summary(
                &state.db_pool,
                &TestnetPromotionFunnelRequest {
                    strategy_id: Some(context.strategy_id.clone()),
                    symbol: Some(context.symbol.clone()),
                    timeframe: Some(context.timeframe.as_str().to_string()),
                    start_time: Some(context.window_start),
                    end_time: Some(context.window_end),
                    limit: None,
                },
            )
            .await?;
            push_check(
                &mut checks,
                &mut blockers,
                &mut recommendations,
                "shadow_would_submit_count_positive",
                "Shadow would-submit count positive",
                summary.shadow_would_submit_count > 0,
                true,
                ExecutionReadinessCheckSeverity::High,
                "Recent shadow would-submit count is positive.",
                "Recent shadow would-submit count is zero.",
                ExecutionReadinessBlockingReason::ZeroShadowWouldSubmitCount,
                ExecutionReadinessRecommendation::IncreaseShadowCoverage,
            );
            if summary.shadow_would_submit_count > 0 && summary.shadow_would_submit_count < 3 {
                push_warning(
                    &mut checks,
                    "shadow_would_submit_low",
                    "Shadow would-submit count low",
                    ExecutionReadinessCheckSeverity::Medium,
                    "Recent shadow would-submit count is low.".to_string(),
                );
            }
            let rejection_rate = summary.reconciliation_required_rate_pct;
            push_check(
                &mut checks,
                &mut blockers,
                &mut recommendations,
                "promotion_rejection_rate_acceptable",
                "Promotion rejection/expiry rate acceptable",
                rejection_rate <= Decimal::from(50),
                true,
                ExecutionReadinessCheckSeverity::High,
                "Promotion rejection/expiry rate is acceptable.",
                "Promotion rejection/expiry rate is too high.",
                ExecutionReadinessBlockingReason::PromotionFunnelHighRejectionRate,
                ExecutionReadinessRecommendation::ReducePromotionRejections,
            );
            let performance = get_strategy_performance_summary(
                &state.db_pool,
                &StrategyPerformanceRequest {
                    strategy_id: Some(context.strategy_id.clone()),
                    symbol: Some(context.symbol.clone()),
                    timeframe: Some(context.timeframe.as_str().to_string()),
                    mode: StrategyPerformanceMode::Combined,
                    start_time: Some(context.window_start),
                    end_time: Some(context.window_end),
                    limit: None,
                },
            )
            .await?;
            push_check(
                &mut checks,
                &mut blockers,
                &mut recommendations,
                "risk_rejection_rate_acceptable",
                "Risk rejection rate acceptable",
                performance.risk_rejection_rate <= Decimal::from(50),
                true,
                ExecutionReadinessCheckSeverity::High,
                "Risk rejection rate is acceptable.",
                "Risk rejection rate is too high.",
                ExecutionReadinessBlockingReason::HighRiskRejectionRate,
                ExecutionReadinessRecommendation::ReduceRiskRejections,
            );
            if performance.risk_rejection_rate > Decimal::from(25) {
                push_warning(
                    &mut checks,
                    "risk_rejection_rate_high",
                    "Risk rejection rate high",
                    ExecutionReadinessCheckSeverity::Medium,
                    "Risk rejection rate is elevated.".to_string(),
                );
            }
            if summary.submit_rate_pct < Decimal::from(25) {
                push_warning(
                    &mut checks,
                    "promotion_submit_rate_low",
                    "Promotion submit rate low",
                    ExecutionReadinessCheckSeverity::Low,
                    "Promotion submit rate is low.".to_string(),
                );
            }
            push_check(
                &mut checks,
                &mut blockers,
                &mut recommendations,
                "local_price_fresh_for_promotion",
                "Local price fresh",
                price_recent,
                true,
                ExecutionReadinessCheckSeverity::High,
                "Local price is fresh for promotion.",
                "Local price is stale for promotion.",
                ExecutionReadinessBlockingReason::StaleLocalPrice,
                ExecutionReadinessRecommendation::SeedRecentMarketPrice,
            );
        }
        ExecutionReadinessTarget::TestnetSubmit => {
            push_check(
                &mut checks,
                &mut blockers,
                &mut recommendations,
                "testnet_adapter_configured",
                "Testnet adapter configured",
                state.exchange_testnet_status.configured,
                true,
                ExecutionReadinessCheckSeverity::High,
                "Testnet adapter is configured.",
                "Testnet adapter is not configured.",
                ExecutionReadinessBlockingReason::TestnetAdapterNotConfigured,
                ExecutionReadinessRecommendation::ConfigureTestnetAdapter,
            );

            let private_stream = get_exchange_private_stream_state(
                &state.db_pool,
                state.exchange_testnet_status.exchange.as_str(),
                state.exchange_testnet_environment.as_str(),
            )
            .await?;
            let private_connected = private_stream
                .as_ref()
                .map(|record| record.status.parse::<ExchangePrivateStreamStatus>())
                .transpose()?
                .map(|status| status == ExchangePrivateStreamStatus::Connected)
                .unwrap_or(false);
            push_check(
                &mut checks,
                &mut blockers,
                &mut recommendations,
                "private_stream_connected",
                "Private stream connected",
                private_connected,
                true,
                ExecutionReadinessCheckSeverity::High,
                "Private stream is connected.",
                "Private stream is disconnected or unavailable.",
                ExecutionReadinessBlockingReason::PrivateStreamDisconnected,
                ExecutionReadinessRecommendation::ReconnectPrivateStream,
            );
            let stream_recent = private_stream
                .as_ref()
                .and_then(|record| record.last_event_at)
                .map(|last_event_at| {
                    context
                        .now
                        .signed_duration_since(last_event_at)
                        .num_seconds()
                        <= feed_threshold_seconds
                })
                .unwrap_or(false);
            push_check(
                &mut checks,
                &mut blockers,
                &mut recommendations,
                "private_stream_recent",
                "Private stream recent",
                stream_recent,
                true,
                ExecutionReadinessCheckSeverity::High,
                "Private stream has recent events.",
                "Private stream is stale.",
                ExecutionReadinessBlockingReason::PrivateStreamStale,
                ExecutionReadinessRecommendation::ReconnectPrivateStream,
            );
            if let Some(last_event_at) = private_stream
                .as_ref()
                .and_then(|record| record.last_event_at)
            {
                let warn_threshold = (feed_threshold_seconds
                    * PRIVATE_STREAM_WARN_AGE_MULTIPLIER_NUM)
                    / PRIVATE_STREAM_WARN_AGE_MULTIPLIER_DEN;
                if context
                    .now
                    .signed_duration_since(last_event_at)
                    .num_seconds()
                    >= warn_threshold
                    && stream_recent
                {
                    push_warning(
                        &mut checks,
                        "private_stream_quiet",
                        "Private stream quiet",
                        ExecutionReadinessCheckSeverity::Low,
                        "Private stream has not produced events recently.".to_string(),
                    );
                }
            }

            let orders = list_exchange_testnet_orders(&state.db_pool, 500).await?;
            let reconciliation_required_count = orders
                .iter()
                .filter(|order| {
                    order.execution_state == TestnetExecutionState::ReconciliationRequired.as_str()
                })
                .count();
            push_check(
                &mut checks,
                &mut blockers,
                &mut recommendations,
                "no_reconciliation_required_orders",
                "No reconciliation-required orders",
                reconciliation_required_count == 0,
                true,
                ExecutionReadinessCheckSeverity::High,
                "No reconciliation-required orders are present.",
                "Reconciliation-required orders are present.",
                ExecutionReadinessBlockingReason::ReconciliationRequiredOrdersPresent,
                ExecutionReadinessRecommendation::ReconcileTestnetOrders,
            );
            let unknown_exchange_state_count = orders
                .iter()
                .filter(|order| {
                    order.execution_state == TestnetExecutionState::UnknownExchangeState.as_str()
                })
                .count();
            push_check(
                &mut checks,
                &mut blockers,
                &mut recommendations,
                "no_unknown_exchange_state_orders",
                "No unknown exchange-state orders",
                unknown_exchange_state_count == 0,
                true,
                ExecutionReadinessCheckSeverity::High,
                "No unknown exchange-state orders are present.",
                "Unknown exchange-state orders are present.",
                ExecutionReadinessBlockingReason::UnknownExchangeStateOrdersPresent,
                ExecutionReadinessRecommendation::ReconcileTestnetOrders,
            );
            let recent_reconciliation_runs = list_exchange_reconciliation_runs(
                &state.db_pool,
                state.exchange_testnet_environment.as_str(),
                10,
            )
            .await?;
            let unresolved_mismatches = recent_reconciliation_runs.iter().any(|run| {
                run.mismatched_orders > 0
                    || run.unknown_orders > 0
                    || run.status.eq_ignore_ascii_case("FAILED")
            });
            push_check(
                &mut checks,
                &mut blockers,
                &mut recommendations,
                "reconciliation_runs_clean",
                "Reconciliation runs clean",
                !unresolved_mismatches,
                true,
                ExecutionReadinessCheckSeverity::High,
                "Recent reconciliation runs are clean.",
                "Recent reconciliation runs contain unresolved mismatches.",
                ExecutionReadinessBlockingReason::UnresolvedReconciliationMismatches,
                ExecutionReadinessRecommendation::ReconcileTestnetOrders,
            );
            let repair_failures = count_recent_exchange_testnet_repair_failures(
                &state.db_pool,
                context.now - Duration::minutes(RECENT_REPAIR_FAILURE_WINDOW_MINUTES),
            )
            .await?;
            push_check(
                &mut checks,
                &mut blockers,
                &mut recommendations,
                "no_recent_repair_failures",
                "No recent repair failures",
                repair_failures == 0,
                true,
                ExecutionReadinessCheckSeverity::High,
                "No recent repair failures were observed.",
                "Recent repair failures were observed.",
                ExecutionReadinessBlockingReason::RecentRepairFailures,
                ExecutionReadinessRecommendation::ResolveRepairFailures,
            );
            if let Some(promotion_id) = request.promotion_id {
                let promotion =
                    get_testnet_shadow_promotion_by_id(&state.db_pool, promotion_id).await?;
                let previewed = promotion
                    .as_ref()
                    .map(|record| {
                        record.status.eq_ignore_ascii_case("PREVIEWED")
                            || record.status.eq_ignore_ascii_case("SUBMITTED")
                    })
                    .unwrap_or(false);
                push_check(
                    &mut checks,
                    &mut blockers,
                    &mut recommendations,
                    "promotion_previewed",
                    "Promotion previewed",
                    previewed,
                    true,
                    ExecutionReadinessCheckSeverity::High,
                    "Promotion has been previewed.",
                    "Promotion has not been previewed.",
                    ExecutionReadinessBlockingReason::PromotionNotPreviewed,
                    ExecutionReadinessRecommendation::PreviewOrRenewPromotion,
                );
                let promotion_not_expired = promotion
                    .as_ref()
                    .map(|record| record.expires_at >= context.now)
                    .unwrap_or(false);
                push_check(
                    &mut checks,
                    &mut blockers,
                    &mut recommendations,
                    "promotion_not_expired",
                    "Promotion not expired",
                    promotion_not_expired,
                    true,
                    ExecutionReadinessCheckSeverity::High,
                    "Promotion is still valid.",
                    "Promotion has expired.",
                    ExecutionReadinessBlockingReason::PromotionExpired,
                    ExecutionReadinessRecommendation::PreviewOrRenewPromotion,
                );
            }
            let risk_approved = match request.risk_decision_id {
                Some(risk_decision_id) => get_risk_decision_by_id(&state.db_pool, risk_decision_id)
                    .await?
                    .map(|record| record.decision.eq_ignore_ascii_case("approved"))
                    .unwrap_or(false),
                None => false,
            };
            push_check(
                &mut checks,
                &mut blockers,
                &mut recommendations,
                "approved_risk_decision_present",
                "Approved risk decision present",
                risk_approved,
                true,
                ExecutionReadinessCheckSeverity::High,
                "Approved risk decision is present.",
                "Approved risk decision is missing.",
                ExecutionReadinessBlockingReason::MissingApprovedRiskDecision,
                ExecutionReadinessRecommendation::ApproveRiskDecision,
            );
            push_check(
                &mut checks,
                &mut blockers,
                &mut recommendations,
                "owner_actor_present",
                "Owner actor present",
                matches!(actor.map(|value| value.role), Some(UserRole::Owner)),
                true,
                ExecutionReadinessCheckSeverity::High,
                "Owner actor is present.",
                "Owner actor is required for submit readiness.",
                ExecutionReadinessBlockingReason::NonOwnerActor,
                ExecutionReadinessRecommendation::UseOwnerActor,
            );
        }
    }

    let backtest_count = count_backtest_runs_in_window(
        &state.db_pool,
        Some(&context.strategy_id),
        Some(&context.symbol),
        Some(context.timeframe.as_str()),
        context.window_start,
        context.window_end,
    )
    .await?;
    if backtest_count == 0 {
        push_warning(
            &mut checks,
            "no_recent_backtest",
            "No recent backtest",
            ExecutionReadinessCheckSeverity::Low,
            "No recent backtest run was found in the readiness window.".to_string(),
        );
        recommendations.insert(ExecutionReadinessRecommendation::RunRecentBacktest);
    }

    let has_unknown = matches!(
        request.target,
        ExecutionReadinessTarget::PaperPipeline
            | ExecutionReadinessTarget::TestnetShadow
            | ExecutionReadinessTarget::TestnetPromotion
            | ExecutionReadinessTarget::TestnetSubmit
    ) && request.symbol.is_none()
        && request.strategy_id.is_none();
    if has_unknown {
        checks.push(ExecutionReadinessCheck {
            code: "minimal_context_missing".to_string(),
            name: "Minimal context missing".to_string(),
            passed: false,
            blocking: true,
            severity: ExecutionReadinessCheckSeverity::Critical,
            summary: "Symbol and strategy filters were both omitted; target-specific checks are incomplete."
                .to_string(),
            details: None,
        });
    }

    let mut score = score_execution_readiness(&checks);
    if !blockers.is_empty() {
        score = score.min(40);
    }
    let warnings = checks
        .iter()
        .filter(|check| !check.passed && !check.blocking)
        .cloned()
        .collect::<Vec<_>>();
    let status = if checks.iter().any(|check| {
        !check.passed
            && check.blocking
            && check.severity == ExecutionReadinessCheckSeverity::Critical
    }) {
        ExecutionReadinessStatus::Unknown
    } else if !blockers.is_empty() || score < 60 {
        ExecutionReadinessStatus::NotReady
    } else if score >= 85 {
        ExecutionReadinessStatus::Ready
    } else {
        ExecutionReadinessStatus::Degraded
    };

    telemetry().inc_execution_readiness_check(request.target.as_str(), status.as_str());
    telemetry().set_execution_readiness_score(request.target.as_str(), f64::from(score));
    for reason in &blockers {
        telemetry()
            .inc_execution_readiness_blocker(request.target.as_str(), blocker_label(*reason));
    }

    let result = ExecutionReadinessResult {
        readiness_id: Uuid::new_v4(),
        target: request.target,
        status,
        score,
        blocking_reasons: blockers.clone(),
        warnings,
        checks: checks.clone(),
        recommendations: recommendations.into_iter().collect(),
        computed_at: context.now,
        correlation_id,
    };

    if request.persist {
        let snapshot = ExecutionReadinessSnapshot {
            id: result.readiness_id,
            target: result.target,
            status: result.status,
            score: result.score,
            blocking_reasons: result.blocking_reasons.clone(),
            warnings: result.warnings.clone(),
            checks: result.checks.clone(),
            recommendations: result.recommendations.clone(),
            created_by: actor.map(|value| value.user_id),
            created_at: result.computed_at,
            correlation_id: Some(result.correlation_id),
        };
        insert_execution_readiness_snapshot(&state.db_pool, &snapshot).await?;
    }

    Ok(result)
}

pub async fn list_snapshots(
    state: &AppState,
    limit: i64,
) -> Result<Vec<ExecutionReadinessSnapshot>> {
    list_execution_readiness_snapshots(&state.db_pool, limit)
        .await?
        .iter()
        .map(execution_readiness_snapshot_from_record)
        .collect()
}

pub async fn get_snapshot(
    state: &AppState,
    id: Uuid,
) -> Result<Option<ExecutionReadinessSnapshot>> {
    get_execution_readiness_snapshot(&state.db_pool, id)
        .await?
        .as_ref()
        .map(execution_readiness_snapshot_from_record)
        .transpose()
}

async fn resolve_context(
    state: &AppState,
    request: &ExecutionReadinessRequest,
) -> Result<ReadinessContext> {
    let now = Utc::now();
    let strategy_id = request
        .strategy_id
        .clone()
        .unwrap_or_else(|| "momentum_v1".to_string());
    let strategy_config = match strategy_id.parse::<StrategyId>() {
        Ok(strategy_id) => get_strategy_status(&state.db_pool, strategy_id)
            .await?
            .as_ref()
            .map(|status| strategy_config_from_record(&status.config))
            .transpose()?,
        Err(_) => None,
    };
    let symbol = request
        .symbol
        .clone()
        .or_else(|| {
            strategy_config
                .as_ref()
                .and_then(|config| config.symbols.first())
                .map(|symbol| symbol.to_string())
        })
        .or_else(|| {
            state
                .strategy_runtime
                .default_symbols
                .first()
                .map(|symbol| symbol.to_string())
        })
        .unwrap_or_else(|| "BTCUSDT".to_string());
    let timeframe = request
        .timeframe
        .clone()
        .or_else(|| {
            strategy_config
                .as_ref()
                .map(|config| config.timeframe.as_str().to_string())
        })
        .unwrap_or_else(|| {
            state
                .strategy_runtime
                .default_timeframe
                .as_str()
                .to_string()
        })
        .parse::<CandleInterval>()?;
    let window_end = request.end_time.unwrap_or(now);
    let window_start = request
        .start_time
        .unwrap_or_else(|| window_end - Duration::hours(DEFAULT_WINDOW_HOURS));

    Ok(ReadinessContext {
        strategy_id,
        symbol,
        timeframe,
        window_start,
        window_end,
        now,
    })
}

fn push_check(
    checks: &mut Vec<ExecutionReadinessCheck>,
    blockers: &mut Vec<ExecutionReadinessBlockingReason>,
    recommendations: &mut BTreeSet<ExecutionReadinessRecommendation>,
    code: &str,
    name: &str,
    passed: bool,
    blocking: bool,
    severity: ExecutionReadinessCheckSeverity,
    pass_summary: &str,
    fail_summary: &str,
    blocker: ExecutionReadinessBlockingReason,
    recommendation: ExecutionReadinessRecommendation,
) {
    if !passed {
        recommendations.insert(recommendation);
        if blocking {
            blockers.push(blocker);
        }
    }
    checks.push(ExecutionReadinessCheck {
        code: code.to_string(),
        name: name.to_string(),
        passed,
        blocking,
        severity,
        summary: if passed { pass_summary } else { fail_summary }.to_string(),
        details: None,
    });
}

fn push_warning(
    checks: &mut Vec<ExecutionReadinessCheck>,
    code: &str,
    name: &str,
    severity: ExecutionReadinessCheckSeverity,
    summary: String,
) {
    checks.push(ExecutionReadinessCheck {
        code: code.to_string(),
        name: name.to_string(),
        passed: false,
        blocking: false,
        severity,
        summary,
        details: None,
    });
}

fn blocker_label(reason: ExecutionReadinessBlockingReason) -> &'static str {
    match reason {
        ExecutionReadinessBlockingReason::DbUnhealthy => "db_unhealthy",
        ExecutionReadinessBlockingReason::KillSwitchActive => "kill_switch_active",
        ExecutionReadinessBlockingReason::MissingValidatedRiskConfig => {
            "missing_validated_risk_config"
        }
        ExecutionReadinessBlockingReason::StaleMarketFeed => "stale_market_feed",
        ExecutionReadinessBlockingReason::AuthDisabled => "auth_disabled",
        ExecutionReadinessBlockingReason::MissingRecentMarketPrice => "missing_recent_market_price",
        ExecutionReadinessBlockingReason::StrategyDisabled => "strategy_disabled",
        ExecutionReadinessBlockingReason::StrategyConfigInvalid => "strategy_config_invalid",
        ExecutionReadinessBlockingReason::MissingRecentClosedCandles => {
            "missing_recent_closed_candles"
        }
        ExecutionReadinessBlockingReason::RiskConfigInvalid => "risk_config_invalid",
        ExecutionReadinessBlockingReason::PaperAccountMissing => "paper_account_missing",
        ExecutionReadinessBlockingReason::PaperAccountUnhealthy => "paper_account_unhealthy",
        ExecutionReadinessBlockingReason::ShadowRunnerError => "shadow_runner_error",
        ExecutionReadinessBlockingReason::ZeroShadowWouldSubmitCount => {
            "zero_shadow_would_submit_count"
        }
        ExecutionReadinessBlockingReason::PromotionFunnelHighRejectionRate => {
            "promotion_funnel_high_rejection_rate"
        }
        ExecutionReadinessBlockingReason::StaleLocalPrice => "stale_local_price",
        ExecutionReadinessBlockingReason::HighRiskRejectionRate => "high_risk_rejection_rate",
        ExecutionReadinessBlockingReason::TestnetAdapterNotConfigured => {
            "testnet_adapter_not_configured"
        }
        ExecutionReadinessBlockingReason::PrivateStreamStale => "private_stream_stale",
        ExecutionReadinessBlockingReason::PrivateStreamDisconnected => {
            "private_stream_disconnected"
        }
        ExecutionReadinessBlockingReason::UnresolvedReconciliationMismatches => {
            "unresolved_reconciliation_mismatches"
        }
        ExecutionReadinessBlockingReason::ReconciliationRequiredOrdersPresent => {
            "reconciliation_required_orders_present"
        }
        ExecutionReadinessBlockingReason::UnknownExchangeStateOrdersPresent => {
            "unknown_exchange_state_orders_present"
        }
        ExecutionReadinessBlockingReason::RecentRepairFailures => "recent_repair_failures",
        ExecutionReadinessBlockingReason::PromotionExpired => "promotion_expired",
        ExecutionReadinessBlockingReason::PromotionNotPreviewed => "promotion_not_previewed",
        ExecutionReadinessBlockingReason::MissingApprovedRiskDecision => {
            "missing_approved_risk_decision"
        }
        ExecutionReadinessBlockingReason::NonOwnerActor => "non_owner_actor",
    }
}
