use aegis_core::User;
use aegis_core::{
    ExchangeTestnetPipelinePreview, PaperTradingPipelineResult, TestnetShadowPromotionPreview,
    TestnetShadowPromotionResult, TestnetShadowRunResult,
};
use colored::Colorize;
use serde::Serialize;

use crate::api::{
    BacktestResult, BacktestRunAcceptedResponse, CandleBackfillRunResponse,
    CandleBackfillRunsResponse, ExchangePrivateStreamEventRecord,
    ExchangePrivateStreamListenKeyResponse, ExchangePrivateStreamStatusResponse,
    ExchangeReconciliationMismatchRecord, ExchangeReconciliationResult,
    ExchangeReconciliationRunRecord, ExchangeTestnetBalancesResponse, ExchangeTestnetOrderResponse,
    ExchangeTestnetPipelineSubmitResponse, ExchangeTestnetRepairActionRecord,
    ExchangeTestnetRepairResponse, ExchangeTestnetStatusResponse, ExchangeTestnetSymbolsResponse,
    ExecutionReadinessResponse, ExecutionReadinessSnapshotsResponse, FeedStatusResponse,
    HealthResponse, OperatorReportResponse, OperatorReportsListResponse, OrderRecord,
    PaperAccountResponse, PaperClosePositionResponse, PaperEquityResponse, PaperPnlResponse,
    PaperPositionRecord, PaperPositionsResponse, PaperTradeJournalResponse, RecentEventsResponse,
    RiskActionResponse, RiskConfigAuditResponse, RiskConfigResponse, RiskConfigValidationResponse,
    RiskConfigVersionsResponse, RiskDecisionsResponse, RiskStatusResponse, StatusResponse,
    StrategyConfigAuditResponse, StrategyConfigValidationResponse, StrategyConfigVersionsResponse,
    StrategyDecisionBreakdownResponse, StrategyDryRunResponse, StrategyListResponse,
    StrategyPerformanceRankingsResponse, StrategyPerformanceSummaryResponse,
    StrategyStatusResponse, TestnetPromotionFunnelOutcomesResponse,
    TestnetPromotionFunnelRowsResponse, TestnetPromotionFunnelSummaryResponse,
    TestnetShadowPromotionsResponse, TestnetShadowRunnerControlResponse,
    TestnetShadowRunnerStatusResponse, TestnetShadowRunsResponse,
};

pub fn print_json<T: Serialize>(value: &T) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

pub fn print_status(
    health: &HealthResponse,
    status: &StatusResponse,
    risk: &RiskStatusResponse,
    feed: &FeedStatusResponse,
) {
    println!(
        "API: {}  Service: {}  Env: {}",
        paint_state(&health.status, health.status.eq_ignore_ascii_case("ok")),
        health.service,
        health.environment
    );
    println!(
        "Mode: {}  Kill switch: {}  Paper allowed: {}  Live allowed: {}",
        status.market_mode,
        if risk.kill_switch.enabled {
            "ACTIVE".red().bold().to_string()
        } else {
            "inactive".green().to_string()
        },
        bool_word(risk.paper_trading_allowed),
        bool_word(risk.live_trading_allowed)
    );
    println!(
        "Dependencies: db={} event_bus={} execution={}",
        status.dependencies.database.status,
        status.dependencies.event_bus.status,
        status.dependencies.exchange_execution.status
    );

    if risk.kill_switch.enabled {
        println!(
            "{} {}",
            "WARNING:".red().bold(),
            risk.kill_switch
                .reason
                .as_deref()
                .unwrap_or("kill switch active")
        );
    }

    let degraded: Vec<_> = feed
        .feeds
        .iter()
        .filter(|item| {
            !item.freshness_status.eq_ignore_ascii_case("fresh")
                || !item.status.eq_ignore_ascii_case("connected")
        })
        .collect();

    println!("Feeds: {}", summarize_feeds(feed));
    if degraded.is_empty() {
        println!("Feed warnings: none");
    } else {
        println!(
            "{} {}",
            "WARNING:".red().bold(),
            degraded
                .iter()
                .map(|item| {
                    format!("{} {} {}", item.symbol, item.status, item.freshness_status)
                })
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
}

pub fn print_auth_login(user: &User) {
    println!("Logged in as {} ({})", user.email, user.role.as_str());
}

pub fn print_auth_me(user: &User) {
    println!("User ID: {}", user.id);
    println!("Email: {}", user.email);
    println!("Role: {}", user.role.as_str());
    println!("Status: {}", user.status.as_str());
}

pub fn print_auth_logout() {
    println!("Logged out.");
}

pub fn print_operator_report(response: &OperatorReportResponse) {
    if let Some(markdown) = response.report.markdown.as_deref() {
        println!("{markdown}");
        return;
    }

    println!("Report ID: {}", response.report.report_id);
    println!("Status: {}", response.report.status.as_str());
    println!(
        "Window: {} -> {}",
        response.report.window_start, response.report.window_end
    );
    println!("Generated At: {}", response.report.generated_at);
    println!("Findings: {}", response.report.findings.len());
    for finding in &response.report.findings {
        println!(
            "- {} {}: {}",
            finding.severity.as_str(),
            finding.section,
            finding.title
        );
    }
}

pub fn print_operator_report_list(response: &OperatorReportsListResponse) {
    for report in &response.reports {
        println!(
            "{}  {}  {} -> {}  created_at={}",
            report.report_id,
            report.status,
            report.window_start,
            report.window_end,
            report.created_at
        );
    }
}

pub fn print_execution_readiness(response: &ExecutionReadinessResponse) {
    let readiness = &response.readiness;
    println!(
        "Readiness: {}  Target: {}  Score: {}",
        readiness.status.as_str(),
        readiness.target.as_str(),
        readiness.score
    );
    println!("Computed: {}", readiness.computed_at);
    println!("ID: {}", readiness.readiness_id);

    if readiness.blocking_reasons.is_empty() {
        println!("Blockers: none");
    } else {
        println!("Blockers:");
        for reason in &readiness.blocking_reasons {
            println!("  - {:?}", reason);
        }
    }

    if readiness.warnings.is_empty() {
        println!("Warnings: none");
    } else {
        println!("Warnings:");
        for warning in &readiness.warnings {
            println!(
                "  - [{}] {}",
                format!("{:?}", warning.severity),
                warning.summary
            );
        }
    }

    if readiness.recommendations.is_empty() {
        println!("Recommendations: none");
    } else {
        println!("Recommendations:");
        for item in &readiness.recommendations {
            println!("  - {}", item.message());
        }
    }
}

pub fn print_execution_readiness_snapshots(response: &ExecutionReadinessSnapshotsResponse) {
    if response.snapshots.is_empty() {
        println!("No readiness snapshots found.");
        return;
    }

    for snapshot in &response.snapshots {
        println!(
            "{}  {}  score={}  {}",
            snapshot.id,
            snapshot.target.as_str(),
            snapshot.score,
            snapshot.status.as_str()
        );
    }
}

pub fn print_exchange_testnet_status(response: &ExchangeTestnetStatusResponse) {
    println!("Exchange: {}", response.exchange);
    println!("Environment: {}", response.environment);
    println!("Configured: {}", response.configured);
    println!("Request mode: {}", response.request_mode);
    println!("REST base URL: {}", response.rest_base_url);
    println!("WS base URL: {}", response.ws_base_url);
}

pub fn print_exchange_private_stream_status(response: &ExchangePrivateStreamStatusResponse) {
    let state = &response.state;
    println!("Exchange: {}", state.exchange);
    println!("Environment: {}", state.environment);
    println!("Status: {}", state.status);
    println!(
        "Listen key hash: {}",
        state.listen_key_hash.as_deref().unwrap_or("-")
    );
    println!(
        "Connected at: {}",
        state
            .connected_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Last event at: {}",
        state
            .last_event_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string())
    );
    println!("Reconnect count: {}", state.reconnect_count);
    println!("Last error: {}", state.last_error.as_deref().unwrap_or("-"));
    println!("Stale: {}", state.is_stale);
}

pub fn print_exchange_private_stream_events(events: &[ExchangePrivateStreamEventRecord]) {
    for event in events {
        println!(
            "{} type={} client_order_id={} status={} received_at={}",
            event.id,
            event.event_type,
            event.client_order_id.as_deref().unwrap_or("-"),
            event.order_status.as_deref().unwrap_or("-"),
            event.received_at
        );
    }
}

pub fn print_exchange_private_stream_listen_key(response: &ExchangePrivateStreamListenKeyResponse) {
    println!("Listen key status: {}", response.listen_key_status);
    println!(
        "Listen key: {}",
        response.listen_key_masked.as_deref().unwrap_or("-")
    );
    print_exchange_private_stream_status(&ExchangePrivateStreamStatusResponse {
        state: response.state.clone(),
        request_id: response.request_id.clone(),
        correlation_id: response.correlation_id.clone(),
        timestamp: response.timestamp,
    });
}

pub fn print_exchange_testnet_symbols(response: &ExchangeTestnetSymbolsResponse) {
    for symbol in &response.symbols {
        println!(
            "{}  {} / {}  status={}",
            symbol.symbol, symbol.base_asset, symbol.quote_asset, symbol.status
        );
    }
}

pub fn print_exchange_testnet_balances(response: &ExchangeTestnetBalancesResponse) {
    for balance in &response.balances {
        println!(
            "{}  free={} locked={}",
            balance.asset, balance.free, balance.locked
        );
    }
}

pub fn print_exchange_testnet_order(response: &ExchangeTestnetOrderResponse) {
    let order = &response.order;
    println!("Client order ID: {}", order.client_order_id);
    println!(
        "Exchange order ID: {}",
        order.exchange_order_id.as_deref().unwrap_or("-")
    );
    println!("Symbol: {}", order.symbol);
    println!("Side: {}", order.side);
    println!("Type: {}", order.order_type);
    println!("Status: {}", order.status);
    println!("Execution state: {}", order.execution_state);
    println!(
        "Requested quantity: {}",
        order.requested_qty.as_deref().unwrap_or("-")
    );
    println!(
        "Requested quote notional: {}",
        order.requested_notional.as_deref().unwrap_or("-")
    );
}

pub fn print_exchange_testnet_pipeline_preview(preview: &ExchangeTestnetPipelinePreview) {
    println!("Risk decision ID: {}", preview.risk_decision_id);
    println!(
        "Signal ID: {}",
        preview
            .signal_id
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Strategy ID: {}",
        preview.strategy_id.as_deref().unwrap_or("-")
    );
    println!("Symbol: {}", preview.symbol);
    println!("Side: {}", preview.side.as_str());
    println!("Order type: {}", preview.order_type.as_str());
    println!("Quantity: {}", preview.quantity);
    println!("Quote notional: {}", preview.quote_notional);
    println!("Reference price: {}", preview.reference_price);
    println!("Confirmation: {}", preview.confirmation_text);
}

pub fn print_exchange_testnet_pipeline_submit(response: &ExchangeTestnetPipelineSubmitResponse) {
    print_exchange_testnet_pipeline_preview(&response.preview);
    println!();
    println!("Submitted order:");
    println!("Client order ID: {}", response.order.client_order_id);
    println!(
        "Exchange order ID: {}",
        response.order.exchange_order_id.as_deref().unwrap_or("-")
    );
    println!("Status: {}", response.order.status);
    println!("Execution state: {}", response.order.execution_state);
}

pub fn print_testnet_shadow_run(run: &TestnetShadowRunResult) {
    println!("Run ID: {}", run.run_id);
    println!("Strategy ID: {}", run.strategy_id);
    println!("Symbol: {}", run.symbol);
    println!("Timeframe: {}", run.timeframe);
    println!("Decision: {}", run.decision.as_str());
    println!(
        "Signal ID: {}",
        run.signal_id
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Risk decision ID: {}",
        run.risk_decision_id
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Resolved price: {}",
        run.resolved_price
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Price source: {}",
        run.price_source.as_deref().unwrap_or("-")
    );
    if let Some(intent) = &run.would_submit_order {
        println!(
            "Would submit: {} {} type={} quote_notional={} quantity={}",
            intent.symbol,
            intent.side.as_str(),
            intent.order_type.as_str(),
            intent
                .quote_notional
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            intent
                .quantity
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        );
    }
    if !run.reasons.is_empty() {
        println!(
            "Reasons: {}",
            run.reasons
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    println!("Correlation ID: {}", run.correlation_id);
}

pub fn print_testnet_shadow_runs(response: &TestnetShadowRunsResponse) {
    for run in &response.runs {
        println!(
            "{} {} {} {} signal={} risk={} price={}",
            run.created_at,
            run.strategy_id,
            run.symbol,
            run.decision.as_str(),
            run.signal_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            run.risk_decision_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            run.resolved_price
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        );
    }
}

pub fn print_testnet_shadow_promotion(promotion: &TestnetShadowPromotionPreview) {
    println!("Promotion ID: {}", promotion.promotion_id);
    println!("Shadow Run ID: {}", promotion.shadow_run_id);
    println!("Status: {}", promotion.status.as_str());
    println!("Strategy: {}", promotion.strategy_id);
    println!("Symbol: {}", promotion.symbol);
    println!("Timeframe: {}", promotion.timeframe);
    println!(
        "Signal ID: {}",
        promotion
            .signal_id
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!("Risk Decision ID: {}", promotion.risk_decision_id);
    println!(
        "Resolved Price: {}",
        promotion
            .resolved_price
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Price Source: {}",
        promotion.price_source.as_deref().unwrap_or("-")
    );
    println!("Expires At: {}", promotion.expires_at);
    println!(
        "Client Order ID: {}",
        promotion.client_order_id.as_deref().unwrap_or("-")
    );
    println!(
        "Reasons: {}",
        promotion
            .reasons
            .iter()
            .map(|value| value.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("Correlation ID: {}", promotion.correlation_id);
}

pub fn print_testnet_shadow_promotions(response: &TestnetShadowPromotionsResponse) {
    for promotion in &response.promotions {
        println!(
            "{} {} {} {} expires={} client_order_id={}",
            promotion.created_at,
            promotion.shadow_run_id,
            promotion.symbol,
            promotion.status.as_str(),
            promotion.expires_at,
            promotion.client_order_id.as_deref().unwrap_or("-")
        );
    }
}

pub fn print_testnet_shadow_promotion_submit(result: &TestnetShadowPromotionResult) {
    println!("Promotion ID: {}", result.promotion_id);
    println!("Shadow Run ID: {}", result.shadow_run_id);
    println!("Testnet Order ID: {}", result.testnet_order_id);
    println!("Client Order ID: {}", result.client_order_id);
    println!("Execution State: {}", result.execution_state.as_str());
    println!("Correlation ID: {}", result.correlation_id);
}

pub fn print_testnet_shadow_runner_status(response: &TestnetShadowRunnerStatusResponse) {
    println!(
        "Status: {}  Enabled: {}  Interval: {}s  Tick total: {}  Run total: {}",
        response.state.status.as_str(),
        response.config.enabled,
        response.config.interval_seconds,
        response.state.total_ticks,
        response.state.total_runs
    );
    println!(
        "Last tick: {}  Last success: {}",
        response
            .state
            .last_tick_at
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        response
            .state
            .last_success_at
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Strategies: {}  Symbols: {}  Timeframe: {}  Max runs/tick: {}",
        response.config.strategies.join(","),
        response.config.symbols.join(","),
        response.config.timeframe,
        response.config.max_runs_per_tick
    );
    println!(
        "Stale feed policy: {}  Last error: {}",
        response.config.stale_feed_policy.as_str(),
        response.state.last_error.as_deref().unwrap_or("-")
    );
}

pub fn print_testnet_shadow_runner_config(config: &aegis_core::TestnetShadowRunnerConfig) {
    println!("Enabled: {}", config.enabled);
    println!("Interval seconds: {}", config.interval_seconds);
    println!("Strategies: {}", config.strategies.join(","));
    println!("Symbols: {}", config.symbols.join(","));
    println!("Timeframe: {}", config.timeframe);
    println!("Max runs per tick: {}", config.max_runs_per_tick);
    println!("Stale feed policy: {}", config.stale_feed_policy.as_str());
    println!("Notes: {}", config.notes.as_deref().unwrap_or("-"));
    println!(
        "Updated by: {}  Updated at: {}",
        config
            .updated_by
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        config.updated_at
    );
}

pub fn print_testnet_shadow_runner_control(response: &TestnetShadowRunnerControlResponse) {
    println!("Status: {}", response.state.status.as_str());
    println!("Total ticks: {}", response.state.total_ticks);
    println!("Total runs: {}", response.state.total_runs);
    println!(
        "Last error: {}",
        response.state.last_error.as_deref().unwrap_or("-")
    );
    if let Some(tick) = &response.tick {
        println!(
            "Tick: {} attempted={} completed={} failed={} correlation={}",
            tick.status.as_str(),
            tick.attempted_runs,
            tick.completed_runs,
            tick.failed_runs,
            tick.correlation_id
        );
        if let Some(message) = &tick.message {
            println!("Tick message: {}", message);
        }
    }
}

pub fn print_exchange_testnet_order_lifecycle(
    response: &crate::api::ExchangeTestnetOrderLifecycleResponse,
) {
    println!("Client order ID: {}", response.client_order_id);
    println!("Current state: {}", response.current_state);
    for event in &response.events {
        println!(
            "{} {} -> {} source={} reason={}",
            event.created_at,
            event.previous_state.as_deref().unwrap_or("-"),
            event.next_state,
            event.transition_source,
            event.reason.as_deref().unwrap_or("-")
        );
    }
}

pub fn print_exchange_testnet_repair(response: &ExchangeTestnetRepairResponse) {
    println!("Client order ID: {}", response.client_order_id);
    println!("Action: {}", response.action);
    println!("Status: {}", response.status);
    println!(
        "Previous state: {}",
        response.previous_state.as_deref().unwrap_or("-")
    );
    println!(
        "Next state: {}",
        response.next_state.as_deref().unwrap_or("-")
    );
    println!("Correlation ID: {}", response.correlation_id);
    for issue in &response.issues {
        println!("Issue: {} {}", issue.code, issue.message);
    }
}

pub fn print_exchange_testnet_repairs(repairs: &[ExchangeTestnetRepairActionRecord]) {
    for repair in repairs {
        println!(
            "{} action={} status={} {} -> {} reason={}",
            repair.created_at,
            repair.action,
            repair.status,
            repair.previous_state.as_deref().unwrap_or("-"),
            repair.next_state.as_deref().unwrap_or("-"),
            repair.reason.as_deref().unwrap_or("-")
        );
    }
}

pub fn print_exchange_reconciliation_result(result: &ExchangeReconciliationResult) {
    println!("Run ID: {}", result.run_id);
    println!("Status: {}", result.status);
    println!("Checked orders: {}", result.checked_orders);
    println!("Matched orders: {}", result.matched_orders);
    println!("Mismatched orders: {}", result.mismatched_orders);
    println!("Unknown orders: {}", result.unknown_orders);
    println!("Correlation ID: {}", result.correlation_id);
}

pub fn print_exchange_reconciliation_runs(runs: &[ExchangeReconciliationRunRecord]) {
    for run in runs {
        println!(
            "{} status={} checked={} matched={} mismatched={} unknown={} started_at={}",
            run.id,
            run.status,
            run.checked_orders,
            run.matched_orders,
            run.mismatched_orders,
            run.unknown_orders,
            run.started_at
        );
    }
}

pub fn print_exchange_reconciliation_run(run: &ExchangeReconciliationRunRecord) {
    println!("Run ID: {}", run.id);
    println!("Exchange: {}", run.exchange);
    println!("Environment: {}", run.environment);
    println!("Status: {}", run.status);
    println!("Checked orders: {}", run.checked_orders);
    println!("Matched orders: {}", run.matched_orders);
    println!("Mismatched orders: {}", run.mismatched_orders);
    println!("Unknown orders: {}", run.unknown_orders);
    println!(
        "Failed reason: {}",
        run.failed_reason.as_deref().unwrap_or("-")
    );
    println!("Started at: {}", run.started_at);
    println!(
        "Completed at: {}",
        run.completed_at
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!("Correlation ID: {}", run.correlation_id);
}

pub fn print_exchange_reconciliation_mismatches(
    mismatches: &[ExchangeReconciliationMismatchRecord],
) {
    for mismatch in mismatches {
        println!(
            "{} client_order_id={} local_status={} exchange_status={} kind={} action={}",
            mismatch.id,
            mismatch.client_order_id,
            mismatch.local_status.as_deref().unwrap_or("-"),
            mismatch.exchange_status.as_deref().unwrap_or("-"),
            mismatch.mismatch_kind,
            mismatch.action
        );
    }
}

pub fn print_risk_action(response: &RiskActionResponse) {
    println!("Status: {}", paint_state(&response.status, true));
    println!("Message: {}", response.message);
    println!(
        "Kill switch: {}",
        if response.kill_switch.enabled {
            "ACTIVE".red().bold().to_string()
        } else {
            "inactive".green().to_string()
        }
    );
    println!("Correlation ID: {}", response.correlation_id);
}

pub fn print_risk_config(response: &RiskConfigResponse) {
    let config = &response.config;
    println!("Risk config ID: {}", config.config_id);
    println!("Max open positions: {}", config.max_open_positions);
    println!("Max daily loss %: {}", config.max_daily_loss_pct);
    println!("Max weekly loss %: {}", config.max_weekly_loss_pct);
    println!("Max position notional: {}", config.max_position_notional);
    println!("Max slippage %: {}", config.max_slippage_pct);
    println!("Max consecutive losses: {}", config.max_consecutive_losses);
    println!("Cooldown seconds: {}", config.cooldown_seconds);
    println!("Max signal age ms: {}", config.max_signal_age_ms);
    println!(
        "Stale feed threshold seconds: {}",
        config.stale_feed_threshold_seconds
    );
    println!("Config version: {}", config.config_version);
}

pub fn print_risk_config_validation(response: &RiskConfigValidationResponse) {
    println!("Valid: {}", response.validation.valid);
    for issue in &response.validation.issues {
        println!(
            "{}  {} {} {}",
            issue.severity.as_str(),
            issue.code,
            issue.field,
            issue.message
        );
    }
}

pub fn print_risk_config_versions(response: &RiskConfigVersionsResponse) {
    for version in &response.versions {
        println!(
            "v{}  config_id={} max_open_positions={} max_notional={}",
            version.version,
            version.config_id,
            version.config.max_open_positions,
            version.config.max_position_notional
        );
    }
}

pub fn print_risk_config_audit(response: &RiskConfigAuditResponse) {
    for entry in &response.audit {
        println!(
            "{}  config_id={} version={} issues={}",
            entry.created_at.to_rfc3339(),
            entry.config_id,
            entry
                .version
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            entry.validation_issues.len()
        );
    }
}

pub fn print_pipeline_result(result: &PaperTradingPipelineResult) {
    println!(
        "Pipeline decision: {}",
        if pipeline_decision_label(result) == "PAPER_ORDER_CREATED"
            || pipeline_decision_label(result) == "PAPER_ORDER_REUSED"
        {
            pipeline_decision_label(result).green().bold().to_string()
        } else {
            format!("WARNING: {}", pipeline_decision_label(result))
                .yellow()
                .bold()
                .to_string()
        }
    );
    println!("Signal ID: {}", display_option(result.signal_id));
    println!(
        "Risk decision ID: {}",
        display_option(result.risk_decision_id)
    );
    println!("Paper order ID: {}", display_option(result.paper_order_id));
    println!("Reasons: {}", display_vec(&result.reasons));
    println!("Correlation ID: {}", result.correlation_id);
}

pub fn print_strategy_list(response: &StrategyListResponse) {
    for strategy in &response.strategies {
        println!(
            "{}  enabled={} mode={} timeframe={} symbols={} notional={} lookback={} version={}",
            strategy.strategy_id,
            strategy.enabled,
            strategy.mode,
            strategy.timeframe,
            strategy.symbols.join(","),
            strategy.suggested_notional,
            strategy.lookback_candles,
            strategy.config_version
        );
    }
}

pub fn print_strategy_status(response: &StrategyStatusResponse) {
    let strategy = &response.strategy;
    println!("Strategy ID: {}", strategy.strategy_id);
    println!("Enabled: {}", strategy.enabled);
    println!("Mode: {}", strategy.mode);
    println!("Timeframe: {}", strategy.timeframe);
    println!("Symbols: {}", strategy.symbols.join(", "));
    println!("Suggested notional: {}", strategy.suggested_notional);
    println!("Lookback candles: {}", strategy.lookback_candles);
    println!("Max signal age ms: {}", strategy.max_signal_age_ms);
    println!("Cooldown seconds: {}", strategy.cooldown_seconds);
    println!("Config version: {}", strategy.config_version);
    println!(
        "Last evaluated at: {}",
        strategy
            .last_evaluated_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Last signal ID: {}",
        display_option(strategy.last_signal_id)
    );
}

pub fn print_strategy_config_validation(response: &StrategyConfigValidationResponse) {
    println!("Strategy ID: {}", response.validation.strategy_id);
    println!("Valid: {}", response.validation.valid);
    for issue in &response.validation.issues {
        println!(
            "{}  {} {} {}",
            issue.severity.as_str(),
            issue.code,
            issue.field,
            issue.message
        );
    }
}

pub fn print_strategy_config_versions(response: &StrategyConfigVersionsResponse) {
    for version in &response.versions {
        println!(
            "v{}  strategy={} mode={} enabled={} timeframe={} symbols={}",
            version.version,
            version.strategy_id,
            version.config.mode.as_str(),
            version.config.enabled,
            version.config.timeframe.as_str(),
            version
                .config
                .symbols
                .iter()
                .map(|symbol| symbol.as_str())
                .collect::<Vec<_>>()
                .join(",")
        );
    }
}

pub fn print_strategy_config_audit(response: &StrategyConfigAuditResponse) {
    for entry in &response.audit {
        println!(
            "{}  strategy={} version={} issues={}",
            entry.created_at.to_rfc3339(),
            entry.strategy_id,
            entry
                .version
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            entry.validation_issues.len()
        );
    }
}

pub fn print_strategy_dry_run(response: &StrategyDryRunResponse) {
    let result = &response.result;
    println!("Strategy ID: {}", result.strategy_id);
    println!("Symbol: {}", result.symbol);
    println!("Timeframe: {}", result.timeframe);
    println!("Config valid: {}", result.config_valid);
    println!("Would generate signal: {}", result.would_generate_signal);
    println!("Reason: {}", result.reason);
    println!(
        "Confidence: {}",
        result
            .confidence
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
}

pub fn print_orders(orders: &[OrderRecord]) {
    for order in orders {
        println!(
            "{}  {} {} qty={} status={} exec={} strategy={} signal={}",
            order.order_id,
            order.symbol,
            order.side,
            order.quantity,
            paint_order_status(&order.status),
            order.execution_state,
            order.strategy_id.as_deref().unwrap_or("-"),
            display_option(order.signal_id)
        );
    }
}

pub fn print_order_detail(order: &OrderRecord) {
    println!("Order ID: {}", order.order_id);
    println!("Client order ID: {}", order.client_order_id);
    println!(
        "Strategy ID: {}",
        order.strategy_id.as_deref().unwrap_or("-")
    );
    println!("Signal ID: {}", display_option(order.signal_id));
    println!("Risk decision ID: {}", order.risk_decision_id);
    println!("Symbol: {}", order.symbol);
    println!("Side: {}", order.side);
    println!("Status: {}", paint_order_status(&order.status));
    println!("Execution state: {}", order.execution_state);
    println!(
        "Requested notional: {}",
        order.requested_notional.as_deref().unwrap_or("-")
    );
    println!("Quantity: {}", order.quantity);
    println!("Filled quantity: {}", order.filled_qty);
    println!(
        "Filled price: {}",
        order.filled_price.as_deref().unwrap_or("-")
    );
    println!(
        "Average fill price: {}",
        order.avg_fill_price.as_deref().unwrap_or("-")
    );
    println!(
        "Status reason: {}",
        order.status_reason.as_deref().unwrap_or("-")
    );
    println!("Correlation ID: {}", order.correlation_id);
}

pub fn print_paper_account(response: &PaperAccountResponse) {
    let account = &response.account;
    println!("Account: {} ({})", account.name, account.id);
    println!("Base currency: {}", account.base_currency);
    println!("Initial equity: {}", account.initial_equity);
    println!("Current equity: {}", account.current_equity);
    println!("Realized PnL: {}", account.realized_pnl);
    println!("Unrealized PnL: {}", account.unrealized_pnl);
    println!("Status: {}", account.status);
}

pub fn print_paper_positions(response: &PaperPositionsResponse) {
    for position in &response.positions {
        print_paper_position(position);
    }
}

pub fn print_paper_position(position: &PaperPositionRecord) {
    println!(
        "{} {} qty={} entry={} mark={} unrealized={} realized={} status={} strategy={} signal={}",
        position.symbol,
        position.side,
        position.quantity,
        position.entry_price,
        position.mark_price.as_deref().unwrap_or("-"),
        position.unrealized_pnl,
        position.realized_pnl,
        position.status,
        position.strategy_id.as_deref().unwrap_or("-"),
        display_option(position.signal_id)
    );
}

pub fn print_paper_pnl(response: &PaperPnlResponse) {
    let pnl = &response.pnl;
    println!("Equity: {}", pnl.equity);
    println!("Realized PnL: {}", pnl.realized_pnl);
    println!("Unrealized PnL: {}", pnl.unrealized_pnl);
    println!("Daily PnL: {}", pnl.daily_pnl);
    println!("Drawdown %: {}", pnl.drawdown_pct);
    println!("Price status: {}", pnl.price_status);
    println!("Open positions: {}", pnl.open_positions_count);
}

pub fn print_paper_close(response: &PaperClosePositionResponse) {
    println!("Status: {}", response.status);
    println!("Position ID: {}", response.position_id);
    println!("Symbol: {}", response.symbol);
    println!("Quantity: {}", response.quantity);
    println!("Entry price: {}", response.entry_price);
    println!("Exit price: {}", response.exit_price);
    println!("Realized PnL: {}", response.realized_pnl);
    println!("Fee: {}", response.fee);
    println!("Slippage: {}", response.slippage_cost);
    println!("Close fill ID: {}", response.close_fill_id);
    println!("Journal entry ID: {}", response.journal_entry_id);
    println!("Correlation ID: {}", response.correlation_id);
}

pub fn print_paper_equity(response: &PaperEquityResponse) {
    for point in &response.equity {
        println!(
            "{} equity={} realized={} unrealized={} drawdown_pct={}",
            point.snapshot_at.to_rfc3339(),
            point.equity,
            point.realized_pnl,
            point.unrealized_pnl,
            point.drawdown_pct
        );
    }
}

pub fn print_paper_journal(response: &PaperTradeJournalResponse) {
    for entry in &response.journal {
        println!(
            "{} {} symbol={} pnl={} corr={}",
            entry.created_at.to_rfc3339(),
            entry.event_type,
            entry.symbol.as_deref().unwrap_or("-"),
            entry.pnl.as_deref().unwrap_or("-"),
            entry.correlation_id
        );
    }
}

pub fn print_events(response: &RecentEventsResponse) {
    for event in &response.events {
        println!(
            "{}  {}  {}  corr={}  event_id={}",
            event.occurred_at.to_rfc3339(),
            event.event_type,
            event.source,
            event.correlation_id,
            event.event_id
        );
    }
}

pub fn print_risk_decisions(response: &RiskDecisionsResponse) {
    for decision in &response.decisions {
        let label = if decision.decision.eq_ignore_ascii_case("rejected") {
            format!("WARNING: {}", decision.decision)
                .red()
                .bold()
                .to_string()
        } else {
            decision.decision.clone()
        };
        println!(
            "{}  decision={} symbol={} strategy={} signal={} reasons={}",
            decision.id,
            label,
            decision.symbol.as_deref().unwrap_or("-"),
            decision.strategy_id.as_deref().unwrap_or("-"),
            display_option(decision.signal_id),
            display_vec(&decision.reasons)
        );
    }
}

pub fn print_backtest_accepted(response: &BacktestRunAcceptedResponse) {
    println!("Run ID: {}", response.run_id);
    println!("Status: {}", response.status);
    println!("Strategy: {}", response.strategy_id);
    println!("Symbol: {}", response.symbol);
    println!("Trade count: {}", response.trade_count);
    println!("PnL: {} ({}%)", response.pnl, response.pnl_pct);
    println!("Max drawdown %: {}", response.max_drawdown_pct);
    println!("Win rate: {}", response.win_rate);
    println!("Fee paid: {}", response.fee_paid);
    println!("Slippage cost: {}", response.slippage_cost);
    println!(
        "Correlation ID: {}",
        response
            .correlation_id
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
}

pub fn print_backtest_runs(runs: &[BacktestResult]) {
    for run in runs {
        println!(
            "{}  {} {} {} status={} pnl={} pnl_pct={} trades={}",
            run.run_id,
            run.strategy_id,
            run.symbol,
            run.timeframe,
            run.status,
            run.pnl,
            run.pnl_pct,
            run.trade_count
        );
    }
}

pub fn print_backtest_run(run: &BacktestResult) {
    println!("Run ID: {}", run.run_id);
    println!("Status: {}", run.status);
    println!("Strategy: {}", run.strategy_id);
    println!("Symbol: {}", run.symbol);
    println!("Timeframe: {}", run.timeframe);
    println!(
        "Window: {} -> {}",
        run.start_time.to_rfc3339(),
        run.end_time.to_rfc3339()
    );
    println!("Initial capital: {}", run.initial_capital);
    println!("Final equity: {}", run.final_equity);
    println!("PnL: {} ({}%)", run.pnl, run.pnl_pct);
    println!("Max drawdown %: {}", run.max_drawdown_pct);
    println!("Win rate: {}", run.win_rate);
    println!(
        "Trade breakdown: total={} wins={} losses={}",
        run.trade_count, run.winning_trades, run.losing_trades
    );
    println!("Fee paid: {}", run.fee_paid);
    println!("Slippage cost: {}", run.slippage_cost);
}

pub fn print_backfill_result(result: &aegis_core::CandleBackfillResult) {
    println!("Run ID: {}", result.run_id);
    println!("Status: {}", result.status.as_str());
    println!("Exchange: {}", result.exchange.as_str());
    println!("Symbol: {}", result.symbol);
    println!("Interval: {}", result.interval);
    println!("Fetched candles: {}", result.fetched_candles);
    println!("Inserted candles: {}", result.inserted_candles);
    println!("Updated candles: {}", result.updated_candles);
    println!("Skipped candles: {}", result.skipped_candles);
    println!("Correlation ID: {}", result.correlation_id);
    if let Some(reason) = &result.failed_reason {
        println!("Failure reason: {}", reason);
    }
}

pub fn print_backfill_runs(response: &CandleBackfillRunsResponse) {
    for run in &response.runs {
        println!(
            "{}  {} {} {} status={} fetched={} inserted={} updated={} skipped={}",
            run.run_id,
            run.exchange.as_str(),
            run.symbol,
            run.interval,
            run.status.as_str(),
            run.fetched_candles,
            run.inserted_candles,
            run.updated_candles,
            run.skipped_candles
        );
    }
}

pub fn print_backfill_run(response: &CandleBackfillRunResponse) {
    print_backfill_result(&response.run);
}

pub fn print_strategy_performance_summary(response: &StrategyPerformanceSummaryResponse) {
    let summary = &response.summary;
    println!(
        "Mode: {}  Strategy: {}  Symbol: {}  Timeframe: {}",
        summary.mode.as_str(),
        summary.strategy_id.as_deref().unwrap_or("ALL"),
        summary.symbol.as_deref().unwrap_or("ALL"),
        summary.timeframe.as_deref().unwrap_or("ALL")
    );
    println!("Window: {} -> {}", summary.window_start, summary.window_end);
    println!(
        "Runs: {}  Signals: {}  Approved risk: {}  Rejected risk: {}  Rejection rate: {}",
        summary.total_runs,
        summary.total_signals,
        summary.approved_risk_decisions,
        summary.rejected_risk_decisions,
        summary.risk_rejection_rate
    );
    println!(
        "Shadow would-submit: {}  No-signal: {}  Shadow risk-rejected: {}",
        summary.shadow_would_submit_count,
        summary.shadow_no_signal_count,
        summary.shadow_risk_rejected_count
    );
    println!(
        "Paper orders: {}  Opened: {}  Closed: {}",
        summary.paper_orders_count, summary.paper_positions_opened, summary.paper_positions_closed
    );
    println!(
        "Realized PnL: {}  Unrealized PnL: {}  Win rate: {}",
        summary.realized_pnl,
        summary.unrealized_pnl,
        summary
            .win_rate
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Backtests: {}  Best: {}  Worst: {}  Avg: {}",
        summary.backtest_runs_count,
        summary
            .best_backtest_pnl_pct
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        summary
            .worst_backtest_pnl_pct
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        summary
            .avg_backtest_pnl_pct
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
}

pub fn print_strategy_performance_rankings(response: &StrategyPerformanceRankingsResponse) {
    for ranking in &response.rankings {
        println!(
            "{} mode={} realized={} would_submit={} rejected={} backtest_avg={}",
            ranking.strategy_id,
            ranking.mode.as_str(),
            ranking.realized_pnl,
            ranking.shadow_would_submit_count,
            ranking.rejected_risk_decisions,
            ranking
                .avg_backtest_pnl_pct
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        );
    }
}

pub fn print_strategy_decision_breakdown(response: &StrategyDecisionBreakdownResponse) {
    let breakdown = &response.breakdown;
    println!(
        "Strategy: {}  Symbol: {}  Timeframe: {}",
        breakdown.strategy_id,
        breakdown.symbol.as_deref().unwrap_or("ALL"),
        breakdown.timeframe.as_deref().unwrap_or("ALL")
    );
    println!(
        "Window: {} -> {}",
        breakdown.window_start, breakdown.window_end
    );
    println!(
        "Runs: {}  Would-submit: {}  No-signal: {}  Risk-rejected: {}  Skipped: {}  Error: {}",
        breakdown.total_runs,
        breakdown.would_submit_count,
        breakdown.no_signal_count,
        breakdown.risk_rejected_count,
        breakdown.skipped_count,
        breakdown.error_count
    );
}

pub fn print_testnet_promotion_funnel_summary(response: &TestnetPromotionFunnelSummaryResponse) {
    let summary = &response.summary;
    println!(
        "Strategy: {}  Symbol: {}  Timeframe: {}",
        summary.strategy_id.as_deref().unwrap_or("ALL"),
        summary.symbol.as_deref().unwrap_or("ALL"),
        summary.timeframe.as_deref().unwrap_or("ALL")
    );
    println!(
        "Window: {} -> {}",
        summary
            .window_start
            .map(|value| value.to_string())
            .unwrap_or_else(|| "ALL".to_string()),
        summary
            .window_end
            .map(|value| value.to_string())
            .unwrap_or_else(|| "ALL".to_string())
    );
    println!(
        "Shadow: {}  Previewed: {}  Submitted: {}  Orders: {}  Acked: {}  Filled: {}",
        summary.shadow_would_submit_count,
        summary.promotion_previewed_count,
        summary.promotion_submitted_count,
        summary.testnet_orders_created_count,
        summary.acked_count,
        summary.filled_count
    );
    println!(
        "Rejected promos: {}  Expired promos: {}  Duplicate rejected: {}",
        summary.promotion_rejected_count,
        summary.promotion_expired_count,
        summary.promotion_duplicate_rejected_count
    );
    println!(
        "Cancelled: {}  Rejected orders: {}  Expired orders: {}  Reconciliation required: {}  Unknown: {}  Failed: {}",
        summary.cancelled_count,
        summary.rejected_count,
        summary.expired_count,
        summary.reconciliation_required_count,
        summary.unknown_exchange_state_count,
        summary.failed_count
    );
    println!(
        "Preview rate: {}%  Submit rate: {}%  Ack rate: {}%  Fill rate: {}%  Reconciliation required rate: {}%",
        summary.preview_rate_pct,
        summary.submit_rate_pct,
        summary.ack_rate_pct,
        summary.fill_rate_pct,
        summary.reconciliation_required_rate_pct
    );
}

pub fn print_testnet_promotion_outcomes(response: &TestnetPromotionFunnelOutcomesResponse) {
    println!("Outcomes:");
    for outcome in &response.outcomes {
        println!(
            "{} count={} rate={}%",
            outcome.outcome, outcome.count, outcome.rate_pct
        );
    }
    println!("Lifecycle:");
    for item in &response.lifecycle {
        println!(
            "{} count={} rate={}%",
            item.execution_state, item.count, item.rate_pct
        );
    }
}

pub fn print_testnet_promotion_rows(response: &TestnetPromotionFunnelRowsResponse) {
    for row in &response.rows {
        println!(
            "{} promotion={} strategy={} symbol={} status={} client_order_id={} execution_state={} previewed_at={} submitted_at={}",
            row.shadow_run_id,
            row.promotion_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            row.strategy_id,
            row.symbol,
            row.promotion_status.as_deref().unwrap_or("-"),
            row.client_order_id.as_deref().unwrap_or("-"),
            row.execution_state
                .map(|value| value.as_str().to_string())
                .unwrap_or_else(|| "-".to_string()),
            row.promotion_created_at
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            row.submitted_at
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        );
    }
}

fn summarize_feeds(feed: &FeedStatusResponse) -> String {
    if feed.feeds.is_empty() {
        return "none".to_string();
    }

    feed.feeds
        .iter()
        .map(|item| format!("{}:{}/{}", item.symbol, item.status, item.freshness_status))
        .collect::<Vec<_>>()
        .join(", ")
}

fn bool_word(value: bool) -> String {
    if value {
        "yes".green().to_string()
    } else {
        "no".red().to_string()
    }
}

fn paint_state(value: &str, ok: bool) -> String {
    if ok {
        value.green().bold().to_string()
    } else {
        value.red().bold().to_string()
    }
}

fn paint_order_status(value: &str) -> String {
    if value.eq_ignore_ascii_case("rejected") || value.eq_ignore_ascii_case("cancelled") {
        value.red().bold().to_string()
    } else {
        value.to_string()
    }
}

fn display_option<T: ToString>(value: Option<T>) -> String {
    value
        .map(|inner| inner.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn display_vec(values: &[String]) -> String {
    if values.is_empty() {
        "-".to_string()
    } else {
        values.join(", ")
    }
}

fn pipeline_decision_label(result: &PaperTradingPipelineResult) -> &'static str {
    match result.pipeline_decision {
        aegis_core::PipelineDecision::NoSignal => "NO_SIGNAL",
        aegis_core::PipelineDecision::RiskRejected => "RISK_REJECTED",
        aegis_core::PipelineDecision::PaperOrderCreated => "PAPER_ORDER_CREATED",
        aegis_core::PipelineDecision::PaperOrderReused => "PAPER_ORDER_REUSED",
        aegis_core::PipelineDecision::StrategyDisabled => "STRATEGY_DISABLED",
        aegis_core::PipelineDecision::SafetyStopped => "SAFETY_STOPPED",
    }
}
