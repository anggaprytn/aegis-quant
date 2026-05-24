use aegis_core::PaperTradingPipelineResult;
use colored::Colorize;
use serde::Serialize;

use crate::api::{
    BacktestResult, BacktestRunAcceptedResponse, CandleBackfillRunResponse,
    CandleBackfillRunsResponse, FeedStatusResponse, HealthResponse, OrderRecord,
    PaperAccountResponse, PaperClosePositionResponse, PaperEquityResponse, PaperPnlResponse,
    PaperPositionRecord, PaperPositionsResponse, PaperTradeJournalResponse, RecentEventsResponse,
    RiskActionResponse, RiskDecisionsResponse, RiskStatusResponse, StatusResponse,
    StrategyConfigAuditResponse, StrategyConfigValidationResponse, StrategyConfigVersionsResponse,
    StrategyDryRunResponse, StrategyListResponse, StrategyStatusResponse,
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
