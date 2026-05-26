use std::{
    collections::BTreeSet,
    sync::{Mutex, OnceLock},
    time::Duration,
};

use prometheus::{
    register_gauge_vec_with_registry, register_gauge_with_registry,
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
    register_int_counter_with_registry, register_int_gauge_vec_with_registry,
    register_int_gauge_with_registry, Encoder, Gauge, GaugeVec, HistogramVec, IntCounter,
    IntCounterVec, IntGauge, IntGaugeVec, Registry, TextEncoder,
};

const FEED_STATUSES: [&str; 4] = ["connected", "disconnected", "stale", "unknown"];

pub struct Telemetry {
    registry: Registry,
    api_requests_total: IntCounterVec,
    api_request_duration_seconds: HistogramVec,
    system_health_status: IntGauge,
    market_ticks_total: IntCounterVec,
    market_candles_closed_total: IntCounterVec,
    market_feed_status: IntGaugeVec,
    market_feed_last_event_age_seconds: GaugeVec,
    market_backfill_runs_total: IntCounterVec,
    market_backfill_candles_total: IntCounterVec,
    strategy_evaluations_total: IntCounterVec,
    strategy_signals_total: IntCounterVec,
    strategy_disabled_total: IntCounterVec,
    strategy_config_validations_total: IntCounterVec,
    strategy_config_updates_total: IntCounterVec,
    risk_config_validations_total: IntCounterVec,
    risk_config_updates_total: IntCounterVec,
    strategy_dry_runs_total: IntCounterVec,
    risk_decisions_total: IntCounterVec,
    risk_rejections_total: IntCounterVec,
    kill_switch_active: IntGauge,
    paper_pipeline_runs_total: IntCounterVec,
    paper_orders_total: IntCounterVec,
    paper_position_closes_total: IntCounterVec,
    paper_fills_total: IntCounterVec,
    paper_positions_open: IntGaugeVec,
    paper_equity: Gauge,
    paper_realized_pnl: Gauge,
    paper_unrealized_pnl: Gauge,
    backtest_runs_total: IntCounterVec,
    backtest_duration_seconds: HistogramVec,
    backtest_trades_total: IntCounterVec,
    analytics_requests_total: IntCounterVec,
    analytics_promotion_funnel_requests_total: IntCounter,
    research_candidates_total: IntCounterVec,
    research_candidate_promotions_total: IntCounterVec,
    exchange_testnet_requests_total: IntCounterVec,
    exchange_testnet_orders_total: IntCounterVec,
    exchange_testnet_errors_total: IntCounterVec,
    exchange_testnet_pipeline_runs_total: IntCounterVec,
    exchange_testnet_shadow_runs_total: IntCounterVec,
    exchange_testnet_shadow_runner_ticks_total: IntCounterVec,
    exchange_testnet_shadow_runner_runs_total: IntCounterVec,
    exchange_testnet_shadow_runner_status: IntGaugeVec,
    exchange_testnet_shadow_runner_last_tick_age_seconds: Gauge,
    exchange_testnet_shadow_would_submit_total: IntCounterVec,
    exchange_testnet_shadow_rejections_total: IntCounterVec,
    exchange_testnet_shadow_promotions_total: IntCounterVec,
    exchange_testnet_shadow_promotion_submits_total: IntCounterVec,
    exchange_testnet_lifecycle_transitions_total: IntCounterVec,
    exchange_testnet_lifecycle_invalid_transitions_total: IntCounterVec,
    exchange_testnet_orders_by_state: IntGaugeVec,
    exchange_testnet_repairs_total: IntCounterVec,
    exchange_testnet_repair_rejections_total: IntCounterVec,
    exchange_private_stream_events_total: IntCounterVec,
    exchange_private_stream_status: IntGaugeVec,
    exchange_private_stream_last_event_age_seconds: GaugeVec,
    exchange_private_stream_errors_total: IntCounterVec,
    exchange_reconciliation_runs_total: IntCounterVec,
    exchange_reconciliation_mismatches_total: IntCounterVec,
    exchange_reconciliation_checked_orders_total: IntCounterVec,
    execution_readiness_checks_total: IntCounterVec,
    execution_readiness_score: GaugeVec,
    execution_readiness_blockers_total: IntCounterVec,
    operator_reports_generated_total: IntCounterVec,
    operator_report_findings_total: IntCounterVec,
    db_health_status: IntGauge,
    db_query_errors_total: IntCounterVec,
    known_position_symbols: Mutex<BTreeSet<String>>,
}

impl Telemetry {
    fn new() -> Self {
        let registry = Registry::new_custom(Some("aegis".to_string()), None)
            .expect("telemetry registry should initialize");

        let api_requests_total = register_int_counter_vec_with_registry!(
            "api_requests_total",
            "Total API requests served.",
            &["method", "path", "status"],
            registry
        )
        .expect("api_requests_total should register");
        let api_request_duration_seconds = register_histogram_vec_with_registry!(
            "api_request_duration_seconds",
            "API request duration in seconds.",
            &["method", "path"],
            registry
        )
        .expect("api_request_duration_seconds should register");
        let system_health_status = register_int_gauge_with_registry!(
            "system_health_status",
            "System health status as 1 for healthy and 0 for unhealthy.",
            registry
        )
        .expect("system_health_status should register");
        let market_ticks_total = register_int_counter_vec_with_registry!(
            "market_ticks_total",
            "Persisted market ticks.",
            &["exchange", "symbol"],
            registry
        )
        .expect("market_ticks_total should register");
        let market_candles_closed_total = register_int_counter_vec_with_registry!(
            "market_candles_closed_total",
            "Closed candles produced by the deterministic candle builder.",
            &["exchange", "symbol", "interval"],
            registry
        )
        .expect("market_candles_closed_total should register");
        let market_feed_status = register_int_gauge_vec_with_registry!(
            "market_feed_status",
            "Market feed status by exchange, symbol, and status label.",
            &["exchange", "symbol", "status"],
            registry
        )
        .expect("market_feed_status should register");
        let market_feed_last_event_age_seconds = register_gauge_vec_with_registry!(
            "market_feed_last_event_age_seconds",
            "Age in seconds since the last market event for a feed.",
            &["exchange", "symbol"],
            registry
        )
        .expect("market_feed_last_event_age_seconds should register");
        let market_backfill_runs_total = register_int_counter_vec_with_registry!(
            "market_backfill_runs_total",
            "Historical candle backfill runs by result.",
            &["exchange", "symbol", "status"],
            registry
        )
        .expect("market_backfill_runs_total should register");
        let market_backfill_candles_total = register_int_counter_vec_with_registry!(
            "market_backfill_candles_total",
            "Historical candle backfill candle counts by result class.",
            &["exchange", "symbol", "result"],
            registry
        )
        .expect("market_backfill_candles_total should register");
        let strategy_evaluations_total = register_int_counter_vec_with_registry!(
            "strategy_evaluations_total",
            "Strategy evaluations by outcome.",
            &["strategy_id", "symbol", "result"],
            registry
        )
        .expect("strategy_evaluations_total should register");
        let strategy_signals_total = register_int_counter_vec_with_registry!(
            "strategy_signals_total",
            "Generated strategy signals.",
            &["strategy_id", "symbol", "side"],
            registry
        )
        .expect("strategy_signals_total should register");
        let strategy_disabled_total = register_int_counter_vec_with_registry!(
            "strategy_disabled_total",
            "Pipeline runs blocked because a strategy is disabled.",
            &["strategy_id"],
            registry
        )
        .expect("strategy_disabled_total should register");
        let strategy_config_validations_total = register_int_counter_vec_with_registry!(
            "strategy_config_validations_total",
            "Strategy config validations by result.",
            &["strategy_id", "result"],
            registry
        )
        .expect("strategy_config_validations_total should register");
        let strategy_config_updates_total = register_int_counter_vec_with_registry!(
            "strategy_config_updates_total",
            "Strategy config updates by result.",
            &["strategy_id", "result"],
            registry
        )
        .expect("strategy_config_updates_total should register");
        let risk_config_validations_total = register_int_counter_vec_with_registry!(
            "risk_config_validations_total",
            "Risk config validations by result.",
            &["result"],
            registry
        )
        .expect("risk_config_validations_total should register");
        let risk_config_updates_total = register_int_counter_vec_with_registry!(
            "risk_config_updates_total",
            "Risk config updates by result.",
            &["result"],
            registry
        )
        .expect("risk_config_updates_total should register");
        let strategy_dry_runs_total = register_int_counter_vec_with_registry!(
            "strategy_dry_runs_total",
            "Strategy dry-runs by result.",
            &["strategy_id", "result"],
            registry
        )
        .expect("strategy_dry_runs_total should register");
        let risk_decisions_total = register_int_counter_vec_with_registry!(
            "risk_decisions_total",
            "Persisted risk decisions by decision and primary reason.",
            &["decision", "reason"],
            registry
        )
        .expect("risk_decisions_total should register");
        let risk_rejections_total = register_int_counter_vec_with_registry!(
            "risk_rejections_total",
            "Rejected risk decisions by rejection reason.",
            &["reason"],
            registry
        )
        .expect("risk_rejections_total should register");
        let kill_switch_active = register_int_gauge_with_registry!(
            "kill_switch_active",
            "Kill switch state as 1 for active and 0 for inactive.",
            registry
        )
        .expect("kill_switch_active should register");
        let paper_pipeline_runs_total = register_int_counter_vec_with_registry!(
            "paper_pipeline_runs_total",
            "Paper pipeline runs by result.",
            &["strategy_id", "symbol", "result"],
            registry
        )
        .expect("paper_pipeline_runs_total should register");
        let paper_orders_total = register_int_counter_vec_with_registry!(
            "paper_orders_total",
            "Paper order lifecycle counts by symbol and status.",
            &["symbol", "status"],
            registry
        )
        .expect("paper_orders_total should register");
        let paper_position_closes_total = register_int_counter_vec_with_registry!(
            "paper_position_closes_total",
            "Paper position close attempts by symbol and result.",
            &["symbol", "result"],
            registry
        )
        .expect("paper_position_closes_total should register");
        let paper_fills_total = register_int_counter_vec_with_registry!(
            "paper_fills_total",
            "Paper fills by symbol and side.",
            &["symbol", "side"],
            registry
        )
        .expect("paper_fills_total should register");
        let paper_positions_open = register_int_gauge_vec_with_registry!(
            "paper_positions_open",
            "Open paper positions by symbol.",
            &["symbol"],
            registry
        )
        .expect("paper_positions_open should register");
        let paper_equity = register_gauge_with_registry!(
            "paper_equity",
            "Current paper trading equity.",
            registry
        )
        .expect("paper_equity should register");
        let paper_realized_pnl = register_gauge_with_registry!(
            "paper_realized_pnl",
            "Current realized paper PnL.",
            registry
        )
        .expect("paper_realized_pnl should register");
        let paper_unrealized_pnl = register_gauge_with_registry!(
            "paper_unrealized_pnl",
            "Current unrealized paper PnL.",
            registry
        )
        .expect("paper_unrealized_pnl should register");
        let backtest_runs_total = register_int_counter_vec_with_registry!(
            "backtest_runs_total",
            "Backtest runs by status.",
            &["strategy_id", "symbol", "status"],
            registry
        )
        .expect("backtest_runs_total should register");
        let backtest_duration_seconds = register_histogram_vec_with_registry!(
            "backtest_duration_seconds",
            "Backtest execution duration in seconds.",
            &["strategy_id", "symbol"],
            registry
        )
        .expect("backtest_duration_seconds should register");
        let backtest_trades_total = register_int_counter_vec_with_registry!(
            "backtest_trades_total",
            "Backtest trade counts.",
            &["strategy_id", "symbol"],
            registry
        )
        .expect("backtest_trades_total should register");
        let analytics_requests_total = register_int_counter_vec_with_registry!(
            "analytics_requests_total",
            "Analytics endpoint requests by kind.",
            &["kind"],
            registry
        )
        .expect("analytics_requests_total should register");
        let analytics_promotion_funnel_requests_total = register_int_counter_with_registry!(
            "analytics_promotion_funnel_requests_total",
            "Promotion funnel analytics requests.",
            registry
        )
        .expect("analytics_promotion_funnel_requests_total should register");
        let research_candidates_total = register_int_counter_vec_with_registry!(
            "research_candidates_total",
            "Research candidates by status.",
            &["status"],
            registry
        )
        .expect("research_candidates_total should register");
        let research_candidate_promotions_total = register_int_counter_vec_with_registry!(
            "research_candidate_promotions_total",
            "Research candidate promotions by status.",
            &["status"],
            registry
        )
        .expect("research_candidate_promotions_total should register");
        let exchange_testnet_requests_total = register_int_counter_vec_with_registry!(
            "exchange_testnet_requests_total",
            "Exchange testnet adapter requests by operation and result.",
            &["operation", "result"],
            registry
        )
        .expect("exchange_testnet_requests_total should register");
        let exchange_testnet_orders_total = register_int_counter_vec_with_registry!(
            "exchange_testnet_orders_total",
            "Exchange testnet orders by symbol, side, and status.",
            &["symbol", "side", "status"],
            registry
        )
        .expect("exchange_testnet_orders_total should register");
        let exchange_testnet_errors_total = register_int_counter_vec_with_registry!(
            "exchange_testnet_errors_total",
            "Exchange testnet adapter errors by operation and kind.",
            &["operation", "kind"],
            registry
        )
        .expect("exchange_testnet_errors_total should register");
        let exchange_testnet_pipeline_runs_total = register_int_counter_vec_with_registry!(
            "exchange_testnet_pipeline_runs_total",
            "Exchange testnet pipeline runs by result.",
            &["result"],
            registry
        )
        .expect("exchange_testnet_pipeline_runs_total should register");
        let exchange_testnet_shadow_runs_total = register_int_counter_vec_with_registry!(
            "exchange_testnet_shadow_runs_total",
            "Exchange testnet shadow runs by strategy, symbol, and decision.",
            &["strategy_id", "symbol", "decision"],
            registry
        )
        .expect("exchange_testnet_shadow_runs_total should register");
        let exchange_testnet_shadow_runner_ticks_total = register_int_counter_vec_with_registry!(
            "exchange_testnet_shadow_runner_ticks_total",
            "Exchange testnet shadow runner ticks by status.",
            &["status"],
            registry
        )
        .expect("exchange_testnet_shadow_runner_ticks_total should register");
        let exchange_testnet_shadow_runner_runs_total = register_int_counter_vec_with_registry!(
            "exchange_testnet_shadow_runner_runs_total",
            "Exchange testnet shadow runner persisted runs by decision.",
            &["decision"],
            registry
        )
        .expect("exchange_testnet_shadow_runner_runs_total should register");
        let exchange_testnet_shadow_runner_status = register_int_gauge_vec_with_registry!(
            "exchange_testnet_shadow_runner_status",
            "Current testnet shadow runner status.",
            &["status"],
            registry
        )
        .expect("exchange_testnet_shadow_runner_status should register");
        let exchange_testnet_shadow_runner_last_tick_age_seconds = register_gauge_with_registry!(
            "exchange_testnet_shadow_runner_last_tick_age_seconds",
            "Age in seconds since the latest testnet shadow runner tick.",
            registry
        )
        .expect("exchange_testnet_shadow_runner_last_tick_age_seconds should register");
        let exchange_testnet_shadow_would_submit_total = register_int_counter_vec_with_registry!(
            "exchange_testnet_shadow_would_submit_total",
            "Exchange testnet shadow WOULD_SUBMIT counts by strategy and symbol.",
            &["strategy_id", "symbol"],
            registry
        )
        .expect("exchange_testnet_shadow_would_submit_total should register");
        let exchange_testnet_shadow_rejections_total = register_int_counter_vec_with_registry!(
            "exchange_testnet_shadow_rejections_total",
            "Exchange testnet shadow rejections by reason.",
            &["reason"],
            registry
        )
        .expect("exchange_testnet_shadow_rejections_total should register");
        let exchange_testnet_shadow_promotions_total = register_int_counter_vec_with_registry!(
            "exchange_testnet_shadow_promotions_total",
            "Exchange testnet shadow promotions by status.",
            &["status"],
            registry
        )
        .expect("exchange_testnet_shadow_promotions_total should register");
        let exchange_testnet_shadow_promotion_submits_total =
            register_int_counter_vec_with_registry!(
                "exchange_testnet_shadow_promotion_submits_total",
                "Exchange testnet shadow promotion submits by result.",
                &["result"],
                registry
            )
            .expect("exchange_testnet_shadow_promotion_submits_total should register");
        let exchange_testnet_lifecycle_transitions_total = register_int_counter_vec_with_registry!(
            "exchange_testnet_lifecycle_transitions_total",
            "Testnet lifecycle transitions by source and next_state.",
            &["source", "next_state"],
            registry
        )
        .expect("exchange_testnet_lifecycle_transitions_total should register");
        let exchange_testnet_lifecycle_invalid_transitions_total =
            register_int_counter_vec_with_registry!(
                "exchange_testnet_lifecycle_invalid_transitions_total",
                "Invalid testnet lifecycle transitions by source.",
                &["source"],
                registry
            )
            .expect("exchange_testnet_lifecycle_invalid_transitions_total should register");
        let exchange_testnet_orders_by_state = register_int_gauge_vec_with_registry!(
            "exchange_testnet_orders_by_state",
            "Current testnet order counts by execution state.",
            &["state"],
            registry
        )
        .expect("exchange_testnet_orders_by_state should register");
        let exchange_testnet_repairs_total = register_int_counter_vec_with_registry!(
            "exchange_testnet_repairs_total",
            "Exchange testnet repair attempts by action and status.",
            &["action", "status"],
            registry
        )
        .expect("exchange_testnet_repairs_total should register");
        let exchange_testnet_repair_rejections_total = register_int_counter_vec_with_registry!(
            "exchange_testnet_repair_rejections_total",
            "Rejected exchange testnet repairs by action and reason.",
            &["action", "reason"],
            registry
        )
        .expect("exchange_testnet_repair_rejections_total should register");
        let exchange_private_stream_events_total = register_int_counter_vec_with_registry!(
            "exchange_private_stream_events_total",
            "Exchange private stream events by environment and event_type.",
            &["environment", "event_type"],
            registry
        )
        .expect("exchange_private_stream_events_total should register");
        let exchange_private_stream_status = register_int_gauge_vec_with_registry!(
            "exchange_private_stream_status",
            "Exchange private stream status by environment and status.",
            &["environment", "status"],
            registry
        )
        .expect("exchange_private_stream_status should register");
        let exchange_private_stream_last_event_age_seconds = register_gauge_vec_with_registry!(
            "exchange_private_stream_last_event_age_seconds",
            "Age of the latest private stream event in seconds by environment.",
            &["environment"],
            registry
        )
        .expect("exchange_private_stream_last_event_age_seconds should register");
        let exchange_private_stream_errors_total = register_int_counter_vec_with_registry!(
            "exchange_private_stream_errors_total",
            "Exchange private stream errors by environment and kind.",
            &["environment", "kind"],
            registry
        )
        .expect("exchange_private_stream_errors_total should register");
        let exchange_reconciliation_runs_total = register_int_counter_vec_with_registry!(
            "exchange_reconciliation_runs_total",
            "Exchange reconciliation runs by environment and status.",
            &["environment", "status"],
            registry
        )
        .expect("exchange_reconciliation_runs_total should register");
        let exchange_reconciliation_mismatches_total = register_int_counter_vec_with_registry!(
            "exchange_reconciliation_mismatches_total",
            "Exchange reconciliation mismatches by environment and kind.",
            &["environment", "kind"],
            registry
        )
        .expect("exchange_reconciliation_mismatches_total should register");
        let exchange_reconciliation_checked_orders_total = register_int_counter_vec_with_registry!(
            "exchange_reconciliation_checked_orders_total",
            "Exchange reconciliation checked order count by environment.",
            &["environment"],
            registry
        )
        .expect("exchange_reconciliation_checked_orders_total should register");
        let execution_readiness_checks_total = register_int_counter_vec_with_registry!(
            "execution_readiness_checks_total",
            "Execution readiness checks by target and status.",
            &["target", "status"],
            registry
        )
        .expect("execution_readiness_checks_total should register");
        let execution_readiness_score = register_gauge_vec_with_registry!(
            "execution_readiness_score",
            "Latest execution readiness score by target.",
            &["target"],
            registry
        )
        .expect("execution_readiness_score should register");
        let execution_readiness_blockers_total = register_int_counter_vec_with_registry!(
            "execution_readiness_blockers_total",
            "Execution readiness blockers by target and reason.",
            &["target", "reason"],
            registry
        )
        .expect("execution_readiness_blockers_total should register");
        let operator_reports_generated_total = register_int_counter_vec_with_registry!(
            "operator_reports_generated_total",
            "Operator reports generated by format and status.",
            &["format", "status"],
            registry
        )
        .expect("operator_reports_generated_total should register");
        let operator_report_findings_total = register_int_counter_vec_with_registry!(
            "operator_report_findings_total",
            "Operator report findings by severity.",
            &["severity"],
            registry
        )
        .expect("operator_report_findings_total should register");
        let db_health_status = register_int_gauge_with_registry!(
            "db_health_status",
            "Database health status as 1 for healthy and 0 for unhealthy.",
            registry
        )
        .expect("db_health_status should register");
        let db_query_errors_total = register_int_counter_vec_with_registry!(
            "db_query_errors_total",
            "Database query failures by operation.",
            &["operation"],
            registry
        )
        .expect("db_query_errors_total should register");

        Self {
            registry,
            api_requests_total,
            api_request_duration_seconds,
            system_health_status,
            market_ticks_total,
            market_candles_closed_total,
            market_feed_status,
            market_feed_last_event_age_seconds,
            market_backfill_runs_total,
            market_backfill_candles_total,
            strategy_evaluations_total,
            strategy_signals_total,
            strategy_disabled_total,
            strategy_config_validations_total,
            strategy_config_updates_total,
            risk_config_validations_total,
            risk_config_updates_total,
            strategy_dry_runs_total,
            risk_decisions_total,
            risk_rejections_total,
            kill_switch_active,
            paper_pipeline_runs_total,
            paper_orders_total,
            paper_position_closes_total,
            paper_fills_total,
            paper_positions_open,
            paper_equity,
            paper_realized_pnl,
            paper_unrealized_pnl,
            backtest_runs_total,
            backtest_duration_seconds,
            backtest_trades_total,
            analytics_requests_total,
            analytics_promotion_funnel_requests_total,
            research_candidates_total,
            research_candidate_promotions_total,
            exchange_testnet_requests_total,
            exchange_testnet_orders_total,
            exchange_testnet_errors_total,
            exchange_testnet_pipeline_runs_total,
            exchange_testnet_shadow_runs_total,
            exchange_testnet_shadow_runner_ticks_total,
            exchange_testnet_shadow_runner_runs_total,
            exchange_testnet_shadow_runner_status,
            exchange_testnet_shadow_runner_last_tick_age_seconds,
            exchange_testnet_shadow_would_submit_total,
            exchange_testnet_shadow_rejections_total,
            exchange_testnet_shadow_promotions_total,
            exchange_testnet_shadow_promotion_submits_total,
            exchange_testnet_lifecycle_transitions_total,
            exchange_testnet_lifecycle_invalid_transitions_total,
            exchange_testnet_orders_by_state,
            exchange_testnet_repairs_total,
            exchange_testnet_repair_rejections_total,
            exchange_private_stream_events_total,
            exchange_private_stream_status,
            exchange_private_stream_last_event_age_seconds,
            exchange_private_stream_errors_total,
            exchange_reconciliation_runs_total,
            exchange_reconciliation_mismatches_total,
            exchange_reconciliation_checked_orders_total,
            execution_readiness_checks_total,
            execution_readiness_score,
            execution_readiness_blockers_total,
            operator_reports_generated_total,
            operator_report_findings_total,
            db_health_status,
            db_query_errors_total,
            known_position_symbols: Mutex::new(BTreeSet::new()),
        }
    }

    pub fn encode(&self) -> Result<String, prometheus::Error> {
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        TextEncoder::new().encode(&metric_families, &mut buffer)?;
        Ok(String::from_utf8_lossy(&buffer).into_owned())
    }

    pub fn observe_api_request(&self, method: &str, path: &str, status: u16, duration: Duration) {
        let status = status.to_string();
        self.api_requests_total
            .with_label_values(&[method, path, status.as_str()])
            .inc();
        self.api_request_duration_seconds
            .with_label_values(&[method, path])
            .observe(duration.as_secs_f64());
    }

    pub fn set_system_health(&self, healthy: bool) {
        self.system_health_status.set(i64::from(healthy));
    }

    pub fn set_db_health(&self, healthy: bool) {
        self.db_health_status.set(i64::from(healthy));
    }

    pub fn record_db_query_error(&self, operation: &str) {
        self.db_query_errors_total
            .with_label_values(&[operation])
            .inc();
    }

    pub fn inc_market_ticks(&self, exchange: &str, symbol: &str) {
        self.market_ticks_total
            .with_label_values(&[exchange, symbol])
            .inc();
    }

    pub fn inc_market_candles_closed(&self, exchange: &str, symbol: &str, interval: &str) {
        self.market_candles_closed_total
            .with_label_values(&[exchange, symbol, interval])
            .inc();
    }

    pub fn set_market_feed_status(&self, exchange: &str, symbol: &str, current_status: &str) {
        for status in FEED_STATUSES {
            let value = i64::from(status == current_status);
            self.market_feed_status
                .with_label_values(&[exchange, symbol, status])
                .set(value);
        }
    }

    pub fn set_market_feed_last_event_age_seconds(
        &self,
        exchange: &str,
        symbol: &str,
        age_seconds: Option<f64>,
    ) {
        self.market_feed_last_event_age_seconds
            .with_label_values(&[exchange, symbol])
            .set(age_seconds.unwrap_or(f64::NAN));
    }

    pub fn inc_market_backfill_run(&self, exchange: &str, symbol: &str, status: &str) {
        self.market_backfill_runs_total
            .with_label_values(&[exchange, symbol, status])
            .inc();
    }

    pub fn add_market_backfill_candles(
        &self,
        exchange: &str,
        symbol: &str,
        result: &str,
        count: u64,
    ) {
        self.market_backfill_candles_total
            .with_label_values(&[exchange, symbol, result])
            .inc_by(count);
    }

    pub fn inc_strategy_evaluation(&self, strategy_id: &str, symbol: &str, result: &str) {
        self.strategy_evaluations_total
            .with_label_values(&[strategy_id, symbol, result])
            .inc();
    }

    pub fn inc_strategy_signal(&self, strategy_id: &str, symbol: &str, side: &str) {
        self.strategy_signals_total
            .with_label_values(&[strategy_id, symbol, side])
            .inc();
    }

    pub fn inc_strategy_disabled(&self, strategy_id: &str) {
        self.strategy_disabled_total
            .with_label_values(&[strategy_id])
            .inc();
    }

    pub fn inc_strategy_config_validation(&self, strategy_id: &str, result: &str) {
        self.strategy_config_validations_total
            .with_label_values(&[strategy_id, result])
            .inc();
    }

    pub fn inc_strategy_config_update(&self, strategy_id: &str, result: &str) {
        self.strategy_config_updates_total
            .with_label_values(&[strategy_id, result])
            .inc();
    }

    pub fn inc_risk_config_validation(&self, result: &str) {
        self.risk_config_validations_total
            .with_label_values(&[result])
            .inc();
    }

    pub fn inc_risk_config_update(&self, result: &str) {
        self.risk_config_updates_total
            .with_label_values(&[result])
            .inc();
    }

    pub fn inc_strategy_dry_run(&self, strategy_id: &str, result: &str) {
        self.strategy_dry_runs_total
            .with_label_values(&[strategy_id, result])
            .inc();
    }

    pub fn inc_risk_decision(&self, decision: &str, reason: &str) {
        self.risk_decisions_total
            .with_label_values(&[decision, reason])
            .inc();
    }

    pub fn inc_risk_rejection(&self, reason: &str) {
        self.risk_rejections_total
            .with_label_values(&[reason])
            .inc();
    }

    pub fn set_kill_switch_active(&self, active: bool) {
        self.kill_switch_active.set(i64::from(active));
    }

    pub fn inc_paper_pipeline_run(&self, strategy_id: &str, symbol: &str, result: &str) {
        self.paper_pipeline_runs_total
            .with_label_values(&[strategy_id, symbol, result])
            .inc();
    }

    pub fn inc_paper_order(&self, symbol: &str, status: &str) {
        self.paper_orders_total
            .with_label_values(&[symbol, status])
            .inc();
    }

    pub fn inc_paper_position_close(&self, symbol: &str, result: &str) {
        self.paper_position_closes_total
            .with_label_values(&[symbol, result])
            .inc();
    }

    pub fn inc_paper_fill(&self, symbol: &str, side: &str) {
        self.paper_fills_total
            .with_label_values(&[symbol, side])
            .inc();
    }

    pub fn set_paper_positions_open<I>(&self, positions: I)
    where
        I: IntoIterator<Item = (String, i64)>,
    {
        let mut seen = BTreeSet::new();
        for (symbol, count) in positions {
            self.paper_positions_open
                .with_label_values(&[symbol.as_str()])
                .set(count);
            seen.insert(symbol);
        }

        let mut known = self
            .known_position_symbols
            .lock()
            .expect("known_position_symbols lock should not poison");
        for symbol in known.iter() {
            if !seen.contains(symbol) {
                self.paper_positions_open
                    .with_label_values(&[symbol.as_str()])
                    .set(0);
            }
        }
        *known = seen;
    }

    pub fn set_paper_account_values(&self, equity: f64, realized_pnl: f64, unrealized_pnl: f64) {
        self.paper_equity.set(equity);
        self.paper_realized_pnl.set(realized_pnl);
        self.paper_unrealized_pnl.set(unrealized_pnl);
    }

    pub fn inc_backtest_run(&self, strategy_id: &str, symbol: &str, status: &str) {
        self.backtest_runs_total
            .with_label_values(&[strategy_id, symbol, status])
            .inc();
    }

    pub fn observe_backtest_duration(&self, strategy_id: &str, symbol: &str, duration: Duration) {
        self.backtest_duration_seconds
            .with_label_values(&[strategy_id, symbol])
            .observe(duration.as_secs_f64());
    }

    pub fn add_backtest_trades(&self, strategy_id: &str, symbol: &str, count: u64) {
        self.backtest_trades_total
            .with_label_values(&[strategy_id, symbol])
            .inc_by(count);
    }

    pub fn inc_analytics_request(&self, kind: &str) {
        self.analytics_requests_total
            .with_label_values(&[kind])
            .inc();
    }

    pub fn inc_analytics_promotion_funnel_request(&self) {
        self.analytics_promotion_funnel_requests_total.inc();
    }

    pub fn inc_research_candidate(&self, status: &str) {
        self.research_candidates_total
            .with_label_values(&[status])
            .inc();
    }

    pub fn inc_research_candidate_promotion(&self, status: &str) {
        self.research_candidate_promotions_total
            .with_label_values(&[status])
            .inc();
    }

    pub fn inc_exchange_testnet_request(&self, operation: &str, result: &str) {
        self.exchange_testnet_requests_total
            .with_label_values(&[operation, result])
            .inc();
    }

    pub fn inc_exchange_testnet_order(&self, symbol: &str, side: &str, status: &str) {
        self.exchange_testnet_orders_total
            .with_label_values(&[symbol, side, status])
            .inc();
    }

    pub fn inc_exchange_testnet_error(&self, operation: &str, kind: &str) {
        self.exchange_testnet_errors_total
            .with_label_values(&[operation, kind])
            .inc();
    }

    pub fn inc_exchange_testnet_pipeline_run(&self, result: &str) {
        self.exchange_testnet_pipeline_runs_total
            .with_label_values(&[result])
            .inc();
    }

    pub fn inc_exchange_testnet_shadow_run(&self, strategy_id: &str, symbol: &str, decision: &str) {
        self.exchange_testnet_shadow_runs_total
            .with_label_values(&[strategy_id, symbol, decision])
            .inc();
    }

    pub fn inc_exchange_testnet_shadow_runner_tick(&self, status: &str) {
        self.exchange_testnet_shadow_runner_ticks_total
            .with_label_values(&[status])
            .inc();
    }

    pub fn inc_exchange_testnet_shadow_runner_run(&self, decision: &str) {
        self.exchange_testnet_shadow_runner_runs_total
            .with_label_values(&[decision])
            .inc();
    }

    pub fn set_exchange_testnet_shadow_runner_status(&self, status: &str) {
        for candidate in ["STOPPED", "RUNNING", "PAUSED", "ERROR"] {
            let value = if candidate == status { 1 } else { 0 };
            self.exchange_testnet_shadow_runner_status
                .with_label_values(&[candidate])
                .set(value);
        }
    }

    pub fn set_exchange_testnet_shadow_runner_last_tick_age_seconds(&self, age_seconds: f64) {
        self.exchange_testnet_shadow_runner_last_tick_age_seconds
            .set(age_seconds);
    }

    pub fn inc_exchange_testnet_shadow_would_submit(&self, strategy_id: &str, symbol: &str) {
        self.exchange_testnet_shadow_would_submit_total
            .with_label_values(&[strategy_id, symbol])
            .inc();
    }

    pub fn inc_exchange_testnet_shadow_rejection(&self, reason: &str) {
        self.exchange_testnet_shadow_rejections_total
            .with_label_values(&[reason])
            .inc();
    }

    pub fn inc_exchange_testnet_shadow_promotion(&self, status: &str) {
        self.exchange_testnet_shadow_promotions_total
            .with_label_values(&[status])
            .inc();
    }

    pub fn inc_exchange_testnet_shadow_promotion_submit(&self, result: &str) {
        self.exchange_testnet_shadow_promotion_submits_total
            .with_label_values(&[result])
            .inc();
    }

    pub fn inc_exchange_testnet_lifecycle_transition(&self, source: &str, next_state: &str) {
        self.exchange_testnet_lifecycle_transitions_total
            .with_label_values(&[source, next_state])
            .inc();
    }

    pub fn inc_exchange_testnet_lifecycle_invalid_transition(&self, source: &str) {
        self.exchange_testnet_lifecycle_invalid_transitions_total
            .with_label_values(&[source])
            .inc();
    }

    pub fn apply_exchange_testnet_order_state_transition(
        &self,
        previous_state: Option<&str>,
        next_state: &str,
    ) {
        if let Some(previous_state) = previous_state {
            self.exchange_testnet_orders_by_state
                .with_label_values(&[previous_state])
                .dec();
        }
        self.exchange_testnet_orders_by_state
            .with_label_values(&[next_state])
            .inc();
    }

    pub fn inc_exchange_testnet_repair(&self, action: &str, status: &str) {
        self.exchange_testnet_repairs_total
            .with_label_values(&[action, status])
            .inc();
    }

    pub fn inc_exchange_testnet_repair_rejection(&self, action: &str, reason: &str) {
        self.exchange_testnet_repair_rejections_total
            .with_label_values(&[action, reason])
            .inc();
    }

    pub fn inc_exchange_private_stream_event(&self, environment: &str, event_type: &str) {
        self.exchange_private_stream_events_total
            .with_label_values(&[environment, event_type])
            .inc();
    }

    pub fn set_exchange_private_stream_status(&self, environment: &str, status: &str) {
        for candidate in ["CONNECTING", "CONNECTED", "STALE", "DISCONNECTED", "ERROR"] {
            let value = if candidate == status { 1 } else { 0 };
            self.exchange_private_stream_status
                .with_label_values(&[environment, candidate])
                .set(value);
        }
    }

    pub fn set_exchange_private_stream_last_event_age_seconds(
        &self,
        environment: &str,
        age_seconds: f64,
    ) {
        self.exchange_private_stream_last_event_age_seconds
            .with_label_values(&[environment])
            .set(age_seconds);
    }

    pub fn inc_exchange_private_stream_error(&self, environment: &str, kind: &str) {
        self.exchange_private_stream_errors_total
            .with_label_values(&[environment, kind])
            .inc();
    }

    pub fn inc_exchange_reconciliation_run(&self, environment: &str, status: &str) {
        self.exchange_reconciliation_runs_total
            .with_label_values(&[environment, status])
            .inc();
    }

    pub fn inc_exchange_reconciliation_mismatch(&self, environment: &str, kind: &str) {
        self.exchange_reconciliation_mismatches_total
            .with_label_values(&[environment, kind])
            .inc();
    }

    pub fn inc_exchange_reconciliation_checked_orders(&self, environment: &str, count: u64) {
        self.exchange_reconciliation_checked_orders_total
            .with_label_values(&[environment])
            .inc_by(count);
    }

    pub fn inc_execution_readiness_check(&self, target: &str, status: &str) {
        self.execution_readiness_checks_total
            .with_label_values(&[target, status])
            .inc();
    }

    pub fn set_execution_readiness_score(&self, target: &str, score: f64) {
        self.execution_readiness_score
            .with_label_values(&[target])
            .set(score);
    }

    pub fn inc_execution_readiness_blocker(&self, target: &str, reason: &str) {
        self.execution_readiness_blockers_total
            .with_label_values(&[target, reason])
            .inc();
    }

    pub fn inc_operator_report_generated(&self, format: &str, status: &str) {
        self.operator_reports_generated_total
            .with_label_values(&[format, status])
            .inc();
    }

    pub fn inc_operator_report_finding(&self, severity: &str) {
        self.operator_report_findings_total
            .with_label_values(&[severity])
            .inc();
    }
}

pub fn telemetry() -> &'static Telemetry {
    static TELEMETRY: OnceLock<Telemetry> = OnceLock::new();
    TELEMETRY.get_or_init(Telemetry::new)
}
