use std::{
    collections::BTreeSet,
    sync::{Mutex, OnceLock},
    time::Duration,
};

use prometheus::{
    register_gauge_vec_with_registry, register_gauge_with_registry,
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
    register_int_gauge_vec_with_registry, register_int_gauge_with_registry, Encoder, Gauge,
    GaugeVec, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Registry, TextEncoder,
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
    risk_decisions_total: IntCounterVec,
    risk_rejections_total: IntCounterVec,
    kill_switch_active: IntGauge,
    paper_pipeline_runs_total: IntCounterVec,
    paper_orders_total: IntCounterVec,
    paper_positions_open: IntGaugeVec,
    paper_equity: Gauge,
    paper_realized_pnl: Gauge,
    paper_unrealized_pnl: Gauge,
    backtest_runs_total: IntCounterVec,
    backtest_duration_seconds: HistogramVec,
    backtest_trades_total: IntCounterVec,
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
            risk_decisions_total,
            risk_rejections_total,
            kill_switch_active,
            paper_pipeline_runs_total,
            paper_orders_total,
            paper_positions_open,
            paper_equity,
            paper_realized_pnl,
            paper_unrealized_pnl,
            backtest_runs_total,
            backtest_duration_seconds,
            backtest_trades_total,
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
}

pub fn telemetry() -> &'static Telemetry {
    static TELEMETRY: OnceLock<Telemetry> = OnceLock::new();
    TELEMETRY.get_or_init(Telemetry::new)
}
