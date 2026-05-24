mod pipeline;

use std::{env, net::SocketAddr, time::Instant};

use accounting::{
    compute_daily_pnl, compute_drawdown, mark_positions_to_market, PaperMarkPriceInput,
};
use aegis_core::{
    BacktestRequest, CandleBackfillRequest, CandleBackfillResult, CandleInterval, MarketMode,
    OrderIntent, PaperPriceStatus, PaperTradingPipelineRequest, RiskCheckContext,
    RiskEvaluationDecision, RiskEvaluationResult, RiskRejectionReason, Side, SignalReason,
    StrategyConfig, StrategyEvaluationContext, StrategyId, StrategyStatus, Symbol,
};
use api::{ensure_default_paper_account, persist_paper_fill_accounting};
use axum::{
    extract::{Path, Query, Request, State},
    http::{HeaderName, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, Json, Router,
};
use chrono::Utc;
use db::{
    backtest_result_from_record, candle_backfill_result_from_record, check_health, connect_pool,
    create_paper_order, ensure_system_state, get_backtest_equity_curve, get_backtest_run,
    get_backtest_trades, get_candle_backfill_run, get_default_paper_account,
    get_latest_market_tick, get_order_by_id, get_paper_position_by_id, get_recent_closed_candles,
    get_risk_decision_by_id, get_strategy_status, get_system_event, get_system_state,
    insert_paper_account, insert_paper_equity_snapshot, insert_risk_evaluation,
    insert_signal_deduped, insert_system_event, list_backtest_runs, list_candle_backfill_runs,
    list_candles, list_market_feed_statuses, list_open_paper_positions, list_orders,
    list_paper_equity_snapshots, list_paper_positions, list_paper_trade_journal,
    list_recent_risk_decisions_filtered, list_recent_signals, list_recent_system_events_filtered,
    list_strategy_status, load_risk_state_snapshot, paper_account_from_record,
    paper_equity_snapshot_from_record, paper_position_from_record, set_kill_switch_state,
    strategy_config_from_record, update_strategy_state, upsert_paper_position,
    upsert_strategy_config, BacktestEquityPointRecord, BacktestTradeRecord,
    CandleBackfillRunRecord, CandleRecord, CreateOrderError, DbConfig, InsertSignalOutcome,
    MarketFeedStatusRecord, MarketTickRecord, OrderRecord, PaperAccountRecord,
    PaperEquitySnapshotRecord, PaperPositionRecord, PaperTradeJournalRecord, PgPool,
    RiskDecisionRecord, SignalRecord, StateActor, StrategyStatusRecord, SystemEventRecord,
    SystemStateRecord,
};
use events::{EventPublisher, PostgresEventPublisher, SystemEventType};
use market_ingest::{HistoricalCandleBackfillService, MarketIngestConfig};
use replay_engine::ReplayEngine;
use risk_engine::RiskEvaluator;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use strategy_engine::{build_default_strategy_configs, evaluate as evaluate_strategy};
use tracing::{error, info};
use uuid::Uuid;

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");
const CORRELATION_ID_HEADER: HeaderName = HeaderName::from_static("x-correlation-id");
const DEFAULT_RECENT_EVENTS_LIMIT: i64 = 100;
const MAX_RECENT_EVENTS_LIMIT: i64 = 500;
const DEFAULT_RISK_DECISIONS_LIMIT: i64 = 50;
const MAX_RISK_DECISIONS_LIMIT: i64 = 200;
const DEFAULT_CANDLE_LIMIT: i64 = 100;
const MAX_CANDLE_LIMIT: i64 = 1_000;
const DEFAULT_BACKFILL_RUNS_LIMIT: i64 = 20;
const MAX_BACKFILL_RUNS_LIMIT: i64 = 200;
const DEFAULT_PAPER_LIMIT: i64 = 50;
const MAX_PAPER_LIMIT: i64 = 500;

#[derive(Clone)]
struct AppState {
    config: AppConfig,
    db_pool: PgPool,
    started_at: chrono::DateTime<Utc>,
    market_mode: MarketMode,
    market_config: MarketIngestConfig,
    strategy_runtime: StrategyRuntimeConfig,
}

#[derive(Clone)]
struct AppConfig {
    app_name: String,
    environment: String,
    bind_addr: SocketAddr,
    database_url: String,
    database_max_connections: u32,
}

#[derive(Clone)]
struct StrategyRuntimeConfig {
    default_symbols: Vec<Symbol>,
    default_timeframe: CandleInterval,
    default_notional: Decimal,
    momentum_lookback_candles: u32,
    breakout_lookback_candles: u32,
}

impl StrategyRuntimeConfig {
    fn from_env() -> Result<Self, String> {
        let default_symbols = env::var("STRATEGY_DEFAULT_SYMBOLS")
            .unwrap_or_else(|_| "BTCUSDT,ETHUSDT".to_string())
            .split(',')
            .map(Symbol::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| err.to_string())?;
        let default_timeframe = env::var("STRATEGY_DEFAULT_TIMEFRAME")
            .unwrap_or_else(|_| "1m".to_string())
            .parse()
            .map_err(|err: aegis_core::CoreError| err.to_string())?;
        let default_notional = env::var("STRATEGY_DEFAULT_NOTIONAL")
            .unwrap_or_else(|_| "100000".to_string())
            .parse::<Decimal>()
            .map_err(|err| format!("invalid STRATEGY_DEFAULT_NOTIONAL: {err}"))?;
        let momentum_lookback_candles = env::var("MOMENTUM_LOOKBACK_CANDLES")
            .unwrap_or_else(|_| "3".to_string())
            .parse::<u32>()
            .map_err(|err| format!("invalid MOMENTUM_LOOKBACK_CANDLES: {err}"))?;
        let breakout_lookback_candles = env::var("BREAKOUT_LOOKBACK_CANDLES")
            .unwrap_or_else(|_| "20".to_string())
            .parse::<u32>()
            .map_err(|err| format!("invalid BREAKOUT_LOOKBACK_CANDLES: {err}"))?;

        Ok(Self {
            default_symbols,
            default_timeframe,
            default_notional,
            momentum_lookback_candles,
            breakout_lookback_candles,
        })
    }

    fn default_configs(&self) -> Vec<StrategyConfig> {
        build_default_strategy_configs(
            self.default_symbols.clone(),
            self.default_timeframe,
            self.default_notional,
            self.momentum_lookback_candles,
            self.breakout_lookback_candles,
        )
    }
}

impl AppConfig {
    fn from_env() -> Result<Self, String> {
        let app_name = env::var("APP_NAME").unwrap_or_else(|_| "aegis-quant-api".to_string());
        let environment = env::var("APP_ENV").unwrap_or_else(|_| "development".to_string());
        let bind_addr = env::var("API_BIND_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:3000".to_string())
            .parse()
            .map_err(|err| format!("invalid API_BIND_ADDR: {err}"))?;
        let database_url =
            env::var("DATABASE_URL").map_err(|_| "DATABASE_URL must be set".to_string())?;
        let database_max_connections = env::var("DATABASE_MAX_CONNECTIONS")
            .ok()
            .map(|value| {
                value
                    .parse()
                    .map_err(|err| format!("invalid DATABASE_MAX_CONNECTIONS: {err}"))
            })
            .transpose()?
            .unwrap_or(5);

        Ok(Self {
            app_name,
            environment,
            bind_addr,
            database_url,
            database_max_connections,
        })
    }
}

#[derive(Clone)]
struct RequestContext {
    request_id: String,
    correlation_id: String,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    service: String,
    environment: String,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct StatusResponse {
    service: String,
    environment: String,
    market_mode: MarketMode,
    started_at: chrono::DateTime<Utc>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
    dependencies: Dependencies,
}

#[derive(Serialize)]
struct DbHealthResponse {
    status: &'static str,
    service: String,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct Dependencies {
    database: DependencyStatus,
    event_bus: DependencyStatus,
    exchange_execution: DependencyStatus,
}

#[derive(Serialize)]
struct DependencyStatus {
    status: &'static str,
}

#[derive(Deserialize)]
struct RecentEventsQuery {
    limit: Option<i64>,
    event_type: Option<String>,
    source: Option<String>,
    correlation_id: Option<String>,
}

#[derive(Deserialize)]
struct RiskDecisionsQuery {
    symbol: Option<String>,
    limit: Option<i64>,
}

#[derive(Deserialize)]
struct BacktestRunsQuery {
    limit: Option<i64>,
}

#[derive(Deserialize)]
struct PaperListQuery {
    limit: Option<i64>,
}

#[derive(Serialize)]
struct RecentEventsResponse {
    events: Vec<SystemEventRecord>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct RiskDecisionView {
    id: Uuid,
    signal_id: Option<Uuid>,
    decision: String,
    approved_notional: Option<String>,
    risk_score: Option<String>,
    reasons: Vec<String>,
    created_at: chrono::DateTime<Utc>,
    correlation_id: Uuid,
    strategy_id: Option<String>,
    symbol: Option<String>,
}

#[derive(Serialize)]
struct RiskDecisionsResponse {
    decisions: Vec<RiskDecisionView>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct RiskDecisionResponse {
    decision: RiskDecisionView,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct EventResponse {
    event: SystemEventRecord,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: &'static str,
    message: String,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct ActorResponse {
    actor: String,
    actor_id: Option<Uuid>,
}

#[derive(Serialize)]
struct KillSwitchResponse {
    enabled: bool,
    reason: Option<String>,
    updated_at: chrono::DateTime<Utc>,
    updated_by: ActorResponse,
    last_correlation_id: Uuid,
}

#[derive(Serialize)]
struct RiskStatusResponse {
    status: &'static str,
    market_mode: MarketMode,
    paper_trading_allowed: bool,
    live_trading_allowed: bool,
    resume_confirmation_required: &'static str,
    kill_switch: KillSwitchResponse,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct RiskActionResponse {
    status: &'static str,
    message: String,
    market_mode: MarketMode,
    paper_trading_allowed: bool,
    live_trading_allowed: bool,
    kill_switch: KillSwitchResponse,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Deserialize)]
struct KillSwitchRequest {
    reason: Option<String>,
}

#[derive(Deserialize)]
struct ResumeRequest {
    confirmation_text: String,
    reason: Option<String>,
}

#[derive(Deserialize)]
struct RiskEvaluateRequest {
    signal_id: Uuid,
    strategy_id: String,
    symbol: String,
    side: Side,
    suggested_notional: String,
    signal_created_at: chrono::DateTime<Utc>,
    correlation_id: Option<Uuid>,
}

#[derive(Serialize)]
struct RiskEvaluateResponse {
    decision: &'static str,
    approved_notional: Option<String>,
    risk_score: String,
    reasons: Vec<String>,
    correlation_id: Uuid,
}

#[derive(Deserialize)]
struct CreatePaperOrderRequest {
    risk_decision_id: Uuid,
    idempotency_key: String,
    symbol: String,
    side: Side,
    quantity: String,
    limit_price: Option<String>,
    correlation_id: Option<Uuid>,
    expires_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Serialize)]
struct OrderResponse {
    order: OrderView,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct OrdersResponse {
    orders: Vec<OrderView>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct OrderView {
    order_id: Uuid,
    client_order_id: String,
    exchange_order_id: Option<String>,
    signal_id: Option<Uuid>,
    risk_decision_id: Uuid,
    strategy_id: Option<String>,
    symbol: String,
    side: String,
    status: String,
    execution_state: String,
    idempotency_key: String,
    requested_notional: Option<String>,
    quantity: String,
    filled_qty: String,
    limit_price: Option<String>,
    filled_price: Option<String>,
    avg_fill_price: Option<String>,
    mode: String,
    market_mode: String,
    status_reason: Option<String>,
    correlation_id: Uuid,
    submitted_at: Option<chrono::DateTime<Utc>>,
    filled_at: Option<chrono::DateTime<Utc>>,
    cancelled_at: Option<chrono::DateTime<Utc>>,
    rejected_at: Option<chrono::DateTime<Utc>>,
    expired_at: Option<chrono::DateTime<Utc>>,
    expires_at: Option<chrono::DateTime<Utc>>,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct MarketSymbolsResponse {
    exchange: String,
    symbols: Vec<String>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Deserialize)]
struct LatestTickQuery {
    symbol: String,
}

#[derive(Serialize)]
struct MarketTickResponse {
    tick: MarketTickRecord,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Deserialize)]
struct CandlesQuery {
    symbol: String,
    interval: String,
    limit: Option<i64>,
}

#[derive(Deserialize)]
struct BackfillRunsQuery {
    limit: Option<i64>,
}

#[derive(Serialize)]
struct CandlesResponse {
    candles: Vec<CandleRecord>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct CandleBackfillRunsResponse {
    runs: Vec<CandleBackfillResult>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct CandleBackfillRunResponse {
    run: CandleBackfillResult,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct BacktestRunsResponse {
    runs: Vec<aegis_core::BacktestResult>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct BacktestRunResponse {
    run: aegis_core::BacktestResult,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct BacktestTradesResponse {
    trades: Vec<BacktestTradeRecord>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct BacktestEquityCurveResponse {
    equity: Vec<BacktestEquityPointRecord>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct BacktestRunAcceptedResponse {
    run_id: Uuid,
    status: aegis_core::ReplayRunStatus,
    strategy_id: String,
    symbol: String,
    trade_count: i32,
    pnl: String,
    pnl_pct: String,
    max_drawdown_pct: String,
    win_rate: String,
    fee_paid: String,
    slippage_cost: String,
    correlation_id: Option<Uuid>,
}

#[derive(Serialize)]
struct FeedStatusResponse {
    feeds: Vec<MarketFeedStatusRecord>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Deserialize)]
struct RecentSignalsQuery {
    symbol: Option<String>,
    limit: Option<i64>,
}

#[derive(Serialize)]
struct RecentSignalsResponse {
    signals: Vec<SignalRecord>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct StrategyListResponse {
    strategies: Vec<StrategyStatusView>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct StrategyStatusResponse {
    strategy: StrategyStatusView,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct StrategyToggleResponse {
    strategy: StrategyStatusView,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct PaperAccountView {
    id: Uuid,
    name: String,
    base_currency: String,
    initial_equity: String,
    current_equity: String,
    realized_pnl: String,
    unrealized_pnl: String,
    status: String,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct PaperAccountResponse {
    account: PaperAccountView,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct PaperPositionView {
    id: Uuid,
    account_id: Uuid,
    symbol: String,
    side: String,
    quantity: String,
    entry_price: String,
    mark_price: Option<String>,
    price_status: String,
    notional: String,
    realized_pnl: String,
    unrealized_pnl: String,
    status: String,
    opened_at: chrono::DateTime<Utc>,
    closed_at: Option<chrono::DateTime<Utc>>,
    strategy_id: Option<String>,
    signal_id: Option<Uuid>,
    risk_decision_id: Option<Uuid>,
    order_id: Option<Uuid>,
    updated_at: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct PaperPositionsResponse {
    positions: Vec<PaperPositionView>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct PaperPositionResponse {
    position: PaperPositionView,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct PaperPnlSummaryView {
    realized_pnl: String,
    unrealized_pnl: String,
    equity: String,
    daily_pnl: String,
    drawdown_pct: String,
    price_status: String,
    open_positions_count: usize,
    calculated_at: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct PaperPnlResponse {
    pnl: PaperPnlSummaryView,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct PaperEquitySnapshotView {
    id: Uuid,
    account_id: Uuid,
    equity: String,
    realized_pnl: String,
    unrealized_pnl: String,
    drawdown_pct: String,
    snapshot_at: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct PaperEquityResponse {
    equity: Vec<PaperEquitySnapshotView>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct PaperTradeJournalView {
    id: Uuid,
    account_id: Uuid,
    position_id: Option<Uuid>,
    order_id: Option<Uuid>,
    event_type: String,
    symbol: Option<String>,
    pnl: Option<String>,
    payload: serde_json::Value,
    created_at: chrono::DateTime<Utc>,
    correlation_id: Uuid,
}

#[derive(Serialize)]
struct PaperTradeJournalResponse {
    journal: Vec<PaperTradeJournalView>,
    request_id: String,
    correlation_id: String,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
struct StrategyStatusView {
    strategy_id: String,
    status: String,
    mode: String,
    symbols: Vec<String>,
    timeframe: String,
    suggested_notional: String,
    momentum_lookback_candles: i32,
    breakout_lookback_candles: i32,
    last_evaluated_at: Option<chrono::DateTime<Utc>>,
    last_evaluation_reason: Option<String>,
    last_signal_id: Option<Uuid>,
    last_signal_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Deserialize)]
struct EvaluateStrategyRequest {
    symbol: Option<String>,
    correlation_id: Option<Uuid>,
}

#[derive(Serialize)]
struct EvaluateStrategyResponse {
    strategy_id: String,
    symbol: String,
    generated: bool,
    signal_id: Option<Uuid>,
    side: Option<String>,
    confidence: Option<String>,
    reason: String,
    source_candle_open_time: Option<chrono::DateTime<Utc>>,
    correlation_id: Uuid,
}

type RunPaperPipelineRequest = PaperTradingPipelineRequest;
type RunBacktestRequest = BacktestRequest;

#[tokio::main]
async fn main() {
    init_tracing();

    let config = AppConfig::from_env().expect("invalid application configuration");
    let db_pool = connect_pool(&DbConfig {
        database_url: config.database_url.clone(),
        max_connections: config.database_max_connections,
    })
    .await
    .expect("failed to connect to Postgres");
    ensure_system_state(&db_pool)
        .await
        .expect("failed to initialize persistent system state");

    let event_publisher = PostgresEventPublisher::new(db_pool.clone());
    let started_at = Utc::now();
    let startup_correlation_id = Uuid::new_v4();
    let market_config =
        MarketIngestConfig::from_env().expect("invalid market ingest configuration");
    let strategy_runtime =
        StrategyRuntimeConfig::from_env().expect("invalid strategy configuration");

    event_publisher
        .publish(SystemEventType::SystemStarted.into_event(
            startup_correlation_id,
            config.app_name.clone(),
            json!({
                "service": config.app_name.clone(),
                "environment": config.environment.clone(),
                "market_mode": MarketMode::Paper,
            }),
        ))
        .await
        .expect("failed to publish system.started event");

    let state = AppState {
        config: config.clone(),
        db_pool,
        started_at,
        market_mode: MarketMode::Paper,
        market_config,
        strategy_runtime,
    };

    let app = Router::new()
        .route("/system/health", get(health))
        .route("/system/status", get(status))
        .route("/system/db-health", get(db_health))
        .route("/events/recent", get(recent_events))
        .route("/events/:id", get(event_by_id))
        .route("/risk/status", get(risk_status))
        .route("/risk/decisions", get(get_risk_decisions))
        .route("/risk/decisions/:id", get(get_risk_decision))
        .route("/risk/kill-switch", post(enable_kill_switch))
        .route("/risk/resume", post(resume_trading))
        .route("/risk/evaluate", post(evaluate_risk))
        .route("/paper/orders", post(create_order))
        .route("/paper/pipeline/run", post(run_paper_pipeline_handler))
        .route("/paper/account", get(get_paper_account))
        .route(
            "/paper/account/mark-to-market",
            post(mark_paper_account_to_market),
        )
        .route("/paper/positions", get(get_paper_positions))
        .route("/paper/positions/:id", get(get_paper_position))
        .route("/paper/pnl/daily", get(get_paper_pnl_daily))
        .route("/paper/equity", get(get_paper_equity))
        .route("/paper/trade-journal", get(get_paper_trade_journal))
        .route("/backtest/run", post(run_backtest_handler))
        .route("/backtest/runs", get(get_backtest_runs))
        .route("/backtest/runs/:id", get(get_backtest_run_handler))
        .route(
            "/backtest/runs/:id/trades",
            get(get_backtest_trades_handler),
        )
        .route(
            "/backtest/runs/:id/equity",
            get(get_backtest_equity_handler),
        )
        .route("/orders", get(get_orders))
        .route("/orders/:id", get(get_order))
        .route("/market/symbols", get(get_market_symbols))
        .route("/market/ticks/latest", get(get_latest_tick))
        .route("/market/candles", get(get_market_candles))
        .route(
            "/market/backfill/candles",
            post(post_market_backfill_candles),
        )
        .route("/market/backfill/runs", get(get_market_backfill_runs))
        .route("/market/backfill/runs/:id", get(get_market_backfill_run))
        .route("/market/feed-status", get(get_market_feed_status))
        .route("/strategy/list", get(get_strategy_list))
        .route("/strategy/:id/status", get(get_strategy_by_id))
        .route("/strategy/:id/enable", post(enable_strategy))
        .route("/strategy/:id/disable", post(disable_strategy))
        .route("/strategy/:id/evaluate", post(evaluate_strategy_handler))
        .route("/signals/recent", get(get_recent_signals))
        .layer(middleware::from_fn(request_context_middleware))
        .with_state(state);

    info!(
        service = %config.app_name,
        environment = %config.environment,
        bind_addr = %config.bind_addr,
        db_max_connections = config.database_max_connections,
        "starting api server"
    );
    info!("TODO: add authn/authz boundary before non-internal exposure");

    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .expect("failed to bind TCP listener");

    axum::serve(listener, app).await.expect("api server failed");
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "info,axum=info,tower_http=info".into());

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .init();
}

async fn request_context_middleware(mut request: Request, next: Next) -> Response {
    let started_at = Instant::now();
    let request_id = get_or_create_header(request.headers(), &REQUEST_ID_HEADER);
    let correlation_id = request
        .headers()
        .get(&CORRELATION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| request_id.clone());

    request.extensions_mut().insert(RequestContext {
        request_id: request_id.clone(),
        correlation_id: correlation_id.clone(),
    });

    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let mut response = next.run(request).await;

    response.headers_mut().insert(
        REQUEST_ID_HEADER.clone(),
        HeaderValue::from_str(&request_id).expect("generated request id must be a valid header"),
    );
    response.headers_mut().insert(
        CORRELATION_ID_HEADER.clone(),
        HeaderValue::from_str(&correlation_id)
            .expect("generated correlation id must be a valid header"),
    );

    info!(
        request_id = %request_id,
        correlation_id = %correlation_id,
        method = %method,
        path = %path,
        status = response.status().as_u16(),
        latency_ms = started_at.elapsed().as_millis(),
        "request completed"
    );

    response
}

fn get_or_create_header(headers: &axum::http::HeaderMap, name: &HeaderName) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

fn request_context(request: Option<Extension<RequestContext>>) -> RequestContext {
    request
        .map(|Extension(value)| value)
        .unwrap_or(RequestContext {
            request_id: Uuid::new_v4().to_string(),
            correlation_id: Uuid::new_v4().to_string(),
        })
}

fn bounded_recent_events_limit(limit: Option<i64>) -> i64 {
    match limit {
        Some(value) if value > 0 => value.min(MAX_RECENT_EVENTS_LIMIT),
        _ => DEFAULT_RECENT_EVENTS_LIMIT,
    }
}

fn bounded_limit(limit: Option<i64>) -> i64 {
    bounded_recent_events_limit(limit)
}

fn bounded_risk_decisions_limit(limit: Option<i64>) -> i64 {
    match limit {
        Some(value) if value > 0 => value.min(MAX_RISK_DECISIONS_LIMIT),
        _ => DEFAULT_RISK_DECISIONS_LIMIT,
    }
}

fn bounded_candle_limit(limit: Option<i64>) -> i64 {
    match limit {
        Some(value) if value > 0 => value.min(MAX_CANDLE_LIMIT),
        _ => DEFAULT_CANDLE_LIMIT,
    }
}

fn bounded_backfill_runs_limit(limit: Option<i64>) -> i64 {
    match limit {
        Some(value) if value > 0 => value.min(MAX_BACKFILL_RUNS_LIMIT),
        _ => DEFAULT_BACKFILL_RUNS_LIMIT,
    }
}

fn bounded_paper_limit(limit: Option<i64>) -> i64 {
    match limit {
        Some(value) if value > 0 => value.min(MAX_PAPER_LIMIT),
        _ => DEFAULT_PAPER_LIMIT,
    }
}

fn parse_strategy_id(value: &str) -> Result<StrategyId, aegis_core::CoreError> {
    value.parse()
}

fn default_strategy_symbol(config: &StrategyConfig) -> Symbol {
    config
        .symbols
        .first()
        .cloned()
        .expect("strategy default symbols must not be empty")
}

async fn ensure_strategy_configs(state: &AppState) -> Result<Vec<StrategyConfig>, anyhow::Error> {
    let mut configs = Vec::new();
    for config in state.strategy_runtime.default_configs() {
        let record = upsert_strategy_config(&state.db_pool, &config).await?;
        configs.push(strategy_config_from_record(&record)?);
    }

    Ok(configs)
}

async fn ensure_strategy_config(
    state: &AppState,
    strategy_id: StrategyId,
) -> Result<StrategyConfig, anyhow::Error> {
    let _ = ensure_strategy_configs(state).await?;
    let record = get_strategy_status(&state.db_pool, strategy_id)
        .await?
        .map(|status| status.config)
        .ok_or_else(|| anyhow::anyhow!("strategy config not found after initialization"))?;
    Ok(strategy_config_from_record(&record)?)
}

fn strategy_status_view(record: StrategyStatusRecord) -> StrategyStatusView {
    let state = record.state;
    StrategyStatusView {
        strategy_id: record.config.strategy_id,
        status: record.config.status,
        mode: record.config.mode,
        symbols: record
            .config
            .symbols
            .split(',')
            .filter(|value| !value.trim().is_empty())
            .map(ToOwned::to_owned)
            .collect(),
        timeframe: record.config.timeframe,
        suggested_notional: record.config.suggested_notional.to_string(),
        momentum_lookback_candles: record.config.momentum_lookback_candles,
        breakout_lookback_candles: record.config.breakout_lookback_candles,
        last_evaluated_at: state.as_ref().and_then(|state| state.last_evaluated_at),
        last_evaluation_reason: state
            .as_ref()
            .and_then(|state| state.last_evaluation_reason.clone()),
        last_signal_id: state.as_ref().and_then(|state| state.last_signal_id),
        last_signal_at: state.as_ref().and_then(|state| state.last_signal_at),
    }
}

fn candle_backfill_result(
    record: &CandleBackfillRunRecord,
) -> Result<CandleBackfillResult, anyhow::Error> {
    Ok(candle_backfill_result_from_record(record)?)
}

fn evaluate_strategy_response(
    strategy_id: StrategyId,
    symbol: &Symbol,
    outcome: Option<&InsertSignalOutcome>,
    generated: bool,
    reason: SignalReason,
    correlation_id: Uuid,
) -> EvaluateStrategyResponse {
    match outcome {
        Some(outcome) => EvaluateStrategyResponse {
            strategy_id: strategy_id.to_string(),
            symbol: symbol.as_str().to_string(),
            generated,
            signal_id: Some(outcome.signal.id),
            side: Some(outcome.signal.side.clone()),
            confidence: Some(outcome.signal.confidence.to_string()),
            reason: outcome.signal.reason.clone(),
            source_candle_open_time: Some(outcome.signal.source_candle_open_time),
            correlation_id,
        },
        None => EvaluateStrategyResponse {
            strategy_id: strategy_id.to_string(),
            symbol: symbol.as_str().to_string(),
            generated: false,
            signal_id: None,
            side: None,
            confidence: None,
            reason: reason.as_str().to_string(),
            source_candle_open_time: None,
            correlation_id,
        },
    }
}

fn is_valid_resume_confirmation(value: &str) -> bool {
    value.trim() == "RESUME TRADING"
}

fn default_actor() -> StateActor {
    StateActor::system("anonymous")
}

fn parse_correlation_id_filter(value: Option<&str>) -> Result<Option<Uuid>, String> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        Some(raw) => Uuid::parse_str(raw)
            .map(Some)
            .map_err(|err| format!("invalid correlation_id: {err}")),
        None => Ok(None),
    }
}

fn map_kill_switch(state: SystemStateRecord) -> KillSwitchResponse {
    KillSwitchResponse {
        enabled: state.kill_switch_enabled,
        reason: state.kill_switch_reason,
        updated_at: state.updated_at,
        updated_by: ActorResponse {
            actor: state.updated_by_actor,
            actor_id: state.updated_by_actor_id,
        },
        last_correlation_id: state.last_correlation_id,
    }
}

fn risk_status_response(
    state: &AppState,
    request: RequestContext,
    system_state: SystemStateRecord,
) -> RiskStatusResponse {
    let kill_switch = map_kill_switch(system_state);

    RiskStatusResponse {
        status: "ok",
        market_mode: state.market_mode,
        paper_trading_allowed: !kill_switch.enabled,
        live_trading_allowed: false,
        resume_confirmation_required: "RESUME TRADING",
        kill_switch,
        request_id: request.request_id,
        correlation_id: request.correlation_id,
        timestamp: Utc::now(),
    }
}

fn risk_action_response(
    state: &AppState,
    request: RequestContext,
    message: String,
    system_state: SystemStateRecord,
) -> RiskActionResponse {
    let kill_switch = map_kill_switch(system_state);

    RiskActionResponse {
        status: "ok",
        message,
        market_mode: state.market_mode,
        paper_trading_allowed: !kill_switch.enabled,
        live_trading_allowed: false,
        kill_switch,
        request_id: request.request_id,
        correlation_id: request.correlation_id,
        timestamp: Utc::now(),
    }
}

fn risk_decision_not_found_error(request: &RequestContext) -> ErrorResponse {
    ErrorResponse {
        error: "risk_decision_not_found",
        message: "Risk decision was not found.".to_string(),
        request_id: request.request_id.clone(),
        correlation_id: request.correlation_id.clone(),
        timestamp: Utc::now(),
    }
}

fn risk_decision_view(record: RiskDecisionRecord) -> RiskDecisionView {
    RiskDecisionView {
        id: record.risk_decision_id,
        signal_id: record.signal_id,
        decision: record.decision,
        approved_notional: record.approved_notional.map(|value| value.to_string()),
        risk_score: record.risk_score.map(|value| value.to_string()),
        reasons: record.reasons,
        created_at: record.created_at,
        correlation_id: record.correlation_id,
        strategy_id: record.strategy_id,
        symbol: record.symbol,
    }
}

fn order_view(record: OrderRecord) -> OrderView {
    OrderView {
        order_id: record.order_id,
        client_order_id: record.client_order_id,
        exchange_order_id: record.exchange_order_id,
        signal_id: record.signal_id,
        risk_decision_id: record.risk_decision_id,
        strategy_id: record.strategy_id,
        symbol: record.symbol,
        side: record.side,
        status: record.status,
        execution_state: record.execution_state,
        idempotency_key: record.idempotency_key,
        requested_notional: record.requested_notional.map(|value| value.to_string()),
        quantity: record.quantity.to_string(),
        filled_qty: record.filled_qty.to_string(),
        limit_price: record.limit_price.map(|value| value.to_string()),
        filled_price: record.filled_price.map(|value| value.to_string()),
        avg_fill_price: record.avg_fill_price.map(|value| value.to_string()),
        mode: record.mode,
        market_mode: record.market_mode,
        status_reason: record.status_reason,
        correlation_id: record.correlation_id,
        submitted_at: record.submitted_at,
        filled_at: record.filled_at,
        cancelled_at: record.cancelled_at,
        rejected_at: record.rejected_at,
        expired_at: record.expired_at,
        expires_at: record.expires_at,
        created_at: record.created_at,
        updated_at: record.updated_at,
    }
}

fn paper_account_view(record: PaperAccountRecord) -> PaperAccountView {
    PaperAccountView {
        id: record.id,
        name: record.name,
        base_currency: record.base_currency,
        initial_equity: record.initial_equity.to_string(),
        current_equity: record.current_equity.to_string(),
        realized_pnl: record.realized_pnl.to_string(),
        unrealized_pnl: record.unrealized_pnl.to_string(),
        status: record.status,
        created_at: record.created_at,
        updated_at: record.updated_at,
    }
}

fn paper_position_view(record: PaperPositionRecord) -> PaperPositionView {
    PaperPositionView {
        id: record.id,
        account_id: record.account_id,
        symbol: record.symbol,
        side: record.side,
        quantity: record.quantity.to_string(),
        entry_price: record.entry_price.to_string(),
        mark_price: record.mark_price.map(|value| value.to_string()),
        price_status: record.price_status,
        notional: record.notional.to_string(),
        realized_pnl: record.realized_pnl.to_string(),
        unrealized_pnl: record.unrealized_pnl.to_string(),
        status: record.status,
        opened_at: record.opened_at,
        closed_at: record.closed_at,
        strategy_id: record.strategy_id,
        signal_id: record.signal_id,
        risk_decision_id: record.risk_decision_id,
        order_id: record.order_id,
        updated_at: record.updated_at,
    }
}

fn paper_equity_snapshot_view(record: PaperEquitySnapshotRecord) -> PaperEquitySnapshotView {
    PaperEquitySnapshotView {
        id: record.id,
        account_id: record.account_id,
        equity: record.equity.to_string(),
        realized_pnl: record.realized_pnl.to_string(),
        unrealized_pnl: record.unrealized_pnl.to_string(),
        drawdown_pct: record.drawdown_pct.to_string(),
        snapshot_at: record.snapshot_at,
    }
}

fn paper_trade_journal_view(record: PaperTradeJournalRecord) -> PaperTradeJournalView {
    PaperTradeJournalView {
        id: record.id,
        account_id: record.account_id,
        position_id: record.position_id,
        order_id: record.order_id,
        event_type: record.event_type,
        symbol: record.symbol,
        pnl: record.pnl.map(|value| value.to_string()),
        payload: record.payload,
        created_at: record.created_at,
        correlation_id: record.correlation_id,
    }
}

async fn health(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> Json<HealthResponse> {
    let request = request_context(request);

    Json(HealthResponse {
        status: "ok",
        service: state.config.app_name,
        environment: state.config.environment,
        request_id: request.request_id,
        correlation_id: request.correlation_id,
        timestamp: Utc::now(),
    })
}

async fn status(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> Json<StatusResponse> {
    let request = request_context(request);

    Json(StatusResponse {
        service: state.config.app_name,
        environment: state.config.environment,
        market_mode: state.market_mode,
        started_at: state.started_at,
        request_id: request.request_id,
        correlation_id: request.correlation_id,
        timestamp: Utc::now(),
        dependencies: Dependencies {
            database: DependencyStatus {
                status: "configured",
            },
            event_bus: DependencyStatus {
                status: "configured",
            },
            exchange_execution: DependencyStatus { status: "disabled" },
        },
    })
}

async fn db_health(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match check_health(&state.db_pool).await {
        Ok(()) => (
            StatusCode::OK,
            Json(DbHealthResponse {
                status: "ok",
                service: state.config.app_name,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "database health check failed"
            );

            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(DbHealthResponse {
                    status: "error",
                    service: state.config.app_name,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn recent_events(
    State(state): State<AppState>,
    Query(query): Query<RecentEventsQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let limit = bounded_recent_events_limit(query.limit);
    let correlation_id = match parse_correlation_id_filter(query.correlation_id.as_deref()) {
        Ok(value) => value,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_correlation_id",
                    message,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    match list_recent_system_events_filtered(
        &state.db_pool,
        limit,
        query.event_type.as_deref(),
        query.source.as_deref(),
        correlation_id,
    )
    .await
    {
        Ok(events) => (
            StatusCode::OK,
            Json(RecentEventsResponse {
                events,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to query recent system events"
            );

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_events",
                    message: "Failed to query recent system events.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn get_risk_decisions(
    State(state): State<AppState>,
    Query(query): Query<RiskDecisionsQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let limit = bounded_risk_decisions_limit(query.limit);

    match list_recent_risk_decisions_filtered(&state.db_pool, query.symbol.as_deref(), limit).await
    {
        Ok(decisions) => (
            StatusCode::OK,
            Json(RiskDecisionsResponse {
                decisions: decisions.into_iter().map(risk_decision_view).collect(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to query risk decisions"
            );

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_risk_decisions",
                    message: "Failed to query persisted risk decisions.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn get_risk_decision(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match get_risk_decision_by_id(&state.db_pool, id).await {
        Ok(Some(decision)) => (
            StatusCode::OK,
            Json(RiskDecisionResponse {
                decision: risk_decision_view(decision),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(risk_decision_not_found_error(&request)),
        )
            .into_response(),
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                risk_decision_id = %id,
                error = %err,
                "failed to query risk decision"
            );

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_risk_decision",
                    message: "Failed to query the requested risk decision.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn event_by_id(
    State(state): State<AppState>,
    Path(event_id): Path<Uuid>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match get_system_event(&state.db_pool, event_id).await {
        Ok(Some(event)) => (
            StatusCode::OK,
            Json(EventResponse {
                event,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "event_not_found",
                message: "System event was not found.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                event_id = %event_id,
                error = %err,
                "failed to query system event"
            );

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_event",
                    message: "Failed to query the requested system event.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn risk_status(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match get_system_state(&state.db_pool).await {
        Ok(system_state) => (
            StatusCode::OK,
            Json(risk_status_response(&state, request, system_state)),
        )
            .into_response(),
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to query risk status"
            );

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_risk_status",
                    message: "Failed to load persistent risk status from the database.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn enable_kill_switch(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Json(payload): Json<KillSwitchRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let correlation_id = parse_correlation_id(&request.correlation_id);

    match set_kill_switch_state(
        &state.db_pool,
        &default_actor(),
        correlation_id,
        &state.config.app_name,
        true,
        payload.reason,
    )
    .await
    {
        Ok(system_state) => (
            StatusCode::OK,
            Json(risk_action_response(
                &state,
                request,
                "Kill switch is active. Paper order execution must remain stopped.".to_string(),
                system_state,
            )),
        )
            .into_response(),
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to activate kill switch"
            );

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_activate_kill_switch",
                    message: "Kill switch activation failed because the database is unavailable or the write could not be completed.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn resume_trading(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Json(payload): Json<ResumeRequest>,
) -> impl IntoResponse {
    let request = request_context(request);

    if !is_valid_resume_confirmation(&payload.confirmation_text) {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "invalid_resume_confirmation",
                message: "Resume requires confirmation_text exactly equal to \"RESUME TRADING\"."
                    .to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    let correlation_id = parse_correlation_id(&request.correlation_id);

    match set_kill_switch_state(
        &state.db_pool,
        &default_actor(),
        correlation_id,
        &state.config.app_name,
        false,
        payload.reason,
    )
    .await
    {
        Ok(system_state) => (
            StatusCode::OK,
            Json(risk_action_response(
                &state,
                request,
                "Kill switch is disabled. Paper trading may resume through the normal risk pipeline."
                    .to_string(),
                system_state,
            )),
        )
            .into_response(),
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to resume trading"
            );

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_resume_trading",
                    message: "Resume failed because the database is unavailable or the write could not be completed.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

fn parse_correlation_id(value: &str) -> Uuid {
    Uuid::parse_str(value).unwrap_or_else(|_| Uuid::new_v4())
}

fn reason_code(reason: RiskRejectionReason) -> &'static str {
    match reason {
        RiskRejectionReason::KillSwitchActive => "kill_switch_active",
        RiskRejectionReason::MaxOpenPositionsExceeded => "max_open_positions_exceeded",
        RiskRejectionReason::MaxDailyLossExceeded => "max_daily_loss_exceeded",
        RiskRejectionReason::SignalTooOld => "signal_too_old",
        RiskRejectionReason::DuplicateOrderDetected => "duplicate_order_detected",
        RiskRejectionReason::DataStale => "data_stale",
        RiskRejectionReason::PositionNotionalExceeded => "position_notional_exceeded",
        RiskRejectionReason::UnsupportedState => "unsupported_state",
    }
}

fn risk_evaluate_response(result: &RiskEvaluationResult) -> RiskEvaluateResponse {
    RiskEvaluateResponse {
        decision: match result.decision {
            RiskEvaluationDecision::Approved => "APPROVED",
            RiskEvaluationDecision::Rejected => "REJECTED",
        },
        approved_notional: result.approved_notional.map(|value| value.to_string()),
        risk_score: result.risk_score.to_string(),
        reasons: result
            .reasons
            .iter()
            .map(|reason| reason_code(*reason).to_string())
            .collect(),
        correlation_id: result.correlation_id,
    }
}

fn parse_risk_check_context(
    payload: RiskEvaluateRequest,
    request_correlation_id: &str,
) -> Result<RiskCheckContext, &'static str> {
    let suggested_notional = Decimal::from_str_exact(&payload.suggested_notional)
        .map_err(|_| "invalid_suggested_notional")?;
    let symbol = Symbol::new(payload.symbol).map_err(|_| "invalid_symbol")?;

    Ok(RiskCheckContext {
        signal_id: payload.signal_id,
        correlation_id: payload
            .correlation_id
            .unwrap_or_else(|| parse_correlation_id(request_correlation_id)),
        strategy_id: payload.strategy_id,
        symbol,
        side: payload.side,
        suggested_notional,
        signal_created_at: payload.signal_created_at,
        evaluated_at: Utc::now(),
    })
}

async fn evaluate_risk(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Json(payload): Json<RiskEvaluateRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let context = match parse_risk_check_context(payload, &request.correlation_id) {
        Ok(context) => context,
        Err("invalid_suggested_notional") => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_suggested_notional",
                    message: "suggested_notional must be a valid decimal string.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
        Err("invalid_symbol") => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_symbol",
                    message: "symbol must be a non-empty market symbol.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_risk_request",
                    message: "Risk evaluation request is invalid.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    let snapshot = match load_risk_state_snapshot(&state.db_pool).await {
        Ok(snapshot) => snapshot,
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to load risk state snapshot"
            );

            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_load_risk_state",
                    message: "Failed to load risk state from the database.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    let evaluator = RiskEvaluator::new(aegis_core::RiskConfig::default());
    let evaluation = evaluator.evaluate(&context, &snapshot);

    if let Err(err) = insert_risk_evaluation(
        &state.db_pool,
        &state.config.app_name,
        &context,
        &evaluation,
    )
    .await
    {
        error!(
            request_id = %request.request_id,
            correlation_id = %request.correlation_id,
            signal_id = %context.signal_id,
            error = %err,
            "failed to persist risk evaluation"
        );

        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_persist_risk_evaluation",
                message: "Risk evaluation could not be persisted transactionally.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    (StatusCode::OK, Json(risk_evaluate_response(&evaluation))).into_response()
}

fn parse_order_intent(
    payload: CreatePaperOrderRequest,
    request_correlation_id: &str,
) -> Result<OrderIntent, &'static str> {
    let quantity = Decimal::from_str_exact(&payload.quantity).map_err(|_| "invalid_quantity")?;
    let limit_price = match payload.limit_price {
        Some(value) => Some(Decimal::from_str_exact(&value).map_err(|_| "invalid_limit_price")?),
        None => None,
    };
    let symbol = Symbol::new(payload.symbol).map_err(|_| "invalid_symbol")?;

    Ok(OrderIntent {
        order_id: Uuid::new_v4(),
        correlation_id: payload
            .correlation_id
            .unwrap_or_else(|| parse_correlation_id(request_correlation_id)),
        risk_decision_id: payload.risk_decision_id,
        idempotency_key: payload.idempotency_key,
        symbol,
        side: payload.side,
        quantity,
        limit_price,
        created_at: Utc::now(),
        expires_at: payload.expires_at,
    })
}

async fn create_order(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Json(payload): Json<CreatePaperOrderRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let intent = match parse_order_intent(payload, &request.correlation_id) {
        Ok(intent) => intent,
        Err("invalid_quantity") => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_quantity",
                    message: "quantity must be a valid decimal string greater than zero."
                        .to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
        Err("invalid_limit_price") => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_limit_price",
                    message: "limit_price must be a valid decimal string greater than zero."
                        .to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
        Err("invalid_symbol") => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_symbol",
                    message: "symbol must be a non-empty market symbol.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_order_request",
                    message: "Paper order request is invalid.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    match create_paper_order(
        &state.db_pool,
        &state.config.app_name,
        &default_actor(),
        intent,
    )
    .await
    {
        Ok(outcome) => {
            if let Err(err) = persist_paper_fill_accounting(&state.db_pool, &outcome.order).await {
                error!(
                    request_id = %request.request_id,
                    correlation_id = %request.correlation_id,
                    error = %err,
                    "failed to persist paper accounting after direct paper order creation"
                );

                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "failed_to_persist_paper_accounting",
                        message: "Paper accounting artifacts could not be persisted.".to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response();
            }

            (
                StatusCode::CREATED,
                Json(OrderResponse {
                    order: order_view(outcome.order),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
        Err(CreateOrderError::RiskDecisionNotFound) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "risk_decision_not_found",
                message: "risk_decision_id must reference an existing persisted risk decision."
                    .to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(CreateOrderError::RiskDecisionNotApproved) => (
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: "risk_decision_not_approved",
                message: "Only APPROVED risk decisions may create paper orders.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(CreateOrderError::DuplicateIdempotencyKey) => (
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: "duplicate_idempotency_key",
                message: "idempotency_key must be unique for each paper order.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(CreateOrderError::InvalidIntent(message)) => (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "invalid_order_intent",
                message,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(CreateOrderError::Unexpected(err)) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to create paper order"
            );

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_create_order",
                    message: "Paper order could not be persisted transactionally.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn run_paper_pipeline_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Json(mut payload): Json<RunPaperPipelineRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    if payload.correlation_id.is_none() {
        payload.correlation_id = Some(parse_correlation_id(&request.correlation_id));
    }

    match pipeline::run_paper_pipeline(&state, payload).await {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(err) => {
            let message = err.to_string();
            let error_code = if message.contains("invalid strategy_id") {
                "invalid_strategy_id"
            } else if message.contains("invalid symbol") {
                "invalid_symbol"
            } else if message.contains("invalid timeframe") {
                "invalid_timeframe"
            } else {
                "failed_to_run_paper_pipeline"
            };
            let status = if error_code == "failed_to_run_paper_pipeline" {
                StatusCode::INTERNAL_SERVER_ERROR
            } else {
                StatusCode::BAD_REQUEST
            };

            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to run paper trading pipeline"
            );

            (
                status,
                Json(ErrorResponse {
                    error: error_code,
                    message,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn run_backtest_handler(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Json(mut payload): Json<RunBacktestRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    if payload.correlation_id.is_none() {
        payload.correlation_id = Some(parse_correlation_id(&request.correlation_id));
    }

    let engine = ReplayEngine::new(state.db_pool.clone(), state.config.app_name.clone());
    match engine.run_backtest(payload).await {
        Ok(execution) => (
            StatusCode::OK,
            Json(BacktestRunAcceptedResponse {
                run_id: execution.result.run_id,
                status: execution.result.status,
                strategy_id: execution.result.strategy_id,
                symbol: execution.result.symbol,
                trade_count: execution.result.trade_count,
                pnl: execution.result.pnl.to_string(),
                pnl_pct: execution.result.pnl_pct.to_string(),
                max_drawdown_pct: execution.result.max_drawdown_pct.to_string(),
                win_rate: execution.result.win_rate.to_string(),
                fee_paid: execution.result.fee_paid.to_string(),
                slippage_cost: execution.result.slippage_cost.to_string(),
                correlation_id: execution.result.correlation_id,
            }),
        )
            .into_response(),
        Err(err) => {
            let message = err.to_string();
            let status = if message.contains("invalid")
                || message.contains("unsupported")
                || message.contains("cannot be empty")
                || message.contains("must be")
            {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };

            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to run backtest"
            );

            (
                status,
                Json(ErrorResponse {
                    error: "failed_to_run_backtest",
                    message,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn get_backtest_runs(
    State(state): State<AppState>,
    Query(query): Query<BacktestRunsQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    match list_backtest_runs(&state.db_pool, bounded_limit(query.limit)).await {
        Ok(runs) => {
            let mut mapped = Vec::with_capacity(runs.len());
            for run in runs {
                match backtest_result_from_record(&run) {
                    Ok(result) => mapped.push(result),
                    Err(err) => {
                        error!(
                            request_id = %request.request_id,
                            correlation_id = %request.correlation_id,
                            error = %err,
                            run_id = %run.id,
                            "failed to map backtest run"
                        );
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ErrorResponse {
                                error: "failed_to_map_backtest_run",
                                message: "Backtest run could not be decoded.".to_string(),
                                request_id: request.request_id,
                                correlation_id: request.correlation_id,
                                timestamp: Utc::now(),
                            }),
                        )
                            .into_response();
                    }
                }
            }

            (
                StatusCode::OK,
                Json(BacktestRunsResponse {
                    runs: mapped,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to list backtest runs"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_backtest_runs",
                    message: "Failed to query backtest runs.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn get_backtest_run_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let run_id = match id.parse::<Uuid>() {
        Ok(run_id) => run_id,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_run_id",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    match get_backtest_run(&state.db_pool, run_id).await {
        Ok(Some(run)) => match backtest_result_from_record(&run) {
            Ok(result) => (
                StatusCode::OK,
                Json(BacktestRunResponse {
                    run: result,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response(),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_map_backtest_run",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response(),
        },
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "backtest_run_not_found",
                message: "Backtest run was not found.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_backtest_run",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_backtest_trades_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let run_id = match id.parse::<Uuid>() {
        Ok(run_id) => run_id,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_run_id",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    match get_backtest_trades(&state.db_pool, run_id).await {
        Ok(trades) => (
            StatusCode::OK,
            Json(BacktestTradesResponse {
                trades,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_backtest_trades",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_backtest_equity_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let run_id = match id.parse::<Uuid>() {
        Ok(run_id) => run_id,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_run_id",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    match get_backtest_equity_curve(&state.db_pool, run_id).await {
        Ok(equity) => (
            StatusCode::OK,
            Json(BacktestEquityCurveResponse {
                equity,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_backtest_equity",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_orders(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match list_orders(&state.db_pool).await {
        Ok(orders) => (
            StatusCode::OK,
            Json(OrdersResponse {
                orders: orders.into_iter().map(order_view).collect(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to list orders"
            );

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_orders",
                    message: "Failed to query persisted orders.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn get_order(
    State(state): State<AppState>,
    Path(order_id): Path<Uuid>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match get_order_by_id(&state.db_pool, order_id).await {
        Ok(Some(order)) => (
            StatusCode::OK,
            Json(OrderResponse {
                order: order_view(order),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "order_not_found",
                message: "Order was not found.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                order_id = %order_id,
                error = %err,
                "failed to query order"
            );

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_order",
                    message: "Failed to query the requested order.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn load_or_create_default_paper_account_record(
    pool: &PgPool,
) -> anyhow::Result<PaperAccountRecord> {
    if let Some(account) = get_default_paper_account(pool).await? {
        return Ok(account);
    }
    ensure_default_paper_account(pool).await?;
    get_default_paper_account(pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("default paper account missing after creation"))
}

async fn get_paper_account(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match load_or_create_default_paper_account_record(&state.db_pool).await {
        Ok(account) => (
            StatusCode::OK,
            Json(PaperAccountResponse {
                account: paper_account_view(account),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_paper_account",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_paper_positions(
    State(state): State<AppState>,
    Query(query): Query<PaperListQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match load_or_create_default_paper_account_record(&state.db_pool).await {
        Ok(account) => {
            match list_paper_positions(&state.db_pool, account.id, bounded_paper_limit(query.limit))
                .await
            {
                Ok(positions) => (
                    StatusCode::OK,
                    Json(PaperPositionsResponse {
                        positions: positions.into_iter().map(paper_position_view).collect(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response(),
                Err(err) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "failed_to_query_paper_positions",
                        message: err.to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response(),
            }
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_paper_account",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_paper_position(
    State(state): State<AppState>,
    Path(position_id): Path<Uuid>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match load_or_create_default_paper_account_record(&state.db_pool).await {
        Ok(account) => {
            match get_paper_position_by_id(&state.db_pool, account.id, position_id).await {
                Ok(Some(position)) => (
                    StatusCode::OK,
                    Json(PaperPositionResponse {
                        position: paper_position_view(position),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response(),
                Ok(None) => (
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse {
                        error: "paper_position_not_found",
                        message: "Paper position was not found.".to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response(),
                Err(err) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "failed_to_query_paper_position",
                        message: err.to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response(),
            }
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_paper_account",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_paper_pnl_daily(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match load_or_create_default_paper_account_record(&state.db_pool).await {
        Ok(account) => {
            let positions = list_open_paper_positions(&state.db_pool, account.id).await;
            let equity = list_paper_equity_snapshots(
                &state.db_pool,
                account.id,
                bounded_paper_limit(Some(500)),
            )
            .await;
            match (positions, equity) {
                (Ok(positions), Ok(equity)) => {
                    let today = Utc::now().date_naive();
                    let snapshots = equity
                        .iter()
                        .map(paper_equity_snapshot_from_record)
                        .collect::<Vec<_>>();
                    let daily_pnl = compute_daily_pnl(&snapshots, today).unwrap_or_default();
                    let peak_equity = equity
                        .iter()
                        .map(|point| point.equity)
                        .max()
                        .unwrap_or(account.current_equity);
                    let drawdown = compute_drawdown(account.current_equity, peak_equity);
                    (
                        StatusCode::OK,
                        Json(PaperPnlResponse {
                            pnl: PaperPnlSummaryView {
                                realized_pnl: account.realized_pnl.to_string(),
                                unrealized_pnl: account.unrealized_pnl.to_string(),
                                equity: account.current_equity.to_string(),
                                daily_pnl: daily_pnl.to_string(),
                                drawdown_pct: drawdown.to_string(),
                                price_status: if positions
                                    .iter()
                                    .any(|position| position.price_status == "missing")
                                {
                                    "missing".to_string()
                                } else {
                                    "live".to_string()
                                },
                                open_positions_count: positions.len(),
                                calculated_at: Utc::now(),
                            },
                            request_id: request.request_id,
                            correlation_id: request.correlation_id,
                            timestamp: Utc::now(),
                        }),
                    )
                        .into_response()
                }
                (Err(err), _) | (_, Err(err)) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "failed_to_query_paper_pnl",
                        message: err.to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response(),
            }
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_paper_account",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_paper_equity(
    State(state): State<AppState>,
    Query(query): Query<PaperListQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match load_or_create_default_paper_account_record(&state.db_pool).await {
        Ok(account) => match list_paper_equity_snapshots(
            &state.db_pool,
            account.id,
            bounded_paper_limit(query.limit),
        )
        .await
        {
            Ok(equity) => (
                StatusCode::OK,
                Json(PaperEquityResponse {
                    equity: equity.into_iter().map(paper_equity_snapshot_view).collect(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response(),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_paper_equity",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response(),
        },
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_paper_account",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn get_paper_trade_journal(
    State(state): State<AppState>,
    Query(query): Query<PaperListQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match load_or_create_default_paper_account_record(&state.db_pool).await {
        Ok(account) => match list_paper_trade_journal(
            &state.db_pool,
            account.id,
            bounded_paper_limit(query.limit),
        )
        .await
        {
            Ok(journal) => (
                StatusCode::OK,
                Json(PaperTradeJournalResponse {
                    journal: journal.into_iter().map(paper_trade_journal_view).collect(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response(),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_paper_trade_journal",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response(),
        },
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_query_paper_account",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
    }
}

async fn mark_paper_account_to_market(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    let account_record = match load_or_create_default_paper_account_record(&state.db_pool).await {
        Ok(account) => account,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_paper_account",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    };

    let account = match paper_account_from_record(&account_record) {
        Ok(account) => account,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_map_paper_account",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    };

    let open_positions = match list_open_paper_positions(&state.db_pool, account.id).await {
        Ok(positions) => positions,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_paper_positions",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    };

    let mut domain_positions = Vec::with_capacity(open_positions.len());
    let mut prices = Vec::with_capacity(open_positions.len());
    for position in &open_positions {
        let domain_position = match paper_position_from_record(position) {
            Ok(position) => position,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "failed_to_map_paper_position",
                        message: err.to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response()
            }
        };
        let symbol = match Symbol::new(position.symbol.clone()) {
            Ok(symbol) => symbol,
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "invalid_symbol",
                        message: err.to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response()
            }
        };

        let latest_tick =
            get_latest_market_tick(&state.db_pool, state.market_config.exchange, &symbol)
                .await
                .ok()
                .flatten();
        let latest_candle = list_candles(
            &state.db_pool,
            state.market_config.exchange,
            &symbol,
            CandleInterval::OneMinute,
            1,
        )
        .await
        .ok()
        .and_then(|candles| candles.into_iter().next());
        let now = Utc::now();
        let price_input = if let Some(tick) = latest_tick {
            let is_stale = now
                .signed_duration_since(tick.received_at)
                .to_std()
                .ok()
                .map(|age| age > state.market_config.stale_threshold)
                .unwrap_or(false);
            PaperMarkPriceInput {
                symbol: position.symbol.clone(),
                mark_price: Some(tick.price),
                priced_at: Some(tick.received_at),
                price_status: if is_stale {
                    PaperPriceStatus::Stale
                } else {
                    PaperPriceStatus::Live
                },
            }
        } else if let Some(candle) = latest_candle {
            let is_stale = now
                .signed_duration_since(candle.close_time)
                .to_std()
                .ok()
                .map(|age| age > state.market_config.stale_threshold)
                .unwrap_or(true);
            PaperMarkPriceInput {
                symbol: position.symbol.clone(),
                mark_price: Some(candle.close),
                priced_at: Some(candle.close_time),
                price_status: if is_stale {
                    PaperPriceStatus::Stale
                } else {
                    PaperPriceStatus::Live
                },
            }
        } else {
            PaperMarkPriceInput {
                symbol: position.symbol.clone(),
                mark_price: None,
                priced_at: None,
                price_status: PaperPriceStatus::Missing,
            }
        };
        domain_positions.push(domain_position);
        prices.push(price_input);
    }

    let snapshot_at = Utc::now();
    let marked = mark_positions_to_market(&account, &domain_positions, &prices, snapshot_at);
    for position in &marked.positions {
        if let Err(err) = upsert_paper_position(&state.db_pool, position).await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_update_paper_position",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    }
    if let Err(err) = insert_paper_account(
        &state.db_pool,
        &aegis_core::PaperAccount {
            current_equity: marked.summary.equity,
            realized_pnl: account.realized_pnl,
            unrealized_pnl: marked.summary.unrealized_pnl,
            updated_at: snapshot_at,
            ..account.clone()
        },
    )
    .await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_update_paper_account",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }
    if let Err(err) = insert_paper_equity_snapshot(&state.db_pool, &marked.snapshot).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_insert_paper_equity_snapshot",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }
    let _ = insert_system_event(
        &state.db_pool,
        &events::EventEnvelope::new(
            "paper.equity.updated",
            Uuid::parse_str(&request.correlation_id).unwrap_or_else(|_| Uuid::new_v4()),
            &state.config.app_name,
            json!({
                "account_id": account.id,
                "equity": marked.summary.equity,
                "realized_pnl": marked.summary.realized_pnl,
                "unrealized_pnl": marked.summary.unrealized_pnl,
                "missing_symbols": marked.missing_symbols,
            }),
        ),
    )
    .await;

    (
        StatusCode::OK,
        Json(PaperPnlResponse {
            pnl: PaperPnlSummaryView {
                realized_pnl: marked.summary.realized_pnl.to_string(),
                unrealized_pnl: marked.summary.unrealized_pnl.to_string(),
                equity: marked.summary.equity.to_string(),
                daily_pnl: marked.summary.daily_pnl.to_string(),
                drawdown_pct: marked.snapshot.drawdown_pct.to_string(),
                price_status: if marked.missing_symbols.is_empty() {
                    "live".to_string()
                } else {
                    "missing".to_string()
                },
                open_positions_count: marked.positions.len(),
                calculated_at: snapshot_at,
            },
            request_id: request.request_id,
            correlation_id: request.correlation_id,
            timestamp: Utc::now(),
        }),
    )
        .into_response()
}

async fn get_market_symbols(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> Json<MarketSymbolsResponse> {
    let request = request_context(request);

    Json(MarketSymbolsResponse {
        exchange: state.market_config.exchange.as_str().to_string(),
        symbols: state.market_config.symbols_as_strings(),
        request_id: request.request_id,
        correlation_id: request.correlation_id,
        timestamp: Utc::now(),
    })
}

async fn get_latest_tick(
    State(state): State<AppState>,
    Query(query): Query<LatestTickQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let symbol = match Symbol::new(query.symbol) {
        Ok(symbol) => symbol,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_symbol",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    match get_latest_market_tick(&state.db_pool, state.market_config.exchange, &symbol).await {
        Ok(Some(tick)) => (
            StatusCode::OK,
            Json(MarketTickResponse {
                tick,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "tick_not_found",
                message: "No market tick found for symbol.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                symbol = %symbol,
                "failed to query latest market tick"
            );

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_market_tick",
                    message: "Failed to query latest market tick.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn get_market_candles(
    State(state): State<AppState>,
    Query(query): Query<CandlesQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let symbol = match Symbol::new(query.symbol) {
        Ok(symbol) => symbol,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_symbol",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };
    let interval = match query.interval.parse::<CandleInterval>() {
        Ok(interval) => interval,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_interval",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    match list_candles(
        &state.db_pool,
        state.market_config.exchange,
        &symbol,
        interval,
        bounded_candle_limit(query.limit),
    )
    .await
    {
        Ok(candles) => (
            StatusCode::OK,
            Json(CandlesResponse {
                candles,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                symbol = %symbol,
                interval = %interval.as_str(),
                "failed to query market candles"
            );

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_market_candles",
                    message: "Failed to query market candles.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn post_market_backfill_candles(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
    Json(mut payload): Json<CandleBackfillRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    if payload.correlation_id.is_none() {
        payload.correlation_id = Uuid::parse_str(&request.correlation_id).ok();
    }
    if let Err(err) = payload.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "invalid_backfill_request",
                message: err.to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    let service = match HistoricalCandleBackfillService::new(
        state.db_pool.clone(),
        state.config.app_name.clone(),
        &state.market_config.binance_rest_base_url,
    ) {
        Ok(service) => service,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "invalid_backfill_service_configuration",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    match service.run(payload).await {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to backfill market candles"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_backfill_market_candles",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn get_market_backfill_runs(
    State(state): State<AppState>,
    Query(query): Query<BackfillRunsQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    match list_candle_backfill_runs(&state.db_pool, bounded_backfill_runs_limit(query.limit)).await
    {
        Ok(runs) => {
            let runs = match runs
                .iter()
                .map(candle_backfill_result)
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(runs) => runs,
                Err(err) => {
                    error!(
                        request_id = %request.request_id,
                        correlation_id = %request.correlation_id,
                        error = %err,
                        "failed to map candle backfill runs"
                    );
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: "failed_to_map_backfill_runs",
                            message: "Failed to map candle backfill runs.".to_string(),
                            request_id: request.request_id,
                            correlation_id: request.correlation_id,
                            timestamp: Utc::now(),
                        }),
                    )
                        .into_response();
                }
            };

            (
                StatusCode::OK,
                Json(CandleBackfillRunsResponse {
                    runs,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to list candle backfill runs"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_list_backfill_runs",
                    message: "Failed to list candle backfill runs.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn get_market_backfill_run(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    match get_candle_backfill_run(&state.db_pool, id).await {
        Ok(Some(record)) => match candle_backfill_result(&record) {
            Ok(run) => (
                StatusCode::OK,
                Json(CandleBackfillRunResponse {
                    run,
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response(),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_map_backfill_run",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response(),
        },
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "backfill_run_not_found",
                message: "Backfill run not found.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                run_id = %id,
                "failed to query candle backfill run"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_backfill_run",
                    message: "Failed to query candle backfill run.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn get_market_feed_status(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    match list_market_feed_statuses(&state.db_pool).await {
        Ok(feeds) => (
            StatusCode::OK,
            Json(FeedStatusResponse {
                feeds,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to query market feed status"
            );

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_market_feed_status",
                    message: "Failed to query market feed status.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn get_strategy_list(
    State(state): State<AppState>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);

    if let Err(err) = ensure_strategy_configs(&state).await {
        error!(
            request_id = %request.request_id,
            correlation_id = %request.correlation_id,
            error = %err,
            "failed to ensure strategy configs"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_initialize_strategy_configs",
                message: "Failed to initialize strategy configs.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    match list_strategy_status(&state.db_pool).await {
        Ok(strategies) => (
            StatusCode::OK,
            Json(StrategyListResponse {
                strategies: strategies.into_iter().map(strategy_status_view).collect(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to list strategy status"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_strategy_status",
                    message: "Failed to query strategy status.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn get_strategy_by_id(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let strategy_id = match parse_strategy_id(&id) {
        Ok(strategy_id) => strategy_id,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_strategy_id",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    if let Err(err) = ensure_strategy_config(&state, strategy_id).await {
        error!(
            request_id = %request.request_id,
            correlation_id = %request.correlation_id,
            error = %err,
            strategy_id = %strategy_id,
            "failed to ensure strategy config"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_initialize_strategy_config",
                message: "Failed to initialize strategy config.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    match get_strategy_status(&state.db_pool, strategy_id).await {
        Ok(Some(strategy)) => (
            StatusCode::OK,
            Json(StrategyStatusResponse {
                strategy: strategy_status_view(strategy),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "strategy_not_found",
                message: "Strategy configuration was not found.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                strategy_id = %strategy_id,
                "failed to query strategy status"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_strategy_status",
                    message: "Failed to query strategy status.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn enable_strategy(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    toggle_strategy_status(state, id, StrategyStatus::Enabled, request).await
}

async fn disable_strategy(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    toggle_strategy_status(state, id, StrategyStatus::Disabled, request).await
}

async fn toggle_strategy_status(
    state: AppState,
    id: String,
    status: StrategyStatus,
    request: Option<Extension<RequestContext>>,
) -> Response {
    let request = request_context(request);
    let strategy_id = match parse_strategy_id(&id) {
        Ok(strategy_id) => strategy_id,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_strategy_id",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    let mut config = match ensure_strategy_config(&state, strategy_id).await {
        Ok(config) => config,
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                strategy_id = %strategy_id,
                "failed to load strategy config"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_load_strategy_config",
                    message: "Failed to load strategy config.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };
    config.status = status;

    match upsert_strategy_config(&state.db_pool, &config).await {
        Ok(_) => match get_strategy_status(&state.db_pool, strategy_id).await {
            Ok(Some(strategy)) => (
                StatusCode::OK,
                Json(StrategyToggleResponse {
                    strategy: strategy_status_view(strategy),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response(),
            Ok(None) => (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: "strategy_not_found",
                    message: "Strategy configuration was not found.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response(),
            Err(err) => {
                error!(
                    request_id = %request.request_id,
                    correlation_id = %request.correlation_id,
                    error = %err,
                    strategy_id = %strategy_id,
                    "failed to read toggled strategy status"
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "failed_to_query_strategy_status",
                        message: "Failed to query updated strategy status.".to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response()
            }
        },
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                strategy_id = %strategy_id,
                "failed to persist strategy status"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_update_strategy_status",
                    message: "Failed to update strategy status.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn get_recent_signals(
    State(state): State<AppState>,
    Query(query): Query<RecentSignalsQuery>,
    request: Option<Extension<RequestContext>>,
) -> impl IntoResponse {
    let request = request_context(request);
    let symbol = match query.symbol {
        Some(symbol) => match Symbol::new(symbol) {
            Ok(symbol) => Some(symbol),
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "invalid_symbol",
                        message: err.to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response();
            }
        },
        None => None,
    };

    match list_recent_signals(&state.db_pool, symbol.as_ref(), bounded_limit(query.limit)).await {
        Ok(signals) => (
            StatusCode::OK,
            Json(RecentSignalsResponse {
                signals,
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response(),
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                "failed to query recent signals"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_recent_signals",
                    message: "Failed to query recent signals.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response()
        }
    }
}

async fn evaluate_strategy_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Option<Extension<RequestContext>>,
    Json(payload): Json<EvaluateStrategyRequest>,
) -> impl IntoResponse {
    let request = request_context(request);
    let strategy_id = match parse_strategy_id(&id) {
        Ok(strategy_id) => strategy_id,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_strategy_id",
                    message: err.to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    let config = match ensure_strategy_config(&state, strategy_id).await {
        Ok(config) => config,
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                strategy_id = %strategy_id,
                "failed to load strategy config"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_load_strategy_config",
                    message: "Failed to load strategy config.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    let symbol = match payload.symbol {
        Some(symbol) => match Symbol::new(symbol) {
            Ok(symbol) => symbol,
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "invalid_symbol",
                        message: err.to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response();
            }
        },
        None => default_strategy_symbol(&config),
    };

    let correlation_id = payload
        .correlation_id
        .unwrap_or_else(|| parse_correlation_id(&request.correlation_id));
    let required_candles = match strategy_id {
        StrategyId::MomentumV1 => config.momentum_lookback_candles as i64 + 1,
        StrategyId::VolatilityBreakoutV1 => config.breakout_lookback_candles as i64 + 1,
    };
    let candles = match get_recent_closed_candles(
        &state.db_pool,
        &symbol,
        config.timeframe,
        required_candles.max(2),
    )
    .await
    {
        Ok(candles) => candles,
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                strategy_id = %strategy_id,
                symbol = %symbol,
                "failed to load recent closed candles"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_query_closed_candles",
                    message: "Failed to query recent closed candles.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    let evaluation_context = StrategyEvaluationContext {
        correlation_id,
        strategy_id,
        symbol: symbol.clone(),
        config,
        candles,
        evaluated_at: Utc::now(),
    };

    let evaluation = match evaluate_strategy(evaluation_context) {
        Ok(evaluation) => evaluation,
        Err(err) => {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                strategy_id = %strategy_id,
                symbol = %symbol,
                "failed to evaluate strategy"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_evaluate_strategy",
                    message: "Strategy evaluation failed.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }
    };

    if let Some(signal) = evaluation.signal.clone() {
        let insert_outcome = match insert_signal_deduped(&state.db_pool, &signal).await {
            Ok(outcome) => outcome,
            Err(err) => {
                error!(
                    request_id = %request.request_id,
                    correlation_id = %request.correlation_id,
                    error = %err,
                    strategy_id = %strategy_id,
                    symbol = %symbol,
                    "failed to persist signal"
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "failed_to_persist_signal",
                        message: "Signal could not be persisted.".to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response();
            }
        };

        if let Err(err) = update_strategy_state(
            &state.db_pool,
            strategy_id,
            evaluation.evaluated_at,
            evaluation.reason,
            Some(insert_outcome.signal.id),
            Some(insert_outcome.signal.created_at),
        )
        .await
        {
            error!(
                request_id = %request.request_id,
                correlation_id = %request.correlation_id,
                error = %err,
                strategy_id = %strategy_id,
                symbol = %symbol,
                "failed to update strategy state after signal generation"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed_to_update_strategy_state",
                    message: "Strategy state could not be updated.".to_string(),
                    request_id: request.request_id,
                    correlation_id: request.correlation_id,
                    timestamp: Utc::now(),
                }),
            )
                .into_response();
        }

        if insert_outcome.inserted {
            let event_publisher = PostgresEventPublisher::new(state.db_pool.clone());
            let event = SystemEventType::SignalGenerated.into_event(
                insert_outcome.signal.correlation_id,
                state.config.app_name.clone(),
                json!({
                    "signal_id": insert_outcome.signal.id,
                    "strategy_id": insert_outcome.signal.strategy_id,
                    "symbol": insert_outcome.signal.symbol,
                    "side": insert_outcome.signal.side,
                    "confidence": insert_outcome.signal.confidence,
                    "timeframe": insert_outcome.signal.timeframe,
                    "reason": insert_outcome.signal.reason,
                    "suggested_notional": insert_outcome.signal.suggested_notional,
                    "source_candle_open_time": insert_outcome.signal.source_candle_open_time,
                    "correlation_id": insert_outcome.signal.correlation_id,
                }),
            );
            if let Err(err) = event_publisher.publish(event).await {
                error!(
                    request_id = %request.request_id,
                    correlation_id = %request.correlation_id,
                    error = %err,
                    strategy_id = %strategy_id,
                    symbol = %symbol,
                    "failed to publish signal.generated event"
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "failed_to_publish_signal_event",
                        message: "signal.generated event could not be published.".to_string(),
                        request_id: request.request_id,
                        correlation_id: request.correlation_id,
                        timestamp: Utc::now(),
                    }),
                )
                    .into_response();
            }
        }

        return (
            StatusCode::OK,
            Json(evaluate_strategy_response(
                strategy_id,
                &symbol,
                Some(&insert_outcome),
                true,
                evaluation.reason,
                correlation_id,
            )),
        )
            .into_response();
    }

    if let Err(err) = update_strategy_state(
        &state.db_pool,
        strategy_id,
        evaluation.evaluated_at,
        evaluation.reason,
        None,
        None,
    )
    .await
    {
        error!(
            request_id = %request.request_id,
            correlation_id = %request.correlation_id,
            error = %err,
            strategy_id = %strategy_id,
            symbol = %symbol,
            "failed to update strategy state after no-signal evaluation"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "failed_to_update_strategy_state",
                message: "Strategy state could not be updated.".to_string(),
                request_id: request.request_id,
                correlation_id: request.correlation_id,
                timestamp: Utc::now(),
            }),
        )
            .into_response();
    }

    (
        StatusCode::OK,
        Json(evaluate_strategy_response(
            strategy_id,
            &symbol,
            None,
            false,
            evaluation.reason,
            correlation_id,
        )),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::{
        bounded_recent_events_limit, bounded_risk_decisions_limit, is_valid_resume_confirmation,
        order_view, parse_correlation_id_filter, parse_order_intent, parse_risk_check_context,
        risk_decision_not_found_error, RequestContext, DEFAULT_RECENT_EVENTS_LIMIT,
        DEFAULT_RISK_DECISIONS_LIMIT, MAX_RECENT_EVENTS_LIMIT, MAX_RISK_DECISIONS_LIMIT,
    };
    use crate::{CreatePaperOrderRequest, RiskEvaluateRequest};
    use aegis_core::Side;
    use chrono::Utc;
    use db::OrderRecord;
    use rust_decimal::Decimal;
    use uuid::Uuid;

    #[test]
    fn recent_events_limit_defaults_when_missing_or_invalid() {
        assert_eq!(
            bounded_recent_events_limit(None),
            DEFAULT_RECENT_EVENTS_LIMIT
        );
        assert_eq!(
            bounded_recent_events_limit(Some(0)),
            DEFAULT_RECENT_EVENTS_LIMIT
        );
        assert_eq!(
            bounded_recent_events_limit(Some(-1)),
            DEFAULT_RECENT_EVENTS_LIMIT
        );
    }

    #[test]
    fn recent_events_limit_is_capped() {
        assert_eq!(bounded_recent_events_limit(Some(25)), 25);
        assert_eq!(
            bounded_recent_events_limit(Some(10_000)),
            MAX_RECENT_EVENTS_LIMIT
        );
    }

    #[test]
    fn risk_decisions_limit_defaults_when_missing_or_invalid() {
        assert_eq!(
            bounded_risk_decisions_limit(None),
            DEFAULT_RISK_DECISIONS_LIMIT
        );
        assert_eq!(
            bounded_risk_decisions_limit(Some(0)),
            DEFAULT_RISK_DECISIONS_LIMIT
        );
        assert_eq!(
            bounded_risk_decisions_limit(Some(-1)),
            DEFAULT_RISK_DECISIONS_LIMIT
        );
    }

    #[test]
    fn risk_decisions_limit_is_capped() {
        assert_eq!(bounded_risk_decisions_limit(Some(25)), 25);
        assert_eq!(
            bounded_risk_decisions_limit(Some(10_000)),
            MAX_RISK_DECISIONS_LIMIT
        );
    }

    #[test]
    fn resume_confirmation_must_match_exact_phrase() {
        assert!(is_valid_resume_confirmation("RESUME TRADING"));
        assert!(!is_valid_resume_confirmation("resume trading"));
        assert!(!is_valid_resume_confirmation("RESUME"));
    }

    #[test]
    fn risk_request_defaults_to_request_correlation_id() {
        let request = RiskEvaluateRequest {
            signal_id: Uuid::new_v4(),
            strategy_id: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            side: Side::Buy,
            suggested_notional: "100000".to_string(),
            signal_created_at: Utc::now(),
            correlation_id: None,
        };

        let context = parse_risk_check_context(request, "2ea0ed54-f2bf-402d-8da0-4e92cde5b2a0")
            .expect("request should parse");

        assert_eq!(
            context.correlation_id,
            Uuid::parse_str("2ea0ed54-f2bf-402d-8da0-4e92cde5b2a0").expect("valid uuid")
        );
    }

    #[test]
    fn order_request_defaults_to_request_correlation_id() {
        let request = CreatePaperOrderRequest {
            risk_decision_id: Uuid::new_v4(),
            idempotency_key: "order-1".to_string(),
            symbol: "BTCUSDT".to_string(),
            side: Side::Buy,
            quantity: "1.25".to_string(),
            limit_price: Some("100000".to_string()),
            correlation_id: None,
            expires_at: None,
        };

        let intent = parse_order_intent(request, "2ea0ed54-f2bf-402d-8da0-4e92cde5b2a0")
            .expect("request should parse");

        assert_eq!(
            intent.correlation_id,
            Uuid::parse_str("2ea0ed54-f2bf-402d-8da0-4e92cde5b2a0").expect("valid uuid")
        );
    }

    #[test]
    fn correlation_id_filter_rejects_invalid_uuid() {
        assert!(parse_correlation_id_filter(Some("not-a-uuid")).is_err());
        assert_eq!(
            parse_correlation_id_filter(Some("2ea0ed54-f2bf-402d-8da0-4e92cde5b2a0"))
                .expect("valid filter"),
            Some(Uuid::parse_str("2ea0ed54-f2bf-402d-8da0-4e92cde5b2a0").expect("valid uuid"))
        );
    }

    #[test]
    fn order_view_exposes_signal_id_and_requested_notional() {
        let record = OrderRecord {
            order_id: Uuid::from_u128(0x11),
            correlation_id: Uuid::from_u128(0x12),
            risk_decision_id: Uuid::from_u128(0x13),
            idempotency_key: "momentum_v1:btc:123".to_string(),
            symbol: "BTCUSDT".to_string(),
            side: "BUY".to_string(),
            quantity: Decimal::ONE,
            limit_price: Some(Decimal::new(100_000, 0)),
            market_mode: "paper".to_string(),
            status: "FILLED".to_string(),
            execution_state: "PaperFilled".to_string(),
            status_reason: None,
            filled_price: Some(Decimal::new(100_100, 0)),
            client_order_id: "momentum_v1:btc:123".to_string(),
            exchange_order_id: None,
            signal_id: Some(Uuid::from_u128(0x14)),
            strategy_id: Some("momentum_v1".to_string()),
            requested_notional: Some(Decimal::new(100_000, 0)),
            filled_qty: Decimal::ONE,
            avg_fill_price: Some(Decimal::new(100_100, 0)),
            mode: "paper".to_string(),
            submitted_at: None,
            filled_at: None,
            cancelled_at: None,
            rejected_at: None,
            expired_at: None,
            expires_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let view = order_view(record);

        assert_eq!(view.signal_id, Some(Uuid::from_u128(0x14)));
        assert_eq!(view.requested_notional.as_deref(), Some("100000"));
        assert_eq!(view.client_order_id, "momentum_v1:btc:123");
    }

    #[test]
    fn risk_decision_not_found_error_uses_expected_message() {
        let request = RequestContext {
            request_id: "req-1".to_string(),
            correlation_id: "corr-1".to_string(),
        };

        let error = risk_decision_not_found_error(&request);

        assert_eq!(error.error, "risk_decision_not_found");
        assert_eq!(error.message, "Risk decision was not found.");
        assert_eq!(error.request_id, "req-1");
        assert_eq!(error.correlation_id, "corr-1");
    }
}
